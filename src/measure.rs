//! The measurements of NFR-RANNOCH-016 and NFR-RANNOCH-025, each run as its NFR writes it and
//! reported against its target. NFR-RANNOCH-025's times the crate's own check that a named event
//! exists, which the public interface reaches only inside a whole write.
//!
//! It runs inside the crate because whether a count read blocked on the writer is observed on
//! the reader connection's busy handler, which rannoch's public interface does not expose. It
//! names no app's class or relation: the graph's shape, its classes, relations, property, keys
//! and count path, comes from the data file `tests/fixtures/nfr-016-shape.json`, and the
//! per-speaker histogram from `tests/fixtures/histogram-per-voice.txt`.
//!
//! It does not run in CI: it needs a machine running nothing else. Run it at verify time with
//!
//! ```text
//! cargo test --release --lib nfr_016 -- --ignored --nocapture --test-threads=1
//! cargo test --release --lib named_event_check -- --ignored --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rusqlite::ErrorCode;
use serde::Deserialize;
use tempfile::TempDir;

use crate::{
    Class, Count, Date, Declaration, Error, Graph, GroupBy, Relation, Step, Unit, ValidTime, Write,
};

const NONE: [(&str, &str); 0] = [];

/// The graph's shape, read from the data file.
#[derive(Deserialize)]
struct Shape {
    speaker_class: String,
    speaker_key: String,
    utterance_class: String,
    utterance_key: String,
    property: String,
    values: Vec<String>,
    subject_class: String,
    subject_key: String,
    subjects: usize,
    speakers: usize,
    speaks: String,
    backs: String,
    backing_share: f64,
    path: Vec<String>,
    source: String,
    seed: u64,
    burst_source: String,
    burst_key: String,
    burst_seed: u64,
}

fn shape() -> Shape {
    serde_json::from_str(include_str!("../tests/fixtures/nfr-016-shape.json")).unwrap()
}

/// The per-speaker utterance histogram (research 01a13996), in its fixed order.
fn histogram() -> Vec<usize> {
    include_str!("../tests/fixtures/histogram-per-voice.txt")
        .lines()
        .map(|line| line.trim().parse().unwrap())
        .collect()
}

fn when() -> ValidTime {
    ValidTime::On(Date::day(2026, 10, 14))
}

/// Adds speaker `who` with `n` utterances to `write`: each with one of the property's values,
/// a share of them backing a subject. Counts each backed subject's utterances in `backed`.
fn speaker(
    shape: &Shape,
    mut write: Write,
    rng: &mut fastrand::Rng,
    who: &str,
    n: usize,
    backed: &mut HashMap<String, u64>,
) -> Write {
    write = write.unit(Unit::new(&shape.speaker_class, who, NONE, when()));
    for k in 0..n {
        let backs = (rng.f64() < shape.backing_share).then(|| {
            let i = (shape.subjects as f64 * rng.f64().powi(2)) as usize;
            format!("{}{i}", shape.subject_key)
        });
        let value = &shape.values[rng.usize(..shape.values.len())];
        let utterance = format!("{who}{}{k}", shape.utterance_key);
        let mut unit = Unit::new(
            &shape.utterance_class,
            utterance.clone(),
            [(shape.property.as_str(), value.as_str())],
            when(),
        )
        .link(&shape.speaks, who, utterance.clone(), when());
        if let Some(subject) = backs {
            unit = unit.link(&shape.backs, utterance, subject.clone(), when());
            *backed.entry(subject).or_default() += 1;
        }
        write = write.unit(unit);
    }
    write
}

/// Builds the graph at `path`: the histogram resampled to the shape's speakers, its subjects
/// and values. Returns the largest subject's key.
fn build(shape: &Shape, path: &Path) -> String {
    let histogram = histogram();
    let mut rng = fastrand::Rng::with_seed(shape.seed);
    let mut backed: HashMap<String, u64> = HashMap::new();
    let mut g = crate::open(path, Some(&crate::TEST_KEY)).unwrap();
    for d in [
        Declaration::Class(Class::new(&shape.speaker_class, [] as [&str; 0])),
        Declaration::Class(Class::new(&shape.utterance_class, [&shape.property])),
        Declaration::Class(Class::new(&shape.subject_class, [] as [&str; 0])),
        Declaration::Relation(Relation::new(
            &shape.speaks,
            &shape.speaker_class,
            &shape.utterance_class,
            false,
        )),
        Declaration::Relation(Relation::new(
            &shape.backs,
            &shape.utterance_class,
            &shape.subject_class,
            false,
        )),
    ] {
        g.declare(&d).unwrap();
    }
    let mut write = Write::new(&shape.source);
    for s in 0..shape.subjects {
        let key = format!("{}{s}", shape.subject_key);
        write = write.unit(Unit::new(&shape.subject_class, key, NONE, when()));
    }
    g.write(&write).unwrap();
    for batch in 0..shape.speakers / 1000 {
        let mut write = Write::new(&shape.source);
        for v in batch * 1000..(batch + 1) * 1000 {
            let n = histogram[rng.usize(..histogram.len())];
            let who = format!("{}{v}", shape.speaker_key);
            write = speaker(shape, write, &mut rng, &who, n, &mut backed);
        }
        g.write(&write).unwrap();
    }
    backed
        .into_iter()
        .max_by_key(|(s, n)| (*n, std::cmp::Reverse(s.clone())))
        .map(|(s, _)| s)
        .unwrap()
}

/// The count NFR-RANNOCH-016 reads: the largest subject's distinct speakers and total
/// utterances, grouped by the property.
fn largest_count(shape: &Shape, largest: &str) -> Count {
    Count {
        class: shape.subject_class.clone(),
        roots: vec![largest.to_string()],
        path: shape.path.iter().map(Step::new).collect(),
        group_by: Some(GroupBy {
            class: shape.utterance_class.clone(),
            property: shape.property.clone(),
        }),
        as_of: None,
        known_at: None,
    }
}

/// The burst, in the same shape: the histogram once over, as new speakers.
fn burst(shape: &Shape) -> Write {
    let mut rng = fastrand::Rng::with_seed(shape.burst_seed);
    let mut backed = HashMap::new();
    let mut write = Write::new(&shape.burst_source);
    for (i, n) in histogram().into_iter().enumerate() {
        let who = format!("{}{i}", shape.burst_key);
        write = speaker(shape, write, &mut rng, &who, n, &mut backed);
    }
    write
}

/// Every call of the reader connection's busy handler: each is a wait on a lock.
static WAITS: AtomicUsize = AtomicUsize::new(0);

/// The reader's busy handler: counts the wait, then waits as the connection's default busy
/// timeout does (about 5 s of retries) before giving up.
fn wait(attempt: i32) -> bool {
    WAITS.fetch_add(1, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(1));
    attempt < 5000
}

/// One count read: its time, whether it returned busy, and whether it blocked on a lock.
struct Read {
    ms: f64,
    busy: bool,
    blocked: bool,
}

fn read(reader: &Graph, count: &Count) -> Read {
    let waits = WAITS.load(Ordering::SeqCst);
    let start = Instant::now();
    let result = reader.count(count);
    let ms = start.elapsed().as_secs_f64() * 1e3;
    let blocked = WAITS.load(Ordering::SeqCst) > waits;
    let busy = match result {
        Ok(_) => false,
        Err(Error::Storage(rusqlite::Error::SqliteFailure(e, _)))
            if matches!(e.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) =>
        {
            true
        }
        Err(e) => panic!("the count failed: {e}"),
    };
    Read { ms, busy, blocked }
}

fn slowest(reads: &[Read]) -> f64 {
    reads.iter().map(|r| r.ms).fold(0.0, f64::max)
}

#[test]
#[ignore = "a measurement: run at verify time on a machine running nothing else (see the module docs)"]
fn nfr_016() {
    let shape = shape();
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("t4.rannoch");
    let count = largest_count(&shape, &build(&shape, &file));
    let reader = crate::open(&file, Some(&crate::TEST_KEY)).unwrap();
    reader.conn.busy_handler(Some(wait)).unwrap();

    let quiet: Vec<Read> = (0..20).map(|_| read(&reader, &count)).collect();

    let write = burst(&shape);
    let done = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&done);
    let path = file.clone();
    let writer = std::thread::spawn(move || {
        let mut g = crate::open(path, Some(&crate::TEST_KEY)).unwrap();
        g.write(&write).unwrap();
        flag.store(true, Ordering::SeqCst);
    });
    let mut during = Vec::new();
    while !done.load(Ordering::SeqCst) {
        during.push(read(&reader, &count));
    }
    writer.join().unwrap();
    assert!(!during.is_empty(), "no read ran during the burst");

    let busy = during.iter().filter(|r| r.busy).count();
    let blocked = during.iter().filter(|r| r.blocked).count();
    let ratio = slowest(&during) / slowest(&quiet);
    println!(
        "NFR-RANNOCH-016: {} reads with no writer, slowest {:.2} ms; {} reads during the burst, slowest {:.2} ms",
        quiet.len(),
        slowest(&quiet),
        during.len(),
        slowest(&during)
    );
    let reports = [
        (
            format!("busy reads of {} during a burst", during.len()),
            busy as f64,
            "reads",
            "no busy or blocked read",
            busy == 0,
        ),
        (
            format!("blocked reads of {} during a burst", during.len()),
            blocked as f64,
            "reads",
            "no busy or blocked read",
            blocked == 0,
        ),
        (
            "slowest read during a burst over slowest with no writer".to_string(),
            ratio,
            "x",
            "ratio <= 2",
            ratio <= 2.0,
        ),
    ];
    for (what, value, unit, target, met) in &reports {
        let verdict = if *met { "met" } else { "NOT met" };
        println!("NFR-RANNOCH-016: {what} {value:.2} {unit}; target {target}; {verdict}");
    }
    let missed: Vec<&str> = reports
        .iter()
        .filter(|r| !r.4)
        .map(|r| r.0.as_str())
        .collect();
    assert!(
        missed.is_empty(),
        "NFR-RANNOCH-016 missed its target: {}",
        missed.join(", ")
    );
}

/// A store at `path` whose log holds `events` events past its one declaration: one node write
/// per event in one keyless class.
fn store_of(path: &Path, events: usize) -> Graph {
    let mut g = crate::open(path, Some(&crate::TEST_KEY)).unwrap();
    g.declare(&Declaration::Class(Class::new("note", [] as [&str; 0])))
        .unwrap();
    let writes: Vec<Write> = (0..events)
        .map(|i| Write::new("app").unit(Unit::new("note", format!("n{i}"), NONE, when())))
        .collect();
    for batch in writes.chunks(10_000) {
        for result in g.write_batch(batch).unwrap() {
            result.unwrap();
        }
    }
    g
}

/// The p99, in microseconds, of checking that `g` holds each of 10,000 ids chosen with a seeded
/// RNG from its log.
fn named_p99(g: &Graph) -> f64 {
    let last = g.events().committed().unwrap().0;
    let mut rng = fastrand::Rng::with_seed(25);
    let ids: Vec<crate::EventId> = (0..10_000)
        .map(|_| {
            crate::events::event_at(&g.conn, crate::Cursor(rng.i64(1..=last)))
                .unwrap()
                .unwrap()
                .id
        })
        .collect();
    let mut times: Vec<f64> = ids
        .into_iter()
        .map(|id| {
            let start = Instant::now();
            assert!(crate::events::holds(&g.conn, id).unwrap());
            start.elapsed().as_secs_f64() * 1e6
        })
        .collect();
    times.sort_by(f64::total_cmp);
    times[times.len() * 99 / 100]
}

/// The query plan of the named-event check, one line per step.
fn named_plan(g: &Graph) -> Vec<String> {
    let mut statement = g
        .conn
        .prepare(&format!("EXPLAIN QUERY PLAN {}", crate::events::HOLDS))
        .unwrap();
    statement
        .query_map(["0"], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
#[ignore = "a measurement: run at verify time on a machine running nothing else (see the module docs)"]
fn named_event_check() {
    let dir = TempDir::new().unwrap();
    let mut plan = Vec::new();
    let p99s: Vec<(usize, f64)> = [1_000, 1_000_000]
        .into_iter()
        .map(|events| {
            let g = store_of(&dir.path().join(format!("{events}.rannoch")), events);
            plan = named_plan(&g);
            (events, named_p99(&g))
        })
        .collect();
    for step in &plan {
        println!("NFR-RANNOCH-025: plan: {step}");
    }
    for (events, p99) in &p99s {
        println!("NFR-RANNOCH-025: {events} events, p99 of the named-event check {p99:.2} us");
    }
    let ratio = p99s[1].1 / p99s[0].1;
    println!("NFR-RANNOCH-025: ratio {ratio:.2} x, for reference");
    let searched = plan
        .iter()
        .any(|step| step.starts_with("SEARCH _events USING"))
        && !plan.iter().any(|step| step.starts_with("SCAN _events"));
    let p99 = p99s[1].1;
    let verdict = if searched && p99 <= 1_000.0 {
        "met"
    } else {
        "NOT met"
    };
    println!(
        "NFR-RANNOCH-025: index search with no scan of the log and p99 <= 1 ms at 1,000,000 events; {verdict}"
    );
    assert!(
        searched,
        "NFR-RANNOCH-025: the check scans the log: {plan:?}"
    );
    assert!(
        p99 <= 1_000.0,
        "NFR-RANNOCH-025 missed its target: p99 {p99:.2} us"
    );
}
