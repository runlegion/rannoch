//! The measurements of NFR-RANNOCH-011, 012, 013 and 014, each run as its NFR writes it
//! and reported against its NFR's target: the measured value, the target, and whether the value
//! meets the bound. Each test fails when a value misses its bound.
//!
//! They are not part of the library and do not run in CI: they need legion's store and a machine
//! running nothing else. They run on Linux, macOS and Windows, by hand from a terminal and never
//! from a daemon: each memory step starts a fresh process, and on Windows that step also starts
//! `powershell.exe` to read its peak. Run them at verify time with
//!
//! ```text
//! cargo test --release --test measure -- --ignored --nocapture --test-threads=1
//! ```
//!
//! They live outside rannoch's own code, and read the graph only through its public interface.
//! The baseline alone reads the file with its own connection, the way T4's baseline did.
//! NFR-RANNOCH-016's measurement observes the reader connection's busy handler, so it runs inside
//! the crate (`src/measure.rs`); run it with
//!
//! ```text
//! cargo test --release --lib nfr_016 -- --ignored --nocapture --test-threads=1
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;

use rannoch::{
    Class, Count, Date, Declaration, GroupBy, NodeRef, Relation, Step, Unit, ValidTime, Write,
};

const NONE: [(&str, &str); 0] = [];

/// One measurement's result against its NFR's target.
struct Report {
    nfr: &'static str,
    what: String,
    value: f64,
    unit: &'static str,
    target: &'static str,
    met: bool,
}

impl Report {
    fn print(&self) {
        let verdict = if self.met { "met" } else { "NOT met" };
        println!(
            "{}: {} {:.2} {}; target {}; {verdict}",
            self.nfr, self.what, self.value, self.unit, self.target
        );
    }
}

/// Prints every report, then fails naming each one that missed its target.
fn record(reports: &[Report]) {
    for report in reports {
        report.print();
    }
    let missed: Vec<String> = reports
        .iter()
        .filter(|r| !r.met)
        .map(|r| format!("{} ({})", r.nfr, r.what))
        .collect();
    assert!(
        missed.is_empty(),
        "missed its target: {}",
        missed.join(", ")
    );
}

fn millis(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1e3
}

fn slowest(times: &[f64]) -> f64 {
    times.iter().copied().fold(0.0, f64::max)
}

fn day(year: u16, month: u8, d: u8) -> ValidTime {
    ValidTime::On(Date::day(year, month, d))
}

/// A burst written on its own connection, in one write, while the test measures.
struct Burst {
    done: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl Burst {
    fn start(path: &Path, write: Write) -> Burst {
        let path = path.to_path_buf();
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        let handle = std::thread::spawn(move || {
            let mut g = rannoch::open(path).unwrap();
            g.write(&write).unwrap();
            flag.store(true, Ordering::SeqCst);
        });
        Burst { done, handle }
    }

    fn running(&self) -> bool {
        !self.done.load(Ordering::SeqCst)
    }

    fn join(self) {
        self.handle.join().unwrap();
    }
}

// ---------- NFR-RANNOCH-011 and 012: legion's store ----------

/// legion's store, where legion keeps it: `LEGION_DATA_DIR`, else `legion` in the platform's data
/// directory (`~/Library/Application Support` on macOS, `$XDG_DATA_HOME` or `~/.local/share` on
/// Linux, `%APPDATA%` on Windows).
fn legion_store() -> PathBuf {
    let dir = std::env::var_os("LEGION_DATA_DIR").map_or_else(
        || {
            dirs::data_dir()
                .expect("the platform has a data directory")
                .join("legion")
        },
        PathBuf::from,
    );
    dir.join("legion.db")
}

/// A copy of legion's store, taken read-only now: its WAL included, the store unchanged.
fn copy_legion_store(dest: &Path) {
    let store = legion_store();
    let conn = Connection::open_with_flags(
        &store,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap_or_else(|e| panic!("legion's store at {}: {e}", store.display()));
    conn.execute("VACUUM INTO ?1", [dest.to_str().unwrap()])
        .unwrap();
}

/// A reflection as T3 read it from the store.
struct Reflection {
    id: String,
    repo: String,
    text: String,
    created: String,
    parent: Option<String>,
    tags: Option<String>,
}

/// The tags T3 read from a reflection's tags column.
fn tags(column: &str) -> Vec<&str> {
    column
        .trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .map(|t| t.trim().trim_matches('"'))
        .filter(|t| !t.is_empty())
        .collect()
}

/// A stored timestamp's date as a valid time; one that holds no date is unknown.
fn valid_from(created: &str) -> ValidTime {
    let year = created.get(0..4).and_then(|s| s.parse().ok());
    let month = created.get(5..7).and_then(|s| s.parse().ok());
    let d = created.get(8..10).and_then(|s| s.parse().ok());
    match (year, month, d) {
        (Some(year), Some(month), Some(d)) => day(year, month, d),
        _ => ValidTime::Unknown,
    }
}

/// Loads the copy of legion's store into a rannoch graph as T3 did: reflections, repos and
/// tags as nodes; about (reflection to repo), follows (reflection to parent) and tagged
/// (reflection to tag) as links. Returns the nodes and links it wrote.
fn load_legion(copy: &Path, graph: &Path) -> (usize, usize) {
    let src = Connection::open_with_flags(copy, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let reflections: Vec<Reflection> = src
        .prepare("SELECT id, repo, text, created_at, parent_id, tags FROM reflections")
        .unwrap()
        .query_map([], |r| {
            Ok(Reflection {
                id: r.get(0)?,
                repo: r.get(1)?,
                text: r.get(2)?,
                created: r.get(3)?,
                parent: r.get(4)?,
                tags: r.get(5)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut g = rannoch::open(graph).unwrap();
    for d in [
        Declaration::Class(Class::new("reflection", ["text"])),
        Declaration::Class(Class::new("repo", [] as [&str; 0])),
        Declaration::Class(Class::new("tag", [] as [&str; 0])),
        Declaration::Relation(Relation::new("about", "reflection", "repo", false)),
        Declaration::Relation(Relation::new("follows", "reflection", "reflection", false)),
        Declaration::Relation(Relation::new("tagged", "reflection", "tag", false)),
    ] {
        g.declare(&d).unwrap();
    }
    // A repo is true from its first reflection, as T3 took it.
    let mut repos: HashMap<&str, &str> = HashMap::new();
    let mut all_tags: Vec<&str> = Vec::new();
    for r in &reflections {
        let first = repos.entry(r.repo.as_str()).or_insert(r.created.as_str());
        if r.created.as_str() < *first {
            *first = r.created.as_str();
        }
        all_tags.extend(r.tags.as_deref().map(tags).unwrap_or_default());
    }
    all_tags.sort_unstable();
    all_tags.dedup();
    let mut write = Write::new("legion");
    for (repo, first) in &repos {
        write = write.unit(Unit::new("repo", *repo, NONE, valid_from(first)));
    }
    for tag in &all_tags {
        write = write.unit(Unit::new("tag", *tag, NONE, ValidTime::Unknown));
    }
    let mut links = 0;
    for r in &reflections {
        let valid = valid_from(&r.created);
        let mut unit = Unit::new("reflection", &r.id, [("text", &r.text)], valid.clone()).link(
            "about",
            &r.id,
            &r.repo,
            valid.clone(),
        );
        links += 1;
        if let Some(parent) = &r.parent {
            unit = unit.link("follows", &r.id, parent, valid.clone());
            links += 1;
        }
        let mine = r.tags.as_deref().map(tags).unwrap_or_default();
        links += mine.len();
        for tag in mine {
            unit = unit.link("tagged", &r.id, tag, valid.clone());
        }
        write = write.unit(unit);
    }
    g.write(&write).unwrap();
    (reflections.len() + repos.len() + all_tags.len(), links)
}

/// A hook's lookup: a fresh connection, find by natural key, a 2-hop walk capped at 200.
fn hook_lookup(graph: &Path) -> f64 {
    let start = Instant::now();
    let g = rannoch::open(graph).unwrap();
    let root = g.find("repo", "legion").unwrap().expect("repo legion");
    let found = g
        .walk(
            &NodeRef::new(root.class, root.key),
            &["about<-", "tagged", "follows"],
            2,
            200,
        )
        .unwrap();
    assert!(!found.is_empty());
    millis(start)
}

/// A hook's write: a fresh connection, one event plus its node in one transaction.
fn hook_write(graph: &Path, i: usize) -> f64 {
    let start = Instant::now();
    let mut g = rannoch::open(graph).unwrap();
    let events = g
        .write(&Write::new("hook").unit(Unit::new(
            "reflection",
            format!("hook-{i}"),
            [("text", "hook note")],
            day(2026, 10, 13),
        )))
        .unwrap();
    assert_eq!(events.len(), 1);
    millis(start)
}

/// The burst T3's writer stood in for: synced reflections about one repo, in one write at
/// rannoch's own piece size, large enough to outlast 150 hook lookups or writes that do not
/// wait on it.
fn sync_burst(name: &str) -> Write {
    let mut write = Write::new("sync");
    for k in 0..400_000 {
        let id = format!("{name}-{k}");
        write = write.unit(
            Unit::new(
                "reflection",
                id.clone(),
                [("text", "synced fact body")],
                day(2026, 10, 13),
            )
            .link("about", id, "smugglr", day(2026, 10, 13)),
        );
    }
    write
}

#[test]
#[ignore = "a measurement: run at verify time on a copy of legion's store (see the module docs)"]
fn nfr_011_012() {
    let dir = TempDir::new().unwrap();
    let copy = dir.path().join("legion-copy.db");
    let graph = dir.path().join("legion.rannoch");
    copy_legion_store(&copy);
    let (nodes, links) = load_legion(&copy, &graph);
    println!("copy of legion's store: wrote {nodes} nodes and {links} links");

    let quiet: Vec<f64> = (0..150).map(|_| hook_lookup(&graph)).collect();

    let burst = Burst::start(&graph, sync_burst("lookup-burst"));
    std::thread::sleep(Duration::from_millis(50));
    let mut during = Vec::new();
    for _ in 0..150 {
        during.push(hook_lookup(&graph));
        std::thread::sleep(Duration::from_millis(7));
    }
    let overlapped = burst.running();
    burst.join();
    assert!(overlapped, "the burst ended before the 150 lookups did");

    // Hook writes while the burst commits: a write waiting on the burst still counts, and the
    // measurement holds only if all 150 land before the burst ends.
    let burst = Burst::start(&graph, sync_burst("write-burst"));
    std::thread::sleep(Duration::from_millis(50));
    let mut writes = Vec::new();
    while writes.len() < 150 && burst.running() {
        writes.push(hook_write(&graph, writes.len()));
        std::thread::sleep(Duration::from_millis(7));
    }
    burst.join();

    record(&[
        Report {
            nfr: "NFR-RANNOCH-011",
            what: format!("slowest of {} hook writes during a burst", writes.len()),
            value: slowest(&writes),
            unit: "ms",
            target: "< 200 ms, every one of 150 writes",
            met: writes.len() == 150 && slowest(&writes) < 200.0,
        },
        Report {
            nfr: "NFR-RANNOCH-012",
            what: format!("slowest of {} lookups with no writer", quiet.len()),
            value: slowest(&quiet),
            unit: "ms",
            target: "every lookup < 200 ms",
            met: slowest(&quiet) < 200.0,
        },
        Report {
            nfr: "NFR-RANNOCH-012",
            what: format!("slowest of {} lookups during a burst", during.len()),
            value: slowest(&during),
            unit: "ms",
            target: "every lookup < 200 ms",
            met: slowest(&during) < 200.0,
        },
    ]);
}

// ---------- NFR-RANNOCH-013 and 014: the 100,000-voice file ----------

const PLACES: [&str; 5] = ["discord", "reddit", "forums", "youtube", "steam"];

/// T4's per-voice statement histogram (research 01a13996), in its fixed order.
fn histogram() -> Vec<usize> {
    include_str!("fixtures/histogram-per-voice.txt")
        .lines()
        .map(|line| line.trim().parse().unwrap())
        .collect()
}

/// Adds voice `v` with `n` statements to `write` as T4 drew them: each at a place, 30% of them
/// supporting a finding. Counts each supported finding's statements in `supported`.
fn voice(
    mut write: Write,
    rng: &mut fastrand::Rng,
    v: &str,
    n: usize,
    supported: &mut HashMap<String, u64>,
) -> Write {
    let when = day(2026, 10, 14);
    write = write.unit(Unit::new("voice", v, NONE, when.clone()));
    for k in 0..n {
        let supports =
            (rng.f64() < 0.3).then(|| format!("f{}", (1000.0 * rng.f64().powi(2)) as usize));
        let place = PLACES[rng.usize(..PLACES.len())];
        let statement = format!("{v}s{k}");
        let mut unit = Unit::new(
            "statement",
            statement.clone(),
            [("place", place)],
            when.clone(),
        )
        .link("said", v, statement.clone(), when.clone());
        if let Some(finding) = supports {
            unit = unit.link("supports", statement, finding.clone(), when.clone());
            *supported.entry(finding).or_default() += 1;
        }
        write = write.unit(unit);
    }
    write
}

/// Builds the 100,000-voice graph at `path`: T4's histogram resampled with seed 4, 1,000
/// findings, 5 places. Returns the largest finding's key.
fn t4(path: &Path) -> String {
    let histogram = histogram();
    let mut rng = fastrand::Rng::with_seed(4);
    let mut supported: HashMap<String, u64> = HashMap::new();
    let mut g = rannoch::open(path).unwrap();
    for d in [
        Declaration::Class(Class::new("voice", [] as [&str; 0])),
        Declaration::Class(Class::new("statement", ["place"])),
        Declaration::Class(Class::new("finding", [] as [&str; 0])),
        Declaration::Relation(Relation::new("said", "voice", "statement", false)),
        Declaration::Relation(Relation::new("supports", "statement", "finding", false)),
    ] {
        g.declare(&d).unwrap();
    }
    let when = day(2026, 10, 14);
    let mut write = Write::new("t4");
    for f in 0..1000 {
        write = write.unit(Unit::new("finding", format!("f{f}"), NONE, when.clone()));
    }
    g.write(&write).unwrap();
    for batch in 0..100 {
        let mut write = Write::new("t4");
        for v in batch * 1000..(batch + 1) * 1000 {
            let n = histogram[rng.usize(..histogram.len())];
            write = voice(write, &mut rng, &format!("v{v}"), n, &mut supported);
        }
        g.write(&write).unwrap();
    }
    supported
        .into_iter()
        .max_by_key(|(f, n)| (*n, std::cmp::Reverse(f.clone())))
        .map(|(f, _)| f)
        .unwrap()
}

/// The count NFR-RANNOCH-013 names: the largest finding's distinct voices and total
/// statements, grouped by place.
fn count_one(largest: &str) -> Count {
    Count {
        class: "finding".into(),
        roots: vec![largest.to_string()],
        path: vec![Step::new("supports<-"), Step::new("said<-")],
        group_by: Some(GroupBy {
            class: "statement".into(),
            property: "place".into(),
        }),
        as_of: None,
    }
}

/// The count NFR-RANNOCH-014 names: every finding's distinct voices and total, in one pass.
fn count_all() -> Count {
    Count {
        class: "finding".into(),
        roots: (0..1000).map(|f| format!("f{f}")).collect(),
        path: vec![Step::new("supports<-"), Step::new("said<-")],
        group_by: None,
        as_of: None,
    }
}

/// A quoted SQL identifier, as the file names its tables and columns.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The baseline: every statement's voice and properties and every supports link, held in
/// memory, read from the file with its own connection.
fn baseline(path: &Path) {
    let g = rannoch::open(path).unwrap();
    let properties: Vec<String> = g.ontology().classes["statement"]
        .properties
        .iter()
        .map(|p| format!("s.{}", ident(&format!("p:{p}"))))
        .collect();
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut statements: HashMap<i64, (i64, Vec<Option<String>>)> = HashMap::new();
    let sql = format!(
        "SELECT s.id, said.src, {} FROM {} s JOIN {} said ON said.dst = s.id",
        properties.join(", "),
        ident("class:statement"),
        ident("relation:said")
    );
    let mut statement = conn.prepare(&sql).unwrap();
    let mut rows = statement.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let held = (0..properties.len())
            .map(|i| row.get(2 + i))
            .collect::<Result<_, _>>()
            .unwrap();
        statements.insert(row.get(0).unwrap(), (row.get(1).unwrap(), held));
    }
    let mut supports: Vec<(i64, i64)> = Vec::new();
    let sql = format!("SELECT src, dst FROM {}", ident("relation:supports"));
    let mut statement = conn.prepare(&sql).unwrap();
    let mut rows = statement.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        supports.push((row.get(0).unwrap(), row.get(1).unwrap()));
    }
    println!(
        "baseline holds {} statements and {} supports links",
        std::hint::black_box(&statements).len(),
        std::hint::black_box(&supports).len()
    );
}

/// Names the step a fresh process runs, and the file and root it runs on.
const STEP: &str = "RANNOCH_MEASURE_STEP";
const FILE: &str = "RANNOCH_MEASURE_FILE";
const ROOT: &str = "RANNOCH_MEASURE_ROOT";

/// The line a step prints its own peak on, for the parent to read.
const PEAK: &str = "peak resident set bytes:";

/// This process's peak resident set in bytes: `VmHWM`, in kilobytes. Not `ru_maxrss`, which
/// Linux carries over from the parent at `exec`, so a step would report the parent's peak.
#[cfg(target_os = "linux")]
fn peak_resident_set() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let kilobytes: u64 = status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .trim()
                .strip_suffix("kB")?
                .trim()
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("no VmHWM in:\n{status}"));
    kilobytes * 1024
}

/// This process's peak resident set in bytes: `ru_maxrss`, which macOS reports in bytes.
#[cfg(target_os = "macos")]
fn peak_resident_set() -> u64 {
    use nix::sys::resource::{UsageWho, getrusage};
    u64::try_from(getrusage(UsageWho::RUSAGE_SELF).unwrap().max_rss()).unwrap()
}

/// This process's peak working set in bytes, Windows' resident set, read while the process is
/// still alive: the kernel keeps it per process, and reading it needs no unsafe code this way.
#[cfg(windows)]
fn peak_resident_set() -> u64 {
    let query = format!("(Get-Process -Id {}).PeakWorkingSet64", std::process::id());
    let out = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &query])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("no peak working set in:\n{stdout}"))
}

/// Runs one step of this test in a fresh process, and returns the peak resident set in bytes
/// that the step reports for itself.
fn peak_in_fresh_process(step: &str, file: &Path, root: &str) -> u64 {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "nfr_013_014", "--ignored", "--nocapture"])
        .env(STEP, step)
        .env(FILE, file)
        .env(ROOT, root)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "step {step} failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("1 passed"),
        "step {step} ran no test:\n{stdout}"
    );
    stdout
        .lines()
        .find_map(|line| line.trim().strip_prefix(PEAK)?.trim().parse().ok())
        .unwrap_or_else(|| panic!("no {PEAK} line in:\n{stdout}"))
}

#[test]
#[ignore = "a measurement: run at verify time on a machine running nothing else (see the module docs)"]
fn nfr_013_014() {
    if let Ok(step) = std::env::var(STEP) {
        let file = PathBuf::from(std::env::var(FILE).unwrap());
        match step.as_str() {
            "baseline" => baseline(&file),
            "one" => {
                let g = rannoch::open(&file).unwrap();
                let tally = g.count(&count_one(&std::env::var(ROOT).unwrap())).unwrap();
                println!("{tally:?}");
            }
            "all" => {
                let g = rannoch::open(&file).unwrap();
                println!("{} tallies", g.count(&count_all()).unwrap().len());
            }
            other => panic!("no step {other}"),
        }
        println!("{PEAK} {}", peak_resident_set());
        return;
    }
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("t4.rannoch");
    let largest = t4(&file);
    let base = peak_in_fresh_process("baseline", &file, &largest);
    let one = peak_in_fresh_process("one", &file, &largest);
    let all = peak_in_fresh_process("all", &file, &largest);
    println!(
        "peak resident set: baseline {base} bytes, one count {one} bytes, all 1,000 findings {all} bytes"
    );
    let share = |peak: u64| 100.0 * peak as f64 / base as f64;
    record(&[
        Report {
            nfr: "NFR-RANNOCH-013",
            what: "one count's peak as a share of the baseline's".into(),
            value: share(one),
            unit: "%",
            target: "< 10% of the whole-graph load's peak",
            met: share(one) < 10.0,
        },
        Report {
            nfr: "NFR-RANNOCH-014",
            what: "all 1,000 findings' peak as a share of the baseline's".into(),
            value: share(all),
            unit: "%",
            target: "< 10% of the whole-graph load's peak",
            met: share(all) < 10.0,
        },
    ]);
}
