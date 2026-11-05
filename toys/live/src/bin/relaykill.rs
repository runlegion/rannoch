//! A relay under kill -9 with a producer writing. `setup` makes a store and four relays. `life`
//! produces and folds until killed, or `--step` commits a few events at a time and holds the set
//! against a cold walk after each. `check` is every restart: it opens, waits for each relay to reach
//! the watermark, compares it with a cold walk, checks what the killed process told, and dies without
//! a clean close, so the kill's state goes on. `verify` counts the log. Modes take the run's directory.
#[path = "../killcommon.rs"]
mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Entry, KEY, amended, append, ask, declared, die, differs, fact, file_cursor, manifest, node, settled};
use rannoch_queue::Queue;
use rannoch_relay::Relays;
use rannoch_store::{Class, Cursor, Declaration, NodeRef, Relation, Roots, Shape, Step, Value, open};
use serde_json::{Value as Json, json};

const SOURCE: &str = "rannoch:feed";

struct Run {
    graph: PathBuf,
    relays: PathBuf,
    violations: PathBuf,
    told: PathBuf,
}

impl Run {
    fn new(dir: &str) -> Run {
        let dir = Path::new(dir);
        Run {
            graph: dir.join("r.rannoch"),
            relays: dir.join("relays"),
            violations: dir.join("violations.txt"),
            told: dir.join("told.txt"),
        }
    }

    fn violation(&self, what: &str) {
        append(&self.violations, what);
    }
}

fn step(relation: &str, cap: Option<usize>) -> Step {
    Step { relation: relation.into(), cap, ..Step::default() }
}

fn shapes() -> Vec<Shape> {
    let root = |key: &str| Roots::Node { node: NodeRef::new("author", key) };
    vec![
        Shape {
            name: "capped".into(),
            roots: root("a1"),
            steps: vec![
                Step {
                    drop: BTreeMap::from([("shelf".to_string(), Value::from("withdrawn"))]),
                    ..step("wrote", Some(40))
                },
                step("wrote<-", None),
            ],
        },
        Shape { name: "exact".into(), roots: root("a2"), steps: vec![step("wrote", None)] },
        Shape {
            name: "featured".into(),
            roots: Roots::Class {
                class: "book".into(),
                filter: BTreeMap::from([("shelf".to_string(), Value::from("feature"))]),
            },
            steps: vec![step("wrote<-", None)],
        },
    ]
}

fn setup(run: &Run) {
    let mut g = open(&run.graph, &KEY).expect("open");
    let mut book = Class::new("book", ["title", "shelf"]);
    book.indexed = BTreeSet::from(["shelf".to_string()]);
    for class in [Class::new("author", ["title"]), book, Class::new("note", ["v"])] {
        let name = class.name.clone();
        g.declare(&Declaration::Class(class), &declared(&name)).expect("class");
    }
    g.declare(&Declaration::Relation(Relation::new("wrote", "author", "book", false)), &declared("wrote"))
        .expect("relation");
    for shape in shapes() {
        let name = shape.name.clone();
        g.declare(&Declaration::Shape(shape), &declared(&name)).expect("shape");
    }
    let mut seed = Vec::new();
    for a in 1..=7 {
        seed.push(node(SOURCE, "author", &format!("a{a}"), &json!({"title": format!("author {a}")})));
    }
    let bodies: Vec<&str> = seed.iter().map(String::as_str).collect();
    for landed in g.commit(&bodies, &[]).expect("commit") {
        landed.expect("seed");
    }
    g.fill().expect("fill");
    let queue = Arc::new(Queue::open(g, Queue::INTERVAL).expect("queue"));
    let relays = Relays::open(&run.relays, Arc::clone(&queue), &run.graph, &KEY).expect("relays");
    // A view fixed at the seed: what lands after it tells nothing and moves nothing in it.
    let fixed = json!({"known": rannoch_store::millis(std::time::SystemTime::now())});
    ask(
        &queue,
        &run.relays,
        &[("capped", json!({})), ("exact", json!({})), ("featured", json!({})), ("capped", fixed)],
    );
    assert!(relays.close().is_empty());
    assert!(queue.flush().1.is_empty());
}

/// The event at position `n`: what it is depends only on `n`, but for an amendment's target, which is
/// the last book this process wrote. Every body says its position, for `verify`.
fn event(n: u64, last: &mut Option<String>) -> String {
    let decade = n / 10;
    let old = format!("b{}", (decade.wrapping_mul(2_654_435_761) >> 7) % (decade + 1) * 10);
    let new = format!("b{}", decade * 10);
    let author = if n % 100 == 1 { "a2" } else { "a1" };
    let note = || node(SOURCE, "note", &format!("n{n}"), &json!({"v": n.to_string()}));
    let body = match n % 10 {
        0 => {
            let shelf = if n % 500 == 0 { "feature" } else { "open" };
            let body = node(SOURCE, "book", &new, &json!({"title": format!("t{n}"), "shelf": shelf}));
            *last = Some(body.clone());
            body
        }
        1 => fact(SOURCE, "link", "wrote", ("author", author), ("book", &new)),
        3 => {
            let shelf = match n % 40 {
                3 => "withdrawn",
                13 => "feature",
                _ => "open",
            };
            node(SOURCE, "book", &old, &json!({"title": format!("t{n}"), "shelf": shelf}))
        }
        4 => fact(SOURCE, "link", "wrote", ("author", &format!("a{}", 3 + n % 5)), ("book", &old)),
        5 if n % 30 == 5 => fact(SOURCE, "ended", "wrote", ("author", "a1"), ("book", &old)),
        6 if n % 60 == 6 => fact(SOURCE, "corrected", "wrote", ("author", "a1"), ("book", &old)),
        7 if n % 50 == 7 => match last.as_ref() {
            Some(target) if n % 500 == 57 => amended(SOURCE, target, &json!({"subject": format!("r{n}")})),
            Some(target) => amended(SOURCE, target, &json!({"data": {"properties": {"title": format!("a{n}")}}})),
            None => note(),
        },
        8 => fact(SOURCE, "link", "wrote", ("author", "a1"), ("book", &format!("b{}", (decade + 1) * 10))),
        _ => note(),
    };
    let mut body: Json = serde_json::from_str(&body).expect("body");
    body["lseq"] = json!(n.to_string());
    body.to_string()
}

type Told = Arc<Mutex<Vec<(String, Json)>>>;

fn hear(run: &Run, relays: &Arc<Relays>) -> Told {
    let told: Told = Arc::default();
    let (heard, file, violations, held) =
        (Arc::clone(&told), run.told.clone(), run.violations.clone(), Arc::downgrade(relays));
    relays.subscribe(move |body: &str| {
        let body: Json = serde_json::from_str(body).expect("change");
        let id = body["subject"].as_str().expect("subject").to_string();
        let cursor: Cursor = serde_json::from_value(body["data"]["cursor"].clone()).expect("cursor");
        append(&file, &format!("{id} {}", body["data"]["cursor"]));
        let at = held.upgrade().and_then(|relays| relays.read(&id)).map(|set| set.cursor());
        if at.is_none_or(|at| at < cursor) {
            append(&violations, &format!("told cursor {cursor:?} for {id} while read is at {at:?}"));
        }
        heard.lock().expect("told").push((id, body["data"].clone()));
    });
    told
}

/// The last change told for a relay claims its set: what entered is in it, what left is not, and what
/// changed reads as told.
fn claims(run: &Run, relays: &Relays, id: &str, change: &Json) {
    let cursor: Cursor = serde_json::from_value(change["cursor"].clone()).expect("cursor");
    let Some(set) = relays.read(id).filter(|set| set.cursor() == cursor) else { return };
    let of = |item: &Json| -> NodeRef { serde_json::from_value(item["node"].clone()).expect("node") };
    let holds = |node: &NodeRef| set.nodes().contains_key(node);
    for item in change["entered"].as_array().into_iter().flatten() {
        if !holds(&of(item)) {
            run.violation(&format!("{id} told {} entered at {cursor:?}, not in the set", of(item).key));
        }
    }
    for item in change["left"].as_array().into_iter().flatten() {
        if holds(&of(item)) {
            run.violation(&format!("{id} told {} left at {cursor:?}, in the set", of(item).key));
        }
    }
    for item in change["changed"].as_array().into_iter().flatten() {
        let now = set.nodes().get(&of(item)).and_then(|version| {
            version.properties.get(item["property"].as_str().expect("property")).cloned()
        });
        if now.map(|value| serde_json::to_value(value).expect("value")) != Some(item["after"].clone()) {
            run.violation(&format!("{id} told {} changed at {cursor:?}, reads otherwise", of(item).key));
        }
    }
}

fn life(run: &Run, total: u64, stepped: bool) {
    let queue = Arc::new(Queue::open(open(&run.graph, &KEY).expect("open"), Queue::INTERVAL).expect("queue"));
    let relays = Arc::new(Relays::open(&run.relays, Arc::clone(&queue), &run.graph, &KEY).expect("relays"));
    let told = hear(run, &relays);
    let entries = manifest(&run.relays);
    let reader = open(&run.graph, &KEY).expect("reader");
    let start = queue.position("feed").expect("position").map_or(0, |p| p.parse::<u64>().expect("n") + 1);
    let mut last = None;
    for n in start..total {
        queue.accept_from(event(n, &mut last), "feed", n.to_string()).expect("accept");
        if stepped {
            if n % 5 == 0 {
                let Some(high) = settled(&queue, &relays, &reader, &entries) else {
                    run.violation("a relay did not reach the watermark");
                    continue;
                };
                for entry in &entries {
                    if let Some(why) = differs(&reader, &relays, entry) {
                        run.violation(&format!("step at {high:?}: {why}"));
                    }
                }
                let heard = std::mem::take(&mut *told.lock().expect("told"));
                let newest: BTreeMap<_, _> = heard.into_iter().collect();
                for (id, change) in &newest {
                    claims(run, &relays, id, change);
                }
            }
        } else if n % 10 == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    let failed = queue.flush().1;
    if !failed.is_empty() {
        run.violation(&format!("queue failures {failed:?}"));
    }
    for failure in relays.take_failures() {
        run.violation(&format!("relay failure {failure}"));
    }
    println!("life reached {total}");
    die(queue, Arc::into_inner(relays).unwrap_or_else(|| std::process::exit(0)));
}

/// A relay's file and worker: the worker is never ahead of the file, and the file never ahead of the
/// log.
fn files(run: &Run, queue: &Queue, entry: &Entry, committed: Cursor, when: &str) {
    let worker = queue.join(&format!("rannoch.relay.{}", entry.id));
    match file_cursor(&run.relays, &entry.id) {
        Some(file) => {
            if worker > file {
                run.violation(&format!("{when} {}: worker {worker:?} ahead of file {file:?}", entry.shape));
            }
            if file > committed {
                run.violation(&format!("{when} {}: file {file:?} ahead of log {committed:?}", entry.shape));
            }
        }
        None => run.violation(&format!("{when} {}: no file", entry.shape)),
    }
}

fn check(run: &Run, dir: &str) {
    let reader = open(&run.graph, &KEY).expect("reader");
    let committed = reader.events().committed().expect("committed");
    let told = std::fs::read_to_string(&run.told).unwrap_or_default();
    let mut count = 0;
    for line in told.lines() {
        let (id, cursor) = line.split_once(' ').expect("told line");
        let cursor: Cursor = serde_json::from_str(cursor).expect("told cursor");
        count += 1;
        if cursor > committed {
            run.violation(&format!("told {id} at {cursor:?}, past the log's {committed:?}"));
        }
    }
    let _ = std::fs::remove_file(&run.told);
    let queue = Arc::new(Queue::open(open(&run.graph, &KEY).expect("open"), Queue::INTERVAL).expect("queue"));
    let entries = manifest(&run.relays);
    let relays = Relays::open(&run.relays, Arc::clone(&queue), &run.graph, &KEY).expect("relays");
    for entry in &entries {
        files(run, &queue, entry, committed, "opened");
    }
    let Some(high) = settled(&queue, &relays, &reader, &entries) else {
        run.violation("after a restart a relay did not reach the watermark");
        die(queue, relays);
    };
    let mut held = 0;
    for entry in &entries {
        match differs(&reader, &relays, entry) {
            Some(why) => run.violation(&format!("restart at {high:?}: {why}")),
            None => held += 1,
        }
    }
    for failure in relays.take_failures() {
        run.violation(&format!("relay failure {failure}"));
    }
    let sizes: Vec<usize> =
        entries.iter().filter_map(|e| relays.read(&e.id)).map(|set| set.nodes().len()).collect();
    let position = reader.position("feed").expect("position");
    append(
        &Path::new(dir).join("checks.txt"),
        &format!("watermark {high:?} position {position:?} equal {held}/{} sizes {sizes:?} told {count}", entries.len()),
    );
    die(queue, relays);
}

fn verify(run: &Run, total: u64) {
    let g = open(&run.graph, &KEY).expect("open");
    let mut seen: BTreeMap<u64, u32> = BTreeMap::new();
    for event in g.events().since(Cursor::START).expect("events") {
        let body: Json = serde_json::from_str(&event.body).expect("json");
        if let Some(n) = body["lseq"].as_str() {
            *seen.entry(n.parse().expect("n")).or_default() += 1;
        }
    }
    let missing = (0..total).filter(|n| !seen.contains_key(n)).count();
    let doubled = seen.values().filter(|c| **c > 1).count();
    let stopped = g.stopped().expect("stopped");
    println!(
        "events {} missing {missing} doubled {doubled} position {:?} stopped {}",
        seen.len(),
        g.position("feed").expect("position"),
        stopped.len()
    );
    for (cursor, why) in &stopped {
        println!("  stopped {cursor:?}: {why}");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let run = Run::new(&args[2]);
    let total = || args[3].parse::<u64>().expect("total");
    match args[1].as_str() {
        "setup" => setup(&run),
        "life" => life(&run, total(), args.get(4).is_some_and(|a| a == "--step")),
        "check" => check(&run, &args[2]),
        "verify" => verify(&run, total()),
        _ => panic!("mode"),
    }
}
