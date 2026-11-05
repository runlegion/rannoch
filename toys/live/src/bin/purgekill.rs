//! Purge under kill -9. `setup` makes a store of targets of every kind, among fillers, with a relay
//! that holds them, and writes the plan. `life` purges what remains, with a producer writing, until
//! killed. `check` is every restart: it opens, finds which rules landed, holds each target to all
//! before or all after, scans the store's file, its write-ahead log and the relays' directory for what
//! left, compares the relay with a cold walk, and dies without a clean close. `last` runs after the
//! last kill: every identity and event id that left, and every body sent again meets the rule.
#[path = "../killcommon.rs"]
mod common;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::{KEY, amended, append, ask, declared, die, differs, fact, manifest, node, settled};
use rannoch_queue::Queue;
use rannoch_relay::Relays;
use rannoch_store::{
    Class, Cursor, Declaration, Error, Graph, NodeRef, Relation, Roots, Shape, Step, Target, open,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

const SHOP: &str = "rannoch:shop";
const KINDS: u64 = 7;

/// One target and what its purge must take.
#[derive(Serialize, Deserialize)]
struct Planned {
    k: u64,
    /// The bodies of the events that must leave.
    gone: Vec<String>,
    /// The ids of events that must stay.
    kept: Vec<String>,
    /// The texts that must leave with them, each starting `secret-`.
    secrets: Vec<String>,
    /// The book's key at the end, its content identity in hex.
    book: String,
    identity: String,
}

fn cloud_id(body: &str) -> String {
    let body: Json = serde_json::from_str(body).expect("body");
    body["id"].as_str().expect("id").to_string()
}

fn source(k: u64) -> String {
    format!("s{k}")
}

/// A text that leaves with target `k`. It ends in a letter: in an index a key is followed by a rowid,
/// whose byte can read as one more digit.
fn mark(what: &str, k: u64) -> String {
    format!("secret-{what}-{k:05}z")
}

fn rule(k: u64) -> String {
    format!("erase-{k}")
}

impl Planned {
    fn target(&self) -> Target {
        let k = self.k;
        let source = source(k);
        let author = format!("keep-author-{k}");
        match k % KINDS {
            0 => Target::Node(NodeRef::new("book", &self.book)),
            1 => Target::Stream { source },
            2 => Target::Source { source },
            3 => {
                let hash = blake3::Hash::from_hex(&self.identity).expect("identity");
                Target::Fact { identity: *hash.as_bytes() }
            }
            4 => Target::NodeWord { node: NodeRef::new("book", &self.book), source },
            5 => Target::Link { relation: "wrote".to_string(), from: author, to: self.book.clone() },
            _ => Target::LinkWord {
                relation: "shelved".to_string(),
                from: self.book.clone(),
                to: "hub".to_string(),
                source,
            },
        }
    }

    /// What the store's reads say of the target now, each true before the purge and false after.
    fn reads(&self, g: &Graph) -> Vec<bool> {
        let k = self.k;
        let book = g.find("book", &self.book).expect("find").is_some();
        let chapter = g.find("chapter", &mark("ckey", k)).expect("find").is_some();
        let hearings = |relation: &str, from: &str, to: &str| {
            !g.link_hearings(relation, from, to).expect("hearings").is_empty()
        };
        let author = format!("keep-author-{k}");
        match k % KINDS {
            0..=2 => vec![
                book,
                chapter,
                hearings("wrote", &author, &self.book),
                hearings("shelved", &self.book, "hub"),
            ],
            3 | 4 => vec![book],
            5 => vec![hearings("wrote", &author, &self.book)],
            _ => vec![hearings("shelved", &self.book, "hub")],
        }
    }
}

struct Run {
    dir: PathBuf,
    graph: PathBuf,
    relays: PathBuf,
}

impl Run {
    fn new(dir: &str) -> Run {
        let dir = PathBuf::from(dir);
        Run { graph: dir.join("p.rannoch"), relays: dir.join("relays"), dir }
    }

    fn violation(&self, what: &str) {
        append(&self.dir.join("violations.txt"), what);
    }

    fn plan(&self) -> Vec<Planned> {
        let text = std::fs::read_to_string(self.dir.join("plan.jsonl")).expect("plan");
        text.lines().map(|line| serde_json::from_str(line).expect("planned")).collect()
    }

    fn journal(&self) -> PathBuf {
        self.dir.join("journal.txt")
    }
}

fn land(g: &mut Graph, bodies: &[String]) {
    let bodies: Vec<&str> = bodies.iter().map(String::as_str).collect();
    for landed in g.commit(&bodies, &[]).expect("commit") {
        landed.expect("land");
    }
}

fn setup(run: &Run, targets: u64, fillers: u64) {
    let mut g = open(&run.graph, &KEY).expect("open");
    for class in ["book", "chapter", "author", "hub", "filler"] {
        g.declare(&Declaration::Class(Class::new(class, ["title"])), &declared(class)).expect("class");
    }
    for relation in [
        Relation::new("wrote", "author", "book", false),
        Relation::new("in", "chapter", "book", true),
        Relation::new("shelved", "book", "hub", false),
    ] {
        let name = relation.name.clone();
        g.declare(&Declaration::Relation(relation), &declared(&name)).expect("relation");
    }
    let shape = Shape {
        name: "shelf".into(),
        roots: Roots::Node { node: NodeRef::new("hub", "hub") },
        steps: vec![
            Step { relation: "shelved<-".into(), ..Step::default() },
            Step { relation: "in<-".into(), ..Step::default() },
        ],
    };
    g.declare(&Declaration::Shape(shape), &declared("shelf")).expect("shape");
    land(&mut g, &[node(SHOP, "hub", "hub", &json!({"title": "the hub"}))]);
    let mut plan = Vec::new();
    for k in 0..targets {
        let from = format!("rannoch:{}", source(k));
        let (key, chapter, author) = (mark("key", k), mark("ckey", k), format!("keep-author-{k}"));
        let book = node(&from, "book", &key, &json!({"title": mark("title", k)}));
        let part = node(&from, "chapter", &chapter, &json!({"title": mark("ctitle", k)}));
        let contained = fact(&from, "link", "in", ("chapter", &chapter), ("book", &key));
        let wrote = fact(&from, "link", "wrote", ("author", &author), ("book", &key));
        let shelved = fact(&from, "link", "shelved", ("book", &key), ("hub", "hub"));
        let patch = amended(&from, &book, &json!({"data": {"properties": {"title": mark("patch", k)}}}));
        let renames = k % 14 == 0;
        let renamed = mark("renamed", k);
        let moved = amended(&from, &book, &json!({"subject": renamed}));
        let kept = vec![
            node(SHOP, "author", &author, &json!({"title": format!("kept-author-{k}")})),
            node(SHOP, "book", &format!("keep-book-{k}"), &json!({"title": format!("kept-title-{k}")})),
            fact(SHOP, "link", "shelved", ("book", &format!("keep-book-{k}")), ("hub", "hub")),
        ];
        let mut bodies = vec![book.clone(), part.clone(), contained.clone(), wrote.clone(), shelved.clone(), patch.clone()];
        if renames {
            bodies.push(moved.clone());
        }
        bodies.extend(kept.iter().cloned());
        land(&mut g, &bodies);
        g.fill().expect("fill");
        let mut named: Vec<String> = ["title", "patch"].map(|what| mark(what, k)).to_vec();
        let gone = match k % KINDS {
            0..=2 => {
                named.extend(["ctitle", "key", "ckey"].map(|what| mark(what, k)));
                let mut all = vec![book.clone(), part, contained, wrote, shelved, patch];
                if renames {
                    named.push(renamed.clone());
                    all.push(moved);
                }
                all
            }
            3 | 4 => vec![book.clone(), patch],
            5 => {
                named.clear();
                vec![wrote]
            }
            _ => {
                named.clear();
                vec![shelved]
            }
        };
        let current = if renames { renamed } else { key };
        let identity = g.hearings(&NodeRef::new("book", &current)).expect("hearings")[0].identity;
        plan.push(Planned {
            k,
            gone,
            kept: kept.iter().map(|body| cloud_id(body)).collect(),
            secrets: named,
            book: current,
            identity: blake3::Hash::from_bytes(identity).to_hex().to_string(),
        });
    }
    let fill: Vec<String> =
        (0..fillers).map(|n| node(SHOP, "filler", &format!("f{n}"), &json!({"title": format!("filler {n}")}))).collect();
    for chunk in fill.chunks(2000) {
        land(&mut g, chunk);
        g.fill().expect("fill");
    }
    for planned in &plan {
        append(&run.dir.join("plan.jsonl"), &serde_json::to_string(planned).expect("plan"));
    }
    let queue = Arc::new(Queue::open(g, Queue::INTERVAL).expect("queue"));
    let relays = Relays::open(&run.relays, Arc::clone(&queue), &run.graph, &KEY).expect("relays");
    ask(&queue, &run.relays, &[("shelf", json!({}))]);
    assert!(relays.close().is_empty());
    assert!(queue.flush().1.is_empty());
    println!("planned {targets} targets, {fillers} fillers");
}

/// The rules in the log.
fn landed(g: &Graph) -> BTreeMap<String, u32> {
    let mut rules = BTreeMap::new();
    for event in g.events().since(Cursor::START).expect("events") {
        if event.body.contains("rannoch.purged") {
            let body: Json = serde_json::from_str(&event.body).expect("purge");
            *rules.entry(body["data"]["rule"].as_str().expect("rule").to_string()).or_insert(0) += 1;
        }
    }
    rules
}

fn life(run: &Run) {
    let queue = Arc::new(Queue::open(open(&run.graph, &KEY).expect("open"), Queue::INTERVAL).expect("queue"));
    let relays = Relays::open(&run.relays, Arc::clone(&queue), &run.graph, &KEY).expect("relays");
    let producer = Arc::clone(&queue);
    std::thread::spawn(move || {
        let mut n = producer.position("feed").expect("position").map_or(0, |p| p.parse::<u64>().expect("n") + 1);
        loop {
            let body = node(SHOP, "filler", &format!("p{n}"), &json!({"title": format!("fed {n}")}));
            if producer.accept_from(body, "feed", n.to_string()).is_err() {
                return;
            }
            n += 1;
            std::thread::sleep(Duration::from_millis(2));
        }
    });
    let remaining = std::fs::read_to_string(run.dir.join("remaining.txt")).unwrap_or_default();
    let remaining: HashSet<u64> = remaining.lines().map(|k| k.parse().expect("k")).collect();
    let plan = run.plan();
    for planned in plan.iter().filter(|planned| remaining.contains(&planned.k)) {
        let k = planned.k;
        append(&run.journal(), &format!("start {k}"));
        match queue.purge(&planned.target(), &rule(k), "privacy") {
            Ok(_) => {}
            // It committed, and a reader kept the write-ahead log from truncating.
            Err(Error::Refused(why)) => {
                append(&run.dir.join("refused.txt"), &format!("{k} {why}"));
            }
            Err(error) => run.violation(&format!("purge {k}: {error}")),
        }
        append(&run.journal(), &format!("done {k}"));
    }
    append(&run.dir.join("all-landed.txt"), "yes");
    die(queue, relays);
}

/// What a kill between a purge's commit and its truncate leaves, on purpose: a reader holds the log
/// through the purge, and the process dies with it unfinished. `check` then says what the next open
/// did about it. Random kills rarely land in so small a window.
fn window(run: &Run, count: usize) {
    let mut g = open(&run.graph, &KEY).expect("open");
    let rules = landed(&g);
    let plan = run.plan();
    let reader = rusqlite::Connection::open(&run.graph).expect("reader");
    reader.execute_batch("BEGIN; SELECT count(*) FROM _events;").expect("read");
    for planned in plan.iter().filter(|planned| !rules.contains_key(&rule(planned.k)) && planned.k % KINDS < 5).take(count) {
        match g.purge(&planned.target(), &rule(planned.k), "privacy") {
            Err(Error::Refused(_)) => append(&run.dir.join("window.txt"), &format!("{} committed, not truncated", planned.k)),
            other => run.violation(&format!("window {}: {other:?}", planned.k)),
        }
    }
    std::process::abort();
}

/// Every occurrence of `secret-<what>-<k>z` in `bytes`.
fn secrets_in(bytes: &[u8], found: &mut BTreeSet<String>) {
    const MARK: &[u8] = b"secret-";
    let mut at = 0;
    while let Some(next) = bytes[at..].windows(MARK.len()).position(|window| window == MARK) {
        let start = at + next;
        let end = bytes[start..]
            .iter()
            .position(|b| !(b.is_ascii_alphanumeric() || *b == b'-'))
            .map_or(bytes.len(), |len| start + len);
        found.insert(String::from_utf8_lossy(&bytes[start..end]).into_owned());
        at = start + MARK.len();
    }
}

/// The store's file, its write-ahead log, and every file in the relays' directory, each with its name.
fn files(run: &Run) -> Vec<(String, Vec<u8>)> {
    let mut paths: Vec<PathBuf> = ["", "-wal"]
        .iter()
        .map(|suffix| PathBuf::from(format!("{}{suffix}", run.graph.display())))
        .collect();
    paths.extend(std::fs::read_dir(&run.relays).expect("relays").map(|entry| entry.expect("entry").path()));
    paths
        .into_iter()
        .filter_map(|path| Some((path.file_name()?.to_string_lossy().into_owned(), std::fs::read(&path).ok()?)))
        .collect()
}

/// Whether a text that left is in any file, for the targets whose rule landed.
fn scan(run: &Run, plan: &[Planned], rules: &BTreeMap<String, u32>, when: &str) -> usize {
    let gone: HashSet<&str> = plan
        .iter()
        .filter(|planned| rules.contains_key(&rule(planned.k)))
        .flat_map(|planned| planned.secrets.iter().map(String::as_str))
        .collect();
    let mut held = 0;
    for (name, bytes) in files(run) {
        let mut found = BTreeSet::new();
        secrets_in(&bytes, &mut found);
        let left: Vec<&String> = found.iter().filter(|text| gone.contains(text.as_str())).collect();
        if !left.is_empty() {
            held += left.len();
            let name = if name.ends_with(".json") && name != "relays.json" { "a relay's file" } else { &name };
            run.violation(&format!("{when}: {name} holds {} that left, such as {}", left.len(), left[0]));
        }
    }
    held
}

/// Each target is wholly before or wholly after, and after exactly when its rule landed.
fn atomic(run: &Run, g: &Graph, plan: &[Planned], rules: &BTreeMap<String, u32>) {
    let logged: HashSet<String> = g
        .events()
        .since(Cursor::START)
        .expect("events")
        .iter()
        .filter_map(|event| serde_json::from_str::<Json>(&event.body).ok()?["id"].as_str().map(String::from))
        .collect();
    for planned in plan {
        let k = planned.k;
        let rule = rule(k);
        if rules.get(&rule).is_some_and(|count| *count > 1) {
            run.violation(&format!("rule {rule} landed {} times", rules[&rule]));
        }
        let after = rules.contains_key(&rule);
        let events: Vec<bool> = planned.gone.iter().map(|body| logged.contains(&cloud_id(body))).collect();
        let reads = planned.reads(g);
        let parts = events.iter().chain(&reads);
        if parts.clone().any(|present| *present == after) {
            run.violation(&format!("target {k} kind {}: rule landed {after}, events {events:?}, reads {reads:?}", k % KINDS));
        }
        if !planned.kept.iter().all(|id| logged.contains(id)) {
            run.violation(&format!("target {k}: an event that stays has gone"));
        }
    }
}

fn check(run: &Run) {
    let plan = run.plan();
    let journal = std::fs::read_to_string(run.journal()).unwrap_or_default();
    let queue = Arc::new(Queue::open(open(&run.graph, &KEY).expect("open"), Queue::INTERVAL).expect("queue"));
    let entries = manifest(&run.relays);
    let relays = Relays::open(&run.relays, Arc::clone(&queue), &run.graph, &KEY).expect("relays");
    let reader = open(&run.graph, &KEY).expect("reader");
    let rules = landed(&reader);
    // What the last life was doing when it died, for the count of kills that landed in a purge.
    let inside = journal.lines().last().and_then(|line| line.strip_prefix("start ")).map(|k| k.parse::<u64>().expect("k"));
    let held = scan(run, &plan, &rules, "restart");
    atomic(run, &reader, &plan, &rules);
    let settle = settled(&queue, &relays, &reader, &entries);
    if settle.is_none() {
        run.violation("after a restart the relay did not reach the watermark");
    }
    let equal = entries.iter().filter(|entry| match differs(&reader, &relays, entry) {
        Some(why) => {
            run.violation(&format!("restart: {why}"));
            false
        }
        None => true,
    });
    let equal = equal.count();
    let held = held + scan(run, &plan, &rules, "restart, relay settled");
    let wal = std::fs::metadata(format!("{}-wal", run.graph.display())).map_or(0, |m| m.len());
    let what = match inside {
        Some(k) if rules.contains_key(&rule(k)) => format!("inside {k}, landed"),
        Some(k) => format!("inside {k}, not landed"),
        None => "between".to_string(),
    };
    append(
        &run.dir.join("checks.txt"),
        &format!("{what} | rules {} of {} | held {held} | relay equal {equal}/{} | wal {wal}", rules.len(), plan.len(), entries.len()),
    );
    let remaining: Vec<String> =
        plan.iter().filter(|planned| !rules.contains_key(&rule(planned.k))).map(|planned| planned.k.to_string()).collect();
    std::fs::write(run.dir.join("remaining.txt"), remaining.join("\n")).expect("remaining");
    die(queue, relays);
}

/// Whether any of `needles` is in `bytes`.
fn holding(bytes: &[u8], needles: &HashSet<Vec<u8>>, len: usize) -> usize {
    if bytes.len() < len {
        return 0;
    }
    bytes.windows(len).filter(|window| needles.contains(*window)).count()
}

/// After the last kill, still with no clean close: every id and identity that left is in no file;
/// each body sent again is refused and nothing is stored. Then a clean close, and the scan again.
fn last(run: &Run) {
    let plan = run.plan();
    let mut ids: HashSet<Vec<u8>> = HashSet::new();
    let mut identities: HashSet<Vec<u8>> = HashSet::new();
    for planned in &plan {
        ids.extend(planned.gone.iter().map(|body| cloud_id(body).into_bytes()));
        if planned.k % KINDS < 5 {
            identities.insert(blake3::Hash::from_hex(&planned.identity).expect("hex").as_bytes().to_vec());
        }
    }
    let id_len = ids.iter().next().expect("ids").len();
    let mut g = open(&run.graph, &KEY).expect("open");
    let rules = landed(&g);
    let all = plan.iter().all(|planned| rules.contains_key(&rule(planned.k)));
    println!("rules landed {} of {}, all {all}", rules.len(), plan.len());
    let holds = |run: &Run, when: &str| {
        for (name, bytes) in files(run) {
            let (a, b) = (holding(&bytes, &ids, id_len), holding(&bytes, &identities, 32));
            if a + b > 0 {
                run.violation(&format!("{when}: {name} holds {a} event ids and {b} identities that left"));
            }
        }
        println!("{when}: scanned {} texts", scan(run, &plan, &rules, when));
    };
    holds(run, "last kill, no clean close");
    atomic(run, &g, &plan, &rules);
    let before = g.events().committed().expect("committed");
    let (mut refused, mut stored) = (0, 0);
    for planned in &plan {
        for body in &planned.gone {
            match &g.commit(&[body], &[]).expect("commit")[0] {
                Err(Error::Purged { .. }) => refused += 1,
                Ok(_) => {
                    stored += 1;
                    run.violation(&format!("target {} sent again: stored, no rule refused it", planned.k));
                }
                Err(error) => run.violation(&format!("target {} sent again: {error}", planned.k)),
            }
        }
    }
    g.fill().expect("fill");
    println!(
        "sent again: refused {refused}, stored {stored}; log {before:?} to {:?}; stopped {}",
        g.events().committed().expect("committed"),
        g.stopped().expect("stopped").len()
    );
    atomic(run, &g, &plan, &rules);
    drop(g);
    let queue = Arc::new(Queue::open(open(&run.graph, &KEY).expect("open"), Queue::INTERVAL).expect("queue"));
    let entries = manifest(&run.relays);
    let relays = Relays::open(&run.relays, Arc::clone(&queue), &run.graph, &KEY).expect("relays");
    let reader = open(&run.graph, &KEY).expect("reader");
    settled(&queue, &relays, &reader, &entries);
    for entry in &entries {
        match differs(&reader, &relays, entry) {
            Some(why) => run.violation(&format!("last: {why}")),
            None => println!("relay {} equal, {} nodes", entry.shape, relays.read(&entry.id).expect("set").nodes().len()),
        }
    }
    drop(reader);
    assert!(relays.close().is_empty());
    let queue = Arc::into_inner(queue).expect("the last handle");
    assert!(queue.close().is_empty());
    holds(run, "clean close");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let run = Run::new(&args[2]);
    match args[1].as_str() {
        "setup" => setup(&run, args[3].parse().expect("targets"), args[4].parse().expect("fillers")),
        "life" => life(&run),
        "check" => check(&run),
        "window" => window(&run, args[3].parse().expect("count")),
        "last" => last(&run),
        _ => panic!("mode"),
    }
}
