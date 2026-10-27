//! Runs every `.feature` file under `tests/`. Each scenario is one acceptance criterion of a
//! legion requirement; a scenario with no matching step code fails, naming the scenario.
//!
//! Steps act and observe through rannoch's public API. Only fixture setup touches the file
//! directly: the triggers that make a write's third piece, or one write of a heartbeat's batch,
//! fail, and reading the file's table and column names so an answer or a refusal can be checked
//! against them.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::needless_pass_by_value
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use rannoch::{At, End, Failure, Hearing, HistoryKey, NodeFact, Queue, Ticket, ValueType, Version};
use rannoch::{
    Change, Class, Count, Cursor, Date, Declaration, Description, Error, Event, EventId, Fact,
    FactRef, Fate, Graph, GroupBy, Link, LinkFact, Node, NodeRef, OpenLink, PartOf, Purge,
    Relation, Snapshot, Step, Tally, Target, Unit, ValidTime, Value, Write, WriteError,
};
use rstest_bdd_macros::{given, scenarios, then, when};
use tempfile::TempDir;

const NONE: [(&str, &str); 0] = [];

/// The content-identity key the scenarios open their graphs under.
const KEY: [u8; 32] = *b"rannoch scenarios' identity key!";

/// The piece size these fixtures are shaped to: rannoch's own.
const PIECE_CHANGES: usize = 1_000;

// ---------- fixtures ----------

/// A text value's text.
fn text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// The T9b declarations (t9b-direct, 2026-10-14): 5 classes and 4 relations.
fn t9b() -> Vec<Declaration> {
    let class = |name: &str, properties: &[&str]| {
        Declaration::Class(Class::new(name, properties.iter().copied()))
    };
    let relation = |name: &str, from: &str, to: &str, containment: bool| {
        Declaration::Relation(Relation::new(name, from, to, containment))
    };
    vec![
        class("place", &["url"]),
        class("conversation", &[]),
        class("voice", &[]),
        class("statement", &["text"]),
        class("finding", &[]),
        relation("at", "conversation", "place", true),
        relation("inside", "statement", "conversation", true),
        relation("said", "voice", "statement", false),
        relation("supports", "statement", "finding", false),
    ]
}

fn day(year: u16, month: u8, d: u8) -> ValidTime {
    ValidTime::On(Date::day(year, month, d))
}

/// A reply stream in T5's shape: T5's per-voice statement histogram, statements in a seeded
/// order, 60% replying to one of the 20 before, arriving shuffled within windows of 200.
struct Stream {
    /// (statement, voice, parent) in arrival order.
    arrivals: Vec<(String, String, Option<String>)>,
    /// The reply links the source gave: (reply, parent).
    links: BTreeSet<(String, String)>,
}

fn histogram() -> Vec<usize> {
    include_str!("fixtures/histogram-per-voice.txt")
        .lines()
        .map(|line| line.trim().parse().unwrap())
        .collect()
}

fn reply_stream() -> Stream {
    let mut rng = fastrand::Rng::with_seed(13);
    let mut voices: Vec<usize> = histogram()
        .iter()
        .enumerate()
        .flat_map(|(voice, &n)| std::iter::repeat_n(voice, n))
        .collect();
    rng.shuffle(&mut voices);
    let mut arrivals: Vec<(String, String, Option<String>)> = Vec::new();
    let mut links = BTreeSet::new();
    for (i, voice) in voices.iter().enumerate() {
        let statement = format!("m{i}");
        let parent =
            (i > 0 && rng.f64() < 0.6).then(|| format!("m{}", i - 1 - rng.usize(..20.min(i))));
        if let Some(parent) = &parent {
            links.insert((statement.clone(), parent.clone()));
        }
        arrivals.push((statement, format!("voice{voice}"), parent));
    }
    for window in arrivals.chunks_mut(200) {
        rng.shuffle(window);
    }
    Stream { arrivals, links }
}

/// The stream as one write: per statement, its voice's unit and then its own unit with its
/// said and answers links.
fn stream_write(arrivals: &[(String, String, Option<String>)]) -> Write {
    let when = day(2021, 3, 2);
    let mut write = Write::new("discord");
    for (statement, voice, parent) in arrivals {
        write = write.unit(Unit::new("voice", voice.clone(), NONE, when.clone()));
        let mut unit = Unit::new(
            "statement",
            statement.clone(),
            [("text", "words")],
            when.clone(),
        )
        .link("said", voice.clone(), statement.clone(), when.clone());
        if let Some(parent) = parent {
            unit = unit.link("answers", statement.clone(), parent.clone(), when.clone());
        }
        write = write.unit(unit);
    }
    write
}

fn declare_replies(g: &mut Graph) {
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    g.declare(&Declaration::Relation(Relation::new(
        "answers",
        "statement",
        "statement",
        false,
    )))
    .unwrap();
}

/// The T9b declarations and instances (t9b-direct, 2026-10-14): place swtor-tc, conversation c1
/// at it, finding f1, voices v0 to v2, and statements s0 to s4, each inside c1, said by a voice
/// and supporting f1.
fn t9b_graph(g: &mut Graph) {
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    let when = day(2021, 3, 2);
    let mut write = Write::new("swtor")
        .unit(Unit::new(
            "place",
            "swtor-tc",
            [("url", "discord.gg/x")],
            day(2019, 10, 1),
        ))
        .unit(Unit::new("conversation", "c1", NONE, when.clone()).link(
            "at",
            "c1",
            "swtor-tc",
            when.clone(),
        ))
        .unit(Unit::new("finding", "f1", NONE, day(2026, 10, 1)));
    for i in 0..5 {
        let (statement, voice) = (format!("s{i}"), format!("v{}", i % 3));
        write = write
            .unit(Unit::new("voice", voice.clone(), NONE, when.clone()))
            .unit(
                Unit::new(
                    "statement",
                    statement.clone(),
                    [("text", "the respec cost is absurd")],
                    when.clone(),
                )
                .link("inside", statement.clone(), "c1", when.clone())
                .link("said", voice, statement.clone(), when.clone())
                .link("supports", statement, "f1", day(2026, 10, 1)),
            );
    }
    g.write(&write).unwrap();
    assert!(g.open_links().unwrap().is_empty());
}

/// T4's source: per finding, its distinct voices, statements and per-place counts.
#[derive(Default)]
struct FindingSource {
    voices: HashSet<usize>,
    statements: u64,
    places: BTreeMap<String, (HashSet<usize>, u64)>,
}

const PLACES: [&str; 5] = ["discord", "reddit", "forums", "youtube", "steam"];

/// T4's size: 100,000 voices with T4's per-voice statement histogram, 1,000 findings, 5 places
/// (a statement's property). Seeded as T4 was. Returns the graph and its source's counts.
fn t4(world: &mut World) -> HashMap<String, FindingSource> {
    let histogram = histogram();
    let mut rng = fastrand::Rng::with_seed(4);
    let when = day(2026, 10, 14);
    let mut source: HashMap<String, FindingSource> = HashMap::new();
    let g = world.open();
    for d in [
        Declaration::Class(Class::new("voice", [] as [&str; 0])),
        Declaration::Class(Class::new("statement", ["place"])),
        Declaration::Class(Class::new("finding", [] as [&str; 0])),
        Declaration::Relation(Relation::new("said", "voice", "statement", false)),
        Declaration::Relation(Relation::new("supports", "statement", "finding", false)),
    ] {
        g.declare(&d).unwrap();
    }
    let mut write = Write::new("t4");
    for f in 0..1000 {
        write = write.unit(Unit::new("finding", format!("f{f}"), NONE, when.clone()));
    }
    g.write(&write).unwrap();
    for batch in 0..100 {
        let mut write = Write::new("t4");
        for v in batch * 1000..(batch + 1) * 1000 {
            let voice = format!("v{v}");
            write = write.unit(Unit::new("voice", voice.clone(), NONE, when.clone()));
            for k in 0..histogram[rng.usize(..histogram.len())] {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let supports = (rng.f64() < 0.3)
                    .then(|| format!("f{}", (1000.0 * rng.f64().powi(2)) as usize));
                let place = PLACES[rng.usize(..PLACES.len())];
                let statement = format!("v{v}s{k}");
                let mut unit = Unit::new(
                    "statement",
                    statement.clone(),
                    [("place", place)],
                    when.clone(),
                )
                .link("said", voice.clone(), statement.clone(), when.clone());
                if let Some(finding) = supports {
                    unit = unit.link("supports", statement, finding.clone(), when.clone());
                    let counts = source.entry(finding).or_default();
                    counts.voices.insert(v);
                    counts.statements += 1;
                    let at = counts.places.entry(place.to_string()).or_default();
                    at.0.insert(v);
                    at.1 += 1;
                }
                write = write.unit(unit);
            }
        }
        g.write(&write).unwrap();
    }
    source
}

/// T2's size (t6-t2-refs-replay): 20 findings, 500 voices, 5,000 statements, each unit its own
/// write. As in T2, each voice holds content: a name, "voice <n>". Returns the events the
/// declaration of that name and the writes returned, in order.
fn t2(g: &mut Graph) -> Vec<Event> {
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    let mut written: Vec<Event> = g
        .declare(&Declaration::Property {
            class: "voice".into(),
            property: "name".into(),
            of: ValueType::Text,
        })
        .unwrap()
        .into_iter()
        .collect();
    let when = day(2026, 10, 1);
    let mut put = |g: &mut Graph, unit: Unit| {
        written.extend(g.write(&Write::new("eavesdrop").unit(unit)).unwrap());
    };
    for f in 0..20 {
        put(g, Unit::new("finding", format!("f{f}"), NONE, when.clone()));
    }
    for v in 0..500 {
        put(
            g,
            Unit::new(
                "voice",
                format!("v{v}"),
                [("name", format!("voice {v}"))],
                when.clone(),
            ),
        );
    }
    for s in 0..5000 {
        let (voice, finding, statement) = (
            format!("v{}", (s * 7) % 500),
            format!("f{}", s % 20),
            format!("st{s}"),
        );
        put(
            g,
            Unit::new(
                "statement",
                statement.clone(),
                [("text", format!("words of statement {s}"))],
                when.clone(),
            )
            .link("said", voice, statement.clone(), when.clone())
            .link("supports", statement, finding, when.clone()),
        );
    }
    written
}

/// T10's synthetic set (t10-as-of): 1 finding, 40 supporting statements from 12 voices with
/// valid times from 2023 to 2026, 3 of them stated unknown; s05 valid 2024-05-01. Per
/// statement: its key, its voice and its valid time.
fn t10_statements() -> Vec<(String, String, ValidTime)> {
    (0u16..40)
        .map(|i| {
            let valid = if i == 5 {
                day(2024, 5, 1)
            } else if i % 13 == 7 {
                ValidTime::Unknown
            } else {
                let small = |n: u16| u8::try_from(n).unwrap();
                day(
                    2023 + i * 7 % 4,
                    small(1 + i * 5 % 12),
                    small(1 + i * 3 % 28),
                )
            };
            (format!("s{i:02}"), format!("v{}", i % 12), valid)
        })
        .collect()
}

/// T10 written from swtor. The finding and the voices are valid from 2023-01-01, the earliest
/// statement's date, so a read as of a date is decided by the statements and their links.
fn t10(g: &mut Graph) {
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    let since = day(2023, 1, 1);
    let mut write = Write::new("swtor").unit(Unit::new("finding", "f1", NONE, since.clone()));
    for v in 0..12 {
        write = write.unit(Unit::new("voice", format!("v{v}"), NONE, since.clone()));
    }
    for (statement, voice, valid) in t10_statements() {
        write = write.unit(
            Unit::new(
                "statement",
                statement.clone(),
                [("text", format!("text of {statement}"))],
                valid.clone(),
            )
            .link("said", voice, statement.clone(), valid.clone())
            .link("supports", statement, "f1", valid),
        );
    }
    g.write(&write).unwrap();
}

/// FR-RANNOCH-048's s41 (2026-10-25), from v12, a voice no other T10 statement has: a version
/// valid 2024-03-01 and a later one valid 2025-06-01.
const S41: [(u16, u8, u8); 2] = [(2024, 3, 1), (2025, 6, 1)];

/// T10 with s41 supporting the finding, by links valid from its first version's date, its
/// later version written after.
fn t10_s41(g: &mut Graph) {
    t10(g);
    let [first, later] = S41.map(|(y, m, d)| day(y, m, d));
    g.write(
        &Write::new("swtor")
            .unit(Unit::new("voice", "v12", NONE, day(2023, 1, 1)))
            .unit(
                Unit::new("statement", "s41", [("text", "text of s41")], first.clone())
                    .link("said", "v12", "s41", first.clone())
                    .link("supports", "s41", "f1", first),
            ),
    )
    .unwrap();
    let restated = Unit::new(
        "statement",
        "s41",
        [("text", "text of s41, restated")],
        later,
    );
    g.write(&Write::new("swtor").unit(restated)).unwrap();
}

/// The statements of T10 and s41 the fixture marks valid on or before `as_of`, with their
/// voices: s41 from its first version's date.
fn t10_valid_by(as_of: &Date) -> Vec<(String, String)> {
    let (y, m, d) = S41[0];
    let s41 = (
        ValidTime::On(Date::day(y, m, d)),
        "s41".into(),
        "v12".into(),
    );
    t10_statements()
        .into_iter()
        .map(|(statement, voice, valid)| (valid, statement, voice))
        .chain([s41])
        .filter(|(valid, _, _)| matches!(valid, ValidTime::On(date) if date <= as_of))
        .map(|(_, statement, voice)| (statement, voice))
        .collect()
}

/// The T10 statements whose valid time is stated unknown.
fn t10_unknown() -> Vec<String> {
    t10_statements()
        .into_iter()
        .filter(|(_, _, valid)| *valid == ValidTime::Unknown)
        .map(|(statement, _, _)| statement)
        .collect()
}

/// The rewrite of s05 (T10): corrected text, still true from 2024-05-01.
fn rewrite_s05(g: &mut Graph) {
    g.write(&Write::new("swtor").unit(Unit::new(
        "statement",
        "s05",
        [("text", "text of s05, corrected")],
        day(2024, 5, 1),
    )))
    .unwrap();
}

/// The nodes and links a fixture wrote: nodes by class and key, links by relation and keys.
#[derive(Default)]
struct Instances {
    nodes: Vec<NodeRef>,
    links: Vec<(String, String, String)>,
}

/// The T9b instances (t9b-direct, 2026-10-14) on its declarations: 27 units, each its own
/// write and so its own event, every link between nodes already present so none waits.
fn t9b_instances(g: &mut Graph) -> Instances {
    let mut instances = Instances::default();
    let mut put =
        |g: &mut Graph, unit: Unit, node: (&str, String), links: Vec<(&str, String, String)>| {
            let written = g.write(&Write::new("t9b").unit(unit)).unwrap();
            assert_eq!(written.len(), 1);
            instances.nodes.push(NodeRef::new(node.0, node.1));
            instances.links.extend(
                links
                    .into_iter()
                    .map(|(relation, from, to)| (relation.to_string(), from, to)),
            );
        };
    let when = day(2026, 10, 14);
    for p in 0..2 {
        let key = format!("place{p}");
        let url = format!("https://example.org/{p}");
        put(
            g,
            Unit::new("place", key.clone(), [("url", url)], when.clone()),
            ("place", key),
            vec![],
        );
    }
    for c in 0..3 {
        let (key, place) = (format!("conv{c}"), format!("place{}", c % 2));
        put(
            g,
            Unit::new("conversation", key.clone(), NONE, when.clone()).link(
                "at",
                key.clone(),
                place.clone(),
                when.clone(),
            ),
            ("conversation", key.clone()),
            vec![("at", key, place)],
        );
    }
    for v in 0..4 {
        let key = format!("voice{v}");
        put(
            g,
            Unit::new("voice", key.clone(), NONE, ValidTime::Unknown),
            ("voice", key),
            vec![],
        );
    }
    for f in 0..3 {
        let key = format!("finding{f}");
        put(
            g,
            Unit::new("finding", key.clone(), NONE, when.clone()),
            ("finding", key),
            vec![],
        );
    }
    for s in 0..15 {
        let (key, voice, conv, finding) = (
            format!("st{s}"),
            format!("voice{}", s % 4),
            format!("conv{}", s % 3),
            format!("finding{}", s % 3),
        );
        put(
            g,
            Unit::new(
                "statement",
                key.clone(),
                [("text", format!("words {s}"))],
                day(2025, 1, 1 + s),
            )
            .link("said", voice.clone(), key.clone(), when.clone())
            .link("inside", key.clone(), conv.clone(), when.clone())
            .link("supports", key.clone(), finding.clone(), ValidTime::Unknown),
            ("statement", key.clone()),
            vec![
                ("said", voice, key.clone()),
                ("inside", key.clone(), conv),
                ("supports", key, finding),
            ],
        );
    }
    instances
}

/// A node without the time rannoch recorded it, which no event carries.
fn node_facts(node: Node) -> (String, String, BTreeMap<String, Value>, String, ValidTime) {
    (
        node.class,
        node.key,
        node.properties,
        node.source,
        node.valid,
    )
}

/// A link without the time rannoch recorded it, which no event carries.
fn link_facts(link: Link) -> (String, NodeRef, NodeRef, String, ValidTime) {
    (link.relation, link.from, link.to, link.source, link.valid)
}

// ---------- the world ----------

/// What a subscriber was given, and what another handle read up to it as it was given.
type Heard = (Cursor, Vec<Event>);

struct World {
    dir: TempDir,
    graph: Option<Graph>,
    cursor: Cursor,
    events_before: usize,
    declared: Vec<Result<Option<Event>, String>>,
    written: Vec<Event>,
    write_error: Option<WriteError>,
    stream: Option<Stream>,
    walked: Vec<(NodeRef, ValidTime)>,
    walks: Vec<(Date, Vec<(NodeRef, ValidTime)>)>,
    tallies: Vec<Tally>,
    found: Option<Node>,
    window: (SystemTime, SystemTime),
    source: HashMap<String, FindingSource>,
    largest: String,
    burst: Option<JoinHandle<()>>,
    burst_done: Arc<AtomicBool>,
    reads: (usize, usize, usize),
    log: Vec<Event>,
    read: Vec<Event>,
    folded: BTreeMap<String, usize>,
    open_links: Vec<OpenLink>,
    instances: Instances,
    rebuilt: Option<Graph>,
    content: String,
    purge: Purged,
    described: Option<Description>,
    answers: Vec<String>,
    shared: Option<Graph>,
    fates: Vec<Fate>,
    references: Vec<Node>,
    queue: Option<Queue>,
    tickets: Vec<Ticket>,
    flushed: Option<Ticket>,
    failed: Vec<Ticket>,
    heard: Arc<Mutex<Vec<Heard>>>,
    snapshot: Option<Snapshot>,
    stored_cursor: String,
    bound: Cursor,
    derived: Vec<FactRef>,
    follower: Option<std::process::Child>,
    followed: Followed,
    graphs: Vec<Graph>,
    restate: Option<Unit>,
    first: Vec<Event>,
    hearings: BTreeMap<String, Vec<Hearing>>,
    refusals: Vec<Error>,
    key: [u8; 32],
    states: Vec<Option<Version>>,
    as_of_counts: Vec<(Date, Result<Vec<Tally>, Error>)>,
    /// What one read of when the node or link was seen returned, and whether it read the link.
    seen: Option<rannoch::Seen>,
    reads_link: bool,
    links: Vec<LinkAt>,
    /// The node whose history a scenario walks.
    walking: Option<NodeRef>,
}

#[rstest::fixture]
fn world() -> World {
    World {
        dir: TempDir::new().unwrap(),
        graph: None,
        cursor: Cursor::START,
        events_before: 0,
        declared: Vec::new(),
        written: Vec::new(),
        write_error: None,
        stream: None,
        walked: Vec::new(),
        walks: Vec::new(),
        tallies: Vec::new(),
        found: None,
        window: (SystemTime::UNIX_EPOCH, SystemTime::UNIX_EPOCH),
        source: HashMap::new(),
        largest: String::new(),
        burst: None,
        burst_done: Arc::new(AtomicBool::new(false)),
        reads: (0, 0, 0),
        log: Vec::new(),
        read: Vec::new(),
        folded: BTreeMap::new(),
        open_links: Vec::new(),
        instances: Instances::default(),
        rebuilt: None,
        content: String::new(),
        purge: Purged::default(),
        described: None,
        answers: Vec::new(),
        shared: None,
        fates: Vec::new(),
        references: Vec::new(),
        queue: None,
        tickets: Vec::new(),
        flushed: None,
        failed: Vec::new(),
        heard: Arc::new(Mutex::new(Vec::new())),
        snapshot: None,
        stored_cursor: String::new(),
        bound: Cursor::START,
        derived: Vec::new(),
        follower: None,
        followed: Followed::default(),
        graphs: Vec::new(),
        restate: None,
        first: Vec::new(),
        hearings: BTreeMap::new(),
        refusals: Vec::new(),
        key: KEY,
        states: Vec::new(),
        as_of_counts: Vec::new(),
        seen: None,
        reads_link: false,
        links: Vec::new(),
        walking: None,
    }
}

impl World {
    fn path(&self) -> PathBuf {
        self.dir.path().join("graph.rannoch")
    }

    /// Opens the graph file, keeping the handle.
    fn open(&mut self) -> &mut Graph {
        self.graph = Some(rannoch::open(self.path(), Some(&KEY)).unwrap());
        self.g()
    }

    fn g(&mut self) -> &mut Graph {
        self.graph.as_mut().expect("the graph is open")
    }

    /// The original graph and the graph rebuilt from its events.
    fn both(&self) -> (&Graph, &Graph) {
        (
            self.graph.as_ref().expect("the graph is open"),
            self.rebuilt.as_ref().expect("the graph was rebuilt"),
        )
    }

    fn all_events(&mut self) -> Vec<Event> {
        self.g().events().since(Cursor::START).unwrap()
    }

    fn mark_events(&mut self) {
        self.events_before = self.all_events().len();
    }

    fn declare(&mut self, declaration: Declaration) {
        let result = self.g().declare(&declaration).map_err(|e| e.to_string());
        self.declared.push(result);
    }

    fn refusal(&self) -> &str {
        match self.declared.last() {
            Some(Err(reason)) => reason,
            other => panic!("expected a refused declaration, got {other:?}"),
        }
    }

    fn write(&mut self, write: &Write) {
        self.window.0 = SystemTime::now();
        self.written = self.g().write(write).unwrap();
        self.window.1 = SystemTime::now();
    }

    fn find(&mut self, class: &str, key: &str) -> Node {
        self.g()
            .find(class, key)
            .unwrap()
            .expect("the node is in the graph")
    }
}

fn names(g: &Graph) -> (Vec<&str>, Vec<&str>) {
    (
        g.ontology().classes.keys().map(String::as_str).collect(),
        g.ontology().relations.keys().map(String::as_str).collect(),
    )
}

fn assert_t9b_ontology(g: &Graph) {
    let (classes, relations) = names(g);
    assert_eq!(
        classes,
        ["conversation", "finding", "place", "statement", "voice"]
    );
    assert_eq!(relations, ["at", "inside", "said", "supports"]);
    for d in t9b() {
        match d {
            Declaration::Class(c) => assert_eq!(g.ontology().classes[&c.name], c),
            Declaration::Relation(r) => assert_eq!(g.ontology().relations[&r.name], r),
            Declaration::Property { .. } => unreachable!(),
        }
    }
}

// ---------- open ----------

#[given("a path that holds no graph file")]
fn no_graph_file(world: &mut World) {
    assert!(!world.path().exists());
}

#[when("the app opens it")]
fn opens_it(world: &mut World) {
    world.open();
}

#[then("the graph's ontology lists no class and no relation")]
fn ontology_empty(world: &mut World) {
    let (classes, relations) = names(world.g());
    assert!(classes.is_empty() && relations.is_empty());
}

#[when("the app opens it and reads the events from the start")]
fn opens_and_reads(world: &mut World) {
    world.open();
    world.read = world.all_events();
}

#[then("it receives no event")]
fn receives_no_event(world: &mut World) {
    assert!(world.read.is_empty());
}

#[given("a graph file whose stream declared 5 classes and 4 relations")]
#[given("a graph whose ontology holds 9 declarations")]
fn file_with_t9b(world: &mut World) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    world.graph = None;
}

#[when("the app opens it and declares nothing")]
fn opens_declares_nothing(world: &mut World) {
    world.open();
}

#[then("the graph's ontology lists those 5 classes and 4 relations")]
fn ontology_lists_t9b(world: &mut World) {
    assert_t9b_ontology(world.g());
}

// ---------- write: declarations ----------

#[given("an empty graph")]
fn empty_graph(world: &mut World) {
    world.open();
}

#[when("the app declares 5 classes and 4 relations")]
fn declares_t9b(world: &mut World) {
    for d in t9b() {
        world.declare(d);
    }
}

#[then("reading the events from the start returns 9 declaration events")]
fn nine_declaration_events(world: &mut World) {
    let events = world.all_events();
    assert_eq!(events.len(), 9);
    let declared: Vec<Declaration> = events
        .into_iter()
        .map(|e| match e.change {
            Change::Declared(d) => d,
            other => panic!("not a declaration event: {other:?}"),
        })
        .collect();
    assert_eq!(declared, t9b());
}

#[given("a declared class statement with property text")]
fn statement_with_text(world: &mut World) {
    world
        .open()
        .declare(&Declaration::Class(Class::new("statement", ["text"])))
        .unwrap();
    world.mark_events();
}

#[given("a declared class statement with property text and one statement written")]
fn statement_with_text_written(world: &mut World) {
    statement_with_text(world);
    world.write(&Write::new("swtor").unit(Unit::new(
        "statement",
        "s1",
        [("text", "hi")],
        day(2021, 3, 2),
    )));
}

#[when("the app declares property need on statement")]
fn declares_need(world: &mut World) {
    world.declare(Declaration::Property {
        class: "statement".into(),
        property: "need".into(),
        of: ValueType::Text,
    });
}

#[then("exactly one declaration event is written")]
fn one_declaration_event(world: &mut World) {
    let events = world.all_events();
    assert_eq!(events.len(), world.events_before + 1);
    assert_eq!(
        events.last().unwrap().change,
        Change::Declared(Declaration::Property {
            class: "statement".into(),
            property: "need".into(),
            of: ValueType::Text,
        })
    );
}

#[then("the statement written before reads need as absent")]
fn need_absent(world: &mut World) {
    assert!(
        world.g().ontology().classes["statement"]
            .properties
            .contains_key("need")
    );
    let s1 = world.find("statement", "s1");
    assert_eq!(s1.properties.get("text"), Some(&Value::from("hi")));
    assert_eq!(s1.properties.get("need"), None);
}

#[given("a graph with class voice declared with no properties")]
#[given("class voice is declared with no properties")]
fn voice_no_properties(world: &mut World) {
    world
        .open()
        .declare(&Declaration::Class(Class::new("voice", [] as [&str; 0])))
        .unwrap();
    world.mark_events();
}

#[when("the app declares class voice with property extra")]
fn declares_voice_extra(world: &mut World) {
    world.declare(Declaration::Class(Class::new("voice", ["extra"])));
}

#[then("the number of events is unchanged")]
#[then("no event is written")]
fn events_unchanged(world: &mut World) {
    assert_eq!(world.all_events().len(), world.events_before);
}

#[then("the graph's ontology still lists voice with no properties")]
fn voice_still_bare(world: &mut World) {
    assert!(world.refusal().contains("voice"));
    world.open();
    assert_eq!(
        world.g().ontology().classes["voice"],
        Class::new("voice", [] as [&str; 0])
    );
}

#[when("the app opens it and declares the same 9 again")]
fn redeclares_t9b(world: &mut World) {
    world.open();
    world.mark_events();
    for d in t9b() {
        world.declare(d);
    }
}

#[then("every declaration returns Ok")]
fn every_ok(world: &mut World) {
    assert_eq!(world.declared.len(), 9);
    assert!(
        world.declared.iter().all(Result::is_ok),
        "{:?}",
        world.declared
    );
}

#[given("class statement is declared with properties text and need")]
fn statement_text_need(world: &mut World) {
    world
        .open()
        .declare(&Declaration::Class(Class::new(
            "statement",
            ["text", "need"],
        )))
        .unwrap();
    world.mark_events();
}

#[when("the app declares class statement with properties need and text")]
fn declares_need_text(world: &mut World) {
    world.declare(Declaration::Class(Class::new(
        "statement",
        ["need", "text"],
    )));
    assert!(world.declared.last().unwrap().is_ok());
}

#[given("class statement is declared with properties text and need, need added mid-stream")]
fn statement_need_mid_stream(world: &mut World) {
    let g = world.open();
    g.declare(&Declaration::Class(Class::new("statement", ["text"])))
        .unwrap();
    g.write(&Write::new("swtor").unit(Unit::new(
        "statement",
        "s1",
        [("text", "hi")],
        day(2021, 3, 2),
    )))
    .unwrap();
    g.declare(&Declaration::Property {
        class: "statement".into(),
        property: "need".into(),
        of: ValueType::Text,
    })
    .unwrap();
    world.mark_events();
}

#[when("the app declares class statement with property text alone")]
fn declares_text_alone(world: &mut World) {
    world.declare(Declaration::Class(Class::new("statement", ["text"])));
}

#[then("the declaration is refused")]
fn refused(world: &mut World) {
    assert!(!world.refusal().is_empty());
    assert_eq!(world.all_events().len(), world.events_before);
}

#[then(
    "the declaration is refused with a reason naming class voice, its properties and the declared properties"
)]
fn refused_naming_voice(world: &mut World) {
    let reason = world.refusal();
    assert!(reason.contains("class voice"), "{reason}");
    assert!(reason.contains("[]"), "{reason}");
    assert!(reason.contains("[extra]"), "{reason}");
}

#[then("the reason contains no table name and no SQL text")]
fn reason_not_sql(world: &mut World) {
    let reason = world.refusal().to_string();
    let file = rusqlite::Connection::open(world.path()).unwrap();
    let mut statement = file.prepare("SELECT name FROM sqlite_master").unwrap();
    let tables: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(!tables.is_empty());
    for table in &tables {
        assert!(!reason.contains(table.as_str()), "{reason} names {table}");
    }
    let lower = reason.to_lowercase();
    for sql in [
        "table",
        "select",
        "insert",
        "create",
        "alter",
        "column",
        "sqlite",
        "constraint",
    ] {
        assert!(!lower.contains(sql), "{reason} has SQL text {sql}");
    }
}

#[given("relation said is declared from voice to statement")]
fn said_declared(world: &mut World) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    world.mark_events();
}

#[when("the app declares relation said from statement to voice")]
fn declares_said_reversed(world: &mut World) {
    world.declare(Declaration::Relation(Relation::new(
        "said",
        "statement",
        "voice",
        false,
    )));
}

#[then(
    "the declaration is refused with a reason naming relation said, its ends and the declared ends"
)]
fn refused_naming_said(world: &mut World) {
    let reason = world.refusal();
    assert!(reason.contains("relation said"), "{reason}");
    assert!(reason.contains("from voice to statement"), "{reason}");
    assert!(reason.contains("from statement to voice"), "{reason}");
}

#[given("no class tweet is declared")]
fn no_tweet(world: &mut World) {
    said_declared(world);
    assert!(!world.g().ontology().classes.contains_key("tweet"));
}

#[when("the app declares relation quotes from statement to tweet")]
fn declares_quotes(world: &mut World) {
    world.declare(Declaration::Relation(Relation::new(
        "quotes",
        "statement",
        "tweet",
        false,
    )));
}

#[then("the declaration is refused with a reason naming class tweet")]
fn refused_naming_tweet(world: &mut World) {
    let reason = world.refusal();
    assert!(reason.contains("class tweet"), "{reason}");
    assert_eq!(world.all_events().len(), world.events_before);
}

// ---------- write: units and pieces ----------

#[given("an open graph and the app's cursor at the end of its events")]
fn open_with_cursor(world: &mut World) {
    declare_replies(world.open());
    world.cursor = world.all_events().last().unwrap().cursor;
}

#[when("the app writes a reply stream in one write")]
fn writes_reply_stream(world: &mut World) {
    let stream = reply_stream();
    world.write(&stream_write(&stream.arrivals));
}

#[then("reading the events since the cursor returns exactly the events the write returned")]
fn since_cursor_is_written(world: &mut World) {
    let cursor = world.cursor;
    let since = world.g().events().since(cursor).unwrap();
    assert!(since.len() > 1);
    assert_eq!(since, world.written);
}

#[given("a write of five pieces whose third piece fails")]
fn third_piece_fails(world: &mut World) {
    world
        .open()
        .declare(&Declaration::Class(Class::new("voice", [] as [&str; 0])))
        .unwrap();
    world.cursor = world.all_events().last().unwrap().cursor;
    // The file refuses the first node of the third piece, as a full disk would.
    let file = rusqlite::Connection::open(world.path()).unwrap();
    file.execute_batch(&format!(
        "CREATE TRIGGER third_piece_fails BEFORE INSERT ON \"class:voice\" \
         WHEN NEW.key = 'v{}' BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END;",
        2 * PIECE_CHANGES
    ))
    .unwrap();
}

#[when("the app writes it")]
fn writes_five_pieces(world: &mut World) {
    let mut write = Write::new("swtor");
    for v in 0..5 * PIECE_CHANGES {
        write = write.unit(Unit::new(
            "voice",
            format!("v{v}"),
            NONE,
            ValidTime::Unknown,
        ));
    }
    world.write_error = Some(world.g().write(&write).expect_err("the third piece fails"));
}

fn piece_nodes(event: &Event) -> Vec<String> {
    match &event.change {
        Change::Wrote { added, .. } => added
            .iter()
            .map(|fact| match fact {
                Fact::Node(node) => node.key.clone(),
                other => panic!("unexpected fact {other:?}"),
            })
            .collect(),
        other => panic!("not a write event: {other:?}"),
    }
}

#[then("the write returns an error holding the events of pieces one and two")]
fn error_holds_two_pieces(world: &mut World) {
    let error = world.write_error.as_ref().unwrap();
    assert_eq!(error.events.len(), 2);
    for (piece, event) in error.events.iter().enumerate() {
        let want: Vec<String> = (piece * PIECE_CHANGES..(piece + 1) * PIECE_CHANGES)
            .map(|v| format!("v{v}"))
            .collect();
        assert_eq!(piece_nodes(event), want);
    }
}

#[then("the event log gains exactly those events")]
fn log_gains_those(world: &mut World) {
    let cursor = world.cursor;
    let since = world.g().events().since(cursor).unwrap();
    assert_eq!(since, world.write_error.as_ref().unwrap().events);
}

#[then("no unit of pieces three to five is in the graph")]
fn later_pieces_absent(world: &mut World) {
    let g = world.g();
    for v in 0..5 * PIECE_CHANGES {
        let present = g.find("voice", &format!("v{v}")).unwrap().is_some();
        assert_eq!(present, v < 2 * PIECE_CHANGES, "voice v{v}");
    }
}

// ---------- read: no reader sees half a unit ----------

#[given("the listener is writing a reply stream as units of a statement, its voice and its links")]
fn listener_writing(world: &mut World) {
    declare_replies(world.open());
    world.graph = None;
    let stream = reply_stream();
    let write = stream_write(&stream.arrivals);
    world.stream = Some(stream);
    let path = world.path();
    let done = Arc::clone(&world.burst_done);
    world.burst = Some(std::thread::spawn(move || {
        let mut g = rannoch::open(path, Some(&KEY)).unwrap();
        g.write(&write).unwrap();
        done.store(true, Ordering::SeqCst);
    }));
}

#[when("a reader counts statements and their voices during the burst")]
fn reader_counts(world: &mut World) {
    let reader = rannoch::open(world.path(), Some(&KEY)).unwrap();
    let keys: Vec<String> = world
        .stream
        .as_ref()
        .unwrap()
        .arrivals
        .iter()
        .map(|a| a.0.clone())
        .collect();
    let count = |path: Vec<Step>| {
        reader
            .count(&Count {
                class: "statement".into(),
                roots: keys.clone(),
                path,
                group_by: None,
                as_of: None,
                known_at: None,
            })
            .unwrap()
    };
    let (mut reads, mut mid_burst, mut orphans) = (0, 0, 0);
    while !world.burst_done.load(Ordering::SeqCst) {
        // Which statements are present, then their voices: a statement present in the first
        // read is present in the second, so one without a voice there was written without it.
        let present = count(Vec::new());
        let voices = count(vec![Step::new("said<-")]);
        let n = present.iter().filter(|t| t.distinct == 1).count();
        reads += 1;
        if n > 0 && n < keys.len() {
            mid_burst += 1;
        }
        orphans += present
            .iter()
            .zip(&voices)
            .filter(|(p, v)| p.distinct == 1 && v.distinct == 0)
            .count();
    }
    world.burst.take().unwrap().join().unwrap();
    world.reads = (reads, mid_burst, orphans);
}

#[then("no read sees a statement without its voice")]
fn no_orphans(world: &mut World) {
    let (reads, mid_burst, orphans) = world.reads;
    assert!(mid_burst > 0, "{reads} reads, none during the burst");
    assert_eq!(orphans, 0, "{orphans} statements seen without their voice");
}

// ---------- write: one node per key ----------

#[given("class voice is declared with a natural key")]
fn voice_with_key(world: &mut World) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
}

#[when("the app writes voice v1 and then writes voice v1 again")]
fn writes_v1_twice(world: &mut World) {
    world.write(&Write::new("swtor").unit(Unit::new("voice", "v1", NONE, day(2021, 3, 2))));
    world.write(&Write::new("reddit").unit(Unit::new("voice", "v1", NONE, day(2022, 1, 1))));
}

#[then("counting voices with key v1 gives 1")]
fn one_v1(world: &mut World) {
    let tallies = world
        .g()
        .count(&Count {
            class: "voice".into(),
            roots: vec!["v1".into()],
            path: Vec::new(),
            group_by: None,
            as_of: None,
            known_at: None,
        })
        .unwrap();
    assert_eq!(tallies.len(), 1);
    assert_eq!(tallies[0].distinct, 1);
    assert_eq!(world.find("voice", "v1").source, "reddit");
}

#[given("voice v1 written once")]
fn v1_once(world: &mut World) {
    voice_with_key(world);
    world.write(&Write::new("swtor").unit(Unit::new("voice", "v1", NONE, day(2021, 3, 2))));
    world.mark_events();
}

#[when("the app writes voice v1 again with the same properties, source and valid time")]
fn writes_v1_same(world: &mut World) {
    let held = world.all_events();
    world.write(&Write::new("swtor").unit(Unit::new("voice", "v1", NONE, day(2021, 3, 2))));
    // It writes nothing, and returns the event the hearing it repeats is named by.
    assert_eq!(world.written.len(), 1);
    assert!(held.contains(&world.written[0]));
}

// ---------- write: replacing a node ----------

#[given("statements s8 and s9")]
fn s8_and_s9(world: &mut World) {
    open_graph(world);
    let when = day(2021, 3, 2);
    world.write(
        &Write::new("eavesdrop")
            .unit(Unit::new(
                "statement",
                "s8",
                [("text", "first")],
                when.clone(),
            ))
            .unit(Unit::new("statement", "s9", [("text", "merged")], when)),
    );
    world.mark_events();
}

#[when("the app replaces s8 by s9")]
fn replaces_s8_by_s9(world: &mut World) {
    let event = world.g().replace("statement", "s8", "s9").unwrap();
    world.written = vec![event];
}

#[then("the event log gains one event naming class statement, s8 and s9")]
fn one_replacement_event(world: &mut World) {
    let after = world.all_events();
    let gained = &after[world.events_before..];
    assert_eq!(gained, world.written.as_slice());
    let [event] = gained else {
        panic!("expected one event, got {gained:?}");
    };
    assert_eq!(
        event.change,
        Change::Replaced {
            class: "statement".into(),
            old: "s8".into(),
            new: "s9".into(),
        }
    );
}

#[then("describing s8 answers replaced by s9")]
fn s8_replaced_by_s9(world: &mut World) {
    let s8 = world
        .g()
        .describe(&NodeRef::new("statement", "s8"))
        .unwrap();
    assert_eq!(s8.replaced_by, Some(NodeRef::new("statement", "s9")));
}

// ---------- FR-RANNOCH-067: replacing a node carries its hearings and links ----------

/// Statement `s9`, the node `s8` is replaced by in FR-RANNOCH-067's scenarios.
fn s9(world: &mut World) {
    let when = day(2021, 3, 2);
    world.write(&Write::new("eavesdrop").unit(Unit::new(
        "statement",
        "s9",
        [("text", "merged")],
        when,
    )));
}

#[given("a node with links")]
fn node_with_links(world: &mut World) {
    open_graph(world);
    let when = day(2021, 3, 2);
    world.write(
        &Write::new("swtor")
            .unit(Unit::new("voice", "v1", NONE, when.clone()))
            .unit(Unit::new("finding", "f1", NONE, when.clone()))
            .unit(
                Unit::new("statement", "s8", [("text", "first")], when.clone())
                    .link("said", "v1", "s8", when.clone())
                    .link("supports", "s8", "f1", when),
            ),
    );
    s9(world);
}

#[given("a node that has been heard more than once")]
fn node_heard_twice(world: &mut World) {
    open_graph(world);
    for (source, text) in [("swtor", "first"), ("reddit", "again")] {
        world.write(&Write::new(source).unit(Unit::new(
            "statement",
            "s8",
            [("text", text)],
            day(2021, 3, 2),
        )));
    }
    let hearings = world
        .g()
        .hearings(&NodeRef::new("statement", "s8"))
        .unwrap();
    assert_eq!(hearings.len(), 2, "{hearings:?}");
    world.hearings.insert("s8".into(), hearings);
    s9(world);
}

#[when("the app replaces it by a node under a new key")]
fn replaces_it_by_new_key(world: &mut World) {
    replaces_s8_by_s9(world);
}

#[then("its links are on the new key")]
fn links_on_new_key(world: &mut World) {
    let g = world.g();
    for (relation, from, to) in [("said", "v1", "s9"), ("supports", "s9", "f1")] {
        assert!(
            g.find_link(relation, from, to).unwrap().is_some(),
            "{relation}"
        );
        assert_eq!(g.link_hearings(relation, from, to).unwrap().len(), 1);
    }
    for (relation, from, to) in [("said", "v1", "s8"), ("supports", "s8", "f1")] {
        assert!(
            g.find_link(relation, from, to).unwrap().is_none(),
            "{relation}"
        );
        assert!(g.link_hearings(relation, from, to).unwrap().is_empty());
    }
    let s8 = g.describe(&NodeRef::new("statement", "s8")).unwrap();
    assert!(
        s8.relations_in.is_empty() && s8.relations_out.is_empty(),
        "{s8:?}"
    );
}

#[then("the new key holds every hearing the old key had")]
fn new_key_holds_hearings(world: &mut World) {
    let had = world.hearings["s8"].clone();
    let g = world.g();
    let now = g.hearings(&NodeRef::new("statement", "s9")).unwrap();
    for hearing in &had {
        assert!(now.contains(hearing), "{hearing:?} not in {now:?}");
    }
    assert_eq!(now.len(), had.len() + 1);
    assert!(
        g.hearings(&NodeRef::new("statement", "s8"))
            .unwrap()
            .is_empty()
    );
}

// ---------- write: source, valid time, recorded time ----------

#[given("an open graph")]
fn open_graph(world: &mut World) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
}

#[when("the app writes a statement with valid time 2019-10 today")]
fn writes_2019_10(world: &mut World) {
    let when = ValidTime::On(Date::month(2019, 10));
    world.write(&Write::new("swtor").unit(Unit::new("statement", "s1", [("text", "hi")], when)));
}

#[then("the statement reports valid time 2019-10")]
fn reports_2019_10(world: &mut World) {
    assert_eq!(
        world.find("statement", "s1").valid,
        ValidTime::On(Date::month(2019, 10))
    );
    assert_eq!(Date::month(2019, 10).to_string(), "2019-10");
}

#[then("it reports a recorded time of today")]
fn recorded_today(world: &mut World) {
    let recorded = world.find("statement", "s1").recorded;
    let (start, end) = world.window;
    // rannoch records in milliseconds.
    let start = start - std::time::Duration::from_millis(1);
    assert!(
        start <= recorded && recorded <= end,
        "{recorded:?} not within the write"
    );
}

#[when("the app writes in one write a statement valid at 2019-10 and a said link valid at 2026-02")]
fn writes_statement_and_link(world: &mut World) {
    world.write(
        &Write::new("swtor")
            .unit(Unit::new(
                "voice",
                "v1",
                NONE,
                ValidTime::On(Date::month(2019, 10)),
            ))
            .unit(
                Unit::new(
                    "statement",
                    "s1",
                    [("text", "hi")],
                    ValidTime::On(Date::month(2019, 10)),
                )
                .link("said", "v1", "s1", ValidTime::On(Date::month(2026, 2))),
            ),
    );
    assert_eq!(world.written.len(), 1);
}

#[then("the said link reports valid time 2026-02")]
fn link_2026_02(world: &mut World) {
    let link = world.g().find_link("said", "v1", "s1").unwrap().unwrap();
    assert_eq!(link.valid, ValidTime::On(Date::month(2026, 2)));
}

#[when("the app writes a statement with source swtor")]
fn writes_from_swtor(world: &mut World) {
    world.write(&Write::new("swtor").unit(Unit::new(
        "statement",
        "s1",
        [("text", "hi")],
        day(2021, 3, 2),
    )));
}

#[then("the statement reports source swtor")]
fn reports_swtor(world: &mut World) {
    assert_eq!(world.find("statement", "s1").source, "swtor");
}

#[when("the app writes a statement whose valid time is stated unknown")]
fn writes_unknown(world: &mut World) {
    world.write(&Write::new("swtor").unit(Unit::new(
        "statement",
        "s1",
        [("text", "hi")],
        ValidTime::Unknown,
    )));
}

#[then("the statement reports its valid time as unknown")]
fn reports_unknown(world: &mut World) {
    assert_eq!(world.find("statement", "s1").valid, ValidTime::Unknown);
}

#[given("statement s05 valid since 2024-05-01")]
fn s05_valid(world: &mut World) {
    t10(world.open());
    assert_eq!(world.find("statement", "s05").valid, day(2024, 5, 1));
}

#[when("the app rewrites s05 on 2026-10-13")]
fn rewrites_s05(world: &mut World) {
    rewrite_s05(world.g());
}

#[then("s05 reports valid time 2024-05-01")]
fn s05_still_2024(world: &mut World) {
    let s05 = world.find("statement", "s05");
    assert_eq!(
        s05.properties["text"],
        Value::from("text of s05, corrected")
    );
    assert_eq!(s05.valid, day(2024, 5, 1));
}

// ---------- open links ----------

#[given("a reply stream and the set of reply links its source gave")]
#[given("a reply stream in which some replies arrive before their parent")]
fn a_reply_stream(world: &mut World) {
    declare_replies(world.open());
    let stream = reply_stream();
    let position: HashMap<&str, usize> = stream
        .arrivals
        .iter()
        .enumerate()
        .map(|(i, a)| (a.0.as_str(), i))
        .collect();
    let early = stream
        .arrivals
        .iter()
        .enumerate()
        .filter(|(i, a)| a.2.as_ref().is_some_and(|p| position[p.as_str()] > *i))
        .count();
    assert!(early > 0);
    world.stream = Some(stream);
}

#[given("a reply stream in which some parents never arrive")]
fn parents_never_arrive(world: &mut World) {
    a_reply_stream(world);
    let stream = world.stream.as_mut().unwrap();
    // T5b: 50 parents dropped from the stream, seeded.
    let parents: Vec<String> = stream
        .links
        .iter()
        .map(|l| l.1.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut rng = fastrand::Rng::with_seed(50);
    let dropped: BTreeSet<String> = rng
        .choose_multiple(parents.into_iter(), 50)
        .into_iter()
        .collect();
    stream.arrivals.retain(|a| !dropped.contains(&a.0));
    stream.links.retain(|l| dropped.contains(&l.1));
    assert!(!stream.links.is_empty());
}

#[when("the listener writes the whole stream")]
fn writes_whole_stream(world: &mut World) {
    let write = stream_write(&world.stream.as_ref().unwrap().arrivals);
    world.write(&write);
}

#[then("the graph's answers links are exactly that set")]
fn answers_equal_source(world: &mut World) {
    let stream = world.stream.take().unwrap();
    let g = world.g();
    let mut stored = BTreeSet::new();
    for (statement, _, _) in &stream.arrivals {
        let root = NodeRef::new("statement", statement.clone());
        for (parent, _) in g.walk(&root, &["answers"], 1, usize::MAX).unwrap().nodes {
            stored.insert((statement.clone(), parent.key));
        }
    }
    assert_eq!(stored.len(), stream.links.len());
    assert_eq!(stored, stream.links);
}

#[then(
    "reading the events from the start returns exactly one closing event for each of those replies' links"
)]
fn one_closing_each(world: &mut World) {
    let stream = world.stream.take().unwrap();
    let position: HashMap<&str, usize> = stream
        .arrivals
        .iter()
        .enumerate()
        .map(|(i, a)| (a.0.as_str(), i))
        .collect();
    let early: BTreeSet<(String, String)> = stream
        .arrivals
        .iter()
        .enumerate()
        .filter_map(|(i, a)| {
            a.2.as_ref()
                .filter(|p| position[p.as_str()] > i)
                .map(|p| (a.0.clone(), p.clone()))
        })
        .collect();
    let closings: Vec<(String, String)> = world
        .all_events()
        .into_iter()
        .filter_map(|e| match e.change {
            Change::Closed(link) => {
                assert_eq!(link.relation, "answers");
                Some((link.from.key, link.to.key))
            }
            _ => None,
        })
        .collect();
    assert_eq!(closings.len(), early.len());
    assert_eq!(closings.into_iter().collect::<BTreeSet<_>>(), early);
}

#[then("walking answers from each reply to those parents finds no parent")]
fn no_parent_found(world: &mut World) {
    let stream = world.stream.take().unwrap();
    let g = world.g();
    for (reply, parent) in &stream.links {
        let walked = g
            .walk(
                &NodeRef::new("statement", reply.clone()),
                &["answers"],
                1,
                usize::MAX,
            )
            .unwrap()
            .nodes;
        assert!(
            walked.is_empty(),
            "{reply} reached {walked:?}, its parent {parent} never arrived"
        );
    }
}

#[when("the listener writes the whole stream and the app reads the open links")]
fn writes_stream_reads_open(world: &mut World) {
    writes_whole_stream(world);
    reads_open_links(world);
}

#[then("the open links listed are exactly the links to the parents that never arrived")]
fn open_links_are_missing_parents(world: &mut World) {
    let stream = world.stream.take().unwrap();
    let arrived: HashSet<&str> = stream.arrivals.iter().map(|a| a.0.as_str()).collect();
    let expected: BTreeSet<(String, String, String)> = stream
        .links
        .iter()
        .filter(|l| arrived.contains(l.0.as_str()))
        .map(|l| ("answers".to_string(), l.0.clone(), l.1.clone()))
        .collect();
    assert!(!expected.is_empty());
    let listed: Vec<(String, String, String)> = world
        .open_links
        .iter()
        .map(|o| (o.relation.clone(), o.from.key.clone(), o.to.key.clone()))
        .collect();
    assert_eq!(listed.len(), expected.len());
    assert_eq!(listed.into_iter().collect::<BTreeSet<_>>(), expected);
}

fn s2_open_on_s1() -> OpenLink {
    OpenLink {
        relation: "answers".into(),
        from: NodeRef::new("statement", "s2"),
        to: NodeRef::new("statement", "s1"),
        waits_on: NodeRef::new("statement", "s1"),
    }
}

#[given("an answers link from statement s2 waiting on statement key s1, which is not in the graph")]
#[given("an answers link from statement s2 waiting on statement key s1")]
fn s2_waits_on_s1(world: &mut World) {
    declare_replies(world.open());
    let when = day(2021, 3, 2);
    world.write(
        &Write::new("discord").unit(
            Unit::new("statement", "s2", [("text", "re")], when.clone())
                .link("answers", "s2", "s1", when),
        ),
    );
    assert!(world.g().find("statement", "s1").unwrap().is_none());
}

#[when("the app reads the open links")]
fn reads_open_links(world: &mut World) {
    world.open_links = world.g().open_links().unwrap();
}

#[when("the app writes a voice under key s1")]
fn writes_voice_s1(world: &mut World) {
    world.write(&Write::new("discord").unit(Unit::new("voice", "s1", NONE, day(2021, 3, 3))));
}

#[then("the link is listed with target kind statement and target key s1")]
#[then("the open links still list the link from s2 waiting on statement s1")]
fn lists_s2_waiting_on_s1(world: &mut World) {
    let listed = world.g().open_links().unwrap();
    assert_eq!(listed, [s2_open_on_s1()]);
}

#[then("the write's result reports a wrong-kind arrival under key s1, naming the open link")]
fn reports_wrong_kind(world: &mut World) {
    let reported: Vec<(NodeRef, LinkFact)> = world
        .written
        .iter()
        .flat_map(|e| match &e.change {
            Change::Wrote { wrong_kind, .. } => wrong_kind.clone(),
            _ => Vec::new(),
        })
        .map(|report| (report.arrived, report.waiting))
        .collect();
    assert_eq!(
        reported,
        [(
            NodeRef::new("voice", "s1"),
            LinkFact {
                relation: "answers".into(),
                from: NodeRef::new("statement", "s2"),
                to: NodeRef::new("statement", "s1"),
                valid: day(2021, 3, 2),
                derived_from: Vec::new(),
            }
        )]
    );
}

// ---------- links between declared classes ----------

#[given("a finding and a statement exist")]
fn finding_and_statement(world: &mut World) {
    let when = day(2021, 3, 2);
    world.write(
        &Write::new("swtor")
            .unit(Unit::new("finding", "f1", NONE, when.clone()))
            .unit(Unit::new("statement", "s1", [("text", "hi")], when)),
    );
    world.mark_events();
}

/// The new statements written ahead of the wrong-class link: enough for several pieces.
const NEW_STATEMENTS: usize = 2 * PIECE_CHANGES + 1;

#[when(
    "the app writes, in one write of several pieces, new statements followed by a said link from the finding to the statement"
)]
fn writes_wrong_class_link(world: &mut World) {
    let when = day(2021, 3, 2);
    let mut write = Write::new("swtor");
    for n in 0..NEW_STATEMENTS {
        write = write.unit(Unit::new(
            "statement",
            format!("n{n}"),
            [("text", "new")],
            when.clone(),
        ));
    }
    write = write.unit(
        Unit::new("statement", "s1", [("text", "hi")], when.clone()).link("said", "f1", "s1", when),
    );
    world.write_error = Some(world.g().write(&write).expect_err("the write is refused"));
}

#[then("the write is refused")]
#[then("the write is refused when it is written")]
fn write_refused(world: &mut World) {
    let error = world.write_error.as_ref().unwrap();
    assert!(
        matches!(error.cause, Error::Refused(_)),
        "not refused: {}",
        error.cause
    );
    assert!(error.events.is_empty(), "{:?}", error.events);
}

#[then("none of the new statements is in the graph")]
fn new_statements_absent(world: &mut World) {
    let g = world.g();
    for n in 0..NEW_STATEMENTS {
        assert!(
            g.find("statement", &format!("n{n}")).unwrap().is_none(),
            "statement n{n} landed"
        );
    }
}

// ---------- read: no dangling link during a burst ----------

#[given("the listener is writing a reply stream in which some parents never arrive")]
fn listener_writing_without_parents(world: &mut World) {
    parents_never_arrive(world);
    world.graph = None;
    let write = stream_write(&world.stream.as_ref().unwrap().arrivals);
    let path = world.path();
    let done = Arc::clone(&world.burst_done);
    world.burst = Some(std::thread::spawn(move || {
        let mut g = rannoch::open(path, Some(&KEY)).unwrap();
        g.write(&write).unwrap();
        done.store(true, Ordering::SeqCst);
    }));
}

#[when("a reader walks answers links during the burst")]
fn reader_walks_answers(world: &mut World) {
    let reader = rannoch::open(world.path(), Some(&KEY)).unwrap();
    let stream = world.stream.as_ref().unwrap();
    // The replies whose parents never arrive, and every reply of the stream's first window.
    let replies: BTreeSet<String> = stream
        .links
        .iter()
        .map(|l| l.0.clone())
        .chain(
            stream
                .arrivals
                .iter()
                .take(200)
                .filter(|a| a.2.is_some())
                .map(|a| a.0.clone()),
        )
        .collect();
    let missing: BTreeSet<&str> = stream.links.iter().map(|l| l.1.as_str()).collect();
    let (mut reads, mut mid_burst, mut dangling) = (0, 0, 0);
    while !world.burst_done.load(Ordering::SeqCst) {
        let mut present = 0;
        for reply in &replies {
            let root = NodeRef::new("statement", reply.clone());
            let parents = match reader.walk(&root, &["answers"], 1, usize::MAX) {
                Ok(walk) => walk.nodes,
                Err(Error::NotFound { .. }) => continue,
                Err(other) => panic!("{other}"),
            };
            present += 1;
            for (parent, _) in parents {
                // A parent read once stays present, so one missing now was never written.
                let found = reader.find("statement", &parent.key).unwrap();
                if missing.contains(parent.key.as_str()) || found.is_none() {
                    dangling += 1;
                }
            }
        }
        reads += 1;
        if present > 0 && present < replies.len() {
            mid_burst += 1;
        }
    }
    world.burst.take().unwrap().join().unwrap();
    world.reads = (reads, mid_burst, dangling);
}

#[then("no answers link it finds points at a missing statement")]
fn no_dangling(world: &mut World) {
    let (reads, mid_burst, dangling) = world.reads;
    assert!(mid_burst > 0, "{reads} reads, none during the burst");
    assert_eq!(
        dangling, 0,
        "{dangling} answers links found to a missing statement"
    );
}

// ---------- events ----------

#[when("the app writes statement s1 with a said link from voice v1")]
fn writes_s1_said(world: &mut World) {
    let when = day(2021, 3, 2);
    world.write(
        &Write::new("discord")
            .unit(Unit::new("voice", "v1", NONE, when.clone()))
            .unit(
                Unit::new("statement", "s1", [("text", "hi")], when.clone())
                    .link("said", "v1", "s1", when),
            ),
    );
}

fn since_cursor(world: &mut World) -> Vec<Event> {
    let cursor = world.cursor;
    world.g().events().since(cursor).unwrap()
}

#[then("the events since the cursor name statement s1 and voice v1 by kind")]
fn names_nodes(world: &mut World) {
    let nodes: BTreeSet<(String, String)> = since_cursor(world)
        .into_iter()
        .flat_map(|e| match e.change {
            Change::Wrote { added, .. } => added,
            _ => Vec::new(),
        })
        .filter_map(|fact| match fact {
            Fact::Node(n) => Some((n.class, n.key)),
            _ => None,
        })
        .collect();
    assert!(nodes.contains(&("statement".into(), "s1".into())));
    assert!(nodes.contains(&("voice".into(), "v1".into())));
}

#[then("they name the said link with both endpoints v1 and s1")]
fn names_link(world: &mut World) {
    let links: Vec<(String, NodeRef, NodeRef)> = since_cursor(world)
        .into_iter()
        .flat_map(|e| match e.change {
            Change::Wrote { added, .. } => added,
            _ => Vec::new(),
        })
        .filter_map(|fact| match fact {
            Fact::Link(l) => Some((l.relation, l.from, l.to)),
            _ => None,
        })
        .collect();
    assert_eq!(
        links,
        [(
            "said".to_string(),
            NodeRef::new("voice", "v1"),
            NodeRef::new("statement", "s1")
        )]
    );
}

#[then("each event carries its own id")]
fn own_ids(world: &mut World) {
    let since = since_cursor(world);
    assert!(!since.is_empty());
    let all = world.all_events();
    let ids: HashSet<String> = all.iter().map(|e| e.id.to_string()).collect();
    assert_eq!(ids.len(), all.len());
}

#[given("a graph whose log holds the events of a graph")]
fn log_of_t2(world: &mut World) {
    let g = world.open();
    let declarations = g.events().since(Cursor::START).unwrap();
    assert!(declarations.is_empty());
    let mut log = Vec::new();
    for d in t9b() {
        log.extend(g.declare(&d).unwrap());
    }
    world.graph = None;
    let g = world.open();
    let mut written = t2(g);
    // Of t2's declarations only the voice's name is new; t9b's were already on the stream.
    log.append(&mut written);
    world.log = log;
}

#[when("the app reads the events since a cursor halfway through the log")]
fn reads_from_halfway(world: &mut World) {
    let half = world.log.len() / 2;
    let cursor = world.log[half - 1].cursor;
    world.read = world.g().events().since(cursor).unwrap();
}

#[then("it receives only the events after the cursor, in the order they were written")]
fn only_after_in_order(world: &mut World) {
    let half = world.log.len() / 2;
    assert!(world.log.len() > 1000);
    assert_eq!(world.read, world.log[half..]);
}

#[given("a graph")]
fn a_t2_graph(world: &mut World) {
    t2(world.open());
}

#[when("the app folds distinct voices per finding from the events since the start")]
fn folds_from_events(world: &mut World) {
    let mut voice_of: HashMap<String, String> = HashMap::new();
    let mut supports: HashSet<(String, String)> = HashSet::new();
    for event in world.all_events() {
        let (added, removed) = match event.change {
            Change::Wrote { added, .. } => (
                added
                    .into_iter()
                    .filter_map(|f| match f {
                        Fact::Link(l) => Some(l),
                        _ => None,
                    })
                    .collect(),
                Vec::new(),
            ),
            Change::Closed(l) => (vec![l], Vec::new()),
            Change::Declared(_)
            | Change::Replaced { .. }
            | Change::Ended { .. }
            | Change::Corrected { .. }
            | Change::Purged { .. } => (Vec::new(), Vec::new()),
            Change::Forgot { links, .. } => (Vec::new(), links),
        };
        for link in added {
            match link.relation.as_str() {
                "said" => {
                    voice_of.insert(link.to.key, link.from.key);
                }
                "supports" => {
                    supports.insert((link.from.key, link.to.key));
                }
                _ => {}
            }
        }
        for link in removed {
            match link.relation.as_str() {
                "said" => {
                    voice_of.remove(&link.to.key);
                }
                "supports" => {
                    supports.remove(&(link.from.key, link.to.key));
                }
                _ => {}
            }
        }
    }
    let mut pairs: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    for (statement, finding) in supports {
        if let Some(voice) = voice_of.get(&statement) {
            pairs.entry(finding).or_default().insert(voice.clone());
        }
    }
    world.folded = pairs
        .into_iter()
        .map(|(f, voices)| (f, voices.len()))
        .collect();
}

#[then("each finding's distinct voices equal its exact count read from live state")]
fn fold_equals_live(world: &mut World) {
    let findings: Vec<String> = (0..20).map(|f| format!("f{f}")).collect();
    let live = world
        .g()
        .count(&Count {
            class: "finding".into(),
            roots: findings.clone(),
            path: vec![Step::new("supports<-"), Step::new("said<-")],
            group_by: None,
            as_of: None,
            known_at: None,
        })
        .unwrap();
    assert_eq!(world.folded.len(), 20);
    for tally in live {
        assert!(tally.distinct > 0);
        assert_eq!(
            u64::try_from(world.folded[&tally.root]).unwrap(),
            tally.distinct,
            "{}",
            tally.root
        );
    }
}

// ---------- events: snapshots ----------

/// The T9b declarations, voices v0..v9 and statements s0..s4 said by them up to the cursor;
/// statements s5..s9 said by v5..v9 after it, so links after the cursor reach nodes before it.
fn events_around_cursor(world: &mut World) {
    let when = day(2026, 10, 14);
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    let said = |s: usize| {
        let statement = format!("s{s}");
        Write::new("eavesdrop").unit(
            Unit::new(
                "statement",
                statement.clone(),
                [("text", "words")],
                when.clone(),
            )
            .link("said", format!("v{s}"), statement, when.clone()),
        )
    };
    for v in 0..10 {
        g.write(&Write::new("eavesdrop").unit(Unit::new(
            "voice",
            format!("v{v}"),
            NONE,
            when.clone(),
        )))
        .unwrap();
    }
    for s in 0..5 {
        g.write(&said(s)).unwrap();
    }
    world.cursor = world.all_events().last().unwrap().cursor;
    for s in 5..10 {
        world.g().write(&said(s)).unwrap();
    }
    world.log = world.all_events();
}

fn take_snapshot(world: &mut World) {
    let (cursor, path) = (world.cursor, world.dir.path().join("snapshot.rannoch"));
    world.snapshot = Some(world.g().snapshot(cursor, path).unwrap());
}

/// Takes a snapshot at the end of the graph's log.
fn snapshot_at_end(world: &mut World) {
    world.cursor = world.all_events().last().unwrap().cursor;
    take_snapshot(world);
}

fn rebuild_from_snapshot(world: &mut World) -> &mut Graph {
    let snapshot = world.snapshot.clone().expect("the app took a snapshot");
    let later = world.g().events().since(snapshot.cursor()).unwrap();
    let path = world.dir.path().join("rebuilt.rannoch");
    world.rebuilt = Some(snapshot.rebuild(&later, path, Some(&KEY)).unwrap());
    world.rebuilt.as_mut().unwrap()
}

#[given("a graph with events")]
fn graph_with_events(world: &mut World) {
    events_around_cursor(world);
}

#[when("the app takes a snapshot at a cursor")]
fn takes_snapshot(world: &mut World) {
    take_snapshot(world);
}

#[then("every event before the cursor is still in the store")]
fn every_event_kept(world: &mut World) {
    let cursor = world.cursor;
    let before = |events: Vec<Event>| -> Vec<Event> {
        events.into_iter().filter(|e| e.cursor <= cursor).collect()
    };
    let kept = before(world.all_events());
    // 9 declarations, 10 voices and 5 statements were written up to the cursor.
    assert_eq!(kept.len(), 24);
    assert_eq!(kept, before(world.log.clone()));
    assert_eq!(world.all_events(), world.log);
}

#[given("a graph with a snapshot at a cursor and events after it")]
fn snapshot_with_later_events(world: &mut World) {
    events_around_cursor(world);
    take_snapshot(world);
}

#[when("the graph is rebuilt from the snapshot and the events after its cursor")]
#[when("the graph is rebuilt from the snapshot")]
fn rebuilt_from_snapshot(world: &mut World) {
    rebuild_from_snapshot(world);
}

#[then("the rebuilt graph holds every node and link the live graph holds")]
fn rebuilt_matches_live(world: &mut World) {
    let (live, rebuilt) = world.both();
    let voices = (0..10).map(|v| ("voice", format!("v{v}")));
    let statements = (0..10).map(|s| ("statement", format!("s{s}")));
    for (class, key) in voices.chain(statements) {
        let have = live.find(class, &key).unwrap().unwrap();
        let back = rebuilt.find(class, &key).unwrap();
        assert_eq!(
            back.map(node_facts),
            Some(node_facts(have)),
            "{class} {key}"
        );
    }
    for s in 0..10 {
        let (from, to) = (format!("v{s}"), format!("s{s}"));
        let have = live.find_link("said", &from, &to).unwrap().unwrap();
        let back = rebuilt.find_link("said", &from, &to).unwrap();
        assert_eq!(
            back.map(link_facts),
            Some(link_facts(have)),
            "said {from} {to}"
        );
    }
    assert_eq!(rebuilt.open_links().unwrap(), live.open_links().unwrap());
}

#[given("a graph with one node forgotten and one node replaced before a snapshot's cursor")]
fn fates_before_snapshot(world: &mut World) {
    events_around_cursor(world);
    let g = world.g();
    let target = Target::Node(NodeRef::new("statement", "s0"));
    g.purge(Purge::Entity { target }, "living", "eavesdrop")
        .unwrap();
    g.replace("statement", "s1", "s2").unwrap();
    snapshot_at_end(world);
}

#[then(
    "the forgotten node resolves as forgotten and the replaced node resolves to its replacement, as in the live graph"
)]
fn fates_survive_snapshot(world: &mut World) {
    let rebuilt = world.dir.path().join("rebuilt.rannoch");
    let mut referrer =
        rannoch::open(world.dir.path().join("referrer.rannoch"), Some(&KEY)).unwrap();
    referrer.mount("live", world.path()).unwrap();
    referrer.mount("rebuilt", rebuilt).unwrap();
    for (key, fate) in [
        (
            "s0",
            Fate::Forgotten {
                rule: "living".into(),
            },
        ),
        (
            "s1",
            Fate::Replaced {
                by: NodeRef::new("statement", "s2"),
            },
        ),
    ] {
        let target = NodeRef::new("statement", key);
        let live = referrer.resolve("live", &target).unwrap();
        assert_eq!(live, fate, "{key} in the live graph");
        assert_eq!(referrer.resolve("rebuilt", &target).unwrap(), live, "{key}");
    }
}

#[given("a graph whose class was declared before a snapshot's cursor")]
fn class_before_snapshot(world: &mut World) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    snapshot_at_end(world);
}

#[when("the graph is rebuilt from the snapshot and the app writes a node of that class")]
fn rebuilt_and_writes(world: &mut World) {
    let unit = Unit::new("voice", "v1", NONE, day(2026, 10, 21));
    world.written = rebuild_from_snapshot(world)
        .write(&Write::new("eavesdrop").unit(unit))
        .unwrap();
}

#[then("the write is accepted")]
fn write_accepted(world: &mut World) {
    assert_eq!(world.written.len(), 1);
    let Change::Wrote { added, .. } = &world.written[0].change else {
        panic!("expected a write, got {:?}", world.written[0].change);
    };
    let (_, rebuilt) = world.both();
    for fact in added {
        if let Fact::Node(node) = fact {
            assert!(rebuilt.find(&node.class, &node.key).unwrap().is_some());
        }
    }
}

#[given("a graph whose class was declared with its history axis before a snapshot's cursor")]
fn history_before_snapshot(world: &mut World) {
    world.open();
    let mut class = Class::new("reading", ["value"]);
    class.history = HistoryKey::Integer;
    declare_class(world, class);
    snapshot_at_end(world);
}

#[then("the class's history axis reads back from the rebuilt graph's ontology")]
fn history_from_rebuilt(world: &mut World) {
    let (_, rebuilt) = world.both();
    assert_eq!(
        rebuilt.ontology().classes["reading"].history,
        HistoryKey::Integer
    );
}

#[given("a graph whose class declared a property as a number before a snapshot's cursor")]
fn number_before_snapshot(world: &mut World) {
    class_with(world, ValueType::Integer);
    snapshot_at_end(world);
}

#[when(
    "the graph is rebuilt from the snapshot and the app writes a node of that class with text for the property"
)]
fn rebuilt_and_writes_text(world: &mut World) {
    let unit = Unit::new("item", "i1", [("v", "nine")], day(2026, 1, 1));
    world.write_error = Some(
        rebuild_from_snapshot(world)
            .write(&Write::new("app").unit(unit))
            .expect_err("the write is refused"),
    );
}

#[when("a new reader starts from the snapshot")]
fn new_reader_from_snapshot(world: &mut World) {
    let cursor = world.snapshot.as_ref().unwrap().cursor();
    world.read = world.g().events().since(cursor).unwrap();
}

#[then("the reader receives only the events after the snapshot's cursor")]
fn reader_gets_later_events(world: &mut World) {
    let after: Vec<Event> = world
        .log
        .iter()
        .filter(|e| e.cursor > world.cursor)
        .cloned()
        .collect();
    // Statements s5..s9 were written after the cursor.
    assert_eq!(after.len(), 5);
    assert_eq!(world.read, after);
}

/// Hearings of visits a, b and c up to the snapshot's cursor, in arrival order: (key, point).
/// Each node is heard at points that arrive out of order, so some arrive after newer ones; the
/// newest point is 50.
const BEFORE_SNAPSHOT: [(&str, u64); 10] = [
    ("a", 30),
    ("b", 20),
    ("c", 40),
    ("a", 10),
    ("b", 50),
    ("c", 15),
    ("a", 45),
    ("b", 5),
    ("c", 35),
    ("a", 25),
];

/// Hearings after the snapshot's cursor: four of them at points before its newest, 50.
const AFTER_SNAPSHOT: [(&str, u64); 6] = [
    ("b", 35),
    ("a", 5),
    ("c", 55),
    ("a", 60),
    ("c", 25),
    ("b", 12),
];

/// Writes each hearing as its own write: the visit heard at its point, with a valid time that
/// does not follow the point, and for b and c a link to a carrying the same valid time.
fn hear_visits(world: &mut World, hearings: &[(&str, u64)]) {
    for (key, n) in hearings {
        let month = u8::try_from(n * 7 % 12 + 1).unwrap();
        let valid = ValidTime::On(Date::day(2025, month, 1));
        let text = format!("{key} at {n}");
        let mut unit =
            Unit::new("visit", *key, [("text", text.as_str())], valid.clone()).at(at(*n));
        if *key != "a" {
            unit = unit.link("follows", *key, "a", valid);
        }
        world.write(&Write::new("crawler").unit(unit));
    }
}

/// A time-keyed class `visit` with relation `follows`, and the hearings before the snapshot.
fn visits_before_snapshot(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    let follows = Relation::new("follows", "visit", "visit", false);
    world.g().declare(&Declaration::Relation(follows)).unwrap();
    hear_visits(world, &BEFORE_SNAPSHOT);
}

/// Asserts each node's and link's state at every point from 0 to `last` is the same in the
/// live graph and the rebuilt one, and that some of those reads return a version.
fn same_states(world: &World, last: u64) {
    let (live, rebuilt) = world.both();
    let mut versions = 0;
    for n in 0..=last {
        let point = At::Time(at(n));
        for key in ["a", "b", "c"] {
            let node = NodeRef::new("visit", key);
            let have = live.state_at(&node, point).unwrap();
            assert_eq!(
                rebuilt.state_at(&node, point).unwrap(),
                have,
                "{key} at {n}"
            );
            versions += usize::from(have.is_some());
        }
        for from in ["b", "c"] {
            let have = live.link_state_at("follows", from, "a", point).unwrap();
            let back = rebuilt.link_state_at("follows", from, "a", point).unwrap();
            assert_eq!(back, have, "follows {from} a at {n}");
            versions += usize::from(have.is_some());
        }
    }
    assert!(versions > 0, "no read returned a version");
}

#[given(
    "a graph of a time-keyed class whose nodes and links have several versions each, some arriving after newer ones, and a snapshot at a cursor"
)]
#[given("a graph of a time-keyed class with a snapshot at a cursor")]
fn visits_and_snapshot(world: &mut World) {
    visits_before_snapshot(world);
    snapshot_at_end(world);
}

#[when("the graph is rebuilt from the snapshot alone")]
fn rebuilt_from_snapshot_alone(world: &mut World) {
    let snapshot = world.snapshot.clone().expect("the app took a snapshot");
    let path = world.dir.path().join("rebuilt.rannoch");
    world.rebuilt = Some(snapshot.rebuild(&[], path, Some(&KEY)).unwrap());
}

#[then(
    "for every node and link, a read of its state at each point before the snapshot's newest point returns the same answer in both graphs"
)]
fn snapshot_answers_the_past(world: &mut World) {
    same_states(world, 49);
}

#[given("events after the cursor, some at points before the snapshot's newest point")]
fn late_events_after_snapshot(world: &mut World) {
    hear_visits(world, &AFTER_SNAPSHOT);
}

#[then(
    "every read of state at a point, of first and last heard, and as of a valid time returns the same answer in both graphs"
)]
fn late_events_replayed(world: &mut World) {
    same_states(world, 61);
    let (live, rebuilt) = world.both();
    for key in ["a", "b", "c"] {
        let node = NodeRef::new("visit", key);
        // First recorded is per store, so only what the axis and the valid times decide.
        let seen = |g: &Graph| {
            let seen = g.seen(&node).unwrap().expect("the node was heard");
            (seen.first_heard, seen.last_heard, seen.first_true)
        };
        assert_eq!(seen(rebuilt), seen(live), "{key} seen");
    }
    let mut found = 0;
    for month in 1..=12 {
        let as_of = Date::day(2025, month, 15);
        for known in [None, Some(At::Time(at(20))), Some(At::Time(at(40)))] {
            for key in ["a", "b", "c"] {
                let read = |g: &Graph| {
                    let node = g.find_as_of("visit", key, &as_of, known).unwrap();
                    node.map(|node| (node.derived_from.clone(), node_facts(node)))
                };
                let have = read(live);
                assert_eq!(read(rebuilt), have, "{key} as of {as_of} known {known:?}");
                found += usize::from(have.is_some());
            }
            let walk = |g: &Graph| {
                let a = NodeRef::new("visit", "a");
                g.walk_as_of(&a, &["follows<-"], 1, 10, &as_of, known)
                    .unwrap()
                    .nodes
            };
            assert_eq!(
                walk(rebuilt),
                walk(live),
                "walk as of {as_of} known {known:?}"
            );
        }
    }
    assert!(found > 0, "no read as of a valid time returned a version");
}

#[given("a graph with events, none of them purged, and a snapshot after them")]
fn events_and_snapshot_after(world: &mut World) {
    events_around_cursor(world);
    snapshot_at_end(world);
}

#[when(
    "the graph is rebuilt from the snapshot and the app writes, for each event before the cursor, a node derived from it"
)]
fn writes_derived_from_each(world: &mut World) {
    let cursor = world.cursor;
    let before: Vec<EventId> = world
        .log
        .iter()
        .filter(|e| e.cursor <= cursor)
        .map(|e| e.id)
        .collect();
    assert_eq!(before.len(), world.log.len());
    let rebuilt = rebuild_from_snapshot(world);
    let mut refusals = Vec::new();
    for (n, id) in before.iter().enumerate() {
        let unit = Unit::new("finding", format!("f{n}"), NONE, day(2026, 10, 21));
        if let Err(error) = rebuilt.write(&Write::new("analyst").unit(unit).derived_from([*id])) {
            refusals.push(error.to_string());
        }
    }
    world.answers = refusals;
    world.events_before = before.len();
}

#[then("every write is accepted")]
fn every_write_accepted(world: &mut World) {
    assert!(world.answers.is_empty(), "{:?}", world.answers);
    let (_, rebuilt) = world.both();
    for n in 0..world.events_before {
        let found = rebuilt.find("finding", &format!("f{n}")).unwrap();
        assert_eq!(found.map(|f| f.derived_from.len()), Some(1), "f{n}");
    }
}

// ---------- events: derived from ----------

/// Classes statement and finding; statements s1 and s2, each written as its own event, which
/// `world.log` holds.
fn two_statements(world: &mut World) {
    let g = world.open();
    g.declare(&Declaration::Class(Class::new("statement", ["text"])))
        .unwrap();
    g.declare(&Declaration::Class(Class::new("finding", ["text"])))
        .unwrap();
    let mut events = Vec::new();
    for key in ["s1", "s2"] {
        events.extend(
            g.write(&Write::new("eavesdrop").unit(Unit::new(
                "statement",
                key,
                [("text", "words")],
                day(2026, 10, 21),
            )))
            .unwrap(),
        );
    }
    world.log = events;
}

/// Finding f1 derived from `events`.
fn finding_from(events: &[Event]) -> Write {
    Write::new("analyst")
        .unit(Unit::new(
            "finding",
            "f1",
            [("text", "knowledge")],
            day(2026, 10, 21),
        ))
        .derived_from(events.iter().map(|e| e.id))
}

fn ids(events: &[Event]) -> HashSet<EventId> {
    events.iter().map(|e| e.id).collect()
}

#[given("a graph with two events")]
fn graph_with_two_events(world: &mut World) {
    two_statements(world);
}

#[when("the app writes a node derived from both events")]
fn writes_from_both(world: &mut World) {
    let write = finding_from(&world.log);
    world.write(&write);
}

#[then("the node reads back naming both events as its source")]
fn names_both(world: &mut World) {
    let node = world.find("finding", "f1");
    assert_eq!(node.derived_from.len(), 2);
    assert_eq!(
        node.derived_from.into_iter().collect::<HashSet<_>>(),
        ids(&world.log)
    );
}

#[given("a graph with a node derived from an event")]
fn node_from_an_event(world: &mut World) {
    two_statements(world);
    let write = finding_from(&world.log[..1]);
    world.write(&write);
}

#[when("the app asks what was derived from that event")]
fn asks_derived(world: &mut World) {
    let event = world.log[0].id;
    world.derived = world.g().derived(event).unwrap();
}

#[then("the answer names that node")]
fn answer_names_node(world: &mut World) {
    assert_eq!(
        world.derived,
        [FactRef::Node(NodeRef::new("finding", "f1"))]
    );
    let other = world.log[1].id;
    assert!(world.g().derived(other).unwrap().is_empty());
}

#[when("the app writes a node derived from an event id the graph does not hold")]
fn writes_from_foreign_event(world: &mut World) {
    let mut other = rannoch::open(world.dir.path().join("other.rannoch"), Some(&KEY)).unwrap();
    let foreign = other
        .declare(&Declaration::Class(Class::new(
            "elsewhere",
            [] as [&str; 0],
        )))
        .unwrap()
        .expect("a new declaration is an event");
    let write = Write::new("eavesdrop")
        .unit(Unit::new("voice", "v-new", NONE, day(2026, 10, 21)))
        .derived_from([foreign.id]);
    world.write_error = Some(world.g().write(&write).expect_err("the write is refused"));
}

#[given("a graph with a node derived from an event and a snapshot after it")]
#[given("a graph with a node derived from an event, and a snapshot after that event")]
fn derived_before_snapshot(world: &mut World) {
    node_from_an_event(world);
    snapshot_at_end(world);
}

#[then("the node reads back naming the same event as its source")]
fn same_source_after_rebuild(world: &mut World) {
    let source = world.log[0].id;
    let (live, rebuilt) = world.both();
    let back = rebuilt.find("finding", "f1").unwrap().unwrap();
    assert_eq!(back.derived_from, [source]);
    assert_eq!(
        back.derived_from,
        live.find("finding", "f1").unwrap().unwrap().derived_from
    );
}

/// Statements s1 and s2 written as their own events, and relation answers between statements.
fn two_statements_and_answers(world: &mut World) {
    two_statements(world);
    world
        .g()
        .declare(&Declaration::Relation(Relation::new(
            "answers",
            "statement",
            "statement",
            false,
        )))
        .unwrap();
}

/// Statement s1 restated with a link answering s2, derived from `events`.
fn answer_from(events: &[Event]) -> Write {
    Write::new("eavesdrop")
        .unit(
            Unit::new("statement", "s1", [("text", "words")], day(2026, 10, 21)).link(
                "answers",
                "s1",
                "s2",
                day(2026, 10, 21),
            ),
        )
        .derived_from(events.iter().map(|e| e.id))
}

fn the_answer() -> FactRef {
    FactRef::Link {
        relation: "answers".into(),
        from: NodeRef::new("statement", "s1"),
        to: NodeRef::new("statement", "s2"),
    }
}

/// The events the link s1 answers s2 names as its source, as `g`'s log holds it.
fn answer_sources(g: &Graph) -> HashSet<EventId> {
    g.events()
        .since(Cursor::START)
        .unwrap()
        .into_iter()
        .flat_map(|e| match e.change {
            Change::Wrote { added, .. } => added,
            _ => Vec::new(),
        })
        .filter_map(|fact| match fact {
            Fact::Link(link) if link.relation == "answers" => Some(link.derived_from),
            _ => None,
        })
        .flatten()
        .collect()
}

#[given("a graph with two nodes and two events")]
fn two_nodes_two_events(world: &mut World) {
    two_statements_and_answers(world);
}

#[when("the app writes a link between the nodes derived from both events")]
fn writes_link_from_both(world: &mut World) {
    let write = answer_from(&world.log);
    world.write(&write);
}

#[then("the link reads back naming both events as its source")]
fn link_names_both(world: &mut World) {
    assert_eq!(answer_sources(world.g()), ids(&world.log));
}

#[given("a graph with a link derived from two events")]
fn link_from_two(world: &mut World) {
    two_statements_and_answers(world);
    writes_link_from_both(world);
}

#[when("the app asks what was derived from each of those events")]
fn asks_derived_each(world: &mut World) {
    let events: Vec<EventId> = world.log.iter().map(|e| e.id).collect();
    world.derived = events
        .into_iter()
        .flat_map(|e| world.g().derived(e).unwrap())
        .collect();
}

#[then("each answer names that link")]
fn each_names_link(world: &mut World) {
    for event in world.log.clone() {
        assert!(
            world.g().derived(event.id).unwrap().contains(&the_answer()),
            "{event:?}"
        );
    }
    assert_eq!(
        world.derived.iter().filter(|f| **f == the_answer()).count(),
        world.log.len()
    );
}

#[when(
    "the graph is rebuilt from the snapshot and the app writes another node derived from the same event"
)]
fn rebuilt_and_writes_derived(world: &mut World) {
    let source = world.log[0].id;
    let write = Write::new("analyst")
        .unit(Unit::new(
            "finding",
            "f2",
            [("text", "more knowledge")],
            day(2026, 10, 21),
        ))
        .derived_from([source]);
    world.written = rebuild_from_snapshot(world).write(&write).unwrap();
}

#[given("a graph with a link derived from an event and a snapshot after it")]
fn link_before_snapshot(world: &mut World) {
    two_statements_and_answers(world);
    let write = answer_from(&world.log[..1]);
    world.write(&write);
    snapshot_at_end(world);
}

#[when(
    "the graph is rebuilt once from its events and once from the snapshot and the events after its cursor"
)]
fn rebuilt_both_ways(world: &mut World) {
    let events = world.all_events();
    let path = world.dir.path().join("from-events.rannoch");
    let from_events = rannoch::rebuild(&events, path, Some(&KEY)).unwrap();
    rebuild_from_snapshot(world);
    let from_snapshot = world.rebuilt.take().unwrap();
    world.graphs = vec![from_events, from_snapshot];
}

#[then("in both rebuilt graphs the link reads back naming the same event as its source")]
fn link_source_in_both(world: &mut World) {
    let source = world.log[0].id;
    assert_eq!(answer_sources(world.g()), HashSet::from([source]));
    for g in &world.graphs {
        assert_eq!(answer_sources(g), HashSet::from([source]));
        assert!(g.derived(source).unwrap().contains(&the_answer()));
    }
}

#[given("a graph with a node derived from an event, and a purge that removed that event")]
fn derived_then_purged(world: &mut World) {
    node_from_an_event(world);
    let target = Target::Node(NodeRef::new("statement", "s1"));
    world
        .g()
        .purge(Purge::Entity { target }, "retention", "eavesdrop")
        .unwrap();
    let purged = world.log[0].id;
    assert!(world.all_events().iter().all(|e| e.id != purged));
}

#[when(
    "the app writes a node derived from the purged event's id, and another derived from an id the graph never held"
)]
fn writes_from_purged_and_never_held(world: &mut World) {
    let mut other = rannoch::open(world.dir.path().join("other.rannoch"), Some(&KEY)).unwrap();
    let never = other
        .declare(&Declaration::Class(Class::new(
            "elsewhere",
            [] as [&str; 0],
        )))
        .unwrap()
        .expect("a new declaration is an event");
    for (key, source) in [("f2", world.log[0].id), ("f3", never.id)] {
        let write = Write::new("analyst")
            .unit(Unit::new(
                "finding",
                key,
                [("text", "knowledge")],
                day(2026, 10, 21),
            ))
            .derived_from([source]);
        let refused = world.g().write(&write).expect_err("the write is refused");
        assert!(refused.events.is_empty(), "{:?}", refused.events);
        world.refusals.push(refused.cause);
    }
}

#[then("both writes are refused with the same refusal")]
fn same_refusal(world: &mut World) {
    let ids = [world.log[0].id.to_string()];
    let shapes: Vec<String> = world
        .refusals
        .iter()
        .map(|error| {
            let Error::Refused(why) = error else {
                panic!("not refused: {error}");
            };
            // Each refusal names its own id; the rest is the same.
            let at = why.find("event ").unwrap() + "event ".len();
            format!("{}<id>{}", &why[..at], &why[at + 36..])
        })
        .collect();
    assert_eq!(shapes.len(), 2);
    assert_eq!(shapes[0], shapes[1]);
    assert!(world.refusals[0].to_string().contains(&ids[0]));
}

#[given("one new graph given A then B and another given B then A")]
fn source_delivered_in_both_orders(world: &mut World) {
    given_both_orders(world);
}

#[when("the app writes to each graph a node derived from B")]
fn writes_from_b(world: &mut World) {
    let b = world.log[world.log.len() - 1].id;
    for g in &mut world.graphs {
        let write = Write::new("log")
            .unit(unit_of(g, "k3", "derived", 3))
            .derived_from([b]);
        if let Err(refused) = g.write(&write) {
            world.refusals.push(refused.cause);
        }
    }
}

#[then("both writes are accepted")]
fn both_accepted(world: &mut World) {
    assert!(world.refusals.is_empty(), "{:?}", world.refusals);
    let b = world.log[world.log.len() - 1].id;
    for g in &world.graphs {
        let class = the_class(g).name;
        assert_eq!(
            g.derived(b).unwrap(),
            [FactRef::Node(NodeRef::new(class, "k3"))]
        );
    }
}

// ---------- read: walks ----------

#[given("a finding whose 2-hop neighbourhood holds more than 200 nodes")]
fn t8_finding(world: &mut World) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    // T8's largest finding: supported by statements from 13,198 voices.
    let when = day(2026, 10, 1);
    let mut write = Write::new("swtor").unit(Unit::new("finding", "f1", NONE, when.clone()));
    for v in 0..13_198 {
        let (voice, statement) = (format!("v{v}"), format!("s{v}"));
        write = write
            .unit(Unit::new("voice", voice.clone(), NONE, when.clone()))
            .unit(
                Unit::new(
                    "statement",
                    statement.clone(),
                    [("text", "words")],
                    when.clone(),
                )
                .link("said", voice, statement.clone(), when.clone())
                .link("supports", statement, "f1", when.clone()),
            );
    }
    g.write(&write).unwrap();
    let all = g
        .walk(
            &NodeRef::new("finding", "f1"),
            &["supports<-", "said<-"],
            2,
            usize::MAX,
        )
        .unwrap()
        .nodes;
    assert!(all.len() > 200);
}

#[when("the app walks 2 hops from that finding capped at 200")]
fn walks_capped(world: &mut World) {
    world.walked = world
        .g()
        .walk(
            &NodeRef::new("finding", "f1"),
            &["supports<-", "said<-"],
            2,
            200,
        )
        .unwrap()
        .nodes;
}

#[then("the walk returns 200 nodes")]
fn returns_200(world: &mut World) {
    assert_eq!(world.walked.len(), 200);
    assert_eq!(
        world
            .walked
            .iter()
            .map(|(node, _)| node)
            .collect::<HashSet<_>>()
            .len(),
        200
    );
}

#[given("statement s2 answers statement s1")]
fn s2_answers_s1(world: &mut World) {
    declare_replies(world.open());
    let when = day(2021, 3, 2);
    world.write(
        &Write::new("discord")
            .unit(Unit::new(
                "statement",
                "s1",
                [("text", "question")],
                when.clone(),
            ))
            .unit(
                Unit::new("statement", "s2", [("text", "answer")], when.clone())
                    .link("answers", "s2", "s1", when),
            ),
    );
}

#[when("the app walks answers<- from s1 for 1 hop")]
fn walks_answers_back(world: &mut World) {
    world.walked = world
        .g()
        .walk(&NodeRef::new("statement", "s1"), &["answers<-"], 1, 200)
        .unwrap()
        .nodes;
}

#[then("the walk returns s2")]
fn returns_s2(world: &mut World) {
    let walked: Vec<&NodeRef> = world.walked.iter().map(|(node, _)| node).collect();
    assert_eq!(walked, [&NodeRef::new("statement", "s2")]);
}

// ---------- read: counts ----------

#[given("four statements by voice v1 that each support finding f1")]
fn four_by_v1(world: &mut World) {
    open_graph(world);
    let when = day(2026, 10, 1);
    let mut write = Write::new("swtor")
        .unit(Unit::new("finding", "f1", NONE, when.clone()))
        .unit(Unit::new("voice", "v1", NONE, when.clone()));
    for s in 1..=4 {
        let statement = format!("s{s}");
        write = write.unit(
            Unit::new(
                "statement",
                statement.clone(),
                [("text", "words")],
                when.clone(),
            )
            .link("said", "v1", statement.clone(), when.clone())
            .link("supports", statement, "f1", when.clone()),
        );
    }
    world.write(&write);
}

fn count_from_f1(world: &mut World, path: Vec<Step>) {
    world.tallies = world
        .g()
        .count(&Count {
            class: "finding".into(),
            roots: vec!["f1".into()],
            path,
            group_by: None,
            as_of: None,
            known_at: None,
        })
        .unwrap();
}

#[when("the app counts distinct voices with the total from f1")]
fn counts_from_f1(world: &mut World) {
    count_from_f1(world, vec![Step::new("supports<-"), Step::new("said<-")]);
}

#[then("the count gives 1 distinct voice")]
fn one_distinct(world: &mut World) {
    assert_eq!(world.tallies[0].distinct, 1);
}

#[then("a total of 4 statements")]
fn total_four(world: &mut World) {
    assert_eq!(world.tallies[0].total, 4);
}

#[given(
    "a graph of 100,000 voices and the largest finding's distinct voices, statements and per-place counts from its source"
)]
fn t4_largest(world: &mut World) {
    world.source = t4(world);
    world.largest = world
        .source
        .iter()
        .max_by_key(|(f, s)| (s.statements, std::cmp::Reverse((*f).clone())))
        .map(|(f, _)| f.clone())
        .unwrap();
}

#[when("the app counts that finding's distinct voices with the total, grouped by place")]
fn counts_largest(world: &mut World) {
    let largest = world.largest.clone();
    world.tallies = world
        .g()
        .count(&Count {
            class: "finding".into(),
            roots: vec![largest],
            path: vec![Step::new("supports<-"), Step::new("said<-")],
            group_by: Some(GroupBy {
                class: "statement".into(),
                property: "place".into(),
            }),
            as_of: None,
            known_at: None,
        })
        .unwrap();
}

#[then("the count returns those distinct voices, that total and those per-place counts")]
fn largest_matches_source(world: &mut World) {
    let source = &world.source[&world.largest];
    let tally = &world.tallies[0];
    assert_eq!(tally.distinct, source.voices.len() as u64);
    assert_eq!(tally.total, source.statements);
    let want: BTreeMap<Option<String>, (u64, u64)> = source
        .places
        .iter()
        .map(|(place, (voices, statements))| {
            (Some(place.clone()), (voices.len() as u64, *statements))
        })
        .collect();
    assert_eq!(tally.groups, want);
}

#[given("a graph of 100,000 voices and 1,000 findings")]
fn t4_graph(world: &mut World) {
    world.source = t4(world);
}

#[when("the app counts distinct voices from all 1,000 findings in one count")]
fn counts_all_findings(world: &mut World) {
    world.tallies = world
        .g()
        .count(&Count {
            class: "finding".into(),
            roots: (0..1000).map(|f| format!("f{f}")).collect(),
            path: vec![Step::new("supports<-"), Step::new("said<-")],
            group_by: None,
            as_of: None,
            known_at: None,
        })
        .unwrap();
}

#[then("the count gives a result for each of the 1,000 findings")]
fn result_per_finding(world: &mut World) {
    assert_eq!(world.tallies.len(), 1000);
    for (f, tally) in world.tallies.iter().enumerate() {
        assert_eq!(tally.root, format!("f{f}"));
        let voices = world.source.get(&tally.root).map_or(0, |s| s.voices.len());
        assert_eq!(tally.distinct, voices as u64, "{}", tally.root);
    }
}

#[given("finding f1 supported by statements s1 and s2 from voice v1")]
fn f1_s1_s2(world: &mut World) {
    open_graph(world);
    let when = day(2026, 10, 1);
    let mut write = Write::new("swtor")
        .unit(Unit::new("finding", "f1", NONE, when.clone()))
        .unit(Unit::new("voice", "v1", NONE, when.clone()));
    for statement in ["s1", "s2"] {
        write = write.unit(
            Unit::new("statement", statement, [("text", "words")], when.clone())
                .link("said", "v1", statement, when.clone())
                .link("supports", statement, "f1", when.clone()),
        );
    }
    world.write(&write);
}

#[given("relation judged-not-supporting is declared from statement to finding")]
fn judged_declared(world: &mut World) {
    world
        .g()
        .declare(&Declaration::Relation(Relation::new(
            "judged-not-supporting",
            "statement",
            "finding",
            false,
        )))
        .unwrap();
}

#[given("s2 is linked to f1 by judged-not-supporting")]
fn s2_judged(world: &mut World) {
    let when = day(2026, 10, 14);
    world.write(&Write::new("judge").unit(
        Unit::new("statement", "s2", [("text", "words")], day(2026, 10, 1)).link(
            "judged-not-supporting",
            "s2",
            "f1",
            when,
        ),
    ));
}

#[when(
    "the app counts statements from f1, leaving out those linked to f1 by judged-not-supporting"
)]
fn counts_unjudged(world: &mut World) {
    count_from_f1(
        world,
        vec![Step::new("supports<-").filter("judged-not-supporting", false)],
    );
}

#[then("the count gives a total of 1 statement")]
fn total_one(world: &mut World) {
    assert_eq!(world.tallies[0].total, 1);
}

// ---------- read: find ----------

#[given("statement s05 written with source swtor and valid time 2024-05-01")]
fn s05_from_swtor(world: &mut World) {
    world.open();
    world.window.0 = SystemTime::now();
    t10(world.g());
    world.window.1 = SystemTime::now();
}

#[when("the app finds statement s05")]
fn finds_s05(world: &mut World) {
    world.found = world.g().find("statement", "s05").unwrap();
}

#[then(
    "the node returned carries source swtor, valid time 2024-05-01 and the time rannoch recorded it"
)]
fn s05_provenance(world: &mut World) {
    let s05 = world.found.as_ref().unwrap();
    assert_eq!(s05.source, "swtor");
    assert_eq!(s05.valid, day(2024, 5, 1));
    let (start, end) = world.window;
    let start = start - std::time::Duration::from_millis(1);
    assert!(start <= s05.recorded && s05.recorded <= end);
}

#[given("statement s05 valid 2024-05-01, rewritten on 2026-10-13 with corrected text")]
fn s05_rewritten(world: &mut World) {
    t10(world.open());
    rewrite_s05(world.g());
}

#[then("the node returned carries the corrected text and valid time 2024-05-01")]
fn s05_corrected(world: &mut World) {
    let s05 = world.found.as_ref().unwrap();
    assert_eq!(
        s05.properties["text"],
        Value::from("text of s05, corrected")
    );
    assert_eq!(s05.valid, day(2024, 5, 1));
}

#[when("the app finds statement s05 as of 2023-06-30")]
fn finds_s05_2023(world: &mut World) {
    world.found = world
        .g()
        .find_as_of("statement", "s05", &Date::day(2023, 6, 30), None)
        .unwrap();
}

#[then("no node is returned")]
fn no_node(world: &mut World) {
    assert!(world.found.is_none());
    assert!(world.g().find("statement", "s05").unwrap().is_some());
}

// ---------- rebuild ----------

#[given("a graph whose stream holds 36 events, 9 of them declarations")]
fn t9b_stream(world: &mut World) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    world.instances = t9b_instances(world.g());
    let events = world.all_events();
    assert_eq!(events.len(), 36);
    let declarations = events
        .iter()
        .filter(|e| matches!(e.change, Change::Declared(_)))
        .count();
    assert_eq!(declarations, 9);
}

#[given("a graph written from a reply stream in which some parents never arrive")]
fn graph_from_stream_missing_parents(world: &mut World) {
    parents_never_arrive(world);
    writes_whole_stream(world);
    assert!(!world.g().open_links().unwrap().is_empty());
}

#[when("the app rebuilds a file from those events into an empty path")]
#[when("the app rebuilds a file from its events into an empty path")]
fn rebuilds(world: &mut World) {
    world.log = world.all_events();
    let path = world.dir.path().join("rebuilt.rannoch");
    assert!(!path.exists());
    rannoch::rebuild(&world.log, &path, Some(&KEY)).unwrap();
    world.rebuilt = Some(rannoch::open(&path, Some(&KEY)).unwrap());
}

#[then("none of the 36 events fails to land")]
fn every_event_lands(world: &mut World) {
    let landed = world
        .rebuilt
        .as_ref()
        .unwrap()
        .events()
        .since(Cursor::START)
        .unwrap();
    assert_eq!(landed.len(), 36);
    let ids =
        |events: &[Event]| -> Vec<_> { events.iter().map(|e| (e.id, e.change.clone())).collect() };
    assert_eq!(ids(&landed), ids(&world.log));
}

#[then("the rebuilt graph's ontology lists the same classes and relations as the original")]
fn same_ontology(world: &mut World) {
    let (original, rebuilt) = world.both();
    assert_t9b_ontology(rebuilt);
    assert_eq!(rebuilt.ontology(), original.ontology());
}

#[then("the rebuilt graph holds the same nodes and links as the original")]
fn same_instances(world: &mut World) {
    let (original, rebuilt) = world.both();
    assert_eq!(world.instances.nodes.len(), 27);
    for node in &world.instances.nodes {
        let have = original.find(&node.class, &node.key).unwrap().unwrap();
        let back = rebuilt.find(&node.class, &node.key).unwrap();
        assert_eq!(back.map(node_facts), Some(node_facts(have)), "{node:?}");
    }
    assert!(!world.instances.links.is_empty());
    for (relation, from, to) in &world.instances.links {
        let have = original.find_link(relation, from, to).unwrap().unwrap();
        let back = rebuilt.find_link(relation, from, to).unwrap();
        assert_eq!(
            back.map(link_facts),
            Some(link_facts(have)),
            "{relation} {from} {to}"
        );
    }
}

// ---------- read: answers in ontology terms ----------

#[given("the T9b graph")]
#[given("the T9b graph, with inside and at declared as containment")]
fn the_t9b_graph(world: &mut World) {
    t9b_graph(world.open());
    let relations = &world.g().ontology().relations;
    assert!(relations["inside"].containment && relations["at"].containment);
}

#[given("a statement's inside link waits for a place that has not arrived")]
fn inside_link_waits(world: &mut World) {
    let when = day(2021, 3, 2);
    world
        .g()
        .write(
            &Write::new("swtor").unit(
                Unit::new("statement", "s9", [("text", "hi")], when.clone())
                    .link("inside", "s9", "c9", when),
            ),
        )
        .unwrap();
    assert_eq!(world.g().open_links().unwrap().len(), 1);
}

#[when("the app describes statement s1")]
fn describes_s1(world: &mut World) {
    world.described = Some(
        world
            .g()
            .describe(&NodeRef::new("statement", "s1"))
            .unwrap(),
    );
}

fn described(world: &World) -> &Description {
    world.described.as_ref().expect("a node was described")
}

#[then("the answer gives class statement and property text")]
fn gives_class_and_text(world: &mut World) {
    let d = described(world);
    assert_eq!(d.node, NodeRef::new("statement", "s1"));
    assert_eq!(
        d.properties,
        BTreeMap::from([("text".to_string(), Value::from("the respec cost is absurd"))])
    );
}

fn counts(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|&(r, n)| (r.to_string(), n)).collect()
}

#[then("the answer gives relations out inside 1 and supports 1")]
fn gives_relations_out(world: &mut World) {
    assert_eq!(
        described(world).relations_out,
        counts(&[("inside", 1), ("supports", 1)])
    );
}

#[then("relations in said 1")]
fn gives_relations_in(world: &mut World) {
    assert_eq!(described(world).relations_in, counts(&[("said", 1)]));
}

#[then("the answer gives s1 part of conversation c1 via inside")]
fn part_of_c1(world: &mut World) {
    let d = described(world);
    assert_eq!(d.node, NodeRef::new("statement", "s1"));
    assert_eq!(
        d.part_of.first(),
        Some(&PartOf {
            node: NodeRef::new("conversation", "c1"),
            via: "inside".into(),
        })
    );
}

#[then("part of place swtor-tc via at")]
fn part_of_swtor_tc(world: &mut World) {
    let d = described(world);
    assert_eq!(d.part_of.len(), 2, "{:?}", d.part_of);
    assert_eq!(
        d.part_of[1],
        PartOf {
            node: NodeRef::new("place", "swtor-tc"),
            via: "at".into(),
        }
    );
}

fn node_ref_texts(node: &NodeRef) -> [String; 2] {
    [node.class.clone(), node.key.clone()]
}

fn description_texts(d: &Description) -> Vec<String> {
    let mut texts = node_ref_texts(&d.node).to_vec();
    texts.extend(d.properties.iter().flat_map(|(k, v)| [k.clone(), text(v)]));
    texts.extend(d.relations_out.keys().cloned());
    texts.extend(d.relations_in.keys().cloned());
    for p in &d.part_of {
        texts.extend(node_ref_texts(&p.node));
        texts.push(p.via.clone());
    }
    texts
}

/// Fails when a text names one of the file's tables or columns. A table name counts anywhere in
/// a text; a column name counts as a whole word, so `id` inside `said` does not.
fn assert_names_no_storage(world: &World, texts: &[String]) {
    let file = rusqlite::Connection::open(world.path()).unwrap();
    let tables: Vec<String> = file
        .prepare("SELECT name FROM sqlite_master")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut columns: BTreeSet<String> = BTreeSet::new();
    for table in &tables {
        let mut statement = file
            .prepare("SELECT name FROM pragma_table_info(?1)")
            .unwrap();
        columns.extend(
            statement
                .query_map([table], |row| row.get::<_, String>(0))
                .unwrap()
                .map(Result::unwrap),
        );
    }
    assert!(!tables.is_empty() && !columns.is_empty());
    assert!(!texts.is_empty());
    for text in texts {
        for table in &tables {
            assert!(
                !text.contains(table.as_str()),
                "{text:?} names table {table}"
            );
        }
        for word in text.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':')) {
            assert!(!columns.contains(word), "{text:?} names column {word}");
        }
    }
}

#[then("no table name and no column name appears in the answer")]
fn describe_names_no_storage(world: &mut World) {
    let texts = description_texts(described(world));
    assert_names_no_storage(world, &texts);
}

#[when(
    "the app runs a find, a walk, a count and an open-links read, and writes a link between the wrong classes"
)]
fn runs_every_read_and_a_refused_write(world: &mut World) {
    let g = world.g();
    let mut texts = Vec::new();
    let found = g
        .find("statement", "s1")
        .unwrap()
        .expect("s1 is in the graph");
    texts.extend([
        found.class,
        found.key,
        found.source,
        format!("{:?}", found.valid),
    ]);
    texts.extend(
        found
            .properties
            .iter()
            .flat_map(|(k, v)| [k.clone(), text(v)]),
    );
    let walked = g
        .walk(&NodeRef::new("statement", "s1"), &["inside", "at"], 2, 200)
        .unwrap()
        .nodes;
    assert_eq!(walked.len(), 2, "{walked:?}");
    texts.extend(walked.iter().flat_map(|(node, _)| node_ref_texts(node)));
    let tallies = g
        .count(&Count {
            class: "finding".into(),
            roots: vec!["f1".into()],
            path: vec![Step::new("supports<-"), Step::new("said<-")],
            group_by: Some(GroupBy {
                class: "statement".into(),
                property: "text".into(),
            }),
            as_of: None,
            known_at: None,
        })
        .unwrap();
    assert_eq!(tallies[0].distinct, 3, "{tallies:?}");
    for tally in tallies {
        texts.push(tally.root);
        texts.extend(tally.groups.into_keys().flatten());
    }
    let open_links = g.open_links().unwrap();
    assert!(
        !open_links.is_empty(),
        "the open-links read has nothing to answer"
    );
    for open in open_links {
        texts.push(open.relation);
        for end in [open.from, open.to, open.waits_on] {
            texts.extend(node_ref_texts(&end));
        }
    }
    let when = day(2021, 3, 2);
    let refused = g
        .write(
            &Write::new("swtor").unit(
                Unit::new(
                    "statement",
                    "s1",
                    [("text", "the respec cost is absurd")],
                    when.clone(),
                )
                .link("said", "f1", "s1", when),
            ),
        )
        .expect_err("a said link from a finding is refused");
    assert!(
        matches!(refused.cause, Error::Refused(_)),
        "not refused: {}",
        refused.cause
    );
    texts.push(refused.cause.to_string());
    texts.push(refused.to_string());
    world.answers = texts;
}

#[then("no table name and no column name appears in any answer or in the refusal")]
fn reads_and_refusal_name_no_storage(world: &mut World) {
    assert_names_no_storage(world, &world.answers);
}

// ---------- refs: references into another graph file (FR-RANNOCH-046) ----------

/// The T6 files' size: the local file holds this many references into the shared file.
const REFERENCES: usize = 300;

/// The T6 files (t6-t2-refs-replay, 2026-10-13): a shared file of statements s0..s299, and a
/// local file whose mentions l0..l299 each refer to the shared statement of the same number by
/// the shared file's name, its class and its key. The local file mounts the shared one.
#[given("the local file mounts the shared file")]
fn local_mounts_shared(world: &mut World) {
    let when = day(2026, 10, 13);
    let shared_path = world.dir.path().join("shared.rannoch");
    let mut shared = rannoch::open(&shared_path, Some(&KEY)).unwrap();
    shared
        .declare(&Declaration::Class(Class::new("statement", ["text"])))
        .unwrap();
    let mut write = Write::new("eavesdrop");
    for i in 0..REFERENCES {
        write = write.unit(Unit::new(
            "statement",
            format!("s{i}"),
            [("text", format!("statement {i}"))],
            when.clone(),
        ));
    }
    shared.write(&write).unwrap();
    world.shared = Some(shared);

    let local = world.open();
    local
        .declare(&Declaration::Class(Class::new(
            "mention",
            ["graph", "class", "key"],
        )))
        .unwrap();
    let mut write = Write::new("legion");
    for i in 0..REFERENCES {
        write = write.unit(Unit::new(
            "mention",
            format!("l{i}"),
            [
                ("graph", "shared".to_string()),
                ("class", "statement".to_string()),
                ("key", format!("s{i}")),
            ],
            when.clone(),
        ));
    }
    local.write(&write).unwrap();
    local.mount("shared", &shared_path).unwrap();
}

impl World {
    fn shared(&mut self) -> &mut Graph {
        self.shared.as_mut().expect("the shared file is open")
    }

    /// The reference the local mention `from` holds: the mounted file's name and the target.
    fn reference(&mut self, from: &str) -> (String, NodeRef) {
        let mention = self
            .g()
            .find("mention", from)
            .unwrap()
            .expect("the mention");
        let property = |name: &str| text(&mention.properties[name]);
        (
            property("graph"),
            NodeRef::new(property("class"), property("key")),
        )
    }

    fn resolve(&mut self, from: &str) -> Fate {
        let (graph, target) = self.reference(from);
        self.g().resolve(&graph, &target).unwrap()
    }

    /// Every mention in the local file, in order, and the local file's events.
    fn local_references(&mut self) -> (Vec<Node>, Vec<Event>) {
        let mentions = (0..REFERENCES)
            .map(|i| {
                self.g()
                    .find("mention", &format!("l{i}"))
                    .unwrap()
                    .expect("the mention")
            })
            .collect();
        (mentions, self.all_events())
    }
}

#[given("shared node s7 was forgotten by rule superseded")]
fn shared_s7_forgotten(world: &mut World) {
    world
        .shared()
        .purge(
            Purge::Entity {
                target: Target::Node(NodeRef::new("statement", "s7")),
            },
            "superseded",
            "eavesdrop",
        )
        .unwrap();
    assert_eq!(world.shared().find("statement", "s7").unwrap(), None);
}

#[given("shared node s8 was replaced by s9 with a replace event")]
fn shared_s8_replaced(world: &mut World) {
    let event = world.shared().replace("statement", "s8", "s9").unwrap();
    assert!(matches!(event.change, Change::Replaced { .. }));
}

#[given("shared node s10 is live")]
fn shared_s10_live(world: &mut World) {
    assert!(world.shared().find("statement", "s10").unwrap().is_some());
}

#[given("shared node s10 was written again with a changed property")]
fn shared_s10_new_version(world: &mut World) {
    let events = world
        .shared()
        .write(&Write::new("eavesdrop").unit(Unit::new(
            "statement",
            "s10",
            [("text", "changed")],
            day(2026, 10, 14),
        )))
        .unwrap();
    assert_eq!(events.len(), 1, "the new version lands as one event");
    let s10 = world.shared().find("statement", "s10").unwrap().unwrap();
    assert_eq!(s10.properties["text"], Value::from("changed"));
}

#[given("the local file holds 300 references into the shared file")]
fn local_holds_300(world: &mut World) {
    let (mentions, _) = world.local_references();
    assert_eq!(mentions.len(), 300);
    assert!(world.g().find("mention", "l300").unwrap().is_none());
    world.references = mentions;
}

#[when("the app resolves reference l7 to s7")]
fn resolves_l7(world: &mut World) {
    assert_eq!(world.reference("l7").1, NodeRef::new("statement", "s7"));
    world.fates = vec![world.resolve("l7")];
}

#[when("the app resolves reference l8 to s8")]
fn resolves_l8(world: &mut World) {
    assert_eq!(world.reference("l8").1, NodeRef::new("statement", "s8"));
    world.fates = vec![world.resolve("l8")];
}

#[when("the app resolves reference l10 to s10")]
fn resolves_l10(world: &mut World) {
    assert_eq!(world.reference("l10").1, NodeRef::new("statement", "s10"));
    world.fates = vec![world.resolve("l10")];
}

#[when("the app resolves them")]
fn resolves_all(world: &mut World) {
    world.log = world.all_events();
    world.fates = (0..REFERENCES)
        .map(|i| world.resolve(&format!("l{i}")))
        .collect();
    assert_eq!(world.fates.len(), REFERENCES);
}

#[then("it answers forgotten by superseded")]
fn answers_forgotten(world: &mut World) {
    assert_eq!(
        world.fates,
        [Fate::Forgotten {
            rule: "superseded".into()
        }]
    );
}

#[then("it answers replaced by s9")]
fn answers_replaced(world: &mut World) {
    assert_eq!(
        world.fates,
        [Fate::Replaced {
            by: NodeRef::new("statement", "s9")
        }]
    );
}

#[then("it answers live")]
fn answers_live(world: &mut World) {
    assert_eq!(world.fates, [Fate::Live]);
}

#[then("the local file's 300 references are unchanged")]
fn local_references_unchanged(world: &mut World) {
    let (mentions, events) = world.local_references();
    assert_eq!(mentions, world.references);
    assert_eq!(events, world.log);
}

// ---------- read: as of a valid time ----------

/// A date written `YYYY-MM-DD`.
fn date(text: &str) -> Date {
    let mut parts = text.split('-');
    let mut next = || parts.next().unwrap();
    let year = next().parse().unwrap();
    let month = next().parse().unwrap();
    let d = next().parse().unwrap();
    Date::day(year, month, d)
}

fn count_f1_as_of(world: &mut World, path: Vec<Step>, as_of: Date) {
    world.tallies = world
        .g()
        .count(&Count {
            class: "finding".into(),
            roots: vec!["f1".into()],
            path,
            group_by: None,
            as_of: Some(as_of),
            known_at: None,
        })
        .unwrap();
}

fn supporting(world: &mut World, as_of: Option<&Date>) -> Vec<(NodeRef, ValidTime)> {
    let f1 = NodeRef::new("finding", "f1");
    let g = world.g();
    match as_of {
        None => g.walk(&f1, &["supports<-"], 1, 1000),
        Some(as_of) => g.walk_as_of(&f1, &["supports<-"], 1, 1000, as_of, None),
    }
    .unwrap()
    .nodes
}

fn keys(nodes: &[(NodeRef, ValidTime)]) -> BTreeSet<String> {
    nodes.iter().map(|(n, _)| n.key.clone()).collect()
}

#[given("T10's finding with 41 supporting statements")]
#[given("T10's finding with 41 supporting statements from 13 voices")]
fn t10_finding(world: &mut World) {
    t10_s41(world.open());
}

#[given("T10's finding with 3 supporting statements of stated-unknown valid time")]
fn t10_unknowns(world: &mut World) {
    t10_s41(world.open());
    assert_eq!(t10_unknown().len(), 3);
}

#[when("the app counts the finding's supporting statements as of 2024-12-31")]
fn counts_statements_2024(world: &mut World) {
    count_f1_as_of(world, vec![Step::new("supports<-")], date("2024-12-31"));
}

#[then(
    "the count equals the number of the 41 that the fixture marks valid on or before 2024-12-31"
)]
fn statements_valid_by_2024(world: &mut World) {
    let expected = t10_valid_by(&date("2024-12-31")).len() as u64;
    assert!(expected > 0 && expected < 41);
    assert_eq!(world.tallies[0].distinct, expected);
    assert_eq!(world.tallies[0].total, expected);
}

#[when("the app counts the finding's distinct voices as of {when}")]
fn counts_voices_as_of(world: &mut World, when: String) {
    count_f1_as_of(
        world,
        vec![Step::new("supports<-"), Step::new("said<-")],
        date(&when),
    );
}

#[then(
    "the count equals the distinct voices of the statements the fixture marks valid on or before {when}"
)]
fn voices_valid_by(world: &mut World, when: String) {
    let valid = t10_valid_by(&date(&when));
    assert!(!valid.is_empty());
    let voices: BTreeSet<&str> = valid.iter().map(|(_, voice)| voice.as_str()).collect();
    assert_eq!(world.tallies[0].distinct, voices.len() as u64);
    assert_eq!(world.tallies[0].total, valid.len() as u64);
}

#[when(
    "the app reads the finding's supporting statements as of 2023-01-01, as of 2026-10-14 and as of 2100-01-01"
)]
fn reads_as_of_three(world: &mut World) {
    for when in ["2023-01-01", "2026-10-14", "2100-01-01"] {
        let as_of = date(when);
        let walked = supporting(world, Some(&as_of));
        world.walks.push((as_of, walked));
    }
}

#[then("none of the 3 statements of unknown valid time is returned")]
fn no_unknown_returned(world: &mut World) {
    let unknown = t10_unknown();
    assert_eq!(world.walks.len(), 3);
    for (as_of, walked) in &world.walks {
        let walked = keys(walked);
        for statement in &unknown {
            assert!(!walked.contains(statement), "{statement} as of {as_of}");
        }
        let expected: BTreeSet<String> = t10_valid_by(as_of).into_iter().map(|(s, _)| s).collect();
        assert!(!expected.is_empty());
        assert_eq!(walked, expected, "as of {as_of}");
    }
}

#[when("the app reads the finding's supporting statements with no as_of")]
fn reads_no_as_of(world: &mut World) {
    world.walked = supporting(world, None);
}

#[then("each of the 3 is returned marked unknown")]
fn unknowns_marked(world: &mut World) {
    for statement in t10_unknown() {
        let marks: Vec<&ValidTime> = world
            .walked
            .iter()
            .filter(|(node, _)| node.class == "statement" && node.key == statement)
            .map(|(_, valid)| valid)
            .collect();
        assert_eq!(
            marks,
            [&ValidTime::Unknown],
            "{statement} is returned marked unknown"
        );
    }
}

#[then("the rebuilt graph's answers links are the original's")]
fn same_answers(world: &mut World) {
    let stream = world.stream.take().unwrap();
    let (original, rebuilt) = world.both();
    let answers = |g: &Graph| {
        let mut links = HashSet::new();
        for (statement, _, _) in &stream.arrivals {
            let root = NodeRef::new("statement", statement.clone());
            for (parent, _) in g.walk(&root, &["answers"], 1, usize::MAX).unwrap().nodes {
                let link = g
                    .find_link("answers", statement, &parent.key)
                    .unwrap()
                    .unwrap();
                links.insert(link_facts(link));
            }
        }
        links
    };
    let have = answers(original);
    assert!(!have.is_empty());
    assert_eq!(answers(rebuilt), have);
}

#[then("the rebuilt graph's open links are the original's")]
fn same_open_links(world: &mut World) {
    let (original, rebuilt) = world.both();
    let have = original.open_links().unwrap();
    assert!(!have.is_empty());
    assert_eq!(rebuilt.open_links().unwrap(), have);
}

#[given(
    "a statement valid before a date t that supports finding f1 by a link none of whose versions was valid on or before t"
)]
fn link_after_t(world: &mut World) {
    open_graph(world);
    let before = day(2024, 1, 1);
    world.write(&Write::new("swtor").unit(Unit::new("finding", "f1", NONE, before.clone())));
    // Two versions of the link, both valid after t (2024-06-30).
    for valid in [day(2025, 1, 1), day(2025, 6, 1)] {
        let statement = Unit::new("statement", "s1", [("text", "words")], before.clone());
        world.write(&Write::new("swtor").unit(statement.link("supports", "s1", "f1", valid)));
    }
    assert_eq!(
        world
            .g()
            .link_hearings("supports", "s1", "f1")
            .unwrap()
            .len(),
        2
    );
}

#[when("the app counts statements supporting f1 as of t")]
fn counts_as_of_t(world: &mut World) {
    count_f1_as_of(world, vec![Step::new("supports<-")], date("2024-06-30"));
}

#[then("the statement is not counted")]
fn not_counted(world: &mut World) {
    assert_eq!(world.tallies[0].distinct, 0);
    assert_eq!(world.tallies[0].total, 0);
}

// ---------- open: value types and history keys ----------

fn declare_class(world: &mut World, class: Class) {
    world.g().declare(&Declaration::Class(class)).unwrap();
}

fn history_after_reopen(world: &mut World) -> HistoryKey {
    world.open().ontology().classes["reading"].history
}

#[when("the app declares a class whose history is ordered by an integer key the writer supplies")]
fn declares_integer_history(world: &mut World) {
    let mut class = Class::new("reading", ["value"]);
    class.history = HistoryKey::Integer;
    declare_class(world, class);
}

#[then("the graph's ontology names a writer-supplied integer key as that class's history order")]
fn history_is_integer(world: &mut World) {
    assert_eq!(history_after_reopen(world), HistoryKey::Integer);
}

#[when("the app declares a class without a history key")]
fn declares_no_history(world: &mut World) {
    declare_class(world, Class::new("reading", ["value"]));
}

#[then("the graph's ontology names the origin's record time as that class's history order")]
fn history_is_origin(world: &mut World) {
    assert_eq!(history_after_reopen(world), HistoryKey::Origin);
}

fn class_with(world: &mut World, of: ValueType) {
    world.open();
    let mut class = Class::new("item", [] as [&str; 0]);
    class.properties.insert("v".to_string(), of);
    declare_class(world, class);
}

fn write_and_read(world: &mut World, value: Value) {
    let unit = Unit::new("item", "i1", [("v", value)], day(2026, 1, 1));
    world.g().write(&Write::new("app").unit(unit)).unwrap();
    world.found = world.g().find("item", "i1").unwrap();
}

fn read_back(world: &World) -> &Value {
    &world.found.as_ref().expect("the node was found").properties["v"]
}

#[given("a class with a property declared as an integer")]
fn class_with_integer(world: &mut World) {
    class_with(world, ValueType::Integer);
}

#[when(
    "the app writes a node of that class with 9007199254740993 for the property and reads it back"
)]
fn writes_integer(world: &mut World) {
    write_and_read(world, Value::Integer(9_007_199_254_740_993));
}

#[then("the property reads back as the integer 9007199254740993")]
fn integer_back(world: &mut World) {
    assert_eq!(read_back(world), &Value::Integer(9_007_199_254_740_993));
}

#[given("a class with a property declared as a decimal")]
fn class_with_decimal(world: &mut World) {
    class_with(world, ValueType::Decimal);
}

#[when(
    "the app writes a node of that class with 12345678901234567890.0000000001 for the property and reads it back"
)]
fn writes_decimal(world: &mut World) {
    write_and_read(
        world,
        Value::Decimal("12345678901234567890.0000000001".to_string()),
    );
}

#[then("the property reads back as the decimal 12345678901234567890.0000000001")]
fn decimal_back(world: &mut World) {
    assert_eq!(
        read_back(world),
        &Value::Decimal("12345678901234567890.0000000001".to_string())
    );
}

#[given("a class with a property declared as a float vector of length 3")]
fn class_with_vector(world: &mut World) {
    class_with(world, ValueType::FloatVector(3));
}

#[when(
    "the app writes a node of that class with 1.5, -2.25 and 0.125 for the property and reads it back"
)]
fn writes_vector(world: &mut World) {
    write_and_read(world, Value::FloatVector(vec![1.5, -2.25, 0.125]));
}

#[then("the property reads back as a float vector of length 3 holding 1.5, -2.25 and 0.125")]
fn vector_back(world: &mut World) {
    let Value::FloatVector(floats) = read_back(world) else {
        panic!("expected a float vector, got {:?}", read_back(world));
    };
    assert_eq!(floats.len(), 3);
    assert_eq!(floats, &vec![1.5, -2.25, 0.125]);
}

/// `tests/fixtures/store-0.1.1.rannoch` was written by rannoch at tag v0.1.1: class book with
/// properties title and pages, then two writes of books b1, b2 and b3; 3 events. It is copied,
/// since opening a file writes to it.
#[given("a store written by rannoch 0.1.1 with a class, its properties and nodes")]
fn store_011(world: &mut World) {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/store-0.1.1.rannoch"
    );
    std::fs::copy(fixture, world.path()).unwrap();
}

#[when("the store is opened")]
fn store_opened(world: &mut World) {
    world.open();
}

/// Asserts every node `keys` names has one hearing, named by an event the store holds, at a
/// point after the epoch.
fn hearings_read_back_points(world: &mut World, keys: &[(&str, &str)]) {
    let held: BTreeSet<EventId> = world.all_events().iter().map(|e| e.id).collect();
    for (class, key) in keys {
        let hearings = world.g().hearings(&NodeRef::new(*class, *key)).unwrap();
        assert_eq!(hearings.len(), 1, "{class} {key}: {hearings:?}");
        assert!(held.contains(&hearings[0].event), "{class} {key}");
        assert!(hearings[0].point > SystemTime::UNIX_EPOCH, "{class} {key}");
    }
}

#[then(
    "every event it held is still read, each of the class's properties reads as text and each hearing reads back a point"
)]
fn store_011_whole(world: &mut World) {
    let events = world.all_events();
    assert_eq!(events.len(), 3);
    let written: Vec<&NodeFact> = events
        .iter()
        .flat_map(|e| match &e.change {
            Change::Wrote { added, .. } => added.iter().collect(),
            _ => Vec::new(),
        })
        .filter_map(|f| match f {
            Fact::Node(node) => Some(node),
            _ => None,
        })
        .collect();
    assert_eq!(written.len(), 3);
    let book = world.g().ontology().classes["book"].clone();
    assert_eq!(book.history, HistoryKey::Origin);
    assert!(book.properties.values().all(|of| *of == ValueType::Text));
    for (key, title) in [("b1", "Dune"), ("b2", "Emma"), ("b3", "Ubik")] {
        let node = world.find("book", key);
        assert_eq!(node.properties["title"], Value::from(title));
        assert!(matches!(node.properties["pages"], Value::Text(_)));
    }
    assert!(
        written
            .iter()
            .all(|n| n.properties.values().all(|v| matches!(v, Value::Text(_))))
    );
    hearings_read_back_points(world, &[("book", "b1"), ("book", "b2"), ("book", "b3")]);
}

// ---------- write: refused when it breaks its class's declaration ----------

fn write_refusing(world: &mut World, unit: Unit) {
    world.write_error = Some(
        world
            .g()
            .write(&Write::new("app").unit(unit))
            .expect_err("the write is refused"),
    );
}

#[given("a class that declares a property")]
fn class_with_text(world: &mut World) {
    class_with(world, ValueType::Text);
}

#[when("the app writes a node of that class without the property")]
fn writes_without_property(world: &mut World) {
    write_refusing(world, Unit::new("item", "i1", NONE, day(2026, 1, 1)));
}

#[when("the app writes a node of that class with text for the property")]
fn writes_text_for_integer(world: &mut World) {
    write_refusing(
        world,
        Unit::new("item", "i1", [("v", "nine")], day(2026, 1, 1)),
    );
}

// ---------- events: following from a stored cursor ----------

/// Stores `cursor` the way an app does, by serializing it.
fn store_cursor(world: &mut World, cursor: Cursor) {
    world.stored_cursor = serde_json::to_string(&cursor).unwrap();
}

fn stored_cursor(world: &World) -> Cursor {
    serde_json::from_str(&world.stored_cursor).unwrap()
}

/// The events of the log written before `World::log` was taken, after `cursor`.
fn logged_after(world: &World, cursor: Cursor) -> Vec<Event> {
    world
        .log
        .iter()
        .filter(|e| e.cursor > cursor)
        .cloned()
        .collect()
}

#[given("a follower that has stored its cursor")]
fn follower_stored_cursor(world: &mut World) {
    events_around_cursor(world);
    store_cursor(world, world.cursor);
    world.graph = None;
}

#[when("the follower restarts from the stored cursor")]
fn follower_restarts(world: &mut World) {
    let cursor = stored_cursor(world);
    world.read = world.open().events().since(cursor).unwrap();
}

#[then("it reads only the events after that cursor")]
fn reads_only_after_stored(world: &mut World) {
    let expected = logged_after(world, stored_cursor(world));
    // The five statements written after the cursor.
    assert_eq!(expected.len(), 5);
    assert_eq!(world.read, expected);
}

#[when("the app reads events since a cursor up to a bound")]
fn reads_up_to_bound(world: &mut World) {
    world.bound = world.log[world.log.len() - 3].cursor;
    let (cursor, bound) = (world.cursor, world.bound);
    world.read = world
        .g()
        .events()
        .since_until(cursor, bound, |_| true)
        .unwrap();
}

#[then("no event past the bound is returned")]
fn none_past_bound(world: &mut World) {
    let (cursor, bound) = (world.cursor, world.bound);
    assert!(world.read.iter().all(|e| e.cursor <= bound));
    let expected: Vec<Event> = logged_after(world, cursor)
        .into_iter()
        .filter(|e| e.cursor <= bound)
        .collect();
    assert_eq!(expected.len(), 3);
    assert_eq!(world.read, expected);
}

#[given("a graph with events of several kinds")]
fn events_of_several_kinds(world: &mut World) {
    events_around_cursor(world);
    let declared = |e: &Event| matches!(e.change, Change::Declared(_));
    assert!(world.log.iter().any(declared));
    assert!(world.log.iter().any(|e| !declared(e)));
}

#[when("the app reads events since a cursor with a filter")]
fn reads_with_filter(world: &mut World) {
    let last = world.log.last().unwrap().cursor;
    world.read = world
        .g()
        .events()
        .since_until(Cursor::START, last, |change| {
            matches!(change, Change::Declared(_))
        })
        .unwrap();
}

#[then("only events the filter asks about are returned")]
fn only_filtered(world: &mut World) {
    let expected: Vec<Event> = world
        .log
        .iter()
        .filter(|e| matches!(e.change, Change::Declared(_)))
        .cloned()
        .collect();
    assert_eq!(expected.len(), t9b().len());
    assert_eq!(world.read, expected);
}

#[given("a follower whose stored cursor is older than a snapshot")]
fn follower_behind_snapshot(world: &mut World) {
    events_around_cursor(world);
    store_cursor(world, world.log[5].cursor);
    take_snapshot(world);
    assert!(stored_cursor(world) < world.snapshot.as_ref().unwrap().cursor());
}

#[when("the follower reads since its stored cursor")]
fn follower_reads_since_stored(world: &mut World) {
    let cursor = stored_cursor(world);
    world.read = world.g().events().since(cursor).unwrap();
}

#[then("it reads every event after its cursor")]
fn reads_every_event_after(world: &mut World) {
    let expected = logged_after(world, stored_cursor(world));
    assert_eq!(expected.len(), world.log.len() - 6);
    assert_eq!(world.read, expected);
}

#[given("a follower that stored its cursor and a graph that is then rebuilt")]
fn follower_then_rebuild(world: &mut World) {
    events_around_cursor(world);
    take_snapshot(world);
    // A cursor two events past the snapshot's, so the rebuild must keep the later events'
    // places for the stored cursor to name the same one.
    let after = world
        .log
        .iter()
        .position(|e| e.cursor == world.cursor)
        .unwrap()
        + 2;
    store_cursor(world, world.log[after].cursor);
    rebuild_from_snapshot(world);
}

#[when("the follower reads the rebuilt graph's events since its stored cursor")]
fn follower_reads_rebuilt(world: &mut World) {
    let cursor = stored_cursor(world);
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    world.read = rebuilt.events().since(cursor).unwrap();
}

#[then("it reads the events that came after that cursor before the rebuild")]
fn reads_events_after_rebuild(world: &mut World) {
    let expected = logged_after(world, stored_cursor(world));
    assert_eq!(expected.len(), 3);
    assert_eq!(world.read, expected);
}

// ---------- events: the processed mark across registered followers ----------

/// The cursor after the `n`th event of the log (1-based).
fn after_event(world: &World, n: usize) -> Cursor {
    world.log[n - 1].cursor
}

fn mark(world: &mut World) -> Option<Cursor> {
    world.g().events().processed().unwrap()
}

#[given("a graph with events and two registered followers")]
fn events_and_two_followers(world: &mut World) {
    events_around_cursor(world);
    let events = world.g().events();
    events.register("slow").unwrap();
    events.register("fast").unwrap();
}

#[when("one follower stores a cursor after the third event and the other after the fifth")]
fn followers_store_third_and_fifth(world: &mut World) {
    let (third, fifth) = (after_event(world, 3), after_event(world, 5));
    let events = world.g().events();
    events.advance("slow", third).unwrap();
    events.advance("fast", fifth).unwrap();
}

#[then("the graph's processed mark is the cursor after the third event")]
fn mark_is_third(world: &mut World) {
    let third = after_event(world, 3);
    assert_eq!(mark(world), Some(third));
}

#[given("a graph whose processed mark is held by the slower of two registered followers")]
fn mark_held_by_slower(world: &mut World) {
    events_and_two_followers(world);
    followers_store_third_and_fifth(world);
    let third = after_event(world, 3);
    assert_eq!(mark(world), Some(third));
}

#[when("the slower follower stores a cursor past the faster one")]
fn slower_passes_faster(world: &mut World) {
    let seventh = after_event(world, 7);
    world.g().events().advance("slow", seventh).unwrap();
}

#[then("the graph's processed mark is the faster follower's cursor")]
fn mark_is_faster(world: &mut World) {
    let fifth = after_event(world, 5);
    assert_eq!(mark(world), Some(fifth));
}

#[given("a graph with events and a registered follower that has read them all")]
fn follower_read_all(world: &mut World) {
    events_around_cursor(world);
    let last = world.log.last().unwrap().cursor;
    let events = world.g().events();
    events.register("first").unwrap();
    events.advance("first", last).unwrap();
    assert_eq!(mark(world), Some(last));
}

#[when("a second follower registers without storing a cursor")]
fn second_registers(world: &mut World) {
    world.g().events().register("second").unwrap();
}

#[then("the graph's processed mark is the start of the events")]
fn mark_is_start(world: &mut World) {
    assert_eq!(mark(world), Some(Cursor::START));
}

#[given("a graph whose processed mark is held by one of two registered followers")]
fn mark_held_by_one(world: &mut World) {
    mark_held_by_slower(world);
}

#[when("that follower unregisters")]
fn holder_unregisters(world: &mut World) {
    world.g().events().unregister("slow").unwrap();
}

#[then("the graph's processed mark is the remaining follower's cursor")]
fn mark_is_remaining(world: &mut World) {
    let fifth = after_event(world, 5);
    assert_eq!(
        world.g().events().followers().unwrap(),
        vec![("fast".to_string(), fifth)]
    );
    assert_eq!(mark(world), Some(fifth));
}

// ---------- events: the committed watermark across a rebuild and a purge ----------

fn rebuilt_committed(world: &World) -> Cursor {
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    rebuilt.events().committed().unwrap()
}

#[given("a graph with committed events and a snapshot at the cursor of its last event")]
fn snapshot_at_last_event(world: &mut World) {
    events_around_cursor(world);
    snapshot_at_end(world);
}

#[then("the rebuilt graph's committed watermark equals the snapshot's cursor")]
fn rebuilt_watermark_is_snapshot(world: &mut World) {
    let cursor = world.snapshot.as_ref().unwrap().cursor();
    assert_eq!(rebuilt_committed(world), cursor);
}

#[given("a graph with a snapshot at a cursor and three committed events after it")]
fn snapshot_then_three(world: &mut World) {
    events_around_cursor(world);
    snapshot_at_end(world);
    for v in 10..13 {
        world
            .g()
            .write(&Write::new("eavesdrop").unit(Unit::new(
                "voice",
                format!("v{v}"),
                NONE,
                day(2026, 10, 14),
            )))
            .unwrap();
    }
    world.log = world.all_events();
}

#[then("the rebuilt graph's committed watermark is the cursor of the third event")]
fn rebuilt_watermark_is_third(world: &mut World) {
    let after = logged_after(world, world.cursor);
    assert_eq!(after.len(), 3);
    assert_eq!(rebuilt_committed(world), after[2].cursor);
}

/// A graph whose last event, at `World::cursor`, holds the one hearing of p-gone, the fact to
/// purge.
#[given("a graph whose last committed event is a hearing a purge rule matches, at cursor c")]
fn last_event_purgeable(world: &mut World) {
    let g = purge_graph(world);
    g.write(&Write::new("app").unit(person("p-other", "before")))
        .unwrap();
    g.write(&Write::new("app").unit(person("p-gone", TOKEN)))
        .unwrap();
    let hearing = &g.hearings(&NodeRef::new("person", "p-gone")).unwrap()[0];
    world.purge.targets.push(Purge::Fact {
        identity: hearing.identity,
    });
    world.cursor = world.all_events().last().unwrap().cursor;
    assert_eq!(world.g().events().committed().unwrap(), world.cursor);
}

#[when("the purge runs")]
fn purge_runs(world: &mut World) {
    purge_targets(world);
}

#[then("the committed watermark is the purge event's cursor, which is greater than c")]
fn watermark_is_purge_event(world: &mut World) {
    let purged = world.all_events().last().unwrap().clone();
    assert!(matches!(purged.change, Change::Purged { .. }), "{purged:?}");
    assert_eq!(world.g().events().committed().unwrap(), purged.cursor);
    assert!(purged.cursor > world.cursor);
}

#[given("a graph whose last event before cursor c was purged, so no event is at c")]
fn purged_at_cursor(world: &mut World) {
    last_event_purgeable(world);
    purge_targets(world);
    let c = world.cursor;
    assert!(world.all_events().iter().all(|e| e.cursor != c));
}

#[when("a snapshot is taken at c and a graph is rebuilt from it with no later events")]
fn snapshot_at_c_rebuilt(world: &mut World) {
    take_snapshot(world);
    rebuilt_from_snapshot_alone(world);
}

#[then("the rebuilt graph's committed watermark is at least c")]
fn rebuilt_watermark_at_least_c(world: &mut World) {
    assert!(rebuilt_committed(world) >= world.cursor);
}

const READER: &str = "reader";

#[given(
    "a graph with a registered follower whose stored cursor is after the second event, and a \
     snapshot at the cursor of the fifth event"
)]
fn follower_second_snapshot_fifth(world: &mut World) {
    events_around_cursor(world);
    let second = after_event(world, 2);
    let events = world.g().events();
    events.register(READER).unwrap();
    events.advance(READER, second).unwrap();
    world.cursor = after_event(world, 5);
    take_snapshot(world);
}

#[then("the follower's stored cursor in the rebuilt graph is the cursor after the second event")]
fn rebuilt_follower_at_second(world: &mut World) {
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    assert_eq!(
        rebuilt.events().followers().unwrap(),
        vec![(READER.to_string(), after_event(world, 2))]
    );
}

#[given(
    "a graph rebuilt from a snapshot, with a registered follower whose stored cursor is before \
     the snapshot's cursor"
)]
fn rebuilt_with_follower_behind(world: &mut World) {
    events_around_cursor(world);
    let second = after_event(world, 2);
    let events = world.g().events();
    events.register(READER).unwrap();
    events.advance(READER, second).unwrap();
    take_snapshot(world);
    rebuild_from_snapshot(world);
}

#[when("the app advances the follower to the snapshot's cursor")]
fn advances_to_snapshot(world: &mut World) {
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    if let Err(error) = rebuilt.events().advance(READER, world.cursor) {
        world.refusals.push(error);
    }
}

#[then("the advance is accepted and the follower's stored cursor is the snapshot's cursor")]
fn advance_accepted(world: &mut World) {
    assert!(world.refusals.is_empty(), "{:?}", world.refusals);
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    assert_eq!(
        rebuilt.events().followers().unwrap(),
        vec![(READER.to_string(), world.cursor)]
    );
}

// ---------- queue: a heartbeat commits the in-memory write queue ----------

/// A heartbeat long enough that no scenario races it: those scenarios commit by flushing.
const NEVER: Duration = Duration::from_secs(3_600);

/// Declares class `item` (property `v`) and hands the graph to a queue beating every `interval`.
fn queued(world: &mut World, interval: Duration) {
    class_with(world, ValueType::Text);
    let graph = world.graph.take().expect("the graph is open");
    world.queue = Some(Queue::open(graph, interval));
}

fn q(world: &World) -> &Queue {
    world.queue.as_ref().expect("the queue is open")
}

/// Queues a write of node `item` `key`, keeping its ticket.
fn enqueue(world: &mut World, key: &str) {
    let unit = Unit::new("item", key, [("v", key)], day(2026, 1, 1));
    let ticket = q(world).write(Write::new("app").unit(unit)).unwrap();
    world.tickets.push(ticket);
}

/// Whether another handle on the file reads node `item` `key`: it is committed.
fn committed_elsewhere(world: &World, key: &str) -> bool {
    rannoch::open(world.path(), Some(&KEY))
        .unwrap()
        .find("item", key)
        .unwrap()
        .is_some()
}

/// Waits up to five seconds for `done`.
fn wait_for(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the heartbeat"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[given("an open graph with its queue")]
fn graph_with_queue(world: &mut World) {
    queued(world, NEVER);
}

#[when("the app writes a node and the write returns")]
fn queue_writes(world: &mut World) {
    enqueue(world, "i1");
}

#[then("the committed watermark has not yet passed that write")]
fn watermark_not_passed(world: &mut World) {
    assert!(q(world).committed() < Some(world.tickets[0]));
    assert!(!committed_elsewhere(world, "i1"));
}

#[given("an open graph with an accepted write in its queue")]
fn graph_with_accepted_write(world: &mut World) {
    queued(world, Queue::INTERVAL);
    enqueue(world, "i1");
}

#[when("the next heartbeat runs")]
fn next_heartbeat(world: &mut World) {
    let ticket = world.tickets[0];
    wait_for(|| q(world).committed() >= Some(ticket));
}

#[then("the committed watermark has passed that write")]
fn watermark_passed(world: &mut World) {
    assert!(q(world).committed() >= Some(world.tickets[0]));
    assert!(committed_elsewhere(world, "i1"));
}

#[given("an open graph with a subscriber")]
fn graph_with_subscriber(world: &mut World) {
    queued(world, Queue::INTERVAL);
    let (path, heard) = (world.path(), Arc::clone(&world.heard));
    q(world).subscribe(move |cursor| {
        // What another handle reads up to the cursor the subscriber was given, as it is given.
        let committed: Vec<Event> = rannoch::open(&path, Some(&KEY))
            .unwrap()
            .events()
            .since(Cursor::START)
            .unwrap()
            .into_iter()
            .filter(|e| e.cursor <= cursor)
            .collect();
        heard.lock().unwrap().push((cursor, committed));
    });
}

#[when("the app writes a node and the subscriber receives its event")]
fn subscriber_receives(world: &mut World) {
    enqueue(world, "i1");
    let heard = Arc::clone(&world.heard);
    wait_for(|| !heard.lock().unwrap().is_empty());
}

#[then("the committed watermark has passed that event")]
fn watermark_passed_event(world: &mut World) {
    assert!(q(world).committed() >= Some(world.tickets[0]));
    let heard = world.heard.lock().unwrap();
    let (cursor, committed) = &heard[0];
    let wrote_i1 = |e: &Event| match &e.change {
        Change::Wrote { added, .. } => added
            .iter()
            .any(|f| matches!(f, Fact::Node(n) if n.key == "i1")),
        _ => false,
    };
    let event = committed
        .iter()
        .find(|e| wrote_i1(e))
        .expect("the event was committed when the subscriber received it");
    assert!(event.cursor <= *cursor);
}

#[given("an open graph with accepted writes in its queue")]
fn graph_with_accepted_writes(world: &mut World) {
    queued(world, NEVER);
    for key in ["i1", "i2", "i3"] {
        enqueue(world, key);
    }
}

#[when("the app flushes")]
fn app_flushes(world: &mut World) {
    let (committed, failed) = q(world).flush();
    world.flushed = committed;
    world.failed = written(failed);
}

#[then("every write accepted before the flush is committed when the flush returns")]
fn flushed_committed(world: &mut World) {
    assert!(world.flushed >= world.tickets.last().copied());
    for key in ["i1", "i2", "i3"] {
        assert!(committed_elsewhere(world, key), "{key} is not committed");
    }
}

// ---------- write: a heartbeat's batch is one transaction, a savepoint per write ----------

/// Makes the file refuse node `item` `key` at commit, as a full disk would; the statement aborts
/// and the transaction around it stays open.
fn item_fails_at_commit(world: &World, key: &str) {
    rusqlite::Connection::open(world.path())
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER {key}_fails BEFORE INSERT ON \"class:item\" \
             WHEN NEW.key = '{key}' BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END;"
        ))
        .unwrap();
}

#[given("an open graph with a batch of writes in its queue, one of which fails when committed")]
fn batch_with_failing_write(world: &mut World) {
    queued(world, NEVER);
    item_fails_at_commit(world, "bad");
    for key in ["i1", "bad", "i3"] {
        enqueue(world, key);
    }
}

#[when("the heartbeat commits the batch")]
fn heartbeat_commits_batch(world: &mut World) {
    world.flushed = q(world).flush().0;
}

#[then("every other write in the batch is committed")]
fn others_committed(world: &mut World) {
    assert!(world.flushed >= world.tickets.last().copied());
    for key in ["i1", "i3"] {
        assert!(committed_elsewhere(world, key), "{key} is not committed");
    }
    assert!(!committed_elsewhere(world, "bad"));
}

/// The batch's size in changes: more than three pieces' worth.
const LARGE_BATCH: usize = 3 * PIECE_CHANGES + 1;

#[given("an open graph with more than 1,000 changes in its queue")]
fn graph_with_large_batch(world: &mut World) {
    queued(world, NEVER);
    let mut write = Write::new("app");
    for i in 0..LARGE_BATCH {
        let key = format!("i{i}");
        write = write.unit(Unit::new(
            "item",
            key.clone(),
            [("v", key)],
            day(2026, 1, 1),
        ));
    }
    let ticket = q(world).write(write).unwrap();
    world.tickets.push(ticket);
}

#[when("the heartbeat commits the batch while a reader reads")]
fn commits_while_reading(world: &mut World) {
    let (path, done) = (world.path(), Arc::clone(&world.burst_done));
    let last = format!("i{}", LARGE_BATCH - 1);
    let reader = std::thread::spawn(move || {
        let reader = rannoch::open(path, Some(&KEY)).unwrap();
        let (mut reads, mut partial, mut whole) = (0, 0, 0);
        loop {
            let finished = done.load(Ordering::SeqCst);
            // The first unit, then the last: once the first is visible, all of it must be.
            let first = reader.find("item", "i0").unwrap().is_some();
            let last = reader.find("item", &last).unwrap().is_some();
            reads += 1;
            partial += usize::from(first && !last);
            whole += usize::from(first && last);
            if finished {
                return (reads, partial, whole);
            }
        }
    });
    world.flushed = q(world).flush().0;
    world.burst_done.store(true, Ordering::SeqCst);
    world.reads = reader.join().unwrap();
}

#[then("the reader sees either none of the batch or all of it")]
fn none_or_all(world: &mut World) {
    let (reads, partial, whole) = world.reads;
    assert!(world.flushed >= world.tickets.last().copied());
    assert_eq!(
        partial, 0,
        "{partial} of {reads} reads saw part of the batch"
    );
    assert!(whole > 0, "no read saw the whole batch");
}

/// Declares class `item` (property `v`) and relation `next` from item to item.
fn items_with_next(world: &mut World) {
    class_with(world, ValueType::Text);
    world
        .g()
        .declare(&Declaration::Relation(Relation::new(
            "next", "item", "item", false,
        )))
        .unwrap();
}

#[given("a queue holding a node and then a link from it, where the node will fail at commit")]
fn link_after_failing_node(world: &mut World) {
    items_with_next(world);
    let graph = world.graph.take().expect("the graph is open");
    world.queue = Some(Queue::open(graph, NEVER));
    item_fails_at_commit(world, "bad");
    enqueue(world, "bad");
    let unit = Unit::new("item", "i2", [("v", "i2")], day(2026, 1, 1)).link(
        "next",
        "bad",
        "i2",
        day(2026, 1, 1),
    );
    let ticket = q(world).write(Write::new("app").unit(unit)).unwrap();
    world.tickets.push(ticket);
}

#[then("the link is not in the graph")]
fn link_not_in_graph(world: &mut World) {
    assert!(world.flushed >= world.tickets.last().copied());
    let g = rannoch::open(world.path(), Some(&KEY)).unwrap();
    assert!(g.find("item", "bad").unwrap().is_none());
    assert!(g.find_link("next", "bad", "i2").unwrap().is_none());
    assert!(g.open_links().unwrap().is_empty(), "the link was left open");
    assert!(g.find("item", "i2").unwrap().is_none());
}

// ---------- write: a link's source must be heard; its target may wait ----------

/// Node `item` `key`, valid from 2026-01-01.
fn item(key: &str) -> Unit {
    Unit::new("item", key, [("v", key)], day(2026, 1, 1))
}

#[given("a graph in which a key has never been heard")]
fn key_never_heard(world: &mut World) {
    items_with_next(world);
    assert!(world.g().find("item", "ghost").unwrap().is_none());
    assert!(
        world
            .g()
            .hearings(&NodeRef::new("item", "ghost"))
            .unwrap()
            .is_empty()
    );
}

#[when("the app writes a link from that key")]
fn writes_link_from_unheard(world: &mut World) {
    world.mark_events();
    let write = Write::new("app").unit(item("i1").link("next", "ghost", "i1", day(2026, 1, 1)));
    world.write_error = Some(world.g().write(&write).expect_err("the write is refused"));
}

#[then("the write is refused naming that key")]
fn refused_naming_unheard_key(world: &mut World) {
    let error = world.write_error.as_ref().expect("the write was refused");
    assert!(error.events.is_empty(), "{error:?}");
    assert!(
        matches!(&error.cause, Error::Refused(why)
            if why == "relation next links from item ghost, which has never been heard"),
        "{error:?}"
    );
    assert_eq!(world.all_events().len(), world.events_before);
    assert!(world.g().find("item", "i1").unwrap().is_none());
    assert!(world.g().open_links().unwrap().is_empty());
}

#[when("the app writes a node and a link from it in the same write")]
fn writes_node_and_link_together(world: &mut World) {
    items_with_next(world);
    // The link comes before its source's unit, so the source is heard only later in the write.
    let write = Write::new("app")
        .unit(item("i2").link("next", "i1", "i2", day(2026, 1, 1)))
        .unit(item("i1"));
    world.write(&write);
}

#[then("the link is in the graph")]
fn link_in_graph(world: &mut World) {
    assert!(world.g().find_link("next", "i1", "i2").unwrap().is_some());
    assert!(world.g().open_links().unwrap().is_empty());
}

#[given("a process that has written a node not yet committed")]
fn process_wrote_uncommitted_node(world: &mut World) {
    items_with_next(world);
    let graph = world.graph.take().expect("the graph is open");
    world.queue = Some(Queue::open(graph, NEVER));
    enqueue(world, "i1");
    assert_eq!(q(world).committed(), None);
    assert!(!committed_elsewhere(world, "i1"));
}

#[when("the process writes a link from that node")]
fn process_writes_link(world: &mut World) {
    let write = Write::new("app").unit(item("i2").link("next", "i1", "i2", day(2026, 1, 1)));
    let ticket = q(world).write(write).unwrap();
    world.tickets.push(ticket);
    let (flushed, failures) = q(world).flush();
    assert!(failures.is_empty(), "{failures:?}");
    world.flushed = flushed;
}

#[then("the link is accepted")]
fn link_accepted(world: &mut World) {
    assert!(world.flushed >= world.tickets.last().copied());
    let g = rannoch::open(world.path(), Some(&KEY)).unwrap();
    assert!(g.find_link("next", "i1", "i2").unwrap().is_some());
}

#[given("a graph with a heard node and a key never heard")]
fn heard_node_and_unheard_key(world: &mut World) {
    items_with_next(world);
    world.write(&Write::new("app").unit(item("i1")));
    assert!(world.g().find("item", "ghost").unwrap().is_none());
}

#[when("the app writes a link from the node to that key")]
fn writes_link_to_unheard(world: &mut World) {
    world.write(&Write::new("app").unit(item("i1").link("next", "i1", "ghost", day(2026, 1, 1))));
}

#[then("the link is listed as open")]
fn listed_as_open(world: &mut World) {
    assert_eq!(
        world.g().open_links().unwrap(),
        [OpenLink {
            relation: "next".into(),
            from: NodeRef::new("item", "i1"),
            to: NodeRef::new("item", "ghost"),
            waits_on: NodeRef::new("item", "ghost"),
        }]
    );
}

// ---------- queue: a write that fails at commit is reported, never dropped in silence ----------

/// The tickets of `failures`, each a write that failed at commit.
fn written(failures: Vec<Failure>) -> Vec<Ticket> {
    failures
        .into_iter()
        .map(|failure| match failure {
            Failure::Write { ticket, .. } => ticket,
            other => panic!("expected a failed write, got {other:?}"),
        })
        .collect()
}

#[given("a process whose queue holds writes whose final commit will fail")]
fn final_commit_will_fail(world: &mut World) {
    queued(world, NEVER);
    // The file rolls back the whole transaction, as a full disk can: the final commit fails.
    rusqlite::Connection::open(world.path())
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER bad_rolls_back BEFORE INSERT ON \"class:item\" \
             WHEN NEW.key = 'bad' BEGIN SELECT RAISE(ROLLBACK, 'database or disk is full'); END;",
        )
        .unwrap();
    for key in ["i1", "bad", "i3"] {
        enqueue(world, key);
    }
}

#[when("the process closes the queue")]
fn process_closes(world: &mut World) {
    app_closes(world);
}

#[then("the process is told the final commit failed")]
fn told_final_commit_failed(world: &mut World) {
    assert_eq!(world.failed, world.tickets);
    for key in ["i1", "bad", "i3"] {
        assert!(!committed_elsewhere(world, key), "{key} is committed");
    }
}

#[given("a queue holding a write that will fail at commit")]
fn write_will_fail(world: &mut World) {
    queued(world, Queue::INTERVAL);
    item_fails_at_commit(world, "bad");
    enqueue(world, "i1");
    enqueue(world, "bad");
}

#[when("the app closes the queue")]
fn app_closes(world: &mut World) {
    let queue = world.queue.take().expect("the queue is open");
    world.failed = written(queue.close());
}

#[when("the heartbeat commits and the app takes the queue's failures")]
fn heartbeat_then_take(world: &mut World) {
    let ticket = world.tickets[1];
    wait_for(|| q(world).committed() >= Some(ticket));
    world.failed = written(q(world).take_failures());
}

/// The failing write, and only it, is reported, and it is not in the graph.
fn bad_reported(world: &World) {
    assert_eq!(world.failed, [world.tickets[1]]);
    assert!(!committed_elsewhere(world, "bad"));
    assert!(committed_elsewhere(world, "i1"));
}

#[then("the flush returns that write as failed")]
fn flush_returns_failed(world: &mut World) {
    bad_reported(world);
}

#[then("the close returns that write as failed")]
fn close_returns_failed(world: &mut World) {
    bad_reported(world);
}

#[then("the app receives that write as failed")]
fn app_receives_failed(world: &mut World) {
    bad_reported(world);
}

#[then("the flush returns the node and the link as failed")]
fn flush_returns_dependents(world: &mut World) {
    assert_eq!(world.failed, world.tickets);
}

#[then("the close returns the node and the link as failed")]
fn close_returns_dependents(world: &mut World) {
    assert_eq!(world.failed, world.tickets);
}

// ---------- queue: dropping or closing the queue commits what it holds ----------

/// How the short-lived process ends, and the graph file it writes to.
const PROCESS_END: &str = "RANNOCH_PROCESS_END";
const PROCESS_FILE: &str = "RANNOCH_PROCESS_FILE";
const PROCESS_PANIC: &str = "the short-lived process panics";
/// The code the process exits with when it does not unwind, so the scenario knows it got there.
const PROCESS_EXIT: i32 = 3;
const PROCESS_KEYS: [&str; 3] = ["p1", "p2", "p3"];

/// The short-lived process: run only by [`run_process`], in a fresh process. It writes to the
/// graph through a queue whose heartbeat never beats, then ends as `PROCESS_END` says.
#[test]
#[ignore = "run in its own process by the FR-RANNOCH-061 scenarios"]
fn short_lived_process() {
    let graph = rannoch::open(std::env::var(PROCESS_FILE).unwrap(), Some(&KEY)).unwrap();
    let queue = Queue::open(graph, NEVER);
    for key in PROCESS_KEYS {
        let unit = Unit::new("item", key, [("v", key)], day(2026, 1, 1));
        queue.write(Write::new("app").unit(unit)).unwrap();
    }
    match std::env::var(PROCESS_END).unwrap().as_str() {
        "return" => {}
        "panic" => panic!("{PROCESS_PANIC}"),
        "exit" => {
            assert!(queue.close().is_empty());
            std::process::exit(PROCESS_EXIT);
        }
        other => panic!("unknown end {other}"),
    }
}

/// Runs [`short_lived_process`] in a fresh process on the world's graph, ending as `end` says,
/// and returns its exit code and its output.
fn run_process(world: &World, end: &str) -> (Option<i32>, String) {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "short_lived_process", "--ignored", "--nocapture"])
        .env(PROCESS_END, end)
        .env(PROCESS_FILE, world.path())
        .output()
        .unwrap();
    let output = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code(), output)
}

#[given("a short-lived process that writes to a graph")]
fn short_lived_process_graph(world: &mut World) {
    class_with(world, ValueType::Text);
    world.graph = None;
}

#[when("the process returns")]
fn process_returns(world: &mut World) {
    let (code, output) = run_process(world, "return");
    assert_eq!(code, Some(0), "the process did not return:\n{output}");
    assert!(
        output.contains("1 passed"),
        "the process ran no test:\n{output}"
    );
}

#[when("the process panics with unwinding")]
fn process_panics(world: &mut World) {
    let (code, output) = run_process(world, "panic");
    assert_ne!(code, Some(0), "the process did not panic:\n{output}");
    assert!(
        output.contains(PROCESS_PANIC),
        "the process did not panic:\n{output}"
    );
}

#[when("the process closes the queue and then exits without unwinding")]
fn process_closes_and_exits(world: &mut World) {
    let (code, output) = run_process(world, "exit");
    assert_eq!(
        code,
        Some(PROCESS_EXIT),
        "the process did not exit:\n{output}"
    );
}

#[then("every write it made is committed")]
fn process_writes_committed(world: &mut World) {
    for key in PROCESS_KEYS {
        assert!(committed_elsewhere(world, key), "{key} is not committed");
    }
}

// ---------- events: another process follows commits through the committed watermark ----------

/// How the follower process reads (`poll` until it sees the last key, or `once`), and the file.
const FOLLOW_MODE: &str = "RANNOCH_FOLLOW_MODE";
const FOLLOW_FILE: &str = "RANNOCH_FOLLOW_FILE";
/// The key the writer writes last, after which a polling follower stops.
const FOLLOW_LAST: &str = "last";

/// What the follower process reported: each watermark it read, and each event it took, by id,
/// with the keys of the nodes it wrote.
#[derive(Debug, Default)]
struct Followed {
    marks: Vec<Cursor>,
    events: Vec<(EventId, Vec<String>)>,
}

/// The keys of the nodes an event wrote.
fn node_keys(event: &Event) -> Vec<String> {
    match &event.change {
        Change::Wrote { added, .. } => added
            .iter()
            .filter_map(|fact| match fact {
                Fact::Node(node) => Some(node.key.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The follower: run only by [`spawn_follower`], in its own process. Each read takes the
/// committed watermark, the events after its cursor up to it, and moves its cursor to it; it
/// prints each mark and each event as one line of JSON. Polling, it stops one read after the
/// read that took the last key, so an event taken twice would show.
#[test]
#[ignore = "run in its own process by the FR-RANNOCH-064 scenarios"]
fn follower_process() {
    let graph = rannoch::open(std::env::var(FOLLOW_FILE).unwrap(), Some(&KEY)).unwrap();
    let once = std::env::var(FOLLOW_MODE).unwrap() == "once";
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut cursor = Cursor::START;
    let mut seen_last = false;
    loop {
        let finishing = once || seen_last;
        let mark = graph.events().committed().unwrap();
        println!("{}", serde_json::json!({ "mark": mark }));
        for event in graph.events().since_until(cursor, mark, |_| true).unwrap() {
            let keys = node_keys(&event);
            seen_last |= keys.iter().any(|k| k == FOLLOW_LAST);
            println!("{}", serde_json::json!({ "event": event.id, "keys": keys }));
        }
        cursor = mark;
        if finishing {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the follower never saw {FOLLOW_LAST}"
        );
        std::thread::sleep(Duration::from_millis(if seen_last { 50 } else { 5 }));
    }
}

/// Starts [`follower_process`] in its own process on the world's graph, reading as `mode` says.
fn spawn_follower(world: &mut World, mode: &str) {
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "follower_process", "--ignored", "--nocapture"])
        .env(FOLLOW_MODE, mode)
        .env(FOLLOW_FILE, world.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    world.follower = Some(child);
}

/// Waits for the follower process to end and keeps what it reported.
fn followed(world: &mut World) {
    let out = world
        .follower
        .take()
        .expect("the follower is running")
        .wait_with_output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let output = format!("{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "the follower failed:\n{output}");
    assert!(
        output.contains("1 passed"),
        "the follower ran no test:\n{output}"
    );
    for line in stdout.lines().filter(|l| l.starts_with('{')) {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        if let Some(mark) = value.get("mark") {
            world
                .followed
                .marks
                .push(serde_json::from_value(mark.clone()).unwrap());
        } else {
            world.followed.events.push((
                serde_json::from_value(value["event"].clone()).unwrap(),
                serde_json::from_value(value["keys"].clone()).unwrap(),
            ));
        }
    }
}

#[given("a writer process and a follower in another process reading the committed watermark")]
fn writer_and_follower(world: &mut World) {
    queued(world, NEVER);
    spawn_follower(world, "poll");
}

#[when("the writer commits events")]
fn writer_commits(world: &mut World) {
    for batch in [
        ["c1", "c2", "c3"],
        ["c4", "c5", "c6"],
        ["c7", "c8", FOLLOW_LAST],
    ] {
        for key in batch {
            enqueue(world, key);
        }
        let (_, failed) = q(world).flush();
        assert!(failed.is_empty(), "{failed:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[then("the follower sees each committed event exactly once")]
fn follower_sees_each_once(world: &mut World) {
    followed(world);
    let committed: Vec<EventId> = rannoch::open(world.path(), Some(&KEY))
        .unwrap()
        .events()
        .since(Cursor::START)
        .unwrap()
        .into_iter()
        .map(|e| e.id)
        .collect();
    let seen: Vec<EventId> = world.followed.events.iter().map(|(id, _)| *id).collect();
    assert_eq!(seen, committed, "the follower saw a different log");
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for key in world.followed.events.iter().flat_map(|(_, keys)| keys) {
        *counts.entry(key).or_default() += 1;
    }
    for key in ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", FOLLOW_LAST] {
        assert_eq!(counts.get(key), Some(&1), "{key}: {counts:?}");
    }
    assert!(
        world
            .followed
            .marks
            .windows(2)
            .filter(|w| w[0] < w[1])
            .count()
            > 1,
        "the follower saw the commits land in one read, not follow them: {:?}",
        world.followed.marks
    );
}

#[given(
    "a writer process with accepted writes not yet committed, and a follower in another process"
)]
fn writer_with_uncommitted(world: &mut World) {
    queued(world, NEVER);
    world.cursor = rannoch::open(world.path(), Some(&KEY))
        .unwrap()
        .events()
        .committed()
        .unwrap();
    for key in ["u1", "u2", "u3"] {
        enqueue(world, key);
    }
}

#[when("the follower reads the committed watermark")]
fn follower_reads_watermark(world: &mut World) {
    spawn_follower(world, "once");
    followed(world);
}

#[then("the follower sees none of the uncommitted writes")]
fn follower_sees_no_uncommitted(world: &mut World) {
    assert_eq!(q(world).committed(), None, "the writes were committed");
    assert_eq!(world.followed.marks, vec![world.cursor]);
    let keys: Vec<&String> = world.followed.events.iter().flat_map(|(_, k)| k).collect();
    assert!(keys.is_empty(), "the follower saw {keys:?}");
}

// ---------- history: every write's point, and hearings by content (FR-RANNOCH-055, FR-RANNOCH-065) ----------

/// A content-identity key other than the one the scenarios open their graphs under.
const OTHER_KEY: [u8; 32] = *b"another app's identity key here!";

/// A time a writer supplies: `minute` minutes after a fixed instant.
fn at(minute: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_790_000_000 + minute * 60)
}

/// Declares the class `kind` names, with one text property: `visit`, keyed by a time the writer
/// supplies; `reading`, keyed by an integer the writer supplies; or `note`, with no key.
fn declare_kind(g: &mut Graph, kind: &str) {
    let (name, history) = match kind {
        "time-keyed" => ("visit", HistoryKey::Time),
        "integer-keyed" => ("reading", HistoryKey::Integer),
        "keyless" => ("note", HistoryKey::Origin),
        other => panic!("no class kind {other}"),
    };
    let mut class = Class::new(name, ["text"]);
    class.history = history;
    g.declare(&Declaration::Class(class)).unwrap();
}

/// The graph's one class.
fn the_class(g: &Graph) -> Class {
    let classes = &g.ontology().classes;
    assert_eq!(classes.len(), 1, "{classes:?}");
    classes.values().next().unwrap().clone()
}

/// A node of the graph's one class keyed `key` and carrying `text`, with the point or the
/// integer key `n` where its class is keyed by one.
fn unit_of(g: &Graph, key: &str, text: &str, n: u64) -> Unit {
    let class = the_class(g);
    let unit = Unit::new(class.name, key, [("text", text)], day(2026, 10, 1));
    match class.history {
        HistoryKey::Time => unit.at(at(n)),
        HistoryKey::Integer => unit.integer(i64::try_from(n).unwrap()),
        HistoryKey::Origin => unit,
    }
}

/// Every hearing of each node of the graph's one class, by key.
fn hearings_by_key(g: &Graph) -> BTreeMap<String, Vec<Hearing>> {
    let class = the_class(g).name;
    ["h0", "h1", "h2", "k1"]
        .into_iter()
        .map(|key| {
            let hearings = g.hearings(&NodeRef::new(&class, key)).unwrap();
            (key.to_string(), hearings)
        })
        .filter(|(_, hearings)| !hearings.is_empty())
        .collect()
}

/// A graph of the class `kind` names, holding nodes h0, h1 and h2 each heard from two sources,
/// a few milliseconds apart so their record times differ. Keeps every hearing as it reads now.
fn hearings_of_kind(world: &mut World, kind: &str) {
    declare_kind(world.open(), kind);
    for (round, source) in [(0, "first"), (1, "second")] {
        for (i, key) in ["h0", "h1", "h2"].into_iter().enumerate() {
            let unit = unit_of(world.g(), key, source, round * 10 + i as u64);
            world.g().write(&Write::new(source).unit(unit)).unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    world.hearings = hearings_by_key(world.g());
    assert_eq!(world.hearings.len(), 3);
    assert!(world.hearings.values().all(|h| h.len() == 2));
}

#[given("a graph with hearings of a {kind} class")]
fn graph_with_hearings(world: &mut World, kind: String) {
    hearings_of_kind(world, &kind);
}

#[given("a graph with hearings of a {kind} class and a snapshot after them")]
fn graph_with_hearings_and_snapshot(world: &mut World, kind: String) {
    hearings_of_kind(world, &kind);
    snapshot_at_end(world);
}

#[when("the graph is rebuilt from its events")]
fn rebuilt_from_events(world: &mut World) {
    let events = world.all_events();
    let path = world.dir.path().join("rebuilt.rannoch");
    world.rebuilt = Some(rannoch::rebuild(&events, path, Some(&KEY)).unwrap());
}

#[then("every hearing reads back the point it had before the rebuild")]
#[then("every hearing reads back the same point in both graphs")]
fn hearings_survive(world: &mut World) {
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    assert_eq!(hearings_by_key(rebuilt), world.hearings);
}

#[given(
    "the events of a graph holding hearings of a class with no key, recorded at different times"
)]
fn keyless_events(world: &mut World) {
    hearings_of_kind(world, "keyless");
    let points: BTreeSet<SystemTime> = world.hearings.values().flatten().map(|h| h.point).collect();
    assert_eq!(points.len(), 6, "each hearing was recorded at its own time");
    world.log = world.all_events();
}

#[when("a new graph is given the same events in reverse order")]
fn given_in_reverse(world: &mut World) {
    let (declared, wrote): (Vec<Event>, Vec<Event>) = world
        .log
        .iter()
        .cloned()
        .partition(|e| matches!(e.change, Change::Declared(_)));
    let events: Vec<Event> = declared
        .into_iter()
        .chain(wrote.into_iter().rev())
        .collect();
    let path = world.dir.path().join("reversed.rannoch");
    world.rebuilt = Some(rannoch::rebuild(&events, path, Some(&KEY)).unwrap());
}

// ---------- history: the walk back to first heard (FR-RANNOCH-069) ----------

/// Writes `unit` from `source`, then waits a few milliseconds so the next hearing's record
/// time differs.
fn hear(world: &mut World, source: &str, unit: Unit) {
    world.write(&Write::new(source).unit(unit));
    std::thread::sleep(Duration::from_millis(2));
}

#[given("a node heard more than once, not in axis order")]
fn heard_out_of_axis_order(world: &mut World) {
    declare_kind(world.open(), "integer-keyed");
    for n in [30, 10, 40, 5, 20] {
        let unit = unit_of(world.g(), "r1", &format!("at {n}"), n);
        hear(world, "meter", unit);
    }
    world.walking = Some(NodeRef::new("reading", "r1"));
}

#[given("a node heard under one key and then replaced by a new key")]
fn heard_then_replaced(world: &mut World) {
    open_graph(world);
    let unit = Unit::new("statement", "s8", [("text", "first")], day(2021, 3, 2));
    hear(world, "swtor", unit);
    let hearings = world
        .g()
        .hearings(&NodeRef::new("statement", "s8"))
        .unwrap();
    assert_eq!(hearings.len(), 1, "{hearings:?}");
    world.hearings.insert("s8".into(), hearings);
    s9(world);
    replaces_s8_by_s9(world);
    world.walking = Some(NodeRef::new("statement", "s9"));
}

#[given("a node first heard before a snapshot and heard again from another source after it")]
fn heard_across_snapshot(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    let unit = unit_of(world.g(), "a", "before", 10);
    hear(world, "crawler", unit);
    let node = NodeRef::new("visit", "a");
    let hearings = world.g().hearings(&node).unwrap();
    world.hearings.insert("before".into(), hearings);
    snapshot_at_end(world);
    let unit = unit_of(world.g(), "a", "after", 20);
    hear(world, "reddit", unit);
    rebuild_from_snapshot(world);
    world.walking = Some(node);
}

#[given("a node that has been heard from more than one source, with no snapshot taken")]
fn heard_from_sources(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    for (source, n, month) in [("crawler", 30, 4), ("reddit", 10, 9), ("swtor", 20, 1)] {
        let unit = Unit::new("visit", "a", [("text", source)], day(2025, month, 1)).at(at(n));
        hear(world, source, unit);
    }
    world.walking = Some(NodeRef::new("visit", "a"));
}

#[when("the app walks its history")]
#[when("the app walks the new key's history")]
fn walks_history(world: &mut World) {
    let node = world.walking.clone().expect("a scenario names the node");
    let g = world.rebuilt.as_ref().or(world.graph.as_ref()).unwrap();
    let hearings = g.hearings(&node).unwrap();
    world.hearings.insert("walked".into(), hearings);
}

#[then("the walk returns every hearing in order on the class's declared axis")]
fn walk_in_axis_order(world: &mut World) {
    let integers: Vec<Option<i64>> = world.hearings["walked"].iter().map(|h| h.integer).collect();
    assert_eq!(integers, [5, 10, 20, 30, 40].map(Some));
}

#[then("the walk reaches first heard under the old key")]
fn walk_reaches_old_key(world: &mut World) {
    let walked = &world.hearings["walked"];
    assert_eq!(walked.len(), 2, "{walked:?}");
    assert_eq!(walked[0], world.hearings["s8"][0]);
}

#[then("the walk reaches the first hearing recorded before the snapshot")]
fn walk_reaches_before_snapshot(world: &mut World) {
    let walked = &world.hearings["walked"];
    assert_eq!(walked.len(), 2, "{walked:?}");
    assert_eq!(walked[0], world.hearings["before"][0]);
    assert_eq!(walked[1].source, "reddit");
}

#[then("each hearing returned matches the event that recorded it")]
fn hearings_match_events(world: &mut World) {
    let node = world.walking.clone().unwrap();
    let walked = world.hearings["walked"].clone();
    assert_eq!(walked.len(), 3, "{walked:?}");
    let events = world.all_events();
    let g = world.g();
    for hearing in &walked {
        let event = events.iter().find(|e| e.id == hearing.event).unwrap();
        let Change::Wrote { source, added, .. } = &event.change else {
            panic!("{event:?} is not a write");
        };
        let fact = added
            .iter()
            .find_map(|fact| match fact {
                Fact::Node(n) if n.class == node.class && n.key == node.key => Some(n),
                _ => None,
            })
            .unwrap();
        let millis = hearing
            .point
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        assert_eq!(source, &hearing.source);
        assert_eq!(fact.valid, hearing.valid);
        assert_eq!(fact.time, Some(i64::try_from(millis.as_millis()).unwrap()));
        let version = g.state_at(&node, At::Time(hearing.point)).unwrap().unwrap();
        assert_eq!(version.event, hearing.event);
        assert_eq!(fact.properties, version.properties);
    }
}

/// `tests/fixtures/store-0.2.0.rannoch` was written by rannoch at tag v0.2.0: class book (no
/// history key) with title (text) and pages (integer), class reading keyed by an integer, class
/// visit keyed by a time, relation of from reading to book; books b1 and b2, readings r1 and r2
/// each linked to a book, visit v1; 7 events, none carrying a point.
#[given("a store written by rannoch 0.2.0 with classes and hearings")]
fn store_020(world: &mut World) {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/store-0.2.0.rannoch"
    );
    std::fs::copy(fixture, world.path()).unwrap();
}

#[then("every event it held is still read and each hearing reads back a point")]
fn store_020_whole(world: &mut World) {
    assert_eq!(world.all_events().len(), 7);
    let nodes = [
        ("book", "b1"),
        ("book", "b2"),
        ("reading", "r1"),
        ("reading", "r2"),
        ("visit", "v1"),
    ];
    hearings_read_back_points(world, &nodes);
    for (from, to) in [("r1", "b1"), ("r2", "b2")] {
        let hearings = world.g().link_hearings("of", from, to).unwrap();
        assert_eq!(hearings.len(), 1, "{from} {to}: {hearings:?}");
        assert!(hearings[0].point > SystemTime::UNIX_EPOCH);
    }
}

#[given("a class declared with a time key the writer supplies")]
#[given("a class whose history is ordered by a time the writer supplies")]
fn time_keyed_class(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    world.mark_events();
}

#[given("a class declared with an integer key the writer supplies")]
#[given("a class whose history is ordered by an integer key the writer supplies")]
#[given("a class whose history is ordered by an integer the writer supplies")]
fn integer_keyed_class(world: &mut World) {
    declare_kind(world.open(), "integer-keyed");
    world.mark_events();
}

#[given("a class declared without a history key")]
fn keyless_class(world: &mut World) {
    declare_kind(world.open(), "keyless");
}

#[when("the app writes a node of that class without a time")]
#[when("the app writes a node of that class without an integer key")]
fn writes_without_its_key(world: &mut World) {
    let class = the_class(world.g()).name;
    write_refusing(
        world,
        Unit::new(class, "k1", [("text", "words")], day(2026, 10, 1)),
    );
}

#[then("the write is refused when it is written, naming the class and the missing key")]
fn refused_naming_key(world: &mut World) {
    let class = the_class(world.g());
    let missing = match class.history {
        HistoryKey::Time => "a time",
        HistoryKey::Integer => "an integer key",
        HistoryKey::Origin => panic!("class {} is not keyed", class.name),
    };
    let error = world.write_error.as_ref().expect("the write was refused");
    assert!(error.events.is_empty(), "{error:?}");
    let Error::Refused(why) = &error.cause else {
        panic!("expected a refusal, got {:?}", error.cause);
    };
    assert!(why.contains(&class.name) && why.contains(missing), "{why}");
    assert_eq!(world.all_events().len(), world.events_before);
}

#[given("one hearing's content delivered twice, under event ids A and B, A the smaller")]
fn delivered_twice(world: &mut World) {
    if world.graph.is_none() {
        keyless_class(world);
    }
    let unit = unit_of(world.g(), "k1", "words", 1);
    world.write(&Write::new("log").unit(unit));
    let a = world.written[0].clone();
    // B carries A's content under an id minted later, so larger, at its own place in the log.
    let later = unit_of(world.g(), "k2", "words", 2);
    world.write(&Write::new("log").unit(later));
    let b = Event {
        id: world.written[0].id,
        cursor: world.written[0].cursor,
        change: a.change.clone(),
    };
    assert!(a.id < b.id);
    world.log = world
        .all_events()
        .into_iter()
        .filter(|e| matches!(e.change, Change::Declared(_)))
        .chain([a, b])
        .collect();
}

#[when("a new graph is given A then B")]
fn given_a_then_b(world: &mut World) {
    let path = world.dir.path().join("given.rannoch");
    world.rebuilt = Some(rannoch::rebuild(&world.log, path, Some(&KEY)).unwrap());
}

#[then("the node has one hearing, and B is held as an alias of it")]
fn b_is_an_alias(world: &mut World) {
    let (a, b) = (
        world.log[world.log.len() - 2].id,
        world.log[world.log.len() - 1].id,
    );
    let given = world
        .rebuilt
        .as_ref()
        .expect("the new graph was given A and B");
    let hearings = given.hearings(&NodeRef::new("visit", "k1")).unwrap();
    assert_eq!(hearings.len(), 1, "{hearings:?}");
    assert_eq!(hearings[0].event, a);
    assert_eq!(hearings[0].aliases, vec![b]);
}

#[given("a node heard once from one source")]
fn heard_once(world: &mut World) {
    let unit = unit_of(world.g(), "k1", "words", 1);
    world.write(&Write::new("crawler").unit(unit));
    world.first = world.written.clone();
    world.hearings = hearings_by_key(world.g());
    world.mark_events();
}

#[when("the same source writes the node again later with the same valid time and properties")]
fn writes_again_later(world: &mut World) {
    std::thread::sleep(Duration::from_millis(5));
    let unit = unit_of(world.g(), "k1", "words", 1);
    world.write(&Write::new("crawler").unit(unit));
}

#[then(
    "the number of events is unchanged, the node has one hearing at the point it had before, and the second write returns the first write's event id"
)]
fn rewrite_adds_nothing(world: &mut World) {
    assert_eq!(world.all_events().len(), world.events_before);
    assert_eq!(hearings_by_key(world.g()), world.hearings);
    assert_eq!(world.hearings["k1"].len(), 1);
    let ids = |events: &[Event]| events.iter().map(|e| e.id).collect::<Vec<_>>();
    assert_eq!(ids(&world.written), ids(&world.first));
}

#[when(
    "the app writes the node again from the same source at the same point with one property changed"
)]
fn writes_changed_account(world: &mut World) {
    let unit = unit_of(world.g(), "k1", "other words", 1);
    world.write(&Write::new("crawler").unit(unit));
}

#[then("the node has two hearings")]
fn two_hearings(world: &mut World) {
    let hearings = hearings_by_key(world.g()).remove("k1").unwrap();
    assert_eq!(hearings.len(), 2, "{hearings:?}");
}

#[given("a node that has been heard from one source")]
fn node_heard(world: &mut World) {
    declare_kind(world.open(), "keyless");
    let unit = unit_of(world.g(), "k1", "words", 0);
    world.write(&Write::new("swtor").unit(unit.clone()));
    world.restate = Some(unit);
    world.found = Some(world.find("note", "k1"));
}

#[given("a link that has been heard from one source")]
fn link_heard(world: &mut World) {
    link_heard_valid(world, day(2021, 3, 2));
}

fn link_heard_valid(world: &mut World, when: ValidTime) {
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    let statement = Unit::new("statement", "s1", [("text", "words")], when.clone()).link(
        "said",
        "v1",
        "s1",
        when.clone(),
    );
    let voice = Unit::new("voice", "v1", NONE, when);
    world.write(&Write::new("swtor").unit(voice).unit(statement.clone()));
    world.restate = Some(statement);
}

// ---------- FR-RANNOCH-066: a link that ends, and a link found wrong ----------

/// The point the scenarios end a link at, whole milliseconds as the graph keeps points: after
/// every hearing they write, so no live hearing comes after the ending and reopens the link.
fn ended_at() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(4_102_444_800_123)
}

#[given("a link that has been heard")]
fn a_link_heard(world: &mut World) {
    link_heard(world);
}

#[given("a link that has been heard more than once")]
fn a_link_heard_twice(world: &mut World) {
    link_heard(world);
    restates(world);
    let hearings = world.g().link_hearings("said", "v1", "s1").unwrap();
    assert_eq!(hearings.len(), 2, "{hearings:?}");
    world.hearings.insert("said".into(), hearings);
}

#[when("the app ends the link at a time")]
fn ends_the_link_at(world: &mut World) {
    ends_the_link(world);
}

#[when("the app ends the link")]
fn ends_the_link(world: &mut World) {
    ends_the_link_valid(world, ValidTime::Unknown);
}

fn ends_the_link_valid(world: &mut World, valid: ValidTime) {
    let event = world
        .g()
        .end_link("said", "v1", "s1", ended_at(), valid, "moderator")
        .unwrap();
    world.written.push(event);
}

#[given("a link that has been heard, valid from 2024-01-01")]
fn a_link_heard_from_2024(world: &mut World) {
    link_heard_valid(world, day(2024, 1, 1));
}

#[when("the app ends the link at a time, with a valid time of 2025-06-01")]
fn ends_the_link_valid_2025(world: &mut World) {
    ends_the_link_valid(world, day(2025, 6, 1));
}

#[then("the link's history holds an ending at that time with a valid time of 2025-06-01")]
fn history_holds_the_valid_ending(world: &mut World) {
    let event = world.written.last().unwrap().id;
    assert_eq!(
        world.g().link_end("said", "v1", "s1").unwrap(),
        Some(End::Ended {
            at: ended_at(),
            valid: day(2025, 6, 1),
            source: "moderator".into(),
            event,
        })
    );
    assert!(
        matches!(
            &world.written.last().unwrap().change,
            Change::Ended { valid, .. } if *valid == day(2025, 6, 1)
        ),
        "the ending's event carries its valid time"
    );
    let hearings = world.g().link_hearings("said", "v1", "s1").unwrap();
    assert_eq!(hearings.len(), 1, "{hearings:?}");
    assert_eq!(hearings[0].valid, day(2024, 1, 1));
}

/// Statement s1 supports finding f1 by a link valid from 2024-01-01, both nodes valid from
/// 2023-01-01, and the link ends with the valid time `ended`.
fn supports_then_ended(world: &mut World, ended: ValidTime) {
    open_graph(world);
    let nodes = day(2023, 1, 1);
    world.write(
        &Write::new("swtor")
            .unit(Unit::new("finding", "f1", NONE, nodes.clone()))
            .unit(
                Unit::new("statement", "s1", [("text", "words")], nodes).link(
                    "supports",
                    "s1",
                    "f1",
                    day(2024, 1, 1),
                ),
            ),
    );
    world
        .g()
        .end_link("supports", "s1", "f1", ended_at(), ended, "moderator")
        .unwrap();
}

#[given(
    "a statement that supports finding f1 by a link valid from 2024-01-01, and the link ended with a valid time of 2025-06-01"
)]
#[given(
    "a statement that supports finding f1 by a link valid from 2024-01-01, and the link rewritten to end with a valid time of 2025-06-01"
)]
fn supports_ended_2025(world: &mut World) {
    supports_then_ended(world, day(2025, 6, 1));
}

#[given(
    "a statement that supports finding f1 by a link valid from 2024-01-01, and the link ended with no valid time"
)]
fn supports_ended_unknown(world: &mut World) {
    supports_then_ended(world, ValidTime::Unknown);
}

#[when(
    "the app counts statements supporting f1 as of valid time {a}, as of valid time {b} and as of valid time {c}"
)]
fn counts_f1_as_of_three(world: &mut World, a: String, b: String, c: String) {
    count_f1_as_of_each(world, [a, b, c]);
}

/// Counts f1's supporting statements as of each date, keeping each answer or refusal.
fn count_f1_as_of_each(world: &mut World, dates: impl IntoIterator<Item = String>) {
    for when in dates {
        let as_of = date(&when);
        let counted = world.g().count(&Count {
            class: "finding".into(),
            roots: vec!["f1".into()],
            path: vec![Step::new("supports<-")],
            group_by: None,
            as_of: Some(as_of.clone()),
            known_at: None,
        });
        world.as_of_counts.push((as_of, counted));
    }
}

#[when("the app counts statements supporting f1 as of valid time {a} and as of valid time {b}")]
fn counts_f1_as_of_two(world: &mut World, a: String, b: String) {
    count_f1_as_of_each(world, [a, b]);
}

#[then("the statement is counted as of 2024-12-01 and not as of 2025-12-01")]
fn counted_until_the_rewrite(world: &mut World) {
    assert_eq!(counted_as_of(world, "2024-12-01"), 1);
    assert_eq!(counted_as_of(world, "2025-12-01"), 0);
}

/// The statements counted as of `when`, when that count was answered.
fn counted_as_of(world: &World, when: &str) -> u64 {
    let (_, counted) = world
        .as_of_counts
        .iter()
        .find(|(as_of, _)| *as_of == date(when))
        .unwrap();
    let tallies = counted.as_ref().unwrap();
    assert_eq!(tallies[0].distinct, tallies[0].total, "{tallies:?}");
    tallies[0].distinct
}

#[then("the statement is counted as of 2024-12-01 and not as of 2025-06-01 or 2025-12-01")]
fn counted_until_the_ending(world: &mut World) {
    assert_eq!(counted_as_of(world, "2024-12-01"), 1);
    assert_eq!(counted_as_of(world, "2025-06-01"), 0);
    assert_eq!(counted_as_of(world, "2025-12-01"), 0);
}

#[then("the count as of 2023-06-01 is answered and does not count the statement")]
fn answered_before_the_link(world: &mut World) {
    assert_eq!(counted_as_of(world, "2023-06-01"), 0);
}

#[then(
    "the counts as of 2024-12-01 and 2100-01-01 are refused, each saying the link's ending's valid time is unknown"
)]
fn refused_on_the_unknown_ending(world: &mut World) {
    for when in ["2024-12-01", "2100-01-01"] {
        let (_, counted) = world
            .as_of_counts
            .iter()
            .find(|(as_of, _)| *as_of == date(when))
            .unwrap();
        match counted {
            Err(Error::Refused(why)) => assert!(
                why.contains("supports link from s1 to f1")
                    && why.contains("ending's valid time is unknown")
                    && why.contains(when),
                "as of {when}: {why}"
            ),
            other => panic!("as of {when}, not refused: {other:?}"),
        }
    }
}

#[when("the app finds the link wrong")]
fn finds_the_link_wrong(world: &mut World) {
    let event = world
        .g()
        .correct_link("said", "v1", "s1", "moderator")
        .unwrap();
    world.written.push(event);
}

#[then("the link's history holds an ending at that time")]
fn history_holds_the_ending(world: &mut World) {
    let event = world.written.last().unwrap().id;
    assert_eq!(
        world.g().link_end("said", "v1", "s1").unwrap(),
        Some(End::Ended {
            at: ended_at(),
            valid: ValidTime::Unknown,
            source: "moderator".into(),
            event,
        })
    );
    assert!(
        matches!(world.written.last().unwrap().change, Change::Ended { .. }),
        "the ending is an event of its own"
    );
}

#[then("the link's history still holds every hearing")]
fn history_keeps_every_hearing(world: &mut World) {
    let hearings = world.g().link_hearings("said", "v1", "s1").unwrap();
    assert_eq!(hearings, world.hearings["said"]);
    assert!(matches!(
        world.g().link_end("said", "v1", "s1").unwrap(),
        Some(End::Ended { .. })
    ));
}

#[then("the link does not read as ended")]
fn link_not_ended(world: &mut World) {
    let event = world.written.last().unwrap().id;
    assert_eq!(
        world.g().link_end("said", "v1", "s1").unwrap(),
        Some(End::Corrected {
            source: "moderator".into(),
            event,
        }),
        "a link found wrong reads as corrected"
    );
}

#[when("the app restates it from another source")]
fn restates(world: &mut World) {
    let unit = world.restate.clone().expect("a fact was heard");
    world.write(&Write::new("reddit").unit(unit));
}

#[then("the graph holds one node for it")]
fn one_node_for_it(world: &mut World) {
    let tallies = world
        .g()
        .count(&Count {
            class: "note".into(),
            roots: vec!["k1".into()],
            path: Vec::new(),
            group_by: None,
            as_of: None,
            known_at: None,
        })
        .unwrap();
    assert_eq!(tallies.len(), 1);
    assert_eq!(tallies[0].distinct, 1);
    assert_eq!(hearings_by_key(world.g())["k1"].len(), 2);
}

#[then("the link has a hearing for each source it was heard from")]
fn link_hearing_per_source(world: &mut World) {
    let sources: BTreeSet<String> = world
        .g()
        .link_hearings("said", "v1", "s1")
        .unwrap()
        .into_iter()
        .map(|h| h.source)
        .collect();
    assert_eq!(sources, BTreeSet::from(["reddit".into(), "swtor".into()]));
}

#[when("the app restates it from another source with other properties and a later valid time")]
fn restates_otherwise(world: &mut World) {
    let unit = Unit::new("note", "k1", [("text", "other words")], day(2026, 10, 20));
    world.write(&Write::new("reddit").unit(unit));
}

#[then("the node's first recorded and first true are those of the first hearing")]
fn first_recorded_and_true_kept(world: &mut World) {
    let first = world.found.clone().expect("the node was heard");
    let hearings = world.g().hearings(&NodeRef::new("note", "k1")).unwrap();
    assert_eq!(hearings.len(), 2, "{hearings:?}");
    assert_eq!(hearings[0].source, "swtor");
    assert_eq!(hearings[0].point, first.recorded);
    assert_eq!(hearings[0].valid, first.valid);
    assert_eq!(world.find("note", "k1").valid, day(2026, 10, 20));
}

/// New graphs of the keyless class, one under each of `keys`.
fn new_graphs(world: &World, keys: &[[u8; 32]]) -> Vec<Graph> {
    keys.iter()
        .enumerate()
        .map(|(i, key)| {
            let path = world.dir.path().join(format!("graph{i}.rannoch"));
            let mut g = rannoch::open(path, Some(key)).unwrap();
            declare_kind(&mut g, "keyless");
            g
        })
        .collect()
}

#[given("two new graphs with the same ontology")]
fn two_new_graphs(world: &mut World) {
    world.graphs = new_graphs(world, &[KEY, KEY]);
}

#[given(
    "three new graphs with the same ontology, two with one content-identity key and one with another key"
)]
fn three_new_graphs(world: &mut World) {
    world.graphs = new_graphs(world, &[KEY, KEY, OTHER_KEY]);
}

#[when("the app writes the same hearing to each graph")]
fn same_hearing_to_each(world: &mut World) {
    for g in &mut world.graphs {
        let unit = Unit::new("note", "k1", [("text", "words")], day(2026, 10, 1));
        g.write(&Write::new("swtor").unit(unit)).unwrap();
    }
}

/// The one hearing of note k1 in each graph.
fn each_hearing(world: &World) -> Vec<Hearing> {
    world
        .graphs
        .iter()
        .map(|g| {
            let mut hearings = g.hearings(&NodeRef::new("note", "k1")).unwrap();
            assert_eq!(hearings.len(), 1, "{hearings:?}");
            hearings.remove(0)
        })
        .collect()
}

#[then("each hearing's event id is a version 7 UUID and the two event ids differ")]
fn ids_are_v7_and_differ(world: &mut World) {
    let ids: Vec<String> = each_hearing(world)
        .iter()
        .map(|h| h.event.to_string())
        .collect();
    for id in &ids {
        assert_eq!(id.as_bytes()[14], b'7', "{id} is not a version 7 UUID");
    }
    assert_ne!(ids[0], ids[1]);
}

#[then(
    "the two graphs with the same key hold it under the same content identity and the graph with the other key holds it under a different one"
)]
fn identity_is_keyed(world: &mut World) {
    let identities: Vec<[u8; 32]> = each_hearing(world).iter().map(|h| h.identity).collect();
    assert_eq!(identities[0], identities[1]);
    assert_ne!(identities[0], identities[2]);
}

#[given("a graph opened under a content-identity key the app supplies")]
fn graph_under_app_key(world: &mut World) {
    let mut rng = fastrand::Rng::new();
    world.key = std::array::from_fn(|_| rng.u8(..));
    let mut g = rannoch::open(world.path(), Some(&world.key)).unwrap();
    declare_kind(&mut g, "keyless");
    world.graph = Some(g);
}

#[when("the app writes hearings, takes a snapshot and closes the queue")]
fn writes_snapshots_closes(world: &mut World) {
    let graph = world.graph.take().expect("the graph is open");
    let queue = Queue::open(graph, Duration::from_millis(5));
    for k in 0..20 {
        let unit = Unit::new(
            "note",
            format!("k{k}"),
            [("text", "words")],
            day(2026, 10, 1),
        );
        queue.write(Write::new("swtor").unit(unit)).unwrap();
    }
    assert!(queue.flush().1.is_empty());
    let reader = rannoch::open(world.path(), Some(&world.key)).unwrap();
    let cursor = reader.events().committed().unwrap();
    reader
        .snapshot(cursor, world.dir.path().join("snapshot.rannoch"))
        .unwrap();
    assert!(queue.close().is_empty());
    // The reader stays open, so the write-ahead log is still on disk to be searched.
    world.graph = Some(reader);
}

#[then(
    "a byte search of the store's files, its write-ahead log and snapshots included, finds no copy of the key"
)]
fn key_in_no_file(world: &mut World) {
    // In FR-RANNOCH-072's scenario the key is the purged node's; in the others, the
    // content-identity key, and nothing is purged.
    if !world.purge.targets.is_empty() {
        assert_eq!(world.purge.snapshots.len(), 1);
        assert_no_bytes(world, STANDING.as_bytes());
        return;
    }
    let hex = world.key.iter().fold(String::new(), |mut hex, b| {
        use std::fmt::Write as _;
        write!(hex, "{b:02x}").unwrap();
        hex
    });
    let mut searched = BTreeSet::new();
    for entry in std::fs::read_dir(world.dir.path()).unwrap() {
        let path = entry.unwrap().path();
        let bytes = std::fs::read(&path).unwrap();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            !bytes.windows(32).any(|w| w == world.key),
            "{name} holds the key"
        );
        assert!(
            !bytes.windows(64).any(|w| w == hex.as_bytes()),
            "{name} holds the key as hex"
        );
        searched.insert(name);
    }
    for name in ["graph.rannoch", "graph.rannoch-wal", "snapshot.rannoch"] {
        assert!(
            searched.contains(name),
            "{name} was not searched: {searched:?}"
        );
    }
}

#[given("a graph first opened under one content-identity key, written to and closed")]
#[given("a graph first opened under a content-identity key, written to and closed")]
fn written_and_closed(world: &mut World) {
    declare_kind(world.open(), "keyless");
    let unit = unit_of(world.g(), "k1", "words", 0);
    world.write(&Write::new("swtor").unit(unit));
    world.graph = None;
}

#[when("the app opens the graph under another key")]
fn opens_under_other_key(world: &mut World) {
    let refused = rannoch::open(world.path(), Some(&OTHER_KEY)).expect_err("the open is refused");
    world.refusals = vec![refused];
}

#[when("the app opens the graph without a content-identity key")]
fn opens_without_key(world: &mut World) {
    let refused = rannoch::open(world.path(), None).expect_err("the open is refused");
    world.refusals = vec![refused];
}

/// Asserts `n` refusals, each saying the key differs from the graph's.
fn key_differs(world: &World, n: usize) {
    assert_eq!(world.refusals.len(), n, "{:?}", world.refusals);
    for refusal in &world.refusals {
        assert!(matches!(refusal, Error::KeyDiffers), "{refusal:?}");
        assert!(refusal.to_string().contains("differs"), "{refusal}");
    }
}

#[then("the open is refused, and the refusal says the key differs from the graph's")]
fn open_refused_key_differs(world: &mut World) {
    key_differs(world, 1);
}

#[then("the open is refused, and the refusal says the key is missing")]
fn open_refused_key_missing(world: &mut World) {
    let [refusal] = &world.refusals[..] else {
        panic!("expected one refusal, got {:?}", world.refusals);
    };
    assert!(matches!(refusal, Error::KeyMissing), "{refusal:?}");
    assert!(refusal.to_string().contains("missing"), "{refusal}");
}

#[given("a graph written under one content-identity key, with a snapshot")]
fn written_with_snapshot(world: &mut World) {
    declare_kind(world.open(), "keyless");
    let unit = unit_of(world.g(), "k1", "words", 0);
    world.write(&Write::new("swtor").unit(unit));
    snapshot_at_end(world);
}

#[when(
    "the graph is rebuilt from its events under another key, and from its snapshot under another key"
)]
fn rebuilt_under_other_key(world: &mut World) {
    let events = world.all_events();
    let from_events = rannoch::rebuild(
        &events,
        world.dir.path().join("from-events.rannoch"),
        Some(&OTHER_KEY),
    )
    .expect_err("the rebuild from events is refused");
    let snapshot = world.snapshot.clone().expect("the app took a snapshot");
    let later = world.g().events().since(snapshot.cursor()).unwrap();
    let from_snapshot = snapshot
        .rebuild(
            &later,
            world.dir.path().join("from-snapshot.rannoch"),
            Some(&OTHER_KEY),
        )
        .expect_err("the rebuild from the snapshot is refused");
    world.refusals = vec![from_events, from_snapshot];
}

#[then("both rebuilds are refused, and each refusal says the key differs from the graph's")]
fn rebuilds_refused_key_differs(world: &mut World) {
    key_differs(world, 2);
}

// ---------- FR-RANNOCH-082, 083, 076: current state and state at a point on the axis ----------

/// Midnight UTC on a day, as a point on the time axis.
fn on(year: i32, month: u32, d: u32) -> SystemTime {
    let days = |y: i32, m: u32, d: u32| {
        // Days since 1970-01-01 (Howard Hinnant's days_from_civil).
        let y = if m <= 2 { y - 1 } else { y };
        let era = y.div_euclid(400);
        let yoe = i64::from(y - era * 400);
        let mp = i64::from((m + 9) % 12);
        let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        i64::from(era) * 146_097 + doe - 719_468
    };
    let secs = u64::try_from(days(year, month, d) * 86_400).unwrap();
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

/// The graph's one class's node `k1` carrying `text`, at the time `point` or, in an
/// integer-keyed class, with the integer key `point`.
fn version_of(g: &Graph, text: &str, point: At) -> Unit {
    let class = the_class(g).name;
    let unit = Unit::new(class, "k1", [("text", text)], day(2026, 10, 1));
    match point {
        At::Time(time) => unit.at(time),
        At::Key(key) => unit.integer(key),
    }
}

/// Writes the versions of `k1` in order, each its own write, keeping every event written.
fn write_versions(world: &mut World, versions: &[(&str, At)]) {
    let mut written = Vec::new();
    for (text, point) in versions {
        let unit = version_of(world.g(), text, *point);
        written.extend(world.g().write(&Write::new("crawler").unit(unit)).unwrap());
    }
    world.written = written;
}

fn k1(world: &mut World) -> NodeRef {
    NodeRef::new(the_class(world.g()).name, "k1")
}

fn text_of(found: &Node) -> String {
    text(&found.properties["text"])
}

fn found_text(world: &mut World) -> String {
    let class = the_class(world.g()).name;
    text_of(&world.find(&class, "k1"))
}

#[when(
    "the app writes a node at 2025-06-01 and then writes the same node, with other properties, at 2024-03-01"
)]
#[when("the app writes a node at 2025-06-01 and then writes the same node at 2024-03-01")]
fn writes_later_then_earlier(world: &mut World) {
    let (june, march) = (At::Time(on(2025, 6, 1)), At::Time(on(2024, 3, 1)));
    write_versions(world, &[("2025-06-01", june), ("2024-03-01", march)]);
}

#[then("find returns the node's state written at 2025-06-01")]
fn finds_june(world: &mut World) {
    assert_eq!(found_text(world), "2025-06-01");
}

#[when(
    "the app writes a node with key 3 and then writes the same node, with other properties, with key 2"
)]
fn writes_key_3_then_2(world: &mut World) {
    write_versions(world, &[("key 3", At::Key(3)), ("key 2", At::Key(2))]);
}

#[then("find returns the node's state written with key 3")]
fn finds_key_3(world: &mut World) {
    assert_eq!(found_text(world), "key 3");
}

#[when(
    "the app writes a node, waits at least one millisecond, and writes the same node with other properties"
)]
fn writes_twice_apart(world: &mut World) {
    let first = unit_of(world.g(), "k1", "first", 0);
    world.write(&Write::new("crawler").unit(first));
    std::thread::sleep(Duration::from_millis(2));
    let second = unit_of(world.g(), "k1", "second", 0);
    world.write(&Write::new("crawler").unit(second));
}

#[when(
    "the app writes a node and, from the same process within the same millisecond, writes the same node with other properties"
)]
fn writes_twice_in_one_millisecond(world: &mut World) {
    // Two writes land in one millisecond almost always; try again under a new key otherwise.
    for attempt in 0..100 {
        let key = format!("k{}", attempt + 1);
        let class = the_class(world.g()).name;
        for text in ["first", "second"] {
            let unit = Unit::new(&class, &key, [("text", text)], day(2026, 10, 1));
            world.g().write(&Write::new("crawler").unit(unit)).unwrap();
        }
        let hearings = world.g().hearings(&NodeRef::new(&class, &key)).unwrap();
        if hearings[0].point == hearings[1].point {
            world.content = key;
            return;
        }
    }
    panic!("no two writes landed in one millisecond");
}

#[then("find returns the node's state from the second write")]
fn finds_second(world: &mut World) {
    let class = the_class(world.g()).name;
    // The key written, when the scenario named one; else k1.
    let key = if world.content.is_empty() {
        "k1".to_string()
    } else {
        world.content.clone()
    };
    assert_eq!(text_of(&world.find(&class, &key)), "second");
}

/// A node's state with when this store recorded it left out: that is the store's own.
fn state_of(node: Node) -> (BTreeMap<String, Value>, String, ValidTime, Vec<EventId>) {
    (node.properties, node.source, node.valid, node.derived_from)
}

/// Writes, in a time-keyed class with a relation `follows`, nodes a and b and a link from b to
/// a three times, each write self-contained: at points 30, then 10 and 20, so the nodes' later
/// versions arrive after newer ones; each link version is later than the one before.
#[given(
    "the events of a graph in which some versions of its nodes and links arrived after newer ones"
)]
fn events_with_late_versions(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    let follows = Relation::new("follows", "visit", "visit", false);
    world.g().declare(&Declaration::Relation(follows)).unwrap();
    for (n, valid) in [
        (30, day(2026, 1, 1)),
        (10, day(2026, 2, 2)),
        (20, day(2026, 3, 3)),
    ] {
        let text = n.to_string();
        let a = Unit::new("visit", "a", [("text", text.as_str())], valid.clone()).at(at(n));
        let b = Unit::new("visit", "b", [("text", text.as_str())], valid.clone())
            .at(at(n))
            .link("follows", "b", "a", valid);
        world
            .g()
            .write(&Write::new("crawler").unit(a).unit(b))
            .unwrap();
    }
    world.log = world.all_events();
}

#[given("two writes of one node at point 2025-06-01 with different properties")]
fn two_writes_at_one_point(world: &mut World) {
    let june = At::Time(on(2025, 6, 1));
    write_versions(world, &[("one", june), ("two", june)]);
    world.log = world.all_events();
}

#[when("two new graphs are given those events, one in recorded order and one in reverse order")]
#[when("one graph is given the two writes in one order and another graph in the other order")]
fn given_in_both_orders(world: &mut World) {
    let path = world.dir.path().join("recorded.rannoch");
    let recorded = rannoch::rebuild(&world.log, path, Some(&KEY)).unwrap();
    given_in_reverse(world);
    let reversed = world.rebuilt.take().expect("the graph was rebuilt");
    world.graphs = vec![recorded, reversed];
}

#[then("find returns the same result in both graphs for every node and link")]
fn same_find_for_nodes_and_links(world: &mut World) {
    let [recorded, reversed] = &world.graphs[..] else {
        panic!("two graphs")
    };
    for key in ["a", "b"] {
        let found = |g: &Graph| state_of(g.find("visit", key).unwrap().unwrap());
        assert_eq!(found(recorded), found(reversed), "node {key}");
        assert_eq!(text(&found(recorded).0["text"]), "30", "node {key}");
    }
    let link = |g: &Graph| {
        let link = g.find_link("follows", "b", "a").unwrap().unwrap();
        (link.source, link.valid)
    };
    assert_eq!(link(recorded), link(reversed));
    assert_eq!(
        link(recorded).1,
        day(2026, 3, 3),
        "the link's newest version"
    );
}

#[then("find returns the same state for the node in both graphs")]
fn same_find_for_the_node(world: &mut World) {
    let found = |g: &Graph| state_of(g.find("visit", "k1").unwrap().unwrap());
    let (one, other) = (found(&world.graphs[0]), found(&world.graphs[1]));
    assert_eq!(one, other);
    // The tie goes to the larger event id: the second write's.
    assert_eq!(text(&one.0["text"]), "two");
}

#[then("a read of the node's state at 2024-06-01 returns the version written at 2024-03-01")]
fn reads_march_in_june(world: &mut World) {
    let node = k1(world);
    let state = world.g().state_at(&node, At::Time(on(2024, 6, 1))).unwrap();
    let state = state.expect("a version held then");
    assert_eq!(text(&state.properties["text"]), "2024-03-01");
    assert_eq!(state.point, on(2024, 3, 1));
}

#[given("a node written at points 10, 20 and 30")]
fn written_at_10_20_30(world: &mut World) {
    let points = [("10", at(10)), ("20", at(20)), ("30", at(30))];
    write_versions(world, &points.map(|(t, p)| (t, At::Time(p))));
}

#[when("the app writes a node at points 10 and 30, and then at point 20")]
fn written_at_10_30_then_20(world: &mut World) {
    let points = [("10", at(10)), ("30", at(30)), ("20", at(20))];
    write_versions(world, &points.map(|(t, p)| (t, At::Time(p))));
}

#[given("a node written with keys 1, 2 and 3")]
fn written_with_keys(world: &mut World) {
    let keys = [("key 1", 1), ("key 2", 2), ("key 3", 3)];
    write_versions(world, &keys.map(|(t, k)| (t, At::Key(k))));
}

#[given("a node written at point 20 from a source with a valid time")]
fn written_at_20_with_source(world: &mut World) {
    let unit = Unit::new("visit", "k1", [("text", "20")], day(2024, 5, 1)).at(at(20));
    world.write(&Write::new("crawler").unit(unit));
}

fn read_state(world: &mut World, point: At) {
    let node = k1(world);
    let state = world.g().state_at(&node, point).unwrap();
    world.states.push(state);
}

#[when("the app reads the node's state at point {n}")]
fn reads_state_at_point(world: &mut World, n: String) {
    read_state(world, At::Time(at(n.parse().unwrap())));
}

#[when("the app reads the node's state at key {k}")]
fn reads_state_at_key(world: &mut World, k: String) {
    read_state(world, At::Key(k.parse().unwrap()));
}

/// The one state read.
fn the_state(world: &World) -> Option<Version> {
    let [state] = &world.states[..] else {
        panic!("one read, got {:?}", world.states)
    };
    state.clone()
}

#[then("the read returns the version written at point 20")]
fn returns_point_20(world: &mut World) {
    let state = the_state(world).expect("a version held then");
    assert_eq!(text(&state.properties["text"]), "20");
    assert_eq!(state.point, at(20));
}

#[then("the read returns no version")]
fn returns_no_version(world: &mut World) {
    assert_eq!(the_state(world), None);
}

#[then("the read returns the version written with key 2")]
fn returns_key_2(world: &mut World) {
    let state = the_state(world).expect("a version held then");
    assert_eq!(text(&state.properties["text"]), "key 2");
    assert_eq!(state.integer, Some(2));
}

#[then("a read of the node's state at point 25 returns the version written at point 20")]
fn reads_20_at_25(world: &mut World) {
    read_state(world, At::Time(at(25)));
    returns_point_20(world);
}

#[then("the answer names point 20, that source, that valid time and the version's event id")]
fn answer_names_the_version(world: &mut World) {
    let state = the_state(world).expect("a version held then");
    assert_eq!(state.point, at(20));
    assert_eq!(state.source, "crawler");
    assert_eq!(state.valid, day(2024, 5, 1));
    assert_eq!(state.event, world.written[0].id);
    let node = k1(world);
    assert_eq!(world.g().hearings(&node).unwrap()[0].event, state.event);
}

#[then("the answer names no table or column of the store")]
fn answer_names_no_storage(world: &mut World) {
    let state = the_state(world).expect("a version held then");
    let point = state.point.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let ValidTime::On(valid) = &state.valid else {
        panic!("a valid time")
    };
    let mut texts = vec![
        state.source.clone(),
        valid.to_string(),
        state.event.to_string(),
        point.as_millis().to_string(),
    ];
    texts.extend(
        state
            .properties
            .iter()
            .map(|(p, v)| format!("{p} {}", text(v))),
    );
    assert_names_no_storage(world, &texts);
}

/// An event id minted at `time`, as the store that recorded an event then would mint it.
fn id_at(time: SystemTime) -> EventId {
    let since = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let stamp = uuid::Timestamp::from_unix(uuid::NoContext, since.as_secs(), since.subsec_nanos());
    serde_json::from_value(uuid::Uuid::new_v7(stamp).to_string().into()).unwrap()
}

/// A link's point is the record time of the store that first recorded it, carried in its
/// event: the graph is given the link's write as recorded at point 10, then ends it at 30.
#[given("a link written at point 10 that ended at point 30")]
fn link_ended_at_30(world: &mut World) {
    let follows = Relation::new("follows", "visit", "visit", false);
    world.g().declare(&Declaration::Relation(follows)).unwrap();
    let when = day(2026, 1, 1);
    let a = Unit::new("visit", "a", [("text", "a")], when.clone()).at(at(10));
    let b = Unit::new("visit", "b", [("text", "b")], when.clone())
        .at(at(10))
        .link("follows", "b", "a", when);
    world
        .g()
        .write(&Write::new("crawler").unit(a).unit(b))
        .unwrap();
    let events: Vec<Event> = world
        .all_events()
        .into_iter()
        .map(|event| match event.change {
            Change::Wrote { .. } => Event {
                id: id_at(at(10)),
                ..event
            },
            _ => event,
        })
        .collect();
    let path = world.dir.path().join("recorded-at-10.rannoch");
    world.graph = Some(rannoch::rebuild(&events, path, Some(&KEY)).unwrap());
    let hearings = world.g().link_hearings("follows", "b", "a").unwrap();
    assert_eq!(hearings[0].point, at(10));
    world
        .g()
        .end_link("follows", "b", "a", at(30), ValidTime::Unknown, "moderator")
        .unwrap();
}

#[when("the app reads the link's state at points 25 and 35")]
fn reads_link_at_25_and_35(world: &mut World) {
    for n in [25, 35] {
        let state = world
            .g()
            .link_state_at("follows", "b", "a", At::Time(at(n)));
        world.states.push(state.unwrap());
    }
}

#[then("the link reads as live at point 25 and as ended at point 35")]
fn link_live_then_ended(world: &mut World) {
    let [Some(live), Some(ended)] = &world.states[..] else {
        panic!("two versions, got {:?}", world.states)
    };
    assert!(!live.ended, "{live:?}");
    assert!(ended.ended, "{ended:?}");
    assert_eq!(live.point, at(10));
}

// FR-RANNOCH-076: first and last heard on the axis, first recorded and first true.

#[when("the app writes a hearing of a node at 2025-06 and then one at 2024-03")]
fn writes_hearings_june_then_march(world: &mut World) {
    writes_later_then_earlier(world);
}

fn seen(world: &mut World) -> rannoch::Seen {
    let node = k1(world);
    world.g().seen(&node).unwrap().expect("the node was heard")
}

#[then("the node's first heard is 2024-03")]
fn first_heard_march(world: &mut World) {
    assert_eq!(seen(world).first_heard.point, on(2024, 3, 1));
}

#[then("the node's last heard is 2025-06")]
fn last_heard_june(world: &mut World) {
    assert_eq!(seen(world).last_heard.point, on(2025, 6, 1));
}

#[then("the node's first recorded is the 2025-06 write")]
fn first_recorded_june(world: &mut World) {
    let first = seen(world).first_recorded;
    assert_eq!(first.event, world.written[0].id);
    assert_eq!(first.point, on(2025, 6, 1));
}

#[given("a node written with a valid time of {valid}")]
fn written_valid(world: &mut World, valid: String) {
    declare_kind(world.open(), "keyless");
    rewrites_valid(world, valid);
}

fn month(text: &str) -> ValidTime {
    let (year, month) = text.split_once('-').unwrap();
    ValidTime::On(Date::month(year.parse().unwrap(), month.parse().unwrap()))
}

#[when("the app rewrites it with a valid time of {valid}")]
fn rewrites_valid(world: &mut World, valid: String) {
    let unit = Unit::new("note", "k1", [("text", "words")], month(&valid));
    world.write(&Write::new("crawler").unit(unit));
}

#[then("the node's first true is {valid}")]
fn first_true_is(world: &mut World, valid: String) {
    assert_eq!(seen(world).first_true, month(&valid));
}

#[given("the events of a graph in which a node was heard at 2025-06 and then at 2024-03")]
fn events_june_then_march(world: &mut World) {
    writes_later_then_earlier(world);
    world.log = world.all_events();
}

#[then(
    "the first graph's first recorded is the 2025-06 hearing and the new graph's first recorded is the 2024-03 hearing"
)]
fn first_recorded_per_store(world: &mut World) {
    assert_eq!(seen(world).first_recorded.point, on(2025, 6, 1));
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    let node = NodeRef::new("visit", "k1");
    let first = rebuilt.seen(&node).unwrap().unwrap().first_recorded;
    assert_eq!(first.point, on(2024, 3, 1));
}

// FR-RANNOCH-048: a read as of a valid time returns the version true then.

/// Writes the graph's one class's node `k1` once per version: its text, its valid time and,
/// in a time-keyed class, the point it is heard at.
fn write_valid_versions(world: &mut World, versions: &[(&str, ValidTime, Option<SystemTime>)]) {
    let class = the_class(world.g()).name;
    for (text, valid, point) in versions {
        let unit = Unit::new(class.as_str(), "k1", [("text", *text)], valid.clone());
        let unit = match point {
            Some(point) => unit.at(*point),
            None => unit,
        };
        world.write(&Write::new("crawler").unit(unit));
    }
}

#[given(
    "a node written with a valid time of 2024-01-01 and then rewritten with a valid time of 2025-06-01"
)]
fn written_then_rewritten_valid(world: &mut World) {
    declare_kind(world.open(), "keyless");
    let versions = [
        ("first", day(2024, 1, 1), None),
        ("second", day(2025, 6, 1), None),
    ];
    write_valid_versions(world, &versions);
}

#[when("the app reads the node as of valid time 2024-12-01")]
fn reads_as_of_2024_12(world: &mut World) {
    let class = the_class(world.g()).name;
    world.found = world
        .g()
        .find_as_of(&class, "k1", &date("2024-12-01"), None)
        .unwrap();
}

#[then("the read returns the version valid from 2024-01-01")]
fn returns_version_from_2024(world: &mut World) {
    let found = world.found.as_ref().expect("a version held then");
    assert_eq!(found.valid, day(2024, 1, 1));
    assert_eq!(text_of(found), "first");
}

#[given(
    "a node with a version heard at point 10 with valid time 2023-01-01 and a version heard at point 40 with valid time 2024-01-01"
)]
fn versions_heard_at_10_and_40(world: &mut World) {
    let versions = [
        ("heard at 10", day(2023, 1, 1), Some(at(10))),
        ("heard at 40", day(2024, 1, 1), Some(at(40))),
    ];
    write_valid_versions(world, &versions);
}

#[when(
    "the app reads the node as of valid time 2024-06-01 as known now, and as of valid time 2024-06-01 as known at point 30"
)]
fn reads_known_now_and_at_30(world: &mut World) {
    let class = the_class(world.g()).name;
    for known in [None, Some(At::Time(at(30)))] {
        let found = world
            .g()
            .find_as_of(&class, "k1", &date("2024-06-01"), known);
        world
            .answers
            .push(text_of(&found.unwrap().expect("a version held then")));
    }
}

#[then(
    "the first read returns the version heard at point 40 and the second returns the version heard at point 10"
)]
fn known_now_then_at_30(world: &mut World) {
    assert_eq!(world.answers, ["heard at 40", "heard at 10"]);
}

// FR-RANNOCH-070, and FR-RANNOCH-066 and FR-RANNOCH-084 on a link heard after its ending: a
// link's facts, each at its own point, given to a graph as the events that carry them.

/// A fact of the `follows` link from `from` to `a`: a hearing from a source, or an ending with
/// no valid time, at a point.
#[derive(Clone, Copy)]
enum LinkAt {
    Heard(&'static str, &'static str, u64),
    Ended(&'static str, u64),
}

/// Declares `follows` between the time-keyed class's nodes and writes nodes a, b and c, so the
/// graph's events so far are what every graph built from link facts starts from.
fn follows_between_visits(world: &mut World) {
    let follows = Relation::new("follows", "visit", "visit", false);
    world.g().declare(&Declaration::Relation(follows)).unwrap();
    let mut write = Write::new("crawler");
    for key in ["a", "b", "c"] {
        let text = [("text", key)];
        write = write.unit(Unit::new("visit", key, text, day(2026, 1, 1)).at(at(1)));
    }
    world.g().write(&write).unwrap();
}

/// A new graph given the world graph's events and then `facts`, each as an event minted at its
/// point, as the store that recorded it then minted it.
fn graph_of(world: &mut World, facts: &[LinkAt]) -> Graph {
    let mut events = world.all_events();
    let link = |from: &str| (NodeRef::new("visit", from), NodeRef::new("visit", "a"));
    for fact in facts {
        let (point, change) = match *fact {
            LinkAt::Heard(from, source, n) => {
                let (from, to) = link(from);
                let valid = day(2026, 1, 1);
                let fact = LinkFact {
                    relation: "follows".into(),
                    from,
                    to,
                    valid,
                    derived_from: Vec::new(),
                };
                let added = vec![Fact::Link(fact)];
                let source = source.into();
                let wrote = Change::Wrote {
                    source,
                    added,
                    wrong_kind: Vec::new(),
                    check: None,
                };
                (at(n), wrote)
            }
            LinkAt::Ended(from, n) => {
                let (from, to) = link(from);
                let millis = at(n)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                let ended = Change::Ended {
                    source: "moderator".into(),
                    relation: "follows".into(),
                    from,
                    to,
                    at: i64::try_from(millis).unwrap(),
                    valid: ValidTime::Unknown,
                };
                (at(n), ended)
            }
        };
        let cursor = serde_json::from_value(serde_json::json!(events.len() + 1)).unwrap();
        events.push(Event {
            id: id_at(point),
            cursor,
            change,
        });
    }
    let path = world
        .dir
        .path()
        .join(format!("links-{}.rannoch", world.graphs.len()));
    let _ = std::fs::remove_file(&path);
    rannoch::rebuild(&events, path, Some(&KEY)).unwrap()
}

#[given("a node that has been heard from more than one source")]
fn node_heard_from_two_sources(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    for (source, n) in [("crawler", 10), ("mirror", 20)] {
        let unit = unit_of(world.g(), "k1", source, n);
        world.write(&Write::new(source).unit(unit));
    }
}

#[given("a link that has been heard from more than one source and has ended")]
fn link_heard_from_two_sources_and_ended(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    follows_between_visits(world);
    let facts = [
        LinkAt::Heard("b", "crawler", 10),
        LinkAt::Heard("b", "mirror", 20),
        LinkAt::Ended("b", 30),
    ];
    world.graph = Some(graph_of(world, &facts));
    world.reads_link = true;
}

#[when("the app reads it once")]
fn reads_it_once(world: &mut World) {
    let seen = if world.reads_link {
        world.g().link_seen("follows", "b", "a")
    } else {
        let node = k1(world);
        world.g().seen(&node)
    };
    world.seen = Some(seen.unwrap().expect("it was heard"));
}

#[then("the read returns its first heard and its last heard")]
fn read_returns_first_and_last(world: &mut World) {
    let seen = world.seen.as_ref().unwrap();
    assert_eq!(
        (seen.first_heard.point, seen.first_heard.source.as_str()),
        (at(10), "crawler")
    );
    assert_eq!(
        (seen.last_heard.point, seen.last_heard.source.as_str()),
        (at(20), "mirror")
    );
    assert_eq!((seen.count, seen.ended), (2, None), "{seen:?}");
}

#[then("the read returns its first heard, its last heard and when it ended")]
fn read_returns_first_last_and_ended(world: &mut World) {
    let seen = world.seen.as_ref().unwrap();
    assert_eq!(
        (seen.first_heard.point, seen.first_heard.source.as_str()),
        (at(10), "crawler")
    );
    // The ending is the link's last fact, as its last version.
    assert!(seen.last_heard.ended, "{seen:?}");
    assert_eq!(seen.last_heard.point, at(30));
    assert_eq!((seen.ended, seen.count), (Some(at(30)), 3), "{seen:?}");
}

#[given(
    "a link written at point 10 that ended at point 20, and another link written at point 10 that ended at point 30"
)]
fn two_links_ended_at_20_and_30(world: &mut World) {
    follows_between_visits(world);
    world.links = vec![
        LinkAt::Heard("b", "crawler", 10),
        LinkAt::Ended("b", 20),
        LinkAt::Heard("c", "crawler", 10),
        LinkAt::Ended("c", 30),
    ];
}

/// Each link is written again from another source: the same content from the same source is
/// one hearing (FR-RANNOCH-065), named by its first event, and so not a hearing at a new point.
#[when("the app writes the first link again at point 30 and the second link again at point 20")]
fn writes_links_again(world: &mut World) {
    let mut facts = world.links.clone();
    facts.extend([
        LinkAt::Heard("b", "mirror", 30),
        LinkAt::Heard("c", "mirror", 20),
    ]);
    world.graph = Some(graph_of(world, &facts));
}

/// Whether the link from `from` reads as ended at point `n`.
fn ended_at_point(world: &mut World, from: &str, n: u64) -> bool {
    let state = world
        .g()
        .link_state_at("follows", from, "a", At::Time(at(n)));
    state.unwrap().expect("the link was heard by then").ended
}

#[then("the first link reads as ended at point 25 and as live at point 35")]
fn first_link_ended_then_live(world: &mut World) {
    assert!(ended_at_point(world, "b", 25));
    assert!(!ended_at_point(world, "b", 35));
    assert_eq!(world.g().link_end("follows", "b", "a").unwrap(), None);
}

#[then("the second link reads as ended at point 35")]
fn second_link_ended(world: &mut World) {
    assert!(ended_at_point(world, "c", 35));
    let end = world.g().link_end("follows", "c", "a").unwrap();
    assert!(
        matches!(end, Some(End::Ended { at: when, .. }) if when == at(30)),
        "{end:?}"
    );
}

#[given(
    "a link written at point 10, ended at point 20, written again at point 30, and ended at point 40 and again at point 50"
)]
fn link_ended_twice_in_a_row(world: &mut World) {
    follows_between_visits(world);
    world.links = vec![
        LinkAt::Heard("b", "crawler", 10),
        LinkAt::Ended("b", 20),
        LinkAt::Heard("b", "mirror", 30),
        LinkAt::Ended("b", 40),
        LinkAt::Ended("b", 50),
    ];
}

#[when(
    "one new graph is given these events in that order and another new graph is given them in the opposite order"
)]
fn link_facts_in_both_orders(world: &mut World) {
    let facts = world.links.clone();
    let forward = graph_of(world, &facts);
    world.graphs.push(forward);
    let backward: Vec<LinkAt> = facts.into_iter().rev().collect();
    let backward = graph_of(world, &backward);
    world.graphs.push(backward);
}

#[then("both graphs return point 40 as when the link ended")]
fn both_ended_at_40(world: &mut World) {
    for g in &world.graphs {
        let seen = g.link_seen("follows", "b", "a").unwrap().unwrap();
        assert_eq!((seen.ended, seen.count), (Some(at(40)), 5), "{seen:?}");
        let end = g.link_end("follows", "b", "a").unwrap();
        assert!(
            matches!(end, Some(End::Ended { at: when, .. }) if when == at(40)),
            "{end:?}"
        );
    }
}

#[given(
    "a node written by one source with content v1, then v2, then v1 again, each in its own commit"
)]
fn returns_to_v1(world: &mut World) {
    for text in ["v1", "v2", "v1"] {
        let unit = unit_of(world.g(), "k1", text, 0);
        world.write(&Write::new("crawler").unit(unit));
        assert_eq!(world.written.len(), 1, "{text} wrote its event");
        std::thread::sleep(Duration::from_millis(3));
    }
}

#[when("the node is read")]
fn the_node_is_read(world: &mut World) {
    let class = the_class(world.g()).name;
    world.found = Some(world.find(&class, "k1"));
}

#[then("its current state is v1")]
fn current_state_v1(world: &mut World) {
    let node = world.found.as_ref().expect("the node was read");
    assert_eq!(node.properties.get("text"), Some(&Value::Text("v1".into())));
}

#[then("it has one hearing of v1 with two occurrences and one hearing of v2")]
fn v1_twice_v2_once(world: &mut World) {
    let class = the_class(world.g()).name;
    let hearings = world.g().hearings(&NodeRef::new(&class, "k1")).unwrap();
    // A hearing occurs under its own event and under each alias.
    let mut occurrences: Vec<usize> = hearings.iter().map(|h| 1 + h.aliases.len()).collect();
    occurrences.sort_unstable();
    assert_eq!(occurrences, [1, 2], "{hearings:?}");
}

/// `tests/fixtures/store-0.2.0-return.rannoch` was written by rannoch at tag v0.2.0: class book
/// (no history key) with title (text), and book b1 written by source shelf with title v1, then
/// v2, then v1 again, each in its own commit; 4 events, none carrying a point.
#[given(
    "a log written by rannoch 0.2.0 in which one source wrote a node with content v1, then v2, then v1 again"
)]
fn log_020_return(world: &mut World) {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/store-0.2.0-return.rannoch"
    );
    std::fs::copy(fixture, world.path()).unwrap();
    world.open();
    world.log = world.all_events();
    assert_eq!(world.log.len(), 4);
}

#[when("a graph is rebuilt from that log")]
fn rebuilt_from_log(world: &mut World) {
    let path = world.dir.path().join("rebuilt.rannoch");
    world.rebuilt = Some(rannoch::rebuild(&world.log, path, Some(&KEY)).unwrap());
}

#[then("the node's current state is v1")]
fn rebuilt_reads_v1(world: &mut World) {
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    let node = rebuilt
        .find("book", "b1")
        .unwrap()
        .expect("b1 is in the graph");
    assert_eq!(
        node.properties.get("title"),
        Some(&Value::Text("v1".into()))
    );
}

// ---------- purge ----------

/// The token the purged fact's property carries: found nowhere else in a scenario's files.
const TOKEN: &str = "zq-purged-token-71";

/// What the purge scenarios hold: what is purged, what it was, and where the snapshots are.
#[derive(Default)]
struct Purged {
    /// What each purge names, in order.
    targets: Vec<Purge>,
    /// The purged hearings' exact contents, as their content identity hashes them.
    contents: Vec<Vec<u8>>,
    /// The keys and source strings that must be gone from the store.
    gone: Vec<String>,
    snapshots: Vec<PathBuf>,
    /// The event ids the test recorded before the purge: the hearing written alone's, then the
    /// two-hearing write's.
    ids: Vec<EventId>,
    /// Hearings that must stay, by node.
    kept: Vec<(NodeRef, Vec<Hearing>)>,
}

/// The purge scenarios' declarations: people who know people and live in houses, and rooms
/// part of houses and shelves part of rooms.
fn purge_graph(world: &mut World) -> &mut Graph {
    let g = world.open();
    for d in [
        Declaration::Class(Class::new("person", ["note"])),
        Declaration::Class(Class::new("house", ["name"])),
        Declaration::Class(Class::new("room", ["name"])),
        Declaration::Class(Class::new("shelf", ["name"])),
        Declaration::Relation(Relation::new("knows", "person", "person", false)),
        Declaration::Relation(Relation::new("lives", "person", "house", false)),
        Declaration::Relation(Relation::new("in_house", "room", "house", true)),
        Declaration::Relation(Relation::new("in_room", "shelf", "room", true)),
    ] {
        g.declare(&d).unwrap();
    }
    g
}

fn person(key: &str, note: &str) -> Unit {
    Unit::new("person", key, [("note", note)], day(2026, 10, 1))
}

/// The bytes a hearing's content identity hashes: a node's content heard from `source`.
fn node_content(unit_key: &str, note: &str, source: &str) -> Vec<u8> {
    let properties: BTreeMap<String, Value> = BTreeMap::from([("note".into(), Value::from(note))]);
    serde_json::to_vec(&(
        "node",
        "person",
        unit_key,
        &day(2026, 10, 1),
        source,
        &properties,
        None::<i64>,
        None::<i64>,
    ))
    .unwrap()
}

/// A graph holding the fact p-gone, whose note carries the token, written in one write with
/// p-stays, and other people's facts around it; the fact to purge is p-gone's one hearing.
fn token_fact(world: &mut World) {
    let g = purge_graph(world);
    g.write(&Write::new("app").unit(person("p-other", "before")))
        .unwrap();
    g.write(
        &Write::new("app")
            .unit(person("p-gone", TOKEN))
            .unit(person("p-stays", "stays")),
    )
    .unwrap();
    g.write(&Write::new("app").unit(person("p-after", "after")))
        .unwrap();
    let hearing = &g.hearings(&NodeRef::new("person", "p-gone")).unwrap()[0];
    world.purge.targets.push(Purge::Fact {
        identity: hearing.identity,
    });
    world
        .purge
        .contents
        .push(node_content("p-gone", TOKEN, "app"));
    world.purge.gone.push(TOKEN.into());
}

/// Purges each target the scenario named, under one rule.
fn purge_targets(world: &mut World) {
    for target in world.purge.targets.clone() {
        let event = world.g().purge(target, "rule-71", "app").unwrap();
        assert!(matches!(event.change, Change::Purged { .. }), "{event:?}");
    }
}

/// Closes every handle on the store, as an app does when its queue closes.
fn close_store(world: &mut World) {
    world.queue = None;
    world.graph = None;
}

/// Every file the store and its snapshots keep, write-ahead logs included, as bytes.
fn store_files(world: &World) -> Vec<(PathBuf, Vec<u8>)> {
    let mut bases = vec![world.path()];
    bases.extend(world.purge.snapshots.iter().cloned());
    files_of(&bases)
}

/// The files of each graph file in `bases`, write-ahead logs and journals included, as bytes.
fn files_of(bases: &[PathBuf]) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    for base in bases {
        for suffix in ["", "-wal", "-journal"] {
            let path = PathBuf::from(format!("{}{suffix}", base.display()));
            if let Ok(bytes) = std::fs::read(&path) {
                files.push((path, bytes));
            }
        }
    }
    assert!(!files.is_empty());
    files
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Asserts no file of the store or its snapshots holds `needle`.
fn assert_no_bytes(world: &World, needle: &[u8]) {
    for (path, bytes) in store_files(world) {
        assert!(
            !find_bytes(&bytes, needle),
            "{} holds {}",
            path.display(),
            String::from_utf8_lossy(needle)
        );
    }
}

/// Asserts no event the graph `g` holds encodes `text` as a JSON string.
fn assert_no_event_holds(g: &Graph, text: &str) {
    let quoted = serde_json::to_string(text).unwrap();
    for event in g.events().since(Cursor::START).unwrap() {
        let body = serde_json::to_string(&event.change).unwrap();
        assert!(!body.contains(&quoted), "{event:?} holds {text}");
    }
}

#[given("a graph holding a fact in its events")]
fn graph_holding_fact(world: &mut World) {
    token_fact(world);
    world.purge.gone.push("p-gone".into());
}

#[given("a graph holding a fact whose property carries a unique token")]
fn graph_holding_token(world: &mut World) {
    token_fact(world);
}

#[given("a graph from which the app has purged a fact")]
fn graph_purged_fact(world: &mut World) {
    graph_holding_fact(world);
    purge_targets(world);
}

#[given("a graph with a snapshot holding a fact")]
fn graph_snapshot_fact(world: &mut World) {
    graph_holding_fact(world);
    let path = world.dir.path().join("snapshot-1.rannoch");
    let at = world.g().events().committed().unwrap();
    world.g().snapshot(at, &path).unwrap();
    world.purge.snapshots.push(path);
}

#[given(
    "a snapshot taken at one cursor and, after later writes, another snapshot at a later cursor"
)]
fn two_snapshots(world: &mut World) {
    for (i, later) in ["later-1", "later-2"].into_iter().enumerate() {
        let path = world.dir.path().join(format!("snapshot-{i}.rannoch"));
        let at = world.g().events().committed().unwrap();
        world.g().snapshot(at, &path).unwrap();
        world.purge.snapshots.push(path);
        world
            .g()
            .write(&Write::new("app").unit(person(later, later)))
            .unwrap();
    }
}

#[when("the app purges the fact")]
fn purges_fact(world: &mut World) {
    purge_targets(world);
}

#[when("the app purges the fact and the queue is closed")]
fn purges_fact_closed(world: &mut World) {
    purge_targets(world);
    close_store(world);
}

#[then("no event in the store holds its key, kind, endpoints or properties")]
fn no_event_holds_fact(world: &mut World) {
    for gone in world.purge.gone.clone() {
        assert_no_event_holds(world.g(), &gone);
    }
    let g = world.g();
    assert_eq!(g.find("person", "p-gone").unwrap(), None);
    assert_eq!(
        g.hearings(&NodeRef::new("person", "p-stays"))
            .unwrap()
            .len(),
        1
    );
}

#[then("no snapshot holds anything of it")]
fn no_snapshot_holds_fact(world: &mut World) {
    close_store(world);
    for path in world.purge.snapshots.clone() {
        for (file, bytes) in files_of(std::slice::from_ref(&path)) {
            for gone in &world.purge.gone {
                assert!(
                    !find_bytes(&bytes, gone.as_bytes()),
                    "{} holds {gone}",
                    file.display()
                );
            }
        }
        let back = rannoch::open(&path, Some(&KEY)).unwrap();
        assert_eq!(back.find("person", "p-gone").unwrap(), None);
        assert!(
            back.hearings(&NodeRef::new("person", "p-gone"))
                .unwrap()
                .is_empty()
        );
        for gone in &world.purge.gone {
            assert_no_event_holds(&back, gone);
        }
    }
}

#[then(
    "a byte search of the store's files, its write-ahead log included, finds no copy of the token"
)]
fn no_token_in_files(world: &mut World) {
    assert!(world.purge.snapshots.is_empty());
    assert_no_bytes(world, TOKEN.as_bytes());
}

#[then("a byte search of each of the two snapshots finds no copy of the token")]
fn no_token_in_snapshots(world: &mut World) {
    assert_eq!(world.purge.snapshots.len(), 2);
    assert_no_bytes(world, TOKEN.as_bytes());
}

#[when("the graph is rebuilt")]
fn graph_rebuilt(world: &mut World) {
    let events = world.all_events();
    let path = world.dir.path().join("rebuilt.rannoch");
    world.rebuilt = Some(rannoch::rebuild(&events, path, Some(&KEY)).unwrap());
}

#[then("the rebuilt graph holds nothing of the purged fact")]
fn rebuilt_holds_nothing(world: &mut World) {
    let rebuilt = world.rebuilt.as_ref().expect("the graph was rebuilt");
    assert_eq!(rebuilt.find("person", "p-gone").unwrap(), None);
    assert!(
        rebuilt
            .hearings(&NodeRef::new("person", "p-gone"))
            .unwrap()
            .is_empty()
    );
    for gone in &world.purge.gone {
        assert_no_event_holds(rebuilt, gone);
    }
    let stays = rebuilt.find("person", "p-stays").unwrap().unwrap();
    assert_eq!(stays.properties["note"], Value::from("stays"));
}

/// The stream the stream scenario purges, by its write source.
const STREAM: &str = "zq-stream-71";

#[given("a graph holding a stream")]
fn graph_holding_stream(world: &mut World) {
    let g = purge_graph(world);
    let when = day(2026, 10, 1);
    g.write(
        &Write::new("app")
            .unit(person("p-both", "app's word"))
            .unit(person("p-other", "other")),
    )
    .unwrap();
    g.write(&Write::new("app").unit(person("p-c", "c").link(
        "knows",
        "p-c",
        "p-both",
        when.clone(),
    )))
    .unwrap();
    g.write(
        &Write::new(STREAM)
            .unit(person("p-stream", "stream only").link(
                "knows",
                "p-stream",
                "p-both",
                when.clone(),
            ))
            .unit(person("p-both", "the stream's word")),
    )
    .unwrap();
    let at = SystemTime::UNIX_EPOCH + Duration::from_hours(500_000);
    g.end_link("knows", "p-c", "p-both", at, when, STREAM)
        .unwrap();
    g.correct_link("knows", "p-stream", "p-both", STREAM)
        .unwrap();
    world.purge.targets.push(Purge::Stream {
        source: STREAM.into(),
    });
    world.purge.gone.extend([
        STREAM.into(),
        "p-stream".into(),
        "stream only".into(),
        "the stream's word".into(),
    ]);
}

#[when("the app purges the stream")]
fn purges_stream(world: &mut World) {
    purge_targets(world);
}

#[then("nothing of the stream remains in the store")]
fn nothing_of_stream(world: &mut World) {
    for gone in world.purge.gone.clone() {
        assert_no_event_holds(world.g(), &gone);
    }
    let g = world.g();
    assert_eq!(g.find("person", "p-stream").unwrap(), None);
    let both = g.hearings(&NodeRef::new("person", "p-both")).unwrap();
    assert_eq!(both.len(), 1);
    assert_eq!(both[0].source, "app");
    let node = g.find("person", "p-both").unwrap().unwrap();
    assert_eq!(
        (node.source.as_str(), &node.properties["note"]),
        ("app", &Value::from("app's word"))
    );
    assert_eq!(g.link_end("knows", "p-c", "p-both").unwrap(), None);
    assert!(
        g.link_hearings("knows", "p-stream", "p-both")
            .unwrap()
            .is_empty()
    );
    close_store(world);
    for gone in world.purge.gone.clone() {
        assert_no_bytes(world, gone.as_bytes());
    }
}

#[given("a graph holding facts about one node and two nodes declared part of it")]
fn graph_node_and_parts(world: &mut World) {
    let g = purge_graph(world);
    let when = day(2026, 10, 1);
    let named = |class: &str, key: &str| {
        Unit::new(class, key, [("name", format!("{key} named"))], when.clone())
    };
    g.write(
        &Write::new("app")
            .unit(named("house", "h-gone"))
            .unit(named("house", "h-stays")),
    )
    .unwrap();
    g.write(&Write::new("app").unit(named("room", "r-gone").link(
        "in_house",
        "r-gone",
        "h-gone",
        when.clone(),
    )))
    .unwrap();
    g.write(&Write::new("app").unit(named("shelf", "s-gone").link(
        "in_room",
        "s-gone",
        "r-gone",
        when.clone(),
    )))
    .unwrap();
    g.write(&Write::new("other").unit(named("room", "r-gone")))
        .unwrap();
    g.write(&Write::new("app").unit(named("room", "r-stays").link(
        "in_house",
        "r-stays",
        "h-stays",
        when.clone(),
    )))
    .unwrap();
    g.write(&Write::new("app").unit(person("p-lives", "lives").link(
        "lives",
        "p-lives",
        "h-gone",
        when.clone(),
    )))
    .unwrap();
    world.purge.targets.push(Purge::Entity {
        target: Target::Node(NodeRef::new("house", "h-gone")),
    });
    world
        .purge
        .gone
        .extend(["h-gone", "r-gone", "s-gone"].map(String::from));
}

#[when("the app purges everything about that node")]
fn purges_node(world: &mut World) {
    purge_targets(world);
}

#[then("nothing about the node or either of its parts remains in the store")]
fn nothing_about_node(world: &mut World) {
    for gone in world.purge.gone.clone() {
        assert_no_event_holds(world.g(), &gone);
    }
    let g = world.g();
    for (class, key) in [("house", "h-gone"), ("room", "r-gone"), ("shelf", "s-gone")] {
        assert_eq!(g.find(class, key).unwrap(), None, "{key}");
        assert!(
            g.hearings(&NodeRef::new(class, key)).unwrap().is_empty(),
            "{key}"
        );
    }
    assert!(g.find("room", "r-stays").unwrap().is_some());
    assert!(g.find("person", "p-lives").unwrap().is_some());
    assert!(
        g.link_hearings("lives", "p-lives", "h-gone")
            .unwrap()
            .is_empty()
    );
    assert!(g.open_links().unwrap().is_empty());
    close_store(world);
    for gone in world.purge.gone.clone() {
        assert_no_bytes(world, gone.as_bytes());
    }
}

#[given("a source whose hearings back more than one node and link")]
fn source_backs_many(world: &mut World) {
    let g = purge_graph(world);
    let when = day(2026, 10, 1);
    g.write(
        &Write::new("other")
            .unit(person("p-a", "other's a"))
            .unit(person("p-c", "c")),
    )
    .unwrap();
    g.write(
        &Write::new("gone-source")
            .unit(person("p-a", "gone's a").link("knows", "p-a", "p-c", when.clone()))
            .unit(person("p-b", "gone's b").link("knows", "p-b", "p-a", when.clone())),
    )
    .unwrap();
    g.write(&Write::new("other").unit(person("p-c", "c").link(
        "knows",
        "p-c",
        "p-a",
        when.clone(),
    )))
    .unwrap();
    world.purge.targets.push(Purge::Source {
        source: "gone-source".into(),
    });
}

#[when("the app purges that source")]
fn purges_source(world: &mut World) {
    purge_targets(world);
}

#[then("no node or link holds a hearing from it")]
fn no_hearing_from_source(world: &mut World) {
    let g = world.g();
    for key in ["p-a", "p-b", "p-c"] {
        for hearing in g.hearings(&NodeRef::new("person", key)).unwrap() {
            assert_ne!(hearing.source, "gone-source", "{key}");
        }
    }
    for (from, to) in [("p-a", "p-c"), ("p-b", "p-a"), ("p-c", "p-a")] {
        for hearing in g.link_hearings("knows", from, to).unwrap() {
            assert_ne!(hearing.source, "gone-source", "{from} {to}");
        }
    }
    assert_eq!(g.hearings(&NodeRef::new("person", "p-a")).unwrap().len(), 1);
    assert_eq!(
        g.find("person", "p-a").unwrap().unwrap().properties["note"],
        Value::from("other's a")
    );
    assert_eq!(g.find("person", "p-b").unwrap(), None);
    assert!(g.find_link("knows", "p-c", "p-a").unwrap().is_some());
    assert_no_event_holds(g, "gone-source");
}

#[given(
    "a graph holding a hearing written alone, and one write whose event names two hearings, each event id recorded by the test before the purge"
)]
fn alone_and_pair(world: &mut World) {
    let g = purge_graph(world);
    let alone = g
        .write(&Write::new("app").unit(person("p-alone", "alone")))
        .unwrap();
    let pair = g
        .write(
            &Write::new("app")
                .unit(person("p-one", "one"))
                .unit(person("p-two", "two")),
        )
        .unwrap();
    let targets: Vec<Purge> = ["p-alone", "p-one"]
        .into_iter()
        .map(|key| Purge::Fact {
            identity: g.hearings(&NodeRef::new("person", key)).unwrap()[0].identity,
        })
        .collect();
    world.purge.ids = vec![alone[0].id, pair[0].id];
    world.purge.targets = targets;
    world
        .purge
        .contents
        .push(node_content("p-alone", "alone", "app"));
    world
        .purge
        .contents
        .push(node_content("p-one", "one", "app"));
}

#[when(
    "the app purges the hearing written alone and one of the write's two hearings, and the queue is closed"
)]
fn purges_alone_and_one(world: &mut World) {
    purge_targets(world);
    close_store(world);
}

#[then(
    "hashing each purged hearing's exact content, plainly and under every key found in the store's files, matches nothing in them"
)]
fn no_hash_matches(world: &mut World) {
    assert_unhashed(world, Some(KEY));
}

/// Asserts that hashing each of the scenario's purged contents, plainly and under every key found
/// in the store's files (and under `key`, when given), matches nothing in them.
fn assert_unhashed(world: &World, key: Option<[u8; 32]>) {
    let files = store_files(world);
    let raw: HashSet<&[u8]> = files.iter().flat_map(|(_, b)| b.windows(32)).collect();
    let text: HashSet<&[u8]> = files.iter().flat_map(|(_, b)| b.windows(64)).collect();
    let mut keys: Vec<[u8; 32]> = raw
        .iter()
        .map(|w| <[u8; 32]>::try_from(*w).unwrap())
        .collect();
    keys.extend(key);
    for content in &world.purge.contents {
        let mut hashes = vec![blake3::hash(content)];
        hashes.extend(keys.iter().map(|key| blake3::keyed_hash(key, content)));
        for hash in hashes {
            assert!(
                !raw.contains(hash.as_bytes().as_slice()),
                "a purged hearing's hash is in the files"
            );
            assert!(
                !text.contains(hash.to_hex().as_bytes()),
                "a purged hearing's hash is in the files as text"
            );
        }
    }
}

#[then("no id in the store's files equals the event id of the hearing written alone")]
fn no_alone_id(world: &mut World) {
    let id = world.purge.ids[0];
    assert_no_bytes(world, id.to_string().as_bytes());
    let raw: uuid::Uuid = serde_json::from_value(serde_json::to_value(id).unwrap()).unwrap();
    assert_no_bytes(world, raw.as_bytes());
}

#[then("a write naming the two-hearing write's event id as its derived-from source is accepted")]
fn pair_id_accepted(world: &mut World) {
    let pair = world.purge.ids[1];
    let g = world.open();
    g.write(
        &Write::new("app")
            .unit(person("p-derived", "derived"))
            .derived_from([pair]),
    )
    .unwrap();
    assert_eq!(
        g.derived(pair).unwrap(),
        [FactRef::Node(NodeRef::new("person", "p-derived"))]
    );
    assert_eq!(
        g.hearings(&NodeRef::new("person", "p-two")).unwrap()[0].event,
        pair
    );
}

#[given("a node heard from two sources")]
fn node_two_sources(world: &mut World) {
    let g = purge_graph(world);
    g.write(&Write::new("first").unit(person("p-n", "first's word")))
        .unwrap();
    g.write(&Write::new("second").unit(person("p-n", "second's word")))
        .unwrap();
    g.write(&Write::new("first").unit(person("p-n", "first's next word")))
        .unwrap();
    let node = NodeRef::new("person", "p-n");
    let second: Vec<Hearing> = g
        .hearings(&node)
        .unwrap()
        .into_iter()
        .filter(|h| h.source == "second")
        .collect();
    world.purge.kept = vec![(node.clone(), second)];
    world.purge.targets.push(Purge::EntityFromSource {
        target: Target::Node(node),
        source: "first".into(),
    });
}

#[when("the app purges what the first source said about that node")]
fn purges_first_source(world: &mut World) {
    purge_targets(world);
}

#[then("the node holds no hearing from the first source")]
fn no_first_hearing(world: &mut World) {
    let hearings = world.g().hearings(&NodeRef::new("person", "p-n")).unwrap();
    assert!(hearings.iter().all(|h| h.source != "first"), "{hearings:?}");
    assert_no_event_holds(world.g(), "first's word");
}

#[then("it still holds every hearing from the second source")]
fn second_hearings_kept(world: &mut World) {
    let (node, kept) = world.purge.kept[0].clone();
    assert_eq!(world.g().hearings(&node).unwrap(), kept);
    let found = world.g().find("person", "p-n").unwrap().unwrap();
    assert_eq!(
        (found.source.as_str(), &found.properties["note"]),
        ("second", &Value::from("second's word"))
    );
}

// FR-RANNOCH-072: a purged hearing is as if never recorded, one arriving after its rule included.

/// The node the standing-rule scenarios hear, in a class whose history is ordered by time.
fn visit() -> NodeRef {
    NodeRef::new("visit", "v-72")
}

/// The point `secs` seconds after the epoch.
fn secs(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

/// Writes the visit's hearing from `source` at `secs` on its axis, noting `note`; returns its
/// events.
fn hear_visit(g: &mut Graph, source: &str, at: u64, note: &str) -> Vec<Event> {
    let unit = Unit::new("visit", "v-72", [("note", note)], day(2026, 10, 1)).at(secs(at));
    g.write(&Write::new(source).unit(unit)).unwrap()
}

/// The purge scenarios' graph with a time-ordered `visit` class.
fn visit_graph(world: &mut World) -> &mut Graph {
    let g = purge_graph(world);
    let mut class = Class::new("visit", ["note"]);
    class.history = HistoryKey::Time;
    g.declare(&Declaration::Class(class)).unwrap();
    g
}

/// The visit's one hearing from `source`, as a fact purge.
fn fact_from(g: &Graph, source: &str) -> Purge {
    let hearings = g.hearings(&visit()).unwrap();
    let mut from = hearings.iter().filter(|h| h.source == source);
    let hearing = from.next().expect("a hearing from the source");
    assert!(from.next().is_none(), "one hearing from {source}");
    Purge::Fact {
        identity: hearing.identity,
    }
}

/// What is seen of the visit in `g`.
fn seen_visit(g: &Graph) -> rannoch::Seen {
    g.seen(&visit()).unwrap().expect("the visit was heard")
}

#[given("a fact heard from more than one source")]
fn heard_from_many(world: &mut World) {
    // Recorded a, b, c, d in that order; on the axis b (1000), d (1500), a (2000), c (3000).
    let g = visit_graph(world);
    for (source, at) in [("a", 2000), ("b", 1000), ("c", 3000), ("d", 1500)] {
        hear_visit(g, source, at, source);
    }
    let seen = seen_visit(g);
    assert_eq!(
        (
            seen.first_heard.source.as_str(),
            seen.last_heard.source.as_str(),
            seen.first_recorded.source.as_str()
        ),
        ("b", "c", "a")
    );
}

#[when("the app purges the hearing earliest on the class's declared axis")]
fn purges_earliest(world: &mut World) {
    let purge = fact_from(world.g(), "b");
    world.purge.targets.push(purge);
    purge_targets(world);
}

#[then("the fact's first heard is the earliest surviving hearing on the axis")]
fn first_heard_survivor(world: &mut World) {
    let seen = seen_visit(world.g());
    assert_eq!(
        (seen.first_heard.source.as_str(), seen.first_heard.point),
        ("d", secs(1500))
    );
    assert_eq!(seen.count, 3);
}

#[when("the app purges the hearing latest on the class's declared axis")]
fn purges_latest(world: &mut World) {
    let purge = fact_from(world.g(), "c");
    world.purge.targets.push(purge);
    purge_targets(world);
}

#[then("the fact's last heard is the latest surviving hearing on the axis")]
fn last_heard_survivor(world: &mut World) {
    let seen = seen_visit(world.g());
    assert_eq!(
        (seen.last_heard.source.as_str(), seen.last_heard.point),
        ("a", secs(2000))
    );
    assert_eq!(seen.count, 3);
}

#[when("the app purges the hearing this store recorded first")]
fn purges_first_recorded(world: &mut World) {
    let purge = fact_from(world.g(), "a");
    world.purge.targets.push(purge);
    purge_targets(world);
}

#[then("the fact's first recorded is the surviving hearing this store recorded first")]
fn first_recorded_survivor(world: &mut World) {
    let seen = seen_visit(world.g());
    assert_eq!(
        (
            seen.first_recorded.source.as_str(),
            seen.first_recorded.point
        ),
        ("b", secs(1000))
    );
    assert_eq!(seen.count, 3);
}

#[given("a fact heard by more than one voice, one of them only once")]
fn heard_by_voices(world: &mut World) {
    let g = visit_graph(world);
    hear_visit(g, "often", 1000, "often's first");
    hear_visit(g, "once", 2000, "once's only");
    hear_visit(g, "often", 3000, "often's second");
    assert_eq!(seen_visit(g).count, 3);
}

#[when("the app purges that voice's only hearing and reads the distinct count of voices")]
fn purges_only_hearing(world: &mut World) {
    let purge = fact_from(world.g(), "once");
    world.purge.targets.push(purge);
    purge_targets(world);
}

#[then("the count no longer counts that voice")]
fn count_drops_voice(world: &mut World) {
    let g = world.g();
    let hearings = g.hearings(&visit()).unwrap();
    assert!(hearings.iter().all(|h| h.source == "often"), "{hearings:?}");
    assert_eq!(seen_visit(g).count, 2);
}

/// The note the purged source's late hearing carries: found nowhere else in a scenario.
const LATE: &str = "zq-late-hearing-72";

#[given("a fact heard from two sources, and a purge rule for one of them")]
fn two_sources_and_rule(world: &mut World) {
    let g = visit_graph(world);
    hear_visit(g, "keeps", 1000, "kept first");
    hear_visit(g, "purged", 2000, "purged's word");
    hear_visit(g, "keeps", 3000, "kept last");
    world.purge.targets.push(Purge::EntityFromSource {
        target: Target::Node(visit()),
        source: "purged".into(),
    });
    purge_targets(world);
}

#[given(
    "a fact heard from two sources, a purge rule for one of them, and a snapshot taken after the purge"
)]
fn two_sources_rule_snapshot(world: &mut World) {
    two_sources_and_rule(world);
    let path = world.dir.path().join("snapshot-72.rannoch");
    let at = world.g().events().committed().unwrap();
    world.snapshot = Some(world.g().snapshot(at, &path).unwrap());
}

/// Writes the purged source's late hearing, earlier on the axis than any it heard, into `g`, and
/// asserts the write returns as if it never came.
fn late_hearing(g: &mut Graph) {
    let events = hear_visit(g, "purged", 500, LATE);
    assert!(events.is_empty(), "{events:?}");
}

#[when("the app writes a hearing of the fact from the purged source")]
fn writes_late_hearing(world: &mut World) {
    late_hearing(world.g());
}

#[when(
    "the graph is rebuilt from the snapshot and the app writes a hearing of the fact from the purged source"
)]
fn rebuilt_and_late_hearing(world: &mut World) {
    let path = world.dir.path().join("rebuilt-72.rannoch");
    let snapshot = world.snapshot.as_ref().expect("a snapshot was taken");
    let mut rebuilt = snapshot.rebuild(&[], path, Some(&KEY)).unwrap();
    late_hearing(&mut rebuilt);
    world.rebuilt = Some(rebuilt);
}

/// Asserts `g` holds nothing of the late hearing, and sees the visit as the keeping source's two
/// hearings alone.
fn only_survivor(g: &Graph) {
    let hearings = g.hearings(&visit()).unwrap();
    assert!(hearings.iter().all(|h| h.source == "keeps"), "{hearings:?}");
    assert_no_event_holds(g, LATE);
    let seen = seen_visit(g);
    assert_eq!(
        (
            seen.first_heard.point,
            seen.last_heard.point,
            seen.count,
            seen.first_heard.source.as_str(),
            seen.last_heard.source.as_str()
        ),
        (secs(1000), secs(3000), 2, "keeps", "keeps")
    );
    let node = g.find("visit", "v-72").unwrap().unwrap();
    assert_eq!(node.properties["note"], Value::from("kept last"));
}

#[then(
    "nothing of that hearing remains in the store, and the fact's first heard, last heard and distinct count are those of the surviving source alone"
)]
fn late_hearing_gone(world: &mut World) {
    only_survivor(world.g());
}

#[then(
    "nothing of that hearing remains in the rebuilt graph, and the fact's first heard, last heard and distinct count are those of the surviving source alone"
)]
fn late_hearing_gone_rebuilt(world: &mut World) {
    only_survivor(world.rebuilt.as_ref().expect("the graph was rebuilt"));
}

// ---------- purge: the purge event carries only the rule (FR-RANNOCH-078) ----------

/// The follower that reads past a purge.
const FOLLOWER_78: &str = "follower-78";

/// Everything of the purged fact no purge event or follower may see: its property value, its
/// key, and each of its hearings' event ids and content identity, as hex and as bytes.
fn purged_fact_marks(world: &World) -> Vec<String> {
    let mut marks = vec![TOKEN.to_owned(), "\"p-gone\"".to_owned()];
    for hearing in &world.hearings["purged"] {
        marks.push(hearing.event.to_string());
        marks.extend(hearing.aliases.iter().map(ToString::to_string));
        marks.push(
            blake3::Hash::from_bytes(hearing.identity)
                .to_hex()
                .to_string(),
        );
        marks.push(serde_json::to_string(&hearing.identity).unwrap());
    }
    marks
}

/// Asserts `text` holds none of `marks`.
fn assert_holds_none(text: &str, marks: &[String]) {
    for mark in marks {
        assert!(!text.contains(mark.as_str()), "{text} holds {mark}");
    }
}

#[given("a graph holding a fact")]
fn graph_holding_a_fact(world: &mut World) {
    token_fact(world);
    // Its one hearing, kept so the scenario can name its event ids and content identity.
    let hearings = world
        .g()
        .hearings(&NodeRef::new("person", "p-gone"))
        .unwrap();
    world.hearings.insert("purged".into(), hearings);
}

#[when("the app purges the fact under a rule")]
fn purges_fact_under_rule(world: &mut World) {
    world.cursor = world.g().events().committed().unwrap();
    purge_targets(world);
}

#[then("the event log gains a purge event carrying the rule's name")]
fn log_gains_rule_event(world: &mut World) {
    let cursor = world.cursor;
    let gained = world.g().events().since(cursor).unwrap();
    assert_eq!(gained.len(), 1, "{gained:?}");
    let Change::Purged { rule, purge, .. } = &gained[0].change else {
        panic!("{gained:?}");
    };
    assert_eq!(rule, "rule-71");
    assert!(matches!(purge, Purge::Fact { .. }), "{purge:?}");
}

#[then("the purge event holds no id or content of the fact and no copy of the fact's key")]
fn rule_event_names_nothing(world: &mut World) {
    let marks = purged_fact_marks(world);
    let cursor = world.cursor;
    let purge = world.g().events().since(cursor).unwrap()[0].id;
    let file = rusqlite::Connection::open(world.path()).unwrap();
    let body: String = file
        .query_row(
            "SELECT body FROM _events WHERE id = ?1",
            [purge.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(body.contains("rule-71"), "{body}");
    assert_holds_none(&body, &marks);
}

#[given("a follower whose cursor is before a fact was written")]
fn follower_before_fact(world: &mut World) {
    let g = purge_graph(world);
    g.write(&Write::new("app").unit(person("p-other", "before")))
        .unwrap();
    let log = g.events();
    log.register(FOLLOWER_78).unwrap();
    log.advance(FOLLOWER_78, log.committed().unwrap()).unwrap();
    // The fact is written alone, so the purge removes its write event whole.
    g.write(&Write::new("app").unit(person("p-gone", TOKEN)))
        .unwrap();
    g.write(&Write::new("app").unit(person("p-after", "after")))
        .unwrap();
    let hearings = g.hearings(&NodeRef::new("person", "p-gone")).unwrap();
    world.purge.targets.push(Purge::Fact {
        identity: hearings[0].identity,
    });
    world.hearings.insert("purged".into(), hearings);
}

#[when("the app purges the fact and the follower reads since its cursor")]
fn purges_and_follower_reads(world: &mut World) {
    purge_targets(world);
    let events = world.g().events();
    let (_, cursor) = events
        .followers()
        .unwrap()
        .into_iter()
        .find(|(name, _)| name == FOLLOWER_78)
        .unwrap();
    world.read = events.since(cursor).unwrap();
}

#[then("the follower receives no id or content of the fact")]
fn follower_receives_nothing_purged(world: &mut World) {
    let marks = purged_fact_marks(world);
    assert!(
        world
            .read
            .iter()
            .any(|e| matches!(&e.change, Change::Purged { rule, .. } if rule == "rule-71")),
        "the follower learns of the purge from its rule event: {:?}",
        world.read
    );
    for event in &world.read {
        let text = format!(
            "{} {}",
            event.id,
            serde_json::to_string(&event.change).unwrap()
        );
        assert_holds_none(&text, &marks);
    }
}

#[when(
    "the graph is rebuilt from its events and the app writes a hearing of the fact from the purged source"
)]
fn rebuilt_from_events_and_late_hearing(world: &mut World) {
    let events = world.all_events();
    let path = world.dir.path().join("rebuilt-78.rannoch");
    let mut rebuilt = rannoch::rebuild(&events, path, Some(&KEY)).unwrap();
    late_hearing(&mut rebuilt);
    world.rebuilt = Some(rebuilt);
}

/// The key of the node the standing rule purges: found nowhere else in a scenario's files.
const STANDING: &str = "zq-standing-key-72";

#[given("a graph holding a node whose key is a unique token")]
fn node_keyed_by_token(world: &mut World) {
    let g = purge_graph(world);
    g.write(
        &Write::new("app")
            .unit(person(STANDING, "standing"))
            .unit(person("p-stays", "stays")),
    )
    .unwrap();
    world.purge.targets.push(Purge::Entity {
        target: Target::Node(NodeRef::new("person", STANDING)),
    });
    world.purge.contents.push(STANDING.as_bytes().to_vec());
}

#[when("the app purges the node, takes a snapshot and closes the queue")]
fn purges_node_snapshots_closes(world: &mut World) {
    purge_targets(world);
    let path = world.dir.path().join("snapshot-72.rannoch");
    let at = world.g().events().committed().unwrap();
    world.g().snapshot(at, &path).unwrap();
    world.purge.snapshots.push(path);
    close_store(world);
}

#[then("hashing the key under every key found in the store's files matches nothing in them")]
fn key_unhashed(world: &mut World) {
    assert_unhashed(world, None);
}

#[when("the graph is reopened and the app writes a hearing of that node")]
fn reopened_and_writes_node(world: &mut World) {
    let g = world.open();
    let events = g
        .write(&Write::new("app").unit(person(STANDING, "back again")))
        .unwrap();
    assert!(events.is_empty(), "{events:?}");
}

#[then("nothing of that hearing remains in the store")]
fn nothing_of_node_hearing(world: &mut World) {
    let g = world.g();
    let node = NodeRef::new("person", STANDING);
    assert_eq!(g.find("person", STANDING).unwrap(), None);
    assert!(g.hearings(&node).unwrap().is_empty());
    assert_eq!(g.seen(&node).unwrap(), None);
    assert_no_event_holds(g, STANDING);
    assert_no_event_holds(g, "back again");
    assert!(g.find("person", "p-stays").unwrap().is_some());
}

// FR-RANNOCH-066: an ending is a version of the link, held whether or not a live hearing of
// the link arrived before it.

#[given(
    "a graph rebuilt from events in which a link's ending arrives before any live hearing of that link"
)]
fn ending_before_hearing(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    follows_between_visits(world);
    let facts = [LinkAt::Ended("b", 20), LinkAt::Heard("b", "crawler", 10)];
    world.graph = Some(graph_of(world, &facts));
}

#[when("the link's state is read at a point after the ending")]
fn state_after_ending(world: &mut World) {
    let state = world
        .g()
        .link_state_at("follows", "b", "a", At::Time(at(25)));
    world.states = vec![state.unwrap()];
}

#[then("the link reads as ended at the ending's point")]
fn ended_at_endings_point(world: &mut World) {
    let state = world.states[0]
        .as_ref()
        .expect("the link has a version by then");
    assert!(state.ended, "{state:?}");
    assert_eq!(state.point, at(20));
}

#[given("a link ended at point 10 and first heard alive at point 20")]
fn ended_then_first_heard(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    follows_between_visits(world);
    let facts = [LinkAt::Ended("b", 10), LinkAt::Heard("b", "crawler", 20)];
    world.graph = Some(graph_of(world, &facts));
}

#[when("its state is read at point 15 and at point 25")]
fn state_at_15_and_25(world: &mut World) {
    world.states = [15, 25]
        .map(|n| {
            let state = world
                .g()
                .link_state_at("follows", "b", "a", At::Time(at(n)));
            state.unwrap()
        })
        .to_vec();
}

#[then("it reads as ended at point 15 and live at point 25")]
fn ended_at_15_live_at_25(world: &mut World) {
    let [Some(early), Some(late)] = &world.states[..] else {
        panic!("the link has a version at both points: {:?}", world.states);
    };
    assert!(early.ended && early.point == at(10), "{early:?}");
    assert!(!late.ended && late.point == at(20), "{late:?}");
}

// FR-RANNOCH-084: the same events in any arrival order, declarations first, give the same
// answers to every read except first recorded.

/// Every node of the arrival-order graph, by class and key.
const ARRIVAL_NODES: [(&str, &str); 6] = [
    ("visit", "a"),
    ("visit", "b"),
    ("visit", "c"),
    ("visit", "z"),
    ("note", "n1"),
    ("note", "n2"),
];

/// Every link of the arrival-order graph: relation, from and to.
const ARRIVAL_LINKS: [(&str, &str, &str); 5] = [
    ("follows", "b", "a"),
    ("follows", "c", "z"),
    ("follows", "a", "b"),
    ("about", "n1", "a"),
    ("about", "n2", "z"),
];

/// What a graph answers about `seen`, but for first recorded, which is per store.
fn seen_but_first_recorded(seen: Option<rannoch::Seen>) -> String {
    format!(
        "{:?}",
        seen.map(|s| (s.first_heard, s.last_heard, s.first_true, s.count, s.ended))
    )
}

/// The answers `g` gives to find, state at a point, reads as of a valid time, first and last
/// heard and counts, for every node and link of the arrival-order graph, and its open links.
fn arrival_answers(g: &Graph) -> Vec<String> {
    let points = [5, 15, 25, 35, 45, 100_000].map(|n| At::Time(at(n)));
    let dates = ["2025-01-01", "2026-06-01"].map(date);
    let mut answers = Vec::new();
    for (class, key) in ARRIVAL_NODES {
        let node = NodeRef::new(class, key);
        answers.push(format!("{:?}", g.find(class, key).unwrap().map(node_facts)));
        for at in &points {
            answers.push(format!("{:?}", g.state_at(&node, *at).unwrap()));
        }
        for as_of in &dates {
            let found = g.find_as_of(class, key, as_of, None).unwrap();
            answers.push(format!("{:?}", found.map(node_facts)));
        }
        answers.push(seen_but_first_recorded(g.seen(&node).unwrap()));
    }
    for (relation, from, to) in ARRIVAL_LINKS {
        let link = g.find_link(relation, from, to).unwrap();
        answers.push(format!("{:?}", link.map(link_facts)));
        for at in &points {
            answers.push(format!(
                "{:?}",
                g.link_state_at(relation, from, to, *at).unwrap()
            ));
        }
        answers.push(seen_but_first_recorded(
            g.link_seen(relation, from, to).unwrap(),
        ));
    }
    answers.push(format!("{:?}", g.open_links().unwrap()));
    answers
}

#[given(
    "a graph holding nodes and links of a time-keyed class and of a class with no key, some of whose versions arrived after newer ones"
)]
fn arrival_graph(world: &mut World) {
    let g = world.open();
    declare_kind(g, "time-keyed");
    declare_kind(g, "keyless");
    for (name, from) in [("follows", "visit"), ("about", "note")] {
        let relation = Relation::new(name, from, "visit", false);
        g.declare(&Declaration::Relation(relation)).unwrap();
    }
    let visit = |key: &str, text: &str, valid: ValidTime, n: u64| {
        Unit::new("visit", key, [("text", text)], valid).at(at(n))
    };
    let note = |key: &str, text: &str| Unit::new("note", key, [("text", text)], day(2026, 2, 1));
    let writes = [
        Write::new("crawler").unit(visit("a", "a at 10", day(2026, 1, 1), 10)),
        Write::new("crawler").unit(visit("b", "b at 30", day(2026, 3, 1), 30).link(
            "follows",
            "b",
            "a",
            day(2026, 3, 1),
        )),
        // Older on the axis than the version held, arriving after it.
        Write::new("mirror").unit(visit("b", "b at 20", day(2025, 6, 1), 20)),
        // z has not been heard: the link waits open until it is.
        Write::new("crawler").unit(visit("c", "c at 15", day(2026, 1, 1), 15).link(
            "follows",
            "c",
            "z",
            day(2026, 1, 1),
        )),
        Write::new("notes").unit(note("n1", "n1").link("about", "n1", "a", day(2026, 2, 1))),
        Write::new("notes").unit(note("n2", "n2").link("about", "n2", "z", day(2026, 2, 1))),
        Write::new("mirror").unit(visit("a", "a at 5", day(2024, 1, 1), 5)),
        Write::new("crawler").unit(visit("z", "z at 40", day(2026, 4, 1), 40)),
        Write::new("archive").unit(note("n1", "n1 again")),
        Write::new("crawler").unit(visit("a", "a at 50", day(2026, 5, 1), 50).link(
            "follows",
            "a",
            "b",
            day(2026, 5, 1),
        )),
    ];
    for write in &writes {
        world.g().write(write).unwrap();
        // Each keyless version takes its own record time as its point.
        std::thread::sleep(Duration::from_millis(2));
    }
    let closed = world.all_events();
    assert!(closed.iter().any(|e| matches!(e.change, Change::Closed(_))));
}

#[when(
    "its declarations and then its other events are given to 20 new graphs, the other events in a different random order in each, some links arriving before their ends"
)]
#[when(
    "its declarations and then its other events are given to 20 new graphs, the other events in a different random order in each"
)]
fn given_in_20_orders(world: &mut World) {
    let (declared, others): (Vec<Event>, Vec<Event>) = world
        .all_events()
        .into_iter()
        .partition(|e| matches!(e.change, Change::Declared(_)));
    // A fixed seed, so each run gives the graphs the same orders.
    let mut rng = fastrand::Rng::with_seed(84);
    for i in 0..20 {
        let mut order = others.clone();
        rng.shuffle(&mut order);
        let events: Vec<Event> = declared.iter().cloned().chain(order).collect();
        let path = world.dir.path().join(format!("order-{i}.rannoch"));
        world
            .graphs
            .push(rannoch::rebuild(&events, path, Some(&KEY)).unwrap());
    }
}

#[then(
    "every graph returns the same answers as the original to find, state at a point, reads as of a valid time, first and last heard, and counts, for every node and link"
)]
fn same_answers_in_every_order(world: &mut World) {
    let original = arrival_answers(world.g());
    for (i, g) in world.graphs.iter().enumerate() {
        assert_eq!(arrival_answers(g), original, "graph {i}");
    }
}

/// Builds a new graph from the world graph's events and `events`, in that order.
fn given_events(world: &mut World, events: &[Event], name: &str) -> Graph {
    let declared = world
        .log
        .iter()
        .take_while(|e| matches!(e.change, Change::Declared(_)));
    let events: Vec<Event> = declared.chain(events).cloned().collect();
    let path = world.dir.path().join(format!("{name}.rannoch"));
    rannoch::rebuild(&events, path, Some(&KEY)).unwrap()
}

/// A and B, the two deliveries of one hearing, as the world's log ends with them.
fn deliveries(world: &World) -> (Event, Event) {
    let n = world.log.len();
    (world.log[n - 2].clone(), world.log[n - 1].clone())
}

#[when("one new graph is given A then B and another new graph is given B then A")]
fn given_both_orders(world: &mut World) {
    let (a, b) = deliveries(world);
    let forward = given_events(world, &[a.clone(), b.clone()], "a-then-b");
    let backward = given_events(world, &[b, a], "b-then-a");
    world.graphs.extend([forward, backward]);
}

#[then("both graphs name the hearing by A")]
fn both_name_a(world: &mut World) {
    let (a, b) = deliveries(world);
    for g in &world.graphs {
        let hearings = g.hearings(&NodeRef::new("visit", "k1")).unwrap();
        assert_eq!(hearings.len(), 1, "{hearings:?}");
        assert_eq!(
            (hearings[0].event, &hearings[0].aliases),
            (a.id, &vec![b.id])
        );
    }
}

#[given(
    "another version of the same node at the same point, with other properties, under an event id between A and B"
)]
fn other_version_between(world: &mut World) {
    let (a, b) = deliveries(world);
    let Change::Wrote {
        source, mut added, ..
    } = a.change.clone()
    else {
        panic!("A is a write: {a:?}");
    };
    let Some(Fact::Node(node)) = added.first_mut() else {
        panic!("A writes a node: {added:?}");
    };
    node.properties.insert("text".into(), Value::from("other"));
    // A's id plus one: minted in the same millisecond as A, before B.
    let id = serde_json::to_value(a.id).unwrap();
    let id = uuid::Uuid::parse_str(id.as_str().unwrap())
        .unwrap()
        .as_u128()
        + 1;
    let id: EventId = serde_json::from_value(uuid::Uuid::from_u128(id).to_string().into()).unwrap();
    assert!(a.id < id && id < b.id);
    let last: i64 =
        serde_json::from_value(serde_json::to_value(a.cursor.max(b.cursor)).unwrap()).unwrap();
    let other = Event {
        id,
        cursor: serde_json::from_value(serde_json::json!(last + 1)).unwrap(),
        change: Change::Wrote {
            source,
            added,
            wrong_kind: Vec::new(),
            check: None,
        },
    };
    world.log.insert(world.log.len() - 1, other);
}

#[when(
    "one new graph is given A, the other version, then B, and another new graph is given B, the other version, then A"
)]
fn given_around_the_other(world: &mut World) {
    let n = world.log.len();
    let (a, other, b) = (
        world.log[n - 3].clone(),
        world.log[n - 2].clone(),
        world.log[n - 1].clone(),
    );
    let forward = given_events(world, &[a.clone(), other.clone(), b.clone()], "a-other-b");
    let backward = given_events(world, &[b, other, a], "b-other-a");
    world.graphs.extend([forward, backward]);
}

/// The hearing delivered under A and B wins the tie: its occurrence under B has the larger
/// (point, event id).
#[then("find returns the hearing delivered under A and B as the node's state in both graphs")]
fn delivered_hearing_in_both(world: &mut World) {
    for g in &world.graphs {
        let found = g.find("visit", "k1").unwrap().expect("the node is held");
        assert_eq!(text_of(&found), "words");
    }
}

#[given(
    "a graph of a time-keyed class holding links that have ended, one of them ended twice at the same point under different event ids, and nodes heard with different valid times, some versions arriving after newer ones"
)]
fn ended_links_graph(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    follows_between_visits(world);
    for (key, n, valid) in [
        ("a", 30, day(2025, 3, 1)),
        // Older on the axis than the version held, arriving after it.
        ("a", 20, day(2024, 1, 1)),
        ("b", 25, day(2023, 5, 1)),
    ] {
        let unit = Unit::new("visit", key, [("text", key)], valid).at(at(n));
        world.g().write(&Write::new("mirror").unit(unit)).unwrap();
    }
    let facts = [
        LinkAt::Heard("b", "crawler", 10),
        LinkAt::Ended("b", 20),
        LinkAt::Ended("b", 20),
        LinkAt::Heard("c", "crawler", 10),
        LinkAt::Ended("c", 30),
    ];
    world.graph = Some(graph_of(world, &facts));
}

/// When each link of `g` ended and each node's and link's first true.
fn ended_and_first_true(g: &Graph) -> Vec<String> {
    let mut answers = Vec::new();
    for from in ["b", "c"] {
        answers.push(format!("{:?}", g.link_end("follows", from, "a").unwrap()));
        let seen = g.link_seen("follows", from, "a").unwrap().unwrap();
        answers.push(format!("{:?}", (seen.ended, seen.first_true)));
    }
    for key in ["a", "b", "c"] {
        let seen = g.seen(&NodeRef::new("visit", key)).unwrap().unwrap();
        answers.push(format!("{:?}", seen.first_true));
    }
    answers
}

#[then(
    "every graph returns the same answer as the original to when each link ended and to each node's and link's first true"
)]
fn same_endings_and_first_true(world: &mut World) {
    let original = ended_and_first_true(world.g());
    for (i, g) in world.graphs.iter().enumerate() {
        assert_eq!(ended_and_first_true(g), original, "graph {i}");
    }
}

#[given(
    "a graph of a time-keyed class holding one node replaced twice at the same point under different event ids, and another node replaced twice at different points"
)]
fn replaced_twice_graph(world: &mut World) {
    declare_kind(world.open(), "time-keyed");
    let mut write = Write::new("crawler");
    for key in ["a", "b", "c", "d", "e"] {
        write = write.unit(Unit::new("visit", key, [("text", key)], day(2026, 1, 1)).at(at(1)));
    }
    world.g().write(&write).unwrap();
    let mut events = world.all_events();
    // The later replacement of d arrives first.
    for (old, new, n) in [
        ("a", "b", 10),
        ("a", "c", 10),
        ("d", "b", 20),
        ("d", "e", 10),
    ] {
        let cursor = serde_json::from_value(serde_json::json!(events.len() + 1)).unwrap();
        events.push(Event {
            id: id_at(at(n)),
            cursor,
            change: Change::Replaced {
                class: "visit".into(),
                old: old.into(),
                new: new.into(),
            },
        });
    }
    let path = world.dir.path().join("replaced.rannoch");
    world.graph = Some(rannoch::rebuild(&events, path, Some(&KEY)).unwrap());
}

/// The fate `g` gives each replaced node.
fn fates_of(g: &Graph) -> Vec<Option<NodeRef>> {
    ["a", "d"]
        .map(|key| g.describe(&NodeRef::new("visit", key)).unwrap().replaced_by)
        .to_vec()
}

#[then("every graph returns the same fate as the original for each node")]
fn same_fates(world: &mut World) {
    let original = fates_of(world.g());
    assert!(original.iter().all(Option::is_some), "{original:?}");
    for (i, g) in world.graphs.iter().enumerate() {
        assert_eq!(fates_of(g), original, "graph {i}");
    }
}

scenarios!("tests", fixtures = [world: World]);
