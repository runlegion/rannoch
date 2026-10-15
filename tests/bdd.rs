//! Runs every `.feature` file under `tests/`. Each scenario is one acceptance criterion of a
//! legion requirement; a scenario with no matching step code fails, naming the scenario.
//!
//! Steps act and observe through rannoch's public API. Only fixture setup touches the file
//! directly: the trigger that makes a write's third piece fail, and reading the file's table
//! names so a refusal can be checked against them.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::needless_pass_by_value
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::SystemTime;

use rannoch::{
    Change, Class, Count, Cursor, Date, Declaration, Error, Event, Fact, Graph, GroupBy, Node,
    NodeRef, Relation, Step, Tally, Unit, ValidTime, Write, WriteError,
};
use rstest_bdd_macros::{given, scenarios, then, when};
use tempfile::TempDir;

const NONE: [(&str, &str); 0] = [];

/// The piece size these fixtures are shaped to: rannoch's own.
const PIECE_CHANGES: usize = 1_000;

// ---------- fixtures ----------

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
/// write. Returns the events the writes returned, in order.
fn t2(g: &mut Graph) -> Vec<Event> {
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    let when = day(2026, 10, 1);
    let mut written = Vec::new();
    let mut put = |g: &mut Graph, unit: Unit| {
        written.extend(g.write(&Write::new("eavesdrop").unit(unit)).unwrap());
    };
    for f in 0..20 {
        put(g, Unit::new("finding", format!("f{f}"), NONE, when.clone()));
    }
    for v in 0..500 {
        put(g, Unit::new("voice", format!("v{v}"), NONE, when.clone()));
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

/// The T10 statements the fixture marks valid on or before `as_of`, with their voices.
fn t10_valid_by(as_of: &Date) -> Vec<(String, String)> {
    t10_statements()
        .into_iter()
        .filter(|(_, _, valid)| matches!(valid, ValidTime::On(date) if date <= as_of))
        .map(|(statement, voice, _)| (statement, voice))
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

// ---------- the world ----------

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
    before: Vec<Tally>,
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
        before: Vec::new(),
    }
}

impl World {
    fn path(&self) -> PathBuf {
        self.dir.path().join("graph.rannoch")
    }

    /// Opens the graph file, keeping the handle.
    fn open(&mut self) -> &mut Graph {
        self.graph = Some(rannoch::open(self.path()).unwrap());
        self.g()
    }

    fn g(&mut self) -> &mut Graph {
        self.graph.as_mut().expect("the graph is open")
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
            property: "need".into()
        })
    );
}

#[then("the statement written before reads need as absent")]
fn need_absent(world: &mut World) {
    assert!(
        world.g().ontology().classes["statement"]
            .properties
            .contains("need")
    );
    let s1 = world.find("statement", "s1");
    assert_eq!(s1.properties.get("text").map(String::as_str), Some("hi"));
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
        let mut g = rannoch::open(path).unwrap();
        g.write(&write).unwrap();
        done.store(true, Ordering::SeqCst);
    }));
}

#[when("a reader counts statements and their voices during the burst")]
fn reader_counts(world: &mut World) {
    let reader = rannoch::open(world.path()).unwrap();
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
    world.write(&Write::new("swtor").unit(Unit::new("voice", "v1", NONE, day(2021, 3, 2))));
    assert!(world.written.is_empty());
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
    assert_eq!(s05.properties["text"], "text of s05, corrected");
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
        for (parent, _) in g.walk(&root, &["answers"], 1, usize::MAX).unwrap() {
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
            .unwrap();
        assert!(
            walked.is_empty(),
            "{reply} reached {walked:?}, its parent {parent} never arrived"
        );
    }
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
        let mut g = rannoch::open(path).unwrap();
        g.write(&write).unwrap();
        done.store(true, Ordering::SeqCst);
    }));
}

#[when("a reader walks answers links during the burst")]
fn reader_walks_answers(world: &mut World) {
    let reader = rannoch::open(world.path()).unwrap();
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
                Ok(parents) => parents,
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
    // t2 declared nothing new: its declarations were already on the stream.
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
    let mut supports: Vec<(String, String)> = Vec::new();
    for event in world.all_events() {
        let links = match event.change {
            Change::Wrote { added, .. } => added
                .into_iter()
                .filter_map(|f| match f {
                    Fact::Link(l) => Some(l),
                    _ => None,
                })
                .collect(),
            Change::Closed(l) => vec![l],
            Change::Declared(_) => Vec::new(),
        };
        for link in links {
            match link.relation.as_str() {
                "said" => {
                    voice_of.insert(link.to.key, link.from.key);
                }
                "supports" => supports.push((link.from.key, link.to.key)),
                _ => {}
            }
        }
    }
    let mut pairs: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    for (statement, finding) in supports {
        pairs
            .entry(finding)
            .or_default()
            .insert(voice_of[&statement].clone());
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

// ---------- events: compaction ----------

#[given("a graph with events before and after a cursor")]
fn events_around_cursor(world: &mut World) {
    let when = day(2026, 10, 14);
    let g = world.open();
    for d in t9b() {
        g.declare(&d).unwrap();
    }
    for v in 0..10 {
        g.write(&Write::new("eavesdrop").unit(Unit::new(
            "voice",
            format!("v{v}"),
            NONE,
            when.clone(),
        )))
        .unwrap();
    }
    world.cursor = world.all_events().last().unwrap().cursor;
    for s in 0..10 {
        let statement = format!("s{s}");
        world
            .g()
            .write(
                &Write::new("eavesdrop").unit(
                    Unit::new(
                        "statement",
                        statement.clone(),
                        [("text", "words")],
                        when.clone(),
                    )
                    .link("said", format!("v{s}"), statement, when.clone()),
                ),
            )
            .unwrap();
    }
    world.log = world.all_events();
}

#[when("the app compacts at the cursor")]
fn compacts_at_the_cursor(world: &mut World) {
    let cursor = world.cursor;
    world.g().compact(cursor).unwrap();
    world.read = world.all_events();
}

#[then("reading the events returns no event up to the cursor")]
fn none_up_to_cursor(world: &mut World) {
    // 9 declarations and 10 voices were written up to the cursor.
    let up_to = world.log.iter().filter(|e| e.cursor <= world.cursor);
    assert_eq!(up_to.count(), 19);
    assert!(world.read.iter().all(|e| e.cursor > world.cursor));
}

#[then("it returns every event after the cursor")]
fn every_after_cursor(world: &mut World) {
    let after: Vec<Event> = world
        .log
        .iter()
        .filter(|e| e.cursor > world.cursor)
        .cloned()
        .collect();
    assert_eq!(after.len(), 10);
    assert_eq!(world.read, after);
}

/// Per finding, all 1,000 of T4's: distinct voices with the total, grouped by place.
fn count_t4_findings(g: &Graph) -> Vec<Tally> {
    g.count(&Count {
        class: "finding".into(),
        roots: (0..1000).map(|f| format!("f{f}")).collect(),
        path: vec![Step::new("supports<-"), Step::new("said<-")],
        group_by: Some(GroupBy {
            class: "statement".into(),
            property: "place".into(),
        }),
        as_of: None,
    })
    .unwrap()
}

/// Compacts at the cursor of the last event, so every event up to now is removed.
fn compact_at_end(world: &mut World) {
    world.cursor = world.all_events().last().unwrap().cursor;
    let cursor = world.cursor;
    world.g().compact(cursor).unwrap();
    assert!(world.all_events().is_empty());
}

#[given("a graph of 100,000 voices and its live count for each of the 1,000 findings")]
fn t4_with_live_counts(world: &mut World) {
    world.source = t4(world);
    world.before = count_t4_findings(world.g());
}

#[when("the app compacts at a cursor")]
fn compacts_at_a_cursor(world: &mut World) {
    compact_at_end(world);
}

#[then("the count for each of the 1,000 findings equals its count before compaction")]
fn counts_unchanged(world: &mut World) {
    let after = count_t4_findings(world.g());
    assert_eq!(world.before.len(), 1000);
    assert!(world.before.iter().any(|t| t.distinct > 0));
    assert_eq!(after, world.before);
}

#[given(
    "a graph compacted at a cursor, and the count for each of the 1,000 findings taken right after compaction"
)]
fn t4_compacted_with_counts(world: &mut World) {
    world.source = t4(world);
    compact_at_end(world);
    world.before = count_t4_findings(world.g());
}

#[when("the app writes more statements and folds the events after the cursor onto those counts")]
fn writes_and_folds(world: &mut World) {
    let when = day(2026, 10, 15);
    let mut rng = fastrand::Rng::with_seed(43);
    let mut write = Write::new("t4");
    for v in 100_000..100_200 {
        let voice = format!("v{v}");
        write = write.unit(Unit::new("voice", voice.clone(), NONE, when.clone()));
        for k in 0..rng.usize(1..4) {
            let statement = format!("v{v}s{k}");
            let finding = format!("f{}", rng.usize(..1000));
            let place = PLACES[rng.usize(..PLACES.len())];
            write = write.unit(
                Unit::new(
                    "statement",
                    statement.clone(),
                    [("place", place)],
                    when.clone(),
                )
                .link("said", voice.clone(), statement.clone(), when.clone())
                .link("supports", statement, finding, when.clone()),
            );
        }
    }
    world.g().write(&write).unwrap();

    // The app's fold: statements' places, their voices and the findings they support.
    let mut place_of: HashMap<String, Option<String>> = HashMap::new();
    let mut voice_of: HashMap<String, String> = HashMap::new();
    let mut supports: Vec<(String, String)> = Vec::new();
    let cursor = world.cursor;
    for event in world.g().events().since(cursor).unwrap() {
        let Change::Wrote { added, .. } = event.change else {
            continue;
        };
        for fact in added {
            match fact {
                Fact::Node(node) if node.class == "statement" => {
                    place_of.insert(node.key, node.properties.get("place").cloned());
                }
                Fact::Link(link) if link.relation == "said" => {
                    voice_of.insert(link.to.key, link.from.key);
                }
                Fact::Link(link) if link.relation == "supports" => {
                    supports.push((link.from.key, link.to.key));
                }
                _ => {}
            }
        }
    }
    // The voices written after the cursor are new, so none is in the base's distinct counts.
    let mut voices: HashMap<String, HashSet<String>> = HashMap::new();
    let mut by_place: HashMap<(String, Option<String>), HashSet<String>> = HashMap::new();
    let mut tallies: HashMap<String, Tally> = world
        .before
        .iter()
        .map(|t| (t.root.clone(), t.clone()))
        .collect();
    for (statement, finding) in supports {
        let voice = voice_of[&statement].clone();
        let place = place_of[&statement].clone();
        let tally = tallies.get_mut(&finding).unwrap();
        tally.total += 1;
        let group = tally.groups.entry(place.clone()).or_default();
        group.1 += 1;
        if by_place
            .entry((finding.clone(), place))
            .or_default()
            .insert(voice.clone())
        {
            group.0 += 1;
        }
        if voices.entry(finding).or_default().insert(voice) {
            tally.distinct += 1;
        }
    }
    world.tallies = world
        .before
        .iter()
        .map(|t| tallies.remove(&t.root).unwrap())
        .collect();
}

#[then("its per-finding numbers equal the live counts for all 1,000 findings")]
fn fold_on_base_equals_live(world: &mut World) {
    let live = count_t4_findings(world.g());
    assert_eq!(live.len(), 1000);
    assert_ne!(live, world.before);
    assert_eq!(world.tallies, live);
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
        .unwrap();
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
        .unwrap();
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
        .unwrap();
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
    assert_eq!(s05.properties["text"], "text of s05, corrected");
    assert_eq!(s05.valid, day(2024, 5, 1));
}

#[when("the app finds statement s05 as of 2023-06-30")]
fn finds_s05_2023(world: &mut World) {
    world.found = world
        .g()
        .find_as_of("statement", "s05", &Date::day(2023, 6, 30))
        .unwrap();
}

#[then("no node is returned")]
fn no_node(world: &mut World) {
    assert!(world.found.is_none());
    assert!(world.g().find("statement", "s05").unwrap().is_some());
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
        })
        .unwrap();
}

fn supporting(world: &mut World, as_of: Option<&Date>) -> Vec<(NodeRef, ValidTime)> {
    let f1 = NodeRef::new("finding", "f1");
    let g = world.g();
    match as_of {
        None => g.walk(&f1, &["supports<-"], 1, 1000),
        Some(as_of) => g.walk_as_of(&f1, &["supports<-"], 1, 1000, as_of),
    }
    .unwrap()
}

fn keys(nodes: &[(NodeRef, ValidTime)]) -> BTreeSet<String> {
    nodes.iter().map(|(n, _)| n.key.clone()).collect()
}

#[given("T10's finding with 40 supporting statements")]
#[given("T10's finding with 40 supporting statements from 12 voices")]
fn t10_finding(world: &mut World) {
    t10(world.open());
}

#[given("T10's finding with 3 supporting statements of stated-unknown valid time")]
fn t10_unknowns(world: &mut World) {
    t10(world.open());
    assert_eq!(t10_unknown().len(), 3);
}

#[when("the app counts the finding's supporting statements as of 2024-12-31")]
fn counts_statements_2024(world: &mut World) {
    count_f1_as_of(world, vec![Step::new("supports<-")], date("2024-12-31"));
}

#[then(
    "the count equals the number of the 40 that the fixture marks valid on or before 2024-12-31"
)]
fn statements_valid_by_2024(world: &mut World) {
    let expected = t10_valid_by(&date("2024-12-31")).len() as u64;
    assert!(expected > 0 && expected < 40);
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

#[given("a statement valid before a date t that supports finding f1 by a link valid after t")]
fn link_after_t(world: &mut World) {
    open_graph(world);
    let before = day(2024, 1, 1);
    world.write(
        &Write::new("swtor")
            .unit(Unit::new("finding", "f1", NONE, before.clone()))
            .unit(
                Unit::new("statement", "s1", [("text", "words")], before).link(
                    "supports",
                    "s1",
                    "f1",
                    day(2025, 1, 1),
                ),
            ),
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

scenarios!("tests", fixtures = [world: World]);
