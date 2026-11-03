//! Relays against the store and the queue, on synthetic fixtures of the apps' toys: cycles and
//! exact counts, a hub cut with boundary churn, two builds as two valid times, and a planet-shaped
//! neighbourhood. The toys on real data are in `toys/relay` on its own branch.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rannoch_cloudevents::{EventId, NodeRef};
use rannoch_queue::{Queue, State};
use rannoch_relay::{Relays, WarmSet};
use rannoch_store::{
    Class, Declaration, Direction, Graph, Relation, Roots, Shape, Step, Value, Version, View, Way,
    open,
};
use serde_json::{Value as Json, json};

const KEY: [u8; 32] = [7; 32];
const INTERVAL: Duration = Duration::from_millis(5);
/// How long a test waits before it calls itself hung. No test asserts how long anything takes:
/// the time bounds are measured in `toys/relay`.
const HUNG: Duration = Duration::from_secs(120);
/// The worker a test reads answers with.
const WAITER: &str = "test.answers";

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn declared(name: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"d-{name}","source":"rannoch:app","type":"rannoch.declared.{name}"}}"#
    )
}

fn node(class: &str, key: &str, title: &str) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:app",
        "type": format!("rannoch.node.{class}"), "subject": key,
        "data": {"properties": {"title": title}},
    })
    .to_string()
}

fn link_kind(kind: &str, relation: &str, from: (&str, &str), to: (&str, &str)) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:app",
        "type": format!("rannoch.{kind}.{relation}"),
        "data": {"from": {"class": from.0, "key": from.1}, "to": {"class": to.0, "key": to.1}},
    })
    .to_string()
}

fn link(relation: &str, from: (&str, &str), to: (&str, &str)) -> String {
    link_kind("link", relation, from, to)
}

fn step(relation: &str, cap: Option<usize>) -> Step {
    Step {
        cap,
        relation: relation.into(),
        ..Step::default()
    }
}

fn shape(name: &str, class: &str, key: &str, steps: Vec<Step>) -> Shape {
    Shape {
        name: name.into(),
        roots: Roots::Node {
            node: NodeRef::new(class, key),
        },
        steps,
    }
}

/// A store with quests in a chain, schematics that use a material and craft an item, and a
/// relation between quests on a date axis.
struct World {
    dir: tempfile::TempDir,
    queue: Arc<Queue>,
    relays: Relays,
    changes: Arc<Mutex<Vec<Json>>>,
}

fn path(dir: &Path) -> PathBuf {
    dir.join("graph.rannoch")
}

fn declare(graph: &mut Graph, shapes: Vec<Shape>) -> Result {
    let mut quest = Class::new("quest", ["title"]);
    quest.indexed = BTreeSet::from(["title".to_string()]);
    for class in [
        quest,
        Class::new("material", ["title"]),
        Class::new("schematic", ["title"]),
        Class::new("item", ["title"]),
    ] {
        let name = class.name.clone();
        graph.declare(&Declaration::Class(class), &declared(&name))?;
    }
    let mut stop = Class::new("stop", ["title"]);
    stop.axis = vec!["date".to_string()];
    graph.declare(&Declaration::Class(stop), &declared("stop"))?;
    let mut dated = Relation::new("arc", "stop", "stop", false);
    dated.axis = vec!["date".to_string()];
    for relation in [
        Relation::new("next", "quest", "quest", false),
        Relation::new("uses", "schematic", "material", false),
        Relation::new("crafts", "schematic", "item", false),
        dated,
    ] {
        let name = relation.name.clone();
        graph.declare(&Declaration::Relation(relation), &declared(&name))?;
    }
    for shape in shapes {
        let name = shape.name.clone();
        graph.declare(&Declaration::Shape(shape), &declared(&name))?;
    }
    Ok(())
}

impl World {
    fn new(shapes: Vec<Shape>) -> Result<World> {
        let dir = tempfile::tempdir()?;
        let mut graph = open(path(dir.path()), &KEY)?;
        declare(&mut graph, shapes)?;
        World::start(dir, graph)
    }

    fn start(dir: tempfile::TempDir, graph: Graph) -> Result<World> {
        let queue = Arc::new(Queue::open(graph, INTERVAL)?);
        let _ = queue.join(WAITER);
        let relays = Relays::open(
            dir.path().join("relays"),
            Arc::clone(&queue),
            path(dir.path()),
            &KEY,
        )?;
        let changes = Arc::new(Mutex::new(Vec::new()));
        let heard = Arc::clone(&changes);
        relays.subscribe(move |body| {
            if let (Ok(change), Ok(mut changes)) = (serde_json::from_str(body), heard.lock()) {
                changes.push(change);
            }
        });
        Ok(World {
            dir,
            queue,
            relays,
            changes,
        })
    }

    /// Stops everything and opens it again on the same files.
    fn restart(self) -> Result<World> {
        let World {
            dir,
            queue,
            relays,
            changes: _,
        } = self;
        assert!(relays.close().is_empty());
        drop(queue);
        let graph = open(path(dir.path()), &KEY)?;
        World::start(dir, graph)
    }

    fn write(&self, bodies: &[String]) -> Result {
        for body in bodies {
            self.queue.accept(body.clone())?;
        }
        let (_, failures) = self.queue.flush();
        assert!(failures.is_empty(), "{failures:?}");
        Ok(())
    }

    /// The relay `id` once it has folded everything committed now.
    fn settled(&self, id: &str) -> Result<WarmSet> {
        let graph = open(path(self.dir.path()), &KEY)?;
        let high = graph.events().committed()?;
        until(|| self.relays.read(id).is_some_and(|set| set.cursor() >= high))?;
        Ok(self.relays.read(id).ok_or("the relay is there")?)
    }

    /// Relay `id`'s file saved past everything folded, and its events closed.
    fn closed(&self, id: &str) -> Result {
        let worker = format!("rannoch.relay.{id}");
        let high = self.settled(id)?.cursor();
        until(|| self.queue.join(&worker) >= high && matches!(self.queue.lag(&worker), Ok(0)))
    }

    /// Asks through the inbox, and returns the relay's id and the answer.
    fn query(&self, source: &str, shape: &str, view: &Json) -> Result<(String, Json)> {
        let asked = query(source, shape, view);
        let id = rannoch_cloudevents::attributes(&asked)?.id;
        self.queue.accept(asked)?;
        let (_, failures) = self.queue.flush();
        assert!(failures.is_empty(), "{failures:?}");
        let answer = self.answer_to(&id)?;
        let relay = answer["data"]["result"]["relay"]
            .as_str()
            .ok_or("a relay id")?
            .to_string();
        Ok((relay, answer))
    }

    /// The answer in the log to the query `id`, read by a worker of the test's own.
    fn answer_to(&self, id: &str) -> Result<Json> {
        let mut answer = None;
        until(|| {
            while let Ok(Some(event)) = self.queue.next(WAITER) {
                let _ = self.queue.done(WAITER, event.cursor);
                if let Ok(read) = serde_json::from_str::<Json>(&event.body)
                    && read["type"] == "rannoch.answer"
                    && read["subject"] == json!(id)
                {
                    answer = Some(read);
                }
            }
            answer.is_some()
        })?;
        Ok(answer.ok_or("an answer")?)
    }

    fn cold(&self, shape_name: &str, view: &View) -> Result<Cold> {
        let graph = open(path(self.dir.path()), &KEY)?;
        let ontology = graph.current_ontology()?;
        cold(&graph, ontology.shape(shape_name)?, view)
    }
}

/// Waits for `done`, and fails only when the test is hung.
fn until(mut done: impl FnMut() -> bool) -> Result {
    let hung = Instant::now() + HUNG;
    while !done() {
        if Instant::now() > hung {
            return Err("hung".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

fn query(source: &str, shape: &str, view: &Json) -> String {
    json!({
        "specversion": "1.0", "id": EventId::now().to_string(), "source": source,
        "type": "rannoch.query", "data": {"shape": shape, "view": view},
    })
    .to_string()
}

/// A shape's walk as #175's reads answer it, written apart from the relay: each step's distinct
/// nodes in key order, with how many a cap dropped, and each node's version.
#[derive(Debug, PartialEq, Eq)]
struct Cold {
    nodes: BTreeMap<NodeRef, Version>,
    steps: Vec<(Vec<NodeRef>, usize)>,
}

fn cold(graph: &Graph, shape: &Shape, view: &View) -> Result<Cold> {
    let ontology = graph.current_ontology()?;
    let mut nodes = BTreeMap::new();
    let mut frontier = Vec::new();
    match &shape.roots {
        Roots::Node { node } => {
            let root = NodeRef::new(node.class.clone(), node.key.clone());
            if let Some(version) = graph.state_at(&root, view)? {
                nodes.insert(root.clone(), version);
                frontier.push(root);
            }
        }
        Roots::Class { class, filter } => {
            for (key, version) in graph.nodes(class, filter, view)? {
                let root = NodeRef::new(class.clone(), key);
                nodes.insert(root.clone(), version);
                frontier.push(root);
            }
        }
    }
    let mut steps = Vec::new();
    for step in &shape.steps {
        let (name, way) = step.way();
        let relation = ontology.relation(name)?;
        let mut found = BTreeMap::new();
        for at in &frontier {
            let mut ends = Vec::new();
            if way != Way::Backward && at.class == relation.from {
                ends.push((Direction::Out, &relation.to));
            }
            if way != Way::Forward && at.class == relation.to {
                ends.push((Direction::In, &relation.from));
            }
            for (direction, other) in ends {
                for (key, _) in graph.links(at, name, direction, view)? {
                    let reached = NodeRef::new(other.clone(), key.clone());
                    if nodes.contains_key(&reached) {
                        continue;
                    }
                    if let Some(version) = graph.state_at(&reached, view)?
                        && step.passes(&version.properties)
                    {
                        found.insert((key, other.clone()), version);
                    }
                }
            }
        }
        let total = found.len();
        let cap = step.cap.unwrap_or(usize::MAX);
        let mut taken = Vec::new();
        for ((key, class), version) in found.into_iter().take(cap) {
            let reached = NodeRef::new(class, key);
            nodes.insert(reached.clone(), version);
            taken.push(reached);
        }
        steps.push((taken.clone(), total.saturating_sub(cap)));
        frontier = taken;
    }
    Ok(Cold { nodes, steps })
}

fn warm(set: &WarmSet) -> Cold {
    Cold {
        nodes: set
            .nodes()
            .iter()
            .map(|(node, version)| ((**node).clone(), (**version).clone()))
            .collect(),
        steps: set
            .steps()
            .iter()
            .map(|step| {
                (
                    step.nodes.iter().map(|n| (**n).clone()).collect(),
                    step.dropped,
                )
            })
            .collect(),
    }
}

fn keys(nodes: &[Arc<NodeRef>]) -> Vec<&str> {
    nodes.iter().map(|n| n.key.as_str()).collect()
}

fn chain_shape() -> Shape {
    shape(
        "chain",
        "quest",
        "q0",
        (0..12).map(|_| step("next", None)).collect(),
    )
}

/// A cycle of `len` quests: q0 -> q1 -> ... -> q0.
fn cycle(len: usize) -> Vec<String> {
    let mut bodies: Vec<String> = (0..len)
        .map(|i| node("quest", &format!("q{i}"), "t"))
        .collect();
    for i in 0..len {
        bodies.push(link(
            "next",
            ("quest", &format!("q{i}")),
            ("quest", &format!("q{}", (i + 1) % len)),
        ));
    }
    bodies
}

fn hub_shape() -> Shape {
    shape(
        "hub",
        "material",
        "stabilizer",
        vec![step("uses<-", Some(5)), step("crafts", None)],
    )
}

/// A material used by `count` schematics s10, s11, ..., each crafting an item of its own.
fn hub(count: usize) -> Vec<String> {
    let mut bodies = vec![node("material", "stabilizer", "t")];
    for i in 0..count {
        let (s, it) = (format!("s{}", 10 + i), format!("i{}", 10 + i));
        bodies.push(node("schematic", &s, "t"));
        bodies.push(node("item", &it, "t"));
        bodies.push(link("uses", ("schematic", &s), ("material", "stabilizer")));
        bodies.push(link("crafts", ("schematic", &s), ("item", &it)));
    }
    bodies
}

#[test]
fn a_cycle_never_repeats_a_node_and_counts_are_distinct() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(7))?;
    let (id, answer) = world.query("app", "chain", &json!({}))?;
    let steps = answer["data"]["result"]["steps"]
        .as_array()
        .ok_or("steps")?;
    assert_eq!(steps.len(), 12);
    let taken: u64 = steps.iter().filter_map(|s| s["nodes"].as_u64()).sum();
    assert_eq!(taken, 6, "six nodes past the root, each once");
    assert!(
        steps
            .iter()
            .all(|s| s["cut"] == json!(false) && s["dropped"] == json!(0))
    );
    let set = world.settled(&id)?;
    assert_eq!(set.nodes().len(), 7);
    assert_eq!(warm(&set), world.cold("chain", &View::default())?);
    Ok(())
}

#[test]
fn an_idle_relay_emits_nothing() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let (id, _) = world.query("app", "chain", &json!({}))?;
    world.settled(&id)?;
    std::thread::sleep(Duration::from_millis(100));
    assert!(world.changes.lock().map_err(|_| "lock")?.is_empty());
    world.write(&[node("schematic", "unrelated", "t")])?;
    world.settled(&id)?;
    assert!(world.changes.lock().map_err(|_| "lock")?.is_empty());
    Ok(())
}

#[test]
fn a_query_in_the_inbox_is_answered_by_the_worker_and_graph_answer_refuses_it() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let asked = query("app", "chain", &json!({}));
    let id = rannoch_cloudevents::attributes(&asked)?.id;
    world.write(std::slice::from_ref(&asked))?;
    let graph = open(path(world.dir.path()), &KEY)?;
    let answer = world.answer_to(&id)?;
    assert_eq!(answer["subject"], json!(id));
    assert_eq!(answer["type"], json!("rannoch.answer"));
    let result = &answer["data"]["result"];
    assert!(
        result["relay"]
            .as_str()
            .is_some_and(|r| r.parse::<EventId>().is_ok())
    );
    assert!(result["cursor"].is_i64());
    assert!(answer["data"]["known"].is_i64());
    assert!(
        graph.answer(&asked).is_err(),
        "Graph::answer refuses a shape"
    );
    let unknown = query("app", "nothing", &json!({}));
    world.write(std::slice::from_ref(&unknown))?;
    let logged = graph.events().committed()?;
    let held = graph
        .events()
        .since(rannoch_store::Cursor::START)?
        .into_iter()
        .find(|e| e.body == unknown)
        .ok_or("the query is in the log")?;
    assert!(held.cursor <= logged);
    until(|| {
        matches!(world.queue.state("rannoch.relay", held.cursor),
                 Ok(State::Failed(why)) if why.contains("shape nothing is not declared"))
    })?;
    Ok(())
}

#[test]
fn a_hub_cut_is_deterministic_and_boundary_churn_is_one_event() -> Result {
    let world = World::new(vec![hub_shape()])?;
    world.write(&hub(8))?;
    let (id, answer) = world.query("app", "hub", &json!({}))?;
    let first = &answer["data"]["result"]["steps"][0];
    assert_eq!(first, &json!({"nodes": 5, "cut": true, "dropped": 3}));
    let set = world.settled(&id)?;
    assert_eq!(
        keys(&set.steps()[0].nodes),
        ["s10", "s11", "s12", "s13", "s14"]
    );
    assert_eq!(
        keys(&set.steps()[1].nodes),
        ["i10", "i11", "i12", "i13", "i14"]
    );
    assert_eq!(warm(&set), world.cold("hub", &View::default())?);

    // A key below the cut: it comes in, the old fifth goes, and what it crafted goes with it.
    world.write(&[
        node("schematic", "s00", "t"),
        link("uses", ("schematic", "s00"), ("material", "stabilizer")),
    ])?;
    let set = world.settled(&id)?;
    assert_eq!(
        keys(&set.steps()[0].nodes),
        ["s00", "s10", "s11", "s12", "s13"]
    );
    assert_eq!(set.steps()[0].dropped, 4);
    assert_eq!(set.nodes().len(), 1 + 5 + 4);
    assert_eq!(warm(&set), world.cold("hub", &View::default())?);
    let changes = world.changes.lock().map_err(|_| "lock")?.clone();
    assert_eq!(changes.len(), 1, "{changes:?}");
    let change = &changes[0];
    assert_eq!(change["type"], json!("rannoch.relay.changed"));
    assert_eq!(change["subject"], json!(id));
    let named = |list: &str| -> Vec<String> {
        change["data"][list]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| e["node"]["key"].as_str().map(str::to_string))
            .collect()
    };
    assert!(named("entered").contains(&"s00".to_string()));
    assert!(named("left").contains(&"s14".to_string()));
    let graph = open(path(world.dir.path()), &KEY)?;
    assert!(
        !graph
            .events()
            .since(rannoch_store::Cursor::START)?
            .iter()
            .any(|e| e.body.contains("rannoch.relay.changed")),
        "a change event is not logged"
    );
    Ok(())
}

#[test]
fn a_node_a_cap_cut_may_be_reached_by_a_later_step() -> Result {
    let around = shape(
        "around",
        "material",
        "stabilizer",
        vec![
            step("uses<-", Some(1)),
            step("crafts", None),
            step("crafts<-", None),
        ],
    );
    let world = World::new(vec![around])?;
    let mut bodies = hub(3);
    bodies.push(link("crafts", ("schematic", "s11"), ("item", "i10")));
    world.write(&bodies)?;
    let (id, _) = world.query("app", "around", &json!({}))?;
    let set = world.settled(&id)?;
    assert_eq!(keys(&set.steps()[0].nodes), ["s10"]);
    assert_eq!(set.steps()[0].dropped, 2);
    assert_eq!(keys(&set.steps()[1].nodes), ["i10"]);
    assert_eq!(
        keys(&set.steps()[2].nodes),
        ["s11"],
        "cut at step one, reached at step three"
    );
    assert_eq!(warm(&set), world.cold("around", &View::default())?);
    Ok(())
}

#[test]
fn either_way_and_filters_and_a_set_of_roots() -> Result {
    let mut filter = BTreeMap::new();
    filter.insert("title".to_string(), Value::from("root"));
    let rooted = Shape {
        name: "rooted".into(),
        roots: Roots::Class {
            class: "quest".into(),
            filter,
        },
        steps: vec![Step {
            relation: "next<->".into(),
            drop: BTreeMap::from([("title".to_string(), Value::from("no"))]),
            ..Step::default()
        }],
    };
    let world = World::new(vec![rooted])?;
    world.write(&[
        node("quest", "a", "root"),
        node("quest", "b", "yes"),
        node("quest", "c", "no"),
        node("quest", "d", "yes"),
        link("next", ("quest", "a"), ("quest", "b")),
        link("next", ("quest", "c"), ("quest", "a")),
        link("next", ("quest", "d"), ("quest", "a")),
    ])?;
    let (id, _) = world.query("app", "rooted", &json!({}))?;
    let set = world.settled(&id)?;
    assert!(
        set.nodes().keys().any(|n| n.key == "a"),
        "a root is in the set"
    );
    assert_eq!(keys(&set.steps()[0].nodes), ["b", "d"]);
    assert_eq!(
        set.by("title", &Value::from("root")).len(),
        1,
        "read by an indexed value"
    );
    assert_eq!(set.by("title", &Value::from("yes")).len(), 2);
    assert!(set.by("title", &Value::from("no")).is_empty());

    // A node becoming a root, and one ceasing to be, move the set.
    world.write(&[node("quest", "e", "root"), node("quest", "a", "gone")])?;
    let set = world.settled(&id)?;
    assert!(set.nodes().keys().any(|n| n.key == "e"), "a new root");
    assert!(
        !set.nodes().keys().any(|n| n.key == "a"),
        "a root no longer"
    );
    assert_eq!(warm(&set), world.cold("rooted", &View::default())?);
    Ok(())
}

#[test]
fn read_equals_a_cold_recompute_after_writes_an_amendment_a_rename_and_a_full_replay() -> Result {
    let world = World::new(vec![chain_shape(), hub_shape()])?;
    world.write(&cycle(5))?;
    world.write(&hub(8))?;
    let now = View::default();
    let (chain, _) = world.query("app", "chain", &json!({}))?;
    let (hub_id, _) = world.query("app", "hub", &json!({}))?;
    let same = |w: &World| -> Result {
        assert_eq!(warm(&w.settled(&chain)?), w.cold("chain", &now)?);
        assert_eq!(warm(&w.settled(&hub_id)?), w.cold("hub", &now)?);
        Ok(())
    };
    same(&world)?;

    // A burst, with a link ended and a property moved.
    let mut burst = Vec::new();
    for i in 0..50 {
        let key = format!("q{}", 100 + i);
        burst.push(node("quest", &key, "new"));
        burst.push(link("next", ("quest", "q2"), ("quest", &key)));
    }
    burst.push(link_kind("ended", "next", ("quest", "q2"), ("quest", "q3")));
    burst.push(node("quest", "q1", "retitled"));
    burst.push(link_kind(
        "corrected",
        "uses",
        ("schematic", "s10"),
        ("material", "stabilizer"),
    ));
    world.write(&burst)?;
    same(&world)?;

    // An amendment of a node's title.
    let titled = node("quest", "q1", "wrong");
    world.write(std::slice::from_ref(&titled))?;
    same(&world)?;
    let target = rannoch_cloudevents::attributes(&titled)?;
    let amendment = json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:app",
        "type": "rannoch.amended",
        "data": {"target": {"source": "rannoch:app", "id": target.id},
                 "patch": {"data": {"properties": {"title": "right"}}}},
    })
    .to_string();
    world.write(&[amendment])?;
    same(&world)?;

    // A rename, and a link naming the old key afterwards.
    let q4 = node("quest", "q4", "renamed soon");
    world.write(std::slice::from_ref(&q4))?;
    let target = rannoch_cloudevents::attributes(&q4)?;
    let rename = json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:app",
        "type": "rannoch.amended",
        "data": {"target": {"source": "rannoch:app", "id": target.id},
                 "patch": {"subject": "q4b"}},
    })
    .to_string();
    world.write(&[rename])?;
    same(&world)?;
    world.write(&[
        node("quest", "q9", "t"),
        link("next", ("quest", "q4"), ("quest", "q9")),
    ])?;
    same(&world)?;

    // A full replay: the relay's file gone, and the relay built again.
    let World {
        dir,
        queue,
        relays,
        changes,
    } = world;
    assert!(relays.close().is_empty());
    drop(queue);
    for file in std::fs::read_dir(dir.path().join("relays"))? {
        let file = file?.path();
        if file.file_name().is_some_and(|name| name != "relays.json") {
            std::fs::remove_file(file)?;
        }
    }
    let graph = open(path(dir.path()), &KEY)?;
    drop(changes);
    let world = World::start(dir, graph)?;
    same(&world)?;
    Ok(())
}

#[test]
fn a_restart_resumes_and_a_file_behind_reseeds() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(4))?;
    let (id, _) = world.query("app", "chain", &json!({}))?;
    world.settled(&id)?;
    let file = world.dir.path().join("relays").join(format!("{id}.json"));
    let early = std::fs::read(&file)?;
    for i in 0..3 {
        world.write(&[node("quest", &format!("q{}", i + 1), &format!("t{i}"))])?;
    }
    world.closed(&id)?;
    assert_ne!(std::fs::read(&file)?, early);
    let world = world.restart()?;
    let set = world.settled(&id)?;
    assert_eq!(warm(&set), world.cold("chain", &View::default())?);
    assert!(set.cursor() > rannoch_store::Cursor::START);

    // The file put back behind its worker's cursor: the relay does not trust it.
    world.write(&[node("quest", "q1", "after")])?;
    world.closed(&id)?;
    let World {
        dir,
        queue,
        relays,
        changes: _,
    } = world;
    assert!(relays.close().is_empty());
    let _ = queue.flush();
    drop(queue);
    std::fs::write(dir.path().join("relays").join(format!("{id}.json")), &early)?;
    let graph = open(path(dir.path()), &KEY)?;
    let world = World::start(dir, graph)?;
    let set = world.settled(&id)?;
    assert_eq!(warm(&set), world.cold("chain", &View::default())?);
    Ok(())
}

#[test]
fn events_close_only_after_the_file_is_saved() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let (id, _) = world.query("app", "chain", &json!({}))?;
    let worker = format!("rannoch.relay.{id}");
    let file = world.dir.path().join("relays").join(format!("{id}.json"));
    let file_cursor = || -> Result<Json> {
        let saved: Json = serde_json::from_slice(&std::fs::read(&file)?)?;
        Ok(saved["cursor"].clone())
    };
    for i in 0..5 {
        world.write(&[node("quest", "q1", &format!("t{i}"))])?;
        // Whenever it is looked at, the worker's cursor is not ahead of the file's.
        let place = serde_json::to_value(world.queue.join(&worker))?;
        assert!(place.as_i64() <= file_cursor()?.as_i64());
    }
    let set = world.settled(&id)?;
    world.closed(&id)?;
    assert_eq!(file_cursor()?, serde_json::to_value(set.cursor())?);
    Ok(())
}

#[test]
fn relays_of_one_shape_and_view_are_shared_and_the_last_holder_drops_it() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let (first, _) = world.query("app-a", "chain", &json!({}))?;
    let (second, _) = world.query("app-b", "chain", &json!({}))?;
    assert_eq!(first, second, "shared");
    let (other_view, _) = world.query("app-a", "chain", &json!({"known": 1}))?;
    assert_ne!(first, other_view, "another view is another relay");
    let worker = format!("rannoch.relay.{first}");
    let file = world
        .dir
        .path()
        .join("relays")
        .join(format!("{first}.json"));
    assert!(file.exists());

    world.relays.release(&first, "app-a")?;
    assert!(world.relays.read(&first).is_some(), "app-b holds it");
    world.relays.release(&first, "app-b")?;
    assert!(world.relays.read(&first).is_none());
    assert!(!file.exists());
    let graph = open(path(world.dir.path()), &KEY)?;
    let _ = world.queue.flush();
    assert!(
        graph
            .places()?
            .iter()
            .all(|(place, _)| place.worker != worker)
    );
    assert!(world.relays.release(&first, "app-b").is_err());
    Ok(())
}

#[test]
fn holders_survive_a_restart() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let (id, _) = world.query("app-a", "chain", &json!({}))?;
    world.query("app-b", "chain", &json!({}))?;
    let world = world.restart()?;
    world.relays.release(&id, "app-a")?;
    assert!(world.relays.read(&id).is_some());
    world.relays.release(&id, "app-b")?;
    assert!(world.relays.read(&id).is_none());
    Ok(())
}

#[test]
fn a_report_stays_where_it_froze() -> Result {
    let world = World::new(vec![shape(
        "speaks",
        "quest",
        "npc",
        vec![step("next", None)],
    )])?;
    let mut bodies = vec![node("quest", "npc", "t")];
    for i in 0..4 {
        let key = format!("c{i}");
        bodies.push(node("quest", &key, "t"));
        bodies.push(link("next", ("quest", "npc"), ("quest", &key)));
    }
    world.write(&bodies)?;
    let (id, _) = world.query("app", "speaks", &json!({}))?;
    let before = world.settled(&id)?;
    assert_eq!(before.steps()[0].nodes.len(), 4);
    let report = world.relays.report(&id)?;
    let read: Json = serde_json::from_str(&report)?;
    assert_eq!(read["type"], json!("rannoch.report"));
    assert_eq!(read["data"]["shape"], json!("speaks"));
    assert!(read["data"]["cursor"].is_i64());
    assert_eq!(
        read["data"].as_object().map(serde_json::Map::len),
        Some(3),
        "nothing inlined"
    );

    let mut more = Vec::new();
    for i in 4..6 {
        let key = format!("c{i}");
        more.push(node("quest", &key, "t"));
        more.push(link("next", ("quest", "npc"), ("quest", &key)));
    }
    world.write(&more)?;
    let after = world.settled(&id)?;
    assert_eq!(after.steps()[0].nodes.len(), 6, "a fresh read moves");
    let frozen = world.relays.reread(&report)?;
    assert_eq!(frozen.steps()[0].nodes.len(), 4, "the report does not");
    assert_eq!(warm(&frozen), warm(&before));
    let graph = open(path(world.dir.path()), &KEY)?;
    assert!(
        graph
            .events()
            .since(rannoch_store::Cursor::START)?
            .iter()
            .any(|e| e.body.contains("rannoch.report"))
    );
    Ok(())
}

/// A link of the dated relation at a build, with that build's date as its valid time.
fn dated(kind: &str, from: &str, to: &str, build: (i64, &str)) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:app",
        "type": format!("rannoch.{kind}.arc"),
        "data": {
            "from": {"class": "stop", "key": from}, "to": {"class": "stop", "key": to},
            "valid": {"on": build.1}, "axis": {"date": build.0},
        },
    })
    .to_string()
}

/// A stop as a build states it.
fn stop(key: &str, build: (i64, &str)) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:app",
        "type": "rannoch.node.stop", "subject": key,
        "data": {"properties": {"title": key}, "valid": {"on": build.1}, "axis": {"date": build.0}},
    })
    .to_string()
}

#[test]
fn two_builds_as_two_valid_times() -> Result {
    let walk = shape(
        "arc",
        "stop",
        "a",
        vec![step("arc", None), step("arc", None)],
    );
    let world = World::new(vec![walk])?;
    let build1 = (1_000, "2025-01-01");
    let build2 = (2_000, "2025-06-01");
    let mut bodies: Vec<String> = ["a", "b", "c", "d"]
        .iter()
        .map(|k| stop(k, build1))
        .collect();
    bodies.push(dated("link", "a", "b", build1));
    bodies.push(dated("link", "b", "c", build1));
    world.write(&bodies)?;
    let (latest, _) = world.query("app", "arc", &json!({}))?;
    let (first, _) = world.query("app", "arc", &json!({"axis": [build1.0]}))?;
    assert_ne!(latest, first);
    let at_first = View {
        axis: Some(vec![build1.0]),
        known: None,
    };
    world.settled(&first)?;
    world.write(&[
        dated("ended", "b", "c", build2),
        dated("link", "b", "d", build2),
    ])?;
    let set = world.settled(&latest)?;
    assert_eq!(keys(&set.steps()[1].nodes), ["d"], "the ended link is gone");
    assert_eq!(warm(&set), world.cold("arc", &View::default())?);
    let held = world.settled(&first)?;
    assert_eq!(keys(&held.steps()[1].nodes), ["c"]);
    assert_eq!(warm(&held), world.cold("arc", &at_first)?);

    let changes = world.changes.lock().map_err(|_| "lock")?.clone();
    assert_eq!(
        changes.len(),
        1,
        "only the latest relay reacts: {changes:?}"
    );
    let data = &changes[0]["data"];
    assert_eq!(changes[0]["subject"], json!(latest));
    assert_eq!(data["entered"][0]["node"]["key"], json!("d"));
    assert_eq!(data["left"][0]["node"]["key"], json!("c"));
    assert_eq!(data["entered"][0]["valid"], json!({"on": build2.1}));
    assert_eq!(data["left"][0]["valid"], json!({"on": build2.1}));
    Ok(())
}

#[test]
fn a_change_carries_a_propertys_before_and_after_once_per_commit() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let (id, _) = world.query("app", "chain", &json!({}))?;
    world.settled(&id)?;
    world.write(&[node("quest", "q1", "a"), node("quest", "q2", "b")])?;
    world.settled(&id)?;
    let changes = world.changes.lock().map_err(|_| "lock")?.clone();
    assert_eq!(changes.len(), 1, "{changes:?}");
    let deltas = changes[0]["data"]["changed"].as_array().ok_or("changed")?;
    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0]["property"], json!("title"));
    assert_eq!(deltas[0]["before"], json!("t"));
    assert_eq!(deltas[0]["after"], json!("a"));
    Ok(())
}

#[test]
fn an_event_whose_fill_is_retried_reaches_the_relay_as_a_change() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let (id, _) = world.query("app", "chain", &json!({}))?;
    world.settled(&id)?;
    let fault = rusqlite::Connection::open(path(world.dir.path()))?;
    fault.execute_batch(
        "CREATE TRIGGER no_q1 BEFORE INSERT ON _hearings WHEN NEW.key = 'q1' \
         BEGIN SELECT RAISE(ABORT, 'the disk is full'); END;",
    )?;
    world.write(&[node("quest", "q1", "late")])?;
    let set = world.settled(&id)?;
    let q1 = NodeRef::new("quest", "q1");
    let title = |set: &WarmSet| {
        set.nodes()
            .iter()
            .find(|(n, _)| ***n == q1)
            .and_then(|(_, v)| v.properties.get("title").cloned())
    };
    assert_eq!(title(&set), Some(Value::from("t")), "the fill failed");
    assert!(world.changes.lock().map_err(|_| "lock")?.is_empty());

    fault.execute_batch("DROP TRIGGER no_q1;")?;
    world.write(&[node("schematic", "drives-a-fill", "t")])?;
    let set = world.settled(&id)?;
    assert_eq!(title(&set), Some(Value::from("late")));
    assert_eq!(warm(&set), world.cold("chain", &View::default())?);
    let changes = world.changes.lock().map_err(|_| "lock")?.clone();
    assert_eq!(changes.len(), 1, "{changes:?}");
    let delta = &changes[0]["data"]["changed"][0];
    assert_eq!(delta["node"]["key"], json!("q1"));
    assert_eq!(delta["after"], json!("late"));
    Ok(())
}

#[test]
fn a_view_known_before_the_epoch_answers_as_graph_answer_does() -> Result {
    let world = World::new(vec![chain_shape()])?;
    world.write(&cycle(3))?;
    let view = json!({"known": -1});
    let (id, answer) = world.query("app", "chain", &view)?;
    let graph = open(path(world.dir.path()), &KEY)?;
    let counts = json!({"specversion": "1.0", "id": "q", "source": "app", "type": "rannoch.query",
                        "data": {"counts": {}, "view": view}})
    .to_string();
    let read: Json = serde_json::from_str(&graph.answer(&counts)?)?;
    assert_eq!(read["data"]["known"], json!(-1));
    assert_eq!(answer["data"]["known"], read["data"]["known"]);
    assert_eq!(read["data"]["result"]["nodes"]["quest"], json!(0));
    assert!(world.settled(&id)?.nodes().is_empty(), "nothing known then");
    Ok(())
}
