//! What the relay and purge kill runs share: event bodies, the relays' manifest, and a cold walk of
//! a shape through the store's reads, written apart from the relay.
#![allow(dead_code)]
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rannoch_cloudevents::{EventId, NodeRef};
use rannoch_queue::Queue;
use rannoch_relay::{Relays, WarmSet};
use rannoch_store::{Cursor, Direction, Graph, Roots, Shape, Version, View, ViewData, Way};
use serde::Deserialize;
use serde_json::{Value as Json, json};

pub const KEY: [u8; 32] = [7; 32];

pub fn id() -> String {
    format!("{}:0", EventId::now())
}

pub fn declared(name: &str) -> String {
    json!({"specversion": "1.0", "id": format!("d-{name}"), "source": "rannoch:live",
           "type": format!("rannoch.declared.{name}")})
    .to_string()
}

pub fn node(source: &str, class: &str, key: &str, properties: &Json) -> String {
    json!({"specversion": "1.0", "id": id(), "source": source,
           "type": format!("rannoch.node.{class}"), "subject": key,
           "data": {"properties": properties, "valid": "unknown"}})
    .to_string()
}

/// A link fact: `kind` is link, ended or corrected.
pub fn fact(source: &str, kind: &str, relation: &str, from: (&str, &str), to: (&str, &str)) -> String {
    json!({"specversion": "1.0", "id": id(), "source": source,
           "type": format!("rannoch.{kind}.{relation}"),
           "data": {"from": {"class": from.0, "key": from.1}, "to": {"class": to.0, "key": to.1},
                    "valid": "unknown"}})
    .to_string()
}

pub fn amended(source: &str, target: &str, patch: &Json) -> String {
    let target: Json = serde_json::from_str(target).expect("target");
    json!({"specversion": "1.0", "id": id(), "source": source, "type": "rannoch.amended",
           "data": {"target": {"source": target["source"], "id": target["id"]}, "patch": patch}})
    .to_string()
}

pub fn query(shape: &str, view: &Json) -> String {
    json!({"specversion": "1.0", "id": EventId::now().to_string(), "source": "live",
           "type": "rannoch.query", "data": {"shape": shape, "view": view}})
    .to_string()
}

pub fn append(path: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).expect("append");
    writeln!(file, "{line}").expect("write");
}

/// One relay as `relays.json` holds it.
#[derive(Deserialize, Clone)]
pub struct Entry {
    pub id: String,
    pub shape: String,
    pub view: ViewData,
}

pub fn manifest(relays: &Path) -> Vec<Entry> {
    std::fs::read(relays.join("relays.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Asks for each shape and view, and waits until the manifest holds them all.
pub fn ask(queue: &Queue, dir: &Path, wanted: &[(&str, Json)]) {
    for (shape, view) in wanted {
        queue.accept(query(shape, view)).expect("ask");
    }
    let (_, failed) = queue.flush();
    assert!(failed.is_empty(), "{failed:?}");
    let hung = Instant::now() + Duration::from_secs(120);
    while manifest(dir).len() < wanted.len() {
        assert!(Instant::now() < hung, "the relays were not made");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Waits until every relay has read to the watermark, and returns the watermark, or `None` when a
/// relay had not after two minutes.
pub fn settled(queue: &Queue, relays: &Relays, reader: &Graph, entries: &[Entry]) -> Option<Cursor> {
    let (_, failed) = queue.flush();
    assert!(failed.is_empty(), "{failed:?}");
    let high = reader.events().committed().expect("committed");
    let hung = Instant::now() + Duration::from_secs(120);
    while !entries
        .iter()
        .all(|entry| relays.read(&entry.id).is_some_and(|set| set.cursor() >= high))
    {
        if Instant::now() > hung {
            return None;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Some(high)
}

/// A shape's walk through the store's reads: each step's distinct nodes in key order, with how many
/// a cap dropped, and each node's version.
#[derive(Debug, PartialEq, Eq)]
pub struct Cold {
    pub nodes: BTreeMap<NodeRef, Version>,
    pub steps: Vec<(Vec<NodeRef>, usize)>,
}

pub fn cold(graph: &Graph, shape: &Shape, view: &View) -> Cold {
    let ontology = graph.current_ontology().expect("ontology");
    let mut nodes = BTreeMap::new();
    let mut frontier = Vec::new();
    match &shape.roots {
        Roots::Node { node } => {
            let root = NodeRef::new(node.class.clone(), node.key.clone());
            if let Some(version) = graph.state_at(&root, view).expect("state") {
                nodes.insert(root.clone(), version);
                frontier.push(root);
            }
        }
        Roots::Class { class, filter } => {
            for (key, version) in graph.nodes(class, filter, view).expect("nodes") {
                let root = NodeRef::new(class.clone(), key);
                nodes.insert(root.clone(), version);
                frontier.push(root);
            }
        }
    }
    let mut steps = Vec::new();
    for step in &shape.steps {
        let (name, way) = step.way();
        let relation = ontology.relation(name).expect("relation");
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
                for (key, _) in graph.links(at, name, direction, view).expect("links") {
                    let reached = NodeRef::new(other.clone(), key.clone());
                    if nodes.contains_key(&reached) {
                        continue;
                    }
                    if let Some(version) = graph.state_at(&reached, view).expect("state")
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
    Cold { nodes, steps }
}

pub fn warm(set: &WarmSet) -> Cold {
    Cold {
        nodes: set.nodes().iter().map(|(n, v)| ((**n).clone(), (**v).clone())).collect(),
        steps: set
            .steps()
            .iter()
            .map(|step| (step.nodes.iter().map(|n| (**n).clone()).collect(), step.dropped))
            .collect(),
    }
}

/// How a warm set differs from the cold walk, or `None` when they are equal.
pub fn differs(graph: &Graph, relays: &Relays, entry: &Entry) -> Option<String> {
    let ontology = graph.current_ontology().expect("ontology");
    let shape = ontology.shape(&entry.shape).expect("shape");
    let walked = cold(graph, shape, &entry.view.view());
    let Some(set) = relays.read(&entry.id) else {
        return Some(format!("relay {} {} is not there", entry.shape, entry.id));
    };
    let held = warm(&set);
    if held == walked {
        return None;
    }
    let mut why = format!("{} cursor {:?}: held {} nodes, cold {}", entry.shape, set.cursor(), held.nodes.len(), walked.nodes.len());
    for (node, version) in &walked.nodes {
        match held.nodes.get(node) {
            None => write!(why, "; missing {}", node.key).expect("text"),
            Some(other) if other != version => write!(why, "; differs {}", node.key).expect("text"),
            Some(_) => {}
        }
    }
    for node in held.nodes.keys().filter(|node| !walked.nodes.contains_key(node)) {
        write!(why, "; extra {}", node.key).expect("text");
    }
    if held.steps != walked.steps {
        why.push_str("; steps differ");
    }
    Some(why)
}

pub fn file_cursor(relays: &Path, id: &str) -> Option<Cursor> {
    #[derive(Deserialize)]
    struct Saved {
        cursor: Cursor,
    }
    let bytes = std::fs::read(relays.join(format!("{id}.json"))).ok()?;
    serde_json::from_slice::<Saved>(&bytes).ok().map(|saved| saved.cursor)
}

/// Ends the process as a kill would, so nothing checkpoints or removes the write-ahead log.
pub fn die(queue: Arc<Queue>, relays: Relays) -> ! {
    std::mem::forget(queue);
    std::mem::forget(relays);
    std::process::abort();
}
