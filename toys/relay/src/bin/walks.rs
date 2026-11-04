//! What a neighbourhood walk costs, what a rewalk costs, and when a short walk can stand in for
//! one. Calls the relay's own fold directly: `warm.rs` is compiled in unchanged. See
//! WALKS-PREDICTIONS.md and WALKS-RESULTS.md.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use rannoch_cloudevents::{EventId, NodeRef};
use rannoch_store::{
    Class, Cursor, Declaration, Direction, Graph, Relation, Roots, Shape, Step, Value,
    ValueType, Version, View, Way, open,
};
use rusqlite::Connection;
use serde_json::{Value as Json, json};

#[path = "../../../../crates/rannoch-relay/src/warm.rs"]
mod warm;

use warm::{Warm, WarmSet};

/// What `warm.rs` asks of its crate.
#[derive(Debug)]
pub enum Error {
    Refused(String),
    Store(rannoch_store::Error),
}

impl From<rannoch_store::Error> for Error {
    fn from(error: rannoch_store::Error) -> Error {
        Error::Store(error)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

const KEY: [u8; 32] = [11; 32];
const SOURCE: &str = "rannoch:toy";

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn declared(name: &str) -> String {
    json!({"specversion":"1.0","id":format!("d-{name}"),"source":SOURCE,"type":format!("rannoch.declared.{name}")}).to_string()
}

fn id() -> String {
    format!("{}:0", EventId::now())
}

fn node_with(id: &str, class: &str, key: &str, properties: &Json) -> String {
    json!({"specversion":"1.0","id":id,"source":SOURCE,"type":format!("rannoch.node.{class}"),
           "subject":key,"data":{"properties":properties}})
    .to_string()
}

fn node(class: &str, key: &str, properties: &Json) -> String {
    node_with(&id(), class, key, properties)
}

fn link(kind: &str, relation: &str, from: &NodeRef, to: &NodeRef) -> String {
    json!({"specversion":"1.0","id":id(),"source":SOURCE,"type":format!("rannoch.{kind}.{relation}"),
           "data":{"from":from,"to":to}})
    .to_string()
}

fn amended(target: &str, patch: &Json) -> String {
    json!({"specversion":"1.0","id":id(),"source":SOURCE,"type":"rannoch.amended",
           "data":{"target":{"source":SOURCE,"id":target},"patch":patch}})
    .to_string()
}

fn step(relation: &str, cap: Option<usize>) -> Step {
    Step { relation: relation.into(), cap, ..Step::default() }
}

fn anchored(name: &str, class: &str, key: &str, steps: Vec<Step>) -> Shape {
    Shape {
        name: name.into(),
        roots: Roots::Node { node: NodeRef::new(class, key) },
        steps,
    }
}

fn no_spawn() -> BTreeMap<String, Value> {
    BTreeMap::from([("kind".to_string(), Value::from("Spawn"))])
}

// --- Loading ---

fn pairs(spice: &Connection, sql: &str) -> Res<Vec<(String, String)>> {
    Ok(spice
        .prepare(sql)?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?)
}

fn location(fqn: &str) -> Option<&str> {
    let (_, rest) = fqn.split_once(".location.")?;
    rest.split('.').next()
}

struct World {
    /// The writer, whose commits and fills are outside every timer.
    g: Graph,
    /// One read handle for the short walks, one for the rewalks, one for the check.
    short: Graph,
    full: Graph,
    check: Graph,
}

fn load(g: &mut Graph, bodies: &[String]) -> Res<Vec<Cursor>> {
    let mut cursors = Vec::with_capacity(bodies.len());
    for chunk in bodies.chunks(5_000) {
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        for landed in g.commit(&refs, &[])? {
            cursors.push(landed?.cursor);
        }
        g.fill()?;
    }
    Ok(cursors)
}

/// Loads the store and returns it with the shapes, smallest first.
fn build(spice: &Connection, path: &Path) -> Res<(Graph, Vec<Shape>)> {
    let mut g = open(path, &KEY)?;
    let mut thing = Class::new("thing", ["kind", "fqn"]);
    thing.properties.insert("logname".into(), ValueType::Integer);
    thing.indexed = BTreeSet::from(["logname".to_string()]);
    for class in [
        Class::new("npc", ["n"]),
        Class::new("cnv", ["n"]),
        Class::new("quest", ["n"]),
        Class::new("material", ["n"]),
        Class::new("schematic", ["n"]),
        Class::new("item", ["n"]),
        Class::new("planet", ["n"]),
        thing,
    ] {
        let name = class.name.clone();
        g.declare(&Declaration::Class(class), &declared(&name))?;
    }
    for relation in [
        Relation::new("speaks_in", "npc", "cnv", false),
        Relation::new("next_in_arc", "quest", "quest", false),
        Relation::new("uses", "schematic", "material", false),
        Relation::new("crafts", "schematic", "item", false),
        Relation::new("on", "thing", "planet", false),
        Relation::new("targets", "thing", "thing", false),
    ] {
        let name = relation.name.clone();
        g.declare(&Declaration::Relation(relation), &declared(&name))?;
    }

    let mut nodes = Vec::new();
    let mut links = Vec::new();
    let mut seen: BTreeSet<(&str, String)> = BTreeSet::new();
    let mut named = |nodes: &mut Vec<String>, class: &'static str, key: &str| {
        if seen.insert((class, key.to_string())) {
            nodes.push(node(class, key, &json!({"n": key})));
        }
    };
    let r = |class: &str, key: &str| NodeRef::new(class, key);
    for (npc, cnv) in pairs(spice, "SELECT npc_fqn, cnv_fqn FROM conversation_npcs")? {
        named(&mut nodes, "npc", &npc);
        named(&mut nodes, "cnv", &cnv);
        links.push(link("link", "speaks_in", &r("npc", &npc), &r("cnv", &cnv)));
    }
    let chain = pairs(spice, "SELECT source_game_id, target_game_id FROM quest_chain")?;
    for (from, to) in &chain {
        named(&mut nodes, "quest", from);
        named(&mut nodes, "quest", to);
        links.push(link("link", "next_in_arc", &r("quest", from), &r("quest", to)));
    }
    for (schematic, material) in pairs(spice, "SELECT schematic_fqn, material_fqn FROM schematic_materials")? {
        named(&mut nodes, "schematic", &schematic);
        named(&mut nodes, "material", &material);
        links.push(link("link", "uses", &r("schematic", &schematic), &r("material", &material)));
    }
    for (schematic, item) in pairs(spice, "SELECT schematic_fqn, output_fqn FROM schematics WHERE output_fqn IS NOT NULL AND output_fqn != ''")? {
        named(&mut nodes, "schematic", &schematic);
        named(&mut nodes, "item", &item);
        links.push(link("link", "crafts", &r("schematic", &schematic), &r("item", &item)));
    }
    let mut things = BTreeSet::new();
    let mut per_planet: BTreeMap<String, usize> = BTreeMap::new();
    let mut rows = spice.prepare(
        "SELECT fqn, kind, string_id FROM objects WHERE fqn LIKE '%.location.%' AND is_canonical = 1
         AND kind IN ('Npc','Quest','Placeable','Codex','Conversation','Encounter','Phase','Spawn')",
    )?;
    let listed: Vec<(String, String, Option<i64>)> = rows
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<_, _>>()?;
    for (fqn, kind, string_id) in listed {
        let Some(planet) = location(&fqn).map(str::to_string) else { continue };
        if !things.insert(fqn.clone()) {
            continue;
        }
        named(&mut nodes, "planet", &planet);
        if kind != "Spawn" {
            *per_planet.entry(planet.clone()).or_default() += 1;
        }
        let properties = match string_id {
            Some(id) => json!({"kind": kind, "fqn": fqn, "logname": {"integer": id << 32}}),
            None => json!({"kind": kind, "fqn": fqn}),
        };
        nodes.push(node("thing", &fqn, &properties));
        links.push(link("link", "on", &r("thing", &fqn), &r("planet", &planet)));
    }
    for (quest, target) in pairs(spice, "SELECT DISTINCT quest_fqn, target_fqn FROM quest_objectives WHERE target_fqn IS NOT NULL")? {
        if things.contains(&quest) && things.contains(&target) {
            links.push(link("link", "targets", &r("thing", &quest), &r("thing", &target)));
        }
    }

    // A small closure, and the cycle's, by walking the chain.
    let mut out: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (from, to) in &chain {
        out.entry(from).or_default().push(to);
    }
    let closure = |root: &str| {
        let mut seen = BTreeSet::from([root.to_string()]);
        let mut at = vec![root.to_string()];
        while let Some(q) = at.pop() {
            for next in out.get(q.as_str()).into_iter().flatten() {
                if seen.insert((*next).to_string()) {
                    at.push((*next).to_string());
                }
            }
        }
        seen.len()
    };
    let small = out
        .keys()
        .find(|q| (4..=8).contains(&closure(q)))
        .ok_or("a small closure")?
        .to_string();
    let cycle: String = spice.query_row(
        "SELECT a.source_game_id FROM quest_chain a JOIN quest_chain b
         ON a.source_game_id = b.target_game_id AND a.target_game_id = b.source_game_id
         ORDER BY 1 LIMIT 1",
        [],
        |r| r.get(0),
    )?;
    let largest = per_planet
        .iter()
        .filter(|(p, _)| p.as_str() != "hutta")
        .max_by_key(|(_, n)| **n)
        .map(|(p, _)| p.clone())
        .ok_or("a planet")?;
    println!("planets by size: largest {largest} with {} before targets", per_planet[&largest]);
    let hub = "itm.mat.craft.endgame.stabilizer.tier2";
    let walk = |name: &str, root: &str| {
        anchored(name, "quest", root, (0..50).map(|_| step("next_in_arc", Some(1000))).collect())
    };
    let planet_steps = || {
        vec![Step { relation: "on<-".into(), drop: no_spawn(), ..Step::default() }, step("targets", None)]
    };
    let shapes = vec![
        walk("small closure", &small),
        walk("cycle", &cycle),
        anchored("npc 216", "npc", "npc.location.universal.invisible_npc", vec![step("speaks_in", None)]),
        anchored("hub capped", "material", hub, vec![step("uses<-", Some(500)), step("crafts", None)]),
        anchored("hub exact", "material", hub, vec![step("uses<-", None), step("crafts", None)]),
        anchored("hutta", "planet", "hutta", planet_steps()),
        anchored("largest planet", "planet", &largest, planet_steps()),
        Shape {
            name: "every planet".into(),
            roots: Roots::Class { class: "planet".into(), filter: BTreeMap::new() },
            steps: planet_steps(),
        },
    ];
    nodes.extend(links);
    let began = Instant::now();
    load(&mut g, &nodes)?;
    println!("loaded {} events in {:?}", nodes.len(), began.elapsed());
    Ok((g, shapes))
}

// --- The check ---

/// A shape's walk through #175's reads, written apart from the relay.
fn independent(graph: &Graph, shape: &Shape) -> Res<(BTreeMap<NodeRef, Version>, Vec<(Vec<String>, usize)>)> {
    let view = View::default();
    let ontology = graph.current_ontology()?;
    let mut seen = BTreeMap::new();
    let mut frontier = Vec::new();
    match &shape.roots {
        Roots::Node { node } => {
            let root = NodeRef::new(node.class.clone(), node.key.clone());
            if let Some(v) = graph.state_at(&root, &view)? {
                seen.insert(root.clone(), v);
                frontier.push(root);
            }
        }
        Roots::Class { class, filter } => {
            for (key, v) in graph.nodes(class, filter, &view)? {
                let root = NodeRef::new(class.clone(), key);
                seen.insert(root.clone(), v);
                frontier.push(root);
            }
        }
    }
    let mut steps = Vec::new();
    for s in &shape.steps {
        let (name, way) = s.way();
        let relation = ontology.relation(name)?;
        let mut found: BTreeMap<(String, String), Version> = BTreeMap::new();
        for at in &frontier {
            let mut ends = Vec::new();
            if way != Way::Backward && at.class == relation.from {
                ends.push((Direction::Out, relation.to.clone()));
            }
            if way != Way::Forward && at.class == relation.to {
                ends.push((Direction::In, relation.from.clone()));
            }
            for (direction, other) in ends {
                for (key, _) in graph.links(at, name, direction, &view)? {
                    let reached = NodeRef::new(other.clone(), key.clone());
                    if seen.contains_key(&reached) || found.contains_key(&(key.clone(), other.clone())) {
                        continue;
                    }
                    if let Some(v) = graph.state_at(&reached, &view)?
                        && s.passes(&v.properties)
                    {
                        found.insert((key, other.clone()), v);
                    }
                }
            }
        }
        let total = found.len();
        let cap = s.cap.unwrap_or(usize::MAX);
        let mut taken = Vec::new();
        frontier.clear();
        for ((key, class), v) in found.into_iter().take(cap) {
            let reached = NodeRef::new(class, key.clone());
            seen.insert(reached.clone(), v);
            frontier.push(reached);
            taken.push(key);
        }
        steps.push((taken, total.saturating_sub(cap)));
    }
    Ok((seen, steps))
}

type Plain = (BTreeMap<NodeRef, Version>, Vec<(Vec<String>, usize)>);

fn plain(set: &WarmSet) -> Plain {
    (
        set.nodes().iter().map(|(n, v)| ((**n).clone(), (**v).clone())).collect(),
        set.steps()
            .iter()
            .map(|s| (s.nodes.iter().map(|n| n.key.clone()).collect(), s.dropped))
            .collect(),
    )
}

fn differs(a: &Plain, b: &Plain) -> Option<String> {
    if a.1 != b.1 {
        return Some(format!("steps differ: {:?} vs {:?}", counts(a), counts(b)));
    }
    if a.0.len() != b.0.len() {
        return Some(format!("{} nodes vs {}", a.0.len(), b.0.len()));
    }
    for (node, version) in &a.0 {
        match b.0.get(node) {
            None => return Some(format!("{node:?} is only on one side")),
            Some(other) if other != version => return Some(format!("{node:?} has another version")),
            Some(_) => {}
        }
    }
    None
}

fn counts(p: &Plain) -> Vec<usize> {
    p.1.iter().map(|(t, _)| t.len()).collect()
}

// --- Measuring ---

#[derive(Default)]
struct Row {
    short_reads: Vec<u64>,
    short_time: Vec<Duration>,
    full_reads: Vec<u64>,
    full_time: Vec<Duration>,
}

fn pct<T: Copy + Ord + Default>(values: &[T], p: f64) -> T {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
    let at = ((sorted.len().saturating_sub(1)) as f64 * p).round() as usize;
    sorted.get(at).copied().unwrap_or_default()
}

fn us(d: Duration) -> String {
    format!("{:.0}", d.as_secs_f64() * 1e6)
}

struct Probe<'a> {
    world: &'a mut World,
    failures: &'a mut Vec<String>,
    samples: &'a mut u64,
}

impl Probe<'_> {
    /// Commits and fills `bodies`, untimed, returning where they landed.
    fn commit(&mut self, bodies: &[String]) -> Res<Vec<Cursor>> {
        load(&mut self.world.g, bodies)
    }

    /// Folds `bodies`, committed at `cursors`, then settles: the short walk, timed and counted.
    fn short(&self, warm: &mut Warm, cursors: &[Cursor], bodies: &[String]) -> Res<(u64, Duration)> {
        let before = warm.reads;
        let began = Instant::now();
        for (cursor, body) in cursors.iter().zip(bodies) {
            warm.fold(&self.world.short, *cursor, body);
        }
        warm.settle(&self.world.short)?;
        Ok((warm.reads - before, began.elapsed()))
    }

    /// A rewalk from nothing, timed and counted.
    fn full(&self, shape: &Shape) -> Res<(Warm, u64, Duration)> {
        let began = Instant::now();
        let warm = Warm::seed(&self.world.full, shape.clone(), View::default(), Cursor::START)?;
        let took = began.elapsed();
        let reads = warm.reads;
        Ok((warm, reads, took))
    }

    /// One whole sample: commit, short walk, rewalk, and the comparison with both checks.
    fn sample(&mut self, label: &str, warm: &mut Warm, shape: &Shape, bodies: &[String], row: &mut Row) -> Res {
        let cursors = self.commit(bodies)?;
        let (reads, took) = self.short(warm, &cursors, bodies)?;
        let (rewalked, full_reads, full_took) = self.full(shape)?;
        row.short_reads.push(reads);
        row.short_time.push(took);
        row.full_reads.push(full_reads);
        row.full_time.push(full_took);
        self.compare(label, warm, &rewalked, shape)
    }

    /// Any difference is a failure, and is listed first in the results.
    fn compare(&mut self, label: &str, warm: &Warm, rewalked: &Warm, shape: &Shape) -> Res {
        *self.samples += 1;
        let mine = plain(&warm.set);
        if let Some(why) = differs(&mine, &plain(&rewalked.set)) {
            self.failures.push(format!("{label}, against a rewalk: {why}"));
        }
        let cold = independent(&self.world.check, shape)?;
        if let Some(why) = differs(&mine, &cold) {
            self.failures.push(format!("{label}, against #175's reads: {why}"));
        }
        Ok(())
    }
}

/// The step-0 relation of `shape` as (relation name, the root is the link's from end, reached class).
fn first_step(world: &World, shape: &Shape) -> Res<(String, bool, String)> {
    let ontology = world.g.current_ontology()?;
    let (name, way) = shape.steps[0].way();
    let relation = ontology.relation(name)?;
    Ok(match way {
        Way::Backward => (name.to_string(), false, relation.from.clone()),
        _ => (name.to_string(), true, relation.to.clone()),
    })
}

/// A root of `shape`: its one node, or the lowest of its class in `set`.
fn root_of(set: &WarmSet, shape: &Shape) -> NodeRef {
    match &shape.roots {
        Roots::Node { node } => node.clone(),
        Roots::Class { class, .. } => set
            .nodes()
            .keys()
            .filter(|node| node.class == *class)
            .map(|node| (**node).clone())
            .min()
            .unwrap_or_else(|| NodeRef::new(class.clone(), "")),
    }
}

fn properties(class: &str, key: &str) -> Json {
    if class == "thing" {
        json!({"kind": "Toy", "fqn": key})
    } else {
        json!({"n": key})
    }
}

fn changed(set: &WarmSet, node: &NodeRef, text: &str) -> Res<Json> {
    let version = set.nodes().iter().find(|(n, _)| ***n == *node).map(|(_, v)| v).ok_or("a held node")?;
    let mut map = serde_json::to_value(&version.properties)?;
    let property = if node.class == "thing" { "fqn" } else { "n" };
    map[property] = json!(text);
    Ok(map)
}

fn property_of(class: &str) -> &'static str {
    if class == "thing" { "fqn" } else { "n" }
}

fn link_between(root: &NodeRef, other: &NodeRef, from_root: bool) -> (NodeRef, NodeRef) {
    if from_root { (root.clone(), other.clone()) } else { (other.clone(), root.clone()) }
}

/// Nodes in the set to change: those of the last step that took any, then the rest, no root.
fn held_nodes(set: &WarmSet) -> Vec<NodeRef> {
    let mut held: Vec<NodeRef> = Vec::new();
    for s in set.steps().iter().rev() {
        held.extend(s.nodes.iter().map(|n| (**n).clone()));
    }
    held
}

const KINDS: [&str; 4] = ["unheld", "held node changed", "link added", "link ended"];

fn reps(size: usize) -> usize {
    (20_000 / size.max(1)).clamp(5, 30)
}

fn measure_kinds(probe: &mut Probe<'_>, shape: &Shape, table: &mut Vec<String>) -> Res {
    let (relation, from_root, reached) = first_step(probe.world, shape)?;
    let mut warm = Warm::seed(&probe.world.short, shape.clone(), View::default(), Cursor::START)?;
    let size = warm.set.nodes().len();
    let n = reps(size);
    let root = root_of(&warm.set, shape);
    let name = &shape.name;
    println!("## {name}: {size} nodes, {n} samples per kind");
    let mut tag = 0usize;

    let mut row = Row::default();
    for _ in 0..n {
        tag += 1;
        let key = format!("zzz.unheld.{name}.{tag}");
        let body = node(&reached, &key, &properties(&reached, &key));
        probe.sample(&format!("{name}, unheld"), &mut warm, shape, &[body], &mut row)?;
    }
    report(table, name, size, "unheld", &row);

    let mut row = Row::default();
    let held = held_nodes(&warm.set);
    for i in 0..n {
        tag += 1;
        let at = &held[i % held.len()];
        let body = node(&at.class, &at.key, &changed(&warm.set, at, &format!("changed {tag}"))?);
        probe.sample(&format!("{name}, held node changed"), &mut warm, shape, &[body], &mut row)?;
    }
    report(table, name, size, "held node changed", &row);

    let mut row = Row::default();
    let mut added = Vec::new();
    for _ in 0..n {
        tag += 1;
        let key = format!("aaa.added.{name}.{tag}");
        let fresh = NodeRef::new(reached.clone(), key.clone());
        let body = node(&reached, &key, &properties(&reached, &key));
        let cursors = probe.commit(std::slice::from_ref(&body))?;
        let _ = probe.short(&mut warm, &cursors, std::slice::from_ref(&body))?;
        let (from, to) = link_between(&root, &fresh, from_root);
        let body = link("link", &relation, &from, &to);
        probe.sample(&format!("{name}, link added"), &mut warm, shape, &[body], &mut row)?;
        added.push((from, to));
    }
    report(table, name, size, "link added", &row);
    // What was added leaves again, so the neighbourhood is the size it was.
    let bodies: Vec<String> = added.iter().map(|(f, t)| link("ended", &relation, f, t)).collect();
    let cursors = probe.commit(&bodies)?;
    let _ = probe.short(&mut warm, &cursors, &bodies)?;

    let mut row = Row::default();
    let first: Vec<NodeRef> = warm.set.steps()[0].nodes.iter().map(|n| (**n).clone()).collect();
    if first.is_empty() {
        return Err(format!("{name} takes nothing at its first step").into());
    }
    for i in 0..n {
        let other = &first[i % first.len()];
        let (from, to) = link_between(&root, other, from_root);
        let body = link("ended", &relation, &from, &to);
        probe.sample(&format!("{name}, link ended"), &mut warm, shape, &[body], &mut row)?;
        let back = link("link", &relation, &from, &to);
        let cursors = probe.commit(std::slice::from_ref(&back))?;
        let _ = probe.short(&mut warm, &cursors, std::slice::from_ref(&back))?;
    }
    report(table, name, size, "link ended", &row);

    let mut row = Row::default();
    let held = held_nodes(&warm.set);
    for i in 0..n.min(10) {
        tag += 1;
        let at = &held[i % held.len()];
        let target = id();
        let body = node_with(&target, &at.class, &at.key, &changed(&warm.set, at, &format!("to amend {tag}"))?);
        let cursors = probe.commit(std::slice::from_ref(&body))?;
        let _ = probe.short(&mut warm, &cursors, std::slice::from_ref(&body))?;
        let patch = json!({"data": {"properties": {property_of(&at.class): format!("amended {tag}")}}});
        let body = amended(&target, &patch);
        probe.sample(&format!("{name}, amendment"), &mut warm, shape, &[body], &mut row)?;
    }
    report(table, name, size, "amendment of one property", &row);
    Ok(())
}

fn report(table: &mut Vec<String>, name: &str, size: usize, kind: &str, row: &Row) {
    let line = format!(
        "| {name} | {size} | {kind} | {} | {} | {} | {} | {} |",
        pct(&row.short_reads, 0.5),
        us(pct(&row.short_time, 0.5)),
        us(pct(&row.short_time, 0.95)),
        pct(&row.full_reads, 0.5),
        us(pct(&row.full_time, 0.5)),
    );
    println!("{line}");
    table.push(line);
}

/// Bursts in one commit. A walk after every event, a walk after them all (the relay's way), and one
/// rewalk, on the same commit.
fn measure_bursts(probe: &mut Probe<'_>, shape: &Shape, burst_kind: &str, table: &mut Vec<String>, crossings: &mut Vec<String>) -> Res {
    let (relation, from_root, reached) = first_step(probe.world, shape)?;
    let probe_warm = Warm::seed(&probe.world.short, shape.clone(), View::default(), Cursor::START)?;
    let size = probe_warm.set.nodes().len();
    let root = root_of(&probe_warm.set, shape);
    let name = &shape.name;
    let mut tag = 0usize;
    let mut cross = Crossing::default();
    let sizes = [1usize, 2, 3, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000];
    for &k in &sizes {
        // Set up the burst: `k` events of the kind, on nodes and links prepared untimed.
        let held = held_nodes(&probe_warm.set);
        let mut undo = Vec::new();
        let mut setup = Vec::new();
        let bodies: Vec<String> = match burst_kind {
            "unheld" => (0..k)
                .map(|_| {
                    tag += 1;
                    let key = format!("zzz.burst.{name}.{tag}");
                    node(&reached, &key, &properties(&reached, &key))
                })
                .collect(),
            "node changed" => {
                if k > held.len() {
                    break;
                }
                let mut out = Vec::new();
                for at in held.iter().take(k) {
                    tag += 1;
                    out.push(node(&at.class, &at.key, &changed(&probe_warm.set, at, &format!("burst {tag}"))?));
                }
                out
            }
            _ => {
                let mut out = Vec::new();
                for _ in 0..k {
                    tag += 1;
                    let key = format!("aaa.burst.{name}.{tag}");
                    setup.push(node(&reached, &key, &properties(&reached, &key)));
                    let fresh = NodeRef::new(reached.clone(), key);
                    let (from, to) = link_between(&root, &fresh, from_root);
                    undo.push((from.clone(), to.clone()));
                    out.push(link("link", &relation, &from, &to));
                }
                out
            }
        };
        if !setup.is_empty() {
            probe.commit(&setup)?;
        }
        let mut each = Warm::seed(&probe.world.short, shape.clone(), View::default(), Cursor::START)?;
        let mut all = Warm::seed(&probe.world.short, shape.clone(), View::default(), Cursor::START)?;
        let cursors = probe.commit(&bodies)?;

        // A walk after every event. The graph already holds the whole commit, so mid-burst the set
        // is not compared with a rewalk; it is after the last event.
        let walks_budget = Duration::from_secs(30);
        let before = each.reads;
        let began = Instant::now();
        let mut done = 0usize;
        for (cursor, body) in cursors.iter().zip(&bodies) {
            each.fold(&probe.world.short, *cursor, body);
            each.settle(&probe.world.short)?;
            done += 1;
            if began.elapsed() > walks_budget {
                break;
            }
        }
        let each_took = began.elapsed();
        let each_reads = each.reads - before;
        let each_whole = done == bodies.len();

        let (all_reads, all_took) = probe.short(&mut all, &cursors, &bodies)?;
        let (rewalked, full_reads, full_took) = probe.full(shape)?;
        let label = format!("{name}, burst of {k} {burst_kind}");
        probe.compare(&label, &all, &rewalked, shape)?;
        if each_whole {
            probe.compare(&format!("{label}, a walk per event"), &each, &rewalked, shape)?;
        }
        let each_cell = if each_whole {
            format!("{each_reads} | {}", us(each_took))
        } else {
            format!("{each_reads}+ | over {} after {done}", us(each_took))
        };
        let line = format!(
            "| {name} | {} | {burst_kind} | {k} | {each_cell} | {all_reads} | {} | {full_reads} | {} |",
            rewalked.set.nodes().len(),
            us(all_took),
            us(full_took),
        );
        println!("{line}");
        table.push(line);
        cross.see(k, (each_reads, each_took, each_whole), (all_reads, all_took), (full_reads, full_took));
        // What was added leaves again, minted now so it follows what it ends.
        if !undo.is_empty() {
            let bodies: Vec<String> = undo.iter().map(|(f, t)| link("ended", &relation, f, t)).collect();
            load(&mut probe.world.g, &bodies)?;
        }
    }
    crossings.push(format!(
        "| {name} | {size} | {burst_kind} | {} | {} | {} | {} |",
        cross.show(cross.each_reads),
        cross.show(cross.each_time),
        cross.show(cross.all_reads),
        cross.show(cross.all_time),
    ));
    Ok(())
}

#[derive(Default)]
struct Crossing {
    each_reads: Option<usize>,
    each_time: Option<usize>,
    all_reads: Option<usize>,
    all_time: Option<usize>,
    largest: usize,
}

impl Crossing {
    fn see(&mut self, k: usize, each: (u64, Duration, bool), all: (u64, Duration), full: (u64, Duration)) {
        self.largest = k;
        if each.2 || each.1 > full.1 {
            if self.each_time.is_none() && each.1 > full.1 {
                self.each_time = Some(k);
            }
            if self.each_reads.is_none() && each.0 > full.0 {
                self.each_reads = Some(k);
            }
        }
        if self.all_reads.is_none() && all.0 > full.0 {
            self.all_reads = Some(k);
        }
        if self.all_time.is_none() && all.1 > full.1 {
            self.all_time = Some(k);
        }
    }

    fn show(&self, at: Option<usize>) -> String {
        at.map_or_else(|| format!("none to {}", self.largest), |k| k.to_string())
    }
}

fn measure_rename(probe: &mut Probe<'_>, shape: &Shape, table: &mut Vec<String>) -> Res {
    let mut warm = Warm::seed(&probe.world.short, shape.clone(), View::default(), Cursor::START)?;
    let size = warm.set.nodes().len();
    let name = shape.name.clone();
    let mut row = Row::default();
    for i in 0..3 {
        let held = held_nodes(&warm.set);
        let at = held.first().ok_or("a held node")?.clone();
        let target = id();
        let body = node_with(&target, &at.class, &at.key, &changed(&warm.set, &at, &format!("to rename {i}"))?);
        let cursors = probe.commit(std::slice::from_ref(&body))?;
        let _ = probe.short(&mut warm, &cursors, std::slice::from_ref(&body))?;
        let body = amended(&target, &json!({"subject": format!("{}.renamed{i}", at.key)}));
        probe.sample(&format!("{name}, rename"), &mut warm, shape, &[body], &mut row)?;
    }
    report(table, &name, size, "rename", &row);
    Ok(())
}

fn main() -> Res {
    let args: Vec<String> = std::env::args().collect();
    let Some(spice_path) = args.get(1) else {
        return Err("usage: walks <spice-7.9.a-v7.sqlite> [only a neighbourhood's name]".into());
    };
    let only = args.get(2);
    let spice = Connection::open_with_flags(Path::new(spice_path), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("graph.rannoch");
    let (g, shapes) = build(&spice, &path)?;
    let mut world = World {
        g,
        short: open(&path, &KEY)?,
        full: open(&path, &KEY)?,
        check: open(&path, &KEY)?,
    };
    let shapes: Vec<Shape> = shapes.into_iter().filter(|s| only.is_none_or(|o| *o == s.name)).collect();
    let mut failures = Vec::new();
    let mut samples = 0;
    let mut kinds_table = Vec::new();
    let mut burst_table = Vec::new();
    let mut crossings = Vec::new();
    let began = Instant::now();
    {
        let mut probe = Probe { world: &mut world, failures: &mut failures, samples: &mut samples };
        for shape in &shapes {
            measure_kinds(&mut probe, shape, &mut kinds_table)?;
        }
        for shape in &shapes {
            for kind in ["unheld", "node changed", "link added"] {
                measure_bursts(&mut probe, shape, kind, &mut burst_table, &mut crossings)?;
            }
        }
        // Last: a rename slows every fill after it, and stays in the store.
        for shape in &shapes {
            measure_rename(&mut probe, shape, &mut kinds_table)?;
        }
    }
    println!("\n# FAILURES: {} differences over {samples} comparisons", failures.len());
    for failure in failures.iter().take(50) {
        println!("- {failure}");
    }
    println!("\n# By event kind (microseconds)\n| neighbourhood | nodes | kind | short reads | short p50 | short p95 | rewalk reads | rewalk p50 |\n|---|---|---|---|---|---|---|---|");
    for line in &kinds_table {
        println!("{line}");
    }
    println!("\n# Bursts in one commit (microseconds)\n| neighbourhood | nodes | kind | k | walk per event: reads | time | relay's way: reads | time | rewalk: reads | time |\n|---|---|---|---|---|---|---|---|---|---|");
    for line in &burst_table {
        println!("{line}");
    }
    println!("\n# Where one rewalk is cheaper: the smallest k tested\n| neighbourhood | nodes | kind | per event, reads | per event, time | relay's way, reads | relay's way, time |\n|---|---|---|---|---|---|---|");
    for line in &crossings {
        println!("{line}");
    }
    println!("\ntook {:?}", began.elapsed());
    if !failures.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}
