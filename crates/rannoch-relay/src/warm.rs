//! One relay's warm set: what a walk of its shape reached in its view, and the fold that keeps it
//! true as events arrive. An event names what it touched; only that is read again from the graph,
//! and the walk runs again over what is held in memory.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use rannoch_cloudevents::{Conclusion, Fact, LinkFact, NodeFact, NodeRef, ValidTime};
use rannoch_store::{
    AMENDED, Cursor, Direction, Graph, Ontology, REFILLED, Roots, Shape, Value, Version, View, Way,
    has_all, millis,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

use crate::Error;

/// A relay's warm set at one cursor. Reading it never reads the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmSet {
    cursor: Cursor,
    known: Option<i64>,
    held: Arc<Held>,
}

#[derive(Debug, Default)]
struct Held {
    nodes: HashMap<Id, Arc<Version>>,
    steps: Vec<StepSet>,
    ontology: Arc<Ontology>,
    /// Each node in the set by a property its class indexes, and the value's text. Built once per
    /// set, after the set is published and its change told.
    indexed: OnceLock<HashMap<String, HashMap<String, Vec<Id>>>>,
}

/// Two sets are equal when they hold the same nodes and steps, whether or not either has built its
/// index.
impl PartialEq for Held {
    fn eq(&self, other: &Held) -> bool {
        self.nodes == other.nodes && self.steps == other.steps
    }
}

impl Eq for Held {}

impl Held {
    fn index(&self) -> &HashMap<String, HashMap<String, Vec<Id>>> {
        self.indexed.get_or_init(|| {
            let mut indexed: HashMap<String, HashMap<String, Vec<Id>>> = HashMap::new();
            for (node, version) in &self.nodes {
                if let Some(class) = self.ontology.classes.get(&node.class) {
                    for property in &class.indexed {
                        if let Some(value) = version.properties.get(property) {
                            indexed
                                .entry(property.clone())
                                .or_default()
                                .entry(value_text(value))
                                .or_default()
                                .push(Arc::clone(node));
                        }
                    }
                }
            }
            for nodes in indexed.values_mut().flat_map(HashMap::values_mut) {
                nodes.sort_by(key_order);
            }
            indexed
        })
    }
}

/// A node's name, shared by everything that holds it, so walking copies no text.
type Id = Arc<NodeRef>;

/// What one step of a walk took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepSet {
    /// The distinct nodes it took, in key order.
    pub nodes: Vec<Arc<NodeRef>>,
    /// How many distinct unvisited nodes its cap dropped.
    pub dropped: usize,
}

impl WarmSet {
    /// The place in the log this set is warm to.
    #[must_use]
    pub fn cursor(&self) -> Cursor {
        self.cursor
    }

    /// The latest known-at among the facts the walk read, or the view's own known-at.
    pub(crate) fn known(&self) -> Option<i64> {
        self.known
    }

    /// Every node in the set, roots included, each once, with its version in the relay's view.
    #[must_use]
    pub fn nodes(&self) -> &HashMap<Arc<NodeRef>, Arc<Version>> {
        &self.held.nodes
    }

    /// What each step took, in the shape's order.
    #[must_use]
    pub fn steps(&self) -> &[StepSet] {
        &self.held.steps
    }

    /// The nodes in the set whose class indexes `property` and whose value of it is `value`.
    #[must_use]
    pub fn by(&self, property: &str, value: &Value) -> &[Arc<NodeRef>] {
        self.held
            .index()
            .get(property)
            .and_then(|values| values.get(&value_text(value)))
            .map_or(&[], Vec::as_slice)
    }

    /// Builds what [`WarmSet::by`] reads, so no reader waits on it.
    pub(crate) fn prepare(&self) {
        self.held.index();
    }

    /// The acknowledgement's `steps`.
    pub(crate) fn counts(&self) -> Json {
        Json::Array(
            self.steps()
                .iter()
                .map(|step| json!({"nodes": step.nodes.len(), "cut": step.dropped > 0, "dropped": step.dropped}))
                .collect(),
        )
    }
}

/// `value` as an index's key: its wire form, which tells one type from another.
fn value_text(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// The other ends of the links that hold, per node, for one relation and direction.
type Ends = HashMap<Id, BTreeSet<Id>>;

/// What is read of one relation: its links from a node, and to it.
#[derive(Debug, Default)]
struct Read {
    out: Ends,
    to: Ends,
}

impl Read {
    fn of(&mut self, direction: Direction) -> &mut Ends {
        match direction {
            Direction::Out => &mut self.out,
            Direction::In => &mut self.to,
        }
    }
}

/// A relay's fold: the walk's result, and every node and link list the walk has read.
#[derive(Debug)]
pub(crate) struct Warm {
    shape: Shape,
    view: View,
    /// Its cursor moves only when a settle has read what was folded, so it never claims more.
    pub(crate) set: WarmSet,
    /// The last event folded.
    folded: Cursor,
    /// The class of root nodes chosen by a filter, whose every node event may change them.
    roots: BTreeSet<Id>,
    /// Every node the walk reached, cut ones too; `None` before its first version in the view.
    versions: HashMap<Id, Option<Arc<Version>>>,
    /// Every list of links the walk has read, by relation.
    adjacent: HashMap<String, Read>,
    /// The latest valid time an event in this batch gave a link, per end.
    via: HashMap<NodeRef, ValidTime>,
    last_via: Option<ValidTime>,
    /// The walk must run again.
    moved: bool,
    /// How many reads of the graph this relay has made: the cause of what a fold costs, which the
    /// tests count. The time it takes is measured in `toys/relay`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) reads: u64,
    /// How many nodes its walks have visited: what a walk costs where no read is made.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) walked: u64,
    /// Everything read may be wrong, as an amendment or a rename makes it: read it all again.
    stale: bool,
}

/// What a relay's file holds besides its shape and view, which the manifest holds.
#[derive(Serialize, Deserialize)]
pub(crate) struct Saved {
    pub(crate) cursor: Cursor,
    known: Option<i64>,
    roots: Vec<NodeRef>,
    versions: Vec<(NodeRef, Option<Version>)>,
    adjacent: Vec<(String, Direction, NodeRef, Vec<NodeRef>)>,
}

impl Warm {
    /// Seeds a relay of `shape` in `view` from `graph`, warm to `cursor`.
    pub(crate) fn seed(
        graph: &Graph,
        shape: Shape,
        view: View,
        cursor: Cursor,
    ) -> Result<Warm, Error> {
        let mut warm = Warm::empty(shape, view, cursor);
        warm.reseed(graph)?;
        warm.settle(graph)?;
        Ok(warm)
    }

    fn empty(shape: Shape, view: View, cursor: Cursor) -> Warm {
        let known = view.known.map(millis);
        Warm {
            shape,
            view,
            set: WarmSet {
                cursor,
                known,
                held: Arc::default(),
            },
            folded: cursor,
            roots: BTreeSet::new(),
            versions: HashMap::default(),
            adjacent: HashMap::new(),
            via: HashMap::new(),
            last_via: None,
            moved: false,
            reads: 0,
            walked: 0,
            stale: false,
        }
    }

    /// A relay as its file saved it. The walk needs no read for what the file holds.
    pub(crate) fn restored(
        graph: &Graph,
        shape: Shape,
        view: View,
        saved: Saved,
    ) -> Result<Warm, Error> {
        let mut warm = Warm::empty(shape, view, saved.cursor);
        warm.set.known = saved.known;
        warm.roots = saved.roots.into_iter().map(Arc::new).collect();
        warm.versions = saved
            .versions
            .into_iter()
            .map(|(node, version)| (Arc::new(node), version.map(Arc::new)))
            .collect();
        for (relation, direction, node, others) in saved.adjacent {
            warm.adjacent
                .entry(relation)
                .or_default()
                .of(direction)
                .insert(Arc::new(node), others.into_iter().map(Arc::new).collect());
        }
        warm.moved = true;
        warm.settle(graph)?;
        Ok(warm)
    }

    /// What the relay's file holds.
    pub(crate) fn saved(&self) -> Saved {
        Saved {
            cursor: self.set.cursor,
            known: self.set.known,
            roots: self.roots.iter().map(|node| (**node).clone()).collect(),
            versions: self
                .versions
                .iter()
                .map(|(node, version)| ((**node).clone(), version.as_deref().cloned()))
                .collect(),
            adjacent: self
                .adjacent
                .iter()
                .flat_map(|(relation, read)| {
                    [(Direction::Out, &read.out), (Direction::In, &read.to)]
                        .into_iter()
                        .flat_map(move |(direction, ends)| {
                            ends.iter().map(move |(node, others)| {
                                (
                                    relation.clone(),
                                    direction,
                                    (**node).clone(),
                                    others.iter().map(|o| (**o).clone()).collect(),
                                )
                            })
                        })
                })
                .collect(),
        }
    }

    /// Forgets everything read and reads the roots again.
    fn reseed(&mut self, graph: &Graph) -> Result<(), Error> {
        self.versions.clear();
        self.adjacent.clear();
        self.roots.clear();
        self.stale = false;
        self.moved = true;
        match &self.shape.roots {
            Roots::Node { node } => {
                self.roots.insert(Arc::new(node.clone()));
            }
            Roots::Class { class, filter } => {
                self.reads += 1;
                let listed = graph.nodes(class, filter, &self.view)?;
                for (key, version) in listed {
                    let node = Arc::new(NodeRef::new(class.clone(), key));
                    raise(&mut self.set.known, &version, &self.view);
                    self.versions
                        .insert(Arc::clone(&node), Some(Arc::new(version)));
                    self.roots.insert(node);
                }
            }
        }
        Ok(())
    }

    /// Whatever went wrong, the next settle reads everything again.
    pub(crate) fn mark_stale(&mut self) {
        self.stale = true;
    }

    /// Whether the next settle must read everything again, so what is held cannot be trusted.
    pub(crate) fn is_stale(&self) -> bool {
        self.stale
    }

    /// Folds one event. An event of nothing this relay holds costs its parse.
    pub(crate) fn fold(&mut self, graph: &Graph, cursor: Cursor, body: &str) {
        self.folded = cursor;
        if self.stale {
            return;
        }
        match rannoch_cloudevents::read(body) {
            Ok(Fact::None) | Err(_) => {
                // An amendment can move anything, and a retried fill was folded before it held.
                self.stale |= rannoch_cloudevents::attributes(body)
                    .is_ok_and(|event| event.ty == AMENDED || event.ty == REFILLED);
            }
            Ok(fact) => self.stale |= self.touch(graph, &fact).is_err(),
        }
    }

    fn touch(&mut self, graph: &Graph, fact: &Fact) -> Result<(), Error> {
        match fact {
            Fact::Node(NodeFact { class, key, .. }) => {
                let node = Arc::new(NodeRef::new(class.clone(), key.clone()));
                let root_class =
                    matches!(&self.shape.roots, Roots::Class { class: c, .. } if c == class);
                if root_class || self.versions.contains_key(&*node) {
                    self.read_node(graph, node)?;
                }
            }
            Fact::Link(LinkFact {
                relation, from, to, ..
            })
            | Fact::Concluded(Conclusion {
                relation, from, to, ..
            }) => {
                if self.follows(relation) {
                    self.read_link(graph, relation, from, to)?;
                }
            }
            Fact::Declared | Fact::None => {}
        }
        Ok(())
    }

    fn follows(&self, relation: &str) -> bool {
        self.shape.steps.iter().any(|s| s.way().0 == relation)
    }

    /// Reads a node an event named again. A node the graph knows under no name it was just heard
    /// by has been renamed: only reading everything again finds where it went.
    fn read_node(&mut self, graph: &Graph, node: Id) -> Result<(), Error> {
        self.reads += 1;
        let version = graph.state_at(&node, &self.view)?;
        if version.is_none() {
            self.reads += 1;
            if graph.state_at(&node, &View::default())?.is_none() {
                self.stale = true;
                return Ok(());
            }
        }
        let root = match &self.shape.roots {
            Roots::Class { class, filter } if *class == node.class => Some(
                version
                    .as_ref()
                    .is_some_and(|v| has_all(&v.properties, filter)),
            ),
            _ => None,
        };
        // A node of the root class that is no root and was never reached is nothing to this relay.
        if root != Some(true) && !self.versions.contains_key(&node) {
            return Ok(());
        }
        let held = self.versions.get(&node).and_then(|v| v.as_deref());
        if held == version.as_ref() {
            return Ok(());
        }
        if let Some(version) = &version {
            raise(&mut self.set.known, version, &self.view);
        }
        self.moved = true;
        match root {
            Some(true) => {
                self.roots.insert(node.clone());
            }
            Some(false) => {
                self.roots.remove(&node);
            }
            None => {}
        }
        self.versions.insert(node, version.map(Arc::new));
        Ok(())
    }

    /// Reads a link an event named again, and moves it in whichever lists hold its ends.
    fn read_link(
        &mut self,
        graph: &Graph,
        relation: &str,
        from: &NodeRef,
        to: &NodeRef,
    ) -> Result<(), Error> {
        self.reads += 1;
        let version = graph.link_state_at(relation, &from.key, &to.key, &self.view)?;
        if version.is_none() {
            self.reads += 1;
            if graph
                .link_state_at(relation, &from.key, &to.key, &View::default())?
                .is_none()
            {
                self.stale = true;
                return Ok(());
            }
        }
        let holds = version.as_ref().is_some_and(|v| !v.ended);
        let mut changed = false;
        if let Some(read) = self.adjacent.get_mut(relation) {
            for (direction, end, other) in [(Direction::Out, from, to), (Direction::In, to, from)] {
                if let Some(others) = read.of(direction).get_mut::<NodeRef>(end) {
                    changed |= if holds {
                        others.insert(Arc::new(other.clone()))
                    } else {
                        others.remove::<NodeRef>(other)
                    };
                }
            }
        }
        if let Some(version) = version
            && changed
        {
            raise(&mut self.set.known, &version, &self.view);
            self.via.insert(from.clone(), version.valid.clone());
            self.via.insert(to.clone(), version.valid.clone());
            self.last_via = Some(version.valid);
            self.moved = true;
        }
        Ok(())
    }

    /// Walks the shape over what is held, reading what is not. Inside a step, nodes are taken in
    /// key order; a node taken once is marked, so a cycle never repeats it. A node a cap cuts is
    /// not marked or expanded.
    fn walk(&mut self, graph: &Graph, ontology: &Arc<Ontology>) -> Result<Held, Error> {
        let size = self.set.held.nodes.len() + 16;
        let mut held = Held {
            nodes: HashMap::with_capacity(size),
            ontology: Arc::clone(ontology),
            ..Held::default()
        };
        // Every node marked: those taken, and those this step has already looked at.
        let mut visited: HashSet<Id> = HashSet::with_capacity(size);
        self.walked += self.roots.len() as u64;
        let mut frontier = Vec::with_capacity(self.roots.len());
        for root in &self.roots {
            if let Some(version) = version_of(
                graph,
                &self.view,
                &mut self.versions,
                (&mut self.set.known, &mut self.reads),
                root,
            )? {
                held.nodes.insert(Arc::clone(root), version);
                frontier.push(Arc::clone(root));
                visited.insert(Arc::clone(root));
            }
        }
        frontier.sort_by(key_order);
        for index in 0..self.shape.steps.len() {
            frontier =
                self.take_step(graph, ontology, index, &frontier, &mut visited, &mut held)?;
        }
        Ok(held)
    }

    /// Takes step `index` of the shape from `frontier`: what it reached goes into `held`, and what
    /// it took is the next frontier.
    fn take_step(
        &mut self,
        graph: &Graph,
        ontology: &Ontology,
        index: usize,
        frontier: &[Id],
        visited: &mut HashSet<Id>,
        held: &mut Held,
    ) -> Result<Vec<Id>, Error> {
        let step = &self.shape.steps[index];
        let (name, way) = step.way();
        let relation = ontology
            .relation(name)
            .map_err(|refused| Error::Refused(refused.0))?;
        let read = self.adjacent.entry(name.to_string()).or_default();
        let mut found: Vec<(Id, Arc<Version>)> = Vec::new();
        let mut looked: Vec<Id> = Vec::new();
        for node in frontier {
            for (direction, end, other, followed) in [
                (
                    Direction::Out,
                    &relation.from,
                    &relation.to,
                    way != Way::Backward,
                ),
                (
                    Direction::In,
                    &relation.to,
                    &relation.from,
                    way != Way::Forward,
                ),
            ] {
                if !followed || *end != node.class {
                    continue;
                }
                let ends = read.of(direction);
                if !ends.contains_key(&**node) {
                    let mut others = BTreeSet::new();
                    self.reads += 1;
                    for (key, version) in graph.links(node, name, direction, &self.view)? {
                        raise(&mut self.set.known, &version, &self.view);
                        others.insert(Arc::new(NodeRef::new(other.clone(), key)));
                    }
                    ends.insert(Arc::clone(node), others);
                }
                let Some(others) = ends.get(&**node) else {
                    continue;
                };
                for reached in others {
                    if !visited.insert(Arc::clone(reached)) {
                        continue;
                    }
                    self.walked += 1;
                    let version = version_of(
                        graph,
                        &self.view,
                        &mut self.versions,
                        (&mut self.set.known, &mut self.reads),
                        reached,
                    )?;
                    match version {
                        Some(v) if step.passes(&v.properties) => {
                            found.push((Arc::clone(reached), v));
                        }
                        _ => looked.push(Arc::clone(reached)),
                    }
                }
            }
        }
        // Already in key order when one node's list reached them all.
        found.sort_by(|a, b| key_order(&a.0, &b.0));
        let cap = step.cap.unwrap_or(usize::MAX);
        let dropped = found.len().saturating_sub(cap);
        // A node filtered out or cut is not marked: a later step may reach it.
        for (node, _) in found.iter().skip(cap) {
            visited.remove(node);
        }
        for node in &looked {
            visited.remove(node);
        }
        found.truncate(cap);
        let mut taken = Vec::with_capacity(found.len());
        for (node, version) in found {
            held.nodes.insert(Arc::clone(&node), version);
            taken.push(node);
        }
        held.steps.push(StepSet {
            nodes: taken.clone(),
            dropped,
        });
        Ok(taken)
    }

    /// Brings the set up to what was folded, and returns the deltas as a change's data, or `None`
    /// when nothing in the set changed.
    pub(crate) fn settle(&mut self, graph: &Graph) -> Result<Option<Json>, Error> {
        if self.stale {
            self.reseed(graph)?;
        }
        if !self.moved {
            self.set.cursor = self.folded;
            return Ok(None);
        }
        let ontology = graph.current_ontology()?;
        let held = self.walk(graph, &ontology)?;
        let change = self.delta(&held);
        self.set.held = Arc::new(held);
        self.set.cursor = self.folded;
        self.moved = false;
        self.via.clear();
        self.last_via = None;
        Ok(change)
    }

    /// What differs between the set as it stands and `new`.
    fn delta(&self, new: &Held) -> Option<Json> {
        let old = &self.set.held.nodes;
        let mut changed = Vec::new();
        let mut entered = Vec::new();
        let mut left = Vec::new();
        // Only what differs, in key order, so the same log tells the same change.
        let mut in_order: Vec<(&Id, &Arc<Version>)> = new
            .nodes
            .iter()
            .filter(|(node, version)| {
                old.get(*node)
                    .is_none_or(|before| !Arc::ptr_eq(before, version))
            })
            .collect();
        in_order.sort_by(|a, b| key_order(a.0, b.0));
        for (node, version) in in_order {
            match old.get(node) {
                None => {
                    entered.push(json!({"node": **node, "valid": self.moved_valid(node, version)}));
                }
                Some(before) => {
                    let names: BTreeSet<&String> = before
                        .properties
                        .keys()
                        .chain(version.properties.keys())
                        .collect();
                    for property in names {
                        let (was, is) = (
                            before.properties.get(property),
                            version.properties.get(property),
                        );
                        if was != is {
                            changed.push(json!({
                                "node": **node, "property": property,
                                "before": was, "after": is, "valid": version.valid,
                            }));
                        }
                    }
                }
            }
        }
        let mut gone: Vec<(&Id, &Arc<Version>)> = old
            .iter()
            .filter(|(node, _)| !new.nodes.contains_key(*node))
            .collect();
        gone.sort_by(|a, b| key_order(a.0, b.0));
        for (node, version) in gone {
            left.push(json!({"node": **node, "valid": self.moved_valid(node, version)}));
        }
        if changed.is_empty() && entered.is_empty() && left.is_empty() {
            return None;
        }
        Some(json!({
            "cursor": self.folded,
            "changed": changed,
            "entered": entered,
            "left": left,
        }))
    }

    /// The valid time of a node's coming or going: of the link that moved it, or of the last link
    /// that moved anything when it moved downstream of one; else of its own version.
    fn moved_valid(&self, node: &Id, version: &Version) -> ValidTime {
        self.via
            .get(&**node)
            .or(self.last_via.as_ref())
            .unwrap_or(&version.valid)
            .clone()
    }
}

/// Moves `known` up to `version`'s, unless the view fixed it.
fn raise(known: &mut Option<i64>, version: &Version, view: &View) {
    if view.known.is_none() {
        let at = millis(version.known);
        if known.is_none_or(|k| k < at) {
            *known = Some(at);
        }
    }
}

/// `node`'s version in `view`, read once.
fn version_of(
    graph: &Graph,
    view: &View,
    versions: &mut HashMap<Id, Option<Arc<Version>>>,
    (known, reads): (&mut Option<i64>, &mut u64),
    node: &Id,
) -> Result<Option<Arc<Version>>, Error> {
    if let Some(version) = versions.get(node) {
        return Ok(version.clone());
    }
    *reads += 1;
    let version = graph.state_at(node, view)?.map(Arc::new);
    if let Some(version) = &version {
        raise(known, version, view);
    }
    versions.insert(Arc::clone(node), version.clone());
    Ok(version)
}

fn key_order(a: &Id, b: &Id) -> std::cmp::Ordering {
    (&a.key, &a.class).cmp(&(&b.key, &b.class))
}

#[cfg(test)]
mod tests {
    use rannoch_cloudevents::EventId;
    use rannoch_store::{Class, Cursor, Declaration, Relation, Step, open};
    use serde_json::json;

    use super::*;

    const KEY: [u8; 32] = [3; 32];

    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    fn node(class: &str, key: &str, title: &str) -> String {
        json!({"specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:t",
               "type": format!("rannoch.node.{class}"), "subject": key,
               "data": {"properties": {"title": title}}})
        .to_string()
    }

    fn link(kind: &str, relation: &str, from: (&str, &str), to: (&str, &str)) -> String {
        json!({"specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": "rannoch:t",
               "type": format!("rannoch.{kind}.{relation}"),
               "data": {"from": {"class": from.0, "key": from.1},
                        "to": {"class": to.0, "key": to.1}}})
        .to_string()
    }

    /// A material used by `count` schematics, each crafting an item, with `other` beside them.
    fn hub(dir: &tempfile::TempDir, count: usize) -> Result<Graph> {
        let mut g = open(dir.path().join("graph"), &KEY)?;
        let declared = |name: &str| {
            json!({"specversion": "1.0", "id": format!("d-{name}"), "source": "rannoch:t",
                   "type": format!("rannoch.declared.{name}")})
            .to_string()
        };
        for class in ["material", "schematic", "item", "other"] {
            g.declare(
                &Declaration::Class(Class::new(class, ["title"])),
                &declared(class),
            )?;
        }
        for (name, from, to) in [
            ("uses", "schematic", "material"),
            ("crafts", "schematic", "item"),
            ("beside", "other", "other"),
        ] {
            g.declare(
                &Declaration::Relation(Relation::new(name, from, to, false)),
                &declared(name),
            )?;
        }
        let mut bodies = vec![node("material", "m", "t")];
        for i in 0..count {
            let (s, it) = (format!("s{:04}", 10 + i), format!("i{:04}", 10 + i));
            bodies.push(node("schematic", &s, "t"));
            bodies.push(node("item", &it, "t"));
            bodies.push(link("link", "uses", ("schematic", &s), ("material", "m")));
            bodies.push(link("link", "crafts", ("schematic", &s), ("item", &it)));
        }
        apply(&mut g, &bodies)?;
        Ok(g)
    }

    /// Commits and fills `bodies`, returning where each landed.
    fn apply(g: &mut Graph, bodies: &[String]) -> Result<Vec<Cursor>> {
        let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
        let mut cursors = Vec::new();
        for landed in g.commit(&refs, &[])? {
            cursors.push(landed?.cursor);
        }
        g.fill()?;
        Ok(cursors)
    }

    fn shape(cap: Option<usize>) -> Shape {
        Shape {
            name: "hub".into(),
            roots: Roots::Node {
                node: NodeRef::new("material", "m"),
            },
            steps: vec![
                Step {
                    cap,
                    relation: "uses<-".into(),
                    ..Step::default()
                },
                Step {
                    relation: "crafts".into(),
                    ..Step::default()
                },
            ],
        }
    }

    /// How many graph reads folding `bodies` in one batch costs `warm`.
    fn reads_to_fold(warm: &mut Warm, g: &mut Graph, bodies: &[String]) -> Result<u64> {
        let cursors = apply(g, bodies)?;
        let before = warm.reads;
        for (cursor, body) in cursors.into_iter().zip(bodies) {
            warm.fold(g, cursor, body);
        }
        warm.settle(g)?;
        Ok(warm.reads - before)
    }

    #[test]
    fn seeding_a_planet_sized_relay_reads_each_node_and_list_once() -> Result {
        let dir = tempfile::tempdir()?;
        let g = hub(&dir, 500)?;
        let warm = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
        assert_eq!(warm.set.nodes().len(), 1_001);
        // The root, its list; each schematic, its list; each item.
        assert_eq!(warm.reads, 1 + 1 + 500 + 500 + 500);
        Ok(())
    }

    #[test]
    fn a_walk_visits_every_node_only_when_something_it_holds_moved() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = hub(&dir, 500)?;
        let mut warm = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
        assert_eq!(warm.walked, 1_001);
        let mut visited = |bodies: &[String]| -> Result<u64> {
            let before = warm.walked;
            reads_to_fold(&mut warm, &mut g, bodies)?;
            Ok(warm.walked - before)
        };
        assert_eq!(visited(&[node("other", "x", "t")])?, 0, "no walk");
        assert_eq!(visited(&[node("schematic", "s0010", "new")])?, 1_001);
        let ended = link("ended", "crafts", ("schematic", "s0010"), ("item", "i0010"));
        assert_eq!(visited(&[ended])?, 1_000);
        let two = [
            node("schematic", "s0011", "a"),
            node("schematic", "s0012", "b"),
        ];
        assert_eq!(visited(&two)?, 1_000, "one walk for the batch");
        Ok(())
    }

    #[test]
    fn a_retried_fill_rewalks() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = hub(&dir, 10)?;
        let mut warm = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
        let seeded = warm.reads;
        let told = json!({"specversion": "1.0", "id": EventId::now().to_string(),
            "source": "rannoch", "type": "rannoch.refilled", "data": {"cursor": 1}})
        .to_string();
        assert_eq!(reads_to_fold(&mut warm, &mut g, &[told])?, seeded);
        Ok(())
    }

    #[test]
    fn equal_sets_are_equal_whether_or_not_their_index_is_built() -> Result {
        let dir = tempfile::tempdir()?;
        let g = hub(&dir, 10)?;
        let one = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
        let two = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
        assert_eq!(one.set, two.set);
        one.set.prepare();
        assert_eq!(one.set, two.set, "one index built");
        two.set.prepare();
        assert_eq!(one.set, two.set, "both built");
        Ok(())
    }

    #[test]
    fn an_index_tells_a_text_from_a_decimal_of_the_same_digits() {
        assert_ne!(
            value_text(&Value::Text("1.5".into())),
            value_text(&Value::Decimal("1.5".into()))
        );
    }

    #[test]
    fn by_answers_in_key_order() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = open(dir.path().join("graph"), &KEY)?;
        let declared = json!({"specversion": "1.0", "id": "d", "source": "rannoch:t",
                              "type": "rannoch.declared.x"})
        .to_string();
        let mut class = Class::new("a", ["title"]);
        class.indexed = BTreeSet::from(["title".to_string()]);
        g.declare(&Declaration::Class(class), &declared)?;
        let bodies: Vec<String> = (0..50)
            .rev()
            .map(|i| node("a", &format!("k{i:02}"), "same"))
            .collect();
        apply(&mut g, &bodies)?;
        let every = Shape {
            name: "all".into(),
            roots: Roots::Class {
                class: "a".into(),
                filter: std::collections::BTreeMap::new(),
            },
            steps: Vec::new(),
        };
        let warm = Warm::seed(&g, every, View::default(), Cursor::START)?;
        let found: Vec<&str> = warm
            .set
            .by("title", &Value::from("same"))
            .iter()
            .map(|n| n.key.as_str())
            .collect();
        let mut sorted = found.clone();
        sorted.sort_unstable();
        assert_eq!(found.len(), 50);
        assert_eq!(found, sorted);
        Ok(())
    }

    #[test]
    fn restoring_a_saved_relay_reads_nothing() -> Result {
        let dir = tempfile::tempdir()?;
        let g = hub(&dir, 50)?;
        let warm = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
        let restored = Warm::restored(&g, shape(None), View::default(), warm.saved())?;
        assert_eq!(restored.reads, 0);
        assert_eq!(restored.set, warm.set);
        Ok(())
    }

    #[test]
    fn an_applied_event_reads_only_what_it_named_at_any_size() -> Result {
        for size in [10, 500] {
            let dir = tempfile::tempdir()?;
            let mut g = hub(&dir, size)?;
            let mut warm = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
            let mut fold = |bodies: &[String]| reads_to_fold(&mut warm, &mut g, bodies);
            // Nothing it holds: no read.
            assert_eq!(fold(&[node("other", "x", "t")])?, 0);
            assert_eq!(
                fold(&[link("link", "beside", ("other", "x"), ("other", "x"))])?,
                0
            );
            // A node it holds, changed: that node, and the walk reads nothing it holds.
            assert_eq!(fold(&[node("schematic", "s0010", "new")])?, 1);
            // A link it holds, ended: that link.
            assert_eq!(
                fold(&[link(
                    "ended",
                    "crafts",
                    ("schematic", "s0010"),
                    ("item", "i0010")
                )])?,
                1
            );
            // A new schematic: nothing for its node, unheard; its link, then its version and list.
            let bodies = [
                node("schematic", "s9", "t"),
                link("link", "uses", ("schematic", "s9"), ("material", "m")),
            ];
            assert_eq!(fold(&bodies)?, 1 + 1 + 1);
        }
        Ok(())
    }

    #[test]
    fn boundary_churn_reads_no_list_again() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = hub(&dir, 8)?;
        let mut warm = Warm::seed(&g, shape(Some(5)), View::default(), Cursor::START)?;
        assert_eq!(warm.set.steps()[0].dropped, 3);
        // A key below the cut comes in: its link, its version and list. The old fifth goes on
        // what is held.
        let bodies = [
            node("schematic", "s0000", "t"),
            link("link", "uses", ("schematic", "s0000"), ("material", "m")),
        ];
        assert_eq!(reads_to_fold(&mut warm, &mut g, &bodies)?, 1 + 1 + 1);
        assert_eq!(warm.set.steps()[0].dropped, 4);
        assert_eq!(warm.set.steps()[0].nodes.len(), 5);
        // A kept link ends: its link. The next key, cut before, was held, and what it crafts was
        // read when it was taken earlier.
        let ended = [link(
            "ended",
            "uses",
            ("schematic", "s0000"),
            ("material", "m"),
        )];
        assert_eq!(reads_to_fold(&mut warm, &mut g, &ended)?, 1);
        assert_eq!(warm.set.steps()[0].dropped, 3);
        Ok(())
    }

    #[test]
    fn an_amendment_reads_everything_again() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = hub(&dir, 10)?;
        let mut warm = Warm::seed(&g, shape(None), View::default(), Cursor::START)?;
        let seeded = warm.reads;
        let amended = json!({"specversion": "1.0", "id": format!("{}:0", EventId::now()),
            "source": "rannoch:t", "type": "rannoch.amended",
            "data": {"target": {"source": "rannoch:t", "id": "none"}, "patch": {}}})
        .to_string();
        assert_eq!(reads_to_fold(&mut warm, &mut g, &[amended])?, seeded);
        Ok(())
    }
}
