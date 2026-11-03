//! One relay's warm set: what a walk of its shape reached in its view, and the fold that keeps it
//! true as events arrive. An event names what it touched; only that is read again from the graph,
//! and the walk runs again over what is held in memory.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, OnceLock};

use rannoch_cloudevents::{Conclusion, Fact, LinkFact, NodeFact, NodeRef, ValidTime};
use rannoch_store::{Cursor, Direction, Graph, Ontology, Roots, Shape, Value, Version, View, Way};
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

#[derive(Debug, Default, PartialEq, Eq)]
struct Held {
    nodes: HashMap<Id, Arc<Version>>,
    roots: Vec<Id>,
    steps: Vec<StepSet>,
    ontology: Arc<Ontology>,
    /// Each node in the set by a property its class indexes, and the value's text. Built once per
    /// set, after the set is published and its change told.
    indexed: OnceLock<HashMap<String, HashMap<String, Vec<Id>>>>,
}

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
            indexed
        })
    }
}

/// A node's name, shared by everything that holds it, so walking copies no text.
type Id = Arc<NodeRef>;

/// A quick hash for the walk, which hashes every name several times over. Names are the app's own
/// and stay in this process, so nothing here needs to resist a chosen key.
#[derive(Default)]
struct Quick(u64);

impl Hasher for Quick {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.0 = (self.0.rotate_left(5) ^ u64::from_le_bytes(word))
                .wrapping_mul(0x517c_c1b7_2722_0a95);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type Fast = BuildHasherDefault<Quick>;

/// What one step of a walk took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepSet {
    /// The distinct nodes it took, in key order.
    pub nodes: Vec<Arc<NodeRef>>,
    /// Whether its cap cut anything.
    pub cut: bool,
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
    #[must_use]
    pub fn known(&self) -> Option<i64> {
        self.known
    }

    /// Every node in the set, roots included, each once, with its version in the relay's view.
    #[must_use]
    pub fn nodes(&self) -> &HashMap<Arc<NodeRef>, Arc<Version>> {
        &self.held.nodes
    }

    /// The roots, in key order.
    #[must_use]
    pub fn roots(&self) -> &[Arc<NodeRef>] {
        &self.held.roots
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
                .map(|step| json!({"nodes": step.nodes.len(), "cut": step.cut, "dropped": step.dropped}))
                .collect(),
        )
    }
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Text(text) | Value::Decimal(text) => text.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// The other ends of the links that hold, per node, for one relation and direction.
type Ends = HashMap<Id, BTreeSet<Id>, Fast>;

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
    pub(crate) set: WarmSet,
    /// The class of root nodes chosen by a filter, whose every node event may change them.
    roots: BTreeSet<Id>,
    /// Every node the walk reached, cut ones too; `None` before its first version in the view.
    versions: HashMap<Id, Option<Arc<Version>>, Fast>,
    /// Every list of links the walk has read, by relation.
    adjacent: HashMap<String, Read>,
    /// The latest valid time an event in this batch gave a link, per end.
    via: HashMap<NodeRef, ValidTime>,
    last_via: Option<ValidTime>,
    /// The walk must run again.
    moved: bool,
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
        let known = view.known.map(crate::millis);
        Warm {
            shape,
            view,
            set: WarmSet {
                cursor,
                known,
                held: Arc::default(),
            },
            roots: BTreeSet::new(),
            versions: HashMap::default(),
            adjacent: HashMap::new(),
            via: HashMap::new(),
            last_via: None,
            moved: false,
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
                self.roots
                    .insert(Arc::new(NodeRef::new(node.class.clone(), node.key.clone())));
            }
            Roots::Class { class, filter } => {
                let listed = graph.nodes(class, filter, &self.view)?;
                for (key, version) in listed {
                    let node = Arc::new(NodeRef::new(class.clone(), key));
                    raise(&mut self.set.known, &version, self.view.known.is_some());
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

    /// Folds one event. An event of nothing this relay holds costs its parse.
    pub(crate) fn fold(&mut self, graph: &Graph, cursor: Cursor, body: &str) {
        self.set.cursor = cursor;
        if self.stale {
            return;
        }
        let Ok(attributes) = rannoch_cloudevents::attributes(body) else {
            return;
        };
        if attributes.ty == "rannoch.amended" {
            self.stale = true;
            return;
        }
        let Ok(fact) = rannoch_cloudevents::read(body) else {
            return;
        };
        if self.touch(graph, &fact).is_err() {
            self.stale = true;
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
        let version = graph.state_at(&node, &self.view)?;
        if version.is_none() && graph.state_at(&node, &View::default())?.is_none() {
            self.stale = true;
            return Ok(());
        }
        let root = match &self.shape.roots {
            Roots::Class { class, filter } if *class == node.class => {
                Some(version.as_ref().is_some_and(|v| {
                    filter
                        .iter()
                        .all(|(p, value)| v.properties.get(p) == Some(value))
                }))
            }
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
            raise(&mut self.set.known, version, self.view.known.is_some());
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
        let version = graph.link_state_at(relation, &from.key, &to.key, &self.view)?;
        if version.is_none()
            && graph
                .link_state_at(relation, &from.key, &to.key, &View::default())?
                .is_none()
        {
            self.stale = true;
            return Ok(());
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
            raise(&mut self.set.known, &version, self.view.known.is_some());
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
        let mut visited: HashSet<Id, Fast> =
            HashSet::with_capacity_and_hasher(size, Fast::default());
        for root in &self.roots {
            if let Some(version) = version_of(
                graph,
                &self.view,
                &mut self.versions,
                &mut self.set.known,
                root,
            )? {
                held.nodes.insert(Arc::clone(root), version);
                held.roots.push(Arc::clone(root));
                visited.insert(Arc::clone(root));
            }
        }
        held.roots.sort_by(key_order);
        let mut frontier = held.roots.clone();
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
        visited: &mut HashSet<Id, Fast>,
        held: &mut Held,
    ) -> Result<Vec<Id>, Error> {
        let fixed = self.view.known.is_some();
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
                    for (key, version) in graph.links(node, name, direction, &self.view)? {
                        raise(&mut self.set.known, &version, fixed);
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
                    let version = version_of(
                        graph,
                        &self.view,
                        &mut self.versions,
                        &mut self.set.known,
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
        let found = taken;
        held.steps.push(StepSet {
            nodes: found.clone(),
            cut: dropped > 0,
            dropped,
        });
        Ok(found)
    }

    /// Brings the set up to what was folded, and returns the deltas as a change's data, or `None`
    /// when nothing in the set changed.
    pub(crate) fn settle(&mut self, graph: &Graph) -> Result<Option<Json>, Error> {
        if self.stale {
            self.reseed(graph)?;
        }
        if !self.moved {
            return Ok(None);
        }
        let ontology = graph.current_ontology()?;
        let held = self.walk(graph, &ontology)?;
        let change = self.delta(&held);
        self.set.held = Arc::new(held);
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
                Some(before) if Arc::ptr_eq(before, version) || before == version => {}
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
            "cursor": self.set.cursor,
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
fn raise(known: &mut Option<i64>, version: &Version, fixed: bool) {
    if !fixed {
        let at = crate::millis(version.known);
        if known.is_none_or(|k| k < at) {
            *known = Some(at);
        }
    }
}

/// `node`'s version in `view`, read once.
fn version_of(
    graph: &Graph,
    view: &View,
    versions: &mut HashMap<Id, Option<Arc<Version>>, Fast>,
    known: &mut Option<i64>,
    node: &Id,
) -> Result<Option<Arc<Version>>, Error> {
    if let Some(version) = versions.get(node) {
        return Ok(version.clone());
    }
    let version = graph.state_at(node, view)?.map(Arc::new);
    if let Some(version) = &version {
        raise(known, version, view.known.is_some());
    }
    versions.insert(Arc::clone(node), version.clone());
    Ok(version)
}

fn key_order(a: &Id, b: &Id) -> std::cmp::Ordering {
    (&a.key, &a.class).cmp(&(&b.key, &b.class))
}
