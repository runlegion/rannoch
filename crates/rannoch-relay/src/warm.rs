//! One relay's warm set: what a walk of its shape reached in its view, and the fold that keeps it
//! true as events arrive. An event names what it touched; only that is read again from the graph,
//! and the walk runs again over what is held in memory.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

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
    nodes: BTreeMap<NodeRef, Arc<Version>>,
    roots: Vec<NodeRef>,
    steps: Vec<StepSet>,
    /// Each node in the set by a property its class indexes, and the value's text.
    indexed: HashMap<(String, String), Vec<NodeRef>>,
}

/// What one step of a walk took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepSet {
    /// The distinct nodes it took, in key order.
    pub nodes: Vec<NodeRef>,
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
    pub fn nodes(&self) -> &BTreeMap<NodeRef, Arc<Version>> {
        &self.held.nodes
    }

    /// The roots, in key order.
    #[must_use]
    pub fn roots(&self) -> &[NodeRef] {
        &self.held.roots
    }

    /// What each step took, in the shape's order.
    #[must_use]
    pub fn steps(&self) -> &[StepSet] {
        &self.held.steps
    }

    /// The nodes in the set whose class indexes `property` and whose value of it is `value`.
    #[must_use]
    pub fn by(&self, property: &str, value: &Value) -> &[NodeRef] {
        self.held
            .indexed
            .get(&(property.to_string(), value_text(value)))
            .map_or(&[], Vec::as_slice)
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

type Adjacent = (String, Direction, NodeRef);

/// A relay's fold: the walk's result, and every node and link list the walk has read.
#[derive(Debug)]
pub(crate) struct Warm {
    shape: Shape,
    view: View,
    pub(crate) set: WarmSet,
    /// The class of root nodes chosen by a filter, whose every node event may change them.
    roots: BTreeSet<NodeRef>,
    /// Every node the walk reached, cut ones too; `None` before its first version in the view.
    versions: HashMap<NodeRef, Option<Arc<Version>>>,
    /// The other end's key of every link that holds, per node, relation and direction read.
    adjacent: HashMap<Adjacent, BTreeSet<String>>,
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
    adjacent: Vec<(Adjacent, BTreeSet<String>)>,
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
            versions: HashMap::new(),
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
        warm.roots = saved.roots.into_iter().collect();
        warm.versions = saved
            .versions
            .into_iter()
            .map(|(node, version)| (node, version.map(Arc::new)))
            .collect();
        warm.adjacent = saved.adjacent.into_iter().collect();
        warm.moved = true;
        warm.settle(graph)?;
        Ok(warm)
    }

    /// What the relay's file holds.
    pub(crate) fn saved(&self) -> Saved {
        Saved {
            cursor: self.set.cursor,
            known: self.set.known,
            roots: self.roots.iter().cloned().collect(),
            versions: self
                .versions
                .iter()
                .map(|(node, version)| (node.clone(), version.as_deref().cloned()))
                .collect(),
            adjacent: self
                .adjacent
                .iter()
                .map(|(key, keys)| (key.clone(), keys.clone()))
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
                    .insert(NodeRef::new(node.class.clone(), node.key.clone()));
            }
            Roots::Class { class, filter } => {
                let listed = graph.nodes(class, filter, &self.view)?;
                for (key, version) in listed {
                    let node = NodeRef::new(class.clone(), key);
                    raise(&mut self.set.known, &version, self.view.known.is_some());
                    self.versions.insert(node.clone(), Some(Arc::new(version)));
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
                let node = NodeRef::new(class.clone(), key.clone());
                let root_class =
                    matches!(&self.shape.roots, Roots::Class { class: c, .. } if c == class);
                if root_class || self.versions.contains_key(&node) {
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
    fn read_node(&mut self, graph: &Graph, node: NodeRef) -> Result<(), Error> {
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
        for (direction, end, other) in [(Direction::Out, from, to), (Direction::In, to, from)] {
            let key = (relation.to_string(), direction, end.clone());
            if let Some(keys) = self.adjacent.get_mut(&key) {
                changed |= if holds {
                    keys.insert(other.key.clone())
                } else {
                    keys.remove(&other.key)
                };
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

    /// The links of `step`'s `relation` at `node`, read once.
    fn adjacent_keys(
        &mut self,
        graph: &Graph,
        relation: &str,
        direction: Direction,
        node: &NodeRef,
    ) -> Result<&BTreeSet<String>, Error> {
        let key = (relation.to_string(), direction, node.clone());
        if !self.adjacent.contains_key(&key) {
            let mut keys = BTreeSet::new();
            for (other, version) in graph.links(node, relation, direction, &self.view)? {
                raise(&mut self.set.known, &version, self.view.known.is_some());
                keys.insert(other);
            }
            self.adjacent.insert(key.clone(), keys);
        }
        Ok(&self.adjacent[&key])
    }

    /// `node`'s version in the view, read once.
    fn version(&mut self, graph: &Graph, node: &NodeRef) -> Result<Option<Arc<Version>>, Error> {
        if let Some(version) = self.versions.get(node) {
            return Ok(version.clone());
        }
        let version = graph.state_at(node, &self.view)?.map(Arc::new);
        if let Some(version) = &version {
            raise(&mut self.set.known, version, self.view.known.is_some());
        }
        self.versions.insert(node.clone(), version.clone());
        Ok(version)
    }

    /// Walks the shape over what is held, reading what is not. Inside a step, nodes are taken in
    /// key order; a node taken once is marked, so a cycle never repeats it. A node a cap cuts is
    /// not marked or expanded.
    fn walk(&mut self, graph: &Graph, ontology: &Ontology) -> Result<Held, Error> {
        let mut held = Held::default();
        let roots: Vec<NodeRef> = self.roots.iter().cloned().collect();
        for root in &roots {
            if let Some(version) = self.version(graph, root)? {
                held.nodes.insert(root.clone(), version);
                held.roots.push(root.clone());
            }
        }
        held.roots.sort_by(key_order);
        let mut frontier = held.roots.clone();
        for step in self.shape.steps.clone() {
            let (name, way) = step.way();
            let relation = ontology
                .relation(name)
                .map_err(|refused| Error::Refused(refused.0))?;
            let mut found = BTreeSet::new();
            for node in &frontier {
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
                    let keys: Vec<String> = self
                        .adjacent_keys(graph, name, direction, node)?
                        .iter()
                        .cloned()
                        .collect();
                    for key in keys {
                        let reached = NodeRef {
                            class: other.clone(),
                            key,
                        };
                        if held.nodes.contains_key(&reached) {
                            continue;
                        }
                        let passes = self
                            .version(graph, &reached)?
                            .is_some_and(|v| step.passes(&v.properties));
                        if passes {
                            found.insert((reached.key, reached.class));
                        }
                    }
                }
            }
            let cap = step.cap.unwrap_or(usize::MAX);
            let dropped = found.len().saturating_sub(cap);
            let taken: Vec<NodeRef> = found
                .into_iter()
                .take(cap)
                .map(|(key, class)| NodeRef { class, key })
                .collect();
            for node in &taken {
                if let Some(version) = self.version(graph, node)? {
                    held.nodes.insert(node.clone(), version);
                }
            }
            frontier.clone_from(&taken);
            held.steps.push(StepSet {
                nodes: taken,
                cut: dropped > 0,
                dropped,
            });
        }
        for (node, version) in &held.nodes {
            if let Ok(class) = ontology.class(&node.class) {
                for property in &class.indexed {
                    if let Some(value) = version.properties.get(property) {
                        held.indexed
                            .entry((property.clone(), value_text(value)))
                            .or_default()
                            .push(node.clone());
                    }
                }
            }
        }
        Ok(held)
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
        if change.is_some() || held != *self.set.held {
            self.set.held = Arc::new(held);
        }
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
        for (node, version) in &new.nodes {
            match old.get(node) {
                None => {
                    entered.push(json!({"node": node, "valid": self.moved_valid(node, version)}));
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
                                "node": node, "property": property,
                                "before": was, "after": is, "valid": version.valid,
                            }));
                        }
                    }
                }
            }
        }
        for (node, version) in old {
            if !new.nodes.contains_key(node) {
                left.push(json!({"node": node, "valid": self.moved_valid(node, version)}));
            }
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
    fn moved_valid(&self, node: &NodeRef, version: &Version) -> ValidTime {
        self.via
            .get(node)
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

fn key_order(a: &NodeRef, b: &NodeRef) -> std::cmp::Ordering {
    (&a.key, &a.class).cmp(&(&b.key, &b.class))
}
