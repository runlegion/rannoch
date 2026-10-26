//! Reads: find a node or a link, describe a node, walk from a root, count exactly.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::events::{Change, EventId, Fact, naming_key};
use crate::history::node_at;
use crate::open::{Graph, Ontology, Relation, Value, class_table, property_column, relation_table};
use crate::write::{Date, Error, ValidTime, node_id};

/// A node named by its class and natural key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeRef {
    pub class: String,
    pub key: String,
}

impl NodeRef {
    pub fn new(class: impl Into<String>, key: impl Into<String>) -> Self {
        NodeRef {
            class: class.into(),
            key: key.into(),
        }
    }
}

/// A node as the graph holds it now: where it came from, when it was true and when rannoch
/// recorded it. A property the node was not written with is absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub class: String,
    pub key: String,
    pub properties: BTreeMap<String, Value>,
    pub source: String,
    pub valid: ValidTime,
    pub recorded: SystemTime,
    /// The events the node was derived from.
    pub derived_from: Vec<EventId>,
}

/// A link still waiting for an end: its relation, both ends, and the end it waits on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpenLink {
    pub relation: String,
    pub from: NodeRef,
    pub to: NodeRef,
    /// The target it waits on, by kind and key.
    pub waits_on: NodeRef,
}

/// A link as the graph holds it now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub relation: String,
    pub from: NodeRef,
    pub to: NodeRef,
    pub source: String,
    pub valid: ValidTime,
    pub recorded: SystemTime,
}

/// A node in ontology terms: its class and key, its properties, its relations by name in both
/// directions with how many links each has, what it is part of, and what replaced it. It names
/// no table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    pub node: NodeRef,
    pub properties: BTreeMap<String, Value>,
    /// Links from the node, by relation: only relations it has a link of.
    pub relations_out: BTreeMap<String, u64>,
    /// Links to the node, by relation: only relations it has a link of.
    pub relations_in: BTreeMap<String, u64>,
    /// Its containment ancestors, nearest first.
    pub part_of: Vec<PartOf>,
    /// The node of the same class that replaced it, if one did.
    pub replaced_by: Option<NodeRef>,
}

/// A containment ancestor: the node something is part of, and the containment relation it is
/// part of it through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartOf {
    pub node: NodeRef,
    pub via: String,
}

/// What a walk reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Walk {
    /// The nodes reached, nearest first, at most `cap`; the root is not among them.
    pub nodes: Vec<(NodeRef, ValidTime)>,
    /// True when at least one more node within `hops` (and valid as of the date, for
    /// `walk_as_of`) would have been returned under a larger cap.
    pub capped: bool,
}

fn recorded(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(u64::try_from(millis).unwrap_or(0))
}

/// One step of a count's path: a relation, read backwards when written `name<-`, and an
/// optional filter on the nodes the step reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub via: String,
    pub filter: Option<Filter>,
}

impl Step {
    pub fn new(via: impl Into<String>) -> Self {
        Step {
            via: via.into(),
            filter: None,
        }
    }

    /// Keeps (`keep`) or leaves out (`!keep`) the nodes this step reaches that have a link of
    /// `via` to the count's root: `name` from the node to the root, `name<-` from the root to
    /// the node.
    #[must_use]
    pub fn filter(mut self, via: impl Into<String>, keep: bool) -> Self {
        self.filter = Some(Filter {
            via: via.into(),
            keep,
        });
        self
    }
}

/// A filter on the nodes a step reaches. See [`Step::filter`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub via: String,
    pub keep: bool,
}

/// Groups a count by a property of the first node of `class` on the path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBy {
    pub class: String,
    pub property: String,
}

/// An exact count from many roots of one class, along a path of relations. It counts the
/// nodes the path ends on. With `as_of`, every node and link the count reads must be valid on
/// or before that date; one whose valid time is unknown is never counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Count {
    pub class: String,
    pub roots: Vec<String>,
    pub path: Vec<Step>,
    pub group_by: Option<GroupBy>,
    pub as_of: Option<Date>,
}

/// One root's count: the distinct nodes the path ends on, the total of paths that reach them,
/// and both per group. A node without the grouped property counts under `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    pub root: String,
    pub distinct: u64,
    pub total: u64,
    pub groups: BTreeMap<Option<String>, (u64, u64)>,
}

/// A date's parts as `YYYY-MM-DD`: the form count's SQL compares valid times in.
fn day_text((year, month, day): (u16, u8, u8)) -> String {
    format!("{year:04}-{month:02}-{day:02}")
}

/// Whether `valid` is on or before `as_of`, a month standing for its first day. A stored date
/// that is not a real date (written before writes checked the range) is compared as count's SQL
/// compares it, so find, walk and count agree on it.
fn on_or_before(valid: &Date, as_of: (u16, u8, u8)) -> bool {
    if let Ok(parts) = valid.parts() {
        return parts <= as_of;
    }
    let text = valid.as_str();
    let text = if text.len() == 7 {
        format!("{text}-01")
    } else {
        text.to_string()
    };
    text <= day_text(as_of)
}

/// The SQL condition that `column`, a valid time, is on or before the count's `?2`, bound as
/// `YYYY-MM-DD`: a month (7 characters) compares as its first day. An unknown valid time is
/// NULL, and the condition on NULL is never true.
fn valid_by_sql(column: &str) -> String {
    format!("(CASE WHEN length({column}) = 7 THEN {column} || '-01' ELSE {column} END) <= ?2")
}

/// A relation named in a path, and whether it is read backwards (`name<-`).
fn parse<'o>(ontology: &'o Ontology, via: &str) -> Result<(&'o Relation, bool), Error> {
    match via.strip_suffix("<-") {
        Some(name) => Ok((ontology.relation(name)?, true)),
        None => Ok((ontology.relation(via)?, false)),
    }
}

fn find_in(
    conn: &Connection,
    ontology: &Ontology,
    class: &str,
    key: &str,
) -> Result<Option<Node>, Error> {
    let class = ontology.class(class)?;
    let Some((version, at)) = node_at(conn, class, key, i64::MAX)? else {
        return Ok(None);
    };
    let events: Vec<String> = conn
        .prepare_cached("SELECT event FROM _derived WHERE class = ?1 AND key = ?2 ORDER BY event")?
        .query_map(params![class.name, key], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(Some(Node {
        class: class.name.clone(),
        key: key.to_string(),
        properties: version.properties,
        source: version.source,
        valid: version.valid,
        recorded: recorded(at),
        derived_from: events
            .iter()
            .map(|event| serde_json::from_value(event.as_str().into()))
            .collect::<Result<_, _>>()?,
    }))
}

impl Graph {
    /// The node of `class` with natural key `key` as it is now: its newest version by its
    /// class's order (by point, or in an integer-keyed class by integer key and then point),
    /// ties going to the larger event id, never the version that arrived last.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn find(&self, class: &str, key: &str) -> Result<Option<Node>, Error> {
        let ontology = self.current_ontology()?;
        find_in(&self.conn, &ontology, class, key)
    }

    /// The node of `class` with natural key `key` as of `as_of`, read from its current
    /// version only: past versions are not readable yet.
    ///
    /// It answers with the current version when that version was valid on or before `as_of`,
    /// and with none when the node had no version by then: it was never written, or its only
    /// version is valid after `as_of` or has an unknown valid time. When the current version is
    /// valid after `as_of` or has an unknown valid time and the node had an earlier version, the
    /// version that held then may be a past one, so the read is refused rather than answered
    /// wrong. A month stands for its first day.
    ///
    /// # Errors
    /// [`Error::Refused`] when `as_of` is not a real date, or when the version that held on
    /// `as_of` may be a past one; the class is not declared, or the graph file cannot be read.
    pub fn find_as_of(&self, class: &str, key: &str, as_of: &Date) -> Result<Option<Node>, Error> {
        let parts = as_of.parts()?;
        let Some(node) = self.find(class, key)? else {
            return Ok(None);
        };
        match &node.valid {
            ValidTime::On(date) if on_or_before(date, parts) => Ok(Some(node)),
            _ if self.had_earlier_version(&node)? => Err(Error::Refused(format!(
                "{class} {key} as of {}: past versions are not readable yet",
                as_of.as_str()
            ))),
            _ => Ok(None),
        }
    }

    /// Whether the log holds a version of `node` other than its current one.
    fn had_earlier_version(&self, node: &Node) -> Result<bool, Error> {
        for (_, stored) in naming_key(&self.conn, &node.key)? {
            let Change::Wrote { source, added, .. } = stored.change else {
                continue;
            };
            if added.iter().any(|fact| {
                matches!(fact, Fact::Node(n) if n.class == node.class && n.key == node.key
                    && (source != node.source || n.valid != node.valid
                        || n.properties != node.properties))
            }) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The link of `relation` from the node keyed `from` to the node keyed `to`.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn find_link(&self, relation: &str, from: &str, to: &str) -> Result<Option<Link>, Error> {
        let ontology = self.current_ontology()?;
        let relation = ontology.relation(relation)?;
        let sql = format!(
            "SELECT r.source, r.valid, r.recorded FROM {} r \
             JOIN {} f ON f.id = r.src JOIN {} t ON t.id = r.dst \
             WHERE f.key = ?1 AND t.key = ?2",
            relation_table(&relation.name),
            class_table(&relation.from),
            class_table(&relation.to)
        );
        Ok(self
            .conn
            .prepare_cached(&sql)?
            .query_row(params![from, to], |row| {
                Ok(Link {
                    relation: relation.name.clone(),
                    from: NodeRef::new(&relation.from, from),
                    to: NodeRef::new(&relation.to, to),
                    source: row.get(0)?,
                    valid: ValidTime::from_sql(row.get(1)?),
                    recorded: recorded(row.get(2)?),
                })
            })
            .optional()?)
    }

    /// Every link still waiting for an end, each naming the kind and key of the target it
    /// waits on.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn open_links(&self) -> Result<Vec<OpenLink>, Error> {
        let tx = self.conn.unchecked_transaction()?;
        let ontology = self.current_ontology()?;
        let rows: Vec<(String, String, String)> = tx
            .prepare_cached(
                "SELECT rel, from_key, to_key FROM _open ORDER BY rel, from_key, to_key",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<_, _>>()?;
        let mut open = Vec::with_capacity(rows.len());
        for (relation, from_key, to_key) in rows {
            let relation = ontology.relation(&relation)?;
            let from = NodeRef::new(&relation.from, from_key);
            let to = NodeRef::new(&relation.to, to_key);
            let waits_on = if node_id(&tx, &to.class, &to.key)?.is_some() {
                from.clone()
            } else {
                to.clone()
            };
            open.push(OpenLink {
                relation: relation.name.clone(),
                from,
                to,
                waits_on,
            });
        }
        Ok(open)
    }

    /// Describes `node` in ontology terms: its class, its properties, its relations by name in
    /// both directions with counts, and its ancestors along the relations declared as
    /// containment, and the node that replaced it, if one did.
    ///
    /// # Errors
    /// [`Error::NotFound`] when the node is not in the graph; its class is not declared; the
    /// graph file cannot be read.
    pub fn describe(&self, node: &NodeRef) -> Result<Description, Error> {
        let tx = self.conn.unchecked_transaction()?;
        let ontology = self.current_ontology()?;
        let not_found = || Error::NotFound {
            class: node.class.clone(),
            key: node.key.clone(),
        };
        let found = find_in(&tx, &ontology, &node.class, &node.key)?.ok_or_else(not_found)?;
        let id = node_id(&tx, &node.class, &node.key)?.ok_or_else(not_found)?;
        let links = |relation: &Relation, end: &str| -> Result<u64, Error> {
            let sql = format!(
                "SELECT count(*) FROM {} WHERE {end} = ?1",
                relation_table(&relation.name)
            );
            let n: i64 = tx
                .prepare_cached(&sql)?
                .query_row(params![id], |row| row.get(0))?;
            Ok(u64::try_from(n).unwrap_or(0))
        };
        let (mut relations_out, mut relations_in) = (BTreeMap::new(), BTreeMap::new());
        for relation in ontology.relations.values() {
            for (class, end, counts) in [
                (&relation.from, "src", &mut relations_out),
                (&relation.to, "dst", &mut relations_in),
            ] {
                if *class == node.class {
                    let n = links(relation, end)?;
                    if n > 0 {
                        counts.insert(relation.name.clone(), n);
                    }
                }
            }
        }
        let mut seen: HashSet<(&str, i64)> = HashSet::from([(node.class.as_str(), id)]);
        let mut frontier: Vec<(&str, i64)> = vec![(node.class.as_str(), id)];
        let mut part_of = Vec::new();
        while !frontier.is_empty() {
            let mut next = Vec::new();
            for &(class, id) in &frontier {
                for relation in ontology.relations.values() {
                    if !relation.containment || relation.from != class {
                        continue;
                    }
                    let sql = format!(
                        "SELECT n.id, n.key FROM {} r JOIN {} n ON n.id = r.dst WHERE r.src = ?1",
                        relation_table(&relation.name),
                        class_table(&relation.to)
                    );
                    let mut statement = tx.prepare_cached(&sql)?;
                    let mut rows = statement.query(params![id])?;
                    while let Some(row) = rows.next()? {
                        let ancestor: (&str, i64) = (relation.to.as_str(), row.get(0)?);
                        if seen.insert(ancestor) {
                            part_of.push(PartOf {
                                node: NodeRef::new(&relation.to, row.get::<_, String>(1)?),
                                via: relation.name.clone(),
                            });
                            next.push(ancestor);
                        }
                    }
                }
            }
            frontier = next;
        }
        let replaced_by = tx
            .prepare_cached("SELECT replaced_by FROM _fates WHERE class = ?1 AND key = ?2")?
            .query_row(params![node.class, node.key], |row| row.get::<_, String>(0))
            .optional()?
            .map(|key| NodeRef::new(&node.class, key));
        Ok(Description {
            node: NodeRef::new(found.class, found.key),
            properties: found.properties,
            relations_out,
            relations_in,
            part_of,
            replaced_by,
        })
    }

    /// Walks from `root` along the relations in `via` (each read backwards when written
    /// `name<-`), for up to `hops` hops, and returns at most `cap` nodes, nearest first, each
    /// with its valid time: a node whose valid time is stated unknown is returned marked
    /// [`ValidTime::Unknown`]. The root itself is not among them. The walk says whether it
    /// stopped at its cap: [`Walk::capped`] is true when a larger cap would have returned
    /// more, so a caller never has to ask for `cap + 1` to tell.
    ///
    /// # Errors
    /// [`Error::NotFound`] when the root is not in the graph; the root's class or a relation in
    /// `via` is not declared; the graph file cannot be read.
    pub fn walk(
        &self,
        root: &NodeRef,
        via: &[&str],
        hops: usize,
        cap: usize,
    ) -> Result<Walk, Error> {
        self.walk_in(root, via, hops, cap, None)
    }

    /// [`Graph::walk`] as of `as_of`: it follows only links and reaches only nodes valid on or
    /// before that date, from a root valid by then. A node or link whose valid time is unknown
    /// is never followed as of a date. A month stands for its first day.
    ///
    /// # Errors
    /// [`Error::Refused`] when `as_of` is not a real date; otherwise as [`Graph::walk`].
    pub fn walk_as_of(
        &self,
        root: &NodeRef,
        via: &[&str],
        hops: usize,
        cap: usize,
        as_of: &Date,
    ) -> Result<Walk, Error> {
        self.walk_in(root, via, hops, cap, Some(as_of))
    }

    fn walk_in(
        &self,
        root: &NodeRef,
        via: &[&str],
        hops: usize,
        cap: usize,
        as_of: Option<&Date>,
    ) -> Result<Walk, Error> {
        let tx = self.conn.unchecked_transaction()?;
        let ontology = self.current_ontology()?;
        ontology.class(&root.class)?;
        let as_of: Option<(u16, u8, u8)> = as_of.map(Date::parts).transpose()?;
        let steps = via
            .iter()
            .map(|v| parse(&ontology, v))
            .collect::<Result<Vec<_>, _>>()?;
        let sql = format!(
            "SELECT id, valid FROM {} WHERE key = ?1",
            class_table(&root.class)
        );
        let (root_id, root_valid): (i64, Option<String>) = tx
            .prepare_cached(&sql)?
            .query_row(params![root.key], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?
            .ok_or_else(|| Error::NotFound {
                class: root.class.clone(),
                key: root.key.clone(),
            })?;
        let valid_by = |valid: Option<String>| {
            as_of.is_none_or(|as_of| {
                matches!(ValidTime::from_sql(valid), ValidTime::On(date) if on_or_before(&date, as_of))
            })
        };
        if !valid_by(root_valid) {
            return Ok(Walk {
                nodes: Vec::new(),
                capped: false,
            });
        }
        let mut seen: HashSet<(&str, i64)> = HashSet::from([(root.class.as_str(), root_id)]);
        let mut frontier: Vec<(&str, i64)> = vec![(root.class.as_str(), root_id)];
        let mut found: Vec<(NodeRef, ValidTime)> = Vec::new();
        let mut capped = false;
        'hops: for _ in 0..hops {
            if frontier.is_empty() {
                break;
            }
            let mut next = Vec::new();
            for &(class, id) in &frontier {
                for &(relation, backwards) in &steps {
                    let (here, there, near, far) = if backwards {
                        (&relation.to, &relation.from, "dst", "src")
                    } else {
                        (&relation.from, &relation.to, "src", "dst")
                    };
                    if here != class {
                        continue;
                    }
                    let sql = format!(
                        "SELECT n.id, n.key, r.valid, n.valid FROM {} r JOIN {} n ON n.id = r.{far} WHERE r.{near} = ?1",
                        relation_table(&relation.name),
                        class_table(there)
                    );
                    let mut statement = tx.prepare_cached(&sql)?;
                    let mut rows = statement.query(params![id])?;
                    while let Some(row) = rows.next()? {
                        let node_valid: Option<String> = row.get(3)?;
                        if !(valid_by(row.get(2)?) && valid_by(node_valid.clone())) {
                            continue;
                        }
                        let node: (&str, i64) = (there.as_str(), row.get(0)?);
                        if seen.insert(node) {
                            // Past the cap, one more node is all it takes to know the walk
                            // stopped short.
                            if found.len() >= cap {
                                capped = true;
                                break 'hops;
                            }
                            found.push((
                                NodeRef::new(there, row.get::<_, String>(1)?),
                                ValidTime::from_sql(node_valid),
                            ));
                            next.push(node);
                        }
                    }
                }
            }
            frontier = next;
        }
        Ok(Walk {
            nodes: found,
            capped,
        })
    }

    /// Counts exactly, in one pass, from every root: the distinct nodes the path ends on, the
    /// total, and both per group. Returns one tally per root, in the order given.
    ///
    /// # Errors
    /// [`Error::Refused`] when `as_of` is not a real date, a class, relation or property is not
    /// declared, or a relation on the path does not join the class the path has reached; the
    /// graph file cannot be read.
    pub fn count(&self, count: &Count) -> Result<Vec<Tally>, Error> {
        let as_of: Option<String> = count
            .as_of
            .as_ref()
            .map(|date| date.parts().map(day_text))
            .transpose()?;
        let tx = self.conn.unchecked_transaction()?;
        let ontology = self.current_ontology()?;
        let query = count_query(&ontology, count)?;
        let roots = serde_json::to_string(&count.roots)?;
        let mut statement = tx.prepare(&query)?;
        let mut rows = match &as_of {
            None => statement.query(params![roots])?,
            Some(date) => statement.query(params![roots, date])?,
        };
        let mut tallies: HashMap<String, Tally> = HashMap::new();
        let mut last: Option<(String, i64)> = None;
        while let Some(row) = rows.next()? {
            let root: String = row.get(0)?;
            let end: i64 = row.get(1)?;
            // A group is read as its value's text, whatever its property's declared type.
            let group = match row.get_ref(2)? {
                ValueRef::Null => None,
                ValueRef::Integer(n) => Some(n.to_string()),
                ValueRef::Real(f) => Some(f.to_string()),
                ValueRef::Text(_) | ValueRef::Blob(_) => Some(row.get::<_, String>(2)?),
            };
            let paths = u64::try_from(row.get::<_, i64>(3)?).unwrap_or(0);
            let tally = tallies.entry(root.clone()).or_insert_with(|| Tally {
                root: root.clone(),
                ..Tally::default()
            });
            let here = Some((root, end));
            if last != here {
                tally.distinct += 1;
                last = here;
            }
            tally.total += paths;
            let entry = tally.groups.entry(group).or_default();
            entry.0 += 1;
            entry.1 += paths;
        }
        Ok(count
            .roots
            .iter()
            .map(|root| {
                tallies.get(root).cloned().unwrap_or_else(|| Tally {
                    root: root.clone(),
                    ..Tally::default()
                })
            })
            .collect())
    }
}

/// The one query a count runs: a row per root, end node and group, with the number of paths.
fn count_query(ontology: &Ontology, count: &Count) -> Result<String, Error> {
    let root_class = ontology.class(&count.class)?;
    let mut joins = vec![format!("FROM {} n0", class_table(&root_class.name))];
    let mut conditions = vec!["n0.key IN (SELECT value FROM json_each(?1))".to_string()];
    // As of a date, every node and link the count reads is valid by then (`?2`, `YYYY-MM-DD`);
    // see `valid_by_sql`.
    let as_of = count.as_of.is_some();
    if as_of {
        conditions.push(valid_by_sql("n0.valid"));
    }
    let mut at: Vec<(&str, String)> = vec![(root_class.name.as_str(), "n0.id".to_string())];
    for (i, step) in count.path.iter().enumerate() {
        let (class, node) = at[i].clone();
        let (relation, backwards) = parse(ontology, &step.via)?;
        let (here, there, near, far) = if backwards {
            (&relation.to, &relation.from, "dst", "src")
        } else {
            (&relation.from, &relation.to, "src", "dst")
        };
        if here != class {
            return Err(Error::Refused(format!(
                "relation {} does not reach class {class} as {}",
                relation.name, step.via
            )));
        }
        let r = format!("r{}", i + 1);
        joins.push(format!(
            "JOIN {} {r} ON {r}.{near} = {node}",
            relation_table(&relation.name)
        ));
        let reached = format!("{r}.{far}");
        if as_of {
            let n = format!("n{}", i + 1);
            joins.push(format!(
                "JOIN {} {n} ON {n}.id = {reached}",
                class_table(there)
            ));
            conditions.push(format!(
                "{} AND {}",
                valid_by_sql(&format!("{r}.valid")),
                valid_by_sql(&format!("{n}.valid"))
            ));
        }
        if let Some(filter) = &step.filter {
            conditions.push(filter_condition(
                ontology,
                filter,
                there,
                &root_class.name,
                &reached,
                i + 1,
                as_of,
            )?);
        }
        at.push((there.as_str(), reached));
    }
    let group = match &count.group_by {
        None => "NULL".to_string(),
        Some(group) => {
            let class = ontology.class(&group.class)?;
            if !class.properties.contains_key(&group.property) {
                return Err(Error::Refused(format!(
                    "class {} has no property {}",
                    class.name, group.property
                )));
            }
            let Some(position) = at.iter().position(|(c, _)| *c == class.name) else {
                return Err(Error::Refused(format!(
                    "class {} is not on the path",
                    class.name
                )));
            };
            joins.push(format!(
                "JOIN {} g ON g.id = {}",
                class_table(&class.name),
                at[position].1
            ));
            format!("g.{}", property_column(&group.property))
        }
    };
    let end = &at[at.len() - 1].1;
    Ok(format!(
        "SELECT n0.key, {end}, {group}, count(*) {} WHERE {} \
         GROUP BY n0.id, {end}, {group} ORDER BY n0.id, {end}",
        joins.join(" "),
        conditions.join(" AND ")
    ))
}

/// The condition a step's filter puts on the node it reached, `reached`, against the root.
fn filter_condition(
    ontology: &Ontology,
    filter: &Filter,
    class: &str,
    root_class: &str,
    reached: &str,
    step: usize,
    as_of: bool,
) -> Result<String, Error> {
    let (by, backwards) = parse(ontology, &filter.via)?;
    let (node_end, root_end, node_col, root_col) = if backwards {
        (&by.to, &by.from, "dst", "src")
    } else {
        (&by.from, &by.to, "src", "dst")
    };
    if node_end != class || root_end != root_class {
        return Err(Error::Refused(format!(
            "relation {} does not join class {class} and class {root_class} as {}",
            by.name, filter.via
        )));
    }
    let f = format!("f{step}");
    Ok(format!(
        "{}EXISTS (SELECT 1 FROM {} {f} WHERE {f}.{node_col} = {reached} AND {f}.{root_col} = n0.id{})",
        if filter.keep { "" } else { "NOT " },
        relation_table(&by.name),
        if as_of {
            format!(" AND {}", valid_by_sql(&format!("{f}.valid")))
        } else {
            String::new()
        }
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        Class, Count, Date, Declaration, Error, Fate, Graph, GroupBy, NodeRef, Relation, Step,
        Unit, ValidTime, Write, open,
    };

    const NONE: [(&str, &str); 0] = [];

    fn temp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()))
    }

    fn late(key: &str) -> Unit {
        Unit::new("late", key, NONE, ValidTime::On(Date::day(2026, 1, 1)))
    }

    #[test]
    fn a_handle_reads_a_class_declared_after_it_opened() -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("late");
        let mut a = open(&path, Some(&crate::TEST_KEY))?;
        let b = open(&path, Some(&crate::TEST_KEY))?;
        a.declare(&Declaration::Class(Class::new("late", [] as [&str; 0])))?;
        a.write(&Write::new("a").unit(late("k")))?;

        let found = b.find("late", "k")?;
        assert_eq!(found.map(|n| n.key), Some("k".to_string()));
        let walked = b.walk(&NodeRef::new("late", "k"), &[], 1, 10);
        assert!(walked.is_ok(), "expected the walk to run, got {walked:?}");
        assert_eq!(b.describe(&NodeRef::new("late", "k"))?.node.class, "late");
        Ok(())
    }

    #[test]
    fn a_handle_counts_by_a_property_added_after_it_opened()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("grown");
        let mut a = open(&path, Some(&crate::TEST_KEY))?;
        a.declare(&Declaration::Class(Class::new("voice", ["name"])))?;
        let b = open(&path, Some(&crate::TEST_KEY))?;
        a.declare(&Declaration::Property {
            class: "voice".to_string(),
            property: "region".to_string(),
            of: crate::ValueType::Text,
        })?;
        let when = ValidTime::On(Date::day(2026, 1, 1));
        a.write(&Write::new("a").unit(Unit::new(
            "voice",
            "v1",
            [("name", "Ana"), ("region", "north")],
            when,
        )))?;

        let tallies = b.count(&Count {
            class: "voice".to_string(),
            roots: vec!["v1".to_string()],
            path: Vec::new(),
            group_by: Some(GroupBy {
                class: "voice".to_string(),
                property: "region".to_string(),
            }),
            as_of: None,
        })?;
        assert_eq!(tallies.len(), 1);
        assert_eq!(
            tallies[0].groups,
            BTreeMap::from([(Some("north".to_string()), (1, 1))])
        );
        Ok(())
    }

    #[test]
    fn a_count_groups_by_an_integer_property() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = open(temp("integer-groups"), Some(&crate::TEST_KEY))?;
        let mut voice = Class::new("voice", NONE.map(|(p, _)| p));
        voice
            .properties
            .insert("age".to_string(), crate::ValueType::Integer);
        g.declare(&Declaration::Class(voice))?;
        g.declare(&Declaration::Class(Class::new(
            "finding",
            NONE.map(|(p, _)| p),
        )))?;
        g.declare(&Declaration::Relation(Relation::new(
            "supports", "voice", "finding", false,
        )))?;
        let mut write = Write::new("a").unit(Unit::new("finding", "f1", NONE, ValidTime::Unknown));
        for (key, age) in [("v1", 30), ("v2", 30), ("v3", 41)] {
            write = write.unit(
                Unit::new(
                    "voice",
                    key,
                    [("age", crate::Value::Integer(age))],
                    ValidTime::Unknown,
                )
                .link("supports", key, "f1", ValidTime::Unknown),
            );
        }
        g.write(&write)?;

        let tallies = g.count(&Count {
            class: "finding".to_string(),
            roots: vec!["f1".to_string()],
            path: vec![Step::new("supports<-")],
            group_by: Some(GroupBy {
                class: "voice".to_string(),
                property: "age".to_string(),
            }),
            as_of: None,
        })?;
        assert_eq!(
            tallies[0].groups,
            BTreeMap::from([
                (Some("30".to_string()), (2, 2)),
                (Some("41".to_string()), (1, 1)),
            ])
        );
        Ok(())
    }

    #[test]
    fn a_mount_resolves_a_class_declared_after_it_mounted() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = temp("mounted");
        let mut a = open(&path, Some(&crate::TEST_KEY))?;
        let mut app = open(temp("app"), Some(&crate::TEST_KEY))?;
        app.mount("other", &path)?;
        a.declare(&Declaration::Class(Class::new("late", [] as [&str; 0])))?;
        a.write(&Write::new("a").unit(late("k")))?;

        assert_eq!(
            app.resolve("other", &NodeRef::new("late", "k"))?,
            Fate::Live
        );
        Ok(())
    }

    #[test]
    fn a_known_class_still_reads_after_another_handle_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("known");
        let mut a = open(&path, Some(&crate::TEST_KEY))?;
        a.declare(&Declaration::Class(Class::new("late", [] as [&str; 0])))?;
        a.declare(&Declaration::Class(Class::new("other", [] as [&str; 0])))?;
        a.write(&Write::new("a").unit(late("k")))?;
        let b = open(&path, Some(&crate::TEST_KEY))?;
        assert!(b.find("late", "k")?.is_some());

        a.write(&Write::new("a").unit(Unit::new("other", "o", NONE, ValidTime::Unknown)))?;
        assert_eq!(b.find("late", "k")?.map(|n| n.key), Some("k".to_string()));
        Ok(())
    }

    #[test]
    fn a_class_never_declared_is_still_refused() -> Result<(), Error> {
        let path = temp("ghost");
        let refused = open(path, Some(&crate::TEST_KEY))?.find("ghost", "k");
        assert!(
            matches!(&refused, Err(Error::Refused(reason)) if reason == "class ghost is not declared"),
            "expected a refusal, got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn walk_from_an_undeclared_class_is_refused_in_ontology_terms() -> Result<(), Error> {
        let path = std::env::temp_dir().join(format!("rannoch-walk-{}.db", uuid::Uuid::now_v7()));
        let refused =
            open(path, Some(&crate::TEST_KEY))?.walk(&NodeRef::new("ghost", "k"), &[], 1, 10);
        assert!(
            matches!(&refused, Err(Error::Refused(reason)) if reason == "class ghost is not declared"),
            "expected a refusal, got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_walk_asked_for_every_hop_ends_when_nothing_is_left()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-hops-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("crate", ["name"])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "depends_on",
            "crate",
            "crate",
            false,
        )))?;
        let when = ValidTime::On(Date::day(2026, 1, 1));
        let unit = |key: &str| Unit::new("crate", key, [("name", key)], when.clone());
        g.write(&Write::new("registry").unit(unit("a")).unit(unit("b").link(
            "depends_on",
            "b",
            "a",
            when.clone(),
        )))?;
        let dependents = g.walk(
            &NodeRef::new("crate", "a"),
            &["depends_on<-"],
            usize::MAX,
            10,
        )?;
        assert_eq!(dependents.nodes.len(), 1);
        assert!(!dependents.capped);
        Ok(())
    }

    /// A graph of crates where `root` has one `depends_on` link to each of `neighbours`, each
    /// link and neighbour valid on its own date.
    fn star(neighbours: &[(&str, Date)]) -> Result<crate::Graph, Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-cap-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("crate", ["name"])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "depends_on",
            "crate",
            "crate",
            false,
        )))?;
        let root_when = ValidTime::On(Date::day(2026, 1, 1));
        let mut root = Unit::new("crate", "root", [("name", "root")], root_when);
        let mut write = Write::new("registry");
        for (key, date) in neighbours {
            let when = ValidTime::On(date.clone());
            root = root.link("depends_on", "root", *key, when.clone());
            write = write.unit(Unit::new("crate", *key, [("name", *key)], when));
        }
        g.write(&write.unit(root))?;
        Ok(g)
    }

    fn early() -> Date {
        Date::day(2026, 1, 1)
    }

    #[test]
    fn a_walk_that_reaches_exactly_its_cap_is_not_capped() -> Result<(), Box<dyn std::error::Error>>
    {
        let g = star(&[("a", early()), ("b", early()), ("c", early())])?;
        let walk = g.walk(&NodeRef::new("crate", "root"), &["depends_on"], 1, 3)?;
        assert_eq!(walk.nodes.len(), 3);
        assert!(!walk.capped);
        Ok(())
    }

    #[test]
    fn a_walk_with_one_more_node_than_its_cap_is_capped() -> Result<(), Box<dyn std::error::Error>>
    {
        let g = star(&[
            ("a", early()),
            ("b", early()),
            ("c", early()),
            ("d", early()),
        ])?;
        let walk = g.walk(&NodeRef::new("crate", "root"), &["depends_on"], 1, 3)?;
        assert_eq!(walk.nodes.len(), 3);
        assert!(walk.capped);
        Ok(())
    }

    #[test]
    fn a_walk_capped_at_zero_says_whether_anything_was_reachable()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = NodeRef::new("crate", "root");
        let one = star(&[("a", early())])?.walk(&root, &["depends_on"], 1, 0)?;
        assert!(one.nodes.is_empty());
        assert!(one.capped);
        let no_hops = star(&[("a", early())])?.walk(&root, &["depends_on"], 0, 0)?;
        assert!(no_hops.nodes.is_empty());
        assert!(!no_hops.capped);
        let none = star(&[])?.walk(&root, &["depends_on"], 1, 0)?;
        assert!(none.nodes.is_empty());
        assert!(!none.capped);
        Ok(())
    }

    #[test]
    fn a_walk_as_of_does_not_count_a_node_valid_after_the_date_toward_its_cap()
    -> Result<(), Box<dyn std::error::Error>> {
        let late = Date::day(2026, 6, 1);
        let g = star(&[("a", early()), ("b", early()), ("c", early()), ("d", late)])?;
        let walk = g.walk_as_of(
            &NodeRef::new("crate", "root"),
            &["depends_on"],
            1,
            3,
            &Date::day(2026, 3, 1),
        )?;
        assert_eq!(walk.nodes.len(), 3);
        assert!(!walk.capped);
        Ok(())
    }

    /// A graph of items, each `part` of another and `flags` another, holding the units given.
    fn items(units: Vec<Unit>) -> Result<Graph, Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-asof-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("item", ["name"])))?;
        for relation in ["part", "flags"] {
            g.declare(&Declaration::Relation(Relation::new(
                relation, "item", "item", false,
            )))?;
        }
        g.write(&units.into_iter().fold(Write::new("src"), Write::unit))?;
        Ok(g)
    }

    fn item(key: &str, valid: &Date) -> Unit {
        Unit::new("item", key, [("name", key)], ValidTime::On(valid.clone()))
    }

    fn refused(result: &Result<impl std::fmt::Debug, Error>) -> bool {
        matches!(result, Err(Error::Refused(why))
            if why == "valid time 2020-13 is not a date: month 13 is out of range")
    }

    #[test]
    fn an_as_of_read_given_an_out_of_range_date_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let jan = Date::day(2020, 1, 1);
        let g = items(vec![item("r", &jan)])?;
        let bad = Date::month(2020, 13);
        let found = g.find_as_of("item", "r", &bad);
        assert!(refused(&found), "find_as_of: {found:?}");
        let walked = g.walk_as_of(&NodeRef::new("item", "r"), &["part<-"], 1, 10, &bad);
        assert!(refused(&walked), "walk_as_of: {walked:?}");
        let counted = g.count(&Count {
            class: "item".to_string(),
            roots: vec!["r".to_string()],
            path: vec![Step::new("part<-")],
            group_by: None,
            as_of: Some(bad),
        });
        assert!(refused(&counted), "count: {counted:?}");
        Ok(())
    }

    #[test]
    fn a_node_valid_on_the_first_of_a_month_is_read_as_of_that_month()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = Date::day(2020, 1, 1);
        let when = ValidTime::On(first.clone());
        let g = items(vec![
            item("r", &first),
            item("n", &first)
                .link("part", "n", "r", when.clone())
                .link("flags", "n", "r", when),
        ])?;
        let month = Date::month(2020, 1);
        assert!(g.find_as_of("item", "n", &month)?.is_some());
        let walked = g.walk_as_of(&NodeRef::new("item", "r"), &["part<-"], 1, 10, &month)?;
        assert_eq!(
            walked.nodes,
            vec![(NodeRef::new("item", "n"), ValidTime::On(first))]
        );
        let tallies = g.count(&Count {
            class: "item".to_string(),
            roots: vec!["r".to_string()],
            path: vec![Step::new("part<-").filter("flags", true)],
            group_by: None,
            as_of: Some(month),
        })?;
        assert_eq!(tallies[0].distinct, 1);
        Ok(())
    }

    #[test]
    fn an_as_of_read_refuses_when_the_version_that_held_is_a_past_one()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = items(vec![item("v", &Date::day(2020, 1, 1))])?;
        g.write(&Write::new("src").unit(item("v", &Date::day(2022, 1, 1))))?;
        let found = g.find_as_of("item", "v", &Date::day(2021, 1, 1));
        assert!(
            matches!(&found, Err(Error::Refused(why))
                if why == "item v as of 2021-01-01: past versions are not readable yet"),
            "find_as_of: {found:?}"
        );
        assert!(g.find_as_of("item", "v", &Date::day(2022, 1, 1))?.is_some());
        Ok(())
    }

    #[test]
    fn an_as_of_read_refuses_when_the_current_version_has_no_date_and_an_earlier_one_held()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = items(vec![item("u", &Date::day(2020, 1, 1))])?;
        g.write(&Write::new("src").unit(Unit::new(
            "item",
            "u",
            [("name", "u")],
            ValidTime::Unknown,
        )))?;
        let found = g.find_as_of("item", "u", &Date::day(2021, 1, 1));
        assert!(
            matches!(&found, Err(Error::Refused(why))
                if why == "item u as of 2021-01-01: past versions are not readable yet"),
            "find_as_of: {found:?}"
        );
        let only = items(vec![Unit::new(
            "item",
            "w",
            [("name", "w")],
            ValidTime::Unknown,
        )])?;
        assert!(
            only.find_as_of("item", "w", &Date::day(2021, 1, 1))?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn an_as_of_read_of_a_node_with_one_version_answers_it_or_none()
    -> Result<(), Box<dyn std::error::Error>> {
        let g = items(vec![item("o", &Date::day(2020, 6, 1))])?;
        let found = g.find_as_of("item", "o", &Date::day(2020, 6, 1))?;
        assert_eq!(
            found.map(|n| n.valid),
            Some(ValidTime::On(Date::day(2020, 6, 1)))
        );
        assert!(
            g.find_as_of("item", "o", &Date::day(2020, 5, 31))?
                .is_none()
        );
        assert!(
            g.find_as_of("item", "absent", &Date::day(2020, 6, 1))?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn a_month_is_read_as_its_first_day() -> Result<(), Box<dyn std::error::Error>> {
        let g = items(vec![
            item("m", &Date::month(2020, 1)),
            item("d", &Date::day(2020, 1, 15)),
        ])?;
        assert!(g.find_as_of("item", "m", &Date::day(2020, 1, 1))?.is_some());
        assert!(g.find_as_of("item", "d", &Date::month(2020, 1))?.is_none());
        Ok(())
    }
}
