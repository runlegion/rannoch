//! Reads: find a node or a link, describe a node, walk from a root, count exactly.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::events::EventId;
use crate::history::{
    At, Version, known_bound, link_as_of, link_entity, node_as_of, node_at, node_entity,
    unknown_ending, valid_day,
};
use crate::open::{
    Class, Graph, Ontology, Relation, Value, class_table, property_column, relation_table,
};
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
/// nodes the path ends on. With `as_of`, a valid time, it reads every node and link on the
/// path, and the grouped property, as the version true then (see [`Graph::find_as_of`]): one
/// with no version valid by then is never counted. A link whose ending is valid on or before
/// that date is not followed, and a count that would follow a link whose ending's valid time is
/// unknown is refused. With `known_at` as well, it reads them as the store knew them at that
/// point, each on its class's axis; a link is read at a time only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Count {
    pub class: String,
    pub roots: Vec<String>,
    pub path: Vec<Step>,
    pub group_by: Option<GroupBy>,
    pub as_of: Option<Date>,
    pub known_at: Option<At>,
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

/// The SQL condition that `column`, a valid time, is on or before the count's `?2`, bound as
/// `YYYY-MM-DD`; see [`valid_day`]. The condition on an unknown valid time is never true.
fn valid_by_sql(column: &str) -> String {
    format!("{} <= ?2", valid_day(column))
}

/// The SQL condition that the node or link of `entity` keyed `key` (and `to`, `NULL` for a
/// node) has a version true at the count's `?2`, heard by its `?3` on the axis `known` names.
fn held_sql(entity: &str, (key, to): (&str, &str), known: Option<&str>) -> String {
    let bound = known.map_or(String::new(), |column| format!(" AND h.{column} <= ?3"));
    format!(
        "EXISTS (SELECT 1 FROM _hearings h WHERE h.entity = {} AND h.key = {key} \
         AND h.to_key IS {to} AND {}{bound})",
        literal(entity),
        valid_by_sql("h.valid")
    )
}

/// [`held_sql`] for the node `n` (a table alias) of `class`.
fn node_held_sql(class: &Class, n: &str, known: Option<&str>) -> String {
    held_sql(
        &node_entity(&class.name),
        (&format!("{n}.key"), "NULL"),
        known,
    )
}

/// `text` as an SQL string literal.
fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
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
    node_in(conn, class, key, node_at(conn, class, key, i64::MAX)?)
}

/// The node keyed `key` in `class` as its `version`, recorded at `at`, holds it.
fn node_in(
    conn: &Connection,
    class: &Class,
    key: &str,
    version: Option<(Version, i64)>,
) -> Result<Option<Node>, Error> {
    let Some((version, at)) = version else {
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

    /// The node of `class` with natural key `key` as of the valid time `as_of`: the version
    /// true then, as the store knows it now or, given `known_at`, as it knew it at that point on
    /// the class's axis (a time, or in an integer-keyed class an integer key). Among the
    /// versions valid on or before `as_of` (and heard at or before `known_at`), the greatest by
    /// valid time wins, then by the class's order. A version whose valid time is unknown is never
    /// returned; `None` when no version was valid by then. A month stands for its first day.
    ///
    /// # Errors
    /// [`Error::Refused`] when `as_of` is not a real date, or `known_at` is not on the class's
    /// axis; the class is not declared, or the graph file cannot be read.
    pub fn find_as_of(
        &self,
        class: &str,
        key: &str,
        as_of: &Date,
        known_at: Option<At>,
    ) -> Result<Option<Node>, Error> {
        let day = day_text(as_of.parts()?);
        let ontology = self.current_ontology()?;
        let class = ontology.class(class)?;
        node_in(
            &self.conn,
            class,
            key,
            node_as_of(&self.conn, class, key, &day, known_at)?,
        )
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

    /// [`Graph::walk`] as of the valid time `as_of`, as the store knows it now or, given
    /// `known_at`, as it knew it at that time: from a root with a version true then, it follows
    /// each link with a version true then and not ended by then, and reaches each node with a
    /// version true then, returned with that version's valid time (see [`Graph::find_as_of`]).
    /// A node or link with no version valid by then is never followed. A month stands for its
    /// first day.
    ///
    /// # Errors
    /// [`Error::Refused`] when `as_of` is not a real date, `known_at` is not on a class's axis or
    /// is an integer key and the walk reads a link, or the walk would follow a link whose
    /// ending's valid time is unknown; otherwise as [`Graph::walk`].
    pub fn walk_as_of(
        &self,
        root: &NodeRef,
        via: &[&str],
        hops: usize,
        cap: usize,
        as_of: &Date,
        known_at: Option<At>,
    ) -> Result<Walk, Error> {
        self.walk_in(root, via, hops, cap, Some((as_of, known_at)))
    }

    fn walk_in(
        &self,
        root: &NodeRef,
        via: &[&str],
        hops: usize,
        cap: usize,
        as_of: Option<(&Date, Option<At>)>,
    ) -> Result<Walk, Error> {
        let tx = self.conn.unchecked_transaction()?;
        let ontology = self.current_ontology()?;
        let root_class = ontology.class(&root.class)?;
        let as_of = as_of
            .map(|(date, known)| date.parts().map(|parts| (day_text(parts), known)))
            .transpose()?;
        let steps = via
            .iter()
            .map(|v| parse(&ontology, v))
            .collect::<Result<Vec<_>, _>>()?;
        let sql = format!("SELECT id FROM {} WHERE key = ?1", class_table(&root.class));
        let root_id: i64 = tx
            .prepare_cached(&sql)?
            .query_row(params![root.key], |row| row.get(0))
            .optional()?
            .ok_or_else(|| Error::NotFound {
                class: root.class.clone(),
                key: root.key.clone(),
            })?;
        if let Some((day, known)) = &as_of
            && node_as_of(&tx, root_class, &root.key, day, *known)?.is_none()
        {
            return Ok(Walk {
                nodes: Vec::new(),
                capped: false,
            });
        }
        let mut seen: HashSet<(&str, i64)> = HashSet::from([(root.class.as_str(), root_id)]);
        let mut frontier: Vec<(&str, i64, String)> =
            vec![(root.class.as_str(), root_id, root.key.clone())];
        let mut found: Vec<(NodeRef, ValidTime)> = Vec::new();
        let mut capped = false;
        'hops: for _ in 0..hops {
            if frontier.is_empty() {
                break;
            }
            let mut next = Vec::new();
            for (class, id, here_key) in &frontier {
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
                        "SELECT n.id, n.key, n.valid FROM {} r JOIN {} n ON n.id = r.{far} WHERE r.{near} = ?1",
                        relation_table(&relation.name),
                        class_table(there)
                    );
                    let mut statement = tx.prepare_cached(&sql)?;
                    let mut rows = statement.query(params![id])?;
                    while let Some(row) = rows.next()? {
                        let key: String = row.get(1)?;
                        let valid = match &as_of {
                            None => ValidTime::from_sql(row.get(2)?),
                            Some((day, known)) => {
                                let ends = if backwards {
                                    (key.as_str(), here_key.as_str())
                                } else {
                                    (here_key.as_str(), key.as_str())
                                };
                                let link = link_as_of(&tx, &relation.name, ends, day, *known)?;
                                if link.is_none_or(|link| link.ended) {
                                    continue;
                                }
                                let class = ontology.class(there)?;
                                match node_as_of(&tx, class, &key, day, *known)? {
                                    Some((version, _)) => version.valid,
                                    None => continue,
                                }
                            }
                        };
                        let node: (&str, i64) = (there.as_str(), row.get(0)?);
                        if seen.insert(node) {
                            // Past the cap, one more node is all it takes to know the walk
                            // stopped short.
                            if found.len() >= cap {
                                capped = true;
                                break 'hops;
                            }
                            found.push((NodeRef::new(there, key.clone()), valid));
                            next.push((node.0, node.1, key));
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
    /// [`Error::Refused`] when `as_of` is not a real date, the count as of it would follow a link
    /// whose ending's valid time is unknown, a class, relation or property is not declared, or a
    /// relation on the path does not join the class the path has reached; the graph file cannot
    /// be read.
    pub fn count(&self, count: &Count) -> Result<Vec<Tally>, Error> {
        let as_of: Option<String> = count
            .as_of
            .as_ref()
            .map(|date| date.parts().map(day_text))
            .transpose()?;
        if as_of.is_none() && count.known_at.is_some() {
            return Err(Error::Refused(
                "a count known at a point is a count as of a valid time: it needs as_of".into(),
            ));
        }
        let tx = self.conn.unchecked_transaction()?;
        let ontology = self.current_ontology()?;
        let query = count_query(&ontology, count)?;
        let roots = serde_json::to_string(&count.roots)?;
        let mut statement = tx.prepare(&query)?;
        let mut rows = match (&as_of, count.known_at) {
            (Some(date), Some(at)) => statement.query(params![roots, date, at.bound()])?,
            (Some(date), None) => statement.query(params![roots, date])?,
            (None, _) => statement.query(params![roots])?,
        };
        let mut tallies: HashMap<String, Tally> = HashMap::new();
        let mut last: Option<(String, i64)> = None;
        while let Some(row) = rows.next()? {
            if let Some(link) = row.get::<_, Option<String>>(4)? {
                return Err(unknown_ending(&link, &as_of.unwrap_or_default()));
            }
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

/// The one query a count runs: a row per root, end node and group, with the number of paths
/// and, as of a date, a link on those paths whose ending's valid time is unknown.
fn count_query(ontology: &Ontology, count: &Count) -> Result<String, Error> {
    let root_class = ontology.class(&count.class)?;
    let mut joins = vec![format!("FROM {} n0", class_table(&root_class.name))];
    let mut conditions = vec!["n0.key IN (SELECT value FROM json_each(?1))".to_string()];
    // As of a date, every node and link the count reads has a version true then (`?2`,
    // `YYYY-MM-DD`), heard by `?3` when known at a point; see `held_sql`.
    let as_of = count.as_of.is_some();
    let known = |class: Option<&Class>| -> Result<Option<&'static str>, Error> {
        count
            .known_at
            .map(|at| known_bound(class, at).map(|(column, _)| column))
            .transpose()
    };
    let link_known = if as_of { known(None)? } else { None };
    if as_of {
        conditions.push(node_held_sql(root_class, "n0", known(Some(root_class))?));
    }
    let mut at: Vec<(&str, String)> = vec![(root_class.name.as_str(), "n0.id".to_string())];
    // As of a date, each step's ending, if its link ended: one valid on or before the date is
    // not followed, and one of unknown valid time names its link for the refusal.
    let mut unknown_endings = Vec::new();
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
            let near = format!("n{i}");
            let ends = if backwards { (&n, &near) } else { (&near, &n) };
            let link = held_sql(
                &link_entity(&relation.name),
                (&format!("{}.key", ends.0), &format!("{}.key", ends.1)),
                link_known,
            );
            let there = ontology.class(there)?;
            let node = node_held_sql(there, &n, known(Some(there))?);
            conditions.push(format!("{link} AND {node}"));
            let (join, condition, unknown, name) =
                ending(&relation.name, ends, &format!("e{}", i + 1), link_known);
            joins.push(join);
            conditions.push(condition);
            unknown_endings.push(format!("WHEN {unknown} THEN {name}"));
        }
        if let Some(filter) = &step.filter {
            let (condition, ended) = filter_condition(
                ontology,
                filter,
                (there, &root_class.name),
                &reached,
                i + 1,
                as_of,
                link_known,
            )?;
            conditions.push(condition);
            if let Some((join, unknown)) = ended {
                joins.push(join);
                unknown_endings.push(unknown);
            }
        }
        at.push((there.as_str(), reached));
    }
    let group = match &count.group_by {
        None => "NULL".to_string(),
        Some(group) => {
            let (join, column) = group_column(ontology, group, &at, as_of, known)?;
            joins.push(join);
            column
        }
    };
    let end = &at[at.len() - 1].1;
    let unknown_ending = if unknown_endings.is_empty() {
        "NULL".to_string()
    } else {
        format!("max(CASE {} END)", unknown_endings.join(" "))
    };
    Ok(format!(
        "SELECT n0.key, {end}, {group}, count(*), {unknown_ending} {} WHERE {} \
         GROUP BY n0.id, {end}, {group} ORDER BY n0.id, {end}",
        joins.join(" "),
        conditions.join(" AND ")
    ))
}

/// The join that reads the grouped node, `g` (the first node of its class on the path, each
/// path node's id in `at`), and the column its property is read from: as of a date, its
/// version true then.
fn group_column(
    ontology: &Ontology,
    group: &GroupBy,
    at: &[(&str, String)],
    as_of: bool,
    known: impl Fn(Option<&Class>) -> Result<Option<&'static str>, Error>,
) -> Result<(String, String), Error> {
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
    let join = format!(
        "JOIN {} g ON g.id = {}",
        class_table(&class.name),
        at[position].1
    );
    let column = if as_of {
        version_property(class, &group.property, known(Some(class))?)
    } else {
        format!("g.{}", property_column(&group.property))
    };
    Ok((join, column))
}

/// As of a date, the grouped node `g`'s `property` as its version true then holds it (see
/// `held_sql`), read as the column a node's current row holds it in would give it: text and a
/// decimal's digits as text, an integer as an integer, a float as a real.
fn version_property(class: &Class, property: &str, known: Option<&str>) -> String {
    let bound = known.map_or(String::new(), |column| format!(" AND v.{column} <= ?3"));
    let path = |tail: &str| literal(&format!("$.\"{}\"{tail}", property.replace('"', "\"\"")));
    format!(
        "(SELECT CASE json_type(v.properties, {at}) WHEN 'text' THEN json_extract(v.properties, {at}) \
         ELSE coalesce(json_extract(v.properties, {integer}), json_extract(v.properties, {float}), \
         json_extract(v.properties, {decimal})) END FROM _hearings v WHERE v.entity = {entity} \
         AND v.key = g.key AND v.to_key IS NULL AND {valid}{bound} \
         ORDER BY {day} DESC, v.integer DESC, v.point DESC, v.event DESC LIMIT 1)",
        at = path(""),
        integer = path(".integer"),
        float = path(".float"),
        decimal = path(".decimal"),
        entity = literal(&node_entity(&class.name)),
        valid = valid_by_sql("v.valid"),
        day = valid_day("v.valid"),
    )
}

/// As of a date (`?2`), the ending, aliased `e`, of the link of `relation` between the nodes
/// `from` and `to`, if it ended (and was heard by `?3` on the column `known` names, when given):
/// the join that reads it, the condition that an ending valid on or before the date is not
/// followed, the condition that its valid time is unknown, and the link's name for the refusal.
/// A correction is not an ending and joins nothing.
fn ending(
    relation: &str,
    (from, to): (&String, &String),
    e: &str,
    known: Option<&str>,
) -> (String, String, String, String) {
    let bound = known.map_or(String::new(), |column| format!(" AND {e}.{column} <= ?3"));
    (
        format!(
            "LEFT JOIN _ends {e} ON {e}.entity = {} AND {e}.key = {from}.key \
             AND {e}.to_key = {to}.key AND {e}.point IS NOT NULL{bound}",
            literal(&link_entity(relation))
        ),
        format!(
            "({e}.valid IS NULL OR NOT {})",
            valid_by_sql(&format!("{e}.valid"))
        ),
        format!("{e}.event IS NOT NULL AND {e}.valid IS NULL"),
        format!(
            "{} || {from}.key || ' to ' || {to}.key",
            literal(&format!("{relation} link from "))
        ),
    )
}

/// The condition a step's filter puts on the node it reached, `reached`, of `class`, against
/// the root, of `root_class`. As of a date (bounded on the column `known` when known at a
/// point), the filter's link must have a version true then and not have ended by then, and the
/// condition comes with its ending's join and the case naming the link when that ending's valid
/// time is unknown, which a count refuses.
fn filter_condition(
    ontology: &Ontology,
    filter: &Filter,
    (class, root_class): (&str, &str),
    reached: &str,
    step: usize,
    as_of: bool,
    known: Option<&str>,
) -> Result<(String, Option<(String, String)>), Error> {
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
    let not = if filter.keep { "" } else { "NOT " };
    if !as_of {
        let f = format!("f{step}");
        return Ok((
            format!(
                "{not}EXISTS (SELECT 1 FROM {} {f} WHERE {f}.{node_col} = {reached} AND {f}.{root_col} = n0.id)",
                relation_table(&by.name),
            ),
            None,
        ));
    }
    let (node, root) = (format!("n{step}"), "n0".to_string());
    let ends = if backwards {
        (&root, &node)
    } else {
        (&node, &root)
    };
    let held = held_sql(
        &link_entity(&by.name),
        (&format!("{}.key", ends.0), &format!("{}.key", ends.1)),
        known,
    );
    let (join, not_ended, unknown, name) = ending(&by.name, ends, &format!("fe{step}"), known);
    // A link that held and whose ending's valid time is unknown keeps the row either way, so
    // the count sees it and refuses.
    let unknown = format!("{held} AND {unknown}");
    Ok((
        format!("({not}({held} AND {not_ended}) OR ({unknown}))"),
        Some((join, format!("WHEN {unknown} THEN {name}"))),
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
            known_at: None,
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
            known_at: None,
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
            None,
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
        let found = g.find_as_of("item", "r", &bad, None);
        assert!(refused(&found), "find_as_of: {found:?}");
        let walked = g.walk_as_of(&NodeRef::new("item", "r"), &["part<-"], 1, 10, &bad, None);
        assert!(refused(&walked), "walk_as_of: {walked:?}");
        let counted = g.count(&Count {
            class: "item".to_string(),
            roots: vec!["r".to_string()],
            path: vec![Step::new("part<-")],
            group_by: None,
            as_of: Some(bad),
            known_at: None,
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
        assert!(g.find_as_of("item", "n", &month, None)?.is_some());
        let walked = g.walk_as_of(&NodeRef::new("item", "r"), &["part<-"], 1, 10, &month, None)?;
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
            known_at: None,
        })?;
        assert_eq!(tallies[0].distinct, 1);
        Ok(())
    }

    fn valid_of(found: Option<crate::Node>) -> Option<ValidTime> {
        found.map(|node| node.valid)
    }

    fn on(year: u16, month: u8, day: u8) -> ValidTime {
        ValidTime::On(Date::day(year, month, day))
    }

    #[test]
    fn an_as_of_read_answers_with_the_past_version_that_held()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = items(vec![item("v", &Date::day(2020, 1, 1))])?;
        g.write(&Write::new("src").unit(item("v", &Date::day(2022, 1, 1))))?;
        let as_of = |date: Date| g.find_as_of("item", "v", &date, None).map(valid_of);
        assert_eq!(as_of(Date::day(2021, 1, 1))?, Some(on(2020, 1, 1)));
        assert_eq!(as_of(Date::day(2022, 1, 1))?, Some(on(2022, 1, 1)));
        assert_eq!(as_of(Date::day(2019, 12, 31))?, None);
        Ok(())
    }

    #[test]
    fn an_as_of_read_passes_over_a_current_version_of_unknown_valid_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = items(vec![item("u", &Date::day(2020, 1, 1))])?;
        g.write(&Write::new("src").unit(Unit::new(
            "item",
            "u",
            [("name", "u")],
            ValidTime::Unknown,
        )))?;
        let found = g.find_as_of("item", "u", &Date::day(2100, 1, 1), None)?;
        assert_eq!(valid_of(found), Some(on(2020, 1, 1)));
        let only = items(vec![Unit::new(
            "item",
            "w",
            [("name", "w")],
            ValidTime::Unknown,
        )])?;
        assert!(
            only.find_as_of("item", "w", &Date::day(2100, 1, 1), None)?
                .is_none()
        );
        Ok(())
    }

    /// Items `r` and `n`, valid from 2020-01-01, with `n` part of `r` and flagging `r` by links
    /// valid from 2020-01-01; the `part` link ends with the valid time `part`, the `flags` link
    /// with `flags`, when given.
    fn ended(
        part: Option<ValidTime>,
        flags: Option<ValidTime>,
    ) -> Result<Graph, Box<dyn std::error::Error>> {
        let jan = Date::day(2020, 1, 1);
        let when = ValidTime::On(jan.clone());
        let mut g = items(vec![
            item("r", &jan),
            item("n", &jan)
                .link("part", "n", "r", when.clone())
                .link("flags", "n", "r", when),
        ])?;
        let at = std::time::SystemTime::now();
        for (relation, valid) in [("part", part), ("flags", flags)] {
            if let Some(valid) = valid {
                g.end_link(relation, "n", "r", at, valid, "moderator")?;
            }
        }
        Ok(g)
    }

    fn walked(g: &Graph, as_of: &Date) -> Result<usize, Error> {
        let walk = g.walk_as_of(&NodeRef::new("item", "r"), &["part<-"], 1, 10, as_of, None)?;
        Ok(walk.nodes.len())
    }

    fn unknown_ending(result: &Result<impl std::fmt::Debug, Error>, link: &str) -> bool {
        matches!(result, Err(Error::Refused(why))
            if why.contains(&format!("the {link} ended, and its ending's valid time is unknown")))
    }

    #[test]
    fn a_walk_as_of_a_date_does_not_follow_a_link_ended_by_then()
    -> Result<(), Box<dyn std::error::Error>> {
        let g = ended(Some(ValidTime::On(Date::day(2021, 1, 1))), None)?;
        assert_eq!(walked(&g, &Date::day(2020, 6, 1))?, 1);
        assert_eq!(walked(&g, &Date::day(2021, 1, 1))?, 0);
        Ok(())
    }

    #[test]
    fn a_walk_as_of_a_date_refuses_a_link_whose_ending_has_no_valid_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let g = ended(Some(ValidTime::Unknown), None)?;
        let refused = walked(&g, &Date::day(2020, 6, 1));
        assert!(
            unknown_ending(&refused, "part link from n to r"),
            "{refused:?}"
        );
        assert_eq!(walked(&g, &Date::day(2019, 6, 1))?, 0);
        Ok(())
    }

    #[test]
    fn a_walk_as_of_a_date_returns_the_valid_time_of_the_version_then()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = ended(None, None)?;
        g.write(&Write::new("src").unit(item("n", &Date::day(2022, 1, 1))))?;
        let walk = g.walk_as_of(
            &NodeRef::new("item", "r"),
            &["part<-"],
            1,
            10,
            &Date::day(2021, 1, 1),
            None,
        )?;
        assert_eq!(
            walk.nodes,
            vec![(
                NodeRef::new("item", "n"),
                ValidTime::On(Date::day(2020, 1, 1))
            )]
        );
        Ok(())
    }

    fn filtered(g: &Graph, keep: bool, as_of: &Date) -> Result<u64, Error> {
        let tallies = g.count(&Count {
            class: "item".to_string(),
            roots: vec!["r".to_string()],
            path: vec![Step::new("part<-").filter("flags", keep)],
            group_by: None,
            as_of: Some(as_of.clone()),
            known_at: None,
        })?;
        Ok(tallies[0].distinct)
    }

    #[test]
    fn a_count_filter_as_of_a_date_reads_its_link_ended_by_then()
    -> Result<(), Box<dyn std::error::Error>> {
        let g = ended(None, Some(ValidTime::On(Date::day(2021, 1, 1))))?;
        let (before, after) = (Date::day(2020, 6, 1), Date::day(2021, 6, 1));
        assert_eq!(filtered(&g, true, &before)?, 1);
        assert_eq!(filtered(&g, true, &after)?, 0);
        assert_eq!(filtered(&g, false, &before)?, 0);
        assert_eq!(filtered(&g, false, &after)?, 1);
        Ok(())
    }

    #[test]
    fn a_count_filter_as_of_a_date_refuses_a_link_whose_ending_has_no_valid_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let g = ended(None, Some(ValidTime::Unknown))?;
        for keep in [true, false] {
            let refused = filtered(&g, keep, &Date::day(2020, 6, 1));
            assert!(
                unknown_ending(&refused, "flags link from n to r"),
                "{refused:?}"
            );
        }
        assert_eq!(filtered(&g, true, &Date::day(2019, 6, 1))?, 0);
        Ok(())
    }

    #[test]
    fn a_count_as_of_a_date_groups_by_the_property_of_the_version_then()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = ended(None, None)?;
        g.write(&Write::new("src").unit(Unit::new(
            "item",
            "r",
            [("name", "renamed")],
            ValidTime::On(Date::day(2022, 1, 1)),
        )))?;
        let groups = |as_of: Date| -> Result<_, Error> {
            Ok(g.count(&Count {
                class: "item".to_string(),
                roots: vec!["r".to_string()],
                path: vec![Step::new("part<-")],
                group_by: Some(GroupBy {
                    class: "item".to_string(),
                    property: "name".to_string(),
                }),
                as_of: Some(as_of),
                known_at: None,
            })?[0]
                .groups
                .clone())
        };
        let one = |name: &str| BTreeMap::from([(Some(name.to_string()), (1, 1))]);
        assert_eq!(groups(Date::day(2021, 1, 1))?, one("r"));
        assert_eq!(groups(Date::day(2022, 1, 1))?, one("renamed"));
        Ok(())
    }

    #[test]
    fn a_read_known_at_a_point_off_its_axis_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let g = ended(None, None)?;
        let jan = Date::day(2021, 1, 1);
        let key = Some(crate::At::Key(3));
        assert!(matches!(
            g.find_as_of("item", "n", &jan, key),
            Err(Error::Refused(_))
        ));
        let walk = g.walk_as_of(&NodeRef::new("item", "r"), &["part<-"], 1, 10, &jan, key);
        assert!(matches!(walk, Err(Error::Refused(_))), "{walk:?}");
        let count = Count {
            class: "item".to_string(),
            roots: vec!["r".to_string()],
            path: Vec::new(),
            group_by: None,
            as_of: None,
            known_at: Some(crate::At::Time(std::time::SystemTime::now())),
        };
        assert!(matches!(g.count(&count), Err(Error::Refused(_))));
        Ok(())
    }

    #[test]
    fn an_as_of_read_of_a_node_with_one_version_answers_it_or_none()
    -> Result<(), Box<dyn std::error::Error>> {
        let g = items(vec![item("o", &Date::day(2020, 6, 1))])?;
        let found = g.find_as_of("item", "o", &Date::day(2020, 6, 1), None)?;
        assert_eq!(
            found.map(|n| n.valid),
            Some(ValidTime::On(Date::day(2020, 6, 1)))
        );
        assert!(
            g.find_as_of("item", "o", &Date::day(2020, 5, 31), None)?
                .is_none()
        );
        assert!(
            g.find_as_of("item", "absent", &Date::day(2020, 6, 1), None)?
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
        assert!(
            g.find_as_of("item", "m", &Date::day(2020, 1, 1), None)?
                .is_some()
        );
        assert!(
            g.find_as_of("item", "d", &Date::month(2020, 1), None)?
                .is_none()
        );
        Ok(())
    }
}
