//! Reads: find a node or a link, walk from a root, count exactly.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::open::{
    Graph, Ontology, Relation, class_table, concat, property_column, relation_table,
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
    pub properties: BTreeMap<String, String>,
    pub source: String,
    pub valid: ValidTime,
    pub recorded: SystemTime,
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
/// nodes the path ends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Count {
    pub class: String,
    pub roots: Vec<String>,
    pub path: Vec<Step>,
    pub group_by: Option<GroupBy>,
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
    let sql = format!(
        "SELECT source, valid, recorded{} FROM {} WHERE key = ?1",
        concat(&class.properties, |p| format!(", {}", property_column(p))),
        class_table(&class.name)
    );
    Ok(conn
        .prepare_cached(&sql)?
        .query_row(params![key], |row| {
            let mut properties = BTreeMap::new();
            for (i, name) in class.properties.iter().enumerate() {
                if let Some(value) = row.get::<_, Option<String>>(3 + i)? {
                    properties.insert(name.clone(), value);
                }
            }
            Ok(Node {
                class: class.name.clone(),
                key: key.to_string(),
                properties,
                source: row.get(0)?,
                valid: ValidTime::from_sql(row.get(1)?),
                recorded: recorded(row.get(2)?),
            })
        })
        .optional()?)
}

impl Graph {
    /// The node of `class` with natural key `key`.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn find(&self, class: &str, key: &str) -> Result<Option<Node>, Error> {
        find_in(&self.conn, &self.ontology, class, key)
    }

    /// The node of `class` with natural key `key` as of `as_of`: none when it was not yet
    /// true then. A node whose valid time is unknown is never returned as of a date.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn find_as_of(&self, class: &str, key: &str, as_of: &Date) -> Result<Option<Node>, Error> {
        Ok(self
            .find(class, key)?
            .filter(|node| matches!(&node.valid, ValidTime::On(date) if date <= as_of)))
    }

    /// The link of `relation` from the node keyed `from` to the node keyed `to`.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn find_link(&self, relation: &str, from: &str, to: &str) -> Result<Option<Link>, Error> {
        let relation = self.ontology.relation(relation)?;
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
        let rows: Vec<(String, String, String)> = tx
            .prepare_cached(
                "SELECT relation, from_key, to_key FROM _open ORDER BY relation, from_key, to_key",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<_, _>>()?;
        let mut open = Vec::with_capacity(rows.len());
        for (relation, from_key, to_key) in rows {
            let relation = self.ontology.relation(&relation)?;
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

    /// Walks from `root` along the relations in `via` (each read backwards when written
    /// `name<-`), for up to `hops` hops, and returns at most `cap` nodes, nearest first. The
    /// root itself is not among them.
    ///
    /// # Errors
    /// [`Error::NotFound`] when the root is not in the graph; a relation in `via` is not
    /// declared; the graph file cannot be read.
    pub fn walk(
        &self,
        root: &NodeRef,
        via: &[&str],
        hops: usize,
        cap: usize,
    ) -> Result<Vec<NodeRef>, Error> {
        let steps = via
            .iter()
            .map(|v| parse(&self.ontology, v))
            .collect::<Result<Vec<_>, _>>()?;
        let tx = self.conn.unchecked_transaction()?;
        let sql = format!("SELECT id FROM {} WHERE key = ?1", class_table(&root.class));
        let root_id: i64 = tx
            .prepare_cached(&sql)?
            .query_row(params![root.key], |row| row.get(0))
            .optional()?
            .ok_or_else(|| Error::NotFound {
                class: root.class.clone(),
                key: root.key.clone(),
            })?;
        let mut seen: HashSet<(&str, i64)> = HashSet::from([(root.class.as_str(), root_id)]);
        let mut frontier: Vec<(&str, i64)> = vec![(root.class.as_str(), root_id)];
        let mut found: Vec<NodeRef> = Vec::new();
        let hops = if cap == 0 { 0 } else { hops };
        'hops: for _ in 0..hops {
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
                        "SELECT n.id, n.key FROM {} r JOIN {} n ON n.id = r.{far} WHERE r.{near} = ?1",
                        relation_table(&relation.name),
                        class_table(there)
                    );
                    let mut statement = tx.prepare_cached(&sql)?;
                    let mut rows = statement.query(params![id])?;
                    while let Some(row) = rows.next()? {
                        let node: (&str, i64) = (there.as_str(), row.get(0)?);
                        if seen.insert(node) {
                            found.push(NodeRef::new(there, row.get::<_, String>(1)?));
                            next.push(node);
                            if found.len() >= cap {
                                break 'hops;
                            }
                        }
                    }
                }
            }
            frontier = next;
        }
        Ok(found)
    }

    /// Counts exactly, in one pass, from every root: the distinct nodes the path ends on, the
    /// total, and both per group. Returns one tally per root, in the order given.
    ///
    /// # Errors
    /// [`Error::Refused`] when a class, relation or property is not declared, or a relation on
    /// the path does not join the class the path has reached; the graph file cannot be read.
    pub fn count(&self, count: &Count) -> Result<Vec<Tally>, Error> {
        let query = count_query(&self.ontology, count)?;
        let roots = serde_json::to_string(&count.roots)?;
        let tx = self.conn.unchecked_transaction()?;
        let mut statement = tx.prepare(&query)?;
        let mut rows = statement.query(params![roots])?;
        let mut tallies: HashMap<String, Tally> = HashMap::new();
        let mut last: Option<(String, i64)> = None;
        while let Some(row) = rows.next()? {
            let root: String = row.get(0)?;
            let end: i64 = row.get(1)?;
            let group: Option<String> = row.get(2)?;
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
        if let Some(filter) = &step.filter {
            conditions.push(filter_condition(
                ontology,
                filter,
                there,
                &root_class.name,
                &reached,
                i + 1,
            )?);
        }
        at.push((there.as_str(), reached));
    }
    let group = match &count.group_by {
        None => "NULL".to_string(),
        Some(group) => {
            let class = ontology.class(&group.class)?;
            if !class.properties.contains(&group.property) {
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
        "{}EXISTS (SELECT 1 FROM {} {f} WHERE {f}.{node_col} = {reached} AND {f}.{root_col} = n0.id)",
        if filter.keep { "" } else { "NOT " },
        relation_table(&by.name)
    ))
}
