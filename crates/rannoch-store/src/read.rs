//! Reading the graph: a node by its key, now or at a view, and at a view a node's links, a
//! class's nodes, the counts of every class and relation, and a node's description. A read can
//! also be asked as an event and answered as one.

use std::collections::BTreeMap;
use std::time::SystemTime;

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::history::{
    Version, VersionColumns, Versioned, View, bounds, columns, link_entity, millis, newest,
    node_entity, node_in_view, time, version_of,
};
use crate::queries::{
    KNOWN_NOW, keys_by_property, link_count, links_in_view, node_count, nodes_in_view, select_node,
};
use crate::{Class, Date, Error, EventId, Graph, NodeRef, Ontology, ValidTime, Value};

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
}

impl Graph {
    /// The node of `class` with natural key `key` as it is now: its newest version by its
    /// class's order, never the version that arrived last.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn find(&self, class: &str, key: &str) -> Result<Option<Node>, Error> {
        let ontology = self.current_ontology()?;
        find_in(&self.conn, &ontology, class, key)
    }

    /// The node of `class` with natural key `key` as of the valid time `as_of`, in `view`: among
    /// its versions in the view valid on or before `as_of`, the greatest by valid time wins, then
    /// by the class's order. A version whose valid time is unknown is never returned; `None` when
    /// no version was valid by then. A month stands for its first day. A read that needs no valid
    /// time is [`Graph::state_at`].
    ///
    /// # Errors
    /// [`Error::Refused`] when `as_of` is not a real date, or the view's point has more parts
    /// than the class's axis; the class is not declared, or the graph file cannot be read.
    pub fn find_as_of(
        &self,
        class: &str,
        key: &str,
        as_of: &Date,
        view: &View,
    ) -> Result<Option<Node>, Error> {
        let day = day_text(as_of.parts()?);
        let ontology = self.current_ontology()?;
        let class = ontology.class(class)?;
        Ok(node_in(
            class,
            key,
            node_in_view(&self.conn, class, key, view, Some(&day))?,
        ))
    }
}

/// Which links of a node a read follows: those from it, or those to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Out,
    In,
}

/// How many nodes each class has and how many links each relation holds, at a view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    pub nodes: BTreeMap<String, u64>,
    pub links: BTreeMap<String, u64>,
}

/// A node at a view: its version, with its properties, and each relation it has links in at
/// that view, by name and direction, with how many.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    pub version: Version,
    pub links: BTreeMap<(String, Direction), u64>,
}

impl Graph {
    /// The links of `relation` from `node` ([`Direction::Out`]) or to it, that hold in `view`,
    /// each as the other end's key and the link's version there, in the other end's key order.
    /// A link holds when [`Graph::link_state_at`] has a version of it that has not ended.
    ///
    /// # Errors
    /// [`Error::Refused`] when `node`'s class is not the relation's end in that direction, or the
    /// view's point has more parts than the relation's axis; the relation is not declared, or the
    /// graph file cannot be read.
    pub fn links(
        &self,
        node: &NodeRef,
        relation: &str,
        direction: Direction,
        view: &View,
    ) -> Result<Vec<(String, Version)>, Error> {
        let ontology = self.current_ontology()?;
        let declared = ontology.relation(relation)?;
        let out = direction == Direction::Out;
        let end = if out { &declared.from } else { &declared.to };
        if *end != node.class {
            return Err(Error::Refused(format!(
                "relation {relation} holds from {} to {}; node {} is of class {}",
                declared.from, declared.to, node.key, node.class
            )));
        }
        let (axis, known) = bounds(&declared.axis, view, &format!("relation {relation}"))?;
        keyed_versions(
            &self.conn,
            &links_in_view(out),
            params![link_entity(relation), axis, known, node.key],
        )
    }

    /// The nodes of `class` in `view`, each as its key and its version there, in key order:
    /// those whose version has every value in `filter`. A filter names only properties the class
    /// declares indexed, and is read through their index.
    ///
    /// # Errors
    /// [`Error::Refused`] when `filter` names a property the class does not index, or the view's
    /// point has more parts than the class's axis; the class is not declared, or the graph file
    /// cannot be read.
    pub fn nodes(
        &self,
        class: &str,
        filter: &BTreeMap<String, Value>,
        view: &View,
    ) -> Result<Vec<(String, Version)>, Error> {
        let ontology = self.current_ontology()?;
        let class = ontology.class(class)?;
        if let Some(property) = filter.keys().find(|p| !class.indexed.contains(*p)) {
            return Err(Error::Refused(format!(
                "class {} does not index property {property}, so nodes cannot be listed by it",
                class.name
            )));
        }
        let (axis, known) = bounds(&class.axis, view, &format!("class {}", class.name))?;
        let entity = node_entity(&class.name);
        let mut nodes = match filter.iter().next() {
            None => keyed_versions(&self.conn, &nodes_in_view(""), params![entity, axis, known])?,
            Some((property, value)) => keyed_versions(
                &self.conn,
                &nodes_in_view(&keys_by_property(&class.name, property)),
                params![entity, axis, known, serde_json::to_string(value)?],
            )?,
        };
        nodes.retain(|(_, version)| {
            filter
                .iter()
                .all(|(property, value)| version.properties.get(property) == Some(value))
        });
        Ok(nodes)
    }

    /// How many nodes each declared class has in `view`, and how many links hold in each
    /// declared relation, each counted in one pass over its own index.
    ///
    /// # Errors
    /// [`Error::Refused`] when the view's point has more parts than a class's or relation's
    /// axis; the graph file cannot be read.
    pub fn counts(&self, view: &View) -> Result<Counts, Error> {
        let ontology = self.current_ontology()?;
        let mut counts = Counts::default();
        for class in ontology.classes.values() {
            let (axis, known) = bounds(&class.axis, view, &format!("class {}", class.name))?;
            let count = counted(
                &self.conn,
                &node_count(),
                params![node_entity(&class.name), axis, known],
            )?;
            counts.nodes.insert(class.name.clone(), count);
        }
        for relation in ontology.relations.values() {
            let (axis, known) =
                bounds(&relation.axis, view, &format!("relation {}", relation.name))?;
            let count = counted(
                &self.conn,
                &link_count(),
                params![link_entity(&relation.name), axis, known],
            )?;
            counts.links.insert(relation.name.clone(), count);
        }
        Ok(counts)
    }

    /// `node` in `view`: its version, and each relation it has links in there, in each direction,
    /// with how many hold, as [`Graph::links`] reads them. `None` before its first version.
    ///
    /// # Errors
    /// [`Error::Refused`] when the view's point has more parts than the class's axis or a
    /// relation's; the class is not declared, or the graph file cannot be read.
    pub fn describe(&self, node: &NodeRef, view: &View) -> Result<Option<Description>, Error> {
        let ontology = self.current_ontology()?;
        let class = ontology.class(&node.class)?;
        let Some((version, ..)) = node_in_view(&self.conn, class, &node.key, view, None)? else {
            return Ok(None);
        };
        let mut links = BTreeMap::new();
        for relation in ontology.relations.values() {
            for (end, direction) in [
                (&relation.from, Direction::Out),
                (&relation.to, Direction::In),
            ] {
                if *end != class.name {
                    continue;
                }
                let (axis, known) =
                    bounds(&relation.axis, view, &format!("relation {}", relation.name))?;
                let count = counted(
                    &self.conn,
                    &format!(
                        "SELECT count(*) FROM ({})",
                        links_in_view(direction == Direction::Out)
                    ),
                    params![link_entity(&relation.name), axis, known, node.key],
                )?;
                if count > 0 {
                    links.insert((relation.name.clone(), direction), count);
                }
            }
        }
        Ok(Some(Description { version, links }))
    }

    /// Answers the `rannoch.query` event `query` with the body of a `rannoch.answer` event: its
    /// subject the query's id, its data the read's result and the known-at it was read at. A
    /// query's data is one read (`links`, `nodes`, `counts` or `describe`) and its `view`; a
    /// view with no known-at is read at the greatest known-at the store holds, which reads all
    /// it knows. The in-process reads are the fast path; this asks them.
    ///
    /// # Errors
    /// [`Error::Refused`] when `query` is not a `CloudEvents` 1.0 `rannoch.query` event or its
    /// read is refused; [`Error::Encoding`] when its data is not a read; the graph file cannot
    /// be read.
    pub fn answer(&self, query: &str) -> Result<String, Error> {
        let attributes = rannoch_cloudevents::attributes(query)?;
        if attributes.ty != QUERY {
            return Err(Error::Refused(format!(
                "event {} is of type {}, not {QUERY}",
                attributes.id, attributes.ty
            )));
        }
        let Envelope { data: asked } = serde_json::from_str(query)?;
        let known = match asked.view.known {
            Some(known) => Some(known),
            None => self.conn.query_row(KNOWN_NOW, [], |row| row.get(0))?,
        };
        let view = View {
            axis: asked.view.axis,
            known: known.map(time),
        };
        let listed = |rows: Vec<(String, Version)>| {
            rows.iter()
                .map(|(key, version)| json!({"key": key, "version": version_json(version)}))
                .collect::<Vec<_>>()
        };
        let result = match asked.read {
            Read::Links {
                node,
                relation,
                direction,
            } => json!(listed(self.links(&node, &relation, direction, &view)?)),
            Read::Nodes { class, filter } => json!(listed(self.nodes(&class, &filter, &view)?)),
            Read::Counts {} => {
                let counts = self.counts(&view)?;
                json!({"nodes": counts.nodes, "links": counts.links})
            }
            Read::Describe { node } => match self.describe(&node, &view)? {
                None => serde_json::Value::Null,
                Some(description) => json!({
                    "version": version_json(&description.version),
                    "links": description.links.iter().map(|((relation, direction), count)| {
                        json!({"relation": relation, "direction": direction, "count": count})
                    }).collect::<Vec<_>>(),
                }),
            },
        };
        Ok(json!({
            "specversion": "1.0",
            "id": EventId::now(),
            "source": "rannoch",
            "type": ANSWER,
            "subject": attributes.id,
            "data": {"result": result, "known": known},
        })
        .to_string())
    }
}

/// The one count `sql` reads with `bound`; refused as out of range if it is negative, which a
/// count never is.
fn counted(conn: &Connection, sql: &str, bound: impl rusqlite::Params) -> Result<u64, Error> {
    let count: i64 = conn
        .prepare_cached(sql)?
        .query_row(bound, |row| row.get(0))?;
    u64::try_from(count)
        .map_err(|_| Error::Storage(rusqlite::Error::IntegralValueOutOfRange(0, count)))
}

/// The rows `sql` reads with `bound`, each a key and the columns of a version (see
/// [`VersionColumns`]): a link's version has not ended, or it would not be read.
fn keyed_versions(
    conn: &Connection,
    sql: &str,
    bound: impl rusqlite::Params,
) -> Result<Vec<(String, Version)>, Error> {
    let rows: Vec<(String, VersionColumns)> = conn
        .prepare_cached(sql)?
        .query_map(bound, |row| Ok((row.get(0)?, columns(row, 1)?)))?
        .collect::<Result<_, _>>()?;
    rows.into_iter()
        .map(|(key, columns)| Ok((key, version_of(columns, false)?)))
        .collect()
}

/// The type of an event that asks a read.
const QUERY: &str = "rannoch.query";
/// The type of an event that answers one.
const ANSWER: &str = "rannoch.answer";

#[derive(Deserialize)]
struct Envelope {
    data: Query,
}

/// A query's data: one read and the view it is asked at.
#[derive(Deserialize)]
struct Query {
    #[serde(flatten)]
    read: Read,
    #[serde(default)]
    view: ViewData,
}

/// A view as a query states it: an axis point, and a known-at in milliseconds since the epoch.
#[derive(Deserialize, Default)]
struct ViewData {
    axis: Option<Vec<i64>>,
    known: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Read {
    Links {
        node: NodeRef,
        relation: String,
        direction: Direction,
    },
    Nodes {
        class: String,
        #[serde(default)]
        filter: BTreeMap<String, Value>,
    },
    Counts {},
    Describe {
        node: NodeRef,
    },
}

/// A version as an answer holds it: its known-at in milliseconds since the epoch.
fn version_json(version: &Version) -> serde_json::Value {
    json!({
        "event": version.event,
        "source": version.source,
        "valid": version.valid,
        "axis": version.axis,
        "known": millis(version.known),
        "properties": version.properties,
        "ended": version.ended,
    })
}

/// A date's parts as `YYYY-MM-DD`: the form valid times are compared in.
fn day_text((year, month, day): (u16, u8, u8)) -> String {
    format!("{year:04}-{month:02}-{day:02}")
}

fn find_in(
    conn: &Connection,
    ontology: &Ontology,
    class: &str,
    key: &str,
) -> Result<Option<Node>, Error> {
    let class = ontology.class(class)?;
    let version = newest(conn, (&node_entity(&class.name), key, None))?;
    if version.is_some() {
        return Ok(node_in(class, key, version));
    }
    // A replaced node keeps its row and nothing else: its history is the new key's now.
    Ok(conn
        .prepare_cached(&select_node(&class.name))?
        .query_row(params![key], |row| {
            Ok(Node {
                class: class.name.clone(),
                key: key.to_string(),
                properties: BTreeMap::new(),
                source: row.get(0)?,
                valid: ValidTime::from_text(row.get(1)?),
                recorded: time(row.get(2)?),
            })
        })
        .optional()?)
}

/// The node keyed `key` in `class` as its `version` holds it.
fn node_in(class: &Class, key: &str, version: Option<Versioned>) -> Option<Node> {
    let (version, recorded, _) = version?;
    Some(Node {
        class: class.name.clone(),
        key: key.to_string(),
        properties: version.properties,
        source: version.source,
        valid: version.valid,
        recorded: time(recorded),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::{Duration, UNIX_EPOCH};

    use super::Direction::{In, Out};
    use crate::{Class, Declaration, Graph, NodeRef, Relation, ValidTime, Value, View, open};

    const KEY: [u8; 32] = [7; 32];

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    fn temp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rannoch-read-{name}-{}.db", uuid::Uuid::now_v7()))
    }

    /// A `CloudEvent` id whose origin event became known `seconds` after the Unix epoch.
    fn id_at(seconds: u64) -> String {
        let at = uuid::Timestamp::from_unix(uuid::NoContext, seconds, 0);
        format!("{}:0", uuid::Uuid::new_v7(at))
    }

    fn at(seconds: u64) -> View {
        View {
            axis: None,
            known: Some(UNIX_EPOCH + Duration::from_secs(seconds)),
        }
    }

    /// A book known at `seconds`, with its title and genre and a valid time `valid`.
    fn book(seconds: u64, key: &str, title: &str, genre: &str, valid: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.node.book","subject":"{key}","data":{{"properties":{{"title":"{title}","genre":"{genre}"}},"valid":{valid}}}}}"#,
            id_at(seconds)
        )
    }

    fn author(seconds: u64, key: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.node.author","subject":"{key}","data":{{"properties":{{"name":"{key}"}}}}}}"#,
            id_at(seconds)
        )
    }

    /// The link of `wrote` from author `from` to book `to` (`kind` `link`), or its ending
    /// (`ended`), known at `seconds`.
    fn wrote(seconds: u64, kind: &str, from: &str, to: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.{kind}.wrote","data":{{"from":{{"class":"author","key":"{from}"}},"to":{{"class":"book","key":"{to}"}}}}}}"#,
            id_at(seconds)
        )
    }

    /// A graph declaring authors, books indexed by genre, and who wrote which.
    fn graph(name: &str) -> std::result::Result<Graph, Box<dyn std::error::Error>> {
        let mut g = open(temp(name), &KEY)?;
        let declared = |name: &str| {
            format!(
                r#"{{"specversion":"1.0","id":"d-{name}","source":"rannoch:shop","type":"rannoch.declared.{name}"}}"#
            )
        };
        g.declare(
            &Declaration::Class(Class::new("author", ["name"])),
            &declared("author"),
        )?;
        g.declare(
            &Declaration::Class(Class {
                indexed: BTreeSet::from(["genre".to_string()]),
                ..Class::new("book", ["title", "genre"])
            }),
            &declared("book"),
        )?;
        g.declare(
            &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
            &declared("wrote"),
        )?;
        Ok(g)
    }

    /// The events every test reads: three books, two authors, four links of which one ended,
    /// and a book retitled into another genre later.
    fn events() -> Vec<String> {
        vec![
            book(10, "b2", "Two", "poetry", r#"{"on":"2020-01"}"#),
            book(10, "b1", "One", "fiction", r#""unknown""#),
            book(11, "b3", "Three", "fiction", r#"{"on":"2021-05-02"}"#),
            author(12, "a1"),
            author(12, "a2"),
            wrote(13, "link", "a1", "b2"),
            wrote(13, "link", "a1", "b1"),
            wrote(13, "link", "a2", "b1"),
            wrote(14, "link", "a1", "b3"),
            wrote(20, "ended", "a1", "b3"),
            book(30, "b2", "Two", "fiction", r#"{"on":"2022-01"}"#),
        ]
    }

    fn filled(
        name: &str,
        bodies: &[String],
    ) -> std::result::Result<Graph, Box<dyn std::error::Error>> {
        let mut g = graph(name)?;
        g.set_try_limit(1)?;
        let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
        for landed in g.commit(&refs, &[])? {
            landed?;
        }
        g.fill()?;
        assert_eq!(g.stopped()?, vec![], "every event fills");
        Ok(g)
    }

    fn keys(rows: &[(String, crate::Version)]) -> Vec<&str> {
        rows.iter().map(|(key, _)| key.as_str()).collect()
    }

    #[test]
    fn links_out_and_in_hold_in_the_view_in_key_order() -> Result {
        let g = filled("links", &events())?;
        let a1 = NodeRef::new("author", "a1");
        let b1 = NodeRef::new("book", "b1");
        let now = View::default();
        assert_eq!(keys(&g.links(&a1, "wrote", Out, &now)?), ["b1", "b2"]);
        assert_eq!(
            keys(&g.links(&a1, "wrote", Out, &at(15))?),
            ["b1", "b2", "b3"]
        );
        assert_eq!(
            keys(&g.links(&a1, "wrote", Out, &at(12))?),
            Vec::<&str>::new()
        );
        assert_eq!(keys(&g.links(&b1, "wrote", In, &now)?), ["a1", "a2"]);
        // Each listed link is what link_state_at reads, and no link it reads as ended is listed.
        for (other, version) in g.links(&a1, "wrote", Out, &at(15))? {
            assert_eq!(
                g.link_state_at("wrote", "a1", &other, &at(15))?,
                Some(version)
            );
        }
        assert!(
            g.link_state_at("wrote", "a1", "b3", &now)?
                .is_some_and(|v| v.ended)
        );
        assert!(
            g.links(&b1, "wrote", Out, &now).is_err(),
            "a book is not the from end"
        );
        Ok(())
    }

    #[test]
    fn nodes_list_by_key_and_filter_through_a_declared_index() -> Result {
        let g = filled("nodes", &events())?;
        let none = BTreeMap::new();
        let fiction = BTreeMap::from([("genre".to_string(), Value::from("fiction"))]);
        assert_eq!(
            keys(&g.nodes("book", &none, &View::default())?),
            ["b1", "b2", "b3"]
        );
        assert_eq!(keys(&g.nodes("book", &none, &at(10))?), ["b1", "b2"]);
        assert_eq!(keys(&g.nodes("book", &fiction, &at(29))?), ["b1", "b3"]);
        assert_eq!(
            keys(&g.nodes("book", &fiction, &View::default())?),
            ["b1", "b2", "b3"]
        );
        let poetry = BTreeMap::from([("genre".to_string(), Value::from("poetry"))]);
        assert_eq!(keys(&g.nodes("book", &poetry, &at(29))?), ["b2"]);
        assert_eq!(
            keys(&g.nodes("book", &poetry, &View::default())?),
            Vec::<&str>::new()
        );
        for (key, version) in g.nodes("book", &none, &at(29))? {
            assert_eq!(
                g.state_at(&NodeRef::new("book", key), &at(29))?,
                Some(version)
            );
        }
        let titled = BTreeMap::from([("title".to_string(), Value::from("One"))]);
        assert!(
            g.nodes("book", &titled, &View::default()).is_err(),
            "title is not indexed"
        );
        Ok(())
    }

    #[test]
    fn writing_pays_for_an_index_only_where_one_is_declared() -> Result {
        let g = filled("index", &events())?;
        let indexes: Vec<String> = g
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE '_by:%'")?
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(
            indexes,
            ["_by:4:book:genre"],
            "only book's genre is indexed"
        );
        let plan: Vec<String> = g
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                crate::queries::nodes_in_view(&crate::queries::keys_by_property("book", "genre"))
            ))?
            .query_map(
                rusqlite::params!["class:book", None::<Vec<u8>>, None::<i64>, "\"fiction\""],
                |row| row.get(3),
            )?
            .collect::<std::result::Result<_, _>>()?;
        assert!(
            plan.iter().any(|step| step.contains("_by:4:book:genre")),
            "the filter reads through the index: {plan:?}"
        );
        Ok(())
    }

    #[test]
    fn counts_need_no_root() -> Result {
        let g = filled("counts", &events())?;
        let counts = g.counts(&View::default())?;
        assert_eq!(
            counts.nodes,
            BTreeMap::from([("author".into(), 2), ("book".into(), 3)])
        );
        assert_eq!(counts.links, BTreeMap::from([("wrote".into(), 3)]));
        let earlier = g.counts(&at(15))?;
        assert_eq!(earlier.links, BTreeMap::from([("wrote".into(), 4)]));
        assert_eq!(g.counts(&at(11))?.nodes["book"], 3);
        assert_eq!(g.counts(&at(10))?.nodes["book"], 2);
        Ok(())
    }

    #[test]
    fn describe_names_properties_and_relations_both_ways() -> Result {
        let g = filled("describe", &events())?;
        let a1 = NodeRef::new("author", "a1");
        let b1 = NodeRef::new("book", "b1");
        for view in [View::default(), at(15)] {
            let Some(described) = g.describe(&a1, &view)? else {
                return Err("a1 is described".into());
            };
            let out = g.links(&a1, "wrote", Out, &view)?.len() as u64;
            assert_eq!(
                described.links,
                BTreeMap::from([(("wrote".into(), Out), out)])
            );
            assert_eq!(described.version.properties["name"], Value::from("a1"));
        }
        let Some(described) = g.describe(&b1, &View::default())? else {
            return Err("b1 is described".into());
        };
        assert_eq!(described.links, BTreeMap::from([(("wrote".into(), In), 2)]));
        assert_eq!(g.describe(&a1, &at(11))?, None);
        Ok(())
    }

    #[test]
    fn an_unknown_valid_time_is_returned_marked_unknown() -> Result {
        let g = filled("unknown", &events())?;
        let books = g.nodes("book", &BTreeMap::new(), &View::default())?;
        assert_eq!(books[0].0, "b1");
        assert_eq!(books[0].1.valid, ValidTime::Unknown);
        let links = g.links(
            &NodeRef::new("author", "a1"),
            "wrote",
            Out,
            &View::default(),
        )?;
        assert!(
            links
                .iter()
                .all(|(_, link)| link.valid == ValidTime::Unknown)
        );
        Ok(())
    }

    /// Every read, as an answer's data would hold it, at `view`.
    fn every_read(
        g: &Graph,
        view: &View,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let a1 = NodeRef::new("author", "a1");
        let b1 = NodeRef::new("book", "b1");
        let fiction = BTreeMap::from([("genre".to_string(), Value::from("fiction"))]);
        Ok(format!(
            "{:?}\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            g.links(&a1, "wrote", Out, view)?,
            g.links(&b1, "wrote", In, view)?,
            g.nodes("book", &BTreeMap::new(), view)?,
            g.nodes("book", &fiction, view)?,
            g.counts(view)?,
            g.describe(&a1, view)?,
        ))
    }

    #[test]
    fn reads_answer_the_same_after_a_rebuild_and_in_any_arrival_order() -> Result {
        let events = events();
        let ordered = filled("ordered", &events)?;
        let mut reversed = events.clone();
        reversed.reverse();
        let reversed = filled("reversed", &reversed)?;
        let logged: Vec<String> = ordered
            .events()
            .since(crate::Cursor::START)?
            .into_iter()
            .map(|event| event.body)
            .filter(|body| !body.contains("rannoch.declared"))
            .collect();
        let rebuilt = filled("rebuilt", &logged)?;
        for view in [View::default(), at(10), at(13), at(15), at(25)] {
            let expected = every_read(&ordered, &view)?;
            assert_eq!(every_read(&reversed, &view)?, expected, "{view:?}");
            assert_eq!(every_read(&rebuilt, &view)?, expected, "{view:?}");
        }
        Ok(())
    }

    /// An amendment known at `seconds` of the event `target`, patched by `patch`.
    fn amending(
        seconds: u64,
        target: &str,
        patch: &str,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let target = rannoch_cloudevents::attributes(target)?;
        Ok(format!(
            r#"{{"specversion":"1.0","id":"{}","source":"rannoch:fixer","type":"rannoch.amended","data":{{"target":{{"source":"{}","id":"{}"}},"patch":{patch}}}}}"#,
            id_at(seconds),
            target.source,
            target.id
        ))
    }

    #[test]
    fn reads_follow_renames_and_amendments_by_known_at() -> Result {
        let mut events = events();
        events.push(amending(40, &events[1], r#"{"subject":"b9"}"#)?);
        events.push(amending(
            50,
            &events[2],
            r#"{"data":{"properties":{"title":"Three","genre":"poetry"}}}"#,
        )?);
        let g = filled("amended", &events)?;
        let none = BTreeMap::new();
        let poetry = BTreeMap::from([("genre".to_string(), Value::from("poetry"))]);
        assert_eq!(keys(&g.nodes("book", &none, &at(35))?), ["b1", "b2", "b3"]);
        assert_eq!(
            keys(&g.nodes("book", &none, &View::default())?),
            ["b2", "b3", "b9"]
        );
        assert_eq!(
            keys(&g.nodes("book", &poetry, &at(45))?),
            Vec::<&str>::new()
        );
        assert_eq!(keys(&g.nodes("book", &poetry, &View::default())?), ["b3"]);
        let b9 = NodeRef::new("book", "b9");
        assert_eq!(
            keys(&g.links(&b9, "wrote", In, &View::default())?),
            ["a1", "a2"]
        );
        assert_eq!(
            keys(&g.links(&b9, "wrote", In, &at(35))?),
            Vec::<&str>::new()
        );
        assert_eq!(g.counts(&View::default())?.nodes["book"], 3);
        assert_eq!(g.counts(&View::default())?.links["wrote"], 3);
        assert_eq!(
            g.describe(&b9, &View::default())?.map(|d| d.links),
            Some(BTreeMap::from([(("wrote".into(), In), 2)]))
        );
        Ok(())
    }

    fn query(id: &str, data: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{id}","source":"app","type":"rannoch.query","data":{data}}}"#
        )
    }

    #[test]
    fn a_query_event_is_answered_by_an_answer_event() -> Result {
        let mut g = filled("answer", &events())?;
        let asked = query(
            "q1",
            r#"{"links":{"node":{"class":"author","key":"a1"},"relation":"wrote","direction":"out"},"view":{"known":15000}}"#,
        );
        let answer: serde_json::Value = serde_json::from_str(&g.answer(&asked)?)?;
        assert_eq!(answer["type"], "rannoch.answer");
        assert_eq!(answer["subject"], "q1");
        assert_eq!(answer["data"]["known"], 15_000);
        let keys: Vec<&str> = answer["data"]["result"]
            .as_array()
            .map(|rows| rows.iter().filter_map(|row| row["key"].as_str()).collect())
            .unwrap_or_default();
        assert_eq!(keys, ["b1", "b2", "b3"]);
        let b1 = &answer["data"]["result"][0]["version"];
        let expected = g.link_state_at("wrote", "a1", "b1", &at(15))?;
        assert_eq!(expected.map(|v| super::version_json(&v)).as_ref(), Some(b1));
        assert_eq!(b1["valid"], "unknown");

        // With no known-at, the read is at the greatest the store holds: all it knows.
        let counted: serde_json::Value =
            serde_json::from_str(&g.answer(&query("q2", r#"{"counts":{}}"#))?)?;
        assert_eq!(counted["data"]["known"], 30_000);
        assert_eq!(counted["data"]["result"]["links"]["wrote"], 3);
        let described: serde_json::Value = serde_json::from_str(&g.answer(&query(
            "q3",
            r#"{"describe":{"node":{"class":"book","key":"b1"}}}"#,
        ))?)?;
        assert_eq!(described["data"]["result"]["links"][0]["direction"], "in");
        let listed: serde_json::Value = serde_json::from_str(&g.answer(&query(
            "q4",
            r#"{"nodes":{"class":"book","filter":{"genre":"poetry"}},"view":{"known":29000}}"#,
        ))?)?;
        assert_eq!(listed["data"]["result"][0]["key"], "b2");

        // A query and its answer are events like any other: logged and filled into no table.
        let answered = g.answer(&asked)?;
        for landed in g.commit(&[&asked, &answered], &[])? {
            landed?;
        }
        g.fill()?;
        assert_eq!(g.stopped()?, vec![]);
        assert!(g.answer(&book(40, "b9", "Nine", "poetry", "null")).is_err());
        Ok(())
    }
}
