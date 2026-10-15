//! Opening a graph file, and the ontology its own stream declared.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::events::{Events, OWN_TABLES};
use crate::write::Error;

/// A class: a kind of node, with its natural key and a set of properties.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub name: String,
    pub properties: BTreeSet<String>,
}

impl Class {
    pub fn new<P: Into<String>>(
        name: impl Into<String>,
        properties: impl IntoIterator<Item = P>,
    ) -> Self {
        Class {
            name: name.into(),
            properties: properties.into_iter().map(Into::into).collect(),
        }
    }
}

/// A relation: links from a node of one class to a node of another. A containment relation
/// says its source is part of its target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub from: String,
    pub to: String,
    pub containment: bool,
}

impl Relation {
    pub fn new(
        name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        containment: bool,
    ) -> Self {
        Relation {
            name: name.into(),
            from: from.into(),
            to: to.into(),
            containment,
        }
    }
}

/// A graph's classes and relations, as its stream declared them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ontology {
    pub classes: BTreeMap<String, Class>,
    pub relations: BTreeMap<String, Relation>,
}

impl Ontology {
    pub(crate) fn load(conn: &Connection) -> Result<Ontology, Error> {
        let mut ontology = Ontology::default();
        let mut statement = conn.prepare_cached("SELECT kind, body FROM _ontology")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let body: String = row.get(1)?;
            if row.get::<_, String>(0)? == CLASS {
                let class: Class = serde_json::from_str(&body)?;
                ontology.classes.insert(class.name.clone(), class);
            } else {
                let relation: Relation = serde_json::from_str(&body)?;
                ontology.relations.insert(relation.name.clone(), relation);
            }
        }
        Ok(ontology)
    }

    pub(crate) fn class(&self, name: &str) -> Result<&Class, Error> {
        self.classes
            .get(name)
            .ok_or_else(|| Error::Refused(format!("class {name} is not declared")))
    }

    pub(crate) fn relation(&self, name: &str) -> Result<&Relation, Error> {
        self.relations
            .get(name)
            .ok_or_else(|| Error::Refused(format!("relation {name} is not declared")))
    }
}

pub(crate) const CLASS: &str = "class";
pub(crate) const RELATION: &str = "relation";

/// Each item formatted by `each` and joined into one string.
pub(crate) fn concat<T>(items: impl IntoIterator<Item = T>, each: impl Fn(T) -> String) -> String {
    items.into_iter().map(each).collect::<Vec<_>>().concat()
}

/// A quoted SQL identifier.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The table that holds a class's nodes. Its name differs from the class's own name, so no
/// answer in ontology terms ever names it.
pub(crate) fn class_table(class: &str) -> String {
    ident(&format!("{CLASS}:{class}"))
}

/// The table that holds a relation's links.
pub(crate) fn relation_table(relation: &str) -> String {
    ident(&format!("{RELATION}:{relation}"))
}

/// The index over a relation's links by their target.
pub(crate) fn relation_index(relation: &str) -> String {
    ident(&format!("{RELATION}:{relation}:to"))
}

/// The column that holds a property. Prefixed, so a property never meets a column of rannoch's.
pub(crate) fn property_column(property: &str) -> String {
    ident(&format!("p:{property}"))
}

/// While another connection holds the file's write lock, a connection retries every millisecond,
/// for the same five seconds the default handler waits, so it takes the lock in the gap a write
/// leaves between pieces. The default handler backs off to 100 ms and misses that gap.
fn retry_lock(attempts: i32) -> bool {
    std::thread::sleep(Duration::from_millis(1));
    attempts < 5_000
}

/// One graph file.
#[derive(Debug)]
pub struct Graph {
    pub(crate) conn: Connection,
    pub(crate) ontology: Ontology,
}

/// Opens the graph the file at `path` holds, with the ontology its own stream declared. A path
/// that holds no graph yet starts an empty graph.
///
/// # Errors
/// The file cannot be opened as a graph file.
pub fn open(path: impl AsRef<Path>) -> Result<Graph, Error> {
    let conn = Connection::open(path)?;
    conn.busy_handler(Some(retry_lock))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;",
    )?;
    conn.set_prepared_statement_cache_capacity(256);
    conn.execute_batch(OWN_TABLES)?;
    let ontology = Ontology::load(&conn)?;
    Ok(Graph { conn, ontology })
}

impl Graph {
    /// The graph's ontology: every class and relation declared on its stream.
    #[must_use]
    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    /// The graph's event log.
    #[must_use]
    pub fn events(&self) -> Events<'_> {
        Events { conn: &self.conn }
    }
}
