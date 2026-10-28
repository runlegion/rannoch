//! Reading the graph as it is now.

use std::collections::BTreeMap;
use std::time::SystemTime;

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::history::{At, Versioned, newest, node_as_of, node_entity, time};
use crate::queries::select_node;
use crate::{Class, Date, Error, Graph, Ontology, ValidTime, Value};

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
        Ok(node_in(
            class,
            key,
            node_as_of(&self.conn, class, key, &day, known_at)?,
        ))
    }
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
        .prepare_cached(&select_node(&class.name, &[]))?
        .query_row(params![key], |row| {
            Ok(Node {
                class: class.name.clone(),
                key: key.to_string(),
                properties: BTreeMap::new(),
                source: row.get(0)?,
                valid: ValidTime::from_sql(row.get(1)?),
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
