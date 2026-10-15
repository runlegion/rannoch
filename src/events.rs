//! The log: every change to a graph is an event, read in order from a cursor. This module owns
//! rannoch's own tables, whose names start with `_`.

use std::collections::BTreeMap;
use std::fmt;

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::read::NodeRef;
use crate::write::{Declaration, Error, ValidTime};

/// rannoch's own tables: the event log, the ontology, and links waiting for an end.
pub(crate) const OWN_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _events(seq INTEGER PRIMARY KEY, body TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS _ontology(
        kind TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL,
        PRIMARY KEY(kind, name)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _open(
        rel TEXT NOT NULL, from_key TEXT NOT NULL, to_key TEXT NOT NULL,
        source TEXT NOT NULL, valid TEXT,
        PRIMARY KEY(rel, from_key, to_key)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _open_to ON _open(rel, to_key);";

/// A place in a graph's event log. Reading since a cursor returns the events after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor(i64);

impl Cursor {
    /// Before the first event.
    pub const START: Cursor = Cursor(0);
}

/// An event's own id (`UUIDv7`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(Uuid);

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One change to a graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub id: EventId,
    /// The event's place in the log: reading since it returns the events written after it.
    pub cursor: Cursor,
    pub change: Change,
}

/// What an event changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    /// One declaration of a class, a relation or a property.
    Declared(Declaration),
    /// What one piece of a write added, and the wrong-kind arrivals it met.
    Wrote {
        source: String,
        added: Vec<Fact>,
        #[serde(default)]
        wrong_kind: Vec<WrongKind>,
    },
    /// An open link whose missing end arrived: it now holds in its relation.
    Closed(LinkFact),
}

/// One thing a piece added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fact {
    /// A node, or a new version of one.
    Node(NodeFact),
    /// A link between two present nodes.
    Link(LinkFact),
    /// A link to a key not yet present: it waits as an open link and joins no relation.
    Opened(LinkFact),
}

/// A node arrived under the key an open link waits on, but of another kind: the link stays open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrongKind {
    /// The node that arrived.
    pub arrived: NodeRef,
    /// The open link still waiting on its key.
    pub waiting: LinkFact,
}

/// A node as a piece wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFact {
    pub class: String,
    pub key: String,
    pub properties: BTreeMap<String, String>,
    pub valid: ValidTime,
}

/// A link, named by its relation and both of its endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkFact {
    pub relation: String,
    pub from: NodeRef,
    pub to: NodeRef,
    pub valid: ValidTime,
}

#[derive(Serialize, Deserialize)]
struct Stored {
    id: EventId,
    change: Change,
}

/// Appends one event inside the caller's transaction, so it lands only if that transaction does.
pub(crate) fn append(tx: &Connection, change: Change) -> Result<Event, Error> {
    let stored = Stored {
        id: EventId(Uuid::now_v7()),
        change,
    };
    tx.prepare_cached("INSERT INTO _events(body) VALUES (?1)")?
        .execute(params![serde_json::to_string(&stored)?])?;
    Ok(Event {
        id: stored.id,
        cursor: Cursor(tx.last_insert_rowid()),
        change: stored.change,
    })
}

/// A graph's event log.
#[derive(Debug)]
pub struct Events<'g> {
    pub(crate) conn: &'g Connection,
}

impl Events<'_> {
    /// Every event after `cursor`, in the order they were written.
    ///
    /// # Errors
    /// The graph file cannot be read, or an event in it cannot be decoded.
    pub fn since(&self, cursor: Cursor) -> Result<Vec<Event>, Error> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT seq, body FROM _events WHERE seq > ?1 ORDER BY seq")?;
        let mut rows = statement.query(params![cursor.0])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            let stored: Stored = serde_json::from_str(&row.get::<_, String>(1)?)?;
            events.push(Event {
                id: stored.id,
                cursor: Cursor(row.get(0)?),
                change: stored.change,
            });
        }
        Ok(events)
    }
}
