//! The log: every change to a graph is an event, read in order from a cursor, and compacted up
//! to a cursor the app picks. This module owns rannoch's own tables, whose names start with `_`.

use std::collections::BTreeMap;
use std::fmt;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::open::{Graph, Ontology, class_table, relation_table};
use crate::read::NodeRef;
use crate::write::{Declaration, Error, ValidTime};

/// rannoch's own tables: the event log, the ontology, and links waiting for an end. An event's
/// place is never reused, so a cursor stays a place in the log after compaction removes the
/// events up to it.
pub(crate) const OWN_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _events(seq INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS _ontology(
        kind TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL,
        PRIMARY KEY(kind, name)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _open(
        relation TEXT NOT NULL, from_key TEXT NOT NULL, to_key TEXT NOT NULL,
        source TEXT NOT NULL, valid TEXT,
        PRIMARY KEY(relation, from_key, to_key)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _open_to ON _open(relation, to_key);";

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
    /// What one piece of a write added.
    Wrote { source: String, added: Vec<Fact> },
    /// An open link whose missing end arrived: it now holds in its relation.
    Closed(LinkFact),
    /// A node the app forgot under its own rule, and the links that left with it. It names
    /// what it removed by id and kind and holds none of the node's content.
    Forgot {
        source: String,
        rule: String,
        nodes: Vec<NodeRef>,
        links: Vec<LinkFact>,
    },
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

impl Graph {
    /// Compacts the graph's events up to `cursor`, a place the app picks: the events up to it
    /// are deleted, and the state they built is kept as the base. Counts read after compaction
    /// equal the counts before it, and numbers an app folds from the base plus the events after
    /// `cursor` equal live state.
    ///
    /// # Errors
    /// The graph file cannot be written.
    pub fn compact(&mut self, cursor: Cursor) -> Result<(), Error> {
        self.conn
            .prepare_cached("DELETE FROM _events WHERE seq <= ?1")?
            .execute(params![cursor.0])?;
        Ok(())
    }

    /// Forgets `target` under the app's own `rule`, from `source`. The node and its links leave
    /// live state; the earlier events that named it keep its id, its kind and its links'
    /// endpoints and lose its content. Returns the forget event, which names what it removed by
    /// id and kind and holds none of the node's content. All of it lands in one transaction or
    /// none of it does.
    ///
    /// # Errors
    /// [`Error::NotFound`] when no node of the class has the key; [`Error::Refused`] when the
    /// class is not declared; the graph file cannot be read or written.
    pub fn forget(&mut self, target: &NodeRef, rule: &str, source: &str) -> Result<Event, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ontology = Ontology::load(&tx)?;
        let table = class_table(&ontology.class(&target.class)?.name);
        let id: i64 = tx
            .prepare_cached(&format!("SELECT id FROM {table} WHERE key = ?1"))?
            .query_row(params![target.key], |row| row.get(0))
            .optional()?
            .ok_or_else(|| Error::NotFound {
                class: target.class.clone(),
                key: target.key.clone(),
            })?;
        let links = unlink(&tx, &ontology, target, id)?;
        tx.prepare_cached(&format!("DELETE FROM {table} WHERE id = ?1"))?
            .execute(params![id])?;
        strip(&tx, target)?;
        let event = append(
            &tx,
            Change::Forgot {
                source: source.to_string(),
                rule: rule.to_string(),
                nodes: vec![target.clone()],
                links,
            },
        )?;
        self.ontology = ontology;
        tx.commit()?;
        Ok(event)
    }
}

/// Removes every link of the node `id`, which is `target`, from live state, and returns them.
fn unlink(
    tx: &Connection,
    ontology: &Ontology,
    target: &NodeRef,
    id: i64,
) -> Result<Vec<LinkFact>, Error> {
    let mut links = Vec::new();
    for relation in ontology.relations.values() {
        for (end, other, near, far) in [
            (&relation.from, &relation.to, "src", "dst"),
            (&relation.to, &relation.from, "dst", "src"),
        ] {
            if *end != target.class {
                continue;
            }
            let table = relation_table(&relation.name);
            let ends: Vec<(String, Option<String>)> = tx
                .prepare_cached(&format!(
                    "SELECT o.key, r.valid FROM {table} r JOIN {} o ON o.id = r.{far} \
                     WHERE r.{near} = ?1",
                    class_table(other)
                ))?
                .query_map(params![id], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?;
            for (key, valid) in ends {
                let there = NodeRef::new(other, key);
                let (from, to) = if near == "src" {
                    (target.clone(), there)
                } else {
                    (there, target.clone())
                };
                links.push(LinkFact {
                    relation: relation.name.clone(),
                    from,
                    to,
                    valid: ValidTime::from_sql(valid),
                });
            }
            tx.prepare_cached(&format!("DELETE FROM {table} WHERE {near} = ?1"))?
                .execute(params![id])?;
        }
    }
    Ok(links)
}

/// Strips `target`'s content from every earlier event that wrote it, keeping its id and kind.
fn strip(tx: &Connection, target: &NodeRef) -> Result<(), Error> {
    // A coarse filter on the key as the events encode it; each candidate is matched exactly.
    let pattern = format!("%{}%", serde_json::to_string(&target.key)?);
    let candidates: Vec<(i64, String)> = tx
        .prepare_cached("SELECT seq, body FROM _events WHERE body LIKE ?1")?
        .query_map(params![pattern], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    for (seq, body) in candidates {
        let mut stored: Stored = serde_json::from_str(&body)?;
        let Change::Wrote { added, .. } = &mut stored.change else {
            continue;
        };
        let mut stripped = false;
        for fact in added {
            if let Fact::Node(node) = fact
                && node.class == target.class
                && node.key == target.key
                && !node.properties.is_empty()
            {
                node.properties.clear();
                stripped = true;
            }
        }
        if stripped {
            tx.prepare_cached("UPDATE _events SET body = ?1 WHERE seq = ?2")?
                .execute(params![serde_json::to_string(&stored)?, seq])?;
        }
    }
    Ok(())
}
