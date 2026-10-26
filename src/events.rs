//! The log: every change to a graph is an event, read in order from a cursor and kept; a
//! snapshot of state at a cursor the app picks is where a new reader or a rebuild starts. This
//! module owns rannoch's own tables, whose names start with `_`.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::history::{Key, aliases, hold_aliases, unhear};
use crate::open::{Graph, Ontology, Value, class_table, open_keyed, relation_table};
use crate::purge::Purge;
use crate::read::NodeRef;
use crate::write::{Declaration, Error, ValidTime, now_millis, replay};

/// rannoch's own tables: the event log, the ontology, links waiting for an end, the fates of
/// nodes that were replaced, the events each node was derived from, the registered followers
/// with their stored cursors, and every snapshot the store made, so a purge reaches it. An
/// event's place is never reused, so a cursor stays a place in the log.
pub(crate) const OWN_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _events(seq INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS _ontology(
        kind TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL,
        PRIMARY KEY(kind, name)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _open(
        rel TEXT NOT NULL, from_key TEXT NOT NULL, to_key TEXT NOT NULL,
        source TEXT NOT NULL, valid TEXT,
        PRIMARY KEY(rel, from_key, to_key)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _open_to ON _open(rel, to_key);
    CREATE TABLE IF NOT EXISTS _fates(
        class TEXT NOT NULL, key TEXT NOT NULL, replaced_by TEXT NOT NULL,
        point INTEGER, event TEXT, PRIMARY KEY(class, key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _derived(
        class TEXT NOT NULL, key TEXT NOT NULL, event TEXT NOT NULL,
        PRIMARY KEY(class, key, event)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _derived_event ON _derived(event);
    CREATE TABLE IF NOT EXISTS _followers(name TEXT PRIMARY KEY, cursor INTEGER NOT NULL)
        WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _snapshots(path TEXT PRIMARY KEY, cursor INTEGER);";

/// A place in a graph's event log. Reading since a cursor returns the events after it. An app
/// stores a cursor by serializing it, and a graph rebuilt from the log keeps each event's place,
/// so a stored cursor names the same place in the rebuilt graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(pub(crate) i64);

impl Cursor {
    /// Before the first event.
    pub const START: Cursor = Cursor(0);
}

/// An event's own id (`UUIDv7`), minted when the event is written and never derived from its
/// content. Ids order as they were minted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(Uuid);

impl EventId {
    /// Mints a new id. `Uuid::now_v7` is monotonic within one process: within a millisecond it
    /// fills the rest of the timestamp field with a counter, so ids minted later sort later.
    /// Pinned by `ids_minted_by_one_process_order_as_minted`.
    pub(crate) fn now() -> EventId {
        EventId(Uuid::now_v7())
    }

    /// The millisecond the id was minted at: when the store that minted it recorded its event.
    pub(crate) fn millis(self) -> i64 {
        self.0.get_timestamp().map_or(0, |at| {
            let (seconds, nanos) = at.to_unix();
            i64::try_from(seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000)
                .saturating_add(i64::from(nanos / 1_000_000))
        })
    }
}

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
        /// The check value of the content-identity key the writing store held, so a rebuild
        /// under another key is refused. Events written before keys hold none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        check: Option<String>,
    },
    /// An open link whose missing end arrived: it now holds in its relation.
    Closed(LinkFact),
    /// A node the app forgot under its own rule, and the links that left with it, as logs
    /// written before purges hold it: it is read and replayed as that node's purge, and no longer
    /// written.
    Forgot {
        source: String,
        rule: String,
        nodes: Vec<NodeRef>,
        links: Vec<LinkFact>,
    },
    /// A purge under the app's own `rule`, from `source`: its kind, with each target as its
    /// keyed match value under the graph's key, never as content.
    Purged {
        source: String,
        rule: String,
        purge: Purge,
    },
    /// The node `old` of `class` was replaced by the node `new` of the same class.
    Replaced {
        class: String,
        old: String,
        new: String,
    },
    /// The link of `relation` from `from` to `to` ended at its own point `at`, in milliseconds
    /// since the Unix epoch (UTC), and stopped holding in the world at `valid`, which may be
    /// unknown. Its hearings stay.
    Ended {
        source: String,
        relation: String,
        from: NodeRef,
        to: NodeRef,
        at: i64,
        valid: ValidTime,
    },
    /// The link of `relation` from `from` to `to` was found wrong: a correction, not an ending.
    Corrected {
        source: String,
        relation: String,
        from: NodeRef,
        to: NodeRef,
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
    pub properties: BTreeMap<String, Value>,
    pub valid: ValidTime,
    /// The time its writer supplied, in milliseconds since the Unix epoch (UTC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<i64>,
    /// The integer key its writer supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integer: Option<i64>,
    /// The events the node was derived from.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub derived_from: Vec<EventId>,
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
pub(crate) struct Stored {
    pub(crate) id: EventId,
    pub(crate) change: Change,
}

/// Appends one event inside the caller's transaction, so it lands only if that transaction does.
pub(crate) fn append(tx: &Connection, change: Change) -> Result<Event, Error> {
    store(tx, EventId::now(), None, change)
}

/// The place the next event appended to the log takes.
pub(crate) fn next_cursor(tx: &Connection) -> Result<Cursor, Error> {
    Ok(Cursor(high_water(tx)?.0 + 1))
}

/// The highest place the log has ever given an event, or [`Cursor::START`] when it has given
/// none: `_events` is `AUTOINCREMENT`, so SQLite keeps it and never lowers it when an event is
/// deleted.
fn high_water(conn: &Connection) -> Result<Cursor, Error> {
    Ok(Cursor(
        conn.prepare_cached(
            "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = '_events'), 0)",
        )?
        .query_row([], |row| row.get(0))?,
    ))
}

/// The event at `cursor`, if the log holds one there.
pub(crate) fn event_at(conn: &Connection, cursor: Cursor) -> Result<Option<Event>, Error> {
    let body: Option<String> = conn
        .prepare_cached("SELECT body FROM _events WHERE seq = ?1")?
        .query_row(params![cursor.0], |row| row.get(0))
        .optional()?;
    body.map(|body| {
        let stored: Stored = serde_json::from_str(&body)?;
        Ok(Event {
            id: stored.id,
            cursor,
            change: stored.change,
        })
    })
    .transpose()
}

/// Appends one event under the id it already has, inside the caller's transaction: at `place`
/// when it has one, otherwise at the end of the log.
pub(crate) fn store(
    tx: &Connection,
    id: EventId,
    place: Option<Cursor>,
    change: Change,
) -> Result<Event, Error> {
    let stored = Stored { id, change };
    tx.prepare_cached("INSERT INTO _events(seq, body) VALUES (?1, ?2)")?
        .execute(params![
            place.map(|cursor| cursor.0),
            serde_json::to_string(&stored)?
        ])?;
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
    /// The committed watermark: the highest place the log has given a committed event, or
    /// [`Cursor::START`] when it has given none. It never moves back, not even when a purge
    /// removes the last event. Another process on the same file learns of a commit by reading
    /// it, then reads [`Events::since_until`] its own cursor up to it; an event not yet
    /// committed is never under it.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn committed(&self) -> Result<Cursor, Error> {
        high_water(self.conn)
    }

    /// Every event after `cursor`, in the order they were written.
    ///
    /// # Errors
    /// The graph file cannot be read, or an event in it cannot be decoded.
    pub fn since(&self, cursor: Cursor) -> Result<Vec<Event>, Error> {
        self.since_until(cursor, Cursor(i64::MAX), |_| true)
    }

    /// The events after `cursor` and up to `until`, `until` included, whose change `keep`
    /// holds for, in the order they were written. No event past `until` is read.
    ///
    /// # Errors
    /// The graph file cannot be read, or an event in it cannot be decoded.
    pub fn since_until(
        &self,
        cursor: Cursor,
        until: Cursor,
        keep: impl Fn(&Change) -> bool,
    ) -> Result<Vec<Event>, Error> {
        let mut statement = self.conn.prepare_cached(
            "SELECT seq, body FROM _events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq",
        )?;
        let mut rows = statement.query(params![cursor.0, until.0])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            let stored: Stored = serde_json::from_str(&row.get::<_, String>(1)?)?;
            if keep(&stored.change) {
                events.push(Event {
                    id: stored.id,
                    cursor: Cursor(row.get(0)?),
                    change: stored.change,
                });
            }
        }
        Ok(events)
    }

    /// Registers the follower `name`. A follower that has stored no cursor holds the processed
    /// mark at [`Cursor::START`]; registering a name already registered keeps its cursor.
    ///
    /// # Errors
    /// The graph file cannot be written.
    pub fn register(&self, name: &str) -> Result<(), Error> {
        self.conn
            .prepare_cached("INSERT OR IGNORE INTO _followers(name, cursor) VALUES (?1, ?2)")?
            .execute(params![name, Cursor::START.0])?;
        Ok(())
    }

    /// Stores `cursor` as what the registered follower `name` has read. It may move back.
    ///
    /// # Errors
    /// [`Error::Refused`] when `name` is not registered, or when `cursor` is past the committed
    /// watermark ([`Events::committed`]), so the mark never covers an event that has not arrived;
    /// the graph file cannot be read or written.
    pub fn advance(&self, name: &str, cursor: Cursor) -> Result<(), Error> {
        let committed = self.committed()?;
        if cursor > committed {
            return Err(Error::Refused(format!(
                "cursor {} is past the committed watermark {}",
                cursor.0, committed.0
            )));
        }
        let changed = self
            .conn
            .prepare_cached("UPDATE _followers SET cursor = ?2 WHERE name = ?1")?
            .execute(params![name, cursor.0])?;
        if changed == 0 {
            return Err(Error::Refused(format!(
                "no follower named {name} is registered"
            )));
        }
        Ok(())
    }

    /// Unregisters the follower `name`, releasing its hold on the processed mark.
    ///
    /// # Errors
    /// The graph file cannot be written.
    pub fn unregister(&self, name: &str) -> Result<(), Error> {
        self.conn
            .prepare_cached("DELETE FROM _followers WHERE name = ?1")?
            .execute(params![name])?;
        Ok(())
    }

    /// Every registered follower and its stored cursor, by name.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn followers(&self) -> Result<Vec<(String, Cursor)>, Error> {
        Ok(self
            .conn
            .prepare_cached("SELECT name, cursor FROM _followers ORDER BY name")?
            .query_map([], |row| Ok((row.get(0)?, Cursor(row.get(1)?))))?
            .collect::<Result<_, _>>()?)
    }

    /// The processed mark: the lowest stored cursor across every registered follower. Every
    /// event at or before it has been read by every registered follower. `None` when no follower
    /// is registered. It deletes nothing.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn processed(&self) -> Result<Option<Cursor>, Error> {
        Ok(self
            .conn
            .prepare_cached("SELECT MIN(cursor) FROM _followers")?
            .query_row([], |row| row.get::<_, Option<i64>>(0))?
            .map(Cursor))
    }
}

/// Rebuilds a graph file at `empty_path` from `events` alone, under the graph's content-identity
/// `key`, replaying them in order: its ontology from the declaration events, and its nodes, links,
/// open links and hearings from the rest. Each event lands in the new file under its own id and
/// at its own place in the log, except one whose every fact is identical to a hearing already
/// replayed, whose id is held as an alias of it. The events land together or not at all.
///
/// # Errors
/// [`Error::KeyMissing`] without a key; [`Error::KeyDiffers`] when an event was written under
/// another key; [`Error::Refused`] naming the event when an event carries a property its class
/// does not declare; the file cannot be opened as a graph file, or an event cannot be replayed
/// into it.
pub fn rebuild(
    events: &[Event],
    empty_path: impl AsRef<Path>,
    key: Option<&[u8; 32]>,
) -> Result<Graph, Error> {
    let key = Key::new(key.ok_or(Error::KeyMissing)?);
    replay_into(open_keyed(empty_path, &key)?, events)
}

/// Replays `events` into `graph` in one transaction, storing each one under its own place in the
/// log, so a cursor names the same event in the new file as in the source.
fn replay_into(mut graph: Graph, events: &[Event]) -> Result<Graph, Error> {
    let key = graph.key.clone().ok_or(Error::KeyMissing)?;
    let tx = graph
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut ontology = Ontology::load(&tx)?;
    let recorded = now_millis();
    for event in events {
        if let Change::Wrote { added, check, .. } = &event.change {
            if check.as_ref().is_some_and(|check| *check != key.check) {
                return Err(Error::KeyDiffers);
            }
            for node in added.iter().filter_map(|fact| match fact {
                Fact::Node(node) => Some(node),
                _ => None,
            }) {
                let class = ontology.class(&node.class)?;
                if let Some(property) = node
                    .properties
                    .keys()
                    .find(|p| !class.properties.contains_key(*p))
                {
                    return Err(Error::Refused(format!(
                        "event {} carries property {property}, which class {} does not declare",
                        event.id, class.name
                    )));
                }
            }
        }
        let added = replay(&tx, &ontology, &key, recorded, event)?;
        if matches!(event.change, Change::Declared(_)) {
            ontology = Ontology::load(&tx)?;
        }
        if added {
            store(&tx, event.id, Some(event.cursor), event.change.clone())?;
        }
    }
    tx.commit()?;
    graph.ontology = ontology;
    Ok(graph)
}

/// A graph's state at a cursor, in a file of its own: its ontology, nodes, links, open links,
/// every version row and hearing, each node's fate, the registered followers and every event up
/// to the cursor under its own id. A new reader or a rebuild starts from it and takes only the
/// events after its cursor, late ones included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    cursor: Cursor,
    path: PathBuf,
}

impl Snapshot {
    /// The place in the source graph's log the snapshot was taken at: a reader starting from
    /// the snapshot reads the source's events since it.
    #[must_use]
    pub fn cursor(&self) -> Cursor {
        self.cursor
    }

    /// Rebuilds a graph file at `empty_path` from the snapshot and `later`, the source's events
    /// after the snapshot's cursor, replayed in order and kept in the new file's log, under the
    /// graph's content-identity `key`. The snapshot file is left as it was.
    ///
    /// # Errors
    /// [`Error::KeyMissing`] without a key; [`Error::KeyDiffers`] when the snapshot or an event
    /// was written under another key; [`Error::Refused`] when `empty_path` is not UTF-8; the
    /// snapshot cannot be copied, the file cannot be opened as a graph file, or an event cannot
    /// be replayed into it.
    pub fn rebuild(
        &self,
        later: &[Event],
        empty_path: impl AsRef<Path>,
        key: Option<&[u8; 32]>,
    ) -> Result<Graph, Error> {
        let key = Key::new(key.ok_or(Error::KeyMissing)?);
        let target = path_text(empty_path.as_ref())?;
        Connection::open(&self.path)?.execute("VACUUM INTO ?1", params![target])?;
        let graph = replay_into(open_keyed(empty_path, &key)?, later)?;
        // A carried follower cursor past what the rebuilt graph holds is lowered to its
        // watermark, so the mark never claims an event the rebuilt graph has not read.
        let committed = graph.events().committed()?;
        graph
            .conn
            .prepare_cached("UPDATE _followers SET cursor = ?1 WHERE cursor > ?1")?
            .execute(params![committed.0])?;
        Ok(graph)
    }
}

/// `path` as text, refused when it is not UTF-8.
fn path_text(path: &Path) -> Result<&str, Error> {
    path.to_str()
        .ok_or_else(|| Error::Refused(format!("the path {} is not UTF-8", path.display())))
}

/// Whether the log holds the event `id`.
pub(crate) fn holds(conn: &Connection, id: EventId) -> Result<bool, Error> {
    Ok(naming_key(conn, &id.to_string())?
        .iter()
        .any(|(_, stored)| stored.id == id))
}

/// Keeps `node`'s derived-from events as the events it was derived from now. Writing a node and
/// replaying its write both keep them through here.
pub(crate) fn keep_derived(tx: &Connection, node: &NodeFact) -> Result<(), Error> {
    drop_derived(tx, &node.class, &node.key)?;
    add_derived(tx, node)?;
    Ok(())
}

/// Adds `node`'s derived-from events to those it already names. Returns whether any was new.
pub(crate) fn add_derived(tx: &Connection, node: &NodeFact) -> Result<bool, Error> {
    let mut added = false;
    for event in &node.derived_from {
        added |= tx
            .prepare_cached(
                "INSERT OR IGNORE INTO _derived(class, key, event) VALUES (?1, ?2, ?3)",
            )?
            .execute(params![node.class, node.key, event.to_string()])?
            > 0;
    }
    Ok(added)
}

/// Removes every derived-from event the node `key` of `class` names.
pub(crate) fn drop_derived(tx: &Connection, class: &str, key: &str) -> Result<(), Error> {
    tx.prepare_cached("DELETE FROM _derived WHERE class = ?1 AND key = ?2")?
        .execute(params![class, key])?;
    Ok(())
}

impl Graph {
    /// The nodes derived from the event `event`, by class and key.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn derived(&self, event: EventId) -> Result<Vec<NodeRef>, Error> {
        Ok(self
            .conn
            .prepare_cached("SELECT class, key FROM _derived WHERE event = ?1 ORDER BY class, key")?
            .query_map(params![event.to_string()], |row| {
                Ok(NodeRef::new(
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                ))
            })?
            .collect::<Result<_, _>>()?)
    }

    /// The cursor of the last event in the log, or [`Cursor::START`] when it holds none.
    pub(crate) fn last_cursor(&self) -> Result<Cursor, Error> {
        self.events().committed()
    }

    /// Takes a snapshot of the graph's state at `cursor`, a place the app picks, into a new file
    /// at `empty_path`: every event up to the cursor replayed into it and kept in its log under
    /// its own id and place, so it holds every version row, each node's and link's hearings and
    /// every event id up to the cursor. It deletes nothing: every event stays in the store. It
    /// reads this graph in one read transaction and fills the new file through its own
    /// connection; the one write it makes on this graph, once the new file is whole, records the
    /// snapshot so a purge reaches it.
    ///
    /// # Errors
    /// The graph file cannot be read, the new file cannot be opened as a graph file, or an
    /// event cannot be replayed into it.
    pub fn snapshot(
        &self,
        cursor: Cursor,
        empty_path: impl AsRef<Path>,
    ) -> Result<Snapshot, Error> {
        let key = self.key.as_ref().ok_or(Error::KeyMissing)?;
        let read = self.conn.unchecked_transaction()?;
        let events = Events { conn: &read }.since_until(Cursor::START, cursor, |_| true)?;
        let followers = Events { conn: &read }.followers()?;
        // An alias is not an event in the log, so replaying the log does not bring it back. It
        // holds no place in the log either: every alias of a hearing the snapshot holds is
        // carried, one that arrived after the cursor included.
        let aliases = aliases(&read)?;
        drop(read);
        // Nothing reads the new file until this returns, so it is filled under a rollback
        // journal, which writes each page once, and goes back to WAL at the end: filled in WAL,
        // every page was written twice on the disk the graph's own commits share.
        let fresh = open_keyed(&empty_path, key)?;
        fresh
            .conn
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                row.get::<_, String>(0)
            })?;
        let snapshot = replay_into(fresh, &events)?;
        hold_aliases(&snapshot.conn, &aliases)?;
        // Registrations are state about readers, not events: the snapshot carries them. A
        // cursor past the snapshot's names a place in this graph's log the snapshot does not
        // hold, so they are copied as they are rather than lowered to its watermark.
        for (name, at) in followers {
            snapshot
                .conn
                .prepare_cached("INSERT INTO _followers(name, cursor) VALUES (?1, ?2)")?
                .execute(params![name, at.0])?;
        }
        // The snapshot's watermark is at least its cursor, even when no event is at that place
        // (a purge removed it), so a rebuild from it never reports less than it holds.
        let raised = snapshot.conn.execute(
            "UPDATE sqlite_sequence SET seq = ?1 WHERE name = '_events' AND seq < ?1",
            params![cursor.0],
        )?;
        if raised == 0 && high_water(&snapshot.conn)? < cursor {
            snapshot.conn.execute(
                "INSERT INTO sqlite_sequence(name, seq) VALUES ('_events', ?1)",
                params![cursor.0],
            )?;
        }
        snapshot
            .conn
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))?;
        // Recorded so a purge reaches it, by its full path: the one write this takes on this
        // graph, once the new file is whole.
        let recorded = std::fs::canonicalize(&empty_path)
            .unwrap_or_else(|_| empty_path.as_ref().to_path_buf());
        self.conn
            .prepare_cached("INSERT OR REPLACE INTO _snapshots(path, cursor) VALUES (?1, ?2)")?
            .execute(params![path_text(&recorded)?, cursor.0])?;
        Ok(Snapshot {
            cursor,
            path: empty_path.as_ref().to_path_buf(),
        })
    }
}

/// Removes `target`, its links and the events it was derived from from live state and returns
/// the links, or `None` when no node of its class has its key. Purging a node and replaying a
/// forget both remove through here.
pub(crate) fn remove(
    tx: &Connection,
    ontology: &Ontology,
    target: &NodeRef,
) -> Result<Option<Vec<LinkFact>>, Error> {
    let table = class_table(&ontology.class(&target.class)?.name);
    let Some(id) = tx
        .prepare_cached(&format!("SELECT id FROM {table} WHERE key = ?1"))?
        .query_row(params![target.key], |row| row.get::<_, i64>(0))
        .optional()?
    else {
        return Ok(None);
    };
    let links = unlink(tx, ontology, target, id)?;
    tx.prepare_cached(&format!("DELETE FROM {table} WHERE id = ?1"))?
        .execute(params![id])?;
    drop_derived(tx, &target.class, &target.key)?;
    unhear(tx, target, &links)?;
    Ok(Some(links))
}

/// Removes every link of the node `id`, which is `target`, from live state, open links waiting
/// on its other end included, and returns them.
pub(crate) fn unlink(
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
            let (mine, theirs) = if near == "src" {
                ("from_key", "to_key")
            } else {
                ("to_key", "from_key")
            };
            let mut ends: Vec<(String, Option<String>)> = tx
                .prepare_cached(&format!(
                    "SELECT o.key, r.valid FROM {table} r JOIN {} o ON o.id = r.{far} \
                     WHERE r.{near} = ?1",
                    class_table(other)
                ))?
                .query_map(params![id], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?;
            ends.extend(
                tx.prepare_cached(&format!(
                    "SELECT {theirs}, valid FROM _open WHERE rel = ?1 AND {mine} = ?2"
                ))?
                .query_map(params![relation.name, target.key], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?,
            );
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
            tx.prepare_cached(&format!("DELETE FROM _open WHERE rel = ?1 AND {mine} = ?2"))?
                .execute(params![relation.name, target.key])?;
        }
    }
    Ok(links)
}

/// Every event whose encoding holds `key`, in log order, with its place in the log. A coarse
/// filter on the key as the events encode it: the caller matches each candidate exactly.
pub(crate) fn naming_key(conn: &Connection, key: &str) -> Result<Vec<(i64, Stored)>, Error> {
    let pattern = format!("%{}%", serde_json::to_string(key)?);
    let rows: Vec<(i64, String)> = conn
        .prepare_cached("SELECT seq, body FROM _events WHERE body LIKE ?1 ORDER BY seq")?
        .query_map(params![pattern], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    rows.into_iter()
        .map(|(seq, body)| Ok((seq, serde_json::from_str(&body)?)))
        .collect()
}

/// Forgets `target` as stores did before purges: it leaves live state and a forget event
/// names it, while earlier events keep its writes. Tests reach the old logs' replay through here.
#[cfg(test)]
pub(crate) fn forget_as_before(graph: &mut Graph, target: &NodeRef) -> Result<Event, Error> {
    let tx = graph.conn.transaction()?;
    let ontology = Ontology::load(&tx)?;
    let links = remove(&tx, &ontology, target)?.unwrap_or_default();
    let event = append(
        &tx,
        Change::Forgot {
            source: "tree".into(),
            rule: "living".into(),
            nodes: vec![target.clone()],
            links,
        },
    )?;
    tx.commit()?;
    Ok(event)
}

#[cfg(test)]
mod tests {
    use crate::{
        Change, Class, Date, Declaration, NodeRef, Purge, Relation, Target, Unit, ValidTime, Write,
        open,
    };

    #[test]
    fn ids_minted_by_one_process_order_as_minted() {
        let mut before = super::EventId::now();
        for _ in 0..100_000 {
            let next = super::EventId::now();
            assert!(
                next > before,
                "{next} was minted after {before} but does not sort after it"
            );
            before = next;
        }
    }

    #[test]
    fn advancing_past_the_committed_watermark_is_refused_naming_both()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-past-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("person", ["name"])))?;
        g.events().register("reader")?;
        let committed = g.events().committed()?;
        g.events().advance("reader", committed)?;
        let past: crate::Cursor = serde_json::from_str("1000")?;
        let Err(crate::Error::Refused(message)) = g.events().advance("reader", past) else {
            panic!("a cursor past the committed watermark was stored");
        };
        assert!(
            message.contains("1000") && message.contains(&serde_json::to_string(&committed)?),
            "{message}"
        );
        assert_eq!(g.events().processed()?, Some(committed));
        Ok(())
    }

    #[test]
    fn registrations_are_carried_by_a_snapshot_and_not_by_a_rebuild_from_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = |name: &str| {
            std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()))
        };
        let mut g = open(temp("followers"), Some(&crate::TEST_KEY))?;
        assert_eq!(g.events().processed()?, None, "no follower, no mark");
        assert!(matches!(
            g.events().advance("typo", crate::Cursor::START),
            Err(crate::Error::Refused(_))
        ));
        g.events().register("reader")?;
        g.declare(&Declaration::Class(Class::new("person", ["name"])))?;
        let at = g.events().committed()?;
        g.declare(&Declaration::Class(Class::new("place", ["name"])))?;
        let last = g.events().committed()?;
        g.events().advance("reader", last)?;
        let snapshot = g.snapshot(at, temp("followers-snapshot"))?;

        // With every later event, the carried cursor names a place the rebuilt graph holds.
        let full = snapshot.rebuild(
            &g.events().since(at)?,
            temp("followers-full"),
            Some(&crate::TEST_KEY),
        )?;
        assert_eq!(
            full.events().followers()?,
            vec![("reader".to_string(), last)]
        );

        // With none, it is lowered to the rebuilt graph's watermark.
        let short = snapshot.rebuild(&[], temp("followers-short"), Some(&crate::TEST_KEY))?;
        let watermark = short.events().committed()?;
        assert!(watermark < last);
        assert_eq!(
            short.events().followers()?,
            vec![("reader".to_string(), watermark)]
        );
        assert!(short.events().processed()? <= Some(watermark));
        short.events().advance("reader", watermark)?;

        let rebuilt = crate::rebuild(
            &g.events().since(crate::Cursor::START)?,
            temp("followers-rebuilt"),
            Some(&crate::TEST_KEY),
        )?;
        assert!(rebuilt.events().followers()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_purge_takes_the_nodes_open_links_with_it() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-forget-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("person", ["name"])))?;
        g.declare(&Declaration::Class(Class::new("place", ["name"])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "born_in", "person", "place", false,
        )))?;
        let when = ValidTime::On(Date::day(1975, 2, 10));
        let fiona = || Unit::new("person", "p05", [("name", "Fiona")], when.clone());
        g.write(&Write::new("tree").unit(fiona().link("born_in", "p05", "pl-late", when.clone())))?;

        let target = Target::Node(NodeRef::new("person", "p05"));
        g.purge(Purge::Entity { target }, "living", "tree")?;
        assert!(
            g.open_links()?.is_empty(),
            "no open link names the purged person"
        );

        g.write(&Write::new("tree").unit(Unit::new(
            "place",
            "pl-late",
            [("name", "Lochend")],
            when.clone(),
        )))?;
        let back = g.write(&Write::new("tree").unit(fiona()))?;
        assert!(
            back.iter().all(|e| !matches!(e.change, Change::Closed(_))),
            "a link the purge removed never closes onto the returning key: {back:?}"
        );
        Ok(())
    }

    #[test]
    fn a_rebuild_refuses_an_event_carrying_an_undeclared_property_and_names_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = |name: &str| {
            std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()))
        };
        let mut g = open(temp("undeclared"), Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("person", ["name"])))?;
        g.write(&Write::new("tree").unit(Unit::new(
            "person",
            "p01",
            [("name", "Ana")],
            ValidTime::Unknown,
        )))?;
        let mut events = g.events().since(crate::Cursor::START)?;
        let last = events.len() - 1;
        let Change::Wrote { added, .. } = &mut events[last].change else {
            panic!("expected a write event, got {:?}", events[last].change);
        };
        let Some(crate::Fact::Node(node)) = added.first_mut() else {
            panic!("expected a node fact, got {added:?}");
        };
        node.properties.insert("age".to_string(), "40".into());

        let Err(error) =
            crate::rebuild(&events, temp("undeclared-rebuilt"), Some(&crate::TEST_KEY))
        else {
            panic!("the rebuild dropped the undeclared property instead of failing");
        };
        let message = error.to_string();
        assert!(
            message.contains(&events[last].id.to_string()) && message.contains("age"),
            "{message}"
        );
        Ok(())
    }

    #[test]
    fn a_snapshot_keeps_the_aliases_its_hearings_hold() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let (g, _) = two_notes()?;
        // The same write arriving again from another store under a larger id: an alias.
        let mut events = g.events().since(crate::Cursor::START)?;
        let mut again = events[events.len() - 1].clone();
        again.id = super::EventId::now();
        again.cursor = crate::Cursor(again.cursor.0 + 1);
        events.push(again.clone());
        let live = crate::rebuild(&events, dir.path().join("live.db"), Some(&crate::TEST_KEY))?;
        let n2 = NodeRef::new("note", "n2");
        assert_eq!(live.hearings(&n2)?[0].aliases, [again.id]);

        let at = live.events().committed()?;
        let snapshot = live.snapshot(at, dir.path().join("snapshot.db"))?;
        let back = snapshot.rebuild(&[], dir.path().join("back.db"), Some(&crate::TEST_KEY))?;
        assert_eq!(back.hearings(&n2)?, live.hearings(&n2)?);
        Ok(())
    }

    #[test]
    fn an_identical_rewrite_on_a_graph_rebuilt_from_a_snapshot_returns_the_hearing_held()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = open(dir.path().join("live.db"), Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("note", ["text"])))?;
        let first = g.write(&Write::new("app").unit(note("n1")))?;
        let at = g.events().committed()?;
        let snapshot = g.snapshot(at, dir.path().join("snapshot.db"))?;
        // Nothing is replayed: the hearing's event reaches the rebuilt graph by the snapshot alone.
        let mut back = snapshot.rebuild(&[], dir.path().join("back.db"), Some(&crate::TEST_KEY))?;

        let on_source = g.write(&Write::new("app").unit(note("n1")))?;
        assert_eq!(on_source, first);
        let before = back.events().committed()?;
        let on_back = back.write(&Write::new("app").unit(note("n1")))?;
        assert_eq!(on_back, on_source);
        assert_eq!(back.events().committed()?, before);
        Ok(())
    }

    #[test]
    fn a_snapshot_is_taken_while_another_connection_holds_the_write_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("live.db");
        let mut g = open(&path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("note", ["text"])))?;
        g.write(&Write::new("app").unit(note("n1")))?;
        let writer = rusqlite::Connection::open(&path)?;
        writer.execute_batch("BEGIN IMMEDIATE")?;
        // The snapshot reads and fills its file under the lock; recording it waits for the lock.
        let held = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            writer.execute_batch("ROLLBACK")
        });
        let at = g.events().committed()?;
        let snapshot = g.snapshot(at, dir.path().join("snapshot.db"))?;
        held.join().map_err(|_| "the writer panicked")??;
        let back = snapshot.rebuild(&[], dir.path().join("back.db"), Some(&crate::TEST_KEY))?;
        assert!(back.find("note", "n1")?.is_some());
        Ok(())
    }

    /// A graph with class note, and notes n1 and n2 each written as its own event.
    fn two_notes() -> Result<(crate::Graph, Vec<crate::Event>), Box<dyn std::error::Error>> {
        let path =
            std::env::temp_dir().join(format!("rannoch-derived-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("note", ["text"])))?;
        let mut sources = Vec::new();
        for key in ["n1", "n2"] {
            sources.extend(g.write(&Write::new("app").unit(note(key)))?);
        }
        Ok((g, sources))
    }

    fn note(key: &str) -> Unit {
        Unit::new("note", key, [("text", "words")], ValidTime::Unknown)
    }

    #[test]
    fn an_identical_rewrite_naming_new_sources_keeps_them() -> Result<(), Box<dyn std::error::Error>>
    {
        let (mut g, sources) = two_notes()?;
        let (first, second) = (sources[0].id, sources[1].id);
        let heard = g.write(&Write::new("app").unit(note("n3")).derived_from([first]))?;
        let events = g.write(&Write::new("app").unit(note("n3")).derived_from([second]))?;
        assert_eq!(events.len(), 1, "the new source is recorded as an event");
        let found = g.find("note", "n3")?.map(|n| n.derived_from);
        assert_eq!(found.map(|ids| ids.len()), Some(2));
        assert_eq!(g.derived(second)?, [NodeRef::new("note", "n3")]);
        let before = g.events().committed()?;
        let again = g.write(&Write::new("app").unit(note("n3")).derived_from([second]))?;
        assert_eq!(
            g.events().committed()?,
            before,
            "a source already recorded adds nothing"
        );
        assert_eq!(again, heard, "it returns the event the hearing is named by");
        Ok(())
    }

    #[test]
    fn a_purge_leaves_no_derived_from_behind() -> Result<(), Box<dyn std::error::Error>> {
        let (mut g, sources) = two_notes()?;
        let source = sources[0].id;
        g.write(&Write::new("app").unit(note("n3")).derived_from([source]))?;
        let target = Target::Node(NodeRef::new("note", "n3"));
        g.purge(Purge::Entity { target }, "retention", "app")?;
        assert!(g.derived(source)?.is_empty());
        let rebuilt = crate::rebuild(
            &g.events().since(crate::Cursor::START)?,
            std::env::temp_dir().join(format!("rannoch-derived-{}.db", uuid::Uuid::now_v7())),
            Some(&crate::TEST_KEY),
        )?;
        assert!(rebuilt.derived(source)?.is_empty());
        Ok(())
    }
}
