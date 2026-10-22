//! The log: every change to a graph is an event, read in order from a cursor and kept; a
//! snapshot of state at a cursor the app picks is where a new reader or a rebuild starts. This
//! module owns rannoch's own tables, whose names start with `_`.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::open::{Graph, Ontology, Value, class_table, open, relation_table, schema_version};
use crate::read::NodeRef;
use crate::write::{Declaration, Error, ValidTime, now_millis, replay};

/// rannoch's own tables: the event log, the ontology, links waiting for an end, the fates of
/// nodes that were replaced, and the registered followers with their stored cursors. An event's
/// place is never reused, so a cursor stays a place in the log.
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
        PRIMARY KEY(class, key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _followers(name TEXT PRIMARY KEY, cursor INTEGER NOT NULL)
        WITHOUT ROWID;";

/// A place in a graph's event log. Reading since a cursor returns the events after it. An app
/// stores a cursor by serializing it, and a graph rebuilt from the log keeps each event's place,
/// so a stored cursor names the same place in the rebuilt graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
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
    /// A node the app forgot under its own rule, and the links that left with it. It names
    /// what it removed by id and kind and holds none of the node's content.
    Forgot {
        source: String,
        rule: String,
        nodes: Vec<NodeRef>,
        links: Vec<LinkFact>,
    },
    /// The node `old` of `class` was replaced by the node `new` of the same class.
    Replaced {
        class: String,
        old: String,
        new: String,
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
    id: EventId,
    pub(crate) change: Change,
}

/// Appends one event inside the caller's transaction, so it lands only if that transaction does.
pub(crate) fn append(tx: &Connection, change: Change) -> Result<Event, Error> {
    store(tx, EventId(Uuid::now_v7()), None, change)
}

/// Appends one event under the id it already has, inside the caller's transaction: at `place`
/// when it has one, otherwise at the end of the log.
fn store(
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
    /// The committed watermark: the cursor of the last committed event, or [`Cursor::START`]
    /// when none is. Another process on the same file learns of a commit by reading it, then
    /// reads [`Events::since_until`] its own cursor up to it; an event not yet committed is
    /// never under it.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn committed(&self) -> Result<Cursor, Error> {
        Ok(Cursor(
            self.conn
                .prepare_cached("SELECT COALESCE(MAX(seq), 0) FROM _events")?
                .query_row([], |row| row.get(0))?,
        ))
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
    /// [`Error::Refused`] when `name` is not registered; the graph file cannot be written.
    pub fn advance(&self, name: &str, cursor: Cursor) -> Result<(), Error> {
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

/// Rebuilds a graph file at `empty_path` from `events` alone, replaying them in order: its
/// ontology from the declaration events, and its nodes, links and open links from the rest.
/// Each event lands in the new file under its own id and at its own place in the log. The
/// events land together or not at all.
///
/// # Errors
/// [`Error::Refused`] naming the event when an event carries a property its class does not
/// declare; the file cannot be opened as a graph file, or an event cannot be replayed into it.
pub fn rebuild(events: &[Event], empty_path: impl AsRef<Path>) -> Result<Graph, Error> {
    replay_into(open(empty_path)?, events, |_| true)
}

/// Replays `events` into `graph` in one transaction, storing each one `keep` holds for under its
/// own place in the log, so a cursor names the same event in the new file as in the source.
fn replay_into(
    mut graph: Graph,
    events: &[Event],
    keep: impl Fn(&Change) -> bool,
) -> Result<Graph, Error> {
    let tx = graph
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut ontology = Ontology::load(&tx)?;
    let recorded = now_millis();
    for event in events {
        if let Change::Wrote { added, .. } = &event.change {
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
        replay(&tx, &ontology, recorded, &event.change)?;
        if matches!(event.change, Change::Declared(_)) {
            ontology = Ontology::load(&tx)?;
        }
        if keep(&event.change) {
            store(&tx, event.id, Some(event.cursor), event.change.clone())?;
        }
    }
    tx.commit()?;
    graph.ontology = ontology;
    Ok(graph)
}

/// A graph's state at a cursor, in a file of its own: its ontology, nodes, links, open links and
/// each node's fate. Of the events, it holds only the forget events, which carry no content and
/// are what a forgotten node's fate is read from. A new reader or a rebuild starts from it and
/// takes only the events after its cursor.
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
    /// after the snapshot's cursor, replayed in order and kept in the new file's log. The
    /// snapshot file is left as it was.
    ///
    /// # Errors
    /// [`Error::Refused`] when `empty_path` is not UTF-8; the snapshot cannot be copied, the
    /// file cannot be opened as a graph file, or an event cannot be replayed into it.
    pub fn rebuild(&self, later: &[Event], empty_path: impl AsRef<Path>) -> Result<Graph, Error> {
        let empty_path = empty_path.as_ref();
        let target = empty_path.to_str().ok_or_else(|| {
            Error::Refused(format!("the path {} is not UTF-8", empty_path.display()))
        })?;
        Connection::open(&self.path)?.execute("VACUUM INTO ?1", params![target])?;
        replay_into(open(empty_path)?, later, |_| true)
    }
}

impl Graph {
    /// The cursor of the last event in the log, or [`Cursor::START`] when it holds none.
    pub(crate) fn last_cursor(&self) -> Result<Cursor, Error> {
        self.events().committed()
    }

    /// Takes a snapshot of the graph's state at `cursor`, a place the app picks, into a new file
    /// at `empty_path`. It deletes nothing: every event stays in the store. It reads this graph's
    /// log and writes only the new file, so it never takes this graph's write lock.
    ///
    /// # Errors
    /// The graph file cannot be read, the new file cannot be opened as a graph file, or an
    /// event cannot be replayed into it.
    pub fn snapshot(
        &self,
        cursor: Cursor,
        empty_path: impl AsRef<Path>,
    ) -> Result<Snapshot, Error> {
        let mut events = self.events().since(Cursor::START)?;
        events.retain(|event| event.cursor <= cursor);
        // A forget event holds no content and is where a forgotten node's fate is read from.
        let snapshot = replay_into(open(&empty_path)?, &events, |change| {
            matches!(change, Change::Forgot { .. })
        })?;
        // Registrations are state about readers, not events: the snapshot carries them.
        for (name, at) in self.events().followers()? {
            snapshot.events().register(&name)?;
            snapshot.events().advance(&name, at)?;
        }
        Ok(Snapshot {
            cursor,
            path: empty_path.as_ref().to_path_buf(),
        })
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
        let version = schema_version(&tx)?;
        let ontology = Ontology::load(&tx)?;
        let links = remove(&tx, &ontology, target)?.ok_or_else(|| Error::NotFound {
            class: target.class.clone(),
            key: target.key.clone(),
        })?;
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
        tx.commit()?;
        self.loaded(version, ontology);
        Ok(event)
    }
}

/// Removes `target` and its links from live state and returns the links, or `None` when no node
/// of its class has its key. Forgetting and replaying a forget both remove through here.
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
    Ok(Some(links))
}

/// Removes every link of the node `id`, which is `target`, from live state, open links waiting
/// on its other end included, and returns them.
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

/// Strips `target`'s content from every earlier event that wrote it, keeping its id and kind.
fn strip(tx: &Connection, target: &NodeRef) -> Result<(), Error> {
    for (seq, mut stored) in naming_key(tx, &target.key)? {
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

#[cfg(test)]
mod tests {
    use crate::{
        Change, Class, Date, Declaration, NodeRef, Relation, Unit, ValidTime, Write, open,
    };

    #[test]
    fn registrations_are_carried_by_a_snapshot_and_not_by_a_rebuild_from_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = |name: &str| {
            std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()))
        };
        let mut g = open(temp("followers"))?;
        assert_eq!(g.events().processed()?, None, "no follower, no mark");
        assert!(matches!(
            g.events().advance("typo", crate::Cursor::START),
            Err(crate::Error::Refused(_))
        ));
        g.events().register("reader")?;
        g.declare(&Declaration::Class(Class::new("person", ["name"])))?;
        let at = g.events().committed()?;
        g.events().advance("reader", at)?;

        let snapshot = g.snapshot(at, temp("followers-snapshot"))?;
        let carried = snapshot.rebuild(&[], temp("followers-from-snapshot"))?;
        assert_eq!(
            carried.events().followers()?,
            vec![("reader".to_string(), at)]
        );
        let rebuilt = crate::rebuild(
            &g.events().since(crate::Cursor::START)?,
            temp("followers-rebuilt"),
        )?;
        assert!(rebuilt.events().followers()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_forget_takes_the_nodes_open_links_with_it() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-forget-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path)?;
        g.declare(&Declaration::Class(Class::new("person", ["name"])))?;
        g.declare(&Declaration::Class(Class::new("place", ["name"])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "born_in", "person", "place", false,
        )))?;
        let when = ValidTime::On(Date::day(1975, 2, 10));
        let fiona = || Unit::new("person", "p05", [("name", "Fiona")], when.clone());
        g.write(&Write::new("tree").unit(fiona().link("born_in", "p05", "pl-late", when.clone())))?;

        let forgot = g.forget(&NodeRef::new("person", "p05"), "living", "tree")?;
        let Change::Forgot { links, .. } = forgot.change else {
            panic!("expected a forget event, got {:?}", forgot.change);
        };
        assert_eq!(links.len(), 1, "the forget names the open born_in link");
        assert!(
            g.open_links()?.is_empty(),
            "no open link names the forgotten person"
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
            "a link the forget removed never closes onto the returning key: {back:?}"
        );
        Ok(())
    }

    #[test]
    fn a_rebuild_refuses_an_event_carrying_an_undeclared_property_and_names_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = |name: &str| {
            std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()))
        };
        let mut g = open(temp("undeclared"))?;
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

        let Err(error) = crate::rebuild(&events, temp("undeclared-rebuilt")) else {
            panic!("the rebuild dropped the undeclared property instead of failing");
        };
        let message = error.to_string();
        assert!(
            message.contains(&events[last].id.to_string()) && message.contains("age"),
            "{message}"
        );
        Ok(())
    }
}
