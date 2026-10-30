//! Rannoch's store, on SQLite.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

mod fact;
mod history;
mod inbox;
mod log;
mod ontology;
mod queries;
mod read;
mod snapshot;

pub use fact::check;
pub use history::{At, End, Hearing, Seen, Version};
pub use inbox::State;
pub use log::{Cursor, Event, Events};
pub use rannoch_cloudevents::{Date, EventId, NodeRef, ValidTime};
pub use rannoch_ontology::{Class, Declaration, HistoryKey, Ontology, Relation, Value, ValueType};
pub use read::Node;
pub use snapshot::Snapshot;

use crate::history::{
    Heard, Key, Of, Stamp, alias, carry, conclude, heard_as, link_entity, newest, node_entity,
    resolve, take,
};
use crate::log::{append, store};
use crate::ontology::{Bound, build, load};
use crate::queries::{
    CLOSE_OPEN, DROP_OPEN, FILLED, HISTORY_TABLES, OPEN_LINK, OWN_TABLES, PRAGMAS, RECORD_FATE,
    RELEASE, ROLLBACK_TO, SAVEPOINT, SCHEMA_VERSION, SET_FILLED, SET_POSITION, clear_node,
    drop_link_row, node_id, upsert_link, upsert_node, waiting,
};

/// Why rannoch did not do what was asked.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Refused, with a reason in ontology terms.
    #[error("{0}")]
    Refused(String),
    /// The graph file failed.
    #[error("the graph file failed: {0}")]
    Storage(#[from] rusqlite::Error),
    /// An event or an ontology entry could not be encoded or decoded.
    #[error("an event could not be encoded or decoded: {0}")]
    Encoding(#[from] serde_json::Error),
    /// The graph was opened under a content-identity key other than its own.
    #[error("the content-identity key differs from the graph's")]
    KeyDiffers,
}

/// While another connection holds the file's write lock, a connection retries every millisecond,
/// for the same five seconds the default handler waits.
fn retry_lock(attempts: i32) -> bool {
    std::thread::sleep(Duration::from_millis(1));
    attempts < 5_000
}

/// The file's `PRAGMA schema_version`. SQLite raises it only when the schema changes, and every
/// declaration that is not identical creates a table or adds a column, so an unchanged version
/// means an unchanged ontology. Read it before loading the ontology it stands for, or inside the
/// same transaction, so a declaration landing in between costs a reload, never a stale ontology.
pub(crate) fn schema_version(conn: &Connection) -> Result<i64, Error> {
    Ok(conn
        .prepare_cached(SCHEMA_VERSION)?
        .query_row([], |row| row.get(0))?)
}

/// One graph file.
#[derive(Debug)]
pub struct Graph {
    pub(crate) conn: Connection,
    /// The app's content-identity key, which every hearing's identity is hashed under.
    pub(crate) key: Key,
    /// As of the last load by `open` or a `&mut self` method; what `ontology()` returns.
    pub(crate) ontology: Ontology,
    /// What reads use: the ontology and the `PRAGMA schema_version` it was loaded at.
    pub(crate) fresh: RefCell<Fresh>,
    /// The events this handle has handed out to each worker, so `next` hands back only what an
    /// earlier run left in progress.
    pub(crate) handed: std::collections::BTreeSet<(String, i64)>,
}

/// An ontology and the schema version of the file it was loaded at.
#[derive(Debug)]
pub(crate) struct Fresh {
    pub(crate) schema_version: i64,
    pub(crate) ontology: Arc<Ontology>,
}

/// Opens the graph at `path` under the app's content-identity key `key`, which is held in memory
/// only. A path that holds no graph yet starts an empty graph, which takes the key.
///
/// # Errors
/// [`Error::KeyDiffers`] when the graph holds another key's check value; the file cannot be
/// opened as a graph file.
pub fn open(path: impl AsRef<Path>, key: &[u8; 32]) -> Result<Graph, Error> {
    open_with(path, Key::new(key))
}

/// Opens the graph at `path` under `key`, as [`open`] does.
pub(crate) fn open_with(path: impl AsRef<Path>, key: Key) -> Result<Graph, Error> {
    let mut conn = Connection::open(path)?;
    conn.busy_handler(Some(retry_lock))?;
    conn.execute_batch(PRAGMAS)?;
    conn.set_prepared_statement_cache_capacity(256);
    conn.execute_batch(OWN_TABLES)?;
    conn.execute_batch(HISTORY_TABLES)?;
    take(&mut conn, &key)?;
    let schema_version = schema_version(&conn)?;
    let ontology = load(&conn)?;
    let fresh = RefCell::new(Fresh {
        schema_version,
        ontology: Arc::new(ontology.clone()),
    });
    Ok(Graph {
        conn,
        key,
        ontology,
        fresh,
        handed: std::collections::BTreeSet::new(),
    })
}

impl Graph {
    /// The graph's ontology as of this handle's last load, by `open` or by one of its own
    /// declarations: every class and relation declared on its stream by then.
    #[must_use]
    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    /// The ontology the file holds now. Another handle may have declared since this one last
    /// loaded, and a read must answer from the file's own stream; checking the schema version
    /// first keeps a plain data write elsewhere from costing a reload.
    pub(crate) fn current_ontology(&self) -> Result<Arc<Ontology>, Error> {
        let version = schema_version(&self.conn)?;
        {
            let fresh = self.fresh.borrow();
            if fresh.schema_version == version {
                return Ok(Arc::clone(&fresh.ontology));
            }
        }
        let ontology = Arc::new(load(&self.conn)?);
        *self.fresh.borrow_mut() = Fresh {
            schema_version: version,
            ontology: Arc::clone(&ontology),
        };
        Ok(ontology)
    }

    /// Takes `ontology`, loaded at `schema_version`, as both what `ontology()` returns and what
    /// reads use.
    pub(crate) fn loaded(&mut self, schema_version: i64, ontology: Ontology) {
        *self.fresh.get_mut() = Fresh {
            schema_version,
            ontology: Arc::new(ontology.clone()),
        };
        self.ontology = ontology;
    }

    /// The graph's event log.
    #[must_use]
    pub fn events(&self) -> Events<'_> {
        Events { conn: &self.conn }
    }

    /// Declares a class, a relation or a property, as one event whose body is the declaration's
    /// `CloudEvent`. The event, what the declaration needs and its place in the ontology land
    /// together or not at all. A declaration identical to the ontology as it is now writes
    /// nothing and returns `Ok(None)`.
    ///
    /// # Errors
    /// [`Error::Refused`] when the declaration conflicts with the ontology, with a reason in
    /// ontology terms; the graph is left as it was.
    pub fn declare(
        &mut self,
        declaration: &Declaration,
        body: &str,
    ) -> Result<Option<Event>, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ontology = load(&tx)?;
        if !build(&tx, &ontology, declaration)? {
            let version = schema_version(&tx)?;
            drop(tx);
            self.loaded(version, ontology);
            return Ok(None);
        }
        let event = append(&tx, body)?;
        let ontology = load(&tx)?;
        let version = schema_version(&tx)?;
        tx.commit()?;
        self.loaded(version, ontology);
        Ok(Some(event))
    }

    /// Appends a batch of events, each body a `CloudEvent` as JSON, in one transaction, in the
    /// order given, with a savepoint per event: an event that fails to append rolls back alone,
    /// and the others land. Bodies are not read here: checking them against the ontology is the
    /// queue's, before they are accepted ([`check`]).
    ///
    /// Each entry `(index, producer, position)` in `positions` says the body at `index` took
    /// `producer` to `position` in its source. Each producer's position lands in the same
    /// transaction, as of its last body that landed. Once one of a producer's bodies fails, its
    /// later bodies in the batch are not appended, so its position stops at the failed event and
    /// a producer that resumes there sends nothing twice and loses nothing. The answer holds one
    /// result per body, in the same order: the event as logged, or why it did not land.
    ///
    /// # Errors
    /// The graph file cannot be written; nothing of the batch lands, positions included.
    pub fn commit(
        &mut self,
        bodies: &[&str],
        positions: &[(usize, &str, &str)],
    ) -> Result<Vec<Result<Event, Error>>, Error> {
        let mut from: Vec<Option<(&str, &str)>> = vec![None; bodies.len()];
        for &(index, producer, position) in positions {
            let slot = from.get_mut(index).ok_or_else(|| {
                Error::Refused(format!(
                    "position {position} of producer {producer} names body {index}, \
                     but the batch has {} bodies",
                    bodies.len()
                ))
            })?;
            *slot = Some((producer, position));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut results = Vec::with_capacity(bodies.len());
        // Each producer's position as of its last body that landed, and whether one has failed.
        let mut reached: BTreeMap<&str, (Option<&str>, bool)> = BTreeMap::new();
        for (body, from) in bodies.iter().zip(from) {
            let producer = from.map(|(producer, position)| {
                (
                    reached.entry(producer).or_insert((None, false)),
                    producer,
                    position,
                )
            });
            if let Some(((_, true), producer, _)) = &producer {
                results.push(Err(Error::Refused(format!(
                    "an earlier event from producer {producer} failed at commit"
                ))));
                continue;
            }
            tx.execute_batch(SAVEPOINT)?;
            let landed = append(&tx, body);
            tx.execute_batch(if landed.is_ok() { RELEASE } else { ROLLBACK_TO })?;
            if let Some(((held, stopped), _, position)) = producer {
                if landed.is_ok() {
                    *held = Some(position);
                } else {
                    *stopped = true;
                }
            }
            results.push(landed);
        }
        for (producer, (position, _)) in reached {
            if let Some(position) = position {
                tx.prepare_cached(SET_POSITION)?
                    .execute([producer, position])?;
            }
        }
        tx.commit()?;
        Ok(results)
    }

    /// Fills the ontology's tables from the log: every committed event after the last one
    /// filled, in order, in one transaction. A node is written into its class's table under its
    /// key; a link into its relation's table between its two nodes. Returns the place filled to.
    ///
    /// # Errors
    /// An event cannot be filled, such as a link whose end is not in its class's table: nothing
    /// of this fill lands, and the error names the event's place.
    pub fn fill(&mut self) -> Result<Cursor, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ontology = load(&tx)?;
        let from = Cursor(tx.prepare_cached(FILLED)?.query_row([], |row| row.get(0))?);
        let events = Events { conn: &tx }.since(from)?;
        let mut to = from;
        for event in &events {
            let failed = |cause: Error| {
                Error::Refused(format!(
                    "event at {} cannot be filled: {cause}",
                    event.cursor.0
                ))
            };
            to = event.cursor;
            let recorded = event.id.recorded().map_err(Error::from).map_err(failed)?;
            let stamp = Stamp {
                cursor: event.cursor,
                recorded,
            };
            fill_one(&tx, &ontology, &self.key, event, stamp).map_err(failed)?;
        }
        tx.execute(SET_FILLED, [to.0])?;
        tx.commit()?;
        Ok(to)
    }
}

/// Fills one committed event: a node or link heard into its history and current state, a
/// conclusion, or a replacement. A fact naming a replaced key lands under the key that replaced
/// it, with the content identity it was written with.
fn fill_one(
    tx: &Connection,
    ontology: &Ontology,
    key: &Key,
    event: &Event,
    stamp: Stamp,
) -> Result<(), Error> {
    let recorded = stamp.recorded;
    let fact = fact::check(&event.body, ontology)?;
    let identity = match &fact {
        fact::Fact::Node(node) => Some(Of::Node(node).identity(key)?),
        fact::Fact::Link(link) => Some(Of::Link(link).identity(key)?),
        _ => None,
    };
    match (redirect(tx, fact)?, identity) {
        (fact::Fact::Replaced(replaced), _) => replace(tx, ontology, &replaced)?,
        (fact::Fact::Node(node), Some(identity)) => {
            let (entity, at) = (node_entity(&node.class), node.key.as_str());
            match heard_as(tx, ontology, identity, stamp, &Of::Node(&node))? {
                Heard::Held(held) => {
                    if alias(tx, &held, node.event, event.cursor)?
                        && let Some((v, recorded, _)) = newest(tx, (&entity, at, None))?
                    {
                        put_node(
                            tx,
                            &node.class,
                            at,
                            (&v.source, &v.valid, recorded),
                            &v.properties,
                        )?;
                    }
                }
                Heard::Newest => {
                    put_node(
                        tx,
                        &node.class,
                        at,
                        (&node.source, &node.valid, recorded),
                        &node.properties,
                    )?;
                    arrived(tx, ontology, &node.class, at, recorded)?;
                }
                Heard::Older => {}
            }
        }
        (fact::Fact::Link(link), Some(identity)) => {
            let entity = link_entity(&link.relation);
            let ends = (link.from.key.as_str(), link.to.key.as_str());
            match heard_as(tx, ontology, identity, stamp, &Of::Link(&link))? {
                Heard::Held(held) => {
                    if alias(tx, &held, link.event, event.cursor)?
                        && let Some((v, ..)) = newest(tx, (&entity, ends.0, Some(ends.1)))?
                    {
                        put_link(
                            tx,
                            ontology,
                            &link.relation,
                            ends,
                            (&v.source, &v.valid, recorded),
                        )?;
                    }
                }
                Heard::Newest => {
                    put_link(
                        tx,
                        ontology,
                        &link.relation,
                        ends,
                        (&link.source, &link.valid, recorded),
                    )?;
                }
                Heard::Older => {}
            }
        }
        (fact::Fact::Concluded(conclusion), _) => conclude(tx, &conclusion)?,
        _ => {}
    }
    Ok(())
}

/// `fact` with every node key it names standing under the key that replaced it, so a fact that
/// arrives after a replacement lands where the replacement moved its node.
fn redirect(tx: &Connection, fact: fact::Fact) -> Result<fact::Fact, Error> {
    let at = |r: &mut NodeRef| -> Result<(), Error> {
        r.key = resolve(tx, &r.class, &r.key)?;
        Ok(())
    };
    Ok(match fact {
        fact::Fact::Node(mut node) => {
            node.key = resolve(tx, &node.class, &node.key)?;
            fact::Fact::Node(node)
        }
        fact::Fact::Link(mut link) => {
            at(&mut link.from)?;
            at(&mut link.to)?;
            fact::Fact::Link(link)
        }
        fact::Fact::Concluded(mut conclusion) => {
            at(&mut conclusion.from)?;
            at(&mut conclusion.to)?;
            fact::Fact::Concluded(conclusion)
        }
        other => other,
    })
}

/// Applies `replaced`: records the node's fate, moves its history and links to the key it stands
/// under now, puts the new node's and the moved links' current state, and keeps the old node as a
/// row with no properties.
fn replace(
    tx: &Connection,
    ontology: &Ontology,
    replaced: &rannoch_cloudevents::Replacement,
) -> Result<(), Error> {
    let class = ontology.class(&replaced.class)?;
    let recorded = replaced.event.recorded()?;
    tx.prepare_cached(RECORD_FATE)?.execute(rusqlite::params![
        class.name,
        replaced.old,
        replaced.new,
        recorded,
        replaced.event.to_string()
    ])?;
    let target = resolve(tx, &class.name, &replaced.old)?;
    for link in carry(tx, ontology, (&class.name, &replaced.old, &target))? {
        let relation = ontology.relation(&link.relation)?;
        tx.prepare_cached(&drop_link_row(&relation.name, &relation.from, &relation.to))?
            .execute([&link.old.0, &link.old.1])?;
        tx.prepare_cached(DROP_OPEN)?
            .execute([&relation.name, &link.old.0, &link.old.1])?;
        let ends = (link.new.0.as_str(), link.new.1.as_str());
        if let Some((v, recorded, _)) =
            newest(tx, (&link_entity(&relation.name), ends.0, Some(ends.1)))?
        {
            put_link(
                tx,
                ontology,
                &relation.name,
                ends,
                (&v.source, &v.valid, recorded),
            )?;
        }
    }
    if let Some((v, at, _)) = newest(tx, (&node_entity(&class.name), &target, None))? {
        put_node(
            tx,
            &class.name,
            &target,
            (&v.source, &v.valid, at),
            &v.properties,
        )?;
        arrived(tx, ontology, &class.name, &target, at)?;
    }
    if present(tx, &class.name, &replaced.old)? {
        let names: Vec<&str> = class.properties.keys().map(String::as_str).collect();
        tx.prepare_cached(&clear_node(&class.name, &names))?
            .execute([&replaced.old])?;
    } else {
        put_node(
            tx,
            &class.name,
            &replaced.old,
            (&replaced.source, &ValidTime::Unknown, recorded),
            &BTreeMap::new(),
        )?;
    }
    Ok(())
}

/// Puts a version of the node keyed `key` into its class's table as its current state, with its
/// source, valid time and record time.
fn put_node(
    tx: &Connection,
    class: &str,
    key: &str,
    (source, valid, recorded): (&str, &ValidTime, i64),
    properties: &BTreeMap<String, Value>,
) -> Result<(), Error> {
    let names: Vec<&str> = properties.keys().map(String::as_str).collect();
    let valid = valid.as_text();
    let mut values: Vec<&dyn rusqlite::ToSql> = vec![&key, &source, &valid, &recorded];
    let bound: Vec<Bound<'_>> = properties.values().map(Bound).collect();
    values.extend(bound.iter().map(|v| v as &dyn rusqlite::ToSql));
    tx.prepare_cached(&upsert_node(class, &names))?
        .execute(values.as_slice())?;
    Ok(())
}

/// Puts a link's current state, with its source, valid time and record time: into its relation
/// when both its ends are present, else open.
fn put_link(
    tx: &Connection,
    ontology: &Ontology,
    relation: &str,
    (from, to): (&str, &str),
    (source, valid, recorded): (&str, &ValidTime, i64),
) -> Result<(), Error> {
    let declared = ontology.relation(relation)?;
    let valid = valid.as_text();
    if present(tx, &declared.from, from)? && present(tx, &declared.to, to)? {
        tx.prepare_cached(&upsert_link(relation, &declared.from, &declared.to))?
            .execute(rusqlite::params![from, to, source, valid, recorded])?;
    } else {
        tx.prepare_cached(OPEN_LINK)?
            .execute(rusqlite::params![relation, from, to, source, valid])?;
    }
    Ok(())
}

/// Whether the node keyed `key` of `class` is in its class's table.
fn present(tx: &Connection, class: &str, key: &str) -> Result<bool, Error> {
    Ok(tx
        .prepare_cached(&node_id(class))?
        .query_row([key], |row| row.get::<_, i64>(0))
        .optional()?
        .is_some())
}

/// Closes every open link that was waiting for the node `key` of `class`, now present, whose
/// other end is present too: the link moves into its relation with the source it was opened with,
/// and its closing is logged as one event.
fn arrived(
    tx: &Connection,
    ontology: &Ontology,
    class: &str,
    key: &str,
    recorded: i64,
) -> Result<(), Error> {
    for relation in ontology.relations.values() {
        for (waits_on, column) in [(&relation.to, "to_key"), (&relation.from, "from_key")] {
            if waits_on != class {
                continue;
            }
            let open: Vec<(String, String, String, Option<String>)> = tx
                .prepare_cached(&waiting(column))?
                .query_map(rusqlite::params![relation.name, key], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?
                .collect::<Result<_, _>>()?;
            for (from_key, to_key, source, valid) in open {
                if !(present(tx, &relation.from, &from_key)? && present(tx, &relation.to, &to_key)?)
                {
                    continue;
                }
                tx.prepare_cached(&upsert_link(&relation.name, &relation.from, &relation.to))?
                    .execute(rusqlite::params![from_key, to_key, source, valid, recorded])?;
                tx.prepare_cached(CLOSE_OPEN)?.execute(rusqlite::params![
                    relation.name,
                    from_key,
                    to_key
                ])?;
                let id = EventId::now();
                let body = serde_json::json!({
                    "specversion": "1.0",
                    "id": id.to_string(),
                    "source": "rannoch",
                    "type": format!("rannoch.closed.{}", relation.name),
                    "subject": from_key,
                    "data": {
                        "from": {"class": relation.from, "key": from_key},
                        "to": {"class": relation.to, "key": to_key},
                        "valid": ValidTime::from_text(valid),
                    },
                });
                store(tx, id, None, &body.to_string())?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{Class, Declaration, Relation, open};

    const KEY: [u8; 32] = [7; 32];

    fn temp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rannoch-store-{name}-{}.db", uuid::Uuid::now_v7()))
    }

    /// A `CloudEvent` id `<event uuid>:<fact>` minted now.
    fn id() -> String {
        format!("{}:0", uuid::Uuid::now_v7())
    }

    /// A `CloudEvent` id whose origin event was recorded at `seconds` after the Unix epoch.
    fn id_at(seconds: u64) -> String {
        let at = uuid::Timestamp::from_unix(uuid::NoContext, seconds, 0);
        format!("{}:0", uuid::Uuid::new_v7(at))
    }

    fn node_with(id: &str, class: &str, key: &str, title: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:shop","type":"rannoch.node.{class}","subject":"{key}","data":{{"properties":{{"title":"{title}"}},"valid":"unknown"}}}}"#
        )
    }

    fn node(class: &str, key: &str, title: &str) -> String {
        node_with(&id(), class, key, title)
    }

    fn link_with(id: &str, from: &str, to: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:shop","type":"rannoch.link.wrote","subject":"{from}","data":{{"from":{{"class":"author","key":"{from}"}},"to":{{"class":"book","key":"{to}"}},"valid":"unknown"}}}}"#
        )
    }

    fn link(from: &str, to: &str) -> String {
        link_with(&id(), from, to)
    }

    /// The link from `from` to `to` ended at `at` milliseconds (`kind` `ended`), or was found
    /// wrong (`kind` `corrected`).
    fn concluded(id: &str, kind: &str, from: &str, to: &str, at: i64) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:shop","type":"rannoch.{kind}.wrote","subject":"{from}","data":{{"from":{{"class":"author","key":"{from}"}},"to":{{"class":"book","key":"{to}"}},"at":{at},"valid":"unknown"}}}}"#
        )
    }

    fn title(g: &crate::Graph, key: &str) -> Result<Option<crate::Value>, crate::Error> {
        Ok(g.find("book", key)?
            .and_then(|node| node.properties.get("title").cloned()))
    }

    fn graph(name: &str) -> Result<crate::Graph, crate::Error> {
        let mut g = open(temp(name), &KEY)?;
        let declared = |name: &str| {
            format!(
                r#"{{"specversion":"1.0","id":"d-{name}","source":"rannoch:shop","type":"rannoch.declared.{name}"}}"#
            )
        };
        g.declare(
            &Declaration::Class(Class::new("author", ["title"])),
            &declared("author"),
        )?;
        g.declare(
            &Declaration::Class(Class::new("book", ["title"])),
            &declared("book"),
        )?;
        g.declare(
            &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
            &declared("wrote"),
        )?;
        Ok(g)
    }

    #[test]
    fn a_late_arrival_does_not_change_current_state() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("late")?;
        g.commit(&[&node_with(&id_at(2_000), "book", "b1", "new")], &[])?;
        g.fill()?;
        g.commit(&[&node_with(&id_at(1_000), "book", "b1", "old")], &[])?;
        g.fill()?;
        assert_eq!(title(&g, "b1")?, Some("new".into()));
        Ok(())
    }

    #[test]
    fn first_and_last_heard_follow_the_axis() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("seen")?;
        g.commit(&[&node_with(&id_at(2_000), "book", "b1", "new")], &[])?;
        g.commit(&[&node_with(&id_at(1_000), "book", "b1", "old")], &[])?;
        g.fill()?;
        let seen = g
            .seen(&crate::NodeRef::new("book", "b1"))?
            .ok_or("b1 was heard")?;
        assert_eq!(
            seen.first_heard.properties.get("title"),
            Some(&"old".into())
        );
        assert_eq!(seen.last_heard.properties.get("title"), Some(&"new".into()));
        assert_eq!(
            seen.first_recorded.properties.get("title"),
            Some(&"new".into())
        );
        assert_eq!(seen.count, 2);
        Ok(())
    }

    #[test]
    fn a_correction_masks_an_ending() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("masks")?;
        g.commit(
            &[
                &node("author", "a1", "ann"),
                &node("book", "b1", "hi"),
                &link("a1", "b1"),
            ],
            &[],
        )?;
        g.commit(&[&concluded(&id(), "ended", "a1", "b1", i64::MAX / 2)], &[])?;
        g.fill()?;
        assert!(matches!(
            g.link_end("wrote", "a1", "b1")?,
            Some(crate::End::Ended { .. })
        ));
        g.commit(&[&concluded(&id(), "corrected", "a1", "b1", 0)], &[])?;
        g.fill()?;
        assert!(matches!(
            g.link_end("wrote", "a1", "b1")?,
            Some(crate::End::Corrected { .. })
        ));
        let seen = g
            .link_seen("wrote", "a1", "b1")?
            .ok_or("the link was heard")?;
        assert_eq!(seen.ended, None, "a correction is not an ending");
        Ok(())
    }

    #[test]
    fn identical_content_is_one_hearing() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("identical")?;
        let (later, earlier) = (id_at(2_000), id_at(1_000));
        g.commit(&[&node_with(&later, "book", "b1", "hi")], &[])?;
        g.commit(&[&node_with(&earlier, "book", "b1", "hi")], &[])?;
        g.fill()?;
        let hearings = g.hearings(&crate::NodeRef::new("book", "b1"))?;
        assert_eq!(hearings.len(), 1);
        let origin = |id: &str| id.split(':').next().unwrap_or_default().to_string();
        assert_eq!(hearings[0].event.to_string(), origin(&earlier));
        let aliases: Vec<String> = hearings[0]
            .aliases
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(aliases, [origin(&later)]);
        Ok(())
    }

    #[test]
    fn the_same_events_in_any_order_give_the_same_answers() -> Result<(), Box<dyn std::error::Error>>
    {
        let events = [
            node_with(&id_at(100), "author", "a1", "ann"),
            node_with(&id_at(1_000), "book", "b1", "one"),
            node_with(&id_at(1_500), "book", "b1", "one"),
            node_with(&id_at(3_000), "book", "b1", "two"),
            link_with(&id_at(500), "a1", "b1"),
            concluded(&id_at(4_000), "ended", "a1", "b1", 2_000_000),
        ];
        let answers = |name: &str, order: &[&String]| -> Result<_, Box<dyn std::error::Error>> {
            let mut g = graph(name)?;
            for event in order {
                g.commit(&[event.as_str()], &[])?;
                g.fill()?;
            }
            let b1 = crate::NodeRef::new("book", "b1");
            let now = g.find("book", "b1")?.ok_or("b1 was heard")?;
            let at = crate::At::Time(std::time::UNIX_EPOCH + std::time::Duration::from_secs(2_000));
            let seen = g.seen(&b1)?.ok_or("b1 was heard")?;
            let link = g
                .link_seen("wrote", "a1", "b1")?
                .ok_or("the link was heard")?;
            Ok((
                (now.properties, now.source, now.valid),
                g.state_at(&b1, at)?,
                (
                    seen.first_heard,
                    seen.last_heard,
                    seen.count,
                    seen.first_true,
                    seen.ended,
                ),
                (link.first_heard, link.last_heard, link.count, link.ended),
                g.link_end("wrote", "a1", "b1")?,
                g.hearings(&b1)?,
            ))
        };
        let forward: Vec<&String> = events.iter().collect();
        let backward: Vec<&String> = events.iter().rev().collect();
        assert_eq!(
            answers("forward", &forward)?,
            answers("backward", &backward)?
        );
        Ok(())
    }

    #[test]
    fn one_failing_event_rolls_back_alone() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("alone")?;
        g.conn.execute_batch(
            "CREATE TRIGGER bad_fails BEFORE INSERT ON _events WHEN NEW.body LIKE '%\"bad\"%' \
             BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END;",
        )?;
        let before = g.events().committed()?;
        let bad = node("book", "bad", "x");
        let results = g.commit(
            &[&node("book", "b1", "hi"), &bad, &node("book", "b2", "yo")],
            &[],
        )?;
        assert!(results[0].is_ok() && results[1].is_err() && results[2].is_ok());
        assert_eq!(g.events().since(before)?.len(), 2);
        Ok(())
    }

    #[test]
    fn a_producer_stops_at_its_first_failed_event() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("stops")?;
        g.conn.execute_batch(
            "CREATE TRIGGER bad_fails BEFORE INSERT ON _events WHEN NEW.body LIKE '%\"bad\"%' \
             BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END;",
        )?;
        let before = g.events().committed()?;
        let (b1, bad, b3, other) = (
            node("book", "b1", "hi"),
            node("book", "bad", "x"),
            node("book", "b3", "yo"),
            node("book", "b4", "ok"),
        );
        let results = g.commit(
            &[&b1, &bad, &b3, &other],
            &[
                (0, "shop", "1"),
                (1, "shop", "2"),
                (2, "shop", "3"),
                (3, "feed", "9"),
            ],
        )?;
        let landed: Vec<bool> = results.iter().map(Result::is_ok).collect();
        assert_eq!(landed, [true, false, false, true]);
        assert_eq!(g.events().since(before)?.len(), 2);
        assert_eq!(g.position("shop")?.as_deref(), Some("1"));
        assert_eq!(g.position("feed")?.as_deref(), Some("9"));
        Ok(())
    }

    #[test]
    fn commit_does_not_check_the_ontology() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("unchecked")?;
        let results = g.commit(&[&node("tweet", "t1", "x")], &[])?;
        assert!(results[0].is_ok(), "{results:?}");
        Ok(())
    }

    /// What a reader can ask of `g` about the shop: every event's id and place, and the book,
    /// its hearings and the link's ending.
    fn answers(g: &crate::Graph) -> Result<String, Box<dyn std::error::Error>> {
        let b1 = crate::NodeRef::new("book", "b1");
        let events: Vec<(String, crate::Cursor)> = g
            .events()
            .since(crate::Cursor::START)?
            .into_iter()
            .map(|e| (e.id.to_string(), e.cursor))
            .collect();
        Ok(format!(
            "{events:?}\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            g.find("book", "b1")?,
            g.find("book", "b2")?,
            g.hearings(&b1)?,
            g.seen(&b1)?,
            g.link_end("wrote", "a1", "b1")?,
        ))
    }

    #[test]
    fn a_graph_rebuilt_from_a_snapshot_answers_as_the_graph_it_came_from()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("snap")?;
        g.commit(
            &[
                &node("author", "a1", "ann"),
                &node_with(&id_at(1_000), "book", "b1", "old"),
                &node_with(&id_at(2_000), "book", "b1", "new"),
                &node_with(&id_at(3_000), "book", "b1", "new"),
                &link("a1", "b1"),
            ],
            &[(0, "shop", "5")],
        )?;
        g.fill()?;
        let at = g.events().committed()?;
        let handed = g.next("w", at)?.ok_or("nothing handed out")?.cursor;
        g.commit(
            &[
                &node("book", "b2", "later"),
                &concluded(&id(), "ended", "a1", "b1", 9_000),
            ],
            &[],
        )?;
        g.fill()?;
        let path = temp("snap-file");
        let snapshot = g.snapshot(at, &path)?;
        assert_eq!(snapshot.cursor(), at);
        let at_snapshot = open(&path, &KEY)?;
        assert!(
            at_snapshot.find("book", "b2")?.is_none(),
            "state as of the cursor"
        );
        assert_eq!(at_snapshot.position("shop")?.as_deref(), Some("5"));
        assert_eq!(at_snapshot.state("w", handed)?, crate::State::InProgress);
        let later = g.events().since(at)?;
        let back = snapshot.rebuild(&later, temp("snap-back"), &KEY)?;
        assert_eq!(answers(&back)?, answers(&g)?);
        assert!(back.events().committed()? >= at);
        Ok(())
    }

    #[test]
    fn a_snapshot_is_taken_while_another_connection_holds_the_write_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("snap-locked");
        let mut g = open(&path, &KEY)?;
        g.declare(
            &Declaration::Class(Class::new("book", ["title"])),
            r#"{"specversion":"1.0","id":"d-book","source":"rannoch:shop","type":"rannoch.declared.book"}"#,
        )?;
        g.commit(&[&node("book", "b1", "hi")], &[])?;
        let at = g.events().committed()?;
        let holder = rusqlite::Connection::open(&path)?;
        holder.execute_batch("BEGIN IMMEDIATE")?;
        let release = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            holder.execute_batch("COMMIT")
        });
        let snapshot = g.snapshot(at, temp("snap-locked-file"))?;
        release.join().map_err(|_| "the lock holder panicked")??;
        let back = snapshot.rebuild(&[], temp("snap-locked-back"), &KEY)?;
        assert!(back.find("book", "b1")?.is_some());
        Ok(())
    }

    #[test]
    fn a_snapshot_under_another_key_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("snap-key")?;
        g.commit(&[&node("book", "b1", "hi")], &[])?;
        let snapshot = g.snapshot(g.events().committed()?, temp("snap-key-file"))?;
        assert!(matches!(
            snapshot.rebuild(&[], temp("snap-key-back"), &[9; 32]),
            Err(crate::Error::KeyDiffers)
        ));
        Ok(())
    }

    /// The node `old` of `class` replaced by `new`, in the event `id`.
    fn replaced(id: &str, class: &str, old: &str, new: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:shop","type":"rannoch.replaced.{class}","subject":"{old}","data":{{"new":"{new}"}}}}"#
        )
    }

    /// The shop before b1 is replaced by b2, and the replacement, in that order.
    fn shop_then_replacement() -> Vec<String> {
        vec![
            node("author", "a1", "ann"),
            node_with(&id_at(1_000), "book", "b1", "old"),
            node_with(&id_at(2_000), "book", "b1", "new"),
            node_with(&id_at(1_500), "book", "b2", "two"),
            link("a1", "b1"),
            link("a1", "b2"),
            replaced(&id_at(3_000), "book", "b1", "b2"),
        ]
    }

    /// What the replacement left: b1's properties and fate, b2's title, and the counts of b2's
    /// hearings, the merged link's hearings and the old link's.
    fn after_replacement(g: &crate::Graph) -> Result<String, Box<dyn std::error::Error>> {
        let b1 = g.find("book", "b1")?.ok_or("b1 keeps its row")?;
        let b2 = crate::NodeRef::new("book", "b2");
        Ok(format!(
            "{:?} {:?} {:?} {} {} {} {:?}",
            b1.properties,
            b1.replaced_by,
            title(g, "b2")?,
            g.hearings(&b2)?.len(),
            g.link_hearings("wrote", "a1", "b2")?.len(),
            g.link_hearings("wrote", "a1", "b1")?.len(),
            g.seen(&b2)?.map(|seen| seen.count),
        ))
    }

    #[test]
    fn a_replacement_carries_the_node_s_history_and_links_to_its_new_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("replace")?;
        let events = shop_then_replacement();
        let bodies: Vec<&str> = events.iter().map(String::as_str).collect();
        g.commit(&bodies, &[])?;
        g.fill()?;
        let b1 = g.find("book", "b1")?.ok_or("b1 keeps its row")?;
        assert!(b1.properties.is_empty());
        assert_eq!(b1.replaced_by.as_deref(), Some("b2"));
        assert_eq!(title(&g, "b2")?, Some(crate::Value::from("new")));
        assert_eq!(g.hearings(&crate::NodeRef::new("book", "b2"))?.len(), 3);
        assert_eq!(
            g.link_hearings("wrote", "a1", "b2")?.len(),
            2,
            "the moved link merged"
        );
        assert!(g.link_hearings("wrote", "a1", "b1")?.is_empty());
        assert_eq!(
            g.seen(&crate::NodeRef::new("book", "b2"))?
                .map(|seen| seen.count),
            Some(3)
        );
        Ok(())
    }

    #[test]
    fn a_replacement_gives_the_same_state_in_any_arrival_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let events = shop_then_replacement();
        let mut in_order = graph("replace-order")?;
        in_order.commit(&events.iter().map(String::as_str).collect::<Vec<_>>(), &[])?;
        in_order.fill()?;
        let mut shuffled = graph("replace-shuffled")?;
        let order = [6, 3, 4, 2, 0, 5, 1];
        shuffled.commit(&order.map(|i| events[i].as_str()), &[])?;
        shuffled.fill()?;
        assert_eq!(after_replacement(&shuffled)?, after_replacement(&in_order)?);
        Ok(())
    }

    #[test]
    fn a_node_s_fate_is_the_replacement_greatest_by_point() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut g = graph("fate")?;
        g.commit(
            &[
                &node("book", "x", "one"),
                &node("book", "y", "two"),
                &node("book", "z", "three"),
                &replaced(&id_at(5_000), "book", "x", "y"),
                &replaced(&id_at(4_000), "book", "x", "z"),
            ],
            &[],
        )?;
        g.fill()?;
        assert_eq!(
            g.find("book", "x")?.ok_or("x")?.replaced_by.as_deref(),
            Some("y")
        );
        Ok(())
    }

    #[test]
    fn replacing_a_node_by_itself_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("self")?;
        let results = g.commit(&[&replaced(&id(), "book", "b1", "b1")], &[])?;
        assert!(results[0].is_ok(), "commit appends without checking");
        assert!(super::check(&replaced(&id(), "book", "b1", "b1"), g.ontology()).is_err());
        Ok(())
    }

    #[test]
    fn fill_writes_nodes_and_links_after_the_log() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("fill")?;
        g.commit(
            &[
                &node("author", "a1", "ann"),
                &node("book", "b1", "hi"),
                &link("a1", "b1"),
            ],
            &[],
        )?;
        let to = g.fill()?;
        assert_eq!(to, g.events().committed()?);
        let title: String = g.conn.query_row(
            "SELECT \"p:title\" FROM \"class:book\" WHERE key = 'b1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(title, "hi");
        let links: i64 =
            g.conn
                .query_row("SELECT count(*) FROM \"relation:wrote\"", [], |row| {
                    row.get(0)
                })?;
        assert_eq!(links, 1);
        Ok(())
    }

    #[test]
    fn a_link_to_a_missing_node_waits_and_closes_once_when_it_arrives()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("waits")?;
        let links = |g: &crate::Graph| -> rusqlite::Result<i64> {
            g.conn
                .query_row("SELECT count(*) FROM \"relation:wrote\"", [], |row| {
                    row.get(0)
                })
        };
        g.commit(&[&node("author", "a1", "ann"), &link("a1", "b1")], &[])?;
        g.fill()?;
        assert_eq!(links(&g)?, 0, "the link waits while its book is missing");
        let before = g.events().committed()?;
        g.commit(&[&node("book", "b1", "hi")], &[])?;
        g.fill()?;
        assert_eq!(links(&g)?, 1, "the link closes when the book arrives");
        let closings = g
            .events()
            .since(before)?
            .iter()
            .filter(|e| e.body.contains("rannoch.closed.wrote"))
            .count();
        assert_eq!(closings, 1, "its closing is logged once");
        Ok(())
    }
}
