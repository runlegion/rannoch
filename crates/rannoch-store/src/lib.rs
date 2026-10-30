//! Rannoch's store, on SQLite.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

mod fact;
mod history;
mod inbox;
mod log;
mod ontology;
mod queries;
mod read;

pub use fact::check;
pub use history::{At, End, Hearing, Seen, Version};
pub use inbox::{FILL, State};
pub use log::{Cursor, Event, Events};
pub use rannoch_cloudevents::{Date, EventId, NodeRef, ValidTime};
pub use rannoch_ontology::{Class, Declaration, HistoryKey, Ontology, Relation, Value, ValueType};
pub use read::Node;

use crate::history::{
    Heard, Key, Of, Stamp, alias, conclude, heard, link_entity, newest, node_entity, take,
};
use crate::inbox::place;
use crate::log::{append, event_at, store};
use crate::ontology::{Bound, build, load};
use crate::queries::{
    CLOSE_OPEN, DONE_RETRY, HISTORY_TABLES, OPEN_LINK, OWN_TABLES, PRAGMAS, RELEASE, RESET_TRIES,
    RETRY_WORK, ROLLBACK_TO, SAVEPOINT, SCHEMA_VERSION, SET_PLACE, SET_POSITION, SET_TRY_LIMIT,
    STOPPED_WORK, TRIED_WORK, TRY_LIMIT, node_id, upsert_link, upsert_node, waiting,
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
    let mut conn = Connection::open(path)?;
    conn.busy_handler(Some(retry_lock))?;
    conn.execute_batch(PRAGMAS)?;
    conn.set_prepared_statement_cache_capacity(256);
    conn.execute_batch(OWN_TABLES)?;
    conn.execute_batch(HISTORY_TABLES)?;
    let key = Key::new(key);
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

    /// Fills the ontology's tables from the log, as the inbox's worker [`FILL`], in one
    /// transaction: every committed event after fill's place, in order, then every event an
    /// earlier fill failed on and has tried fewer times than the store's try limit. A node is
    /// written into its class's table under its key; a link into its relation's table between its
    /// two nodes. An event that cannot be filled, such as one of an undeclared class, rolls back
    /// alone: it is closed as failed with its reason and one more try, and fill carries on.
    /// Returns the place filled to.
    ///
    /// # Errors
    /// The graph file cannot be read or written; nothing of this fill lands.
    pub fn fill(&mut self) -> Result<Cursor, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ontology = load(&tx)?;
        let limit: i64 = tx.query_row(TRY_LIMIT, [], |row| row.get(0))?;
        // Tried again after the new events, so a cause they fix is fixed by then.
        let again: Vec<i64> = tx
            .prepare_cached(RETRY_WORK)?
            .query_map(params![FILL, limit], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        let from = place(&tx, FILL)?;
        let events = Events { conn: &tx }.since(from)?;
        for event in &events {
            if let Err(cause) = attempt(&tx, &ontology, &self.key, event)? {
                tx.prepare_cached(TRIED_WORK)?.execute(params![
                    FILL,
                    event.cursor.0,
                    cause.to_string()
                ])?;
            }
        }
        for seq in again {
            let Some(event) = event_at(&tx, Cursor(seq))? else {
                continue;
            };
            match attempt(&tx, &ontology, &self.key, &event)? {
                Ok(()) => tx.prepare_cached(DONE_RETRY)?.execute(params![FILL, seq])?,
                Err(cause) => {
                    tx.prepare_cached(TRIED_WORK)?
                        .execute(params![FILL, seq, cause.to_string()])?
                }
            };
        }
        let to = events.last().map_or(from, |event| event.cursor);
        tx.prepare_cached(SET_PLACE)?.execute(params![FILL, to.0])?;
        tx.commit()?;
        Ok(to)
    }

    /// Sets the store's try limit: how many times fill tries an event before it stops (3 until
    /// set).
    ///
    /// # Errors
    /// The graph file cannot be written.
    pub fn set_try_limit(&mut self, limit: u32) -> Result<(), Error> {
        self.conn.execute(SET_TRY_LIMIT, [limit])?;
        Ok(())
    }

    /// The events fill has stopped trying, oldest first, each with the reason its last try
    /// failed.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn stopped(&self) -> Result<Vec<(Cursor, String)>, Error> {
        let limit: i64 = self.conn.query_row(TRY_LIMIT, [], |row| row.get(0))?;
        Ok(self
            .conn
            .prepare_cached(STOPPED_WORK)?
            .query_map(params![FILL, limit], |row| {
                Ok((Cursor(row.get(0)?), row.get(1)?))
            })?
            .collect::<Result<_, _>>()?)
    }

    /// Sets the tries of the event at `cursor`, which fill failed on, back to none, so the next
    /// fill tries it again.
    ///
    /// # Errors
    /// [`Error::Refused`] when fill has not failed on that event; the graph file cannot be
    /// written.
    pub fn retry(&mut self, cursor: Cursor) -> Result<(), Error> {
        if self.conn.execute(RESET_TRIES, params![FILL, cursor.0])? == 0 {
            return Err(Error::Refused(format!(
                "fill has not failed on the event at {}",
                cursor.0
            )));
        }
        Ok(())
    }
}

/// Fills one event under a savepoint: the outer result is the graph file's, the inner one the
/// event's, and an event that cannot be filled leaves nothing behind.
fn attempt(
    tx: &Connection,
    ontology: &Ontology,
    key: &Key,
    event: &Event,
) -> Result<Result<(), Error>, Error> {
    tx.execute_batch(SAVEPOINT)?;
    let filled = fill_one(tx, ontology, key, event).map_err(|cause| {
        Error::Refused(format!(
            "event at {} cannot be filled: {cause}",
            event.cursor.0
        ))
    });
    tx.execute_batch(if filled.is_ok() { RELEASE } else { ROLLBACK_TO })?;
    Ok(filled)
}

/// Writes one event into the ontology's tables.
fn fill_one(
    tx: &Connection,
    ontology: &Ontology,
    store_key: &Key,
    event: &Event,
) -> Result<(), Error> {
    let recorded = event.id.recorded().map_err(Error::from)?;
    let stamp = Stamp {
        cursor: event.cursor,
        recorded,
    };
    match fact::check(&event.body, ontology)? {
        fact::Fact::Node(node) => {
            let (entity, key) = (node_entity(&node.class), node.key.as_str());
            match heard(tx, ontology, store_key, stamp, &Of::Node(&node))? {
                Heard::Held(held) => {
                    if alias(tx, &held, node.event, event.cursor)?
                        && let Some((v, recorded, _)) = newest(tx, (&entity, key, None))?
                    {
                        put_node(
                            tx,
                            &node.class,
                            key,
                            (&v.source, &v.valid, recorded),
                            &v.properties,
                        )?;
                    }
                }
                Heard::Newest => {
                    put_node(
                        tx,
                        &node.class,
                        key,
                        (&node.source, &node.valid, recorded),
                        &node.properties,
                    )?;
                    arrived(tx, ontology, &node.class, key, recorded)?;
                }
                Heard::Older => {}
            }
        }
        fact::Fact::Link(link) => {
            let entity = link_entity(&link.relation);
            let ends = (link.from.key.as_str(), link.to.key.as_str());
            match heard(tx, ontology, store_key, stamp, &Of::Link(&link))? {
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
        fact::Fact::Concluded(conclusion) => {
            conclude(tx, &conclusion)?;
        }
        fact::Fact::Declared | fact::Fact::None => {}
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

    fn declare_film(g: &mut crate::Graph) -> Result<(), crate::Error> {
        g.declare(
            &Declaration::Class(Class::new("film", ["title"])),
            r#"{"specversion":"1.0","id":"d-film","source":"rannoch:shop","type":"rannoch.declared.film"}"#,
        )?;
        Ok(())
    }

    fn tries(g: &crate::Graph, cursor: crate::Cursor) -> rusqlite::Result<i64> {
        g.conn.query_row(
            "SELECT tries FROM _work WHERE worker = ?1 AND seq = ?2",
            rusqlite::params![crate::FILL, cursor.0],
            |row| row.get(0),
        )
    }

    fn film(g: &crate::Graph, key: &str) -> Result<Option<crate::Value>, crate::Error> {
        Ok(g.find("film", key)?
            .and_then(|node| node.properties.get("title").cloned()))
    }

    #[test]
    fn fill_takes_events_through_the_inbox() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("inbox-fill")?;
        g.commit(&[&node("book", "b1", "hi")], &[])?;
        let to = g.fill()?;
        g.commit(&[&node("book", "b2", "yo")], &[])?;
        assert_eq!(g.state(crate::FILL, to)?, crate::State::Done);
        assert_eq!(
            g.state(crate::FILL, g.events().committed()?)?,
            crate::State::Waiting
        );
        Ok(())
    }

    #[test]
    fn an_event_fill_cannot_apply_fails_alone() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("fails-alone")?;
        let landed = g.commit(&[&node("film", "f1", "x"), &node("book", "b1", "hi")], &[])?;
        let f1 = landed[0].as_ref().map_err(ToString::to_string)?.cursor;
        g.fill()?;
        assert!(
            matches!(g.state(crate::FILL, f1)?, crate::State::Failed(reason) if reason.contains("film"))
        );
        assert_eq!(tries(&g, f1)?, 1);
        assert_eq!(title(&g, "b1")?, Some("hi".into()), "fill carried on");
        g.commit(&[&node("book", "b2", "yo")], &[])?;
        g.fill()?;
        assert_eq!(tries(&g, f1)?, 2, "one more try");
        assert_eq!(title(&g, "b2")?, Some("yo".into()));
        Ok(())
    }

    #[test]
    fn a_failed_event_filled_later_reads_as_if_filled_first_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let body = node("film", "f1", "x");
        let mut late = graph("late-film")?;
        let landed = late.commit(&[&body], &[])?;
        let f1 = landed[0].as_ref().map_err(ToString::to_string)?.cursor;
        late.fill()?;
        declare_film(&mut late)?;
        late.fill()?;
        assert_eq!(late.state(crate::FILL, f1)?, crate::State::Done);
        let mut first = graph("first-film")?;
        declare_film(&mut first)?;
        first.commit(&[&body], &[])?;
        first.fill()?;
        let f = crate::NodeRef::new("film", "f1");
        assert_eq!(film(&late, "f1")?, Some("x".into()));
        assert_eq!(film(&late, "f1")?, film(&first, "f1")?);
        assert_eq!(late.hearings(&f)?.len(), first.hearings(&f)?.len());
        assert_eq!(
            late.seen(&f)?.map(|s| s.count),
            first.seen(&f)?.map(|s| s.count)
        );
        Ok(())
    }

    #[test]
    fn fill_stops_at_the_try_limit() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("stops")?;
        let limit: i64 = g
            .conn
            .query_row(crate::queries::TRY_LIMIT, [], |row| row.get(0))?;
        assert_eq!(limit, 3, "3 unless set");
        g.set_try_limit(2)?;
        let landed = g.commit(&[&node("film", "f1", "x")], &[])?;
        let f1 = landed[0].as_ref().map_err(ToString::to_string)?.cursor;
        g.fill()?;
        assert!(g.stopped()?.is_empty(), "one try is under the limit");
        g.fill()?;
        g.fill()?;
        assert_eq!(tries(&g, f1)?, 2, "not tried past the limit");
        let stopped = g.stopped()?;
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].0, f1);
        assert!(stopped[0].1.contains("film"), "with its last reason");
        assert!(matches!(g.state(crate::FILL, f1)?, crate::State::Failed(_)));
        Ok(())
    }

    #[test]
    fn a_reset_event_is_tried_again() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("reset")?;
        g.set_try_limit(1)?;
        let landed = g.commit(&[&node("film", "f1", "x")], &[])?;
        let f1 = landed[0].as_ref().map_err(ToString::to_string)?.cursor;
        g.fill()?;
        declare_film(&mut g)?;
        g.fill()?;
        assert_eq!(film(&g, "f1")?, None, "stopped, so not tried");
        g.retry(f1)?;
        g.fill()?;
        assert_eq!(g.state(crate::FILL, f1)?, crate::State::Done);
        assert_eq!(film(&g, "f1")?, Some("x".into()));
        assert!(g.retry(f1).is_err(), "nothing failed to reset");
        Ok(())
    }

    #[test]
    fn the_try_count_lives_on_the_inbox_row() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("row")?;
        let body = node("film", "f1", "x");
        let landed = g.commit(&[&body], &[])?;
        let f1 = landed[0].as_ref().map_err(ToString::to_string)?.cursor;
        g.fill()?;
        g.fill()?;
        assert_eq!(tries(&g, f1)?, 2);
        let logged = g.events().since(crate::Cursor::START)?;
        let event = logged
            .iter()
            .find(|e| e.cursor == f1)
            .ok_or("f1 is logged")?;
        let written: serde_json::Value = serde_json::from_str(&body)?;
        let held: serde_json::Value = serde_json::from_str(&event.body)?;
        assert_eq!(held, written, "the event is never changed");
        Ok(())
    }
}
