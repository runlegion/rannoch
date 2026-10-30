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
    Heard, Key, Of, Stamp, Subject, alias, amend, conclude, derive, heard, link_entity,
    names_renamed, newest, node_entity, reseen_of, take,
};
use crate::inbox::place;
use crate::log::{append, event_at, store};
use crate::ontology::{Bound, build, load};
use crate::queries::{
    CLOSE_OPEN, DONE_RETRY, HISTORY_TABLES, OPEN_LINK, OWN_TABLES, PRAGMAS, RELEASE, RESET_TRIES,
    RETRY_WORK, ROLLBACK_TO, SAVEPOINT, SCHEMA_VERSION, SET_PLACE, SET_POSITION, SET_TRY_LIMIT,
    STOPPED_WORK, TRIED_WORK, TRY_LIMIT, delete_link, delete_node, node_id, upsert_link,
    upsert_node, waiting,
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
        let tried = |seq: i64, cause: Error| {
            tx.prepare_cached(TRIED_WORK)?
                .execute(params![FILL, seq, cause.to_string()])
        };
        for event in &events {
            if let Err(cause) = attempt(&tx, &ontology, &self.key, event)? {
                tried(event.cursor.0, cause)?;
            }
        }
        for seq in again {
            let Some(event) = event_at(&tx, Cursor(seq))? else {
                continue;
            };
            match attempt(&tx, &ontology, &self.key, &event)? {
                Ok(()) => tx.prepare_cached(DONE_RETRY)?.execute(params![FILL, seq])?,
                Err(cause) => tried(seq, cause)?,
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
            follow(tx, ontology, store_key, event.cursor, &Of::Node(&node))?;
        }
        fact::Fact::Link(link) => fill_link(tx, (ontology, store_key), stamp, &link)?,
        fact::Fact::Concluded(conclusion) => {
            conclude(tx, &conclusion)?;
        }
        fact::Fact::None => amended(tx, ontology, store_key, event)?,
        fact::Fact::Declared => {}
    }
    Ok(())
}

/// Fills the link `link`, from the log event at `stamp`: a new hearing, or an identical one held,
/// into its relation's table (or open while an end is missing), then through any rename of its
/// ends.
fn fill_link(
    tx: &Connection,
    (ontology, key): (&Ontology, &Key),
    stamp: Stamp,
    link: &rannoch_cloudevents::LinkFact,
) -> Result<(), Error> {
    let entity = link_entity(&link.relation);
    let ends = (link.from.key.as_str(), link.to.key.as_str());
    match heard(tx, ontology, key, stamp, &Of::Link(link))? {
        Heard::Held(held) => {
            if alias(tx, &held, link.event, stamp.cursor)?
                && let Some((v, ..)) = newest(tx, (&entity, ends.0, Some(ends.1)))?
            {
                put_link(
                    tx,
                    ontology,
                    &link.relation,
                    ends,
                    (&v.source, &v.valid, stamp.recorded),
                )?;
            }
        }
        Heard::Newest => put_link(
            tx,
            ontology,
            &link.relation,
            ends,
            (&link.source, &link.valid, stamp.recorded),
        )?,
        Heard::Older => {}
    }
    follow(tx, ontology, key, stamp.cursor, &Of::Link(link))
}

/// Whether `body` is an amendment: type `rannoch.amended`.
fn amendment(body: &str) -> bool {
    rannoch_cloudevents::attributes(body).is_ok_and(|event| event.ty == "rannoch.amended")
}

/// Folds `event` when it is an amendment, and puts what it touched into the tables kept for
/// speed; any other event that says no fact fills nothing.
fn amended(tx: &Connection, ontology: &Ontology, key: &Key, event: &Event) -> Result<(), Error> {
    if !amendment(&event.body) {
        return Ok(());
    }
    let touched = amend(tx, ontology, key, &event.body, event.cursor)?;
    refresh(tx, ontology, touched)
}

/// Hears the event at `cursor`, whose fact is `of`, again through the renames of the keys it
/// names when any was renamed, so it lands on the new key, and puts what that touched into the
/// tables kept for speed.
fn follow(
    tx: &Connection,
    ontology: &Ontology,
    key: &Key,
    cursor: Cursor,
    of: &Of,
) -> Result<(), Error> {
    if !names_renamed(tx, of)? {
        return Ok(());
    }
    let touched = derive(tx, (ontology, key), cursor.0)?;
    refresh(tx, ontology, touched)
}

/// Puts the current state of every subject in `touched` into the tables kept for speed: first the
/// links that have no version known now let go of their nodes, then the nodes, then the links that
/// have one, so a renamed node's links follow it.
fn refresh(tx: &Connection, ontology: &Ontology, touched: Vec<Subject>) -> Result<(), Error> {
    let (links, nodes): (Vec<Subject>, Vec<Subject>) =
        touched.into_iter().partition(|(.., to)| to.is_some());
    let mut live = Vec::new();
    for link in links {
        let (entity, from, to) = &link;
        if newest(tx, (entity, from, to.as_deref()))?.is_some() {
            live.push(link);
        } else {
            current(tx, ontology, &link)?;
        }
    }
    for subject in nodes.iter().chain(&live) {
        current(tx, ontology, subject)?;
    }
    Ok(())
}

/// Puts the current state of `subject` into the tables kept for speed after an amendment: its
/// newest version known now, or no row once it has none (a node with links still in its
/// relations' tables is refused); and keeps what is seen of it in step.
fn current(tx: &Connection, ontology: &Ontology, subject: &Subject) -> Result<(), Error> {
    let (entity, key, to) = subject;
    reseen_of(tx, subject)?;
    let version = newest(tx, (entity, key, to.as_deref()))?;
    match to {
        None => {
            let class = entity.trim_start_matches(&node_entity(""));
            match version {
                Some((v, recorded, _)) => {
                    put_node(
                        tx,
                        class,
                        key,
                        (&v.source, &v.valid, recorded),
                        &v.properties,
                    )?;
                    arrived(tx, ontology, class, key, recorded)?;
                }
                None => {
                    tx.prepare_cached(&delete_node(class))?
                        .execute([key])
                        .map_err(|e| {
                            Error::Refused(format!(
                                "node {key} of {class} has no version known now, and links \
                                 still name it: {e}"
                            ))
                        })?;
                }
            }
        }
        Some(to) => {
            let relation = entity.trim_start_matches(&link_entity(""));
            if let Some((v, recorded, _)) = version {
                return put_link(
                    tx,
                    ontology,
                    relation,
                    (key, to),
                    (&v.source, &v.valid, recorded),
                );
            }
            let declared = ontology.relation(relation)?;
            tx.prepare_cached(&delete_link(relation, &declared.from, &declared.to))?
                .execute([key, to])?;
            tx.prepare_cached(CLOSE_OPEN)?
                .execute(rusqlite::params![relation, key, to])?;
        }
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

    /// A book node recorded at `seconds`, valid on 2024-01-01, titled `title`.
    fn book_at(seconds: u64, key: &str, title: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.node.book","subject":"{key}","data":{{"properties":{{"title":"{title}"}},"valid":{{"on":"2024-01-01"}}}}}}"#,
            id_at(seconds)
        )
    }

    /// An amendment recorded at `seconds` of the logged event `target`, its data `patch`.
    fn amended(seconds: u64, target: crate::EventId, patch: &str) -> String {
        format!(
            r#"{{"specversion":"1.0","id":"{}","source":"rannoch:fixer","type":"rannoch.amended","subject":"{target}","data":{patch}}}"#,
            id_at(seconds)
        )
    }

    /// Commits one body and returns the event the log holds for it.
    fn logged(
        g: &mut crate::Graph,
        body: &str,
    ) -> Result<crate::Event, Box<dyn std::error::Error>> {
        Ok(g.commit(&[body], &[])?.remove(0)?)
    }

    fn known_at(seconds: u64) -> crate::At {
        crate::At::Time(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds))
    }

    fn title_known(
        g: &crate::Graph,
        key: &str,
        seconds: u64,
    ) -> Result<Option<crate::Value>, crate::Error> {
        let day = crate::Date::day(2024, 1, 1);
        Ok(g.find_as_of("book", key, &day, Some(known_at(seconds)))?
            .and_then(|node| node.properties.get("title").cloned()))
    }

    #[test]
    fn an_amendment_merge_patches_the_event_it_names() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("patch")?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "tpyo"))?;
        let b2 = logged(&mut g, &book_at(1_000, "b2", "gone"))?;
        g.commit(
            &[
                &amended(2_000, b1.id, r#"{"data":{"properties":{"title":"typo"}}}"#),
                &amended(2_000, b2.id, r#"{"data":{"properties":{"title":null}}}"#),
            ],
            &[],
        )?;
        g.fill()?;
        let b1 = g.find("book", "b1")?.ok_or("b1 is still there")?;
        assert_eq!(
            b1.properties.get("title"),
            Some(&"typo".into()),
            "a value replaces"
        );
        assert_eq!(b1.source, "shop", "an omitted field stays");
        let b2 = g.find("book", "b2")?.ok_or("b2 is still there")?;
        assert_eq!(b2.properties.get("title"), None, "null removes");
        Ok(())
    }

    #[test]
    fn an_amended_event_stays_in_the_log_as_written() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("as-written")?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "tpyo"))?;
        let fix = logged(
            &mut g,
            &amended(2_000, b1.id, r#"{"data":{"properties":{"title":"typo"}}}"#),
        )?;
        g.fill()?;
        assert_eq!(
            g.events().at(b1.cursor)?,
            Some(b1.clone()),
            "the event is as written"
        );
        assert!(
            fix.cursor > b1.cursor,
            "the amendment is appended like any fact"
        );
        assert_eq!(g.events().committed()?, fix.cursor);
        assert_eq!(
            title(&g, "b1")?,
            Some("typo".into()),
            "and a query applies it"
        );
        Ok(())
    }

    #[test]
    fn a_read_before_the_amendment_reads_the_event_as_written()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("known")?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "tpyo"))?;
        g.commit(
            &[&amended(
                2_000,
                b1.id,
                r#"{"data":{"properties":{"title":"typo"}}}"#,
            )],
            &[],
        )?;
        g.fill()?;
        assert_eq!(title_known(&g, "b1", 1_500)?, Some("tpyo".into()));
        assert_eq!(title_known(&g, "b1", 2_500)?, Some("typo".into()));
        Ok(())
    }

    #[test]
    fn amendments_stack_in_point_order() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("stack")?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "one"))?;
        g.commit(
            &[
                &amended(2_000, b1.id, r#"{"data":{"properties":{"title":"two"}}}"#),
                &amended(3_000, b1.id, r#"{"source":"rannoch:editor"}"#),
            ],
            &[],
        )?;
        g.fill()?;
        let now = g.find("book", "b1")?.ok_or("b1 is there")?;
        assert_eq!(now.properties.get("title"), Some(&"two".into()));
        assert_eq!(
            now.source, "editor",
            "the later amendment patches the amended event"
        );
        assert_eq!(title_known(&g, "b1", 1_500)?, Some("one".into()));
        assert_eq!(title_known(&g, "b1", 2_500)?, Some("two".into()));
        Ok(())
    }

    #[test]
    fn renaming_a_node_amends_its_events_subject() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("rename")?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "hi"))?;
        g.commit(&[&amended(2_000, b1.id, r#"{"subject":"b9"}"#)], &[])?;
        g.fill()?;
        assert_eq!(
            title(&g, "b9")?,
            Some("hi".into()),
            "it answers under its new key"
        );
        assert!(g.find("book", "b1")?.is_none(), "and not under its old one");
        assert_eq!(title_known(&g, "b1", 1_500)?, Some("hi".into()));
        assert_eq!(title_known(&g, "b9", 1_500)?, None);
        assert_eq!(title_known(&g, "b9", 2_500)?, Some("hi".into()));
        assert_eq!(title_known(&g, "b1", 2_500)?, None);
        Ok(())
    }

    #[test]
    fn the_tables_kept_for_speed_follow_an_amendment() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("kept")?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "tpyo"))?;
        let b2 = logged(&mut g, &book_at(1_000, "b2", "moved"))?;
        g.fill()?;
        g.commit(
            &[
                &amended(2_000, b1.id, r#"{"data":{"properties":{"title":"typo"}}}"#),
                &amended(2_000, b2.id, r#"{"subject":"b3"}"#),
            ],
            &[],
        )?;
        g.fill()?;
        let kept = |key: &str| -> rusqlite::Result<Option<String>> {
            use rusqlite::OptionalExtension;
            g.conn
                .query_row(
                    &format!(
                        "SELECT {} FROM {} WHERE key = ?1",
                        crate::queries::property_column("title"),
                        crate::queries::class_table("book")
                    ),
                    [key],
                    |row| row.get(0),
                )
                .optional()
        };
        assert_eq!(kept("b1")?, Some("typo".to_string()));
        assert_eq!(kept("b2")?, None, "the old key's row is gone");
        assert_eq!(kept("b3")?, Some("moved".to_string()));
        let seen = g
            .seen(&crate::NodeRef::new("book", "b3"))?
            .ok_or("b3 is seen")?;
        assert_eq!(
            seen.last_heard.properties.get("title"),
            Some(&"moved".into())
        );
        assert!(g.seen(&crate::NodeRef::new("book", "b2"))?.is_none());
        Ok(())
    }

    /// A store with author a1, book b1 titled "hi" and a1 wrote b1, all recorded at 1,000 and
    /// filled; returns the book's and the link's logged events.
    fn wrote_b1(
        name: &str,
    ) -> Result<(crate::Graph, crate::Event, crate::Event), Box<dyn std::error::Error>> {
        let mut g = graph(name)?;
        logged(&mut g, &node_with(&id_at(1_000), "author", "a1", "ann"))?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "hi"))?;
        let wrote = logged(&mut g, &link_with(&id_at(1_000), "a1", "b1"))?;
        g.fill()?;
        Ok((g, b1, wrote))
    }

    /// The links in `wrote`'s table, and the links waiting in `_open`.
    fn kept_links(g: &crate::Graph) -> rusqlite::Result<(i64, i64)> {
        let table: i64 = g.conn.query_row(
            &format!(
                "SELECT count(*) FROM {}",
                crate::queries::relation_table("wrote")
            ),
            [],
            |row| row.get(0),
        )?;
        let open: i64 = g
            .conn
            .query_row("SELECT count(*) FROM _open", [], |row| row.get(0))?;
        Ok((table, open))
    }

    fn known_link(
        g: &crate::Graph,
        to: &str,
        seconds: u64,
    ) -> Result<Option<crate::Version>, crate::Error> {
        g.link_known_at("wrote", "a1", to, known_at(seconds))
    }

    #[test]
    fn a_renamed_nodes_links_answer_under_its_new_key() -> Result<(), Box<dyn std::error::Error>> {
        let (mut g, b1, _) = wrote_b1("rename-links")?;
        g.commit(&[&amended(2_000, b1.id, r#"{"subject":"b9"}"#)], &[])?;
        g.fill()?;
        assert!(
            g.link_seen("wrote", "a1", "b9")?.is_some(),
            "after: new key"
        );
        assert!(g.link_seen("wrote", "a1", "b1")?.is_none());
        assert!(known_link(&g, "b1", 1_500)?.is_some(), "before: old key");
        assert!(known_link(&g, "b9", 1_500)?.is_none());
        assert!(known_link(&g, "b9", 2_500)?.is_some());
        assert!(known_link(&g, "b1", 2_500)?.is_none());
        assert_eq!(kept_links(&g)?, (1, 0), "one link row, none waiting");
        g.commit(&[&book_at(3_000, "b2", "next")], &[])?;
        g.fill()?;
        assert_eq!(title(&g, "b2")?, Some("next".into()), "later fills go on");
        Ok(())
    }

    #[test]
    fn a_second_rename_maps_on_from_the_new_key() -> Result<(), Box<dyn std::error::Error>> {
        let (mut g, b1, _) = wrote_b1("rename-twice")?;
        g.commit(
            &[
                &amended(2_000, b1.id, r#"{"subject":"b9"}"#),
                &amended(3_000, b1.id, r#"{"subject":"b10"}"#),
            ],
            &[],
        )?;
        g.fill()?;
        assert_eq!(title(&g, "b10")?, Some("hi".into()));
        assert!(g.find("book", "b9")?.is_none());
        assert!(
            g.link_seen("wrote", "a1", "b10")?.is_some(),
            "the link went on too"
        );
        assert!(g.link_seen("wrote", "a1", "b9")?.is_none());
        assert!(known_link(&g, "b9", 2_500)?.is_some());
        assert!(known_link(&g, "b10", 2_500)?.is_none());
        assert_eq!(kept_links(&g)?, (1, 0));
        Ok(())
    }

    #[test]
    fn an_amended_link_keeps_its_renamed_end() -> Result<(), Box<dyn std::error::Error>> {
        let (mut g, b1, wrote) = wrote_b1("rename-amend-link")?;
        // The link's amendment arrives before the rename it comes after on the axis.
        g.commit(
            &[
                &amended(3_000, wrote.id, r#"{"source":"rannoch:editor"}"#),
                &amended(2_000, b1.id, r#"{"subject":"b9"}"#),
            ],
            &[],
        )?;
        g.fill()?;
        let now = g.link_hearings("wrote", "a1", "b9")?;
        assert_eq!(now.len(), 1);
        assert_eq!(now[0].source, "editor", "amended, on the new key");
        assert!(g.link_hearings("wrote", "a1", "b1")?.is_empty());
        let between = known_link(&g, "b9", 2_500)?.ok_or("moved by 2,500")?;
        assert_eq!(between.source, "shop", "not yet amended at 2,500");
        assert_eq!(kept_links(&g)?, (1, 0));
        Ok(())
    }

    #[test]
    fn a_link_arriving_after_a_rename_lands_on_the_new_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut g, b1, _) = wrote_b1("rename-late-link")?;
        logged(&mut g, &node_with(&id_at(1_000), "author", "a2", "bob"))?;
        g.commit(&[&amended(2_000, b1.id, r#"{"subject":"b9"}"#)], &[])?;
        g.fill()?;
        g.commit(&[&link_with(&id_at(3_000), "a2", "b1")], &[])?;
        g.fill()?;
        assert!(g.link_seen("wrote", "a2", "b9")?.is_some());
        assert!(g.link_seen("wrote", "a2", "b1")?.is_none());
        assert!(
            g.link_known_at("wrote", "a2", "b9", known_at(2_500))?
                .is_none(),
            "not known before it arrived"
        );
        assert_eq!(
            kept_links(&g)?,
            (2, 0),
            "it lands in the table, not in _open"
        );
        Ok(())
    }

    #[test]
    fn a_link_reads_as_written_before_its_amendment() -> Result<(), Box<dyn std::error::Error>> {
        let (mut g, _, wrote) = wrote_b1("link-known")?;
        g.commit(
            &[&amended(2_000, wrote.id, r#"{"source":"rannoch:editor"}"#)],
            &[],
        )?;
        g.fill()?;
        let before = known_link(&g, "b1", 1_500)?.ok_or("known at 1,500")?;
        assert_eq!(before.source, "shop");
        let after = known_link(&g, "b1", 2_500)?.ok_or("known at 2,500")?;
        assert_eq!(after.source, "editor");
        Ok(())
    }

    #[test]
    fn renaming_onto_a_key_with_facts_counts_them_all() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("rename-merge")?;
        let b2 = logged(&mut g, &book_at(1_000, "b2", "two"))?;
        logged(&mut g, &book_at(1_500, "b3", "three"))?;
        g.commit(&[&amended(2_000, b2.id, r#"{"subject":"b3"}"#)], &[])?;
        g.fill()?;
        let seen = g
            .seen(&crate::NodeRef::new("book", "b3"))?
            .ok_or("b3 is seen")?;
        assert_eq!(seen.count, 2);
        assert_eq!(
            seen.first_heard.properties.get("title"),
            Some(&"two".into())
        );
        assert_eq!(
            seen.last_heard.properties.get("title"),
            Some(&"three".into())
        );
        assert_eq!(title(&g, "b3")?, Some("three".into()));
        assert!(g.seen(&crate::NodeRef::new("book", "b2"))?.is_none());
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

    #[test]
    fn an_amendment_fill_cannot_apply_never_stops_fill() -> Result<(), Box<dyn std::error::Error>> {
        let mut elsewhere = graph("elsewhere")?;
        let stranger = logged(&mut elsewhere, &book_at(1_000, "b0", "not here"))?;
        let mut g = graph("unappliable")?;
        g.set_try_limit(1)?;
        let bad = logged(
            &mut g,
            &amended(
                2_000,
                stranger.id,
                r#"{"data":{"properties":{"title":"x"}}}"#,
            ),
        )?;
        logged(&mut g, &book_at(3_000, "b2", "after"))?;
        g.fill()?;
        assert_eq!(
            title(&g, "b2")?,
            Some("after".into()),
            "the event after it fills"
        );
        let stopped = g.stopped()?;
        assert_eq!(stopped.len(), 1, "the amendment stops at the limit");
        assert_eq!(stopped[0].0, bad.cursor);
        Ok(())
    }

    #[test]
    fn an_amendment_before_the_latest_one_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("before")?;
        g.set_try_limit(1)?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "one"))?;
        logged(
            &mut g,
            &amended(2_000, b1.id, r#"{"data":{"properties":{"title":"two"}}}"#),
        )?;
        g.fill()?;
        let late = logged(
            &mut g,
            &amended(1_500, b1.id, r#"{"data":{"properties":{"title":"x"}}}"#),
        )?;
        let before = logged(
            &mut g,
            &amended(500, b1.id, r#"{"data":{"properties":{"title":"y"}}}"#),
        )?;
        g.fill()?;
        assert_eq!(
            title(&g, "b1")?,
            Some("two".into()),
            "it reads as amended at 2000"
        );
        assert_eq!(title_known(&g, "b1", 1_700)?, Some("one".into()));
        let stopped: Vec<crate::Cursor> = g.stopped()?.into_iter().map(|(at, _)| at).collect();
        assert_eq!(stopped, [late.cursor, before.cursor], "both are refused");
        Ok(())
    }

    #[test]
    fn amendments_at_one_point_stack_by_event_id() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("same-point")?;
        let b1 = logged(&mut g, &book_at(1_000, "b1", "one"))?;
        let first = amended(2_000, b1.id, r#"{"data":{"properties":{"title":"a"}}}"#);
        let second = amended(2_000, b1.id, r#"{"data":{"properties":{"title":"b"}}}"#);
        let id = |body: &str| -> Result<String, serde_json::Error> {
            let v: serde_json::Value = serde_json::from_str(body)?;
            Ok(v["id"].as_str().unwrap_or_default().to_owned())
        };
        let last = if id(&first)? < id(&second)? { "b" } else { "a" };
        g.commit(&[&second, &first], &[])?;
        g.fill()?;
        assert!(g.stopped()?.is_empty(), "one point is not before another");
        assert_eq!(
            title(&g, "b1")?,
            Some(last.into()),
            "the higher event id is on top"
        );
        Ok(())
    }
}
