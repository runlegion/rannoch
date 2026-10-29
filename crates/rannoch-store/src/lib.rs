//! Rannoch's store, on SQLite.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

mod fact;
mod history;
mod log;
mod ontology;
mod queries;
mod read;

pub use history::{At, End, Hearing, Seen, Version};
pub use log::{Cursor, Event, Events};
pub use rannoch_cloudevents::{Date, EventId, NodeRef, ValidTime};
pub use rannoch_ontology::{Class, Declaration, HistoryKey, Ontology, Relation, Value, ValueType};
pub use read::Node;

use crate::history::{
    Heard, Key, Of, Stamp, alias, conclude, heard, link_entity, newest, node_entity, take,
};
use crate::log::{append, store};
use crate::ontology::{Bound, build, load};
use crate::queries::{
    CLOSE_OPEN, FILLED, HISTORY_TABLES, OPEN_LINK, OWN_TABLES, PRAGMAS, RELEASE, ROLLBACK_TO,
    SAVEPOINT, SCHEMA_VERSION, SET_FILLED, node_id, upsert_link, upsert_node, waiting,
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
    /// order given, with a savepoint per event: an event the ontology does not allow, or that
    /// fails to append, rolls back alone, and the others land. The answer holds one result per
    /// body, in the same order: the event as logged, or why it was refused.
    ///
    /// # Errors
    /// The graph file cannot be written; nothing of the batch lands.
    pub fn commit(&mut self, bodies: &[&str]) -> Result<Vec<Result<Event, Error>>, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ontology = load(&tx)?;
        let mut results = Vec::with_capacity(bodies.len());
        for body in bodies {
            tx.execute_batch(SAVEPOINT)?;
            let landed = fact::read(body, &ontology).and_then(|fact| match fact {
                fact::Fact::Declared => Err(Error::Refused(
                    "a declaration is made with declare, not committed".to_string(),
                )),
                _ => append(&tx, body),
            });
            tx.execute_batch(if landed.is_ok() { RELEASE } else { ROLLBACK_TO })?;
            results.push(landed);
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
            match fact::read(&event.body, &ontology).map_err(failed)? {
                fact::Fact::Node(node) => {
                    let (entity, key) = (node_entity(&node.class), node.key.as_str());
                    match heard(&tx, &ontology, &self.key, stamp, &Of::Node(&node))
                        .map_err(failed)?
                    {
                        Heard::Held(held) => {
                            if alias(&tx, &held, node.event, event.cursor).map_err(failed)?
                                && let Some((v, recorded, _)) =
                                    newest(&tx, (&entity, key, None)).map_err(failed)?
                            {
                                put_node(
                                    &tx,
                                    &node.class,
                                    key,
                                    (&v.source, &v.valid, recorded),
                                    &v.properties,
                                )
                                .map_err(failed)?;
                            }
                        }
                        Heard::Newest => {
                            put_node(
                                &tx,
                                &node.class,
                                key,
                                (&node.source, &node.valid, recorded),
                                &node.properties,
                            )
                            .map_err(failed)?;
                            arrived(&tx, &ontology, &node.class, key, recorded).map_err(failed)?;
                        }
                        Heard::Older => {}
                    }
                }
                fact::Fact::Link(link) => {
                    let entity = link_entity(&link.relation);
                    let ends = (link.from.key.as_str(), link.to.key.as_str());
                    match heard(&tx, &ontology, &self.key, stamp, &Of::Link(&link))
                        .map_err(failed)?
                    {
                        Heard::Held(held) => {
                            if alias(&tx, &held, link.event, event.cursor).map_err(failed)?
                                && let Some((v, ..)) =
                                    newest(&tx, (&entity, ends.0, Some(ends.1))).map_err(failed)?
                            {
                                put_link(
                                    &tx,
                                    &ontology,
                                    &link.relation,
                                    ends,
                                    (&v.source, &v.valid, recorded),
                                )
                                .map_err(failed)?;
                            }
                        }
                        Heard::Newest => {
                            put_link(
                                &tx,
                                &ontology,
                                &link.relation,
                                ends,
                                (&link.source, &link.valid, recorded),
                            )
                            .map_err(failed)?;
                        }
                        Heard::Older => {}
                    }
                }
                fact::Fact::Concluded(conclusion) => {
                    conclude(&tx, &conclusion).map_err(failed)?;
                }
                fact::Fact::Declared | fact::Fact::None => {}
            }
        }
        tx.execute(SET_FILLED, [to.0])?;
        tx.commit()?;
        Ok(to)
    }
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
        g.commit(&[&node_with(&id_at(2_000), "book", "b1", "new")])?;
        g.fill()?;
        g.commit(&[&node_with(&id_at(1_000), "book", "b1", "old")])?;
        g.fill()?;
        assert_eq!(title(&g, "b1")?, Some("new".into()));
        Ok(())
    }

    #[test]
    fn first_and_last_heard_follow_the_axis() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("seen")?;
        g.commit(&[&node_with(&id_at(2_000), "book", "b1", "new")])?;
        g.commit(&[&node_with(&id_at(1_000), "book", "b1", "old")])?;
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
        g.commit(&[
            &node("author", "a1", "ann"),
            &node("book", "b1", "hi"),
            &link("a1", "b1"),
        ])?;
        g.commit(&[&concluded(&id(), "ended", "a1", "b1", i64::MAX / 2)])?;
        g.fill()?;
        assert!(matches!(
            g.link_end("wrote", "a1", "b1")?,
            Some(crate::End::Ended { .. })
        ));
        g.commit(&[&concluded(&id(), "corrected", "a1", "b1", 0)])?;
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
        g.commit(&[&node_with(&later, "book", "b1", "hi")])?;
        g.commit(&[&node_with(&earlier, "book", "b1", "hi")])?;
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
                g.commit(&[event.as_str()])?;
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
    fn one_refused_event_rolls_back_alone() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("alone")?;
        let before = g.events().committed()?;
        let bad = node("tweet", "t1", "x");
        let results = g.commit(&[&node("book", "b1", "hi"), &bad, &node("book", "b2", "yo")])?;
        assert!(results[0].is_ok() && results[1].is_err() && results[2].is_ok());
        assert_eq!(g.events().since(before)?.len(), 2);
        Ok(())
    }

    #[test]
    fn fill_writes_nodes_and_links_after_the_log() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("fill")?;
        g.commit(&[
            &node("author", "a1", "ann"),
            &node("book", "b1", "hi"),
            &link("a1", "b1"),
        ])?;
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
        g.commit(&[&node("author", "a1", "ann"), &link("a1", "b1")])?;
        g.fill()?;
        assert_eq!(links(&g)?, 0, "the link waits while its book is missing");
        let before = g.events().committed()?;
        g.commit(&[&node("book", "b1", "hi")])?;
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
