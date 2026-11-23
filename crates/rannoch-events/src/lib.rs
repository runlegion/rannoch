#![warn(missing_docs)]
//! The events crate: it owns `_events`, appends each batch to it in one
//! write, and signals the queue once that write has committed.
//!
//! [`Events`] is the append an app puts in core's `Config`: each inbox hands it its batches.
//! Each event becomes one `_events` row with its fields as given: `id` and `source` as text,
//! `ty` as its `u16`, `data` as its JSON from serde, `run_at` as RFC 3339 in UTC to the
//! millisecond, and `status` as its int. This crate only inserts rows; it updates and deletes
//! none. The one row written to `_events` by anything else is rannoch-queue's -1 for an event
//! whose `ty` is not the app's, which the app's `Sender` cannot take.
//!
//! The append signals once for each batch it commits, whether or not the queue still listens.
//! The receiver is the queue's: rannoch-queue's `Queue` stops once every sender on that channel
//! has dropped, and the sender in an [`Events`] drops when that `Events` does.
//!
//! [`write_in`] writes a batch and its 2s' items inside the caller's transaction, for an append
//! that writes rows of its own in the same commit, as rannoch-graph's `GraphAppend` does.
//!
//! An `Events` made [`with_writes`](Events::with_writes) also writes the store rows that
//! rannoch-processor's `store` step carries in its [`Writes`]: for each 2 in the batch,
//! [`taking`] takes the items under the 2's source, as rannoch-graph's `GraphAppend` does, and
//! [`write_in`] inserts each [`StoreRow`] in the batch's transaction, so every 2 has its rows. An
//! item with a `store` key that is not a `StoreRow` fails the batch, and an item with none, as a
//! graph `Op`, is not written. A batch that fails puts the items back for its retry. The row is
//! inserted with `INSERT ... ON CONFLICT DO NOTHING`, so T's own keys decide whether a rerun adds
//! a row: a string is bound as text, an integer or a float as an integer or a real, a bool as 0
//! or 1, null as NULL, and an array or an object as its JSON text. A row T refuses, by a NOT
//! NULL, CHECK, foreign key or type rule or a trigger, is rolled back with the rest of its 2's
//! items, and that 2 is not written: the append returns in its place a -1 for the 2's source, its
//! data `store: ` and the store's message, as `store: NOT NULL constraint failed:
//! payments.amount`, which the inbox sends; the rest of the batch commits, the 2's nexts
//! included.
//!
//! # Failures
//!
//! Apart from a 2 whose store row is refused, a batch is written whole or not at all. When an
//! event's `data` does not serialize, or a row cannot be inserted, as with an id already in
//! `_events`, the append fails with `Error::Store`, writes nothing, and sends no signal.
//!
//! # Example
//!
//! The append on its own, over a store migrated as in rannoch-store's example: a batch written
//! and signalled, then a batch that fails whole. Through rannoch, an inbox makes these calls and
//! sets each id at accept; called directly, the append writes the ids it is given.
//!
//! ```
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//! use std::sync::mpsc;
//!
//! use rannoch_core::{Append, Event, Status, Storage};
//! use rannoch_events::Events;
//! use rannoch_store::{Opened, Store};
//! use rusqlite::Connection;
//! use serde_json::json;
//! use uuid::Uuid;
//!
//! /// The app's event types, written to `_events` as their `u16`.
//! #[derive(Debug, Clone, Copy)]
//! enum Ty {
//!     Page = 1,
//! }
//!
//! impl From<Ty> for u16 {
//!     fn from(ty: Ty) -> u16 {
//!         ty as u16
//!     }
//! }
//!
//! /// A connection to the migrated database at `path`.
//! fn ready(path: &Path) -> Result<Connection, Box<dyn Error>> {
//!     match rannoch_store::open(path, &rannoch_core::migrator())? {
//!         Opened::Ready(conn) => Ok(conn),
//!         Opened::Pending(versions) => Err(format!("migrations pending: {versions:?}").into()),
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let dir = std::env::temp_dir().join(Uuid::now_v7().to_string());
//!     fs::create_dir_all(&dir)?;
//!     let path = dir.join("app.db");
//!     let store = Store::new(path.clone(), rannoch_core::migrator());
//!     if store.pending()? {
//!         store.apply()?;
//!     }
//!
//!     let (signal, wake) = mpsc::channel();
//!     let events = Events::new(ready(&path)?, signal);
//!     let page = |id| Event {
//!         id,
//!         source: None,
//!         ty: Ty::Page,
//!         data: json!({ "url": "/about" }),
//!         run_at: None,
//!         status: Status::Added,
//!     };
//!
//!     // One batch, one write, one signal.
//!     let first = Uuid::now_v7();
//!     events.append(&[page(first)])?;
//!     wake.try_recv()?;
//!
//!     // A batch holding an id already written fails whole: nothing written, no signal.
//!     let failed = events.append(&[page(Uuid::now_v7()), page(first)]);
//!     assert_eq!(failed.err(), Some(rannoch_core::Error::Store));
//!     assert!(wake.try_recv().is_err());
//!
//!     let rows: i64 = ready(&path)?.query_row("SELECT count(*) FROM _events", [], |r| r.get(0))?;
//!     assert_eq!(rows, 1);
//!
//!     drop((events, store));
//!     fs::remove_dir_all(&dir)?;
//!     Ok(())
//! }
//! ```

mod queries;

use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::SecondsFormat;
use rannoch_core::{Append, Error, Event, Status};
use rusqlite::types::Value as Sql;
use rusqlite::{CachedStatement, Connection, ErrorCode, Transaction, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;
/// Where this crate's migratr migrations directory is, for the app's `build.rs`.
pub const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// rannoch-processor's graph writes and store rows by the id of the event they are about, until
/// the append that carries its 2 takes them.
pub type Writes = Arc<Mutex<HashMap<Uuid, Vec<Value>>>>;

/// The row rannoch-processor's `store` step carries to the append that writes its event's 2,
/// `{"store": {"table": T, "row": {column: value}}}`; any other field is refused.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum StoreRow {
    /// One row into `table`.
    Store {
        /// The table.
        table: String,
        /// The row's values, by column.
        row: Map<String, Value>,
    },
}

impl StoreRow {
    /// The `INSERT ... ON CONFLICT DO NOTHING` of the row, a parameter for each column in order,
    /// prepared on `conn` and not run; each name is quoted.
    /// # Errors
    /// The store's error, as `no such table: <table>` or `table <table> has no column named
    /// <column>`.
    pub fn insert<'c>(&self, conn: &'c Connection) -> rusqlite::Result<CachedStatement<'c>> {
        let Self::Store { table, row } = self;
        let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let columns: Vec<String> = row.keys().map(|c| quote(c)).collect();
        let (columns, marks) = (columns.join(", "), vec!["?"; row.len()].join(", "));
        let sql = format!(
            "INSERT INTO {} ({columns}) VALUES ({marks}) ON CONFLICT DO NOTHING",
            quote(table)
        );
        conn.prepare_cached(&sql)
    }
}

/// The events crate's append for the app's `Config`: the connection it
/// writes `_events` through, the queue it signals and the store rows it writes.
#[derive(Debug)]
pub struct Events {
    conn: Mutex<Connection>,
    queue: Sender<()>,
    writes: Writes,
}

impl Events {
    /// The append over `conn`, signalling `queue` after each commit.
    #[must_use]
    pub fn new(conn: Connection, queue: Sender<()>) -> Self {
        Self {
            conn: Mutex::new(conn),
            queue,
            writes: Writes::default(),
        }
    }

    /// The append, writing with each 2 the store rows rannoch-processor's `writes` holds under
    /// the 2's source.
    #[must_use]
    pub fn with_writes(self, writes: Writes) -> Self {
        Self { writes, ..self }
    }
}

impl<T: Copy + Into<u16>, D: Serialize + From<String>> Append<T, D> for Events {
    fn append(&self, batch: &[Event<T, D>]) -> Result<Vec<Event<T, D>>, Error> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        let replies = taking(&self.writes, batch, |taken| {
            let mut tx = conn.transaction().map_err(|_| Error::Store)?;
            let replies = write_in(&mut tx, batch, taken, |_, _, _| Ok(()))?;
            tx.commit().map_err(|_| Error::Store)?;
            Ok(replies)
        })?;
        let _ = self.queue.send(());
        Ok(replies)
    }
}

/// Takes the items `writes` holds under the source of each 2 in `batch` and hands them to
/// `commit`. When it fails they go back for the batch's retry; a rerun's newer items stay.
/// # Errors
/// `commit`'s.
pub fn taking<T, D, R>(
    writes: &Writes,
    batch: &[Event<T, D>],
    commit: impl FnOnce(&[(Uuid, Vec<Value>)]) -> Result<R, Error>,
) -> Result<R, Error> {
    let held = || writes.lock().unwrap_or_else(PoisonError::into_inner);
    let twos = batch.iter().filter(|e| e.status == Status::Processed);
    // Held only to take them, so steps keep putting their writes while this batch commits.
    let taken: Vec<_> = twos
        .filter_map(|e| held().remove_entry(&e.source?))
        .collect();
    let done = commit(&taken);
    for (source, items) in taken.into_iter().filter(|_| done.is_err()) {
        held().entry(source).or_insert(items);
    }
    done
}

/// Appends `batch` to `_events` in one write, each event as one row with its
/// fields as given, and signals `queue` once that write has committed.
///
/// # Errors
///
/// `Error::Store` when the append fails; nothing is written and `queue` is
/// not signalled.
pub fn append<T, D>(
    conn: &mut Connection,
    batch: &[Event<T, D>],
    queue: &Sender<()>,
) -> Result<(), Error>
where
    T: Copy + Into<u16>,
    D: Serialize,
{
    let tx = conn.transaction().map_err(|_| Error::Store)?;
    append_in(&tx, batch)?;
    tx.commit().map_err(|_| Error::Store)?;
    // The events are kept whether or not the queue is still listening.
    let _ = queue.send(());
    Ok(())
}

/// Writes `batch` in `tx` with the items `taken` for its 2s, each 2's items inside a savepoint of
/// their own: a `store` row with `ON CONFLICT DO NOTHING`, so T's own keys decide whether it adds
/// a row, and any other item with `other`, which gets the 2's source. When T refuses a store row,
/// by any of its constraints, the savepoint is rolled back and that 2 is not written: returned in
/// its place is a -1 for its source, its data naming the reason, for the caller to send. The
/// caller commits.
/// # Errors
/// `Error::Store` when an event cannot be inserted, `other` fails, or a store row fails for a
/// reason other than a constraint, as an item with a `store` key that is not a [`StoreRow`], or a
/// table or a column the store does not have.
pub fn write_in<T, D>(
    tx: &mut Transaction<'_>,
    batch: &[Event<T, D>],
    taken: &[(Uuid, Vec<Value>)],
    mut other: impl FnMut(&Connection, &Uuid, &Value) -> Result<(), Error>,
) -> Result<Vec<Event<T, D>>, Error>
where
    T: Copy + Into<u16>,
    D: Serialize + From<String>,
{
    let mut refused = Vec::new();
    for (source, items) in taken {
        let point = tx.savepoint().map_err(|_| Error::Store)?;
        let written = items.iter().try_for_each(|item| {
            if store_in(&point, item)? {
                return Ok(());
            }
            other(&point, source, item).map_err(|_| Unwritten::Failed)
        });
        let ended = match written {
            Ok(()) => point.commit(),
            Err(Unwritten::Failed) => return Err(Error::Store),
            Err(Unwritten::Refused(reason)) => {
                refused.push((*source, reason));
                // A savepoint finishes by rolling back to where it began.
                point.finish()
            }
        };
        ended.map_err(|_| Error::Store)?;
    }
    let two_of =
        |e: &Event<T, D>, source| e.status == Status::Processed && e.source == Some(source);
    let kept = batch
        .iter()
        .filter(|e| !refused.iter().any(|(s, _)| two_of(e, *s)));
    append_in(tx, kept)?;
    let binned = refused.into_iter().filter_map(|(source, reason)| {
        let two = batch.iter().find(|e| two_of(e, source))?;
        Some(Event {
            id: Uuid::nil(),
            source: Some(source),
            ty: two.ty,
            data: D::from(reason),
            run_at: None,
            status: Status::Binned,
        })
    });
    Ok(binned.collect())
}

/// Why a 2's items were not written.
enum Unwritten {
    /// T refused a store row by one of its constraints: `store: ` and the store's message.
    Refused(String),
    /// Anything else, which fails the batch.
    Failed,
}

/// Inserts `item` on `conn` when it has a `store` key, as a [`StoreRow`]: true; false for an item
/// with no `store` key, left to the caller.
fn store_in(conn: &Connection, item: &Value) -> Result<bool, Unwritten> {
    if item.get("store").is_none() {
        return Ok(false);
    }
    let stored = StoreRow::deserialize(item).map_err(|_| Unwritten::Failed)?;
    let StoreRow::Store { row, .. } = &stored;
    let values = row.values().map(|value| match value {
        Value::Null => Sql::Null,
        Value::Bool(b) => Sql::Integer(i64::from(*b)),
        Value::Number(n) => n
            .as_i64()
            .map_or(Sql::Real(n.as_f64().unwrap_or_default()), Sql::Integer),
        Value::String(s) => Sql::Text(s.clone()),
        value => Sql::Text(value.to_string()),
    });
    let mut insert = stored.insert(conn).map_err(|_| Unwritten::Failed)?;
    match insert.execute(params_from_iter(values)) {
        Ok(_) => Ok(true),
        Err(e) if e.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) => {
            Err(Unwritten::Refused(format!("store: {e}")))
        }
        Err(_) => Err(Unwritten::Failed),
    }
}

/// Appends `batch` to `_events` inside `tx`; the caller commits and signals the queue.
fn append_in<'e, T: Copy + Into<u16> + 'e, D: Serialize + 'e>(
    tx: &Transaction<'_>,
    batch: impl IntoIterator<Item = &'e Event<T, D>>,
) -> Result<(), Error> {
    let mut insert = tx.prepare(queries::APPEND).map_err(|_| Error::Store)?;
    for event in batch {
        let data = serde_json::to_string(&event.data).map_err(|_| Error::Store)?;
        insert
            .execute(params![
                event.id.to_string(),
                event.source.map(|s| s.to_string()),
                event.ty.into(),
                data,
                event
                    .run_at
                    .map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true)),
                event.status as i8,
            ])
            .map_err(|_| Error::Store)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, mpsc};

    use rannoch_core::{Append, Error, Event, Status};
    use serde_json::json;
    use uuid::Uuid;

    use super::{Events, append};

    #[derive(Debug, Clone, Copy)]
    #[repr(u16)]
    enum Ty {
        Page = 7,
    }

    impl From<Ty> for u16 {
        fn from(ty: Ty) -> Self {
            ty as u16
        }
    }

    fn event(id: Uuid) -> Event<Ty, String> {
        Event {
            id,
            source: None,
            ty: Ty::Page,
            data: "data".to_owned(),
            run_at: None,
            status: Status::Added,
        }
    }

    fn count(conn: &rusqlite::Connection) -> i64 {
        conn.query_row("SELECT count(*) FROM _events", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn a_batch_that_fails_part_way_writes_none_of_it() {
        let mut store = rannoch_core::forgr::empty_store();
        let (queue, signals) = mpsc::channel();
        let first = Uuid::now_v7();
        let batch = [event(first), event(Uuid::now_v7()), event(first)];

        assert_eq!(append(&mut store, &batch, &queue), Err(Error::Store));
        assert_eq!(count(&store), 0);
        assert!(signals.try_recv().is_err());
    }

    #[test]
    fn events_appends_through_core_append_and_signals() {
        let (queue, signals) = mpsc::channel();
        let events = Events::new(rannoch_core::forgr::empty_store(), queue);
        let batch = [event(Uuid::now_v7()), event(Uuid::now_v7())];

        Append::append(&events, &batch).unwrap();

        assert_eq!(count(&events.conn.lock().unwrap()), 2);
        assert_eq!(signals.try_recv(), Ok(()));
    }

    #[test]
    fn with_writes_inserts_each_2s_store_rows_and_a_failed_batch_puts_them_back() {
        let store = rannoch_core::forgr::empty_store();
        let sql = "CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER, b INTEGER, l TEXT, z TEXT)";
        store.execute_batch(sql).unwrap();
        let (queue, signals) = mpsc::channel();
        let writes = Arc::new(Mutex::new(HashMap::new()));
        let row = json!({ "k": "a", "n": 2, "b": true, "l": [1], "z": null });
        let item = |table| json!({ "store": { "table": table, "row": row } });
        let op = json!({ "set": { "node": "a", "key": "k", "value": 1 } });
        let two = |source| Event {
            source: Some(source),
            status: Status::Processed,
            ..event(Uuid::now_v7())
        };
        let events = Events::new(store, queue).with_writes(writes.clone());
        let rows = |events: &Events| {
            let conn = events.conn.lock().unwrap();
            let sql = "SELECT k, n, b, l, z FROM t";
            let mut rows = conn.prepare(sql).unwrap();
            let rows = rows.query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            });
            let rows: Vec<(String, i64, i64, String, Option<String>)> =
                rows.unwrap().collect::<rusqlite::Result<_>>().unwrap();
            rows
        };

        // Two 2s, each of a source holding a store row of key a and a graph Op: the first's row is
        // written, the second's adds none, and neither's Op is written.
        for _ in 0..2 {
            let source = Uuid::now_v7();
            writes
                .lock()
                .unwrap()
                .insert(source, vec![item("t"), op.clone()]);
            Append::append(&events, &[two(source)]).unwrap();
            assert_eq!(signals.try_recv(), Ok(()));
        }
        let a = ("a".to_owned(), 2, 1, "[1]".to_owned(), None);
        assert_eq!(rows(&events), [a]);
        assert!(writes.lock().unwrap().is_empty());

        // A store row into a table the store does not have fails the batch, which puts it back.
        let source = Uuid::now_v7();
        writes.lock().unwrap().insert(source, vec![item("nope")]);
        let failed = Append::append(&events, &[event(Uuid::now_v7()), two(source)]);
        assert_eq!(failed.err(), Some(Error::Store));
        assert!(signals.try_recv().is_err());
        assert_eq!(count(&events.conn.lock().unwrap()), 2);
        assert_eq!(writes.lock().unwrap()[&source], [item("nope")]);

        // An item with a store key that is not a StoreRow fails the batch the same way.
        let near = Uuid::now_v7();
        let item = json!({ "store": { "table": "t" } });
        writes.lock().unwrap().insert(near, vec![item]);
        let failed = Append::append(&events, &[two(near)]);
        assert_eq!(failed.err(), Some(Error::Store));
    }

    #[test]
    fn a_store_row_t_refuses_is_rolled_back_with_its_2_and_a_minus_1_is_returned() {
        let store = rannoch_core::forgr::empty_store();
        let sql = "CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER NOT NULL)";
        store.execute_batch(sql).unwrap();
        let (queue, signals) = mpsc::channel();
        let writes = Arc::new(Mutex::new(HashMap::new()));
        let events = Events::new(store, queue).with_writes(writes.clone());
        // `n` is null, as `eval` gives for a field the input lacks.
        let item = json!({ "store": { "table": "t", "row": { "k": "a", "n": null } } });
        let source = Uuid::now_v7();
        writes.lock().unwrap().insert(source, vec![item]);
        let two = Event {
            source: Some(source),
            status: Status::Processed,
            ..event(Uuid::now_v7())
        };
        let other = event(Uuid::now_v7());

        let replies = Append::append(&events, &[other.clone(), two]).unwrap();

        // The rest of the batch commits and signals; the 2 and its row are not written.
        assert_eq!(signals.try_recv(), Ok(()));
        let conn = events.conn.lock().unwrap();
        let ids = "SELECT id FROM _events";
        let ids: String = conn.query_row(ids, [], |r| r.get(0)).unwrap();
        assert_eq!((count(&conn), ids), (1, other.id.to_string()));
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0);
        assert!(writes.lock().unwrap().is_empty());
        // In its place, a -1 for its source naming the reason, for the inbox to send.
        let [binned] = replies.as_slice() else {
            panic!("{replies:?}")
        };
        assert_eq!(
            (binned.source, binned.status),
            (Some(source), Status::Binned)
        );
        assert_eq!(binned.data, "store: NOT NULL constraint failed: t.n");
    }

    #[test]
    fn append_in_a_transaction_that_is_rolled_back_writes_nothing() {
        let mut store = rannoch_core::forgr::empty_store();
        let tx = store.transaction().unwrap();

        super::append_in(&tx, &[event(Uuid::now_v7()), event(Uuid::now_v7())]).unwrap();
        assert_eq!(count(&tx), 2);
        tx.rollback().unwrap();

        assert_eq!(count(&store), 0);
    }

    #[test]
    fn run_at_is_rfc3339_utc_at_millis() {
        let mut store = rannoch_core::forgr::empty_store();
        let (queue, _signals) = mpsc::channel();
        let at = chrono::DateTime::from_timestamp_micros(1_763_100_000_123_456).unwrap();
        let batch = [Event {
            run_at: Some(at),
            ..event(Uuid::now_v7())
        }];

        append(&mut store, &batch, &queue).unwrap();

        let stored: String = store
            .query_row("SELECT run_at FROM _events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stored, "2025-11-14T06:00:00.123Z");
    }
}
