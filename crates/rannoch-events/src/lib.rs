#![warn(missing_docs)]
//! The events crate: it owns `_events`, appends each batch to it in one
//! write, and signals the queue once that write has committed.
//!
//! [`Events`] is the append an app puts in core's `Config`: each inbox hands it its batches.
//! Each event becomes one `_events` row with its fields as given: `id` and `source` as text,
//! `ty` as its `u16`, `data` as its JSON from serde, `run_at` as RFC 3339 in UTC to the
//! millisecond, and `status` as its int. This crate only inserts rows; it updates and deletes
//! none.
//!
//! The append signals once for each batch it commits, whether or not the queue still listens.
//! The receiver is the queue's: rannoch-queue's `Queue` stops once every sender on that channel
//! has dropped, and the sender in an [`Events`] drops when that `Events` does.
//!
//! [`append_in`] writes a batch inside the caller's transaction, for an append that writes rows
//! of its own in the same commit, as rannoch-graph's `GraphAppend` does.
//!
//! # Failures
//!
//! A batch is written whole or not at all. When an event's `data` does not serialize, or a row
//! cannot be inserted, as with an id already in `_events`, the append fails with
//! `Error::Store`, writes nothing, and sends no signal.
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
//!     assert_eq!(failed, Err(rannoch_core::Error::Store));
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

use std::sync::mpsc::Sender;
use std::sync::{Mutex, PoisonError};

use chrono::SecondsFormat;
use rannoch_core::{Append, Error, Event};
use rusqlite::{Connection, params};
use serde::Serialize;
/// Where this crate's migratr migrations directory is, for the app's `build.rs`.
pub const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// The events crate's append for the app's `Config`: the connection it
/// writes `_events` through and the queue it signals.
#[derive(Debug)]
pub struct Events {
    conn: Mutex<Connection>,
    queue: Sender<()>,
}

impl Events {
    /// The append over `conn`, signalling `queue` after each commit.
    #[must_use]
    pub fn new(conn: Connection, queue: Sender<()>) -> Self {
        Self {
            conn: Mutex::new(conn),
            queue,
        }
    }
}

impl<T: Copy + Into<u16>, D: Serialize> Append<T, D> for Events {
    fn append(&self, batch: &[Event<T, D>]) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        append(&mut conn, batch, &self.queue)
    }
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

/// Appends `batch` to `_events` inside `tx`; the caller commits and signals the queue.
///
/// # Errors
///
/// `Error::Store` when an insert fails; the caller then commits nothing.
pub fn append_in<T: Copy + Into<u16>, D: Serialize>(
    tx: &rusqlite::Transaction<'_>,
    batch: &[Event<T, D>],
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
    use std::sync::mpsc;

    use rannoch_core::{Error, Event, Status};
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

    fn event(id: Uuid) -> Event<Ty, &'static str> {
        Event {
            id,
            source: None,
            ty: Ty::Page,
            data: "data",
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

        rannoch_core::Append::append(&events, &batch).unwrap();

        assert_eq!(count(&events.conn.lock().unwrap()), 2);
        assert_eq!(signals.try_recv(), Ok(()));
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
