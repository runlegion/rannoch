//! The events crate: it owns `_events`, appends each batch to it in one
//! write, and signals the queue once that write has committed.

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
    {
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
    }
    tx.commit().map_err(|_| Error::Store)?;
    // The events are kept whether or not the queue is still listening.
    let _ = queue.send(());
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
        let mut store = rannoch_app::forgr::empty_store();
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
        let events = Events::new(rannoch_app::forgr::empty_store(), queue);
        let batch = [event(Uuid::now_v7()), event(Uuid::now_v7())];

        rannoch_core::Append::append(&events, &batch).unwrap();

        assert_eq!(count(&events.conn.lock().unwrap()), 2);
        assert_eq!(signals.try_recv(), Ok(()));
    }

    #[test]
    fn run_at_is_rfc3339_utc_at_millis() {
        let mut store = rannoch_app::forgr::empty_store();
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
