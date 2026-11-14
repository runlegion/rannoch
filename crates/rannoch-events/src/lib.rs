//! The events crate: it owns `_events`, appends each batch to it in one
//! write, and signals the queue once that write has committed.

mod queries;

use std::sync::mpsc::Sender;
use std::sync::{Mutex, PoisonError};

use chrono::{DateTime, SecondsFormat, Utc};
use rannoch_core::{Append, Error, Event, Status};
use rusqlite::{Connection, Params, params};
use serde::Serialize;
use uuid::Uuid;

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

/// The status function: for the root event `root`, its current status and
/// the root with every `_events` row whose source chain leads back to it,
/// ordered by id, each as stored: ty as its `u16`, data as its JSON text.
/// The current status is that of the root's newest row whose
/// source is the root and whose status is not 0, or the root's own.
///
/// # Errors
///
/// `Error::Store` when the read fails or `root` is not in `_events`.
pub fn status(conn: &Connection, root: Uuid) -> Result<(Status, Vec<Event<u16, String>>), Error> {
    let current = conn
        .query_row(queries::CURRENT_STATUS, [root.to_string()], |r| {
            r.get::<_, i64>(0)
        })
        .map_err(|_| Error::Store)
        .and_then(status_of)?;
    Ok((current, read(conn, queries::TREE, [root.to_string()])?))
}

/// The events of `ty` that are waiting: their current status is 0, so
/// nothing has changed their state. Ordered by id.
///
/// # Errors
///
/// `Error::Store` when the read fails.
pub fn waiting<T: Copy + Into<u16>>(
    conn: &Connection,
    ty: T,
) -> Result<Vec<Event<u16, String>>, Error> {
    read(conn, queries::WAITING, [ty.into()])
}

/// The waiting events whose `run_at` is at or before `at`. Ordered by id.
///
/// # Errors
///
/// `Error::Store` when the read fails.
pub fn due(conn: &Connection, at: DateTime<Utc>) -> Result<Vec<Event<u16, String>>, Error> {
    read(
        conn,
        queries::DUE,
        [at.to_rfc3339_opts(SecondsFormat::Millis, true)],
    )
}

/// The rows `sql` selects, as id, source, ty, data, `run_at` and status.
fn read(
    conn: &Connection,
    sql: &str,
    params: impl Params,
) -> Result<Vec<Event<u16, String>>, Error> {
    let mut select = conn.prepare(sql).map_err(|_| Error::Store)?;
    let rows = select
        .query_map(params, |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, u16>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .map_err(|_| Error::Store)?;
    rows.map(|row| {
        let (id, source, ty, data, run_at, status) = row.map_err(|_| Error::Store)?;
        Ok(Event {
            id: id.parse().map_err(|_| Error::Store)?,
            source: source
                .map(|s| s.parse())
                .transpose()
                .map_err(|_| Error::Store)?,
            ty,
            data,
            run_at: run_at
                .map(|t| t.parse())
                .transpose()
                .map_err(|_| Error::Store)?,
            status: status_of(status)?,
        })
    })
    .collect()
}

/// The `Status` a stored status column holds.
fn status_of(stored: i64) -> Result<Status, Error> {
    match stored {
        -1 => Ok(Status::Binned),
        0 => Ok(Status::Added),
        1 => Ok(Status::InProgress),
        2 => Ok(Status::Processed),
        _ => Err(Error::Store),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use rannoch_core::{Error, Event, Status};
    use uuid::Uuid;

    use super::{Events, append, due, status, waiting};

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

    /// A row written through the app's forgr factory: ty 7, its status and
    /// source as given.
    fn row(conn: &rusqlite::Connection, source: Option<Uuid>, status: i64) -> Uuid {
        let id = Uuid::now_v7();
        let factory = rannoch_app::forgr::_events::factory()
            .id(id.to_string())
            .ty(7)
            .data("{}")
            .status(status);
        match source {
            Some(source) => factory.source(source.to_string()),
            None => factory,
        }
        .insert(conn)
        .unwrap();
        id
    }

    #[test]
    fn the_tree_follows_the_source_chain_past_the_first_level() {
        let store = rannoch_app::forgr::empty_store();
        let root = row(&store, None, 0);
        let produced = row(&store, Some(root), 0);
        let grandchild = row(&store, Some(produced), 0);
        row(&store, None, 0);

        let (_, tree) = status(&store, root).unwrap();

        let ids: Vec<Uuid> = tree.iter().map(|e| e.id).collect();
        assert_eq!(ids, [root, produced, grandchild]);
    }

    #[test]
    fn a_root_with_no_status_rows_has_its_own_status() {
        let store = rannoch_app::forgr::empty_store();
        let root = row(&store, None, 0);
        row(&store, Some(root), 0);

        assert_eq!(status(&store, root).unwrap().0, Status::Added);
    }

    #[test]
    fn a_produced_events_state_change_leaves_the_root_unchanged() {
        let store = rannoch_app::forgr::empty_store();
        let root = row(&store, None, 0);
        let produced = row(&store, Some(root), 0);
        row(&store, Some(produced), 2);

        let (current, tree) = status(&store, root).unwrap();

        assert_eq!(current, Status::Added);
        assert_eq!(tree.len(), 3);
    }

    #[test]
    fn the_status_of_an_unknown_id_is_a_store_error() {
        let store = rannoch_app::forgr::empty_store();

        assert_eq!(status(&store, Uuid::now_v7()).unwrap_err(), Error::Store);
    }

    #[test]
    fn rows_come_back_as_stored() {
        let mut store = rannoch_app::forgr::empty_store();
        let (queue, _signals) = mpsc::channel();
        let at = chrono::DateTime::from_timestamp_millis(1_763_100_000_123).unwrap();
        let root = Uuid::now_v7();
        let event = Event {
            source: Some(root),
            run_at: Some(at),
            ..event(Uuid::now_v7())
        };
        append(&mut store, std::slice::from_ref(&event), &queue).unwrap();

        let read = waiting(&store, Ty::Page).unwrap();

        assert_eq!(read.len(), 1);
        assert_eq!(read[0].id, event.id);
        assert_eq!(read[0].source, Some(root));
        assert_eq!(read[0].ty, 7);
        assert_eq!(read[0].data, r#""data""#);
        assert_eq!(read[0].run_at, Some(at));
        assert_eq!(read[0].status, Status::Added);
    }

    #[test]
    fn a_binned_or_in_progress_event_is_not_waiting() {
        let store = rannoch_app::forgr::empty_store();
        let binned = row(&store, None, 0);
        row(&store, Some(binned), -1);
        let started = row(&store, None, 0);
        row(&store, Some(started), 1);

        assert!(waiting(&store, Ty::Page).unwrap().is_empty());
    }

    #[test]
    fn due_is_at_or_before_the_time_and_skips_no_run_at() {
        let store = rannoch_app::forgr::empty_store();
        let at: chrono::DateTime<chrono::Utc> = "2026-11-14T08:00:00.000Z".parse().unwrap();
        let on_time = Uuid::now_v7();
        rannoch_app::forgr::_events::factory()
            .id(on_time.to_string())
            .ty(7)
            .data("{}")
            .run_at("2026-11-14T08:00:00.000Z")
            .status(0)
            .insert(&store)
            .unwrap();
        row(&store, None, 0);

        let ids: Vec<Uuid> = due(&store, at).unwrap().iter().map(|e| e.id).collect();

        assert_eq!(ids, [on_time]);
    }
}
