//! `@criterion-FR-RANNOCH-013-c2` and `-c3`, `@criterion-FR-RANNOCH-014-c1`
//! and `-c2`, and `@criterion-FR-RANNOCH-015-c1` to `-c3`, from
//! `tests/features/`. Each store is the app's forgr empty store.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use chrono::{DateTime, SecondsFormat, Utc};
use rannoch_app::forgr::_events;
use rannoch_core::{Error, Event, Status};
use rannoch_events::append;
use rusqlite::Connection;
use serde_json::{Value, json};
use uuid::Uuid;

/// The app's plain fieldless enum.
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

/// An event as the inbox accepts it: a `UUIDv7` id and status 0.
fn accepted(source: Option<Uuid>) -> Event<Ty, Value> {
    Event {
        id: Uuid::now_v7(),
        source,
        ty: Ty::Page,
        data: json!({"page": 1}),
        run_at: None,
        status: Status::Added,
    }
}

/// One `_events` row: id, source, ty, data, `run_at` and status.
type Row = (String, Option<String>, i64, String, Option<String>, i64);

fn row(conn: &Connection, id: &str) -> rusqlite::Result<Row> {
    conn.query_row(
        "SELECT id, source, ty, data, run_at, status FROM _events WHERE id = ?1",
        [id],
        |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        },
    )
}

fn ids(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    conn.prepare("SELECT id FROM _events ORDER BY id")?
        .query_map([], |r| r.get(0))?
        .collect()
}

/// The app's empty store, written to a file of its own under `name` so a
/// second connection can read it.
fn file_store(name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let path = dir.join("store.db");
    rannoch_app::forgr::empty_store().execute("VACUUM INTO ?1", [path.to_string_lossy()])?;
    Ok(path)
}

#[test]
fn fr_rannoch_013_c2() {
    // Given an accepted event
    let mut store = rannoch_app::forgr::empty_store();
    let (queue, _signals) = mpsc::channel();
    let run_at: DateTime<Utc> = "2026-11-14T08:00:00.250Z".parse().unwrap();
    let event = Event {
        run_at: Some(run_at),
        ..accepted(Some(Uuid::now_v7()))
    };

    // When it is written to _events
    append(&mut store, std::slice::from_ref(&event), &queue).unwrap();

    // Then its row holds its id, source, ty, data, run_at and status
    let id = event.id.to_string();
    assert_eq!(
        row(&store, &id).unwrap(),
        (
            id.clone(),
            event.source.map(|s| s.to_string()),
            7,
            r#"{"page":1}"#.to_owned(),
            Some(run_at.to_rfc3339_opts(SecondsFormat::Millis, true)),
            0,
        )
    );
}

#[test]
fn fr_rannoch_013_c3() {
    let store = rannoch_app::forgr::empty_store();

    // When a row with status 3 is written to _events
    let refused = _events::factory()
        .id(Uuid::now_v7().to_string())
        .ty(7)
        .data("{}")
        .status(3)
        .insert(&store)
        .unwrap_err();

    // Then the write is refused
    assert!(
        refused.to_string().contains("CHECK constraint failed"),
        "{refused}"
    );
    assert_eq!(ids(&store).unwrap(), Vec::<String>::new());
}

#[test]
fn fr_rannoch_014_c1() {
    // Given a root event in _events with status 0
    let mut store = rannoch_app::forgr::empty_store();
    let (queue, _signals) = mpsc::channel();
    let root = _events::factory()
        .id(Uuid::now_v7().to_string())
        .ty(7)
        .data("{}")
        .status(0)
        .insert(&store)
        .unwrap();
    let root_id: Uuid = root.id.parse().unwrap();

    // When its status moves to 2
    let update = Event {
        status: Status::Processed,
        ..accepted(Some(root_id))
    };
    append(&mut store, std::slice::from_ref(&update), &queue).unwrap();

    // Then _events holds a new row with a new id, status 2 and the root's id
    // as its source
    let new = row(&store, &update.id.to_string()).unwrap();
    assert_ne!(new.0, root.id);
    assert_eq!(new.1.as_deref(), Some(root.id.as_str()));
    assert_eq!(new.5, 2);

    // And the root's row still has status 0
    assert_eq!(row(&store, &root.id).unwrap().5, 0);
    assert_eq!(ids(&store).unwrap().len(), 2);
}

#[test]
fn fr_rannoch_014_c2() {
    // Given a root event with two status rows
    let mut store = rannoch_app::forgr::empty_store();
    let (queue, _signals) = mpsc::channel();
    let root = accepted(None);
    append(&mut store, std::slice::from_ref(&root), &queue).unwrap();
    let updates = [
        Event {
            status: Status::InProgress,
            ..accepted(Some(root.id))
        },
        Event {
            status: Status::Processed,
            ..accepted(Some(root.id))
        },
    ];
    append(&mut store, &updates, &queue).unwrap();

    // When each status row's source is followed
    let followed: Vec<Row> = store
        .prepare("SELECT source FROM _events WHERE status <> 0")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|source| row(&store, &source.unwrap()).unwrap())
        .collect();

    // Then it leads to the root event
    assert_eq!(followed.len(), 2);
    for reached in followed {
        assert_eq!(reached.0, root.id.to_string());
        assert_eq!(reached.1, None);
        assert_eq!(reached.5, 0);
    }
}

#[test]
fn fr_rannoch_015_c1() {
    // Given a batch of three events
    let mut store = rannoch_app::forgr::empty_store();
    let (queue, _signals) = mpsc::channel();
    let batch = [accepted(None), accepted(None), accepted(None)];

    // When the events crate appends it
    append(&mut store, &batch, &queue).unwrap();

    // Then _events holds all three
    let mut expected: Vec<String> = batch.iter().map(|e| e.id.to_string()).collect();
    expected.sort();
    assert_eq!(ids(&store).unwrap(), expected);
}

#[test]
fn fr_rannoch_015_c2() {
    // Given a batch of events
    let path = file_store("fr_rannoch_015_c2").unwrap();
    let mut store = Connection::open(&path).unwrap();
    let batch = [accepted(None), accepted(None), accepted(None)];
    let (queue, signals) = mpsc::channel();
    let reader = Connection::open(&path).unwrap();
    let listener = thread::spawn(move || signals.recv().map(|()| ids(&reader)));

    // When the events crate appends it
    append(&mut store, &batch, &queue).unwrap();

    // Then the queue is signalled
    let seen = listener
        .join()
        .unwrap()
        .expect("the queue was not signalled")
        .unwrap();

    // And every event in the batch is in _events when the signal arrives,
    // read through a connection of the queue's own
    let mut expected: Vec<String> = batch.iter().map(|e| e.id.to_string()).collect();
    expected.sort();
    assert_eq!(seen, expected);
}

#[test]
fn fr_rannoch_015_c3() {
    // Given the events crate cannot write
    let mut store = rannoch_app::forgr::empty_store();
    store.pragma_update(None, "query_only", true).unwrap();
    let (queue, signals) = mpsc::channel();

    // When it appends a batch
    let appended = append(&mut store, &[accepted(None), accepted(None)], &queue);

    // Then the error is Store
    assert_eq!(appended, Err(Error::Store));

    // And the queue is not signalled
    assert_eq!(signals.try_recv(), Err(mpsc::TryRecvError::Empty));
    assert_eq!(ids(&store).unwrap(), Vec::<String>::new());
}
