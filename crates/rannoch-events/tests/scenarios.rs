//! The events crate's one behaviour, `tests/features/a_batch_handed_to_the_events_crate.feature`:
//! `@criterion-FR-RANNOCH-013-c2`, `@criterion-FR-RANNOCH-014-c1` and `-c2`, and
//! `@criterion-FR-RANNOCH-015-c1` to `-c3`. Events come from the app's forgr factory; each store
//! is the app's forgr empty store.
//!
//! And `tests/features/clock.feature`, one scenario for each of #162's criteria: rannoch-events
//! reads and waits on time only through rannoch-core's clock and tokio, and every test runs with
//! the clock frozen.

use std::fs;
use std::path::{Path, PathBuf};

use rannoch_core::clock::new_id;
use rannoch_core::forgr::_events;
use rannoch_core::{Event, Status};
use rannoch_events::append;
use rusqlite::Connection;
use serde_json::Value;
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

/// An accepted event made by the app's forgr factory: the factory writes the row to a scratch
/// store and the event is that row, as the inbox would hand it over.
fn accepted(
    scratch: &Connection,
    source: Option<Uuid>,
    status: Status,
) -> Result<Event<Ty, Value>, Box<dyn std::error::Error>> {
    let mut factory = _events::factory()
        .id(new_id().to_string())
        .ty(7)
        .data(r#"{"page":1}"#)
        .status(status as i64);
    if let Some(source) = source {
        factory = factory.source(source.to_string());
    }
    let row = factory.insert(scratch)?;
    Ok(Event {
        id: row.id.parse()?,
        source,
        ty: Ty::Page,
        data: serde_json::from_str(&row.data)?,
        run_at: None,
        status,
    })
}

/// One `_events` row: id, source, ty, data, `run_at` and status.
type Row = (String, Option<String>, i64, String, Option<String>, i64);

fn rows(conn: &Connection) -> rusqlite::Result<Vec<Row>> {
    conn.prepare("SELECT id, source, ty, data, run_at, status FROM _events ORDER BY id")?
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })?
        .collect()
}

/// The app's empty store, written to a file of its own under `name` so the queue can read it
/// through a connection of its own.
fn file_store(name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let path = dir.join("store.db");
    rannoch_core::forgr::empty_store().execute("VACUUM INTO ?1", [path.to_string_lossy()])?;
    Ok(path)
}

#[tokio::test(start_paused = true)]
async fn a_batch_handed_to_the_events_crate_is_kept_and_the_queue_is_told() {
    // Given a batch of accepted events from the app's forgr factory, one of them a status
    // update for another
    let scratch = rannoch_core::forgr::empty_store();
    let event = accepted(&scratch, None, Status::Added).unwrap();
    let other = accepted(&scratch, None, Status::Added).unwrap();
    let update = accepted(&scratch, Some(event.id), Status::Processed).unwrap();
    let batch = [event.clone(), other.clone(), update.clone()];

    // When the batch is handed to the events crate over forgr's empty store
    let path = file_store("a_batch_handed_to_the_events_crate").unwrap();
    let mut store = Connection::open(&path).unwrap();
    let (queue, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let reader = Connection::open(&path).unwrap();
    let queue_side =
        tokio::task::spawn_blocking(move || signals.blocking_recv().map(|()| rows(&reader)));
    append(&mut store, &batch, &queue).unwrap();

    // Then each event is in _events as one row with all its fields, in one commit
    // And the queue is signalled after the commit, and not before: the queue's own connection
    // already sees every row when the signal arrives
    let seen = queue_side
        .await
        .unwrap()
        .expect("the queue was not signalled")
        .unwrap();
    let mut expected: Vec<Row> = batch
        .iter()
        .map(|e| {
            (
                e.id.to_string(),
                e.source.map(|s| s.to_string()),
                7,
                r#"{"page":1}"#.to_owned(),
                None,
                e.status as i64,
            )
        })
        .collect();
    expected.sort();
    assert_eq!(seen, expected);

    // And the status update is a new row whose source is its event, and that event's own row
    // is unchanged
    let by_id = |id: Uuid| seen.iter().find(|r| r.0 == id.to_string()).unwrap();
    assert_ne!(update.id, event.id);
    assert_eq!(
        by_id(update.id).1.as_deref(),
        Some(event.id.to_string().as_str())
    );
    assert_eq!(by_id(update.id).5, 2);
    assert_eq!(by_id(event.id).5, 0);

    // When a batch is handed to the events crate over a store that cannot be written
    let mut locked = rannoch_core::forgr::empty_store();
    locked.pragma_update(None, "query_only", true).unwrap();
    let (queue, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let more = [accepted(&scratch, None, Status::Added).unwrap(), other];
    let _ = append(&mut locked, &more, &queue);

    // Then nothing is kept and the queue is not signalled
    assert_eq!(rows(&locked).unwrap(), Vec::<Row>::new());
    assert_eq!(
        signals.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    );
}

/// Every `.rs` file of rannoch-events's code and tests, with its path.
fn sources() -> std::io::Result<Vec<(PathBuf, String)>> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in ["src", "tests"] {
        for entry in fs::read_dir(crate_dir.join(dir))? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "rs") {
                let text = fs::read_to_string(&path)?;
                files.push((path, text));
            }
        }
    }
    Ok(files)
}

#[tokio::test(start_paused = true)]
async fn rannoch_events_calls_no_real_time_function() {
    // Given rannoch-events's code and tests
    let files = sources().unwrap();
    assert!(files.len() >= 3, "{files:?}");

    // When they are read for a call to any real-time function; the opening paren is added here,
    // so this list is not itself a call
    let names = [
        "thread::sleep",
        "thread::spawn",
        "recv_timeout",
        "Instant::now",
        "SystemTime::now",
        "Utc::now",
        "now_v7",
    ];
    let calls: Vec<String> = names.iter().map(|name| format!("{name}(")).collect();

    // Then none is found
    for (path, text) in &files {
        for call in &calls {
            assert!(
                !text.contains(call.as_str()),
                "{} calls {call}",
                path.display()
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn every_rannoch_events_test_runs_with_the_clock_frozen() {
    // Given rannoch-events's tests, the doc example among them
    let files = sources().unwrap();

    // When they are read for how each one runs
    let mut runs = 0;
    for (path, text) in &files {
        for line in text.lines().map(str::trim) {
            let line = line.trim_start_matches("//!").trim();
            // Then none runs as a plain test, and each tokio test or main freezes the clock
            assert_ne!(line, "#[test]", "{} has a plain test", path.display());
            let tokio_run = line.starts_with("#[tokio::test") || line.starts_with("#[tokio::main");
            if tokio_run {
                assert!(
                    line.ends_with("start_paused = true)]"),
                    "{}: {line}",
                    path.display()
                );
                runs += 1;
            }
        }
    }
    // 10 in the library, its doc example's main, and 3 here
    assert_eq!(runs, 14);
}
