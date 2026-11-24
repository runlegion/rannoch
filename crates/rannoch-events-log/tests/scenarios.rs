//! #129's scenario. The store is the app's database through the real store; events are sent
//! through rannoch, run by the real queue and rannoch-processor on the built-in `emit`, and
//! written by rannoch-events' append, which the app wraps in the log. This binary holds one
//! test, so the one store is this test's.

use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use rannoch_core::{Append, Config, Event, Rannoch, Status};
use rannoch_events::Events;
use rannoch_events_log::Log;
use rannoch_processor::{FlowDoc, Processor, check};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Value, json};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const FLOWS: &str = r#"[
  {"name": "page", "ty": 10, "takes": {"n": "number"},
   "steps": [{"do": "emit", "next": {"ty": 11, "shape": {"n": "$n"}}}]},
  {"name": "seen", "ty": 11, "takes": {"n": "number"}, "steps": []}
]"#;

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// Waits up to five seconds for `done`.
fn until(done: impl Fn() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    done()
}

/// A fresh store for the app, with every migration applied by opening rannoch once.
fn migrated(dir: &Path) -> Result<PathBuf> {
    drop(fs::remove_dir_all(dir));
    fs::create_dir_all(dir)?;
    let path = dir.join("store.db");
    let store = Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator())) as _);
    Rannoch::<u16, Value>::open(Config {
        store,
        ..Config::default()
    })?
    .shutdown();
    Ok(path)
}

/// Every row in `_events`, each as an object of its columns as the table holds them, by id.
fn rows(path: &Path) -> Result<Vec<Value>> {
    let conn = ready(path)?;
    let sql = "SELECT id, source, ty, data, run_at, status FROM _events ORDER BY id";
    let mut select = conn.prepare(sql)?;
    let row = |r: &rusqlite::Row| {
        Ok(json!({
            "id": r.get::<_, String>(0)?,
            "source": r.get::<_, Option<String>>(1)?,
            "ty": r.get::<_, i64>(2)?,
            "data": r.get::<_, String>(3)?,
            "run_at": r.get::<_, Option<String>>(4)?,
            "status": r.get::<_, i64>(5)?,
        }))
    };
    Ok(select
        .query_map([], row)?
        .collect::<rusqlite::Result<_>>()?)
}

#[test]
fn every_event_written_to_events_is_one_json_line_in_the_log_as_events_holds_it() {
    // Given an app whose flow runs each page on the built-in emit and sends a next
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("events-log");
    let path = migrated(&dir).unwrap();
    let docs: Vec<FlowDoc> = serde_json::from_str(FLOWS).unwrap();
    let docs: Vec<(String, FlowDoc)> = docs.into_iter().map(|d| (d.name.clone(), d)).collect();
    let flows = check(&docs, &[]).unwrap();
    let processor: Processor<u16, Value> =
        Processor::new(&flows, vec![], ready(&path).unwrap()).unwrap();

    // And rannoch running over the store, its append wrapped in the log with the path the
    // app's settings give
    let log = dir.join("events.log");
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    let (signal, wake) = mpsc::channel();
    let events = Events::new(ready(&path).unwrap(), signal.clone());
    let append = Arc::new(Log::new(events, log.clone()));
    let rannoch = Rannoch::open(Config {
        store: Some(store),
        append: Some(append as Arc<dyn Append<u16, Value>>),
        ..Config::default()
    })
    .unwrap();
    let settings = Settings {
        attempts: 1,
        ..Settings::new(Duration::from_millis(100))
    };
    let (conn, sender) = (ready(&path).unwrap(), rannoch.sender());
    let queue = Queue::start(conn, wake, sender, Arc::new(processor), settings);

    // When the app sends three pages and each runs, with its next
    for n in 0..3 {
        let page = Event {
            id: Uuid::nil(),
            source: None,
            ty: 10,
            data: json!({ "n": n }),
            run_at: None,
            status: Status::Added,
        };
        rannoch.sender().send(page).unwrap();
    }
    let twos = || {
        let rows = rows(&path).unwrap_or_default();
        rows.iter().filter(|r| r["status"] == 2).count()
    };
    assert!(until(|| twos() == 6));
    drop(signal);
    rannoch.shutdown();
    assert_eq!(queue.join(), Ok(()));

    // Then every row in _events is one JSON line in the log, holding the row as _events does
    let written = rows(&path).unwrap();
    assert_eq!(written.len(), 18);
    let text = fs::read_to_string(&log).unwrap();
    let mut lines: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    lines.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    assert_eq!(lines, written);
}
