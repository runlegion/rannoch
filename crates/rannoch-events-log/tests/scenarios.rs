//! #129's scenario. The store is the app's database through the real store; events are sent
//! through rannoch, run by the real queue and rannoch-processor on the built-in `emit`, and
//! written by rannoch-events' append, which the app wraps in the log; a row of a `ty` the app
//! no longer has comes from the app's forgr factory, and the queue writes its -1; then
//! rannoch-events-trim deletes every row. This binary holds one test, so the one store is this
//! test's.

use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Event, Rannoch, Status};
use rannoch_events::Events;
use rannoch_events_log::Log;
use rannoch_processor::{FlowDoc, Processor, check};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The app's event types: a page, and its next.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Ty {
    Page = 10,
    Seen = 11,
}

impl From<Ty> for u16 {
    fn from(ty: Ty) -> u16 {
        ty as u16
    }
}

impl TryFrom<u16> for Ty {
    type Error = u16;
    fn try_from(ty: u16) -> std::result::Result<Self, u16> {
        match ty {
            10 => Ok(Self::Page),
            11 => Ok(Self::Seen),
            other => Err(other),
        }
    }
}

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

/// Every row in `_events`, each as an object of its columns as the table holds them, in the
/// order written.
fn rows(path: &Path) -> Result<Vec<Value>> {
    let conn = ready(path)?;
    let sql = "SELECT id, source, ty, data, run_at, status FROM _events ORDER BY rowid";
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
    let processor: Processor<Ty, Value> =
        Processor::new(&flows, vec![], ready(&path).unwrap()).unwrap();

    // And rannoch running over the store, its append wrapped in the log with the path the
    // app's settings give
    let log = dir.join("events.log");
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    let (signal, wake) = mpsc::channel();
    let events = Events::new(ready(&path).unwrap(), signal.clone());
    let append = Arc::new(Log::new(events, log.clone()).unwrap());
    let rannoch = Rannoch::open(Config {
        store: Some(store),
        append: Some(append as Arc<dyn Append<Ty, Value>>),
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
    let page = |n: u8| Event {
        id: Uuid::nil(),
        source: None,
        ty: Ty::Page,
        data: json!({ "n": n }),
        run_at: None,
        status: Status::Added,
    };
    for n in 0..3 {
        rannoch.sender().send(page(n)).unwrap();
    }
    let count = |status: i64| {
        let rows = rows(&path).unwrap_or_default();
        rows.iter().filter(|r| r["status"] == status).count()
    };
    assert!(until(|| count(2) == 6));

    // And a row of a ty the app no longer has reaches _events, and the queue writes its -1
    let gone = _events::factory().id(Uuid::now_v7().to_string()).ty(12);
    gone.data("{}")
        .status(0)
        .insert(&ready(&path).unwrap())
        .unwrap();
    signal.send(()).unwrap();
    assert!(until(|| count(-1) == 1));

    // And the app sends a fourth page
    rannoch.sender().send(page(3)).unwrap();
    assert!(until(|| count(2) == 8));
    let written = rows(&path).unwrap();
    assert_eq!(written.len(), 26);

    // And trim deletes every row, the newest included, and the app sends a fifth page
    thread::sleep(Duration::from_millis(5));
    let trim = &rannoch_events_trim::actions()[0];
    trim.run(&json!({ "older_than_ms": 0 }), &Map::new())
        .unwrap();
    assert_eq!(rows(&path).unwrap(), Vec::<Value>::new());
    rannoch.sender().send(page(4)).unwrap();
    assert!(until(|| count(2) == 2));
    drop(signal);
    rannoch.shutdown();
    assert_eq!(queue.join(), Ok(()));

    // Then every row written to _events is one JSON line in the log, holding the row as _events
    // does, in the order written, the rows after the trim included
    let after = rows(&path).unwrap();
    assert_eq!(after.len(), 6);
    let lines = || {
        let text = fs::read_to_string(&log).unwrap();
        let lines = text.lines().map(|l| serde_json::from_str(l).unwrap());
        lines.collect::<Vec<Value>>()
    };
    assert_eq!(lines(), [written.clone(), after].concat());

    // When a row reaches _events after the log's last read, and the app starts again and
    // appends a page
    let late = _events::factory().id(Uuid::now_v7().to_string()).ty(12);
    late.data("{}")
        .status(0)
        .insert(&ready(&path).unwrap())
        .unwrap();
    let events = Events::new(ready(&path).unwrap(), mpsc::channel().0);
    let restarted = Log::new(events, log.clone()).unwrap();
    let again = Event {
        id: Uuid::now_v7(),
        ..page(5)
    };
    restarted.append(&[again]).unwrap();

    // Then the log holds that row and the page after the lines before the restart
    let all = rows(&path).unwrap();
    assert_eq!(all.len(), 8);
    assert_eq!(lines(), [written, all].concat());
}
