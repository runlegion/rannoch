//! #128's scenario. The store is the app's database through the real store, its rows from the
//! app's forgr factory, handed over by the real queue to rannoch-processor running the trim
//! step, which the app adds with no arguments. This binary holds one test, so the one store is
//! this test's.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs, thread};

use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_processor::{FlowDoc, Processor, Sig, check};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::Value;
use uuid::{Builder, Uuid};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const FLOWS: &str = r#"[
  {"name": "trim", "ty": 10, "takes": {},
   "steps": [{"do": "trim", "with": {"trim": {"older_than_ms": 60000}}}]}
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
fn migrated() -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("events-trim");
    drop(fs::remove_dir_all(&dir));
    fs::create_dir_all(&dir)?;
    let path = dir.join("store.db");
    let store = Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator())) as _);
    Rannoch::<u16, Value>::open(Config {
        store,
        ..Config::default()
    })?
    .shutdown();
    Ok(path)
}

/// A finished event made `ago_ms` before now, from the app's forgr factory: its 0 of ty 11 and
/// that 0's 1 and 2, so the queue never takes it. Returns the three ids.
fn finished(conn: &Connection, ago_ms: u64, n: u8) -> Result<[Uuid; 3]> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let at = u64::try_from(now)? - ago_ms;
    let id = |s| Builder::from_unix_timestamp_millis(at, &[n, s, 0, 0, 0, 0, 0, 0, 0, 0]);
    let ids = [0, 1, 2].map(|s| id(s).into_uuid());
    for (status, id) in ids.iter().enumerate() {
        let row = _events::factory().id(id.to_string()).ty(11).data("{}");
        let row = row.status(i64::try_from(status)?);
        match status {
            0 => row,
            _ => row.source(ids[0].to_string()),
        }
        .insert(conn)?;
    }
    Ok(ids)
}

/// Every id in `_events`.
fn ids(path: &Path) -> Result<BTreeSet<Uuid>> {
    let conn = ready(path)?;
    let mut select = conn.prepare("SELECT id FROM _events")?;
    let ids = select.query_map([], |r| r.get::<_, String>(0))?;
    let ids: Vec<String> = ids.collect::<rusqlite::Result<_>>()?;
    Ok(ids
        .iter()
        .map(|i| i.parse())
        .collect::<std::result::Result<_, _>>()?)
}

#[test]
fn a_flow_whose_step_is_trim_deletes_the_rows_older_than_its_options_say_and_keeps_the_rest() {
    // Given a store holding two events an hour old and one made now, each with its 1 and 2
    let path = migrated().unwrap();
    let seed = ready(&path).unwrap();
    let old: BTreeSet<Uuid> = [1, 2]
        .into_iter()
        .flat_map(|n| finished(&seed, 3_600_000, n).unwrap())
        .collect();
    let fresh: BTreeSet<Uuid> = finished(&seed, 0, 3).unwrap().into();
    assert_eq!(ids(&path).unwrap(), &old | &fresh);

    // And an app whose flow's step is trim, older than a minute, the step added to its
    // signatures and its actions with no arguments
    let sigs: Vec<Sig> = serde_json::from_str(rannoch_events_trim::SIGNATURES).unwrap();
    let docs: Vec<FlowDoc> = serde_json::from_str(FLOWS).unwrap();
    let docs: Vec<(String, FlowDoc)> = docs.into_iter().map(|d| (d.name.clone(), d)).collect();
    let flows = check(&docs, &sigs).unwrap();
    let actions = rannoch_events_trim::actions();
    let processor: Processor<u16, Value> =
        Processor::new(&flows, actions, ready(&path).unwrap()).unwrap();

    // And rannoch running over the store
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    let (signal, wake) = mpsc::channel();
    let append = Arc::new(Events::new(ready(&path).unwrap(), signal.clone()));
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

    // When the queue hands the flow an event
    let trim = Uuid::now_v7();
    let row = _events::factory().id(trim.to_string()).ty(10).data("{}");
    row.status(0).insert(&seed).unwrap();
    signal.send(()).unwrap();

    // Then the event has its 2, and _events holds no row an hour old and every other row
    let about = |status: i64| {
        let sql = "SELECT id FROM _events WHERE source = ?1 AND status = ?2";
        let id = seed.query_row(sql, (trim.to_string(), status), |r| r.get::<_, String>(0));
        id.ok().and_then(|i| i.parse::<Uuid>().ok())
    };
    assert!(until(|| about(2).is_some()));
    let ran = [Some(trim), about(1), about(2)].into_iter().flatten();
    let kept: BTreeSet<Uuid> = fresh.iter().copied().chain(ran).collect();
    assert_eq!(kept.len(), 6);
    let left = ids(&path).unwrap();
    assert_eq!(left, kept, "old: {old:?}");

    drop(signal);
    rannoch.shutdown();
    assert_eq!(queue.join(), Ok(()));
}
