//! #128's scenario, with #142's events that have no 2. The store is the app's database through the real store, its rows from the
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

const HOUR: u64 = 3_600_000;

/// An event of ty 11 from the app's forgr factory: its 0, made `ago_ms` before now with `source`
/// and a `run_at` in 2999, so the queue never takes it; then a row about it for each (status,
/// made that many ms before now) in `statuses`. Returns the ids, the 0's first.
fn event(
    conn: &Connection,
    n: u8,
    ago_ms: u64,
    source: Option<Uuid>,
    statuses: &[(i64, u64)],
) -> Result<Vec<Uuid>> {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let id =
        |s, ago| Builder::from_unix_timestamp_millis(now - ago, &[n, s, 0, 0, 0, 0, 0, 0, 0, 0]);
    let zero = id(0, ago_ms).into_uuid();
    let row = _events::factory().id(zero.to_string()).ty(11).data("{}");
    let row = row.status(0).run_at("2999-01-01T00:00:00.000Z");
    match source {
        Some(s) => row.source(s.to_string()),
        None => row,
    }
    .insert(conn)?;
    let mut ids = vec![zero];
    for (s, &(status, ago)) in (1..).zip(statuses) {
        let about = id(s, ago).into_uuid();
        let row = _events::factory().id(about.to_string()).ty(11).data("{}");
        row.status(status).source(zero.to_string()).insert(conn)?;
        ids.push(about);
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
fn a_flow_whose_step_is_trim_deletes_the_processed_events_older_than_its_options_say_and_keeps_the_rest()
 {
    // Given a store holding two events an hour old and one made now, each with its 1 and 2
    let path = migrated().unwrap();
    let seed = ready(&path).unwrap();
    let done = [(1, HOUR), (2, HOUR)];
    let first = event(&seed, 1, HOUR, None, &done).unwrap();
    let mut old: BTreeSet<Uuid> = first.iter().copied().collect();
    old.extend(event(&seed, 2, HOUR, None, &done).unwrap());
    let fresh: BTreeSet<Uuid> = event(&seed, 3, 0, None, &[(1, 0), (2, 0)])
        .unwrap()
        .into_iter()
        .collect();

    // And an event an hour old whose 1 and 2 were made now
    old.extend(event(&seed, 4, HOUR, None, &[(1, 0), (2, 0)]).unwrap());

    // And events an hour old with no 2: one waiting, one running, one failed, and one waiting
    // whose source is the first old event
    let undone: BTreeSet<Uuid> = [
        event(&seed, 5, HOUR, None, &[]),
        event(&seed, 6, HOUR, None, &[(1, HOUR)]),
        event(&seed, 7, HOUR, None, &[(1, HOUR), (-1, HOUR)]),
        event(&seed, 8, HOUR, Some(first[0]), &[]),
    ]
    .into_iter()
    .flat_map(Result::unwrap)
    .collect();
    assert_eq!(undone.len(), 7);
    assert_eq!(ids(&path).unwrap(), &(&old | &fresh) | &undone);

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

    // Then the event has its 2, and _events holds no row of an old event with a 2 and every
    // other row
    let about = |status: i64| {
        let sql = "SELECT id FROM _events WHERE source = ?1 AND status = ?2";
        let id = seed.query_row(sql, (trim.to_string(), status), |r| r.get::<_, String>(0));
        id.ok().and_then(|i| i.parse::<Uuid>().ok())
    };
    assert!(until(|| about(2).is_some()));
    let ran = [Some(trim), about(1), about(2)].into_iter().flatten();
    let kept: BTreeSet<Uuid> = fresh.iter().chain(&undone).copied().chain(ran).collect();
    assert_eq!(kept.len(), 13);
    let left = ids(&path).unwrap();
    assert_eq!(left, kept, "old: {old:?}");

    // And no event the step deleted waits at 0 with no status row, so none is due again
    let waiting = "SELECT e.id FROM _events e WHERE e.status = 0
        AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0)";
    let mut waiting = seed.prepare(waiting).unwrap();
    let waiting = waiting.query_map([], |r| r.get::<_, String>(0)).unwrap();
    let waiting: BTreeSet<Uuid> = waiting.map(|i| i.unwrap().parse().unwrap()).collect();
    assert!(
        waiting.is_disjoint(&old),
        "due again: {:?}",
        &waiting & &old
    );
    assert_eq!(waiting.len(), 2);

    drop(signal);
    rannoch.shutdown();
    assert_eq!(queue.join(), Ok(()));
}
