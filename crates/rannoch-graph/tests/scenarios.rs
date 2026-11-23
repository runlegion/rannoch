//! FR-RANNOCH-123-c10 through the real store, queue and rannoch-processor, as in
//! rannoch-processor-retrieve's scenarios: events from the app's forgr factory, handed over by
//! the queue to a flow whose one step gives the event's graph rows as `$graph`, and
//! `GraphAppend` writing them with the 2.

use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_graph::{GraphAppend, edges, prop};
use rannoch_processor::{Action, FlowDoc, Processor, Sig, check};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// How many events the queue hands to the flow.
const N: i64 = 500;

const FLOWS: &str = r#"[{"name": "write", "ty": 40, "takes": {"n": "string"},
  "steps": [{"do": "write"}]}]"#;

/// The step's action: the event's node gets a name and a next edge, given as `$graph`.
struct Write(Sig);
impl Action for Write {
    fn sig(&self) -> &Sig {
        &self.0
    }
    fn run(
        &self,
        _: &Value,
        input: &Map<String, Value>,
    ) -> std::result::Result<Map<String, Value>, String> {
        let n = input.get("n").and_then(Value::as_str).unwrap_or_default();
        let graph = json!([
            { "set": { "node": n, "key": "name", "value": format!("thing {n}") } },
            { "link": { "src": n, "name": "next", "dst": format!("{n}+"), "props": {} } }
        ]);
        Ok([("$graph".to_owned(), graph)].into_iter().collect())
    }
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// A fresh store `name` for the app, with every migration applied by opening rannoch once.
fn migrated(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
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

/// Waits up to thirty seconds for `done`.
fn until(done: impl Fn() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    done()
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap_or(-1)
}

/// Every event that has its 2 has its graph rows: they commit in the same transaction.
#[test]
fn fr_rannoch_123_c10() {
    // Given an app whose flow's step gives each event's graph rows, appended by GraphAppend
    let path = migrated("graph").unwrap();
    let sig = Sig {
        action: "write".to_owned(),
        ..Sig::default()
    };
    let actions: Vec<Arc<dyn Action>> = vec![Arc::new(Write(sig))];
    let processor: Processor<u16, Value> =
        Processor::new(FLOWS, actions, ready(&path).unwrap()).unwrap();
    let (signal, wake) = mpsc::channel();
    let append = GraphAppend::new(ready(&path).unwrap(), signal.clone(), processor.writes());
    let rannoch = Rannoch::open(Config {
        store: Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator()))),
        append: Some(Arc::new(append) as Arc<dyn Append<u16, Value>>),
        ..Config::default()
    })
    .unwrap();
    let settings = Settings::new(Duration::from_millis(100));
    let (conn, sender) = (ready(&path).unwrap(), rannoch.sender());
    let queue = Queue::start(conn, wake, sender, Arc::new(processor), settings);

    // When the queue hands the flow N events from the app's forgr factory
    let conn = ready(&path).unwrap();
    let ids: Vec<Uuid> = (0..N)
        .map(|i| {
            let id = Uuid::now_v7();
            let data = json!({ "n": format!("t{i}") }).to_string();
            let row = _events::factory().id(id.to_string()).ty(40).data(data);
            row.status(0).insert(&conn).unwrap();
            id
        })
        .collect();
    signal.send(()).unwrap();
    let twos = "SELECT count(*) FROM _events WHERE status = 2";
    assert!(
        until(|| count(&conn, twos) == N),
        "{} of {N} 2s",
        count(&conn, twos)
    );

    // Then every event that has its 2 has its graph rows, stamped with the event
    let missing = "SELECT count(*) FROM _events e WHERE e.status = 0 AND e.ty = 40
        AND EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status = 2)
        AND (e.id NOT IN (SELECT event FROM _nodes) OR e.id NOT IN (SELECT event FROM _edges))";
    assert_eq!(count(&conn, missing), 0);
    assert_eq!(count(&conn, "SELECT count(*) FROM _nodes"), N);
    assert_eq!(count(&conn, "SELECT count(*) FROM _edges"), N);
    let name = prop(&conn, "t0", "name", None).unwrap().unwrap();
    assert_eq!((name.value, name.event), (json!("thing t0"), ids[0]));
    let next = edges(&conn, "t0", "next", false, None).unwrap();
    assert_eq!((next[0].0.as_str(), next[0].1.event), ("t0+", ids[0]));
    drop(signal);
    rannoch.shutdown();
    queue.join().unwrap();
}

/// #92's flows, from the fixture `tests/payments.json`, of the built-in steps alone: a payment
/// stores its row and sends its receipt.
const PAYMENTS: &str = include_str!("payments.json");

/// Payments that must name an order the store has.
const ORDERS: &str = "CREATE TABLE orders (id TEXT PRIMARY KEY);
    CREATE TABLE payments (order_id TEXT PRIMARY KEY REFERENCES orders(id), amount INTEGER)";

/// #92: the README's "With `GraphAppend`, an event's 2 and its graph rows commit together", on
/// the built-in `store`, whose row is the only kind a built-in step gives; and #102: a row the
/// table refuses is written neither with its 2 nor in place of the rest of the batch.
#[test]
fn with_graph_append_a_2_and_its_rows_commit_together_and_a_refused_row_gets_a_minus_1() {
    // Given an app whose payment flow stores each payment's row, appended by GraphAppend, and
    // whose payments must name an order the store has
    let path = migrated("graph_built_in").unwrap();
    let conn = ready(&path).unwrap();
    conn.execute_batch(ORDERS).unwrap();
    let docs: Vec<(String, FlowDoc)> = serde_json::from_str(PAYMENTS).unwrap();
    let flows = check(&docs, &[]).unwrap();
    let processor: Processor<u16, Value> =
        Processor::new(&flows, Vec::new(), ready(&path).unwrap()).unwrap();
    let (signal, wake) = mpsc::channel();
    let writes = processor.writes();
    let append = GraphAppend::new(ready(&path).unwrap(), signal.clone(), writes.clone());
    let rannoch = Rannoch::open(Config {
        store: Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator()))),
        append: Some(Arc::new(append) as Arc<dyn Append<u16, Value>>),
        ..Config::default()
    })
    .unwrap();
    let settings = Settings::new(Duration::from_millis(100));
    let queue = Queue::start(
        ready(&path).unwrap(),
        wake,
        rannoch.sender(),
        Arc::new(processor),
        settings,
    );
    let pay = |order: &str| {
        let data = json!({ "order": order, "amount": 5 }).to_string();
        let id = Uuid::now_v7();
        let row = _events::factory().id(id.to_string()).ty(86).data(data);
        row.status(0).insert(&conn).unwrap();
        signal.send(()).unwrap();
        id
    };

    // When 200 payments for orders the store has are handed to the flow, one a millisecond
    // Then no read, while they run, sees a payment with its 2 and without its row
    let twos = "SELECT count(*) FROM _events e WHERE e.ty = 86 AND e.source IS NULL
        AND EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status = 2)";
    let bare = format!(
        "{twos} AND NOT EXISTS (SELECT 1 FROM payments p
        WHERE p.order_id = json_extract(e.data, '$.order'))"
    );
    let (start, mut sent) = (Instant::now(), 0);
    while count(&conn, twos) < 200 {
        if sent < 200 {
            let order = format!("o{sent}");
            conn.execute("INSERT INTO orders (id) VALUES (?1)", [&order])
                .unwrap();
            pay(&order);
            sent += 1;
        }
        assert_eq!(count(&conn, &bare), 0);
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "{} of 200",
            count(&conn, twos)
        );
        thread::sleep(Duration::from_millis(1));
    }

    // When a payment for an order the store does not have runs
    let none = pay("none");

    // Then neither its row nor its 2 is written, and it gets a -1 naming the reason in their
    // place; the rest of its batch, its receipt, is written
    let about = |status| {
        let sql = "SELECT json_group_array(data) FROM _events WHERE source = ?1 AND status = ?2";
        let data = conn.query_row(sql, rusqlite::params![none.to_string(), status], |r| {
            r.get(0)
        });
        data.unwrap_or_else(|e: rusqlite::Error| e.to_string())
    };
    assert!(until(|| about(-1) != "[]"));
    assert_eq!(about(-1), r#"["\"store: FOREIGN KEY constraint failed\""]"#);
    assert_eq!(about(2), "[]");
    assert_eq!(about(0), r#"["{\"order\":\"none\"}"]"#);
    let row = "SELECT count(*) FROM payments WHERE order_id = 'none'";
    assert_eq!(count(&conn, row), 0);
    assert!(!writes.lock().unwrap().contains_key(&none));
    drop(signal);
    rannoch.shutdown();
    queue.join().unwrap();
}
