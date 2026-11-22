//! graph toy round 2: the whole flow path on rannoch main's real crates. 10,000 events of one
//! ty go through the queue and rannoch-processor to their 2; the one step's action writes
//! nothing (noop) or the event's graph rows, on a connection with synchronous FULL (full) or
//! NORMAL (normal). Predictions in predictions-flow.txt were written before this ran.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use rannoch_core::{Append, Config, Error, Event, Rannoch, Status};
use std::collections::HashMap;
use rannoch_events::Events;
use rannoch_processor::{Action, Processor, Sig};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::{Connection, params};
use serde_json::{Map, Value, json};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq)]
struct Ty(u16);
impl From<Ty> for u16 {
    fn from(t: Ty) -> u16 {
        t.0
    }
}
impl TryFrom<u16> for Ty {
    type Error = ();
    fn try_from(v: u16) -> Result<Self, ()> {
        Ok(Ty(v))
    }
}

const GRAPH: &str = "
CREATE TABLE IF NOT EXISTS _nodes (node TEXT NOT NULL, key TEXT NOT NULL, value TEXT, event TEXT NOT NULL,
  written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (node, key, event)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _edges (src TEXT NOT NULL, name TEXT NOT NULL, dst TEXT NOT NULL, props TEXT,
  event TEXT NOT NULL, written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (src, name, dst, event)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _edges_in ON _edges(dst, name, src, event);";

fn ready(path: &Path) -> Connection {
    match rannoch_store::open(path, &rannoch_core::migrator()).expect("open") {
        Opened::Ready(conn) => conn,
        Opened::Pending(p) => panic!("pending: {p:?}"),
    }
}

/// The action: writes nothing, or the event's graph rows in one transaction.
struct Write {
    sig: Sig,
    conn: Option<Mutex<Connection>>,
    /// batched mode: the rows wait here, by event id, for the append that carries the 2.
    pending: Option<Pending>,
}

type GraphRows = Vec<(String, String, String)>;
type Pending = Arc<Mutex<HashMap<String, GraphRows>>>;

/// batched mode: the events crate's append, plus each 2's graph rows in the same write.
#[derive(Debug)]
struct GraphAppend {
    conn: Mutex<Connection>,
    queue: mpsc::Sender<()>,
    pending: Pending,
}

impl Append<Ty, Value> for GraphAppend {
    fn append(&self, batch: &[Event<Ty, Value>]) -> Result<(), Error> {
        let mut conn = self.conn.lock().map_err(|_| Error::Store)?;
        let written = chrono_ms();
        let tx = conn.transaction().map_err(|_| Error::Store)?;
        {
            let mut ev = tx.prepare_cached("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)").map_err(|_| Error::Store)?;
            let mut node = tx.prepare_cached("INSERT OR IGNORE INTO _nodes (node, key, value, event, written) VALUES (?1, ?2, ?3, ?4, ?5)").map_err(|_| Error::Store)?;
            let mut edge = tx.prepare_cached("INSERT OR IGNORE INTO _edges (src, name, dst, props, event, written) VALUES (?1, 'next', ?2, '{}', ?3, ?4)").map_err(|_| Error::Store)?;
            let mut pending = self.pending.lock().map_err(|_| Error::Store)?;
            for e in batch {
                let run_at = e.run_at.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
                ev.execute(params![e.id.to_string(), e.source.map(|s| s.to_string()), u16::from(e.ty), e.data.to_string(), run_at, e.status as i8])
                    .map_err(|_| Error::Store)?;
                if e.status == Status::Processed
                    && let Some(src) = e.source
                    && let Some(rows) = pending.remove(&src.to_string())
                {
                    for (n, k, v) in &rows {
                        if k == "->" {
                            edge.execute(params![n, v, src.to_string(), written]).map_err(|_| Error::Store)?;
                        } else {
                            node.execute(params![n, k, v, src.to_string(), written]).map_err(|_| Error::Store)?;
                        }
                    }
                }
            }
        }
        tx.commit().map_err(|_| Error::Store)?;
        let _ = self.queue.send(());
        Ok(())
    }
}

impl Action for Write {
    fn sig(&self) -> &Sig {
        &self.sig
    }
    fn run(&self, _options: &Value, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        // Action::run is not given the event's id, so the toy carries it in the data.
        let (node, event) = (input["n"].as_str().unwrap_or_default(), input["event"].as_str().unwrap_or_default());
        if let Some(pending) = &self.pending {
            let rows = vec![
                (node.to_string(), "type".to_string(), json!("thing").to_string()),
                (node.to_string(), "name".to_string(), json!(format!("thing {node}")).to_string()),
                (node.to_string(), "n".to_string(), json!(node).to_string()),
                (node.to_string(), "->".to_string(), format!("{node}+")),
            ];
            pending.lock().map_err(|e| e.to_string())?.insert(event.to_string(), rows);
            return Ok(Map::new());
        }
        let Some(conn) = &self.conn else { return Ok(Map::new()) };
        let written = chrono_ms();
        let mut conn = conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        for (k, v) in [("type", json!("thing")), ("name", json!(format!("thing {node}"))), ("n", json!(node))] {
            tx.execute(
                "INSERT OR IGNORE INTO _nodes (node, key, value, event, written) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![node, k, v.to_string(), event, written],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.execute(
            "INSERT OR IGNORE INTO _edges (src, name, dst, props, event, written) VALUES (?1, 'next', ?2, '{}', ?3, ?4)",
            params![node, format!("{node}+"), event, written],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(Map::new())
    }
}

fn chrono_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "noop".into());
    let dir = PathBuf::from(std::env::args().nth(2).unwrap_or_else(|| "data".into()));
    let n: usize = std::env::args().nth(3).and_then(|v| v.parse().ok()).unwrap_or(10_000);
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join(format!("flow-{mode}.db"));
    for ext in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{ext}", path.display()));
    }

    // Migrate by opening rannoch once, as the processor scenarios do; then the graph tables.
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    Rannoch::<Ty, Value>::open(Config { store: Some(store.clone()), ..Config::default() }).expect("migrate").shutdown();
    ready(&path).execute_batch(GRAPH).expect("graph tables");
    // Round 2b: an index on the expression rannoch-processor's waits_for filters on.
    if std::env::var("WAIT_INDEX").is_ok() {
        ready(&path)
            .execute_batch(
                "CREATE INDEX _events_wait ON _events (json_extract(data, '$.\"$wait\".for'))
                 WHERE json_extract(data, '$.\"$wait\".for') IS NOT NULL",
            )
            .expect("wait index");
        let plan: String = ready(&path)
            .query_row(
                "EXPLAIN QUERY PLAN SELECT w.id FROM _events w WHERE json_extract(w.data, '$.\"$wait\".for') = 40",
                [],
                |r| r.get(3),
            )
            .expect("plan");
        println!("{mode}: wait index plan: {plan}");
    }

    if mode == "inbox" {
        // The inbox and events path alone: no queue, no processor.
        let (dummy, _parked) = mpsc::channel();
        let append = Arc::new(Events::new(ready(&path), dummy)) as Arc<dyn Append<Ty, Value>>;
        let rannoch = Rannoch::open(Config { store: Some(store), append: Some(append), ..Config::default() }).expect("rannoch");
        let sender = rannoch.sender();
        let read = ready(&path);
        let before: i64 = read.query_row("SELECT count(*) FROM _events", [], |r| r.get(0)).expect("count");
        let start = Instant::now();
        for i in 0..n {
            let e = Event { id: Uuid::nil(), source: None, ty: Ty(99), data: json!({"n": format!("t{i}")}), run_at: None, status: Status::Added };
            sender.send(e).expect("send");
        }
        let sent = start.elapsed();
        loop {
            let c: i64 = read.query_row("SELECT count(*) FROM _events", [], |r| r.get(0)).expect("count");
            if (c - before) as usize >= n {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let elapsed = start.elapsed();
        println!("inbox: {n} events sent in {sent:?}, all in _events in {elapsed:?} = {:.0} events/s", n as f64 / elapsed.as_secs_f64());
        rannoch.shutdown();
        return;
    }
    let pending: Option<Pending> = (mode == "batched").then(Pending::default);
    let conn = match mode.as_str() {
        "noop" | "batched" => None,
        "full" => Some(Mutex::new(ready(&path))),
        "normal" => {
            let c = ready(&path);
            c.execute_batch("PRAGMA synchronous = NORMAL").expect("normal");
            Some(Mutex::new(c))
        }
        other => panic!("mode {other}"),
    };
    let sig = Sig { action: "write".into(), ..Sig::default() };
    let actions: Vec<Arc<dyn Action>> = vec![Arc::new(Write { sig, conn, pending: pending.clone() })];
    let flows = r#"[{"name": "write", "ty": 40, "takes": {"n": "string", "event": "string"}, "steps": [{"do": "write"}]}]"#;
    let processor: Processor<Ty, Value> = Processor::new(flows, actions, ready(&path)).expect("processor");
    let (signal, wake) = mpsc::channel();
    let append = match &pending {
        Some(p) => Arc::new(GraphAppend { conn: Mutex::new(ready(&path)), queue: signal.clone(), pending: p.clone() }) as Arc<dyn Append<Ty, Value>>,
        None => Arc::new(Events::new(ready(&path), signal.clone())) as Arc<dyn Append<Ty, Value>>,
    };
    let rannoch = Rannoch::open(Config { store: Some(store), append: Some(append), ..Config::default() }).expect("rannoch");
    let queue = Queue::start(ready(&path), wake, rannoch.sender(), Arc::new(processor), Settings::new(Duration::from_millis(100)));

    // 10,000 events at status 0 in one write, then the queue signalled.
    let read = ready(&path);
    {
        let mut c = ready(&path);
        let tx = c.transaction().expect("tx");
        for i in 0..n {
            let id = Uuid::now_v7().to_string();
            let data = json!({"n": format!("t{i}"), "event": id}).to_string();
            tx.execute("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, NULL, 40, ?2, NULL, 0)", params![id, data])
                .expect("event");
        }
        tx.commit().expect("commit");
    }
    let start = Instant::now();
    // Round 2c: once a second, the WAL and database sizes and what a PASSIVE checkpoint
    // reports (busy, frames in the WAL, frames checkpointed).
    if std::env::var("SAMPLE").is_ok() {
        let (p, m) = (path.clone(), mode.clone());
        std::thread::spawn(move || {
            let c = ready(&p);
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let twos: i64 = c.query_row("SELECT count(*) FROM _events WHERE status = 2", [], |r| r.get(0)).unwrap_or(-1);
                let ck: (i64, i64, i64) = c.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap_or((-1, -1, -1));
                let size = |ext: &str| std::fs::metadata(format!("{}{ext}", p.display())).map_or(0, |x| x.len() / 1_000_000);
                println!("{m} sample {:?}: 2s {twos}, wal {} MB, db {} MB, checkpoint (busy, log, done) {ck:?}", start.elapsed(), size("-wal"), size(""));
            }
        });
    }
    signal.send(()).expect("signal");
    let done = |read: &Connection| -> i64 { read.query_row("SELECT count(*) FROM _events WHERE status = 2", [], |r| r.get(0)).expect("count") };
    let mut last = 0;
    let mut stalled = Instant::now();
    loop {
        let d = done(&read);
        if d as usize >= n {
            break;
        }
        if d != last {
            last = d;
            stalled = Instant::now();
        } else if stalled.elapsed() > Duration::from_secs(30) {
            println!("{mode}: stalled at {d} of {n} 2s");
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let elapsed = start.elapsed();
    let twos = done(&read);
    let nodes: i64 = read.query_row("SELECT count(DISTINCT node) FROM _nodes", [], |r| r.get(0)).expect("nodes");
    let missing: i64 = read
        .query_row(
            "SELECT count(*) FROM _events e WHERE e.status = 0 AND e.ty = 40
               AND EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND +s.status = 2)
               AND e.id NOT IN (SELECT event FROM _nodes)",
            [],
            |r| r.get(0),
        )
        .expect("missing");
    let ones: i64 = read.query_row("SELECT count(*) FROM _events WHERE status = 1", [], |r| r.get(0)).expect("ones");
    println!(
        "{mode}: {twos} of {n} events to their 2 in {elapsed:?} = {:.0} events/s; 1s {ones}; graph nodes {nodes}; events with a 2 and no graph rows {missing}",
        twos as f64 / elapsed.as_secs_f64()
    );
    drop(signal);
    rannoch.shutdown();
    queue.join();
}
