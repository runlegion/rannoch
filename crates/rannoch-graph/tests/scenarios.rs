//! #134's and #135's scenarios through the real store, queue and rannoch-processor: an app whose ontology
//! is its own tables, whose flows write them with the built-in `store` step through a `Warm`
//! append, and whose reads are events whose flows call rannoch-graph's steps. Each event is
//! written into `_events` from the app's forgr factory. rannoch-store has one store per
//! process, so the scenarios take turns.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_graph::{Hoods, SIGNATURES, Warm};
use rannoch_processor::{FlowDoc, Processor, Sig, check};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// One scenario at a time: the store is the process's one.
static ONE: Mutex<()> = Mutex::new(());

/// The app's ontology, as its ontology crate's migrations make it: names rannoch-graph never
/// names.
const ONTOLOGY: &str = "CREATE TABLE doc_titles (doc TEXT, title TEXT, event TEXT,
    written INTEGER, PRIMARY KEY (doc, event));
    CREATE TABLE cites (doc TEXT, cited TEXT, written INTEGER, PRIMARY KEY (doc, cited))";

/// The app's flows: a cite written with `store`; the reads, each returning its result as an
/// event of the app's; and the flows those events go to.
const FLOWS: &str = r#"[
  ["cite.json", {"name": "cite", "ty": 40, "takes": {"doc": "string", "cited": "string"},
    "steps": [{"do": "store", "with": {"store": {"table": "cites",
      "row": {"doc": "$doc", "cited": "$cited"}}}}]}],
  ["reach.json", {"name": "reach", "ty": 50, "takes": {"node": "string"},
    "steps": [{"do": "walk", "with": {"walk": {
      "via": {"cites": {"from": "doc", "to": "cited"}}, "hops": 3, "cap": 10}},
      "next": {"ty": 60, "shape": {"walk": "$walk"}}}]}],
  ["nowhere.json", {"name": "nowhere", "ty": 53, "takes": {"node": "string"},
    "steps": [{"do": "walk", "with": {"walk": {
      "via": {"nope": {"from": "doc", "to": "cited"}}, "hops": 3, "cap": 10}},
      "next": {"ty": 60, "shape": {"walk": "$walk"}}}]}],
  ["explain.json", {"name": "explain", "ty": 51, "takes": {"node": "string"},
    "steps": [{"do": "why", "with": {"why": {"table": "doc_titles", "key": "doc",
      "event": "event"}}, "next": {"ty": 61, "shape": {"why": "$why"}}}]}],
  ["then.json", {"name": "then", "ty": 52, "takes": {"node": "string", "at": "number"},
    "steps": [{"do": "as_of", "with": {"as_of": {"table": "doc_titles", "key": "doc",
      "time": "written"}}, "next": {"ty": 62, "shape": {"row": "$row"}}}]}],
  ["known.json", {"name": "known", "ty": 56, "takes": {"node": "string", "at": "number"},
    "steps": [{"do": "as_of", "with": {"as_of": {"table": "doc_titles", "key": "doc",
      "time": "event"}}, "next": {"ty": 62, "shape": {"row": "$row"}}}]}],
  ["since.json", {"name": "since", "ty": 54, "takes": {"node": "string", "at": "number"},
    "steps": [{"do": "walk", "with": {"walk": {
      "via": {"cites": {"from": "doc", "to": "cited", "time": "written"}}, "hops": 3, "cap": 10}},
      "next": {"ty": 60, "shape": {"walk": "$walk"}}}]}],
  ["cited.json", {"name": "cited", "ty": 55, "takes": {"node": "string", "at": "number"},
    "steps": [{"do": "edges", "with": {"edges": {"table": "cites", "from": "doc",
      "time": "written"}}, "next": {"ty": 63, "shape": {"edges": "$edges"}}}]}],
  ["walked.json", {"name": "walked", "ty": 60, "takes": {"walk": "any"}, "steps": []}],
  ["explained.json", {"name": "explained", "ty": 61, "takes": {"why": "list<string>"},
    "steps": []}],
  ["read.json", {"name": "read", "ty": 62, "takes": {"row": "any"}, "steps": []}],
  ["edged.json", {"name": "edged", "ty": 63, "takes": {"edges": "any"}, "steps": []}]
]"#;

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// A fresh store `name`, with every migration of rannoch's applied by opening rannoch once.
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

/// The app running: rannoch, the queue, a connection and the signal it hands events with.
struct App {
    conn: Connection,
    signal: mpsc::Sender<()>,
    rannoch: Rannoch<u16, Value>,
    queue: Queue,
}

impl App {
    /// The app over a fresh store `name` holding its ontology, its flows checked against
    /// rannoch-graph's signatures, and its append `Events` with its writes, in a `Warm`.
    fn start(name: &str) -> Result<Self> {
        let path = migrated(name)?;
        let conn = ready(&path)?;
        conn.execute_batch(ONTOLOGY)?;
        let docs: Vec<(String, FlowDoc)> = serde_json::from_str(FLOWS)?;
        let sigs: Vec<Sig> = serde_json::from_str(SIGNATURES)?;
        let flows = check(&docs, &sigs).map_err(|e| e.join("\n"))?;
        let hoods = Hoods::default();
        let actions = rannoch_graph::actions(&hoods);
        let processor: Processor<u16, Value> = Processor::new(&flows, actions, ready(&path)?)?;
        let (signal, wake) = mpsc::channel();
        let writes = processor.writes();
        let events = Events::new(ready(&path)?, signal.clone()).with_writes(writes.clone());
        let append = Warm::new(events, writes, hoods);
        let rannoch = Rannoch::open(Config {
            store: Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator()))),
            append: Some(Arc::new(append) as Arc<dyn Append<u16, Value>>),
            ..Config::default()
        })?;
        let settings = Settings::new(Duration::from_millis(100));
        let processor = Arc::new(processor);
        let queue = Queue::start(ready(&path)?, wake, rannoch.sender(), processor, settings);
        Ok(Self {
            conn,
            signal,
            rannoch,
            queue,
        })
    }

    /// Hands the queue an event of `ty` with `data`; returns its id.
    fn hand(&self, ty: i64, data: &Value) -> Result<Uuid> {
        let id = Uuid::now_v7();
        let row = _events::factory()
            .id(id.to_string())
            .ty(ty)
            .data(data.to_string());
        row.status(0).insert(&self.conn)?;
        self.signal.send(())?;
        Ok(id)
    }

    /// The data of the row about `id` at `status`, when there is one.
    fn now(&self, id: Uuid, status: i64) -> Option<Value> {
        let sql = "SELECT data FROM _events WHERE source = ?1 AND status = ?2";
        let data = self
            .conn
            .query_row(sql, params![id.to_string(), status], |r| {
                r.get::<_, String>(0)
            });
        data.ok().and_then(|d| serde_json::from_str(&d).ok())
    }

    /// The data of the row about `id` at `status`, once there is one.
    fn about(&self, id: Uuid, status: i64) -> Option<Value> {
        until(|| self.now(id, status).is_some());
        self.now(id, status)
    }

    /// Hands a read of `ty` with `data`, and returns its result: the data of the event it
    /// returns, whose source it is.
    fn read(&self, ty: i64, data: &Value) -> Result<Value> {
        let id = self.hand(ty, data)?;
        self.about(id, 0)
            .ok_or_else(|| format!("read {id} returned nothing").into())
    }

    /// Writes the cite of `cited` by `doc` through the app's flow, and waits for its 2.
    fn cite(&self, doc: &str, cited: &str) -> Result<()> {
        let id = self.hand(40, &json!({ "doc": doc, "cited": cited }))?;
        self.about(id, 2)
            .map(drop)
            .ok_or_else(|| format!("cite {id} has no 2").into())
    }

    fn stop(self) {
        drop(self.signal);
        self.rannoch.shutdown();
        assert!(self.queue.join().is_ok());
    }
}

/// The ids a walk's result reached, in order.
fn ids(result: &Value) -> Vec<&str> {
    let nodes = result["walk"]["nodes"].as_array().into_iter().flatten();
    nodes.filter_map(|n| n["id"].as_str()).collect()
}

/// The names of the tables in the store at `conn`.
fn tables(conn: &Connection) -> Vec<String> {
    let sql = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name";
    let mut st = conn.prepare(sql).unwrap_or_else(|e| panic!("{e}"));
    let names = st
        .query_map([], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{e}"));
    names.collect::<rusqlite::Result<_>>().unwrap_or_default()
}

/// FR-RANNOCH-125-c1: rannoch-graph has no _nodes or _edges tables.
#[test]
fn fr_rannoch_125_c1() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    // Given a store with every one of rannoch's migrations applied
    let path = migrated("c1").unwrap();

    // Then it holds no _nodes or _edges, and rannoch-graph has no migrations to make them
    let held = tables(&ready(&path).unwrap());
    assert!(
        !held.iter().any(|t| t == "_nodes" || t == "_edges"),
        "{held:?}"
    );
    assert!(held.iter().any(|t| t == "_events"), "{held:?}");
    let own = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    assert!(!own.exists());
}

/// FR-RANNOCH-125-c2 and 01a216a5-c6f6-7051-82f2-2c64fc1703ed: the app's shape is its
/// ontology's tables, and they are what rannoch-graph's steps read.
#[test]
fn fr_rannoch_125_c2() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    // Given an app whose ontology holds doc_titles and cites
    let app = App::start("c2").unwrap();

    // Then the store's tables beyond rannoch's _events are the ontology's
    let mut held = tables(&app.conn);
    held.retain(|t| t != "_events" && !t.starts_with("_migratr") && !t.starts_with("sqlite"));
    assert_eq!(held, ["cites", "doc_titles"]);

    // When a flow writes cites into the app's table, and a read walks them
    app.cite("d1", "d2").unwrap();
    app.cite("d2", "d3").unwrap();

    // Then the walk reaches what the app's table holds
    let walked = app.read(50, &json!({ "node": "d1" })).unwrap();
    assert_eq!(ids(&walked), ["d2", "d3"]);
    app.stop();
}

/// FR-RANNOCH-125-c4: every read is an event, and that event returns its result.
#[test]
fn fr_rannoch_125_c4() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    // Given an app whose doc d1 was titled by one event and retitled by the next, at 1,000 and
    // 2,000, and which cites d2 from d1 at 1,000
    let app = App::start("c4").unwrap();
    let (first, fix) = (Uuid::now_v7().to_string(), Uuid::now_v7().to_string());
    let row = |id: &String| {
        _events::factory()
            .id(id.clone())
            .ty(99)
            .data("{}")
            .status(2)
    };
    row(&first).insert(&app.conn).unwrap();
    row(&fix).source(first.clone()).insert(&app.conn).unwrap();
    let title = "INSERT INTO doc_titles VALUES ('d1', ?1, ?2, ?3)";
    app.conn
        .execute(title, params!["graph notes", first, 1_000])
        .unwrap();
    app.conn
        .execute(title, params!["graph store notes", fix, 2_000])
        .unwrap();
    let cite = "INSERT INTO cites VALUES ('d1', 'd2', 1000)";
    app.conn.execute(cite, []).unwrap();

    // When the app sends a walk, a why, and as-of reads of a row, a walk and edges, each as an
    // event
    let walked = app.read(50, &json!({ "node": "d1" })).unwrap();
    let why = app.read(51, &json!({ "node": "d1" })).unwrap();
    let then = app.read(52, &json!({ "node": "d1", "at": 1_500 })).unwrap();
    let before = app.read(54, &json!({ "node": "d1", "at": 999 })).unwrap();
    let since = app.read(54, &json!({ "node": "d1", "at": 1_500 })).unwrap();
    let none = app.read(55, &json!({ "node": "d1", "at": 999 })).unwrap();
    let cited = app.read(55, &json!({ "node": "d1", "at": 1_500 })).unwrap();

    // Then each read event returns its result as an event whose source it is
    assert_eq!(
        walked,
        json!({ "walk": { "nodes": [{ "id": "d2", "hops": 1 }], "cut": false } })
    );
    assert_eq!(why, json!({ "why": [fix, first] }));
    assert_eq!(then["row"]["title"], json!("graph notes"));
    assert_eq!(then["row"]["event"], json!(first));
    assert_eq!(before, json!({ "walk": { "nodes": [], "cut": false } }));
    assert_eq!(since, walked);
    assert_eq!(none, json!({ "edges": { "rows": [] } }));
    let row = json!({ "doc": "d1", "cited": "d2", "written": 1_000 });
    assert_eq!(cited, json!({ "edges": { "rows": [row] } }));
    app.stop();
}

/// FR-RANNOCH-126-c1: rannoch-graph is bitemporal: as of one time, a read by recorded time and
/// a read by valid time give each its own row.
#[test]
fn fr_rannoch_126_c1() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    // Given d1 titled by an event at 1,000, valid from 1,000, and retitled by an event at 2,000,
    // valid from 1,500
    let app = App::start("bitemporal").unwrap();
    let title = "INSERT INTO doc_titles VALUES ('d1', ?1, ?2, ?3)";
    for (name, recorded, valid) in [
        ("graph notes", 1_000, 1_000),
        ("graph store notes", 2_000, 1_500),
    ] {
        let id = uuid::Builder::from_unix_timestamp_millis(recorded, &[0; 10]).into_uuid();
        app.conn
            .execute(title, params![name, id.to_string(), valid])
            .unwrap();
    }

    // When the app reads d1 as of 1,700 by valid time and by recorded time, each as an event
    let at = json!({ "node": "d1", "at": 1_700 });
    let valid = app.read(52, &at).unwrap();
    let recorded = app.read(56, &at).unwrap();

    // Then valid time gives the retitle, and recorded time the title the store held by then
    assert_eq!(valid["row"]["title"], json!("graph store notes"));
    assert_eq!(recorded["row"]["title"], json!("graph notes"));
    app.stop();
}

/// 01a2166f-db5e-76d0-9f75-25c911d0f98c: rannoch-graph reads nothing of its own: a step reads
/// only the table its options name, and one the store does not have fails its read event.
#[test]
fn criterion_01a2166f_rannoch_graph_reads_nothing_of_its_own() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    // Given an app with its ontology
    let app = App::start("utils").unwrap();

    // When a read's walk names a table the store does not have
    let id = app.hand(53, &json!({ "node": "d1" })).unwrap();

    // Then the read event gets a -1 naming it, and returns nothing
    assert_eq!(app.about(id, -1), Some(json!("walk: no such table: nope")));
    let returned = "SELECT count(*) FROM _events WHERE source = ?1 AND ty = 60";
    let returned = app
        .conn
        .query_row(returned, [id.to_string()], |r| r.get::<_, i64>(0));
    assert_eq!(returned.unwrap(), 0);
    app.stop();
}

/// FR-RANNOCH-127-c1: warm hoods are part of rannoch-graph's core: a walk is kept warm, and a
/// cite the app writes through `Warm` drops it, so the next read returns the new reach.
#[test]
fn fr_rannoch_127_c1() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    // Given an app whose d1 cites d2, read once, which warms its hood
    let app = App::start("hoods").unwrap();
    app.cite("d1", "d2").unwrap();
    let node = json!({ "node": "d1" });
    assert_eq!(ids(&app.read(50, &node).unwrap()), ["d2"]);

    // When d2 cites d3 through the app's flow
    app.cite("d2", "d3").unwrap();

    // Then the next read returns the new reach
    assert_eq!(ids(&app.read(50, &node).unwrap()), ["d2", "d3"]);
    app.stop();
}
