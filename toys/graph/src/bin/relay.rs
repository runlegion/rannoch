//! graph toy round 3: T8's relay rerun on rannoch main's real crates. T4's data goes through the
//! queue and rannoch-processor into FR-RANNOCH-123's _nodes and _edges, written by the append
//! that carries each event's 2; the relay is seeded by describe and folds the committed rows
//! that touch it. Predictions in predictions-relay.txt were written before this ran.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use rannoch_core::{Append, Config, Error, Event, Rannoch, Status};
use rannoch_processor::{Action, Processor, Sig};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
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
CREATE INDEX IF NOT EXISTS _edges_in ON _edges(dst, name, src, event);
CREATE INDEX IF NOT EXISTS _events_wait ON _events (json_extract(data, '$.\"$wait\".for'))
  WHERE json_extract(data, '$.\"$wait\".for') IS NOT NULL;";

fn ready(path: &Path) -> Connection {
    match rannoch_store::open(path, &rannoch_core::migrator()).expect("open") {
        Opened::Ready(conn) => conn,
        Opened::Pending(p) => panic!("pending: {p:?}"),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

#[derive(Debug, Clone)]
enum GOp {
    Set(String, String, Value),
    Link(String, String, String),
    Unlink(String, String, String),
}

impl GOp {
    fn touched(&self) -> Vec<String> {
        match self {
            GOp::Set(n, _, _) => vec![n.clone()],
            GOp::Link(s, _, d) | GOp::Unlink(s, _, d) => vec![s.clone(), d.clone()],
        }
    }
}

type Pending = Arc<Mutex<HashMap<String, Vec<GOp>>>>;

/// One event's committed rows, handed to the relay once.
#[derive(Debug, Clone)]
struct Delivery {
    event: String,
    touched: HashSet<String>,
    ops: Vec<GOp>,
}

type Log = Arc<Mutex<Vec<Delivery>>>;

/// The events crate's append, plus each 2's graph rows in the same transaction; after the
/// commit, each event's added rows go to the delivery log while `recording` is on.
#[derive(Debug)]
struct GraphAppend {
    conn: Mutex<Connection>,
    queue: mpsc::Sender<()>,
    pending: Pending,
    log: Log,
    recording: Arc<AtomicBool>,
}

impl Append<Ty, Value> for GraphAppend {
    fn append(&self, batch: &[Event<Ty, Value>]) -> Result<(), Error> {
        let mut conn = self.conn.lock().map_err(|_| Error::Store)?;
        let written = now_ms();
        let mut done = Vec::new();
        let tx = conn.transaction().map_err(|_| Error::Store)?;
        {
            let mut ev = tx
                .prepare_cached("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")
                .map_err(|_| Error::Store)?;
            let mut node = tx
                .prepare_cached("INSERT OR IGNORE INTO _nodes (node, key, value, event, written, retracted) VALUES (?1, ?2, ?3, ?4, ?5, 0)")
                .map_err(|_| Error::Store)?;
            let mut edge = tx
                .prepare_cached("INSERT OR IGNORE INTO _edges (src, name, dst, props, event, written, retracted) VALUES (?1, ?2, ?3, '{}', ?4, ?5, ?6)")
                .map_err(|_| Error::Store)?;
            let mut pending = self.pending.lock().map_err(|_| Error::Store)?;
            for e in batch {
                let run_at = e.run_at.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
                ev.execute(params![e.id.to_string(), e.source.map(|s| s.to_string()), u16::from(e.ty), e.data.to_string(), run_at, e.status as i8])
                    .map_err(|_| Error::Store)?;
                if e.status != Status::Processed {
                    continue;
                }
                let Some(src) = e.source.map(|s| s.to_string()) else { continue };
                let Some(ops) = pending.remove(&src) else { continue };
                let mut added = Vec::new();
                for op in ops {
                    let n = match &op {
                        GOp::Set(n, k, v) => node.execute(params![n, k, v.to_string(), src, written]),
                        GOp::Link(s, name, d) => edge.execute(params![s, name, d, src, written, 0]),
                        GOp::Unlink(s, name, d) => edge.execute(params![s, name, d, src, written, 1]),
                    }
                    .map_err(|_| Error::Store)?;
                    if n == 1 {
                        added.push(op);
                    }
                }
                if !added.is_empty() {
                    done.push(Delivery { event: src, touched: added.iter().flat_map(GOp::touched).collect(), ops: added });
                }
            }
        }
        tx.commit().map_err(|_| Error::Store)?;
        if self.recording.load(Ordering::SeqCst) {
            self.log.lock().map_err(|_| Error::Store)?.extend(done);
        }
        let _ = self.queue.send(());
        Ok(())
    }
}

/// The one action: turns an event's data into graph writes, held by event id for the append.
struct GraphAction {
    sig: Sig,
    pending: Pending,
    source: Mutex<Connection>,
}

impl Action for GraphAction {
    fn sig(&self) -> &Sig {
        &self.sig
    }
    fn run(&self, _options: &Value, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let s = |k: &str| input.get(k).and_then(Value::as_str).unwrap_or_default().to_string();
        let event = s("event");
        let ops = match s("do").as_str() {
            "import" => {
                let (from, to) = (input["from"].as_i64().unwrap_or(0), input["to"].as_i64().unwrap_or(0));
                let src = self.source.lock().map_err(|e| e.to_string())?;
                let mut ops = Vec::new();
                let mut st = src
                    .prepare_cached(
                        "SELECT s.id, json_extract(s.props, '$.place'), said.src FROM node s
                         JOIN link said ON said.kind = 'said' AND said.dst = s.id
                         WHERE s.kind = 'statement' AND s.id >= ?1 AND s.id < ?2 AND s.key NOT LIKE 't8b-%'",
                    )
                    .map_err(|e| e.to_string())?;
                let rows = st
                    .query_map(params![from, to], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, i64>(2)?)))
                    .map_err(|e| e.to_string())?;
                for r in rows {
                    let (sid, place, v) = r.map_err(|e| e.to_string())?;
                    ops.push(GOp::Set(format!("s{sid}"), "place".into(), json!(place.unwrap_or_default())));
                    ops.push(GOp::Link(format!("v{v}"), "said".into(), format!("s{sid}")));
                }
                let mut st = src
                    .prepare_cached(
                        "SELECT l.src, l.dst FROM link l JOIN node s ON s.id = l.src
                         WHERE l.kind = 'supports' AND l.src >= ?1 AND l.src < ?2 AND s.key NOT LIKE 't8b-%'",
                    )
                    .map_err(|e| e.to_string())?;
                let rows = st.query_map(params![from, to], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))).map_err(|e| e.to_string())?;
                for r in rows {
                    let (sid, f) = r.map_err(|e| e.to_string())?;
                    ops.push(GOp::Link(format!("s{sid}"), "supports".into(), format!("f{f}")));
                }
                ops
            }
            "judge" => {
                let (st, f) = (s("stmt"), s("finding"));
                if s("verdict") == "not_supports" {
                    vec![GOp::Unlink(st, "supports".into(), f)]
                } else {
                    vec![GOp::Link(st, "supports".into(), f)]
                }
            }
            "write" => vec![
                GOp::Set(s("stmt"), "place".into(), json!(s("place"))),
                GOp::Link(s("voice"), "said".into(), s("stmt")),
                GOp::Link(s("stmt"), "supports".into(), s("finding")),
            ],
            other => return Err(format!("no such do: {other}")),
        };
        self.pending.lock().map_err(|e| e.to_string())?.insert(event, ops);
        Ok(Map::new())
    }
}

// ---- reads: the graph toy's prop, edges and describe, over the tables as written ----

fn parse(v: Option<String>) -> Value {
    v.map_or(Value::Null, |s| serde_json::from_str(&s).unwrap_or(Value::Null))
}

fn prop(c: &Connection, node: &str, key: &str) -> Option<(Value, String, i64)> {
    c.prepare_cached("SELECT value, event, written, retracted FROM _nodes WHERE node = ?1 AND key = ?2 ORDER BY event DESC LIMIT 1")
        .expect("prep")
        .query_row(params![node, key], |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?)))
        .optional()
        .expect("prop")
        .and_then(|(v, e, w, ret)| (ret == 0).then(|| (parse(v), e, w)))
}

fn edges(c: &Connection, node: &str, name: &str, reverse: bool) -> Vec<(String, String, i64)> {
    let sql = if reverse {
        "SELECT src, event, written, retracted FROM _edges WHERE dst = ?1 AND name = ?2 ORDER BY src, event DESC"
    } else {
        "SELECT dst, event, written, retracted FROM _edges WHERE src = ?1 AND name = ?2 ORDER BY dst, event DESC"
    };
    let mut st = c.prepare_cached(sql).expect("prep");
    let rows = st
        .query_map(params![node, name], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?)))
        .expect("q");
    let mut out = Vec::new();
    let mut last: Option<String> = None;
    for r in rows {
        let (far, e, w, ret) = r.expect("row");
        if last.as_deref() == Some(far.as_str()) {
            continue;
        }
        last = Some(far.clone());
        if ret == 0 {
            out.push((far, e, w));
        }
    }
    out
}

/// describe, as the graph toy's, for named properties and edges (forwards and backwards).
fn describe(c: &Connection, node: &str, shape: &Value) -> Value {
    let mut out = Map::new();
    out.insert("id".into(), json!(node));
    for item in shape.as_array().expect("list") {
        match item {
            Value::String(k) => {
                if let Some((v, e, w)) = prop(c, node, k) {
                    out.insert(k.clone(), json!({"value": v, "event": e, "written": w}));
                }
            }
            Value::Object(m) => {
                for (spec, sub) in m {
                    let (name, reverse) = spec.strip_suffix("<-").map_or((spec.as_str(), false), |n| (n, true));
                    let kids: Vec<Value> = edges(c, node, name, reverse)
                        .into_iter()
                        .map(|(far, e, w)| {
                            let mut child = describe(c, &far, sub);
                            child["edge"] = json!({"event": e, "written": w});
                            child
                        })
                        .collect();
                    out.insert(spec.clone(), Value::Array(kids));
                }
            }
            _ => panic!("bad shape"),
        }
    }
    Value::Object(out)
}

// ---- app side: T8's fold, the Crowd, over the deliveries that touch it ----

#[derive(Default, Clone, PartialEq, Debug)]
struct Counts {
    distinct: usize,
    total: usize,
    by_place: Vec<(String, usize, usize)>,
}

#[derive(Default)]
struct Crowd {
    finding: String,
    stmt: HashMap<String, (String, String)>,
    active: HashSet<String>,
    voice_n: HashMap<String, usize>,
    place_voice_n: HashMap<(String, String), usize>,
    applied: HashSet<String>,
}

impl Crowd {
    fn activate(&mut self, s: &str) -> bool {
        if !self.active.insert(s.to_string()) {
            return false;
        }
        let (v, p) = self.stmt[s].clone();
        *self.voice_n.entry(v.clone()).or_default() += 1;
        *self.place_voice_n.entry((p, v)).or_default() += 1;
        true
    }
    fn deactivate(&mut self, s: &str) -> bool {
        if !self.active.remove(s) {
            return false;
        }
        let (v, p) = self.stmt[s].clone();
        let n = self.voice_n.get_mut(&v).expect("voice");
        *n -= 1;
        if *n == 0 {
            self.voice_n.remove(&v);
        }
        let k = (p, v);
        let n = self.place_voice_n.get_mut(&k).expect("place voice");
        *n -= 1;
        if *n == 0 {
            self.place_voice_n.remove(&k);
        }
        true
    }
    /// Folds one delivery; returns whether the counts changed and any statement that joined.
    fn apply(&mut self, d: &Delivery) -> (bool, Option<(String, String)>) {
        if !self.applied.insert(d.event.clone()) {
            return (false, None);
        }
        let mut changed = false;
        let mut joined = None;
        for op in &d.ops {
            match op {
                GOp::Unlink(s, name, f) if name == "supports" && *f == self.finding => {
                    if self.stmt.contains_key(s) {
                        changed |= self.deactivate(s);
                    }
                }
                GOp::Link(s, name, f) if name == "supports" && *f == self.finding => {
                    if !self.stmt.contains_key(s) {
                        let voice = d.ops.iter().find_map(|o| match o {
                            GOp::Link(v, n, st) if n == "said" && st == s => Some(v.clone()),
                            _ => None,
                        });
                        let place = d.ops.iter().find_map(|o| match o {
                            GOp::Set(n, k, v) if n == s && k == "place" => v.as_str().map(str::to_string),
                            _ => None,
                        });
                        let (Some(v), Some(p)) = (voice, place) else { continue };
                        self.stmt.insert(s.clone(), (v.clone(), p));
                        joined = Some((s.clone(), v));
                    }
                    changed |= self.activate(s);
                }
                _ => {}
            }
        }
        (changed, joined)
    }
    fn counts(&self) -> Counts {
        let mut places: HashMap<String, (usize, usize)> = HashMap::new();
        for ((p, _), n) in &self.place_voice_n {
            let e = places.entry(p.clone()).or_default();
            e.0 += 1;
            e.1 += n;
        }
        let mut by_place: Vec<_> = places.into_iter().map(|(p, (d, n))| (p, d, n)).collect();
        by_place.sort();
        Counts { distinct: self.voice_n.len(), total: self.active.len(), by_place }
    }
}

struct Relay {
    nodes: HashSet<String>,
    crowd: Crowd,
    cursor: usize,
    seen: usize,
    delivered: usize,
}

impl Relay {
    /// Seeded by describe; the walk only seeds it.
    fn seed(c: &Connection, finding: &str) -> Relay {
        let hood = describe(c, finding, &json!([{"supports<-": ["place", {"said<-": []}]}]));
        let mut crowd = Crowd { finding: finding.to_string(), ..Crowd::default() };
        for st in hood["supports<-"].as_array().expect("statements") {
            let s = st["id"].as_str().expect("id").to_string();
            let p = st["place"]["value"].as_str().unwrap_or_default().to_string();
            let Some(v) = st["said<-"][0]["id"].as_str() else { continue };
            crowd.stmt.insert(s.clone(), (v.to_string(), p));
            let _ = crowd.activate(&s);
        }
        let mut nodes: HashSet<String> = crowd.stmt.keys().cloned().collect();
        nodes.insert(finding.to_string());
        nodes.extend(crowd.stmt.values().map(|(v, _)| v.clone()));
        Relay { nodes, crowd, cursor: 0, seen: 0, delivered: 0 }
    }

    /// Takes the deliveries after the cursor; folds those that touch the relay's nodes.
    fn pump(&mut self, log: &Log) -> bool {
        let fresh: Vec<Delivery> = log.lock().expect("log")[self.cursor..].to_vec();
        let mut changed = false;
        for d in fresh {
            self.cursor += 1;
            self.seen += 1;
            if d.touched.iter().any(|n| self.nodes.contains(n)) {
                self.delivered += 1;
                let (c, joined) = self.crowd.apply(&d);
                if let Some((s, v)) = joined {
                    self.nodes.insert(s);
                    self.nodes.insert(v);
                }
                changed |= c;
            }
        }
        changed
    }
}

/// Control: the counts recomputed from _nodes and _edges.
fn recompute(c: &Connection, finding: &str) -> Counts {
    let sql = "WITH sup AS (
        SELECT e.src AS s FROM _edges e WHERE e.dst = ?1 AND e.name = 'supports'
          AND e.event = (SELECT max(f.event) FROM _edges f WHERE f.src = e.src AND f.name = e.name AND f.dst = e.dst)
          AND e.retracted = 0),
      sv AS (SELECT sup.s, (SELECT e.src FROM _edges e WHERE e.dst = sup.s AND e.name = 'said' AND e.retracted = 0
                            ORDER BY e.event DESC LIMIT 1) AS v FROM sup),
      pl AS (SELECT sv.s, sv.v, (SELECT n.value FROM _nodes n WHERE n.node = sv.s AND n.key = 'place'
                                 ORDER BY n.event DESC LIMIT 1) AS p FROM sv)
      SELECT p, count(DISTINCT v), count(*) FROM pl GROUP BY p ORDER BY p";
    let mut st = c.prepare_cached(sql).expect("recompute");
    let rows: Vec<(String, usize, usize)> = st
        .query_map(params![finding], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as usize, r.get::<_, i64>(2)? as usize)))
        .expect("q")
        .map(|r| {
            let (p, d, n) = r.expect("row");
            (serde_json::from_str::<String>(&p).unwrap_or(p), d, n)
        })
        .collect();
    let (distinct, total): (i64, i64) = c
        .query_row(
            &format!("{} ", sql.replace("SELECT p, count(DISTINCT v), count(*) FROM pl GROUP BY p ORDER BY p", "SELECT count(DISTINCT v), count(*) FROM pl")),
            params![finding],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("totals");
    let mut by_place = rows;
    by_place.sort();
    Counts { distinct: distinct as usize, total: total as usize, by_place }
}

struct App {
    path: PathBuf,
    signal: mpsc::Sender<()>,
    read: Connection,
}

impl App {
    /// Events of (ty, data) put in _events at status 0 with known ids, the queue signalled,
    /// and waited for until each has its 2.
    fn hand(&self, evs: Vec<(u16, Value)>) -> Vec<String> {
        let mut c = ready(&self.path);
        let tx = c.transaction().expect("tx");
        let mut ids = Vec::new();
        for (ty, mut data) in evs {
            let id = Uuid::now_v7().to_string();
            data["event"] = json!(id);
            tx.execute("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, NULL, ?2, ?3, NULL, 0)", params![id, ty, data.to_string()])
                .expect("event");
            ids.push(id);
        }
        tx.commit().expect("commit");
        self.signal.send(()).expect("signal");
        let last = ids.last().cloned().expect("ids");
        let start = Instant::now();
        loop {
            let twos: i64 = self
                .read
                .query_row("SELECT count(*) FROM _events WHERE source = ?1 AND +status = 2", [&last], |r| r.get(0))
                .expect("count");
            let all: i64 = if twos > 0 {
                let marks = ids.iter().map(|i| format!("'{i}'")).collect::<Vec<_>>().join(",");
                self.read.query_row(&format!("SELECT count(*) FROM _events WHERE source IN ({marks}) AND +status = 2"), [], |r| r.get(0)).expect("all")
            } else {
                0
            };
            if all as usize == ids.len() {
                return ids;
            }
            assert!(start.elapsed() < Duration::from_secs(600), "events did not reach their 2");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| "data".into()));
    let source_path = std::env::args().nth(2).expect("the T8 data file");
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("relay.db");
    for ext in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{ext}", path.display()));
    }
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    Rannoch::<Ty, Value>::open(Config { store: Some(store.clone()), ..Config::default() }).expect("migrate").shutdown();
    ready(&path).execute_batch(GRAPH).expect("graph tables");

    let open_source = || {
        Connection::open_with_flags(format!("file:{source_path}?immutable=1"), OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI).expect("source")
    };
    let src = open_source();
    let pending = Pending::default();
    let log = Log::default();
    let recording = Arc::new(AtomicBool::new(false));
    let sig = Sig { action: "graph".into(), ..Sig::default() };
    let action = GraphAction { sig, pending: pending.clone(), source: Mutex::new(open_source()) };
    let flows = r#"[
      {"name": "import", "ty": 50, "takes": {}, "steps": [{"do": "graph"}]},
      {"name": "judge", "ty": 51, "takes": {}, "steps": [{"do": "graph"}]},
      {"name": "write", "ty": 52, "takes": {}, "steps": [{"do": "graph"}]}]"#;
    let processor: Processor<Ty, Value> = Processor::new(flows, vec![Arc::new(action)], ready(&path)).expect("processor");
    let (signal, wake) = mpsc::channel();
    let append = Arc::new(GraphAppend { conn: Mutex::new(ready(&path)), queue: signal.clone(), pending, log: log.clone(), recording: recording.clone() });
    // KEEP_STORE=1 holds the app's own Arc to the store, so its checkpointer outlives open.
    let _kept = std::env::var("KEEP_STORE").is_ok().then(|| store.clone());
    let rannoch = Rannoch::open(Config { store: Some(store), append: Some(append as Arc<dyn Append<Ty, Value>>), ..Config::default() }).expect("rannoch");
    let queue = Queue::start(ready(&path), wake, rannoch.sender(), Arc::new(processor), Settings::new(Duration::from_millis(100)));
    let app = App { path: path.clone(), signal: signal.clone(), read: ready(&path) };

    // R0: load T4's data through the real path, 1,000 import events.
    let (lo, hi): (i64, i64) = src.query_row("SELECT min(id), max(id) FROM node WHERE kind = 'statement'", [], |r| Ok((r.get(0)?, r.get(1)?))).expect("range");
    let step = (hi - lo) / 1000 + 1;
    let imports: Vec<(u16, Value)> = (0..1000).map(|i| (50, json!({"do": "import", "from": lo + i * step, "to": lo + (i + 1) * step}))).collect();
    let t = Instant::now();
    app.hand(imports);
    let load = t.elapsed();
    let count = |sql: &str| -> i64 { app.read.query_row(sql, [], |r| r.get(0)).expect("count") };
    let rows = count("SELECT count(*) FROM _nodes") + count("SELECT count(*) FROM _edges");
    println!(
        "R0 load: 1,000 import events to their 2 in {load:?}: {} statement rows, {} edge rows, {:.0} graph rows/s",
        count("SELECT count(*) FROM _nodes"),
        count("SELECT count(*) FROM _edges"),
        rows as f64 / load.as_secs_f64()
    );

    // The largest finding, as T8 chose it.
    let finding_id: i64 = src
        .query_row("SELECT l.dst FROM link l JOIN node s ON s.id = l.src WHERE l.kind = 'supports' AND s.key NOT LIKE 't8b-%' GROUP BY l.dst ORDER BY count(*) DESC LIMIT 1", [], |r| r.get(0))
        .expect("finding");
    let finding = format!("f{finding_id}");

    let t = Instant::now();
    let mut relay = Relay::seed(&app.read, &finding);
    let seed_ms = t.elapsed();
    recording.store(true, Ordering::SeqCst);
    let seed = relay.crowd.counts();
    let t = Instant::now();
    let ctl0 = recompute(&app.read, &finding);
    let ctl_ms = t.elapsed();
    println!(
        "seed by describe {seed_ms:?}: {} voices, {} statements, {} places, {} relay nodes; control {ctl_ms:?}, equal {}",
        seed.distinct,
        seed.total,
        seed.by_place.len(),
        relay.nodes.len(),
        seed == ctl0
    );

    let mut by_voice: HashMap<String, Vec<String>> = HashMap::new();
    for (s, (v, _)) in &relay.crowd.stmt {
        by_voice.entry(v.clone()).or_default().push(s.clone());
    }
    let mut vs: Vec<_> = by_voice.iter().collect();
    vs.sort();
    let many = vs.iter().find(|(_, s)| s.len() >= 3).expect("a voice with three").1.clone();
    let a1 = many[0].clone();
    let b = vs.iter().find(|(_, s)| s.len() == 1).expect("a voice with one").1[0].clone();
    let others: Vec<(String, String)> = {
        let mut st = src
            .prepare("SELECT l.src, l.dst FROM link l JOIN node s ON s.id = l.src WHERE l.kind = 'supports' AND l.dst != ?1 AND s.key NOT LIKE 't8b-%' LIMIT 200")
            .expect("others");
        st.query_map([finding_id], |r| Ok((format!("s{}", r.get::<_, i64>(0)?), format!("f{}", r.get::<_, i64>(1)?))))
            .expect("q")
            .map(|r| r.expect("row"))
            .collect()
    };
    let judge = |s: &str, f: &str, verdict: &str| (51u16, json!({"do": "judge", "stmt": s, "finding": f, "verdict": verdict}));
    let noise = |k: usize| -> Vec<(u16, Value)> { others[k * 50..(k + 1) * 50].iter().map(|(s, f)| judge(s, f, "not_supports")).collect() };

    let mut pumps = Vec::new();
    let mut ctls = Vec::new();
    let mut step = |name: &str, relay: &mut Relay, expect: &dyn Fn(&Counts, &Counts) -> bool, before: &Counts| -> Counts {
        let d0 = relay.delivered;
        let t = Instant::now();
        let changed = relay.pump(&log);
        let pump = t.elapsed();
        let now = relay.crowd.counts();
        let t = Instant::now();
        let ctl = recompute(&app.read, &finding);
        let ctl_t = t.elapsed();
        if relay.delivered > d0 {
            pumps.push(pump);
        }
        ctls.push(ctl_t);
        println!(
            "{name}: pump {pump:?} ({} delivered), changed {changed}, voices {} -> {}, statements {} -> {}, as predicted {}, control {ctl_t:?} equal {}",
            relay.delivered - d0,
            before.distinct,
            now.distinct,
            before.total,
            now.total,
            expect(before, &now),
            now == ctl
        );
        now
    };

    let mut evs = noise(0);
    evs.push(judge(&a1, &finding, "not_supports"));
    let ids = app.hand(evs);
    let one_event = count(&format!("SELECT count(*) FROM _events WHERE id = '{}'", ids[50]));
    let one_row = count(&format!("SELECT count(*) FROM _edges WHERE event = '{}'", ids[50])) + count(&format!("SELECT count(*) FROM _nodes WHERE event = '{}'", ids[50]));
    println!("R1 the judgment is {one_event} event with {one_row} graph row");
    let s1 = step("R3 judge A1 (voice has more)", &mut relay, &|b, n| n.distinct == b.distinct && n.total + 1 == b.total, &seed);
    let mut evs = noise(1);
    evs.push(judge(&b, &finding, "not_supports"));
    app.hand(evs);
    let s2 = step("R3 judge B (voice's only one)", &mut relay, &|b, n| n.distinct + 1 == b.distinct && n.total + 1 == b.total, &s1);
    app.hand(vec![judge(&a1, &finding, "not_supports")]);
    let s3 = step("R7 judge A1 again", &mut relay, &|b, n| b == n, &s2);
    let last = log.lock().expect("log")[relay.cursor - 1].clone();
    let (again, _) = relay.crowd.apply(&last);
    println!("R7 the same event folded twice: changed {again}");
    let mut evs = noise(2);
    evs.push(judge(&b, &finding, "supports"));
    app.hand(evs);
    let s4 = step("R6 replace B's judgment", &mut relay, &|_, n| n.distinct == s1.distinct && n.total == s1.total, &s3);
    app.hand(vec![judge(&a1, &finding, "supports")]);
    let s5 = step("R6 replace A1's judgment", &mut relay, &|_, n| *n == seed, &s4);
    app.hand(noise(3));
    let s6 = step("noise only", &mut relay, &|b, n| b == n, &s5);

    // T8b: new members join a live relay.
    let known = relay.crowd.stmt[&a1].0.clone();
    let other_finding = others[0].1.clone();
    let outside = (0..200_000).map(|i| format!("v{i}")).find(|v| !relay.nodes.contains(v)).expect("outside voice");
    let write = |s: &str, v: &str, f: &str, p: &str| (52u16, json!({"do": "write", "stmt": s, "voice": v, "finding": f, "place": p}));
    let n0 = relay.nodes.len();
    app.hand(vec![write("s-new1", &known, &finding, "discord")]);
    let s7 = step("R8 new statement, known voice", &mut relay, &|b, n| n.distinct == b.distinct && n.total == b.total + 1, &s6);
    println!("R12 node set grew by {} (predicted 1)", relay.nodes.len() - n0);
    let n1 = relay.nodes.len();
    app.hand(vec![write("s-new2", "v-new", &finding, "reddit")]);
    let s8 = step("R9 new statement, new voice", &mut relay, &|b, n| n.distinct == b.distinct + 1 && n.total == b.total + 1, &s7);
    println!("R12 node set grew by {} (predicted 2; new voice in set {})", relay.nodes.len() - n1, relay.nodes.contains("v-new"));
    let d0 = relay.delivered;
    app.hand(vec![judge("s-new2", &finding, "not_supports")]);
    let s9 = step("R10 judge the new voice's only statement", &mut relay, &|b, n| n.distinct + 1 == b.distinct && n.total + 1 == b.total, &s8);
    println!("R10 delivered {}", relay.delivered - d0);
    let d1 = relay.delivered;
    app.hand(vec![write("s-new3", &outside, &other_finding, "steam")]);
    let s10 = step("R11 other finding, outside voice", &mut relay, &|b, n| b == n, &s9);
    println!("R11 delivered {} (predicted 0)", relay.delivered - d1);
    let d2 = relay.delivered;
    app.hand(vec![write("s-new4", &known, &other_finding, "steam")]);
    let _ = step("R11b other finding, a voice in the relay", &mut relay, &|b, n| b == n, &s10);
    println!("R11b delivered {} (predicted 1, changing nothing)", relay.delivered - d2);
    println!("R5 deliveries seen {}, folded by the relay {} (judgments on this finding 5, noise 200, T8b 5)", relay.seen, relay.delivered);
    pumps.sort();
    ctls.sort();
    println!(
        "R2 delivered pumps: min {:?}, max {:?}; control recompute: min {:?}, max {:?}",
        pumps.first(),
        pumps.last(),
        ctls.first(),
        ctls.last()
    );
    // The App holds a clone of the queue's wake Sender; drop it, or the queue never stops.
    drop(app);
    drop(signal);
    rannoch.shutdown();
    queue.join();
}
