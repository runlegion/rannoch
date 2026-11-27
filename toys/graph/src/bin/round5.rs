//! graph toy round 5 (predictions-round5.txt, written before this code): round 3's run (T4's
//! file through rannoch main's real crates into _nodes and _edges, then T8's relay) in a harness
//! that does not read the database it measures, with the switches each toy changes:
//!   BUDGET=1    round 4's pragmas on every connection (the baseline for round 5)
//!   KEEP_STORE=1 the app holds its Store, so its 1 s checkpointer runs
//!   TRACE=1     5a: trace every statement for the first 60 s of the load
//!   GAP=1       5b: after its commit, the append checkpoints and writes once before it signals
//!   HANDOFF=1   5c: the append hands the committed events to the fork of the queue
//!   KILL_AFTER=n / RESUME=1   5c: abort after n imports reach their 2; resume and check
//!   LAT_N=n     single judgments timed after the load (default 20)

#[path = "../trace5.rs"]
mod trace5;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use rannoch_core::{Append, Config, Error, Event, Rannoch, Status};
use rannoch_processor::{Action, Processor, Sig};
use rannoch_queue::{Row, Settings};
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
  WHERE json_extract(data, '$.\"$wait\".for') IS NOT NULL;
CREATE TABLE IF NOT EXISTS _gap (k INTEGER PRIMARY KEY, n INTEGER NOT NULL);";

fn on(name: &str) -> bool {
    std::env::var(name).is_ok()
}

struct Opener {
    path: PathBuf,
    tracer: Option<trace5::Tracer>,
}

impl Opener {
    fn open(&self, tag: &'static str) -> Connection {
        let mut conn = match rannoch_store::open(&self.path, &rannoch_core::migrator()).expect("open") {
            Opened::Ready(conn) => conn,
            Opened::Pending(p) => panic!("pending: {p:?}"),
        };
        if on("BUDGET") {
            conn.execute_batch("PRAGMA wal_autocheckpoint = 1280; PRAGMA journal_size_limit = 5242880; PRAGMA cache_size = -2000;")
                .expect("budget pragmas");
        }
        if let Some(t) = &self.tracer {
            t.attach(&mut conn, tag);
        }
        conn
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

/// This process's bytes written to disk so far (macOS proc_pid_rusage).
fn disk_written() -> u64 {
    let mut info: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(std::process::id() as i32, libc::RUSAGE_INFO_V2, (&raw mut info).cast::<libc::rusage_info_t>())
    };
    if rc == 0 { info.ri_diskio_byteswritten } else { 0 }
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

#[derive(Debug, Clone)]
struct Delivery {
    event: String,
    touched: HashSet<String>,
    ops: Vec<GOp>,
}

type Log = Arc<Mutex<Vec<Delivery>>>;

/// The ids an App waits for, removed when their 2 commits.
type Waiting = Arc<(Mutex<HashSet<String>>, Condvar)>;

#[derive(Debug, Clone)]
enum Wake {
    Signal(mpsc::Sender<()>),
    Hand(mpsc::Sender<Vec<Row>>),
}

impl Wake {
    fn wake(&self, rows: Vec<Row>) {
        match self {
            Wake::Signal(s) => {
                let _ = s.send(());
            }
            Wake::Hand(s) => {
                let _ = s.send(rows);
            }
        }
    }
}

fn to_row(e: &Event<Ty, Value>, data: String) -> Row {
    Row { id: e.id, source: e.source, ty: u16::from(e.ty), data, run_at: e.run_at, status: e.status }
}

#[derive(Debug)]
struct GraphAppend {
    conn: Mutex<Connection>,
    wake: Wake,
    pending: Pending,
    log: Log,
    recording: Arc<AtomicBool>,
    waiting: Waiting,
    wal: String,
    gaps: AtomicU64,
    kill_after: Option<u64>,
    import_twos: AtomicU64,
    /// 5h: the largest number of graph rows one transaction held, and how many transactions.
    tx_max: AtomicU64,
    tx_n: AtomicU64,
    /// 5i: commit after the event that takes a transaction to this many graph rows.
    cap: Option<u64>,
}

impl Append<Ty, Value> for GraphAppend {
    fn append(&self, batch: &[Event<Ty, Value>]) -> Result<(), Error> {
        let mut conn = self.conn.lock().map_err(|_| Error::Store)?;
        let written = now_ms();
        let (mut done, mut rows, mut twos) = (Vec::new(), Vec::new(), Vec::new());
        // Take every 2's rows out of the pending map now, and release it before any write, so
        // rannoch-processor never waits on this append's commits.
        let mut taken: HashMap<String, Vec<GOp>> = {
            let mut pending = self.pending.lock().map_err(|_| Error::Store)?;
            batch
                .iter()
                .filter(|e| e.status == Status::Processed)
                .filter_map(|e| e.source.map(|s| s.to_string()))
                .filter_map(|src| pending.remove(&src).map(|ops| (src, ops)))
                .collect()
        };
        // 5i: split the batch at event boundaries so no transaction passes `cap` graph rows; an
        // event's rows always commit with its 2. Without a cap the batch is one transaction.
        let mut groups: Vec<&[Event<Ty, Value>]> = Vec::new();
        let (mut from, mut in_group) = (0usize, 0u64);
        for (i, e) in batch.iter().enumerate() {
            if e.status == Status::Processed
                && let Some(src) = e.source
            {
                in_group += taken.get(&src.to_string()).map_or(0, |o| o.len() as u64);
            }
            if self.cap.is_some_and(|c| in_group >= c) {
                groups.push(&batch[from..=i]);
                from = i + 1;
                in_group = 0;
            }
        }
        if from < batch.len() {
            groups.push(&batch[from..]);
        }
        for group in groups {
            let mut in_tx = 0u64;
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
                for e in group {
                    let run_at = e.run_at.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
                    let data = e.data.to_string();
                    ev.execute(params![e.id.to_string(), e.source.map(|s| s.to_string()), u16::from(e.ty), data, run_at, e.status as i8])
                        .map_err(|_| Error::Store)?;
                    rows.push(to_row(e, data));
                    if e.status != Status::Processed {
                        continue;
                    }
                    let Some(src) = e.source.map(|s| s.to_string()) else { continue };
                    twos.push((src.clone(), e.ty.0));
                    let Some(ops) = taken.remove(&src) else { continue };
                    in_tx += ops.len() as u64;
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
            self.tx_max.fetch_max(in_tx, Ordering::SeqCst);
            self.tx_n.fetch_add(1, Ordering::SeqCst);
        }
        if self.recording.load(Ordering::SeqCst) {
            self.log.lock().map_err(|_| Error::Store)?.extend(done);
        }
        if let Some(n) = self.kill_after {
            let imports = twos.iter().filter(|(_, ty)| *ty == 50).count() as u64;
            if self.import_twos.fetch_add(imports, Ordering::SeqCst) + imports >= n {
                eprintln!("KILL: {n} imports have their 2; aborting after the commit");
                std::process::abort();
            }
        }
        {
            let (set, cv) = &*self.waiting;
            let mut set = set.lock().map_err(|_| Error::Store)?;
            let before = set.len();
            for (src, _) in &twos {
                set.remove(src);
            }
            if set.len() != before {
                cv.notify_all();
            }
        }
        if on("GAP") && std::fs::metadata(&self.wal).map_or(0, |m| m.len()) > 5 * 1024 * 1024 {
            let _: Option<i64> = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| r.get(0)).optional().map_err(|_| Error::Store)?;
            conn.execute("INSERT OR REPLACE INTO _gap (k, n) VALUES (1, ?1)", [self.gaps.fetch_add(1, Ordering::SeqCst) as i64])
                .map_err(|_| Error::Store)?;
        }
        self.wake.wake(rows);
        Ok(())
    }
}

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
                         WHERE s.kind = 'statement' AND s.id >= ?1 AND s.id < ?2 AND said.dst >= ?1 AND said.dst < ?2
                           AND s.key NOT LIKE 't8b-%'",
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

fn recompute(c: &Connection, finding: &str) -> Counts {
    let base = "WITH sup AS (
        SELECT e.src AS s FROM _edges e WHERE e.dst = ?1 AND e.name = 'supports'
          AND e.event = (SELECT max(f.event) FROM _edges f WHERE f.src = e.src AND f.name = e.name AND f.dst = e.dst)
          AND e.retracted = 0),
      sv AS (SELECT sup.s, (SELECT e.src FROM _edges e WHERE e.dst = sup.s AND e.name = 'said' AND e.retracted = 0
                            ORDER BY e.event DESC LIMIT 1) AS v FROM sup),
      pl AS (SELECT sv.s, sv.v, (SELECT n.value FROM _nodes n WHERE n.node = sv.s AND n.key = 'place'
                                 ORDER BY n.event DESC LIMIT 1) AS p FROM sv)";
    let mut st = c.prepare_cached(&format!("{base} SELECT p, count(DISTINCT v), count(*) FROM pl GROUP BY p ORDER BY p")).expect("recompute");
    let mut by_place: Vec<(String, usize, usize)> = st
        .query_map(params![finding], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as usize, r.get::<_, i64>(2)? as usize)))
        .expect("q")
        .map(|r| {
            let (p, d, n) = r.expect("row");
            (serde_json::from_str::<String>(&p).unwrap_or(p), d, n)
        })
        .collect();
    by_place.sort();
    let (distinct, total): (i64, i64) = c
        .query_row(&format!("{base} SELECT count(DISTINCT v), count(*) FROM pl"), params![finding], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("totals");
    Counts { distinct: distinct as usize, total: total as usize, by_place }
}

struct App {
    open: Arc<Opener>,
    wake: Wake,
    read: Connection,
    waiting: Waiting,
}

impl App {
    /// Events put in _events at status 0 with known ids, handed to the queue, and waited for,
    /// without reading the database, until each has its 2.
    fn hand(&self, evs: Vec<(u16, Value)>) -> Vec<String> {
        let mut c = self.open.open("hand");
        let tx = c.transaction().expect("tx");
        let (mut ids, mut rows) = (Vec::new(), Vec::new());
        for (ty, mut data) in evs {
            let id = Uuid::now_v7();
            data["event"] = json!(id.to_string());
            let d = data.to_string();
            tx.execute("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, NULL, ?2, ?3, NULL, 0)", params![id.to_string(), ty, d])
                .expect("event");
            rows.push(Row { id, source: None, ty, data: d, run_at: None, status: Status::Added });
            ids.push(id.to_string());
        }
        {
            let (set, _) = &*self.waiting;
            set.lock().expect("waiting").extend(ids.iter().cloned());
        }
        tx.commit().expect("commit");
        self.wake.wake(rows);
        let (set, cv) = &*self.waiting;
        let mut guard = set.lock().expect("waiting");
        let start = Instant::now();
        while ids.iter().any(|i| guard.contains(i)) {
            let (g, _) = cv.wait_timeout(guard, Duration::from_secs(1)).expect("wait");
            guard = g;
            assert!(start.elapsed() < Duration::from_secs(900), "events did not reach their 2");
        }
        ids
    }
}

fn pct(v: &mut [Duration], p: f64) -> Duration {
    v.sort();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| "data".into()));
    let source_path = std::env::args().nth(2).expect("the T8 data file");
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("round5.db");
    let resume = on("RESUME");
    if !resume {
        for ext in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{ext}", path.display()));
        }
    }
    let tracer = on("TRACE").then(|| trace5::Tracer::new(Duration::from_secs(60)));
    let open = Arc::new(Opener { path: path.clone(), tracer: tracer.clone() });
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    Rannoch::<Ty, Value>::open(Config { store: Some(store.clone()), ..Config::default() }).expect("migrate").shutdown();
    if on("TIMEKEYS") {
        // 5f: the graph tables keyed by the event first (UUIDv7, time order), lookups as indexes.
        open.open("setup")
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS _nodes (event TEXT NOT NULL, node TEXT NOT NULL, key TEXT NOT NULL, value TEXT,
                   written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (event, node, key)) WITHOUT ROWID;
                 CREATE INDEX IF NOT EXISTS _nodes_at ON _nodes(node, key, event);
                 CREATE TABLE IF NOT EXISTS _edges (event TEXT NOT NULL, src TEXT NOT NULL, name TEXT NOT NULL, dst TEXT NOT NULL,
                   props TEXT, written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0,
                   PRIMARY KEY (event, src, name, dst)) WITHOUT ROWID;
                 CREATE INDEX IF NOT EXISTS _edges_out ON _edges(src, name, dst, event);",
            )
            .expect("time-keyed tables");
    }
    open.open("setup").execute_batch(GRAPH).expect("graph tables");

    let open_source = || {
        Connection::open_with_flags(format!("file:{source_path}?immutable=1"), OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI).expect("source")
    };
    let src = open_source();
    let pending = Pending::default();
    let log = Log::default();
    let recording = Arc::new(AtomicBool::new(false));
    let waiting: Waiting = Arc::default();
    let sig = Sig { action: "graph".into(), ..Sig::default() };
    let action = GraphAction { sig, pending: pending.clone(), source: Mutex::new(open_source()) };
    let flows = r#"[
      {"name": "import", "ty": 50, "takes": {}, "steps": [{"do": "graph"}]},
      {"name": "judge", "ty": 51, "takes": {}, "steps": [{"do": "graph"}]},
      {"name": "write", "ty": 52, "takes": {}, "steps": [{"do": "graph"}]}]"#;
    let processor: Processor<Ty, Value> = Processor::new(flows, vec![Arc::new(action)], open.open("processor")).expect("processor");
    let handoff = on("HANDOFF");
    let (sig_tx, sig_rx) = mpsc::channel::<()>();
    let (hand_tx, hand_rx) = mpsc::channel::<Vec<Row>>();
    let wake = if handoff { Wake::Hand(hand_tx) } else { Wake::Signal(sig_tx) };
    let append = Arc::new(GraphAppend {
        conn: Mutex::new({
            let c = open.open("append");
            // 5j: the append's commits without a flush each; flushed at checkpoints.
            if on("APPEND_NORMAL") {
                c.execute_batch("PRAGMA synchronous = NORMAL").expect("normal");
            }
            c
        }),
        wake: wake.clone(),
        pending,
        log: log.clone(),
        recording: recording.clone(),
        waiting: waiting.clone(),
        wal: format!("{}-wal", path.display()),
        gaps: AtomicU64::new(0),
        kill_after: std::env::var("KILL_AFTER").ok().and_then(|v| v.parse().ok()),
        import_twos: AtomicU64::new(0),
        tx_max: AtomicU64::new(0),
        tx_n: AtomicU64::new(0),
        cap: std::env::var("CAP").ok().and_then(|v| v.parse().ok()),
    });
    let _kept = on("KEEP_STORE").then(|| store.clone());
    let rannoch = Rannoch::open(Config { store: Some(store), append: Some(append.clone() as Arc<dyn Append<Ty, Value>>), ..Config::default() }).expect("rannoch");
    let settings = if resume {
        Settings { window: Duration::from_secs(1), hunt: Duration::from_secs(2), ..Settings::new(Duration::from_millis(100)) }
    } else {
        Settings::new(Duration::from_millis(100))
    };
    let processor = Arc::new(processor);
    enum Q {
        Real(rannoch_queue::Queue),
        Fork(queue_handoff::Queue),
    }
    let queue = if handoff {
        Q::Fork(queue_handoff::Queue::start(open.open("queue"), hand_rx, rannoch.sender(), processor, settings))
    } else {
        Q::Real(rannoch_queue::Queue::start(open.open("queue"), sig_rx, rannoch.sender(), processor, settings))
    };
    let app = App { open: open.clone(), wake, read: open.open("app"), waiting };
    let count = |sql: &str| -> i64 { app.read.query_row(sql, [], |r| r.get(0)).expect("count") };
    let mode = ["BUDGET", "KEEP_STORE", "TRACE", "GAP", "HANDOFF", "RESUME", "TIMEKEYS", "CAP", "APPEND_NORMAL"].iter().filter(|m| on(m)).copied().collect::<Vec<_>>().join("+");

    if resume {
        // 5c: after the abort, the queue's start-up read and the hunt finish what was left.
        let t = Instant::now();
        let all = "SELECT count(*) FROM _events o WHERE o.ty = 50 AND o.status = 0 AND o.source IS NULL";
        let finished = "SELECT count(*) FROM _events o WHERE o.ty = 50 AND o.status = 0 AND o.source IS NULL AND (
            EXISTS (SELECT 1 FROM _events s WHERE s.source = o.id AND +s.status = 2)
            OR EXISTS (SELECT 1 FROM _events r JOIN _events s ON s.source = r.id AND +s.status = 2 WHERE r.source = o.id AND +r.status = 0))";
        let n = count(all);
        loop {
            if count(finished) == n {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(900), "resume did not finish");
            std::thread::sleep(Duration::from_millis(500));
        }
        let twice = count(
            "SELECT count(*) FROM (SELECT o.id,
                (SELECT count(*) FROM _events s WHERE s.source = o.id AND +s.status = 2)
              + (SELECT count(*) FROM _events r JOIN _events s ON s.source = r.id AND +s.status = 2 WHERE r.source = o.id AND +r.status = 0) AS twos
              FROM _events o WHERE o.ty = 50 AND o.status = 0 AND o.source IS NULL) WHERE twos != 1",
        );
        println!(
            "P5c resume ({mode}): {n} imports, all with their 2 after {:?}; imports whose tree has other than one 2: {twice}; graph rows: {} node, {} edge",
            t.elapsed(),
            count("SELECT count(*) FROM _nodes"),
            count("SELECT count(*) FROM _edges")
        );
    } else {
        let w0 = disk_written();
        let (lo, hi): (i64, i64) = src.query_row("SELECT min(id), max(id) FROM node WHERE kind = 'statement'", [], |r| Ok((r.get(0)?, r.get(1)?))).expect("range");
        // 5f: IMPORTS splits the same statements into that many import events (default 1,000).
        let k: i64 = std::env::var("IMPORTS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
        let step = (hi - lo) / k + 1;
        let imports: Vec<(u16, Value)> = (0..k).map(|i| (50, json!({"do": "import", "from": lo + i * step, "to": lo + (i + 1) * step}))).collect();
        let t = Instant::now();
        app.hand(imports);
        let load = t.elapsed();
        let (nr, er) = (count("SELECT count(*) FROM _nodes"), count("SELECT count(*) FROM _edges"));
        let written = disk_written() - w0;
        println!(
            "load ({mode}): {k} import events to their 2 in {load:?}: {nr} node rows, {er} edge rows, {:.0} graph rows/s; disk written {} MB, {:.0} bytes per graph row; gap writes {}; P5h append transactions {}, largest {} graph rows",
            (nr + er) as f64 / load.as_secs_f64(),
            written / 1_000_000,
            written as f64 / (nr + er) as f64,
            append.gaps.load(Ordering::SeqCst),
            append.tx_n.load(Ordering::SeqCst),
            append.tx_max.load(Ordering::SeqCst)
        );
        if let Some(t) = &tracer {
            println!("P5a {}", t.report(&mode));
        }

        let finding_id: i64 = src
            .query_row("SELECT l.dst FROM link l JOIN node s ON s.id = l.src WHERE l.kind = 'supports' AND s.key NOT LIKE 't8b-%' GROUP BY l.dst ORDER BY count(*) DESC LIMIT 1", [], |r| r.get(0))
            .expect("finding");
        let finding = format!("f{finding_id}");
        let mut relay = Relay::seed(&app.read, &finding);
        recording.store(true, Ordering::SeqCst);
        let seed = relay.crowd.counts();
        let mut ok = seed == recompute(&app.read, &finding);
        let mut by_voice: HashMap<String, Vec<String>> = HashMap::new();
        for (s, (v, _)) in &relay.crowd.stmt {
            by_voice.entry(v.clone()).or_default().push(s.clone());
        }
        let mut vs: Vec<_> = by_voice.iter().collect();
        vs.sort();
        let a1 = vs.iter().find(|(_, s)| s.len() >= 3).expect("a voice with three").1[0].clone();
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
        let mut steps: Vec<(&str, bool)> = Vec::new();
        let mut step = |name: &'static str, relay: &mut Relay, expect: &dyn Fn(&Counts, &Counts) -> bool, before: &Counts, steps: &mut Vec<(&str, bool)>| -> Counts {
            relay.pump(&log);
            let now = relay.crowd.counts();
            let good = expect(before, &now) && now == recompute(&app.read, &finding);
            steps.push((name, good));
            now
        };
        let mut evs = noise(0);
        evs.push(judge(&a1, &finding, "not_supports"));
        app.hand(evs);
        let s1 = step("R3 A1", &mut relay, &|b, n| n.distinct == b.distinct && n.total + 1 == b.total, &seed, &mut steps);
        let mut evs = noise(1);
        evs.push(judge(&b, &finding, "not_supports"));
        app.hand(evs);
        let s2 = step("R3 B", &mut relay, &|b, n| n.distinct + 1 == b.distinct && n.total + 1 == b.total, &s1, &mut steps);
        app.hand(vec![judge(&a1, &finding, "not_supports")]);
        let s3 = step("R7", &mut relay, &|b, n| b == n, &s2, &mut steps);
        let mut evs = noise(2);
        evs.push(judge(&b, &finding, "supports"));
        app.hand(evs);
        let s4 = step("R6 B", &mut relay, &|_, n| n.distinct == s1.distinct && n.total == s1.total, &s3, &mut steps);
        app.hand(vec![judge(&a1, &finding, "supports")]);
        let s5 = step("R6 A1", &mut relay, &|_, n| *n == seed, &s4, &mut steps);
        app.hand(noise(3));
        let _ = step("noise", &mut relay, &|b, n| b == n, &s5, &mut steps);
        ok &= steps.iter().all(|s| s.1) && relay.delivered == 5;
        println!("relay ({mode}): steps {:?}; delivered {} of {}; all held and control equal: {ok}", steps, relay.delivered, relay.seen);

        let n: usize = std::env::var("LAT_N").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
        let mut lat = Vec::new();
        for i in 0..n {
            let t = Instant::now();
            app.hand(vec![judge(&a1, &finding, if i % 2 == 0 { "not_supports" } else { "supports" })]);
            lat.push(t.elapsed());
        }
        if n % 2 == 1 {
            app.hand(vec![judge(&a1, &finding, "supports")]);
        }
        println!("latency ({mode}): {n} single judgments, send to 2: p50 {:?}, p99 {:?}, max {:?}", pct(&mut lat, 0.5), pct(&mut lat, 0.99), pct(&mut lat, 1.0));
    }

    let t = Instant::now();
    // Every hand-off and signal sender must go, or the queue never sees its channel close.
    drop(app);
    drop(append);
    rannoch.shutdown();
    match queue {
        Q::Real(q) => q.join(),
        Q::Fork(q) => q.join(),
    }
    let shut = t.elapsed();
    println!("shutdown ({mode}): {shut:?}{}", if shut > Duration::from_secs(10) { " FAIL: over 10 s" } else { "" });
}
