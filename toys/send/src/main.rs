//! Toy for research 01a1e924, round 4: from send. Opens rannoch from main (rannoch-core,
//! rannoch-events by path), sends through Sender::send, the broker, the inbox heap and the
//! inbox thread's hand-over, and times each event from send to commit.
//! main as it is uses rannoch_events::Events (plain INSERT). FR-RANNOCH-023's shape is a copy
//! of that append with INSERT OR IGNORE, over a unique index on the rannoch_hash expression.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use rannoch_core::{Append, Config, Error, Event, Rannoch, Status};
use rannoch_events::Events;
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, params};
use serde::Serialize;
use uuid::Uuid;

const TABLE: &str = "CREATE TABLE \"_events\" (\"id\" TEXT, \"source\" TEXT, \"ty\" INTEGER NOT NULL, \"data\" TEXT NOT NULL, \"run_at\" TEXT, \"status\" INTEGER NOT NULL CHECK (status IN (-1, 0, 1, 2)), PRIMARY KEY (\"id\")); CREATE INDEX _events_source ON _events (source);";
const ONCE_INDEX: &str = "CREATE UNIQUE INDEX _events_once ON _events (rannoch_hash(quote(ty), quote(source), quote(status), quote(data)));";
const APPEND_ONCE: &str = "INSERT OR IGNORE INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

static START: OnceLock<Instant> = OnceLock::new();
fn now_ns() -> u64 {
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Ty(u16);
impl From<Ty> for u16 {
    fn from(t: Ty) -> u16 {
        t.0
    }
}

#[derive(Debug, Clone, Serialize)]
struct Payload {
    n: u64,
    page: String,
    sent_ns: u64,
}

fn register(c: &Connection) {
    c.create_scalar_function("rannoch_hash", -1, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let mut h = blake3::Hasher::new();
        for i in 0..ctx.len() {
            let s: String = ctx.get(i)?;
            h.update(&(s.len() as u64).to_be_bytes());
            h.update(s.as_bytes());
        }
        Ok(h.finalize().as_bytes()[..16].to_vec())
    })
    .expect("function");
}

/// FR-RANNOCH-023's append: rannoch_events::append with INSERT OR IGNORE.
#[derive(Debug)]
struct Once {
    conn: Mutex<Connection>,
}
impl Append<Ty, Payload> for Once {
    fn append(&self, batch: &[Event<Ty, Payload>]) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        let tx = conn.transaction().map_err(|_| Error::Store)?;
        {
            let mut ins = tx.prepare_cached(APPEND_ONCE).map_err(|_| Error::Store)?;
            for e in batch {
                let data = serde_json::to_string(&e.data).map_err(|_| Error::Store)?;
                ins.execute(params![
                    e.id.to_string(),
                    e.source.map(|s| s.to_string()),
                    u16::from(e.ty),
                    data,
                    e.run_at.map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true)),
                    e.status as i8
                ])
                .map_err(|_| Error::Store)?;
            }
        }
        tx.commit().map_err(|_| Error::Store)
    }
}

/// Wraps an append: after it commits, records each event's send-to-commit time.
#[derive(Debug)]
struct Timed {
    inner: Arc<dyn Append<Ty, Payload>>,
    latencies: Mutex<Vec<u64>>,
    failures: AtomicU64,
}
impl Append<Ty, Payload> for Timed {
    fn append(&self, batch: &[Event<Ty, Payload>]) -> Result<(), Error> {
        let r = self.inner.append(batch);
        let at = now_ns();
        if r.is_ok() {
            let mut l = self.latencies.lock().unwrap_or_else(PoisonError::into_inner);
            l.extend(batch.iter().map(|e| at.saturating_sub(e.data.sent_ns)));
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        r
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    MainAsIs,
    MainWal,
    Fr023Wal,
    MainWithUniqueIndex,
}
impl Shape {
    fn label(self) -> &'static str {
        match self {
            Shape::MainAsIs => "main as it is (Events, plain INSERT, rollback journal)",
            Shape::MainWal => "main's Events with WAL",
            Shape::Fr023Wal => "FR-023 shape (unique rannoch_hash index, INSERT OR IGNORE) with WAL",
            Shape::MainWithUniqueIndex => "main's Events (plain INSERT) with the unique index, WAL",
        }
    }
}

fn conn(path: &str, shape: Shape) -> Connection {
    let c = Connection::open(path).expect("open");
    c.pragma_update(None, "foreign_keys", true).expect("fk");
    if shape != Shape::MainAsIs {
        c.execute_batch("PRAGMA journal_mode = WAL;").expect("wal");
    }
    if let Ok(kib) = std::env::var("TOY_CACHE_KIB") {
        c.execute_batch(&format!("PRAGMA cache_size = -{kib};")).expect("cache");
    }
    if std::env::var("TOY_NOCKPT").is_ok() {
        c.execute_batch("PRAGMA wal_autocheckpoint = 0;").expect("ckpt");
    }
    if std::env::var("TOY_FULLFSYNC").is_ok() {
        c.execute_batch("PRAGMA fullfsync = ON; PRAGMA checkpoint_fullfsync = ON;").expect("fullfsync");
    }
    register(&c);
    c
}

fn payload(n: u64) -> Payload {
    Payload { n, page: format!("https://example.com/p/{n}"), sent_ns: now_ns() }
}

fn event(n: u64) -> Event<Ty, Payload> {
    Event { id: Uuid::nil(), source: None, ty: Ty((n % 8) as u16), data: payload(n), run_at: None, status: Status::Added }
}

/// A fresh store for `shape` with `history` root events already in it.
fn store(dir: &str, shape: Shape, history: u64) -> String {
    let path = format!("{dir}/send.db");
    for x in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{x}"));
    }
    let mut c = conn(&path, shape);
    c.execute_batch(TABLE).expect("table");
    if matches!(shape, Shape::Fr023Wal | Shape::MainWithUniqueIndex) {
        c.execute_batch(ONCE_INDEX).expect("index");
    }
    let mut n = 0;
    while n < history {
        let tx = c.transaction().expect("tx");
        {
            let mut ins = tx.prepare_cached("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, NULL, ?2, ?3, NULL, 0)").expect("prep");
            for _ in 0..10_000.min(history - n) {
                n += 1;
                let d = format!("{{\"n\":{},\"page\":\"https://example.com/h/{n}\",\"sent_ns\":0}}", n + 1_000_000_000);
                ins.execute(params![Uuid::now_v7().to_string(), (n % 8) as i64, d]).expect("ins");
            }
        }
        tx.commit().expect("commit");
    }
    path
}

fn open(path: &str, shape: Shape) -> (Rannoch<Ty, Payload>, Arc<Timed>) {
    let inner: Arc<dyn Append<Ty, Payload>> = match shape {
        Shape::Fr023Wal => Arc::new(Once { conn: Mutex::new(conn(path, shape)) }),
        _ => {
            let (queue, signals) = mpsc::channel();
            thread::spawn(move || for () in signals {});
            Arc::new(Events::new(conn(path, shape), queue))
        }
    };
    let timed = Arc::new(Timed { inner, latencies: Mutex::new(Vec::new()), failures: AtomicU64::new(0) });
    let config = Config { append: Some(timed.clone() as Arc<dyn Append<Ty, Payload>>), ..Config::default() };
    (Rannoch::open(config).expect("rannoch open"), timed)
}

fn count(path: &str, shape: Shape) -> i64 {
    conn(path, shape).query_row("SELECT count(*) FROM _events", [], |r| r.get(0)).expect("count")
}

fn pct(v: &mut [u64], p: f64) -> f64 {
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p) as usize] as f64 / 1e6
}

/// P1: a steady 200 a second for 10 seconds; send-to-commit per event.
fn steady(dir: &str, shape: Shape, history: u64) {
    let path = store(dir, shape, history);
    let before = count(&path, shape);
    let (rannoch, timed) = open(&path, shape);
    let ck = checkpointer(&path);
    let tx = rannoch.sender();
    let t0 = Instant::now();
    for n in 0..2_000u64 {
        let due = t0 + Duration::from_millis(n * 5);
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            thread::sleep(wait);
        }
        tx.send(event(n)).expect("send");
    }
    rannoch.shutdown();
    if let Some((stop, h)) = ck { stop.store(true, Ordering::Relaxed); let _ = h.join(); }
    let mut l = timed.latencies.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let kept = count(&path, shape) - before;
    println!(
        "P1 {} at history {history}: 2,000 events at 200/s, kept {kept}, send to commit p50 {:.1} ms, p99 {:.1} ms, max {:.1} ms",
        shape.label(), pct(&mut l, 0.5), pct(&mut l, 0.99), pct(&mut l, 1.0)
    );
}

/// P2 and P4: flat out from one producer.
fn flat_out(dir: &str, shape: Shape, history: u64) {
    let path = store(dir, shape, history);
    let before = count(&path, shape);
    let (rannoch, timed) = open(&path, shape);
    let ck = checkpointer(&path);
    let tx = rannoch.sender();
    let n = 100_000u64;
    let events: Vec<Event<Ty, Payload>> = (0..n).map(event).collect();
    let t0 = Instant::now();
    for e in events {
        tx.send(e).expect("send");
    }
    let sending = t0.elapsed();
    rannoch.shutdown();
    let total = t0.elapsed();
    if let Some((stop, h)) = ck { stop.store(true, Ordering::Relaxed); let _ = h.join(); }
    let kept = count(&path, shape) - before;
    let mut l = timed.latencies.lock().unwrap_or_else(PoisonError::into_inner).clone();
    println!(
        "P2/P4 {} at history {history}: {n} sent in {:.3}s ({:.2} us a send), all kept ({kept}) in {:.3}s = {:.0}/s end to end; send to commit p50 {:.1} ms, p99 {:.1} ms",
        shape.label(), sending.as_secs_f64(), sending.as_secs_f64() * 1e6 / n as f64, total.as_secs_f64(), n as f64 / total.as_secs_f64(), pct(&mut l, 0.5), pct(&mut l, 0.99)
    );
}

/// P3: the same event sent twice, in separate intervals, then a different one.
fn repeat(dir: &str, shape: Shape) {
    let path = store(dir, shape, 0);
    let (rannoch, timed) = open(&path, shape);
    let tx = rannoch.sender();
    let fixed = Payload { n: 1, page: "https://example.com/same".into(), sent_ns: 0 };
    let mk = |p: Payload| Event { id: Uuid::nil(), source: None, ty: Ty(1), data: p, run_at: None, status: Status::Added };
    tx.send(mk(fixed.clone())).expect("send");
    thread::sleep(Duration::from_millis(100));
    tx.send(mk(fixed)).expect("send");
    tx.send(mk(Payload { n: 2, page: "https://example.com/other".into(), sent_ns: 0 })).expect("send");
    thread::sleep(Duration::from_millis(300));
    let failures = timed.failures.load(Ordering::Relaxed);
    let rows = count(&path, shape);
    println!("P3 {}: sent A, then A again with B: rows {rows} (want 2: A once and B), failed hand-overs in 300 ms: {failures}", shape.label());
    if shape == Shape::MainWithUniqueIndex {
        // shutdown would hand the refused batch over once more and return; it does not hang
        rannoch.shutdown();
    } else {
        rannoch.shutdown();
    }
}

/// With wal_autocheckpoint 0, a separate connection checkpoints once a second, off the append.
fn checkpointer(path: &str) -> Option<(Arc<std::sync::atomic::AtomicBool>, thread::JoinHandle<()>)> {
    std::env::var("TOY_NOCKPT").ok()?;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (flag, path) = (stop.clone(), path.to_owned());
    Some((stop, thread::spawn(move || {
        let c = Connection::open(&path).expect("open");
        while !flag.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
            let _ = c.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
        }
    })))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(2).cloned().unwrap_or_else(|| ".".into());
    START.get_or_init(Instant::now);
    let shapes = [Shape::MainAsIs, Shape::MainWal, Shape::Fr023Wal];
    match args.get(1).map(String::as_str) {
        Some("p1") => {
            for h in [0, 1_000_000] {
                for s in shapes {
                    steady(&dir, s, h);
                }
            }
        }
        Some("p2") => {
            for h in [0, 1_000_000] {
                for s in shapes {
                    flat_out(&dir, s, h);
                }
            }
        }
        Some("p5") => {
            steady(&dir, Shape::Fr023Wal, 1_000_000);
            flat_out(&dir, Shape::Fr023Wal, 1_000_000);
        }
        Some("p3") => {
            repeat(&dir, Shape::Fr023Wal);
            repeat(&dir, Shape::MainWithUniqueIndex);
        }
        _ => eprintln!("usage: send-toy p1|p2|p3 <dir>"),
    }
}
