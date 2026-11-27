//! graph toy round 4c: what keeps the WAL from restarting. Predictions in predictions-budget.txt.
//! Round 5a adds TRACE=1, the same statement tracer as round5, for the control.

#[path = "../trace5.rs"]
mod trace5;

use rusqlite::{Connection, params};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

const PRAGMAS: &str = "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA fullfsync = ON;
PRAGMA wal_autocheckpoint = 1280; PRAGMA journal_size_limit = 5242880; PRAGMA cache_size = -2000;";

fn open(path: &str) -> Connection {
    let c = Connection::open(path).expect("open");
    c.execute_batch(PRAGMAS).expect("pragmas");
    c
}

fn main() {
    let mode = std::env::args().nth(1).expect("alone | same | other20 | other1");
    let dir = std::env::args().nth(2).unwrap_or_else(|| "data".into());
    std::fs::create_dir_all(&dir).expect("dir");
    let path = format!("{dir}/walbound-{mode}.db");
    for ext in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{ext}"));
    }
    let tracer = std::env::var("TRACE").is_ok().then(|| trace5::Tracer::new(Duration::from_secs(600)));
    let mut w = open(&path);
    if let Some(t) = &tracer {
        t.attach(&mut w, "writer");
    }
    w.execute_batch(
        "CREATE TABLE _nodes (node TEXT NOT NULL, key TEXT NOT NULL, value TEXT, event TEXT NOT NULL, written INTEGER NOT NULL,
           retracted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (node, key, event)) WITHOUT ROWID;
         CREATE TABLE _edges (src TEXT NOT NULL, name TEXT NOT NULL, dst TEXT NOT NULL, props TEXT, event TEXT NOT NULL,
           written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (src, name, dst, event)) WITHOUT ROWID;
         CREATE INDEX _edges_in ON _edges(dst, name, src, event);",
    )
    .expect("schema");
    let (stop, max) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicU64::new(0)));
    {
        let (stop, max, wal) = (stop.clone(), max.clone(), format!("{path}-wal"));
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                max.fetch_max(std::fs::metadata(&wal).map_or(0, |m| m.len()), Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
            }
        });
    }
    let reads = Arc::new(AtomicU64::new(0));
    let every = match mode.as_str() {
        "other20" => Some(20),
        "other1" => Some(1),
        _ => None,
    };
    if let Some(ms) = every {
        let (stop, reads, p, tr) = (stop.clone(), reads.clone(), path.clone(), tracer.clone());
        std::thread::spawn(move || {
            let mut r = open(&p);
            if let Some(t) = &tr {
                t.attach(&mut r, "reader");
            }
            while !stop.load(Ordering::SeqCst) {
                let _: i64 = r.query_row("SELECT count(*) FROM (SELECT 1 FROM _edges ORDER BY dst DESC LIMIT 100)", [], |x| x.get(0)).unwrap_or(0);
                reads.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(ms));
            }
        });
    }
    let t = Instant::now();
    let mut seq = 0u64;
    for _ in 0..300 {
        let ev = Uuid::now_v7().to_string();
        let tx = w.transaction().expect("tx");
        {
            let mut n = tx.prepare_cached("INSERT INTO _nodes VALUES (?1, 'place', '\"steam\"', ?2, 0, 0)").expect("n");
            let mut e = tx.prepare_cached("INSERT INTO _edges VALUES (?1, 'said', ?2, '{}', ?3, 0, 0)").expect("e");
            for _ in 0..1000 {
                seq += 1;
                n.execute(params![format!("s{seq}"), ev]).expect("node");
                e.execute(params![format!("v{}", seq * 7919 % 100_003), format!("s{seq}"), ev]).expect("edge");
            }
        }
        tx.commit().expect("commit");
        if mode == "same" {
            let _: i64 = w.query_row("SELECT count(*) FROM (SELECT 1 FROM _edges ORDER BY dst DESC LIMIT 100)", [], |x| x.get(0)).unwrap_or(0);
            reads.fetch_add(1, Ordering::SeqCst);
        }
    }
    let el = t.elapsed();
    stop.store(true, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(50));
    println!(
        "{mode}: 300 transactions of 2,000 rows in {el:?}; reads {}; WAL file largest {:.1} MiB",
        reads.load(Ordering::SeqCst),
        max.load(Ordering::SeqCst) as f64 / 1_048_576.0
    );
    if let Some(t) = &tracer {
        println!("P5a control {}", t.report(&mode));
    }
}
