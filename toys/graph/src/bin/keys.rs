//! graph toy round 5e: transaction size times key order. Predictions in predictions-round5.txt.

use rusqlite::{Connection, params};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

fn disk_written() -> u64 {
    let mut info: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::proc_pid_rusage(std::process::id() as i32, libc::RUSAGE_INFO_V2, (&raw mut info).cast::<libc::rusage_info_t>()) };
    if rc == 0 { info.ri_diskio_byteswritten } else { 0 }
}

fn main() {
    let mode = std::env::args().nth(1).expect("seq-big | rand-big | rand-small | time-big");
    let dir = std::env::args().nth(2).unwrap_or_else(|| "data".into());
    std::fs::create_dir_all(&dir).expect("dir");
    let path = format!("{dir}/keys-{mode}.db");
    for ext in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{ext}"));
    }
    let mut w = Connection::open(&path).expect("open");
    w.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA fullfsync = ON;
        PRAGMA wal_autocheckpoint = 1280; PRAGMA journal_size_limit = 5242880; PRAGMA cache_size = -2000;").expect("pragmas");
    let time_first = mode == "time-big";
    if time_first {
        w.execute_batch(
            "CREATE TABLE _nodes (event TEXT NOT NULL, node TEXT NOT NULL, key TEXT NOT NULL, value TEXT, written INTEGER NOT NULL,
               retracted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (event, node, key)) WITHOUT ROWID;
             CREATE INDEX _nodes_at ON _nodes(node, key, event);
             CREATE TABLE _edges (event TEXT NOT NULL, src TEXT NOT NULL, name TEXT NOT NULL, dst TEXT NOT NULL, props TEXT,
               written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (event, src, name, dst)) WITHOUT ROWID;
             CREATE INDEX _edges_out ON _edges(src, name, dst, event);
             CREATE INDEX _edges_in ON _edges(dst, name, src, event);",
        )
        .expect("schema");
    } else {
        w.execute_batch(
            "CREATE TABLE _nodes (node TEXT NOT NULL, key TEXT NOT NULL, value TEXT, event TEXT NOT NULL, written INTEGER NOT NULL,
               retracted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (node, key, event)) WITHOUT ROWID;
             CREATE TABLE _edges (src TEXT NOT NULL, name TEXT NOT NULL, dst TEXT NOT NULL, props TEXT, event TEXT NOT NULL,
               written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (src, name, dst, event)) WITHOUT ROWID;
             CREATE INDEX _edges_in ON _edges(dst, name, src, event);",
        )
        .expect("schema");
    }
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
    let per = if mode == "rand-small" { 335 } else { 3_350 };
    let txns = if mode == "rand-small" { 3_000 } else { 300 };
    let hash = |x: u64| x.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29) % 10_000_000;
    let (w0, t) = (disk_written(), Instant::now());
    let mut seq = 0u64;
    for _ in 0..txns {
        let ev = Uuid::now_v7().to_string();
        let tx = w.transaction().expect("tx");
        {
            let (nsql, esql) = if time_first {
                ("INSERT INTO _nodes (event, node, key, value, written) VALUES (?2, ?1, 'place', '\"steam\"', 0)",
                 "INSERT INTO _edges (event, src, name, dst, props, written) VALUES (?3, ?1, 'said', ?2, '{}', 0)")
            } else {
                ("INSERT INTO _nodes (node, key, value, event, written) VALUES (?1, 'place', '\"steam\"', ?2, 0)",
                 "INSERT INTO _edges (src, name, dst, props, event, written) VALUES (?1, 'said', ?2, '{}', ?3, 0)")
            };
            let mut n = tx.prepare_cached(nsql).expect("n");
            let mut e = tx.prepare_cached(esql).expect("e");
            for _ in 0..per {
                seq += 1;
                let node = if mode == "seq-big" { format!("s{seq:012}") } else { format!("s{}", hash(seq)) };
                n.execute(params![node, ev]).expect("node");
                e.execute(params![format!("v{}", hash(seq ^ 0xABCD) % 100_003), node, ev]).expect("edge");
            }
        }
        tx.commit().expect("commit");
    }
    let (el, written) = (t.elapsed(), disk_written() - w0);
    stop.store(true, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(50));
    let rows = (txns * per * 2) as f64;
    println!(
        "{mode}: {txns} transactions of {} rows in {el:?}; WAL file largest {:.1} MiB; disk written {} MB, {:.0} bytes per row",
        per * 2,
        max.load(Ordering::SeqCst) as f64 / 1_048_576.0,
        written / 1_000_000,
        written as f64 / rows
    );
}
