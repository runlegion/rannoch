//! Toy for research 01a1e924, round 2: a repeat is kept once by a hash computed inside SQLite
//! (smugglr's shape: BLAKE3 over length-prefixed quote() text, 16 bytes) and never stored;
//! a trim of chosen ids; the waiting and due reads at realistic history.
//! The table and the APPEND, WAITING and DUE SQL are copied from rannoch-events at main 6f63f17.

use std::collections::HashSet;
use std::time::Instant;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, params};
use uuid::Uuid;

const TABLE: &str = "CREATE TABLE \"_events\" (\"id\" TEXT, \"source\" TEXT, \"ty\" INTEGER NOT NULL, \"data\" TEXT NOT NULL, \"run_at\" TEXT, \"status\" INTEGER NOT NULL CHECK (status IN (-1, 0, 1, 2)), PRIMARY KEY (\"id\"))";
const APPEND: &str =
    "INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
/// APPEND, kept only where no row with that source, ty and status hashes the same.
const APPEND_ONCE: &str = "INSERT INTO _events (id, source, ty, data, run_at, status)
SELECT ?1, ?2, ?3, ?4, ?5, ?6
WHERE NOT EXISTS (SELECT 1 FROM _events e
  WHERE e.source IS ?2 AND e.ty = ?3 AND e.status = ?6
  AND rannoch_hash(quote(e.ty), quote(e.source), quote(e.status), quote(e.data))
    = rannoch_hash(quote(?3), quote(?2), quote(?6), quote(?4)))";
const WAITING: &str = "SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM _events e WHERE e.ty = ?1 AND e.status = 0
AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0)
ORDER BY e.id";
const DUE: &str = "SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM _events e WHERE e.run_at <= ?1 AND e.status = 0
AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0)
ORDER BY e.id";

#[derive(Clone)]
struct Ev {
    id: Uuid,
    source: Option<Uuid>,
    ty: u16,
    data: String,
    run_at: Option<DateTime<Utc>>,
    status: i8,
}

fn ev(source: Option<Uuid>, ty: u16, data: String, run_at: Option<DateTime<Utc>>, status: i8) -> Ev {
    Ev { id: Uuid::now_v7(), source, ty, data, run_at, status }
}

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn data(i: usize) -> String {
    format!("{{\"n\":{i},\"page\":\"https://example.com/p/{i}\",\"seen\":true}}")
}

/// smugglr's row hash: each argument is a column's quote() text, length-prefixed (u64 big-endian),
/// BLAKE3 over all of it, first 16 bytes.
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

fn open(path: &str, index: bool) -> Connection {
    let _ = std::fs::remove_file(path);
    let c = Connection::open(path).expect("open");
    c.pragma_update(None, "foreign_keys", true).expect("pragma");
    c.execute_batch(TABLE).expect("table");
    if index {
        c.execute_batch("CREATE INDEX _events_source ON _events (source); CREATE INDEX _events_id_source ON _events (id, source);")
            .expect("index");
    }
    register(&c);
    c
}

/// Appends a batch in one transaction; returns how many rows went in.
fn append(c: &mut Connection, batch: &[Ev], once: bool) -> usize {
    let tx = c.transaction().expect("tx");
    let mut n = 0;
    {
        let mut ins = tx.prepare(if once { APPEND_ONCE } else { APPEND }).expect("prep");
        for e in batch {
            n += ins
                .execute(params![e.id.to_string(), e.source.map(|s| s.to_string()), e.ty, e.data, e.run_at.map(ts), e.status])
                .expect("ins");
        }
    }
    tx.commit().expect("commit");
    n
}

fn count(c: &Connection) -> i64 {
    c.query_row("SELECT count(*) FROM _events", [], |r| r.get(0)).expect("count")
}

fn ids(c: &Connection, sql: &str, p: &dyn rusqlite::ToSql) -> Vec<String> {
    let mut s = c.prepare(sql).expect("prep");
    s.query_map([p], |r| r.get::<_, String>(0)).expect("q").map(|r| r.expect("row")).collect()
}

fn median_ms(mut v: Vec<f64>) -> (f64, f64) {
    v.sort_by(f64::total_cmp);
    (v[v.len() / 2], v[v.len() - 1])
}

fn p1(dir: &str) {
    let mut c = open(&format!("{dir}/p1.db"), true);
    let a = ev(None, 1, data(1), None, 0);
    let mut again = a.clone();
    again.id = Uuid::now_v7();
    append(&mut c, &[a.clone()], true);
    append(&mut c, &[again], true);
    let repeat = count(&c);
    append(&mut c, &[ev(None, 1, data(2), None, 0)], true);
    let other = count(&c);
    // a failed; attempt 1 takes a as source, attempt 2 takes attempt 1 as source.
    let r1 = ev(Some(a.id), 1, data(1), Some(Utc::now()), 0);
    append(&mut c, &[r1.clone()], true);
    let after1 = count(&c);
    let r2 = ev(Some(r1.id), 1, data(1), Some(Utc::now()), 0);
    append(&mut c, &[r2], true);
    let after2 = count(&c);
    let mut r1_again = r1.clone();
    r1_again.id = Uuid::now_v7();
    append(&mut c, &[r1_again], true);
    let after_dup = count(&c);
    println!("P1 rows: repeat {repeat} (want 1), other data {other} (want 2), retry 1 {after1} (want 3), retry 2 {after2} (want 4), retry 1 sent twice {after_dup} (want 4)");
}

/// A history of `n` root events, 99% processed through a state-change row, 0.1% waiting,
/// 1% with a past run_at, and a flow result for every tenth root.
fn history(c: &mut Connection, n: usize, now: DateTime<Utc>) -> Vec<Ev> {
    let mut all = Vec::with_capacity(n * 2);
    for i in 0..n {
        let run_at = if i % 100 == 0 { Some(now - TimeDelta::minutes(5)) } else { None };
        let r = ev(None, (i % 8) as u16, data(i), run_at, 0);
        if i % 1000 != 0 {
            all.push(ev(Some(r.id), r.ty, "null".into(), None, 2));
        }
        if i % 10 == 0 {
            all.push(ev(Some(r.id), r.ty, data(i + n), None, 0));
        }
        all.push(r);
    }
    for b in all.chunks(10_000) {
        append(c, b, false);
    }
    all
}

fn p2_p3(dir: &str) {
    let now = Utc::now();
    for n in [1_000usize, 10_000, 100_000] {
        let mut c = open(&format!("{dir}/p2-{n}.db"), true);
        let hist = history(&mut c, n, now);
        let roots: Vec<&Ev> = hist.iter().filter(|e| e.source.is_none()).collect();
        for (label, sourced) in [("with a source", true), ("roots", false)] {
            for once in [false, true] {
                let mut v = Vec::new();
                for round in 0..20 {
                    // 100 new events: results of existing roots, or new roots.
                    let batch: Vec<Ev> = (0..100)
                        .map(|k| {
                            let i = round * 100 + k;
                            if sourced {
                                let r = roots[i % roots.len()];
                                ev(Some(r.id), r.ty, data(i + (7 + usize::from(once)) * 10_000_000), None, 0)
                            } else {
                                ev(None, (i % 8) as u16, data(i + (9 + usize::from(once)) * 10_000_000), None, 0)
                            }
                        })
                        .collect();
                    let t = Instant::now();
                    let kept = append(&mut c, &batch, once);
                    v.push(t.elapsed().as_secs_f64() * 1000.0);
                    assert_eq!(kept, 100, "every event in the batch is new");
                }
                let (med, max) = median_ms(v);
                println!("P2/P3 history {n}: {label}, {}: batch of 100 median {med:.2} ms, max {max:.2} ms", if once { "kept once" } else { "plain" });
            }
        }
    }
}

fn reference(rows: &[Ev], ty: u16, now: DateTime<Utc>) -> (Vec<String>, Vec<String>) {
    let changed: HashSet<Uuid> = rows.iter().filter(|e| e.status != 0).filter_map(|e| e.source).collect();
    let mut w: Vec<String> = rows.iter().filter(|e| e.ty == ty && e.status == 0 && !changed.contains(&e.id)).map(|e| e.id.to_string()).collect();
    let mut d: Vec<String> = rows
        .iter()
        .filter(|e| e.status == 0 && e.run_at.is_some_and(|t| ts(t) <= ts(now)) && !changed.contains(&e.id))
        .map(|e| e.id.to_string())
        .collect();
    w.sort();
    d.sort();
    (w, d)
}

fn p4(dir: &str) {
    let now = Utc::now();
    let mut c = open(&format!("{dir}/p4.db"), true);
    let all = history(&mut c, 10_000, now);
    let before = count(&c);
    let processed: HashSet<Uuid> = all.iter().filter(|e| e.status == 2).filter_map(|e| e.source).collect();
    let chosen: HashSet<Uuid> = all.iter().filter(|e| e.source.is_none() && processed.contains(&e.id)).step_by(3).map(|e| e.id).collect();
    let tx = c.transaction().expect("tx");
    {
        let mut del = tx.prepare("DELETE FROM _events WHERE id = ?1 OR (source = ?1 AND status <> 0)").expect("prep");
        for id in &chosen {
            del.execute([id.to_string()]).expect("del");
        }
    }
    tx.commit().expect("commit");
    let remain: Vec<Ev> = all
        .into_iter()
        .filter(|e| !chosen.contains(&e.id) && !(e.status != 0 && e.source.is_some_and(|s| chosen.contains(&s))))
        .collect();
    let after = count(&c);
    let mut ok = after as usize == remain.len();
    for ty in 0..8u16 {
        let (w, d) = reference(&remain, ty, now);
        ok &= ids(&c, WAITING, &ty) == w;
        if ty == 0 {
            ok &= ids(&c, DUE, &ts(now)) == d;
        }
    }
    let dangling: i64 = c
        .query_row("SELECT count(*) FROM _events e WHERE e.source IS NOT NULL AND NOT EXISTS (SELECT 1 FROM _events p WHERE p.id = e.source)", [], |r| r.get(0))
        .expect("dangling");
    println!("P4 trimmed {} chosen roots: rows {before} -> {after} (reference {}), WAITING and DUE match: {ok}; flow results left pointing at a removed cause: {dangling}", chosen.len(), remain.len());
}

fn p5(dir: &str) {
    let now = Utc::now();
    for n in [1_000usize, 10_000, 100_000] {
        let path = format!("{dir}/p5-{n}.db");
        let mut c = open(&path, false);
        history(&mut c, n, now);
        for (label, sql) in [
            ("no source index", ""),
            ("(source)", "CREATE INDEX _events_source ON _events (source);"),
            ("(source)+(id, source)", "CREATE INDEX _events_id_source ON _events (id, source);"),
        ] {
            if !sql.is_empty() {
                c.execute_batch(sql).expect("index");
            }
            if n == 100_000 && label == "no source index" {
                println!("P5 history {n}, {label}: skipped (round 2 first run: WAITING 47,100 ms, DUE 4,146 ms)");
                continue;
            }
            for (name, run) in [("WAITING ty=0", true), ("DUE", false)] {
                let f = || if run { ids(&c, WAITING, &0u16).len() } else { ids(&c, DUE, &ts(now)).len() };
                let rows = f();
                let v: Vec<f64> = (0..15)
                    .map(|_| {
                        let t = Instant::now();
                        f();
                        t.elapsed().as_secs_f64() * 1000.0
                    })
                    .collect();
                let (med, max) = median_ms(v);
                println!("P5 history {n}, {label}, {name}: {rows} rows, median {med:.2} ms, max {max:.2} ms");
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(2).cloned().unwrap_or_else(|| ".".into());
    match args.get(1).map(String::as_str) {
        Some("p1") => p1(&dir),
        Some("p2") => p2_p3(&dir),
        Some("p4") => p4(&dir),
        Some("p5") => p5(&dir),
        _ => eprintln!("usage: events-toy p1|p2|p4|p5 <dir>"),
    }
}
