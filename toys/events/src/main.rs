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
        c.execute_batch("CREATE INDEX _events_source ON _events (source);")
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
        let (rid, rty) = (r.id, r.ty);
        // the event first, then what follows it, as rannoch appends them
        all.push(r);
        if i % 1000 != 0 {
            all.push(ev(Some(rid), rty, "null".into(), None, 2));
        }
        if i % 10 == 0 {
            all.push(ev(Some(rid), rty, data(i + n), None, 0));
        }
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

fn p6(dir: &str) {
    let now = Utc::now();
    let mut c = open(&format!("{dir}/p6.db"), true);
    let hist = history(&mut c, 100_000, now);
    let roots: Vec<&Ev> = hist.iter().filter(|e| e.source.is_none()).collect();
    let mut seq = 0usize;
    for size in [100usize, 1_000, 10_000] {
        for once in [false, true] {
            let total = 50_000;
            let evs: Vec<Ev> = (0..total)
                .map(|k| {
                    seq += 1;
                    let r = roots[k % roots.len()];
                    ev(Some(r.id), r.ty, data(seq + 50_000_000), None, 0)
                })
                .collect();
            let t = Instant::now();
            let mut kept = 0;
            for b in evs.chunks(size) {
                kept += append(&mut c, b, once);
            }
            let secs = t.elapsed().as_secs_f64();
            assert_eq!(kept, total);
            println!("P6 history 100,000, with a source, batch {size}, {}: {total} events in {secs:.2}s = {:.0}/s", if once { "kept once" } else { "plain" }, total as f64 / secs);
        }
    }
}

/// Plain append, one change at a time, to find where the time goes.
fn p7(dir: &str) {
    let now = Utc::now();
    let total = 50_000usize;
    let variants: [(&str, &str, &str, usize); 8] = [
        ("baseline + (id, source) index", "IDXIDSRC", "TEXT", 1),
        ("baseline: TEXT id, rollback journal, sync FULL, one INSERT per row", "", "TEXT", 1),
        ("no fsync (synchronous OFF), attribution only", "PRAGMA synchronous = OFF;", "TEXT", 1),
        ("WAL, synchronous NORMAL", "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;", "TEXT", 1),
        ("WAL, synchronous FULL", "PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;", "TEXT", 1),
        ("id and source as 16-byte BLOB", "", "BLOB", 1),
        ("multi-row INSERT, 100 rows per statement", "", "TEXT", 100),
        ("WAL NORMAL + BLOB ids + multi-row", "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;", "BLOB", 100),
    ];
    for (label, pragmas, idty, rows) in variants {
        let path = format!("{dir}/p7.db");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
        let mut c = Connection::open(&path).expect("open");
        let extra = pragmas == "IDXIDSRC";
        if !extra { c.execute_batch(pragmas).expect("pragmas"); }
        c.execute_batch(&format!("CREATE TABLE _events (id {idty}, source {idty}, ty INTEGER NOT NULL, data TEXT NOT NULL, run_at TEXT, status INTEGER NOT NULL CHECK (status IN (-1, 0, 1, 2)), PRIMARY KEY (id)); CREATE INDEX _events_source ON _events (source);")).expect("table");
        if extra { c.execute_batch("CREATE INDEX _events_id_source ON _events (id, source);").expect("idx"); }
        let blob = idty == "BLOB";
        let row = |e: &Ev| -> Vec<rusqlite::types::Value> {
            use rusqlite::types::Value;
            let id = if blob { Value::Blob(e.id.as_bytes().to_vec()) } else { Value::Text(e.id.to_string()) };
            let src = match e.source { None => Value::Null, Some(s) => if blob { Value::Blob(s.as_bytes().to_vec()) } else { Value::Text(s.to_string()) } };
            vec![id, src, Value::Integer(i64::from(e.ty)), Value::Text(e.data.clone()), e.run_at.map_or(Value::Null, |t| Value::Text(ts(t))), Value::Integer(i64::from(e.status))]
        };
        // history of 100,000 roots with a state change each
        let mut hist = Vec::new();
        for i in 0..100_000usize {
            let r = ev(None, (i % 8) as u16, data(i), None, 0);
            hist.push(ev(Some(r.id), r.ty, "null".into(), None, 2));
            hist.push(r);
        }
        let insert_sql = |n: usize| format!("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES {}", vec!["(?, ?, ?, ?, ?, ?)"; n].join(", "));
        let write = |c: &mut Connection, evs: &[Ev]| {
            for b in evs.chunks(1000) {
                let tx = c.transaction().expect("tx");
                {
                    let mut st = tx.prepare_cached(&insert_sql(rows)).expect("prep");
                    for chunk in b.chunks(rows) {
                        if chunk.len() == rows {
                            let vals: Vec<rusqlite::types::Value> = chunk.iter().flat_map(row).collect();
                            st.execute(rusqlite::params_from_iter(vals)).expect("ins");
                        } else {
                            let mut one = tx.prepare_cached(&insert_sql(1)).expect("prep");
                            for e in chunk {
                                one.execute(rusqlite::params_from_iter(row(e))).expect("ins");
                            }
                        }
                    }
                }
                tx.commit().expect("commit");
            }
        };
        write(&mut c, &hist);
        let roots: Vec<Uuid> = hist.iter().filter(|e| e.source.is_none()).map(|e| e.id).collect();
        let evs: Vec<Ev> = (0..total).map(|k| ev(Some(roots[k % roots.len()]), (k % 8) as u16, data(k + 60_000_000), None, 0)).collect();
        let t = Instant::now();
        write(&mut c, &evs);
        let secs = t.elapsed().as_secs_f64();
        println!("P7 {label}: {:.0}/s ({:.1} us per event)", total as f64 / secs, secs * 1e6 / total as f64);
    }
    let evs: Vec<Ev> = (0..total).map(|k| ev(None, (k % 8) as u16, data(k), Some(now), 0)).collect();
    let t = Instant::now();
    let mut n = 0usize;
    for e in &evs {
        n += e.id.to_string().len() + e.run_at.map(ts).map_or(0, |s| s.len());
    }
    println!("P7 Rust side only (id to text, run_at to text): {:.2} us per event ({n})", t.elapsed().as_secs_f64() * 1e6 / total as f64);
}

const TREE: &str = "WITH RECURSIVE tree(id) AS (
    SELECT id FROM _events WHERE id = ?1
    UNION SELECT e.id FROM _events e JOIN tree t ON e.source = t.id
) SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM _events e JOIN tree USING (id) ORDER BY e.id";
const CURRENT_STATUS: &str = "SELECT coalesce((
    SELECT s.status FROM _events s WHERE s.source = e.id AND s.status <> 0
    ORDER BY s.id DESC LIMIT 1
), e.status) FROM _events e WHERE e.id = ?1";

/// Every read in rannoch-events, plan and time, with (source) alone and with (id, source) added.
fn p8(dir: &str) {
    let now = Utc::now();
    let mut c = open(&format!("{dir}/p8.db"), false);
    let hist = history(&mut c, 100_000, now);
    let roots: Vec<String> = hist.iter().filter(|e| e.source.is_none()).step_by(997).map(|e| e.id.to_string()).collect();
    for (label, sql) in [("(source)", "CREATE INDEX _events_source ON _events (source);"), ("(source)+(id, source)", "CREATE INDEX _events_id_source ON _events (id, source);")] {
        c.execute_batch(sql).expect("index");
        let reads: [(&str, &str); 4] = [("TREE", TREE), ("CURRENT_STATUS", CURRENT_STATUS), ("WAITING", WAITING), ("DUE", DUE)];
        for (name, q) in reads {
            let plan: Vec<String> = {
                let mut st = c.prepare(&format!("EXPLAIN QUERY PLAN {q}")).expect("plan");
                let arg: rusqlite::types::Value = match name {
                    "WAITING" => rusqlite::types::Value::Integer(0),
                    "DUE" => rusqlite::types::Value::Text(ts(now)),
                    _ => rusqlite::types::Value::Text(roots[0].clone()),
                };
                st.query_map([arg], |r| r.get::<_, String>(3)).expect("q").map(|r| r.expect("row")).collect()
            };
            let mut v = Vec::new();
            for _ in 0..5 {
                let t = Instant::now();
                match name {
                    "WAITING" => { ids(&c, WAITING, &0u16); }
                    "DUE" => { ids(&c, DUE, &ts(now)); }
                    "CURRENT_STATUS" => for r in &roots { let _: i64 = c.query_row(q, [r], |x| x.get(0)).expect("status"); },
                    _ => for r in &roots { let _ = ids(&c, q, r); },
                }
                v.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let (med, _) = median_ms(v);
            let per = if matches!(name, "TREE" | "CURRENT_STATUS") { format!("{:.3} ms per call over {} roots", med / roots.len() as f64, roots.len()) } else { format!("{med:.2} ms") };
            println!("P8 {label} {name}: {per}; plan: {}", plan.join(" | "));
        }
    }
}

/// The levers stacked, with the repeat check on: journal, id type, rows per statement, one statement per batch.
fn p9(dir: &str) {
    use rusqlite::types::Value;
    let total = 100_000usize;
    // (label, wal, blob, mode) mode: 0 plain per row, 1 check per row, 2 plain multi-row, 3 check multi-row, 4 check whole batch via temp table
    let variants: [(&str, bool, bool, u8); 10] = [
        ("rollback, TEXT, plain per row", false, false, 0),
        ("rollback, TEXT, check per row", false, false, 1),
        ("WAL NORMAL, TEXT, check per row", true, false, 1),
        ("WAL NORMAL, BLOB, plain per row", true, true, 0),
        ("WAL NORMAL, BLOB, check per row", true, true, 1),
        ("WAL NORMAL, BLOB, plain 100 rows per statement", true, true, 2),
        ("WAL NORMAL, BLOB, check 100 rows per statement", true, true, 3),
        ("WAL NORMAL, BLOB, check whole batch in one statement", true, true, 4),
        ("WAL NORMAL, TEXT, check whole batch in one statement", true, false, 4),
        ("rollback, TEXT, check whole batch in one statement", false, false, 4),
    ];
    for per_commit in [1_000usize, 10_000] {
        for (label, wal, blob, mode) in variants {
            let path = format!("{dir}/p9.db");
            for x in ["", "-wal", "-shm"] { let _ = std::fs::remove_file(format!("{path}{x}")); }
            let mut c = Connection::open(&path).expect("open");
            if wal { c.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;").expect("wal"); }
            let idty = if blob { "BLOB" } else { "TEXT" };
            c.execute_batch(&format!("CREATE TABLE _events (id {idty}, source {idty}, ty INTEGER NOT NULL, data TEXT NOT NULL, run_at TEXT, status INTEGER NOT NULL CHECK (status IN (-1, 0, 1, 2)), PRIMARY KEY (id)); CREATE INDEX _events_source ON _events (source);")).expect("table");
            register(&c);
            let key = |u: Uuid| if blob { Value::Blob(u.as_bytes().to_vec()) } else { Value::Text(u.to_string()) };
            let row = |e: &Ev| vec![key(e.id), e.source.map_or(Value::Null, key), Value::Integer(i64::from(e.ty)), Value::Text(e.data.clone()), e.run_at.map_or(Value::Null, |t| Value::Text(ts(t))), Value::Integer(i64::from(e.status))];
            let one = "INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)".to_string();
            let check_one = APPEND_ONCE.to_string();
            let multi = |n: usize| format!("INSERT INTO _events (id, source, ty, data, run_at, status) VALUES {}", vec!["(?, ?, ?, ?, ?, ?)"; n].join(", "));
            let check_multi = |n: usize| format!("INSERT INTO _events (id, source, ty, data, run_at, status)
SELECT v.column1, v.column2, v.column3, v.column4, v.column5, v.column6 FROM (VALUES {}) v
WHERE NOT EXISTS (SELECT 1 FROM _events e WHERE e.source IS v.column2 AND e.ty = v.column3 AND e.status = v.column6
  AND rannoch_hash(quote(e.ty), quote(e.source), quote(e.status), quote(e.data)) = rannoch_hash(quote(v.column3), quote(v.column2), quote(v.column6), quote(v.column4)))", vec!["(?, ?, ?, ?, ?, ?)"; n].join(", "));
            c.execute_batch(&format!("CREATE TEMP TABLE batch (id {idty}, source {idty}, ty INTEGER, data TEXT, run_at TEXT, status INTEGER);")).expect("temp");
            let write = |c: &mut Connection, evs: &[Ev], mode: u8, size: usize| -> usize {
                let mut kept = 0;
                for b in evs.chunks(size) {
                    let tx = c.transaction().expect("tx");
                    match mode {
                        0 | 1 => {
                            let mut st = tx.prepare_cached(if mode == 0 { &one } else { &check_one }).expect("prep");
                            for e in b { kept += st.execute(rusqlite::params_from_iter(row(e))).expect("ins"); }
                        }
                        2 | 3 => {
                            for chunk in b.chunks(100) {
                                let sql = if mode == 2 { multi(chunk.len()) } else { check_multi(chunk.len()) };
                                let mut st = tx.prepare_cached(&sql).expect("prep");
                                kept += st.execute(rusqlite::params_from_iter(chunk.iter().flat_map(row))).expect("ins");
                            }
                        }
                        _ => {
                            {
                                let mut st = tx.prepare_cached("INSERT INTO temp.batch VALUES (?1, ?2, ?3, ?4, ?5, ?6)").expect("prep");
                                for e in b { st.execute(rusqlite::params_from_iter(row(e))).expect("ins"); }
                            }
                            kept += tx.execute("INSERT INTO _events (id, source, ty, data, run_at, status)
SELECT v.id, v.source, v.ty, v.data, v.run_at, v.status FROM temp.batch v
WHERE NOT EXISTS (SELECT 1 FROM _events e WHERE e.source IS v.source AND e.ty = v.ty AND e.status = v.status
  AND rannoch_hash(quote(e.ty), quote(e.source), quote(e.status), quote(e.data)) = rannoch_hash(quote(v.ty), quote(v.source), quote(v.status), quote(v.data)))", []).expect("ins");
                            tx.execute("DELETE FROM temp.batch", []).expect("clear");
                        }
                    }
                    tx.commit().expect("commit");
                }
                kept
            };
            let mut hist = Vec::with_capacity(200_000);
            for i in 0..100_000usize {
                let r = ev(None, (i % 8) as u16, data(i), None, 0);
                hist.push(ev(Some(r.id), r.ty, "null".into(), None, 2));
                hist.push(r);
            }
            write(&mut c, &hist, 0, 10_000);
            let roots: Vec<Uuid> = hist.iter().filter(|e| e.source.is_none()).map(|e| e.id).collect();
            let evs: Vec<Ev> = (0..total).map(|k| ev(Some(roots[k % roots.len()]), (k % 8) as u16, data(k + 70_000_000), None, 0)).collect();
            let t = Instant::now();
            let kept = write(&mut c, &evs, mode, per_commit);
            let secs = t.elapsed().as_secs_f64();
            assert_eq!(kept, total);
            println!("P9 {per_commit} per commit, {label}: {:.0}/s", total as f64 / secs);
        }
    }
}

/// Upsert of root events at growing history: expression unique index (nothing stored in the table),
/// stored hash column with a unique index, and today's NOT EXISTS check. Rollback journal, TEXT ids, as main.
fn p10(dir: &str) {
    const EXPR: &str = "rannoch_hash(quote(ty), quote(source), quote(status), quote(data))";
    let sizes: Vec<usize> = std::env::var("TOY_SIZES").map(|v| v.split(',').map(|x| x.parse().expect("n")).collect()).unwrap_or(vec![1_000, 10_000, 100_000, 1_000_000]);
    for n in sizes {
        for (label, mode) in [("expression unique index", 0u8), ("stored hash column, unique index", 1), ("NOT EXISTS check", 2)] {
            if std::env::var("TOY_ONLY_EXPR").is_ok() && mode != 0 { continue; }
            if mode == 2 && n > 100_000 { println!("P10 history {n}, {label}: skipped (1,951 ms per 100 at 100,000)"); continue; }
            let path = format!("{dir}/p10.db");
            let _ = std::fs::remove_file(&path);
            for x in ["-wal", "-shm"] { let _ = std::fs::remove_file(format!("{path}{x}")); }
            let mut c = Connection::open(&path).expect("open");
            if let Ok(pr) = std::env::var("TOY_PRAGMAS") { c.execute_batch(&pr).expect("pragmas"); }
            register(&c);
            c.execute_batch(TABLE).expect("table");
            c.execute_batch("CREATE INDEX _events_source ON _events (source);").expect("idx");
            match mode {
                0 => c.execute_batch(&format!("CREATE UNIQUE INDEX _events_once ON _events ({EXPR});")).expect("expr"),
                1 => c.execute_batch("ALTER TABLE _events ADD COLUMN hash BLOB; CREATE UNIQUE INDEX _events_once ON _events (hash);").expect("col"),
                _ => {}
            }
            let ins: String = match mode {
                0 => "INSERT OR IGNORE INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)".into(),
                1 => "INSERT OR IGNORE INTO _events (id, source, ty, data, run_at, status, hash) VALUES (?1, ?2, ?3, ?4, ?5, ?6, rannoch_hash(quote(?3), quote(?2), quote(?6), quote(?4)))".into(),
                _ => APPEND_ONCE.into(),
            };
            let write = |c: &mut Connection, evs: &[Ev], size: usize| -> (usize, Vec<f64>) {
                let mut kept = 0; let mut ms = Vec::new();
                for b in evs.chunks(size) {
                    let t = Instant::now();
                    let tx = c.transaction().expect("tx");
                    {
                        let mut st = tx.prepare_cached(&ins).expect("prep");
                        for e in b { kept += st.execute(params![e.id.to_string(), e.source.map(|s| s.to_string()), e.ty, e.data, e.run_at.map(ts), e.status]).expect("ins"); }
                    }
                    tx.commit().expect("commit");
                    ms.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                (kept, ms)
            };
            let hist: Vec<Ev> = (0..n).map(|i| ev(None, (i % 8) as u16, data(i), None, 0)).collect();
            let t = Instant::now();
            write(&mut c, &hist, 10_000);
            let build = t.elapsed().as_secs_f64();
            // 2,000 new roots in batches of 100, then the same 2,000 again (all repeats)
            let fresh: Vec<Ev> = (0..2_000).map(|i| ev(None, (i % 8) as u16, data(i + 80_000_000), None, 0)).collect();
            let again: Vec<Ev> = fresh.iter().map(|e| { let mut x = e.clone(); x.id = Uuid::now_v7(); x }).collect();
            let (k1, m1) = write(&mut c, &fresh, 100);
            let (k2, m2) = write(&mut c, &again, 100);
            let (a, _) = median_ms(m1); let (b, _) = median_ms(m2);
            let rate_build = n as f64 / build;
            println!("P10 history {n}, {label}: new roots batch of 100 median {a:.2} ms (kept {k1}/2000), repeats batch of 100 median {b:.2} ms (kept {k2}/2000); history built at {rate_build:.0}/s in batches of 10,000");
        }
    }
}

/// Checks, not predictions: does a connection without rannoch_hash write to a table with the expression index;
/// WAITING and DUE at 1,000,000 events with (source).
fn p11(dir: &str) {
    let path = format!("{dir}/p11.db");
    let _ = std::fs::remove_file(&path);
    {
        let c = Connection::open(&path).expect("open");
        register(&c);
        c.execute_batch(TABLE).expect("table");
        c.execute_batch("CREATE UNIQUE INDEX _events_once ON _events (rannoch_hash(quote(ty), quote(source), quote(status), quote(data)));").expect("idx");
    }
    let bare = Connection::open(&path).expect("open");
    let r = bare.execute(APPEND, params![Uuid::now_v7().to_string(), Option::<String>::None, 1, "{}", Option::<String>::None, 0]);
    println!("P11 insert from a connection without rannoch_hash: {r:?}");
    let read = bare.query_row("SELECT count(*) FROM _events", [], |r| r.get::<_, i64>(0));
    println!("P11 read from a connection without rannoch_hash: {read:?}");
    let now = Utc::now();
    let mut c = open(&format!("{dir}/p11b.db"), false);
    history(&mut c, 1_000_000, now);
    c.execute_batch("CREATE INDEX _events_source ON _events (source);").expect("idx");
    for (name, run) in [("WAITING ty=0", true), ("DUE", false)] {
        let f = || if run { ids(&c, WAITING, &0u16).len() } else { ids(&c, DUE, &ts(now)).len() };
        let rows = f();
        let v: Vec<f64> = (0..5).map(|_| { let t = Instant::now(); f(); t.elapsed().as_secs_f64() * 1000.0 }).collect();
        let (med, max) = median_ms(v);
        println!("P11 history 1000000, (source), {name}: {rows} rows, median {med:.1} ms, max {max:.1} ms");
    }
}

const TREE_FIXED: &str = "WITH RECURSIVE tree(id) AS (
    SELECT id FROM _events WHERE id = ?1
    UNION SELECT e.id FROM _events e JOIN tree t ON e.source = t.id
) SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM tree CROSS JOIN _events e ON e.id = tree.id ORDER BY e.id";

fn p12(dir: &str) {
    let now = Utc::now();
    for n in [100_000usize, 1_000_000] {
        let mut c = open(&format!("{dir}/p12.db"), false);
        let hist = history(&mut c, n, now);
        c.execute_batch("CREATE INDEX _events_source ON _events (source);").expect("idx");
        let roots: Vec<String> = hist.iter().filter(|e| e.source.is_none()).step_by(997).map(|e| e.id.to_string()).collect();
        let mut same = true;
        for r in &roots { same &= ids(&c, TREE, r) == ids(&c, TREE_FIXED, r); }
        for (label, q) in [("TREE today", TREE), ("TREE from the tree", TREE_FIXED)] {
            let t = Instant::now();
            for _ in 0..3 { for r in &roots { let _ = ids(&c, q, r); } }
            let per = t.elapsed().as_secs_f64() * 1000.0 / (3 * roots.len()) as f64;
            println!("P12 history {n}, {label}: {per:.3} ms per root over {} roots; same rows: {same}", roots.len());
        }
    }
}

const WAITING_SET: &str = "CREATE TABLE _waiting (id TEXT PRIMARY KEY, ty INTEGER NOT NULL, run_at TEXT) WITHOUT ROWID;
CREATE INDEX _waiting_ty ON _waiting (ty);
CREATE INDEX _waiting_run_at ON _waiting (run_at);
CREATE TRIGGER _events_waiting AFTER INSERT ON _events BEGIN
  INSERT INTO _waiting (id, ty, run_at) SELECT NEW.id, NEW.ty, NEW.run_at WHERE NEW.status = 0;
  DELETE FROM _waiting WHERE NEW.status <> 0 AND id = NEW.source;
END;";
const WAITING_T: &str = "SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status FROM _waiting w CROSS JOIN _events e ON e.id = w.id WHERE w.ty = ?1 ORDER BY e.id";
const DUE_T: &str = "SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status FROM _waiting w CROSS JOIN _events e ON e.id = w.id WHERE w.run_at <= ?1 ORDER BY e.id";

fn p13(dir: &str) {
    let now = Utc::now();
    for n in [100_000usize, 1_000_000] {
        for trig in [false, true] {
            let mut c = open(&format!("{dir}/p13.db"), false);
            c.execute_batch("CREATE INDEX _events_source ON _events (source);").expect("idx");
            if trig { c.execute_batch(WAITING_SET).expect("trigger"); }
            let t = Instant::now();
            let hist = history(&mut c, n, now);
            let build = t.elapsed().as_secs_f64();
            let roots: Vec<&Ev> = hist.iter().filter(|e| e.source.is_none()).collect();
            // append cost: 20 batches of 100 sourced events, 1,000 per commit not needed here
            let mut v = Vec::new();
            for round in 0..20 {
                let batch: Vec<Ev> = (0..100).map(|k| { let i = round * 100 + k; let r = roots[i % roots.len()]; ev(Some(r.id), r.ty, data(i + 90_000_000), None, 0) }).collect();
                let t = Instant::now(); append(&mut c, &batch, false); v.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let (app, _) = median_ms(v);
            let reads: [(&str, &str, bool); 2] = if trig { [("WAITING", WAITING_T, true), ("DUE", DUE_T, false)] } else { [("WAITING", WAITING, true), ("DUE", DUE, false)] };
            let mut line = format!("P13 history {n}, {}: history built {:.0}/s, append batch of 100 median {app:.2} ms", if trig { "trigger-kept _waiting" } else { "today's reads" }, n as f64 / build);
            for (name, q, w) in reads {
                let f = || if w { ids(&c, q, &0u16) } else { ids(&c, q, &ts(now)) };
                let rows = f();
                let mut tv = Vec::new();
                for _ in 0..5 { let t = Instant::now(); f(); tv.push(t.elapsed().as_secs_f64() * 1000.0); }
                let (med, _) = median_ms(tv);
                let refr = if w { ids(&c, WAITING, &0u16) } else { ids(&c, DUE, &ts(now)) };
                line += &format!("; {name} {} rows {med:.2} ms (same as today's read: {})", rows.len(), rows == refr);
            }
            println!("{line}");
        }
    }
}

fn p14(dir: &str) {
    let now = Utc::now();
    let mut c = open(&format!("{dir}/p14.db"), false);
    c.execute_batch("CREATE INDEX _events_source ON _events (source);").expect("idx");
    c.execute_batch(WAITING_SET).expect("trigger");
    c.execute_batch("CREATE TRIGGER _events_waiting_out AFTER DELETE ON _events BEGIN DELETE FROM _waiting WHERE id = OLD.id; END;").expect("del trigger");
    let all = history(&mut c, 10_000, now);
    // the app chooses every third root, processed or still waiting
    let chosen: HashSet<Uuid> = all.iter().filter(|e| e.source.is_none()).step_by(3).map(|e| e.id).collect();
    let waiting_chosen = { let changed: HashSet<Uuid> = all.iter().filter(|e| e.status != 0).filter_map(|e| e.source).collect(); chosen.iter().filter(|id| !changed.contains(id)).count() };
    let tx = c.transaction().expect("tx");
    { let mut del = tx.prepare("DELETE FROM _events WHERE id = ?1 OR (source = ?1 AND status <> 0)").expect("prep"); for id in &chosen { del.execute([id.to_string()]).expect("del"); } }
    tx.commit().expect("commit");
    let remain: Vec<Ev> = all.into_iter().filter(|e| !chosen.contains(&e.id) && !(e.status != 0 && e.source.is_some_and(|s| chosen.contains(&s)))).collect();
    let mut ok = true;
    for ty in 0..8u16 { let (w, d) = reference(&remain, ty, now); ok &= ids(&c, WAITING_T, &ty) == w; if ty == 0 { ok &= ids(&c, DUE_T, &ts(now)) == d; } }
    println!("P14 trimmed {} roots ({waiting_chosen} of them waiting): WAITING and DUE from _waiting match the reference: {ok}", chosen.len());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(2).cloned().unwrap_or_else(|| ".".into());
    match args.get(1).map(String::as_str) {
        Some("p1") => p1(&dir),
        Some("p2") => p2_p3(&dir),
        Some("p4") => p4(&dir),
        Some("p5") => p5(&dir),
        Some("p6") => p6(&dir),
        Some("p7") => p7(&dir),
        Some("p8") => p8(&dir),
        Some("p9") => p9(&dir),
        Some("p10") => p10(&dir),
        Some("p11") => p11(&dir),
        Some("p12") => p12(&dir),
        Some("p13") => p13(&dir),
        Some("p14") => p14(&dir),
        _ => eprintln!("usage: events-toy p1|p2|p4|p5 <dir>"),
    }
}
