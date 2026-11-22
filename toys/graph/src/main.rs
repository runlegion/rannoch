//! graph toy for FR-RANNOCH-123 (draft): two plain tables in rannoch's SQLite file, rows only
//! ever added, read now, as of T and why, with describe, walk, search and warm hoods.
//! Predictions in predictions.txt were written before this ran.

use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use uuid::{NoContext, Timestamp, Uuid};

/// rannoch-store's pragmas, as built.
const PRAGMAS: &str = "PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA cache_size = -65536;
PRAGMA wal_autocheckpoint = 0;
PRAGMA fullfsync = ON;";

/// _events as rannoch-events' three migrations build it, then the graph's tables.
const SCHEMA: &str = "
CREATE TABLE _events (id TEXT PRIMARY KEY, source TEXT, ty INTEGER NOT NULL, data TEXT NOT NULL,
  run_at TEXT, status INTEGER NOT NULL CHECK (status IN (-1, 0, 1, 2)));
CREATE INDEX _events_status ON _events(status);
CREATE INDEX _events_source ON _events(source);
CREATE TABLE _nodes (node TEXT NOT NULL, key TEXT NOT NULL, value TEXT, event TEXT NOT NULL,
  written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (node, key, event)) WITHOUT ROWID;
CREATE TABLE _edges (src TEXT NOT NULL, name TEXT NOT NULL, dst TEXT NOT NULL, props TEXT,
  event TEXT NOT NULL, written INTEGER NOT NULL, retracted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (src, name, dst, event)) WITHOUT ROWID;
CREATE INDEX _edges_in ON _edges(dst, name, src, event);
CREATE VIRTUAL TABLE _search USING fts5(text);
CREATE TABLE _search_map (node TEXT NOT NULL, key TEXT NOT NULL, row INTEGER NOT NULL,
  PRIMARY KEY (node, key)) WITHOUT ROWID;
";

#[derive(Clone, Debug)]
enum Op {
    Set(String, String, Value),
    Retract(String, String),
    Link(String, String, String, Value),
    Unlink(String, String, String),
}

#[derive(Clone, Debug)]
struct Ev {
    id: String,
    source: Option<String>,
    ops: Vec<Op>,
}

fn v7_at(ms: u64) -> String {
    Uuid::new_v7(Timestamp::from_unix(NoContext, ms / 1000, ((ms % 1000) * 1_000_000) as u32)).to_string()
}

#[derive(Clone, Copy, Default)]
struct Opts {
    at: Option<i64>,
    history: bool,
}

struct Db {
    conn: Connection,
    clock: i64,
}

type Row = (Value, String, i64);

impl Db {
    fn open(path: &str) -> Db {
        let _ = std::fs::remove_file(path);
        for ext in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{ext}"));
        }
        let conn = Connection::open(path).expect("open");
        conn.execute_batch(PRAGMAS).expect("pragmas");
        conn.execute_batch(SCHEMA).expect("schema");
        Db { conn, clock: 0 }
    }

    /// One event's writes in one transaction. Returns the nodes whose rows were added.
    fn apply(&mut self, ev: &Ev) -> HashSet<String> {
        self.apply_many(std::slice::from_ref(ev))
    }

    fn apply_many(&mut self, evs: &[Ev]) -> HashSet<String> {
        let written = self.clock;
        let tx = self.conn.transaction().expect("tx");
        let mut touched = HashSet::new();
        for ev in evs {
            tx.execute(
                "INSERT OR IGNORE INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, 1, '{}', NULL, 2)",
                params![ev.id, ev.source],
            )
            .expect("event");
            for op in &ev.ops {
                match op {
                    Op::Set(n, k, _) | Op::Retract(n, k) => {
                        let (val, retracted) = match op {
                            Op::Set(_, _, v) => (Some(v.to_string()), 0),
                            _ => (None, 1),
                        };
                        let added = tx
                            .execute(
                                "INSERT OR IGNORE INTO _nodes (node, key, value, event, written, retracted) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                params![n, k, val, ev.id, written, retracted],
                            )
                            .expect("node row");
                        if added == 1 {
                            touched.insert(n.clone());
                            reindex(&tx, n, k);
                        }
                    }
                    Op::Link(s, name, d, _) | Op::Unlink(s, name, d) => {
                        let (props, retracted) = match op {
                            Op::Link(_, _, _, p) => (Some(p.to_string()), 0),
                            _ => (None, 1),
                        };
                        let added = tx
                            .execute(
                                "INSERT OR IGNORE INTO _edges (src, name, dst, props, event, written, retracted) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                                params![s, name, d, props, ev.id, written, retracted],
                            )
                            .expect("edge row");
                        if added == 1 {
                            touched.insert(s.clone());
                            touched.insert(d.clone());
                        }
                    }
                }
            }
        }
        tx.commit().expect("commit");
        touched
    }

    fn count(&self, sql: &str) -> i64 {
        self.conn.query_row(sql, [], |r| r.get(0)).expect("count")
    }

    /// The current row of a node's key, as of `at`.
    fn prop(&self, node: &str, key: &str, at: Option<i64>) -> Option<Row> {
        self.conn
            .prepare_cached(
                "SELECT value, event, written, retracted FROM _nodes WHERE node = ?1 AND key = ?2
                 AND (?3 IS NULL OR written <= ?3) ORDER BY event DESC LIMIT 1",
            )
            .expect("prep")
            .query_row(params![node, key, at], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?))
            })
            .optional()
            .expect("prop")
            .and_then(|(v, e, w, ret)| (ret == 0).then(|| (parse(v), e, w)))
    }

    fn props(&self, node: &str, at: Option<i64>) -> Vec<(String, Row)> {
        let mut st = self
            .conn
            .prepare_cached(
                "SELECT key, value, event, written, retracted FROM _nodes WHERE node = ?1
                 AND (?2 IS NULL OR written <= ?2) ORDER BY key, event DESC",
            )
            .expect("prep");
        let rows = st
            .query_map(params![node, at], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?, r.get::<_, i64>(4)?))
            })
            .expect("q");
        let mut out = Vec::new();
        let mut last: Option<String> = None;
        for r in rows {
            let (k, v, e, w, ret) = r.expect("row");
            if last.as_deref() == Some(k.as_str()) {
                continue;
            }
            last = Some(k.clone());
            if ret == 0 {
                out.push((k, (parse(v), e, w)));
            }
        }
        out
    }

    /// Every row of a node's key, oldest first, each marked current, replaced or retracted.
    fn history(&self, node: &str, key: &str, at: Option<i64>) -> Vec<Value> {
        let mut st = self
            .conn
            .prepare_cached(
                "SELECT value, event, written, retracted FROM _nodes WHERE node = ?1 AND key = ?2
                 AND (?3 IS NULL OR written <= ?3) ORDER BY event",
            )
            .expect("prep");
        let rows: Vec<(Option<String>, String, i64, i64)> = st
            .query_map(params![node, key, at], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .expect("q")
            .map(|r| r.expect("row"))
            .collect();
        let n = rows.len();
        rows.into_iter()
            .enumerate()
            .map(|(i, (v, e, w, ret))| {
                let state = if ret == 1 { "retracted" } else if i + 1 == n { "current" } else { "replaced" };
                json!({"value": parse(v), "event": e, "written": w, "state": state})
            })
            .collect()
    }

    /// Current edges from (or, reversed, into) a node, as of `at`, far node in id order.
    fn edges(&self, node: &str, name: &str, reverse: bool, at: Option<i64>) -> Vec<(String, Row)> {
        let sql = if reverse {
            "SELECT src, props, event, written, retracted FROM _edges WHERE dst = ?1 AND name = ?2
             AND (?3 IS NULL OR written <= ?3) ORDER BY src, event DESC"
        } else {
            "SELECT dst, props, event, written, retracted FROM _edges WHERE src = ?1 AND name = ?2
             AND (?3 IS NULL OR written <= ?3) ORDER BY dst, event DESC"
        };
        let mut st = self.conn.prepare_cached(sql).expect("prep");
        let rows = st
            .query_map(params![node, name, at], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?, r.get::<_, i64>(4)?))
            })
            .expect("q");
        let mut out = Vec::new();
        let mut last: Option<String> = None;
        for r in rows {
            let (far, p, e, w, ret) = r.expect("row");
            if last.as_deref() == Some(far.as_str()) {
                continue;
            }
            last = Some(far.clone());
            if ret == 0 {
                out.push((far, (parse(p), e, w)));
            }
        }
        out
    }

    fn value_json(&self, node: &str, key: &str, o: Opts) -> Option<Value> {
        if o.history {
            let h = self.history(node, key, o.at);
            (!h.is_empty()).then_some(Value::Array(h))
        } else {
            self.prop(node, key, o.at).map(|(v, e, w)| json!({"value": v, "event": e, "written": w}))
        }
    }

    /// describe: the node shaped by `shape`; every node it reads goes into `touched`.
    fn describe(&self, node: &str, shape: &Value, o: Opts, touched: &mut HashSet<String>) -> Value {
        touched.insert(node.to_string());
        let mut out = Map::new();
        out.insert("id".into(), json!(node));
        for item in shape.as_array().expect("shape is a list") {
            match item {
                Value::String(k) if k == "*" => {
                    for (key, _) in self.props(node, o.at) {
                        if let Some(v) = self.value_json(node, &key, o) {
                            out.insert(key, v);
                        }
                    }
                }
                Value::String(k) => {
                    if let Some(v) = self.value_json(node, k, o) {
                        out.insert(k.clone(), v);
                    }
                }
                Value::Object(m) => {
                    for (spec, sub) in m {
                        let (name, reverse) = spec.strip_suffix("<-").map_or((spec.as_str(), false), |n| (n, true));
                        let (subshape, limit, filter) = match sub {
                            Value::Array(_) => (sub, None, None),
                            Value::Object(x) => (
                                x.get("shape").expect("shape"),
                                x.get("limit").and_then(Value::as_u64).map(|l| l as usize),
                                x.get("where").and_then(Value::as_object),
                            ),
                            _ => panic!("bad shape"),
                        };
                        let mut kids = Vec::new();
                        for (far, (p, e, w)) in self.edges(node, name, reverse, o.at) {
                            if limit.is_some_and(|l| kids.len() >= l) {
                                break;
                            }
                            touched.insert(far.clone());
                            if let Some(f) = filter {
                                let ok = f.iter().all(|(k, want)| self.prop(&far, k, o.at).is_some_and(|(v, _, _)| &v == want));
                                if !ok {
                                    continue;
                                }
                            }
                            let mut child = self.describe(&far, subshape, o, touched);
                            child["edge"] = json!({"props": p, "event": e, "written": w});
                            kids.push(child);
                        }
                        out.insert(spec.clone(), Value::Array(kids));
                    }
                }
                _ => panic!("bad shape item"),
            }
        }
        Value::Object(out)
    }

    /// walk: one recursive SQL query. Returns (node, hops) nearest first, and whether cap cut it.
    fn walk(&self, root: &str, via: &[&str], hops: i64, cap: usize, at: Option<i64>) -> (Vec<(String, i64)>, bool) {
        let fwd: Vec<&str> = via.iter().filter(|v| !v.ends_with("<-")).copied().collect();
        let rev: Vec<&str> = via.iter().filter_map(|v| v.strip_suffix("<-")).collect();
        let mut args: Vec<Value> = vec![json!(root), json!(hops), at.map_or(Value::Null, |a| json!(a))];
        let mut branch = |names: &[&str], from: &str, to: &str| -> String {
            let marks: Vec<String> = names
                .iter()
                .map(|n| {
                    args.push(json!(n));
                    format!("?{}", args.len())
                })
                .collect();
            format!(
                "SELECT e.{to}, w.depth + 1 FROM w JOIN _edges e ON e.{from} = w.node
                 WHERE w.depth < ?2 AND e.name IN ({}) AND e.retracted = 0 AND (?3 IS NULL OR e.written <= ?3)
                   AND e.event = (SELECT max(f.event) FROM _edges f WHERE f.src = e.src AND f.name = e.name
                                  AND f.dst = e.dst AND (?3 IS NULL OR f.written <= ?3))",
                marks.join(", ")
            )
        };
        let mut parts = vec!["SELECT ?1, 0".to_string()];
        if !fwd.is_empty() {
            parts.push(branch(&fwd, "src", "dst"));
        }
        if !rev.is_empty() {
            parts.push(branch(&rev, "dst", "src"));
        }
        args.push(json!(cap as i64 + 1));
        let sql = format!(
            "WITH RECURSIVE w(node, depth) AS ({}) SELECT node, min(depth) AS d FROM w WHERE node <> ?1
             GROUP BY node ORDER BY d, node LIMIT ?{}",
            parts.join(" UNION "),
            args.len()
        );
        let sql_args: Vec<rusqlite::types::Value> = args
            .iter()
            .map(|v| match v {
                Value::Null => rusqlite::types::Value::Null,
                Value::Number(n) => rusqlite::types::Value::Integer(n.as_i64().expect("int")),
                Value::String(s) => rusqlite::types::Value::Text(s.clone()),
                _ => unreachable!(),
            })
            .collect();
        let mut st = self.conn.prepare(&sql).expect("walk sql");
        let mut got: Vec<(String, i64)> = st
            .query_map(params_from_iter(sql_args), |r| Ok((r.get(0)?, r.get(1)?)))
            .expect("walk")
            .map(|r| r.expect("row"))
            .collect();
        let cut = got.len() > cap;
        got.truncate(cap);
        (got, cut)
    }

    /// The same walk done in Rust, breadth first, over `edges`.
    fn bfs(&self, root: &str, via: &[&str], hops: i64, at: Option<i64>) -> Vec<(String, i64)> {
        let mut seen: HashMap<String, i64> = HashMap::from([(root.to_string(), 0)]);
        let mut q = VecDeque::from([(root.to_string(), 0i64)]);
        while let Some((n, d)) = q.pop_front() {
            if d == hops {
                continue;
            }
            for v in via {
                let (name, rev) = v.strip_suffix("<-").map_or((*v, false), |x| (x, true));
                for (far, _) in self.edges(&n, name, rev, at) {
                    if !seen.contains_key(&far) {
                        seen.insert(far.clone(), d + 1);
                        q.push_back((far, d + 1));
                    }
                }
            }
        }
        let mut out: Vec<(String, i64)> = seen.into_iter().filter(|(n, _)| n != root).collect();
        out.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        out
    }

    /// why: the current row's event, then its source chain through _events.
    fn why(&self, node: &str, key: &str) -> Vec<String> {
        let Some((_, event, _)) = self.prop(node, key, None) else { return vec![] };
        let mut st = self
            .conn
            .prepare_cached(
                "WITH RECURSIVE c(id, source, depth) AS (
                   SELECT id, source, 0 FROM _events WHERE id = ?1
                   UNION ALL SELECT e.id, e.source, c.depth + 1 FROM _events e JOIN c ON e.id = c.source)
                 SELECT id FROM c ORDER BY depth",
            )
            .expect("prep");
        st.query_map([event], |r| r.get(0)).expect("why").map(|r| r.expect("row")).collect()
    }

    fn search(&self, text: &str) -> Vec<String> {
        let mut st = self
            .conn
            .prepare_cached("SELECT DISTINCT m.node FROM _search s JOIN _search_map m ON m.row = s.rowid WHERE _search MATCH ?1 ORDER BY m.node")
            .expect("prep");
        st.query_map([text], |r| r.get(0)).expect("search").map(|r| r.expect("row")).collect()
    }
}

/// Keep the full-text index on the current text of (node, key).
fn reindex(tx: &rusqlite::Transaction, node: &str, key: &str) {
    let cur: Option<(Option<String>, i64)> = tx
        .query_row(
            "SELECT value, retracted FROM _nodes WHERE node = ?1 AND key = ?2 ORDER BY event DESC LIMIT 1",
            params![node, key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .expect("current");
    let old: Option<i64> = tx
        .query_row("SELECT row FROM _search_map WHERE node = ?1 AND key = ?2", params![node, key], |r| r.get(0))
        .optional()
        .expect("map");
    if let Some(row) = old {
        tx.execute("DELETE FROM _search WHERE rowid = ?1", [row]).expect("fts delete");
        tx.execute("DELETE FROM _search_map WHERE node = ?1 AND key = ?2", params![node, key]).expect("map delete");
    }
    if let Some((Some(v), 0)) = cur
        && let Value::String(text) = parse(Some(v))
    {
        tx.execute("INSERT INTO _search (text) VALUES (?1)", [text]).expect("fts insert");
        let row = tx.last_insert_rowid();
        tx.execute("INSERT INTO _search_map (node, key, row) VALUES (?1, ?2, ?3)", params![node, key, row]).expect("map insert");
    }
}

fn parse(v: Option<String>) -> Value {
    v.map_or(Value::Null, |s| serde_json::from_str(&s).expect("json value"))
}

// ---- warm hoods ------------------------------------------------------------------------

struct Hood {
    node: String,
    shape: Value,
    result: Value,
    touched: HashSet<String>,
    reads: u64,
}

#[derive(Default)]
struct Warm {
    hoods: HashMap<String, Hood>,
    by_node: HashMap<String, HashSet<String>>,
    hits: u64,
    misses: u64,
    recomputes: u64,
}

fn hood_key(node: &str, shape: &Value) -> String {
    format!("{node}\u{0}{shape}")
}

impl Warm {
    fn describe(&mut self, db: &Db, node: &str, shape: &Value) -> Value {
        let k = hood_key(node, shape);
        if let Some(h) = self.hoods.get_mut(&k) {
            h.reads += 1;
            self.hits += 1;
            return h.result.clone();
        }
        self.misses += 1;
        let mut touched = HashSet::new();
        let result = db.describe(node, shape, Opts::default(), &mut touched);
        for n in &touched {
            self.by_node.entry(n.clone()).or_default().insert(k.clone());
        }
        self.hoods.insert(k, Hood { node: node.into(), shape: shape.clone(), result: result.clone(), touched, reads: 1 });
        result
    }

    /// Recompute every warm hood that a write touched.
    fn rewarm(&mut self, db: &Db, touched: &HashSet<String>) {
        let keys: HashSet<String> = touched.iter().filter_map(|n| self.by_node.get(n)).flatten().cloned().collect();
        for k in keys {
            let h = self.hoods.get_mut(&k).expect("hood");
            for n in &h.touched {
                if let Some(s) = self.by_node.get_mut(n) {
                    s.remove(&k);
                }
            }
            let mut t = HashSet::new();
            h.result = db.describe(&h.node, &h.shape, Opts::default(), &mut t);
            for n in &t {
                self.by_node.entry(n.clone()).or_default().insert(k.clone());
            }
            h.touched = t;
            self.recomputes += 1;
        }
    }
}

// ---- the runs -----------------------------------------------------------------------------

fn verdict(ok: bool) -> &'static str {
    if ok { "HOLDS  " } else { "FAILS  " }
}

fn p50(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn s(x: &str) -> String {
    x.to_string()
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "data".into());
    std::fs::create_dir_all(&dir).expect("dir");

    // P0
    let fts_ok = Connection::open_in_memory().expect("mem").execute_batch("CREATE VIRTUAL TABLE t USING fts5(x)").is_ok();
    let ids: Vec<String> = (0..200_000).map(|_| Uuid::now_v7().to_string()).collect();
    let increasing = ids.windows(2).all(|w| w[0] < w[1]);
    println!("{}P0 FTS5 in rusqlite 0.41 bundled: {fts_ok}; 200,000 now_v7 ids strictly increase as strings: {increasing}", verdict(fts_ok && increasing));

    let mut db = Db::open(&format!("{dir}/small.sqlite"));
    let ev = |ms: u64, source: Option<&String>, ops: Vec<Op>| Ev { id: v7_at(ms), source: source.cloned(), ops };

    // The small graph: ada wrote doc1..doc3; doc1 cites doc2 and doc3.
    let e0 = ev(1_000, None, vec![]);
    let e1 = ev(1_100, Some(&e0.id), vec![
        Op::Set(s("ada"), s("type"), json!("person")),
        Op::Set(s("ada"), s("name"), json!("Ada")),
        Op::Set(s("doc1"), s("title"), json!("graph store notes")),
        Op::Set(s("doc1"), s("status"), json!("draft")),
        Op::Set(s("doc2"), s("title"), json!("queue notes")),
        Op::Set(s("doc2"), s("status"), json!("open")),
        Op::Set(s("doc3"), s("title"), json!("flow notes")),
        Op::Set(s("doc3"), s("status"), json!("closed")),
        Op::Link(s("ada"), s("wrote"), s("doc1"), json!({})),
        Op::Link(s("ada"), s("wrote"), s("doc2"), json!({})),
        Op::Link(s("ada"), s("wrote"), s("doc3"), json!({})),
        Op::Link(s("doc1"), s("cites"), s("doc2"), json!({"page": 3})),
        Op::Link(s("doc1"), s("cites"), s("doc3"), json!({})),
    ]);
    let e2 = ev(2_000, Some(&e1.id), vec![Op::Set(s("ada"), s("name"), json!("Ada Lovelace"))]);
    let e3 = ev(3_000, Some(&e2.id), vec![Op::Retract(s("doc3"), s("status"))]);
    let small = [e0.clone(), e1.clone(), e2.clone(), e3.clone()];
    for (e, t) in small.iter().zip([1_000, 1_100, 2_000, 3_000]) {
        db.clock = t;
        db.apply(e);
    }

    // P1
    let rows = |db: &Db| (db.count("SELECT count(*) FROM _nodes"), db.count("SELECT count(*) FROM _edges"));
    let before = rows(&db);
    db.clock = 9_999;
    for e in &small {
        db.apply(e);
    }
    let after = rows(&db);
    let moved = db.count("SELECT count(*) FROM _nodes WHERE written = 9999") + db.count("SELECT count(*) FROM _edges WHERE written = 9999");
    println!("{}P1 rerun: rows {before:?} -> {after:?}, rows stamped by the rerun: {moved}", verdict(before == after && moved == 0));

    // P2
    let early = ev(4_000, None, vec![Op::Set(s("k"), s("v"), json!("early")), Op::Set(s("k2"), s("v"), json!("early"))]);
    let late = ev(4_001, None, vec![Op::Set(s("k"), s("v"), json!("late")), Op::Set(s("k2"), s("v"), json!("late"))]);
    db.clock = 4_100;
    db.apply(&late);
    db.apply(&early);
    let k = db.prop("k", "v", None).map(|r| r.0);
    println!("{}P2 late applied before early: current {k:?} (expected \"late\")", verdict(k == Some(json!("late"))));

    // P3: d accepted at 5,000, runs at 9,000; b accepted at 6,000, runs at 6,000.
    let d = ev(5_000, None, vec![Op::Set(s("x"), s("K"), json!("d")), Op::Set(s("x"), s("L"), json!("d"))]);
    let b = ev(6_000, None, vec![Op::Set(s("x"), s("K"), json!("b"))]);
    db.clock = 6_000;
    db.apply(&b);
    db.clock = 9_000;
    db.apply(&d);
    db.clock = 10_000;
    let (k_then, l_then) = (db.prop("x", "K", Some(7_000)).map(|r| r.0), db.prop("x", "L", Some(7_000)).map(|r| r.0));
    let (k_now, l_now) = (db.prop("x", "K", None).map(|r| r.0), db.prop("x", "L", None).map(|r| r.0));
    let ok = k_then == Some(json!("b")) && l_then.is_none() && k_now == Some(json!("b")) && l_now == Some(json!("d"));
    println!("{}P3 read at 10,000 as of 7,000: K {k_then:?}, L {l_then:?}; now: K {k_now:?}, L {l_now:?}", verdict(ok));

    // P4
    let name_now = db.prop("ada", "name", None).map(|r| r.0);
    let name_then = db.prop("ada", "name", Some(1_500)).map(|r| r.0);
    let why = db.why("ada", "name");
    let why_ok = why == vec![e2.id.clone(), e1.id.clone(), e0.id.clone()];
    let st_now = db.prop("doc3", "status", None);
    let st_then = db.prop("doc3", "status", Some(2_500)).map(|r| r.0);
    let ok = name_now == Some(json!("Ada Lovelace")) && name_then == Some(json!("Ada")) && why_ok && st_now.is_none() && st_then == Some(json!("closed"));
    println!("{}P4 name now {name_now:?}, as of 1,500 {name_then:?}; why is e2, e1, e0: {why_ok}; doc3 status now {st_now:?}, as of 2,500 {st_then:?}", verdict(ok));

    // P5
    let mut t = HashSet::new();
    let o = Opts::default();
    let r_star = db.describe("ada", &json!(["*"]), o, &mut t);
    let star_ok = r_star.get("type").is_some() && r_star["name"]["value"] == json!("Ada Lovelace");
    let r_fwd = db.describe("ada", &json!([{"wrote": ["title"]}]), o, &mut t);
    let fwd_ok = r_fwd["wrote"].as_array().map(Vec::len) == Some(3);
    let r_rev = db.describe("doc1", &json!([{"wrote<-": ["name"]}]), o, &mut t);
    let rev_ok = r_rev["wrote<-"][0]["id"] == json!("ada");
    let r_lim = db.describe("ada", &json!([{"wrote": {"shape": ["title"], "limit": 2}}]), o, &mut t);
    let lim_ok = r_lim["wrote"].as_array().map(Vec::len) == Some(2);
    let r_where = db.describe("ada", &json!([{"wrote": {"shape": ["title"], "where": {"status": "open"}}}]), o, &mut t);
    let where_ok = r_where["wrote"].as_array().map(|a| a.iter().map(|c| c["id"].clone()).collect::<Vec<_>>()) == Some(vec![json!("doc2")]);
    let r_hist = db.describe("ada", &json!(["name"]), Opts { history: true, ..o }, &mut t);
    let states: Vec<Value> = r_hist["name"].as_array().map(|a| a.iter().map(|h| h["state"].clone()).collect()).unwrap_or_default();
    let hist_ok = states == vec![json!("replaced"), json!("current")];
    let r_at = db.describe("ada", &json!(["name", {"wrote": ["title"]}]), Opts { at: Some(1_500), ..o }, &mut t);
    let at_ok = r_at["name"]["value"] == json!("Ada");
    let stamps_ok = r_fwd["wrote"][0]["title"].get("event").is_some() && r_fwd["wrote"][0]["title"].get("written").is_some() && r_fwd["wrote"][0]["edge"].get("event").is_some();
    let nested = db.describe("ada", &json!(["name", {"wrote": ["title", {"cites": ["title"]}]}]), o, &mut t);
    let nested_ok = nested["wrote"][0]["cites"].as_array().map(Vec::len) == Some(2);
    let all = star_ok && fwd_ok && rev_ok && lim_ok && where_ok && hist_ok && at_ok && stamps_ok && nested_ok;
    println!("{}P5 describe: * {star_ok}, forwards {fwd_ok}, backwards {rev_ok}, limit {lim_ok}, where {where_ok}, history {states:?} {hist_ok}, at {at_ok}, event and written on values and edges {stamps_ok}, nested two levels {nested_ok}", verdict(all));
    println!("        example: {}", serde_json::to_string(&db.describe("doc1", &json!(["title", {"wrote<-": ["name"]}, {"cites": {"shape": ["title"], "limit": 1}}]), o, &mut t)).expect("json"));

    // P7
    let found = db.search("graph");
    db.clock = 11_000;
    db.apply(&ev(11_000, None, vec![Op::Set(s("doc1"), s("title"), json!("vector index notes"))]));
    let (old, new) = (db.search("graph"), db.search("vector"));
    let ok = found == vec![s("doc1")] && old.is_empty() && new == vec![s("doc1")];
    println!("{}P7 search \"graph\" before the correction {found:?}; after: \"graph\" {old:?}, \"vector\" {new:?}", verdict(ok));

    // ---- at scale ----
    let mut db = Db::open(&format!("{dir}/big.sqlite"));
    let (persons, docs) = (20_000usize, 80_000usize);
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut events: Vec<Ev> = Vec::with_capacity(persons + docs);
    for p in 0..persons {
        let id = format!("p{p}");
        let mut ops = vec![Op::Set(id.clone(), s("type"), json!("person")), Op::Set(id.clone(), s("name"), json!(format!("person {p}")))];
        if p > 0 {
            ops.push(Op::Link(id.clone(), s("knows"), format!("p{}", rng.below(p)), json!({})));
        }
        events.push(Ev { id: Uuid::now_v7().to_string(), source: None, ops });
    }
    for dn in 0..docs {
        let id = format!("d{dn}");
        let mut ops = vec![
            Op::Set(id.clone(), s("type"), json!("doc")),
            Op::Set(id.clone(), s("title"), json!(format!("doc {dn} about topic {}", dn % 97))),
            Op::Set(id.clone(), s("status"), json!(["open", "closed", "draft"][dn % 3])),
            Op::Link(format!("p{}", rng.below(persons)), s("wrote"), id.clone(), json!({})),
        ];
        for _ in 0..5.min(dn) {
            ops.push(Op::Link(id.clone(), s("cites"), format!("d{}", rng.below(dn)), json!({})));
        }
        events.push(Ev { id: Uuid::now_v7().to_string(), source: None, ops });
    }
    // One event per transaction for the first 2,000, then 1,000 per transaction to finish loading.
    let one = 2_000;
    let t0 = Instant::now();
    for (i, e) in events.iter().take(one).enumerate() {
        db.clock = i as i64;
        db.apply(e);
    }
    let per_event = t0.elapsed();
    let t1 = Instant::now();
    for (i, chunk) in events[one..].chunks(1_000).enumerate() {
        db.clock = (one + i * 1_000) as i64;
        db.apply_many(chunk);
    }
    let batched = t1.elapsed();
    let (nodes, edges) = (db.count("SELECT count(DISTINCT node) FROM _nodes"), db.count("SELECT count(*) FROM _edges"));
    let rate = one as f64 / per_event.as_secs_f64();
    println!(
        "        loaded {nodes} nodes, {edges} edges, {} node rows; one event per transaction: {rate:.0} events/s; 1,000 per transaction: {:.0} events/s",
        db.count("SELECT count(*) FROM _nodes"),
        (events.len() - one) as f64 / batched.as_secs_f64()
    );

    // P6
    let mut walk_ok = true;
    let mut cut_seen = false;
    for r in 0..50 {
        let root = format!("d{}", docs - 1 - r * 7);
        let via = ["cites", "wrote<-"];
        let full = db.bfs(&root, &via, 2, None);
        let (sql_full, cut_full) = db.walk(&root, &via, 2, 1_000_000, None);
        let (sql_cap, cut_cap) = db.walk(&root, &via, 2, 10, None);
        walk_ok &= sql_full == full && !cut_full && sql_cap.as_slice() == &full[..10.min(full.len())] && cut_cap == (full.len() > 10);
        cut_seen |= cut_cap;
    }
    println!("{}P6 walk (recursive SQL) equals a breadth-first walk in Rust on 50 roots, 2 hops over cites and wrote<-, and cap 10 cuts and says so: {walk_ok} (a cap cut seen: {cut_seen})", verdict(walk_ok && cut_seen));

    // P9 describe, cold and warm
    let shape = json!(["title", "status", {"wrote<-": ["name"]}, {"cites": {"shape": ["title", {"cites": {"shape": ["title"], "limit": 5}}], "limit": 5}}]);
    let roots: Vec<String> = (0..200).map(|i| format!("d{}", docs - 1 - i * 13)).collect();
    let mut cold = Vec::new();
    let mut sizes = Vec::new();
    for r in &roots {
        let mut t = HashSet::new();
        let st = Instant::now();
        let _ = db.describe(r, &shape, Opts::default(), &mut t);
        cold.push(st.elapsed());
        sizes.push(t.len());
    }
    let mut warm = Warm::default();
    for r in &roots {
        warm.describe(&db, r, &shape);
    }
    let mut hot = Vec::new();
    for r in &roots {
        let st = Instant::now();
        let _ = warm.describe(&db, r, &shape);
        hot.push(st.elapsed());
    }
    let (c50, h50) = (p50(cold), p50(hot));
    sizes.sort_unstable();
    println!("{}P9 cold describe, two levels, p50 {c50:?} ({} nodes p50); one event per transaction {rate:.0} events/s", verdict(c50 <= Duration::from_millis(5) && rate >= 1_000.0), sizes[sizes.len() / 2]);
    let ratio = c50.as_secs_f64() / h50.as_secs_f64();
    println!("        warm describe p50 {h50:?}, {ratio:.0}x faster than cold");

    // P8 a burst of 5,000 writes that touch warm hoods, then a rerun of the burst.
    let mut warm = Warm::default();
    let hoods: Vec<String> = roots.iter().take(50).cloned().collect();
    for r in &hoods {
        warm.describe(&db, r, &shape);
    }
    let pool: Vec<String> = {
        let mut v: Vec<String> = warm.hoods.values().flat_map(|h| h.touched.iter().cloned()).filter(|n| n.starts_with('d')).collect();
        v.sort();
        v.dedup();
        v
    };
    let mut burst = Vec::new();
    for i in 0..5_000 {
        let n = pool[rng.below(pool.len())].clone();
        let op = match i % 5 {
            0 => Op::Set(n, s("status"), json!(["open", "closed", "draft"][rng.below(3)])),
            1 => Op::Set(n.clone(), s("title"), json!(format!("retitled {i}"))),
            2 => Op::Link(n, s("cites"), format!("d{}", rng.below(docs)), json!({"burst": i})),
            3 => {
                let cited = db.edges(&n, "cites", false, None);
                if cited.is_empty() { Op::Retract(n, s("status")) } else { Op::Unlink(n, s("cites"), cited[rng.below(cited.len())].0.clone()) }
            }
            _ => Op::Retract(n, s("status")),
        };
        burst.push(Ev { id: Uuid::now_v7().to_string(), source: None, ops: vec![op] });
    }
    let tb = Instant::now();
    for (i, e) in burst.iter().enumerate() {
        db.clock = 200_000 + i as i64;
        let touched = db.apply(e);
        warm.rewarm(&db, &touched);
    }
    let burst_time = tb.elapsed();
    let equal = |warm: &Warm, db: &Db| {
        warm.hoods.values().filter(|h| {
            let mut t = HashSet::new();
            db.describe(&h.node, &h.shape, Opts::default(), &mut t) == h.result
        }).count()
    };
    let eq1 = equal(&warm, &db);
    let rows_before = rows(&db);
    let recomputes = warm.recomputes;
    db.clock = 900_000;
    for e in &burst {
        let touched = db.apply(e);
        warm.rewarm(&db, &touched);
    }
    let eq2 = equal(&warm, &db);
    let rows_after = rows(&db);
    let ok = eq1 == 50 && eq2 == 50 && rows_before == rows_after && warm.recomputes == recomputes && ratio >= 10.0;
    println!(
        "{}P8 warm equals cold after the burst: {eq1}/50; after its rerun: {eq2}/50; rerun added rows: {}; recomputes in the burst {recomputes}, in the rerun {}; burst of 5,000 one-event transactions with rewarm took {burst_time:?}",
        verdict(ok),
        rows_after.0 - rows_before.0 + rows_after.1 - rows_before.1,
        warm.recomputes - recomputes
    );

    // P10 a synthetic Zipf trace over 2,000 roots.
    let n_roots = 2_000usize;
    let weights: Vec<f64> = (1..=n_roots).map(|k| 1.0 / k as f64).collect();
    let total: f64 = weights.iter().sum();
    let mut cum = Vec::with_capacity(n_roots);
    let mut acc = 0.0;
    for w in &weights {
        acc += w / total;
        cum.push(acc);
    }
    let mut warm = Warm::default();
    for _ in 0..10_000 {
        let u = (rng.next() % 1_000_000) as f64 / 1_000_000.0;
        let i = cum.partition_point(|c| *c < u).min(n_roots - 1);
        warm.describe(&db, &format!("d{}", docs - 1 - i), &shape);
    }
    let waste = warm.hoods.values().filter(|h| h.reads == 1).count();
    let hit = warm.hits as f64 / (warm.hits + warm.misses) as f64;
    println!(
        "{}P10 synthetic Zipf trace, 10,000 describes over 2,000 roots: hits {} ({:.0}%), misses {}, hoods warmed and never read again {waste} of {}",
        verdict(hit >= 0.70),
        warm.hits,
        hit * 100.0,
        warm.misses,
        warm.hoods.len()
    );
    let size = std::fs::metadata(format!("{dir}/big.sqlite")).map(|m| m.len()).unwrap_or(0) + std::fs::metadata(format!("{dir}/big.sqlite-wal")).map(|m| m.len()).unwrap_or(0);
    println!("        file with its WAL: {} MB", size / 1_000_000);
}
