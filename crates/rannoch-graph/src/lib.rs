#![warn(missing_docs)]
//! Graph utils over the app's own ontology tables. rannoch-graph has no tables and defines no
//! shape: an app's shape is its ontology crate's tables, and every table and column a util reads
//! is one the app names in a step's options. The utils are why, capped walks, as-of reads and
//! warm hoods.
//!
//! A read is an event. Each util is a step, and an app reads by sending an event whose flow
//! calls it; the step gives the result, and the step's `next` returns it as an event of the
//! app's own. The step reaches the one store through rannoch-store's `Store::connection`, which
//! is only storage. The app adds [`SIGNATURES`] to the signatures its `build.rs` hands
//! `compile`, and [`actions`] to the actions it hands `Processor::new`:
//!
//! ```json
//! {"name": "reach", "ty": 50, "takes": {"node": "string"},
//!  "steps": [{"do": "walk",
//!    "with": {"walk": {"via": {"cites": {"from": "doc", "to": "cited"}}, "hops": 2, "cap": 10}},
//!    "next": {"ty": 60, "shape": {"walk": "$walk"}}}]}
//! ```
//!
//! # The steps
//!
//! Each takes `node`, a string: the key of the row it starts from.
//!
//! | Step | Options | What it gives |
//! |---|---|---|
//! | `as_of` | `{"table": T, "key": K, "time": W}`; it also takes `at`, a number | `row`: the row of T whose K is `node` with the greatest W at or before `at`, as an object of its columns, or null when it has none |
//! | `why` | `{"table": T, "key": K, "event": E}` | `why`: the greatest E among T's rows whose K is `node`, then that event's source chain through `_events`: its source, that event's source, and so on; empty when there is none in `_events` |
//! | `walk` | `{"via": {T: {"from": F, "to": D}, ..}, "hops": n, "cap": m}` | `walk`: `{"nodes": [{"id", "hops"}, ..], "cut": bool}`, each node reached from `node` over each T's rows from F to D, the root left out, once with its fewest hops: nearest first, then by id, at most m of them, and whether the cap cut the walk |
//!
//! A walk goes at most n hops. To follow a table the other way, swap its `from` and `to`. Node
//! ids are read as text. A column holding a blob is read as a list of its bytes.
//!
//! A step fails with its name and the reason, as `walk: no such table: nope`, when an option
//! does not parse or the store cannot answer.
//!
//! # Warm hoods
//!
//! Every walk is warm. The first walk of a node with given options reads the store and keeps
//! its result as a hood, and later walks of the same node and options return that copy. Make
//! one [`Hoods`], hand it to [`actions`], and wrap the app's append in a [`Warm`] with the same
//! hoods and the processor's writes:
//!
//! ```ignore
//! let hoods = Hoods::default();
//! let processor = Processor::new(FLOWS, rannoch_graph::actions(&hoods), conn)?;
//! let events = Events::new(conn, signal).with_writes(processor.writes());
//! let append = Warm::new(events, processor.writes(), hoods);
//! ```
//!
//! After the append it wraps commits a batch, the `Warm` drops every hood that a `store` row
//! of the batch touches: a row into one of a hood's via tables whose `from` column holds the
//! hood's root or a node its walk returned. It reads nothing: the next walk of that node and
//! those options reads the store, as a step of its read event, and keeps the result warm again.
//! A row the app writes by other means than the `store` step through the `Warm` touches no
//! hood, so its walks read as they were kept.

mod hoods;

pub use hoods::{Hoods, Warm};

use std::fmt::Display;
use std::sync::{Arc, LazyLock};

use rannoch_processor::{Action, Sig};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Result, params};
use serde_json::{Map, Value, json};

/// The steps' signatures; the app's build gathers them.
pub const SIGNATURES: &str = include_str!("../signatures.json");

/// The `Sig`s in [`SIGNATURES`]: `as_of`, `why` and `walk`.
static SIGS: LazyLock<[Sig; 3]> =
    LazyLock::new(|| serde_json::from_str(SIGNATURES).unwrap_or_default());

/// The crate's three steps, `as_of`, `why` and `walk`, whose walks are warm in `hoods`.
#[must_use]
pub fn actions(hoods: &Hoods) -> Vec<Arc<dyn Action>> {
    (0..3)
        .map(|util| Arc::new(Step(util, hoods.clone())) as Arc<dyn Action>)
        .collect()
}

/// One step: its index in [`SIGS`], and the hoods its walks are warm in.
struct Step(usize, Hoods);

impl Action for Step {
    fn sig(&self) -> &Sig {
        &SIGS[self.0]
    }

    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String> {
        let name = &self.sig().action;
        let fail = |e: &dyn Display| format!("{name}: {e}");
        let node = input
            .get("node")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let open = || rannoch_store::Store::connection().map_err(|e| fail(&e));
        let (field, value) = match self.0 {
            0 => {
                let at = input.get("at").and_then(Value::as_i64);
                let at = at.ok_or_else(|| fail(&"at is not a whole number of ms"))?;
                let [table, key, time] =
                    names(options, ["table", "key", "time"]).map_err(|e| fail(&e))?;
                let row = as_of(&open()?, table, key, time, node, at).map_err(|e| fail(&e))?;
                ("row", row)
            }
            1 => {
                let [table, key, event] =
                    names(options, ["table", "key", "event"]).map_err(|e| fail(&e))?;
                let why = why(&open()?, table, key, event, node).map_err(|e| fail(&e))?;
                ("why", json!(why))
            }
            _ => (
                "walk",
                self.1.walk(options, node, open).map_err(|e| fail(&e))?,
            ),
        };
        Ok(Map::from_iter([(field.to_owned(), value)]))
    }
}

/// The options' names `fields`, each a string.
fn names<'o, const N: usize>(
    options: &'o Value,
    fields: [&str; N],
) -> Result<[&'o str; N], String> {
    let mut out = [""; N];
    for (slot, field) in out.iter_mut().zip(fields) {
        *slot = options[field]
            .as_str()
            .ok_or_else(|| format!("{field} is not a name"))?;
    }
    Ok(out)
}

/// A name as an SQL identifier, quoted.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The row of `table` whose `key` is `node` with the greatest `time` at or before `at`, as an
/// object of its columns; null when it has none.
fn as_of(
    conn: &Connection,
    table: &str,
    key: &str,
    time: &str,
    node: &str,
    at: i64,
) -> Result<Value> {
    let (table, key, time) = (quote(table), quote(key), quote(time));
    let sql = format!(
        "SELECT * FROM {table} WHERE {key} = ?1 AND {time} <= ?2 ORDER BY {time} DESC LIMIT 1"
    );
    let mut st = conn.prepare_cached(&sql)?;
    let columns: Vec<String> = st.column_names().map(str::to_owned).collect();
    let mut rows = st.query(params![node, at])?;
    let Some(r) = rows.next()? else {
        return Ok(Value::Null);
    };
    let mut row = Map::new();
    for (i, column) in columns.into_iter().enumerate() {
        let value = match r.get_ref(i)? {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(n) => json!(n),
            ValueRef::Real(x) => json!(x),
            ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
            ValueRef::Blob(b) => json!(b),
        };
        row.insert(column, value);
    }
    Ok(Value::Object(row))
}

/// The greatest `event` among `table`'s rows whose `key` is `node`, then its source chain
/// through `_events`.
fn why(conn: &Connection, table: &str, key: &str, event: &str, node: &str) -> Result<Vec<String>> {
    let (table, key, event) = (quote(table), quote(key), quote(event));
    let sql = format!(
        "WITH RECURSIVE c(id, source, depth) AS (
        SELECT id, source, 0 FROM _events WHERE id = (SELECT max({event}) FROM {table} WHERE {key} = ?1)
        UNION ALL SELECT e.id, e.source, c.depth + 1 FROM _events e JOIN c ON e.id = c.source)
        SELECT id FROM c ORDER BY depth"
    );
    let mut st = conn.prepare_cached(&sql)?;
    let ids = st.query_map([node], |r| r.get(0))?;
    ids.collect()
}

/// An edge table a walk follows: the table, the column it goes from and the one it goes to.
pub(crate) type Via = (String, String, String);

/// The nodes reached from `root` over `via` in at most `hops` hops, nearest first as (node,
/// hops), at most `cap`, and whether the cap cut it.
pub(crate) fn walk(
    conn: &Connection,
    via: &[Via],
    root: &str,
    hops: i64,
    cap: usize,
) -> Result<(Vec<(String, i64)>, bool)> {
    let steps = via.iter().map(|(table, from, to)| {
        let (table, from, to) = (quote(table), quote(from), quote(to));
        format!(
            " UNION SELECT CAST(e.{to} AS TEXT), w.depth + 1 FROM w JOIN {table} e
            ON e.{from} = w.node WHERE w.depth < ?2"
        )
    });
    let sql = format!(
        "WITH RECURSIVE w(node, depth) AS (SELECT ?1, 0{})
        SELECT node, min(depth) AS d FROM w WHERE node <> ?1 GROUP BY node ORDER BY d, node
        LIMIT ?3",
        steps.collect::<String>()
    );
    // One past the cap, so a walk the cap cut tells itself from one that ended at it.
    let limit = i64::try_from(cap).map_or(i64::MAX, |cap| cap.saturating_add(1));
    let mut st = conn.prepare_cached(&sql)?;
    let rows = st.query_map(params![root, hops, limit], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let mut nodes = rows.collect::<Result<Vec<_>>>()?;
    let cut = nodes.len() > cap;
    nodes.truncate(cap);
    Ok((nodes, cut))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, VecDeque};

    use rusqlite::{Connection, params};
    use serde_json::json;
    use uuid::Uuid;

    use super::{Via, as_of, walk, why};

    /// An app's ontology, as its ontology crate's migrations make it, with names the app chose:
    /// docs with titles over time, and the docs each one cites.
    pub(crate) const DOCS: &str = "CREATE TABLE doc_titles (doc TEXT, title TEXT, event TEXT,
        written INTEGER, PRIMARY KEY (doc, event));
        CREATE TABLE cites (doc TEXT, cited TEXT, PRIMARY KEY (doc, cited));
        CREATE TABLE wrote (author TEXT, doc TEXT, PRIMARY KEY (author, doc))";

    /// The same ontology under another app's names.
    const PAPERS: &str = "CREATE TABLE paper (pid TEXT, name TEXT, by_event TEXT, at_ms INTEGER);
        CREATE TABLE refs (src TEXT, dst TEXT)";

    /// A store with rannoch's tables and the app's ontology `ddl`.
    pub(crate) fn store(ddl: &str) -> Connection {
        let conn = rannoch_core::forgr::empty_store();
        conn.execute_batch(ddl).unwrap();
        conn
    }

    /// An event in `_events` with `source`; returns its id.
    fn event(conn: &Connection, source: Option<&str>) -> String {
        let id = Uuid::now_v7().to_string();
        let sql = "INSERT INTO _events (id, source, ty, data, run_at, status)
            VALUES (?1, ?2, 1, '{}', NULL, 0)";
        conn.execute(sql, params![id, source]).unwrap();
        id
    }

    fn via(table: &str, from: &str, to: &str) -> Via {
        (table.to_owned(), from.to_owned(), to.to_owned())
    }

    /// d1 cites d2 and d3, d2 cites d4, d4 cites d1; ada wrote d1 and d4. d1 is titled at
    /// 1,000 by its first event, and retitled at 2,000 by an event whose source is the first.
    fn small(conn: &Connection, [titles, cites, wrote]: [&str; 3]) -> [String; 2] {
        let first = event(conn, None);
        let fix = event(conn, Some(&first));
        let title = format!("INSERT INTO {titles} VALUES (?1, ?2, ?3, ?4)");
        conn.execute(&title, params!["d1", "graph notes", first, 1_000])
            .unwrap();
        conn.execute(&title, params!["d1", "graph store notes", fix, 2_000])
            .unwrap();
        let cite = format!("INSERT INTO {cites} VALUES (?1, ?2)");
        for (a, b) in [("d1", "d2"), ("d1", "d3"), ("d2", "d4"), ("d4", "d1")] {
            conn.execute(&cite, [a, b]).unwrap();
        }
        if !wrote.is_empty() {
            let wrote = format!("INSERT INTO {wrote} VALUES (?1, ?2)");
            for doc in ["d1", "d4"] {
                conn.execute(&wrote, ["ada", doc]).unwrap();
            }
        }
        [first, fix]
    }

    /// FR-RANNOCH-125-c3: why, a capped walk and an as-of read, over the app's ontology tables.
    #[test]
    fn fr_rannoch_125_c3() {
        let conn = store(DOCS);
        let [first, fix] = small(&conn, ["doc_titles", "cites", "wrote"]);

        // As of: the row at or before the time, or none before the first.
        let title = |at| as_of(&conn, "doc_titles", "doc", "written", "d1", at).unwrap();
        assert_eq!(title(1_500)["title"], json!("graph notes"));
        assert_eq!(title(9_999)["title"], json!("graph store notes"));
        assert_eq!(title(9_999)["event"], json!(fix));
        assert_eq!(title(999), json!(null));

        // Why: the latest row's event, then its source chain.
        assert_eq!(
            why(&conn, "doc_titles", "doc", "event", "d1").unwrap(),
            [fix, first]
        );
        assert!(
            why(&conn, "doc_titles", "doc", "event", "none")
                .unwrap()
                .is_empty()
        );

        // A walk over cites and wrote backwards, each node once at its fewest hops; capped.
        let both = [via("cites", "doc", "cited"), via("wrote", "doc", "author")];
        let (nodes, cut) = walk(&conn, &both, "d1", 2, 10).unwrap();
        let hops = |n: &[(String, i64)]| n.iter().map(|(n, h)| (n.clone(), *h)).collect::<Vec<_>>();
        let want = [("ada", 1), ("d2", 1), ("d3", 1), ("d4", 2)].map(|(n, h)| (n.to_owned(), h));
        assert_eq!((hops(&nodes), cut), (want.to_vec(), false));
        assert_eq!(
            walk(&conn, &both, "d1", 2, 2).unwrap(),
            (want[..2].to_vec(), true)
        );
        assert!(!walk(&conn, &both, "d1", 2, 4).unwrap().1);
    }

    /// 01a216c8-b0d1-76f2-b5b2-b39d928247e3: the same utils on another app's tables and
    /// columns give the same answers.
    #[test]
    fn criterion_01a216c8_utils_work_on_variable_tables_and_columns() {
        let (docs, papers) = (store(DOCS), store(PAPERS));
        let [_, fix] = small(&docs, ["doc_titles", "cites", "wrote"]);
        let [_, fixed] = small(&papers, ["paper", "refs", ""]);

        let title = as_of(&docs, "doc_titles", "doc", "written", "d1", 1_500).unwrap();
        let name = as_of(&papers, "paper", "pid", "at_ms", "d1", 1_500).unwrap();
        assert_eq!(
            (&title["title"], &name["name"]),
            (&json!("graph notes"), &json!("graph notes"))
        );
        assert_eq!(
            why(&docs, "doc_titles", "doc", "event", "d1").unwrap()[0],
            fix
        );
        assert_eq!(
            why(&papers, "paper", "pid", "by_event", "d1").unwrap()[0],
            fixed
        );
        let reached = |conn, v: Via| walk(conn, &[v], "d1", 3, 10).unwrap();
        assert_eq!(
            reached(&docs, via("cites", "doc", "cited")),
            reached(&papers, via("refs", "src", "dst"))
        );
    }

    /// The toy's xorshift, so the graph below is the toy's.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            usize::try_from(self.0 % u64::try_from(n).unwrap()).unwrap()
        }
    }

    /// The same walk done in Rust, breadth first, one row at a time.
    fn bfs(conn: &Connection, root: &str, via: &[Via], hops: i64) -> Vec<(String, i64)> {
        let mut seen = HashMap::from([(root.to_owned(), 0)]);
        let mut queue = VecDeque::from([(root.to_owned(), 0)]);
        while let Some((node, depth)) = queue.pop_front() {
            if depth == hops {
                continue;
            }
            for (table, from, to) in via {
                let sql = format!("SELECT {to} FROM {table} WHERE {from} = ?1");
                let mut st = conn.prepare(&sql).unwrap();
                let far = st.query_map([&node], |r| r.get::<_, String>(0)).unwrap();
                for far in far.map(Result::unwrap) {
                    seen.entry(far.clone()).or_insert_with(|| {
                        queue.push_back((far, depth + 1));
                        depth + 1
                    });
                }
            }
        }
        let mut out: Vec<_> = seen.into_iter().filter(|(n, _)| n != root).collect();
        out.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        out
    }

    /// A capped walk equals a breadth-first walk in Rust on 50 roots of a graph of 2,000 people
    /// and 8,000 docs, 2 hops over cites and wrote backwards, and a cap of 10 cuts and says so.
    #[test]
    fn a_capped_walk_equals_a_breadth_first_walk() {
        let (people, docs) = (2_000, 8_000);
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut conn = store(DOCS);
        let tx = conn.transaction().unwrap();
        for d in 0..docs {
            let doc = format!("d{d}");
            let author = format!("p{}", rng.below(people));
            tx.execute("INSERT INTO wrote VALUES (?1, ?2)", [&author, &doc])
                .unwrap();
            for _ in 0..d.min(5) {
                let cited = format!("d{}", rng.below(d));
                tx.execute(
                    "INSERT OR IGNORE INTO cites VALUES (?1, ?2)",
                    [&doc, &cited],
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
        let both = [via("cites", "doc", "cited"), via("wrote", "doc", "author")];
        let mut cut_seen = false;
        for r in 0..50 {
            let root = format!("d{}", docs - 1 - r * 7);
            let full = bfs(&conn, &root, &both, 2);

            let whole = walk(&conn, &both, &root, 2, 1_000_000).unwrap();
            let (capped, cut) = walk(&conn, &both, &root, 2, 10).unwrap();

            assert_eq!(whole, (full.clone(), false), "root {root}");
            assert_eq!(capped, full[..10.min(full.len())], "root {root}");
            assert_eq!(cut, full.len() > 10, "root {root}");
            cut_seen |= cut;
        }
        assert!(cut_seen);
    }
}
