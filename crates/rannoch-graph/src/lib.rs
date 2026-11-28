#![warn(missing_docs)]
//! Graph utils over the app's own ontology tables. rannoch-graph has no tables and defines no
//! shape: an app's shape is its ontology crate's tables, and every table and column a util reads
//! is one the app names in a step's options. The utils are why, capped walks, as-of reads,
//! warm hoods and the scoring tools.
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
//! | `as_of` | `{"table": T, "key": K, "time": W}`; it also takes `at`, a number | `row`: the row of T whose K is `node` with the greatest W at or before `at`, as an object of its columns, or null when it has none. A W holding text is recorded time, the writing event's v7 id, read at or before the greatest v7 id of `at`; a W holding a number is valid time, in ms |
//! | `why` | `{"table": T, "key": K, "event": E}` | `why`: the greatest E among T's rows whose K is `node`, then that event's source chain through `_events`: its source, that event's source, and so on; empty when there is none in `_events` |
//! | `walk` | `{"via": {T: {"from": F, "to": D, "time": W}, ..}, "hops": n, "cap": m}`, `time` optional; it also reads `at`, a number, when the event holds one | `walk`: `{"nodes": [{"id", "hops"}, ..], "cut": bool}`, each node reached from `node` over each T's rows from F to D, the root left out, once with its fewest hops: nearest first, then by id, at most m of them, and whether the cap cut the walk |
//! | `edges` | `{"table": T, "from": F, "time": W}`; it also takes `at`, a number | `edges`: `{"rows": [..]}`, the rows of T whose F is `node` and whose W is at or before `at`, oldest first, each an object of its columns |
//! | `katz` | `{"via": {T: {"from": F, "to": D, "weight": X}, ..}, "hops": n, "beta": b, "cap": m}` | `scores`: each node's Katz score from `node`, the sum over walks of 1 to n rows of b to the walk's length |
//! | `pagerank` | `{"via": .., "hops": n, "restart": a, "cap": m}` | `scores`: each node's personalized page rank from `node`, a random walk that restarts at `node` with chance a, run n hops |
//! | `shared` | `{"via": .., "rare": r, "cap": m}` | `scores`: each node's shared neighbours with `node`; with r true, each counts 1 over the log of its degree (Adamic-Adar) |
//! | `learned` | `{"via": .., "cap": m}` | `scores`: each node's learned weight from `node`, the sum of X over the rows between them |
//!
//! A walk goes at most n hops. To follow a table the other way, swap its `from` and `to`. A walk
//! with `at` is as of `at`: it follows only the rows of a via table that names `time` whose W is
//! at or before `at`, and a via table with no `time` whole. Node ids are read as text. A column
//! holding a blob is read as a list of its bytes.
//!
//! The scoring tools read graph structure alone: no vectors, and no score from outside the
//! graph. Each reads each via table both ways, from F to D and from D to F, and counts a row by
//! its X, or by 1 when the table names no `weight`; the app writes the learned weights into its
//! own table. Each gives `scores`: `{"nodes": [{"id", "score"}, ..], "cut": bool}`, the nodes it
//! scores, `node` left out, highest first, then by id, at most m of them, and whether the cap
//! cut them.
//!
//! A step fails with its name and the reason, as `walk: no such table: nope`, when an option
//! or what it takes does not parse, or the store cannot answer.
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
//! hood, so its walks read as they were kept. A walk that runs after the append commits a
//! batch and before the `Warm` drops its hoods may still return the kept result.

mod hoods;
mod queries;
mod score;

pub use hoods::{Hoods, Warm};
use score::Scorer;

use std::fmt::Display;
use std::sync::{Arc, LazyLock};

use rannoch_processor::{Action, Sig};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Result, params};
use serde_json::{Map, Value, json};
use uuid::Builder;

/// The steps' signatures; the app's build gathers them.
pub const SIGNATURES: &str = include_str!("../signatures.json");

/// The `Sig`s in [`SIGNATURES`]: `as_of`, `why`, `walk`, `edges`, `katz`, `pagerank`, `shared`
/// and `learned`.
static SIGS: LazyLock<[Sig; 8]> =
    LazyLock::new(|| serde_json::from_str(SIGNATURES).unwrap_or_default());

/// The crate's steps, `as_of`, `why`, `walk` and `edges`, whose walks are warm in `hoods`, and
/// the scoring tools `katz`, `pagerank`, `shared` and `learned`.
#[must_use]
pub fn actions(hoods: &Hoods) -> Vec<Arc<dyn Action>> {
    (0..8)
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
        let node = input.get("node").and_then(Value::as_str);
        let node = node.ok_or_else(|| fail(&"node is not a string"))?;
        let open = || rannoch_store::Store::connection().map_err(|e| fail(&e));
        let at = || {
            let at = input.get("at").map(Value::as_i64);
            at.map(|at| at.ok_or_else(|| fail(&"at is not a whole number of ms")))
                .transpose()
        };
        let needed = || at()?.ok_or_else(|| fail(&"at is not a whole number of ms"));
        let (field, value) = match self.0 {
            0 => {
                let at = needed()?;
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
            2 => {
                let walked = self.1.walk(options, node, at()?, open);
                ("walk", walked.map_err(|e| fail(&e))?)
            }
            3 => {
                let at = needed()?;
                let [table, from, time] =
                    names(options, ["table", "from", "time"]).map_err(|e| fail(&e))?;
                let rows = edges(&open()?, table, from, time, node, at).map_err(|e| fail(&e))?;
                ("edges", json!({ "rows": rows }))
            }
            util => {
                let tools = [
                    Scorer::Katz,
                    Scorer::PageRank,
                    Scorer::Shared,
                    Scorer::Learned,
                ];
                let scored = score::score(&open()?, tools[util - 4], options, node);
                ("scores", scored.map_err(|e| fail(&e))?)
            }
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
/// object of its columns; null when it has none. A text `time` is a v7 id, bound by the
/// greatest v7 id of `at`; any other is `at` itself.
fn as_of(
    conn: &Connection,
    table: &str,
    key: &str,
    time: &str,
    node: &str,
    at: i64,
) -> Result<Value> {
    let (table, key, time) = (quote(table), quote(key), quote(time));
    let sql = queries::as_of(&table, &key, &time);
    // The greatest v7 id of ms `at`; before 1970 no id is at or before it.
    let bound = u64::try_from(at).map_or_else(
        |_| String::new(),
        |at| {
            let at = at.min(0xffff_ffff_ffff);
            Builder::from_unix_timestamp_millis(at, &[0xff; 10])
                .into_uuid()
                .to_string()
        },
    );
    let mut st = conn.prepare_cached(&sql)?;
    let columns: Vec<String> = st.column_names().map(str::to_owned).collect();
    let mut rows = st.query(params![node, at, bound])?;
    rows.next()?
        .map_or(Ok(Value::Null), |r| object(&columns, r))
}

/// The rows of `table` whose `from` is `node` and whose `time` is at or before `at`, oldest
/// first, each as an object of its columns.
fn edges(
    conn: &Connection,
    table: &str,
    from: &str,
    time: &str,
    node: &str,
    at: i64,
) -> Result<Vec<Value>> {
    let (table, from, time) = (quote(table), quote(from), quote(time));
    let sql = queries::edges(&table, &from, &time);
    let mut st = conn.prepare_cached(&sql)?;
    let columns: Vec<String> = st.column_names().map(str::to_owned).collect();
    let mut rows = st.query(params![node, at])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        out.push(object(&columns, r)?);
    }
    Ok(out)
}

/// A row as an object of its `columns`.
fn object(columns: &[String], r: &rusqlite::Row<'_>) -> Result<Value> {
    let mut row = Map::new();
    for (i, column) in columns.iter().enumerate() {
        let value = match r.get_ref(i)? {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(n) => json!(n),
            ValueRef::Real(x) => json!(x),
            ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
            ValueRef::Blob(b) => json!(b),
        };
        row.insert(column.clone(), value);
    }
    Ok(Value::Object(row))
}

/// The greatest `event` among `table`'s rows whose `key` is `node`, then its source chain
/// through `_events`.
fn why(conn: &Connection, table: &str, key: &str, event: &str, node: &str) -> Result<Vec<String>> {
    let (table, key, event) = (quote(table), quote(key), quote(event));
    let sql = queries::why(&table, &key, &event);
    let mut st = conn.prepare_cached(&sql)?;
    let ids = st.query_map([node], |r| r.get(0))?;
    ids.collect()
}

/// An edge table a walk or a scorer follows: the table, the column it goes from, the one it
/// goes to, and the column of its rows' time for a walk, or of their weight for a scorer, when
/// it names one.
pub(crate) type Via = (String, String, String, Option<String>);

/// The options' `via` tables, each with its `from` and `to` columns and its `fourth` column
/// when it names one.
pub(crate) fn vias(options: &Value, fourth: &str) -> Result<Vec<Via>, String> {
    let via = options["via"].as_object().ok_or("via is not an object")?;
    let via = via.iter().map(|(table, cols)| {
        let col = |c: &str| cols[c].as_str().map(str::to_owned);
        let not = || format!("via {table} has no from and to");
        Ok((
            table.clone(),
            col("from").ok_or_else(not)?,
            col("to").ok_or_else(not)?,
            col(fourth),
        ))
    });
    via.collect()
}

/// The options' `hops`.
pub(crate) fn hops(options: &Value) -> Result<i64, String> {
    options["hops"]
        .as_i64()
        .ok_or_else(|| "hops is not a whole number".to_owned())
}

/// The options' `cap`.
pub(crate) fn cap(options: &Value) -> Result<usize, String> {
    let cap = options["cap"].as_u64().ok_or("cap is not a whole number")?;
    Ok(usize::try_from(cap).unwrap_or(usize::MAX))
}

/// The nodes reached from `root` over `via` in at most `hops` hops, as of `at` when given,
/// nearest first as (node, hops), at most `cap`, and whether the cap cut it.
pub(crate) fn walk(
    conn: &Connection,
    via: &[Via],
    root: &str,
    hops: i64,
    cap: usize,
    at: Option<i64>,
) -> Result<(Vec<(String, i64)>, bool)> {
    let steps = via.iter().map(|(table, from, to, time)| {
        let (table, from, to) = (quote(table), quote(from), quote(to));
        queries::walk_step(&table, &from, &to, time.as_deref().map(quote).as_deref())
    });
    let sql = queries::walk(&steps.collect::<String>());
    // One past the cap, so a walk the cap cut tells itself from one that ended at it.
    let limit = i64::try_from(cap).map_or(i64::MAX, |cap| cap.saturating_add(1));
    let mut st = conn.prepare_cached(&sql)?;
    let args = params![root, at, hops, limit];
    let rows = st.query_map(args, |r| Ok((r.get(0)?, r.get(1)?)))?;
    let mut nodes = rows.collect::<Result<Vec<_>>>()?;
    let cut = nodes.len() > cap;
    nodes.truncate(cap);
    Ok((nodes, cut))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::time::Duration;

    use rusqlite::{Connection, params};
    use serde_json::json;

    use super::score::{Scorer, score};
    use super::{Via, as_of, edges, walk, why};

    /// An app's ontology, as its ontology crate's migrations make it, with names the app chose:
    /// docs with titles over time, and the docs each one cites, from when.
    pub(crate) const DOCS: &str = "CREATE TABLE doc_titles (doc TEXT, title TEXT, event TEXT,
        written INTEGER, PRIMARY KEY (doc, event));
        CREATE TABLE cites (doc TEXT, cited TEXT, written INTEGER, PRIMARY KEY (doc, cited));
        CREATE TABLE wrote (author TEXT, doc TEXT, PRIMARY KEY (author, doc))";

    /// The same ontology under another app's names.
    const PAPERS: &str = "CREATE TABLE paper (pid TEXT, name TEXT, by_event TEXT, at_ms INTEGER);
        CREATE TABLE refs (src TEXT, dst TEXT, at_ms INTEGER)";

    /// A store with rannoch's tables and the app's ontology `ddl`.
    pub(crate) fn store(ddl: &str) -> Connection {
        let conn = rannoch_core::forgr::empty_store();
        conn.execute_batch(ddl).unwrap();
        conn
    }

    /// An event in `_events` with `source`; returns its id.
    fn event(conn: &Connection, source: Option<&str>) -> String {
        let id = rannoch_core::clock::new_id().to_string();
        let sql = "INSERT INTO _events (id, source, ty, data, run_at, status)
            VALUES (?1, ?2, 1, '{}', NULL, 0)";
        conn.execute(sql, params![id, source]).unwrap();
        id
    }

    fn via(table: &str, from: &str, to: &str) -> Via {
        (table.to_owned(), from.to_owned(), to.to_owned(), None)
    }

    /// d1 cites d2 and d3, d2 cites d4, d4 cites d1, each at 1,000 but d1's cite of d3 at 2,000;
    /// ada wrote d1 and d4. d1 is titled at 1,000 by its first event, and retitled at 2,000 by
    /// an event whose source is the first.
    /// The two events' ids carry times a millisecond apart, so the fix's id is the greater.
    async fn small(conn: &Connection, [titles, cites, wrote]: [&str; 3]) -> [String; 2] {
        let first = event(conn, None);
        tokio::time::advance(Duration::from_millis(1)).await;
        let fix = event(conn, Some(&first));
        let title = format!("INSERT INTO {titles} VALUES (?1, ?2, ?3, ?4)");
        conn.execute(&title, params!["d1", "graph notes", first, 1_000])
            .unwrap();
        conn.execute(&title, params!["d1", "graph store notes", fix, 2_000])
            .unwrap();
        let cite = format!("INSERT INTO {cites} VALUES (?1, ?2, ?3)");
        let rows = [("d1", "d2", 1_000), ("d1", "d3", 2_000)];
        for (a, b, at) in rows
            .into_iter()
            .chain([("d2", "d4", 1_000), ("d4", "d1", 1_000)])
        {
            conn.execute(&cite, params![a, b, at]).unwrap();
        }
        if !wrote.is_empty() {
            let wrote = format!("INSERT INTO {wrote} VALUES (?1, ?2)");
            for doc in ["d1", "d4"] {
                conn.execute(&wrote, ["ada", doc]).unwrap();
            }
        }
        [first, fix]
    }

    /// FR-RANNOCH-125-c3: why, a capped walk and as-of reads, of a row, a walk and a node's
    /// edges, over the app's ontology tables.
    #[tokio::test(start_paused = true)]
    async fn fr_rannoch_125_c3() {
        let conn = store(DOCS);
        let [first, fix] = small(&conn, ["doc_titles", "cites", "wrote"]).await;

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
        assert_eq!(
            why(&conn, "doc_titles", "doc", "event", "none").unwrap(),
            Vec::<String>::new()
        );

        // A walk over cites and wrote backwards, each node once at its fewest hops; capped.
        let both = [via("cites", "doc", "cited"), via("wrote", "doc", "author")];
        let (nodes, cut) = walk(&conn, &both, "d1", 2, 10, None).unwrap();
        let hops = |n: &[(String, i64)]| n.iter().map(|(n, h)| (n.clone(), *h)).collect::<Vec<_>>();
        let want = [("ada", 1), ("d2", 1), ("d3", 1), ("d4", 2)].map(|(n, h)| (n.to_owned(), h));
        assert_eq!((hops(&nodes), cut), (want.to_vec(), false));
        assert_eq!(
            walk(&conn, &both, "d1", 2, 2, None).unwrap(),
            (want[..2].to_vec(), true)
        );
        assert!(!walk(&conn, &both, "d1", 2, 4, None).unwrap().1);

        // An as-of walk follows the cites written by then; wrote names no time and is whole.
        let mut timed = both.clone();
        timed[0].3 = Some("written".to_owned());
        let then = walk(&conn, &timed, "d1", 2, 10, Some(1_500)).unwrap().0;
        let want_then = [("ada", 1), ("d2", 1), ("d4", 2)].map(|(n, h)| (n.to_owned(), h));
        assert_eq!(hops(&then), want_then);
        let now = walk(&conn, &timed, "d1", 2, 10, Some(9_999)).unwrap();
        assert_eq!(now, (want.to_vec(), false));
        assert_eq!(walk(&conn, &timed, "d1", 2, 10, None).unwrap(), now);

        // A node's edges as of a time: the rows written by then, oldest first.
        let cited = |at| {
            let rows = edges(&conn, "cites", "doc", "written", "d1", at).unwrap();
            rows.iter().map(|r| r["cited"].clone()).collect::<Vec<_>>()
        };
        assert_eq!(cited(999), Vec::<serde_json::Value>::new());
        assert_eq!(cited(1_500), [json!("d2")]);
        assert_eq!(cited(9_999), [json!("d2"), json!("d3")]);
    }

    /// d1 titled "graph notes" by an event at 1,000, valid from 1,000, and retitled "graph store
    /// notes" by an event at 2,000, valid from 1,500: learned late.
    fn late(conn: &Connection) {
        let title = "INSERT INTO doc_titles VALUES ('d1', ?1, ?2, ?3)";
        for (name, recorded, valid) in [
            ("graph notes", 1_000, 1_000),
            ("graph store notes", 2_000, 1_500),
        ] {
            let id = uuid::Builder::from_unix_timestamp_millis(recorded, &[0; 10]).into_uuid();
            conn.execute(title, params![name, id.to_string(), valid])
                .unwrap();
        }
    }

    /// 01a219a7-4cc4-7d50-b375-8fdf0d724ded: an as-of read on the recorded-time column, the
    /// writing event's v7 id, returns the latest row whose id is at or before t's v7 bound.
    #[tokio::test(start_paused = true)]
    async fn criterion_01a219a7_as_of_by_recorded_time() {
        let conn = store(DOCS);
        late(&conn);
        let title =
            |at| as_of(&conn, "doc_titles", "doc", "event", "d1", at).unwrap()["title"].clone();
        assert_eq!(title(-1), json!(null));
        assert_eq!(title(999), json!(null));
        assert_eq!(title(1_000), json!("graph notes"));
        assert_eq!(title(1_999), json!("graph notes"));
        assert_eq!(title(2_000), json!("graph store notes"));
    }

    /// 01a219a7-4cc4-7d50-b375-8feda7bb8f31: an as-of read on the valid-time column returns
    /// the row valid at t, whenever it was recorded.
    #[tokio::test(start_paused = true)]
    async fn criterion_01a219a7_as_of_by_valid_time() {
        let conn = store(DOCS);
        late(&conn);
        let title =
            |at| as_of(&conn, "doc_titles", "doc", "written", "d1", at).unwrap()["title"].clone();
        assert_eq!(title(999), json!(null));
        assert_eq!(title(1_000), json!("graph notes"));
        assert_eq!(title(1_499), json!("graph notes"));
        assert_eq!(title(1_500), json!("graph store notes"));
    }

    /// 01a216c8-b0d1-76f2-b5b2-b39d928247e3: the same utils on another app's tables and
    /// columns give the same answers.
    #[tokio::test(start_paused = true)]
    async fn criterion_01a216c8_utils_work_on_variable_tables_and_columns() {
        let (docs, papers) = (store(DOCS), store(PAPERS));
        let [_, fix] = small(&docs, ["doc_titles", "cites", "wrote"]).await;
        let [_, fixed] = small(&papers, ["paper", "refs", ""]).await;

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
        let reached = |conn, v: Via| walk(conn, &[v], "d1", 3, 10, None).unwrap();
        assert_eq!(
            reached(&docs, via("cites", "doc", "cited")),
            reached(&papers, via("refs", "src", "dst"))
        );
    }

    /// 01a216c8-b138-7ce1-8599-45d05b451b2c: the scoring tools on another app's tables and
    /// columns give the same scores.
    #[tokio::test(start_paused = true)]
    async fn criterion_01a216c8_scoring_tools_work_on_variable_tables_and_columns() {
        let (docs, papers) = (store(DOCS), store(PAPERS));
        small(&docs, ["doc_titles", "cites", "wrote"]).await;
        small(&papers, ["paper", "refs", ""]).await;
        let scored = |conn, scorer, options: serde_json::Value| {
            let scores = score(conn, scorer, &options, "d1").unwrap();
            let nodes = scores["nodes"].as_array().unwrap().iter();
            let nodes = nodes.map(|n| (n["id"].as_str().unwrap().to_owned(), n["score"].as_f64()));
            (
                nodes.map(|(id, s)| (id, s.unwrap())).collect::<Vec<_>>(),
                scores["cut"].clone(),
            )
        };
        let ln2 = 2f64.ln().recip();
        // d1 meets d2, d3 and d4, d2 meets d4, read both ways; a weighed case's row weighs its
        // time: d1 to d3 2,000, the rest 1,000.
        let cases = [
            (
                Scorer::Katz,
                false,
                json!({ "hops": 2, "beta": 0.5, "cap": 10 }),
                vec![("d2", 0.75), ("d4", 0.75), ("d3", 0.5)],
            ),
            (
                Scorer::Katz,
                true,
                json!({ "hops": 2, "beta": 0.0005, "cap": 10 }),
                vec![("d3", 1.0), ("d2", 0.75), ("d4", 0.75)],
            ),
            (
                Scorer::PageRank,
                false,
                json!({ "hops": 2, "restart": 0.5, "cap": 10 }),
                vec![("d2", 5.0 / 48.0), ("d4", 5.0 / 48.0), ("d3", 1.0 / 12.0)],
            ),
            (
                Scorer::PageRank,
                true,
                json!({ "hops": 2, "restart": 0.5, "cap": 10 }),
                vec![("d3", 0.125), ("d2", 5.0 / 64.0), ("d4", 5.0 / 64.0)],
            ),
            (
                Scorer::Shared,
                false,
                json!({ "rare": false, "cap": 10 }),
                vec![("d2", 1.0), ("d4", 1.0)],
            ),
            (
                Scorer::Shared,
                false,
                json!({ "rare": true, "cap": 1 }),
                vec![("d2", ln2)],
            ),
            (
                Scorer::Shared,
                true,
                json!({ "rare": true, "cap": 10 }),
                vec![("d2", 1e6 / 2_000f64.ln()), ("d4", 1e6 / 2_000f64.ln())],
            ),
            (
                Scorer::Learned,
                true,
                json!({ "cap": 10 }),
                vec![("d3", 2_000.0), ("d2", 1_000.0), ("d4", 1_000.0)],
            ),
        ];
        for (scorer, weighed, mut options, want) in cases {
            let mut via = |table: &str, from: &str, to: &str, weight: &str| {
                let mut cols = json!({ "from": from, "to": to });
                if weighed {
                    cols["weight"] = json!(weight);
                }
                options["via"] = json!({ table: cols });
                options.clone()
            };
            let mine = scored(&docs, scorer, via("cites", "doc", "cited", "written"));
            let theirs = scored(&papers, scorer, via("refs", "src", "dst", "at_ms"));
            assert_eq!(mine, theirs, "{scorer:?}");
            let got: Vec<(&str, f64)> = mine.0.iter().map(|(id, s)| (id.as_str(), *s)).collect();
            assert_eq!(got.len(), want.len(), "{scorer:?} {got:?}");
            for ((id, s), (want_id, want_s)) in got.iter().zip(&want) {
                assert_eq!(id, want_id, "{scorer:?} {got:?}");
                assert!((s - want_s).abs() < 1e-12, "{scorer:?} {got:?}");
            }
            assert_eq!(mine.1, json!(options["cap"] == json!(1)), "{scorer:?}");
        }
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
            for (table, from, to, _) in via {
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
    #[tokio::test(start_paused = true)]
    async fn a_capped_walk_equals_a_breadth_first_walk() {
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
                    "INSERT OR IGNORE INTO cites (doc, cited) VALUES (?1, ?2)",
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

            let whole = walk(&conn, &both, &root, 2, 1_000_000, None).unwrap();
            let (capped, cut) = walk(&conn, &both, &root, 2, 10, None).unwrap();

            assert_eq!(whole, (full.clone(), false), "root {root}");
            assert_eq!(capped, full[..10.min(full.len())], "root {root}");
            assert_eq!(cut, full.len() > 10, "root {root}");
            cut_seen |= cut;
        }
        assert!(cut_seen);
    }
}
