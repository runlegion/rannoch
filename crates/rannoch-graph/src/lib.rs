//! The crate an app builds its ontology with: two tables in rannoch's file, rows only added.

mod queries;

use std::sync::{Mutex, PoisonError, mpsc::Sender};
use std::time::UNIX_EPOCH;

use rannoch_core::{Append, Error, Event, Status};
use rannoch_processor::Writes;
use rusqlite::{CachedStatement, Connection, OptionalExtension, Params, Result, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// One `$graph` item: what a step writes. Serialized as JSON in an action's `$graph` output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Set {
        node: String,
        key: String,
        value: Value,
    },
    Retract {
        node: String,
        key: String,
    },
    Link {
        src: String,
        name: String,
        dst: String,
        props: Value,
    },
    Unlink {
        src: String,
        name: String,
        dst: String,
    },
}

/// A row as read: its value, the event that wrote it, and when it was written.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub value: Value,
    pub event: Uuid,
    pub written: i64,
}

/// Where a row stands in its key's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Current,
    Replaced,
    Retracted,
}

/// rannoch-events' append, plus each 2's graph rows from `writes` in the same transaction.
#[derive(Debug)]
pub struct GraphAppend {
    conn: Mutex<Connection>,
    queue: Sender<()>,
    writes: Writes,
}

impl GraphAppend {
    /// The append over `conn`, writing what `writes` holds and signalling `queue` on commit.
    #[must_use]
    pub fn new(conn: Connection, queue: Sender<()>, writes: Writes) -> Self {
        Self {
            conn: Mutex::new(conn),
            queue,
            writes,
        }
    }
}

impl<T: Copy + Into<u16>, D: Serialize> Append<T, D> for GraphAppend {
    fn append(&self, batch: &[Event<T, D>]) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        let twos = batch.iter().filter(|e| e.status == Status::Processed);
        let mut writes = self.writes.lock().unwrap_or_else(PoisonError::into_inner);
        let taken: Vec<(Uuid, Vec<Value>)> = twos
            .filter_map(|e| writes.remove_entry(&e.source?))
            .collect();
        // Held only to take them, so steps keep putting their writes while this batch commits.
        drop(writes);
        let done = commit(&mut conn, batch, &taken);
        if done.is_err() {
            // A batch that fails puts its rows back for its retry; a rerun's newer writes stay.
            let mut writes = self.writes.lock().unwrap_or_else(PoisonError::into_inner);
            for (source, items) in taken {
                writes.entry(source).or_insert(items);
            }
        }
        done?;
        let _ = self.queue.send(());
        Ok(())
    }
}

/// `batch` and the rows `taken` for its 2s, stamped with each 2's source, in one transaction.
fn commit<T: Copy + Into<u16>, D: Serialize>(
    conn: &mut Connection,
    batch: &[Event<T, D>],
    taken: &[(Uuid, Vec<Value>)],
) -> Result<(), Error> {
    let since = UNIX_EPOCH.elapsed().unwrap_or_default().as_millis();
    let written = i64::try_from(since).unwrap_or(i64::MAX);
    let tx = conn.transaction().map_err(|_| Error::Store)?;
    rannoch_events::append_in(&tx, batch)?;
    for (source, items) in taken {
        let event = source.to_string();
        for item in items {
            let op = Op::deserialize(item).map_err(|_| Error::Store)?;
            write(&tx, &event, &op, written).map_err(|_| Error::Store)?;
        }
    }
    tx.commit().map_err(|_| Error::Store)
}

/// Adds `op`'s row, stamped with `event` and `written`; a row its key already holds stays.
fn write(conn: &Connection, event: &str, op: &Op, written: i64) -> Result<usize> {
    let (value, retracted) = match op {
        Op::Set { value, .. } | Op::Link { props: value, .. } => (Some(value.to_string()), 0),
        Op::Retract { .. } | Op::Unlink { .. } => (None, 1),
    };
    match op {
        Op::Set { node, key, .. } | Op::Retract { node, key } => {
            let row = params![node, key, value, event, written, retracted];
            conn.prepare_cached(queries::NODE)?.execute(row)
        }
        Op::Link { src, name, dst, .. } | Op::Unlink { src, name, dst } => {
            let row = params![src, name, dst, value, event, written, retracted];
            conn.prepare_cached(queries::EDGE)?.execute(row)
        }
    }
}

/// The current row of `node`'s `key` as of `at`: the greatest event id's, unless retracted.
/// # Errors
/// The store's error, or a value or event id in the row that does not parse.
pub fn prop(conn: &Connection, node: &str, key: &str, at: Option<i64>) -> Result<Option<Row>> {
    let mut st = conn.prepare_cached(queries::PROP)?;
    let row = st.query_row(params![node, key, at], |r| read(r, 0));
    let row = row.optional()?;
    Ok(row.and_then(|(row, gone)| (!gone).then_some(row)))
}

/// The current row of each of `node`'s keys, as of `at` when given, by key.
/// # Errors
/// As `prop`.
pub fn props(conn: &Connection, node: &str, at: Option<i64>) -> Result<Vec<(String, Row)>> {
    current(&mut conn.prepare_cached(queries::PROPS)?, params![node, at])
}

/// Every row of `node`'s `key`, as of `at` when given, oldest first, each with its state.
/// # Errors
/// As `prop`.
pub fn history(
    conn: &Connection,
    node: &str,
    key: &str,
    at: Option<i64>,
) -> Result<Vec<(Row, State)>> {
    let mut st = conn.prepare_cached(queries::HISTORY)?;
    let rows = st.query_map(params![node, key, at], |r| read(r, 0))?;
    let rows = rows.collect::<Result<Vec<_>>>()?;
    let last = rows.len().saturating_sub(1);
    let state = |i, gone| match (gone, i == last) {
        (true, _) => State::Retracted,
        (false, true) => State::Current,
        (false, false) => State::Replaced,
    };
    let rows = rows.into_iter().enumerate();
    Ok(rows.map(|(i, (row, gone))| (row, state(i, gone))).collect())
}

/// The current `name` edges from `node`, or into it when `reverse`, as of `at`, by far node.
/// # Errors
/// As `prop`.
pub fn edges(
    conn: &Connection,
    node: &str,
    name: &str,
    reverse: bool,
    at: Option<i64>,
) -> Result<Vec<(String, Row)>> {
    let sql = if reverse { queries::IN } else { queries::OUT };
    current(&mut conn.prepare_cached(sql)?, params![node, name, at])
}

/// Why `node`'s `key` holds its current value: its event, then that event's source chain.
/// # Errors
/// As `prop`.
pub fn why(conn: &Connection, node: &str, key: &str) -> Result<Vec<Uuid>> {
    let event = prop(conn, node, key, None)?.map(|row| row.event.to_string());
    let mut st = conn.prepare_cached(queries::WHY)?;
    let ids = st.query_map([event], |r| r.get::<_, String>(0))?;
    ids.map(|id| id?.parse().map_err(bad(0))).collect()
}

/// The first row of each group (a key or a far node, the first column), unless retracted.
fn current(st: &mut CachedStatement<'_>, args: impl Params) -> Result<Vec<(String, Row)>> {
    let (mut rows, mut out, mut last) = (st.query(args)?, Vec::new(), None);
    while let Some(r) = rows.next()? {
        let (group, (row, gone)) = (r.get::<_, String>(0)?, read(r, 1)?);
        if last.replace(group.clone()).as_ref() != Some(&group) && !gone {
            out.push((group, row));
        }
    }
    Ok(out)
}

/// The value, event, written and retracted columns from column `at` on.
fn read(r: &rusqlite::Row<'_>, at: usize) -> Result<(Row, bool)> {
    let (value, event): (Option<String>, String) = (r.get(at)?, r.get(at + 1)?);
    let value = value.map_or(Ok(Value::Null), |v| serde_json::from_str(&v));
    let row = Row {
        value: value.map_err(bad(at))?,
        event: event.parse().map_err(bad(at + 1))?,
        written: r.get(at + 2)?,
    };
    Ok((row, r.get::<_, i64>(at + 3)? == 1))
}

/// A column's text that does not parse, as rusqlite's conversion failure.
fn bad<E: std::error::Error + Send + Sync + 'static>(at: usize) -> impl Fn(E) -> rusqlite::Error {
    move |e| rusqlite::Error::FromSqlConversionFailure(at, rusqlite::types::Type::Text, e.into())
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, params};
    use serde_json::{Value, json};
    use uuid::{NoContext, Timestamp, Uuid};

    use super::{Op, prop, why, write};

    /// A v7 id at `ms` milliseconds since the Unix epoch.
    fn v7_at(ms: u64) -> Uuid {
        let nanos = u32::try_from((ms % 1000) * 1_000_000).unwrap();
        Uuid::new_v7(Timestamp::from_unix(NoContext, ms / 1000, nanos))
    }

    /// An event: its id, its source and the ops its flow wrote.
    type Ev = (Uuid, Option<Uuid>, Vec<Op>);

    fn ev(ms: u64, source: Option<&Ev>, ops: Vec<Op>) -> Ev {
        (v7_at(ms), source.map(|s| s.0), ops)
    }

    fn set(node: &str, key: &str, value: Value) -> Op {
        let (node, key) = (node.to_owned(), key.to_owned());
        Op::Set { node, key, value }
    }

    fn link(src: &str, name: &str, dst: &str, props: Value) -> Op {
        let (src, name, dst) = (src.to_owned(), name.to_owned(), dst.to_owned());
        Op::Link {
            src,
            name,
            dst,
            props,
        }
    }

    /// The events' rows and their ops' rows in one transaction, each op's rows written at
    /// `clock`, as the toy's `apply_many`.
    fn apply(conn: &mut Connection, evs: &[&Ev], clock: i64) {
        let tx = conn.transaction().unwrap();
        for (id, source, ops) in evs {
            let row = params![id.to_string(), source.map(|s| s.to_string())];
            let sql = "INSERT OR IGNORE INTO _events (id, source, ty, data, run_at, status)
                VALUES (?1, ?2, 1, '{}', NULL, 2)";
            tx.execute(sql, row).unwrap();
            for op in ops {
                write(&tx, &id.to_string(), op, clock).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    fn value(conn: &Connection, node: &str, key: &str, at: Option<i64>) -> Option<Value> {
        prop(conn, node, key, at).unwrap().map(|r| r.value)
    }

    /// The toy's small graph: ada wrote doc1..doc3; doc1 cites doc2 and doc3; then ada's name
    /// is corrected and doc3's status retracted. Each event applied at its own time.
    fn small() -> (Connection, [Ev; 4]) {
        let e0 = ev(1_000, None, vec![]);
        let e1 = ev(
            1_100,
            Some(&e0),
            vec![
                set("ada", "type", json!("person")),
                set("ada", "name", json!("Ada")),
                set("doc1", "title", json!("graph store notes")),
                set("doc1", "status", json!("draft")),
                set("doc2", "title", json!("queue notes")),
                set("doc2", "status", json!("open")),
                set("doc3", "title", json!("flow notes")),
                set("doc3", "status", json!("closed")),
                link("ada", "wrote", "doc1", json!({})),
                link("ada", "wrote", "doc2", json!({})),
                link("ada", "wrote", "doc3", json!({})),
                link("doc1", "cites", "doc2", json!({ "page": 3 })),
                link("doc1", "cites", "doc3", json!({})),
            ],
        );
        let e2 = ev(
            2_000,
            Some(&e1),
            vec![set("ada", "name", json!("Ada Lovelace"))],
        );
        let (node, key) = ("doc3".to_owned(), "status".to_owned());
        let e3 = ev(3_000, Some(&e2), vec![Op::Retract { node, key }]);
        let mut conn = rannoch_core::forgr::empty_store();
        for (e, t) in [&e0, &e1, &e2, &e3]
            .into_iter()
            .zip([1_000, 1_100, 2_000, 3_000])
        {
            apply(&mut conn, &[e], t);
        }
        (conn, [e0, e1, e2, e3])
    }

    /// A rerun adds no row and moves no stamp (the toy's P1).
    #[test]
    fn fr_rannoch_123_c1() {
        let (mut conn, small) = small();
        let rows = |c: &Connection| {
            let nodes = count(c, "SELECT count(*) FROM _nodes");
            (nodes, count(c, "SELECT count(*) FROM _edges"))
        };
        let before = rows(&conn);

        apply(&mut conn, &small.iter().collect::<Vec<_>>(), 9_999);

        assert_eq!(before, (10, 5));
        assert_eq!(rows(&conn), before);
        let moved = count(&conn, "SELECT count(*) FROM _nodes WHERE written = 9999")
            + count(&conn, "SELECT count(*) FROM _edges WHERE written = 9999");
        assert_eq!(moved, 0);
    }

    /// The greater event id wins whichever is applied first (the toy's P2).
    #[test]
    fn fr_rannoch_123_c2() {
        let mut conn = rannoch_core::forgr::empty_store();
        let ops = |v: &str| vec![set("k", "v", json!(v)), set("k2", "v", json!(v))];
        let (early, late) = (ev(4_000, None, ops("early")), ev(4_001, None, ops("late")));

        apply(&mut conn, &[&late], 4_100);
        apply(&mut conn, &[&early], 4_100);

        assert_eq!(value(&conn, "k", "v", None), Some(json!("late")));
        assert_eq!(value(&conn, "k2", "v", None), Some(json!("late")));
    }

    /// As of T leaves out a row written after T of an event accepted before it (the toy's P3).
    #[test]
    fn fr_rannoch_123_c3() {
        let mut conn = rannoch_core::forgr::empty_store();
        // d accepted at 5,000 and run at 9,000; b accepted at 6,000 and run at 6,000.
        let d = ev(
            5_000,
            None,
            vec![set("x", "K", json!("d")), set("x", "L", json!("d"))],
        );
        let b = ev(6_000, None, vec![set("x", "K", json!("b"))]);

        apply(&mut conn, &[&b], 6_000);
        apply(&mut conn, &[&d], 9_000);

        // Read at 10,000.
        assert_eq!(value(&conn, "x", "K", Some(7_000)), Some(json!("b")));
        assert_eq!(value(&conn, "x", "L", Some(7_000)), None);
        assert_eq!(value(&conn, "x", "K", None), Some(json!("b")));
        assert_eq!(value(&conn, "x", "L", None), Some(json!("d")));
    }

    /// A correction and a retraction, now, as of and why (the toy's P4).
    #[test]
    fn fr_rannoch_123_c4() {
        let (conn, [e0, e1, e2, _]) = small();

        assert_eq!(
            value(&conn, "ada", "name", None),
            Some(json!("Ada Lovelace"))
        );
        assert_eq!(value(&conn, "ada", "name", Some(1_500)), Some(json!("Ada")));
        assert_eq!(why(&conn, "ada", "name").unwrap(), [e2.0, e1.0, e0.0]);
        assert_eq!(value(&conn, "doc3", "status", None), None);
        assert_eq!(
            value(&conn, "doc3", "status", Some(2_500)),
            Some(json!("closed"))
        );
    }
}
