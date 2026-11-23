#![warn(missing_docs)]
//! The crate an app builds its ontology with: two tables in rannoch's file, `_nodes` and
//! `_edges`, rows only added. Node types, property keys and edge names are the app's; the crate
//! holds no ontology of its own and checks no schema.
//!
//! A step writes the graph by giving `$graph` in its output: a list of [`Op`]s as JSON.
//! rannoch-processor holds the list in its [`Writes`] under the id of the event the flow ran on,
//! and [`GraphAppend`], the app's [`Append`], writes the rows in the same transaction as that
//! event's 2, each row stamped with the event. Reads take a connection to the same file:
//! [`prop`], [`props`], [`history`], [`edges`] and [`why`] read rows, [`walk`] follows named edges
//! out from a node, [`describe`] shapes a node, and [`Hoods`] keeps a describe warm.
//!
//! # Example
//!
//! In an app, the processor fills `Writes` from each step's `$graph`, and rannoch hands the
//! append its batches. This example does both itself, so it can read the rows back at once: it
//! writes a person, a doc and the edge between them, corrects the person's name, then reads them
//! back.
//!
//! ```
//! use std::sync::{PoisonError, mpsc};
//! use std::{error::Error, thread, time::Duration};
//!
//! use rannoch_core::{Append, Event, Status, Storage};
//! use rannoch_graph::{GraphAppend, Opts, State, describe, edges, history, prop, why};
//! use rannoch_processor::Writes;
//! use rannoch_store::{Opened, Store};
//! use rusqlite::Connection;
//! use serde_json::{Value, json};
//! use uuid::Uuid;
//!
//! # fn main() -> Result<(), Box<dyn Error>> {
//! // A store in a directory of its own, with every migration applied.
//! let dir = std::env::temp_dir().join(format!("rannoch-graph-{}", Uuid::now_v7()));
//! std::fs::create_dir_all(&dir)?;
//! let path = dir.join("store.db");
//! Store::new(path.clone(), rannoch_core::migrator()).apply()?;
//! let open = || -> Result<Connection, Box<dyn Error>> {
//!     match rannoch_store::open(&path, &rannoch_core::migrator())? {
//!         Opened::Ready(conn) => Ok(conn),
//!         Opened::Pending(versions) => Err(format!("pending: {versions:?}").into()),
//!     }
//! };
//!
//! // The append writes through one connection, and the reads go through another.
//! let writes = Writes::default();
//! let (signal, _wake) = mpsc::channel();
//! let append = GraphAppend::new(open()?, signal, writes.clone());
//! let conn = open()?;
//!
//! // An event with the `$graph` items its flow gave, appended with its 2; returns its id.
//! let run = |source: Option<Uuid>, items: Vec<Value>| -> Result<Uuid, Box<dyn Error>> {
//!     let id = Uuid::now_v7();
//!     writes.lock().unwrap_or_else(PoisonError::into_inner).insert(id, items);
//!     let event = |id, source, status| Event {
//!         id,
//!         source,
//!         ty: 1_u16,
//!         data: json!({}),
//!         run_at: None,
//!         status,
//!     };
//!     let two = event(Uuid::now_v7(), Some(id), Status::Processed);
//!     append.append(&[event(id, source, Status::Added), two])?;
//!     Ok(id)
//! };
//!
//! let note = run(None, vec![
//!     json!({ "set": { "node": "ada", "key": "name", "value": "Ada" } }),
//!     json!({ "set": { "node": "doc1", "key": "title", "value": "graph store notes" } }),
//!     json!({ "link": {
//!         "src": "ada", "name": "wrote", "dst": "doc1", "props": { "year": 1843 }
//!     } }),
//! ])?;
//! // A later millisecond, so that as of the first append leaves the correction out.
//! thread::sleep(Duration::from_millis(5));
//! let fix = run(Some(note), vec![
//!     json!({ "set": { "node": "ada", "key": "name", "value": "Ada Lovelace" } }),
//! ])?;
//!
//! // Now: the row with the greatest event id. As of the first append: the row it wrote.
//! let name = prop(&conn, "ada", "name", None)?.ok_or("no name")?;
//! assert_eq!((&name.value, name.event), (&json!("Ada Lovelace"), fix));
//! let first = prop(&conn, "doc1", "title", None)?.ok_or("no title")?.written;
//! let then = prop(&conn, "ada", "name", Some(first))?.ok_or("no name then")?;
//! assert_eq!(then.value, json!("Ada"));
//! let states: Vec<State> = history(&conn, "ada", "name", None)?.iter().map(|h| h.1).collect();
//! assert_eq!(states, [State::Replaced, State::Current]);
//!
//! // Edges out of a node and into it; why: the row's event, then its source chain.
//! let wrote = edges(&conn, "ada", "wrote", false, None)?;
//! assert_eq!((wrote[0].0.as_str(), &wrote[0].1.value), ("doc1", &json!({ "year": 1843 })));
//! assert_eq!(edges(&conn, "doc1", "wrote", true, None)?[0].0, "ada");
//! assert_eq!(why(&conn, "ada", "name")?, [fix, note]);
//!
//! // A describe mirrors its shape, and every value and edge carries its event and written.
//! let (note, fix) = (note.to_string(), fix.to_string());
//! assert_eq!(
//!     describe(&conn, "ada", &json!(["name", { "wrote": ["title"] }]), Opts::default())?,
//!     json!({
//!         "id": "ada",
//!         "name": { "value": "Ada Lovelace", "event": fix, "written": name.written },
//!         "wrote": [{
//!             "id": "doc1",
//!             "title": { "value": "graph store notes", "event": note, "written": first },
//!             "edge": { "props": { "year": 1843 }, "event": note, "written": first }
//!         }]
//!     })
//! );
//! # drop(append);
//! # drop(conn);
//! # std::fs::remove_dir_all(&dir)?;
//! # Ok(())
//! # }
//! ```
//!
//! # What a step writes: `$graph`
//!
//! Each `$graph` item is one [`Op`]: an object whose one key is the op's name, holding the op's
//! fields. Every field is required, and node ids, keys and edge names are strings.
//!
//! | Item | What it writes |
//! |---|---|
//! | `{"set": {"node": "ada", "key": "name", "value": "Ada"}}` | `node`'s `key` holds `value`, any JSON value |
//! | `{"retract": {"node": "ada", "key": "name"}}` | `node`'s `key` holds no value |
//! | `{"link": {"src": "ada", "name": "wrote", "dst": "doc1", "props": {}}}` | a `name` edge from `src` to `dst`, carrying `props`, any JSON value |
//! | `{"unlink": {"src": "ada", "name": "wrote", "dst": "doc1"}}` | no `name` edge from `src` to `dst` |
//!
//! Each item takes effect as its key's or edge's current row only when its event has the greatest
//! id among that key's or edge's rows, as **Now** below says.
//!
//! A `set` with `"value": null` holds `null`; only `retract` takes a key's value away. A row's
//! primary key includes its event, so one event writes each node's key, and each edge by its
//! `src`, `name` and `dst`, at most once: when its items write the same one twice, the first is
//! written and the later one is left out. A flow that sets a key and then retracts it keeps the
//! set. For the same reason, applying the same event's items again adds no row and moves no
//! stamp. A retry or a hunt's resend is a new event: its items add rows stamped with its own id,
//! and they become current.
//!
//! A store row that rannoch-processor's `store` step carries, `{"store": {"table": T, "row":
//! {..}}}`, is not an `Op`: rannoch-events' `write_in` inserts it in the same transaction, each
//! 2's items inside a savepoint. When T refuses it there, a row the `store` step found T would
//! take, that 2's rows are rolled back and the 2 is not written; the append returns a -1 for its
//! source naming the reason, which the inbox sends, and the rest of the batch commits. Any other item that does not parse as an `Op` fails the append
//! with [`Error::Store`]: nothing in the batch is written, and `Writes` gets the batch's items
//! back, unless that same event has items there again by then. Rannoch's inbox puts the failed
//! batch back ahead of later events and tries it again each interval, so while the bad item is in
//! it, nothing more from that inbox is written.
//!
//! # What the reads return: now, as of T and why
//!
//! [`prop`], [`props`], [`history`] and [`edges`] return each row as a [`Row`]: its `value` (an
//! edge's `props`), the `event` whose flow wrote it, and `written`, when the append that wrote it
//! ran, in milliseconds since the Unix epoch; [`why`] returns event ids.
//!
//! - **Now.** The current row of a node's key, or of an edge, is its row with the greatest event
//!   id, unless that row is a retraction, which leaves it no current row. Rannoch's event ids
//!   are `UUIDv7`s set at accept, so the event accepted last wins, in whatever order the flows
//!   ran. [`prop`] reads one key; [`props`] reads every key of a node that has a current row, in
//!   key order; [`edges`] reads a node's current edges of one name, out of it or, with
//!   `reverse`, into it, in far-node order.
//! - **As of T.** Each of those reads takes `at`. `Some(T)`, in milliseconds since the Unix
//!   epoch, reads only the rows written at or before T and picks the current one among them the
//!   same way; `None` reads every row. `written`, not the event id, decides which rows T can
//!   see, because an event can run long after it was accepted.
//! - **History.** [`history`] returns every row of a node's key, as of `at`, in event-id order,
//!   each with its [`State`]. A retraction is a row of its own, with a `null` value.
//! - **Why.** [`why`] returns the event of a key's current row, then that event's source chain
//!   through `_events`: its source, that event's source, and so on. It reads now only, and
//!   returns an empty list when the key has no current row.
//!
//! # Walk: the nodes a node reaches
//!
//! [`walk`] follows named edges out from a node: each name in `via` follows its edges forwards,
//! and `name<-` backwards. It reads the current edges as of `at`, goes at most `hops` steps, and
//! returns each node it reaches, the root left out, once with its fewest hops: nearest first,
//! then in node-id order, at most `cap` of them, and whether the cap cut the walk.
//!
//! # Describe: a node by a shape
//!
//! [`describe`] returns a node shaped by a shape, a JSON list whose items say what to read:
//!
//! | Item | What the result holds |
//! |---|---|
//! | `"title"` | under `"title"`, the key's current row as `{"value", "event", "written"}`; nothing when it has none |
//! | `"*"` | every key with a current row, each under its own name, as above |
//! | `{"wrote": shape}` | under `"wrote"`, a list of the far nodes of the node's current `wrote` edges, in far-node order, each shaped by `shape` |
//! | `{"wrote<-": shape}` | under `"wrote<-"`, the same for the `wrote` edges into the node |
//! | `{"wrote": {"shape": shape, "limit": 2, "where": {"status": "open"}}}` | the same, keeping only the far nodes whose current value of each `where` key equals the JSON given, and only the first `limit` of those |
//!
//! One object can name several edges. In `{"shape", "limit", "where"}` only `shape` is required;
//! `limit` is ignored unless it is a non-negative integer, and `where` when it is not an object.
//!
//! The result is an object with the node's `"id"` and what each item reads; each far node also
//! carries `"edge"`, the edge's `{"props", "event", "written"}`. A key named `id` replaces the
//! node's `"id"` in the result, and on a far node the edge replaces a key named `edge`.
//!
//! [`Opts`] holds the options. With `at`, describe reads every row as of `at`: keys, edges and
//! `where` values alike. With `history`, each key holds a list in place of its current row:
//! every row it has had, in event-id order, each with a `"state"` of `"current"`, `"replaced"` or
//! `"retracted"`; `"*"` still lists only the keys with a current row.
//!
//! A shape that is not a list of these items is refused with rusqlite's
//! `ToSqlConversionFailure`. Describe reads each list when it reaches it, so a bad list under an
//! edge that keeps no far node is not refused.
//!
//! # Warm hoods
//!
//! [`Hoods`] keeps describes warm. A node and a shape are a hood: [`Hoods::describe`] describes
//! them cold the first time and keeps the result, and later describes of the same node and shape
//! return that copy. A [`GraphAppend`] made [`with_hoods`](GraphAppend::with_hoods) recomputes,
//! after each commit and before it signals the queue, every warm hood that read a node its new
//! rows touch: a `set` or `retract` touches its node, a `link` or `unlink` both its ends, and an
//! item that adds no row touches nothing. A hood records every node its describe read,
//! including far nodes its `where` left out, so while the graph's rows come through that append,
//! a warm hood equals a cold describe of its node and shape once the append has recomputed it. A
//! hood whose recompute fails goes cold, and its next describe is a cold one.
//!
//! `Hoods` warms only describes with default [`Opts`]; a describe with `at` or `history` goes
//! through [`describe`] and is always cold. Only a failed recompute drops a warm hood; nothing
//! else evicts one.
//!
//! With the store, `open` and `run` of the example above, and an append made with hoods:
//!
//! ```
//! # use std::sync::{Arc, Mutex, PoisonError, mpsc};
//! # use std::error::Error;
//! # use rannoch_core::{Append, Event, Status, Storage};
//! # use rannoch_graph::{GraphAppend, Hoods, Opts, describe};
//! # use rannoch_processor::Writes;
//! # use rannoch_store::{Opened, Store};
//! # use rusqlite::Connection;
//! # use serde_json::{Value, json};
//! # use uuid::Uuid;
//! # fn main() -> Result<(), Box<dyn Error>> {
//! # let dir = std::env::temp_dir().join(format!("rannoch-graph-{}", Uuid::now_v7()));
//! # std::fs::create_dir_all(&dir)?;
//! # let path = dir.join("store.db");
//! # Store::new(path.clone(), rannoch_core::migrator()).apply()?;
//! # let open = || -> Result<Connection, Box<dyn Error>> {
//! #     match rannoch_store::open(&path, &rannoch_core::migrator())? {
//! #         Opened::Ready(conn) => Ok(conn),
//! #         Opened::Pending(versions) => Err(format!("pending: {versions:?}").into()),
//! #     }
//! # };
//! # let writes = Writes::default();
//! # let (signal, _wake) = mpsc::channel();
//! # let conn = open()?;
//! let hoods = Arc::new(Mutex::new(Hoods::default()));
//! let append = GraphAppend::new(open()?, signal, writes.clone()).with_hoods(hoods.clone());
//! # let run = |source: Option<Uuid>, items: Vec<Value>| -> Result<Uuid, Box<dyn Error>> {
//! #     let id = Uuid::now_v7();
//! #     writes.lock().unwrap_or_else(PoisonError::into_inner).insert(id, items);
//! #     let event = |id, source, status| Event {
//! #         id,
//! #         source,
//! #         ty: 1_u16,
//! #         data: json!({}),
//! #         run_at: None,
//! #         status,
//! #     };
//! #     let two = event(Uuid::now_v7(), Some(id), Status::Processed);
//! #     append.append(&[event(id, source, Status::Added), two])?;
//! #     Ok(id)
//! # };
//! run(None, vec![
//!     json!({ "set": { "node": "doc1", "key": "status", "value": "draft" } }),
//!     json!({ "link": { "src": "ada", "name": "wrote", "dst": "doc1", "props": {} } }),
//! ])?;
//!
//! // Ada's open docs: the first describe warms the hood; the lock is held only to describe.
//! let shape = json!([{ "wrote": { "shape": ["status"], "where": { "status": "open" } } }]);
//! let warm = |hoods: &Mutex<Hoods>| {
//!     let mut hoods = hoods.lock().unwrap_or_else(PoisonError::into_inner);
//!     hoods.describe(&conn, "ada", &shape)
//! };
//! assert_eq!(warm(&hoods)?["wrote"], json!([]));
//!
//! // doc1 opens. The hood read doc1, though its `where` left it out, so the append recomputes it.
//! run(None, vec![json!({ "set": { "node": "doc1", "key": "status", "value": "open" } })])?;
//! assert_eq!(warm(&hoods)?["wrote"][0]["id"], "doc1");
//! assert_eq!(warm(&hoods)?, describe(&conn, "ada", &shape, Opts::default())?);
//! # drop(append);
//! # drop(conn);
//! # std::fs::remove_dir_all(&dir)?;
//! # Ok(())
//! # }
//! ```

mod describe;
mod hoods;
mod queries;
mod walk;

pub use walk::walk;

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError, mpsc::Sender};
use std::time::UNIX_EPOCH;

use rannoch_core::{Append, Error, Event};
use rannoch_processor::Writes;
use rusqlite::{CachedStatement, Connection, OptionalExtension, Params, Result, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub use describe::{Opts, describe};
pub use hoods::Hoods;

/// One `$graph` item: what a step writes. Serialized as JSON in an action's `$graph` output: an
/// object whose one key is the op's name in lowercase, holding the op's fields. The crate doc
/// lists the four.
///
/// ```
/// use rannoch_graph::Op;
/// use serde_json::json;
///
/// let item = json!({ "link": { "src": "ada", "name": "wrote", "dst": "doc1", "props": {} } });
/// let (src, name, dst) = ("ada".to_owned(), "wrote".to_owned(), "doc1".to_owned());
/// let link = Op::Link { src, name, dst, props: json!({}) };
/// assert_eq!(serde_json::from_value::<Op>(item)?, link);
///
/// // Every field is required: a link without props is not an op.
/// let bare = json!({ "link": { "src": "ada", "name": "wrote", "dst": "doc1" } });
/// assert!(serde_json::from_value::<Op>(bare).is_err());
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    /// `node`'s `key` holds `value`.
    Set {
        /// The node's id.
        node: String,
        /// The property key.
        key: String,
        /// Any JSON value; `null` is a value, not a retraction.
        value: Value,
    },
    /// A retraction: `node`'s `key` has no current value, unless an event with a greater id sets
    /// it again.
    Retract {
        /// The node's id.
        node: String,
        /// The property key.
        key: String,
    },
    /// A `name` edge from `src` to `dst`, carrying `props`.
    Link {
        /// The id of the node the edge leaves.
        src: String,
        /// The edge's name.
        name: String,
        /// The id of the node the edge reaches.
        dst: String,
        /// Any JSON value, read back as the edge's [`Row::value`].
        props: Value,
    },
    /// A retraction of the `name` edge from `src` to `dst`.
    Unlink {
        /// The id of the node the edge leaves.
        src: String,
        /// The edge's name.
        name: String,
        /// The id of the node the edge reaches.
        dst: String,
    },
}

/// A row as read: its value, the event that wrote it, and when it was written.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The property's value, or the edge's `props`; `null` for a retraction, which only
    /// [`history`] returns.
    pub value: Value,
    /// The id of the event whose flow wrote the row: the source of the 2 that carried it.
    pub event: Uuid,
    /// When the append that wrote the row ran, in milliseconds since the Unix epoch; the rows of
    /// one append share it.
    pub written: i64,
}

/// Where a row stands in its key's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// The key's current row: the last in event-id order, and not a retraction.
    Current,
    /// A value a later row replaced or retracted.
    Replaced,
    /// A retraction.
    Retracted,
}

/// rannoch-events' append, plus each 2's graph rows from `writes` in the same transaction.
#[derive(Debug)]
pub struct GraphAppend {
    conn: Mutex<Connection>,
    queue: Sender<()>,
    writes: Writes,
    hoods: Option<Arc<Mutex<Hoods>>>,
}

impl GraphAppend {
    /// The append over `conn`, writing what `writes` holds and signalling `queue` on commit.
    #[must_use]
    pub fn new(conn: Connection, queue: Sender<()>, writes: Writes) -> Self {
        Self {
            conn: Mutex::new(conn),
            queue,
            writes,
            hoods: None,
        }
    }

    /// The append, recomputing after each commit the warm hoods in `hoods` its rows touched.
    #[must_use]
    pub fn with_hoods(self, hoods: Arc<Mutex<Hoods>>) -> Self {
        let hoods = Some(hoods);
        Self { hoods, ..self }
    }
}

impl<T: Copy + Into<u16>, D: Serialize + From<String>> Append<T, D> for GraphAppend {
    fn append(&self, batch: &[Event<T, D>]) -> Result<Vec<Event<T, D>>, Error> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        let (replies, touched) =
            rannoch_events::taking(&self.writes, batch, |t| commit(&mut conn, batch, t))?;
        if let Some(hoods) = &self.hoods {
            let mut hoods = hoods.lock().unwrap_or_else(PoisonError::into_inner);
            hoods.rewarm(&conn, &touched);
        }
        let _ = self.queue.send(());
        Ok(replies)
    }
}

/// The -1s an append returns, and the nodes of the rows it added.
type Committed<T, D> = (Vec<Event<T, D>>, HashSet<String>);

/// `batch` and the rows `taken` for its 2s, stamped with each 2's source, in one transaction,
/// with rannoch-events' `write_in`: the -1s it returns, and the nodes of the rows it added.
fn commit<T: Copy + Into<u16>, D: Serialize + From<String>>(
    conn: &mut Connection,
    batch: &[Event<T, D>],
    taken: &[(Uuid, Vec<Value>)],
) -> Result<Committed<T, D>, Error> {
    let since = UNIX_EPOCH.elapsed().unwrap_or_default().as_millis();
    let written = i64::try_from(since).unwrap_or(i64::MAX);
    let mut tx = conn.transaction().map_err(|_| Error::Store)?;
    let mut touched = Vec::new();
    let replies = rannoch_events::write_in(&mut tx, batch, taken, |tx, source, item| {
        let op = Op::deserialize(item).map_err(|_| Error::Store)?;
        if write(tx, &source.to_string(), &op, written).map_err(|_| Error::Store)? == 1 {
            match op {
                Op::Set { node, .. } | Op::Retract { node, .. } => touched.push((*source, node)),
                Op::Link { src, dst, .. } | Op::Unlink { src, dst, .. } => {
                    touched.extend([(*source, src), (*source, dst)]);
                }
            }
        }
        Ok(())
    })?;
    tx.commit().map_err(|_| Error::Store)?;
    // A source whose rows were rolled back touched nothing.
    let added = touched.into_iter().filter(|(source, _)| {
        !replies
            .iter()
            .any(|binned: &Event<T, D>| binned.source == Some(*source))
    });
    let added = added.map(|(_, node)| node).collect();
    Ok((replies, added))
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

    pub(crate) fn set(node: &str, key: &str, value: Value) -> Op {
        let (node, key) = (node.to_owned(), key.to_owned());
        Op::Set { node, key, value }
    }

    pub(crate) fn link(src: &str, name: &str, dst: &str, props: Value) -> Op {
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
    pub(crate) fn apply(conn: &mut Connection, evs: &[&Ev], clock: i64) {
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
    pub(crate) fn small() -> (Connection, [Ev; 4]) {
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

    /// A 2's store row is inserted with its graph rows; one into a table the store does not have
    /// fails the batch, writing none of it.
    #[test]
    fn a_store_row_is_written_with_its_2_and_one_the_store_does_not_have_fails_the_batch() {
        use rannoch_core::{Event, Status};
        let mut conn = rannoch_core::forgr::empty_store();
        conn.execute_batch("CREATE TABLE t (k TEXT PRIMARY KEY)")
            .unwrap();
        let store = |table| json!({ "store": { "table": table, "row": { "k": "a" } } });
        let set = json!({ "set": { "node": "a", "key": "k", "value": 1 } });
        let two = |source| Event {
            id: Uuid::now_v7(),
            source: Some(source),
            ty: 1_u16,
            data: json!({}),
            run_at: None,
            status: Status::Processed,
        };
        let rows = |c: &Connection| {
            let tables = ["t", "_nodes", "_events"];
            tables.map(|t| count(c, &format!("SELECT count(*) FROM {t}")))
        };

        let (first, second) = (v7_at(1_000), v7_at(2_000));
        let taken = [(first, vec![store("t"), set])];
        super::commit(&mut conn, &[two(first)], &taken).unwrap();
        assert_eq!(rows(&conn), [1, 1, 1]);

        let taken = [(second, vec![store("nope")])];
        assert!(super::commit(&mut conn, &[two(second)], &taken).is_err());
        assert_eq!(rows(&conn), [1, 1, 1]);
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
