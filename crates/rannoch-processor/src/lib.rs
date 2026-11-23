#![warn(missing_docs)]
//! The processor runs an app's flows. A flow is a JSON document for one event `ty`: a list of
//! steps, each of which calls one or more of the app's [`Action`]s or built-in steps and can send
//! events, branch on a value or wait for another event. This page is the reference for writing
//! flow documents.
//!
//! # Flow documents in an app
//!
//! 1. Each action declares a [`Sig`]: its name, its options, the fields it takes and gives, and
//!    whether it is a job. A signatures export is a JSON array of `Sig`s.
//! 2. The app's `build.rs` calls [`compile`] with the directory of its flow documents, one per
//!    `.json` file, and its signatures exports. `compile` runs [`check`] and fails the build on
//!    any error; otherwise it writes the flows as one JSON array to `$OUT_DIR/flows.json`.
//! 3. The app compiles that file in with `include_str!(concat!(env!("OUT_DIR"), "/flows.json"))`
//!    and hands it to [`Processor::new`] with its actions and a connection to the store. The
//!    processor is the queue's `Flows`.
//!
//! # Example
//!
//! A support ticket is rated and logged by its severity, then paged to a team, which has an
//! hour to answer. The ticket flow's first step branches and its second waits; the answer flow
//! runs the event the wait waits for, and the log flow is where every next goes. `check`
//! accepts the three documents, and names the error when a case's `when` is of a type other
//! than the field's.
//!
//! ```
//! use rannoch_processor::{FlowDoc, Sig, check};
//! use serde_json::{Value, json};
//!
//! // What the app's actions declare, as its signatures export holds it.
//! let sigs: Vec<Sig> = serde_json::from_value(json!([
//!     { "action": "rate", "takes": { "text": "string" }, "gives": { "severity": "number" } },
//!     { "action": "page", "options": { "team": "string" }, "takes": { "ticket": "string" },
//!       "gives": { "at": "string" }, "job": true }
//! ]))?;
//!
//! let ticket = json!({
//!     "name": "ticket", "ty": 10, "takes": { "ticket": "string", "text": "string" },
//!     "steps": [
//!         { "do": "rate",
//!           "next": { "branch": { "on": "severity",
//!               "cases": [{ "when": 3, "next": { "ty": 30,
//!                   "shape": { "ticket": "$ticket", "note": "urgent" } } }],
//!               "else": { "ty": 30, "delay_ms": 60000,
//!                   "shape": { "ticket": "$ticket",
//!                       "note": { "$concat": ["severity ", "$severity"] } } } } } },
//!         { "do": "page", "with": { "page": { "team": "support" } }, "as": "paged",
//!           "next": { "wait": { "for": 21, "key": "$ticket", "within_ms": 3600000,
//!               "next": { "ty": 30, "shape": { "ticket": "$ticket",
//!                   "note": { "$concat": ["answered by ", { "$": "$got.by", "or": "someone" }] } } },
//!               "timeout": { "ty": 30, "shape": { "ticket": "$ticket", "note": "no answer" } },
//!               "late": { "ty": 30, "shape": { "ticket": "$ticket",
//!                   "note": { "$concat": ["late answer by ", "$got.by"] } } } } } }
//!     ]
//! });
//! let answer = json!({ "name": "answer", "ty": 21,
//!     "takes": { "ticket": "string", "by": "string" }, "steps": [] });
//! let log = json!({ "name": "log", "ty": 30,
//!     "takes": { "ticket": "string", "note": "string" }, "steps": [] });
//!
//! // check takes each document with its file name, which its errors name.
//! let docs = |ticket: &Value| -> serde_json::Result<Vec<(String, FlowDoc)>> {
//!     [("ticket.json", ticket), ("answer.json", &answer), ("log.json", &log)]
//!         .into_iter()
//!         .map(|(file, doc)| Ok((file.to_owned(), serde_json::from_value(doc.clone())?)))
//!         .collect()
//! };
//!
//! let flows = check(&docs(&ticket)?, &sigs).map_err(|errors| errors.join("\n"))?;
//! let flows: Vec<FlowDoc> = serde_json::from_str(&flows)?;
//! // page is a job, so the ticket flow runs as one.
//! assert!(flows[0].job);
//!
//! let mut wrong = ticket.clone();
//! wrong["steps"][0]["next"]["branch"]["cases"][0]["when"] = json!("3");
//! assert_eq!(
//!     check(&docs(&wrong)?, &sigs),
//!     Err(vec![r#"E4 ticket.json step 0 next: branch on severity as number, a case when "3""#
//!         .to_owned()])
//! );
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # How a flow runs
//!
//! The queue hands the processor an event, and the processor runs the flow for its `ty` on the
//! data of the event's root, a JSON object: the event's own data, or for a retry the data of the
//! event that failed first. That object, and the fields the steps add to it, are the flow's
//! values.
//!
//! The steps run in order, and so do the links of a step, the action names in its `do`. Each
//! link is passed its options from the step's `with`, and as input the values with the outputs
//! of the links before it in the step, a later field replacing an earlier one. The processor
//! takes `$graph` out of each output, then fails the step when the output lacks a field the
//! action's `Sig` gives, or holds it as another type: `<link> gave no <field>`, or `<link> gave
//! <field> as <type>, not <type>`. When the step's links have run, their outputs join the values,
//! or with `as` go under one field. The step's next is worked out from the values as they then
//! stand, and the run goes on to the next step: a next, a branch or a wait does not end it.
//!
//! When the last step has run, the processor sends every step's nexts, each at status 0 with the
//! event as its source, then the event's 2, whose data is `"done"`. When a step fails, it sends
//! the event's -1, whose data is the failure, and no next. The queue retries the event while its
//! chain has attempts left, and the retry runs the flow from its first step.
//!
//! # The flow document
//!
//! The flow, its steps, and each `To`, `Branch`, `Case` and `Wait` refuse fields they do not
//! define. A document that does not parse fails `compile` with its path and the parse error,
//! before `check` runs.
//!
//! ## The flow: [`FlowDoc`]
//!
//! - `name`, required: the flow's name, by which `check` names a next to it.
//! - `ty`, required: the event `ty` the flow runs. Nexts and waits reach the flow by it.
//! - `takes`, required: a [`Shape`] of the fields the flow's data holds. It is the first step's
//!   scope, and every next to this `ty` must give each of its fields, of its type.
//! - `steps`, required: the [`Step`]s, in order. The list may be empty, as the example's answer
//!   and log flows show.
//! - `job`, default `false`: whether the queue runs the flow as a job, on a thread of its own.
//!   `check` sets it, true when any step's action has a job `Sig`, in place of what the document
//!   says.
//!
//! ## A step: [`Step`]
//!
//! - `do`, required: the step's links, action names joined by `.`, as in `"trim.upper.greet"`.
//! - `with`, default `{}`: each link's options, by its action name, as in `{"greet":
//!   {"salutation": "hi"}}`. Options are literal JSON, not shape expressions; a link with no
//!   entry is passed `{}`. `check` requires exactly the fields the action's `Sig` options name,
//!   each of its type.
//! - `as`, default none: a field name. The step's result, its links' outputs merged, goes under
//!   that field as an object, and the rest of the values stay as they were; in `check`'s scope
//!   the field's type is `any`. Without `as`, the outputs join the values, a later field
//!   replacing an earlier one.
//! - `next`, default none: what the step sends, in one of the three forms of [`Next`].
//!
//! ## What a step sends: [`Next`]
//!
//! A next has one of three forms, told apart by their fields:
//!
//! - A [`To`], such as `{"ty": 30, "shape": {"ticket": "$ticket"}}`: one event of that `ty`, or
//!   with `each` one per item of a list.
//! - `{"branch": {..}}`, a [`Branch`]: the `To` of the first case whose `when` equals a field's
//!   value, else the branch's `else`.
//! - `{"wait": {..}}`, a [`Wait`]: waits for an event of the `ty` it names with the same key,
//!   then sends one `To` on a match, or another when none comes in time.
//!
//! ## A next to one `ty`: [`To`]
//!
//! - `ty`, required: the `ty` of the event sent. `check` requires a flow for it.
//! - `shape`, default `{}`: the event's data, field by field, each value a shape expression
//!   worked out from the values. `check` requires a field for each field the receiving flow
//!   takes, of its type; it does not check the other fields.
//! - `delay_ms`, default `0`: the event comes due this many milliseconds after the run.
//! - `each`, default none: the name of a list field. One event is sent per item, its shape
//!   reading the item as `"$item"`; at run time, a field that is absent or not a list sends
//!   none. `check` requires the field's type to be a list.
//!
//! ## A branch: [`Branch`] and [`Case`]
//!
//! - `on`, required: the name of the field to branch on, a bare name and not a shape
//!   expression. An absent field is null.
//! - `cases`, required, and may be empty: the [`Case`]s, in order. A case has two fields:
//!   - `when`, required: a JSON value.
//!   - `next`, required: a [`To`].
//! - `else`, required: the [`To`] sent when no case's `when` equals the field's value.
//!
//! The first case whose `when` equals the field's value sends its `next`. The comparison is of
//! JSON values, so the number `1` does not equal the string `"1"`. `check` requires the field in
//! scope and every `when` of its type.
//!
//! ## A wait: [`Wait`]
//!
//! - `for`, required: the `ty` of the event waited for. `check` requires a flow for it.
//! - `key`, required: a shape expression, worked out from the values at the wait. An event of
//!   the `for` ty matches when the same expression, worked out from that event's data, gives an
//!   equal value. `check` requires the expression to give the same type both ways: in the
//!   step's scope, and over the `takes` of the flow for `for`.
//! - `within_ms`, required: how long the wait lasts, in milliseconds.
//! - `next`, required: the [`To`] sent on the first match before the wait times out.
//! - `timeout`, required: the [`To`] sent when the wait times out with no match.
//! - `late`, default none: the [`To`] sent on the first match after the timeout. Without it, a
//!   late match sends nothing.
//!
//! Each of the three is sent at most once, so a second match sends nothing. All three read the
//! values as they stood at the wait; `next` and `late` also read each field of the matching
//! event's data as `"$got.<field>"`. The matching event's own flow runs as it would without the
//! wait.
//!
//! The wait is itself an event. The step sends an event of its own flow's `ty`, due `within_ms`
//! after the run, whose data is `{"$wait": {"step": <step>, "for": <for>}, "key": <key>, "vals":
//! <values>}`; a match sends `next` with that event as its source. When the event comes due, the
//! processor runs none of the flow's steps. It sends `timeout` when nothing has matched, and the
//! event's 2 says `"timeout"`, or `"matched"` when a match came first.
//!
//! # Shape expressions
//!
//! A `To`'s shape values and a wait's `key` are shape expressions, which [`eval`] works out from
//! the values:
//!
//! - `"$field"`: the field's value, or null when the values do not hold it. A string that starts
//!   with `$` reads the field named by the rest of it, whole: `"$a.b"` reads a field named `a.b`,
//!   not `b` inside `a`.
//! - `{"$concat": [..]}`: the parts, each a shape expression, joined as one string: a string
//!   part as its text, any other value as its JSON text, so the number `2` adds `2` and null adds
//!   `null`.
//! - `{"$": <expression>, "or": <default>}`: the expression's value, or the default when that
//!   value is null, as it is for an absent field. Without `or`, a null value stays null.
//! - `"$item"`, in a `To` with `each`: the item.
//! - `"$got.<field>"`, in a wait's `next` and `late`: that field of the matching event's data.
//! - Anything else, an object without `$concat` or `$` included, is itself.
//!
//! `check` types the wait key, and each shape field the receiving flow takes, with
//! [`type_expr`]: a `"$field"` has the field's type in scope, `$concat` gives `string`, and
//! `{"$": .., "or": ..}` gives the inner expression's type, or `type_of` the default when the
//! expression names something not in scope. `"$item"` has the type of the list's items, and
//! `"$got.<field>"` the type the flow for `for` takes the field as.
//!
//! # Shape types
//!
//! A [`Shape`] maps field names to type names: `string`, `number`, `bool`, `list<string>`,
//! `list<number>` and `any`. `check` compares the names as written. At run time a value's type is
//! what [`type_of`] gives: a JSON string, number or bool is `string`, `number` or `bool`; an array
//! of at least one item, every item a number, is `list<number>`, and any other array, an empty
//! one included, is `list<string>`; an object or null is `any`. The processor checks an action's
//! output against its `Sig`'s gives by these names, so a field it declares as `any` must hold an
//! object or null.
//!
//! # `$graph`
//!
//! An action gives graph rows in a `$graph` field of its output: a list, each item a row for the
//! app's append to write. The processor takes `$graph` out of the output before it checks the
//! output or merges it, so the later links and the nexts never see it, and a `Sig` must not
//! declare it in its gives. A `$graph` that is not a list fails the step with `<link> gave $graph
//! as <type>, not list`. The rows of every link in the run are kept in order and, when the run
//! succeeds, held in [`Processor::writes`] under the event's id; a failed run keeps none.
//! rannoch-graph's `GraphAppend` takes them with the event's 2 and writes them in the same
//! transaction; its `Op` is the form of an item, as in `{"set": {"node": "n1", "key": "name",
//! "value": "Sam"}}`. rannoch-events' `Events` made `with_writes` takes them and writes only the
//! `store` rows among them. With any other append, nothing takes them.
//!
//! # Built-in steps
//!
//! A step can name `emit` or `store` with no action registered for it and no signature exported
//! for it: `check` and the processor know both, and an app action of the same name replaces it.
//! Neither reaches outside rannoch.
//!
//! - `emit` takes, gives and has no options: its step's `next`, a branch, an each, a delay or a
//!   wait, is all it does.
//! - `store`, with options `{"table": T, "row": {column: expression}}`, inserts one row into T,
//!   each value worked out with [`eval`] from the step's input; it never updates, deletes or reads
//!   a row. It gives the row as `$graph`, `{"store": {"table": T, "row": {column: value}}}`, and
//!   the append that writes the event's 2, `GraphAppend` or `Events` made `with_writes`, inserts
//!   it in the same transaction with `INSERT OR IGNORE`, so T's own keys decide whether a rerun
//!   adds a row and a flow that fails writes none. A table or a column the store does not have
//!   fails the step, as `store: no such table: <T>` or `store: table <T> has no column named
//!   <column>`, and leaves the batch alone.
//!
//! # What `check` reports
//!
//! [`check`] returns every error in every document, in order, one line each: `E<n> <file> step
//! <i> <where>: <what>`, where `<where>` is the link's action name, or `next` for an error in the
//! step's next; an error in no step is `E<n> <file>: <what>`. For example: `E3 a.json step 0
//! upper: upper takes text, which nothing before it gives`.
//!
//! - `E1`, a link that names neither an action in the signatures nor a built-in step:
//!   `no action named <link>`.
//! - `E2`, a link's options that do not fit its `Sig`'s options: `the options for <link> do not
//!   parse: <why>`, where `<why>` is `not an object`, ``unknown field `<field>` ``, ``missing
//!   field `<field>` `` or `<field> is <type>, not <type>`.
//! - `E3`, a field that nothing gives: `<link> takes <field>, which nothing before it gives`, or
//!   `branch on <field>, which nothing gives`; or a wait key that does not give the same type both
//!   ways: `wait key <key> does not render the same on both sides (<step> / <for>)`, each side
//!   `Ok("<type>")` or `Err("<why>")`.
//! - `E4`, a field of another type: `<link> takes <field> as <type>, it is <type>`, `branch on
//!   <field> as <type>, a case when <value>`, or `each over <field>, which is not a list`.
//! - `E5`, a field a next does not give: `the next to <flow> gives no <field>, which it takes`,
//!   or `$<field> names nothing in scope` for a field an expression reads.
//! - `E6`, a field a next gives as another type: `the next to <flow> gives <field> as <type>, it
//!   takes <type>`.
//! - `E7`, a `ty` that no flow runs: `the next goes to ty <ty>, which no flow takes`, or `wait for
//!   ty <ty>, which no flow takes`.
//! - `E8`, a `ty` that an earlier document's flow also runs: `ty <ty>, which <file> also takes`,
//!   where `<file>` is the first document of that `ty`.
//!
//! `compile` prints each line to stderr, then panics with `<n> errors in the flow docs in <dir>`,
//! which fails the build.

mod check;
mod doc;

use std::{collections::HashMap, sync::Arc, sync::Mutex, sync::PoisonError, time::Duration};

use chrono::{TimeDelta, Utc};
use rannoch_core::{Event, Sender, Status};
use rannoch_queue::{Flow, Flows, Row};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

pub use check::{check, compile};
pub use doc::{Branch, Case, Flow as FlowDoc, Next, Shape, Step, To, Wait};
pub use doc::{eval, type_expr, type_of};

/// What an action declares: its name, its options, the fields it takes and gives, and whether it
/// is a job. A signatures export is a JSON array of these; a field a `Sig` does not define is
/// refused.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sig {
    /// The name a step's `do` calls the action by.
    pub action: String,
    /// The fields and types the action's options hold, which a step gives in its `with`; empty
    /// by default.
    #[serde(default)]
    pub options: Shape,
    /// The fields and types the action reads, each of which `check` requires something before it
    /// to give, of that type; empty by default.
    #[serde(default)]
    pub takes: Shape,
    /// The fields and types the action's output holds. `check` adds them to the step's scope;
    /// after a step with `as`, only the `as` field, as `any`. The processor fails the step when
    /// an output lacks one or holds it as another type; empty by default.
    #[serde(default)]
    pub gives: Shape,
    /// Whether the action is long-running work; `false` by default. `check` reads it from the
    /// signatures export and sets the flow's `job`: a flow with a job action runs as a job, on a
    /// thread of its own.
    #[serde(default)]
    pub job: bool,
}

/// One of the app's actions, which a step calls by its `Sig`'s `action`.
pub trait Action: Send + Sync {
    /// The action's signature.
    fn sig(&self) -> &Sig;
    /// Runs the action on `options`, the step's `with` for it or `{}` when it has none, and on
    /// `input`, the values with the outputs of the links before it in the step. The output's
    /// fields join the next link's input and the step's result, except `$graph`, which holds
    /// the action's graph rows.
    /// # Errors
    /// The failure, which fails the step.
    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String>;
}

/// The built-in steps' `Sig`s, which `check` and the processor know with no action of the app's:
/// `emit`, and `store` with its options `table` and `row`.
pub(crate) fn builtins() -> [Sig; 2] {
    let store = [("table", "string"), ("row", "any")].map(|(f, t)| (f.to_owned(), t.to_owned()));
    let sig = |action: &str| Sig {
        action: action.to_owned(),
        ..Sig::default()
    };
    let (emit, store) = (
        sig("emit"),
        Sig {
            options: store.into(),
            ..sig("store")
        },
    );
    [emit, store]
}

/// A built-in step on the processor's connection: `emit` gives nothing, and `store` gives its row
/// as `$graph`, `{"store": {"table": T, "row": {column: value}}}`, once the store has T and each
/// column.
struct Builtin(Sig, Arc<Mutex<Connection>>);

impl Action for Builtin {
    fn sig(&self) -> &Sig {
        &self.0
    }

    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String> {
        if self.0.action == "emit" {
            return Ok(Map::new());
        }
        let table = options["table"].as_str().unwrap_or_default();
        let row = options["row"].as_object().into_iter().flatten();
        let row: Map<String, Value> = row.map(|(c, e)| (c.clone(), eval(e, input))).collect();
        rannoch_events::insert(&lock(&self.1), table, row.keys())
            .map_err(|e| format!("store: {e}"))?;
        let item = json!({ "store": { "table": table, "row": row } });
        Ok(Map::from_iter([("$graph".to_owned(), json!([item]))]))
    }
}

/// Graph writes and store rows by the id of the event they are about, until the append that
/// carries its 2 takes them.
pub type Writes = Arc<Mutex<HashMap<Uuid, Vec<Value>>>>;

/// The app's flows, one [`Flow`] for each flow's `ty`: the queue's [`Flows`]. `T` is the app's
/// event type, made from each `ty` with `TryFrom<u16>`, and `D` its data, made from a JSON value.
#[derive(Debug)]
pub struct Processor<T, D> {
    flows: HashMap<u16, Arc<Flow<T, D>>>,
    writes: Writes,
}

impl<T: TryFrom<u16> + PartialEq + 'static, D: From<Value> + 'static> Processor<T, D> {
    /// The processor over `flows`, the JSON array of flow docs that [`compile`] writes and the
    /// app compiles in, and `actions`, found by their `Sig`'s `action`, with the built-in steps
    /// `emit` and `store`, which an action of the same name replaces; `events` reads the waits in
    /// `_events` and prepares each `store`. It does not check the docs: a link that names none of
    /// these fails its step with `no action named <link>`.
    /// # Errors
    /// The error of a flow doc that does not parse.
    pub fn new(
        flows: &str,
        actions: Vec<Arc<dyn Action>>,
        events: Connection,
    ) -> Result<Self, serde_json::Error> {
        let (conn, seen) = (Arc::new(Mutex::new(events)), Arc::new(Mutex::default()));
        let mut by_name = HashMap::new();
        for sig in builtins() {
            let builtin: Arc<dyn Action> = Arc::new(Builtin(sig, conn.clone()));
            by_name.insert(builtin.sig().action.clone(), builtin);
        }
        for action in actions {
            by_name.insert(action.sig().action.clone(), action);
        }
        let docs: Arc<Vec<FlowDoc>> = Arc::new(serde_json::from_str(flows)?);
        let mut by_ty: HashMap<u16, Arc<Flow<T, D>>> = HashMap::new();
        let writes = Writes::default();
        for doc in docs.iter().cloned() {
            let (actions, ty, job) = (by_name.clone(), doc.ty, doc.job);
            let (docs, conn, seen, writes) =
                (docs.clone(), conn.clone(), seen.clone(), writes.clone());
            let process = move |event: &Row, root: &Row, sender: &Sender<T, D>| {
                let (id, at, mut graph) = (event.id, (root.id, event.ty), Vec::new());
                let ty = |ty| T::try_from(ty).map_err(|_| format!("ty {ty} is not the app's"));
                let sends = serde_json::from_str(&root.data)
                    .map_err(|e| format!("root data: {e}"))
                    .and_then(|data: Map<String, Value>| {
                        let (mut outs, said) = waits(&lock(&conn), &lock(&seen), &docs, at, &data)?;
                        if said == "done" {
                            let nexts = run(&doc, &actions, data, &mut graph)?.into_iter();
                            outs.extend(nexts.map(|(to, data, delay)| (id, to, data, delay)));
                        }
                        // A match answers its waits; the wait's own run, or a rerun's, does not.
                        let answers = |s: &Uuid| *s != id && *s != at.0;
                        lock(&seen).extend(outs.iter().map(|o| o.0).filter(answers));
                        let done =
                            ty(event.ty).map(|t| (id, t, said.into(), None, Status::Processed));
                        let nexts = outs.into_iter().map(|(source, to, data, delay)| {
                            let at = TimeDelta::from_std(Duration::from_millis(delay)).ok();
                            let at = at.and_then(|d| Utc::now().checked_add_signed(d));
                            Ok((source, ty(to)?, Value::Object(data), at, Status::Added))
                        });
                        nexts.chain([done]).collect::<Result<Vec<_>, String>>()
                    });
                let (sends, result) = match sends {
                    Ok(sends) => {
                        if !graph.is_empty() {
                            lock(&writes).insert(id, graph);
                        }
                        (sends, Ok(()))
                    }
                    Err(e) => {
                        let binned = (id, ty(event.ty)?, e.as_str().into(), None, Status::Binned);
                        (vec![binned], Err(e))
                    }
                };
                for (source, ty, data, run_at, status) in sends {
                    let (id, source, data) = (Uuid::nil(), Some(source), D::from(data));
                    let sent = Event {
                        id,
                        source,
                        ty,
                        data,
                        run_at,
                        status,
                    };
                    if sender.send(sent).is_err() {
                        log::error!("could not send about {}: rannoch is closed", event.id);
                    }
                }
                result
            };
            let run = Box::new(process);
            by_ty.insert(ty, Arc::new(Flow { job, run }));
        }
        Ok(Self {
            flows: by_ty,
            writes,
        })
    }
}

impl<T, D> Processor<T, D> {
    /// The processor's writes, to hand to the append that writes them.
    #[must_use]
    pub fn writes(&self) -> Writes {
        self.writes.clone()
    }
}

impl<T, D> Flows<T, D> for Processor<T, D> {
    fn flow(&self, ty: u16) -> Option<Arc<Flow<T, D>>> {
        self.flows.get(&ty).cloned()
    }
}

fn lock<X>(mutex: &Mutex<X>) -> std::sync::MutexGuard<'_, X> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The waits in `_events` waiting for `ty`: (id, data), and the rows about each.
#[expect(clippy::type_complexity, reason = "the issue's signature")]
fn waits_for(
    conn: &Connection,
    ty: u16,
) -> rusqlite::Result<Vec<(Uuid, Value, Vec<(i8, String)>)>> {
    let sql = "SELECT w.id, w.data, json_group_array(json_array(s.status, s.data))
        FILTER (WHERE s.id IS NOT NULL) FROM _events w LEFT JOIN _events s ON s.source = w.id
        WHERE json_extract(w.data, '$.\"$wait\".for') = ?1 GROUP BY w.id ORDER BY w.id";
    let row = |r: &rusqlite::Row<'_>| {
        let (id, data, about): (String, String, String) = (r.get(0)?, r.get(1)?, r.get(2)?);
        let (data, about) = (serde_json::from_str(&data), serde_json::from_str(&about));
        let id = id.parse().unwrap_or_default();
        Ok((id, data.unwrap_or_default(), about.unwrap_or_default()))
    };
    conn.prepare_cached(sql)?.query_map([ty], row)?.collect()
}

/// The 2 of a retry or resend that ran the wait `id`, found down the chain of retries and
/// resends below it, whose data are strings.
fn rerun_two(conn: &Connection, id: Uuid) -> Result<Option<String>, String> {
    let sql = "WITH RECURSIVE chain(id) AS (SELECT ?1
        UNION SELECT e.id FROM _events e JOIN chain c ON e.source = c.id
        WHERE e.status = 0 AND e.data NOT LIKE '{%')
        SELECT s.data FROM chain c JOIN _events s ON s.source = c.id WHERE s.status = 2 LIMIT 1";
    let mut select = conn.prepare_cached(sql).map_err(|e| e.to_string())?;
    let mut twos = select
        .query_map([id.to_string()], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    twos.next().transpose().map_err(|e| e.to_string())
}

/// What an event of `ty` whose root is `id` sends about waits as (source, ty, data, delay), and
/// its 2's data.
#[expect(clippy::type_complexity, reason = "a next with its source")]
fn waits<'d>(
    conn: &Connection,
    answered: &std::collections::HashSet<Uuid>,
    docs: &'d [FlowDoc],
    (id, ty): (Uuid, u16),
    data: &Map<String, Value>,
) -> Result<(Vec<(Uuid, u16, Map<String, Value>, u64)>, &'static str), String> {
    let due = data.get("$wait");
    let ty = due.map_or(Some(ty), |at| at["for"].as_u64()?.try_into().ok());
    let (mut outs, mut said) = (Vec::new(), if due.is_some() { "matched" } else { "done" });
    for (wait_id, w, rows) in waits_for(conn, ty.unwrap_or(0)).map_err(|e| e.to_string())? {
        let step: Option<usize> = w["$wait"]["step"].as_u64().and_then(|s| s.try_into().ok());
        let at = |d: &'d FlowDoc| d.steps.get(step?)?.next.as_ref();
        let fits = |wait: &Wait| due.map_or(eval(&wait.key, data) == w["key"], |_| wait_id == id);
        let is = |n: &&Next| matches!(n, Next::Wait { wait: x } if Some(x.for_ty) == ty && fits(x));
        if let Some(Next::Wait { wait }) = docs.iter().filter_map(at).find(is) {
            // The wait's nexts are objects; the queue's retry and the hunt's resend are strings.
            let zeros = rows
                .iter()
                .filter(|r| r.0 == 0 && r.1.starts_with('{'))
                .count();
            let two = rows.iter().find(|r| r.0 == 2).map(|r| r.1.clone());
            let two = match two {
                None if due.is_none() && zeros == 1 => rerun_two(conn, wait_id)?,
                two => two,
            };
            let to = match (
                due.is_some(),
                answered.contains(&wait_id),
                zeros,
                two.as_deref(),
            ) {
                (true, false, 0, _) => Some(&wait.timeout).inspect(|_| said = "timeout"),
                (false, false, 0, None) => Some(&wait.next),
                (false, false, 1, Some("\"timeout\"")) => wait.late.as_ref(),
                _ => None,
            };
            let mut vals = w["vals"].as_object().cloned().unwrap_or_default();
            let got = data.iter().filter(|_| due.is_none());
            vals.extend(got.map(|(k, v)| (format!("got.{k}"), v.clone())));
            let nexts = to.into_iter().flat_map(|to| shaped(to, &vals));
            outs.extend(nexts.map(|(to, d, delay)| (wait_id, to, d, delay)));
        }
    }
    Ok((outs, said))
}

/// Runs `flow` on the root's data: per step, each link of the chain in order, its options
/// from `with`, its input the options so far, its output checked against its sig's gives and
/// merged in (later wins), the step's result put under `as` when set; then the step's next is
/// shaped with `eval`, a wait as an event of the flow's ty. Returns them as (ty, data, delay).
/// A link's `$graph` is taken out of its output first: a list joins `graph`; else the step fails.
#[expect(clippy::type_complexity, reason = "the issue's signature")]
fn run(
    flow: &FlowDoc,
    actions: &HashMap<String, Arc<dyn Action>>,
    data: Map<String, Value>,
    graph: &mut Vec<Value>,
) -> Result<Vec<(u16, Map<String, Value>, u64)>, String> {
    let (mut vals, mut nexts) = (data, Vec::new());
    for (i, step) in flow.steps.iter().enumerate() {
        let (mut input, mut result) = (vals.clone(), Map::new());
        for link in step.chain.split('.') {
            let action = actions.get(link).ok_or(format!("no action named {link}"))?;
            let options = step.with.get(link).cloned().unwrap_or(json!({}));
            let mut out = action.run(&options, &input)?;
            match out.remove("$graph") {
                Some(Value::Array(mut writes)) => graph.append(&mut writes),
                Some(g) => return Err(format!("{link} gave $graph as {}, not list", type_of(&g))),
                None => {}
            }
            for (field, ty) in &action.sig().gives {
                match out.get(field).map(type_of) {
                    Some(t) if t == ty => {}
                    Some(t) => return Err(format!("{link} gave {field} as {t}, not {ty}")),
                    None => return Err(format!("{link} gave no {field}")),
                }
            }
            input.extend(out.clone());
            result.extend(out);
        }
        match &step.under {
            Some(field) => drop(vals.insert(field.clone(), Value::Object(result))),
            None => vals = input,
        }
        let to = match &step.next {
            None => continue,
            Some(Next::To(to)) => to,
            Some(Next::Branch { branch }) => {
                let on = vals.get(&branch.on).unwrap_or(&Value::Null);
                let case = branch.cases.iter().find(|c| &c.when == on);
                case.map_or(&branch.otherwise, |c| &c.next)
            }
            Some(Next::Wait { wait }) => {
                let at = json!({ "step": i, "for": wait.for_ty });
                let data = json!({ "$wait": at, "key": eval(&wait.key, &vals), "vals": vals });
                let Value::Object(data) = data else { continue };
                nexts.push((flow.ty, data, wait.within_ms));
                continue;
            }
        };
        nexts.extend(shaped(to, &vals));
    }
    Ok(nexts)
}

/// The nexts `to` sends from `vals`: one, or with `each` one per item of the list it names,
/// each shaped with the item in scope as "$item".
fn shaped(to: &To, vals: &Map<String, Value>) -> Vec<(u16, Map<String, Value>, u64)> {
    let list = |l: &String| vals.get(l).and_then(Value::as_array).into_iter().flatten();
    let items: Option<Vec<_>> = to.each.as_ref().map(|l| list(l).map(Some).collect());
    let next = |item: Option<&Value>| {
        let mut vals = vals.clone();
        vals.extend(item.map(|item| ("item".to_owned(), item.clone())));
        let data = to.shape.iter().map(|(k, e)| (k.clone(), eval(e, &vals)));
        (to.ty, data.collect(), to.delay_ms)
    };
    items.unwrap_or(vec![None]).into_iter().map(next).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use rannoch_core::{Append, Config, Event, Rannoch, Status};
    use rannoch_queue::Flows;
    use rusqlite::Connection;
    use serde_json::{Map, Value, json};
    use uuid::Uuid;

    use super::{Action, FlowDoc, Processor, Sig};

    type Out = (Uuid, u16, Map<String, Value>, u64);

    type Calls = Vec<(String, Map<String, Value>)>;
    type Ran = Result<Vec<(u16, Map<String, Value>, u64)>, String>;
    type Act = fn(&Map<String, Value>) -> Result<Map<String, Value>, String>;

    /// An action that records its name and input in `calls`, then does `act`.
    struct Link(Sig, Act, Arc<Mutex<Calls>>);
    impl Action for Link {
        fn sig(&self) -> &Sig {
            &self.0
        }
        fn run(&self, _: &Value, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
            if let Ok(mut calls) = self.2.lock() {
                calls.push((self.0.action.clone(), input.clone()));
            }
            (self.1)(input)
        }
    }

    fn obj(v: &Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    /// The actions `links` (name, gives, act), each recording its calls in `calls`.
    fn actions(
        links: &[(&str, &[&str], Act)],
        calls: &Arc<Mutex<Calls>>,
    ) -> HashMap<String, Arc<dyn Action>> {
        let mut actions: HashMap<String, Arc<dyn Action>> = HashMap::new();
        for (name, gives, act) in links {
            let gives = gives
                .iter()
                .map(|g| ((*g).to_owned(), "string".to_owned()))
                .collect();
            let sig = Sig {
                action: (*name).to_owned(),
                gives,
                ..Sig::default()
            };
            actions.insert(
                (*name).to_owned(),
                Arc::new(Link(sig, *act, Arc::clone(calls))),
            );
        }
        actions
    }

    /// Runs the flow doc `flow` on `data` over the actions `links` (name, gives, act).
    fn run(flow: &Value, data: &Value, links: &[(&str, &[&str], Act)]) -> (Ran, Calls) {
        let calls = Arc::default();
        let actions = actions(links, &calls);
        let flow: FlowDoc = serde_json::from_value(flow.clone()).unwrap();
        let ran = super::run(&flow, &actions, obj(data), &mut Vec::new());
        let calls = calls.lock().unwrap().clone();
        (ran, calls)
    }

    #[test]
    fn a_links_graph_is_in_neither_the_next_links_options_nor_the_nexts_data() {
        let flow = json!({ "name": "f", "ty": 1, "takes": {}, "steps": [{ "do": "a.b", "as": "got",
            "next": { "ty": 2, "shape": { "got": "$got" } } }] });
        let flow: FlowDoc = serde_json::from_value(flow).unwrap();
        let calls = Arc::default();
        let actions = actions(
            &[
                ("a", &["x"], |_| {
                    Ok(obj(&json!({ "x": "a", "$graph": [{ "node": 1 }] })))
                }),
                ("b", &[], |_| Ok(obj(&json!({ "$graph": [{ "node": 2 }] })))),
            ],
            &calls,
        );
        let mut graph = Vec::new();

        let ran = super::run(&flow, &actions, Map::new(), &mut graph);

        assert_eq!(calls.lock().unwrap()[1].1, obj(&json!({ "x": "a" })));
        assert_eq!(ran.unwrap(), [(2, obj(&json!({ "got": { "x": "a" } })), 0)]);
        assert_eq!(graph, [json!({ "node": 1 }), json!({ "node": 2 })]);
    }

    /// Runs an event of `ty`, whose root is another event, through the processor's flow for
    /// `ty`: 70 gives a write and succeeds, 71 gives a write and then fails, 72 gives a write and
    /// then a `$graph` that is not a list. Returns the run's result, the event's id, what the
    /// processor's writes then hold and what it sent.
    fn wrote(ty: u16) -> (Result<(), String>, Uuid, HashMap<Uuid, Vec<Value>>, Sent) {
        let flows = json!([
            { "name": "writes", "ty": 70, "takes": {}, "steps": [{ "do": "a" }] },
            { "name": "fails", "ty": 71, "takes": {}, "steps": [{ "do": "a.fail" }] },
            { "name": "odd", "ty": 72, "takes": {}, "steps": [{ "do": "a.odd", "next": { "ty": 73 } }] }
        ]);
        let links = actions(
            &[
                ("a", &[], |_| {
                    Ok(obj(&json!({ "$graph": [{ "node": "n1" }] })))
                }),
                ("fail", &[], |_| Err("fail: no".to_owned())),
                ("odd", &[], |_| Ok(obj(&json!({ "$graph": "n1" })))),
            ],
            &Arc::default(),
        );
        let links = links.into_values().collect();
        let conn = events(Uuid::now_v7(), &[]);
        let processor = Processor::<u16, Value>::new(&flows.to_string(), links, conn).unwrap();
        let kept = Arc::new(Kept::default());
        let append = Some(kept.clone() as Arc<dyn Append<u16, Value>>);
        let rannoch = Rannoch::open(Config {
            append,
            ..Config::default()
        })
        .unwrap();
        let (root, event) = (Uuid::now_v7(), Uuid::now_v7());
        let row = |id, source| Event {
            id,
            source,
            ty,
            data: "{}".to_owned(),
            run_at: None,
            status: Status::Added,
        };
        let flow = processor.flow(ty).unwrap();
        let ran = (flow.run)(&row(event, Some(root)), &row(root, None), &rannoch.sender());
        rannoch.shutdown();
        let writes = processor.writes().lock().unwrap().clone();
        let sent = kept.0.lock().unwrap().clone();
        (ran, event, writes, sent)
    }

    #[test]
    fn a_flow_that_succeeds_leaves_its_writes_under_the_events_id() {
        let (ran, event, writes, _) = wrote(70);
        assert_eq!(ran, Ok(()));
        let node = json!({ "node": "n1" });
        assert_eq!(writes, HashMap::from([(event, vec![node])]));
    }

    #[test]
    fn a_failing_step_leaves_no_writes() {
        let (ran, _, writes, _) = wrote(71);
        assert_eq!(ran, Err("fail: no".to_owned()));
        assert!(writes.is_empty());
    }

    #[test]
    fn a_graph_that_is_not_a_list_fails_the_step_with_one_binned_no_next_and_no_writes() {
        let (ran, event, writes, sent) = wrote(72);
        let failure = "odd gave $graph as string, not list";
        assert_eq!(ran, Err(failure.to_owned()));
        assert_eq!(sent, [(72, Some(event), -1, json!(failure))]);
        assert!(writes.is_empty());
    }

    #[test]
    fn run_merges_each_links_output_into_the_next_links_input_the_later_winning() {
        let flow = json!({ "name": "f", "ty": 1, "takes": {}, "steps": [{ "do": "a.b.c",
            "next": { "ty": 2, "shape": { "x": "$x", "y": "$y", "keep": "$keep" } } }] });
        let (ran, calls) = run(
            &flow,
            &json!({ "x": "root", "keep": "k" }),
            &[
                ("a", &["x", "y"], |_| {
                    Ok(obj(&json!({ "x": "a", "y": "a" })))
                }),
                ("b", &["x"], |_| Ok(obj(&json!({ "x": "b" })))),
                ("c", &[], |_| Ok(Map::new())),
            ],
        );
        assert_eq!(calls[1].1, obj(&json!({ "x": "a", "y": "a", "keep": "k" })));
        assert_eq!(calls[2].1, obj(&json!({ "x": "b", "y": "a", "keep": "k" })));
        let next = obj(&json!({ "x": "b", "y": "a", "keep": "k" }));
        assert_eq!(ran.unwrap(), [(2, next, 0)]);
    }

    #[test]
    fn run_puts_a_steps_result_under_as_and_keeps_the_input() {
        let flow = json!({ "name": "f", "ty": 1, "takes": {}, "steps": [{ "do": "a", "as": "got",
            "next": { "ty": 2, "shape": { "got": "$got", "text": "$text" } } }] });
        let (ran, _) = run(
            &flow,
            &json!({ "text": "sam" }),
            &[("a", &["text"], |_| Ok(obj(&json!({ "text": "SAM" }))))],
        );
        let next = obj(&json!({ "got": { "text": "SAM" }, "text": "sam" }));
        assert_eq!(ran.unwrap(), [(2, next, 0)]);
    }

    #[test]
    fn run_returns_the_failure_naming_the_field_an_actions_output_lacks() {
        let flow = json!({ "name": "f", "ty": 1, "takes": {}, "steps": [{ "do": "rate" }] });
        let (ran, _) = run(
            &flow,
            &json!({}),
            &[("rate", &["severity"], |_| Ok(Map::new()))],
        );
        assert_eq!(ran.unwrap_err(), "rate gave no severity");
        let (ran, _) = run(
            &flow,
            &json!({}),
            &[("rate", &["severity"], |_| {
                Ok(obj(&json!({ "severity": 3 })))
            })],
        );
        assert_eq!(ran.unwrap_err(), "rate gave severity as number, not string");
    }

    #[test]
    fn run_stops_at_a_failing_link_and_no_later_link_runs() {
        let flow = json!({ "name": "f", "ty": 1, "takes": {}, "steps": [{ "do": "a.fail.c",
            "next": { "ty": 2 } }] });
        let (ran, calls) = run(
            &flow,
            &json!({}),
            &[
                ("a", &[], |_| Ok(Map::new())),
                ("fail", &[], |_| Err("fail: no".to_owned())),
                ("c", &[], |_| Ok(Map::new())),
            ],
        );
        assert_eq!(ran.unwrap_err(), "fail: no");
        let names: Vec<&str> = calls.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["a", "fail"]);
    }
    #[test]
    fn run_with_a_branch_picks_the_case_by_json_value_and_falls_to_else() {
        let flow = json!({ "name": "f", "ty": 1, "takes": {}, "steps": [{ "do": "a",
            "next": { "branch": { "on": "level", "cases": [{ "when": 1, "next": { "ty": 31 } }],
                "else": { "ty": 33 } } } }] });
        let ran = |level: Value| {
            let (ran, _) = run(
                &flow,
                &json!({ "level": level }),
                &[("a", &[], |_| Ok(Map::new()))],
            );
            ran.unwrap().iter().map(|n| n.0).collect::<Vec<u16>>()
        };
        assert_eq!(ran(json!(1)), [31]);
        assert_eq!(ran(json!("1")), [33]);
        assert_eq!(ran(json!(2)), [33]);
    }

    #[test]
    fn store_gives_its_row_as_graph_and_fails_naming_a_table_or_column_the_store_does_not_have() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER)")
            .unwrap();
        let [_, sig] = super::builtins();
        let store = super::Builtin(sig, Arc::new(Mutex::new(conn)));
        let input = obj(&json!({ "k": "a", "m": 2 }));
        let ran =
            |table: &str, row: Value| store.run(&json!({ "table": table, "row": row }), &input);

        let item = json!({ "store": { "table": "t", "row": { "k": "a", "n": 2 } } });
        let row = json!({ "k": "$k", "n": "$m" });
        assert_eq!(ran("t", row.clone()), Ok(obj(&json!({ "$graph": [item] }))));
        assert_eq!(
            ran("nope", row),
            Err("store: no such table: nope".to_owned())
        );
        let column = Err("store: table t has no column named x".to_owned());
        assert_eq!(ran("t", json!({ "x": "$k" })), column);
    }

    #[test]
    fn shaped_with_each_gives_one_next_per_item_with_item() {
        let to = json!({ "ty": 41, "each": "list", "shape": { "word": "$item", "n": "$n" } });
        let to: super::To = serde_json::from_value(to).unwrap();
        let nexts = super::shaped(&to, &obj(&json!({ "list": ["a", "b", "c"], "n": 7 })));
        let words = ["a", "b", "c"].map(|w| (41, obj(&json!({ "word": w, "n": 7 })), 0));
        assert_eq!(nexts, words);
    }

    /// The order flow 60 waits for a payment, 61, keyed by its order; and the payment flow.
    fn waiting() -> Vec<FlowDoc> {
        let wait = json!({ "for": 61, "key": "$order", "within_ms": 2000,
            "next": { "ty": 62, "shape": { "order": "$order", "paid": "$got.amount" } },
            "timeout": { "ty": 63, "shape": { "order": "$order" } },
            "late": { "ty": 64, "shape": { "paid": "$got.amount" } } });
        serde_json::from_value(json!([
            { "name": "order", "ty": 60, "takes": {}, "steps": [{ "do": "a", "next": { "wait": wait } }] },
            { "name": "payment", "ty": 61, "takes": {}, "steps": [] }
        ]))
        .unwrap()
    }

    /// `_events` as the store's migration makes it, holding the wait `w` for order o1 and the
    /// `rows` about it, as (status, data).
    fn events(w: Uuid, rows: &[(i8, Value)]) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE _events (id TEXT PRIMARY KEY, source TEXT, ty INTEGER NOT NULL,
            data TEXT NOT NULL, run_at TEXT, status INTEGER NOT NULL)",
        )
        .unwrap();
        let wait = json!({ "$wait": { "step": 0, "for": 61 }, "key": "o1",
            "vals": { "order": "o1" } });
        let rows = rows
            .iter()
            .map(|(s, d)| (Uuid::now_v7(), Some(w), *s, d.clone()));
        for (id, source, status, data) in [(w, None, 0, wait)].into_iter().chain(rows) {
            let (id, source) = (id.to_string(), source.map(|s| s.to_string()));
            let row = (id, source, data.to_string(), status);
            conn.execute("INSERT INTO _events VALUES (?1, ?2, 60, ?3, NULL, ?4)", row)
                .unwrap();
        }
        conn
    }

    fn pay(conn: &Connection, answered: &HashSet<Uuid>, order: &str) -> (Vec<Out>, &'static str) {
        let data = obj(&json!({ "order": order, "amount": 5 }));
        super::waits(conn, answered, &waiting(), (Uuid::now_v7(), 61), &data).unwrap()
    }

    fn due(conn: &Connection, w: Uuid) -> (Vec<Out>, &'static str) {
        let data: String = conn
            .query_row(
                "SELECT data FROM _events WHERE id = ?1",
                [w.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        let data = serde_json::from_str(&data).unwrap();
        super::waits(conn, &HashSet::new(), &waiting(), (w, 60), &data).unwrap()
    }

    /// What rannoch was handed, as (ty, source, status, data).
    type Sent = Vec<(u16, Option<Uuid>, i8, Value)>;

    /// What rannoch hands over, kept off `_events`.
    #[derive(Debug, Default)]
    struct Kept(Mutex<Sent>);
    impl Append<u16, Value> for Kept {
        fn append(&self, batch: &[Event<u16, Value>]) -> Result<(), rannoch_core::Error> {
            let mut kept = self.0.lock().unwrap();
            kept.extend(
                batch
                    .iter()
                    .map(|e| (e.ty, e.source, e.status as i8, e.data.clone())),
            );
            Ok(())
        }
    }

    /// Runs each payment for `orders` through the processor's flow 61 over `conn`, its sends
    /// never reaching `conn`; returns what was sent, as (ty, source).
    fn through(conn: Connection, orders: &[&str]) -> Vec<(u16, Option<Uuid>)> {
        let kept = Arc::new(Kept::default());
        let append = Some(kept.clone() as Arc<dyn Append<u16, Value>>);
        let rannoch = Rannoch::open(Config {
            append,
            ..Config::default()
        })
        .unwrap();
        let docs = serde_json::to_string(&waiting()).unwrap();
        let processor = Processor::<u16, Value>::new(&docs, vec![], conn).unwrap();
        let flow = processor.flow(61).unwrap();
        for order in orders {
            let data = json!({ "order": order, "amount": 5 }).to_string();
            let (id, source, run_at, status) = (Uuid::now_v7(), None, None, Status::Added);
            let row = Event {
                id,
                source,
                ty: 61,
                data,
                run_at,
                status,
            };
            (flow.run)(&row, &row, &rannoch.sender()).unwrap();
        }
        rannoch.shutdown();
        let sent = kept.0.lock().unwrap().clone();
        let sent = sent.into_iter().filter(|s| s.0 != 61);
        sent.map(|(ty, source, _, _)| (ty, source)).collect()
    }

    #[test]
    fn a_wait_is_an_event_of_the_flows_ty_with_its_step_key_and_vals_due_within_ms() {
        let (ran, _) = run(
            &serde_json::to_value(&waiting()[0]).unwrap(),
            &json!({ "order": "o1" }),
            &[("a", &[], |_| Ok(Map::new()))],
        );
        let data = obj(&json!({ "$wait": { "step": 0, "for": 61 }, "key": "o1",
            "vals": { "order": "o1" } }));
        assert_eq!(ran.unwrap(), [(60, data, 2000)]);
    }

    #[test]
    fn a_matching_event_adds_the_match_next_once_and_a_second_adds_none() {
        let w = Uuid::now_v7();
        let conn = events(w, &[]);
        let next = (w, 62, obj(&json!({ "order": "o1", "paid": 5 })), 0);
        assert_eq!(
            pay(&conn, &HashSet::new(), "o1"),
            (vec![next.clone()], "done")
        );
        assert_eq!(pay(&conn, &HashSet::new(), "o2"), (vec![], "done"));
        assert_eq!(pay(&conn, &HashSet::from([w]), "o1"), (vec![], "done"));
        let conn = events(w, &[(0, Value::Object(next.2))]);
        assert_eq!(pay(&conn, &HashSet::new(), "o1"), (vec![], "done"));
        // Through the processor, the first match's next still on its way: sent once.
        assert_eq!(through(events(w, &[]), &["o1", "o1"]), [(62, Some(w))]);
    }

    #[test]
    fn a_due_wait_with_no_match_sends_its_timeout_and_says_timeout() {
        let w = Uuid::now_v7();
        let timeout = (w, 63, obj(&json!({ "order": "o1" })), 0);
        assert_eq!(due(&events(w, &[]), w), (vec![timeout], "timeout"));
        let matched = events(w, &[(0, json!({ "order": "o1", "paid": 5 }))]);
        assert_eq!(due(&matched, w), (vec![], "matched"));
    }

    #[test]
    fn a_match_after_timeout_sends_late_once() {
        let w = Uuid::now_v7();
        let timed_out = [(0, json!({ "order": "o1" })), (2, json!("timeout"))];
        let late = (w, 64, obj(&json!({ "paid": 5 })), 0);
        assert_eq!(
            pay(&events(w, &timed_out), &HashSet::new(), "o1"),
            (vec![late], "done")
        );
        let sent = [
            timed_out[0].clone(),
            timed_out[1].clone(),
            (0, json!({ "paid": 5 })),
        ];
        assert_eq!(
            pay(&events(w, &sent), &HashSet::new(), "o1"),
            (vec![], "done")
        );
        assert_eq!(
            pay(&events(w, &timed_out), &HashSet::from([w]), "o1"),
            (vec![], "done")
        );
    }

    #[test]
    fn waits_for_searches_the_wait_index_on_a_migrated_store() {
        use rannoch_core::Storage;
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/tmp/rannoch-processor-wait-index");
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        rannoch_store::Store::new(path.clone(), rannoch_core::migrator())
            .apply()
            .unwrap();
        let rannoch_store::Opened::Ready(conn) =
            rannoch_store::open(&path, &rannoch_core::migrator()).unwrap()
        else {
            panic!("migrations pending");
        };
        // A copy of `waits_for`'s SQL, which the issue keeps unchanged.
        let sql =
            "EXPLAIN QUERY PLAN SELECT w.id, w.data, json_group_array(json_array(s.status, s.data))
        FILTER (WHERE s.id IS NOT NULL) FROM _events w LEFT JOIN _events s ON s.source = w.id
        WHERE json_extract(w.data, '$.\"$wait\".for') = ?1 GROUP BY w.id ORDER BY w.id";
        let plan: Vec<String> = conn
            .prepare(sql)
            .unwrap()
            .query_map([61], |r| r.get(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|p| p.starts_with("SEARCH w USING INDEX _events_wait")),
            "{plan:#?}"
        );
    }
}
