//! rannoch-processor runs the app's flows. The app compiles its flow docs in and hands them, with
//! the actions of the processor crates it uses, to [`Processor::new`]; the queue hands each event
//! to the flow for its `ty` through [`Flows`]. A step's chain runs its links within the one
//! event, each link's output merged into the options passed on, the later winning, every link
//! reading the root's data; each action's real output is checked against what it declares it
//! gives. A failing link sends one -1 and nothing more; otherwise the flow's nexts are sent, then
//! its 2.
//!
//! Each processor crate exports its actions' signatures as data; the app's build.rs gathers them
//! with [`gather`], its own included, and checks its flow docs against them with [`check`], which
//! lists every error or gives the docs to compile in, each with its job flag set.

mod check;
mod doc;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use rannoch_core::{Event, Sender, Status};
use rannoch_queue::{Flow, Flows, Row};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

pub use check::{check, gather};
pub use doc::{Branch, Case, FlowDoc, Next, Route, Shape, Step};

/// An action's signature: its name, its typed options, what it takes, what it declares it gives,
/// and whether it is a job. A processor crate exports its actions' signatures as data.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Signature {
    /// The name a step's chain calls it by.
    pub action: String,
    /// Each option a step gives it, with the option's type.
    #[serde(default)]
    pub options: Shape,
    /// Each field it takes, with the field's type.
    #[serde(default)]
    pub takes: Shape,
    /// Each field its output gives, with the field's type.
    #[serde(default)]
    pub gives: Shape,
    /// Long-running work: a flow with a step that uses it has a job step.
    #[serde(default)]
    pub job: bool,
}

/// One processor action, from a processor crate the app uses.
pub trait Action: Send + Sync {
    /// The action's signature.
    fn signature(&self) -> &Signature;

    /// Runs the action with its own `options` from the step, on `input`: the root's data with the
    /// options the links before it gave merged over it.
    ///
    /// # Errors
    ///
    /// The failure, which fails the step.
    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String>;
}

/// The app's flows, compiled in, run by `ty`.
pub struct Processor<T, D> {
    flows: HashMap<u16, Arc<Flow<T, D>>>,
}

impl<T, D> std::fmt::Debug for Processor<T, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Processor")
            .field("flows", &self.flows.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl<T, D> Processor<T, D>
where
    T: TryFrom<u16> + PartialEq + 'static,
    D: From<Value> + 'static,
{
    /// The processor for the flow docs `docs`, each a flow doc's JSON text as [`check`] gives
    /// it, over `actions`; each flow's job step is the doc's job flag.
    ///
    /// # Errors
    ///
    /// The doc that does not parse, and why.
    pub fn new(docs: &[&str], actions: Vec<Arc<dyn Action>>) -> Result<Self, String> {
        let actions: Arc<HashMap<String, Arc<dyn Action>>> = Arc::new(
            actions
                .into_iter()
                .map(|a| (a.signature().action.clone(), a))
                .collect(),
        );
        let mut flows = HashMap::new();
        for text in docs {
            let doc: FlowDoc =
                serde_json::from_str(text).map_err(|err| format!("a flow doc: {err}"))?;
            let (ty, job) = (doc.ty, doc.job);
            let actions = actions.clone();
            let flow = Flow {
                job,
                run: Box::new(move |event: &Row, root: &Row, sender: &Sender<T, D>| {
                    process(&doc.steps, &actions, event, root, sender)
                }),
            };
            flows.insert(ty, Arc::new(flow));
        }
        Ok(Self { flows })
    }
}

impl<T, D> Flows<T, D> for Processor<T, D> {
    fn flow(&self, ty: u16) -> Option<Arc<Flow<T, D>>> {
        self.flows.get(&ty).cloned()
    }
}

/// A next to send: its `ty`, its data and its delay.
type Out = (u16, Map<String, Value>, u64);

/// Runs `steps` on `event`, whose root is `root`, and sends the result: one -1 when a step
/// fails, otherwise every next, then the 2.
fn process<T, D>(
    steps: &[Step],
    actions: &HashMap<String, Arc<dyn Action>>,
    event: &Row,
    root: &Row,
    sender: &Sender<T, D>,
) -> Result<(), String>
where
    T: TryFrom<u16> + PartialEq,
    D: From<Value>,
{
    let (events, result) = outcome(steps, actions, event.id, event.ty, &root.data, Utc::now());
    for event in events {
        send(sender, event);
    }
    result
}

/// What running `steps` on the event `source` of `ty`, whose root's data is `root`, sends at
/// `now`: one -1 when the root's data is not a JSON object or a step fails, otherwise every
/// next, then the 2; and the failure, if any.
fn outcome<T, D>(
    steps: &[Step],
    actions: &HashMap<String, Arc<dyn Action>>,
    source: Uuid,
    ty: u16,
    root: &str,
    now: DateTime<Utc>,
) -> (Vec<Event<T, D>>, Result<(), String>)
where
    T: TryFrom<u16>,
    D: From<Value>,
{
    let Ok(event_ty) = T::try_from(ty) else {
        return (Vec::new(), Err(format!("ty {ty} is not the app's")));
    };
    let nexts = match serde_json::from_str(root) {
        Ok(Value::Object(data)) => run(steps, actions, data),
        _ => Err("root data is not a JSON object".to_owned()),
    }
    .and_then(|outs| {
        outs.into_iter()
            .map(|(next_ty, data, delay)| {
                let next_ty = T::try_from(next_ty)
                    .map_err(|_| format!("next ty {next_ty} is not the app's"))?;
                Ok(next(source, next_ty, data, now, delay))
            })
            .collect::<Result<Vec<_>, String>>()
    });
    match nexts {
        Err(failure) => (
            vec![status(source, event_ty, failure.clone(), Status::Binned)],
            Err(failure),
        ),
        Ok(mut events) => {
            events.push(status(
                source,
                event_ty,
                "done".to_owned(),
                Status::Processed,
            ));
            (events, Ok(()))
        }
    }
}

/// Runs `steps` in order on the root's `data`, the options carrying from step to step; the
/// nexts to send, or the first failure.
fn run(
    steps: &[Step],
    actions: &HashMap<String, Arc<dyn Action>>,
    data: Map<String, Value>,
) -> Result<Vec<Out>, String> {
    let mut vals = data;
    let mut outs = Vec::new();
    for step in steps {
        chain(step, actions, &mut vals)?;
        if let Some(route) = &step.next {
            outs.extend(sends(pick(route, &vals), &vals));
        }
    }
    Ok(outs)
}

/// The next `route` sends over `vals`: its one next, or the next of the branch's first case whose
/// `when` equals the option by JSON value, or else the else next.
fn pick<'a>(route: &'a Route, vals: &Map<String, Value>) -> &'a Next {
    match route {
        Route::Next(next) => next,
        Route::Branch { branch } => {
            let on = vals.get(&branch.on).unwrap_or(&Value::Null);
            branch
                .cases
                .iter()
                .find(|case| case.when == *on)
                .map_or(&branch.otherwise, |case| &case.next)
        }
    }
}

/// What `next` sends over `vals`: one next, or with `each`, one per item of that list, the item
/// as `item`.
fn sends(next: &Next, vals: &Map<String, Value>) -> Vec<Out> {
    let Some(list) = &next.each else {
        return vec![(next.ty, doc::shaped(next, vals), next.delay_ms)];
    };
    let items = vals
        .get(list)
        .and_then(Value::as_array)
        .map_or_default(Vec::as_slice);
    items
        .iter()
        .map(|item| {
            let mut vals = vals.clone();
            vals.insert("item".to_owned(), item.clone());
            (next.ty, doc::shaped(next, &vals), next.delay_ms)
        })
        .collect()
}

/// Runs `step`'s links on `vals`, each link's output merged into what the next link is passed,
/// the later winning; the step's result is merged into `vals`, or put under its `as`. A failing
/// link fails the step and no later link runs.
fn chain(
    step: &Step,
    actions: &HashMap<String, Arc<dyn Action>>,
    vals: &mut Map<String, Value>,
) -> Result<(), String> {
    let mut input = vals.clone();
    let mut result = Map::new();
    for link in step.chain.split('.') {
        let action = actions
            .get(link)
            .ok_or_else(|| format!("no action named {link}"))?;
        let options = step
            .with
            .get(link)
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()));
        let out = action.run(&options, &input)?;
        check_gives(&out, &action.signature().gives, link)?;
        input.extend(out.clone());
        result.extend(out);
    }
    match &step.under {
        Some(field) => {
            vals.insert(field.clone(), Value::Object(result));
        }
        None => *vals = input,
    }
    Ok(())
}

/// Whether `out`, the real output of `action`, gives every field it declares in `gives`, each
/// of its declared type; the mismatch names the field.
fn check_gives(out: &Map<String, Value>, gives: &Shape, action: &str) -> Result<(), String> {
    for (field, declared) in gives {
        match out.get(field) {
            None => return Err(format!("{action} gave no {field}")),
            Some(value) if !matches(value, declared) => {
                return Err(format!(
                    "{action} gave {field} as {}, not {declared}",
                    type_of(value)
                ));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// Whether `value` is of the `declared` type: `any` matches every value, and an empty list
/// matches either list type.
fn matches(value: &Value, declared: &str) -> bool {
    match value {
        _ if declared == "any" => true,
        Value::Array(items) if items.is_empty() => declared.starts_with("list<"),
        _ => type_of(value) == declared,
    }
}

/// The type of a JSON value, in the signature vocabulary.
pub(crate) fn type_of(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "bool",
        Value::Array(items) if !items.is_empty() && items.iter().all(Value::is_number) => {
            "list<number>"
        }
        Value::Array(_) => "list<string>",
        Value::Object(_) | Value::Null => "any",
    }
}

/// A next: a new event whose data is `data`, whose source is `source`, at status 0, due `delay`
/// milliseconds after `now`.
fn next<T, D: From<Value>>(
    source: Uuid,
    ty: T,
    data: Map<String, Value>,
    now: DateTime<Utc>,
    delay: u64,
) -> Event<T, D> {
    let delay = i64::try_from(delay)
        .ok()
        .and_then(TimeDelta::try_milliseconds)
        .unwrap_or(TimeDelta::MAX);
    Event {
        id: Uuid::nil(),
        source: Some(source),
        ty,
        data: D::from(Value::Object(data)),
        run_at: Some(
            now.checked_add_signed(delay)
                .unwrap_or(DateTime::<Utc>::MAX_UTC),
        ),
        status: Status::Added,
    }
}

/// The processor's status event about `source`, with `message` as its data.
fn status<T, D: From<Value>>(source: Uuid, ty: T, message: String, status: Status) -> Event<T, D> {
    Event {
        id: Uuid::nil(),
        source: Some(source),
        ty,
        data: D::from(Value::String(message)),
        run_at: None,
        status,
    }
}

fn send<T: PartialEq, D>(sender: &Sender<T, D>, event: Event<T, D>) {
    let source = event.source;
    if sender.send(event).is_err() {
        log::error!("the processor could not send about {source:?}: rannoch is closed");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use chrono::{TimeDelta, Utc};
    use rannoch_core::{Event, Status};
    use serde_json::{Map, Value, json};
    use uuid::Uuid;

    use rannoch_queue::Flows;

    use super::{
        Action, Out, Processor, Route, Signature, Step, chain, check_gives, next, outcome, pick,
        run, sends,
    };

    fn obj(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    /// The names of the actions run, in order.
    type Calls = Arc<Mutex<Vec<String>>>;

    /// An action that gives `out`, and a marker of what it was passed; each run is recorded in
    /// `calls`.
    struct Gives {
        sig: Signature,
        out: Value,
        calls: Calls,
    }
    impl Action for Gives {
        fn signature(&self) -> &Signature {
            &self.sig
        }
        fn run(
            &self,
            options: &Value,
            input: &Map<String, Value>,
        ) -> Result<Map<String, Value>, String> {
            if let Ok(mut calls) = self.calls.lock() {
                calls.push(self.sig.action.clone());
            }
            let mut out = obj(self.out.clone());
            out.insert(
                format!("{}_saw", self.sig.action),
                Value::Object(input.clone()),
            );
            if !options.is_null() && options.get("fail").is_some() {
                return Err(format!("{} failed", self.sig.action));
            }
            Ok(out)
        }
    }

    fn actions(list: &[(&str, Value)]) -> HashMap<String, Arc<dyn Action>> {
        recorded(list, &Calls::default())
    }

    fn recorded(list: &[(&str, Value)], calls: &Calls) -> HashMap<String, Arc<dyn Action>> {
        list.iter()
            .map(|(name, out)| {
                let action: Arc<dyn Action> = Arc::new(Gives {
                    sig: Signature {
                        action: (*name).to_owned(),
                        gives: obj(out.clone())
                            .keys()
                            .map(|k| (k.clone(), super::type_of(&out[k]).to_owned()))
                            .collect(),
                        ..Signature::default()
                    },
                    out: out.clone(),
                    calls: calls.clone(),
                });
                ((*name).to_owned(), action)
            })
            .collect()
    }

    fn step(v: Value) -> Step {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn each_link_is_passed_the_root_and_what_the_links_before_it_gave_the_later_winning() {
        let acts = actions(&[
            ("a", json!({ "x": 1, "y": 1 })),
            ("b", json!({ "y": 2 })),
            ("c", json!({})),
        ]);
        let mut vals = obj(json!({ "root": "r" }));

        chain(&step(json!({ "do": "a.b.c" })), &acts, &mut vals).unwrap();

        let saw = &vals["c_saw"];
        assert_eq!(
            (&saw["root"], &saw["x"], &saw["y"]),
            (&json!("r"), &json!(1), &json!(2))
        );
        assert_eq!(vals["a_saw"], json!({ "root": "r" }));
    }

    #[test]
    fn a_failing_link_stops_the_chain() {
        let calls = Calls::default();
        let acts = recorded(
            &[("a", json!({})), ("b", json!({})), ("c", json!({}))],
            &calls,
        );
        let mut vals = Map::new();

        let failed = chain(
            &step(json!({ "do": "a.b.c", "with": { "b": { "fail": true } } })),
            &acts,
            &mut vals,
        );

        assert_eq!(failed, Err("b failed".to_owned()));
        assert!(
            vals.is_empty(),
            "the step's options were kept after a failure"
        );
        assert_eq!(
            *calls.lock().unwrap(),
            ["a", "b"],
            "a link ran after b failed"
        );
    }

    #[test]
    fn as_puts_the_result_under_its_field_and_keeps_the_input() {
        let acts = actions(&[("a", json!({ "text": "SAM" }))]);
        let mut vals = obj(json!({ "text": "sam" }));

        chain(&step(json!({ "do": "a", "as": "shout" })), &acts, &mut vals).unwrap();

        assert_eq!(vals["text"], json!("sam"));
        assert_eq!(vals["shout"]["text"], json!("SAM"));
    }

    #[test]
    fn a_link_naming_no_action_fails_the_step() {
        let failed = chain(
            &step(json!({ "do": "nope" })),
            &HashMap::new(),
            &mut Map::new(),
        );

        assert_eq!(failed, Err("no action named nope".to_owned()));
    }

    #[test]
    fn real_output_is_checked_against_what_the_action_declares_it_gives() {
        let gives = obj(json!({ "severity": "number" }))
            .into_iter()
            .map(|(k, v)| (k, v.as_str().unwrap_or_default().to_owned()))
            .collect();

        assert_eq!(
            check_gives(&obj(json!({ "severity": 2 })), &gives, "rate"),
            Ok(())
        );
        assert_eq!(
            check_gives(&obj(json!({})), &gives, "rate"),
            Err("rate gave no severity".to_owned())
        );
        assert_eq!(
            check_gives(&obj(json!({ "severity": "2" })), &gives, "rate"),
            Err("rate gave severity as string, not number".to_owned())
        );
    }

    fn declares(v: Value) -> super::Shape {
        obj(v)
            .into_iter()
            .map(|(k, v)| (k, v.as_str().unwrap_or_default().to_owned()))
            .collect()
    }

    #[test]
    fn a_field_declared_any_takes_every_value() {
        let gives = declares(json!({ "v": "any" }));

        for v in [
            json!("s"),
            json!(1),
            json!(true),
            json!([1]),
            json!({}),
            json!(null),
        ] {
            assert_eq!(
                check_gives(&obj(json!({ "v": v })), &gives, "a"),
                Ok(()),
                "{v}"
            );
        }
    }

    #[test]
    fn a_bool_is_checked_as_a_bool() {
        let gives = declares(json!({ "v": "bool" }));

        assert_eq!(check_gives(&obj(json!({ "v": true })), &gives, "a"), Ok(()));
        assert_eq!(
            check_gives(&obj(json!({ "v": 1 })), &gives, "a"),
            Err("a gave v as number, not bool".to_owned())
        );
    }

    #[test]
    fn a_list_is_checked_by_what_it_holds_and_an_empty_list_is_either() {
        let numbers = declares(json!({ "v": "list<number>" }));
        let strings = declares(json!({ "v": "list<string>" }));

        assert_eq!(
            check_gives(&obj(json!({ "v": [1, 2] })), &numbers, "a"),
            Ok(())
        );
        assert_eq!(
            check_gives(&obj(json!({ "v": ["x"] })), &strings, "a"),
            Ok(())
        );
        assert_eq!(check_gives(&obj(json!({ "v": [] })), &numbers, "a"), Ok(()));
        assert_eq!(check_gives(&obj(json!({ "v": [] })), &strings, "a"), Ok(()));
        assert_eq!(
            check_gives(&obj(json!({ "v": ["x"] })), &numbers, "a"),
            Err("a gave v as list<string>, not list<number>".to_owned())
        );
        assert_eq!(
            check_gives(&obj(json!({ "v": [1] })), &strings, "a"),
            Err("a gave v as list<number>, not list<string>".to_owned())
        );
    }

    #[test]
    fn a_flow_that_runs_sends_its_nexts_then_its_2() {
        let acts = actions(&[("a", json!({ "greeting": "hi" }))]);
        let steps = [step(json!({
            "do": "a",
            "next": { "ty": 20, "delay_ms": 500, "shape": { "msg": { "$concat": ["$greeting", "!"] } } }
        }))];
        let (source, now) = (Uuid::now_v7(), Utc::now());

        let (sent, result): (Vec<Event<u16, Value>>, _) =
            outcome(&steps, &acts, source, 10, &json!({}).to_string(), now);

        assert_eq!(result, Ok(()));
        let sent: Vec<_> = sent
            .iter()
            .map(|e| (e.ty, e.status, e.source, e.data.clone(), e.run_at))
            .collect();
        assert_eq!(
            sent,
            [
                (
                    20,
                    Status::Added,
                    Some(source),
                    json!({ "msg": "hi!" }),
                    Some(now + TimeDelta::milliseconds(500))
                ),
                (10, Status::Processed, Some(source), json!("done"), None),
            ]
        );
    }

    #[test]
    fn a_flow_that_fails_sends_one_minus_1_and_no_next() {
        let acts = actions(&[("a", json!({}))]);
        let steps = [step(json!({
            "do": "a", "with": { "a": { "fail": true } }, "next": { "ty": 20 }
        }))];
        let source = Uuid::now_v7();

        let (sent, result): (Vec<Event<u16, Value>>, _) = outcome(
            &steps,
            &acts,
            source,
            10,
            &json!({}).to_string(),
            Utc::now(),
        );

        assert_eq!(result, Err("a failed".to_owned()));
        let sent: Vec<_> = sent
            .iter()
            .map(|e| (e.ty, e.status, e.source, e.data.clone()))
            .collect();
        assert_eq!(
            sent,
            [(10, Status::Binned, Some(source), json!("a failed"))]
        );
    }

    #[test]
    fn a_later_steps_failure_sends_no_earlier_steps_next() {
        let acts = actions(&[("a", json!({})), ("b", json!({}))]);
        let steps = [
            step(json!({ "do": "a", "next": { "ty": 20 } })),
            step(json!({ "do": "b", "with": { "b": { "fail": true } } })),
        ];
        let source = Uuid::now_v7();

        let (sent, result): (Vec<Event<u16, Value>>, _) =
            outcome(&steps, &acts, source, 10, "{}", Utc::now());

        assert_eq!(result, Err("b failed".to_owned()));
        let sent: Vec<_> = sent.iter().map(|e| (e.ty, e.status)).collect();
        assert_eq!(sent, [(10, Status::Binned)]);
    }

    #[test]
    fn options_carry_from_step_to_step_and_each_steps_next_is_sent_after_the_last() {
        let acts = actions(&[("a", json!({ "x": 1 })), ("b", json!({}))]);
        let steps = [
            step(json!({ "do": "a", "next": { "ty": 20, "shape": { "x": "$x" } } })),
            step(json!({ "do": "b", "next": { "ty": 21, "shape": { "x": "$x" } } })),
        ];

        let outs = run(&steps, &acts, Map::new()).unwrap();

        assert_eq!(
            outs,
            [
                (20, obj(json!({ "x": 1 })), 0),
                (21, obj(json!({ "x": 1 })), 0)
            ],
            "step 1's next was not shaped from step 0's options"
        );
        let (sent, result): (Vec<Event<u16, Value>>, _) =
            outcome(&steps, &acts, Uuid::now_v7(), 10, "{}", Utc::now());
        assert_eq!(result, Ok(()));
        let sent: Vec<_> = sent.iter().map(|e| (e.ty, e.status)).collect();
        assert_eq!(
            sent,
            [
                (20, Status::Added),
                (21, Status::Added),
                (10, Status::Processed)
            ]
        );
    }

    #[test]
    fn a_root_whose_data_is_not_a_json_object_is_refused_with_one_minus_1() {
        let calls = Calls::default();
        let acts = recorded(&[("a", json!({}))], &calls);
        let steps = [step(json!({ "do": "a", "next": { "ty": 20 } }))];
        let source = Uuid::now_v7();

        let (sent, result): (Vec<Event<u16, Value>>, _) = outcome(
            &steps,
            &acts,
            source,
            10,
            &json!("hung").to_string(),
            Utc::now(),
        );

        let refused = "root data is not a JSON object".to_owned();
        assert_eq!(result, Err(refused.clone()));
        let sent: Vec<_> = sent.iter().map(|e| (e.status, e.data.clone())).collect();
        assert_eq!(sent, [(Status::Binned, json!(refused))]);
        assert!(calls.lock().unwrap().is_empty(), "the flow ran on no data");
    }

    #[test]
    fn a_next_whose_ty_is_not_the_apps_fails_the_flow() {
        let acts = actions(&[("a", json!({}))]);
        let steps = [step(json!({ "do": "a", "next": { "ty": 300 } }))];

        let (sent, result): (Vec<Event<u8, Value>>, _) =
            outcome(&steps, &acts, Uuid::now_v7(), 10, "{}", Utc::now());

        assert_eq!(result, Err("next ty 300 is not the app's".to_owned()));
        let sent: Vec<_> = sent.iter().map(|e| (e.ty, e.status)).collect();
        assert_eq!(sent, [(10, Status::Binned)]);
    }

    #[test]
    fn a_flow_gives_each_steps_next_shaped_from_the_options() {
        let acts = actions(&[("a", json!({ "greeting": "hi" }))]);
        let steps = [step(json!({
            "do": "a",
            "next": { "ty": 20, "delay_ms": 500, "shape": { "msg": { "$concat": ["$greeting", "!"] } } }
        }))];

        let outs = run(&steps, &acts, Map::new()).unwrap();

        assert_eq!(outs, [(20, obj(json!({ "msg": "hi!" })), 500)]);
    }

    #[test]
    fn each_flows_job_step_is_its_compiled_docs_job_flag() {
        let docs = [
            json!({ "ty": 10, "job": true }).to_string(),
            json!({ "ty": 20 }).to_string(),
        ];
        let docs: Vec<&str> = docs.iter().map(String::as_str).collect();

        let processor: Processor<u16, Value> = Processor::new(&docs, Vec::new()).unwrap();

        let job = |ty| processor.flow(ty).map(|flow| flow.job);
        assert_eq!((job(10), job(20)), (Some(true), Some(false)));
    }

    #[test]
    fn a_next_is_sourced_at_status_0_and_due_after_its_delay() {
        let (source, now) = (Uuid::now_v7(), Utc::now());

        let sent: Event<u16, Value> = next(source, 20, obj(json!({ "m": 1 })), now, 500);

        assert_eq!(sent.source, Some(source));
        assert_eq!(sent.status, Status::Added);
        assert_eq!(sent.data, json!({ "m": 1 }));
        assert_eq!(sent.run_at, Some(now + TimeDelta::milliseconds(500)));
    }

    fn route(v: Value) -> Route {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn a_branch_sends_the_next_of_the_case_whose_when_equals_the_option_by_json_value() {
        let level = route(json!({ "branch": { "on": "level",
            "cases": [{ "when": 1, "next": { "ty": 31 } }], "else": { "ty": 32 } } }));
        let ty = |vals: Value| pick(&level, &obj(vals)).ty;

        assert_eq!(ty(json!({ "level": 1 })), 31);
        assert_eq!(ty(json!({ "level": "1" })), 32, "\"1\" met when 1");
        assert_eq!(ty(json!({ "level": 2 })), 32);
        assert_eq!(ty(json!({})), 32);
    }

    #[test]
    fn a_branch_takes_the_first_case_that_holds() {
        let value = route(json!({ "branch": { "on": "value", "cases": [
            { "when": "crash", "next": { "ty": 31 } },
            { "when": "question", "next": { "ty": 32 } },
            { "when": "crash", "next": { "ty": 34 } }
        ], "else": { "ty": 33 } } }));
        let ty = |vals: Value| pick(&value, &obj(vals)).ty;

        assert_eq!(ty(json!({ "value": "crash" })), 31);
        assert_eq!(ty(json!({ "value": "question" })), 32);
        assert_eq!(ty(json!({ "value": "other" })), 33);
    }

    #[test]
    fn each_sends_one_next_per_item_shaped_with_the_item() {
        let Route::Next(words) = route(json!({ "ty": 41, "each": "list", "delay_ms": 5,
            "shape": { "word": "$item", "n": "$n" } }))
        else {
            panic!("each is not a next");
        };

        assert_eq!(
            sends(&words, &obj(json!({ "list": ["a", "b", "c"], "n": 1 }))),
            [
                (41, obj(json!({ "word": "a", "n": 1 })), 5),
                (41, obj(json!({ "word": "b", "n": 1 })), 5),
                (41, obj(json!({ "word": "c", "n": 1 })), 5),
            ]
        );
        assert_eq!(
            sends(&words, &obj(json!({ "list": [] }))),
            Vec::<Out>::new()
        );
        assert_eq!(sends(&words, &obj(json!({}))), Vec::<Out>::new());
    }

    #[test]
    fn a_flows_branch_and_each_nexts_are_each_sourced_at_the_event() {
        let acts = actions(&[("a", json!({}))]);
        let steps = [
            step(json!({ "do": "a", "next": { "branch": { "on": "v",
                "cases": [{ "when": "crash", "next": { "ty": 31 } }], "else": { "ty": 33 } } } })),
            step(
                json!({ "do": "a", "next": { "ty": 41, "each": "list", "shape": { "w": "$item" } } }),
            ),
        ];
        let source = Uuid::now_v7();

        let (sent, result): (Vec<Event<u16, Value>>, _) = outcome(
            &steps,
            &acts,
            source,
            10,
            &json!({ "v": "crash", "list": ["a", "b"] }).to_string(),
            Utc::now(),
        );

        assert_eq!(result, Ok(()));
        let sent: Vec<_> = sent
            .iter()
            .map(|e| (e.ty, e.status, e.source, e.data.clone()))
            .collect();
        assert_eq!(
            sent,
            [
                (31, Status::Added, Some(source), json!({})),
                (41, Status::Added, Some(source), json!({ "w": "a" })),
                (41, Status::Added, Some(source), json!({ "w": "b" })),
                (10, Status::Processed, Some(source), json!("done")),
            ]
        );
    }
}
