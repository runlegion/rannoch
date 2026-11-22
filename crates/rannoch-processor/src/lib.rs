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

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sig {
    pub action: String,
    #[serde(default)]
    pub options: Shape,
    #[serde(default)]
    pub takes: Shape,
    #[serde(default)]
    pub gives: Shape,
    #[serde(default)]
    pub job: bool,
}

pub trait Action: Send + Sync {
    fn sig(&self) -> &Sig;
    /// # Errors
    /// The failure, which fails the step.
    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String>;
}

/// The app's flows by ty, each a `rannoch_queue::Flow` whose run closure calls `process`.
#[derive(Debug)]
pub struct Processor<T, D> {
    flows: HashMap<u16, Arc<Flow<T, D>>>,
}

impl<T: TryFrom<u16> + PartialEq + 'static, D: From<Value> + 'static> Processor<T, D> {
    /// `flows` is the JSON array of flow docs the app compiles in; `events` reads `_events`.
    /// # Errors
    /// The error of a flow doc that does not parse.
    pub fn new(
        flows: &str,
        actions: Vec<Arc<dyn Action>>,
        events: Connection,
    ) -> Result<Self, serde_json::Error> {
        let mut by_name = HashMap::new();
        for action in actions {
            by_name.insert(action.sig().action.clone(), action);
        }
        let docs: Arc<Vec<FlowDoc>> = Arc::new(serde_json::from_str(flows)?);
        let (conn, seen) = (Arc::new(Mutex::new(events)), Arc::new(Mutex::default()));
        let mut by_ty: HashMap<u16, Arc<Flow<T, D>>> = HashMap::new();
        for doc in docs.iter().cloned() {
            let (actions, ty, job) = (by_name.clone(), doc.ty, doc.job);
            let (docs, conn, seen) = (docs.clone(), conn.clone(), seen.clone());
            let process = move |event: &Row, root: &Row, sender: &Sender<T, D>| {
                let (id, at) = (event.id, (event.id, event.ty));
                let ty = |ty| T::try_from(ty).map_err(|_| format!("ty {ty} is not the app's"));
                let sends = serde_json::from_str(&root.data)
                    .map_err(|e| format!("root data: {e}"))
                    .and_then(|data: Map<String, Value>| {
                        let (mut outs, said) = waits(&lock(&conn), &lock(&seen), &docs, at, &data)?;
                        if said == "done" {
                            let nexts = run(&doc, &actions, data)?.into_iter();
                            outs.extend(nexts.map(|(to, data, delay)| (id, to, data, delay)));
                        }
                        lock(&seen).extend(outs.iter().map(|o| o.0).filter(|s| *s != id));
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
                    Ok(sends) => (sends, Ok(())),
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
        Ok(Self { flows: by_ty })
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

/// What `id` of `ty` sends about waits as (source, ty, data, delay), and its 2's data.
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
            let zeros = rows.iter().filter(|r| r.0 == 0).count();
            let two = rows.iter().find(|r| r.0 == 2).map(|r| r.1.as_str());
            let to = match (due.is_some(), answered.contains(&wait_id), zeros, two) {
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
#[expect(clippy::type_complexity, reason = "the issue's signature")]
fn run(
    flow: &FlowDoc,
    actions: &HashMap<String, Arc<dyn Action>>,
    data: Map<String, Value>,
) -> Result<Vec<(u16, Map<String, Value>, u64)>, String> {
    let (mut vals, mut nexts) = (data, Vec::new());
    for (i, step) in flow.steps.iter().enumerate() {
        let (mut input, mut result) = (vals.clone(), Map::new());
        for link in step.chain.split('.') {
            let action = actions.get(link).ok_or(format!("no action named {link}"))?;
            let options = step.with.get(link).cloned().unwrap_or(json!({}));
            let out = action.run(&options, &input)?;
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

    /// Runs the flow doc `flow` on `data` over the actions `links` (name, gives, act).
    fn run(flow: &Value, data: &Value, links: &[(&str, &[&str], Act)]) -> (Ran, Calls) {
        let calls = Arc::default();
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
                Arc::new(Link(sig, *act, Arc::clone(&calls))),
            );
        }
        let flow: FlowDoc = serde_json::from_value(flow.clone()).unwrap();
        let ran = super::run(&flow, &actions, obj(data));
        let calls = calls.lock().unwrap().clone();
        (ran, calls)
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

    /// What rannoch hands over, as (ty, source), kept off `_events`.
    #[derive(Debug, Default)]
    struct Kept(Mutex<Vec<(u16, Option<Uuid>)>>);
    impl Append<u16, Value> for Kept {
        fn append(&self, batch: &[Event<u16, Value>]) -> Result<(), rannoch_core::Error> {
            let mut kept = self.0.lock().unwrap();
            kept.extend(batch.iter().map(|e| (e.ty, e.source)));
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
        sent.into_iter().filter(|s| s.0 != 61).collect()
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
