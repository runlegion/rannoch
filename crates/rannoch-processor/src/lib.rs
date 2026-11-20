mod check;
mod doc;

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{TimeDelta, Utc};
use rannoch_core::{Event, Sender, Status};
use rannoch_queue::{Flow, Flows, Row};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use check::{check, compile};
pub use doc::{Branch, Case, Flow as FlowDoc, Next, Shape, Step, To, eval, type_expr, type_of};

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
    /// `flows` is the JSON array of flow docs the app compiles in.
    /// # Errors
    /// The error of a flow doc that does not parse.
    pub fn new(flows: &str, actions: Vec<Arc<dyn Action>>) -> Result<Self, serde_json::Error> {
        let mut by_name = HashMap::new();
        for action in actions {
            by_name.insert(action.sig().action.clone(), action);
        }
        let mut by_ty: HashMap<u16, Arc<Flow<T, D>>> = HashMap::new();
        for doc in serde_json::from_str::<Vec<FlowDoc>>(flows)? {
            let (actions, ty, job) = (by_name.clone(), doc.ty, doc.job);
            let process = move |event: &Row, root: &Row, sender: &Sender<T, D>| {
                let ty = |ty| T::try_from(ty).map_err(|_| format!("ty {ty} is not the app's"));
                let sends = serde_json::from_str(&root.data)
                    .map_err(|e| format!("root data: {e}"))
                    .and_then(|data| run(&doc, &actions, data))
                    .and_then(|nexts| {
                        let done =
                            ty(event.ty).map(|t| (t, "done".into(), None, Status::Processed));
                        let nexts = nexts.into_iter().map(|(to, data, delay)| {
                            let at = TimeDelta::from_std(Duration::from_millis(delay)).ok();
                            let at = at.and_then(|d| Utc::now().checked_add_signed(d));
                            Ok((ty(to)?, Value::Object(data), at, Status::Added))
                        });
                        nexts.chain([done]).collect::<Result<Vec<_>, String>>()
                    });
                let (sends, result) = match sends {
                    Ok(sends) => (sends, Ok(())),
                    Err(failure) => {
                        let binned = (ty(event.ty)?, failure.as_str().into(), None, Status::Binned);
                        (vec![binned], Err(failure))
                    }
                };
                for (ty, data, run_at, status) in sends {
                    let (id, source, data) = (uuid::Uuid::nil(), Some(event.id), D::from(data));
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

/// Runs `flow` on the root's data: per step, each link of the chain in order, its options
/// from `with`, its input the options so far, its output checked against its sig's gives and
/// merged in (later wins), the step's result put under `as` when set; then the step's next is
/// shaped with `eval`. Returns the nexts as (ty, data, `delay_ms`), or the first failure.
#[expect(clippy::type_complexity, reason = "the issue's signature")]
fn run(
    flow: &FlowDoc,
    actions: &HashMap<String, Arc<dyn Action>>,
    data: Map<String, Value>,
) -> Result<Vec<(u16, Map<String, Value>, u64)>, String> {
    let (mut vals, mut nexts) = (data, Vec::new());
    for step in &flow.steps {
        let (mut input, mut result) = (vals.clone(), Map::new());
        for link in step.chain.split('.') {
            let action = actions.get(link).ok_or(format!("no action named {link}"))?;
            let options = step
                .with
                .get(link)
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use serde_json::{Map, Value, json};

    use super::{Action, FlowDoc, Sig};

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
}
