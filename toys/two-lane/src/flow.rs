//! A flow: a file of JSON lines, its header naming the flow, its domain, the row it lives in, how
//! its events are ordered, and the correlation that picks the row; then rows of `on`, guard,
//! step, and `next` or `else`. A flow keeps no state of its own: its state is a part of the row
//! it is keyed to, read with the row and written with it.
//!
//! Ordering: `clock` (an event waits until nothing earlier can still arrive within the lateness),
//! `dependency` (an event waits until the types it needs exist on its row), or `none`. A
//! dependency flow may name types it takes in clock order too.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::entrance::{Event, mint};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    flow: String,
    domain: String,
    correlation: BTreeMap<String, String>,
    row: String,
    ordering: String,
    #[serde(default)]
    needs: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    clock: Vec<String>,
}

/// One row of a flow.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    pub id: String,
    pub on: Option<String>,
    pub guard: Option<Value>,
    pub step: Option<String>,
    pub action: Option<String>,
    #[serde(rename = "type")]
    pub ty: Option<String>,
    pub delay_ms: Option<i64>,
    pub delay_from: Option<String>,
    pub data: Option<String>,
    pub each: Option<String>,
    pub next: Option<String>,
    #[serde(rename = "else")]
    pub otherwise: Option<String>,
}

/// How a flow's events are ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    Clock,
    Dependency,
    None,
}

/// A flow, read and checked.
#[derive(Debug, Clone)]
pub struct Flow {
    pub name: String,
    pub domain: String,
    pub row_kind: String,
    pub ordering: Ordering,
    pub needs: BTreeMap<String, Vec<String>>,
    pub clock_types: BTreeSet<String>,
    correlation: BTreeMap<String, String>,
    rows: Vec<Row>,
    index: HashMap<String, usize>,
}

impl Flow {
    /// # Errors
    /// A row names a row that is not there, a step lacks what it needs, or a row sends a type of
    /// another domain.
    pub fn parse(text: &str) -> Result<Flow, String> {
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());
        let header: Header = serde_json::from_str(lines.next().ok_or("empty flow")?).map_err(|e| format!("header: {e}"))?;
        let rows: Vec<Row> = lines.map(|l| serde_json::from_str(l).map_err(|e| format!("{l}: {e}"))).collect::<Result<_, _>>()?;
        let index: HashMap<String, usize> = rows.iter().enumerate().map(|(i, r)| (r.id.clone(), i)).collect();
        for row in &rows {
            for target in [&row.next, &row.otherwise].into_iter().flatten() {
                if !index.contains_key(target) {
                    return Err(format!("row {} names row {target}, which is not there", row.id));
                }
            }
            if let Some(ty) = &row.ty
                && !ty.starts_with(&format!("{}.", header.domain))
            {
                return Err(format!("row {} sends {ty}, not of domain {}", row.id, header.domain));
            }
        }
        let ordering = match header.ordering.as_str() {
            "clock" => Ordering::Clock,
            "dependency" => Ordering::Dependency,
            "none" => Ordering::None,
            other => return Err(format!("no ordering {other}")),
        };
        Ok(Flow {
            name: header.flow,
            domain: header.domain,
            row_kind: header.row,
            ordering,
            needs: header.needs,
            clock_types: header.clock.into_iter().collect(),
            correlation: header.correlation,
            rows,
            index,
        })
    }

    #[must_use]
    pub fn patterns(&self) -> Vec<String> {
        let mut types: Vec<String> = self.rows.iter().filter_map(|r| r.on.clone()).collect();
        types.sort();
        types.dedup();
        types
    }

    /// The types this flow's `after` steps send: timers, delivered at their event time.
    #[must_use]
    pub fn timer_types(&self) -> Vec<String> {
        self.rows.iter().filter(|r| r.step.as_deref() == Some("after")).filter_map(|r| r.ty.clone()).collect()
    }

    /// Whether an event of `ty` waits for the clock in this flow.
    #[must_use]
    pub fn clocked(&self, ty: &str) -> bool {
        self.ordering == Ordering::Clock || self.clock_types.contains(ty)
    }

    #[must_use]
    pub fn key(&self, event: &Event) -> String {
        match self.correlation.get(&event.ty).map(String::as_str) {
            Some("@id") => event.id.clone(),
            Some(pointer) => event.data.pointer(pointer).map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string)).unwrap_or_default(),
            None => String::new(),
        }
    }
}

/// A flow's state, as it lives inside its row.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct State {
    pub vars: Map<String, Value>,
    pub types: BTreeSet<String>,
    /// Events handed within the lateness window, each with what it sent.
    pub handed: BTreeMap<String, (i64, Vec<Event>)>,
    pub clock: i64,
}

/// A host step's answer: what it said and the variables it set.
pub type Answer = (Value, Map<String, Value>);

/// What one event did in a flow.
#[derive(Debug, Default)]
pub struct Done {
    pub sent: Vec<Event>,
    /// Host answers asked for now, by action and event, to keep.
    pub asked: Vec<((String, String), Answer)>,
    pub again: bool,
}

fn holds(guard: &Value, input: &Value, state: &State) -> bool {
    let Value::Object(parts) = guard else { return false };
    parts.iter().all(|(name, want)| match name.as_str() {
        "unseen" => want.as_array().is_some_and(|l| l.iter().filter_map(Value::as_str).all(|t| !state.types.contains(t))),
        "seen" => want.as_array().is_some_and(|l| l.iter().filter_map(Value::as_str).all(|t| state.types.contains(t))),
        _ => input.get(name).is_some_and(|got| matches(want, got)),
    })
}

fn matches(want: &Value, got: &Value) -> bool {
    match (want, got) {
        (Value::Object(want), Value::Object(got)) => want.iter().all(|(k, w)| got.get(k).is_some_and(|g| matches(w, g))),
        _ => want == got,
    }
}

fn subject_of(data: &Value, key: &str) -> String {
    ["order_id", "ticket", "subscription_id", "product_id"].iter().find_map(|k| data.get(*k).and_then(Value::as_str)).unwrap_or(key).to_string()
}

impl Flow {
    /// Runs the rows for `event` over `state`, in place. A host step's answer comes from
    /// `remembered` when the host has given it before, else from `ask`. An event handed again
    /// sends again what it sent.
    ///
    /// # Errors
    /// A host step fails or stalls; nothing is changed then.
    pub fn handle(
        &self,
        state: &mut State,
        key: &str,
        event: &Event,
        window: i64,
        remembered: &dyn Fn(&str, &str) -> Option<Answer>,
        ask: &mut dyn FnMut(&str, &Value) -> Result<Answer, String>,
    ) -> Result<Done, String> {
        if let Some((_, sent)) = state.handed.get(&event.id) {
            return Ok(Done { sent: sent.clone(), asked: Vec::new(), again: true });
        }
        let mut done = Done::default();
        let mut vars = state.vars.clone();
        let starts: Vec<usize> = (0..self.rows.len()).filter(|&i| self.rows[i].on.as_deref() == Some(event.ty.as_str())).collect();
        for start in starts {
            let mut answer = Value::Null;
            let mut at = Some(start);
            while let Some(index) = at {
                let row = &self.rows[index];
                let input = json!({"type": event.ty, "subject": event.subject, "time": event.time, "id": event.id,
                                   "data": event.data, "vars": Value::Object(vars.clone()), "answer": answer});
                if let Some(guard) = &row.guard
                    && !holds(guard, &input, state)
                {
                    at = row.otherwise.as_ref().map(|r| self.index[r]);
                    continue;
                }
                let sent = |ty: &str, time: i64, data: Value, n: usize| -> Event {
                    // Its id is a hash of its content and its cause's key: never of which worker
                    // or shard answered first, nor of which event happened to come last.
                    let _ = n;
                    let seed = format!("{ty}\n{time}\n{data}\n{}\n{key}", self.domain);
                    Event {
                        id: mint(time, seed.as_bytes()),
                        ty: ty.to_string(),
                        source: format!("{}/flow/{}", self.domain, self.name),
                        subject: Some(subject_of(&data, key)),
                        time,
                        data,
                        causation: Some(event.id.clone()),
                        position: 0,
                    }
                };
                let pointed = |p: &Option<String>| input.pointer(p.as_deref().unwrap_or("/data")).cloned().unwrap_or(Value::Null);
                match row.step.as_deref() {
                    Some("host") => {
                        let action = row.action.as_deref().unwrap_or_default();
                        let (got, set) = match remembered(action, &event.id) {
                            Some(a) => a,
                            None => {
                                let a = ask(action, &input)?;
                                done.asked.push(((action.to_string(), event.id.clone()), a.clone()));
                                a
                            }
                        };
                        for (k, v) in &set {
                            vars.insert(k.clone(), v.clone());
                        }
                        answer = got;
                    }
                    Some("emit") => done.sent.push(sent(row.ty.as_deref().unwrap_or_default(), event.time, pointed(&row.data), 0)),
                    Some("emit_each") => {
                        for (n, item) in pointed(&row.each).as_array().cloned().unwrap_or_default().into_iter().enumerate() {
                            done.sent.push(sent(row.ty.as_deref().unwrap_or_default(), event.time, item, n));
                        }
                    }
                    Some("after") => {
                        let delay = row.delay_ms.or_else(|| row.delay_from.as_deref().and_then(|p| input.pointer(p)).and_then(Value::as_i64)).unwrap_or(0);
                        done.sent.push(sent(row.ty.as_deref().unwrap_or_default(), event.time + delay, pointed(&row.data), 0));
                    }
                    _ => {}
                }
                at = row.next.as_ref().map(|r| self.index[r]);
            }
        }
        state.vars = vars;
        state.types.insert(event.ty.clone());
        state.handed.insert(event.id.clone(), (event.time, done.sent.clone()));
        state.clock = state.clock.max(event.time);
        let horizon = state.clock.saturating_sub(window);
        state.handed.retain(|_, (t, _)| *t >= horizon);
        Ok(done)
    }
}
