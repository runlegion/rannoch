//! The flow runner, written fresh from the design. A flow is a file of JSON lines: a header that
//! names the flow and its correlation, then rows of guard, step, and `next` or `else`. A row with
//! `on` starts when an event of that type arrives. Each step's result is recorded in the
//! instance's history, so an instance rebuilds by replaying its history: the same rows run again,
//! each step answered from the record instead of done again.
//!
//! Steps: `emit` (send an event), `host` (call an action outside the store), `after` (a timer on
//! event time that sends an event when it fires), `store` (write a node).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::entrance::{Event, mint};
use crate::runtime::Handler;
use crate::store::{Writer, node_body};

#[derive(Debug, Clone, Deserialize)]
struct Header {
    flow: String,
    /// For each type, a JSON pointer into its data to the instance's key; a type not named here
    /// goes to the one instance keyed "".
    #[serde(default)]
    correlation: BTreeMap<String, String>,
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
    pub class: Option<String>,
    pub properties: Option<BTreeMap<String, String>>,
    pub next: Option<String>,
    #[serde(rename = "else")]
    pub otherwise: Option<String>,
}

/// A flow, read and checked.
#[derive(Debug, Clone)]
pub struct Flow {
    pub name: String,
    correlation: BTreeMap<String, String>,
    rows: Vec<Row>,
    index: HashMap<String, usize>,
}

impl Flow {
    /// Reads a flow from its JSON lines and checks every row before anything runs.
    ///
    /// # Errors
    /// A line is not a row, a row names a row that is not there, or a step lacks what it needs.
    pub fn parse(text: &str) -> Result<Flow, String> {
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());
        let header: Header = serde_json::from_str(lines.next().ok_or("empty flow")?).map_err(|e| format!("header: {e}"))?;
        let rows: Vec<Row> = lines
            .map(|l| serde_json::from_str(l).map_err(|e| format!("{l}: {e}")))
            .collect::<Result<_, _>>()?;
        let index: HashMap<String, usize> = rows.iter().enumerate().map(|(i, r)| (r.id.clone(), i)).collect();
        for row in &rows {
            for target in [&row.next, &row.otherwise].into_iter().flatten() {
                if !index.contains_key(target) {
                    return Err(format!("row {} names row {target}, which is not there", row.id));
                }
            }
            let needs: &[(&str, bool)] = match row.step.as_deref() {
                None => &[],
                Some("emit") => &[("type", row.ty.is_some())],
                Some("host") => &[("action", row.action.is_some())],
                Some("after") => &[("type", row.ty.is_some()), ("delay_ms", row.delay_ms.is_some())],
                Some("store") => &[("class", row.class.is_some())],
                Some(other) => return Err(format!("row {}: no step {other}", row.id)),
            };
            for (field, present) in needs {
                if !present {
                    return Err(format!("row {}: its step needs {field}", row.id));
                }
            }
        }
        Ok(Flow { name: header.flow, correlation: header.correlation, rows, index })
    }

    /// The types it starts on, for the router.
    #[must_use]
    pub fn patterns(&self) -> Vec<String> {
        let mut types: Vec<String> = self.rows.iter().filter_map(|r| r.on.clone()).collect();
        types.sort();
        types.dedup();
        types
    }

    fn key(&self, event: &Event) -> String {
        self.correlation
            .get(&event.ty)
            .and_then(|pointer| event.data.pointer(pointer))
            .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
            .unwrap_or_default()
    }
}

/// An action outside the store, which remembers each answer it gave, so asking again about the
/// same event gets the same answer without doing the work twice.
pub trait Host {
    /// # Errors
    /// The action is not registered, or its input is not what it takes.
    fn call(&mut self, action: &str, input: &Value, event: &str) -> Result<Value, String>;
}

/// Westwind's warehouse: each product's stock, starting from its row, and its reorder level.
#[derive(Debug, Default)]
pub struct Warehouse {
    stock: HashMap<String, (i64, i64)>,
    remembered: HashMap<(String, String), Value>,
    pub calls: u64,
}

impl Warehouse {
    /// Stocks the warehouse from each product's starting stock; its reorder level is
    /// `percent` of that.
    #[must_use]
    pub fn new(products: &[(String, i64)], percent: i64) -> Warehouse {
        Warehouse {
            stock: products.iter().map(|(id, stock)| (id.clone(), (*stock, stock * percent / 100))).collect(),
            ..Warehouse::default()
        }
    }
}

impl Host for Warehouse {
    fn call(&mut self, action: &str, input: &Value, event: &str) -> Result<Value, String> {
        let at = (action.to_string(), event.to_string());
        if let Some(answer) = self.remembered.get(&at) {
            return Ok(answer.clone());
        }
        self.calls += 1;
        let answer = match action {
            "take_stock" => {
                let product = input.get("product_id").and_then(Value::as_str).ok_or("take_stock needs product_id")?;
                let quantity = input.get("quantity").and_then(Value::as_i64).ok_or("take_stock needs quantity")?;
                let (stock, level) = self.stock.get_mut(product).ok_or_else(|| format!("no product {product}"))?;
                let before = *stock;
                *stock -= quantity;
                json!({"line": input.get("id"), "product_id": product, "before": before, "after": *stock, "level": *level,
                       "crossed": before >= *level && *stock < *level})
            }
            other => return Err(format!("no action {other}")),
        };
        self.remembered.insert(at, answer.clone());
        Ok(answer)
    }
}

/// One step that ran, and what it answered.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Step {
    pub row: String,
    pub result: Value,
}

/// One event an instance was handed, or one of its timers that fired, with what its rows did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub position: u64,
    pub event: String,
    pub ty: String,
    pub time: i64,
    /// The row and due time of the timer this entry fired; `None` for a handed event.
    pub fired: Option<(String, i64)>,
    pub steps: Vec<Step>,
}

/// A flow instance: its history, and what that history says.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Instance {
    pub history: Vec<Entry>,
    pub handed: BTreeSet<String>,
    pub types: BTreeSet<String>,
    /// Its timers not yet fired, by due time and row, each with the data it sends.
    pub timers: BTreeMap<(i64, String), Value>,
}

/// What the runner does with a step: do it, or answer it from the record.
enum Mode<'a> {
    Live,
    Replay(&'a [Step]),
}

/// A flow, run against routed events.
pub struct Runner {
    pub flow: Flow,
    pub instances: BTreeMap<String, Instance>,
    /// Every unfired timer, in the order they fire: due time, instance, row.
    timers: BTreeSet<(i64, String, String)>,
    pub clock: i64,
    host: Option<Rc<RefCell<dyn Host>>>,
    pub writer: Option<Writer>,
    /// The history file, and the entries since the last checkpoint. They reach the file only at
    /// a checkpoint, after what their steps wrote to the store is committed: a history that got
    /// ahead of the store would say a step was done that a crash undid.
    history: Option<(File, Vec<u8>)>,
    last: Option<u64>,
    durable: Option<u64>,
    /// Every timer that fired, in the order it fired.
    pub fired: Vec<(i64, String, String)>,
}

impl std::fmt::Debug for Runner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runner").field("flow", &self.flow.name).finish_non_exhaustive()
    }
}

impl Runner {
    /// What its instances, its fired timers and its clock say, as text to compare.
    #[must_use]
    pub fn state(&self) -> String {
        format!("{:?}\n{:?}\n{}", self.instances, self.fired, self.clock)
    }
}

#[derive(Serialize, Deserialize)]
struct Line {
    key: String,
    entry: Entry,
}

impl Runner {
    /// Starts a runner, rebuilding every instance from the history file at `history` when one is
    /// given and exists.
    ///
    /// # Errors
    /// The history cannot be read, or replaying it does not give back the steps it records.
    pub fn start(
        flow: Flow,
        host: Option<Rc<RefCell<dyn Host>>>,
        writer: Option<Writer>,
        history: Option<&Path>,
    ) -> Result<Runner, String> {
        let mut runner = Runner {
            flow,
            instances: BTreeMap::new(),
            timers: BTreeSet::new(),
            clock: i64::MIN,
            host,
            writer,
            history: None,
            last: None,
            durable: None,
            fired: Vec::new(),
        };
        if let Some(path) = history {
            let lines = read_history(path)?;
            for line in &lines {
                runner.replay(&line.key, &line.entry)?;
            }
            runner.durable = lines.iter().map(|l| l.entry.position).max();
            runner.last = runner.durable;
            let file = OpenOptions::new().create(true).append(true).open(path).map_err(|e| e.to_string())?;
            runner.history = Some((file, Vec::new()));
        }
        Ok(runner)
    }

    /// Rebuilds one entry of instance `key` by running its rows again, each step answered from
    /// the record.
    ///
    /// # Errors
    /// The rows the replay runs are not the rows the entry records.
    pub fn replay(&mut self, key: &str, entry: &Entry) -> Result<(), String> {
        if let Some((row, due)) = &entry.fired {
            self.timers.remove(&(*due, key.to_string(), row.clone()));
            let instance = self.instances.entry(key.to_string()).or_default();
            instance.timers.remove(&(*due, row.clone()));
            instance.history.push(entry.clone());
            self.fired.push((*due, key.to_string(), row.clone()));
            return Ok(());
        }
        self.clock = self.clock.max(entry.time);
        let input = json!({"type": entry.ty, "time": entry.time, "id": entry.event});
        let mut instance = self.instances.remove(key).unwrap_or_default();
        let ran = self.run(&mut instance, key, entry, &input, &mut Vec::new(), &Mode::Replay(&entry.steps));
        let steps = ran?;
        if steps != entry.steps {
            self.instances.insert(key.to_string(), instance);
            return Err(format!("replaying {} ran other steps than its history records", entry.event));
        }
        record(&mut instance, entry.clone());
        self.instances.insert(key.to_string(), instance);
        Ok(())
    }

    fn write(&mut self, key: &str, entry: &Entry) -> Result<(), String> {
        if let Some((_, pending)) = &mut self.history {
            let line = serde_json::to_string(&Line { key: key.to_string(), entry: entry.clone() }).map_err(|e| e.to_string())?;
            writeln!(pending, "{line}").map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Fires every timer due at or before the clock, in order.
    fn fire(&mut self, position: u64, out: &mut Vec<Event>) -> Result<(), String> {
        while let Some(first) = self.timers.first().cloned() {
            if first.0 > self.clock {
                break;
            }
            self.timers.pop_first();
            let (due, key, row) = first;
            let instance = self.instances.entry(key.clone()).or_default();
            let data = instance.timers.remove(&(due, row.clone())).unwrap_or(Value::Null);
            let ty = self.flow.rows[self.flow.index[&row]].ty.clone().unwrap_or_default();
            let id = mint(due, format!("{}\n{key}\n{row}\n{due}", self.flow.name).as_bytes());
            out.push(Event {
                id: id.clone(),
                ty: ty.clone(),
                source: format!("flow/{}", self.flow.name),
                subject: Some(key.clone()),
                time: due,
                data,
                position,
            });
            let entry = Entry { position, event: id, ty, time: due, fired: Some((row.clone(), due)), steps: Vec::new() };
            instance.history.push(entry.clone());
            self.fired.push((due, key.clone(), row));
            self.write(&key, &entry)?;
        }
        Ok(())
    }

    /// Runs the rows `on` the entry's type for one instance; returns the steps that ran.
    fn run(
        &mut self,
        instance: &mut Instance,
        key: &str,
        entry: &Entry,
        input: &Value,
        out: &mut Vec<Event>,
        mode: &Mode<'_>,
    ) -> Result<Vec<Step>, String> {
        let mut steps = Vec::new();
        let starts: Vec<usize> = (0..self.flow.rows.len())
            .filter(|&i| self.flow.rows[i].on.as_deref() == Some(entry.ty.as_str()))
            .collect();
        for start in starts {
            let mut input = input.clone();
            let mut at = Some(start);
            while let Some(index) = at {
                let row = self.flow.rows[index].clone();
                if let Some(guard) = &row.guard
                    && !holds(guard, &input, instance)
                {
                    at = row.otherwise.as_ref().map(|r| self.flow.index[r]);
                    continue;
                }
                if let Some(step) = &row.step {
                    let recorded = match mode {
                        Mode::Live => None,
                        Mode::Replay(record) => Some(
                            record
                                .get(steps.len())
                                .filter(|s| s.row == row.id)
                                .ok_or_else(|| format!("history has no step {} here", row.id))?
                                .result
                                .clone(),
                        ),
                    };
                    let (result, next_input) = self.step(step, &row, instance, key, entry, &input, recorded, out)?;
                    steps.push(Step { row: row.id.clone(), result });
                    input = next_input;
                }
                at = row.next.as_ref().map(|r| self.flow.index[r]);
            }
        }
        Ok(steps)
    }

    #[allow(clippy::too_many_arguments)]
    fn step(
        &mut self,
        step: &str,
        row: &Row,
        instance: &mut Instance,
        key: &str,
        entry: &Entry,
        input: &Value,
        recorded: Option<Value>,
        out: &mut Vec<Event>,
    ) -> Result<(Value, Value), String> {
        let data = input.get("data").cloned().unwrap_or(Value::Null);
        let live = recorded.is_none();
        match step {
            "host" => {
                let answer = match recorded {
                    Some(answer) => answer,
                    None => {
                        let host = self.host.as_ref().ok_or("no host")?;
                        let action = row.action.as_deref().unwrap_or_default();
                        host.borrow_mut().call(action, &data, &entry.event)?
                    }
                };
                let next = json!({"type": format!("{}.{}.answered", self.flow.name, row.id), "data": answer});
                Ok((answer, next))
            }
            "emit" => {
                let ty = row.ty.clone().unwrap_or_default();
                let id = mint(entry.time, format!("{}\n{key}\n{}\n{}", self.flow.name, row.id, entry.event).as_bytes());
                if live {
                    out.push(Event {
                        id: id.clone(),
                        ty: ty.clone(),
                        source: format!("flow/{}", self.flow.name),
                        subject: Some(key.to_string()),
                        time: entry.time,
                        data: data.clone(),
                        position: entry.position,
                    });
                }
                let result = recorded.unwrap_or_else(|| json!({"emitted": id, "type": ty}));
                Ok((result, json!({"type": ty, "id": id, "data": data})))
            }
            "after" => {
                let due = entry.time + row.delay_ms.unwrap_or_default();
                // The data the timer sends is part of its result, so a rebuilt timer sends it too.
                let result = recorded.unwrap_or_else(|| json!({"due": due, "data": data}));
                let sends = result.get("data").cloned().unwrap_or(Value::Null);
                instance.timers.insert((due, row.id.clone()), sends);
                self.timers.insert((due, key.to_string(), row.id.clone()));
                Ok((result.clone(), json!({"type": format!("{}.{}.set", self.flow.name, row.id), "data": result})))
            }
            "store" => {
                let class = row.class.clone().unwrap_or_default();
                if live {
                    let mut properties = Map::new();
                    for (name, pointer) in row.properties.iter().flatten() {
                        if let Some(value) = data.pointer(pointer) {
                            properties.insert(name.clone(), value.clone());
                        }
                    }
                    let id = mint(entry.time, format!("{}\n{key}\n{}\n{}", self.flow.name, row.id, entry.event).as_bytes());
                    let body = node_body(&id, 0, &format!("flow/{}", self.flow.name), &class, key, &properties);
                    self.writer.as_mut().ok_or("no store")?.add(vec![body], entry.position);
                }
                let result = recorded.unwrap_or_else(|| json!({"stored": class, "key": key}));
                Ok((result.clone(), json!({"type": format!("{}.{}.stored", self.flow.name, row.id), "data": result})))
            }
            other => Err(format!("no step {other}")),
        }
    }
}

fn record(instance: &mut Instance, entry: Entry) {
    instance.handed.insert(entry.event.clone());
    instance.types.insert(entry.ty.clone());
    instance.history.push(entry);
}

/// A guard holds when every field it names matches the input (objects match field by field),
/// and when the instance has had no event of the type `unseen` names.
fn holds(guard: &Value, input: &Value, instance: &Instance) -> bool {
    let Value::Object(fields) = guard else {
        return guard == input;
    };
    fields.iter().all(|(name, want)| {
        if name == "unseen" {
            return want.as_str().is_none_or(|ty| !instance.types.contains(ty));
        }
        input.get(name).is_some_and(|got| matches(want, got))
    })
}

fn matches(want: &Value, got: &Value) -> bool {
    match (want, got) {
        (Value::Object(want), Value::Object(got)) => {
            want.iter().all(|(k, w)| got.get(k).is_some_and(|g| matches(w, g)))
        }
        _ => want == got,
    }
}

fn read_history(path: &Path) -> Result<Vec<Line>, String> {
    let Ok(file) = File::open(path) else {
        return Ok(Vec::new());
    };
    let mut lines = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|e| e.to_string())?;
        // A line a crash cut short was never part of a checkpoint; what follows it cannot be.
        match serde_json::from_str(&line) {
            Ok(line) => lines.push(line),
            Err(_) => break,
        }
    }
    Ok(lines)
}

impl Handler for Runner {
    fn handle(&mut self, event: &Event, out: &mut Vec<Event>) -> Result<(), String> {
        if event.time > self.clock {
            self.clock = event.time;
            self.fire(event.position, out)?;
        }
        let key = self.flow.key(event);
        let mut instance = self.instances.remove(&key).unwrap_or_default();
        if instance.handed.contains(&event.id) {
            self.instances.insert(key, instance);
            return Ok(());
        }
        let mut entry = Entry {
            position: event.position,
            event: event.id.clone(),
            ty: event.ty.clone(),
            time: event.time,
            fired: None,
            steps: Vec::new(),
        };
        let input = json!({"type": event.ty, "subject": event.subject, "time": event.time, "id": event.id, "data": event.data});
        let ran = self.run(&mut instance, &key, &entry, &input, out, &Mode::Live);
        match ran {
            Ok(steps) => {
                entry.steps = steps;
                self.write(&key, &entry)?;
                record(&mut instance, entry);
                self.instances.insert(key, instance);
                self.last = Some(event.position);
                Ok(())
            }
            Err(e) => {
                self.instances.insert(key, instance);
                Err(e)
            }
        }
    }

    fn checkpoint(&mut self) -> Result<Option<u64>, String> {
        if let Some(writer) = &mut self.writer {
            writer.checkpoint()?;
        }
        if let Some((file, pending)) = &mut self.history {
            file.write_all(pending).map_err(|e| e.to_string())?;
            pending.clear();
        }
        self.durable = self.last;
        Ok(self.durable)
    }

    fn place(&self) -> Option<u64> {
        self.durable
    }

    fn snapshot(&self) -> Option<String> {
        Some(self.state())
    }
}

/// What the flows sent, each event once, in the order each first arrived.
#[derive(Debug, Default)]
pub struct Collected {
    pub order: Vec<Event>,
    pub ids: BTreeSet<String>,
    pub handed: u64,
}

/// Collects what the flows emit, for checking against SQL.
#[derive(Debug)]
pub struct Collector(pub Rc<RefCell<Collected>>);

impl Handler for Collector {
    fn handle(&mut self, event: &Event, _: &mut Vec<Event>) -> Result<(), String> {
        let mut collected = self.0.borrow_mut();
        collected.handed += 1;
        if collected.ids.insert(event.id.clone()) {
            collected.order.push(event.clone());
        }
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<Option<u64>, String> {
        Ok(None)
    }
    fn place(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STOCK: &str = include_str!("../flows/stock.jsonl");
    const UNSHIPPED: &str = include_str!("../flows/unshipped.jsonl");

    fn line(position: u64, product: &str, quantity: i64) -> Event {
        Event {
            id: format!("l{position}"),
            ty: "westwind.order_line.added".into(),
            source: "t".into(),
            subject: None,
            time: i64::try_from(position).unwrap() * 1000,
            data: json!({"product_id": product, "quantity": quantity}),
            position,
        }
    }

    fn warehouse() -> Rc<RefCell<dyn Host>> {
        Rc::new(RefCell::new(Warehouse::new(&[("p".into(), 100)], 20)))
    }

    #[test]
    fn both_flows_read_and_a_bad_row_is_refused() {
        assert_eq!(Flow::parse(STOCK).unwrap().patterns(), vec!["westwind.order_line.added"]);
        assert_eq!(Flow::parse(UNSHIPPED).unwrap().patterns().len(), 3);
        let bad = "{\"flow\":\"x\"}\n{\"id\":\"a\",\"on\":\"t\",\"step\":\"emit\",\"next\":\"nowhere\",\"type\":\"u\"}";
        assert!(Flow::parse(bad).unwrap_err().contains("nowhere"));
    }

    #[test]
    fn stock_low_is_sent_once_when_a_line_takes_stock_under_its_level() {
        let mut runner = Runner::start(Flow::parse(STOCK).unwrap(), Some(warehouse()), None, None).unwrap();
        let mut out = Vec::new();
        for (position, quantity) in [(1, 50), (2, 25), (3, 10), (4, 10)] {
            runner.handle(&line(position, "p", quantity), &mut out).unwrap();
        }
        // 100 -> 50 -> 25 -> 15 (under 20: sent) -> 5 (already under: not sent).
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ty, "westwind.stock.low");
        assert_eq!((out[0].data["before"].clone(), out[0].data["after"].clone()), (json!(25), json!(15)));
        assert_eq!(out[0].position, 3);
    }

    #[test]
    fn an_event_handed_again_changes_nothing() {
        let mut runner = Runner::start(Flow::parse(STOCK).unwrap(), Some(warehouse()), None, None).unwrap();
        let mut out = Vec::new();
        runner.handle(&line(1, "p", 90), &mut out).unwrap();
        let before = runner.instances.clone();
        runner.handle(&line(1, "p", 90), &mut out).unwrap();
        assert_eq!(runner.instances, before);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn an_instance_rebuilt_from_its_history_equals_one_that_never_stopped() {
        let dir = std::env::temp_dir().join(format!("workers-toy-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stock.jsonl");
        let host = warehouse();
        let mut runner = Runner::start(Flow::parse(STOCK).unwrap(), Some(Rc::clone(&host)), None, Some(&path)).unwrap();
        let mut out = Vec::new();
        for position in 1..=6 {
            runner.handle(&line(position, "p", 15), &mut out).unwrap();
        }
        runner.checkpoint().unwrap();
        let rebuilt = Runner::start(Flow::parse(STOCK).unwrap(), Some(host), None, Some(&path)).unwrap();
        assert_eq!(rebuilt.instances, runner.instances);
        assert_eq!(rebuilt.place(), Some(6));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_rebuilt_timer_sends_what_the_live_one_would() {
        let dir = std::env::temp_dir().join(format!("workers-toy-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("unshipped.jsonl");
        let mut runner = Runner::start(Flow::parse(UNSHIPPED).unwrap(), None, None, Some(&path)).unwrap();
        let placed = Event {
            id: "p1".into(),
            ty: "westwind.order.placed".into(),
            source: "t".into(),
            subject: Some("a".into()),
            time: 0,
            data: json!({"id": "a", "freight": 19.9}),
            position: 1,
        };
        runner.handle(&placed, &mut Vec::new()).unwrap();
        runner.checkpoint().unwrap();
        let rebuilt = Runner::start(Flow::parse(UNSHIPPED).unwrap(), None, None, Some(&path)).unwrap();
        assert_eq!(rebuilt.instances, runner.instances);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_timer_fires_on_event_time_unless_the_order_shipped() {
        let dir = std::env::temp_dir().join(format!("workers-toy-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let writer = Writer::open(&dir.join("s.rannoch"), "flow/unshipped").unwrap();
        let mut runner = Runner::start(Flow::parse(UNSHIPPED).unwrap(), None, Some(writer), None).unwrap();
        let day = 86_400_000;
        let order = |id: &str, ty: &str, time: i64, position: u64| Event {
            id: format!("{ty}{id}"),
            ty: ty.into(),
            source: "t".into(),
            subject: Some(id.into()),
            time,
            data: json!({"id": id}),
            position,
        };
        let mut out = Vec::new();
        runner.handle(&order("a", "westwind.order.placed", 0, 1), &mut out).unwrap();
        runner.handle(&order("b", "westwind.order.placed", 0, 2), &mut out).unwrap();
        runner.handle(&order("b", "westwind.order.shipped", 5 * day, 3), &mut out).unwrap();
        assert!(out.is_empty());
        runner.handle(&order("c", "westwind.order.placed", 31 * day, 4), &mut out).unwrap();
        // Both timers came due at day 30; they fire when event time passes it, never by the wall.
        assert_eq!(out.len(), 2);
        let due: Vec<Event> = std::mem::take(&mut out);
        for event in &due {
            runner.handle(event, &mut out).unwrap();
        }
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ty, "westwind.order.unshipped");
        assert_eq!(out[0].subject.as_deref(), Some("a"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
