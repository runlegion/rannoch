//! The flow runner. A flow is a file of JSON lines: a header naming the flow, its domain, its
//! correlation and how long a history it keeps, then rows of `on`, guard, step, and `next` or
//! `else`. A row with `on` starts when an event of that type arrives; no flow names the stage
//! after it, only the types it emits.
//!
//! Each instance keeps its own history file. An entry records the event handed, each step's
//! answer and the variables it set, and every event the steps sent: emitted events and timers in
//! full. An instance rebuilds by running its rows again over its history, each step answered from
//! the record, and refuses a history whose rows send anything else. Once a history holds `carry`
//! entries the instance carries its state into a fresh history, so a long-lived instance rebuilds
//! in the same time at any age. An event handed again sends again what its entry records.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::entrance::{Event, mint};
use crate::host::Pool;
use crate::runtime::Handler;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    flow: String,
    domain: String,
    /// For each type, a JSON pointer into its data to the instance's key, or `@id` for the
    /// event's own id.
    correlation: BTreeMap<String, String>,
    #[serde(default)]
    carry: Option<usize>,
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

/// A flow, read and checked.
#[derive(Debug, Clone)]
pub struct Flow {
    pub name: String,
    pub domain: String,
    correlation: BTreeMap<String, String>,
    pub carry: Option<usize>,
    rows: Vec<Row>,
    index: HashMap<String, usize>,
}

impl Flow {
    /// # Errors
    /// A row names a row that is not there, a step lacks what it needs, or a row emits a type of
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
            let needs: &[(&str, bool)] = match row.step.as_deref() {
                None => &[],
                Some("emit") => &[("type", row.ty.is_some())],
                Some("emit_each") => &[("type", row.ty.is_some()), ("each", row.each.is_some())],
                Some("host") => &[("action", row.action.is_some())],
                Some("after") => &[("type", row.ty.is_some()), ("delay", row.delay_ms.is_some() || row.delay_from.is_some())],
                Some(other) => return Err(format!("row {}: no step {other}", row.id)),
            };
            for (field, present) in needs {
                if !present {
                    return Err(format!("row {}: its step needs {field}", row.id));
                }
            }
            if let Some(ty) = &row.ty
                && !ty.starts_with(&format!("{}.", header.domain))
            {
                return Err(format!("row {} sends {ty}, which is not of domain {}", row.id, header.domain));
            }
            if let Some(on) = &row.on
                && !header.correlation.contains_key(on)
            {
                return Err(format!("row {} starts on {on}, which the correlation does not key", row.id));
            }
        }
        Ok(Flow { name: header.flow, domain: header.domain, correlation: header.correlation, carry: header.carry, rows, index })
    }

    #[must_use]
    pub fn patterns(&self) -> Vec<String> {
        let mut types: Vec<String> = self.rows.iter().filter_map(|r| r.on.clone()).collect();
        types.sort();
        types.dedup();
        types
    }

    #[must_use]
    pub fn key(&self, event: &Event) -> String {
        match self.correlation.get(&event.ty).map(String::as_str) {
            Some("@id") => event.id.clone(),
            Some(pointer) => event
                .data
                .pointer(pointer)
                .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
                .unwrap_or_default(),
            None => String::new(),
        }
    }
}

/// One step that ran: its row, its answer and the variables it set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Step {
    pub row: String,
    pub answer: Value,
    pub set: Map<String, Value>,
}

/// One event handed to an instance, and everything its rows did and sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub event: Event,
    pub steps: Vec<Step>,
    pub emitted: Vec<Event>,
    pub timers: Vec<Event>,
}

/// What an instance's history says.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Instance {
    pub vars: Map<String, Value>,
    pub types: BTreeSet<String>,
    /// Events handed within the lateness window, each with what it sent.
    pub handed: BTreeMap<String, (i64, Vec<Event>)>,
    pub timers: BTreeMap<String, Event>,
    pub clock: i64,
    /// Entries since the last carry.
    pub entries: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Line {
    Carry(Instance),
    Entry(Entry),
}

/// A flow, run against routed events.
pub struct Runner {
    pub flow: Flow,
    pub instances: BTreeMap<String, Instance>,
    dir: Option<PathBuf>,
    window: i64,
    host: Pool,
    /// Entries not yet durable, by instance, and instances to carry at the next checkpoint.
    unwritten: BTreeMap<String, Vec<Entry>>,
    carry_due: BTreeSet<String>,
    last: Option<u64>,
    durable: Option<u64>,
    pub carries: u64,
}

impl std::fmt::Debug for Runner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runner").field("flow", &self.flow.name).finish_non_exhaustive()
    }
}

fn file_name(key: &str) -> String {
    if key.is_empty() { "_".into() } else { key.replace(['/', '\\', '.'], "_") }
}

fn apply_entry(instance: &mut Instance, entry: &Entry, window: i64) {
    for step in &entry.steps {
        for (k, v) in &step.set {
            instance.vars.insert(k.clone(), v.clone());
        }
    }
    instance.types.insert(entry.event.ty.clone());
    instance.timers.remove(&entry.event.id);
    for timer in &entry.timers {
        instance.timers.insert(timer.id.clone(), timer.clone());
    }
    let mut sent = entry.emitted.clone();
    sent.extend(entry.timers.iter().cloned());
    instance.handed.insert(entry.event.id.clone(), (entry.event.time, sent));
    instance.clock = instance.clock.max(entry.event.time);
    let horizon = instance.clock.saturating_sub(window);
    instance.handed.retain(|_, (t, _)| *t >= horizon);
    // A timer due before the instance's clock has been released, whichever flow it went to: one
    // that went to another flow never comes back to say so, so it is forgotten by its time.
    instance.timers.retain(|_, t| t.time >= horizon);
    instance.entries += 1;
}

/// A guard holds when each part it names holds: `unseen` and `seen` list types the instance has
/// or has not been handed; `data`, `vars` and `answer` match the input field by field.
fn holds(guard: &Value, input: &Value, instance: &Instance) -> bool {
    let Value::Object(parts) = guard else { return false };
    parts.iter().all(|(name, want)| match name.as_str() {
        "unseen" => want.as_array().is_some_and(|l| l.iter().filter_map(Value::as_str).all(|t| !instance.types.contains(t))),
        "seen" => want.as_array().is_some_and(|l| l.iter().filter_map(Value::as_str).all(|t| instance.types.contains(t))),
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
    ["order_id", "ticket", "subscription_id", "product_id"]
        .iter()
        .find_map(|k| data.get(*k).and_then(Value::as_str))
        .unwrap_or(key)
        .to_string()
}

impl Runner {
    /// Starts a runner, rebuilding every instance from its history file in `dir`.
    ///
    /// # Errors
    /// A history cannot be read, or replaying it sends anything other than it records.
    pub fn start(flow: Flow, dir: Option<&Path>, window: i64) -> Result<Runner, String> {
        let mut runner = Runner {
            flow,
            instances: BTreeMap::new(),
            dir: None,
            window,
            host: Pool::default(),
            unwritten: BTreeMap::new(),
            carry_due: BTreeSet::new(),
            last: None,
            durable: None,
            carries: 0,
        };
        if let Some(dir) = dir {
            let dir = dir.join(&runner.flow.name);
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
                .map_err(|e| e.to_string())?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
                .collect();
            files.sort();
            for path in files {
                let (key, instance) = runner.rebuild_file(&path)?;
                runner.instances.insert(key, instance);
            }
            runner.dir = Some(dir);
        }
        Ok(runner)
    }

    /// Rebuilds one instance from its history file: from its last carry, then each entry run
    /// again with its steps answered from the record.
    ///
    /// # Errors
    /// The file cannot be read, or an entry replays differently.
    pub fn rebuild_file(&mut self, path: &Path) -> Result<(String, Instance), String> {
        let file = File::open(path).map_err(|e| e.to_string())?;
        let mut key = None;
        let mut instance = Instance::default();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|e| e.to_string())?;
            let Ok(parsed) = serde_json::from_str::<(String, Line)>(&line) else { break };
            key = Some(parsed.0.clone());
            match parsed.1 {
                Line::Carry(state) => instance = state,
                Line::Entry(entry) => {
                    let (steps, emitted, timers) = self.run(&parsed.0, &instance, &entry.event, Some(&entry.steps))?;
                    if steps != entry.steps || emitted != entry.emitted || timers != entry.timers {
                        return Err(format!("{}: replaying {} sends other than its history records", self.flow.name, entry.event.id));
                    }
                    apply_entry(&mut instance, &entry, self.window);
                }
            }
        }
        Ok((key.unwrap_or_default(), instance))
    }

    /// The history file of instance `key`.
    #[must_use]
    pub fn path_of(&self, key: &str) -> Option<PathBuf> {
        self.dir.as_ref().map(|d| d.join(format!("{}.jsonl", file_name(key))))
    }

    /// Runs the rows for one event over `instance` as it stands: live, or with each step's
    /// answer taken from `record`. Returns the steps, the events emitted and the timers set.
    #[allow(clippy::type_complexity)]
    fn run(&mut self, key: &str, instance: &Instance, event: &Event, record: Option<&[Step]>) -> Result<(Vec<Step>, Vec<Event>, Vec<Event>), String> {
        let mut steps: Vec<Step> = Vec::new();
        let mut emitted = Vec::new();
        let mut timers = Vec::new();
        let mut vars = instance.vars.clone();
        let starts: Vec<usize> = (0..self.flow.rows.len()).filter(|&i| self.flow.rows[i].on.as_deref() == Some(event.ty.as_str())).collect();
        for start in starts {
            let mut answer = Value::Null;
            let mut at = Some(start);
            while let Some(index) = at {
                let row = self.flow.rows[index].clone();
                let input = json!({"type": event.ty, "subject": event.subject, "time": event.time, "id": event.id,
                                   "data": event.data, "vars": Value::Object(vars.clone()), "answer": answer});
                if let Some(guard) = &row.guard
                    && !holds(guard, &input, instance)
                {
                    at = row.otherwise.as_ref().map(|r| self.flow.index[r]);
                    continue;
                }
                let mut sent = |ty: &str, time: i64, data: Value, n: usize| -> Event {
                    let seed = format!("{}\n{key}\n{}\n{}\n{n}\n{time}", self.flow.name, row.id, event.id);
                    Event {
                        id: mint(time, seed.as_bytes()),
                        ty: ty.to_string(),
                        source: format!("{}/flow/{}", self.flow.domain, self.flow.name),
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
                        let (got, set) = match record {
                            Some(r) => {
                                let s = r.get(steps.len()).filter(|s| s.row == row.id).ok_or_else(|| format!("history has no step {} here", row.id))?;
                                (s.answer.clone(), s.set.clone())
                            }
                            None => self.host.run(row.action.as_deref().unwrap_or_default(), &input)?,
                        };
                        for (k, v) in &set {
                            vars.insert(k.clone(), v.clone());
                        }
                        answer = got.clone();
                        steps.push(Step { row: row.id.clone(), answer: got, set });
                    }
                    Some("emit") => emitted.push(sent(row.ty.as_deref().unwrap_or_default(), event.time, pointed(&row.data), 0)),
                    Some("emit_each") => {
                        let items = pointed(&row.each).as_array().cloned().unwrap_or_default();
                        for (n, item) in items.into_iter().enumerate() {
                            emitted.push(sent(row.ty.as_deref().unwrap_or_default(), event.time, item, n));
                        }
                    }
                    Some("after") => {
                        let delay = row.delay_ms.or_else(|| row.delay_from.as_deref().and_then(|p| input.pointer(p)).and_then(Value::as_i64)).unwrap_or(0);
                        timers.push(sent(row.ty.as_deref().unwrap_or_default(), event.time + delay, pointed(&row.data), 0));
                    }
                    _ => {}
                }
                at = row.next.as_ref().map(|r| self.flow.index[r]);
            }
        }
        Ok((steps, emitted, timers))
    }

    /// Rebuild time of instance `key` from its file, in microseconds, and its entries.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn time_rebuild(&mut self, key: &str) -> Result<(f64, usize), String> {
        let path = self.path_of(key).ok_or("no history dir")?;
        let lines = std::fs::read_to_string(&path).map_err(|e| e.to_string())?.lines().count();
        let started = std::time::Instant::now();
        let (_, instance) = self.rebuild_file(&path)?;
        let us = started.elapsed().as_secs_f64() * 1e6;
        if self.instances.get(key) != Some(&instance) {
            return Err(format!("{key}: the rebuilt instance differs from the live one"));
        }
        Ok((us, lines))
    }

    #[must_use]
    pub fn state(&self) -> String {
        let mut text = String::new();
        for (key, i) in &self.instances {
            let handed: Vec<&String> = i.handed.keys().collect();
            text.push_str(&format!("{key} vars {:?} types {:?} handed {handed:?} timers {:?} clock {}\n", i.vars, i.types, i.timers.keys().collect::<Vec<_>>(), i.clock));
        }
        text
    }
}

impl Handler for Runner {
    fn handle(&mut self, event: &Event, out: &mut Vec<Event>) -> Result<(), String> {
        let key = self.flow.key(event);
        let instance = self.instances.get(&key).cloned().unwrap_or_default();
        if let Some((_, sent)) = instance.handed.get(&event.id) {
            out.extend(sent.iter().cloned());
            return Ok(());
        }
        let (steps, emitted, timers) = self.run(&key, &instance, event, None)?;
        let entry = Entry { event: event.clone(), steps, emitted, timers };
        let mut instance = instance;
        apply_entry(&mut instance, &entry, self.window);
        out.extend(entry.emitted.iter().cloned());
        out.extend(entry.timers.iter().cloned());
        if self.flow.carry.is_some_and(|n| instance.entries >= n) {
            self.carry_due.insert(key.clone());
        }
        self.unwritten.entry(key.clone()).or_default().push(entry);
        self.instances.insert(key, instance);
        self.last = Some(event.position);
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<Option<u64>, String> {
        if self.dir.is_some() {
            for (key, entries) in std::mem::take(&mut self.unwritten) {
                let path = self.path_of(&key).ok_or("no dir")?;
                if self.carry_due.remove(&key) {
                    let instance = self.instances.get_mut(&key).ok_or("no instance")?;
                    instance.entries = 0;
                    let line = serde_json::to_string(&(&key, Line::Carry(instance.clone()))).map_err(|e| e.to_string())?;
                    let tmp = path.with_extension("tmp");
                    std::fs::write(&tmp, format!("{line}\n")).map_err(|e| e.to_string())?;
                    std::fs::rename(&tmp, &path).map_err(|e| e.to_string())?;
                    self.carries += 1;
                    continue;
                }
                let mut text = String::new();
                for entry in entries {
                    text.push_str(&serde_json::to_string(&(&key, Line::Entry(entry))).map_err(|e| e.to_string())?);
                    text.push('\n');
                }
                let mut file = OpenOptions::new().create(true).append(true).open(&path).map_err(|e| e.to_string())?;
                file.write_all(text.as_bytes()).map_err(|e| e.to_string())?;
            }
        } else {
            self.unwritten.clear();
            self.carry_due.clear();
        }
        self.durable = self.last;
        Ok(self.durable)
    }

    fn place(&self) -> Option<u64> {
        self.durable
    }

    fn pending(&self) -> Vec<Event> {
        self.instances.values().flat_map(|i| i.timers.values().cloned()).collect()
    }

    fn snapshot(&self) -> Option<String> {
        Some(self.state())
    }
}
