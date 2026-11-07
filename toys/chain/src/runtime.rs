//! The router, a supervisor per handler, and one scheduler for every event in event time.
//!
//! Each event is checked at the entrance: a CloudEvent, of a domain its source belongs to, whose
//! data passes its type's schema, and no later than the declared lateness. One that fails goes
//! to the bin with its reason. Events wait in one queue ordered by event time, then id: events
//! from outside, events handlers emit, and timers, which are events scheduled for a later time.
//! An event is released only once nothing earlier can still arrive in time: its time is under
//! the highest outside time seen, less the lateness. So every handler sees every event in event
//! time, whatever order it arrived in, provided it arrived within the lateness.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::entrance::{Event, admit, to_json};
use crate::schema::Schemas;

/// One handler of routed events.
pub trait Handler {
    /// # Errors
    /// The event could not be handled: the supervisor treats it as a crash.
    fn handle(&mut self, event: &Event, out: &mut Vec<Event>) -> Result<(), String>;

    /// Makes what it has done durable; returns the last release position it holds for good.
    ///
    /// # Errors
    /// What it has done cannot be made durable.
    fn checkpoint(&mut self) -> Result<Option<u64>, String>;

    /// Its durable place as it started, by release position within this process.
    fn place(&self) -> Option<u64>;

    /// Events it has scheduled and still waits on: its timers, so a restart can queue them again.
    fn pending(&self) -> Vec<Event> {
        Vec::new()
    }

    /// Its state as text, when it has one to compare.
    fn snapshot(&self) -> Option<String> {
        None
    }
}

/// Starts a handler from its durable state.
pub type Start = Box<dyn FnMut() -> Result<Box<dyn Handler>, String>>;

/// A table from type pattern to handlers: a pattern is a type, or a prefix ending in `*`.
#[derive(Debug, Default)]
pub struct Router {
    exact: HashMap<String, Vec<usize>>,
    prefixes: Vec<(String, usize)>,
    resolved: HashMap<String, Vec<usize>>,
}

impl Router {
    pub fn register(&mut self, pattern: &str, handler: usize) {
        if let Some(prefix) = pattern.strip_suffix('*') {
            self.prefixes.push((prefix.to_string(), handler));
        } else {
            self.exact.entry(pattern.to_string()).or_default().push(handler);
        }
        self.resolved.clear();
    }

    pub fn route(&mut self, ty: &str) -> &[usize] {
        if !self.resolved.contains_key(ty) {
            let mut handlers: Vec<usize> = self.exact.get(ty).cloned().unwrap_or_default();
            for (prefix, handler) in &self.prefixes {
                if ty.starts_with(prefix.as_str()) && !handlers.contains(handler) {
                    handlers.push(*handler);
                }
            }
            handlers.sort_unstable();
            handlers.dedup();
            self.resolved.insert(ty.to_string(), handlers);
        }
        self.resolved.get(ty).map_or(&[], Vec::as_slice)
    }
}

/// One handler's supervisor, one for one: a crash or a stall restarts this handler alone, from
/// its durable state, and hands it again what it was handed since.
pub struct Supervisor {
    pub name: String,
    start: Start,
    handler: Box<dyn Handler>,
    place: Option<u64>,
    pending: VecDeque<Rc<Event>>,
    crashes: HashMap<String, u32>,
    binned: HashSet<String>,
    limit: u32,
    pub bin: Vec<(Rc<Event>, String)>,
    pub restarts: u64,
    pub handed: u64,
    pub reasons: Vec<String>,
}

impl std::fmt::Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Supervisor {
    /// # Errors
    /// The handler cannot start.
    pub fn new(name: &str, mut start: Start, limit: u32) -> Result<Supervisor, String> {
        let handler = start()?;
        let place = handler.place();
        Ok(Supervisor {
            name: name.to_string(),
            start,
            handler,
            place,
            pending: VecDeque::new(),
            crashes: HashMap::new(),
            binned: HashSet::new(),
            limit,
            bin: Vec::new(),
            restarts: 0,
            handed: 0,
            reasons: Vec::new(),
        })
    }

    /// # Errors
    /// The handler cannot be restarted.
    pub fn deliver(&mut self, event: Rc<Event>, out: &mut Vec<Event>) -> Result<(), String> {
        self.pending.push_back(event);
        let mut index = self.pending.len() - 1;
        while index < self.pending.len() {
            let event = Rc::clone(&self.pending[index]);
            self.handed += 1;
            let handler = &mut self.handler;
            let mut emitted = Vec::new();
            let outcome = catch_unwind(AssertUnwindSafe(|| handler.handle(&event, &mut emitted)));
            let reason = match outcome {
                Ok(Ok(())) => {
                    out.extend(emitted);
                    index += 1;
                    continue;
                }
                Ok(Err(reason)) => reason,
                Err(panic) => panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
                    .unwrap_or_else(|| "panicked".into()),
            };
            self.reasons.push(reason.clone());
            let crashes = self.crashes.entry(event.id.clone()).or_insert(0);
            *crashes += 1;
            if *crashes >= self.limit {
                self.binned.insert(event.id.clone());
                self.bin.push((Rc::clone(&event), reason));
            }
            self.restart()?;
            index = 0;
        }
        Ok(())
    }

    fn restart(&mut self) -> Result<(), String> {
        self.restarts += 1;
        self.handler = (self.start)()?;
        self.place = self.handler.place();
        let place = self.place;
        let binned = &self.binned;
        self.pending
            .retain(|e| place.is_none_or(|p| e.position > p) && !binned.contains(&e.id));
        Ok(())
    }

    /// # Errors
    /// The checkpoint failed.
    pub fn checkpoint(&mut self) -> Result<(), String> {
        if let Some(place) = self.handler.checkpoint()? {
            self.place = Some(place);
        }
        let place = self.place;
        self.pending.retain(|e| place.is_none_or(|p| e.position > p));
        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> Option<String> {
        self.handler.snapshot()
    }

    #[must_use]
    pub fn pending_events(&self) -> Vec<Event> {
        self.handler.pending()
    }
}

/// Why an event went to the bin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    Malformed(String),
    Domain(String),
    Late(String),
}

impl Reason {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Reason::Malformed(_) => "malformed",
            Reason::Domain(_) => "another domain",
            Reason::Late(_) => "later than the declared lateness",
        }
    }
}

/// Whether an event's type and source are of the same known domain.
///
/// # Errors
/// They are not: the event belongs to another domain.
#[inline]
pub fn check_domain(ty: &str, source: &str) -> Result<(), String> {
    let of_type = ty.split_once('.').map_or(ty, |(d, _)| d);
    let of_source = source.split_once('/').map_or(source, |(d, _)| d);
    if of_type == of_source && (of_type == "westwind" || of_type == "oz") {
        Ok(())
    } else {
        Err(format!("type {ty} is {of_type}'s, source {source} is {of_source}'s"))
    }
}

/// The router, the supervised handlers and the event-time queue.
pub struct Runtime {
    pub router: Router,
    pub handlers: Vec<Supervisor>,
    schemas: Schemas,
    lateness: i64,
    queue: BTreeMap<(i64, String), Event>,
    released: HashSet<String>,
    released_at: BTreeSet<(i64, String)>,
    max_external: i64,
    ended: bool,
    seq: u64,
    /// What went to the bin at the entrance, with why: the first few of each kind in full.
    pub bin: Vec<(String, Option<String>, Reason)>,
    /// How many went to the bin, by kind.
    pub bin_counts: BTreeMap<&'static str, u64>,
    pub duplicates: u64,
    pub skipped: u64,
    pub admitted: u64,
    pub released_count: u64,
    /// Emitted events that are not valid CloudEvents of a type whose schema they pass.
    pub invalid: Vec<(String, String)>,
    pub max_lateness: i64,
    log: Option<(File, Vec<u8>)>,
    state: Option<PathBuf>,
    last: Option<(i64, String)>,
    durable_last: Option<(i64, String)>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime").field("handlers", &self.handlers).finish_non_exhaustive()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct State {
    last: Option<(i64, String)>,
    max_external: i64,
}

impl Runtime {
    /// A runtime whose log and state live in `dir`, if given. It resumes from the state there.
    ///
    /// # Errors
    /// The log or state cannot be opened.
    pub fn new(schemas: Schemas, lateness: i64, dir: Option<&Path>) -> Result<Runtime, String> {
        let mut runtime = Runtime {
            router: Router::default(),
            handlers: Vec::new(),
            schemas,
            lateness,
            queue: BTreeMap::new(),
            released: HashSet::new(),
            released_at: BTreeSet::new(),
            max_external: i64::MIN,
            ended: false,
            seq: 0,
            bin: Vec::new(),
            bin_counts: BTreeMap::new(),
            duplicates: 0,
            skipped: 0,
            admitted: 0,
            released_count: 0,
            invalid: Vec::new(),
            max_lateness: 0,
            log: None,
            state: None,
            last: None,
            durable_last: None,
        };
        if let Some(dir) = dir {
            std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            let state = dir.join("runtime.json");
            if let Ok(text) = std::fs::read_to_string(&state)
                && let Ok(s) = serde_json::from_str::<State>(&text)
            {
                runtime.last = s.last.clone();
                runtime.durable_last = s.last;
                runtime.max_external = s.max_external;
            }
            let file = OpenOptions::new().create(true).append(true).open(dir.join("log.jsonl")).map_err(|e| e.to_string())?;
            runtime.log = Some((file, Vec::new()));
            runtime.state = Some(state);
        }
        Ok(runtime)
    }

    /// Adds a supervised handler for the patterns it registers, and queues the timers it waits on.
    ///
    /// # Errors
    /// The handler cannot start.
    pub fn add(&mut self, name: &str, patterns: &[String], start: Start, limit: u32) -> Result<usize, String> {
        let index = self.handlers.len();
        let supervisor = Supervisor::new(name, start, limit)?;
        for event in supervisor.pending_events() {
            let key = (event.time, event.id.clone());
            if self.durable_last.as_ref().is_none_or(|last| key > *last) {
                self.queue.insert(key, event);
            }
        }
        self.handlers.push(supervisor);
        for pattern in patterns {
            self.router.register(pattern, index);
        }
        Ok(index)
    }

    fn binned(&mut self, line: &str, id: Option<String>, reason: Reason) {
        let count = self.bin_counts.entry(reason.kind()).or_insert(0);
        *count += 1;
        if *count <= 50 {
            self.bin.push((line.chars().take(160).collect(), id, reason));
        }
    }

    /// Takes one event from outside, as its JSON line, and releases what it can.
    ///
    /// # Errors
    /// A handler cannot be restarted.
    pub fn admit(&mut self, line: &str) -> Result<(), String> {
        self.admitted += 1;
        let event = match admit(line, 0) {
            Ok(event) => event,
            Err(e) => {
                self.binned(line, None, Reason::Malformed(e));
                return Ok(());
            }
        };
        if let Err(e) = check_domain(&event.ty, &event.source) {
            self.binned(line, Some(event.id), Reason::Domain(e));
            return Ok(());
        }
        if let Err(e) = self.schemas.check(&event.ty, &event.data) {
            self.binned(line, Some(event.id), Reason::Malformed(e));
            return Ok(());
        }
        let key = (event.time, event.id.clone());
        if self.released.contains(&event.id) || self.queue.contains_key(&key) {
            self.duplicates += 1;
            return Ok(());
        }
        if self.durable_last.as_ref().is_some_and(|last| key <= *last) {
            self.skipped += 1;
            return Ok(());
        }
        if self.max_external != i64::MIN && event.time < self.max_external - self.lateness {
            let late = self.max_external - event.time;
            self.binned(line, Some(event.id), Reason::Late(format!("{} ms behind the latest event, past {} ms", late, self.lateness)));
            return Ok(());
        }
        if self.max_external != i64::MIN {
            self.max_lateness = self.max_lateness.max(self.max_external - event.time);
        }
        self.max_external = self.max_external.max(event.time);
        self.queue.insert(key, event);
        self.release()
    }

    /// Releases everything nothing earlier can still arrive in time for.
    ///
    /// # Errors
    /// A handler cannot be restarted.
    pub fn release(&mut self) -> Result<(), String> {
        loop {
            let watermark = if self.ended { self.max_external.saturating_add(1) } else { self.max_external - self.lateness };
            let Some(entry) = self.queue.first_entry() else { break };
            if entry.key().0 >= watermark {
                break;
            }
            let event = entry.remove();
            self.dispatch(event, watermark)?;
        }
        Ok(())
    }

    fn dispatch(&mut self, mut event: Event, watermark: i64) -> Result<(), String> {
        self.seq += 1;
        self.released_count += 1;
        event.position = self.seq;
        self.released.insert(event.id.clone());
        self.released_at.insert((event.time, event.id.clone()));
        let horizon = watermark.saturating_sub(2 * self.lateness);
        while let Some(first) = self.released_at.first().cloned() {
            if first.0 >= horizon {
                break;
            }
            self.released_at.pop_first();
            self.released.remove(&first.1);
        }
        let line = to_json(&event);
        if let Some((_, buffer)) = &mut self.log {
            buffer.extend_from_slice(line.as_bytes());
            buffer.push(b'\n');
        }
        self.last = Some((event.time, event.id.clone()));
        let event = Rc::new(event);
        let mut out = Vec::new();
        let routes: Vec<usize> = self.router.route(&event.ty).to_vec();
        for handler in routes {
            self.handlers[handler].deliver(Rc::clone(&event), &mut out)?;
        }
        for emitted in out {
            // Every emitted event goes back through the entrance's own reading, so it is checked
            // as a CloudEvent, as of its domain and against its type's schema.
            let check = admit(&to_json(&emitted), 0)
                .and_then(|e| check_domain(&e.ty, &e.source).map(|()| e))
                .and_then(|e| self.schemas.check(&e.ty, &e.data));
            if let Err(e) = check {
                self.invalid.push((emitted.ty.clone(), e));
            }
            let key = (emitted.time, emitted.id.clone());
            if self.released.contains(&emitted.id) || self.queue.contains_key(&key) {
                continue;
            }
            self.queue.insert(key, emitted);
        }
        Ok(())
    }

    /// The stream has ended: release everything up to the last outside time.
    ///
    /// # Errors
    /// A handler cannot be restarted.
    pub fn finish(&mut self) -> Result<(), String> {
        self.ended = true;
        self.release()?;
        self.ended = false;
        Ok(())
    }

    /// Checkpoints every handler in order, then the log, then the runtime's own place.
    ///
    /// # Errors
    /// A checkpoint failed.
    pub fn checkpoint(&mut self) -> Result<(), String> {
        for handler in &mut self.handlers {
            handler.checkpoint()?;
        }
        if let Some((file, buffer)) = &mut self.log {
            file.write_all(buffer).map_err(|e| e.to_string())?;
            buffer.clear();
        }
        if let Some(path) = &self.state {
            let state = State { last: self.last.clone(), max_external: self.max_external };
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, serde_json::to_string(&state).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
            std::fs::rename(&tmp, path).map_err(|e| e.to_string())?;
        }
        self.durable_last = self.last.clone();
        Ok(())
    }

    #[must_use]
    pub fn queued(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_domain_is_its_types_and_its_sources() {
        assert!(check_domain("westwind.order.placed", "westwind/web").is_ok());
        assert!(check_domain("oz.order.placed", "oz/emerald-city").is_ok());
        assert!(check_domain("westwind.order.placed", "oz/emerald-city").is_err());
        assert!(check_domain("oz.order.placed", "westwind/web").is_err());
        assert!(check_domain("mordor.ring.forged", "mordor/doom").is_err());
    }
}
