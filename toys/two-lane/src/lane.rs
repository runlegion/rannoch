//! The two lanes. The fast lane: a durable inbox of CloudEvents, read in rounds; each event's
//! fact written as its row; then the flows registered for its type, run in the row they are keyed
//! to, by workers that each own a shard of the rows. What a flow sends goes back on the inbox as
//! new CloudEvents; a timer is one such event, delivered at its event time. The only thing this
//! code knows of ordering is how to hold an event until its flow's condition is met.
//!
//! The slow lane: every event is appended to a log stream beside the fast lane, never read on it.
//! A trickle reader makes hearings from it afterwards.
//!
//! Each round is one durable step: what the round's flows sent is appended to the inbox and
//! synced, then every shard commits its rows, and the lane's place goes in with the first shard's
//! commit. A round never spans more than the lateness of event time, so whatever a restart hands
//! again is still inside the windows that know it was done.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};

use serde::{Deserialize, Serialize};

use crate::entrance::{Event, admit, to_json};
use crate::flow::Flow;
use crate::host::Pool;
use crate::schema::Schemas;
use crate::store::{Shard, fact_of};

/// What a unit of work is for: an event's own fact, or one flow's handling of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Target {
    Fact,
    Flow(usize),
}

/// One event's work on one row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unit {
    pub event: Event,
    pub target: Target,
    pub kind: String,
    pub key: String,
}

#[derive(Debug, Default)]
struct Outcome {
    sent: Vec<Event>,
    waiting: Vec<Unit>,
    retry: Vec<Unit>,
    stalls: Vec<String>,
}

enum Job {
    Units(Vec<Unit>),
    Commit(Option<String>),
    Text,
    Reads,
    Stop,
}

enum Reply {
    Done(Outcome),
    Committed(Result<(), String>),
    Text(Result<String, String>),
    Reads(u64, u64, u64),
}

fn worker(path: PathBuf, flows: Arc<Vec<Flow>>, window: i64, jobs: Receiver<Job>, replies: Sender<Reply>) {
    let mut shard = match Shard::open(&path) {
        Ok(s) => s,
        Err(e) => {
            let _ = replies.send(Reply::Committed(Err(e)));
            return;
        }
    };
    let mut pool = Pool::default();
    while let Ok(job) = jobs.recv() {
        let reply = match job {
            Job::Units(units) => Reply::Done(work(&mut shard, &flows, &mut pool, window, units)),
            Job::Commit(meta) => Reply::Committed(shard.commit(meta.as_deref())),
            Job::Text => Reply::Text(shard.text()),
            Job::Reads => Reply::Reads(shard.found, shard.absent, shard.from_memory),
            Job::Stop => break,
        };
        if replies.send(reply).is_err() {
            break;
        }
    }
}

fn work(shard: &mut Shard, flows: &[Flow], pool: &mut Pool, window: i64, units: Vec<Unit>) -> Outcome {
    let mut out = Outcome::default();
    for unit in units {
        let event = &unit.event;
        match unit.target {
            Target::Fact => {
                let Some(fact) = fact_of(event) else { continue };
                let Ok(row) = shard.row(&unit.kind, &unit.key) else { continue };
                if row.facts.contains_key(&event.id) {
                    continue;
                }
                if fact.defines {
                    row.data = event.data.clone();
                }
                row.types.insert(event.ty.clone(), event.time);
                row.facts.insert(event.id.clone(), event.time);
                row.event = event.id.clone();
                row.time = row.time.max(event.time);
                let horizon = row.time.saturating_sub(window);
                row.facts.retain(|_, t| *t >= horizon);
                shard.touched(&unit.kind, &unit.key);
                for (rel, kind, to) in fact.edges {
                    shard.edge((fact.kind, &fact.key), rel, (kind, &to), &event.id);
                }
            }
            Target::Flow(f) => {
                let flow = &flows[f];
                let needs_met = {
                    let Ok(row) = shard.row(&unit.kind, &unit.key) else { continue };
                    flow.needs.get(&event.ty).is_none_or(|needs| needs.iter().all(|t| row.types.contains_key(t)))
                };
                if !needs_met {
                    out.waiting.push(unit);
                    continue;
                }
                let mut state = shard.row(&unit.kind, &unit.key).map(|r| r.flows.get(&flow.name).cloned().unwrap_or_default()).unwrap_or_default();
                let remembered = |action: &str, id: &str| shard.answer(action, id);
                let result = flow.handle(&mut state, &unit.key, event, window, &remembered, &mut |a, i| pool.run(a, i));
                match result {
                    Ok(done) => {
                        for (at, answer) in done.asked {
                            shard.keep_answer(at, answer);
                        }
                        if !done.again {
                            if let Ok(row) = shard.row(&unit.kind, &unit.key) {
                                row.flows.insert(flow.name.clone(), state);
                                row.event = event.id.clone();
                                row.time = row.time.max(event.time);
                            }
                            shard.touched(&unit.kind, &unit.key);
                        }
                        out.sent.extend(done.sent);
                    }
                    Err(e) => {
                        // A stall or a crash: the step is handed again next round, as a
                        // supervisor restarts its handler; its host thread is replaced.
                        out.stalls.push(e);
                        *pool = Pool::default();
                        out.retry.push(unit);
                    }
                }
            }
        }
    }
    out
}

/// What the lane must remember across a restart.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Place {
    offset: u64,
    max_external: Option<i64>,
    held: Vec<Unit>,
}

/// The fast lane and its log.
pub struct Lane {
    flows: Arc<Vec<Flow>>,
    routes: HashMap<String, Vec<usize>>,
    timers: std::collections::HashSet<String>,
    schemas: Schemas,
    lateness: i64,
    inbox: BufWriter<File>,
    inbox_file: File,
    reader: BufReader<File>,
    offset: u64,
    log: Option<BufWriter<File>>,
    workers: Vec<(Sender<Job>, Receiver<Reply>, std::thread::JoinHandle<()>)>,
    clocked: BTreeMap<(i64, String, Target), Unit>,
    waiting: Vec<Unit>,
    max_external: Option<i64>,
    pub ended: bool,
    pub bins: BTreeMap<&'static str, u64>,
    pub binned: Vec<(String, String)>,
    pub invalid: Vec<(String, String)>,
    pub read: u64,
    pub units: u64,
    pub rounds: u64,
    pub stalls: Vec<String>,
    pub max_lateness: i64,
}

fn shard_of(kind: &str, key: &str, n: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (kind, key).hash(&mut h);
    usize::try_from(h.finish() % n as u64).unwrap_or(0)
}

/// Lines a round reads at most: `TWO_LANE_BATCH`, else 4096.
fn round_lines() -> usize {
    std::env::var("TWO_LANE_BATCH").ok().and_then(|v| v.parse().ok()).unwrap_or(4096)
}

impl Lane {
    /// Opens the lane in `dir` with `workers` shards, resuming from what is durable there.
    ///
    /// # Errors
    /// A file cannot be opened.
    pub fn open(dir: &Path, flows: Vec<Flow>, schemas: Schemas, lateness: i64, workers: usize, log: bool) -> Result<Lane, String> {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
        let flows = Arc::new(flows);
        let mut routes: HashMap<String, Vec<usize>> = HashMap::new();
        let mut timers = std::collections::HashSet::new();
        for (i, f) in flows.iter().enumerate() {
            for p in f.patterns() {
                routes.entry(p).or_default().push(i);
            }
            timers.extend(f.timer_types());
        }
        let inbox_path = dir.join("inbox.jsonl");
        // A kill can leave a line half written at the end of the inbox or the log: cut it off.
        for path in [inbox_path.clone(), dir.join("log.jsonl")] {
            if let Ok(bytes) = std::fs::read(&path)
                && let Some(end) = bytes.iter().rposition(|b| *b == b'\n').map(|p| p + 1).or(Some(0))
                && end < bytes.len()
            {
                let f = OpenOptions::new().write(true).open(&path).map_err(|e| e.to_string())?;
                f.set_len(end as u64).map_err(|e| e.to_string())?;
            }
        }
        let inbox_file = OpenOptions::new().create(true).append(true).open(&inbox_path).map_err(|e| e.to_string())?;
        let inbox = BufWriter::with_capacity(1 << 20, inbox_file.try_clone().map_err(|e| e.to_string())?);
        let place: Place = Shard::open(&dir.join("shard-0.db"))?.meta().and_then(|m| serde_json::from_str(&m).ok()).unwrap_or_default();
        let mut started = Vec::new();
        for n in 0..workers {
            let (tx, rx) = channel();
            let (rtx, rrx) = channel();
            let path = dir.join(format!("shard-{n}.db"));
            let f = Arc::clone(&flows);
            let handle = std::thread::spawn(move || worker(path, f, 2 * lateness, rx, rtx));
            started.push((tx, rrx, handle));
        }
        let mut reader = BufReader::with_capacity(1 << 20, File::open(&inbox_path).map_err(|e| e.to_string())?);
        reader.seek(SeekFrom::Start(place.offset)).map_err(|e| e.to_string())?;
        let log = if log {
            Some(BufWriter::with_capacity(1 << 20, OpenOptions::new().create(true).append(true).open(dir.join("log.jsonl")).map_err(|e| e.to_string())?))
        } else {
            None
        };
        let mut lane = Lane {
            flows,
            routes,
            timers,
            schemas,
            lateness,
            inbox,
            inbox_file,
            reader,
            offset: place.offset,
            log,
            workers: started,
            clocked: BTreeMap::new(),
            waiting: Vec::new(),
            max_external: place.max_external,
            ended: false,
            bins: BTreeMap::new(),
            binned: Vec::new(),
            invalid: Vec::new(),
            read: 0,
            units: 0,
            rounds: 0,
            stalls: Vec::new(),
            max_lateness: 0,
        };
        for unit in place.held {
            lane.hold(unit);
        }
        Ok(lane)
    }

    fn hold(&mut self, unit: Unit) {
        let clocked = match unit.target {
            Target::Fact => false,
            Target::Flow(f) => self.flows[f].clocked(&unit.event.ty) || self.timers.contains(&unit.event.ty),
        };
        if clocked {
            self.clocked.insert((unit.event.time, unit.event.id.clone(), unit.target), unit);
        } else {
            self.waiting.push(unit);
        }
    }

    /// Takes events from outside onto the inbox, durably: one sync per batch.
    ///
    /// # Errors
    /// The inbox cannot be written.
    pub fn take(&mut self, lines: &[&str]) -> Result<(), String> {
        for line in lines {
            self.inbox.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
            self.inbox.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        self.inbox.flush().map_err(|e| e.to_string())?;
        self.inbox_file.sync_data().map_err(|e| e.to_string())
    }

    fn bin(&mut self, kind: &'static str, line: &str, why: String) {
        let n = self.bins.entry(kind).or_insert(0);
        *n += 1;
        if *n <= 20 {
            self.binned.push((kind.to_string(), format!("{why}: {}", line.chars().take(100).collect::<String>())));
        }
    }

    /// Checks one event, logs it, and makes its units: an event in clock order waits in the
    /// clock queue; the rest are returned, ready.
    fn units_of(&mut self, event: &Event, text: &str) -> Result<Option<Vec<Unit>>, String> {
        if let Err(e) = crate::domain::check(&event.ty, &event.source) {
            self.bin("another domain", text, e);
            return Ok(None);
        }
        if let Err(e) = self.schemas.check(&event.ty, &event.data) {
            if event.source.contains("/flow/") {
                self.invalid.push((event.ty.clone(), e));
            } else {
                self.bin("malformed", text, e);
            }
            return Ok(None);
        }
        if let Some(log) = &mut self.log {
            log.write_all(text.as_bytes()).map_err(|e| e.to_string())?;
            log.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        let mut ready = Vec::new();
        if let Some(fact) = fact_of(event) {
            ready.push(Unit { event: event.clone(), target: Target::Fact, kind: fact.kind.to_string(), key: fact.key });
        }
        for &f in self.routes.get(&event.ty).map(Vec::as_slice).unwrap_or(&[]) {
            let flow = &self.flows[f];
            let unit = Unit { event: event.clone(), target: Target::Flow(f), kind: flow.row_kind.clone(), key: flow.key(event) };
            if flow.clocked(&event.ty) || self.timers.contains(&event.ty) {
                self.clocked.insert((event.time, event.id.clone(), unit.target), unit);
            } else {
                ready.push(unit);
            }
        }
        Ok(Some(ready))
    }

    /// Hands units to the shards and waits for them; returns what they sent and how many finished.
    fn pass(&mut self, units: Vec<Unit>, waiting: &mut Vec<Unit>, retry: &mut Vec<Unit>) -> Result<(Vec<Event>, usize), String> {
        let total = units.len();
        let n = self.workers.len();
        let mut parts: Vec<Vec<Unit>> = vec![Vec::new(); n];
        for unit in units {
            parts[shard_of(&unit.kind, &unit.key, n)].push(unit);
        }
        for (i, part) in parts.into_iter().enumerate() {
            self.workers[i].0.send(Job::Units(part)).map_err(|e| e.to_string())?;
        }
        let mut sent = Vec::new();
        let mut unfinished = 0;
        for i in 0..n {
            match self.workers[i].1.recv().map_err(|e| e.to_string())? {
                Reply::Done(o) => {
                    sent.extend(o.sent);
                    unfinished += o.waiting.len() + o.retry.len();
                    self.stalls.extend(o.stalls);
                    waiting.extend(o.waiting);
                    retry.extend(o.retry);
                }
                _ => return Err("worker out of step".into()),
            }
        }
        self.units += total as u64;
        Ok((sent, total - unfinished))
    }

    /// Runs one round: reads what the inbox holds (no more than the lateness of event time), runs
    /// it and everything it causes now, the clock queue one moment of event time at a time, then
    /// makes it all durable together. Returns whether it did anything.
    ///
    /// # Errors
    /// A file cannot be read or written, or a shard fails.
    #[allow(clippy::too_many_lines)]
    pub fn round(&mut self) -> Result<bool, String> {
        self.rounds += 1;
        let mut ready: Vec<Unit> = Vec::new();
        let mut line = String::new();
        let mut first_time: Option<i64> = None;
        let mut read = 0;
        let most = round_lines();
        while read < most {
            line.clear();
            let n = self.reader.read_line(&mut line).map_err(|e| e.to_string())?;
            if n == 0 || !line.ends_with('\n') {
                if n > 0 {
                    self.reader.seek(SeekFrom::Start(self.offset)).map_err(|e| e.to_string())?;
                }
                break;
            }
            self.offset += n as u64;
            read += 1;
            self.read += 1;
            let text = line.trim_end().to_string();
            let event = match admit(&text, 0) {
                Ok(e) => e,
                Err(e) => {
                    self.bin("malformed", &text, e);
                    continue;
                }
            };
            // A step that ran in the round that sent it is on the inbox marked so: the round that
            // ran it committed with it, or was undone with it and will send it again.
            if text.contains("\"inline\":true") {
                continue;
            }
            if !event.source.contains("/flow/") {
                if let Some(max) = self.max_external {
                    if event.time < max - self.lateness {
                        self.bin("later than the declared lateness", &text, format!("{} ms behind", max - event.time));
                        continue;
                    }
                    self.max_lateness = self.max_lateness.max(max - event.time);
                }
                self.max_external = Some(self.max_external.map_or(event.time, |m| m.max(event.time)));
                // With more than one shard a round is not one transaction, so it spans no more than
                // the lateness: what a restart hands again stays inside the rows' windows. With one
                // shard the round and the lane's place commit together.
                let start = *first_time.get_or_insert(event.time);
                if (self.workers.len() > 1 || std::env::var("TWO_LANE_SPAN").is_ok()) && event.time - start > self.lateness {
                    read = most;
                }
            }
            if let Some(units) = self.units_of(&event, &text)? {
                ready.extend(units);
            }
        }
        let watermark = self.max_external.map(|max| if self.ended { max.saturating_add(1) } else { max - self.lateness });
        let mut waiting = std::mem::take(&mut self.waiting);
        let mut retry = Vec::new();
        let mut out: Vec<Event> = Vec::new();
        let mut retry_waiting = true;
        let mut finished = 0;
        loop {
            if ready.is_empty() {
                if retry_waiting && !waiting.is_empty() {
                    ready = std::mem::take(&mut waiting);
                    retry_waiting = false;
                } else if let Some(w) = watermark
                    && let Some(t) = self.clocked.keys().next().map(|k| k.0)
                    && t < w
                {
                    while self.clocked.keys().next().is_some_and(|k| k.0 == t) {
                        if let Some((_, u)) = self.clocked.pop_first() {
                            ready.push(u);
                        }
                    }
                } else {
                    break;
                }
            }
            let (sent, done) = self.pass(std::mem::take(&mut ready), &mut waiting, &mut retry)?;
            finished += done;
            if done > 0 {
                retry_waiting = true;
            }
            for event in sent {
                let text = to_json(&event);
                if let Some(units) = self.units_of(&event, &text)? {
                    ready.extend(units);
                }
                out.push(event);
            }
        }
        self.waiting = waiting;
        self.waiting.extend(retry);
        if read == 0 && finished == 0 && out.is_empty() {
            return Ok(false);
        }
        if !out.is_empty() {
            for e in &out {
                let mut text = to_json(e);
                text.truncate(text.len() - 1);
                text.push_str(",\"inline\":true}");
                self.inbox.write_all(text.as_bytes()).map_err(|e| e.to_string())?;
                self.inbox.write_all(b"\n").map_err(|e| e.to_string())?;
            }
            self.inbox.flush().map_err(|e| e.to_string())?;
            self.inbox_file.sync_data().map_err(|e| e.to_string())?;
        }
        if let Some(log) = &mut self.log {
            log.flush().map_err(|e| e.to_string())?;
        }
        let mut held: Vec<Unit> = self.clocked.values().cloned().collect();
        held.extend(self.waiting.iter().cloned());
        let place = serde_json::to_string(&Place { offset: self.offset, max_external: self.max_external, held }).map_err(|e| e.to_string())?;
        // The other shards commit together; the first shard, which holds the lane's place, last.
        let n = self.workers.len();
        for i in 1..n {
            self.workers[i].0.send(Job::Commit(None)).map_err(|e| e.to_string())?;
        }
        for i in 1..n {
            match self.workers[i].1.recv().map_err(|e| e.to_string())? {
                Reply::Committed(r) => r?,
                _ => return Err("worker out of step".into()),
            }
        }
        self.workers[0].0.send(Job::Commit(Some(place))).map_err(|e| e.to_string())?;
        match self.workers[0].1.recv().map_err(|e| e.to_string())? {
            Reply::Committed(r) => r?,
            _ => return Err("worker out of step".into()),
        }
        Ok(true)
    }

    /// Runs rounds until nothing is left to do.
    ///
    /// # Errors
    /// A round fails.
    pub fn drain(&mut self) -> Result<(), String> {
        let mut idle = 0;
        while idle < 3 {
            if self.round()? { idle = 0 } else { idle += 1 }
        }
        Ok(())
    }

    /// Every shard's rows and edges, as text.
    ///
    /// # Errors
    /// A shard cannot be read.
    pub fn text(&self) -> Result<String, String> {
        let mut out = String::new();
        let mut parts = Vec::new();
        for w in &self.workers {
            w.0.send(Job::Text).map_err(|e| e.to_string())?;
            match w.1.recv().map_err(|e| e.to_string())? {
                Reply::Text(t) => parts.push(t?),
                _ => return Err("worker out of step".into()),
            }
        }
        let mut lines: Vec<&str> = parts.iter().flat_map(|p| p.lines()).collect();
        lines.sort_unstable();
        for l in lines {
            out.push_str(l);
            out.push('\n');
        }
        Ok(out)
    }

    /// Reads of the row store across shards: rows read back, rows looked for and absent, and
    /// rows handed from memory.
    ///
    /// # Errors
    /// A shard does not answer.
    pub fn reads(&self) -> Result<(u64, u64, u64), String> {
        let mut total = (0, 0, 0);
        for w in &self.workers {
            w.0.send(Job::Reads).map_err(|e| e.to_string())?;
            match w.1.recv().map_err(|e| e.to_string())? {
                Reply::Reads(a, b, c) => total = (total.0 + a, total.1 + b, total.2 + c),
                _ => return Err("worker out of step".into()),
            }
        }
        Ok(total)
    }

    #[must_use]
    pub fn held(&self) -> usize {
        self.clocked.len() + self.waiting.len()
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        for (tx, _, _) in &self.workers {
            let _ = tx.send(Job::Stop);
        }
        for (_, _, handle) in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}
