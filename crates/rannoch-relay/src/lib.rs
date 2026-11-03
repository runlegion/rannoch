//! Relays: a declared shape's neighbourhood kept warm in memory. A `rannoch.query` naming a shape
//! and a view is answered by an acknowledgement, not the nodes; the app reads the warm set in
//! process and hears of its changes as events. Everything here can be rebuilt from the log: a
//! relay's file only saves time.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rannoch_cloudevents::{EventId, attributes};
use rannoch_queue::Queue;
use rannoch_store::{Cursor, Declaration, Graph, Shape, View, open};
use serde::{Deserialize, Serialize};
use serde_json::json;

use warm::{Saved, Warm};
pub use warm::{StepSet, WarmSet};

mod warm;

/// Why a relay did not do what was asked.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Refused, with the reason.
    #[error("{0}")]
    Refused(String),
    /// The graph refused or failed.
    #[error(transparent)]
    Store(#[from] rannoch_store::Error),
    /// The relay's directory failed.
    #[error("the relays' directory failed: {0}")]
    Io(#[from] std::io::Error),
    /// A relay's file or an event could not be encoded or decoded.
    #[error("a relay could not be encoded or decoded: {0}")]
    Encoding(#[from] serde_json::Error),
    /// A subscriber panicked, with the panic's text.
    #[error("a relay's subscriber panicked: {0}")]
    Subscriber(String),
}

impl From<rannoch_cloudevents::Error> for Error {
    fn from(error: rannoch_cloudevents::Error) -> Error {
        Error::Store(error.into())
    }
}

/// The worker that takes `rannoch.query` events carrying `shape`.
pub const QUERIES: &str = "rannoch.relay";
/// How often a relay's file is replaced, at most.
const SAVE_EVERY: Duration = Duration::from_secs(1);
/// How long the relays' thread sleeps with no commit, so a file due is still saved.
const IDLE: Duration = Duration::from_millis(250);
const MANIFEST: &str = "relays.json";
const QUERY: &str = "rannoch.query";
const ANSWER: &str = "rannoch.answer";
const REPORT: &str = "rannoch.report";
const CHANGED: &str = "rannoch.relay.changed";
const REPLAY_CHUNK: usize = 4096;

type Subscriber = Box<dyn FnMut(&str) + Send>;

/// A view as an event or the manifest states it: an axis point, and a known-at in milliseconds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct ViewData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    axis: Option<Vec<i64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    known: Option<i64>,
}

impl ViewData {
    fn view(&self) -> View {
        View {
            axis: self.axis.clone(),
            known: self
                .known
                .and_then(|known| u64::try_from(known).ok())
                .map(|known| UNIX_EPOCH + Duration::from_millis(known)),
        }
    }
}

pub(crate) fn millis(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |since| {
        i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
    })
}

/// One relay as the manifest holds it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    id: String,
    shape: String,
    view: ViewData,
    holders: BTreeSet<String>,
}

struct Live {
    entry: Entry,
    warm: Warm,
    /// Events folded and not yet closed: a relay closes them only once its file is saved.
    unsaved: Vec<Cursor>,
    saved_at: Option<Instant>,
    /// Folded since the file was last saved.
    dirty: bool,
}

struct State {
    graph: Graph,
    relays: HashMap<String, Live>,
}

struct Wake {
    pending: bool,
    stop: bool,
}

struct Inner {
    queue: Arc<Queue>,
    dir: PathBuf,
    path: PathBuf,
    key: [u8; 32],
    state: Mutex<State>,
    /// What `read` answers from: taking it never waits on a fold or touches the graph.
    published: RwLock<HashMap<String, WarmSet>>,
    subscribers: Mutex<Vec<Subscriber>>,
    failures: Mutex<Vec<Error>>,
    wake: Mutex<Wake>,
    woken: Condvar,
}

/// An app's relays: their worker thread, their directory and the warm sets.
pub struct Relays {
    inner: Arc<Inner>,
    thread: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Relays {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relays")
            .field("dir", &self.inner.dir)
            .finish_non_exhaustive()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Relays {
    /// Opens the relays kept in `dir` for the graph at `path`, under `queue`: each relay in the
    /// manifest resumes from its file, or reseeds when the file is missing or behind its worker's
    /// cursor. `key` is the graph's, which reading a report back needs.
    ///
    /// # Errors
    /// The directory or the graph cannot be read, or a relay cannot be seeded.
    pub fn open(
        dir: impl AsRef<Path>,
        queue: Arc<Queue>,
        path: impl AsRef<Path>,
        key: &[u8; 32],
    ) -> Result<Relays, Error> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let graph = open(&path, key)?;
        let entries: Vec<Entry> = match fs::read(dir.join(MANIFEST)) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let _ = queue.join(QUERIES);
        let mut relays = HashMap::new();
        let mut published = HashMap::new();
        for entry in entries {
            let live = resume(&dir, &queue, &graph, entry)?;
            published.insert(live.entry.id.clone(), live.warm.set.clone());
            relays.insert(live.entry.id.clone(), live);
        }
        let inner = Arc::new(Inner {
            queue,
            dir,
            path: path.as_ref().to_path_buf(),
            key: *key,
            state: Mutex::new(State { graph, relays }),
            published: RwLock::new(published),
            subscribers: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            wake: Mutex::new(Wake {
                pending: true,
                stop: false,
            }),
            woken: Condvar::new(),
        });
        let told = Arc::downgrade(&inner);
        inner.queue.subscribe(move |_| notify(&told));
        let beat = Arc::clone(&inner);
        let thread = std::thread::spawn(move || beat.run());
        Ok(Relays {
            inner,
            thread: Some(thread),
        })
    }

    /// Answers the `rannoch.query` event `query`, carrying `shape` and `view`, with the body of a
    /// `rannoch.answer` event acknowledging the relay of that shape and view, seeded now unless
    /// one is held already. The query's `source` holds the relay. A query in the inbox is answered
    /// by the worker [`QUERIES`] the same way, its answer accepted back into the inbox.
    ///
    /// # Errors
    /// [`Error::Refused`] when `query` is not a `rannoch.query` carrying `shape`, or the shape is
    /// not declared; the view does not fit the shape, or the graph cannot be read.
    pub fn answer(&self, query: &str) -> Result<String, Error> {
        self.inner.answer(query)
    }

    /// The warm set of relay `id`, read without touching the graph.
    #[must_use]
    pub fn read(&self, id: &str) -> Option<WarmSet> {
        self.inner
            .published
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// `app` no longer holds relay `id`. A relay no app holds is dropped: its worker and file go.
    ///
    /// # Errors
    /// [`Error::Refused`] when no relay has that id; the directory or graph cannot be written.
    pub fn release(&self, id: &str, app: &str) -> Result<(), Error> {
        self.inner.release(id, app)
    }

    /// Calls `subscriber` with the body of each `rannoch.relay.changed` event, from the relays'
    /// thread. Change events are not logged: they can be rebuilt from the log.
    pub fn subscribe(&self, subscriber: impl FnMut(&str) + Send + 'static) {
        lock(&self.inner.subscribers).push(Box::new(subscriber));
    }

    /// Makes relay `id`'s warm set durable: accepts a `rannoch.report` event naming its shape,
    /// view and the cursor it is warm to now, nothing inlined, and returns the event's body.
    ///
    /// # Errors
    /// [`Error::Refused`] when no relay has that id; the event is not accepted.
    pub fn report(&self, id: &str) -> Result<String, Error> {
        self.inner.report(id)
    }

    /// The warm set a `rannoch.report` event froze: the log up to its cursor is replayed into a
    /// temporary graph through fill and its shape walked there.
    ///
    /// # Errors
    /// [`Error::Refused`] when `report` is no report or names a shape not declared; the graph or
    /// the temporary graph cannot be read or written.
    pub fn reread(&self, report: &str) -> Result<WarmSet, Error> {
        self.inner.reread(report)
    }

    /// Takes what failed on the relays' thread, oldest first. A relay that fails to read the graph
    /// reseeds on its next wake, so nothing here means a warm set is wrong for long.
    #[must_use]
    pub fn take_failures(&self) -> Vec<Error> {
        std::mem::take(&mut lock(&self.inner.failures))
    }

    fn stop(&mut self) {
        lock(&self.inner.wake).stop = true;
        self.inner.woken.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// Stops the relays' thread, and returns what failed. A file not due is not saved: its events
    /// are handed again after a restart, which costs a moment and no truth.
    #[must_use]
    pub fn close(mut self) -> Vec<Error> {
        self.stop();
        self.take_failures()
    }
}

impl Drop for Relays {
    fn drop(&mut self) {
        self.stop();
    }
}

fn notify(inner: &Weak<Inner>) {
    if let Some(inner) = inner.upgrade() {
        lock(&inner.wake).pending = true;
        inner.woken.notify_all();
    }
}

fn worker_name(id: &str) -> String {
    format!("{QUERIES}.{id}")
}

fn relay_file(dir: &Path, id: &str) -> PathBuf {
    dir.join(format!("{id}.json"))
}

/// Replaces `path` with `bytes` by an atomic rename.
fn replace(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes)?;
    fs::rename(&temporary, path)?;
    Ok(())
}

/// A relay of the manifest, from its file when that is not behind its worker's cursor, otherwise
/// seeded again. Its file's cursor is the one that counts.
fn resume(dir: &Path, queue: &Queue, graph: &Graph, entry: Entry) -> Result<Live, Error> {
    let shape = shape_of(graph, &entry.shape)?;
    let place = queue.join(&worker_name(&entry.id));
    let saved = fs::read(relay_file(dir, &entry.id))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Saved>(&bytes).ok())
        .filter(|saved| saved.cursor >= place);
    let view = entry.view.view();
    let (warm, fresh) = match saved {
        Some(saved) => (Warm::restored(graph, shape, view, saved)?, false),
        None => (Warm::seed(graph, shape, view, place)?, true),
    };
    let mut live = Live {
        entry,
        warm,
        unsaved: Vec::new(),
        saved_at: None,
        dirty: fresh,
    };
    if fresh {
        save(dir, &mut live)?;
    }
    Ok(live)
}

fn shape_of(graph: &Graph, name: &str) -> Result<Shape, Error> {
    Ok(graph
        .current_ontology()?
        .shape(name)
        .map_err(|refused| Error::Refused(refused.0))?
        .clone())
}

/// Saves `live`'s file.
fn save(dir: &Path, live: &mut Live) -> Result<(), Error> {
    replace(
        &relay_file(dir, &live.entry.id),
        &serde_json::to_vec(&live.warm.saved())?,
    )?;
    live.saved_at = Some(Instant::now());
    live.dirty = false;
    Ok(())
}

#[derive(Deserialize)]
struct Asked {
    data: AskedData,
}

#[derive(Deserialize)]
struct AskedData {
    shape: String,
    #[serde(default)]
    view: ViewData,
}

impl Inner {
    fn run(self: Arc<Self>) {
        loop {
            {
                let wake = lock(&self.wake);
                let (mut wake, _) = self
                    .woken
                    .wait_timeout_while(wake, IDLE, |w| !w.pending && !w.stop)
                    .unwrap_or_else(PoisonError::into_inner);
                wake.pending = false;
                if wake.stop {
                    drop(wake);
                    self.pump();
                    return;
                }
            }
            self.pump();
        }
    }

    fn failed(&self, error: Error) {
        lock(&self.failures).push(error);
    }

    /// Answers the queries in the inbox, then brings every relay up to the watermark.
    fn pump(&self) {
        if let Err(error) = self.pump_queries() {
            self.failed(error);
        }
        let mut state = lock(&self.state);
        let State { graph, relays } = &mut *state;
        let mut changes = Vec::new();
        for live in relays.values_mut() {
            match self.pump_relay(graph, live) {
                Ok(Some(change)) => changes.push(change),
                Ok(None) => {}
                Err(error) => self.failed(error),
            }
            self.publish(live);
        }
        drop(state);
        self.emit(&changes);
    }

    fn pump_queries(&self) -> Result<(), Error> {
        while let Some(event) = self.queue.next(QUERIES)? {
            if is_shape_query(&event.body) {
                match self.answer(&event.body) {
                    Ok(answer) => {
                        self.queue.accept(answer)?;
                        self.queue.done(QUERIES, event.cursor)?;
                    }
                    Err(error) => {
                        self.queue
                            .failed(QUERIES, event.cursor, &error.to_string())?;
                    }
                }
            } else {
                self.queue.done(QUERIES, event.cursor)?;
            }
        }
        Ok(())
    }

    /// Folds everything past `live`'s cursor as one batch, so one commit makes at most one change.
    fn pump_relay(&self, graph: &Graph, live: &mut Live) -> Result<Option<String>, Error> {
        let worker = worker_name(&live.entry.id);
        let mut folded = false;
        while let Some(event) = self.queue.next(&worker)? {
            if event.cursor <= live.warm.set.cursor() {
                self.queue.done(&worker, event.cursor)?;
                continue;
            }
            live.warm.fold(graph, event.cursor, &event.body);
            live.unsaved.push(event.cursor);
            folded = true;
        }
        let mut change = None;
        if folded {
            live.dirty = true;
            match live.warm.settle(graph) {
                Ok(Some(data)) => {
                    change = Some(
                        json!({
                            "specversion": "1.0",
                            "id": EventId::now(),
                            "source": "rannoch",
                            "type": CHANGED,
                            "subject": live.entry.id,
                            "data": data,
                        })
                        .to_string(),
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    live.warm.mark_stale();
                    self.failed(error);
                }
            }
        }
        if live.dirty && live.saved_at.is_none_or(|at| at.elapsed() >= SAVE_EVERY) {
            save(&self.dir, live)?;
            for cursor in live.unsaved.drain(..) {
                self.queue.done(&worker, cursor)?;
            }
        }
        Ok(change)
    }

    fn publish(&self, live: &Live) {
        self.published
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(live.entry.id.clone(), live.warm.set.clone());
    }

    fn emit(&self, changes: &[String]) {
        let mut subscribers = lock(&self.subscribers);
        for change in changes {
            for subscriber in subscribers.iter_mut() {
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| subscriber(change))) {
                    let text = panic
                        .downcast_ref::<&str>()
                        .map(ToString::to_string)
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    self.failed(Error::Subscriber(text));
                }
            }
        }
    }

    fn answer(&self, query: &str) -> Result<String, Error> {
        let event = attributes(query)?;
        if event.ty != QUERY {
            return Err(Error::Refused(format!(
                "event {} is of type {}, not {QUERY}",
                event.id, event.ty
            )));
        }
        if !is_shape_query(query) {
            return Err(Error::Refused(format!("query {} names no shape", event.id)));
        }
        let Asked { data } = serde_json::from_str(query)?;
        let mut state = lock(&self.state);
        let State { graph, relays } = &mut *state;
        let shared = relays
            .values_mut()
            .find(|live| live.entry.shape == data.shape && live.entry.view == data.view);
        let (id, set) = if let Some(live) = shared {
            if live.entry.holders.insert(event.source.clone()) {
                write_manifest(&self.dir, relays)?;
            }
            let live = relays
                .values()
                .find(|live| live.entry.shape == data.shape && live.entry.view == data.view)
                .ok_or_else(|| Error::Refused("the relay went".to_string()))?;
            (live.entry.id.clone(), live.warm.set.clone())
        } else {
            let id = EventId::now().to_string();
            let shape = shape_of(graph, &data.shape)?;
            let at = self.queue.join(&worker_name(&id));
            let seeded = Warm::seed(graph, shape, data.view.view(), at);
            let warm = match seeded {
                Ok(warm) => warm,
                Err(error) => {
                    self.queue.remove_worker(&worker_name(&id))?;
                    return Err(error);
                }
            };
            let mut live = Live {
                entry: Entry {
                    id: id.clone(),
                    shape: data.shape,
                    view: data.view,
                    holders: BTreeSet::from([event.source.clone()]),
                },
                warm,
                unsaved: Vec::new(),
                saved_at: None,
                dirty: true,
            };
            save(&self.dir, &mut live)?;
            let set = live.warm.set.clone();
            self.publish(&live);
            relays.insert(id.clone(), live);
            write_manifest(&self.dir, relays)?;
            (id, set)
        };
        Ok(json!({
            "specversion": "1.0",
            "id": EventId::now(),
            "source": "rannoch",
            "type": ANSWER,
            "subject": event.id,
            "data": {
                "result": {"relay": id, "cursor": set.cursor(), "steps": set.counts()},
                "known": set.known(),
            },
        })
        .to_string())
    }

    fn release(&self, id: &str, app: &str) -> Result<(), Error> {
        let mut state = lock(&self.state);
        let relays = &mut state.relays;
        let live = relays
            .get_mut(id)
            .ok_or_else(|| Error::Refused(format!("no relay has the id {id}")))?;
        live.entry.holders.remove(app);
        if live.entry.holders.is_empty() {
            relays.remove(id);
            self.published
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(id);
            self.queue.remove_worker(&worker_name(id))?;
            match fs::remove_file(relay_file(&self.dir, id)) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    return Err(error.into());
                }
                _ => {}
            }
        }
        write_manifest(&self.dir, relays)
    }

    fn report(&self, id: &str) -> Result<String, Error> {
        let state = lock(&self.state);
        let live = state
            .relays
            .get(id)
            .ok_or_else(|| Error::Refused(format!("no relay has the id {id}")))?;
        let body = json!({
            "specversion": "1.0",
            "id": EventId::now(),
            "source": "rannoch",
            "type": REPORT,
            "subject": id,
            "data": {
                "shape": live.entry.shape,
                "view": live.entry.view,
                "cursor": live.warm.set.cursor(),
            },
        })
        .to_string();
        drop(state);
        self.queue.accept(body.clone())?;
        Ok(body)
    }

    fn reread(&self, report: &str) -> Result<WarmSet, Error> {
        #[derive(Deserialize)]
        struct Reported {
            data: ReportedData,
        }
        #[derive(Deserialize)]
        struct ReportedData {
            shape: String,
            #[serde(default)]
            view: ViewData,
            cursor: Cursor,
        }
        let event = attributes(report)?;
        if event.ty != REPORT {
            return Err(Error::Refused(format!(
                "event {} is of type {}, not {REPORT}",
                event.id, event.ty
            )));
        }
        let Reported { data } = serde_json::from_str(report)?;
        let source = open(&self.path, &self.key)?;
        let shape = shape_of(&source, &data.shape)?;
        let temporary = Temporary(self.dir.join(format!("report-{}.db", EventId::now())));
        let mut replayed = open(&temporary.0, &self.key)?;
        replay(&source, &mut replayed, data.cursor)?;
        let warm = Warm::seed(&replayed, shape, data.view.view(), data.cursor)?;
        Ok(warm.set)
    }
}

/// Whether `body` is a `rannoch.query` event whose data names a shape.
fn is_shape_query(body: &str) -> bool {
    #[derive(Deserialize)]
    struct Probe {
        #[serde(rename = "type")]
        ty: String,
        #[serde(default)]
        data: Option<serde_json::Value>,
    }
    body.contains(QUERY)
        && serde_json::from_str::<Probe>(body).is_ok_and(|probe| {
            probe.ty == QUERY && probe.data.is_some_and(|data| data.get("shape").is_some())
        })
}

/// The manifest: every relay's shape, view and holders.
fn write_manifest(dir: &Path, relays: &HashMap<String, Live>) -> Result<(), Error> {
    let by_id: BTreeMap<&String, &Entry> = relays.iter().map(|(id, l)| (id, &l.entry)).collect();
    let entries: Vec<&Entry> = by_id.into_values().collect();
    replace(&dir.join(MANIFEST), &serde_json::to_vec(&entries)?)
}

/// A file and its SQLite companions, removed when it goes out of scope.
struct Temporary(PathBuf);

impl Drop for Temporary {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.0.clone().into_os_string();
            path.push(suffix);
            let _ = fs::remove_file(path);
        }
    }
}

/// Fills `into`, an empty graph, from `source`'s log up to `until`: what `source` declares now,
/// then every event but its declarations and the closed events fill appended, which filling
/// appends again.
fn replay(source: &Graph, into: &mut Graph, until: Cursor) -> Result<(), Error> {
    let ontology = source.current_ontology()?;
    let declared = |name: &str| {
        json!({
            "specversion": "1.0", "id": format!("declared-{name}"),
            "source": "rannoch", "type": format!("rannoch.declared.{name}"),
        })
        .to_string()
    };
    for class in ontology.classes.values() {
        into.declare(&Declaration::Class(class.clone()), &declared(&class.name))?;
    }
    for relation in ontology.relations.values() {
        into.declare(
            &Declaration::Relation(relation.clone()),
            &declared(&relation.name),
        )?;
    }
    let mut from = Cursor::START;
    loop {
        let events = source.events().chunk(from, until, REPLAY_CHUNK)?;
        let Some(last) = events.last() else {
            break;
        };
        from = last.cursor;
        let bodies: Vec<&str> = events
            .iter()
            .map(|event| event.body.as_str())
            .filter(|body| {
                attributes(body).is_ok_and(|event| {
                    !event.ty.starts_with("rannoch.declared.")
                        && !event.ty.starts_with("rannoch.closed.")
                })
            })
            .collect();
        for landed in into.commit(&bodies, &[])? {
            landed?;
        }
        into.fill()?;
    }
    Ok(())
}
