//! Relays: a declared shape's neighbourhood kept warm in memory. A `rannoch.query` naming a shape
//! and a view is answered by an acknowledgement, not the nodes; the app reads the warm set in
//! process and hears of its changes as events. Everything here can be rebuilt from the log: a
//! relay's file only saves time.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rannoch_cloudevents::{Attributes, EventId, attributes};
use rannoch_queue::Queue;
use rannoch_store::{
    ANSWER, CLOSED_PREFIX, Cursor, DECLARED_PREFIX, Declaration, Graph, PURGED, QUERY, REFILLED,
    Shape, ViewData, open,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

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
    /// The relays' thread panicked, with the panic's text, and stopped.
    #[error("the relays' thread panicked: {0}")]
    Thread(String),
    /// This many older failures were dropped before they were taken.
    #[error("{0} older failures were dropped before they were taken")]
    Lost(u64),
}

/// The worker that takes `rannoch.query` events carrying `shape`.
const QUERIES: &str = "rannoch.relay";
/// How often a relay's file is replaced, at most.
const SAVE_EVERY: Duration = Duration::from_secs(1);
/// How long the relays' thread sleeps with no commit, so a file due is still saved.
const IDLE: Duration = Duration::from_millis(250);
const MANIFEST: &str = "relays.json";
const REPORT: &str = "rannoch.report";
const CHANGED: &str = "rannoch.relay.changed";
const REPLAY_CHUNK: usize = 4096;
/// How many failures wait to be taken. Past this the oldest go, and are counted.
const MAX_FAILURES: usize = 256;

type Subscriber = Box<dyn FnMut(&str) + Send>;

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
}

impl Live {
    fn new(entry: Entry, warm: Warm) -> Live {
        Live {
            entry,
            warm,
            unsaved: Vec::new(),
            saved_at: None,
        }
    }
}

struct State {
    graph: Graph,
    relays: HashMap<String, Live>,
}

/// What failed on the relays' thread and has not been taken: the newest [`MAX_FAILURES`], and how
/// many older ones went.
#[derive(Default)]
struct Failures {
    newest: VecDeque<Error>,
    lost: u64,
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
    failures: Mutex<Failures>,
    wake: Mutex<Wake>,
    woken: Condvar,
    /// Makes the next pump panic, so a test can see the panic surface from `close`.
    #[cfg(test)]
    panic_next: std::sync::atomic::AtomicBool,
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
            failures: Mutex::default(),
            wake: Mutex::new(Wake {
                pending: true,
                stop: false,
            }),
            woken: Condvar::new(),
            #[cfg(test)]
            panic_next: std::sync::atomic::AtomicBool::new(false),
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
        let inner = &*self.inner;
        let mut state = lock(&inner.state);
        let relays = &mut state.relays;
        let live = relays
            .get_mut(id)
            .ok_or_else(|| Error::Refused(format!("no relay has the id {id}")))?;
        live.entry.holders.remove(app);
        if live.entry.holders.is_empty() {
            relays.remove(id);
            inner
                .published
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(id);
            inner.queue.remove_worker(&worker_name(id))?;
            match fs::remove_file(relay_file(&inner.dir, id)) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    return Err(error.into());
                }
                _ => {}
            }
        }
        write_manifest(&inner.dir, relays)
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
        let state = lock(&self.inner.state);
        let live = state
            .relays
            .get(id)
            .ok_or_else(|| Error::Refused(format!("no relay has the id {id}")))?;
        let body = cloud_event(
            REPORT,
            id,
            &json!({
                "shape": live.entry.shape,
                "view": live.entry.view,
                "cursor": live.warm.set.cursor(),
            }),
        );
        drop(state);
        self.inner.queue.accept(body.clone())?;
        Ok(body)
    }

    /// The warm set a `rannoch.report` event froze: the log up to its cursor is replayed into a
    /// temporary graph through fill and its shape walked there.
    ///
    /// # Errors
    /// [`Error::Refused`] when `report` is no report or names a shape not declared; the graph or
    /// the temporary graph cannot be read or written.
    pub fn reread(&self, report: &str) -> Result<WarmSet, Error> {
        #[derive(Deserialize)]
        struct Reported {
            shape: String,
            #[serde(default)]
            view: ViewData,
            cursor: Cursor,
        }
        let inner = &*self.inner;
        let data: Reported = read_event(report)?.of_type(REPORT)?;
        let source = open(&inner.path, &inner.key)?;
        let shape = shape_of(&source, &data.shape)?;
        let temporary = Temporary(inner.dir.join(format!("report-{}.db", EventId::now())));
        let mut replayed = open(&temporary.0, &inner.key)?;
        replay(&source, &mut replayed, data.cursor)?;
        let warm = Warm::seed(&replayed, shape, data.view.view(), data.cursor)?;
        Ok(warm.set)
    }

    /// Takes what failed on the relays' thread, oldest first, so a long-running app need not wait
    /// for [`Relays::close`]. A relay that fails to read the graph reseeds on its next wake, so
    /// nothing here means a warm set is wrong for long. At most 256 wait: past that the oldest go,
    /// and an [`Error::Lost`] comes first, saying how many.
    #[must_use]
    pub fn take_failures(&self) -> Vec<Error> {
        let taken = std::mem::take(&mut *lock(&self.inner.failures));
        (taken.lost > 0)
            .then_some(Error::Lost(taken.lost))
            .into_iter()
            .chain(taken.newest)
            .collect()
    }

    fn stop(&mut self) {
        lock(&self.inner.wake).stop = true;
        self.inner.woken.notify_all();
        if let Some(thread) = self.thread.take()
            && let Err(panic) = thread.join()
        {
            self.inner.failed(Error::Thread(panic_text(&*panic)));
        }
    }

    /// Stops the relays' thread, and returns what failed, a panic of that thread included. A file not due is not saved: its events
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

/// Removes relay `id`'s file and the temporary a save writes before it replaces the file.
fn remove_files(dir: &Path, id: &str) -> Result<(), Error> {
    let file = relay_file(dir, id);
    for path in [file.with_extension("tmp"), file] {
        match fs::remove_file(path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(error.into());
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether the log holds a purge event after `cursor`: a relay's file from before it may hold what
/// left.
fn purged_since(graph: &Graph, mut cursor: Cursor) -> Result<bool, Error> {
    let log = graph.events();
    let high = log.committed()?;
    loop {
        let events = log.chunk(cursor, high, REPLAY_CHUNK)?;
        let Some(last) = events.last() else {
            return Ok(false);
        };
        cursor = last.cursor;
        if events
            .iter()
            .any(|event| attributes(&event.body).is_ok_and(|event| event.ty == PURGED))
        {
            return Ok(true);
        }
    }
}

/// A relay of the manifest, from its file when that is not behind its worker's cursor and no purge
/// came after it, otherwise seeded again, its file replaced before anything can read it. Its
/// file's cursor is the one that counts.
fn resume(dir: &Path, queue: &Queue, graph: &Graph, entry: Entry) -> Result<Live, Error> {
    let shape = shape_of(graph, &entry.shape)?;
    let place = queue.join(&worker_name(&entry.id));
    let saved = fs::read(relay_file(dir, &entry.id))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Saved>(&bytes).ok())
        .filter(|saved| saved.cursor >= place);
    let saved = match saved {
        Some(saved) if purged_since(graph, saved.cursor)? => {
            remove_files(dir, &entry.id)?;
            None
        }
        saved => saved,
    };
    let view = entry.view.view();
    if let Some(saved) = saved {
        return Ok(Live::new(entry, Warm::restored(graph, shape, view, saved)?));
    }
    let mut live = Live::new(entry, Warm::seed(graph, shape, view, place)?);
    save(dir, &mut live)?;
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
    Ok(())
}

/// An event with its data read as `D`, from one parse of its body.
#[derive(Deserialize)]
struct Read<D> {
    #[serde(flatten)]
    attributes: Attributes,
    data: D,
}

fn read_event<D: DeserializeOwned>(body: &str) -> Result<Read<D>, Error> {
    Ok(serde_json::from_str(body)?)
}

impl<D> Read<D> {
    /// Its data, when it is a `CloudEvents` 1.0 event of type `ty`.
    fn of_type(self, ty: &str) -> Result<D, Error> {
        let event = self.attributes;
        if event.specversion != "1.0" || event.ty != ty {
            return Err(Error::Refused(format!(
                "event {} is of type {}, not {ty}",
                event.id, event.ty
            )));
        }
        Ok(self.data)
    }
}

#[derive(Deserialize)]
struct Asked {
    shape: String,
    #[serde(default)]
    view: ViewData,
}

/// A `CloudEvent` from rannoch, under a fresh id.
fn cloud_event(ty: &str, subject: &str, data: &Json) -> String {
    json!({
        "specversion": "1.0",
        "id": EventId::now(),
        "source": "rannoch",
        "type": ty,
        "subject": subject,
        "data": data,
    })
    .to_string()
}

/// The text a panic carried, when it carried text.
fn panic_text(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(ToString::to_string)
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_default()
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
        let mut failures = lock(&self.failures);
        if failures.newest.len() == MAX_FAILURES {
            failures.newest.pop_front();
            failures.lost += 1;
        }
        failures.newest.push_back(error);
    }

    /// Answers the queries in the inbox, then brings every relay up to the watermark.
    fn pump(&self) {
        #[cfg(test)]
        assert!(
            !self.panic_next.load(std::sync::atomic::Ordering::SeqCst),
            "the pump panics"
        );
        if let Err(error) = self.pump_queries() {
            self.failed(error);
        }
        let mut state = lock(&self.state);
        let State { graph, relays } = &mut *state;
        let mut changes = Vec::new();
        let mut retired = Vec::new();
        for live in relays.values_mut() {
            match self.pump_relay(graph, live) {
                Ok(Some(change)) => changes.push(change),
                Ok(None) => {}
                Err(error) => self.failed(error),
            }
            retired.extend(self.publish(live));
            retired.push(live.warm.set.clone());
        }
        drop(state);
        self.emit(&changes);
        // After the change is told: indexing the new sets and freeing the old ones wait for nobody.
        for set in retired {
            set.prepare();
        }
    }

    fn pump_queries(&self) -> Result<(), Error> {
        let mut answered = false;
        while let Some(event) = self.queue.next(QUERIES)? {
            let asked = shape_query(&event.body);
            let accepted = asked.map(|(id, source, data)| {
                let answer = self.answer(&id, &source, data)?;
                Ok::<_, Error>(self.queue.accept(answer)?)
            });
            match accepted {
                Some(Ok(_)) => {
                    self.queue.done(QUERIES, event.cursor)?;
                    answered = true;
                }
                Some(Err(error)) => {
                    self.queue
                        .failed(QUERIES, event.cursor, &error.to_string())?;
                }
                None => self.queue.done(QUERIES, event.cursor)?,
            }
        }
        // Someone is waiting: the answer does not wait for the next heartbeat.
        if answered {
            self.queue.commit();
        }
        Ok(())
    }

    /// Folds everything past `live`'s cursor as one batch, so one commit makes at most one change.
    fn pump_relay(&self, graph: &Graph, live: &mut Live) -> Result<Option<String>, Error> {
        let worker = worker_name(&live.entry.id);
        while let Some(event) = self.queue.next(&worker)? {
            if event.cursor <= live.warm.set.cursor() {
                self.queue.done(&worker, event.cursor)?;
                continue;
            }
            live.warm.fold(graph, event.cursor, &event.body);
            live.unsaved.push(event.cursor);
            // The file goes before anything is read again, so a failed read or save leaves none
            // holding what left. A relay with no file only reseeds.
            if live.warm.purged {
                remove_files(&self.dir, &live.entry.id)?;
            }
        }
        // Costs nothing with nothing folded, and a relay that failed to read tries again here.
        let change = match live.warm.settle(graph) {
            Ok(data) => data.map(|data| cloud_event(CHANGED, &live.entry.id, &data)),
            Err(error) => {
                live.warm.mark_stale();
                self.failed(error);
                None
            }
        };
        // The set has moved whether or not the file can say so: the change is still told.
        if let Err(error) = self.save_due(&worker, live) {
            self.failed(error);
        }
        Ok(change)
    }

    /// Saves `live`'s file when one is due and closes what it now holds. A stale relay is not
    /// saved: its file would claim a cursor whose truth it has not read.
    fn save_due(&self, worker: &str, live: &mut Live) -> Result<(), Error> {
        let due = live.warm.purged || live.saved_at.is_none_or(|at| at.elapsed() >= SAVE_EVERY);
        if live.unsaved.is_empty() || !due || live.warm.is_stale() {
            return Ok(());
        }
        save(&self.dir, live)?;
        live.warm.purged = false;
        for cursor in live.unsaved.drain(..) {
            self.queue.done(worker, cursor)?;
        }
        Ok(())
    }

    /// Publishes `live`'s set, and returns the one it replaces for the caller to drop.
    fn publish(&self, live: &Live) -> Option<WarmSet> {
        self.published
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(live.entry.id.clone(), live.warm.set.clone())
    }

    fn emit(&self, changes: &[String]) {
        let mut subscribers = lock(&self.subscribers);
        for change in changes {
            for subscriber in subscribers.iter_mut() {
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| subscriber(change))) {
                    self.failed(Error::Subscriber(panic_text(&*panic)));
                }
            }
        }
    }

    /// The `rannoch.answer` for query `id` from `source`: the relay of that shape and view, seeded
    /// now unless one is held already. `source` holds it.
    fn answer(&self, id: &str, source: &str, data: Json) -> Result<String, Error> {
        let asked: Asked = serde_json::from_value(data)?;
        let mut state = lock(&self.state);
        let State { graph, relays } = &mut *state;
        let shared = relays
            .values_mut()
            .find(|live| live.entry.shape == asked.shape && live.entry.view == asked.view);
        let (relay, set) = if let Some(live) = shared {
            let found = (live.entry.id.clone(), live.warm.set.clone());
            if live.entry.holders.insert(source.to_string()) {
                write_manifest(&self.dir, relays)?;
            }
            found
        } else {
            let relay = EventId::now().to_string();
            let shape = shape_of(graph, &asked.shape)?;
            let at = self.queue.join(&worker_name(&relay));
            let seeded = Warm::seed(graph, shape, asked.view.view(), at).and_then(|warm| {
                let entry = Entry {
                    id: relay.clone(),
                    shape: asked.shape,
                    view: asked.view,
                    holders: BTreeSet::from([source.to_string()]),
                };
                let mut live = Live::new(entry, warm);
                save(&self.dir, &mut live)?;
                Ok(live)
            });
            let live = match seeded {
                Ok(live) => live,
                Err(error) => {
                    // The cause is what the asker hears; a cleanup that also fails is told apart.
                    if let Err(cleanup) = self.queue.remove_worker(&worker_name(&relay)) {
                        self.failed(cleanup.into());
                    }
                    return Err(error);
                }
            };
            let set = live.warm.set.clone();
            set.prepare();
            let _ = self.publish(&live);
            relays.insert(relay.clone(), live);
            write_manifest(&self.dir, relays)?;
            (relay, set)
        };
        Ok(cloud_event(
            ANSWER,
            id,
            &json!({
                "result": {"relay": relay, "cursor": set.cursor(), "steps": set.counts()},
                "known": set.known(),
            }),
        ))
    }
}

/// The id, source and data of `body` when it is a `rannoch.query` event whose data names a shape,
/// from one parse of it.
fn shape_query(body: &str) -> Option<(String, String, Json)> {
    if !body.contains(QUERY) {
        return None;
    }
    let event: Read<Option<Json>> = read_event(body).ok()?;
    let (id, source) = (event.attributes.id.clone(), event.attributes.source.clone());
    let data = event.of_type(QUERY).ok()??;
    data.get("shape").is_some().then_some((id, source, data))
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
/// then every event but its declarations and what fill appended, which filling appends again.
fn replay(source: &Graph, into: &mut Graph, until: Cursor) -> Result<(), Error> {
    let ontology = source.current_ontology()?;
    let declared = |name: &str| {
        json!({
            "specversion": "1.0", "id": format!("declared-{name}"),
            "source": "rannoch", "type": format!("{DECLARED_PREFIX}{name}"),
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
                    !event.ty.starts_with(DECLARED_PREFIX)
                        && !event.ty.starts_with(CLOSED_PREFIX)
                        && event.ty != REFILLED
                })
            })
            .collect();
        // A body a rule refuses is one that rule took out of the log it came from.
        for landed in into.commit(&bodies, &[])? {
            match landed {
                Ok(_) | Err(rannoch_store::Error::Purged { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        into.fill()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use rannoch_store::{Class, NodeRef, Roots, Value};

    use super::*;

    const KEY: [u8; 32] = [4; 32];
    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    /// Relays on a graph with one node of an indexed class and a shape rooted on it, and the id of
    /// the relay a query through the inbox made.
    fn relay(dir: &tempfile::TempDir) -> Result<(Arc<Queue>, Relays, String)> {
        let path = dir.path().join("graph");
        let mut g = open(&path, &KEY)?;
        let declared = json!({"specversion": "1.0", "id": "d", "source": "t",
                              "type": "rannoch.declared.x"})
        .to_string();
        let mut class = Class::new("a", ["n"]);
        class.indexed = BTreeSet::from(["n".to_string()]);
        g.declare(&Declaration::Class(class), &declared)?;
        let shape = Shape {
            name: "s".into(),
            roots: Roots::Node {
                node: NodeRef::new("a", "k"),
            },
            steps: Vec::new(),
        };
        g.declare(&Declaration::Shape(shape), &declared)?;
        let body = node("v");
        for landed in g.commit(&[&body], &[])? {
            landed?;
        }
        g.fill()?;
        let queue = Arc::new(Queue::open(g, Duration::from_secs(3_600))?);
        let relays = Relays::open(dir.path().join("relays"), Arc::clone(&queue), &path, &KEY)?;
        queue.accept(
            json!({"specversion": "1.0", "id": EventId::now().to_string(), "source": "app",
                   "type": "rannoch.query", "data": {"shape": "s"}})
            .to_string(),
        )?;
        let _ = queue.flush();
        let id = until(|| lock(&relays.inner.state).relays.keys().next().cloned())?;
        Ok((queue, relays, id))
    }

    fn node(value: &str) -> String {
        json!({"specversion": "1.0", "id": format!("{}:0", EventId::now()),
            "source": "t", "type": "rannoch.node.a", "subject": "k",
            "data": {"properties": {"n": value}}})
        .to_string()
    }

    /// Waits for `found`, and fails only when the test is hung.
    fn until<T>(mut found: impl FnMut() -> Option<T>) -> Result<T> {
        let hung = Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(value) = found() {
                return Ok(value);
            }
            if Instant::now() > hung {
                return Err("hung".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn read_makes_no_graph_read() -> Result {
        let dir = tempfile::tempdir()?;
        let (_queue, relays, id) = relay(&dir)?;
        let reads = |relays: &Relays| {
            let state = lock(&relays.inner.state);
            let warm = &state.relays[&id].warm;
            (warm.reads, warm.walked)
        };
        let before = reads(&relays);
        for _ in 0..1_000 {
            let set = relays.read(&id).ok_or("the relay")?;
            assert_eq!(set.by("n", &Value::from("v")).len(), 1);
        }
        assert_eq!(reads(&relays), before);
        Ok(())
    }

    #[test]
    fn take_failures_drains_what_failed_while_the_relays_run() -> Result {
        let dir = tempfile::tempdir()?;
        let (queue, relays, id) = relay(&dir)?;
        relays.subscribe(|_| panic!("the subscriber panics"));
        queue.accept(node("w"))?;
        let _ = queue.flush();
        let taken = until(|| {
            let taken = relays.take_failures();
            (!taken.is_empty()).then_some(taken)
        })?;
        assert!(
            matches!(&taken[..], [Error::Subscriber(text)] if text == "the subscriber panics"),
            "{taken:?}"
        );
        assert!(relays.take_failures().is_empty(), "taken once");
        assert_eq!(
            relays
                .read(&id)
                .ok_or("the relay")?
                .by("n", &Value::from("w"))
                .len(),
            1,
            "the relays run on"
        );
        Ok(())
    }

    #[test]
    fn failures_waiting_are_bounded_and_say_how_many_went() -> Result {
        let dir = tempfile::tempdir()?;
        let (_queue, relays, _) = relay(&dir)?;
        for i in 0..MAX_FAILURES + 10 {
            relays.inner.failed(Error::Refused(i.to_string()));
        }
        let taken = relays.take_failures();
        assert_eq!(taken.len(), MAX_FAILURES + 1);
        assert!(matches!(taken[0], Error::Lost(10)), "{:?}", taken[0]);
        assert_eq!(taken[1].to_string(), "10", "the oldest kept");
        assert_eq!(
            taken[MAX_FAILURES].to_string(),
            (MAX_FAILURES + 9).to_string()
        );
        assert!(relays.take_failures().is_empty());
        Ok(())
    }

    #[test]
    fn a_panic_of_the_relays_thread_surfaces_from_close() -> Result {
        let dir = tempfile::tempdir()?;
        let (_queue, relays, _) = relay(&dir)?;
        relays.inner.panic_next.store(true, Ordering::SeqCst);
        notify(&Arc::downgrade(&relays.inner));
        let failures = relays.close();
        assert!(
            matches!(&failures[..], [Error::Thread(text)] if text == "the pump panics"),
            "{failures:?}"
        );
        Ok(())
    }

    /// Swaps the graph the relays read for an empty one, so each read of the shape fails, and
    /// returns the real one. A relay's file comes due at once.
    fn break_reads(dir: &tempfile::TempDir, relays: &Relays, id: &str) -> Result<Graph> {
        let empty = open(dir.path().join("empty"), &KEY)?;
        let mut state = lock(&relays.inner.state);
        if let Some(live) = state.relays.get_mut(id) {
            live.saved_at = None;
        }
        Ok(std::mem::replace(&mut state.graph, empty))
    }

    fn file_cursor(relays: &Relays, id: &str) -> Result<Cursor> {
        let bytes = fs::read(relay_file(&relays.inner.dir, id))?;
        Ok(serde_json::from_slice::<Saved>(&bytes)?.cursor)
    }

    fn holds(relays: &Relays, id: &str, value: &str) -> bool {
        relays
            .read(id)
            .is_some_and(|set| set.by("n", &Value::from(value)).len() == 1)
    }

    fn failures_of(relays: &Relays) -> Result<Vec<Error>> {
        until(|| {
            let taken = relays.take_failures();
            (!taken.is_empty()).then_some(taken)
        })
    }

    #[test]
    fn a_failed_settle_reseeds_with_no_new_event() -> Result {
        let dir = tempfile::tempdir()?;
        let (queue, relays, id) = relay(&dir)?;
        let real = break_reads(&dir, &relays, &id)?;
        queue.accept(node("w"))?;
        let _ = queue.flush();
        failures_of(&relays)?;
        assert!(!holds(&relays, &id, "w"), "nothing could be read");
        let committed = open(dir.path().join("graph"), &KEY)?.events().committed()?;
        // Taking the state waits out the pump that failed.
        let state = lock(&relays.inner.state);
        let claimed = relays.read(&id).ok_or("the relay")?.cursor();
        assert!(claimed < committed, "it claims no cursor it has not read");
        drop(state);
        lock(&relays.inner.state).graph = real;
        // Nothing more commits: only the relay's own wake can bring it back.
        until(|| holds(&relays, &id, "w").then_some(()))?;
        Ok(())
    }

    #[test]
    fn a_stale_relay_saves_nothing_and_closes_nothing_until_it_reseeds() -> Result {
        let dir = tempfile::tempdir()?;
        let (queue, relays, id) = relay(&dir)?;
        let real = break_reads(&dir, &relays, &id)?;
        queue.accept(node("w"))?;
        let _ = queue.flush();
        failures_of(&relays)?;
        let worker = worker_name(&id);
        {
            // Taking the state waits out the pump that failed, and any save it would make.
            let mut state = lock(&relays.inner.state);
            let committed = open(dir.path().join("graph"), &KEY)?.events().committed()?;
            assert!(
                file_cursor(&relays, &id)? < committed,
                "the file does not take a cursor it does not hold the truth of"
            );
            assert!(queue.lag(&worker)? > 0, "its events stay open");
            state.graph = real;
        }
        until(|| {
            let set = relays.read(&id)?;
            (holds(&relays, &id, "w")
                && file_cursor(&relays, &id).is_ok_and(|saved| saved == set.cursor())
                && matches!(queue.lag(&worker), Ok(0)))
            .then_some(())
        })?;
        Ok(())
    }

    #[test]
    fn a_failed_save_still_tells_the_change_and_saves_later() -> Result {
        let dir = tempfile::tempdir()?;
        let (queue, relays, id) = relay(&dir)?;
        let heard = Arc::new(Mutex::new(Vec::new()));
        let hearing = Arc::clone(&heard);
        relays.subscribe(move |change: &str| lock(&hearing).push(change.to_string()));
        // A directory where the save writes its temporary file: the save fails, and only the save.
        let blocker = relay_file(&relays.inner.dir, &id).with_extension("tmp");
        fs::create_dir(&blocker)?;
        lock(&relays.inner.state)
            .relays
            .get_mut(&id)
            .ok_or("the relay")?
            .saved_at = None;
        queue.accept(node("w"))?;
        let _ = queue.flush();
        until(|| (!lock(&heard).is_empty()).then_some(()))?;
        assert_eq!(lock(&heard).len(), 1, "the change is told once");
        assert!(lock(&heard)[0].contains("\"after\":\"w\""), "{heard:?}");
        let failures = failures_of(&relays)?;
        assert!(matches!(failures[0], Error::Io(_)), "{failures:?}");

        let worker = worker_name(&id);
        assert!(queue.lag(&worker)? > 0, "not closed while unsaved");
        fs::remove_dir(&blocker)?;
        until(|| {
            let set = relays.read(&id)?;
            (file_cursor(&relays, &id).is_ok_and(|saved| saved == set.cursor())
                && matches!(queue.lag(&worker), Ok(0)))
            .then_some(())
        })?;
        assert_eq!(lock(&heard).len(), 1, "the retry tells nothing again");
        Ok(())
    }

    #[test]
    fn a_replay_does_not_stop_on_a_rules_refusal() -> Result {
        let dir = tempfile::tempdir()?;
        let (queue, relays, _) = relay(&dir)?;
        queue.accept(node("gone"))?;
        queue.accept(
            json!({"specversion": "1.0", "id": format!("{}:0", EventId::now()),
            "source": "rannoch:s", "type": "rannoch.node.a", "subject": "kept",
            "data": {"properties": {"n": "1"}}})
            .to_string(),
        )?;
        let _ = queue.flush();
        drop(relays);
        let source = open(dir.path().join("graph"), &KEY)?;
        let mut into = open(dir.path().join("into"), &KEY)?;
        // The rule stands in the store replayed into, so its match is refused there mid-replay.
        let until = source.events().committed()?;
        into.purge(
            &rannoch_store::Target::Stream {
                source: "t".to_string(),
            },
            "r",
            "app",
        )?;
        replay(&source, &mut into, until)?;
        assert!(
            into.find("a", "kept")?.is_some(),
            "the replay went on past it"
        );
        Ok(())
    }

    #[test]
    fn a_query_with_no_shape_is_left_to_graph_answer() {
        let reads = json!({"specversion": "1.0", "id": "q", "source": "a", "type": "rannoch.query",
                           "data": {"counts": {}}})
        .to_string();
        assert!(shape_query(&reads).is_none());
        let other = json!({"specversion": "1.0", "id": "q", "source": "a",
                           "type": "app.rannoch.query", "data": {"shape": "s"}})
        .to_string();
        assert!(shape_query(&other).is_none());
        let asked = json!({"specversion": "1.0", "id": "q", "source": "a", "type": "rannoch.query",
                           "data": {"shape": "s"}})
        .to_string();
        assert!(shape_query(&asked).is_some());
    }
}
