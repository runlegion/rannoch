//! The README's "What you can rely on" and "How it fails", one scenario for each item, through
//! the real crates: the store, rannoch and its inboxes, `Events` or `GraphAppend`, the queue, and
//! rannoch-processor running the flow documents of `tests/readme/flows.json`, checked against
//! `tests/readme/signatures.json` as an app's build checks them. Every action is a step crate's:
//! `AgentAction` over the app's agents triage, hold and page, and `Retrieve` over its directory.
//! A crash is driven by putting `_events` in the state a crash leaves.
//!
//! Each scenario is named for its item. Three items are driven in another scenario too:
//! `GraphAppend` writing a batch whole or not at all, in the batch that can never be written; the
//! same event's rows written again, in the second app's; and a resend's rows, in a crash's resend.
//!
//! No event sent through rannoch makes `Events` fail a hand-over, since each id is new at accept
//! and every `Value` serializes. So its scenario gives the store a trigger that refuses one event,
//! then holds the store's write lock from another connection, as another writer would.
//!
//! Not driven in one process, and why:
//! - A process that dies before its batch commits loses it: a scenario cannot kill its own
//!   process. The first scenario shows the events are not in `_events` until then, which is what
//!   a death would lose.
//! - A heap with "no size limit", a failed hand-over tried again "with no limit", and a kept
//!   signal sender making `join` wait "for good": a scenario sees the heap grow, and the tries
//!   and the wait go on, for as long as it watches.
//! - A second process: driven as a second app over the same file in this process. Rannoch takes
//!   no lock on the file and keeps nothing per process, and SQLite locks per connection, so a
//!   second app does what a second process does.
//!
//! Left to its open issue: a run still going `window` after its 1, resent while it runs (#85).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use chrono::{DateTime, TimeDelta, Utc};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Error, Event, InboxConfig, Rannoch, Status, Storage};
use rannoch_events::Events;
use rannoch_graph::{GraphAppend, State, history, prop, why};
use rannoch_processor::{Action, FlowDoc, Processor, Sig, check};
use rannoch_processor_agent::{Agent, AgentAction};
use rannoch_processor_retrieve::{Retrieve, Source};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use uuid::Uuid;

type Obj = Map<String, Value>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const FLOWS: &str = include_str!("readme/flows.json");
const SIGS: &str = include_str!("readme/signatures.json");

/// How long a scenario waits for what it expects.
const WAIT: Duration = Duration::from_secs(10);

fn lock<X>(mutex: &Mutex<X>) -> MutexGuard<'_, X> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn obj(v: &Value) -> Obj {
    v.as_object().cloned().unwrap_or_default()
}

/// The app's triage agent: rates a ticket's text and gives its severity as a graph row. A text
/// with "crash" rates 3, with "slow" 2, any other 1; "resolved" retracts the severity, and
/// "garbled" gives an item that is not an op. Empty text fails, and so does the first text with
/// "flaky" it is asked about.
#[derive(Default)]
struct Triage(AtomicBool);

impl Agent for Triage {
    fn ask(&self, input: &Obj) -> std::result::Result<Obj, String> {
        let text = input
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let node = input.get("ticket").cloned().unwrap_or_default();
        if text.is_empty() {
            return Err("triage: nothing to read".to_owned());
        }
        if text.contains("flaky") && !self.0.swap(true, Ordering::SeqCst) {
            return Err("triage: no answer".to_owned());
        }
        let severity = match text {
            "resolved" => 0,
            t if t.contains("crash") => 3,
            t if t.contains("slow") => 2,
            _ => 1,
        };
        let row = match text {
            "resolved" => json!({ "retract": { "node": node, "key": "severity" } }),
            "garbled" => json!({ "rate": { "node": node, "severity": severity } }),
            _ => json!({ "set": { "node": node, "key": "severity", "value": severity } }),
        };
        Ok(obj(&json!({ "severity": severity, "$graph": [row] })))
    }
}

/// The app's directory, which knows ada; it keeps every user it was asked for.
#[derive(Default)]
struct Directory(Mutex<Vec<String>>);

impl Source for Directory {
    fn get(&self, input: &Obj) -> std::result::Result<Obj, String> {
        let user = input
            .get("user")
            .and_then(Value::as_str)
            .unwrap_or_default();
        lock(&self.0).push(user.to_owned());
        match user {
            "ada" => Ok(obj(&json!({ "email": "ada@example.com" }))),
            _ => Err(format!("lookup: no user {user}")),
        }
    }
}

/// The app's hold and page agents: each says it started, then answers once released.
struct Gate {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Agent for Gate {
    fn ask(&self, _: &Obj) -> std::result::Result<Obj, String> {
        let _ = self.started.send(());
        let released = lock(&self.release).recv_timeout(WAIT);
        released.map(|()| Obj::new()).map_err(|e| e.to_string())
    }
}

/// What a scenario holds of a gate: word that its agent started, and its release.
struct Held {
    started: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}

fn gate() -> (Held, Arc<Gate>) {
    let ((says, started), (release, waits)) = (mpsc::channel(), mpsc::channel());
    let gate = Gate {
        started: says,
        release: Mutex::new(waits),
    };
    (Held { started, release }, Arc::new(gate))
}

/// The app's flows, checked against its signatures as its build checks them.
fn flows(sigs: &[Sig]) -> Result<String> {
    let docs: Vec<FlowDoc> = serde_json::from_str(FLOWS)?;
    let docs: Vec<_> = docs
        .into_iter()
        .map(|d| (format!("{}.json", d.name), d))
        .collect();
    Ok(check(&docs, sigs).map_err(|e| e.join("\n"))?)
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// A fresh store path for the scenario `name`.
fn database(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("readme")
        .join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir.join("store.db"))
}

/// A fresh store for the scenario `name`, with its migrations applied, for a crash to seed.
fn migrated(name: &str) -> Result<PathBuf> {
    let path = database(name)?;
    let store = Store::new(path.clone(), rannoch_core::migrator());
    if store.pending()? {
        store.apply()?;
    }
    Ok(path)
}

/// A v7 id `secs` seconds ago.
fn ago(secs: u64) -> Uuid {
    let now = u64::try_from(Utc::now().timestamp()).unwrap_or_default();
    Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        now.saturating_sub(secs),
        0,
    ))
}

/// When a row was accepted: its v7 id's time.
fn accepted(id: Uuid) -> DateTime<Utc> {
    let (secs, nanos) = id.get_timestamp().map_or((0, 0), |t| t.to_unix());
    let secs = i64::try_from(secs).unwrap_or_default();
    DateTime::from_timestamp(secs, nanos).unwrap_or_default()
}

/// Writes one `_events` row through the app's forgr factory, as a crash leaves it.
fn put(conn: &Connection, id: Uuid, source: Option<Uuid>, data: &Value, status: i64) -> Result<()> {
    let row = _events::factory()
        .id(id.to_string())
        .ty(10)
        .data(data.to_string())
        .status(status);
    match source {
        Some(s) => row.source(s.to_string()),
        None => row,
    }
    .insert(conn)?;
    Ok(())
}

/// A ticket two hours old whose 1 is `secs` old with nothing after it, as a crash leaves a run;
/// the ticket's id.
fn stall(conn: &Connection, data: &Value, secs: u64) -> Result<Uuid> {
    let id = ago(7200);
    put(conn, id, None, data, 0)?;
    put(conn, ago(secs), Some(id), &json!("taken"), 1)?;
    Ok(id)
}

/// Polls `found` every 5 ms for up to `limit`: its first `Some`.
fn poll<T>(limit: Duration, found: impl Fn() -> Option<T>) -> Option<T> {
    let start = Instant::now();
    loop {
        if let Some(t) = found() {
            return Some(t);
        }
        if start.elapsed() > limit {
            return None;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn until(limit: Duration, done: impl Fn() -> bool) -> bool {
    poll(limit, || done().then_some(())).is_some()
}

fn settings(attempts: u32) -> Settings {
    Settings {
        attempts,
        ..Settings::new(Duration::from_millis(100))
    }
}

/// A rated event's data: its flow has no steps.
fn rated(ticket: &str) -> Value {
    json!({ "ticket": ticket, "severity": 1 })
}

/// The error lines rannoch logs, for the scenarios that read them.
struct Errors(Mutex<Vec<String>>);

impl log::Log for Errors {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Error
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            lock(&self.0).push(record.args().to_string());
        }
    }
    fn flush(&self) {}
}

static ERRORS: Errors = Errors(Mutex::new(Vec::new()));

/// One scenario that reads the log at a time: every app in this binary logs to `ERRORS`.
static LOGGED: Mutex<()> = Mutex::new(());

/// Holds `LOGGED`, with `ERRORS` taking rannoch's `error` lines, emptied.
fn logged() -> MutexGuard<'static, ()> {
    let one = lock(&LOGGED);
    let _ = log::set_logger(&ERRORS);
    log::set_max_level(log::LevelFilter::Error);
    lock(&ERRORS.0).clear();
    one
}

/// The failed hand-overs of the default inbox logged so far: how many events each held.
fn failed_hand_overs() -> Vec<usize> {
    let held = |line: &String| {
        let rest = line.strip_prefix("inbox 0 could not hand over ")?;
        rest.split(' ').next()?.parse().ok()
    };
    lock(&ERRORS.0).iter().filter_map(held).collect()
}

/// Which append the app hands rannoch.
#[derive(Clone, Copy)]
enum Kind {
    Events,
    Graph,
    Neither,
}

/// One row about an event: id, ty, status, data, `run_at`.
type About = (Uuid, i64, i64, Value, Option<String>);

/// The app, started in the README's order: the store, the processor over its flows and actions,
/// the signal channel, the append, rannoch, the queue. It keeps a signal sender of its own.
struct App {
    read: Connection,
    signal: mpsc::Sender<()>,
    rannoch: Rannoch<u16, Value>,
    queue: Queue,
    directory: Arc<Directory>,
    hold: Held,
    page: Held,
}

impl App {
    /// The app over the store at `path`, with `config`'s inboxes and `kind`'s append.
    fn open(
        path: &Path,
        kind: Kind,
        config: Config<u16, Value>,
        settings: Settings,
    ) -> Result<Self> {
        let store = Arc::new(Store::new(path.to_owned(), rannoch_core::migrator()));
        if store.pending()? {
            store.apply()?;
        }
        let sigs: Vec<Sig> = serde_json::from_str(SIGS)?;
        let sig = |name: &str| {
            let found = sigs.iter().find(|s| s.action == name).cloned();
            found.ok_or_else(|| format!("no sig {name}"))
        };
        let (directory, (hold, holding), (page, paging)) =
            (Arc::new(Directory::default()), gate(), gate());
        let actions: Vec<Arc<dyn Action>> = vec![
            Arc::new(AgentAction::new(
                sig("triage")?,
                Arc::new(Triage::default()),
            )),
            Arc::new(Retrieve::new(sig("lookup")?, directory.clone())),
            Arc::new(AgentAction::new(sig("hold")?, holding)),
            Arc::new(AgentAction::new(sig("page")?, paging)),
        ];
        let processor = Processor::<u16, Value>::new(&flows(&sigs)?, actions, ready(path)?)?;
        let (signal, wake) = mpsc::channel();
        let append: Option<Arc<dyn Append<u16, Value>>> = match kind {
            Kind::Events => Some(Arc::new(Events::new(ready(path)?, signal.clone()))),
            Kind::Graph => Some(Arc::new(GraphAppend::new(
                ready(path)?,
                signal.clone(),
                processor.writes(),
            ))),
            Kind::Neither => None,
        };
        let rannoch = Rannoch::open(Config {
            store: Some(store),
            append,
            ..config
        })?;
        let queue = Queue::start(
            ready(path)?,
            wake,
            rannoch.sender(),
            Arc::new(processor),
            settings,
        );
        Ok(Self {
            read: ready(path)?,
            signal,
            rannoch,
            queue,
            directory,
            hold,
            page,
        })
    }

    fn send(&self, ty: u16, data: &Value) -> Result<()> {
        self.send_at(ty, data, None)
    }

    /// Sends an event of `ty` with `data` through rannoch, due at `run_at`.
    fn send_at(&self, ty: u16, data: &Value, run_at: Option<DateTime<Utc>>) -> Result<()> {
        let event = Event {
            id: Uuid::nil(),
            source: None,
            ty,
            data: data.clone(),
            run_at,
            status: Status::Added,
        };
        self.rannoch.sender().send(event)?;
        Ok(())
    }

    /// The id of the event without a source whose data is `data`, once it is on disk.
    fn root(&self, data: &Value) -> Option<Uuid> {
        let sql = "SELECT id FROM _events WHERE source IS NULL AND data = ?1";
        let id = self
            .read
            .query_row(sql, [data.to_string()], |r| r.get::<_, String>(0));
        id.ok()?.parse().ok()
    }

    /// The rows whose source is `id`, in order.
    fn about(&self, id: Uuid) -> Vec<About> {
        let sql = "SELECT id, ty, status, data, run_at FROM _events WHERE source = ?1 ORDER BY id";
        let rows = self.read.prepare(sql).and_then(|mut select| {
            select
                .query_map([id.to_string()], |r| {
                    let (id, data): (String, String) = (r.get(0)?, r.get(3)?);
                    let data = serde_json::from_str(&data).unwrap_or(Value::Null);
                    let id = id.parse().unwrap_or_default();
                    Ok((id, r.get(1)?, r.get(2)?, data, r.get(4)?))
                })?
                .collect()
        });
        rows.unwrap_or_default()
    }

    fn statuses(&self, id: Uuid) -> Vec<i64> {
        self.about(id).iter().map(|r| r.2).collect()
    }

    /// The (ty, data) of the rows at `status` whose source is `id`.
    fn sent(&self, id: Uuid, status: i64) -> Vec<(i64, Value)> {
        let rows = self.about(id).into_iter().filter(|r| r.2 == status);
        rows.map(|r| (r.1, r.3)).collect()
    }

    /// Waits for the 0 sent about `event`, a retry or a resend, and returns its id.
    fn retry(&self, event: Uuid) -> Uuid {
        let found = poll(WAIT, || self.about(event).into_iter().find(|r| r.2 == 0));
        assert!(found.is_some(), "a 0 about {event}");
        found.map_or_default(|r| r.0)
    }

    /// Waits for the event sent with `data` to have its 2, and returns its id.
    fn ran(&self, data: &Value) -> Uuid {
        let id = poll(WAIT, || self.root(data)).unwrap_or_default();
        assert!(until(WAIT, || self.statuses(id).contains(&2)), "{data} ran");
        id
    }

    fn count(&self, sql: &str) -> i64 {
        self.read.query_row(sql, [], |r| r.get(0)).unwrap_or(-1)
    }

    /// Every `_events` row as JSON text, in the order the rows were inserted.
    fn rows(&self) -> Vec<String> {
        let sql = "SELECT json_array(rowid, id, source, ty, data, run_at, status) FROM _events
            ORDER BY rowid";
        let rows = self
            .read
            .prepare(sql)
            .and_then(|mut select| select.query_map([], |r| r.get(0))?.collect());
        rows.unwrap_or_default()
    }

    fn stop(self) {
        drop(self.signal);
        self.rannoch.shutdown();
        self.queue.join();
    }
}

const ROOTS: &str = "SELECT count(*) FROM _events WHERE source IS NULL";

#[test]
fn an_accepted_event_is_in_memory_until_its_inbox_hands_it_over() {
    // Given an app whose default inbox hands over every 400 ms; under Config::default, every 20
    assert_eq!(
        Config::<u16, Value>::default().default_interval,
        TimeDelta::milliseconds(20)
    );
    let config = Config {
        default_interval: TimeDelta::milliseconds(400),
        ..Config::default()
    };
    let path = database("in_memory").unwrap();
    let app = App::open(&path, Kind::Events, config, settings(1)).unwrap();
    let landed = || app.count(ROOTS);

    // When it sends three events
    for ticket in ["m1", "m2", "m3"] {
        app.send(11, &rated(ticket)).unwrap();
    }

    // Then none is in _events before the inbox's interval has passed
    thread::sleep(Duration::from_millis(150));
    assert_eq!(landed(), 0, "on the heap, not on disk");

    // And the inbox hands its whole heap over as one batch: the first read to see one sees three
    assert_eq!(poll(WAIT, || Some(landed()).filter(|n| *n > 0)), Some(3));

    // And each was given a v7 id at accept, and written at 0
    let ids = ["m1", "m2", "m3"].map(|t| app.root(&rated(t)).unwrap_or_default());
    assert!(ids.iter().all(|id| id.get_version_num() == 7), "{ids:?}");
    let at_0 = "SELECT count(*) FROM _events WHERE source IS NULL AND status = 0";
    assert_eq!(app.count(at_0), 3);

    // When it sends two more just after that hand-over
    for ticket in ["m4", "m5"] {
        app.send(11, &rated(ticket)).unwrap();
    }

    // Then they wait out the inbox's interval after it too, and are handed over together
    thread::sleep(Duration::from_millis(150));
    assert_eq!(landed(), 3);
    assert_eq!(poll(WAIT, || Some(landed()).filter(|n| *n > 3)), Some(5));
    app.stop();

    // Given an app with no append
    let path = database("no_append").unwrap();
    let app = App::open(&path, Kind::Neither, Config::default(), settings(1)).unwrap();

    // When it sends an event, and its queue is woken
    app.send(11, &rated("kept")).unwrap();
    thread::sleep(Duration::from_millis(200));
    app.signal.send(()).unwrap();

    // Then nothing is handed over, before shutdown or after it
    thread::sleep(Duration::from_millis(100));
    assert_eq!(app.count("SELECT count(*) FROM _events"), 0);
    let read = ready(&path).unwrap();
    app.stop();
    let all = read.query_row("SELECT count(*) FROM _events", [], |r| r.get::<_, i64>(0));
    assert_eq!(all.unwrap(), 0);
}

/// Refuses to insert an event about the ticket "refused".
const REFUSE: &str = "CREATE TRIGGER refuse BEFORE INSERT ON _events
    WHEN json_extract(NEW.data, '$.ticket') = 'refused'
    BEGIN SELECT RAISE(ABORT, 'refused'); END";

#[test]
fn events_writes_a_batch_whole_or_not_at_all_and_tries_it_again_ahead_of_later_events() {
    let _errors = logged();
    // Given an app with Events, whose default inbox hands over every 300 ms
    let config = Config {
        default_interval: TimeDelta::milliseconds(300),
        ..Config::default()
    };
    let path = database("whole_or_not").unwrap();
    let app = App::open(&path, Kind::Events, config, settings(1)).unwrap();

    // And a store that refuses to insert one of the events of a batch
    let store = ready(&path).unwrap();
    store.execute_batch(REFUSE).unwrap();

    // When it sends three events in one batch, the second refused
    for ticket in ["a", "refused", "b"] {
        app.send(11, &rated(ticket)).unwrap();
    }

    // Then the hand-over fails whole and is logged at error: none of the three is written
    assert!(until(WAIT, || !failed_hand_overs().is_empty()));
    assert_eq!(failed_hand_overs()[0], 3);
    assert_eq!(app.count(ROOTS), 0);

    // When it sends a fourth
    app.send(11, &rated("c")).unwrap();

    // Then the failed batch goes back on the heap, and the inbox tries it and the fourth at its
    // next interval
    assert!(until(WAIT, || failed_hand_overs().last() == Some(&4)));
    assert_eq!(app.count(ROOTS), 0);

    // When the store takes them again
    store.execute_batch("DROP TRIGGER refuse").unwrap();

    // Then the four are written in one hand-over
    let landed = poll(WAIT, || Some(app.count(ROOTS)).filter(|n| *n > 0));
    assert_eq!(landed, Some(4));

    // And as they run, the rows already in _events are only added to
    let written = app.rows();
    for ticket in ["a", "refused", "b", "c"] {
        app.ran(&rated(ticket));
    }
    let rows = app.rows();
    assert!(rows.len() > written.len() && rows.starts_with(&written));

    // When another connection holds the store's write lock past the append's busy timeout of
    // 5 s, and an event is sent while the hand-over of the one before it waits on the lock
    store.execute_batch("BEGIN IMMEDIATE").unwrap();
    let tried = failed_hand_overs().len();
    app.send(11, &rated("d")).unwrap();
    thread::sleep(Duration::from_millis(600));
    app.send(11, &rated("e")).unwrap();

    // Then that hand-over fails holding the one before it alone
    assert!(until(WAIT, || failed_hand_overs().len() > tried));
    assert_eq!(failed_hand_overs()[tried], 1);

    // And once the lock is released, the failed batch is written ahead of what arrived since
    store.execute_batch("ROLLBACK").unwrap();
    app.ran(&rated("d"));
    app.ran(&rated("e"));
    let sql = "SELECT json_extract(data, '$.ticket') FROM _events WHERE source IS NULL
        ORDER BY rowid";
    let order: Vec<String> = store
        .prepare(sql)
        .and_then(|mut s| s.query_map([], |r| r.get(0))?.collect())
        .unwrap();
    assert_eq!(order, ["a", "refused", "b", "c", "d", "e"]);
    app.stop();
}

#[test]
fn the_queue_takes_each_due_event_once_and_a_crash_before_its_1_leaves_it_due() {
    // Given an app whose default inbox hands over every 300 ms, so each 1 is on the heap a while
    let config = Config {
        default_interval: TimeDelta::milliseconds(300),
        ..Config::default()
    };
    let path = database("taken_once").unwrap();
    let app = App::open(&path, Kind::Events, config, settings(1)).unwrap();

    // When it sends ten events, and the queue is woken every 10 ms until each has its 2
    let events: Vec<Value> = (0..10).map(|i| rated(&format!("once{i}"))).collect();
    for data in &events {
        app.send(11, data).unwrap();
    }
    let ids: Vec<Uuid> = events
        .iter()
        .filter_map(|d| poll(WAIT, || app.root(d)))
        .collect();
    assert_eq!(ids.len(), 10);
    let start = Instant::now();
    while !ids.iter().all(|id| app.statuses(*id).contains(&2)) {
        app.signal.send(()).unwrap();
        thread::sleep(Duration::from_millis(10));
        assert!(start.elapsed() < WAIT, "each ran");
    }

    // Then each was taken once: one 1, then its 2
    for id in &ids {
        assert_eq!(app.statuses(*id), [1, 2]);
    }
    app.stop();

    // Given the store as a crash leaves it when an event's 1 was not yet on disk: the event at
    // 0, nothing about it
    let crashed = ago(60);
    put(
        &ready(&path).unwrap(),
        crashed,
        None,
        &json!({ "ticket": "c", "text": "x" }),
        0,
    )
    .unwrap();

    // When the app starts again, and its queue wakes
    let app = App::open(&path, Kind::Events, Config::default(), settings(1)).unwrap();
    app.signal.send(()).unwrap();

    // Then the event is still due, and is taken under its own id: its 1's source is the event,
    // its flow runs, and nothing resends it
    assert!(until(WAIT, || app.statuses(crashed).contains(&2)));
    let rows: Vec<(i64, Value)> = app.about(crashed).into_iter().map(|r| (r.2, r.3)).collect();
    let next = json!({ "ticket": "c", "severity": 1 });
    assert_eq!(rows, [(1, json!("taken")), (0, next), (2, json!("done"))]);
    let hung = "SELECT count(*) FROM _events WHERE data = '\"hung\"'";
    assert_eq!(app.count(hung), 0);
    app.stop();
}

#[test]
fn the_hunt_resends_an_event_whose_1_has_had_nothing_after_it_for_window() {
    // Given the queue's defaults: a 20-minute window and an hour between hunts
    let defaults = Settings::new(Duration::from_millis(100));
    assert_eq!(
        (defaults.window, defaults.hunt),
        (Duration::from_mins(20), Duration::from_hours(1))
    );

    // And a store as a crash leaves it: a ticket whose 1 is an hour old with nothing after it,
    // and one whose 1 is a second old
    let path = migrated("hunt").unwrap();
    let seed = ready(&path).unwrap();
    let ticket = |t: &str| json!({ "ticket": t, "text": "it crashed" });
    let stalled = stall(&seed, &ticket("h1"), 3600).unwrap();
    let young = stall(&seed, &ticket("h3"), 1).unwrap();

    // When the app starts with a window of a minute and a hunt every second
    let settings = Settings {
        window: Duration::from_mins(1),
        hunt: Duration::from_secs(1),
        ..settings(1)
    };
    let started = Instant::now();
    let app = App::open(&path, Kind::Events, Config::default(), settings).unwrap();

    // Then nothing is resent before the first hunt, one hunt after the start, though the queue
    // is woken before it
    thread::sleep(Duration::from_millis(100));
    app.signal.send(()).unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_eq!(app.sent(stalled, 0), []);

    // And the hunt resends the stalled ticket: a new event at 0 whose source is the ticket and
    // whose data is "hung"
    app.retry(stalled);
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(app.sent(stalled, 0), [(10, json!("hung"))]);

    // When another ticket is left stalled after that hunt, and the queue is woken
    let later = stall(&seed, &ticket("h2"), 3600).unwrap();
    app.signal.send(()).unwrap();

    // Then the wake does not hunt: the ticket is resent at the next hunt, a hunt after the first
    thread::sleep(Duration::from_millis(300));
    assert_eq!(app.sent(later, 0), []);
    app.retry(later);
    assert!(started.elapsed() >= Duration::from_secs(2));
    assert_eq!(app.sent(later, 0), [(10, json!("hung"))]);

    // And the first is not resent again, its resend's 1 being after its 1; nor is the ticket
    // whose 1 is younger than the window
    assert_eq!(app.sent(stalled, 0).len(), 1);
    assert_eq!(app.sent(young, 0), []);
    app.stop();
}

#[test]
fn a_failed_run_is_retried_with_backoff_until_its_chain_has_had_its_attempts() {
    // Given the queue's default: 10 attempts
    assert_eq!(Settings::new(Duration::from_millis(100)).attempts, 10);

    // And an app whose queue gives a chain 3 attempts, the first retry 200 ms after a failure,
    // and whose directory does not know bob
    let settings = Settings {
        attempts: 3,
        ..Settings::new(Duration::from_millis(200))
    };
    let path = database("retried").unwrap();
    let app = App::open(&path, Kind::Events, Config::default(), settings).unwrap();

    // When it sends an invite for bob
    let invite = json!({ "user": "bob" });
    app.send(20, &invite).unwrap();
    let mut run = poll(WAIT, || app.root(&invite)).unwrap();

    // Then each failed run's step sends a -1 whose data is the lookup's failure, and the queue
    // sends a retry: a new event at 0 whose source is the failed run and whose data is the
    // failure, due 200 ms after the first failure and twice that after the second
    let failed = json!("lookup: no user bob");
    let mut written = Vec::new();
    for wait in [200, 400] {
        let retry = app.retry(run);
        let rows = app.about(run);
        let one = |status| rows.iter().find(|r| r.2 == status).cloned().unwrap();
        let (binned, due) = (one(-1), one(0));
        assert_eq!((binned.1, &binned.3), (20, &failed));
        assert_eq!((due.0, due.1, &due.3), (retry, 20, &failed));
        let at: DateTime<Utc> = due.4.unwrap().parse().unwrap();
        let (wait, slack) = (TimeDelta::milliseconds(wait), TimeDelta::milliseconds(5));
        assert!(
            at - accepted(binned.0) >= wait - slack,
            "{wait} after the failure"
        );
        assert!(
            at - accepted(retry) <= wait + slack,
            "{wait} after the failure"
        );
        written = app.rows();
        run = retry;
    }

    // And the third failure sends its -1 and no retry: the chain has had its 3 attempts
    assert!(until(WAIT, || app.statuses(run).contains(&-1)));
    thread::sleep(Duration::from_secs(1));
    assert_eq!(app.statuses(run), [1, -1]);
    assert_eq!(app.sent(run, -1), [(20, failed)]);

    // And every attempt ran on the invite's data
    assert_eq!(*lock(&app.directory.0), ["bob", "bob", "bob"]);

    // And the rows in _events were only added to
    assert!(app.rows().starts_with(&written));
    app.stop();
}

#[test]
fn with_graph_append_an_events_2_and_its_rows_commit_together() {
    // Given an app whose triage agent gives each ticket's severity as a graph row, appended by
    // GraphAppend
    let path = database("commit_together").unwrap();
    let app = App::open(&path, Kind::Graph, Config::default(), settings(2)).unwrap();

    // When it sends 200 tickets, one a millisecond, so their 2s come in many batches
    // Then no read, while they run, sees a ticket with its 2 and without its row
    let ticket = |i| json!({ "ticket": format!("t{i}"), "text": "it crashed" });
    let twos = "SELECT count(*) FROM _events e WHERE e.ty = 10 AND e.source IS NULL
        AND EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status = 2)";
    let bare = format!("{twos} AND NOT EXISTS (SELECT 1 FROM _nodes n WHERE n.event = e.id)");
    let (start, mut sent) = (Instant::now(), 0);
    while app.count(twos) < 200 {
        if sent < 200 {
            app.send(10, &ticket(sent)).unwrap();
            sent += 1;
        }
        assert_eq!(app.count(&bare), 0);
        assert!(start.elapsed() < WAIT * 3, "{} of 200 2s", app.count(twos));
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(app.count(&bare), 0);

    // And each row is stamped with the event whose flow wrote it
    let t0 = app.root(&ticket(0)).unwrap();
    let row = prop(&app.read, "t0", "severity", None).unwrap().unwrap();
    assert_eq!((row.value, row.event), (json!(3), t0));

    // Given a ticket about tf rated slow
    let slow = json!({ "ticket": "tf", "text": "slow" });
    app.send(10, &slow).unwrap();
    let slow = app.ran(&slow);

    // When the triage agent fails the first run of a later ticket about tf, and its retry runs
    let flaky = json!({ "ticket": "tf", "text": "flaky crash" });
    app.send(10, &flaky).unwrap();
    let failed = poll(WAIT, || app.root(&flaky)).unwrap();
    let retry = app.retry(failed);
    assert!(until(WAIT, || app.statuses(retry).contains(&2)));

    // Then the retry's row is added, stamped with the retry, and the slow ticket's row stays
    let rows = history(&app.read, "tf", "severity", None).unwrap();
    let rows: Vec<_> = rows
        .into_iter()
        .map(|(r, s)| (r.value, r.event, s))
        .collect();
    let current = (json!(3), retry, State::Current);
    assert_eq!(rows, [(json!(2), slow, State::Replaced), current]);
    app.stop();
}

#[test]
fn reads_are_decided_by_ids_and_stamps() {
    let path = database("reads").unwrap();
    let app = App::open(&path, Kind::Graph, Config::default(), settings(2)).unwrap();

    // Given a ticket about t7 sent to come due in 700 ms, then a ticket about t7 due now
    let slow = json!({ "ticket": "t7", "text": "slow" });
    let crash = json!({ "ticket": "t7", "text": "it crashed" });
    let due = Utc::now() + TimeDelta::milliseconds(700);
    app.send_at(10, &slow, Some(due)).unwrap();
    app.send(10, &crash).unwrap();

    // When both have run, the one accepted later first
    let (a, b) = (app.ran(&slow), app.ran(&crash));
    assert!(a < b, "the delayed ticket was accepted first");
    let rows = history(&app.read, "t7", "severity", None).unwrap();
    let (delayed, crashed) = (rows[0].0.clone(), rows[1].0.clone());
    assert_eq!((delayed.event, crashed.event), (a, b));
    assert!(
        delayed.written > crashed.written,
        "the delayed ticket ran last"
    );

    // Then the current value is the row with the greatest event id, whatever order they ran in
    let now = |at| prop(&app.read, "t7", "severity", at).unwrap();
    assert_eq!(now(None), Some(crashed.clone()));

    // When a third ticket about t7 resolves it, retracting its severity
    let resolved = json!({ "ticket": "t7", "text": "resolved" });
    app.send(10, &resolved).unwrap();
    let c = app.ran(&resolved);

    // Then t7 has no current severity
    assert_eq!(now(None), None);
    let rows = history(&app.read, "t7", "severity", None).unwrap();
    let states: Vec<_> = rows.iter().map(|(r, s)| (r.event, *s)).collect();
    assert_eq!(
        states,
        [
            (a, State::Replaced),
            (b, State::Replaced),
            (c, State::Retracted)
        ]
    );

    // And as of T, only the rows written at or before T are read
    assert_eq!(now(Some(delayed.written)), Some(crashed.clone()));
    assert_eq!(now(Some(crashed.written)), Some(crashed.clone()));
    assert_eq!(now(Some(crashed.written - 1)), None);

    // When the triage agent fails the first run of a ticket about t8, and its retry rates it
    let flaky = json!({ "ticket": "t8", "text": "flaky slow" });
    app.send(10, &flaky).unwrap();
    let failed = poll(WAIT, || app.root(&flaky)).unwrap();
    let retry = app.retry(failed);
    assert!(until(WAIT, || app.statuses(retry).contains(&2)));

    // Then why returns the retry, which wrote the current value, then its chain of sources
    assert_eq!(why(&app.read, "t8", "severity").unwrap(), [retry, failed]);
    app.stop();
}

#[test]
fn a_batch_that_can_never_be_written_stops_its_inbox_and_is_lost_at_shutdown() {
    let _errors = logged();
    // Given an app with GraphAppend, whose default inbox hands over every 300 ms and whose mail
    // inbox every 20 ms
    let mail = InboxConfig {
        name: "mail".to_owned(),
        types: vec![21],
        interval: TimeDelta::milliseconds(20),
    };
    let config = Config {
        default_interval: TimeDelta::milliseconds(300),
        inboxes: vec![mail],
        ..Config::default()
    };
    let path = database("never_written").unwrap();
    let app = App::open(&path, Kind::Graph, config, settings(1)).unwrap();

    // When the triage agent gives a $graph item that is not one of rannoch-graph's ops
    let garbled = json!({ "ticket": "tg", "text": "garbled" });
    app.send(10, &garbled).unwrap();
    let ticket = poll(WAIT, || app.root(&garbled)).unwrap();

    // Then the hand-over of the batch with the ticket's 2 fails, and is logged at error
    assert!(until(WAIT, || !failed_hand_overs().is_empty()));

    // And none of that batch is written: no next, no 2, no graph row
    assert_eq!(app.sent(ticket, 0), []);
    assert_eq!(app.sent(ticket, 2), []);
    assert_eq!(app.count("SELECT count(*) FROM _nodes"), 0);

    // When it sends three more events to the default inbox
    let held = *failed_hand_overs().last().unwrap();
    for i in 0..3 {
        app.send(11, &rated(&format!("after{i}"))).unwrap();
    }

    // Then every later hand-over holds the failed batch and them, and fails: the heap grows
    assert!(until(WAIT, || failed_hand_overs().last() == Some(&(held + 3))));
    let after = "SELECT count(*) FROM _events WHERE json_extract(data, '$.ticket') LIKE 'after%'";
    assert_eq!(app.count(after), 0);

    // When it sends mail, which goes to the mail inbox
    let mail = json!({ "email": "ada@example.com" });
    app.send(21, &mail).unwrap();

    // Then the mail inbox still writes: the mail is written and runs to its 2
    app.ran(&mail);
    assert_eq!(app.count(after), 0, "nothing more from the default inbox");

    // When it sends one more event just after a failed hand-over, then shuts down
    let tried = failed_hand_overs().len();
    assert!(until(WAIT, || failed_hand_overs().len() > tried));
    let held = *failed_hand_overs().last().unwrap();
    app.send(11, &rated("last")).unwrap();
    drop(app.signal);
    app.rannoch.shutdown();

    // Then shutdown's hand-over, which holds it, fails and is logged
    assert_eq!(failed_hand_overs().last(), Some(&(held + 1)));

    // And it is not tried again
    let tried = failed_hand_overs().len();
    thread::sleep(Duration::from_millis(400));
    assert_eq!(failed_hand_overs().len(), tried);
    app.queue.join();

    // And the batch and every event after it are lost
    let read = ready(&path).unwrap();
    let count = |sql: &str| read.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
    let lost = "SELECT count(*) FROM _events WHERE json_extract(data, '$.ticket') IN
        ('after0', 'after1', 'after2', 'last')";
    assert_eq!(count(lost), 0);
    let two = format!("SELECT count(*) FROM _events WHERE source = '{ticket}' AND status = 2");
    assert_eq!(count(&two), 0);

    // And no failed hand-over held fewer events than the one before it
    let held = failed_hand_overs();
    assert!(held.windows(2).all(|w| w[0] <= w[1]), "{held:?}");
}

#[test]
fn a_crashs_resend_runs_the_stalled_events_flow_on_that_events_data() {
    // Given a store as a crash leaves it: a ticket about t3 whose 1 is an hour old with nothing
    // after it
    let path = migrated("crash_resend").unwrap();
    let crashed = json!({ "ticket": "t3", "text": "it crashed" });
    let stalled = stall(&ready(&path).unwrap(), &crashed, 3600).unwrap();

    // And an app with GraphAppend whose hunt runs soon, and which rates another ticket about t3
    // as slow first
    let settings = Settings {
        window: Duration::from_mins(1),
        hunt: Duration::from_millis(500),
        ..settings(1)
    };
    let app = App::open(&path, Kind::Graph, Config::default(), settings).unwrap();
    let slow = json!({ "ticket": "t3", "text": "slow" });
    app.send(10, &slow).unwrap();
    let slow = app.ran(&slow);

    // When the hunt resends the stalled ticket
    let resend = app.retry(stalled);

    // Then the resend runs the ticket flow on the stalled ticket's data: its next rates t3 as
    // crashed
    assert!(until(WAIT, || app
        .statuses(resend)
        .iter()
        .any(|s| *s == 2 || *s == -1)));
    assert_eq!(app.sent(resend, -1), [], "the resend failed");
    assert_eq!(
        app.sent(resend, 0),
        [(11, json!({ "ticket": "t3", "severity": 3 }))]
    );
    assert_eq!(app.statuses(resend), [1, 0, 2]);

    // And its graph row is added, stamped with the resend, after the slow ticket's
    let rows = history(&app.read, "t3", "severity", None).unwrap();
    let rows: Vec<_> = rows
        .into_iter()
        .map(|(r, s)| (r.value, r.event, s))
        .collect();
    let current = (json!(3), resend, State::Current);
    assert_eq!(rows, [(json!(2), slow, State::Replaced), current]);

    // And why names the resend, then the stalled ticket it resent
    assert_eq!(why(&app.read, "t3", "severity").unwrap(), [resend, stalled]);
    app.stop();
}

#[test]
fn a_failing_step_sends_its_minus_1_and_an_event_with_no_flow_gets_no_processor() {
    let path = database("failing_step").unwrap();
    let app = App::open(&path, Kind::Events, Config::default(), settings(2)).unwrap();

    // When it sends a ticket whose text is a number, and an event of ty 99, which no flow runs
    let numbered = json!({ "ticket": "t4", "text": 5 });
    let orphan = json!({ "ticket": "t99" });
    app.send(10, &numbered).unwrap();
    app.send(99, &orphan).unwrap();

    // Then the ticket's step sends a -1 whose data is the triage action's message, and the
    // queue retries it, and the retry fails the same way
    let failed = json!("agent triage input gave text as number, it declares string");
    let ticket = poll(WAIT, || app.root(&numbered)).unwrap();
    let retry = app.retry(ticket);
    assert_eq!(app.sent(ticket, -1), [(10, failed.clone())]);
    assert_eq!(app.sent(ticket, 0), [(10, failed.clone())]);
    assert!(until(WAIT, || app.statuses(retry).contains(&-1)));
    assert_eq!(app.sent(retry, -1), [(10, failed)]);

    // And the ty 99 event gets a -1 saying "no processor", and no retry though its chain has
    // attempts left
    let other = poll(WAIT, || app.root(&orphan)).unwrap();
    assert!(until(WAIT, || app.statuses(other).contains(&-1)));
    thread::sleep(Duration::from_millis(300));
    let rows: Vec<(i64, Value)> = app.about(other).into_iter().map(|r| (r.2, r.3)).collect();
    assert_eq!(rows, [(1, json!("taken")), (-1, json!("no processor"))]);
    app.stop();
}

#[test]
fn after_shutdown_a_send_is_closed_and_join_waits_for_signal_senders_runs_and_jobs() {
    let path = database("after_shutdown").unwrap();
    let app = App::open(&path, Kind::Events, Config::default(), settings(1)).unwrap();

    // Given a run and a job the queue has started, each held by its agent
    let (hold, page) = (json!({ "note": "hold" }), json!({ "note": "page" }));
    app.send(40, &hold).unwrap();
    app.send(41, &page).unwrap();
    app.hold.started.recv_timeout(WAIT).unwrap();
    app.page.started.recv_timeout(WAIT).unwrap();
    let ids = [app.root(&hold).unwrap(), app.root(&page).unwrap()];

    // When the app shuts rannoch down, keeping its signal sender
    let sender = app.rannoch.sender();
    app.rannoch.shutdown();

    // Then a send returns Closed
    let late = Event {
        id: Uuid::nil(),
        source: None,
        ty: 11,
        data: rated("late"),
        run_at: None,
        status: Status::Added,
    };
    assert_eq!(sender.send(late), Err(Error::Closed));

    // And join does not return while the app keeps its signal sender
    let (returned, done) = mpsc::channel();
    let queue = app.queue;
    let waiting = thread::spawn(move || {
        queue.join();
        let _ = returned.send(());
    });
    let waits = |what: &str| {
        let got = done.recv_timeout(Duration::from_millis(300));
        assert!(got.is_err(), "{what}");
    };
    waits("a kept signal sender keeps join waiting");

    // When the app drops it, join still waits for the run, then for the job
    drop(app.signal);
    waits("join waits for the run");
    app.hold.release.send(()).unwrap();
    waits("join waits for the job");
    app.page.release.send(()).unwrap();

    // Then join returns once both have finished
    assert!(done.recv_timeout(WAIT).is_ok());
    waiting.join().unwrap();

    // And what they sent after shutdown is not written
    for id in ids {
        let sql = "SELECT count(*) FROM _events WHERE source = ?1 AND status = 2";
        let twos = app
            .read
            .query_row(sql, [id.to_string()], |r| r.get::<_, i64>(0));
        assert_eq!(twos.unwrap(), 0);
    }
}

#[test]
fn a_second_app_on_the_same_store_is_not_stopped() {
    // Given an app with GraphAppend whose default inbox hands over every second
    let path = database("second_app").unwrap();
    let config = || Config {
        default_interval: TimeDelta::seconds(1),
        ..Config::default()
    };
    let first = App::open(&path, Kind::Graph, config(), settings(1)).unwrap();

    // When a second app opens rannoch and starts its queue over the same store
    // Then nothing stops it
    let second = App::open(&path, Kind::Graph, config(), settings(1)).unwrap();

    // When the first app's ticket is written, and both queues are woken while the first's 1 is
    // on its heap
    let data = json!({ "ticket": "t6", "text": "it crashed" });
    first.send(10, &data).unwrap();
    let ticket = poll(WAIT, || first.root(&data)).unwrap();
    second.signal.send(()).unwrap();

    // Then both take it and both run its flow: two 1s, two nexts, two 2s
    assert!(until(WAIT, || first.sent(ticket, 2).len() == 2));
    let mut statuses = first.statuses(ticket);
    statuses.sort_unstable();
    assert_eq!(statuses, [0, 0, 1, 1, 2, 2]);

    // And the ticket's row is written once: writing the same event's rows again adds none
    let rows = history(&first.read, "t6", "severity", None).unwrap();
    let rows: Vec<_> = rows
        .into_iter()
        .map(|(r, s)| (r.value, r.event, s))
        .collect();
    assert_eq!(rows, [(json!(3), ticket, State::Current)]);
    second.stop();
    first.stop();
}
