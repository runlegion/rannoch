//! FR-RANNOCH-119's scenarios from `tests/features/lib.feature` (`@criterion-FR-RANNOCH-119-c3`
//! to `-c9`, `-c11`, `-c13`, `-c14`). The store is the app's database through the real store,
//! events from the app's forgr factory, handed over by the real queue; the flow docs are
//! `tests/flows.json` and the test processor crate's actions are `Fake`s. The build scenarios
//! (`-c1`, `-c2`, `-c10`, `-c12`, `-c16`) build the test app of `tests/app` with cargo; #122's
//! ontology path builds and runs pay-app of the same workspace, over onto-pay's table.
//! FR-RANNOCH-124's scenarios, from `tests/features/fr_rannoch_124.feature`, run `BUILT_IN`'s
//! flows on the built-in steps alone, `check`ed with no signature, the append an `Events` made
//! `with_writes`. `tests/features/clock.feature`'s two scenarios read this crate's code and
//! tests: every test runs with the clock frozen, and moves time only through tokio.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Error, Event, InboxConfig, Rannoch, Status, clock};
use rannoch_events::Events;
use rannoch_processor::{Action, FlowDoc, Processor, Sig, check};
use rannoch_queue::{Flows, Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq)]
struct Ty(u16);
impl From<Ty> for u16 {
    fn from(t: Ty) -> u16 {
        t.0
    }
}
impl TryFrom<u16> for Ty {
    type Error = ();
    fn try_from(v: u16) -> std::result::Result<Self, ()> {
        Ok(Ty(v))
    }
}

type Obj = Map<String, Value>;
type Calls = Arc<Mutex<Vec<(String, Obj)>>>;
type Act = Box<dyn Fn(&Value, &Obj) -> std::result::Result<Obj, String> + Send + Sync>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// A test processor crate's action, built from a `Sig` and a closure; records each run.
struct Fake(Sig, Act, Calls);
impl Action for Fake {
    fn sig(&self) -> &Sig {
        &self.0
    }
    fn run(&self, options: &Value, input: &Obj) -> std::result::Result<Obj, String> {
        if let Ok(mut calls) = self.2.lock() {
            calls.push((self.0.action.clone(), input.clone()));
        }
        (self.1)(options, input)
    }
}

fn obj(v: &Value) -> Obj {
    v.as_object().cloned().unwrap_or_default()
}

fn text(input: &Obj) -> String {
    input
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The test processor crate: trim, upper, greet, limit, note, rate (gives `rating`) and
/// classify, which fails on its first run only; each a `Fake` of its `Sig` and this closure.
fn actions(calls: &Calls, rating: &Arc<Mutex<Obj>>) -> Vec<Arc<dyn Action>> {
    let (rating, runs) = (rating.clone(), Arc::new(AtomicUsize::new(0)));
    let gives = json!({ "trim": { "text": "string" }, "upper": { "text": "string" },
        "greet": { "greeting": "string" }, "limit": {}, "note": {},
        "rate": { "severity": "number" }, "classify": { "category": "string" } });
    let sigs: Vec<Sig> = obj(&gives)
        .into_iter()
        .map(|(action, gives)| Sig {
            action,
            gives: serde_json::from_value(gives).unwrap_or_default(),
            ..Sig::default()
        })
        .collect();
    sigs.into_iter().map(|sig| {
        let (name, rating, runs) = (sig.action.clone(), rating.clone(), runs.clone());
        let act: Act = Box::new(move |o, i| {
            let t = text(i);
            Ok(obj(&match name.as_str() {
                "trim" => json!({ "text": t.trim() }),
                "upper" => json!({ "text": t.to_uppercase() }),
                "greet" => json!({ "greeting": format!("{} {t}", o["salutation"].as_str().unwrap_or_default()) }),
                "limit" if t.len() as u64 > o["max"].as_u64().unwrap_or_default() => {
                    return Err(format!("limit: text is {} long, max {}", t.len(), o["max"]));
                }
                "rate" => Value::Object(rating.lock().map_or_default(|r| r.clone())),
                "classify" if runs.fetch_add(1, Ordering::SeqCst) == 0 => return Err("classify: no answer".into()),
                "classify" => json!({ "category": "bug" }),
                _ => json!({}),
            }))
        });
        Arc::new(Fake(sig, act, calls.clone())) as Arc<dyn Action>
    }).collect()
}

fn database(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir.join("store.db"))
}

/// The app's store over `path`, with every migration applied by opening rannoch once.
async fn migrated(path: &Path) -> Result<Arc<Store>> {
    let store = Arc::new(Store::new(path.to_owned(), rannoch_core::migrator()));
    let rannoch = Rannoch::<Ty, Value>::open(Config {
        store: Some(store.clone()),
        ..Config::default()
    })?;
    shutdown(rannoch).await;
    Ok(store)
}

/// Shuts `rannoch` down from async code: shutdown blocks, so it runs through `spawn_blocking`.
async fn shutdown(rannoch: Rannoch<Ty, Value>) {
    if let Err(e) = tokio::task::spawn_blocking(move || rannoch.shutdown()).await {
        panic!("shutdown did not return: {e}");
    }
}

/// Joins `queue` from async code: join blocks, so it runs through `spawn_blocking`.
async fn join(queue: Queue) -> std::result::Result<(), Error> {
    match tokio::task::spawn_blocking(move || queue.join()).await {
        Ok(joined) => joined,
        Err(e) => panic!("join did not return: {e}"),
    }
}

/// Held for each scenario's whole run, so the scenarios take turns: ids made in the same frozen
/// millisecond keep their order only while no other test's paused runtime makes ids beside them.
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// Waits up to `limit` on the clock for `done`.
async fn until(limit: Duration, done: impl Fn() -> bool) -> bool {
    let start = clock::elapsed();
    while start.elapsed() < limit {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    done()
}

/// One row about an event: id, ty, status, data, `run_at`.
type About = (Uuid, i64, i64, Value, Option<String>);

/// The app's actions over what they record and the rating `rate` gives.
type Made = fn(&Calls, &Arc<Mutex<Obj>>) -> Vec<Arc<dyn Action>>;

/// An append that tells its channel of each batch it is handed, then appends it with `Events`.
#[derive(Debug)]
struct Told(Events, std::sync::mpsc::Sender<()>);

impl Append<Ty, Value> for Told {
    fn append(
        &self,
        batch: &[Event<Ty, Value>],
    ) -> std::result::Result<Vec<Event<Ty, Value>>, Error> {
        let _ = self.1.send(());
        self.0.append(batch)
    }
}

/// The app: the store, rannoch, the queue handing events to rannoch-processor, the processor,
/// and what the fake actions saw.
struct App {
    path: PathBuf,
    read: Connection,
    signal: tokio::sync::mpsc::UnboundedSender<()>,
    rannoch: Rannoch<Ty, Value>,
    queue: Queue,
    processor: Arc<Processor<Ty, Value>>,
    calls: Calls,
    rating: Arc<Mutex<Obj>>,
}

impl App {
    async fn start(name: &str, attempts: u32) -> Result<Self> {
        Self::open(database(name)?, attempts).await
    }

    /// The app over the store at `path`, as it is.
    async fn open(path: PathBuf, attempts: u32) -> Result<Self> {
        let settings = Settings {
            attempts,
            ..Settings::new(Duration::from_millis(100))
        };
        Self::open_with(path, settings).await
    }

    /// The app over a fresh store `name` running `flows` on the built-in steps, no action of its
    /// own.
    async fn built_in(name: &str, flows: &str) -> Result<Self> {
        Self::built_in_at(database(name)?, flows, Config::default()).await
    }

    /// As `built_in`, over the store at `path`, rannoch's inboxes as `config` has them.
    async fn built_in_at(path: PathBuf, flows: &str, config: Config<Ty, Value>) -> Result<Self> {
        let settings = Settings {
            attempts: 1,
            ..Settings::new(Duration::from_millis(100))
        };
        Self::with(path, flows, |_, _| Vec::new(), config, settings).await
    }

    /// Runs the flow of the event `id`, of `ty` with `data`, again, its sends handed to rannoch.
    fn rerun(&self, id: Uuid, ty: u16, data: &Value) -> std::result::Result<(), String> {
        let (source, run_at, status, data) =
            (None, None, rannoch_core::Status::Added, data.to_string());
        let row = rannoch_core::Event {
            id,
            source,
            ty,
            data,
            run_at,
            status,
        };
        let flow = self.processor.flow(ty).ok_or("no flow")?;
        (flow.run)(&row, &row, &self.rannoch.sender())
    }

    /// The app over the store at `path`, as it is, its queue on `settings`.
    async fn open_with(path: PathBuf, settings: Settings) -> Result<Self> {
        Self::over(path, include_str!("flows.json"), actions, settings).await
    }

    /// The app over the store at `path` running `flows` with the actions `made`, its queue on
    /// `settings`; its `Events` writes the processor's store rows with each 2.
    async fn over(path: PathBuf, flows: &str, made: Made, settings: Settings) -> Result<Self> {
        Self::with(path, flows, made, Config::default(), settings).await
    }

    /// As `over`, rannoch's inboxes as `config` has them.
    async fn with(
        path: PathBuf,
        flows: &str,
        made: Made,
        config: Config<Ty, Value>,
        settings: Settings,
    ) -> Result<Self> {
        Self::told(path, flows, made, (config, None), settings).await
    }

    /// As `with`, its append first telling `told`, when there is one, of each batch it is
    /// handed.
    async fn told(
        path: PathBuf,
        flows: &str,
        made: Made,
        (config, told): (Config<Ty, Value>, Option<std::sync::mpsc::Sender<()>>),
        settings: Settings,
    ) -> Result<Self> {
        let store = migrated(&path).await?;
        let (calls, rating) = (Calls::default(), Arc::default());
        let actions = made(&calls, &rating);
        let processor = Arc::new(Processor::new(flows, actions, ready(&path)?)?);
        let (signal, wake) = tokio::sync::mpsc::unbounded_channel();
        let events = Events::new(ready(&path)?, signal.clone()).with_writes(processor.writes());
        let append = match told {
            Some(told) => Arc::new(Told(events, told)) as Arc<dyn Append<Ty, Value>>,
            None => Arc::new(events) as Arc<dyn Append<Ty, Value>>,
        };
        let rannoch = Rannoch::open(Config {
            store: Some(store),
            append: Some(append),
            ..config
        })?;
        let queue = Queue::start(
            ready(&path)?,
            wake,
            rannoch.sender(),
            processor.clone(),
            settings,
        );
        Ok(Self {
            read: ready(&path)?,
            path,
            signal,
            rannoch,
            queue,
            processor,
            calls,
            rating,
        })
    }

    /// An event of `ty` with `data` from the app's forgr factory, and the queue signalled.
    fn hand(&self, ty: i64, data: &Value) -> Result<Uuid> {
        let id = clock::new_id();
        let row = _events::factory()
            .id(id.to_string())
            .ty(ty)
            .data(data.to_string())
            .status(0);
        row.insert(&ready(&self.path)?)?;
        self.signal.send(())?;
        Ok(id)
    }

    /// The rows whose source is `id`, in order.
    fn about(&self, id: Uuid) -> Vec<About> {
        let sql = "SELECT id, ty, status, data, run_at FROM _events WHERE source = ?1 ORDER BY id";
        let rows = self.read.prepare(sql).and_then(|mut select| {
            select
                .query_map([id.to_string()], |r| {
                    let (id, data): (String, String) = (r.get(0)?, r.get(3)?);
                    let data = serde_json::from_str(&data).unwrap_or(Value::Null);
                    Ok((
                        id.parse().unwrap_or_default(),
                        r.get(1)?,
                        r.get(2)?,
                        data,
                        r.get(4)?,
                    ))
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
        self.about(id)
            .into_iter()
            .filter(|r| r.2 == status)
            .map(|r| (r.1, r.3))
            .collect()
    }

    fn count(&self, sql: &str) -> i64 {
        self.read.query_row(sql, [], |r| r.get(0)).unwrap_or(-1)
    }

    fn names(&self) -> Vec<String> {
        self.calls
            .lock()
            .map_or_default(|c| c.iter().map(|(n, _)| n.clone()).collect())
    }

    /// Waits for the retry of `event` and returns its id.
    async fn retry(&self, event: Uuid) -> Uuid {
        assert!(until(WAIT, || !self.sent(event, 0).is_empty()).await);
        self.about(event)
            .into_iter()
            .find(|r| r.2 == 0)
            .map_or_default(|r| r.0)
    }

    async fn stop(self) {
        drop(self.signal);
        shutdown(self.rannoch).await;
        assert_eq!(join(self.queue).await, Ok(()));
    }
}

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test(start_paused = true)]
async fn rannoch_processor_runs_the_flow_for_an_event_the_queue_hands_it() {
    let _turn = TURN.lock().await;
    // Given an app built with a flow whose step is the chain trim.upper.greet, from a test
    // processor crate, ending in a next with a delay of 500 whose shape fills who with or and
    // builds msg with $concat
    // And a flow for the receiving ty whose step has no next
    // And a flow whose step puts its result under a field with as
    let app = App::start("runs_the_flow", 10).await.unwrap();

    // When the queue hands rannoch-processor an event of the chain's ty
    let before = clock::now() - TimeDelta::milliseconds(1);
    let event = app
        .hand(10, &json!({ "text": "  sam ", "lang": "en" }))
        .unwrap();

    // Then trim, upper and greet run on the one event, with no event written between them
    assert!(until(WAIT, || app.statuses(event).contains(&2)).await);
    let after = clock::now();
    assert_eq!(app.names(), ["trim", "upper", "greet"]);
    assert_eq!(app.statuses(event), [1, 0, 2]);
    assert_eq!(
        app.count("SELECT count(*) FROM _events"),
        4,
        "the event, its 1, one next, one 2"
    );

    // And each link reads the root's data, and is passed the options the links before it gave,
    // the later winning
    let calls = app.calls.lock().unwrap().clone();
    assert!(
        calls.iter().all(|(_, input)| input["lang"] == "en"),
        "{calls:?}"
    );
    let texts: Vec<&Value> = calls.iter().map(|(_, input)| &input["text"]).collect();
    assert_eq!(texts, ["  sam ", "sam", "SAM"]);

    // And exactly one next is written, with the processor's 2: data msg 'hi SAM!' and who
    // 'friend', source the event, status 0, run_at the time of the run plus 500
    assert_eq!(
        app.sent(event, 0),
        [(20, json!({ "msg": "hi SAM!", "who": "friend" }))]
    );
    let next = app.about(event).into_iter().find(|r| r.2 == 0).unwrap();
    let run_at: DateTime<Utc> = next.4.unwrap().parse().unwrap();
    let delay = TimeDelta::milliseconds(500);
    assert!(
        run_at >= before + delay && run_at <= after + delay,
        "{run_at} is not the run plus 500"
    );

    // When the queue hands rannoch-processor the next
    // Then its flow ends with the processor's 2 and nothing more is sent
    assert!(until(WAIT, || app.statuses(next.0).contains(&2)).await);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(app.statuses(next.0), [1, 2]);
    assert_eq!(
        app.count("SELECT count(*) FROM _events"),
        6,
        "nothing more was sent"
    );

    // When the queue hands rannoch-processor an event of the flow that uses as
    let shout = app.hand(12, &json!({ "text": "sam" })).unwrap();

    // Then the step's result is under the field as names, and the input's fields are still there
    assert!(until(WAIT, || app.statuses(shout).contains(&2)).await);
    assert_eq!(
        app.sent(shout, 0),
        [(22, json!({ "shout": { "text": "SAM" }, "text": "sam" }))]
    );

    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failing_link_fails_the_step_and_the_retry_reruns_the_chain() {
    let _turn = TURN.lock().await;
    // Given an app built with a flow whose step is a chain with a link limit that fails on text
    // longer than 5
    let app = App::start("a_failing_link", 2).await.unwrap();

    // When the queue hands rannoch-processor an event whose text is 13 long
    let event = app.hand(15, &json!({ "text": "too long text" })).unwrap();

    // Then the links after limit do not run
    assert!(until(WAIT, || app.statuses(event).contains(&-1)).await);
    assert_eq!(app.names(), ["trim", "limit"]);

    // And the processor sends one -1 whose source is the event and whose data is
    // 'limit: text is 13 long, max 5'
    let failed = json!("limit: text is 13 long, max 5");
    assert_eq!(app.sent(event, -1), [(15, failed.clone())]);

    // And no next and no 2 are sent
    assert!(!app.statuses(event).contains(&2));
    // the only 0 about the event is the queue's retry, of its own ty with the failure as data
    let retry = app.retry(event).await;
    assert_eq!(app.sent(event, 0), [(15, failed.clone())]);

    // When the queue's retry of the event comes due
    // Then the chain runs again from its first link
    assert!(until(WAIT, || app.statuses(retry).contains(&-1)).await);
    assert_eq!(app.names(), ["trim", "limit", "trim", "limit"]);
    assert_eq!(
        app.sent(retry, -1),
        [(15, failed)],
        "the retry ran on the root's data"
    );

    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn branch_and_each_send_their_nexts() {
    let _turn = TURN.lock().await;
    // Given an app built with a flow whose branch has a case when crash naming ty 31, a case when
    // question naming ty 32, and an else
    // And a flow whose branch has a case when level is the number 1, and an else
    // And a flow whose each sends one next per item of a list
    let app = App::start("branch_and_each", 1).await.unwrap();
    // the nexts whose source is `event`, once its flow has sent its 2
    let nexts = async |event| {
        assert!(until(WAIT, || app.statuses(event).contains(&2)).await);
        app.sent(event, 0)
    };

    // When the queue hands rannoch-processor an event whose value is crash, one whose value is
    // question, and one whose value has no case
    let crash = app.hand(50, &json!({ "value": "crash" })).unwrap();
    let question = app.hand(50, &json!({ "value": "question" })).unwrap();
    let other = app.hand(50, &json!({ "value": "hang" })).unwrap();

    // Then one next of ty 31, one of ty 32 and one else next are sent, each with its event as
    // source
    assert_eq!(nexts(crash).await, [(31, json!({ "severity": 1 }))]);
    assert_eq!(nexts(question).await, [(32, json!({}))]);
    assert_eq!(nexts(other).await, [(33, json!({}))]);

    // When the queue hands rannoch-processor an event whose level is the number 1, and one whose
    // level is the string "1"
    let number = app.hand(51, &json!({ "level": 1 })).unwrap();
    let string = app.hand(51, &json!({ "level": "1" })).unwrap();

    // Then the first sends the case's next and the second sends the else next
    assert_eq!(nexts(number).await, [(31, json!({ "severity": 1 }))]);
    assert_eq!(nexts(string).await, [(33, json!({}))]);

    // When the queue hands rannoch-processor an event whose list is 'a b c'
    let list = app.hand(52, &json!({ "list": ["a", "b", "c"] })).unwrap();

    // Then three nexts are sent, one per item, each with that event as source
    let mut sent = nexts(list).await;
    sent.sort_by_key(|n| n.1.to_string());
    let words = ["a", "b", "c"].map(|w| (41, json!({ "word": w })));
    assert_eq!(sent, words);

    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_two_step_flow_sends_its_next_only_when_every_step_succeeds() {
    let _turn = TURN.lock().await;
    // Given an app built with a two-step flow whose step 0 sends a next and whose step 1 runs a
    // test action that fails on the first attempt and succeeds on the next
    let app = App::start("a_two_step_flow", 2).await.unwrap();

    // When the queue hands rannoch-processor an event of that flow
    let event = app.hand(40, &json!({ "text": "  sam " })).unwrap();

    // Then step 1 is passed the options step 0 gave
    assert!(until(WAIT, || app.statuses(event).contains(&-1)).await);
    assert_eq!(app.names(), ["trim", "classify"]);
    assert_eq!(app.calls.lock().unwrap()[1].1["text"], "sam");

    // And the processor sends one -1 for the event, and no next
    let failed = json!("classify: no answer");
    assert_eq!(app.sent(event, -1), [(40, failed.clone())]);
    assert!(!app.statuses(event).contains(&2));
    let retry = app.retry(event).await;
    assert_eq!(
        app.sent(event, 0),
        [(40, failed)],
        "the only 0 is the queue's retry"
    );

    // When the queue's retry of the event comes due
    // Then step 0's next is sent, and the processor sends its 2
    assert!(until(WAIT, || app.statuses(retry).contains(&2)).await);
    assert_eq!(app.names()[..4], ["trim", "classify", "trim", "classify"]);
    assert_eq!(
        app.sent(retry, 0),
        [(20, json!({ "msg": "sam", "who": "two" }))]
    );

    // And across both attempts exactly one next was sent
    let nexts = app.count("SELECT count(*) FROM _events WHERE ty = 20 AND status = 0");
    assert_eq!(nexts, 1, "one next across both attempts");

    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_actions_real_output_is_checked_at_run_time() {
    let _turn = TURN.lock().await;
    // Given an app built with a flow whose step runs a test action that declares it gives
    // severity
    let app = App::start("an_actions_real_output", 1).await.unwrap();

    // When the queue hands rannoch-processor an event and the action's output has a severity
    *app.rating.lock().unwrap() = obj(&json!({ "severity": 2 }));
    let rated = app.hand(30, &json!({ "text": "it crashed" })).unwrap();

    // Then the step's next carries it on
    assert!(until(WAIT, || app.statuses(rated).contains(&2)).await);
    assert_eq!(app.sent(rated, 0), [(31, json!({ "severity": 2 }))]);

    // When the queue hands rannoch-processor an event and the action's output has no severity
    *app.rating.lock().unwrap() = obj(&json!({ "category": "bug" }));
    let unrated = app.hand(30, &json!({ "text": "it crashed" })).unwrap();

    // Then the step sends a -1 naming severity, and no next and no 2
    assert!(until(WAIT, || app.statuses(unrated).contains(&-1)).await);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(app.statuses(unrated), [1, -1]);
    assert_eq!(
        app.sent(unrated, -1),
        [(30, json!("rate gave no severity"))]
    );

    app.stop().await;
}

/// The builds of the test app that fail, in the order of the scenario: the flow set, whether
/// proc-text's `renamed` feature is on, and the errors the build lists, one per line.
#[rustfmt::skip]
const BAD: [(&str, bool, &str); 10] = [
    ("e1", false, "E1 a.json step 0 nope: no action named nope"),
    ("e2", false, "E2 a.json step 0 greet: the options for greet do not parse: unknown field `salut`"),
    ("e3", false, "E3 a.json step 0 upper: upper takes text, which nothing before it gives"),
    ("e4", false, "E4 a.json step 0 upper: upper takes text as string, it is number"),
    ("e5", false, "E5 a.json step 0 next: the next to notice gives no who, which it takes"),
    ("e6", false, "E6 a.json step 0 next: the next to notice gives who as number, it takes string"),
    ("valid", true, "E5 10-greet.json step 0 next: $greeting names nothing in scope"),
    ("e7", false, "E7 a.json step 0 next: the next goes to ty 99, which no flow takes"),
    ("forget", false, "E1 a.json step 0 forget: no action named forget"),
    ("multi", false, "E1 a.json step 0 nope: no action named nope
E7 a.json step 0 next: the next goes to ty 99, which no flow takes
E4 b.json step 0 upper: upper takes text as string, it is number"),
];

/// One fixture build at a time: every build shares one target dir and one built app.
static CARGO: Mutex<()> = Mutex::new(());

/// The test app of `tests/app`, a workspace of proc-text and the app, with onto-pay and
/// pay-app, copied to a dir of its own so its flow docs can be removed; every copy builds into
/// the one shared target dir.
struct Fixture(PathBuf);

impl Fixture {
    fn copy(name: &str) -> Result<Self> {
        let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        let mut todo = vec![PathBuf::new()];
        while let Some(rel) = todo.pop() {
            fs::create_dir_all(dir.join(&rel))?;
            for entry in fs::read_dir(crate_dir.join("tests/app").join(&rel))? {
                let rel = rel.join(entry?.file_name());
                let from = crate_dir.join("tests/app").join(&rel);
                if from.is_dir() {
                    todo.push(rel);
                } else if rel.ends_with("Cargo.toml") {
                    let toml = fs::read_to_string(from)?;
                    let toml = toml.replace("\"../../..", &format!("\"{}", crate_dir.display()));
                    fs::write(dir.join(rel), toml)?;
                } else {
                    fs::copy(from, dir.join(rel))?;
                }
            }
        }
        fs::copy(crate_dir.join("../../Cargo.lock"), dir.join("Cargo.lock"))?;
        Ok(Self(dir))
    }

    /// Builds the app with the flow set `set`: Ok when the build succeeds, else every error
    /// the build lists.
    fn build(&self, set: &str, renamed: bool) -> Result<std::result::Result<(), Vec<String>>> {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let mut build = std::process::Command::new(cargo);
        build
            .current_dir(&self.0)
            .args(["build", "-q", "-p", "app"]);
        if renamed {
            build.args(["--features", "renamed"]);
        }
        let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("app-target");
        let out = build
            .env("CARGO_TARGET_DIR", target)
            .env("FLOWS", set)
            .output()?;
        if out.status.success() {
            return Ok(Ok(()));
        }
        let stderr = String::from_utf8(out.stderr)?;
        let is_error =
            |l: &&str| l.starts_with('E') && l[1..].starts_with(|c: char| c.is_ascii_digit());
        let errors: Vec<String> = stderr
            .lines()
            .map(str::trim)
            .filter(is_error)
            .map(str::to_owned)
            .collect();
        if errors.is_empty() {
            return Err(stderr.into());
        }
        Ok(Err(errors))
    }

    /// Runs the built app with its flow docs removed from disk: what it prints.
    fn run_without_docs(&self) -> Result<String> {
        let (docs, gone) = (self.0.join("app/flows"), self.0.join("app/flows-removed"));
        fs::rename(&docs, &gone)?;
        let app = Path::new(env!("CARGO_TARGET_TMPDIR")).join("app-target/debug/app");
        let out = std::process::Command::new(app).output();
        fs::rename(&gone, &docs)?;
        let out = out?;
        assert!(out.status.success(), "the app ran");
        Ok(String::from_utf8(out.stdout)?)
    }

    /// Builds pay-app and runs it over the store at `path`: Ok, or the stderr of the build or
    /// the run that failed.
    fn run_pay_app(&self, path: &Path) -> Result<()> {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("app-target");
        let built = std::process::Command::new(cargo)
            .current_dir(&self.0)
            .args(["build", "-q", "-p", "pay-app"])
            .env("CARGO_TARGET_DIR", &target)
            .output()?;
        if !built.status.success() {
            return Err(String::from_utf8(built.stderr)?.into());
        }
        let app = target.join("debug/pay-app");
        let ran = std::process::Command::new(app).arg(path).output()?;
        if !ran.status.success() {
            return Err(String::from_utf8(ran.stderr)?.into());
        }
        Ok(())
    }

    /// Builds each of `BAD` (the renamed build only when `renamed`) and asserts it fails
    /// listing exactly its errors.
    fn assert_bad(&self, renamed: bool) -> Result<()> {
        for (set, with_renamed, errors) in BAD.iter().filter(|b| renamed || !b.1) {
            let errors = errors.lines().map(str::to_owned).collect();
            assert_eq!(self.build(set, *with_renamed)?, Err(errors), "{set}");
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn the_apps_build_checks_its_flow_docs() -> Result<()> {
    let _one = CARGO.lock().unwrap_or_else(PoisonError::into_inner);
    // Given an app whose flow docs are a valid set over the actions of a test processor crate,
    // one of whose actions is a job
    let app = Fixture::copy("the_apps_build_checks")?;

    // When the app is built
    // Then the build succeeds
    assert_eq!(app.build("valid", false)?, Ok(()));

    // And each flow that uses the job action reports a job step, and each flow that uses none
    // does not
    // And the built app runs its flows with the flow doc files removed from disk
    let ran = app.run_without_docs()?;
    assert_eq!(ran, "10 false\n15 false\n20 false\n30 true\n");

    // When the app is built with each bad flow doc in turn (a step naming nope; options for
    // greet with the unknown field salut; upper taking text nothing gives; upper taking text as
    // a number; a next to notice giving no who; one giving who as a number; a $concat naming
    // greeting, which nothing in scope gives; a next to ty 99; a step naming forget; two docs
    // holding three errors)
    // Then each build fails listing exactly its errors
    app.assert_bad(true)
}

#[tokio::test(start_paused = true)]
async fn the_apps_build_gathers_the_signatures_a_separate_processor_crate_exports() -> Result<()> {
    let _one = CARGO.lock().unwrap_or_else(PoisonError::into_inner);
    // Given a workspace with a processor crate, separate from the app, that exports its
    // actions' signatures as data, among them greet
    // And an app with its own processor crate, whose flow docs use actions of both, including a
    // next whose template names the field greet gives
    let app = Fixture::copy("the_apps_build_gathers")?;

    // When the app is built
    // Then its build.rs gathers the signatures of both crates and the build succeeds
    assert_eq!(app.build("valid", false)?, Ok(()));

    // When the app is built with each bad set of flow docs
    // Then each build fails listing every error in it
    app.assert_bad(false)?;

    // When the separate crate renames the field greet gives, and the app is built
    // Then the build fails listing one error: the next's template names a field nothing gives
    let renamed = "E5 10-greet.json step 0 next: $greeting names nothing in scope".to_owned();
    assert_eq!(app.build("valid", true)?, Err(vec![renamed]));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_apps_ontology_crate_holds_the_table_its_store_step_writes() -> Result<()> {
    let _one = CARGO.lock().unwrap_or_else(PoisonError::into_inner);
    // Given an ontology crate, separate from the app, with a payments table in its own
    // migrations and its own MIGRATIONS_DIR
    // And an app whose build.rs gathers rannoch-events' and the ontology crate's migrations
    // into its migrations directory and embeds them
    let app = Fixture::copy("an_apps_ontology_crate")?;
    let path = app.0.join("store.db");

    // When the app is built and run: it opens its store with that migrator, and sends a payment
    // whose flow's built-in store step writes a row into the payments table
    app.run_pay_app(&path)?;

    // Then the payment has its 2, and the payments table its row
    let store = Connection::open(&path)?;
    let count = |sql: &str| store.query_row(sql, [], |r| r.get::<_, i64>(0));
    let twos = "SELECT count(*) FROM _events e JOIN _events s ON s.source = e.id
        WHERE e.ty = 1 AND e.status = 0 AND s.status = 2";
    assert_eq!(count(twos)?, 1);
    let row: (String, i64) = store.query_row("SELECT order_id, amount FROM payments", [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?;
    assert_eq!(row, ("o1".to_owned(), 10));

    // And the store holds no _nodes or _edges: the app's shape is its ontology crate's
    let graph = "SELECT count(*) FROM sqlite_master WHERE name IN ('_nodes', '_edges')";
    assert_eq!(count(graph)?, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_wait_kept_in_events_matches_times_out_and_sends_a_late_match_to_late_across_a_restart() {
    let _turn = TURN.lock().await;
    // Given an app built with a flow whose step waits for a payment keyed by its order, with a
    // timeout, a match next, a timeout next and a late next
    let app = App::start("a_wait_kept_in_events", 1).await.unwrap();

    // When orders o1 and o2 start waiting
    let before = clock::now() - TimeDelta::milliseconds(1);
    let o1 = app.hand(70, &json!({ "order": "o1" })).unwrap();
    let o2 = app.hand(70, &json!({ "order": "o2" })).unwrap();

    // Then each wait is kept in _events as an event whose source is the waiting event and whose
    // run_at is its timeout
    let wait = async |order: Uuid, key: &str| {
        assert!(until(WAIT, || app.statuses(order).contains(&2)).await);
        let after = clock::now();
        let wait = app.about(order).into_iter().find(|r| r.2 == 0).unwrap();
        let data = json!({ "$wait": { "step": 0, "for": 71 }, "key": key,
            "vals": { "order": key } });
        assert_eq!((wait.1, &wait.3), (70, &data));
        let run_at: DateTime<Utc> = wait.4.unwrap().parse().unwrap();
        let within = TimeDelta::milliseconds(2500);
        assert!(run_at >= before + within && run_at <= after + within);
        wait.0
    };
    let (w1, w2) = (wait(o1, "o1").await, wait(o2, "o2").await);

    // When the app's process is dropped and its _events file reopened
    let path = app.path.clone();
    app.stop().await;
    let app = App::open(path, 1).await.unwrap();

    // And two payments for o1 arrive before o1's timeout
    let pay = async |order: &str, amount: u64| {
        let paid = app.hand(71, &json!({ "order": order, "amount": amount }));
        let paid = paid.unwrap();
        assert!(until(WAIT, || app.statuses(paid).contains(&2)).await);
    };
    pay("o1", 5).await;
    pay("o1", 6).await;

    // Then o1's match next is sent once, with the wait as its source
    let matched = (72, json!({ "order": "o1", "paid": 5 }));
    assert!(until(WAIT, || !app.sent(w1, 0).is_empty()).await);
    assert!(
        app.statuses(w1).iter().all(|s| *s == 0),
        "o1 is not due yet"
    );
    assert_eq!(app.sent(w1, 0), std::slice::from_ref(&matched));

    // When the waits come due, and the due events are run again
    let due = async |w| until(WAIT, || app.statuses(w).contains(&2)).await;
    assert!(due(w1).await && due(w2).await);

    // Then o1's wait sends no timeout, and o2's wait sends its timeout next once
    assert_eq!(app.sent(w1, 0), [matched]);
    assert_eq!(app.sent(w1, 2), [(70, json!("matched"))]);
    let timeout = (73, json!({ "order": "o2" }));
    assert_eq!(app.sent(w2, 0), std::slice::from_ref(&timeout));
    assert_eq!(app.sent(w2, 2), [(70, json!("timeout"))]);

    // When a payment for o2 arrives after its timeout, and the due events are run again
    pay("o2", 7).await;

    // Then o2's late next is sent once, and no match next
    let late = (74, json!({ "order": "o2", "paid": 7 }));
    assert!(until(WAIT, || app.sent(w2, 0).len() > 1).await);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(app.sent(w2, 0), [timeout, late]);
    let sent = app.count("SELECT count(*) FROM _events WHERE ty = 72 AND status = 0");
    assert_eq!(sent, 1, "o1's match only");

    app.stop().await;
}

/// Not a scenario: #84. The hunt's resend of a run a crash dropped runs that run's flow on the
/// dropped event's data.
#[tokio::test(start_paused = true)]
async fn the_hunts_resend_runs_the_dropped_events_flow_on_its_data() {
    let _turn = TURN.lock().await;
    // Given a store with an event of the flow that uses as, whose 1 is an hour old with nothing
    // after it, as a crash leaves it
    let path = database("the_hunts_resend").unwrap();
    migrated(&path).await.unwrap();
    let seed = ready(&path).unwrap();
    let ago = |secs: i64| {
        let at = u64::try_from(clock::now().timestamp() - secs).unwrap();
        Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, at, 0))
    };
    let dropped = ago(7200);
    _events::factory()
        .id(dropped.to_string())
        .ty(12)
        .data(json!({ "text": "sam" }).to_string())
        .status(0)
        .insert(&seed)
        .unwrap();
    _events::factory()
        .id(ago(3600).to_string())
        .source(dropped.to_string())
        .ty(12)
        .data("\"taken\"")
        .status(1)
        .insert(&seed)
        .unwrap();

    // When the queue starts with a hunt that runs soon
    let settings = Settings {
        attempts: 1,
        window: Duration::from_mins(1),
        hunt: Duration::from_millis(300),
        ..Settings::new(Duration::from_millis(100))
    };
    let app = App::open_with(path, settings).await.unwrap();

    // Then the hunt resends the dropped event
    let resend = app.retry(dropped).await;
    assert_eq!(app.sent(dropped, 0), [(12, json!("hung"))]);

    // And rannoch-processor runs the resend's flow on the dropped event's data
    assert!(
        until(WAIT, || app
            .statuses(resend)
            .iter()
            .any(|s| *s == 2 || *s == -1))
        .await
    );
    assert_eq!(
        app.sent(resend, -1),
        Vec::<(i64, Value)>::new(),
        "the resend failed"
    );
    assert_eq!(
        app.sent(resend, 0),
        [(22, json!({ "shout": { "text": "SAM" }, "text": "sam" }))]
    );
    assert_eq!(app.statuses(resend), [1, 0, 2]);

    app.stop().await;
}

/// An id `secs` seconds ago.
fn ago(secs: i64) -> Uuid {
    let at = u64::try_from(clock::now().timestamp() - secs).unwrap_or_default();
    Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, at, 0))
}

/// A store at `path` holding o9's wait, as the order flow keeps it, two hours old with a 1 after
/// it, then each of `about` as (seconds ago, status, data) with the wait as its source; the
/// wait's id.
async fn waiting(path: &Path, about: &[(i64, i64, Value)]) -> Result<Uuid> {
    migrated(path).await?;
    let seed = ready(path)?;
    let wait = ago(7200);
    let data = json!({ "$wait": { "step": 0, "for": 71 }, "key": "o9", "vals": { "order": "o9" } });
    _events::factory()
        .id(wait.to_string())
        .ty(70)
        .data(data.to_string())
        .status(0)
        .insert(&seed)?;
    let taken = (5400, 1, json!("taken"));
    for (secs, status, data) in std::iter::once(&taken).chain(about) {
        _events::factory()
            .id(ago(*secs).to_string())
            .source(wait.to_string())
            .ty(70)
            .data(data.to_string())
            .status(*status)
            .insert(&seed)?;
    }
    Ok(wait)
}

/// Not a scenario: #91. The hunt's resend of a wait a crash dropped sends the wait's timeout.
#[tokio::test(start_paused = true)]
async fn a_wait_the_hunt_resends_sends_its_timeout_once_and_its_2_says_timeout() {
    let _turn = TURN.lock().await;
    // Given a store with o9's wait, whose 1 is on disk with nothing after it, as a crash leaves it
    let path = database("a_wait_the_hunt_resends").unwrap();
    let wait = waiting(&path, &[]).await.unwrap();

    // When the queue starts with a hunt that runs soon
    let settings = Settings {
        attempts: 1,
        window: Duration::from_mins(1),
        hunt: Duration::from_millis(300),
        ..Settings::new(Duration::from_millis(100))
    };
    let app = App::open_with(path, settings).await.unwrap();

    // Then the hunt resends the wait
    let resend = app.retry(wait).await;
    assert_eq!(app.sent(wait, 0).first(), Some(&(70, json!("hung"))));

    // And the resend sends the wait's timeout once, with the wait as its source, and its 2 says
    // "timeout"
    assert!(
        until(WAIT, || app
            .statuses(resend)
            .iter()
            .any(|s| *s == 2 || *s == -1))
        .await
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let timeout = (73, json!({ "order": "o9" }));
    assert_eq!(app.sent(wait, 0), [(70, json!("hung")), timeout]);
    assert_eq!(app.sent(resend, 2), [(70, json!("timeout"))]);
    assert_eq!(app.statuses(resend), [1, 2]);

    app.stop().await;
}

/// Not a scenario: #91. The queue's retry of a wait whose run failed sends the wait's timeout.
#[tokio::test(start_paused = true)]
async fn a_wait_the_queue_retries_sends_its_timeout_once_and_its_2_says_timeout() {
    let _turn = TURN.lock().await;
    // Given a store with o9's wait, whose run failed, and the queue's retry of it
    let path = database("a_wait_the_queue_retries").unwrap();
    let failed = json!("database is locked");
    let about = [(5000, -1, failed.clone()), (4800, 0, failed.clone())];
    let wait = waiting(&path, &about).await.unwrap();

    // When the queue starts and takes the retry
    let app = App::open(path, 3).await.unwrap();
    app.signal.send(()).unwrap();
    let retry = app.retry(wait).await;

    // Then the retry sends the wait's timeout once, with the wait as its source, and its 2 says
    // "timeout"
    assert!(
        until(WAIT, || app
            .statuses(retry)
            .iter()
            .any(|s| *s == 2 || *s == -1))
        .await
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let timeout = (73, json!({ "order": "o9" }));
    assert_eq!(app.sent(wait, 0), [(70, failed), timeout]);
    assert_eq!(app.sent(retry, 2), [(70, json!("timeout"))]);
    assert_eq!(app.statuses(retry), [1, 2]);

    app.stop().await;
}

/// Two payments for o9, `paid` the first's amount, each waited for until its 2; then the wait's
/// 0s, which must be `first`, the timeout and one late, for the first payment.
async fn pays_late(app: &App, wait: Uuid, first: (i64, Value), paid: u64) -> Result<()> {
    for amount in [paid, paid + 1] {
        let payment = app.hand(71, &json!({ "order": "o9", "amount": amount }))?;
        assert!(until(WAIT, || app.statuses(payment).contains(&2)).await);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (timeout, late) = (
        json!({ "order": "o9" }),
        json!({ "order": "o9", "paid": paid }),
    );
    assert_eq!(app.sent(wait, 0), [first, (73, timeout), (74, late)]);
    Ok(())
}

/// Not a scenario: #95. After the hunt's resend of a wait sends its timeout, the first match
/// sends the wait's late, once.
#[tokio::test(start_paused = true)]
async fn a_wait_the_hunt_resends_sends_late_on_the_first_match_after_its_timeout() {
    let _turn = TURN.lock().await;
    // Given a store with o9's wait, whose 1 is on disk with nothing after it, as a crash leaves it
    let path = database("a_wait_the_hunt_resends_late").unwrap();
    let wait = waiting(&path, &[]).await.unwrap();

    // And the hunt's resend of the wait has sent its timeout
    let settings = Settings {
        attempts: 1,
        window: Duration::from_mins(1),
        hunt: Duration::from_millis(300),
        ..Settings::new(Duration::from_millis(100))
    };
    let app = App::open_with(path, settings).await.unwrap();
    let resend = app.retry(wait).await;
    assert!(until(WAIT, || app.statuses(resend).contains(&2)).await);
    assert_eq!(app.sent(resend, 2), [(70, json!("timeout"))]);

    // When two payments for o9 arrive
    // Then the wait's late is sent once, for the first, with the wait as its source
    pays_late(&app, wait, (70, json!("hung")), 7).await.unwrap();

    app.stop().await;
}

/// Not a scenario: #95. After the queue's retry of a wait sends its timeout, the first match
/// sends the wait's late, once.
#[tokio::test(start_paused = true)]
async fn a_wait_the_queue_retries_sends_late_on_the_first_match_after_its_timeout() {
    let _turn = TURN.lock().await;
    // Given a store with o9's wait, whose run failed, and the queue's retry of it
    let path = database("a_wait_the_queue_retries_late").unwrap();
    let failed = json!("database is locked");
    let about = [(5000, -1, failed.clone()), (4800, 0, failed.clone())];
    let wait = waiting(&path, &about).await.unwrap();

    // And the retry has sent the wait's timeout
    let app = App::open(path, 3).await.unwrap();
    app.signal.send(()).unwrap();
    let retry = app.retry(wait).await;
    assert!(until(WAIT, || app.statuses(retry).contains(&2)).await);
    assert_eq!(app.sent(retry, 2), [(70, json!("timeout"))]);

    // When two payments for o9 arrive
    // Then the wait's late is sent once, for the first, with the wait as its source
    pays_late(&app, wait, (70, failed), 7).await.unwrap();

    app.stop().await;
}

/// Not a scenario: #97. A queue that starts over a store holding due events takes them without
/// a signal.
#[tokio::test(start_paused = true)]
async fn a_queue_that_starts_takes_the_events_already_due_without_a_signal() {
    let _turn = TURN.lock().await;
    // Given a store with two events at 0 with nothing after them, as a crash leaves them: one
    // with no run_at, and one whose run_at has passed
    let path = database("a_queue_that_starts").unwrap();
    migrated(&path).await.unwrap();
    let seed = ready(&path).unwrap();
    let (unset, passed) = (clock::new_id(), clock::new_id());
    let event = |id: Uuid| {
        let data = json!({ "ticket": id.to_string() }).to_string();
        _events::factory()
            .id(id.to_string())
            .ty(81)
            .data(data)
            .status(0)
    };
    event(unset).insert(&seed).unwrap();
    let past = (clock::now() - TimeDelta::hours(1)).to_rfc3339_opts(SecondsFormat::Millis, true);
    event(passed).run_at(past).insert(&seed).unwrap();

    // When the queue starts, and nothing signals it
    let settings = Settings {
        attempts: 1,
        ..Settings::new(Duration::from_millis(100))
    };
    let app = App::over(path, &built_in().unwrap(), |_, _| Vec::new(), settings)
        .await
        .unwrap();

    // Then it takes both, and rannoch-processor runs each
    for event in [unset, passed] {
        assert!(
            until(WAIT, || app.statuses(event) == [1, 2]).await,
            "{event} was not taken"
        );
    }

    app.stop().await;
}

/// FR-RANNOCH-124's flows, from the fixture `tests/built_in.json`, each with the name of its
/// file: a ticket's emit steps branch on its kind and wait for its answer, a payment stores its
/// row, a lost order stores into a table the store does not have, an invoice stores its row and
/// sends its receipt, and the flows their nexts reach. No action of the app's is in them.
const BUILT_IN: &str = include_str!("built_in.json");

/// `BUILT_IN` checked as the app's build checks it, with no signature of the app's: the flows to
/// compile in, or every error.
fn built_in() -> std::result::Result<String, Vec<String>> {
    let docs: Vec<(String, FlowDoc)> =
        serde_json::from_str(BUILT_IN).map_err(|e| vec![e.to_string()])?;
    check(&docs, &[])
}

/// The table the payment flow stores into, keyed by its order.
const PAYMENTS: &str = "CREATE TABLE payments (order_id TEXT PRIMARY KEY, amount INTEGER)";

#[tokio::test(start_paused = true)]
async fn a_flow_of_built_in_steps_runs_with_no_action_of_the_apps() {
    let _turn = TURN.lock().await;
    // Given an app whose flow has an emit step that branches on a field and an emit step that
    // waits for another event, and no action of its own
    // When the app's build checks the flow and an event of its ty is handed to it
    // Then the build passes
    let flows = built_in().unwrap();
    let app = App::built_in("a_flow_of_built_in_steps", &flows)
        .await
        .unwrap();
    let rush = app
        .hand(80, &json!({ "ticket": "t1", "kind": "rush" }))
        .unwrap();
    let slow = app
        .hand(80, &json!({ "ticket": "t2", "kind": "slow" }))
        .unwrap();
    let wait = |event| {
        app.about(event)
            .into_iter()
            .find(|r| r.1 == 80 && r.2 == 0)
            .unwrap()
            .0
    };
    // t1's answer is handed as soon as its wait is kept, well before the wait is due.
    assert!(until(WAIT, || app.statuses(rush).contains(&2)).await);
    let w1 = wait(rush);
    let answer = app.hand(83, &json!({ "ticket": "t1" })).unwrap();
    assert!(until(WAIT, || app.statuses(slow).contains(&2)).await);

    // the branch's next is sent for the case the field matches
    let next = |event, ty| app.sent(event, 0).into_iter().filter(move |n| n.0 == ty);
    assert_eq!(
        next(rush, 81).collect::<Vec<_>>(),
        [(81, json!({ "ticket": "t1" }))]
    );
    assert_eq!(
        next(slow, 82).collect::<Vec<_>>(),
        [(82, json!({ "ticket": "t2" }))]
    );
    assert_eq!(next(rush, 82).count() + next(slow, 81).count(), 0);

    // and the wait sends its next when the other event arrives
    let w2 = wait(slow);
    assert!(until(WAIT, || app.statuses(answer).contains(&2)).await);
    assert!(until(WAIT, || app.statuses(w1).contains(&2)).await);
    assert_eq!(app.sent(w1, 0), [(84, json!({ "ticket": "t1" }))]);
    assert_eq!(app.sent(w1, 2), [(80, json!("matched"))]);

    // and its timeout when none does
    assert!(until(WAIT, || app.statuses(w2).contains(&2)).await);
    assert_eq!(app.sent(w2, 0), [(85, json!({ "ticket": "t2" }))]);
    assert_eq!(app.sent(w2, 2), [(80, json!("timeout"))]);

    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn store_keeps_one_row_with_the_events_2() {
    let _turn = TURN.lock().await;
    // Given a table with a key, and a flow whose step stores a row shaped from the event's fields
    // into it
    let app = App::built_in("store_keeps_one_row", &built_in().unwrap())
        .await
        .unwrap();
    app.read.execute_batch(PAYMENTS).unwrap();

    // When events are handed to the flow, and one of them is run again
    let data = |i: i64| json!({ "order": format!("o{i}"), "amount": i * 10 });
    let paid: Vec<Uuid> = (0..5).map(|i| app.hand(86, &data(i)).unwrap()).collect();
    for event in &paid {
        assert!(until(WAIT, || app.statuses(*event).contains(&2)).await);
    }
    app.rerun(paid[0], 86, &data(0)).unwrap();
    assert!(until(WAIT, || app.statuses(paid[0]) == [1, 2, 2]).await);

    // Then every event that has its 2 has exactly its one row in the table, with the values its
    // fields gave
    let mut select = app
        .read
        .prepare("SELECT order_id, amount FROM payments")
        .unwrap();
    let rows = select
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    let rows: Vec<(String, i64)> = rows.collect::<rusqlite::Result<_>>().unwrap();
    drop(select);
    assert_eq!(
        rows,
        (0..5)
            .map(|i| (format!("o{i}"), i * 10))
            .collect::<Vec<_>>()
    );
    let without = "SELECT count(*) FROM _events e WHERE e.ty = 86 AND e.status = 0
        AND EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status = 2)
        AND (SELECT count(*) FROM payments WHERE order_id = json_extract(e.data, '$.order')) != 1";
    assert_eq!(app.count(without), 0);

    // and the event run again added none
    assert_eq!(app.count("SELECT count(*) FROM payments"), 5);

    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn store_refuses_a_table_it_does_not_have() {
    let _turn = TURN.lock().await;
    // Given a flow whose step stores a row into a table the store does not have
    let app = App::built_in("store_refuses_a_table", &built_in().unwrap())
        .await
        .unwrap();
    app.read.execute_batch(PAYMENTS).unwrap();

    // When an event is handed to the flow
    let lost = app.hand(87, &json!({ "order": "o1" })).unwrap();
    let later = app
        .hand(86, &json!({ "order": "o2", "amount": 5 }))
        .unwrap();

    // Then the event gets a -1 naming the table
    assert!(until(WAIT, || app.statuses(lost).contains(&-1)).await);
    assert_eq!(
        app.sent(lost, -1),
        [(87, json!("store: no such table: nope"))]
    );

    // and later events of the same inbox are written
    assert!(until(WAIT, || app.statuses(later).contains(&2)).await);
    let row = "SELECT count(*) FROM payments WHERE order_id = 'o2' AND amount = 5";
    assert_eq!(app.count(row), 1);

    app.stop().await;
}

/// Not a scenario: #102. An invoice whose row `payments` refuses gets, in place of its 2 and its
/// row, a -1 whose data is `reason`, and the receipt its step sent stands; a later invoice of the
/// same inbox is written.
async fn a_refused_row_gets_a_minus_1(
    name: &str,
    payments: &str,
    refused: &Value,
    reason: &str,
) -> Result<()> {
    // Given a table that refuses the first invoice's row, and a flow whose step stores into it
    // and sends a receipt
    let app = built_in_with(name, Config::default()).await?;
    app.read.execute_batch(payments)?;

    // When that invoice is handed to the flow, then one the table takes
    let first = app.hand(88, refused)?;
    let later = app.hand(88, &json!({ "order": "o2", "amount": 5 }))?;

    // Then the first gets a -1 naming the reason, and neither its 2 nor its row is written; the
    // receipt its step sent stands
    assert!(until(WAIT, || app.statuses(first).contains(&-1)).await);
    assert_eq!(app.sent(first, -1), [(88, json!(reason))]);
    assert_eq!(app.sent(first, 2), []);
    assert_eq!(
        app.count("SELECT count(*) FROM payments WHERE order_id = 'o1'"),
        0
    );
    assert_eq!(app.sent(first, 0), [(89, json!({ "order": "o1" }))]);

    // And the later invoice is written, with its 2, its row and its receipt
    assert!(until(WAIT, || app.statuses(later).contains(&2)).await);
    let row = "SELECT count(*) FROM payments WHERE order_id = 'o2' AND amount = 5";
    assert_eq!(app.count(row), 1);
    assert_eq!(app.sent(later, 0), [(89, json!({ "order": "o2" }))]);

    app.stop().await;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_row_that_breaks_not_null_gets_a_minus_1_and_no_2() {
    let _turn = TURN.lock().await;
    let payments = "CREATE TABLE payments (order_id TEXT PRIMARY KEY, amount INTEGER NOT NULL)";
    let absent = json!({ "order": "o1" });
    let reason = "store: NOT NULL constraint failed: payments.amount";
    a_refused_row_gets_a_minus_1("refused_not_null", payments, &absent, reason)
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_row_that_breaks_a_foreign_key_gets_a_minus_1_and_no_2() {
    let _turn = TURN.lock().await;
    let payments = format!("{ORDERS}; INSERT INTO orders (id) VALUES ('o2')");
    let unknown = json!({ "order": "o1", "amount": 5 });
    let reason = "store: FOREIGN KEY constraint failed";
    a_refused_row_gets_a_minus_1("refused_foreign_key", &payments, &unknown, reason)
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_number_above_i64_max_gets_a_minus_1_and_no_2() {
    let _turn = TURN.lock().await;
    let payments = "CREATE TABLE payments (order_id TEXT PRIMARY KEY, amount INTEGER NOT NULL)";
    let above = json!({ "order": "o1", "amount": u64::MAX });
    let reason = "store: 18446744073709551615 cannot be held exactly: payments.amount";
    a_refused_row_gets_a_minus_1("refused_above_i64_max", payments, &above, reason)
        .await
        .unwrap();
}

/// How many events wait their turn behind each other in #85's backlog.
const BACKLOG: i64 = 5_000;

/// The queue's settings for #85: one attempt, and the `window` and `hunt` given.
fn hunting(window: Duration, hunt: Duration) -> Settings {
    Settings {
        attempts: 1,
        window,
        hunt,
        ..Settings::new(Duration::from_millis(100))
    }
}

/// Not a scenario: #85. The hunt does not resend an event whose run the running queue still
/// holds: waiting its turn, going, or with its 2 not yet on disk.
#[tokio::test(start_paused = true)]
async fn the_hunt_does_not_resend_a_run_the_queue_still_holds() {
    let _turn = TURN.lock().await;
    // Given a store with a backlog of events of a flow of built-in steps, each at 0 with nothing
    // after it
    let path = database("the_hunt_leaves_held_runs").unwrap();
    migrated(&path).await.unwrap();
    let mut seed = ready(&path).unwrap();
    let tx = seed.transaction().unwrap();
    for i in 0..BACKLOG {
        let data = json!({ "ticket": format!("t{i}") }).to_string();
        let row = _events::factory().id(clock::new_id().to_string()).ty(82);
        row.data(data).status(0).insert(&tx).unwrap();
    }
    tx.commit().unwrap();

    // When the queue runs them one after another, its window and hunt far shorter than the
    // backlog
    let settings = hunting(Duration::from_millis(20), Duration::from_millis(60));
    let app = App::over(path, &built_in().unwrap(), |_, _| Vec::new(), settings)
        .await
        .unwrap();
    app.signal.send(()).unwrap();

    // Then each event runs once, to its 2, and the hunt resends none of them
    let twos = "SELECT count(*) FROM _events WHERE ty = 82 AND status = 2";
    assert!(until(Duration::from_secs(60), || app.count(twos) >= BACKLOG).await);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let hung = "SELECT count(*) FROM _events WHERE data = '\"hung\"'";
    assert_eq!(app.count(hung), 0, "the hunt resent runs the queue held");
    assert_eq!(app.count(twos), BACKLOG);

    app.stop().await;
}

/// Not a scenario: #85. The hunt still resends an event whose 1 is on disk with nothing after it
/// and no running queue holds, as a crash leaves it.
#[tokio::test(start_paused = true)]
async fn the_hunt_resends_an_event_a_crash_dropped() {
    let _turn = TURN.lock().await;
    // Given a store with an event of a flow of built-in steps whose 1 is an hour old with nothing
    // after it
    let path = database("the_hunt_resends_a_crash_dropped").unwrap();
    migrated(&path).await.unwrap();
    let seed = ready(&path).unwrap();
    let dropped = ago(7200);
    let data = json!({ "ticket": "t1" }).to_string();
    let event = _events::factory().id(dropped.to_string()).ty(82).data(data);
    event.status(0).insert(&seed).unwrap();
    let one = _events::factory()
        .id(ago(3600).to_string())
        .source(dropped.to_string());
    one.ty(82)
        .data("\"taken\"")
        .status(1)
        .insert(&seed)
        .unwrap();

    // When the queue starts with a hunt that runs soon
    let settings = hunting(Duration::from_mins(1), Duration::from_millis(300));
    let app = App::over(path, &built_in().unwrap(), |_, _| Vec::new(), settings)
        .await
        .unwrap();

    // Then the hunt resends it once, and the resend runs to its 2
    let resend = app.retry(dropped).await;
    assert!(until(WAIT, || app.statuses(resend) == [1, 2]).await);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(app.sent(dropped, 0), [(82, json!("hung"))]);

    app.stop().await;
}

// #92: the README's "What you can rely on" and "How it fails", each item through the real store,
// rannoch, `Events`, the queue and rannoch-processor, on `BUILT_IN`'s flows of `emit` and `store`.
// An item a scenario above already drives is not driven again. A batch written whole or not at
// all is driven here with `Events`: a payment whose row breaks a deferred foreign key, which
// SQLite checks only at commit, makes the README's batch that can never be written.
//
// rannoch-graph drives its own reads, why, capped walks and as-of reads over the app's tables,
// each read an event, in its scenarios.
//
// Not driven, and why:
// - A process that dies before its batch commits loses it, and a second process on the store:
//   each needs a second process (a death, or another process on the store); these scenarios run
//   in one. The first scenario below shows the events are not on disk until the commit; a second
//   app over the same file stands in for a second process, since rannoch takes no lock on the
//   file and SQLite locks per connection.
// - A heap with "no size limit", a hand-over tried again "with no limit", a kept signal sender
//   making `join` wait "for good": a scenario sees them for as long as it watches.
// - `join` waiting for a job, and the hunt leaving a job the running queue holds: an app gets a
//   job only from a job signature, and no built-in step has one.

impl App {
    /// Sends an event of `ty` with `data` through rannoch, as a producer does.
    fn send(&self, ty: u16, data: &Value) -> Result<()> {
        self.rannoch.sender().send(event(ty, data))?;
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

    /// Waits for the event sent with `data` to have its 2, and returns its id.
    async fn ran(&self, data: &Value) -> Uuid {
        let id = poll(WAIT, || self.root(data)).await.unwrap_or_default();
        assert!(
            until(WAIT, || self.statuses(id).contains(&2)).await,
            "{data} ran"
        );
        id
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
}

fn event(ty: u16, data: &Value) -> Event<Ty, Value> {
    let (id, data) = (Uuid::nil(), data.clone());
    let (source, run_at, status) = (None, None, Status::Added);
    Event {
        id,
        source,
        ty: Ty(ty),
        data,
        run_at,
        status,
    }
}

/// Polls `found` every 5 ms for up to `limit`: its first `Some`.
async fn poll<T>(limit: Duration, found: impl Fn() -> Option<T>) -> Option<T> {
    let start = clock::elapsed();
    loop {
        if let Some(t) = found() {
            return Some(t);
        }
        if start.elapsed() > limit {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The app over a fresh store `name` on `BUILT_IN`'s flows, rannoch's inboxes as `config` has
/// them.
async fn built_in_with(name: &str, config: Config<Ty, Value>) -> Result<App> {
    let flows = built_in().map_err(|e| e.join("\n"))?;
    App::built_in_at(database(name)?, &flows, config).await
}

/// A default inbox that hands over every `ms` milliseconds.
fn every(ms: i64) -> Config<Ty, Value> {
    Config {
        default_interval: TimeDelta::milliseconds(ms),
        ..Config::default()
    }
}

/// When a row was accepted: its v7 id's time.
fn accepted(id: Uuid) -> DateTime<Utc> {
    let (secs, nanos) = id.get_timestamp().map_or((0, 0), |t| t.to_unix());
    DateTime::from_timestamp(i64::try_from(secs).unwrap_or_default(), nanos).unwrap_or_default()
}

/// A payment two hours old whose 1 is `secs` old with nothing after it, as a crash leaves a run.
fn stall(conn: &Connection, data: &Value, secs: i64) -> Result<Uuid> {
    let id = ago(7200);
    let row = _events::factory().id(id.to_string()).ty(86);
    row.data(data.to_string()).status(0).insert(conn)?;
    let one = _events::factory().id(ago(secs).to_string()).ty(86);
    one.source(id.to_string())
        .data("\"taken\"")
        .status(1)
        .insert(conn)?;
    Ok(id)
}

const ROOTS: &str = "SELECT count(*) FROM _events WHERE source IS NULL";

/// Payments that must name an order the store has.
const ORDERS: &str = "CREATE TABLE orders (id TEXT PRIMARY KEY);
    CREATE TABLE payments (order_id TEXT PRIMARY KEY REFERENCES orders(id), amount INTEGER)";

/// Payments that must name an order the store has by the time their batch commits: a deferred
/// foreign key, which SQLite checks only at commit.
const DEFERRED: &str = "CREATE TABLE orders (id TEXT PRIMARY KEY);
    CREATE TABLE payments (order_id TEXT PRIMARY KEY
        REFERENCES orders(id) DEFERRABLE INITIALLY DEFERRED, amount INTEGER)";

/// The `error` lines rannoch logs, for the scenarios that read them.
struct Errors(Mutex<Vec<String>>);

impl log::Log for Errors {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Error
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            let mut lines = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            lines.push(record.args().to_string());
        }
    }
    fn flush(&self) {}
}

static ERRORS: Errors = Errors(Mutex::new(Vec::new()));

/// One scenario that reads the log at a time: every app in this binary logs to `ERRORS`.
static LOGGED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Holds `LOGGED`, with `ERRORS` taking rannoch's `error` lines, emptied.
async fn logged() -> tokio::sync::MutexGuard<'static, ()> {
    let one = LOGGED.lock().await;
    let _ = log::set_logger(&ERRORS);
    log::set_max_level(log::LevelFilter::Error);
    ERRORS
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clear();
    one
}

/// The failed hand-overs of the default inbox logged so far: how many events each held.
fn failed_hand_overs() -> Vec<usize> {
    let held = |line: &String| {
        let rest = line.strip_prefix("inbox 0 could not hand over ")?;
        rest.split(' ').next()?.parse().ok()
    };
    let lines = ERRORS.0.lock().unwrap_or_else(PoisonError::into_inner);
    lines.iter().filter_map(held).collect()
}

#[tokio::test(start_paused = true)]
async fn an_accepted_event_is_in_memory_until_its_inbox_hands_it_over() {
    let _turn = TURN.lock().await;
    // Given an app whose default inbox hands over every 400 ms; under Config::default, every 20
    assert_eq!(
        Config::<Ty, Value>::default().default_interval,
        TimeDelta::milliseconds(20)
    );
    let app = built_in_with("in_memory", every(400)).await.unwrap();
    let landed = || app.count(ROOTS);

    // When it sends three events
    for ticket in ["m1", "m2", "m3"] {
        app.send(82, &json!({ "ticket": ticket })).unwrap();
    }

    // Then none is in _events before the inbox's interval has passed
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(landed(), 0, "on the heap, not on disk");

    // And the inbox hands its whole heap over as one batch: the first read to see one sees three,
    // each at 0 with a v7 id
    assert_eq!(
        poll(WAIT, || Some(landed()).filter(|n| *n > 0)).await,
        Some(3)
    );
    let v7 = "SELECT count(*) FROM _events WHERE source IS NULL AND status = 0
        AND substr(id, 15, 1) = '7'";
    assert_eq!(app.count(v7), 3);

    // When it sends two more just after that hand-over
    for ticket in ["m4", "m5"] {
        app.send(82, &json!({ "ticket": ticket })).unwrap();
    }

    // Then they wait out the inbox's interval after it too, and are handed over together
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(landed(), 3);
    assert_eq!(
        poll(WAIT, || Some(landed()).filter(|n| *n > 3)).await,
        Some(5)
    );
    app.stop().await;

    // Given rannoch opened over a store with no append
    let path = database("no_append").unwrap();
    let store = migrated(&path).await.unwrap();
    let rannoch = Rannoch::<Ty, Value>::open(Config {
        store: Some(store),
        ..Config::default()
    })
    .unwrap();

    // When it is sent an event, then shut down
    let kept = event(82, &json!({ "ticket": "kept" }));
    rannoch.sender().send(kept).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown(rannoch).await;

    // Then nothing was handed over
    let all = ready(&path)
        .unwrap()
        .query_row(ROOTS, [], |r| r.get::<_, i64>(0));
    assert_eq!(all.unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_batch_the_store_never_takes_stops_its_inbox_and_is_lost_at_shutdown() {
    let _turn = TURN.lock().await;
    let _errors = logged().await;
    // Given an app whose payments must name an order the store has by the time their batch
    // commits, whose default inbox hands over every 300 ms, and whose queued tickets have an inbox
    // of their own
    let queued = InboxConfig {
        name: "queued".to_owned(),
        types: vec![Ty(82)],
        interval: TimeDelta::milliseconds(20),
    };
    let config = Config {
        inboxes: vec![queued],
        ..every(300)
    };
    let app = built_in_with("never_taken", config).await.unwrap();
    app.read.execute_batch(DEFERRED).unwrap();

    // When a payment for an order the store does not have runs
    let paid = json!({ "order": "o1", "amount": 5 });
    app.send(86, &paid).unwrap();
    let payment = poll(WAIT, || app.root(&paid)).await.unwrap();

    // Then the hand-over of the batch with its 2 and its row fails whole at commit, and is logged
    // at error
    assert!(until(WAIT, || !failed_hand_overs().is_empty()).await);
    assert_eq!(app.sent(payment, 2), []);
    assert_eq!(app.count("SELECT count(*) FROM payments"), 0);

    // When it sends two more events to the default inbox, and a queued ticket to its own
    let held = *failed_hand_overs().last().unwrap();
    for ticket in ["r1", "r2"] {
        app.send(81, &json!({ "ticket": ticket })).unwrap();
    }
    let queued = json!({ "ticket": "q1" });
    app.send(82, &queued).unwrap();

    // Then every later hand-over of the default inbox holds the failed batch and them, and
    // fails: its heap grows, and nothing more from it reaches the disk
    assert!(until(WAIT, || failed_hand_overs().last() == Some(&(held + 2))).await);
    let rushed = "SELECT count(*) FROM _events WHERE ty = 81";
    assert_eq!(app.count(rushed), 0);

    // And the other inbox still writes: the queued ticket is written and runs
    app.ran(&queued).await;

    // When it sends one more just after a failed hand-over, then shuts down
    let tried = failed_hand_overs().len();
    assert!(until(WAIT, || failed_hand_overs().len() > tried).await);
    let held = *failed_hand_overs().last().unwrap();
    app.send(81, &json!({ "ticket": "r3" })).unwrap();
    drop(app.signal);
    shutdown(app.rannoch).await;

    // Then shutdown's hand-over, which holds it, fails and is logged, and is not tried again
    assert_eq!(failed_hand_overs().last(), Some(&(held + 1)));
    let tried = failed_hand_overs().len();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(failed_hand_overs().len(), tried);
    assert_eq!(join(app.queue).await, Ok(()));

    // And the batch and every event after it are lost
    let read = ready(&app.path).unwrap();
    let count = |sql: &str| read.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
    assert_eq!(count(rushed), 0);
    let two = format!("SELECT count(*) FROM _events WHERE source = '{payment}' AND status = 2");
    assert_eq!(count(&two), 0);
}

#[tokio::test(start_paused = true)]
async fn a_failed_hand_over_is_tried_again_ahead_of_what_arrived_since() {
    let _turn = TURN.lock().await;
    let _errors = logged().await;
    // Given an app whose default inbox hands over every 300 ms, and another connection holding
    // the store's write lock past the append's busy timeout of 5 s
    let (told, handed) = std::sync::mpsc::channel();
    let (path, flows) = (database("tried_again").unwrap(), built_in().unwrap());
    let settings = Settings {
        attempts: 1,
        ..Settings::new(Duration::from_millis(100))
    };
    let config = (every(300), Some(told));
    let app = App::told(path, &flows, |_, _| Vec::new(), config, settings)
        .await
        .unwrap();
    let writer = ready(&app.path).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();

    // When it sends an event, and another while the first's hand-over waits on the lock: the
    // append tells of the batch it is handed before it waits, and the second is sent then, from a
    // thread of its own, since the waiting hand-over holds the test's. The check confirms it: no
    // hand-over has failed when the second is sent, and the one that fails holds the first alone.
    let (first, second) = (json!({ "ticket": "d" }), json!({ "ticket": "e" }));
    app.send(82, &first).unwrap();
    let (sender, sent) = (app.rannoch.sender(), second.clone());
    let sending = tokio::task::spawn_blocking(move || {
        handed.recv().unwrap();
        sender.send(event(82, &sent)).unwrap();
        failed_hand_overs()
    });
    // The thread waiting on the append stops tokio's auto-advance, so the clock is moved here.
    assert!(advancing(WAIT, || sending.is_finished()).await);
    let none = Vec::<usize>::new();
    assert_eq!(
        sending.await.unwrap(),
        none,
        "the first's hand-over still waits"
    );

    // Then that hand-over fails holding the first alone, and is logged at error
    assert!(until(Duration::from_secs(10), || !failed_hand_overs().is_empty()).await);
    assert_eq!(failed_hand_overs(), [1]);

    // And once the lock is released, the inbox tries again at its next interval: both are
    // written in one hand-over, the failed batch ahead of what arrived since
    writer.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        poll(WAIT, || Some(app.count(ROOTS)).filter(|n| *n > 0)).await,
        Some(2)
    );
    let sql = "SELECT json_extract(data, '$.ticket') FROM _events WHERE source IS NULL
        ORDER BY rowid";
    let order: Vec<String> = app
        .read
        .prepare(sql)
        .and_then(|mut s| s.query_map([], |r| r.get(0))?.collect())
        .unwrap();
    assert_eq!(order, ["d", "e"]);

    // And as they run, the rows already in _events are only added to
    let written = app.rows();
    assert_ne!(written, Vec::<String>::new());
    app.ran(&first).await;
    app.ran(&second).await;
    let rows = app.rows();
    assert!(rows.len() > written.len() && rows.starts_with(&written));
    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_queue_takes_each_due_event_once() {
    let _turn = TURN.lock().await;
    // Given an app whose default inbox hands over every 300 ms, so each 1 is on the heap a while
    let app = built_in_with("taken_once", every(300)).await.unwrap();

    // When it sends ten events, and the queue is woken every 10 ms until each has its 2
    let events: Vec<Value> = (0..10)
        .map(|i| json!({ "ticket": format!("once{i}") }))
        .collect();
    for data in &events {
        app.send(82, data).unwrap();
    }
    let mut ids: Vec<Uuid> = Vec::new();
    for data in &events {
        ids.extend(poll(WAIT, || app.root(data)).await);
    }
    assert_eq!(ids.len(), 10);
    let start = clock::elapsed();
    while !ids.iter().all(|id| app.statuses(*id).contains(&2)) {
        app.signal.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(start.elapsed() < WAIT, "each ran");
    }

    // Then each was taken once: one 1, then its 2
    for id in &ids {
        assert_eq!(app.statuses(*id), [1, 2]);
    }
    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_hunt_runs_a_hunt_after_the_queue_starts_then_every_hunt_on_the_stalled_events_data() {
    let _turn = TURN.lock().await;
    // Given the queue's defaults: a 20-minute window and an hour between hunts
    let defaults = Settings::new(Duration::from_millis(100));
    assert_eq!(
        (defaults.window, defaults.hunt),
        (Duration::from_mins(20), Duration::from_hours(1))
    );

    // And a store as a crash leaves it: a payment whose 1 is an hour old with nothing after it,
    // and one whose 1 is 30 s old
    let path = database("hunt_every").unwrap();
    migrated(&path).await.unwrap();
    let seed = ready(&path).unwrap();
    seed.execute_batch(PAYMENTS).unwrap();
    let stalled = stall(&seed, &json!({ "order": "o1", "amount": 10 }), 3600).unwrap();
    let young = stall(&seed, &json!({ "order": "o3", "amount": 30 }), 30).unwrap();

    // When the queue starts with a window of a minute and a hunt every 2 s, and is woken before
    // the first hunt
    let settings = hunting(Duration::from_mins(1), Duration::from_secs(2));
    let started = clock::elapsed();
    let app = App::over(path, &built_in().unwrap(), |_, _| Vec::new(), settings)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    app.signal.send(()).unwrap();

    // Then nothing is resent before the first hunt, a hunt after the start
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(app.sent(stalled, 0), []);
    let resend = app.retry(stalled).await;
    assert!(started.elapsed() >= Duration::from_secs(2));
    assert_eq!(app.sent(stalled, 0), [(86, json!("hung"))]);

    // And the resend runs the payment's flow on the stalled payment's data
    assert!(until(WAIT, || app.statuses(resend) == [1, 2]).await);
    let row = "SELECT count(*) FROM payments WHERE order_id = 'o1' AND amount = 10";
    assert_eq!(app.count(row), 1);

    // When another payment is left stalled after that hunt, and the queue is woken
    let later = stall(&seed, &json!({ "order": "o2", "amount": 20 }), 3600).unwrap();
    app.signal.send(()).unwrap();

    // Then the wake does not hunt: the next hunt, a hunt after the first, resends it
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(app.sent(later, 0), []);
    app.retry(later).await;
    assert!(started.elapsed() >= Duration::from_secs(4));
    assert_eq!(app.sent(later, 0), [(86, json!("hung"))]);

    // And neither hunt resent the payment whose 1 is younger than the window
    assert_eq!(app.sent(young, 0), []);
    app.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_run_is_retried_with_backoff_and_an_event_with_no_flow_gets_no_processor() {
    let _turn = TURN.lock().await;
    // Given the queue's default of 10 attempts
    assert_eq!(Settings::new(Duration::from_millis(100)).attempts, 10);

    // And an app whose queue gives a chain 3 attempts, the first retry 200 ms after a failure
    let settings = Settings {
        attempts: 3,
        ..Settings::new(Duration::from_millis(200))
    };
    let (path, flows) = (database("retried").unwrap(), built_in().unwrap());
    let app = App::over(path, &flows, |_, _| Vec::new(), settings)
        .await
        .unwrap();

    // When it sends an order lost to a table the store does not have, and an event of ty 99,
    // which no flow runs
    let (lost, orphan) = (json!({ "order": "o1" }), json!({ "ticket": "t99" }));
    app.send(87, &lost).unwrap();
    app.send(99, &orphan).unwrap();

    // Then each failed run's step sends a -1 whose data is its message, and the queue sends a
    // retry: a new event at 0 whose source is the failed run and whose data is the failure, due
    // 200 ms after the first failure and twice that after the second
    let failed = json!("store: no such table: nope");
    let mut run = poll(WAIT, || app.root(&lost)).await.unwrap();
    let mut written = Vec::new();
    for wait in [200, 400] {
        let retry = app.retry(run).await;
        let rows = app.about(run);
        let one = |status| rows.iter().find(|r| r.2 == status).cloned().unwrap();
        let (binned, due) = (one(-1), one(0));
        assert_eq!((binned.1, &binned.3), (87, &failed));
        assert_eq!((due.0, due.1, &due.3), (retry, 87, &failed));
        let at: DateTime<Utc> = due.4.unwrap().parse().unwrap();
        let (wait, slack) = (TimeDelta::milliseconds(wait), TimeDelta::milliseconds(5));
        assert!(at - accepted(binned.0) >= wait - slack, "{wait} after");
        assert!(at - accepted(retry) <= wait + slack, "{wait} after");
        written = app.rows();
        run = retry;
    }

    // And the third failure sends its -1 and no retry: the chain has had its 3 attempts
    assert!(until(WAIT, || app.statuses(run).contains(&-1)).await);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(app.statuses(run), [1, -1]);
    assert_ne!(written, Vec::<String>::new());
    assert!(app.rows().starts_with(&written), "rows are only added");

    // And the ty 99 event got a -1 saying "no processor", and no retry
    let other = app.root(&orphan).unwrap();
    let rows: Vec<(i64, Value)> = app.about(other).into_iter().map(|r| (r.2, r.3)).collect();
    assert_eq!(rows, [(1, json!("taken")), (-1, json!("no processor"))]);
    app.stop().await;
}

/// Advances the clock 20 ms at a time, up to `limit`, until `done`. A run held on a blocking
/// thread stops tokio's auto-advance, so a scenario that holds one moves the clock itself.
async fn advancing(limit: Duration, done: impl Fn() -> bool) -> bool {
    let start = clock::elapsed();
    while start.elapsed() < limit {
        if done() {
            return true;
        }
        tokio::time::advance(Duration::from_millis(20)).await;
    }
    done()
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::await_holding_lock,
    reason = "the writes are held across the scenario's waits, so the runs cannot finish"
)]
async fn after_shutdown_a_send_is_closed_and_join_waits_for_signal_senders_and_runs() {
    let _turn = TURN.lock().await;
    // Given an app whose payment flow's step stores each payment's row, and the processor's writes
    // held, so a payment's run cannot put its row there and finish until they are let go
    let app = App::built_in("after_shutdown", &built_in().unwrap())
        .await
        .unwrap();
    app.read.execute_batch(PAYMENTS).unwrap();
    let writes = app.processor.writes();
    let holding = writes.lock().unwrap();

    // And payments the queue has taken: each 1 is on disk, so each run is the queue's, held
    let data = |i: i64| json!({ "order": format!("o{i}"), "amount": i * 10 });
    let paid: Vec<Uuid> = (0..3).map(|i| app.hand(86, &data(i)).unwrap()).collect();
    for event in &paid {
        assert!(advancing(WAIT, || app.statuses(*event) == [1]).await);
    }

    // When the app shuts rannoch down while they run, and drops its signal sender
    let sender = app.rannoch.sender();
    shutdown(app.rannoch).await;
    drop(app.signal);
    let at_drop = holding.len();

    // Then a send returns Closed
    let late = sender.send(event(82, &json!({ "ticket": "late" })));
    assert_eq!(late, Err(Error::Closed));

    // And join does not return while the runs the queue started are going
    let joining = tokio::task::spawn_blocking(move || app.queue.join());
    tokio::time::advance(Duration::from_millis(300)).await;
    assert!(
        !joining.is_finished(),
        "a run still going keeps join waiting"
    );

    // And returns once they have finished: each ran after the drop and none after join, each
    // keeping the row its 2 could not carry
    drop(holding);
    assert_eq!(joining.await.unwrap(), Ok(()));
    let held = || writes.lock().map_or(0, |w| w.len());
    let at_join = held();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(held(), at_join, "a run went on after join returned");
    assert_eq!(
        (at_drop, at_join),
        (0, paid.len()),
        "the runs ran after the drop"
    );

    // Given another app, with nothing running, that shuts rannoch down and keeps its signal
    // sender
    let app = App::built_in("kept_signal", &built_in().unwrap())
        .await
        .unwrap();
    shutdown(app.rannoch).await;

    // Then join does not return while the app keeps it, and returns once it is dropped
    let joining = tokio::task::spawn_blocking(move || app.queue.join());
    tokio::time::advance(Duration::from_millis(300)).await;
    assert!(
        !joining.is_finished(),
        "a kept signal sender keeps join waiting"
    );
    drop(app.signal);
    assert_eq!(joining.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn a_second_app_on_the_same_store_is_not_stopped() {
    let _turn = TURN.lock().await;
    // Given an app whose default inbox hands over every second
    let path = database("second_app").unwrap();
    let open = async || {
        let flows = built_in().unwrap();
        App::built_in_at(path.clone(), &flows, every(1000))
            .await
            .unwrap()
    };
    let first = open().await;

    // When a second app opens rannoch and starts its queue over the same store
    // Then nothing stops it
    let second = open().await;

    // When an event is written, and both queues are woken while the first's 1 is on its heap
    let ticket = first.hand(82, &json!({ "ticket": "t6" })).unwrap();
    second.signal.send(()).unwrap();

    // Then both take it, and both run its flow
    assert!(until(WAIT, || first.sent(ticket, 2).len() == 2).await);
    let mut statuses = first.statuses(ticket);
    statuses.sort_unstable();
    assert_eq!(statuses, [1, 1, 2, 2]);
    second.stop().await;
    first.stop().await;
}

/// #109's flows on the built-in steps: a list whose emit step sends one next per item of
/// `items`, a later one whose next is due more milliseconds on than a time can hold, an asked one
/// whose wait for its reply sends one next per item of the reply's `items`, the reply, and the
/// flow their nexts reach.
const FAILING: &str = r#"[
  ["list.json", {"name": "list", "ty": 90, "takes": {"items": "list<string>"},
    "steps": [{"do": "emit", "next": {"ty": 91, "each": "items", "shape": {"item": "$item"}}}]}],
  ["later.json", {"name": "later", "ty": 92, "takes": {},
    "steps": [{"do": "emit", "next": {"ty": 91, "delay_ms": 18446744073709551615,
      "shape": {"item": "x"}}}]}],
  ["asked.json", {"name": "asked", "ty": 93, "takes": {"ticket": "string"},
    "steps": [{"do": "emit", "next": {"wait": {"for": 94, "key": "$ticket", "within_ms": 60000,
      "next": {"ty": 91, "each": "got.items", "shape": {"item": "$item"}},
      "timeout": {"ty": 91, "shape": {"item": "none"}}}}}]}],
  ["reply.json", {"name": "reply", "ty": 94,
    "takes": {"ticket": "string", "items": "list<string>"}, "steps": []}],
  ["item.json", {"name": "item", "ty": 91, "takes": {"item": "string"}, "steps": []}]
]"#;

/// `BUILT_IN`'s flows and `FAILING`'s, checked with no signature of the app's.
fn failing() -> Result<String> {
    let docs = |json| serde_json::from_str::<Vec<(String, FlowDoc)>>(json);
    let docs = [docs(BUILT_IN)?, docs(FAILING)?].concat();
    check(&docs, &[]).map_err(|e| e.join("\n").into())
}

/// The data of each -1 about `event` once its flow has run, which must have sent no next and
/// no 2.
async fn failed(app: &App, event: Uuid) -> Vec<Value> {
    let ran = until(WAIT, || app.statuses(event).contains(&-1)).await;
    assert!(ran, "{:?}", app.about(event));
    assert_eq!(app.statuses(event), [1, -1]);
    app.sent(event, -1).into_iter().map(|r| r.1).collect()
}

/// Not a scenario: #109. A next due further on than a time can hold fails its step.
#[tokio::test(start_paused = true)]
async fn a_next_delayed_past_the_last_time_fails_its_step() {
    let _turn = TURN.lock().await;
    // Given a flow whose emit step sends a next with a delay_ms of u64::MAX
    let app = App::built_in("a_next_delayed_past", &failing().unwrap())
        .await
        .unwrap();

    // When an event is handed to it
    let event = app.hand(92, &json!({})).unwrap();

    // Then its -1 names the delay, and no next is sent
    let delay = "a delay of 18446744073709551615 ms to ty 91 is out of range";
    assert_eq!(failed(&app, event).await, [json!(delay)]);
    app.stop().await;
}

/// Not a scenario: #109. A due wait whose `$wait` names no ty fails its step.
#[tokio::test(start_paused = true)]
async fn a_due_wait_whose_for_is_not_a_ty_fails_its_step() {
    let _turn = TURN.lock().await;
    // Given the ticket flow, which waits for its answer
    let app = App::built_in("a_due_wait_whose_for", &failing().unwrap())
        .await
        .unwrap();

    // When a due wait of it is handed over whose $wait is for 70000, which is no u16
    let data = json!({ "$wait": { "step": 1, "for": 70000 }, "key": "t1",
        "vals": { "ticket": "t1" } });
    let wait = app.hand(80, &data).unwrap();

    // Then its -1 names the for, and it sends no timeout
    assert_eq!(
        failed(&app, wait).await,
        [json!("$wait for 70000 is not a ty")]
    );
    app.stop().await;
}

/// Not a scenario: #109. An each over a field that is absent or not a list fails its step.
#[tokio::test(start_paused = true)]
async fn each_over_a_field_that_is_not_a_list_fails_its_step() {
    let _turn = TURN.lock().await;
    // Given a flow whose emit step sends one next per item of items
    let app = App::built_in("each_over_a_field", &failing().unwrap())
        .await
        .unwrap();

    // When events are handed to it whose items is absent, and a string
    for data in [json!({}), json!({ "items": "a" })] {
        let event = app.hand(90, &data).unwrap();

        // Then each one's -1 names the field, and no next is sent
        let reason = json!("each over items, which is not a list");
        assert_eq!(failed(&app, event).await, [reason]);
    }
    app.stop().await;
}

/// Not a scenario: #109. A reply whose wait's next cannot be shaped from it fails, and answers
/// the wait all the same, so a later reply with the same key runs.
#[tokio::test(start_paused = true)]
async fn a_reply_a_waits_next_cannot_be_shaped_from_fails_once_and_a_later_reply_runs() {
    let _turn = TURN.lock().await;
    // Given t1's asked event, whose wait for its reply sends one next per item of its items
    let app = App::built_in("a_reply_a_waits_next", &failing().unwrap())
        .await
        .unwrap();
    let asked = app.hand(93, &json!({ "ticket": "t1" })).unwrap();
    assert!(until(WAIT, || app.statuses(asked).contains(&2)).await);
    let wait = app.about(asked).into_iter().find(|r| r.1 == 93 && r.2 == 0);
    let wait = wait.unwrap().0;

    // When t1's reply is handed over with no items
    let reply = json!({ "ticket": "t1" });
    let first = app.hand(94, &reply).unwrap();

    // Then its -1 names the wait and the field its next cannot be shaped from
    let reason = format!("wait {wait}: each over got.items, which is not a list");
    assert_eq!(failed(&app, first).await, [json!(reason)]);

    // When a later reply for t1 is handed over, also with no items
    let later = app.hand(94, &reply).unwrap();

    // Then it runs as if the wait were not there, and the wait sends nothing
    assert!(until(WAIT, || app.statuses(later).contains(&2)).await);
    assert_eq!(app.statuses(later), [1, 2]);
    assert_eq!(app.sent(wait, 0), Vec::<(i64, Value)>::new());
    app.stop().await;
}

/// Every `.rs` file of rannoch-processor's code and tests, the test apps' among them, with its
/// path.
fn sources() -> std::io::Result<Vec<(PathBuf, String)>> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (mut files, mut dirs) = (
        Vec::new(),
        vec![crate_dir.join("src"), crate_dir.join("tests")],
    );
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = fs::read_to_string(&path)?;
                files.push((path, text));
            }
        }
    }
    Ok(files)
}

#[tokio::test(start_paused = true)]
async fn rannoch_processor_calls_no_real_time_function() {
    // Given rannoch-processor's code and tests
    let files = sources().unwrap();
    assert!(files.len() >= 9, "{files:?}");

    // When they are read for a call to any real-time function; the opening paren is added here,
    // so this list is not itself a call
    let names = [
        "thread::sleep",
        "thread::spawn",
        "recv_timeout",
        "Instant::now",
        "SystemTime::now",
        "Utc::now",
        "now_v7",
    ];
    let calls: Vec<String> = names.iter().map(|name| format!("{name}(")).collect();

    // Then none is found
    for (path, text) in &files {
        for call in &calls {
            assert!(
                !text.contains(call.as_str()),
                "{} calls {call}",
                path.display()
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn every_rannoch_processor_test_runs_with_the_clock_frozen() {
    // Given rannoch-processor's tests
    let files = sources().unwrap();

    // When they are read for how each one runs
    let mut runs = 0;
    for (path, text) in &files {
        for line in text.lines().map(str::trim) {
            // Then none runs as a plain test, and each tokio test or main freezes the clock
            assert_ne!(line, "#[test]", "{} has a plain test", path.display());
            let tokio_run = line.starts_with("#[tokio::test") || line.starts_with("#[tokio::main");
            if tokio_run {
                assert!(
                    line.ends_with("start_paused = true)]"),
                    "{}: {line}",
                    path.display()
                );
                runs += 1;
            }
        }
    }
    // 17 in lib.rs, 15 in check.rs, 3 in doc.rs, 37 here and pay-app's main
    assert_eq!(runs, 73);
}
