//! FR-RANNOCH-119's scenarios, from `tests/features/lib.feature`: "rannoch-processor runs the
//! flow for an event the queue hands it" (`@criterion-FR-RANNOCH-119-c3` to `-c5`), "A failing
//! link fails the step, and the retry reruns the chain" (`-c6`), "branch and each send their
//! nexts" (`-c7`), "An action's real output is
//! checked at run time" (`-c14`) and "A two-step flow sends its next only when every step
//! succeeds" (`-c11`, `-c13`), "The app's build checks its flow docs" (`-c1`, `-c2`, `-c12`,
//! `-c16`) and "The app's build gathers the signatures a separate processor crate exports"
//! (`-c10`). The store is the app's database opened through the real store, its events from the
//! app's forgr factory, handed over by the real queue; the app's flow docs are compiled in from
//! `tests/flows`, and the test processor crate's actions are fakes. The build scenarios build a
//! test app from `tests/app` with a nested cargo: its build.rs gathers the signatures of the
//! separate processor crate proc-text and its own, and checks each flow doc set in
//! `tests/app/flows`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, thread};

use chrono::{DateTime, TimeDelta, Utc};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_processor::{Action, Processor, Signature};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use uuid::Uuid;

/// The app's `ty`, as its `u16`.
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

/// The app's flow docs, compiled in.
const FLOWS: [&str; 14] = [
    include_str!("flows/10-greet.json"),
    include_str!("flows/12-shout.json"),
    include_str!("flows/15-limited.json"),
    include_str!("flows/20-notice.json"),
    include_str!("flows/22-shouted.json"),
    include_str!("flows/30-rate.json"),
    include_str!("flows/31-rated.json"),
    include_str!("flows/32-question.json"),
    include_str!("flows/33-else.json"),
    include_str!("flows/40-two.json"),
    include_str!("flows/41-item.json"),
    include_str!("flows/50-value.json"),
    include_str!("flows/51-level.json"),
    include_str!("flows/52-list.json"),
];

/// Every link run, in order: the action and the input it was passed.
type Calls = Arc<Mutex<Vec<(String, Map<String, Value>)>>>;

/// What the fake `rate` gives.
type Rating = Arc<Mutex<Map<String, Value>>>;

/// What a fake action does with its options and input.
type Act =
    Box<dyn Fn(&Value, &Map<String, Value>) -> Result<Map<String, Value>, String> + Send + Sync>;

/// A test processor crate's action: records each run and gives what `act` makes of it.
struct Fake {
    sig: Signature,
    calls: Calls,
    act: Act,
}

impl Action for Fake {
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String> {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push((self.sig.action.clone(), input.clone()));
        }
        (self.act)(options, input)
    }
}

fn obj(v: Value) -> Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => Map::new(),
    }
}

fn text(input: &Map<String, Value>) -> String {
    input
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The test processor crate: trim, upper, greet, limit, note, rate and classify, which fails on
/// its first run and succeeds on every run after.
fn actions(calls: &Calls, rating: &Rating) -> Vec<Arc<dyn Action>> {
    let fake = |name: &str, gives: Value, act: Act| -> Arc<dyn Action> {
        let gives = obj(gives)
            .into_iter()
            .map(|(field, ty)| (field, ty.as_str().unwrap_or_default().to_owned()))
            .collect();
        Arc::new(Fake {
            sig: Signature {
                action: name.to_owned(),
                gives,
                ..Signature::default()
            },
            calls: calls.clone(),
            act,
        })
    };
    let rating = rating.clone();
    let runs = Arc::new(AtomicUsize::new(0));
    vec![
        fake(
            "trim",
            json!({ "text": "string" }),
            Box::new(|_, i| Ok(obj(json!({ "text": text(i).trim() })))),
        ),
        fake(
            "upper",
            json!({ "text": "string" }),
            Box::new(|_, i| Ok(obj(json!({ "text": text(i).to_uppercase() })))),
        ),
        fake(
            "greet",
            json!({ "greeting": "string" }),
            Box::new(|o, i| {
                let hi = o["salutation"].as_str().unwrap_or_default();
                Ok(obj(json!({ "greeting": format!("{hi} {}", text(i)) })))
            }),
        ),
        fake(
            "limit",
            json!({}),
            Box::new(|o, i| {
                let max = o["max"].as_u64().unwrap_or_default();
                let len = text(i).len();
                if u64::try_from(len).unwrap_or(u64::MAX) > max {
                    return Err(format!("limit: text is {len} long, max {max}"));
                }
                Ok(Map::new())
            }),
        ),
        fake("note", json!({}), Box::new(|_, _| Ok(Map::new()))),
        fake(
            "rate",
            json!({ "severity": "number" }),
            Box::new(move |_, _| Ok(rating.lock().map_or_default(|r| r.clone()))),
        ),
        fake(
            "classify",
            json!({ "category": "string" }),
            Box::new(move |_, _| {
                if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("classify: no answer".to_owned());
                }
                Ok(obj(json!({ "category": "bug" })))
            }),
        ),
    ]
}

type Try<T> = Result<T, Box<dyn std::error::Error>>;

fn database(name: &str) -> Try<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir.join("store.db"))
}

fn ready(path: &Path) -> Try<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// The app: its store, rannoch, the queue handing events to rannoch-processor, and what its
/// fake actions saw.
struct App {
    path: PathBuf,
    read: Connection,
    signal: mpsc::Sender<()>,
    rannoch: Rannoch<Ty, Value>,
    queue: Queue,
    calls: Calls,
    rating: Rating,
}

impl App {
    fn start(name: &str, settings: Settings) -> Try<Self> {
        let path = database(name)?;
        let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
        Rannoch::<Ty, Value>::open(Config {
            store: Some(store.clone()),
            ..Config::default()
        })?
        .shutdown();
        let (calls, rating) = (Calls::default(), Rating::default());
        let processor: Processor<Ty, Value> = Processor::new(&FLOWS, actions(&calls, &rating))?;
        let (signal, wake) = mpsc::channel();
        let rannoch = Rannoch::open(Config {
            store: Some(store),
            append: Some(
                Arc::new(Events::new(ready(&path)?, signal.clone())) as Arc<dyn Append<Ty, Value>>
            ),
            ..Config::default()
        })?;
        let queue = Queue::start(
            ready(&path)?,
            wake,
            rannoch.sender(),
            Arc::new(processor),
            settings,
        );
        Ok(Self {
            read: ready(&path)?,
            path,
            signal,
            rannoch,
            queue,
            calls,
            rating,
        })
    }

    /// An event of `ty` with `data` from the app's forgr factory, and the queue signalled.
    fn hand(&self, ty: i64, data: &Value) -> Try<Uuid> {
        let id = Uuid::now_v7();
        _events::factory()
            .id(id.to_string())
            .ty(ty)
            .data(data.to_string())
            .status(0)
            .insert(&ready(&self.path)?)?;
        self.signal.send(())?;
        Ok(id)
    }

    /// The rows whose source is `id`.
    fn about(&self, id: Uuid) -> rusqlite::Result<Vec<About>> {
        self.read
            .prepare(
                "SELECT id, ty, status, data, run_at FROM _events WHERE source = ?1 ORDER BY id",
            )?
            .query_map([id.to_string()], |r| {
                let id: String = r.get(0)?;
                let data: String = r.get(3)?;
                Ok(About {
                    id: id.parse().unwrap_or_default(),
                    ty: r.get(1)?,
                    status: r.get(2)?,
                    data: serde_json::from_str(&data).unwrap_or(Value::Null),
                    run_at: r.get(4)?,
                })
            })?
            .collect()
    }

    fn statuses(&self, id: Uuid) -> Vec<i64> {
        self.about(id)
            .unwrap_or_default()
            .iter()
            .map(|r| r.status)
            .collect()
    }

    fn count(&self) -> rusqlite::Result<i64> {
        self.read
            .query_row("SELECT count(*) FROM _events", [], |r| r.get(0))
    }

    fn calls(&self) -> Vec<(String, Map<String, Value>)> {
        self.calls.lock().map_or_default(|c| c.clone())
    }

    fn stop(self) {
        drop(self.signal);
        self.rannoch.shutdown();
        self.queue.join();
    }
}

/// One row about an event.
#[derive(Debug)]
struct About {
    id: Uuid,
    ty: i64,
    status: i64,
    data: Value,
    run_at: Option<String>,
}

/// Waits up to `limit` for `done`.
fn until(limit: Duration, done: impl Fn() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < limit {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    done()
}

const WAIT: Duration = Duration::from_secs(5);

fn names(calls: &[(String, Map<String, Value>)]) -> Vec<&str> {
    calls.iter().map(|(name, _)| name.as_str()).collect()
}

#[test]
fn rannoch_processor_runs_the_flow_for_an_event_the_queue_hands_it() {
    // Given an app built with a flow whose step is the chain trim.upper.greet, from a test
    // processor crate, ending in a next with a delay of 500 whose shape fills who with or and
    // builds msg with $concat
    // And a flow for the receiving ty whose step has no next
    // And a flow whose step puts its result under a field with as
    let app = App::start(
        "rannoch_processor_runs_the_flow",
        Settings::new(Duration::from_millis(100)),
    )
    .unwrap();

    // When the queue hands rannoch-processor an event of the chain's ty
    // run_at is kept to the millisecond
    let before = Utc::now() - TimeDelta::milliseconds(1);
    let event = app
        .hand(10, &json!({ "text": "  sam ", "lang": "en" }))
        .unwrap();

    // Then trim, upper and greet run on the one event, with no event written between them
    assert!(until(WAIT, || app.statuses(event).contains(&2)));
    let after = Utc::now();
    let calls = app.calls();
    assert_eq!(names(&calls), ["trim", "upper", "greet"]);
    assert_eq!(app.statuses(event), [1, 0, 2]);
    assert_eq!(
        app.count().unwrap(),
        4,
        "the event, its 1, one next and one 2"
    );

    // And each link reads the root's data, and is passed the options the links before it gave,
    // the later winning
    for (_, input) in &calls {
        assert_eq!(input["lang"], json!("en"), "{input:?}");
    }
    assert_eq!(calls[0].1["text"], json!("  sam "));
    assert_eq!(calls[1].1["text"], json!("sam"));
    assert_eq!(calls[2].1["text"], json!("SAM"));

    // And exactly one next is written, with the processor's 2: data msg 'hi SAM!' and who
    // 'friend', source the event, status 0, run_at the time of the run plus 500
    let rows = app.about(event).unwrap();
    let nexts: Vec<&About> = rows.iter().filter(|r| r.status == 0).collect();
    assert_eq!(nexts.len(), 1);
    let next = nexts[0];
    assert_eq!(next.ty, 20);
    assert_eq!(next.data, json!({ "msg": "hi SAM!", "who": "friend" }));
    let run_at: DateTime<Utc> = next.run_at.as_deref().unwrap().parse().unwrap();
    let delay = TimeDelta::milliseconds(500);
    assert!(
        run_at >= before + delay && run_at <= after + delay,
        "{run_at} is not the run plus 500"
    );
    assert!(rows.iter().any(|r| r.status == 2));

    // When the queue hands rannoch-processor the next
    // Then its flow ends with the processor's 2 and nothing more is sent
    let next = next.id;
    assert!(until(WAIT, || app.statuses(next).contains(&2)));
    thread::sleep(Duration::from_millis(200));
    assert_eq!(app.statuses(next), [1, 2]);
    assert_eq!(app.count().unwrap(), 6, "nothing more was sent");

    // When the queue hands rannoch-processor an event of the flow that uses as
    let shout = app.hand(12, &json!({ "text": "sam" })).unwrap();

    // Then the step's result is under the field as names, and the input's fields are still there
    assert!(until(WAIT, || app.statuses(shout).contains(&2)));
    let rows = app.about(shout).unwrap();
    let sent: Vec<&Value> = rows
        .iter()
        .filter(|r| r.status == 0)
        .map(|r| &r.data)
        .collect();
    assert_eq!(
        sent,
        [&json!({ "shout": { "text": "SAM" }, "text": "sam" })]
    );

    app.stop();
}

#[test]
fn a_failing_link_fails_the_step_and_the_retry_reruns_the_chain() {
    // Given an app built with a flow whose step is a chain with a link limit that fails on text
    // longer than 5
    let app = App::start(
        "a_failing_link_fails_the_step",
        Settings {
            attempts: 2,
            ..Settings::new(Duration::from_millis(100))
        },
    )
    .unwrap();

    // When the queue hands rannoch-processor an event whose text is 13 long
    let event = app.hand(15, &json!({ "text": "too long text" })).unwrap();

    // Then the links after limit do not run
    assert!(until(WAIT, || app.statuses(event).contains(&-1)));
    assert_eq!(names(&app.calls()), ["trim", "limit"]);

    // And the processor sends one -1 whose source is the event and whose data is
    // 'limit: text is 13 long, max 5'
    let failed = "limit: text is 13 long, max 5";
    let rows = app.about(event).unwrap();
    let ones: Vec<&Value> = rows
        .iter()
        .filter(|r| r.status == -1)
        .map(|r| &r.data)
        .collect();
    assert_eq!(ones, [&json!(failed)]);

    // And no next and no 2 are sent
    assert!(!rows.iter().any(|r| r.status == 2));
    // the only 0 is the queue's retry, of the event's own ty
    let zeros: Vec<&About> = rows.iter().filter(|r| r.status == 0).collect();
    assert!(
        zeros.iter().all(|r| r.ty == 15 && r.data == json!(failed)),
        "a next was sent: {zeros:?}"
    );

    // When the queue's retry of the event comes due
    assert!(until(WAIT, || app
        .about(event)
        .unwrap()
        .iter()
        .any(|r| r.status == 0)));
    let retry = app
        .about(event)
        .unwrap()
        .into_iter()
        .find(|r| r.status == 0)
        .unwrap()
        .id;

    // Then the chain runs again from its first link
    assert!(until(WAIT, || app.statuses(retry).contains(&-1)));
    assert_eq!(names(&app.calls()), ["trim", "limit", "trim", "limit"]);
    let again = app.about(retry).unwrap();
    assert!(
        again
            .iter()
            .any(|r| r.status == -1 && r.data == json!(failed)),
        "the retry did not run on the root's data: {again:?}"
    );

    app.stop();
}

#[test]
fn branch_and_each_send_their_nexts() {
    // Given an app built with a flow whose branch has a case when crash naming ty 31, a case when
    // question naming ty 32, and an else
    // And a flow whose branch has a case when level is the number 1, and an else
    // And a flow whose each sends one next per item of a list
    let app = App::start(
        "branch_and_each_send_their_nexts",
        Settings::new(Duration::from_millis(100)),
    )
    .unwrap();
    // the nexts sent with `id` as their source, once its flow has sent its 2
    let sent = |id: Uuid| -> Vec<(i64, Value)> {
        assert!(until(WAIT, || app.statuses(id).contains(&2)));
        app.about(id)
            .unwrap()
            .into_iter()
            .filter(|r| r.status == 0)
            .map(|r| (r.ty, r.data))
            .collect()
    };

    // When the queue hands rannoch-processor an event whose value is crash, one whose value is
    // question, and one whose value has no case
    let crash = app.hand(50, &json!({ "value": "crash" })).unwrap();
    let question = app.hand(50, &json!({ "value": "question" })).unwrap();
    let other = app.hand(50, &json!({ "value": "hang" })).unwrap();

    // Then one next of ty 31, one of ty 32 and one else next are sent, each with its event as
    // source
    assert_eq!(sent(crash), [(31, json!({ "severity": 1 }))]);
    assert_eq!(sent(question), [(32, json!({}))]);
    assert_eq!(sent(other), [(33, json!({}))]);

    // When the queue hands rannoch-processor an event whose level is the number 1, and one whose
    // level is the string "1"
    let number = app.hand(51, &json!({ "level": 1 })).unwrap();
    let string = app.hand(51, &json!({ "level": "1" })).unwrap();

    // Then the first sends the case's next and the second sends the else next
    assert_eq!(sent(number), [(31, json!({ "severity": 1 }))]);
    assert_eq!(sent(string), [(33, json!({}))]);

    // When the queue hands rannoch-processor an event whose list is 'a b c'
    let list = app.hand(52, &json!({ "list": ["a", "b", "c"] })).unwrap();

    // Then three nexts are sent, one per item, each with that event as source
    assert_eq!(
        sent(list),
        [
            (41, json!({ "item": "a" })),
            (41, json!({ "item": "b" })),
            (41, json!({ "item": "c" })),
        ]
    );

    app.stop();
}

#[test]
fn an_actions_real_output_is_checked_at_run_time() {
    // Given an app built with a flow whose step runs a test action that declares it gives
    // severity
    let app = App::start(
        "an_actions_real_output_is_checked",
        Settings {
            attempts: 1,
            ..Settings::new(Duration::from_millis(100))
        },
    )
    .unwrap();

    // When the queue hands rannoch-processor an event and the action's output has a severity
    *app.rating.lock().unwrap() = obj(json!({ "severity": 2 }));
    let rated = app.hand(30, &json!({ "text": "it crashed" })).unwrap();

    // Then the step's next carries it on
    assert!(until(WAIT, || app.statuses(rated).contains(&2)));
    let rows = app.about(rated).unwrap();
    let sent: Vec<(i64, &Value)> = rows
        .iter()
        .filter(|r| r.status == 0)
        .map(|r| (r.ty, &r.data))
        .collect();
    assert_eq!(sent, [(31, &json!({ "severity": 2 }))]);

    // When the queue hands rannoch-processor an event and the action's output has no severity
    *app.rating.lock().unwrap() = obj(json!({ "category": "bug" }));
    let unrated = app.hand(30, &json!({ "text": "it crashed" })).unwrap();

    // Then the step sends a -1 naming severity, and no next and no 2
    assert!(until(WAIT, || app.statuses(unrated).contains(&-1)));
    thread::sleep(Duration::from_millis(200));
    let rows = app.about(unrated).unwrap();
    assert_eq!(app.statuses(unrated), [1, -1]);
    assert!(
        rows[1]
            .data
            .as_str()
            .unwrap_or_default()
            .contains("severity"),
        "{:?}",
        rows[1].data
    );

    app.stop();
}

#[test]
fn a_two_step_flow_sends_its_next_only_when_every_step_succeeds() {
    // Given an app built with a two-step flow whose step 0 sends a next and whose step 1 runs a
    // test action that fails on the first attempt and succeeds on the next
    let app = App::start(
        "a_two_step_flow_sends_its_next",
        Settings {
            attempts: 2,
            ..Settings::new(Duration::from_millis(100))
        },
    )
    .unwrap();

    // When the queue hands rannoch-processor an event of that flow
    let event = app.hand(40, &json!({ "text": "  sam " })).unwrap();

    // Then step 1 is passed the options step 0 gave
    assert!(until(WAIT, || app.statuses(event).contains(&-1)));
    let calls = app.calls();
    assert_eq!(names(&calls), ["trim", "classify"]);
    assert_eq!(calls[1].1["text"], json!("sam"));

    // And the processor sends one -1 for the event, and no next
    let failed = "classify: no answer";
    let rows = app.about(event).unwrap();
    let ones: Vec<&Value> = rows
        .iter()
        .filter(|r| r.status == -1)
        .map(|r| &r.data)
        .collect();
    assert_eq!(ones, [&json!(failed)]);
    assert!(!rows.iter().any(|r| r.status == 2));
    // the only 0 is the queue's retry, of the event's own ty
    let zeros: Vec<&About> = rows.iter().filter(|r| r.status == 0).collect();
    assert!(
        zeros.iter().all(|r| r.ty == 40 && r.data == json!(failed)),
        "a next was sent: {zeros:?}"
    );

    // When the queue's retry of the event comes due
    assert!(until(WAIT, || app
        .about(event)
        .unwrap()
        .iter()
        .any(|r| r.status == 0)));
    let retry = app
        .about(event)
        .unwrap()
        .into_iter()
        .find(|r| r.status == 0)
        .unwrap()
        .id;

    // Then step 0's next is sent, and the processor sends its 2
    assert!(until(WAIT, || app.statuses(retry).contains(&2)));
    // the next's own flow, notice, may already have run its note
    assert_eq!(
        names(&app.calls())[..4],
        ["trim", "classify", "trim", "classify"]
    );
    let rows = app.about(retry).unwrap();
    let sent: Vec<(i64, &Value)> = rows
        .iter()
        .filter(|r| r.status == 0)
        .map(|r| (r.ty, &r.data))
        .collect();
    assert_eq!(sent, [(20, &json!({ "msg": "sam", "who": "two" }))]);

    // And across both attempts exactly one next was sent
    let nexts = app
        .read
        .query_row(
            "SELECT count(*) FROM _events WHERE ty = 20 AND status = 0",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(nexts, 1, "one next across both attempts");

    app.stop();
}

/// Where the test app is built, by a nested cargo of its own.
fn app_build() -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR")).join("app-build")
}

/// Copies the directory `from` to `to`, every file in it.
fn copy(from: &Path, to: &Path) -> Try<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let path = entry?.path();
        let dest = to.join(path.file_name().ok_or("a file has no name")?);
        if path.is_dir() {
            copy(&path, &dest)?;
        } else {
            // written, not copied, so each copy is newer than every build before it
            fs::write(&dest, fs::read(&path)?)?;
        }
    }
    Ok(())
}

/// What a build of the test app gave: whether it succeeded, each error line it listed, and its
/// whole output.
struct Build {
    ok: bool,
    errors: Vec<String>,
    output: String,
}

/// The test app's workspace, from `tests/app`: the app, whose own processor crate is itself, and
/// proc-text, a processor crate separate from it; both depend on this rannoch-processor, and the
/// app's build.rs checks the flow doc set it is built with. Both packages are named for the
/// workspace, so two workspaces never share a build in the one target dir.
struct Workspace {
    root: PathBuf,
    name: String,
}

impl Workspace {
    fn new(name: &str) -> Try<Self> {
        let root = app_build().join(name);
        if root.exists() {
            fs::remove_dir_all(&root)?;
        }
        let here = Path::new(env!("CARGO_MANIFEST_DIR"));
        copy(&here.join("tests/app"), &root)?;
        let crates = here
            .parent()
            .ok_or("rannoch-processor is in no crates dir")?;
        let path = |krate: &str| crates.join(krate).display().to_string();
        let (core, queue, processor) = (
            path("rannoch-core"),
            path("rannoch-queue"),
            path("rannoch-processor"),
        );
        fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nresolver = \"3\"\nmembers = [\"app\", \"proc-text\"]\n",
        )?;
        fs::write(
            root.join("proc-text/Cargo.toml"),
            format!(
                "[package]\nname = \"proc-text-{name}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\
                 publish = false\n\n[dependencies]\n\
                 rannoch-processor = {{ path = \"{processor}\" }}\nserde_json = \"1\"\n"
            ),
        )?;
        fs::write(
            root.join("app/Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\
                 publish = false\n\n[dependencies]\n\
                 proc-text = {{ path = \"../proc-text\", package = \"proc-text-{name}\" }}\n\
                 rannoch-core = {{ path = \"{core}\" }}\n\
                 rannoch-processor = {{ path = \"{processor}\" }}\n\
                 rannoch-queue = {{ path = \"{queue}\" }}\nserde_json = \"1\"\n\
                 uuid = {{ version = \"1\", features = [\"v7\"] }}\n\n[build-dependencies]\n\
                 proc-text = {{ path = \"../proc-text\", package = \"proc-text-{name}\" }}\n\
                 rannoch-processor = {{ path = \"{processor}\" }}\n"
            ),
        )?;
        fs::copy(
            crates
                .parent()
                .ok_or("crates is in no workspace")?
                .join("Cargo.lock"),
            root.join("Cargo.lock"),
        )?;
        Ok(Self {
            root,
            name: name.to_owned(),
        })
    }

    /// Builds the app with the flow doc set `set`, from `flows/`.
    fn build(&self, set: &str) -> Try<Build> {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let out = Command::new(cargo)
            .args(["build", "--offline"])
            .current_dir(&self.root)
            .env("CARGO_TARGET_DIR", app_build().join("target"))
            .env("FLOWS", self.root.join("flows").join(set))
            .output()?;
        let output = String::from_utf8_lossy(&out.stderr).into_owned();
        let errors = output
            .lines()
            .map(str::trim)
            .filter(|line| {
                let b = line.as_bytes();
                b.len() > 2 && b[0] == b'E' && b[1].is_ascii_digit() && b[2] == b' '
            })
            .map(str::to_owned)
            .collect();
        Ok(Build {
            ok: out.status.success(),
            errors,
            output,
        })
    }

    /// Runs the built app with `args`: each line it printed.
    fn run(&self, args: &[&str]) -> Try<Vec<String>> {
        let bin = app_build().join("target/debug").join(format!(
            "{}{}",
            self.name,
            std::env::consts::EXE_SUFFIX
        ));
        let out = Command::new(bin).args(args).output()?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).into_owned().into());
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::to_owned)
            .collect())
    }

    /// Builds the app with `set`, and the error lines of a build that failed.
    fn fails(&self, set: &str) -> Try<Vec<String>> {
        let build = self.build(set)?;
        assert!(!build.ok, "{set} built");
        assert!(!build.errors.is_empty(), "{set}: {}", build.output);
        Ok(build.errors)
    }
}

/// Each bad set of flow docs, and every error in it.
const BAD: [(&str, &[&str]); 10] = [
    (
        "bad-e1",
        &["E1 a.json step 0 link nope: no action named nope"],
    ),
    (
        "bad-e2",
        &["E2 a.json step 0 link greet: options for greet do not parse: unknown field `salut`"],
    ),
    (
        "bad-e3",
        &["E3 a.json step 0 link upper: takes text, which nothing before it gives"],
    ),
    (
        "bad-e4",
        &["E4 a.json step 0 link upper: takes text as string, it is number"],
    ),
    (
        "bad-e5",
        &["E5 a.json step 0 next: next to 20-notice.json gives no who, which it takes"],
    ),
    (
        "bad-e6",
        &["E6 a.json step 0 next: next to 20-notice.json gives who as number, it takes string"],
    ),
    (
        "bad-concat",
        &[
            "E5 a.json step 0 next: next to 20-notice.json field msg: $greeting names nothing in scope",
        ],
    ),
    (
        "bad-e7",
        &["E7 a.json step 0 next: next to ty 99, which no flow takes"],
    ),
    (
        "bad-emit",
        &["E1 a.json step 0 link emit: no action named emit"],
    ),
    (
        "bad-multi",
        &[
            "E1 a.json step 0 link nope: no action named nope",
            "E7 a.json step 0 next: next to ty 99, which no flow takes",
            "E4 b.json step 0 link upper: takes text as string, it is number",
        ],
    ),
];

/// The errors `BAD` gives for `set`.
fn bad(set: &str) -> Vec<String> {
    BAD.iter()
        .find(|(name, _)| *name == set)
        .map_or_default(|(_, errors)| errors.iter().map(|e| (*e).to_owned()).collect())
}

#[test]
fn the_apps_build_checks_its_flow_docs() {
    // Given an app whose flow docs are a valid set over the actions of a test processor crate,
    // one of whose actions is a job
    let app = Workspace::new("app-checks").unwrap();

    // When the app is built
    let build = app.build("valid").unwrap();

    // Then the build succeeds
    assert!(build.ok, "{}", build.output);

    // And each flow that uses the job action reports a job step, and each flow that uses none
    // does not
    assert_eq!(
        app.run(&["jobs"]).unwrap(),
        ["10 false", "15 false", "20 false", "40 true"]
    );

    // And the built app runs its flows with the flow doc files removed from disk
    fs::remove_dir_all(app.root.join("flows/valid")).unwrap();
    let lines = app.run(&["run", "10", r#"{"text":"  sam "}"#]).unwrap();
    let event = json!(lines[0]);
    let sent: Vec<Value> = lines[1..]
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let sent: Vec<(&Value, &Value, &Value, &Value)> = sent
        .iter()
        .map(|e| (&e["ty"], &e["status"], &e["source"], &e["data"]))
        .collect();
    assert_eq!(
        sent,
        [
            (
                &json!(20),
                &json!(0),
                &event,
                &json!({ "msg": "hi SAM!", "who": "friend" })
            ),
            (&json!(10), &json!(2), &event, &json!("done")),
        ]
    );

    // When the app is built with a flow doc whose step names an action nope that does not exist
    // Then the build fails listing one error: no action named nope
    assert_eq!(app.fails("bad-e1").unwrap(), bad("bad-e1"));

    // When the app is built with a flow doc whose options for greet have the unknown field salut
    // Then the build fails listing one error: the options for greet do not parse
    assert_eq!(app.fails("bad-e2").unwrap(), bad("bad-e2"));

    // When the app is built with a flow doc whose link upper takes text, which nothing before it
    // gives
    // Then the build fails listing one error saying so
    assert_eq!(app.fails("bad-e3").unwrap(), bad("bad-e3"));

    // When the app is built with a flow doc whose link upper takes text as a string and is given
    // a number
    // Then the build fails listing one error saying so
    assert_eq!(app.fails("bad-e4").unwrap(), bad("bad-e4"));

    // When the app is built with a flow doc whose next to notice gives no who, which notice takes
    // Then the build fails listing one error saying so
    assert_eq!(app.fails("bad-e5").unwrap(), bad("bad-e5"));

    // When the app is built with a flow doc whose next to notice gives who as a number, where
    // notice takes a string
    // Then the build fails listing one error saying so
    assert_eq!(app.fails("bad-e6").unwrap(), bad("bad-e6"));

    // When the app is built with a flow doc whose next's $concat names a field greeting that
    // nothing in scope gives
    // Then the build fails listing one error: greeting names nothing in scope
    assert_eq!(app.fails("bad-concat").unwrap(), bad("bad-concat"));

    // When the app is built with a flow doc whose next goes to ty 99, which no flow takes
    // Then the build fails listing one error saying so
    assert_eq!(app.fails("bad-e7").unwrap(), bad("bad-e7"));

    // When the app is built with a flow doc whose step names emit
    // Then the build fails listing one error: no action named emit
    assert_eq!(app.fails("bad-emit").unwrap(), bad("bad-emit"));

    // When the app is built with two flow docs holding three errors between them
    // Then the build fails listing all three errors
    assert_eq!(app.fails("bad-multi").unwrap(), bad("bad-multi"));
}

#[test]
fn the_apps_build_gathers_the_signatures_a_separate_processor_crate_exports() {
    // Given a workspace with a processor crate, separate from the app, that exports its actions'
    // signatures as data, among them greet
    // And an app with its own processor crate, whose flow docs use actions of both, including a
    // next whose template names the field greet gives
    let app = Workspace::new("app-gathers").unwrap();

    // When the app is built
    let build = app.build("valid").unwrap();

    // Then its build.rs gathers the signatures of both crates and the build succeeds
    assert!(build.ok, "{}", build.output);
    let jobs = app.run(&["jobs"]).unwrap();
    assert!(
        jobs.contains(&"40 true".to_owned()),
        "the app's own job action was not gathered: {jobs:?}"
    );

    // When the app is built with each bad set of flow docs
    // Then each build fails listing every error in it
    for (set, errors) in BAD {
        assert_eq!(app.fails(set).unwrap(), errors, "{set}");
    }

    // When the separate crate renames the field greet gives, and the app is built
    let exported = app.root.join("proc-text/signatures.json");
    let signatures = fs::read_to_string(&exported).unwrap();
    let renamed = signatures.replace(
        r#""gives": { "greeting": "string" }"#,
        r#""gives": { "hello": "string" }"#,
    );
    assert_ne!(renamed, signatures, "greet's gives was not renamed");
    fs::write(&exported, renamed).unwrap();
    let build = app.build("valid").unwrap();

    // Then the build fails listing one error: the next's template names a field nothing gives
    assert!(!build.ok);
    assert_eq!(
        build.errors,
        [
            "E5 10-greet.json step 0 next: next to 20-notice.json field msg: $greeting names nothing in scope"
        ]
    );
}
