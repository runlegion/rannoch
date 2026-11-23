//! FR-RANNOCH-119's scenarios from `tests/features/lib.feature` (`@criterion-FR-RANNOCH-119-c3`
//! to `-c9`, `-c11`, `-c13`, `-c14`). The store is the app's database through the real store,
//! events from the app's forgr factory, handed over by the real queue; the flow docs are
//! `tests/flows.json` and the test processor crate's actions are `Fake`s. The build scenarios
//! (`-c1`, `-c2`, `-c10`, `-c12`, `-c16`) build the test app of `tests/app` with cargo.
//! FR-RANNOCH-124's scenarios, from `tests/features/fr_rannoch_124.feature`, run `BUILT_IN`'s
//! flows on the built-in steps alone, `check`ed with no signature, the append an `Events` made
//! `with_writes`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use chrono::{DateTime, TimeDelta, Utc};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
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
fn migrated(path: &Path) -> Result<Arc<Store>> {
    let store = Arc::new(Store::new(path.to_owned(), rannoch_core::migrator()));
    Rannoch::<Ty, Value>::open(Config {
        store: Some(store.clone()),
        ..Config::default()
    })?
    .shutdown();
    Ok(store)
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// Waits up to `limit` for `done`.
fn until(limit: Duration, done: impl Fn() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < limit {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    done()
}

/// One row about an event: id, ty, status, data, `run_at`.
type About = (Uuid, i64, i64, Value, Option<String>);

/// The app's actions over what they record and the rating `rate` gives.
type Made = fn(&Calls, &Arc<Mutex<Obj>>) -> Vec<Arc<dyn Action>>;

/// The app: the store, rannoch, the queue handing events to rannoch-processor, the processor,
/// and what the fake actions saw.
struct App {
    path: PathBuf,
    read: Connection,
    signal: mpsc::Sender<()>,
    rannoch: Rannoch<Ty, Value>,
    queue: Queue,
    processor: Arc<Processor<Ty, Value>>,
    calls: Calls,
    rating: Arc<Mutex<Obj>>,
}

impl App {
    fn start(name: &str, attempts: u32) -> Result<Self> {
        Self::open(database(name)?, attempts)
    }

    /// The app over the store at `path`, as it is.
    fn open(path: PathBuf, attempts: u32) -> Result<Self> {
        let settings = Settings {
            attempts,
            ..Settings::new(Duration::from_millis(100))
        };
        Self::open_with(path, settings)
    }

    /// The app over a fresh store `name` running `flows` on the built-in steps, no action of its
    /// own.
    fn built_in(name: &str, flows: &str) -> Result<Self> {
        let settings = Settings {
            attempts: 1,
            ..Settings::new(Duration::from_millis(100))
        };
        Self::over(database(name)?, flows, |_, _| Vec::new(), settings)
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
    fn open_with(path: PathBuf, settings: Settings) -> Result<Self> {
        Self::over(path, include_str!("flows.json"), actions, settings)
    }

    /// The app over the store at `path` running `flows` with the actions `made`, its queue on
    /// `settings`; its `Events` writes the processor's store rows with each 2.
    fn over(path: PathBuf, flows: &str, made: Made, settings: Settings) -> Result<Self> {
        let store = migrated(&path)?;
        let (calls, rating) = (Calls::default(), Arc::default());
        let actions = made(&calls, &rating);
        let processor = Arc::new(Processor::new(flows, actions, ready(&path)?)?);
        let (signal, wake) = mpsc::channel();
        let events = Events::new(ready(&path)?, signal.clone()).with_writes(processor.writes());
        let append = Arc::new(events) as Arc<dyn Append<Ty, Value>>;
        let rannoch = Rannoch::open(Config {
            store: Some(store),
            append: Some(append),
            ..Config::default()
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
        let id = Uuid::now_v7();
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
    fn retry(&self, event: Uuid) -> Uuid {
        assert!(until(WAIT, || !self.sent(event, 0).is_empty()));
        self.about(event)
            .into_iter()
            .find(|r| r.2 == 0)
            .map_or_default(|r| r.0)
    }

    fn stop(self) {
        drop(self.signal);
        self.rannoch.shutdown();
        self.queue.join();
    }
}

const WAIT: Duration = Duration::from_secs(5);

#[test]
fn rannoch_processor_runs_the_flow_for_an_event_the_queue_hands_it() {
    // Given an app built with a flow whose step is the chain trim.upper.greet, from a test
    // processor crate, ending in a next with a delay of 500 whose shape fills who with or and
    // builds msg with $concat
    // And a flow for the receiving ty whose step has no next
    // And a flow whose step puts its result under a field with as
    let app = App::start("runs_the_flow", 10).unwrap();

    // When the queue hands rannoch-processor an event of the chain's ty
    let before = Utc::now() - TimeDelta::milliseconds(1);
    let event = app
        .hand(10, &json!({ "text": "  sam ", "lang": "en" }))
        .unwrap();

    // Then trim, upper and greet run on the one event, with no event written between them
    assert!(until(WAIT, || app.statuses(event).contains(&2)));
    let after = Utc::now();
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
    assert!(until(WAIT, || app.statuses(next.0).contains(&2)));
    thread::sleep(Duration::from_millis(200));
    assert_eq!(app.statuses(next.0), [1, 2]);
    assert_eq!(
        app.count("SELECT count(*) FROM _events"),
        6,
        "nothing more was sent"
    );

    // When the queue hands rannoch-processor an event of the flow that uses as
    let shout = app.hand(12, &json!({ "text": "sam" })).unwrap();

    // Then the step's result is under the field as names, and the input's fields are still there
    assert!(until(WAIT, || app.statuses(shout).contains(&2)));
    assert_eq!(
        app.sent(shout, 0),
        [(22, json!({ "shout": { "text": "SAM" }, "text": "sam" }))]
    );

    app.stop();
}

#[test]
fn a_failing_link_fails_the_step_and_the_retry_reruns_the_chain() {
    // Given an app built with a flow whose step is a chain with a link limit that fails on text
    // longer than 5
    let app = App::start("a_failing_link", 2).unwrap();

    // When the queue hands rannoch-processor an event whose text is 13 long
    let event = app.hand(15, &json!({ "text": "too long text" })).unwrap();

    // Then the links after limit do not run
    assert!(until(WAIT, || app.statuses(event).contains(&-1)));
    assert_eq!(app.names(), ["trim", "limit"]);

    // And the processor sends one -1 whose source is the event and whose data is
    // 'limit: text is 13 long, max 5'
    let failed = json!("limit: text is 13 long, max 5");
    assert_eq!(app.sent(event, -1), [(15, failed.clone())]);

    // And no next and no 2 are sent
    assert!(!app.statuses(event).contains(&2));
    // the only 0 about the event is the queue's retry, of its own ty with the failure as data
    let retry = app.retry(event);
    assert_eq!(app.sent(event, 0), [(15, failed.clone())]);

    // When the queue's retry of the event comes due
    // Then the chain runs again from its first link
    assert!(until(WAIT, || app.statuses(retry).contains(&-1)));
    assert_eq!(app.names(), ["trim", "limit", "trim", "limit"]);
    assert_eq!(
        app.sent(retry, -1),
        [(15, failed)],
        "the retry ran on the root's data"
    );

    app.stop();
}

#[test]
fn branch_and_each_send_their_nexts() {
    // Given an app built with a flow whose branch has a case when crash naming ty 31, a case when
    // question naming ty 32, and an else
    // And a flow whose branch has a case when level is the number 1, and an else
    // And a flow whose each sends one next per item of a list
    let app = App::start("branch_and_each", 1).unwrap();
    // the nexts whose source is `event`, once its flow has sent its 2
    let nexts = |event| {
        assert!(until(WAIT, || app.statuses(event).contains(&2)));
        app.sent(event, 0)
    };

    // When the queue hands rannoch-processor an event whose value is crash, one whose value is
    // question, and one whose value has no case
    let crash = app.hand(50, &json!({ "value": "crash" })).unwrap();
    let question = app.hand(50, &json!({ "value": "question" })).unwrap();
    let other = app.hand(50, &json!({ "value": "hang" })).unwrap();

    // Then one next of ty 31, one of ty 32 and one else next are sent, each with its event as
    // source
    assert_eq!(nexts(crash), [(31, json!({ "severity": 1 }))]);
    assert_eq!(nexts(question), [(32, json!({}))]);
    assert_eq!(nexts(other), [(33, json!({}))]);

    // When the queue hands rannoch-processor an event whose level is the number 1, and one whose
    // level is the string "1"
    let number = app.hand(51, &json!({ "level": 1 })).unwrap();
    let string = app.hand(51, &json!({ "level": "1" })).unwrap();

    // Then the first sends the case's next and the second sends the else next
    assert_eq!(nexts(number), [(31, json!({ "severity": 1 }))]);
    assert_eq!(nexts(string), [(33, json!({}))]);

    // When the queue hands rannoch-processor an event whose list is 'a b c'
    let list = app.hand(52, &json!({ "list": ["a", "b", "c"] })).unwrap();

    // Then three nexts are sent, one per item, each with that event as source
    let mut sent = nexts(list);
    sent.sort_by_key(|n| n.1.to_string());
    let words = ["a", "b", "c"].map(|w| (41, json!({ "word": w })));
    assert_eq!(sent, words);

    app.stop();
}

#[test]
fn a_two_step_flow_sends_its_next_only_when_every_step_succeeds() {
    // Given an app built with a two-step flow whose step 0 sends a next and whose step 1 runs a
    // test action that fails on the first attempt and succeeds on the next
    let app = App::start("a_two_step_flow", 2).unwrap();

    // When the queue hands rannoch-processor an event of that flow
    let event = app.hand(40, &json!({ "text": "  sam " })).unwrap();

    // Then step 1 is passed the options step 0 gave
    assert!(until(WAIT, || app.statuses(event).contains(&-1)));
    assert_eq!(app.names(), ["trim", "classify"]);
    assert_eq!(app.calls.lock().unwrap()[1].1["text"], "sam");

    // And the processor sends one -1 for the event, and no next
    let failed = json!("classify: no answer");
    assert_eq!(app.sent(event, -1), [(40, failed.clone())]);
    assert!(!app.statuses(event).contains(&2));
    let retry = app.retry(event);
    assert_eq!(
        app.sent(event, 0),
        [(40, failed)],
        "the only 0 is the queue's retry"
    );

    // When the queue's retry of the event comes due
    // Then step 0's next is sent, and the processor sends its 2
    assert!(until(WAIT, || app.statuses(retry).contains(&2)));
    assert_eq!(app.names()[..4], ["trim", "classify", "trim", "classify"]);
    assert_eq!(
        app.sent(retry, 0),
        [(20, json!({ "msg": "sam", "who": "two" }))]
    );

    // And across both attempts exactly one next was sent
    let nexts = app.count("SELECT count(*) FROM _events WHERE ty = 20 AND status = 0");
    assert_eq!(nexts, 1, "one next across both attempts");

    app.stop();
}

#[test]
fn an_actions_real_output_is_checked_at_run_time() {
    // Given an app built with a flow whose step runs a test action that declares it gives
    // severity
    let app = App::start("an_actions_real_output", 1).unwrap();

    // When the queue hands rannoch-processor an event and the action's output has a severity
    *app.rating.lock().unwrap() = obj(&json!({ "severity": 2 }));
    let rated = app.hand(30, &json!({ "text": "it crashed" })).unwrap();

    // Then the step's next carries it on
    assert!(until(WAIT, || app.statuses(rated).contains(&2)));
    assert_eq!(app.sent(rated, 0), [(31, json!({ "severity": 2 }))]);

    // When the queue hands rannoch-processor an event and the action's output has no severity
    *app.rating.lock().unwrap() = obj(&json!({ "category": "bug" }));
    let unrated = app.hand(30, &json!({ "text": "it crashed" })).unwrap();

    // Then the step sends a -1 naming severity, and no next and no 2
    assert!(until(WAIT, || app.statuses(unrated).contains(&-1)));
    thread::sleep(Duration::from_millis(200));
    assert_eq!(app.statuses(unrated), [1, -1]);
    assert_eq!(
        app.sent(unrated, -1),
        [(30, json!("rate gave no severity"))]
    );

    app.stop();
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

/// The test app of `tests/app`, a workspace of proc-text and the app, copied to a dir of its
/// own so its flow docs can be removed; every copy builds into the one shared target dir.
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

#[test]
fn the_apps_build_checks_its_flow_docs() -> Result<()> {
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

#[test]
fn the_apps_build_gathers_the_signatures_a_separate_processor_crate_exports() -> Result<()> {
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

#[test]
fn a_wait_kept_in_events_matches_times_out_and_sends_a_late_match_to_late_across_a_restart() {
    // Given an app built with a flow whose step waits for a payment keyed by its order, with a
    // timeout, a match next, a timeout next and a late next
    let app = App::start("a_wait_kept_in_events", 1).unwrap();

    // When orders o1 and o2 start waiting
    let before = Utc::now() - TimeDelta::milliseconds(1);
    let o1 = app.hand(70, &json!({ "order": "o1" })).unwrap();
    let o2 = app.hand(70, &json!({ "order": "o2" })).unwrap();

    // Then each wait is kept in _events as an event whose source is the waiting event and whose
    // run_at is its timeout
    let wait = |order: Uuid, key: &str| {
        assert!(until(WAIT, || app.statuses(order).contains(&2)));
        let after = Utc::now();
        let wait = app.about(order).into_iter().find(|r| r.2 == 0).unwrap();
        let data = json!({ "$wait": { "step": 0, "for": 71 }, "key": key,
            "vals": { "order": key } });
        assert_eq!((wait.1, &wait.3), (70, &data));
        let run_at: DateTime<Utc> = wait.4.unwrap().parse().unwrap();
        let within = TimeDelta::milliseconds(2500);
        assert!(run_at >= before + within && run_at <= after + within);
        wait.0
    };
    let (w1, w2) = (wait(o1, "o1"), wait(o2, "o2"));

    // When the app's process is dropped and its _events file reopened
    let path = app.path.clone();
    app.stop();
    let app = App::open(path, 1).unwrap();

    // And two payments for o1 arrive before o1's timeout
    let pay = |order: &str, amount: u64| {
        let paid = app.hand(71, &json!({ "order": order, "amount": amount }));
        let paid = paid.unwrap();
        assert!(until(WAIT, || app.statuses(paid).contains(&2)));
    };
    pay("o1", 5);
    pay("o1", 6);

    // Then o1's match next is sent once, with the wait as its source
    let matched = (72, json!({ "order": "o1", "paid": 5 }));
    assert!(until(WAIT, || !app.sent(w1, 0).is_empty()));
    assert!(
        app.statuses(w1).iter().all(|s| *s == 0),
        "o1 is not due yet"
    );
    assert_eq!(app.sent(w1, 0), std::slice::from_ref(&matched));

    // When the waits come due, and the due events are run again
    let due = |w| until(WAIT, || app.statuses(w).contains(&2));
    assert!(due(w1) && due(w2));

    // Then o1's wait sends no timeout, and o2's wait sends its timeout next once
    assert_eq!(app.sent(w1, 0), [matched]);
    assert_eq!(app.sent(w1, 2), [(70, json!("matched"))]);
    let timeout = (73, json!({ "order": "o2" }));
    assert_eq!(app.sent(w2, 0), std::slice::from_ref(&timeout));
    assert_eq!(app.sent(w2, 2), [(70, json!("timeout"))]);

    // When a payment for o2 arrives after its timeout, and the due events are run again
    pay("o2", 7);

    // Then o2's late next is sent once, and no match next
    let late = (74, json!({ "order": "o2", "paid": 7 }));
    assert!(until(WAIT, || app.sent(w2, 0).len() > 1));
    thread::sleep(Duration::from_millis(200));
    assert_eq!(app.sent(w2, 0), [timeout, late]);
    let sent = app.count("SELECT count(*) FROM _events WHERE ty = 72 AND status = 0");
    assert_eq!(sent, 1, "o1's match only");

    app.stop();
}

/// Not a scenario: #84. The hunt's resend of a run a crash dropped runs that run's flow on the
/// dropped event's data.
#[test]
fn the_hunts_resend_runs_the_dropped_events_flow_on_its_data() {
    // Given a store with an event of the flow that uses as, whose 1 is an hour old with nothing
    // after it, as a crash leaves it
    let path = database("the_hunts_resend").unwrap();
    migrated(&path).unwrap();
    let seed = ready(&path).unwrap();
    let ago = |secs: i64| {
        let at = u64::try_from(Utc::now().timestamp() - secs).unwrap();
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
    let app = App::open_with(path, settings).unwrap();

    // Then the hunt resends the dropped event
    let resend = app.retry(dropped);
    assert_eq!(app.sent(dropped, 0), [(12, json!("hung"))]);

    // And rannoch-processor runs the resend's flow on the dropped event's data
    assert!(until(WAIT, || app
        .statuses(resend)
        .iter()
        .any(|s| *s == 2 || *s == -1)));
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

    app.stop();
}

/// An id `secs` seconds ago.
fn ago(secs: i64) -> Uuid {
    let at = u64::try_from(Utc::now().timestamp() - secs).unwrap_or_default();
    Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, at, 0))
}

/// A store at `path` holding o9's wait, as the order flow keeps it, two hours old with a 1 after
/// it, then each of `about` as (seconds ago, status, data) with the wait as its source; the
/// wait's id.
fn waiting(path: &Path, about: &[(i64, i64, Value)]) -> Result<Uuid> {
    migrated(path)?;
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
#[test]
fn a_wait_the_hunt_resends_sends_its_timeout_once_and_its_2_says_timeout() {
    // Given a store with o9's wait, whose 1 is on disk with nothing after it, as a crash leaves it
    let path = database("a_wait_the_hunt_resends").unwrap();
    let wait = waiting(&path, &[]).unwrap();

    // When the queue starts with a hunt that runs soon
    let settings = Settings {
        attempts: 1,
        window: Duration::from_mins(1),
        hunt: Duration::from_millis(300),
        ..Settings::new(Duration::from_millis(100))
    };
    let app = App::open_with(path, settings).unwrap();

    // Then the hunt resends the wait
    let resend = app.retry(wait);
    assert_eq!(app.sent(wait, 0).first(), Some(&(70, json!("hung"))));

    // And the resend sends the wait's timeout once, with the wait as its source, and its 2 says
    // "timeout"
    assert!(until(WAIT, || app
        .statuses(resend)
        .iter()
        .any(|s| *s == 2 || *s == -1)));
    thread::sleep(Duration::from_millis(200));
    let timeout = (73, json!({ "order": "o9" }));
    assert_eq!(app.sent(wait, 0), [(70, json!("hung")), timeout]);
    assert_eq!(app.sent(resend, 2), [(70, json!("timeout"))]);
    assert_eq!(app.statuses(resend), [1, 2]);

    app.stop();
}

/// Not a scenario: #91. The queue's retry of a wait whose run failed sends the wait's timeout.
#[test]
fn a_wait_the_queue_retries_sends_its_timeout_once_and_its_2_says_timeout() {
    // Given a store with o9's wait, whose run failed, and the queue's retry of it
    let path = database("a_wait_the_queue_retries").unwrap();
    let failed = json!("database is locked");
    let about = [(5000, -1, failed.clone()), (4800, 0, failed.clone())];
    let wait = waiting(&path, &about).unwrap();

    // When the queue starts and takes the retry
    let app = App::open(path, 3).unwrap();
    app.signal.send(()).unwrap();
    let retry = app.retry(wait);

    // Then the retry sends the wait's timeout once, with the wait as its source, and its 2 says
    // "timeout"
    assert!(until(WAIT, || app
        .statuses(retry)
        .iter()
        .any(|s| *s == 2 || *s == -1)));
    thread::sleep(Duration::from_millis(200));
    let timeout = (73, json!({ "order": "o9" }));
    assert_eq!(app.sent(wait, 0), [(70, failed), timeout]);
    assert_eq!(app.sent(retry, 2), [(70, json!("timeout"))]);
    assert_eq!(app.statuses(retry), [1, 2]);

    app.stop();
}

/// Two payments for o9, `paid` the first's amount, each waited for until its 2; then the wait's
/// 0s, which must be `first`, the timeout and one late, for the first payment.
fn pays_late(app: &App, wait: Uuid, first: (i64, Value), paid: u64) -> Result<()> {
    for amount in [paid, paid + 1] {
        let payment = app.hand(71, &json!({ "order": "o9", "amount": amount }))?;
        assert!(until(WAIT, || app.statuses(payment).contains(&2)));
    }
    thread::sleep(Duration::from_millis(200));
    let (timeout, late) = (
        json!({ "order": "o9" }),
        json!({ "order": "o9", "paid": paid }),
    );
    assert_eq!(app.sent(wait, 0), [first, (73, timeout), (74, late)]);
    Ok(())
}

/// Not a scenario: #95. After the hunt's resend of a wait sends its timeout, the first match
/// sends the wait's late, once.
#[test]
fn a_wait_the_hunt_resends_sends_late_on_the_first_match_after_its_timeout() {
    // Given a store with o9's wait, whose 1 is on disk with nothing after it, as a crash leaves it
    let path = database("a_wait_the_hunt_resends_late").unwrap();
    let wait = waiting(&path, &[]).unwrap();

    // And the hunt's resend of the wait has sent its timeout
    let settings = Settings {
        attempts: 1,
        window: Duration::from_mins(1),
        hunt: Duration::from_millis(300),
        ..Settings::new(Duration::from_millis(100))
    };
    let app = App::open_with(path, settings).unwrap();
    let resend = app.retry(wait);
    assert!(until(WAIT, || app.statuses(resend).contains(&2)));
    assert_eq!(app.sent(resend, 2), [(70, json!("timeout"))]);

    // When two payments for o9 arrive
    // Then the wait's late is sent once, for the first, with the wait as its source
    pays_late(&app, wait, (70, json!("hung")), 7).unwrap();

    app.stop();
}

/// Not a scenario: #95. After the queue's retry of a wait sends its timeout, the first match
/// sends the wait's late, once.
#[test]
fn a_wait_the_queue_retries_sends_late_on_the_first_match_after_its_timeout() {
    // Given a store with o9's wait, whose run failed, and the queue's retry of it
    let path = database("a_wait_the_queue_retries_late").unwrap();
    let failed = json!("database is locked");
    let about = [(5000, -1, failed.clone()), (4800, 0, failed.clone())];
    let wait = waiting(&path, &about).unwrap();

    // And the retry has sent the wait's timeout
    let app = App::open(path, 3).unwrap();
    app.signal.send(()).unwrap();
    let retry = app.retry(wait);
    assert!(until(WAIT, || app.statuses(retry).contains(&2)));
    assert_eq!(app.sent(retry, 2), [(70, json!("timeout"))]);

    // When two payments for o9 arrive
    // Then the wait's late is sent once, for the first, with the wait as its source
    pays_late(&app, wait, (70, failed), 7).unwrap();

    app.stop();
}

/// FR-RANNOCH-124's flows, each with the name of its file: a ticket's emit steps branch on its
/// kind and wait for its answer, a payment stores its row, a lost order stores into a table the
/// store does not have, and the flows their nexts reach. No action of the app's is in them.
const BUILT_IN: &str = r#"[
  ["ticket.json", {"name": "ticket", "ty": 80, "takes": {"ticket": "string", "kind": "string"},
    "steps": [
      {"do": "emit", "next": {"branch": {"on": "kind",
        "cases": [{"when": "rush", "next": {"ty": 81, "shape": {"ticket": "$ticket"}}}],
        "else": {"ty": 82, "shape": {"ticket": "$ticket"}}}}},
      {"do": "emit", "next": {"wait": {"for": 83, "key": "$ticket", "within_ms": 2500,
        "next": {"ty": 84, "shape": {"ticket": "$ticket"}},
        "timeout": {"ty": 85, "shape": {"ticket": "$ticket"}}}}}]}],
  ["rushed.json", {"name": "rushed", "ty": 81, "takes": {"ticket": "string"}, "steps": []}],
  ["queued.json", {"name": "queued", "ty": 82, "takes": {"ticket": "string"}, "steps": []}],
  ["answer.json", {"name": "answer", "ty": 83, "takes": {"ticket": "string"}, "steps": []}],
  ["answered.json", {"name": "answered", "ty": 84, "takes": {"ticket": "string"}, "steps": []}],
  ["unanswered.json", {"name": "unanswered", "ty": 85, "takes": {"ticket": "string"},
    "steps": []}],
  ["payment.json", {"name": "payment", "ty": 86, "takes": {"order": "string", "amount": "number"},
    "steps": [{"do": "store", "with": {"store": {"table": "payments",
      "row": {"order_id": "$order", "amount": "$amount"}}}}]}],
  ["lost.json", {"name": "lost", "ty": 87, "takes": {"order": "string"},
    "steps": [{"do": "store", "with": {"store": {"table": "nope",
      "row": {"order_id": "$order"}}}}]}]
]"#;

/// `BUILT_IN` checked as the app's build checks it, with no signature of the app's: the flows to
/// compile in, or every error.
fn built_in() -> std::result::Result<String, Vec<String>> {
    let docs: Vec<(String, FlowDoc)> =
        serde_json::from_str(BUILT_IN).map_err(|e| vec![e.to_string()])?;
    check(&docs, &[])
}

/// The table the payment flow stores into, keyed by its order.
const PAYMENTS: &str = "CREATE TABLE payments (order_id TEXT PRIMARY KEY, amount INTEGER)";

#[test]
fn a_flow_of_built_in_steps_runs_with_no_action_of_the_apps() {
    // Given an app whose flow has an emit step that branches on a field and an emit step that
    // waits for another event, and no action of its own
    // When the app's build checks the flow and an event of its ty is handed to it
    // Then the build passes
    let flows = built_in().unwrap();
    let app = App::built_in("a_flow_of_built_in_steps", &flows).unwrap();
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
    assert!(until(WAIT, || app.statuses(rush).contains(&2)));
    let w1 = wait(rush);
    let answer = app.hand(83, &json!({ "ticket": "t1" })).unwrap();
    assert!(until(WAIT, || app.statuses(slow).contains(&2)));

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
    assert!(until(WAIT, || app.statuses(answer).contains(&2)));
    assert!(until(WAIT, || app.statuses(w1).contains(&2)));
    assert_eq!(app.sent(w1, 0), [(84, json!({ "ticket": "t1" }))]);
    assert_eq!(app.sent(w1, 2), [(80, json!("matched"))]);

    // and its timeout when none does
    assert!(until(WAIT, || app.statuses(w2).contains(&2)));
    assert_eq!(app.sent(w2, 0), [(85, json!({ "ticket": "t2" }))]);
    assert_eq!(app.sent(w2, 2), [(80, json!("timeout"))]);

    app.stop();
}

#[test]
fn store_keeps_one_row_with_the_events_2() {
    // Given a table with a key, and a flow whose step stores a row shaped from the event's fields
    // into it
    let app = App::built_in("store_keeps_one_row", &built_in().unwrap()).unwrap();
    app.read.execute_batch(PAYMENTS).unwrap();

    // When events are handed to the flow, and one of them is run again
    let data = |i: i64| json!({ "order": format!("o{i}"), "amount": i * 10 });
    let paid: Vec<Uuid> = (0..5).map(|i| app.hand(86, &data(i)).unwrap()).collect();
    for event in &paid {
        assert!(until(WAIT, || app.statuses(*event).contains(&2)));
    }
    app.rerun(paid[0], 86, &data(0)).unwrap();
    assert!(until(WAIT, || app.statuses(paid[0]) == [1, 2, 2]));

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

    app.stop();
}

#[test]
fn store_refuses_a_table_it_does_not_have() {
    // Given a flow whose step stores a row into a table the store does not have
    let app = App::built_in("store_refuses_a_table", &built_in().unwrap()).unwrap();
    app.read.execute_batch(PAYMENTS).unwrap();

    // When an event is handed to the flow
    let lost = app.hand(87, &json!({ "order": "o1" })).unwrap();
    let later = app
        .hand(86, &json!({ "order": "o2", "amount": 5 }))
        .unwrap();

    // Then the event gets a -1 naming the table
    assert!(until(WAIT, || app.statuses(lost).contains(&-1)));
    assert_eq!(
        app.sent(lost, -1),
        [(87, json!("store: no such table: nope"))]
    );

    // and later events of the same inbox are written
    assert!(until(WAIT, || app.statuses(later).contains(&2)));
    let row = "SELECT count(*) FROM payments WHERE order_id = 'o2' AND amount = 5";
    assert_eq!(app.count(row), 1);

    app.stop();
}
