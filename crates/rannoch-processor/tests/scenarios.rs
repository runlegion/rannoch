//! FR-RANNOCH-119's scenarios from `tests/features/lib.feature` (`@criterion-FR-RANNOCH-119-c3`
//! to `-c6`, `-c11`, `-c13`, `-c14`). The store is the app's database through the real store,
//! events from the app's forgr factory, handed over by the real queue; the flow docs are
//! `tests/flows.json` and the test processor crate's actions are `Fake`s. The build scenarios
//! (`-c1`, `-c2`, `-c10`, `-c12`, `-c16`) build the test app of `tests/app` with cargo.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use chrono::{DateTime, TimeDelta, Utc};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_processor::{Action, Processor, Sig};
use rannoch_queue::{Queue, Settings};
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

/// The app: the store, rannoch, the queue handing events to rannoch-processor, and what the
/// fake actions saw.
struct App {
    path: PathBuf,
    read: Connection,
    signal: mpsc::Sender<()>,
    rannoch: Rannoch<Ty, Value>,
    queue: Queue,
    calls: Calls,
    rating: Arc<Mutex<Obj>>,
}

impl App {
    fn start(name: &str, attempts: u32) -> Result<Self> {
        let path = database(name)?;
        let store = migrated(&path)?;
        let (calls, rating) = (Calls::default(), Arc::default());
        let flows = include_str!("flows.json");
        let processor: Processor<Ty, Value> = Processor::new(flows, actions(&calls, &rating))?;
        let (signal, wake) = mpsc::channel();
        let append =
            Arc::new(Events::new(ready(&path)?, signal.clone())) as Arc<dyn Append<Ty, Value>>;
        let rannoch = Rannoch::open(Config {
            store: Some(store),
            append: Some(append),
            ..Config::default()
        })?;
        let settings = Settings {
            attempts,
            ..Settings::new(Duration::from_millis(100))
        };
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
    ("emit", false, "E1 a.json step 0 emit: no action named emit"),
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
    // greeting, which nothing in scope gives; a next to ty 99; a step naming emit; two docs
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
