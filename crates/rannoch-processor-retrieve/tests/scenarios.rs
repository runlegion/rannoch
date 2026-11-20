//! FR-RANNOCH-122's scenario from `tests/features/lib.feature` (`@criterion-FR-RANNOCH-122-c1`).
//! The store is the app's database through the real store, events from the app's forgr
//! factory, handed over by the real queue to rannoch-processor, as in rannoch-processor's
//! scenarios; the app's source is a stub behind `Source`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_processor::{FlowDoc, Processor, Sig, check};
use rannoch_processor_retrieve::{Retrieve, Source};
use rannoch_queue::{Flows, Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The test app's stub source: answers with the event's `user` as its email.
struct Stub;
impl Source for Stub {
    fn get(&self, input: &Map<String, Value>) -> std::result::Result<Map<String, Value>, String> {
        let email = input.get("user").cloned().unwrap_or_default();
        Ok([("email".to_owned(), email)].into_iter().collect())
    }
}

const SIG: &str = r#"{ "action": "lookup", "gives": { "email": "string" }, "job": true }"#;
const FLOWS: &str = r#"[
  {"name": "lookup", "ty": 10, "takes": {"user": "any"},
   "steps": [{"do": "lookup", "next": {"ty": 11, "shape": {"email": "$email"}}}]},
  {"name": "found", "ty": 11, "takes": {"email": "string"}, "steps": []}
]"#;

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// Waits up to five seconds for `done`.
fn until(done: impl Fn() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    done()
}

/// The app's store, rannoch, and the queue handing events to rannoch-processor.
struct App(PathBuf, mpsc::Sender<()>, Rannoch<u16, Value>, Queue);

impl App {
    fn start(processor: Processor<u16, Value>) -> Result<Self> {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("retrieve");
        drop(fs::remove_dir_all(&dir));
        fs::create_dir_all(&dir)?;
        let path = dir.join("store.db");
        let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
        let config = || Config::<u16, Value> {
            store: Some(store.clone()),
            ..Config::default()
        };
        Rannoch::open(config())?.shutdown();
        let (signal, wake) = mpsc::channel();
        let append = Arc::new(Events::new(ready(&path)?, signal.clone()));
        let append = Some(append as Arc<dyn Append<u16, Value>>);
        let rannoch = Rannoch::open(Config { append, ..config() })?;
        let settings = Settings {
            attempts: 1,
            ..Settings::new(Duration::from_millis(100))
        };
        let (conn, sender, flows) = (ready(&path)?, rannoch.sender(), Arc::new(processor));
        let queue = Queue::start(conn, wake, sender, flows, settings);
        Ok(Self(path, signal, rannoch, queue))
    }

    /// An event of ty 10 with `data` from the app's forgr factory, and the queue signalled.
    fn hand(&self, data: &Value) -> Result<Uuid> {
        let id = Uuid::now_v7();
        let row = _events::factory().id(id.to_string()).ty(10);
        row.data(data.to_string())
            .status(0)
            .insert(&ready(&self.0)?)?;
        self.1.send(())?;
        Ok(id)
    }

    /// The (status, data) of the rows whose source is `id`, in order.
    fn about(&self, id: Uuid) -> Vec<(i64, Value)> {
        let sql = "SELECT status, data FROM _events WHERE source = ?1 ORDER BY id";
        let rows = ready(&self.0).ok().map(|c| {
            let row = |r: &rusqlite::Row| Ok((r.get(0)?, r.get::<_, String>(1)?));
            c.prepare(sql)?.query_map([id.to_string()], row)?.collect()
        });
        let rows: Vec<(i64, String)> = rows.and_then(rusqlite::Result::ok).unwrap_or_default();
        let parse = |d: &str| serde_json::from_str(d).unwrap_or(Value::Null);
        rows.into_iter().map(|(s, d)| (s, parse(&d))).collect()
    }

    fn stop(self) {
        drop(self.1);
        self.2.shutdown();
        self.3.join();
    }
}

#[test]
fn a_flow_step_queries_an_outside_source_the_app_reaches_through_the_retrieve_trait() {
    // Given a test app that implements the retrieve trait with a stub source, declaring that
    // it gives email as a string
    let sig: Sig = serde_json::from_str(SIG).unwrap();
    let retrieve = Arc::new(Retrieve::new(sig.clone(), Arc::new(Stub)));

    // And an app built with a flow whose step queries that source
    let docs: Vec<FlowDoc> = serde_json::from_str(FLOWS).unwrap();
    let docs: Vec<(String, FlowDoc)> = docs.into_iter().map(|d| (d.name.clone(), d)).collect();
    let flows = check(&docs, &[sig]).unwrap();
    let processor: Processor<u16, Value> = Processor::new(&flows, vec![retrieve]).unwrap();

    // Then the flow reports a job step
    assert!(processor.flow(10).unwrap().job);
    assert!(!processor.flow(11).unwrap().job);
    let app = App::start(processor).unwrap();

    // When the queue hands rannoch-processor an event the stub answers with an email string
    let found = app.hand(&json!({ "user": "sam@example.com" })).unwrap();

    // Then the step's next carries the email on
    assert!(until(|| app.about(found).iter().any(|r| r.0 == 2)));
    let sent = app.about(found);
    let nexts: Vec<&(i64, Value)> = sent.iter().filter(|r| r.0 == 0).collect();
    assert_eq!(nexts, [&(0, json!({ "email": "sam@example.com" }))]);

    // When the stub answers an event with email 7
    let wrong = app.hand(&json!({ "user": 7 })).unwrap();

    // Then the step sends a -1 whose data is 'email as number', and no next
    assert!(until(|| app.about(wrong).iter().any(|r| r.0 == -1)));
    thread::sleep(Duration::from_millis(200));
    let sent = app.about(wrong);
    assert_eq!(sent, [(1, json!("taken")), (-1, json!("email as number"))]);
    app.stop();
}
