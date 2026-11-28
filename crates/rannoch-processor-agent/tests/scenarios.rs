//! FR-RANNOCH-120's scenario from `tests/features/lib.feature` (`@criterion-FR-RANNOCH-120-c1`),
//! on rannoch-processor's scenario fixture: its store and queue setup, copied. The test app's
//! agent is a stub; its Sig is `tests/signatures.json`, its flows `tests/flows.json`.
//!
//! And `tests/features/clock.feature`, one scenario for each of #169's criteria:
//! rannoch-processor-agent reads and waits on time only through rannoch-core's clock and tokio,
//! and every test runs with the clock frozen.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rannoch_core::clock;
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_processor::{Action, FlowDoc, Processor, Sig, check};
use rannoch_processor_agent::{Agent, AgentAction};
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
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The test app's stub agent classify: counts its calls; answers "unsure" with no severity.
#[derive(Default)]
struct Stub(AtomicUsize);
impl Agent for Stub {
    fn ask(&self, input: &Obj) -> std::result::Result<Obj, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let answer = match input.get("text").and_then(Value::as_str) {
            Some("unsure") => json!({}),
            _ => json!({ "severity": 2 }),
        };
        Ok(answer.as_object().cloned().unwrap_or_default())
    }
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// The (ty, status, data) of the rows whose source is `id`, in order.
fn about(read: &Connection, id: Uuid) -> Vec<(i64, i64, Value)> {
    let sql = "SELECT ty, status, data FROM _events WHERE source = ?1 ORDER BY id";
    let rows = read.prepare(sql).and_then(|mut select| {
        let row = |r: &rusqlite::Row| {
            let data: String = r.get(2)?;
            let data = serde_json::from_str(&data).unwrap_or(Value::Null);
            Ok((r.get(0)?, r.get(1)?, data))
        };
        select.query_map([id.to_string()], row)?.collect()
    });
    rows.unwrap_or_default()
}

/// Waits up to five seconds on the clock for a row at `status` about `id`, then gives the rows
/// about it.
async fn ended(read: &Connection, id: Uuid, status: i64) -> Vec<(i64, i64, Value)> {
    let start = clock::elapsed();
    while start.elapsed() < Duration::from_secs(5) && about(read, id).iter().all(|r| r.1 != status)
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    about(read, id)
}

#[tokio::test(start_paused = true)]
async fn a_flow_step_calls_an_agent_the_app_reaches_through_the_agent_trait() -> Result<()> {
    // Given a test app that implements the agent trait with a stub agent classify, declaring
    // that it takes text as a string and gives severity
    let sigs: Vec<Sig> = serde_json::from_str(include_str!("signatures.json"))?;
    let stub = Arc::new(Stub::default());
    let agent = Arc::new(AgentAction::new(sigs[0].clone(), stub.clone())) as Arc<dyn Action>;

    // And an app built with a flow whose step calls classify
    let docs: Vec<FlowDoc> = serde_json::from_str(include_str!("flows.json"))?;
    let docs: Vec<_> = docs.into_iter().map(|d| (d.name.clone(), d)).collect();
    let flows = check(&docs, &sigs).map_err(|e| e.join("\n"))?;
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("calls_an_agent");
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let path = dir.join("store.db");
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    let config = || Config::<Ty, Value> {
        store: Some(store.clone()),
        ..Config::default()
    };
    // Shutdown blocks, so it runs through `spawn_blocking`.
    let migrated = Rannoch::open(config())?;
    tokio::task::spawn_blocking(move || migrated.shutdown()).await?;
    let processor = Arc::new(Processor::<Ty, Value>::new(
        &flows,
        vec![agent],
        ready(&path)?,
    )?);
    let (signal, wake) = tokio::sync::mpsc::unbounded_channel();
    let append = Arc::new(Events::new(ready(&path)?, signal.clone())) as Arc<dyn Append<Ty, Value>>;
    let append = Some(append);
    let rannoch = Rannoch::open(Config { append, ..config() })?;
    let settings = Settings {
        attempts: 1,
        ..Settings::new(Duration::from_millis(100))
    };
    let (sender, read) = (rannoch.sender(), ready(&path)?);
    let queue = Queue::start(ready(&path)?, wake, sender, processor.clone(), settings);
    let hand = |data: Value| -> Result<Uuid> {
        let id = clock::new_id();
        let row = _events::factory().id(id.to_string()).ty(60);
        row.data(data.to_string()).status(0).insert(&read)?;
        signal.send(())?;
        Ok(id)
    };

    // Then the flow reports a job step
    assert!(processor.flow(60).is_some_and(|f| f.job));
    assert!(processor.flow(61).is_some_and(|f| !f.job));

    // When the queue hands rannoch-processor an event whose text the stub answers with a severity
    let rated = hand(json!({ "text": "it crashed" }))?;

    // Then the step's next carries the severity on
    let next = (61, 0, json!({ "severity": 2 }));
    let done = (60, 2, json!("done"));
    assert_eq!(
        ended(&read, rated, 2).await,
        [(60, 1, json!("taken")), next, done]
    );

    // When the queue hands rannoch-processor an event whose text is the number 5
    let calls = stub.0.load(Ordering::SeqCst);
    let five = hand(json!({ "text": 5 }))?;

    // Then the step sends a -1 whose data is 'agent classify input gave text as number, it
    // declares string', and the stub is not called
    let failed = json!("agent classify input gave text as number, it declares string");
    let rows = [(60, 1, json!("taken")), (60, -1, failed)];
    assert_eq!(ended(&read, five, -1).await, rows);
    assert_eq!(
        stub.0.load(Ordering::SeqCst),
        calls,
        "the stub is not called"
    );

    // When the stub answers an event without a severity
    let unsure = hand(json!({ "text": "unsure" }))?;

    // Then the step sends a -1 whose data is 'agent classify gave no severity', and no next
    let failed = json!("agent classify gave no severity");
    let rows = [(60, 1, json!("taken")), (60, -1, failed)];
    assert_eq!(ended(&read, unsure, -1).await, rows);

    // Shutdown and join block, so they run through `spawn_blocking`.
    drop(signal);
    tokio::task::spawn_blocking(move || {
        rannoch.shutdown();
        queue.join()
    })
    .await??;
    Ok(())
}

/// Every `.rs` file of rannoch-processor-agent's code and tests, with its path.
fn sources() -> std::io::Result<Vec<(PathBuf, String)>> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in ["src", "tests"] {
        for entry in fs::read_dir(crate_dir.join(dir))? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "rs") {
                let text = fs::read_to_string(&path)?;
                files.push((path, text));
            }
        }
    }
    Ok(files)
}

#[tokio::test(start_paused = true)]
async fn rannoch_processor_agent_calls_no_real_time_function() -> Result<()> {
    // Given rannoch-processor-agent's code and tests
    let files = sources()?;
    assert_eq!(files.len(), 2, "{files:?}");

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

    // Then none is found, and their time and waiting go through rannoch-core's clock and tokio
    for (path, text) in &files {
        for call in &calls {
            assert!(
                !text.contains(call.as_str()),
                "{} calls {call}",
                path.display()
            );
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn every_rannoch_processor_agent_test_runs_with_the_clock_frozen() -> Result<()> {
    // Given rannoch-processor-agent's tests
    let files = sources()?;

    // When they are read for how each one runs
    let mut runs = 0;
    for (path, text) in &files {
        for line in text.lines().map(str::trim) {
            // Then none runs as a plain test, each freezes the clock, and none resumes it
            assert_ne!(line, "#[test]", "{} has a plain test", path.display());
            assert!(
                !line.contains(&format!("{}(", "resume")),
                "{}: {line}",
                path.display()
            );
            if line.starts_with("#[tokio::test") {
                assert_eq!(
                    line,
                    "#[tokio::test(start_paused = true)]",
                    "{}",
                    path.display()
                );
                runs += 1;
            }
        }
    }
    // 3 in the library and 3 here
    assert_eq!(runs, 6);
    Ok(())
}
