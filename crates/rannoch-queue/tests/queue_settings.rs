//! Issue #152's scenarios, from `tests/features/queue_settings.feature`, one test each. The
//! settings are held for the life of the process, so each test re-runs this test binary as a
//! child process that runs the scenario alone with the settings file's path; the parent writes
//! the file and checks that the child ran the scenario and passed.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Event, Rannoch, Sender, Status, Storage};
use rannoch_events::Events;
use rannoch_queue::{Flow, Flows, Queue, Row, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Set in the child process, which runs the scenario's body.
const CHILD: &str = "RANNOCH_QUEUE_SETTINGS_CHILD";

/// Set in the child process to the settings file's path.
const SETTINGS: &str = "RANNOCH_QUEUE_SETTINGS_FILE";

/// The app's `ty`.
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

/// One flow, for `ty` 4, whose run sends a -1 about the event it runs and fails, every time.
struct Failing(Arc<Flow<Ty, String>>);
impl Flows<Ty, String> for Failing {
    fn flow(&self, ty: u16) -> Option<Arc<Flow<Ty, String>>> {
        (ty == 4).then(|| Arc::clone(&self.0))
    }
}

fn failing() -> Failing {
    Failing(Arc::new(Flow {
        job: false,
        run: Box::new(|event: &Row, _: &Row, sender: &Sender<Ty, String>| {
            let failure = format!("failed {}", event.id);
            let _ = sender.send(Event {
                id: Uuid::nil(),
                source: Some(event.id),
                ty: Ty(event.ty),
                data: failure.clone(),
                run_at: None,
                status: Status::Binned,
            });
            Err(failure)
        }),
    }))
}

/// An empty directory of the scenario's own.
fn dir(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("queue_settings")
        .join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Runs `test` alone in a child process of this test binary, with the settings file `settings`
/// written in the scenario's directory, and checks it passed.
fn child(test: &str, settings: &str) -> Result<()> {
    let path = dir(test)?.join("app.toml");
    fs::write(&path, settings)?;
    let output = Command::new(env::current_exe()?)
        .args([test, "--exact", "--nocapture"])
        .env(CHILD, "1")
        .env(SETTINGS, &path)
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed"), "{stdout}");
    Ok(())
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// The ids of the rows at `status` whose source is `id`.
fn about(conn: &Connection, id: Uuid, status: i64) -> Result<Vec<Uuid>> {
    let ids: Vec<String> = conn
        .prepare("SELECT id FROM _events WHERE source = ?1 AND status = ?2 ORDER BY id")?
        .query_map((id.to_string(), status), |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(ids.iter().filter_map(|id| id.parse().ok()).collect())
}

/// Waits up to five seconds for the retry of `id`.
fn retry_of(conn: &Connection, id: Uuid) -> Result<Option<Uuid>> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if let Some(retry) = about(conn, id, 0)?.first() {
            return Ok(Some(*retry));
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(about(conn, id, 0)?.first().copied())
}

/// Waits up to five seconds for a -1 about `id`.
fn binned(conn: &Connection, id: Uuid) -> Result<bool> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if !about(conn, id, -1)?.is_empty() {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(!about(conn, id, -1)?.is_empty())
}

/// Opens rannoch with the settings file the parent wrote, starts the queue with `settings` over
/// the failing flow, and sends one event for it. Returns the attempts the chain ran: the event
/// and each retry, each followed by its -1.
fn attempts(name: &str, settings: Settings) -> Result<usize> {
    let path = dir(&format!("{name}-store"))?.join("store.db");
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    if store.pending()? {
        store.apply()?;
    }
    let (signal, wake) = mpsc::channel();
    let rannoch = Rannoch::open(Config {
        settings: Some(PathBuf::from(env::var(SETTINGS)?)),
        store: Some(store),
        append: Some(
            Arc::new(Events::new(ready(&path)?, signal.clone())) as Arc<dyn Append<Ty, String>>
        ),
        ..Config::default()
    })?;
    let queue = Queue::start(
        ready(&path)?,
        wake,
        rannoch.sender(),
        Arc::new(failing()),
        settings,
    );
    let read = ready(&path)?;

    let mut attempt = Uuid::now_v7();
    _events::factory()
        .id(attempt.to_string())
        .ty(4)
        .data("{}")
        .status(0)
        .insert(&read)?;
    signal.send(())?;

    let mut ran = 0;
    while binned(&read, attempt)? {
        ran += 1;
        // A chain with attempts left sends its retry within the first wait.
        let Some(retry) = retry_of(&read, attempt)? else {
            break;
        };
        attempt = retry;
    }

    drop(signal);
    rannoch.shutdown();
    queue.join()?;
    Ok(ran)
}

#[test]
fn the_queue_sections_attempts_replaces_the_attempts_passed_to_start() -> Result<()> {
    const NAME: &str = "the_queue_sections_attempts_replaces_the_attempts_passed_to_start";
    if env::var(CHILD).is_ok() {
        // And Settings with 10 attempts passed to Queue::start
        let settings = Settings::new(Duration::from_millis(100));
        assert_eq!(settings.attempts, 10);

        // When a run that always fails is taken
        // Then it is binned after its second attempt, and no third attempt is sent
        assert_eq!(attempts(NAME, settings)?, 2);
        return Ok(());
    }
    // Given a settings file whose [queue] section sets attempts = 2
    child(NAME, "[queue]\nattempts = 2\n")
}

#[test]
fn with_no_queue_section_the_queue_behaves_as_today() -> Result<()> {
    const NAME: &str = "with_no_queue_section_the_queue_behaves_as_today";
    if env::var(CHILD).is_ok() {
        // And Settings with 3 attempts passed to Queue::start
        let settings = Settings {
            attempts: 3,
            ..Settings::new(Duration::from_millis(100))
        };

        // When a run that always fails is taken
        // Then it is binned after its third attempt, as the Settings passed in say
        assert_eq!(attempts(NAME, settings)?, 3);
        return Ok(());
    }
    // Given a settings file with no [queue] section
    child(NAME, "[app]\nname = \"kessel\"\n")
}
