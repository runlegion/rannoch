//! `@criterion-FR-RANNOCH-010-c1` and `-c2`, and `@criterion-FR-RANNOCH-005-c1`
//! to `-c3`, from `tests/features/`. Each builds its database in a directory
//! of its own: for FR-010 with the app's one migrator, opening rannoch with
//! the real store in its `Config`; for FR-005 as the app's forgr empty store,
//! opening rannoch with the events crate's append in its `Config`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once, mpsc};
use std::time::Duration;
use std::{env, fs, process, thread};

use chrono::TimeDelta;
use log::{Level, Log, Metadata, Record};
use rannoch_core::{Append, Config, Error, Event, Rannoch, Status};
use rannoch_events::Events;
use rannoch_store::{Opened, Store};
use uuid::Uuid;

/// A fresh, empty directory for `name`, and the database path inside it.
fn database(name: &str) -> std::io::Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir.join("store.db"))
}

/// An executor over the database at `path`, for building and reading it.
fn executor(path: &Path) -> rusqlite::Result<migratr::RusqliteExecutor> {
    rusqlite::Connection::open(path).map(migratr::RusqliteExecutor::new)
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The versions of the app's migrations applied to the database at `path`.
fn applied(path: &Path) -> Result<Vec<u64>> {
    Ok(rannoch_app::migrator()
        .status(&mut executor(path)?)?
        .iter()
        .filter(|s| s.applied)
        .map(|s| s.version)
        .collect())
}

/// The database at `path` with every migration but the newest applied, and
/// that newest version.
fn missing_newest(path: &Path) -> Result<u64> {
    let migrator = rannoch_app::migrator();
    let mut exec = executor(path)?;
    migrator.up(&mut exec)?;
    let down = migrator.down(&mut exec, 1)?;
    Ok(*down.reverted.first().ok_or("nothing reverted")?)
}

/// What the store reports pending at `path`.
fn pending(path: &Path) -> Result<Vec<u64>> {
    Ok(match rannoch_store::open(path, &rannoch_app::migrator())? {
        Opened::Ready(_) => Vec::new(),
        Opened::Pending(pending) => pending,
    })
}

/// The app's `Config`, carrying the real store over the database at `path`.
fn config(path: &Path) -> Config<u8, ()> {
    Config {
        store: Some(Arc::new(Store::new(
            path.to_owned(),
            rannoch_app::migrator(),
        ))),
        ..Config::default()
    }
}

#[test]
fn fr_rannoch_010_c1() {
    // Given a database missing the newest migration
    let path = database("fr_rannoch_010_c1").unwrap();
    let newest = missing_newest(&path).unwrap();
    let pending_before = pending(&path).unwrap();
    let applied_before = applied(&path).unwrap();
    assert_eq!(pending_before, [newest]);

    // When the app opens rannoch
    let rannoch = Rannoch::<u8, ()>::open(config(&path)).unwrap();

    // Then the newest migration is applied
    let applied_after = applied(&path).unwrap();
    let applied_by_open: Vec<u64> = applied_after
        .iter()
        .copied()
        .filter(|v| !applied_before.contains(v))
        .collect();
    println!("pending before open: {pending_before:?}; applied by open: {applied_by_open:?}");
    assert_eq!(applied_by_open, [newest]);
    assert_eq!(pending(&path).unwrap(), Vec::<u64>::new());

    // And rannoch is open
    let event = Event {
        id: Uuid::nil(),
        source: None,
        ty: 0,
        data: (),
        run_at: None,
        status: Status::Added,
    };
    assert_eq!(rannoch.sender().send(event), Ok(Status::Added));
}

#[test]
fn fr_rannoch_010_c2() {
    // Given a pending migration that cannot be applied: the real store on a
    // read-only database
    let path = database("fr_rannoch_010_c2").unwrap();
    let newest = missing_newest(&path).unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&path, permissions).unwrap();
    assert_eq!(pending(&path).unwrap(), [newest]);
    let before = fs::read(&path).unwrap();

    // When the app opens rannoch
    let opened = Rannoch::<u8, ()>::open(config(&path));

    // Then open fails with Store
    assert!(matches!(opened, Err(Error::Store)), "{opened:?}");

    // And nothing has started: no handle came back, and the database is
    // unchanged, its newest migration still pending
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(pending(&path).unwrap(), [newest]);
}

/// An event as the app sends it, with no source.
fn event() -> Event<u8, ()> {
    Event {
        id: Uuid::nil(),
        source: None,
        ty: 0,
        data: (),
        run_at: None,
        status: Status::Added,
    }
}

/// The app's forgr empty store, written to its own file under `name`.
fn empty_store(name: &str) -> Result<PathBuf> {
    let path = database(name)?;
    rannoch_app::forgr::empty_store().execute("VACUUM INTO ?1", [path.to_string_lossy()])?;
    Ok(path)
}

/// The number of rows in `_events` at `path`, read through a connection of its own.
fn events_in(path: &Path) -> Result<i64> {
    Ok(rusqlite::Connection::open(path)?
        .query_row("SELECT count(*) FROM _events", [], |r| r.get(0))?)
}

/// Rannoch with one default inbox at `interval`, handing over to `append`.
fn open_with(
    append: Arc<dyn Append<u8, ()>>,
    interval: TimeDelta,
) -> std::result::Result<Rannoch<u8, ()>, Error> {
    Rannoch::open(Config {
        default_interval: interval,
        append: Some(append),
        ..Config::default()
    })
}

/// The events crate's append over the store at `path`, signalling `queue`.
fn events(path: &Path, queue: mpsc::Sender<()>) -> Result<Arc<Events>> {
    Ok(Arc::new(Events::new(
        rusqlite::Connection::open(path)?,
        queue,
    )))
}

#[test]
fn fr_rannoch_005_c1() {
    // Given events on an inbox's heap
    let path = empty_store("fr_rannoch_005_c1").unwrap();
    let (queue, signals) = mpsc::channel();
    let rannoch = open_with(events(&path, queue).unwrap(), TimeDelta::milliseconds(200)).unwrap();
    let sender = rannoch.sender();
    for _ in 0..3 {
        assert_eq!(sender.send(event()), Ok(Status::Added));
    }
    assert_eq!(events_in(&path).unwrap(), 0);

    // When the inbox's batch interval passes
    let signalled = signals.recv_timeout(Duration::from_secs(5));

    // Then the events crate's append receives them as one batch
    assert_eq!(signalled, Ok(()));
    assert_eq!(events_in(&path).unwrap(), 3);

    // And the heap is empty: shutdown hands over what is left, and nothing is
    rannoch.shutdown();
    assert_eq!(signals.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    assert_eq!(events_in(&path).unwrap(), 3);
}

/// Set in the child process `fr_rannoch_005_c2` runs: the store it crashes over.
const CRASH_STORE: &str = "RANNOCH_FR_005_C2_STORE";

#[test]
fn fr_rannoch_005_c2() {
    if let Ok(path) = env::var(CRASH_STORE) {
        // The child: events on an inbox's heap, then the process stops
        // before the batch is handed over.
        let (queue, _signals) = mpsc::channel();
        let rannoch = open_with(
            events(Path::new(&path), queue).unwrap(),
            TimeDelta::hours(1),
        )
        .unwrap();
        for _ in 0..3 {
            assert_eq!(rannoch.sender().send(event()), Ok(Status::Added));
        }
        println!("accepted 3");
        process::abort();
    }

    // Given events on an inbox's heap
    let path = empty_store("fr_rannoch_005_c2").unwrap();

    // When the process stops before the batch is handed over
    let child = Command::new(env::current_exe().unwrap())
        .args(["fr_rannoch_005_c2", "--exact", "--nocapture"])
        .env(CRASH_STORE, &path)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(stdout.contains("accepted 3"), "{stdout}");
    assert!(!child.status.success());

    // Then those events are not in _events
    assert_eq!(events_in(&path).unwrap(), 0);
}

/// The events crate's append, failing with `Store` while `failing` is set.
#[derive(Debug)]
struct Flaky {
    failing: AtomicBool,
    events: Arc<Events>,
}

impl Append<u8, ()> for Flaky {
    fn append(&self, batch: &[Event<u8, ()>]) -> std::result::Result<(), Error> {
        if self.failing.load(Ordering::SeqCst) {
            Err(Error::Store)
        } else {
            self.events.append(batch)
        }
    }
}

/// Every error line logged in this test binary.
struct Errors(Mutex<Vec<String>>);

impl Log for Errors {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() == Level::Error
    }
    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata())
            && let Ok(mut lines) = self.0.lock()
        {
            lines.push(record.args().to_string());
        }
    }
    fn flush(&self) {}
}

static ERRORS: Errors = Errors(Mutex::new(Vec::new()));

fn store_errors_logged() -> usize {
    let store = Error::Store.to_string();
    ERRORS.0.lock().map_or(0, |lines| {
        lines.iter().filter(|l| l.contains(&store)).count()
    })
}

#[test]
fn fr_rannoch_005_c3() {
    static LOGGER: Once = Once::new();
    LOGGER.call_once(|| {
        log::set_logger(&ERRORS).unwrap();
        log::set_max_level(log::LevelFilter::Error);
    });

    // Given the events crate cannot append
    let path = empty_store("fr_rannoch_005_c3").unwrap();
    let (queue, signals) = mpsc::channel();
    let flaky = Arc::new(Flaky {
        failing: AtomicBool::new(true),
        events: events(&path, queue).unwrap(),
    });
    let rannoch = open_with(flaky.clone(), TimeDelta::milliseconds(100)).unwrap();
    for _ in 0..2 {
        assert_eq!(rannoch.sender().send(event()), Ok(Status::Added));
    }

    // When an inbox's batch interval passes
    for _ in 0..50 {
        if store_errors_logged() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    // Then a Store error is logged
    assert!(store_errors_logged() > 0);

    // And the events are still on the heap: none reached _events
    assert_eq!(events_in(&path).unwrap(), 0);
    assert_eq!(signals.try_recv(), Err(mpsc::TryRecvError::Empty));

    // When the events crate can append again and the next interval passes
    flaky.failing.store(false, Ordering::SeqCst);
    let signalled = signals.recv_timeout(Duration::from_secs(5));

    // Then the append receives them
    assert_eq!(signalled, Ok(()));
    assert_eq!(events_in(&path).unwrap(), 2);
    rannoch.shutdown();
}
