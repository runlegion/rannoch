//! `@criterion-FR-RANNOCH-010-c1` and `-c2`, and `@criterion-FR-RANNOCH-005-c1`
//! to `-c3`, from `tests/features/`. Each builds its database in a directory
//! of its own: for FR-010 with the app's one migrator, opening rannoch with
//! the real store in its `Config`; for FR-005 as the app's forgr empty store,
//! opening rannoch with the events crate's append in its `Config`.
//! `@criterion-FR-RANNOCH-011-c2` runs cargo on a copy of this workspace, so
//! a developer's edit never touches the real checkout.

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
    Ok(rannoch_core::migrator()
        .status(&mut executor(path)?)?
        .iter()
        .filter(|s| s.applied)
        .map(|s| s.version)
        .collect())
}

/// The database at `path` with every migration but the newest applied, and
/// that newest version.
fn missing_newest(path: &Path) -> Result<u64> {
    let migrator = rannoch_core::migrator();
    let mut exec = executor(path)?;
    migrator.up(&mut exec)?;
    let down = migrator.down(&mut exec, 1)?;
    Ok(*down.reverted.first().ok_or("nothing reverted")?)
}

/// What the store reports pending at `path`.
fn pending(path: &Path) -> Result<Vec<u64>> {
    Ok(rannoch_core::migrator()
        .status(&mut executor(path)?)?
        .iter()
        .filter(|s| !s.applied)
        .map(|s| s.version)
        .collect())
}

/// The app's `Config`, carrying the real store over the database at `path`.
fn config(path: &Path) -> Config<u8, ()> {
    Config {
        store: Some(Arc::new(Store::new(
            path.to_owned(),
            rannoch_core::migrator(),
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
    rannoch_core::forgr::empty_store().execute("VACUUM INTO ?1", [path.to_string_lossy()])?;
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

/// The workspace root, two levels above this crate.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A fresh copy of the workspace's manifests and crates under `name`.
fn copy_workspace(name: &str) -> std::io::Result<PathBuf> {
    let to = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name).join("ws");
    if to.exists() {
        fs::remove_dir_all(&to)?;
    }
    fs::create_dir_all(&to)?;
    for file in ["Cargo.toml", "Cargo.lock", "clippy.toml"] {
        fs::copy(root().join(file), to.join(file))?;
    }
    copy_dir(&root().join("crates"), &to.join("crates"))?;
    Ok(to)
}

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let path = entry?.path();
        let dest = to.join(entry_name(&path)?);
        if path.is_dir() {
            copy_dir(&path, &dest)?;
        } else {
            fs::copy(&path, &dest)?;
        }
    }
    Ok(())
}

fn entry_name(path: &Path) -> std::io::Result<&std::ffi::OsStr> {
    path.file_name()
        .ok_or_else(|| std::io::Error::other(format!("{} has no name", path.display())))
}

/// Runs cargo in `ws` with a target directory of its own: cargo names a
/// workspace member's build the same at any path, so two copies sharing one
/// target directory would overwrite each other's build script.
fn cargo(ws: &Path, args: &[&str]) -> std::io::Result<process::Output> {
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    Command::new(cargo)
        .args(args)
        .current_dir(ws)
        .env("CARGO_TARGET_DIR", ws.with_file_name("target"))
        .output()
}

fn text(out: &process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn fr_rannoch_011_c2() {
    // Given a crate whose queries all match the app's schema
    let ws = copy_workspace("fr_rannoch_011_c2").unwrap();
    let check = [
        "test",
        "-p",
        "rannoch-core",
        "--lib",
        "forgr_sql_matches_schema",
    ];
    let out = cargo(&ws, &check).unwrap();
    assert!(out.status.success(), "{}", text(&out));

    // When a developer renames a column in a migration and builds again
    let migration = ws.join("crates/rannoch-events/migrations/20261114000004_events.json");
    let renamed = fs::read_to_string(&migration)
        .unwrap()
        .replace(r#""name": "ty""#, r#""name": "kind""#);
    fs::write(&migration, renamed).unwrap();
    let out = cargo(&ws, &check).unwrap();

    // Then cargo test fails at the query that still names the old column
    let queries = "crates/rannoch-events/src/queries.rs";
    let line = fs::read_to_string(root().join(queries))
        .unwrap()
        .lines()
        .position(|l| l.contains("INSERT INTO _events"))
        .unwrap()
        + 1;
    let report = text(&out);
    assert!(!out.status.success(), "{report}");
    assert!(
        report.contains(&format!(
            "{queries}:{line}: table _events has no column named ty"
        )),
        "{report}"
    );
}

/// One value of `pragma` on `conn`, as text.
fn pragma(conn: &rusqlite::Connection, pragma: &str) -> rusqlite::Result<String> {
    conn.query_row(&format!("PRAGMA {pragma}"), [], |r| {
        r.get::<_, rusqlite::types::Value>(0)
    })
    .map(|v| match v {
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Text(t) => t,
        other => format!("{other:?}"),
    })
}

#[test]
fn an_app_opens_rannoch_over_its_database() {
    // Given an app's database without its newest migration
    let path = database("an_app_opens_rannoch_over_its_database").unwrap();
    let newest = missing_newest(&path).unwrap();

    // When the app opens rannoch with its store
    let opened = Rannoch::<u8, ()>::open(Config {
        store: Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator()))),
        ..Config::default()
    })
    .unwrap();

    // Then the newest migration is applied before rannoch accepts an event
    assert!(applied(&path).unwrap().contains(&newest));
    assert_eq!(opened.sender().send(event()), Ok(Status::Added));
    opened.shutdown();

    // And every connection the store opens has foreign keys on, journal mode
    // WAL, a 64 MiB cache, automatic checkpoints off and fullfsync on
    let Opened::Ready(conn) = rannoch_store::open(&path, &rannoch_core::migrator()).unwrap() else {
        panic!("migrations still pending");
    };
    assert_eq!(pragma(&conn, "foreign_keys").unwrap(), "1");
    assert_eq!(pragma(&conn, "journal_mode").unwrap(), "wal");
    assert_eq!(pragma(&conn, "cache_size").unwrap(), "-65536");
    assert_eq!(pragma(&conn, "wal_autocheckpoint").unwrap(), "0");
    assert_eq!(pragma(&conn, "fullfsync").unwrap(), "1");

    // When the app sends events and then sends nothing for two seconds, with
    // a connection held open throughout so that no close checkpoints the WAL,
    // and rannoch holding the app's only store
    let held = rusqlite::Connection::open(&path).unwrap();
    held.query_row("SELECT count(*) FROM _events", [], |r| r.get::<_, i64>(0))
        .unwrap();
    let (queue, _signals) = mpsc::channel();
    let rannoch = Rannoch::<u8, ()>::open(Config {
        store: Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator()))),
        append: Some(Arc::new(Events::new(conn, queue))),
        ..Config::default()
    })
    .unwrap();
    let sender = rannoch.sender();
    for _ in 0..3 {
        sender.send(event()).unwrap();
    }
    thread::sleep(Duration::from_secs(2));

    // Then the WAL's frames are in the database file while rannoch runs: a
    // copy of the file alone, without its WAL, holds the events
    let copy = path.with_file_name("database-only.db");
    fs::copy(&path, &copy).unwrap();
    let kept: i64 = rusqlite::Connection::open(&copy)
        .unwrap()
        .query_row("SELECT count(*) FROM _events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(kept, 3);
    rannoch.shutdown();
    drop(held);

    // When the app opens rannoch over a path the store cannot open
    let missing = path.with_file_name("missing/store.db");
    let refused = Rannoch::<u8, ()>::open(Config {
        store: Some(Arc::new(Store::new(missing, rannoch_core::migrator()))),
        ..Config::default()
    });

    // Then open fails with Store and nothing starts
    assert!(matches!(refused, Err(Error::Store)), "{refused:?}");
}

/// Nanoseconds since the first call, for timing an event from its send to its commit.
fn nanos() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    u64::try_from(
        START
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

/// The events crate's append, recording each event's time from its send (the event's data)
/// to its append's commit.
#[derive(Debug)]
struct Timed {
    events: Events,
    send_to_commit: Mutex<Vec<u64>>,
}

impl Append<u8, u64> for Timed {
    fn append(&self, batch: &[Event<u8, u64>]) -> std::result::Result<(), Error> {
        let appended = Append::<u8, u64>::append(&self.events, batch);
        let committed = nanos();
        if appended.is_ok()
            && let Ok(mut times) = self.send_to_commit.lock()
        {
            times.extend(batch.iter().map(|e| committed.saturating_sub(e.data)));
        }
        appended
    }
}

/// The app's database at a fresh path under `name`, opened through the real store, with
/// 50,000 events already in `_events` from the app's forgr mock, each status picked from
/// the four values; rannoch open over it with
/// the timed append.
/// Rannoch over a kept history, the store it runs on, and its timed append.
type Kept = (Rannoch<u8, u64>, Arc<Store>, Arc<Timed>);

fn history_kept(name: &str) -> Result<Kept> {
    let path = database(name)?;
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    Rannoch::<u8, ()>::open(Config {
        store: Some(store.clone()),
        ..Config::default()
    })?
    .shutdown();
    let Opened::Ready(conn) = rannoch_store::open(&path, &rannoch_core::migrator())? else {
        return Err("migrations still pending".into());
    };
    // One transaction for the whole history, so building it is not a million commits.
    conn.execute_batch("BEGIN")?;
    rannoch_core::forgr::_events::mock(50_000)
        .with(rannoch_core::forgr::_events::Overrides {
            status: rannoch_core::forgr::Override::Pick(vec![-1, 0, 1, 2]),
            ..Default::default()
        })
        .insert(&conn)?;
    conn.execute_batch("COMMIT")?;
    // A store at rest: its history is in the database file, not waiting in the WAL.
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
    let (queue, signals) = mpsc::channel();
    thread::spawn(move || for () in signals {});
    let timed = Arc::new(Timed {
        events: Events::new(conn, queue),
        send_to_commit: Mutex::new(Vec::new()),
    });
    let rannoch = Rannoch::open(Config {
        store: Some(store.clone()),
        append: Some(timed.clone() as Arc<dyn Append<u8, u64>>),
        ..Config::default()
    })?;
    Ok((rannoch, store, timed))
}

fn sent_now(n: u64) -> Event<u8, u64> {
    Event {
        id: Uuid::nil(),
        source: None,
        ty: u8::try_from(n % 8).unwrap_or(0),
        data: nanos(),
        run_at: None,
        status: Status::Added,
    }
}

#[test]
fn rannoch_keeps_up_with_the_peak_load() {
    // Given rannoch is open over a store at rest already holding 50,000 events
    let (rannoch, _store, timed) = history_kept("nfr_rannoch_026").unwrap();

    // When the app sends 2,000 events at 200 a second
    let sender = rannoch.sender();
    let start = std::time::Instant::now();
    for n in 0..2_000_u64 {
        let due = start + Duration::from_millis(n * 5);
        if let Some(wait) = due.checked_duration_since(std::time::Instant::now()) {
            thread::sleep(wait);
        }
        sender.send(sent_now(n)).unwrap();
    }
    rannoch.shutdown();

    // Then all 2,000 are in _events
    let mut landed = timed.send_to_commit.lock().unwrap().clone();
    assert_eq!(landed.len(), 2_000);
    landed.sort_unstable();
    println!(
        "send to commit at 200 a second over 50,000 kept: p99 {} us",
        landed[(landed.len() - 1) * 99 / 100] / 1_000
    );
}

#[test]
fn a_flat_out_producer_beats_the_peak_load() {
    // Given rannoch is open over a store at rest already holding 50,000 events
    let (rannoch, _store, timed) = history_kept("nfr_rannoch_027").unwrap();

    // When one producer sends 100,000 events as fast as it can
    let sender = rannoch.sender();
    let start = std::time::Instant::now();
    for n in 0..100_000_u64 {
        sender.send(sent_now(n)).unwrap();
    }
    rannoch.shutdown();
    let elapsed = start.elapsed().as_secs_f64();

    // Then all 100,000 are in _events at more than 200 a second
    assert_eq!(timed.send_to_commit.lock().unwrap().len(), 100_000);
    let rate = 100_000.0 / elapsed;
    println!("flat out from one producer over 50,000 kept: {rate:.0} events a second");
    assert!(rate > 200.0, "{rate:.0} a second");
}
