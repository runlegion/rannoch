//! `@criterion-FR-RANNOCH-010-c1` and `-c2`, and `@criterion-FR-RANNOCH-005-c1`
//! to `-c3`, from `tests/features/`. Each builds its database in a directory
//! of its own: for FR-010 with the app's one migrator, opening rannoch with
//! the real store in its `Config`; for FR-005 as the app's forgr empty store,
//! opening rannoch with the events crate's append in its `Config`.
//! `@criterion-FR-RANNOCH-011-c2` runs cargo on a copy of this workspace, so
//! a developer's edit never touches the real checkout.
//!
//! Every test runs with the clock frozen and calls [`Rannoch::shutdown`] through
//! `spawn_blocking`. The first two scenarios of `fr_rannoch_131.feature` check the clock itself;
//! the third runs clippy on a copy of this workspace.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;
use std::{env, fs, process};

use chrono::TimeDelta;
use log::{Level, Log, Metadata, Record};
use rannoch_core::{Append, Config, Error, Event, InboxConfig, Rannoch, Status, clock};
use rannoch_events::Events;
use rannoch_store::{Opened, Store};
use uuid::Uuid;

/// Shuts `rannoch` down from async code: shutdown blocks, so it runs through `spawn_blocking`.
async fn shutdown<D: Send + 'static>(rannoch: Rannoch<u8, D>) {
    if let Err(e) = tokio::task::spawn_blocking(move || rannoch.shutdown()).await {
        panic!("shutdown did not return: {e}");
    }
}

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

#[tokio::test(start_paused = true)]
async fn fr_rannoch_010_c1() {
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

#[tokio::test(start_paused = true)]
async fn fr_rannoch_010_c2() {
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
fn event() -> Event<u8, String> {
    Event {
        id: Uuid::nil(),
        source: None,
        ty: 0,
        data: String::new(),
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
    append: Arc<dyn Append<u8, String>>,
    interval: TimeDelta,
) -> std::result::Result<Rannoch<u8, String>, Error> {
    Rannoch::open(Config {
        default_interval: interval,
        append: Some(append),
        ..Config::default()
    })
}

/// The events crate's append over the store at `path`, signalling `queue`.
fn events(path: &Path, queue: tokio::sync::mpsc::UnboundedSender<()>) -> Result<Arc<Events>> {
    Ok(Arc::new(Events::new(
        rusqlite::Connection::open(path)?,
        queue,
    )))
}

#[tokio::test(start_paused = true)]
async fn fr_rannoch_005_c1() {
    // Given events on an inbox's heap
    let path = empty_store("fr_rannoch_005_c1").unwrap();
    let (queue, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let rannoch = open_with(events(&path, queue).unwrap(), TimeDelta::milliseconds(200)).unwrap();
    let sender = rannoch.sender();
    for _ in 0..3 {
        assert_eq!(sender.send(event()), Ok(Status::Added));
    }
    assert_eq!(events_in(&path).unwrap(), 0);

    // When the inbox's batch interval passes
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Then the events crate's append receives them as one batch
    assert_eq!(signals.try_recv(), Ok(()));
    assert_eq!(events_in(&path).unwrap(), 3);

    // And the heap is empty: shutdown hands over what is left, and nothing is
    shutdown(rannoch).await;
    assert_eq!(
        signals.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    );
    assert_eq!(events_in(&path).unwrap(), 3);
}

/// Set in the child process `fr_rannoch_005_c2` runs: the store it crashes over.
const CRASH_STORE: &str = "RANNOCH_FR_005_C2_STORE";

#[tokio::test(start_paused = true)]
async fn fr_rannoch_005_c2() {
    if let Ok(path) = env::var(CRASH_STORE) {
        // The child: events on an inbox's heap, then the process stops
        // before the batch is handed over.
        let (queue, _signals) = tokio::sync::mpsc::unbounded_channel();
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

impl Append<u8, String> for Flaky {
    fn append(
        &self,
        batch: &[Event<u8, String>],
    ) -> std::result::Result<Vec<Event<u8, String>>, Error> {
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

#[tokio::test(start_paused = true)]
async fn fr_rannoch_005_c3() {
    static LOGGER: Once = Once::new();
    LOGGER.call_once(|| {
        log::set_logger(&ERRORS).unwrap();
        log::set_max_level(log::LevelFilter::Error);
    });

    // Given the events crate cannot append
    let path = empty_store("fr_rannoch_005_c3").unwrap();
    let (queue, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let flaky = Arc::new(Flaky {
        failing: AtomicBool::new(true),
        events: events(&path, queue).unwrap(),
    });
    let rannoch = open_with(flaky.clone(), TimeDelta::milliseconds(100)).unwrap();
    for _ in 0..2 {
        assert_eq!(rannoch.sender().send(event()), Ok(Status::Added));
    }

    // When an inbox's batch interval passes
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Then a Store error is logged
    assert!(store_errors_logged() > 0);

    // And the events are still on the heap: none reached _events
    assert_eq!(events_in(&path).unwrap(), 0);
    assert_eq!(
        signals.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    );

    // When the events crate can append again and the next interval passes
    flaky.failing.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Then the append receives them
    assert_eq!(signals.try_recv(), Ok(()));
    assert_eq!(events_in(&path).unwrap(), 2);
    shutdown(rannoch).await;
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

#[tokio::test(start_paused = true)]
async fn fr_rannoch_011_c2() {
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

#[tokio::test(start_paused = true)]
async fn an_app_opens_rannoch_over_its_database() {
    // Given an app's database without its newest migration
    let path = database("an_app_opens_rannoch_over_its_database").unwrap();
    let newest = missing_newest(&path).unwrap();

    // When the app opens rannoch with its store
    let opened = Rannoch::<u8, String>::open(Config {
        store: Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator()))),
        ..Config::default()
    })
    .unwrap();

    // Then the newest migration is applied before rannoch accepts an event
    assert!(applied(&path).unwrap().contains(&newest));
    assert_eq!(opened.sender().send(event()), Ok(Status::Added));
    shutdown(opened).await;

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
    let (queue, _signals) = tokio::sync::mpsc::unbounded_channel();
    let rannoch = Rannoch::<u8, String>::open(Config {
        store: Some(Arc::new(Store::new(path.clone(), rannoch_core::migrator()))),
        append: Some(Arc::new(Events::new(conn, queue))),
        ..Config::default()
    })
    .unwrap();
    let sender = rannoch.sender();
    for _ in 0..3 {
        sender.send(event()).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Then the WAL's frames are in the database file while rannoch runs: a
    // copy of the file alone, without its WAL, holds the events
    let copy = path.with_file_name("database-only.db");
    fs::copy(&path, &copy).unwrap();
    let kept: i64 = rusqlite::Connection::open(&copy)
        .unwrap()
        .query_row("SELECT count(*) FROM _events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(kept, 3);
    shutdown(rannoch).await;
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

/// Set in the child process a `core_settings` scenario runs: the settings file it opens with.
const CORE_SETTINGS: &str = "RANNOCH_CORE_SETTINGS";

/// Runs `test` alone in a child process of this test binary, with `settings` as the file it
/// opens with, and checks it passed. The settings are held for the life of the process.
fn with_settings(test: &str, settings: &Path) -> Result<()> {
    let output = Command::new(env::current_exe()?)
        .args([test, "--exact", "--nocapture"])
        .env(CORE_SETTINGS, settings)
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    assert!(stdout.contains("1 passed"), "{stdout}");
    Ok(())
}

/// Opens rannoch over a fresh store under `name` with the settings file at `settings` and a
/// default inbox of an hour, sends one event, and returns rannoch and the append's signals.
fn open_an_hour(
    name: &str,
    settings: PathBuf,
) -> Result<(
    Rannoch<u8, String>,
    tokio::sync::mpsc::UnboundedReceiver<()>,
)> {
    let path = empty_store(name)?;
    let (queue, signals) = tokio::sync::mpsc::unbounded_channel();
    let rannoch = Rannoch::open(Config {
        default_interval: TimeDelta::hours(1),
        append: Some(events(&path, queue)?),
        settings: Some(settings),
        ..Config::default()
    })?;
    rannoch.sender().send(event())?;
    Ok((rannoch, signals))
}

#[tokio::test(start_paused = true)]
async fn core_settings_c1() -> Result<()> {
    if let Ok(settings) = env::var(CORE_SETTINGS) {
        // When Rannoch::open is called with that file's path and an event is sent
        let (rannoch, mut signals) = open_an_hour("core_settings_c1", settings.into())?;

        // Then the default inbox hands it over at the interval the [core] section gives
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(signals.try_recv(), Ok(()));
        shutdown(rannoch).await;
        return Ok(());
    }
    // Given a settings file whose [core] section sets default_interval_ms
    let settings = database("core_settings_c1_file")?.with_file_name("app.toml");
    fs::write(&settings, "[core]\ndefault_interval_ms = 50\n")?;
    with_settings("core_settings_c1", &settings)
}

#[tokio::test(start_paused = true)]
async fn core_settings_c2() -> Result<()> {
    if let Ok(settings) = env::var(CORE_SETTINGS) {
        // When Rannoch::open is called with that file's path and an event is sent
        let (rannoch, mut signals) = open_an_hour("core_settings_c2", settings.into())?;

        // Then the default inbox does not hand it over before shutdown
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            signals.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
        shutdown(rannoch).await;
        assert_eq!(signals.try_recv(), Ok(()));
        return Ok(());
    }
    // Given a settings file with no [core] section
    let settings = database("core_settings_c2_file")?.with_file_name("app.toml");
    fs::write(&settings, "[queue]\ndepth = 4\n")?;
    with_settings("core_settings_c2", &settings)
}

/// An append keeping the `ty` and id of every event handed to it.
#[derive(Debug, Default)]
struct Ids(Mutex<Vec<(u8, Uuid)>>);

impl Append<u8, String> for Ids {
    fn append(
        &self,
        batch: &[Event<u8, String>],
    ) -> std::result::Result<Vec<Event<u8, String>>, Error> {
        let mut kept = self.0.lock().map_err(|_| Error::Store)?;
        kept.extend(batch.iter().map(|e| (e.ty, e.id)));
        Ok(Vec::new())
    }
}

/// The time inside a `UUIDv7`, in milliseconds since the Unix epoch.
fn id_millis(id: Uuid) -> i64 {
    let Some(timestamp) = id.get_timestamp() else {
        panic!("{id} carries no time");
    };
    let (seconds, nanos) = timestamp.to_unix();
    i64::try_from(seconds).unwrap_or(i64::MAX / 1_000) * 1_000 + i64::from(nanos / 1_000_000)
}

#[tokio::test(start_paused = true)]
async fn a_frozen_clock_does_not_move() {
    // Given a test running with the clock frozen
    let ids = Arc::new(Ids::default());
    let rannoch = open_with(ids.clone(), TimeDelta::hours(1)).unwrap();

    // When it reads the clock's wall time, sends an event, waits on nothing and reads the wall
    // time and sends an event again
    let first = clock::now();
    rannoch.sender().send(event()).unwrap();
    tokio::task::yield_now().await;
    let second = clock::now();
    rannoch.sender().send(event()).unwrap();
    shutdown(rannoch).await;

    // Then both wall times are equal and both events' ids carry that same time
    assert_eq!(first, second);
    let kept = ids.0.lock().unwrap().clone();
    assert_eq!(kept.len(), 2);
    for (_, id) in kept {
        assert_eq!(id_millis(id), first.timestamp_millis());
    }
}

#[tokio::test(start_paused = true)]
async fn advancing_the_clock_moves_time_exactly_and_wakes_the_waits_it_passes() {
    // Given a frozen clock, and two waits with deadlines 50 ms and 200 ms from now: an inbox at
    // 50 ms for ty 1 and the default inbox at 200 ms, each holding one event
    let ids = Arc::new(Ids::default());
    let rannoch = Rannoch::open(Config {
        default_interval: TimeDelta::milliseconds(200),
        inboxes: vec![InboxConfig {
            name: "short".to_owned(),
            types: vec![1],
            interval: TimeDelta::milliseconds(50),
        }],
        append: Some(ids.clone() as Arc<dyn Append<u8, String>>),
        ..Config::default()
    })
    .unwrap();
    rannoch.sender().send(Event { ty: 1, ..event() }).unwrap();
    rannoch.sender().send(event()).unwrap();
    // The inboxes' tasks start their waits once they first run.
    tokio::task::yield_now().await;
    let (wall, elapsed, id) = (clock::now(), clock::elapsed(), clock::new_id());

    // When the test advances the clock by 100 ms
    tokio::time::advance(Duration::from_millis(100)).await;
    // The wait the advance passed wakes its task; the yield lets that task run.
    tokio::task::yield_now().await;

    // Then the wall time, the elapsed time and a new id's time have each moved by exactly
    // 100 ms, the 50 ms wait has woken and the 200 ms wait has not
    assert_eq!(clock::now() - wall, TimeDelta::milliseconds(100));
    assert_eq!(clock::elapsed() - elapsed, Duration::from_millis(100));
    assert_eq!(id_millis(clock::new_id()) - id_millis(id), 100);
    let handed: Vec<u8> = ids.0.lock().unwrap().iter().map(|(ty, _)| *ty).collect();
    assert_eq!(handed, [1]);
    shutdown(rannoch).await;
}

#[tokio::test(start_paused = true)]
async fn no_real_time_call_outside_the_clock() {
    // Given the rannoch workspace with clippy's disallowed-methods rule on
    let ws = copy_workspace("no_real_time_call_outside_the_clock").unwrap();
    let clippy = [
        "clippy",
        "--workspace",
        "--all-targets",
        "--",
        "-D",
        "warnings",
    ];

    // When clippy runs over every crate, its code and its tests
    let out = cargo(&ws, &clippy).unwrap();

    // Then it passes, and adding a call to any of std::thread::sleep, Instant::now,
    // SystemTime::now, Utc::now or Uuid::now_v7 outside rannoch-core's clock makes it fail
    assert!(out.status.success(), "{}", text(&out));
    let lib = ws.join("crates/rannoch-core/src/lib.rs");
    let mut source = fs::read_to_string(&lib).unwrap();
    source.push_str(
        "\npub fn real_time() {\n    std::thread::sleep(std::time::Duration::ZERO);\n    \
         let _ = std::time::Instant::now();\n    let _ = std::time::SystemTime::now();\n    \
         let _ = chrono::Utc::now();\n    let _ = uuid::Uuid::now_v7();\n}\n",
    );
    fs::write(&lib, source).unwrap();
    let out = cargo(&ws, &clippy).unwrap();
    let report = text(&out);
    assert!(!out.status.success(), "{report}");
    for method in [
        "std::thread::sleep",
        "std::time::Instant::now",
        "std::time::SystemTime::now",
        "chrono::Utc::now",
        "uuid::Uuid::now_v7",
    ] {
        assert!(
            report.contains(&format!("use of a disallowed method `{method}`")),
            "{method}: {report}"
        );
    }
}

#[test]
fn ids_from_one_rannoch_keep_their_order() {
    const SENT: usize = 200;
    // One frozen test: a Rannoch on a paused runtime of its own whose clock stands `ahead` past
    // its start, sending `SENT` events in one frozen millisecond and waiting on `turn` after
    // each; it returns the ids its append kept, in send order.
    let frozen_test = |ahead: Duration, turn: Arc<std::sync::Barrier>| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async move {
            tokio::time::advance(ahead).await;
            let ids = Arc::new(Ids::default());
            let rannoch = open_with(ids.clone(), TimeDelta::hours(1)).unwrap();
            let first = clock::now();
            for _ in 0..SENT {
                rannoch.sender().send(event()).unwrap();
                turn.wait();
            }
            assert_eq!(clock::now(), first);
            shutdown(rannoch).await;
            ids.0
                .lock()
                .unwrap()
                .iter()
                .map(|(_, id)| *id)
                .collect::<Vec<Uuid>>()
        })
    };

    // Given two Rannochs in one process, each in its own frozen test running in parallel: one
    // clock stands an hour past the other
    let turn = Arc::new(std::sync::Barrier::new(2));
    let tests = [Duration::ZERO, Duration::from_secs(3_600)].map(|ahead| {
        let turn = Arc::clone(&turn);
        std::thread::spawn(move || frozen_test(ahead, turn))
    });

    // When each sends events in the same frozen millisecond, the two taking turns
    for test in tests {
        let ids = test.join().unwrap();

        // Then each Rannoch's ids strictly increase in the order its events were sent
        assert_eq!(ids.len(), SENT);
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    }
}
