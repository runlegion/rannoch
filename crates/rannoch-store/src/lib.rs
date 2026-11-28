#![warn(missing_docs)]
//! The store: [`open`] opens the app's `SQLite` database against the app's one
//! migrator, reports pending migrations and never applies them, and hands
//! back the connection when none are pending. [`Store`], the store's
//! implementation of core's `Storage`, applies them; core calls it at start.
//!
//! Every connection the store opens has foreign keys on, the WAL journal, a
//! 64 MiB page cache, automatic checkpoints off and fullfsync on; while a
//! [`Store`] is alive it checkpoints the WAL once a second from a connection
//! of its own, never inside an append.
//!
//! [`Store::new`] reads the `[store]` section of the settings `rannoch_settings::load` loaded, so
//! an app that sets it loads the settings first: `cache_size_kib` sets the page cache in KiB
//! (65536 when absent) and `checkpoint_every_ms` how often the WAL is checkpointed (1000 when
//! absent).
//!
//! Rannoch keeps the [`Store`] in its `Config` for as long as it runs. A checkpoint query that
//! fails is logged at `error`, and so is each try at opening the checkpointer's connection that
//! fails. Either way it tries again a second later.
//!
//! # Failures
//!
//! [`open`] fails with `Error::Store` when the database cannot be opened with the store's
//! pragmas, as with a file that is not a database, or when the migrator cannot plan against it.
//! Pending migrations are not a failure: [`open`] returns [`Opened::Pending`] with their versions
//! and no connection.
//!
//! # Example
//!
//! The first two steps of starting rannoch: make the store and apply its pending migrations,
//! then open a connection. [`rannoch_core::Rannoch::open`] makes the same `pending` and `apply`
//! calls before anything else. The database here is in a new directory under the system's
//! temporary directory; an app gives its own path.
//!
//! ```
//! use std::error::Error;
//! use std::fs;
//!
//! use rannoch_core::Storage;
//! use rannoch_store::{Opened, Store};
//!
//! // The clock frozen, as in every rannoch-store test.
//! #[tokio::main(flavor = "current_thread", start_paused = true)]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     let dir = std::env::temp_dir().join(rannoch_core::clock::new_id().to_string());
//!     fs::create_dir_all(&dir)?;
//!     let path = dir.join("app.db");
//!     let migrator = rannoch_core::migrator();
//!
//!     // A new database has every migration pending, so open gives no connection yet.
//!     let store = Store::new(path.clone(), rannoch_core::migrator());
//!     assert!(matches!(rannoch_store::open(&path, &migrator)?, Opened::Pending(_)));
//!     if store.pending()? {
//!         store.apply()?;
//!     }
//!
//!     let Opened::Ready(conn) = rannoch_store::open(&path, &migrator)? else {
//!         return Err("a migration is still pending".into());
//!     };
//!     let rows: i64 = conn.query_row("SELECT count(*) FROM _events", [], |r| r.get(0))?;
//!     assert_eq!(rows, 0);
//!
//!     drop((conn, store));
//!     fs::remove_dir_all(&dir)?;
//!     Ok(())
//! }
//! ```

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use rannoch_core::{Error, Storage};
use rusqlite::Connection;
use serde::Deserialize;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::oneshot;

/// The pragmas every connection the store opens has, beside its page cache.
const PRAGMAS: &str = "PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA wal_autocheckpoint = 0;
PRAGMA fullfsync = ON;";

/// The store's `[store]` section; a key absent keeps its default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default)]
struct Settings {
    /// The page cache of every connection, in KiB.
    cache_size_kib: u32,
    /// How often the store checkpoints the WAL, in milliseconds.
    checkpoint_every_ms: u64,
}

impl Default for Settings {
    /// The store's values when no `[store]` section sets them.
    fn default() -> Self {
        Self {
            cache_size_kib: 65536,
            checkpoint_every_ms: 1000,
        }
    }
}

/// The database path of the one store and the settings it read, which [`Store::new`] sets.
static STORE: Mutex<Option<(PathBuf, Settings)>> = Mutex::new(None);

/// The settings of the one store, or the defaults when no store has been made.
fn settings() -> Settings {
    STORE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .map_or_else(Settings::default, |(_, settings)| *settings)
}

/// A connection to the database at `path` with the store's pragmas.
fn connection(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open(path).map_err(|_| Error::Store)?;
    conn.execute_batch(PRAGMAS).map_err(|_| Error::Store)?;
    conn.pragma_update(None, "cache_size", -i64::from(settings().cache_size_kib))
        .map_err(|_| Error::Store)?;
    Ok(conn)
}

/// The checkpointer's connection, and whether it was stopped. The task checkpoints only while
/// holding it, so a drop that takes it waits out a checkpoint underway, as joining the thread
/// did, without waiting on the task.
type Checkpoints = Arc<Mutex<(bool, Option<Connection>)>>;

/// The task that checkpoints the WAL every `every`; dropping it stops it.
#[derive(Debug)]
struct Checkpointer {
    stop: Option<oneshot::Sender<()>>,
    checkpoints: Checkpoints,
    /// The runtime `start` started when it was called outside one.
    runtime: Option<Runtime>,
}

impl Checkpointer {
    /// Spawns the task on the tokio runtime this is called in, or on one it starts when called
    /// outside a runtime.
    fn start(path: PathBuf, every: Duration) -> Self {
        let (stop, mut wait) = oneshot::channel::<()>();
        let checkpoints: Checkpoints = Arc::new(Mutex::new((false, None)));
        let shared = Arc::clone(&checkpoints);
        let mut runtime = None;
        let handle = Handle::try_current().unwrap_or_else(|_| {
            // Building a runtime fails only when the system refuses its threads or its driver,
            // as starting a thread did before.
            #[allow(clippy::expect_used)]
            let started = Runtime::new().expect("the store could not start a tokio runtime");
            runtime.insert(started).handle().clone()
        });
        handle.spawn(async move {
            while tokio::time::timeout(every, &mut wait).await.is_err() {
                let mut held = shared.lock().unwrap_or_else(PoisonError::into_inner);
                let (stopped, conn) = &mut *held;
                if *stopped {
                    return;
                }
                if conn.is_none() {
                    match connection(&path) {
                        Ok(opened) => *conn = Some(opened),
                        Err(e) => log::error!("checkpointer cannot open its connection: {e}"),
                    }
                }
                if let Some(conn) = conn
                    && conn
                        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))
                        .is_err()
                {
                    log::error!("checkpoint failed: {}", Error::Store);
                }
            }
        });
        Self {
            stop: Some(stop),
            checkpoints,
            runtime,
        }
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        // Taking the lock waits out a checkpoint underway; the connection closes here, and the
        // task returns without another.
        *self
            .checkpoints
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = (true, None);
        drop(self.stop.take());
        // Dropping a runtime blocks, which panics in async code; this does not.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// The app's store, for the app's `Config`: the database path and the app's
/// one migrator, which core applies pending migrations through at start.
#[derive(Debug)]
pub struct Store {
    path: PathBuf,
    migrator: migratr::Migrator,
    _checkpointer: Checkpointer,
}

impl Store {
    /// The store for the database at `path`, against the app's one `migrator`, with the
    /// `[store]` section of the loaded settings.
    /// It checkpoints the WAL every `checkpoint_every_ms` until it is dropped. It is the one
    /// store: each `new` replaces the path [`Store::connection`] opens and the settings every
    /// connection has.
    #[must_use]
    pub fn new(path: PathBuf, migrator: migratr::Migrator) -> Self {
        let settings: Settings = rannoch_settings::section("store");
        *STORE.lock().unwrap_or_else(PoisonError::into_inner) = Some((path.clone(), settings));
        let checkpointer = Checkpointer::start(
            path.clone(),
            Duration::from_millis(settings.checkpoint_every_ms),
        );
        Self {
            path,
            migrator,
            _checkpointer: checkpointer,
        }
    }

    /// A connection to the one store, the database of the last [`Store::new`], with the store's
    /// pragmas.
    ///
    /// # Errors
    ///
    /// `Error::Store` when no store has been made, or its database cannot be opened.
    pub fn connection() -> Result<Connection, Error> {
        let path = STORE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(path, _)| path.clone());
        connection(&path.ok_or(Error::Store)?)
    }
}

impl Storage for Store {
    fn pending(&self) -> Result<bool, Error> {
        Ok(matches!(
            open(&self.path, &self.migrator)?,
            Opened::Pending(_)
        ))
    }

    fn apply(&self) -> Result<(), Error> {
        let conn = connection(&self.path)?;
        self.migrator
            .up(&mut migratr::RusqliteExecutor::new(conn))
            .map_err(|_| Error::Store)?;
        Ok(())
    }
}

/// What opening the store found.
#[derive(Debug)]
pub enum Opened {
    /// Every migration is applied: the connection, with the store's pragmas.
    Ready(Connection),
    /// The versions of the migrations not yet applied, in order; nothing
    /// was applied.
    Pending(Vec<u64>),
}

/// Opens the database at `path` against the app's one `migrator`.
///
/// # Errors
///
/// `Error::Store` when the database cannot be opened, or when the migrator
/// cannot plan against it.
pub fn open(path: &Path, migrator: &migratr::Migrator) -> Result<Opened, Error> {
    let conn = connection(path)?;
    // migratr's executor owns the connection it is given, so the plan reads
    // through a second one to the same file.
    let mut exec = migratr::RusqliteExecutor::new(connection(path)?);
    let pending: Vec<u64> = migrator
        .plan(&mut exec)
        .map_err(|_| Error::Store)?
        .iter()
        .map(|m| m.version)
        .collect();
    if pending.is_empty() {
        Ok(Opened::Ready(conn))
    } else {
        Ok(Opened::Pending(pending))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::{Mutex, PoisonError};
    use std::time::Duration;

    use rannoch_core::Storage;

    use super::{Error, Settings, Store, connection, open};

    #[tokio::test(start_paused = true)]
    async fn with_no_settings_loaded_the_store_keeps_todays_values() {
        let settings: Settings = rannoch_settings::section("store");
        assert_eq!(
            settings,
            Settings {
                cache_size_kib: 65536,
                checkpoint_every_ms: 1000,
            }
        );

        let dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp/rannoch-store-defaults");
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("store.db");
        let _store = Store::new(db.clone(), rannoch_core::migrator());
        // Another test may make its own store meanwhile, so this opens its database by path.
        let cache: i64 = connection(&db)
            .unwrap()
            .query_row("PRAGMA cache_size", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cache, -65536);
    }

    #[tokio::test(start_paused = true)]
    async fn store_reports_pending_then_applies_through_its_migrator() {
        let dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp/rannoch-store-apply");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir_all(&dir).unwrap();
        let store = Store::new(dir.join("store.db"), rannoch_core::migrator());

        assert_eq!(store.pending(), Ok(true));
        assert_eq!(store.apply(), Ok(()));
        assert_eq!(store.pending(), Ok(false));
    }

    /// The `error` lines logged in this test binary.
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

    #[tokio::test(start_paused = true)]
    async fn a_checkpointer_that_cannot_open_its_connection_logs_at_error_each_time() {
        let _ = log::set_logger(&ERRORS);
        log::set_max_level(log::LevelFilter::Error);
        // A database in a directory that does not exist: no connection opens.
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp/rannoch-store-none");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        let unopened = || {
            let lines = ERRORS.0.lock().unwrap();
            let line = format!("checkpointer cannot open its connection: {}", Error::Store);
            lines.iter().filter(|l| **l == line).count()
        };

        let store = Store::new(dir.join("store.db"), rannoch_core::migrator());
        // It tries once a second: the first two tries, each logged, by the time the frozen clock
        // has moved two and a half seconds.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        drop(store);

        assert!(unopened() >= 2, "{} lines", unopened());
    }

    #[tokio::test(start_paused = true)]
    async fn a_plan_that_fails_fails_with_store() {
        // A file that is not a database: it opens, and the plan cannot read it.
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp/rannoch-store-plan");
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("store.db");
        fs::write(
            &db,
            b"not a database, but long enough to be read as one by sqlite",
        )
        .unwrap();

        assert!(matches!(
            open(&db, &rannoch_core::migrator()),
            Err(Error::Store)
        ));
    }
}
