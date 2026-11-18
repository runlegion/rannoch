//! The store: opens the app's `SQLite` database against the app's one
//! migrator, reports pending migrations and never applies them, and hands
//! back the connection when none are pending. Core applies them at start
//! through [`Store`], its implementation of core's `Storage`.
//!
//! Every connection the store opens has foreign keys on, the WAL journal, a
//! 64 MiB page cache, automatic checkpoints off and fullfsync on; while a
//! [`Store`] is alive it checkpoints the WAL once a second from a connection
//! of its own, never inside an append.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rannoch_core::{Error, Storage};
use rusqlite::Connection;

/// The pragmas every connection the store opens has.
const PRAGMAS: &str = "PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA cache_size = -65536;
PRAGMA wal_autocheckpoint = 0;
PRAGMA fullfsync = ON;";

/// How often the store checkpoints the WAL.
const CHECKPOINT_EVERY: Duration = Duration::from_secs(1);

/// A connection to the database at `path` with the store's pragmas.
fn connection(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open(path).map_err(|_| Error::Store)?;
    conn.execute_batch(PRAGMAS).map_err(|_| Error::Store)?;
    Ok(conn)
}

/// The thread that checkpoints the WAL once a second; dropping it stops it.
#[derive(Debug)]
struct Checkpointer {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Checkpointer {
    fn start(path: PathBuf) -> Self {
        let (stop, wait) = mpsc::channel::<()>();
        let thread = thread::spawn(move || {
            let mut conn = None;
            while wait.recv_timeout(CHECKPOINT_EVERY) == Err(RecvTimeoutError::Timeout) {
                if conn.is_none() {
                    conn = connection(&path).ok();
                }
                if let Some(conn) = &conn
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
            thread: Some(thread),
        }
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
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
    /// The store for the database at `path`, against the app's one `migrator`.
    /// It checkpoints the WAL once a second until it is dropped.
    #[must_use]
    pub fn new(path: PathBuf, migrator: migratr::Migrator) -> Self {
        let checkpointer = Checkpointer::start(path.clone());
        Self {
            path,
            migrator,
            _checkpointer: checkpointer,
        }
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

    use rannoch_core::Storage;

    use super::{Error, Store, open};

    #[test]
    fn store_reports_pending_then_applies_through_its_migrator() {
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

    #[test]
    fn a_plan_that_fails_fails_with_store() {
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
