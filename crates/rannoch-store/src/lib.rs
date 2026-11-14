//! The store: opens the app's `SQLite` database against the app's one
//! migrator, reports pending migrations and never applies them, and hands
//! back the connection when none are pending. Core applies them at start
//! through [`Store`], its implementation of core's `Storage`.

use std::path::{Path, PathBuf};

use rannoch_core::{Error, Storage};
use rusqlite::Connection;

/// The app's store, for the app's `Config`: the database path and the app's
/// one migrator, which core applies pending migrations through at start.
#[derive(Debug)]
pub struct Store {
    path: PathBuf,
    migrator: migratr::Migrator,
}

impl Store {
    /// The store for the database at `path`, against the app's one `migrator`.
    #[must_use]
    pub fn new(path: PathBuf, migrator: migratr::Migrator) -> Self {
        Self { path, migrator }
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
        let conn = Connection::open(&self.path).map_err(|_| Error::Store)?;
        self.migrator
            .up(&mut migratr::RusqliteExecutor::new(conn))
            .map_err(|_| Error::Store)?;
        Ok(())
    }
}

/// What opening the store found.
#[derive(Debug)]
pub enum Opened {
    /// Every migration is applied: the connection, with foreign keys on.
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
    let conn = Connection::open(path).map_err(|_| Error::Store)?;
    conn.pragma_update(None, "foreign_keys", true)
        .map_err(|_| Error::Store)?;
    // migratr's executor owns the connection it is given, so the plan reads
    // through a second one to the same file.
    let mut exec =
        migratr::RusqliteExecutor::new(Connection::open(path).map_err(|_| Error::Store)?);
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

    use super::{Error, open};

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
            open(&db, &rannoch_app::migrator()),
            Err(Error::Store)
        ));
    }
}
