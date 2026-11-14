//! The store: opens the app's `SQLite` database against the app's one
//! migrator, reports pending migrations and never applies them, and hands
//! back the connection when none are pending.

use std::path::Path;

use rannoch_core::Error;
use rusqlite::Connection;

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

    use super::{Error, open};

    #[test]
    fn a_plan_that_fails_fails_with_store() {
        // A ledger row whose file the app's migrator does not have.
        let dir = std::env::temp_dir().join(format!("rannoch-store-plan-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let migrations = dir.join("migrations");
        fs::create_dir_all(&migrations).unwrap();
        fs::write(
            migrations.join("20261114999999_other.json"),
            r#"{"up": [{"op": "create_table", "table": "other", "columns": [{"name": "id", "type": "TEXT", "primary_key": 1}]}]}"#,
        )
        .unwrap();
        let db = dir.join("store.db");
        let mut exec = migratr::RusqliteExecutor::new(rusqlite::Connection::open(&db).unwrap());
        migratr::up(&mut exec, &migratr::load_dir(&migrations).unwrap(), None).unwrap();
        drop(exec);

        assert!(matches!(
            open(&db, &rannoch_app::migrator()),
            Err(Error::Store)
        ));
        fs::remove_dir_all(&dir).unwrap();
    }
}
