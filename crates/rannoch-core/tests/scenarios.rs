//! `@criterion-FR-RANNOCH-010-c1` and `-c2`, from `tests/features/`. Each
//! builds its database with the app's one migrator in a directory of its own,
//! and opens rannoch with the real store in its `Config`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rannoch_core::{Config, Error, Event, Rannoch, Status};
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
fn config(path: &Path) -> Config<u8> {
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
