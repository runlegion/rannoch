//! `@criterion-FR-RANNOCH-009-c1` to `-c3` and `@criterion-FR-RANNOCH-011-c1`,
//! from `tests/features/`. Each builds its database with the app's one
//! migrator in a directory of its own.

use std::fs;
use std::path::{Path, PathBuf};

use rannoch_core::Error;
use rannoch_store::{Opened, open};

/// A fresh, empty directory for `name`, and the database path inside it.
fn database(name: &str) -> std::io::Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir.join("store.db"))
}

/// An executor over the database at `path`, for building it.
fn executor(path: &Path) -> rusqlite::Result<migratr::RusqliteExecutor> {
    rusqlite::Connection::open(path).map(migratr::RusqliteExecutor::new)
}

/// The database at `path` with every migration applied.
fn current(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    rannoch_core::migrator().up(&mut executor(path)?)?;
    Ok(())
}

fn ready(opened: Opened) -> rusqlite::Connection {
    match opened {
        Opened::Ready(conn) => conn,
        Opened::Pending(pending) => panic!("pending: {pending:?}"),
    }
}

#[test]
fn fr_rannoch_009_c1() {
    // Given a database with every migration applied
    let path = database("fr_rannoch_009_c1").unwrap();
    current(&path).unwrap();

    // When the store opens it
    let opened = open(&path, &rannoch_core::migrator()).unwrap();

    // Then it hands back a connection
    let conn = ready(opened);

    // And a row whose foreign key names no parent is refused: foreign keys
    // are on
    let on: i64 = conn
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .unwrap();
    assert_eq!(on, 1);
}

#[test]
fn fr_rannoch_009_c2() {
    // Given a database missing the newest migration
    let path = database("fr_rannoch_009_c2").unwrap();
    let migrator = rannoch_core::migrator();
    let mut exec = executor(&path).unwrap();
    migrator.up(&mut exec).unwrap();
    migrator.down(&mut exec, 1).unwrap();
    let newest = migrator
        .status(&mut exec)
        .unwrap()
        .iter()
        .map(|s| s.version)
        .max()
        .unwrap();
    drop(exec);
    let before = fs::read(&path).unwrap();

    // When the store opens it
    let opened = open(&path, &migrator).unwrap();

    // Then it reports that migration as pending
    assert!(
        matches!(&opened, Opened::Pending(p) if *p == [newest]),
        "{opened:?}"
    );

    // And the database is unchanged
    drop(opened);
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn fr_rannoch_009_c3() {
    // Given a path the store cannot open
    let path = database("fr_rannoch_009_c3")
        .unwrap()
        .with_file_name("missing/store.db");

    // When the store opens it
    let opened = open(&path, &rannoch_core::migrator());

    // Then open fails with Store
    assert!(matches!(opened, Err(Error::Store)), "{opened:?}");
}

#[test]
fn fr_rannoch_011_c1() {
    // Given an open store
    let path = database("fr_rannoch_011_c1").unwrap();
    current(&path).unwrap();
    let mut conn = ready(open(&path, &rannoch_core::migrator()).unwrap());
    let (queue, _signals) = std::sync::mpsc::channel();
    let event = rannoch_core::Event {
        id: "01a1dd40-c99f-7113-bb63-037e4ec38675".parse().unwrap(),
        source: None,
        ty: 7_u16,
        data: "{}",
        run_at: None,
        status: rannoch_core::Status::Added,
    };

    // When a crate runs one of its queries on the connection
    rannoch_events::append(&mut conn, std::slice::from_ref(&event), &queue).unwrap();

    // Then the query answers from the store
    let id: String = conn
        .query_row("SELECT id FROM _events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(id, "01a1dd40-c99f-7113-bb63-037e4ec38675");
}
