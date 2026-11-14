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
    rannoch_app::migrator().up(&mut executor(path)?)?;
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
    let opened = open(&path, &rannoch_app::migrator()).unwrap();

    // Then it hands back a connection
    let conn = ready(opened);
    let on: bool = conn
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .unwrap();
    assert!(on);

    // And a row whose foreign key names no parent is refused
    let refused = rannoch_app::forgr::fixture_child::factory()
        .id("c1")
        .fixture_id("no-parent")
        .insert(&conn)
        .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("FOREIGN KEY constraint failed"),
        "{refused}"
    );
}

#[test]
fn fr_rannoch_009_c2() {
    // Given a database missing the newest migration
    let path = database("fr_rannoch_009_c2").unwrap();
    let migrator = rannoch_app::migrator();
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
    let opened = open(&path, &rannoch_app::migrator());

    // Then open fails with Store
    assert!(matches!(opened, Err(Error::Store)), "{opened:?}");
}

#[test]
fn fr_rannoch_011_c1() {
    // Given an open store
    let path = database("fr_rannoch_011_c1").unwrap();
    current(&path).unwrap();
    let conn = ready(open(&path, &rannoch_app::migrator()).unwrap());
    rannoch_app::forgr::fixture::factory()
        .id("f1")
        .name("one")
        .insert(&conn)
        .unwrap();

    // When a crate runs one of its queries on the connection
    let rows: Vec<(String, String)> = conn
        .prepare(rannoch_fixture::queries::FIXTURE_NAMES)
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    // Then the query answers from the store
    assert_eq!(rows, [("f1".to_owned(), "one".to_owned())]);
}
