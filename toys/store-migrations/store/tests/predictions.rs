//! One test per prediction in PREDICTIONS.md. Each prints what it saw.

use migratr::{Migrator, RusqliteExecutor};
use rusqlite::Connection;

const LATER: (&str, &str) = (
    "20261110000004_prediction.json",
    include_str!("../../sym/later/20261110000004_prediction.json"),
);

fn db() -> RusqliteExecutor {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    RusqliteExecutor::new(conn)
}

fn all() -> Vec<(&'static str, &'static str)> {
    [ontology::SOURCES, uncertainty::SOURCES, sym::SOURCES].concat()
}

fn tables(exec: &RusqliteExecutor) -> Vec<String> {
    let c = exec.connection();
    let mut s = c
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .unwrap();
    s.query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
}

#[test]
fn p1_one_migrator_per_crate_on_one_database() {
    let mut exec = db();
    let first = ontology::migrator().up(&mut exec);
    let second = uncertainty::migrator().up(&mut exec);
    println!("P1 first: {first:?}");
    println!("P1 second: {second:?}");
    assert!(first.is_ok());
    assert!(second.is_err(), "the second crate's migrator ran over the first's ledger");
}

#[test]
fn p2_and_p3_a_combined_migrator_applies_every_crate_in_order() {
    let mut sources = all();
    sources.push(LATER);
    let combined = Migrator::from_validated(&sources);
    let mut exec = db();
    let report = combined.up(&mut exec).unwrap();
    println!("P3 applied: {:?}", report.applied);
    println!("P3 tables: {:?}", tables(&exec));
    let c = exec.connection();
    c.execute("INSERT INTO symbol VALUES ('s1', 'a.rs')", []).unwrap();
    c.execute("INSERT INTO estimate VALUES ('e1', 0.6)", []).unwrap();
    c.execute("INSERT INTO prediction VALUES ('p1', 's1', 'e1')", []).unwrap();
    let bad = c.execute("INSERT INTO prediction VALUES ('p2', 's1', 'missing')", []);
    println!("P3 a prediction pointing at no estimate: {bad:?}");
    assert!(bad.is_err());
}

#[test]
fn p4_two_crates_with_the_same_version_collide() {
    let clash = [
        ("20261110000001_concept.json", ontology::SOURCES[0].1),
        ("20261110000001_estimate.json", uncertainty::SOURCES[0].1),
    ];
    let result = std::panic::catch_unwind(|| Migrator::from_validated(&clash));
    println!("P4 combining a duplicate version panicked: {}", result.is_err());
    assert!(result.is_err());
}

#[test]
fn p5_one_schema_json_for_the_whole_store() {
    let mut exec = db();
    Migrator::from_validated(&all()).up(&mut exec).unwrap();
    let dir = std::env::temp_dir().join(format!("forgr-toy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    migratr::write_schema(&mut exec, &dir).unwrap();
    let schema = std::fs::read_to_string(dir.join("schema.json")).unwrap();
    let found: Vec<&str> = ["concept", "estimate", "symbol"]
        .into_iter()
        .filter(|t| schema.contains(&format!("\"{t}\"")))
        .collect();
    println!("P5 tables named in schema.json: {found:?} ({} bytes)", schema.len());
    assert_eq!(found.len(), 3);
}

#[test]
fn p6_a_new_migration_applies_at_open() {
    let mut exec = db();
    let before = Migrator::from_validated(&all()).up(&mut exec).unwrap();
    let mut after_sources = all();
    after_sources.push(LATER);
    let after = Migrator::from_validated(&after_sources);
    let pending: Vec<_> = after.plan(&mut exec).unwrap().iter().map(|m| m.version).collect();
    let applied = after.up(&mut exec).unwrap();
    println!("P6 before applied {:?}; pending after the change {pending:?}; applied {:?}", before.applied, applied.applied);
    assert_eq!(applied.applied, vec![20261110000004]);
}
