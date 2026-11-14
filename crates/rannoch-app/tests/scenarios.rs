//! `@criterion-FR-RANNOCH-008-c2` and `@criterion-FR-RANNOCH-011-c2`, from
//! `tests/features/`. Each runs cargo on a copy of this workspace, so a
//! developer's edit never touches the real checkout.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The workspace root, two levels above this crate.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A fresh copy of the workspace's manifests and crates under `name`.
fn copy_workspace(name: &str) -> io::Result<PathBuf> {
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

fn copy_dir(from: &Path, to: &Path) -> io::Result<()> {
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

fn entry_name(path: &Path) -> io::Result<&std::ffi::OsStr> {
    path.file_name()
        .ok_or_else(|| io::Error::other(format!("{} has no name", path.display())))
}

/// Runs cargo in `ws` with a target directory of its own: cargo names a
/// workspace member's build the same at any path, so two copies sharing one
/// target directory would overwrite each other's build script.
fn cargo(ws: &Path, args: &[&str]) -> io::Result<Output> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    Command::new(cargo)
        .args(args)
        .current_dir(ws)
        .env("CARGO_TARGET_DIR", ws.with_file_name("target"))
        .output()
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn fr_rannoch_008_c2() {
    // Given an app built on rannoch whose schema.json is current
    let ws = copy_workspace("fr_rannoch_008_c2").unwrap();
    let app = ws.join("crates/rannoch-app/migrations");
    let out = cargo(&ws, &["build", "-p", "rannoch-app"]).unwrap();
    assert!(out.status.success(), "{}", text(&out));
    let committed = fs::read(root().join("crates/rannoch-app/migrations/schema.json")).unwrap();
    assert_eq!(fs::read(app.join("schema.json")).unwrap(), committed);

    // When a developer adds a migration creating a table to one of the app's
    // crates and builds again
    let name = "20261114000002_added.json";
    let body = r#"{"up": [{"op": "create_table", "table": "added", "columns": [{"name": "id", "type": "TEXT", "primary_key": 1}]}]}"#;
    fs::write(
        ws.join("crates/rannoch-fixture/migrations").join(name),
        body,
    )
    .unwrap();
    let out = cargo(&ws, &["build", "-p", "rannoch-app"]).unwrap();
    assert!(out.status.success(), "{}", text(&out));

    // Then the app's committed migrations directory holds the new migration
    assert_eq!(fs::read_to_string(app.join(name)).unwrap(), body);

    // And its schema.json names the new table
    let schema = fs::read_to_string(app.join("schema.json")).unwrap();
    assert!(schema.contains(r#""name": "added""#), "{schema}");
}

#[test]
fn fr_rannoch_011_c2() {
    // Given a crate whose queries all match the app's schema
    let ws = copy_workspace("fr_rannoch_011_c2").unwrap();
    let check = [
        "test",
        "-p",
        "rannoch-app",
        "--lib",
        "forgr_sql_matches_schema",
    ];
    let out = cargo(&ws, &check).unwrap();
    assert!(out.status.success(), "{}", text(&out));

    // When a developer renames a column in a migration and builds again
    let migration = ws.join("crates/rannoch-fixture/migrations/20261114000001_fixture.json");
    let renamed = fs::read_to_string(&migration)
        .unwrap()
        .replace(r#""name": "name""#, r#""name": "label""#);
    fs::write(&migration, renamed).unwrap();
    let out = cargo(&ws, &check).unwrap();

    // Then cargo test fails at the query that still names the old column
    let queries = "crates/rannoch-fixture/src/queries.rs";
    let line = fs::read_to_string(root().join(queries))
        .unwrap()
        .lines()
        .position(|l| l.contains("SELECT id, name FROM fixture"))
        .unwrap()
        + 1;
    let report = text(&out);
    assert!(!out.status.success(), "{report}");
    assert!(
        report.contains(&format!("{queries}:{line}: no such column: name")),
        "{report}"
    );
}
