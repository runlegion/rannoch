//! Copies every crate's migrations into core's committed `migrations/` and
//! writes `schema.json` there, each file only when its bytes differ, so the
//! directory and `schema.json` are never out of date. Each crate's
//! `migrations/` is read by its workspace path: those crates depend on core,
//! so core cannot build-depend on them.

use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

/// The migrations directory of every crate with tables, from core's own.
const CRATES: &[&str] = &[
    "../rannoch-events/migrations",
    "../rannoch-graph/migrations",
];

fn main() -> Result<(), Box<dyn Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let here = manifest.join("migrations");
    fs::create_dir_all(&here)?;
    let mut wanted = Vec::new();
    for crate_dir in CRATES {
        let dir = manifest.join(crate_dir);
        println!("cargo:rerun-if-changed={}", dir.display());
        for entry in fs::read_dir(&dir)? {
            let from = entry?.path();
            if is_json(&from) {
                let name = from
                    .file_name()
                    .ok_or("a migration file has no name")?
                    .to_owned();
                write_if_changed(&here.join(&name), &fs::read(&from)?)?;
                wanted.push(name);
            }
        }
    }
    let schema = OsString::from("schema.json");
    for entry in fs::read_dir(&here)? {
        let path = entry?.path();
        if is_json(&path)
            && path
                .file_name()
                .is_some_and(|n| n != schema && !wanted.iter().any(|w| w == n))
        {
            fs::remove_file(path)?;
        }
    }
    let migrations = migratr::load_dir(&here)?;
    let mut exec = migratr::RusqliteExecutor::new(rusqlite::Connection::open_in_memory()?);
    migratr::up(&mut exec, &migrations, None)?;
    let scratch = PathBuf::from(std::env::var("OUT_DIR")?).join("schema");
    fs::create_dir_all(&scratch)?;
    migratr::write_schema(&mut exec, &scratch)?;
    write_if_changed(&here.join(&schema), &fs::read(scratch.join(&schema))?)?;
    Ok(())
}

fn is_json(path: &Path) -> bool {
    path.extension().is_some_and(|x| x == "json")
}

/// Writes `body` to `to` only when the file's bytes differ, so an unchanged
/// build leaves the committed files untouched.
fn write_if_changed(to: &Path, body: &[u8]) -> Result<(), Box<dyn Error>> {
    if fs::read(to).ok().as_deref() != Some(body) {
        fs::write(to, body)?;
    }
    Ok(())
}
