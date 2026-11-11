//! Copies every crate's migrations into the app's committed migrations directory and writes
//! schema.json there, so the directory and schema.json are never out of date.

use std::path::Path;

fn main() {
    let here = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("migrations");
    std::fs::create_dir_all(&here).unwrap();
    let mut wanted = Vec::new();
    for dir in [ontology::MIGRATIONS_DIR, uncertainty::MIGRATIONS_DIR, sym::MIGRATIONS_DIR] {
        println!("cargo:rerun-if-changed={dir}");
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|x| x == "json") {
                let name = p.file_name().unwrap().to_owned();
                let to = here.join(&name);
                let body = std::fs::read(&p).unwrap();
                // Write only what changed, so an unchanged build leaves the files untouched.
                if std::fs::read(&to).ok().as_deref() != Some(body.as_slice()) {
                    std::fs::write(&to, &body).unwrap();
                }
                wanted.push(name);
            }
        }
    }
    for e in std::fs::read_dir(&here).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_owned();
        if p.extension().is_some_and(|x| x == "json") && name != "schema.json" && !wanted.contains(&name) {
            std::fs::remove_file(p).unwrap();
        }
    }
    let migrations = migratr::load_dir(&here).unwrap();
    let mut exec = migratr::RusqliteExecutor::new(rusqlite::Connection::open_in_memory().unwrap());
    migratr::up(&mut exec, &migrations, None).unwrap();
    let before = std::fs::read(here.join("schema.json")).ok();
    let scratch = Path::new(&std::env::var("OUT_DIR").unwrap()).join("schema");
    std::fs::create_dir_all(&scratch).unwrap();
    migratr::write_schema(&mut exec, &scratch).unwrap();
    let fresh = std::fs::read(scratch.join("schema.json")).unwrap();
    if before.as_deref() != Some(fresh.as_slice()) {
        std::fs::write(here.join("schema.json"), fresh).unwrap();
    }
}
