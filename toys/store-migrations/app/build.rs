//! Collects every crate's migrations into one migrator, and reruns when any directory changes.

use std::fmt::Write as _;
use std::path::Path;

fn main() {
    let mut entries = String::new();
    for dir in [ontology::MIGRATIONS_DIR, uncertainty::MIGRATIONS_DIR, sym::MIGRATIONS_DIR] {
        println!("cargo:rerun-if-changed={dir}");
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .collect();
        files.sort();
        for path in files {
            let name = path.file_name().unwrap().to_str().unwrap().to_owned();
            let full = path.to_str().unwrap().to_owned();
            write!(entries, "({name:?}, include_str!({full:?})),").unwrap();
        }
    }
    let out = Path::new(&std::env::var("OUT_DIR").unwrap()).join("migrator.rs");
    std::fs::write(out, format!("::migratr::Migrator::from_validated(&[{entries}])")).unwrap();
}
