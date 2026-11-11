//! Copies every crate's migrations into `migrations/` here, for the public `embed!`.

use std::path::Path;

fn main() {
    let here = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("migrations");
    if here.exists() {
        for e in std::fs::read_dir(&here).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|x| x == "json") && p.file_name().unwrap() != "schema.json" {
                std::fs::remove_file(p).unwrap();
            }
        }
    } else {
        std::fs::create_dir_all(&here).unwrap();
    }
    for dir in [ontology::MIGRATIONS_DIR, uncertainty::MIGRATIONS_DIR, sym::MIGRATIONS_DIR] {
        println!("cargo:rerun-if-changed={dir}");
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|x| x == "json") {
                std::fs::copy(&p, here.join(p.file_name().unwrap())).unwrap();
            }
        }
    }
}
