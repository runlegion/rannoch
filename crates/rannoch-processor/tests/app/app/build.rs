//! The app's check at build: gathers the signatures of every processor crate the app uses, its
//! own included, checks every flow doc in the set `FLOWS` names against them, and compiles the
//! checked docs in. Any error fails the build, listing every error.

use std::error::Error;
use std::path::PathBuf;
use std::{env, fs, process};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-env-changed=FLOWS");
    println!("cargo:rerun-if-changed=signatures.json");
    let dir = PathBuf::from(env::var("FLOWS")?);
    println!("cargo:rerun-if-changed={}", dir.display());
    let mut files = fs::read_dir(&dir)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    files.sort();
    let mut docs = Vec::new();
    for path in &files {
        let name = path
            .file_name()
            .ok_or("a flow doc has no name")?
            .to_string_lossy()
            .into_owned();
        docs.push((name, fs::read_to_string(path)?));
    }
    let own = fs::read_to_string("signatures.json")?;
    let signatures = rannoch_processor::gather(&[&own, proc_text::SIGNATURES])?;
    match rannoch_processor::check(&docs, &signatures) {
        Ok(flows) => {
            let out = PathBuf::from(env::var("OUT_DIR")?).join("flows.rs");
            fs::write(out, format!("const FLOWS: &[&str] = &{flows:?};\n"))?;
            Ok(())
        }
        Err(errors) => {
            for error in &errors {
                eprintln!("{error}");
            }
            eprintln!("{} flow doc errors", errors.len());
            process::exit(1);
        }
    }
}
