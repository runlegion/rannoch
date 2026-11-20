//! The app's build: checks the flow set `FLOWS` names (default `valid`) against the signatures
//! proc-text exports and the app's own, and compiles the flows in.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-env-changed=FLOWS");
    let set = std::env::var("FLOWS").unwrap_or_else(|_| "valid".to_owned());
    let own = include_str!("signatures.json");
    rannoch_processor::compile(&Path::new("flows").join(set), &[proc_text::SIGNATURES, own]);
}
