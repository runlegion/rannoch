//! The app's build: checks its flow docs against its own signatures and compiles them in.

use std::path::Path;

fn main() {
    rannoch_processor::compile(Path::new("flows"), &[include_str!("signatures.json")]);
}
