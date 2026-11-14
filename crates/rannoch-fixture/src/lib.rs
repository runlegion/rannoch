//! A crate with one table, for rannoch's own tests of the app's schema: its
//! migrations are gathered by `rannoch-app`'s `build.rs`.

pub mod queries;

/// Where this crate's migratr migrations directory is, for the app's `build.rs`.
pub const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
