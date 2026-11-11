//! The sym crate's migrations, embedded.

/// This crate's migrations, through migratr's embed!.
#[must_use]
pub fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}

/// This crate's migration files as (name, contents), for a store that combines crates.
pub const SOURCES: &[(&str, &str)] = &[("20261110000003_symbol.json", include_str!("../migrations/20261110000003_symbol.json"))];

/// Where this crate's migrations directory is, for an app's build.rs.
pub const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
