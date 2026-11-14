//! This crate's SQL, checked by forgr against the app's schema.json.

/// Every fixture row's id and name.
pub const FIXTURE_NAMES: &str = "SELECT id, name FROM fixture";
