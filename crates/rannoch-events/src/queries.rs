//! This crate's SQL, checked by forgr against the app's schema.json.

/// One event as one `_events` row.
pub(crate) const APPEND: &str =
    "INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
