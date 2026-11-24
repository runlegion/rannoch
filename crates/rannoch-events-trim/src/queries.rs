//! This crate's SQL, checked by forgr against the app's schema.json.

/// The `_events` rows whose id is below `?1`, deleted.
pub(crate) const TRIM: &str = "DELETE FROM _events WHERE id < ?1";
