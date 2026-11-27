//! This crate's SQL, checked by forgr against the app's schema.json.

/// One event as one `_events` row.
pub(crate) const APPEND: &str =
    "INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

/// One row into `table`, quoted, of `columns`, quoted and joined, from `marks`, a parameter for
/// each column; a row that conflicts is not written.
pub(crate) fn store_row(table: &str, columns: &str, marks: &str) -> String {
    format!("INSERT INTO {table} ({columns}) VALUES ({marks}) ON CONFLICT DO NOTHING")
}
