//! This crate's SQL, checked by forgr against the app's schema.json.

/// The largest rowid in `_events`, or 0 when it is empty.
pub(crate) const LAST: &str = "SELECT coalesce(max(rowid), 0) FROM _events";

/// Each `_events` row whose id is in the JSON array `?1` or whose rowid is above `?2`, once, in
/// the order written: its rowid, and the row as one JSON object of its columns.
pub(crate) const ROWS: &str = "SELECT rowid, json_object('id', id, 'source', source, 'ty', ty,
    'data', data, 'run_at', run_at, 'status', status)
FROM _events WHERE id IN (SELECT value FROM json_each(?1)) OR rowid > ?2 ORDER BY rowid";
