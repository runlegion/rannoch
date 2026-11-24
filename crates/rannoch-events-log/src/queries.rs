//! This crate's SQL, checked by forgr against the app's schema.json.

/// The largest rowid in `_events`, or 0 when it is empty.
pub(crate) const LAST: &str = "SELECT coalesce(max(rowid), 0) FROM _events";

/// Each `_events` row whose rowid is above `?1`, in the order written: its rowid, and the row as
/// one JSON object of its columns.
pub(crate) const SINCE: &str = "SELECT rowid, json_object('id', id, 'source', source, 'ty', ty,
    'data', data, 'run_at', run_at, 'status', status)
FROM _events WHERE rowid > ?1 ORDER BY rowid";
