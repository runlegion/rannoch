//! This crate's SQL, checked by forgr against the app's schema.json.

/// Each `_events` row whose id is in the JSON array `?1`, in the order written, as one JSON
/// object of its columns.
pub(crate) const ROWS: &str = "SELECT json_object('id', id, 'source', source, 'ty', ty,
    'data', data, 'run_at', run_at, 'status', status)
FROM _events WHERE id IN (SELECT value FROM json_each(?1)) ORDER BY rowid";
