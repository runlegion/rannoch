//! This crate's SQL, checked by forgr against the app's schema.json.

/// The events at status 0 that are due at `?1` and have no status event whose source is them.
pub(crate) const DUE: &str = "SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM _events e WHERE e.status = 0 AND (e.run_at IS NULL OR e.run_at <= ?1)
AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0)
ORDER BY e.id";

/// The earliest `run_at` after `?1` among the events at status 0 that no status event has taken.
pub(crate) const NEXT_RUN_AT: &str = "SELECT min(e.run_at) FROM _events e
WHERE e.status = 0 AND e.run_at > ?1
AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0)";

/// One event by its id.
pub(crate) const EVENT: &str =
    "SELECT id, source, ty, data, run_at, status FROM _events WHERE id = ?1";

/// Whether the event `?1` has a -1.
pub(crate) const FAILED: &str =
    "SELECT EXISTS (SELECT 1 FROM _events WHERE source = ?1 AND status = -1)";
