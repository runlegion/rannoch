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

/// The 1s whose event has no 2 or -1: the 1's id, its event's id and the event's ty.
pub(crate) const ONES: &str = "SELECT s.id, s.source, s.ty FROM _events s WHERE s.status = 1
AND NOT EXISTS (SELECT 1 FROM _events r WHERE r.source = s.source AND r.status IN (2, -1))";

/// Every row in the tree below `?1`, read from the tree's ids into `_events` by primary key.
pub(crate) const TREE: &str = "WITH RECURSIVE tree(id) AS (
    SELECT id FROM _events WHERE source = ?1
    UNION SELECT e.id FROM _events e JOIN tree t ON e.source = t.id
) SELECT e.id FROM tree CROSS JOIN _events e ON e.id = tree.id";

/// The number of `_events` rows at each status.
pub(crate) const REPORT: &str =
    "SELECT status, count(*) FROM _events GROUP BY status ORDER BY status";
