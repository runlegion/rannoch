//! This crate's SQL, checked by forgr against the app's schema.json.

/// One event as one `_events` row.
pub(crate) const APPEND: &str =
    "INSERT INTO _events (id, source, ty, data, run_at, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

/// The root `?1` and every row whose source chain leads back to it.
pub(crate) const TREE: &str = "WITH RECURSIVE tree(id) AS (
    SELECT id FROM _events WHERE id = ?1
    UNION SELECT e.id FROM _events e JOIN tree t ON e.source = t.id
) SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM _events e JOIN tree USING (id) ORDER BY e.id";

/// Row `?1`'s current status: its newest state change's, or its own.
pub(crate) const CURRENT_STATUS: &str = "SELECT coalesce((
    SELECT s.status FROM _events s WHERE s.source = e.id AND s.status <> 0
    ORDER BY s.id DESC LIMIT 1
), e.status) FROM _events e WHERE e.id = ?1";

/// The events of ty `?1` whose current status is 0.
pub(crate) const WAITING: &str = "SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM _events e WHERE e.ty = ?1 AND e.status = 0
AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0)
ORDER BY e.id";

/// The events whose current status is 0 and whose `run_at` is at or before `?1`.
pub(crate) const DUE: &str = "SELECT e.id, e.source, e.ty, e.data, e.run_at, e.status
FROM _events e WHERE e.run_at <= ?1 AND e.status = 0
AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0)
ORDER BY e.id";
