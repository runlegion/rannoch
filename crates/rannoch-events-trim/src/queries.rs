//! This crate's SQL, checked by forgr against the app's schema.json.

/// The events at 0 whose id is below `?1` and that have a 2, deleted with their status rows:
/// the rows whose source is one of them and whose status is not 0.
pub(crate) const TRIM: &str = "WITH done(id) AS (SELECT e.id FROM _events e
WHERE e.status = 0 AND e.id < ?1
AND EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status = 2))
DELETE FROM _events WHERE id IN done OR (status <> 0 AND source IN done)";
