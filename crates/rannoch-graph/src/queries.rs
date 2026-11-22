//! This crate's SQL, checked by forgr against the app's schema.json.

/// One property value as one `_nodes` row; a rerun adds none.
pub(crate) const NODE: &str = "INSERT OR IGNORE INTO _nodes (node, key, value, event, written, retracted) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

/// One edge as one `_edges` row; a rerun adds none.
pub(crate) const EDGE: &str = "INSERT OR IGNORE INTO _edges (src, name, dst, props, event, written, retracted) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// The current row of a node's key, as of ?3.
pub(crate) const PROP: &str =
    "SELECT value, event, written, retracted FROM _nodes WHERE node = ?1 AND key = ?2
    AND (?3 IS NULL OR written <= ?3) ORDER BY event DESC LIMIT 1";

/// A node's rows as of ?2, by key, the greatest event first.
pub(crate) const PROPS: &str =
    "SELECT key, value, event, written, retracted FROM _nodes WHERE node = ?1
    AND (?2 IS NULL OR written <= ?2) ORDER BY key, event DESC";

/// Every row of a node's key as of ?3, oldest first.
pub(crate) const HISTORY: &str =
    "SELECT value, event, written, retracted FROM _nodes WHERE node = ?1 AND key = ?2
    AND (?3 IS NULL OR written <= ?3) ORDER BY event";

/// The edge rows from a node as of ?3, by far node, the greatest event first.
pub(crate) const OUT: &str =
    "SELECT dst, props, event, written, retracted FROM _edges WHERE src = ?1 AND name = ?2
    AND (?3 IS NULL OR written <= ?3) ORDER BY dst, event DESC";

/// The edge rows into a node as of ?3, by far node, the greatest event first.
pub(crate) const IN: &str =
    "SELECT src, props, event, written, retracted FROM _edges WHERE dst = ?1 AND name = ?2
    AND (?3 IS NULL OR written <= ?3) ORDER BY src, event DESC";

/// An event, then its source chain through `_events`.
pub(crate) const WHY: &str = "WITH RECURSIVE c(id, source, depth) AS (
    SELECT id, source, 0 FROM _events WHERE id = ?1
    UNION ALL SELECT e.id, e.source, c.depth + 1 FROM _events e JOIN c ON e.id = c.source)
    SELECT id FROM c ORDER BY depth";
