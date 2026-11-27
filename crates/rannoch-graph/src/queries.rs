//! This crate's SQL, over the tables and columns the app names; each name comes in quoted.

/// The row of `table` whose `key` is `?1` with the greatest `time` at or before `?3` when `time`
/// is text, else `?2`.
pub(crate) fn as_of(table: &str, key: &str, time: &str) -> String {
    format!(
        "SELECT * FROM {table} WHERE {key} = ?1
        AND {time} <= CASE typeof({time}) WHEN 'text' THEN ?3 ELSE ?2 END
        ORDER BY {time} DESC LIMIT 1"
    )
}

/// The rows of `table` whose `from` is `?1` and whose `time` is at or before `?2`, oldest first.
pub(crate) fn edges(table: &str, from: &str, time: &str) -> String {
    format!("SELECT * FROM {table} WHERE {from} = ?1 AND {time} <= ?2 ORDER BY {time}")
}

/// The greatest `event` among `table`'s rows whose `key` is `?1`, then its source chain through
/// `_events`, nearest first.
pub(crate) fn why(table: &str, key: &str, event: &str) -> String {
    format!(
        "WITH RECURSIVE c(id, source, depth) AS (
        SELECT id, source, 0 FROM _events WHERE id = (SELECT max({event}) FROM {table} WHERE {key} = ?1)
        UNION ALL SELECT e.id, e.source, c.depth + 1 FROM _events e JOIN c ON e.id = c.source)
        SELECT id FROM c ORDER BY depth"
    )
}

/// One hop of a walk over `table`, from `from` to `to`, below `?3` hops and, when `time` is
/// given, over the rows whose `time` is at or before `?2` when `?2` is not null.
pub(crate) fn walk_step(table: &str, from: &str, to: &str, time: Option<&str>) -> String {
    let time = time.map_or_else(String::new, |time| {
        format!(" AND (?2 IS NULL OR e.{time} <= ?2)")
    });
    format!(
        " UNION SELECT CAST(e.{to} AS TEXT), w.depth + 1 FROM w JOIN {table} e
            ON e.{from} = w.node WHERE w.depth < ?3{time}"
    )
}

/// The nodes a walk from `?1` reaches over `steps`, each [`walk_step`], each at its fewest hops,
/// nearest first, at most `?4`.
pub(crate) fn walk(steps: &str) -> String {
    format!(
        "WITH RECURSIVE w(node, depth) AS (SELECT ?1, 0{steps})
        SELECT node, min(depth) AS d FROM w WHERE node <> ?1 GROUP BY node ORDER BY d, node
        LIMIT ?4"
    )
}

/// The rows of `table` that hold `?1` in `a` or `b`, read both ways: each row's other node, as
/// text, and its `weight`, 1 when the table names none.
pub(crate) fn rows(table: &str, a: &str, b: &str, weight: Option<&str>) -> String {
    let weight = weight.unwrap_or("1.0");
    format!(
        "SELECT CAST({b} AS TEXT), {weight} FROM {table} WHERE {a} = ?1
            UNION ALL SELECT CAST({a} AS TEXT), {weight} FROM {table} WHERE {b} = ?1"
    )
}
