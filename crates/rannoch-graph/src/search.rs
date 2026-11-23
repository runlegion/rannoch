//! Finding nodes by their current text, through SQLite's full-text index (FTS5) in `_search`.

use rusqlite::{Connection, Result, params};
use serde_json::Value;

use crate::{Row, prop, queries};

/// The nodes whose current text values match `text` (FTS5 query syntax).
/// # Errors
/// The store's error, among them `text` that is not an FTS5 query.
pub fn search(conn: &Connection, text: &str) -> Result<Vec<String>> {
    let mut st = conn.prepare_cached(queries::SEARCH)?;
    let nodes = st.query_map([text], |r| r.get(0))?;
    nodes.collect()
}

/// Keeps `_search` on `node`'s `key`: its old text out, and its current value in when that is a
/// string.
pub(crate) fn reindex(conn: &Connection, node: &str, key: &str) -> Result<()> {
    conn.prepare_cached(queries::UNINDEX)?
        .execute(params![node, key])?;
    conn.prepare_cached(queries::UNMAP)?
        .execute(params![node, key])?;
    if let Some(Row {
        value: Value::String(text),
        ..
    }) = prop(conn, node, key, None)?
    {
        conn.prepare_cached(queries::INDEX)?.execute([text])?;
        let row = conn.last_insert_rowid();
        conn.prepare_cached(queries::MAP)?
            .execute(params![node, key, row])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::search;
    use crate::tests::{apply, set, small};

    /// search finds a node by its current text, and after a correction by the new text and not
    /// the old (the toy's P7).
    #[test]
    fn fr_rannoch_123_c7() {
        let (mut conn, _) = small();
        let found = search(&conn, "graph").unwrap();

        let title = set("doc1", "title", json!("vector index notes"));
        apply(&mut conn, &[&(Uuid::now_v7(), None, vec![title])], 11_000);

        assert_eq!(found, ["doc1"]);
        assert_eq!(search(&conn, "graph").unwrap(), Vec::<String>::new());
        assert_eq!(search(&conn, "vector").unwrap(), ["doc1"]);
    }
}
