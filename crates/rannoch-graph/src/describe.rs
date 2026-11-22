//! A node shaped by a JSON pull-style shape, each value with the event that wrote it and when.

use std::collections::HashSet;

use rusqlite::{Connection, Result};
use serde_json::{Map, Value, json};

use crate::{Row, State, edges, history, prop, props};

/// A describe's options: as of `at` when given, and every value over time when `history`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Opts {
    pub at: Option<i64>,
    pub history: bool,
}

/// The node shaped by `shape`; a cold read.
/// # Errors
/// As `prop`, or rusqlite's `ToSqlConversionFailure` for a shape that is not a list of property
/// names, "*", {edge: shape} and {edge: {shape, limit, where}}: an item that does not parse is
/// refused when the walk reaches it.
pub fn describe(conn: &Connection, node: &str, shape: &Value, opts: Opts) -> Result<Value> {
    shaped(conn, node, shape, opts, &mut HashSet::new()).map(Value::Object)
}

/// `node` shaped by `shape`; every node it reads, a far node filtered out or not, goes into
/// `touched`.
pub(crate) fn shaped(
    conn: &Connection,
    node: &str,
    shape: &Value,
    o: Opts,
    touched: &mut HashSet<String>,
) -> Result<Map<String, Value>> {
    touched.insert(node.to_owned());
    let mut out = Map::new();
    out.insert("id".to_owned(), json!(node));
    for item in shape.as_array().ok_or_else(bad_shape)? {
        match item {
            Value::String(k) if k == "*" => {
                for (key, _) in props(conn, node, o.at)? {
                    if let Some(v) = value(conn, node, &key, o)? {
                        out.insert(key, v);
                    }
                }
            }
            Value::String(k) => {
                if let Some(v) = value(conn, node, k, o)? {
                    out.insert(k.clone(), v);
                }
            }
            Value::Object(m) => {
                for (spec, sub) in m {
                    let (name, reverse) = spec
                        .strip_suffix("<-")
                        .map_or((spec.as_str(), false), |n| (n, true));
                    let (subshape, limit, filter) = match sub {
                        Value::Array(_) => (sub, None, None),
                        Value::Object(x) => (
                            x.get("shape").ok_or_else(bad_shape)?,
                            x.get("limit").and_then(Value::as_u64),
                            x.get("where").and_then(Value::as_object),
                        ),
                        _ => return Err(bad_shape()),
                    };
                    let limit = limit.and_then(|l| usize::try_from(l).ok());
                    let mut kids = Vec::new();
                    for (far, edge) in edges(conn, node, name, reverse, o.at)? {
                        if limit.is_some_and(|l| kids.len() >= l) {
                            break;
                        }
                        touched.insert(far.clone());
                        let mut kept = true;
                        for (k, want) in filter.into_iter().flatten() {
                            kept = kept
                                && prop(conn, &far, k, o.at)?.is_some_and(|r| r.value == *want);
                        }
                        if kept {
                            let mut child = shaped(conn, &far, subshape, o, touched)?;
                            child.insert("edge".to_owned(), stamped(&edge, "props"));
                            kids.push(Value::Object(child));
                        }
                    }
                    out.insert(spec.clone(), Value::Array(kids));
                }
            }
            _ => return Err(bad_shape()),
        }
    }
    Ok(out)
}

/// `node`'s `key` with its event and written, or with `o.history` every value it has held, each
/// marked current, replaced or retracted; None when it has none.
fn value(conn: &Connection, node: &str, key: &str, o: Opts) -> Result<Option<Value>> {
    if !o.history {
        return Ok(prop(conn, node, key, o.at)?.map(|row| stamped(&row, "value")));
    }
    let rows = history(conn, node, key, o.at)?
        .into_iter()
        .map(|(row, state)| {
            let mut v = stamped(&row, "value");
            v["state"] = json!(match state {
                State::Current => "current",
                State::Replaced => "replaced",
                State::Retracted => "retracted",
            });
            v
        });
    let rows: Vec<Value> = rows.collect();
    Ok((!rows.is_empty()).then_some(Value::Array(rows)))
}

/// `row` as JSON: its value under `name`, the event that wrote it and when it was written.
fn stamped(row: &Row, name: &str) -> Value {
    json!({ name: row.value, "event": row.event.to_string(), "written": row.written })
}

/// A shape that does not parse, as rusqlite's conversion failure of what goes in.
fn bad_shape() -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure("not a describe shape".into())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{Opts, describe};
    use crate::tests::small;

    /// The ids of a followed edge's far nodes, in order.
    fn ids(kids: &Value) -> Vec<&str> {
        kids.as_array()
            .unwrap()
            .iter()
            .map(|k| k["id"].as_str().unwrap())
            .collect()
    }

    /// Every shape feature, nested two levels, on the toy's small graph (the toy's P5).
    #[test]
    fn fr_rannoch_123_c5() {
        let (conn, [_, e1, e2, _]) = small();
        let now = Opts::default();
        let d = |node: &str, shape: Value, o: Opts| describe(&conn, node, &shape, o).unwrap();

        let star = d("ada", json!(["*"]), now);
        assert_eq!(star["type"]["value"], json!("person"));
        assert_eq!(star["name"]["value"], json!("Ada Lovelace"));
        let forwards = d("ada", json!([{ "wrote": ["title"] }]), now);
        assert_eq!(ids(&forwards["wrote"]), ["doc1", "doc2", "doc3"]);
        let backwards = d("doc1", json!([{ "wrote<-": ["name"] }]), now);
        assert_eq!(ids(&backwards["wrote<-"]), ["ada"]);
        let limit = json!([{ "wrote": { "shape": ["title"], "limit": 2 } }]);
        assert_eq!(ids(&d("ada", limit, now)["wrote"]), ["doc1", "doc2"]);
        let open = json!([{ "wrote": { "shape": ["title"], "where": { "status": "open" } } }]);
        assert_eq!(ids(&d("ada", open, now)["wrote"]), ["doc2"]);
        let history = d(
            "ada",
            json!(["name"]),
            Opts {
                history: true,
                ..now
            },
        );
        let states: Vec<&Value> = history["name"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| &h["state"])
            .collect();
        assert_eq!(states, [&json!("replaced"), &json!("current")]);
        let then = Opts {
            at: Some(1_500),
            ..now
        };
        assert_eq!(
            d("ada", json!(["name", { "wrote": ["title"] }]), then)["name"]["value"],
            json!("Ada")
        );
        let nested = d(
            "ada",
            json!(["name", { "wrote": ["title", { "cites": ["title"] }] }]),
            now,
        );
        assert_eq!(ids(&nested["wrote"][0]["cites"]), ["doc2", "doc3"]);

        // The result mirrors the shape, and every value and edge carries its event and written.
        let shape = json!(["title", { "wrote<-": ["name"] }, { "cites": { "shape": ["title"], "limit": 1 } }]);
        let (e1, e2) = (e1.0.to_string(), e2.0.to_string());
        let stamp = |value: Value, event: &str, written: i64| json!({ "value": value, "event": event, "written": written });
        let edge = |props: Value| json!({ "props": props, "event": e1, "written": 1_100 });
        assert_eq!(
            d("doc1", shape, now),
            json!({
                "id": "doc1",
                "title": stamp(json!("graph store notes"), &e1, 1_100),
                "wrote<-": [{ "id": "ada", "name": stamp(json!("Ada Lovelace"), &e2, 2_000), "edge": edge(json!({})) }],
                "cites": [{ "id": "doc2", "title": stamp(json!("queue notes"), &e1, 1_100), "edge": edge(json!({ "page": 3 })) }]
            })
        );
    }

    /// A shape that is not a list of names, "*", {edge: shape} and {edge: {shape, limit, where}}
    /// items is rusqlite's conversion failure, not a panic.
    #[test]
    fn a_shape_that_does_not_parse_is_an_error() {
        let (conn, _) = small();
        for shape in [
            json!("name"),
            json!([1]),
            json!([{ "wrote": 1 }]),
            json!([{ "wrote": {} }]),
        ] {
            let refused = describe(&conn, "ada", &shape, Opts::default());
            assert!(
                matches!(refused, Err(rusqlite::Error::ToSqlConversionFailure(_))),
                "{shape}: {refused:?}"
            );
        }
    }
}
