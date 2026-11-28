//! The scoring tools: Katz, personalized page rank, shared neighbours and learned weight, each
//! a score for the nodes near a root over the app's edge tables, read from graph structure
//! alone.

use std::collections::HashMap;

use rusqlite::{Connection, Result};
use serde_json::{Value, json};

use crate::{Via, cap, hops, queries, quote, vias};

/// The scorers, in the order of their steps: `katz`, `pagerank`, `shared` and `learned`.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Scorer {
    Katz,
    PageRank,
    Shared,
    Learned,
}

/// A scorer with the options it is tuned by.
#[derive(Debug)]
enum Tuned {
    Katz { hops: i64, beta: f64 },
    PageRank { hops: i64, restart: f64 },
    Shared { rare: bool },
    Learned,
}

/// A scorer's options, parsed: its via tables, the scorer tuned, and its cap.
fn parse(scorer: Scorer, options: &Value) -> Result<(Vec<Via>, Tuned, usize), String> {
    let via = vias(options, "weight")?;
    let number = |field: &str| {
        options[field]
            .as_f64()
            .ok_or_else(|| format!("{field} is not a number"))
    };
    let tuned = match scorer {
        Scorer::Katz => Tuned::Katz {
            hops: hops(options)?,
            beta: number("beta")?,
        },
        Scorer::PageRank => Tuned::PageRank {
            hops: hops(options)?,
            restart: number("restart")?,
        },
        Scorer::Shared => Tuned::Shared {
            rare: options["rare"].as_bool().ok_or("rare is not a bool")?,
        },
        Scorer::Learned => Tuned::Learned,
    };
    Ok((via, tuned, cap(options)?))
}

/// The scores of the nodes near `root` by `scorer` and its `options`, as the step gives them:
/// `{"nodes": [{"id", "score"}, ..], "cut": bool}`.
pub(crate) fn score(
    conn: &Connection,
    scorer: Scorer,
    options: &Value,
    root: &str,
) -> Result<Value, String> {
    let (links, tuned, cap) = parse(scorer, options)?;
    let hops = match tuned {
        Tuned::Katz { hops, .. } | Tuned::PageRank { hops, .. } => hops,
        Tuned::Shared { .. } => 2,
        Tuned::Learned => 1,
    };
    // What a row passes on: from the hop it is in, its weight and its node's whole weight.
    let pass = |hop: i64, w: f64, whole: f64| match tuned {
        Tuned::Katz { beta, .. } => beta * w,
        Tuned::PageRank { restart, .. } => (1.0 - restart) * w / whole,
        Tuned::Shared { rare: true } if hop == 1 => w / whole.max(2.0).ln(),
        Tuned::Shared { .. } | Tuned::Learned => w,
    };
    let mass = spread(conn, &links, root, hops, pass).map_err(|e| e.to_string())?;
    let mut totals: HashMap<String, f64> = HashMap::new();
    let counted = match tuned {
        Tuned::Katz { .. } | Tuned::PageRank { .. } => &mass[1..],
        Tuned::Shared { .. } | Tuned::Learned => &mass[mass.len() - 1..],
    };
    let keep = match tuned {
        Tuned::PageRank { restart, .. } => restart,
        _ => 1.0,
    };
    for (node, m) in counted.iter().flatten() {
        *totals.entry(node.clone()).or_default() += keep * m;
    }
    totals.remove(root);
    let mut nodes: Vec<(String, f64)> = totals.into_iter().collect();
    nodes.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let cut = nodes.len() > cap;
    nodes.truncate(cap);
    let nodes: Vec<Value> = nodes
        .iter()
        .map(|(id, score)| json!({ "id": id, "score": score }))
        .collect();
    Ok(json!({ "nodes": nodes, "cut": cut }))
}

/// The mass at each hop from 0 to `hops`, starting as 1 on `root`: each hop, every node passes
/// its mass along each of its rows by `pass(hop, weight, whole)`, whole being the sum of the
/// node's rows' weights.
fn spread(
    conn: &Connection,
    links: &[Via],
    root: &str,
    hops: i64,
    pass: impl Fn(i64, f64, f64) -> f64,
) -> Result<Vec<HashMap<String, f64>>> {
    let mut mass = vec![HashMap::from([(root.to_owned(), 1.0)])];
    for hop in 0..hops {
        let mut next: HashMap<String, f64> = HashMap::new();
        for (node, m) in &mass[mass.len() - 1] {
            let rows = rows(conn, links, node)?;
            let whole: f64 = rows.iter().map(|r| r.1).sum();
            for (far, w) in rows {
                *next.entry(far).or_default() += m * pass(hop, w, whole);
            }
        }
        mass.push(next);
    }
    Ok(mass)
}

/// The rows of `links` that hold `node`, read both ways: each row's other node, as text, and
/// its weight, 1 when its table names no weight.
fn rows(conn: &Connection, links: &[Via], node: &str) -> Result<Vec<(String, f64)>> {
    let mut out = Vec::new();
    for (table, a, b, weight) in links {
        let (table, a, b) = (quote(table), quote(a), quote(b));
        let weight = weight.as_deref().map(quote);
        let sql = queries::rows(&table, &a, &b, weight.as_deref());
        let mut st = conn.prepare_cached(&sql)?;
        let found = st.query_map([node], |r| Ok((r.get(0)?, r.get(1)?)))?;
        for row in found {
            out.push(row?);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{Scorer, parse};

    #[tokio::test(start_paused = true)]
    async fn an_option_that_does_not_parse_fails_with_the_reason() {
        let via = json!({ "cites": { "from": "doc", "to": "cited" } });
        let cases = [
            (Scorer::Katz, json!({ "hops": 2, "beta": 0.5 })),
            (Scorer::PageRank, json!({ "hops": 2, "restart": 0.5 })),
            (Scorer::Shared, json!({ "rare": true })),
            (Scorer::Learned, json!({})),
        ];
        for (scorer, tuning) in cases {
            let fails = |options: Value| parse(scorer, &options).unwrap_err();
            assert_eq!(fails(json!({})), "via is not an object", "{scorer:?}");
            assert_eq!(
                fails(json!({ "via": { "cites": { "from": "doc" } } })),
                "via cites has no from and to",
                "{scorer:?}"
            );
            let mut options = tuning;
            options["via"] = via.clone();
            assert_eq!(
                fails(options.clone()),
                "cap is not a whole number",
                "{scorer:?}"
            );
            options["cap"] = json!(-1);
            assert_eq!(fails(options), "cap is not a whole number", "{scorer:?}");
        }
        let fails = |scorer, options: Value| parse(scorer, &options).unwrap_err();
        for scorer in [Scorer::Katz, Scorer::PageRank] {
            assert_eq!(
                fails(scorer, json!({ "via": via, "hops": 1.5 })),
                "hops is not a whole number",
                "{scorer:?}"
            );
        }
        assert_eq!(
            fails(
                Scorer::Katz,
                json!({ "via": via, "hops": 2, "beta": "half" })
            ),
            "beta is not a number"
        );
        assert_eq!(
            fails(Scorer::PageRank, json!({ "via": via, "hops": 2 })),
            "restart is not a number"
        );
        assert_eq!(
            fails(Scorer::Shared, json!({ "via": via, "rare": 1 })),
            "rare is not a bool"
        );
    }
}
