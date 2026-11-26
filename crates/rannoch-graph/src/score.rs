//! The scoring tools: Katz, personalized page rank, shared neighbours and learned weight, each
//! a score for the nodes near a root over the app's edge tables, read from graph structure
//! alone.

use std::collections::HashMap;

use rusqlite::{Connection, Result};
use serde_json::{Value, json};

use crate::quote;

/// An edge table a scorer reads both ways: the table, its two node columns, and the column of
/// its rows' weight, when it names one.
type Link = (String, String, String, Option<String>);

/// The scorers, in the order of their steps: `katz`, `pagerank`, `shared` and `learned`.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Scorer {
    Katz,
    PageRank,
    Shared,
    Learned,
}

/// A scorer's options, parsed: its links, its hops, the number it is tuned by, and its cap.
fn parse(scorer: Scorer, options: &Value) -> Result<(Vec<Link>, i64, f64, usize), String> {
    let via = options["via"].as_object().ok_or("via is not an object")?;
    let via = via.iter().map(|(table, cols)| {
        let col = |c: &str| cols[c].as_str().map(str::to_owned);
        let not = || format!("via {table} has no from and to");
        Ok((
            table.clone(),
            col("from").ok_or_else(not)?,
            col("to").ok_or_else(not)?,
            col("weight"),
        ))
    });
    let via = via.collect::<Result<Vec<_>, String>>()?;
    let number = |field: &str| {
        options[field]
            .as_f64()
            .ok_or_else(|| format!("{field} is not a number"))
    };
    let hops = || {
        options["hops"]
            .as_i64()
            .ok_or_else(|| "hops is not a whole number".to_owned())
    };
    let (hops, by) = match scorer {
        Scorer::Katz => (hops()?, number("beta")?),
        Scorer::PageRank => (hops()?, number("restart")?),
        Scorer::Shared => {
            let rare = options["rare"].as_bool().ok_or("rare is not a bool")?;
            (2, f64::from(u8::from(rare)))
        }
        Scorer::Learned => (1, 1.0),
    };
    let cap = options["cap"].as_u64().ok_or("cap is not a whole number")?;
    Ok((via, hops, by, usize::try_from(cap).unwrap_or(usize::MAX)))
}

/// The scores of the nodes near `root` by `scorer` and its `options`, as the step gives them:
/// `{"nodes": [{"id", "score"}, ..], "cut": bool}`.
pub(crate) fn score(
    conn: &Connection,
    scorer: Scorer,
    options: &Value,
    root: &str,
) -> Result<Value, String> {
    let (links, hops, by, cap) = parse(scorer, options)?;
    // What a row passes on: from the hop it is in, its weight and its node's whole weight.
    let pass = |hop: i64, w: f64, whole: f64| match scorer {
        Scorer::Katz => by * w,
        Scorer::PageRank => (1.0 - by) * w / whole,
        Scorer::Shared if hop == 1 && by > 0.0 => w / whole.max(2.0).ln(),
        Scorer::Shared | Scorer::Learned => w,
    };
    let mass = spread(conn, &links, root, hops, pass).map_err(|e| e.to_string())?;
    let mut totals: HashMap<String, f64> = HashMap::new();
    let counted = match scorer {
        Scorer::Katz | Scorer::PageRank => &mass[1..],
        Scorer::Shared | Scorer::Learned => &mass[mass.len() - 1..],
    };
    let keep = if matches!(scorer, Scorer::PageRank) {
        by
    } else {
        1.0
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
    links: &[Link],
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
fn rows(conn: &Connection, links: &[Link], node: &str) -> Result<Vec<(String, f64)>> {
    let mut out = Vec::new();
    for (table, a, b, weight) in links {
        let (table, a, b) = (quote(table), quote(a), quote(b));
        let weight = weight.as_deref().map_or_else(|| "1.0".to_owned(), quote);
        let sql = format!(
            "SELECT CAST({b} AS TEXT), {weight} FROM {table} WHERE {a} = ?1
            UNION ALL SELECT CAST({a} AS TEXT), {weight} FROM {table} WHERE {b} = ?1"
        );
        let mut st = conn.prepare_cached(&sql)?;
        let found = st.query_map([node], |r| Ok((r.get(0)?, r.get(1)?)))?;
        for row in found {
            out.push(row?);
        }
    }
    Ok(out)
}
