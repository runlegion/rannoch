//! Walking the named edges out from a node, in one recursive query.

use rusqlite::{Connection, Result, params};
use serde_json::Value;

use crate::queries;

/// The nodes reached from `root` over `via` (a name follows an edge forwards, name<- backwards),
/// nearest first as (node, hops), at most `cap`, and whether the cap cut it.
/// # Errors
/// The store's error.
pub fn walk(
    conn: &Connection,
    root: &str,
    via: &[&str],
    hops: i64,
    cap: usize,
    at: Option<i64>,
) -> Result<(Vec<(String, i64)>, bool)> {
    let back: Vec<&str> = via.iter().filter_map(|v| v.strip_suffix("<-")).collect();
    let forth: Vec<&str> = via.iter().copied().filter(|v| !v.ends_with("<-")).collect();
    let (forth, back) = (
        Value::from(forth).to_string(),
        Value::from(back).to_string(),
    );
    // One past the cap, so a walk the cap cut tells itself from one that ended at it.
    let limit = i64::try_from(cap).map_or(i64::MAX, |cap| cap.saturating_add(1));
    let mut st = conn.prepare_cached(queries::WALK)?;
    let args = params![root, hops, at, forth, back, limit];
    let rows = st.query_map(args, |r| Ok((r.get(0)?, r.get(1)?)))?;
    let mut nodes = rows.collect::<Result<Vec<_>>>()?;
    let cut = nodes.len() > cap;
    nodes.truncate(cap);
    Ok((nodes, cut))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use rusqlite::Connection;
    use serde_json::json;
    use uuid::Uuid;

    use super::walk;
    use crate::{Op, edges, write};

    /// The toy's xorshift, so the graph below is the toy's.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            usize::try_from(self.0 % u64::try_from(n).unwrap()).unwrap()
        }
    }

    const DOCS: usize = 80_000;

    /// The toy's graph at scale, its edges only: 20,000 people, each after the first knowing an
    /// earlier one, and 80,000 docs, each written by one person and citing up to 5 earlier docs.
    fn big() -> Connection {
        let people = 20_000;
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut conn = rannoch_core::forgr::empty_store();
        let tx = conn.transaction().unwrap();
        let link = |event: &str, src: String, name: &str, dst: String| {
            let name = name.to_owned();
            let op = Op::Link {
                src,
                name,
                dst,
                props: json!({}),
            };
            write(&tx, event, &op, 0).unwrap();
        };
        for p in 1..people {
            let event = Uuid::now_v7().to_string();
            link(
                &event,
                format!("p{p}"),
                "knows",
                format!("p{}", rng.below(p)),
            );
        }
        for d in 0..DOCS {
            let event = Uuid::now_v7().to_string();
            let author = format!("p{}", rng.below(people));
            link(&event, author, "wrote", format!("d{d}"));
            for _ in 0..d.min(5) {
                link(
                    &event,
                    format!("d{d}"),
                    "cites",
                    format!("d{}", rng.below(d)),
                );
            }
        }
        tx.commit().unwrap();
        conn
    }

    /// The same walk done in Rust, breadth first, over `edges` (the toy's `Db::bfs`).
    fn bfs(conn: &Connection, root: &str, via: &[&str], hops: i64) -> Vec<(String, i64)> {
        let mut seen = HashMap::from([(root.to_owned(), 0)]);
        let mut queue = VecDeque::from([(root.to_owned(), 0)]);
        while let Some((node, depth)) = queue.pop_front() {
            if depth == hops {
                continue;
            }
            for v in via {
                let (name, reverse) = v.strip_suffix("<-").map_or((*v, false), |n| (n, true));
                for (far, _) in edges(conn, &node, name, reverse, None).unwrap() {
                    seen.entry(far.clone()).or_insert_with(|| {
                        queue.push_back((far, depth + 1));
                        depth + 1
                    });
                }
            }
        }
        let mut out: Vec<_> = seen.into_iter().filter(|(n, _)| n != root).collect();
        out.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        out
    }

    /// walk equals a breadth-first walk in Rust on 50 roots, 2 hops over cites and wrote<-, and
    /// a cap of 10 cuts and says so (the toy's P6).
    #[test]
    fn fr_rannoch_123_c6() {
        let conn = big();
        let via = ["cites", "wrote<-"];
        let mut cut_seen = false;
        for r in 0..50 {
            let root = format!("d{}", DOCS - 1 - r * 7);
            let full = bfs(&conn, &root, &via, 2);

            let whole = walk(&conn, &root, &via, 2, 1_000_000, None).unwrap();
            let (capped, cut) = walk(&conn, &root, &via, 2, 10, None).unwrap();

            assert_eq!(whole, (full.clone(), false), "root {root}");
            assert_eq!(capped, full[..10.min(full.len())], "root {root}");
            assert_eq!(cut, full.len() > 10, "root {root}");
            cut_seen |= cut;
        }
        assert!(cut_seen);
    }
}
