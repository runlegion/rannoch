//! Warm hoods: a node and shape described once and kept, recomputed whole when a write touches
//! any node it read.

use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, Result};
use serde_json::Value;

use crate::describe::{Opts, shaped};

/// One warm hood: its node and shape, their describe, and every node that describe read.
#[derive(Debug)]
struct Hood {
    node: String,
    shape: Value,
    result: Value,
    touched: HashSet<String>,
}

/// The warm hoods: a describe with default opts warms its node and shape, later ones read the copy.
#[derive(Debug, Default)]
pub struct Hoods {
    hoods: HashMap<String, Hood>,
    by_node: HashMap<String, HashSet<String>>,
}

/// A hood's key: its node and its shape's JSON text.
fn hood_key(node: &str, shape: &Value) -> String {
    format!("{node}\u{0}{shape}")
}

impl Hoods {
    /// `node` shaped by `shape` with default opts: the warm copy, or a cold describe kept warm.
    /// # Errors
    /// As `describe`.
    pub fn describe(&mut self, conn: &Connection, node: &str, shape: &Value) -> Result<Value> {
        match self.hoods.get(&hood_key(node, shape)) {
            Some(hood) => Ok(hood.result.clone()),
            None => self.warm(conn, node.to_owned(), shape.clone()),
        }
    }

    /// Describes `node` by `shape` cold and keeps it warm, indexed by every node it read.
    fn warm(&mut self, conn: &Connection, node: String, shape: Value) -> Result<Value> {
        let (key, mut touched) = (hood_key(&node, &shape), HashSet::new());
        let result = Value::Object(shaped(conn, &node, &shape, Opts::default(), &mut touched)?);
        for n in &touched {
            let keys = self.by_node.entry(n.clone()).or_default();
            keys.insert(key.clone());
        }
        let hood = Hood {
            node,
            shape,
            result: result.clone(),
            touched,
        };
        self.hoods.insert(key, hood);
        Ok(result)
    }

    /// Recomputes every warm hood that read a node in `touched`, and says how many it
    /// recomputed; one whose recompute fails goes cold, so the next describe of it is a cold one.
    pub(crate) fn rewarm(&mut self, conn: &Connection, touched: &HashSet<String>) -> usize {
        let keys = touched.iter().filter_map(|n| self.by_node.get(n));
        let keys: HashSet<String> = keys.flatten().cloned().collect();
        let mut recomputed = 0;
        for key in keys {
            let Some(hood) = self.hoods.remove(&key) else {
                continue;
            };
            for n in &hood.touched {
                if let Some(keys) = self.by_node.get_mut(n) {
                    keys.remove(&key);
                }
            }
            recomputed += usize::from(self.warm(conn, hood.node, hood.shape).is_ok());
        }
        recomputed
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use rannoch_core::{Append, Event, Status};
    use rusqlite::Connection;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::Hoods;
    use crate::describe::{Opts, describe, shaped};
    use crate::tests::{apply, link, set, small};
    use crate::{GraphAppend, Op, commit, edges};

    /// The toy's xorshift, so the graph and the burst are the toy's.
    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            usize::try_from(self.0 % n as u64).unwrap()
        }
    }

    const DOCS: usize = 80_000;

    /// The toy's graph at scale: 20,000 people, each knowing one before; 80,000 docs, each
    /// written by one person and citing up to five earlier docs. 100,000 nodes, about 500,000 edges.
    fn big(rng: &mut Rng) -> Connection {
        let persons = 20_000;
        let mut evs = Vec::with_capacity(persons + DOCS);
        for p in 0..persons {
            let id = format!("p{p}");
            let name = json!(format!("person {p}"));
            let mut ops = vec![set(&id, "type", json!("person")), set(&id, "name", name)];
            if p > 0 {
                ops.push(link(&id, "knows", &format!("p{}", rng.below(p)), json!({})));
            }
            evs.push((Uuid::now_v7(), None, ops));
        }
        for d in 0..DOCS {
            let id = format!("d{d}");
            let mut ops = vec![
                set(&id, "type", json!("doc")),
                set(
                    &id,
                    "title",
                    json!(format!("doc {d} about topic {}", d % 97)),
                ),
                set(&id, "status", json!(["open", "closed", "draft"][d % 3])),
                link(&format!("p{}", rng.below(persons)), "wrote", &id, json!({})),
            ];
            for _ in 0..5.min(d) {
                ops.push(link(&id, "cites", &format!("d{}", rng.below(d)), json!({})));
            }
            evs.push((Uuid::now_v7(), None, ops));
        }
        let mut conn = rannoch_core::forgr::empty_store();
        for chunk in evs.chunks(1_000) {
            apply(&mut conn, &chunk.iter().collect::<Vec<_>>(), 0);
        }
        conn
    }

    /// The toy's two-level shape, and its 200 roots.
    fn two_levels() -> (Value, Vec<String>) {
        let cites = json!({ "shape": ["title"], "limit": 5 });
        let cites = json!({ "shape": ["title", { "cites": cites }], "limit": 5 });
        let shape = json!(["title", "status", { "wrote<-": ["name"] }, { "cites": cites }]);
        (
            shape,
            (0..200)
                .map(|i| format!("d{}", DOCS - 1 - i * 13))
                .collect(),
        )
    }

    fn p50<T: Ord + Copy>(mut v: Vec<T>) -> T {
        v.sort_unstable();
        v[v.len() / 2]
    }

    /// The toy's P9: a cold describe of each root, then a warm one; the p50 of each and the p50
    /// of the nodes a describe reads.
    fn timed(conn: &Connection) -> (Duration, Duration, usize) {
        let (shape, roots) = two_levels();
        let (mut cold, mut sizes, mut warm) = (Vec::new(), Vec::new(), Vec::new());
        for r in &roots {
            let mut touched = HashSet::new();
            let start = Instant::now();
            shaped(conn, r, &shape, Opts::default(), &mut touched).unwrap();
            cold.push(start.elapsed());
            sizes.push(touched.len());
        }
        let mut hoods = Hoods::default();
        for r in &roots {
            hoods.describe(conn, r, &shape).unwrap();
        }
        for r in &roots {
            let start = Instant::now();
            hoods.describe(conn, r, &shape).unwrap();
            warm.push(start.elapsed());
        }
        let (cold, warm, nodes) = (p50(cold), p50(warm), p50(sizes));
        println!("cold describe p50 {cold:?} ({nodes} nodes p50), warm p50 {warm:?}");
        (cold, warm, nodes)
    }

    /// How many warm hoods equal a cold describe of their node and shape.
    fn equal(hoods: &Hoods, conn: &Connection) -> usize {
        let cold = |h: &&super::Hood| describe(conn, &h.node, &h.shape, Opts::default()).unwrap();
        hoods.hoods.values().filter(|h| cold(h) == h.result).count()
    }

    /// 50 warm hoods equal a cold describe after a burst of 5,000 writes that touch them and after
    /// its rerun, which recomputes nothing; a warm describe is at least 10 times faster than a
    /// cold one (the toy's P8 and P9).
    #[test]
    fn fr_rannoch_123_c8() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut conn = big(&mut rng);
        let (cold, warm, _) = timed(&conn);
        assert!(cold >= warm * 10, "cold {cold:?}, warm {warm:?}");

        let (shape, roots) = two_levels();
        let mut hoods = Hoods::default();
        for r in roots.iter().take(50) {
            hoods.describe(&conn, r, &shape).unwrap();
        }
        let mut pool: Vec<&String> = hoods.hoods.values().flat_map(|h| &h.touched).collect();
        pool.retain(|n| n.starts_with('d'));
        pool.sort();
        pool.dedup();
        let mut burst = Vec::new();
        for i in 0..5_000 {
            let n = pool[rng.below(pool.len())].clone();
            let status = Op::Retract {
                node: n.clone(),
                key: "status".to_owned(),
            };
            let op = match i % 5 {
                0 => set(
                    &n,
                    "status",
                    json!(["open", "closed", "draft"][rng.below(3)]),
                ),
                1 => set(&n, "title", json!(format!("retitled {i}"))),
                2 => link(
                    &n,
                    "cites",
                    &format!("d{}", rng.below(DOCS)),
                    json!({ "burst": i }),
                ),
                3 => {
                    let cited = edges(&conn, &n, "cites", false, None).unwrap();
                    if cited.is_empty() {
                        status
                    } else {
                        let dst = cited[rng.below(cited.len())].0.clone();
                        let name = "cites".to_owned();
                        Op::Unlink { src: n, name, dst }
                    }
                }
                _ => status,
            };
            burst.push((Uuid::now_v7(), vec![serde_json::to_value(op).unwrap()]));
        }
        let none: &[Event<u16, Value>] = &[];
        let run = |conn: &mut Connection, hoods: &mut Hoods| {
            let mut recomputes = 0;
            for write in &burst {
                let touched = commit(conn, none, std::slice::from_ref(write)).unwrap();
                recomputes += hoods.rewarm(conn, &touched);
            }
            recomputes
        };
        let rows = |c: &Connection| {
            let count = |sql: &str| c.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
            (
                count("SELECT count(*) FROM _nodes"),
                count("SELECT count(*) FROM _edges"),
            )
        };

        let in_burst = run(&mut conn, &mut hoods);
        let (after_burst, before) = (equal(&hoods, &conn), rows(&conn));
        let in_rerun = run(&mut conn, &mut hoods);

        println!("recomputes in the burst {in_burst}, in the rerun {in_rerun}");
        assert_eq!((after_burst, equal(&hoods, &conn)), (50, 50));
        assert!(in_burst > 0);
        assert_eq!(in_rerun, 0);
        assert_eq!(rows(&conn), before);
    }

    /// At 100,000 nodes and about 500,000 edges, a cold describe of a two-level shape answers at
    /// 5 ms p50 or less (the toy's P9).
    #[test]
    fn fr_rannoch_123_c9() {
        let conn = big(&mut Rng(0x9e37_79b9_7f4a_7c15));
        let edges = conn.query_row("SELECT count(*) FROM _edges", [], |r| r.get::<_, i64>(0));
        let (cold, _, _) = timed(&conn);
        assert!(edges.unwrap() > 499_000);
        assert!(
            cold <= Duration::from_millis(5),
            "cold describe p50 {cold:?}"
        );
    }

    /// An append through `GraphAppend` with hoods recomputes the warm hood its rows touched, even
    /// through a far node the hood's filter left out.
    #[test]
    fn an_append_recomputes_the_hoods_its_rows_touch() {
        let (conn, _) = small();
        let shape = json!([{ "wrote": { "shape": ["title"], "where": { "status": "open" } } }]);
        let hoods = Arc::new(Mutex::new(Hoods::default()));
        let writes = rannoch_processor::Writes::default();
        let (queue, _wake) = mpsc::channel();
        let append = GraphAppend::new(conn, queue, writes.clone()).with_hoods(hoods.clone());
        let warm = |append: &GraphAppend| {
            let conn = append.conn.lock().unwrap();
            hoods
                .lock()
                .unwrap()
                .describe(&conn, "ada", &shape)
                .unwrap()
        };
        assert_eq!(warm(&append)["wrote"].as_array().unwrap().len(), 1);

        // doc1, left out by the filter, opens.
        let source = Uuid::now_v7();
        let open = serde_json::to_value(set("doc1", "status", json!("open"))).unwrap();
        writes.lock().unwrap().insert(source, vec![open]);
        let two = Event {
            id: Uuid::now_v7(),
            source: Some(source),
            ty: 1_u16,
            data: json!({}),
            run_at: None,
            status: Status::Processed,
        };
        append.append(&[two]).unwrap();

        let now = warm(&append);
        let cold = describe(&append.conn.lock().unwrap(), "ada", &shape, Opts::default());
        assert_eq!(now, cold.unwrap());
        assert_eq!(now["wrote"].as_array().unwrap().len(), 2);
    }
}
