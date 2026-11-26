//! Warm hoods: a walk kept with its result, dropped when a `store` row touches it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rannoch_core::{Append, Error, Event, Status};
use rannoch_events::{StoreRow, Writes};
use rusqlite::Connection;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{Via, walk};

/// Where a row touches a hood: its table, a `from` column, and a node the hood read.
type Touch = (String, String, String);

/// One warm hood: a walk's result, and where a row touches it.
#[derive(Debug)]
struct Hood {
    result: Value,
    touches: Vec<Touch>,
}

/// The hoods and, for each place a row touches, the hoods it touches.
#[derive(Debug, Default)]
struct Kept {
    hoods: HashMap<String, Hood>,
    by_touch: HashMap<Touch, HashSet<String>>,
}

/// The warm hoods the `walk` step reads through and a [`Warm`] append drops.
#[derive(Debug, Clone, Default)]
pub struct Hoods(Arc<Mutex<Kept>>);

/// A walk's options, parsed: its via tables, its hops and its cap.
fn parse(options: &Value) -> Result<(Vec<Via>, i64, usize), String> {
    let via = options["via"].as_object().ok_or("via is not an object")?;
    let via = via.iter().map(|(table, cols)| {
        let col = |c: &str| cols[c].as_str().map(str::to_owned);
        let not = || format!("via {table} has no from and to");
        Ok((
            table.clone(),
            col("from").ok_or_else(not)?,
            col("to").ok_or_else(not)?,
        ))
    });
    let via = via.collect::<Result<Vec<_>, String>>()?;
    let hops = options["hops"]
        .as_i64()
        .ok_or("hops is not a whole number")?;
    let cap = options["cap"].as_u64().ok_or("cap is not a whole number")?;
    Ok((via, hops, usize::try_from(cap).unwrap_or(usize::MAX)))
}

/// The walk from `root` by `options`, cold, as the step gives it, and where a row touches it.
fn cold(conn: &Connection, options: &Value, root: &str) -> Result<(Value, Vec<Touch>), String> {
    let (via, hops, cap) = parse(options)?;
    let (nodes, cut) = walk(conn, &via, root, hops, cap).map_err(|e| e.to_string())?;
    // A row from a node the walk did not return cannot change it, the cap's cut included: what
    // such a row reaches is further than every node returned.
    let read = nodes.iter().map(|(n, _)| n.as_str()).chain([root]);
    let read: Vec<&str> = read.collect();
    let touches = via.iter().flat_map(|(table, from, _)| {
        read.iter()
            .map(|n| (table.clone(), from.clone(), (*n).to_owned()))
    });
    let nodes: Vec<Value> = nodes
        .iter()
        .map(|(id, hops)| json!({ "id": id, "hops": hops }))
        .collect();
    Ok((json!({ "nodes": nodes, "cut": cut }), touches.collect()))
}

impl Hoods {
    fn lock(&self) -> MutexGuard<'_, Kept> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The walk from `root` by `options`: the warm copy, or a cold walk on `open`'s connection,
    /// kept warm. The hoods stay locked while it reads, so no drop passes it by.
    pub(crate) fn walk(
        &self,
        options: &Value,
        root: &str,
        open: impl FnOnce() -> Result<Connection, String>,
    ) -> Result<Value, String> {
        let mut kept = self.lock();
        let key = format!("{options}\u{0}{root}");
        if let Some(hood) = kept.hoods.get(&key) {
            return Ok(hood.result.clone());
        }
        let (result, touches) = cold(&open()?, options, root)?;
        kept.keep(key, result.clone(), touches);
        Ok(result)
    }

    /// Drops every hood a row of `rows` touches, so its next walk reads the store, and says how
    /// many it dropped. It reads nothing.
    pub(crate) fn cool(&self, rows: &[StoreRow]) -> usize {
        let mut kept = self.lock();
        let mut keys = HashSet::new();
        for StoreRow::Store { table, row } in rows {
            for (column, value) in row {
                let node = value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned);
                let touch = (table.clone(), column.clone(), node);
                keys.extend(kept.by_touch.get(&touch).into_iter().flatten().cloned());
            }
        }
        keys.iter().filter(|key| kept.drop_hood(key)).count()
    }
}

impl Kept {
    /// Keeps a hood warm under `key`, indexed by where a row touches it.
    fn keep(&mut self, key: String, result: Value, touches: Vec<Touch>) {
        for touch in &touches {
            self.by_touch
                .entry(touch.clone())
                .or_default()
                .insert(key.clone());
        }
        let hood = Hood { result, touches };
        self.hoods.insert(key, hood);
    }

    /// Drops the hood under `key`, with its index entries; whether there was one.
    fn drop_hood(&mut self, key: &str) -> bool {
        let Some(hood) = self.hoods.remove(key) else {
            return false;
        };
        for touch in &hood.touches {
            if let Some(keys) = self.by_touch.get_mut(touch) {
                keys.remove(key);
                if keys.is_empty() {
                    self.by_touch.remove(touch);
                }
            }
        }
        true
    }
}

/// The app's append, with the hoods each batch's `store` rows touch dropped once it commits.
#[derive(Debug)]
pub struct Warm<A> {
    append: A,
    writes: Writes,
    hoods: Hoods,
}

impl<A> Warm<A> {
    /// `append`, which writes the store rows the processor's `writes` holds, with the `hoods`
    /// its rows touch dropped after each batch it commits.
    #[must_use]
    pub fn new(append: A, writes: Writes, hoods: Hoods) -> Self {
        Self {
            append,
            writes,
            hoods,
        }
    }
}

impl<T, D, A: Append<T, D>> Append<T, D> for Warm<A> {
    fn append(&self, batch: &[Event<T, D>]) -> Result<Vec<Event<T, D>>, Error> {
        // The items of each 2, read before the append takes them.
        let items: Vec<(Uuid, Vec<Value>)> = {
            let held = self.writes.lock().unwrap_or_else(PoisonError::into_inner);
            let twos = batch.iter().filter(|e| e.status == Status::Processed);
            twos.filter_map(|e| Some((e.source?, held.get(&e.source?)?.clone())))
                .collect()
        };
        let replies = self.append.append(batch)?;
        // A 2 the append refused wrote no row; dropping a hood it names only makes it read again.
        let rows = items.into_iter().flat_map(|(_, items)| items);
        let rows: Vec<StoreRow> = rows
            .filter_map(|i| serde_json::from_value(i).ok())
            .collect();
        self.hoods.cool(&rows);
        Ok(replies)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::path::Path;

    use rusqlite::Connection;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use rannoch_core::{Append, Error, Event, Status};
    use rannoch_events::{StoreRow, Writes};
    use rannoch_processor::Action;

    use super::{Hoods, Warm, cold, parse};
    use crate::Step;
    use crate::tests::{DOCS, store};

    /// A store row into `table`.
    fn row(table: &str, row: Value) -> StoreRow {
        let row = serde_json::from_value(row).unwrap();
        StoreRow::Store {
            table: table.to_owned(),
            row,
        }
    }

    fn ids(walked: &Value) -> Vec<&str> {
        let nodes = walked["nodes"].as_array().unwrap();
        nodes.iter().map(|n| n["id"].as_str().unwrap()).collect()
    }

    fn none() -> Result<Connection, String> {
        Err("no store".to_owned())
    }

    /// FR-RANNOCH-127-c1: every walk the step makes is warm. The second walk is the kept copy,
    /// read with no connection; a store row that touches the hood drops it, reading nothing, so
    /// the next walk reads and equals a cold walk; one that touches no hood drops none.
    #[test]
    fn fr_rannoch_127_c1() {
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        let path = target.join(format!("rannoch-hoods-{}.db", Uuid::now_v7()));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(DOCS).unwrap();
        conn.execute_batch("INSERT INTO cites VALUES ('d1', 'd2'), ('d2', 'd3'), ('d9', 'd8')")
            .unwrap();
        let options =
            json!({ "via": { "cites": { "from": "doc", "to": "cited" } }, "hops": 3, "cap": 10 });
        let hoods = Hoods::default();
        let opened = Cell::new(0);
        let open = || {
            opened.set(opened.get() + 1);
            Connection::open(&path).map_err(|e| e.to_string())
        };

        let first = hoods.walk(&options, "d1", open).unwrap();
        let again = hoods.walk(&options, "d1", none).unwrap();
        assert_eq!((ids(&first), &again), (vec!["d2", "d3"], &first));
        assert_eq!(opened.get(), 1);

        // d3 cites d4: d3 is a node the walk returned, so its hood is dropped.
        conn.execute("INSERT INTO cites VALUES ('d3', 'd4')", [])
            .unwrap();
        let touching = [row("cites", json!({ "doc": "d3", "cited": "d4" }))];
        assert_eq!(hoods.cool(&touching), 1);
        let read = hoods.walk(&options, "d1", open).unwrap();
        assert_eq!(opened.get(), 2);
        assert_eq!(read, cold(&conn, &options, "d1").unwrap().0);
        assert_eq!(ids(&read), ["d2", "d3", "d4"]);
        assert_eq!(hoods.walk(&options, "d1", none).unwrap(), read);

        // d9 is no node the hood read, and doc_titles is none of its via tables.
        let elsewhere = [
            row("cites", json!({ "doc": "d9", "cited": "d1" })),
            row("doc_titles", json!({ "doc": "d1" })),
        ];
        assert_eq!(hoods.cool(&elsewhere), 0);
        assert_eq!(hoods.walk(&options, "d1", none).unwrap(), read);
        drop(conn);
        std::fs::remove_file(&path).unwrap();
    }

    /// An inner append that fails, or takes each event's held items as the real one does.
    #[derive(Debug)]
    struct Stub(Writes, bool);

    impl Append<u8, ()> for Stub {
        fn append(&self, batch: &[Event<u8, ()>]) -> Result<Vec<Event<u8, ()>>, Error> {
            if self.1 {
                return Err(Error::Store);
            }
            let mut held = self.0.lock().unwrap();
            for source in batch.iter().filter_map(|e| e.source) {
                held.remove(&source);
            }
            Ok(Vec::new())
        }
    }

    /// An event whose source is `source`, at `status`.
    fn event(source: Uuid, status: Status) -> Event<u8, ()> {
        Event {
            id: Uuid::now_v7(),
            source: Some(source),
            ty: 1,
            data: (),
            run_at: None,
            status,
        }
    }

    /// A `Warm` drops the hood a 2's held store row touches once the inner append commits,
    /// and drops nothing when the append fails or for an event that is not a 2.
    #[test]
    fn warm_drops_the_hoods_a_committed_2_touches() {
        let options =
            json!({ "via": { "cites": { "from": "doc", "to": "cited" } }, "hops": 2, "cap": 10 });
        let conn = store(DOCS);
        conn.execute("INSERT INTO cites VALUES ('d1', 'd2')", [])
            .unwrap();
        let hoods = Hoods::default();
        hoods.walk(&options, "d1", || Ok(conn)).unwrap();
        let kept = || hoods.walk(&options, "d1", none).is_ok();

        let writes = Writes::default();
        let touching =
            json!({ "store": { "table": "cites", "row": { "doc": "d2", "cited": "d5" } } });
        let (one, two) = (Uuid::now_v7(), Uuid::now_v7());
        writes.lock().unwrap().insert(one, vec![touching.clone()]);
        writes.lock().unwrap().insert(two, vec![touching]);

        let failing = Warm::new(Stub(writes.clone(), true), writes.clone(), hoods.clone());
        assert!(failing.append(&[event(two, Status::Processed)]).is_err());
        assert!(kept());

        let warm = Warm::new(Stub(writes.clone(), false), writes.clone(), hoods.clone());
        warm.append(&[event(one, Status::InProgress)]).unwrap();
        assert!(kept());

        warm.append(&[event(two, Status::Processed)]).unwrap();
        assert!(writes.lock().unwrap().is_empty());
        assert!(!kept());
    }

    /// Each option or input that does not parse fails its step with the reason.
    #[test]
    fn an_option_that_does_not_parse_fails_with_the_reason() {
        let fails = |options: Value| parse(&options).unwrap_err();
        assert_eq!(fails(json!({})), "via is not an object");
        assert_eq!(
            fails(json!({ "via": { "cites": { "from": "doc" } } })),
            "via cites has no from and to"
        );
        let via = json!({ "cites": { "from": "doc", "to": "cited" } });
        assert_eq!(
            fails(json!({ "via": via, "hops": 1.5 })),
            "hops is not a whole number"
        );
        assert_eq!(
            fails(json!({ "via": via, "hops": 2, "cap": -1 })),
            "cap is not a whole number"
        );

        let run = |step, options: Value, input: Value| {
            let input = input.as_object().unwrap().clone();
            Step(step, Hoods::default())
                .run(&options, &input)
                .unwrap_err()
        };
        let as_of = json!({ "table": "doc_titles", "key": "doc", "time": "written" });
        assert_eq!(
            run(0, as_of.clone(), json!({ "node": "d1" })),
            "as_of: at is not a whole number of ms"
        );
        assert_eq!(
            run(0, json!({ "table": 7 }), json!({ "node": "d1", "at": 1 })),
            "as_of: table is not a name"
        );
        assert_eq!(run(1, json!({}), json!({})), "why: node is not a string");
    }
}
