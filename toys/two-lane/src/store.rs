//! The toy's own store, on SQLite, built to the two-lane shape. One write per fact: the row as it
//! is now, with the id of the event that made it. Edges join rows. A flow's state is a part of
//! its row. The host's answers are kept by action and event, in the same transaction as the rows
//! they changed. No log table, no hearing table, no second copy of a row.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::entrance::Event;
use crate::flow::{Answer, State};

/// A row as it is now.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Row {
    /// What the event that defines the row said: an order's placing, a product's listing.
    pub data: Value,
    /// Each type of event that has happened to the row, at its latest time.
    pub types: BTreeMap<String, i64>,
    /// The ids of facts applied within the lateness window.
    pub facts: BTreeMap<String, i64>,
    /// Each flow's state, by flow.
    pub flows: BTreeMap<String, State>,
    pub event: String,
    pub time: i64,
}

/// Where an event's own fact lands: the row's kind and key, whether the event defines the row,
/// and the edges it draws.
pub struct Fact {
    pub kind: &'static str,
    pub key: String,
    pub defines: bool,
    pub edges: Vec<(&'static str, &'static str, String)>,
}

fn s(data: &Value, name: &str) -> String {
    data.get(name).and_then(Value::as_str).unwrap_or_default().to_string()
}

/// The fact an event says, if it says one.
#[must_use]
pub fn fact_of(event: &Event) -> Option<Fact> {
    let d = &event.data;
    let ty = event.ty.as_str();
    let fact = |kind, key: String, defines, edges| Some(Fact { kind, key, defines, edges });
    match ty {
        "westwind.customer.joined" => fact("westwind_customer", s(d, "id"), true, vec![]),
        "westwind.supplier.joined" => fact("westwind_supplier", s(d, "id"), true, vec![]),
        "westwind.shipper.joined" => fact("westwind_shipper", s(d, "id"), true, vec![]),
        "westwind.product.listed" => fact("westwind_product", s(d, "id"), true, vec![("supplied_by", "westwind_supplier", s(d, "supplier_id"))]),
        "westwind.employee.hired" => {
            let boss = s(d, "reports_to");
            let edges = if boss.is_empty() { vec![] } else { vec![("reports_to", "westwind_employee", boss)] };
            fact("westwind_employee", s(d, "id"), true, edges)
        }
        "westwind.order.placed" => {
            let mut edges = vec![("ordered_by", "westwind_customer", s(d, "customer_id"))];
            let products: BTreeSet<String> = d.get("lines").and_then(Value::as_array).map(|l| l.iter().map(|x| s(x, "product_id")).collect()).unwrap_or_default();
            edges.extend(products.into_iter().map(|p| ("contains", "westwind_product", p)));
            fact("westwind_order", s(d, "order_id"), true, edges)
        }
        "westwind.supplier.delivered" => fact("westwind_product", s(d, "product_id"), false, vec![]),
        "westwind.ticket.opened" => fact("westwind_ticket", event.id.clone(), true, vec![("about", "westwind_order", s(d, "order_id"))]),
        "westwind.subscription.started" => fact("westwind_subscription", s(d, "subscription_id"), true, vec![]),
        "westwind.subscription.due" => fact("westwind_subscription", s(d, "subscription_id"), false, vec![]),
        "oz.citizen.joined" => fact("oz_citizen", s(d, "id"), true, vec![]),
        "oz.order.placed" => fact("oz_order", s(d, "order_id"), true, vec![("ordered_by", "oz_citizen", s(d, "citizen_id"))]),
        "westwind.clock.tick" => None,
        t if ["ticket.", "remake.", "return.", "returns.", "support.", "compensation."].iter().any(|p| t.starts_with(&format!("westwind.{p}"))) => {
            fact("westwind_ticket", s(d, "ticket"), false, vec![])
        }
        _ if d.get("order_id").is_some() => fact("westwind_order", s(d, "order_id"), false, vec![]),
        _ => None,
    }
}

/// One shard of the store: one SQLite file.
pub struct Shard {
    conn: Connection,
    cache: HashMap<(String, String), Row>,
    dirty: BTreeSet<(String, String)>,
    edges: Vec<(String, String, String, String, String, String)>,
    answers: Vec<((String, String), Answer)>,
    pub writes: u64,
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard").finish_non_exhaustive()
    }
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS rows(kind TEXT NOT NULL, key TEXT NOT NULL, event TEXT NOT NULL, time INTEGER NOT NULL,
        body TEXT NOT NULL, UNIQUE(kind, key));
    CREATE TABLE IF NOT EXISTS edges(src TEXT NOT NULL, rel TEXT NOT NULL, dst TEXT NOT NULL,
        PRIMARY KEY(src, rel, dst)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS edges_to ON edges(dst, rel);
    CREATE TABLE IF NOT EXISTS host(action TEXT NOT NULL, event TEXT NOT NULL, answer TEXT NOT NULL,
        PRIMARY KEY(action, event)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;";

impl Shard {
    /// # Errors
    /// The file cannot be opened.
    pub fn open(path: &Path) -> Result<Shard, String> {
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        conn.busy_timeout(std::time::Duration::from_secs(30)).map_err(|e| e.to_string())?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON;").map_err(|e| e.to_string())?;
        conn.execute_batch(SCHEMA).map_err(|e| e.to_string())?;
        Ok(Shard { conn, cache: HashMap::new(), dirty: BTreeSet::new(), edges: Vec::new(), answers: Vec::new(), writes: 0 })
    }

    /// The row of `kind` keyed `key`. A row this process has written is handed back from memory,
    /// never read back from the file; a row it has not seen is looked for once.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn row(&mut self, kind: &str, key: &str) -> Result<&mut Row, String> {
        let at = (kind.to_string(), key.to_string());
        if !self.cache.contains_key(&at) {
            let body: Option<String> = self
                .conn
                .prepare_cached("SELECT body FROM rows WHERE kind = ?1 AND key = ?2")
                .map_err(|e| e.to_string())?
                .query_row(params![kind, key], |r| r.get(0))
                .optional()
                .map_err(|e| e.to_string())?;
            let row = match body {
                Some(b) => serde_json::from_str(&b).map_err(|e| e.to_string())?,
                None => Row::default(),
            };
            self.cache.insert(at.clone(), row);
        }
        self.cache.get_mut(&at).ok_or_else(|| "no row".to_string())
    }

    pub fn touched(&mut self, kind: &str, key: &str) {
        self.dirty.insert((kind.to_string(), key.to_string()));
    }

    pub fn edge(&mut self, src: (&str, &str), rel: &str, dst: (&str, &str), event: &str) {
        self.edges.push((src.0.into(), src.1.into(), rel.into(), dst.0.into(), dst.1.into(), event.into()));
    }

    pub fn keep_answer(&mut self, at: (String, String), answer: Answer) {
        self.answers.push((at, answer));
    }

    /// The host's answer for `action` on `event`, if it gave one.
    #[must_use]
    pub fn answer(&self, action: &str, event: &str) -> Option<Answer> {
        if let Some((_, a)) = self.answers.iter().find(|(k, _)| k.0 == action && k.1 == event) {
            return Some(a.clone());
        }
        let text: Option<String> = self
            .conn
            .prepare_cached("SELECT answer FROM host WHERE action = ?1 AND event = ?2")
            .ok()?
            .query_row(params![action, event], |r| r.get(0))
            .optional()
            .ok()?;
        text.and_then(|t| serde_json::from_str(&t).ok())
    }

    /// Writes every changed row, its new edges and the host's new answers in one transaction,
    /// with `meta` if given.
    ///
    /// # Errors
    /// The file cannot be written.
    pub fn commit(&mut self, meta: Option<&str>) -> Result<(), String> {
        let err = |e: rusqlite::Error| e.to_string();
        let tx = self.conn.transaction().map_err(err)?;
        {
            let mut put = tx.prepare_cached("INSERT INTO rows(kind, key, event, time, body) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(kind, key) DO UPDATE SET event = excluded.event, time = excluded.time, body = excluded.body").map_err(err)?;
            for at in &self.dirty {
                if let Some(row) = self.cache.get(at) {
                    let body = serde_json::to_string(row).map_err(|e| e.to_string())?;
                    put.execute(params![at.0, at.1, row.event, row.time, body]).map_err(err)?;
                    self.writes += 1;
                }
            }
            // An edge's relation names the kinds at both ends, so the edge holds only the keys.
            let mut edge = tx.prepare_cached("INSERT OR IGNORE INTO edges VALUES (?1, ?2, ?3)").map_err(err)?;
            for e in &self.edges {
                edge.execute(params![e.1, format!("{}.{}.{}", e.0, e.2, e.3), e.4]).map_err(err)?;
            }
            let mut host = tx.prepare_cached("INSERT OR IGNORE INTO host VALUES (?1, ?2, ?3)").map_err(err)?;
            for ((action, event), answer) in &self.answers {
                host.execute(params![action, event, serde_json::to_string(answer).map_err(|e| e.to_string())?]).map_err(err)?;
            }
            if let Some(meta) = meta {
                tx.execute("INSERT INTO meta(k, v) VALUES ('lane', ?1) ON CONFLICT(k) DO UPDATE SET v = excluded.v", [meta]).map_err(err)?;
            }
        }
        tx.commit().map_err(err)?;
        self.dirty.clear();
        self.edges.clear();
        self.answers.clear();
        Ok(())
    }

    /// The lane's state as last committed.
    #[must_use]
    pub fn meta(&self) -> Option<String> {
        self.conn.query_row("SELECT v FROM meta WHERE k = 'lane'", [], |r| r.get(0)).optional().ok().flatten()
    }

    /// Every row and edge, by key, as text.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn text(&self) -> Result<String, String> {
        let err = |e: rusqlite::Error| e.to_string();
        let mut out = String::new();
        let mut st = self.conn.prepare("SELECT kind, key, event, time, body FROM rows ORDER BY kind, key").map_err(err)?;
        let rows = st.query_map([], |r| Ok(format!("{}|{}|{}|{}|{}", r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?, r.get::<_, String>(4)?))).map_err(err)?;
        for r in rows {
            out.push_str(&r.map_err(err)?);
            out.push('\n');
        }
        let mut st = self.conn.prepare("SELECT src, rel, dst FROM edges ORDER BY 1, 2, 3").map_err(err)?;
        let rows = st.query_map([], |r| Ok(format!("{}|{}|{}", r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))).map_err(err)?;
        for r in rows {
            out.push_str(&r.map_err(err)?);
            out.push('\n');
        }
        Ok(out)
    }

    /// How many host answers it holds.
    #[must_use]
    pub fn answers_kept(&self, action: &str) -> i64 {
        self.conn.query_row("SELECT COUNT(*) FROM host WHERE action = ?1", [action], |r| r.get(0)).unwrap_or(0)
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}
