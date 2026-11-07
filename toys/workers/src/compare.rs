//! Graph against nodes-only against relational: the same Westwind events loaded three ways, and
//! the same six scenarios answered by each.
//!
//! - Graph: the worker toy's store, each foreign key an edge (`rannoch.link.<relation>`).
//! - Nodes only: the same rows into rannoch as nodes that keep their foreign keys as plain
//!   properties, with no edges.
//! - Relational: the same rows into SQLite tables shaped like `westwind/schema.sql`, an index on
//!   every foreign-key column, written in batched transactions. No rannoch.
//!
//! Every rannoch read is SQL over the store's own tables (`class:<name>`, `relation:<name>`),
//! since rannoch's API reads one node or one link at a time and has no traversal.

use std::path::{Path, PathBuf};
use std::time::Instant;

use rannoch_store::Declaration;
use rusqlite::{Connection, params_from_iter};
use serde_json::{Map, Value};

use crate::entrance::Event;
use crate::store::{Declare, PRODUCER, Writer, node_body, translate};
use crate::westwind::{SHIPPED, TABLES, declarations};

/// One of the three ways.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Graph,
    Nodes,
    Relational,
}

impl Version {
    pub const ALL: [Version; 3] = [Version::Graph, Version::Nodes, Version::Relational];

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Version::Graph => "graph",
            Version::Nodes => "nodes-only",
            Version::Relational => "relational",
        }
    }
}

const EVERY: usize = 4096;

fn table_of(ty: &str) -> Option<&'static (&'static str, &'static str, &'static str, &'static [(&'static str, &'static str, &'static str)])> {
    let ty = if ty == SHIPPED { "westwind.order.placed" } else { ty };
    TABLES.iter().find(|t| t.1 == ty)
}

/// Nodes only: the row as one node, its foreign keys kept as text properties.
fn translate_nodes(event: &Event) -> Vec<String> {
    let Some(&(_, _, class, _)) = table_of(&event.ty) else {
        return Vec::new();
    };
    let Value::Object(row) = &event.data else {
        return Vec::new();
    };
    let Some(key) = row.get("id").and_then(Value::as_str) else {
        return Vec::new();
    };
    // `translate` types every property; with no links declared, its node body is the row less
    // its foreign keys, so the foreign keys are added back as text.
    let mut bodies = translate(&Event { ty: event.ty.clone(), ..event.clone() });
    bodies.truncate(1);
    let Some(body) = bodies.pop() else {
        return Vec::new();
    };
    let Ok(mut node) = serde_json::from_str::<Value>(&body) else {
        return Vec::new();
    };
    if let Some(Value::Object(properties)) = node.pointer_mut("/data/properties")
        && let Some(&(_, _, _, links)) = table_of(&event.ty)
    {
        for &(column, _, _) in links {
            if let Some(Value::String(target)) = row.get(column) {
                properties.insert(column.to_string(), Value::String(target.clone()));
            }
        }
        let properties = properties.clone();
        return vec![node_body(&event.id, 0, &event.source, class, key, &properties)];
    }
    Vec::new()
}

/// The nodes-only ontology: every class with its foreign keys as text properties, no relations.
fn nodes_declarations(source: &str) -> Vec<Event> {
    declarations(source)
        .into_iter()
        .filter_map(|mut event| {
            let mut declaration: Declaration = serde_json::from_value(event.data.clone()).ok()?;
            match &mut declaration {
                Declaration::Class(class) => {
                    if let Some(&(_, _, _, links)) = TABLES.iter().find(|t| t.2 == class.name) {
                        for &(column, _, _) in links {
                            class.properties.insert(column.to_string(), rannoch_store::ValueType::Text);
                        }
                    }
                }
                _ => return None,
            }
            event.data = serde_json::to_value(&declaration).ok()?;
            event.id = crate::entrance::mint(0, format!("nodes\n{}", event.id).as_bytes());
            Some(event)
        })
        .collect()
}

/// The path each version's files live at.
#[must_use]
pub fn path(dir: &Path, version: Version) -> PathBuf {
    dir.join(format!("{}.db", version.name()))
}

fn remove(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// Loads `events` (Westwind rows, without the worker toy's declarations) into `version` at
/// `path`, in batches of [`EVERY`] events, each batch one durable commit. Returns the seconds the
/// load took, translation included.
///
/// # Errors
/// The file cannot be written.
pub fn load(version: Version, path: &Path, events: &[Event]) -> Result<f64, String> {
    remove(path);
    let rows: Vec<&Event> = events.iter().filter(|e| table_of(&e.ty).is_some()).collect();
    match version {
        Version::Graph | Version::Nodes => {
            let declared = if version == Version::Graph { declarations("compare") } else { nodes_declarations("compare") };
            {
                let mut declare = Declare::open(path)?;
                for event in &declared {
                    crate::runtime::Handler::handle(&mut declare, event, &mut Vec::new())?;
                }
            }
            let mut writer = Writer::open(path, PRODUCER)?;
            let started = Instant::now();
            for chunk in rows.chunks(EVERY) {
                for event in chunk {
                    let bodies = if version == Version::Graph { translate(event) } else { translate_nodes(event) };
                    writer.add(bodies, event.position);
                }
                writer.checkpoint()?;
            }
            let seconds = started.elapsed().as_secs_f64();
            let stopped = writer.graph.stopped().map_err(|e| e.to_string())?;
            if !stopped.is_empty() {
                return Err(format!("{}: fill stopped on {} events: {:?}", version.name(), stopped.len(), stopped.first()));
            }
            Ok(seconds)
        }
        Version::Relational => load_relational(path, &rows),
    }
}

fn load_relational(path: &Path, rows: &[&Event]) -> Result<f64, String> {
    let err = |e: rusqlite::Error| e.to_string();
    let mut conn = Connection::open(path).map_err(err)?;
    // The same durability rannoch's store asks for.
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON;").map_err(err)?;
    conn.execute_batch(include_str!("../westwind/schema.sql")).map_err(err)?;
    conn.execute_batch(
        "CREATE INDEX products_supplier ON products(supplier_id);
         CREATE INDEX products_category ON products(category_id);
         CREATE INDEX employees_reports_to ON employees(reports_to);
         CREATE INDEX orders_customer ON orders(customer_id);
         CREATE INDEX orders_employee ON orders(employee_id);
         CREATE INDEX orders_shipper ON orders(shipper_id);
         CREATE INDEX order_details_order ON order_details(order_id);
         CREATE INDEX order_details_product ON order_details(product_id);",
    )
    .map_err(err)?;
    let mut columns: std::collections::HashMap<&str, Vec<String>> = std::collections::HashMap::new();
    for &(table, _, _, _) in TABLES {
        let mut statement = conn.prepare(&format!("SELECT name FROM pragma_table_info('{table}')")).map_err(err)?;
        let names = statement.query_map([], |r| r.get(0)).map_err(err)?.collect::<Result<Vec<String>, _>>().map_err(err)?;
        columns.insert(table, names);
    }
    let started = Instant::now();
    for chunk in rows.chunks(EVERY) {
        let tx = conn.transaction().map_err(err)?;
        for event in chunk {
            let Some(&(table, _, _, _)) = table_of(&event.ty) else { continue };
            let Value::Object(row) = &event.data else { continue };
            if event.ty == SHIPPED {
                tx.prepare_cached("UPDATE orders SET shipped_date = ?1, updated_at = ?2 WHERE id = ?3")
                    .map_err(err)?
                    .execute(rusqlite::params![
                        row.get("shipped_date").and_then(Value::as_str),
                        row.get("updated_at").and_then(Value::as_i64),
                        row.get("id").and_then(Value::as_str),
                    ])
                    .map_err(err)?;
                continue;
            }
            let names = &columns[table];
            let sql = format!(
                "INSERT INTO {table} ({}) VALUES ({})",
                names.join(", "),
                (1..=names.len()).map(|i| format!("?{i}")).collect::<Vec<_>>().join(", ")
            );
            let values = names.iter().map(|n| sql_value(row, n));
            tx.prepare_cached(&sql).map_err(err)?.execute(params_from_iter(values)).map_err(err)?;
        }
        tx.commit().map_err(err)?;
    }
    Ok(started.elapsed().as_secs_f64())
}

fn sql_value(row: &Map<String, Value>, name: &str) -> rusqlite::types::Value {
    use rusqlite::types::Value as V;
    match row.get(name) {
        Some(Value::String(s)) => V::Text(s.clone()),
        Some(Value::Number(n)) => n.as_i64().map_or_else(|| V::Real(n.as_f64().unwrap_or(0.0)), V::Integer),
        _ => V::Null,
    }
}

/// The scenarios' fixed inputs, taken from the seed.
#[derive(Debug, Clone)]
pub struct Inputs {
    pub order: String,
    pub customer: String,
    pub employee: String,
    pub manager: String,
    pub supplier: String,
}

/// One scenario's SQL for one version, and the inputs it binds.
fn scenario(version: Version, name: &str) -> &'static str {
    use Version::{Graph as G, Nodes as N, Relational as R};
    match (name, version) {
        ("A", R) => "WITH l AS (SELECT d.id, d.quantity AS q, p.in_stock * 20 / 100 AS level,
               p.in_stock - COALESCE(SUM(d.quantity) OVER (PARTITION BY d.product_id ORDER BY o.order_date, d.rowid
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS before, o.order_date AS od, d.rowid AS r
             FROM order_details d JOIN orders o ON o.id = d.order_id JOIN products p ON p.id = d.product_id)
             SELECT id FROM l WHERE before >= level AND before - q < level ORDER BY od, r",
        ("A", N) => r#"WITH l AS (SELECT d.key AS id, d."p:quantity" AS q, p."p:in_stock" * 20 / 100 AS level,
               p."p:in_stock" - COALESCE(SUM(d."p:quantity") OVER (PARTITION BY d."p:product_id" ORDER BY o."p:order_date", d.id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS before, o."p:order_date" AS od, d.id AS r
             FROM "class:order_line" d JOIN "class:order" o ON o.key = d."p:order_id" JOIN "class:product" p ON p.key = d."p:product_id")
             SELECT id FROM l WHERE before >= level AND before - q < level ORDER BY od, r"#,
        ("A", G) => r#"WITH l AS (SELECT d.key AS id, d."p:quantity" AS q, p."p:in_stock" * 20 / 100 AS level,
               p."p:in_stock" - COALESCE(SUM(d."p:quantity") OVER (PARTITION BY p.id ORDER BY o."p:order_date", d.id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS before, o."p:order_date" AS od, d.id AS r
             FROM "class:order_line" d JOIN "relation:line_of" lo ON lo.src = d.id JOIN "class:order" o ON o.id = lo.dst
             JOIN "relation:line_product" lp ON lp.src = d.id JOIN "class:product" p ON p.id = lp.dst)
             SELECT id FROM l WHERE before >= level AND before - q < level ORDER BY od, r"#,
        ("B", R) => "WITH c AS (SELECT MAX(MAX(order_date), MAX(COALESCE(shipped_date, ''))) AS clock FROM orders)
             SELECT id FROM orders, c WHERE julianday(order_date) + 30 <= julianday(clock)
               AND (shipped_date IS NULL OR julianday(shipped_date) > julianday(order_date) + 30) ORDER BY id",
        ("B", N | G) => r#"WITH c AS (SELECT MAX(MAX("p:order_date"), MAX(COALESCE("p:shipped_date", ''))) AS clock FROM "class:order")
             SELECT key FROM "class:order", c WHERE julianday("p:order_date") + 30 <= julianday(clock)
               AND ("p:shipped_date" IS NULL OR julianday("p:shipped_date") > julianday("p:order_date") + 30) ORDER BY key"#,
        ("C", R) => "SELECT d.id, p.name FROM order_details d JOIN products p ON p.id = d.product_id WHERE d.order_id = ?1 ORDER BY 1",
        ("C", N) => r#"SELECT l.key, p."p:name" FROM "class:order_line" l JOIN "class:product" p ON p.key = l."p:product_id"
             WHERE l."p:order_id" = ?1 ORDER BY 1"#,
        ("C", G) => r#"SELECT l.key, p."p:name" FROM "class:order" o JOIN "relation:line_of" lo ON lo.dst = o.id
             JOIN "class:order_line" l ON l.id = lo.src JOIN "relation:line_product" lp ON lp.src = l.id
             JOIN "class:product" p ON p.id = lp.dst WHERE o.key = ?1 ORDER BY 1"#,
        ("D", R) => "SELECT DISTINCT d.product_id FROM orders o JOIN order_details d ON d.order_id = o.id WHERE o.customer_id = ?1 ORDER BY 1",
        ("D", N) => r#"SELECT DISTINCT l."p:product_id" FROM "class:order" o JOIN "class:order_line" l ON l."p:order_id" = o.key
             WHERE o."p:customer_id" = ?1 ORDER BY 1"#,
        ("D", G) => r#"SELECT DISTINCT p.key FROM "class:customer" c JOIN "relation:ordered_by" ob ON ob.dst = c.id
             JOIN "relation:line_of" lo ON lo.dst = ob.src JOIN "relation:line_product" lp ON lp.src = lo.src
             JOIN "class:product" p ON p.id = lp.dst WHERE c.key = ?1 ORDER BY 1"#,
        ("E-up", R) => "WITH RECURSIVE up(id) AS (SELECT reports_to FROM employees WHERE id = ?1
               UNION SELECT e.reports_to FROM employees e JOIN up ON e.id = up.id)
             SELECT id FROM up WHERE id IS NOT NULL ORDER BY 1",
        ("E-up", N) => r#"WITH RECURSIVE up(id) AS (SELECT "p:reports_to" FROM "class:employee" WHERE key = ?1
               UNION SELECT e."p:reports_to" FROM "class:employee" e JOIN up ON e.key = up.id)
             SELECT id FROM up WHERE id IS NOT NULL ORDER BY 1"#,
        ("E-up", G) => r#"WITH RECURSIVE up(i) AS (SELECT r.dst FROM "relation:reports_to" r JOIN "class:employee" e ON e.id = r.src WHERE e.key = ?1
               UNION SELECT r.dst FROM "relation:reports_to" r JOIN up ON r.src = up.i)
             SELECT e.key FROM up JOIN "class:employee" e ON e.id = up.i ORDER BY 1"#,
        ("E-down", R) => "WITH RECURSIVE down(id) AS (SELECT id FROM employees WHERE reports_to = ?1
               UNION SELECT e.id FROM employees e JOIN down ON e.reports_to = down.id)
             SELECT id FROM down ORDER BY 1",
        ("E-down", N) => r#"WITH RECURSIVE down(id) AS (SELECT key FROM "class:employee" WHERE "p:reports_to" = ?1
               UNION SELECT e.key FROM "class:employee" e JOIN down ON e."p:reports_to" = down.id)
             SELECT id FROM down ORDER BY 1"#,
        ("E-down", G) => r#"WITH RECURSIVE down(i) AS (SELECT r.src FROM "relation:reports_to" r JOIN "class:employee" e ON e.id = r.dst WHERE e.key = ?1
               UNION SELECT r.src FROM "relation:reports_to" r JOIN down ON r.dst = down.i)
             SELECT e.key FROM down JOIN "class:employee" e ON e.id = down.i ORDER BY 1"#,
        ("F", R) => "SELECT DISTINCT o.customer_id FROM products p JOIN order_details d ON d.product_id = p.id
             JOIN orders o ON o.id = d.order_id WHERE p.supplier_id = ?1 ORDER BY 1",
        ("F", N) => r#"SELECT DISTINCT o."p:customer_id" FROM "class:product" p JOIN "class:order_line" l ON l."p:product_id" = p.key
             JOIN "class:order" o ON o.key = l."p:order_id" WHERE p."p:supplier_id" = ?1 ORDER BY 1"#,
        ("F", G) => r#"SELECT DISTINCT c.key FROM "class:supplier" s JOIN "relation:supplied_by" sb ON sb.dst = s.id
             JOIN "relation:line_product" lp ON lp.dst = sb.src JOIN "relation:line_of" lo ON lo.src = lp.src
             JOIN "relation:ordered_by" ob ON ob.src = lo.dst JOIN "class:customer" c ON c.id = ob.dst WHERE s.key = ?1 ORDER BY 1"#,
        _ => "",
    }
}

pub const SCENARIOS: [&str; 7] = ["A", "B", "C", "D", "E-up", "E-down", "F"];

fn input<'a>(name: &str, inputs: &'a Inputs) -> Option<&'a str> {
    match name {
        "C" => Some(&inputs.order),
        "D" => Some(&inputs.customer),
        "E-up" => Some(&inputs.employee),
        "E-down" => Some(&inputs.manager),
        "F" => Some(&inputs.supplier),
        _ => None,
    }
}

/// Answers one scenario on one version's file: its rows as text, and the best of `repeats`
/// timings in milliseconds.
///
/// # Errors
/// The query fails.
pub fn answer(conn: &Connection, version: Version, name: &str, inputs: &Inputs, repeats: usize) -> Result<(Vec<String>, f64), String> {
    let sql = scenario(version, name);
    let mut statement = conn.prepare(sql).map_err(|e| format!("{} {name}: {e}", version.name()))?;
    let mut best = f64::MAX;
    let mut rows = Vec::new();
    for _ in 0..repeats.max(1) {
        let started = Instant::now();
        let bound: Vec<&str> = input(name, inputs).into_iter().collect();
        rows = statement
            .query_map(params_from_iter(bound), |row| {
                let count = row.as_ref().column_count();
                let mut parts = Vec::with_capacity(count);
                for i in 0..count {
                    let value: rusqlite::types::Value = row.get(i)?;
                    parts.push(format!("{value:?}"));
                }
                Ok(parts.join("|"))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        best = best.min(started.elapsed().as_secs_f64() * 1e3);
    }
    Ok((rows, best))
}

/// The scenarios' inputs: the order with the most lines (then the smallest id), customer WINTF,
/// employee SALLO, manager DAVOS and the Qohor Smiths' Guild, read from the seed.
///
/// # Errors
/// The seed cannot be read.
pub fn inputs(seed: &Connection, relational: &Connection) -> Result<Inputs, String> {
    let one = |conn: &Connection, sql: &str| -> Result<String, String> { conn.query_row(sql, [], |r| r.get(0)).map_err(|e| e.to_string()) };
    Ok(Inputs {
        order: one(relational, "SELECT order_id FROM order_details GROUP BY order_id ORDER BY COUNT(*) DESC, order_id LIMIT 1")?,
        customer: one(seed, "SELECT id FROM customers WHERE code = 'WINTF'")?,
        employee: one(seed, "SELECT id FROM employees WHERE code = 'SALLO'")?,
        manager: one(seed, "SELECT id FROM employees WHERE code = 'DAVOS'")?,
        supplier: one(seed, "SELECT id FROM suppliers WHERE company = 'Qohor Smiths'' Guild'")?,
    })
}

/// Bytes on disk once the write-ahead log is folded back into the file.
///
/// # Errors
/// The file cannot be opened.
pub fn size(path: &Path) -> Result<u64, String> {
    let conn = Connection::open(path).map_err(|e| e.to_string())?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").map_err(|e| e.to_string())?;
    drop(conn);
    let mut total = 0;
    for suffix in ["", "-wal", "-shm"] {
        total += std::fs::metadata(format!("{}{suffix}", path.display())).map(|m| m.len()).unwrap_or(0);
    }
    Ok(total)
}
