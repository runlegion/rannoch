// Shared with the worker toy; not every part is used here.
//! Westwind row for row, as the worker toy loaded it: kept to rebuild the same orders event-shaped.
//! Westwind as typed `CloudEvents`: its seed, checked against the research document's hash, read
//! into events; the toy's extension of its order history; its ontology; and the SQL answers the
//! flows are checked against.

use std::path::Path;

use rannoch_store::{Class, Declaration, Relation, ValueType};
use rusqlite::Connection;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::entrance::{Event, date_ms, mint};

/// The seed's SHA-256, from research document 01a1b37a-e007-75e1-8cbc-8f029c6d397a.
pub const SEED_SHA256: &str = "6d38971dc46a9677b46d8b6b072c88fdc6af67d381b4d731316064c166e60e71";

/// The reorder level, as a percentage of a product's starting stock. The predictions name none,
/// and Westwind has no reorder-level column; the SQL check uses the same number.
pub const REORDER_PERCENT: i64 = 20;

/// How long after it was placed, by event time, an order still without a shipped date is
/// reported. Westwind ships within 1 to 20 days, so this reports exactly the unshipped orders.
pub const UNSHIPPED_AFTER_MS: i64 = 30 * 86_400_000;

/// Each table: its event type, its class, and each column that names another row, with the
/// relation it is and the class it names.
pub const TABLES: &[(&str, &str, &str, &[(&str, &str, &str)])] = &[
    ("categories", "westwind.category.listed", "category", &[]),
    ("suppliers", "westwind.supplier.joined", "supplier", &[]),
    (
        "products",
        "westwind.product.listed",
        "product",
        &[
            ("supplier_id", "supplied_by", "supplier"),
            ("category_id", "in_category", "category"),
        ],
    ),
    ("customers", "westwind.customer.joined", "customer", &[]),
    (
        "employees",
        "westwind.employee.hired",
        "employee",
        &[("reports_to", "reports_to", "employee")],
    ),
    ("shippers", "westwind.shipper.joined", "shipper", &[]),
    (
        "orders",
        "westwind.order.placed",
        "order",
        &[
            ("customer_id", "ordered_by", "customer"),
            ("employee_id", "taken_by", "employee"),
            ("shipper_id", "shipped_by", "shipper"),
        ],
    ),
    (
        "order_details",
        "westwind.order_line.added",
        "order_line",
        &[
            ("order_id", "line_of", "order"),
            ("product_id", "line_product", "product"),
        ],
    ),
];

/// An order that ships enters a second time, as this type, carrying its whole row.
pub const SHIPPED: &str = "westwind.order.shipped";

/// Reads `seed.sql` into an in-memory database, after checking its hash.
///
/// # Errors
/// The seed's hash is not the document's, or it does not load.
pub fn load_seed(dir: &Path) -> Result<Connection, String> {
    let seed = std::fs::read(dir.join("seed.sql")).map_err(|e| format!("seed.sql: {e}"))?;
    let hash = format!("{:x}", Sha256::digest(&seed));
    if hash != SEED_SHA256 {
        return Err(format!("seed.sql hashes {hash}, not {SEED_SHA256}"));
    }
    let schema = std::fs::read_to_string(dir.join("schema.sql")).map_err(|e| e.to_string())?;
    let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
    conn.execute_batch(&schema).map_err(|e| e.to_string())?;
    conn.execute_batch(&String::from_utf8_lossy(&seed))
        .map_err(|e| e.to_string())?;
    Ok(conn)
}

pub fn rows(conn: &Connection, table: &str) -> Result<Vec<Map<String, Value>>, String> {
    let mut statement = conn
        .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
        .map_err(|e| e.to_string())?;
    let names: Vec<String> = statement.column_names().iter().map(|s| (*s).to_string()).collect();
    let rows = statement
        .query_map([], |row| {
            let mut object = Map::new();
            for (index, name) in names.iter().enumerate() {
                let value = match row.get_ref(index)? {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(i) => json!(i),
                    rusqlite::types::ValueRef::Real(f) => json!(f),
                    rusqlite::types::ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
                    rusqlite::types::ValueRef::Blob(_) => Value::Null,
                };
                object.insert(name.clone(), value);
            }
            Ok(object)
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<_, _>>().map_err(|e| e.to_string())
}

/// A Westwind row as an event: its time is inside it, and its id is minted from its source,
/// position and content, so it is the same event every time.
fn event(ty: &str, source: &str, position: u64, time: i64, row: Map<String, Value>) -> Event {
    let subject = row.get("id").and_then(Value::as_str).map(str::to_string);
    let data = Value::Object(row);
    let seed = format!("{source}\n{position}\n{ty}\n{data}");
    Event {
        id: mint(time, seed.as_bytes()),
        ty: ty.to_string(),
        source: source.to_string(),
        subject,
        time,
        data,
        causation: None,
        position,
    }
}

fn order_time(order: &Map<String, Value>) -> Result<i64, String> {
    date_ms(order.get("order_date").and_then(Value::as_str).unwrap_or(""))
}

/// Every Westwind row as an event, plus a shipped event for each order that shipped, in log
/// order: by event time, then by place in the seed. The ontology's declarations come first.
///
/// # Errors
/// The seed cannot be read.
pub fn seed_events(conn: &Connection) -> Result<Vec<Event>, String> {
    let source = "westwind/seed";
    let mut events = declarations(source);
    let mut position = 0u64;
    let mut order_times = std::collections::HashMap::new();
    let mut rest = Vec::new();
    for &(table, ty, _, _) in TABLES {
        for row in rows(conn, table)? {
            position += 1;
            let time = match table {
                "orders" => {
                    let time = order_time(&row)?;
                    let id = row.get("id").and_then(Value::as_str).unwrap_or("").to_string();
                    order_times.insert(id, time);
                    if let Some(shipped) = row.get("shipped_date").and_then(Value::as_str) {
                        rest.push(event(SHIPPED, source, u64::MAX - position, date_ms(shipped)?, row.clone()));
                    }
                    let mut placed = row;
                    placed.insert("shipped_date".into(), Value::Null);
                    rest.push(event(ty, source, position, time, placed));
                    continue;
                }
                "order_details" => {
                    let order = row.get("order_id").and_then(Value::as_str).unwrap_or("");
                    *order_times.get(order).ok_or("a line names no order")?
                }
                _ => row.get("updated_at").and_then(Value::as_i64).unwrap_or(0) * 1000,
            };
            rest.push(event(ty, source, position, time, row));
        }
    }
    rest.sort_by_key(|e| (e.time, e.position));
    events.extend(rest);
    renumber(&mut events);
    Ok(events)
}

/// Numbers the events by their place in the log, from 1.
pub fn renumber(events: &mut [Event]) {
    for (index, event) in events.iter_mut().enumerate() {
        event.position = index as u64 + 1;
    }
}

/// The Westwind ontology as declaration events: a class per table and the relations its tables
/// leave implicit, `employees.reports_to` among them. The flow's store step writes
/// `unshipped_order`.
#[must_use]
pub fn declarations(source: &str) -> Vec<Event> {
    let integer = ["in_stock", "discontinued", "quantity", "updated_at"];
    let float = ["unit_price", "freight", "discount"];
    let columns: &[(&str, &[&str])] = &[
        ("category", &["name", "description", "updated_at"]),
        ("supplier", &["company", "contact", "city", "region", "updated_at"]),
        ("product", &["name", "unit", "unit_price", "in_stock", "discontinued", "updated_at"]),
        ("customer", &["code", "company", "contact", "title", "city", "region", "updated_at"]),
        ("employee", &["code", "last_name", "first_name", "title", "city", "updated_at"]),
        ("shipper", &["company", "phone", "updated_at"]),
        (
            "order",
            &["order_date", "required_date", "shipped_date", "freight", "ship_city", "ship_region", "updated_at"],
        ),
        ("order_line", &["unit_price", "quantity", "discount", "updated_at"]),
        ("unshipped_order", &["order_date", "customer_id"]),
    ];
    let mut declared = Vec::new();
    for (class, properties) in columns {
        let properties = properties
            .iter()
            .map(|p| {
                let of = if integer.contains(p) {
                    ValueType::Integer
                } else if float.contains(p) {
                    ValueType::Float
                } else {
                    ValueType::Text
                };
                ((*p).to_string(), of)
            })
            .collect();
        declared.push(Declaration::Class(Class {
            name: (*class).to_string(),
            properties,
            axis: Vec::new(),
        }));
    }
    for &(_, _, class, links) in TABLES {
        for &(_, relation, to) in links {
            declared.push(Declaration::Relation(Relation::new(relation, class, to, relation == "line_of")));
        }
    }
    declared
        .into_iter()
        .enumerate()
        .map(|(index, declaration)| {
            let data = serde_json::to_value(&declaration).unwrap_or(Value::Null);
            let seed = format!("{source}\ndeclared\n{index}\n{data}");
            Event {
                id: mint(0, seed.as_bytes()),
                ty: "westwind.ontology.declared".into(),
                source: source.to_string(),
                subject: None,
                time: 0,
                data,
                causation: None,
                position: 0,
            }
        })
        .collect()
}

/// A small seeded generator (`SplitMix64`), so every run extends the history the same way.
#[derive(Debug)]
pub struct Rng(u64);

impl Rng {
    #[must_use]
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    /// In `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Westwind's events, then an order history extended from its customers, employees, shippers and
/// products with a fixed seed until it holds at least `lines` order lines, in log order.
///
/// # Errors
/// The seed cannot be read.
pub fn extended_events(conn: &Connection, lines: usize) -> Result<Vec<Event>, String> {
    let mut events = seed_events(conn)?;
    let customers = rows(conn, "customers")?;
    let employees = rows(conn, "employees")?;
    let shippers = rows(conn, "shippers")?;
    let products = rows(conn, "products")?;
    let id_of = |row: &Map<String, Value>, key: &str| row.get(key).cloned().unwrap_or(Value::Null);
    let source = "westwind/extend";
    let base = date_ms("2026-01-05")?;
    let mut rng = Rng::new(0x5745_5354_2026);
    let mut extra = Vec::new();
    let mut made = 0usize;
    let mut position = 0u64;
    let mut index = 0u64;
    let mint_id = |kind: &str, index: u64, time: i64| mint(time, format!("{source}\n{kind}\n{index}").as_bytes());
    while made < lines {
        index += 1;
        let day = i64::try_from(rng.below(3650)).unwrap_or(0);
        let placed = base + day * 86_400_000;
        let order_date = crate::entrance::format_time(placed)[..10].to_string();
        let customer = &customers[usize::try_from(rng.below(customers.len() as u64)).unwrap_or(0)];
        let employee = &employees[usize::try_from(rng.below(employees.len() as u64)).unwrap_or(0)];
        let shipper = &shippers[usize::try_from(rng.below(shippers.len() as u64)).unwrap_or(0)];
        let shipped = (rng.below(100) >= 12).then(|| placed + i64::try_from(1 + rng.below(20)).unwrap_or(1) * 86_400_000);
        let order_id = mint_id("order", index, placed);
        let mut row = Map::new();
        row.insert("id".into(), json!(order_id));
        row.insert("customer_id".into(), id_of(customer, "id"));
        row.insert("employee_id".into(), id_of(employee, "id"));
        row.insert("shipper_id".into(), id_of(shipper, "id"));
        row.insert("order_date".into(), json!(order_date));
        row.insert("required_date".into(), Value::Null);
        row.insert("shipped_date".into(), Value::Null);
        row.insert("freight".into(), json!(f64::from(u32::try_from(rng.below(17_800)).unwrap_or(0)) / 100.0 + 2.0));
        row.insert("ship_city".into(), id_of(customer, "city"));
        row.insert("ship_region".into(), id_of(customer, "region"));
        row.insert("updated_at".into(), json!(placed / 1000));
        position += 1;
        extra.push(event("westwind.order.placed", source, position, placed, row.clone()));
        if let Some(shipped) = shipped {
            let mut done = row.clone();
            done.insert("shipped_date".into(), json!(crate::entrance::format_time(shipped)[..10].to_string()));
            done.insert("updated_at".into(), json!(shipped / 1000));
            position += 1;
            extra.push(event(SHIPPED, source, position, shipped, done));
        }
        let count = 1 + rng.below(4);
        let mut taken = Vec::new();
        for line in 0..count {
            let product = loop {
                let pick = usize::try_from(rng.below(products.len() as u64)).unwrap_or(0);
                if !taken.contains(&pick) {
                    taken.push(pick);
                    break &products[pick];
                }
            };
            let discount = [0.0, 0.0, 0.0, 0.05, 0.1, 0.15][usize::try_from(rng.below(6)).unwrap_or(0)];
            let mut line_row = Map::new();
            line_row.insert("id".into(), json!(mint_id("line", index * 8 + line, placed)));
            line_row.insert("order_id".into(), json!(order_id));
            line_row.insert("product_id".into(), id_of(product, "id"));
            line_row.insert("unit_price".into(), id_of(product, "unit_price"));
            line_row.insert("quantity".into(), json!(1 + rng.below(40)));
            line_row.insert("discount".into(), json!(discount));
            line_row.insert("updated_at".into(), json!(placed / 1000));
            position += 1;
            extra.push(event("westwind.order_line.added", source, position, placed, line_row));
            made += 1;
        }
    }
    extra.sort_by_key(|e| (e.time, e.position));
    events.extend(extra);
    renumber(&mut events);
    Ok(events)
}

/// The order lines that take their product's stock under its reorder level, in log order: the
/// SQL answer for `westwind.stock.low` over the seed.
///
/// # Errors
/// The query fails.
pub fn sql_stock_low(conn: &Connection) -> Result<Vec<String>, String> {
    let sql = format!(
        "WITH lines AS (
           SELECT od.id, od.product_id, od.quantity, p.in_stock,
                  p.in_stock * {REORDER_PERCENT} / 100 AS level,
                  p.in_stock - COALESCE(SUM(od.quantity) OVER (
                      PARTITION BY od.product_id ORDER BY o.order_date, od.rowid
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS before,
                  o.order_date, od.rowid AS r
           FROM order_details od JOIN orders o ON o.id = od.order_id
           JOIN products p ON p.id = od.product_id)
         SELECT id FROM lines WHERE before >= level AND before - quantity < level
         ORDER BY order_date, r"
    );
    let mut statement = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let ids = statement
        .query_map([], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    ids.collect::<Result<_, _>>().map_err(|e| e.to_string())
}

/// The orders still without a shipped date `UNSHIPPED_AFTER_MS` after they were placed, for every
/// such moment at or before `clock`: the SQL answer for `westwind.order.unshipped`.
///
/// # Errors
/// The query fails.
pub fn sql_unshipped(conn: &Connection, clock: i64) -> Result<Vec<String>, String> {
    let mut statement = conn
        .prepare("SELECT id, order_date, shipped_date FROM orders ORDER BY order_date, rowid")
        .map_err(|e| e.to_string())?;
    let rows: Vec<(String, String, Option<String>)> = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .map_err(|e| e.to_string())?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    let mut ids = Vec::new();
    for (id, placed, shipped) in rows {
        let due = date_ms(&placed)? + UNSHIPPED_AFTER_MS;
        let shipped_by_due = match shipped {
            Some(shipped) => date_ms(&shipped)? <= due,
            None => false,
        };
        if due <= clock && !shipped_by_due {
            ids.push(id);
        }
    }
    Ok(ids)
}
