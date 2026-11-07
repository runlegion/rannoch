//! The store built-in: each event that says a fact becomes the `rannoch` node and link events it
//! says, in the ontology of its own domain, committed and filled at each checkpoint. Two
//! ontologies share one store, Westwind's and Oz's; every class and relation carries its domain
//! as a prefix, since one store has one namespace.

use std::path::Path;

use rannoch_store::{Class, Declaration, Graph, Relation, ValueType};
use serde_json::{Map, Value, json};

use crate::entrance::Event;
use crate::runtime::Handler;

/// The app's content-identity key.
pub const KEY: [u8; 32] = [0x43; 32];

/// The producer name the store commits its place under.
pub const PRODUCER: &str = "chain/store";

/// Every class: its name and properties with their types.
const CLASSES: &[(&str, &[(&str, ValueType)])] = &[
    ("westwind_customer", &[("name", ValueType::Text), ("city", ValueType::Text)]),
    ("westwind_supplier", &[("company", ValueType::Text), ("city", ValueType::Text)]),
    (
        "westwind_product",
        &[
            ("name", ValueType::Text),
            ("unit_price", ValueType::Float),
            ("stock", ValueType::Integer),
            ("made_to_order", ValueType::Integer),
            ("available", ValueType::Integer),
        ],
    ),
    ("westwind_employee", &[("name", ValueType::Text), ("title", ValueType::Text)]),
    ("westwind_shipper", &[("company", ValueType::Text)]),
    (
        "westwind_order",
        &[("placed_on", ValueType::Text), ("required_on", ValueType::Text), ("ship_to", ValueType::Text), ("lines", ValueType::Text)],
    ),
    ("westwind_order_status", &[("status", ValueType::Text)]),
    ("westwind_shipment", &[("tracking", ValueType::Text)]),
    ("westwind_ticket", &[("text", ValueType::Text)]),
    ("westwind_triage", &[("category", ValueType::Text)]),
    ("westwind_refund", &[("reason", ValueType::Text)]),
    ("oz_citizen", &[("name", ValueType::Text)]),
    ("oz_order", &[("item", ValueType::Text)]),
];

/// Every relation: name, from class, to class.
const RELATIONS: &[(&str, &str, &str)] = &[
    ("westwind_supplied_by", "westwind_product", "westwind_supplier"),
    ("westwind_reports_to", "westwind_employee", "westwind_employee"),
    ("westwind_ordered_by", "westwind_order", "westwind_customer"),
    ("westwind_contains", "westwind_order", "westwind_product"),
    ("westwind_shipment_of", "westwind_shipment", "westwind_order"),
    ("westwind_ticket_about", "westwind_ticket", "westwind_order"),
    ("westwind_triage_of", "westwind_triage", "westwind_ticket"),
    ("westwind_refund_for", "westwind_refund", "westwind_order"),
    ("oz_ordered_by", "oz_order", "oz_citizen"),
];

/// The order events that move an order's status on.
const STATUSES: &[&str] = &[
    "westwind.order.accepted",
    "westwind.order.abandoned",
    "westwind.order.cancelled",
    "westwind.order.packed",
    "westwind.order.shipped",
    "westwind.order.unshipped",
    "westwind.order.delivered",
    "westwind.order.lost",
    "westwind.order.spoiled",
    "westwind.order.closed",
];

/// Both ontologies' declarations.
#[must_use]
pub fn declarations() -> Vec<Declaration> {
    let mut all: Vec<Declaration> = CLASSES
        .iter()
        .map(|(name, properties)| {
            Declaration::Class(Class {
                name: (*name).to_string(),
                properties: properties.iter().map(|(p, t)| ((*p).to_string(), *t)).collect(),
                axis: Vec::new(),
            })
        })
        .collect();
    all.extend(RELATIONS.iter().map(|(n, f, t)| Declaration::Relation(Relation::new(*n, *f, *t, false))));
    all
}

/// The types the store built-in takes.
#[must_use]
pub fn patterns() -> Vec<String> {
    let mut types: Vec<String> = [
        "westwind.customer.joined",
        "westwind.supplier.joined",
        "westwind.product.listed",
        "westwind.employee.hired",
        "westwind.shipper.joined",
        "westwind.order.placed",
        "westwind.ticket.opened",
        "westwind.ticket.triaged",
        "westwind.refund.issued",
        "oz.citizen.joined",
        "oz.order.placed",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    types.extend(STATUSES.iter().map(|s| (*s).to_string()));
    types.sort();
    types.dedup();
    types
}

fn text(data: &Value, name: &str) -> Option<String> {
    data.get(name).and_then(Value::as_str).map(str::to_string)
}

struct Facts<'a> {
    event: &'a Event,
    bodies: Vec<String>,
}

impl Facts<'_> {
    fn node(&mut self, class: &str, key: &str, properties: Map<String, Value>) {
        let fact = self.bodies.len();
        self.bodies.push(
            json!({
                "specversion": "1.0",
                "id": format!("{}:{fact}", self.event.id),
                "source": self.event.source,
                "type": format!("rannoch.node.{class}"),
                "subject": key,
                "data": {"properties": properties, "valid": "unknown"},
            })
            .to_string(),
        );
    }

    fn link(&mut self, relation: &str, from: (&str, &str), to: (&str, &str)) {
        let fact = self.bodies.len();
        self.bodies.push(
            json!({
                "specversion": "1.0",
                "id": format!("{}:{fact}", self.event.id),
                "source": self.event.source,
                "type": format!("rannoch.link.{relation}"),
                "data": {"from": {"class": from.0, "key": from.1}, "to": {"class": to.0, "key": to.1}, "valid": "unknown"},
            })
            .to_string(),
        );
    }
}

fn props(data: &Value, names: &[&str]) -> Map<String, Value> {
    let mut out = Map::new();
    for name in names {
        match data.get(*name) {
            Some(Value::String(s)) => {
                out.insert((*name).to_string(), Value::String(s.clone()));
            }
            Some(Value::Number(n)) if n.is_i64() => {
                out.insert((*name).to_string(), json!({"integer": n.as_i64()}));
            }
            Some(Value::Number(n)) => {
                out.insert((*name).to_string(), json!({"float": n.as_f64()}));
            }
            Some(Value::Bool(b)) => {
                out.insert((*name).to_string(), json!({"integer": i64::from(*b)}));
            }
            _ => {}
        }
    }
    out
}

/// The `rannoch` events one event says, in its own domain's ontology.
#[must_use]
pub fn translate(event: &Event) -> Vec<String> {
    let d = &event.data;
    let mut facts = Facts { event, bodies: Vec::new() };
    let id = |name: &str| text(d, name).unwrap_or_default();
    match event.ty.as_str() {
        "westwind.customer.joined" => facts.node("westwind_customer", &id("id"), props(d, &["name", "city"])),
        "westwind.supplier.joined" => facts.node("westwind_supplier", &id("id"), props(d, &["company", "city"])),
        "westwind.shipper.joined" => facts.node("westwind_shipper", &id("id"), props(d, &["company"])),
        "westwind.product.listed" => {
            facts.node("westwind_product", &id("id"), props(d, &["name", "unit_price", "stock", "made_to_order", "available"]));
            facts.link("westwind_supplied_by", ("westwind_product", &id("id")), ("westwind_supplier", &id("supplier_id")));
        }
        "westwind.employee.hired" => {
            facts.node("westwind_employee", &id("id"), props(d, &["name", "title"]));
            if let Some(boss) = text(d, "reports_to") {
                facts.link("westwind_reports_to", ("westwind_employee", &id("id")), ("westwind_employee", &boss));
            }
        }
        "westwind.order.placed" => {
            let order = id("order_id");
            let mut p = props(d, &["placed_on", "required_on", "ship_to"]);
            p.insert("lines".into(), Value::String(d.get("lines").map(Value::to_string).unwrap_or_default()));
            facts.node("westwind_order", &order, p);
            facts.link("westwind_ordered_by", ("westwind_order", &order), ("westwind_customer", &id("customer_id")));
            let mut products: Vec<String> = d
                .get("lines")
                .and_then(Value::as_array)
                .map(|lines| lines.iter().filter_map(|l| text(l, "product_id")).collect())
                .unwrap_or_default();
            products.sort();
            products.dedup();
            for product in products {
                facts.link("westwind_contains", ("westwind_order", &order), ("westwind_product", &product));
            }
        }
        "westwind.ticket.opened" => {
            facts.node("westwind_ticket", &event.id, props(d, &["text"]));
            facts.link("westwind_ticket_about", ("westwind_ticket", &event.id), ("westwind_order", &id("order_id")));
        }
        "westwind.ticket.triaged" => {
            facts.node("westwind_triage", &id("ticket"), props(d, &["category"]));
            facts.link("westwind_triage_of", ("westwind_triage", &id("ticket")), ("westwind_ticket", &id("ticket")));
        }
        "westwind.refund.issued" => {
            let mut p = Map::new();
            p.insert("reason".into(), Value::String(event.causation.clone().unwrap_or_default()));
            facts.node("westwind_refund", &event.id, p);
            facts.link("westwind_refund_for", ("westwind_refund", &event.id), ("westwind_order", &id("order_id")));
        }
        "oz.citizen.joined" => facts.node("oz_citizen", &id("id"), props(d, &["name"])),
        "oz.order.placed" => {
            facts.node("oz_order", &id("order_id"), props(d, &["item"]));
            facts.link("oz_ordered_by", ("oz_order", &id("order_id")), ("oz_citizen", &id("citizen_id")));
        }
        ty if STATUSES.contains(&ty) => {
            let mut p = Map::new();
            p.insert("status".into(), Value::String(ty.trim_start_matches("westwind.order.").to_string()));
            facts.node("westwind_order_status", &id("order_id"), p);
            if ty == "westwind.order.shipped" {
                facts.node("westwind_shipment", &id("shipment_id"), props(d, &["tracking"]));
                facts.link("westwind_shipment_of", ("westwind_shipment", &id("shipment_id")), ("westwind_order", &id("order_id")));
            }
        }
        _ => {}
    }
    facts.bodies
}

/// Buffers `rannoch` bodies and commits them, then fills, at each checkpoint.
#[derive(Debug)]
pub struct Writer {
    pub graph: Graph,
    bodies: Vec<String>,
    pub committed: u64,
    seq: u64,
}

impl Writer {
    /// Opens the store and declares both ontologies; an identical declaration writes nothing.
    ///
    /// # Errors
    /// The store cannot be opened, or a declaration is refused.
    pub fn open(path: &Path) -> Result<Writer, String> {
        let mut graph = rannoch_store::open(path, &KEY).map_err(|e| e.to_string())?;
        for (index, declaration) in declarations().iter().enumerate() {
            let name = match declaration {
                Declaration::Class(c) => c.name.clone(),
                Declaration::Relation(r) => r.name.clone(),
                Declaration::Property { class, property, .. } => format!("{class}.{property}"),
            };
            let body = json!({
                "specversion": "1.0",
                "id": format!("{}:0", crate::entrance::mint(0, format!("declared\n{index}\n{name}").as_bytes())),
                "source": "chain/declare",
                "type": format!("rannoch.declared.{name}"),
            })
            .to_string();
            graph.declare(declaration, &body).map_err(|e| e.to_string())?;
        }
        Ok(Writer { graph, bodies: Vec::new(), committed: 0, seq: 0 })
    }

    pub fn add(&mut self, bodies: Vec<String>) {
        self.bodies.extend(bodies);
    }

    /// Commits what is buffered, then fills.
    ///
    /// # Errors
    /// An event would not land, or the store cannot be written.
    pub fn checkpoint(&mut self) -> Result<(), String> {
        if self.bodies.is_empty() {
            return Ok(());
        }
        self.seq += 1;
        let refs: Vec<&str> = self.bodies.iter().map(String::as_str).collect();
        let position = format!("{:012}", self.seq);
        for result in self
            .graph
            .commit(&refs, &[(refs.len() - 1, PRODUCER, &position)])
            .map_err(|e| e.to_string())?
        {
            result.map_err(|e| e.to_string())?;
        }
        self.committed += refs.len() as u64;
        self.bodies.clear();
        self.graph.fill().map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// The store built-in, as a handler.
#[derive(Debug)]
pub struct Store {
    pub writer: Writer,
    last: Option<u64>,
    durable: Option<u64>,
}

impl Store {
    /// # Errors
    /// The store cannot be opened.
    pub fn open(path: &Path) -> Result<Store, String> {
        Ok(Store { writer: Writer::open(path)?, last: None, durable: None })
    }
}

impl Handler for Store {
    fn handle(&mut self, event: &Event, _: &mut Vec<Event>) -> Result<(), String> {
        self.writer.add(translate(event));
        self.last = Some(event.position);
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<Option<u64>, String> {
        self.writer.checkpoint()?;
        self.durable = self.last;
        Ok(self.durable)
    }
    fn place(&self) -> Option<u64> {
        self.durable
    }
}
