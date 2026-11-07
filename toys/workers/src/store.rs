//! The built-ins: store and declare. Store is v0.5.0's fill applied to routed events: each
//! Westwind event becomes the `rannoch.node` and `rannoch.link` events it says, committed to the
//! log with the router's position and filled. Declare takes the ontology's declarations.

use std::path::{Path, PathBuf};

use rannoch_store::{Declaration, Graph, NodeRef, View};
use serde_json::{Map, Value, json};

use crate::entrance::Event;
use crate::runtime::Handler;
use crate::westwind::{SHIPPED, TABLES};

/// The app's content-identity key.
pub const KEY: [u8; 32] = [0x57; 32];

/// The producer name store commits its router position under.
pub const PRODUCER: &str = "router/store";

const INTEGER: [&str; 4] = ["in_stock", "discontinued", "quantity", "updated_at"];
const FLOAT: [&str; 3] = ["unit_price", "freight", "discount"];

fn typed(name: &str, value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::Number(n) if INTEGER.contains(&name) => n.as_i64().map(|i| json!({"integer": i})),
        Value::Number(n) if FLOAT.contains(&name) => n.as_f64().map(|f| json!({"float": f})),
        Value::Number(n) => Some(Value::String(n.to_string())),
        other => Some(other.clone()),
    }
}

/// A node body: `rannoch.node.<class>` keyed `key`.
#[must_use]
pub fn node_body(id: &str, fact: usize, source: &str, class: &str, key: &str, properties: &Map<String, Value>) -> String {
    json!({
        "specversion": "1.0",
        "id": format!("{id}:{fact}"),
        "source": source,
        "type": format!("rannoch.node.{class}"),
        "subject": key,
        "data": {"properties": properties, "valid": "unknown"},
    })
    .to_string()
}

/// The `rannoch` events one Westwind event says: its row as a node, and a link for each column
/// that names another row. A shipped order says only its node, now with its shipped date.
#[must_use]
pub fn translate(event: &Event) -> Vec<String> {
    let ty = if event.ty == SHIPPED { "westwind.order.placed" } else { event.ty.as_str() };
    let Some(&(_, _, class, links)) = TABLES.iter().find(|t| t.1 == ty) else {
        return Vec::new();
    };
    let Value::Object(row) = &event.data else {
        return Vec::new();
    };
    let Some(key) = row.get("id").and_then(Value::as_str) else {
        return Vec::new();
    };
    let mut properties = Map::new();
    for (name, value) in row {
        if name == "id" || links.iter().any(|l| l.0 == name) {
            continue;
        }
        if let Some(value) = typed(name, value) {
            properties.insert(name.clone(), value);
        }
    }
    let mut bodies = vec![node_body(&event.id, 0, &event.source, class, key, &properties)];
    if event.ty == SHIPPED {
        return bodies;
    }
    for (fact, &(column, relation, to)) in links.iter().enumerate() {
        let Some(target) = row.get(column).and_then(Value::as_str) else {
            continue;
        };
        bodies.push(
            json!({
                "specversion": "1.0",
                "id": format!("{}:{}", event.id, fact + 1),
                "source": event.source,
                "type": format!("rannoch.link.{relation}"),
                "data": {
                    "from": {"class": class, "key": key},
                    "to": {"class": to, "key": target},
                    "valid": "unknown",
                },
            })
            .to_string(),
        );
    }
    bodies
}

/// The position `producer` last committed, from the store's own positions table.
///
/// # Errors
/// The store cannot be read.
pub fn committed_position(path: &Path, producer: &str) -> Result<Option<u64>, String> {
    let conn = rusqlite::Connection::open(path).map_err(|e| e.to_string())?;
    let found: Option<String> = conn
        .query_row("SELECT position FROM _positions WHERE producer = ?1", [producer], |row| row.get(0))
        .ok();
    Ok(found.and_then(|p| p.parse().ok()))
}

/// Buffers `rannoch` bodies and commits them, with the position they took the router to, then
/// fills: fill applied to what was routed since the last checkpoint.
#[derive(Debug)]
pub struct Writer {
    pub graph: Graph,
    producer: String,
    bodies: Vec<String>,
    position: Option<u64>,
    durable: Option<u64>,
    pub committed: u64,
}

impl Writer {
    /// # Errors
    /// The store cannot be opened.
    pub fn open(path: &Path, producer: &str) -> Result<Writer, String> {
        let durable = if path.exists() { committed_position(path, producer)? } else { None };
        Ok(Writer {
            graph: rannoch_store::open(path, &KEY).map_err(|e| e.to_string())?,
            producer: producer.to_string(),
            bodies: Vec::new(),
            position: durable,
            durable,
            committed: 0,
        })
    }

    pub fn add(&mut self, bodies: Vec<String>, position: u64) {
        self.bodies.extend(bodies);
        self.position = Some(position);
    }

    /// Commits what is buffered with its position, then fills.
    ///
    /// # Errors
    /// An event would not land, or the store cannot be written.
    pub fn checkpoint(&mut self) -> Result<Option<u64>, String> {
        if self.position == self.durable {
            return Ok(self.durable);
        }
        let position = self.position.map(|p| format!("{p:012}")).unwrap_or_default();
        let refs: Vec<&str> = self.bodies.iter().map(String::as_str).collect();
        // A batch with no bodies still moves the position: an empty commit records nothing.
        let landed = if refs.is_empty() {
            Vec::new()
        } else {
            self.graph
                .commit(&refs, &[(refs.len() - 1, self.producer.as_str(), &position)])
                .map_err(|e| e.to_string())?
        };
        for result in landed {
            result.map_err(|e| e.to_string())?;
        }
        self.committed += refs.len() as u64;
        self.bodies.clear();
        self.graph.fill().map_err(|e| e.to_string())?;
        self.durable = self.position;
        Ok(self.durable)
    }

    #[must_use]
    pub fn durable(&self) -> Option<u64> {
        self.durable
    }
}

/// The built-in store, as a handler.
#[derive(Debug)]
pub struct Store {
    pub writer: Writer,
}

impl Store {
    /// The types it registers: every Westwind row type, and shipped.
    #[must_use]
    pub fn patterns() -> Vec<String> {
        TABLES.iter().map(|t| t.1.to_string()).chain([SHIPPED.to_string()]).collect()
    }

    /// # Errors
    /// The store cannot be opened.
    pub fn open(path: &Path) -> Result<Store, String> {
        Ok(Store { writer: Writer::open(path, PRODUCER)? })
    }
}

impl Handler for Store {
    fn handle(&mut self, event: &Event, _: &mut Vec<Event>) -> Result<(), String> {
        self.writer.add(translate(event), event.position);
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<Option<u64>, String> {
        self.writer.checkpoint()
    }
    fn place(&self) -> Option<u64> {
        self.writer.durable()
    }
}

/// The built-in declare: each `westwind.ontology.declared` event's data is a declaration. An
/// identical declaration writes nothing, so handing one again changes nothing.
#[derive(Debug)]
pub struct Declare {
    graph: Graph,
}

impl Declare {
    /// # Errors
    /// The store cannot be opened.
    pub fn open(path: &Path) -> Result<Declare, String> {
        Ok(Declare { graph: rannoch_store::open(path, &KEY).map_err(|e| e.to_string())? })
    }
}

impl Handler for Declare {
    fn handle(&mut self, event: &Event, _: &mut Vec<Event>) -> Result<(), String> {
        let declaration: Declaration = serde_json::from_value(event.data.clone()).map_err(|e| e.to_string())?;
        let name = match &declaration {
            Declaration::Class(c) => c.name.clone(),
            Declaration::Relation(r) => r.name.clone(),
            Declaration::Property { class, property, .. } => format!("{class}.{property}"),
        };
        let body = json!({
            "specversion": "1.0",
            "id": format!("{}:0", event.id),
            "source": event.source,
            "type": format!("rannoch.declared.{name}"),
        })
        .to_string();
        self.graph.declare(&declaration, &body).map_err(|e| e.to_string())?;
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<Option<u64>, String> {
        Ok(None)
    }
    fn place(&self) -> Option<u64> {
        None
    }
}

/// The store as an app reads it, for every node and link the events name: each node as it is
/// now, its hearings and what was seen of it; each link's hearings and state. Written in key
/// order, so two stores that hold the same graph give the same text, whatever order it arrived
/// in and however its file is laid out.
///
/// # Errors
/// The store cannot be read.
pub fn canonical(path: &Path, events: &[Event], with_recorded: bool) -> Result<String, String> {
    let graph = rannoch_store::open(path, &KEY).map_err(|e| e.to_string())?;
    let mut nodes = std::collections::BTreeSet::new();
    let mut links = std::collections::BTreeSet::new();
    for event in events {
        let ty = if event.ty == SHIPPED { "westwind.order.placed" } else { event.ty.as_str() };
        let Some(&(_, _, class, relations)) = TABLES.iter().find(|t| t.1 == ty) else {
            continue;
        };
        let Some(key) = event.data.get("id").and_then(Value::as_str) else {
            continue;
        };
        nodes.insert((class.to_string(), key.to_string()));
        for &(column, relation, _) in relations {
            if let Some(to) = event.data.get(column).and_then(Value::as_str) {
                links.insert((relation.to_string(), key.to_string(), to.to_string()));
            }
        }
    }
    let mut text = String::new();
    let err = |e: rannoch_store::Error| e.to_string();
    for (class, key) in &nodes {
        let node = NodeRef::new(class.clone(), key.clone());
        let now = graph.find(class, key).map_err(err)?.map(|n| {
            if with_recorded {
                format!("{n:?}")
            } else {
                format!("{:?} {:?} {:?}", n.properties, n.source, n.valid)
            }
        });
        text.push_str(&format!(
            "node {class} {key}\n  now {:?}\n  hearings {:?}\n  seen {:?}\n",
            now,
            graph.hearings(&node).map_err(err)?,
            graph.seen(&node).map_err(err)?,
        ));
    }
    let now = View { axis: None, known: None };
    for (relation, from, to) in &links {
        text.push_str(&format!(
            "link {relation} {from} {to}\n  now {:?}\n  hearings {:?}\n",
            graph.link_state_at(relation, from, to, &now).map_err(err)?,
            graph.link_hearings(relation, from, to).map_err(err)?,
        ));
    }
    Ok(text)
}

/// The SHA-256 of a file's bytes.
///
/// # Errors
/// The file cannot be read.
pub fn file_hash(path: &Path) -> Result<String, String> {
    use sha2::Digest;
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    Ok(format!("{:x}", sha2::Sha256::digest(bytes)))
}

/// A fresh path for a store in `dir`.
#[must_use]
pub fn store_path(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(format!("{name}.rannoch"));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    path
}
