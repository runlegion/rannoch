//! Opening a graph file, and the ontology its own stream declared.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::types::ToSqlOutput;
use rusqlite::{Connection, ToSql};
use serde::{Deserialize, Serialize};

use crate::events::{Events, OWN_TABLES};
use crate::history::{HISTORY_TABLES, Key, see_all, take};
use crate::write::{Error, date_fates};

/// A class: a kind of node, with its natural key, its properties and each one's value type,
/// and the key its history is ordered by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub name: String,
    /// Each property and its value type. A store written before value types existed lists its
    /// properties by name alone; each of those reads as text.
    #[serde(deserialize_with = "properties")]
    pub properties: BTreeMap<String, ValueType>,
    /// The key the class's history is ordered by; the origin's record time when it declares none.
    #[serde(default)]
    pub history: HistoryKey,
}

impl Class {
    /// A class whose properties are all text and whose history is ordered by the origin's
    /// record time.
    pub fn new<P: Into<String>>(
        name: impl Into<String>,
        properties: impl IntoIterator<Item = P>,
    ) -> Self {
        Class {
            name: name.into(),
            properties: properties
                .into_iter()
                .map(|p| (p.into(), ValueType::Text))
                .collect(),
            history: HistoryKey::Origin,
        }
    }
}

/// A class's properties as a map from name to type, or as the list of names a store written
/// before value types existed holds, each of them text.
fn properties<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, ValueType>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Either {
        Typed(BTreeMap<String, ValueType>),
        Names(BTreeSet<String>),
    }
    Ok(match Either::deserialize(deserializer)? {
        Either::Typed(typed) => typed,
        Either::Names(names) => names.into_iter().map(|p| (p, ValueType::Text)).collect(),
    })
}

/// The one value type a property declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    #[default]
    Text,
    /// 64-bit signed.
    Integer,
    /// Exact: read back with every digit it was written with.
    Decimal,
    /// 64-bit.
    Float,
    /// 64-bit floats, exactly this many.
    FloatVector(usize),
}

/// What a class's history is ordered by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryKey {
    /// The record time of the store that first recorded each hearing, carried in its event:
    /// never the order this store received it in. It is stored as `recorded`, as every store
    /// written before it holds it.
    #[default]
    #[serde(rename = "recorded")]
    Origin,
    /// An integer the writer supplies with each write.
    Integer,
    /// A time the writer supplies with each write: UTC, millisecond precision.
    Time,
}

/// A property's value, of its property's declared type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "Wire", into = "Wire")]
pub enum Value {
    Text(String),
    Integer(i64),
    /// The digits as written.
    Decimal(String),
    Float(f64),
    FloatVector(Vec<f64>),
}

/// A value as events encode it: text as a bare string, as every event before value types
/// holds it, and every other type tagged with its name.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Wire {
    Text(String),
    Typed(Typed),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Typed {
    Integer(i64),
    Decimal(String),
    Float(f64),
    FloatVector(Vec<f64>),
}

impl From<Wire> for Value {
    fn from(wire: Wire) -> Value {
        match wire {
            Wire::Text(text) => Value::Text(text),
            Wire::Typed(Typed::Integer(n)) => Value::Integer(n),
            Wire::Typed(Typed::Decimal(d)) => Value::Decimal(d),
            Wire::Typed(Typed::Float(f)) => Value::Float(f),
            Wire::Typed(Typed::FloatVector(v)) => Value::FloatVector(v),
        }
    }
}

impl From<Value> for Wire {
    fn from(value: Value) -> Wire {
        match value {
            Value::Text(text) => Wire::Text(text),
            Value::Integer(n) => Wire::Typed(Typed::Integer(n)),
            Value::Decimal(d) => Wire::Typed(Typed::Decimal(d)),
            Value::Float(f) => Wire::Typed(Typed::Float(f)),
            Value::FloatVector(v) => Wire::Typed(Typed::FloatVector(v)),
        }
    }
}

/// Floats compare by their bits, so a value equals itself and the graph's types keep `Eq`.
impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        let bits = |v: &[f64]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
        match (self, other) {
            (Value::Text(a), Value::Text(b)) | (Value::Decimal(a), Value::Decimal(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::FloatVector(a), Value::FloatVector(b)) => bits(a) == bits(b),
            _ => false,
        }
    }
}

impl Eq for Value {}

impl From<&str> for Value {
    fn from(text: &str) -> Value {
        Value::Text(text.to_string())
    }
}

impl From<String> for Value {
    fn from(text: String) -> Value {
        Value::Text(text)
    }
}

impl Value {
    /// Whether the value is of type `of`, a float vector also of its length.
    pub(crate) fn is(&self, of: ValueType) -> bool {
        match (self, of) {
            (Value::Text(_), ValueType::Text)
            | (Value::Integer(_), ValueType::Integer)
            | (Value::Decimal(_), ValueType::Decimal)
            | (Value::Float(_), ValueType::Float) => true,
            (Value::FloatVector(v), ValueType::FloatVector(len)) => v.len() == len,
            _ => false,
        }
    }
}

/// Text and decimals are held as text, so a decimal keeps every digit; a float vector as its
/// floats' little-endian bytes.
impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Value::Text(s) | Value::Decimal(s) => ToSqlOutput::from(s.as_str()),
            Value::Integer(n) => ToSqlOutput::from(*n),
            Value::Float(f) => ToSqlOutput::from(*f),
            Value::FloatVector(v) => {
                ToSqlOutput::from(v.iter().flat_map(|f| f.to_le_bytes()).collect::<Vec<u8>>())
            }
        })
    }
}

impl ValueType {
    /// The column type that holds values of this type.
    pub(crate) fn column(self) -> &'static str {
        match self {
            ValueType::Text | ValueType::Decimal => "TEXT",
            ValueType::Integer => "INTEGER",
            ValueType::Float => "REAL",
            ValueType::FloatVector(_) => "BLOB",
        }
    }
}

/// A relation: links from a node of one class to a node of another. A containment relation
/// says its source is part of its target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub from: String,
    pub to: String,
    pub containment: bool,
}

impl Relation {
    pub fn new(
        name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        containment: bool,
    ) -> Self {
        Relation {
            name: name.into(),
            from: from.into(),
            to: to.into(),
            containment,
        }
    }
}

/// A graph's classes and relations, as its stream declared them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ontology {
    pub classes: BTreeMap<String, Class>,
    pub relations: BTreeMap<String, Relation>,
}

#[cfg(test)]
thread_local! {
    /// How many times `Ontology::load` ran on this thread, so a test can see a reload.
    pub(crate) static LOADS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Ontology {
    pub(crate) fn load(conn: &Connection) -> Result<Ontology, Error> {
        #[cfg(test)]
        LOADS.with(|loads| loads.set(loads.get() + 1));
        let mut ontology = Ontology::default();
        let mut statement = conn.prepare_cached("SELECT kind, body FROM _ontology")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let body: String = row.get(1)?;
            if row.get::<_, String>(0)? == CLASS {
                let class: Class = serde_json::from_str(&body)?;
                ontology.classes.insert(class.name.clone(), class);
            } else {
                let relation: Relation = serde_json::from_str(&body)?;
                ontology.relations.insert(relation.name.clone(), relation);
            }
        }
        Ok(ontology)
    }

    pub(crate) fn class(&self, name: &str) -> Result<&Class, Error> {
        self.classes
            .get(name)
            .ok_or_else(|| Error::Refused(format!("class {name} is not declared")))
    }

    pub(crate) fn relation(&self, name: &str) -> Result<&Relation, Error> {
        self.relations
            .get(name)
            .ok_or_else(|| Error::Refused(format!("relation {name} is not declared")))
    }
}

pub(crate) const CLASS: &str = "class";
pub(crate) const RELATION: &str = "relation";

/// Each item formatted by `each` and joined into one string.
pub(crate) fn concat<T>(items: impl IntoIterator<Item = T>, each: impl Fn(T) -> String) -> String {
    items.into_iter().map(each).collect::<Vec<_>>().concat()
}

/// A quoted SQL identifier.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The table that holds a class's nodes. Its name differs from the class's own name, so no
/// answer in ontology terms ever names it.
pub(crate) fn class_table(class: &str) -> String {
    ident(&format!("{CLASS}:{class}"))
}

/// The table that holds a relation's links.
pub(crate) fn relation_table(relation: &str) -> String {
    ident(&format!("{RELATION}:{relation}"))
}

/// The index over a relation's links by their target.
pub(crate) fn relation_index(relation: &str) -> String {
    ident(&format!("{RELATION}:{relation}:to"))
}

/// The column that holds a property. Prefixed, so a property never meets a column of rannoch's.
pub(crate) fn property_column(property: &str) -> String {
    ident(&format!("p:{property}"))
}

/// While another connection holds the file's write lock, a connection retries every millisecond,
/// for the same five seconds the default handler waits, so it takes the lock in the gap a write
/// leaves between pieces. The default handler backs off to 100 ms and misses that gap.
fn retry_lock(attempts: i32) -> bool {
    std::thread::sleep(Duration::from_millis(1));
    attempts < 5_000
}

/// The file's `PRAGMA schema_version`. SQLite raises it only when the schema changes, and every
/// declaration that is not identical creates a table or adds a column, so an unchanged version
/// means an unchanged ontology. Read it before loading the ontology it stands for, or inside the
/// same transaction, so a declaration landing in between costs a reload, never a stale ontology.
pub(crate) fn schema_version(conn: &Connection) -> Result<i64, Error> {
    Ok(conn
        .prepare_cached("PRAGMA schema_version")?
        .query_row([], |row| row.get(0))?)
}

/// One graph file.
#[derive(Debug)]
pub struct Graph {
    pub(crate) conn: Connection,
    /// As of the last load by `open` or a `&mut self` method; what `ontology()` returns.
    pub(crate) ontology: Ontology,
    /// What reads use: the ontology and the `PRAGMA schema_version` it was loaded at.
    /// `Graph` is already `!Sync` through `Connection`, so a `RefCell` costs nothing; `Arc`
    /// (not `Rc`) keeps `Graph` `Send`, as it is today.
    pub(crate) fresh: RefCell<Fresh>,
    /// Other graph files this graph's references can name, by the name each was mounted under.
    pub(crate) mounts: BTreeMap<String, Graph>,
    /// The app's content-identity key; a mounted graph, which is only read, holds none.
    pub(crate) key: Option<Key>,
}

/// An ontology and the schema version of the file it was loaded at.
#[derive(Debug)]
pub(crate) struct Fresh {
    pub(crate) schema_version: i64,
    pub(crate) ontology: Arc<Ontology>,
}

/// Opens the graph the file at `path` holds, with the ontology its own stream declared, under the
/// app's content-identity `key`. A path that holds no graph yet starts an empty graph. The key is
/// never written to the store's files: the store keeps a check value, and a store that holds none
/// yet, as one written before keys does, takes the key it is first opened with.
///
/// # Errors
/// [`Error::KeyMissing`] without a key; [`Error::KeyDiffers`] when the graph was built under
/// another key; the file cannot be opened as a graph file.
pub fn open(path: impl AsRef<Path>, key: Option<&[u8; 32]>) -> Result<Graph, Error> {
    open_keyed(path, &Key::new(key.ok_or(Error::KeyMissing)?))
}

/// Opens the graph at `path` under `key`.
pub(crate) fn open_keyed(path: impl AsRef<Path>, key: &Key) -> Result<Graph, Error> {
    let mut graph = open_file(path)?;
    take(&mut graph.conn, key)?;
    graph.key = Some(key.clone());
    Ok(graph)
}

/// Whether `conn` holds a table named `name`.
pub(crate) fn table(conn: &Connection, name: &str) -> Result<bool, Error> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        [name],
        |row| row.get(0),
    )?)
}

/// Opens the graph at `path` without a key, to be read only.
pub(crate) fn open_file(path: impl AsRef<Path>) -> Result<Graph, Error> {
    let mut conn = Connection::open(path)?;
    conn.busy_handler(Some(retry_lock))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;",
    )?;
    conn.set_prepared_statement_cache_capacity(256);
    conn.execute_batch(OWN_TABLES)?;
    date_fates(&mut conn)?;
    // A store whose hearings were written before what is seen of them was kept.
    if table(&conn, "_hearings")? && !table(&conn, "_seen")? {
        see_all(&mut conn)?;
    }
    conn.execute_batch(HISTORY_TABLES)?;
    let schema_version = schema_version(&conn)?;
    let ontology = Ontology::load(&conn)?;
    let fresh = RefCell::new(Fresh {
        schema_version,
        ontology: Arc::new(ontology.clone()),
    });
    Ok(Graph {
        conn,
        ontology,
        fresh,
        mounts: BTreeMap::new(),
        key: None,
    })
}

impl Graph {
    /// The graph's ontology as of this handle's last load, by `open` or by one of its own
    /// declarations or writes: every class and relation declared on its stream by then. A
    /// declaration another handle made since is not in it; reads see it all the same.
    #[must_use]
    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    /// The ontology the file holds now. Another handle may have declared since this one last
    /// loaded, and a read must answer from the file's own stream; checking the schema version
    /// first keeps a plain data write elsewhere from costing a reload.
    pub(crate) fn current_ontology(&self) -> Result<Arc<Ontology>, Error> {
        let version = schema_version(&self.conn)?;
        {
            let fresh = self.fresh.borrow();
            if fresh.schema_version == version {
                return Ok(Arc::clone(&fresh.ontology));
            }
        }
        let ontology = Arc::new(Ontology::load(&self.conn)?);
        *self.fresh.borrow_mut() = Fresh {
            schema_version: version,
            ontology: Arc::clone(&ontology),
        };
        Ok(ontology)
    }

    /// Takes `ontology`, loaded at `schema_version`, as both what `ontology()` returns and what
    /// reads use.
    pub(crate) fn loaded(&mut self, schema_version: i64, ontology: Ontology) {
        *self.fresh.get_mut() = Fresh {
            schema_version,
            ontology: Arc::new(ontology.clone()),
        };
        self.ontology = ontology;
    }

    /// The graph's event log.
    #[must_use]
    pub fn events(&self) -> Events<'_> {
        Events { conn: &self.conn }
    }
}

#[cfg(test)]
mod tests {
    use super::LOADS;
    use crate::{Class, Date, Declaration, Unit, ValidTime, Write, open};

    fn loads() -> usize {
        LOADS.with(std::cell::Cell::get)
    }

    fn temp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()))
    }

    fn unit(class: &str, key: &str) -> Unit {
        Unit::new(
            class,
            key,
            [] as [(&str, &str); 0],
            ValidTime::On(Date::day(2026, 1, 1)),
        )
    }

    #[test]
    fn a_value_survives_its_encoding_and_bare_text_is_text()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::Value;
        for value in [
            Value::Text("1".into()),
            Value::Integer(9_007_199_254_740_993),
            Value::Decimal("12345678901234567890.0000000001".into()),
            Value::Float(-0.1),
            Value::FloatVector(vec![1.5, -2.25, 0.125]),
        ] {
            let back: Value = serde_json::from_str(&serde_json::to_string(&value)?)?;
            assert_eq!(back, value);
        }
        assert_eq!(
            serde_json::from_str::<Value>("\"412\"")?,
            Value::from("412")
        );
        Ok(())
    }

    #[test]
    fn a_graph_is_still_send() {
        fn send<T: Send>() {}
        send::<crate::Graph>();
    }

    #[test]
    fn a_data_write_elsewhere_costs_a_read_no_reload() -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("no-reload");
        let mut a = open(&path, Some(&crate::TEST_KEY))?;
        a.declare(&Declaration::Class(Class::new("crate", [] as [&str; 0])))?;
        let b = open(&path, Some(&crate::TEST_KEY))?;
        a.write(&Write::new("a").unit(unit("crate", "k")))?;

        let before = loads();
        let found = b.find("crate", "k")?;
        assert_eq!(
            loads(),
            before,
            "a plain data write must not reload the ontology"
        );
        assert!(found.is_some());
        Ok(())
    }

    #[test]
    fn a_declaration_elsewhere_reloads_once_on_the_first_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("reload-once");
        let mut a = open(&path, Some(&crate::TEST_KEY))?;
        let b = open(&path, Some(&crate::TEST_KEY))?;
        a.declare(&Declaration::Class(Class::new("late", [] as [&str; 0])))?;
        a.write(&Write::new("a").unit(unit("late", "k")))?;

        let before = loads();
        assert!(b.find("late", "k")?.is_some());
        assert_eq!(loads(), before + 1, "the first read reloads once");
        assert!(b.find("late", "k")?.is_some());
        assert_eq!(loads(), before + 1, "the second read does not reload");
        Ok(())
    }
}
