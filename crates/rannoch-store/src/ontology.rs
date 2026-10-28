//! The ontology a graph's own stream declared: its classes and relations, the declarations that
//! build them, and the values their properties hold.

use std::collections::BTreeMap;

use rusqlite::types::ToSqlOutput;
use rusqlite::{Connection, ToSql, params};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::queries::{
    LOAD_ONTOLOGY, SAVE_ONTOLOGY, add_property_column, create_class_table, create_relation_table,
    property_column,
};

/// A class: a kind of node, with its natural key, its properties and each one's value type,
/// and the key its history is ordered by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub name: String,
    /// Each property and its value type.
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
    /// never the order this store received it in.
    #[default]
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

/// A value as events encode it: text as a bare string, and every other type tagged with its name.
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

impl Ontology {
    pub(crate) fn load(conn: &Connection) -> Result<Ontology, Error> {
        let mut ontology = Ontology::default();
        let mut statement = conn.prepare_cached(LOAD_ONTOLOGY)?;
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

/// A date a fact was true from: a year and month, or a year, month and day.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Date(String);

impl Date {
    #[must_use]
    pub fn month(year: u16, month: u8) -> Date {
        Date(format!("{year:04}-{month:02}"))
    }

    #[must_use]
    pub fn day(year: u16, month: u8, day: u8) -> Date {
        Date(format!("{year:04}-{month:02}-{day:02}"))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Year, month and day; a month date's day is 1. Err when the month is outside 1..=12 or
    /// the day is outside the month (leap years counted).
    pub(crate) fn parts(&self) -> Result<(u16, u8, u8), Error> {
        let refused =
            |why: String| Error::Refused(format!("valid time {self} is not a date: {why}"));
        let fields: Vec<&str> = self.0.split('-').collect();
        let (year, month, day) = match fields.as_slice() {
            [year, month] => (*year, *month, None),
            [year, month, day] => (*year, *month, Some(*day)),
            _ => {
                return Err(refused(
                    "it is not year-month or year-month-day".to_string(),
                ));
            }
        };
        let number = |field: &str, part: &str| -> Result<u16, Error> {
            if field.is_empty() || !field.bytes().all(|b| b.is_ascii_digit()) {
                return Err(refused(format!("{part} {field} is not a number")));
            }
            field
                .parse()
                .map_err(|_| refused(format!("{part} {field} is out of range")))
        };
        let year = number(year, "year")?;
        let month = u8::try_from(number(month, "month")?)
            .ok()
            .filter(|m| (1..=12).contains(m))
            .ok_or_else(|| refused(format!("month {month} is out of range")))?;
        let Some(day) = day else {
            return Ok((year, month, 1));
        };
        let leap =
            (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400);
        let last = match month {
            2 if leap => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        let day = u8::try_from(number(day, "day")?)
            .ok()
            .filter(|d| (1..=last).contains(d))
            .ok_or_else(|| refused(format!("day {day} is out of range")))?;
        Ok((year, month, day))
    }
}

impl std::fmt::Display for Date {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// When a fact was true: a date, or stated unknown. Unknown is never read as now.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidTime {
    On(Date),
    Unknown,
}

impl ValidTime {
    pub(crate) fn to_sql(&self) -> Option<&str> {
        match self {
            ValidTime::On(date) => Some(date.as_str()),
            ValidTime::Unknown => None,
        }
    }

    pub(crate) fn from_sql(value: Option<String>) -> ValidTime {
        value.map_or(ValidTime::Unknown, |date| ValidTime::On(Date(date)))
    }
}

/// One declaration on a graph's stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Declaration {
    Class(Class),
    Relation(Relation),
    /// A property added to a declared class, of value type `of`.
    Property {
        class: String,
        property: String,
        of: ValueType,
    },
}

fn list(properties: &BTreeMap<String, ValueType>) -> String {
    let names: Vec<String> = properties
        .iter()
        .map(|(name, of)| match of {
            ValueType::Text => name.clone(),
            other => format!("{name} as {other:?}"),
        })
        .collect();
    format!("[{}]", names.join(", "))
}

fn ends(relation: &Relation) -> String {
    let containment = if relation.containment {
        ", as containment"
    } else {
        ""
    };
    format!("from {} to {}{containment}", relation.from, relation.to)
}

fn save(tx: &Connection, kind: &str, name: &str, body: &impl Serialize) -> Result<(), Error> {
    tx.execute(
        SAVE_ONTOLOGY,
        params![kind, name, serde_json::to_string(body)?],
    )?;
    Ok(())
}

/// Builds what a declaration needs in `tx`. Returns false, building nothing, when the
/// declaration is identical to the ontology as it is now.
pub(crate) fn build(
    tx: &Connection,
    ontology: &Ontology,
    declaration: &Declaration,
) -> Result<bool, Error> {
    match declaration {
        Declaration::Class(class) => {
            if let Some(have) = ontology.classes.get(&class.name) {
                if have == class {
                    return Ok(false);
                }
                return Err(Error::Refused(format!(
                    "class {} is already declared with properties {} and history {:?}; \
                     this declaration has {} and {:?}",
                    class.name,
                    list(&have.properties),
                    have.history,
                    list(&class.properties),
                    class.history
                )));
            }
            let columns = concat(&class.properties, |(p, of)| {
                format!(", {} {}", property_column(p), of.column())
            });
            tx.execute_batch(&create_class_table(&class.name, &columns))?;
            save(tx, CLASS, &class.name, class)?;
        }
        Declaration::Relation(relation) => {
            if let Some(have) = ontology.relations.get(&relation.name) {
                if have == relation {
                    return Ok(false);
                }
                return Err(Error::Refused(format!(
                    "relation {} is already declared {}; this declaration is {}",
                    relation.name,
                    ends(have),
                    ends(relation)
                )));
            }
            for end in [&relation.from, &relation.to] {
                if !ontology.classes.contains_key(end) {
                    return Err(Error::Refused(format!(
                        "relation {} names class {end}, which is not declared",
                        relation.name
                    )));
                }
            }
            tx.execute_batch(&create_relation_table(
                &relation.name,
                &relation.from,
                &relation.to,
            ))?;
            save(tx, RELATION, &relation.name, relation)?;
        }
        Declaration::Property {
            class,
            property,
            of,
        } => {
            let have = ontology.class(class)?;
            match have.properties.get(property) {
                Some(declared) if declared == of => return Ok(false),
                Some(declared) => {
                    return Err(Error::Refused(format!(
                        "property {property} of class {class} is already declared {declared:?}; \
                         this declaration has {of:?}"
                    )));
                }
                None => {}
            }
            tx.execute_batch(&add_property_column(class, property, of.column()))?;
            let mut grown = have.clone();
            grown.properties.insert(property.clone(), *of);
            save(tx, CLASS, class, &grown)?;
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn a_value_survives_its_encoding_and_bare_text_is_text()
    -> Result<(), Box<dyn std::error::Error>> {
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
}
