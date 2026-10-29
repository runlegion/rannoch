//! What an app's world is: classes with natural keys and typed properties, and relations with the
//! class at each end. Declaring, checking a declaration against what is declared, and checking a
//! value against its declared type. Pure code with no I/O.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Why the ontology does not allow what was asked, in ontology terms: it names classes,
/// relations and properties, never a table.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Refused(pub String);

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

    /// Checks each value against its property's declared type.
    ///
    /// # Errors
    /// A property the class does not declare, or a value not of its property's type.
    pub fn check(&self, properties: &BTreeMap<String, Value>) -> Result<(), Refused> {
        for (property, value) in properties {
            let declared = self.properties.get(property).ok_or_else(|| {
                Refused(format!(
                    "property {property} is not declared on class {}",
                    self.name
                ))
            })?;
            if !value.is(*declared) {
                return Err(Refused(format!(
                    "property {property} of class {} is declared {declared:?}",
                    self.name
                )));
            }
        }
        Ok(())
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
    #[must_use]
    pub fn is(&self, of: ValueType) -> bool {
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

    /// Checks that a link from a node of class `from` to a node of class `to` is one this
    /// relation holds.
    ///
    /// # Errors
    /// Either end is not of the relation's declared class.
    pub fn holds(&self, from: &str, to: &str) -> Result<(), Refused> {
        if from != self.from || to != self.to {
            return Err(Refused(format!(
                "relation {} holds from {} to {}; this link is from {from} to {to}",
                self.name, self.from, self.to
            )));
        }
        Ok(())
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

/// A graph's classes and relations, as its stream declared them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ontology {
    pub classes: BTreeMap<String, Class>,
    pub relations: BTreeMap<String, Relation>,
}

impl Ontology {
    /// The declared class `name`.
    ///
    /// # Errors
    /// No class of that name is declared.
    pub fn class(&self, name: &str) -> Result<&Class, Refused> {
        self.classes
            .get(name)
            .ok_or_else(|| Refused(format!("class {name} is not declared")))
    }

    /// The declared relation `name`.
    ///
    /// # Errors
    /// No relation of that name is declared.
    pub fn relation(&self, name: &str) -> Result<&Relation, Refused> {
        self.relations
            .get(name)
            .ok_or_else(|| Refused(format!("relation {name} is not declared")))
    }

    /// Checks a declaration against what is declared. Returns false when the declaration is
    /// identical to what is declared, so it changes nothing; true when it declares something new.
    ///
    /// # Errors
    /// The declaration conflicts with what is declared: a class or relation of the same name
    /// with other properties, history or ends, a property of the same name with another type,
    /// a property of a class that is not declared, or a relation whose end names a class that
    /// is not declared.
    pub fn check(&self, declaration: &Declaration) -> Result<bool, Refused> {
        match declaration {
            Declaration::Class(class) => match self.classes.get(&class.name) {
                Some(have) if have == class => Ok(false),
                Some(have) => Err(Refused(format!(
                    "class {} is already declared with properties {} and history {:?}; \
                     this declaration has {} and {:?}",
                    class.name,
                    list(&have.properties),
                    have.history,
                    list(&class.properties),
                    class.history
                ))),
                None => Ok(true),
            },
            Declaration::Relation(relation) => {
                if let Some(have) = self.relations.get(&relation.name) {
                    if have == relation {
                        return Ok(false);
                    }
                    return Err(Refused(format!(
                        "relation {} is already declared {}; this declaration is {}",
                        relation.name,
                        ends(have),
                        ends(relation)
                    )));
                }
                for end in [&relation.from, &relation.to] {
                    if !self.classes.contains_key(end) {
                        return Err(Refused(format!(
                            "relation {} names class {end}, which is not declared",
                            relation.name
                        )));
                    }
                }
                Ok(true)
            }
            Declaration::Property {
                class,
                property,
                of,
            } => match self.class(class)?.properties.get(property) {
                Some(declared) if declared == of => Ok(false),
                Some(declared) => Err(Refused(format!(
                    "property {property} of class {class} is already declared {declared:?}; \
                     this declaration has {of:?}"
                ))),
                None => Ok(true),
            },
        }
    }
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
