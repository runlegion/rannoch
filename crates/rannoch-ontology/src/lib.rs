//! What an app's world is: classes with natural keys and typed properties, and relations with the
//! class at each end. Declaring, checking a declaration against what is declared, and checking a
//! value against its declared type. Pure code with no I/O.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Why the ontology does not allow what was asked, in ontology terms: it names classes,
/// relations and properties, never a table.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Refused(pub String);

/// A class: a kind of node, with its natural key, its properties and each one's value type,
/// and the axis its history is ordered by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub name: String,
    /// Each property and its value type.
    pub properties: BTreeMap<String, ValueType>,
    /// The named integer parts of the point its history is ordered by, compared in this order:
    /// each part refines the one before it. Empty: its axis is when each fact became known.
    #[serde(default)]
    pub axis: Vec<String>,
    /// The properties its nodes can be listed by. Only these are indexed, so writing a node
    /// pays for an index only where one is declared.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub indexed: BTreeSet<String>,
}

impl Class {
    /// A class whose properties are all text and that declares no axis, so its history is
    /// ordered by when each fact became known.
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
            axis: Vec::new(),
            indexed: BTreeSet::new(),
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
    /// The named integer parts of the point its links and endings are ordered by, as a class
    /// declares them. Empty: its axis is when each fact became known.
    #[serde(default)]
    pub axis: Vec<String>,
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
            axis: Vec::new(),
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

/// A node named by its class and natural key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeRef {
    pub class: String,
    pub key: String,
}

impl NodeRef {
    pub fn new(class: impl Into<String>, key: impl Into<String>) -> Self {
        NodeRef {
            class: class.into(),
            key: key.into(),
        }
    }
}

/// A shape: a named neighbourhood, walked from its roots one step at a time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shape {
    pub name: String,
    pub roots: Roots,
    pub steps: Vec<Step>,
}

/// Where a shape's walk starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Roots {
    /// One node.
    Node { node: NodeRef },
    /// The nodes of a class that have every value in `filter`, each an indexed property.
    Class {
        class: String,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        filter: BTreeMap<String, Value>,
    },
}

/// One step of a shape: the links it follows, what it keeps of the nodes they reach, and how many
/// it takes. A step with no cap is exact.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    /// A relation with its direction in its name: `r` forward, `r<-` backward, `r<->` either.
    pub relation: String,
    /// A reached node stays only if it has every one of these values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keep: BTreeMap<String, Value>,
    /// A reached node goes if it has every one of these values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub drop: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cap: Option<usize>,
}

/// Which way a step follows its relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Way {
    Forward,
    Backward,
    Either,
}

impl Step {
    /// The relation's name without its direction, and the direction.
    #[must_use]
    pub fn way(&self) -> (&str, Way) {
        if let Some(name) = self.relation.strip_suffix("<->") {
            (name, Way::Either)
        } else if let Some(name) = self.relation.strip_suffix("<-") {
            (name, Way::Backward)
        } else {
            (&self.relation, Way::Forward)
        }
    }

    /// Whether a reached node with `properties` passes this step's filters.
    #[must_use]
    pub fn passes(&self, properties: &BTreeMap<String, Value>) -> bool {
        let has = |wanted: &BTreeMap<String, Value>| {
            wanted.iter().all(|(p, v)| properties.get(p) == Some(v))
        };
        has(&self.keep) && (self.drop.is_empty() || !has(&self.drop))
    }
}

/// One declaration on a graph's stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Declaration {
    Class(Class),
    Relation(Relation),
    Shape(Shape),
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
    pub shapes: BTreeMap<String, Shape>,
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

    /// The declared shape `name`.
    ///
    /// # Errors
    /// No shape of that name is declared.
    pub fn shape(&self, name: &str) -> Result<&Shape, Refused> {
        self.shapes
            .get(name)
            .ok_or_else(|| Refused(format!("shape {name} is not declared")))
    }

    /// Checks a shape against what is declared: its roots' class and indexed properties, each
    /// step's relation and that the classes the step before reached are at one of its ends, and
    /// that a property a step filters on is declared on a class it reaches.
    fn check_shape(&self, shape: &Shape) -> Result<(), Refused> {
        let mut reached = BTreeSet::new();
        match &shape.roots {
            Roots::Node { node } => {
                self.class(&node.class)?;
                reached.insert(node.class.as_str());
            }
            Roots::Class { class, filter } => {
                let declared = self.class(class)?;
                if let Some(property) = filter.keys().find(|p| !declared.indexed.contains(*p)) {
                    return Err(Refused(format!(
                        "shape {} chooses its roots by property {property}, which class {class} \
                         does not index",
                        shape.name
                    )));
                }
                reached.insert(class.as_str());
            }
        }
        for step in &shape.steps {
            let (name, way) = step.way();
            let relation = self.relation(name)?;
            let mut next = BTreeSet::new();
            if way != Way::Backward && reached.contains(relation.from.as_str()) {
                next.insert(relation.to.as_str());
            }
            if way != Way::Forward && reached.contains(relation.to.as_str()) {
                next.insert(relation.from.as_str());
            }
            if next.is_empty() {
                return Err(Refused(format!(
                    "shape {} steps along {} from {reached:?}, which that relation does not hold",
                    shape.name, step.relation
                )));
            }
            for property in step.keep.keys().chain(step.drop.keys()) {
                let declared = next.iter().any(|class| {
                    self.classes
                        .get(*class)
                        .is_some_and(|c| c.properties.contains_key(property))
                });
                if !declared {
                    return Err(Refused(format!(
                        "shape {} filters step {} on property {property}, which {next:?} do not \
                         declare",
                        shape.name, step.relation
                    )));
                }
            }
            reached = next;
        }
        Ok(())
    }

    /// Checks a declaration against what is declared. Returns false when the declaration is
    /// identical to what is declared, so it changes nothing; true when it declares something new.
    ///
    /// # Errors
    /// The declaration conflicts with what is declared: a class or relation of the same name
    /// with other properties, axis or ends, a property of the same name with another type,
    /// a property of a class that is not declared, a relation whose end names a class that
    /// is not declared, or a shape that does not fit what is declared.
    pub fn check(&self, declaration: &Declaration) -> Result<bool, Refused> {
        match declaration {
            Declaration::Class(class) => match self.classes.get(&class.name) {
                Some(have) if have == class => Ok(false),
                Some(have) => Err(Refused(format!(
                    "class {} is already declared with properties {}, axis {:?} and indexed \
                     {:?}; this declaration has {}, {:?} and {:?}",
                    class.name,
                    list(&have.properties),
                    have.axis,
                    have.indexed,
                    list(&class.properties),
                    class.axis,
                    class.indexed
                ))),
                None => match class
                    .indexed
                    .iter()
                    .find(|p| !class.properties.contains_key(*p))
                {
                    Some(property) => Err(Refused(format!(
                        "class {} indexes property {property}, which it does not declare",
                        class.name
                    ))),
                    None => Ok(true),
                },
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
            Declaration::Shape(shape) => {
                if let Some(have) = self.shapes.get(&shape.name) {
                    if have == shape {
                        return Ok(false);
                    }
                    return Err(Refused(format!(
                        "shape {} is already declared differently",
                        shape.name
                    )));
                }
                self.check_shape(shape)?;
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
    let axis = if relation.axis.is_empty() {
        String::new()
    } else {
        format!(", on axis {:?}", relation.axis)
    };
    format!(
        "from {} to {}{containment}{axis}",
        relation.from, relation.to
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        Class, Declaration, NodeRef, Ontology, Relation, Roots, Shape, Step, Value, ValueType, Way,
    };

    fn declared() -> Ontology {
        let mut ontology = Ontology::default();
        for class in [
            Class::new("author", ["name"]),
            Class::new("book", ["title"]),
        ] {
            ontology.classes.insert(class.name.clone(), class);
        }
        let wrote = Relation::new("wrote", "author", "book", false);
        ontology.relations.insert(wrote.name.clone(), wrote);
        ontology
    }

    #[test]
    fn a_declaration_identical_to_what_is_declared_changes_nothing() {
        let ontology = declared();
        for declaration in [
            Declaration::Class(Class::new("author", ["name"])),
            Declaration::Relation(Relation::new("wrote", "author", "book", false)),
            Declaration::Property {
                class: "book".into(),
                property: "title".into(),
                of: ValueType::Text,
            },
        ] {
            assert_eq!(ontology.check(&declaration), Ok(false), "{declaration:?}");
        }
        assert_eq!(
            ontology.check(&Declaration::Class(Class::new("shelf", ["label"]))),
            Ok(true)
        );
    }

    fn along(relation: &str) -> Step {
        Step {
            relation: relation.into(),
            ..Step::default()
        }
    }

    #[test]
    fn a_step_reads_its_direction_from_its_name() {
        assert_eq!(along("wrote").way(), ("wrote", Way::Forward));
        assert_eq!(along("wrote<-").way(), ("wrote", Way::Backward));
        assert_eq!(along("wrote<->").way(), ("wrote", Way::Either));
    }

    #[test]
    fn a_shape_is_checked_against_what_is_declared() {
        let shape = |roots: Roots, steps: &[&str]| {
            Declaration::Shape(Shape {
                name: "s".into(),
                roots,
                steps: steps.iter().map(|s| along(s)).collect(),
            })
        };
        let author = || Roots::Node {
            node: NodeRef::new("author", "a1"),
        };
        let mut ontology = declared();
        let books = shape(author(), &["wrote", "wrote<-"]);
        assert_eq!(ontology.check(&books), Ok(true));
        let Declaration::Shape(declared_shape) = books.clone() else {
            return;
        };
        ontology.shapes.insert("s".into(), declared_shape);
        assert_eq!(ontology.check(&books), Ok(false), "identical");
        assert!(
            ontology.check(&shape(author(), &["wrote"])).is_err(),
            "conflict"
        );
        let fresh = declared();
        assert!(
            fresh.check(&shape(author(), &["wrote<-"])).is_err(),
            "wrong end"
        );
        assert!(
            fresh.check(&shape(author(), &["sold"])).is_err(),
            "no relation"
        );
        let by_name = Roots::Class {
            class: "book".into(),
            filter: BTreeMap::from([("title".to_string(), Value::from("Dune"))]),
        };
        assert!(
            fresh
                .check(&shape(by_name, &[]))
                .is_err_and(|refused| refused.0.contains("does not index"))
        );
        let filtered = Declaration::Shape(Shape {
            name: "f".into(),
            roots: author(),
            steps: vec![Step {
                keep: BTreeMap::from([("pages".to_string(), Value::Integer(1))]),
                ..along("wrote")
            }],
        });
        assert!(fresh.check(&filtered).is_err_and(|r| r.0.contains("pages")));
    }

    #[test]
    fn a_shape_reads_as_issue_151_writes_it() -> Result<(), serde_json::Error> {
        let node: Shape = serde_json::from_str(
            r#"{"name":"n","roots":{"node":{"class":"a","key":"k"}},"steps":[{"relation":"r<-","cap":2}]}"#,
        )?;
        assert!(matches!(node.roots, Roots::Node { .. }));
        assert_eq!(node.steps[0].cap, Some(2));
        let class: Shape = serde_json::from_str(
            r#"{"name":"c","roots":{"class":"a","filter":{"p":"v"}},"steps":[{"relation":"r","keep":{"p":"v"},"drop":{"q":"w"}}]}"#,
        )?;
        assert!(matches!(class.roots, Roots::Class { .. }));
        let props = |v: &str| BTreeMap::from([("p".to_string(), Value::from(v))]);
        assert!(class.steps[0].passes(&props("v")));
        assert!(!class.steps[0].passes(&props("x")));
        Ok(())
    }

    #[test]
    fn a_class_indexes_only_properties_it_declares() {
        let ontology = declared();
        let shelf = |indexed: &str| {
            Declaration::Class(Class {
                indexed: [indexed.to_string()].into(),
                ..Class::new("shelf", ["label"])
            })
        };
        assert_eq!(ontology.check(&shelf("label")), Ok(true));
        assert!(
            ontology
                .check(&shelf("colour"))
                .is_err_and(|refused| refused.0.contains("colour"))
        );
        let mut indexed = declared();
        let Declaration::Class(label) = shelf("label") else {
            return;
        };
        indexed.classes.insert(label.name.clone(), label);
        assert!(
            indexed
                .check(&Declaration::Class(Class::new("shelf", ["label"])))
                .is_err_and(|refused| refused.0.contains(r#"indexed {"label"}"#)),
            "a redeclaration that differs only in what it indexes names it"
        );
    }

    #[test]
    fn a_conflicting_declaration_is_refused_naming_what_differs_and_no_table() {
        let ontology = declared();
        let mut ordered = Class::new("author", ["name"]);
        ordered.axis = vec!["build".into()];
        for (declaration, names) in [
            (
                Declaration::Class(Class::new("author", ["born"])),
                "class author",
            ),
            (Declaration::Class(ordered), "class author"),
            (
                Declaration::Relation(Relation {
                    axis: vec!["build".into()],
                    ..Relation::new("wrote", "author", "book", false)
                }),
                "relation wrote",
            ),
            (
                Declaration::Relation(Relation::new("wrote", "book", "author", false)),
                "relation wrote",
            ),
            (
                Declaration::Property {
                    class: "book".into(),
                    property: "title".into(),
                    of: ValueType::Integer,
                },
                "property title of class book",
            ),
            (
                Declaration::Property {
                    class: "shelf".into(),
                    property: "label".into(),
                    of: ValueType::Text,
                },
                "class shelf",
            ),
            (
                Declaration::Relation(Relation::new("holds", "shelf", "book", true)),
                "class shelf",
            ),
        ] {
            let Err(refused) = ontology.check(&declaration) else {
                panic!("{declaration:?} must be refused");
            };
            let refused = refused.0;
            assert!(refused.contains(names), "{refused}");
            assert!(!refused.contains("table"), "{refused}");
        }
    }

    #[test]
    fn a_link_holds_only_between_its_relations_classes() {
        let wrote = Relation::new("wrote", "author", "book", false);
        assert_eq!(wrote.holds("author", "book"), Ok(()));
        assert!(wrote.holds("book", "author").is_err());
        assert!(wrote.holds("author", "author").is_err());
    }

    #[test]
    fn a_value_is_checked_against_its_propertys_declared_type() {
        let mut book = Class::new("book", ["title"]);
        book.properties.insert("pages".into(), ValueType::Integer);
        book.properties
            .insert("embedding".into(), ValueType::FloatVector(2));
        let check = |pairs: &[(&str, Value)]| {
            book.check(
                &pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), v.clone()))
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        assert_eq!(
            check(&[
                ("title", Value::from("Dune")),
                ("pages", Value::Integer(412)),
                ("embedding", Value::FloatVector(vec![0.5, 1.0])),
            ]),
            Ok(())
        );
        assert!(check(&[("pages", Value::from("412"))]).is_err());
        assert!(check(&[("embedding", Value::FloatVector(vec![0.5]))]).is_err());
        assert!(check(&[("isbn", Value::from("x"))]).is_err());
    }

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
