//! Keeping the ontology a graph's own stream declared: loading it, saving each declaration, and
//! building the tables and columns a declaration needs, and the values their properties hold.

use rannoch_ontology::{Declaration, Ontology, Refused, Value, ValueType};
use rusqlite::types::ToSqlOutput;
use rusqlite::{Connection, ToSql, params};
use serde::Serialize;

use crate::Error;
use crate::queries::{
    LOAD_ONTOLOGY, SAVE_ONTOLOGY, add_property_column, concat, create_class_table,
    create_relation_table, property_column,
};

impl From<Refused> for Error {
    fn from(refused: Refused) -> Error {
        Error::Refused(refused.0)
    }
}

/// The ontology as the graph's tables hold it.
pub(crate) fn load(conn: &Connection) -> Result<Ontology, Error> {
    let mut ontology = Ontology::default();
    let mut statement = conn.prepare_cached(LOAD_ONTOLOGY)?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let body: String = row.get(1)?;
        if row.get::<_, String>(0)? == CLASS {
            let class: rannoch_ontology::Class = serde_json::from_str(&body)?;
            ontology.classes.insert(class.name.clone(), class);
        } else {
            let relation: rannoch_ontology::Relation = serde_json::from_str(&body)?;
            ontology.relations.insert(relation.name.clone(), relation);
        }
    }
    Ok(ontology)
}

pub(crate) const CLASS: &str = "class";
pub(crate) const RELATION: &str = "relation";

/// A value bound as a parameter. Text and decimals are held as text, so a decimal keeps every
/// digit; a float vector as its floats' little-endian bytes.
pub(crate) struct Bound<'a>(pub(crate) &'a Value);

impl ToSql for Bound<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self.0 {
            Value::Text(s) | Value::Decimal(s) => ToSqlOutput::from(s.as_str()),
            Value::Integer(n) => ToSqlOutput::from(*n),
            Value::Float(f) => ToSqlOutput::from(*f),
            Value::FloatVector(v) => {
                ToSqlOutput::from(v.iter().flat_map(|f| f.to_le_bytes()).collect::<Vec<u8>>())
            }
        })
    }
}

/// The column type that holds values of type `of`.
fn column(of: ValueType) -> &'static str {
    match of {
        ValueType::Text | ValueType::Decimal => "TEXT",
        ValueType::Integer => "INTEGER",
        ValueType::Float => "REAL",
        ValueType::FloatVector(_) => "BLOB",
    }
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
    if !ontology.check(declaration)? {
        return Ok(false);
    }
    match declaration {
        Declaration::Class(class) => {
            let columns = concat(&class.properties, |(p, of)| {
                format!(", {} {}", property_column(p), column(*of))
            });
            tx.execute_batch(&create_class_table(&class.name, &columns))?;
            save(tx, CLASS, &class.name, class)?;
        }
        Declaration::Relation(relation) => {
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
            tx.execute_batch(&add_property_column(class, property, column(*of)))?;
            let mut grown = ontology.class(class)?.clone();
            grown.properties.insert(property.clone(), *of);
            save(tx, CLASS, class, &grown)?;
        }
    }
    Ok(true)
}
