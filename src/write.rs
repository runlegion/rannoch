//! Declarations and writes. A declaration is one event; a write is whole units committed in
//! pieces, each piece one transaction whose events land only if it lands.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::events::{
    Change, Event, EventId, Fact, LinkFact, NodeFact, WrongKind, add_derived, append, holds,
    keep_derived,
};
use crate::open::{
    CLASS, Class, Graph, Ontology, RELATION, Relation, Value, ValueType, class_table, concat,
    property_column, read_properties, relation_index, relation_table, schema_version,
};
use crate::read::NodeRef;

/// The most changes one piece holds (a node is one change, a link is one); a unit larger than
/// this is a piece of its own. A piece of this size commits well inside a hook's deadline (T3).
pub(crate) const PIECE_CHANGES: usize = 1_000;

/// How long a write leaves the file free after each piece but its last. It is longer than the
/// interval at which a waiting connection retries the lock (see `open`), so a hook's write
/// waiting on a burst takes the lock between pieces instead of waiting for the whole burst.
const BETWEEN_PIECES: Duration = Duration::from_millis(5);

/// Why rannoch did not do what was asked.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Refused, with a reason in ontology terms.
    #[error("{0}")]
    Refused(String),
    /// No node of the class has the key.
    #[error("no {class} has key {key}")]
    NotFound { class: String, key: String },
    /// The graph file failed.
    #[error("the graph file failed: {0}")]
    Storage(#[from] rusqlite::Error),
    /// An event or an ontology entry could not be encoded or decoded.
    #[error("an event could not be encoded or decoded: {0}")]
    Encoding(#[from] serde_json::Error),
}

/// A write that failed at a piece. The pieces before it landed, and these are their events, so
/// the app can resume the write from the failed piece.
#[derive(Debug, thiserror::Error)]
#[error("a piece of the write failed, after {} events landed: {cause}", events.len())]
pub struct WriteError {
    pub events: Vec<Event>,
    pub cause: Error,
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
    ///
    /// The constructors stay infallible and a stored date is kept as written, so the range is
    /// checked where a write or an as-of read takes a date, and as-of reads compare by value
    /// through here rather than as text.
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

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    /// A property added to a declared class, of value type `of`: text when the declaration
    /// names none, as every declaration before value types holds it.
    Property {
        class: String,
        property: String,
        #[serde(default)]
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
        "INSERT OR REPLACE INTO _ontology(kind, name, body) VALUES (?1, ?2, ?3)",
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
            tx.execute_batch(&format!(
                "CREATE TABLE {}(id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE, \
                 source TEXT NOT NULL, valid TEXT, recorded INTEGER NOT NULL{columns});",
                class_table(&class.name)
            ))?;
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
            let table = relation_table(&relation.name);
            tx.execute_batch(&format!(
                "CREATE TABLE {table}(src INTEGER NOT NULL REFERENCES {}(id), \
                 dst INTEGER NOT NULL REFERENCES {}(id), source TEXT NOT NULL, valid TEXT, \
                 recorded INTEGER NOT NULL, PRIMARY KEY(src, dst)) WITHOUT ROWID; \
                 CREATE INDEX {} ON {table}(dst, src);",
                class_table(&relation.from),
                class_table(&relation.to),
                relation_index(&relation.name)
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
            tx.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                class_table(class),
                property_column(property),
                of.column()
            ))?;
            let mut grown = have.clone();
            grown.properties.insert(property.clone(), *of);
            save(tx, CLASS, class, &grown)?;
        }
    }
    Ok(true)
}

/// One link of a unit, from the node with `from` key to the node with `to` key.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LinkSpec {
    relation: String,
    from: String,
    to: String,
    valid: ValidTime,
}

/// A unit: one node and its own links. A unit is never split across pieces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unit {
    node: NodeFact,
    links: Vec<LinkSpec>,
}

impl Unit {
    /// A node of `class` under natural key `key`, true from `valid`.
    pub fn new<K: Into<String>, V: Into<Value>>(
        class: impl Into<String>,
        key: impl Into<String>,
        properties: impl IntoIterator<Item = (K, V)>,
        valid: ValidTime,
    ) -> Self {
        Unit {
            node: NodeFact {
                class: class.into(),
                key: key.into(),
                properties: properties
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
                valid,
                derived_from: Vec::new(),
            },
            links: Vec::new(),
        }
    }

    /// A link of `relation` from the node keyed `from` to the node keyed `to`, true from
    /// `valid`. An end not yet present leaves the link open until it arrives.
    #[must_use]
    pub fn link(
        mut self,
        relation: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        valid: ValidTime,
    ) -> Self {
        self.links.push(LinkSpec {
            relation: relation.into(),
            from: from.into(),
            to: to.into(),
            valid,
        });
        self
    }

    fn changes(&self) -> usize {
        1 + self.links.len()
    }
}

/// A write: whole units from one source, and the events its nodes were derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    source: String,
    units: Vec<Unit>,
    derived_from: Vec<EventId>,
}

impl Write {
    pub fn new(source: impl Into<String>) -> Self {
        Write {
            source: source.into(),
            units: Vec::new(),
            derived_from: Vec::new(),
        }
    }

    #[must_use]
    pub fn unit(mut self, unit: Unit) -> Self {
        self.units.push(unit);
        self
    }

    /// Names the events the write's nodes were derived from: each node keeps them.
    #[must_use]
    pub fn derived_from(mut self, events: impl IntoIterator<Item = EventId>) -> Self {
        self.derived_from.extend(events);
        self
    }
}

/// Splits units into pieces of whole units, each at most `PIECE_CHANGES` changes unless one
/// unit alone is larger.
fn pieces(units: &[Unit]) -> Vec<&[Unit]> {
    let mut pieces = Vec::new();
    let mut start = 0;
    while start < units.len() {
        let mut end = start + 1;
        let mut changes = units[start].changes();
        while end < units.len() && changes + units[end].changes() <= PIECE_CHANGES {
            changes += units[end].changes();
            end += 1;
        }
        pieces.push(&units[start..end]);
        start = end;
    }
    pieces
}

pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

pub(crate) fn node_id(tx: &Connection, class: &str, key: &str) -> Result<Option<i64>, Error> {
    let sql = format!("SELECT id FROM {} WHERE key = ?1", class_table(class));
    Ok(tx
        .prepare_cached(&sql)?
        .query_row(params![key], |row| row.get(0))
        .optional()?)
}

/// Checks `write` against what `ontology` declares, reading nothing from the store: each node's
/// class is declared, it carries every property its class declares and no other, and each value
/// is of its declared type.
pub(crate) fn check_declared(ontology: &Ontology, write: &Write) -> Result<(), Error> {
    for node in write.units.iter().map(|u| &u.node) {
        let class = ontology.class(&node.class)?;
        for (property, value) in &node.properties {
            let Some(of) = class.properties.get(property) else {
                return Err(Error::Refused(format!(
                    "class {} has no property {property}",
                    class.name
                )));
            };
            if !value.is(*of) {
                return Err(Error::Refused(format!(
                    "property {property} of class {} is declared {of:?}; {value:?} is not",
                    class.name
                )));
            }
            let finite = match value {
                Value::Float(f) => f.is_finite(),
                Value::FloatVector(v) => v.iter().all(|f| f.is_finite()),
                _ => true,
            };
            if !finite {
                return Err(Error::Refused(format!(
                    "property {property} of class {} carries {value:?}; \
                     NaN and infinities are not values",
                    class.name
                )));
            }
        }
        if let Some(missing) = class
            .properties
            .keys()
            .find(|p| !node.properties.contains_key(*p))
        {
            return Err(Error::Refused(format!(
                "class {} declares property {missing}; node {} does not carry it",
                class.name, node.key
            )));
        }
    }
    Ok(())
}

/// Checks a whole write before any piece commits: refuses it when a unit's node does not carry
/// exactly the properties its class declares, each a value of its declared type, when a unit's
/// or a link's valid time is not a real date, when it names as its source an event the graph does
/// not hold, or when one of its links has a source that is not of its relation's declared
/// source class. A target is named by kind and key, so a target key not yet a node of the
/// declared target class leaves the link open.
fn check(conn: &Connection, ontology: &Ontology, write: &Write) -> Result<(), Error> {
    check_declared(ontology, write)?;
    for event in &write.derived_from {
        if !holds(conn, *event)? {
            return Err(Error::Refused(format!(
                "the write names event {event} as its source; the graph holds no such event"
            )));
        }
    }
    let valid_times = write
        .units
        .iter()
        .flat_map(|u| std::iter::once(&u.node.valid).chain(u.links.iter().map(|l| &l.valid)));
    for valid in valid_times {
        if let ValidTime::On(date) = valid {
            date.parts()?;
        }
    }
    let brought: BTreeSet<(&str, &str)> = write
        .units
        .iter()
        .map(|u| (u.node.class.as_str(), u.node.key.as_str()))
        .collect();
    let present = |class: &str, key: &str| -> Result<bool, Error> {
        Ok(brought.contains(&(class, key)) || node_id(conn, class, key)?.is_some())
    };
    for link in write.units.iter().flat_map(|u| &u.links) {
        let relation = ontology.relation(&link.relation)?;
        let (class, key) = (&relation.from, &link.from);
        if present(class, key)? {
            continue;
        }
        for other in ontology.classes.keys().filter(|c| *c != class) {
            if present(other, key)? {
                return Err(Error::Refused(format!(
                    "relation {} is declared {}; {key} is a {other}, not a {class}",
                    relation.name,
                    ends(relation)
                )));
            }
        }
    }
    Ok(())
}

/// What a piece is writing: its transaction, source, recorded time and what it added so far.
struct Piece<'a> {
    tx: &'a Connection,
    ontology: &'a Ontology,
    source: &'a str,
    recorded: i64,
    added: Vec<Fact>,
    wrong_kind: Vec<WrongKind>,
    closed: Vec<LinkFact>,
}

impl<'a> Piece<'a> {
    fn new(tx: &'a Connection, ontology: &'a Ontology, source: &'a str, recorded: i64) -> Self {
        Piece {
            tx,
            ontology,
            source,
            recorded,
            added: Vec::new(),
            wrong_kind: Vec::new(),
            closed: Vec::new(),
        }
    }

    /// Writes a node, or a new version of it. Returns the node's id and whether it is new.
    fn node(&mut self, node: &NodeFact) -> Result<(i64, bool), Error> {
        let class = self.ontology.class(&node.class)?;
        let table = class_table(&class.name);
        let columns: Vec<String> = class
            .properties
            .keys()
            .map(|p| property_column(p))
            .collect();
        let select = format!(
            "SELECT id, source, valid{} FROM {table} WHERE key = ?1",
            concat(&columns, |c| format!(", {c}"))
        );
        let have = self
            .tx
            .prepare_cached(&select)?
            .query_row(params![node.key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    ValidTime::from_sql(row.get(2)?),
                    read_properties(class, row, 3)?,
                ))
            })
            .optional()?;
        let valid = node.valid.to_sql();
        let values: Vec<Option<&Value>> = class
            .properties
            .keys()
            .map(|p| node.properties.get(p))
            .collect();
        let (id, new) = match have {
            Some((id, source, valid, properties))
                if source == self.source
                    && valid == node.valid
                    && properties == node.properties =>
            {
                // The same node naming sources it did not name yet records them.
                if add_derived(self.tx, node)? {
                    self.added.push(Fact::Node(node.clone()));
                }
                return Ok((id, false));
            }
            Some((id, ..)) => {
                let sets = concat(&columns, |c| format!(", {c} = ?"));
                let update = format!(
                    "UPDATE {table} SET source = ?, valid = ?, recorded = ?{sets} WHERE id = ?"
                );
                let mut args: Vec<&dyn rusqlite::ToSql> =
                    vec![&self.source, &valid, &self.recorded];
                args.extend(values.iter().map(|v| v as &dyn rusqlite::ToSql));
                args.push(&id);
                self.tx.prepare_cached(&update)?.execute(&args[..])?;
                (id, false)
            }
            None => {
                let insert = format!(
                    "INSERT INTO {table}(key, source, valid, recorded{}) VALUES (?, ?, ?, ?{})",
                    concat(&columns, |c| format!(", {c}")),
                    ", ?".repeat(columns.len())
                );
                let mut args: Vec<&dyn rusqlite::ToSql> =
                    vec![&node.key, &self.source, &valid, &self.recorded];
                args.extend(values.iter().map(|v| v as &dyn rusqlite::ToSql));
                self.tx.prepare_cached(&insert)?.execute(&args[..])?;
                (self.tx.last_insert_rowid(), true)
            }
        };
        keep_derived(self.tx, node)?;
        self.added.push(Fact::Node(node.clone()));
        Ok((id, new))
    }

    /// Puts a link between two present nodes into its relation. Returns whether it changed.
    fn relate(
        &self,
        relation: &str,
        from: i64,
        to: i64,
        source: &str,
        valid: &ValidTime,
    ) -> Result<bool, Error> {
        let sql = format!(
            "INSERT INTO {}(src, dst, source, valid, recorded) VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(src, dst) DO UPDATE SET source = excluded.source, \
             valid = excluded.valid, recorded = excluded.recorded \
             WHERE source IS NOT excluded.source OR valid IS NOT excluded.valid",
            relation_table(relation)
        );
        let changed = self.tx.prepare_cached(&sql)?.execute(params![
            from,
            to,
            source,
            valid.to_sql(),
            self.recorded
        ])?;
        Ok(changed > 0)
    }

    /// Writes one link of a unit: into its relation when both ends are present, else open.
    fn link(&mut self, link: &LinkSpec) -> Result<(), Error> {
        let relation = self.ontology.relation(&link.relation)?;
        let fact = LinkFact {
            relation: relation.name.clone(),
            from: NodeRef::new(&relation.from, &link.from),
            to: NodeRef::new(&relation.to, &link.to),
            valid: link.valid.clone(),
        };
        let from = node_id(self.tx, &relation.from, &link.from)?;
        let to = node_id(self.tx, &relation.to, &link.to)?;
        if let (Some(from), Some(to)) = (from, to) {
            if self.relate(&relation.name, from, to, self.source, &link.valid)? {
                self.added.push(Fact::Link(fact));
            }
            return Ok(());
        }
        if self.open(&relation.name, &link.from, &link.to, &link.valid)? {
            self.added.push(Fact::Opened(fact));
        }
        Ok(())
    }

    /// Puts a link that waits for an end into `_open`. Returns whether it changed.
    fn open(
        &self,
        relation: &str,
        from_key: &str,
        to_key: &str,
        valid: &ValidTime,
    ) -> Result<bool, Error> {
        let changed = self
            .tx
            .prepare_cached(
                "INSERT INTO _open(rel, from_key, to_key, source, valid) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(rel, from_key, to_key) DO UPDATE SET \
                 source = excluded.source, valid = excluded.valid \
                 WHERE source IS NOT excluded.source OR valid IS NOT excluded.valid",
            )?
            .execute(params![
                relation,
                from_key,
                to_key,
                self.source,
                valid.to_sql()
            ])?;
        Ok(changed > 0)
    }

    /// Moves an open link, both of whose ends are now present, into its relation with the
    /// source it was opened with.
    fn close(&self, link: &LinkFact, from: i64, to: i64, source: &str) -> Result<(), Error> {
        self.relate(&link.relation, from, to, source, &link.valid)?;
        self.tx
            .prepare_cached("DELETE FROM _open WHERE rel = ?1 AND from_key = ?2 AND to_key = ?3")?
            .execute(params![link.relation, link.from.key, link.to.key])?;
        Ok(())
    }

    /// The ids of a link's two ends, both present.
    fn ends(&self, link: &LinkFact) -> Result<(i64, i64), Error> {
        let id = |end: &NodeRef| {
            node_id(self.tx, &end.class, &end.key)?.ok_or_else(|| Error::NotFound {
                class: end.class.clone(),
                key: end.key.clone(),
            })
        };
        Ok((id(&link.from)?, id(&link.to)?))
    }

    /// Closes every open link that was waiting for the node `key` of `class`, now present, and
    /// reports every open link that waits on `key` as a node of another kind, which stays open.
    fn arrived(&mut self, class: &str, key: &str) -> Result<(), Error> {
        for relation in self.ontology.relations.values() {
            for (waits_on, column) in [(&relation.to, "to_key"), (&relation.from, "from_key")] {
                let wrong_kind = waits_on != class;
                if wrong_kind && node_id(self.tx, waits_on, key)?.is_some() {
                    continue;
                }
                let waiting: Vec<(String, String, String, Option<String>)> = self
                    .tx
                    .prepare_cached(&format!(
                        "SELECT from_key, to_key, source, valid FROM _open \
                         WHERE rel = ?1 AND {column} = ?2"
                    ))?
                    .query_map(params![relation.name, key], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .collect::<Result<_, _>>()?;
                for (from_key, to_key, source, valid) in waiting {
                    if wrong_kind {
                        self.wrong_kind.push(WrongKind {
                            arrived: NodeRef::new(class, key),
                            waiting: LinkFact {
                                relation: relation.name.clone(),
                                from: NodeRef::new(&relation.from, from_key),
                                to: NodeRef::new(&relation.to, to_key),
                                valid: ValidTime::from_sql(valid),
                            },
                        });
                        continue;
                    }
                    let from = node_id(self.tx, &relation.from, &from_key)?;
                    let to = node_id(self.tx, &relation.to, &to_key)?;
                    let (Some(from), Some(to)) = (from, to) else {
                        continue;
                    };
                    let link = LinkFact {
                        relation: relation.name.clone(),
                        from: NodeRef::new(&relation.from, from_key),
                        to: NodeRef::new(&relation.to, to_key),
                        valid: ValidTime::from_sql(valid),
                    };
                    self.close(&link, from, to, &source)?;
                    self.closed.push(link);
                }
            }
        }
        Ok(())
    }
}

/// Applies one event's change to the state in `tx`, as it was applied when the event was
/// written: a declaration builds what it needs, a write puts what it added, a closing moves its
/// open link into its relation, a forget removes the node and its links, a replacement records
/// the node's fate. Facts are stamped as recorded at `recorded`.
pub(crate) fn replay(
    tx: &Connection,
    ontology: &Ontology,
    recorded: i64,
    change: &Change,
) -> Result<(), Error> {
    match change {
        Change::Declared(declaration) => {
            build(tx, ontology, declaration)?;
        }
        Change::Wrote { source, added, .. } => {
            let mut piece = Piece::new(tx, ontology, source, recorded);
            for fact in added {
                match fact {
                    Fact::Node(node) => {
                        piece.node(node)?;
                    }
                    Fact::Link(link) => {
                        let (from, to) = piece.ends(link)?;
                        piece.relate(&link.relation, from, to, source, &link.valid)?;
                    }
                    Fact::Opened(link) => {
                        piece.open(&link.relation, &link.from.key, &link.to.key, &link.valid)?;
                    }
                }
            }
        }
        Change::Closed(link) => {
            let source: String = tx
                .prepare_cached(
                    "SELECT source FROM _open WHERE rel = ?1 AND from_key = ?2 AND to_key = ?3",
                )?
                .query_row(params![link.relation, link.from.key, link.to.key], |row| {
                    row.get(0)
                })?;
            let piece = Piece::new(tx, ontology, &source, recorded);
            let (from, to) = piece.ends(link)?;
            piece.close(link, from, to, &source)?;
        }
        Change::Forgot { nodes, .. } => {
            for node in nodes {
                crate::events::remove(tx, ontology, node)?;
            }
        }
        Change::Replaced { class, old, new } => {
            record_fate(tx, class, old, new)?;
        }
    }
    Ok(())
}

/// Records that the node `old` of `class` was replaced by the node `new`. Replacing and
/// replaying a replacement both record through here.
fn record_fate(tx: &Connection, class: &str, old: &str, new: &str) -> Result<(), Error> {
    tx.prepare_cached(
        "INSERT INTO _fates(class, key, replaced_by) VALUES (?1, ?2, ?3) \
         ON CONFLICT(class, key) DO UPDATE SET replaced_by = excluded.replaced_by",
    )?
    .execute(params![class, old, new])?;
    Ok(())
}

impl Graph {
    /// Declares a class, a relation or a property on the graph's own stream, as one event. The
    /// event, what the declaration needs and its place in the ontology land together or not at
    /// all. A declaration identical to the ontology as it is now writes nothing and returns
    /// `Ok(None)`.
    ///
    /// # Errors
    /// [`Error::Refused`] when the declaration conflicts with the ontology, with a reason in
    /// ontology terms; the graph is left as it was.
    pub fn declare(&mut self, declaration: &Declaration) -> Result<Option<Event>, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ontology = Ontology::load(&tx)?;
        if !build(&tx, &ontology, declaration)? {
            let version = schema_version(&tx)?;
            drop(tx);
            self.loaded(version, ontology);
            return Ok(None);
        }
        let event = append(&tx, Change::Declared(declaration.clone()))?;
        let ontology = Ontology::load(&tx)?;
        let version = schema_version(&tx)?;
        tx.commit()?;
        self.loaded(version, ontology);
        Ok(Some(event))
    }

    /// Replaces the node `old` of `class` by the node `new` of the same class, under a different
    /// key, as one event naming the class, the old key and the new key. The old node stays in
    /// the graph and describes as replaced by the new one. The event and the replacement land
    /// together or not at all.
    ///
    /// # Errors
    /// `class` is not declared; [`Error::Refused`] when `old` and `new` are the same key;
    /// [`Error::NotFound`] when no node of `class` has key `old`, or key `new`; the graph file
    /// cannot be written.
    pub fn replace(&mut self, class: &str, old: &str, new: &str) -> Result<Event, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version = schema_version(&tx)?;
        let ontology = Ontology::load(&tx)?;
        let class = ontology.class(class)?.name.clone();
        if old == new {
            return Err(Error::Refused(format!(
                "replacing {class} {old} by itself is not a replacement; \
                 a new version under the same key is a write"
            )));
        }
        for key in [old, new] {
            if node_id(&tx, &class, key)?.is_none() {
                return Err(Error::NotFound {
                    class,
                    key: key.to_string(),
                });
            }
        }
        record_fate(&tx, &class, old, new)?;
        let event = append(
            &tx,
            Change::Replaced {
                class,
                old: old.to_string(),
                new: new.to_string(),
            },
        )?;
        tx.commit()?;
        self.loaded(version, ontology);
        Ok(event)
    }

    /// Writes whole units in pieces, each piece one transaction. Returns the events the pieces
    /// wrote: per piece, one for what it added and one per open link it closed. A unit that adds
    /// nothing new writes no event. A new node under the key an open link waits on, but of
    /// another kind, leaves the link open and is reported as a [`WrongKind`] on its piece's
    /// `Wrote` event, apart from what the piece added.
    ///
    /// # Errors
    /// [`WriteError`] holding [`Error::Refused`] and no events when a unit's node lacks a
    /// property its class declares, carries one it does not declare, or carries a value of
    /// another type than declared, or a float that is NaN or an infinity; when a unit's or a
    /// link's valid time is not a real date (a month outside 1 to 12, or a day past its month's
    /// end); when it names as its source an event the graph does not hold; or when a link's
    /// source is not of its relation's declared source class: the write is
    /// checked before any piece commits, so it lands nothing.
    /// [`WriteError`] when a piece fails: the pieces before it are kept, and the error holds
    /// their events.
    pub fn write(&mut self, write: &Write) -> Result<Vec<Event>, WriteError> {
        let mut events = Vec::new();
        // The version is read before the load: a declaration landing between them leaves the
        // cached version behind the ontology, which costs the next read a reload and nothing more.
        match schema_version(&self.conn).and_then(|v| Ok((v, Ontology::load(&self.conn)?))) {
            Ok((version, ontology)) => self.loaded(version, ontology),
            Err(cause) => return Err(WriteError { events, cause }),
        }
        if let Err(cause) = check(&self.conn, &self.ontology, write) {
            return Err(WriteError { events, cause });
        }
        for (i, piece) in pieces(&write.units).into_iter().enumerate() {
            if i > 0 {
                std::thread::sleep(BETWEEN_PIECES);
            }
            match self.piece(&write.source, &write.derived_from, piece) {
                Ok(written) => events.extend(written),
                Err(cause) => return Err(WriteError { events, cause }),
            }
        }
        Ok(events)
    }

    fn piece(
        &mut self,
        source: &str,
        derived_from: &[EventId],
        units: &[Unit],
    ) -> Result<Vec<Event>, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut piece = Piece::new(&tx, &self.ontology, source, now_millis());
        for unit in units {
            let node = NodeFact {
                derived_from: derived_from.to_vec(),
                ..unit.node.clone()
            };
            let (_, new) = piece.node(&node)?;
            if new {
                piece.arrived(&unit.node.class, &unit.node.key)?;
            }
            for link in &unit.links {
                piece.link(link)?;
            }
        }
        let Piece {
            added,
            wrong_kind,
            closed,
            ..
        } = piece;
        let mut events = Vec::new();
        if !added.is_empty() || !wrong_kind.is_empty() {
            events.push(append(
                &tx,
                Change::Wrote {
                    source: source.to_string(),
                    added,
                    wrong_kind,
                },
            )?);
        }
        for link in closed {
            events.push(append(&tx, Change::Closed(link))?);
        }
        tx.commit()?;
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Change, Class, Cursor, Date, Declaration, Error, Fact, Graph, NodeRef, Relation, Unit,
        ValidTime, Value, ValueType, Write, WriteError, open, rebuild,
    };

    /// A graph with class `item` holding nodes `x` and `y`.
    fn items() -> Result<Graph, Box<dyn std::error::Error>> {
        let path =
            std::env::temp_dir().join(format!("rannoch-replace-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path)?;
        g.declare(&Declaration::Class(Class::new("item", ["name"])))?;
        let when = ValidTime::On(Date::day(2026, 1, 1));
        let unit = |key: &str| Unit::new("item", key, [("name", key)], when.clone());
        g.write(&Write::new("toy").unit(unit("x")).unit(unit("y")))?;
        Ok(g)
    }

    fn log_len(g: &Graph) -> Result<usize, Error> {
        Ok(g.events().since(Cursor::START)?.len())
    }

    #[test]
    fn replacing_a_node_by_its_own_key_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = items()?;
        let before = log_len(&g)?;
        let refused = g.replace("item", "x", "x");
        assert!(
            matches!(&refused, Err(Error::Refused(reason)) if reason
                == "replacing item x by itself is not a replacement; \
                    a new version under the same key is a write"),
            "expected a refusal, got {refused:?}"
        );
        assert_eq!(log_len(&g)?, before);
        assert_eq!(g.describe(&NodeRef::new("item", "x"))?.replaced_by, None);
        Ok(())
    }

    #[test]
    fn replacing_a_key_that_is_not_a_node_is_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = items()?;
        let before = log_len(&g)?;
        let missing = g.replace("item", "nope1", "x");
        assert!(
            matches!(&missing, Err(Error::NotFound { class, key }) if class == "item" && key == "nope1"),
            "expected not found, got {missing:?}"
        );
        assert_eq!(log_len(&g)?, before);
        Ok(())
    }

    #[test]
    fn replacing_by_a_key_that_is_not_a_node_is_not_found() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut g = items()?;
        let before = log_len(&g)?;
        let missing = g.replace("item", "x", "nope2");
        assert!(
            matches!(&missing, Err(Error::NotFound { class, key }) if class == "item" && key == "nope2"),
            "expected not found, got {missing:?}"
        );
        assert_eq!(log_len(&g)?, before);
        assert_eq!(g.describe(&NodeRef::new("item", "x"))?.replaced_by, None);
        Ok(())
    }

    #[test]
    fn replacing_a_node_by_another_present_node_writes_one_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = items()?;
        let before = log_len(&g)?;
        let event = g.replace("item", "x", "y")?;
        assert_eq!(
            event.change,
            Change::Replaced {
                class: "item".into(),
                old: "x".into(),
                new: "y".into(),
            }
        );
        assert_eq!(log_len(&g)?, before + 1);
        assert_eq!(
            g.describe(&NodeRef::new("item", "x"))?.replaced_by,
            Some(NodeRef::new("item", "y"))
        );
        Ok(())
    }

    fn graph(name: &str) -> Result<Graph, Error> {
        let path = std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(path)?;
        g.declare(&Declaration::Class(Class::new("item", ["name"])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "part", "item", "item", false,
        )))?;
        Ok(g)
    }

    fn unit(key: &str, valid: Date) -> Unit {
        Unit::new("item", key, [("name", key)], ValidTime::On(valid))
    }

    /// Asserts the write is refused for `reason`, with no events, and the log is unchanged.
    fn assert_refused(g: &mut Graph, write: &Write, reason: &str) -> Result<(), Error> {
        let before = g.events().since(Cursor::START)?;
        let refused = g.write(write);
        assert!(
            matches!(&refused, Err(WriteError { events, cause: Error::Refused(why) })
                if events.is_empty() && why == reason),
            "expected a refusal for {reason}, got {refused:?}"
        );
        assert_eq!(g.events().since(Cursor::START)?, before);
        Ok(())
    }

    #[test]
    fn a_write_with_month_13_is_refused_and_lands_nothing() -> Result<(), Error> {
        let mut g = graph("month13")?;
        let write = Write::new("src")
            .unit(unit("a", Date::day(2020, 1, 1)))
            .unit(unit("b", Date::month(2020, 13)));
        assert_refused(
            &mut g,
            &write,
            "valid time 2020-13 is not a date: month 13 is out of range",
        )?;
        assert_eq!(g.find("item", "a")?, None);
        Ok(())
    }

    #[test]
    fn a_write_with_a_day_past_its_month_is_refused_and_a_leap_day_lands()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("leap")?;
        let write = Write::new("src").unit(unit("a", Date::day(2021, 2, 29)));
        assert_refused(
            &mut g,
            &write,
            "valid time 2021-02-29 is not a date: day 29 is out of range",
        )?;
        let events = g.write(&Write::new("src").unit(unit("a", Date::day(2020, 2, 29))))?;
        assert_eq!(events.len(), 1);
        Ok(())
    }

    #[test]
    fn a_link_with_a_day_past_its_month_is_refused() -> Result<(), Error> {
        let mut g = graph("link")?;
        let when = Date::day(2021, 4, 1);
        let write = Write::new("src")
            .unit(unit("a", when.clone()))
            .unit(unit("b", when).link("part", "b", "a", ValidTime::On(Date::day(2021, 4, 31))));
        assert_refused(
            &mut g,
            &write,
            "valid time 2021-04-31 is not a date: day 31 is out of range",
        )
    }

    #[test]
    fn parts_reads_a_month_as_its_first_day() -> Result<(), Error> {
        assert_eq!(Date::month(2020, 1).parts()?, (2020, 1, 1));
        assert_eq!(Date::day(2000, 2, 29).parts()?, (2000, 2, 29));
        assert!(matches!(
            Date::day(1900, 2, 29).parts(),
            Err(Error::Refused(why)) if why == "valid time 1900-02-29 is not a date: day 29 is out of range"
        ));
        assert!(matches!(
            Date::month(2020, 0).parts(),
            Err(Error::Refused(_))
        ));
        Ok(())
    }

    #[test]
    fn a_nan_or_an_infinity_is_refused_when_it_is_written() -> Result<(), Error> {
        let mut g = graph("non-finite")?;
        for (property, of) in [
            ("score", ValueType::Float),
            ("at", ValueType::FloatVector(2)),
        ] {
            g.declare(&Declaration::Property {
                class: "item".to_string(),
                property: property.to_string(),
                of,
            })?;
        }
        let item = |score: f64, at: Vec<f64>| {
            Unit::new(
                "item",
                "a",
                [
                    ("name", Value::from("a")),
                    ("score", Value::Float(score)),
                    ("at", Value::FloatVector(at)),
                ],
                ValidTime::Unknown,
            )
        };
        for (score, at) in [
            (f64::NAN, vec![0.0, 0.0]),
            (f64::INFINITY, vec![0.0, 0.0]),
            (f64::NEG_INFINITY, vec![0.0, 0.0]),
            (1.0, vec![0.0, f64::NAN]),
            (1.0, vec![f64::INFINITY, 0.0]),
        ] {
            let before = g.events().since(Cursor::START)?;
            let refused = g.write(&Write::new("src").unit(item(score, at.clone())));
            assert!(
                matches!(&refused, Err(WriteError { events, cause: Error::Refused(_) })
                    if events.is_empty()),
                "{score} and {at:?}: {refused:?}"
            );
            assert_eq!(g.events().since(Cursor::START)?, before);
        }
        g.write(&Write::new("src").unit(item(1.5, vec![0.5, -0.5])))
            .map_err(|e| e.cause)?;
        Ok(())
    }

    #[test]
    fn a_property_declared_later_carries_its_value_type() -> Result<(), Error> {
        let mut g = graph("typed-later")?;
        g.declare(&Declaration::Property {
            class: "item".to_string(),
            property: "count".to_string(),
            of: ValueType::Integer,
        })?;
        let item = |count: Value| {
            Unit::new(
                "item",
                "a",
                [("name", Value::from("a")), ("count", count)],
                ValidTime::Unknown,
            )
        };
        assert!(matches!(
            g.write(&Write::new("src").unit(item(Value::from("7")))),
            Err(WriteError {
                cause: Error::Refused(_),
                ..
            })
        ));
        g.write(&Write::new("src").unit(item(Value::Integer(9_007_199_254_740_993))))
            .map_err(|e| e.cause)?;
        assert_eq!(
            g.find("item", "a")?
                .and_then(|n| n.properties.get("count").cloned()),
            Some(Value::Integer(9_007_199_254_740_993))
        );
        assert!(matches!(
            g.declare(&Declaration::Property {
                class: "item".to_string(),
                property: "count".to_string(),
                of: ValueType::Text,
            }),
            Err(Error::Refused(_))
        ));
        Ok(())
    }

    #[test]
    fn a_stored_out_of_range_date_still_rebuilds() -> Result<(), Box<dyn std::error::Error>> {
        let mut g = graph("rebuild")?;
        g.write(&Write::new("src").unit(unit("a", Date::day(2020, 1, 1))))?;
        let mut events = g.events().since(Cursor::START)?;
        for event in &mut events {
            if let Change::Wrote { added, .. } = &mut event.change {
                for fact in added {
                    if let Fact::Node(node) = fact {
                        node.valid = ValidTime::On(Date::month(2020, 13));
                    }
                }
            }
        }
        let path =
            std::env::temp_dir().join(format!("rannoch-rebuilt-{}.db", uuid::Uuid::now_v7()));
        let rebuilt = rebuild(&events, path)?;
        assert_eq!(
            rebuilt.find("item", "a")?.map(|n| n.valid),
            Some(ValidTime::On(Date::month(2020, 13)))
        );
        Ok(())
    }
}
