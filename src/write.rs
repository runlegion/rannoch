//! Declarations and writes. A declaration is one event; a write is whole units committed in
//! pieces, each piece one transaction whose events land only if it lands.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::events::{Change, Event, Fact, LinkFact, NodeFact, WrongKind, append};
use crate::open::{
    CLASS, Class, Graph, Ontology, RELATION, Relation, class_table, concat, property_column,
    relation_index, relation_table,
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
    /// A property added to a declared class.
    Property {
        class: String,
        property: String,
    },
}

fn list(names: &BTreeSet<String>) -> String {
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
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
fn build(tx: &Connection, ontology: &Ontology, declaration: &Declaration) -> Result<bool, Error> {
    match declaration {
        Declaration::Class(class) => {
            if let Some(have) = ontology.classes.get(&class.name) {
                if have.properties == class.properties {
                    return Ok(false);
                }
                return Err(Error::Refused(format!(
                    "class {} is already declared with properties {}; this declaration has {}",
                    class.name,
                    list(&have.properties),
                    list(&class.properties)
                )));
            }
            let columns = concat(&class.properties, |p| {
                format!(", {} TEXT", property_column(p))
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
        Declaration::Property { class, property } => {
            let have = ontology.class(class)?;
            if have.properties.contains(property) {
                return Ok(false);
            }
            tx.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {} TEXT;",
                class_table(class),
                property_column(property)
            ))?;
            let mut grown = have.clone();
            grown.properties.insert(property.clone());
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
    pub fn new<K: Into<String>, V: Into<String>>(
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

/// A write: whole units from one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    source: String,
    units: Vec<Unit>,
}

impl Write {
    pub fn new(source: impl Into<String>) -> Self {
        Write {
            source: source.into(),
            units: Vec::new(),
        }
    }

    #[must_use]
    pub fn unit(mut self, unit: Unit) -> Self {
        self.units.push(unit);
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

fn now_millis() -> i64 {
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

/// Checks a whole write before any piece commits: refuses it when one of its links has a source
/// that is not of its relation's declared source class. A target is named by kind and key, so a
/// target key not yet a node of the declared target class leaves the link open.
fn check(conn: &Connection, ontology: &Ontology, write: &Write) -> Result<(), Error> {
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

impl Piece<'_> {
    /// Writes a node, or a new version of it. Returns the node's id and whether it is new.
    fn node(&mut self, node: &NodeFact) -> Result<(i64, bool), Error> {
        let class = self.ontology.class(&node.class)?;
        if let Some(unknown) = node
            .properties
            .keys()
            .find(|p| !class.properties.contains(*p))
        {
            return Err(Error::Refused(format!(
                "class {} has no property {unknown}",
                class.name
            )));
        }
        let table = class_table(&class.name);
        let columns: Vec<String> = class
            .properties
            .iter()
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
                let mut properties = BTreeMap::new();
                for (i, name) in class.properties.iter().enumerate() {
                    if let Some(value) = row.get::<_, Option<String>>(3 + i)? {
                        properties.insert(name.clone(), value);
                    }
                }
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    ValidTime::from_sql(row.get(2)?),
                    properties,
                ))
            })
            .optional()?;
        let valid = node.valid.to_sql();
        let values: Vec<Option<&str>> = class
            .properties
            .iter()
            .map(|p| node.properties.get(p).map(String::as_str))
            .collect();
        let (id, new) = match have {
            Some((id, source, valid, properties))
                if source == self.source
                    && valid == node.valid
                    && properties == node.properties =>
            {
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
                relation.name,
                link.from,
                link.to,
                self.source,
                link.valid.to_sql()
            ])?;
        if changed > 0 {
            self.added.push(Fact::Opened(fact));
        }
        Ok(())
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
                    let valid = ValidTime::from_sql(valid);
                    self.relate(&relation.name, from, to, &source, &valid)?;
                    self.tx
                        .prepare_cached(
                            "DELETE FROM _open WHERE rel = ?1 AND from_key = ?2 AND to_key = ?3",
                        )?
                        .execute(params![relation.name, from_key, to_key])?;
                    self.closed.push(LinkFact {
                        relation: relation.name.clone(),
                        from: NodeRef::new(&relation.from, from_key),
                        to: NodeRef::new(&relation.to, to_key),
                        valid,
                    });
                }
            }
        }
        Ok(())
    }
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
            self.ontology = ontology;
            return Ok(None);
        }
        let event = append(&tx, Change::Declared(declaration.clone()))?;
        self.ontology = Ontology::load(&tx)?;
        tx.commit()?;
        Ok(Some(event))
    }

    /// Writes whole units in pieces, each piece one transaction. Returns the events the pieces
    /// wrote: per piece, one for what it added and one per open link it closed. A unit that adds
    /// nothing new writes no event. A new node under the key an open link waits on, but of
    /// another kind, leaves the link open and is reported as a [`WrongKind`] on its piece's
    /// `Wrote` event, apart from what the piece added.
    ///
    /// # Errors
    /// [`WriteError`] holding [`Error::Refused`] and no events when a link's source is not of its
    /// relation's declared source class: the write is checked before any piece commits, so it
    /// lands nothing.
    /// [`WriteError`] when a piece fails: the pieces before it are kept, and the error holds
    /// their events.
    pub fn write(&mut self, write: &Write) -> Result<Vec<Event>, WriteError> {
        let mut events = Vec::new();
        match Ontology::load(&self.conn) {
            Ok(ontology) => self.ontology = ontology,
            Err(cause) => return Err(WriteError { events, cause }),
        }
        if let Err(cause) = check(&self.conn, &self.ontology, write) {
            return Err(WriteError { events, cause });
        }
        for (i, piece) in pieces(&write.units).into_iter().enumerate() {
            if i > 0 {
                std::thread::sleep(BETWEEN_PIECES);
            }
            match self.piece(&write.source, piece) {
                Ok(written) => events.extend(written),
                Err(cause) => return Err(WriteError { events, cause }),
            }
        }
        Ok(events)
    }

    fn piece(&mut self, source: &str, units: &[Unit]) -> Result<Vec<Event>, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut piece = Piece {
            tx: &tx,
            ontology: &self.ontology,
            source,
            recorded: now_millis(),
            added: Vec::new(),
            wrong_kind: Vec::new(),
            closed: Vec::new(),
        };
        for unit in units {
            let (_, new) = piece.node(&unit.node)?;
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
