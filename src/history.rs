//! Hearings: every write is a hearing of a node or a link, at a point on the time axis. A
//! hearing's identity is its content, hashed under the app's key: identical content is one
//! hearing, and an identical event that arrives from another store under its own id is held as an
//! alias of it. The key never reaches the store's files; the store keeps only a check value.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::events::{Change, Cursor, EventId, Events, Fact, LinkFact, NodeFact};
use crate::open::{CLASS, Graph, RELATION};
use crate::read::NodeRef;
use crate::write::{Error, ValidTime};

/// The hearings, the other ids each arrived under, and the check value of the graph's key.
pub(crate) const HISTORY_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _hearings(
        identity BLOB PRIMARY KEY, entity TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT,
        event TEXT NOT NULL, seq INTEGER NOT NULL, source TEXT NOT NULL, valid TEXT,
        point INTEGER NOT NULL, integer INTEGER) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _hearings_of ON _hearings(entity, key, to_key);
    CREATE TABLE IF NOT EXISTS _aliases(
        identity BLOB NOT NULL, alias TEXT NOT NULL, PRIMARY KEY(identity, alias)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _check(value TEXT NOT NULL);";

/// The fixed label whose keyed hash is the check value.
const CHECK_LABEL: &[u8] = b"rannoch content-identity check";

/// The app's content-identity key, held in memory only and never printed.
#[derive(Clone)]
pub(crate) struct Key {
    bytes: [u8; 32],
    /// The keyed hash of a fixed label: what the store and its events keep in the key's place.
    pub(crate) check: String,
}

impl Key {
    pub(crate) fn new(bytes: &[u8; 32]) -> Key {
        Key {
            bytes: *bytes,
            check: blake3::keyed_hash(bytes, CHECK_LABEL).to_hex().to_string(),
        }
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Key(..)")
    }
}

/// What a hearing is of, as an event holds it.
pub(crate) enum Of<'a> {
    Node(&'a NodeFact),
    Link(&'a LinkFact),
}

impl Of<'_> {
    /// Its entity (a class or a relation), its key and, for a link, its target's key.
    fn entity(&self) -> (String, &str, Option<&str>) {
        match self {
            Of::Node(node) => (node_entity(&node.class), &node.key, None),
            Of::Link(link) => (
                link_entity(&link.relation),
                &link.from.key,
                Some(&link.to.key),
            ),
        }
    }

    fn valid(&self) -> &ValidTime {
        match self {
            Of::Node(node) => &node.valid,
            Of::Link(link) => &link.valid,
        }
    }

    fn time(&self) -> Option<i64> {
        match self {
            Of::Node(node) => node.time,
            Of::Link(_) => None,
        }
    }

    fn integer(&self) -> Option<i64> {
        match self {
            Of::Node(node) => node.integer,
            Of::Link(_) => None,
        }
    }

    /// Its identity: its content heard from `source`, hashed under `key`. A point rannoch
    /// assigns is not content; a time or an integer key the writer supplied is.
    fn identity(&self, key: &Key, source: &str) -> Result<[u8; 32], Error> {
        let content = match self {
            Of::Node(n) => serde_json::to_vec(&(
                "node",
                &n.class,
                &n.key,
                &n.valid,
                source,
                &n.properties,
                n.time,
                n.integer,
            ))?,
            Of::Link(l) => {
                serde_json::to_vec(&("link", &l.relation, &l.from, &l.to, &l.valid, source))?
            }
        };
        Ok(*blake3::keyed_hash(&key.bytes, &content).as_bytes())
    }
}

fn node_entity(class: &str) -> String {
    format!("{CLASS}:{class}")
}

fn link_entity(relation: &str) -> String {
    format!("{RELATION}:{relation}")
}

/// A hearing the graph holds, identical to one that arrived.
pub(crate) struct Held {
    identity: [u8; 32],
    event: EventId,
    /// The place in the log of the event it is named by.
    pub(crate) cursor: Cursor,
    time: Option<i64>,
}

/// Records what `of` heard from `source` as a new hearing named by `event`, at `cursor` in the
/// log, and returns `None`; or, when the graph holds a hearing identical to it, records nothing
/// and returns that one.
pub(crate) fn hear(
    tx: &Connection,
    key: &Key,
    event: EventId,
    cursor: Cursor,
    source: &str,
    of: &Of,
) -> Result<Option<Held>, Error> {
    let identity = of.identity(key, source)?;
    let held = tx
        .prepare_cached("SELECT event, seq FROM _hearings WHERE identity = ?1")?
        .query_row(params![identity], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .optional()?;
    if let Some((held, seq)) = held {
        return Ok(Some(Held {
            identity,
            event: serde_json::from_value(held.into())?,
            cursor: Cursor(seq),
            time: of.time(),
        }));
    }
    let (entity, from, to) = of.entity();
    tx.prepare_cached(
        "INSERT INTO _hearings(identity, entity, key, to_key, event, seq, source, valid, point, \
         integer) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?
    .execute(params![
        identity,
        entity,
        from,
        to,
        event.to_string(),
        cursor.0,
        source,
        of.valid().to_sql(),
        of.time().unwrap_or(event.millis()),
        of.integer()
    ])?;
    Ok(None)
}

/// Holds `event`, at `cursor`, as an alias of `held`, whose content it arrived with. When
/// `event` is the smaller id the hearing takes its name and its point, the id it was named by
/// becomes the alias, and this returns true.
pub(crate) fn alias(
    tx: &Connection,
    held: &Held,
    event: EventId,
    cursor: Cursor,
) -> Result<bool, Error> {
    if event == held.event {
        return Ok(false);
    }
    let renamed = event < held.event;
    if renamed {
        tx.prepare_cached(
            "UPDATE _hearings SET event = ?2, seq = ?3, point = ?4 WHERE identity = ?1",
        )?
        .execute(params![
            held.identity,
            event.to_string(),
            cursor.0,
            held.time.unwrap_or(event.millis())
        ])?;
    }
    let alias = if renamed { held.event } else { event };
    tx.prepare_cached("INSERT OR IGNORE INTO _aliases(identity, alias) VALUES (?1, ?2)")?
        .execute(params![held.identity, alias.to_string()])?;
    Ok(renamed)
}

/// Drops the hearings of `node`, which left live state, and of `links`, which left with it.
pub(crate) fn unhear(tx: &Connection, node: &NodeRef, links: &[LinkFact]) -> Result<(), Error> {
    let drop = |entity: String, key: &str, to: Option<&str>| -> Result<(), Error> {
        let of = "SELECT identity FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS ?3";
        tx.prepare_cached(&format!("DELETE FROM _aliases WHERE identity IN ({of})"))?
            .execute(params![entity, key, to])?;
        tx.prepare_cached("DELETE FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS ?3")?
            .execute(params![entity, key, to])?;
        Ok(())
    };
    drop(node_entity(&node.class), &node.key, None)?;
    for link in links {
        drop(
            link_entity(&link.relation),
            &link.from.key,
            Some(&link.to.key),
        )?;
    }
    Ok(())
}

/// Takes `key` as the graph's. A store holding another key's check value refuses it; a store
/// holding none takes it and hears every write already in its log, in order, dropping what each
/// forget in it removed.
pub(crate) fn take(conn: &mut Connection, key: &Key) -> Result<(), Error> {
    let check = |conn: &Connection| -> Result<Option<String>, Error> {
        Ok(conn
            .prepare_cached("SELECT value FROM _check")?
            .query_row([], |row| row.get(0))
            .optional()?)
    };
    let same = |held: String| {
        if held == key.check {
            Ok(())
        } else {
            Err(Error::KeyDiffers)
        }
    };
    if let Some(held) = check(conn)? {
        return same(held);
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(held) = check(&tx)? {
        return same(held);
    }
    tx.execute("INSERT INTO _check(value) VALUES (?1)", params![key.check])?;
    for event in (Events { conn: &tx }).since(Cursor::START)? {
        let (source, added) = match &event.change {
            Change::Wrote { source, added, .. } => (source, added),
            Change::Forgot { nodes, links, .. } => {
                for node in nodes {
                    unhear(&tx, node, links)?;
                }
                continue;
            }
            _ => continue,
        };
        for fact in added {
            let of = match fact {
                Fact::Node(node) => Of::Node(node),
                Fact::Link(link) | Fact::Opened(link) => Of::Link(link),
            };
            if let Some(held) = hear(&tx, key, event.id, event.cursor, source, &of)? {
                alias(&tx, &held, event.id, event.cursor)?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}

/// Milliseconds since the Unix epoch, negative before it.
pub(crate) fn millis(time: SystemTime) -> i64 {
    let ms = |d: Duration| i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => ms(after),
        Err(before) => -ms(before.duration()),
    }
}

fn time(millis: i64) -> SystemTime {
    let span = Duration::from_millis(millis.unsigned_abs());
    if millis < 0 {
        UNIX_EPOCH - span
    } else {
        UNIX_EPOCH + span
    }
}

/// One hearing of a node or a link: one account of it, from one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hearing {
    /// The event it is named by: the smallest id it arrived under.
    pub event: EventId,
    pub source: String,
    pub valid: ValidTime,
    /// Its point on the time axis: the time its writer supplied, or else the record time of
    /// the store that first recorded it, which its event's id carries.
    pub point: SystemTime,
    /// The integer key its writer supplied.
    pub integer: Option<i64>,
    /// Its content identity: its content hashed under the graph's key.
    pub identity: [u8; 32],
    /// Every other event id it arrived under.
    pub aliases: Vec<EventId>,
}

impl Graph {
    /// Every hearing of `node`, by point and then by event id.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn hearings(&self, node: &NodeRef) -> Result<Vec<Hearing>, Error> {
        let class = self.current_ontology()?.class(&node.class)?.name.clone();
        self.heard(&node_entity(&class), &node.key, None)
    }

    /// Every hearing of the link of `relation` from the node keyed `from` to the node keyed
    /// `to`, by point and then by event id.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn link_hearings(
        &self,
        relation: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<Hearing>, Error> {
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        self.heard(&link_entity(&relation), from, Some(to))
    }

    fn heard(&self, entity: &str, key: &str, to: Option<&str>) -> Result<Vec<Hearing>, Error> {
        /// A hearing's row: identity, event, source, valid time, point and integer key.
        type Row = ([u8; 32], String, String, Option<String>, i64, Option<i64>);
        let rows: Vec<Row> = self
            .conn
            .prepare_cached(
                "SELECT identity, event, source, valid, point, integer FROM _hearings \
                 WHERE entity = ?1 AND key = ?2 AND to_key IS ?3 ORDER BY point, event",
            )?
            .query_map(params![entity, key, to], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .collect::<Result<_, _>>()?;
        let mut hearings = Vec::new();
        for (identity, event, source, valid, point, integer) in rows {
            let aliases: Vec<String> = self
                .conn
                .prepare_cached("SELECT alias FROM _aliases WHERE identity = ?1 ORDER BY alias")?
                .query_map(params![identity], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            hearings.push(Hearing {
                event: serde_json::from_value(event.into())?,
                source,
                valid: ValidTime::from_sql(valid),
                point: time(point),
                integer,
                identity,
                aliases: aliases
                    .into_iter()
                    .map(|alias| serde_json::from_value(alias.into()))
                    .collect::<Result<_, _>>()?,
            });
        }
        Ok(hearings)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Class, Date, Declaration, NodeRef, Relation, Unit, ValidTime, Write, open};

    #[test]
    fn a_pre_key_store_takes_its_key_without_the_hearings_a_forget_removed()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-prekey-{}.db", uuid::Uuid::now_v7()));
        let when = ValidTime::On(Date::day(1975, 2, 10));
        let bare =
            |class: &str, key: &str| Unit::new(class, key, [] as [(&str, &str); 0], when.clone());
        let ana = || Unit::new("person", "p01", [("name", "Ana")], when.clone());
        {
            let mut g = open(&path, Some(&crate::TEST_KEY))?;
            g.declare(&Declaration::Class(Class::new("person", ["name"])))?;
            g.declare(&Declaration::Class(Class::new("visitor", [] as [&str; 0])))?;
            g.declare(&Declaration::Class(Class::new("place", [] as [&str; 0])))?;
            g.declare(&Declaration::Relation(Relation::new(
                "visited", "visitor", "place", false,
            )))?;
            g.write(&Write::new("tree").unit(bare("place", "pl1")))?;
            g.write(&Write::new("tree").unit(ana()))?;
            g.write(&Write::new("tree").unit(bare("visitor", "p05").link(
                "visited",
                "p05",
                "pl1",
                when.clone(),
            )))?;
            g.forget(&NodeRef::new("visitor", "p05"), "living", "tree")?;
            // As a store written before keys: no check value and no hearings.
            g.conn.execute_batch(
                "DELETE FROM _check; DELETE FROM _hearings; DELETE FROM _aliases;",
            )?;
        }

        let mut g = open(&path, Some(&crate::TEST_KEY))?;
        let forgotten = NodeRef::new("visitor", "p05");
        assert_eq!(
            g.hearings(&forgotten)?,
            vec![],
            "the forgotten node keeps no hearing"
        );
        assert_eq!(
            g.link_hearings("visited", "p05", "pl1")?,
            vec![],
            "the links the forget took keep no hearing"
        );
        assert_eq!(g.hearings(&NodeRef::new("person", "p01"))?.len(), 1);
        g.write(&Write::new("tree").unit(ana()))?;
        assert_eq!(g.hearings(&NodeRef::new("person", "p01"))?.len(), 1);
        g.write(&Write::new("tree").unit(bare("visitor", "p05")))?;
        assert_eq!(g.hearings(&forgotten)?.len(), 1);
        Ok(())
    }
}
