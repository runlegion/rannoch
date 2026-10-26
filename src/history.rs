//! Hearings: every write is a hearing of a node or a link, at a point on the time axis. A
//! hearing's identity is its content, hashed under the app's key: identical content is one
//! hearing, and an identical event that arrives from another store under its own id is held as an
//! alias of it. The key never reaches the store's files; the store keeps only a check value.
//! A link that ends records its ending, at its own point, beside its hearings; a link found
//! wrong records a correction, which is not an ending.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::events::{Change, Cursor, EventId, Events, Fact, LinkFact, NodeFact};
use crate::open::{CLASS, Graph, HistoryKey, Ontology, RELATION, Value};
use crate::read::NodeRef;
use crate::write::{Error, Stamp, ValidTime};

/// The hearings, one version row each, with no valid-to: `arrived` counts up as this store
/// records them, `integer` is set only in an integer-keyed class, and `properties` holds a
/// node's. Then the other ids each arrived under, how each link that ended or was found wrong
/// came to that (`point` is the ending's, and null for a correction), and the check value of
/// the graph's key.
pub(crate) const HISTORY_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _hearings(
        arrived INTEGER PRIMARY KEY, identity BLOB NOT NULL UNIQUE, entity TEXT NOT NULL,
        key TEXT NOT NULL, to_key TEXT, event TEXT NOT NULL, seq INTEGER NOT NULL,
        source TEXT NOT NULL, valid TEXT, point INTEGER NOT NULL, integer INTEGER,
        recorded INTEGER NOT NULL, properties TEXT);
    CREATE INDEX IF NOT EXISTS _hearings_at
        ON _hearings(entity, key, to_key, integer, point, event);
    CREATE TABLE IF NOT EXISTS _aliases(
        identity BLOB NOT NULL, alias TEXT NOT NULL, PRIMARY KEY(identity, alias)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _ends(
        entity TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT NOT NULL, event TEXT NOT NULL,
        source TEXT NOT NULL, point INTEGER, PRIMARY KEY(entity, key, to_key)) WITHOUT ROWID;
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
    pub(crate) fn identity(&self, key: &Key, source: &str) -> Result<[u8; 32], Error> {
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

/// Whether what `of` is a hearing of orders its history by an integer key.
fn by_integer(ontology: &Ontology, of: &Of) -> Result<bool, Error> {
    Ok(match of {
        Of::Node(node) => ontology.class(&node.class)?.history == HistoryKey::Integer,
        Of::Link(_) => false,
    })
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

/// Records what `of` heard from `source`, whose content identity is `identity`, as a new version
/// row named by `stamp`'s event and
/// returns `None`; or, when the graph holds a hearing identical to it, records nothing and
/// returns that one. Its integer key is kept only when its class is ordered by one, so every
/// other class's rows order by point alone.
pub(crate) fn hear(
    tx: &Connection,
    identity: [u8; 32],
    stamp: &Stamp,
    source: &str,
    of: &Of,
    ontology: &Ontology,
) -> Result<Option<Held>, Error> {
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
    let properties = match of {
        Of::Node(node) => Some(serde_json::to_string(&node.properties)?),
        Of::Link(_) => None,
    };
    tx.prepare_cached(
        "INSERT INTO _hearings(identity, entity, key, to_key, event, seq, source, valid, point, \
         integer, recorded, properties) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
    )?
    .execute(params![
        identity,
        entity,
        from,
        to,
        stamp.id.to_string(),
        stamp.cursor.0,
        source,
        of.valid().to_sql(),
        of.time().unwrap_or(stamp.id.millis()),
        if by_integer(ontology, of)? {
            of.integer()
        } else {
            None
        },
        stamp.recorded,
        properties
    ])?;
    Ok(None)
}

/// The newest version of what `of` is a hearing of, by its class's order: its identity, source,
/// valid time, record time and properties.
pub(crate) fn newest(tx: &Connection, of: &Of) -> Result<Option<Newest>, Error> {
    let (entity, key, to) = of.entity();
    Ok(tx
        .prepare_cached(
            "SELECT identity, source, valid, recorded, properties FROM _hearings \
             WHERE entity = ?1 AND key = ?2 AND to_key IS ?3 \
             ORDER BY integer DESC, point DESC, event DESC LIMIT 1",
        )?
        .query_row(params![entity, key, to], |row| {
            Ok(Newest {
                identity: row.get(0)?,
                source: row.get(1)?,
                valid: ValidTime::from_sql(row.get(2)?),
                recorded: row.get(3)?,
                properties: row.get(4)?,
            })
        })
        .optional()?)
}

/// A node's or a link's newest version, as [`newest`] reads it.
pub(crate) struct Newest {
    pub(crate) identity: [u8; 32],
    pub(crate) source: String,
    pub(crate) valid: ValidTime,
    pub(crate) recorded: i64,
    properties: Option<String>,
}

impl Newest {
    pub(crate) fn properties(&self) -> Result<BTreeMap<String, Value>, Error> {
        Ok(serde_json::from_str(
            self.properties.as_deref().unwrap_or("{}"),
        )?)
    }
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
        let entity = link_entity(&link.relation);
        tx.prepare_cached("DELETE FROM _ends WHERE entity = ?1 AND key = ?2 AND to_key = ?3")?
            .execute(params![entity, link.from.key, link.to.key])?;
        drop(entity, &link.from.key, Some(&link.to.key))?;
    }
    Ok(())
}

/// Whether the graph holds a hearing of the link of `relation` from `from` to `to`.
pub(crate) fn link_heard(
    tx: &Connection,
    relation: &str,
    from: &str,
    to: &str,
) -> Result<bool, Error> {
    Ok(tx
        .prepare_cached(
            "SELECT 1 FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key = ?3 LIMIT 1",
        )?
        .query_row(params![link_entity(relation), from, to], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Records how a link came to an end, when `change` is an ending (at its point) or a correction
/// (with none), as the event `event`; any other change records nothing. The latest replaces any
/// before it. Ending, correcting and replaying either record through here.
pub(crate) fn conclude(tx: &Connection, event: EventId, change: &Change) -> Result<(), Error> {
    let (source, relation, from, to, at) = match change {
        Change::Ended {
            source,
            relation,
            from,
            to,
            at,
        } => (source, relation, from, to, Some(*at)),
        Change::Corrected {
            source,
            relation,
            from,
            to,
        } => (source, relation, from, to, None),
        _ => return Ok(()),
    };
    tx.prepare_cached(
        "INSERT INTO _ends(entity, key, to_key, event, source, point) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(entity, key, to_key) DO UPDATE SET \
         event = excluded.event, source = excluded.source, point = excluded.point",
    )?
    .execute(params![
        link_entity(relation),
        from.key,
        to.key,
        event.to_string(),
        source,
        at
    ])?;
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
    let ontology = Ontology::load(&tx)?;
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
        let stamp = Stamp {
            id: event.id,
            cursor: event.cursor,
            recorded: event.id.millis(),
            arrived: true,
        };
        for fact in added {
            let of = match fact {
                Fact::Node(node) => Of::Node(node),
                Fact::Link(link) | Fact::Opened(link) => Of::Link(link),
            };
            let identity = of.identity(key, source)?;
            if let Some(held) = hear(&tx, identity, &stamp, source, &of, &ontology)? {
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
    /// The integer key its writer supplied, in an integer-keyed class.
    pub integer: Option<i64>,
    /// Its content identity: its content hashed under the graph's key.
    pub identity: [u8; 32],
    /// Every other event id it arrived under.
    pub aliases: Vec<EventId>,
}

/// How a link came to an end: it ended, a new fact with its own point, or it was found wrong,
/// which is a correction and not an ending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum End {
    Ended {
        /// The ending's point on the time axis.
        at: SystemTime,
        source: String,
        event: EventId,
    },
    Corrected {
        source: String,
        event: EventId,
    },
}

impl Graph {
    /// How the link of `relation` from the node keyed `from` to the node keyed `to` came to an
    /// end, or `None` while it has neither ended nor been found wrong. Its hearings stay in
    /// [`Graph::link_hearings`] either way.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn link_end(&self, relation: &str, from: &str, to: &str) -> Result<Option<End>, Error> {
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        let row: Option<(String, String, Option<i64>)> = self
            .conn
            .prepare_cached(
                "SELECT event, source, point FROM _ends WHERE entity = ?1 AND key = ?2 AND to_key = ?3",
            )?
            .query_row(params![link_entity(&relation), from, to], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()?;
        let Some((event, source, at)) = row else {
            return Ok(None);
        };
        let event = serde_json::from_value(event.into())?;
        Ok(Some(match at {
            Some(at) => End::Ended {
                at: time(at),
                source,
                event,
            },
            None => End::Corrected { source, event },
        }))
    }

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

/// A point to read a node's or a link's state at: a time on the axis, or, in an integer-keyed
/// class, an integer key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum At {
    Time(SystemTime),
    Key(i64),
}

/// One version of a node or a link: the hearing that held at a point, by its class's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    /// The event it is named by: the smallest id it arrived under.
    pub event: EventId,
    pub source: String,
    pub valid: ValidTime,
    /// Its point on the time axis.
    pub point: SystemTime,
    /// Its integer key, in an integer-keyed class.
    pub integer: Option<i64>,
    /// A node's properties as that version holds them; a link has none.
    pub properties: BTreeMap<String, Value>,
    /// Whether a link had ended by the point read; a node never has.
    pub ended: bool,
}

/// When a node was heard: its first and last versions on its class's axis, the one this store
/// recorded first, and the earliest valid time it has been given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub first_heard: Version,
    pub last_heard: Version,
    /// Per store: another store given the same events in another order may name another.
    pub first_recorded: Version,
    pub first_true: ValidTime,
}

/// Picks the newest version row at or before the point `?4`, in a class not ordered by an
/// integer key (and for a link, which is never ordered by one).
const BY_POINT: &str =
    "AND integer IS NULL AND point <= ?4 ORDER BY point DESC, event DESC LIMIT 1";

/// Reads one version row of `entity` keyed `key` (and `to`, for a link), as `tail` picks it,
/// with `at` bound as `?4` when the tail names it: the version and when this store recorded it.
fn version_row(
    conn: &Connection,
    (entity, key, to): (&str, &str, Option<&str>),
    tail: &str,
    at: Option<i64>,
) -> Result<Option<(Version, i64)>, Error> {
    /// A version row: event, source, valid time, point, integer key, properties, record time.
    type Row = (
        String,
        String,
        Option<String>,
        i64,
        Option<i64>,
        Option<String>,
        i64,
    );
    let sql = format!(
        "SELECT event, source, valid, point, integer, properties, recorded FROM _hearings \
         WHERE entity = ?1 AND key = ?2 AND to_key IS ?3 {tail}"
    );
    let mut statement = conn.prepare_cached(&sql)?;
    let read = |row: &rusqlite::Row| -> rusqlite::Result<Row> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        ))
    };
    let row = match at {
        Some(at) => statement.query_row(params![entity, key, to, at], read),
        None => statement.query_row(params![entity, key, to], read),
    }
    .optional()?;
    let Some((event, source, valid, point, integer, properties, recorded)) = row else {
        return Ok(None);
    };
    let version = Version {
        event: serde_json::from_value(event.into())?,
        source,
        valid: ValidTime::from_sql(valid),
        point: time(point),
        integer,
        properties: serde_json::from_str(properties.as_deref().unwrap_or("{}"))?,
        ended: false,
    };
    Ok(Some((version, recorded)))
}

/// The version of the node keyed `key` in `class` that holds at `at` (a point, or an integer key
/// when the class is ordered by one): the greatest by the class's order at or before it, ties
/// going to the larger event id. At `i64::MAX` it is the node's current version.
pub(crate) fn node_at(
    conn: &Connection,
    class: &crate::Class,
    key: &str,
    at: i64,
) -> Result<Option<(Version, i64)>, Error> {
    let tail = if class.history == HistoryKey::Integer {
        "AND integer <= ?4 ORDER BY integer DESC, point DESC, event DESC LIMIT 1"
    } else {
        BY_POINT
    };
    version_row(conn, (&node_entity(&class.name), key, None), tail, Some(at))
}

/// Refuses an integer key for a class not ordered by one, and a time for one that is.
fn axis(class: &crate::Class, at: At) -> Result<i64, Error> {
    match (class.history == HistoryKey::Integer, at) {
        (false, At::Time(time)) => Ok(millis(time)),
        (true, At::Key(key)) => Ok(key),
        _ => Err(Error::Refused(format!(
            "class {} is not read at {at:?}: it is ordered by {:?}",
            class.name, class.history
        ))),
    }
}

impl Graph {
    /// The version of `node` that held at `at`: the newest by its class's order at or before
    /// it (by point, or in an integer-keyed class by integer key and then point), ties going to
    /// the larger event id. `None` before its first version. A version that arrived late
    /// answers at its own point like any other.
    ///
    /// # Errors
    /// [`Error::Refused`] when `at` is a time and the class is integer-keyed, or an integer key
    /// and it is not; the class is not declared, or the graph file cannot be read.
    pub fn state_at(&self, node: &NodeRef, at: At) -> Result<Option<Version>, Error> {
        let ontology = self.current_ontology()?;
        let class = ontology.class(&node.class)?;
        Ok(node_at(&self.conn, class, &node.key, axis(class, at)?)?.map(|(v, _)| v))
    }

    /// The version of the link of `relation` from the node keyed `from` to the node keyed `to`
    /// that held at the time `at`, by point and then event id, and whether the link had ended
    /// by then. `None` before its first version.
    ///
    /// # Errors
    /// [`Error::Refused`] when `at` is an integer key; the relation is not declared, or the
    /// graph file cannot be read.
    pub fn link_state_at(
        &self,
        relation: &str,
        from: &str,
        to: &str,
        at: At,
    ) -> Result<Option<Version>, Error> {
        let At::Time(at) = at else {
            return Err(Error::Refused(format!(
                "a link of {relation} is read at a time, not at {at:?}"
            )));
        };
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        let entity = link_entity(&relation);
        let Some((mut version, _)) = version_row(
            &self.conn,
            (&entity, from, Some(to)),
            BY_POINT,
            Some(millis(at)),
        )?
        else {
            return Ok(None);
        };
        version.ended = matches!(
            self.link_end(&relation, from, to)?,
            Some(End::Ended { at: ended, .. }) if ended <= at
        );
        Ok(Some(version))
    }

    /// When `node` was heard: its first and last versions on its class's axis, the version this
    /// store recorded first, and the earliest valid time any of its versions was given (a month
    /// standing for its first day). `None` when it has no version.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn seen(&self, node: &NodeRef) -> Result<Option<Seen>, Error> {
        let ontology = self.current_ontology()?;
        let class = ontology.class(&node.class)?;
        let entity = node_entity(&class.name);
        let of = (entity.as_str(), node.key.as_str(), None);
        let read = |tail: &str| version_row(&self.conn, of, tail, None);
        let (Some((first_heard, _)), Some((last_heard, _)), Some((first_recorded, _))) = (
            read("ORDER BY integer, point, event LIMIT 1")?,
            read("ORDER BY integer DESC, point DESC, event DESC LIMIT 1")?,
            read(
                "AND arrived = (SELECT min(arrived) FROM _hearings \
                 WHERE entity = ?1 AND key = ?2 AND to_key IS ?3)",
            )?,
        ) else {
            return Ok(None);
        };
        let first_true = self
            .conn
            .prepare_cached(
                "SELECT valid FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS NULL \
                 AND valid IS NOT NULL \
                 ORDER BY CASE WHEN length(valid) = 7 THEN valid || '-01' ELSE valid END LIMIT 1",
            )?
            .query_row(params![of.0, of.1], |row| row.get(0))
            .optional()?;
        Ok(Some(Seen {
            first_heard,
            last_heard,
            first_recorded,
            first_true: ValidTime::from_sql(first_true),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{
        Class, Cursor, Date, Declaration, End, NodeRef, Relation, Unit, ValidTime, Write, open,
        rebuild,
    };

    #[test]
    fn an_ending_and_a_correction_rebuild_from_the_events() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let mut g = open(dir.path().join("live.rannoch"), Some(&crate::TEST_KEY))?;
        let when = ValidTime::On(Date::day(2021, 3, 2));
        g.declare(&Declaration::Class(Class::new("voice", [] as [&str; 0])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "knows", "voice", "voice", false,
        )))?;
        let voice = |key: &str| Unit::new("voice", key, [] as [(&str, &str); 0], when.clone());
        g.write(
            &Write::new("swtor")
                .unit(voice("v1"))
                .unit(voice("v2").link("knows", "v2", "v1", when.clone()))
                .unit(voice("v3").link("knows", "v3", "v1", when.clone())),
        )?;
        let at = UNIX_EPOCH + Duration::from_millis(1_760_000_000_123);
        g.end_link("knows", "v2", "v1", at, "moderator")?;
        g.correct_link("knows", "v3", "v1", "moderator")?;
        assert!(g.end_link("knows", "v1", "v3", at, "moderator").is_err());

        let events = g.events().since(Cursor::START)?;
        let back = rebuild(
            &events,
            dir.path().join("back.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        for (from, to) in [("v2", "v1"), ("v3", "v1")] {
            assert_eq!(
                back.link_end("knows", from, to)?,
                g.link_end("knows", from, to)?
            );
        }
        assert!(matches!(
            back.link_end("knows", "v3", "v1")?,
            Some(End::Corrected { .. })
        ));
        Ok(())
    }

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
