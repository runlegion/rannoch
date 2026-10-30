//! Hearings: every write is a hearing of a node or a link, at a point on the time axis. A
//! hearing's identity is its content, hashed under the app's key: identical content is one
//! hearing, and an identical event that arrives from another store under its own id is held as an
//! alias of it. The key never reaches the store's files; the store keeps only a check value.
//! A link that ends records its ending, at its own point, beside its hearings, and a live
//! hearing at a later point reopens it; a link found wrong records a correction, which is not an
//! ending. When each node and link was first and last heard is kept as it is recorded.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rannoch_ontology::{Class, HistoryKey, Ontology, Value};
use rusqlite::{Connection, OptionalExtension, ToSql, TransactionBehavior, params};

use crate::fact::{Conclusion, LinkFact, NodeFact, ValidTime};
use crate::log::{Cursor, EventId};
use crate::ontology::{CLASS, RELATION};
use crate::queries::{
    ALIAS, ALIASES_OF, CHECK, CONCLUDE, CORRECTED, ENDING, HEAR, HEARING_OF, HELD, RENAME,
    SEEN_OTHER, SEEN_START, SET_CHECK, as_of, by_integer_key, by_point, edge_ending, edge_hearing,
    ended_sql, hearings, link_ending, link_state, order, seen_keep, seen_kept, seen_sql,
    version_row,
};
use crate::read::NodeRef;
use crate::{Error, Graph};

/// The fixed label whose keyed hash is the check value.
const CHECK_LABEL: &[u8] = b"rannoch content-identity check";

/// The app's content-identity key, held in memory only and never printed.
#[derive(Clone)]
pub(crate) struct Key {
    bytes: [u8; 32],
    /// The keyed hash of a fixed label: what the store keeps in the key's place.
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
    pub(crate) fn entity(&self) -> (String, &str, Option<&str>) {
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

    pub(crate) fn time(&self) -> Option<i64> {
        match self {
            Of::Node(node) => node.time,
            Of::Link(link) => link.time,
        }
    }

    fn integer(&self) -> Option<i64> {
        match self {
            Of::Node(node) => node.integer,
            Of::Link(_) => None,
        }
    }

    /// The origin's event it arrived in.
    fn event(&self) -> EventId {
        match self {
            Of::Node(node) => node.event,
            Of::Link(link) => link.event,
        }
    }

    fn source(&self) -> &str {
        match self {
            Of::Node(node) => &node.source,
            Of::Link(link) => &link.source,
        }
    }

    /// Its identity: its content, with the source it was heard from, hashed under `key`. A point
    /// rannoch assigns is not content; a time or an integer key the writer supplied is.
    pub(crate) fn identity(&self, key: &Key) -> Result<[u8; 32], Error> {
        let source = self.source();
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
            // A link's time joins its content only when its writer supplied one, so every link
            // heard without one keeps the identity it always had.
            Of::Link(l) => match l.time {
                None => {
                    serde_json::to_vec(&("link", &l.relation, &l.from, &l.to, &l.valid, source))?
                }
                Some(time) => serde_json::to_vec(&(
                    "link",
                    &l.relation,
                    &l.from,
                    &l.to,
                    &l.valid,
                    source,
                    time,
                ))?,
            },
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

pub(crate) fn node_entity(class: &str) -> String {
    format!("{CLASS}:{class}")
}

pub(crate) fn link_entity(relation: &str) -> String {
    format!("{RELATION}:{relation}")
}

/// The log event a fact is filled from: its place in this store's log and when this store
/// recorded it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Stamp {
    pub(crate) cursor: Cursor,
    pub(crate) recorded: i64,
}

/// A hearing the graph holds, identical to one that arrived.
pub(crate) struct Held {
    identity: [u8; 32],
    event: EventId,
    time: Option<i64>,
}

/// Records what `of` heard, whose content identity is `identity`, as a new version row named by
/// its origin's event and filled at `stamp`, and returns `None`; or, when the graph holds a
/// hearing identical to it, records nothing and returns that one. Its integer key is kept only
/// when its class is ordered by one, so every other class's rows order by point alone.
fn hear(
    tx: &Connection,
    identity: [u8; 32],
    stamp: Stamp,
    of: &Of,
    ontology: &Ontology,
) -> Result<Option<Held>, Error> {
    let held = tx
        .prepare_cached(HELD)?
        .query_row(params![identity], |row| row.get::<_, String>(0))
        .optional()?;
    if let Some(held) = held {
        return Ok(Some(Held {
            identity,
            event: serde_json::from_value(held.into())?,
            time: of.time(),
        }));
    }
    let (entity, from, to) = of.entity();
    let properties = match of {
        Of::Node(node) => Some(serde_json::to_string(&node.properties)?),
        Of::Link(_) => None,
    };
    let integer = if by_integer(ontology, of)? {
        of.integer()
    } else {
        None
    };
    let point = of.time().map_or_else(|| of.event().recorded(), Ok)?;
    tx.prepare_cached(HEAR)?.execute(params![
        identity,
        entity,
        from,
        to,
        of.event().to_string(),
        stamp.cursor.0,
        of.source(),
        of.valid().to_sql(),
        point,
        integer,
        stamp.recorded,
        properties
    ])?;
    let mark = Mark {
        hearing: Some(tx.last_insert_rowid()),
        ending: None,
        integer,
        point,
        event: of.event().to_string(),
    };
    saw(tx, (&entity, from, to), &mark, of.valid().to_sql())?;
    Ok(None)
}

/// A fact a kept seen row names, by its row: a hearing by when this store recorded it, or an
/// ending by its id; with what orders it among the others.
struct Mark {
    hearing: Option<i64>,
    ending: Option<i64>,
    integer: Option<i64>,
    point: i64,
    event: String,
}

impl Mark {
    /// Its place in its class's order (see [`order`]): no integer key orders first, as SQL's
    /// NULL does.
    fn rank(&self) -> (Option<i64>, i64, &str) {
        (self.integer, self.point, &self.event)
    }

    fn row(row: &rusqlite::Row, at: usize) -> rusqlite::Result<Mark> {
        Ok(Mark {
            hearing: row.get(at)?,
            ending: row.get(at + 1)?,
            integer: row.get(at + 2)?,
            point: row.get(at + 3)?,
            event: row.get(at + 4)?,
        })
    }
}

/// Keeps what is seen of `of` (entity, key, target key) up to date with `mark`, a new hearing or
/// ending of it with the valid time `valid`, in the transaction that records it: the first and
/// last of its facts by its class's order, the first this store recorded, the earliest valid
/// time it was given (a month standing for its first day) and how many facts it has. A link's
/// endings count among its facts, as its versions do.
fn saw(
    tx: &Connection,
    (entity, key, to): (&str, &str, Option<&str>),
    mark: &Mark,
    valid: Option<&str>,
) -> Result<(), Error> {
    let to = to.unwrap_or_default();
    let kept = tx
        .prepare_cached(&seen_kept())?
        .query_row(params![entity, key, to], |row| {
            Ok((Mark::row(row, 0)?, Mark::row(row, 5)?))
        })
        .optional()?;
    let Some((first, last)) = kept else {
        // One fact keeps no row, since it is its own first, last and first recorded; the second
        // starts the row from the one before it.
        let other = tx
            .prepare_cached(SEEN_OTHER)?
            .query_row(params![entity, key, to, mark.hearing, mark.ending], |row| {
                Ok((Mark::row(row, 0)?, row.get::<_, Option<String>>(5)?))
            })
            .optional()?;
        let Some((other, was)) = other else {
            return Ok(());
        };
        tx.prepare_cached(SEEN_START)?.execute(params![
            entity,
            key,
            to,
            other.hearing,
            other.ending,
            was
        ])?;
        let (first, last) = if mark.rank() < other.rank() {
            (mark, &other)
        } else {
            (&other, mark)
        };
        return keep(tx, (entity, key, to), (first, last), (1, valid));
    };
    let first = if mark.rank() < first.rank() {
        mark
    } else {
        &first
    };
    let last = if mark.rank() > last.rank() {
        mark
    } else {
        &last
    };
    keep(tx, (entity, key, to), (first, last), (1, valid))
}

/// Keeps `first` and `last` as the first and last facts of `of` (its target key `''` for a
/// node), adds `added` facts to its count and takes `valid` as its first true when earlier.
fn keep(
    tx: &Connection,
    (entity, key, to): (&str, &str, &str),
    (first, last): (&Mark, &Mark),
    (added, valid): (i64, Option<&str>),
) -> Result<(), Error> {
    tx.prepare_cached(&seen_keep())?.execute(params![
        entity,
        key,
        to,
        first.hearing,
        first.ending,
        last.hearing,
        last.ending,
        added,
        valid
    ])?;
    Ok(())
}

/// What hearing a fact did: one identical to a hearing held, a new newest version, or a new
/// version older on its class's axis than one held.
pub(crate) enum Heard {
    Held(Held),
    Newest,
    Older,
}

/// Hears `of`: the identical hearing held when it is its node's or link's current version, or
/// when its writer supplied its point, which is then part of its identity; else a new hearing,
/// or a return to an older one, which this event is a new occurrence of. Current state takes
/// either only when it is now the newest version.
pub(crate) fn heard(
    tx: &Connection,
    ontology: &Ontology,
    key: &Key,
    stamp: Stamp,
    of: &Of,
) -> Result<Heard, Error> {
    let identity = of.identity(key)?;
    let (entity, from, to) = of.entity();
    if let Some(held) = hear(tx, identity, stamp, of, ontology)? {
        if of.time().is_some()
            || newest(tx, (&entity, from, to))?.is_some_and(|(.., newest)| newest == identity)
        {
            return Ok(Heard::Held(held));
        }
        alias(tx, &held, of.event(), stamp.cursor)?;
    }
    Ok(match newest(tx, (&entity, from, to))? {
        Some((.., newest)) if newest != identity => Heard::Older,
        _ => Heard::Newest,
    })
}

/// Holds `event`, at `cursor`, as an alias of `held`, whose content it arrived with: an
/// occurrence of the hearing at its own point. When `event` is the smaller id the hearing takes
/// its name and its point, the id it was named by becomes the alias, and this returns true.
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
        tx.prepare_cached(RENAME)?.execute(params![
            held.identity,
            event.to_string(),
            cursor.0,
            held.time.map_or_else(|| event.recorded(), Ok)?
        ])?;
    }
    let alias = if renamed { held.event } else { event };
    let new = tx.prepare_cached(ALIAS)?.execute(params![
        held.identity,
        alias.to_string(),
        held.time.map_or_else(|| alias.recorded(), Ok)?
    ])?;
    if renamed || new == 1 {
        reseen(tx, held.identity)?;
    }
    Ok(renamed)
}

/// Finds the first and last facts of the node or link the hearing `identity` is of again, by
/// index seeks, after that hearing took another event's id and maybe another point.
fn reseen(tx: &Connection, identity: [u8; 32]) -> Result<(), Error> {
    let (entity, key, to): (String, String, Option<String>) = tx
        .prepare_cached(HEARING_OF)?
        .query_row(params![identity], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if let Some((first, last)) = edges(tx, (&entity, &key, to.as_deref()))? {
        let of = (
            entity.as_str(),
            key.as_str(),
            to.as_deref().unwrap_or_default(),
        );
        keep(tx, of, (&first, &last), (0, None))?;
    }
    Ok(())
}

/// The first and last facts of `of` (entity, key, target key), hearings by their occurrences
/// and endings, by index seeks; `None` when it has none.
fn edges(
    tx: &Connection,
    (entity, key, to): (&str, &str, Option<&str>),
) -> Result<Option<(Mark, Mark)>, Error> {
    let edge = |direction: &str| -> Result<Option<Mark>, Error> {
        let hearing = tx
            .prepare_cached(&edge_hearing(direction))?
            .query_row(params![entity, key, to], |row| Mark::row(row, 0))
            .optional()?;
        let ending = tx
            .prepare_cached(&edge_ending(direction))?
            .query_row(params![entity, key, to], |row| Mark::row(row, 0))
            .optional()?;
        let later = direction == "DESC";
        Ok(match (hearing, ending) {
            (Some(h), Some(e)) if (e.rank() > h.rank()) == later => Some(e),
            (Some(h), _) => Some(h),
            (None, e) => e,
        })
    };
    Ok(edge("ASC")?.zip(edge("DESC")?))
}

/// Records how a link came to an end, when `conclusion` is an ending (at its point, with its
/// valid time) or a correction (with neither). Every ending is kept, and counts as a fact of the
/// link; the latest conclusion to arrive replaces any before it, so a correction that came last
/// masks the endings.
pub(crate) fn conclude(tx: &Connection, conclusion: &Conclusion) -> Result<(), Error> {
    let Conclusion {
        event,
        source,
        relation,
        from,
        to,
        ended,
    } = conclusion;
    let at = ended.as_ref().map(|(at, _)| *at);
    let valid = ended.as_ref().and_then(|(_, valid)| valid.to_sql());
    let entity = link_entity(relation);
    tx.prepare_cached(CONCLUDE)?.execute(params![
        entity,
        from.key,
        to.key,
        event.to_string(),
        source,
        at,
        valid
    ])?;
    if let Some(at) = at {
        let new = tx.prepare_cached(ENDING)?.execute(params![
            entity,
            from.key,
            to.key,
            at,
            event.to_string(),
            source,
            valid
        ])?;
        if new == 1 {
            let mark = Mark {
                hearing: None,
                ending: Some(tx.last_insert_rowid()),
                integer: None,
                point: at,
                event: event.to_string(),
            };
            saw(tx, (&entity, &from.key, Some(&to.key)), &mark, valid)?;
        }
    }
    Ok(())
}

/// Takes `key` as the graph's. A store holding another key's check value refuses it; a store
/// holding none takes it.
pub(crate) fn take(conn: &mut Connection, key: &Key) -> Result<(), Error> {
    let check = |conn: &Connection| -> Result<Option<String>, Error> {
        Ok(conn
            .prepare_cached(CHECK)?
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
    tx.execute(SET_CHECK, params![key.check])?;
    tx.commit()?;
    Ok(())
}

/// The current version of the node or link `of` (entity, key, target key): the newest by its
/// class's order, as every read and write of current state chooses it.
pub(crate) fn newest(
    conn: &Connection,
    of: (&str, &str, Option<&str>),
) -> Result<Option<Versioned>, Error> {
    read_version(conn, of, &format!("ORDER BY {}", order("DESC")), &[])
}

/// A version row's properties: a node's, or none for a link, whose row holds null.
fn decode(json: Option<&str>) -> Result<BTreeMap<String, Value>, Error> {
    Ok(serde_json::from_str(json.unwrap_or("{}"))?)
}

/// Milliseconds since the Unix epoch, negative before it.
pub(crate) fn millis(time: SystemTime) -> i64 {
    let ms = |d: Duration| i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => ms(after),
        Err(before) => -ms(before.duration()),
    }
}

/// The time `millis` milliseconds from the Unix epoch: the inverse of [`millis`].
pub(crate) fn time(millis: i64) -> SystemTime {
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
        /// When the link stopped holding in the world, which may be unknown.
        valid: ValidTime,
        source: String,
        event: EventId,
    },
    Corrected {
        source: String,
        event: EventId,
    },
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
    /// Whether a link had ended by the point read, or, as [`Seen`] names a link's fact, whether
    /// that fact is one of its endings; a node never has.
    pub ended: bool,
}

/// When a node or a link was heard: its first and last facts on its class's axis, the one this
/// store recorded first, the earliest valid time it has been given and how many facts it has. A
/// link's endings are among its facts, each named as a version with `ended` set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub first_heard: Version,
    pub last_heard: Version,
    /// Per store: another store given the same events in another order may name another.
    pub first_recorded: Version,
    pub first_true: ValidTime,
    /// Its hearings, and a link's endings with them.
    pub count: u64,
    /// When a link ended: the point of its earliest ending after its last live hearing, by
    /// point and then event id; `None` for a node, and for a link that has not ended, was found
    /// wrong or was heard live after its endings.
    pub ended: Option<SystemTime>,
}

/// One version row: the version, when this store recorded it and its content identity.
pub(crate) type Versioned = (Version, i64, [u8; 32]);

/// Reads one version row of `entity` keyed `key` (and `to`, for a link), as `tail` picks it
/// among its hearings' occurrences, with `bound` bound from `?4` on: see [`Versioned`]. The
/// version is named by its hearing's event, at the occurrence's point.
fn read_version(
    conn: &Connection,
    (entity, key, to): (&str, &str, Option<&str>),
    tail: &str,
    bound: &[&dyn ToSql],
) -> Result<Option<Versioned>, Error> {
    /// A version row: event, source, valid, point, integer key, properties, recorded, identity.
    type Row = (
        String,
        String,
        Option<String>,
        i64,
        Option<i64>,
        Option<String>,
        i64,
        [u8; 32],
    );
    let mut statement = conn.prepare_cached(&version_row(tail))?;
    let read = |row: &rusqlite::Row| -> rusqlite::Result<Row> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
        ))
    };
    let mut all: Vec<&dyn ToSql> = vec![&entity, &key, &to];
    all.extend_from_slice(bound);
    let row = statement.query_row(all.as_slice(), read).optional()?;
    let Some((event, source, valid, point, integer, properties, recorded, identity)) = row else {
        return Ok(None);
    };
    let version = version_of((event, source, valid, point, integer, properties), false)?;
    Ok(Some((version, recorded, identity)))
}

/// A version's columns as a row holds them: event, source, valid, point, integer key, properties.
type VersionColumns = (
    String,
    String,
    Option<String>,
    i64,
    Option<i64>,
    Option<String>,
);

/// The version a row's [`VersionColumns`] name, `ended` when it is an ending.
fn version_of(
    (event, source, valid, point, integer, properties): VersionColumns,
    ended: bool,
) -> Result<Version, Error> {
    Ok(Version {
        event: serde_json::from_value(event.into())?,
        source,
        valid: ValidTime::from_sql(valid),
        point: time(point),
        integer,
        properties: decode(properties.as_deref())?,
        ended,
    })
}

/// The version of the node keyed `key` in `class` that holds at `at` (a point, or an integer key
/// when the class is ordered by one): the greatest by the class's order at or before it, ties
/// going to the larger event id.
fn node_at(
    conn: &Connection,
    class: &Class,
    key: &str,
    at: i64,
) -> Result<Option<Versioned>, Error> {
    let tail = if class.history == HistoryKey::Integer {
        by_integer_key()
    } else {
        by_point()
    };
    read_version(conn, (&node_entity(&class.name), key, None), &tail, &[&at])
}

/// What a read known at `at` bounds the versions of `class` by, or of a link when `class` is
/// `None`: the column on its axis and the bound. A link is read at a time only.
pub(crate) fn known_bound(class: Option<&Class>, at: At) -> Result<(&'static str, i64), Error> {
    match (class, at) {
        (Some(class), at) if class.history == HistoryKey::Integer => {
            Ok(("integer", axis(class, at)?))
        }
        (Some(class), at) => Ok(("point", axis(class, at)?)),
        (None, At::Time(time)) => Ok(("point", millis(time))),
        (None, At::Key(_)) => Err(Error::Refused(format!(
            "a link is read at a time, not at {at:?}"
        ))),
    }
}

/// The version of the node keyed `key` in `class` true at the valid time `day`
/// (`YYYY-MM-DD`), as known at `known` when given: among its versions valid on or before `day`,
/// the greatest by valid time, then by its class's order. A version of unknown valid time is
/// never one.
pub(crate) fn node_as_of(
    conn: &Connection,
    class: &Class,
    key: &str,
    day: &str,
    known: Option<At>,
) -> Result<Option<Versioned>, Error> {
    let of = (node_entity(&class.name), key, None);
    match known.map(|at| known_bound(Some(class), at)).transpose()? {
        None => read_version(conn, (&of.0, of.1, of.2), &as_of(None), &[&day]),
        Some((column, at)) => read_version(
            conn,
            (&of.0, of.1, of.2),
            &as_of(Some(column)),
            &[&day, &at],
        ),
    }
}

/// Whether the link of `relation` from `from` to `to` had ended by the point `known` and, as of
/// `day` when given, by then; refused as of `day` when its ending's valid time is unknown.
fn ended(
    conn: &Connection,
    relation: &str,
    (from, to): (&str, &str),
    known: i64,
    day: Option<&str>,
) -> Result<bool, Error> {
    let ending: Option<Option<bool>> = conn
        .prepare_cached(&ended_sql())?
        .query_row(
            params![link_entity(relation), from, to, known, day],
            |row| row.get(0),
        )
        .optional()?;
    ending.map_or(Ok(false), |ended| {
        ended.ok_or_else(|| unknown_ending(relation, (from, to), day.unwrap_or_default()))
    })
}

/// The refusal of a read as of `day` that would follow the link of `relation` from `from` to
/// `to`, whose ending has an unknown valid time.
pub(crate) fn unknown_ending(relation: &str, (from, to): (&str, &str), day: &str) -> Error {
    Error::Refused(format!(
        "the {relation} link from {from} to {to} ended, and its ending's valid time is unknown: \
         a read as of {day} cannot say whether the link held"
    ))
}

/// Refuses an integer key for a class not ordered by one, and a time for one that is.
fn axis(class: &Class, at: At) -> Result<i64, Error> {
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
    /// How the link of `relation` from the node keyed `from` to the node keyed `to` came to an
    /// end: found wrong, when a correction is its latest conclusion to arrive; else its ending,
    /// the earliest after its last live hearing by point and then event id; `None` while it has
    /// neither ended nor been found wrong, and once heard live after its endings.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn link_end(&self, relation: &str, from: &str, to: &str) -> Result<Option<End>, Error> {
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        let link = params![link_entity(&relation), from, to];
        let corrected: Option<(String, String)> = self
            .conn
            .prepare_cached(CORRECTED)?
            .query_row(link, |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        if let Some((event, source)) = corrected {
            let event = serde_json::from_value(event.into())?;
            return Ok(Some(End::Corrected { source, event }));
        }
        let row: Option<(String, String, i64, Option<String>)> = self
            .conn
            .prepare_cached(&link_ending())?
            .query_row(link, |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .optional()?;
        let Some((event, source, at, valid)) = row else {
            return Ok(None);
        };
        Ok(Some(End::Ended {
            at: time(at),
            valid: ValidTime::from_sql(valid),
            source,
            event: serde_json::from_value(event.into())?,
        }))
    }

    /// Every hearing of `node`, in order on its class's axis: by integer key in an
    /// integer-keyed class, then by point, then by event id.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn hearings(&self, node: &NodeRef) -> Result<Vec<Hearing>, Error> {
        let class = self.current_ontology()?.class(&node.class)?.name.clone();
        self.hearings_of(&node_entity(&class), &node.key, None)
    }

    /// Every hearing of the link of `relation` from the node keyed `from` to the node keyed
    /// `to`, by point and then by event id (a link is never ordered by an integer key).
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
        self.hearings_of(&link_entity(&relation), from, Some(to))
    }

    fn hearings_of(
        &self,
        entity: &str,
        key: &str,
        to: Option<&str>,
    ) -> Result<Vec<Hearing>, Error> {
        /// A hearing's row: identity, event, source, valid time, point and integer key.
        type Row = ([u8; 32], String, String, Option<String>, i64, Option<i64>);
        let rows: Vec<Row> = self
            .conn
            .prepare_cached(&hearings())?
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
                .prepare_cached(ALIASES_OF)?
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
        Ok(node_at(&self.conn, class, &node.key, axis(class, at)?)?.map(|(v, ..)| v))
    }

    /// The version of the link of `relation` from the node keyed `from` to the node keyed `to`
    /// that held at the time `at`: the newest of its live hearings and endings at or before it,
    /// by point and then event id, and whether the link had ended by then. `None` before its
    /// first version.
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
        let (_, at) = known_bound(None, at)?;
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        let newest = self
            .conn
            .prepare_cached(&link_state())?
            .query_row(params![link_entity(&relation), from, to, at], |row| {
                let columns: VersionColumns = (
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                );
                Ok((columns, row.get::<_, bool>(6)?))
            })
            .optional()?;
        let Some((columns, ending)) = newest else {
            return Ok(None);
        };
        let mut version = version_of(columns, ending)?;
        version.ended = ended(&self.conn, &relation, (from, to), at, None)?;
        Ok(Some(version))
    }

    /// When `node` was heard: its first and last versions on its class's axis, the version this
    /// store recorded first, the earliest valid time any of its versions was given (a month
    /// standing for its first day) and how many it has, read as one kept row. `None` when it
    /// has no version.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn seen(&self, node: &NodeRef) -> Result<Option<Seen>, Error> {
        let class = self.current_ontology()?.class(&node.class)?.name.clone();
        self.seen_of((&node_entity(&class), &node.key, ""))
    }

    /// When the link of `relation` from the node keyed `from` to the node keyed `to` was heard,
    /// as [`Graph::seen`] reads a node, its endings among its facts, and when it ended, in the
    /// same one read. `None` when it has neither a hearing nor an ending.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn link_seen(&self, relation: &str, from: &str, to: &str) -> Result<Option<Seen>, Error> {
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        self.seen_of((&link_entity(&relation), from, to))
    }

    /// The kept seen row of `of` (its target key `''` for a node), with the facts it names: each
    /// a hearing joined by when it arrived, or an ending by its key.
    fn seen_of(&self, of: (&str, &str, &str)) -> Result<Option<Seen>, Error> {
        let sql = seen_sql();
        let version = |row: &rusqlite::Row, at: usize| -> rusqlite::Result<_> {
            let columns: VersionColumns = (
                row.get(at)?,
                row.get(at + 1)?,
                row.get(at + 2)?,
                row.get(at + 3)?,
                row.get(at + 4)?,
                row.get(at + 5)?,
            );
            Ok((columns, row.get::<_, bool>(at + 6)?))
        };
        let row = self
            .conn
            .prepare_cached(&sql)?
            .query_row(params![of.0, of.1, of.2], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    [version(row, 3)?, version(row, 10)?, version(row, 17)?],
                ))
            })
            .optional()?;
        let Some((count, first_true, ended, facts)) = row else {
            return Ok(None);
        };
        let [first_heard, last_heard, first_recorded] =
            facts.map(|(columns, ended)| version_of(columns, ended));
        Ok(Some(Seen {
            first_heard: first_heard?,
            last_heard: last_heard?,
            first_recorded: first_recorded?,
            first_true: ValidTime::from_sql(first_true),
            count: u64::try_from(count).unwrap_or(0),
            ended: ended.map(time),
        }))
    }
}
