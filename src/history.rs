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

use rusqlite::{Connection, OptionalExtension, ToSql, TransactionBehavior, params};

use crate::events::{Change, Cursor, EventId, Events, Fact, LinkFact, NodeFact};
use crate::open::{CLASS, Graph, HistoryKey, Ontology, RELATION, Value, table};
use crate::read::NodeRef;
use crate::write::{Error, Heard, Piece, Stamp, ValidTime};

/// The hearings, one version row each, with no valid-to: `arrived` counts up as this store
/// records them, `integer` is set only in an integer-keyed class, and `properties` holds a
/// node's. Then the other ids each arrived under, each an occurrence of its hearing at its own
/// point; the latest conclusion to arrive for each link
/// that ended or was found wrong (`point` null for a correction, which masks its endings); every
/// ending of a link, at its point (`valid` null when unknown); what is kept of when each node and
/// link was seen (see [`saw`]); and the check value of the graph's key.
pub(crate) const HISTORY_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _hearings(
        arrived INTEGER PRIMARY KEY, identity BLOB NOT NULL UNIQUE, entity TEXT NOT NULL,
        key TEXT NOT NULL, to_key TEXT, event TEXT NOT NULL, seq INTEGER NOT NULL,
        source TEXT NOT NULL, valid TEXT, point INTEGER NOT NULL, integer INTEGER,
        recorded INTEGER NOT NULL, properties TEXT);
    CREATE INDEX IF NOT EXISTS _hearings_at
        ON _hearings(entity, key, to_key, integer, point, event);
    CREATE TABLE IF NOT EXISTS _aliases(
        identity BLOB NOT NULL, alias TEXT NOT NULL, point INTEGER,
        PRIMARY KEY(identity, alias)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _aliases_at ON _aliases(identity, point);
    CREATE TABLE IF NOT EXISTS _ends(
        entity TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT NOT NULL, event TEXT NOT NULL,
        source TEXT NOT NULL, point INTEGER, valid TEXT,
        PRIMARY KEY(entity, key, to_key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _endings(
        id INTEGER PRIMARY KEY, entity TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT NOT NULL,
        point INTEGER NOT NULL, event TEXT NOT NULL, source TEXT NOT NULL, valid TEXT,
        UNIQUE(entity, key, to_key, point, event));
    CREATE TABLE IF NOT EXISTS _seen(
        entity TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT NOT NULL,
        first_hearing INTEGER, first_ending INTEGER, last_hearing INTEGER, last_ending INTEGER,
        recorded_hearing INTEGER, recorded_ending INTEGER, first_true TEXT,
        count INTEGER NOT NULL, PRIMARY KEY(entity, key, to_key)) WITHOUT ROWID;
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

    /// `text`'s keyed match value: its keyed hash under the key, so a purge names what it
    /// matched without holding it.
    pub(crate) fn matched(&self, text: &[u8]) -> blake3::Hash {
        blake3::keyed_hash(&self.bytes, text)
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

impl<'a> Of<'a> {
    /// What `fact` is a hearing of: a node, or a link, open or not.
    pub(crate) fn fact(fact: &'a Fact) -> Of<'a> {
        match fact {
            Fact::Node(node) => Of::Node(node),
            Fact::Link(link) | Fact::Opened(link) => Of::Link(link),
        }
    }

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

pub(crate) fn node_entity(class: &str) -> String {
    format!("{CLASS}:{class}")
}

pub(crate) fn link_entity(relation: &str) -> String {
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
/// row named by `stamp`'s event and returns `None`; or, when the graph holds a hearing identical
/// to it, records nothing and returns that one. Its integer key is kept only when its class is ordered by one, so every
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
    let integer = if by_integer(ontology, of)? {
        of.integer()
    } else {
        None
    };
    let point = of.time().unwrap_or(stamp.id.millis());
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
        event: stamp.id.to_string(),
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
        .prepare_cached(&format!(
            "SELECT s.first_hearing, s.first_ending, fh.integer, coalesce(fh.point, fe.point), \
             coalesce(fh.event, fe.event), s.last_hearing, s.last_ending, lh.integer, \
             coalesce({}, le.point), coalesce({}, le.event) FROM _seen s \
             LEFT JOIN _hearings fh ON fh.arrived = s.first_hearing \
             LEFT JOIN _endings fe ON fe.id = s.first_ending \
             LEFT JOIN _hearings lh ON lh.arrived = s.last_hearing \
             LEFT JOIN _endings le ON le.id = s.last_ending \
             WHERE s.entity = ?1 AND s.key = ?2 AND s.to_key = ?3",
            newest_occurrence("s.last_hearing", "point"),
            newest_occurrence("s.last_hearing", "event")
        ))?
        .query_row(params![entity, key, to], |row| {
            Ok((Mark::row(row, 0)?, Mark::row(row, 5)?))
        })
        .optional()?;
    let Some((first, last)) = kept else {
        // One fact keeps no row, since it is its own first, last and first recorded; the second
        // starts the row from the one before it.
        let other = tx
            .prepare_cached(
                "SELECT * FROM (SELECT arrived, NULL, integer, point, event, valid FROM _hearings \
                 WHERE entity = ?1 AND key = ?2 AND to_key IS nullif(?3, '') \
                 AND arrived IS NOT ?4 UNION ALL SELECT NULL, id, NULL, point, event, valid \
                 FROM _endings WHERE entity = ?1 AND key = ?2 AND to_key = ?3 AND id IS NOT ?5) \
                 LIMIT 1",
            )?
            .query_row(params![entity, key, to, mark.hearing, mark.ending], |row| {
                Ok((Mark::row(row, 0)?, row.get::<_, Option<String>>(5)?))
            })
            .optional()?;
        let Some((other, was)) = other else {
            return Ok(());
        };
        tx.prepare_cached("INSERT INTO _seen VALUES (?1, ?2, ?3, ?4, ?5, ?4, ?5, ?4, ?5, ?6, 1)")?
            .execute(params![entity, key, to, other.hearing, other.ending, was])?;
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
    let earlier = format!("{} < {}", valid_day("?9"), valid_day("first_true"));
    tx.prepare_cached(&format!(
        "UPDATE _seen SET first_hearing = ?4, first_ending = ?5, last_hearing = ?6, \
         last_ending = ?7, count = count + ?8, first_true = CASE WHEN ?9 IS NOT NULL \
         AND (first_true IS NULL OR {earlier}) THEN ?9 ELSE first_true END \
         WHERE entity = ?1 AND key = ?2 AND to_key = ?3"
    ))?
    .execute(params![
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

/// Finds the first and last facts of the node or link the hearing `identity` is of again, by
/// index seeks, after that hearing took another event's id and maybe another point.
fn reseen(tx: &Connection, identity: [u8; 32]) -> Result<(), Error> {
    let (entity, key, to): (String, String, Option<String>) = tx
        .prepare_cached("SELECT entity, key, to_key FROM _hearings WHERE identity = ?1")?
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
            .prepare_cached(&format!(
                "SELECT arrived, NULL, integer, point, event FROM {} ORDER BY {} LIMIT 1",
                occurrences_of(),
                order(direction)
            ))?
            .query_row(params![entity, key, to], |row| Mark::row(row, 0))
            .optional()?;
        let ending = tx
            .prepare_cached(&format!(
                "SELECT NULL, id, NULL, point, event FROM _endings \
                 WHERE entity = ?1 AND key = ?2 AND to_key = ?3 \
                 ORDER BY point {direction}, event {direction} LIMIT 1"
            ))?
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

/// The current version of the node or link `of` (entity, key, target key): the newest by its
/// class's order, as every read and write of current state chooses it.
pub(crate) fn newest(
    conn: &Connection,
    of: (&str, &str, Option<&str>),
) -> Result<Option<Versioned>, Error> {
    version_row(conn, of, &format!("ORDER BY {}", order("DESC")), &[])
}

/// A version row's properties: a node's, or none for a link, whose row holds null.
fn decode(json: Option<&str>) -> Result<BTreeMap<String, Value>, Error> {
    Ok(serde_json::from_str(json.unwrap_or("{}"))?)
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
    let new = tx
        .prepare_cached(
            "INSERT OR IGNORE INTO _aliases(identity, alias, point) VALUES (?1, ?2, ?3)",
        )?
        .execute(params![
            held.identity,
            alias.to_string(),
            held.time.unwrap_or(alias.millis())
        ])?;
    if renamed || new == 1 {
        reseen(tx, held.identity)?;
    }
    Ok(renamed)
}

/// An alias as a store holds it: the identity of the hearing it is an alias of, its event id
/// and its point.
pub(crate) type Alias = (Vec<u8>, String, Option<i64>);

/// Every alias `conn` holds.
pub(crate) fn aliases(conn: &Connection) -> Result<Vec<Alias>, Error> {
    Ok(conn
        .prepare_cached("SELECT identity, alias, point FROM _aliases")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?)
}

/// Holds each of `aliases` whose hearing the graph holds, at its point, in one transaction.
pub(crate) fn hold_aliases(conn: &Connection, aliases: &[Alias]) -> Result<(), Error> {
    let tx = conn.unchecked_transaction()?;
    for (identity, alias, point) in aliases {
        tx.prepare_cached(
            "INSERT OR IGNORE INTO _aliases(identity, alias, point) SELECT ?1, ?2, ?3 \
             WHERE EXISTS (SELECT 1 FROM _hearings WHERE identity = ?1)",
        )?
        .execute(params![identity, alias, point])?;
    }
    tx.commit()?;
    Ok(())
}

/// Drops the hearings of `node`, which left live state, and of `links`, which left with it, with
/// the links' endings and what was seen of each.
pub(crate) fn unhear(tx: &Connection, node: &NodeRef, links: &[LinkFact]) -> Result<(), Error> {
    let drop = |entity: String, key: &str, to: Option<&str>| -> Result<(), Error> {
        let of = "SELECT identity FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS ?3";
        tx.prepare_cached(&format!("DELETE FROM _aliases WHERE identity IN ({of})"))?
            .execute(params![entity, key, to])?;
        tx.prepare_cached("DELETE FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS ?3")?
            .execute(params![entity, key, to])?;
        tx.prepare_cached("DELETE FROM _seen WHERE entity = ?1 AND key = ?2 AND to_key = ?3")?
            .execute(params![entity, key, to.unwrap_or_default()])?;
        Ok(())
    };
    drop(node_entity(&node.class), &node.key, None)?;
    for link in links {
        let entity = link_entity(&link.relation);
        for table in ["_ends", "_endings"] {
            tx.prepare_cached(&format!(
                "DELETE FROM {table} WHERE entity = ?1 AND key = ?2 AND to_key = ?3"
            ))?
            .execute(params![entity, link.from.key, link.to.key])?;
        }
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

/// Records how a link came to an end, when `change` is an ending (at its point, with its valid
/// time) or a correction (with neither), as the event `event`; any other change records
/// nothing. Every ending is kept, and counts as a fact of the link; the latest conclusion to
/// arrive replaces any before it, so a correction that came last masks the endings. Ending,
/// correcting and replaying either record through here.
pub(crate) fn conclude(tx: &Connection, event: EventId, change: &Change) -> Result<(), Error> {
    let (source, relation, from, to, at, valid) = match change {
        Change::Ended {
            source,
            relation,
            from,
            to,
            at,
            valid,
        } => (source, relation, from, to, Some(*at), valid.to_sql()),
        Change::Corrected {
            source,
            relation,
            from,
            to,
        } => (source, relation, from, to, None, None),
        _ => return Ok(()),
    };
    tx.prepare_cached(
        "INSERT INTO _ends(entity, key, to_key, event, source, point, valid) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(entity, key, to_key) DO UPDATE SET \
         event = excluded.event, source = excluded.source, point = excluded.point, \
         valid = excluded.valid",
    )?
    .execute(params![
        link_entity(relation),
        from.key,
        to.key,
        event.to_string(),
        source,
        at,
        valid
    ])?;
    if let Some(at) = at {
        let entity = link_entity(relation);
        let new = tx
            .prepare_cached(
                "INSERT OR IGNORE INTO _endings(entity, key, to_key, point, event, source, valid) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?
            .execute(params![
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
/// holding none takes it and hears every write already in its log, in order, dropping what each
/// forget in it removed and moving what each replacement in it moved.
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
            Change::Replaced { class, old, new } => {
                carry(&tx, &ontology, key, (class, old, new))?;
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
        // Into the versions only, as a replay hears them: current state stays as it is.
        let mut piece = Piece::new(&tx, &ontology, key, source, stamp);
        for fact in added {
            let of = Of::fact(fact);
            if let Heard::Held(held) = piece.hear(&of)? {
                piece.held(&held, false)?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}

/// Keeps every ending and what is seen of each node and link of a store whose hearings were
/// written before they were kept: its endings from each link's latest one, and a seen row for
/// each node and link with more than one fact.
pub(crate) fn see_all(conn: &mut Connection) -> Result<(), Error> {
    /// A node or link to see: its entity, key and target key.
    type Of = (String, String, Option<String>);
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // The tables are made in the same transaction as their rows, so a backfill that fails or is
    // cut off leaves no `_seen` behind and runs again; one another open finished first is done.
    if table(&tx, "_seen")? {
        return Ok(());
    }
    tx.execute_batch(HISTORY_TABLES)?;
    tx.execute(
        "INSERT OR IGNORE INTO _endings(entity, key, to_key, point, event, source, valid) \
         SELECT entity, key, to_key, point, event, source, valid FROM _ends \
         WHERE point IS NOT NULL",
        [],
    )?;
    let many: Vec<Of> = tx
        .prepare(
            "SELECT entity, key, to_key FROM (SELECT entity, key, to_key FROM _hearings \
             UNION ALL SELECT entity, key, to_key FROM _endings) \
             GROUP BY entity, key, to_key HAVING count(*) > 1",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    for (entity, key, to) in many {
        see(&tx, (&entity, &key, to.as_deref()))?;
    }
    tx.commit()?;
    Ok(())
}

/// Gives each alias of a store written before aliases kept a point the point of its event id,
/// in one transaction. A store with no aliases yet makes them with points.
pub(crate) fn point_aliases(conn: &mut Connection) -> Result<(), Error> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let columns = "SELECT 1 FROM pragma_table_info('_aliases')";
    let unpointed: bool = tx.query_row(
        &format!("SELECT EXISTS({columns}) AND NOT EXISTS({columns} WHERE name = 'point')"),
        [],
        |row| row.get(0),
    )?;
    if unpointed {
        tx.execute_batch("ALTER TABLE _aliases ADD COLUMN point INTEGER")?;
        for (identity, alias, _) in aliases(&tx)? {
            let id: EventId = serde_json::from_value(alias.clone().into())?;
            tx.prepare_cached("UPDATE _aliases SET point = ?3 WHERE identity = ?1 AND alias = ?2")?
                .execute(params![identity, alias, id.millis()])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Keeps what is seen of `of` (entity, key, target key) from its hearings and endings as they are
/// now, when it has more than one fact; a node or link with one fact keeps no row. Its old row,
/// if any, is dropped first.
pub(crate) fn see(
    tx: &Connection,
    (entity, key, to): (&str, &str, Option<&str>),
) -> Result<(), Error> {
    tx.prepare_cached("DELETE FROM _seen WHERE entity = ?1 AND key = ?2 AND to_key = ?3")?
        .execute(params![entity, key, to.unwrap_or_default()])?;
    let facts: i64 = tx
        .prepare_cached(
            "SELECT (SELECT count(*) FROM _hearings WHERE entity = ?1 AND key = ?2 \
             AND to_key IS ?3) + (SELECT count(*) FROM _endings WHERE entity = ?1 AND key = ?2 \
             AND to_key = coalesce(?3, ''))",
        )?
        .query_row(params![entity, key, to], |row| row.get(0))?;
    if facts < 2 {
        return Ok(());
    }
    let Some((first, last)) = edges(tx, (entity, key, to))? else {
        return Ok(());
    };
    let day = valid_day("valid");
    tx.prepare_cached(&format!(
        "INSERT INTO _seen SELECT ?1, ?2, coalesce(?3, ''), ?4, ?5, ?6, ?7, \
         (SELECT min(arrived) FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS ?3), \
         (SELECT min(id) FROM _endings WHERE entity = ?1 AND key = ?2 AND to_key = ?3 \
         AND NOT EXISTS (SELECT 1 FROM _hearings WHERE entity = ?1 AND key = ?2 \
         AND to_key IS ?3)), (SELECT valid FROM (SELECT valid FROM _hearings \
         WHERE entity = ?1 AND key = ?2 AND to_key IS ?3 UNION ALL SELECT valid \
         FROM _endings WHERE entity = ?1 AND key = ?2 AND to_key = ?3) \
         WHERE valid IS NOT NULL ORDER BY {day} LIMIT 1), \
         (SELECT count(*) FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS ?3) \
         + (SELECT count(*) FROM _endings WHERE entity = ?1 AND key = ?2 AND to_key = ?3)"
    ))?
    .execute(params![
        entity,
        key,
        to,
        first.hearing,
        first.ending,
        last.hearing,
        last.ending
    ])?;
    Ok(())
}

/// Moves the history and links of the node `old` of `class` to the node `new`, which replaced
/// it: its hearings, and every link it is an end of, with their hearings, conclusions, endings
/// and derivations, keep what they are under the new key. A moved link that lands on a link
/// already held becomes that link, holding both sets of hearings; of two conclusions the one
/// later by point and then event id is kept. What is seen of each and their current state are read again from their
/// hearings. The old node keeps its row and nothing else. Replacing and replaying a replacement
/// both move through here.
pub(crate) fn carry(
    tx: &Connection,
    ontology: &Ontology,
    key: &Key,
    (class, old, new): (&str, &str, &str),
) -> Result<(), Error> {
    let mut moved = Vec::new();
    for relation in ontology.relations.values() {
        let entity = link_entity(&relation.name);
        let links: Vec<(String, String)> = tx
            .prepare_cached(
                "SELECT DISTINCT key, to_key FROM _hearings WHERE entity = ?1 \
                 AND ((?2 AND key = ?4) OR (?3 AND to_key = ?4))",
            )?
            .query_map(
                params![entity, relation.from == class, relation.to == class, old],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
            .collect::<Result<_, _>>()?;
        for (from, to) in links {
            let end = |at: &str, of: &str| {
                if of == class && at == old {
                    new.to_string()
                } else {
                    at.to_string()
                }
            };
            let (f, t) = (end(&from, &relation.from), end(&to, &relation.to));
            crate::purge::drop_link(tx, ontology, (&relation.name, &from, &to))?;
            let keys = params![entity, from, to, f, t];
            tx.prepare_cached(
                "UPDATE _hearings SET key = ?4, to_key = ?5 \
                 WHERE entity = ?1 AND key = ?2 AND to_key = ?3",
            )?
            .execute(keys)?;
            tx.prepare_cached(&format!(
                "INSERT INTO _ends SELECT entity, ?4, ?5, event, source, point, valid FROM \
                 (SELECT NULL AS integer, * FROM _ends WHERE entity = ?1 \
                 AND ((key = ?2 AND to_key = ?3) OR (key = ?4 AND to_key = ?5)) \
                 ORDER BY {} LIMIT 1) WHERE true \
                 ON CONFLICT(entity, key, to_key) DO UPDATE SET event = excluded.event, \
                 source = excluded.source, point = excluded.point, valid = excluded.valid",
                order("DESC")
            ))?
            .execute(keys)?;
            tx.prepare_cached(
                "UPDATE OR IGNORE _endings SET key = ?4, to_key = ?5 \
                 WHERE entity = ?1 AND key = ?2 AND to_key = ?3",
            )?
            .execute(keys)?;
            for table in ["_ends", "_endings", "_seen"] {
                tx.prepare_cached(&format!(
                    "DELETE FROM {table} WHERE entity = ?1 AND key = ?2 AND to_key = ?3"
                ))?
                .execute(params![entity, from, to])?;
            }
            moved.push((entity.clone(), f, Some(t)));
        }
    }
    let node = node_entity(class);
    tx.prepare_cached(
        "UPDATE _hearings SET key = ?3 WHERE entity = ?1 AND key = ?2 AND to_key IS NULL",
    )?
    .execute(params![node, old, new])?;
    tx.prepare_cached("UPDATE OR IGNORE _derived SET key = ?3 WHERE class = ?1 AND key = ?2")?
        .execute(params![class, old, new])?;
    tx.prepare_cached("DELETE FROM _derived WHERE class = ?1 AND key = ?2")?
        .execute(params![class, old])?;
    tx.prepare_cached("DELETE FROM _seen WHERE entity = ?1 AND key = ?2 AND to_key = ''")?
        .execute(params![node, old])?;
    moved.insert(0, (node, new.to_string(), None));
    for of in &moved {
        see(tx, (&of.0, &of.1, of.2.as_deref()))?;
        crate::purge::current(tx, ontology, key, of, &mut crate::purge::Left::default())?;
    }
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

impl Graph {
    /// How the link of `relation` from the node keyed `from` to the node keyed `to` came to an
    /// end: found wrong, when a correction is its latest conclusion to arrive; else its ending,
    /// the earliest after its last live hearing by point and then event id; `None` while it has
    /// neither ended nor been found wrong, and once heard live after its endings. Its hearings
    /// stay in [`Graph::link_hearings`] either way.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn link_end(&self, relation: &str, from: &str, to: &str) -> Result<Option<End>, Error> {
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        let link = params![link_entity(&relation), from, to];
        let corrected: Option<(String, String)> = self
            .conn
            .prepare_cached(
                "SELECT event, source FROM _ends \
                 WHERE entity = ?1 AND key = ?2 AND to_key = ?3 AND point IS NULL",
            )?
            .query_row(link, |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        if let Some((event, source)) = corrected {
            let event = serde_json::from_value(event.into())?;
            return Ok(Some(End::Corrected { source, event }));
        }
        let (ending, _) = ending_sql("e", ("?1", "?2", "?3"), None, "NULL");
        let row: Option<(String, String, i64, Option<String>)> = self
            .conn
            .prepare_cached(&format!(
                "SELECT event, source, point, valid FROM _endings e WHERE {ending}"
            ))?
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

impl At {
    /// The point or key as the axis column holds it: a time as milliseconds since the epoch.
    pub(crate) fn bound(self) -> i64 {
        match self {
            At::Time(time) => millis(time),
            At::Key(key) => key,
        }
    }
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

/// The order versions are chosen by, `DESC` putting the newest first: by integer key (only an
/// integer-keyed class's rows hold one), then point, then event id.
pub(crate) fn order(direction: &str) -> String {
    format!("integer {direction}, point {direction}, event {direction}")
}

/// Every occurrence of the hearings `filter` picks, over their `_hearings` row `h`: its own, at
/// its row's point under its event, and each alias's, at the alias's point under the alias. A
/// row carries its hearing's columns, `name` the event the hearing is named by, and the
/// occurrence's `point` and `event`, which [`order`] picks by.
fn occurrences(filter: &str) -> String {
    let hearing = "h.arrived, h.identity, h.event AS name, h.source, h.valid, h.integer, \
                   h.recorded, h.properties";
    format!(
        "(SELECT {hearing}, h.point AS point, h.event AS event FROM _hearings h WHERE {filter} \
         UNION ALL SELECT {hearing}, a.point, a.alias FROM _hearings h \
         JOIN _aliases a ON a.identity = h.identity WHERE {filter})"
    )
}

/// The occurrences of the node or link `?1`, `?2`, `?3` (entity, key, target key).
fn occurrences_of() -> String {
    occurrences("h.entity = ?1 AND h.key = ?2 AND h.to_key IS ?3")
}

/// The `column` of the newest occurrence of the hearing whose `arrived` is `hearing` (SQL).
fn newest_occurrence(hearing: &str, column: &str) -> String {
    format!(
        "(SELECT {column} FROM {} ORDER BY {} LIMIT 1)",
        occurrences(&format!("h.arrived = {hearing}")),
        order("DESC")
    )
}

/// Picks the newest version row at or before the point `?4`, in a class not ordered by an
/// integer key (and for a link, which is never ordered by one).
fn by_point() -> String {
    format!(
        "AND integer IS NULL AND point <= ?4 ORDER BY {} LIMIT 1",
        order("DESC")
    )
}

/// One version row: the version, when this store recorded it and its content identity.
pub(crate) type Versioned = (Version, i64, [u8; 32]);

/// Reads one version row of `entity` keyed `key` (and `to`, for a link), as `tail` picks it
/// among its hearings' occurrences, with `bound` bound from `?4` on: see [`Versioned`]. The
/// version is named by its hearing's event, at the occurrence's point.
fn version_row(
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
    let sql = format!(
        "SELECT name, source, valid, point, integer, properties, recorded, identity \
         FROM {} WHERE true {tail}",
        occurrences_of()
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
    class: &crate::Class,
    key: &str,
    at: i64,
) -> Result<Option<Versioned>, Error> {
    let tail = if class.history == HistoryKey::Integer {
        format!("AND integer <= ?4 ORDER BY {} LIMIT 1", order("DESC"))
    } else {
        by_point()
    };
    version_row(conn, (&node_entity(&class.name), key, None), &tail, &[&at])
}

/// A valid time in `column` as `YYYY-MM-DD`: a month (7 characters) stands for its first day,
/// and an unknown valid time is NULL, which no comparison holds for.
pub(crate) fn valid_day(column: &str) -> String {
    format!("(CASE WHEN length({column}) = 7 THEN {column} || '-01' ELSE {column} END)")
}

/// What a read known at `at` bounds the versions of `class` by, or of a link when `class` is
/// `None`: the column on its axis and the bound. A link is read at a time only.
pub(crate) fn known_bound(
    class: Option<&crate::Class>,
    at: At,
) -> Result<(&'static str, i64), Error> {
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

/// The version of `of` true at the valid time `day` (`YYYY-MM-DD`), as known at `known` (its
/// axis column and bound) when given: among its versions valid on or before `day`, the greatest
/// by valid time, then by its class's order. A version of unknown valid time is never one.
fn version_as_of(
    conn: &Connection,
    of: (&str, &str, Option<&str>),
    day: &str,
    known: Option<(&str, i64)>,
) -> Result<Option<Versioned>, Error> {
    let valid = valid_day("valid");
    let by = format!("ORDER BY {valid} DESC, {} LIMIT 1", order("DESC"));
    match known {
        None => version_row(conn, of, &format!("AND {valid} <= ?4 {by}"), &[&day]),
        Some((column, at)) => version_row(
            conn,
            of,
            &format!("AND {valid} <= ?4 AND {column} <= ?5 {by}"),
            &[&day, &at],
        ),
    }
}

/// The version of the node keyed `key` in `class` true at the valid time `day`, as known at
/// `known` when given; see [`version_as_of`].
pub(crate) fn node_as_of(
    conn: &Connection,
    class: &crate::Class,
    key: &str,
    day: &str,
    known: Option<At>,
) -> Result<Option<Versioned>, Error> {
    let known = known.map(|at| known_bound(Some(class), at)).transpose()?;
    version_as_of(conn, (&node_entity(&class.name), key, None), day, known)
}

/// The version of the link of `relation` from `from` to `to` true at the valid time `day`, as
/// known at `known` when given, and whether it had ended by then: an ending heard by `known`
/// whose valid time is on or before `day`.
///
/// # Errors
/// [`Error::Refused`] when the link held then and its ending, heard by `known`, has an unknown
/// valid time, or `known` is an integer key.
pub(crate) fn link_as_of(
    conn: &Connection,
    relation: &str,
    (from, to): (&str, &str),
    day: &str,
    known: Option<At>,
) -> Result<Option<Version>, Error> {
    let known = known.map(|at| known_bound(None, at)).transpose()?;
    let entity = link_entity(relation);
    let Some((mut version, ..)) = version_as_of(conn, (&entity, from, Some(to)), day, known)?
    else {
        return Ok(None);
    };
    let known = known.map_or(i64::MAX, |(_, at)| at);
    version.ended = ended(conn, relation, (from, to), known, Some(day))?;
    Ok(Some(version))
}

/// The SQL deciding a link's ending, the `_endings` row `e`, of the link `(entity, from, to)` (SQL
/// expressions): that it is the link's ending by the point `known` when given, and that its
/// valid time is on or before `day`, NULL unknown. Its ending by then is the earliest ending,
/// by point and then event id, after its last live hearing by then; there is none once a live
/// hearing comes after every ending, or while a correction is its latest conclusion. Only the
/// endings after that hearing are read, never its history.
pub(crate) fn ending_sql(
    e: &str,
    (entity, from, to): (&str, &str, &str),
    known: Option<&str>,
    day: &str,
) -> (String, String) {
    let link =
        |t: &str| format!("{t}.entity = {entity} AND {t}.key = {from} AND {t}.to_key = {to}");
    let by = |t: &str| known.map_or(String::new(), |known| format!(" AND {t}.point <= {known}"));
    // The last live hearing's point and event, by its occurrences, or below every ending when
    // there is none.
    let live = |column: &str, none: &str| {
        format!(
            "coalesce((SELECT {column} FROM {} WHERE true{} ORDER BY {} LIMIT 1), {none})",
            occurrences(&format!("{} AND h.integer IS NULL", link("h"))),
            known.map_or(String::new(), |known| format!(" AND point <= {known}")),
            order("DESC")
        )
    };
    let after = live("point", &i64::MIN.to_string());
    (
        format!(
            "{e}.id = (SELECT x.id FROM _endings x \
             WHERE {link_x}{by_x} AND x.point >= {after} AND (x.point, x.event) > ({after}, {}) \
             ORDER BY x.point, x.event LIMIT 1) \
             AND NOT EXISTS (SELECT 1 FROM _ends c WHERE {link_c} AND c.point IS NULL)",
            live("event", "''"),
            link_x = link("x"),
            by_x = by("x"),
            link_c = link("c"),
        ),
        format!("{} <= {day}", valid_day(&format!("{e}.valid"))),
    )
}

/// The one read of what is seen of `?1`, `?2`, `?3` (its target key `''` for a node): its kept
/// row, with the facts it names, each a hearing joined by when it arrived or an ending by its
/// id; or, for a node or link with one fact, which keeps no row, that fact. Then when a link
/// ended: count, first true, when it ended, then first heard, last heard and first recorded as
/// event, source, valid time, point, integer key, properties and whether an ending.
fn seen_sql() -> String {
    let fact = |p: &str, point: &str| {
        format!(
            "coalesce({p}h.event, {p}e.event), coalesce({p}h.source, {p}e.source), \
             CASE WHEN {p}h.arrived IS NULL THEN {p}e.valid ELSE {p}h.valid END, \
             coalesce({point}, {p}e.point), {p}h.integer, {p}h.properties, \
             {p}h.arrived IS NULL"
        )
    };
    let join = |p: &str, column: &str| {
        format!(
            "LEFT JOIN _hearings {p}h ON {p}h.arrived = s.{column}_hearing \
             LEFT JOIN _endings {p}e ON {p}e.id = s.{column}_ending"
        )
    };
    let (ending, _) = ending_sql("n", ("?1", "?2", "?3"), None, "NULL");
    // A node or link with one fact, a hearing or an ending, keeps no row: that fact is read.
    let one = |fact: &str, from: &str, to: &str| {
        format!(
            "UNION ALL SELECT 1, valid, (SELECT n.point FROM _endings n WHERE {ending}), \
             {fact}, {fact}, {fact} FROM {from} WHERE entity = ?1 AND key = ?2 AND {to} \
             AND NOT EXISTS (SELECT 1 FROM _seen WHERE entity = ?1 AND key = ?2 AND to_key = ?3)"
        )
    };
    format!(
        "SELECT s.count, s.first_true, (SELECT n.point FROM _endings n WHERE {ending}), \
         {}, {}, {} FROM _seen s {} {} {} WHERE s.entity = ?1 AND s.key = ?2 AND s.to_key = ?3 \
         {} {} LIMIT 1",
        fact("f", "fh.point"),
        // A hearing heard again is last heard at its newest occurrence.
        fact("l", &newest_occurrence("s.last_hearing", "point")),
        fact("r", "rh.point"),
        join("f", "first"),
        join("l", "last"),
        join("r", "recorded"),
        one(
            "event, source, valid, point, integer, properties, 0",
            "_hearings",
            "to_key IS nullif(?3, '')"
        ),
        one(
            "event, source, valid, point, NULL, NULL, 1",
            "_endings",
            "to_key = ?3"
        ),
    )
}

/// Whether the link `?1`, `?2`, `?3` had ended by the point `?4`, and as of the valid time `?5`
/// when given (NULL when its ending's valid time is unknown); no row when it had not ended.
fn ended_sql() -> String {
    let (ending, valid_by) = ending_sql("e", ("?1", "?2", "?3"), Some("?4"), "?5");
    format!("SELECT ?5 IS NULL OR {valid_by} FROM _endings e WHERE {ending}")
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
        Ok(node_at(&self.conn, class, &node.key, axis(class, at)?)?.map(|(v, ..)| v))
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
        let (_, at) = known_bound(None, at)?;
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        let entity = link_entity(&relation);
        let Some((mut version, ..)) =
            version_row(&self.conn, (&entity, from, Some(to)), &by_point(), &[&at])?
        else {
            return Ok(None);
        };
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{
        Class, Cursor, Date, Declaration, End, NodeRef, Relation, Unit, ValidTime, Write, open,
        rebuild,
    };

    /// A graph where `v2` and `v3` each know `v1`, from different sources, and `v3` was
    /// replaced by `v2`.
    fn replaced(dir: &tempfile::TempDir) -> Result<crate::Graph, Box<dyn std::error::Error>> {
        let mut g = open(dir.path().join("live.rannoch"), Some(&crate::TEST_KEY))?;
        let when = ValidTime::On(Date::day(2021, 3, 2));
        g.declare(&Declaration::Class(Class::new("voice", [] as [&str; 0])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "knows", "voice", "voice", false,
        )))?;
        let voice = |key: &str| Unit::new("voice", key, [] as [(&str, &str); 0], when.clone());
        g.write(
            &Write::new("swtor").unit(voice("v1")).unit(voice("v2").link(
                "knows",
                "v2",
                "v1",
                when.clone(),
            )),
        )?;
        g.write(&Write::new("reddit").unit(voice("v3").link("knows", "v3", "v1", when.clone())))?;
        g.replace("voice", "v3", "v2")?;
        Ok(g)
    }

    #[test]
    fn a_moved_link_landing_on_a_link_held_merges_into_one_with_both_hearings()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let g = replaced(&dir)?;
        let sources: Vec<String> = g
            .link_hearings("knows", "v2", "v1")?
            .into_iter()
            .map(|h| h.source)
            .collect();
        assert_eq!(sources.len(), 2, "{sources:?}");
        assert!(sources.contains(&"swtor".into()) && sources.contains(&"reddit".into()));
        assert!(g.link_hearings("knows", "v3", "v1")?.is_empty());
        assert!(g.find_link("knows", "v3", "v1")?.is_none());
        let rows: i64 = g.conn.query_row(
            &format!(
                "SELECT count(*) FROM {}",
                crate::open::relation_table("knows")
            ),
            [],
            |row| row.get(0),
        )?;
        assert_eq!(rows, 1);
        assert_eq!(g.link_seen("knows", "v2", "v1")?.map(|s| s.count), Some(2));
        assert!(g.hearings(&NodeRef::new("voice", "v3"))?.is_empty());
        assert_eq!(g.hearings(&NodeRef::new("voice", "v2"))?.len(), 2);
        Ok(())
    }

    #[test]
    fn a_rebuild_after_a_replace_reads_the_hearings_and_links_of_the_live_graph()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let g = replaced(&dir)?;
        let events = g.events().since(Cursor::START)?;
        let back = rebuild(
            &events,
            dir.path().join("back.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        for key in ["v1", "v2", "v3"] {
            let node = NodeRef::new("voice", key);
            assert_eq!(back.hearings(&node)?, g.hearings(&node)?);
            let found = |g: &crate::Graph| -> Result<_, crate::Error> {
                Ok(g.find("voice", key)?
                    .map(|n| (n.source, n.valid, n.properties)))
            };
            assert_eq!(found(&back)?, found(&g)?);
            assert_eq!(back.seen(&node)?, g.seen(&node)?);
        }
        for from in ["v2", "v3"] {
            assert_eq!(
                back.link_hearings("knows", from, "v1")?,
                g.link_hearings("knows", from, "v1")?
            );
            let link = |g: &crate::Graph| -> Result<_, crate::Error> {
                Ok(g.find_link("knows", from, "v1")?
                    .map(|l| (l.source, l.valid)))
            };
            assert_eq!(link(&back)?, link(&g)?);
        }
        Ok(())
    }

    #[test]
    fn a_pre_key_store_with_a_replacement_takes_its_key_with_what_the_replacement_moved()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let g = replaced(&dir)?;
        let nodes = ["v1", "v2", "v3"].map(|key| NodeRef::new("voice", key));
        let heard = |g: &crate::Graph| -> Result<_, crate::Error> {
            let mut all = Vec::new();
            for node in &nodes {
                all.push((g.hearings(node)?, g.seen(node)?));
            }
            for from in ["v2", "v3"] {
                all.push((
                    g.link_hearings("knows", from, "v1")?,
                    g.link_seen("knows", from, "v1")?,
                ));
            }
            Ok(all)
        };
        let keyed = heard(&g)?;
        // As a store written before keys: no check value, no hearings and nothing seen.
        g.conn.execute_batch(
            "DELETE FROM _check; DELETE FROM _hearings; DELETE FROM _aliases; DELETE FROM _seen;",
        )?;
        drop(g);
        let g = open(dir.path().join("live.rannoch"), Some(&crate::TEST_KEY))?;
        assert_eq!(heard(&g)?, keyed);
        assert_eq!(g.link_hearings("knows", "v2", "v1")?.len(), 2);
        Ok(())
    }

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
        let at = UNIX_EPOCH + Duration::from_millis(4_102_444_800_123);
        let ended = ValidTime::On(Date::day(2025, 6, 1));
        g.end_link("knows", "v2", "v1", at, ended, "moderator")?;
        g.correct_link("knows", "v3", "v1", "moderator")?;
        assert!(
            g.end_link("knows", "v1", "v3", at, ValidTime::Unknown, "moderator")
                .is_err()
        );

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
    fn a_store_written_before_seen_was_kept_is_seen_from_its_hearings_and_endings()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("before-seen.rannoch");
        let when = ValidTime::On(Date::day(2024, 3, 1));
        let link = ("knows", "v2", "v1");
        let read = |g: &crate::Graph| -> Result<_, crate::Error> {
            let node = g.seen(&NodeRef::new("voice", "v2"))?;
            Ok((
                node,
                g.link_seen(link.0, link.1, link.2)?,
                g.link_end(link.0, link.1, link.2)?,
            ))
        };
        let kept = {
            let mut g = open(&path, Some(&crate::TEST_KEY))?;
            g.declare(&Declaration::Class(Class::new("voice", ["name"])))?;
            g.declare(&Declaration::Relation(Relation::new(
                "knows", "voice", "voice", false,
            )))?;
            let voice = |key: &str, name: &str, valid: ValidTime| {
                Unit::new("voice", key, [("name", name)], valid)
            };
            g.write(
                &Write::new("swtor")
                    .unit(voice("v1", "Ana", when.clone()))
                    .unit(voice("v2", "Bo", when.clone()).link("knows", "v2", "v1", when.clone())),
            )?;
            let earlier = ValidTime::On(Date::day(2023, 1, 1));
            g.write(&Write::new("reddit").unit(voice("v2", "Bob", earlier)))?;
            let at = UNIX_EPOCH + Duration::from_millis(4_102_444_800_123);
            g.end_link(link.0, link.1, link.2, at, ValidTime::Unknown, "moderator")?;
            let kept = read(&g)?;
            assert_eq!(kept.0.as_ref().map(|s| s.count), Some(2));
            assert_eq!(
                kept.1.as_ref().map(|s| (s.count, s.ended)),
                Some((2, Some(at)))
            );
            // As a store written before endings and what is seen were kept.
            g.conn
                .execute_batch("DROP TABLE _seen; DROP TABLE _endings;")?;
            kept
        };
        let g = open(&path, Some(&crate::TEST_KEY))?;
        assert_eq!(read(&g)?, kept);
        // A snapshot, and a graph rebuilt from it, are seen as the graph is.
        let snapshot = g.snapshot(g.events().committed()?, dir.path().join("snap.rannoch"))?;
        let back =
            snapshot.rebuild(&[], dir.path().join("back.rannoch"), Some(&crate::TEST_KEY))?;
        assert_eq!(read(&back)?, kept);
        for fixture in ["store-0.1.1.rannoch", "store-0.2.0.rannoch"] {
            let copy = dir.path().join(fixture);
            std::fs::copy(
                format!("{}/tests/fixtures/{fixture}", env!("CARGO_MANIFEST_DIR")),
                &copy,
            )?;
            let g = open(&copy, Some(&crate::TEST_KEY))?;
            let seen = g.seen(&NodeRef::new("book", "b1"))?;
            assert_eq!(seen.map(|s| s.count), Some(1), "{fixture}");
        }
        Ok(())
    }

    /// A backfill that fails leaves the store as it was, so the next open runs it again.
    #[test]
    fn a_backfill_that_fails_runs_again_on_the_next_open() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("before-seen.rannoch");
        let when = ValidTime::On(Date::day(2024, 3, 1));
        let read = |g: &crate::Graph| -> Result<_, crate::Error> {
            Ok((
                g.seen(&NodeRef::new("voice", "v2"))?,
                g.link_seen("knows", "v2", "v1")?,
            ))
        };
        let kept = {
            let mut g = open(&path, Some(&crate::TEST_KEY))?;
            g.declare(&Declaration::Class(Class::new("voice", ["name"])))?;
            g.declare(&Declaration::Relation(Relation::new(
                "knows", "voice", "voice", false,
            )))?;
            let voice =
                |key: &str, name: &str| Unit::new("voice", key, [("name", name)], when.clone());
            g.write(
                &Write::new("swtor")
                    .unit(voice("v1", "Ana"))
                    .unit(voice("v2", "Bo").link("knows", "v2", "v1", when.clone())),
            )?;
            g.write(&Write::new("reddit").unit(voice("v2", "Bob")))?;
            let at = UNIX_EPOCH + Duration::from_millis(4_102_444_800_123);
            g.end_link("knows", "v2", "v1", at, ValidTime::Unknown, "moderator")?;
            let kept = read(&g)?;
            assert_eq!(kept.0.as_ref().map(|s| s.count), Some(2));
            // As a store written before what is seen was kept, whose backfill is cut off.
            g.conn.execute_batch(
                "DROP TABLE _seen; CREATE TRIGGER cut BEFORE INSERT ON _endings \
                 BEGIN SELECT RAISE(ABORT, 'cut off'); END;",
            )?;
            kept
        };
        assert!(open(&path, Some(&crate::TEST_KEY)).is_err());
        rusqlite::Connection::open(&path)?.execute_batch("DROP TRIGGER cut;")?;
        let g = open(&path, Some(&crate::TEST_KEY))?;
        assert_eq!(read(&g)?, kept);
        Ok(())
    }

    /// The read of what is seen and the read of an ending each seek by key: neither scans the
    /// hearings nor the endings, the endings are sought from after the last live hearing, and
    /// neither reads the log.
    #[test]
    fn the_seen_read_and_the_ending_read_seek_and_never_scan()
    -> Result<(), Box<dyn std::error::Error>> {
        let g = open(
            tempfile::tempdir()?.path().join("plans.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        for sql in [super::seen_sql(), super::ended_sql()] {
            let mut explain = g.conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
            let unbound = vec![rusqlite::types::Null; explain.parameter_count()];
            let plan: Vec<String> = explain
                .query_map(rusqlite::params_from_iter(unbound), |row| row.get(3))?
                .collect::<Result<_, _>>()?;
            println!("{}", plan.join("\n"));
            // A hearing's occurrences are sought by its row, then read in turn: no table is
            // scanned.
            let scans = |step: &&String| step.contains("SCAN") && !step.contains("(subquery-");
            assert!(!plan.iter().any(|step| scans(&step)), "{plan:#?}");
            // The endings are read from after the last live hearing: the trailing run only.
            let mut endings = plan.iter().filter(|step| step.starts_with("SEARCH x "));
            assert!(endings.all(|step| step.contains("point>?")), "{plan:#?}");
            assert!(!sql.contains("_events"), "{sql}");
        }
        Ok(())
    }

    /// A graph of a class with no key whose node k1 one source wrote as v1, then v2, then v1
    /// again, each in its own commit a few milliseconds apart; with the events each wrote.
    fn returned(
        path: &std::path::Path,
    ) -> Result<(crate::Graph, Vec<crate::Event>), Box<dyn std::error::Error>> {
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("note", ["text"])))?;
        let mut written = Vec::new();
        for text in ["v1", "v2", "v1"] {
            let when = ValidTime::On(Date::day(2026, 10, 1));
            let events = g.write(&Write::new("crawler").unit(Unit::new(
                "note",
                "k1",
                [("text", text)],
                when,
            )))?;
            assert_eq!(events.len(), 1, "{text} wrote its event");
            written.extend(events);
            std::thread::sleep(Duration::from_millis(3));
        }
        Ok((g, written))
    }

    fn text(version: Option<crate::Version>) -> Option<crate::Value> {
        version.and_then(|v| v.properties.get("text").cloned())
    }

    #[test]
    fn a_return_holds_from_its_own_point_and_v2_holds_between()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let (g, written) = returned(&dir.path().join("return.rannoch"))?;
        let k1 = NodeRef::new("note", "k1");
        let point =
            |i: usize| UNIX_EPOCH + Duration::from_millis(written[i].id.millis().cast_unsigned());
        let between = super::At::Time(point(2) - Duration::from_millis(1));
        assert!(point(1) <= point(2) - Duration::from_millis(1));
        assert_eq!(text(g.state_at(&k1, between)?), Some("v2".into()));
        assert_eq!(
            text(g.state_at(&k1, super::At::Time(point(2)))?),
            Some("v1".into())
        );
        let seen = g.seen(&k1)?.ok_or("k1 was seen")?;
        assert_eq!(
            (seen.count, seen.first_heard.point, seen.last_heard.point),
            (2, point(0), point(2))
        );
        assert_eq!(
            seen.last_heard.event, written[0].id,
            "named by its hearing's event"
        );
        Ok(())
    }

    #[test]
    fn rewriting_the_current_version_after_a_return_writes_no_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let (mut g, written) = returned(&dir.path().join("rewrite.rannoch"))?;
        let before = g.events().since(Cursor::START)?.len();
        let again = g.write(&Write::new("crawler").unit(Unit::new(
            "note",
            "k1",
            [("text", "v1")],
            ValidTime::On(Date::day(2026, 10, 1)),
        )))?;
        assert_eq!(g.events().since(Cursor::START)?.len(), before);
        let ids: Vec<_> = again.iter().map(|e| e.id).collect();
        assert_eq!(ids, [written[0].id], "the held hearing's event is returned");
        Ok(())
    }

    #[test]
    fn a_store_whose_aliases_kept_no_point_points_them_at_their_event_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("old-aliases.rannoch");
        let written = {
            let (g, written) = returned(&path)?;
            // As a store written before aliases kept a point.
            g.conn.execute_batch(
                "CREATE TABLE old(identity BLOB NOT NULL, alias TEXT NOT NULL, \
                 PRIMARY KEY(identity, alias)) WITHOUT ROWID; \
                 INSERT INTO old SELECT identity, alias FROM _aliases; \
                 DROP TABLE _aliases; ALTER TABLE old RENAME TO _aliases;",
            )?;
            written
        };
        let g = open(&path, Some(&crate::TEST_KEY))?;
        let points: Vec<(String, Option<i64>)> = g
            .conn
            .prepare("SELECT alias, point FROM _aliases")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        assert_eq!(
            points,
            [(written[2].id.to_string(), Some(written[2].id.millis()))]
        );
        let indexed: bool = g.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_index_info('_aliases_at'))",
            [],
            |row| row.get(0),
        )?;
        assert!(indexed);
        let k1 = g.find("note", "k1")?.ok_or("k1 is in the graph")?;
        assert_eq!(k1.properties.get("text"), Some(&"v1".into()));
        Ok(())
    }

    #[test]
    fn resending_a_time_keyed_version_after_a_newer_one_writes_no_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = open(dir.path().join("timed.rannoch"), Some(&crate::TEST_KEY))?;
        let mut class = Class::new("visit", ["text"]);
        class.history = crate::HistoryKey::Time;
        g.declare(&Declaration::Class(class))?;
        let at = |minute: u64| UNIX_EPOCH + Duration::from_secs(1_790_000_000 + minute * 60);
        let visit = |text: &str, minute: u64| {
            let when = ValidTime::On(Date::day(2026, 10, 1));
            Write::new("crawler")
                .unit(Unit::new("visit", "k1", [("text", text)], when).at(at(minute)))
        };
        let first = g.write(&visit("v1", 1))?;
        g.write(&visit("v2", 2))?;
        let before = g.events().since(Cursor::START)?.len();
        let again = g.write(&visit("v1", 1))?;
        assert_eq!(g.events().since(Cursor::START)?.len(), before);
        let ids: Vec<_> = again.iter().map(|e| e.id).collect();
        assert_eq!(ids, [first[0].id], "the held hearing's event is returned");
        let k1 = g.find("visit", "k1")?.ok_or("k1 is in the graph")?;
        assert_eq!(k1.properties.get("text"), Some(&"v2".into()));
        Ok(())
    }

    #[test]
    fn a_link_ended_then_heard_again_as_before_reads_live_after_that_occurrence()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = open(dir.path().join("link.rannoch"), Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("voice", [] as [&str; 0])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "knows", "voice", "voice", false,
        )))?;
        let first = ValidTime::On(Date::day(2021, 3, 2));
        let knows = |valid: &ValidTime| {
            let voice = |key: &str| Unit::new("voice", key, [] as [(&str, &str); 0], valid.clone());
            Write::new("swtor").unit(voice("v1")).unit(voice("v2").link(
                "knows",
                "v2",
                "v1",
                valid.clone(),
            ))
        };
        let pause = || std::thread::sleep(Duration::from_millis(3));
        g.write(&knows(&first))?;
        pause();
        g.write(&knows(&ValidTime::On(Date::day(2022, 4, 5))))?;
        pause();
        let ended = std::time::SystemTime::now();
        g.end_link("knows", "v2", "v1", ended, ValidTime::Unknown, "moderator")?;
        assert!(g.link_end("knows", "v2", "v1")?.is_some());
        pause();
        g.write(&knows(&first))?;
        assert_eq!(g.link_end("knows", "v2", "v1")?, None);
        let now = super::At::Time(std::time::SystemTime::now());
        let state = g
            .link_state_at("knows", "v2", "v1", now)?
            .ok_or("the link held")?;
        assert!(!state.ended, "{state:?}");
        let before = g.link_state_at("knows", "v2", "v1", super::At::Time(ended))?;
        assert!(before.is_some_and(|v| v.ended));
        assert_eq!(g.link_hearings("knows", "v2", "v1")?.len(), 2);
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
            crate::events::forget_as_before(&mut g, &NodeRef::new("visitor", "p05"))?;
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
