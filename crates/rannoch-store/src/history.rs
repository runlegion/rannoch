//! Hearings: every write is a hearing of a node or a link, at a point on its class's or
//! relation's axis, known from the millisecond in its `CloudEvent` id. A
//! hearing's identity is its content, hashed under the app's key: identical content is one
//! hearing, and an identical event that arrives from another store under its own id is held as an
//! alias of it. The key never reaches the store's files; the store keeps only a check value.
//! A link that ends records its ending, at its own point, beside its hearings, and a live
//! hearing at a later point reopens it; a link found wrong records a correction, which is not an
//! ending. When each node and link was first and last heard is kept as it is recorded.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rannoch_ontology::{Class, Ontology, Value};
use rusqlite::{Connection, OptionalExtension, ToSql, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use rannoch_cloudevents::{Conclusion, EventId, Fact, LinkFact, NodeFact, NodeRef, ValidTime};

use crate::handles::Run;
use crate::log::Cursor;
use crate::ontology::{CLASS, RELATION};
use crate::queries::{
    ALIASES_OF, CHECK, SET_CHECK, Sql, Stmt, ended_sql, hearings, link_correction, link_ending,
    link_state, seen_sql,
};
use crate::{Error, Graph};

thread_local! {
    /// The greatest known-at fill has written since the last take: what its batch keeps.
    static NOTED: Cell<Option<i64>> = const { Cell::new(None) };
}

/// Notes a known-at fill writes into the history tables.
pub(crate) fn note(known: Option<i64>) {
    if let Some(known) = known {
        NOTED.with(|noted| noted.set(Some(noted.get().map_or(known, |k| k.max(known)))));
    }
}

/// The known-ats noted since the last take, leaving none: an event rolled back drops its own.
pub(crate) fn take_noted() -> Option<i64> {
    NOTED.with(Cell::take)
}

/// Raises the store's highest known-at ever filled to the greatest noted in the batch.
pub(crate) fn keep_noted(tx: &impl Run) -> Result<(), Error> {
    if let Some(known) = take_noted() {
        tx.with(Sql::KeepNewest, |s| s.execute([known]))?;
    }
    Ok(())
}

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

    /// The keyed match value of a purge target of the kind `kind` made of `parts`. Its fixed
    /// label keeps it from ever equalling a content identity.
    pub(crate) fn matched(&self, kind: &str, parts: &[&str]) -> [u8; 32] {
        let named = serde_json::json!(["rannoch purge match", kind, parts]).to_string();
        *blake3::keyed_hash(&self.bytes, named.as_bytes()).as_bytes()
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

    fn axis(&self) -> &BTreeMap<String, i64> {
        match self {
            Of::Node(node) => &node.axis,
            Of::Link(link) => &link.axis,
        }
    }

    /// The axis its class or relation declares, and the kind and name of what declares it.
    fn declared<'o>(
        &'o self,
        ontology: &'o Ontology,
    ) -> Result<(&'o [String], (&'static str, &'o str)), Error> {
        Ok(match self {
            Of::Node(node) => (&ontology.class(&node.class)?.axis, ("class", &node.class)),
            Of::Link(link) => (
                &ontology.relation(&link.relation)?.axis,
                ("relation", &link.relation),
            ),
        })
    }

    /// Its point on its declared axis, encoded; `None` when no axis is declared, so its point is
    /// its known-at.
    fn point(&self, ontology: &Ontology) -> Result<Option<Vec<u8>>, Error> {
        let (declared, owner) = self.declared(ontology)?;
        declared_point(declared, self.axis(), owner)
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

    /// Its identity: its content, with the source it was heard from, hashed under `key`. Its
    /// known-at is not content; its point on a declared axis is.
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
                &n.axis,
            ))?,
            Of::Link(l) => serde_json::to_vec(&(
                "link",
                &l.relation,
                &l.from,
                &l.to,
                &l.valid,
                source,
                &l.axis,
            ))?,
        };
        Ok(*blake3::keyed_hash(&key.bytes, &content).as_bytes())
    }
}

/// An axis point's parts as bytes that compare as the parts do, part by part: each part
/// big-endian with its sign bit flipped, so a byte comparison orders negative before positive.
pub(crate) fn encode(parts: &[i64]) -> Vec<u8> {
    parts
        .iter()
        .flat_map(|part| (part.cast_unsigned() ^ (1 << 63)).to_be_bytes())
        .collect()
}

/// The parts [`encode`] encoded.
pub(crate) fn decode_axis(bytes: &[u8]) -> Vec<i64> {
    bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|part| (u64::from_be_bytes(*part) ^ (1 << 63)).cast_signed())
        .collect()
}

/// The encoded point `given` names on the axis `declared` of `owner` (its kind and name); `None`
/// when no axis is declared, so the point is the fact's known-at.
fn declared_point(
    declared: &[String],
    given: &BTreeMap<String, i64>,
    owner: (&str, &str),
) -> Result<Option<Vec<u8>>, Error> {
    if declared.is_empty() {
        return Ok(None);
    }
    Ok(Some(encode(&crate::fact::point(declared, given, owner)?)))
}

/// The point of a fact known at `known` whose declared point is `declared`: the declared one,
/// else its known-at.
fn point_or_known(declared: Option<&Vec<u8>>, known: i64) -> Vec<u8> {
    declared.cloned().unwrap_or_else(|| encode(&[known]))
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
    /// Its point on a declared axis, which is part of its content; `None` when the axis is its
    /// known-at, so each occurrence has its own.
    axis: Option<Vec<u8>>,
}

/// What [`hear`] did: held an identical hearing, recording nothing, or recorded a new one, which
/// may be its node's or link's only fact.
enum Recorded {
    Held(Held),
    New { alone: bool },
}

/// Records what `of` heard, whose content identity is `identity`, as a new version row named by
/// its origin's event and filled at `stamp`; or, when the graph holds a hearing identical to it,
/// records nothing and returns that one.
fn hear(
    tx: &impl Run,
    identity: [u8; 32],
    stamp: Stamp,
    of: &Of,
    ontology: &Ontology,
) -> Result<Recorded, Error> {
    let held = tx.with(Sql::Held, |s| {
        s.query_row(params![identity], |row| row.get::<_, String>(0))
            .optional()
    })?;
    let declared = of.point(ontology)?;
    if let Some(held) = held {
        return Ok(Recorded::Held(Held {
            identity,
            event: serde_json::from_value(held.into())?,
            axis: declared,
        }));
    }
    let (entity, from, to) = of.entity();
    let properties = match of {
        Of::Node(node) => Some(serde_json::to_string(&node.properties)?),
        Of::Link(_) => None,
    };
    let known = of.event().recorded()?;
    let axis = point_or_known(declared.as_ref(), known);
    note(Some(known));
    tx.with(Sql::Hear, |s| {
        s.execute(params![
            identity,
            entity,
            from,
            to,
            of.event().to_string(),
            stamp.cursor.0,
            of.source(),
            of.valid().as_text(),
            axis,
            known,
            stamp.recorded,
            properties
        ])
    })?;
    let mark = Mark {
        hearing: Some(tx.conn().last_insert_rowid()),
        ending: None,
        axis,
        known,
        event: of.event().to_string(),
    };
    let alone = saw(tx, (&entity, from, to), &mark, of.valid().as_text())?;
    Ok(Recorded::New { alone })
}

/// A fact a kept seen row names, by its row: a hearing by when this store recorded it, or an
/// ending by its id; with what orders it among the others.
struct Mark {
    hearing: Option<i64>,
    ending: Option<i64>,
    axis: Vec<u8>,
    known: i64,
    event: String,
}

impl Mark {
    /// Its place in its class's order (see [`order`]).
    fn rank(&self) -> (&[u8], i64, &str) {
        (&self.axis, self.known, &self.event)
    }

    fn row(row: &rusqlite::Row, at: usize) -> rusqlite::Result<Mark> {
        Ok(Mark {
            hearing: row.get(at)?,
            ending: row.get(at + 1)?,
            axis: row.get(at + 2)?,
            known: row.get(at + 3)?,
            event: row.get(at + 4)?,
        })
    }
}

/// Keeps what is seen of `of` (entity, key, target key) up to date with `mark`, a new hearing or
/// ending of it with the valid time `valid`, in the transaction that records it: the first and
/// last of its facts by its class's order, the first this store recorded, the earliest valid
/// time it was given (a month standing for its first day) and how many facts it has. A link's
/// endings count among its facts, as its versions do. Says whether `mark` is its only fact.
fn saw(
    tx: &impl Run,
    (entity, key, to): (&str, &str, Option<&str>),
    mark: &Mark,
    valid: Option<&str>,
) -> Result<bool, Error> {
    let to = to.unwrap_or_default();
    let kept = tx.with(Sql::SeenKept, |s| {
        s.query_row(params![entity, key, to], |row| {
            Ok((Mark::row(row, 0)?, Mark::row(row, 5)?))
        })
        .optional()
    })?;
    let Some((first, last)) = kept else {
        // One fact keeps no row, since it is its own first, last and first recorded; the second
        // starts the row from the one before it.
        let other = tx.with(Sql::SeenOther, |s| {
            s.query_row(params![entity, key, to, mark.hearing, mark.ending], |row| {
                Ok((Mark::row(row, 0)?, row.get::<_, Option<String>>(5)?))
            })
            .optional()
        })?;
        let Some((other, was)) = other else {
            return Ok(true);
        };
        tx.with(Sql::SeenStart, |s| {
            s.execute(params![entity, key, to, other.hearing, other.ending, was])
        })?;
        let (first, last) = if mark.rank() < other.rank() {
            (mark, &other)
        } else {
            (&other, mark)
        };
        keep(tx, (entity, key, to), (first, last), (1, valid))?;
        return Ok(false);
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
    keep(tx, (entity, key, to), (first, last), (1, valid))?;
    Ok(false)
}

/// Keeps `first` and `last` as the first and last facts of `of` (its target key `''` for a
/// node), adds `added` facts to its count and takes `valid` as its first true when earlier.
fn keep(
    tx: &impl Run,
    (entity, key, to): (&str, &str, &str),
    (first, last): (&Mark, &Mark),
    (added, valid): (i64, Option<&str>),
) -> Result<(), Error> {
    tx.with(Sql::SeenKeep, |s| {
        s.execute(params![
            entity,
            key,
            to,
            first.hearing,
            first.ending,
            last.hearing,
            last.ending,
            added,
            valid
        ])
    })?;
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
/// when its class or relation declares an axis, whose point is then part of its identity; else
/// a new hearing,
/// or a return to an older one, which this event is a new occurrence of. Current state takes
/// either only when it is now the newest version.
pub(crate) fn heard(
    tx: &impl Run,
    ontology: &Ontology,
    key: &Key,
    stamp: Stamp,
    of: &Of,
) -> Result<Heard, Error> {
    let identity = of.identity(key)?;
    let (entity, from, to) = of.entity();
    match hear(tx, identity, stamp, of, ontology)? {
        Recorded::Held(held) => {
            if held.axis.is_some()
                || newest(tx, (&entity, from, to))?.is_some_and(|(.., newest)| newest == identity)
            {
                return Ok(Heard::Held(held));
            }
            alias(tx, &held, of.event(), stamp.cursor)?;
        }
        // Its only fact is its newest version.
        Recorded::New { alone: true } => return Ok(Heard::Newest),
        Recorded::New { alone: false } => {}
    }
    Ok(match newest(tx, (&entity, from, to))? {
        Some((.., newest)) if newest != identity => Heard::Older,
        _ => Heard::Newest,
    })
}

/// Holds `event`, at `cursor`, as an alias of `held`, whose content it arrived with: an
/// occurrence of the hearing at its own known-at. When `event` is the smaller id the hearing takes
/// its name and its known-at, the id it was named by becomes the alias, and this returns true.
pub(crate) fn alias(
    tx: &impl Run,
    held: &Held,
    event: EventId,
    cursor: Cursor,
) -> Result<bool, Error> {
    if event == held.event {
        return Ok(false);
    }
    let renamed = event < held.event;
    if renamed {
        let known = event.recorded()?;
        tx.with(Sql::Rename, |s| {
            s.execute(params![
                held.identity,
                event.to_string(),
                cursor.0,
                point_or_known(held.axis.as_ref(), known),
                known
            ])
        })?;
    }
    let alias = if renamed { held.event } else { event };
    let known = alias.recorded()?;
    note(Some(known));
    let new = tx.with(Sql::Alias, |s| {
        s.execute(params![
            held.identity,
            alias.to_string(),
            point_or_known(held.axis.as_ref(), known),
            known
        ])
    })?;
    if renamed || new == 1 {
        reseen(tx, held.identity)?;
    }
    Ok(renamed)
}

/// Finds the first and last facts of the node or link the hearing `identity` is of again, by
/// index seeks, after that hearing took another event's id and maybe another known-at.
fn reseen(tx: &impl Run, identity: [u8; 32]) -> Result<(), Error> {
    let (entity, key, to): (String, String, Option<String>) = tx.with(Sql::HearingOf, |s| {
        s.query_row(params![identity], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
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
    tx: &impl Run,
    (entity, key, to): (&str, &str, Option<&str>),
) -> Result<Option<(Mark, Mark)>, Error> {
    let edge = |later: bool| -> Result<Option<Mark>, Error> {
        let (hearing, ending) = if later {
            (Sql::LastHearing, Sql::LastEnding)
        } else {
            (Sql::FirstHearing, Sql::FirstEnding)
        };
        let mark = |sql: Sql| {
            tx.with(sql, |s| {
                s.query_row(params![entity, key, to], |row| Mark::row(row, 0))
                    .optional()
            })
        };
        let (hearing, ending) = (mark(hearing)?, mark(ending)?);
        Ok(match (hearing, ending) {
            (Some(h), Some(e)) if (e.rank() > h.rank()) == later => Some(e),
            (Some(h), _) => Some(h),
            (None, e) => e,
        })
    };
    Ok(edge(false)?.zip(edge(true)?))
}

/// Records how a link came to an end, when `conclusion` is an ending (at its point on its
/// relation's axis, with its valid time) or a correction (with neither). Every ending and every
/// correction is kept, and an ending counts as a fact of the link; the latest conclusion by
/// known-at and then event id replaces any before it, whatever order they arrive in.
pub(crate) fn conclude(
    tx: &impl Run,
    ontology: &Ontology,
    conclusion: &Conclusion,
    renames: bool,
) -> Result<(), Error> {
    let Conclusion {
        event,
        source,
        relation,
        from,
        to,
        ended,
    } = conclusion;
    let known = event.recorded()?;
    let declared = &ontology.relation(relation)?.axis;
    let at = ended
        .as_ref()
        .map(|(axis, _)| {
            let declared = declared_point(declared, axis, ("relation", relation))?;
            Ok::<_, Error>(point_or_known(declared.as_ref(), known))
        })
        .transpose()?;
    let valid = ended.as_ref().and_then(|(_, valid)| valid.as_text());
    let entity = link_entity(relation);
    // Each end as renamed by the conclusion's own known-at, then every key it was renamed to
    // after: the conclusion is kept under each, as a rename copies the endings it finds, so the
    // order a conclusion and a rename arrive in never changes where it is kept. An ending is known
    // under a key only from the rename that reached it. A store with no rename is not looked up.
    let by = event.to_string();
    let keys = |end: &NodeRef| -> Result<Vec<(String, i64)>, Error> {
        if !renames {
            return Ok(vec![(end.key.clone(), known)]);
        }
        Ok(renamed_from(
            &renames_in(tx, &node_entity(&end.class))?,
            &end.key,
            (known, &by),
        ))
    };
    let (froms, tos) = (keys(from)?, keys(to)?);
    for (from, from_known) in &froms {
        for (to, to_known) in &tos {
            note(Some(known));
            tx.with(Sql::Conclude, |s| {
                s.execute(params![entity, from, to, by, source, at, valid, known])
            })?;
            let Some(at) = &at else {
                tx.with(Sql::Correction, |s| {
                    s.execute(params![entity, from, to, known, by, source])
                })?;
                continue;
            };
            let known = known.max(*from_known).max(*to_known);
            let new = tx.with(Sql::Ending, |s| {
                s.execute(params![entity, from, to, at, by, source, valid, known])
            })?;
            if new == 1 {
                let mark = Mark {
                    hearing: None,
                    ending: Some(tx.conn().last_insert_rowid()),
                    axis: at.clone(),
                    known,
                    event: by.clone(),
                };
                saw(tx, (&entity, from, Some(to)), &mark, valid)?;
            }
        }
    }
    Ok(())
}

/// The key `key` resolves to through `renames` as known at `upto`, then each key it was renamed
/// to after `upto`, in order, each with the known-at it holds from.
fn renamed_from(renames: &[Rename], key: &str, upto: (i64, &str)) -> Vec<(String, i64)> {
    let mut key = resolve(renames, key, upto);
    let mut keys = vec![(key.clone(), upto.0)];
    for (old, new, point, event) in renames {
        if (*point, event.as_str()) > upto && key == *old {
            key.clone_from(new);
            keys.push((key.clone(), *point));
        }
    }
    keys
}

/// What a node or a link is, as its hearings are kept: its entity, key and target key.
pub(crate) type Subject = (String, String, Option<String>);

/// The hearing an event made itself, as an amendment re-hears it.
struct Made {
    arrived: i64,
    of: Subject,
    /// The event it is named by.
    name: String,
    axis: Vec<u8>,
    known: i64,
}

/// The source and id of the event `amendment` names as its target.
pub(crate) fn amendment_target(amendment: &serde_json::Value) -> Option<(&str, &str)> {
    let target = amendment.get("data")?.get("target")?;
    Some((target.get("source")?.as_str()?, target.get("id")?.as_str()?))
}

/// Folds the amendment `body`, at its place `cursor`: its data names the event it amends by that
/// event's `CloudEvent` source and id (`target`), and holds a JSON Merge Patch of it (`patch`).
/// The amended event stays in the log as written. An amendment whose target this store has not
/// filled yet, because the log does not hold it or holds it after the amendment, waits for it and
/// returns `None`; filling the target applies it, `target_filled` set. An amendment that gives a node event another
/// key renames the node: its key as known just before is mapped to the new one from the
/// amendment's known-at, and every event naming the old key is heard again through the mapping.
/// Returns what the hearings made and dropped are of, whose current state is to be read again.
pub(crate) fn amend(
    tx: &impl Run,
    ontology: &Ontology,
    key: &Key,
    (body, cursor): (&str, Cursor),
    target_filled: bool,
) -> Result<Option<Vec<Subject>>, Error> {
    let amendment: serde_json::Value = serde_json::from_str(body)?;
    let id = amendment
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let uuid = |id: &str| -> Option<EventId> { id.split(':').next()?.parse().ok() };
    let event = uuid(id)
        .ok_or_else(|| Error::Refused(format!("amendment {id} is not <event uuid>:<fact>")))?;
    let data = amendment.get("data");
    let Some((source, target)) = amendment_target(&amendment) else {
        return Err(Error::Refused(format!(
            "amendment {id} names no target: its data has no target {{source, id}}"
        )));
    };
    if data.and_then(|data| data.get("patch")).is_none() {
        return Err(Error::Refused(format!("amendment {id} has no patch")));
    }
    let known = event.recorded()?;
    let found: Option<(i64, String)> = tx.with(Sql::EventByCloudevent, |s| {
        s.query_row(params![source, target], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
    })?;
    let made = match &found {
        Some((seq, _)) => made_by(tx, *seq)?,
        None => None,
    };
    let filled = target_filled || (made.is_none() && filled_before(tx, found.as_ref(), cursor)?);
    let Some((seq, written)) = found.filter(|_| made.is_some() || filled) else {
        tx.with(Sql::Wait, |s| {
            s.execute(params![source, target, known, event.to_string(), cursor.0])
        })?;
        return Ok(None);
    };
    let made = made.ok_or_else(|| {
        Error::Refused(format!(
            "amendment {id} names {target}, which made no hearing of its own"
        ))
    })?;
    let by = event.to_string();
    tx.with(Sql::Amendment, |s| {
        s.execute(params![seq, known, by, cursor.0])
    })?;
    let mut places = vec![seq];
    let renamed_to = data
        .and_then(|data| data.get("patch"))
        .and_then(|patch| patch.get("subject"))
        .and_then(serde_json::Value::as_str);
    if let (Some(new), None) = (renamed_to, &made.of.2) {
        let entity = &made.of.0;
        let old = key_before(tx, (seq, &written), entity, (known, &by))?;
        if old != new {
            tx.with(Sql::Renamed, |s| {
                s.execute(params![entity, old, new, known, by])
            })?;
            rename_ends(tx, ontology, entity, (&old, new), known)?;
            places.extend(naming(tx, ontology, entity, &old)?);
        }
    }
    places.sort_unstable();
    places.dedup();
    let mut touched = Vec::new();
    for place in places {
        touched.extend(derive(tx, (ontology, key), place)?);
    }
    touched.sort();
    touched.dedup();
    Ok(Some(touched))
}

/// Whether the target `found` (its place and body) has already filled: the log holds it before
/// the amendment at `cursor`, or at or below fill's committed place, as on fill's retry of the
/// amendment. A target that filled with no hearing never makes one. Fill's place is read only
/// for a target with no hearing, which is rare.
fn filled_before(
    tx: &impl Run,
    found: Option<&(i64, String)>,
    cursor: Cursor,
) -> Result<bool, Error> {
    let Some((seq, _)) = found else {
        return Ok(false);
    };
    Ok(*seq <= cursor.0 || Cursor(*seq) <= crate::inbox::place(tx, crate::FILL)?)
}

/// The hearing the event at the place `seq` made itself, if it made one.
fn made_by(tx: &impl Run, seq: i64) -> Result<Option<Made>, Error> {
    Ok(tx.with(Sql::MadeBy, |s| {
        s.query_row(params![seq], |row| {
            Ok(Made {
                arrived: row.get(0)?,
                of: (row.get(1)?, row.get(2)?, row.get(3)?),
                name: row.get(4)?,
                axis: row.get(5)?,
                known: row.get(6)?,
            })
        })
        .optional()
    })?)
}

/// A rename: old key, new key, known-at and the amendment that made it.
type Rename = (String, String, i64, String);

/// Every rename in the class entity `entity`, by known-at and then event id.
pub(crate) fn renames_in(tx: &impl Run, entity: &str) -> Result<Vec<Rename>, Error> {
    Ok(tx.with(Sql::RenamesIn, |s| {
        s.query_map(params![entity], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect()
    })?)
}

/// The key `key` resolves to through `renames`, taken in order, as known at `upto` (a point and
/// event id): each rename known by then that names the key it has reached maps it on.
fn resolve(renames: &[Rename], key: &str, upto: (i64, &str)) -> String {
    let mut key = key.to_string();
    for (old, new, point, event) in renames {
        if (*point, event.as_str()) <= upto && *old == key {
            key.clone_from(new);
        }
    }
    key
}

/// Whether a key `of` names, its node's or a link's ends, was ever renamed.
pub(crate) fn names_renamed(tx: &impl Run, of: &Of) -> Result<bool, Error> {
    let ends = match of {
        Of::Node(node) => vec![(&node.class, &node.key)],
        Of::Link(link) => vec![
            (&link.from.class, &link.from.key),
            (&link.to.class, &link.to.key),
        ],
    };
    for (class, key) in ends {
        let renamed: bool = tx.with(Sql::WasRenamed, |s| {
            s.query_row(params![node_entity(class), key], |row| row.get(0))
        })?;
        if renamed {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The key the event at `seq` (`written` as the log holds it) gives its node as known just
/// before `upto`: its amendments before then applied, then the renames before then.
fn key_before(
    tx: &impl Run,
    (seq, written): (i64, &str),
    entity: &str,
    upto: (i64, &str),
) -> Result<String, Error> {
    let mut body: serde_json::Value = serde_json::from_str(written)?;
    for (at, by, patch) in amendments_of(tx, seq)? {
        if (at, by.as_str()) < upto {
            merge(&mut body, &patch);
        }
    }
    let key = body
        .get("subject")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let before: Vec<Rename> = renames_in(tx, entity)?
        .into_iter()
        .filter(|(_, _, point, event)| (*point, event.as_str()) < upto)
        .collect();
    Ok(resolve(&before, key, upto))
}

/// Every amendment of the event at `seq` by known-at and then event id: its known-at, event and
/// the merge patch its data holds.
fn amendments_of(tx: &impl Run, seq: i64) -> Result<Vec<(i64, String, serde_json::Value)>, Error> {
    let rows: Vec<(i64, String, String)> = tx.with(Sql::AmendmentsOf, |s| {
        s.query_map(params![seq], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect()
    })?;
    rows.into_iter()
        .map(|(at, by, body)| {
            let body: serde_json::Value = serde_json::from_str(&body)?;
            let patch = body
                .get("data")
                .and_then(|data| data.get("patch"))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok((at, by, patch))
        })
        .collect()
}

/// The places of every event with a hearing that names the node keyed `key` of the class entity
/// `entity`: its own, and every link to or from it.
pub(crate) fn naming(
    tx: &impl Run,
    ontology: &Ontology,
    entity: &str,
    key: &str,
) -> Result<Vec<i64>, Error> {
    let mut asks = vec![(entity.to_string(), false)];
    asks.extend(link_ends(ontology, entity));
    let mut places = Vec::new();
    for (of, target) in asks {
        let found: Vec<i64> = tx.with(Sql::PlacesNaming, |s| {
            s.query_map(params![of, key, target], |row| row.get(0))?
                .collect()
        })?;
        places.extend(found);
    }
    Ok(places)
}

/// The link entities whose start (`false`) or end (`true`) is a node of `entity`.
pub(crate) fn link_ends(ontology: &Ontology, entity: &str) -> Vec<(String, bool)> {
    let mut ends = Vec::new();
    for relation in ontology.relations.values() {
        if node_entity(&relation.from) == entity {
            ends.push((link_entity(&relation.name), false));
        }
        if node_entity(&relation.to) == entity {
            ends.push((link_entity(&relation.name), true));
        }
    }
    ends
}

/// Copies the endings and conclusions of every link keyed `old` at a node of `entity` onto
/// `new`; the rows under `old` stay, so the link reads as it was known before the rename, and an
/// ending under `new` is known from the rename's `known` at the earliest.
fn rename_ends(
    tx: &impl Run,
    ontology: &Ontology,
    entity: &str,
    (old, new): (&str, &str),
    known: i64,
) -> Result<(), Error> {
    for (of, target) in link_ends(ontology, entity) {
        tx.with(Sql::RenameEndings, |s| {
            s.execute(params![of, old, new, target, known])
        })?;
        tx.with(Sql::RenameEnds, |s| {
            s.execute(params![of, old, new, target])
        })?;
        tx.with(Sql::RenameCorrections, |s| {
            s.execute(params![of, old, new, target])
        })?;
    }
    Ok(())
}

/// Hears the event at the place `seq` again from the log: its own hearing stays, known until the
/// first change, and each amendment of it and each rename of a key it names is a known-at where
/// it may read differently. At each, by known-at and then event id, the event is its body with
/// every amendment by then applied and its keys resolved through every rename by then; when that
/// differs from the hearing before, it is heard at its axis point (the patched one on a declared
/// axis, else the event's own known-at), known from then (never before the event itself), and the
/// hearing before is known until then. Returns what the hearings it made and dropped are of.
pub(crate) fn derive(
    tx: &impl Run,
    (ontology, key): (&Ontology, &Key),
    seq: i64,
) -> Result<Vec<Subject>, Error> {
    let Some(made) = made_by(tx, seq)? else {
        return Ok(Vec::new());
    };
    let written: String = tx.with(Sql::EventAt, |s| {
        s.query_row(params![seq], |row| row.get(1))
    })?;
    let mut touched: Vec<Subject> = tx.with(Sql::AmendedOf, |s| {
        s.query_map(params![seq], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect()
    })?;
    touched.push(made.of.clone());
    tx.with(Sql::DropAmended, |s| s.execute(params![seq]))?;
    tx.with(Sql::KnownAgain, |s| s.execute(params![made.arrived]))?;
    let mut body: serde_json::Value = serde_json::from_str(&written)?;
    let own = body
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let named: Vec<String> = match crate::fact::check(&written, ontology)? {
        Fact::Node(node) => vec![node_entity(&node.class)],
        Fact::Link(link) => vec![node_entity(&link.from.class), node_entity(&link.to.class)],
        _ => Vec::new(),
    };
    let mut renames: BTreeMap<String, Vec<Rename>> = BTreeMap::new();
    for entity in named {
        let found = renames_in(tx, &entity)?;
        renames.insert(entity, found);
    }
    // Each change: its known-at, event, and the patch when an amendment (before a rename at the
    // same known-at and event).
    let mut points: Vec<(i64, String, Option<serde_json::Value>)> = amendments_of(tx, seq)?
        .into_iter()
        .map(|(at, by, patch)| (at, by, Some(patch)))
        .collect();
    for (_, _, at, by) in renames.values().flatten() {
        points.push((*at, by.clone(), None));
    }
    points.sort_by(|a, b| (a.0, &a.1, a.2.is_none()).cmp(&(b.0, &b.1, b.2.is_none())));
    let mut before = made.arrived;
    let mut heard = heard_as(&written, ontology, &renames, None)?.identity(key)?;
    for (at, by, patch) in points {
        if let Some(patch) = &patch {
            merge(&mut body, patch);
        }
        let fact = heard_as(&body.to_string(), ontology, &renames, Some((at, &by)))?;
        let identity = fact.identity(key)?;
        if identity == heard {
            continue;
        }
        heard = identity;
        // The hearing before is known until `at`, and the new one from `at` or later.
        note(Some(at.max(made.known)));
        tx.with(Sql::KnownUntil, |s| s.execute(params![before, at]))?;
        let of = fact.of(&by)?;
        let (entity, of_key, to) = of.entity();
        let mut named = identity.to_vec();
        named.extend_from_slice(by.as_bytes());
        // The target's own id, not its place in this store's log, so every store replaying the
        // log names the hearing alike.
        named.extend_from_slice(own.as_bytes());
        let properties = match &of {
            Of::Node(node) => Some(serde_json::to_string(&node.properties)?),
            Of::Link(_) => None,
        };
        let axis = of.point(ontology)?.unwrap_or_else(|| made.axis.clone());
        tx.with(Sql::HearAmended, |s| {
            s.execute(params![
                blake3::keyed_hash(&key.bytes, &named).as_bytes(),
                entity,
                of_key,
                to,
                made.name,
                seq,
                of.source(),
                of.valid().as_text(),
                axis,
                // A rename can be known before the event itself; an amendment never is.
                at.max(made.known),
                at,
                properties
            ])
        })?;
        before = tx.conn().last_insert_rowid();
        touched.push((entity, of_key.to_string(), to.map(str::to_string)));
    }
    Ok(touched)
}

/// An event's fact as it is heard at a point: a node or a link, its keys resolved through the
/// renames known by then.
struct HeardAs(Fact);

impl HeardAs {
    fn identity(&self, key: &Key) -> Result<[u8; 32], Error> {
        self.of("")?.identity(key)
    }

    fn of(&self, by: &str) -> Result<Of<'_>, Error> {
        match &self.0 {
            Fact::Node(node) => Ok(Of::Node(node)),
            Fact::Link(link) => Ok(Of::Link(link)),
            _ => Err(Error::Refused(format!(
                "amendment {by} makes an event say no node or link"
            ))),
        }
    }
}

/// The fact `body` says, with its keys resolved through `renames` as known at `upto`, or as
/// written when `upto` is `None`.
fn heard_as(
    body: &str,
    ontology: &Ontology,
    renames: &BTreeMap<String, Vec<Rename>>,
    upto: Option<(i64, &str)>,
) -> Result<HeardAs, Error> {
    let mut fact = crate::fact::check(body, ontology)?;
    if let Some(upto) = upto {
        let through = |class: &str, key: &mut String| {
            if let Some(renames) = renames.get(&node_entity(class)) {
                *key = resolve(renames, key, upto);
            }
        };
        match &mut fact {
            Fact::Node(node) => through(&node.class, &mut node.key),
            Fact::Link(link) => {
                through(&link.from.class, &mut link.from.key);
                through(&link.to.class, &mut link.to.key);
            }
            _ => {}
        }
    }
    Ok(HeardAs(fact))
}

/// Applies the JSON Merge Patch `patch` to `target` (RFC 7386): an object's members patch the
/// target's, `null` removes one, and any other value replaces the target whole.
pub(crate) fn merge(target: &mut serde_json::Value, patch: &serde_json::Value) {
    let serde_json::Value::Object(members) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    if let serde_json::Value::Object(held) = target {
        for (name, value) in members {
            if value.is_null() {
                held.remove(name);
            } else {
                merge(
                    held.entry(name.clone()).or_insert(serde_json::Value::Null),
                    value,
                );
            }
        }
    }
}

/// Keeps what is seen of `of` in step after amendments and renames, counted again from what is
/// known now: its first and last facts, the first this store recorded, its first true and its
/// count; or no row once it has one fact or none, as one fact keeps no row.
pub(crate) fn reseen_of(tx: &impl Run, (entity, key, to): &Subject) -> Result<(), Error> {
    let to = to.as_deref().unwrap_or_default();
    let count: i64 = tx.with(Sql::FactsKnown, |s| {
        s.query_row(params![entity, key, to], |row| row.get(0))
    })?;
    let edges = edges(tx, (entity, key, (!to.is_empty()).then_some(to)))?;
    let Some((first, last)) = edges.filter(|_| count > 1) else {
        tx.with(Sql::Unseen, |s| s.execute(params![entity, key, to]))?;
        return Ok(());
    };
    let (hearing, ending, first_true): (Option<i64>, Option<i64>, Option<String>) =
        tx.with(Sql::SeenRecount, |s| {
            s.query_row(params![entity, key, to], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
        })?;
    let ending = if hearing.is_some() { None } else { ending };
    tx.with(Sql::SeenPut, |s| {
        s.execute(params![
            entity,
            key,
            to,
            first.hearing,
            first.ending,
            last.hearing,
            last.ending,
            hearing,
            ending,
            first_true,
            count
        ])
    })?;
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
    conn: &impl Run,
    of: (&str, &str, Option<&str>),
) -> Result<Option<Versioned>, Error> {
    read_version(conn, of, Sql::NewestVersion, &[])
}

/// A version row's properties: a node's, or none for a link, whose row holds null.
fn decode(json: Option<&str>) -> Result<BTreeMap<String, Value>, Error> {
    Ok(serde_json::from_str(json.unwrap_or("{}"))?)
}

/// Milliseconds since the Unix epoch, negative before it.
#[must_use]
pub fn millis(time: SystemTime) -> i64 {
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
    /// Its point on its class's or relation's axis, a part per declared part; on an undeclared
    /// axis, its known-at in milliseconds.
    pub axis: Vec<i64>,
    /// When it became known: the millisecond in its event's id.
    pub known: SystemTime,
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
        /// The ending's point on its relation's axis.
        axis: Vec<i64>,
        /// When the ending became known.
        known: SystemTime,
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

/// What a read sees: the versions at or before a point on the axis, as they were known at a
/// time. Each defaults to now: no point reads the top of the axis, and no known-at reads all the
/// store knows. A point with fewer parts than the axis declares reads the top of its prefix; on
/// an undeclared axis a point is one part, a known-at in milliseconds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct View {
    pub axis: Option<Vec<i64>>,
    pub known: Option<SystemTime>,
}

/// One version of a node or a link: the hearing that held at a point, by its class's order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    /// The event it is named by: the smallest id it arrived under.
    pub event: EventId,
    pub source: String,
    pub valid: ValidTime,
    /// Its point on the axis.
    pub axis: Vec<i64>,
    /// When it became known.
    pub known: SystemTime,
    /// A node's properties as that version holds them; a link has none.
    pub properties: BTreeMap<String, Value>,
    /// Whether a link does not hold at the point read, because it ended or was found wrong, or,
    /// as [`Seen`] names a link's fact, whether that fact is one of its endings; a node never has.
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
    /// When a link ended: the axis point of its earliest ending after its last live hearing, by
    /// axis point, known-at and then event id; `None` for a node, and for a link that has not
    /// ended, was found wrong or was heard live after its endings.
    pub ended: Option<Vec<i64>>,
}

/// One version row: the version, when this store recorded it and its content identity.
pub(crate) type Versioned = (Version, i64, [u8; 32]);

/// Reads one version row of `entity` keyed `key` (and `to`, for a link), as `tail` picks it
/// among its hearings' occurrences, with `bound` bound from `?4` on: see [`Versioned`]. The
/// version is named by its hearing's event, at the occurrence's axis point and known-at.
fn read_version<'s>(
    conn: &impl Run,
    (entity, key, to): (&str, &str, Option<&str>),
    rows: impl Into<Stmt<'s>>,
    bound: &[&dyn ToSql],
) -> Result<Option<Versioned>, Error> {
    let read = |row: &rusqlite::Row| -> rusqlite::Result<(VersionColumns, i64, [u8; 32])> {
        Ok((columns(row, 0)?, row.get(6)?, row.get(7)?))
    };
    let mut all: Vec<&dyn ToSql> = vec![&entity, &key, &to];
    all.extend_from_slice(bound);
    let row = conn.with(rows, |s| s.query_row(all.as_slice(), read).optional())?;
    let Some((columns, recorded, identity)) = row else {
        return Ok(None);
    };
    Ok(Some((version_of(columns, false)?, recorded, identity)))
}

/// A version's columns as a row holds them: event, source, valid, axis point, known-at,
/// properties.
pub(crate) type VersionColumns = (String, String, Option<String>, Vec<u8>, i64, Option<String>);

/// The [`VersionColumns`] of `row` from the column `at`.
pub(crate) fn columns(row: &rusqlite::Row, at: usize) -> rusqlite::Result<VersionColumns> {
    Ok((
        row.get(at)?,
        row.get(at + 1)?,
        row.get(at + 2)?,
        row.get(at + 3)?,
        row.get(at + 4)?,
        row.get(at + 5)?,
    ))
}

/// The version a row's [`VersionColumns`] name, `ended` when it is an ending.
pub(crate) fn version_of(
    (event, source, valid, axis, known, properties): VersionColumns,
    ended: bool,
) -> Result<Version, Error> {
    Ok(Version {
        event: serde_json::from_value(event.into())?,
        source,
        valid: ValidTime::from_text(valid),
        axis: decode_axis(&axis),
        known: time(known),
        properties: decode(properties.as_deref())?,
        ended,
    })
}

/// The bounds `view` puts on the versions of `owner` (a class or relation) whose axis is
/// `declared`: the encoded top of its axis prefix, and its known-at in milliseconds; each `None`
/// for now.
pub(crate) fn bounds(
    declared: &[String],
    view: &View,
    owner: &str,
) -> Result<(Option<Vec<u8>>, Option<i64>), Error> {
    let parts = declared.len().max(1);
    let axis = view
        .axis
        .as_ref()
        .map(|point| {
            if point.len() > parts {
                return Err(Error::Refused(format!(
                    "{owner} has an axis of {parts} part(s), {declared:?}; the view names {}",
                    point.len()
                )));
            }
            let mut top = encode(point);
            top.resize(parts * 8, 0xFF);
            Ok(top)
        })
        .transpose()?;
    Ok((axis, view.known.map(millis)))
}

/// The version of the node keyed `key` in `class` in `view`: the newest by its class's order,
/// or, given the valid time `day` (`YYYY-MM-DD`), the one true then: among its versions valid on
/// or before `day`, the greatest by valid time, then by its class's order. A version of unknown
/// valid time is never true on a day.
pub(crate) fn node_in_view(
    conn: &Connection,
    class: &Class,
    key: &str,
    view: &View,
    day: Option<&str>,
) -> Result<Option<Versioned>, Error> {
    let (axis, known) = bounds(&class.axis, view, &format!("class {}", class.name))?;
    let of = node_entity(&class.name);
    match day {
        None => read_version(conn, (&of, key, None), Sql::NewestInView, &[&axis, &known]),
        Some(day) => read_version(conn, (&of, key, None), Sql::AsOf, &[&axis, &known, &day]),
    }
}

/// Whether the link of `relation` from `from` to `to` had ended in the view `(axis, known)` and,
/// as of `day` when given, by then; refused as of `day` when its ending's valid time is unknown.
fn ended(
    conn: &Connection,
    relation: &str,
    (from, to): (&str, &str),
    (axis, known): (Option<&[u8]>, Option<i64>),
    day: Option<&str>,
) -> Result<bool, Error> {
    let ending: Option<Option<bool>> = conn
        .prepare_cached(ended_sql())?
        .query_row(
            params![link_entity(relation), from, to, axis, known, day],
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

/// The version of the link of `relation` from `from` to `to` in the view of the encoded axis
/// bound and the known-at `(axis, known)`: see [`Graph::link_state_at`].
pub(crate) fn link_in_view(
    conn: &Connection,
    relation: &str,
    (from, to): (&str, &str),
    (axis, known): (Option<Vec<u8>>, Option<i64>),
) -> Result<Option<Version>, Error> {
    let newest = conn
        .prepare_cached(link_state())?
        .query_row(
            params![link_entity(relation), from, to, axis, known],
            |row| {
                Ok((
                    columns(row, 0)?,
                    row.get::<_, bool>(6)?,
                    row.get::<_, bool>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((columns, ending, corrected)) = newest else {
        return Ok(None);
    };
    let mut version = version_of(columns, ending)?;
    version.ended = corrected || ended(conn, relation, (from, to), (axis.as_deref(), known), None)?;
    Ok(Some(version))
}

/// An ending's row: event, source, axis point, known-at, valid time.
type EndingRow = (String, String, Vec<u8>, i64, Option<String>);

impl Graph {
    /// How the link of `relation` from the node keyed `from` to the node keyed `to` came to an
    /// end: found wrong, when a correction no newer live hearing follows is its latest conclusion
    /// by known-at or it has no ending; else its ending, the earliest after its last live hearing
    /// by axis point, known-at and then event id; `None` while it has neither ended nor been found
    /// wrong, and once heard live after its endings and corrections.
    ///
    /// # Errors
    /// The relation is not declared, or the graph file cannot be read.
    pub fn link_end(&self, relation: &str, from: &str, to: &str) -> Result<Option<End>, Error> {
        let relation = self.current_ontology()?.relation(relation)?.name.clone();
        let link = params![link_entity(&relation), from, to];
        let corrected: Option<(String, String, bool)> = self
            .conn
            .prepare_cached(link_correction())?
            .query_row(link, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .optional()?;
        let row: Option<EndingRow> = self
            .conn
            .prepare_cached(link_ending())?
            .query_row(link, |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .optional()?;
        if let Some((event, source, latest)) = corrected
            && (latest || row.is_none())
        {
            let event = serde_json::from_value(event.into())?;
            return Ok(Some(End::Corrected { source, event }));
        }
        let Some((event, source, axis, known, valid)) = row else {
            return Ok(None);
        };
        Ok(Some(End::Ended {
            axis: decode_axis(&axis),
            known: time(known),
            valid: ValidTime::from_text(valid),
            source,
            event: serde_json::from_value(event.into())?,
        }))
    }

    /// Every hearing of `node`, in order on its class's axis: by axis point, then known-at,
    /// then event id.
    ///
    /// # Errors
    /// The class is not declared, or the graph file cannot be read.
    pub fn hearings(&self, node: &NodeRef) -> Result<Vec<Hearing>, Error> {
        let class = self.current_ontology()?.class(&node.class)?.name.clone();
        self.hearings_of(&node_entity(&class), &node.key, None)
    }

    /// Every hearing of the link of `relation` from the node keyed `from` to the node keyed
    /// `to`, in order on its relation's axis.
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
        /// A hearing's row: identity, event, source, valid time, axis point and known-at.
        type Row = ([u8; 32], String, String, Option<String>, Vec<u8>, i64);
        let rows: Vec<Row> = self
            .conn
            .prepare_cached(hearings())?
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
        for (identity, event, source, valid, axis, known) in rows {
            let aliases: Vec<String> = self
                .conn
                .prepare_cached(ALIASES_OF)?
                .query_map(params![identity], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            hearings.push(Hearing {
                event: serde_json::from_value(event.into())?,
                source,
                valid: ValidTime::from_text(valid),
                axis: decode_axis(&axis),
                known: time(known),
                identity,
                aliases: aliases
                    .into_iter()
                    .map(|alias| serde_json::from_value(alias.into()))
                    .collect::<Result<_, _>>()?,
            });
        }
        Ok(hearings)
    }

    /// The version of `node` in `view`: the newest by its class's order (axis point, then
    /// known-at, then event id) at or before the view's point, among the versions known at its
    /// known-at. `None` before its first version. A version that arrived late answers at its own
    /// point like any other, and a correction at the same point answers only from its known-at.
    ///
    /// # Errors
    /// [`Error::Refused`] when the view's point has more parts than the class's axis; the class
    /// is not declared, or the graph file cannot be read.
    pub fn state_at(&self, node: &NodeRef, view: &View) -> Result<Option<Version>, Error> {
        let ontology = self.current_ontology()?;
        let class = ontology.class(&node.class)?;
        Ok(node_in_view(&self.conn, class, &node.key, view, None)?.map(|(v, ..)| v))
    }

    /// The version of the link of `relation` from the node keyed `from` to the node keyed `to`
    /// in `view`: the newest of its live hearings and endings at or before the view's point and
    /// known at its known-at, and whether the link had ended or been found wrong by then. A
    /// correction known in the view holds across the whole axis until a live hearing whose event
    /// follows it is the newest in the view. `None` before its first
    /// version. An amendment or rename known after the view's known-at is not in it.
    ///
    /// # Errors
    /// [`Error::Refused`] when the view's point has more parts than the relation's axis; the
    /// relation is not declared, or the graph file cannot be read.
    pub fn link_state_at(
        &self,
        relation: &str,
        from: &str,
        to: &str,
        view: &View,
    ) -> Result<Option<Version>, Error> {
        let ontology = self.current_ontology()?;
        let declared = ontology.relation(relation)?;
        let bounds = bounds(&declared.axis, view, &format!("relation {}", declared.name))?;
        link_in_view(&self.conn, &declared.name, (from, to), bounds)
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
            Ok((columns(row, at)?, row.get::<_, bool>(at + 6)?))
        };
        let row = self
            .conn
            .prepare_cached(sql)?
            .query_row(params![of.0, of.1, of.2], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
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
            first_true: ValidTime::from_text(first_true),
            count: u64::try_from(count).unwrap_or(0),
            ended: ended.as_deref().map(decode_axis),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_axis, encode};

    #[test]
    fn encoded_points_order_as_their_parts() {
        let points: [&[i64]; 7] = [
            &[i64::MIN],
            &[-256, 0],
            &[-1, 300],
            &[0, -5],
            &[0, 255],
            &[0, 256],
            &[1, i64::MIN],
        ];
        for pair in points.windows(2) {
            assert!(
                encode(pair[0]) < encode(pair[1]),
                "{:?} orders before {:?}",
                pair[0],
                pair[1]
            );
        }
        for point in points {
            assert_eq!(decode_axis(&encode(point)), point);
        }
    }
}
