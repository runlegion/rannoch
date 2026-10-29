//! What a committed `CloudEvent` says the store must fill: a node of a class, a link of a
//! relation, or nothing. The shapes are the ones the `CloudEvents` toy proved lossless
//! (research 01a16140): a node is type `rannoch.node.<class>`, its subject the node's key and its
//! data `{properties, valid}`; a link is type `rannoch.link.<relation>`, its data
//! `{from: {class, key}, to: {class, key}, valid}`; the source is `rannoch:<the write's source>`.
//! Its id is `<event uuid>:<fact>`: the origin's event, a `UUIDv7`, and the fact within it. The
//! writer's point is `data.time` (milliseconds since the Unix epoch, UTC) and its integer key
//! `data.integer`, as the oracle's `NodeFact` names them. A link that ended is type
//! `rannoch.ended.<relation>`, its data `{from, to, at, valid}` with `at` its point in
//! milliseconds; a link found wrong is type `rannoch.corrected.<relation>`, its data `{from, to}`:
//! the oracle's `Change::Ended` and `Change::Corrected` fields.

use std::collections::BTreeMap;

use rannoch_ontology::{HistoryKey, Ontology, Relation, Value};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::log::EventId;
use crate::read::NodeRef;

/// The `CloudEvents` 1.0 attributes the store reads.
#[derive(Deserialize)]
struct Envelope {
    specversion: String,
    id: String,
    source: String,
    #[serde(rename = "type")]
    ty: String,
    subject: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct NodeData {
    properties: BTreeMap<String, Value>,
    valid: ValidTime,
    #[serde(default)]
    time: Option<i64>,
    #[serde(default)]
    integer: Option<i64>,
}

#[derive(Deserialize)]
struct LinkData {
    from: NodeRef,
    to: NodeRef,
    valid: ValidTime,
    #[serde(default)]
    time: Option<i64>,
}

#[derive(Deserialize)]
struct EndedData {
    from: NodeRef,
    to: NodeRef,
    at: i64,
    valid: ValidTime,
}

#[derive(Deserialize)]
struct CorrectedData {
    from: NodeRef,
    to: NodeRef,
}

/// The origin's event a `CloudEvent` id `<event uuid>:<fact>` names: refused when the id is not
/// of that form or its uuid carries no record time.
fn origin(id: &str) -> Result<EventId, Error> {
    let refused = || Error::Refused(format!("event id {id} is not <event uuid>:<fact>"));
    let (uuid, fact) = id.split_once(':').ok_or_else(refused)?;
    if fact.is_empty() {
        return Err(refused());
    }
    let event = EventId(uuid.parse().map_err(|_| refused())?);
    event.recorded()?;
    Ok(event)
}

/// The write's source from a `rannoch:<source>` URI, percent-decoded as the `CloudEvents` toy
/// encoded it; a source that is not rannoch's is kept as it is.
fn source_back(uri: &str) -> Result<String, Error> {
    let Some(rest) = uri.strip_prefix("rannoch:") else {
        return Ok(uri.to_string());
    };
    let refused = || Error::Refused(format!("source {uri} is not percent-encoded correctly"));
    let b = rest.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let h = std::str::from_utf8(&b[i + 1..i + 3]).map_err(|_| refused())?;
            out.push(u8::from_str_radix(h, 16).map_err(|_| refused())?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| refused())
}

/// A node as an event holds it.
#[derive(Debug, Clone)]
pub(crate) struct NodeFact {
    /// The origin's event.
    pub(crate) event: EventId,
    pub(crate) class: String,
    pub(crate) key: String,
    pub(crate) source: String,
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) valid: ValidTime,
    /// The time its writer supplied, in milliseconds since the Unix epoch (UTC).
    pub(crate) time: Option<i64>,
    /// The integer key its writer supplied.
    pub(crate) integer: Option<i64>,
}

/// A link, named by its relation and both of its endpoints, as an event holds it.
#[derive(Debug, Clone)]
pub(crate) struct LinkFact {
    /// The origin's event.
    pub(crate) event: EventId,
    pub(crate) relation: String,
    pub(crate) from: NodeRef,
    pub(crate) to: NodeRef,
    pub(crate) source: String,
    pub(crate) valid: ValidTime,
    /// The time its writer supplied, in milliseconds since the Unix epoch (UTC): the link's
    /// point. `None` puts it at the origin's record time.
    pub(crate) time: Option<i64>,
}

/// How a link came to an end, as an event holds it: it ended, at its own point and with the
/// valid time it stopped holding at, or it was found wrong.
#[derive(Debug, Clone)]
pub(crate) struct Conclusion {
    /// The origin's event.
    pub(crate) event: EventId,
    pub(crate) source: String,
    pub(crate) relation: String,
    pub(crate) from: NodeRef,
    pub(crate) to: NodeRef,
    /// The ending's point, in milliseconds since the Unix epoch (UTC), and its valid time;
    /// `None` for a correction.
    pub(crate) ended: Option<(i64, ValidTime)>,
}

/// What one committed event fills.
pub(crate) enum Fact {
    Node(NodeFact),
    Link(LinkFact),
    /// A link that ended or was found wrong.
    Concluded(Conclusion),
    /// A declaration: already built when it was declared, so it fills nothing.
    Declared,
    /// An event of the app's own, or of rannoch's that fills no table: kept in the log only.
    None,
}

/// Reads `body` as a `CloudEvent` and checks what it says against `ontology`. A body that is not
/// a `CloudEvents` 1.0 event, or a node or link the
/// ontology does not allow is refused with the reason.
pub(crate) fn read(body: &str, ontology: &Ontology) -> Result<Fact, Error> {
    let event: Envelope = serde_json::from_str(body)?;
    if event.specversion != "1.0" {
        return Err(Error::Refused(format!(
            "specversion {} is not CloudEvents 1.0",
            event.specversion
        )));
    }
    let Some(rest) = event.ty.strip_prefix("rannoch.") else {
        return Ok(Fact::None);
    };
    let (kind, name) = rest.split_once('.').unwrap_or((rest, ""));
    let source = source_back(&event.source)?;
    let data = event.data.unwrap_or(serde_json::Value::Null);
    match kind {
        "declared" => Ok(Fact::Declared),
        "node" => {
            let class = ontology.class(name)?;
            let key = event
                .subject
                .ok_or_else(|| Error::Refused(format!("{} has no subject", event.ty)))?;
            let node: NodeData = serde_json::from_value(data)?;
            let missing = match class.history {
                HistoryKey::Time if node.time.is_none() => Some("a time"),
                HistoryKey::Integer if node.integer.is_none() => Some("an integer key"),
                _ => None,
            };
            if let Some(missing) = missing {
                return Err(Error::Refused(format!(
                    "class {} is ordered by {missing} the writer supplies; node {key} carries none",
                    class.name
                )));
            }
            class.check(&node.properties)?;
            if let ValidTime::On(date) = &node.valid {
                date.parts()?;
            }
            Ok(Fact::Node(NodeFact {
                event: origin(&event.id)?,
                class: class.name.clone(),
                key,
                source,
                properties: node.properties,
                valid: node.valid,
                time: node.time,
                integer: node.integer,
            }))
        }
        "link" => {
            let relation = ontology.relation(name)?;
            let link: LinkData = serde_json::from_value(data)?;
            ends(relation, &link.from, &link.to)?;
            if let ValidTime::On(date) = &link.valid {
                date.parts()?;
            }
            Ok(Fact::Link(LinkFact {
                event: origin(&event.id)?,
                relation: relation.name.clone(),
                from: link.from,
                to: link.to,
                source,
                valid: link.valid,
                time: link.time,
            }))
        }
        "ended" | "corrected" => Ok(Fact::Concluded(concluded(
            ontology.relation(name)?,
            kind == "ended",
            origin(&event.id)?,
            source,
            data,
        )?)),
        _ => Ok(Fact::None),
    }
}

/// What an ending (`ended`) or a correction of a link of `relation` says, from its `data`.
fn concluded(
    relation: &Relation,
    ended: bool,
    event: EventId,
    source: String,
    data: serde_json::Value,
) -> Result<Conclusion, Error> {
    let (from, to, ended) = if ended {
        let ended: EndedData = serde_json::from_value(data)?;
        if let ValidTime::On(date) = &ended.valid {
            date.parts()?;
        }
        (ended.from, ended.to, Some((ended.at, ended.valid)))
    } else {
        let corrected: CorrectedData = serde_json::from_value(data)?;
        (corrected.from, corrected.to, None)
    };
    ends(relation, &from, &to)?;
    Ok(Conclusion {
        event,
        source,
        relation: relation.name.clone(),
        from,
        to,
        ended,
    })
}

/// Refuses a link whose ends are not of its relation's classes.
fn ends(relation: &Relation, from: &NodeRef, to: &NodeRef) -> Result<(), Error> {
    Ok(relation.holds(&from.class, &to.class)?)
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

impl std::fmt::Display for Date {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
