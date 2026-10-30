//! The language rannoch speaks: a rannoch event to and from `CloudEvents` 1.0. Pure code with no I/O.
//!
//! A committed `CloudEvent` says what the store must fill: a node of a class, a link of a
//! relation, or nothing. The shapes are the ones the `CloudEvents` toy proved lossless
//! (research 01a16140): a node is type `rannoch.node.<class>`, its subject the node's key and its
//! data `{properties, valid}`; a link is type `rannoch.link.<relation>`, its data
//! `{from: {class, key}, to: {class, key}, valid}`; the source is `rannoch:<the write's source>`.
//! Its id is `<event uuid>:<fact>`: the origin's event, a `UUIDv7`, and the fact within it. The
//! writer's point is `data.time` (milliseconds since the Unix epoch, UTC) and its integer key
//! `data.integer`, as the oracle's `NodeFact` names them. A link that ended is type
//! `rannoch.ended.<relation>`, its data `{from, to, at, valid}` with `at` its point in
//! milliseconds; a link found wrong is type `rannoch.corrected.<relation>`, its data `{from, to}`:
//! the oracle's `Change::Ended` and `Change::Corrected` fields. A node replaced by another of its
//! class is type `rannoch.replaced.<class>`, its subject the old key and its data `{new}`, the
//! new key: the oracle's `Change::Replaced` fields.
//!
//! Reading checks the format only: whether a class or relation is declared, and whether a fact
//! fits it, is for the caller to check against its ontology.

use std::collections::BTreeMap;
use std::fmt;

use rannoch_ontology::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Why a body was not read.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Refused, with the reason.
    #[error("{0}")]
    Refused(String),
    /// The body, or its data, could not be decoded.
    #[error("an event could not be encoded or decoded: {0}")]
    Encoding(#[from] serde_json::Error),
}

/// An event's own id (`UUIDv7`), minted when the event is written and never derived from its
/// content. Ids order as they were minted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(Uuid);

impl EventId {
    /// Mints a new id. `Uuid::now_v7` is monotonic within one process: within a millisecond it
    /// fills the rest of the timestamp field with a counter, so ids minted later sort later.
    #[must_use]
    pub fn now() -> EventId {
        EventId(Uuid::now_v7())
    }

    /// The millisecond the id was minted at: when the store that minted it recorded its event.
    /// None when the id carries no timestamp, as an id that is not a `UUIDv7` may not.
    fn millis(self) -> Option<i64> {
        self.0.get_timestamp().map(|at| {
            let (seconds, nanos) = at.to_unix();
            i64::try_from(seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000)
                .saturating_add(i64::from(nanos / 1_000_000))
        })
    }

    /// The millisecond the id was minted at, for an event that came from outside this store's
    /// own write path: refused when the id carries none, since no other time stands in for it.
    ///
    /// # Errors
    /// [`Error::Refused`] when the id is not a `UUIDv7`.
    pub fn recorded(self) -> Result<i64, Error> {
        self.millis().ok_or_else(|| {
            Error::Refused(format!(
                "event {self} has no record time: its id is not a UUIDv7"
            ))
        })
    }
}

/// An id as the log holds it: any UUID, since the store minted it. An id from outside is read
/// through the event's own id, which must be a `UUIDv7`.
impl std::str::FromStr for EventId {
    type Err = uuid::Error;

    fn from_str(id: &str) -> Result<EventId, uuid::Error> {
        id.parse().map(EventId)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
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

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Year, month and day; a month date's day is 1.
    ///
    /// # Errors
    /// [`Error::Refused`] when the month is outside 1..=12 or the day is outside the month (leap
    /// years counted).
    pub fn parts(&self) -> Result<(u16, u8, u8), Error> {
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
    /// The date as text, or `None` when unknown.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ValidTime::On(date) => Some(date.as_str()),
            ValidTime::Unknown => None,
        }
    }

    /// A valid time from its text, `None` read as unknown.
    #[must_use]
    pub fn from_text(value: Option<String>) -> ValidTime {
        value.map_or(ValidTime::Unknown, |date| ValidTime::On(Date(date)))
    }

    /// Refuses a date that is not a real date.
    fn checked(&self) -> Result<(), Error> {
        if let ValidTime::On(date) = self {
            date.parts()?;
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

/// The `CloudEvents` 1.0 attributes rannoch reads, without the event's data.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Attributes {
    pub specversion: String,
    pub id: String,
    pub source: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub subject: Option<String>,
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(flatten)]
    attributes: Attributes,
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
struct ReplacedData {
    new: String,
}

#[derive(Deserialize)]
struct CorrectedData {
    from: NodeRef,
    to: NodeRef,
}

/// A node as an event holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeFact {
    /// The origin's event.
    pub event: EventId,
    pub class: String,
    pub key: String,
    pub source: String,
    pub properties: BTreeMap<String, Value>,
    pub valid: ValidTime,
    /// The time its writer supplied, in milliseconds since the Unix epoch (UTC).
    pub time: Option<i64>,
    /// The integer key its writer supplied.
    pub integer: Option<i64>,
}

/// A link, named by its relation and both of its endpoints, as an event holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkFact {
    /// The origin's event.
    pub event: EventId,
    pub relation: String,
    pub from: NodeRef,
    pub to: NodeRef,
    pub source: String,
    pub valid: ValidTime,
    /// The time its writer supplied, in milliseconds since the Unix epoch (UTC): the link's
    /// point. `None` puts it at the origin's record time.
    pub time: Option<i64>,
}

/// How a link came to an end, as an event holds it: it ended, at its own point and with the
/// valid time it stopped holding at, or it was found wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conclusion {
    /// The origin's event.
    pub event: EventId,
    pub source: String,
    pub relation: String,
    pub from: NodeRef,
    pub to: NodeRef,
    /// The ending's point, in milliseconds since the Unix epoch (UTC), and its valid time;
    /// `None` for a correction.
    pub ended: Option<(i64, ValidTime)>,
}

/// A node of `class` replaced by another under the key `new`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replacement {
    /// The origin's event.
    pub event: EventId,
    pub source: String,
    pub class: String,
    pub old: String,
    pub new: String,
}

/// What one event says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fact {
    Node(NodeFact),
    Link(LinkFact),
    /// A link that ended or was found wrong.
    Concluded(Conclusion),
    /// A node replaced by another of its class.
    Replaced(Replacement),
    /// A declaration.
    Declared,
    /// An event of the app's own, or of rannoch's that says no fact.
    None,
}

/// Reads `body`'s `CloudEvents` attributes, leaving its data unread.
///
/// # Errors
/// [`Error::Refused`] when the body is not a `CloudEvents` 1.0 event; [`Error::Encoding`] when it
/// is not an event at all.
pub fn attributes(body: &str) -> Result<Attributes, Error> {
    spec(serde_json::from_str(body)?)
}

fn spec(attributes: Attributes) -> Result<Attributes, Error> {
    if attributes.specversion != "1.0" {
        return Err(Error::Refused(format!(
            "specversion {} is not CloudEvents 1.0",
            attributes.specversion
        )));
    }
    Ok(attributes)
}

/// Reads `body` as a `CloudEvent` into the fact it says.
///
/// # Errors
/// [`Error::Refused`] with the reason when the body is not a `CloudEvents` 1.0 event, a fact's id
/// is not `<uuidv7>:<fact>`, its source is not percent-encoded correctly, a node has no subject
/// or a valid time is not a date; [`Error::Encoding`] when the body or a fact's data cannot be
/// decoded.
pub fn read(body: &str) -> Result<Fact, Error> {
    let envelope: Envelope = serde_json::from_str(body)?;
    let event = spec(envelope.attributes)?;
    let Some(rest) = event.ty.strip_prefix("rannoch.") else {
        return Ok(Fact::None);
    };
    let (kind, name) = rest.split_once('.').unwrap_or((rest, ""));
    let source = source_back(&event.source)?;
    let data = envelope.data.unwrap_or(serde_json::Value::Null);
    match kind {
        "declared" => Ok(Fact::Declared),
        "node" => {
            let key = event
                .subject
                .ok_or_else(|| Error::Refused(format!("{} has no subject", event.ty)))?;
            let node: NodeData = serde_json::from_value(data)?;
            node.valid.checked()?;
            Ok(Fact::Node(NodeFact {
                event: origin(&event.id)?,
                class: name.to_string(),
                key,
                source,
                properties: node.properties,
                valid: node.valid,
                time: node.time,
                integer: node.integer,
            }))
        }
        "link" => {
            let link: LinkData = serde_json::from_value(data)?;
            link.valid.checked()?;
            Ok(Fact::Link(LinkFact {
                event: origin(&event.id)?,
                relation: name.to_string(),
                from: link.from,
                to: link.to,
                source,
                valid: link.valid,
                time: link.time,
            }))
        }
        "ended" | "corrected" => {
            let (from, to, ended) = if kind == "ended" {
                let ended: EndedData = serde_json::from_value(data)?;
                ended.valid.checked()?;
                (ended.from, ended.to, Some((ended.at, ended.valid)))
            } else {
                let corrected: CorrectedData = serde_json::from_value(data)?;
                (corrected.from, corrected.to, None)
            };
            Ok(Fact::Concluded(Conclusion {
                event: origin(&event.id)?,
                source,
                relation: name.to_string(),
                from,
                to,
                ended,
            }))
        }
        "replaced" => {
            let old = event
                .subject
                .ok_or_else(|| Error::Refused(format!("{} has no subject", event.ty)))?;
            let replaced: ReplacedData = serde_json::from_value(data)?;
            Ok(Fact::Replaced(Replacement {
                event: origin(&event.id)?,
                source,
                class: name.to_string(),
                old,
                new: replaced.new,
            }))
        }
        _ => Ok(Fact::None),
    }
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

#[cfg(test)]
mod tests {
    use super::{Error, Fact, NodeRef, ValidTime, attributes, read};

    fn id() -> String {
        format!("{}:0", uuid::Uuid::now_v7())
    }

    #[test]
    fn a_node_reads_into_its_fact() -> Result<(), Box<dyn std::error::Error>> {
        let body = format!(
            r#"{{"specversion":"1.0","id":"{}","source":"rannoch:my%20shop","type":"rannoch.node.book","subject":"b1","data":{{"properties":{{"title":"hi"}},"valid":{{"on":"2024-02-29"}},"time":5}}}}"#,
            id()
        );
        let Fact::Node(node) = read(&body)? else {
            return Err("a node event is a node fact".into());
        };
        assert_eq!((node.class.as_str(), node.key.as_str()), ("book", "b1"));
        assert_eq!(node.source, "my shop");
        assert_eq!(node.properties.get("title"), Some(&"hi".into()));
        assert_eq!(node.valid.as_text(), Some("2024-02-29"));
        assert_eq!((node.time, node.integer), (Some(5), None));
        Ok(())
    }

    #[test]
    fn a_link_an_ending_and_a_correction_read_into_their_facts()
    -> Result<(), Box<dyn std::error::Error>> {
        let ends = r#""from":{"class":"author","key":"a1"},"to":{"class":"book","key":"b1"}"#;
        let body = |kind: &str, data: &str| {
            format!(
                r#"{{"specversion":"1.0","id":"{}","source":"shop","type":"rannoch.{kind}.wrote","data":{{{ends}{data}}}}}"#,
                id()
            )
        };
        let Fact::Link(link) = read(&body("link", r#","valid":"unknown""#))? else {
            return Err("a link event is a link fact".into());
        };
        assert_eq!(link.relation, "wrote");
        assert_eq!(link.to, NodeRef::new("book", "b1"));
        assert_eq!(link.source, "shop");
        let Fact::Concluded(ended) = read(&body("ended", r#","at":7,"valid":"unknown""#))? else {
            return Err("an ended event is a conclusion".into());
        };
        assert_eq!(ended.ended, Some((7, ValidTime::Unknown)));
        let Fact::Concluded(corrected) = read(&body("corrected", ""))? else {
            return Err("a corrected event is a conclusion".into());
        };
        assert_eq!(corrected.ended, None);
        Ok(())
    }

    #[test]
    fn other_events_say_no_fact() -> Result<(), Box<dyn std::error::Error>> {
        let body =
            |ty: &str| format!(r#"{{"specversion":"1.0","id":"x","source":"shop","type":"{ty}"}}"#);
        assert_eq!(read(&body("shop.order"))?, Fact::None);
        assert_eq!(read(&body("rannoch.closed.wrote"))?, Fact::None);
        assert_eq!(read(&body("rannoch.declared.book"))?, Fact::Declared);
        Ok(())
    }

    #[test]
    fn attributes_read_without_the_data() -> Result<(), Box<dyn std::error::Error>> {
        let body = r#"{"specversion":"1.0","id":"e1","source":"shop","type":"rannoch.node.book","subject":"b1","data":{"not":"a node"}}"#;
        assert!(read(body).is_err(), "its data is not a node's");
        let read = attributes(body)?;
        assert_eq!(read.id, "e1");
        assert_eq!(read.ty, "rannoch.node.book");
        assert_eq!(read.subject.as_deref(), Some("b1"));
        Ok(())
    }

    #[test]
    fn a_body_not_cloudevents_1_0_is_refused_with_the_reason() {
        let body = r#"{"specversion":"0.3","id":"e1","source":"shop","type":"shop.order"}"#;
        for refused in [read(body).err(), attributes(body).err()] {
            assert!(
                matches!(&refused, Some(Error::Refused(why)) if why == "specversion 0.3 is not CloudEvents 1.0"),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn a_fact_whose_id_is_not_uuidv7_and_fact_is_refused_with_the_reason() {
        let v4 = "67e55044-10b1-426f-9247-bb680e5fe0c8:0";
        let v7 = uuid::Uuid::now_v7().to_string();
        let no_fact = format!("{v7}:");
        for id in ["e1", v4, v7.as_str(), no_fact.as_str()] {
            let body = format!(
                r#"{{"specversion":"1.0","id":"{id}","source":"shop","type":"rannoch.node.book","subject":"b1","data":{{"properties":{{}},"valid":"unknown"}}}}"#
            );
            let refused = read(&body).err();
            assert!(
                matches!(&refused, Some(Error::Refused(why)) if why.contains(id.split(':').next().unwrap_or_default())),
                "{id}: {refused:?}"
            );
        }
    }

    #[test]
    fn a_replacement_reads_into_its_fact() -> Result<(), Box<dyn std::error::Error>> {
        let body = format!(
            r#"{{"specversion":"1.0","id":"{}:0","source":"rannoch:shop","type":"rannoch.replaced.book","subject":"b1","data":{{"new":"b2"}}}}"#,
            crate::EventId::now()
        );
        let Fact::Replaced(r) = read(&body)? else {
            return Err("not a replacement".into());
        };
        assert_eq!(
            (r.class.as_str(), r.old.as_str(), r.new.as_str()),
            ("book", "b1", "b2")
        );
        Ok(())
    }
}
