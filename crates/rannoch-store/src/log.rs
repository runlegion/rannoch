//! The log: every change to a graph is an event, read in order from a cursor and kept. An
//! event's body is the `CloudEvent` itself, as JSON.

use std::fmt;

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Error;
use crate::queries::{APPEND_EVENT, EVENT_AT, EVENTS_BETWEEN, HIGH_WATER};

/// A place in a graph's event log. Reading since a cursor returns the events after it. An app
/// stores a cursor by serializing it, and a graph rebuilt from the log keeps each event's place,
/// so a stored cursor names the same place in the rebuilt graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(pub(crate) i64);

impl Cursor {
    /// Before the first event.
    pub const START: Cursor = Cursor(0);
}

/// An event's own id (`UUIDv7`), minted when the event is written and never derived from its
/// content. Ids order as they were minted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(pub(crate) Uuid);

impl EventId {
    /// Mints a new id. `Uuid::now_v7` is monotonic within one process: within a millisecond it
    /// fills the rest of the timestamp field with a counter, so ids minted later sort later.
    pub(crate) fn now() -> EventId {
        EventId(Uuid::now_v7())
    }

    /// The millisecond the id was minted at: when the store that minted it recorded its event.
    /// None when the id carries no timestamp, as an id that is not a `UUIDv7` may not.
    pub(crate) fn millis(self) -> Option<i64> {
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
    pub(crate) fn recorded(self) -> Result<i64, Error> {
        self.millis().ok_or_else(|| {
            Error::Refused(format!(
                "event {self} has no record time: its id is not a UUIDv7"
            ))
        })
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One change to a graph: its id, its place in the log, and its body, the `CloudEvent` as JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub id: EventId,
    /// The event's place in the log: reading since it returns the events written after it.
    pub cursor: Cursor,
    pub body: String,
}

/// Appends one event inside the caller's transaction, so it lands only if that transaction does.
pub(crate) fn append(tx: &Connection, body: &str) -> Result<Event, Error> {
    store(tx, EventId::now(), None, body)
}

/// The place the next event appended to the log takes.
pub(crate) fn next_cursor(tx: &Connection) -> Result<Cursor, Error> {
    Ok(Cursor(high_water(tx)?.0 + 1))
}

/// The highest place the log has ever given an event, or [`Cursor::START`] when it has given
/// none: `_events` is `AUTOINCREMENT`, so SQLite keeps it and never lowers it when an event is
/// deleted.
fn high_water(conn: &Connection) -> Result<Cursor, Error> {
    Ok(Cursor(
        conn.prepare_cached(HIGH_WATER)?
            .query_row([], |row| row.get(0))?,
    ))
}

/// The event at `cursor`, if the log holds one there.
pub(crate) fn event_at(conn: &Connection, cursor: Cursor) -> Result<Option<Event>, Error> {
    let row: Option<(String, String)> = conn
        .prepare_cached(EVENT_AT)?
        .query_row(params![cursor.0], |row| Ok((row.get(0)?, row.get(1)?)))
        .optional()?;
    row.map(|(id, body)| {
        Ok(Event {
            id: EventId(id.parse().map_err(|_| {
                Error::Refused(format!(
                    "event at {} has an id that is not a UUID",
                    cursor.0
                ))
            })?),
            cursor,
            body,
        })
    })
    .transpose()
}

/// Appends one event under the id it already has, inside the caller's transaction: at `place`
/// when it has one, otherwise at the end of the log.
pub(crate) fn store(
    tx: &Connection,
    id: EventId,
    place: Option<Cursor>,
    body: &str,
) -> Result<Event, Error> {
    tx.prepare_cached(APPEND_EVENT)?.execute(params![
        place.map(|cursor| cursor.0),
        body,
        id.to_string()
    ])?;
    Ok(Event {
        id,
        cursor: Cursor(tx.last_insert_rowid()),
        body: body.to_string(),
    })
}

/// A graph's event log.
#[derive(Debug)]
pub struct Events<'g> {
    pub(crate) conn: &'g Connection,
}

impl Events<'_> {
    /// The committed watermark: the highest place the log has given a committed event, or
    /// [`Cursor::START`] when it has given none. It never moves back. Another process on the same
    /// file learns of a commit by reading it, then reads [`Events::since_until`] its own cursor up
    /// to it; an event not yet committed is never under it.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn committed(&self) -> Result<Cursor, Error> {
        high_water(self.conn)
    }

    /// Every event after `cursor`, in the order they were written.
    ///
    /// # Errors
    /// The graph file cannot be read, or an event's id in it cannot be decoded.
    pub fn since(&self, cursor: Cursor) -> Result<Vec<Event>, Error> {
        self.since_until(cursor, Cursor(i64::MAX))
    }

    /// The events after `cursor` and up to `until`, `until` included, in the order they were
    /// written. No event past `until` is read.
    ///
    /// # Errors
    /// The graph file cannot be read, or an event's id in it cannot be decoded.
    pub fn since_until(&self, cursor: Cursor, until: Cursor) -> Result<Vec<Event>, Error> {
        let mut statement = self.conn.prepare_cached(EVENTS_BETWEEN)?;
        let mut rows = statement.query(params![cursor.0, until.0])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let id: String = row.get(1)?;
            events.push(Event {
                id: EventId(id.parse().map_err(|_| {
                    Error::Refused(format!("event at {seq} has an id that is not a UUID"))
                })?),
                cursor: Cursor(seq),
                body: row.get(2)?,
            });
        }
        Ok(events)
    }
}
