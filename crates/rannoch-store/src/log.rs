//! The log: every change to a graph is an event, read in order from a cursor and kept. An
//! event's body is the `CloudEvent` itself, as JSON.

use rannoch_cloudevents::EventId;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::handles::Run;
use crate::queries::{COUNT_BETWEEN, EVENTS_BETWEEN, EVENTS_CHUNK, HIGH_WATER, Sql};

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

/// An event as a row of the log holds it: its place, its id as text and its body.
pub(crate) fn decode(seq: i64, id: &str, body: String) -> Result<Event, Error> {
    Ok(Event {
        id: id
            .parse()
            .map_err(|_| Error::Refused(format!("event at {seq} has an id that is not a UUID")))?,
        cursor: Cursor(seq),
        body,
    })
}

/// The event at `cursor`, if the log holds one there.
pub(crate) fn event_at(conn: &impl Run, cursor: Cursor) -> Result<Option<Event>, Error> {
    let row: Option<(String, String)> = conn.with(Sql::EventAt, |s| {
        s.query_row(params![cursor.0], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
    })?;
    row.map(|(id, body)| decode(cursor.0, &id, body))
        .transpose()
}

/// Appends one event under the id it already has, inside the caller's transaction: at `place`
/// when it has one, otherwise at the end of the log.
pub(crate) fn store(
    tx: &impl Run,
    id: EventId,
    place: Option<Cursor>,
    body: &str,
) -> Result<Event, Error> {
    tx.with(Sql::AppendEvent, |s| {
        s.execute(params![place.map(|cursor| cursor.0), body, id.to_string()])
    })?;
    Ok(Event {
        id,
        cursor: Cursor(tx.conn().last_insert_rowid()),
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

    /// The place the next event appended to the log takes.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn next(&self) -> Result<Cursor, Error> {
        next_cursor(self.conn)
    }

    /// The event at `cursor`, if the log holds one there.
    ///
    /// # Errors
    /// The graph file cannot be read, or the event's id in it cannot be decoded.
    pub fn at(&self, cursor: Cursor) -> Result<Option<Event>, Error> {
        event_at(self.conn, cursor)
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
        let rows = statement.query(params![cursor.0, until.0])?;
        decode_rows(rows)
    }

    /// At most `limit` of the events after `cursor` and up to `until`, in the order they were
    /// written: [`Events::since_until`] a chunk at a time.
    ///
    /// # Errors
    /// The graph file cannot be read, or an event's id in it cannot be decoded.
    pub fn chunk(&self, cursor: Cursor, until: Cursor, limit: usize) -> Result<Vec<Event>, Error> {
        let mut statement = self.conn.prepare_cached(EVENTS_CHUNK)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = statement.query(params![cursor.0, until.0, limit])?;
        decode_rows(rows)
    }

    /// How many events the log holds after `cursor` and up to `until`.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn count(&self, cursor: Cursor, until: Cursor) -> Result<u64, Error> {
        let count: i64 = self
            .conn
            .prepare_cached(COUNT_BETWEEN)?
            .query_row(params![cursor.0, until.0], |row| row.get(0))?;
        Ok(u64::try_from(count).unwrap_or(0))
    }
}

/// Every event in `rows` of seq, id and body, in order.
fn decode_rows(mut rows: rusqlite::Rows<'_>) -> Result<Vec<Event>, Error> {
    let mut events = Vec::new();
    while let Some(row) = rows.next()? {
        let id: String = row.get(1)?;
        events.push(decode(row.get(0)?, &id, row.get(2)?)?);
    }
    Ok(events)
}
