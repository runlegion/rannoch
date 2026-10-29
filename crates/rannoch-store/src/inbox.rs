//! The inbox's places: each producer's position in its source, and for each worker where every
//! committed event stands. A worker's place is the last cursor handed out to it; every event after
//! it is waiting, and of those at or before it only the ones in progress or failed have a row.

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::log::{Cursor, Event};
use crate::queries::{
    DONE_WORK, FAIL_WORK, NEXT_EVENT, PLACE, POSITION, SET_PLACE, START_WORK, WORK,
};
use crate::{Error, Graph};

/// Where a committed event stands for one worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Not yet handed out to the worker.
    Waiting,
    /// Handed out and not yet closed.
    InProgress,
    /// Closed as done.
    Done,
    /// Closed as failed, with the reason.
    Failed(String),
}

impl Graph {
    /// Producer `producer`'s position in its source, as last committed, or `None` before its
    /// first.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn position(&self, producer: &str) -> Result<Option<String>, Error> {
        Ok(self
            .conn
            .prepare_cached(POSITION)?
            .query_row([producer], |row| row.get(0))
            .optional()?)
    }

    /// Hands out to worker `worker` the first event after its place and at or below `until`,
    /// marking it in progress and moving the worker's place to it, or `None` when there is none.
    ///
    /// # Errors
    /// The graph file cannot be read or written, or the event's id cannot be decoded.
    pub fn next(&mut self, worker: &str, until: Cursor) -> Result<Option<Event>, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let place = place(&tx, worker)?;
        let row: Option<(i64, String, String)> = tx
            .prepare_cached(NEXT_EVENT)?
            .query_row(params![place.0, until.0], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()?;
        let Some((seq, id, body)) = row else {
            return Ok(None);
        };
        let id = id
            .parse()
            .map_err(|_| Error::Refused(format!("event at {seq} has an id that is not a UUID")))?;
        tx.prepare_cached(SET_PLACE)?
            .execute(params![worker, seq])?;
        tx.prepare_cached(START_WORK)?
            .execute(params![worker, seq])?;
        tx.commit()?;
        Ok(Some(Event {
            id,
            cursor: Cursor(seq),
            body,
        }))
    }

    /// Closes the event at `cursor`, in progress for worker `worker`, as done.
    ///
    /// # Errors
    /// [`Error::Refused`] when the event is not in progress for the worker; the graph file
    /// cannot be written.
    pub fn done(&mut self, worker: &str, cursor: Cursor) -> Result<(), Error> {
        let closed = self
            .conn
            .prepare_cached(DONE_WORK)?
            .execute(params![worker, cursor.0])?;
        in_progress(closed, worker, cursor)
    }

    /// Closes the event at `cursor`, in progress for worker `worker`, as failed with `reason`.
    ///
    /// # Errors
    /// [`Error::Refused`] when the event is not in progress for the worker; the graph file
    /// cannot be written.
    pub fn failed(&mut self, worker: &str, cursor: Cursor, reason: &str) -> Result<(), Error> {
        let closed = self
            .conn
            .prepare_cached(FAIL_WORK)?
            .execute(params![worker, cursor.0, reason])?;
        in_progress(closed, worker, cursor)
    }

    /// Where the event at `cursor` stands for worker `worker`.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn state(&self, worker: &str, cursor: Cursor) -> Result<State, Error> {
        if cursor > place(&self.conn, worker)? {
            return Ok(State::Waiting);
        }
        let row: Option<Option<String>> = self
            .conn
            .prepare_cached(WORK)?
            .query_row(params![worker, cursor.0], |row| row.get(0))
            .optional()?;
        Ok(match row {
            None => State::Done,
            Some(None) => State::InProgress,
            Some(Some(reason)) => State::Failed(reason),
        })
    }
}

/// Refuses a close that closed no row: the event was not in progress for the worker.
fn in_progress(closed: usize, worker: &str, cursor: Cursor) -> Result<(), Error> {
    if closed == 0 {
        return Err(Error::Refused(format!(
            "event at {} is not in progress for worker {worker}",
            cursor.0
        )));
    }
    Ok(())
}

/// Worker `worker`'s place, or [`Cursor::START`] before its first event.
fn place(conn: &rusqlite::Connection, worker: &str) -> Result<Cursor, Error> {
    Ok(Cursor(
        conn.prepare_cached(PLACE)?
            .query_row([worker], |row| row.get(0))
            .optional()?
            .unwrap_or(0),
    ))
}

#[cfg(test)]
mod tests {
    use crate::{Cursor, State, open};

    #[test]
    fn a_worker_s_states_survive_reopening() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-inbox-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(&path, &[7; 32])?;
        let body = r#"{"specversion":"1.0","id":"x","source":"s","type":"t"}"#;
        g.commit(&[body, body, body, body], &[("p", "line 4")])?;
        let until = g.events().committed()?;
        let mut handed = Vec::new();
        while let Some(event) = g.next("w", until)? {
            handed.push(event.cursor);
            if handed.len() == 3 {
                break;
            }
        }
        g.done("w", handed[0])?;
        g.failed("w", handed[1], "no such book")?;
        assert!(g.done("w", handed[0]).is_err(), "closed twice");
        drop(g);
        let g = open(&path, &[7; 32])?;
        let states: Vec<State> = (1..=4)
            .map(|c| g.state("w", Cursor(c)))
            .collect::<Result<_, _>>()?;
        assert_eq!(
            states,
            [
                State::Done,
                State::Failed("no such book".to_string()),
                State::InProgress,
                State::Waiting
            ]
        );
        assert_eq!(g.state("other", Cursor(1))?, State::Waiting);
        assert_eq!(g.position("p")?.as_deref(), Some("line 4"));
        Ok(())
    }

    #[test]
    fn nothing_past_until_is_handed_out() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-inbox-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(&path, &[7; 32])?;
        let body = r#"{"specversion":"1.0","id":"x","source":"s","type":"t"}"#;
        g.commit(&[body, body], &[])?;
        assert_eq!(g.next("w", Cursor(1))?.map(|e| e.cursor), Some(Cursor(1)));
        assert_eq!(g.next("w", Cursor(1))?, None);
        Ok(())
    }
}
