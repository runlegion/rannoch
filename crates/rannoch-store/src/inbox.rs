//! The inbox's places: each producer's position in its source, and for each worker where every
//! committed event stands. A worker's place is the last cursor handed out to it; every event after
//! it is waiting, and of those at or before it only the ones in progress or failed have a row.

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::log::{Cursor, Event, decode, event_at};
use crate::queries::{
    DONE_WORK, FAIL_WORK, JOIN_WORKER, NEXT_EVENT, OPEN_WORK, PLACE, POSITION, PROCESSED,
    REMOVE_PLACE, REMOVE_WORK, SET_PLACE, START_WORK, WORK,
};
use crate::{Error, Graph};

/// The inbox worker that fill runs as: its place is the last event filled, and its failed rows
/// are the events fill could not fill. A worker of an app must not take this name.
pub const FILL: &str = "rannoch.fill";

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

    /// Hands out to worker `worker` its oldest event an earlier run left in progress (one this
    /// handle has not handed out), so a worker that restarts gets back what it had not closed;
    /// otherwise the first event after its place and at or below `until`, marking it in progress
    /// and moving the worker's place to it; `None` when there is neither. A worker exists from its
    /// first call, holding the processed mark at the start until it finishes something.
    ///
    /// # Errors
    /// The graph file cannot be read or written, or the event's id cannot be decoded.
    pub fn next(&mut self, worker: &str, until: Cursor) -> Result<Option<Event>, Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let open: Vec<i64> = tx
            .prepare_cached(OPEN_WORK)?
            .query_map([worker], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        if let Some(seq) = open
            .into_iter()
            .find(|seq| !self.handed.contains(&(worker.to_string(), *seq)))
        {
            let event = event_at(&tx, Cursor(seq))?;
            drop(tx);
            self.handed.insert((worker.to_string(), seq));
            return Ok(event);
        }
        tx.prepare_cached(JOIN_WORKER)?.execute([worker])?;
        let place = place(&tx, worker)?;
        let row: Option<(i64, String, String)> = tx
            .prepare_cached(NEXT_EVENT)?
            .query_row(params![place.0, until.0], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()?;
        let Some((seq, id, body)) = row else {
            tx.commit()?;
            return Ok(None);
        };
        let event = decode(seq, &id, body)?;
        tx.prepare_cached(SET_PLACE)?
            .execute(params![worker, seq])?;
        tx.prepare_cached(START_WORK)?
            .execute(params![worker, seq])?;
        tx.commit()?;
        self.handed.insert((worker.to_string(), seq));
        Ok(Some(event))
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

impl Graph {
    /// The processed mark: the lowest point every worker has finished, so no event at or before
    /// it is waiting or in progress for any worker. A failed event counts as finished. With no
    /// worker it is the committed watermark, and it never passes it. It deletes nothing.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn processed(&self) -> Result<Cursor, Error> {
        let committed = self.events().committed()?;
        let mark: Option<i64> = self
            .conn
            .prepare_cached(PROCESSED)?
            .query_row([], |row| row.get(0))?;
        Ok(mark.map_or(committed, |mark| Cursor(mark.min(committed.0))))
    }

    /// Removes worker `worker`, its place and its open and failed events, releasing the mark it
    /// held.
    ///
    /// # Errors
    /// The graph file cannot be written.
    pub fn remove_worker(&mut self, worker: &str) -> Result<(), Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.prepare_cached(REMOVE_PLACE)?.execute([worker])?;
        tx.prepare_cached(REMOVE_WORK)?.execute([worker])?;
        tx.commit()?;
        self.handed.retain(|(w, _)| w != worker);
        Ok(())
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
pub(crate) fn place(conn: &rusqlite::Connection, worker: &str) -> Result<Cursor, Error> {
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
        g.commit(&[body, body, body, body], &[(3, "p", "line 4")])?;
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
    fn a_worker_gets_back_what_it_left_in_progress() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-inbox-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(&path, &[7; 32])?;
        let body = r#"{"specversion":"1.0","id":"x","source":"s","type":"t"}"#;
        g.commit(&[body, body], &[])?;
        let until = g.events().committed()?;
        let first = g.next("w", until)?.map(|e| e.cursor);
        drop(g);
        let mut g = open(&path, &[7; 32])?;
        assert_eq!(
            g.next("w", until)?.map(|e| e.cursor),
            first,
            "handed back after reopening"
        );
        g.done("w", Cursor(1))?;
        assert_eq!(g.next("w", until)?.map(|e| e.cursor), Some(Cursor(2)));
        Ok(())
    }

    #[test]
    fn the_processed_mark_is_the_lowest_point_every_worker_has_finished()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("rannoch-inbox-{}.db", uuid::Uuid::now_v7()));
        let mut g = open(&path, &[7; 32])?;
        let body = r#"{"specversion":"1.0","id":"x","source":"s","type":"t"}"#;
        assert_eq!(g.next("late", Cursor(0))?, None);
        g.commit(&[body, body, body, body], &[])?;
        let until = g.events().committed()?;
        assert_eq!(
            g.processed()?,
            Cursor(0),
            "a worker with nothing handed out holds the start"
        );
        g.remove_worker("late")?;
        assert_eq!(g.processed()?, until, "no worker: the committed watermark");
        let first = g.next("w", until)?.ok_or("first")?.cursor;
        assert_eq!(g.processed()?, Cursor(0));
        g.done("w", first)?;
        assert_eq!(g.processed()?, first);
        let (second, third) = (
            g.next("w", until)?.ok_or("second")?.cursor,
            g.next("w", until)?.ok_or("third")?.cursor,
        );
        g.done("w", third)?;
        assert_eq!(g.processed()?, first, "the second is still in progress");
        g.failed("w", second, "bad")?;
        assert_eq!(g.processed()?, third, "a failed event counts as finished");
        let other = open(&path, &[7; 32])?;
        assert_eq!(other.processed()?, third, "readable from another handle");
        g.next("v", until)?;
        assert_eq!(g.processed()?, Cursor(0));
        g.remove_worker("v")?;
        assert_eq!(g.processed()?, third, "removing a worker releases the mark");
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
