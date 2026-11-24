#![warn(missing_docs)]
//! Every event in `_events` also goes to a log file, one JSON line per event, as `_events` holds
//! it. The log sits on the event, not in a flow: an app opts in by wrapping its append in a
//! [`Log`] with the log file's path from its settings, and putting the `Log` in rannoch's
//! `Config` as its append, as in `Log::new(Events::new(conn, signal), path)?`.
//!
//! Once the append it wraps has written a batch, the `Log` reads back through
//! [`Store::connection`], the one store, the batch's rows by their ids and every row that reached
//! `_events` since its last read, each once, in the order the rows were written, and appends one
//! line for each: `{"id": .., "source": .., "ty": .., "data": .., "run_at": .., "status": ..}`,
//! each value as the column holds it. A row written outside the append, as the queue's -1 for an
//! event whose `ty` is not the app's, has its line with the next batch. The `Log`'s first read is
//! at `Log::new`, so the rows already in `_events` then have no line. An event the append did not
//! write, as a 2 whose `store` row was refused, has no row and no line.
//!
//! SQLite gives a new row the largest rowid in `_events` plus one. When the newest rows are
//! deleted, as by rannoch-events-trim with an `older_than_ms` near 0, the rows written next take
//! rowids at or below the last the `Log` read. The batch's rows are still read by their ids, but a
//! row written outside the append between that delete and the next batch is never logged.
//!
//! # Failures
//!
//! When the append it wraps fails, the `Log` returns that failure and logs nothing. When reading
//! back or writing the file fails, the batch stays written, the failure is logged at `error`, and
//! the rows are read again after the next batch.

mod queries;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};

use rannoch_core::{Append, Error, Event};
use rannoch_store::Store;
use serde_json::Value;

/// The app's append, with each row that reaches `_events` also logged to the file at its path.
#[derive(Debug)]
pub struct Log<A> {
    append: A,
    path: PathBuf,
    /// The rowid of the last row read, held across each append, read and write, so the batches
    /// of two inboxes neither interleave their lines nor log a row twice.
    last: Mutex<i64>,
}

impl<A> Log<A> {
    /// `append`, with each row that reaches `_events` from now on logged to the file at `path`,
    /// which is created when it is not there.
    ///
    /// # Errors
    ///
    /// `Error::Store` when the store cannot be read.
    pub fn new(append: A, path: PathBuf) -> Result<Self, Error> {
        let conn = Store::connection()?;
        let last = conn.query_row(queries::LAST, [], |r| r.get(0));
        let last = Mutex::new(last.map_err(|_| Error::Store)?);
        Ok(Self { append, path, last })
    }

    /// Appends one line to the file for each row of `batch` in `_events` and each row that
    /// reached it since the last read, `last`, which then becomes the last row read.
    fn log<T, D>(
        &self,
        batch: &[Event<T, D>],
        last: &mut i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ids: Vec<Value> = batch.iter().map(|e| e.id.to_string().into()).collect();
        let conn = Store::connection()?;
        let mut select = conn.prepare(queries::ROWS)?;
        let rows = (Value::Array(ids).to_string(), *last);
        let rows = select.query_map(rows, |r| Ok((r.get(0)?, r.get::<_, String>(1)?)))?;
        let (mut lines, mut read) = (String::new(), *last);
        for row in rows {
            let (rowid, line) = row?;
            lines.push_str(&line);
            lines.push('\n');
            read = rowid;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(lines.as_bytes())?;
        *last = read;
        Ok(())
    }
}

impl<T, D, A: Append<T, D>> Append<T, D> for Log<A> {
    fn append(&self, batch: &[Event<T, D>]) -> Result<Vec<Event<T, D>>, Error> {
        let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        let replies = self.append.append(batch)?;
        if let Err(e) = self.log(batch, &mut last) {
            let path = self.path.display();
            log::error!("the events log could not log a batch to {path}: {e}");
        }
        Ok(replies)
    }
}
