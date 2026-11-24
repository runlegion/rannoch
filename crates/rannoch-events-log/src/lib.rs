#![warn(missing_docs)]
//! Every event rannoch writes to `_events` also goes to a log file, one JSON line per event, as
//! `_events` holds it. The log sits on the event, not in a flow: an app opts in by wrapping its
//! append in a [`Log`] with the log file's path from its settings, and putting the `Log` in
//! rannoch's `Config` as its append, as in `Log::new(Events::new(conn, signal), path)`.
//!
//! Once the append it wraps has written a batch, the `Log` reads each event of the batch back from
//! `_events` through [`Store::connection`], the one store, and appends one line for each row it
//! finds, in the order written: `{"id": .., "source": .., "ty": .., "data": .., "run_at": ..,
//! "status": ..}`, each value as the column holds it. An event the append did not write, as a 2
//! whose `store` row was refused, has no row and no line.
//!
//! # Failures
//!
//! When the append it wraps fails, the `Log` returns that failure and logs nothing. When reading
//! the batch back or writing the file fails, the batch stays written, the failure is logged at
//! `error`, and the batch's events have no line.

mod queries;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};

use rannoch_core::{Append, Error, Event};
use rannoch_store::Store;
use serde_json::Value;

/// The app's append, with each event it writes also logged to the file at its path.
#[derive(Debug)]
pub struct Log<A> {
    append: A,
    path: PathBuf,
    /// Held across each write, so the lines of two inboxes' batches do not interleave.
    writing: Mutex<()>,
}

impl<A> Log<A> {
    /// `append`, with each event it writes logged to the file at `path`, which is created when it
    /// is not there.
    #[must_use]
    pub fn new(append: A, path: PathBuf) -> Self {
        Self {
            append,
            path,
            writing: Mutex::new(()),
        }
    }

    /// Appends one line to the file for each event of `batch` that `_events` holds.
    fn log<T, D>(&self, batch: &[Event<T, D>]) -> Result<(), Box<dyn std::error::Error>> {
        let ids: Vec<Value> = batch.iter().map(|e| e.id.to_string().into()).collect();
        let conn = Store::connection()?;
        let mut select = conn.prepare(queries::ROWS)?;
        let rows = select.query_map([Value::Array(ids).to_string()], |r| r.get::<_, String>(0))?;
        let mut lines = String::new();
        for row in rows {
            lines.push_str(&row?);
            lines.push('\n');
        }
        let _writing = self.writing.lock().unwrap_or_else(PoisonError::into_inner);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(lines.as_bytes())?;
        Ok(())
    }
}

impl<T, D, A: Append<T, D>> Append<T, D> for Log<A> {
    fn append(&self, batch: &[Event<T, D>]) -> Result<Vec<Event<T, D>>, Error> {
        let replies = self.append.append(batch)?;
        if let Err(e) = self.log(batch) {
            let path = self.path.display();
            log::error!("the events log could not log a batch to {path}: {e}");
        }
        Ok(replies)
    }
}
