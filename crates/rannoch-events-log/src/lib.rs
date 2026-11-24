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
//! event whose `ty` is not the app's, has its line with the next batch. An event the append did
//! not write, as a 2 whose `store` row was refused, has no row and no line.
//!
//! `Log::new` starts after the row the file's last line names, so a restart loses no line; it reads
//! that line back from the end of the file, so its start does not grow with the log. When the file
//! is not there or has no line, or its last line names no row in `_events`, as after trim deleted
//! that row, or does not parse, as after a crash mid-write, it starts at the table's largest rowid,
//! and the rows already in `_events` then have no line. When the file is there but cannot be read,
//! `Log::new` fails with `Error::Config`.
//!
//! SQLite gives a new row the largest rowid in `_events` plus one. When the newest rows are
//! deleted, as by rannoch-events-trim with an `older_than_ms` near 0, the rows written next take
//! rowids at or below the last the `Log` read. The batch's rows are still read by their ids, but a
//! row written outside the append between that delete and the next batch is never logged.
//!
//! # Failures
//!
//! When the append it wraps fails, the `Log` returns that failure and logs nothing. When opening
//! the file, reading back or writing the file fails, the batch stays written, the failure is
//! logged at `error`, and the rows are read again after the next batch.

mod queries;

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
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
    /// `append`, with each row that reaches `_events` after the row the last line of the file at
    /// `path` names, or from now on, logged to that file, which is created when it is not there.
    ///
    /// # Errors
    ///
    /// `Error::Config` when the file is there but cannot be read; `Error::Store` when the store
    /// cannot be read.
    pub fn new(append: A, path: PathBuf) -> Result<Self, Error> {
        let id = last_line(&path)?.and_then(|line| {
            let line: Value = serde_json::from_slice(&line).ok()?;
            line["id"].as_str().map(str::to_owned)
        });
        let conn = Store::connection()?;
        let last = conn.query_row(queries::START, [id], |r| r.get(0));
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
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
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
        file.write_all(lines.as_bytes())?;
        *last = read;
        Ok(())
    }
}

/// How many bytes [`last_line`] reads back from the file's end at a time.
const CHUNK: u64 = 4096;

/// The last line of the file at `path`, read back from its end, or none when the file has no
/// line or is not there.
fn last_line(path: &Path) -> Result<Option<Vec<u8>>, Error> {
    let back = |mut file: File| -> std::io::Result<Option<Vec<u8>>> {
        let (mut end, mut tail) = (file.seek(SeekFrom::End(0))?, Vec::new());
        loop {
            // The last line ends at the file's final newline, or at its end without one.
            let line = tail.strip_suffix(b"\n").unwrap_or(&tail);
            if let Some(at) = line.iter().rposition(|b| *b == b'\n') {
                return Ok(Some(line[at + 1..].to_vec()));
            }
            if end == 0 {
                return Ok((!line.is_empty()).then(|| line.to_vec()));
            }
            let start = end.saturating_sub(CHUNK);
            let mut chunk = vec![0; usize::try_from(end - start).map_err(std::io::Error::other)?];
            file.seek(SeekFrom::Start(start))?;
            file.read_exact(&mut chunk)?;
            chunk.append(&mut tail);
            (end, tail) = (start, chunk);
        }
    };
    match File::open(path) {
        Ok(file) => back(file),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
    .map_err(|_| Error::Config)
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    #[test]
    fn last_line_reads_a_line_longer_than_a_chunk_back_from_the_end() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp/events-log-last");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("events.log");
        let long = "x".repeat(10_000);
        for (body, last) in [
            (format!("a\n{long}\n"), Some(long.as_str())),
            (format!("a\n{long}"), Some(long.as_str())),
            (format!("{long}\n"), Some(long.as_str())),
            ("a\n".to_owned(), Some("a")),
            (String::new(), None),
        ] {
            fs::write(&path, &body).unwrap();
            let want = last.map(|l| l.as_bytes().to_vec());
            assert_eq!(super::last_line(&path).unwrap(), want, "{}", body.len());
        }
        assert_eq!(super::last_line(&dir.join("none.log")).unwrap(), None);
    }
}
