#![warn(missing_docs)]
//! Every event rannoch writes also goes to a log file. The log sits on the event, not in a flow:
//! an app opts in by wrapping its append in a [`Log`] with the log file's path from its settings,
//! and putting the `Log` in rannoch's `Config` as its append, as in
//! `Log::new(Events::new(conn, signal), path)`. After the append it wraps commits a batch, the
//! `Log` writes each event of the batch to the file as one JSON line: `{"id": .., "source": ..,
//! "ty": .., "data": .., "run_at": .., "status": ..}`.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use chrono::SecondsFormat;
use rannoch_core::{Append, Error, Event};
use serde::Serialize;
use serde_json::json;

/// The app's append, with each event of each batch it commits also written to the file at its
/// path.
#[derive(Debug)]
pub struct Log<A> {
    append: A,
    path: PathBuf,
}

impl<A> Log<A> {
    /// `append`, with each event of each batch it commits written to the file at `path`, which
    /// is created when it is not there.
    #[must_use]
    pub fn new(append: A, path: PathBuf) -> Self {
        Self { append, path }
    }

    /// Writes each event of `batch` to the file as one JSON line.
    fn write<T: Copy + Into<u16>, D: Serialize>(
        &self,
        batch: &[Event<T, D>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut lines = String::new();
        for event in batch {
            let (id, source) = (event.id.to_string(), event.source.map(|s| s.to_string()));
            let (ty, status): (u16, i8) = (event.ty.into(), event.status as i8);
            let data = serde_json::to_value(&event.data)?;
            let run_at = event.run_at;
            let run_at = run_at.map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true));
            let line = json!({ "id": id, "source": source, "ty": ty, "data": data,
                "run_at": run_at, "status": status });
            lines.push_str(&line.to_string());
            lines.push('\n');
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(lines.as_bytes())?;
        Ok(())
    }
}

impl<T: Copy + Into<u16>, D: Serialize, A: Append<T, D>> Append<T, D> for Log<A> {
    fn append(&self, batch: &[Event<T, D>]) -> Result<Vec<Event<T, D>>, Error> {
        let replies = self.append.append(batch)?;
        if let Err(e) = self.write(batch) {
            let path = self.path.display();
            log::error!("the events log could not write a batch to {path}: {e}");
        }
        Ok(replies)
    }
}
