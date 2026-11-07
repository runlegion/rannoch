//! The trickle reader: reads the log stream after the fact and builds hearings (who said what,
//! when) in a file of its own, apart from the fast lane's store.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rusqlite::{Connection, params};

use crate::entrance::admit;

/// Reads `log` into hearings at `hearings`, following the file while `writing` is set, then to its
/// end. Counts every line read into `done`.
///
/// # Errors
/// A file cannot be read or written.
pub fn follow(log: &Path, hearings: &Path, writing: &Arc<AtomicBool>, done: &Arc<AtomicU64>) -> Result<u64, String> {
    let err = |e: rusqlite::Error| e.to_string();
    let mut conn = Connection::open(hearings).map_err(err)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
         CREATE TABLE IF NOT EXISTS hearings(id TEXT PRIMARY KEY, type TEXT NOT NULL, source TEXT NOT NULL,
             subject TEXT, time INTEGER NOT NULL, causation TEXT, heard INTEGER NOT NULL,
             content TEXT NOT NULL, at INTEGER NOT NULL, len INTEGER NOT NULL) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS hearings_subject ON hearings(subject, time);
         CREATE INDEX IF NOT EXISTS hearings_content ON hearings(content);",
    )
    .map_err(err)?;
    while !log.exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut reader = BufReader::with_capacity(1 << 20, File::open(log).map_err(|e| e.to_string())?);
    let mut line = String::new();
    let mut n = 0u64;
    // Where each line starts in the log, so a hearing can say where its event is.
    let mut at = 0u64;
    loop {
        let still = writing.load(Ordering::SeqCst);
        let tx = conn.transaction().map_err(err)?;
        let mut batch = 0;
        {
            let mut put = tx
                .prepare_cached("INSERT OR IGNORE INTO hearings VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)")
                .map_err(err)?;
            loop {
                line.clear();
                let got = reader.read_line(&mut line).map_err(|e| e.to_string())?;
                if got == 0 || !line.ends_with('\n') {
                    if got > 0 {
                        let back = i64::try_from(got).unwrap_or(0);
                        std::io::Seek::seek_relative(&mut reader, -back).map_err(|e| e.to_string())?;
                    }
                    break;
                }
                if let Ok(e) = admit(line.trim_end(), 0) {
                    n += 1;
                    // Who said what: content is what the event says about its subject, so the
                    // same content under another id is the same hearing's alias.
                    let content = blake3::hash(format!("{}\n{}\n{}", e.ty, e.subject.as_deref().unwrap_or(""), e.data).as_bytes()).to_hex().to_string();
                    let start = i64::try_from(at).unwrap_or(0);
                    let len = i64::try_from(got).unwrap_or(0);
                    put.execute(params![e.id, e.ty, e.source, e.subject, e.time, e.causation, i64::try_from(n).unwrap_or(0), content, start, len]).map_err(err)?;
                }
                at += got as u64;
                done.fetch_add(1, Ordering::SeqCst);
                batch += 1;
                if batch >= 8192 {
                    break;
                }
            }
        }
        tx.commit().map_err(err)?;
        if batch == 0 {
            if !still {
                return Ok(n);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}
