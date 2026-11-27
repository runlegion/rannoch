//! graph toy round 5a: a statement tracer on each connection. A write start is the start of the
//! first write statement of a transaction (or of an autocommit write), the moment SQLite decides
//! whether it can rewind the WAL; a read is a SELECT or WITH outside a write transaction.

use rusqlite::Connection;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct Rec {
    pub tag: &'static str,
    pub label: &'static str,
    pub start: u64,
    pub end: u64,
    pub write: bool,
}

#[derive(Clone)]
pub struct Tracer {
    t0: Instant,
    until: Duration,
    recs: Arc<Mutex<Vec<Rec>>>,
}

fn label(tag: &'static str, sql: &str) -> &'static str {
    if sql.contains("e.status = 0 AND (e.run_at") {
        "queue: due select"
    } else if sql.contains("min(e.run_at)") {
        "queue: next run_at"
    } else if sql.contains("json_extract(w.data") {
        "processor: wait lookup"
    } else if sql.contains("WITH RECURSIVE tree") {
        "queue: tree"
    } else if sql.contains("status = -1") {
        "queue: failed check"
    } else if sql.contains("FROM _events WHERE id = ?1") {
        "queue: event by id"
    } else if sql.contains("wal_checkpoint") {
        "checkpoint"
    } else {
        tag
    }
}

impl Tracer {
    /// Records statements for `until` from now.
    pub fn new(until: Duration) -> Self {
        Self { t0: Instant::now(), until, recs: Arc::default() }
    }

    pub fn attach(&self, conn: &mut Connection, tag: &'static str) {
        let (t0, until, recs) = (self.t0, self.until, self.recs.clone());
        let (mut in_tx, mut first_write) = (false, false);
        let f = move |ev: TraceEvent<'_>| {
            let TraceEvent::Profile(stmt, dur) = ev else { return };
            let end = t0.elapsed();
            if end > until {
                return;
            }
            let sql = stmt.sql();
            let head = sql.trim_start().to_ascii_uppercase();
            let start = end.saturating_sub(dur);
            let rec = |write: bool, label: &'static str| Rec {
                tag,
                label,
                start: start.as_nanos() as u64,
                end: end.as_nanos() as u64,
                write,
            };
            if head.starts_with("BEGIN") {
                in_tx = true;
                first_write = true;
            } else if head.starts_with("COMMIT") || head.starts_with("ROLLBACK") || head.starts_with("END") {
                in_tx = false;
            } else if head.starts_with("INSERT") || head.starts_with("UPDATE") || head.starts_with("DELETE") || head.starts_with("REPLACE") {
                if !in_tx || first_write {
                    first_write = false;
                    if let Ok(mut r) = recs.lock() {
                        r.push(rec(true, tag));
                    }
                }
            } else if (head.starts_with("SELECT") || head.starts_with("WITH") || head.starts_with("PRAGMA WAL_CHECKPOINT")) && !in_tx {
                if let Ok(mut r) = recs.lock() {
                    r.push(rec(false, label(tag, &sql)));
                }
            }
        };
        conn.trace_v2(TraceEventCodes::SQLITE_TRACE_PROFILE, Some(f)).expect("trace");
    }

    /// The write starts, how many a read on another connection was open across, and by what.
    pub fn report(&self, name: &str) -> String {
        let recs = self.recs.lock().expect("recs").clone();
        let mut reads: Vec<Rec> = recs.iter().filter(|r| !r.write).copied().collect();
        reads.sort_by_key(|r| r.start);
        let writes: Vec<Rec> = recs.iter().filter(|r| r.write).copied().collect();
        let longest = reads.iter().map(|r| r.end - r.start).max().unwrap_or(0);
        let mut overlapped = 0usize;
        let mut by: HashMap<&'static str, usize> = HashMap::new();
        for w in &writes {
            // Reads that started before the write start and may still be open: those whose start
            // is within the longest read before it.
            let lo = reads.partition_point(|r| r.start < w.start.saturating_sub(longest));
            let hi = reads.partition_point(|r| r.start <= w.start);
            let open: Vec<&Rec> = reads[lo..hi].iter().filter(|r| r.end >= w.start && r.tag != w.tag).collect();
            if !open.is_empty() {
                overlapped += 1;
                for r in open {
                    *by.entry(r.label).or_default() += 1;
                }
            }
        }
        let mut by: Vec<_> = by.into_iter().collect();
        by.sort_by(|a, b| b.1.cmp(&a.1));
        let pct = if writes.is_empty() { 0.0 } else { 100.0 * overlapped as f64 / writes.len() as f64 };
        format!(
            "{name}: {} write starts, {overlapped} ({pct:.1}%) with a read open on another connection; {} reads traced; open reads by kind: {:?}",
            writes.len(),
            reads.len(),
            by
        )
    }
}
