//! The queue: when the events crate signals, or when an event comes due, it takes the events at
//! status 0 that are due and have no status event whose source is them, sends a status 1 for
//! each, and hands each to its processor: runs, whose flow has no job step, one after another;
//! jobs, whose flow has a job step, each on a thread of its own. Processors send their own
//! results; the queue sends only the 1s, and a -1 saying "no processor" for an event whose `ty`
//! has no flow.

mod queries;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use rannoch_core::{Event, Sender, Status};
use rusqlite::{Connection, Params};
use uuid::Uuid;

/// An event as `_events` holds it: `ty` as its `u16`, data as its JSON text.
pub type Row = Event<u16, String>;

/// A processor: handed the event it runs, that event's root and a sender, it sends its own
/// result about the event it runs.
pub type Processor<T, D> = Box<dyn Fn(&Row, &Row, &Sender<T, D>) + Send + Sync>;

/// What runs an event: the processor, and whether its flow has a job step.
pub struct Flow<T, D> {
    /// Long-running work: a job runs on a thread of its own and never holds up a run.
    pub job: bool,
    /// The processor: it is handed the event's root and a sender, and sends its own result.
    pub run: Processor<T, D>,
}

impl<T, D> std::fmt::Debug for Flow<T, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flow")
            .field("job", &self.job)
            .finish_non_exhaustive()
    }
}

/// The app's flows: the flow for an event's `ty`, or none.
pub trait Flows<T, D>: Send + Sync {
    /// The flow that runs events of `ty`, or `None` when there is none.
    fn flow(&self, ty: u16) -> Option<Arc<Flow<T, D>>>;
}

/// The running queue; [`Queue::join`] waits for it to stop, which it does when the events
/// crate's signal closes.
#[derive(Debug)]
pub struct Queue {
    thread: JoinHandle<()>,
}

impl Queue {
    /// Starts the queue over `conn`, woken by `signal`, sending through `sender`.
    #[must_use]
    pub fn start<T, D>(
        conn: Connection,
        signal: Receiver<()>,
        sender: Sender<T, D>,
        flows: Arc<dyn Flows<T, D>>,
    ) -> Self
    where
        T: TryFrom<u16> + Copy + PartialEq + Send + 'static,
        D: From<String> + Send + 'static,
    {
        Self {
            thread: thread::spawn(move || run(&conn, &signal, &sender, &*flows)),
        }
    }

    /// Waits until the queue has stopped and every run and job it started has finished.
    pub fn join(self) {
        let _ = self.thread.join();
    }
}

/// How long the queue waits for a signal when nothing it knows of comes due.
const IDLE: Duration = Duration::from_secs(3600);

fn run<T, D>(
    conn: &Connection,
    signal: &Receiver<()>,
    sender: &Sender<T, D>,
    flows: &dyn Flows<T, D>,
) where
    T: TryFrom<u16> + Copy + PartialEq + Send + 'static,
    D: From<String> + Send + 'static,
{
    let (runs, bucket) = mpsc::channel::<(Arc<Flow<T, D>>, Row, Row)>();
    let run_sender = sender.clone();
    let runs_thread = thread::spawn(move || {
        for (flow, event, root) in bucket {
            (flow.run)(&event, &root, &run_sender);
        }
    });
    let mut jobs: Vec<JoinHandle<()>> = Vec::new();
    let mut taken = HashSet::new();
    // Events already due when the queue starts are taken at once.
    let mut wait = Duration::ZERO;
    loop {
        if signal.recv_timeout(wait) == Err(RecvTimeoutError::Disconnected) {
            break;
        }
        jobs.retain(|job| !job.is_finished());
        let Ok(due) = read(conn, queries::DUE, [now()]) else {
            log::error!("the queue could not read the due events");
            continue;
        };
        // An event whose 1 is kept has left the due events; until then the queue remembers it.
        taken.retain(|id| due.iter().any(|row| row.id == *id));
        for row in due {
            if !taken.insert(row.id) {
                continue;
            }
            let Ok(ty) = T::try_from(row.ty) else {
                log::error!(
                    "event {} has a ty the app does not know: {}",
                    row.id,
                    row.ty
                );
                continue;
            };
            let root = match root(conn, row.clone()) {
                Ok(root) => root,
                Err(err) => {
                    log::error!("the queue could not find the root of {}: {err}", row.id);
                    taken.remove(&row.id);
                    continue;
                }
            };
            // An event whose 1 was not accepted stays at 0 and is not run.
            if !send(sender, row.id, ty, "taken", Status::InProgress) {
                taken.remove(&row.id);
                continue;
            }
            let Some(flow) = flows.flow(row.ty) else {
                send(sender, row.id, ty, "no processor", Status::Binned);
                continue;
            };
            if flow.job {
                let job_sender = sender.clone();
                jobs.push(thread::spawn(move || {
                    (flow.run)(&row, &root, &job_sender);
                }));
            } else if runs.send((flow, row, root)).is_err() {
                log::error!("the queue's runs have stopped");
            }
        }
        wait = next_due(conn).unwrap_or(IDLE);
    }
    drop(runs);
    let _ = runs_thread.join();
    for job in jobs {
        let _ = job.join();
    }
}

/// Sends a status event about `source`, with `message` as its data; whether it was accepted.
fn send<T: PartialEq, D: From<String>>(
    sender: &Sender<T, D>,
    source: Uuid,
    ty: T,
    message: &str,
    status: Status,
) -> bool {
    let event = Event {
        id: Uuid::nil(),
        source: Some(source),
        ty,
        data: D::from(message.to_owned()),
        run_at: None,
        status,
    };
    let sent = sender.send(event).is_ok();
    if !sent {
        log::error!("the queue could not send a status for {source}: rannoch is closed");
    }
    sent
}

/// The root whose data every flow uses: back from `row` through each source that has a -1.
fn root(conn: &Connection, row: Row) -> rusqlite::Result<Row> {
    let mut at = row;
    while let Some(source) = at.source {
        let failed: bool = conn.query_row(queries::FAILED, [source.to_string()], |r| r.get(0))?;
        if !failed {
            break;
        }
        match read(conn, queries::EVENT, [source.to_string()])?.pop() {
            Some(earlier) => at = earlier,
            None => break,
        }
    }
    Ok(at)
}

/// How long until the earliest `run_at` the queue knows of, or `None` when there is none.
fn next_due(conn: &Connection) -> Option<Duration> {
    let next: Option<String> = match conn.query_row(queries::NEXT_RUN_AT, [now()], |r| r.get(0)) {
        Ok(next) => next,
        Err(err) => {
            log::error!("the queue could not read the next run_at: {err}");
            return None;
        }
    };
    let at: DateTime<Utc> = match next?.parse() {
        Ok(at) => at,
        Err(err) => {
            log::error!("the queue could not read a run_at: {err}");
            return None;
        }
    };
    Some((at - Utc::now()).to_std().unwrap_or(Duration::ZERO))
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The rows `sql` selects, as id, source, ty, data, `run_at` and status.
fn read(conn: &Connection, sql: &str, params: impl Params) -> rusqlite::Result<Vec<Row>> {
    let mut select = conn.prepare_cached(sql)?;
    let rows = select.query_map(params, |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, u16>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, i64>(5)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, source, ty, data, run_at, status) = row?;
        let (Ok(id), Ok(source), Ok(run_at), Some(status)) = (
            id.parse(),
            source.map(|s| s.parse()).transpose(),
            run_at.map(|t| t.parse()).transpose(),
            status_of(status),
        ) else {
            log::error!("the queue skipped a row it could not read");
            continue;
        };
        out.push(Event {
            id,
            source,
            ty,
            data,
            run_at,
            status,
        });
    }
    Ok(out)
}

fn status_of(stored: i64) -> Option<Status> {
    match stored {
        -1 => Some(Status::Binned),
        0 => Some(Status::Added),
        1 => Some(Status::InProgress),
        2 => Some(Status::Processed),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{queries, read, root};
    use rannoch_core::forgr::_events;
    use uuid::Uuid;

    fn put(conn: &rusqlite::Connection, source: Option<&str>, data: &str, status: i64) -> String {
        let id = Uuid::now_v7().to_string();
        let row = _events::factory()
            .id(id.clone())
            .ty(1)
            .data(data)
            .status(status);
        match source {
            Some(s) => row.source(s.to_owned()),
            None => row,
        }
        .insert(conn)
        .unwrap();
        id
    }

    fn event(conn: &rusqlite::Connection, id: &str) -> super::Row {
        read(conn, queries::EVENT, [id]).unwrap().pop().unwrap()
    }

    #[test]
    fn an_event_that_is_not_a_retry_is_its_own_root() {
        let store = rannoch_core::forgr::empty_store();
        let first = put(&store, None, "{\"in\":1}", 0);
        let done = put(&store, Some(&first), "made", 2);
        let output = put(&store, Some(&first), "{\"out\":1}", 0);

        assert_eq!(
            root(&store, event(&store, &first)).unwrap().id.to_string(),
            first
        );
        assert_eq!(
            root(&store, event(&store, &output)).unwrap().id.to_string(),
            output
        );
        let _ = done;
    }

    #[test]
    fn a_retry_of_a_retry_walks_back_to_the_original() {
        let store = rannoch_core::forgr::empty_store();
        let original = put(&store, None, "{\"in\":1}", 0);
        put(&store, Some(&original), "failed", -1);
        let first = put(&store, Some(&original), "failed", 0);
        put(&store, Some(&first), "failed again", -1);
        let second = put(&store, Some(&first), "failed again", 0);

        let found = root(&store, event(&store, &second)).unwrap();

        assert_eq!(found.id.to_string(), original);
        assert_eq!(found.data, "{\"in\":1}");
    }
}
