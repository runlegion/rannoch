//! graph toy round 5c, never merged: rannoch-queue's loop, woken by the events the append has
//! just committed instead of a bare signal, so the plain path does not read the due events back
//! out of `_events`. It reads `_events` at start, when a delayed event comes due, in the hunt,
//! and for the root of an event that has a source. Everything below `run` is rannoch-queue's,
//! copied as it is on rannoch main 5fb558f.

mod queries;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use rannoch_core::{Event, Sender, Status};
pub use rannoch_queue::{Flow, Flows, Row, Settings};
use rusqlite::{Connection, Params};
use uuid::Uuid;

/// The running queue; [`Queue::join`] waits for it to stop, which it does when the append's
/// hand-off channel closes.
#[derive(Debug)]
pub struct Queue {
    thread: JoinHandle<()>,
}

impl Queue {
    /// Starts the queue over `conn`, handed each committed batch through `handed`.
    #[must_use]
    pub fn start<T, D>(
        conn: Connection,
        handed: Receiver<Vec<Row>>,
        sender: Sender<T, D>,
        flows: Arc<dyn Flows<T, D>>,
        settings: Settings,
    ) -> Self
    where
        T: TryFrom<u16> + Copy + PartialEq + Send + 'static,
        D: From<String> + Send + 'static,
    {
        Self {
            thread: thread::spawn(move || run(&conn, &handed, &sender, &*flows, settings)),
        }
    }

    /// Waits until the queue has stopped and every run and job it started has finished.
    pub fn join(self) {
        let _ = self.thread.join();
    }
}

/// How long the queue waits when nothing it knows of comes due.
const IDLE: Duration = Duration::from_secs(3600);

fn run<T, D>(
    conn: &Connection,
    handed: &Receiver<Vec<Row>>,
    sender: &Sender<T, D>,
    flows: &dyn Flows<T, D>,
    settings: Settings,
) where
    T: TryFrom<u16> + Copy + PartialEq + Send + 'static,
    D: From<String> + Send + 'static,
{
    let (runs, bucket) = mpsc::channel::<(Arc<Flow<T, D>>, Row, Row, u32)>();
    let run_sender = sender.clone();
    let runs_thread = thread::spawn(move || {
        for (flow, event, root, before) in bucket {
            attempt(&flow, &event, &root, before, &run_sender, settings);
        }
    });
    let mut jobs: Vec<JoinHandle<()>> = Vec::new();
    let mut taken = HashSet::new();
    let mut next_hunt = Instant::now() + settings.hunt;
    // At start, what was due before: the only read of the due events on the plain path.
    let mut due = read(conn, queries::DUE, [now()]).unwrap_or_default();
    let mut deadline: Option<Instant> = next_due(conn).map(|d| Instant::now() + d);
    loop {
        jobs.retain(|job| !job.is_finished());
        for row in due.drain(..) {
            if !taken.insert(row.id) {
                continue;
            }
            let Ok(ty) = T::try_from(row.ty) else {
                log::error!("event {} has a ty the app does not know: {}", row.id, row.ty);
                continue;
            };
            let (root, before) = match root(conn, row.clone()) {
                Ok(found) => found,
                Err(err) => {
                    log::error!("the queue could not find the root of {}: {err}", row.id);
                    taken.remove(&row.id);
                    continue;
                }
            };
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
                    attempt(&flow, &row, &root, before, &job_sender, settings);
                }));
            } else if runs.send((flow, row, root, before)).is_err() {
                log::error!("the queue's runs have stopped");
            }
        }
        if Instant::now() >= next_hunt {
            hunt(conn, sender, settings.window);
            next_hunt = Instant::now() + settings.hunt;
        }
        let until = deadline.map_or(next_hunt, |d| d.min(next_hunt));
        match handed.recv_timeout(until.saturating_duration_since(Instant::now())) {
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    // A delayed event has come due: read the due events, then the next deadline.
                    due = read(conn, queries::DUE, [now()]).unwrap_or_default();
                    deadline = next_due(conn).map(|d| Instant::now() + d);
                }
            }
            Ok(batch) => {
                let mut rows = batch;
                while let Ok(more) = handed.try_recv() {
                    rows.extend(more);
                }
                let now = Utc::now();
                for row in &rows {
                    // A status about an event means its 1 is kept: it has left the due events.
                    if row.status != Status::Added
                        && let Some(source) = row.source
                    {
                        taken.remove(&source);
                    }
                    if row.status == Status::Added
                        && let Some(at) = row.run_at
                        && at > now
                    {
                        let wait = (at - now).to_std().unwrap_or(Duration::ZERO);
                        let at = Instant::now() + wait;
                        deadline = Some(deadline.map_or(at, |d| d.min(at)));
                    }
                }
                due = rows
                    .into_iter()
                    .filter(|r| r.status == Status::Added && r.run_at.is_none_or(|at| at <= now))
                    .collect();
            }
        }
    }
    drop(runs);
    let _ = runs_thread.join();
    for job in jobs {
        let _ = job.join();
    }
}

/// Runs `event` through its processor. When the run fails and its chain, with `before` failed
/// attempts behind it, has attempts left, sends a retry: status 0, the failed attempt as its
/// source, the failure's result as its data, and a `run_at` after the backoff, which doubles the
/// first wait for each attempt before.
fn attempt<T, D>(
    flow: &Flow<T, D>,
    event: &Row,
    root: &Row,
    before: u32,
    sender: &Sender<T, D>,
    settings: Settings,
) where
    T: TryFrom<u16> + PartialEq,
    D: From<String>,
{
    let Err(failure) = (flow.run)(event, root, sender) else {
        return;
    };
    let Some(wait) = backoff(settings, before) else {
        return;
    };
    let Ok(ty) = T::try_from(event.ty) else {
        return;
    };
    let run_at = chrono::TimeDelta::from_std(wait)
        .ok()
        .and_then(|delay| Utc::now().checked_add_signed(delay));
    let retry = Event {
        id: Uuid::nil(),
        source: Some(event.id),
        ty,
        data: D::from(failure),
        run_at,
        status: Status::Added,
    };
    if sender.send(retry).is_err() {
        log::error!(
            "the queue could not send a retry for {}: rannoch is closed",
            event.id
        );
    }
}

/// The wait before retrying a run that failed with `before` failed attempts behind it: the first
/// wait doubled once for each of them, or `None` when the chain has reached the attempts setting.
fn backoff(settings: Settings, before: u32) -> Option<Duration> {
    if before.saturating_add(1) >= settings.attempts {
        return None;
    }
    Some(
        settings
            .first_wait
            .checked_mul(2_u32.saturating_pow(before))
            .unwrap_or(Duration::MAX),
    )
}

/// The hunt: sends again, as a retry with that event as its source, each event whose 1 is
/// older than `window` with nothing after it in the event's tree, as a crash leaves it.
fn hunt<T, D>(conn: &Connection, sender: &Sender<T, D>, window: Duration)
where
    T: TryFrom<u16> + PartialEq,
    D: From<String>,
{
    let ones = match ones_without_result(conn) {
        Ok(ones) => ones,
        Err(err) => {
            log::error!("the hunt could not read the 1s: {err}");
            return;
        }
    };
    for (one, event, ty) in ones {
        if !older_than(one, window, Utc::now()) {
            continue;
        }
        match after(conn, event, one) {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                log::error!("the hunt could not read the tree of {event}: {err}");
                continue;
            }
        }
        let Ok(ty) = T::try_from(ty) else {
            continue;
        };
        let retry = Event {
            id: Uuid::nil(),
            source: Some(event),
            ty,
            data: D::from("hung".to_owned()),
            run_at: None,
            status: Status::Added,
        };
        if sender.send(retry).is_err() {
            log::error!("the hunt could not send {event} again: rannoch is closed");
        }
    }
}

/// The 1s whose event has no 2 or -1 yet: the 1's id, its event's id and the event's `ty`.
fn ones_without_result(conn: &Connection) -> rusqlite::Result<Vec<(Uuid, Uuid, u16)>> {
    let mut select = conn.prepare_cached(queries::ONES)?;
    let rows = select.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, u16>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (one, event, ty) = row?;
        if let (Ok(one), Ok(event)) = (one.parse(), event.parse()) {
            out.push((one, event, ty));
        }
    }
    Ok(out)
}

/// Whether the row `id` was accepted at least `window` before `now`.
fn older_than(id: Uuid, window: Duration, now: DateTime<Utc>) -> bool {
    let Ok(window) = chrono::TimeDelta::from_std(window) else {
        return false;
    };
    taken_at(id).is_some_and(|at| at <= now - window)
}

/// When a row was accepted: its `UUIDv7` id's time.
fn taken_at(id: Uuid) -> Option<DateTime<Utc>> {
    let (secs, nanos) = id.get_timestamp()?.to_unix();
    DateTime::from_timestamp(i64::try_from(secs).ok()?, nanos)
}

/// Whether anything follows the 1 `one` in `event`'s tree.
fn after(conn: &Connection, event: Uuid, one: Uuid) -> rusqlite::Result<bool> {
    let mut select = conn.prepare_cached(queries::TREE)?;
    let ids = select.query_map([event.to_string()], |r| r.get::<_, String>(0))?;
    for id in ids {
        if id?.as_str() > one.to_string().as_str() {
            return Ok(true);
        }
    }
    Ok(false)
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

/// The root whose data every flow uses, and how many failed attempts are behind `row` in its
/// chain: back from `row` through each source that has a -1.
fn root(conn: &Connection, row: Row) -> rusqlite::Result<(Row, u32)> {
    let mut at = row;
    let mut before = 0_u32;
    while let Some(source) = at.source {
        let failed: bool = conn.query_row(queries::FAILED, [source.to_string()], |r| r.get(0))?;
        if !failed {
            break;
        }
        match read(conn, queries::EVENT, [source.to_string()])?.pop() {
            Some(earlier) => {
                at = earlier;
                before = before.saturating_add(1);
            }
            None => break,
        }
    }
    Ok((at, before))
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

