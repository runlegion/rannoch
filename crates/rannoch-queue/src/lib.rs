//! The queue: when the events crate signals, or when an event comes due, it takes the events at
//! status 0 that are due and have no status event whose source is them, sends a status 1 for
//! each, and hands each to its processor: runs, whose flow has no job step, one after another;
//! jobs, whose flow has a job step, each on a thread of its own. Processors send their own
//! results; the queue sends only the 1s, a retry when a run fails while its chain has attempts
//! left, the hunt's resend of a run a crash dropped, and a -1 saying "no processor" for an event
//! whose `ty` has no flow.

mod queries;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use rannoch_core::{Event, Sender, Status};
use rusqlite::{Connection, Params};
use uuid::Uuid;

/// An event as `_events` holds it: `ty` as its `u16`, data as its JSON text.
pub type Row = Event<u16, String>;

/// A processor: handed the event it runs, that event's root and a sender, it sends its own
/// result about the event it runs, and tells the queue whether the run failed, with the
/// failure's result, so the queue can retry it.
pub type Processor<T, D> =
    Box<dyn Fn(&Row, &Row, &Sender<T, D>) -> Result<(), String> + Send + Sync>;

/// The queue's settings.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    /// How many attempts a chain may have before nothing more is sent; 10 by default.
    pub attempts: u32,
    /// The backoff's first wait; each later wait doubles it.
    pub first_wait: Duration,
    /// How old a 1 with nothing after it must be before the hunt sends its event again; 20
    /// minutes by default.
    pub window: Duration,
    /// How often the hunt runs; one hour by default.
    pub hunt: Duration,
}

impl Settings {
    /// The settings with `first_wait` and the default attempts.
    #[must_use]
    pub fn new(first_wait: Duration) -> Self {
        Self {
            attempts: 10,
            first_wait,
            window: Duration::from_mins(20),
            hunt: Duration::from_hours(1),
        }
    }
}

/// What runs an event: the processor, and whether its flow has a job step.
pub struct Flow<T, D> {
    /// Long-running work: a job runs on a thread of its own and never holds up a run.
    pub job: bool,
    /// The processor.
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
        settings: Settings,
    ) -> Self
    where
        T: TryFrom<u16> + Copy + PartialEq + Send + 'static,
        D: From<String> + Send + 'static,
    {
        Self {
            thread: thread::spawn(move || run(&conn, &signal, &sender, &*flows, settings)),
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
    let mut wait = next_due(conn).unwrap_or(IDLE).min(settings.hunt);
    loop {
        if signal.recv_timeout(wait) == Err(RecvTimeoutError::Disconnected) {
            break;
        }
        jobs.retain(|job| !job.is_finished());
        if Instant::now() >= next_hunt {
            hunt(conn, sender, settings.window);
            next_hunt = Instant::now() + settings.hunt;
        }
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
            let (root, before) = match root(conn, row.clone()) {
                Ok(found) => found,
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
                    attempt(&flow, &row, &root, before, &job_sender, settings);
                }));
            } else if runs.send((flow, row, root, before)).is_err() {
                log::error!("the queue's runs have stopped");
            }
        }
        wait = next_due(conn)
            .unwrap_or(IDLE)
            .min(next_hunt.saturating_duration_since(Instant::now()));
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
    fn a_row_is_old_once_the_window_has_passed_since_it_was_accepted() {
        let now = chrono::Utc::now();
        let at = |ago: i64| {
            let t = now - chrono::TimeDelta::seconds(ago);
            let secs = u64::try_from(t.timestamp()).unwrap();
            Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, secs, 0))
        };
        let window = std::time::Duration::from_secs(20);

        assert!(super::older_than(at(30), window, now));
        assert!(!super::older_than(at(10), window, now));
    }

    #[test]
    fn something_after_a_1_anywhere_in_the_tree_counts() {
        let store = rannoch_core::forgr::empty_store();
        let hung = put(&store, None, "{}", 0);
        let one = put(&store, Some(&hung), "taken", 1);
        let hung_id: Uuid = hung.parse().unwrap();
        let one_id: Uuid = one.parse().unwrap();

        assert!(!super::after(&store, hung_id, one_id).unwrap());

        let failed = put(&store, None, "{}", 0);
        let failed_one = put(&store, Some(&failed), "taken", 1);
        let retry = put(&store, Some(&failed), "failed", 0);
        put(&store, Some(&retry), "taken", 1);
        let failed_id: Uuid = failed.parse().unwrap();

        assert!(super::after(&store, failed_id, failed_one.parse().unwrap()).unwrap());
    }

    #[test]
    fn the_hunt_reads_only_the_1s_whose_event_has_no_result() {
        let store = rannoch_core::forgr::empty_store();
        let hung = put(&store, None, "{}", 0);
        let hung_one = put(&store, Some(&hung), "taken", 1);
        let done = put(&store, None, "{}", 0);
        put(&store, Some(&done), "taken", 1);
        put(&store, Some(&done), "made", 2);
        let failed = put(&store, None, "{}", 0);
        put(&store, Some(&failed), "taken", 1);
        put(&store, Some(&failed), "failed", -1);

        let ones: Vec<String> = super::ones_without_result(&store)
            .unwrap()
            .into_iter()
            .map(|(one, event, _)| format!("{one} {event}"))
            .collect();

        assert_eq!(ones, [format!("{hung_one} {hung}")]);
    }

    #[test]
    fn the_backoff_doubles_and_stops_at_the_attempts_setting() {
        let settings = super::Settings {
            attempts: 3,
            ..super::Settings::new(std::time::Duration::from_millis(100))
        };
        let ms = |n| Some(std::time::Duration::from_millis(n));

        assert_eq!(super::backoff(settings, 0), ms(100));
        assert_eq!(super::backoff(settings, 1), ms(200));
        assert_eq!(super::backoff(settings, 2), None);
    }

    #[test]
    fn an_event_that_is_not_a_retry_is_its_own_root() {
        let store = rannoch_core::forgr::empty_store();
        let first = put(&store, None, "{\"in\":1}", 0);
        let done = put(&store, Some(&first), "made", 2);
        let output = put(&store, Some(&first), "{\"out\":1}", 0);

        assert_eq!(
            root(&store, event(&store, &first))
                .unwrap()
                .0
                .id
                .to_string(),
            first
        );
        assert_eq!(
            root(&store, event(&store, &output))
                .unwrap()
                .0
                .id
                .to_string(),
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

        let (found, before) = root(&store, event(&store, &second)).unwrap();

        assert_eq!(found.id.to_string(), original);
        assert_eq!(found.data, "{\"in\":1}");
        assert_eq!(before, 2);
    }
}
