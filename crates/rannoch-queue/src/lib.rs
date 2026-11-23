#![warn(missing_docs)]
//! The queue: when the events crate signals, or when an event comes due, it takes the events at
//! status 0 that are due and have no status event whose source is them, sends a status 1 for
//! each, and hands each to its processor: runs, whose flow has no job step, one after another;
//! jobs, whose flow has a job step, each on a thread of its own. Processors send their own
//! results; the queue sends only the 1s, a retry when a run fails while its chain has attempts
//! left, the hunt's resend of an event whose 1 has had nothing after it for `window`, and a -1
//! for each failure it meets about an event, whose data names the reason, such as "no processor"
//! for an event whose `ty` has no flow.
//!
//! # Taking an event
//!
//! The queue runs on a thread of its own. It wakes when it starts, on each signal, when the
//! earliest `run_at` among the waiting events comes, and when the hunt is due. For each due event
//! it sends a 1 with the data `"taken"`, then asks [`Flows::flow`] for the event's flow; an event
//! whose `ty` has no flow gets a -1 with the data `"no processor"` and no retry. When rannoch
//! refuses the 1, the event stays at 0 and is not run. Every status the queue sends goes through
//! the app's [`rannoch_core::Sender`], so it is written at its inbox's next hand-over, like any
//! other event, save one: the -1 for an event whose `ty` is not the app's `T`, which
//! `Sender::send` cannot take, the queue writes to `_events` itself.
//!
//! # Retries
//!
//! A [`Processor`] is handed the event it runs and that event's root, the chain's first event:
//! the queue walks back from the event through each source that has a -1, counting the failed
//! attempts as it goes. So a processor whose run fails sends a -1 about the event it ran, then
//! returns `Err` with the failure's result; rannoch-processor's does. While the chain has
//! attempts left, the queue then sends a retry: a new event at 0 whose source is the failed one,
//! whose data is the failure's result, and whose `run_at` is the backoff from now. The backoff
//! is [`Settings::first_wait`] for the first retry and doubles for each one after it. A chain
//! gets [`Settings::attempts`] runs in all, the first included; when the last one fails, the
//! queue sends nothing more.
//!
//! # The hunt
//!
//! The hunt first runs one [`Settings::hunt`] after the queue starts, then every `hunt`. It looks
//! for the 1s whose event has no 2 or -1. For each 1 older than [`Settings::window`] with nothing
//! after it anywhere in its event's tree, it sends a resend: a new event at 0 whose source is the
//! stalled event and whose data is `"hung"`. It leaves every 1 accepted since the queue started,
//! however long its run or job has run or waited: only the queue sends 1s and one process runs
//! over the store, so such a 1 is the running queue's own, its run or job going, waiting its
//! turn, or with its result not yet handed over. The 1s a crash left are from before the start.
//!
//! From a resend the queue walks back to the stalled event, without counting a failed attempt, so
//! the processor is handed the stalled event's root and runs the flow on that root's data.
//!
//! # Failures
//!
//! A failure the queue meets about an event becomes a -1 about that event, whose data names the
//! reason:
//!
//! - a due event, or a stalled one the hunt would resend, whose `ty` is not the app's `T`, as
//!   when the app's enum changes: `ty <ty> is not the app's`;
//! - a run that panics, with no retry: `the run panicked: <message>`;
//! - a failed run whose retry's `run_at` is past what a time can hold, with no retry: `its retry's
//!   wait overflows`;
//! - a run the queue cannot hand over to its runs: `the queue's runs have stopped`.
//!
//! A row rannoch never writes is passed over with a log line at `error`, and no -1: one whose id,
//! source or `run_at` does not parse, a 1 whose id is not a `UUIDv7`, and a retry whose source is
//! not in `_events`. A read that fails in the store is logged at `error` and read again after
//! [`Settings::first_wait`], never binned: the due events, the next `run_at` and a due event's
//! chain; when the hunt cannot read the 1s or an event's tree, it runs again after `first_wait`.
//!
//! # Stopping
//!
//! The queue stops when every sender on its signal channel has dropped. The sender in the append
//! drops when rannoch shuts down, so a signal sender the app keeps, or an `Arc` of the append it
//! holds, keeps the queue running and [`Queue::join`] waiting. After
//! [`rannoch_core::Rannoch::shutdown`], every send fails with `Closed`, the queue's and the
//! processors' alike, and the queue logs each of its own; a result sent then is not written. So
//! the app shuts rannoch down once the results it waits for are written, then joins the queue.
//! [`Queue::join`] returns `Error::Queue` when the queue's thread panicked.
//!
//! # Example
//!
//! Starting rannoch in the order the README gives, against a store in a new directory under the
//! system's temporary directory, then one page sent, run and shut down. In place of
//! rannoch-processor's `Processor`, the `Flows` an app with flow documents passes, this app has
//! a `Flows` of its own: one flow, which sends a 2 for each page it runs.
//!
//! ```
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//! use std::sync::{Arc, mpsc};
//! use std::thread;
//! use std::time::{Duration, Instant};
//!
//! use rannoch_core::{Config, Event, Rannoch, Sender, Status, Storage};
//! use rannoch_events::Events;
//! use rannoch_queue::{Flow, Flows, Queue, Row, Settings};
//! use rannoch_store::{Opened, Store};
//! use rusqlite::Connection;
//! use uuid::Uuid;
//!
//! /// The app's event types, written to `_events` as their `u16`.
//! #[derive(Debug, Clone, Copy, PartialEq)]
//! enum Ty {
//!     Page = 1,
//! }
//!
//! impl From<Ty> for u16 {
//!     fn from(ty: Ty) -> u16 {
//!         ty as u16
//!     }
//! }
//!
//! impl TryFrom<u16> for Ty {
//!     type Error = u16;
//!     fn try_from(n: u16) -> Result<Self, u16> {
//!         match n {
//!             1 => Ok(Ty::Page),
//!             other => Err(other),
//!         }
//!     }
//! }
//!
//! /// The app's flows: one, for pages.
//! struct Pages(Arc<Flow<Ty, String>>);
//!
//! impl Flows<Ty, String> for Pages {
//!     fn flow(&self, ty: u16) -> Option<Arc<Flow<Ty, String>>> {
//!         (ty == u16::from(Ty::Page)).then(|| Arc::clone(&self.0))
//!     }
//! }
//!
//! /// The page flow: a run that sends a 2 about the page it runs.
//! fn pages() -> Pages {
//!     Pages(Arc::new(Flow {
//!         job: false,
//!         run: Box::new(|event: &Row, _root: &Row, sender: &Sender<Ty, String>| {
//!             let done = Event {
//!                 id: Uuid::nil(),
//!                 source: Some(event.id),
//!                 ty: Ty::Page,
//!                 data: "done".to_owned(),
//!                 run_at: None,
//!                 status: Status::Processed,
//!             };
//!             sender.send(done).map(|_| ()).map_err(|e| e.to_string())
//!         }),
//!     }))
//! }
//!
//! /// A connection to the migrated database at `path`.
//! fn ready(path: &Path) -> Result<Connection, Box<dyn Error>> {
//!     match rannoch_store::open(path, &rannoch_core::migrator())? {
//!         Opened::Ready(conn) => Ok(conn),
//!         Opened::Pending(versions) => Err(format!("migrations pending: {versions:?}").into()),
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let dir = std::env::temp_dir().join(Uuid::now_v7().to_string());
//!     fs::create_dir_all(&dir)?;
//!     let path = dir.join("app.db");
//!
//!     // 1. The store, with its pending migrations applied.
//!     let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
//!     if store.pending()? {
//!         store.apply()?;
//!     }
//!
//!     // 2. A connection for the append, one for the queue, and one for the app's reads.
//!     let (for_append, for_queue, reads) = (ready(&path)?, ready(&path)?, ready(&path)?);
//!
//!     // 3. The app's flows.
//!     let flows = Arc::new(pages());
//!
//!     // 4. The channel the append signals the queue on.
//!     let (signal, wake) = mpsc::channel();
//!
//!     // 5. The append, holding the only signal sender, so the queue stops once it drops.
//!     let append = Events::new(for_append, signal);
//!
//!     // 6. Rannoch, holding the store and the append.
//!     let rannoch = Rannoch::<Ty, String>::open(Config {
//!         store: Some(store),
//!         append: Some(Arc::new(append)),
//!         ..Config::default()
//!     })?;
//!
//!     // 7. The queue.
//!     let settings = Settings::new(Duration::from_millis(100));
//!     let queue = Queue::start(for_queue, wake, rannoch.sender(), flows, settings);
//!
//!     // A producer sends a page.
//!     let page = Event {
//!         id: Uuid::nil(),
//!         source: None,
//!         ty: Ty::Page,
//!         data: "/about".to_owned(),
//!         run_at: None,
//!         status: Status::Added,
//!     };
//!     rannoch.sender().send(page)?;
//!
//!     // The page, the queue's 1 and the flow's 2 are each written at a hand-over.
//!     let written = [(Status::Added, 1), (Status::InProgress, 1), (Status::Processed, 1)];
//!     let start = Instant::now();
//!     while rannoch_queue::report(&reads)? != written {
//!         if start.elapsed() > Duration::from_secs(10) {
//!             return Err("the page was not processed".into());
//!         }
//!         thread::sleep(Duration::from_millis(20));
//!     }
//!
//!     // Shutdown drops the append, and with it the signal sender, so the queue stops.
//!     rannoch.shutdown();
//!     queue.join()?;
//!
//!     drop(reads);
//!     fs::remove_dir_all(&dir)?;
//!     Ok(())
//! }
//! ```

mod queries;

use std::collections::HashSet;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use rannoch_core::{Error, Event, Sender, Status};
use rusqlite::{Connection, Params, params};
use uuid::Uuid;

/// An event as `_events` holds it: `ty` as its `u16`, data as its JSON text.
pub type Row = Event<u16, String>;

/// A processor: handed the event it runs, that event's root and a sender, it sends its own
/// result about the event it runs, and tells the queue whether the run failed, with the
/// failure's result, so the queue can retry it. A run that fails sends its -1 before it returns
/// `Err`: the queue counts a chain's attempts by its -1s.
pub type Processor<T, D> =
    Box<dyn Fn(&Row, &Row, &Sender<T, D>) -> Result<(), String> + Send + Sync>;

/// The queue's settings.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    /// How many attempts a chain may have before nothing more is sent; 10 by default.
    pub attempts: u32,
    /// The backoff's first wait; each later wait doubles it.
    pub first_wait: Duration,
    /// How old a 1 with nothing after it must be before the hunt sends a resend for its event,
    /// when the 1 is from before the queue started, as a crash leaves it; 20 minutes by default.
    pub window: Duration,
    /// How often the hunt runs; one hour by default.
    pub hunt: Duration,
}

impl Settings {
    /// The settings with `first_wait`, and the defaults for the rest: 10 attempts, a 20-minute
    /// window and a hunt every hour.
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
    /// Starts the queue over `conn`, woken by `signal`, sending through `sender`, on a thread of
    /// its own. `T` is the app's `ty`, read back from its `u16` in `_events`; `D` is the app's
    /// data, made from text: the queue's own, such as `"taken"`, and a failed run's result for a
    /// retry.
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

    /// Waits until the queue has stopped and every run and job it started has finished. The
    /// queue stops only once every sender on its signal channel has dropped, so a sender still
    /// alive keeps this waiting.
    ///
    /// # Errors
    ///
    /// `Error::Queue` when the queue's thread panicked, as a panic in the app's [`Flows::flow`]
    /// makes it.
    pub fn join(self) -> Result<(), Error> {
        self.thread.join().map_err(|_| Error::Queue)
    }
}

/// How long the queue waits for a signal when nothing it knows of comes due.
const IDLE: Duration = Duration::from_secs(3600);

/// The data of the hunt's resend, by which [`root`] knows it.
const HUNG: &str = "hung";

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
    // Every 1 accepted from this millisecond on is this queue's own, so the hunt leaves it.
    let started = Utc::now().timestamp_millis();
    let (runs, bucket) = mpsc::channel::<(Arc<Flow<T, D>>, Row, Row, u32, T)>();
    let run_sender = sender.clone();
    let runs_thread = thread::spawn(move || {
        for (flow, event, root, before, ty) in bucket {
            attempt(&flow, &event, &root, before, ty, &run_sender, settings);
        }
    });
    let mut jobs: Vec<JoinHandle<()>> = Vec::new();
    let mut taken = HashSet::new();
    let mut next_hunt = Instant::now() + settings.hunt;
    // The first pass takes the events already due when the queue starts.
    let mut wait = Duration::ZERO;
    loop {
        if signal.recv_timeout(wait) == Err(RecvTimeoutError::Disconnected) {
            break;
        }
        jobs.retain(|job| !job.is_finished());
        if Instant::now() >= next_hunt {
            // A hunt the store could not read for runs again after `first_wait`.
            let read_all = hunt(conn, sender, settings.window, started);
            next_hunt = Instant::now() + settings.hunt;
            if !read_all {
                next_hunt = Instant::now() + settings.first_wait;
            }
        }
        let Ok(due) = read(conn, queries::DUE, [now()]) else {
            log::error!("the queue could not read the due events");
            wait = settings.first_wait;
            continue;
        };
        // An event whose 1 is kept has left the due events; until then the queue remembers it.
        taken.retain(|id| due.iter().any(|row| row.id == *id));
        // An event whose chain the store could not read is read again after `first_wait`.
        let mut unread = false;
        for row in due {
            if !taken.insert(row.id) {
                continue;
            }
            let Ok(ty) = T::try_from(row.ty) else {
                not_the_apps(conn, row.id, row.ty);
                continue;
            };
            let (root, before) = match root(conn, row.clone()) {
                Ok(Some(found)) => found,
                Ok(None) => continue,
                Err(err) => {
                    log::error!("the queue could not read the chain of {}: {err}", row.id);
                    taken.remove(&row.id);
                    unread = true;
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
                    attempt(&flow, &row, &root, before, ty, &job_sender, settings);
                }));
            } else if let Err(mpsc::SendError((_, row, ..))) =
                runs.send((flow, row, root, before, ty))
            {
                let why = "the queue's runs have stopped";
                send(sender, row.id, ty, why, Status::Binned);
            }
        }
        wait = next_due(conn, settings.first_wait)
            .unwrap_or(IDLE)
            .min(next_hunt.saturating_duration_since(Instant::now()));
        if unread {
            wait = wait.min(settings.first_wait);
        }
    }
    drop(runs);
    let _ = runs_thread.join();
    for job in jobs {
        let _ = job.join();
    }
}

/// Runs `event`, whose `ty` is the app's `ty`, through its processor. When the run fails and its
/// chain, with `before` failed attempts behind it, has attempts left, sends a retry: status 0, the
/// failed attempt as its source, the failure's result as its data, and a `run_at` after the
/// backoff, which doubles the first wait for each attempt before. A run that panics, and a retry
/// whose `run_at` the backoff puts past what a time can hold, get a -1 saying so, and no retry.
fn attempt<T, D>(
    flow: &Flow<T, D>,
    event: &Row,
    root: &Row,
    before: u32,
    ty: T,
    sender: &Sender<T, D>,
    settings: Settings,
) where
    T: PartialEq,
    D: From<String>,
{
    let failure = match panic::catch_unwind(AssertUnwindSafe(|| (flow.run)(event, root, sender))) {
        Ok(Ok(())) => return,
        Ok(Err(failure)) => failure,
        Err(panicked) => {
            let what = panicked.downcast_ref::<String>().map(String::as_str);
            let what = what.or_else(|| panicked.downcast_ref::<&str>().copied());
            let why = format!("the run panicked: {}", what.unwrap_or_default());
            send(sender, event.id, ty, &why, Status::Binned);
            return;
        }
    };
    let Some(wait) = backoff(settings, before) else {
        return;
    };
    let run_at = chrono::TimeDelta::from_std(wait)
        .ok()
        .and_then(|delay| Utc::now().checked_add_signed(delay));
    let Some(run_at) = run_at else {
        let why = "its retry's wait overflows";
        send(sender, event.id, ty, why, Status::Binned);
        return;
    };
    let retry = Event {
        id: Uuid::nil(),
        source: Some(event.id),
        ty,
        data: D::from(failure),
        run_at: Some(run_at),
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
/// older than `window` and from before `started`, the millisecond the queue started, with nothing
/// after it in the event's tree, as a crash leaves it; such an event whose `ty` is not the app's
/// gets a -1 saying so. Whether it read all it needed: `false` when the store could not read the
/// 1s or an event's tree.
fn hunt<T, D>(conn: &Connection, sender: &Sender<T, D>, window: Duration, started: i64) -> bool
where
    T: TryFrom<u16> + PartialEq,
    D: From<String>,
{
    let ones = match ones_without_result(conn) {
        Ok(ones) => ones,
        Err(err) => {
            log::error!("the hunt could not read the 1s: {err}");
            return false;
        }
    };
    let mut read_all = true;
    for (one, event, ty) in ones {
        let ours = taken_at(one).is_none_or(|at| at.timestamp_millis() >= started);
        if ours || !older_than(one, window, Utc::now()) {
            continue;
        }
        match after(conn, event, one) {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                log::error!("the hunt could not read the tree of {event}: {err}");
                read_all = false;
                continue;
            }
        }
        let Ok(ty) = T::try_from(ty) else {
            not_the_apps(conn, event, ty);
            continue;
        };
        let retry = Event {
            id: Uuid::nil(),
            source: Some(event),
            ty,
            data: D::from(HUNG.to_owned()),
            run_at: None,
            status: Status::Added,
        };
        if sender.send(retry).is_err() {
            log::error!("the hunt could not send {event} again: rannoch is closed");
        }
    }
    read_all
}

/// The 1s whose event has no 2 or -1 yet: the 1's id, its event's id and the event's `ty`. A 1
/// whose id is not a `UUIDv7`, or whose source is not a UUID, rannoch never writes: it is passed
/// over with a log line.
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
        match (one.parse::<Uuid>(), event.parse()) {
            (Ok(one), Ok(event)) if one.get_version_num() == 7 => out.push((one, event, ty)),
            _ => log::error!("the hunt passed over the 1 {one} of {event}: not a UUIDv7 of a UUID"),
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

/// The queue's report: the number of `_events` rows at each status, in status order, over the
/// app's store connection `conn`. Only the statuses that have rows appear.
///
/// # Errors
///
/// The store's error when the count cannot be read.
pub fn report(conn: &Connection) -> rusqlite::Result<Vec<(Status, u64)>> {
    let mut select = conn.prepare_cached(queries::REPORT)?;
    let rows = select.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
    let mut out = Vec::new();
    for row in rows {
        let (status, count) = row?;
        if let (Some(status), Ok(count)) = (status_of(status), u64::try_from(count)) {
            out.push((status, count));
        } else {
            log::error!(
                "the report skipped a row it could not read: status {status}, count {count}"
            );
        }
    }
    Ok(out)
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

/// Writes a -1 about the event `source`, whose `ty` is not the app's, saying so. `Sender::send`
/// takes the app's `T`, so this one -1 cannot go through it: the queue writes it to `_events`
/// itself, as the append writes one.
fn not_the_apps(conn: &Connection, source: Uuid, ty: u16) {
    let why = format!("ty {ty} is not the app's");
    let row = params![Uuid::now_v7().to_string(), source.to_string(), ty, why];
    if let Err(err) = conn.execute(queries::BIN, row) {
        log::error!("the queue could not write a -1 for {source}: {err}");
    }
}

/// The root whose data every flow uses, and how many failed attempts are behind `row` in its
/// chain: back from `row` through each source that has a -1, one failed attempt each, and from
/// each hunt's resend to the event it resends. `None`, logged, when the walk reaches a source not
/// in `_events`, which rannoch never writes.
fn root(conn: &Connection, row: Row) -> rusqlite::Result<Option<(Row, u32)>> {
    let id = row.id;
    let mut at = row;
    let mut before = 0_u32;
    while let Some(source) = at.source {
        let failed: bool = conn.query_row(queries::FAILED, [source.to_string()], |r| r.get(0))?;
        // The append writes the resend's data as a JSON string.
        let resend = at.data.strip_prefix('"').and_then(|d| d.strip_suffix('"')) == Some(HUNG);
        if !failed && !resend {
            break;
        }
        let Some(earlier) = read(conn, queries::EVENT, [source.to_string()])?.pop() else {
            log::error!("the queue passed over {id}: {source} in its chain cannot be found");
            return Ok(None);
        };
        at = earlier;
        before = before.saturating_add(u32::from(failed));
    }
    Ok(Some((at, before)))
}

/// How long until the earliest `run_at` among the waiting events, or `None` when there is none.
/// A `run_at` that is not a time, which rannoch never writes, is passed over with a log line.
/// When the read fails, the queue reads again after `first_wait`.
fn next_due(conn: &Connection, first_wait: Duration) -> Option<Duration> {
    let next = || -> rusqlite::Result<Option<DateTime<Utc>>> {
        let mut select = conn.prepare_cached(queries::NEXT_RUN_AT)?;
        let mut rows = select.query([now()])?;
        while let Some(row) = rows.next()? {
            let (id, at): (String, String) = (row.get(0)?, row.get(1)?);
            match at.parse() {
                Ok(at) => return Ok(Some(at)),
                Err(_) => log::error!("the queue passed over {id}: its run_at {at} is not a time"),
            }
        }
        Ok(None)
    };
    match next() {
        Ok(at) => at.map(|at| (at - Utc::now()).to_std().unwrap_or(Duration::ZERO)),
        Err(err) => {
            log::error!("the queue could not read the next run_at: {err}");
            Some(first_wait)
        }
    }
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
        let one = put(&store, Some(&hung), "\"taken\"", 1);
        let hung_id: Uuid = hung.parse().unwrap();
        let one_id: Uuid = one.parse().unwrap();

        assert!(!super::after(&store, hung_id, one_id).unwrap());

        let failed = put(&store, None, "{}", 0);
        let failed_one = put(&store, Some(&failed), "\"taken\"", 1);
        let retry = put(&store, Some(&failed), "\"failed\"", 0);
        put(&store, Some(&retry), "\"taken\"", 1);
        let failed_id: Uuid = failed.parse().unwrap();

        assert!(super::after(&store, failed_id, failed_one.parse().unwrap()).unwrap());
        // Only a grandchild comes after this 1, so only the walk down the tree finds it.
        let deep = put(&store, None, "{}", 0);
        let child = put(&store, Some(&deep), "{}", 0);
        let deep_one = put(&store, Some(&deep), "\"taken\"", 1);
        put(&store, Some(&child), "\"later\"", 0);
        let deep_id: Uuid = deep.parse().unwrap();
        assert!(super::after(&store, deep_id, deep_one.parse().unwrap()).unwrap());
    }

    #[test]
    fn the_hunt_reads_only_the_1s_whose_event_has_no_result() {
        let store = rannoch_core::forgr::empty_store();
        let hung = put(&store, None, "{}", 0);
        let hung_one = put(&store, Some(&hung), "\"taken\"", 1);
        let done = put(&store, None, "{}", 0);
        put(&store, Some(&done), "\"taken\"", 1);
        put(&store, Some(&done), "\"made\"", 2);
        let failed = put(&store, None, "{}", 0);
        put(&store, Some(&failed), "\"taken\"", 1);
        put(&store, Some(&failed), "\"failed\"", -1);

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
        let done = put(&store, Some(&first), "\"made\"", 2);
        let output = put(&store, Some(&first), "{\"out\":1}", 0);

        assert_eq!(
            root(&store, event(&store, &first))
                .unwrap()
                .unwrap()
                .0
                .id
                .to_string(),
            first
        );
        assert_eq!(
            root(&store, event(&store, &output))
                .unwrap()
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
        put(&store, Some(&original), "\"failed\"", -1);
        let first = put(&store, Some(&original), "\"failed\"", 0);
        put(&store, Some(&first), "\"failed again\"", -1);
        let second = put(&store, Some(&first), "\"failed again\"", 0);

        let (found, before) = root(&store, event(&store, &second)).unwrap().unwrap();

        assert_eq!(found.id.to_string(), original);
        assert_eq!(found.data, "{\"in\":1}");
        assert_eq!(before, 2);
    }
}
