//! The inbox: accepting `CloudEvents`, committing them about every 20 ms, telling workers what is
//! committed, and never losing track of anything that arrived.
//!
//! An event is checked against the ontology when it is accepted and held in memory; a heartbeat
//! commits everything accepted since the last one in one transaction, with a savepoint per event,
//! then fills the ontology's tables on the queue's own connection, moves the committed watermark
//! and tells the subscribers. Nothing past the watermark is handed out to a worker.
//!
//! What a crash loses: the events accepted since the last heartbeat, and the batch whose commit
//! was in flight. Nothing committed is lost. A producer's position in its source commits in the
//! same transaction as the events it wrote, so a producer that resumes from [`Queue::position`]
//! after a crash sends again exactly what was lost, and nothing twice.
//!
//! Each worker reads with its own cursor, held in memory: taking an event and closing it write
//! nothing. Every worker that changed is saved in one transaction by the heartbeat, a flush or a
//! close, so a worker that resumes after a crash is handed again whatever came after its last
//! saved cursor: delivery is at least once.

use std::any::Any;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use rannoch_cloudevents::Fact;
use rannoch_ontology::Ontology;
pub use rannoch_store::State;
use rannoch_store::{Cursor, Error, Event, Graph, check};

use workers::{CHUNK, Save, Worker};

mod workers;

/// An event's place in its queue. A later event has a greater ticket, so the committed watermark
/// has passed an event once [`Queue::committed`] is at or above its ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket(u64);

/// Something that went wrong in the queue after an event was accepted. Each is reported once: by
/// [`Queue::flush`], [`Queue::close`] or [`Queue::take_failures`], whichever takes it first, or
/// when the queue drops, to the [`Queue::on_failure`] handler.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// An event failed at commit and did not land.
    #[error("an event failed at commit, {ticket:?}: {error}")]
    Event {
        ticket: Ticket,
        #[source]
        error: Error,
    },
    /// The heartbeat panicked, with the panic's text, and stopped. A batch it was writing went
    /// back to the front of the queue; what is queued commits only through a flush, a close or a
    /// drop.
    #[error("the queue's heartbeat panicked: {0}")]
    Heartbeat(String),
    /// A subscriber panicked, with the panic's text, when told of the commit through `ticket`.
    #[error("a subscriber panicked on the commit through {ticket:?}: {message}")]
    Subscriber { ticket: Ticket, message: String },
    /// The commit through `ticket` landed, but the ontology's tables could not be filled from it.
    #[error("the commit through {ticket:?} landed, but its tables were not filled: {error}")]
    Fill {
        ticket: Ticket,
        #[source]
        error: Error,
    },
    /// The workers' cursors could not be saved; they stay in memory and the next group commit
    /// saves them.
    #[error("the workers' cursors were not saved: {0}")]
    Workers(#[source] Error),
}

type Subscriber = Box<dyn FnMut(Cursor) + Send>;
type Handler = Box<dyn FnMut(Failure) + Send>;

/// One process's inbox for a graph, held in memory, and the heartbeat that commits it. The queue
/// owns the graph and its connection; other handles on the same file read what it commits.
pub struct Queue {
    /// The graph's ontology when the queue took it, which an event is checked against at once.
    ontology: Ontology,
    shared: Arc<Shared>,
    heartbeat: Option<JoinHandle<()>>,
}

struct Shared {
    graph: Mutex<Graph>,
    state: Mutex<Inbox>,
    /// Each worker's place in memory, by name.
    workers: Mutex<HashMap<String, Worker>>,
    stopped: Condvar,
    subscribers: Mutex<Vec<Subscriber>>,
    handler: Mutex<Option<Handler>>,
    /// Makes the next commit panic once it has taken its batch, so a test can see the panic
    /// reported and the batch kept.
    #[cfg(test)]
    panic_beat: std::sync::atomic::AtomicBool,
}

/// The producer an event came from and the position in its source it reached.
type Origin = (String, String);

struct Inbox {
    /// Each held event, with the producer and the position in its source it came from.
    queued: Vec<(Ticket, String, Option<Origin>)>,
    last: u64,
    committed: Option<Ticket>,
    /// The highest place in the log a commit has landed; nothing past it is handed out.
    watermark: Cursor,
    /// The place fill has reached.
    filled: Cursor,
    /// Failures the app has not yet taken, oldest first.
    failed: Vec<Failure>,
    stop: bool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Queue {
    /// The heartbeat interval an app takes unless it needs another.
    pub const INTERVAL: Duration = Duration::from_millis(20);

    /// Takes `graph`, fills its tables with whatever the log holds past them (a process that
    /// stopped between a commit and its fill leaves them behind), and starts a heartbeat that
    /// commits the queue every `interval`.
    ///
    /// # Errors
    /// The graph file cannot be read, or the tables cannot be filled from the log.
    pub fn open(mut graph: Graph, interval: Duration) -> Result<Queue, Error> {
        let filled = graph.fill()?;
        let ontology = graph.ontology().clone();
        let watermark = graph.events().committed()?;
        let workers = graph
            .places()?
            .into_iter()
            .map(|(place, last_failed)| {
                let worker = Worker::resumed(&place, last_failed);
                (place.worker, worker)
            })
            .collect();
        let shared = Arc::new(Shared {
            graph: Mutex::new(graph),
            state: Mutex::new(Inbox {
                queued: Vec::new(),
                last: 0,
                committed: None,
                watermark,
                filled,
                failed: Vec::new(),
                stop: false,
            }),
            workers: Mutex::new(workers),
            stopped: Condvar::new(),
            subscribers: Mutex::new(Vec::new()),
            handler: Mutex::new(None),
            #[cfg(test)]
            panic_beat: std::sync::atomic::AtomicBool::new(false),
        });
        let beat = Arc::clone(&shared);
        let heartbeat = std::thread::spawn(move || {
            loop {
                let state = lock(&beat.state);
                let (state, _) = beat
                    .stopped
                    .wait_timeout_while(state, interval, |s| !s.stop)
                    .unwrap_or_else(PoisonError::into_inner);
                if state.stop {
                    return;
                }
                drop(state);
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| beat.commit())) {
                    beat.failed(Failure::Heartbeat(message(&*panic).to_string()));
                    return;
                }
            }
        });
        Ok(Queue {
            ontology,
            shared,
            heartbeat: Some(heartbeat),
        })
    }

    /// Checks `body`, a `CloudEvent` as JSON, against the ontology, holds it in memory and returns
    /// its ticket, without waiting on the store.
    ///
    /// # Errors
    /// [`Error::Refused`] with the reason, and nothing held, when the ontology does not allow the
    /// event (see [`rannoch_store::check`]) or it is a declaration, which goes through
    /// [`Graph::declare`]; [`Error::Encoding`] when the body is not JSON.
    pub fn accept(&self, body: impl Into<String>) -> Result<Ticket, Error> {
        let body = self.checked(body.into())?;
        Ok(lock(&self.shared.state).hold(body, None))
    }

    /// Accepts `body` as [`Queue::accept`] does, from `producer`, which has reached `position` in
    /// its source: the position commits in the same transaction as the event. Should the event
    /// fail at commit, the producer's later events in the same batch are not committed and its
    /// position stays at its last event that landed, so it resumes at the failed event.
    ///
    /// # Errors
    /// As [`Queue::accept`]; a refused event leaves the producer's position as it was.
    pub fn accept_from(
        &self,
        body: impl Into<String>,
        producer: &str,
        position: impl Into<String>,
    ) -> Result<Ticket, Error> {
        let body = self.checked(body.into())?;
        let from = (producer.to_string(), position.into());
        Ok(lock(&self.shared.state).hold(body, Some(from)))
    }

    fn checked(&self, body: String) -> Result<String, Error> {
        if check(&body, &self.ontology)? == Fact::Declared {
            return Err(Error::Refused(
                "a declaration is made with declare, not accepted".to_string(),
            ));
        }
        Ok(body)
    }

    /// `producer`'s position in its source as last committed, or `None` before its first.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn position(&self, producer: &str) -> Result<Option<String>, Error> {
        lock(&self.shared.graph).position(producer)
    }

    /// Hands out to worker `worker` its next committed event after its cursor, at or below the
    /// committed watermark, and marks it in progress; `None` when there is none. A worker joins at
    /// the start of the log on its first call. Nothing is written: the worker reads ahead from the
    /// log a chunk at a time, and its place reaches disk only in a group commit.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn next(&self, worker: &str) -> Result<Option<Event>, Error> {
        let from = {
            let mut workers = lock(&self.shared.workers);
            let held = match workers.get_mut(worker) {
                Some(held) => held,
                None => workers
                    .entry(worker.to_string())
                    .or_insert_with(|| Worker::joining(Cursor::START)),
            };
            if let Some(event) = held.take() {
                return Ok(Some(event));
            }
            held.handed()
        };
        let watermark = lock(&self.shared.state).watermark;
        if from >= watermark {
            return Ok(None);
        }
        let events = lock(&self.shared.graph)
            .events()
            .chunk(from, watermark, CHUNK)?;
        let mut workers = lock(&self.shared.workers);
        let Some(held) = workers.get_mut(worker) else {
            return Ok(None);
        };
        held.read(from, events);
        Ok(held.take())
    }

    /// Where `worker` stands: its cursor when it has a place, otherwise it joins at the committed
    /// watermark, so everything committed before now is never handed to it. A worker that reads
    /// the tables first has no need of the log behind them.
    #[must_use]
    pub fn join(&self, worker: &str) -> Cursor {
        let watermark = lock(&self.shared.state).watermark;
        lock(&self.shared.workers)
            .entry(worker.to_string())
            .or_insert_with(|| Worker::joining(watermark))
            .cursor()
    }

    /// Closes the event at `cursor`, in progress for `worker`, as done, in memory.
    ///
    /// # Errors
    /// [`Error::Refused`] when the event is not in progress for the worker.
    pub fn done(&self, worker: &str, cursor: Cursor) -> Result<(), Error> {
        self.close_as(worker, cursor, None, None)
    }

    /// Closes the event at `cursor` as [`Queue::done`] does, and keeps `kept`, an opaque string,
    /// with the worker's cursor once the cursor reaches the event. It is saved in the same group
    /// commit as the cursor and read back with [`Queue::kept`], after a restart too.
    ///
    /// # Errors
    /// As [`Queue::done`].
    pub fn done_keeping(
        &self,
        worker: &str,
        cursor: Cursor,
        kept: impl Into<String>,
    ) -> Result<(), Error> {
        self.close_as(worker, cursor, None, Some(kept.into()))
    }

    /// Closes the event at `cursor`, in progress for `worker`, as failed with `reason`, in
    /// memory; the failure is recorded with its reason in the next group commit.
    ///
    /// # Errors
    /// [`Error::Refused`] when the event is not in progress for the worker.
    pub fn failed(&self, worker: &str, cursor: Cursor, reason: &str) -> Result<(), Error> {
        self.close_as(worker, cursor, Some(reason), None)
    }

    fn close_as(
        &self,
        worker: &str,
        cursor: Cursor,
        reason: Option<&str>,
        kept: Option<String>,
    ) -> Result<(), Error> {
        match lock(&self.shared.workers).get_mut(worker) {
            Some(held) => held.close(worker, cursor, reason, kept),
            None => Err(workers::not_in_progress(worker, cursor)),
        }
    }

    /// The state `worker` keeps with its cursor ([`Queue::done_keeping`]), or `None` when it keeps
    /// none.
    #[must_use]
    pub fn kept(&self, worker: &str) -> Option<String> {
        lock(&self.shared.workers)
            .get(worker)
            .and_then(|held| held.kept().map(str::to_string))
    }

    /// Where the committed event at `cursor` stands for `worker`, closes not yet saved included.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn state(&self, worker: &str, cursor: Cursor) -> Result<State, Error> {
        let known = lock(&self.shared.workers)
            .get(worker)
            .map(|held| held.state(cursor));
        match known {
            Some(Some(state)) => Ok(state),
            _ => lock(&self.shared.graph).state(worker, cursor),
        }
    }

    /// How many committed events `worker` has not yet closed: its lag behind the newest, read
    /// from memory and the log. A worker not yet seen has every committed event ahead of it.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn lag(&self, worker: &str) -> Result<u64, Error> {
        let cursor = lock(&self.shared.workers)
            .get(worker)
            .map_or(Cursor::START, Worker::cursor);
        let watermark = lock(&self.shared.state).watermark;
        lock(&self.shared.graph).events().count(cursor, watermark)
    }

    /// The slowest worker's cursor, the lowest of them, or `None` with no worker. A producer or a
    /// ring reads it to apply back-pressure; the queue itself never blocks or refuses `accept`.
    #[must_use]
    pub fn slowest(&self) -> Option<Cursor> {
        lock(&self.shared.workers)
            .values()
            .map(Worker::cursor)
            .min()
    }

    /// The processed mark: the lowest point every worker has finished, fill included, closes not
    /// yet saved included, and never past the committed watermark. With no worker it is the
    /// lower of fill's place and the watermark.
    ///
    /// # Errors
    /// None today; the signature keeps room for a read.
    pub fn processed(&self) -> Result<Cursor, Error> {
        let (watermark, filled) = {
            let state = lock(&self.shared.state);
            (state.watermark, state.filled)
        };
        let mark = self.slowest().map_or(filled, |slowest| slowest.min(filled));
        Ok(mark.min(watermark))
    }

    /// Removes `worker`, its place, the state it keeps and its failures, and releases the
    /// processed mark it held ([`Graph::remove_worker`]).
    ///
    /// # Errors
    /// The graph file cannot be written.
    pub fn remove_worker(&self, worker: &str) -> Result<(), Error> {
        let mut graph = lock(&self.shared.graph);
        lock(&self.shared.workers).remove(worker);
        graph.remove_worker(worker)
    }

    /// The committed watermark: the highest ticket the heartbeat or a flush has taken through a
    /// commit, or `None` before the first. An event that failed at commit is passed too, and is
    /// not lost: the queue keeps its [`Failure`] until it is reported.
    #[must_use]
    pub fn committed(&self) -> Option<Ticket> {
        lock(&self.shared.state).committed
    }

    /// Commits everything held now, without waiting for the heartbeat, and returns the committed
    /// watermark, which has passed every event accepted before the call, with every failure the
    /// app has not yet taken, this flush's and the heartbeat's. The queue keeps none of them after.
    #[must_use]
    pub fn flush(&self) -> (Option<Ticket>, Vec<Failure>) {
        self.shared.commit();
        let mut state = lock(&self.shared.state);
        (state.committed, std::mem::take(&mut state.failed))
    }

    /// Commits everything held now, without waiting for the heartbeat, and leaves every failure
    /// for the app to take: for a worker whose answer someone is waiting on.
    pub fn commit(&self) {
        self.shared.commit();
    }

    /// Takes the failures the app has not yet taken, oldest first; the queue keeps none of them
    /// after.
    #[must_use]
    pub fn take_failures(&self) -> Vec<Failure> {
        std::mem::take(&mut lock(&self.shared.state).failed)
    }

    /// Registers `handler` for the failures still untaken when the queue is dropped, which cannot
    /// return them; it receives each one. Without a handler, a drop writes them to stderr, the
    /// last resort. A later call replaces the handler. [`Queue::close`] returns them instead.
    pub fn on_failure(&self, handler: impl FnMut(Failure) + Send + 'static) {
        *lock(&self.shared.handler) = Some(Box::new(handler));
    }

    /// Calls `subscriber` after each commit with the committed watermark, the cursor of the last
    /// committed event. The commit has released the graph when it tells subscribers, so a
    /// subscriber may call back into the queue to take its next event; it must not subscribe,
    /// flush or close from inside the call.
    pub fn subscribe(&self, subscriber: impl FnMut(Cursor) + Send + 'static) {
        lock(&self.shared.subscribers).push(Box::new(subscriber));
    }

    /// Stops the heartbeat and commits everything held, as dropping the queue does, and returns
    /// every failure the app has not yet taken. A process that exits without unwinding
    /// (`std::process::exit`, `panic = "abort"`) runs no drop, so it closes the queue first.
    #[must_use]
    pub fn close(mut self) -> Vec<Failure> {
        self.stop();
        self.take_failures()
    }

    /// Stops the heartbeat, then commits what is still held. A heartbeat that panicked is kept as
    /// a failure. Stopping again does nothing more.
    fn stop(&mut self) {
        lock(&self.shared.state).stop = true;
        self.shared.stopped.notify_all();
        if let Some(heartbeat) = self.heartbeat.take()
            && let Err(panic) = heartbeat.join()
        {
            self.shared
                .failed(Failure::Heartbeat(message(&*panic).to_string()));
        }
        self.shared.commit();
    }
}

impl Inbox {
    /// Holds `body`, from the producer and position `from`, under the next ticket.
    fn hold(&mut self, body: String, from: Option<Origin>) -> Ticket {
        self.last += 1;
        let ticket = Ticket(self.last);
        self.queued.push((ticket, body, from));
        ticket
    }
}

impl Shared {
    /// Keeps `failure` for the app to take.
    fn failed(&self, failure: Failure) {
        lock(&self.state).failed.push(failure);
    }

    /// Commits what is held with the producers' positions, fills the tables, moves the watermark,
    /// saves every worker that changed in one transaction, then releases the graph and tells the
    /// subscribers. Commits never interleave; a subscriber that calls back into the queue finds
    /// the graph free.
    fn commit(&self) {
        let mut graph = lock(&self.graph);
        let told = self.write(&mut graph);
        self.save(&mut graph);
        drop(graph);
        let Some((last, watermark)) = told else {
            return;
        };
        // A subscriber that panics is reported and the others are still told; the heartbeat lives
        // on, and a drop that is already unwinding does not abort.
        for subscriber in lock(&self.subscribers).iter_mut() {
            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| subscriber(watermark))) {
                self.failed(Failure::Subscriber {
                    ticket: last,
                    message: message(&*panic).to_string(),
                });
            }
        }
    }

    /// Saves every worker that changed since the last group commit, in one transaction. A save
    /// that fails is reported, and the next group commit saves those workers again.
    fn save(&self, graph: &mut Graph) {
        let saves: Vec<Save> = lock(&self.workers)
            .iter_mut()
            .filter_map(|(name, held)| held.save(name))
            .collect();
        if saves.is_empty() {
            return;
        }
        let places: Vec<_> = saves.iter().map(|(place, _)| place.clone()).collect();
        let closes: Vec<(&str, Cursor, Option<&str>)> = saves
            .iter()
            .flat_map(|(place, closes)| {
                closes
                    .iter()
                    .map(|(cursor, reason)| (place.worker.as_str(), *cursor, reason.as_deref()))
            })
            .collect();
        let written = graph.save_places(&places, &closes);
        let mut workers = lock(&self.workers);
        for save in &saves {
            if let Some(held) = workers.get_mut(&save.0.worker) {
                match written {
                    Ok(()) => held.saved(save),
                    Err(_) => held.unsaved(),
                }
            }
        }
        drop(workers);
        if let Err(error) = written {
            self.failed(Failure::Workers(error));
        }
    }

    /// Commits what is held with the producers' positions, fills the tables and moves the
    /// watermark; returns the last ticket and the watermark to tell subscribers, or `None` when
    /// nothing was held.
    fn write(&self, graph: &mut Graph) -> Option<(Ticket, Cursor)> {
        let batch = std::mem::take(&mut lock(&self.state).queued);
        let &(last, ..) = batch.last()?;
        // A panic while the batch is written rolls its transaction back; the batch goes back to the front of the queue, in order, for the next flush, close or drop.
        let written = catch_unwind(AssertUnwindSafe(|| {
            #[cfg(test)]
            if self
                .panic_beat
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                std::panic::resume_unwind(Box::new("the heartbeat panics"));
            }
            let bodies: Vec<&str> = batch.iter().map(|(_, body, _)| body.as_str()).collect();
            let places: Vec<(usize, &str, &str)> = batch
                .iter()
                .enumerate()
                .filter_map(|(index, (_, _, from))| {
                    from.as_ref()
                        .map(|(producer, position)| (index, producer.as_str(), position.as_str()))
                })
                .collect();
            graph.commit(&bodies, &places)
        }));
        let written = match written {
            Ok(written) => written,
            Err(panic) => {
                lock(&self.state).queued.splice(0..0, batch);
                std::panic::resume_unwind(panic);
            }
        };
        let mut landed = None;
        let mut failed = Vec::new();
        match written {
            Ok(results) => {
                for ((ticket, ..), result) in batch.iter().zip(results) {
                    match result {
                        Ok(event) => landed = Some(event.cursor),
                        Err(error) => failed.push(Failure::Event {
                            ticket: *ticket,
                            error,
                        }),
                    }
                }
            }
            // The batch's transaction failed, so none of it landed: every ticket fails, and the
            // last keeps the cause itself.
            Err(cause) => {
                let text = format!("the batch's transaction failed: {cause}");
                for (ticket, ..) in &batch[..batch.len() - 1] {
                    failed.push(Failure::Event {
                        ticket: *ticket,
                        error: Error::Refused(text.clone()),
                    });
                }
                failed.push(Failure::Event {
                    ticket: last,
                    error: cause,
                });
            }
        }
        let mut filled = None;
        if landed.is_some() {
            // Fill logs a closed event for each link it closes, after the batch; the watermark
            // takes them in, so they are handed out without waiting for a later commit.
            match graph
                .fill()
                .and_then(|to| Ok((to, graph.events().committed()?)))
            {
                Ok((to, high)) => {
                    filled = Some(to);
                    landed = Some(high);
                }
                Err(error) => failed.push(Failure::Fill {
                    ticket: last,
                    error,
                }),
            }
        }
        let watermark = {
            let mut state = lock(&self.state);
            state.committed = Some(last);
            if let Some(cursor) = landed {
                state.watermark = cursor;
            }
            if let Some(to) = filled {
                state.filled = to;
            }
            state.failed.extend(failed);
            state.watermark
        };
        Some((last, watermark))
    }
}

/// The text a panic carried, when it carried text.
fn message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("(no message)")
}

impl Drop for Queue {
    /// Stops the heartbeat, then commits what is still held, so a process that returns or panics
    /// with unwinding loses no event. Every failure the app has not taken goes to the failure
    /// handler, or to stderr when none is registered.
    fn drop(&mut self) {
        self.stop();
        let failed = self.take_failures();
        let mut handler = lock(&self.shared.handler);
        for failure in failed {
            match handler.as_mut() {
                Some(handler) => {
                    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| handler(failure))) {
                        eprintln!(
                            "rannoch: the queue's failure handler panicked: {}",
                            message(&*panic)
                        );
                    }
                }
                None => eprintln!("rannoch: {failure}"),
            }
        }
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock(&self.shared.state);
        f.debug_struct("Queue")
            .field("committed", &state.committed)
            .field("watermark", &state.watermark)
            .field("failed", &state.failed)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
