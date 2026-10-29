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

use std::any::Any;
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use rannoch_cloudevents::Fact;
use rannoch_ontology::Ontology;
pub use rannoch_store::State;
use rannoch_store::{Cursor, Error, Event, Graph, check};

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
    stopped: Condvar,
    subscribers: Mutex<Vec<Subscriber>>,
    handler: Mutex<Option<Handler>>,
    /// Makes the next commit panic once it has taken its batch, so a test can see the panic
    /// reported and the batch kept.
    #[cfg(test)]
    panic_beat: std::sync::atomic::AtomicBool,
}

struct Inbox {
    queued: Vec<(Ticket, String)>,
    /// Each producer's latest position since the last commit.
    positions: BTreeMap<String, String>,
    last: u64,
    committed: Option<Ticket>,
    /// The highest place in the log a commit has landed; nothing past it is handed out.
    watermark: Cursor,
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

    /// Takes `graph` and starts a heartbeat that commits the queue every `interval`.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn open(graph: Graph, interval: Duration) -> Result<Queue, Error> {
        let ontology = graph.ontology().clone();
        let watermark = graph.events().committed()?;
        let shared = Arc::new(Shared {
            graph: Mutex::new(graph),
            state: Mutex::new(Inbox {
                queued: Vec::new(),
                positions: BTreeMap::new(),
                last: 0,
                committed: None,
                watermark,
                failed: Vec::new(),
                stop: false,
            }),
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
        let mut state = lock(&self.shared.state);
        Ok(state.hold(body))
    }

    /// Accepts `body` as [`Queue::accept`] does, from `producer`, which has reached `position` in
    /// its source: the position commits in the same transaction as the event.
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
        let mut state = lock(&self.shared.state);
        state
            .positions
            .insert(producer.to_string(), position.into());
        Ok(state.hold(body))
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

    /// Hands out to worker `worker` (its worker file's id) the next committed event, at or below
    /// the committed watermark, and marks it in progress; `None` when there is none.
    ///
    /// # Errors
    /// The graph file cannot be read or written.
    pub fn next(&self, worker: &str) -> Result<Option<Event>, Error> {
        let mut graph = lock(&self.shared.graph);
        let watermark = lock(&self.shared.state).watermark;
        graph.next(worker, watermark)
    }

    /// Closes the event at `cursor`, in progress for `worker`, as done.
    ///
    /// # Errors
    /// [`Error::Refused`] when the event is not in progress for the worker.
    pub fn done(&self, worker: &str, cursor: Cursor) -> Result<(), Error> {
        lock(&self.shared.graph).done(worker, cursor)
    }

    /// Closes the event at `cursor`, in progress for `worker`, as failed with `reason`.
    ///
    /// # Errors
    /// [`Error::Refused`] when the event is not in progress for the worker.
    pub fn failed(&self, worker: &str, cursor: Cursor, reason: &str) -> Result<(), Error> {
        lock(&self.shared.graph).failed(worker, cursor, reason)
    }

    /// Where the committed event at `cursor` stands for `worker`.
    ///
    /// # Errors
    /// The graph file cannot be read.
    pub fn state(&self, worker: &str, cursor: Cursor) -> Result<State, Error> {
        lock(&self.shared.graph).state(worker, cursor)
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
    /// committed event. A subscriber must not call back into the queue, which holds the commit
    /// that is telling it.
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
    /// Holds `body` under the next ticket.
    fn hold(&mut self, body: String) -> Ticket {
        self.last += 1;
        let ticket = Ticket(self.last);
        self.queued.push((ticket, body));
        ticket
    }
}

impl Shared {
    /// Keeps `failure` for the app to take.
    fn failed(&self, failure: Failure) {
        lock(&self.state).failed.push(failure);
    }

    /// Commits what is held with the producers' positions, fills the tables, moves the watermark,
    /// then tells the subscribers. The graph's lock is held throughout, so commits never
    /// interleave.
    fn commit(&self) {
        let mut graph = lock(&self.graph);
        let (batch, positions) = {
            let mut state = lock(&self.state);
            (
                std::mem::take(&mut state.queued),
                std::mem::take(&mut state.positions),
            )
        };
        let Some(&(last, _)) = batch.last() else {
            return;
        };
        // A panic while the batch is written rolls its transaction back; the batch and positions
        // go back to the front of the queue, in order, for the next flush, close or drop.
        let written = catch_unwind(AssertUnwindSafe(|| {
            #[cfg(test)]
            if self
                .panic_beat
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                std::panic::resume_unwind(Box::new("the heartbeat panics"));
            }
            let bodies: Vec<&str> = batch.iter().map(|(_, body)| body.as_str()).collect();
            let places: Vec<(&str, &str)> = positions
                .iter()
                .map(|(producer, position)| (producer.as_str(), position.as_str()))
                .collect();
            graph.commit(&bodies, &places)
        }));
        let written = match written {
            Ok(written) => written,
            Err(panic) => {
                let mut state = lock(&self.state);
                state.queued.splice(0..0, batch);
                for (producer, position) in positions {
                    state.positions.entry(producer).or_insert(position);
                }
                drop(state);
                std::panic::resume_unwind(panic);
            }
        };
        let mut landed = None;
        let mut failed = Vec::new();
        match written {
            Ok(results) => {
                for ((ticket, _), result) in batch.iter().zip(results) {
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
                for (ticket, _) in &batch[..batch.len() - 1] {
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
        if landed.is_some()
            && let Err(error) = graph.fill()
        {
            failed.push(Failure::Fill {
                ticket: last,
                error,
            });
        }
        let watermark = {
            let mut state = lock(&self.state);
            state.committed = Some(last);
            if let Some(cursor) = landed {
                state.watermark = cursor;
            }
            state.failed.extend(failed);
            state.watermark
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
