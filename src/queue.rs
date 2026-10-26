//! The in-memory write queue: a write is appended and returns at once, and a heartbeat commits
//! everything queued, advances the committed watermark and then fires the subscribers, so a
//! subscriber never hears of an event that is not committed.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::events::Cursor;
use crate::open::{Graph, Ontology};
use crate::write::{Error, Write, WriteError, check_declared};

/// A write's place in its queue. A later write has a greater ticket, so the committed watermark
/// has passed a write once [`Queue::committed`] is at or above its ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket(u64);

/// Something that went wrong in the queue after a write returned. Each is reported once: by
/// [`Queue::flush`], [`Queue::close`] or [`Queue::take_failures`], whichever takes it first, or
/// when the queue drops, to the [`Queue::on_failure`] handler.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// A write failed at commit and did not land.
    #[error("a write failed at commit, {ticket:?}: {error}")]
    Write {
        ticket: Ticket,
        #[source]
        error: WriteError,
    },
    /// The heartbeat panicked, with the panic's text, and stopped. A batch it was writing went
    /// back to the front of the queue; what is queued commits only through a flush, a close or a
    /// drop.
    #[error("the queue's heartbeat panicked: {0}")]
    Heartbeat(String),
    /// A subscriber panicked, with the panic's text, when fired for the commit through `ticket`.
    #[error("a subscriber panicked on the commit through {ticket:?}: {message}")]
    Subscriber { ticket: Ticket, message: String },
    /// The commit through `ticket` landed, but its subscribers did not fire: the committed
    /// cursor could not be read.
    #[error(
        "the commit through {ticket:?} landed, but its subscribers did not fire: the committed \
         cursor could not be read: {error}"
    )]
    Unfired {
        ticket: Ticket,
        #[source]
        error: Error,
    },
}

type Subscriber = Box<dyn FnMut(Cursor) + Send>;
type Handler = Box<dyn FnMut(Failure) + Send>;

/// One process's queue of writes to a graph, held in memory only, and the heartbeat that
/// commits it, each batch as one transaction with a savepoint per write. The queue owns the
/// graph, and other handles on the same file read what it commits.
pub struct Queue {
    /// The graph's ontology when the queue took it, which a write is checked against at once.
    ontology: Ontology,
    shared: Arc<Shared>,
    heartbeat: Option<JoinHandle<()>>,
}

struct Shared {
    graph: Mutex<Graph>,
    state: Mutex<State>,
    stopped: Condvar,
    subscribers: Mutex<Vec<Subscriber>>,
    handler: Mutex<Option<Handler>>,
    /// Makes the next commit panic once it has taken its batch, so a test can see the panic
    /// reported and the batch kept.
    #[cfg(test)]
    panic_beat: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct State {
    queued: Vec<(Ticket, Write)>,
    last: u64,
    committed: Option<Ticket>,
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
    #[must_use]
    pub fn open(graph: Graph, interval: Duration) -> Queue {
        let ontology = graph.ontology().clone();
        let shared = Arc::new(Shared {
            graph: Mutex::new(graph),
            state: Mutex::new(State::default()),
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
                let beaten = catch_unwind(AssertUnwindSafe(|| {
                    beat.commit();
                }));
                if let Err(panic) = beaten {
                    beat.failed(Failure::Heartbeat(message(&*panic).to_string()));
                    return;
                }
            }
        });
        Queue {
            ontology,
            shared,
            heartbeat: Some(heartbeat),
        }
    }

    /// Checks `write` against the ontology without reading the store, appends it to the queue
    /// and returns its ticket, without waiting on the store. Checks that need the store run when
    /// it commits.
    ///
    /// # Errors
    /// [`Error::Refused`], and nothing queued, when a unit's class is not declared, or its node
    /// does not carry exactly the properties its class declares, each of its declared type.
    pub fn write(&self, write: Write) -> Result<Ticket, Error> {
        check_declared(&self.ontology, &write)?;
        let mut state = lock(&self.shared.state);
        state.last += 1;
        let ticket = Ticket(state.last);
        state.queued.push((ticket, write));
        Ok(ticket)
    }

    /// The committed watermark: the highest ticket the heartbeat or a flush has taken through a
    /// commit, or `None` before the first. A write that failed at commit is passed too, and is
    /// not lost: the queue keeps its [`Failure`] until it is reported.
    #[must_use]
    pub fn committed(&self) -> Option<Ticket> {
        lock(&self.shared.state).committed
    }

    /// Commits everything queued now, without waiting for the heartbeat, and returns the
    /// committed watermark, which has passed every write accepted before the call, with every
    /// failure the app has not yet taken, this flush's and the heartbeat's. A write that failed
    /// comes with every later write in its batch that linked from a node it would have brought.
    /// The queue keeps none of them after.
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

    /// Calls `subscriber` after each commit, with the cursor of the last committed event; a
    /// follower reads [`Graph::events`] since its own cursor up to it. A subscriber must not
    /// call back into the queue's [`Queue::flush`], which waits on the commit firing it.
    pub fn subscribe(&self, subscriber: impl FnMut(Cursor) + Send + 'static) {
        lock(&self.shared.subscribers).push(Box::new(subscriber));
    }

    /// Stops the heartbeat and commits everything queued, as dropping the queue does, and returns
    /// every failure the app has not yet taken, as [`Queue::flush`] does.
    /// A process that exits without unwinding (`std::process::exit`, `panic = "abort"`) runs no
    /// drop, so it closes the queue first.
    #[must_use]
    pub fn close(mut self) -> Vec<Failure> {
        self.stop();
        self.take_failures()
    }

    /// Stops the heartbeat, then commits what is still queued. A heartbeat that panicked is kept
    /// as a failure. Stopping again does nothing more.
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

impl Shared {
    /// Keeps `failure` for the app to take.
    fn failed(&self, failure: Failure) {
        lock(&self.state).failed.push(failure);
    }

    /// Commits what is queued, then advances the watermark, then fires the subscribers. The
    /// graph's lock is held throughout, so commits and their firings never interleave.
    fn commit(&self) {
        let mut graph = lock(&self.graph);
        let batch = std::mem::take(&mut lock(&self.state).queued);
        let Some(&(last, _)) = batch.last() else {
            return;
        };
        // A panic while the batch is written rolls its transaction back; the batch goes back to
        // the front of the queue, in order, for the next flush, close or drop to commit.
        let written = catch_unwind(AssertUnwindSafe(|| {
            #[cfg(test)]
            if self
                .panic_beat
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                std::panic::resume_unwind(Box::new("the heartbeat panics"));
            }
            graph.write_batch(batch.iter().map(|(_, write)| write))
        }));
        let written = match written {
            Ok(written) => written,
            Err(panic) => {
                lock(&self.state).queued.splice(0..0, batch);
                std::panic::resume_unwind(panic);
            }
        };
        // A write that fails at commit is kept as a failure, not dropped, until it is reported.
        let failed: Vec<(Ticket, Error)> = match written {
            Ok(results) => batch
                .iter()
                .zip(results)
                .filter_map(|((ticket, _), result)| result.err().map(|cause| (*ticket, cause)))
                .collect(),
            // The batch's transaction failed, so none of it landed: every ticket fails, and
            // the last keeps the cause itself.
            Err(cause) => {
                let text = format!("the batch's transaction failed: {cause}");
                let mut failed: Vec<(Ticket, Error)> = batch
                    .iter()
                    .take(batch.len() - 1)
                    .map(|(ticket, _)| (*ticket, batch_failed(&text)))
                    .collect();
                failed.push((last, cause));
                failed
            }
        };
        let failed = failed.into_iter().map(|(ticket, cause)| Failure::Write {
            ticket,
            error: WriteError {
                events: Vec::new(),
                cause,
            },
        });
        {
            let mut state = lock(&self.state);
            state.committed = Some(last);
            state.failed.extend(failed);
        }
        let cursor = match graph.last_cursor() {
            Ok(cursor) => cursor,
            Err(error) => {
                self.failed(Failure::Unfired {
                    ticket: last,
                    error,
                });
                return;
            }
        };
        // A subscriber that panics is reported and the others still fire; the heartbeat lives on,
        // and a drop that is already unwinding does not abort.
        for subscriber in lock(&self.subscribers).iter_mut() {
            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| subscriber(cursor))) {
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

/// A storage failure carrying `text`, for each write of a batch whose transaction failed.
fn batch_failed(text: &str) -> Error {
    Error::Storage(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ABORT),
        Some(text.to_string()),
    ))
}

impl Drop for Queue {
    /// Stops the heartbeat, then commits what is still queued, so a process that returns or
    /// panics with unwinding loses no write. Every failure the app has not taken goes to the
    /// failure handler, or to stderr when none is registered.
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
            .field("failed", &state.failed)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{Failure, JoinHandle, Queue, Ticket};
    use crate::{Class, Date, Declaration, Error, Unit, ValidTime, ValueType, Write, open};

    #[test]
    fn a_batch_that_fails_whole_fails_every_ticket() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        let mut g = open(&path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("item", ["v"])))?;
        rusqlite::Connection::open(&path)?.execute_batch(
            "CREATE TRIGGER bad_rolls_back BEFORE INSERT ON \"class:item\" \
             WHEN NEW.key = 'bad' BEGIN SELECT RAISE(ROLLBACK, 'database or disk is full'); END;",
        )?;
        let queue = Queue::open(g, Duration::from_secs(3_600));
        let mut tickets = Vec::new();
        for key in ["i1", "bad", "i3"] {
            let unit = Unit::new(
                "item",
                key,
                [("v", key)],
                ValidTime::On(Date::day(2026, 1, 1)),
            );
            tickets.push(queue.write(Write::new("app").unit(unit))?);
        }
        let (_, failed) = queue.flush();
        assert_eq!(written(failed), tickets);
        assert!(
            queue.take_failures().is_empty(),
            "a failure was returned twice"
        );
        Ok(())
    }

    /// The tickets of `failures`, each a write that failed at commit.
    fn written(failures: Vec<Failure>) -> Vec<Ticket> {
        failures
            .into_iter()
            .map(|failure| match failure {
                Failure::Write { ticket, .. } => ticket,
                other => panic!("expected a failed write, got {other:?}"),
            })
            .collect()
    }

    /// Waits up to five seconds for `done`.
    fn wait_for(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "timed out");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Takes the queue's failures until there is at least one.
    fn taken(queue: &Queue) -> Vec<Failure> {
        let mut taken = Vec::new();
        wait_for(|| {
            taken.extend(queue.take_failures());
            !taken.is_empty()
        });
        taken
    }

    /// Registers a failure handler on `queue` and returns what it receives.
    fn collected(queue: &Queue) -> Arc<Mutex<Vec<Failure>>> {
        let handled = Arc::new(Mutex::new(Vec::new()));
        let into = Arc::clone(&handled);
        queue.on_failure(move |failure| super::lock(&into).push(failure));
        handled
    }

    /// Makes the queue's next commit panic once it has taken its batch.
    fn panic_next_beat(queue: &Queue) {
        queue
            .shared
            .panic_beat
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// A graph with class `item` and a queue on it beating every [`Queue::INTERVAL`].
    fn beating(dir: &tempfile::TempDir) -> Result<Queue, Box<dyn std::error::Error>> {
        let mut g = open(dir.path().join("graph.rannoch"), Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("item", ["v"])))?;
        Ok(Queue::open(g, Queue::INTERVAL))
    }

    #[test]
    fn a_subscriber_panic_is_taken() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = beating(&dir)?;
        queue.subscribe(|_| panic!("a subscriber panics"));
        let ticket = queue.write(item("i1"))?;
        let taken = taken(&queue);
        assert!(
            matches!(&taken[..], [Failure::Subscriber { ticket: t, message }]
                if *t == ticket && message == "a subscriber panics"),
            "{taken:?}"
        );
        Ok(())
    }

    #[test]
    fn a_subscriber_panic_reaches_the_handler() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = beating(&dir)?;
        let handled = collected(&queue);
        queue.subscribe(|_| panic!("a subscriber panics"));
        queue.write(item("i1"))?;
        wait_for(|| !super::lock(&queue.shared.state).failed.is_empty());
        drop(queue);
        let handled = super::lock(&handled);
        assert!(
            matches!(&handled[..], [Failure::Subscriber { .. }]),
            "{handled:?}"
        );
        Ok(())
    }

    #[test]
    fn a_heartbeat_panic_is_taken() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = beating(&dir)?;
        panic_next_beat(&queue);
        queue.write(item("i1"))?;
        let taken = taken(&queue);
        assert!(
            matches!(&taken[..], [Failure::Heartbeat(message)] if message == "the heartbeat panics"),
            "{taken:?}"
        );
        Ok(())
    }

    #[test]
    fn a_batch_taken_by_a_panicking_heartbeat_is_committed_by_the_next_flush()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = beating(&dir)?;
        panic_next_beat(&queue);
        queue.write(item("i1"))?;
        let ticket = queue.write(item("i2"))?;
        let taken = taken(&queue);
        assert!(matches!(&taken[..], [Failure::Heartbeat(_)]), "{taken:?}");
        assert_eq!(queue.committed(), None);
        let (committed, failed) = queue.flush();
        assert_eq!(committed, Some(ticket));
        assert!(failed.is_empty(), "{failed:?}");
        let g = open(dir.path().join("graph.rannoch"), Some(&crate::TEST_KEY))?;
        for key in ["i1", "i2"] {
            assert!(g.find("item", key)?.is_some(), "{key} is not committed");
        }
        Ok(())
    }

    #[test]
    fn a_heartbeat_panic_reaches_the_handler() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = beating(&dir)?;
        let handled = collected(&queue);
        panic_next_beat(&queue);
        queue.write(item("i1"))?;
        wait_for(|| {
            queue
                .heartbeat
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
        });
        drop(queue);
        let handled = super::lock(&handled);
        assert!(
            matches!(&handled[..], [Failure::Heartbeat(_)]),
            "{handled:?}"
        );
        Ok(())
    }

    /// A graph with class `item`, whose node `bad` fails at commit, and a queue on it.
    fn queue_failing_bad(dir: &tempfile::TempDir) -> Result<Queue, Box<dyn std::error::Error>> {
        let path = dir.path().join("graph.rannoch");
        let mut g = open(&path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("item", ["v"])))?;
        rusqlite::Connection::open(&path)?.execute_batch(
            "CREATE TRIGGER bad_fails BEFORE INSERT ON \"class:item\" \
             WHEN NEW.key = 'bad' BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END;",
        )?;
        Ok(Queue::open(g, Duration::from_secs(3_600)))
    }

    fn item(key: &str) -> Write {
        Write::new("app").unit(Unit::new(
            "item",
            key,
            [("v", key)],
            ValidTime::On(Date::day(2026, 1, 1)),
        ))
    }

    #[test]
    fn a_drop_hands_untaken_failures_to_the_handler() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = queue_failing_bad(&dir)?;
        let handled = collected(&queue);
        queue.write(item("i1"))?;
        let bad = queue.write(item("bad"))?;
        drop(queue);
        assert_eq!(written(std::mem::take(&mut *super::lock(&handled))), [bad]);
        Ok(())
    }

    /// Drops a queue holding a write that fails at commit, with no handler registered: run only
    /// by [`a_drop_without_a_handler_writes_failures_to_stderr`], in a fresh process.
    #[test]
    #[ignore = "run in its own process by a_drop_without_a_handler_writes_failures_to_stderr"]
    fn drop_a_failing_write() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = queue_failing_bad(&dir)?;
        queue.write(item("bad"))?;
        drop(queue);
        Ok(())
    }

    #[test]
    fn a_drop_without_a_handler_writes_failures_to_stderr() -> Result<(), Box<dyn std::error::Error>>
    {
        let out = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "queue::tests::drop_a_failing_write",
                "--ignored",
                "--nocapture",
            ])
            .output()?;
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "{stderr}");
        assert!(
            stderr.contains("rannoch: a write failed at commit, Ticket(1)"),
            "nothing on stderr: {stderr}"
        );
        Ok(())
    }

    #[test]
    fn a_closed_queue_reports_nothing_twice() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = queue_failing_bad(&dir)?;
        let handled = Arc::new(Mutex::new(0));
        let into = Arc::clone(&handled);
        queue.on_failure(move |_| *super::lock(&into) += 1);
        let bad = queue.write(item("bad"))?;
        assert_eq!(written(queue.close()), [bad]);
        assert_eq!(*super::lock(&handled), 0);
        Ok(())
    }

    #[test]
    fn a_panicking_subscriber_does_not_stop_the_others() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = queue_failing_bad(&dir)?;
        let heard = Arc::new(Mutex::new(0));
        let into = Arc::clone(&heard);
        queue.subscribe(|_| panic!("a subscriber panics"));
        queue.subscribe(move |_| *super::lock(&into) += 1);
        queue.write(item("i1"))?;
        let _ = queue.flush();
        queue.write(item("i2"))?;
        let _ = queue.flush();
        assert_eq!(*super::lock(&heard), 2);
        Ok(())
    }

    #[test]
    fn the_heartbeat_outlives_a_panicking_subscriber() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = beating(&dir)?;
        let heard = Arc::new(Mutex::new(0));
        let into = Arc::clone(&heard);
        queue.subscribe(|_| panic!("a subscriber panics"));
        queue.subscribe(move |_| *super::lock(&into) += 1);
        for (key, beats) in [("i1", 1), ("i2", 2)] {
            queue.write(item(key))?;
            wait_for(|| *super::lock(&heard) >= beats);
        }
        Ok(())
    }

    #[test]
    fn a_subscriber_panic_in_an_unwinding_drop_does_not_abort()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let queue = queue_failing_bad(&dir)?;
        queue.subscribe(|_| panic!("a subscriber panics"));
        queue.write(item("i1"))?;
        let unwound = std::panic::catch_unwind(AssertUnwindSafe(move || {
            let _queue = queue;
            panic!("the app panics");
        }));
        assert!(unwound.is_err());
        assert!(
            open(dir.path().join("graph.rannoch"), Some(&crate::TEST_KEY))?
                .find("item", "i1")?
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn a_wrongly_typed_value_is_refused_when_queued() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = open(dir.path().join("graph.rannoch"), Some(&crate::TEST_KEY))?;
        let mut class = Class::new("item", [] as [&str; 0]);
        class.properties.insert("v".to_string(), ValueType::Integer);
        g.declare(&Declaration::Class(class))?;
        let queue = Queue::open(g, Duration::from_secs(3_600));
        let unit = Unit::new(
            "item",
            "i1",
            [("v", "nine")],
            ValidTime::On(Date::day(2026, 1, 1)),
        );
        let refused = queue.write(Write::new("app").unit(unit));
        assert!(
            matches!(&refused, Err(Error::Refused(reason)) if reason.contains("declared Integer")),
            "expected a refusal, got {refused:?}"
        );
        assert_eq!(queue.flush().0, None);
        Ok(())
    }

    /// A graph with class item and item i1 written as its own event, and that event.
    fn with_source(
        path: &std::path::Path,
    ) -> Result<(crate::Graph, crate::Event), Box<dyn std::error::Error>> {
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("item", ["v"])))?;
        let source = g.write(&item("i1"))?.remove(0);
        Ok((g, source))
    }

    #[test]
    fn a_queued_write_keeps_its_derived_from() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        let (g, source) = with_source(&path)?;
        let queue = Queue::open(g, Duration::from_secs(3_600));
        queue.write(item("i2").derived_from([source.id]))?;
        let (_, failed) = queue.flush();
        assert!(failed.is_empty(), "{failed:?}");
        let reader = open(&path, Some(&crate::TEST_KEY))?;
        let found = reader.find("item", "i2")?.map(|n| n.derived_from);
        assert_eq!(found, Some(vec![source.id]));
        assert_eq!(
            reader.derived(source.id)?,
            [crate::NodeRef::new("item", "i2")]
        );
        Ok(())
    }

    #[test]
    fn a_queued_write_naming_an_unknown_event_is_refused() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        let (g, _) = with_source(&path)?;
        let (_, foreign) = with_source(&dir.path().join("other.rannoch"))?;
        let queue = Queue::open(g, Duration::from_secs(3_600));
        let ticket = queue.write(item("i2").derived_from([foreign.id]))?;
        let (_, failed) = queue.flush();
        assert!(
            matches!(
                failed.as_slice(),
                [Failure::Write { ticket: t, error }]
                    if *t == ticket && matches!(error.cause, Error::Refused(_))
            ),
            "expected the write refused, got {failed:?}"
        );
        assert!(
            open(&path, Some(&crate::TEST_KEY))?
                .find("item", "i2")?
                .is_none()
        );
        Ok(())
    }
}
