//! The in-memory write queue: a write is appended and returns at once, and a heartbeat commits
//! everything queued, advances the committed watermark and then fires the subscribers, so a
//! subscriber never hears of an event that is not committed.

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

type Subscriber = Box<dyn FnMut(Cursor) + Send>;

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
}

#[derive(Default)]
struct State {
    queued: Vec<(Ticket, Write)>,
    last: u64,
    committed: Option<Ticket>,
    /// Writes that failed at commit, kept for FR-RANNOCH-075 to report.
    failed: Vec<(Ticket, WriteError)>,
    /// Commits whose subscribers did not fire because the committed cursor could not be read.
    unfired: Vec<(Ticket, Error)>,
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
                beat.commit();
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
    /// commit, or `None` before the first. A write that failed at commit is passed too; the queue
    /// keeps its error, and reporting it is FR-RANNOCH-075's.
    #[must_use]
    pub fn committed(&self) -> Option<Ticket> {
        lock(&self.shared.state).committed
    }

    /// Commits everything queued now, without waiting for the heartbeat, and returns the
    /// committed watermark, which has passed every write accepted before the call.
    // An app flushes for the commit; the watermark it returns is there when the app wants it.
    #[allow(clippy::must_use_candidate)]
    pub fn flush(&self) -> Option<Ticket> {
        self.shared.commit();
        self.committed()
    }

    /// Calls `subscriber` after each commit, with the cursor of the last committed event; a
    /// follower reads [`Graph::events`] since its own cursor up to it. A subscriber must not
    /// call back into the queue's [`Queue::flush`], which waits on the commit firing it.
    pub fn subscribe(&self, subscriber: impl FnMut(Cursor) + Send + 'static) {
        lock(&self.shared.subscribers).push(Box::new(subscriber));
    }

    /// Stops the heartbeat and commits everything queued, as dropping the queue does. A process
    /// that exits without unwinding (`std::process::exit`, `panic = "abort"`) runs no drop, so it
    /// closes the queue first.
    pub fn close(self) {
        drop(self);
    }
}

impl Shared {
    /// Commits what is queued, then advances the watermark, then fires the subscribers. The
    /// graph's lock is held throughout, so commits and their firings never interleave.
    fn commit(&self) {
        let mut graph = lock(&self.graph);
        let batch = std::mem::take(&mut lock(&self.state).queued);
        let Some(&(last, _)) = batch.last() else {
            return;
        };
        // A write that fails at commit is kept, not dropped; reporting it is FR-RANNOCH-075's.
        let failed: Vec<(Ticket, Error)> =
            match graph.write_batch(batch.iter().map(|(_, write)| write)) {
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
        let failed = failed.into_iter().map(|(ticket, cause)| {
            (
                ticket,
                WriteError {
                    events: Vec::new(),
                    cause,
                },
            )
        });
        let cursor = {
            let mut state = lock(&self.state);
            state.committed = Some(last);
            state.failed.extend(failed);
            match graph.last_cursor() {
                Ok(cursor) => cursor,
                Err(e) => {
                    state.unfired.push((last, e));
                    return;
                }
            }
        };
        for subscriber in lock(&self.subscribers).iter_mut() {
            subscriber(cursor);
        }
    }
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
    /// panics with unwinding loses no write.
    fn drop(&mut self) {
        lock(&self.shared.state).stop = true;
        self.shared.stopped.notify_all();
        if let Some(heartbeat) = self.heartbeat.take() {
            let _ = heartbeat.join();
        }
        self.shared.commit();
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock(&self.shared.state);
        f.debug_struct("Queue")
            .field("committed", &state.committed)
            .field("failed", &state.failed)
            .field("unfired", &state.unfired)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Queue;
    use crate::{Class, Date, Declaration, Error, Unit, ValidTime, ValueType, Write, open};

    #[test]
    fn a_batch_that_fails_whole_fails_every_ticket() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        let mut g = open(&path)?;
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
        queue.flush();
        let failed: Vec<_> = super::lock(&queue.shared.state)
            .failed
            .iter()
            .map(|(ticket, _)| *ticket)
            .collect();
        assert_eq!(failed, tickets);
        Ok(())
    }

    #[test]
    fn a_wrongly_typed_value_is_refused_when_queued() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = open(dir.path().join("graph.rannoch"))?;
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
        assert_eq!(queue.flush(), None);
        Ok(())
    }

    /// A graph with class item and item i1 written as its own event, and that event.
    fn with_source(
        path: &std::path::Path,
    ) -> Result<(crate::Graph, crate::Event), Box<dyn std::error::Error>> {
        let mut g = open(path)?;
        g.declare(&Declaration::Class(Class::new("item", [] as [&str; 0])))?;
        let source = g.write(&Write::new("app").unit(item("i1")))?.remove(0);
        Ok((g, source))
    }

    fn item(key: &str) -> Unit {
        Unit::new("item", key, [] as [(&str, &str); 0], ValidTime::Unknown)
    }

    #[test]
    fn a_queued_write_keeps_its_derived_from() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        let (g, source) = with_source(&path)?;
        let queue = Queue::open(g, Duration::from_secs(3_600));
        queue.write(Write::new("app").unit(item("i2")).derived_from([source.id]))?;
        queue.flush();
        let reader = open(&path)?;
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
        let ticket = queue.write(
            Write::new("app")
                .unit(item("i2"))
                .derived_from([foreign.id]),
        )?;
        queue.flush();
        let state = super::lock(&queue.shared.state);
        assert!(
            matches!(
                state.failed.as_slice(),
                [(failed, error)] if *failed == ticket && matches!(error.cause, Error::Refused(_))
            ),
            "expected the write refused, got {:?}",
            state.failed
        );
        assert!(open(&path)?.find("item", "i2")?.is_none());
        Ok(())
    }
}
