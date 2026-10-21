//! The in-memory write queue: a write is appended and returns at once, and a heartbeat commits
//! everything queued, advances the committed watermark and then fires the subscribers, so a
//! subscriber never hears of an event that is not committed.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::events::Cursor;
use crate::open::Graph;
use crate::write::Write;

/// A write's place in its queue. A later write has a greater ticket, so the committed watermark
/// has passed a write once [`Queue::committed`] is at or above its ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket(u64);

type Subscriber = Box<dyn FnMut(Cursor) + Send>;

/// One process's queue of writes to a graph, held in memory only, and the heartbeat that
/// commits it. The queue owns the graph: [`Graph::write`] is the synchronous commit the
/// heartbeat uses, and other handles on the same file read what it commits.
pub struct Queue {
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
            shared,
            heartbeat: Some(heartbeat),
        }
    }

    /// Appends `write` to the queue and returns its ticket, without waiting on the store.
    #[must_use]
    pub fn write(&self, write: Write) -> Ticket {
        let mut state = lock(&self.shared.state);
        state.last += 1;
        let ticket = Ticket(state.last);
        state.queued.push((ticket, write));
        ticket
    }

    /// The committed watermark: the highest ticket the heartbeat or a flush has taken through a
    /// commit, or `None` before the first. A write that failed at commit is passed too; reporting
    /// it is FR-RANNOCH-075's.
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
        // A write that fails at commit is FR-RANNOCH-075's to report; the watermark passes it,
        // since it is no longer waiting in the queue.
        for (_, write) in &batch {
            let _ = graph.write(write);
        }
        let cursor = graph.last_cursor();
        lock(&self.state).committed = Some(last);
        if let Ok(cursor) = cursor {
            for subscriber in lock(&self.subscribers).iter_mut() {
                subscriber(cursor);
            }
        }
    }
}

impl Drop for Queue {
    /// Stops the heartbeat. Committing what is still queued on drop is FR-RANNOCH-061's.
    fn drop(&mut self) {
        lock(&self.shared.state).stop = true;
        self.shared.stopped.notify_all();
        if let Some(heartbeat) = self.heartbeat.take() {
            let _ = heartbeat.join();
        }
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("committed", &self.committed())
            .finish_non_exhaustive()
    }
}
