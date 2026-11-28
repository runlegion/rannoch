//! The lane: it holds its accepted events on its heap until its batch is written.

use std::mem;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use uuid::Uuid;

use crate::broker::{Shared, lock};
use crate::{Append, Error, Event, Status};

/// A lane and its heap, which holds the lane's accepted events in memory until its batch is
/// written.
#[derive(Debug)]
pub(crate) struct Lane<T, D> {
    pub(crate) name: String,
    pub(crate) tys: Vec<T>,
    /// The heap's size, in bytes, at which the batch is written without waiting on the heartbeat.
    pub(crate) limit: usize,
    /// How long the lane waits, after its last batch, before it writes the next.
    pub(crate) heartbeat: Duration,
    pub(crate) heap: Vec<Event<T, D>>,
    /// The size of the events on the heap, in bytes.
    pub(crate) bytes: usize,
    /// Wakes the lane's task: when the heap reaches the limit, and at shutdown.
    pub(crate) wake: Arc<Notify>,
}

impl<T, D> Lane<T, D> {
    pub(crate) fn new(name: String, tys: Vec<T>, limit: usize, heartbeat: Duration) -> Self {
        Self {
            name,
            tys,
            limit,
            heartbeat,
            heap: Vec::new(),
            bytes: 0,
            wake: Arc::new(Notify::new()),
        }
    }

    /// Accepts the event, `bytes` in size: sets its id to `id`, sets status 0 when it has no
    /// source (one with a source keeps the status it carries), and puts it on the heap, waking
    /// the lane's task once the heap reaches the lane's limit.
    pub(crate) fn accept(&mut self, id: Uuid, mut event: Event<T, D>, bytes: usize) -> Status {
        event.id = id;
        if event.source.is_none() {
            event.status = Status::Added;
        }
        log::debug!("accepted {} into {}", event.id, self.name);
        self.heap.push(event);
        self.bytes += bytes;
        if self.bytes >= self.limit {
            self.wake.notify_one();
        }
        Status::Added
    }
}

/// Lane `index`'s task: it writes the lane's batch each time its heartbeat expires or the lane
/// wakes it; once rannoch is shutting down, it writes what is left and returns.
pub(crate) async fn run<T, D>(
    broker: Shared<T, D>,
    index: usize,
    heartbeat: Duration,
    wake: Arc<Notify>,
    append: Arc<dyn Append<T, D>>,
) {
    loop {
        let _ = tokio::time::timeout(heartbeat, wake.notified()).await;
        let stopping = lock(&broker).closed;
        hand_over(&broker, index, &*append);
        if stopping {
            return;
        }
    }
}

/// Hands lane `index`'s heap to `append` as one batch; a batch that fails is logged and goes
/// back on the heap, ahead of what arrived since. The events the append replies with, as a -1
/// for an event it refused, are accepted onto the lane's own heap and handed over in turn.
pub(crate) fn hand_over<T, D>(broker: &Shared<T, D>, index: usize, append: &dyn Append<T, D>) {
    loop {
        let (batch, bytes) = {
            let mut held = lock(broker);
            let lane = &mut held.lanes[index];
            (mem::take(&mut lane.heap), mem::take(&mut lane.bytes))
        };
        if batch.is_empty() {
            return;
        }
        let Ok(replies) = append.append(&batch) else {
            let mut held = lock(broker);
            let lane = &mut held.lanes[index];
            log::error!(
                "lane {} could not hand over {} events: {}",
                lane.name,
                batch.len(),
                Error::Store
            );
            lane.heap.splice(0..0, batch);
            lane.bytes += bytes;
            return;
        };
        let mut held = lock(broker);
        log::debug!(
            "handed over {} events from lane {}",
            batch.len(),
            held.lanes[index].name
        );
        for reply in replies {
            held.put(index, reply);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use uuid::Uuid;
    use uuid::timestamp::context::ContextV7;

    use super::{Lane, hand_over};
    use crate::broker::{Broker, lock};
    use crate::{Append, Error, Event, Status, clock};

    fn event(source: Option<Uuid>) -> Event<u8, ()> {
        Event {
            id: Uuid::nil(),
            source,
            ty: 0,
            data: (),
            run_at: None,
            status: Status::Processed,
        }
    }

    fn lane(name: &str, tys: Vec<u8>) -> Lane<u8, ()> {
        Lane::new(name.to_owned(), tys, usize::MAX, Duration::ZERO)
    }

    fn broker(lanes: Vec<Lane<u8, ()>>) -> Arc<Mutex<Broker<u8, ()>>> {
        Arc::new(Mutex::new(Broker {
            lanes,
            closed: false,
            ids: ContextV7::new(),
            weigh: |_| 0,
        }))
    }

    #[tokio::test(start_paused = true)]
    async fn an_event_with_a_source_keeps_its_status_and_one_without_is_accepted_at_0() {
        let mut lane = lane("default", Vec::new());

        let accepted =
            [event(Some(clock::new_id())), event(None)].map(|e| lane.accept(clock::new_id(), e, 0));
        assert_eq!(accepted, [Status::Added; 2]);

        let statuses: Vec<Status> = lane.heap.iter().map(|e| e.status).collect();
        assert_eq!(statuses, [Status::Processed, Status::Added]);
        assert!(lane.heap.iter().all(|e| e.id.get_version_num() == 7));
    }

    /// An append that fails while `failing` is set, keeping each batch's
    /// statuses it received, and replying to the next batch it writes with `replies`.
    #[derive(Debug, Default)]
    struct Recorder {
        failing: Mutex<bool>,
        batches: Mutex<Vec<Vec<Status>>>,
        replies: Mutex<Vec<Event<u8, ()>>>,
    }

    impl Append<u8, ()> for Recorder {
        fn append(&self, batch: &[Event<u8, ()>]) -> Result<Vec<Event<u8, ()>>, Error> {
            if *self.failing.lock().unwrap() {
                return Err(Error::Store);
            }
            let statuses = batch.iter().map(|e| e.status).collect();
            self.batches.lock().unwrap().push(statuses);
            Ok(std::mem::take(&mut *self.replies.lock().unwrap()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_batch_goes_back_ahead_of_later_events_and_an_empty_heap_hands_nothing() {
        let broker = broker(vec![lane("default", Vec::new())]);
        let accept = |e| lock(&broker).lanes[0].accept(clock::new_id(), e, 0);
        let recorder = Recorder {
            failing: Mutex::new(true),
            ..Recorder::default()
        };
        accept(event(None));
        hand_over(&broker, 0, &recorder);
        accept(event(Some(clock::new_id())));
        let heap: Vec<Status> = lock(&broker).lanes[0]
            .heap
            .iter()
            .map(|e| e.status)
            .collect();
        assert_eq!(heap, [Status::Added, Status::Processed]);

        *recorder.failing.lock().unwrap() = false;
        hand_over(&broker, 0, &recorder);
        hand_over(&broker, 0, &recorder);
        assert_eq!(
            *recorder.batches.lock().unwrap(),
            [vec![Status::Added, Status::Processed]]
        );
        assert!(lock(&broker).lanes[0].heap.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_is_written_through_its_own_lanes_batch() {
        let broker = broker(vec![lane("default", Vec::new()), lane("ones", vec![1])]);
        let recorder = Recorder::default();

        // A reply of ty 1 to a batch of the default lane is stamped and written by the default
        // lane, in one more hand-over; it is not sent to the lane for its ty.
        lock(&broker).lanes[0].accept(clock::new_id(), event(None), 0);
        *recorder.replies.lock().unwrap() = vec![Event {
            ty: 1,
            status: Status::Binned,
            ..event(Some(clock::new_id()))
        }];
        hand_over(&broker, 0, &recorder);
        assert_eq!(
            *recorder.batches.lock().unwrap(),
            [vec![Status::Added], vec![Status::Binned]]
        );
        assert!(lock(&broker).lanes.iter().all(|l| l.heap.is_empty()));
    }

    #[tokio::test(start_paused = true)]
    async fn the_heap_wakes_its_lane_once_it_reaches_the_limit() {
        let mut lane = Lane::<u8, ()>::new("big".to_owned(), Vec::new(), 10, Duration::ZERO);
        let wake = Arc::clone(&lane.wake);
        let woken = || tokio::time::timeout(Duration::ZERO, wake.notified());
        lane.accept(clock::new_id(), event(None), 9);
        assert!(woken().await.is_err());
        lane.accept(clock::new_id(), event(None), 1);
        assert_eq!(lane.bytes, 10);
        assert!(woken().await.is_ok());
    }
}
