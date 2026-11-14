//! The inbox: it holds accepted events on its heap.

use std::mem;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use chrono::TimeDelta;
use uuid::Uuid;

use crate::broker::{Shared, lock};
use crate::{Append, Error, Event, Status};

/// An inbox and its heap, which holds accepted events in memory, with no size limit.
#[derive(Debug)]
pub(crate) struct Inbox<T, D> {
    pub(crate) name: String,
    pub(crate) types: Vec<T>,
    pub(crate) interval: TimeDelta,
    pub(crate) heap: Vec<Event<T, D>>,
}

impl<T, D> Inbox<T, D> {
    pub(crate) fn new(name: String, types: Vec<T>, interval: TimeDelta) -> Self {
        Self {
            name,
            types,
            interval,
            heap: Vec::new(),
        }
    }

    /// Accepts the event: sets its id, sets status 0 when it has no source
    /// (one with a source keeps the status it carries), and puts it on the heap.
    pub(crate) fn accept(&mut self, mut event: Event<T, D>) -> Status {
        event.id = Uuid::now_v7();
        if event.source.is_none() {
            event.status = Status::Added;
        }
        log::debug!("accepted {} into {}", event.id, self.name);
        self.heap.push(event);
        Status::Added
    }
}

/// Inbox `index`'s thread: on each interval it hands over its heap; when
/// `stop` closes, it hands over what is left and returns.
pub(crate) fn run<T, D>(
    broker: &Shared<T, D>,
    index: usize,
    every: Duration,
    stop: &Receiver<()>,
    append: &dyn Append<T, D>,
) {
    loop {
        let stopping = stop.recv_timeout(every) != Err(RecvTimeoutError::Timeout);
        hand_over(broker, index, append);
        if stopping {
            return;
        }
    }
}

/// Hands inbox `index`'s heap to `append` as one batch; a batch that fails is
/// logged and goes back on the heap, ahead of what arrived since.
fn hand_over<T, D>(broker: &Shared<T, D>, index: usize, append: &dyn Append<T, D>) {
    let batch = mem::take(&mut lock(broker).inboxes[index].heap);
    if batch.is_empty() {
        return;
    }
    if append.append(&batch).is_ok() {
        log::debug!("handed over {} events from inbox {index}", batch.len());
    } else {
        log::error!(
            "inbox {index} could not hand over {} events: {}",
            batch.len(),
            Error::Store
        );
        lock(broker).inboxes[index].heap.splice(0..0, batch);
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use uuid::Uuid;

    use std::sync::{Arc, Mutex};

    use super::{Inbox, hand_over};
    use crate::broker::{Broker, lock};
    use crate::{Append, Error, Event, Status};

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

    #[test]
    fn an_event_with_a_source_keeps_its_status_and_one_without_is_accepted_at_0() {
        let mut inbox = Inbox::new("default".to_owned(), Vec::new(), TimeDelta::zero());

        assert_eq!(inbox.accept(event(Some(Uuid::now_v7()))), Status::Added);
        assert_eq!(inbox.accept(event(None)), Status::Added);

        let statuses: Vec<Status> = inbox.heap.iter().map(|e| e.status).collect();
        assert_eq!(statuses, [Status::Processed, Status::Added]);
        assert!(inbox.heap.iter().all(|e| e.id.get_version_num() == 7));
    }

    /// An append that fails while `failing` is set, keeping each batch's
    /// statuses it received.
    #[derive(Debug, Default)]
    struct Recorder {
        failing: Mutex<bool>,
        batches: Mutex<Vec<Vec<Status>>>,
    }

    impl Append<u8, ()> for Recorder {
        fn append(&self, batch: &[Event<u8, ()>]) -> Result<(), Error> {
            if *self.failing.lock().unwrap() {
                return Err(Error::Store);
            }
            let statuses = batch.iter().map(|e| e.status).collect();
            self.batches.lock().unwrap().push(statuses);
            Ok(())
        }
    }

    #[test]
    fn a_failed_batch_goes_back_ahead_of_later_events_and_an_empty_heap_hands_nothing() {
        let broker = Arc::new(Mutex::new(Broker {
            inboxes: vec![Inbox::new(
                "default".to_owned(),
                Vec::new(),
                TimeDelta::zero(),
            )],
            closed: false,
        }));
        let accept = |e| lock(&broker).inboxes[0].accept(e);
        let recorder = Recorder {
            failing: Mutex::new(true),
            ..Recorder::default()
        };
        accept(event(None));
        hand_over(&broker, 0, &recorder);
        accept(event(Some(Uuid::now_v7())));
        let heap: Vec<Status> = lock(&broker).inboxes[0]
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
        assert!(lock(&broker).inboxes[0].heap.is_empty());
    }
}
