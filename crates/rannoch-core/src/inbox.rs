//! The inbox: it holds accepted events on its heap.

use chrono::TimeDelta;

use uuid::Uuid;

use crate::{Event, Status};

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

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use uuid::Uuid;

    use super::Inbox;
    use crate::{Event, Status};

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
}
