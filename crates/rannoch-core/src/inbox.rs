//! The inbox: it holds accepted events on its heap.

use chrono::TimeDelta;

use uuid::Uuid;

use crate::{Event, Status};

/// An inbox and its heap, which holds accepted events in memory, with no size limit.
#[derive(Debug)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "name and interval are read by the batch, #3")
)]
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

    /// Accepts the event: sets its id and status and puts it on the heap.
    pub(crate) fn accept(&mut self, mut event: Event<T, D>) -> Status {
        event.id = Uuid::now_v7();
        event.status = Status::Added;
        self.heap.push(event);
        Status::Added
    }
}
