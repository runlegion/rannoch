//! Each worker's place, held in memory. Taking an event and closing it change only this; the
//! queue's group commit saves every worker that changed in one transaction.

use std::collections::{BTreeMap, VecDeque};

use rannoch_store::{Cursor, Error, Event, Place, State};

/// How many events a worker reads from the log at a time.
pub(crate) const CHUNK: usize = 1024;

/// An event handed out to a worker and not yet passed by its cursor.
struct Taken {
    cursor: Cursor,
    /// In progress until closed, then done or failed.
    state: State,
    /// The state the worker asked to keep once its cursor reaches this event.
    kept: Option<String>,
}

/// One worker's place in memory.
pub(crate) struct Worker {
    /// Every event at or before it is closed; it reaches disk only in a group commit.
    cursor: Cursor,
    /// The state the worker keeps with `cursor`.
    kept: Option<String>,
    /// The last event handed out.
    handed: Cursor,
    /// Events read from the log after `handed`, not yet handed out.
    ahead: VecDeque<Event>,
    /// The events handed out after `cursor`, oldest first.
    taken: VecDeque<Taken>,
    /// Events closed as failed and not yet saved, with their reasons.
    failed: BTreeMap<Cursor, String>,
    /// Events closed as done that an earlier run saved as failed, not yet cleared.
    cleared: Vec<Cursor>,
    /// The last event an earlier run saved as failed: a later close as done may clear it.
    last_failed: Cursor,
    /// The cursor last saved.
    saved: Cursor,
    /// Changed since the last group commit.
    dirty: bool,
}

/// What a group commit saves of one worker: its place and its unsaved closes.
pub(crate) type Save = (Place, Vec<(Cursor, Option<String>)>);

impl Worker {
    /// A worker as an earlier run saved it, resuming at its cursor.
    pub(crate) fn resumed(place: &Place, last_failed: Cursor) -> Worker {
        Worker {
            cursor: place.cursor,
            kept: place.kept.clone(),
            handed: place.cursor,
            ahead: VecDeque::new(),
            taken: VecDeque::new(),
            failed: BTreeMap::new(),
            cleared: Vec::new(),
            last_failed,
            saved: place.cursor,
            dirty: false,
        }
    }

    /// A worker on its first call, at the start of the log.
    pub(crate) fn joining() -> Worker {
        let mut worker = Worker::resumed(
            &Place {
                worker: String::new(),
                cursor: Cursor::START,
                kept: None,
            },
            Cursor::START,
        );
        worker.dirty = true;
        worker
    }

    pub(crate) fn cursor(&self) -> Cursor {
        self.cursor
    }

    pub(crate) fn kept(&self) -> Option<&str> {
        self.kept.as_deref()
    }

    /// Where the next read from the log starts, once nothing is read ahead.
    pub(crate) fn handed(&self) -> Cursor {
        self.handed
    }

    /// Hands out the next event read ahead, if any.
    pub(crate) fn take(&mut self) -> Option<Event> {
        let event = self.ahead.pop_front()?;
        self.handed = event.cursor;
        self.taken.push_back(Taken {
            cursor: event.cursor,
            state: State::InProgress,
            kept: None,
        });
        Some(event)
    }

    /// Keeps `events`, read from the log after `from`, unless the worker moved on meanwhile.
    pub(crate) fn read(&mut self, from: Cursor, events: Vec<Event>) {
        if self.handed == from && self.ahead.is_empty() {
            self.ahead.extend(events);
        }
    }

    /// Closes the event at `cursor`, as failed with a reason or as done, keeping `kept` with it,
    /// and moves the cursor past every closed event at the front.
    pub(crate) fn close(
        &mut self,
        name: &str,
        cursor: Cursor,
        failed: Option<&str>,
        kept: Option<String>,
    ) -> Result<(), Error> {
        let Some(taken) = self
            .taken
            .iter_mut()
            .find(|taken| taken.cursor == cursor && taken.state == State::InProgress)
        else {
            return Err(Error::Refused(format!(
                "event at {cursor:?} is not in progress for worker {name}"
            )));
        };
        taken.kept = kept;
        if let Some(reason) = failed {
            taken.state = State::Failed(reason.to_string());
            self.failed.insert(cursor, reason.to_string());
        } else {
            taken.state = State::Done;
            if cursor <= self.last_failed {
                self.cleared.push(cursor);
            }
        }
        while let Some(front) = self.taken.front()
            && front.state != State::InProgress
        {
            if let Some(Taken { cursor, kept, .. }) = self.taken.pop_front() {
                self.cursor = cursor;
                if kept.is_some() {
                    self.kept = kept;
                }
            }
        }
        self.dirty = true;
        Ok(())
    }

    /// Where the event at `cursor` stands, as far as memory knows; `None` when only the store
    /// knows, for an event closed before the last save.
    pub(crate) fn state(&self, cursor: Cursor) -> Option<State> {
        if cursor > self.handed {
            return Some(State::Waiting);
        }
        if let Some(taken) = self.taken.iter().find(|taken| taken.cursor == cursor) {
            return Some(taken.state.clone());
        }
        if let Some(reason) = self.failed.get(&cursor) {
            return Some(State::Failed(reason.clone()));
        }
        if cursor > self.saved || self.cleared.contains(&cursor) {
            return Some(State::Done);
        }
        None
    }

    /// What a group commit saves of this worker, when it changed, and marks it saved; a save that
    /// fails calls [`Worker::unsaved`].
    pub(crate) fn save(&mut self, name: &str) -> Option<Save> {
        if !self.dirty {
            return None;
        }
        self.dirty = false;
        let place = Place {
            worker: name.to_string(),
            cursor: self.cursor,
            kept: self.kept.clone(),
        };
        let closes = self
            .failed
            .iter()
            .map(|(cursor, reason)| (*cursor, Some(reason.clone())))
            .chain(self.cleared.iter().map(|cursor| (*cursor, None)))
            .collect();
        Some((place, closes))
    }

    /// The group commit saved `save`: what it carried is in the store now.
    pub(crate) fn saved(&mut self, save: &Save) {
        let (place, closes) = save;
        self.saved = place.cursor;
        for (cursor, reason) in closes {
            match reason {
                // A failure ahead of the cursor stays: memory still answers for it.
                Some(reason)
                    if *cursor <= place.cursor && self.failed.get(cursor) == Some(reason) =>
                {
                    self.failed.remove(cursor);
                }
                Some(_) => {}
                None => self.cleared.retain(|c| c != cursor),
            }
        }
    }

    /// The group commit failed: the next one saves this worker again.
    pub(crate) fn unsaved(&mut self) {
        self.dirty = true;
    }
}
