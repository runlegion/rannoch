#![warn(missing_docs)]
//! Rannoch's core types: the event, its status and the error vocabulary, and the broker an app
//! opens rannoch with.
//!
//! An app opens [`Rannoch`] with a [`Config`]: its inboxes, the default inbox's batch interval,
//! its store as a [`Storage`] and the events crate's [`Append`]. [`Rannoch::open`] applies the
//! store's pending migrations before anything else; [`Rannoch::sender`] gives each producer a
//! [`Sender`], and [`Rannoch::shutdown`] stops it. [`migrator`] is every crate's migrations as
//! one migrator, for the app's store.
//!
//! # From a send to the store
//!
//! [`Sender::send`] puts the event on the heap of the inbox for its `ty`, in memory, and returns
//! [`Status::Added`]: the event is not on disk yet. Each inbox hands its whole heap to the append
//! as one batch once per interval, so a sent event reaches the append at its inbox's next
//! hand-over. The default inbox's interval is 20 ms in [`Config::default`].
//!
//! The append returns the events it sends in reply, as a -1 for an event of the batch it did not
//! write, and the inbox sends each as [`Sender::send`] does, to the inbox for its `ty`. Once
//! rannoch is shutting down the broker takes no send, so the inbox that handed the batch over
//! accepts its replies onto its own heap and hands them over itself, in one more hand-over.
//!
//! When the append fails, the inbox logs the failure at `error`, puts the batch back on its heap
//! ahead of the events sent since, and hands all of them over again at its next interval. With
//! rannoch-events' `Events`, which writes a batch in one transaction, a batch that always fails
//! holds up its inbox: nothing sent to that inbox after it is written either.
//!
//! [`Rannoch::shutdown`] refuses every send after it with [`Error::Closed`], has each inbox hand
//! over what is left on its heap, and joins each inbox's thread. When that last hand-over fails,
//! the failure is logged and those events are not written.
//!
//! With no append in the [`Config`], no inbox hands anything over and no sent event is written.
//!
//! # Failures and logs
//!
//! Every call in this crate that can fail returns an [`Error`], whose variant is all it carries.
//! A binned event is a [`Status`], not an error.
//!
//! Rannoch emits through the `log` crate at two levels, `error` and `debug`;
//! the app chooses where the output goes.
//!
//! # Example
//!
//! The app's own `ty` enum, an inbox for one of its types, a send, and shutdown. This `Config`
//! has no store and no append, so open checks no migration and nothing is written;
//! rannoch-queue's crate docs start rannoch over a store.
//!
//! ```
//! use chrono::TimeDelta;
//! use rannoch_core::{Config, Error, Event, InboxConfig, Rannoch, Status};
//! use uuid::Uuid;
//!
//! /// The app's event types.
//! #[derive(Debug, Clone, Copy, PartialEq)]
//! enum Ty {
//!     Page,
//!     Asset,
//! }
//!
//! fn main() -> Result<(), Error> {
//!     // Assets go to an inbox with a half-second interval; every other ty goes to the default
//!     // inbox.
//!     let assets = InboxConfig {
//!         name: "assets".to_owned(),
//!         types: vec![Ty::Asset],
//!         interval: TimeDelta::milliseconds(500),
//!     };
//!
//!     // A ty listed in two inboxes fails open.
//!     let twice = Config {
//!         inboxes: vec![assets.clone(), assets.clone()],
//!         ..Config::default()
//!     };
//!     assert_eq!(Rannoch::<Ty, String>::open(twice).err(), Some(Error::Config));
//!
//!     let config = Config {
//!         inboxes: vec![assets],
//!         ..Config::default()
//!     };
//!     let rannoch = Rannoch::<Ty, String>::open(config)?;
//!     let sender = rannoch.sender();
//!
//!     let page = Event {
//!         id: Uuid::nil(), // rannoch sets the id at accept
//!         source: None,
//!         ty: Ty::Page,
//!         data: "/about".to_owned(),
//!         run_at: None,
//!         status: Status::Added,
//!     };
//!     assert_eq!(sender.send(page.clone())?, Status::Added);
//!
//!     rannoch.shutdown();
//!     assert_eq!(sender.send(page), Err(Error::Closed));
//!     Ok(())
//! }
//! ```

mod broker;
mod inbox;

pub use broker::{Append, Config, InboxConfig, Rannoch, Sender, Storage};

use std::fmt;

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Rannoch's one I/O struct, starting from the `CloudEvents` shape.
#[derive(Debug, Clone)]
pub struct Event<T, D> {
    /// Set by rannoch at accept to a new `UUIDv7`, replacing the id the event was sent with.
    pub id: Uuid,
    /// Another event's id, or None; set by whoever calls `send`.
    pub source: Option<Uuid>,
    /// The app's plain fieldless enum.
    pub ty: T,
    /// The app's; rannoch never reads it.
    pub data: D,
    /// When the event comes due: the queue takes it once this time has passed. `None` is due
    /// as soon as it is written.
    pub run_at: Option<DateTime<Utc>>,
    /// Set by rannoch to 0 at accept for an event without a source; an event with a source keeps
    /// the status it is sent with.
    pub status: Status,
}

/// An event's status: one int field with four values.
///
/// An event's progress is recorded as further events whose `source` is that event, each with a
/// status other than 0; the event's own row keeps the status it was written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum Status {
    /// -1: binned, and not run again. The queue sends it for each failure it meets about an
    /// event, as for one whose `ty` has no flow, and a processor sends it for a run that failed;
    /// its data names the reason.
    Binned = -1,
    /// 0: added. An event at 0 is due to the queue once its `run_at` has passed, until an event
    /// with another status names it as its source.
    Added = 0,
    /// 1: in progress. The queue sends it when it takes a due event, before it looks up the
    /// event's flow.
    InProgress = 1,
    /// 2: processed. The processor sends it, with its result, once the event's flow has run.
    Processed = 2,
}

/// Rannoch's error vocabulary. A binned event is a status, not an error.
///
/// The variant is all an `Error` carries; its `Display` is one short line, such as "the store
/// failed".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// The configuration does not make sense at open.
    Config,
    /// An event is sent while rannoch is shutting down.
    Closed,
    /// A batch or a read failed in the store.
    Store,
    /// Processing failed where the caller needs to know.
    Queue,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Config => "the configuration does not make sense",
            Self::Closed => "rannoch is shutting down",
            Self::Store => "the store failed",
            Self::Queue => "processing failed",
        })
    }
}

impl std::error::Error for Error {}

/// Every crate's migrations as one migrator, gathered by `build.rs` into the
/// committed `migrations/`.
#[must_use]
pub fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}

/// Test data from the store's schema, for this crate's tests and, through
/// `testing`, every other crate's.
#[cfg(any(test, feature = "testing"))]
// The items come from forgr's `schema!` macro and cannot take a doc comment here.
#[allow(missing_docs)]
pub mod forgr {
    forgr::schema!("migrations/schema.json");
}

#[cfg(test)]
mod tests {
    use super::Status;
    use log::{Level, Log, Metadata, Record};
    use std::sync::Mutex;

    struct TestLogger(Mutex<Vec<(Level, String)>>);

    impl Log for TestLogger {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn log(&self, record: &Record<'_>) {
            if let Ok(mut lines) = self.0.lock() {
                lines.push((record.level(), record.args().to_string()));
            }
        }
        fn flush(&self) {}
    }

    static LOGGER: TestLogger = TestLogger(Mutex::new(Vec::new()));

    #[test]
    fn fr_rannoch_007_c1() {
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Debug);
        log::error!("an error line");
        log::debug!("a debug line");
        let lines = LOGGER.0.lock().unwrap();
        assert!(lines.contains(&(Level::Error, "an error line".to_owned())));
        assert!(lines.contains(&(Level::Debug, "a debug line".to_owned())));
    }

    #[test]
    fn status_is_stored_as_an_int() {
        let ints = [
            Status::Binned,
            Status::Added,
            Status::InProgress,
            Status::Processed,
        ]
        .map(|s| s as i8);
        assert_eq!(ints, [-1, 0, 1, 2]);
    }
}
