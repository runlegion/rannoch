#![warn(missing_docs)]
//! Rannoch's core types: the event, its status and the error vocabulary, and the broker an app
//! opens rannoch with.
//!
//! An app opens [`Rannoch`] with a [`Config`]: its store as a [`Storage`], the events crate's
//! [`Append`] and its settings file. [`Rannoch::open`] applies the store's pending migrations
//! before anything else; [`Rannoch::sender`] gives each producer a [`Sender`], and
//! [`Rannoch::shutdown`] stops it. [`migrator`] is every crate's migrations as one migrator, for
//! the app's store.
//!
//! # Lanes
//!
//! The lanes come from the settings file, one `[lane.<name>]` section each: the `tys` it holds,
//! its `memory_limit_bytes` (8 MB by default) and its `heartbeat_ms` (20 by default). A `ty` no
//! lane lists goes to the default lane, and an event with a `run_at` goes to the scheduled lane;
//! a `[lane.default]` or `[lane.scheduled]` section sets that lane's limit and heartbeat. A `ty`
//! listed in two lanes, or one that is not the app's, fails open with [`Error::Config`].
//!
//! ```toml
//! [lane.default]
//! heartbeat_ms = 50
//!
//! [lane.assets]
//! tys = [2, 3]
//! memory_limit_bytes = 1048576
//! heartbeat_ms = 500
//! ```
//!
//! # From a send to the store
//!
//! [`Sender::send`] stamps the event's id and puts it on its lane's heap, in memory, and returns
//! [`Status::Added`]: the event is not on disk yet. Each lane hands its whole heap to the append
//! as one batch when its heartbeat expires or when the heap reaches its memory limit, whichever
//! comes first; each lane runs as a task of its own, so a large event in one lane does not delay
//! another lane's batches. An event's size is the event and its data's JSON. The id is stamped
//! under the lock that puts the event on its heap, so a lane's events reach the append in id
//! order.
//!
//! The append returns the events it sends in reply, as a -1 for an event of the batch it did not
//! write; the lane stamps each, accepts it onto its own heap and hands it over in one more batch.
//!
//! When the append fails, the lane logs the failure at `error`, puts the batch back on its heap
//! ahead of the events sent since, and hands all of them over again at its next batch. With
//! rannoch-events' `Events`, which writes a batch in one transaction, a batch that always fails
//! holds up its lane: nothing sent to that lane after it is written either.
//!
//! [`Rannoch::shutdown`] refuses every send after it with [`Error::Closed`], has each lane hand
//! over what is left on its heap, and blocks until each lane's task has returned. When that last
//! hand-over fails, the failure is logged and those events are not written.
//!
//! With no append in the [`Config`], no lane hands anything over and no sent event is written.
//!
//! # Time
//!
//! Every time rannoch reads or waits on goes through [`clock`], on tokio's time. Each lane runs
//! as a task on the tokio runtime [`Rannoch::open`] is called in, or on one it starts when called
//! outside a runtime. [`Rannoch::shutdown`] blocks, so async code calls it through
//! `tokio::task::spawn_blocking`.
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
//! The app's own `ty` enum, a send, and shutdown. This `Config` has no store and no append, so
//! open checks no migration and nothing is written; rannoch-queue's crate docs start rannoch over
//! a store.
//!
//! ```
//! use rannoch_core::{Config, Error, Event, Rannoch, Status};
//! use uuid::Uuid;
//!
//! /// The app's event types.
//! #[derive(Debug, Clone, Copy, PartialEq)]
//! enum Ty {
//!     Page = 1,
//! }
//!
//! /// A settings file lists a lane's `tys` as their `u16`.
//! impl TryFrom<u16> for Ty {
//!     type Error = u16;
//!     fn try_from(ty: u16) -> Result<Self, u16> {
//!         if ty == 1 { Ok(Ty::Page) } else { Err(ty) }
//!     }
//! }
//!
//! // The clock frozen, as in every rannoch-core test.
//! #[tokio::main(flavor = "current_thread", start_paused = true)]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let rannoch = Rannoch::<Ty, String>::open(Config::default())?;
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
//!     // Shutdown blocks, so async code runs it through spawn_blocking.
//!     tokio::task::spawn_blocking(move || rannoch.shutdown())
//!         .await?;
//!     assert_eq!(sender.send(page), Err(Error::Closed));
//!     Ok(())
//! }
//! ```

mod broker;
pub mod clock;
mod lane;

pub use broker::{Append, Config, Rannoch, Sender, Storage};

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

    #[tokio::test(start_paused = true)]
    async fn fr_rannoch_007_c1() {
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Debug);
        log::error!("an error line");
        log::debug!("a debug line");
        let lines = LOGGER.0.lock().unwrap();
        assert!(lines.contains(&(Level::Error, "an error line".to_owned())));
        assert!(lines.contains(&(Level::Debug, "a debug line".to_owned())));
    }

    #[tokio::test(start_paused = true)]
    async fn status_is_stored_as_an_int() {
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
