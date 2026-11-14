//! Rannoch's core types: the event, its status and the error vocabulary.
//!
//! Rannoch emits through the `log` crate at two levels, `error` and `debug`;
//! the app chooses where the output goes.

mod broker;
mod inbox;

pub use broker::{Config, InboxConfig, Rannoch, Sender};

use std::fmt;

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Rannoch's one I/O struct, starting from the `CloudEvents` shape.
#[derive(Debug, Clone)]
pub struct Event<T, D> {
    /// Set by rannoch at accept; a `UUIDv7`.
    pub id: Uuid,
    /// Another event's id, or None; set by whoever calls `send`.
    pub source: Option<Uuid>,
    /// The app's plain fieldless enum.
    pub ty: T,
    /// The app's; rannoch never reads it.
    pub data: D,
    pub run_at: Option<DateTime<Utc>>,
    /// Set by rannoch, 0 at accept.
    pub status: Status,
}

/// An event's status: one int field with four values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum Status {
    Binned = -1,
    Added = 0,
    InProgress = 1,
    Processed = 2,
}

/// Rannoch's error vocabulary. A binned event is a status, not an error.
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
