//! Rannoch's one clock, on tokio's time.
//!
//! Elapsed time and waiting are `tokio::time`. Wall time is derived from it: the wall time when
//! the clock started, plus tokio time elapsed since. So freezing tokio's clock
//! (`#[tokio::test(start_paused = true)]`) freezes wall time and new ids too, and
//! `tokio::time::advance` moves all three by exactly what it is given.

use std::sync::{Mutex, OnceLock, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::Instant;
use uuid::timestamp::context::ContextV7;
use uuid::{Timestamp, Uuid};

/// The wall time and tokio's time when the clock started: the first time any of its functions
/// is called in this process.
#[allow(
    clippy::disallowed_methods,
    reason = "the clock alone reads the real wall time, once, when it starts"
)]
fn start() -> &'static (DateTime<Utc>, Instant) {
    static START: OnceLock<(DateTime<Utc>, Instant)> = OnceLock::new();
    START.get_or_init(|| (Utc::now(), Instant::now()))
}

/// The wall time: the wall time when the clock started, plus tokio time elapsed since.
#[must_use]
pub fn now() -> DateTime<Utc> {
    let (wall, at) = *start();
    let tokio = elapsed();
    // Each paused runtime has a clock of its own, which can stand before the one the clock
    // started on, so the time since the start is signed.
    let since = match tokio.checked_duration_since(at) {
        Some(ahead) => TimeDelta::from_std(ahead),
        None => TimeDelta::from_std(at.duration_since(tokio)).map(|behind| -behind),
    };
    // Only a span past chrono's range, hundreds of millions of years, fails to convert.
    wall + since.unwrap_or_default()
}

/// Tokio's time now: frozen while tokio's clock is paused.
#[must_use]
pub fn elapsed() -> Instant {
    Instant::now()
}

/// A new `UUIDv7` made from [`now`], greater than every id made before it in this process. Its
/// one counter is never started over, so when the clock moves back, as when two paused runtimes
/// stand at different times, the id carries the latest time the counter has seen.
#[must_use]
pub fn new_id() -> Uuid {
    static CONTEXT: Mutex<ContextV7> = Mutex::new(ContextV7::new());
    let now = now();
    let context = CONTEXT.lock().unwrap_or_else(PoisonError::into_inner);
    let seconds = u64::try_from(now.timestamp()).unwrap_or(0);
    Uuid::new_v7(Timestamp::from_unix(
        &*context,
        seconds,
        now.timestamp_subsec_nanos(),
    ))
}
