//! Measures what a host allows: its memory, and how fast a directory takes durable writes.
//!
//! A measurement is a value the caller keeps; the probe stores nothing. It records the operating
//! system and when it was taken, so the caller can tell when to measure again.
//!
//! Durable means what the operating system needs for the data to survive power loss:
//! [`File::sync_all`], which issues `F_FULLFSYNC` on macOS. The probe measures the disk by writing
//! to it instead of reading its bus type, because a label misleads.

use std::fs::File;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, System};

/// The bytes written and durably flushed once to time one flush.
const FLUSH_BYTES: usize = 4 * 1024;
/// The most bytes written and durably flushed in one round of the rate measurement. Rounds start
/// at [`FLUSH_BYTES`] and double while the budget fits them.
const ROUND_BYTES: usize = 1024 * 1024;
/// The most bytes the rate measurement writes, however much budget remains.
const MAX_BYTES: u64 = 64 * 1024 * 1024;

/// A host's memory: its total, and the memory it allows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memory {
    /// The operating system, as [`std::env::consts::OS`] names it.
    pub os: String,
    /// When the measurement was taken.
    pub taken_at: SystemTime,
    /// The host's total memory, in bytes.
    pub total: u64,
    /// The least of the total, the limit the host's container or job sets where one is set, and
    /// the cap the caller passed where one was passed, in bytes.
    pub allowed: u64,
}

/// A directory's durable write rate and the time one durable flush takes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Disk {
    /// The operating system, as [`std::env::consts::OS`] names it.
    pub os: String,
    /// When the measurement was taken.
    pub taken_at: SystemTime,
    /// The directory measured.
    pub directory: PathBuf,
    /// Bytes written and durably flushed per second.
    pub bytes_per_second: u64,
    /// The time one durable flush takes.
    pub flush: Duration,
}

/// Why a directory could not be measured.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Writing, flushing or removing the probe's file failed.
    #[error("measuring {path}: {source}")]
    Io {
        /// The probe's file.
        path: PathBuf,
        /// The failure.
        source: io::Error,
    },
    /// The budget ran out before one round of the rate measurement could finish.
    #[error("the budget of {0:?} ran out before the directory could be measured")]
    Budget(Duration),
}

/// Measures the host's memory. `cap` is the caller's cap in bytes, where it passes one.
#[must_use]
pub fn memory(cap: Option<u64>) -> Memory {
    let mut system = System::new();
    system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
    let total = system.total_memory();
    Memory {
        os: std::env::consts::OS.to_owned(),
        taken_at: SystemTime::now(),
        total,
        allowed: allowed(total, limit(&mut system), cap),
    }
}

/// The memory limit the process's container or job sets: the least limit on its cgroup and the
/// cgroup's ancestors. Only Linux sets one this way; elsewhere there is none.
fn limit(system: &mut System) -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing(),
    );
    system
        .process(pid)?
        .cgroup_limits()
        .map(|limits| limits.total_memory)
}

/// The least of the total and whichever of the limit and the cap are set.
fn allowed(total: u64, limit: Option<u64>, cap: Option<u64>) -> u64 {
    [limit, cap].into_iter().flatten().fold(total, u64::min)
}

/// Measures `directory` by writing and durably flushing a file in it, within `budget`.
///
/// The file is removed before this returns, whether the measurement succeeds or fails.
///
/// # Errors
///
/// [`Error::Io`] when the file cannot be created, written, flushed or removed, and
/// [`Error::Budget`] when the budget runs out before the rate can be measured.
pub fn disk(directory: &Path, budget: Duration) -> Result<Disk, Error> {
    let start = Instant::now();
    let taken_at = SystemTime::now();
    let mut probe = Probe::create(directory)?;

    let flush = probe.round(&[0; FLUSH_BYTES])?;

    let chunk = vec![0; ROUND_BYTES];
    let mut written: u64 = 0;
    let mut spent = Duration::ZERO;
    let mut last = (flush, FLUSH_BYTES);
    let mut size = FLUSH_BYTES;
    // A round fits when twice the time it is predicted to take, scaled from the last round's
    // bytes, still lands inside the budget.
    let fits = |bytes: usize, (took, of): (Duration, usize)| {
        let predicted = took.as_nanos() * bytes as u128 / of as u128;
        start.elapsed().as_nanos() + predicted * 2 < budget.as_nanos()
    };
    while written < MAX_BYTES {
        let grown = (size * 2).min(ROUND_BYTES);
        if fits(grown, last) {
            size = grown;
        } else if !fits(size, last) {
            break;
        }
        let took = probe.round(&chunk[..size])?;
        spent += took;
        written += size as u64;
        last = (took, size);
    }
    if written == 0 || start.elapsed() > budget {
        return Err(Error::Budget(budget));
    }
    probe.remove()?;

    let nanos = spent.as_nanos().max(1);
    let bytes_per_second =
        u64::try_from(u128::from(written) * 1_000_000_000 / nanos).unwrap_or(u64::MAX);
    Ok(Disk {
        os: std::env::consts::OS.to_owned(),
        taken_at,
        directory: directory.to_owned(),
        bytes_per_second,
        flush,
    })
}

/// The probe's file, removed when dropped.
struct Probe {
    path: PathBuf,
    file: Option<File>,
}

impl Probe {
    fn create(directory: &Path) -> Result<Self, Error> {
        let path = directory.join(format!(".probe-{}", uuid::Uuid::now_v7()));
        let file = File::create_new(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        Ok(Self {
            path,
            file: Some(file),
        })
    }

    /// Writes `bytes` and durably flushes them, returning how long that took.
    fn round(&mut self, bytes: &[u8]) -> Result<Duration, Error> {
        let start = Instant::now();
        let result = match self.file.as_mut() {
            Some(file) => file.write_all(bytes).and_then(|()| file.sync_all()),
            None => Err(io::Error::from(io::ErrorKind::NotFound)),
        };
        result.map_err(|source| self.error(source))?;
        Ok(start.elapsed())
    }

    /// Closes and removes the file, reporting a removal that fails.
    fn remove(&mut self) -> Result<(), Error> {
        self.file = None;
        std::fs::remove_file(&self.path).map_err(|source| self.error(source))
    }

    fn error(&self, source: io::Error) -> Error {
        Error::Io {
            path: self.path.clone(),
            source,
        }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        if self.file.take().is_some() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::allowed;

    #[test]
    fn allowed_is_the_total_when_nothing_else_is_set() {
        assert_eq!(allowed(16, None, None), 16);
    }

    #[test]
    fn allowed_is_the_limit_when_it_is_least() {
        assert_eq!(allowed(16, Some(8), None), 8);
        assert_eq!(allowed(16, Some(8), Some(12)), 8);
    }

    #[test]
    fn allowed_is_the_cap_when_it_is_least() {
        assert_eq!(allowed(16, None, Some(4)), 4);
        assert_eq!(allowed(16, Some(8), Some(4)), 4);
    }

    #[test]
    fn allowed_is_the_total_when_it_is_least() {
        assert_eq!(allowed(16, Some(32), Some(64)), 16);
    }
}
