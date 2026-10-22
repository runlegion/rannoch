//! The probe measured on the machine the tests run on.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use rannoch_probe::{Disk, Error, Memory, disk, memory};

fn entries(directory: &std::path::Path) -> usize {
    std::fs::read_dir(directory).unwrap().count()
}

#[test]
fn memory_reports_a_total_greater_than_zero() {
    let measured = memory(None);
    assert!(measured.total > 0);
    assert!(measured.allowed > 0);
    assert!(measured.allowed <= measured.total);
    assert_eq!(measured.os, std::env::consts::OS);
}

#[test]
fn memory_allows_no_more_than_the_cap() {
    let measured = memory(Some(1024));
    assert_eq!(measured.allowed, 1024);
}

/// Run under a memory limit with `RANNOCH_PROBE_EXPECT_LIMIT` naming it in bytes; skipped
/// otherwise. CI runs it in a systemd scope with `MemoryMax` set.
#[cfg(target_os = "linux")]
#[test]
fn memory_allows_no_more_than_the_cgroup_limit() {
    let Ok(expected) = std::env::var("RANNOCH_PROBE_EXPECT_LIMIT") else {
        return;
    };
    let expected: u64 = expected.parse().unwrap();
    assert!(memory(None).allowed <= expected);
}

#[test]
fn disk_reports_a_rate_and_a_flush_and_leaves_no_file() {
    let directory = tempfile::tempdir().unwrap();
    let budget = Duration::from_secs(5);
    let start = Instant::now();
    let measured = disk(directory.path(), budget).unwrap();
    assert!(start.elapsed() <= budget);
    assert!(measured.bytes_per_second > 0);
    assert!(measured.flush > Duration::ZERO);
    assert_eq!(measured.directory, directory.path());
    assert_eq!(measured.os, std::env::consts::OS);
    assert_eq!(entries(directory.path()), 0);
}

#[test]
fn disk_within_a_tight_budget_returns_within_it_measured_or_not() {
    let directory = tempfile::tempdir().unwrap();
    let budget = Duration::from_millis(250);
    let start = Instant::now();
    let overrun = match disk(directory.path(), budget) {
        Ok(measured) => {
            assert!(measured.bytes_per_second > 0);
            Duration::ZERO
        }
        // A durable flush cannot be interrupted: the one in progress may overrun the budget.
        Err(Error::Budget { flush, .. }) => flush,
        Err(error) => panic!("{error}"),
    };
    assert!(start.elapsed() <= budget + overrun);
    assert_eq!(entries(directory.path()), 0);
}

#[test]
fn disk_that_runs_out_of_budget_fails_and_leaves_no_file() {
    let directory = tempfile::tempdir().unwrap();
    let result = disk(directory.path(), Duration::from_nanos(1));
    assert!(matches!(result, Err(Error::Budget { .. })));
    assert_eq!(entries(directory.path()), 0);
}

#[test]
fn disk_of_a_missing_directory_fails_and_leaves_no_file() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing");
    let result = disk(&missing, Duration::from_secs(5));
    assert!(matches!(result, Err(Error::Io { .. })));
    assert_eq!(entries(directory.path()), 0);
}

#[test]
fn measurements_round_trip_through_serialization() {
    let directory = tempfile::tempdir().unwrap();
    let measured = disk(directory.path(), Duration::from_secs(5)).unwrap();
    let json = serde_json::to_string(&measured).unwrap();
    assert_eq!(serde_json::from_str::<Disk>(&json).unwrap(), measured);

    let measured = memory(Some(1 << 30));
    let json = serde_json::to_string(&measured).unwrap();
    assert_eq!(serde_json::from_str::<Memory>(&json).unwrap(), measured);
}
