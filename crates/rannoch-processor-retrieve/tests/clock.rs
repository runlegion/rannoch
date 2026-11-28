//! Issue #171's scenarios, from `tests/features/clock.feature`, one test each: they read this
//! crate's code and tests. Each name below is split in two, so this file does not name it.

use std::path::{Path, PathBuf};
use std::{fs, io};

/// Every `.rs` file under this crate's `src` and `tests`, with its text.
fn sources() -> io::Result<Vec<(PathBuf, String)>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut dirs = vec![root.join("src"), root.join("tests")];
    let mut files = Vec::new();
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push((path.clone(), fs::read_to_string(path)?));
            }
        }
    }
    Ok(files)
}

#[tokio::test(start_paused = true)]
async fn rannoch_processor_retrieve_calls_no_real_time_function() {
    // Given rannoch-processor-retrieve's code and tests
    let files = sources().unwrap();
    assert!(files.len() >= 3, "{files:?}");

    // When they are read
    let real = [
        ["thread::", "sleep"],
        ["thread::", "spawn"],
        ["recv_", "timeout"],
        ["Instant::", "now"],
        ["SystemTime::", "now"],
        ["Utc::", "now"],
        ["now_", "v7"],
    ]
    .map(|parts| parts.concat());

    // Then none calls a real-time function
    for (path, text) in &files {
        for name in &real {
            assert!(
                !text.contains(name.as_str()),
                "{} calls {name}",
                path.display()
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn every_rannoch_processor_retrieve_test_runs_with_the_clock_frozen() {
    // Given rannoch-processor-retrieve's tests
    let files = sources().unwrap();

    // When they are read
    let plain = ["#[", "test]"].concat();
    let tokio = ["#[", "tokio::test"].concat();
    let frozen = ["#[", "tokio::test(start_paused = true)]"].concat();

    // Then each runs with the clock frozen
    let mut tests = 0;
    for (path, text) in &files {
        assert!(
            !text.contains(plain.as_str()),
            "{} has a test off tokio",
            path.display()
        );
        let on_tokio = text.matches(tokio.as_str()).count();
        let paused = text.matches(frozen.as_str()).count();
        assert_eq!(
            on_tokio,
            paused,
            "{} has a test on a running clock",
            path.display()
        );
        tests += paused;
    }
    // At least the two unit tests, the FR-RANNOCH-122 scenario and these two.
    assert!(tests >= 5, "{tests}");
}
