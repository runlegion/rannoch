//! #168's scenarios, from `tests/features/clock.feature`, one test each, named for its criterion.
//! Each reads the crate's own source and tests. The names each looks for are split with
//! `concat!` so this file does not match them itself.

use std::path::Path;
use std::{fs, io};

/// The text of every `.rs` file in the crate's `src` and `tests`, with its path.
fn sources() -> io::Result<Vec<(String, String)>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in ["src", "tests"] {
        for entry in fs::read_dir(root.join(dir))? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "rs") {
                files.push((path.display().to_string(), fs::read_to_string(&path)?));
            }
        }
    }
    Ok(files)
}

#[tokio::test(start_paused = true)]
async fn criterion_1_no_real_time_calls() -> io::Result<()> {
    let calls = [
        concat!("thread::", "sleep"),
        concat!("thread::", "spawn"),
        concat!("recv_", "timeout"),
        concat!("Instant::", "now"),
        concat!("SystemTime::", "now"),
        concat!("Utc::", "now"),
        concat!("now_", "v7"),
    ];
    for (path, text) in sources()? {
        for call in calls {
            assert!(!text.contains(call), "{path} calls {call}");
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn criterion_2_every_test_runs_frozen() -> io::Result<()> {
    let frozen = concat!("#[tokio::", "test(start_paused = true)]");
    let mut tests = 0;
    for (path, text) in sources()? {
        assert!(
            !text.contains(concat!("#[", "test]")),
            "{path} has a test off the clock"
        );
        let all = text.matches(concat!("#[tokio::", "test")).count();
        assert_eq!(
            all,
            text.matches(frozen).count(),
            "{path} has a test not frozen"
        );
        for call in [concat!("time::", "resume"), concat!("time::", "pause")] {
            assert!(!text.contains(call), "{path} calls {call}");
        }
        tests += all;
    }
    // The three tests of scenarios.rs and settings.rs, and this file's two.
    assert_eq!(tests, 5);
    Ok(())
}
