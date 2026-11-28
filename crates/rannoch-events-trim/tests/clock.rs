//! #167's scenarios, `@criterion-167-c1` and `-c2` in `tests/features/clock.feature`. Both read
//! the crate's own source.

use std::fs;
use std::path::Path;

/// Every Rust file under the crate's `src` and `tests`, with its text.
fn sources() -> std::io::Result<Vec<(String, String)>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in ["src", "tests"] {
        for entry in fs::read_dir(root.join(dir))? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "rs") {
                let text = fs::read_to_string(&path)?;
                files.push((path.display().to_string(), text));
            }
        }
    }
    Ok(files)
}

#[tokio::test(start_paused = true)]
async fn clock_c1() {
    // Each call split in two, so this file does not name the calls it looks for.
    let calls = [
        ["thread::", "sleep"],
        ["thread::", "spawn"],
        ["recv_", "timeout"],
        ["Instant::", "now"],
        ["SystemTime::", "now"],
        ["Utc::", "now"],
        ["now_", "v7"],
    ]
    .map(|parts| parts.concat());
    let sources = sources().unwrap();
    assert!(sources.len() >= 4, "{} files", sources.len());
    for (path, text) in &sources {
        for call in &calls {
            assert!(!text.contains(call.as_str()), "{path} calls {call}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn clock_c2() {
    // Split in two as well, so this file's own text counts only its two tests.
    let plain = ["#[", "test]"].concat();
    let frozen = ["#[tokio::test(", "start_paused = true)]"].concat();
    let mut tests = 0;
    for (path, text) in &sources().unwrap() {
        assert!(
            !text.contains(plain.as_str()),
            "{path} has a test off the frozen clock"
        );
        tests += text.matches(frozen.as_str()).count();
    }
    // The one in scenarios.rs, two here.
    assert_eq!(tests, 3);
}
