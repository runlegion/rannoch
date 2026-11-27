//! #155's scenarios, from `tests/features/events_log_settings.feature`, one test each, named for
//! its criterion. The settings are held for the life of the process, so each test re-runs this
//! test binary as a child process that loads its settings and runs the scenario alone; the
//! parent checks that the child ran it and passed.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

use rannoch_core::{Append, Error, Event, Status};
use rannoch_events_log::Log;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Set in the child process, which runs the scenario's body.
const CHILD: &str = "RANNOCH_EVENTS_LOG_CHILD";

/// An append that commits every batch and sends nothing in reply.
#[derive(Debug)]
struct Committed;

impl Append<u16, u8> for Committed {
    fn append(&self, _: &[Event<u16, u8>]) -> std::result::Result<Vec<Event<u16, u8>>, Error> {
        Ok(Vec::new())
    }
}

/// An empty directory of the scenario's own.
fn dir(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("events-log-settings")
        .join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Runs `test` alone in a child process of this test binary, with `DIR` set to `dir`, and
/// checks it passed.
fn child(test: &str, dir: &Path) -> Result<()> {
    let output = Command::new(env::current_exe()?)
        .args([test, "--exact", "--nocapture"])
        .env(CHILD, "1")
        .env("DIR", dir)
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    assert!(stdout.contains("1 passed"), "{stdout}");
    Ok(())
}

/// Builds the log over `passed` and commits a batch of two events; returns their ids.
fn commit(passed: PathBuf) -> Result<Vec<String>> {
    let event = |data| Event {
        id: Uuid::now_v7(),
        source: None,
        ty: 10_u16,
        data,
        run_at: None,
        status: Status::Added,
    };
    let batch = [event(1_u8), event(2)];
    Log::new(Committed, passed).append(&batch)?;
    Ok(batch.iter().map(|e| e.id.to_string()).collect())
}

/// The ids of the events written to the log file at `path`, in order.
fn written(path: &Path) -> Result<Vec<String>> {
    let text = fs::read_to_string(path)?;
    let mut ids = Vec::new();
    for line in text.lines() {
        let line: serde_json::Value = serde_json::from_str(line)?;
        ids.push(line["id"].as_str().unwrap_or_default().to_owned());
    }
    Ok(ids)
}

#[test]
fn criterion_1_the_section_path_replaces_the_path_passed_in() -> Result<()> {
    if env::var(CHILD).is_ok() {
        let dir = PathBuf::from(env::var("DIR")?);
        // Given settings loaded before Log::new whose [events-log] section sets path
        rannoch_settings::load(&dir.join("app.toml"))?;

        // When the log's append commits a batch
        let ids = commit(dir.join("passed.log"))?;

        // Then each event of the batch is written to that file, and none to the passed path
        assert_eq!(written(&dir.join("set.log"))?, ids);
        assert!(!dir.join("passed.log").exists());
        return Ok(());
    }
    let dir = dir("1")?;
    let set = dir.join("set.log");
    let settings = format!("[events-log]\npath = {:?}\n", set.display().to_string());
    fs::write(dir.join("app.toml"), settings)?;
    child(
        "criterion_1_the_section_path_replaces_the_path_passed_in",
        &dir,
    )
}

#[test]
fn criterion_2_no_section_the_passed_path_stands() -> Result<()> {
    if env::var(CHILD).is_ok() {
        let dir = PathBuf::from(env::var("DIR")?);
        // Given no settings loaded: the passed path stands
        let ids = commit(dir.join("unloaded.log"))?;
        assert_eq!(written(&dir.join("unloaded.log"))?, ids);

        // Given settings loaded with no [events-log] section: the passed path stands
        rannoch_settings::load(&dir.join("app.toml"))?;
        let ids = commit(dir.join("passed.log"))?;
        assert_eq!(written(&dir.join("passed.log"))?, ids);
        return Ok(());
    }
    let dir = dir("2")?;
    fs::write(dir.join("app.toml"), "[app]\nname = \"kessel\"\n")?;
    child("criterion_2_no_section_the_passed_path_stands", &dir)
}
