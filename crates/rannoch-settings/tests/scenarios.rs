//! FR-RANNOCH-130's scenarios, from `tests/features/fr_rannoch_130.feature`, one test each,
//! named for its criterion. The settings are held for the life of the process, so each test
//! re-runs this test binary as a child process that runs the scenario alone; the parent checks
//! that the child ran it and passed. Criteria `09447f08` and `0951a3d9` run a copy of the binary
//! from a directory of their own, with or without a `rannoch.config.toml` beside it.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

use rannoch_core::{Config, Error, Rannoch};
use serde::Deserialize;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Set in the child process, which runs the scenario's body.
const CHILD: &str = "RANNOCH_SETTINGS_CHILD";

/// An app's section.
#[derive(Debug, Default, PartialEq, Deserialize)]
struct App {
    name: String,
    count: u32,
}

/// A queue's section.
#[derive(Debug, Default, PartialEq, Deserialize)]
struct Queue {
    depth: u32,
}

/// An empty directory of the scenario's own.
fn dir(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("settings")
        .join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Writes `text` to `file` in the scenario's directory.
fn file(name: &str, file: &str, text: &str) -> Result<PathBuf> {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("settings")
        .join(name)
        .join(file);
    fs::write(&path, text)?;
    Ok(path)
}

/// Runs `test` alone in a child process of `exe`, with `env`, and checks it passed.
fn child(exe: &Path, test: &str, env: &[(&str, &Path)]) -> Result<()> {
    let mut command = Command::new(exe);
    command
        .args([test, "--exact", "--nocapture"])
        .env(CHILD, "1");
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    assert!(stdout.contains("1 passed"), "{stdout}");
    Ok(())
}

/// Runs `test` alone in a child process of this test binary.
fn this(test: &str, env: &[(&str, &Path)]) -> Result<()> {
    child(&env::current_exe()?, test, env)
}

/// Runs `test` alone in a child process of a copy of this binary in `dir`.
fn copied(dir: &Path, test: &str) -> Result<()> {
    let exe = dir.join("scenarios");
    fs::copy(env::current_exe()?, &exe)?;
    child(&exe, test, &[])
}

fn open(settings: Option<PathBuf>) -> std::result::Result<Rannoch<u8, String>, Error> {
    Rannoch::open(Config {
        settings,
        ..Config::default()
    })
}

fn path(key: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(env::var(key)?))
}

const APP: &str = "[app]\nname = \"kessel\"\ncount = 3\n";

fn kessel() -> App {
    App {
        name: "kessel".to_owned(),
        count: 3,
    }
}

#[test]
fn criterion_01a21e58_0933_opened_with_a_path_each_crate_takes_its_own_section() -> Result<()> {
    if env::var(CHILD).is_ok() {
        // When rannoch opens and each section is taken by name as its own type
        let _rannoch = open(Some(path("SETTINGS")?)).unwrap();

        // Then each type holds the values that section of the file gives
        assert_eq!(
            rannoch_settings::section::<Queue>("queue"),
            Queue { depth: 4 }
        );
        assert_eq!(rannoch_settings::section::<App>("app"), kessel());
        return Ok(());
    }
    // Given a settings file with a [queue] and an [app] section, and open with its path
    dir("0933")?;
    let settings = file("0933", "app.toml", &format!("[queue]\ndepth = 4\n\n{APP}"))?;
    this(
        "criterion_01a21e58_0933_opened_with_a_path_each_crate_takes_its_own_section",
        &[("SETTINGS", &settings)],
    )?;
    Ok(())
}

#[test]
fn criterion_01a21e58_0944_opened_with_no_path_reads_the_file_beside_the_binary() -> Result<()> {
    if env::var(CHILD).is_ok() {
        // When rannoch opens and the [app] section is taken
        let _rannoch = open(None).unwrap();

        // Then it holds the values rannoch.config.toml gives
        assert_eq!(rannoch_settings::section::<App>("app"), kessel());
        return Ok(());
    }
    // Given a rannoch.config.toml with an [app] section beside the running binary
    let dir = dir("0944")?;
    file("0944", "rannoch.config.toml", APP)?;
    copied(
        &dir,
        "criterion_01a21e58_0944_opened_with_no_path_reads_the_file_beside_the_binary",
    )?;
    Ok(())
}

#[test]
fn criterion_01a21e58_0951_no_file_or_no_section_gives_the_defaults() -> Result<()> {
    const NO_FILE: &str = "criterion_01a21e58_0951_no_file_child";
    const NO_SECTION: &str = "criterion_01a21e58_0951_no_section_child";
    if env::var(CHILD).is_ok() {
        return Ok(());
    }
    // Given no settings file at all, and then a settings file with no [app] section
    copied(&dir("0951")?, NO_FILE)?;
    dir("0951-section")?;
    let settings = file("0951-section", "app.toml", "[queue]\ndepth = 4\n")?;
    this(NO_SECTION, &[("SETTINGS", &settings)])?;
    Ok(())
}

#[test]
fn criterion_01a21e58_0951_no_file_child() {
    if env::var(CHILD).is_ok() {
        // When rannoch opens and the [app] section is taken
        let _rannoch = open(None).unwrap();

        // Then it holds that type's defaults
        assert_eq!(rannoch_settings::section::<App>("app"), App::default());
    }
}

#[test]
fn criterion_01a21e58_0951_no_section_child() -> Result<()> {
    if env::var(CHILD).is_ok() {
        // When rannoch opens and the [app] section is taken
        let _rannoch = open(Some(path("SETTINGS")?)).unwrap();

        // Then it holds that type's defaults
        assert_eq!(rannoch_settings::section::<App>("app"), App::default());
    }
    Ok(())
}

#[test]
fn criterion_01a21e6f_ff16_a_file_that_does_not_parse_stops_the_app() -> Result<()> {
    if env::var(CHILD).is_ok() {
        // When Rannoch::open is called with its path
        let opened = open(Some(path("SETTINGS")?));

        // Then open fails with Error::Config
        assert_eq!(opened.err(), Some(Error::Config));
        return Ok(());
    }
    // Given a settings file that is not valid TOML
    dir("ff16")?;
    let settings = file("ff16", "app.toml", "[app\nname = = kessel\n")?;
    this(
        "criterion_01a21e6f_ff16_a_file_that_does_not_parse_stops_the_app",
        &[("SETTINGS", &settings)],
    )?;
    Ok(())
}

#[test]
fn criterion_01a21e6f_ff25_settings_loaded_before_open_are_the_ones_used() -> Result<()> {
    if env::var(CHILD).is_ok() {
        rannoch_settings::load(&path("LOADED")?).unwrap();
        let _rannoch = open(Some(path("SETTINGS")?)).unwrap();

        // When a crate takes its section
        let app = rannoch_settings::section::<App>("app");

        // Then it holds the values of the file given to load, and open did not load the other
        assert_eq!(app, kessel());
        return Ok(());
    }
    // Given an app that loads one file, then opens rannoch with a different file
    dir("ff25")?;
    let loaded = file("ff25", "loaded.toml", APP)?;
    let other = file("ff25", "other.toml", "[app]\nname = \"other\"\ncount = 9\n")?;
    this(
        "criterion_01a21e6f_ff25_settings_loaded_before_open_are_the_ones_used",
        &[("LOADED", &loaded), ("SETTINGS", &other)],
    )?;
    Ok(())
}

#[test]
fn criterion_01a21e7c_7faf_a_section_that_does_not_read_as_its_type_gives_the_defaults()
-> Result<()> {
    if env::var(CHILD).is_ok() {
        // When rannoch opens and the [app] section is taken
        let _rannoch = open(Some(path("SETTINGS")?)).unwrap();

        // Then it holds that type's defaults
        assert_eq!(rannoch_settings::section::<App>("app"), App::default());
        return Ok(());
    }
    // Given a settings file whose [app] section holds text where the type has a number
    dir("7faf")?;
    let settings = file(
        "7faf",
        "app.toml",
        "[app]\nname = \"kessel\"\ncount = \"three\"\n",
    )?;
    this(
        "criterion_01a21e7c_7faf_a_section_that_does_not_read_as_its_type_gives_the_defaults",
        &[("SETTINGS", &settings)],
    )?;
    Ok(())
}

#[test]
fn criterion_01a21e7c_7fb9_a_second_load_is_refused() -> Result<()> {
    const BY_LOAD: &str = "criterion_01a21e7c_7fb9_loaded_by_load_child";
    const BY_OPEN: &str = "criterion_01a21e7c_7fb9_loaded_by_open_child";
    if env::var(CHILD).is_ok() {
        return Ok(());
    }
    // Given an app that has loaded its settings, by load or by Rannoch::open
    dir("7fb9")?;
    let first = file("7fb9", "first.toml", APP)?;
    let other = file("7fb9", "other.toml", "[app]\nname = \"other\"\ncount = 9\n")?;
    let env = [("FIRST", first.as_path()), ("OTHER", other.as_path())];
    this(BY_LOAD, &env)?;
    this(BY_OPEN, &env)?;
    Ok(())
}

/// When it calls load again with another file, then the call fails with `Error::Config`, and
/// every section still holds the first file's values.
fn loads_again() -> Result<()> {
    assert_eq!(
        rannoch_settings::load(&path("OTHER")?),
        Err(rannoch_settings::Error::Config)
    );
    assert_eq!(rannoch_settings::section::<App>("app"), kessel());
    Ok(())
}

#[test]
fn criterion_01a21e7c_7fb9_loaded_by_load_child() -> Result<()> {
    if env::var(CHILD).is_ok() {
        rannoch_settings::load(&path("FIRST")?).unwrap();
        loads_again()?;
    }
    Ok(())
}

#[test]
fn criterion_01a21e7c_7fb9_loaded_by_open_child() -> Result<()> {
    if env::var(CHILD).is_ok() {
        let _rannoch = open(Some(path("FIRST")?)).unwrap();
        loads_again()?;
    }
    Ok(())
}
