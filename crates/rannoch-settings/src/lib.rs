#![warn(missing_docs)]
//! The app's settings file, read once and held for the life of the process.
//!
//! The settings are loaded once: by the app calling [`load`] with its path before anything else,
//! or else at the first `Rannoch::open`. Each crate, rannoch's and the app's, takes its own
//! section by name as its own type with [`section`]. A missing file, a missing section, or a
//! section that does not read as its type gives that type's defaults. There is no reload: a
//! change to the file takes effect at the next restart.

use std::fmt;
use std::path::Path;
use std::sync::OnceLock;

use config::{Config, File, FileFormat};
use serde::de::DeserializeOwned;

/// The settings once loaded, for the life of the process.
static SETTINGS: OnceLock<Config> = OnceLock::new();

/// The settings' error vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// The file does not parse, or the settings are already loaded.
    Config,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Config => "the settings could not be loaded",
        })
    }
}

impl std::error::Error for Error {}

/// Loads the TOML settings file at `path` and holds its values for the life of the process. A
/// missing file loads no values, so every section gives its defaults.
///
/// # Errors
///
/// `Config` when the file does not parse, or when the settings are already loaded; the settings
/// already loaded stay.
pub fn load(path: &Path) -> Result<(), Error> {
    if loaded() {
        return Err(Error::Config);
    }
    let settings = Config::builder()
        .add_source(File::from(path).format(FileFormat::Toml).required(false))
        .build()
        .map_err(|_| Error::Config)?;
    SETTINGS.set(settings).map_err(|_| Error::Config)
}

/// Whether the settings are loaded.
#[must_use]
pub fn loaded() -> bool {
    SETTINGS.get().is_some()
}

/// The section `name` as `T`: `T`'s defaults when nothing is loaded, the section is missing, or
/// it does not read as `T`.
#[must_use]
pub fn section<T: DeserializeOwned + Default>(name: &str) -> T {
    SETTINGS
        .get()
        .and_then(|settings| settings.get::<T>(name).ok())
        .unwrap_or_default()
}
