//! The app: the committed migrations directory, embedded with the public `embed!`.

/// Every crate's migrations as one migrator.
#[must_use]
pub fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}
