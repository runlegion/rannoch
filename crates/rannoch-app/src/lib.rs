//! The app that owns the store for rannoch's own tests: every crate's
//! migrations, gathered by `build.rs` into the committed `migrations/`,
//! embedded as one migrator, and forgr over its `schema.json`.

/// Every crate's migrations as one migrator.
#[must_use]
pub fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}

/// Test data from this store's schema, for this crate's tests and, through
/// `testing`, every other crate's.
#[cfg(any(test, feature = "testing"))]
pub mod forgr {
    forgr::schema!("migrations/schema.json");
}
