//! The app: the committed migrations directory, embedded, and forgr over its schema.json.

/// Every crate's migrations as one migrator.
#[must_use]
pub fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}

/// The app's one query, for forgr's SQL check.
pub const SYMBOL_PATHS: &str = "SELECT id, path FROM symbol";

/// Test data from this store's schema, for this crate's tests and, through `testing`, every other crate's.
#[cfg(any(test, feature = "testing"))]
pub mod forgr {
    forgr::schema!("migrations/schema.json");
}

#[cfg(test)]
mod tests {
    #[test]
    fn p19_a_symbol_from_the_built_schema() {
        let conn = super::forgr::empty_store();
        let s = super::forgr::symbol::factory().id("s1").path("a.rs").insert(&conn).unwrap();
        println!("R5 P19 inserted symbol {}", s.id);
    }
}
