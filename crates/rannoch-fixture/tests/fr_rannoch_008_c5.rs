//! `@criterion-FR-RANNOCH-008-c5`, from
//! `crates/rannoch-app/tests/features/fr_rannoch_008_c5.feature`.

/// The app's committed schema.json.
const SCHEMA: &str = include_str!("../../rannoch-app/migrations/schema.json");

/// This crate's manifest.
const MANIFEST: &str = include_str!("../Cargo.toml");

#[test]
fn fr_rannoch_008_c5() {
    // Given an app whose schema.json names a table
    assert!(SCHEMA.contains(r#""name": "fixture""#), "{SCHEMA}");

    // When another crate's test opens the app's empty test store and builds a
    // row for that table through the app's forgr module
    let store = rannoch_app::forgr::empty_store();
    let row = rannoch_app::forgr::fixture::factory()
        .id("f1")
        .name("one")
        .insert(&store)
        .unwrap();

    // Then the row is in the store
    let name: String = store
        .query_row("SELECT name FROM fixture WHERE id = ?1", [&row.id], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(name, "one");

    // And that crate has no forgr dependency of its own
    assert!(
        !MANIFEST
            .lines()
            .any(|l| l.trim_start().starts_with("forgr")),
        "{MANIFEST}"
    );
}
