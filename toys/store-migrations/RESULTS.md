# Store migrations toy: results

Run 2026-11-10 against migratr v0.3.0 (ba45748), unchanged, rusqlite 0.41 bundled. Predictions in PREDICTIONS.md, committed before any toy code.

| # | Prediction | Result |
|---|---|---|
| 1 | One migrator per crate on one database fails | **Holds.** Ontology's `up` applied 20261110000001; uncertainty's `up` then refused: `MissingFile { version: 20261110000001, name: "concept" }`. |
| 2 | No public way to combine migrators | **Holds.** `Migrator` keeps its migrations private. Combining works only by each crate exporting its (file, contents) pairs by hand (`SOURCES`) and the store calling the `#[doc(hidden)]` `Migrator::from_validated` on the concatenation. |
| 3 | A combined migrator applies every crate in version order; a cross-crate foreign key holds | **Holds.** Applied 20261110000001 to 20261110000004 in order; a prediction pointing at a missing estimate failed with `FOREIGN KEY constraint failed`. |
| 4 | Two crates with the same version collide | **Holds**, as a panic: `embedded migrations do not parse: duplicate migration version 20261110000001`. |
| 5 | One schema.json for the whole store | **Holds.** `write_schema` on the combined database named all three crates' tables (540 bytes). |
| 6 | A new migration applies at open with nothing run by hand | **Holds** once the program embeds it: on a database built before the change, the plan held only 20261110000004 and `up` applied only it. |
| 7 | A change shows at rebuild | **Half fails.** Editing an existing migration file rebuilds the crate (`Dirty ontology ... the file ... has changed`). Adding a new file to a migrations directory does not: cargo reported `Fresh sym`, and the rebuilt program still embedded 1 migration. Only after `cargo clean -p sym` did it embed 2. |

## What it means

- Combining crates' migrations into one store works, and one `schema.json` can describe the whole store, but only through migratr's hidden constructor and a hand-kept list of files per crate.
- The real break is prediction 7: a new migration file is silently left out of the build until a clean build. So "a change shows at rebuild with nothing run by hand" does not hold today, and prediction 6 only holds in a build that happened to be clean.
- Both gaps are in migratr: a public way to combine migrators, and `embed!` tracking its directory so a new file triggers a rebuild.

## How to rerun

From this directory: `cargo test -p store -- --nocapture --test-threads=1` runs predictions 1 to 6. For 7: build once, copy `sym/later/20261110000004_prediction.json` into `sym/migrations/`, then `cargo test -p store --test p7_probe -- --nocapture` (prints 1), `cargo clean -p sym`, and run it again (prints 2). Remove the copied file afterwards. The `migratr` path in `Cargo.toml` points at a local migratr checkout at v0.3.0.
