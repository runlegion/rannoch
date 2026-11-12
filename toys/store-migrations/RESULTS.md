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

## Round 2: the app's build.rs

Run 2026-11-10. Each crate exposes `MIGRATIONS_DIR`; the `app` crate lists the three as build-dependencies, and its `build.rs` reads every directory, generates one migrator into `OUT_DIR` and prints `cargo:rerun-if-changed` for each directory.

| # | Prediction | Result |
|---|---|---|
| 8 | No cycle | **Holds.** The three crates build as build-dependencies of the app. |
| 9 | One history from build.rs | **Holds.** The generated migrator applied 20261110000001 to 20261110000003 in order. |
| 10 | A new file is picked up without a clean build | **Holds.** After copying the predictions migration into `sym/migrations`, cargo reported `Dirty app ... the file sym/migrations has changed`, reran the build script, and the migrator applied 20261110000001 to 20261110000004. Removing the file reverted it to three, again with no clean. |

Round 2 closes round 1's real gap: watching the directories from the app's `build.rs` picks up a new migration file, which `embed!` alone does not. One gap stays: the generated code still calls migratr's `#[doc(hidden)]` `Migrator::from_validated`, the only way to build a migrator from collected files.

Rerun round 2: `cargo test -p app -- --nocapture` prints three versions; copy `sym/later/20261110000004_prediction.json` into `sym/migrations/` and run it again (four, no clean); remove the file afterwards.

## Round 3: only migratr's public API

Run 2026-11-10. The `app3` crate's `build.rs` clears `app3/migrations/` (keeping `schema.json`), copies every crate's migration files into it, and prints `cargo:rerun-if-changed` for each crate's directory. `app3` uses the public `migratr::embed!("migrations")`. The copied directory is generated and ignored by git.

| # | Prediction | Result |
|---|---|---|
| 11 | Public API only | **Holds.** `embed!` on the copied directory applied 20261110000001 to 20261110000003 in order. Nothing hidden is called. |
| 12 | A new file still shows without a clean build | **Holds.** After adding the predictions migration to `sym/migrations`, the next test applied 20261110000001 to 20261110000004. |
| 13 | A removed file disappears too | **Holds.** After removing it, the next test applied three again. |
| 14 | One schema.json for the whole store beside it | **Holds.** `write_schema` into the copied directory named every crate's tables, including `prediction` once it existed, and `embed!` ignored the file. A run with nothing changed reported `Fresh app3`: writing `schema.json` there does not trigger a rebuild. |

So the whole shape works on migratr as it is, with no change to migratr: each crate keeps its own migrations directory and exposes where it is; the app's `build.rs` gathers them into one directory and watches each source; the public `embed!` builds one migrator; one `schema.json` describes the whole store for the structs, forgr and `check!`.

Rerun round 3: `cargo test -p app3 -- --nocapture` prints three versions and three tables; copy `sym/later/20261110000004_prediction.json` into `sym/migrations/` and run it again (four, with `prediction`); remove the file and run again (three).

## Round 4: build.rs writes schema.json, and the migrations directory is committed

Run 2026-11-10. The `app4` crate's `build.rs` copies every crate's migrations into `app4/migrations/` (committed), writing a file only when its bytes differ and removing files no crate has any more, then applies them to an in-memory database with migratr's public `load_dir` and `up` (migratr and rusqlite as build-dependencies), and writes `schema.json` there with `write_schema`, again only when its bytes differ. `app4` embeds the directory with the public `embed!`.

| # | Prediction | Result |
|---|---|---|
| 15 | build.rs writes schema.json | **Holds.** After the first build, `app4/migrations/` held the three crates' files and a `schema.json` naming concept, estimate and symbol. |
| 16 | A new migration updates schema.json with no clean build | **Holds.** After copying the predictions migration into `sym/migrations`, the next build added the file to `app4/migrations/` and `schema.json` named prediction; git showed the new file and `M schema.json`, a change to review and commit like any other. |
| 17 | No rebuild loop | **Holds.** A second build with nothing changed reported `Fresh app4`. |
| 18 | Committed and stable | **Holds.** After committing the first build's output, an unchanged build left git with no diff; removing the predictions file and building again returned the directory and `schema.json` to the committed bytes. |

So the schema lives in the app, committed: the app's migrations directory and its `schema.json` are written by the app's `build.rs` from every crate's migrations, the same method for rannoch's crates and the app's, and forgr's `schema!` in the app reads a `schema.json` that is always current and always in the repo. 17 of 18 predictions held across four rounds.

Rerun round 4: `cargo build -p app4` writes `app4/migrations/`; copy `sym/later/20261110000004_prediction.json` into `sym/migrations/` and build again (the file and prediction appear, git shows the change); remove it and build again (git clean).

## Round 5: with forgr

Run 2026-11-11 with forgr 0.1.0 from crates.io. `app5` is round 4's `app4` plus forgr as its README sets it up: `forgr::schema!("migrations/schema.json")` inside `pub mod forgr` behind a `testing` feature, and one query, `SYMBOL_PATHS`, in `src/lib.rs`. `user` depends on `app5` with `testing` as a dev-dependency and has no forgr dependency.

| # | Prediction | Result |
|---|---|---|
| 19 | schema! reads the schema.json build.rs writes | **Holds.** After `cargo clean -p app5`, `cargo test` ran `build.rs` before `schema!` expanded; `symbol::factory().id("s1").path("a.rs")` inserted into `empty_store()`. |
| 20 | Another crate's tests use it | **Holds.** `user`'s test built a `symbol` row through `app5::forgr`, with no forgr dependency of its own. |
| 21 | A new migration reaches forgr with no clean build | **Holds.** After adding the predictions migration to `sym/migrations` and a test using `prediction::factory()` linked to a `symbol` and an `estimate`, the next `cargo test` compiled and passed it. |
| 22 | The SQL check passes and catches drift | **Holds.** forgr emitted `forgr::forgr_sql_matches_schema`, which passed on `SELECT id, path FROM symbol` and failed on `SELECT id, filename FROM symbol` with `app5/src/lib.rs:10: no such column: filename`. |

So the order the operator doubted holds with forgr in the loop: the app's `build.rs` writes `schema.json` before anything in the app compiles, `schema!` expands against it, and every other crate's tests compile after the app. 21 of 22 predictions held across five rounds.

Rerun round 5: `cargo clean -p app5 && cargo test -p app5 -p user -- --nocapture`. For 21, copy `sym/later/20261110000004_prediction.json` into `sym/migrations/` and add a test in `user/tests/` using `app5::forgr::prediction::factory()`. For 22, change `path` to `filename` in `SYMBOL_PATHS` and run `cargo test -p app5`.
