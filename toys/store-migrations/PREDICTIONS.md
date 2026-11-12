# Store migrations toy: predictions

Written 2026-11-10, before any toy code exists. Against migratr v0.3.0 (ba45748), unchanged.

The question: can several crates each embed their own migratr migrations, and the store combine them into one database, one ordered history and one schema.json, with a change showing at rebuild and at open and nothing run by hand?

The shape, from the operator's example: three crates. `ontology` (a table that never changes), `uncertainty` (an engine's tables), and `sym` (SCIP-like tables, which later gains a predictions table linked to uncertainty's tables). A fourth part, the store, opens one SQLite database and applies all of them.

Read before predicting: migratr's `Migrator` holds its migrations privately; `embed!` expands to `Migrator::from_validated(&[(file, include_str!(path)), ...])`, a `#[doc(hidden)]` constructor; versions are 14-digit UTC timestamps; the ledger is keyed by version; `status` and `up` refuse a ledger row with no file (`MissingFile`); `write_schema(exec, dir)` writes `schema.json` from `sqlite_master`.

## Predictions

1. **One migrator per crate on one database fails.** Running ontology's migrator, then uncertainty's, on the same database: the second refuses with `MissingFile`, because the ledger holds ontology's versions and uncertainty's migrator has no file for them. Confidence 0.85.
2. **There is no public way to combine migrators.** Combining needs each crate to hand the store its raw (file, contents) pairs and the store to build one `Migrator` through the hidden `from_validated`. Confidence 0.8.
3. **A combined migrator applies all three crates' migrations in version order**, so sym's predictions table, whose foreign key points at uncertainty's table and whose version is later, applies after it and the foreign key holds. Confidence 0.7.
4. **Two crates with the same version collide** with `DuplicateVersion` when combined. Confidence 0.9.
5. **One schema.json describes the whole store**: `write_schema` on the combined database lists every crate's tables in one file. Confidence 0.75.
6. **A change shows at open with nothing run by hand**: after sym gains its predictions migration and the program is rebuilt, opening a database built before the change applies only the new migration. Confidence 0.85.
7. **A change shows at rebuild**: editing a crate's migration file makes cargo rebuild that crate, because `embed!` uses `include_str!`. Confidence 0.9.

## Round 2: the app's build.rs

Written after round 1's results, before any round 2 code. The shape (operator, 2026-11-10): not the store's build, "the app build. so all in one". Each crate exposes where its migrations directory is. The app lists those crates as build-dependencies; its `build.rs` reads every directory, generates one combined migrator into `OUT_DIR`, and marks each directory `rerun-if-changed`.

8. **No cycle.** The three crates build as build-dependencies of the app. Confidence 0.9.
9. **One history from build.rs.** The generated migrator applies all three crates' migrations in version order. Confidence 0.8.
10. **A new file is picked up without a clean build.** Adding a migration file to `sym/migrations` reruns the app's build script, and the app's migrator then holds the new migration. Confidence 0.7.

## Round 3: only migratr's public API

Written after round 2's results, before any round 3 code. Round 2's generated code calls the hidden `Migrator::from_validated`. Round 3 tries to need nothing hidden: the app's `build.rs` copies every crate's migration files into one directory inside the app crate (generated, not committed), and the app uses the public `migratr::embed!` on that directory.

11. **Public API only.** `embed!` on the copied directory gives one history in version order. Confidence 0.75.
12. **A new file still shows without a clean build.** Adding a file to a crate's directory reruns `build.rs`, which rewrites the copied directory, and the app recompiles, so `embed!` sees the new file. Confidence 0.65.
13. **A removed file disappears too**, because `build.rs` clears the copied directory before writing it. Confidence 0.8.
14. **One schema.json for the whole store lives beside it**: `write_schema` into the copied directory writes one `schema.json`, which `embed!` ignores. Confidence 0.8.

## Round 4: build.rs writes schema.json, and the migrations directory is committed

Written after round 3's results and the operator's correction (forgr reads a committed schema.json in the crate that owns the store; "the crate is a crate and should use the same methods"), before any round 4 code. The app's migrations directory is committed, like Cargo.lock. Its `build.rs` copies every crate's migrations into it, applies them to an in-memory database through migratr's public `load_dir` and `up` (migratr and rusqlite as build-dependencies), and writes `schema.json` there with `write_schema`.

15. **build.rs writes schema.json.** After a build, the app's `schema.json` names every crate's tables. Confidence 0.75.
16. **A new migration updates schema.json with no clean build.** Adding a file to a crate's directory reruns `build.rs`, and `schema.json` then names the new table. Confidence 0.75.
17. **No rebuild loop.** A second build with nothing changed reports the app `Fresh`. Confidence 0.8.
18. **Committed and stable.** A build with nothing changed leaves the committed directory, `schema.json` included, byte for byte the same, so git shows no diff. Confidence 0.7.

## Round 5: with forgr

Written after round 4's results, before any round 5 code. The operator's doubt: with forgr, would the tests exist before the schema, and would order hold? `app5` is round 4's `app4` plus forgr 0.1.0 from crates.io, set up as forgr's README says: `forgr::schema!("migrations/schema.json")` in `app5`'s library, inside `pub mod forgr` behind a `testing` feature. `app5` holds one query in `src/`. A second crate, `user`, depends on `app5` with `testing` as a dev-dependency and has no forgr dependency of its own.

19. **schema! reads the schema.json build.rs writes.** In a build from no prior target output, `app5`'s `build.rs` writes `schema.json` before `schema!` expands, and a factory for `symbol` inserts into forgr's empty test store. Confidence 0.75.
20. **Another crate's tests use it.** `user`'s test builds a `symbol` row through `app5::forgr` with no forgr dependency. Confidence 0.8.
21. **A new migration reaches forgr with no clean build.** After adding the predictions migration to `sym/migrations`, the next `cargo test` compiles a test that uses `prediction::factory()`, linked to a `symbol` and an `estimate`. Confidence 0.7.
22. **The SQL check passes and catches drift.** forgr's emitted check passes on `app5`'s query, and fails once the query names a column the schema lacks. Confidence 0.7.
