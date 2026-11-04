# AGENTS.md

rannoch is one graph for the Rust projects in our ecosystem. Read `README.md` first, then `docs/architecture.md`, which is the whole picture of the second edition and needs nothing outside this repo.

Rust 1.98, edition 2024, `unsafe_code` forbidden, clippy pedantic. Every other crate or app, ours included, stays outside rannoch.

A test never asserts on wall-clock time: a bound on time proves little and flakes under load. Count what the code does instead (statements, trips to the store) over a few events and over more, so any rise per event fails. The one allowed use of the clock is a guard that only fails a hung test, far past anything a loaded machine takes, so a test that never finishes fails instead of running forever.

The first edition is at tag `v0.4.0.a`, not on main.
