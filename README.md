# rannoch

rannoch is a local graph store for one app. It keeps every change as an event and never changes an event once written. Facts carry two times: when they were true, and when the store learned them, so any read can be asked "as known then". Events arrive as [CloudEvents 1.0](https://cloudevents.io) through an inbox, which commits them and hands each one to the app's workers.

It is a Rust library on SQLite. An app links it and runs it in its own process. There is no executable, no CLI, no server and no network: sync, sockets and anything that moves data between machines belong to the app.

## What is built

| Crate | What it does | State |
|---|---|---|
| `rannoch-store` | The graph file: the event log, the ontology, fill, reads, amendments, fill retries, `copy_to` | Built, crash-tested |
| `rannoch-queue` | The inbox: accepts CloudEvents, commits them every 20 ms, hands each committed event to every worker, survives crashes | Built, crash-tested |
| `rannoch-cloudevents` | How a rannoch fact reads as a CloudEvent | Built |
| `rannoch-ontology` | Classes, relations, properties and their types | Built |
| `rannoch-probe` | Measures a host's memory and how fast it takes durable writes | Built |
| `rannoch-worker` | Worker files (JSON lines in a folder) | **Designed, not built** |
| `rannoch-harness` | Runs worker files: guards, steps, hand-offs | **Designed, not built** |
| `rannoch-processor` and every `rannoch-processor-*` | The steps a worker file can use | **Designed, not built** |

What works today: an app declares its ontology, accepts CloudEvents through the queue, and writes its workers in its own code, calling `next`, then `done` or `failed`. Crash tests killed the process every 50-300 ms through 300,000 events and through a run of amendments: nothing was lost, doubled or left unfilled (`toys/live/RESULTS.md` on the `chore/live-test` branch).

Worker files, the harness and the processors are a design. [docs/flows.md](docs/flows.md) describes it so it can be marked up before it is built.

## Depend on it

Pin the tag. Main keeps moving; `v0.5.0` is the version to build against.

```toml
[dependencies]
rannoch-store = { git = "https://github.com/runlegion/rannoch", tag = "v0.5.0" }
rannoch-queue = { git = "https://github.com/runlegion/rannoch", tag = "v0.5.0" }
```

`rannoch-store` re-exports from `rannoch-ontology`: `Class`, `Declaration`, `Ontology`, `Relation`, `Value`, `ValueType`; and from `rannoch-cloudevents`: `Date`, `EventId`, `NodeRef`, `ValidTime`. A worker that parses event bodies itself also depends on `rannoch-cloudevents` (its `read` and `attributes`) or on `serde_json`.

Rust 1.98, edition 2024.

## Start here

1. [docs/store.md](docs/store.md): open a store, declare the ontology, write, read, amend, copy. Runnable: `cargo run -p rannoch-store --example store`.
2. [docs/inbox.md](docs/inbox.md): the queue, producers, and workers in your own code. Runnable: `cargo run -p rannoch-queue --example worker`.
3. [docs/flows.md](docs/flows.md): the flow-writing guide. A design, not yet built.

4. [docs/architecture.md](docs/architecture.md): how the second edition is built, every part labelled built or not built, and its limits.

The name: Rannoch, the geth home world in Mass Effect.
