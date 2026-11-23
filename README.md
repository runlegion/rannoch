# rannoch

Rannoch is a Rust library that keeps an app's specialized knowledge current, traceable to where it came from, correctable, and readable as it stood at any time. It is for software that takes in events and the agents that act on what those events say.

The name means walled garden: rannoch is the app's safe space. Each app has its own.

## Status

Version 0.1.0, built crate by crate on `main`.

- A library only: no CLI, no UI, no network service.
- Not for use in wasm in 0.1.

## How it works

An app sends events to rannoch. Each event is a row in `_events`, the log in the app's SQLite file, and its progress is recorded as further events whose `source` is that event, each with a status: 0 when it is accepted, 1 when the queue hands it to its flow, 2 when its flow has run, and -1 when it is binned. Nothing in `_events` is updated or deleted.

The queue hands each due event at status 0 to the flow for its `ty`. A flow is a JSON document of steps; each step calls one or more of the app's actions, and its result can send further events, branch on a value, or wait for another event. When the app's append is rannoch-graph's `GraphAppend`, the graph rows a step gives are written in the same transaction as the event's 2, so an event that has its 2 has its rows.

## The crates

| Crate | What it does |
|---|---|
| `rannoch-core` | The `Event` and its `Status`, and `Rannoch`: opened with a `Config`, it applies the store's pending migrations, then passes each event sent through a `Sender` to the inbox for its `ty`; each inbox hands its batches to the `Append`. `migrator()` is every crate's migrations as one migrator. |
| `rannoch-store` | Opens the app's SQLite file against that migrator, and reports pending migrations without applying them. `Store` is core's `Storage`: it applies them, and checkpoints the WAL once a second while it lives. |
| `rannoch-events` | Owns `_events`. `Events` is core's `Append`: it writes each batch in one transaction, then signals the queue. |
| `rannoch-queue` | Takes the due events at status 0, sends a 1 for each, and hands each to its processor; retries a failed run while its chain has attempts left, resends a run a crash dropped, and sends a -1 for an event whose `ty` has no flow. |
| `rannoch-processor` | Runs the app's flow documents: steps that call the app's `Action`s, and nexts, branches and waits. `compile` checks the documents when the app builds. |
| `rannoch-processor-agent` | A step that calls an agent the app reaches through `Agent`, checked against what the agent takes and gives. |
| `rannoch-processor-onnx` | Steps that run a local ONNX model: `Infer`, `Embed` and `Classify`, through ort, or through tract with the `tract` feature. |
| `rannoch-processor-retrieve` | A step that queries one of the app's outside sources through `Source`. |
| `rannoch-graph` | The tables an app builds its ontology on, `_nodes` and `_edges`, rows only added. `GraphAppend` writes a step's graph rows with its 2. Reads: `prop`, `props`, `history`, `edges`, `why`, `describe`, and `Hoods`, which keeps a describe warm. |

## Starting rannoch in an app

The store opens a connection only once no migration is pending, so the order matters:

1. Make the store with `Store::new(path, rannoch_core::migrator())` and apply its pending migrations through `Storage`: `pending`, then `apply`, the calls `Rannoch::open` makes before anything else.
2. Open each connection with `rannoch_store::open(path, &migrator)`, which returns `Opened::Ready` with the connection once nothing is pending.
3. Make the processor with `Processor::new`, from the app's flows as JSON, its actions and a connection.
4. Make a channel with `std::sync::mpsc::channel`. The append signals on its sender, and the queue waits on its receiver.
5. Make the append: `Events::new(conn, signal)`, or `GraphAppend::new(conn, signal, processor.writes())` when steps give graph rows, with `with_hoods` to keep describes warm.
6. Open rannoch with `Rannoch::open`, its `Config` holding the store and the append.
7. Start the queue with `Queue::start`, over a connection, the receiver, `rannoch.sender()`, the processor and its `Settings`.

Producers send events with `Sender::send`. To stop, drop any signal sender the app kept, call `rannoch.shutdown()`, then `queue.join()`.

[`crates/rannoch-graph/tests/scenarios.rs`](crates/rannoch-graph/tests/scenarios.rs) wires steps 2 to 7 against a file store and runs 500 events through a flow whose step gives graph rows. It applies the migrations by opening rannoch once with only its store, and writes its events straight into `_events` with forgr's factory instead of sending them.

## What the app owns

- One process opens the store and runs rannoch over it. Keeping a second process from doing the same is the app's job.
- Rannoch keeps one app's file on one machine. Moving knowledge between machines is the app's.
- What nodes, keys and edges mean. rannoch-graph stores rows and reads them; it has no ontology and checks no schema.
- Where logs go. Rannoch emits through the `log` crate at `error` and `debug`.

## Docs

Each crate's documentation is in its source: `cargo doc --workspace --no-deps --open`.
