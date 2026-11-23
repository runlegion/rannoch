# rannoch

Rannoch is a Rust library that keeps an app's specialized knowledge current, traceable to where it came from, correctable, and readable as it stood at any time. It is for software that takes in events and the agents that act on what those events say.

The name means walled garden: rannoch is the app's safe space. Each app has its own.

## Status

Version 0.1.0, built crate by crate on `main`.

- A library only: no CLI, no UI, no network service.
- Not for use in wasm in 0.1.

## How it works

An app sends events to rannoch. Each event is a row in `_events`, the log in the app's SQLite file, and its progress is recorded as further events whose `source` is that event, each with a status: 0 when it is accepted, 1 when the queue takes it, 2 when its flow has run, and -1 when it is binned. Nothing in `_events` is updated or deleted.

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
| `rannoch-graph` | The tables an app builds its ontology on, `_nodes` and `_edges`, rows only added. `GraphAppend` writes a step's graph rows with its 2. Reads: `prop`, `props`, `history`, `edges`, `why`, `walk`, which follows named edges out from a node, `search`, which finds nodes by their current text through SQLite's full-text index (FTS5), `describe`, and `Hoods`, which keeps a describe warm. |

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

## What you can rely on

- **An accepted event is in memory until its inbox hands it over.** `Sender::send` gives the event a UUIDv7 id and puts it on its inbox's heap, which has no size limit. When `Config` has an append, each inbox waits `interval` (20 ms for the default inbox under `Config::default`) after each hand-over, then hands its whole heap to the append as one batch, and the events are on disk once that batch commits; with no append, nothing is handed over. A process that dies before the commit loses them.
- **`Events` and `GraphAppend` write a batch whole or not at all,** in one transaction. Rows in `_events` are only added.
- **The queue takes each due event once.** It sends a 1 for it and keeps the events it has taken in memory until their 1 is on disk. If the process dies before an event's 1 is on disk, the event is still due when the queue starts again and is taken under its own id.
- **The hunt resends an event whose 1 has had nothing after it for `window`** (20 minutes by default). It first runs one `hunt` (an hour by default) after the queue starts, then every `hunt`. Its resend is a new event at 0, with the stalled event as its `source` and `"hung"` as its data. It leaves every 1 accepted since the queue started, so a run or job the running queue holds, going, waiting its turn or with its 2 not yet on disk, is not resent, however long it takes.
- **A failed run is retried, with backoff.** The queue sends a retry: a new event with the failed one as its `source`, due after `first_wait`, doubled for each failed attempt before it, until the chain has had `attempts` (10 by default).
- **With `GraphAppend`, an event's 2 and its graph rows commit together,** so a reader never sees a 2 without its rows. Writing the same event's rows again adds none, since every row is keyed by the event that wrote it; a retry or a hunt's resend is a new event, and its rows are added.
- **Reads are decided by ids and stamps.** The current value of a key is the row with the greatest event id, whatever order the events ran in, and there is none when that row is a retraction; as of T reads only rows whose `written` stamp is at or before T; `why` returns the event that wrote the current value and its chain of sources.

## How it fails

- **A hand-over that fails** is logged at `error`, and the batch goes back on the heap ahead of what arrived since; the inbox tries again at its next interval, with no limit. A batch that can never be written therefore stops its inbox: every later hand-over includes it, nothing more from that inbox reaches the disk, and its heap keeps growing. One way to get there is a step whose `$graph` holds an item that is not one of rannoch-graph's ops.
- **The last hand-over at shutdown** is not retried: if it fails, the failure is logged and its events are lost.
- **A crash's resend runs the stalled event's flow on that event's data,** because the queue walks back from the resend to the event it resends.
- **A step that fails** sends the event's -1, its data the step's message, and the queue retries it as above. An event whose `ty` has no flow gets a -1 saying "no processor".
- **After `shutdown`**, `Sender::send` returns `Closed`. `queue.join()` returns only once every sender of the append's signal is dropped, so a signal sender the app keeps makes it wait for good; it also waits for every run and job the queue started.
- **A second process** on the same store is not stopped by rannoch.

## What was measured

All on one developer machine, as recorded on PRs #74 and #76 and issues #65 and #66:

| What | Result | What ran it |
|---|---|---|
| A flow whose step writes nothing, 100,000 events to their 2 | 55,679 to 58,795 events/s | 55,679: `toys/graph/src/bin/flow.rs` in `noop` mode on the `toys/graph` branch, which is in the repo (PR #74); 57,011 to 58,795: the `noop` mode of the copy in the next row, which is not |
| The same flow, each step giving three node rows and one edge through `GraphAppend` | 48,112 to 49,611 events/s over 6 runs, 15% to 18% below the step that writes nothing in the same runs; every 2 had its rows | a copy of that bench pointed at `GraphAppend`; the copy is not in the repo |
| A `describe` of a two-level shape at 100,000 nodes and 499,876 edges, 32 nodes read, median, release build, file store | 0.30 to 0.32 ms cold, 13 to 15 µs warm | a timing run of the toy's P9a against the crate (PR #76), whose harness is not in the repo; the committed test `fr_rannoch_123_c9` asserts 5 ms or less cold, and measured 1.58 ms under `cargo test` |

## What the app owns

- One process opens the store and runs rannoch over it. Keeping a second process from doing the same is the app's job.
- Rannoch keeps one app's file on one machine. Moving knowledge between machines is the app's.
- What nodes, keys and edges mean. rannoch-graph stores rows and reads them; it has no ontology and checks no schema.
- Where logs go. Rannoch emits through the `log` crate at `error` and `debug`.

## Docs

Each crate's documentation is in its source: `cargo doc --workspace --no-deps --open`.
