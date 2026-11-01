# rannoch architecture

This page describes the second edition of rannoch as the code at this commit builds it. Every part is labelled **built** or **designed, not built**. A built part does what this page says; a designed part does nothing yet and has its own section at the end. The first edition is the tag `v0.4.0` (`v0.4.0.a` holds the same content); it is a reference for working code only, never for what rannoch is.

How to use what is built: [store.md](store.md) and [inbox.md](inbox.md). The design for flows: [flows.md](flows.md).

## What rannoch is

rannoch is one app's graph store, in one SQLite file, linked into the app's own process as a Rust library. It has no executable, no CLI, no server and no network. Nothing it does reaches another machine; sync and transport belong to the app.

It keeps every change as an event in an append-only log and never changes an event once written. The tables are built from the log. Every fact carries its point in time (when it happened), the window in which the store knew it, and a valid time (when it was true in the world). Reads can be asked as of each.

## The crates

| Crate | Owns | State |
|---|---|---|
| `rannoch-ontology` | Classes, relations, properties, value types and history keys; checking a declaration against what is declared and a value against its type. No I/O. | Built |
| `rannoch-cloudevents` | Reading a CloudEvents 1.0 body into the fact it states: a node, a link, a link's ending or correction, a declaration, or no fact. Event ids and dates. No I/O. | Built |
| `rannoch-store` | The graph file: the log, the ontology tables, fill, hearings, amendments and renames, the tables kept for speed, reads, the inbox's places and work rows, fill retries, `copy_to`. | Built |
| `rannoch-queue` | The inbox in memory: accepting events, the heartbeat that commits them, producers' positions, handing events to workers, failures, subscribers. | Built |
| `rannoch-probe` | Measuring a host's memory and how fast a directory takes durable writes. | Built |
| `rannoch-worker` | Reading a worker file and checking it against the ontology. | Designed, not built |
| `rannoch-harness` | Running worker files. | Designed, not built |
| `rannoch-processor` | The contract every step meets. | Designed, not built |
| `rannoch-processor-*` (after, approve, bin, branch, count, emit, host, lookup, onnx, store, wait) | One kind of step each. | Designed, not built |

The root package `rannoch` (`src/`) is the first edition, kept as a reference. Apps depend on `rannoch-store` and `rannoch-queue`.

## How an event moves

```
app ──accept──▶ Queue (memory) ──heartbeat, every 20 ms──▶ _events (the log)
                                                              │
                                              fill (worker rannoch.fill)
                                                              ▼
                              _hearings, _aliases, _ends, _endings, _amendments, _renames, _open
                                                              │
                                           tables kept for speed: class:<name>, relation:<name>,
                                           _seen
                                                              │
                              reads ◀────────────────────────┘     workers ◀── next/done/failed
```

1. **Accept.** `Queue::accept` or `accept_from` reads the body as CloudEvents 1.0 and checks the fact against the ontology the queue took when it opened. A refused body is refused at once with its reason and is not held. An accepted body is held in memory with a ticket. Declarations are refused here; they go through `Graph::declare`.
2. **Commit.** The heartbeat (at the interval passed to `Queue::open`, `Queue::INTERVAL` = 20 ms by default, or on `flush`, `close` or drop) commits everything held in one transaction, with a savepoint per event. An event that fails at commit rolls back alone and is reported as a `Failure::Event`. A producer's position commits in the same transaction as its events; after a producer's event fails, its later events in the batch are not committed. The log gives each event a place (`Cursor`) that is never reused, and a log id (`Event.id`, a fresh `UUIDv7`).
3. **Fill.** After each commit, the queue's heartbeat runs `Graph::fill`, which folds the new events into the history tables and the tables kept for speed (details below). The queue then moves its committed watermark and tells its subscribers.
4. **Hand out.** `Queue::next(worker)` hands each worker the next committed event at or below the watermark. Every worker sees every event, including the `rannoch.closed.<relation>` events fill appends (see Links).
5. **Read.** Reads go through any handle on the file. They read the history tables and `_seen`, never the queue's memory.

What a crash loses: events accepted since the last heartbeat, and the batch whose commit was in flight. Nothing committed is lost. Every commit is durable: the file runs in WAL mode with `synchronous=FULL` and `fullfsync=ON`, so on macOS each commit is flushed to the device.

## The tables

All in one file. rannoch's own tables start with `_`.

| Table | Holds |
|---|---|
| `_events` | The log: place, log id, body. |
| `_ontology` | Every declared class and relation. |
| `_hearings` | Every hearing of a node or link: its identity, entity, key (and a link's target key), event, place, source, valid time, point, integer key, record time, properties, and its known window (`known`, `until`) and the place of the event it amends (`amended`). |
| `_aliases` | Other events that arrived with a hearing's identical content. |
| `_ends`, `_endings` | A link's latest conclusion (ended or found wrong), and every ending at its point. |
| `_amendments` | Each amendment by target place, point and event. |
| `_renames` | Each rename: entity, old key, new key, point, event. |
| `_open` | Links waiting for a node that has not arrived. |
| `_seen` | Per node and link: first and last heard, first recorded, first true, count. |
| `class:<name>` | One row per node of the class, one column per property. |
| `relation:<name>` | One row per link with a version known now (endings are read from `_endings`, not from this table), with foreign keys to both classes' tables. |
| `_places`, `_work` | The inbox: each worker's place, and a row for each event in progress or failed, with its reason and try count. |
| `_positions` | Each producer's position in its source. |
| `_fill` | Fill's try limit (3 unless set). |
| `_check` | The check value of the app's key. |

The tables kept for speed are `class:<name>`, `relation:<name>` and `_seen`; fill keeps them current. Reads answer from `_hearings`, `_aliases`, `_ends`, `_endings` and `_seen`. The class and relation tables serve the app's own SQL. `find` returns `None` for a key renamed away. A link of the wrong classes cannot exist in a relation table, since both ends are foreign keys.

## The bitemporal model

Three times are kept, and they are different things:

- **Its valid time**: when the fact was true in the world, as the writer states it (`ValidTime`): unknown, a month, or a day. Unknown is never read as now.
- **Its point on the axis**: when the fact happened. It is the time the writer supplied (`data.time`), or, without one, the record time of the store that first recorded the event, carried in the millisecond of its CloudEvent id. A class can order its history by an integer the writer supplies instead.
- **Its known window**: from when the store knew it (`known`) until an amendment superseded it (`until`). A hearing written directly is known from its own point and has no `until`. A hearing made by an amendment is known from the later of the amendment's point and the event's own point; the hearing it supersedes gets `until` set to that point.

Reads use them like this:

- **Now** (`find`, `hearings`, `seen`, `link_seen`, `link_end`): hearings with no `until`, the newest on the class's axis. A version that arrives late answers at its own point and becomes current only if it is the newest on the axis.
- **As of a valid date, as known at a point** (`find_as_of(class, key, date, known_at)`): the version valid on that date, among hearings known now or, given `known_at`, known at that point (`known` at or before it, `until` after it or absent). `link_known_at` reads a link as known at a point. An amendment or rename made later is not in the answer.
- **On the axis** (`state_at`, `link_state_at`): the version that held at a point on the axis.

**Identity.** A hearing's identity is its content together with its source, hashed with BLAKE3 under the app's 32-byte key. Identical content from the same source is one hearing: a repeat under the same CloudEvent id records nothing; a repeat under a new id becomes an alias, an occurrence of the hearing at the repeat's own point. The key is held in memory only; the file keeps a check value, and opening it under another key returns `Error::KeyDiffers`.

**Links.** A link's endings sit beside its hearings at their own points. A live hearing at a later point reopens a link that ended. A correction records that the link was wrong, which is not an ending. A link that names a node that has not arrived waits in `_open`. When the node arrives, fill moves the link into its relation's table and appends one event to the log: type `rannoch.closed.<relation>`, source `rannoch`, a bare `UUIDv7` id, subject the link's from-key, data `{from, to, valid}`. Every worker is handed these events; a worker that does not care about them closes them as done.

## Amend and rename

**Amend is rare.** It corrects a mistake: an event that was written wrong. It is not how an app records change. A value that changes is a new event, a new hearing at its own point, and the store's history keeps both. An app that amends routinely is modelling change the wrong way.

An amendment is an event of type `rannoch.amended`. Its subject is the log id (`Event.id`) of the event it amends; its data is a JSON Merge Patch (RFC 7386) of that event. Its point is the record time in its own CloudEvent id.

When fill reaches an amendment, it records it in `_amendments`, then derives the target's hearings again: it applies every amendment of the target in point order, then event id, and wherever the event reads differently, it writes a new hearing known from that amendment's point and closes the hearing before it at that point. The amended event in the log is never changed. Read as known before the amendment, the event reads as written.

**A rename is an amendment that patches the subject.** Fill records it in `_renames` as a mapping from the old key to the new key, known from the amendment's point. Every event that names the old key is heard again through the mapping: the node's own events and every link to or from it. Link endings and corrections are copied onto the new key; their old-key rows stay, so reads as known before the rename are unchanged. A link, ending or node event arriving after the rename that names the old key lands on the new key. A second rename maps on from the new key.

Fill refuses an amendment whose id is not `<uuid>:<fact>`; that has no subject; that names an event not in the log; that names an event which made no hearing of its own (a declaration, an app's own event, a link ending or correction, content identical to an earlier hearing, or any event that states no fact); that amends an event of an integer-keyed class; or whose point is before the target's record time, the target's own point, or the latest amendment already applied to it. Fill also refuses an amendment whose patch leaves the event invalid against the ontology or stating no node or link. A refused amendment is a failed try (below); it never stops fill.

While the store holds no rename, fill does no rename lookup for nodes, links or endings. It checks once per batch, and again after each amendment lands.

## Fill

Fill is a worker on the inbox, named `rannoch.fill` (`FILL`), with its place in `_places`.

- **Batches.** Each batch takes its own write transaction, reads fill's place inside it, fills events until its time budget is spent, and commits its events together with fill's new place. The first batch's budget is 25 ms (`FILL_BUDGET`); each next batch runs four times longer, up to 1 s (`FILL_BUDGET_MAX`). The budget is checked after each event, so every batch commits at least one event, and a crash loses at most the batch in flight.
- **One savepoint per batch.** When an event in a batch fails, the batch rolls back to its savepoint and is filled again one event at a time, so the failed event fails alone and the rest commits.
- **Retries.** A failed event gets a `_work` row for `rannoch.fill` with its reason and its try count, raised by one. Each later fill tries again the failed events still under the try limit, besides filling the new events; a fill reads its list of retries before it fills new events, so an event that fails in one fill is not tried twice in it. An event that succeeds on retry reads exactly as if it had filled the first time.
- **Stopping.** At the try limit (3, set with `set_try_limit`) an event is no longer tried. `stopped()` lists those events with their last reasons; `retry(cursor)` resets one's tries. The event in the log never changes.

Every error inside one event's fill counts as that event's try, a storage fault included. A fault outside any one event's fill (beginning a batch, loading the ontology, reading the log, writing tries or fill's place, committing) ends that fill with an error. Batches already committed stay; the queue reports the error as `Failure::Fill`.

`FILL` is reserved: an app's worker must not take the name `rannoch.fill`.

## The inbox

A worker is a name with a place in `_places`: the last event handed out to it. Events after the place are waiting. Of the events at or before it, only those in progress or failed have a `_work` row.

- `next(worker)` takes the worker's oldest event an earlier run left in progress, if any; otherwise the first event after its place, at or below the committed watermark, marking it in progress. A worker joins at the start of the log on its first `next`.
- `done` deletes the event's row; `failed` keeps it with the reason. The inbox retries only fill's failures, never an app worker's.
- Each worker sees every committed event, in log order, independent of every other worker.

## The processed mark

`processed()` is the lowest point every worker has finished: no event at or before it is waiting or in progress for any worker. A failed event counts as finished. Fill is a worker, so the mark never passes what fill has not reached. With no workers it equals the committed watermark, and it never passes it. It deletes nothing. `remove_worker` deletes a worker's place and rows and releases the mark it held.

## copy_to

`copy_to(path)` runs SQLite's `VACUUM INTO` inside one read transaction and returns the cursor the copy holds, read from the copy itself. It takes no write lock and records nothing in the store. It refuses a path that exists and removes its file if the copy fails. The copy is not flushed durably; the snapshot tool that does that is designed in #154 and not built.

## Limits

- **No lookup from a CloudEvent id to a log id.** Issue #164 adds one that returns the first log id an event's source and id arrived under; it is not built, and it is for tracking down the event a mistake came from, and nothing else. An app that will amend an event keeps its log id from `commit`'s result, from `events().since(...)`, or from the `Event` a worker takes. A queue app gets a `Ticket` from `accept`, not an `Event`, so it has the last two.
- **At-least-once, no dedupe by CloudEvent id.** The log mints a fresh log id for every event, and the queue accepts a body it has accepted before. A repeat under the same CloudEvent id records nothing in the graph. A repeat under a new id is an alias of its hearing at the repeat's own point; if that point is newer than the current version, the repeated (older) content becomes current through it. The log holds both events and every worker is handed both, so workers must be idempotent.
- **The queue owns its graph.** `Queue::open` takes the `Graph` by value. An app with a queue open reads, and calls `stopped`, `retry`, `set_try_limit` and `copy_to`, through a second handle (`rannoch_store::open`).
- **An event a worker takes and never closes stays in progress** until the process restarts, and holds the processed mark below it until it is closed.
- **Sources.** A source that starts with `rannoch:` is read back percent-decoded without the prefix; any other source is stored exactly as given.
- **The queue takes the ontology once.** Declarations made after `Queue::open` are not seen by that queue; close it, declare, and open it again.
- **A removed property stays in its class table column** for as long as the row exists. A later write that omits the property leaves the old value; only deleting the row clears it. `find` reads from `_hearings` and answers correctly.
- **One history line.** The history tables other than `_amendments` and `_renames` have a `line` column, and every read and write uses the line `main`.
- **Copies are not durable on return.** See `copy_to`.

## Designed, not built

None of the following exists in code. Each crate is an empty scaffold with one doc comment.

### Flows

Worker files (`rannoch-worker`), the harness (`rannoch-harness`) and the steps (`rannoch-processor`, `rannoch-processor-*`). The design: each worker is a file of JSON lines in a folder; the harness runs each as an inbox worker, reads guards, hands each row to its processor, follows `next` and `else`, and commits what a step did together with the worker's place. How a step's effect commits together with the worker's place is not designed, and no public call does it today. [flows.md](flows.md) is the design for mark-up, with its open questions.

### Purge (#148)

Erasing a fact from the store and its WAL, with a standing rule that refuses it coming back.

### Trim

Retention declared per class, such as dropping raw events once processed. No issue is filed.

### Relay (#151)

Neighbourhoods kept warm outside the store.

### Snapshot tool (#154)

A durable copy of the store, built on `copy_to`, outside the engine.
