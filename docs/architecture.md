# rannoch architecture

This page describes the second edition of rannoch as the code at this commit builds it. Every part is labelled **built** or **designed, not built**. A built part does what this page says; a designed part does nothing yet and has its own section at the end. The first edition lives at the tag `v0.4.0.a`, not on main; it is a reference for working code only, never for what rannoch is.

How to use what is built: [store.md](store.md) and [inbox.md](inbox.md). The design for flows: [flows.md](flows.md).

## What rannoch is

rannoch is one app's graph store, in one SQLite file, linked into the app's own process as a Rust library. It has no executable, no CLI, no server and no network. Nothing it does reaches another machine; sync and transport belong to the app.

It keeps every change as an event in an append-only log and never changes an event once written. The tables are built from the log. Every fact carries three times: when it became known (the clock in its CloudEvent id), its point on the axis its class or relation declares, and a valid time (when it was true in the world). Reads can be asked as of each.

## The crates

| Crate | Owns | State |
|---|---|---|
| `rannoch-ontology` | Classes, relations, properties, value types and history keys; checking a declaration against what is declared and a value against its type. No I/O. | Built |
| `rannoch-cloudevents` | Reading a CloudEvents 1.0 body into the fact it states: a node, a link, a link's ending or correction, a declaration, or no fact. Event ids and dates. No I/O. | Built |
| `rannoch-store` | The graph file: the log, the ontology tables, fill, hearings, amendments and renames, the tables kept for speed, reads, the inbox's places and work rows, fill retries, `copy_to`. | Built |
| `rannoch-queue` | The inbox in memory: accepting events, the heartbeat that commits them, producers' positions, each worker's cursor and the group commit that saves them, lag, failures, subscribers. | Built |
| `rannoch-probe` | Measuring a host's memory and how fast a directory takes durable writes. | Built |
| `rannoch-worker` | Reading a worker file and checking it against the ontology. | Designed, not built |
| `rannoch-harness` | Running worker files. | Designed, not built |
| `rannoch-processor` | The contract every step meets. | Designed, not built |
| `rannoch-processor-*` (after, approve, bin, branch, count, emit, host, lookup, onnx, store, wait) | One kind of step each. | Designed, not built |

The root `Cargo.toml` is a workspace with no package of its own. Apps depend on `rannoch-store` and `rannoch-queue`.

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
4. **Hand out.** `Queue::next(worker)` hands each worker the next committed event after its cursor, at or below the watermark, from memory. Every worker sees every event, including the `rannoch.closed.<relation>` events fill appends (see Links).
5. **Read.** Reads go through any handle on the file. They read the history tables and `_seen`, never the queue's memory.

What a crash loses: events accepted since the last heartbeat, and the batch whose commit was in flight. Nothing committed is lost. Every commit is durable: the file runs in WAL mode with `synchronous=FULL` and `fullfsync=ON`, so on macOS each commit is flushed to the device.

## The tables

All in one file. rannoch's own tables start with `_`.

| Table | Holds |
|---|---|
| `_events` | The log: place, log id, body. |
| `_ontology` | Every declared class and relation. |
| `_hearings` | Every hearing of a node or link: its identity, entity, key (and a link's target key), event, place, source, valid time, axis point, receipt stamp (`recorded`), properties, and its known window (`known`, `until`) and the place of the event it amends (`amended`). |
| `_aliases` | Other events that arrived with a hearing's identical content. |
| `_ends`, `_endings` | A link's latest conclusion by known-at (ended or found wrong), and every ending at its axis point and known-at. |
| `_amendments` | Each amendment by target place, known-at and event. |
| `_renames` | Each rename: entity, old key, new key, known-at, event. |
| `_waiting` | Each amendment waiting for its target: the target's source and id, the amendment's known-at, event and place. |
| `_open` | Links waiting for a node that has not arrived. |
| `_seen` | Per node and link: first and last heard, first recorded, first true, count. |
| `class:<name>` | One row per node of the class, one column per property. |
| `relation:<name>` | One row per link with a version known now (endings are read from `_endings`, not from this table), with foreign keys to both classes' tables. |
| `_places`, `_work` | The inbox: each worker's place, and a row for each event in progress or failed, with its reason and try count. |
| `_positions` | Each producer's position in its source. |
| `_fill` | Fill's try limit (3 unless set). |
| `_check` | The check value of the app's key. |

The tables kept for speed are `class:<name>`, `relation:<name>` and `_seen`; fill keeps them current. Reads answer from `_hearings`, `_aliases`, `_ends`, `_endings` and `_seen`. The class and relation tables serve the app's own SQL. `find` returns `None` for a key renamed away. A link of the wrong classes cannot exist in a relation table, since both ends are foreign keys.

## Three times

Every fact has three times, each with one job:

- **Known-at**: when the fact became known. It is the millisecond in its CloudEvent id (a `UUIDv7`), so it trusts the clock of the host that minted the event. A store's own receipt stamp (the log id it mints, and `recorded`) orders its log and is never read as known-at.
- **Its axis point**: where the fact sits on the axis its class or relation declares, as named integer parts compared in order, such as `(build)` or `(build, step)`; a time is a part in milliseconds. The event carries it as `data.axis`. A class or relation that declares no axis is ordered by known-at.
- **Its valid time**: what it claims about the world (`ValidTime`): unknown, a month or a day. It is optional, and unknown is never read as now.

A hearing written directly is known from its known-at and has no `until`. A hearing made by an amendment is known from the later of the amendment's known-at and the target's; the hearing it supersedes gets `until` set to then. Versions order by axis point, then known-at, then event id, so a correction at the same point follows what it corrects.

Reads take a `View`: an optional axis point and an optional known-at, each defaulting to now. A point with fewer parts than the axis reads the top of its prefix.

- **Now** (`find`, `hearings`, `seen`, `link_seen`, `link_end`): hearings with no `until`, the newest on the axis. A version that arrives late answers at its own point and becomes current only if it is the newest on the axis.
- **In a view** (`state_at`, `link_state_at`): the newest version at or before the view's point among the hearings known at its known-at (`known` at or before it, `until` after it or absent). None needs a valid date. An amendment or rename known later is not in the answer.
- **As of a valid date, in a view** (`find_as_of(class, key, date, view)`): the version valid on that date among those the view sees.

**Identity.** A hearing's identity is its content together with its source, hashed with BLAKE3 under the app's 32-byte key. Identical content from the same source is one hearing: a repeat under the same CloudEvent id records nothing; a repeat under a new id becomes an alias, an occurrence of the hearing at the repeat's own known-at. A declared axis point is content; known-at is not. The key is held in memory only; the file keeps a check value, and opening it under another key returns `Error::KeyDiffers`.

**Links.** A link's hearings and endings carry axis points through the same path as nodes: the same link heard at two builds is two hearings, each at its build. Its endings sit beside its hearings at their own points. A live hearing at a later point reopens a link that ended. A correction records that the link was wrong, which is not an ending. The latest conclusion, ending or correction, is chosen by known-at and then event id, never by arrival. A link that names a node that has not arrived waits in `_open`. When the node arrives, fill moves the link into its relation's table and appends one event to the log: type `rannoch.closed.<relation>`, source `rannoch`, a bare `UUIDv7` id, subject the link's from-key, data `{from, to, valid}`. Every worker is handed these events; a worker that does not care about them closes them as done.

## Amend and rename

**Amend is rare.** It corrects a mistake: an event that was written wrong. It is not how an app records change. A value that changes is a new event, a new hearing at its own point, and the store's history keeps both. An app that amends routinely is modelling change the wrong way.

An amendment is an event of type `rannoch.amended` whose data is `{"target": {"source": ..., "id": ...}, "patch": {...}}`: the target's own CloudEvent source and id, and a JSON Merge Patch (RFC 7386) of that event. It is known at the clock in its own CloudEvent id. Amendments are allowed in every class.

Each store resolves the target when it fills the amendment, by reading the log for the first event with that source and id. That read scans the log; only amendments, which are rare, run it, and nothing on the write path pays for it. So a store replaying another's log applies the same amendment. An amendment whose target the store has not filled yet (not in the log, or logged after the amendment) waits in `_waiting` and applies when the target fills, so arrival order never changes the answer. While nothing waits, fill looks nothing up for it.

When fill applies an amendment, it records it in `_amendments`, then derives the target's hearings again: it applies every amendment of the target by known-at, then event id, and wherever the event reads differently, it writes a new hearing known from that amendment's known-at and closes the hearing before it then. A patched `data.axis` moves the hearing to the patched point. The amended event in the log is never changed. Read as known before the amendment, the event reads as written.

**A rename is an amendment that patches the subject.** Fill records it in `_renames` as a mapping from the old key to the new key, known from the amendment's known-at. Every event that names the old key is heard again through the mapping: the node's own events and every link to or from it. Link endings and corrections are copied onto the new key; their old-key rows stay, so reads as known before the rename are unchanged. A link, ending or node event arriving after the rename that names the old key lands on the new key. A second rename maps on from the new key.

Fill refuses an amendment whose id is not `<uuid>:<fact>`; that names no target or has no patch; whose target id is not `<uuid>:<fact>`; that names an event logged before it which made no hearing of its own (a declaration, an app's own event, a link ending or correction, content identical to an earlier hearing, or any event that states no fact); or that is known before the target's known-at or the latest amendment already applied to it. Fill also refuses an amendment whose patch leaves the event invalid against the ontology or stating no node or link. A refused amendment is a failed try (below); it never stops fill.

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

Each worker reads with its own cursor, held in the queue's memory: every event at or before it is closed. Taking an event and closing it change only memory; neither writes to disk.

- `next(worker)` hands out the first event after the last one handed out, at or below the committed watermark, and marks it in progress. The worker reads ahead from the log 1,024 events at a time (`Events::chunk`). A worker joins at the start of the log on its first `next`.
- `done` and `failed` close an event. The cursor moves past every closed event at its front, so a worker that holds several events and closes them out of order keeps its cursor before the oldest one still open.
- `done_keeping(worker, cursor, state)` closes as `done` does and keeps an opaque string with the cursor once the cursor reaches that event; `kept(worker)` reads it back.
- **The group commit.** The heartbeat, `flush` and `close` save every worker that changed in one transaction (`Graph::save_places`), also when no event was accepted: each worker's cursor in `_places`, its kept state in `_kept`, and each event it closed as failed as a `_work` row with its reason. A save that fails is reported as `Failure::Workers` and tried again by the next group commit.
- **After a crash** a worker resumes from its last saved cursor and is handed again everything after it, closed or not: delivery is at least once. An event saved as failed ahead of the cursor and done when handed again has its failure cleared.
- The inbox retries only fill's failures, never an app worker's. Fill keeps its own place and `_work` rows, written in its own transactions, and the group commit never touches them.
- Each worker sees every committed event, in log order, independent of every other worker.
- `lag(worker)` is how many committed events are after the worker's cursor; `slowest()` is the lowest cursor of all workers. A producer or a ring reads them to apply back-pressure; the queue never blocks or refuses `accept`.

`Graph::next`, `done` and `failed` still exist on the store, one durable write each; the queue no longer calls them.

## The processed mark

`processed()` is the lowest point every worker has finished: no event at or before it is waiting or in progress for any worker. A failed event counts as finished. Fill is a worker, so the mark never passes what fill has not reached. With no workers it equals the committed watermark, and it never passes it. It deletes nothing. `Queue::processed` reads the workers' cursors from memory; `Graph::processed` reads what the last group commit saved. `remove_worker` deletes a worker's place, kept state and rows at once and releases the mark it held.

## copy_to

`copy_to(path)` runs SQLite's `VACUUM INTO` inside one read transaction and returns the cursor the copy holds, read from the copy itself. It takes no write lock and records nothing in the store. It refuses a path that exists and removes its file if the copy fails. The copy is not flushed durably; the snapshot tool that does that is designed in #154 and not built.

## Limits

- **No public lookup from a CloudEvent id to a log id.** Fill resolves an amendment's target by source and id itself; issue #164 exposes that lookup to apps, and is not built.
- **At-least-once, no dedupe by CloudEvent id.** The log mints a fresh log id for every event, and the queue accepts a body it has accepted before. A repeat under the same CloudEvent id records nothing in the graph. A repeat under a new id is an alias of its hearing at the repeat's own known-at; on an undeclared axis, if that is newer than the current version, the repeated (older) content becomes current through it. The log holds both events and every worker is handed both, so workers must be idempotent.
- **The queue owns its graph.** `Queue::open` takes the `Graph` by value. An app with a queue open reads, and calls `stopped`, `retry`, `set_try_limit` and `copy_to`, through a second handle (`rannoch_store::open`).
- **An event a worker takes and never closes stays in progress** until the process restarts, and holds the worker's cursor and the processed mark below it until it is closed.
- **A crash hands a worker again what it closed since the last group commit**, about 20 ms of work at the default interval.
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
