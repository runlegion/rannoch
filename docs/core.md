# rannoch core

| | |
|---|---|
| Surface | rannoch, crate `rannoch-core` (modules: broker, inbox) |
| Status | draft |
| Owner | legion |
| Author | legion, from the operator's words |
| Date | 2026-11-10 |
| Traces to | intent `01a1c6d3-f02d-75f2-a236-9fda613818e1`; design brief `01a1c6e4-03ce-7db1-9a3b-5796b3b4d57d` |

Each requirement follows legion's requirement format: id, priority (SHALL, SHOULD, MAY), description, provenance, dependencies, API, errors, and verification (acceptance criteria, each with an id and an evidence kind, and one Gherkin scenario per scenario-kind criterion). Provenance quotes the operator verbatim. Lines marked **[legion]** are mine and unconfirmed.

## Scope

Core is the broker and the inbox: "for core modules broker inbox". The broker is core's input and passes each event to an inbox ("the broker is I and it's O is passing to inbox"); the inbox holds events until it writes them into the store ("inbox does does everything up to sending to rannoch-store").

Out of scope for core, each in the operator's words:

- A CloudEvents crate or wire format: "ya we don't need it."
- Serde at the edge: "we don't need to serde if we just do rust types as the edge. this is a rust lib afterall."
- Sizing, including a sanity limit: "fuck it we solve this if it comes up. no sizing."
- Flushing under memory pressure: "the presure should be machine based and that could cause all kinds of issues. lets leave it out of this version".
- Use in wasm: "add to the docs not to use in wasm now".
- Durability of the heap: "there is nothing we can do about the crash while in the buffer. this is not a spacecraft we don't need parity".
- Queues, flows, processors, the store and the status of an event after 0: other crates. Their questions are in `later.md`.

---

## FR-CORE-001 The event

**SHALL.** Rannoch's I/O is one Rust struct, starting from the CloudEvents shape:

```rust
pub struct Event<T, D> {
    pub id: Uuid,                // set by rannoch at accept; UUIDv7
    pub source: Option<Uuid>,    // another event id, or None
    pub ty: T,                   // the app's enum
    pub data: D,                 // the app's
    pub run_at: Option<Timestamp>,
    pub status: Status,          // set by rannoch, 0 at accept
}
```

- `ty` is an enum the app defines, a plain fieldless enum.
- `source` is set by whoever calls `send`: another event's id, or None. Core does not track it.
- `data` is the app's type; rannoch never reads it.
- `id` is a UUIDv7, so it carries when the event was made; there is no separate time field.
- There is no `subject` or `time`; an app that needs them puts them in `data`.

**Traces to.** "lets create our own struct for whats i/o we can start at the CE shape." "ty should not be a string. enum"; legion proposed a plain fieldless enum, the operator: "yes for idea 2. this allows the app to create its own lanes." "a source is another event id or nil"; "option is fine"; "source is for tracking by rannoch not the app." "the source is set by whomever is calling send. How is the core suposed to track that?" "we can drop time if we're setting id." "it's part of the data, no reason to have that on the envelope". "we don't choose what an app's data struct is, they do." "Data is not ours, at all."

**[legion]** `run_at` came from the scheduling talk; what it means is the queue's (`later.md`).

**Verification.**
- AC-001-1 (artifact): the struct above is the public event type of `rannoch-core`.

## FR-CORE-002 Status

**SHALL.** An event's status is one int field with four values, typed in Rust as an enum: -1 binned, 0 added, 1 in progress, 2 processed. Core sets only 0.

**Traces to.** "the event should have -1|0|1|2 for it's status", an int "for speed". "it's a single field, just like so many other queues." "enum it is". The labels "are for the docs".

**Verification.**
- AC-002-1 (artifact): `Status` is an enum over exactly -1, 0, 1 and 2, stored as an int.

## FR-CORE-003 Configuration and open

**SHALL.** The app opens rannoch with a configuration: a plain struct, not a trait. It names the app's inboxes, which `ty` values go to each, and each inbox's batch interval. Every type not listed goes to the default inbox. With nothing configured there is one inbox, at 20 ms. Where the values come from, such as a settings file, is the app's choice.

**Traces to.** "inbox is not singleton, an app can create n-inboxes." "the type CAN have it's own inbox IF the app wants that. default is a single inbox." "these are configration not code." "20ms, this is a config value too". "an app that has huge data can create a lane that is specific to large memory events. like kessel's assets." Not a trait: the operator, "your want to make the 'lanes' a trait, makes zero sense."

**API.** `Rannoch::open(config) -> Result<Rannoch, Error>`.

**Errors.** `Config` when the configuration does not make sense.

**Verification.**
- AC-003-1 (scenario): with no configuration there is one inbox at 20 ms.
- AC-003-2 (scenario): an inbox configured for a type receives that type, and every other type goes to the default inbox.
- AC-003-3 (scenario): a configuration that does not make sense fails open with `Config`.

```gherkin
@criterion-AC-003-1
Scenario: Open with no configuration
  Given no inboxes are configured
  When the app opens rannoch
  Then there is one inbox with a 20 ms batch interval

@criterion-AC-003-2
Scenario: An inbox for a type
  Given an inbox "assets" for type Asset at 500 ms
  When the app sends an event of type Asset and an event of type Page
  Then the Asset event is on the "assets" heap
  And the Page event is on the default inbox's heap

@criterion-AC-003-3
Scenario: A configuration that does not make sense
  Given a configuration that does not make sense
  When the app opens rannoch
  Then open fails with Config
```

## FR-CORE-004 Send and accept

**SHALL.** The app sends an event to the broker with `send`. The broker passes it to the inbox for its `ty`. The event is accepted when it is on that inbox's heap: rannoch sets its `id`, a UUIDv7, and its status, 0, and `send` answers 0.

**Traces to.** "event producers send to the broker. the broker scans the event and chooses how it should be processed." "we have the struct, thats the shape that gets sent"; "send or queue? whatever". "producers should get a recived result. it's up to them to figure out what to do without one." "just 0". "end of buffer is 0, that is when it's accepted, it's on the heap".

**API.** `send(event) -> Result<_, Error>`; the accept result is 0.

**Errors.** `Closed` when rannoch is shutting down.

**Verification.**
- AC-004-1 (scenario): an accepted event is on its inbox's heap, with an id and status 0, and the answer is 0.
- AC-004-2 (scenario): sending while rannoch is shutting down fails with `Closed`.

```gherkin
@criterion-AC-004-1
Scenario: An event is accepted
  When the app sends an event
  Then the event is on its inbox's heap
  And it has a UUIDv7 id and status 0
  And send answers 0

@criterion-AC-004-2
Scenario: Sending while shutting down
  Given rannoch is shutting down
  When the app sends an event
  Then send fails with Closed
```

## FR-CORE-005 The batch

**SHALL.** Each inbox's heap holds accepted events in memory, with no size limit. On its batch interval the inbox writes them into `_events`, a table `rannoch-store` defines, and when the write is done it signals the queue. Rannoch does not measure event size. A crash loses what is on the heap.

**Traces to.** "the heap is just the mem that holds what accumulates until the batch is sent. and the batch timing should be a config setting." "into _events, one of the defined tables in store. The queue watches that." "I planned on when the write was done to signal queue." "at this point I don't see a value in making a box xxMB or something." "thats why we have the configured lane for known large data. besides that we can't nor care." "if it matters to a producer, they really should not be using rannoch."

**Depends on.** `rannoch-store` defines `_events`.

**Errors.** `Store` when the write fails.

**Verification.**
- AC-005-1 (scenario): on its interval an inbox writes its heap into `_events` and signals the queue.
- AC-005-2 (scenario): a crash before the write loses the heap's events.
- AC-005-3 (scenario): a failed write comes back as `Store`.

```gherkin
@criterion-AC-005-1
Scenario: An inbox writes its batch on its interval
  Given events on an inbox's heap
  When the inbox's batch interval passes
  Then the events are in _events
  And the queue has been signalled that the write is done

@criterion-AC-005-2
Scenario: A crash loses the heap
  Given events on an inbox's heap
  When the process stops before the batch is written
  Then those events are not in _events

@criterion-AC-005-3
Scenario: A failed write
  Given the store cannot write
  When an inbox's batch interval passes
  Then the error is Store
```

## FR-CORE-006 Errors

**SHALL.** Core owns rannoch's error vocabulary, and every crate below it hands errors back up in it.

| Error | When | From |
|---|---|---|
| `Config` | The configuration does not make sense at open | core |
| `Closed` | An event is sent while rannoch is shutting down | core |
| `Store` | A batch or a read failed in the store | below |
| `Queue` | Processing failed where the caller needs to know | below |

A binned event (-1) is a status, not an error. There is no malformed-event error: with Rust types at the edge, the compiler rejects a missing field or a type mismatch.

**Traces to.** "map the error vocab, everything below this should hand it back up"; the map: "correct". "then drop malforned. you're right".

**Verification.**
- AC-006-1 (artifact): `rannoch-core` exports one error type with exactly these four variants.

## FR-CORE-007 Logging

**SHALL.** Rannoch emits through the `log` crate at two levels, `error` and `debug`. The app chooses where the output goes. Everything rannoch's crates write emits for both.

**Traces to.** "three levels of logging, debug|error|out"; the `log` crate: "Yes, and everything we write will need to emit for both."

**Verification.**
- AC-007-1 (scenario): an error and a debug line from rannoch reach the logger the app installed.

```gherkin
@criterion-AC-007-1
Scenario: Rannoch logs through the log crate
  Given the app has installed a logger
  When rannoch emits an error line and a debug line
  Then the logger receives both
```
