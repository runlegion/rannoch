# The inbox

`rannoch-queue` is the way events get into a store and out to an app's workers. It checks each CloudEvent against the ontology when it is accepted, commits what it holds about every 20 ms, fills the tables, and hands every committed event to every worker. This page follows `crates/rannoch-queue/examples/worker.rs`:

```sh
cargo run -p rannoch-queue --example worker
```

Workers today are code in your app. Worker files run by a harness are a design ([flows.md](flows.md)). To keep a neighbourhood of the graph warm instead of writing a worker for it, see [relays.md](relays.md).

## Open a queue

```rust
fn graph(path: &Path) -> Result<Graph, rannoch_store::Error> {
    let mut g = open(path, &KEY)?;
    let body = format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:tailer","type":"rannoch.declared.line"}}"#,
        id()
    );
    g.declare(&Declaration::Class(Class::new("line", ["text"])), &body)?;
    Ok(g)
}

let queue = Queue::open(graph(&path)?, Queue::INTERVAL)?;
```

- Declare the ontology first. The queue checks events against the ontology it took when it opened; to declare more, close the queue, declare, and open it again.
- `Queue::open` takes the graph, fills whatever the log holds past the tables (a process that stopped between a commit and its fill leaves some behind), and starts the heartbeat.
- The queue owns the graph: `Queue::open` takes the `Graph` by value. Reads, and the store calls the queue does not expose (`stopped`, `retry`, `set_try_limit`, `copy_to`), go through a second handle on the same file: `rannoch_store::open(&path, &KEY)?`. The queue does all commits and fills; the second handle only reads and calls those.

## Accept events

```rust
for (number, text) in lines {
    queue.accept_from(line("app.log", number, text), "tailer", number.to_string())?;
}
let (_, failures) = queue.flush();
```

`accept(body)` does the same without a producer. Both check the event, hold it in memory and return a ticket; neither waits for the commit.

`accept` refuses at once, with the reason, an event the ontology does not allow, a body that is not CloudEvents 1.0 JSON, or a declaration. A refused event is not held.

The heartbeat commits everything held in one transaction, a savepoint per event, then fills the tables and tells subscribers. `flush()` commits now and returns the committed watermark with any failures. `close()` stops the heartbeat, commits what is held and returns the failures; dropping the queue does the same, but a process that exits without unwinding (`std::process::exit`, `panic = "abort"`) runs no drop, so close the queue first.

### Producers and positions

A producer is anything reading a source: a log file, an archive, a socket. `accept_from(body, producer, position)` commits the producer's position in its source in the same transaction as the event. If the event fails at commit, the producer's later events in that batch are not committed and its position stays at its last event that landed.

```rust
let from = queue.position("tailer")?;
assert_eq!(from, None, "a new producer has no position yet");
```

After a crash, a producer that resumes from `position` sends again exactly what was lost and nothing twice. What a crash can lose: the events accepted since the last heartbeat, and the batch whose commit was in flight. Nothing committed is lost.

### Failures

Something that goes wrong after an event was accepted is a `Failure`, reported once: by `flush`, `close` or `take_failures`, or, when the queue drops, to the handler set with `on_failure` (stderr without one).

- `Failure::Event`: an event failed at commit and did not land.
- `Failure::Fill`: a commit landed but its tables were not filled.
- `Failure::Heartbeat`: the heartbeat panicked; what is queued commits only through a flush, a close or a drop.
- `Failure::Subscriber`: a subscriber panicked.
- `Failure::Workers`: the workers' cursors could not be saved; the next group commit tries again.

`subscribe(|cursor| ...)` calls back after each commit with the cursor of the last committed event. A subscriber may call `next` from inside; it must not subscribe, flush or close there.

## Write a worker

A worker is a name and a loop. Each worker has its own place in the log and sees every committed event, in order: declarations, nodes, links, endings, amendments, the `rannoch.closed.<relation>` events fill appends when an arriving node closes a waiting link, the `rannoch.refilled` events it appends when an event succeeds on retry, `rannoch.query`, `rannoch.answer` and `rannoch.report` events, and the app's own event types. The worker decides what it cares about and closes everything else as done.

```rust
fn drain(queue: &Queue, worker: &str) -> Result<usize, rannoch_store::Error> {
    let mut closed = 0;
    while let Some(event) = queue.next(worker)? {
        match report(&event) {
            Ok(()) => queue.done(worker, event.cursor)?,
            Err(why) => queue.failed(worker, event.cursor, &why)?,
        }
        closed += 1;
    }
    Ok(closed)
}
```

- Each worker reads with its own cursor, held in memory. Taking an event and closing it write nothing to disk.
- `next(worker)` hands out the worker's next committed event and marks it in progress; `None` when there is nothing past it. It never hands out an event past the committed watermark. The worker reads ahead from the log a chunk at a time.
- `done(worker, cursor)` and `failed(worker, cursor, reason)` close it. Closing an event that is not in progress for that worker is refused. The cursor moves past every closed event at its front, so an event still open holds it back.
- `state(worker, cursor)` says where an event stands for a worker: `Waiting`, `InProgress`, `Done` or `Failed(reason)`, closes not yet saved included.
- `join(worker)` returns where a worker stands, joining it at the committed watermark when it has no place, for a worker that reads the tables first and needs only what follows.
- `commit()` commits what is held now, without waiting for the heartbeat, and leaves every failure for the app to take, for a worker whose answer someone is waiting on.
- Workers are independent. Two workers on one queue each get every event; one falling behind holds no other back.
- A worker name is any string except `rannoch.fill`, which fill uses for itself, and, with relays open, `rannoch.relay` and `rannoch.relay.<id>`.

A failed event is recorded with its reason and stays failed for that worker; the inbox does not retry an app's worker. Retry it in your own code if your work can succeed later.

Close every event you take. A worker that returns or errors without calling `done` or `failed` leaves the event in progress: no other `next` hands it out until the process restarts, and the worker's cursor and the processed mark stay below it until it is closed.

### Keeping state with the cursor

```rust
queue.done_keeping(worker, event.cursor, total.to_string())?;
let total = queue.kept(worker);
```

`done_keeping` closes the event as `done` does and keeps an opaque string with the worker's cursor once the cursor reaches that event. It is saved in the same group commit as the cursor, as a producer's position is with its events, and `kept` reads it back, after a restart too.

### The group commit

A worker's cursor, its kept state and its failures reach disk only in a group commit: the heartbeat, `flush` or `close`. One transaction saves every worker that changed, and a heartbeat with only worker changes still commits them. A save that fails is reported as `Failure::Workers` and tried again by the next group commit.

### After a crash

A worker resumes from its last saved cursor and is handed again everything after it, including what it closed since the last group commit. Delivery is at least once, so workers must be idempotent, as they already are for repeated events. An event saved as failed and done when handed again reads as done.

### Lag and back-pressure

```rust
let behind = queue.lag("reports")?;
let slowest = queue.slowest();
```

`lag(worker)` is how many committed events are after the worker's cursor. `slowest()` is the lowest cursor of all workers, `None` with none. A producer or a ring reads them to apply back-pressure; the queue itself never blocks or refuses `accept`.

## The processed mark

```rust
let last = queue.processed()?;
```

The processed mark is the lowest point every worker has finished: everything at or before it has been closed, done or failed, by every worker, fill included, closes not yet saved included. It never passes what fill has not reached. Use it to know what is safe to act on everywhere, or to report progress.

A worker that leaves for good holds the mark back until it is removed:

```rust
queue.remove_worker("index")?;
```

## What was measured

From the [release check](https://github.com/runlegion/rannoch/blob/chore/187-release-check/toys/live/RESULTS.md) for v0.6.0, on main 63681e5:

- 300,000 events produced under `kill -9` every 50-300 ms (1,630 kills): none missing, doubled or unfilled.
- A worker over 30,000 events under 105 kills: every event done, none left in progress.
- A clean fill of 306,000 events: 88,294-91,381 events a second, with other agents on the machine.

A clean produce through the queue was not measured again. On main 7fe3df9, before the fill work in this release, it ran at 34,716-36,872 events a second, with another process holding a core.

Worker costs: `next` goes to the store once a chunk and `done` never does, which `a_worker_goes_to_the_store_once_a_chunk_and_not_to_close` in `crates/rannoch-queue/src/tests.rs` counts, so any trip to the store per event fails. Measured once, on an Apple-silicon laptop with an SSD, one worker taking and closing 20,001 events on a clean store, with no work in the worker, took 0.86 µs per event in the debug build and 0.20 µs in a release build. Before the worker contract, each `next` and each `done` was a durable commit: 5.4 ms per event, measured by an app on v0.5.0.
