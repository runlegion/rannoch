# The inbox

`rannoch-queue` is the way events get into a store and out to an app's workers. It checks each CloudEvent against the ontology when it is accepted, commits what it holds about every 20 ms, fills the tables, and hands every committed event to every worker. This page follows `crates/rannoch-queue/examples/worker.rs`:

```sh
cargo run -p rannoch-queue --example worker
```

Workers today are code in your app. Worker files run by a harness are a design ([flows.md](flows.md)).

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
- The queue owns the graph's connection. Read through another handle on the same file: `rannoch_store::open(&path, &KEY)?.find(...)`.

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

`subscribe(|cursor| ...)` calls back after each commit with the cursor of the last committed event. A subscriber may call `next` from inside; it must not subscribe, flush or close there.

## Write a worker

A worker is a name and a loop. Each worker has its own place in the log and sees every committed event, in order: declarations, nodes, links, amendments and the app's own event types. The worker decides what it cares about and closes everything else as done.

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

- `next(worker)` hands out the worker's next committed event and marks it in progress; `None` when there is nothing past its place. It never hands out an event past the committed watermark.
- `done(worker, cursor)` and `failed(worker, cursor, reason)` close it. Closing an event that is not in progress for that worker is refused.
- `state(worker, cursor)` says where an event stands for a worker: `Waiting`, `InProgress`, `Done` or `Failed(reason)`.
- Workers are independent. Two workers on one queue each get every event; one falling behind holds no other back.
- A worker name is any string except `rannoch.fill`, which fill uses for itself.

A failed event stays failed for that worker; the inbox does not retry an app's worker. Retry it in your own code if your work can succeed later.

### After a crash

A worker's place and every event's state are in the file. When the process restarts, `next` first hands the worker back any event an earlier run left in progress, then carries on after its place. Nothing it closed is handed out again.

## The processed mark

```rust
let last = queue.processed()?;
```

The processed mark is the lowest point every worker has finished: everything at or before it has been closed, done or failed, by every worker, fill included. It never passes what fill has not reached. Use it to know what is safe to act on everywhere, or to report progress.

A worker that leaves for good holds the mark back until it is removed:

```rust
queue.remove_worker("index")?;
```

## What was measured

From `toys/live/RESULTS.md` on the `chore/live-test` branch, main 7fe3df9, stores on an external USB drive:

- 300,000 events produced under `kill -9` every 50-300 ms (1,621 kills): none missing, doubled or unfilled.
- A worker over 3,001 events under 623 kills: every event done, none left in progress.
- Clean fill throughput: about 44,000-45,000 events a second on a quiet machine (35,000-37,000 with another process holding a core).
