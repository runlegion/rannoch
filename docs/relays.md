# Relays

`rannoch-relay` keeps a declared neighbourhood of the graph warm in memory. An app names a shape and a view; the relay walks it once, and from then on the app reads the result in process, without touching the graph, and hears what changes as events. Everything a relay holds can be rebuilt from the log. This page follows `crates/rannoch-relay/examples/relay.rs`, which runs:

```sh
cargo run -p rannoch-relay --example relay
```

Use a relay when the same neighbourhood is read over and over and something must react when it moves. For a read now and then, the store's reads ([store.md](store.md#read)) cost nothing to keep. The architecture's [Relays section](architecture.md#relays) is the reference for how it is built.

The snippets call helpers defined at the top of the example: `declared`, `author`, `book` and `wrote` build event bodies, `write` accepts bodies and flushes, and `until` polls with a guard against hanging.

## Declare a shape

A shape is a declaration, as a class is. Declare it before opening a queue on the graph, as with every declaration ([inbox.md](inbox.md#open-a-queue)).

```rust
Shape {
    name: "colleagues".into(),
    roots: Roots::Node {
        node: NodeRef::new("author", "a1"),
    },
    steps: vec![
        Step {
            relation: "wrote".into(),
            drop: BTreeMap::from([("shelf".to_string(), Value::from("withdrawn"))]),
            cap: Some(2),
            ..Step::default()
        },
        Step {
            relation: "wrote<-".into(),
            ..Step::default()
        },
    ],
}
```

```rust
g.declare(&Declaration::Shape(colleagues()), &declared("colleagues"))?;
```

- **Roots** are one node (`Roots::Node`), or a class's nodes that have every value in `filter` (`Roots::Class`). A filter's properties must be ones the class declares `indexed`.
- **A step's relation carries its direction in its name:** `r` forward, `r<-` backward, `r<->` either.
- **`keep` and `drop`** are maps of property to value, on the node reached. `keep`: it stays only if it has every value. `drop`: it goes if it has every value.
- **`cap`** limits how many nodes a step takes. A step with no cap is exact.

Each step takes the distinct nodes its links reach that have a version in the view, pass its filters and have not been taken, in key order, up to its cap. So a cycle never repeats a node, and a cut is the same cut every time. A node a cap cuts is not expanded, and a later step may still reach it.

## Open the relays

```rust
let queue = Arc::new(Queue::open(open(path, &KEY)?, Queue::INTERVAL)?);
// Joined at the watermark: this worker needs only what follows, the answers.
let _ = queue.join(ANSWERS);
let relays = Relays::open(dir, Arc::clone(&queue), path, &KEY)?;
```

`Relays::open` takes the directory the relays keep their files in, the queue, and the graph's path and key. It starts one thread for all an app's relays. It opens its own handle on the file, so a queue and relays are two handles, up to 128 MB of page cache ([store.md](store.md#open)).

The queue must be in an `Arc` because the relays' thread shares it. The workers `rannoch.relay` and `rannoch.relay.<id>` are the relays'; do not name your own those.

## Ask

A relay is asked for through the inbox, with a `rannoch.query` whose data names a shape and a view:

```rust
json!({
    "specversion": "1.0", "id": EventId::now().to_string(), "source": APP,
    "type": "rannoch.query", "data": {"shape": shape, "view": view},
})
```

The view is `{"axis": [...], "known": <ms>}`, each part optional, read as every read reads a view ([store.md](store.md#read)). `{}` is now, and that relay moves with the log. A view fixed at a point or a known-at reacts only to what lands inside it: a later build changes nothing in it and tells no change. A class with no axis reads an axis point as a known-at in milliseconds, so a shape viewed at a point needs its classes and relations on the same axis.

The answer is a `rannoch.answer` in the log, whose subject is the query's id. An app reads it with a worker of its own, joined before it asks:

```rust
let answer = take(queue, |_, body| {
    (body["type"] == "rannoch.answer" && body["subject"] == id["id"]).then(|| body.clone())
})?;
let result = &answer["data"]["result"];
println!("answer: steps {}", result["steps"]);
Ok(result["relay"].as_str().ok_or("a relay id")?.to_string())
```

```json
{"result": {"relay": "<uuidv7>", "cursor": 15,
            "steps": [{"nodes": 2, "cut": true, "dropped": 1}, {"nodes": 1, "cut": false, "dropped": 0}]},
 "known": 1793000000000}
```

The answer holds the relay's id, the place in the log it is warm to and what each step took, not the nodes. `dropped` counts the distinct nodes the cap cut; a node a filter refused is not counted. `known` is the latest known-at among the facts the walk read, or the view's own.

Relays of one shape and view are shared: a second query gets the same id, and its `source` becomes another holder. `Graph::answer` refuses a shape query; it answers only the reads in [store.md](store.md#read).

## Read

```rust
let set = relays.read(&relay).ok_or("the relay is there")?;
assert_eq!(steps(&set), [vec!["b1", "b2"], vec!["a2"]]);
assert_eq!(set.steps()[0].dropped, 1, "b3 is past the cap");
assert_eq!(set.nodes().len(), 4, "the root, two books, one colleague");
assert_eq!(set.by("shelf", &Value::from("open")).len(), 2);
```

`read` answers from memory and never touches the graph. It returns the set as of its last fold, or `None` when no relay has that id.

| Call | Answers |
|---|---|
| `nodes()` | every node in the set, roots included, each once, with its version in the view |
| `steps()` | what each step took, in the shape's order: `nodes` in key order, and `dropped` |
| `by(property, value)` | the nodes whose class indexes `property` and whose value of it is `value`, in key order; a property no class indexes finds nothing |
| `cursor()` | the place in the log the set is warm to |

A relay is a worker, so a set is behind the log until its thread has folded the last commit. To read your own write, wait for `cursor()` to reach it, or for its change event.

## Change events

```rust
let (tell, changes) = channel();
relays.subscribe(move |body: &str| {
    let _ = tell.send(body.to_string());
});
```

A commit that moves the set tells one `rannoch.relay.changed` event, however many events it held, once the new set can be read. Its subject is the relay's id:

```json
{"cursor": 19,
 "changed": [],
 "entered": [{"node": {"class": "book", "key": "b00"}, "valid": "unknown"}],
 "left": [{"node": {"class": "book", "key": "b2"}, "valid": "unknown"}]}
```

- `changed` holds `{node, property, before, after, valid}` for each property that moved on a node that stayed.
- `entered` and `left` hold each node that came or went. Its `valid` is that of the link that moved it, or of the last link in the batch when it moved downstream of one.
- Here one new book below the cut came in and pushed another out: both moves, in one event.

A commit that moves nothing in the set tells nothing, and so does an idle relay. An amendment, a retried fill or a rename makes the relay read everything again, and what differs is told the same way.

Change events are not logged, since the log can rebuild them, and they are not a ledger. After a restart one may be told again, and a relay rebuilt from the graph tells nothing of what moved while it was away. Read the set after a restart. Subscribers run on the relays' thread, so a slow one holds up every relay.

## Reports

A report makes a warm set durable: an event that names the shape, the view and the cursor, with nothing inlined.

```rust
let report = relays.report(&relay)?;
write(
    &queue,
    &[book("b000", "Earlier still", "open"), wrote("a1", "b000")],
)?;
assert_eq!(steps(&settled(&relays, &relay, &path)?)[0], ["b00", "b000"]);
assert_eq!(steps(&relays.reread(&report)?)[0], ["b00", "b1"]);
```

`report` accepts a `rannoch.report` event and returns its body; keep the body, or take it from the log. `reread` replays the log up to that cursor into a temporary graph in the relays' directory and walks the shape there, so it takes as long as the log is, and it uses the ontology declared now. It needs no live relay of that shape.

## Failures

A query that cannot be answered gets no answer. It is closed as failed on the worker `rannoch.relay`, with its reason:

```rust
let why = until(|| {
    Ok(match queue.state("rannoch.relay", at)? {
        State::Failed(why) => Some(why),
        _ => None,
    })
})?;
```

Here the reason is `shape nothing is not declared`. An app waiting on an answer should check this, or give up after its own time.

What fails on the relays' thread waits for `take_failures` or `close`, oldest first. At most 256 wait; past that the oldest go and an `Error::Lost` comes first, saying how many.

- **A subscriber panics:** `Error::Subscriber`. The relays run on.
- **A read of the graph fails:** the relay's cursor does not move and it reads everything again on its next wake, with no new event needed. Its set stays as it was until then.
- **Its file cannot be saved:** the change is still told and the event stays open. The next due save tries again, and the change is not told twice.
- **The thread panics:** `Error::Thread`, from `close`.

`release` and `report` return `Error::Refused` when no relay has the id.

## Restart

```rust
let failures = relays.close();
assert!(failures.is_empty(), "{failures:?}");
let (_, failed) = queue.flush();
assert!(failed.is_empty(), "{failed:?}");
drop(queue);
drop(changes);
let (queue, relays, _changes) = start(&path, &dir)?;
assert_eq!(steps(&settled(&relays, &relay, &path)?)[0], ["b00", "b000"]);
assert_eq!(ask(&queue, "colleagues", &json!({}))?, relay);
```

Close the relays before the queue. `Relays::open` brings back every relay in `relays.json`, under the same id and holders. Keep the id, or ask again: the same shape and view find the same relay.

**After a purge** a relay holds nothing of what left, in memory or in its file, from the moment its `read` set's `cursor()` reaches the purge event's cursor. `Queue::purge` returns before that: wait for the cursor when it matters.

- When its worker is handed the rule event, in the pump the purge wakes (`Queue::purge` tells the relays), the relay drops everything it holds and removes its file before it reads anything. It then reads the graph again and saves at once, not when a save next falls due. A read that fails, or a save that fails, leaves no file, and an empty set until a read succeeds. The change it tells has no value from before: what stays is told as entered.
- A purge made while no relays run, or through another handle: `Relays::open` finds the rule event after a file's cursor, removes the file and seeds again before it returns. Purge through another handle while relays run is told only at the queue's next commit, so use `Queue::purge`.
- `reread` replays the purged log, so it answers without what left, even for a report made before the purge; a body a rule refuses during a replay does not stop it.

The guarantee is about the file: removing and replacing it is all rannoch does. It does not overwrite the old file's blocks, so what the disk beneath keeps after an unlink is the disk's. A set an app took from `read` before the purge is the app's.

A relay's file (`<id>.json`) only saves time, and is replaced at most once a second, or at once after a purge. A relay closes events only after saving them, so its worker is never ahead of its file. After a restart it is handed again what it had not closed. A file missing, unreadable or behind is reseeded from the graph. Until it folds what it was handed, `read` returns the set as of the file, so wait for `cursor()` if that matters.

## Drop

```rust
relays.release(&relay, APP)?;
assert!(relays.read(&relay).is_none());
```

`release(id, holder)` takes one holder off; the holder is the `source` of the query that asked. When the last holder goes, the relay's worker and file go with it.

A relay is a worker, and it holds the [processed mark](inbox.md#the-processed-mark) back like any other, by about a second while it runs, since it closes events only after a save. A relay nobody needs holds the mark and costs a fold per commit, so release it.

## What was measured

From the relay toy, on [`toys/relay/RESULTS.md`](https://github.com/runlegion/rannoch/blob/toys/relay/toys/relay/RESULTS.md) on the `toys/relay` branch. Its predictions were committed before any run.

**The numbers come from a stand-in shape.** The data is real: two builds of one game's data, `spice-7.9.a-v7` and `spice-7.8.1.c-v26`, which are not in this repo. The shape is not an app's own. It roots on a `planet` node keyed `hutta`, taken from the location segment of each object's fully qualified name, where the app that asked would root on `planet:<area>`. Every located object is a `thing`, with a `kind` and an indexed `logname`. The steps are `on<-`, dropping `kind` Spawn, then `targets`. That reaches 902 nodes: 1 root, 860 at the first step and 41 at the second. "Line to change" below runs from `accept_from` to the change-event callback with 4 other workers draining, so it measures the relay, not any app's parser or display. An app's real path stays that app's to measure.

Three runs on `feat/151-relay` at f1fd2df, release build, default 20 ms heartbeat, graphs on an internal disk, with other agents on the machine (load average 4.1, 2.8 and 11.8 at the start of each). Every judged line passed in all three. `crates/rannoch-relay` and `crates/rannoch-queue` have not changed since; `crates/rannoch-store` has, for #196.

| What | Result |
|---|---|
| Seed the 902-node relay, through the inbox | 44-46 ms |
| `read` | p50 41 ns, p99 42 ns |
| `by` on an indexed property | p50 83 ns, p99 125-167 ns |
| Round trip, query to answer, over 50 trips | median 27.2-28.3 ms, p95 49.3-54.3 ms, p99 68.0-84.3 ms |
| Line to change | p50 19-20 ms, p99 26-32 ms, max 57-60 ms |
| Rebuild with its file gone | 20-21 ms |
| `reread` | 1.33-1.97 s; earlier runs replayed 54,000 events to do it |
| Memory of a capped step at the largest hub: 703 found, 500 kept, 513 in the set | resident size up 1.3-1.6 MB, file about 423 KB |

The round trip is mostly the wait for a heartbeat (up to 20 ms) and two commits, so a slower disk costs more; on a USB disk an earlier run had a p50 of 65 ms. An importer's prediction of under 40 ms was met at the median and not at the tail.

Two measures were reported and not judged, because they swing for a reason not found:

- **Applying an event, the fold plus the walk, one link per commit:** p50 1.49-1.75 ms, p99 2.49-3.09 ms. It sits near 0.5 ms in some runs and near 1.8 ms in others, and neither load nor the build separates them.
- **An importer's in-process read of 216 links, read once a run:** 2.28, 0.91 and 2.43 ms. Its prediction of under 2 ms did not hold.

A larger run, [`WALKS-RESULTS.md`](https://github.com/runlegion/rannoch/blob/toys/relay/toys/relay/WALKS-RESULTS.md) on the same branch, used one store of 247,404 events from `spice-7.9.a-v7`, and its neighbourhoods are stand-ins too: the same planet shape and a material hub, at sizes from 7 to 51,243 nodes. It compared the warm set with a rewalk and with a recompute through the store's reads: 0 differences over 1,368 comparisons, all in the view "now". It ran on an earlier build and was not rerun at f1fd2df. What it says about size:

| Nodes held | An event on a held node or link, p50 | An amendment, which reads everything again, p50 |
|---|---|---|
| 902 | about 0.4 ms | about 19 ms |
| 4,296 | about 2.2 ms | about 116 ms |
| 51,243 | about 30 ms | about 1.5 s |

The 0.4 ms at 902 nodes is the low end of the swing above, on that earlier build. An event that touches nothing the relay holds cost 3 to 9 µs at every size, and no read of the graph.

## What these runs do not show

- No relay has been run under `kill -9`. The [release check](https://github.com/runlegion/rannoch/blob/chore/187-release-check/toys/live/RESULTS.md) did not include one. Tests in `crates/rannoch-relay/tests/relay.rs` cover a restart after a clean close, a file gone, and a file behind.
- The toy's runs do not take the failure paths above. Tests cover those.
- A relay is one view of one shape. There is no paging, and `by` is equality on a property the class indexes.
