# Worker toy: results

Run 2026-11-06 on rannoch main fabf9fd (v0.5.0, #213, #215), outside the rannoch crates, against the predictions in [PREDICTIONS.md](PREDICTIONS.md) as committed in legion (`evidence/worker-toy/PREDICTIONS.md`). Built fresh: no earlier toy or PR was read or ported. `./run.sh` rebuilds Westwind and runs everything; each run's output is under `out/`.

The machine was not quiet. Dozens of agents were running, and every speed line carries its load average.

## The data

Westwind was rebuilt from the generator in research document 01a1b37a-e007-75e1-8cbc-8f029c6d397a. Its `seed.sql` hashes `6d38971dc46a9677b46d8b6b072c88fdc6af67d381b4d731316064c166e60e71`, the document's hash, and the toy checks the hash on every load.

- Each row enters as a typed CloudEvent: `westwind.category.listed`, `supplier.joined`, `product.listed`, `customer.joined`, `employee.hired`, `shipper.joined`, `order.placed`, `order_line.added`. A shipped order also enters as `westwind.order.shipped` at its shipped date, carrying its whole row.
- The ontology enters first, as 17 `westwind.ontology.declared` events. It has 9 classes and 8 relations, which declare the links Westwind's tables leave implicit, `employees.reports_to` among them.
- The seed log has 1,496 events.
- Each event's time is inside it: the `time` attribute, and the millisecond of its `UUIDv7` id. The id is minted from the event's source, position and content, so the same row is the same event on every run.
- For speed, `westwind::extended_events` extends the order history from Westwind's customers, employees, shippers and products with a fixed seed (SplitMix64). It reaches 1,000,788 order lines and 1,754,619 events in all.

### Parameters, not outcomes

The predictions name no number for these. The SQL checks use the same values, so the values cannot move pass or fail.

- **Reorder level:** 20% of a product's starting `in_stock`. Westwind has no reorder-level column. `stock.low` is sent when a line takes stock from at or above the level to under it.
- **Unshipped:** 30 days after the order was placed, by event time. Westwind ships within 1 to 20 days, so on the seed "unshipped" is exactly `shipped_date IS NULL`.
- **Shipping:** it enters as its own event, `westwind.order.shipped`.

### Where the build differs from the predictions' sketch

- **Two flows:** the predictions describe one flow guarding `order_line.added`. The toy has two, because their instances key on different things. `flows/stock.jsonl` is keyed by `product_id`, with a host step and an emit. `flows/unshipped.jsonl` is keyed by order id, with an after row on `order.placed`, a shipped row, and an emit plus a store step when the timer fires.
- **One thread:** handlers run on one thread, in log order, and an emitted event is routed before the next one from the log. A supervisor is a `catch_unwind` boundary with a restart from the handler's durable place, not a process. Parallel handlers were not tested.
- **Batched store:** store is fill applied to routed events, through rannoch's public API: `Graph::commit` with the router's position, then `Graph::fill`. Both happen at each checkpoint, every 4,096 events from the log, not per event. A per-event commit plus fill would cost two synced transactions per event.

## What runs

| Part | Where |
|---|---|
| Entrance: attributes read once; travels as type, source, subject, body | `src/entrance.rs` |
| Router: type pattern (a type, or a prefix with `*`) to handlers, from what they register | `src/runtime.rs` `Router` |
| Supervisor per handler, one_for_one, with a bin after 3 crashes on one event | `src/runtime.rs` `Supervisor` |
| Built-ins: store and declare | `src/store.rs` |
| Flow runner: JSONL rows of `on`, guard, step, `next`/`else`; instance by declared correlation; rebuild by replaying history | `src/flow.rs` |
| Steps: emit, host, after (timer on event time), store | `src/flow.rs` `Runner::step` |

There are 12 unit tests. They cover:
- routing;
- an event that keeps crashing one handler going to its bin while the other handler carries on;
- a restart handing again what came after the durable place;
- an event handed twice changing nothing;
- an instance and a timer rebuilt from history equalling the live ones;
- a timer firing on event time and never by the wall clock.

## Correctness on the seed (`out/correct.txt`)

- `westwind.stock.low`: 19 lines from the flow, and the same 19 lines in the same order from SQL. The SQL is a window sum per product, ordered by order date and then seed row.
- `westwind.order.unshipped`: 37 orders from the flow, and 37 from SQL. The seed has 37 orders with `shipped_date IS NULL`.
- Fill stopped on no events. No handler restarted or binned anything.

## Predictions

All speed lines are from `out/speed.txt`, on the extended log (1,754,619 events, 1,000,788 order lines), using a release build. The run was stopped at the lead's request after P4, before the full-path line ran.

### 1. One attribute read per event, counted

**Holds.** The entrance counted 1,754,619 attribute reads for 1,754,619 events (and 1,496 for 1,496 on the seed). No handler reads an event's attributes again; emitted events are built as `Event`s and never parsed. The count leaves out one thing: fill parses every `rannoch.*` body again inside the store, which is rannoch's own read, behind the store built-in.

### 2. Routing costs under 1 microsecond per event

**Holds: 24.0 ns per event**, best of 5 over all 1,754,619 events (load 4.26). The table resolves each type once and caches its handler list.

### 3. A flow with no store step at 500,000 events a second or more; the full path at about fill's speed, near 80,000 a second

**Flow with no store step: refuted.**

| run | from JSON (entrance included) | past the entrance | load |
|---|---|---|---|
| 0 | 262,941 /s | 392,893 /s | 4.24 / 5.48 |
| 1 | 289,400 /s | 403,338 /s | 4.58 / 5.27 |
| 2 | 303,106 /s | 411,649 /s | 4.95 / 5.17 |

The path measured is entrance, router, supervisor, the stock flow (a host call, a guard and an emit on each line), its JSON history and the collector. About 750k of the events (placed, shipped) route to no handler here. Taking the entrance's JSON parse out gets to about 400k/s; most of what is left is serialising one history entry per event. The machine was loaded, so a quiet machine might come closer. Even so, 500k is not reached with a durable history.

**Full path: not measured** (stopped). It is bounded by store, and store is nowhere near 80,000 a second. On this machine fill ran at **4,846 to 6,172 rannoch events/s**, which is 1,715 to 2,184 Westwind events/s (each Westwind event says about 2.8 rannoch events). The "near 80,000" figure does not hold for v0.5.0 plus #213 on this store and data: about 5 million bodies, `synchronous=FULL` and `fullfsync=ON`, and a log that grows to millions of hearings. That is a finding about fill, not about the router.

### 4. Store through the router no more than 20% slower than v0.5.0's fill on the same events

**Holds.**
- **Hard-wired:** the rannoch bodies are translated beforehand (untimed), then committed and filled at the same checkpoints, every 4,096 events, with no entrance, router or supervisor.
- **Routed:** the same events go from JSON through the entrance, the router, the supervisor and the store built-in, which translates, commits and fills.
- The runs were interleaved: hard-wired, routed, hard-wired, routed, and so on.

| run | hard-wired | routed | routed against hard-wired | load (wired / routed) |
|---|---|---|---|---|
| 0 | 1023.29 s | 1045.75 s | +2.2% | 3.67 / 3.54 |
| 1 | 974.35 s | 978.47 s | +0.4% | 3.79 / 3.47 |
| 2 | 1642.16 s | 1269.04 s | -22.7% | 12.41 / 6.15 |

The median is +2.2%. Run 2 is noise from load: the hard-wired run sat at load 12-14. Fill alone (every body committed first, then one fill, timed by itself) took 803.44 s, at 6,172 rannoch events/s (load 4.53). The router, entrance and translation cost almost nothing next to commit and fill.

### 5. Replays leave the store byte-identical; a rebuilt instance equals one that never stopped

**Byte-identical file: refuted, by construction.** The file's SHA-256 differs on every run, including two in-order single passes of the same log, for three reasons:
- the log only grows, so every replay adds to it;
- every log id is minted by `EventId::now()` at append (`crates/rannoch-store/src/log.rs:34`);
- class tables key rows by arrival rowid (`crates/rannoch-store/src/queries.rs:699`).

**The store as read: refuted by one field.** `canonical` reads, in key order through the public API, every node's `find`, `hearings` and `seen`, and every link's state and hearings. That text also differs on every run, because `Node.recorded` is the wall-clock time of the log append (it read 2026-11-06 on rows from 2025), not a time inside the event. Hearings' `known` does come from the event's id, as #213 has it.

**The store as read without `recorded`: holds for every in-order replay.** In-order means the log in event-time order, as the predictions' replays are. The lead decided this comparison is the one that says whether two replays leave different stores.

| run | file | graph as read | graph without `recorded` | unshipped nodes | stock flow | unshipped flow | timers fired |
|---|---|---|---|---|---|---|---|
| in-order x1 | 427883610c494aa0 | a82203ac10bd275c | **b0a43da17286bd7d** | 78c20cf123f80f73 | f845844a8f6a4248 | 852c748deb6c24c9 | 23f0a7b80a8c10d9 |
| in-order x1, again | 59ddb62cbabf0a72 | 30ec6689454701a8 | **b0a43da17286bd7d** | 78c20cf123f80f73 | f845844a8f6a4248 | 852c748deb6c24c9 | 23f0a7b80a8c10d9 |
| in-order x10 | a16a9ccf488f95de | a1ae877ffda4962d | **b0a43da17286bd7d** | 78c20cf123f80f73 | f845844a8f6a4248 | 852c748deb6c24c9 | 23f0a7b80a8c10d9 |
| in-order x1000 | 63c8785644c3e995 | 133672b14d2a6134 | **b0a43da17286bd7d** | 78c20cf123f80f73 | f845844a8f6a4248 | 852c748deb6c24c9 | 23f0a7b80a8c10d9 |
| stopped in process, 8 random stops | ea74c001c74ef7b2 | 7460da1299faa64f | **b0a43da17286bd7d** | 78c20cf123f80f73 | f845844a8f6a4248 | 852c748deb6c24c9 | 23f0a7b80a8c10d9 |
| killed in order, SIGKILL x6 (4 mid-run) | f079f10a5cc8a39d | 9648e425b4067239 | **b0a43da17286bd7d** | 78c20cf123f80f73 | not run | 852c748deb6c24c9 | 23f0a7b80a8c10d9 |
| shuffled, seed 1 | 0038e21cd6fd8a7e | cf00e5064ba1d5a5 | c90b7691fe29371f | 11b2488c0c5e766b | fcdb5fbc2007fd89 | 9afff927fbc3817a | ef1701816ccb641a |
| shuffled, seed 2 | d5bc96aa03f943aa | a8d54768de18d78b | a995ccbaad068417 | f1c2852eb03a69f7 | fb2257d47ac709f0 | 61b127a7f6227f8e | 09ffc8db9b7347b6 |
| shuffled, seed 3 | 4cbd4f47ef7458f1 | 74ec968d882a9cff | f200d496314f6681 | 063810e1b9745d5d | 7626acf2a50beded | 775b5f4f4bc0ea27 | 40ef62ef4e74ae20 |
| killed, shuffled seed 1, SIGKILL x6 (4 mid-run) | bf808254ed5be605 | 6b66a4b424c353f1 | c90b7691fe29371f | 11b2488c0c5e766b | not run | 9afff927fbc3817a | ef1701816ccb641a |

Each cell is the first 16 hex digits of a digest; `out/replays.txt` has the run, and each run's directory under `out/` holds the texts. The killed runs leave out the stock flow: its warehouse is the host's, outside the store, and a killed process takes the host's memory with it. Killing a host is a test of the host, not of the router.

**Shuffled.** The ontology's declarations stay first; everything after them is shuffled.
- **The graph:** it differs from in-order only in `seen.first_recorded`, which is which version of a node arrived first. That is arrival order, so it is not a time inside the event. With `first_recorded` also left out, all three shuffles and the killed shuffle read identical to in-order (checked on the dumped texts). Each node's current version and every hearing are the same.
- **What a shuffle changes:** the flows' outputs, which the guarantee leaves to the flow. stock.low still gives 19 crossings, but on different lines: 0, 3 and 1 of the in-order 19. order.unshipped reports 129, 115 and 90 orders against the true 37, because the event clock jumps ahead, a timer fires before its order's `shipped` arrives, and the flow reports an order that did ship.
- **A kill does not change a shuffle:** killing a shuffled run leaves exactly what the same shuffle leaves unkilled.

**Rebuilt instance: holds.** Both flows rebuilt from their history files equal the live instances: history, events handed, types seen, timers, timers fired and clock. They also equal one another across stops and kills. A rebuild runs the same rows again with each step answered from the record, and it refuses a history whose rows replay differently.

Two bugs in the toy's first runner were caught by these replays and fixed before the table above:
1. **A rebuilt timer sent nothing.** A timer's result recorded only its due time, not the data it sends. A rebuilt timer sent `null`, its due event keyed to the wrong instance, and after a stop 36 of 37 `unshipped_order` nodes were missing. The after step's result now carries its data.
2. **History could get ahead of the store.** The history file was buffered and could reach disk before the store writes of the same steps committed, so a crash could leave the history saying a step was done that the store never got. History now reaches its file only at a checkpoint, after the store commits.

### 6. Timers count from event time, so a replay fires the same timers in the same order

**Holds for replays of the same log order.** The sequence of fired timers (due, instance, row) is identical across in-order x1, x10 and x1000, the in-process stops and the kills: `23f0a7b80a8c10d9`. A timer fires only when an event's time passes its due time, never by the wall clock (test `a_timer_fires_on_event_time_unless_the_order_shipped`).

**Does not hold across arrival orders.** Each shuffle fires a different sequence. The event clock is the highest event time seen, so a log out of order moves it early, and timers fire before the events that would have cancelled them. The killed shuffle fires the same sequence as the same shuffle unkilled.

## What breaks the idea

- **Store through the router more than 20% slower than the hard-wired fill: did not happen.** It was +2.2% and +0.4% at comparable load, and -22.7% when the hard-wired run sat under load 12.
- **Two replays of the same log that leave different stores:**
  - The file bytes always differ.
  - The graph as read differs only by `Node.recorded`, a wall-clock field in rannoch's store.
  - With that field left out, no two replays of the same log differed. This covered 1, 10 and 1,000 passes, random stops and SIGKILLs, both in order and shuffled.
  - Different arrival orders leave the same graph except for `seen.first_recorded`.
- **Rebuilding an instance gets slower without bound as its history grows: it does, linearly** (`out/rebuild.txt`). It is about 2 us per entry: 0.25 ms at 100 entries, 2.1 ms at 1k, 20 ms at 10k, 207 ms at 100k, and every rebuild equals the live instance. Cost per entry is flat, so it never gets worse than linear, but there is no bound without a snapshot.

## Not done

- No parallel handlers: everything runs on one thread.
- Fill's own attribute read is outside the entrance's count. Fill parses every `rannoch.*` body again inside the store (`rannoch_cloudevents::read`).
- No supervision of the host: the warehouse remembers its answers by event id in memory only.
