# Chain toy: results

Run 2026-11-06 on rannoch main 7aa6841, outside the rannoch crates, against [PREDICTIONS.md](PREDICTIONS.md) as committed in legion at 6902038c. The toy builds on the worker toy's router, supervisors and flow runner, changed where the worker toy's findings required. The cast's expected trails were committed (df7c9e3) before any run. Outputs are under `out/`: `run.txt`, `replays.txt`, `age.txt`, `domain.txt`, `load.txt`.

Nothing else was building during any run. The machine's load average still ran from 4.5 to 12, from other agents, and every line below gives it.

## What runs

- **Data.** Westwind's world is rebuilt to fit the events (`src/world.rs`), from the seed whose SHA-256 matches research document 01a1b37a.
  - Each event is one thing that happened. `order.placed` carries its lines, `carrier.collected` its shipment, `ticket.opened` its text.
  - Suppliers, products, employees, shippers and customers enter as `joined`, `listed` and `hired` events.
  - The 320 seed orders are placed, paid an hour later, collected on their shipped date and delivered three days after. An order never shipped is never collected.
  - The cast of 13 stories and Oz are added, plus a tick a day for ten years, so event time reaches Tyrion's last month.
  - The world comes to 5,329 arrivals.
- **The chain:** 11 flow files (`flows/`).
  - The stages: accept, allocate (fan-out by line), stock (keyed by product), pick and pack (a join over every line, with split shipments), ship, deliver, after-ship, support, returns.
  - Two more: cancel, and subscription.
  - No flow names another; a flow that emits a type of another domain is refused when the flow is read.
- **One scheduler** (`src/runtime.rs`).
  - Outside events, emitted events and timers wait in one queue, ordered by event time and then id.
  - An event is released once its time is under the latest outside time, less the declared lateness (2 days).
- **The entrance** reads each event once and bins it with one of three reasons: malformed (not a CloudEvent 1.0, or its data fails its type's schema, or its type has none), another domain, or later than the declared lateness.
- **Store built-in** (`src/store.rs`):
  - Two ontologies share the store: Westwind with 11 classes and 8 relations, Oz with 2 classes and 1 relation.
  - Every class and relation is prefixed by its domain.
  - An order is a node with its lines held as JSON text, since 7aa6841 has no structured property type, and edges join it to its customer and to each product.
- **Host actions** (`src/host.rs`) are pure functions of the step's input and the instance's variables.
  - Each call runs on a host thread under a 300 ms deadline.
  - The host remembers each answer by action and event id, so a call again for the same event does no work twice.

### Changes from the worker toy its findings required

- **Timer data:** a step's history entry records every event it sends, in full: emitted events and timers. An event handed again sends again what its entry records.
- **Durability order:** at a checkpoint, the log is written first, then the store, then each flow's histories, then the runtime's place.
- **Stock in flow state:** stock lives in a flow instance keyed by product, not in the host, so a kill takes nothing that is not rebuilt.
- **Carry:** each instance keeps its own history file and carries its state into a fresh one every 64 entries.

## Predictions

### 1. Every cast member's trail matches its expected events in order, rebuilt from the log alone

**Holds: 13 of 13.**
- **How the trail is rebuilt:** each trail is rebuilt from `log.jsonl` alone. Each event's `causation` attribute is followed back to the story's root, which is the `order.placed`, or for Tyrion the `subscription.started`.
- **How it is compared:** in event time, ordered by type within one millisecond, against the trail the script expects.

| story | events |
|---|---|
| Cersei | 30 |
| Jon Snow | 43 |
| Davos | 16 |
| Tyrion, 120 months | 1,921 |
| Arya, cancelled before paying | 4 |
| Arya, cancelled after packing | 17 |
| Arya, cancelled while backordered | 10 |
| Arya, cancelled after shipping (refused) | 17 |
| Daenerys | 6 |
| Hodor | 21 |
| Lion, abandoned | 3 |
| Lion, unanswered | 23 |
| Tin Man | 18 |

### 2. The seeded orders' outcomes match SQL over the same seed

**Holds: 320 of 320.**
- SQL over `seed.sql` gives 283 shipped orders, delivered three days after their shipped date. 100 of those arrive after their required date, and 37 orders were never shipped.
- Each matching order has `order.delivered` at exactly that time, `order.late` exactly when SQL says so, and `order.closed`.
- Each unshipped order has `order.unshipped` and no delivery.

### 3. Every emitted event is a valid CloudEvent 1.0, and every type's data passes its schema

**Holds.** 6,749 emitted events of 38 types: 0 invalid. Every emitted event goes back through the entrance's own reading, its domain check and its type's schema (`schemas.json`, 57 types). A type with no schema counts as a failure.

### 4. No event lands in an ontology it does not belong to

**Holds.**
- **Bins at the entrance:** 4 for another domain, 4 malformed, 1 later than the lateness. Each reason is distinct.
  - The Scarecrow's `westwind.order.placed` and `westwind.ticket.opened` from `oz/emerald-city` went to the bin.
  - So did an `oz.order.placed` and an `oz.citizen.joined` sent from `westwind/web`, which crossed the other way.
  - The malformed ones: a line that is not JSON, `specversion` 0.3, an `order.placed` missing its lines, and a type with no schema.
- **Hearings in the store:** 4,974 in Westwind's classes and 5 in Oz's. The 5 are Oz's own three citizens, Dorothy's order and its edge.
- **No cross-landings:** no hearing in either domain's classes came from the other domain's sources, and the Scarecrow's order is not in Westwind's orders.

### 5. Checking an event's domain costs under 100 ns per event

**Holds: 10.9 ns per event**, best of 20 by 20 passes over the 5,327 arrivals that parse (load 12.45).

### 6. A duplicate `id` changes nothing anywhere; two identical tickets with different ids are two tickets

**Holds.**
- **Hodor's delivery:** it arrived three times under one id: once on time, once an hour later, and once three days later, after its release. The two extra copies were dropped as duplicates.
- **With and without the copies:** the run with the copies and a run without them leave the same store, flow states and outcomes (see the table under 12).
- **Hodor's tickets:** his two identical tickets are two `westwind_ticket` nodes. Each is triaged and closed once.

### 7. The Tin Man's stalled handler is restarted, his event handed again, and the work happens once

**Holds.**
- **The restart:** his triage stalled; the host call missed its 300 ms deadline. The support flow's supervisor restarted it once, with the reason "host action triage stalled: no answer within 300 ms", and handed his ticket again. His trail is complete (18 events).
- **The work, counted per ticket:** triage was done exactly once for each of the 5 tickets. The stalled thread is parked for good and never does the work.
- **Bug found on the way:** the first run showed 4 other tickets triaged twice. The restart had handed again tickets from before the stall that were not yet durable. The fix was the host remembering each answer by event id.

### 8. The Lion's open ends all close by timer, by event time

**Holds.**
- The abandoned order closes with `order.abandoned` at day 1.
- The approval nobody answers closes with `remake.expired` at day 9 after the order: three days after the ticket.
- The return never sent closes with `return.expired` at day 20: fourteen days after the ticket.
- All three are timers released by event time.

### 9. Late events: a shuffled replay within the declared lateness gives the same alerts and outcomes as an in-order one

**Holds.**
- Three shuffles each moved every arrival up to 2 days behind its time, at random. The largest lateness seen was 1.62, 1.67 and 1.62 days.
- Each shuffle leaves the same store, the same flow states and the same outcomes as the in-order run. The outcomes are every alert and stage event, by order or ticket, with its time.
- **Against the worker toy:** its full shuffles reported 90 to 129 unshipped orders against 37. Here the unshipped alerts are the same 37 in every order.
- **The late event:** one deliberately late event, a customer arriving ten days after its time, went to the bin as later than the lateness in every run.

### 10. Tyrion's subscription rebuilds in the same time in year 10 as in year 1

**Holds with a carry below a year's entries. With the carry at 64, the history is bounded, but not equal at year 1.** Rebuild time of Tyrion's subscription instance from its own history file, best of 50 (`out/age.txt`, load 8.8 to 10.6):

| carry | year 1 | year 5 | year 10 |
|---|---|---|---|
| none | 13 lines, 240 us | 61 lines, 990 us | 121 lines, 1,938 us |
| every 64 entries | 13 lines, 246 us | 61 lines, 988 us | 58 lines, 918 us |
| every 12 entries | 2 lines, 31 us | 2 lines, 30 us | 2 lines, 21 us |

- **Without a carry:** rebuild time grows linearly with age, as in the worker toy.
- **Carry every 64 entries:** the history never passes 64 entries, so year 10 rebuilds in the same time as year 5. Year 1 (13 entries) is under the threshold.
- **Carry every 12 entries:** year 1 and year 10 are the same.
- **Bug found on the way:** the first carried state grew to 46 KB by year 10. The subscription kept every payment timer it had sent to the accept flow, which never reports back. An instance now forgets a timer once its time is more than the window behind the instance's clock; by then the timer has been released.

### 11. Everything a step sends is in its history, and a history entry is durable only after the store writes it records

**Holds by construction, and the replays below test it.**
- **Everything sent is recorded:** a history entry records each emitted event and timer in full. A rebuild runs the rows again with each step answered from the record, and refuses a history whose rows would send anything different. No rebuild was refused in any run, stop or kill.
- **Store before history:** at a checkpoint the store is committed before any flow's history.
- **Bug the kills caught:** the first kill run missed 5 timers from the log, because the log was written after the handlers. If a kill landed between the two, a timer already handed was never logged. The log is now written first.

### 12. Replayed 1, 10 and 1,000 times, stopped and killed, the full chain leaves the same store contents and flow states, all but `recorded`

**Holds.** These are the digests of the store (every class and relation table by key, and every hearing, leaving out rowids and `recorded`), of the flow states (every instance rebuilt from its history), and of the outcomes, from `out/replays.txt`:

| run | store | flow states | outcomes | note |
|---|---|---|---|---|
| in order x1 | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | |
| in order x10 | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | passes 2 to 10 binned as late, or dropped as duplicates |
| in order x1000 | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | 5,311,684 binned as late, 3,998 duplicates; 14 s |
| stopped x8, in process | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | |
| killed x6, in order | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | 6 of 6 SIGKILLs landed mid-run |
| killed x6, shuffled | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | 6 of 6 SIGKILLs landed mid-run |
| shuffled within lateness, 3 seeds | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | |
| without Hodor's duplicate copies | 40f13da57bb77d93 | 647bdcfc1d4156fc | a8184fc0102a6e76 | |

- **Why the later passes change nothing:** a pass after the first is later than the lateness by construction. It goes to the bin as late, or is dropped as a duplicate within the window.
- **Third bug:** the runtime now checkpoints at least once per lateness of event time. Before that, a supervisor restart could hand again events older than the windows that recognise them as handed. Cersei's return was authorized twice before the fix.

### 13. The event-shaped stream stores an order at least 3 times faster, in less than half the bytes, than the row-for-row load on the same main

**Refuted on both counts** (`out/load.txt`).
- **The data:** the same 400,839 extended orders, reshaped. Each is one `order.placed` with its lines inside, and each shipped order one `order.shipped`, after the same suppliers, products, customers, employees and shippers.
- **The load:** loaded into the store through the store built-in's translation, with commit and fill every 4,096 events, and nothing else building.

| | orders/s | bytes per order |
|---|---|---|
| row for row, nodes-only, 7aa6841 (graph-vs-relational toy) | 1,712 | 6,573 |
| target | 5,135 or more | under 3,287 |
| event-shaped, 7aa6841 | **1,122 (0.66x)** | **8,913 (1.36x)** |

The event-shaped load took 357.30 s for 753,806 events (load 6.84 at the start, 4.46 at the end), and fill stopped on no events. An order node holds its lines as JSON text, which rannoch keeps three times: the log body, the hearing's properties and the class table. Each order also adds one edge to its customer and one per product, about 3.5 more rannoch events per order, each with its own log body and hearing. Shaping events after the business does not make rannoch's store cheaper per order. Its cost per fact, which the graph-vs-relational toy measured, is still where the time and bytes go.

## What breaks the idea

- **An event lands in another ontology's graph.** It did not: 0 hearings across domains, and all 4 cross-domain events binned.
- **A stage has to name the stage after it.** None does. Each flow starts only on types, and the chain runs from `order.placed` to `order.closed`, and through every cast path, without one.
- **A shuffled replay within the declared lateness gives different outcomes.** It did not: three shuffles, each the same as in order.
- **Rebuilding a long-lived instance still grows with its age.** It grows only without a carry. With a carry, it is bounded by the carry's size; with a carry of 12 it is the same at year 1 and year 10.

## Not done

- **Parallel handlers:** everything runs on one thread in event-time order.
- **Host memory across a kill:** the remembered host answers live in memory. The host actions here are pure, so a kill changes nothing, but a host with real side effects would need its idempotency keys kept durably.
- **Extended-scale chain:** the full chain was not run on the extended log, only on the seed and the cast. P13 loads the extended orders through the store alone.
