# Two-lane toy: results

Run 2026-11-06 and 07 in `toys/two-lane` on rannoch main 7aa6841, against [PREDICTIONS.md](PREDICTIONS.md) as committed in legion at d6f443f1. Rannoch's crates are not used. The store is the toy's own, on SQLite, built to the predictions' shape.

Legion's later commit 0ddeb25e adds that flows run on the row the worker just wrote, handed in memory, and that nothing reads a row back after writing it. The toy does this too: a row this process has written stays in memory and is never read back from the file. A row it has not seen yet is looked for once.

The toy reuses the chain toy's world, cast, expected trails, schemas, host actions and flow rows. Each flow header now names its row and its ordering. Outputs are in `results/`.

Nothing else was building during any timed run. The load average from other agents ran from 3.3 to 9.6 and is on every line.

## The shape as built

- **Inbox:** an append-only file of CloudEvents, synced once per batch taken.
- **Rounds:** the lane reads the inbox in rounds. Each event is checked as it is read, all of it from the event itself, before anything is written:
  - a CloudEvent 1.0;
  - its type's domain is its source's;
  - its data passes its type's schema;
  - an outside event is no later than the lateness, 2 days.
  An event that fails goes to the bin with its reason.
- **One write per fact.** An event's fact is written as its row: kind and key, with the event that last made it, its time, and a JSON body.
  - **Order row:** for an order, the body is its placing (lines included), the types of event that have happened to it with their latest times, the ids of facts applied within the window, and each flow's state.
  - **Edges:** an order has one edge to its customer and one per product. An edge holds only `(src, rel, dst)`, and its relation names both kinds.
  - **Not on the main path:** no log table, no hearing table, no second copy of a row.
- **Flows (`flows/`)** run after the write, on the row in memory. Each flow's state is a part of the row it is keyed to: an order, a product, a ticket or a subscription.
  - What a step sends goes on the inbox as a CloudEvent. A step run in the same round as the step that sent it is marked `"inline": true` on the inbox, so a restart never runs it twice.
  - A timer is a CloudEvent whose time lies ahead. It is delivered when event time reaches it.
- **Ordering, declared per flow.**
  - Clock: accept, stock, cancel, and ship's `address.changed` and `order.cancelled`.
  - Dependency:
    - pick and pack waits for `order.accepted`;
    - ship waits for `order.packed`;
    - deliver waits for `order.shipped`;
    - support's approval waits for `remake.requested`;
    - returns waits for `return.authorized`.
  - Neither: allocate's lines, after-ship, subscription.
  - The lane's own code only holds an event: in event-time order until nothing earlier can still arrive within the lateness, or until the types its flow needs exist on its row.
  - Within a round, the clock queue is run one moment of event time at a time, with everything each moment sends, before the next moment.
- **Host answers** are kept by action and event in each shard's `host` table, committed in the same transaction as the rows.
- **Log:** a plain append-only stream, `log.jsonl`, written beside the fast lane and never read on it. The trickle reader (`src/trickle.rs`) follows it into `hearings.db`.
- **Durability.** A round is one durable step, in this order:
  1. what the round sent is appended to the inbox and synced;
  2. every shard commits its rows;
  3. the lane's place (inbox offset, the events still held) commits with the first shard.

  With more than one shard, a round spans no more than the lateness of event time. Whatever a restart hands again then stays inside the rows' dedupe windows.

## Predictions

### 1. Every cast trail and every seeded order matches the chain toy's expected answers

**Holds: 13 of 13 trails and 320 of 320 orders** (`results/run.txt`).
- **Trails:** each is rebuilt from the log alone through `causation`, and compared with the expected trails the chain toy committed from the script.
- **Seed orders:** each matches SQL over `seed.sql`: 283 delivered three days after their shipped date, 100 of them late, and 37 unshipped.
- **Against the chain toy:** the outcomes digest is the same, `a8184fc0102a6e76`. That is every alert and stage event by order and ticket, with its time.

### 2. The fast lane stores at least 40,000 facts a second on the extended orders, with the inbox durable

**Refuted at the batch size the earlier toys used. It holds at four times that batch.**
- **The data:** 753,806 facts: the extended orders' 400,839 placings with their lines, their shipments, and the suppliers, products, customers, employees and shippers.
- **The path:** each fact is taken onto the inbox, synced per batch, then read, checked, and written as its row and edges. One shard, no flows.
- **Durability:** shard commits use `synchronous=FULL` and `fullfsync=ON`, as the relational baseline did.

| batch | log | run 1 | run 2 | load |
|---|---|---|---|---|
| 4,096 | off | 33,060 facts/s | 33,636 | 3.8, 3.7 |
| 4,096 | on | 32,615 | 32,866 | 3.6, 3.3 |
| 16,384 | off | 49,795 | 49,393 | 3.4, 3.5 |
| 16,384 | on | 45,284 | 45,193 | 3.3, 3.5 |

- **Where the time goes:** syncs. A profile of the 4,096 run (`results/sample.txt`) has `__fcntl`, the full sync, at 2,848 samples, against a few hundred for all the work in between. JSON, SQLite b-trees and allocation add up to about 1,500.
- **What moves the rate:** each batch costs a sync of the inbox and a sync of the shard, so the batch size sets the rate.

### 3. The graph's file, without the log, is no more than 2 times relational's 471 MB

**Holds: 910.4 MB at batches of 4,096 (1.93 times), 898.6 MB at 16,384.**

| table or index | MB |
|---|---|
| `rows` | 491 (400,839 orders at about 940 B of body each, plus 81 customers, products, employees, shippers and suppliers) |
| `edges` | 192 (1,401,655 edges) |
| `edges_to` index | 188 |
| the rows' key index | 27 |

- **Beside the store:** the inbox (488 MB) and the log (488 MB) are append-only streams. Neither is part of the graph's file.
- **The first build** was 1,607 MB. It held rows in a `WITHOUT ROWID` table, whose large rows overflowed badly, and it gave every edge both kinds and an event id. Rows in an ordinary table and edges of three keys brought it under the line.

### 4. Writing the log beside the fast lane costs it under 5%

**Holds at batches of 4,096: +1.8%** (mean of two interleaved runs each).

**At 16,384 it is +9.6%.** With fewer syncs, the log's own writes stop hiding behind them.

### 5. The trickle reader keeps up, and its hearings answer who said what and when for every cast event

**Holds.**
- **Keeping up:** in every log-on run, the reader followed the log while the fast lane wrote it. It held all 753,806 hearings within 0.01 s of the fast lane finishing, at 33,000 and 49,000 facts a second.
- **Lag:** it was sampled every 20 batches and never exceeded 0 events. The samples are coarse, and only show the reader never fell a batch behind.
- **Who said what and when:** the cast run's hearings answer source and time for 2,129 of 2,129 cast events (`results/hearings.txt`).

### 6. No flow state outside the row: killing the process 10 times gives the same rows, emitted events and outcomes as no kill

**Holds** (`results/replays.txt`).
- **How it was killed:** a child process was aborted (`SIGABRT`, no cleanup) after a random 1 to 3 rounds of what was left, 10 times. Each time it resumed from its own files, then finished.
- **Result:** the same rows, the same set of emitted event ids and the same outcomes as an unkilled run, in order and shuffled.
- **What lives where:** every flow's state is in its row. The only other durable state is:
  - the lane's place, which is the inbox offset and the events still held;
  - the host's answers.
- **Bug found on the way:** the first kill runs moved Tyrion's later months by a month. A restart re-read timers its last committed round had already run inline, and the subscription row's window had forgotten them. Marking inline steps on the inbox fixed it.

### 7. Ordering per flow: clock, dependency and unordered flows each give the same outcomes under three shuffles as in order

**Holds.**
- **The shuffles:** three, each moving every arrival up to 2 days behind its time. The largest lateness seen was 1.62, 1.67 and 1.62 days.
- **Result:** each shuffle gives the same rows, emitted events and outcomes as in order.
- **The flows covered:** stock (clock), ship, approval and returns (dependency), and allocate's lines (neither).
- **First design failed:** releasing every clocked event below the watermark in one batch gave 8 of 13 trails. Arya's cancellation reached the cancel flow before the `order.accepted` it depended on, because that event had not been sent yet. The fix is to run one moment of event time, with all it sends, before the next.

### 8. Fan-out runs in parallel: 4 workers at least 1.5 times faster than 1, with the same outcomes

**Refuted: 1.21 times faster, same outcomes** (`results/parallel.txt`). The run was the full chain over the first 50,000 extended orders, 2,175,494 units of work, with each round bounded to the lateness on both sides:

| workers | run 1 | run 2 | load |
|---|---|---|---|
| 1 | 100.1 s (21,728 units/s) | 105.4 s | 3.9, 6.5 |
| 4 | 83.0 s (26,209 units/s) | 86.2 s | 4.0, 4.8 |

- **Outcomes agree:** both counts give outcomes digest `48e501467641f5b1`.
- **Rows and emitted ids differ:** the rows and the emitted ids differ between 1 and 4 workers.
  - **Cause:** the last line to arrive at an order, and so the cause and id of its `order.packed`, depends on which product shard answers first.
  - **What is the same:** the events, their types and times, and every outcome.
- **Why only 1.21 times:**
  - each round ends in syncs, which the profile shows dominate;
  - the shard that holds the lane's place commits after the others;
  - one dispatcher thread reads, checks and routes everything.
- **Before the shards committed together**, 4 workers were no faster than 1 (1.00 times).

### 9. Timers as future CloudEvents: the Lion's open ends close, and Tyrion's 10 years of monthly orders arrive, at their event times

**Holds.**
- **The Lion:** the abandoned order closes at day 1. The unanswered approval closes at day 9 and the unsent return at day 20, each by its timer.
- **Tyrion:** his subscription placed 120 monthly orders by timer, the last at day 3,570.
- **His trail:** all 1,921 events match.

### 10. A duplicate id changes nothing, and a host step's answer is kept by event id durably

**Holds.**
- **Hodor's delivery:** it arrived three times under one id. Copies inside the window changed nothing; the copy three days late went to the bin as later than the lateness.
- **Hodor's tickets:** his two identical tickets are two rows.
- **Host answers:** triage answers are kept for 5 tickets in every run, killed or not. The Tin Man's stalled triage was handed again and answered once.
- **Cersei:** her return is authorized once, in every run, killed or not.

## Finding: lines are JSON in the order's row

The shape keeps an order's lines inside its row. As in the chain toy, a line is not a fact of its own:
- the store reads one order's lines with the order;
- it cannot find a product's lines without reading every order;
- each product's stock lives in the stock flow's state inside the product row.

The edges say which orders hold a product, not how many of it.

## What breaks the idea

- **The fast lane has to read the log or a hearing to write a row.** It never does. The log is written beside it and read only by the trickle reader.
- **A flow needs state the row cannot hold.** None did. Every flow's state lives in its row:
  - each product's stock and the lines waiting on it;
  - each order's picked and packed lines;
  - a subscription's month;
  - each flow's handed window.
- **A shuffle gives different outcomes for a flow whose ordering covers it.** None did, in three shuffles.
- **The fast lane stays under 10,000 facts a second.** It did not: 33,000 at batches of 4,096, and 49,000 at 16,384.

## Not done

- **One dispatcher thread:** it reads, checks and routes all events; only rows are sharded.
- **Batch sizes:** none was tried between 4,096 and 16,384, nor larger, so the batch where P2 and P4 both hold is not found.
- **Host answers:** they are kept durably for every action, including pure ones that did not need it.
