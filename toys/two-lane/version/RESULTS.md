# Version toy: results

Run 2026-11-07 on branch `toys/version`, against [PREDICTIONS.md](PREDICTIONS.md) as committed in legion at 619d09b8. The toy extends the two-lane toy (toys/two-lane at 5579695) in place, in `toys/two-lane`, so the branch's diff is what the predictions name. Outputs are in `version/results/`.

Nothing else was building during any timed run. Load averages ran from 3.1 to 4.1 and are on every line.

## What changed from the two-lane toy

- **Versions** (`src/store.rs`):
  - A changed fact appends a version of its row to `versions(kind, key, axis, known, event, body)`.
  - `axis` is the value of its kind's declared axis: the build, for build objects; none (0) for Westwind's kinds.
  - `known` is the millisecond in its event id's clock.
  - That version is the fact's one write. A fact whose content is unchanged writes nothing.
  - The row as it is now is the version with the greatest axis, then the last written. It stays in memory once written, and is never read back.
  - Reads as of an axis, as known at a time, take the greatest axis at or before it, then the latest known at or before it.
- **Emitted ids** (`src/flow.rs`): an emitted event's id is a hash of its content (type, time, data, domain) and its cause's key, which is the row the step ran on. It no longer hashes the cause's event id, which depended on which event happened to come last.
- **The build stream** (`src/build.rs`), from a fixed seed:
  - 200,000 objects of about 3 KB of JSON each, in six builds.
  - Build 1 writes every object. Each later build sends every object again, with 2 to 5% of them changed.
  - After build 4, a fix pass rewrites 1% of the objects at build 4.
  - The build is the axis. No kessel data is used.

Three changes the predictions do not name, each needed for one of their checks:
- **Sorted pick lists:** pick and pack keeps its picked, waiting and packed lines sorted, so what it sends does not depend on arrival order (P9).
- **64 KB pages:** the store uses them, so a 3 KB version does not leave most of a 4 KB page empty (P2). `TWO_LANE_PAGE` sets another size.
- **Last writer:** a row records as its maker the latest event by time, then id, not the last one handled (P9).

## Predictions

### 1. Builds 2 to 6 write exactly the objects that differ

**Holds**, in all four runs (`version/results/builds.txt`):

| build | objects sent | versions written | generator's changed count |
|---|---|---|---|
| 1 | 200,000 | 200,000 | 200,000 |
| 2 | 200,000 | 8,000 | 8,000 |
| 3 | 200,000 | 10,000 | 10,000 |
| 4, with its fix pass | 202,000 | 12,000 | 10,000 changed + 2,000 fixed |
| 5 | 200,000 | 10,000 | 10,000 |
| 6 | 200,000 | 8,000 | 8,000 |

The other 190,000 to 192,000 objects of each later build wrote nothing.

### 2. Each later build grows the store by no more than 1.2 times the bytes of its changed objects

**Holds: 1.16 to 1.19 times.** Growth of the store file is measured after each build's write-ahead log is folded in.

| build | grew | changed bytes | ratio |
|---|---|---|---|
| 2 | 27.6 MB | 23.8 MB | 1.16 |
| 3 | 34.5 MB | 29.8 MB | 1.16 |
| 4 | 42.7 MB | 35.7 MB | 1.19 |
| 5 | 34.9 MB | 29.8 MB | 1.17 |
| 6 | 27.9 MB | 23.8 MB | 1.17 |

With 4 KB pages, the 2,000-object trial grew 1.38 to 1.43 times; 64 KB pages brought it to about 1.10.

What the excess over 1 is made of:
- the row's envelope beside the object: its types, the facts window and its event;
- the version index on `(kind, key, axis, known)`;
- the space left free at the ends of pages.

### 3. Build 1 stores at least 5,000 objects a second of 3 KB, at 4,096-event batches

**Holds: 21,666 to 21,940 objects/s, 71.5 to 72.4 MB/s**, over four runs at load 3.8 to 4.0. Beside it, the disk's durable append rate was 500.3 MB/s (3.3 KB records, 4,096 per sync). Build 1 uses about 14% of it, counting the store's write only.

The inbox carries the same bytes again, and with the log on, the log carries them a third time.

### 4. An as-of read of any object at any build equals the generator's object, and a single-key read takes under 1 ms at p50

**Holds.**
- **Every object at every build:** 1,200,000 of 1,200,000 as-of reads, every object at every build, equal the generator's object at that build.
- **Speed:** a single-key as-of read takes 0.012 ms at p50 and 0.015 ms at p99, over 20,000 random reads (load 4.1).

### 5. An as-known read of a fix-pass object gives the answer from before the fix, as known before it, and the fixed one after

**Holds: 2,000 of 2,000 each way.** At build 4, as known one millisecond before an object's fix, each read gives the object before the fix; as known at the fix, the fixed one.

### 6. Neither as-of nor as-known reads touch the log or the hearings

**Holds.** Every read for 4 and 5 ran with `log.jsonl` and `hearings.db` moved out of the lane's directory. The reads are one indexed query on `versions` in the store file.

### 7. With versions, the Westwind fast lane stays within 10% of the two-lane toy's 32,615-33,636 facts/s, and the cast's digest stays a8184fc0102a6e76

**The digest holds:** the cast's outcomes digest is a8184fc0102a6e76, with 13 of 13 trails and 320 of 320 seed orders.

**The rate holds on the mean of four runs, but not on every run** (`version/results/fast.txt`). This is the extended orders, 753,806 facts, at 4,096-event batches, with 64 KB pages:

| run | facts/s | load |
|---|---|---|
| log off, 1 | 29,261 | 3.3 |
| log on, 1 | 34,305 | |
| log off, 2 | 29,287 | 3.8 |
| log on, 2 | 34,502 | |
| mean | 31,839, which is 2.4 to 5.3% under the two-lane range | |

- **Log-off runs:** they are 10.3% under the range's low end, just outside 10%. The log-on runs, which do more work, were faster in both pairs, so the spread is noise in the measurement as well as cost.
- **With 4 KB pages** (`version/results/fast-4k.txt`): 30,881, 30,626, 29,487 and 28,245; mean 29,810, 8.6 to 11.4% under.
- **Size:** versions grow Westwind's store to 1,221 MB with 64 KB pages, from 910 MB. Each shipped order now keeps the version from before its shipment.
- **Reads:** the fast lane still reads back 0 rows after writing them.

### 8. 10 aborts at random rounds, on both streams, give the same versions, emitted ids and outcomes as an unkilled run

**Holds.**
- **Westwind chain:** 10 of 10 children aborted, in order and shuffled. Each gives the same rows, the same set of emitted ids and the same outcomes as an unkilled run (`version/results/replays.txt`).
- **Build stream:** 10 of 10 children aborted at random rounds. The versions, every one in order, have the same digest as an unkilled run: a45299598bdb4f2d (`version/results/build-kills.txt`).
- **Bug found on the way:** the first Westwind kill runs differed. A restart chose its current row by the latest known-at, but a timer handled late carries an earlier known-at than the step before it. For a kind without an axis, the current row is now the last version written.

### 9. Four workers give the same set of emitted ids and the same rows as one worker, on the Westwind chain

**Partly.**
- **Emitted ids:** the set is the same with four workers as with one, `dc0204f060c0ffd3`. In the two-lane toy it differed.
- **Rows:** they are not all the same. 127 of 556 order rows differ.
  - **Where:** in the pick-and-pack flow's handed window, it differs which line's entry records the order's `order.packed`, and that event's `causation`. Whichever line arrives last sends the pack.
  - **Why the ids still agree:** an id no longer depends on which line came last.
  - **What would make the rows agree too:** a handed entry would have to stop carrying what it sent, or the pack would have to be sent by something other than the last line.

### 10. The log, carrying 3 KB bodies, costs the build stream's main path under 10% at 4,096-event batches

**Holds: +2.2%**, the whole stream, mean of two interleaved runs each: 41.9 and 41.7 s with the log off, 42.7 and 42.7 s with it on.

## What breaks the idea

- **An as-of or as-known read needs the log or the hearings.** No read did; every one ran with both moved away.
- **An unchanged object costs disk.** In the store it costs nothing: 0 versions for each of the 190,000 to 192,000 unchanged objects of each later build. The inbox and the log still append every arrival, changed or not: 3.85 GB each for the six builds.
- **Versions cost the Westwind fast lane more than 25%.** They cost 2.4 to 5.3% on the mean of four runs with 64 KB pages, and 8.6 to 11.4% with 4 KB.
- **Emitted ids still differ between one worker and four.** They do not. The rows still differ in which line's entry carries the shared pack.
