# Graph versus relational toy: results

Run 2026-11-06 in `toys/workers` against [PREDICTIONS.md](PREDICTIONS.md) (legion f35d9529). The code is `src/compare.rs`, run as `workers-toy compare`, `compare-load` and `compare-ask`. The lead stopped the comparison before the graph load and the extended-log scenarios ran, so several predictions are not measured.

## How it runs

- **Graph:** the worker toy's store translation, each foreign key an edge.
- **Nodes only:** the same rows into rannoch as nodes that keep their foreign keys as text properties, with no relations declared.
- **Relational:** SQLite tables from `westwind/schema.sql` plus an index on each of the 8 foreign-key columns. It uses the same `journal_mode=WAL`, `synchronous=FULL` and `fullfsync=ON` as rannoch's store, and the same 4,096-event batches, each one transaction.
- **Load timing:** includes each version's own translation of the events.
- **Reads:** rannoch's API reads one node or one link at a time and has no traversal. Every rannoch scenario is therefore SQL over the store's own tables: `class:<name>` with a column per property, and `relation:<name>` with key `(src, dst)` and an index on `(dst, src)`.
- **Scenario inputs:**
  - C: the order with the most lines.
  - D: customer WINTF.
  - E: employee SALLO's chain up, and everyone under DAVOS.
  - F: Qohor Smiths' Guild.

## Measured

**Seed, 1,479 rows** (`out/compare-seed.txt`; nothing else building, load 5.0):

| scenario | graph | nodes-only | relational | rows | agree | matches SQL over seed.sql |
|---|---|---|---|---|---|---|
| A stock.low | 0.557 ms | 0.648 ms | 0.682 ms | 19 | yes | yes; also the worker toy's 19 |
| B unshipped | 0.810 ms | 0.794 ms | 0.769 ms | 37 | yes | yes; also the worker toy's 37 |
| C one hop | 0.004 ms | 0.059 ms | 0.003 ms | 4 | yes | yes |
| D two hops | 0.011 ms | 0.124 ms | 0.010 ms | 17 | yes | yes |
| E chain up | 0.006 ms | 0.004 ms | 0.004 ms | 2 | yes | yes |
| E under manager | 0.008 ms | 0.008 ms | 0.007 ms | 8 | yes | yes |
| F fan-in | 0.029 ms | 0.115 ms | 0.031 ms | 31 | yes | yes |

On the seed, the files take 4.4 MB (graph), 2.4 MB (nodes-only) and 0.5 MB (relational).

**Extended log, 1,754,602 rows**, each load in a process of its own with nothing else building (`out/quiet-pass.txt`, `out/v060/load.txt`):

| version | rannoch | load | rows/s | on disk | load average (start / end) |
|---|---|---|---|---|---|
| relational | none | 22.59 s | **77,688** | 470.9 MB | 4.72 / 4.86 |
| nodes-only | main 7aa6841 (#217, #218) | 234.17 s | **7,493** | 2,634.7 MB | 5.11 / 4.28 |
| nodes-only | v0.6.0 ec47f25 | 222.72 s | **7,878** | 2,764.1 MB | 5.00 / 3.89 |
| graph | either | not measured | | | |

An earlier, interrupted pass had relational at 82,198 rows/s and nodes-only on fabf9fd at 4,752 rows/s (`out/compare-extended.txt`). v0.6.0 needed two changes for the toy to compile against it: `Class.indexed`, left empty, and a match arm for `Declaration::Shape`.

**Where the 2.6 GB goes**, by dbstat on main's nodes-only file. That is 1,502 bytes per input row, against relational's 268:

| table or index | MB | what it holds |
|---|---|---|
| `_events` | 995 | the log: one body per input row, 474 B on average |
| `_hearings` | 825 | one hearing per input row, carrying a 259 B `properties` JSON copy plus identity, entity, key, event and source |
| `_hearings_at` | 238 | index on (entity, line, key, to_key, axis, known, event) |
| `class:order_line` | 164 | the lines' table |
| `class:order` | 106 | the orders' table |
| `_events_id` | 91 | unique index on the log's ids |
| `sqlite_autoindex__hearings_1` | 91 | unique (line, identity) |
| `class:order_line` key index | 50 | |
| `_seen` | 30 | first and last heard, per node |
| `_hearings_seq` | 24 | index on the log position |
| `class:order` key index | 21 | |

Each row's content is stored three times: the log body, the hearing's properties and the class table's columns. Five indexes sit on top of those copies. The class tables and their key indexes, which are the data relational also stores, take 341 MB of the 2,635 MB.

## Predictions

1. **All three versions give the same answers for A to F, on the seed and on the extended log.** Holds on the seed: every scenario agrees across all three versions and matches SQL over `seed.sql`. Not measured on the extended log.
2. **Relational loads at 150,000 rows a second or more.** Refuted: 77,688 rows/s, and 82,198 in the earlier pass. This is with `synchronous=FULL` and `fullfsync=ON` on every batch commit, which is the store's durability.
3. **Nodes-only loads at least 2 times faster than graph.** Not measured: the graph load did not run. Nodes-only by itself is 10.4 times slower than relational on main and 9.9 times on v0.6.0, with no edges at all. Most of rannoch's cost on this data is therefore in how the store records any node, not in the edges.
4. **Graph loads within 10 times of relational.** Not measured. Nodes-only already sits at the 10-times line (10.4 on main, 9.9 on v0.6.0), and graph adds about 1.8 link events per row on top. The worker toy measured graph through commit and fill at 1,715 to 2,184 Westwind rows/s on fabf9fd, 36 to 45 times slower than relational. That ran in a different harness on a loaded machine, so it is context, not this prediction's measurement.
5. **C and D under 1 ms on relational and under 10 ms on graph, on the extended log.** Not measured on the extended log. On the seed, C and D take about 0.01 ms on both relational and graph.
6. **E and F answer faster on graph than on nodes-only.** Not measured on the extended log. On the seed, F is faster on graph (0.029 ms against 0.115 ms). E is a tie (0.006 against 0.004 ms, and 0.008 against 0.008 ms), because the seed has only 9 employees.
7. **Each version's size is recorded, and graph is no more than 5 times relational.** Recorded for relational (470.9 MB) and nodes-only (2,634.7 MB on main, 2,764.1 MB on v0.6.0), which is 5.6 to 5.9 times relational with no edges. Graph is not measured, but it can only be larger than nodes-only, since it adds link hearings and log bodies. On the seed it is 8.2 times relational (4.4 MB against 0.5 MB).

## What breaks the idea

- **The versions disagree on any answer.** They did not disagree on the seed. The extended log was not measured.
- **Graph loads more than 10 times slower than relational.** Not measured directly. Nodes-only, with no edges, is already 9.9 to 10.4 times slower.
- **Graph reads lose to nodes-only on E and F.** Not measured on the extended log. On the seed, graph wins F and ties E.

## Not done

- The graph loads, on fabf9fd and on 7aa6841.
- Every extended-log scenario.
- A sampled profile of the fill.
- Statement counts per row. A counting copy of the store, built with SQLite's trace hook, is prepared in the scratchpad but was never built or run.
