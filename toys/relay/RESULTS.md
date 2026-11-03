# Results

The latest runs: 3 on 2026-11-03 at 12:05, on `feat/151-relay` at 6540415, release build, default 20 ms heartbeat, graphs on the internal disk, with other agents using the machine (load average 3.5 to 6.5). `./target/release/relay-toy ~/swtor/data/spice-7.9.a-v7.sqlite ~/swtor/data/spice-7.8.1.c-v26.sqlite`. Their output is `results-run1.txt` to `results-run3.txt`. **Every prediction passes in all 3, with the two measures the operator changed on 2026-11-03.**

## The two changed measures

| Measure | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Round trip, median under 40 ms | 24 ms | 23 ms | 25 ms |
| Its p95, stated | 48 ms | 35 ms | 49 ms |
| Its p99, stated (over 50 trips, so the slowest; the first, which seeds, in runs 2 and 3) | 71 ms | 83 ms | 116 ms |
| Applying an event: fold plus walk, median under 1 ms | 0.98 ms | 0.50 ms | 0.51 ms |
| Its p99 | 2.5 ms | 1.2 ms | 1.2 ms |

Kessel's prediction as written, "round trip under 40 ms", does not hold at its tail; the operator judges it at the median because the heartbeat alone waits up to 20 ms. Run 1's fold plus walk is close to its limit, at the highest load of the three. Timing is around the relay's own `fold` and `settle`, compiled in unchanged, on one read handle, one link per commit.

The numbers from here to the end of this page come from the earlier runs of 2026-11-02 and 2026-11-03, before those two measures changed. The 3 latest runs repeat every other pass, with the same counts and with times in the same range (seed 21 to 25 ms, read() 41 ns, by logname 83 ns, line to overlay p99 25 to 26 ms, max 36 to 69 ms; the hub's memory up 1.4 to 1.7 MB, file 423 KB). The predictions were committed before any run, at 22:48 on 2026-11-02 (commit ca68d86, the same author date as 08766e5). A later rebase onto the feature branch put them after 969e62b, a fix the first run prompted, so this branch's order alone does not show it: read the author date. The two changed measures' expectations were committed before the 3 latest runs (42b6e35).

## The importer's

| Prediction | Result |
|---|---|
| T1 count 216, as a query | 216 |
| T1 known-at present, not later than the load | yes |
| T1 round trip under 40 ms, as the kessel prediction wrote it | Fails at the tail, passes at the median, which is the measure now (above). In these earlier runs p50 was 28 ms, p95 48 to 51 ms, p99 81 to 86 ms over 50 trips in each of 4 runs. The first trip, which seeds, took 46 to 51 ms. |
| T1 under 2 ms in process | 1 to 1.8 ms for the 216 links |
| T1 report still 216 at the old known-at; fresh count 266 | yes; reread took 1.4 to 2.0 s, replaying 54,000 events |
| T1 warm set equals the SQL closure, each node once, not cut | 25 of 25 |
| T1 no change events while nothing commits | none in 500 ms idle |
| T2 hop 1 cut, 703 found, 203 dropped, the 500 lowest keys | yes |
| T2 same shape shared, a second relay on the same definition identical | yes, same 513 nodes in the same order |
| T2 boundary churn: one event, both moves, dropped 204, step stays 500 | yes |
| T3 change events equal the SQL diff | 34 in, 1 out, in 1 event, as predicted |
| T3 7.9's valid time on each | yes |
| T3 ended link out of the latest warm set; 7.8.1 relay emits nothing | yes, 0 events |

The round trip's cost is the wait for a heartbeat (up to 20 ms) and two commits, each with up to three flushes (the batch, fill, the workers' places). Without committing the answer at once (`Queue::commit`), p50 was 62 ms. On an internal disk it fits two heartbeats at the middle and not at the tail; on the USB disk it was worse (first run before that change: p50 65 ms, p99 151 ms).

## The recorder's, with #151's stand-ins

The planet relay roots on a `planet` node keyed `hutta`, taken from the fqns' own location segment, not on `planet:<area>`. Line to overlay runs from `accept_from` to the change-event callback with 4 other workers draining, so it measures the relay and not warden's parser or overlay; warden's real path stays warden's to measure when it adopts.

| Prediction | Result |
|---|---|
| About 1,000 nodes | 902: 1 root, 860 at the first step (dropping 608 Spawns by filter), 41 at the second |
| T1 Hutta relay seeds under 1 s | 20 to 29 ms, on a graph of 177,676 events |
| T1 equals a cold recompute (#175's reads) and the SQL | yes |
| read() under 0.05 ms | p50 41 ns, p99 42 ns |
| T2 read() by logname under 0.05 ms | p50 83 ns, p99 166 ns |
| T3 line to overlay under 60 ms p99 with 4 workers | p50 18 to 19 ms, p99 25 to 29 ms in the last 6 runs, max 34 to 79 ms. One earlier run, on the USB disk, before the walk was trimmed: p99 77 ms. |
| Rebuild with its file gone equals a cold recompute | yes, reopened in 21 ms |

## #151's numbers

| Number | Result |
|---|---|
| Seed under 1 s, about 1,000 nodes | 20 to 29 ms |
| read() under 0.05 ms | 41 ns |
| Applying an event, measured the old way, through the wake | p50 by run, over 300 one-link commits each: 0.43, 0.59, 0.90, 1.14, 1.20, 1.23, 1.31, 1.53, 1.60, 1.86, 1.91 ms. p99 2 to 7.5 ms. The measure is now the fold plus the walk (above). |

That old measure ran from the commit's subscriber call to the relay's change callback: the wake, reading the event from the log, reading the link again, the walk over 902 nodes, the deltas and the publish. PREDICTIONS.md said to read()'s cursor; a reader spinning on read() competes with the publish, so this run hears the callback instead. One link is linked or ended per commit, each its own target: one link toggled 300 times reads slower for its own history. What is left after the callback (indexing the new set, freeing the old) is not in the number.

The result splits by run, not by sample: a run is near 0.5 ms or near 1.5 ms. That looks like the relay's thread landing on a slower core, which this toy did not prove. On one run's timing, the walk and deltas took about 580 microseconds and reading the link about 270, of which most is SQLite reloading pages after another connection's commit. p99 carries a file save, at most one a second.

## Memory a capped step costs, at the largest hub

`itm.mat.craft.endgame.stabilizer.tier2`, 703 found, 500 kept, 513 in the warm set (a second step, `crafts`, reached 12): resident size up about 1.3 to 1.7 MB in the seed, file 393,160 to 423,342 bytes.

## What the toy changed from what was predicted

- A cycle: `quest_chain` has 5 two-node cycles (10 rows), only across two `link_type`s, so `next_in_arc` holds all 3,304 rows (PREDICTIONS.md).
- T3's real diff has no link that left. One v26 link on the neighbourhood is ended by hand in the 7.9 commit, and the SQL diff drops the same one (PREDICTIONS.md).
- Paging is not in #151, and was not checked.
- The kessel prediction's "count" is the acknowledgement's step count on a one-step shape. `Graph::answer` refuses a shape query, as #151 says.
