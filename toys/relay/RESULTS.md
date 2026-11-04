# Results

The latest runs: 3 on 2026-11-04 from 03:31 UTC, on `feat/151-relay` at bf016b8 (the toy at 3e0526d), release build, default 20 ms heartbeat, graphs on the internal disk, load average 2.5 to 2.8 with other agents on the machine. `./target/release/relay-toy ~/swtor/data/spice-7.9.a-v7.sqlite ~/swtor/data/spice-7.8.1.c-v26.sqlite`. Their output is `results-run1.txt` to `results-run3.txt`. **Every judged line passes in all 3, 0 failed and exit 0.** Two measures are reported and not judged, by the operator's call on #193.

## The round trip and the two reported measures

| Measure | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Round trip, median under 40 ms (judged) | 28.2 ms | 28.2 ms | 28.2 ms |
| Its p95, stated | 48.3 ms | 49.4 ms | 49.3 ms |
| Its p99, stated (over 50 trips, so the slowest; the first, which seeds, in run 1) | 93.2 ms | 62.2 ms | 65.1 ms |
| Applying an event: fold plus walk, median (reported) | 0.53 ms | 0.49 ms | 0.54 ms |
| Its p99 (reported) | 1.97 ms | 1.54 ms | 2.41 ms |
| Kessel's in-process read of 216 links, one reading a run (reported) | 1.13 ms | 0.91 ms | 0.99 ms |

**Kessel's "under 2 ms in-process" did not hold.** All 3 of these runs read under 2 ms, but #193's full runs on the same data met it in 6 of 9 before the simplify and 8 of 17 after it, so it is not a bound this read keeps. It is one reading a run, and the cause of the swing is not known.

Kessel's "round trip under 40 ms", as written, does not hold at its tail; the operator judges it at the median because the heartbeat alone waits up to 20 ms.

Fold plus walk is around the relay's own `fold` and `settle`, compiled in unchanged, on one read handle, one link per commit. These 3 runs are at the low end of what #193 recorded after the simplify (0.55 to 2.38 ms at the median over 16 runs), and 0.49 ms is below it. Three quiet runs do not narrow that range; the swing between runs is still not explained.

What was expected before these runs:
- The simplify's cost: "rise by 10 to 30%, to about 0.55 to 0.65 ms". It did not show. Two medians sit at the bottom of that range and one under it, and #193's alternating A/B found the build not slower.
- #193's expectation here: "the in-process read over 2 ms on about half of runs" and fold plus walk "from about 0.55 to 2.4 ms, split by run". Neither happened in these 3: no read over 2 ms, and no split. "Every other line passes with 0 differences" held.
- Kessel's "at the old known-at", read from the ack's `known` and from the newest known-at a report holds: holds in all 3.

The other judged lines in these runs: seed 41 to 44 ms through the inbox, up from 20 to 29 ms before the inbox was on the path; read() p50 41 ns, p99 42 ns; by logname p50 83 to 84 ns, p99 125 to 167 ns; line to overlay p50 18 to 20 ms, p99 26 to 29 ms, max 41 to 54 ms; rebuild with the file gone 20 to 21 ms; the report's reread 1.34 to 1.39 s; the hub's memory up 1.2 to 1.7 MB, file 423 KB.

`src/bin/walks` was moved to the simplified API and builds, and was not rerun, so WALKS-RESULTS.md is the build it names. Its comparison with "under 1 ms" predates #193.

## How this page is dated

The numbers from here to the end of this page come from the earlier runs of 2026-11-02 and 2026-11-03, before the simplify and before #193. The predictions were committed before any run, at 22:48 on 2026-11-02 (commit ca68d86, the same author date as 08766e5). A later rebase onto the feature branch put them after 969e62b, a fix the first run prompted, so this branch's order alone does not show it: read the author date. Later expectations were each committed before their rerun: 42b6e35, f382a54 and 3e0526d.

## The importer's

| Prediction | Result |
|---|---|
| T1 count 216, as a query | 216 |
| T1 known-at present, not later than the load | yes |
| T1 round trip under 40 ms, as the kessel prediction wrote it | Fails at the tail, passes at the median, which is the measure now (above). In these earlier runs p50 was 28 ms, p95 48 to 51 ms, p99 81 to 86 ms over 50 trips in each of 4 runs. The first trip, which seeds, took 46 to 51 ms. |
| T1 under 2 ms in process | 1 to 1.8 ms for the 216 links in these runs; did not hold across #193's runs (above), and is reported, not judged |
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
| Applying an event, measured the old way, through the wake | p50 by run, over 300 one-link commits each: 0.43, 0.59, 0.90, 1.14, 1.20, 1.23, 1.31, 1.53, 1.60, 1.86, 1.91 ms. p99 2 to 7.5 ms. The measure is now the fold plus the walk, reported and not judged (above). |

That old measure ran from the commit's subscriber call to the relay's change callback: the wake, reading the event from the log, reading the link again, the walk over 902 nodes, the deltas and the publish. PREDICTIONS.md said to read()'s cursor; a reader spinning on read() competes with the publish, so this run hears the callback instead. One link is linked or ended per commit, each its own target: one link toggled 300 times reads slower for its own history. What is left after the callback (indexing the new set, freeing the old) is not in the number.

The result splits by run, not by sample: a run is near 0.5 ms or near 1.5 ms. That looks like the relay's thread landing on a slower core, which this toy did not prove. On one run's timing, the walk and deltas took about 580 microseconds and reading the link about 270, of which most is SQLite reloading pages after another connection's commit. p99 carries a file save, at most one a second.

## Memory a capped step costs, at the largest hub

`itm.mat.craft.endgame.stabilizer.tier2`, 703 found, 500 kept, 513 in the warm set (a second step, `crafts`, reached 12): resident size up about 1.3 to 1.7 MB in the seed, file 393,160 to 423,342 bytes.

## What the toy changed from what was predicted

- A cycle: `quest_chain` has 5 two-node cycles (10 rows), only across two `link_type`s, so `next_in_arc` holds all 3,304 rows (PREDICTIONS.md).
- T3's real diff has no link that left. One v26 link on the neighbourhood is ended by hand in the 7.9 commit, and the SQL diff drops the same one (PREDICTIONS.md).
- Paging is not in #151, and was not checked.
- The kessel prediction's "count" is the acknowledgement's step count on a one-step shape. `Graph::answer` refuses a shape query, as #151 says.
