# Results

Release builds; stores on the external SSD; base PR #179 (34be65e). Raw output in `results/`.

## A finding first: fill was quadratic once a store held aliases

Before any stats work, a clean fill of 300,000 events ran at **1,011 events/s** (296 s), and the rate
fell with size: 31,971/s at 10,000 events, 13,898/s at 30,000, 3,470/s at 100,000. The 300,000-event
store held 5,000 aliases (repeated links in the generator). Every per-fact query fill runs (`edges`,
`reseen`, `newest`) reads occurrences through `known_occurrences`, whose alias side the planner ran as
`SEARCH a USING COVERING INDEX _aliases_at (line=?)`: a scan of every alias in the line, once per
fact. Forcing the join order (`FROM _hearings h CROSS JOIN _aliases a`) gives
`SEARCH a USING COVERING INDEX _aliases_at (line=? AND identity=?)`, and fill became linear:
54,931/s at 10,000, 54,900/s at 30,000, 46,829/s at 100,000, about 40,000/s at 300,000. The same scan
puts a ~100 ms floor under every `links` and `describe` read at 10% aliases (`results/reads-10-nofix.txt`:
links out of a 1-link node 103 ms, describe 205 ms). The one-word fix is on main's code path today
(`known_occurrences`, from #158/#171), not only in this toy. All numbers below include it.

## Q1: stats kept by fill

| Prediction | Result | |
|---|---|---|
| P1: fill within 1% with stats | First design (temporary triggers per row): **-25%**. Final design (in-memory tally in Rust, new rows detected by `INSERT ... ON CONFLICT DO NOTHING` changing one row, one `_stats` write per touched entity per batch): 4 runs each, off 39,684-41,218/s (median 40,437), on 37,580-41,316/s (median 40,055): **about -1%**, within run noise. About 1.3% of it is the one lookup per link event that asks whether the link ever ended. | held only after redesign |
| P2: "now" counts from stats under 0.1 ms | stats read **0.001 ms**; `counts()` now **151 ms** at 742,234 events (149 ms at 1.1 M) | held |
| P3: stats equal `counts()` after fill, amendments and renames, rebuild | First version **failed**: relation tables keep ended links, so a row count is not "holding". Adding a tally of each ending that stops a link holding (and each re-hearing that reopens one) fixed it: AGREE after fill, after endings + reopen + open links closing + amendment + rename, and after a rebuild from the log | held after fix |

## Q2: a node's link reads, 742,234-event store (330,012 nodes, 411,111 links)

Whole reads grow with rows returned, about 1.4 us a row at 0% aliases:

| fan | links out now | links in now | describe now |
|---|---|---|---|
| 1 | 0.008 ms | 0.009 ms | 0.012 ms |
| 10 | 0.028 | 0.021 | 0.021 |
| 100 | 0.184 | 0.145 | 0.120 |
| 1,000 | 1.508 | 1.407 | 1.147 |
| 10,000 | 14.5 | 14.9 | 12.1 |
| 120,000 | 245 | 247 | 215 |

At a known-at the times match within a few percent. With aliases (each re-send is one more
occurrence row): 1,000 links out 2.2 ms at 10%, 3.1 ms at 50%; 10,000 links out 38.6 ms at 50%.

A page (toy `links_page`: at most 100 links whose other end's key is after a cursor, the range found
by an index seek before any version is picked) is flat:

| fan | page out, 0% | page in, 0% | page out, 50% aliases | page in, 50% |
|---|---|---|---|---|
| 1 | 0.126 ms | 0.116 | 0.127 | 0.116 |
| 100 | 0.268 | 0.263 | 0.360 | 0.354 |
| 1,000 | 0.270 | 0.264 | 0.361 | 0.354 |
| 10,000 | 0.273 | 0.265 | 0.369 | 0.356 |
| 120,000 | 0.276 | 0.264 | 0.370 | 0.357 |

The same at a known-at, after a cursor. A first page query using `IN (SELECT DISTINCT ... LIMIT)`
was flat for links in but 17.8 ms at the 120,000 hub out; a key range (`> cursor AND <= max of the
next 100 keys`) fixed it.

| Prediction | Result | |
|---|---|---|
| P4: links out sub-ms to fan 100; ~2-5 ms at 1,000; ~30 ms at 10,000; ~300 ms at hub | 0.18 / 1.5 / 14.5 / 245 ms | held (faster than predicted) |
| P5: known-at within 2x of now | within a few percent | held |
| P6: links in slower than out | no: links in matched links out | failed |
| P7: describe over 1 ms from fan 1,000 | 1.15 ms at 1,000; 215 ms at the hub | held |
| P8: per-node link counts make describe sub-ms everywhere | not built in this toy | not tested |
| P9: no full read of a 10,000-link node is sub-ms; a page is, at every fan-out | full 14.5 ms; page 0.26-0.37 ms at every fan-out, hub and 50% aliases included | held |

## What each fix costs fill

- Alias join order (`CROSS JOIN`): nothing; it makes fill linear (1,011/s to ~40,000/s at 300,000 events with 1.7% aliases).
- Pages: nothing; reads only, on the existing `_hearings_at` and `_hearings_to` indexes.
- Stats as built here: about 1%, within noise; drop the per-link "ever ended" lookup (track ending state another way) to recover ~1.3%.
