# Predictions (written before any run)

Base: PR #179's reads (feat/175-reads at 34be65e). Release builds, stores on the external SSD.

## Q1: class and relation stats kept by fill, one write per touched class or relation per batch

- P1. Fill throughput with stats stays within 1% of the same build without them, on 300,000 clean events. Predicted cost 0.3%. Confidence 0.7.
- P2. "Now" counts read from stats take under 0.1 ms, against tens of ms for `counts()` (65 ms at 160,000 events in the PR). Confidence 0.9.
- P3. Stats equal `counts()` (view now) after a clean fill, after amendments and renames, and after a rebuild from the log. Confidence 0.6: endings, reopened links, open links and renames are where a delta count goes wrong.

## Q2: a node's link reads under 1 ms at game-build size, hubs included

Store: about 330,000 nodes and 400,000 links, with authors of fan-out 1, 10, 100, 1,000, 10,000 and one hub of 120,000 links out, and books with 10,000 and 120,000 links in.

- P4. `links` out, now, is under 1 ms for fan-out up to 100, about 2 to 5 ms at 1,000, about 30 ms at 10,000 and about 300 ms at the hub: time grows with rows returned. Confidence 0.6.
- P5. `links` at a known-at behaves like P4, within 2x. Confidence 0.6.
- P6. `links` in is slower than out at the same fan-in, because endings are found through a subquery on the target. Confidence 0.5.
- P7. `describe` grows with fan-out like `links` (it counts the rows): over 1 ms from fan-out 1,000. Confidence 0.7.
- P8. Per-node link counts kept per batch make `describe` under 1 ms at every fan-out, hub included. Confidence 0.6.
- P9. No read that returns every link of a 10,000-link node can be under 1 ms; a page (a cap on rows plus a key cursor) makes `links` under 1 ms at every fan-out. Confidence 0.6.
