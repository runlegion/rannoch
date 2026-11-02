# Results (main b839e71)

**The graph comes back.** Across every read checked, the only difference from truth is one question:
a link found wrong still reads as holding.

| Proof | Compared | Differences |
|---|---|---|
| P1 small graph, hand-written edges | cycle, self-loop, hub, isolated node, dangling link, ended, reopened, rename, axis builds | 4, all one cause: the corrected link m>n |
| P2 real repository, truth from git | 3,435 commits, 6,878 files, 4,121 parent and 29,837 touched links, both directions, counts, describe, indexed filter; now, at the median commit time, and in a second arrival order | 0 |
| P3 seeded random histories vs a plain model | 26,880 read comparisons, axis and no-axis, 3 arrival orders, chunked fill | 0 |

## The finding: a link found wrong is still in the graph

m>n, then `rannoch.corrected.follows` at 20: `links`, `counts` and the read at known-at 25 all still
return m>n. Worse, u>v heard, ended at 14 and corrected at 20 comes back: from 20 on it reads as holding
although it ended and was found wrong. PR #179 recorded this as a deliberate choice ("a corrected link
reads as holding", "the correction masks its endings"), and docs/architecture.md says a correction "is
not an ending". So this is a known design choice measured here, not a new bug, and the prediction that
it would not hold failed. Not changed in this toy.

The u>v case's prediction was written before it ran but committed with the results, because its commit
failed on the ignored `toys` path and was not noticed.

## Predictions

| | Held? |
|---|---|
| P1 every shape but the correction | held |
| P1 corrected link does not hold | **failed**, see above |
| P2 0 differences | held |
| P2 about 7,000-25,000 reads under 10 s | held: about 13,749 reads plus describes in 0.43 s |
| P3 no re-sends, 0 differences | held |
| P3 re-sends, 50/50 | answered: a re-send after an ending reopens the link, as the model says (212 cases). A re-send is an alias in both kinds of relation: 1 hearing with no axis; 2 with an axis, which are builds 1 and 3 (the re-send at build 1 added none) |

P3's first version was too weak: endings rarely hit a real link. It was changed before being relied
on, to 4 keys with endings aimed at sent links, and it counts its cases: per 13,440 comparisons, about
2,500-2,800 links ended in a view, 1,450-1,850 reopened, 820-830 live and ended at the same point.

## Not covered

- No single read returns the graph: it takes `nodes` per class and `links` per node. That is fine at
  this size; the largest fan-out here is far under the 120,000-link hub in the earlier index toy.
- `answer` (reads as events), amendments of properties, and renames beyond P1's one case.
- Walks and relays, on purpose.
