# Walks: predictions

Written and committed before any run of `src/bin/walks.rs`. The operator's questions: what does a neighbourhood walk cost, what does a rewalk cost, and when can a short walk stand in for one?

## Terms

- **Short walk:** what the relay does now for one batch, called directly on `crates/rannoch-relay/src/warm.rs` with no change to it: `fold` for each event (reads again what the event named), then `settle` (walks again over what is held, reading only what is new). An amendment or a rename makes `settle` forget everything and read it all again, so for those two kinds the short walk is a rewalk by construction.
- **Rewalk:** `Warm::seed` from nothing: every root, node and list read from the graph.
- **Measured:** time around `fold` + `settle`, and around `seed`, each on its own read handle, so each pays the page reload after the commit. Graph reads are the `reads` counter on the same call. Commit and fill are outside the timers. So are the queue, the wake, the log read and the publish.

## Neighbourhoods

From spice-7.9.a-v7, in one store holding all of it: a small quest closure; a cycle (the 25-node closure); one NPC's 216 conversations; the stabilizer hub capped at 500; the same hub exact; Hutta; the largest planet; every planet at once (a set of roots).

## Predictions

1. **Differences: none.** After every short walk the warm set equals a rewalk and an independent recompute through #175's reads. Any difference is a failure and goes first in the results.
2. **Rewalk reads** are about 1 per root, 1 per list, 1 per node: about 2 to 3 times the size. **Rewalk time** is about 20 to 30 microseconds per node (Hutta seeded in 20 to 29 ms at 902 nodes).
3. **Short walk, by kind:**

| Kind | Reads | Time |
|---|---|---|
| Unheld | 0 | under 20 microseconds at any size; no walk runs |
| Held node changed | 1 | about 0.3 ms plus about 0.5 microseconds per node |
| Link added | 1 for the link, plus 2 for what it newly reaches | the same, a little more |
| Link ended | 1 | the same as a node changed |
| Amendment of one property | the same as a rewalk | the same as a rewalk: it is one |
| Rename | the same as a rewalk | the same as a rewalk: it is one |

4. **The walk grows with size even when reads do not.** At every planet at once, a short walk of one read still takes tens of milliseconds, because `settle` walks the whole set.
5. **Crossovers, per event** (a walk after every event): by time, one rewalk is cheaper than k short walks from about k = 3 on the smallest neighbourhoods to about k = 30 to 50 at Hutta. At the largest, k stays near 30 to 50, since both sides grow with size.
6. **Crossovers, per commit** (the relay's own way: all the folds, then one walk): by time, about k = 1,000 or more at Hutta, and not below the size of the neighbourhood. By reads, near k = 2 to 3 times the size: k events cost k reads against a rewalk's 2 to 3 per node.
7. **Bursts of unheld events** never cross by reads: they cost no reads at any size.

## How I would be wrong

- The fold's 270 microseconds at one read is mostly SQLite reloading pages after another connection's commit. If that holds, the second read in a commit is far cheaper than the first, and the per-commit crossover lands higher than prediction 6.
- A rewalk's time may not scale evenly: one long list is one read, so a hub may cost less per node than a planet.
- Toggling the same link makes it read slower for its own history, so link-ended numbers on the small neighbourhoods may run high. They end and re-add a few real links over and over.
