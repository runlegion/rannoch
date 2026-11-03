# Walks: results

Predictions: WALKS-PREDICTIONS.md, committed at 23:26:55 on 2026-11-02 (b4e123c), before any run. Every number here comes from the full run that started at 23:28 and finished at 23:48: exit 0, 1,220 s, on spice-7.9.a-v7 in one store of 247,404 events, release build, graphs on the internal disk, load average about 2.5 with other agents running. The whole output, with every burst row, is `walks-output.txt`. Rerun: `./target/release/walks <spice-7.9.a-v7.sqlite> [a neighbourhood's name]`.

## Failures

**None. 0 differences over 1,368 comparisons.** After every short walk the warm set was compared with a rewalk and with a recompute through #175's reads, written apart from the relay: every node's version, each step's nodes in order, and what each step dropped.

What that does not cover: inside a burst with a walk after every event, the set was compared only after the last event, since the graph already holds the whole commit and a set part way through is meant to differ. Views were all "now": no axis or known-at view was walked here.

One bug in the toy, not the relay, was found by a smoke run on the smallest neighbourhood, after the predictions and before this run: the burst's undo minted each ending before the link it ended, so the links stayed. It was fixed in the toy before the full run. No number here comes from the smoke run.

## What it shows

1. **A short walk stood in for a rewalk every time, for truth.** Only the time varies.
2. **Reads are not what a short walk costs: the walk is.** One read at every planet (51,243 nodes) takes 30 ms. Past a few hundred nodes, time is about 0.45 to 0.6 microseconds per node while reads stay at 1. A count of graph reads cannot see this cost.
3. **A rewalk costs about what was predicted:** 2 to 2.7 reads per node, about 20 microseconds per node on the planets, 15 to 17 on the hub, 10 on the NPC, and 26 to 108 on the two smallest.
4. **An unheld event costs almost nothing at every size:** 0 reads, 3 to 9 microseconds, no walk.
5. **An amendment and a rename are already rewalks in today's relay**, with exactly the rewalk's reads. An amendment of one property took as long as a rewalk or longer (116 ms against 90 at the largest planet, 1.50 s against 1.10 s at every planet), since it also frees what it held.
6. **The first read after a commit is the expensive one; later reads in that commit are not.** The relay's way on a held node changed at Hutta: k = 1 took 304 microseconds, k = 10 took 456, k = 100 took 1,350. That is about 10 microseconds per further event. At every planet, k = 5,000 took 83 ms against 34 ms at k = 1.

## When one rewalk becomes cheaper

Two different comparisons, because the relay does not walk once per event:

- **A walk after every event:** by time, one rewalk is cheaper from k = 50 to 200 on most rows, about the ratio in the kind table (Hutta: 18.4 ms against 0.43 ms). The outliers are the smallest neighbourhood at 20 and the exact hub's node changes at 500. The grid is coarse: 20, 50, 100, 200, 500.
- **The relay's way, every fold and then one walk:** the crossing moves out to about the size of the neighbourhood or beyond, and on most rows this run never reached it. Each event adds about 10 to 20 microseconds against a rewalk's 20 per node.

The relay's way is what runs today, so the first comparison is what a design with a walk per event would pay, not what the relay pays.

Read the table with care:
- "none to N" means no crossing up to the largest k tested.
- **Link added grows the neighbourhood as k grows,** since a burst adds k nodes before the rewalk runs, so at k = 5,000 Hutta holds 5,902 nodes. Its crossing is measured against a growing rewalk. On the walks with a cap per step, the set stops at about 1,010.
- **Node changed stops at k = the nodes held,** as each event changes a different node. So "none to N" there means not reached by construction.
- At every planet, a walk per event was cut off after 30 s, at about 1,000 events; those rows say "over" in the output.

| Neighbourhood | Nodes | Kind | Walk per event, reads | Walk per event, time | Relay's way, reads | Relay's way, time |
|---|---|---|---|---|---|---|
| small closure | 7 | unheld | none to 5000 | 500 | none to 5000 | 500 |
| small closure | 7 | node changed | none to 5 | none to 5 | none to 5 | none to 5 |
| small closure | 7 | link added | 20 | 20 | 20 | 100 |
| cycle | 25 | unheld | none to 5000 | 1000 | none to 5000 | 1000 |
| cycle | 25 | node changed | none to 20 | none to 20 | none to 20 | none to 20 |
| cycle | 25 | link added | 100 | 50 | 100 | 100 |
| npc 216 | 217 | unheld | none to 5000 | 5000 | none to 5000 | 5000 |
| npc 216 | 217 | node changed | none to 200 | 100 | none to 200 | none to 200 |
| npc 216 | 217 | link added | 500 | 100 | 500 | 500 |
| hub capped | 513 | unheld | none to 5000 | none to 5000 | none to 5000 | none to 5000 |
| hub capped | 513 | node changed | none to 500 | 100 | none to 500 | none to 500 |
| hub capped | 513 | link added | 1000 | 100 | 1000 | 1000 |
| hub exact | 716 | unheld | none to 5000 | none to 5000 | none to 5000 | none to 5000 |
| hub exact | 716 | node changed | none to 500 | 500 | none to 500 | none to 500 |
| hub exact | 716 | link added | 2000 | 200 | 2000 | none to 5000 |
| hutta | 902 | unheld | none to 5000 | none to 5000 | none to 5000 | none to 5000 |
| hutta | 902 | node changed | none to 500 | 100 | none to 500 | none to 500 |
| hutta | 902 | link added | 5000 | 100 | 5000 | 5000 |
| largest planet | 4296 | unheld | none to 5000 | none to 5000 | none to 5000 | none to 5000 |
| largest planet | 4296 | node changed | none to 2000 | 100 | none to 2000 | none to 2000 |
| largest planet | 4296 | link added | none to 5000 | 100 | none to 5000 | none to 5000 |
| every planet | 51243 | unheld | none to 5000 | none to 5000 | none to 5000 | none to 5000 |
| every planet | 51243 | node changed | none to 5000 | 50 | none to 5000 | none to 5000 |
| every planet | 51243 | link added | none to 5000 | 50 | none to 5000 | none to 5000 |

## By event kind

Microseconds, from the start of `fold` to the end of `settle`, and around `seed`. Each on its own read handle, so each pays the page reload after the commit. Commit, fill, the queue, the wake, the log read and the publish are outside the timers. The rename rows ran last, in a store that by then held renames and every earlier write; 3 samples each. 5 to 30 samples each otherwise.

| Neighbourhood | Nodes | Kind | Short: reads | Short: p50 | Short: p95 | Rewalk: reads | Rewalk: p50 |
|---|---|---|---|---|---|---|---|
| small closure | 7 | unheld | 0 | 4 | 11 | 14 | 197 |
| small closure | 7 | held node changed | 1 | 152 | 192 | 14 | 754 |
| small closure | 7 | link added | 3 | 94 | 324 | 46 | 452 |
| small closure | 7 | link ended | 1 | 60 | 147 | 14 | 384 |
| small closure | 7 | amendment of one property | 14 | 365 | 382 | 14 | 311 |
| cycle | 25 | unheld | 0 | 3 | 9 | 50 | 652 |
| cycle | 25 | held node changed | 1 | 63 | 103 | 50 | 1078 |
| cycle | 25 | link added | 3 | 141 | 176 | 82 | 1770 |
| cycle | 25 | link ended | 1 | 114 | 254 | 50 | 1645 |
| cycle | 25 | amendment of one property | 50 | 776 | 820 | 50 | 697 |
| npc 216 | 217 | unheld | 0 | 3 | 11 | 218 | 2141 |
| npc 216 | 217 | held node changed | 1 | 72 | 105 | 218 | 2163 |
| npc 216 | 217 | link added | 2 | 115 | 304 | 234 | 2509 |
| npc 216 | 217 | link ended | 1 | 103 | 120 | 217 | 2994 |
| npc 216 | 217 | amendment of one property | 218 | 2543 | 2586 | 218 | 2480 |
| hub capped | 513 | unheld | 0 | 9 | 16 | 1217 | 8748 |
| hub capped | 513 | held node changed | 1 | 194 | 234 | 1217 | 8770 |
| hub capped | 513 | link added | 3 | 226 | 285 | 1232 | 9102 |
| hub capped | 513 | link ended | 1 | 192 | 224 | 1216 | 9032 |
| hub capped | 513 | amendment of one property | 1217 | 9411 | 9552 | 1217 | 9384 |
| hub exact | 716 | unheld | 0 | 4 | 9 | 1420 | 10729 |
| hub exact | 716 | held node changed | 1 | 228 | 280 | 1420 | 10681 |
| hub exact | 716 | link added | 3 | 274 | 306 | 1448 | 10721 |
| hub exact | 716 | link ended | 1 | 223 | 255 | 1418 | 11048 |
| hub exact | 716 | amendment of one property | 1420 | 11073 | 11290 | 1420 | 10981 |
| hutta | 902 | unheld | 0 | 8 | 17 | 2330 | 18329 |
| hutta | 902 | held node changed | 1 | 429 | 491 | 2330 | 18382 |
| hutta | 902 | link added | 3 | 469 | 504 | 2354 | 18947 |
| hutta | 902 | link ended | 1 | 375 | 396 | 2328 | 19004 |
| hutta | 902 | amendment of one property | 2330 | 19204 | 19463 | 2330 | 19098 |
| largest planet | 4296 | unheld | 0 | 6 | 9 | 11621 | 90243 |
| largest planet | 4296 | held node changed | 1 | 2233 | 3218 | 11621 | 88273 |
| largest planet | 4296 | link added | 3 | 2319 | 2499 | 11627 | 90682 |
| largest planet | 4296 | link ended | 1 | 2020 | 2101 | 11619 | 89424 |
| largest planet | 4296 | amendment of one property | 11621 | 116403 | 119138 | 11621 | 89628 |
| every planet | 51243 | unheld | 0 | 7 | 13 | 135349 | 1179002 |
| every planet | 51243 | held node changed | 1 | 30188 | 30991 | 135349 | 1054009 |
| every planet | 51243 | link added | 3 | 30482 | 31184 | 135355 | 1014658 |
| every planet | 51243 | link ended | 1 | 28831 | 29167 | 135347 | 1016668 |
| every planet | 51243 | amendment of one property | 135349 | 1502908 | 1635067 | 135349 | 1097841 |
| small closure | 7 | rename | 14 | 23069 | 23234 | 14 | 29143 |
| cycle | 25 | rename | 50 | 21935 | 22256 | 50 | 30506 |
| npc 216 | 217 | rename | 216 | 27090 | 28055 | 216 | 35182 |
| hub capped | 513 | rename | 1217 | 96204 | 98901 | 1217 | 110210 |
| hub exact | 716 | rename | 1420 | 97012 | 97762 | 1420 | 113504 |
| hutta | 902 | rename | 2330 | 76298 | 164189 | 2330 | 80378 |
| largest planet | 4296 | rename | 11621 | 193460 | 194958 | 11621 | 203646 |
| every planet | 51243 | rename | 135349 | 1753582 | 1903464 | 135349 | 1855228 |

**Not explained:** the rename rows are slower than the same reads were before any rename, on the short walk and the rewalk alike. The smallest neighbourhood took 23 ms for 14 reads against about 0.3 ms before; the capped hub 96 ms for 1,217 reads against about 9; Hutta 76 ms for 2,330 against about 19. This run did not look at why, or separate the rename from the growth of the store by then.

## Against #151's numbers, reported only

Direct fold and walk, with no wake or log read, under #151's "applying an event under 1 ms":
- Under it at every size to Hutta (902 nodes): at Hutta, p50 375 to 469 microseconds, p95 up to 504.
- Over it at the largest planet (4,296 nodes): p50 about 2.2 ms.
- Over it at every planet (51,243 nodes): p50 about 30 ms.

The earlier toy's 0.43 to 1.91 ms at Hutta also carries the wake, the log read and the publish, so at Hutta the fold and walk fit and part of that number sits outside these timers. This run did not measure how much of it.

## The predictions

| # | Prediction | Result |
|---|---|---|
| 1 | No differences | Held: 0 over 1,368 |
| 2 | Rewalk 2 to 3 reads per node, 20 to 30 microseconds per node | Reads held. Time at the low end or under: 10 to 21 microseconds, except the two smallest (26 to 108) |
| 3 | Unheld 0 reads, under 20 microseconds | Held |
| 3 | Node changed and link ended 1 read, about 0.3 ms plus 0.5 microseconds per node | Reads held. The constant was too high: 60 to 230 microseconds up to 716 nodes |
| 3 | Link added 3 reads | Held, 2 on the one-step NPC |
| 3 | Amendment and rename the same as a rewalk | Held in reads, and in time for a rename. An amendment ran up to about 40% over |
| 4 | Tens of milliseconds at every planet on one read | Held: 30 ms |
| 5 | Per event: k = 3 small, k = 30 to 50 at Hutta and at the largest | Wrong: 20 to 100 small, 100 at Hutta and at the largest, 50 at every planet |
| 6 | Relay's way: k of 1,000 or more at Hutta, not below the size | Held. By reads: near 1 to 5 times the size, wider than the 2 to 3 predicted |
| 7 | Unheld bursts never cross by reads | Held. They do cross by time on small neighbourhoods, which was not predicted: about 1 microsecond per event against a small rewalk, at k = 500 to 5,000 |
| "wrong if" | A later read in a commit is far cheaper than the first | That is what happened |
| "wrong if" | A hub costs less per node than a planet | Yes: 15 to 17 microseconds per node against 20 |
