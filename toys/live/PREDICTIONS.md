# Predictions, 2026-10-30 (written before any run below)

Code under test: rannoch main 3f48131 (mark #152, copy #155, fill retries #156, amend #147). Machine: Puck, M4, release build. Last recorded run: README results (produce 300,000 clean in 6.6 s, 45,700 events/s).

## Run 1: baseline on today's main

- P1.1 Produce 300,000 clean: invariants hold (0 missing, 0 doubled, 0 unfilled, position 299999). Throughput lower than 45,700 events/s because fill now runs each event in its own savepoint through the inbox: expect 25,000 to 45,000 events/s. Confidence 0.7 on the range, 0.95 on the invariants.
- P1.2 Produce 300,000 through repeated kill -9: 0 missing, 0 doubled, 0 unfilled, position 299999. Confidence 0.9.
- P1.3 Worker, about 3,000 events through repeated kill -9: every event done, none in progress. Confidence 0.9.

## Run 2: valid amendments under kill -9

A seed of N = 2,000 books, each with an author and a link (every tenth link, i % 10 == 3, ended), plus time-keyed entries, an integer-keyed class and app events. Then a script of amendments and later events fed through the queue with positions, killed at random until done. The oracle is computed by the script generator from its own plan, not from rannoch.

- P2.1 Every valid amendment applies exactly once: each book's title now equals the oracle (patched, removed by `null`, the later of a stacked pair, the larger event id of a same-point pair). 0 mismatches. Confidence 0.8.
- P2.2 Renamed books (i % 4 == 3) answer under the new key; the old key is absent now; their link answers under the new key, ended exactly when the oracle says (ended in the seed, or by an ending after the rename naming the old key); a link arriving after the rename naming the old key lands on the new key. 0 mismatches. Confidence 0.7 (the rename paths are the newest code).
- P2.3 Reads as known at a sampled point before each book's first amendment give the seeded title, and before a rename the old key. 0 mismatches. Confidence 0.75.
- P2.4 No `_open` link is left waiting at the end. Confidence 0.8.

## Run 3: amendments fill must refuse (kicked back to the inbox)

Unknown target id, a point before the latest amendment, a point before the event's own time, an integer-keyed class, an event with no hearing (an app event).

- P3.1 Fill never wedges: every good event after a refused one fills (P2's checks hold with the refused ones mixed in). Confidence 0.85.
- P3.2 The stopped set equals the refused set exactly, each with a reason, each with tries equal to the limit (3). No failed fill row besides them. Confidence 0.8.
- P3.3 Tries never go down across a kill -9 and never exceed the limit (snapshots of tries taken between kills are monotone per event). Confidence 0.85.
- P3.4 `retry(cursor)` on a stopped event sets tries to 0, and further fills take it back to the limit and into `stopped()` again; a kill during those retries does not change that. Confidence 0.85.

## Run 4: legion's real memory

- P4.1 Importing legion's reflections the way 0.4.0 did gives the same live sets as recorded then. Confidence 0.5: the second edition's API (rannoch-store) differs from 0.4.0's and the import may need rewriting.
- P4.2 Amending and renaming real linked reflections gives the expected reads before and after. Confidence 0.6.
