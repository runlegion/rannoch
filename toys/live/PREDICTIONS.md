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

# Predictions, 2026-11-03: the release check (#187), written before any run on this code

Code under test: rannoch main 6606817 (#183, #185, #186, #151 and #192 merged). Machine: Puck, M4, release build, stores on /Volumes/store. The suite is ported to main's API first: reads take a `View`, axis classes replace history keys, amendments name `{target, patch}`, endings carry no `at`. The docs now refuse no amendment for its timing, so the oracle's refusals are the 40 amendments of an app event; an amendment of an unknown target waits; `memory` reads with no valid date.

- R1. Produce 300,000 under kill -9 every 50-300 ms: 0 missing, 0 doubled, 0 unfilled, position 299999. Confidence 0.95.
- R2. Amendment kill run: 0 mismatches against the oracle, refused 40 = stopped 40, each at tries 3, no other fill row, tries never down or over 3 across snapshots. Confidence 0.8. One check is held to the docs ("Read as known before the rename, everything answers under the old key") though #188 relaxed it after seeing a link ended before a rename answer under the new key: expect up to 100 mismatches of that one kind unless it was fixed since. Confidence the kind still shows: 0.6.
- R3. Worker, 3,001 events under kill -9: every event done, none in progress. Confidence 0.95.
- R4. Memory import, 22,619 events from legion's slice: 0 text mismatches of 9,682, live sets per repo exact, follows 808/808, 300 amendments and renames with 0 mismatches. Confidence 0.8.
- R5. Clean fill of 306,000: 85,000-92,000 events/s on a quiet machine (#186 measured 90,455), at least 50,000 under the load this session has. Confidence 0.85 on the 50,000 floor, 0.6 on the range.

## The rerun on main 63681e5, after the rename fix (#196), written before running it

- R6. Amendment kill run: 0 mismatches, refused 40 = stopped 40, all 40 at tries 3 in a snapshot taken after the check, tries never down or over. Confidence 0.9.
- R7. Produce under kills, worker (30,000 events, paced so kills land) and memory import hold as on 6606817; fill stays within 5% of 91,800 events/s, since the fix adds nothing to a store with no rename. Confidence 0.9.

# Predictions, 2026-11-04: the release check (#205), written before any kill run of a relay or a purge

Code under test: main ea94dae, with purge (#148) and the relay (#151), merged into the suite. Machine: Puck, M4, release build, stores on /Volumes/store, other agents running. Clean calibration runs of the two new programs came first (a relay life of 3,000 events; a purge life of 7 targets at about 140 ms each), and no kill.

- R8. `kr.sh`: a relay under kill -9 every 50-300 ms with a producer writing, four relays (capped, exact, class-rooted, a fixed known-at), a check after every restart. Every relay equals a cold walk at every restart. Confidence 0.8. No told cursor is past the log; no told cursor is ahead of what `read` holds; no worker is ahead of its file, no file ahead of the log. Confidence 0.9. In the stepped run, each set equals a cold walk after each commit and each last change reads as told. Confidence 0.75: the walk's cut boundaries are the code most likely to diverge after a restored file.
- R9. Producer under those kills: 0 missing, 0 doubled. Confidence 0.95.
- R10. Stopped events, 0. **Expected to fail:** the calibration of `kr.sh`'s mix stopped three plain node events, from a known defect: after a rename, the same link sent twice under the old key leaves a row naming the old key, and the next node event at that key fails its foreign key. Confidence it shows: 0.95. A clean relay still equals its cold walk, since both read the same tables.
- R11. `kg.sh`: 140 purges of the seven target kinds among 20,000 fillers, with a producer, under kill -9 every 50-300 ms. Each target is wholly before or wholly after, and after exactly when its rule landed. Confidence 0.95, since one transaction holds it. Kills land inside purge calls, both before and after commit; a run with none proves nothing. Confidence 0.7 that some land between commit and truncate.
- R12. After a restart, no byte that left is in the store's file, its write-ahead log or a relay's file. **Confidence 0.3.** A kill between a purge's commit and its truncate leaves the log holding bytes, and `open` does not truncate. A clean close would hide this, so the check after every restart and the last check die without one.
- R13. After the last kill every body sent again is refused, the log does not grow, and the relay equals its cold walk. Confidence 0.9.
- R14. The #187 suite holds as before on purge's merge, with one change expected: "a store with no rule pays one read per commit". Produce under kills, amendment run, worker run, memory import all hold; clean fill at or above 80,000 events a second and in any case over 50,000. Confidence 0.85.
