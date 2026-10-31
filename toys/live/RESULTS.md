# Results, 2026-10-30 (rannoch main 3f48131, Puck, M4, release build)

Predictions in PREDICTIONS.md were committed (fac2727) before any of these runs.

## Run 1: baseline

| | Prediction | Result | Verdict |
|---|---|---|---|
| P1.1 | Produce 300,000 clean: invariants hold, 25,000-45,000 events/s | 0 missing, 0 doubled, 0 unfilled, position 299999; **12,814 events/s** (23.4 s, of which close+drain 18.3 s) | Invariants held; throughput FAILED |
| P1.2 | Produce 300,000 through kill -9 (window 50-300 ms): invariants hold | **Livelock.** After about 10 minutes the producer stood at position 10,673 and fill at 4,781, and neither moved. | FAILED |
| P1.2b | Same, kill window 0.5-3.0 s | Progressing but slow: I stopped it at position 93,000 after about 8 minutes, with fill at 22,502. One final run without a kill finished it: 0 missing, 0 doubled, 0 unfilled, position 299999. Its open took 2.65 s to fill the 70,000-event backlog. | Invariants HELD after recovery; finishing under continuous kills NOT shown |
| P1.3 | Worker, 3,001 events through kill -9 | 119 kills; every event done (3,002 rows Done, the declaration included), none in progress | HELD |

Throughput by commit (same harness, clean produce of 300,000):

| main at | events/s |
|---|---|
| fdc0d29 (before fill retries, #157) | 41,576 |
| 08d651e (fill retries, #157) | 25,351 |
| 3f48131 (amend, #158) | 12,814 |
| README, 2026-10-29 | 45,700 |

## Run 2 and 3: amendments, clean

Seed of 2,000 books (6,320 events), script of 4,930 events (640 of them must be refused), fed through the queue without kills:

- P2.1-P2.4 HELD: 2,000 books checked, 0 mismatches (titles patched, removed by `null`, stacked, same-point by event id; renames answer under the new key, the old key is gone, links follow, ended exactly as the oracle says, late links naming the old key land on the new one; reads as known before give the seeded title and the old key); `_open` 0.
- P3.1-P3.2 HELD: refused 640, stopped 640, the same set; every stopped row has tries 3; no other fill row. Reasons: 540 before own point or latest amendment (500 before-latest, 40 before an entry's own time), 20 unknown target, 40 integer-keyed class, 40 no hearing.
- P3.4 HELD: `retry` on a stopped amendment set tries to 0; three fills took it to 1, 2, 3; two more left it at 3; it is in `stopped()` again. Under a fill loop killed six times after a retry, tries ended at 3 and never passed it (the loop reached 3 before the first kill, so this is weak evidence for a kill mid-retry).
- Cost: the script's 4,930 events took 40.4 s to feed (about 8 ms each), against 0.68 s for the 6,320-event seed.

## Run 2 and 3: amendments under kill -9

- Window 50-300 ms: **livelock**, as in P1.2. The seed finished; the script's producer stood at position 600 and fill at 6,325 (the end of the seed) and did not move.
- Window 0.5-3.0 s: also stuck. The script was fully committed (position 4,929), but fill stayed at 6,926, 600 events into the script, across 43 kills. One final run without a kill filled it: opening took **19.1 s** to fill the 4,329 events behind (about 4.4 ms an amendment). Then the check: 2,000 books, 0 mismatches; refused 640 = stopped 640, every one at tries 3, no other fill row, `_open` 0, processed = committed = 11,255. HELD after recovery.
- P3.3: 44 snapshots of the tries, one after each kill and one at the end: none went down and none passed 3. Weak evidence: in 43 of them fill was stuck, so the same 79 rows held the same counts.

## Run 4: legion's real memory (9,682 reflections from the 0.4.0 toy's slice)

- Load: 22,619 events in 3.5 s, none refused, none stopped.
- P4.1 HELD: 0 text mismatches of 9,682; live sets per repo equal the slice exactly (eavesdrop 1,128, kessel 1,051, rafters 3,074, rannoch 34, smugglr 1,708, warden 246); follows 808/808.
- P4.2 HELD: 200 reflections amended and 100 renamed (each with follows children), 0 mismatches: amended text now, original text as known before; renamed ones answer under the new key with the same text, the old key is gone, the in-link keeps its live or ended state, children's follows links answer under the new key and as known before under the old one.
- Cost: **106 ms per amendment** (300 in 31.9 s, committed in chunks of 50 with a fill after each).
- Toy fixes along the way, not rannoch defects: reflections need a valid day for `find_as_of` to answer (the 0.4.0 toy gave them one too); a renamed child's follows link answers under its own new key.

## Bugs

1. **Fill does all-or-nothing work behind a crash, so a store crashing faster than it can fill never recovers (livelock).** `Queue::open` fills the whole backlog in one transaction before it returns (rannoch-queue `lib.rs:113`, `graph.fill()?`), and `fill` commits only at its end. A backlog that takes longer to fill than the time between crashes never commits; the producer cannot accept until open returns. Repro: a copy of the stuck run-1 file (10,675 events, fill at 4,781) takes 516 ms to fill (`src/bin/fillprobe.rs`), longer than the 50-300 ms kill window, so every restart starts over. The amendment run's 600-event backlog took 2.3 s. Before #157 the same backlog filled in about a third of the time and the README's 149-kill run finished. The fix is not this toy's to choose: fill could commit in bounded pieces so progress survives a crash, or fill could get faster (2 and 3).
2. **Fill throughput fell to a third in two merges.** Clean produce of 300,000: 41,576 events/s before #157, 25,351 after #157 (a savepoint and an inbox write per event), 12,814 after #158 (amend: rename checks and refreshes on every node and link fill). Measured with the same harness on detached worktrees of fdc0d29 and 08d651e.
3. **An amendment's fill cost grows with the store.** About 4 ms per amendment on the 6,000-event seed, 106 ms on the 22,000-event memory store. One cause: `PLACES_NAMING` (queries.rs) filters with `CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END`, so its plan is `SEARCH _hearings USING INDEX _hearings_at (entity=? AND line=?)` and it walks every hearing of the entity (9,782 for `relation:in`) on each call. There is no index that leads with `to_key`.
