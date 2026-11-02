# Results

Stores: 742k-event game shape (330k nodes, 411k links, fan-outs 1 to 120,000, 5,000 endings, a
declared index on `book.title`), with 0%, 10% and 50% of hearings re-sent under new ids. Fill bench:
300,000 events with 2% re-sent, 4 runs, median. Release builds, stores on the external drive.

## The finding that dominates: partial indexes make SQLite re-prepare statements

Both partial indexes on `_hearings` (`_hearings_to ... WHERE to_key IS NOT NULL`, and each declared
property index `... WHERE entity = 'class:<c>' AND line = 'main'`) name a column that the store's
statements bind (`entity = ?1`, `to_key IS ?3`). SQLite then marks those statements to re-prepare
whenever the bound value changes, so fill and reads re-parse their SQL on nearly every call (sampled:
`sqlite3LockAndPrepare` inside `read_version` and `saw`). Replacing them with full indexes
(`_hearings_to(entity, line, to_key, key)`; a property index led by `entity`) removes it.

| | today (partial) | no partial indexes |
|---|---|---|
| fill, 300k, declared index | 13,440/s | **36,574/s** (2.7x) |
| fill, 300k, no declared index (resend off) | 13,640/s | **40,802/s** |
| game build, 747k / 821k / 1.12M events | 69.8 / 87.7 / 178.9 s | **28.6 / 39.7 / 98.8 s** (all changes) |
| state_at / find | 0.044 / 0.028 ms | **0.005 / 0.004 ms** |
| link_state_at | 0.18 ms | **0.013 ms** |
| links of a 1-link node | 0.10 ms | **0.009 ms** |

The stats toy's ~40k fill (and PR #181's) came from a run whose binding order happened to avoid most
re-prepares; the same build with resend off ran at 13.6k.

## Each candidate (ActiveRecord pattern), fill cost against read gain

| Pattern (ActiveRecord name) | rannoch change | Fill (vs no-partial 36,574/s) | Read gain |
|---|---|---|---|
| polymorphic `belongs_to` index `[item_type, item_id]` | full `(entity, line, key, ...)` and `(entity, line, to_key, key)`; no partial `WHERE entity =` | baseline of this column | point reads 8-14x, small listings 10x (table above) |
| paper_trail `where(t <= x).order(desc).first` | `_hearings_at(entity, line, key, to_key, axis, known, event)` already is this index; point reads use `ORDER BY ... LIMIT 1` on it | 0 (exists) | state_at / link_state_at 0.005 / 0.013 ms at now and at a known-at |
| paper_trail backward seek for listing | per-key `ORDER BY ... LIMIT 1` instead of the window | 0 (rewrite) | **slower**: 1.4x at 0% aliases, 1.07x at 50% (`results/ab-window-vs-seek.txt`); the window stays |
| has_many :through, one index per foreign key | from end: `_hearings_at`; to end: `_hearings_to` made full | (in the fix) | links in as fast as links out |
| counter_cache | per-node link counts per relation and direction, kept per batch (`o:/i:<rel>:<key>`) | **-3.1%** (35,430/s) | describe 200-470 ms on a 120,000 hub -> **0.008 ms** at every fan-out; verified equal to real counts on 277 nodes x 3 stores |
| (kept value) | the store's newest known-at, kept per batch | 0% (36,677/s) | answer with no known-at: 66-100 ms of scan -> **0.04 ms**; equals the scan on 0% and 50% stores |
| (covering index) | `until` added to `_hearings_at` | +0.6% (noise) | counts at a view 278 -> 242 ms (13%); not 2x |
| A, current-version lookup | `_now(entity, line, key, to_key) -> hearing`, built once (`a_now.py`) | not measured fairly: a trigger proxy ran 13,904/s (triggers on `_hearings` bring back slow paths); a real one is one cached upsert per fact, like `_seen` | links at now **0.4 µs/row flat**: 3.3x faster than the window at 0% aliases, 7x at 50% (120,000 hub: 208 -> 64 ms; 418 -> 64 ms) |

Combined, no-partial + counter_cache + kept known-at + covering `until`: **35,657/s**, -2.5% against
no-partial alone and 2.65x today. counter_cache is the only measurable cost.

## Reads with all chosen changes (ms; 0% / 10% / 50% aliases)

| read | today | all changes |
|---|---|---|
| links out, 1 / 100 / 1,000 links | 0.10 / 0.23 / 1.49 | 0.010 / 0.14 / 1.36 (0%) |
| links out, 10,000 / 120,000 | 14.9 / 244 (0%), 128 / 501 (50%) | 14.2 / 242 (0%), 40 / 487 (50%) |
| page of 100 links, any fan-out up to 120,000 | 0.27-0.60 | 0.27-0.58 |
| describe, any fan-out | 0.14 -> 471 | **0.007-0.008** |
| state_at, find, link_state_at | 0.044, 0.028, 0.18 | 0.006, 0.005, 0.014 |
| nodes by indexed value, 6 rows / 4,000 rows | 0.105 / 26.7 | 0.026 / 28-34 |
| counts now (pass) / stats | 186 / 0.001 | 235 / stats |
| answer, default known-at | 100-610 | 0.04 + the read |

Whole listings stay about 1.2 / 2.2 / 3.3 µs a row (0 / 10 / 50% aliases); a page bounds them under
0.6 ms. Note: with per-node counters in the same `_stats` table, reading the class stats scans 330k
rows (21 ms); the counters belong in their own table.

## Plans (`results/plans-0-before.txt`, `plans-0-fix.txt`)

No read or common fill statement scans a table. Remaining table fetches carry payload columns
(event, source, valid, properties) that no index should cover. Scans left on rare paths: the
amendment lookup by CloudEvent id scans `_events` (#164's known cost), and fill's amendment path
finds `amended = ?` by scanning `_hearings` by line (amendments are under 1% of events).

## Predictions

| | | Held? |
|---|---|---|
| P1 | listing reads window over every version; counts fetch `until` | held (payload fetches remain by design) |
| P2 | kept known-at < 0.05 ms at no fill cost | held (0.04 ms, 0%); the scan was 66-100 ms, above my 15-40 |
| P3 | A makes now point reads < 0.05 ms, listing ~1 µs/row | point reads already 0.005 ms without A (the re-prepare fix did it); listing 0.4 µs/row held; fill cost unmeasured |
| P4 | B gives past point reads one backward seek; listing gains less; aliases a second seek | held: the index already exists; per-key seek is slower than the window for listing |
| P5 | counter_cache makes describe sub-ms at 1-2% fill | held on reads (0.008 ms); fill -3.1%, above my 1-2% |
| P6 | covering `until` halves counts at a view | failed: 13% |
| P7 | all changes within 2% of today | held against today (2.65x faster); -2.5% against no-partial alone |
| P8 | the window form can't be dropped | held: it beats per-key seek for listing, and A covers only now |
| P9 (Sean) | proper indexes give about another 25% fill over #180 | the gain is 2.7x, not 25%, and it comes from removing the partial indexes rather than adding indexes; adding counter_cache costs 3%. His point that fill's own lookups would speed up held: fill's version lookups were re-preparing too |

## Recommendation

1. Replace both partial indexes on `_hearings` with full ones (`_hearings_to(entity, line, to_key, key)`;
   declared property indexes led by `entity`), and keep any future index free of `WHERE` clauses on
   columns statements bind. This is the largest win for fill and for every point read.
2. counter_cache for describe (3% fill), in its own table; the newest known-at as a kept value (0%).
3. Page every listing read (links, and nodes, which is 7 µs a row) with the key-range form.
4. A current-version table is worth building for listings at now (3-7x), measured with a real cached
   upsert in fill, not triggers. Keep the window form for past views.
5. Skip the covering `until` index: it buys 13% on counts at a view, which stats and paging make rare.
