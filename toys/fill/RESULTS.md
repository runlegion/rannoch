# Results: fill past ~36,500 events/s

Clean fill, 306,000 events (300,000 plus 2% re-sent), release build, /Volumes/store, median of 3 runs
unless noted. All with the full-index fix (#182). Run-to-run noise is about +/-5%.

## Answer to the target

**50,000 events/s is reachable with durability kept, by SQLite settings alone: 57,000-58,600/s.** No code
change is needed for it. The settings leave `synchronous=FULL` and `fullfsync=ON` as they are, so a
committed batch is written and fully synced exactly as before. They were not re-run under the kill -9
harness in this toy.

## Each change

| Change | Median events/s | vs baseline | Durability | Risk |
|---|---|---|---|---|
| Baseline (full indexes, default settings) | 35,158 | - | as now | - |
| `cache_size` 256 MB (default is 2 MB) | 53,612 | +52% | unchanged | memory per connection |
| + `mmap_size` 1 GB | 52,134 | none | unchanged | none gained |
| `cache_size` 1 GB | 51,568 | none over 256 MB | unchanged | more memory |
| cache + `secure_delete=OFF` | 52,957 | none | unchanged | purge needs it on |
| cache + `temp_store=MEMORY` | 56,123 | +5% | unchanged | none |
| cache + `wal_autocheckpoint=10000` | 56,788 | +6% | unchanged | larger WAL, longer open after a crash |
| **cache 256 MB + temp_store memory + autocheckpoint 10000** | **58,604** (recheck 57,096) | **+63-67%** | unchanged | as above |
| same, cache 64 MB | 57,848 | +65% | unchanged | the knee: 32 MB gives 52,942 |
| + `page_size=16384` | 58,487 | none | - | did not apply: the page size stays 4096 once WAL is set first |
| `synchronous=NORMAL` (measure only) | 40,721, unstable | - | loses power-loss durability | rejected |
| `fullfsync=OFF` (measure only), default cache | 34,780 | none | loses power-loss durability | rejected |
| `fullfsync=OFF` (measure only), with the settings | 58,833 | +0.4% over the settings | loses power-loss durability | rejected |

## Where the time goes

- **The page cache was the bottleneck.** With SQLite's default 2 MB cache, fill read index pages back
  from disk and spilled dirty pages to the WAL mid-transaction: `pread`, `pwrite` and `pagerStress`
  under `sqlite3BtreeIndexMoveto` filled the CPU samples. A 64 MB cache keeps the working set in memory
  until commit.
- **fsync is small.** The full fsync per commit shows as about 11% of fill's samples with the settings,
  but turning it off gains under 1%, because a fill makes about 10 commits.
- **What is left is SQLite's B-tree work and Rust overhead.** In the samples with the settings, the top
  entries are B-tree compares (`memcmp`, `sqlite3VdbeRecordCompareWithSkip`, `vdbeRecordCompareString`:
  index keys start with long text, the entity name and `line`), `sqlite3VdbeExec`, `memmove`, and Rust
  formatting (`fmt::write`, `String::write_str`) plus prepared-statement cache lookups (`foldhash`
  hashing each SQL string per call). Decoding and the identity hash (BLAKE3) are under 1%.
- **Statements per event:** about 7 for a node and 9 for a link: the identity lookup, the current-version
  lookup, the hearing insert, the `_seen` read and write, the class or relation row, and for links two
  extra class-id lookups (one redundant with the relation insert's own subselects).

## Predictions

| | Prediction | Result |
|---|---|---|
| P1 | 8-12 statements per event; history statements over half the time | 7-9 statements; history statements were the largest single share, but the dominant cost was page-cache misses, not the statements themselves |
| P2 | commit and fsync under 10% | held for fsync (under 1% to remove); COMMIT's page writes grow with a large cache but stay inside the gain |
| P3 | decode and hash under 10% | held (under 1%) |
| P4 | settings gain 10-25% | **failed low: settings gained 63-67%** |
| P5 | fewer statements gain 30-60% | not built in this toy; see below |
| P6 | all combined 60,000-90,000/s | settings alone reach 57,000-58,600/s |
| P7 | SQLite ceiling 100,000-150,000/s | estimate below |
| P8 | fsync under 2% | held (under 1%) |

## Ceiling, estimated

With the settings, fill spends about 5.3 s on 306,000 events (17 us an event). The statement profile puts
the SQL work, not counting commit, at about 8.5 us an event; the rest is Rust (formatting SQL strings and
hashing them for the statement cache per call, allocation) and commit. Removing the redundant class-id
lookups, building each statement's SQL once and keeping its handle, and keying the history indexes by an
integer entity id instead of text would plausibly reach 75,000-90,000/s. That is an estimate from the
profile, not a measurement. Past that, the schema's several index updates per event bound it at roughly
100,000/s on this machine.

## Not tried

- A larger first fill batch: a fill makes about 10 commits, so it cannot move the rate measurably.
- Code changes to cut statements per event (P5): not needed for the target; they are the path past
  ~60,000/s.
