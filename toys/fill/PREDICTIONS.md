# Predictions: where fill spends its time past ~36,500 events/s

Written before any run in this toy. Base: branch toy/stats with the partial indexes replaced by full ones
(#182's change, switched by `RANNOCH_TOY_SCHEMA=$FIX RANNOCH_TOY_PROPIDX=entity`). Workload: the index
toy's `fill` bench, 300,000 events (half nodes, half links, 2% re-sent), release build, store on
/Volumes/store.

- P1. Fill runs 8-12 SQL statements per event. The history statements (find the current version, insert
  the hearing, the alias check) take at least half of fill's time. Confidence 0.6.
- P2. Commit and fsync take under 10% of fill, because batches grow to 1 s of work. Confidence 0.7.
- P3. Decoding the CloudEvent body and hashing the identity take under 10%. Confidence 0.6.
- P4. SQLite settings alone (larger cache, mmap, temp_store memory, a larger page) gain 10-25%. Confidence 0.5.
- P5. Fewer statements per event (one write per key per batch for `_seen` and stats, no class-table write
  for an unchanged row, reusing the version found once per event) gain 30-60%. Confidence 0.4.
- P6. All changes combined reach 60,000-90,000 events/s with durability kept (a committed batch survives
  kill -9 and power loss). Confidence 0.4.
- P7. The ceiling for SQLite on this machine with this schema (several index updates per event) is
  100,000-150,000 events/s. Confidence 0.3.
- P8. A full fsync per commit costs under 2% of fill, because a fill makes few commits. Confidence 0.7.
