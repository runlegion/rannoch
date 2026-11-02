# Toy: where fill spends its time past ~36,500 events/s

Base: branch `toy/stats` with the partial indexes on `_hearings` replaced by full ones (#182's change),
switched by environment variables (`run.sh` sets them). Workload: the index toy's `fill` bench, 300,000
events plus 2% re-sent (306,000), release build, store on /Volumes/store. Predictions were committed
before any run (`PREDICTIONS.md`); results are in `RESULTS.md`, raw output in `results/`.

## Rerun

```
cd ../indexes && cargo build --release && cd ../fill
./run.sh baseline 3
./run.sh fast 3 RANNOCH_TOY_PRAGMAS='PRAGMA cache_size=-65536; PRAGMA temp_store=MEMORY; PRAGMA wal_autocheckpoint=10000;'
```

`run.sh <label> <runs> [ENV=VAL ...]` prints each run's rate and the median. `RANNOCH_TOY_PROFILE=1`
prints per-statement times (it slows fill a lot; use it for shares, not rates). CPU samples were taken
with macOS `sample` during a fill; one is in `results/sample-combined.txt`.
