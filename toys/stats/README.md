# Toy: class stats kept by fill, and sub-millisecond node link reads

Base: PR #179's reads (`feat/175-reads` at 34be65e). This branch (`toy/stats`) also carries toy-only
changes to `crates/rannoch-store`, marked `TOY ONLY`; none of them is on main or on the PR branch.

Predictions were committed before any run (`PREDICTIONS.md`); results are in `RESULTS.md`.

## Rerun

```
cargo build --release                                  # in toys/stats
D=/path/on/the/test/disk; mkdir -p $D
./target/release/rannoch-toy-stats check $D            # stats against counts(): fill, amend + rename, rebuild
for r in 1 2 3 4; do RANNOCH_TOY_STATS=0 ./target/release/rannoch-toy-stats fill $D 300000; \
  ./target/release/rannoch-toy-stats fill $D 300000; done   # fill cost of stats, alternating
for a in 0 10 50; do ./target/release/rannoch-toy-stats reads $D $a; done   # game-size store, a% re-sent
RANNOCH_TOY_REUSE=1 ./target/release/rannoch-toy-stats reads $D 50          # re-time an existing store
```

`RANNOCH_TOY_RUNS=0` times a single call. To see the store without the alias join fix, change
`CROSS JOIN _aliases` back to `JOIN _aliases` in `crates/rannoch-store/src/queries.rs` and build to
another target dir.
