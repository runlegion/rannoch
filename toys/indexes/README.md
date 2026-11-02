# Toy: indexes for rannoch's core schema

Base: branch `toy/stats` (PR #179's reads, the #180 alias join rewrite, per-class stats), merged with
main at 2bac9b5. Toy-only store changes are marked `TOY ONLY` and switched by environment variables;
none is on main. Predictions were committed before any run (`PREDICTIONS.md`); results are in
`RESULTS.md`, raw output in `results/`.

## Rerun

```
cargo build --release
D=/path/on/the/test/disk; mkdir -p $D
FIX='DROP INDEX IF EXISTS _hearings_to; CREATE INDEX IF NOT EXISTS _hearings_to ON _hearings(entity, line, to_key, key);'
COV='DROP INDEX IF EXISTS _hearings_at; CREATE INDEX IF NOT EXISTS _hearings_at ON _hearings(entity, line, key, to_key, axis, known, event, until);'
# fill, today and with each change (4 runs each, median):
./target/release/rannoch-toy-indexes fill $D 300000
RANNOCH_TOY_SCHEMA="$FIX" RANNOCH_TOY_PROPIDX=entity ./target/release/rannoch-toy-indexes fill $D 300000
#   add RANNOCH_TOY_CC=1 (counter_cache), RANNOCH_TOY_KNOWN=1 (kept known-at), $COV (covering until)
# game-size stores and reads:
for a in 0 10 50; do ./target/release/rannoch-toy-indexes build $D $a; done
for a in 0 10 50; do ./target/release/rannoch-toy-indexes reads $D $a; done
RANNOCH_TOY_CC=1 ./target/release/rannoch-toy-indexes verify $D 0      # counter_cache against real counts
# plans of every statement fill and the reads run:
./target/release/rannoch-toy-indexes capture $D 0 > statements.txt; python3 plans.py $D/idx-0.rannoch statements.txt
python3 ab.py $D/idx-50.rannoch       # window vs per-key backward seek for listing
python3 a_now.py <copy of a store>    # A: a current-version table for listing at now
```

`RANNOCH_TOY_PROFILE=1` prints the slowest statements of a fill; `RANNOCH_TOY_PHASES=1` times nodes,
links and re-sent events as separate fills.
