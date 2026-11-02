#!/bin/zsh
# run.sh <label> <runs> [ENV=VAL ...]: fill 300k events <runs> times with the full-index fix on, print each rate and the median.
B=/Volumes/store/projects/runlegion/rannoch-wt/toy-stats/toys/indexes/target/release/rannoch-toy-indexes
D=${FILL_DIR:-/Volumes/store/tmp/claude-501/fill-toy}; mkdir -p $D
FIX='DROP INDEX IF EXISTS _hearings_to; CREATE INDEX IF NOT EXISTS _hearings_to ON _hearings(entity, line, to_key, key);'
label=$1; runs=$2; shift 2
rates=()
for i in $(seq 1 $runs); do
  r=$(env RANNOCH_TOY_SCHEMA="$FIX" RANNOCH_TOY_PROPIDX=entity "$@" $B fill $D 300000 2>/dev/null | grep -o '[0-9]* events/s' | cut -d' ' -f1)
  rates+=($r)
done
sorted=(${(on)rates}); med=${sorted[$(( (runs+1)/2 ))]}
echo "$label | ${rates[*]} | median $med"
