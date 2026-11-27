#!/bin/sh
# graph toy round 5 runner: runs round5 with the given environment, samples the WAL file from
# this separate process (stat only, never opening the database) every 100 ms, and reports the
# largest size and /usr/bin/time -l's peak memory footprint.
# usage: run5.sh <name> <data dir> <T8 file> [VAR=value ...]
name=$1; dir=$2; t8=$3; shift 3
mkdir -p "$dir"
wal="$dir/round5.db-wal"
out="$dir/$name.out"
/usr/bin/time -l env "$@" ./target/release/round5 "$dir" "$t8" > "$out" 2>&1 &
pid=$!
max=0
while kill -0 $pid 2>/dev/null; do
  s=$(stat -f %z "$wal" 2>/dev/null || echo 0)
  [ "$s" -gt "$max" ] && max=$s
  sleep 0.1
done
wait $pid
echo "== $name ($*)"
grep -E "^(load|P5a|P5c|relay|latency|shutdown|KILL)" "$out"
grep -E "peak memory footprint|real" "$out" | sed 's/^ *//'
echo "WAL largest, sampled from outside every 100 ms: $max bytes ($((max / 1048576)) MiB)"
