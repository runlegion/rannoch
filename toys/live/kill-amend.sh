#!/bin/zsh
# Runs 2 and 3 under kill -9: seed, script, feed the script, each killed at random until done.
# After every kill the fill rows' tries are snapshotted, to check they never go down or past 3.
set -u
A=target/release/amend
D=${1:-runs/am-kill.rannoch}
N=${2:-2000}
rm -f $D $D-wal $D-shm runs/tries-*.txt
kills() {  # kills <out> <cmd...>: run cmd, kill -9 at random until it finishes on its own
  local out=$1; shift; local k=0
  while :; do "$@" > $out 2>&1 & local P=$!
    if [ -n "${WIDE:-}" ]; then sleep $(( (RANDOM % 26 + 5) / 10.0 )); else sleep 0.$(printf "%02d" $((RANDOM % 40 + 5))); fi
    if kill -9 $P 2>/dev/null; then wait $P 2>/dev/null; k=$((k+1))
      $A tries $D > runs/tries-$(printf '%04d' $SNAP).txt 2>/dev/null; SNAP=$((SNAP+1))
    else wait $P; cat $out; break; fi
  done
  echo "kills $k"
}
SNAP=0
kills runs/seed.out $A seed $D $N
$A script $D $N runs/k-script.jsonl runs/k-oracle.json
kills runs/feed.out $A feed $D runs/k-script.jsonl
$A tries $D > runs/tries-$(printf '%04d' $SNAP).txt
$A check $D runs/k-oracle.json runs/k-script.jsonl
python3 - <<'EOF'
import glob
prev = {}; down = over = 0; snaps = sorted(glob.glob("runs/tries-*.txt"))
for f in snaps:
    cur = {}
    for line in open(f):
        parts = line.split(" ", 2)
        if len(parts) >= 2: cur[int(parts[0])] = int(parts[1])
    for s, t in cur.items():
        if t > 3: over += 1
        if s in prev and t < prev[s]: down += 1
    prev = cur
print(f"tries snapshots {len(snaps)}: went down {down}, over the limit {over}")
EOF
