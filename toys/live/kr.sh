#!/bin/zsh
# Relay kill loop: a producer writing under kill -9, a check after every restart. Then the same with
# the set held against a cold walk after each commit. Resumable: the store and counts stay in runs/kr.
B=target/release/relaykill; D=${D:-runs/kr}; N=${N:-40000}; S=${S:-44000}; mkdir -p $D
[ -f $D/r.rannoch ] || $B setup $D
loop() { # loop <name> <total> [--step]
  local name=$1 total=$2; local k=$(cat $D/$name.kills 2>/dev/null || echo 0)
  [ -f $D/$name.done ] && return
  while :; do $B life $D $total $3 > $D/out.txt 2>&1 & P=$!
    sleep 0.$(printf '%02d' $((RANDOM % 25 + 5)))
    if kill -9 $P 2>/dev/null; then wait $P 2>/dev/null; k=$((k+1)); echo $k > $D/$name.kills; else wait $P; cat $D/out.txt; touch $D/$name.done; fi
    $B check $D 2>/dev/null
    [ -f $D/$name.done ] && break
  done
  echo "$name done, kills $k"
}
loop free $N
loop step $S --step
$B verify $D $S
echo "checks $(wc -l < $D/checks.txt), violations $(cat $D/violations.txt 2>/dev/null | wc -l)"
head -20 $D/violations.txt 2>/dev/null
tail -1 $D/checks.txt
