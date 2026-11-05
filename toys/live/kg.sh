#!/bin/zsh
# Purge kill loop: purges and a producer under kill -9, a check after every restart, then the last
# check with no clean close, then a clean close. Resumable: the store and counts stay in runs/kg.
B=target/release/purgekill; D=${D:-runs/kg}; T=${T:-140}; F=${F:-20000}; mkdir -p $D
[ -f $D/p.rannoch ] || { $B setup $D $T $F; seq 0 $((T-1)) > $D/remaining.txt; }
k=$(cat $D/kills 2>/dev/null || echo 0)
while [ -s $D/remaining.txt ]; do $B life $D > $D/out.txt 2>&1 & P=$!
  sleep 0.$(printf '%02d' $((RANDOM % 25 + 5)))
  if kill -9 $P 2>/dev/null; then k=$((k+1)); echo $k > $D/kills; fi
  wait $P 2>/dev/null
  $B check $D 2>/dev/null
done
echo "kills $k; inside a purge, landed $(grep -c 'landed |' <(grep -v 'not landed' $D/checks.txt)); inside, not landed $(grep -c 'not landed' $D/checks.txt); between $(grep -c '^between' $D/checks.txt)"
echo "refused for a reader $(cat $D/refused.txt 2>/dev/null | wc -l)"
$B last $D
echo "checks $(wc -l < $D/checks.txt), violations $(cat $D/violations.txt 2>/dev/null | wc -l)"
head -20 $D/violations.txt 2>/dev/null
tail -1 $D/checks.txt
