#!/bin/zsh
# Purge kill loop: purges and a producer under kill -9, a check after every restart, then the last
# check with no clean close, then a clean close. Resumable: the store and counts stay in runs/kg.
B=target/release/purgekill; D=${D:-runs/kg}; T=${T:-140}; F=${F:-20000}; mkdir -p $D
[ -f $D/p.rannoch ] || { $B setup $D $T $F; seq 0 $((T-1)) > $D/remaining.txt; }
if [ ! -f $D/window.done ]; then # a kill in the window a random one rarely hits
  for i in 1 2 3; do $B window $D 1 2>/dev/null; $B check $D 2>/dev/null; done; touch $D/window.done
fi
k=$(cat $D/kills 2>/dev/null || echo 0); still=0; STALL=${STALL:-200}
# A kill every 50 ms to 2.5 s: short ones land inside a purge, long ones let one finish.
while [ -s $D/remaining.txt ]; do left=$(wc -l < $D/remaining.txt)
  $B life $D > $D/out.txt 2>&1 & P=$!
  ms=$((RANDOM % 2450 + 50)); sleep $((ms / 1000)).$(printf '%03d' $((ms % 1000)))
  if kill -9 $P 2>/dev/null; then k=$((k+1)); echo $k > $D/kills; fi
  wait $P 2>/dev/null
  $B check $D 2>/dev/null
  if [ $(wc -l < $D/remaining.txt) -lt $left ]; then still=0; else still=$((still+1)); fi
  if [ $still -ge $STALL ]; then echo "STALLED: no target landed in $STALL kills, $(wc -l < $D/remaining.txt) remain, kills $k"; exit 1; fi
done
echo "kills $k; inside a purge, landed $(grep -c 'landed |' <(grep -v 'not landed' $D/checks.txt)); inside, not landed $(grep -c 'not landed' $D/checks.txt); between $(grep -c '^between' $D/checks.txt)"
echo "refused for a reader $(cat $D/refused.txt 2>/dev/null | wc -l)"
$B last $D
echo "checks $(wc -l < $D/checks.txt), violations $(cat $D/violations.txt 2>/dev/null | wc -l)"
head -20 $D/violations.txt 2>/dev/null
tail -1 $D/checks.txt
