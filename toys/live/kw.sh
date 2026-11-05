#!/bin/zsh
# Worker kill loop: produce N events clean, then work them, kill -9 at random until a run finishes unkilled.
B=target/release/rannoch-live; D=runs/kw.rannoch; N=${N:-30000}; k=0
rm -f $D $D-wal $D-shm
$B produce $D $N | tail -1
while :; do $B work $D > runs/kw.out 2>&1 & P=$!
  sleep 0.$(printf '%02d' $((RANDOM % 25 + 5)))
  if kill -9 $P 2>/dev/null; then wait $P 2>/dev/null; k=$((k+1)); else wait $P; cat runs/kw.out; break; fi
done
echo "kills $k"; $B states $D
