#!/bin/zsh
# Resumable producer kill loop: runs at most CAP seconds per call; DONE file when produce finished unkilled.
B=target/release/rannoch-live; D=runs/kill.rannoch; N=300000; CAP=${CAP:-540}
k=$(cat runs/kp.kills 2>/dev/null || echo 0); start=$(date +%s)
while :; do $B produce $D $N > runs/out.txt 2>&1 & P=$!
  sleep 0.$(printf '%02d' $((RANDOM % 25 + 5)))
  if kill -9 $P 2>/dev/null; then wait $P 2>/dev/null; k=$((k+1)); else wait $P; cat runs/out.txt; touch runs/kp.done; break; fi
  if [ $(( $(date +%s) - start )) -gt $CAP ]; then echo "paused"; break; fi
done
echo $k > runs/kp.kills; echo "kills so far $k"; $B verify $D $N | tail -1
