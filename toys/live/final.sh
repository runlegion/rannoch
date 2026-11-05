#!/bin/zsh
# The whole release check, in #187's order and then the relay and purge runs, into one results file.
# Each kill run starts on a fresh store. Usage: ./final.sh <results file>
OUT=$1; R=runs; B=target/release
for f in kill.rannoch kill.rannoch-wal kill.rannoch-shm kp.kills kp.done out.txt; do mv $R/$f $R/old-$f 2>/dev/null; done
for f in $R/ak* $R/aktries-*; do mv $f $R/old-${f:t} 2>/dev/null; done
{
echo "start $(date) $(uptime)"; git rev-parse HEAD; git rev-parse origin/main
for i in 1 2 3; do $B/fillbench $R; done
echo "--- memory"; $B/memory /Volumes/store/projects/scratch/rannoch-toys/legion-memory/slice.jsonl $R/memory-$$.rannoch
echo "--- worker"; ./kw.sh | tail -6
echo "--- produce"; until [ -f $R/kp.done ]; do CAP=540 ./kp.sh; done
echo "--- clean verify"; $B/rannoch-live verify $R/kill.rannoch 300000
echo "--- amend"; until [ -f $R/ak.all.done ]; do CAP=540 ./ka.sh; done
echo "--- relay"; D=$R/final-kr ./kr.sh
echo "--- purge"; D=$R/final-kg ./kg.sh
echo "end $(date) $(uptime)"
} 2>&1 | tee -a $OUT
