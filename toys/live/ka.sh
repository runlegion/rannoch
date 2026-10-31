#!/bin/zsh
# Resumable amendment kill loop (kill-amend.sh, split so each call runs at most CAP seconds).
A=target/release/amend; D=runs/ak.rannoch; N=2000; CAP=${CAP:-540}; start=$(date +%s)
setopt null_glob
SNAP=$(ls runs/aktries-*.txt 2>/dev/null | wc -l | tr -d ' ')
phase() { # phase <name> <cmd...>: kill at random until it finishes unkilled; marks runs/ak.<name>.done
  local name=$1; shift; local k=$(cat runs/ak.$name.kills 2>/dev/null || echo 0)
  [ -f runs/ak.$name.done ] && return 0
  while :; do "$@" > runs/ak.$name.out 2>&1 & local P=$!
    sleep 0.$(printf "%02d" $((RANDOM % 25 + 5)))
    if kill -9 $P 2>/dev/null; then wait $P 2>/dev/null; k=$((k+1))
      $A tries $D > runs/aktries-$(printf '%05d' $SNAP).txt 2>/dev/null; SNAP=$((SNAP+1))
    else wait $P; cat runs/ak.$name.out; touch runs/ak.$name.done; break; fi
    if [ $(( $(date +%s) - start )) -gt $CAP ]; then echo $k > runs/ak.$name.kills; echo "paused in $name, kills $k"; return 1; fi
  done
  echo $k > runs/ak.$name.kills; echo "$name done, kills $k"
}
phase seed $A seed $D $N || exit 0
[ -f runs/ak-script.jsonl ] || $A script $D $N runs/ak-script.jsonl runs/ak-oracle.json
phase feed $A feed $D runs/ak-script.jsonl || exit 0
$A tries $D > runs/aktries-$(printf '%05d' $SNAP).txt
echo "CHECK:"; $A check $D runs/ak-oracle.json runs/ak-script.jsonl
python3 - <<'PY'
import glob
prev={}; down=over=0; snaps=sorted(glob.glob("runs/aktries-*.txt"))
for f in snaps:
    cur={}
    for line in open(f):
        p=line.split(" ",2)
        if len(p)>=2: cur[int(p[0])]=int(p[1])
    for s,t in cur.items():
        if t>3: over+=1
        if s in prev and t<prev[s]: down+=1
    prev=cur
final=prev
print(f"tries snapshots {len(snaps)}: went down {down}, over the limit {over}; final rows {len(final)}, at 3: {sum(1 for t in final.values() if t==3)}, other: {sorted(set(final.values()))}")
PY
touch runs/ak.all.done
