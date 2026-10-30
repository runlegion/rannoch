# Live test for rannoch's core

A small program that drives a real graph file through the queue the way an app does, so the core is tested against crashes, not only in unit tests. It is its own package (not a workspace member) and builds against the crates by path.

```sh
cargo build --release
B=target/release/rannoch-live
```

## Modes

- `produce <graph> <total>`: writes events 0..total through `Queue::accept_from` with the producer position `n`, resuming from the stored position, then closes. Prints timings.
- `verify <graph> <total>`: counts each event in the log (missing, doubled), checks every node is filled into its table, and prints the producer's position.
- `work <graph>`: drains the log as worker `w` (`next`, then `done`). Prints timings per call.
- `states <graph>`: each committed event's state for worker `w`, and any left in progress.

## Crash runs

Producer, killed at random until it finishes, then verified:

```sh
D=kill.rannoch; N=300000
while :; do $B produce $D $N > out.txt 2>&1 & P=$!
  sleep 0.$(printf '%02d' $((RANDOM % 25 + 5)))
  if kill -9 $P 2>/dev/null; then wait $P 2>/dev/null; else wait $P; cat out.txt; break; fi
done; $B verify $D $N
```

Worker, the same loop around `$B work $D` on a graph produced first, then `$B states $D`.

## Results, 2026-10-29/30 (Puck, M4, release build)

| Run | Result |
|---|---|
| Produce 300,000, clean | 6.6 s, 45,700 events/s; accept about 14 us each |
| Produce 300,000 through 149 kill -9s | none missing, none doubled, none unfilled, position 299999 |
| Worker, 3,001 events through 34 kill -9s | every event done, none in progress |
| Worker, clean | about 133 events/s: `next` 3.4 ms, `done` 4.1 ms (one full sync each) |

Two defects these runs found, both fixed: tables left behind the log after a kill between commit and fill (opening the queue now fills), and events stranded in progress after a worker crash (#145, a restarted worker gets them back).
