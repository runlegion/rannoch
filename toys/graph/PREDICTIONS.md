# Predictions (written before any run)

Question: does the store give back the graph that went in? Everything is read through the public
reads (`nodes`, `links`, `counts`, `describe`) at main b839e71. Walks and relays are out of scope.
What a read should answer is taken from docs/store.md, never from the store's code. Timings are
recorded but are not part of any pass.

## P1. A small graph, expected edges written by hand

One class `person`, relation `follows` (person to person), events known at fixed seconds.

- Cycle a>b>c>a, self-loop d>d, hub h to h1..h5, isolated node i: all come back exactly.
- A link to a node never heard (x>ghost) holds: `links(x, Out)` gives ghost, `links(ghost, In)` gives
  x, `counts` counts it, and `nodes` does not list ghost.
- e>f ended at 20: gone now, there at known-at 15.
- g>k heard, ended, heard again: holds now, not between the ending and the second hearing.
- m>n corrected at 20: docs say "the link was wrong", so it does not hold now. PR #179's notes say
  a corrected link reads as holding. Prediction: it does not hold. A miss here is a finding.
- r>t, then node r renamed to s at 30: now the link is s>t, `links(r)` is empty and `nodes` lists s
  and not r. At known-at 25 the link is r>t and `nodes` lists r.
- Relation `ships` with axis `build`, p>q heard at build 1, ended at build 2, heard at build 3: holds
  at builds 1 and 3, not at 2.

## P2. Real data, truth from git

A repository's commits and files as nodes, parent and touched as links, known-at the commit time,
sent in shuffled order. Truth is parsed from `git rev-list` and `git log` output into plain sets.

- Every node, every link in both directions, every `counts` and `describe` number, and a filter on an
  indexed property equal the truth: 0 differences, now and at the median commit time.
- A second store fed the same events in another order reads identically.
- Rebuilding the graph takes one `nodes` per class and two `links` per node. Prediction: about 7,000
  to 25,000 reads on a 3,435-commit repository, finishing in under 10 s.

## P3. Seeded random histories against a plain model

A model in this toy, no store code, written from the docs: within a view (facts at or before its
axis point and known-at), a node's version and a link's state are its newest fact by point, then
known-at, then event id; a link holds when that fact is a live hearing. Axis and no-axis relations
both. Three arrival orders, fill in chunks, every axis and known-at pair on a grid.

- Without re-sends: 0 differences.
- With identical content re-sent under a new id (held as an alias): the docs do not say whether a
  re-send after an ending reopens the link. The model says it does. Prediction: 50/50; any difference
  is a question about the docs, not automatically a defect.

## What a miss means

Any difference from P1 or P2 is a defect or a docs gap to report, not to explain away. No code in
rannoch changes in this toy.

## Added after the first P1 run, before this case ran

u>v heard, then ended, then corrected (PR #179's notes say a correction masks a link's endings).
From the docs, the link was wrong and has also ended, so it does not hold. Prediction: it does not
hold; PR #179's note suggests it may come back. Also, one re-sent link in each kind of relation is
asked for its hearings: an axis re-send at the same build should be 1 hearing; a no-axis re-send has
a new known-at, and the docs do not say whether that is an alias. Prediction: 1 hearing for both.
