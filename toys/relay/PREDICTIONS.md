# Predictions

Written before any run of this toy. The apps' predictions are copied as written, from kessel's post 01a195c0 and warden's reply to rannoch-query rev 2. Ground truth is SQL on the spice builds, read before the toy (below). Issue #151 names the two recorder stand-ins.

## The importer's toys (kessel), as written

T1, cycles and exact counts, as answers and a relay.
- count(the 216-conversation NPC, ["speaks_in"]) as a rannoch.query: one rannoch.answer, subject = the query id, result 216, known-at = the commit that loaded the cnv links. Round trip under 40 ms (two heartbeats at most), against under 2 ms in-process. Breaks if the count is off by one or known-at is missing or later than the load.
- Same count made durable as a report, then commit 50 more speaks_in links for that NPC. Re-reading the report still says 216 at the old known-at; a fresh count says 266. Breaks if the report moves.
- walk(a quest in one of the 10 arc cycles, ["next_in_arc"], cap 1000) as a relay: the ack says nodes per hop, not cut, each node once; the warm set read in-process equals the SQL closure. No change events while nothing commits. Breaks if the ack lists a node twice or the relay emits on an idle heartbeat.

T2, hub cut and determinism, as a relay.
- Relay(stabilizer tier2, ["uses<-", "crafts"], caps hop1=500). Ack: hop 1 cut, 703 found, 203 dropped, warm set = the 500 lowest keys. Two relays on the same shape (or one shared, if sharing by shape lands) hold identical warm sets; paging and read() return the same 500 in the same order.
- Boundary churn, new: commit one schematic whose key sorts below the cut. Prediction: one change event carries both moves (the new key in, the old 500th key out), dropped becomes 204. Breaks if the event shows only the insert, if the warm set grows to 501, or if the out-going key never leaves.

T3, two builds, now as change events.
- Load 7.8.1.c quest-chain, open a relay on a quest whose arc changed, then commit 7.9 (new links, and ends for links that left). Prediction: the relay's change events equal the SQL diff of that neighbourhood exactly, with 7.9's valid time on each. A second relay opened with view = 7.8.1 date emits nothing for the 7.9 commit. Breaks if an ended link stays in the latest relay's warm set, or the 7.8.1 relay reacts to 7.9.

## The recorder's toys (warden), as written

T1 Hutta relay seeds under 1 s and equals a cold recompute. T2 read() by logname under 0.05 ms. T3 line to overlay change event under 60 ms p99 with 4 workers running.

## Issue #151's numbers

A relay of about 1,000 nodes (a planet-sized shape on real game data) seeds in under 1 second; applying an event costs under 1 ms; read() answers in under 0.05 ms.

## How the toy maps them, decided before the run

- Data: spice-7.9.a-v7 and spice-7.8.1.c-v26, both paths arguments. Ground truth read first: `npc.location.universal.invisible_npc` has 216 rows in `conversation_npcs`; `itm.mat.craft.endgame.stabilizer.tier2` has 703 in `schematic_materials` and is the largest hub.
- Paging is not in #151, so T2 checks only read() and order. Sharing by shape landed, so T2 opens one shared relay and a second shape with the same definition, and compares them.
- A cycle: `quest_chain` holds 5 two-node cycles (10 rows), each across two `link_type`s (`guid_ref` and `npc_giver`), none within one. So `next_in_arc` here holds all 3,304 rows, as T1 was loaded, and the walk is 50 steps of it at cap 1000, as kessel's walk had 50 hops.
- T3's real diff has 2,580 new rows and 0 gone (v26 holds 724). To exercise "ends for links that left", the 7.9 commit also ends one v26 link on the relay's neighbourhood, and the SQL diff drops the same one. RESULTS says so. Each build's date on the axis in ms and as valid time is the date its file was built: 2026-06-02 and 2026-06-18.
- Recorder stand-ins (#151): the planet relay roots on a `planet` node keyed `hutta` from the fqns' own location segment, not `planet:<area>`. Line to overlay runs from `accept_from` to the change-event callback, with 4 other workers draining, at the default 20 ms heartbeat, so it measures the relay and not warden's parser or overlay.
- The planet shape: every object with a location in its fqn (86,784 of them) is a `thing` with `kind` and an indexed `logname` (string id << 32, kessel's seed for warden's toy). Steps: `on<-` dropping `kind` Spawn, then `targets` (quest to objective target, from `quest_objectives`). Hutta takes about 860 at the first step. Spawns are in the graph and filtered, so the keep/drop path runs on real data.
- Applying an event: the time from the commit's subscriber call to read()'s cursor passing it, one link per commit, on the Hutta relay: the wake, the fold, the walk over about 1,000 nodes and the publish. I expect under 1 ms at p50, and p99 to carry a file save (at most one a second).
- The memory a capped step costs at the largest hub (#151): the process's resident size before and after seeding the 703-schematic relay, and its file's bytes. No prediction was made for this; it is reported.

## Changes from issue #151, 2026-11-03, before this rerun

The apps' predictions above stay as written. The operator changed two measures:

- The importer's round trip is judged at the median: under 40 ms. p95 and p99 are stated. Expected from the earlier runs: p50 about 28 ms, p95 about 50 ms, p99 about 85 ms.
- Applying an event is the fold plus the walk, under 1 ms at the median, on the Hutta relay, with one link per commit. Expected from the walk measurement (WALKS-RESULTS.md): p50 about 0.4 to 0.5 ms, on any run, no longer split by core placement as much, since the wake is out of the measure. If it still splits by run, it is the thread and not the wake.
