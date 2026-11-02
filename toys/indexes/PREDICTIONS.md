# Predictions (written before any run)

Store: the toys/stats game-size shape (330k nodes, 411k links, fan-outs 1..120,000, 5,000 endings), with 0%, 10% and 50% of hearings re-sent under new ids (aliases). The base is toy/stats, which already has the #180 alias join rewrite (`CROSS JOIN _aliases`) and per-class stats. "Today" means this base.

P1. Plans. Every read that picks a version per key at a view (links, nodes, describe, state_at, link_state_at) runs a window over every version of the keys it touches, and fetches table rows for `known`/`until`/`body` because no index covers them. Counts fetch a row for every hearing it checks. (Confidence 0.7.)

P2. Answer default known-at. `KNOWN_NOW` scans every history table: 15-40 ms at 742k events, growing with the store. A newest-known-at value kept by fill per batch makes it under 0.05 ms, at no measurable fill cost. (0.8)

P3. A, current-version lookup for reads at now. The class and relation tables fill already keeps hold the current version, but relation rows also keep ended links, so a now read needs a "holding" test. Answering now reads from them (or a slim `(entity, line, key) -> current hearing` table kept per batch) makes state_at, find and link_state_at at now a single seek (< 0.05 ms), and whole links/nodes at now cost about 1 µs a row with no window. Fill cost: 0 if the existing tables suffice, 2-5% for a new slim table. (0.6)

P4. B, one index shaped for the self-join, `(entity, line, key, known)` (plus axis where declared). state_at and link_state_at at a past known-at become one backward seek (< 0.1 ms). Listing reads at a past view still choose a version per key, so they gain less: within 2x of their now cost. Aliases cannot join the same seek without moving alias rows into the hearing shape; as they are, they stay a second seek per key. Fill cost of the index: 1-3%. (0.5)

P5. describe on hubs. Counting links per relation grows with fan-out (215 ms at 120,000). Per-node link counts kept by fill per batch make describe sub-ms at every fan-out, at 1-2% fill. (0.6)

P6. Covering index for counts. Including `until` and `known` in the index counts use removes the per-hearing table fetch and cuts counts at a known-at by at least 2x. Fill cost 1-2%. With stats, counts at now already read 0.001 ms, so this matters only for past views. (0.5)

P7. Combined. All chosen changes together keep fill within 2% of today. (0.4: the per-batch values are cheap, but two new indexes may push it past 2%.)

P8. Dropping the window form. If A answers every now read and B every past point read, the window form is still needed for listing at a past view (links, nodes at a known-at), so it cannot be dropped entirely. (0.6)

P9 (Sean's prediction, 2026-11-02, his words relayed): "the proper indexes (the ActiveRecord patterns) give about another quarter": roughly 25% more fill throughput on top of the alias fix (#180, PR #181, which brings fill to about 39,000-41,000 events/s at 300k with aliases), and the common reads speed up with it. Fill's own lookups of a key's current version use the history SQL, so the backward-seek index may speed fill as well as the reads.

ActiveRecord patterns to test, mapped onto rannoch (added before the candidate runs):
- polymorphic belongs_to index `[item_type, item_id]` -> `(entity, line, key)`; with the history order column, `(entity, line, key, known)`.
- paper_trail `where(created_at <= t).order(created_at desc).first` -> a backward seek per key instead of a window over every version.
- has_many :through, one index per foreign key -> from end and to end of a link (`_hearings_at` leads with the from key; `_hearings_to` with the to key).
- counter_cache -> per-node link counts per relation and direction, kept by fill per batch, read by describe.
