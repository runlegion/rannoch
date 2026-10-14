# rannoch architecture

## What it is

rannoch is one graph for the Rust projects in our ecosystem that need a graph. It is a library and nothing else: no executable, no network, no person or agent uses it directly. An app links it and keeps its own graphs in it.

It is also the core of the events. Every change to a graph is an event, and the tables are state built from the events. A graph's shape is published the same way: classes, relations and properties are declared as events, and judgments are events. rannoch presents everything as an ontology.

The name: Rannoch, the geth home world in Mass Effect; it means walled garden.

## What it refuses

- It knows no domain. Each app declares its own classes and relations.
- It syncs nothing and opens no connection. Sync, sockets, publishing and subscribing across processes or machines belong to the apps.
- It depends on no other crate or app of ours. Owning the code makes them no different from any other dependency: they stay outside.
- It computes and stores no embeddings, and ranks nothing. Any weighting is an app's visible policy.
- Existing data comes in by hand; a graph starts from an empty file.

## The parts

1. **Events are the record.** Every change is an event in one order. An app reads the events after a cursor it holds. Each event names what it added and removed, by kind and endpoint. State rebuilt from the events alone equals live state, and replay to a cursor then on equals one replay.
2. **Writes are whole units, in pieces.** A unit is one node and its own links. A write commits in pieces of at most about 1,000 changes, one transaction each, and each committed piece is one event. A unit is never split; a unit larger than a piece commits as its own. An event is written in the same transaction as its write, and only if the write lands. A refused unit commits none of its node or links, the rest of the piece commits, and the write returns each refused unit with its reason (forgotten key, wrong kinds, missing valid time).
3. **The ontology is declared on the stream.** Declaring a class, a relation or a property is one event; rannoch projects it straight into a table, as a fact becomes a row. Each class is its own table, with an id, a unique natural key and a valid time. Each relation is its own table whose two ends reference their classes, so the database refuses a link between the wrong kinds. Replaying the events into an empty file rebuilds the schema and the data. A property added later reads as absent on older instances, never default-filled.
4. **Facts carry their source and two times.** Every node and link keeps when it was true (its valid time, which may be stated as unknown) apart from when rannoch recorded it. Reads can be asked as of a valid time; an unknown valid time is never read as now.
5. **Links can wait.** A link to a key that has not arrived waits as an open link, recording the kind it waits for. When a node of that kind arrives, the link moves into its relation's table, and that move is one closing event. A node of another kind under that key leaves the link open and is reported. The graph always says what is missing.
6. **Reads are capped walks and exact counts.** A walk follows named relations in either direction, to a number of hops, under a cap, and says when it stopped at the cap. A count is exact and uncapped: distinct, total and grouped in one pass, over many roots, ending on a class. A judgment is a step on the path, not a special case.
7. **Forgetting strips content.** Forgetting follows the app's rule. The forgotten node and its links leave live state; earlier events keep their ids, kinds and endpoints and lose their content, so numbers an app derived can subtract exactly what was removed. What was forgotten stays gone through every replay, and writing a forgotten key again is refused.
8. **References cross files.** A graph can refer to a node in another graph file. The reference resolves from the target file's own facts: live, forgotten (with its rule) or replaced (with what replaced it). The referring file never changes.
9. **Relays are warm collections of memory.** A relay is rooted at a set of nodes and held by the app's long-running process. It is seeded once, then pumped: an event reaches it only when what the event touched meets the relay's nodes, every link event touches both of its ends, and each event reaches a relay at most once. The app's fold applies each event idempotently. A relay presents as a sub-ontology: the classes among its nodes with counts equal to its own, and a relation only when one of its links has both ends in the relay.
10. **Compaction keeps state.** An app may compact a graph's events up to a cursor it picks; the state they built is kept as the base, and the base plus the later events gives the same state and counts.

## How a node reads

Asked about a node, rannoch answers in ontology terms: its class, its properties, its relations by name in both directions with counts, and what it is part of, through the relations an app declared as part-of. The answer names no table or column.

## Modules

One crate:

- `open`: opens a graph file and knows its classes and relations.
- `write`: units, pieces, refusals; owns typed refusals.
- `read`: walks, counts, describing a node in ontology terms, reading as of a valid time.
- `events`: the log, cursors, replay, forgetting, compaction; owns rannoch's own tables (events, the ontology, fates, open links).
- `refs`: references into other graph files.
- `relay`: relays and their sub-ontologies.

rannoch's own tables start with `_`; every other table is a class or a relation an app declared.

## What was measured

These numbers come from toys run on real data on a laptop, each rerun by a second agent.

- A fresh lookup (find by key, two-hop walk capped at 200) on a 27,000-node graph: p99 under 7 ms. Reads never wait on a writer.
- A small write while another writer commits 1,000-change pieces: under 70 ms. Pieces of 10,000 lost a race at over a second.
- An exact count at 100,000 voices: 8 MB peak for one count, 8.5 MB for all 1,000 findings, against 353 MB for loading the whole graph. On a cold disk one count took 3-6 s (0.3 s warm); rannoch sets no cold-disk target.
- A relay at a 13,198-voice finding reshapes per judgment in 14-123 us, about 1,000 times faster than recounting; on a 6,446-node relay, 145-241 us.
- Per-class tables count about 30% faster than one generic table; per-relation tables faster again.
- 200 references resolved across files in 0.63 ms.
