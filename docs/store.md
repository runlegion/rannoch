# The store

`rannoch-store` is one graph in one SQLite file. Every change is an event in its log; the tables are built from the log. The log only grows, apart from [purge](#purge), which is rare and deliberate; an app that wants raw data gone routinely keeps it outside the store. This page follows `crates/rannoch-store/examples/store.rs`, which runs:

```sh
cargo run -p rannoch-store --example store
```

Most apps write through the queue ([inbox.md](inbox.md)), not `Graph::commit`. This page uses `commit` and `fill` directly to show what the store does with each event.

The snippets call four helpers that build event bodies, `declared`, `node`, `wrote` and `amended`, plus `id`; they are defined at the top of `crates/rannoch-store/examples/store.rs`. `node` is shown below.

**With a queue open,** the queue owns the `Graph` it was opened with (`Queue::open` takes it by value). Everything on this page that is not a write, `find` and the other reads, `stopped`, `retry`, `set_try_limit` and `copy_to`, is reached through a second handle: `rannoch_store::open(&path, &key)`. Do not call `commit` or `fill` on the second handle while the queue runs; the queue does both.

## Open

```rust
let key = [7u8; 32];
let mut g = rannoch_store::open(dir.join("shop.rannoch"), &key)?;
```

A path with no graph starts an empty one. The 32-byte key hashes every hearing's content identity; rannoch holds it in memory only. Keep it with the app: opening the file under another key returns `Error::KeyDiffers`.

Each open handle keeps up to 64 MB of the file's pages in memory as its cache, and a copy made with `copy_to` holds one more while it reads the copy back. A queue and a second handle for reads are two handles: up to 128 MB.

## Declare the ontology

An app declares its own classes and relations; rannoch knows no domain. Each declaration is one event, with its own CloudEvent as the body.

```rust
g.declare(&Declaration::Class(Class::new("author", ["title"])), &declared("author"))?;
g.declare(&Declaration::Class(Class::new("book", ["title"])), &declared("book"))?;
g.declare(
    &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
    &declared("wrote"),
)?;
```

- `Class::new` gives every property the type text. Set `properties` to use `integer`, `decimal` (exact digits), `float` or `float_vector(n)`.
- A class or relation may declare an axis: `axis` is an ordered list of part names, such as `["build"]` or `["build", "step"]`. Each part is an integer (a time is a part in milliseconds). Every event of it carries its point as `data.axis`, a value per part; a missing part, or a part not declared, is refused. With no axis declared, its history is ordered by each fact's known-at. See [Three times](#three-times).
- A class may index properties: `indexed` names the properties its nodes can be listed by. Each one gets an index when the class is declared; a class that indexes nothing adds none, so a write pays for an index only where one is declared.
- `Declaration::Property` adds a property to a declared class. Older nodes read without it; nothing is default-filled.
- `Relation::new(name, from, to, containment)`: the last argument says the source is part of the target. Set `axis` to declare one.
- `Declaration::Shape` names a neighbourhood a relay keeps warm: see [relays.md](relays.md).
- An identical declaration writes nothing and returns `Ok(None)`. A conflicting one is refused with a reason in ontology terms.

Declare before opening a queue on the graph: the queue checks events against the ontology it took when it opened.

## What an event looks like

Every event is a CloudEvents 1.0 JSON object. rannoch reads the `rannoch.*` types; any other type is the app's own, logged and handed to workers but filled into no table.

| Type | Subject | `data` |
|---|---|---|
| `rannoch.node.<class>` | the node's natural key | `{"properties": {...}, "valid"?: ..., "axis"?: {...}}` |
| `rannoch.link.<relation>` | (any) | `{"from": {"class", "key"}, "to": {"class", "key"}, "valid"?: ..., "axis"?: {...}}` |
| `rannoch.ended.<relation>` | (any) | `{"from", "to", "valid"?: ..., "axis"?: {...}}`: the link stopped holding |
| `rannoch.corrected.<relation>` | (any) | `{"from", "to"}`: the link was wrong, and does not hold from this event's known-at on |
| `rannoch.amended` | (any) | `{"target": {"source", "id"}, "patch": {...}}`: a JSON Merge Patch of the target |
| `rannoch.declared.<name>` | (any) | none; made with `declare`, never accepted |
| `rannoch.purged` | (none) | `{"rule", "kind", "match", "also"}`: see [Purge](#purge) |
| `rannoch.query` | (any) | a read and its view, or a shape and its view: see [Read](#read) and [relays.md](relays.md) |

Fill and the relays write the rest of rannoch's types, and every worker is handed them: `rannoch.closed.<relation>` when an arriving node closes a waiting link, `rannoch.refilled` when an event succeeds on retry, `rannoch.answer` for a query a relay answered, and `rannoch.report` from `Relays::report`. None of them fills a table.

- `id` is `<uuid>:<fact>`: a `UUIDv7` for the origin's event, then which fact within it. The millisecond in the uuid is the event's known-at: when it became known. `format!("{}:0", EventId::now())` makes one.
- `axis` is the event's point on its class's or relation's declared axis, `{"<part>": n, ...}`; left out when none is declared.
- `source` names who wrote it. A source that starts with `rannoch:` is read back percent-decoded without the prefix; any other source (such as `importer/<run>`) is stored exactly as given.
- `valid` is when the fact was true: `"unknown"`, or `{"on": "2026-10"}` or `{"on": "2026-10-31"}`. Left out, it is unknown. Unknown is never read as now.
- Property values: text is a bare string; other types are tagged: `{"integer": 5}`, `{"decimal": "1.20"}`, `{"float": 1.5}`, `{"float_vector": [0.1, 0.2]}`.

```rust
fn node(class: &str, key: &str, title: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.node.{class}","subject":"{key}","data":{{"properties":{{"title":"{title}"}},"valid":"unknown"}}}}"#,
        id()
    )
}
```

## Commit and fill

```rust
let book = node("book", "b1", "Teh Book");
g.commit(
    &[&node("author", "a1", "Ann"), &book, &wrote("a1", "b1")],
    &[],
)?;
g.fill()?;
// An amendment names its target by the target's own CloudEvent source and id.
let b1: serde_json::Value = serde_json::from_str(&book)?;
let b1 = (
    b1["source"].as_str().unwrap_or_default(),
    b1["id"].as_str().unwrap_or_default(),
);
```

`commit` appends a batch in one transaction; each body that fails rolls back alone. It does not check bodies against the ontology: the queue does that when it accepts them. Each result is the event as logged (`Event { id, cursor, body }`) or why it did not land. `Event.id` is this store's receipt stamp: it orders the log and is never read as known-at.

`fill` builds the tables from every committed event after its place. It commits in batches, so a crash loses at most the batch in flight: the first batch commits after about 25 ms of work (`FILL_BUDGET`), each next one runs four times longer, up to 1 s (`FILL_BUDGET_MAX`). A batch runs under one savepoint; when an event in it fails, the batch is filled again one event at a time, so the failure stays alone and the rest commits.

A hearing's identity is its content hashed under the store's key, so identical content is one hearing: the same content arriving again under another id is held as an alias of it. A link that ends records its ending at its own point; a live hearing at a later point reopens it. A link to a node that has not arrived waits, and lands when the node does.

## Three times

Every fact has three times, each with one job:

- **Known-at**, when it became known: the millisecond in its CloudEvent id. Known-at trusts the clock of the host that minted the event; rannoch does not correct it.
- **Its axis point**, the key its class or relation declares, carried in `data.axis`. With no axis declared, the axis is known-at.
- **Its valid time**, what it claims about the world: optional, and possibly unknown.

Declare an axis when facts arrive in an order of your own, such as builds an importer reads, and a fix to a build must read as that build, known from when it was fixed. A correction is a new event at the same axis point: read as known before it, the point reads as the original; from its known-at on, as the correction. That holds in every class. In a class with no axis declared, every event lands at its own known-at, so a correction there is an amendment, which reads the same way.

**Axis parts must refine each other in order.** In `(build, step)`, a step orders only within its build. Counters that move independently of each other are not an axis: make them properties.

**The axis is not for everything that has an order.** Branches, candidate runs, removal, and reads across two points are the app's ontology or a worker's job, not the axis's. To try a candidate run, a worker writes it to a copy of the store (`copy_to`), not to another axis point.

## Read

Reads take a `View { axis, known }`: an optional axis point and an optional known-at (`SystemTime`), each defaulting to now (`View::default()`). A point with fewer parts than the axis reads the top of its prefix: on `(build, step)`, `axis: Some(vec![12])` reads build 12 at its last step. On an undeclared axis, a point is one part, a known-at in milliseconds. No read with a known-at needs a valid date.

| Call | Answers |
|---|---|
| `find(class, key)` | the node as it is now: its newest version on the class's axis, never just the last to arrive |
| `state_at(node, view)` | the node's version in the view: the newest at or before its point, as known at its known-at |
| `find_as_of(class, key, date, view)` | the version valid on `date`, among those the view sees |
| `hearings(node)` | every account of the node, each with its source, axis point, known-at, valid time and identity |
| `seen(node)` | first and last heard, first recorded, first true, and how many hearings |
| `link_state_at(rel, from, to, view)` | the link's version in the view, and whether it does not hold there (`ended`: it ended or was found wrong); an amendment or rename known later is not in it |
| `link_hearings`, `link_seen`, `link_end` | as for nodes; `link_end` says whether it ended or was found wrong |
| `links(node, rel, direction, view)` | the links of `rel` from the node (`Direction::Out`) or to it that hold in the view (a version, not ended), each as the other end's key and the link's version, in the other end's key order |
| `nodes(class, filter, view)` | the class's nodes in the view, each as its key and version, in key order; `filter` maps indexed properties to values |
| `counts(view)` | how many nodes each class has and how many links hold in each relation, in the view |
| `describe(node, view)` | the node's version and each relation it has links in, by name and direction, with how many hold |
| `answer(query)` | the `rannoch.answer` event body for a `rannoch.query` event body: see below |
| `events().since(cursor)`, `at(cursor)`, `committed()` | the log itself |

None of these reads drops a version whose valid time is unknown: it is returned with `ValidTime::Unknown`.

A read can be asked as an event. A `rannoch.query` event's data is one read and its view: `{"links": {"node": {"class", "key"}, "relation", "direction": "out" | "in"}}`, `{"nodes": {"class", "filter": {...}}}`, `{"counts": {}}` or `{"describe": {"node": {...}}}`, with `"view": {"axis": [...], "known": <ms>}`, each part optional. `answer` returns a `rannoch.answer` event whose subject is the query's id and whose data is `{"result", "known"}`: the read's result and the known-at it was read at. A view with no known-at is read at the highest known-at ever filled, which reads all the store knows, so asking again with that known-at gives the same answer on the same log. Fill keeps it as a high-water mark: after an amendment filled later makes an earlier one redundant, it can exceed the greatest known-at the store's facts hold now. The state read is the same either way; only the answer's `known` differs. Both events can be logged and filled like any other; they fill no table.

A version that arrives late answers at its own point; it does not change what is current unless it is newer on the axis. A link's latest conclusion (ending or correction) is chosen by known-at, then event id, never by arrival. A link found wrong does not hold in any view from the correction's known-at on, across the whole axis, even when a later conclusion replaces it as the latest. A live hearing reopens it only when it is the newest occurrence in the view and its event id sorts after the correction's: a later build holds from that build, an earlier build stays not holding, a re-send at the same build reopens that build, and a late hearing at an earlier build holds at that build only. `link_state_at` returns such a link as a version with `ended: true`: `ended` on a version means the link does not hold, whether it ended or was found wrong. `link_end` returns `Corrected`, and none once reopened. Reading the same log in any order, or after a rebuild, gives the same answers; only `Node.recorded` and `Seen.first_recorded`, which name this store's own receipt, may differ.

```rust
let book = g.find("book", "b1")?.ok_or("b1 is there")?;
assert_eq!(g.hearings(&NodeRef::new("book", "b1"))?.len(), 1);
```

## Amend

**Amend is rare.** It corrects a mistake: an event that was written wrong. It is not how an app records change. A value that changes is a new event, a new hearing at its own point, and the store's history keeps both. An app that amends routinely is modelling change the wrong way.

An amendment corrects what an event says without changing the event. It is an event of type `rannoch.amended` whose data names its target by the target's own CloudEvent source and id, and holds a JSON Merge Patch (RFC 7386) of it: a value replaces, `null` removes, an omitted field stays.

```json
{"target": {"source": "rannoch:shop", "id": "<uuid>:0"}, "patch": {"data": {"properties": {"title": "The Book"}}}}
```

```rust
g.commit(&[&amended(b1, r#"{"data":{"properties":{"title":"The Book"}}}"#)], &[])?;
g.commit(&[&amended(b1, r#"{"subject":"b9"}"#)], &[])?;
g.fill()?;
```

- Each store resolves the target when it fills the amendment, so a store replaying another's log applies the same amendment. An amendment that arrives before its target waits and applies when the target fills, so the target's arrival order does not change the answer.
- The store refuses no amendment for its timing. Amendments of one event stack by known-at, then event id, whatever order they arrive in; one known before its target counts from the target's own known-at.
- Amendments are allowed in every class. A patch of `data.axis` moves the version to the patched point.
- The amended event stays in the log as written. Read as known before the amendment, it reads as written; from the amendment's known-at on, it reads patched.
- An amendment is known at the clock in its own CloudEvent id.
- **A rename is a patch of the subject.** It maps the old key to the new one from the amendment's known-at. The node and its links (live or ended) answer under the new key; a link or ending that arrives later naming the old key lands on the new one; a second rename maps on from the new key. Read as known before the rename, everything answers under the old key. Under the new key, `link_end` and `Version.known` give a copied ending the rename's known-at, as they do a link's hearing heard again there; the ending keeps its own known-at under the old key.

Amendments are the slow path: finding the target reads the whole log. One amendment's fill took about 23 ms on a 22,619-event store (release build, this machine's SSD). Events that are not amendments pay nothing for them while a store has no renames and no amendment waits.

### Refused amendments

Fill refuses only an amendment that cannot be stored at all, one that:

- is malformed: it names no target or has no patch, or its patch leaves the event invalid against the ontology;
- amends an event with no hearing of its own: a declaration, an app's own event, or content identical to an earlier hearing.

An amendment whose target is not in the log is not refused: it waits.

A refused amendment never stops fill. It goes through fill retries, below.

## Fill retries

Any event fill cannot apply is closed as failed with its reason, its try count goes up by one, and fill carries on with the events after it. Each later fill tries it again, until the try limit.

```rust
for _ in 0..3 {
    g.fill()?;
}
let stopped = g.stopped()?;
```

- The limit is 3 unless `set_try_limit(n)` sets another.
- `stopped()` lists the events that reached the limit as `(cursor, last reason)` pairs.
- `retry(cursor)` resets one event's tries, so the next fill tries it again.
- An event that succeeds on retry reads as if it had filled the first time, and fill appends a `rannoch.refilled` event naming its place, which every worker is handed.
- Fill counts its tries on its inbox row, as the worker `rannoch.fill` (`FILL`). Do not name a worker of your own `rannoch.fill`. The event in the log is never changed.

Every failure counts as a try, including a storage fault inside one event's fill.

## Copy

```rust
let at = g.copy_to(dir.join("shop-copy.rannoch"))?;
```

`copy_to` writes a copy of the store at one committed point and returns the cursor the copy holds. It takes no write lock, so writers carry on, and it records nothing in the store: a copy is outside the store, like a backup. It refuses a path that exists and leaves no file when it fails. The copy is not flushed durably to disk; a snapshot tool that does that is issue #154.

## Purge

**Purge is erasure: rare and selective.** It is not how an app records change, or how it corrects a mistake (that is an amendment).

```rust
g.purge(&Target::Node(NodeRef::new("book", "b1")), "erasure-2026-114", "privacy-desk")?;
```

`purge(target, rule, source)` runs in one immediate transaction under the connection's `secure_delete`, then truncates the write-ahead log. It first fills, so every part and rename the log holds is known. It reads the whole log, so what is not yet filled, or failed fill, goes too. It returns the purge event. A purge that matches nothing still writes one, and its rule stands.

| `Target` | What leaves |
|---|---|
| `Fact { identity }` | one fact, every event it arrived under. An amended version's identity (from `hearings`) names the event it was heard from: that event and every amendment of it |
| `Stream { source }` | every event with this source: its facts, an app's own events, its amendments |
| `Node(node)` | the node under every key a rename connects it to, every node part of it by a containment link that holds now, its node arrived or not, and all their links. A former part, whose link ended or was found wrong, survives with no link to the whole, and is purged by its own rule |
| `Link { relation, from, to }` | the link's hearings, endings and corrections, under every key its ends went by |
| `NodeWord { node, source }` | that source's hearings of the node |
| `LinkWord { relation, from, to, source }` | that source's hearings, endings and corrections of the link |
| `Source { source }` | that source's hearings, endings and corrections of every node and link. Unlike `Stream`, it leaves the source's app events and amendments |

A `source` is compared as `hearings` reports it, so `rannoch:shop` and `shop` are one source.

- **What leaves with it.** Each matched event's body, its hearings and aliases, what is kept of it in the tables, the failures and in-progress rows workers had for it, and the `rannoch.closed` and `rannoch.refilled` events that named it. Every amendment naming a purged event goes too, wherever it waits. A place in the log is never reused, so cursors stay true and the log has a gap.
- **What stays right.** A node or link left with other hearings reads as its newest left. A link whose node has no hearing left waits open. A latest conclusion that left gives way to the one before it. When a rename's amendment leaves but the node stays (`Stream` of the amendment's source), the rename is undone: the node and its links read under the old key.
- **The purge event** records its rule and kind plainly and each target only as a keyed match value under the graph's key, so it names what it matched and holds none of it. `also` holds the same for what else the rule stands against: each part, each other key, each event that left.
- **The rule stands.** Once a store holds a rule, `commit` checks each body against the rules. A match is not stored and answers `Error::Purged { rule }`: a hearing, an ending, a correction, an amendment naming a purged event, or an amendment whose patched event would match, such as a rename onto a purged key. A producer's position moves past such a body and its later bodies still land, since sending it again can only meet the same rule. A store with no rule pays one read per commit. Once one stands, every body committed and filled is hashed into its match values and looked up, and each amendment reads the log to find its target's body at commit as well as in fill.
- **A rule travels in the log.** A store that commits or fills a `rannoch.purged` event takes its rules, under the same key: a match later in the same batch is refused, and a match in the log that fill has not reached leaves when fill reaches it, though only a purge truncates the write-ahead log. So a store replaying a purged log holds the same rules, and `copy_to` carries them in the file. Filling a rule event removes nothing a store already held.
- **A reader still open** can keep the write-ahead log from truncating. The purge has committed, and `purge` answers `Error::Refused`; the log holds purged bytes until the next open or purge after the readers close.
- **A kill** leaves a purge either not committed, with nothing changed, or committed. A purge committed and not yet truncated is marked in its own transaction, and the next `open` truncates the log before it returns.

**With a queue open, purge through it:** `queue.purge(target, rule, source)`. It commits what is held first, so a match among it goes too, drops what workers read ahead, and hands each worker the rule event. A separate handle cannot reach what a queue has read ahead, and the queue's workers hear of that purge only at its next commit. A worker sees a purge only as its rule event. An event already handed out stays with its worker, whose close of it still counts; on `Graph::next`, a close of an event purged in progress is refused as not in progress.

**What it does not reach.** Copies outside the store (`copy_to`, replicas, backups) are the app's to purge. An app's own event that mentions a purged value leaves only with its stream, and that includes a `rannoch.query` or `rannoch.answer` an app chose to log. A shape's declaration that names a purged node keeps naming it, and producers' positions and kept worker state are the app's text. `secure_delete` zeroes what SQLite frees in the file; it says nothing of what the disk beneath it keeps.
