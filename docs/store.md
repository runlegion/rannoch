# The store

`rannoch-store` is one graph in one SQLite file. Every change is an event in its log; the tables are built from the log. This page follows `crates/rannoch-store/examples/store.rs`, which runs:

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
- A class's history is ordered by `HistoryKey::Origin` (the record time of the store that first recorded each event) unless it declares `Time` (the writer supplies `data.time`, milliseconds since the Unix epoch) or `Integer` (the writer supplies `data.integer`). A node of a `Time` or `Integer` class that carries none is refused.
- `Declaration::Property` adds a property to a declared class. Older nodes read without it; nothing is default-filled.
- `Relation::new(name, from, to, containment)`: the last argument says the source is part of the target.
- An identical declaration writes nothing and returns `Ok(None)`. A conflicting one is refused with a reason in ontology terms.

Declare before opening a queue on the graph: the queue checks events against the ontology it took when it opened.

## What an event looks like

Every event is a CloudEvents 1.0 JSON object. rannoch reads the `rannoch.*` types; any other type is the app's own, logged and handed to workers but filled into no table.

| Type | Subject | `data` |
|---|---|---|
| `rannoch.node.<class>` | the node's natural key | `{"properties": {...}, "valid": ..., "time": ms?, "integer": n?}` |
| `rannoch.link.<relation>` | (any) | `{"from": {"class", "key"}, "to": {"class", "key"}, "valid": ..., "time": ms?}` |
| `rannoch.ended.<relation>` | (any) | `{"from", "to", "at": ms, "valid": ...}`: the link stopped holding |
| `rannoch.corrected.<relation>` | (any) | `{"from", "to"}`: the link was wrong |
| `rannoch.amended` | the log id of the event it amends | a JSON Merge Patch of that event |
| `rannoch.declared.<name>` | (any) | none; made with `declare`, never accepted |

- `id` is `<uuid>:<fact>`: a `UUIDv7` for the origin's event, then which fact within it. The millisecond in the uuid is when the origin recorded the event, and it is that event's point unless the writer supplies `time`. `format!("{}:0", EventId::now())` makes one.
- `source` names who wrote it. A source that starts with `rannoch:` is read back percent-decoded without the prefix; any other source (such as `importer/<run>`) is stored exactly as given.
- `valid` is when the fact was true: `"unknown"`, or `{"on": "2026-10"}` or `{"on": "2026-10-31"}`. Unknown is never read as now.
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
let landed = g.commit(
    &[
        &node("author", "a1", "Ann"),
        &node("book", "b1", "Teh Book"),
        &wrote("a1", "b1"),
    ],
    &[],
)?;
let b1 = landed[1].as_ref().map_err(ToString::to_string)?.clone();
g.fill()?;
```

`commit` appends a batch in one transaction; each body that fails rolls back alone. It does not check bodies against the ontology: the queue does that when it accepts them. Each result is the event as logged (`Event { id, cursor, body }`) or why it did not land. Keep `Event.id` if you may amend the event later.

`fill` builds the tables from every committed event after its place. It commits in batches, so a crash loses at most the batch in flight: the first batch commits after about 25 ms of work (`FILL_BUDGET`), each next one runs four times longer, up to 1 s (`FILL_BUDGET_MAX`). A batch runs under one savepoint; when an event in it fails, the batch is filled again one event at a time, so the failure stays alone and the rest commits.

A hearing's identity is its content hashed under the store's key, so identical content is one hearing: the same content arriving again under another id is held as an alias of it. A link that ends records its ending at its own point; a live hearing at a later point reopens it. A link to a node that has not arrived waits, and lands when the node does.

## Read

| Call | Answers |
|---|---|
| `find(class, key)` | the node as it is now: its newest version on the class's axis, never just the last to arrive |
| `find_as_of(class, key, date, known_at)` | the version valid on `date`, as the store knows it now or, given `known_at`, as it knew it then |
| `state_at(node, at)` | the version that held at a point on the axis |
| `hearings(node)` | every account of the node, each with its source, point, valid time and identity |
| `seen(node)` | first and last heard, first recorded, first true, and how many hearings |
| `link_state_at(rel, from, to, at)` | the link's version at a point, and whether it had ended |
| `link_known_at(rel, from, to, known)` | the link as known at `known`: an amendment or rename made later is not in it |
| `link_hearings`, `link_seen`, `link_end` | as for nodes; `link_end` says whether it ended or was found wrong |
| `events().since(cursor)`, `at(cursor)`, `committed()` | the log itself |

`At` is `At::Time(SystemTime)`, or `At::Key(i64)` in an integer-keyed class. A version that arrives late answers at its own point; it does not change what is current unless it is newer on the axis.

```rust
let book = g.find("book", "b1")?.ok_or("b1 is there")?;
assert_eq!(g.hearings(&NodeRef::new("book", "b1"))?.len(), 1);
```

## Amend

**Amend is rare.** It corrects a mistake: an event that was written wrong. It is not how an app records change. A value that changes is a new event, a new hearing at its own point, and the store's history keeps both. An app that amends routinely is modelling change the wrong way.

An amendment corrects what an event says without changing the event. It is an event of type `rannoch.amended` whose subject is the log id (`Event.id`) of the event it amends, not that event's CloudEvent id, and whose data is a JSON Merge Patch (RFC 7386) of the event: a value replaces, `null` removes, an omitted field stays.

```rust
g.commit(&[&amended(b1.id, r#"{"data":{"properties":{"title":"The Book"}}}"#)], &[])?;
g.commit(&[&amended(b1.id, r#"{"subject":"b9"}"#)], &[])?;
g.fill()?;
```

- The amended event stays in the log as written. Read as known before the amendment, it reads as written; from the amendment's point on, it reads patched.
- An amendment's point is the record time in its own CloudEvent id. Amendments stack in point order; at the same point, by event id.
- **A rename is a patch of the subject.** It maps the old key to the new one from the amendment's point. The node and its links (live or ended) answer under the new key; a link or ending that arrives later naming the old key lands on the new one; a second rename maps on from the new key. Read as known before the rename, everything answers under the old key.
- An app finds a log id in `commit`'s result, in `events().since(...)`, or in the `Event` a worker takes. There is no lookup from a CloudEvent id to a log id; issue #164 adds one, not built, that returns the first log id an event arrived under. It is for tracking down the event a mistake came from, and nothing else.

Amendments are the slow path: about 53 ms each on a 22,619-event store, measured on main 7fe3df9. Events that are not amendments pay nothing for them while a store has no renames.

### Refused amendments

Fill refuses an amendment that:

- names an event that is not in the log;
- has a point before the event's record time, its own point (its writer's `time`, else its record time), or its latest amendment already applied;
- amends an event of an integer-keyed class;
- amends an event with no hearing of its own: a declaration, an app's own event, or content identical to an earlier hearing.

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
- Fill counts its tries on its inbox row, as the worker `rannoch.fill` (`FILL`). Do not name a worker of your own `rannoch.fill`. The event in the log is never changed.

Every failure counts as a try, including a storage fault inside one event's fill.

## Copy

```rust
let at = g.copy_to(dir.join("shop-copy.rannoch"))?;
```

`copy_to` writes a copy of the store at one committed point and returns the cursor the copy holds. It takes no write lock, so writers carry on, and it records nothing in the store: a copy is outside the store, like a backup. It refuses a path that exists and leaves no file when it fails. The copy is not flushed durably to disk; a snapshot tool that does that is issue #154.

## Not built yet

- Purge (erasure of a fact from the store, with a rule that refuses it coming back): #148.
- Trim (retention declared per class, such as dropping raw rows once processed): no issue yet.
- Relays: #151.
