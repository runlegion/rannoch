# Your app's own crates

An app on rannoch keeps its work in two kinds of crate of its own: processor crates, which hold the actions its flows call, and an ontology crate, which holds its tables. Each is an ordinary crate. The app's build checks its flows against every processor crate's signatures, and gathers every crate's migrations, rannoch's and its own, into the one schema its store opens.

Both paths are proven by fixtures in this repo, each built and run by a test:

- `crates/rannoch-processor/tests/app`: a processor crate, `proc-text`, and an app that builds against it (`the_apps_build_gathers_the_signatures_a_separate_processor_crate_exports`).
- The same workspace's `onto-pay`, an ontology crate, and `pay-app`, an app whose store holds onto-pay's table and whose flow writes a row into it (`an_apps_ontology_crate_holds_the_table_its_store_step_writes`).

The app takes rannoch's crates from its git tag, one line per crate it uses:

```toml
rannoch-processor = { git = "https://github.com/runlegion/rannoch", tag = "v0.1.0" }
```

## A processor crate

### Its actions

An action is a type that implements `rannoch_processor::Action`:

- `sig()` returns its `Sig`: the name a step's `do` calls it by (`action`), the `options` a step's `with` gives it, the fields it `takes` and `gives`, each with its type, and whether it is a `job`, work long enough to run on a thread of its own.
- `run(options, input)` returns the action's output fields, or `Err` with a message. An error fails the step: the processor sends the event's -1 with the message as its data, and the queue retries the event while its chain has attempts left. An output that lacks a field its `gives` names, or holds it as another type, fails the step too.

An action cannot give a `store` row: only the built-in `store` step can, and a step whose action gives one fails. Any other item in its output's `$graph` field gets the event a -1 instead of its 2, its data `graph: Events writes no graph rows`.

### Its exports

The crate exports its signatures as data, a JSON array of `Sig`s, and its actions:

```rust
use std::sync::Arc;
use rannoch_processor::{Action, Sig};

/// Every action's signature; the app's build gathers it.
pub const SIGNATURES: &str = include_str!("../signatures.json");

/// The crate's actions, one per signature.
pub fn actions() -> Vec<Arc<dyn Action>> { /* ... */ }
```

```json
[
  { "action": "trim", "takes": { "text": "string" }, "gives": { "text": "string" } },
  { "action": "greet", "options": { "salutation": "string" }, "takes": { "text": "string" }, "gives": { "greeting": "string" } },
  { "action": "index", "takes": { "text": "string" }, "job": true }
]
```

A field a `Sig` does not define is refused.

### The app's build

The app build-depends on `rannoch-processor` and on each processor crate, and its `build.rs` hands `compile` the flow docs' directory and every crate's signatures, its own included:

```rust
use std::path::Path;

fn main() {
    let own = include_str!("signatures.json");
    rannoch_processor::compile(Path::new("flows"), &[proc_text::SIGNATURES, own]);
}
```

`compile` checks every flow doc against those signatures and rannoch's built-in `emit` and `store`, and writes the flows to `$OUT_DIR/flows.json`. A doc that calls an action nothing declares, or takes a field that nothing before it gives or that comes as another type, fails the build, listing every error. When a processor crate changes what an action gives, the app's next build names each flow that relied on it.

At run time the app compiles the flows in and hands `Processor::new` every crate's actions:

```rust
const FLOWS: &str = include_str!(concat!(env!("OUT_DIR"), "/flows.json"));

let processor = Processor::new(FLOWS, proc_text::actions(), conn)?;
```

An app action named `emit` or `store` replaces the built-in step of that name.

### rannoch's own step crates

`rannoch-processor-agent`, `rannoch-processor-onnx` and `rannoch-processor-retrieve` take the other route: the action is generic, and the app supplies what it reaches. The app declares the `Sig` in its own signatures and wraps its implementation, as `Retrieve::new(sig, Arc::new(source))` does with a `Source`. Each crate's docs give its example.

## An ontology crate

An app's ontology is its schema: a class is a table, a property a column, a relation a foreign key, and the migrations are its history. The ontology crate keeps those tables in its own migratr migrations directory and says where it is, as rannoch-events does:

```rust
/// Where this crate's migratr migrations directory is, for the app's `build.rs`.
pub const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
```

```json
{"up": [{"op": "create_table", "table": "payments", "columns": [
  {"name": "order_id", "type": "TEXT", "primary_key": 1},
  {"name": "amount", "type": "INTEGER"}]}]}
```

### The app's build

The app's store holds one schema, rannoch's tables and the app's together, so the app's `build.rs` gathers every crate's migrations into the app's own `migrations/` directory, which the app commits as it commits `Cargo.lock`. It build-depends on each crate with tables: rannoch-events and its ontology crate.

```rust
/// The migrations directory of every crate with tables.
const CRATES: &[&str] = &[rannoch_events::MIGRATIONS_DIR, onto_pay::MIGRATIONS_DIR];
```

For each directory it prints `cargo:rerun-if-changed`, and copies each migration into `migrations/`, writing a file only when its bytes differ and removing one that no crate has any more. It then applies them in memory with migratr's `load_dir` and `up`, and writes `schema.json` beside them with `write_schema`, again only when its bytes differ. `pay-app/build.rs` does all of it; rannoch-core's `build.rs` does the same for rannoch's own crates.

### The app's store

The app embeds the gathered directory, and uses that migrator wherever the README's "Starting rannoch in an app" uses `rannoch_core::migrator()`: for `Store::new` and for every `rannoch_store::open`.

```rust
fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}
```

### Writing its rows

A flow writes a row into one of the ontology crate's tables with the built-in `store` step, whose options name the table and the row:

```json
{"name": "payment", "ty": 1, "takes": {"order": "string", "amount": "number"},
  "steps": [{"do": "store", "with": {"store": {"table": "payments",
    "row": {"order_id": "$order", "amount": "$amount"}}}}]}
```

The row is written in the same transaction as the event's 2, under `Events::new(conn, signal).with_writes(processor.writes())`. It is inserted with `ON CONFLICT DO NOTHING`, so a row whose primary or unique key the table already holds adds nothing, and an event run again adds no second row to a table keyed that way. A step whose table or column the store does not have fails, and a row the table refuses is rolled back with the event's 2; either way the event gets a -1 naming the reason, as the README's "How it fails" says.

### What the ontology crate owns

What its tables mean, and the reads its app needs over them, such as the trees a UI shows. rannoch writes the rows a flow gives. rannoch-graph has no tables: its steps, `as_of`, `why`, `walk`, `edges` and the scoring tools `katz`, `pagerank`, `shared` and `learned`, read the ontology crate's tables when a read event's flow calls them, each table and column named in the step's options.
