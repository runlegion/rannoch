//! The store on its own: open a graph, declare its ontology, commit `CloudEvents`, fill, read
//! (now, as known at a point, hearings, links), amend and rename, see a refused amendment stop,
//! and copy the store. `docs/store.md` walks through it.
//!
//! Run it with `cargo run -p rannoch-store --example store`.

use std::time::{Duration, SystemTime};

use rannoch_store::{Class, Declaration, EventId, NodeRef, Relation, Value, View, open};

/// A `CloudEvent` id: `<origin event uuid>:<fact within it>`. The uuid is a `UUIDv7`, and the
/// millisecond it carries is the event's known-at: when it became known.
fn id() -> String {
    format!("{}:0", EventId::now())
}

/// A declaration's own `CloudEvent`, which the store logs as the declaration's event.
fn declared(name: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.declared.{name}"}}"#,
        id()
    )
}

/// A node of `class` keyed `key`: type `rannoch.node.<class>`, subject the key.
fn node(class: &str, key: &str, title: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.node.{class}","subject":"{key}","data":{{"properties":{{"title":"{title}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

/// A link of the relation `wrote` from an author to a book.
fn wrote(author: &str, book: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:shop","type":"rannoch.link.wrote","data":{{"from":{{"class":"author","key":"{author}"}},"to":{{"class":"book","key":"{book}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

/// An amendment of the event whose `CloudEvent` source and id are `target`, its patch a JSON
/// Merge Patch of that event.
fn amended((source, target): (&str, &str), patch: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:fixer","type":"rannoch.amended","data":{{"target":{{"source":"{source}","id":"{target}"}},"patch":{patch}}}}}"#,
        id()
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("rannoch-store-example-{}", EventId::now()));
    std::fs::create_dir_all(&dir)?;

    // The key hashes every hearing's content identity. Keep it with the app; a store opened
    // under another key is refused.
    let key = [7u8; 32];
    let mut g = open(dir.join("shop.rannoch"), &key)?;

    // The ontology is declared on the stream: each declaration is one event.
    g.declare(
        &Declaration::Class(Class::new("author", ["title"])),
        &declared("author"),
    )?;
    g.declare(
        &Declaration::Class(Class::new("book", ["title"])),
        &declared("book"),
    )?;
    g.declare(
        &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
        &declared("wrote"),
    )?;

    // Commit appends events; fill builds the tables from the log. An app normally commits
    // through the queue (see docs/inbox.md), which checks each event against the ontology.
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

    let book = g.find("book", "b1")?.ok_or("b1 is there")?;
    assert_eq!(
        book.properties.get("title"),
        Some(&Value::Text("Teh Book".into()))
    );
    assert_eq!(g.hearings(&NodeRef::new("book", "b1"))?.len(), 1);
    assert!(g.link_seen("wrote", "a1", "b1")?.is_some());

    // A moment between the write and its amendments, to read "as known then".
    std::thread::sleep(Duration::from_millis(5));
    let before = View {
        axis: None,
        known: Some(SystemTime::now()),
    };
    std::thread::sleep(Duration::from_millis(5));

    // Amend the title, then rename the book from b1 to b9. The event stays as written; reads
    // apply the amendments from their points on.
    g.commit(
        &[&amended(
            b1,
            r#"{"data":{"properties":{"title":"The Book"}}}"#,
        )],
        &[],
    )?;
    g.commit(&[&amended(b1, r#"{"subject":"b9"}"#)], &[])?;
    g.fill()?;

    let renamed = g.find("book", "b9")?.ok_or("b9 is there")?;
    assert_eq!(
        renamed.properties.get("title"),
        Some(&Value::Text("The Book".into()))
    );
    assert!(g.find("book", "b1")?.is_none());
    // The link followed the rename.
    assert!(g.link_seen("wrote", "a1", "b9")?.is_some());
    // As known before the amendments, the link was a1 -> b1.
    assert!(g.link_state_at("wrote", "a1", "b1", &before)?.is_some());
    assert!(g.link_state_at("wrote", "a1", "b9", &before)?.is_none());

    // An amendment fill cannot apply never stops fill: it is tried up to the try limit (3) and
    // then stops, with its reason. This one names an app's own event, which makes no hearing, so
    // there is nothing to amend.
    let order = format!(
        r#"{{"specversion":"1.0","id":"{}","source":"shop","type":"shop.order"}}"#,
        id()
    );
    let order_id: serde_json::Value = serde_json::from_str(&order)?;
    let bad = amended(
        ("shop", order_id["id"].as_str().unwrap_or_default()),
        r#"{"data":{"x":1}}"#,
    );
    let bad = g.commit(&[&order, &bad], &[])?;
    let bad = bad[1].as_ref().map_err(ToString::to_string)?.cursor;
    for _ in 0..3 {
        g.fill()?;
    }
    let stopped = g.stopped()?;
    assert_eq!(stopped.len(), 1);
    assert_eq!(stopped[0].0, bad);
    println!("stopped: {}", stopped[0].1);

    // A copy of the store at one committed point, taken without a write lock.
    let at = g.copy_to(dir.join("shop-copy.rannoch"))?;
    println!("copied at {at:?}");

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
