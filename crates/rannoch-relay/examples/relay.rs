//! A relay: declare a shape, ask for it with a `rannoch.query`, read its warm set in process, hear
//! its change events, freeze it in a report, see a query fail, restart, and let it go.
//! `docs/relays.md` walks through it.
//!
//! Run it with `cargo run -p rannoch-relay --example relay`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

use rannoch_queue::{Queue, State};
use rannoch_relay::{Relays, WarmSet};
use rannoch_store::{
    Class, Cursor, Declaration, EventId, NodeRef, Relation, Roots, Shape, Step, Value, open,
};
use serde_json::{Value as Json, json};

const KEY: [u8; 32] = [7; 32];
/// Who asks. A relay is held by the `source` of the query that asked for it.
const APP: &str = "shop";
/// The app's own worker, which reads the answers.
const ANSWERS: &str = "shop.answers";

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn declared(name: &str) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": APP,
        "type": format!("rannoch.declared.{name}"),
    })
    .to_string()
}

fn author(key: &str, title: &str) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": APP,
        "type": "rannoch.node.author", "subject": key,
        "data": {"properties": {"title": title}},
    })
    .to_string()
}

fn book(key: &str, title: &str, shelf: &str) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": APP,
        "type": "rannoch.node.book", "subject": key,
        "data": {"properties": {"title": title, "shelf": shelf}},
    })
    .to_string()
}

fn wrote(author: &str, book: &str) -> String {
    json!({
        "specversion": "1.0", "id": format!("{}:0", EventId::now()), "source": APP,
        "type": "rannoch.link.wrote",
        "data": {"from": {"class": "author", "key": author}, "to": {"class": "book", "key": book}},
    })
    .to_string()
}

/// The shape: from one author, up to two of the books they wrote that are not withdrawn, then
/// everyone who wrote those books.
fn colleagues() -> Shape {
    Shape {
        name: "colleagues".into(),
        roots: Roots::Node {
            node: NodeRef::new("author", "a1"),
        },
        steps: vec![
            Step {
                relation: "wrote".into(),
                drop: BTreeMap::from([("shelf".to_string(), Value::from("withdrawn"))]),
                cap: Some(2),
                ..Step::default()
            },
            Step {
                relation: "wrote<-".into(),
                ..Step::default()
            },
        ],
    }
}

/// Declares the ontology and the shape, as with any declaration before a queue opens.
fn declare(path: &Path) -> Result {
    let mut g = open(path, &KEY)?;
    g.declare(
        &Declaration::Class(Class::new("author", ["title"])),
        &declared("author"),
    )?;
    let mut books = Class::new("book", ["title", "shelf"]);
    books.indexed = BTreeSet::from(["shelf".to_string()]);
    g.declare(&Declaration::Class(books), &declared("book"))?;
    g.declare(
        &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
        &declared("wrote"),
    )?;
    g.declare(&Declaration::Shape(colleagues()), &declared("colleagues"))?;
    Ok(())
}

/// A `rannoch.query` for a shape in a view. `{}` is the view "now", which moves with the log.
fn query(shape: &str, view: &Json) -> String {
    json!({
        "specversion": "1.0", "id": EventId::now().to_string(), "source": APP,
        "type": "rannoch.query", "data": {"shape": shape, "view": view},
    })
    .to_string()
}

fn write(queue: &Queue, bodies: &[String]) -> Result {
    for body in bodies {
        queue.accept(body.clone())?;
    }
    let (_, failures) = queue.flush();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("{failures:?}").into())
    }
}

/// The last place committed in the log, through a second handle: the queue owns its graph.
fn committed(path: &Path) -> Result<Cursor> {
    Ok(open(path, &KEY)?.events().committed()?)
}

/// Waits for `found`, far past anything a loaded machine takes.
fn until<T>(mut found: impl FnMut() -> Result<Option<T>>) -> Result<T> {
    let hung = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(value) = found()? {
            return Ok(value);
        }
        if Instant::now() > hung {
            return Err("nothing came".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Takes events on the app's worker until `wanted` picks one out.
fn take<T>(queue: &Queue, wanted: impl Fn(Cursor, &Json) -> Option<T>) -> Result<T> {
    until(|| {
        while let Some(event) = queue.next(ANSWERS)? {
            queue.done(ANSWERS, event.cursor)?;
            let body: Json = serde_json::from_str(&event.body)?;
            if let Some(value) = wanted(event.cursor, &body) {
                return Ok(Some(value));
            }
        }
        Ok(None)
    })
}

/// Asks through the inbox, and returns the relay's id from the `rannoch.answer`, printing its
/// counts.
fn ask(queue: &Queue, shape: &str, view: &Json) -> Result<String> {
    let asked = query(shape, view);
    let id: Json = serde_json::from_str(&asked)?;
    write(queue, &[asked])?;
    let answer = take(queue, |_, body| {
        (body["type"] == "rannoch.answer" && body["subject"] == id["id"]).then(|| body.clone())
    })?;
    let result = &answer["data"]["result"];
    println!("answer: steps {}", result["steps"]);
    Ok(result["relay"].as_str().ok_or("a relay id")?.to_string())
}

/// The relay's warm set once it has folded everything committed now.
fn settled(relays: &Relays, id: &str, path: &Path) -> Result<WarmSet> {
    let high = committed(path)?;
    until(|| Ok(relays.read(id).filter(|set| set.cursor() >= high)))
}

/// The keys of each step, in the order the walk took them.
fn steps(set: &WarmSet) -> Vec<Vec<&str>> {
    set.steps()
        .iter()
        .map(|step| step.nodes.iter().map(|n| n.key.as_str()).collect())
        .collect()
}

/// The relays and their channel of change events, on a queue of their own.
fn start(path: &Path, dir: &Path) -> Result<(Arc<Queue>, Relays, Receiver<String>)> {
    let queue = Arc::new(Queue::open(open(path, &KEY)?, Queue::INTERVAL)?);
    // Joined at the watermark: this worker needs only what follows, the answers.
    let _ = queue.join(ANSWERS);
    let relays = Relays::open(dir, Arc::clone(&queue), path, &KEY)?;
    let (tell, changes) = channel();
    relays.subscribe(move |body: &str| {
        let _ = tell.send(body.to_string());
    });
    Ok((queue, relays, changes))
}

fn main() -> Result {
    let root = std::env::temp_dir().join(format!("rannoch-relay-example-{}", EventId::now()));
    std::fs::create_dir_all(&root)?;
    let path = root.join("shop.rannoch");
    let dir = root.join("relays");
    declare(&path)?;
    let (queue, relays, changes) = start(&path, &dir)?;

    write(
        &queue,
        &[
            author("a1", "Ann"),
            author("a2", "Bea"),
            book("b1", "First", "open"),
            book("b2", "Second", "open"),
            book("b3", "Third", "open"),
            book("b0", "Pulled", "withdrawn"),
            wrote("a1", "b0"),
            wrote("a1", "b1"),
            wrote("a1", "b2"),
            wrote("a1", "b3"),
            wrote("a2", "b1"),
        ],
    )?;

    // Ask. The answer holds the relay's id and its counts, not the nodes.
    let relay = ask(&queue, "colleagues", &json!({}))?;

    // Read. This never touches the graph.
    let set = relays.read(&relay).ok_or("the relay is there")?;
    assert_eq!(steps(&set), [vec!["b1", "b2"], vec!["a2"]]);
    assert_eq!(set.steps()[0].dropped, 1, "b3 is past the cap");
    assert_eq!(set.nodes().len(), 4, "the root, two books, one colleague");
    assert_eq!(set.by("shelf", &Value::from("open")).len(), 2);

    // A commit moves the set, and tells one change event: a book below the cut comes in, and the
    // book it pushes out goes.
    write(
        &queue,
        &[book("b00", "Earliest", "open"), wrote("a1", "b00")],
    )?;
    let change: Json = serde_json::from_str(&changes.recv_timeout(Duration::from_secs(60))?)?;
    assert_eq!(change["type"], "rannoch.relay.changed");
    assert_eq!(change["subject"], json!(relay));
    assert_eq!(change["data"]["entered"][0]["node"]["key"], "b00");
    assert_eq!(change["data"]["left"][0]["node"]["key"], "b2");
    println!("change: {}", change["data"]);
    let set = relays.read(&relay).ok_or("the relay is there")?;
    assert_eq!(steps(&set)[0], ["b00", "b1"]);

    // A report freezes the set as an event that names a place in the log. Reading it back replays
    // the log to there, so the commit after it does not show.
    let report = relays.report(&relay)?;
    write(
        &queue,
        &[book("b000", "Earlier still", "open"), wrote("a1", "b000")],
    )?;
    assert_eq!(steps(&settled(&relays, &relay, &path)?)[0], ["b00", "b000"]);
    assert_eq!(steps(&relays.reread(&report)?)[0], ["b00", "b1"]);

    // A query for a shape not declared is closed as failed on the relays' query worker, with its
    // reason. No answer comes.
    let asked = query("nothing", &json!({}));
    let id: Json = serde_json::from_str(&asked)?;
    write(&queue, &[asked])?;
    let at = take(&queue, |cursor, body| {
        (body["id"] == id["id"]).then_some(cursor)
    })?;
    let why = until(|| {
        Ok(match queue.state("rannoch.relay", at)? {
            State::Failed(why) => Some(why),
            _ => None,
        })
    })?;
    println!("refused: {why}");

    // What failed on the relays' thread waits here. Nothing has.
    assert!(relays.take_failures().is_empty());

    // Restart: close the relays before the queue. The relay comes back from its file under the
    // same id, and asking again finds it instead of making another.
    let failures = relays.close();
    assert!(failures.is_empty(), "{failures:?}");
    let (_, failed) = queue.flush();
    assert!(failed.is_empty(), "{failed:?}");
    drop(queue);
    drop(changes);
    let (queue, relays, _changes) = start(&path, &dir)?;
    assert_eq!(steps(&settled(&relays, &relay, &path)?)[0], ["b00", "b000"]);
    assert_eq!(ask(&queue, "colleagues", &json!({}))?, relay);

    // Drop: the last holder lets go, and the relay's worker and file go with it.
    relays.release(&relay, APP)?;
    assert!(relays.read(&relay).is_none());
    assert!(!dir.join(format!("{relay}.json")).exists());

    let failures = relays.close();
    assert!(failures.is_empty(), "{failures:?}");
    drop(queue);
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
