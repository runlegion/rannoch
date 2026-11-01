//! The inbox: a producer accepts `CloudEvents` with its position in its source, the queue commits
//! them on its heartbeat, and two workers written in the app's own code each take every event,
//! close it as done or failed, and pick up after a restart. `docs/inbox.md` walks through it.
//!
//! Run it with `cargo run -p rannoch-queue --example worker`.

use std::path::Path;

use rannoch_queue::{Queue, State};
use rannoch_store::{Class, Declaration, Event, EventId, Graph, open};

const KEY: [u8; 32] = [7; 32];

fn id() -> String {
    format!("{}:0", EventId::now())
}

/// A log line from a source file: a node of class `line`, keyed by file and line number.
fn line(file: &str, number: u32, text: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:tailer","type":"rannoch.node.line","subject":"{file}:{number}","data":{{"properties":{{"text":"{text}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

/// Opens the graph and declares its ontology. Declare before opening the queue: the queue checks
/// events against the ontology it took when it opened.
fn graph(path: &Path) -> Result<Graph, rannoch_store::Error> {
    let mut g = open(path, &KEY)?;
    let body = format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:tailer","type":"rannoch.declared.line"}}"#,
        id()
    );
    g.declare(&Declaration::Class(Class::new("line", ["text"])), &body)?;
    Ok(g)
}

/// The app's own work for one event. The worker decides what it cares about: here, nodes of
/// class `line`; everything else (declarations, other apps' events) is done at once.
fn report(event: &Event) -> Result<(), String> {
    if !event.body.contains(r#""type":"rannoch.node.line""#) {
        return Ok(());
    }
    if event.body.contains("garbled") {
        return Err("the line does not parse".into());
    }
    println!("report on event {:?}", event.cursor);
    Ok(())
}

/// One worker's loop: take the next committed event, do the work, close it. Returns how many it
/// closed.
fn drain(queue: &Queue, worker: &str) -> Result<usize, rannoch_store::Error> {
    let mut closed = 0;
    while let Some(event) = queue.next(worker)? {
        match report(&event) {
            Ok(()) => queue.done(worker, event.cursor)?,
            Err(why) => queue.failed(worker, event.cursor, &why)?,
        }
        closed += 1;
    }
    Ok(closed)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("rannoch-queue-example-{}", EventId::now()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("logs.rannoch");

    let queue = Queue::open(graph(&path)?, Queue::INTERVAL)?;

    // A producer resumes from its last committed position after a crash, and sends again
    // exactly what was lost.
    let from = queue.position("tailer")?;
    assert_eq!(from, None, "a new producer has no position yet");
    let lines = [(1, "boot"), (2, "garbled"), (3, "ready")];
    for (number, text) in lines {
        queue.accept_from(line("app.log", number, text), "tailer", number.to_string())?;
    }
    // The heartbeat commits about every 20 ms; flush commits now and returns any failures.
    let (_, failures) = queue.flush();
    assert!(failures.is_empty());
    assert_eq!(queue.position("tailer")?, Some("3".to_string()));

    // Two workers, each with its own place: both see every committed event.
    let reports = drain(&queue, "reports")?;
    let index = drain(&queue, "index")?;
    println!("reports closed {reports}, index closed {index}");

    // The garbled line failed for `reports`, with its reason.
    let last = queue.processed()?;
    println!("processed mark: {last:?}");

    // Reads go through another handle on the same file.
    let reader = open(&path, &KEY)?;
    assert!(reader.find("line", "app.log:3")?.is_some());

    // A worker that leaves for good releases the processed mark it held.
    queue.remove_worker("index")?;

    let failures = queue.close();
    assert!(failures.is_empty());

    // After a restart, each worker carries on from its place; nothing is handed out twice.
    let queue = Queue::open(open(&path, &KEY)?, Queue::INTERVAL)?;
    assert_eq!(drain(&queue, "reports")?, 0);
    let garbled = reader
        .events()
        .since(rannoch_store::Cursor::START)?
        .into_iter()
        .find(|event| event.body.contains("garbled"))
        .ok_or("the garbled line is logged")?;
    assert!(matches!(
        queue.state("reports", garbled.cursor)?,
        State::Failed(_)
    ));
    let failures = queue.close();
    assert!(failures.is_empty());

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
