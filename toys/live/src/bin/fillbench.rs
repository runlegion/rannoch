//! A clean fill: 300,000 events (half nodes, half links) plus 2% re-sent, committed first, then
//! one timed fill. `fillbench <dir> [events]`; the store is written fresh in `<dir>`.
use std::time::Instant;

use rannoch_store::{Class, Declaration, EventId, Relation, open};

fn id() -> String {
    format!("{}:0", EventId::now())
}

fn declared(name: &str) -> String {
    format!(r#"{{"specversion":"1.0","id":"{}","source":"rannoch:live","type":"rannoch.declared.{name}"}}"#, id())
}

fn node(class: &str, key: &str, title: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:live","type":"rannoch.node.{class}","subject":"{key}","data":{{"properties":{{"title":"{title}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

fn wrote(author: &str, book: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:live","type":"rannoch.link.wrote","data":{{"from":{{"class":"author","key":"{author}"}},"to":{{"class":"book","key":"{book}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = std::path::Path::new(&args[1]).join("fill.rannoch");
    let events: usize = args.get(2).map_or(300_000, |e| e.parse().expect("events"));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let mut g = open(&path, &[7; 32]).expect("open");
    let mut book = Class::new("book", ["title"]);
    book.indexed.insert("title".into());
    g.declare(&Declaration::Class(Class::new("author", ["title"])), &declared("author")).expect("author");
    g.declare(&Declaration::Class(book), &declared("book")).expect("book");
    g.declare(&Declaration::Relation(Relation::new("wrote", "author", "book", false)), &declared("wrote")).expect("wrote");

    let half = events / 2;
    let (authors, books) = (half / 10, half - half / 10);
    let mut bodies = Vec::with_capacity(events + events / 50);
    bodies.extend((0..authors).map(|i| node("author", &format!("a{i}"), "x")));
    bodies.extend((0..books).map(|i| node("book", &format!("b{i}"), &format!("g{}", i % 50))));
    bodies.extend((0..events - half).map(|i| wrote(&format!("a{}", i % authors), &format!("b{}", (i * 7919) % books))));
    // A share of re-sent content: 2% of events, the same content under a new id.
    let resend: Vec<String> = bodies
        .iter()
        .step_by(50)
        .map(|b| {
            let old = b.split("\"id\":\"").nth(1).and_then(|s| s.split('"').next()).expect("id");
            b.replacen(old, &id(), 1)
        })
        .collect();
    bodies.extend(resend);

    for chunk in bodies.chunks(5_000) {
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        for landed in g.commit(&refs, &[]).expect("commit") {
            landed.expect("landed");
        }
    }
    let started = Instant::now();
    g.fill().expect("fill");
    let secs = started.elapsed().as_secs_f64();
    println!(
        "fill {} events in {secs:.3} s: {:.0} events/s, stopped {}",
        bodies.len(),
        bodies.len() as f64 / secs,
        g.stopped().expect("stopped").len()
    );
}
