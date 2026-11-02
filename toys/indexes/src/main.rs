//! Toy: indexes and rewrites for rannoch's core schema, judged by query plans and timings.
//!
//!   rannoch-toy-indexes build <dir> <alias-percent>     build the game-size store
//!   rannoch-toy-indexes capture <dir> <alias-percent>   print every statement fill and each read runs
//!   rannoch-toy-indexes reads <dir> <alias-percent>     time every read on the built store
//!   rannoch-toy-indexes fill <dir> <events>             clean fill throughput

use std::collections::BTreeSet;
use std::time::{Instant, SystemTime};

use rannoch_store::{Class, Declaration, Direction, EventId, Graph, NodeRef, Relation, View, open};

const KEY: [u8; 32] = [7; 32];
const GENRES: usize = 50;

fn id() -> String {
    format!("{}:0", EventId::now())
}

fn declared(name: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:toy","type":"rannoch.declared.{name}"}}"#,
        id()
    )
}

fn node(class: &str, key: &str, title: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:toy","type":"rannoch.node.{class}","subject":"{key}","data":{{"properties":{{"title":"{title}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

fn wrote(author: &str, book: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:toy","type":"rannoch.link.wrote","data":{{"from":{{"class":"author","key":"{author}"}},"to":{{"class":"book","key":"{book}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

fn ended(author: &str, book: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:toy","type":"rannoch.ended.wrote","data":{{"from":{{"class":"author","key":"{author}"}},"to":{{"class":"book","key":"{book}"}},"valid":"unknown"}}}}"#,
        id()
    )
}

fn amended(target: &str, patch: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:fixer","type":"rannoch.amended","data":{{"target":{{"source":"rannoch:toy","id":"{target}"}},"patch":{patch}}}}}"#,
        id()
    )
}

fn id_of(body: &str) -> String {
    body.split("\"id\":\"").nth(1).and_then(|s| s.split('"').next()).unwrap_or_default().to_string()
}

fn book_class() -> Class {
    let mut c = Class::new("book", ["title"]);
    if std::env::var("RANNOCH_TOY_NOINDEX").is_err() {
        c.indexed.insert("title".into());
    }
    c
}

fn store(dir: &str, name: &str) -> Result<Graph, Box<dyn std::error::Error>> {
    let path = std::path::Path::new(dir).join(name);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let mut g = open(&path, &KEY)?;
    g.declare(&Declaration::Class(Class::new("author", ["title"])), &declared("author"))?;
    g.declare(&Declaration::Class(book_class()), &declared("book"))?;
    g.declare(
        &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
        &declared("wrote"),
    )?;
    Ok(g)
}

fn reopen(dir: &str, alias_percent: usize) -> Result<Graph, Box<dyn std::error::Error>> {
    Ok(open(std::path::Path::new(dir).join(format!("idx-{alias_percent}.rannoch")), &KEY)?)
}

fn commit_all(g: &mut Graph, bodies: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    for chunk in bodies.chunks(5_000) {
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        for landed in g.commit(&refs, &[])? {
            landed.map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn now() -> View {
    View { axis: None, known: None }
}

fn known() -> View {
    View { axis: None, known: Some(SystemTime::now()) }
}

const FANS: [usize; 6] = [1, 10, 100, 1_000, 10_000, 120_000];

fn build(dir: &str, alias_percent: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut g = store(dir, &format!("idx-{alias_percent}.rannoch"))?;
    let authors = 130_000usize;
    let books = 200_000usize;
    let mut bodies = Vec::new();
    for i in 0..authors {
        bodies.push(node("author", &format!("a{i}"), "x"));
    }
    for i in 0..books {
        bodies.push(node("book", &format!("b{i}"), &format!("g{}", i % GENRES)));
    }
    for fan in FANS {
        bodies.push(node("author", &format!("f{fan}"), "x"));
        for j in 0..fan {
            bodies.push(wrote(&format!("f{fan}"), &format!("b{j}")));
        }
    }
    for fan in FANS {
        bodies.push(node("book", &format!("in{fan}"), "hub"));
        for j in 0..fan {
            bodies.push(wrote(&format!("a{j}"), &format!("in{fan}")));
        }
    }
    for i in 0..150_000usize {
        bodies.push(wrote(&format!("a{}", (i * 31) % authors), &format!("b{}", (i * 7919) % books)));
    }
    for i in 0..5_000usize {
        bodies.push(ended(&format!("a{}", (i * 31) % authors), &format!("b{}", (i * 7919) % books)));
    }
    let resend: Vec<String> = if alias_percent == 0 {
        Vec::new()
    } else {
        let step = 100 / alias_percent;
        bodies
            .iter()
            .enumerate()
            .filter(|(i, b)| i % step == 0 && !b.contains("rannoch.ended."))
            .map(|(_, b)| b.replacen(&id_of(b), &id(), 1))
            .collect()
    };
    bodies.extend(resend);
    commit_all(&mut g, &bodies)?;
    let started = Instant::now();
    g.fill()?;
    println!(
        "store with {alias_percent}% re-sent: {} events filled in {:.1} s",
        bodies.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn query(read: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}","source":"rannoch:toy","type":"rannoch.query","data":{{{read},"view":{{}}}}}}"#,
        id()
    )
}

/// Every read the toy times, by label.
fn reads_of(g: &Graph) -> Vec<(String, Box<dyn Fn() -> usize + '_>)> {
    let mut all: Vec<(String, Box<dyn Fn() -> usize + '_>)> = Vec::new();
    for fan in FANS {
        let out = NodeRef::new("author", format!("f{fan}"));
        let into = NodeRef::new("book", format!("in{fan}"));
        let (o, i) = (out.clone(), into.clone());
        all.push((format!("links out {fan} now"), Box::new(move || g.links(&o, "wrote", Direction::Out, &now()).map(|v| v.len()).unwrap_or(0))));
        let o = out.clone();
        all.push((format!("links out {fan} known"), Box::new(move || g.links(&o, "wrote", Direction::Out, &known()).map(|v| v.len()).unwrap_or(0))));
        all.push((format!("links in {fan} now"), Box::new(move || g.links(&i, "wrote", Direction::In, &now()).map(|v| v.len()).unwrap_or(0))));
        let o = out.clone();
        all.push((format!("page out {fan}"), Box::new(move || g.links_page(&o, "wrote", Direction::Out, &now(), "", 100).map(|v| v.len()).unwrap_or(0))));
        let i = into.clone();
        all.push((format!("page in {fan}"), Box::new(move || g.links_page(&i, "wrote", Direction::In, &now(), "", 100).map(|v| v.len()).unwrap_or(0))));
        let o = out.clone();
        all.push((format!("describe out-hub {fan}"), Box::new(move || g.describe(&o, &now()).map(|d| usize::from(d.is_some())).unwrap_or(0))));
        let i = into.clone();
        all.push((format!("describe in-hub {fan}"), Box::new(move || g.describe(&i, &now()).map(|d| usize::from(d.is_some())).unwrap_or(0))));
    }
    let b = NodeRef::new("book", "b17");
    let b2 = b.clone();
    all.push(("state_at now".into(), Box::new(move || g.state_at(&b, &now()).map(|v| usize::from(v.is_some())).unwrap_or(0))));
    all.push(("state_at known".into(), Box::new(move || g.state_at(&b2, &known()).map(|v| usize::from(v.is_some())).unwrap_or(0))));
    all.push(("link_state_at now".into(), Box::new(move || g.link_state_at("wrote", "f10", "b3", &now()).map(|v| usize::from(v.is_some())).unwrap_or(0))));
    all.push(("link_state_at known".into(), Box::new(move || g.link_state_at("wrote", "f10", "b3", &known()).map(|v| usize::from(v.is_some())).unwrap_or(0))));
    all.push(("find".into(), Box::new(move || g.find("book", "b17").map(|v| usize::from(v.is_some())).unwrap_or(0))));
    let mut filter = std::collections::BTreeMap::new();
    filter.insert("title".to_string(), rannoch_store::Value::Text("g7".into()));
    let f2 = filter.clone();
    all.push(("nodes book title=g7 now (4k rows)".into(), Box::new(move || g.nodes("book", &filter, &now()).map(|v| v.len()).unwrap_or(0))));
    all.push(("nodes book title=g7 known".into(), Box::new(move || g.nodes("book", &f2, &known()).map(|v| v.len()).unwrap_or(0))));
    let mut hub = std::collections::BTreeMap::new();
    hub.insert("title".to_string(), rannoch_store::Value::Text("hub".into()));
    all.push(("nodes book title=hub (6 rows)".into(), Box::new(move || g.nodes("book", &hub, &now()).map(|v| v.len()).unwrap_or(0))));
    all.push(("counts now".into(), Box::new(move || g.counts(&now()).map(|c| c.links.len()).unwrap_or(0))));
    all.push(("counts known".into(), Box::new(move || g.counts(&known()).map(|c| c.links.len()).unwrap_or(0))));
    all.push(("toy stats read".into(), Box::new(move || g.toy_stats().map(|s| s.len()).unwrap_or(0))));
    let q = query(r#""counts":{}"#);
    all.push(("answer counts, default known-at".into(), Box::new(move || g.answer(&q).map(|s| s.len()).unwrap_or(0))));
    let q2 = query(r#""links":{"node":{"class":"author","key":"f10"},"relation":"wrote","direction":"out"}"#);
    all.push(("answer links f10, default known-at".into(), Box::new(move || g.answer(&q2).map(|s| s.len()).unwrap_or(0))));
    all
}

fn capture(dir: &str, alias_percent: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut g = reopen(dir, alias_percent)?;
    let mut seen = BTreeSet::new();
    let mut emit = |label: &str, stmts: Vec<String>| {
        for s in stmts {
            let s = s.trim().to_string();
            if seen.insert(s.clone()) {
                println!("== {label}\n{s}\n;;");
            }
        }
    };
    {
        let reads = reads_of(&g);
        for (label, f) in &reads {
            g.toy_capture(true);
            f();
            emit(label, g.toy_capture(false));
        }
    }
    // Fill: new nodes and links, an ending, re-sent content, and an amendment.
    let first = node("book", "zz1", "g1");
    let mut bodies = vec![first.clone(), node("author", "zz", "x"), wrote("zz", "zz1"), wrote("zz", "b5"), ended("zz", "b5")];
    bodies.push(first.replacen(&id_of(&first), &id(), 1));
    bodies.push(amended(&id_of(&first), r#"{"data":{"properties":{"title":"g2"}}}"#));
    bodies.push(wrote("nobody-yet", "zz1"));
    commit_all(&mut g, &bodies)?;
    g.toy_capture(true);
    g.fill()?;
    emit("fill", g.toy_capture(false));
    Ok(())
}

fn time<T>(runs: u32, mut f: impl FnMut() -> T) -> f64 {
    let runs = std::env::var("RANNOCH_TOY_RUNS").ok().and_then(|r| r.parse().ok()).unwrap_or(runs);
    std::hint::black_box(f());
    let started = Instant::now();
    for _ in 0..runs.max(1) {
        std::hint::black_box(f());
    }
    started.elapsed().as_secs_f64() * 1000.0 / f64::from(runs.max(1))
}

fn time_reads(dir: &str, alias_percent: usize) -> Result<(), Box<dyn std::error::Error>> {
    let g = reopen(dir, alias_percent)?;
    let only = std::env::var("RANNOCH_TOY_ONLY").ok();
    println!("| read ({alias_percent}% aliases) | ms | rows |");
    for (label, f) in reads_of(&g) {
        if only.as_deref().is_some_and(|o| !label.contains(o)) {
            continue;
        }
        let rows = f();
        let runs = if label.contains("120000") || label.starts_with("counts") || label.contains("answer counts") { 5 } else { 20 };
        let ms = time(runs, &f);
        println!("| {label} | {ms:.3} | {rows} |");
    }
    Ok(())
}

fn fill_bench(dir: &str, events: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut g = store(dir, "fill.rannoch")?;
    let half = events / 2;
    let mut bodies = Vec::with_capacity(events);
    for i in 0..half / 10 {
        bodies.push(node("author", &format!("a{i}"), "x"));
    }
    for i in 0..half - half / 10 {
        bodies.push(node("book", &format!("b{i}"), &format!("g{}", i % GENRES)));
    }
    for i in 0..events - half {
        bodies.push(wrote(&format!("a{}", i % (half / 10)), &format!("b{}", (i * 7919) % (half - half / 10))));
    }
    // A realistic share of re-sent content: 2% of events.
    if std::env::var("RANNOCH_TOY_NORESEND").is_err() {
        let resend: Vec<String> = bodies.iter().step_by(50).map(|b| b.replacen(&id_of(b), &id(), 1)).collect();
        bodies.extend(resend);
    }
    if std::env::var("RANNOCH_TOY_PHASES").is_ok() {
        let n_nodes = half / 10 + (half - half / 10);
        let n_links = events - half;
        let phases = [("nodes", 0, n_nodes), ("links", n_nodes, n_nodes + n_links), ("resend", n_nodes + n_links, bodies.len())];
        let mut g = store(dir, "fill-phases.rannoch")?;
        for (name, a, b) in phases {
            if a >= b { continue; }
            commit_all(&mut g, &bodies[a..b])?;
            let t = Instant::now();
            g.fill()?;
            let s = t.elapsed().as_secs_f64();
            println!("  phase {name}: {} events in {s:.3} s: {:.0}/s", b - a, (b - a) as f64 / s);
        }
        return Ok(());
    }
    commit_all(&mut g, &bodies)?;
    let profile = std::env::var("RANNOCH_TOY_PROFILE").is_ok();
    if profile {
        g.toy_profile(true);
    }
    let started = Instant::now();
    g.fill()?;
    let secs = started.elapsed().as_secs_f64();
    println!("fill {} events in {secs:.3} s: {:.0} events/s", bodies.len(), bodies.len() as f64 / secs);
    eprintln!("stopped {}", g.stopped().map(|v| v.len()).unwrap_or(0));
    if profile {
        for (sql, ms, n) in g.toy_profile(false).into_iter().take(25) {
            println!("  {ms:9.0} ms {n:8} x  {}", sql.replace('\n', " ").chars().take(160).collect::<String>());
        }
    }
    Ok(())
}

fn verify(dir: &str, alias_percent: usize) -> Result<(), Box<dyn std::error::Error>> {
    let g = reopen(dir, alias_percent)?;
    let mut nodes: Vec<NodeRef> = FANS.iter().flat_map(|f| [NodeRef::new("author", format!("f{f}")), NodeRef::new("book", format!("in{f}"))]).collect();
    for i in (0..130_000).step_by(997) {
        nodes.push(NodeRef::new("author", format!("a{i}")));
    }
    for i in (0..200_000).step_by(1499) {
        nodes.push(NodeRef::new("book", format!("b{i}")));
    }
    let mut bad = 0;
    for n in &nodes {
        let Some(d) = g.describe(n, &now())? else { continue };
        for (dir, out) in [(Direction::Out, true), (Direction::In, false)] {
            if (n.class == "author") != out { continue; }
            let want = g.links(n, "wrote", dir, &now())?.len() as u64;
            let got = d.links.get(&("wrote".to_string(), dir)).copied().unwrap_or(0);
            if want != got {
                bad += 1;
                println!("  differ: {}:{} {:?} counter {got}, links {want}", n.class, n.key, dir);
            }
        }
    }
    println!("verify describe counts on {} nodes: {bad} differ", nodes.len());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("build") => build(&args[2], args[3].parse()?),
        Some("capture") => capture(&args[2], args[3].parse()?),
        Some("reads") => time_reads(&args[2], args[3].parse()?),
        Some("fill") => fill_bench(&args[2], args[3].parse()?),
        Some("verify") => verify(&args[2], args[3].parse()?),
        _ => {
            eprintln!("usage: build|capture|reads <dir> <alias-percent> | fill <dir> <events>");
            Ok(())
        }
    }
}
