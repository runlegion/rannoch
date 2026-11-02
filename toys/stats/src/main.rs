//! Toy: class and relation stats kept by fill, and a node's link reads at game-build size.
//!
//!   rannoch-toy-stats fill <dir> <events>          clean fill throughput (RANNOCH_TOY_STATS=0 turns stats off)
//!   rannoch-toy-stats check <dir>                   stats against counts(): fill, amend and rename, rebuild
//!   rannoch-toy-stats reads <dir> <alias-percent>   build a game-size store, time link reads and describe

use std::time::{Instant, SystemTime};

use rannoch_store::{Class, Declaration, Direction, EventId, Graph, NodeRef, Relation, View, open};

const KEY: [u8; 32] = [7; 32];

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

fn store(dir: &str, name: &str) -> Result<Graph, Box<dyn std::error::Error>> {
    let path = std::path::Path::new(dir).join(name);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let mut g = open(&path, &KEY)?;
    g.declare(&Declaration::Class(Class::new("author", ["title"])), &declared("author"))?;
    g.declare(&Declaration::Class(Class::new("book", ["title"])), &declared("book"))?;
    g.declare(
        &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
        &declared("wrote"),
    )?;
    Ok(g)
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

/// The stats against counts() at view now; returns whether they agree.
fn agree(g: &Graph, label: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let counts = g.counts(&now())?;
    let stats: std::collections::BTreeMap<String, i64> = g.toy_stats()?.into_iter().collect();
    let mut ok = true;
    for (class, n) in &counts.nodes {
        let s = stats.get(&format!("class:{class}")).copied().unwrap_or(0);
        if s != i64::try_from(*n)? {
            ok = false;
        }
        println!("  {label}: class {class}: counts {n}, stats {s}");
    }
    for (rel, n) in &counts.links {
        let s = stats.get(&format!("link:{rel}")).copied().unwrap_or(0);
        if s != i64::try_from(*n)? {
            ok = false;
        }
        println!("  {label}: relation {rel}: counts {n}, stats {s}");
    }
    println!("  {label}: {}", if ok { "AGREE" } else { "DIFFER" });
    Ok(ok)
}

fn fill_bench(dir: &str, events: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut g = store(dir, "fill.rannoch")?;
    let half = events / 2;
    let mut bodies = Vec::with_capacity(events);
    for i in 0..half / 10 {
        bodies.push(node("author", &format!("a{i}"), "x"));
    }
    for i in 0..half - half / 10 {
        bodies.push(node("book", &format!("b{i}"), "x"));
    }
    for i in 0..events - half {
        bodies.push(wrote(&format!("a{}", i % (half / 10)), &format!("b{}", (i * 7919) % (half - half / 10))));
    }
    commit_all(&mut g, &bodies)?;
    let started = Instant::now();
    g.fill()?;
    let secs = started.elapsed().as_secs_f64();
    println!(
        "fill {} events in {secs:.3} s: {:.0} events/s (stats {})",
        bodies.len(),
        bodies.len() as f64 / secs,
        std::env::var("RANNOCH_TOY_STATS").unwrap_or_else(|_| "on".into())
    );
    Ok(())
}

fn check(dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut g = store(dir, "check.rannoch")?;
    let mut bodies = Vec::new();
    for i in 0..200 {
        bodies.push(node("author", &format!("a{i}"), "x"));
    }
    let renamed_book = node("book", "b0", "x");
    bodies.push(renamed_book.clone());
    for i in 1..500 {
        bodies.push(node("book", &format!("b{i}"), "x"));
    }
    for i in 0..1_000 {
        bodies.push(wrote(&format!("a{}", i % 200), &format!("b{}", (i * 7) % 500)));
    }
    // Open links: an author not yet heard.
    bodies.push(wrote("late", "b1"));
    bodies.push(wrote("late", "b2"));
    commit_all(&mut g, &bodies)?;
    g.fill()?;
    let mut all = agree(&g, "after fill")?;

    // Endings, a reopened link, the late author closing open links, an amendment and a rename.
    let mut more = vec![ended("a0", "b0"), ended("a1", "b7"), wrote("a1", "b7"), node("author", "late", "x")];
    let target = renamed_book.split("\"id\":\"").nth(1).and_then(|s| s.split('"').next()).ok_or("id")?.to_string();
    more.push(amended(&target, r#"{"data":{"properties":{"title":"y"}}}"#));
    more.push(amended(&target, r#"{"subject":"b9999"}"#));
    commit_all(&mut g, &more)?;
    g.fill()?;
    all &= agree(&g, "after endings, amendment and rename")?;

    // Rebuild: the same log committed into a fresh store, in order.
    let log: Vec<String> = g.events().since(rannoch_store::Cursor::START)?.into_iter().map(|e| e.body).collect();
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{dir}/rebuilt.rannoch{suffix}"));
    }
    let mut r = open(std::path::Path::new(dir).join("rebuilt.rannoch"), &KEY)?;
    for body in &log {
        let v: &str = body;
        if v.contains("rannoch.declared.") {
            let name = v.split("rannoch.declared.").nth(1).and_then(|s| s.split('"').next()).ok_or("name")?;
            let decl = match name {
                "author" => Declaration::Class(Class::new("author", ["title"])),
                "book" => Declaration::Class(Class::new("book", ["title"])),
                _ => Declaration::Relation(Relation::new("wrote", "author", "book", false)),
            };
            r.declare(&decl, v)?;
        } else if !v.contains("\"source\":\"rannoch\"") {
            for landed in r.commit(&[v], &[])? {
                landed.map_err(|e| e.to_string())?;
            }
        }
    }
    r.fill()?;
    all &= agree(&r, "after rebuild")?;
    println!("check: {}", if all { "ALL AGREE" } else { "SOME DIFFER" });
    Ok(())
}

fn time<T>(runs: u32, mut f: impl FnMut() -> T) -> (f64, T) {
    let runs = std::env::var("RANNOCH_TOY_RUNS").ok().and_then(|r| r.parse().ok()).unwrap_or(runs);
    let started = Instant::now();
    let first = f();
    if runs == 0 {
        return (started.elapsed().as_secs_f64() * 1000.0, first);
    }
    let started = Instant::now();
    for _ in 0..runs {
        std::hint::black_box(f());
    }
    (started.elapsed().as_secs_f64() * 1000.0 / f64::from(runs), first)
}

fn reads(dir: &str, alias_percent: usize) -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("RANNOCH_TOY_REUSE").is_ok() {
        let g = open(std::path::Path::new(dir).join(format!("reads-{alias_percent}.rannoch")), &KEY)?;
        return time_reads(&g);
    }
    let mut g = store(dir, &format!("reads-{alias_percent}.rannoch"))?;
    let authors = 130_000usize;
    let books = 200_000usize;
    let mut bodies = Vec::new();
    for i in 0..authors {
        bodies.push(node("author", &format!("a{i}"), "x"));
    }
    for i in 0..books {
        bodies.push(node("book", &format!("b{i}"), "x"));
    }
    let fans = [1usize, 10, 100, 1_000, 10_000, 120_000];
    for fan in fans {
        bodies.push(node("author", &format!("f{fan}"), "x"));
        for j in 0..fan {
            bodies.push(wrote(&format!("f{fan}"), &format!("b{j}")));
        }
    }
    for fan in [1usize, 10, 100, 1_000, 10_000, 120_000] {
        bodies.push(node("book", &format!("in{fan}"), "x"));
        for j in 0..fan {
            bodies.push(wrote(&format!("a{j}"), &format!("in{fan}")));
        }
    }
    for i in 0..150_000usize {
        bodies.push(wrote(&format!("a{}", (i * 31) % authors), &format!("b{}", (i * 7919) % books)));
    }
    // Aliases: a share of the hearings re-sent under new ids with the same content.
    let resend: Vec<String> = if alias_percent == 0 {
        Vec::new()
    } else {
        let step = 100 / alias_percent;
        bodies
            .iter()
            .enumerate()
            .filter(|(i, _)| i % step == 0)
            .map(|(_, b)| {
                let old = b.split("\"id\":\"").nth(1).and_then(|s| s.split('"').next()).unwrap_or_default();
                b.replacen(old, &id(), 1)
            })
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
    time_reads(&g)
}

fn time_reads(g: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    let fans = [1usize, 10, 100, 1_000, 10_000, 120_000];
    let known = View { axis: None, known: Some(SystemTime::now()) };
    println!("| read | fan | now ms | known-at ms | rows |");
    for fan in fans {
        let n = NodeRef::new("author", format!("f{fan}"));
        let (t, rows) = time(20, || g.links(&n, "wrote", Direction::Out, &now()).map(|v| v.len()).unwrap_or(0));
        let (tk, _) = time(20, || g.links(&n, "wrote", Direction::Out, &known).map(|v| v.len()).unwrap_or(0));
        println!("| links out | {fan} | {t:.3} | {tk:.3} | {rows} |");
    }
    for fan in fans {
        let n = NodeRef::new("book", format!("in{fan}"));
        let (t, rows) = time(20, || g.links(&n, "wrote", Direction::In, &now()).map(|v| v.len()).unwrap_or(0));
        let (tk, _) = time(20, || g.links(&n, "wrote", Direction::In, &known).map(|v| v.len()).unwrap_or(0));
        println!("| links in | {fan} | {t:.3} | {tk:.3} | {rows} |");
    }
    for fan in fans {
        let n = NodeRef::new("author", format!("f{fan}"));
        let (t, rows) = time(20, || g.links_page(&n, "wrote", Direction::Out, &now(), "", 100).map(|v| v.len()).unwrap_or(0));
        let (tk, _) = time(20, || g.links_page(&n, "wrote", Direction::Out, &known, "b5", 100).map(|v| v.len()).unwrap_or(0));
        println!("| page out (100; then after b5 at known-at) | {fan} | {t:.3} | {tk:.3} | {rows} |");
    }
    for fan in fans {
        let n = NodeRef::new("book", format!("in{fan}"));
        let (t, rows) = time(20, || g.links_page(&n, "wrote", Direction::In, &now(), "", 100).map(|v| v.len()).unwrap_or(0));
        let (tk, _) = time(20, || g.links_page(&n, "wrote", Direction::In, &known, "a5", 100).map(|v| v.len()).unwrap_or(0));
        println!("| page in (100; then after a5 at known-at) | {fan} | {t:.3} | {tk:.3} | {rows} |");
    }
    for fan in fans {
        let n = NodeRef::new("author", format!("f{fan}"));
        let (t, _) = time(20, || g.describe(&n, &now()).map(|d| d.is_some()).unwrap_or(false));
        let (tk, _) = time(20, || g.describe(&n, &known).map(|d| d.is_some()).unwrap_or(false));
        println!("| describe | {fan} | {t:.3} | {tk:.3} | - |");
    }
    let (t, _) = time(5, || g.counts(&now()).map(|c| c.links.len()).unwrap_or(0));
    let (ts, _) = time(20, || g.toy_stats().map(|s| s.len()).unwrap_or(0));
    println!("| counts() now | - | {t:.3} | - | - |");
    println!("| stats read | - | {ts:.4} | - | - |");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("fill") => fill_bench(&args[2], args[3].parse()?),
        Some("check") => check(&args[2]),
        Some("reads") => reads(&args[2], args[3].parse()?),
        _ => {
            eprintln!("usage: fill <dir> <events> | check <dir> | reads <dir> <alias-percent>");
            Ok(())
        }
    }
}
