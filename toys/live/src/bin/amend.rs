//! Amendment runs for rannoch's core: a seed fed through the queue, then a script of amendments
//! (valid ones and ones fill must refuse) fed the same way, each resumable after kill -9, and a
//! check against an oracle the script generator computes from its own plan, never from rannoch.
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rannoch_queue::Queue;
use rannoch_store::{At, Class, Cursor, Date, Declaration, Graph, HistoryKey, Relation, open};
use serde_json::{Value, json};

const KEY: [u8; 32] = [7; 32];
const SEED: u64 = 1_000_000;
const AMEND: u64 = 3_000_000;

fn id_at(secs: u64) -> String {
    let at = uuid::Timestamp::from_unix(uuid::NoContext, secs, 0);
    format!("{}:0", uuid::Uuid::new_v7(at))
}

fn declared(name: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"d-{name}","source":"rannoch:live","type":"rannoch.declared.{name}"}}"#
    )
}

fn graph(path: &str) -> Graph {
    let mut g = open(path, &KEY).expect("open");
    g.declare(&Declaration::Class(Class::new("author", ["title"])), &declared("author"))
        .expect("author");
    g.declare(&Declaration::Class(Class::new("book", ["title"])), &declared("book"))
        .expect("book");
    g.declare(
        &Declaration::Class(Class { history: HistoryKey::Time, ..Class::new("entry", ["title"]) }),
        &declared("entry"),
    )
    .expect("entry");
    g.declare(
        &Declaration::Class(Class {
            history: HistoryKey::Integer,
            ..Class::new("counter", ["title"])
        }),
        &declared("counter"),
    )
    .expect("counter");
    g.declare(
        &Declaration::Relation(Relation::new("wrote", "author", "book", false)),
        &declared("wrote"),
    )
    .expect("wrote");
    g
}

fn node(id: &str, class: &str, key: &str, title: &str, extra: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:seed","type":"rannoch.node.{class}","subject":"{key}","data":{{"properties":{{"title":"{title}"}},"valid":{{"on":"2024-01-01"}}{extra}}}}}"#
    )
}

fn link(id: &str, from: &str, to: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:seed","type":"rannoch.link.wrote","subject":"{from}","data":{{"from":{{"class":"author","key":"{from}"}},"to":{{"class":"book","key":"{to}"}},"valid":"unknown"}}}}"#
    )
}

fn ended(id: &str, from: &str, to: &str, at_ms: u64) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:seed","type":"rannoch.ended.wrote","subject":"{from}","data":{{"from":{{"class":"author","key":"{from}"}},"to":{{"class":"book","key":"{to}"}},"at":{at_ms},"valid":"unknown"}}}}"#
    )
}

fn amended(id: &str, target: &str, patch: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{id}","source":"rannoch:fixer","type":"rannoch.amended","subject":"{target}","data":{patch}}}"#
    )
}

fn title_patch(t: &str) -> String {
    format!(r#"{{"data":{{"properties":{{"title":"{t}"}}}}}}"#)
}

/// The seed, in order: every body for book `i` with its author, link and extras.
fn seed_bodies(n: u64) -> Vec<String> {
    let mut out = Vec::new();
    for i in 0..n {
        let t = SEED + 10 * i;
        out.push(node(&id_at(t), "author", &format!("a{i}"), &format!("a{i}"), ""));
        out.push(node(&id_at(t + 1), "book", &format!("b{i}"), &format!("b{i}"), ""));
        out.push(link(&id_at(t + 2), &format!("a{i}"), &format!("b{i}")));
        if i % 10 == 3 {
            out.push(ended(&id_at(t + 3), &format!("a{i}"), &format!("b{i}"), (t + 3) * 1000));
        }
        if i % 50 == 0 {
            let time = (5_000_000 + 10 * i) * 1000;
            out.push(node(&id_at(t + 4), "entry", &format!("e{i}"), &format!("e{i}"), &format!(r#","time":{time}"#)));
            out.push(node(&id_at(t + 5), "counter", &format!("c{i}"), &format!("c{i}"), &format!(r#","integer":{i}"#)));
            out.push(format!(
                r#"{{"specversion":"1.0","id":"{}","source":"rannoch:seed","type":"app.ping","subject":"p{i}","data":{{}}}}"#,
                id_at(t + 6)
            ));
        }
    }
    out
}

fn feed(path: &str, producer: &str, bodies: &[String]) {
    let t0 = std::time::Instant::now();
    let queue = Queue::open(graph(path), Queue::INTERVAL).expect("queue");
    let start = queue
        .position(producer)
        .expect("position")
        .map_or(0, |p| p.parse::<usize>().expect("numeric") + 1);
    for (k, body) in bodies.iter().enumerate().skip(start) {
        queue.accept_from(body.clone(), producer, k.to_string()).expect("accept");
        if k % 200 == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    let failures = queue.close();
    println!(
        "fed {producer} {start}..{} failures {} in {:.1}ms",
        bodies.len(),
        failures.len(),
        t0.elapsed().as_secs_f64() * 1e3
    );
}

fn subject_of(body: &str) -> (String, String) {
    let v: Value = serde_json::from_str(body).expect("json");
    (v["type"].as_str().unwrap_or("").to_owned(), v["subject"].as_str().unwrap_or("").to_owned())
}

/// The script and the oracle, from the seeded log's event ids and the plan alone.
fn script(path: &str, n: u64) -> (Vec<Value>, Value) {
    let g = open(path, &KEY).expect("open");
    let mut ids: BTreeMap<(String, String), String> = BTreeMap::new();
    for e in g.events().since(Cursor::START).expect("events") {
        ids.entry(subject_of(&e.body)).or_insert_with(|| e.id.to_string());
    }
    let target = |ty: &str, subject: &str| {
        ids.get(&(ty.to_owned(), subject.to_owned()))
            .unwrap_or_else(|| panic!("seeded {ty} {subject}"))
            .clone()
    };
    let mut lines = Vec::new();
    let mut books = serde_json::Map::new();
    let mut entries = serde_json::Map::new();
    let mut push = |body: String, refuse: bool, why: &str| {
        lines.push(json!({"body": body, "refuse": refuse, "why": why}));
    };
    for i in 0..n {
        let p0 = AMEND + 10 * i;
        let b = target("rannoch.node.book", &format!("b{i}"));
        let mut expect = json!({"key": format!("b{i}"), "title": format!("b{i}"), "known_before": p0,
            "old": Value::Null, "ended": i % 10 == 3, "late_link": Value::Null});
        match i % 4 {
            0 => {
                push(amended(&id_at(p0 + 1), &b, &title_patch(&format!("t1-{i}"))), false, "patch");
                push(amended(&id_at(p0 + 3), &b, &title_patch(&format!("t2-{i}"))), false, "stacked");
                push(amended(&id_at(p0 + 2), &b, &title_patch(&format!("late-{i}"))), true, "before latest");
                expect["title"] = json!(format!("t2-{i}"));
            }
            1 => {
                push(amended(&id_at(p0 + 1), &b, r#"{"data":{"properties":{"title":null}}}"#), false, "null");
                expect["title"] = Value::Null;
            }
            2 => {
                let (u1, u2) = (id_at(p0 + 2), id_at(p0 + 2));
                push(amended(&u1, &b, &title_patch(&format!("s1-{i}"))), false, "same point");
                push(amended(&u2, &b, &title_patch(&format!("s2-{i}"))), false, "same point");
                expect["title"] = json!(if u2 > u1 { format!("s2-{i}") } else { format!("s1-{i}") });
            }
            _ => {
                push(amended(&id_at(p0 + 1), &b, &format!(r#"{{"subject":"r{i}"}}"#)), false, "rename");
                push(node(&id_at(p0 + 4), "author", &format!("x{i}"), &format!("x{i}"), ""), false, "late author");
                push(link(&id_at(p0 + 5), &format!("x{i}"), &format!("b{i}")), false, "late link, old key");
                if i % 8 == 7 {
                    push(ended(&id_at(p0 + 6), &format!("a{i}"), &format!("b{i}"), (p0 + 6) * 1000), false, "ending after rename, old key");
                    expect["ended"] = json!(true);
                }
                expect["key"] = json!(format!("r{i}"));
                expect["old"] = json!(format!("b{i}"));
                expect["late_link"] = json!(format!("x{i}"));
            }
        }
        if i % 100 == 0 {
            let unknown = rannoch_store::EventId::now().to_string();
            push(amended(&id_at(p0 + 7), &unknown, &title_patch("nobody")), true, "unknown target");
        }
        if i % 50 == 0 {
            let e = target("rannoch.node.entry", &format!("e{i}"));
            push(amended(&id_at(4_000_000 + 10 * i), &e, &title_patch("early")), true, "before own time");
            push(amended(&id_at(6_000_000 + 10 * i), &e, &title_patch(&format!("e-late-{i}"))), false, "after own time");
            entries.insert(format!("e{i}"), json!(format!("e-late-{i}")));
            let c = target("rannoch.node.counter", &format!("c{i}"));
            push(amended(&id_at(p0 + 8), &c, &title_patch("x")), true, "integer-keyed");
            let p = target("app.ping", &format!("p{i}"));
            push(amended(&id_at(p0 + 9), &p, &title_patch("x")), true, "no hearing");
        }
        books.insert(format!("{i}"), expect);
    }
    (lines, json!({"n": n, "books": books, "entries": entries}))
}

fn known(secs: u64) -> At {
    At::Time(UNIX_EPOCH + Duration::from_secs(secs))
}

fn title(node: Option<rannoch_store::Node>) -> Option<String> {
    node.and_then(|n| n.properties.get("title").cloned()).map(|v| match v {
        rannoch_store::Value::Text(s) => s,
        other => format!("{other:?}"),
    })
}

fn tries(path: &str) -> BTreeMap<i64, (i64, Option<String>)> {
    let c = rusqlite::Connection::open(path).expect("sqlite");
    let mut st = c
        .prepare("SELECT seq, tries, reason FROM _work WHERE worker = 'rannoch.fill' ORDER BY seq")
        .expect("prepare");
    st.query_map([], |r| Ok((r.get(0)?, (r.get(1)?, r.get(2)?))))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

fn check(path: &str, oracle: &Value, lines: &[Value]) {
    let mut g = open(path, &KEY).expect("open");
    for _ in 0..5 {
        g.fill().expect("fill");
    }
    let day = Date::day(2024, 1, 1);
    let far = known(9_000_000_000);
    let mut bad: Vec<String> = Vec::new();
    let mut checked = 0u64;
    for (i, e) in oracle["books"].as_object().expect("books") {
        let key = e["key"].as_str().expect("key");
        let want = e["title"].as_str().map(str::to_owned);
        let got = title(g.find("book", key).expect("find"));
        if got != want {
            bad.push(format!("title {key}: want {want:?} got {got:?}"));
        }
        let kb = e["known_before"].as_u64().expect("kb");
        let orig = format!("b{i}");
        let before = title(g.find_as_of("book", &orig, &day, Some(known(kb))).expect("as of"));
        if before.as_deref() != Some(orig.as_str()) {
            bad.push(format!("known before {orig}: got {before:?}"));
        }
        let a = format!("a{i}");
        match g.link_state_at("wrote", &a, key, far).expect("link") {
            None => bad.push(format!("link {a}->{key} missing")),
            Some(v) if v.ended != e["ended"].as_bool().expect("ended") => {
                bad.push(format!("link {a}->{key} ended {} want {}", v.ended, e["ended"]))
            }
            _ => {}
        }
        if let Some(old) = e["old"].as_str() {
            if g.find("book", old).expect("find").is_some() {
                bad.push(format!("old key {old} still answers"));
            }
            if g.link_known_at("wrote", &a, old, known(kb)).expect("k").is_none() {
                bad.push(format!("link {a}->{old} not known before the rename"));
            }
            if g.link_known_at("wrote", &a, key, known(kb)).expect("k").is_some() {
                bad.push(format!("link {a}->{key} known before the rename"));
            }
            let x = e["late_link"].as_str().expect("x");
            match g.link_state_at("wrote", x, key, far).expect("late") {
                Some(v) if !v.ended => {}
                other => bad.push(format!("late link {x}->{key}: {other:?}")),
            }
        }
        checked += 1;
    }
    for (k, want) in oracle["entries"].as_object().expect("entries") {
        let got = title(g.find("entry", k).expect("find"));
        if got.as_deref() != want.as_str() {
            bad.push(format!("entry {k}: want {want} got {got:?}"));
        }
    }
    // Refused bodies by their own CloudEvent id, mapped to log cursors.
    let refused_ids: BTreeSet<String> = lines
        .iter()
        .filter(|l| l["refuse"].as_bool() == Some(true))
        .map(|l| {
            let v: Value = serde_json::from_str(l["body"].as_str().expect("b")).expect("j");
            v["id"].as_str().expect("id").to_owned()
        })
        .collect();
    let mut refused: BTreeSet<i64> = BTreeSet::new();
    for e in g.events().since(Cursor::START).expect("events") {
        let v: Value = serde_json::from_str(&e.body).expect("json");
        if v["id"].as_str().is_some_and(|id| refused_ids.contains(id)) {
            refused.insert(cursor_n(e.cursor));
        }
    }
    let stopped: BTreeMap<i64, String> =
        g.stopped().expect("stopped").into_iter().map(|(c, r)| (cursor_n(c), r)).collect();
    let stopped_set: BTreeSet<i64> = stopped.keys().copied().collect();
    let rows = tries(path);
    let others: Vec<_> = rows.iter().filter(|(s, _)| !refused.contains(s)).collect();
    let off_limit: Vec<_> = rows.iter().filter(|(s, (t, _))| refused.contains(s) && *t != 3).collect();
    let mut reasons: BTreeMap<String, u64> = BTreeMap::new();
    for r in stopped.values() {
        let kind = if r.contains("not in the log") {
            "unknown target"
        } else if r.contains("integer key") {
            "integer-keyed class"
        } else if r.contains("no hearing") {
            "no hearing"
        } else if r.contains("before") {
            "before own point or latest"
        } else {
            "other"
        };
        *reasons.entry(kind.to_owned()).or_default() += 1;
    }
    let open_links: i64 = rusqlite::Connection::open(path)
        .expect("sqlite")
        .query_row("SELECT count(*) FROM _open", [], |r| r.get(0))
        .expect("open");
    println!("books checked {checked}, mismatches {}", bad.len());
    for b in bad.iter().take(15) {
        println!("  {b}");
    }
    println!(
        "refused {} stopped {} equal {} | fill rows other than refused {} | refused rows with tries != 3 {} | _open {open_links}",
        refused.len(),
        stopped_set.len(),
        refused == stopped_set,
        others.len(),
        off_limit.len()
    );
    for o in others.iter().take(5) {
        println!("  other row {o:?}");
    }
    for (r, c) in &reasons {
        println!("  reason x{c}: {r}");
    }
    println!("processed {:?} committed {:?}", g.processed().expect("p"), g.events().committed().expect("c"));
}

fn cursor_n(c: Cursor) -> i64 {
    serde_json::to_value(c).expect("cursor").as_i64().expect("int")
}

fn read_script(path: &str) -> Vec<Value> {
    std::fs::read_to_string(path)
        .expect("script")
        .lines()
        .map(|l| serde_json::from_str(l).expect("line"))
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mode, path) = (args[1].as_str(), args[2].as_str());
    match mode {
        "seed" => feed(path, "seed", &seed_bodies(args[3].parse().expect("n"))),
        "script" => {
            let (lines, oracle) = script(path, args[3].parse().expect("n"));
            let out: String = lines.iter().map(|l| format!("{l}\n")).collect();
            std::fs::write(&args[4], out).expect("write script");
            std::fs::write(&args[5], oracle.to_string()).expect("write oracle");
            let refused = lines.iter().filter(|l| l["refuse"] == json!(true)).count();
            println!("script {} lines, {refused} to refuse", lines.len());
        }
        "feed" => {
            let bodies: Vec<String> =
                read_script(&args[3]).iter().map(|l| l["body"].as_str().expect("b").to_owned()).collect();
            feed(path, "script", &bodies);
        }
        "check" => {
            let oracle: Value =
                serde_json::from_str(&std::fs::read_to_string(&args[3]).expect("oracle")).expect("json");
            check(path, &oracle, &read_script(&args[4]));
        }
        "tries" => {
            for (s, (t, r)) in tries(path) {
                println!("{s} {t} {}", r.unwrap_or_default().chars().take(40).collect::<String>());
            }
        }
        "retry" => {
            let c: i64 = args[3].parse().expect("cursor");
            let mut g = open(path, &KEY).expect("open");
            let cur: Cursor = serde_json::from_value(json!(c)).expect("cursor");
            g.retry(cur).expect("retry");
            println!("after retry: {:?}", tries(path).get(&c));
            for k in 0..5 {
                g.fill().expect("fill");
                println!("after fill {k}: {:?}", tries(path).get(&c));
            }
            println!("stopped holds it: {}", g.stopped().expect("s").iter().any(|(x, _)| cursor_n(*x) == c));
        }
        "retry-only" => {
            let c: i64 = args[3].parse().expect("cursor");
            let mut g = open(path, &KEY).expect("open");
            let cur: Cursor = serde_json::from_value(json!(c)).expect("cursor");
            g.retry(cur).expect("retry");
            println!("after retry: {:?}", tries(path).get(&c));
        }
        "fill-loop" => {
            let mut g = open(path, &KEY).expect("open");
            loop {
                g.fill().expect("fill");
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        _ => panic!("mode"),
    }
    let _ = SystemTime::now();
}
