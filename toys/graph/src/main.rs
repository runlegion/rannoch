//! Does the store give back the graph that went in? See PREDICTIONS.md.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, UNIX_EPOCH};

use rannoch_store::{Class, Declaration, Direction, Graph, NodeRef, Relation, Value, View, open};
use serde_json::json;

type Res<T> = Result<T, Box<dyn std::error::Error>>;
type Edge = (String, String);

const KEY: [u8; 32] = [7; 32];

fn id_at(seconds: u64) -> String {
    let at = uuid::Timestamp::from_unix(uuid::NoContext, seconds, 0);
    format!("{}:0", uuid::Uuid::new_v7(at))
}

fn at(seconds: u64) -> View {
    View { axis: None, known: Some(UNIX_EPOCH + Duration::from_secs(seconds)) }
}

fn point(axis: Option<i64>, seconds: Option<u64>) -> View {
    View {
        axis: axis.map(|a| vec![a]),
        known: seconds.map(|s| UNIX_EPOCH + Duration::from_secs(s)),
    }
}

fn node_event(id: &str, class: &str, key: &str, props: serde_json::Value, build: Option<i64>) -> String {
    let mut data = json!({"properties": props});
    if let Some(b) = build {
        data["axis"] = json!({"build": b});
    }
    json!({"specversion":"1.0","id":id,"source":"rannoch:toy","type":format!("rannoch.node.{class}"),
           "subject":key,"data":data}).to_string()
}

fn link_event(id: &str, kind: &str, rel: &str, ends: (&str, &str, &str, &str), build: Option<i64>) -> String {
    let (from_class, from, to_class, to) = ends;
    let mut data = json!({"from":{"class":from_class,"key":from},"to":{"class":to_class,"key":to}});
    if let Some(b) = build {
        data["axis"] = json!({"build": b});
    }
    json!({"specversion":"1.0","id":id,"source":"rannoch:toy","type":format!("rannoch.{kind}.{rel}"),
           "data":data}).to_string()
}

fn declared(name: &str) -> String {
    format!(r#"{{"specversion":"1.0","id":"d-{name}","source":"rannoch:toy","type":"rannoch.declared.{name}"}}"#)
}

fn feed(g: &mut Graph, bodies: &[String], chunk: usize) -> Res<()> {
    for part in bodies.chunks(chunk) {
        let refs: Vec<&str> = part.iter().map(String::as_str).collect();
        for landed in g.commit(&refs, &[])? {
            landed.map_err(|e| e.to_string())?;
        }
        g.fill()?;
    }
    let stopped = g.stopped()?;
    if !stopped.is_empty() {
        return Err(format!("{} events stopped, first: {:?}", stopped.len(), stopped[0]).into());
    }
    Ok(())
}

/// Seeded shuffle, so a run repeats.
fn shuffle<T>(items: &mut [T], seed: u64) {
    let mut s = seed.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(3_037_000_493);
    for i in (1..items.len()).rev() {
        s = s.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        items.swap(i, ((s >> 33) as usize) % (i + 1));
    }
}

fn fresh(dir: &Path, name: &str) -> PathBuf {
    let p = dir.join(format!("{name}-{}.rannoch", uuid::Uuid::now_v7()));
    p
}

/// The whole graph of one relation, as the public reads give it: every node's links out, plus the
/// links in of the extra keys asked, checked against the links in of the other end.
fn edges(g: &Graph, class: &str, rel: &str, keys: &BTreeSet<String>, view: &View) -> Res<BTreeSet<Edge>> {
    let mut out = BTreeSet::new();
    for key in keys {
        for (other, _) in g.links(&NodeRef::new(class, key), rel, Direction::Out, view)? {
            out.insert((key.clone(), other));
        }
    }
    Ok(out)
}

fn in_edges(g: &Graph, class: &str, rel: &str, keys: &BTreeSet<String>, view: &View) -> Res<BTreeSet<Edge>> {
    let mut out = BTreeSet::new();
    for key in keys {
        for (other, _) in g.links(&NodeRef::new(class, key), rel, Direction::In, view)? {
            out.insert((other, key.clone()));
        }
    }
    Ok(out)
}

fn listed(g: &Graph, class: &str, view: &View) -> Res<BTreeSet<String>> {
    Ok(g.nodes(class, &BTreeMap::new(), view)?.into_iter().map(|(k, _)| k).collect())
}

fn show(edges: &BTreeSet<Edge>) -> String {
    edges.iter().map(|(a, b)| format!("{a}>{b}")).collect::<Vec<_>>().join(" ")
}

fn check(fails: &mut u32, name: &str, got: &BTreeSet<Edge>, want: &[(&str, &str)]) {
    let want: BTreeSet<Edge> = want.iter().map(|(a, b)| ((*a).into(), (*b).into())).collect();
    if *got == want {
        println!("  ok    {name}: {}", show(got));
    } else {
        *fails += 1;
        println!("  DIFF  {name}\n        got : {}\n        want: {}", show(got), show(&want));
    }
}

fn check_keys(fails: &mut u32, name: &str, got: &BTreeSet<String>, want: &[&str]) {
    let want: BTreeSet<String> = want.iter().map(|k| (*k).into()).collect();
    if *got == want {
        println!("  ok    {name}: {got:?}");
    } else {
        *fails += 1;
        println!("  DIFF  {name}\n        got : {got:?}\n        want: {want:?}");
    }
}

// ---- P1 ----

fn small(dir: &Path) -> Res<u32> {
    let mut g = open(fresh(dir, "small"), &KEY)?;
    g.set_try_limit(1)?;
    g.declare(&Declaration::Class(Class::new("person", ["name"])), &declared("person"))?;
    g.declare(&Declaration::Relation(Relation::new("follows", "person", "person", false)), &declared("follows"))?;
    g.declare(
        &Declaration::Class(Class { axis: vec!["build".into()], ..Class::new("part", ["name"]) }),
        &declared("part"),
    )?;
    g.declare(
        &Declaration::Relation(Relation { axis: vec!["build".into()], ..Relation::new("ships", "part", "part", false) }),
        &declared("ships"),
    )?;
    let person = |t: u64, k: &str| node_event(&id_at(t), "person", k, json!({"name": k}), None);
    let f = |t: u64, kind: &str, a: &str, b: &str| link_event(&id_at(t), kind, "follows", ("person", a, "person", b), None);
    let s = |t: u64, kind: &str, b: i64| link_event(&id_at(t), kind, "ships", ("part", "p", "part", "q"), Some(b));
    let named = ["a","b","c","d","h","h1","h2","h3","h4","h5","i","x","e","f","g","k","m","n","r","t","u","v"];
    let mut ev: Vec<String> = named.iter().filter(|k| **k != "r").map(|k| person(10, k)).collect();
    let rid = id_at(10);
    ev.push(node_event(&rid, "person", "r", json!({"name": "r"}), None));
    for (a, b) in [("a","b"),("b","c"),("c","a"),("d","d"),("h","h1"),("h","h2"),("h","h3"),("h","h4"),("h","h5"),("x","ghost"),("e","f"),("m","n"),("r","t")] {
        ev.push(f(11, "link", a, b));
    }
    ev.push(f(20, "ended", "e", "f"));
    ev.push(f(11, "link", "g", "k"));
    ev.push(f(14, "ended", "g", "k"));
    ev.push(f(18, "link", "g", "k"));
    ev.push(f(20, "corrected", "m", "n"));
    ev.push(f(11, "link", "u", "v"));
    ev.push(f(14, "ended", "u", "v"));
    ev.push(f(20, "corrected", "u", "v"));
    for k in ["p", "q"] {
        ev.push(node_event(&id_at(10), "part", k, json!({"name": k}), Some(1)));
    }
    ev.push(s(11, "link", 1));
    ev.push(s(12, "ended", 2));
    ev.push(s(13, "link", 3));
    feed(&mut g, &ev, 7)?;
    // Rename r to s, at 30, by amending the subject of the event that first named r.
    let amend = json!({"specversion":"1.0","id":id_at(30),"source":"rannoch:toy","type":"rannoch.amended",
        "data":{"target":{"source":"rannoch:toy","id":rid},"patch":{"subject":"s"}}}).to_string();
    feed(&mut g, &[amend], 1)?;

    let mut fails = 0;
    let mut all: BTreeSet<String> = named.iter().map(|k| (*k).to_string()).collect();
    all.extend(["ghost".to_string(), "s".to_string()]);
    let now = View::default();
    println!("the graph as read now:");
    let got = edges(&g, "person", "follows", &all, &now)?;
    println!("  {}", show(&got));
    let want = [("a","b"),("b","c"),("c","a"),("d","d"),("h","h1"),("h","h2"),("h","h3"),("h","h4"),("h","h5"),("x","ghost"),("g","k"),("s","t")];
    check(&mut fails, "follows out, now", &got, &want);
    check(&mut fails, "follows in, now", &in_edges(&g, "person", "follows", &all, &now)?, &want);
    let mut people: Vec<&str> = named.iter().copied().filter(|k| *k != "r").collect();
    people.push("s");
    check_keys(&mut fails, "nodes, now", &listed(&g, "person", &now)?, &people);
    let c = g.counts(&now)?;
    println!("  counts now: {:?}", c.links);
    if c.links.get("follows") != Some(&12) { fails += 1; println!("  DIFF  counts follows want 12"); }

    let v15 = at(15);
    let got = edges(&g, "person", "follows", &all, &v15)?;
    let want15 = [("a","b"),("b","c"),("c","a"),("d","d"),("h","h1"),("h","h2"),("h","h3"),("h","h4"),("h","h5"),("x","ghost"),("e","f"),("m","n"),("r","t")];
    check(&mut fails, "follows out, known-at 15 (g>k ended, e>f and m>n there)", &got, &want15);
    let got = edges(&g, "person", "follows", &all, &at(25))?;
    let want25 = [("a","b"),("b","c"),("c","a"),("d","d"),("h","h1"),("h","h2"),("h","h3"),("h","h4"),("h","h5"),("x","ghost"),("g","k"),("r","t")];
    check(&mut fails, "follows out, known-at 25 (before the rename)", &got, &want25);
    let names25 = listed(&g, "person", &at(25))?;
    if names25.contains("r") && !names25.contains("s") { println!("  ok    nodes at 25 list r, not s"); } else { fails += 1; println!("  DIFF  nodes at 25: {names25:?}"); }

    let keys: BTreeSet<String> = ["p".to_string(), "q".to_string()].into();
    for (b, want) in [(1, vec![("p","q")]), (2, vec![]), (3, vec![("p","q")])] {
        check(&mut fails, &format!("ships out, build {b}"), &edges(&g, "part", "ships", &keys, &point(Some(b), None))?, &want);
    }
    let d = g.describe(&NodeRef::new("person", "h"), &now)?.ok_or("h is there")?;
    println!("  describe h: {:?}", d.links);
    if d.links.get(&("follows".to_string(), Direction::Out)) != Some(&5) { fails += 1; println!("  DIFF  describe h out want 5"); }
    feed(&mut g, &[f(40, "link", "a", "b"), s(40, "link", 1)], 2)?;
    println!("  re-sent link, no axis: {} hearing(s); axis, same build: {} hearing(s)",
        g.link_hearings("follows", "a", "b")?.len(), g.link_hearings("ships", "p", "q")?.len());
    Ok(fails)
}

// ---- P2 ----

struct Truth {
    commits: BTreeMap<String, u64>,
    parents: BTreeSet<Edge>,
    touched: BTreeSet<Edge>,
    files: BTreeSet<String>,
}

fn run_git(repo: &str, args: &[&str]) -> Res<String> {
    let out = Command::new("/usr/bin/git").arg("-C").arg(repo).args(args).output()?;
    if !out.status.success() { return Err(String::from_utf8_lossy(&out.stderr).to_string().into()); }
    Ok(String::from_utf8(out.stdout)?)
}

fn truth(repo: &str) -> Res<Truth> {
    let mut t = Truth { commits: BTreeMap::new(), parents: BTreeSet::new(), touched: BTreeSet::new(), files: BTreeSet::new() };
    for line in run_git(repo, &["rev-list", "--all", "--parents", "--timestamp"])?.lines() {
        let mut parts = line.split(' ');
        let ts: u64 = parts.next().ok_or("ts")?.parse()?;
        let c = parts.next().ok_or("sha")?.to_string();
        for p in parts { t.parents.insert((c.clone(), p.to_string())); }
        t.commits.insert(c, ts);
    }
    let mut current = String::new();
    for line in run_git(repo, &["-c", "core.quotepath=off", "log", "--all", "--no-renames", "--name-only", "--format=@%H"])?.lines() {
        if let Some(h) = line.strip_prefix('@') { current = h.to_string(); }
        else if !line.is_empty() { t.touched.insert((current.clone(), line.to_string())); t.files.insert(line.to_string()); }
    }
    Ok(t)
}

fn year(ts: u64) -> String { (1970 + ts / 31_556_952).to_string() }

fn git_events(t: &Truth) -> Vec<String> {
    let mut ev = Vec::new();
    for (c, ts) in &t.commits {
        ev.push(node_event(&id_at(*ts), "commit", c, json!({"year": year(*ts)}), None));
    }
    let first = t.commits.values().min().copied().unwrap_or(1);
    for f in &t.files {
        ev.push(node_event(&id_at(first), "file", f, json!({"path": f}), None));
    }
    for (a, b) in &t.parents {
        ev.push(link_event(&id_at(t.commits[a]), "link", "parent", ("commit", a, "commit", b), None));
    }
    for (a, b) in &t.touched {
        ev.push(link_event(&id_at(t.commits[a]), "link", "touched", ("commit", a, "file", b), None));
    }
    ev
}

fn git_store(dir: &Path, ev: &[String], seed: u64) -> Res<Graph> {
    let mut g = open(fresh(dir, "git"), &KEY)?;
    g.set_try_limit(1)?;
    g.declare(&Declaration::Class(Class { indexed: BTreeSet::from(["year".to_string()]), ..Class::new("commit", ["year"]) }), &declared("commit"))?;
    g.declare(&Declaration::Class(Class::new("file", ["path"])), &declared("file"))?;
    g.declare(&Declaration::Relation(Relation::new("parent", "commit", "commit", false)), &declared("parent"))?;
    g.declare(&Declaration::Relation(Relation::new("touched", "commit", "file", false)), &declared("touched"))?;
    let mut ev = ev.to_vec();
    shuffle(&mut ev, seed);
    feed(&mut g, &ev, 5_000)?;
    Ok(g)
}

fn diff(fails: &mut u32, name: &str, got: &BTreeSet<Edge>, want: &BTreeSet<Edge>) {
    let extra = got.difference(want).count();
    let missing = want.difference(got).count();
    if extra + missing == 0 { println!("  ok    {name}: {} edges", got.len()); }
    else {
        *fails += 1;
        println!("  DIFF  {name}: {} got, {} want, {extra} extra, {missing} missing; first extra {:?}, first missing {:?}",
            got.len(), want.len(), got.difference(want).next(), want.difference(got).next());
    }
}

/// Every read of the graph at `view`, against `t` as cut to that view.
fn compare(g: &Graph, t: &Truth, view: &View, cut: Option<u64>, fails: &mut u32) -> Res<u64> {
    let keep = |c: &String| cut.is_none_or(|k| t.commits[c] <= k);
    let commits: BTreeSet<String> = t.commits.keys().filter(|c| keep(c)).cloned().collect();
    let parents: BTreeSet<Edge> = t.parents.iter().filter(|(a, _)| keep(a)).cloned().collect();
    let touched: BTreeSet<Edge> = t.touched.iter().filter(|(a, _)| keep(a)).cloned().collect();
    let all: BTreeSet<String> = t.commits.keys().cloned().collect();
    let got = listed(g, "commit", view)?;
    if got == commits { println!("  ok    commit nodes: {}", got.len()); } else { *fails += 1; println!("  DIFF  commit nodes: {} want {}", got.len(), commits.len()); }
    let f = listed(g, "file", view)?;
    println!("  file nodes listed: {}", f.len());
    diff(fails, "parent, read out", &edges(g, "commit", "parent", &all, view)?, &parents);
    diff(fails, "parent, read in", &in_edges(g, "commit", "parent", &all, view)?, &parents);
    diff(fails, "touched, read out", &edges(g, "commit", "touched", &all, view)?, &touched);
    diff(fails, "touched, read in", &in_edges(g, "file", "touched", &t.files, view)?, &touched);
    let c = g.counts(view)?;
    let cn = |m: &BTreeMap<String, u64>, k: &str| m.get(k).copied().unwrap_or(0);
    if cn(&c.nodes, "commit") == commits.len() as u64 && cn(&c.links, "parent") == parents.len() as u64 && cn(&c.links, "touched") == touched.len() as u64 {
        println!("  ok    counts: {:?} {:?}", c.nodes, c.links);
    } else { *fails += 1; println!("  DIFF  counts: {:?} {:?}", c.nodes, c.links); }
    let mut bad = 0u64;
    let mut asked = 0u64;
    let mut per: BTreeMap<&str, u64> = BTreeMap::new();
    for (a, _) in &parents { *per.entry(a).or_default() += 1; }
    let mut per_t: BTreeMap<&str, u64> = BTreeMap::new();
    for (a, _) in &touched { *per_t.entry(a).or_default() += 1; }
    for k in &commits {
        asked += 1;
        let d = g.describe(&NodeRef::new("commit", k), view)?.ok_or("commit described")?;
        let up = d.links.get(&("parent".to_string(), Direction::Out)).copied().unwrap_or(0);
        let tt = d.links.get(&("touched".to_string(), Direction::Out)).copied().unwrap_or(0);
        if up != per.get(k.as_str()).copied().unwrap_or(0) || tt != per_t.get(k.as_str()).copied().unwrap_or(0) { bad += 1; }
    }
    if bad == 0 { println!("  ok    describe: {asked} nodes"); } else { *fails += 1; println!("  DIFF  describe: {bad} of {asked} wrong"); }
    let mut by_year: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for c in &commits { by_year.entry(year(t.commits[c])).or_default().insert(c.clone()); }
    for (y, want) in &by_year {
        let filter = BTreeMap::from([("year".to_string(), Value::Text(y.clone()))]);
        let got: BTreeSet<String> = g.nodes("commit", &filter, view)?.into_iter().map(|(k, _)| k).collect();
        if &got == want { println!("  ok    nodes year={y}: {}", got.len()); } else { *fails += 1; println!("  DIFF  nodes year={y}: {} want {}", got.len(), want.len()); }
    }
    Ok(1 + 2 * all.len() as u64 + t.files.len() as u64)
}

fn real(repo: &str, dir: &Path) -> Res<u32> {
    let t = truth(repo)?;
    println!("truth from git: {} commits, {} files, {} parent links, {} touched links", t.commits.len(), t.files.len(), t.parents.len(), t.touched.len());
    let ev = git_events(&t);
    let start = Instant::now();
    let g = git_store(dir, &ev, 1)?;
    println!("filled {} events in {:.2?}", ev.len(), start.elapsed());
    let mut fails = 0;
    let started = Instant::now();
    println!("now:");
    let reads = compare(&g, &t, &View::default(), None, &mut fails)?;
    println!("  whole-graph check took {:.2?} (about {reads} link and node reads, plus describes)", started.elapsed());
    let mut times: Vec<u64> = t.commits.values().copied().collect();
    times.sort_unstable();
    let median = times[times.len() / 2];
    println!("at the median commit time ({median}):");
    compare(&g, &t, &at(median), Some(median), &mut fails)?;
    println!("a second store, another arrival order:");
    let h = git_store(dir, &ev, 2)?;
    compare(&h, &t, &View::default(), None, &mut fails)?;
    Ok(fails)
}

// ---- P3 ----

#[derive(Clone)]
struct Fact { point: i64, known: u64, id: String, live: bool, title: String }

/// The newest fact in the view, by point, then known-at, then event id.
fn newest<'a>(facts: &'a [Fact], a: Option<i64>, k: Option<u64>, axis: bool) -> Option<&'a Fact> {
    facts.iter()
        .filter(|f| k.is_none_or(|k| f.known <= k))
        .filter(|f| !axis || a.is_none_or(|a| f.point <= a))
        .max_by(|x, y| (x.point, x.known, &x.id).cmp(&(y.point, y.known, &y.id)))
}

fn random(dir: &Path, seeds: u64, resend: bool) -> Res<u32> {
    let keys: Vec<String> = (0..4).map(|i| format!("k{i}")).collect();
    let (mut c_ended, mut c_reopened, mut c_tie, mut c_resend_after_end) = (0u64, 0u64, 0u64, 0u64);
    let mut total = 0u64;
    let mut diffs = 0u32;
    let mut shown = 0;
    for seed in 0..seeds {
        let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut r = |n: u64| { s ^= s << 13; s ^= s >> 7; s ^= s << 17; s % n };
        let mut bodies = Vec::new();
        let mut nodes: BTreeMap<(bool, String), Vec<Fact>> = BTreeMap::new();
        let mut links: BTreeMap<(bool, String, String), Vec<Fact>> = BTreeMap::new();
        let mut sent: Vec<(bool, String, String, i64, bool)> = Vec::new();
        for n in 0..60u64 {
            let axis = r(2) == 0;
            let build = (r(5) + 1) as i64;
            let known = 100 + n;
            let id = id_at(known);
            let (class, rel) = if axis { ("p", "pr") } else { ("q", "qr") };
            let b = axis.then_some(build);
            let pt = if axis { build } else { known as i64 };
            match r(10) {
                0..=2 => {
                    let k = keys[r(4) as usize].clone();
                    let title = format!("t{n}");
                    bodies.push(node_event(&id, class, &k, json!({"name": title}), b));
                    nodes.entry((axis, k)).or_default().push(Fact { point: pt, known, id, live: true, title });
                }
                3..=5 => {
                    let (a, c) = (keys[r(4) as usize].clone(), keys[r(4) as usize].clone());
                    bodies.push(link_event(&id, "link", rel, (class, &a, class, &c), b));
                    links.entry((axis, a.clone(), c.clone())).or_default().push(Fact { point: pt, known, id, live: true, title: String::new() });
                    sent.push((axis, a, c, build, true));
                }
                6..=7 => {
                    let picked: Vec<_> = sent.iter().filter(|x| x.0 == axis).cloned().collect();
                    let (a, c) = if picked.is_empty() { (keys[r(4) as usize].clone(), keys[r(4) as usize].clone()) }
                        else { let p = &picked[r(picked.len() as u64) as usize]; (p.1.clone(), p.2.clone()) };
                    bodies.push(link_event(&id, "ended", rel, (class, &a, class, &c), b));
                    links.entry((axis, a, c)).or_default().push(Fact { point: pt, known, id, live: false, title: String::new() });
                }
                _ => {
                    if resend && !sent.is_empty() {
                        let (ax, a, c, bd, _) = sent[r(sent.len() as u64) as usize].clone();
                        let (cl, rl) = if ax { ("p", "pr") } else { ("q", "qr") };
                        let bb = ax.then_some(bd);
                        let pt2 = if ax { bd } else { known as i64 };
                        bodies.push(link_event(&id, "link", rl, (cl, &a, cl, &c), bb));
                        if links.get(&(ax, a.clone(), c.clone())).is_some_and(|f| f.iter().any(|x| !x.live)) { c_resend_after_end += 1; }
                        links.entry((ax, a, c)).or_default().push(Fact { point: pt2, known, id, live: true, title: String::new() });
                    }
                }
            }
        }
        for order in 0..3u64 {
            let mut g = open(fresh(dir, "rand"), &KEY)?;
            g.set_try_limit(1)?;
            g.declare(&Declaration::Class(Class { axis: vec!["build".into()], ..Class::new("p", ["name"]) }), &declared("p"))?;
            g.declare(&Declaration::Class(Class::new("q", ["name"])), &declared("q"))?;
            g.declare(&Declaration::Relation(Relation { axis: vec!["build".into()], ..Relation::new("pr", "p", "p", false) }), &declared("pr"))?;
            g.declare(&Declaration::Relation(Relation::new("qr", "q", "q", false)), &declared("qr"))?;
            let mut ev = bodies.clone();
            shuffle(&mut ev, seed * 7 + order);
            feed(&mut g, &ev, 9)?;
            let grid_a = [None, Some(1), Some(2), Some(3), Some(4), Some(5)];
            let grid_k = [None, Some(115), Some(130), Some(145)];
            for a in grid_a { for k in grid_k {
                let view = point(a, k);
                for axis in [true, false] {
                    if !axis && a.is_some() { continue; }
                    let (class, rel) = if axis { ("p", "pr") } else { ("q", "qr") };
                    let view_q = if axis { view.clone() } else { point(None, k) };
                    let want_nodes: BTreeMap<String, String> = nodes.iter().filter(|((x, _), _)| *x == axis)
                        .filter_map(|((_, key), f)| newest(f, a, k, axis).map(|n| (key.clone(), n.title.clone()))).collect();
                    let got_nodes: BTreeMap<String, String> = g.nodes(class, &BTreeMap::new(), &view_q)?.into_iter()
                        .map(|(key, v)| (key, match v.properties.get("name") { Some(Value::Text(t)) => t.clone(), o => format!("{o:?}") })).collect();
                    total += 1;
                    if want_nodes != got_nodes { diffs += 1; if shown < 8 { shown += 1; println!("  DIFF  seed {seed} order {order} nodes {class} view {a:?}/{k:?}\n        got  {got_nodes:?}\n        want {want_nodes:?}"); } }
                    let want: BTreeSet<Edge> = links.iter().filter(|((x, ..), _)| *x == axis)
                        .filter(|(_, f)| newest(f, a, k, axis).is_some_and(|n| n.live))
                        .map(|((_, f, t), _)| (f.clone(), t.clone())).collect();
                    for (_, f) in links.iter().filter(|((x, ..), _)| *x == axis) {
                        let seen: Vec<&Fact> = f.iter().filter(|x| k.is_none_or(|k| x.known <= k)).filter(|x| !axis || a.is_none_or(|a| x.point <= a)).collect();
                        if let Some(n) = newest(f, a, k, axis) {
                            if !n.live && seen.iter().any(|x| x.live) { c_ended += 1; }
                            if n.live && seen.iter().any(|x| !x.live) { c_reopened += 1; }
                            if seen.iter().any(|x| x.point == n.point && x.live != n.live) { c_tie += 1; }
                        }
                    }
                    let all: BTreeSet<String> = keys.iter().cloned().collect();
                    let got = edges(&g, class, rel, &all, &view_q)?;
                    let got_in = in_edges(&g, class, rel, &all, &view_q)?;
                    total += 2;
                    if got != want || got_in != want { diffs += 1; if shown < 8 { shown += 1;
                        let ex: Vec<_> = got.symmetric_difference(&want).take(3).cloned().collect();
                        println!("  DIFF  seed {seed} order {order} links {rel} view {a:?}/{k:?}: out {} in {} want {}; differ on {ex:?}", got.len(), got_in.len(), want.len());
                        if let Some((f, t)) = ex.first() { if let Some(fs) = links.get(&(axis, f.clone(), t.clone())) {
                            for x in fs { println!("          fact point {} known {} live {}", x.point, x.known, x.live); }
                        } }
                    } }
                    let c = g.counts(&view_q)?;
                    total += 1;
                    if c.links.get(rel).copied().unwrap_or(0) != want.len() as u64 || c.nodes.get(class).copied().unwrap_or(0) != want_nodes.len() as u64 {
                        diffs += 1; if shown < 8 { shown += 1; println!("  DIFF  seed {seed} order {order} counts {rel} view {a:?}/{k:?}: {:?} {:?} want links {} nodes {}", c.links, c.nodes, want.len(), want_nodes.len()); }
                    }
                }
            } }
        }
    }
    println!("  {total} read comparisons, {diffs} differences");
    println!("  coverage: {c_ended} links ended in a view, {c_reopened} reopened, {c_tie} live and ended at the same point, {c_resend_after_end} re-sends after an ending");
    Ok(diffs)
}

fn main() -> Res<()> {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map_or("", String::as_str);
    let dir = PathBuf::from(args.get(2).ok_or("usage: rannoch-toy-graph <small|git|random> <dir> [repo|seeds]")?);
    std::fs::create_dir_all(&dir)?;
    let fails = match cmd {
        "small" => small(&dir)?,
        "git" => real(args.get(3).ok_or("repo")?, &dir)?,
        "random" => {
            let n: u64 = args.get(3).map_or(Ok(20), |s| s.parse())?;
            println!("no re-sends:");
            let a = random(&dir, n, false)?;
            println!("with re-sends:");
            let b = random(&dir, n, true)?;
            a + b
        }
        other => return Err(format!("unknown command {other}").into()),
    };
    println!("\ndifferences: {fails}");
    Ok(())
}
