//! Run 4: legion's real memory on rannoch-store. Loads the 9,682-reflection slice the 0.4.0 toy
//! used (kept local, never committed), checks the live sets and follows links, then amends and
//! renames real linked reflections and checks reads before and after.
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rannoch_store::{At, Class, Declaration, Graph, HistoryKey, Relation, open};
use serde_json::{Value, json};

const KEY: [u8; 32] = [42; 32];

#[derive(serde::Deserialize)]
struct Row {
    id: String,
    repo: String,
    text: String,
    created_at: String,
    domain: String,
    parent: Option<String>,
    archived_at: Option<String>,
}

/// "2026-10-27T11:33:00.123456+00:00" -> milliseconds since the epoch.
fn ms(s: &str) -> u64 {
    let n = |a: usize, b: usize| s[a..b].parse::<u64>().expect("digits");
    let (y, m, d) = (n(0, 4) as i64, n(5, 7), n(8, 10));
    let y2 = if m <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = (y2 - era * 400) as u64;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let days = (era * 146_097 + (yoe * 365 + yoe / 4 - yoe / 100 + doy) as i64 - 719_468) as u64;
    let millis = s.get(20..23).and_then(|v| v.parse().ok()).unwrap_or(0);
    (days * 86_400 + n(11, 13) * 3600 + n(14, 16) * 60 + n(17, 19)) * 1000 + millis
}

fn id_at_ms(ms: u64) -> String {
    let at = uuid::Timestamp::from_unix(uuid::NoContext, ms / 1000, ((ms % 1000) * 1_000_000) as u32);
    format!("{}:0", uuid::Uuid::new_v7(at))
}

fn ev(id: &str, ty: &str, subject: &str, data: Value) -> String {
    json!({"specversion":"1.0","id":id,"source":"legion:memory","type":ty,"subject":subject,"data":data})
        .to_string()
}

fn link(id: &str, rel: &str, from: (&str, &str), to: (&str, &str)) -> String {
    ev(id, &format!("rannoch.link.{rel}"), from.1, json!({"from":{"class":from.0,"key":from.1},"to":{"class":to.0,"key":to.1},"valid":"unknown"}))
}

fn text(g: &Graph, key: &str, at: Option<At>) -> Option<String> {
    let node = match at {
        None => g.find("reflection", key).expect("find"),
        Some(at) => {
            let day = rannoch_store::Date::day(2026, 12, 31);
            g.find_as_of("reflection", key, &day, Some(at)).expect("as of")
        }
    };
    node.and_then(|n| n.properties.get("text").cloned()).map(|v| match v {
        rannoch_store::Value::Text(t) => t,
        o => format!("{o:?}"),
    })
}

fn live(g: &Graph, rel: &str, from: &str, to: &str) -> Option<bool> {
    let far = At::Time(UNIX_EPOCH + Duration::from_secs(9_000_000_000));
    g.link_state_at(rel, from, to, far).expect("link").map(|v| !v.ended)
}

fn main() {
    let slice = std::env::args().nth(1).expect("slice.jsonl");
    let path = std::env::args().nth(2).unwrap_or_else(|| "runs/memory.rannoch".into());
    for f in [path.clone(), format!("{path}-wal"), format!("{path}-shm")] {
        let _ = std::fs::remove_file(f);
    }
    let rows: Vec<Row> = std::fs::read_to_string(&slice)
        .expect("slice")
        .lines()
        .map(|l| serde_json::from_str(l).expect("row"))
        .collect();
    let ids: BTreeSet<&str> = rows.iter().map(|r| r.id.as_str()).collect();
    let decl = |n: &str| {
        format!(r#"{{"specversion":"1.0","id":"d-{n}","source":"legion:memory","type":"rannoch.declared.{n}"}}"#)
    };
    let mut g = open(&path, &KEY).expect("open");
    g.declare(&Declaration::Class(Class::new("repo", ["name"])), &decl("repo")).expect("repo");
    g.declare(
        &Declaration::Class(Class { history: HistoryKey::Time, ..Class::new("reflection", ["text", "domain"]) }),
        &decl("reflection"),
    )
    .expect("reflection");
    g.declare(&Declaration::Relation(Relation::new("in", "reflection", "repo", false)), &decl("in")).expect("in");
    g.declare(&Declaration::Relation(Relation::new("follows", "reflection", "reflection", false)), &decl("follows"))
        .expect("follows");

    // Load: repos, then each reflection with its in-link (at its own creation), its follows link,
    // and an ending of its in-link at its archive time.
    let t0 = Instant::now();
    let repos: BTreeSet<&str> = rows.iter().map(|r| r.repo.as_str()).collect();
    let mut bodies: Vec<String> = repos
        .iter()
        .map(|r| ev(&id_at_ms(1_700_000_000_000), "rannoch.node.repo", r, json!({"properties":{"name":r},"valid":"unknown"})))
        .collect();
    for r in &rows {
        let c = ms(&r.created_at);
        bodies.push(ev(&id_at_ms(c), "rannoch.node.reflection", &r.id,
            json!({"properties":{"text":r.text,"domain":r.domain},"valid":{"on":&r.created_at[0..10]},"time":c})));
        bodies.push(link(&id_at_ms(c), "in", ("reflection", &r.id), ("repo", &r.repo)));
        if let Some(p) = r.parent.as_deref().filter(|p| ids.contains(p)) {
            bodies.push(link(&id_at_ms(c), "follows", ("reflection", &r.id), ("reflection", p)));
        }
        if let Some(a) = &r.archived_at {
            let at = ms(a);
            bodies.push(ev(&id_at_ms(at), "rannoch.ended.in", &r.id,
                json!({"from":{"class":"reflection","key":r.id},"to":{"class":"repo","key":r.repo},"at":at,"valid":"unknown"})));
        }
    }
    let mut refused = 0;
    for chunk in bodies.chunks(500) {
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        refused += g.commit(&refs, &[]).expect("commit").iter().filter(|r| r.is_err()).count();
        g.fill().expect("fill");
    }
    let load = t0.elapsed();
    println!("load {} events in {:.1}s, refused at commit {refused}, stopped {}", bodies.len(), load.as_secs_f64(), g.stopped().expect("s").len());

    // P4.1: every reflection's text; the live set per repo; follows links.
    let mut text_bad = 0;
    for r in &rows {
        if text(&g, &r.id, None).as_deref() != Some(r.text.as_str()) {
            text_bad += 1;
        }
    }
    let mut live_want: BTreeMap<&str, u64> = BTreeMap::new();
    let mut live_got: BTreeMap<&str, u64> = BTreeMap::new();
    for r in &rows {
        if r.archived_at.is_none() {
            *live_want.entry(&r.repo).or_default() += 1;
        }
        if live(&g, "in", &r.id, &r.repo) == Some(true) {
            *live_got.entry(&r.repo).or_default() += 1;
        }
    }
    let follows_want: Vec<&Row> = rows.iter().filter(|r| r.parent.as_deref().is_some_and(|p| ids.contains(p))).collect();
    let follows_got = follows_want
        .iter()
        .filter(|r| live(&g, "follows", &r.id, r.parent.as_deref().expect("p")) == Some(true))
        .count();
    println!("P4.1 text mismatches {text_bad} of {} | live sets equal {} want {live_want:?} got {live_got:?} | follows {follows_got}/{}",
        rows.len(), live_want == live_got, follows_want.len());

    // P4.2: amend the text of 200 reflections that have a follows child, rename 100 others with a
    // follows child, then check reads now and as known before.
    let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for r in &follows_want {
        children.entry(r.parent.as_deref().expect("p")).or_default().push(&r.id);
    }
    let parents: Vec<&str> = children.keys().copied().collect();
    let by_id: BTreeMap<&str, &Row> = rows.iter().map(|r| (r.id.as_str(), r)).collect();
    let log: BTreeMap<String, String> = g
        .events()
        .since(rannoch_store::Cursor::START)
        .expect("events")
        .into_iter()
        .filter_map(|e| {
            let v: Value = serde_json::from_str(&e.body).ok()?;
            let subject = v["subject"].as_str()?.to_owned();
            (v["type"] == "rannoch.node.reflection").then(|| (subject, e.id.to_string()))
        })
        .collect();
    let before = At::Time(SystemTime::now());
    std::thread::sleep(Duration::from_millis(20));
    let (amend, rename): (Vec<&str>, Vec<&str>) = (parents.iter().take(200).copied().collect(), parents.iter().skip(200).take(100).copied().collect());
    let mut am: Vec<String> = Vec::new();
    for k in &amend {
        let patch = json!({"data":{"properties":{"text":format!("amended: {k}")}}});
        am.push(ev(&format!("{}:0", uuid::Uuid::now_v7()), "rannoch.amended", &log[*k], patch));
    }
    for k in &rename {
        am.push(ev(&format!("{}:0", uuid::Uuid::now_v7()), "rannoch.amended", &log[*k], json!({"subject": format!("renamed-{k}")})));
    }
    let t1 = Instant::now();
    for chunk in am.chunks(50) {
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        g.commit(&refs, &[]).expect("commit");
        g.fill().expect("fill");
    }
    let amend_time = t1.elapsed();
    let mut bad: Vec<String> = Vec::new();
    for k in &amend {
        if text(&g, k, None) != Some(format!("amended: {k}")) {
            bad.push(format!("amend now {k}"));
        }
        if text(&g, k, Some(before)).as_deref() != Some(by_id[k].text.as_str()) {
            bad.push(format!("amend before {k}"));
        }
    }
    for k in &rename {
        let new = format!("renamed-{k}");
        let row = by_id[k];
        if text(&g, &new, None).as_deref() != Some(row.text.as_str()) {
            bad.push(format!("rename new key {k}"));
        }
        if text(&g, k, None).is_some() {
            bad.push(format!("rename old key answers {k}"));
        }
        if text(&g, k, Some(before)).as_deref() != Some(row.text.as_str()) {
            bad.push(format!("rename before {k}"));
        }
        if live(&g, "in", &new, &row.repo) != Some(row.archived_at.is_none()) {
            bad.push(format!("rename in-link {k}: {:?}", live(&g, "in", &new, &row.repo)));
        }
        for c0 in &children[k] {
            // A child renamed too answers under its own new key.
            let c = &if rename.contains(c0) { format!("renamed-{c0}") } else { (*c0).to_owned() };
            if live(&g, "follows", c, &new) != Some(true) || live(&g, "follows", c, k).is_some() {
                bad.push(format!("rename follows {c}->{k}"));
            }
            if g.link_known_at("follows", c0, k, before).expect("k").is_none() {
                bad.push(format!("rename follows known before {c}->{k}"));
            }
        }
    }
    println!("P4.2 amended {} renamed {} in {:.1}s ({:.1} ms each), stopped {}, mismatches {}",
        amend.len(), rename.len(), amend_time.as_secs_f64(), amend_time.as_secs_f64() * 1e3 / am.len() as f64,
        g.stopped().expect("s").len(), bad.len());
    let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
    for b in &bad {
        *kinds.entry(b.split(' ').take(2).collect::<Vec<_>>().join(" ")).or_default() += 1;
    }
    println!("  kinds {kinds:?}");
    for b in bad.iter().filter(|b| !b.starts_with("amend before")).take(6) {
        println!("  {b}");
    }
    let probe = parents[500];
    println!("  probe unamended {probe}: now {:?} | as of before {:?}", text(&g, probe, None).map(|t| t.len()), text(&g, probe, Some(before)).map(|t| t.len()));
    let k = amend[0];
    println!("  probe amended {k}: as of before {:?} | as of now {:?}", text(&g, k, Some(before)).map(|t| t.chars().take(30).collect::<String>()), text(&g, k, Some(At::Time(SystemTime::now()))).map(|t| t.chars().take(30).collect::<String>()));
}
