//! The relay toy: the importer's and the recorder's predictions on real game data. See README.md.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rannoch_cloudevents::{EventId, NodeRef};
use rannoch_queue::Queue;
use rannoch_relay::{Relays, WarmSet};
use rannoch_store::{
    Anchor, Class, Declaration, Direction, Graph, Relation, Roots, Shape, Step, Value, View, Way,
    open,
};
use rusqlite::Connection;
use serde_json::{Value as Json, json};

const KEY: [u8; 32] = [9; 32];
const OLD_DATE: &str = "2026-06-02";
const NEW_DATE: &str = "2026-06-18";
const OLD_POINT: i64 = 1_780_358_400_000;
const NEW_POINT: i64 = 1_781_740_800_000;

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Tally {
    failed: u32,
}

impl Tally {
    fn check(&mut self, name: &str, passed: bool, measured: impl std::fmt::Display) {
        println!("{} | {name} | {measured}", if passed { "PASS" } else { "FAIL" });
        if !passed {
            self.failed += 1;
        }
    }
}

fn declared(name: &str) -> String {
    json!({"specversion":"1.0","id":format!("d-{name}"),"source":"rannoch:toy","type":format!("rannoch.declared.{name}")}).to_string()
}

fn node(class: &str, key: &str, properties: &Json, extra: Option<(i64, &str)>) -> String {
    let mut data = json!({"properties": properties});
    if let Some((point, date)) = extra {
        data["axis"] = json!({"date": point});
        data["valid"] = json!({"on": date});
    }
    json!({"specversion":"1.0","id":format!("{}:0",EventId::now()),"source":"rannoch:toy",
           "type":format!("rannoch.node.{class}"),"subject":key,"data":data})
    .to_string()
}

fn link(kind: &str, relation: &str, from: (&str, &str), to: (&str, &str), extra: Option<(i64, &str)>) -> String {
    let mut data = json!({"from":{"class":from.0,"key":from.1},"to":{"class":to.0,"key":to.1}});
    if let Some((point, date)) = extra {
        data["axis"] = json!({"date": point});
        data["valid"] = json!({"on": date});
    }
    json!({"specversion":"1.0","id":format!("{}:0",EventId::now()),"source":"rannoch:toy",
           "type":format!("rannoch.{kind}.{relation}"),"data":data})
    .to_string()
}

fn step(relation: &str, cap: Option<usize>) -> Step {
    Step { cap, ..Step::new(relation) }
}

fn anchored(name: &str, class: &str, key: &str, steps: Vec<Step>) -> Shape {
    Shape {
        name: name.into(),
        roots: Roots::Node { node: Anchor { class: class.into(), key: key.into() } },
        steps,
    }
}

fn declare_all(g: &mut Graph, classes: Vec<Class>, relations: Vec<Relation>, shapes: Vec<Shape>) -> Res {
    for class in classes {
        let name = class.name.clone();
        g.declare(&Declaration::Class(class), &declared(&name))?;
    }
    for relation in relations {
        let name = relation.name.clone();
        g.declare(&Declaration::Relation(relation), &declared(&name))?;
    }
    for shape in shapes {
        let name = shape.name.clone();
        g.declare(&Declaration::Shape(shape), &declared(&name))?;
    }
    Ok(())
}

/// Commits and fills `bodies`, a batch at a time.
fn load(g: &mut Graph, bodies: &[String]) -> Res {
    for chunk in bodies.chunks(5_000) {
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        for landed in g.commit(&refs, &[])? {
            landed?;
        }
        g.fill()?;
    }
    let stopped = g.stopped()?;
    if !stopped.is_empty() {
        return Err(format!("{} events did not fill: {:?}", stopped.len(), stopped.first()).into());
    }
    Ok(())
}

struct Run {
    _dir: tempfile::TempDir,
    path: PathBuf,
    queue: Arc<Queue>,
    relays: Relays,
    changes: Arc<Mutex<Vec<Json>>>,
}

fn start(dir: tempfile::TempDir, path: PathBuf, g: Graph) -> Res<Run> {
    let queue = Arc::new(Queue::open(g, Queue::INTERVAL)?);
    let relays = Relays::open(dir.path().join("relays"), Arc::clone(&queue), &path, &KEY)?;
    let changes = Arc::new(Mutex::new(Vec::new()));
    let heard = Arc::clone(&changes);
    relays.subscribe(move |body| {
        if let (Ok(change), Ok(mut list)) = (serde_json::from_str(body), heard.lock()) {
            list.push(change);
        }
    });
    Ok(Run { _dir: dir, path, queue, relays, changes })
}

impl Run {
    fn ask(&self, shape: &str, view: &Json) -> Res<(String, Json)> {
        let query = json!({"specversion":"1.0","id":EventId::now().to_string(),"source":"toy",
                           "type":"rannoch.query","data":{"shape":shape,"view":view}})
        .to_string();
        let answer: Json = serde_json::from_str(&self.relays.answer(&query)?)?;
        let id = answer["data"]["result"]["relay"].as_str().ok_or("relay id")?.to_string();
        Ok((id, answer))
    }

    fn write(&self, bodies: &[String]) -> Res {
        for body in bodies {
            self.queue.accept(body.clone())?;
        }
        let (_, failures) = self.queue.flush();
        if !failures.is_empty() {
            return Err(format!("{failures:?}").into());
        }
        Ok(())
    }

    fn settled(&self, id: &str) -> Res<WarmSet> {
        let high = open(&self.path, &KEY)?.events().committed()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let set = self.relays.read(id).ok_or("relay")?;
            if set.cursor() >= high {
                return Ok(set);
            }
            if Instant::now() > deadline {
                return Err("the relay did not settle".into());
            }
            std::thread::sleep(Duration::from_micros(200));
        }
    }

    fn changes_now(&self) -> Vec<Json> {
        self.changes.lock().map(|c| c.clone()).unwrap_or_default()
    }
}

fn pct(mut samples: Vec<Duration>, p: f64) -> Duration {
    samples.sort();
    let at = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples.get(at).copied().unwrap_or_default()
}

fn keys(nodes: &[Arc<NodeRef>]) -> Vec<String> {
    nodes.iter().map(|n| n.key.clone()).collect()
}

fn in_graph(graph: &Graph, shape: &Shape, view: &View) -> Res<(BTreeSet<NodeRef>, Vec<Vec<String>>)> {
    let ontology = graph.current_ontology()?;
    let mut seen = BTreeSet::new();
    let mut frontier = Vec::new();
    if let Roots::Node { node } = &shape.roots {
        let root = NodeRef::new(node.class.clone(), node.key.clone());
        if graph.state_at(&root, view)?.is_some() {
            seen.insert(root.clone());
            frontier.push(root);
        }
    }
    let mut per_step = Vec::new();
    for s in &shape.steps {
        let (name, way) = s.way();
        let relation = ontology.relation(name)?;
        let mut found = BTreeMap::new();
        for at in &frontier {
            let mut ends = Vec::new();
            if way != Way::Backward && at.class == relation.from {
                ends.push((Direction::Out, relation.to.clone()));
            }
            if way != Way::Forward && at.class == relation.to {
                ends.push((Direction::In, relation.from.clone()));
            }
            for (direction, other) in ends {
                for (key, _) in graph.links(at, name, direction, view)? {
                    let reached = NodeRef::new(other.clone(), key.clone());
                    if seen.contains(&reached) {
                        continue;
                    }
                    if let Some(v) = graph.state_at(&reached, view)?
                        && s.passes(&v.properties)
                    {
                        found.insert(key, reached);
                    }
                }
            }
        }
        let taken: Vec<NodeRef> = found.into_values().take(s.cap.unwrap_or(usize::MAX)).collect();
        seen.extend(taken.iter().cloned());
        per_step.push(taken.iter().map(|n| n.key.clone()).collect());
        frontier = taken;
    }
    Ok((seen, per_step))
}

fn warm_nodes(set: &WarmSet) -> BTreeSet<NodeRef> {
    set.nodes().keys().map(|n| (**n).clone()).collect()
}

fn warm_steps(set: &WarmSet) -> Vec<Vec<String>> {
    set.steps().iter().map(|s| keys(&s.nodes)).collect()
}

fn rss_kb() -> u64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(0)
}

fn fresh() -> Res<(tempfile::TempDir, PathBuf)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("graph.rannoch");
    Ok((dir, path))
}

// --- The importer's T1 ---

fn importer_t1(spice: &Connection, tally: &mut Tally) -> Res {
    println!("\n# importer T1: cycles and exact counts");
    const NPC: &str = "npc.location.universal.invisible_npc";
    let (dir, path) = fresh()?;
    let mut g = open(&path, &KEY)?;
    let shapes = vec![
        anchored("speaks", "npc", NPC, vec![step("speaks_in", None)]),
    ];
    declare_all(
        &mut g,
        vec![Class::new("npc", ["n"]), Class::new("cnv", ["n"]), Class::new("quest", ["n"])],
        vec![
            Relation::new("speaks_in", "npc", "cnv", false),
            Relation::new("next_in_arc", "quest", "quest", false),
        ],
        shapes,
    )?;
    let mut bodies = Vec::new();
    let mut seen = BTreeSet::new();
    let mut links = Vec::new();
    let mut rows = spice.prepare("SELECT npc_fqn, cnv_fqn FROM conversation_npcs")?;
    for row in rows.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (npc, cnv) = row?;
        if seen.insert(("npc", npc.clone())) {
            bodies.push(node("npc", &npc, &json!({"n": npc}), None));
        }
        if seen.insert(("cnv", cnv.clone())) {
            bodies.push(node("cnv", &cnv, &json!({"n": cnv}), None));
        }
        links.push(link("link", "speaks_in", ("npc", &npc), ("cnv", &cnv), None));
    }
    let mut rows = spice.prepare("SELECT source_game_id, target_game_id FROM quest_chain")?;
    for row in rows.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (from, to) = row?;
        for quest in [&from, &to] {
            if seen.insert(("quest", quest.clone())) {
                bodies.push(node("quest", quest, &json!({"n": quest}), None));
            }
        }
        links.push(link("link", "next_in_arc", ("quest", &from), ("quest", &to), None));
    }
    bodies.extend(links);
    // A quest in a cycle, and its closure by SQL.
    let quest: String = spice.query_row(
        "SELECT a.source_game_id FROM quest_chain a JOIN quest_chain b
         ON a.source_game_id = b.target_game_id AND a.target_game_id = b.source_game_id
         ORDER BY 1 LIMIT 1",
        [],
        |r| r.get(0),
    )?;
    let closure: BTreeSet<String> = spice
        .prepare(
            "WITH RECURSIVE c(id) AS (SELECT ?1 UNION SELECT target_game_id FROM quest_chain
             JOIN c ON source_game_id = c.id) SELECT id FROM c",
        )?
        .query_map([&quest], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    let walk = anchored("arc", "quest", &quest, (0..50).map(|_| step("next_in_arc", Some(1000))).collect());
    let walk_body = declared("arc");
    g.declare(&Declaration::Shape(walk.clone()), &walk_body)?;
    let began = Instant::now();
    load(&mut g, &bodies)?;
    println!("loaded {} events in {:?}", bodies.len(), began.elapsed());
    let load_known = EventId::now().recorded()?;
    let run = start(dir, path.clone(), g)?;

    // Through the inbox: the query in, the answer back.
    let mut trips = Vec::new();
    let waiter = "toy.answers";
    let _ = run.queue.join(waiter);
    let mut first = None;
    for _ in 0..50 {
        let id = EventId::now().to_string();
        let query = json!({"specversion":"1.0","id":id,"source":"toy","type":"rannoch.query",
                           "data":{"shape":"speaks","view":{}}})
        .to_string();
        let began = Instant::now();
        run.queue.accept(query)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let answer: Json = 'found: loop {
            while let Some(event) = run.queue.next(waiter)? {
                let cursor = event.cursor;
                let read: Json = serde_json::from_str(&event.body)?;
                run.queue.done(waiter, cursor)?;
                if read["type"] == "rannoch.answer" && read["subject"] == json!(id) {
                    break 'found read;
                }
            }
            if Instant::now() > deadline {
                return Err("no answer".into());
            }
            std::thread::sleep(Duration::from_micros(200));
        };
        trips.push(began.elapsed());
        first.get_or_insert(answer);
        std::thread::sleep(Duration::from_millis(7));
    }
    let answer = first.ok_or("an answer")?;
    let known = answer["data"]["known"].as_i64().unwrap_or(0);
    let count = answer["data"]["result"]["steps"][0]["nodes"].as_u64();
    tally.check("count is 216", count == Some(216), format!("{count:?}"));
    tally.check(
        "known-at is present and not later than the load",
        known > 0 && known <= load_known,
        format!("known {known}, load finished {load_known}"),
    );
    tally.check(
        "round trip under 40 ms, inbox answer",
        pct(trips.clone(), 0.99) < Duration::from_millis(40),
        format!(
            "p50 {:?}, p95 {:?}, p99 {:?}, the first, which seeds, {:?}",
            pct(trips.clone(), 0.5),
            pct(trips.clone(), 0.95),
            pct(trips.clone(), 0.99),
            trips[0]
        ),
    );
    let reader = open(&path, &KEY)?;
    let began = Instant::now();
    let in_process = reader.links(&NodeRef::new("npc", NPC), "speaks_in", Direction::Out, &View::default())?;
    let took = began.elapsed();
    tally.check(
        "under 2 ms in process",
        in_process.len() == 216 && took < Duration::from_millis(2),
        format!("{} links in {took:?}", in_process.len()),
    );

    // The report.
    let relay = answer["data"]["result"]["relay"].as_str().ok_or("relay")?.to_string();
    let before = run.settled(&relay)?;
    let report = run.relays.report(&relay)?;
    let mut more = Vec::new();
    for i in 0..50 {
        let cnv = format!("cnv.toy.more.{i:02}");
        more.push(node("cnv", &cnv, &json!({"n": cnv}), None));
        more.push(link("link", "speaks_in", ("npc", NPC), ("cnv", &cnv), None));
    }
    run.write(&more)?;
    let fresh_count = run.settled(&relay)?.steps()[0].nodes.len();
    let began = Instant::now();
    let frozen = run.relays.reread(&report)?;
    let reread = began.elapsed();
    tally.check(
        "the report still says 216 at the old known-at",
        frozen.steps()[0].nodes.len() == 216 && frozen.known() == before.known(),
        format!("{} at {:?} (first answer {:?}), reread in {reread:?}", frozen.steps()[0].nodes.len(), frozen.known(), before.known()),
    );
    tally.check("a fresh count says 266", fresh_count == 266, fresh_count);

    // The walk.
    let (arc, ack) = run.ask("arc", &json!({}))?;
    let set = run.settled(&arc)?;
    let warm: BTreeSet<String> = set.nodes().keys().map(|n| n.key.clone()).collect();
    let steps = ack["data"]["result"]["steps"].as_array().ok_or("steps")?;
    let per_hop: u64 = steps.iter().filter_map(|s| s["nodes"].as_u64()).sum();
    tally.check(
        "the warm set equals the SQL closure",
        warm == closure,
        format!("{} warm, {} closure", warm.len(), closure.len()),
    );
    tally.check(
        "each node once, not cut",
        per_hop as usize + 1 == warm.len() && steps.iter().all(|s| s["cut"] == json!(false)),
        format!("{per_hop} over the hops + 1 root, {} distinct", warm.len()),
    );
    let quiet = run.changes_now().len();
    std::thread::sleep(Duration::from_millis(500));
    tally.check(
        "no change events while nothing commits",
        run.changes_now().len() == quiet,
        format!("{} before, {} after 500 ms idle", quiet, run.changes_now().len()),
    );
    Ok(())
}

// --- The importer's T2 ---

fn importer_t2(spice: &Connection, tally: &mut Tally) -> Res {
    println!("\n# importer T2: hub cut and determinism");
    const HUB: &str = "itm.mat.craft.endgame.stabilizer.tier2";
    let (dir, path) = fresh()?;
    let mut g = open(&path, &KEY)?;
    let steps = || vec![step("uses<-", Some(500)), step("crafts", None)];
    declare_all(
        &mut g,
        vec![Class::new("material", ["n"]), Class::new("schematic", ["n"]), Class::new("item", ["n"])],
        vec![
            Relation::new("uses", "schematic", "material", false),
            Relation::new("crafts", "schematic", "item", false),
        ],
        vec![anchored("hub", "material", HUB, steps()), anchored("hub2", "material", HUB, steps())],
    )?;
    let mut bodies = Vec::new();
    let mut seen = BTreeSet::new();
    let mut links = Vec::new();
    let mut rows = spice.prepare("SELECT schematic_fqn, material_fqn FROM schematic_materials")?;
    for row in rows.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (schematic, material) = row?;
        if seen.insert(("schematic", schematic.clone())) {
            bodies.push(node("schematic", &schematic, &json!({"n": schematic}), None));
        }
        if seen.insert(("material", material.clone())) {
            bodies.push(node("material", &material, &json!({"n": material}), None));
        }
        links.push(link("link", "uses", ("schematic", &schematic), ("material", &material), None));
    }
    let mut rows = spice.prepare("SELECT schematic_fqn, output_fqn FROM schematics WHERE output_fqn IS NOT NULL AND output_fqn != ''")?;
    for row in rows.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (schematic, item) = row?;
        if seen.insert(("schematic", schematic.clone())) {
            bodies.push(node("schematic", &schematic, &json!({"n": schematic}), None));
        }
        if seen.insert(("item", item.clone())) {
            bodies.push(node("item", &item, &json!({"n": item}), None));
        }
        links.push(link("link", "crafts", ("schematic", &schematic), ("item", &item), None));
    }
    bodies.extend(links);
    load(&mut g, &bodies)?;
    let lowest: Vec<String> = spice
        .prepare("SELECT schematic_fqn FROM schematic_materials WHERE material_fqn = ?1 ORDER BY 1 LIMIT 500")?
        .query_map([HUB], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    let run = start(dir, path, g)?;
    let rss_before = rss_kb();
    let began = Instant::now();
    let (id, ack) = run.ask("hub", &json!({}))?;
    let seeded = began.elapsed();
    let rss_after = rss_kb();
    let file = std::fs::metadata(run._dir.path().join("relays").join(format!("{id}.json")))?.len();
    let first = &ack["data"]["result"]["steps"][0];
    tally.check(
        "hop 1 cut, 703 found, 203 dropped",
        *first == json!({"nodes": 500, "cut": true, "dropped": 203}),
        first,
    );
    let set = run.settled(&id)?;
    tally.check(
        "the warm set holds the 500 lowest keys",
        keys(&set.steps()[0].nodes) == lowest,
        format!("{} keys, first {:?}", set.steps()[0].nodes.len(), set.steps()[0].nodes.first().map(|n| &n.key)),
    );
    let (shared, _) = run.ask("hub", &json!({}))?;
    tally.check("one shared relay for the same shape", shared == id, &shared);
    let (twin, _) = run.ask("hub2", &json!({}))?;
    let other = run.settled(&twin)?;
    tally.check(
        "a second relay on the same definition holds identical nodes in the same order",
        warm_steps(&other) == warm_steps(&set) && warm_nodes(&other) == warm_nodes(&set),
        format!("{} and {} nodes", other.nodes().len(), set.nodes().len()),
    );
    println!(
        "memory at the largest hub (703 found, {} held in the walk): resident {} kB -> {} kB, file {} bytes, seed {:?}",
        set.nodes().len(), rss_before, rss_after, file, seeded
    );

    // Boundary churn.
    let before = run.changes_now().len();
    let old_500th = lowest.last().ok_or("a 500th key")?.clone();
    let key = "aaa.toy.below_the_cut";
    run.write(&[
        node("schematic", key, &json!({"n": key}), None),
        link("link", "uses", ("schematic", key), ("material", HUB), None),
    ])?;
    let set = run.settled(&id)?;
    run.settled(&twin)?;
    let changes: Vec<Json> = run.changes_now().into_iter().skip(before).filter(|c| c["subject"] == json!(id)).collect();
    let names = |list: &str| -> BTreeSet<String> {
        changes.iter().flat_map(|c| c["data"][list].as_array().cloned().unwrap_or_default())
            .filter_map(|e| e["node"]["key"].as_str().map(str::to_string)).collect()
    };
    tally.check(
        "one change event carries both moves",
        changes.len() == 1 && names("entered").contains(key) && names("left").contains(&old_500th),
        format!("{} event(s), in {:?}, out {:?}", changes.len(), names("entered").iter().filter(|k| k.starts_with("aaa") || k.starts_with("sch")).collect::<Vec<_>>(), names("left").contains(&old_500th)),
    );
    tally.check(
        "dropped becomes 204, the warm step stays at 500, the old 500th left",
        set.steps()[0].dropped == 204 && set.steps()[0].nodes.len() == 500 && !set.nodes().keys().any(|n| n.key == old_500th),
        format!("dropped {}, nodes {}", set.steps()[0].dropped, set.steps()[0].nodes.len()),
    );
    Ok(())
}

// --- The importer's T3 ---

type Edges = BTreeSet<(String, String)>;

fn chain(spice: &Connection) -> Res<Edges> {
    Ok(spice
        .prepare("SELECT source_game_id, target_game_id FROM quest_chain")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?)
}

fn closure(edges: &Edges, root: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::from([root.to_string()]);
    let mut frontier = vec![root.to_string()];
    while let Some(at) = frontier.pop() {
        for (_, to) in edges.range((at.clone(), String::new())..).take_while(|(f, _)| *f == at) {
            if seen.insert(to.clone()) {
                frontier.push(to.clone());
            }
        }
    }
    seen
}

fn importer_t3(new_spice: &Connection, old_spice: &Connection, tally: &mut Tally) -> Res {
    println!("\n# importer T3: two builds, as change events");
    let old = chain(old_spice)?;
    let mut new = chain(new_spice)?;
    println!("v26 {} links, 7.9.a {} links, {} new, {} gone", old.len(), new.len(), new.difference(&old).count(), old.difference(&new).count());
    // The root whose neighbourhood changes most without being all of it, with an old link to end.
    let quests: BTreeSet<&String> = old.iter().map(|(f, _)| f).collect();
    let root = quests
        .iter()
        .map(|q| (q, closure(&old, q), closure(&new, q)))
        .filter(|(_, was, _)| was.len() >= 3)
        .max_by_key(|(_, was, is)| is.difference(was).count().min(100) * 10 + was.len())
        .map(|(q, ..)| (*q).clone())
        .ok_or("a quest whose arc changed")?;
    let ended = old
        .iter()
        .find(|(f, _)| *f != root && closure(&old, &root).contains(f))
        .cloned()
        .or_else(|| old.iter().find(|(f, _)| *f == root).cloned())
        .ok_or("a link to end")?;
    new.remove(&ended);
    let was = closure(&old, &root);
    let is = closure(&new, &root);
    let entered: BTreeSet<String> = is.difference(&was).cloned().collect();
    let left: BTreeSet<String> = was.difference(&is).cloned().collect();
    println!("root {root}: {} before, {} after, {} in, {} out; ended link (synthetic) {ended:?}", was.len(), is.len(), entered.len(), left.len());

    let (dir, path) = fresh()?;
    let mut g = open(&path, &KEY)?;
    let mut quest = Class::new("quest", ["n"]);
    quest.axis = vec!["date".into()];
    let mut next = Relation::new("next_in_arc", "quest", "quest", false);
    next.axis = vec!["date".into()];
    declare_all(
        &mut g,
        vec![quest],
        vec![next],
        vec![anchored("arc", "quest", &root, (0..50).map(|_| step("next_in_arc", None)).collect())],
    )?;
    let build_old = (OLD_POINT, OLD_DATE);
    let build_new = (NEW_POINT, NEW_DATE);
    let mut seen = BTreeSet::new();
    let mut bodies = Vec::new();
    for (from, to) in &old {
        for q in [from, to] {
            if seen.insert(q.clone()) {
                bodies.push(node("quest", q, &json!({"n": q}), Some(build_old)));
            }
        }
    }
    for (from, to) in &old {
        bodies.push(link("link", "next_in_arc", ("quest", from), ("quest", to), Some(build_old)));
    }
    load(&mut g, &bodies)?;
    let run = start(dir, path.clone(), g)?;
    let (latest, _) = run.ask("arc", &json!({}))?;
    let (first, _) = run.ask("arc", &json!({"axis": [OLD_POINT]}))?;
    tally.check("two views are two relays", latest != first, "");
    let at_old = run.settled(&latest)?;
    let warm_keys = |set: &WarmSet| set.nodes().keys().map(|n| n.key.clone()).collect::<BTreeSet<_>>();
    tally.check("at the old build the warm set is the old closure", warm_keys(&at_old) == was, at_old.nodes().len());

    // The 7.9 commit.
    let mut commit = Vec::new();
    for (from, to) in new.difference(&old) {
        for q in [from, to] {
            if seen.insert(q.clone()) {
                commit.push(node("quest", q, &json!({"n": q}), Some(build_new)));
            }
        }
    }
    for (from, to) in new.difference(&old) {
        commit.push(link("link", "next_in_arc", ("quest", from), ("quest", to), Some(build_new)));
    }
    commit.push(link("ended", "next_in_arc", ("quest", &ended.0), ("quest", &ended.1), Some(build_new)));
    let before = run.changes_now().len();
    run.write(&commit)?;
    let now = run.settled(&latest)?;
    let held = run.settled(&first)?;
    let changes: Vec<Json> = run.changes_now().into_iter().skip(before).collect();
    let on_latest: Vec<&Json> = changes.iter().filter(|c| c["subject"] == json!(latest)).collect();
    let on_first: Vec<&Json> = changes.iter().filter(|c| c["subject"] == json!(first)).collect();
    let moved = |list: &str| -> (BTreeSet<String>, bool) {
        let mut names = BTreeSet::new();
        let mut valid = true;
        for change in &on_latest {
            for entry in change["data"][list].as_array().into_iter().flatten() {
                names.insert(entry["node"]["key"].as_str().unwrap_or_default().to_string());
                valid &= entry["valid"] == json!({"on": NEW_DATE});
            }
        }
        (names, valid)
    };
    let (came, came_valid) = moved("entered");
    let (went, went_valid) = moved("left");
    tally.check(
        "the change events equal the SQL diff",
        came == entered && went == left,
        format!("{} in / {} predicted, {} out / {} predicted, {} event(s)", came.len(), entered.len(), went.len(), left.len(), on_latest.len()),
    );
    tally.check("7.9's valid time on each", came_valid && went_valid, format!("in {came_valid}, out {went_valid}"));
    tally.check("the ended link is out of the latest warm set", warm_keys(&now) == is, format!("{} nodes", now.nodes().len()));
    tally.check(
        "the 7.8.1 relay emits nothing and holds the old closure",
        on_first.is_empty() && warm_keys(&held) == was,
        format!("{} event(s), {} nodes", on_first.len(), held.nodes().len()),
    );
    let reader = open(&path, &KEY)?;
    let shape = reader.current_ontology()?.shape("arc")?.clone();
    let (cold_new, _) = in_graph(&reader, &shape, &View::default())?;
    let (cold_old, _) = in_graph(&reader, &shape, &View { axis: Some(vec![OLD_POINT]), known: None })?;
    tally.check(
        "both warm sets equal a cold recompute",
        warm_nodes(&now) == cold_new && warm_nodes(&held) == cold_old,
        format!("{} and {}", cold_new.len(), cold_old.len()),
    );
    Ok(())
}

// --- The recorder's ---

fn location(fqn: &str) -> Option<&str> {
    let (_, rest) = fqn.split_once(".location.")?;
    rest.split('.').next()
}

fn recorder(spice: &Connection, tally: &mut Tally) -> Res {
    println!("\n# recorder: the planet relay (stand-in: rooted on the importer's own planet key)");
    let (dir, path) = fresh()?;
    let mut g = open(&path, &KEY)?;
    let mut thing = Class::new("thing", ["kind", "fqn"]);
    thing.properties.insert("logname".into(), rannoch_store::ValueType::Integer);
    thing.indexed = BTreeSet::from(["logname".to_string()]);
    let shape = anchored(
        "planet",
        "planet",
        "hutta",
        vec![
            Step {
                drop: Some(BTreeMap::from([("kind".to_string(), Value::from("Spawn"))])),
                ..Step::new("on<-")
            },
            Step::new("targets"),
        ],
    );
    declare_all(
        &mut g,
        vec![Class::new("planet", ["n"]), thing],
        vec![
            Relation::new("on", "thing", "planet", false),
            Relation::new("targets", "thing", "thing", false),
        ],
        vec![shape.clone()],
    )?;
    let mut bodies = Vec::new();
    let mut links = Vec::new();
    let mut planets = BTreeSet::new();
    let mut things = BTreeSet::new();
    let mut hutta: BTreeMap<String, (String, Option<i64>)> = BTreeMap::new();
    let mut rows = spice.prepare(
        "SELECT fqn, kind, string_id FROM objects WHERE fqn LIKE '%.location.%' AND is_canonical = 1
         AND kind IN ('Npc','Quest','Placeable','Codex','Conversation','Encounter','Phase','Spawn')",
    )?;
    for row in rows.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, Option<i64>>(2)?)))? {
        let (fqn, kind, string_id) = row?;
        let Some(planet) = location(&fqn).map(str::to_string) else { continue };
        if !things.insert(fqn.clone()) {
            continue;
        }
        if planets.insert(planet.clone()) {
            bodies.push(node("planet", &planet, &json!({"n": planet}), None));
        }
        let logname = string_id.map(|id| id << 32);
        let properties = match logname {
            Some(name) => json!({"kind": kind, "fqn": fqn, "logname": {"integer": name}}),
            None => json!({"kind": kind, "fqn": fqn}),
        };
        bodies.push(node("thing", &fqn, &properties, None));
        links.push(link("link", "on", ("thing", &fqn), ("planet", &planet), None));
        if planet == "hutta" && kind != "Spawn" {
            hutta.insert(fqn.clone(), (kind.clone(), logname));
        }
    }
    let mut rows = spice.prepare("SELECT DISTINCT quest_fqn, target_fqn FROM quest_objectives WHERE target_fqn IS NOT NULL")?;
    let mut targets: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for row in rows.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (quest, target) = row?;
        if things.contains(&quest) && things.contains(&target) {
            links.push(link("link", "targets", ("thing", &quest), ("thing", &target), None));
            targets.entry(quest).or_default().insert(target);
        }
    }
    bodies.extend(links);
    let began = Instant::now();
    load(&mut g, &bodies)?;
    println!("loaded {} events ({} things, {} planets) in {:?}", bodies.len(), things.len(), planets.len(), began.elapsed());
    let mut sql: BTreeSet<String> = hutta.keys().cloned().collect();
    let first_step = sql.clone();
    for at in &first_step {
        if let Some(reached) = targets.get(at) {
            sql.extend(reached.iter().cloned());
        }
    }
    let run = start(dir, path.clone(), g)?;
    let began = Instant::now();
    let (id, ack) = run.ask("planet", &json!({}))?;
    let seeded = began.elapsed();
    let set = run.settled(&id)?;
    println!("ack: {}", ack["data"]["result"]["steps"]);
    tally.check("about 1,000 nodes", (700..=1500).contains(&set.nodes().len()), set.nodes().len());
    tally.check("the Hutta relay seeds under 1 s", seeded < Duration::from_secs(1), format!("{seeded:?}"));
    let reader = open(&path, &KEY)?;
    let (cold, cold_steps) = in_graph(&reader, &shape, &View::default())?;
    let warm_keys: BTreeSet<String> = set.nodes().keys().filter(|n| n.class == "thing").map(|n| n.key.clone()).collect();
    tally.check(
        "it equals a cold recompute and the SQL",
        warm_nodes(&set) == cold && warm_steps(&set) == cold_steps && warm_keys == sql,
        format!("{} warm, {} cold, {} SQL things", set.nodes().len(), cold.len(), sql.len()),
    );

    // read() and a read by indexed name.
    let names: Vec<i64> = hutta.values().filter_map(|(_, n)| *n).collect();
    let mut reads = Vec::new();
    let mut by_name = Vec::new();
    for i in 0..20_000 {
        let name = Value::Integer(names[i % names.len()]);
        let began = Instant::now();
        let got = run.relays.read(&id).ok_or("relay")?;
        reads.push(began.elapsed());
        let found = got.by("logname", &name).len();
        let total = began.elapsed();
        by_name.push(total);
        if found == 0 {
            return Err("a name with nothing".into());
        }
    }
    tally.check(
        "read() under 0.05 ms",
        pct(reads.clone(), 0.99) < Duration::from_micros(50),
        format!("p50 {:?}, p99 {:?}", pct(reads.clone(), 0.5), pct(reads, 0.99)),
    );
    tally.check(
        "read() by logname under 0.05 ms",
        pct(by_name.clone(), 0.99) < Duration::from_micros(50),
        format!("p50 {:?}, p99 {:?}", pct(by_name.clone(), 0.5), pct(by_name, 0.99)),
    );

    // Applying an event: from the commit's subscriber call to the cursor passing it.
    let quest = hutta.iter().find(|(_, (kind, _))| kind == "Quest").map(|(fqn, _)| fqn.clone()).ok_or("a quest")?;
    // Each its own target: one link heard over and over reads slower for its own history, which
    // is the store's cost for that link and not the relay's. Half link, half end what was linked.
    let targets: Vec<String> = (0..150).map(|i| format!("spn.toy.churn.{i:03}")).collect();
    run.write(
        &targets
            .iter()
            .map(|fqn| node("thing", fqn, &json!({"kind": "Spawn", "fqn": fqn}), None))
            .collect::<Vec<_>>(),
    )?;
    let (per_event, walked_nodes) = apply_cost(&run, &id, &reader, &quest, &targets)?;
    tally.check(
        "applying an event under 1 ms",
        pct(per_event.clone(), 0.5) < Duration::from_millis(1),
        format!("p50 {:?}, p99 {:?}, over {} commits, {} nodes", pct(per_event.clone(), 0.5), pct(per_event.clone(), 0.99), per_event.len(), walked_nodes),
    );

    // Line to overlay.
    let stop = Arc::new(AtomicBool::new(false));
    let mut drains = Vec::new();
    for w in 0..4 {
        let queue = Arc::clone(&run.queue);
        let stop = Arc::clone(&stop);
        drains.push(std::thread::spawn(move || {
            let name = format!("toy.drain.{w}");
            let _ = queue.join(&name);
            while !stop.load(Ordering::Relaxed) {
                match queue.next(&name) {
                    Ok(Some(event)) => {
                        let _ = queue.done(&name, event.cursor);
                    }
                    _ => std::thread::sleep(Duration::from_millis(1)),
                }
            }
        }));
    }
    let heard = Arc::new(Mutex::new(BTreeMap::<String, Instant>::new()));
    {
        let heard = Arc::clone(&heard);
        run.relays.subscribe(move |body| {
            let at = Instant::now();
            if let (Ok(change), Ok(mut heard)) = (serde_json::from_str::<Json>(body), heard.lock()) {
                for delta in change["data"]["changed"].as_array().into_iter().flatten() {
                    if let Some(after) = delta["after"].as_str() {
                        heard.insert(after.to_string(), at);
                    }
                }
            }
        });
    }
    let mut lines = Vec::new();
    for i in 0..400 {
        let marker = format!("line-{i}");
        let began = Instant::now();
        run.queue.accept_from(
            node("thing", &quest, &json!({"kind": marker, "fqn": quest}), None),
            "toy.lines",
            i.to_string(),
        )?;
        let deadline = began + Duration::from_secs(5);
        loop {
            let at = heard.lock().map_err(|_| "lock")?.get(&marker).copied();
            if let Some(at) = at {
                lines.push(at.duration_since(began));
                break;
            }
            if Instant::now() > deadline {
                return Err(format!("no change for {marker}").into());
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        std::thread::sleep(Duration::from_millis(13));
    }
    stop.store(true, Ordering::Relaxed);
    for drain in drains {
        let _ = drain.join();
    }
    tally.check(
        "line to overlay change event under 60 ms p99 with 4 workers running",
        pct(lines.clone(), 0.99) < Duration::from_millis(60),
        format!("p50 {:?}, p99 {:?}, max {:?}, {} lines", pct(lines.clone(), 0.5), pct(lines.clone(), 0.99), pct(lines.clone(), 1.0), lines.len()),
    );

    // After all that, and an amendment, and a restart with the file gone.
    let cold_now = |reader: &Graph| in_graph(reader, &shape, &View::default());
    let set = run.settled(&id)?;
    tally.check("after the burst it equals a cold recompute", warm_nodes(&set) == cold_now(&reader)?.0, set.nodes().len());
    let Run { _dir: dir, path, queue, relays, changes: _ } = run;
    let failures = relays.close();
    tally.check("nothing failed on the relays' thread", failures.is_empty(), format!("{failures:?}"));
    drop(queue);
    let relays_dir = dir.path().join("relays");
    let file: PathBuf = relays_dir.join(format!("{id}.json"));
    let bytes = std::fs::metadata(&file)?.len();
    std::fs::remove_file(&file)?;
    let g = open(&path, &KEY)?;
    let began = Instant::now();
    let run = start(dir, path, g)?;
    let reseeded = began.elapsed();
    let set = run.settled(&id)?;
    tally.check(
        "with its file gone the relay rebuilds to a cold recompute",
        warm_nodes(&set) == cold_now(&reader)?.0,
        format!("reopened in {reseeded:?}; its file was {bytes} bytes"),
    );
    Ok(())
}

/// One link per commit on a relay of about 1,000 nodes, timed from the moment the commit is told
/// to its subscribers to the relay's change callback, which comes right after it publishes: no
/// reader spins on the relay meanwhile, as a reader spinning on read() competes with the publish.
fn apply_cost(
    run: &Run,
    id: &str,
    reader: &Graph,
    quest: &str,
    targets: &[String],
) -> Res<(Vec<Duration>, usize)> {
    let told = Arc::new(Mutex::new(None::<Instant>));
    {
        let told = Arc::clone(&told);
        run.queue.subscribe(move |_| {
            if let Ok(mut told) = told.lock() {
                *told = Some(Instant::now());
            }
        });
    }
    let heard = Arc::new(Mutex::new(None::<Instant>));
    {
        let heard = Arc::clone(&heard);
        run.relays.subscribe(move |_| {
            if let Ok(mut heard) = heard.lock() {
                *heard = Some(Instant::now());
            }
        });
    }
    let mut samples = Vec::new();
    for (kind, target) in targets.iter().map(|t| ("link", t)).chain(targets.iter().map(|t| ("ended", t))) {
        *heard.lock().map_err(|_| "lock")? = None;
        run.queue.accept(link(kind, "targets", ("thing", quest), ("thing", target), None))?;
        let _ = run.queue.flush();
        let deadline = Instant::now() + Duration::from_secs(5);
        let done = loop {
            if let Some(at) = *heard.lock().map_err(|_| "lock")? {
                break at;
            }
            if Instant::now() > deadline {
                return Err("no change".into());
            }
            std::thread::sleep(Duration::from_micros(500));
        };
        if let Some(at) = *told.lock().map_err(|_| "lock")? {
            samples.push(done.saturating_duration_since(at));
        }
        std::thread::sleep(Duration::from_millis(3));
    }
    let _ = reader;
    let set = run.settled(id)?;
    Ok((samples, set.nodes().len()))
}

fn main() -> Res {
    let args: Vec<String> = std::env::args().collect();
    let (Some(new), Some(old)) = (args.get(1), args.get(2)) else {
        return Err("usage: relay-toy <spice-7.9.a-v7.sqlite> <spice-7.8.1.c-v26.sqlite> [only]".into());
    };
    let only = args.get(3).map(String::as_str);
    let new_spice = Connection::open_with_flags(Path::new(new), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let old_spice = Connection::open_with_flags(Path::new(old), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut tally = Tally { failed: 0 };
    if only.is_none_or(|o| o == "t1") {
        importer_t1(&new_spice, &mut tally)?;
    }
    if only.is_none_or(|o| o == "t2") {
        importer_t2(&new_spice, &mut tally)?;
    }
    if only.is_none_or(|o| o == "t3") {
        importer_t3(&new_spice, &old_spice, &mut tally)?;
    }
    if only.is_none_or(|o| o == "recorder") {
        recorder(&new_spice, &mut tally)?;
    }
    println!("\n{} failed", tally.failed);
    if tally.failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
