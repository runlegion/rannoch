//! The chain toy: a whole service as a chain of flows, each firing on the type of an event an
//! earlier step emitted, with two ontologies in one store. Each subcommand runs part of
//! PREDICTIONS.md.
//!
//! ```sh
//! chain-toy run      <westwind dir> <out dir>   # the chain once, in order, and its checks
//! chain-toy replays  <westwind dir> <out dir>   # predictions 6, 9 and 12
//! chain-toy age      <westwind dir> <out dir>   # prediction 10
//! chain-toy domain   <westwind dir> <out dir>   # prediction 5
//! chain-toy load     <westwind dir> <out dir>   # prediction 13
//! ```

mod entrance;
mod flow;
mod host;
mod rows;
mod runtime;
mod schema;
mod store;
mod world;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusqlite::Connection;
use serde_json::Value;

use crate::entrance::{Event, admit};
use crate::flow::{Flow, Runner};
use crate::runtime::{Handler, Runtime, Start, check_domain};
use crate::schema::Schemas;
use crate::store::Store;
use crate::world::{DAY, Inbound, LATENESS, World};

type Result<T> = std::result::Result<T, String>;

const FLOWS: &[(&str, &str)] = &[
    ("accept", include_str!("../flows/accept.jsonl")),
    ("allocate", include_str!("../flows/allocate.jsonl")),
    ("stock", include_str!("../flows/stock.jsonl")),
    ("pickpack", include_str!("../flows/pickpack.jsonl")),
    ("ship", include_str!("../flows/ship.jsonl")),
    ("deliver", include_str!("../flows/deliver.jsonl")),
    ("aftership", include_str!("../flows/aftership.jsonl")),
    ("support", include_str!("../flows/support.jsonl")),
    ("returns", include_str!("../flows/returns.jsonl")),
    ("cancel", include_str!("../flows/cancel.jsonl")),
    ("subscription", include_str!("../flows/subscription.jsonl")),
];
const SCHEMAS: &str = include_str!("../schemas.json");

/// How many arrivals between checkpoints.
const EVERY: usize = 512;
const LIMIT: u32 = 3;

fn load_average() -> String {
    std::process::Command::new("sysctl").args(["-n", "vm.loadavg"]).output().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default()
}

fn building() -> String {
    let out = std::process::Command::new("pgrep").args(["-lx", "rustc|cargo|clang|ld|cc"]).output();
    let names: Vec<String> = out
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().filter_map(|l| l.split_whitespace().nth(1).map(str::to_string)).collect())
        .unwrap_or_default();
    if names.is_empty() { "nothing else building".into() } else { format!("also running: {}", names.join(",")) }
}

/// The runtime with the store first, then every flow, resuming from `dir`.
fn chain(dir: &Path, carry: bool) -> Result<Runtime> {
    let mut runtime = Runtime::new(Schemas::load(SCHEMAS)?, LATENESS, Some(dir))?;
    let path = dir.join("store.rannoch");
    runtime.add("store", &store::patterns(), Box::new(move || Ok(Box::new(Store::open(&path)?) as Box<dyn Handler>)), LIMIT)?;
    for (name, text) in FLOWS {
        let mut flow = Flow::parse(text)?;
        if !carry {
            flow.carry = None;
        }
        let patterns = flow.patterns();
        let histories = dir.join("flows");
        let start: Start = Box::new(move || Ok(Box::new(Runner::start(flow.clone(), Some(&histories), 2 * LATENESS)?) as Box<dyn Handler>));
        runtime.add(name, &patterns, start, LIMIT)?;
    }
    Ok(runtime)
}

fn fresh(out: &Path, name: &str) -> Result<PathBuf> {
    let dir = out.join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| e.to_string())?;
    }
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(dir)
}

/// Feeds `inbound` into the chain in `dir`, checkpointing every [`EVERY`] arrivals, and returns
/// the runtime.
fn feed(dir: &Path, inbound: &[Inbound], carry: bool) -> Result<Runtime> {
    let mut runtime = chain(dir, carry)?;
    for (i, item) in inbound.iter().enumerate() {
        runtime.admit(&item.line)?;
        if (i + 1) % EVERY == 0 {
            runtime.checkpoint()?;
        }
    }
    runtime.finish()?;
    runtime.checkpoint()?;
    Ok(runtime)
}

/// Every released event in the log, once each by id.
fn read_log(dir: &Path) -> Result<Vec<Event>> {
    let text = std::fs::read_to_string(dir.join("log.jsonl")).map_err(|e| e.to_string())?;
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for line in text.lines() {
        let event = admit(line, 0)?;
        if seen.insert(event.id.clone()) {
            out.push(event);
        }
    }
    Ok(out)
}

/// Each story's trail, rebuilt from the log alone by following causation back to its root.
fn trails(log: &[Event], world: &World) -> BTreeMap<String, Vec<(i64, String)>> {
    let by_id: HashMap<&str, &Event> = log.iter().map(|e| (e.id.as_str(), e)).collect();
    let mut root_of: HashMap<&str, Option<&str>> = HashMap::new();
    fn find<'a>(id: &'a str, by_id: &HashMap<&'a str, &'a Event>, memo: &mut HashMap<&'a str, Option<&'a str>>, depth: usize) -> Option<&'a str> {
        if let Some(r) = memo.get(id) {
            return *r;
        }
        let root = match by_id.get(id).and_then(|e| e.causation.as_deref()) {
            None => Some(id),
            Some(_) if depth > 100_000 => None,
            Some(cause) => by_id.get(cause).map(|c| c.id.as_str()).and_then(|c| find(c, by_id, memo, depth + 1)),
        };
        memo.insert(id, root);
        root
    }
    let roots: HashMap<&str, &str> = world.stories.iter().map(|s| (s.root.as_str(), s.who.as_str())).collect();
    let mut out: BTreeMap<String, Vec<(i64, String)>> = BTreeMap::new();
    let at: HashMap<&str, i64> = world.stories.iter().map(|s| (s.root.as_str(), s.at)).collect();
    for event in log {
        if let Some(root) = find(&event.id, &by_id, &mut root_of, 0)
            && let Some(who) = roots.get(root)
        {
            out.entry((*who).to_string()).or_default().push((event.time - at[root], event.ty.trim_start_matches("westwind.").to_string()));
        }
    }
    for trail in out.values_mut() {
        trail.sort();
    }
    out
}

/// The store's contents as text, by key, leaving out arrival times and rowids.
fn store_text(path: &Path) -> Result<String> {
    let conn = Connection::open(path).map_err(|e| e.to_string())?;
    let err = |e: rusqlite::Error| e.to_string();
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND (name LIKE 'class:%' OR name LIKE 'relation:%') ORDER BY name")
        .map_err(err)?
        .query_map([], |r| r.get(0))
        .map_err(err)?
        .collect::<std::result::Result<_, _>>()
        .map_err(err)?;
    let mut text = String::new();
    for table in tables {
        let columns: Vec<String> = conn
            .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
            .map_err(err)?
            .query_map([], |r| r.get(0))
            .map_err(err)?
            .collect::<std::result::Result<_, _>>()
            .map_err(err)?;
        let sql = if table.starts_with("class:") {
            let keep: Vec<String> = columns.iter().filter(|c| *c != "id" && *c != "recorded").map(|c| format!("\"{c}\"")).collect();
            format!("SELECT {} FROM \"{table}\" ORDER BY key", keep.join(", "))
        } else {
            let from = conn
                .query_row("SELECT sql FROM sqlite_schema WHERE name = ?1", [&table], |r| r.get::<_, String>(0))
                .map_err(err)?;
            let refs: Vec<&str> = from.split("REFERENCES \"").skip(1).map(|s| s.split('"').next().unwrap_or("")).collect();
            format!(
                "SELECT a.key, b.key, r.source, r.valid FROM \"{table}\" r JOIN \"{}\" a ON a.id = r.src JOIN \"{}\" b ON b.id = r.dst ORDER BY 1, 2",
                refs.first().copied().unwrap_or(""),
                refs.get(1).copied().unwrap_or("")
            )
        };
        text.push_str(&format!("== {table}\n"));
        let mut statement = conn.prepare(&sql).map_err(err)?;
        let n = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..n).map(|i| row.get::<_, rusqlite::types::Value>(i).map(|v| format!("{v:?}"))).collect::<std::result::Result<Vec<_>, _>>()
            })
            .map_err(err)?;
        for row in rows {
            text.push_str(&row.map_err(err)?.join("|"));
            text.push('\n');
        }
    }
    text.push_str("== hearings\n");
    let mut statement = conn
        .prepare("SELECT entity, key, to_key, event, source, valid, hex(axis), known, properties FROM _hearings ORDER BY entity, key, to_key, event")
        .map_err(err)?;
    let rows = statement
        .query_map([], |row| (0..9).map(|i| row.get::<_, rusqlite::types::Value>(i).map(|v| format!("{v:?}"))).collect::<std::result::Result<Vec<_>, _>>())
        .map_err(err)?;
    for row in rows {
        text.push_str(&row.map_err(err)?.join("|"));
        text.push('\n');
    }
    Ok(text)
}

/// Every flow's instances, rebuilt from their histories, as text.
fn flow_text(dir: &Path) -> Result<String> {
    let mut text = String::new();
    for (name, flow) in FLOWS {
        let runner = Runner::start(Flow::parse(flow)?, Some(&dir.join("flows")), 2 * LATENESS)?;
        text.push_str(&format!("== {name}\n{}", runner.state()));
    }
    Ok(text)
}

fn digest(text: &str) -> String {
    blake3::hash(text.as_bytes()).to_hex()[..16].to_string()
}

/// What a run's outcomes are: the outside-facing events each order and ticket ended with.
fn outcomes(log: &[Event]) -> BTreeMap<String, Vec<String>> {
    const ALERTS: &[&str] = &[
        "order.accepted", "order.abandoned", "order.cancelled", "cancel.refused", "order.packed", "order.shipped", "order.unshipped",
        "order.delivered", "order.late", "order.lost", "order.spoiled", "order.closed", "refund.issued", "ticket.triaged",
        "ticket.closed", "remake.requested", "remake.ordered", "remake.expired", "compensation.issued", "return.authorized",
        "return.received", "return.expired", "shipment.redirected", "address.updated", "line.backordered", "supplier.ordered",
        "stock.released",
    ];
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for e in log {
        let ty = e.ty.trim_start_matches("westwind.");
        if ALERTS.contains(&ty) {
            out.entry(e.subject.clone().unwrap_or_default()).or_default().push(format!("{} {ty}", e.time));
        }
    }
    for v in out.values_mut() {
        v.sort();
    }
    out
}

/// Checks predictions 1 to 4, 6 to 8 and 11 on one in-order run.
#[allow(clippy::too_many_lines)]
fn run(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    println!("seed.sql sha256 matches {}; {}; load {}", rows::SEED_SHA256, building(), load_average());
    let world = world::build(&seed)?;
    println!("world: {} arrivals, {} stories", world.inbound.len(), world.stories.len());
    let dir = fresh(out, "in-order")?;
    let started = Instant::now();
    let runtime = feed(&dir, &world.inbound, true)?;
    println!(
        "ran in {:.1}s: admitted {}, released {}, duplicates dropped {}, queued at end {}; load {}",
        started.elapsed().as_secs_f64(),
        runtime.admitted,
        runtime.released_count,
        runtime.duplicates,
        runtime.queued(),
        load_average()
    );
    let log = read_log(&dir)?;

    // 1. Trails.
    let trails = trails(&log, &world);
    let mut matched = 0;
    for story in &world.stories {
        let mut expected: Vec<(i64, String)> = story.expected.iter().map(|(o, t)| (*o, t.clone())).collect();
        expected.sort();
        let got = trails.get(&story.who).cloned().unwrap_or_default();
        if got == expected {
            matched += 1;
            println!("P1 {:26} trail of {:4} events matches", story.who, got.len());
        } else {
            println!("P1 {:26} trail DIFFERS: expected {} events, got {}", story.who, expected.len(), got.len());
            let e: BTreeSet<_> = expected.iter().collect();
            let g: BTreeSet<_> = got.iter().collect();
            for x in e.difference(&g).take(8) {
                println!("     missing {} {}", x.0 as f64 / DAY as f64, x.1);
            }
            for x in g.difference(&e).take(8) {
                println!("     extra   {} {}", x.0 as f64 / DAY as f64, x.1);
            }
        }
    }
    println!("P1 {matched} of {} trails match", world.stories.len());

    // 2. Seed outcomes against SQL over the seed.
    let mut by_order: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut delivered_at: HashMap<String, i64> = HashMap::new();
    for e in &log {
        if let Some(order) = e.data.get("order_id").and_then(Value::as_str) {
            by_order.entry(order.to_string()).or_default().insert(e.ty.clone());
            if e.ty == "westwind.order.delivered" {
                delivered_at.insert(order.to_string(), e.time);
            }
        }
    }
    let mut statement = seed.prepare("SELECT id, shipped_date, required_date FROM orders").map_err(|e| e.to_string())?;
    let rows: Vec<(String, Option<String>, Option<String>)> = statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(|e| e.to_string())?
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| e.to_string())?;
    let (mut agree, mut differ) = (0, Vec::new());
    let (mut d, mut u, mut l) = (0, 0, 0);
    for (id, shipped, required) in &rows {
        let types = by_order.get(id).cloned().unwrap_or_default();
        let has = |t: &str| types.contains(&format!("westwind.{t}"));
        let ok = match shipped {
            Some(s) => {
                let at = entrance::date_ms(s)? + 3 * DAY;
                let late = required.as_deref().map(entrance::date_ms).transpose()?.is_some_and(|r| at > r);
                d += 1;
                if late {
                    l += 1;
                }
                has("order.delivered") && delivered_at.get(id) == Some(&at) && has("order.late") == late && !has("order.unshipped") && has("order.closed")
            }
            None => {
                u += 1;
                has("order.unshipped") && !has("order.delivered")
            }
        };
        if ok { agree += 1 } else { differ.push(id.clone()) }
    }
    println!("P2 seed orders: {agree} of {} match SQL (SQL: {d} delivered, {l} of them late, {u} unshipped); differing {:?}", rows.len(), &differ[..differ.len().min(5)]);

    // 3. Every emitted event a valid CloudEvent whose data passes its schema.
    let schemas = Schemas::load(SCHEMAS)?;
    let emitted: Vec<&Event> = log.iter().filter(|e| e.source.contains("/flow/")).collect();
    let mut failing = runtime.invalid.len();
    for e in &emitted {
        if admit(&entrance::to_json(e), 0).is_err() || schemas.check(&e.ty, &e.data).is_err() {
            failing += 1;
        }
    }
    let kinds: BTreeSet<&str> = emitted.iter().map(|e| e.ty.as_str()).collect();
    println!("P3 {} emitted events of {} types: {failing} invalid", emitted.len(), kinds.len());

    // 4. Ontologies: bins by reason, and no hearing in another domain's classes.
    println!("P4 bin by reason: {:?}", runtime.bin_counts);
    for (line, id, reason) in &runtime.bin {
        println!("     {:34} {:?} {}", reason.kind(), id.as_deref().unwrap_or("-"), &line[..line.len().min(90)]);
    }
    let conn = Connection::open(dir.join("store.rannoch")).map_err(|e| e.to_string())?;
    let count = |sql: &str| -> Result<i64> { conn.query_row(sql, [], |r| r.get(0)).map_err(|e| e.to_string()) };
    let cross = count("SELECT COUNT(*) FROM _hearings WHERE (entity LIKE '%:westwind_%' AND source NOT LIKE 'westwind/%') OR (entity LIKE '%:oz_%' AND source NOT LIKE 'oz/%')")?;
    let in_oz = count("SELECT COUNT(*) FROM _hearings WHERE entity LIKE '%:oz_%'")?;
    let in_ww = count("SELECT COUNT(*) FROM _hearings WHERE entity LIKE '%:westwind_%'")?;
    let scarecrow = count("SELECT COUNT(*) FROM \"class:westwind_order\" WHERE key IN (SELECT key FROM \"class:oz_order\")")?;
    println!("P4 hearings: Westwind {in_ww}, Oz {in_oz}, in another domain's classes {cross}; Scarecrow's order in Westwind: {scarecrow}");

    // 6. Hodor.
    let tickets = count("SELECT COUNT(*) FROM \"class:westwind_ticket\" WHERE \"p:text\" = 'Hodor. Hodor hodor.'")?;
    println!("P6 duplicates dropped {}; Hodor's identical tickets stored as {tickets} tickets", runtime.duplicates);

    // 7. Tin Man.
    for h in &runtime.handlers {
        if h.restarts > 0 || !h.bin.is_empty() {
            println!("P7 handler {} restarted {} times, binned {}; reasons {:?}", h.name, h.restarts, h.bin.len(), h.reasons);
        }
    }
    let effects = host::effects();
    let tin: Vec<_> = effects.iter().filter(|((a, _), _)| a == "triage").map(|(_, n)| *n).collect();
    println!("P7 triage work per ticket: {:?} (each ticket's triage done this many times)", tin);

    // 8. Lion.
    for who in ["Lion, abandoned", "Lion, unanswered"] {
        let closes: Vec<String> = trails.get(who).map(|t| t.iter().filter(|(_, ty)| ty.ends_with("expired") || ty.ends_with("abandoned")).map(|(o, ty)| format!("{ty} at day {}", *o as f64 / DAY as f64)).collect()).unwrap_or_default();
        println!("P8 {who}: {closes:?}");
    }
    println!("P9 largest lateness seen in order: {} ms", runtime.max_lateness);
    std::fs::write(dir.join("store.txt"), store_text(&dir.join("store.rannoch"))?).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("flows.txt"), flow_text(&dir)?).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("outcomes.txt"), format!("{:#?}", outcomes(&log))).map_err(|e| e.to_string())?;
    println!("store {}, flows {}, outcomes {}", digest(&store_text(&dir.join("store.rannoch"))?), digest(&flow_text(&dir)?), digest(&format!("{:?}", outcomes(&log))));
    Ok(())
}

fn summary(dir: &Path) -> Result<(String, String, String)> {
    let log = read_log(dir)?;
    Ok((digest(&store_text(&dir.join("store.rannoch"))?), digest(&flow_text(dir)?), digest(&format!("{:?}", outcomes(&log)))))
}

/// The child a kill test runs and kills: the world in `order`, resuming from `dir`.
fn child(westwind: &Path, dir: &Path, order: &str) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let world = world::build(&seed)?;
    let inbound = ordered(&world, order)?;
    feed(dir, &inbound, true).map(|_| ())
}

fn ordered(world: &World, order: &str) -> Result<Vec<Inbound>> {
    match order.strip_prefix("shuffled:") {
        None if order == "in" => Ok(world.inbound.clone()),
        Some(seed) => Ok(world.shuffled_within(LATENESS, seed.parse().map_err(|_| "bad seed")?)),
        None => Err(format!("no order {order}")),
    }
}

/// Predictions 6, 9 and 12.
#[allow(clippy::too_many_lines)]
fn replays(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let world = world::build(&seed)?;
    println!("{}; load {}", building(), load_average());
    let mut table = Vec::new();
    let base = fresh(out, "base")?;
    feed(&base, &world.inbound, true)?;
    table.push(("in order x1".to_string(), summary(&base)?, String::new()));

    for times in [10usize, 1000] {
        let dir = fresh(out, &format!("x{times}"))?;
        let mut runtime = chain(&dir, true)?;
        let t = Instant::now();
        for pass in 0..times {
            for (i, item) in world.inbound.iter().enumerate() {
                runtime.admit(&item.line)?;
                if (i + 1) % EVERY == 0 {
                    runtime.checkpoint()?;
                }
            }
            if pass == 0 {
                runtime.finish()?;
            }
            runtime.checkpoint()?;
        }
        let note = format!("{:.0}s; bins {:?}; duplicates {}", t.elapsed().as_secs_f64(), runtime.bin_counts, runtime.duplicates);
        drop(runtime);
        table.push((format!("in order x{times}"), summary(&dir)?, note));
    }

    // Stopped in process: drop everything at random arrivals and start again from what is durable.
    {
        let dir = fresh(out, "stopped")?;
        let mut rng = rows::Rng::new(17);
        let mut cuts: Vec<usize> = (0..8).map(|_| usize::try_from(rng.below(world.inbound.len() as u64)).unwrap_or(0)).collect();
        cuts.sort_unstable();
        for cut in cuts {
            let mut runtime = chain(&dir, true)?;
            for (i, item) in world.inbound[..cut].iter().enumerate() {
                runtime.admit(&item.line)?;
                if (i + 1) % EVERY == 0 {
                    runtime.checkpoint()?;
                }
            }
        }
        feed(&dir, &world.inbound, true)?;
        table.push(("stopped x8, in process".into(), summary(&dir)?, String::new()));
    }

    // Killed: the child is killed at random moments, then finishes.
    for (name, order) in [("killed x6, in order", "in"), ("killed x6, shuffled 1", "shuffled:1")] {
        let dir = fresh(out, &name.replace([' ', ','], "-"))?;
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let mut rng = rows::Rng::new(29);
        let mut landed = 0;
        for _ in 0..6 {
            let mut child = std::process::Command::new(&exe)
                .args(["child", &westwind.display().to_string(), &dir.display().to_string(), order])
                .spawn()
                .map_err(|e| e.to_string())?;
            std::thread::sleep(std::time::Duration::from_millis(80 + rng.below(1100)));
            if child.try_wait().map_err(|e| e.to_string())?.is_none() {
                landed += 1;
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        let status = std::process::Command::new(&exe)
            .args(["child", &westwind.display().to_string(), &dir.display().to_string(), order])
            .status()
            .map_err(|e| e.to_string())?;
        if !status.success() {
            return Err(format!("{name}: the last child failed"));
        }
        table.push((name.to_string(), summary(&dir)?, format!("{landed} of 6 kills landed mid-run")));
    }

    // Shuffled within the lateness.
    for s in [1u64, 2, 3] {
        let dir = fresh(out, &format!("shuffled-{s}"))?;
        let inbound = world.shuffled_within(LATENESS, s);
        let runtime = feed(&dir, &inbound, true)?;
        let note = format!("largest lateness {:.2} days; bins {:?}", runtime.max_lateness as f64 / DAY as f64, runtime.bin_counts);
        drop(runtime);
        table.push((format!("shuffled within lateness, seed {s}"), summary(&dir)?, note));
    }

    // Hodor's duplicates left out: the same world without the second and third copies.
    {
        let dir = fresh(out, "no-duplicates")?;
        let mut seen = BTreeSet::new();
        let inbound: Vec<Inbound> = world
            .inbound
            .iter()
            .filter(|i| admit(&i.line, 0).map(|e| seen.insert(e.id)).unwrap_or(true))
            .cloned()
            .collect();
        feed(&dir, &inbound, true)?;
        table.push(("without the duplicate copies".into(), summary(&dir)?, format!("{} fewer arrivals", world.inbound.len() - inbound.len())));
    }

    println!("\n{:38} {:17} {:17} {:17}  note", "run", "store", "flow states", "outcomes");
    for (name, (s, f, o), note) in &table {
        println!("{name:38} {s:17} {f:17} {o:17}  {note}");
    }
    Ok(())
}

/// Prediction 10: Tyrion's subscription instance rebuilt at year 1, 5 and 10, with and without
/// carrying its state into a fresh history.
fn age(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let world = world::build(&seed)?;
    let tyrion = world.stories.iter().find(|s| s.who == "Tyrion").ok_or("no Tyrion")?.clone();
    let sub_key = admit(&world.inbound.iter().find(|i| i.line.contains(&tyrion.root)).ok_or("no root")?.line, 0)?.data["subscription_id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    println!("{}; load {}", building(), load_average());
    for carry in [true, false] {
        let dir = fresh(out, if carry { "age-carry" } else { "age-no-carry" })?;
        let mut runtime = chain(&dir, carry)?;
        let marks: Vec<(i64, i64)> = [1, 5, 10].iter().map(|y| (*y, tyrion.at + y * 360 * DAY)).collect();
        let mut next = 0;
        for (i, item) in world.inbound.iter().enumerate() {
            runtime.admit(&item.line)?;
            if (i + 1) % EVERY == 0 {
                runtime.checkpoint()?;
            }
            if next < marks.len() && item.time >= marks[next].1 + LATENESS + DAY {
                runtime.checkpoint()?;
                let mut runner = Runner::start(Flow::parse(FLOWS.iter().find(|f| f.0 == "subscription").ok_or("no flow")?.1)?, Some(&dir.join("flows")), 2 * LATENESS)?;
                let mut best = f64::MAX;
                let mut lines = 0;
                for _ in 0..50 {
                    let (us, n) = runner.time_rebuild(&sub_key)?;
                    best = best.min(us);
                    lines = n;
                }
                let n = runner.instances.get(&sub_key).map_or(0, |i| i64::try_from(i.handed.len()).unwrap_or(0));
                println!(
                    "P10 carry {:5} year {:2}: Tyrion's subscription history {lines:4} lines, rebuild {best:8.1} us (best of 50), {n} ids in its window; load {}",
                    carry,
                    marks[next].0,
                    load_average()
                );
                next += 1;
            }
        }
    }
    Ok(())
}

/// Prediction 5: the domain check, per event, over every arrival's type and source.
fn domain(westwind: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let world = world::build(&seed)?;
    let pairs: Vec<(String, String)> = world.inbound.iter().filter_map(|i| admit(&i.line, 0).ok()).map(|e| (e.ty, e.source)).collect();
    let mut best = f64::MAX;
    for _ in 0..20 {
        let t = Instant::now();
        let mut ok = 0usize;
        for _ in 0..20 {
            for (ty, source) in &pairs {
                if check_domain(std::hint::black_box(ty), std::hint::black_box(source)).is_ok() {
                    ok += 1;
                }
            }
        }
        std::hint::black_box(ok);
        best = best.min(t.elapsed().as_secs_f64() / (20.0 * pairs.len() as f64));
    }
    println!("P5 domain check: {:.1} ns per event over {} events (best of 20 x 20 passes); {}; load {}", best * 1e9, pairs.len(), building(), load_average());
    Ok(())
}

/// Prediction 13: the extended orders loaded event-shaped, against the row-for-row nodes-only
/// load on the same main.
fn load(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let row_events = rows::extended_events(&seed, 1_000_000)?;
    let (events, orders) = world::reshaped(&seed, &row_events)?;
    let base_rows = 1_754_602.0;
    let base_seconds = 234.17;
    let base_bytes = 2_634_719_232.0;
    let base_rate = orders as f64 / base_seconds;
    let base_per_order = base_bytes / orders as f64;
    println!(
        "P13 baseline (nodes-only, row for row, 7aa6841): {base_rows} rows in {base_seconds}s = {orders} orders, {base_rate:.0} orders/s, {base_per_order:.0} bytes per order"
    );
    println!("P13 targets: {:.0} orders/s or more, under {:.0} bytes per order", 3.0 * base_rate, base_per_order / 2.0);
    let dir = fresh(out, "load")?;
    let path = dir.join("store.rannoch");
    let mut writer = store::Writer::open(&path)?;
    println!("P13 load starting: {} events; {}; load {}", events.len(), building(), load_average());
    let t = Instant::now();
    for chunk in events.chunks(4096) {
        for event in chunk {
            writer.add(store::translate(event));
        }
        writer.checkpoint()?;
    }
    let seconds = t.elapsed().as_secs_f64();
    let stopped = writer.graph.stopped().map_err(|e| e.to_string())?;
    drop(writer);
    let conn = Connection::open(&path).map_err(|e| e.to_string())?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").map_err(|e| e.to_string())?;
    drop(conn);
    let bytes: u64 = ["", "-wal", "-shm"].iter().map(|s| std::fs::metadata(format!("{}{s}", path.display())).map(|m| m.len()).unwrap_or(0)).sum();
    println!(
        "P13 event-shaped: {orders} orders in {seconds:.2}s = {:.0} orders/s ({:.2}x); {bytes} bytes = {:.0} bytes per order ({:.2} of the baseline); fill stopped on {}; {}; load {}",
        orders as f64 / seconds,
        orders as f64 / seconds / base_rate,
        bytes as f64 / orders as f64,
        bytes as f64 / orders as f64 / base_per_order,
        stopped.len(),
        building(),
        load_average()
    );
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize| args.get(i).map(PathBuf::from).unwrap_or_default();
    std::panic::set_hook(Box::new(|_| {}));
    let result = match args.get(1).map(String::as_str) {
        Some("run") => run(&arg(2), &arg(3)),
        Some("replays") => replays(&arg(2), &arg(3)),
        Some("age") => age(&arg(2), &arg(3)),
        Some("domain") => domain(&arg(2)),
        Some("load") => load(&arg(2), &arg(3)),
        Some("dump") => flow_text(&arg(2)).and_then(|f| std::fs::write(arg(2).join("flows.txt"), f).map_err(|e| e.to_string())),
        Some("child") => child(&arg(2), &arg(3), args.get(4).map_or("in", String::as_str)),
        _ => Err("usage: chain-toy run|replays|age|domain|load <westwind dir> <out dir>".into()),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
