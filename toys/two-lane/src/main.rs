//! The two-lane toy: a fast lane (inbox, one write per fact, flows in the row) and a slow lane
//! (a log stream, read afterwards into hearings), on the toy's own SQLite store. Each subcommand
//! runs part of PREDICTIONS.md.
//!
//! ```sh
//! two-lane-toy run      <westwind dir> <out dir>   # predictions 1, 9, 10 and the cast checks
//! two-lane-toy replays  <westwind dir> <out dir>   # predictions 6, 7 and 10
//! two-lane-toy fast     <westwind dir> <out dir>   # predictions 2 to 5
//! two-lane-toy parallel <westwind dir> <out dir>   # prediction 8
//! ```

mod domain;
mod entrance;
mod flow;
mod host;
mod lane;
#[allow(dead_code)]
mod rows;
mod schema;
mod store;
mod trickle;
mod world;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use rusqlite::Connection;
use serde_json::{Value, json};

use crate::entrance::{Event, admit, mint, to_json};
use crate::flow::Flow;
use crate::lane::Lane;
use crate::schema::Schemas;
use crate::world::{DAY, HOUR, Inbound, LATENESS, World};

type Result<T> = std::result::Result<T, String>;

const FLOWS: &[&str] = &[
    include_str!("../flows/accept.jsonl"),
    include_str!("../flows/allocate.jsonl"),
    include_str!("../flows/stock.jsonl"),
    include_str!("../flows/pickpack.jsonl"),
    include_str!("../flows/ship.jsonl"),
    include_str!("../flows/deliver.jsonl"),
    include_str!("../flows/aftership.jsonl"),
    include_str!("../flows/support.jsonl"),
    include_str!("../flows/returns.jsonl"),
    include_str!("../flows/cancel.jsonl"),
    include_str!("../flows/subscription.jsonl"),
];
const SCHEMAS: &str = include_str!("../schemas.json");

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

fn flows(with: bool) -> Result<Vec<Flow>> {
    if with { FLOWS.iter().map(|f| Flow::parse(f)).collect() } else { Ok(Vec::new()) }
}

fn fresh(out: &Path, name: &str) -> Result<PathBuf> {
    let dir = out.join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| e.to_string())?;
    }
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(dir)
}

/// Streams `inbound` into a lane in `dir`, a batch at a time, then drains it.
fn stream(dir: &Path, inbound: &[Inbound], workers: usize, with_flows: bool, log: bool, skip: usize) -> Result<Lane> {
    let mut lane = Lane::open(dir, flows(with_flows)?, Schemas::load(SCHEMAS)?, LATENESS, workers, log)?;
    // A kill test sets this: the process dies, as by SIGKILL, after this many rounds.
    let die_after: Option<u64> = std::env::var("TWO_LANE_DIE_AFTER").ok().and_then(|v| v.parse().ok());
    let size = std::env::var("TWO_LANE_CHUNK").ok().and_then(|v| v.parse().ok()).unwrap_or(512);
    for chunk in inbound[skip..].chunks(size) {
        let lines: Vec<&str> = chunk.iter().map(|i| i.line.as_str()).collect();
        lane.take(&lines)?;
        while lane.round()? {
            if die_after.is_some_and(|n| lane.rounds >= n) {
                std::process::abort();
            }
        }
    }
    lane.ended = true;
    lane.drain()?;
    Ok(lane)
}

/// Every event in the log, once each by id.
fn read_log(dir: &Path) -> Result<Vec<Event>> {
    let text = std::fs::read_to_string(dir.join("log.jsonl")).map_err(|e| e.to_string())?;
    let mut seen = BTreeSet::new();
    Ok(text.lines().filter_map(|l| admit(l, 0).ok()).filter(|e| seen.insert(e.id.clone())).collect())
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

fn digest(text: &str) -> String {
    blake3::hash(text.as_bytes()).to_hex()[..16].to_string()
}
/// The outcomes text of a run: every order's and ticket's outside-facing events with their times.
fn outcome_text(dir: &Path) -> Result<String> {
    Ok(format!("{:?}", outcomes(&read_log(dir)?)))
}

/// Predictions 1, 9 and 10, and the cast checks, on one in-order run.
#[allow(clippy::too_many_lines)]
fn run(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    println!("seed.sql sha256 matches {}; {}; load {}", rows::SEED_SHA256, building(), load_average());
    let world = world::build(&seed)?;
    let dir = fresh(out, "in-order")?;
    let t = Instant::now();
    let lane = stream(&dir, &world.inbound, 1, true, true, 0)?;
    println!(
        "ran in {:.1}s: {} lines read, {} units, {} rounds, {} held at end; load {}",
        t.elapsed().as_secs_f64(), lane.read, lane.units, lane.rounds, lane.held(), load_average()
    );
    let log = read_log(&dir)?;
    let trails = trails(&log, &world);
    let mut matched = 0;
    for story in &world.stories {
        let mut expected = story.expected.clone();
        expected.sort();
        let got = trails.get(&story.who).cloned().unwrap_or_default();
        if got == expected {
            matched += 1;
            println!("P1 {:26} trail of {:4} events matches", story.who, got.len());
        } else {
            println!("P1 {:26} trail DIFFERS: expected {}, got {}", story.who, expected.len(), got.len());
            let e: BTreeSet<_> = expected.iter().collect();
            let g: BTreeSet<_> = got.iter().collect();
            for x in e.difference(&g).take(6) {
                println!("     missing day {} {}", x.0 as f64 / DAY as f64, x.1);
            }
            for x in g.difference(&e).take(6) {
                println!("     extra   day {} {}", x.0 as f64 / DAY as f64, x.1);
            }
        }
    }
    println!("P1 {matched} of {} trails match", world.stories.len());

    // Seed orders against SQL over the seed.
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
    let mut st = seed.prepare("SELECT id, shipped_date, required_date FROM orders").map_err(|e| e.to_string())?;
    let rows: Vec<(String, Option<String>, Option<String>)> = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(|e| e.to_string())?
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| e.to_string())?;
    let mut agree = 0;
    for (id, shipped, required) in &rows {
        let types = by_order.get(id).cloned().unwrap_or_default();
        let has = |t: &str| types.contains(&format!("westwind.{t}"));
        let ok = match shipped {
            Some(s) => {
                let at = entrance::date_ms(s)? + 3 * DAY;
                let late = required.as_deref().map(entrance::date_ms).transpose()?.is_some_and(|r| at > r);
                has("order.delivered") && delivered_at.get(id) == Some(&at) && has("order.late") == late && !has("order.unshipped") && has("order.closed")
            }
            None => has("order.unshipped") && !has("order.delivered"),
        };
        if ok {
            agree += 1;
        }
    }
    println!("P1 seed orders: {agree} of {} match SQL over the seed", rows.len());
    println!("    emitted events invalid: {}; bins {:?}", lane.invalid.len(), lane.bins);
    for (kind, what) in &lane.binned {
        println!("      {kind:34} {}", &what[..what.len().min(110)]);
    }
    println!("    stalls handed again: {:?}", lane.stalls);
    let shard = store::Shard::open(&dir.join("shard-0.db"))?;
    let count = |sql: &str| -> Result<i64> { shard.conn().query_row(sql, [], |r| r.get(0)).map_err(|e| e.to_string()) };
    println!(
        "    rows {} (Oz {}), edges {}; host answers kept: triage {}; Hodor's identical tickets: {} rows",
        count("SELECT COUNT(*) FROM rows")?,
        count("SELECT COUNT(*) FROM rows WHERE kind LIKE 'oz_%'")?,
        count("SELECT COUNT(*) FROM edges")?,
        shard.answers_kept("triage"),
        count("SELECT COUNT(*) FROM rows WHERE kind = 'westwind_ticket' AND body LIKE '%Hodor. Hodor hodor.%'")?
    );
    let triaged = host::effects().iter().filter(|((a, _), _)| a == "triage").map(|(_, n)| *n).collect::<Vec<_>>();
    println!("    triage work per ticket in this process: {triaged:?}");
    for who in ["Lion, abandoned", "Lion, unanswered"] {
        let closes: Vec<String> = trails.get(who).map(|t| t.iter().filter(|(_, ty)| ty.ends_with("expired") || ty.ends_with("abandoned")).map(|(o, ty)| format!("{ty} at day {}", *o as f64 / DAY as f64)).collect()).unwrap_or_default();
        println!("P9 {who}: {closes:?}");
    }
    let tyrion = trails.get("Tyrion").map(|t| t.iter().filter(|(_, ty)| ty == "order.placed").count()).unwrap_or(0);
    let last = trails.get("Tyrion").and_then(|t| t.iter().filter(|(_, ty)| ty == "order.placed").map(|(o, _)| *o).max()).unwrap_or(0);
    println!("P9 Tyrion: {tyrion} monthly orders placed by timer, the last at day {}", last / DAY);
    let cersei = world.stories.iter().find(|s| s.who == "Cersei").ok_or("no Cersei")?;
    let authorized = log.iter().filter(|e| e.ty == "westwind.return.authorized" && trails_root(&log, &e.id) == Some(cersei.root.clone())).count();
    println!("P10 Cersei's return authorized {authorized} time(s); duplicate ids dropped by the rows' windows");
    let text = lane.text()?;
    println!("rows {}, outcomes {}", digest(&text), digest(&outcome_text(&dir)?));
    Ok(())
}

fn trails_root(log: &[Event], id: &str) -> Option<String> {
    let by_id: HashMap<&str, &Event> = log.iter().map(|e| (e.id.as_str(), e)).collect();
    let mut at = id;
    for _ in 0..100_000 {
        match by_id.get(at).and_then(|e| e.causation.as_deref()) {
            Some(c) => at = c,
            None => return Some(at.to_string()),
        }
    }
    None
}

fn ordered(world: &World, order: &str) -> Result<Vec<Inbound>> {
    match order.strip_prefix("shuffled:") {
        None if order == "in" => Ok(world.inbound.clone()),
        Some(seed) => Ok(world.shuffled_within(LATENESS, seed.parse().map_err(|_| "bad seed")?)),
        None => Err(format!("no order {order}")),
    }
}

/// The child a kill test runs and kills: resumes in `dir`, skipping what the inbox already took.
fn child(westwind: &Path, dir: &Path, order: &str) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let world = world::build(&seed)?;
    let inbound = ordered(&world, order)?;
    let taken = std::fs::read_to_string(dir.join("inbox.jsonl")).unwrap_or_default().lines().filter(|l| !l.contains("/flow/")).count();
    stream(dir, &inbound, 1, true, true, taken.min(inbound.len())).map(|_| ())
}

fn summary(dir: &Path, lane: &Lane) -> Result<(String, String, String)> {
    let log = read_log(dir)?;
    let emitted: BTreeSet<&str> = log.iter().filter(|e| e.source.contains("/flow/")).map(|e| e.id.as_str()).collect();
    Ok((digest(&lane.text()?), digest(&format!("{emitted:?}")), digest(&outcome_text(dir)?)))
}

/// Predictions 6, 7 and 10.
fn replays(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let world = world::build(&seed)?;
    println!("{}; load {}", building(), load_average());
    let mut table = Vec::new();
    let base = fresh(out, "base")?;
    let lane = stream(&base, &world.inbound, 1, true, true, 0)?;
    let rounds = lane.rounds;
    table.push(("in order".to_string(), summary(&base, &lane)?, format!("{rounds} rounds")));
    drop(lane);
    for order in ["in", "shuffled:1"] {
        let dir = fresh(out, &format!("killed-{}", order.replace(':', "-")))?;
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let mut rng = rows::Rng::new(41);
        let mut landed = 0;
        let mut tries = 0;
        while landed < 10 && tries < 30 {
            tries += 1;
            let after = 1 + rng.below(3);
            let status = std::process::Command::new(&exe)
                .args(["child", &westwind.display().to_string(), &dir.display().to_string(), order])
                .env("TWO_LANE_DIE_AFTER", after.to_string())
                .env("TWO_LANE_CHUNK", "128")
                .status()
                .map_err(|e| e.to_string())?;
            if !status.success() {
                landed += 1;
            }
        }
        let status = std::process::Command::new(&exe).args(["child", &westwind.display().to_string(), &dir.display().to_string(), order]).env("TWO_LANE_CHUNK", "128").status().map_err(|e| e.to_string())?;
        if !status.success() {
            return Err("the last child failed".into());
        }
        let lane = Lane::open(&dir, flows(true)?, Schemas::load(SCHEMAS)?, LATENESS, 1, true)?;
        let shard = store::Shard::open(&dir.join("shard-0.db"))?;
        let note = format!("{landed} children of {tries} died mid-run (each aborted after a random 1 to 3 rounds of what was left); host triage answers kept {}", shard.answers_kept("triage"));
        table.push((format!("killed x10, {order}"), summary(&dir, &lane)?, note));
    }
    for s in [1u64, 2, 3] {
        let dir = fresh(out, &format!("shuffled-{s}"))?;
        let inbound = world.shuffled_within(LATENESS, s);
        let lane = stream(&dir, &inbound, 1, true, true, 0)?;
        let note = format!("largest lateness {:.2} days; bins {:?}", lane.max_lateness as f64 / DAY as f64, lane.bins);
        table.push((format!("shuffled within lateness, seed {s}"), summary(&dir, &lane)?, note));
    }
    {
        let dir = fresh(out, "four-workers")?;
        let lane = stream(&dir, &world.inbound, 4, true, true, 0)?;
        table.push(("in order, 4 workers".into(), summary(&dir, &lane)?, String::new()));
    }
    println!("\n{:36} {:17} {:17} {:17}  note", "run", "rows", "emitted", "outcomes");
    for (name, (r, e, o), note) in &table {
        println!("{name:36} {r:17} {e:17} {o:17}  {note}");
    }
    Ok(())
}

/// The extended orders as events: the reshaped stream, each order also paid an hour after it is
/// placed, collected on its shipped date and delivered three days later; stock never runs out.
fn extended_world(seed: &Connection, orders: usize, with_flows: bool) -> Result<(Vec<String>, usize)> {
    let rows_events = rows::extended_events(seed, 1_000_000)?;
    let (events, n) = world::reshaped(seed, &rows_events)?;
    let mut out = Vec::new();
    let mut count = 0;
    let mut placed_at: HashMap<String, (i64, String)> = HashMap::new();
    for mut e in events {
        let source = if e.ty == "westwind.customer.joined" { "westwind/web" } else { "westwind/warehouse" };
        e.source = source.to_string();
        match e.ty.as_str() {
            "westwind.product.listed" => {
                e.data["stock"] = json!(1_000_000_000_i64);
                e.data["available"] = json!(true);
                e.data["spoils_in_days"] = Value::Null;
            }
            "westwind.order.placed" => {
                count += 1;
                if count > orders {
                    continue;
                }
                e.source = "westwind/web".into();
                if e.data["required_on"].is_null() {
                    e.data["required_on"] = Value::Null;
                }
                placed_at.insert(e.data["order_id"].as_str().unwrap_or_default().to_string(), (e.time, e.id.clone()));
                out.push((e.time, to_json(&e)));
                if with_flows {
                    let order = e.data["order_id"].clone();
                    let pay = Event { id: mint(e.time + HOUR, format!("pay\n{}", e.id).as_bytes()), ty: "westwind.payment.received".into(),
                        source: "westwind/payments".into(), subject: e.subject.clone(), time: e.time + HOUR,
                        data: json!({"order_id": order, "amount": 1.0}), causation: Some(e.id.clone()), position: 0 };
                    out.push((pay.time, to_json(&pay)));
                }
                continue;
            }
            "westwind.order.shipped" => {
                let order = e.data["order_id"].as_str().unwrap_or_default().to_string();
                let Some((_, root)) = placed_at.get(&order).cloned() else { continue };
                if with_flows {
                    let mut c = e.clone();
                    c.ty = "westwind.carrier.collected".into();
                    c.source = "westwind/carrier".into();
                    c.causation = Some(root.clone());
                    out.push((c.time, to_json(&c)));
                    let mut d = c.clone();
                    d.ty = "westwind.carrier.delivered".into();
                    d.time += 3 * DAY;
                    d.id = mint(d.time, format!("delivered\n{}", c.id).as_bytes());
                    out.push((d.time, to_json(&d)));
                } else {
                    e.source = "westwind/carrier".into();
                    out.push((e.time, to_json(&e)));
                }
                continue;
            }
            _ => {}
        }
        out.push((e.time, to_json(&e)));
    }
    out.sort_by_key(|x| x.0);
    Ok((out.into_iter().map(|x| x.1).collect(), count.min(orders).min(n)))
}

/// Predictions 2 to 5: the fast lane on the extended orders, the log off and on, the trickle
/// reader beside it, and the file's size.
fn fast(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let (lines, orders) = extended_world(&seed, usize::MAX, false)?;
    println!(
        "extended, fast lane only: {} events, {orders} orders, batches of {}; {}; load {}",
        lines.len(),
        std::env::var("TWO_LANE_BATCH").unwrap_or_else(|_| "4096".into()),
        building(),
        load_average()
    );
    let mut results = Vec::new();
    for (run, log) in [(0, false), (0, true), (1, false), (1, true)] {
        let dir = fresh(out, &format!("fast-{}-{run}", if log { "log" } else { "nolog" }))?;
        let writing = Arc::new(AtomicBool::new(true));
        let done = Arc::new(AtomicU64::new(0));
        let reader = log.then(|| {
            let (l, h, w, d) = (dir.join("log.jsonl"), dir.join("hearings.db"), Arc::clone(&writing), Arc::clone(&done));
            std::thread::spawn(move || trickle::follow(&l, &h, &w, &d))
        });
        let mut lane = Lane::open(&dir, Vec::new(), Schemas::load(SCHEMAS)?, LATENESS, 1, log)?;
        println!("  load starting (log {log}, run {run}); {}; load {}", building(), load_average());
        let t = Instant::now();
        let mut lags = Vec::new();
        let batch = std::env::var("TWO_LANE_BATCH").ok().and_then(|v| v.parse().ok()).unwrap_or(4096);
        for (i, chunk) in lines.chunks(batch).enumerate() {
            let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
            lane.take(&refs)?;
            while lane.round()? {}
            if log && i % 20 == 0 {
                lags.push(lane.read.saturating_sub(done.load(Ordering::SeqCst)));
            }
        }
        lane.ended = true;
        lane.drain()?;
        let seconds = t.elapsed().as_secs_f64();
        let mut trickle_note = String::new();
        if let Some(r) = reader {
            writing.store(false, Ordering::SeqCst);
            let after = Instant::now();
            let heard = r.join().map_err(|_| "trickle panicked")??;
            let tail = after.elapsed().as_secs_f64();
            let max_lag = lags.iter().max().copied().unwrap_or(0);
            trickle_note = format!(
                "; trickle heard {heard}, lag sampled every 20 batches max {max_lag} events (last {}), finished {tail:.2}s after the fast lane",
                lags.last().copied().unwrap_or(0)
            );
        }
        drop(lane);
        let shard = dir.join("shard-0.db");
        let c = Connection::open(&shard).map_err(|e| e.to_string())?;
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").map_err(|e| e.to_string())?;
        drop(c);
        let size: u64 = ["", "-wal", "-shm"].iter().map(|s| std::fs::metadata(format!("{}{s}", shard.display())).map(|m| m.len()).unwrap_or(0)).sum();
        let log_size = std::fs::metadata(dir.join("log.jsonl")).map(|m| m.len()).unwrap_or(0);
        let inbox_size = std::fs::metadata(dir.join("inbox.jsonl")).map(|m| m.len()).unwrap_or(0);
        println!(
            "P2 fast lane, log {log}: {} facts in {seconds:.2}s = {:.0} facts/s; store file {:.1} MB (inbox {:.1} MB, log {:.1} MB){trickle_note}; {}; load {}",
            lines.len(), lines.len() as f64 / seconds, size as f64 / 1e6, inbox_size as f64 / 1e6, log_size as f64 / 1e6, building(), load_average()
        );
        results.push((log, seconds));
        if log && run == 0 {
            let h = Connection::open(dir.join("hearings.db")).map_err(|e| e.to_string())?;
            let t = Instant::now();
            let n: i64 = h.query_row("SELECT COUNT(*) FROM hearings", [], |r| r.get(0)).map_err(|e| e.to_string())?;
            println!("P5 hearings built: {n} ({:.3}s to count)", t.elapsed().as_secs_f64());
        }
    }
    let mean = |log: bool| {
        let v: Vec<f64> = results.iter().filter(|r| r.0 == log).map(|r| r.1).collect();
        v.iter().sum::<f64>() / v.len() as f64
    };
    println!("P4 log on against off: {:+.1}% (mean of 2 runs each, interleaved)", (mean(true) / mean(false) - 1.0) * 100.0);
    Ok(())
}

/// Prediction 5's second half: the hearings answer who said what, and when, for every cast event.
fn hearings(westwind: &Path, out: &Path) -> Result<()> {
    let seed = rows::load_seed(westwind)?;
    let world = world::build(&seed)?;
    let dir = fresh(out, "hearings")?;
    let lane = stream(&dir, &world.inbound, 1, true, true, 0)?;
    drop(lane);
    let writing = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicU64::new(0));
    trickle::follow(&dir.join("log.jsonl"), &dir.join("hearings.db"), &writing, &done)?;
    let log = read_log(&dir)?;
    let h = Connection::open(dir.join("hearings.db")).map_err(|e| e.to_string())?;
    let roots: BTreeSet<String> = world.stories.iter().map(|s| s.root.clone()).collect();
    let (mut asked, mut answered) = (0, 0);
    for e in &log {
        if trails_root(&log, &e.id).is_some_and(|r| roots.contains(&r)) {
            asked += 1;
            let got: Option<(String, i64)> = h
                .query_row("SELECT source, time FROM hearings WHERE id = ?1 AND type = ?2", [&e.id, &e.ty], |r| Ok((r.get(0)?, r.get(1)?)))
                .ok();
            if got == Some((e.source.clone(), e.time)) {
                answered += 1;
            }
        }
    }
    println!("P5 hearings answer who said what and when for {answered} of {asked} cast events");
    Ok(())
}

/// Prediction 8: the full chain over extended orders, 1 worker against 4.
fn parallel(westwind: &Path, out: &Path, orders: usize) -> Result<()> {
    // Both sides bound each round to the lateness of event time, so they make the same rounds.
    if std::env::var("TWO_LANE_SPAN").is_err() {
        return Err("run parallel with TWO_LANE_SPAN=1, so 1 worker and 4 make the same rounds".into());
    }
    let seed = rows::load_seed(westwind)?;
    let (lines, n) = extended_world(&seed, orders, true)?;
    let inbound: Vec<Inbound> = lines.iter().enumerate().map(|(i, l)| Inbound { line: l.clone(), time: 0, arrival: 0, seq: i as u64 }).collect();
    println!("extended with the chain: {} arrivals, {n} orders; {}; load {}", inbound.len(), building(), load_average());
    let mut digests = Vec::new();
    for (run, workers) in [(0, 1), (0, 4), (1, 1), (1, 4)] {
        let dir = fresh(out, &format!("parallel-{workers}-{run}"))?;
        println!("  starting {workers} worker(s), run {run}; {}; load {}", building(), load_average());
        let t = Instant::now();
        let lane = stream(&dir, &inbound, workers, true, true, 0)?;
        let seconds = t.elapsed().as_secs_f64();
        let units = lane.units;
        let text = lane.text()?;
        drop(lane);
        let o = outcome_text(&dir)?;
        println!(
            "P8 {workers} worker(s), run {run}: {seconds:.1}s, {units} units = {:.0} units/s; outcomes {}; rows {}; load {}",
            units as f64 / seconds, digest(&o), digest(&text), load_average()
        );
        digests.push((workers, seconds, digest(&o)));
    }
    let mean = |w: usize| digests.iter().filter(|d| d.0 == w).map(|d| d.1).sum::<f64>() / 2.0;
    println!("P8 4 workers against 1: {:.2}x faster; outcomes all the same: {}", mean(1) / mean(4), digests.windows(2).all(|w| w[0].2 == w[1].2));
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize| args.get(i).map(PathBuf::from).unwrap_or_default();
    std::panic::set_hook(Box::new(|_| {}));
    let result = match args.get(1).map(String::as_str) {
        Some("run") => run(&arg(2), &arg(3)),
        Some("replays") => replays(&arg(2), &arg(3)),
        Some("fast") => fast(&arg(2), &arg(3)),
        Some("hearings") => hearings(&arg(2), &arg(3)),
        Some("parallel") => parallel(&arg(2), &arg(3), args.get(4).and_then(|s| s.parse().ok()).unwrap_or(50_000)),
        Some("child") => child(&arg(2), &arg(3), args.get(4).map_or("in", String::as_str)),
        _ => Err("usage: two-lane-toy run|replays|fast|hearings|parallel <westwind dir> <out dir>".into()),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
