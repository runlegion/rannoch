//! The worker toy: an event's type alone decides where it goes, handlers are supervised, and a
//! flow rebuilds by replaying its history. Each subcommand runs one part of PREDICTIONS.md.
//!
//! ```sh
//! workers-toy correct  <westwind dir> <out dir>   # the flows' answers against SQL; prediction 1
//! workers-toy replays  <westwind dir> <out dir>   # predictions 5 and 6
//! workers-toy speed    <westwind dir> <out dir>   # predictions 1 to 4, on ~1,000,000 lines
//! workers-toy rebuild  <westwind dir> <out dir>   # rebuild time against history length
//! ```

mod compare;
mod entrance;
mod flow;
mod runtime;
mod store;
mod westwind;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use serde_json::Value;

use crate::entrance::{Event, READS, admit, to_json};
use crate::flow::{Collected, Collector, Flow, Host, Runner, Warehouse};
use crate::runtime::{Handler, Runtime, Start};
use crate::store::{Declare, Store, Writer, canonical, file_hash, store_path};
use crate::westwind::{REORDER_PERCENT, Rng, extended_events, load_seed, renumber, seed_events, sql_stock_low, sql_unshipped};

const STOCK: &str = include_str!("../flows/stock.jsonl");
const UNSHIPPED: &str = include_str!("../flows/unshipped.jsonl");

/// How many events from the log between checkpoints.
const EVERY: usize = 4096;
/// How many crashes on one event before it goes to the bin.
const LIMIT: u32 = 3;

type Result<T> = std::result::Result<T, String>;

/// Which handlers a run has.
#[derive(Debug, Clone, Copy)]
struct Parts {
    store: bool,
    stock: bool,
    unshipped: bool,
}

const ALL: Parts = Parts { store: true, stock: true, unshipped: true };

/// What a run leaves to look at.
struct Run {
    runtime: Runtime,
    collected: Rc<RefCell<Collected>>,
}

fn products(conn: &rusqlite::Connection) -> Result<Vec<(String, i64)>> {
    let mut statement = conn.prepare("SELECT id, in_stock FROM products ORDER BY rowid").map_err(|e| e.to_string())?;
    let rows = statement.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).map_err(|e| e.to_string())?;
    rows.collect::<std::result::Result<_, _>>().map_err(|e| e.to_string())
}

/// The runtime: declare and store built-ins, the two flows and a collector, as `parts` says,
/// against the store at `path` with flow histories in `dir`. The warehouse is the host's own and
/// outlives a handler's restart, as an action outside the store does.
fn build(path: &Path, dir: &Path, parts: Parts, host: Rc<RefCell<Warehouse>>, history: bool) -> Result<Run> {
    let mut runtime = Runtime::default();
    let collected = Rc::new(RefCell::new(Collected::default()));
    if parts.store {
        let p = path.to_path_buf();
        runtime.add("declare", &["westwind.ontology.declared".into()], Box::new(move || Ok(Box::new(Declare::open(&p)?) as Box<dyn Handler>)), LIMIT)?;
        let p = path.to_path_buf();
        runtime.add("store", &Store::patterns(), Box::new(move || Ok(Box::new(Store::open(&p)?) as Box<dyn Handler>)), LIMIT)?;
    }
    if parts.stock {
        let flow = Flow::parse(STOCK)?;
        let patterns = flow.patterns();
        let h: Rc<RefCell<dyn Host>> = host.clone();
        let file = history.then(|| dir.join("stock.history.jsonl"));
        let start: Start = Box::new(move || Ok(Box::new(Runner::start(flow.clone(), Some(Rc::clone(&h)), None, file.as_deref())?) as Box<dyn Handler>));
        runtime.add("stock", &patterns, start, LIMIT)?;
    }
    if parts.unshipped {
        let flow = Flow::parse(UNSHIPPED)?;
        let patterns = flow.patterns();
        let p = path.to_path_buf();
        let file = history.then(|| dir.join("unshipped.history.jsonl"));
        let start: Start = Box::new(move || {
            let writer = Writer::open(&p, "flow/unshipped")?;
            Ok(Box::new(Runner::start(flow.clone(), None, Some(writer), file.as_deref())?) as Box<dyn Handler>)
        });
        runtime.add("unshipped", &patterns, start, LIMIT)?;
    }
    let c = Rc::clone(&collected);
    runtime.add(
        "collector",
        &["westwind.stock.low".into(), "westwind.order.unshipped".into()],
        Box::new(move || Ok(Box::new(Collector(Rc::clone(&c))) as Box<dyn Handler>)),
        LIMIT,
    )?;
    let _ = host;
    Ok(Run { runtime, collected })
}

fn feed(runtime: &mut Runtime, events: impl IntoIterator<Item = Event>) -> Result<()> {
    for (index, event) in events.into_iter().enumerate() {
        runtime.push(event)?;
        if (index + 1) % EVERY == 0 {
            runtime.checkpoint()?;
        }
    }
    runtime.checkpoint()
}

fn load_average() -> String {
    std::process::Command::new("sysctl")
        .args(["-n", "vm.loadavg"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

fn fresh_dir(out: &Path, name: &str) -> Result<PathBuf> {
    let dir = out.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(dir)
}

fn ids(events: &[Event]) -> Vec<String> {
    events.iter().filter_map(|e| e.data.get("id").and_then(Value::as_str).map(str::to_string)).collect()
}

/// Rebuilds a flow from its history file and returns what its instances and timers say.
fn rebuilt(flow: &str, dir: &Path, host: Rc<RefCell<dyn Host>>) -> Result<String> {
    let (text, file) = if flow == "stock" { (STOCK, "stock.history.jsonl") } else { (UNSHIPPED, "unshipped.history.jsonl") };
    let runner = Runner::start(Flow::parse(text)?, Some(host), None, Some(&dir.join(file)))?;
    let state = state(&runner);
    std::fs::write(dir.join(format!("{flow}-rebuilt.txt")), &state).map_err(|e| e.to_string())?;
    Ok(state)
}

fn state(runner: &Runner) -> String {
    runner.state()
}

fn digest(text: &str) -> String {
    blake3::hash(text.as_bytes()).to_hex()[..16].to_string()
}

/// The flows' answers on the Westwind seed against SQL over the same seed, and one attribute
/// read per event.
fn correct(westwind: &Path, out: &Path) -> Result<()> {
    let conn = load_seed(westwind)?;
    println!("seed.sql sha256 matches {}", westwind::SEED_SHA256);
    let events = seed_events(&conn)?;
    let lines: Vec<String> = events.iter().map(to_json).collect();
    let dir = fresh_dir(out, "correct")?;
    let path = store_path(&dir, "store");
    let host = Rc::new(RefCell::new(Warehouse::new(&products(&conn)?, REORDER_PERCENT)));
    let mut run = build(&path, &dir, ALL, host, true)?;
    let before = READS.load(Ordering::Relaxed);
    let admitted: Vec<Event> = lines.iter().enumerate().map(|(i, l)| admit(l, i as u64 + 1)).collect::<Result<_>>()?;
    let reads = READS.load(Ordering::Relaxed) - before;
    feed(&mut run.runtime, admitted)?;
    println!("events in the log {}, attribute reads {reads}, events routed (log and emitted) {}", lines.len(), run.runtime.routed);
    let collected = run.collected.borrow();
    let low: Vec<String> = collected.order.iter().filter(|e| e.ty == "westwind.stock.low").map(|e| e.data["line"].as_str().unwrap_or("").to_string()).collect();
    let sql_low = sql_stock_low(&conn)?;
    println!("stock.low: flow {} lines, sql {} lines, equal in order: {}", low.len(), sql_low.len(), low == sql_low);
    let (mut a, mut b) = (low.clone(), sql_low.clone());
    a.sort();
    b.sort();
    println!("stock.low: equal as sets: {}", a == b);
    if low != sql_low {
        for (x, y) in low.iter().zip(&sql_low) {
            println!("    flow {x}  sql {y}");
        }
    }
    let unshipped: Vec<Event> = collected.order.iter().filter(|e| e.ty == "westwind.order.unshipped").cloned().collect();
    let mut flow_unshipped = ids(&unshipped);
    flow_unshipped.sort();
    let Some(unshipped_clock) = rebuilt_clock(&dir)? else {
        return Err("no unshipped history".into());
    };
    let mut sql = sql_unshipped(&conn, unshipped_clock)?;
    sql.sort();
    println!(
        "order.unshipped: flow {} orders, sql {} orders (shipped_date IS NULL, due by the flow's clock {}), equal: {}",
        flow_unshipped.len(),
        sql.len(),
        entrance::format_time(unshipped_clock),
        flow_unshipped == sql
    );
    let null: i64 = conn.query_row("SELECT COUNT(*) FROM orders WHERE shipped_date IS NULL", [], |r| r.get(0)).map_err(|e| e.to_string())?;
    println!("orders with shipped_date IS NULL in the seed: {null}");
    for handler in &run.runtime.handlers {
        println!("  handler {:10} handed {:6} restarts {} binned {}", handler.name, handler.handed, handler.restarts, handler.bin.len());
    }
    let graph = rannoch_store::open(&path, &store::KEY).map_err(|e| e.to_string())?;
    println!("fill stopped on {} events", graph.stopped().map_err(|e| e.to_string())?.len());
    let manager = graph.find("employee", &first_employee_with_manager(&conn)?).map_err(|e| e.to_string())?;
    println!("an employee node as read back: {manager:?}");
    Ok(())
}

fn first_employee_with_manager(conn: &rusqlite::Connection) -> Result<String> {
    conn.query_row("SELECT id FROM employees WHERE reports_to IS NOT NULL ORDER BY rowid LIMIT 1", [], |r| r.get(0))
        .map_err(|e| e.to_string())
}

fn rebuilt_clock(dir: &Path) -> Result<Option<i64>> {
    let runner = Runner::start(Flow::parse(UNSHIPPED)?, None, None, Some(&dir.join("unshipped.history.jsonl")))?;
    Ok((runner.clock != i64::MIN).then_some(runner.clock))
}

/// Shuffles the events after the declarations, with a fixed seed, and numbers them again.
fn shuffled(events: &[Event], seed: u64) -> Vec<Event> {
    let declared = events.iter().take_while(|e| e.ty == "westwind.ontology.declared").count();
    let mut rest: Vec<Event> = events[declared..].to_vec();
    let mut rng = Rng::new(seed);
    for i in (1..rest.len()).rev() {
        let j = usize::try_from(rng.below(i as u64 + 1)).unwrap_or(0);
        rest.swap(i, j);
    }
    let mut all: Vec<Event> = events[..declared].to_vec();
    all.extend(rest);
    renumber(&mut all);
    all
}

/// One outcome of a replay, to set beside the others.
struct Outcome {
    name: String,
    file: String,
    westwind: String,
    unrecorded: String,
    unshipped_nodes: String,
    stock: Option<String>,
    unshipped: String,
    fired: String,
}

fn store_text(path: &Path, events: &[Event], unshipped_keys: &[String]) -> Result<(String, String, String)> {
    let westwind = canonical(path, events, true)?;
    let unrecorded = canonical(path, events, false)?;
    let graph = rannoch_store::open(path, &store::KEY).map_err(|e| e.to_string())?;
    let mut nodes = String::new();
    for key in unshipped_keys {
        let node = graph.find("unshipped_order", key).map_err(|e| e.to_string())?;
        nodes.push_str(&format!("{key} {:?}\n", node.map(|n| (n.properties, n.source))));
    }
    if let Some(dir) = path.parent() {
        for (file, text) in [("graph.txt", &westwind), ("graph-unrecorded.txt", &unrecorded), ("unshipped-nodes.txt", &nodes)] {
            std::fs::write(dir.join(file), text).map_err(|e| e.to_string())?;
        }
    }
    Ok((westwind, unrecorded, nodes))
}

/// Runs `events` into one store `times` times over, in process.
fn replay_in_process(name: &str, events: &[Event], times: u64, conn: &rusqlite::Connection, out: &Path, keys: &[String]) -> Result<Outcome> {
    let dir = fresh_dir(out, name)?;
    let path = store_path(&dir, "store");
    let host = Rc::new(RefCell::new(Warehouse::new(&products(conn)?, REORDER_PERCENT)));
    let mut run = build(&path, &dir, ALL, Rc::clone(&host), true)?;
    let len = events.len() as u64;
    let started = Instant::now();
    for time in 0..times {
        feed(&mut run.runtime, events.iter().map(|e| Event { position: time * len + e.position, ..e.clone() }))?;
    }
    let seconds = started.elapsed().as_secs_f64();
    let stock_rebuilt = rebuilt("stock", &dir, host.clone())?;
    let unshipped_rebuilt = rebuilt("unshipped", &dir, host.clone())?;
    let fired = fired_of(&dir)?;
    drop(run);
    let (westwind, unrecorded, nodes) = store_text(&path, events, keys)?;
    println!("  {name}: {times} pass(es) in {seconds:.1}s");
    Ok(Outcome {
        name: name.to_string(),
        file: file_hash(&path)?[..16].to_string(),
        westwind: digest(&westwind),
        unrecorded: digest(&unrecorded),
        unshipped_nodes: digest(&nodes),
        stock: Some(digest(&stock_rebuilt)),
        unshipped: digest(&unshipped_rebuilt),
        fired: digest(&fired),
    })
}

fn fired_of(dir: &Path) -> Result<String> {
    let runner = Runner::start(Flow::parse(UNSHIPPED)?, None, None, Some(&dir.join("unshipped.history.jsonl")))?;
    Ok(format!("{:?}", runner.fired))
}

/// The child a kill test runs and kills: the seed in `order` into the store in `dir`, resuming
/// from each handler's durable place. No stock flow: its warehouse is the host's, and a killed
/// process takes the host's memory with it.
fn child(westwind: &Path, dir: &Path, order: &str) -> Result<()> {
    let conn = load_seed(westwind)?;
    let events = ordered(&seed_events(&conn)?, order)?;
    let path = dir.join("store.rannoch");
    let host = Rc::new(RefCell::new(Warehouse::new(&products(&conn)?, REORDER_PERCENT)));
    let parts = Parts { store: true, stock: false, unshipped: true };
    let mut run = build(&path, dir, parts, host, true)?;
    // Small checkpoints, so a kill lands between many of them.
    for (index, event) in events.into_iter().enumerate() {
        run.runtime.push(event)?;
        if (index + 1) % 64 == 0 {
            run.runtime.checkpoint()?;
        }
    }
    run.runtime.checkpoint()
}

fn ordered(events: &[Event], order: &str) -> Result<Vec<Event>> {
    match order.strip_prefix("shuffled:") {
        None if order == "in" => Ok(events.to_vec()),
        Some(seed) => Ok(shuffled(events, seed.parse().map_err(|_| "bad seed")?)),
        None => Err(format!("no order {order}")),
    }
}

/// Runs the child, killing it at random moments `kills` times, then lets it finish.
fn replay_killed(name: &str, westwind: &Path, out: &Path, order: &str, kills: u32, seed: u64, events: &[Event], keys: &[String]) -> Result<(Outcome, u32)> {
    let dir = fresh_dir(out, name)?;
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut rng = Rng::new(seed);
    let mut landed = 0;
    for _ in 0..kills {
        let mut child = std::process::Command::new(&exe)
            .args(["child", &westwind.display().to_string(), &dir.display().to_string(), order])
            .spawn()
            .map_err(|e| e.to_string())?;
        std::thread::sleep(std::time::Duration::from_millis(30 + rng.below(400)));
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
        return Err(format!("{name}: the last child failed: {status}"));
    }
    let path = dir.join("store.rannoch");
    let (westwind_text, unrecorded, nodes) = store_text(&path, events, keys)?;
    let host: Rc<RefCell<dyn Host>> = Rc::new(RefCell::new(Warehouse::default()));
    let unshipped = rebuilt("unshipped", &dir, host)?;
    Ok((
        Outcome {
            name: name.to_string(),
            file: file_hash(&path)?[..16].to_string(),
            westwind: digest(&westwind_text),
            unrecorded: digest(&unrecorded),
            unshipped_nodes: digest(&nodes),
            stock: None,
            unshipped: digest(&unshipped),
            fired: digest(&fired_of(&dir)?),
            },
        landed,
    ))
}

/// Stops a run at random places, drops every handler's memory, starts them again from their
/// histories and the store, and carries on: the flows' instances must equal a run that never
/// stopped.
fn replay_stopped(name: &str, events: &[Event], conn: &rusqlite::Connection, out: &Path, stops: u32, seed: u64, keys: &[String]) -> Result<Outcome> {
    let dir = fresh_dir(out, name)?;
    let path = store_path(&dir, "store");
    let host = Rc::new(RefCell::new(Warehouse::new(&products(conn)?, REORDER_PERCENT)));
    let mut rng = Rng::new(seed);
    let mut cuts: Vec<usize> = (0..stops).map(|_| usize::try_from(rng.below(events.len() as u64)).unwrap_or(0)).collect();
    cuts.sort_unstable();
    cuts.push(events.len());
    let mut from = 0;
    let mut live = String::new();
    for cut in cuts {
        // A stop: what was not checkpointed is lost; the handlers start again from what was.
        let mut run = build(&path, &dir, ALL, Rc::clone(&host), true)?;
        for (index, event) in events[from..cut].iter().enumerate() {
            run.runtime.push(event.clone())?;
            if (index + 1) % EVERY == 0 {
                run.runtime.checkpoint()?;
            }
        }
        // Read the log again from its start: each supervisor skips what its handler's durable
        // place already holds and hands the rest again.
        from = 0;
        if cut == events.len() {
            run.runtime.checkpoint()?;
            live = run.runtime.handlers.iter().map(|h| format!("{}: {}", h.name, h.handed)).collect::<Vec<_>>().join(", ");
        }
    }
    let stock = rebuilt("stock", &dir, host.clone())?;
    let unshipped = rebuilt("unshipped", &dir, host.clone())?;
    let (westwind_text, unrecorded, nodes) = store_text(&path, events, keys)?;
    println!("  {name}: {stops} stops; handed {live}");
    Ok(Outcome {
        name: name.to_string(),
        file: file_hash(&path)?[..16].to_string(),
        westwind: digest(&westwind_text),
        unrecorded: digest(&unrecorded),
        unshipped_nodes: digest(&nodes),
        stock: Some(digest(&stock)),
        unshipped: digest(&unshipped),
        fired: digest(&fired_of(&dir)?),
    })
}

/// Predictions 5 and 6.
fn replays(westwind: &Path, out: &Path) -> Result<()> {
    std::panic::set_hook(Box::new(|_| {}));
    let conn = load_seed(westwind)?;
    let events = seed_events(&conn)?;
    let keys = {
        let mut keys = sql_unshipped(&conn, i64::MAX)?;
        keys.sort();
        keys
    };
    println!("seed log: {} events; load {}", events.len(), load_average());

    // The live state of a run that never stopped, against the same run rebuilt from history.
    let dir = fresh_dir(out, "live")?;
    let path = store_path(&dir, "store");
    let host = Rc::new(RefCell::new(Warehouse::new(&products(&conn)?, REORDER_PERCENT)));
    let mut run = build(&path, &dir, ALL, Rc::clone(&host), true)?;
    let mut live_states = BTreeMap::new();
    for (index, event) in events.iter().enumerate() {
        run.runtime.push(event.clone())?;
        if (index + 1) % EVERY == 0 {
            run.runtime.checkpoint()?;
        }
    }
    run.runtime.checkpoint()?;
    for name in ["stock", "unshipped"] {
        let live = snapshot(&run.runtime, name).ok_or("no snapshot")?;
        std::fs::write(dir.join(format!("{name}-live.txt")), &live).map_err(|e| e.to_string())?;
        live_states.insert(name, live);
    }
    drop(run);
    for name in ["stock", "unshipped"] {
        let rebuilt_state = rebuilt(name, &dir, host.clone())?;
        println!("  {name}: instance rebuilt from history equals the live one: {}", rebuilt_state == live_states[name]);
    }

    let mut outcomes = Vec::new();
    for (name, times) in [("in-order-x1", 1), ("in-order-x1-again", 1), ("in-order-x10", 10), ("in-order-x1000", 1000)] {
        outcomes.push(replay_in_process(name, &events, times, &conn, out, &keys)?);
    }
    for seed in [1u64, 2, 3] {
        outcomes.push(replay_in_process(&format!("shuffled-{seed}"), &shuffled(&events, seed), 1, &conn, out, &keys)?);
    }
    outcomes.push(replay_stopped("stopped-in-process", &events, &conn, out, 8, 7, &keys)?);
    let (killed, landed) = replay_killed("killed-in-order", westwind, out, "in", 6, 11, &events, &keys)?;
    println!("  killed-in-order: {landed} of 6 kills landed mid-run");
    outcomes.push(killed);
    let (killed, landed) = replay_killed("killed-shuffled-1", westwind, out, "shuffled:1", 6, 13, &events, &keys)?;
    println!("  killed-shuffled-1: {landed} of 6 kills landed mid-run");
    outcomes.push(killed);

    println!(
        "\n{:22} {:17} {:17} {:17} {:17} {:17} {:17} {:17}",
        "run", "file sha256", "graph as read", "graph - recorded", "unshipped nodes", "stock flow", "unshipped flow", "timers fired"
    );
    for o in &outcomes {
        println!(
            "{:22} {:17} {:17} {:17} {:17} {:17} {:17} {:17}",
            o.name,
            o.file,
            o.westwind,
            o.unrecorded,
            o.unshipped_nodes,
            o.stock.as_deref().unwrap_or("-"),
            o.unshipped,
            o.fired
        );
    }
    Ok(())
}

fn snapshot(runtime: &Runtime, name: &str) -> Option<String> {
    runtime.handlers.iter().find(|h| h.name == name).and_then(runtime::Supervisor::snapshot)
}

/// Predictions 1 to 4 on the extended history.
fn speed(westwind: &Path, out: &Path, lines: usize, runs: usize) -> Result<()> {
    let conn = load_seed(westwind)?;
    let started = Instant::now();
    let events = extended_events(&conn, lines)?;
    let json: Vec<String> = events.iter().map(to_json).collect();
    let line_count = events.iter().filter(|e| e.ty == "westwind.order_line.added").count();
    println!(
        "extended log: {} events, {line_count} order lines, built in {:.1}s; load {}",
        events.len(),
        started.elapsed().as_secs_f64(),
        load_average()
    );

    // 1. One attribute read per event.
    let before = READS.load(Ordering::Relaxed);
    let admitted: Vec<Event> = json.iter().enumerate().map(|(i, l)| admit(l, i as u64 + 1)).collect::<Result<_>>()?;
    let reads = READS.load(Ordering::Relaxed) - before;
    println!("P1 entrance: {} events, {reads} attribute reads", json.len());

    // 2. Routing alone, over the log's types.
    let host = Rc::new(RefCell::new(Warehouse::new(&products(&conn)?, REORDER_PERCENT)));
    let dir = fresh_dir(out, "route")?;
    let mut run = build(&store_path(&dir, "store"), &dir, ALL, Rc::clone(&host), false)?;
    let mut best = f64::MAX;
    for _ in 0..5 {
        let t = Instant::now();
        let mut total = 0usize;
        for event in &admitted {
            total += std::hint::black_box(run.runtime.router.route(&event.ty)).len();
        }
        std::hint::black_box(total);
        best = best.min(t.elapsed().as_secs_f64());
    }
    println!("P2 routing: {:.1} ns per event (best of 5, {} events); load {}", best * 1e9 / admitted.len() as f64, admitted.len(), load_average());
    drop(run);

    // 3a. A flow with no store step: entrance, router, stock flow with its history, collector.
    for run_index in 0..runs {
        let host = Rc::new(RefCell::new(Warehouse::new(&products(&conn)?, REORDER_PERCENT)));
        let dir = fresh_dir(out, "flow-only")?;
        let parts = Parts { store: false, stock: true, unshipped: false };
        let mut run = build(&store_path(&dir, "unused"), &dir, parts, host, true)?;
        let t = Instant::now();
        feed(&mut run.runtime, json.iter().enumerate().map(|(i, l)| admit(l, i as u64 + 1)).collect::<Result<Vec<_>>>()?)?;
        let seconds = t.elapsed().as_secs_f64();
        println!(
            "P3 flow, no store step, from JSON: {:.0} events/s ({} events, {:.2}s, {} stock.low); load {} [run {run_index}]",
            json.len() as f64 / seconds,
            json.len(),
            seconds,
            run.collected.borrow().order.len(),
            load_average()
        );
    }
    for run_index in 0..runs {
        let host = Rc::new(RefCell::new(Warehouse::new(&products(&conn)?, REORDER_PERCENT)));
        let dir = fresh_dir(out, "flow-only")?;
        let parts = Parts { store: false, stock: true, unshipped: false };
        let mut run = build(&store_path(&dir, "unused"), &dir, parts, host, true)?;
        let parsed = admitted.clone();
        let t = Instant::now();
        feed(&mut run.runtime, parsed)?;
        let seconds = t.elapsed().as_secs_f64();
        println!(
            "P3 flow, no store step, past the entrance: {:.0} events/s ({:.2}s); load {} [run {run_index}]",
            json.len() as f64 / seconds,
            seconds,
            load_average()
        );
    }

    // 4. Store through the router against the hard-wired fill, interleaved, on the same events.
    let mut routed_rates = Vec::new();
    let mut wired_rates = Vec::new();
    for run_index in 0..runs {
        // Hard-wired: the rannoch bodies made beforehand, committed and filled at the same
        // checkpoints, with no entrance, router or supervisor.
        let dir = fresh_dir(out, "wired")?;
        let path = store_path(&dir, "store");
        {
            let mut declare = Declare::open(&path)?;
            for event in admitted.iter().take_while(|e| e.ty == "westwind.ontology.declared") {
                declare.handle(event, &mut Vec::new())?;
            }
        }
        let batches: Vec<(Vec<String>, u64)> = admitted
            .chunks(EVERY)
            .map(|chunk| (chunk.iter().flat_map(store::translate).collect(), chunk.last().map_or(0, |e| e.position)))
            .collect();
        let bodies: usize = batches.iter().map(|b| b.0.len()).sum();
        let mut writer = Writer::open(&path, store::PRODUCER)?;
        let t = Instant::now();
        for (batch, position) in batches {
            writer.add(batch, position);
            writer.checkpoint()?;
        }
        let wired = t.elapsed().as_secs_f64();
        wired_rates.push(wired);
        println!(
            "P4 hard-wired commit+fill: {:.2}s, {:.0} westwind events/s, {:.0} rannoch events/s ({bodies} bodies); load {} [run {run_index}]",
            wired,
            json.len() as f64 / wired,
            bodies as f64 / wired,
            load_average()
        );
        drop(writer);

        // Routed: from JSON through the entrance and router to the store built-in.
        let dir = fresh_dir(out, "routed")?;
        let path = store_path(&dir, "store");
        let host = Rc::new(RefCell::new(Warehouse::default()));
        let parts = Parts { store: true, stock: false, unshipped: false };
        let mut run = build(&path, &dir, parts, host, false)?;
        let t = Instant::now();
        feed(&mut run.runtime, json.iter().enumerate().map(|(i, l)| admit(l, i as u64 + 1)).collect::<Result<Vec<_>>>()?)?;
        let routed = t.elapsed().as_secs_f64();
        routed_rates.push(routed);
        println!(
            "P4 routed store from JSON: {:.2}s, {:.0} westwind events/s; {:+.1}% against hard-wired; load {} [run {run_index}]",
            routed,
            json.len() as f64 / routed,
            (routed / wired - 1.0) * 100.0,
            load_average()
        );
        drop(run);
    }

    // Fill alone: every body committed first, then one fill, timed by itself.
    {
        let dir = fresh_dir(out, "fill-alone")?;
        let path = store_path(&dir, "store");
        {
            let mut declare = Declare::open(&path)?;
            for event in admitted.iter().take_while(|e| e.ty == "westwind.ontology.declared") {
                declare.handle(event, &mut Vec::new())?;
            }
        }
        let mut graph = rannoch_store::open(&path, &store::KEY).map_err(|e| e.to_string())?;
        let mut bodies = 0usize;
        for chunk in admitted.chunks(EVERY) {
            let batch: Vec<String> = chunk.iter().flat_map(store::translate).collect();
            bodies += batch.len();
            let refs: Vec<&str> = batch.iter().map(String::as_str).collect();
            graph.commit(&refs, &[]).map_err(|e| e.to_string())?;
        }
        let t = Instant::now();
        graph.fill().map_err(|e| e.to_string())?;
        let seconds = t.elapsed().as_secs_f64();
        println!(
            "P4 fill alone: {seconds:.2}s, {:.0} rannoch events/s, {:.0} westwind events/s; load {}",
            bodies as f64 / seconds,
            json.len() as f64 / seconds,
            load_average()
        );
    }

    // 3b. The full path: declare, store, both flows with their histories, collector.
    let host = Rc::new(RefCell::new(Warehouse::new(&products(&conn)?, REORDER_PERCENT)));
    let dir = fresh_dir(out, "full")?;
    let mut run = build(&store_path(&dir, "store"), &dir, ALL, host, true)?;
    let t = Instant::now();
    feed(&mut run.runtime, json.iter().enumerate().map(|(i, l)| admit(l, i as u64 + 1)).collect::<Result<Vec<_>>>()?)?;
    let seconds = t.elapsed().as_secs_f64();
    println!(
        "P3 full path from JSON: {:.0} events/s ({:.2}s, {} routed incl. emitted); load {}",
        json.len() as f64 / seconds,
        seconds,
        run.runtime.routed,
        load_average()
    );
    let median = |v: &mut Vec<f64>| {
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    println!(
        "P4 median routed {:.2}s against hard-wired {:.2}s: {:+.1}%",
        median(&mut routed_rates.clone()),
        median(&mut wired_rates.clone()),
        (median(&mut routed_rates) / median(&mut wired_rates) - 1.0) * 100.0
    );
    Ok(())
}

/// Rebuild time against history length, for one stock instance.
fn rebuild(out: &Path) -> Result<()> {
    println!("load {}", load_average());
    for n in [10u64, 100, 1_000, 10_000, 100_000] {
        let dir = fresh_dir(out, &format!("rebuild-{n}"))?;
        let file = dir.join("stock.history.jsonl");
        let host: Rc<RefCell<dyn Host>> = Rc::new(RefCell::new(Warehouse::new(&[("p".into(), 1_000_000_000)], 20)));
        let mut runner = Runner::start(Flow::parse(STOCK)?, Some(Rc::clone(&host)), None, Some(&file))?;
        let mut out_events = Vec::new();
        for position in 1..=n {
            let event = Event {
                id: entrance::mint(i64::try_from(position).unwrap_or(0), &position.to_be_bytes()),
                ty: "westwind.order_line.added".into(),
                source: "rebuild".into(),
                subject: None,
                time: i64::try_from(position).unwrap_or(0),
                data: serde_json::json!({"product_id": "p", "quantity": 1}),
                position,
            };
            runner.handle(&event, &mut out_events)?;
        }
        runner.checkpoint()?;
        let live = state(&runner);
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t = Instant::now();
            let again = Runner::start(Flow::parse(STOCK)?, Some(Rc::clone(&host)), None, Some(&file))?;
            best = best.min(t.elapsed().as_secs_f64());
            if state(&again) != live {
                return Err(format!("rebuild of {n} differs"));
            }
        }
        println!("history {n:>7}: rebuild {:>10.3} ms, {:.2} us per entry, equal to live", best * 1e3, best * 1e6 / n as f64);
    }
    Ok(())
}

/// What else is building on the machine, so a speed line can say it ran alone.
fn building() -> String {
    let out = std::process::Command::new("pgrep").args(["-lx", "rustc|cargo|clang|ld|cc"]).output();
    let names: Vec<String> = out
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().filter_map(|l| l.split_whitespace().nth(1).map(str::to_string)).collect())
        .unwrap_or_default();
    if names.is_empty() { "nothing else building".into() } else { format!("also running: {}", names.join(",")) }
}

/// The graph against nodes-only against relational toy, on the seed or the extended log.
fn compare_run(westwind: &Path, out: &Path, which: &str, runs: usize) -> Result<()> {
    use compare::{SCENARIOS, Version};
    let conn = load_seed(westwind)?;
    let events = if which == "seed" { seed_events(&conn)? } else { extended_events(&conn, 1_000_000)? };
    let rows = events.iter().filter(|e| e.ty != "westwind.ontology.declared").count();
    println!("{which}: {} events, {rows} rows; load {}", events.len(), load_average());
    let dir = fresh_dir(out, &format!("compare-{which}"))?;
    let mut loads: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for run in 0..runs {
        for version in [Version::Relational, Version::Nodes, Version::Graph] {
            println!("  {} load starting [run {run}]; {}; load {}", version.name(), building(), load_average());
            let seconds = compare::load(version, &compare::path(&dir, version), &events)?;
            loads.entry(version.name()).or_default().push(seconds);
            println!(
                "  {} load: {seconds:.2}s, {:.0} rows/s [run {run}]; load {}",
                version.name(),
                rows as f64 / seconds,
                load_average()
            );
        }
    }
    let relational = rusqlite::Connection::open(compare::path(&dir, Version::Relational)).map_err(|e| e.to_string())?;
    let inputs = compare::inputs(&conn, &relational)?;
    println!("  inputs {inputs:?}");
    let conns: Vec<(Version, rusqlite::Connection)> = Version::ALL
        .iter()
        .map(|&v| rusqlite::Connection::open(compare::path(&dir, v)).map(|c| (v, c)).map_err(|e| e.to_string()))
        .collect::<Result<_>>()?;
    let repeats = if which == "seed" { 5 } else { 20 };
    for name in SCENARIOS {
        let mut answers = Vec::new();
        let mut line = format!("  {name:7}");
        for (version, c) in &conns {
            let (rows, ms) = compare::answer(c, *version, name, &inputs, repeats)?;
            line.push_str(&format!(" {} {:>9.3} ms ({} rows)", version.name(), ms, rows.len()));
            answers.push(rows);
        }
        let agree = answers.windows(2).all(|w| w[0] == w[1]);
        println!("{line}; all three agree: {agree}; load {}", load_average());
        if which == "seed" {
            let (direct, _) = compare::answer(&conn, Version::Relational, name, &inputs, 1)?;
            println!("          against SQL over seed.sql itself: {}", answers[2] == direct);
        }
        if which == "seed" {
            let expected = match name {
                "A" => Some(sql_stock_low(&conn)?.into_iter().map(|id| format!("Text({id:?})")).collect::<Vec<_>>()),
                _ => None,
            };
            if let Some(expected) = expected {
                println!("          A against the worker toy's SQL over the seed: {}", answers[2] == expected);
            }
        }
        if which == "seed" && name == "B" {
            let clock: String = conn.query_row("SELECT MAX(MAX(order_date), MAX(COALESCE(shipped_date, ''))) FROM orders", [], |r| r.get(0)).map_err(|e| e.to_string())?;
            let mut expected: Vec<String> = sql_unshipped(&conn, entrance::date_ms(&clock)?)?.into_iter().map(|id| format!("Text({id:?})")).collect();
            expected.sort();
            println!("          B against the worker toy's SQL over the seed: {}", answers[2] == expected);
        }
    }
    for version in Version::ALL {
        let bytes = compare::size(&compare::path(&dir, version))?;
        println!("  {} on disk: {bytes} bytes ({:.1} MB)", version.name(), bytes as f64 / 1e6);
    }
    for (name, seconds) in &loads {
        println!("  {name} loads: {seconds:.2?}");
    }
    Ok(())
}

fn version_named(name: &str) -> Result<compare::Version> {
    compare::Version::ALL.into_iter().find(|v| v.name() == name).ok_or_else(|| format!("no version {name}"))
}

/// Loads the extended log into one version at `dir/<label>.db`, in a process of its own.
fn compare_load(westwind: &Path, dir: &Path, version: &str, label: &str) -> Result<()> {
    let version = version_named(version)?;
    let conn = load_seed(westwind)?;
    let events = extended_events(&conn, 1_000_000)?;
    let rows = events.iter().filter(|e| e.ty != "westwind.ontology.declared").count();
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let path = dir.join(format!("{label}.db"));
    println!("{label} ({}) load starting: {rows} rows; {}; load {}", version.name(), building(), load_average());
    let seconds = compare::load(version, &path, &events)?;
    println!("{label} load: {seconds:.2}s, {:.0} rows/s; {}; load {}", rows as f64 / seconds, building(), load_average());
    let bytes = compare::size(&path)?;
    println!("{label} on disk: {bytes} bytes ({:.1} MB)", bytes as f64 / 1e6);
    Ok(())
}

/// Answers every scenario on each `label=version:path`, and says whether they all agree.
fn compare_ask(westwind: &Path, specs: &[String]) -> Result<()> {
    let conn = load_seed(westwind)?;
    let mut targets = Vec::new();
    for spec in specs {
        let (label, rest) = spec.split_once('=').ok_or("label=version:path")?;
        let (version, path) = rest.split_once(':').ok_or("label=version:path")?;
        let c = rusqlite::Connection::open(path).map_err(|e| e.to_string())?;
        targets.push((label.to_string(), version_named(version)?, c));
    }
    let relational = &targets.iter().find(|t| t.1 == compare::Version::Relational).ok_or("no relational")?.2;
    let inputs = compare::inputs(&conn, relational)?;
    println!("inputs {inputs:?}; {}; load {}", building(), load_average());
    for name in compare::SCENARIOS {
        let mut answers = Vec::new();
        let mut line = format!("{name:7}");
        for (label, version, c) in &targets {
            let (rows, ms) = compare::answer(c, *version, name, &inputs, 20)?;
            line.push_str(&format!(" | {label} {ms:.3} ms"));
            answers.push(rows);
        }
        let agree = answers.windows(2).all(|w| w[0] == w[1]);
        println!("{line} | {} rows | all agree: {agree} | load {}", answers[0].len(), load_average());
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize| args.get(i).map(PathBuf::from).unwrap_or_default();
    let result = match args.get(1).map(String::as_str) {
        Some("correct") => correct(&arg(2), &arg(3)),
        Some("replays") => replays(&arg(2), &arg(3)),
        Some("speed") => {
            let lines = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
            let runs = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(3);
            speed(&arg(2), &arg(3), lines, runs)
        }
        Some("rebuild") => rebuild(&arg(3)),
        Some("compare") => compare_run(&arg(2), &arg(3), args.get(4).map_or("seed", String::as_str), args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1)),
        Some("compare-load") => compare_load(&arg(2), &arg(3), args.get(4).map_or("", String::as_str), args.get(5).map_or("", String::as_str)),
        Some("compare-ask") => compare_ask(&arg(2), &args[3.min(args.len())..]),
        Some("child") => child(&arg(2), &arg(3), args.get(4).map_or("in", String::as_str)),
        _ => Err("usage: workers-toy correct|replays|speed|rebuild <westwind dir> <out dir>".into()),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
