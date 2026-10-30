//! Live test for rannoch's core: produce through the queue with positions, verify, work.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rannoch_queue::{Queue, State};
use rannoch_store::{Class, Cursor, Declaration, Graph, open};

const KEY: [u8; 32] = [7; 32];

fn item(n: u64) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}:0","source":"rannoch:feed","type":"rannoch.node.item","subject":"i{n}","data":{{"properties":{{"v":"{n}"}},"valid":"unknown"}}}}"#,
        rannoch_cloudevents::EventId::now()
    )
}

fn graph(path: &str) -> Graph {
    let mut g = open(path, &KEY).expect("open");
    g.declare(
        &Declaration::Class(Class::new("item", ["v"])),
        r#"{"specversion":"1.0","id":"d-item","source":"rannoch:live","type":"rannoch.declared.item"}"#,
    )
    .expect("declare");
    g
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mode, path) = (args[1].as_str(), args[2].as_str());
    match mode {
        "produce" => {
            let total: u64 = args[3].parse().expect("total");
            let t0 = Instant::now();
            let queue = Queue::open(graph(path), Queue::INTERVAL).expect("queue");
            let start = queue
                .position("feed")
                .expect("position")
                .map_or(0, |p| p.parse::<u64>().expect("numeric") + 1);
            let t_open = t0.elapsed();
            for n in start..total {
                queue.accept_from(item(n), "feed", n.to_string()).expect("accept");
                if n % 500 == 0 {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            let t_accept = t0.elapsed();
            let failures = queue.close();
            let t_all = t0.elapsed();
            let n = total - start;
            println!(
                "produced {start}..{total}, failures {} | open {:.1}ms, accept {:.1}ms, close+drain {:.1}ms, total {:.1}ms, {:.0} events/s",
                failures.len(),
                t_open.as_secs_f64() * 1e3,
                (t_accept - t_open).as_secs_f64() * 1e3,
                (t_all - t_accept).as_secs_f64() * 1e3,
                t_all.as_secs_f64() * 1e3,
                n as f64 / t_all.as_secs_f64()
            );
        }
        "verify" => {
            let total: u64 = args[3].parse().expect("total");
            let t0 = Instant::now();
            let g = open(path, &KEY).expect("open");
            let mut seen: BTreeMap<u64, u32> = BTreeMap::new();
            for event in g.events().since(Cursor::START).expect("events") {
                let v: serde_json::Value = serde_json::from_str(&event.body).expect("json");
                if let Some(s) = v["subject"].as_str().and_then(|s| s.strip_prefix('i')) {
                    *seen.entry(s.parse().expect("n")).or_default() += 1;
                }
            }
            let t_log = t0.elapsed();
            let missing = (0..total).filter(|n| !seen.contains_key(n)).count();
            let doubled = seen.values().filter(|c| **c > 1).count();
            let unfilled = (0..total)
                .filter(|n| g.find("item", &format!("i{n}")).expect("find").is_none())
                .count();
            let t_find = t0.elapsed() - t_log;
            println!(
                "read log {:.0}ms, {total} finds {:.0}ms ({:.1}us each)",
                t_log.as_secs_f64() * 1e3,
                t_find.as_secs_f64() * 1e3,
                t_find.as_secs_f64() * 1e6 / total as f64
            );
            println!(
                "events {} distinct {} missing {missing} doubled {doubled} unfilled {unfilled} position {:?}",
                seen.values().map(|c| u64::from(*c)).sum::<u64>(),
                seen.len(),
                g.position("feed").expect("position")
            );
        }
        "work" => {
            let queue = Queue::open(graph(path), Queue::INTERVAL).expect("queue");
            let mut n = 0u64;
            let (mut t_next, mut t_done) = (Duration::ZERO, Duration::ZERO);
            let t0 = Instant::now();
            loop {
                let a = Instant::now();
                let Some(event) = queue.next("w").expect("next") else { break };
                let b = Instant::now();
                queue.done("w", event.cursor).expect("done");
                t_next += b - a;
                t_done += b.elapsed();
                n += 1;
            }
            let all = t0.elapsed();
            println!(
                "worked {n} in {:.1}s, {:.0} events/s | next {:.2}ms each, done {:.2}ms each",
                all.as_secs_f64(),
                n as f64 / all.as_secs_f64(),
                t_next.as_secs_f64() * 1e3 / n.max(1) as f64,
                t_done.as_secs_f64() * 1e3 / n.max(1) as f64
            );
            drop(queue.close());
        }
        "states" => {
            let g = open(path, &KEY).expect("open");
            let last = g.events().committed().expect("committed");
            let mut counts: BTreeMap<String, u64> = BTreeMap::new();
            let mut stuck = Vec::new();
            for c in 1..=last_cursor(last) {
                let s = g.state("w", cursor(c)).expect("state");
                if s == State::InProgress {
                    stuck.push(c);
                }
                *counts.entry(format!("{s:?}").chars().take_while(|c| *c != '(').collect()).or_default() += 1;
            }
            println!("states {counts:?} in-progress cursors {stuck:?}");
        }
        _ => panic!("mode"),
    }
}

fn last_cursor(c: Cursor) -> i64 {
    format!("{c:?}").trim_start_matches("Cursor(").trim_end_matches(')').parse().expect("cursor")
}

fn cursor(n: i64) -> Cursor {
    serde_json::from_value(serde_json::json!(n)).expect("cursor from json")
}
