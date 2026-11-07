//! Host actions: the app's own logic, each a pure function of the step's input (the event, the
//! instance's variables and any earlier answer). An action answers and names the variables it
//! sets; the runner records both, so a rebuilt instance gets them from its history. Every call
//! runs on a host thread under a deadline: a call that stalls past it fails like a crash, and the
//! thread is abandoned, never to finish its work.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use serde_json::{Map, Value, json};

use crate::world::{DAY, subscription_line, subscription_order};

/// What an action answers, and the variables it sets.
pub type Answer = (Value, Map<String, Value>);

/// How long a host call may take before it counts as a stall.
pub const DEADLINE: Duration = Duration::from_millis(300);

/// Work done outside the store, by action and event: what "happens once" is counted against.
pub static EFFECTS: OnceLock<Mutex<HashMap<(String, String), u32>>> = OnceLock::new();

/// Whether the Tin Man's triage has stalled yet: it stalls the first time only.
static STALLED: AtomicBool = AtomicBool::new(false);

fn effect(action: &str, event: &str) {
    let map = EFFECTS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut m) = map.lock() {
        *m.entry((action.to_string(), event.to_string())).or_insert(0) += 1;
    }
}

fn var<'a>(input: &'a Value, name: &str) -> &'a Value {
    input.pointer(&format!("/vars/{name}")).unwrap_or(&Value::Null)
}

fn int(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0)
}

fn set(pairs: Vec<(&str, Value)>) -> Map<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn ids(v: &Value) -> Vec<Value> {
    v.as_array().cloned().unwrap_or_default()
}

/// Whether every line is picked or waiting, and the picked lines not yet packed.
fn pack_decision(lines: &[Value], picked: &[Value], waiting: &[Value], packed: &[Value]) -> Vec<Value> {
    let covered = lines.iter().all(|l| packed.contains(l) || picked.contains(l) || waiting.contains(l));
    if !covered {
        return Vec::new();
    }
    picked.iter().filter(|l| !packed.contains(l)).cloned().collect()
}

/// Runs action `name` on `input`.
///
/// # Errors
/// The action is not registered, or its input is not what it takes.
#[allow(clippy::too_many_lines)]
pub fn call(name: &str, input: &Value) -> Result<Answer, String> {
    let data = input.get("data").cloned().unwrap_or(Value::Null);
    let event = input.get("id").and_then(Value::as_str).unwrap_or_default().to_string();
    let time = input.get("time").and_then(Value::as_i64).unwrap_or(0);
    Ok(match name {
        "remember_order" => (json!({}), set(vec![("order", data), ("has_order", json!(true))])),
        "stock_listed" => (
            json!({}),
            set(vec![
                ("stock", data["stock"].clone()),
                ("made_to_order", data["made_to_order"].clone()),
                ("available", data["available"].clone()),
                ("waiting", json!([])),
            ]),
        ),
        "reserve" => {
            let stock = int(var(input, "stock"));
            let q = int(&data["quantity"]);
            let made = var(input, "made_to_order").as_bool().unwrap_or(false);
            let available = var(input, "available").as_bool().unwrap_or(true);
            if available && !made && stock >= q {
                (json!({"allocated": true}), set(vec![("stock", json!(stock - q))]))
            } else {
                let mut waiting = ids(var(input, "waiting"));
                waiting.push(data.clone());
                (json!({"allocated": false, "reorder": available}), set(vec![("waiting", json!(waiting))]))
            }
        }
        "restock" => {
            let mut stock = int(var(input, "stock")) + int(&data["quantity"]);
            let mut released = Vec::new();
            let mut waiting = Vec::new();
            for line in ids(var(input, "waiting")) {
                let q = int(&line["quantity"]);
                if released.len() == waiting.len() && stock >= q {
                    stock -= q;
                    released.push(line);
                } else {
                    waiting.push(line);
                }
            }
            (json!({"released": released}), set(vec![("stock", json!(stock)), ("waiting", json!(waiting))]))
        }
        "unreserve" => (json!({}), set(vec![("stock", json!(int(var(input, "stock")) + int(&data["quantity"])))])),
        "expect_lines" => {
            let lines: Vec<Value> = ids(&data["lines"]).iter().map(|l| l["line_id"].clone()).collect();
            (json!({}), set(vec![("lines", json!(lines)), ("picked", json!([])), ("waiting", json!([])), ("packed", json!([])), ("shipments", json!(0))]))
        }
        "pick" | "wait_line" => {
            let line = data["line_id"].clone();
            let lines = ids(var(input, "lines"));
            let mut picked = ids(var(input, "picked"));
            let mut waiting = ids(var(input, "waiting"));
            let mut packed = ids(var(input, "packed"));
            if name == "pick" {
                waiting.retain(|l| *l != line);
                if !picked.contains(&line) {
                    picked.push(line);
                }
            } else if !waiting.contains(&line) {
                waiting.push(line);
            }
            let pack = pack_decision(&lines, &picked, &waiting, &packed);
            let mut shipments = int(var(input, "shipments"));
            let answer = if pack.is_empty() {
                json!({"pack": false})
            } else {
                shipments += 1;
                packed.extend(pack.iter().cloned());
                json!({"pack": true, "packed": {"order_id": data["order_id"], "shipment_no": shipments, "lines": pack}})
            };
            (answer, set(vec![("picked", json!(picked)), ("waiting", json!(waiting)), ("packed", json!(packed)), ("shipments", json!(shipments))]))
        }
        "remember_delivery" => {
            let spoils = ids(&data["lines"]).iter().filter_map(|l| l["spoils_in_days"].as_i64()).min();
            (
                json!({}),
                set(vec![
                    ("required_on", data["required_on"].clone()),
                    ("perishable", json!(spoils.is_some())),
                    ("spoils_in_days", json!(spoils)),
                ]),
            )
        }
        "spoil_after" => (json!({"delay_ms": int(var(input, "spoils_in_days")) * DAY}), Map::new()),
        "is_late" => {
            let late = var(input, "required_on")
                .as_str()
                .and_then(|r| crate::entrance::date_ms(r).ok())
                .is_some_and(|required| time > required);
            (json!({"late": late}), Map::new())
        }
        "triage" => {
            let text = data["text"].as_str().unwrap_or_default().to_lowercase();
            if text.contains("[stall]") && !STALLED.swap(true, Ordering::SeqCst) {
                // Stalls mid-step: never answers, and never does the work.
                loop {
                    std::thread::park();
                }
            }
            let category = if text.contains("wrong item") {
                "remake"
            } else if text.contains("refund") {
                "refund"
            } else if text.contains("lost") || text.contains("never arrived") {
                "lost"
            } else if text.contains("address") {
                "address"
            } else {
                "noise"
            };
            effect("triage", &event);
            (json!({"ticket": event, "order_id": data["order_id"], "category": category, "ship_to": data["text"]}), Map::new())
        }
        "remember_return" => (json!({}), set(vec![("category", data["category"].clone())])),
        "track_allocated" => {
            let mut allocated = ids(var(input, "allocated"));
            if !allocated.contains(&data) {
                allocated.push(data.clone());
            }
            (json!({}), set(vec![("allocated", json!(allocated))]))
        }
        "subscribe" | "renew" => {
            let (sub, n, months, customer, product, quantity, price, ship_to) = if name == "subscribe" {
                (
                    data["subscription_id"].as_str().unwrap_or_default().to_string(),
                    0,
                    int(&data["months"]),
                    data["customer_id"].clone(),
                    data["product_id"].clone(),
                    int(&data["quantity"]),
                    data["unit_price"].as_f64().unwrap_or(0.0),
                    data["ship_to"].clone(),
                )
            } else {
                let s = var(input, "sub");
                (
                    s["subscription_id"].as_str().unwrap_or_default().to_string(),
                    int(var(input, "n")) + 1,
                    int(&s["months"]),
                    s["customer_id"].clone(),
                    s["product_id"].clone(),
                    int(&s["quantity"]),
                    s["unit_price"].as_f64().unwrap_or(0.0),
                    s["ship_to"].clone(),
                )
            };
            let order = subscription_order(&sub, n);
            let placed_on = crate::entrance::format_time(time)[..10].to_string();
            let line = json!({"line_id": subscription_line(&sub, n), "order_id": order, "product_id": product,
                              "quantity": quantity, "unit_price": price, "spoils_in_days": null});
            let mut vars = set(vec![("n", json!(n))]);
            if name == "subscribe" {
                vars.insert("sub".into(), data.clone());
            }
            (
                json!({
                    "active": n < months,
                    "order": {"order_id": order, "customer_id": customer, "placed_on": placed_on, "required_on": null,
                              "ship_to": ship_to, "lines": [line]},
                    "payment": {"order_id": order, "amount": price * f64::from(i32::try_from(quantity).unwrap_or(0))},
                    "sub": {"subscription_id": sub},
                }),
                vars,
            )
        }
        other => return Err(format!("no host action {other}")),
    })
}

type Job = (String, Value, Sender<Result<Answer, String>>);

/// The host thread, replaced when a call stalls.
#[derive(Debug)]
pub struct Pool {
    jobs: Sender<Job>,
    pub stalls: u64,
}

fn spawn() -> Sender<Job> {
    let (tx, rx): (Sender<Job>, Receiver<Job>) = channel();
    std::thread::spawn(move || {
        while let Ok((name, input, reply)) = rx.recv() {
            let _ = reply.send(call(&name, &input));
        }
    });
    tx
}

impl Default for Pool {
    fn default() -> Pool {
        Pool { jobs: spawn(), stalls: 0 }
    }
}

impl Pool {
    /// Runs an action on the host thread, waiting at most [`DEADLINE`].
    ///
    /// # Errors
    /// The action fails, or does not answer in time: then its thread is abandoned.
    pub fn run(&mut self, name: &str, input: &Value) -> Result<Answer, String> {
        let (tx, rx) = channel();
        if self.jobs.send((name.to_string(), input.clone(), tx.clone())).is_err() {
            self.jobs = spawn();
            self.jobs.send((name.to_string(), input.clone(), tx)).map_err(|e| e.to_string())?;
        }
        match rx.recv_timeout(DEADLINE) {
            Ok(answer) => answer,
            Err(_) => {
                self.stalls += 1;
                self.jobs = spawn();
                Err(format!("host action {name} stalled: no answer within {} ms", DEADLINE.as_millis()))
            }
        }
    }
}

/// How many times each action's work was done for each event.
#[must_use]
pub fn effects() -> Vec<((String, String), u32)> {
    EFFECTS
        .get()
        .and_then(|m| m.lock().ok().map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_waits_on_stock_and_is_released_when_it_comes() {
        let line = json!({"line_id": "l", "order_id": "o", "product_id": "p", "quantity": 3, "unit_price": 1.0});
        let vars = json!({"stock": 2, "made_to_order": false, "available": true, "waiting": []});
        let (answer, set) = call("reserve", &json!({"data": line, "vars": vars})).unwrap();
        assert_eq!(answer["allocated"], false);
        let vars = json!({"stock": 2, "waiting": set["waiting"]});
        let (answer, set) = call("restock", &json!({"data": {"product_id": "p", "quantity": 5}, "vars": vars})).unwrap();
        assert_eq!(answer["released"].as_array().unwrap().len(), 1);
        assert_eq!(set["stock"], 4);
    }

    #[test]
    fn an_order_packs_what_is_picked_once_every_line_is_picked_or_waiting() {
        let vars = json!({"lines": ["a", "b", "c"], "picked": ["a"], "waiting": [], "packed": [], "shipments": 0});
        let (answer, set) = call("pick", &json!({"data": {"line_id": "b", "order_id": "o"}, "vars": vars})).unwrap();
        assert_eq!(answer["pack"], false);
        let vars = json!({"lines": ["a", "b", "c"], "picked": set["picked"], "waiting": [], "packed": [], "shipments": 0});
        let (answer, _) = call("wait_line", &json!({"data": {"line_id": "c", "order_id": "o"}, "vars": vars})).unwrap();
        assert_eq!(answer["packed"]["lines"], json!(["a", "b"]));
    }

    #[test]
    fn a_stalled_call_fails_by_its_deadline() {
        let mut pool = Pool::default();
        let input = json!({"id": "t1", "data": {"order_id": "o", "text": "[stall] help"}});
        assert!(pool.run("triage", &input).unwrap_err().contains("stalled"));
        let (answer, _) = pool.run("triage", &input).unwrap();
        assert_eq!(answer["category"], "noise");
        assert!(effects().iter().any(|((a, e), n)| a == "triage" && e == "t1" && *n == 1));
    }
}
