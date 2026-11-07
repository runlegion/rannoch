//! Westwind's world, rebuilt to fit the events, not the schema: each event is one thing that
//! happened, carrying what it is about. The seed's 320 orders take the plain path; a cast of
//! scripted customers takes every other path, and each story's expected trail is written here,
//! from the script, before anything runs. Oz sits beside Westwind in the same store.

use std::collections::BTreeMap;

use rusqlite::Connection;
use serde_json::{Map, Value, json};

use crate::entrance::{Event, date_ms, format_time, mint, to_json};
use crate::rows::{Rng, rows};

pub const HOUR: i64 = 3_600_000;
pub const DAY: i64 = 86_400_000;

/// How late an event may arrive, by event time.
pub const LATENESS: i64 = 2 * DAY;

/// Months of Tyrion's subscription.
pub const MONTHS: i64 = 120;

/// One event as it arrives: its line, its event time and when it arrives.
#[derive(Debug, Clone)]
pub struct Inbound {
    pub line: String,
    pub time: i64,
    pub arrival: i64,
    pub seq: u64,
}

/// One story: its root event's id and time, and the trail expected from it, as offsets from the
/// root's time with their types.
#[derive(Debug, Clone)]
pub struct Story {
    pub who: String,
    pub root: String,
    pub at: i64,
    pub expected: Vec<(i64, String)>,
}

/// The world: what arrives, and what each story should leave.
#[derive(Debug, Default)]
pub struct World {
    pub inbound: Vec<Inbound>,
    pub stories: Vec<Story>,
    /// Each seed order's id.
    pub seed_orders: Vec<String>,
    seq: u64,
}

/// The id of month `n`'s order in subscription `sub`, which the subscription flow and the
/// generator both compute.
#[must_use]
pub fn subscription_order(sub: &str, n: i64) -> String {
    mint(0, format!("subscription\n{sub}\n{n}").as_bytes())
}

/// The id of line `n` of a subscription's month.
#[must_use]
pub fn subscription_line(sub: &str, n: i64) -> String {
    mint(0, format!("subscription line\n{sub}\n{n}").as_bytes())
}

impl World {
    fn push_line(&mut self, line: String, time: i64, arrival: i64) {
        self.seq += 1;
        self.inbound.push(Inbound { line, time, arrival, seq: self.seq });
    }

    /// Sends an event, arriving at its own time; returns its id.
    fn send(&mut self, time: i64, ty: &str, source: &str, subject: Option<&str>, data: Value, causation: Option<&str>) -> String {
        let seed = format!("{source}\n{ty}\n{time}\n{data}\n{}", self.seq);
        let id = mint(time, seed.as_bytes());
        self.send_as(&id, time, time, ty, source, subject, data, causation);
        id
    }

    #[allow(clippy::too_many_arguments)]
    fn send_as(&mut self, id: &str, time: i64, arrival: i64, ty: &str, source: &str, subject: Option<&str>, data: Value, causation: Option<&str>) {
        let event = Event {
            id: id.to_string(),
            ty: ty.to_string(),
            source: source.to_string(),
            subject: subject.map(str::to_string),
            time,
            data,
            causation: causation.map(str::to_string),
            position: 0,
        };
        self.push_line(to_json(&event), time, arrival);
    }

    /// Arrival order: by arrival, then by when the generator sent it.
    pub fn arrange(&mut self) {
        self.inbound.sort_by_key(|i| (i.arrival, i.seq));
    }

    /// The same events, each arriving up to `lateness` after its time, at random (fixed seed).
    #[must_use]
    pub fn shuffled_within(&self, lateness: i64, seed: u64) -> Vec<Inbound> {
        let mut rng = Rng::new(seed);
        let span = u64::try_from(lateness).unwrap_or(1).max(1);
        let mut out: Vec<Inbound> = self
            .inbound
            .iter()
            .map(|i| Inbound { arrival: i.arrival + i64::try_from(rng.below(span)).unwrap_or(0), ..i.clone() })
            .collect();
        out.sort_by_key(|i| (i.arrival, i.seq));
        out
    }
}

/// A normal order's chain, from its placing at offset 0 with `lines` lines in stock, collected
/// at `collect` and delivered three days later. `required` late means `order.late` too.
fn plain_trail(lines: usize, collect: i64, late: bool) -> Vec<(i64, String)> {
    let mut t = vec![(0, "order.placed"), (HOUR, "payment.received"), (HOUR, "order.accepted"), (DAY, "accept.payment_due")];
    for _ in 0..lines {
        t.extend([(HOUR, "line.requested"), (HOUR, "line.allocated"), (HOUR, "line.picked")]);
    }
    t.extend([(HOUR, "order.packed"), (HOUR + 30 * DAY, "ship.overdue")]);
    t.extend(shipped_trail(collect, late));
    t.into_iter().map(|(o, ty)| (o, ty.to_string())).collect()
}

/// From collection on: shipped, delivered three days later, the after-ship window two weeks on.
fn shipped_trail(collect: i64, late: bool) -> Vec<(i64, &'static str)> {
    let delivered = collect + 3 * DAY;
    let mut t = vec![
        (collect, "carrier.collected"),
        (collect, "order.shipped"),
        (delivered, "carrier.delivered"),
        (delivered, "order.delivered"),
        (delivered + 14 * DAY, "aftership.window_closed"),
        (delivered + 14 * DAY, "order.closed"),
    ];
    if late {
        t.push((delivered, "order.late"));
    }
    t
}

fn w(ty: &str) -> String {
    format!("westwind.{ty}")
}

fn line(order: &str, n: usize, product: &Map<String, Value>, quantity: i64) -> Value {
    json!({
        "line_id": mint(0, format!("line\n{order}\n{n}").as_bytes()),
        "order_id": order,
        "product_id": product.get("id"),
        "quantity": quantity,
        "unit_price": product.get("unit_price"),
        "spoils_in_days": product.get("spoils_in_days").cloned().unwrap_or(Value::Null),
    })
}

/// The whole world, from the seed.
///
/// # Errors
/// The seed cannot be read.
#[allow(clippy::too_many_lines)]
pub fn build(seed: &Connection) -> Result<World, String> {
    let mut world = World::default();
    let base = date_ms("2025-01-06")? - 9 * HOUR;
    let web = "westwind/web";
    let carrier = "westwind/carrier";
    let warehouse = "westwind/warehouse";

    // The cast's own products, listed beside Westwind's.
    let suppliers = rows(seed, "suppliers")?;
    for s in &suppliers {
        let data = json!({"id": s["id"], "company": s["company"], "city": s["city"]});
        world.send(base, &w("supplier.joined"), warehouse, s["id"].as_str(), data, None);
    }
    let supplier = suppliers[0]["id"].clone();
    let mut products: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    for p in rows(seed, "products")? {
        let mut product = Map::new();
        for (k, v) in [
            ("id", p["id"].clone()),
            ("name", p["name"].clone()),
            ("supplier_id", p["supplier_id"].clone()),
            ("unit_price", p["unit_price"].clone()),
            ("stock", json!(100_000)),
            ("made_to_order", json!(false)),
            ("available", json!(true)),
            ("spoils_in_days", Value::Null),
        ] {
            product.insert(k.into(), v);
        }
        products.insert(p["name"].as_str().unwrap_or_default().to_string(), product);
    }
    let cast_products: &[(&str, i64, bool, bool, Option<i64>)] = &[
        ("The Lannister Crown, Remade to Order", 0, true, true, None),
        ("A Lannister Cloak, Made to Order", 0, true, true, None),
        ("Longswords, Jon's Lot", 5, false, true, None),
        ("Fresh Dornish Figs", 1000, false, true, Some(5)),
        ("Dragon Egg, Pre-order", 0, false, false, None),
    ];
    for &(name, stock, made, available, spoils) in cast_products {
        let mut product = Map::new();
        product.insert("id".into(), json!(mint(0, format!("product\n{name}").as_bytes())));
        product.insert("name".into(), json!(name));
        product.insert("supplier_id".into(), supplier.clone());
        product.insert("unit_price".into(), json!(100.0));
        product.insert("stock".into(), json!(stock));
        product.insert("made_to_order".into(), json!(made));
        product.insert("available".into(), json!(available));
        product.insert("spoils_in_days".into(), json!(spoils));
        products.insert(name.to_string(), product);
    }
    for product in products.values() {
        let id = product["id"].as_str().map(str::to_string);
        world.send(base, &w("product.listed"), warehouse, id.as_deref(), Value::Object(product.clone()), None);
    }
    for e in rows(seed, "employees")? {
        let name = format!("{} {}", e["first_name"].as_str().unwrap_or(""), e["last_name"].as_str().unwrap_or(""));
        let data = json!({"id": e["id"], "name": name, "title": e["title"], "reports_to": e["reports_to"]});
        world.send(base, &w("employee.hired"), warehouse, e["id"].as_str(), data, None);
    }
    for s in rows(seed, "shippers")? {
        world.send(base, &w("shipper.joined"), warehouse, s["id"].as_str(), json!({"id": s["id"], "company": s["company"]}), None);
    }
    let mut customers: BTreeMap<String, String> = BTreeMap::new();
    for c in rows(seed, "customers")? {
        let data = json!({"id": c["id"], "name": c["company"], "city": c["city"]});
        world.send(base, &w("customer.joined"), web, c["id"].as_str(), data, None);
        customers.insert(c["id"].as_str().unwrap_or_default().to_string(), c["city"].as_str().unwrap_or_default().to_string());
    }
    let cast = ["Cersei", "Jon Snow", "Davos", "Tyrion", "Arya", "Daenerys", "Hodor", "Lion", "Tin Man"];
    let mut who: BTreeMap<&str, String> = BTreeMap::new();
    for name in cast {
        let id = mint(0, format!("customer\n{name}").as_bytes());
        world.send(base, &w("customer.joined"), web, Some(&id), json!({"id": id, "name": name, "city": "King's Landing"}), None);
        who.insert(name, id);
    }

    // The seed's orders: placed, paid an hour later, collected on their shipped date and
    // delivered three days after; an order never shipped is never collected.
    let orders = rows(seed, "orders")?;
    let details = rows(seed, "order_details")?;
    let by_id: BTreeMap<String, &Map<String, Value>> = rows(seed, "products")?
        .iter()
        .filter_map(|p| p["name"].as_str().and_then(|n| products.get(n)).map(|q| (p["id"].as_str().unwrap_or("").to_string(), q)))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(k, v)| (k, v))
        .collect();
    for order in &orders {
        let id = order["id"].as_str().unwrap_or_default().to_string();
        let placed = date_ms(order["order_date"].as_str().unwrap_or(""))?;
        let lines: Vec<Value> = details
            .iter()
            .filter(|d| d["order_id"].as_str() == Some(id.as_str()))
            .map(|d| {
                let spoils = by_id.get(d["product_id"].as_str().unwrap_or("")).and_then(|p| p.get("spoils_in_days")).cloned().unwrap_or(Value::Null);
                json!({"line_id": d["id"], "order_id": id, "product_id": d["product_id"], "quantity": d["quantity"],
                       "unit_price": d["unit_price"], "spoils_in_days": spoils})
            })
            .collect();
        let amount: f64 = lines.iter().map(|l| l["unit_price"].as_f64().unwrap_or(0.0) * l["quantity"].as_f64().unwrap_or(0.0)).sum();
        let customer = order["customer_id"].as_str().unwrap_or_default();
        let data = json!({"order_id": id, "customer_id": customer, "placed_on": order["order_date"], "required_on": order["required_date"],
                          "ship_to": customers.get(customer).cloned().unwrap_or_default(), "lines": lines});
        let root = world.send(placed, &w("order.placed"), web, Some(&id), data, None);
        world.send(placed + HOUR, &w("payment.received"), "westwind/payments", Some(&id), json!({"order_id": id, "amount": amount}), Some(&root));
        if let Some(shipped) = order["shipped_date"].as_str() {
            let at = date_ms(shipped)?;
            let shipment = mint(at, format!("shipment\n{id}").as_bytes());
            let data = json!({"order_id": id, "shipment_id": shipment, "tracking": format!("WW-{}", &shipment[24..])});
            world.send(at, &w("carrier.collected"), carrier, Some(&id), data.clone(), Some(&root));
            world.send(at + 3 * DAY, &w("carrier.delivered"), carrier, Some(&id), data, Some(&root));
        }
        world.seed_orders.push(id);
    }

    // The cast.
    let place = |world: &mut World, name: &str, n: usize, at: i64, items: &[(&str, i64)], required: Option<i64>| -> (String, String) {
        let order = mint(at, format!("order\n{name}\n{n}").as_bytes());
        let lines: Vec<Value> = items.iter().enumerate().map(|(i, (p, q))| line(&order, i, &products[*p], *q)).collect();
        let data = json!({"order_id": order, "customer_id": who[name], "placed_on": format_time(at)[..10],
                          "required_on": required.map(|r| format_time(r)[..10].to_string()), "ship_to": "King's Landing", "lines": lines});
        let root = world.send(at, &w("order.placed"), web, Some(&order), data, None);
        (order, root)
    };
    let pay = |world: &mut World, order: &str, root: &str, at: i64| {
        world.send(at + HOUR, &w("payment.received"), "westwind/payments", Some(order), json!({"order_id": order, "amount": 100.0}), Some(root));
    };
    let ship = |world: &mut World, order: &str, root: &str, at: i64, n: usize| -> Value {
        let shipment = mint(at, format!("shipment\n{order}\n{n}").as_bytes());
        let data = json!({"order_id": order, "shipment_id": shipment, "tracking": format!("WW-{}", &shipment[24..])});
        world.send(at, &w("carrier.collected"), carrier, Some(order), data.clone(), Some(root));
        data
    };
    let deliver = |world: &mut World, order: &str, root: &str, at: i64, data: &Value| {
        world.send(at, &w("carrier.delivered"), carrier, Some(order), data.clone(), Some(root));
    };
    let story = |world: &mut World, name: &str, root: &str, at: i64, expected: Vec<(i64, String)>| {
        world.stories.push(Story { who: name.to_string(), root: root.to_string(), at, expected });
    };
    let named = |t: Vec<(i64, &str)>| -> Vec<(i64, String)> { t.into_iter().map(|(o, ty)| (o, ty.to_string())).collect() };
    let mut cast_day = date_ms("2025-03-03")? - 9 * HOUR;
    let mut next_day = || {
        cast_day += 7 * DAY;
        cast_day
    };

    // Cersei: made to order, late, the wrong item, returned, a remake approved, compensated.
    {
        let t = next_day();
        let (order, root) = place(&mut world, "Cersei", 0, t, &[("The Lannister Crown, Remade to Order", 1)], Some(t + 10 * DAY));
        pay(&mut world, &order, &root, t);
        let crown = products["The Lannister Crown, Remade to Order"]["id"].clone();
        world.send(t + 20 * DAY, &w("supplier.delivered"), "westwind/suppliers", None, json!({"product_id": crown, "quantity": 1}), Some(&root));
        let s = ship(&mut world, &order, &root, t + 21 * DAY, 0);
        deliver(&mut world, &order, &root, t + 24 * DAY, &s);
        let ticket = world.send(t + 25 * DAY, &w("ticket.opened"), web, Some(&order), json!({"order_id": order, "text": "Wrong item: this is not the crown I asked for."}), Some(&root));
        world.send(t + 26 * DAY, &w("return.shipped"), web, Some(&order), json!({"ticket": ticket, "order_id": order}), Some(&root));
        world.send(t + 27 * DAY, &w("remake.approved"), "westwind/staff", Some(&order), json!({"ticket": ticket, "order_id": order, "approver": "Pycelle"}), Some(&root));
        let mut e = named(vec![
            (0, "order.placed"), (HOUR, "payment.received"), (HOUR, "order.accepted"), (HOUR, "line.requested"),
            (HOUR, "line.backordered"), (HOUR, "supplier.ordered"), (DAY, "accept.payment_due"),
            (20 * DAY, "supplier.delivered"), (20 * DAY, "line.allocated"), (20 * DAY, "line.picked"), (20 * DAY, "order.packed"),
            (50 * DAY, "ship.overdue"),
            (25 * DAY, "ticket.opened"), (25 * DAY, "ticket.triaged"), (25 * DAY, "remake.requested"), (25 * DAY, "return.authorized"),
            (28 * DAY, "support.approval_due"), (39 * DAY, "returns.due"),
            (26 * DAY, "return.shipped"), (26 * DAY, "return.received"),
            (27 * DAY, "remake.approved"), (27 * DAY, "remake.ordered"), (27 * DAY, "compensation.issued"),
        ]);
        e.extend(named(shipped_trail(21 * DAY, true)));
        story(&mut world, "Cersei", &root, t, e);
    }

    // Jon Snow: six lines, one backordered, two shipments, an address change mid-shipment.
    {
        let t = next_day();
        let items = [("Myrish Lace", 2), ("Salt Cod", 3), ("Onions, Yellow", 10), ("Saffron", 1), ("Black Pepper", 4), ("Longswords, Jon's Lot", 20)];
        let (order, root) = place(&mut world, "Jon Snow", 0, t, &items, None);
        pay(&mut world, &order, &root, t);
        let s1 = ship(&mut world, &order, &root, t + 2 * DAY, 0);
        world.send(t + 3 * DAY, &w("address.changed"), web, Some(&order), json!({"order_id": order, "ship_to": "Castle Black"}), Some(&root));
        deliver(&mut world, &order, &root, t + 5 * DAY, &s1);
        let swords = products["Longswords, Jon's Lot"]["id"].clone();
        world.send(t + 10 * DAY, &w("supplier.delivered"), "westwind/suppliers", None, json!({"product_id": swords, "quantity": 30}), Some(&root));
        let s2 = ship(&mut world, &order, &root, t + 11 * DAY, 1);
        deliver(&mut world, &order, &root, t + 14 * DAY, &s2);
        let mut e = named(vec![(0, "order.placed"), (HOUR, "payment.received"), (HOUR, "order.accepted"), (DAY, "accept.payment_due")]);
        for _ in 0..6 {
            e.push((HOUR, "line.requested".into()));
        }
        for _ in 0..5 {
            e.extend(named(vec![(HOUR, "line.allocated"), (HOUR, "line.picked")]));
        }
        e.extend(named(vec![
            (HOUR, "line.backordered"), (HOUR, "supplier.ordered"), (HOUR, "order.packed"), (HOUR + 30 * DAY, "ship.overdue"),
            (2 * DAY, "carrier.collected"), (2 * DAY, "order.shipped"), (3 * DAY, "address.changed"), (3 * DAY, "shipment.redirected"),
            (5 * DAY, "carrier.delivered"), (5 * DAY, "order.delivered"), (19 * DAY, "aftership.window_closed"), (19 * DAY, "order.closed"),
            (10 * DAY, "supplier.delivered"), (10 * DAY, "line.allocated"), (10 * DAY, "line.picked"), (10 * DAY, "order.packed"),
            (40 * DAY, "ship.overdue"),
            (11 * DAY, "carrier.collected"), (11 * DAY, "order.shipped"), (14 * DAY, "carrier.delivered"), (14 * DAY, "order.delivered"),
            (28 * DAY, "aftership.window_closed"), (28 * DAY, "order.closed"),
        ]));
        story(&mut world, "Jon Snow", &root, t, e);
    }

    // Davos: a perishable, lost in transit, spoiled by its spoil-by time.
    {
        let t = next_day();
        let (order, root) = place(&mut world, "Davos", 0, t, &[("Fresh Dornish Figs", 12)], None);
        pay(&mut world, &order, &root, t);
        let s = ship(&mut world, &order, &root, t + 2 * DAY, 0);
        world.send(t + 3 * DAY, &w("carrier.lost"), carrier, Some(&order), s, Some(&root));
        let e = named(vec![
            (0, "order.placed"), (HOUR, "payment.received"), (HOUR, "order.accepted"), (DAY, "accept.payment_due"),
            (HOUR, "line.requested"), (HOUR, "line.allocated"), (HOUR, "line.picked"), (HOUR, "order.packed"), (HOUR + 30 * DAY, "ship.overdue"),
            (2 * DAY, "carrier.collected"), (2 * DAY, "order.shipped"), (3 * DAY, "carrier.lost"), (3 * DAY, "order.lost"),
            (3 * DAY, "refund.issued"), (7 * DAY, "deliver.spoil_check"), (7 * DAY, "order.spoiled"),
        ]);
        story(&mut world, "Davos", &root, t, e);
    }

    // Tyrion: a monthly subscription, each next order placed by a timer, for ten years.
    {
        let t = next_day();
        let sub = mint(t, b"subscription\nTyrion");
        let wine = &products["Arbor Gold, Untaxed"];
        let data = json!({"subscription_id": sub, "customer_id": who["Tyrion"], "product_id": wine["id"], "quantity": 2,
                          "unit_price": wine["unit_price"], "months": MONTHS, "ship_to": "Casterly Rock"});
        let root = world.send(t, &w("subscription.started"), web, Some(&sub), data, None);
        let mut e = vec![(0, "subscription.started".to_string())];
        for n in 0..MONTHS {
            let at = t + n * 30 * DAY;
            let order = subscription_order(&sub, n);
            let shipment = mint(at, format!("shipment\n{order}").as_bytes());
            let s = json!({"order_id": order, "shipment_id": shipment, "tracking": format!("WW-{}", &shipment[24..])});
            world.send(at + 2 * DAY, &w("carrier.collected"), carrier, Some(&order), s.clone(), Some(&root));
            world.send(at + 5 * DAY, &w("carrier.delivered"), carrier, Some(&order), s, Some(&root));
            let o = n * 30 * DAY;
            if n > 0 {
                e.push((o, "subscription.due".into()));
            }
            for (d, ty) in plain_trail(1, 2 * DAY, false) {
                e.push((o + d, ty));
            }
        }
        e.push((MONTHS * 30 * DAY, "subscription.due".into()));
        story(&mut world, "Tyrion", &root, t, e);
    }

    // Arya: cancelled before paying, after packing, while backordered, and after shipping.
    {
        let t = next_day();
        let (order, root) = place(&mut world, "Arya", 0, t, &[("Saffron", 1)], None);
        world.send(t + HOUR / 2, &w("order.cancel_requested"), web, Some(&order), json!({"order_id": order}), Some(&root));
        story(&mut world, "Arya, before paying", &root, t, named(vec![
            (0, "order.placed"), (HOUR / 2, "order.cancel_requested"), (HOUR / 2, "order.cancelled"), (DAY, "accept.payment_due"),
        ]));

        let t = next_day();
        let (order, root) = place(&mut world, "Arya", 1, t, &[("Saffron", 1), ("Black Pepper", 2)], None);
        pay(&mut world, &order, &root, t);
        world.send(t + 12 * HOUR, &w("order.cancel_requested"), web, Some(&order), json!({"order_id": order}), Some(&root));
        let mut e = named(vec![(0, "order.placed"), (HOUR, "payment.received"), (HOUR, "order.accepted"), (DAY, "accept.payment_due")]);
        for _ in 0..2 {
            e.extend(named(vec![(HOUR, "line.requested"), (HOUR, "line.allocated"), (HOUR, "line.picked")]));
        }
        e.extend(named(vec![
            (HOUR, "order.packed"), (HOUR + 30 * DAY, "ship.overdue"), (12 * HOUR, "order.cancel_requested"),
            (12 * HOUR, "order.cancelled"), (12 * HOUR, "refund.issued"), (12 * HOUR, "stock.released"), (12 * HOUR, "stock.released"),
        ]));
        story(&mut world, "Arya, after packing", &root, t, e);

        let t = next_day();
        let (order, root) = place(&mut world, "Arya", 2, t, &[("A Lannister Cloak, Made to Order", 1)], None);
        pay(&mut world, &order, &root, t);
        world.send(t + 2 * DAY, &w("order.cancel_requested"), web, Some(&order), json!({"order_id": order}), Some(&root));
        story(&mut world, "Arya, while backordered", &root, t, named(vec![
            (0, "order.placed"), (HOUR, "payment.received"), (HOUR, "order.accepted"), (DAY, "accept.payment_due"),
            (HOUR, "line.requested"), (HOUR, "line.backordered"), (HOUR, "supplier.ordered"),
            (2 * DAY, "order.cancel_requested"), (2 * DAY, "order.cancelled"), (2 * DAY, "refund.issued"),
        ]));

        let t = next_day();
        let (order, root) = place(&mut world, "Arya", 3, t, &[("Salt Cod", 1)], None);
        pay(&mut world, &order, &root, t);
        let s = ship(&mut world, &order, &root, t + 2 * DAY, 0);
        world.send(t + 3 * DAY, &w("order.cancel_requested"), web, Some(&order), json!({"order_id": order}), Some(&root));
        deliver(&mut world, &order, &root, t + 5 * DAY, &s);
        let mut e = plain_trail(1, 2 * DAY, false);
        e.extend(named(vec![(3 * DAY, "order.cancel_requested"), (3 * DAY, "cancel.refused")]));
        story(&mut world, "Arya, after shipping", &root, t, e);
    }

    // Daenerys: a pre-order with no date, open by design.
    {
        let t = next_day();
        let (order, root) = place(&mut world, "Daenerys", 0, t, &[("Dragon Egg, Pre-order", 3)], None);
        pay(&mut world, &order, &root, t);
        story(&mut world, "Daenerys", &root, t, named(vec![
            (0, "order.placed"), (HOUR, "payment.received"), (HOUR, "order.accepted"), (DAY, "accept.payment_due"),
            (HOUR, "line.requested"), (HOUR, "line.backordered"),
        ]));
    }

    // Hodor: the same delivery twice under one id, and two identical tickets under two ids.
    {
        let t = next_day();
        let (order, root) = place(&mut world, "Hodor", 0, t, &[("Onions, Red", 5)], None);
        pay(&mut world, &order, &root, t);
        let s = ship(&mut world, &order, &root, t + 2 * DAY, 0);
        let at = t + 5 * DAY;
        let id = mint(at, format!("hodor delivered\n{order}").as_bytes());
        for arrival in [at, at + HOUR, at + 3 * DAY] {
            world.send_as(&id, at, arrival, &w("carrier.delivered"), carrier, Some(&order), s.clone(), Some(&root));
        }
        let mut e = plain_trail(1, 2 * DAY, false);
        for _ in 0..2 {
            world.send(t + 6 * DAY, &w("ticket.opened"), web, Some(&order), json!({"order_id": order, "text": "Hodor. Hodor hodor."}), Some(&root));
            e.extend(named(vec![(6 * DAY, "ticket.opened"), (6 * DAY, "ticket.triaged"), (6 * DAY, "ticket.closed")]));
        }
        story(&mut world, "Hodor", &root, t, e);
    }

    // Lion: an abandoned order, an approval nobody answers, a return never sent.
    {
        let t = next_day();
        let (_, root) = place(&mut world, "Lion", 0, t, &[("Saffron", 1)], None);
        story(&mut world, "Lion, abandoned", &root, t, named(vec![(0, "order.placed"), (DAY, "accept.payment_due"), (DAY, "order.abandoned")]));

        let t = next_day();
        let (order, root) = place(&mut world, "Lion", 1, t, &[("Myrish Lace", 1)], None);
        pay(&mut world, &order, &root, t);
        let s = ship(&mut world, &order, &root, t + 2 * DAY, 0);
        deliver(&mut world, &order, &root, t + 5 * DAY, &s);
        world.send(t + 6 * DAY, &w("ticket.opened"), web, Some(&order), json!({"order_id": order, "text": "Wrong item. I want a remake."}), Some(&root));
        let mut e = plain_trail(1, 2 * DAY, false);
        e.extend(named(vec![
            (6 * DAY, "ticket.opened"), (6 * DAY, "ticket.triaged"), (6 * DAY, "remake.requested"), (6 * DAY, "return.authorized"),
            (9 * DAY, "support.approval_due"), (9 * DAY, "remake.expired"), (20 * DAY, "returns.due"), (20 * DAY, "return.expired"),
        ]));
        story(&mut world, "Lion, unanswered", &root, t, e);
    }

    // Tin Man: his ticket's triage stalls mid-step the first time.
    {
        let t = next_day();
        let (order, root) = place(&mut world, "Tin Man", 0, t, &[("Salt Cod", 1)], None);
        pay(&mut world, &order, &root, t);
        let s = ship(&mut world, &order, &root, t + 2 * DAY, 0);
        deliver(&mut world, &order, &root, t + 5 * DAY, &s);
        world.send(t + 6 * DAY, &w("ticket.opened"), web, Some(&order), json!({"order_id": order, "text": "[stall] If I only had a heart."}), Some(&root));
        let mut e = plain_trail(1, 2 * DAY, false);
        e.extend(named(vec![(6 * DAY, "ticket.opened"), (6 * DAY, "ticket.triaged"), (6 * DAY, "ticket.closed")]));
        story(&mut world, "Tin Man", &root, t, e);
    }

    // Oz, beside Westwind in the same store; and the Scarecrow, sent at Westwind from Oz.
    let oz = "oz/emerald-city";
    let t = next_day();
    let mut citizens = BTreeMap::new();
    for name in ["Dorothy", "Scarecrow", "Toto"] {
        let id = mint(0, format!("oz citizen\n{name}").as_bytes());
        world.send(t, "oz.citizen.joined", oz, Some(&id), json!({"id": id, "name": name}), None);
        citizens.insert(name, id);
    }
    let dorothy = mint(t, b"oz order Dorothy");
    world.send(t + HOUR, "oz.order.placed", oz, Some(&dorothy), json!({"order_id": dorothy, "citizen_id": citizens["Dorothy"], "item": "Ruby slippers"}), None);
    let scarecrow = mint(t, b"scarecrow order");
    let lines = vec![json!({"line_id": mint(0, b"scarecrow line"), "order_id": scarecrow, "product_id": products["Saffron"]["id"],
                            "quantity": 1, "unit_price": 14.0, "spoils_in_days": null})];
    world.send(t + 2 * HOUR, &w("order.placed"), oz, Some(&scarecrow),
        json!({"order_id": scarecrow, "customer_id": citizens["Scarecrow"], "placed_on": "2025-06-01", "required_on": null, "ship_to": "Emerald City", "lines": lines}), None);
    world.send(t + 3 * HOUR, &w("ticket.opened"), oz, Some(&scarecrow), json!({"order_id": scarecrow, "text": "If I only had a brain."}), None);
    world.send(t + 4 * HOUR, "oz.order.placed", web, Some(&scarecrow), json!({"order_id": scarecrow, "citizen_id": citizens["Scarecrow"], "item": "A brain"}), None);
    world.send(t + 5 * HOUR, "oz.citizen.joined", web, None, json!({"id": who["Arya"], "name": "Arya"}), None);

    // Malformed events, each failing differently.
    world.push_line("this is not an event".into(), t + 6 * HOUR, t + 6 * HOUR);
    world.push_line(json!({"specversion": "0.3", "id": mint(t, b"m1"), "source": web, "type": "westwind.customer.joined", "time": format_time(t + 6 * HOUR), "data": {}}).to_string(), t + 6 * HOUR, t + 6 * HOUR);
    world.push_line(json!({"specversion": "1.0", "id": mint(t, b"m2"), "source": web, "type": "westwind.order.placed", "time": format_time(t + 6 * HOUR), "data": {"order_id": "x"}}).to_string(), t + 6 * HOUR, t + 6 * HOUR);
    world.push_line(json!({"specversion": "1.0", "id": mint(t, b"m3"), "source": web, "type": "westwind.dragon.sighted", "time": format_time(t + 6 * HOUR), "data": {}}).to_string(), t + 6 * HOUR, t + 6 * HOUR);

    // One event later than the lateness allows: a customer whose joining arrives ten days late.
    let late = mint(t, b"late customer");
    world.send_as(&late, t, t + 10 * DAY, &w("customer.joined"), web, Some(&late), json!({"id": late, "name": "Late Larry", "city": "Pentos"}), None);

    // The clock: a tick each day for ten years, so event time reaches Tyrion's last month.
    for day in 0..=3720 {
        let at = base + day * DAY;
        world.send(at, &w("clock.tick"), "westwind/clock", None, json!({"day": format_time(at)[..10]}), None);
    }
    world.arrange();
    Ok(world)
}

/// The same orders the row-for-row load stored, event-shaped: each order one `order.placed` with
/// its lines inside, each shipped order one `order.shipped`, after the suppliers, products,
/// customers, employees and shippers. Returns the events and how many orders they hold.
///
/// # Errors
/// The seed cannot be read.
pub fn reshaped(seed: &Connection, row_events: &[Event]) -> Result<(Vec<Event>, usize), String> {
    let mut out = Vec::new();
    let mut lines: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for e in row_events.iter().filter(|e| e.ty == "westwind.order_line.added") {
        let order = e.data["order_id"].as_str().unwrap_or_default().to_string();
        lines.entry(order.clone()).or_default().push(json!({
            "line_id": e.data["id"], "order_id": order, "product_id": e.data["product_id"],
            "quantity": e.data["quantity"], "unit_price": e.data["unit_price"], "spoils_in_days": null,
        }));
    }
    let cities: BTreeMap<String, String> = rows(seed, "customers")?
        .iter()
        .map(|c| (c["id"].as_str().unwrap_or("").to_string(), c["city"].as_str().unwrap_or("").to_string()))
        .collect();
    let mut orders = 0;
    for e in row_events {
        let d = &e.data;
        let (ty, data) = match e.ty.as_str() {
            "westwind.supplier.joined" => ("westwind.supplier.joined", json!({"id": d["id"], "company": d["company"], "city": d["city"]})),
            "westwind.product.listed" => ("westwind.product.listed", json!({"id": d["id"], "name": d["name"], "supplier_id": d["supplier_id"],
                "unit_price": d["unit_price"], "stock": d["in_stock"], "made_to_order": false, "available": d["discontinued"] == 0})),
            "westwind.customer.joined" => ("westwind.customer.joined", json!({"id": d["id"], "name": d["company"], "city": d["city"]})),
            "westwind.employee.hired" => ("westwind.employee.hired", json!({"id": d["id"],
                "name": format!("{} {}", d["first_name"].as_str().unwrap_or(""), d["last_name"].as_str().unwrap_or("")), "title": d["title"], "reports_to": d["reports_to"]})),
            "westwind.shipper.joined" => ("westwind.shipper.joined", json!({"id": d["id"], "company": d["company"]})),
            "westwind.order.placed" => {
                orders += 1;
                let id = d["id"].as_str().unwrap_or_default();
                ("westwind.order.placed", json!({"order_id": id, "customer_id": d["customer_id"], "placed_on": d["order_date"],
                    "required_on": d["required_date"], "ship_to": cities.get(d["customer_id"].as_str().unwrap_or("")).cloned().unwrap_or_default(),
                    "lines": lines.remove(id).unwrap_or_default()}))
            }
            "westwind.order.shipped" => {
                let id = d["id"].as_str().unwrap_or_default();
                let shipment = mint(e.time, format!("shipment\n{id}").as_bytes());
                ("westwind.order.shipped", json!({"order_id": id, "shipment_id": shipment, "tracking": format!("WW-{}", &shipment[24..])}))
            }
            _ => continue,
        };
        out.push(Event { id: e.id.clone(), ty: ty.to_string(), source: e.source.clone(), subject: e.subject.clone(), time: e.time, data, causation: None, position: e.position });
    }
    Ok((out, orders))
}
