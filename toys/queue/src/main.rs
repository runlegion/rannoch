//! Toy for research 01a1ee4b: FR-RANNOCH-019's chain rules as SQL over chains of events.
//! Time is logical: each row's UUIDv7 carries the toy's clock, in milliseconds.

use rusqlite::{Connection, params};
use uuid::{NoContext, Timestamp, Uuid};

const TABLE: &str = "CREATE TABLE _events (id TEXT PRIMARY KEY, source TEXT, ty INTEGER NOT NULL, data TEXT NOT NULL, run_at INTEGER, status INTEGER NOT NULL CHECK (status IN (-1, 0, 1, 2)));
CREATE INDEX _events_source ON _events (source); CREATE INDEX _events_status ON _events (status);";
/// The due 0s with no status event whose source is them.
const SELECT: &str = "SELECT e.id FROM _events e WHERE e.status = 0 AND (e.run_at IS NULL OR e.run_at <= ?1)
AND NOT EXISTS (SELECT 1 FROM _events s WHERE s.source = e.id AND s.status <> 0) ORDER BY e.id";
/// Every row below `?1`, read from the tree's ids into _events by primary key.
const TREE: &str = "WITH RECURSIVE tree(id) AS (SELECT id FROM _events WHERE source = ?1
UNION SELECT e.id FROM _events e JOIN tree t ON e.source = t.id)
SELECT e.id, e.status FROM tree CROSS JOIN _events e ON e.id = tree.id";

struct Sim {
    c: Connection,
    now: u64,
    attempts: usize,
    backoff: u64,
    window: u64,
}

impl Sim {
    fn new(attempts: usize) -> Self {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(TABLE).unwrap();
        Sim { c, now: 1_000_000, attempts, backoff: 100, window: 1_000 }
    }
    fn id(&mut self) -> String {
        self.now += 1;
        Uuid::new_v7(Timestamp::from_unix(NoContext, self.now / 1000, ((self.now % 1000) * 1_000_000) as u32)).to_string()
    }
    fn at(id: &str) -> u64 {
        let (s, n) = Uuid::parse_str(id).unwrap().get_timestamp().unwrap().to_unix();
        s * 1000 + u64::from(n) / 1_000_000
    }
    fn put(&mut self, source: Option<&str>, ty: i64, data: &str, run_at: Option<u64>, status: i64) -> String {
        let id = self.id();
        self.c.execute("INSERT INTO _events VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![id, source, ty, data, run_at.map(|t| t as i64), status]).unwrap();
        id
    }
    fn row(&self, id: &str) -> (Option<String>, i64, String) {
        self.c.query_row("SELECT source, ty, data FROM _events WHERE id = ?1", [id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap()
    }
    /// The original event of an attempt: a retry carries its original's ty and data, with the original as source.
    fn original(&self, x: &str) -> String {
        let (source, ty, data) = self.row(x);
        if let Some(s) = source {
            let (_, sty, sdata) = self.row(&s);
            if sty == ty && sdata == data {
                return s;
            }
        }
        x.to_owned()
    }
    fn tree(&self, x: &str) -> Vec<(String, i64)> {
        let mut s = self.c.prepare(TREE).unwrap();
        s.query_map([x], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(Result::unwrap).collect()
    }
    fn minus_ones(&self, o: &str) -> usize {
        self.tree(o).iter().filter(|(_, s)| *s == -1).count()
    }
    /// The select: take the due 0s and send a 1 for each.
    fn select(&mut self) -> Vec<String> {
        let taken: Vec<String> = {
            let mut s = self.c.prepare(SELECT).unwrap();
            s.query_map([self.now as i64], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
        };
        for x in &taken {
            let ty = self.row(x).1;
            self.put(Some(x), ty, "{}", None, 1);
        }
        taken
    }
    fn succeed(&mut self, x: &str, outputs: &[i64]) -> Vec<String> {
        let ty = self.row(x).1;
        self.put(Some(x), ty, "{}", None, 2);
        outputs.iter().map(|t| self.put(Some(x), *t, "{\"out\":1}", None, 0)).collect()
    }
    /// A failed attempt: a -1 for it, and a retry while the -1s in the original's tree are fewer than the attempts.
    fn fail(&mut self, x: &str) -> Option<String> {
        let ty = self.row(x).1;
        self.put(Some(x), ty, "{}", None, -1);
        let o = self.original(x);
        if self.minus_ones(&o) < self.attempts {
            let (_, oty, odata) = self.row(&o);
            let due = self.now + self.backoff;
            return Some(self.put(Some(&o), oty, &odata, Some(due), 0));
        }
        None
    }
    /// The hunt: a 1 older than the window with nothing after it in the event's tree is sent again as a retry.
    fn hunt(&mut self) -> Vec<String> {
        let ones: Vec<(String, String)> = {
            let mut s = self.c.prepare("SELECT id, source FROM _events WHERE status = 1").unwrap();
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(Result::unwrap).collect()
        };
        let mut sent = Vec::new();
        for (one, x) in ones {
            if Self::at(&one) + self.window > self.now {
                continue;
            }
            let o = self.original(&x);
            let after = self.tree(&o).iter().any(|(id, _)| *id != one && Self::at(id) > Self::at(&one));
            if !after {
                let (_, oty, odata) = self.row(&o);
                let id = self.put(Some(&o), oty, &odata, None, 0);
                sent.push(id);
            }
        }
        sent
    }
    fn count(&self, sql: &str, p: &str) -> i64 {
        self.c.query_row(sql, [p], |r| r.get(0)).unwrap()
    }
}

fn check(name: &str, ok: bool, detail: String) {
    println!("{} {name}: {detail}", if ok { "HOLDS  " } else { "REFUTED" });
}

fn main() {
    // P1: success
    let mut s = Sim::new(3);
    let o = s.put(None, 1, "{\"n\":1}", None, 0);
    let t1 = s.select();
    s.succeed(&o, &[]);
    let t2 = s.select();
    s.now += 5_000;
    let h = s.hunt();
    check("P1 success", t1 == [o.clone()] && t2.is_empty() && h.is_empty() && s.count("SELECT count(*) FROM _events WHERE source = ?1 AND status = 2", &o) == 1,
        format!("taken {:?} then {:?}, hunt sent {}", t1.len(), t2.len(), h.len()));

    // P2: fail once then succeed
    let mut s = Sim::new(3);
    let o = s.put(None, 1, "{\"n\":2}", None, 0);
    s.select();
    let r = s.fail(&o).expect("a retry");
    let early = s.select();
    s.now += 200;
    let later = s.select();
    s.succeed(&r, &[]);
    s.now += 5_000;
    let h = s.hunt();
    check("P2 fail once then succeed", early.is_empty() && later == [r.clone()] && s.minus_ones(&o) == 1 && h.is_empty(),
        format!("before run_at {:?}, after {:?}, -1s {}, hunt sent {}", early.len(), later.len(), s.minus_ones(&o), h.len()));

    // P3 and P7: fail every time, attempts 3
    let mut s = Sim::new(3);
    let o = s.put(None, 1, "{\"n\":3}", None, 0);
    let mut x = s.select().pop().unwrap();
    let mut retries = 0;
    let mut early_taken = false;
    loop {
        match s.fail(&x) {
            Some(r) => {
                retries += 1;
                early_taken |= s.select().contains(&r);
                s.now += 200;
                x = s.select().pop().unwrap();
            }
            None => break,
        }
    }
    s.now += 500;
    let after = s.select();
    check("P3 binned at attempts", s.minus_ones(&o) == 3 && retries == 2 && after.is_empty(),
        format!("-1s in tree {}, retries {retries}, taken after binning {}", s.minus_ones(&o), after.len()));
    check("P7 retry waits for run_at", !early_taken, format!("a retry taken before its run_at: {early_taken}"));

    // P4: an output of a successful flow fails every time
    let mut s = Sim::new(3);
    let o = s.put(None, 1, "{\"n\":4}", None, 0);
    s.select();
    let e = s.succeed(&o, &[5]).pop().unwrap();
    let before = s.minus_ones(&o);
    let mut x = s.select().pop().unwrap();
    while let Some(r) = s.fail(&x) {
        let _ = r;
        s.now += 200;
        x = s.select().pop().unwrap();
    }
    let e_count = s.minus_ones(&e);
    let o_after = s.minus_ones(&o);
    check("P4 output's own attempts", e_count == 3 && o_after == before,
        format!("output's -1s {e_count}; original's -1s before {before}, after {o_after}"));

    // P5 and P6: crash after the 1
    let mut s = Sim::new(3);
    let o = s.put(None, 1, "{\"n\":5}", None, 0);
    s.select();
    s.now += 500;
    let young = s.hunt();
    s.now += 1_000;
    let first = s.hunt();
    s.now += 1_000;
    let second = s.hunt();
    check("P6 young 1 left alone", young.is_empty(), format!("hunt at half the window sent {}", young.len()));
    check("P5 hunt sends a crash once", first.len() == 1 && second.is_empty(), format!("first hunt sent {}, second {}", first.len(), second.len()));

    // P8: a run that crashes every time
    let mut s = Sim::new(3);
    let o = s.put(None, 1, "{\"n\":8}", None, 0);
    s.select();
    let mut reruns = 0;
    for _ in 0..10 {
        s.now += 1_500;
        let sent = s.hunt();
        reruns += sent.len();
        s.select();
    }
    check("P8 crash every time is never binned", reruns == 10 && s.minus_ones(&o) == 0, format!("hunt reruns in 10 rounds {reruns}, -1s {}", s.minus_ones(&o)));
    let _ = o;
}
