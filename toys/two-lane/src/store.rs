//! The toy's own store, on SQLite, built to the two-lane shape, with versions. One write per
//! changed fact: a version of its row, appended, carrying the value of its kind's declared axis
//! and the known-at from the clock in its event's id. A fact written again unchanged writes
//! nothing. The row as it is now is its latest version. Edges join rows. A flow's state is a part of
//! its row. The host's answers are kept by action and event, in the same transaction as the rows
//! they changed. No log table, no hearing table, no second copy of a row.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::entrance::Event;
use crate::flow::{Answer, State};

/// A row as it is now.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Row {
    /// What the event that defines the row said: an order's placing, a product's listing.
    pub data: Value,
    /// Each type of event that has happened to the row, at its latest time.
    pub types: BTreeMap<String, i64>,
    /// The ids of facts applied within the lateness window.
    pub facts: BTreeMap<String, i64>,
    /// Each flow's state, by flow.
    pub flows: BTreeMap<String, State>,
    pub event: String,
    pub time: i64,
    /// The value of its kind's declared axis at its latest version; 0 for a kind with none.
    #[serde(default)]
    pub axis: i64,
    /// The fact no longer holds under this key: a rename moved it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ended: bool,
}

/// The known-at of an event: the millisecond in its `UUIDv7` id.
#[must_use]
pub fn known_of(event: &Event) -> i64 {
    uuid::Uuid::parse_str(&event.id)
        .ok()
        .and_then(|u| u.get_timestamp())
        .map_or(event.time, |t| {
            let (s, n) = t.to_unix();
            i64::try_from(s).unwrap_or(0) * 1000 + i64::from(n / 1_000_000)
        })
}

/// A kind's declared axis: the build, for build objects; none otherwise.
#[must_use]
pub fn axis_of(kind: &str, event: &Event) -> i64 {
    if kind == "build_object" { event.data.get("build").and_then(Value::as_i64).unwrap_or(0) } else { 0 }
}

/// What a defining event says the row is: for a kind with an axis, its data without the axis.
#[must_use]
pub fn defined(kind: &str, event: &Event) -> Value {
    let mut data = event.data.clone();
    if kind == "build_object"
        && let Value::Object(m) = &mut data
    {
        m.remove("build");
    }
    data
}

/// Where an event's own fact lands: the row's kind and key, whether the event defines the row,
/// and the edges it draws.
pub struct Fact {
    pub kind: &'static str,
    pub key: String,
    pub defines: bool,
    pub edges: Vec<(&'static str, &'static str, String)>,
}

fn s(data: &Value, name: &str) -> String {
    data.get(name).and_then(Value::as_str).unwrap_or_default().to_string()
}

/// The fact an event says, if it says one.
#[must_use]
pub fn fact_of(event: &Event) -> Option<Fact> {
    let d = &event.data;
    let ty = event.ty.as_str();
    let fact = |kind, key: String, defines, edges| Some(Fact { kind, key, defines, edges });
    match ty {
        "westwind.customer.joined" => fact("westwind_customer", s(d, "id"), true, vec![]),
        "westwind.supplier.joined" => fact("westwind_supplier", s(d, "id"), true, vec![]),
        "westwind.shipper.joined" => fact("westwind_shipper", s(d, "id"), true, vec![]),
        "westwind.product.listed" => fact("westwind_product", s(d, "id"), true, vec![("supplied_by", "westwind_supplier", s(d, "supplier_id"))]),
        "westwind.employee.hired" => {
            let boss = s(d, "reports_to");
            let edges = if boss.is_empty() { vec![] } else { vec![("reports_to", "westwind_employee", boss)] };
            fact("westwind_employee", s(d, "id"), true, edges)
        }
        "westwind.order.placed" => {
            let mut edges = vec![("ordered_by", "westwind_customer", s(d, "customer_id"))];
            let products: BTreeSet<String> = d.get("lines").and_then(Value::as_array).map(|l| l.iter().map(|x| s(x, "product_id")).collect()).unwrap_or_default();
            edges.extend(products.into_iter().map(|p| ("contains", "westwind_product", p)));
            fact("westwind_order", s(d, "order_id"), true, edges)
        }
        "westwind.supplier.delivered" => fact("westwind_product", s(d, "product_id"), false, vec![]),
        "westwind.ticket.opened" => fact("westwind_ticket", event.id.clone(), true, vec![("about", "westwind_order", s(d, "order_id"))]),
        "westwind.subscription.started" => fact("westwind_subscription", s(d, "subscription_id"), true, vec![]),
        "westwind.subscription.due" => fact("westwind_subscription", s(d, "subscription_id"), false, vec![]),
        "oz.citizen.joined" => fact("oz_citizen", s(d, "id"), true, vec![]),
        "oz.order.placed" => fact("oz_order", s(d, "order_id"), true, vec![("ordered_by", "oz_citizen", s(d, "citizen_id"))]),
        "westwind.clock.tick" => None,
        "build.object.written" => {
            let dep = d.pointer("/body/depends_on").and_then(Value::as_str).unwrap_or_default().to_string();
            let edges = if dep.is_empty() { vec![] } else { vec![("depends_on", "build_object", dep)] };
            fact("build_object", event.subject.clone().unwrap_or_default(), true, edges)
        }
        t if ["ticket.", "remake.", "return.", "returns.", "support.", "compensation."].iter().any(|p| t.starts_with(&format!("westwind.{p}"))) => {
            fact("westwind_ticket", s(d, "ticket"), false, vec![])
        }
        _ if d.get("order_id").is_some() => fact("westwind_order", s(d, "order_id"), false, vec![]),
        _ => None,
    }
}

/// One shard of the store: one SQLite file.
pub struct Shard {
    conn: Connection,
    cache: HashMap<(String, String), Row>,
    /// Versions to append at the next commit: one per changed fact, each its row as it became.
    versions: Vec<(String, String, i64, i64, String, String)>,
    edges: Vec<(String, String, String, String, String, i64)>,
    links: Vec<(String, String, String, i64, String, String)>,
    renames: HashMap<(String, String), (String, i64)>,
    new_renames: Vec<(String, String, String, i64, String)>,
    amended: Vec<(String, String)>,
    hearings: Option<Connection>,
    dir: PathBuf,
    answers: Vec<((String, String), Answer)>,
    pub writes: u64,
    /// Reads of the row table: a row found (read back), and a row looked for and not there.
    pub found: u64,
    pub absent: u64,
    /// Rows handed from memory instead.
    pub from_memory: u64,
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard").finish_non_exhaustive()
    }
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS versions(kind TEXT NOT NULL, key TEXT NOT NULL, axis INTEGER NOT NULL,
        known INTEGER NOT NULL, event TEXT NOT NULL, body TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS versions_at ON versions(kind, key, axis, known);
    CREATE TABLE IF NOT EXISTS edges(src TEXT NOT NULL, rel TEXT NOT NULL, dst TEXT NOT NULL, known INTEGER NOT NULL,
        PRIMARY KEY(src, rel, dst)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS link_versions(src TEXT NOT NULL, rel TEXT NOT NULL, dst TEXT NOT NULL,
        known INTEGER NOT NULL, event TEXT NOT NULL, state TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS link_versions_at ON link_versions(src, rel, dst, known);
    CREATE TABLE IF NOT EXISTS renames(kind TEXT NOT NULL, old TEXT NOT NULL, new TEXT NOT NULL, known INTEGER NOT NULL,
        event TEXT NOT NULL, PRIMARY KEY(kind, old)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS amended(event TEXT PRIMARY KEY, outcome TEXT NOT NULL) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS edges_to ON edges(dst, rel);
    CREATE TABLE IF NOT EXISTS host(action TEXT NOT NULL, event TEXT NOT NULL, answer TEXT NOT NULL,
        PRIMARY KEY(action, event)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;";

impl Shard {
    /// # Errors
    /// The file cannot be opened.
    pub fn open(path: &Path) -> Result<Shard, String> {
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        conn.busy_timeout(std::time::Duration::from_secs(30)).map_err(|e| e.to_string())?;
        // Versions of 3 KB objects fit one to a 4 KB page with most of a page left over; a
        // larger page packs them. Set before the file has a journal, so it takes.
        let page = std::env::var("TWO_LANE_PAGE").ok().and_then(|v| v.parse::<u32>().ok()).unwrap_or(65536);
        conn.execute_batch(&format!("PRAGMA page_size={page};")).map_err(|e| e.to_string())?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON;").map_err(|e| e.to_string())?;
        conn.execute_batch(SCHEMA).map_err(|e| e.to_string())?;
        let mut renames = HashMap::new();
        {
            let mut st = conn.prepare("SELECT kind, old, new, known FROM renames").map_err(|e| e.to_string())?;
            let rows = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?))).map_err(|e| e.to_string())?;
            for r in rows {
                let (kind, old, new, known) = r.map_err(|e| e.to_string())?;
                renames.insert((kind, old), (new, known));
            }
        }
        let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
        Ok(Shard {
            conn,
            cache: HashMap::new(),
            versions: Vec::new(),
            edges: Vec::new(),
            links: Vec::new(),
            renames,
            new_renames: Vec::new(),
            amended: Vec::new(),
            hearings: None,
            dir,
            answers: Vec::new(),
            writes: 0,
            found: 0,
            absent: 0,
            from_memory: 0,
        })
    }

    /// The row of `kind` keyed `key`. A row this process has written is handed back from memory,
    /// never read back from the file; a row it has not seen is looked for once.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn row(&mut self, kind: &str, key: &str) -> Result<&mut Row, String> {
        let at = (kind.to_string(), key.to_string());
        if self.cache.contains_key(&at) {
            self.from_memory += 1;
        } else {
            let body: Option<String> = self
                .conn
                .prepare_cached("SELECT body FROM versions WHERE kind = ?1 AND key = ?2 ORDER BY axis DESC, rowid DESC LIMIT 1")
                .map_err(|e| e.to_string())?
                .query_row(params![kind, key], |r| r.get(0))
                .optional()
                .map_err(|e| e.to_string())?;
            let row = match body {
                Some(b) => {
                    self.found += 1;
                    serde_json::from_str(&b).map_err(|e| e.to_string())?
                }
                None => {
                    self.absent += 1;
                    Row::default()
                }
            };
            self.cache.insert(at.clone(), row);
        }
        self.cache.get_mut(&at).ok_or_else(|| "no row".to_string())
    }

    /// Appends a version of the row as it is now, made by `event` at `axis`.
    ///
    /// # Errors
    /// The row cannot be written as JSON.
    pub fn touched(&mut self, kind: &str, key: &str, event: &Event, axis: i64) -> Result<(), String> {
        let row = self.cache.get(&(kind.to_string(), key.to_string())).ok_or("no row")?;
        let body = serde_json::to_string(row).map_err(|e| e.to_string())?;
        self.versions.push((kind.to_string(), key.to_string(), axis, known_of(event), event.id.clone(), body));
        Ok(())
    }

    /// The row of `kind` keyed `key` as of `axis`, as known at `known`: its version with the
    /// greatest axis at or before `axis`, then the latest known at or before `known`.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn as_of(&self, kind: &str, key: &str, axis: i64, known: i64) -> Result<Option<Row>, String> {
        let body: Option<String> = self
            .conn
            .prepare_cached(
                "SELECT body FROM versions WHERE kind = ?1 AND key = ?2 AND axis <= ?3 AND known <= ?4
                 ORDER BY axis DESC, known DESC, rowid DESC LIMIT 1",
            )
            .map_err(|e| e.to_string())?
            .query_row(params![kind, key, axis, known], |r| r.get(0))
            .optional()
            .map_err(|e| e.to_string())?;
        body.map(|b| serde_json::from_str(&b).map_err(|e| e.to_string())).transpose()
    }

    pub fn edge(&mut self, src: (&str, &str), rel: &str, dst: (&str, &str), known: i64) {
        self.edges.push((src.0.into(), src.1.into(), rel.into(), dst.0.into(), dst.1.into(), known));
    }

    /// The key a fact of `kind` named `key` lands on, known at `known`: the new key, once a rename
    /// of it is known.
    #[must_use]
    pub fn renamed(&self, kind: &str, key: &str, known: i64) -> String {
        let mut key = key.to_string();
        for _ in 0..16 {
            match self.renames.get(&(kind.to_string(), key.clone())) {
                Some((new, at)) if known >= *at => key = new.clone(),
                _ => break,
            }
        }
        key
    }

    /// One more version of a link: it was found wrong, or it stopped holding.
    pub fn link_change(&mut self, event: &Event) {
        let state = if event.ty.starts_with("rannoch.corrected.") { "wrong" } else { "ended" };
        let rel = event.ty.split('.').nth(2).unwrap_or_default();
        let d = &event.data;
        let (fc, fk) = (s(&d["from"], "class"), s(&d["from"], "key"));
        let (tc, tk) = (s(&d["to"], "class"), s(&d["to"], "key"));
        self.links.push((fk, format!("{fc}.{rel}.{tc}"), tk, known_of(event), event.id.clone(), state.to_string()));
    }

    /// A link's state as known at `known`: holds, wrong or ended; `None` before it was known.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn link_state(&self, src: &str, rel: &str, dst: &str, known: i64) -> Result<Option<String>, String> {
        let later: Option<String> = self
            .conn
            .prepare_cached("SELECT state FROM link_versions WHERE src = ?1 AND rel = ?2 AND dst = ?3 AND known <= ?4 ORDER BY known DESC, rowid DESC LIMIT 1")
            .map_err(|e| e.to_string())?
            .query_row(params![src, rel, dst, known], |r| r.get(0))
            .optional()
            .map_err(|e| e.to_string())?;
        if later.is_some() {
            return Ok(later);
        }
        let held: Option<i64> = self
            .conn
            .prepare_cached("SELECT known FROM edges WHERE src = ?1 AND rel = ?2 AND dst = ?3 AND known <= ?4")
            .map_err(|e| e.to_string())?
            .query_row(params![src, rel, dst, known], |r| r.get(0))
            .optional()
            .map_err(|e| e.to_string())?;
        Ok(held.map(|_| "holds".to_string()))
    }

    fn version_of(&mut self, kind: &str, key: &str, axis: i64, known: i64, event: &str, row: &Row) -> Result<(), String> {
        let body = serde_json::to_string(row).map_err(|e| e.to_string())?;
        self.versions.push((kind.to_string(), key.to_string(), axis, known, event.to_string(), body));
        Ok(())
    }

    /// Applies one `rannoch.amended`: finds what the amended event said from its hearing, patches
    /// it, and writes each fact it made again at its own axis point, known at the amendment. A
    /// patch to the subject ends the old key's fact and writes the new key's. Returns `None` while
    /// the trickle reader has not yet heard the amended event, so the amendment waits.
    ///
    /// # Errors
    /// The hearings or the log cannot be read.
    pub fn amend(&mut self, amendment: &Event) -> Result<Option<String>, String> {
        let done: Option<String> = self
            .conn
            .prepare_cached("SELECT outcome FROM amended WHERE event = ?1")
            .map_err(|e| e.to_string())?
            .query_row([&amendment.id], |r| r.get(0))
            .optional()
            .map_err(|e| e.to_string())?;
        if done.is_some() || self.amended.iter().any(|(e, _)| *e == amendment.id) {
            return Ok(Some("already applied".into()));
        }
        if self.hearings.is_none() {
            let path = self.dir.join("hearings.db");
            if !path.exists() {
                return Ok(None);
            }
            let conn = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e| e.to_string())?;
            conn.busy_timeout(std::time::Duration::from_secs(30)).map_err(|e| e.to_string())?;
            self.hearings = Some(conn);
        }
        let target = amendment.subject.clone().unwrap_or_default();
        let heard: Option<(i64, i64)> = self
            .hearings
            .as_ref()
            .ok_or("no hearings")?
            .prepare_cached("SELECT at, len FROM hearings WHERE id = ?1")
            .map_err(|e| e.to_string())?
            .query_row([&target], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .map_err(|e| e.to_string())?;
        let Some((at, len)) = heard else { return Ok(None) };
        let line = {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = std::fs::File::open(self.dir.join("log.jsonl")).map_err(|e| e.to_string())?;
            f.seek(SeekFrom::Start(u64::try_from(at).unwrap_or(0))).map_err(|e| e.to_string())?;
            let mut buf = vec![0u8; usize::try_from(len).unwrap_or(0)];
            f.read_exact(&mut buf).map_err(|e| e.to_string())?;
            String::from_utf8(buf).map_err(|e| e.to_string())?
        };
        let mut patched: Value = serde_json::from_str(line.trim_end()).map_err(|e| e.to_string())?;
        let original = crate::entrance::admit(line.trim_end(), 0)?;
        merge_patch(&mut patched, &amendment.data);
        let patched = crate::entrance::admit(&patched.to_string(), 0)?;
        let outcome = self.apply_amendment(&original, &patched, amendment)?;
        self.amended.push((amendment.id.clone(), outcome.clone()));
        Ok(Some(outcome))
    }

    fn apply_amendment(&mut self, original: &Event, patched: &Event, amendment: &Event) -> Result<String, String> {
        let Some(fact) = fact_of(original) else { return Ok("refused: the amended event says no fact".into()) };
        let kind = fact.kind;
        let made: Option<i64> = self
            .conn
            .prepare_cached("SELECT axis FROM versions WHERE kind = ?1 AND event = ?2 LIMIT 1")
            .map_err(|e| e.to_string())?
            .query_row(params![kind, original.id], |r| r.get(0))
            .optional()
            .map_err(|e| e.to_string())?;
        let in_memory = self.versions.iter().any(|v| v.0 == kind && v.4 == original.id);
        if made.is_none() && !in_memory {
            return Ok("refused: the amended event has no version of its own".into());
        }
        let axis = axis_of(kind, original);
        let known = known_of(amendment);
        let old_key = self.renamed(kind, &fact.key, known_of(original));
        let new_fact = fact_of(patched).ok_or("the patched event says no fact")?;
        let data = defined(kind, patched);
        if new_fact.key == fact.key {
            let current = self.row(kind, &old_key)?.clone();
            let mut row = current.clone();
            row.data = data.clone();
            row.axis = axis;
            self.version_of(kind, &old_key, axis, known, &amendment.id, &row)?;
            if current.axis == axis {
                self.row(kind, &old_key)?.data = data;
            }
            return Ok("patched".into());
        }
        let mut ended = self.row(kind, &old_key)?.clone();
        ended.ended = true;
        ended.axis = ended.axis.max(axis);
        let end_axis = ended.axis;
        self.version_of(kind, &old_key, end_axis, known, &amendment.id, &ended)?;
        *self.row(kind, &old_key)? = ended.clone();
        let mut fresh = ended;
        fresh.ended = false;
        fresh.data = data;
        fresh.event = amendment.id.clone();
        self.version_of(kind, &new_fact.key, end_axis, known, &amendment.id, &fresh)?;
        self.cache.insert((kind.to_string(), new_fact.key.clone()), fresh);
        self.renames.insert((kind.to_string(), old_key.clone()), (new_fact.key.clone(), known));
        self.new_renames.push((kind.to_string(), old_key, new_fact.key, known, amendment.id.clone()));
        Ok("renamed".into())
    }

    pub fn keep_answer(&mut self, at: (String, String), answer: Answer) {
        self.answers.push((at, answer));
    }

    /// The host's answer for `action` on `event`, if it gave one.
    #[must_use]
    pub fn answer(&self, action: &str, event: &str) -> Option<Answer> {
        if let Some((_, a)) = self.answers.iter().find(|(k, _)| k.0 == action && k.1 == event) {
            return Some(a.clone());
        }
        let text: Option<String> = self
            .conn
            .prepare_cached("SELECT answer FROM host WHERE action = ?1 AND event = ?2")
            .ok()?
            .query_row(params![action, event], |r| r.get(0))
            .optional()
            .ok()?;
        text.and_then(|t| serde_json::from_str(&t).ok())
    }

    /// Writes every changed row, its new edges and the host's new answers in one transaction,
    /// with `meta` if given.
    ///
    /// # Errors
    /// The file cannot be written.
    pub fn commit(&mut self, meta: Option<&str>) -> Result<(), String> {
        let err = |e: rusqlite::Error| e.to_string();
        let tx = self.conn.transaction().map_err(err)?;
        {
            let mut put = tx.prepare_cached("INSERT INTO versions(kind, key, axis, known, event, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6)").map_err(err)?;
            for v in &self.versions {
                put.execute(params![v.0, v.1, v.2, v.3, v.4, v.5]).map_err(err)?;
                self.writes += 1;
            }
            // An edge's relation names the kinds at both ends, so the edge holds only the keys.
            let mut edge = tx.prepare_cached("INSERT OR IGNORE INTO edges VALUES (?1, ?2, ?3, ?4)").map_err(err)?;
            for e in &self.edges {
                edge.execute(params![e.1, format!("{}.{}.{}", e.0, e.2, e.3), e.4, e.5]).map_err(err)?;
            }
            let mut link = tx.prepare_cached("INSERT INTO link_versions VALUES (?1, ?2, ?3, ?4, ?5, ?6)").map_err(err)?;
            for l in &self.links {
                link.execute(params![l.0, l.1, l.2, l.3, l.4, l.5]).map_err(err)?;
            }
            let mut rename = tx.prepare_cached("INSERT OR REPLACE INTO renames VALUES (?1, ?2, ?3, ?4, ?5)").map_err(err)?;
            for r in &self.new_renames {
                rename.execute(params![r.0, r.1, r.2, r.3, r.4]).map_err(err)?;
            }
            let mut amended = tx.prepare_cached("INSERT OR IGNORE INTO amended VALUES (?1, ?2)").map_err(err)?;
            for a in &self.amended {
                amended.execute(params![a.0, a.1]).map_err(err)?;
            }
            let mut host = tx.prepare_cached("INSERT OR IGNORE INTO host VALUES (?1, ?2, ?3)").map_err(err)?;
            for ((action, event), answer) in &self.answers {
                host.execute(params![action, event, serde_json::to_string(answer).map_err(|e| e.to_string())?]).map_err(err)?;
            }
            if let Some(meta) = meta {
                tx.execute("INSERT INTO meta(k, v) VALUES ('lane', ?1) ON CONFLICT(k) DO UPDATE SET v = excluded.v", [meta]).map_err(err)?;
            }
        }
        tx.commit().map_err(err)?;
        self.versions.clear();
        self.edges.clear();
        self.links.clear();
        self.new_renames.clear();
        self.amended.clear();
        self.answers.clear();
        Ok(())
    }

    /// The lane's state as last committed.
    #[must_use]
    pub fn meta(&self) -> Option<String> {
        self.conn.query_row("SELECT v FROM meta WHERE k = 'lane'", [], |r| r.get(0)).optional().ok().flatten()
    }

    /// Every version, in the order it was written, as text.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn versions_text(&self) -> Result<String, String> {
        let err = |e: rusqlite::Error| e.to_string();
        let mut st = self.conn.prepare("SELECT kind, key, axis, known, event, body FROM versions ORDER BY kind, key, axis, known, rowid").map_err(err)?;
        let rows = st
            .query_map([], |r| Ok(format!("{}|{}|{}|{}|{}|{}", r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?)))
            .map_err(err)?;
        let mut out = String::new();
        for r in rows {
            out.push_str(&r.map_err(err)?);
            out.push('\n');
        }
        Ok(out)
    }

    /// Every row as it is now, and every edge, by key, as text.
    ///
    /// # Errors
    /// The file cannot be read.
    pub fn text(&self) -> Result<String, String> {
        let err = |e: rusqlite::Error| e.to_string();
        let mut out = String::new();
        let mut st = self
            .conn
            .prepare("SELECT kind, key, body FROM versions v WHERE rowid = (SELECT rowid FROM versions w WHERE w.kind = v.kind AND w.key = v.key ORDER BY axis DESC, rowid DESC LIMIT 1) ORDER BY kind, key")
            .map_err(err)?;
        let rows = st.query_map([], |r| Ok(format!("{}|{}|{}", r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))).map_err(err)?;
        for r in rows {
            out.push_str(&r.map_err(err)?);
            out.push('\n');
        }
        let mut st = self.conn.prepare("SELECT src, rel, dst FROM edges ORDER BY 1, 2, 3").map_err(err)?;
        let rows = st.query_map([], |r| Ok(format!("{}|{}|{}", r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))).map_err(err)?;
        for r in rows {
            out.push_str(&r.map_err(err)?);
            out.push('\n');
        }
        Ok(out)
    }

    /// How many host answers it holds.
    #[must_use]
    pub fn answers_kept(&self, action: &str) -> i64 {
        self.conn.query_row("SELECT COUNT(*) FROM host WHERE action = ?1", [action], |r| r.get(0)).unwrap_or(0)
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

/// RFC 7386 JSON Merge Patch: a value replaces, `null` removes, an omitted field stays.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    if let Value::Object(p) = patch {
        if !target.is_object() {
            *target = Value::Object(serde_json::Map::new());
        }
        if let Value::Object(t) = target {
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else {
                    merge_patch(t.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
        }
    } else {
        *target = patch.clone();
    }
}
