//! TOY ONLY: nodes per class and links per relation, kept by fill. Temporary triggers tally each
//! row a fill adds to or removes from a class table, a relation table or `_open` into an
//! in-memory table; each batch's commit folds the tally into `_stats`, one row per touched class
//! or relation. `RANNOCH_TOY_STATS=0` turns it off, for measuring its cost on the same build.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::OnceLock;

thread_local! {
    static TALLY: RefCell<HashMap<String, i64>> = RefCell::new(HashMap::new());
    static KNOWN: std::cell::Cell<i64> = const { std::cell::Cell::new(i64::MIN) };
}

fn cc() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("RANNOCH_TOY_CC").is_ok())
}

fn kept_known() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("RANNOCH_TOY_KNOWN").is_ok())
}

/// counter_cache: tallies a change in whether the link from `from` to `to` of `relation` holds,
/// for the relation and, with `RANNOCH_TOY_CC`, for each of its two ends.
pub(crate) fn tally_link(relation: &str, from: &str, to: &str, d: i64) {
    tally(format!("link:{relation}"), d);
    if cc() {
        tally(format!("o:{relation}:{from}"), d);
        tally(format!("i:{relation}:{to}"), d);
    }
}

/// Notes a fact's known-at, so each batch keeps the newest the store holds.
pub(crate) fn note_known(known: i64) {
    if kept_known() {
        KNOWN.with(|k| {
            if known > k.get() {
                k.set(known);
            }
        });
    }
}

/// The newest known-at the store holds, as fill keeps it (with `RANNOCH_TOY_KNOWN`).
pub(crate) fn known_now(conn: &Connection) -> Result<Option<i64>, Error> {
    if !kept_known() {
        return Ok(None);
    }
    Ok(conn
        .prepare_cached("SELECT n FROM _stats WHERE entity = 'known_now'")?
        .query_row([], |row| row.get(0))
        .ok())
}

/// counter_cache: how many links of `relation` from (`out`) or to the node keyed `key` hold now.
pub(crate) fn link_count_now(conn: &Connection, relation: &str, out: bool, key: &str) -> Result<Option<i64>, Error> {
    if !cc() {
        return Ok(None);
    }
    let entity = format!("{}:{relation}:{key}", if out { "o" } else { "i" });
    Ok(Some(
        conn.prepare_cached("SELECT n FROM _stats WHERE entity = ?1")?
            .query_row([entity], |row| row.get(0))
            .unwrap_or(0),
    ))
}

/// Tallies a row a fill added to (1) or removed from (-1) a class or relation table, in memory.
pub(crate) fn tally(entity: String, d: i64) {
    if on() {
        TALLY.with(|t| *t.borrow_mut().entry(entity).or_default() += d);
    }
}

use rusqlite::Connection;

use crate::Error;
use rannoch_ontology::Ontology;

fn on() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("RANNOCH_TOY_STATS").map_or(true, |v| v != "0"))
}

fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Creates the stats table, the in-memory tally and the triggers for every declared class and
/// relation, once per connection and declaration.
pub(crate) fn prepare(tx: &Connection, ontology: &Ontology) -> Result<(), Error> {
    if !on() {
        return Ok(());
    }
    let mut sql = String::from(
        "CREATE TABLE IF NOT EXISTS _stats(entity TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID;\
         CREATE TEMP TABLE IF NOT EXISTS stats_delta(entity TEXT NOT NULL, d INTEGER NOT NULL);",
    );
    for (i, relation) in ontology.relations.keys().enumerate() {
        let entity = quote(&format!("link:{relation}"));
        let rel = quote(relation);
        let (o, i_) = if cc() {
            (
                format!(", ('o:' || new.rel || ':' || new.from_key, 1), ('i:' || new.rel || ':' || new.to_key, 1)"),
                format!(", ('o:' || old.rel || ':' || old.from_key, -1), ('i:' || old.rel || ':' || old.to_key, -1)"),
            )
        } else {
            (String::new(), String::new())
        };
        sql.push_str(&format!(
            "CREATE TEMP TRIGGER IF NOT EXISTS stats_o{i}_i AFTER INSERT ON main._open \
             WHEN new.rel = {rel} BEGIN INSERT INTO stats_delta VALUES ({entity}, 1){o}; END;\
             CREATE TEMP TRIGGER IF NOT EXISTS stats_o{i}_d AFTER DELETE ON main._open \
             WHEN old.rel = {rel} BEGIN INSERT INTO stats_delta VALUES ({entity}, -1){i_}; END;"
        ));
    }
    tx.execute_batch(&sql)?;
    Ok(())
}

/// Folds the batch's tally into `_stats`: one write per touched class or relation.
pub(crate) fn flush(tx: &Connection) -> Result<(), Error> {
    if !on() {
        return Ok(());
    }
    let tally: Vec<(String, i64)> = TALLY.with(|t| t.borrow_mut().drain().collect());
    for (entity, d) in tally {
        tx.prepare_cached("INSERT INTO stats_delta VALUES (?1, ?2)")?
            .execute(rusqlite::params![entity, d])?;
    }
    tx.execute_batch(
        "INSERT INTO _stats(entity, n) SELECT entity, sum(d) FROM stats_delta WHERE true \
         GROUP BY entity ON CONFLICT(entity) DO UPDATE SET n = n + excluded.n;\
         DELETE FROM stats_delta;",
    )?;
    let known = KNOWN.with(|k| k.replace(i64::MIN));
    if known > i64::MIN {
        tx.prepare_cached(
            "INSERT INTO _stats(entity, n) VALUES ('known_now', ?1) \
             ON CONFLICT(entity) DO UPDATE SET n = max(n, excluded.n)",
        )?
        .execute([known])?;
    }
    Ok(())
}

/// The stats as kept: each class's node count and each relation's link count, now.
///
/// # Errors
/// The graph file cannot be read.
pub fn read(conn: &Connection) -> Result<Vec<(String, i64)>, Error> {
    let mut statement = conn.prepare_cached("SELECT entity, n FROM _stats ORDER BY entity")?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// Whether the link holds now: heard, and not read as ended.
pub(crate) fn holding(tx: &Connection, relation: &str, from: &str, to: &str) -> Result<bool, Error> {
    Ok(crate::history::link_in_view(tx, relation, (from, to), (None, None))?
        .is_some_and(|v| !v.ended))
}

/// Whether the link has ever had an ending: only such a link can change whether it holds when
/// heard again.
pub(crate) fn ever_ended(tx: &Connection, relation: &str, from: &str, to: &str) -> Result<bool, Error> {
    static SKIP: OnceLock<bool> = OnceLock::new();
    if *SKIP.get_or_init(|| std::env::var("RANNOCH_TOY_STATS_NOENDED").is_ok()) {
        return Ok(false);
    }
    let entity = crate::history::link_entity(relation);
    let found: Option<i64> = tx
        .prepare_cached("SELECT 1 FROM _endings WHERE entity = ?1 AND key = ?2 AND to_key = ?3 LIMIT 1")?
        .query_row(rusqlite::params![entity, from, to], |row| row.get(0))
        .ok();
    Ok(found.is_some())
}

/// Tallies a change in whether a link of `relation` holds.
pub(crate) fn moved(tx: &Connection, (relation, from, to): (&str, &str, &str), before: bool, after: bool) -> Result<(), Error> {
    if !on() || before == after {
        return Ok(());
    }
    let _ = tx;
    tally_link(relation, from, to, if after { 1 } else { -1 });
    Ok(())
}

pub(crate) fn enabled() -> bool {
    on()
}

/// Whether the relation's table already holds the link from `from_key` to `to_key`.
pub(crate) fn linked(tx: &Connection, relation: &str, from: &str, to: &str, from_key: &str, to_key: &str) -> Result<bool, Error> {
    let sql = format!(
        "SELECT 1 FROM {} WHERE src = (SELECT id FROM {} WHERE key = ?1) AND dst = (SELECT id FROM {} WHERE key = ?2)",
        crate::queries::relation_table(relation),
        crate::queries::class_table(from),
        crate::queries::class_table(to)
    );
    let found: Option<i64> = tx.prepare_cached(&sql)?.query_row(rusqlite::params![from_key, to_key], |r| r.get(0)).ok();
    Ok(found.is_some())
}

/// The class table write split in two: an insert that does nothing on a conflict, whose change
/// count says whether the row is new, and the update to run when it was not.
pub(crate) fn node_split(class: &str, names: &[&str]) -> (String, String) {
    thread_local! {
        static SQL: RefCell<HashMap<String, (String, String)>> = RefCell::new(HashMap::new());
    }
    let memo = format!("{class}\u{0}{}", names.join("\u{0}"));
    if let Some(hit) = SQL.with(|m| m.borrow().get(&memo).cloned()) {
        return hit;
    }
    let built = node_split_build(class, names);
    SQL.with(|m| m.borrow_mut().insert(memo, built.clone()));
    built
}

fn node_split_build(class: &str, names: &[&str]) -> (String, String) {
    let upsert = crate::queries::upsert_node(class, names);
    let insert = format!("{} ON CONFLICT(key) DO NOTHING", upsert.split(" ON CONFLICT").next().unwrap_or_default());
    let sets: String = names
        .iter()
        .enumerate()
        .map(|(i, p)| format!(", {} = ?{}", crate::queries::property_column(p), i + 5))
        .collect();
    let update = format!(
        "UPDATE {} SET source = ?2, valid = ?3, recorded = ?4{sets} WHERE key = ?1",
        crate::queries::class_table(class)
    );
    (insert, update)
}

/// Puts a link into its relation's table, tallying it when the row is new.
pub(crate) fn put_link(tx: &Connection, relation: &str, from: &str, to: &str, keys: (&str, &str), params: impl rusqlite::Params + Clone) -> Result<(), Error> {
    thread_local! {
        static SQL: RefCell<HashMap<String, (String, String, String)>> = RefCell::new(HashMap::new());
    }
    let memo = format!("{relation}\u{0}{from}\u{0}{to}");
    let (upsert, insert, update) = SQL.with(|m| {
        m.borrow_mut()
            .entry(memo)
            .or_insert_with(|| {
                let upsert = crate::queries::upsert_link(relation, from, to);
                let insert = format!("{} ON CONFLICT(src, dst) DO NOTHING", upsert.split(" ON CONFLICT").next().unwrap_or_default());
                let update = format!(
                    "UPDATE {} SET source = ?3, valid = ?4, recorded = ?5 WHERE src = (SELECT id FROM {} WHERE key = ?1) AND dst = (SELECT id FROM {} WHERE key = ?2)",
                    crate::queries::relation_table(relation),
                    crate::queries::class_table(from),
                    crate::queries::class_table(to)
                );
                (upsert, insert, update)
            })
            .clone()
    });
    if !on() {
        tx.prepare_cached(&upsert)?.execute(params)?;
        return Ok(());
    }
    if tx.prepare_cached(&insert)?.execute(params.clone())? == 1 {
        tally_link(relation, keys.0, keys.1, 1);
    } else {
        tx.prepare_cached(&update)?.execute(params)?;
    }
    Ok(())
}
