//! rdf toy: rannoch's knowledge as RDF in its own SQLite file, one named graph per event,
//! described with PROV-O, read with SPARQL through oxilite and through spareval over a table
//! of my own. Predictions in predictions.txt were written before this ran.

use chrono::{DateTime, SecondsFormat, Utc};
use oxilite::StoreOptions;
use oxilite::core::schema::schema_sql;
use oxilite::store::Store;
use oxrdf::vocab::xsd;
use oxrdf::{GraphName, Literal, NamedNode, Quad, Term};
use rusqlite::Connection;
use spareval::{InternalQuad, QueryEvaluator, QueryResults, QueryableDataset};
use std::collections::BTreeSet;
use std::str::FromStr;
use std::time::{Duration, Instant};
use uuid::{NoContext, Timestamp, Uuid};

const PROV: &str = "http://www.w3.org/ns/prov#";
const EX: &str = "http://example.com/";
const T0: u64 = 1_790_000_000; // 2026-09-21, seconds

fn ex(local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{EX}{local}"))
}
fn prov(local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{PROV}{local}"))
}
fn graph(id: Uuid) -> NamedNode {
    NamedNode::new_unchecked(format!("urn:uuid:{id}"))
}
fn v7_at(ms: u64) -> Uuid {
    Uuid::new_v7(Timestamp::from_unix(NoContext, ms / 1000, ((ms % 1000) * 1_000_000) as u32))
}
fn v7_time(id: Uuid) -> DateTime<Utc> {
    let (s, n) = id.get_timestamp().expect("v7").to_unix();
    DateTime::from_timestamp(s as i64, n).expect("time")
}
fn xsd_time(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

struct Event {
    id: Uuid,
    source: Option<Uuid>,
    revises: Option<Uuid>,
    facts: Vec<(NamedNode, NamedNode, Term)>,
}

/// An event's facts in its own graph, and the graph described in the default graph.
fn quads(e: &Event) -> Vec<Quad> {
    let g = graph(e.id);
    let mut out: Vec<Quad> = e
        .facts
        .iter()
        .map(|(s, p, o)| Quad::new(s.clone(), p.clone(), o.clone(), g.clone()))
        .collect();
    let time = Literal::new_typed_literal(xsd_time(v7_time(e.id)), xsd::DATE_TIME);
    out.push(Quad::new(g.clone(), prov("generatedAtTime"), time, GraphName::DefaultGraph));
    if let Some(src) = e.source {
        out.push(Quad::new(g.clone(), prov("wasDerivedFrom"), graph(src), GraphName::DefaultGraph));
    }
    if let Some(old) = e.revises {
        out.push(Quad::new(g.clone(), prov("wasRevisionOf"), graph(old), GraphName::DefaultGraph));
    }
    out
}

fn q_now(s: &str, p: &str) -> String {
    format!(
        "PREFIX prov: <{PROV}>
SELECT ?o ?g WHERE {{
  GRAPH ?g {{ <{s}> <{p}> ?o }}
  ?g prov:generatedAtTime ?t .
  FILTER NOT EXISTS {{ GRAPH ?g2 {{ <{s}> <{p}> ?o2 }} ?g2 prov:generatedAtTime ?t2 . FILTER(?t2 > ?t) }}
}}"
    )
}
fn q_as_of(s: &str, p: &str, at: &str) -> String {
    format!(
        "PREFIX prov: <{PROV}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?o ?g WHERE {{
  GRAPH ?g {{ <{s}> <{p}> ?o }}
  ?g prov:generatedAtTime ?t .
  FILTER(?t <= \"{at}\"^^xsd:dateTime)
  FILTER NOT EXISTS {{ GRAPH ?g2 {{ <{s}> <{p}> ?o2 }} ?g2 prov:generatedAtTime ?t2 .
    FILTER(?t2 > ?t && ?t2 <= \"{at}\"^^xsd:dateTime) }}
}}"
    )
}
fn q_why(g: &str) -> String {
    format!(
        "PREFIX prov: <{PROV}>
SELECT ?t ?src ?rev WHERE {{
  <{g}> prov:generatedAtTime ?t .
  OPTIONAL {{ <{g}> prov:wasDerivedFrom ?src }}
  OPTIONAL {{ <{g}> prov:wasRevisionOf ?rev }}
}}"
    )
}
fn q_count_graph(g: &str) -> String {
    format!("SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH <{g}> {{ ?s ?p ?o }} }}")
}

/// Rows as sorted "var=term" strings, so two engines' answers compare exactly.
fn rows(r: QueryResults<'_>) -> BTreeSet<String> {
    let QueryResults::Solutions(sols) = r else { panic!("not a select") };
    sols.map(|s| {
        let s = s.expect("row");
        let mut cells: Vec<String> = s.iter().map(|(v, t)| format!("{}={t}", v.as_str())).collect();
        cells.sort();
        cells.join(" ")
    })
    .collect()
}

fn ox_rows(store: &Store, q: &str) -> BTreeSet<String> {
    rows(store.query(q).expect("oxilite query"))
}

// ---- path B: spareval over a quads table of my own --------------------------------------

struct Mine {
    conn: Connection,
}

impl Mine {
    fn open(conn: Connection) -> Self {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS my_quads (s TEXT NOT NULL, p TEXT NOT NULL, o TEXT NOT NULL,
               g TEXT NOT NULL, PRIMARY KEY (s, p, o, g)) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS my_posg ON my_quads(p, o, s, g);
             CREATE INDEX IF NOT EXISTS my_ospg ON my_quads(o, s, p, g);
             CREATE INDEX IF NOT EXISTS my_gspo ON my_quads(g, s, p, o);",
        )
        .expect("my schema");
        Self { conn }
    }
    fn insert(&mut self, qs: impl IntoIterator<Item = Quad>) {
        let tx = self.conn.transaction().expect("tx");
        {
            let mut st = tx
                .prepare_cached("INSERT OR IGNORE INTO my_quads VALUES (?1, ?2, ?3, ?4)")
                .expect("prep");
            for q in qs {
                let g = match &q.graph_name {
                    GraphName::DefaultGraph => String::new(),
                    other => other.to_string(),
                };
                st.execute((q.subject.to_string(), q.predicate.to_string(), q.object.to_string(), g))
                    .expect("insert");
            }
        }
        tx.commit().expect("commit");
    }
    fn query(&self, q: &str) -> BTreeSet<String> {
        let query = spargebra::SparqlParser::new().parse_query(q).expect("parse");
        rows(QueryEvaluator::new().execute(self, &query).expect("spareval"))
    }
}

fn io(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

impl<'a> QueryableDataset<'a> for &'a Mine {
    type InternalTerm = String;
    type Error = std::io::Error;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&String>,
        predicate: Option<&String>,
        object: Option<&String>,
        graph_name: Option<Option<&String>>,
    ) -> impl Iterator<Item = Result<InternalQuad<String>, std::io::Error>> + use<'a> {
        let mut sql = String::from("SELECT s, p, o, g FROM my_quads WHERE 1");
        let mut args: Vec<String> = Vec::new();
        for (col, v) in [("s", subject), ("p", predicate), ("o", object)] {
            if let Some(v) = v {
                args.push(v.clone());
                sql.push_str(&format!(" AND {col} = ?{}", args.len()));
            }
        }
        match graph_name {
            None => sql.push_str(" AND g <> ''"),
            Some(None) => sql.push_str(" AND g = ''"),
            Some(Some(g)) => {
                args.push(g.clone());
                sql.push_str(&format!(" AND g = ?{}", args.len()));
            }
        }
        let out: Vec<Result<InternalQuad<String>, std::io::Error>> = (|| {
            let mut st = self.conn.prepare_cached(&sql).map_err(io)?;
            let it = st
                .query_map(rusqlite::params_from_iter(args.iter()), |r| {
                    let g: String = r.get(3)?;
                    Ok(InternalQuad {
                        subject: r.get(0)?,
                        predicate: r.get(1)?,
                        object: r.get(2)?,
                        graph_name: (!g.is_empty()).then_some(g),
                    })
                })
                .map_err(io)?;
            it.map(|x| x.map_err(io)).collect::<Result<Vec<_>, _>>()
        })()
        .map_or_else(|e| vec![Err(e)], |v| v.into_iter().map(Ok).collect());
        out.into_iter()
    }

    fn internalize_term(&self, term: Term) -> Result<String, std::io::Error> {
        Ok(term.to_string())
    }

    fn externalize_term(&self, term: String) -> Result<Term, std::io::Error> {
        Term::from_str(&term).map_err(io)
    }
}

// ---- the runs ----------------------------------------------------------------------------

fn p50(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "data".into());
    std::fs::create_dir_all(&dir).expect("dir");
    let path = format!("{dir}/toy.sqlite");
    let _ = std::fs::remove_file(&path);

    // P1: an app file with _events, then oxilite's schema as one migration, then oxilite opens it.
    {
        let c = Connection::open(&path).expect("open");
        c.execute_batch(
            "CREATE TABLE _events (id TEXT PRIMARY KEY, ty INTEGER NOT NULL, data TEXT NOT NULL,
               status INTEGER NOT NULL);
             INSERT INTO _events VALUES ('e0', 1, '{}', 2);",
        )
        .expect("_events");
        c.execute_batch(&schema_sql(&StoreOptions::default())).expect("migration");
    }
    let store = Store::open(&path).expect("oxilite open");
    let tables: Vec<String> = {
        let c = Connection::open(&path).expect("open");
        let mut st = c
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .expect("prep");
        st.query_map([], |r| r.get(0)).expect("q").map(Result::unwrap).collect()
    };
    let events_rows: i64 = Connection::open(&path)
        .expect("open")
        .query_row("SELECT count(*) FROM _events", [], |r| r.get(0))
        .expect("count");
    println!("P1 tables after migration + open: {}", tables.join(", "));
    println!("P1 _events rows still: {events_rows} (expected 1)");

    // P2 and P4: two events about one design; the second corrects the first.
    let s = ex("design/cache-layout");
    let value = ex("value");
    let src = v7_at(T0 * 1000);
    let e1 = Event {
        id: v7_at(T0 * 1000 + 1_000),
        source: Some(src),
        revises: None,
        facts: vec![
            (s.clone(), value.clone(), Literal::new_simple_literal("a cache per repo").into()),
            (s.clone(), ex("owner"), ex("team/build").into()),
            (s.clone(), ex("status"), Literal::new_simple_literal("draft").into()),
        ],
    };
    let e2 = Event {
        id: v7_at(T0 * 1000 + 10_000),
        source: Some(e1.id),
        revises: Some(e1.id),
        facts: vec![(s.clone(), value.clone(), Literal::new_simple_literal("one shared cache").into())],
    };
    let mut mine = Mine::open(Connection::open(format!("{dir}/mine.sqlite")).expect("mine"));
    mine.conn.execute_batch("DELETE FROM my_quads").expect("clear");
    for e in [&e1, &e2] {
        store.extend(quads(e)).expect("extend");
        mine.insert(quads(e));
    }
    let g1 = graph(e1.id).into_string();
    let g2 = graph(e2.id).into_string();
    println!("P2 e1 graph quads: {:?}", ox_rows(&store, &q_count_graph(&g1)));
    println!("P2 e1 generatedAtTime: {:?} (UUIDv7 time {})", ox_rows(&store, &q_why(&g1)), xsd_time(v7_time(e1.id)));

    // P3: replay.
    let before = store.len().expect("len");
    store.extend(quads(&e1)).expect("replay");
    println!("P3 replay of e1 (IRIs and literals): {before} -> {} quads", store.len().expect("len"));
    let g3 = graph(v7_at(T0 * 1000 + 20_000)).into_string();
    let trig_bnode = format!("@prefix ex: <{EX}> . <{g3}> {{ ex:x ex:has [ ex:name \"n\" ] . }}");
    let trig_skolem = format!("@prefix ex: <{EX}> . <{g3}> {{ ex:y ex:has <{g3}#b0> . <{g3}#b0> ex:name \"n\" . }}");
    for (name, doc) in [("blank node", &trig_bnode), ("blank node named from the event id", &trig_skolem)] {
        store.load_from_slice(oxrdfio_trig(), doc.as_bytes()).expect("load");
        let a = store.len().expect("len");
        store.load_from_slice(oxrdfio_trig(), doc.as_bytes()).expect("reload");
        println!("P3 replay with {name}: {a} -> {} quads", store.len().expect("len"));
    }

    // P4: now, as of a time between e1 and e2, why; e1's graph untouched.
    let between = xsd_time(v7_time(e1.id) + chrono::Duration::seconds(5));
    let (sv, vv) = (s.as_str(), value.as_str());
    let checks = [
        ("now", q_now(sv, vv)),
        ("as of between e1 and e2", q_as_of(sv, vv, &between)),
        ("why e2", q_why(&g2)),
        ("e1 graph quads after e2", q_count_graph(&g1)),
    ];
    for (name, q) in &checks {
        let a = ox_rows(&store, q);
        let b = mine.query(q);
        println!("P4 {name}: {a:?}");
        println!("P6 {name}: spareval over my table gives the same rows: {}", a == b);
    }

    // P5: what the queries compile to.
    for (name, q) in [("now", q_now(sv, vv)), ("as of", q_as_of(sv, vv, &between))] {
        let plan = store.explain(q.as_str()).expect("explain");
        let fallback = plan.to_lowercase().contains("fallback") || plan.to_lowercase().contains("spareval");
        println!("P5 {name}: mentions fallback/spareval: {fallback}; plan starts: {}", plan.lines().take(3).collect::<Vec<_>>().join(" | "));
    }

    // P7, P8: 100,000 events of 5 facts, 20,000 subjects, so each (s, p) has 5 versions.
    let n_events = std::env::var("EVENTS").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000u64);
    let subjects = n_events / 5;
    let big = format!("{dir}/big.sqlite");
    let _ = std::fs::remove_file(&big);
    let bstore = Store::open(&big).expect("big");
    let mut bmine = Mine::open(Connection::open(format!("{dir}/big-mine.sqlite")).expect("bm"));
    bmine.conn.execute_batch("DELETE FROM my_quads").expect("clear");
    let mut prev: Option<Uuid> = None;
    let events: Vec<Event> = (0..n_events)
        .map(|i| {
            let id = v7_at(T0 * 1000 + 100_000 + i * 10);
            let subj = ex(&format!("thing/{}", i % subjects));
            let facts = (0..5)
                .map(|k| (subj.clone(), ex(&format!("p{k}")), Literal::new_simple_literal(format!("v{i}-{k}")).into()))
                .collect();
            let e = Event { id, source: prev, revises: None, facts };
            prev = Some(id);
            e
        })
        .collect();
    let all: Vec<Quad> = events.iter().flat_map(quads).collect();
    let nq = all.len();
    let t = Instant::now();
    let mut loader = bstore.bulk_loader();
    loader.load_quads(all.iter().cloned()).expect("bulk");
    loader.commit().expect("commit");
    bstore.optimize().expect("optimize");
    let ox_load = t.elapsed();
    let t = Instant::now();
    bmine.insert(all);
    let mine_load = t.elapsed();
    println!("P7 oxilite load: {nq} quads in {ox_load:?} = {:.0} quads/s", nq as f64 / ox_load.as_secs_f64());
    println!("P8 my table load: {nq} quads in {mine_load:?} = {:.0} quads/s", nq as f64 / mine_load.as_secs_f64());

    let mid = xsd_time(v7_time(events[(n_events / 2) as usize].id));
    let probe: Vec<String> = (0..200).map(|k| ex(&format!("thing/{}", (k * 97) % subjects)).into_string()).collect();
    let p0 = ex("p0").into_string();
    let mut agree = true;
    let mut times = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for subj in &probe {
        let qn = q_now(subj, &p0);
        let qa = q_as_of(subj, &p0, &mid);
        let t = Instant::now();
        let a = ox_rows(&bstore, &qn);
        times[0].push(t.elapsed());
        let t = Instant::now();
        let b = ox_rows(&bstore, &qa);
        times[1].push(t.elapsed());
        let t = Instant::now();
        let c = bmine.query(&qn);
        times[2].push(t.elapsed());
        let t = Instant::now();
        let d = bmine.query(&qa);
        times[3].push(t.elapsed());
        agree &= a == c && b == d && a.len() == 1;
    }
    let [a, b, c, d] = times;
    println!("P7 oxilite now p50 {:?}, as of p50 {:?}", p50(a), p50(b));
    println!("P8 spareval over my table now p50 {:?}, as of p50 {:?}", p50(c), p50(d));
    println!("P6 at scale: both engines agree on all 200 subjects, one current value each: {agree}");
    let sizes: Vec<String> = [&big, &format!("{dir}/big-mine.sqlite")]
        .iter()
        .map(|f| format!("{f}: {} MB", std::fs::metadata(f).map(|m| m.len() / 1_000_000).unwrap_or(0)))
        .collect();
    println!("sizes: {}", sizes.join(", "));

    // P10 (added after the first run, not predicted): one event per write, the way a flow step
    // writes, on a fresh file.
    let one = format!("{dir}/one.sqlite");
    let _ = std::fs::remove_file(&one);
    let ostore = Store::open(&one).expect("one");
    let n = 10_000usize;
    let t = Instant::now();
    for e in events.iter().take(n) {
        ostore.extend(quads(e)).expect("extend one event");
    }
    let el = t.elapsed();
    println!(
        "P10 oxilite one event per extend: {n} events ({} quads) in {el:?} = {:.0} events/s",
        ostore.len().expect("len"),
        n as f64 / el.as_secs_f64()
    );
}

fn oxrdfio_trig() -> oxilite::io::RdfFormat {
    oxilite::io::RdfFormat::TriG
}
