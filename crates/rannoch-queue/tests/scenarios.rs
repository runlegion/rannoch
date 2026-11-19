//! FR-RANNOCH-019's scenarios, from `tests/features/lib.feature`: here "The queue runs what is
//! due" (`@criterion-FR-RANNOCH-019-c1` to `-c4` and `-c8`). The store is the app's database
//! opened through the real store, its rows from the app's forgr factory; the flows are fakes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, thread};

use chrono::{SecondsFormat, TimeDelta, Utc};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Event, Rannoch, Status};
use rannoch_events::Events;
use rannoch_queue::{Flow, Flows, Queue, Row};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use uuid::Uuid;

/// The app's `ty`: 1 a run, 2 a job, 3 a run for later, 9 no flow.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Ty(u16);
impl From<Ty> for u16 {
    fn from(t: Ty) -> u16 {
        t.0
    }
}
impl TryFrom<u16> for Ty {
    type Error = ();
    fn try_from(v: u16) -> std::result::Result<Self, ()> {
        Ok(Ty(v))
    }
}

/// When each processor ran, by root id.
type Ran = Arc<Mutex<HashMap<Uuid, (Instant, Instant)>>>;

struct Fake {
    flows: HashMap<u16, Arc<Flow<Ty, String>>>,
}
impl Flows<Ty, String> for Fake {
    fn flow(&self, ty: u16) -> Option<Arc<Flow<Ty, String>>> {
        self.flows.get(&ty).cloned()
    }
}

/// A processor that works for `take`, records when, and sends its own 2 with its log line.
fn processor(job: bool, take: Duration, ran: Ran) -> Arc<Flow<Ty, String>> {
    Arc::new(Flow {
        job,
        run: Box::new(move |event: &Row, _root: &Row, sender| {
            let start = Instant::now();
            thread::sleep(take);
            if let Ok(mut times) = ran.lock() {
                times.insert(event.id, (start, Instant::now()));
            }
            let _ = sender.send(Event {
                id: Uuid::nil(),
                source: Some(event.id),
                ty: Ty(event.ty),
                data: format!("ran {}", event.id),
                run_at: None,
                status: Status::Processed,
            });
        }),
    })
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn database(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir.join("store.db"))
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// An event at status 0 from the app's forgr factory, with `run_at` when given.
fn waiting(conn: &Connection, ty: i64, run_at: Option<chrono::DateTime<Utc>>) -> Result<Uuid> {
    let id = Uuid::now_v7();
    let row = _events::factory()
        .id(id.to_string())
        .ty(ty)
        .data("{}")
        .status(0);
    match run_at {
        Some(at) => row.run_at(at.to_rfc3339_opts(SecondsFormat::Millis, true)),
        None => row,
    }
    .insert(conn)?;
    Ok(id)
}

/// The statuses, with their data, of the rows whose source is `id`.
fn about(conn: &Connection, id: Uuid) -> rusqlite::Result<Vec<(i64, String)>> {
    conn.prepare("SELECT status, data FROM _events WHERE source = ?1 ORDER BY id")?
        .query_map([id.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

/// Waits up to `limit` for `done`.
fn until(limit: Duration, done: impl Fn() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < limit {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    done()
}

#[test]
fn the_queue_runs_what_is_due() {
    // Given a store with events from the app's forgr factory at status 0: two whose flow has no
    // job step, one whose flow has a job step, one whose run_at is a short wait away, and one
    // whose ty has no flow
    let path = database("the_queue_runs_what_is_due").unwrap();
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    Rannoch::<Ty, String>::open(Config {
        store: Some(store.clone()),
        ..Config::default()
    })
    .unwrap()
    .shutdown();
    let seed = ready(&path).unwrap();
    let run_a = waiting(&seed, 1, None).unwrap();
    let run_b = waiting(&seed, 1, None).unwrap();
    let job = waiting(&seed, 2, None).unwrap();
    let later = waiting(&seed, 3, Some(Utc::now() + TimeDelta::milliseconds(1_500))).unwrap();
    let unknown = waiting(&seed, 9, None).unwrap();

    let ran: Ran = Arc::default();
    let flows = Arc::new(Fake {
        flows: HashMap::from([
            (1, processor(false, Duration::from_millis(120), ran.clone())),
            (2, processor(true, Duration::from_millis(400), ran.clone())),
            (3, processor(false, Duration::from_millis(10), ran.clone())),
        ]),
    });
    let (signal, wake) = mpsc::channel();
    let rannoch = Rannoch::open(Config {
        store: Some(store.clone()),
        append: Some(Arc::new(Events::new(ready(&path).unwrap(), signal.clone()))
            as Arc<dyn Append<Ty, String>>),
        ..Config::default()
    })
    .unwrap();
    let queue = Queue::start(ready(&path).unwrap(), wake, rannoch.sender(), flows);

    // When the events crate signals the queue
    signal.send(()).unwrap();

    // Then the queue sends a status 1 for each due event, with that event as its source, and
    // none for the one not yet due
    let read = ready(&path).unwrap();
    let due = [run_a, run_b, job, unknown];
    assert!(until(Duration::from_secs(5), || due
        .iter()
        .all(|id| about(&read, *id).unwrap().iter().any(|r| r.0 == 1))));
    assert!(
        about(&read, later).unwrap().is_empty(),
        "{:?}",
        about(&read, later).unwrap()
    );

    // And the two runs run one after the other while the job runs beside them
    assert!(until(Duration::from_secs(5), || ran.lock().unwrap().len() == 3));
    let times = ran.lock().unwrap().clone();
    let (a, b, j) = (times[&run_a], times[&run_b], times[&job]);
    let (first, second) = if a.0 < b.0 { (a, b) } else { (b, a) };
    assert!(first.1 <= second.0, "the runs overlapped");
    assert!(
        j.0 < second.1 && j.1 > first.0,
        "the job did not run beside the runs"
    );

    // And each processor sends its own result, a 2 with its log line
    assert!(until(Duration::from_secs(5), || [run_a, run_b, job]
        .iter()
        .all(|id| {
            about(&read, *id)
                .unwrap()
                .contains(&(2, format!("\"ran {id}\"")))
        })));

    // And for the event with no flow the queue sends a -1 saying "no processor", and no retry
    assert_eq!(
        about(&read, unknown).unwrap(),
        [
            (1, "\"taken\"".to_owned()),
            (-1, "\"no processor\"".to_owned())
        ]
    );

    // When the signal comes again
    signal.send(()).unwrap();
    thread::sleep(Duration::from_millis(200));

    // Then no event with a status 1 is taken again
    for id in due {
        assert_eq!(
            about(&read, id)
                .unwrap()
                .iter()
                .filter(|r| r.0 == 1)
                .count(),
            1,
            "{id} taken twice"
        );
    }

    // When the event not yet due comes due with nothing else appended
    assert_eq!(about(&read, later).unwrap(), Vec::<(i64, String)>::new());

    // Then the queue takes it and runs it
    assert!(until(Duration::from_secs(5), || {
        about(&read, later)
            .unwrap()
            .contains(&(2, format!("\"ran {later}\"")))
    }));
    assert_eq!(
        about(&read, later)
            .unwrap()
            .iter()
            .filter(|r| r.0 == 1)
            .count(),
        1
    );

    drop(signal);
    rannoch.shutdown();
    queue.join();
}

/// Not a scenario: the queue's own memory of what it took. With an hour-long inbox interval each
/// 1 stays on the inbox's heap, so only that memory stops a second signal taking the event again.
#[test]
fn an_event_taken_before_its_1_is_kept_is_not_taken_again() {
    let path = database("an_event_taken_before_its_1_is_kept").unwrap();
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    Rannoch::<Ty, String>::open(Config {
        store: Some(store.clone()),
        ..Config::default()
    })
    .unwrap()
    .shutdown();
    let event = waiting(&ready(&path).unwrap(), 1, None).unwrap();
    let runs = Arc::new(Mutex::new(0_u32));
    let count = runs.clone();
    let flows = Arc::new(Fake {
        flows: HashMap::from([(
            1,
            Arc::new(Flow {
                job: false,
                run: Box::new(
                    move |_: &Row, _: &Row, _: &rannoch_core::Sender<Ty, String>| {
                        if let Ok(mut n) = count.lock() {
                            *n += 1;
                        }
                    },
                ),
            }),
        )]),
    });
    let (signal, wake) = mpsc::channel();
    let rannoch = Rannoch::open(Config {
        default_interval: TimeDelta::hours(1),
        store: Some(store.clone()),
        append: Some(Arc::new(Events::new(ready(&path).unwrap(), signal.clone()))
            as Arc<dyn Append<Ty, String>>),
        ..Config::default()
    })
    .unwrap();
    let queue = Queue::start(ready(&path).unwrap(), wake, rannoch.sender(), flows);

    signal.send(()).unwrap();
    thread::sleep(Duration::from_millis(200));
    signal.send(()).unwrap();
    thread::sleep(Duration::from_millis(200));

    assert_eq!(*runs.lock().unwrap(), 1, "event {event} ran more than once");
    drop(signal);
    rannoch.shutdown();
    queue.join();
}
