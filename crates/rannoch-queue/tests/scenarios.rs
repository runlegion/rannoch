//! FR-RANNOCH-019's scenarios, from `tests/features/lib.feature`: "The queue runs what is due"
//! (`@criterion-FR-RANNOCH-019-c1` to `-c4` and `-c8`) and "A failing flow is retried, then
//! binned" (`-c5` and `-c9`), "The hunt reruns a dropped run" (`-c7`), and "The app reads the queue's report" (`-c6`). The store is the app's database
//! opened through the real store, its rows from the app's forgr factory; the flows are fakes.
//! Every test runs with the clock frozen.
//!
//! And `tests/features/clock.feature`, one scenario for each of #163's criteria: rannoch-queue
//! reads and waits on time only through rannoch-core's clock and tokio, and every test runs with
//! the clock frozen.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use chrono::{SecondsFormat, TimeDelta, Utc};
use rannoch_core::clock::{self, new_id};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Event, Rannoch, Status};
use rannoch_events::Events;
use rannoch_queue::{Flow, Flows, Queue, Row, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use tokio::sync::mpsc;
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

/// What the processors did: when each started and finished, by event id, counted in the order
/// of every start and finish, and whether a job has started.
#[derive(Debug, Default)]
struct Done {
    count: u64,
    times: HashMap<Uuid, (u64, u64)>,
    job_started: bool,
}

/// The processors' record, and the condition each change to it is told on.
type Ran = Arc<(Mutex<Done>, Condvar)>;

fn done(ran: &Ran) -> std::sync::MutexGuard<'_, Done> {
    ran.0.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Fake {
    flows: HashMap<u16, Arc<Flow<Ty, String>>>,
}
impl Flows<Ty, String> for Fake {
    fn flow(&self, ty: u16) -> Option<Arc<Flow<Ty, String>>> {
        self.flows.get(&ty).cloned()
    }
}

/// What a processor waits for before it finishes.
#[derive(Debug, Clone, Copy)]
enum Wait {
    /// Nothing.
    Nothing,
    /// A job to have started.
    Job,
    /// Two other processors to have finished.
    Two,
}

/// A processor that records when it started and finished, waiting between the two for `wait`,
/// and sends its own 2 with its log line. A run waiting on a job and a job waiting on two runs
/// all finish only when the job runs beside the runs.
fn processor(job: bool, wait: Wait, ran: Ran) -> Arc<Flow<Ty, String>> {
    Arc::new(Flow {
        job,
        run: Box::new(move |event: &Row, _root: &Row, sender| {
            let mut state = done(&ran);
            let start = state.count;
            state.count += 1;
            state.job_started |= job;
            ran.1.notify_all();
            let mut state = ran
                .1
                .wait_while(state, |state| match wait {
                    Wait::Nothing => false,
                    Wait::Job => !state.job_started,
                    Wait::Two => state.times.len() < 2,
                })
                .unwrap_or_else(PoisonError::into_inner);
            let end = state.count;
            state.count += 1;
            state.times.insert(event.id, (start, end));
            ran.1.notify_all();
            drop(state);
            let _ = sender.send(Event {
                id: Uuid::nil(),
                source: Some(event.id),
                ty: Ty(event.ty),
                data: format!("ran {}", event.id),
                run_at: None,
                status: Status::Processed,
            });
            Ok(())
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

/// The app's store over `path`, with every migration applied by opening rannoch once.
async fn migrated(path: &Path) -> Result<Arc<Store>> {
    let store = Arc::new(Store::new(path.to_owned(), rannoch_core::migrator()));
    let rannoch = Rannoch::<Ty, String>::open(Config {
        store: Some(store.clone()),
        ..Config::default()
    })?;
    tokio::task::spawn_blocking(move || rannoch.shutdown()).await?;
    Ok(store)
}

/// Shuts `rannoch` down, then joins `queue`: both block, so they run through `spawn_blocking`.
async fn stop(rannoch: Rannoch<Ty, String>, queue: Queue) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        rannoch.shutdown();
        queue.join()
    })
    .await??;
    Ok(())
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// An event at status 0 from the app's forgr factory, with `run_at` when given.
fn waiting(conn: &Connection, ty: i64, run_at: Option<chrono::DateTime<Utc>>) -> Result<Uuid> {
    let id = new_id();
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

/// Waits up to `limit` on the clock for `done`.
async fn until(limit: Duration, done: impl Fn() -> bool) -> bool {
    let start = clock::elapsed();
    while start.elapsed() < limit {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    done()
}

#[tokio::test(start_paused = true)]
async fn the_queue_runs_what_is_due() {
    // Given a store with events from the app's forgr factory at status 0: two whose flow has no
    // job step, one whose flow has a job step, one whose run_at is a short wait away, and one
    // whose ty has no flow
    let path = database("the_queue_runs_what_is_due").unwrap();
    let store = migrated(&path).await.unwrap();
    let seed = ready(&path).unwrap();
    let run_a = waiting(&seed, 1, None).unwrap();
    let run_b = waiting(&seed, 1, None).unwrap();
    let job = waiting(&seed, 2, None).unwrap();
    let soon = clock::now() + TimeDelta::milliseconds(1_500);
    let later = waiting(&seed, 3, Some(soon)).unwrap();
    let unknown = waiting(&seed, 9, None).unwrap();

    let ran: Ran = Arc::default();
    let flows = Arc::new(Fake {
        flows: HashMap::from([
            (1, processor(false, Wait::Job, ran.clone())),
            (2, processor(true, Wait::Two, ran.clone())),
            (3, processor(false, Wait::Nothing, ran.clone())),
        ]),
    });
    let (signal, wake) = mpsc::unbounded_channel();
    let rannoch = Rannoch::open(Config {
        store: Some(store.clone()),
        append: Some(Arc::new(Events::new(ready(&path).unwrap(), signal.clone()))
            as Arc<dyn Append<Ty, String>>),
        ..Config::default()
    })
    .unwrap();
    let queue = Queue::start(
        ready(&path).unwrap(),
        wake,
        rannoch.sender(),
        flows,
        Settings::new(Duration::from_millis(100)),
    );

    // When the events crate signals the queue
    signal.send(()).unwrap();

    // Then the queue sends a status 1 for each due event, with that event as its source, and
    // none for the one not yet due
    let read = ready(&path).unwrap();
    let due = [run_a, run_b, job, unknown];
    let taken = |id: &Uuid| about(&read, *id).unwrap().iter().any(|r| r.0 == 1);
    assert!(until(Duration::from_secs(5), || due.iter().all(taken)).await);
    assert!(
        about(&read, later).unwrap().is_empty(),
        "{:?}",
        about(&read, later).unwrap()
    );

    // And the two runs run one after the other while the job runs beside them
    assert!(until(Duration::from_secs(5), || done(&ran).times.len() == 3).await);
    let times = done(&ran).times.clone();
    let (a, b, j) = (times[&run_a], times[&run_b], times[&job]);
    let (first, second) = if a.0 < b.0 { (a, b) } else { (b, a) };
    assert!(first.1 <= second.0, "the runs overlapped");
    assert!(
        j.0 < second.1 && j.1 > first.0,
        "the job did not run beside the runs"
    );

    // And each processor sends its own result, a 2 with its log line
    let sent_2 = |id: &Uuid| {
        about(&read, *id)
            .unwrap()
            .contains(&(2, format!("\"ran {id}\"")))
    };
    assert!(
        until(Duration::from_secs(5), || [run_a, run_b, job]
            .iter()
            .all(sent_2))
        .await
    );

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
    tokio::time::sleep(Duration::from_millis(200)).await;

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
    assert!(until(Duration::from_secs(5), || sent_2(&later)).await);
    assert_eq!(
        about(&read, later)
            .unwrap()
            .iter()
            .filter(|r| r.0 == 1)
            .count(),
        1
    );

    drop(signal);
    stop(rannoch, queue).await.unwrap();
}

/// Not a scenario: the queue's own memory of what it took. With an hour-long inbox interval each
/// 1 stays on the inbox's heap, so only that memory stops a second signal taking the event again.
#[tokio::test(start_paused = true)]
async fn an_event_taken_before_its_1_is_kept_is_not_taken_again() {
    let path = database("an_event_taken_before_its_1_is_kept").unwrap();
    let store = migrated(&path).await.unwrap();
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
                        Ok(())
                    },
                ),
            }),
        )]),
    });
    let (signal, wake) = mpsc::unbounded_channel();
    let rannoch = Rannoch::open(Config {
        default_interval: TimeDelta::hours(1),
        store: Some(store.clone()),
        append: Some(Arc::new(Events::new(ready(&path).unwrap(), signal.clone()))
            as Arc<dyn Append<Ty, String>>),
        ..Config::default()
    })
    .unwrap();
    let queue = Queue::start(
        ready(&path).unwrap(),
        wake,
        rannoch.sender(),
        flows,
        Settings::new(Duration::from_millis(100)),
    );

    signal.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    signal.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(*runs.lock().unwrap(), 1, "event {event} ran more than once");
    drop(signal);
    stop(rannoch, queue).await.unwrap();
}

/// One row about an event: id, status, data and `run_at`.
type About = (Uuid, i64, String, Option<String>);

/// The rows whose source is `id`.
fn rows_about(conn: &Connection, id: Uuid) -> rusqlite::Result<Vec<About>> {
    conn.prepare("SELECT id, status, data, run_at FROM _events WHERE source = ?1 ORDER BY id")?
        .query_map([id.to_string()], |r| {
            let id: String = r.get(0)?;
            Ok((
                id.parse().unwrap_or_default(),
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
            ))
        })?
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_failing_flow_is_retried_then_binned() {
    // Given a store with an event at status 0 whose flow fails every time
    let path = database("a_failing_flow_is_retried_then_binned").unwrap();
    let store = migrated(&path).await.unwrap();
    let failing: Arc<Flow<Ty, String>> = Arc::new(Flow {
        job: false,
        run: Box::new(
            |event: &Row, _: &Row, sender: &rannoch_core::Sender<Ty, String>| {
                let failure = format!("failed {}", event.id);
                let _ = sender.send(Event {
                    id: Uuid::nil(),
                    source: Some(event.id),
                    ty: Ty(event.ty),
                    data: failure.clone(),
                    run_at: None,
                    status: Status::Binned,
                });
                Err(failure)
            },
        ),
    });
    let flows = Arc::new(Fake {
        flows: HashMap::from([(4, failing)]),
    });

    // And the queue's settings are 3 attempts and a short first wait
    let settings = Settings {
        attempts: 3,
        ..Settings::new(Duration::from_millis(100))
    };
    let (signal, wake) = mpsc::unbounded_channel();
    let rannoch = Rannoch::open(Config {
        store: Some(store.clone()),
        append: Some(Arc::new(Events::new(ready(&path).unwrap(), signal.clone()))
            as Arc<dyn Append<Ty, String>>),
        ..Config::default()
    })
    .unwrap();
    let queue = Queue::start(
        ready(&path).unwrap(),
        wake,
        rannoch.sender(),
        flows,
        settings,
    );
    let read = ready(&path).unwrap();
    let retry_of = |id: Uuid| {
        rows_about(&read, id)
            .unwrap()
            .into_iter()
            .find(|r| r.1 == 0)
    };

    // When the events crate signals the queue
    // run_at is kept to the millisecond
    let before = clock::now() - TimeDelta::milliseconds(1);
    // The event is written after the queue starts, so the signal is what takes it.
    let original = waiting(&ready(&path).unwrap(), 4, None).unwrap();
    signal.send(()).unwrap();

    // Then the processor sends a -1 for the attempt, and the queue sends a retry with status 0,
    // the attempt as its source, the failure's result as its data and a run_at after the first
    // wait
    assert!(until(Duration::from_secs(5), || retry_of(original).is_some()).await);
    let failed = format!("\"failed {original}\"");
    assert!(
        rows_about(&read, original)
            .unwrap()
            .iter()
            .any(|r| r.1 == -1 && r.2 == failed)
    );
    let (first, _, data, run_at) = retry_of(original).unwrap();
    assert_eq!(data, failed);
    let run_at: chrono::DateTime<Utc> = run_at.unwrap().parse().unwrap();
    assert!(
        run_at >= before + TimeDelta::milliseconds(100),
        "{run_at} too soon"
    );

    // When the retry comes due and fails
    // Then the processor sends a -1 for it, and the queue sends a second retry whose source is
    // the first retry, not the original event
    assert!(until(Duration::from_secs(5), || retry_of(first).is_some()).await);
    assert!(rows_about(&read, first).unwrap().iter().any(|r| r.1 == -1));
    let (second, ..) = retry_of(first).unwrap();
    let pointing_at_original = rows_about(&read, original)
        .unwrap()
        .iter()
        .filter(|r| r.1 == 0)
        .count();
    assert_eq!(
        pointing_at_original, 1,
        "a retry pointed at the original event"
    );

    // When the second retry comes due and fails
    // Then the processor sends a -1 for it, and the queue sends nothing more, because its chain
    // has reached 3 attempts
    assert!(
        until(Duration::from_secs(5), || {
            rows_about(&read, second).unwrap().iter().any(|r| r.1 == -1)
        })
        .await
    );
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(retry_of(second), None, "a fourth attempt was sent");

    drop(signal);
    stop(rannoch, queue).await.unwrap();
}

/// A row from the app's forgr factory about `source`, with its status and, when given, `run_at`.
fn status_row(
    conn: &Connection,
    source: Uuid,
    ty: i64,
    status: i64,
    run_at: Option<chrono::DateTime<Utc>>,
) -> Result<Uuid> {
    let id = new_id();
    let row = _events::factory()
        .id(id.to_string())
        .source(source.to_string())
        .ty(ty)
        .data("{}")
        .status(status);
    match run_at {
        Some(at) => row.run_at(at.to_rfc3339_opts(SecondsFormat::Millis, true)),
        None => row,
    }
    .insert(conn)?;
    Ok(id)
}

#[tokio::test(start_paused = true)]
async fn the_hunt_reruns_a_dropped_run() {
    // Given a store with an event from the app's forgr factory whose status 1 is older than the
    // retry window, with no 2 or -1 after it, as a crash leaves it
    let path = database("the_hunt_reruns_a_dropped_run").unwrap();
    let store = migrated(&path).await.unwrap();
    let seed = ready(&path).unwrap();
    // Each row is a millisecond after the last, so ids keep their order whatever another test's
    // frozen clock reads.
    let tick = || tokio::time::advance(Duration::from_millis(1));
    let hung = waiting(&seed, 1, None).unwrap();
    tick().await;
    status_row(&seed, hung, 1, 1, None).unwrap();

    // And the queue's settings are a short retry window and a short hunt interval
    let settings = Settings {
        window: Duration::from_millis(100),
        hunt: Duration::from_millis(300),
        ..Settings::new(Duration::from_millis(100))
    };

    // And an event whose status 1 is older than the retry window and is followed by a -1 and a
    // retry waiting on its run_at
    tick().await;
    let failed = waiting(&seed, 4, None).unwrap();
    tick().await;
    status_row(&seed, failed, 4, 1, None).unwrap();
    tick().await;
    status_row(&seed, failed, 4, -1, None).unwrap();
    tick().await;
    status_row(
        &seed,
        failed,
        4,
        0,
        Some(clock::now() + TimeDelta::hours(1)),
    )
    .unwrap();
    tokio::time::advance(Duration::from_millis(150)).await;

    let ran: Ran = Arc::default();
    let flows = Arc::new(Fake {
        flows: HashMap::from([(1, processor(false, Wait::Nothing, ran))]),
    });
    let (signal, wake) = mpsc::unbounded_channel();
    let rannoch = Rannoch::open(Config {
        store: Some(store.clone()),
        append: Some(Arc::new(Events::new(ready(&path).unwrap(), signal.clone()))
            as Arc<dyn Append<Ty, String>>),
        ..Config::default()
    })
    .unwrap();
    let queue = Queue::start(
        ready(&path).unwrap(),
        wake,
        rannoch.sender(),
        flows,
        settings,
    );
    let read = ready(&path).unwrap();
    let resent = |id: Uuid| {
        rows_about(&read, id)
            .unwrap()
            .into_iter()
            .filter(|r| r.1 == 0)
            .collect::<Vec<_>>()
    };

    // When the hunt runs
    // Then the hung event is sent again as a retry, with status 0 and the hung event as its
    // source
    assert!(until(Duration::from_secs(5), || resent(hung).len() == 1).await);
    assert_eq!(resent(hung)[0].2, "\"hung\"");

    // And the failure waiting on its retry is left alone
    assert_eq!(
        resent(failed).len(),
        1,
        "the waiting failure was sent again"
    );

    // and a hunt after that finds the retry after the 1, so sends the hung event only once
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(resent(hung).len(), 1, "the hung event was sent again twice");

    drop(signal);
    stop(rannoch, queue).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_app_reads_the_queues_report() {
    // Given a store with rows from the app's forgr factory at each of the four statuses
    let path = database("the_app_reads_the_queues_report").unwrap();
    migrated(&path).await.unwrap();
    let conn = ready(&path).unwrap();
    for (status, rows) in [(-1, 1), (0, 2), (1, 3), (2, 4)] {
        for _ in 0..rows {
            _events::factory()
                .id(new_id().to_string())
                .ty(1)
                .data("{}")
                .status(status)
                .insert(&conn)
                .unwrap();
        }
    }

    // When the app asks the queue for its report
    let report = rannoch_queue::report(&conn).unwrap();

    // Then it gives the number of _events rows at each status
    assert_eq!(
        report,
        [
            (Status::Binned, 1),
            (Status::Added, 2),
            (Status::InProgress, 3),
            (Status::Processed, 4),
        ]
    );
}

/// Every `.rs` file of rannoch-queue's code and tests, with its path.
fn sources() -> std::io::Result<Vec<(PathBuf, String)>> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in ["src", "tests"] {
        for entry in fs::read_dir(crate_dir.join(dir))? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "rs") {
                let text = fs::read_to_string(&path)?;
                files.push((path, text));
            }
        }
    }
    Ok(files)
}

#[tokio::test(start_paused = true)]
async fn rannoch_queue_calls_no_real_time_function() {
    // Given rannoch-queue's code and tests
    let files = sources().unwrap();
    assert!(files.len() >= 5, "{files:?}");

    // When they are read for a call to any real-time function; the opening paren is added here,
    // so this list is not itself a call
    let names = [
        "thread::sleep",
        "thread::spawn",
        "recv_timeout",
        "Instant::now",
        "SystemTime::now",
        "Utc::now",
        "now_v7",
    ];
    let calls: Vec<String> = names.iter().map(|name| format!("{name}(")).collect();

    // Then none is found
    for (path, text) in &files {
        for call in &calls {
            assert!(
                !text.contains(call.as_str()),
                "{} calls {call}",
                path.display()
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn every_rannoch_queue_test_runs_with_the_clock_frozen() {
    // Given rannoch-queue's tests, the doc example among them
    let files = sources().unwrap();

    // When they are read for how each one runs
    let mut runs = 0;
    for (path, text) in &files {
        for line in text.lines().map(str::trim) {
            let line = line.trim_start_matches("//!").trim();
            // Then none runs as a plain test, and each tokio test or main freezes the clock
            assert_ne!(line, "#[test]", "{} has a plain test", path.display());
            let tokio_run = line.starts_with("#[tokio::test") || line.starts_with("#[tokio::main");
            if tokio_run {
                assert!(
                    line.ends_with("start_paused = true)]"),
                    "{}: {line}",
                    path.display()
                );
                runs += 1;
            }
        }
    }
    // 7 in the library, its doc example's main, 7 here, 3 in failures.rs and 2 in
    // queue_settings.rs
    assert_eq!(runs, 20);
}
