//! #108: each failure the queue meets about an event becomes one -1 for that event, whose data
//! names the reason. Each test runs the real store, rannoch, `Events`, the queue and
//! rannoch-processor on the built-in steps of rannoch-processor's fixture `tests/built_in.json`,
//! over `_events` seeded from the app's forgr factory as a crash or a bad write leaves it. A run
//! that panics, the runs channel closing and a panic on the queue's thread have no test here: no
//! built-in step panics.

use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use std::{fs, thread};

use chrono::{SecondsFormat, TimeDelta, Utc};
use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_processor::{FlowDoc, Processor, check};
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::Value;
use uuid::Uuid;

/// The app's `ty`: the built-in flows' 80 to 87, and no other.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Ty(u16);
impl From<Ty> for u16 {
    fn from(t: Ty) -> u16 {
        t.0
    }
}
impl TryFrom<u16> for Ty {
    type Error = u16;
    fn try_from(v: u16) -> std::result::Result<Self, u16> {
        if (80..=87).contains(&v) {
            Ok(Ty(v))
        } else {
            Err(v)
        }
    }
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// An inbox that hands over only at shutdown, so a status the queue sent twice is on disk twice.
const HOUR: TimeDelta = TimeDelta::hours(1);
/// The default inbox's interval.
const FAST: TimeDelta = TimeDelta::milliseconds(20);
const WAIT: Duration = Duration::from_secs(5);

/// A migrated store in a fresh directory `name`, applied by opening rannoch once.
fn store(name: &str) -> Result<PathBuf> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let path = dir.join("store.db");
    let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
    let config = Config {
        store: Some(store),
        ..Config::default()
    };
    Rannoch::<Ty, Value>::open(config)?.shutdown();
    Ok(path)
}

fn ready(path: &Path) -> Result<Connection> {
    match rannoch_store::open(path, &rannoch_core::migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(p) => Err(format!("pending: {p:?}").into()),
    }
}

/// A row for the app's forgr factory: id, source, ty, data, `run_at` and status.
type Seed<'a> = (&'a str, Option<&'a str>, i64, &'a str, Option<&'a str>, i64);

fn seed(path: &Path, rows: &[Seed<'_>]) -> Result<()> {
    let conn = ready(path)?;
    for &(id, source, ty, data, run_at, status) in rows {
        let row = _events::factory().id(id.to_owned()).ty(ty).data(data);
        let row = match source {
            Some(source) => row.source(source.to_owned()),
            None => row,
        };
        let row = match run_at {
            Some(at) => row.run_at(at.to_owned()),
            None => row,
        };
        row.status(status).insert(&conn)?;
    }
    Ok(())
}

/// The app: rannoch over the store with `Events` as its append, its default inbox handing over
/// every `interval`, and the queue handing each event to rannoch-processor on the built-in flows.
struct App {
    signal: mpsc::Sender<()>,
    rannoch: Rannoch<Ty, Value>,
    queue: Queue,
}

impl App {
    fn start(path: &Path, interval: TimeDelta, settings: Settings) -> Result<Self> {
        let built_in = include_str!("../../rannoch-processor/tests/built_in.json");
        let docs: Vec<(String, FlowDoc)> = serde_json::from_str(built_in)?;
        let flows = check(&docs, &[]).map_err(|errors| errors.join("\n"))?;
        let processor = Arc::new(Processor::new(&flows, Vec::new(), ready(path)?)?);
        let (signal, wake) = mpsc::channel();
        let events = Events::new(ready(path)?, signal.clone()).with_writes(processor.writes());
        let rannoch = Rannoch::open(Config {
            default_interval: interval,
            store: Some(Arc::new(Store::new(
                path.to_owned(),
                rannoch_core::migrator(),
            ))),
            append: Some(Arc::new(events) as Arc<dyn Append<Ty, Value>>),
            ..Config::default()
        })?;
        let queue = Queue::start(ready(path)?, wake, rannoch.sender(), processor, settings);
        Ok(Self {
            signal,
            rannoch,
            queue,
        })
    }

    /// Signals the queue twice, a pass apart, so an event it would take twice is taken twice.
    fn signal_twice(&self) -> Result<()> {
        for _ in 0..2 {
            self.signal.send(())?;
            thread::sleep(Duration::from_millis(200));
        }
        Ok(())
    }

    /// Stops the app; every event it sent is then on disk.
    fn stop(self) -> Result<()> {
        drop(self.signal);
        self.rannoch.shutdown();
        self.queue.join()?;
        Ok(())
    }
}

/// One attempt, a short first wait and the default hunt.
fn once() -> Settings {
    Settings {
        attempts: 1,
        ..Settings::new(Duration::from_millis(100))
    }
}

/// As `once`, with a short window and a hunt that runs soon and often.
fn hunting() -> Settings {
    Settings {
        window: Duration::from_millis(100),
        hunt: Duration::from_millis(300),
        ..once()
    }
}

/// The (status, ty, data) of each row whose source is `id`, in order of id.
fn about(path: &Path, id: &str) -> Result<Vec<(i64, i64, String)>> {
    let conn = ready(path)?;
    let sql = "SELECT status, ty, data FROM _events WHERE source = ?1 ORDER BY id";
    let mut select = conn.prepare(sql)?;
    let rows = select.query_map([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// `why` as the append writes the queue's text: a JSON string.
fn said(why: &str) -> String {
    Value::from(why).to_string()
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

/// An id `secs` seconds ago.
fn ago(secs: i64) -> String {
    let at = u64::try_from(Utc::now().timestamp() - secs).unwrap_or_default();
    Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, at, 0)).to_string()
}

fn id() -> String {
    Uuid::now_v7().to_string()
}

/// 357-364: the queue cannot send a status of the app's `T` for this event, so it writes the -1.
#[test]
fn a_due_event_whose_ty_is_not_the_apps_gets_a_minus_1() -> Result<()> {
    let path = store("ty_not_the_apps")?;
    let event = id();
    seed(&path, &[(&event, None, 9, r#"{"ticket":"t1"}"#, None, 0)])?;

    let app = App::start(&path, HOUR, once())?;
    app.signal_twice()?;
    app.stop()?;

    assert_eq!(
        about(&path, &event)?,
        [(-1, 9, said("ty 9 is not the app's"))]
    );
    Ok(())
}

/// 659-667, the ty: a ty that is not a `u16` fails its own row, not the whole due read.
#[test]
fn a_due_event_whose_ty_is_not_a_u16_gets_a_minus_1_and_the_rest_run() -> Result<()> {
    let path = store("ty_not_a_u16")?;
    let (big, next) = (id(), id());
    seed(
        &path,
        &[
            (&big, None, 70_000, r#"{"ticket":"t1"}"#, None, 0),
            (&next, None, 82, r#"{"ticket":"t2"}"#, None, 0),
        ],
    )?;

    let app = App::start(&path, HOUR, once())?;
    app.signal_twice()?;
    app.stop()?;

    let binned = (-1, 70_000, said("ty 70000 is not the app's"));
    assert_eq!(about(&path, &big)?, [binned]);
    let ran = [(1, 82, said("taken")), (2, 82, said("done"))];
    assert_eq!(about(&path, &next)?, ran);
    Ok(())
}

/// 659-667: a due event whose source is not a UUID.
#[test]
fn a_due_event_that_cannot_be_read_gets_a_minus_1() -> Result<()> {
    let path = store("cannot_be_read")?;
    let event = id();
    let data = r#"{"ticket":"t1"}"#;
    seed(&path, &[(&event, Some("nobody"), 82, data, None, 0)])?;

    let app = App::start(&path, HOUR, once())?;
    app.signal_twice()?;
    app.stop()?;

    let binned = (-1, 82, said("its source nobody is not a UUID"));
    assert_eq!(about(&path, &event)?, [binned]);
    Ok(())
}

/// 365-372: the walk to a retry's root reads a source it cannot read.
#[test]
fn a_retry_whose_chain_cannot_be_read_gets_a_minus_1() -> Result<()> {
    let path = store("chain_cannot_be_read")?;
    let (first, retry) = (id(), id());
    let data = r#"{"ticket":"t1"}"#;
    seed(
        &path,
        &[
            (&first, None, 82, data, Some("soon"), 0),
            (&id(), Some(&first), 82, r#""failed""#, None, -1),
            (&retry, Some(&first), 82, data, None, 0),
        ],
    )?;

    let app = App::start(&path, HOUR, once())?;
    app.signal_twice()?;
    app.stop()?;

    let why = format!("{first} in its chain cannot be read: its run_at soon is not a time");
    assert_eq!(about(&path, &retry)?, [(-1, 82, said(&why))]);
    Ok(())
}

/// 609-615: the walk to a retry's root reaches a source that is not in `_events`.
#[test]
fn a_retry_whose_source_is_not_in_events_gets_a_minus_1() -> Result<()> {
    let path = store("source_not_in_events")?;
    let (gone, retry) = (id(), id());
    seed(
        &path,
        &[
            (&id(), Some(&gone), 82, r#""failed""#, None, -1),
            (&retry, Some(&gone), 82, r#"{"ticket":"t1"}"#, None, 0),
        ],
    )?;

    let app = App::start(&path, HOUR, once())?;
    app.signal_twice()?;
    app.stop()?;

    let why = format!("{gone} in its chain is not in _events");
    assert_eq!(about(&path, &retry)?, [(-1, 82, said(&why))]);
    Ok(())
}

/// Starts the app over the store at `path` with the hunt running every 300 ms, waits for a -1
/// about `event` and for two more hunts, and stops it.
fn hunt(path: &Path, event: &str) -> Result<()> {
    let app = App::start(path, FAST, hunting())?;
    let binned = || about(path, event).is_ok_and(|rows| rows.iter().any(|r| r.0 == -1));
    assert!(until(WAIT, binned), "no -1 about {event}");
    thread::sleep(Duration::from_millis(700));
    app.stop()
}

/// 515-519: a 1 whose id does not parse, as a crash leaves it.
#[test]
fn the_hunt_bins_an_event_whose_1_has_an_id_that_does_not_parse() -> Result<()> {
    let path = store("hunt_one_does_not_parse")?;
    let event = ago(7200);
    seed(
        &path,
        &[
            (&event, None, 82, r#"{"ticket":"t1"}"#, None, 0),
            ("not-an-id", Some(&event), 82, r#""taken""#, None, 1),
        ],
    )?;

    hunt(&path, &event)?;

    let binned = (-1, 82, said("its 1 not-an-id is not a UUIDv7"));
    assert_eq!(about(&path, &event)?, [binned, (1, 82, said("taken"))]);
    Ok(())
}

/// 475: a 1 whose id is not a `UUIDv7`, so it has no time to tell the hunt how old it is.
#[test]
fn the_hunt_bins_an_event_whose_1_is_not_a_uuidv7() -> Result<()> {
    let path = store("hunt_one_not_v7")?;
    let event = ago(7200);
    let one = uuid::Builder::from_random_bytes([7; 16])
        .into_uuid()
        .to_string();
    seed(
        &path,
        &[
            (&event, None, 82, r#"{"ticket":"t1"}"#, None, 0),
            (&one, Some(&event), 82, r#""taken""#, None, 1),
        ],
    )?;

    hunt(&path, &event)?;

    let binned = (-1, 82, said(&format!("its 1 {one} is not a UUIDv7")));
    assert_eq!(about(&path, &event)?, [binned, (1, 82, said("taken"))]);
    Ok(())
}

/// 487-489: a stalled event the hunt would resend, but whose ty is not the app's.
#[test]
fn the_hunt_bins_a_stalled_event_whose_ty_is_not_the_apps() -> Result<()> {
    let path = store("hunt_ty_not_the_apps")?;
    let event = ago(7200);
    seed(
        &path,
        &[
            (&event, None, 9, r#"{"ticket":"t1"}"#, None, 0),
            (&ago(3600), Some(&event), 9, r#""taken""#, None, 1),
        ],
    )?;

    hunt(&path, &event)?;

    let binned = (-1, 9, said("ty 9 is not the app's"));
    assert_eq!(about(&path, &event)?, [(1, 9, said("taken")), binned]);
    Ok(())
}

/// 426-428 and 447-457: a failed run whose retry's wait overflows.
#[test]
fn a_failed_run_whose_retry_wait_overflows_gets_a_minus_1_and_no_retry() -> Result<()> {
    let path = store("retry_wait_overflows")?;
    let event = id();
    seed(&path, &[(&event, None, 87, r#"{"order":"o1"}"#, None, 0)])?;
    let settings = Settings {
        attempts: 2,
        ..Settings::new(Duration::MAX)
    };

    let app = App::start(&path, HOUR, settings)?;
    app.signal_twice()?;
    app.stop()?;

    let rows = [
        (1, 87, said("taken")),
        (-1, 87, said("store: no such table: nope")),
        (-1, 87, said("its retry's wait overflows")),
    ];
    assert_eq!(about(&path, &event)?, rows);
    Ok(())
}

/// 391 and 620-637: the earliest waiting event's `run_at` is not a time, and an event waits
/// after it.
#[test]
fn a_waiting_event_whose_run_at_is_not_a_time_gets_a_minus_1_and_the_next_runs() -> Result<()> {
    let path = store("run_at_not_a_time")?;
    let at = |ms| {
        (Utc::now() + TimeDelta::milliseconds(ms)).to_rfc3339_opts(SecondsFormat::Millis, true)
    };
    let (bad, next) = (id(), id());
    let not_a_time = at(1_000).replace('Z', "Q");
    seed(
        &path,
        &[
            (&bad, None, 82, r#"{"ticket":"t1"}"#, Some(&not_a_time), 0),
            (&next, None, 82, r#"{"ticket":"t2"}"#, Some(&at(2_000)), 0),
        ],
    )?;

    let app = App::start(&path, FAST, once())?;
    let ran = || about(&path, &next).is_ok_and(|rows| rows.iter().any(|r| r.0 == 2));
    let on_time = until(WAIT, ran);
    app.stop()?;

    assert!(on_time, "the event after it did not run");
    let why = format!("its run_at {not_a_time} is not a time");
    assert_eq!(about(&path, &bad)?, [(-1, 82, said(&why))]);
    Ok(())
}
