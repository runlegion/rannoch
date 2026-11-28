//! The configuration, open, and the broker that puts each event in its lane.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use std::{env, fmt, io, mem};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc;
use uuid::timestamp::context::ContextV7;
use uuid::{Timestamp, Uuid};

use crate::lane::{Lane, run};
use crate::{Error, Event, Status, clock};

/// What core needs from the app's store at start.
pub trait Storage: fmt::Debug + Send + Sync {
    /// Whether migrations are pending.
    ///
    /// # Errors
    ///
    /// `Store` when the store cannot tell.
    fn pending(&self) -> Result<bool, Error>;

    /// Applies the pending migrations.
    ///
    /// # Errors
    ///
    /// `Store` when applying a pending migration fails.
    fn apply(&self) -> Result<(), Error>;
}

/// What core needs from the events crate: the append each lane hands its batch to.
pub trait Append<T, D>: fmt::Debug + Send + Sync {
    /// Appends `batch` as one write, and returns the events it sends in reply, such as a -1 for
    /// an event of the batch it did not write; the lane accepts each onto its own heap and hands
    /// them over in one more batch.
    ///
    /// # Errors
    ///
    /// `Store` when the write fails. The lane then logs the failure, puts the batch back on its
    /// heap ahead of the events sent since, and hands all of them over at its next batch.
    fn append(&self, batch: &[Event<T, D>]) -> Result<Vec<Event<T, D>>, Error>;
}

/// The app's configuration: the app's store, the events crate's append and the settings file
/// its lanes come from.
#[derive(Debug, Clone)]
pub struct Config<T, D> {
    /// The app's store; with none, open runs no migration check. Rannoch
    /// keeps it for as long as it runs.
    pub store: Option<Arc<dyn Storage>>,
    /// The events crate's append; with none, no lane hands anything over.
    pub append: Option<Arc<dyn Append<T, D>>>,
    /// The app's settings file, which the first open loads when the app has not loaded its
    /// settings itself; with none, `rannoch.config.toml` in the directory of the running binary.
    pub settings: Option<PathBuf>,
}

impl<T, D> Default for Config<T, D> {
    fn default() -> Self {
        Self {
            store: None,
            append: None,
            settings: None,
        }
    }
}

/// One `[lane.<name>]` section of the app's settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
struct LaneSettings {
    /// The `ty` values whose events go to the lane.
    tys: Vec<u16>,
    /// The size of the lane's heap, in bytes, at which its batch is written: 8 MB by default.
    memory_limit_bytes: usize,
    /// How long the lane waits after a batch before it writes the next: 20 ms by default.
    heartbeat_ms: u64,
}

impl Default for LaneSettings {
    fn default() -> Self {
        Self {
            tys: Vec::new(),
            memory_limit_bytes: 8 * 1024 * 1024,
            heartbeat_ms: 20,
        }
    }
}

/// The lane for every `ty` no lane lists.
const DEFAULT: usize = 0;
/// The lane for every event with a `run_at`.
const SCHEDULED: usize = 1;

#[derive(Debug)]
pub(crate) struct Broker<T, D> {
    /// The default lane, the scheduled lane, then the other lanes the settings name.
    pub(crate) lanes: Vec<Lane<T, D>>,
    pub(crate) closed: bool,
    /// This rannoch's own id counter, never started over, so its ids strictly increase even
    /// when another rannoch's frozen clock stands later.
    pub(crate) ids: ContextV7,
    /// An event's size in memory, in bytes, which its lane's memory limit counts.
    pub(crate) weigh: fn(&Event<T, D>) -> usize,
}

impl<T, D> Broker<T, D> {
    /// A new id from [`clock::now`]: a `UUIDv7` greater than every id this broker stamped
    /// before, taken under the lock the caller holds.
    pub(crate) fn stamp(&self) -> Uuid {
        let now = clock::now();
        let seconds = u64::try_from(now.timestamp()).unwrap_or(0);
        Uuid::new_v7(Timestamp::from_unix(
            &self.ids,
            seconds,
            now.timestamp_subsec_nanos(),
        ))
    }

    /// Stamps `event` and accepts it onto lane `index`'s heap. The stamp and the heap's push are
    /// under one lock, so a lane's heap is in id order.
    pub(crate) fn put(&mut self, index: usize, event: Event<T, D>) -> Status {
        let (id, bytes) = (self.stamp(), (self.weigh)(&event));
        self.lanes[index].accept(id, event, bytes)
    }
}

impl<T: PartialEq, D> Broker<T, D> {
    /// Puts `event` in its lane: the scheduled lane when it has a `run_at`, else the lane its
    /// `ty` is listed in, else the default lane.
    pub(crate) fn send(&mut self, event: Event<T, D>) -> Result<Status, Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        let index = if event.run_at.is_some() {
            SCHEDULED
        } else {
            self.lanes
                .iter()
                .position(|lane| lane.tys.contains(&event.ty))
                .unwrap_or(DEFAULT)
        };
        Ok(self.put(index, event))
    }
}

/// Counts the bytes written to it.
struct Count(usize);

impl io::Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// An event's size: the event itself and its data's JSON. Data that does not serialize weighs
/// what it wrote before it failed.
fn weigh<T, D: Serialize>(event: &Event<T, D>) -> usize {
    let mut count = Count(0);
    let _ = serde_json::to_writer(&mut count, &event.data);
    mem::size_of::<Event<T, D>>() + count.0
}

pub(crate) type Shared<T, D> = Arc<Mutex<Broker<T, D>>>;

pub(crate) fn lock<T, D>(broker: &Shared<T, D>) -> MutexGuard<'_, Broker<T, D>> {
    broker.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The owner's handle: the only value that can shut rannoch down.
#[derive(Debug)]
pub struct Rannoch<T, D> {
    broker: Shared<T, D>,
    /// Closes once every lane's task has returned: each holds a sender until then.
    done: mpsc::Receiver<()>,
    /// The runtime open started when it was called outside one, kept for as long as rannoch
    /// runs.
    runtime: Option<Runtime>,
    /// The app's store, kept for as long as rannoch runs.
    _store: Option<Arc<dyn Storage>>,
}

impl<T: PartialEq + Send + 'static, D: Send + 'static> Rannoch<T, D> {
    /// Opens rannoch with a configuration. Before anything else, loads the settings when the app
    /// has not: the file at `settings`, else `rannoch.config.toml` beside the running binary.
    /// Then applies the store's pending migrations. Its lanes are the default lane, the
    /// scheduled lane and one for each `[lane.<name>]` section of the settings, each with the
    /// `tys` it holds, its `memory_limit_bytes` (8 MB by default) and its `heartbeat_ms` (20 by
    /// default); a `[lane.default]` or `[lane.scheduled]` section sets that lane's. With an
    /// append, it spawns one task for each lane on the tokio runtime it is called in, or on one
    /// it starts when called outside a runtime.
    ///
    /// # Panics
    ///
    /// When it is called outside a runtime and the runtime it starts cannot be built, as the
    /// operating system refusing it threads.
    ///
    /// # Errors
    ///
    /// `Store` when the store cannot tell what is pending or applying fails;
    /// `Config` when the settings file does not parse, the same `ty` is listed in two lanes, or
    /// a listed `ty` is not one of the app's.
    pub fn open(config: Config<T, D>) -> Result<Self, Error>
    where
        T: TryFrom<u16>,
        D: Serialize,
    {
        if !rannoch_settings::loaded() {
            let path = config.settings.clone().or_else(|| {
                let exe = env::current_exe().ok()?;
                Some(exe.parent()?.join("rannoch.config.toml"))
            });
            // A load refused because an open on another thread loaded first is not a failure.
            if let Some(path) = path
                && rannoch_settings::load(&path).is_err()
                && !rannoch_settings::loaded()
            {
                return Err(Error::Config);
            }
        }
        if let Some(store) = &config.store
            && store.pending().map_err(|_| Error::Store)?
        {
            store.apply().map_err(|_| Error::Store)?;
            log::debug!("applied pending migrations");
        }
        let lanes = lanes(rannoch_settings::section("lane"))?;
        log::debug!("opened with {} lanes", lanes.len());
        let tasks: Vec<_> = lanes
            .iter()
            .map(|lane| (lane.heartbeat, Arc::clone(&lane.wake)))
            .collect();
        let broker = Arc::new(Mutex::new(Broker {
            lanes,
            closed: false,
            ids: ContextV7::new(),
            weigh: weigh::<T, D>,
        }));
        let (finished, done) = mpsc::channel::<()>(1);
        let mut runtime = None;
        if let Some(append) = config.append {
            let handle = Handle::try_current().unwrap_or_else(|_| {
                // Building a runtime fails only when the system refuses its threads or its
                // driver, as `std::thread::spawn` panicked on before.
                #[allow(clippy::expect_used)]
                let started = Runtime::new().expect("rannoch could not start a tokio runtime");
                runtime.insert(started).handle().clone()
            });
            for (index, (heartbeat, wake)) in tasks.into_iter().enumerate() {
                let (broker, append, finished) =
                    (Arc::clone(&broker), Arc::clone(&append), finished.clone());
                handle.spawn(async move {
                    run(broker, index, heartbeat, wake, append).await;
                    drop(finished);
                });
            }
        }
        Ok(Self {
            broker,
            done,
            runtime,
            _store: config.store,
        })
    }

    /// A sender for a producer.
    #[must_use]
    pub fn sender(&self) -> Sender<T, D> {
        Sender {
            broker: Arc::clone(&self.broker),
        }
    }

    /// Shuts rannoch down: every send after this fails with `Closed`, each lane hands over what
    /// is left on its heap, and this blocks until each lane's task has returned. When that last
    /// hand-over fails, the failure is logged and those events are not written.
    ///
    /// It blocks, so async code calls it through `tokio::task::spawn_blocking`.
    ///
    /// # Panics
    ///
    /// When it is called on a thread that runs async tasks, rather than through
    /// `spawn_blocking`.
    pub fn shutdown(self) {
        {
            let mut held = lock(&self.broker);
            held.closed = true;
            for lane in &held.lanes {
                lane.wake.notify_one();
            }
        }
        let mut done = self.done;
        // No task sends; the channel closes once the last one has returned.
        let _ = done.blocking_recv();
        drop(self.runtime);
    }
}

/// The lanes the `[lane.<name>]` sections give, after the default and the scheduled lane.
fn lanes<T: PartialEq + TryFrom<u16>, D>(
    sections: BTreeMap<String, LaneSettings>,
) -> Result<Vec<Lane<T, D>>, Error> {
    let lane = |name: String, settings: LaneSettings| -> Result<Lane<T, D>, Error> {
        let tys = settings.tys.into_iter().map(T::try_from);
        Ok(Lane::new(
            name,
            tys.collect::<Result<_, _>>().map_err(|_| Error::Config)?,
            settings.memory_limit_bytes,
            Duration::from_millis(settings.heartbeat_ms),
        ))
    };
    let mut lanes = Vec::new();
    for name in ["default", "scheduled"] {
        lanes.push(lane(name.to_owned(), LaneSettings::default())?);
    }
    for (name, settings) in sections {
        let made = lane(name, settings)?;
        match lanes.iter().position(|l| l.name == made.name) {
            Some(i) => lanes[i] = made,
            None => lanes.push(made),
        }
    }
    for (i, a) in lanes.iter().enumerate() {
        if lanes[i + 1..]
            .iter()
            .any(|b| a.tys.iter().any(|t| b.tys.contains(t)))
        {
            return Err(Error::Config);
        }
    }
    Ok(lanes)
}

/// A producer's handle, with `send` and nothing else.
#[derive(Debug)]
pub struct Sender<T, D> {
    broker: Shared<T, D>,
}

impl<T, D> Clone for Sender<T, D> {
    fn clone(&self) -> Self {
        Self {
            broker: Arc::clone(&self.broker),
        }
    }
}

impl<T: PartialEq, D> Sender<T, D> {
    /// Sends an event to the broker, which puts it in its lane: the scheduled lane when it has a
    /// `run_at`, else the lane its `ty` is listed in, else the default lane. Returns
    /// `Status::Added` once the event is on that lane's heap; the lane writes it with its next
    /// batch.
    ///
    /// # Errors
    ///
    /// `Closed` when rannoch is shutting down.
    pub fn send(&self, event: Event<T, D>) -> Result<Status, Error> {
        lock(&self.broker).send(event)
    }
}

#[cfg(test)]
mod tests {
    use super::{Append, Config, LaneSettings, Rannoch, Storage, lanes, lock};
    use crate::{Error, Event, Status, clock};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use uuid::Uuid;

    /// A store with a fixed answer to `pending` and to `apply`, counting applies.
    #[derive(Debug)]
    struct FakeStore {
        pending: Result<bool, Error>,
        apply: Result<(), Error>,
        applied: Mutex<usize>,
    }

    impl Storage for FakeStore {
        fn pending(&self) -> Result<bool, Error> {
            self.pending
        }
        fn apply(&self) -> Result<(), Error> {
            *self.applied.lock().unwrap() += 1;
            self.apply
        }
    }

    fn open_with(
        pending: Result<bool, Error>,
        apply: Result<(), Error>,
    ) -> (Result<(), Error>, usize) {
        let store = Arc::new(FakeStore {
            pending,
            apply,
            applied: Mutex::new(0),
        });
        let config = Config::<Ty, &str> {
            store: Some(store.clone()),
            ..Config::default()
        };
        let opened = Rannoch::<Ty, &str>::open(config).map(|_| ());
        let applied = *store.applied.lock().unwrap();
        (opened, applied)
    }

    #[tokio::test(start_paused = true)]
    async fn pending_migrations_are_applied_once_then_open_succeeds() {
        assert_eq!(open_with(Ok(true), Ok(())), (Ok(()), 1));
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_pending_applies_nothing() {
        assert_eq!(open_with(Ok(false), Ok(())), (Ok(()), 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_pending_check_fails_open_with_store_and_applies_nothing() {
        assert_eq!(open_with(Err(Error::Queue), Ok(())), (Err(Error::Store), 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_apply_fails_open_with_store() {
        assert_eq!(
            open_with(Ok(true), Err(Error::Queue)),
            (Err(Error::Store), 1)
        );
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Ty {
        Asset,
        Page,
    }

    impl TryFrom<u16> for Ty {
        type Error = u16;
        fn try_from(ty: u16) -> Result<Self, u16> {
            match ty {
                0 => Ok(Self::Asset),
                1 => Ok(Self::Page),
                other => Err(other),
            }
        }
    }

    fn event(ty: Ty) -> Event<Ty, &'static str> {
        Event {
            id: Uuid::nil(),
            source: None,
            ty,
            data: "data",
            run_at: None,
            status: Status::Processed,
        }
    }

    /// The `[lane.<name>]` sections, each listing the asset ty (0).
    fn assets(names: &[&str]) -> BTreeMap<String, LaneSettings> {
        let section = || LaneSettings {
            tys: vec![0],
            heartbeat_ms: 500,
            ..LaneSettings::default()
        };
        names.iter().map(|n| ((*n).to_owned(), section())).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn fr_rannoch_003_c1() {
        let rannoch = Rannoch::<Ty, &str>::open(Config::default()).unwrap();
        let broker = lock(&rannoch.broker);
        let names: Vec<&str> = broker.lanes.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, ["default", "scheduled"]);
        for lane in &broker.lanes {
            assert_eq!(lane.heartbeat, Duration::from_millis(20));
            assert_eq!(lane.limit, 8 * 1024 * 1024);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fr_rannoch_003_c2() {
        let rannoch = Rannoch::<Ty, &str>::open(Config::default()).unwrap();
        lock(&rannoch.broker).lanes = lanes(assets(&["assets"])).unwrap();
        let sender = rannoch.sender();
        sender.send(event(Ty::Asset)).unwrap();
        sender.send(event(Ty::Page)).unwrap();
        let later = Some(clock::now() + chrono::TimeDelta::hours(1));
        sender
            .send(Event {
                run_at: later,
                ..event(Ty::Asset)
            })
            .unwrap();
        let broker = lock(&rannoch.broker);
        let heap_of = |name: &str| {
            let lane = broker.lanes.iter().find(|l| l.name == name).unwrap();
            lane.heap.iter().map(|e| e.ty).collect::<Vec<_>>()
        };
        assert_eq!(heap_of("assets"), [Ty::Asset]);
        assert_eq!(heap_of("default"), [Ty::Page]);
        assert_eq!(heap_of("scheduled"), [Ty::Asset]);
    }

    #[tokio::test(start_paused = true)]
    async fn fr_rannoch_003_c3() {
        let twice = lanes::<Ty, &str>(assets(&["assets", "large"]));
        assert_eq!(twice.unwrap_err(), Error::Config);
        let mut unknown = assets(&["assets"]);
        unknown.insert(
            "other".to_owned(),
            LaneSettings {
                tys: vec![9],
                ..LaneSettings::default()
            },
        );
        assert_eq!(lanes::<Ty, &str>(unknown).unwrap_err(), Error::Config);
    }

    #[tokio::test(start_paused = true)]
    async fn a_default_or_scheduled_section_sets_that_lane() {
        let made = lanes::<Ty, &str>(assets(&["default", "scheduled"]));
        assert_eq!(made.unwrap_err(), Error::Config, "ty 0 in both");
        let mut sections = assets(&["default"]);
        sections.insert("scheduled".to_owned(), LaneSettings::default());
        let made = lanes::<Ty, &str>(sections).unwrap();
        let names: Vec<&str> = made.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, ["default", "scheduled"]);
        assert_eq!(made[0].heartbeat, Duration::from_millis(500));
    }

    #[tokio::test(start_paused = true)]
    async fn fr_rannoch_004_c1() {
        let rannoch = Rannoch::open(Config::default()).unwrap();
        let answer = rannoch.sender().send(event(Ty::Page)).unwrap();
        assert_eq!(answer as i8, 0);
        let broker = lock(&rannoch.broker);
        let accepted = &broker.lanes[0].heap[0];
        assert_eq!(accepted.id.get_version_num(), 7);
        assert_eq!(accepted.status, Status::Added);
    }

    #[tokio::test(start_paused = true)]
    async fn fr_rannoch_004_c2() {
        let rannoch = Rannoch::open(Config::default()).unwrap();
        let sender = rannoch.sender().clone();
        tokio::task::spawn_blocking(move || rannoch.shutdown())
            .await
            .unwrap();
        assert_eq!(sender.send(event(Ty::Page)).unwrap_err(), Error::Closed);
    }

    /// An append that fails while `failing` is set, keeping each batch's
    /// `ty`s it received.
    #[derive(Debug, Default)]
    struct Recorder {
        failing: Mutex<bool>,
        batches: Mutex<Vec<Vec<Ty>>>,
    }

    impl Append<Ty, &'static str> for Recorder {
        fn append(
            &self,
            batch: &[Event<Ty, &'static str>],
        ) -> Result<Vec<Event<Ty, &'static str>>, Error> {
            if *self.failing.lock().unwrap() {
                return Err(Error::Store);
            }
            let tys = batch.iter().map(|e| e.ty).collect();
            self.batches.lock().unwrap().push(tys);
            Ok(Vec::new())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_hands_over_what_is_left_on_each_heap() {
        let recorder = Arc::new(Recorder::default());
        let config = Config {
            append: Some(recorder.clone()),
            ..Config::default()
        };
        let rannoch = Rannoch::open(config).unwrap();
        let later = Some(clock::now() + chrono::TimeDelta::hours(1));
        rannoch
            .sender()
            .send(Event {
                run_at: later,
                ..event(Ty::Asset)
            })
            .unwrap();
        rannoch.sender().send(event(Ty::Page)).unwrap();

        tokio::task::spawn_blocking(move || rannoch.shutdown())
            .await
            .unwrap();

        let mut batches = recorder.batches.lock().unwrap().clone();
        batches.sort_by_key(|b| b[0] as u8);
        assert_eq!(batches, [vec![Ty::Asset], vec![Ty::Page]]);
    }

    #[tokio::test(start_paused = true)]
    async fn rannoch_holding_its_store_is_send_and_sync() {
        fn send_and_sync<S: Send + Sync>() {}
        send_and_sync::<Rannoch<Ty, &str>>();
    }
}
