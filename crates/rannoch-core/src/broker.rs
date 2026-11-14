//! The configuration, open, and the broker that passes each event to its inbox.

use chrono::TimeDelta;
use std::fmt;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::inbox::{Inbox, run};
use crate::{Error, Event, Status};

/// What core needs from the app's store at start.
pub trait Storage: fmt::Debug {
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

/// What core needs from the events crate: the append each inbox hands its
/// batch to.
pub trait Append<T, D>: fmt::Debug + Send + Sync {
    /// Appends `batch` as one write.
    ///
    /// # Errors
    ///
    /// `Store` when the write fails.
    fn append(&self, batch: &[Event<T, D>]) -> Result<(), Error>;
}

/// The app's configuration: its inboxes, the default inbox's batch interval,
/// the app's store and the events crate's append.
#[derive(Debug, Clone)]
pub struct Config<T, D> {
    /// The default inbox's batch interval; every type not listed goes there.
    pub default_interval: TimeDelta,
    pub inboxes: Vec<InboxConfig<T>>,
    /// The app's store; with none, open runs no migration check.
    pub store: Option<Arc<dyn Storage>>,
    /// The events crate's append; with none, no inbox hands anything over.
    pub append: Option<Arc<dyn Append<T, D>>>,
}

impl<T, D> Default for Config<T, D> {
    fn default() -> Self {
        Self {
            default_interval: TimeDelta::milliseconds(20),
            inboxes: Vec::new(),
            store: None,
            append: None,
        }
    }
}

/// One inbox: its name, the `ty` values that go to it, and its batch interval.
#[derive(Debug, Clone)]
pub struct InboxConfig<T> {
    pub name: String,
    pub types: Vec<T>,
    pub interval: TimeDelta,
}

#[derive(Debug)]
pub(crate) struct Broker<T, D> {
    /// The default inbox first, then the configured inboxes.
    pub(crate) inboxes: Vec<Inbox<T, D>>,
    pub(crate) closed: bool,
}

pub(crate) type Shared<T, D> = Arc<Mutex<Broker<T, D>>>;

pub(crate) fn lock<T, D>(broker: &Shared<T, D>) -> MutexGuard<'_, Broker<T, D>> {
    broker.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The owner's handle: the only value that can shut rannoch down.
#[derive(Debug)]
pub struct Rannoch<T, D> {
    broker: Shared<T, D>,
    /// Dropping these wakes each inbox's thread for its last hand-over.
    stops: Vec<mpsc::Sender<()>>,
    threads: Vec<JoinHandle<()>>,
}

impl<T: PartialEq + Send + 'static, D: Send + 'static> Rannoch<T, D> {
    /// Opens rannoch with a configuration. Before anything else, applies the
    /// store's pending migrations.
    ///
    /// # Errors
    ///
    /// `Store` when the store cannot tell what is pending or applying fails;
    /// `Config` when the same `ty` is listed in two inboxes, or an interval
    /// is negative.
    pub fn open(config: Config<T, D>) -> Result<Self, Error> {
        if let Some(store) = &config.store
            && store.pending().map_err(|_| Error::Store)?
        {
            store.apply().map_err(|_| Error::Store)?;
            log::debug!("applied pending migrations");
        }
        let configured = &config.inboxes;
        for (i, a) in configured.iter().enumerate() {
            if configured[i + 1..]
                .iter()
                .any(|b| a.types.iter().any(|t| b.types.contains(t)))
            {
                return Err(Error::Config);
            }
        }
        let mut inboxes = vec![Inbox::new(
            "default".to_owned(),
            Vec::new(),
            config.default_interval,
        )];
        inboxes.extend(
            config
                .inboxes
                .into_iter()
                .map(|c| Inbox::new(c.name, c.types, c.interval)),
        );
        let intervals = inboxes
            .iter()
            .map(|inbox| inbox.interval.to_std().map_err(|_| Error::Config))
            .collect::<Result<Vec<Duration>, Error>>()?;
        log::debug!("opened with {} inboxes", inboxes.len());
        let broker = Arc::new(Mutex::new(Broker {
            inboxes,
            closed: false,
        }));
        let (mut stops, mut threads) = (Vec::new(), Vec::new());
        for (index, every) in intervals.into_iter().enumerate() {
            let Some(append) = config.append.clone() else {
                break;
            };
            let (stop, wait) = mpsc::channel::<()>();
            let broker = Arc::clone(&broker);
            stops.push(stop);
            threads.push(thread::spawn(move || {
                run(&broker, index, every, &wait, &*append);
            }));
        }
        Ok(Self {
            broker,
            stops,
            threads,
        })
    }

    /// A sender for a producer.
    #[must_use]
    pub fn sender(&self) -> Sender<T, D> {
        Sender {
            broker: Arc::clone(&self.broker),
        }
    }

    /// Shuts rannoch down: every send after this fails with `Closed`, each
    /// inbox hands over what is left on its heap, and its thread is joined.
    pub fn shutdown(self) {
        lock(&self.broker).closed = true;
        drop(self.stops);
        for thread in self.threads {
            let _ = thread.join();
        }
    }
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
    /// Sends an event to the broker, which passes it to the inbox for its `ty`.
    ///
    /// # Errors
    ///
    /// `Closed` when rannoch is shutting down.
    pub fn send(&self, event: Event<T, D>) -> Result<Status, Error> {
        let mut broker = lock(&self.broker);
        if broker.closed {
            return Err(Error::Closed);
        }
        let index = broker
            .inboxes
            .iter()
            .position(|inbox| inbox.types.contains(&event.ty))
            .unwrap_or(0);
        Ok(broker.inboxes[index].accept(event))
    }
}

#[cfg(test)]
mod tests {
    use super::{Append, Config, InboxConfig, Rannoch, Storage, lock};
    use crate::{Error, Event, Status};
    use chrono::TimeDelta;
    use std::sync::{Arc, Mutex};
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

    #[test]
    fn pending_migrations_are_applied_once_then_open_succeeds() {
        assert_eq!(open_with(Ok(true), Ok(())), (Ok(()), 1));
    }

    #[test]
    fn nothing_pending_applies_nothing() {
        assert_eq!(open_with(Ok(false), Ok(())), (Ok(()), 0));
    }

    #[test]
    fn a_failed_pending_check_fails_open_with_store_and_applies_nothing() {
        assert_eq!(open_with(Err(Error::Queue), Ok(())), (Err(Error::Store), 0));
    }

    #[test]
    fn a_failed_apply_fails_open_with_store() {
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

    fn assets(name: &str) -> InboxConfig<Ty> {
        InboxConfig {
            name: name.to_owned(),
            types: vec![Ty::Asset],
            interval: TimeDelta::milliseconds(500),
        }
    }

    #[test]
    fn fr_rannoch_003_c1() {
        let rannoch = Rannoch::<Ty, &str>::open(Config::default()).unwrap();
        let broker = lock(&rannoch.broker);
        assert_eq!(broker.inboxes.len(), 1);
        assert_eq!(broker.inboxes[0].interval, TimeDelta::milliseconds(20));
    }

    #[test]
    fn fr_rannoch_003_c2() {
        let config = Config {
            inboxes: vec![assets("assets")],
            ..Config::default()
        };
        let rannoch = Rannoch::open(config).unwrap();
        let sender = rannoch.sender();
        sender.send(event(Ty::Asset)).unwrap();
        sender.send(event(Ty::Page)).unwrap();
        let broker = lock(&rannoch.broker);
        let heap_of = |name: &str| {
            let inbox = broker.inboxes.iter().find(|i| i.name == name).unwrap();
            inbox.heap.iter().map(|e| e.ty).collect::<Vec<_>>()
        };
        assert_eq!(heap_of("assets"), [Ty::Asset]);
        assert_eq!(heap_of("default"), [Ty::Page]);
    }

    #[test]
    fn fr_rannoch_003_c3() {
        let config = Config {
            inboxes: vec![assets("assets"), assets("large")],
            ..Config::default()
        };
        assert_eq!(
            Rannoch::<Ty, &str>::open(config).unwrap_err(),
            Error::Config
        );
    }

    #[test]
    fn fr_rannoch_004_c1() {
        let rannoch = Rannoch::open(Config::default()).unwrap();
        let answer = rannoch.sender().send(event(Ty::Page)).unwrap();
        assert_eq!(answer as i8, 0);
        let broker = lock(&rannoch.broker);
        let accepted = &broker.inboxes[0].heap[0];
        assert_eq!(accepted.id.get_version_num(), 7);
        assert_eq!(accepted.status, Status::Added);
    }

    #[test]
    fn fr_rannoch_004_c2() {
        let rannoch = Rannoch::open(Config::default()).unwrap();
        let sender = rannoch.sender().clone();
        rannoch.shutdown();
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
        fn append(&self, batch: &[Event<Ty, &'static str>]) -> Result<(), Error> {
            if *self.failing.lock().unwrap() {
                return Err(Error::Store);
            }
            let tys = batch.iter().map(|e| e.ty).collect();
            self.batches.lock().unwrap().push(tys);
            Ok(())
        }
    }

    #[test]
    fn shutdown_hands_over_what_is_left_on_each_heap() {
        let recorder = Arc::new(Recorder::default());
        let config = Config {
            default_interval: TimeDelta::hours(1),
            inboxes: vec![assets("assets")],
            append: Some(recorder.clone()),
            ..Config::default()
        };
        let rannoch = Rannoch::open(config).unwrap();
        rannoch.sender().send(event(Ty::Asset)).unwrap();
        rannoch.sender().send(event(Ty::Page)).unwrap();

        rannoch.shutdown();

        let mut batches = recorder.batches.lock().unwrap().clone();
        batches.sort_by_key(|b| b[0] as u8);
        assert_eq!(batches, [vec![Ty::Asset], vec![Ty::Page]]);
    }

    #[test]
    fn a_negative_interval_fails_open_with_config() {
        let config = Config::<Ty, &str> {
            default_interval: TimeDelta::milliseconds(-1),
            ..Config::default()
        };
        assert_eq!(Rannoch::open(config).unwrap_err(), Error::Config);
    }
}
