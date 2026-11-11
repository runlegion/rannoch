//! The configuration, open, and the broker that passes each event to its inbox.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::inbox::Inbox;
use crate::{Error, Event, Status};

/// The app's configuration: its inboxes, and the default inbox's batch interval.
#[derive(Debug, Clone)]
pub struct Config<T> {
    /// The default inbox's batch interval; every type not listed goes there.
    pub default_interval: Duration,
    pub inboxes: Vec<InboxConfig<T>>,
}

impl<T> Default for Config<T> {
    fn default() -> Self {
        Self {
            default_interval: Duration::from_millis(20),
            inboxes: Vec::new(),
        }
    }
}

/// One inbox: its name, the `ty` values that go to it, and its batch interval.
#[derive(Debug, Clone)]
pub struct InboxConfig<T> {
    pub name: String,
    pub types: Vec<T>,
    pub interval: Duration,
}

#[derive(Debug)]
struct Broker<T, D> {
    /// The default inbox first, then the configured inboxes.
    inboxes: Vec<Inbox<T, D>>,
    closed: bool,
}

type Shared<T, D> = Arc<Mutex<Broker<T, D>>>;

fn lock<T, D>(broker: &Shared<T, D>) -> MutexGuard<'_, Broker<T, D>> {
    broker.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The owner's handle: the only value that can shut rannoch down.
#[derive(Debug)]
pub struct Rannoch<T, D> {
    broker: Shared<T, D>,
}

impl<T: PartialEq, D> Rannoch<T, D> {
    /// Opens rannoch with a configuration.
    ///
    /// # Errors
    ///
    /// `Config` when the same `ty` is listed in two inboxes.
    pub fn open(config: Config<T>) -> Result<Self, Error> {
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
        let broker = Broker {
            inboxes,
            closed: false,
        };
        Ok(Self {
            broker: Arc::new(Mutex::new(broker)),
        })
    }

    /// A sender for a producer.
    #[must_use]
    pub fn sender(&self) -> Sender<T, D> {
        Sender {
            broker: Arc::clone(&self.broker),
        }
    }

    /// Shuts rannoch down; every send after this fails with `Closed`.
    pub fn shutdown(self) {
        lock(&self.broker).closed = true;
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
    use super::{Config, InboxConfig, Rannoch, lock};
    use crate::{Error, Event, Status};
    use std::time::Duration;
    use uuid::Uuid;

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
            interval: Duration::from_millis(500),
        }
    }

    #[test]
    fn fr_rannoch_003_c1() {
        let rannoch = Rannoch::<Ty, &str>::open(Config::default()).unwrap();
        let broker = lock(&rannoch.broker);
        assert_eq!(broker.inboxes.len(), 1);
        assert_eq!(broker.inboxes[0].interval, Duration::from_millis(20));
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
}
