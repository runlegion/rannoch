//! The app: opens its store with the migrator its build gathered, runs rannoch over it with the
//! processor's writes, and sends one payment, whose flow's `store` step writes its row into
//! onto-pay's payments table with the payment's 2. Its one argument is the store's path.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use rannoch_core::{Config, Event, Rannoch, Status, Storage};
use rannoch_events::Events;
use rannoch_processor::Processor;
use rannoch_queue::{Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Value, json};
use uuid::Uuid;

const FLOWS: &str = include_str!(concat!(env!("OUT_DIR"), "/flows.json"));

/// Every crate's migrations, as the app's build gathered them into `migrations/`.
fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}

/// A connection to the migrated store at `path`.
fn ready(path: &Path) -> Result<Connection, Box<dyn Error>> {
    match rannoch_store::open(path, &migrator())? {
        Opened::Ready(conn) => Ok(conn),
        Opened::Pending(versions) => Err(format!("migrations pending: {versions:?}").into()),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = PathBuf::from(std::env::args().nth(1).ok_or("no store path")?);
    let store = Arc::new(Store::new(path.clone(), migrator()));
    store.apply()?;
    let processor = Arc::new(Processor::<u16, Value>::new(
        FLOWS,
        Vec::new(),
        ready(&path)?,
    )?);
    let (signal, wake) = mpsc::channel();
    let append = Events::new(ready(&path)?, signal).with_writes(processor.writes());
    let rannoch = Rannoch::<u16, Value>::open(Config {
        store: Some(store),
        append: Some(Arc::new(append)),
        ..Config::default()
    })?;
    let settings = Settings::new(Duration::from_millis(100));
    let queue = Queue::start(ready(&path)?, wake, rannoch.sender(), processor, settings);

    let payment = Event {
        id: Uuid::nil(),
        source: None,
        ty: 1,
        data: json!({ "order": "o1", "amount": 10 }),
        run_at: None,
        status: Status::Added,
    };
    rannoch.sender().send(payment)?;
    let (reads, start) = (ready(&path)?, Instant::now());
    while !rannoch_queue::report(&reads)?
        .iter()
        .any(|(status, _)| *status == Status::Processed)
    {
        if start.elapsed() > Duration::from_secs(10) {
            return Err("the payment has no 2".into());
        }
        thread::sleep(Duration::from_millis(20));
    }
    rannoch.shutdown();
    queue.join()?;
    Ok(())
}
