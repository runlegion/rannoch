//! NFR-RANNOCH-030: the events a second the inbox accepts from one producer, the commit rate
//! (events durable in `_events` a second) and the send-to-commit p99.
//!
//! On demand, not in CI: `cargo bench -p rannoch-core --bench inbox`. It opens rannoch under
//! `Config::default()` on a real-time multi-thread tokio runtime, over rannoch-events' append to
//! the app's forgr empty store, sends 1,000,000 events from one producer as fast as it can, and
//! times everything with `clock::elapsed`.

use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use rannoch_core::{Append, Config, Event, Rannoch, Status, clock};
use rannoch_events::Events;
use tokio::sync::oneshot;
use tokio::time::Instant;
use uuid::Uuid;

/// How many events the producer sends.
const EVENTS: usize = 1_000_000;

/// When each event, by the index its data carries, was committed, and how many have been.
#[derive(Debug)]
struct Commits {
    at: Vec<Option<Instant>>,
    count: usize,
    done: Option<oneshot::Sender<()>>,
}

/// rannoch-events' append, noting the time each batch it commits was committed.
#[derive(Debug)]
struct Timed {
    events: Events,
    commits: Mutex<Commits>,
}

impl Append<u8, String> for Timed {
    fn append(
        &self,
        batch: &[Event<u8, String>],
    ) -> Result<Vec<Event<u8, String>>, rannoch_core::Error> {
        let replies = self.events.append(batch)?;
        let now = clock::elapsed();
        let mut commits = self.commits.lock().unwrap_or_else(PoisonError::into_inner);
        for event in batch {
            if let Ok(index) = event.data.parse::<usize>()
                && let Some(slot @ None) = commits.at.get_mut(index)
            {
                *slot = Some(now);
                commits.count += 1;
            }
        }
        if commits.count == EVENTS
            && let Some(done) = commits.done.take()
        {
            let _ = done.send(());
        }
        Ok(replies)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(bench())
}

async fn bench() -> Result<(), Box<dyn Error>> {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("inbox_bench");
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let path = dir.join("store.db");
    rannoch_core::forgr::empty_store().execute("VACUUM INTO ?1", [path.to_string_lossy()])?;

    let (queue, _signals) = tokio::sync::mpsc::unbounded_channel();
    let (done, finished) = oneshot::channel();
    let timed = Arc::new(Timed {
        events: Events::new(rusqlite::Connection::open(&path)?, queue),
        commits: Mutex::new(Commits {
            at: vec![None; EVENTS],
            count: 0,
            done: Some(done),
        }),
    });
    let rannoch = Rannoch::open(Config {
        append: Some(Arc::clone(&timed) as Arc<dyn Append<u8, String>>),
        ..Config::default()
    })?;
    let sender = rannoch.sender();
    let events: Vec<Event<u8, String>> = (0..EVENTS)
        .map(|i| Event {
            id: Uuid::nil(),
            source: None,
            ty: 0,
            data: i.to_string(),
            run_at: None,
            status: Status::Added,
        })
        .collect();

    let mut sent = Vec::with_capacity(EVENTS);
    let start = clock::elapsed();
    for event in events {
        sender.send(event)?;
        sent.push(clock::elapsed());
    }
    let accepted = clock::elapsed() - start;
    finished.await?;
    tokio::task::spawn_blocking(move || rannoch.shutdown()).await?;

    let commits = timed.commits.lock().unwrap_or_else(PoisonError::into_inner);
    let at: Vec<Instant> = commits.at.iter().flatten().copied().collect();
    let last = at.iter().max().copied().unwrap_or(start);
    let mut latencies: Vec<_> = at.iter().zip(&sent).map(|(c, s)| *c - *s).collect();
    latencies.sort_unstable();
    let rate = |span: std::time::Duration| {
        f64::from(u32::try_from(EVENTS).unwrap_or(u32::MAX)) / span.as_secs_f64()
    };
    println!("events sent: {EVENTS}, from one producer");
    println!("accepted: {:.0} events/s ({accepted:?})", rate(accepted));
    println!(
        "committed: {:.0} events/s ({:?})",
        rate(last - start),
        last - start
    );
    let p99 = latencies[(latencies.len() * 99).div_ceil(100) - 1];
    println!("send-to-commit p99: {p99:?}");
    drop(commits);
    fs::remove_dir_all(&dir)?;
    Ok(())
}
