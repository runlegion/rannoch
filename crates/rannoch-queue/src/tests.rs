use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rannoch_store::{Class, Declaration, Error, Graph, open};

use super::{Failure, Queue, State, Ticket, lock};

const KEY: [u8; 32] = [7; 32];
const HOUR: Duration = Duration::from_secs(3_600);

/// A graph at `path` with class `item`, property `v`.
fn graph(path: &Path) -> Result<Graph, Error> {
    let mut g = open(path, &KEY)?;
    g.declare(
        &Declaration::Class(Class::new("item", ["v"])),
        r#"{"specversion":"1.0","id":"d-item","source":"rannoch:app","type":"rannoch.declared.item"}"#,
    )?;
    Ok(g)
}

fn path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("graph.rannoch")
}

/// The `CloudEvent` of node `key` of `class`.
fn node(class: &str, key: &str) -> String {
    format!(
        r#"{{"specversion":"1.0","id":"{}:0","source":"rannoch:app","type":"rannoch.node.{class}","subject":"{key}","data":{{"properties":{{"v":"{key}"}},"valid":"unknown"}}}}"#,
        rannoch_cloudevents::EventId::now()
    )
}

fn item(key: &str) -> String {
    node("item", key)
}

/// Makes every event whose key is `bad` fail at commit: with `ABORT` it rolls back alone, with
/// `ROLLBACK` it takes its whole transaction.
fn failing_bad(path: &Path, how: &str) -> rusqlite::Result<()> {
    rusqlite::Connection::open(path)?.execute_batch(&format!(
        "CREATE TRIGGER bad_fails BEFORE INSERT ON _events WHEN NEW.body LIKE '%\"subject\":\"bad\"%' \
         BEGIN SELECT RAISE({how}, 'database or disk is full'); END;"
    ))
}

/// Waits up to five seconds for `done`.
fn wait_for(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(Instant::now() < deadline, "timed out");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The tickets of `failures`, each an event that failed at commit.
fn tickets(failures: Vec<Failure>) -> Vec<Ticket> {
    failures
        .into_iter()
        .map(|failure| match failure {
            Failure::Event { ticket, .. } => ticket,
            other => panic!("expected a failed event, got {other:?}"),
        })
        .collect()
}

#[test]
fn a_refused_event_is_refused_at_once_with_the_reason() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    let refused = queue.accept(node("tweet", "t1"));
    assert!(
        matches!(&refused, Err(Error::Refused(why)) if why.contains("tweet")),
        "{refused:?}"
    );
    let declared = queue.accept(
        r#"{"specversion":"1.0","id":"d","source":"rannoch:app","type":"rannoch.declared.book"}"#,
    );
    assert!(matches!(declared, Err(Error::Refused(_))), "{declared:?}");
    assert_eq!(queue.flush().0, None, "a refused event was held");
    Ok(())
}

#[test]
fn an_accepted_event_returns_before_it_is_committed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    let ticket = queue.accept(item("i1"))?;
    assert!(queue.committed() < Some(ticket));
    let declared = queue.next("w")?.ok_or("the declaration is committed")?;
    assert!(declared.body.contains("rannoch.declared.item"));
    assert_eq!(
        queue.next("w")?,
        None,
        "an uncommitted event was handed out"
    );
    Ok(())
}

#[test]
fn the_heartbeat_commits_and_fills_then_tells_subscribers() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, Queue::INTERVAL)?;
    let heard = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&heard);
    queue.subscribe(move |cursor| lock(&into).push(cursor));
    let ticket = queue.accept(item("i1"))?;
    wait_for(|| !lock(&heard).is_empty());
    assert!(queue.committed() >= Some(ticket));
    let reader = open(path(&dir), &KEY)?;
    let watermark = lock(&heard)[0];
    assert!(watermark <= reader.events().committed()?);
    assert!(
        reader.find("item", "i1")?.is_some(),
        "the tables were not filled"
    );
    let handed = queue.next("w")?.ok_or("nothing handed out")?;
    assert!(handed.cursor <= watermark);
    Ok(())
}

#[test]
fn one_failing_event_rolls_back_alone_and_is_reported_once()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let g = graph(&path(&dir))?;
    failing_bad(&path(&dir), "ABORT")?;
    let queue = Queue::open(g, HOUR)?;
    queue.accept(item("i1"))?;
    let bad = queue.accept(item("bad"))?;
    queue.accept(item("i3"))?;
    let (_, failed) = queue.flush();
    assert_eq!(tickets(failed), [bad]);
    assert!(
        queue.take_failures().is_empty(),
        "a failure was reported twice"
    );
    let reader = open(path(&dir), &KEY)?;
    assert!(reader.find("item", "i1")?.is_some() && reader.find("item", "i3")?.is_some());
    assert!(reader.find("item", "bad")?.is_none());
    Ok(())
}

#[test]
fn a_position_commits_with_its_events_and_reads_back_on_reopening()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    queue.accept_from(item("i1"), "feed", "line 1")?;
    queue.accept_from(item("i2"), "feed", "line 2")?;
    assert_eq!(
        queue.position("feed")?,
        None,
        "a position committed before its events"
    );
    drop(queue);
    let queue = Queue::open(open(path(&dir), &KEY)?, HOUR)?;
    assert_eq!(queue.position("feed")?.as_deref(), Some("line 2"));
    Ok(())
}

#[test]
fn a_position_does_not_land_without_its_events() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let g = graph(&path(&dir))?;
    failing_bad(&path(&dir), "ROLLBACK")?;
    let queue = Queue::open(g, HOUR)?;
    let first = queue.accept_from(item("i1"), "feed", "line 1")?;
    let bad = queue.accept_from(item("bad"), "feed", "line 2")?;
    assert_eq!(tickets(queue.close()), [first, bad]);
    assert_eq!(open(path(&dir), &KEY)?.position("feed")?, None);
    Ok(())
}

#[test]
fn dropping_the_queue_commits_what_it_holds() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    queue.accept(item("i1"))?;
    drop(queue);
    assert!(open(path(&dir), &KEY)?.find("item", "i1")?.is_some());
    Ok(())
}

#[test]
fn a_drop_hands_untaken_failures_to_the_handler() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let g = graph(&path(&dir))?;
    failing_bad(&path(&dir), "ABORT")?;
    let queue = Queue::open(g, HOUR)?;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&handled);
    queue.on_failure(move |failure| lock(&into).push(failure));
    let bad = queue.accept(item("bad"))?;
    drop(queue);
    assert_eq!(tickets(std::mem::take(&mut *lock(&handled))), [bad]);
    Ok(())
}

#[test]
fn every_committed_event_has_a_state_for_each_worker_that_survives_reopening()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    for key in ["i1", "i2", "i3", "i4"] {
        queue.accept(item(key))?;
    }
    let _ = queue.flush();
    let mut handed = Vec::new();
    for _ in 0..3 {
        handed.push(queue.next("w")?.ok_or("nothing handed out")?.cursor);
    }
    queue.done("w", handed[0])?;
    queue.failed("w", handed[1], "no such book")?;
    drop(queue);
    let queue = Queue::open(open(path(&dir), &KEY)?, HOUR)?;
    assert_eq!(queue.state("w", handed[0])?, State::Done);
    assert_eq!(
        queue.state("w", handed[1])?,
        State::Failed("no such book".to_string())
    );
    assert_eq!(queue.state("w", handed[2])?, State::InProgress);
    let fourth = queue.next("w")?.ok_or("the fourth is waiting")?.cursor;
    assert!(fourth > handed[2]);
    assert_eq!(queue.state("other", handed[0])?, State::Waiting);
    Ok(())
}

#[test]
fn a_subscriber_panic_is_reported_and_the_others_are_told() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    let heard = Arc::new(Mutex::new(0));
    let into = Arc::clone(&heard);
    queue.subscribe(|_| panic!("a subscriber panics"));
    queue.subscribe(move |_| *lock(&into) += 1);
    let ticket = queue.accept(item("i1"))?;
    let (_, failed) = queue.flush();
    assert!(
        matches!(&failed[..], [Failure::Subscriber { ticket: t, message }]
            if *t == ticket && message == "a subscriber panics"),
        "{failed:?}"
    );
    assert_eq!(*lock(&heard), 1);
    Ok(())
}

#[test]
fn a_batch_taken_by_a_panicking_heartbeat_is_kept() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, Queue::INTERVAL)?;
    queue
        .shared
        .panic_beat
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let ticket = queue.accept_from(item("i1"), "feed", "line 1")?;
    let mut taken = Vec::new();
    wait_for(|| {
        taken.extend(queue.take_failures());
        !taken.is_empty()
    });
    assert!(
        matches!(&taken[..], [Failure::Heartbeat(m)] if m == "the heartbeat panics"),
        "{taken:?}"
    );
    let (committed, failed) = queue.flush();
    assert_eq!(committed, Some(ticket));
    assert!(failed.is_empty(), "{failed:?}");
    assert_eq!(queue.position("feed")?.as_deref(), Some("line 1"));
    Ok(())
}

#[test]
fn a_producer_resumes_at_its_failed_event_without_loss_or_doubling()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let g = graph(&path(&dir))?;
    failing_bad(&path(&dir), "ABORT")?;
    let queue = Queue::open(g, HOUR)?;
    queue.accept_from(item("i1"), "feed", "1")?;
    let bad = queue.accept_from(item("bad"), "feed", "2")?;
    let after = queue.accept_from(item("i3"), "feed", "3")?;
    let (_, failed) = queue.flush();
    assert_eq!(tickets(failed), [bad, after]);
    assert_eq!(queue.position("feed")?.as_deref(), Some("1"));
    // Resuming after position 1 sends i3 again; it was not committed before, so it lands once.
    queue.accept_from(item("i3"), "feed", "3")?;
    let (_, failed) = queue.flush();
    assert!(failed.is_empty(), "{failed:?}");
    assert_eq!(queue.position("feed")?.as_deref(), Some("3"));
    let reader = open(path(&dir), &KEY)?;
    let i3 = reader
        .events()
        .since(rannoch_store::Cursor::START)?
        .iter()
        .filter(|e| e.body.contains("\"subject\":\"i3\""))
        .count();
    assert_eq!(i3, 1, "i3 was doubled or lost");
    Ok(())
}

#[test]
fn a_subscriber_can_take_its_next_event_from_the_queue() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Arc::new(Queue::open(graph(&path(&dir))?, Queue::INTERVAL)?);
    let taken = Arc::new(Mutex::new(Vec::new()));
    let (from, into) = (Arc::downgrade(&queue), Arc::clone(&taken));
    queue.subscribe(move |_| {
        if let Some(queue) = from.upgrade()
            && let Ok(Some(event)) = queue.next("w")
        {
            lock(&into).push(event.cursor);
        }
    });
    queue.accept(item("i1"))?;
    wait_for(|| !lock(&taken).is_empty());
    Ok(())
}

#[test]
fn opening_the_queue_fills_what_the_log_holds_past_the_tables()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let mut g = graph(&path(&dir))?;
    g.commit(&[&item("i1")], &[])?;
    assert!(
        g.find("item", "i1")?.is_none(),
        "committed but not yet filled"
    );
    let queue = Queue::open(g, HOUR)?;
    drop(queue);
    assert!(open(path(&dir), &KEY)?.find("item", "i1")?.is_some());
    Ok(())
}
