use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rannoch_store::{Class, Declaration, Error, Graph, Relation, open};

use super::{CHUNK, Failure, Queue, State, Ticket, lock};

const KEY: [u8; 32] = [7; 32];
const HOUR: Duration = Duration::from_secs(3_600);
/// How long a test waits before it calls itself hung and fails.
const HUNG: Duration = Duration::from_secs(120);

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

/// Waits for `done`, and fails only when the test is hung.
fn wait_for(mut done: impl FnMut() -> bool) -> Result<(), Box<dyn std::error::Error>> {
    let hung = Instant::now() + HUNG;
    while !done() {
        if Instant::now() > hung {
            return Err("hung".into());
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(())
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
    wait_for(|| !lock(&heard).is_empty())?;
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
fn every_closed_event_has_a_state_for_each_worker_that_survives_reopening()
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
    assert_eq!(
        queue.state("w", handed[2])?,
        State::Waiting,
        "what the earlier run left open is after its cursor"
    );
    let back = queue.next("w")?.ok_or("the third comes back")?.cursor;
    assert_eq!(
        back, handed[2],
        "what the earlier run left in progress is handed back first"
    );
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
    })?;
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
    wait_for(|| !lock(&taken).is_empty())?;
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

#[test]
fn a_closed_link_event_is_handed_out_by_the_commit_that_filled_it()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let mut g = graph(&path(&dir))?;
    g.declare(
        &Declaration::Class(Class::new("box", ["v"])),
        r#"{"specversion":"1.0","id":"d-box","source":"rannoch:app","type":"rannoch.declared.box"}"#,
    )?;
    g.declare(
        &Declaration::Relation(Relation::new("holds", "box", "item", false)),
        r#"{"specversion":"1.0","id":"d-holds","source":"rannoch:app","type":"rannoch.declared.holds"}"#,
    )?;
    let queue = Queue::open(g, HOUR)?;
    queue.accept(node("box", "b1"))?;
    queue.accept(format!(
        r#"{{"specversion":"1.0","id":"{}:0","source":"rannoch:app","type":"rannoch.link.holds","subject":"b1","data":{{"from":{{"class":"box","key":"b1"}},"to":{{"class":"item","key":"i1"}},"valid":"unknown"}}}}"#,
        rannoch_cloudevents::EventId::now()
    ))?;
    let (_, failed) = queue.flush();
    assert!(failed.is_empty(), "{failed:?}");
    while let Some(event) = queue.next("w")? {
        queue.done("w", event.cursor)?;
    }
    queue.accept(item("i1"))?;
    let (_, failed) = queue.flush();
    assert!(failed.is_empty(), "{failed:?}");
    let mut handed = Vec::new();
    while let Some(event) = queue.next("w")? {
        queue.done("w", event.cursor)?;
        handed.push(event.body);
    }
    assert!(
        handed
            .iter()
            .any(|body| body.contains("rannoch.closed.holds")),
        "the closed event is handed out with no later commit: {handed:?}"
    );
    Ok(())
}

/// A queue holding `count` committed items after the declaration, and the graph's path.
fn committed(dir: &tempfile::TempDir, count: usize) -> Result<Queue, Box<dyn std::error::Error>> {
    let queue = Queue::open(graph(&path(dir))?, HOUR)?;
    for n in 0..count {
        queue.accept(item(&format!("i{n}")))?;
    }
    let (_, failed) = queue.flush();
    assert!(failed.is_empty(), "{failed:?}");
    Ok(queue)
}

#[test]
fn taking_and_closing_write_nothing_until_a_group_commit() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let queue = committed(&dir, 2)?;
    let first = queue.next("w")?.ok_or("first")?.cursor;
    let second = queue.next("w")?.ok_or("second")?.cursor;
    queue.done("w", first)?;
    queue.failed("w", second, "no such book")?;
    let reader = open(path(&dir), &KEY)?;
    assert_eq!(
        reader.state("w", first)?,
        State::Waiting,
        "a close reached disk"
    );
    assert_eq!(reader.state("w", second)?, State::Waiting);
    assert_eq!(queue.state("w", first)?, State::Done, "memory knows");
    assert_eq!(
        queue.state("w", second)?,
        State::Failed("no such book".to_string())
    );
    let _ = queue.flush();
    assert_eq!(reader.state("w", first)?, State::Done);
    assert_eq!(
        reader.state("w", second)?,
        State::Failed("no such book".to_string()),
        "the failure is recorded with its reason"
    );
    Ok(())
}

#[test]
fn a_heartbeat_with_only_worker_changes_commits_them() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    drop(committed(&dir, 1)?);
    let queue = Queue::open(open(path(&dir), &KEY)?, Queue::INTERVAL)?;
    let reader = open(path(&dir), &KEY)?;
    let mut last = None;
    while let Some(event) = queue.next("a")? {
        queue.done("a", event.cursor)?;
        last = Some(event.cursor);
    }
    let last = last.ok_or("nothing handed out")?;
    let other = queue.next("b")?.ok_or("b's first")?.cursor;
    queue.done("b", other)?;
    wait_for(|| {
        reader.state("a", last).ok() == Some(State::Done)
            && reader.state("b", other).ok() == Some(State::Done)
    })?;
    Ok(())
}

#[test]
fn after_a_crash_a_worker_is_handed_again_what_came_after_its_saved_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = committed(&dir, 4)?;
    let first = queue.next("w")?.ok_or("first")?.cursor;
    queue.done("w", first)?;
    let _ = queue.flush();
    let second = queue.next("w")?.ok_or("second")?.cursor;
    queue.done("w", second)?;
    // A crash: nothing runs after this, no drop, no group commit.
    std::mem::forget(queue);
    let queue = Queue::open(open(path(&dir), &KEY)?, HOUR)?;
    assert_eq!(
        queue.next("w")?.map(|e| e.cursor),
        Some(second),
        "the close after the last group commit is handed again"
    );
    Ok(())
}

#[test]
fn a_worker_keeps_state_with_its_cursor_across_a_restart() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let queue = committed(&dir, 1)?;
    let first = queue.next("w")?.ok_or("first")?.cursor;
    let second = queue.next("w")?.ok_or("second")?.cursor;
    queue.done_keeping("w", second, "count=2")?;
    assert_eq!(
        queue.kept("w"),
        None,
        "the cursor has not reached the second while the first is open"
    );
    queue.done_keeping("w", first, "count=1")?;
    assert_eq!(queue.kept("w").as_deref(), Some("count=2"));
    drop(queue);
    let queue = Queue::open(open(path(&dir), &KEY)?, HOUR)?;
    assert_eq!(queue.kept("w").as_deref(), Some("count=2"));
    assert_eq!(queue.next("w")?, None, "the cursor came back with it");
    Ok(())
}

#[test]
fn a_failure_saved_ahead_of_the_cursor_clears_when_handed_again_and_done()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = committed(&dir, 2)?;
    let open_one = queue.next("w")?.ok_or("first")?.cursor;
    let failing = queue.next("w")?.ok_or("second")?.cursor;
    queue.failed("w", failing, "not yet")?;
    let _ = queue.flush();
    std::mem::forget(queue);
    let queue = Queue::open(open(path(&dir), &KEY)?, HOUR)?;
    assert_eq!(queue.next("w")?.map(|e| e.cursor), Some(open_one));
    assert_eq!(queue.next("w")?.map(|e| e.cursor), Some(failing));
    queue.done("w", open_one)?;
    queue.done("w", failing)?;
    drop(queue);
    assert_eq!(open(path(&dir), &KEY)?.state("w", failing)?, State::Done);
    Ok(())
}

#[test]
fn every_worker_s_lag_and_the_slowest_cursor_can_be_read() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let queue = committed(&dir, 3)?;
    assert_eq!(queue.slowest(), None, "no worker yet");
    let total = queue.lag("fast")?;
    assert_eq!(
        total, 4,
        "the declaration and three items are ahead of a new worker"
    );
    while let Some(event) = queue.next("fast")? {
        queue.done("fast", event.cursor)?;
    }
    let slow = queue.next("slow")?.ok_or("slow's first")?.cursor;
    queue.done("slow", slow)?;
    assert_eq!(queue.lag("fast")?, 0);
    assert_eq!(queue.lag("slow")?, 3);
    assert_eq!(queue.slowest(), Some(slow));
    assert_eq!(queue.processed()?, slow);
    queue.accept(item("i9"))?;
    assert_eq!(
        queue.lag("fast")?,
        0,
        "accept is not refused, and lag counts committed events"
    );
    let _ = queue.flush();
    assert_eq!(queue.lag("fast")?, 1);
    Ok(())
}

/// What keeps a worker cheap: `next` goes to the store once a chunk, and `done` never does, so
/// any trip to the store per event fails. The statements of one trip are counted in the store's
/// `a_chunk_is_one_statement_however_many_events_it_holds`.
#[test]
fn a_worker_goes_to_the_store_once_a_chunk_and_not_to_close()
-> Result<(), Box<dyn std::error::Error>> {
    const CHUNKS: usize = 3;
    let dir = tempfile::tempdir()?;
    // Less one: the declaration is in the log too.
    let queue = committed(&dir, CHUNKS * CHUNK - 1)?;
    let taken = || queue.shared.graph_taken.load(Ordering::SeqCst);
    let (mut by_next, mut by_done, mut closed) = (0, 0, 0);
    loop {
        let before = taken();
        let event = queue.next("w")?;
        by_next += taken() - before;
        let Some(event) = event else { break };
        let before = taken();
        queue.done("w", event.cursor)?;
        by_done += taken() - before;
        closed += 1;
    }
    assert_eq!(closed, CHUNKS * CHUNK, "every event and the declaration");
    assert_eq!(by_next, CHUNKS, "next goes to the store once a chunk");
    assert_eq!(by_done, 0, "done closes in memory");
    Ok(())
}

#[test]
fn a_saved_failure_ahead_of_the_cursor_still_reads_as_failed()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = committed(&dir, 1)?;
    let open_one = queue.next("w")?.ok_or("first")?.cursor;
    let failing = queue.next("w")?.ok_or("second")?.cursor;
    queue.failed("w", failing, "bad")?;
    let _ = queue.flush();
    queue.done("w", open_one)?;
    assert_eq!(queue.state("w", failing)?, State::Failed("bad".to_string()));
    Ok(())
}

#[test]
fn a_worker_joins_at_the_watermark_and_is_handed_only_what_follows()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = committed(&dir, 3)?;
    let at = queue.join("late");
    assert_eq!(queue.next("late")?.map(|e| e.cursor), None);
    queue.accept(item("after"))?;
    let _ = queue.flush();
    let handed = queue.next("late")?.ok_or("the event after joining")?;
    assert!(handed.cursor > at);
    assert!(handed.body.contains("after"));
    assert_eq!(queue.join("late"), at, "a worker with a place stays at it");
    queue.done("late", handed.cursor)?;
    assert_eq!(queue.join("late"), handed.cursor);
    Ok(())
}

#[test]
fn a_purge_through_the_queue_reaches_what_a_worker_read_ahead()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    let told = Arc::new(Mutex::new(Vec::new()));
    let heard = Arc::clone(&told);
    queue.subscribe(move |cursor| lock(&heard).push(cursor));
    for key in ["first", "secret", "last"] {
        queue.accept(item(key))?;
    }
    let (_, failures) = queue.flush();
    assert!(failures.is_empty());
    // The declaration, then the first: the rest are read ahead in memory.
    let mut taken = queue.next("w")?.ok_or("the declaration")?;
    queue.done("w", taken.cursor)?;
    taken = queue.next("w")?.ok_or("the first")?;
    assert!(taken.body.contains("first"));
    let before = lock(&told).len();
    // Held when the purge comes, so it goes with what the log holds.
    queue.accept(item("secret"))?;
    let purged = queue.purge(
        &rannoch_store::Target::Node(rannoch_store::NodeRef::new("item", "secret")),
        "r",
        "app",
    )?;
    assert!(lock(&told).len() > before, "the subscribers hear of it");
    let mut seen = Vec::new();
    while let Some(event) = queue.next("w")? {
        seen.push(event.body);
    }
    assert!(seen.iter().all(|body| !body.contains("secret")), "{seen:?}");
    assert!(seen.iter().any(|body| body.contains("last")));
    assert_eq!(
        seen.last(),
        Some(&purged.body),
        "the rule event, and nothing of it"
    );
    queue.done("w", taken.cursor)?;
    Ok(())
}

#[test]
fn a_fill_that_fails_after_a_purge_is_reported_and_the_rule_event_still_reaches_workers()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let queue = Queue::open(graph(&path(&dir))?, HOUR)?;
    queue.accept(item("k"))?;
    let (_, failures) = queue.flush();
    assert!(failures.is_empty());
    while let Some(event) = queue.next("w")? {
        queue.done("w", event.cursor)?;
    }
    // Fill's place cannot move, so filling the rule event fails in the store, not in one event.
    rusqlite::Connection::open(path(&dir))?.execute_batch(
        "CREATE TRIGGER fill_stuck BEFORE UPDATE ON _places WHEN NEW.worker = 'rannoch.fill' \
         BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END;",
    )?;
    let purged = queue.purge(
        &rannoch_store::Target::Node(rannoch_store::NodeRef::new("item", "k")),
        "r",
        "app",
    )?;
    let failures = queue.take_failures();
    assert!(
        matches!(&failures[..], [Failure::Fill { .. }]),
        "{failures:?}"
    );
    let handed = queue.next("w")?.ok_or("the rule event")?;
    assert_eq!(handed.body, purged.body);
    Ok(())
}
