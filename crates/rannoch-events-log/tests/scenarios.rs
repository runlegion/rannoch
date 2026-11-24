//! #132's scenario. The app's append is rannoch-events' over the app's database through the real
//! store, wrapped in the log; after it commits a batch, each event of the batch is one JSON line
//! in the log file.

use std::fs;
use std::path::Path;
use std::sync::mpsc;

use chrono::DateTime;
use rannoch_core::{Append, Event, Status, Storage};
use rannoch_events::Events;
use rannoch_events_log::Log;
use rannoch_store::{Opened, Store};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn each_event_of_a_committed_batch_is_one_json_line_in_the_log() {
    // Given an app whose append over its store is wrapped in the log, with the path its settings
    // give
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("events-log");
    drop(fs::remove_dir_all(&dir));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("store.db");
    Store::new(path.clone(), rannoch_core::migrator())
        .apply()
        .unwrap();
    let ready = || match rannoch_store::open(&path, &rannoch_core::migrator()).unwrap() {
        Opened::Ready(conn) => conn,
        Opened::Pending(p) => panic!("pending: {p:?}"),
    };
    let file = dir.join("events.log");
    let log = Log::new(Events::new(ready(), mpsc::channel().0), file.clone());

    // When it commits a batch of three events
    let at = DateTime::from_timestamp_millis(1_790_000_000_000);
    let event = |source, data, run_at, status| Event {
        id: Uuid::now_v7(),
        source,
        ty: 10_u16,
        data,
        run_at,
        status,
    };
    let page = event(None, json!({ "n": 1 }), None, Status::Added);
    let batch = [
        event(Some(page.id), json!("taken"), None, Status::InProgress),
        event(None, json!({ "n": 2 }), at, Status::Added),
        page,
    ];
    log.append(&batch).unwrap();
    let rows: i64 = ready()
        .query_row("SELECT count(*) FROM _events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 3);

    // Then each event of the batch is one JSON line in the log, in the batch's order
    let text = fs::read_to_string(&file).unwrap();
    let lines: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let id = |i: usize| batch[i].id.to_string();
    let want = [
        json!({ "id": id(0), "source": id(2), "ty": 10, "data": "taken", "run_at": null,
            "status": 1 }),
        json!({ "id": id(1), "source": null, "ty": 10, "data": { "n": 2 },
            "run_at": "2026-09-21T14:13:20.000Z", "status": 0 }),
        json!({ "id": id(2), "source": null, "ty": 10, "data": { "n": 1 }, "run_at": null,
            "status": 0 }),
    ];
    assert_eq!(lines, want);
}
