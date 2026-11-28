//! #154's first scenario, `@criterion-154-c1` in `tests/features/store_settings.feature`. The
//! settings are held for the life of the process, so this scenario has a test binary of its own.

use std::fs;
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use rannoch_core::Storage;
use rannoch_store::{Opened, Store, open};

/// The checkpointer's failed opens logged in this test binary.
struct Unopened(Mutex<usize>);

impl log::Log for Unopened {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Error
    }
    fn log(&self, record: &log::Record<'_>) {
        if record
            .args()
            .to_string()
            .starts_with("checkpointer cannot open its connection")
        {
            *self.0.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        }
    }
    fn flush(&self) {}
}

static UNOPENED: Unopened = Unopened(Mutex::new(0));

fn unopened() -> usize {
    *UNOPENED.0.lock().unwrap_or_else(PoisonError::into_inner)
}

#[tokio::test(start_paused = true)]
async fn store_settings_c1() {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("store_settings_c1");
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();
    log::set_logger(&UNOPENED).unwrap();
    log::set_max_level(log::LevelFilter::Error);

    // Given settings loaded before Store::new whose [store] section sets both keys
    let file = dir.join("app.toml");
    fs::write(
        &file,
        "[store]\ncache_size_kib = 2048\ncheckpoint_every_ms = 50\n",
    )
    .unwrap();
    rannoch_settings::load(&file).unwrap();

    // When the store is made and opens its connections
    let db = dir.join("store.db");
    let store = Store::new(db.clone(), rannoch_core::migrator());
    store.apply().unwrap();
    let Opened::Ready(opened) = open(&db, &rannoch_core::migrator()).unwrap() else {
        panic!("a migration is still pending");
    };

    // Then every connection has that cache size
    for conn in [opened, Store::connection().unwrap()] {
        let cache: i64 = conn
            .query_row("PRAGMA cache_size", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cache, -2048);
    }
    drop(store);

    // And the checkpointer runs at that interval: a store whose database cannot open logs each
    // try, and at 50 ms it tries several times before the default's first try at one second.
    let store = Store::new(dir.join("none").join("store.db"), rannoch_core::migrator());
    tokio::time::sleep(Duration::from_millis(900)).await;
    drop(store);
    assert!(unopened() >= 5, "{} tries in 900 ms", unopened());
}
