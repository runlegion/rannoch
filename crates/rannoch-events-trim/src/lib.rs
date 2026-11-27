#![warn(missing_docs)]
//! A step, `trim`, that deletes the processed events in `_events` older than its options say. An
//! app calls it from a flow it sets up; its options are `{"older_than_ms": n}`, the app's
//! choice, and it deletes each event whose v7 id was made more than n milliseconds before the
//! step runs and that has a 2, together with its status rows: the rows whose source is the
//! event and whose status is not 0. It leaves every event with no 2, and its status rows:
//!
//! ```json
//! {"do": "trim", "with": {"trim": {"older_than_ms": 86400000}}}
//! ```
//!
//! The app adds [`SIGNATURES`] to the signatures its `build.rs` hands `compile`, and
//! [`actions`] to the actions it hands `Processor::new`; neither takes an argument. The step
//! reaches the one store through [`Store::connection`]. It gives nothing.
//!
//! The step fails, with `trim: ` and the reason, when `older_than_ms` is not a whole number of
//! milliseconds, or when the store cannot be reached or the delete fails.

mod queries;

use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rannoch_processor::{Action, Sig};
use rannoch_store::Store;
use serde_json::{Map, Value};
use uuid::Builder;

/// The step's signature; the app's build gathers it.
pub const SIGNATURES: &str = include_str!("../signatures.json");

/// The `Sig` in [`SIGNATURES`].
static SIG: LazyLock<Sig> = LazyLock::new(|| {
    let sigs: Vec<Sig> = serde_json::from_str(SIGNATURES).unwrap_or_default();
    sigs.into_iter().next().unwrap_or_default()
});

/// The step.
struct Trim;

impl Action for Trim {
    fn sig(&self) -> &Sig {
        &SIG
    }

    fn run(&self, options: &Value, _: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let fail = |e: &dyn std::fmt::Display| format!("trim: {e}");
        let n = &options["older_than_ms"];
        let n = n
            .as_u64()
            .ok_or_else(|| fail(&format!("{n} is not a whole number of ms")))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| fail(&e))?;
        let at = now.as_millis().saturating_sub(u128::from(n));
        let at = u64::try_from(at).map_err(|e| fail(&e))?;
        // The least v7 id of that millisecond: every id below it was made before it.
        let cutoff = Builder::from_unix_timestamp_millis(at, &[0; 10]).into_uuid();
        let conn = Store::connection().map_err(|e| fail(&e))?;
        conn.execute(queries::TRIM, [cutoff.to_string()])
            .map_err(|e| fail(&e))?;
        Ok(Map::new())
    }
}

/// The crate's one action, `trim`.
#[must_use]
pub fn actions() -> Vec<Arc<dyn Action>> {
    vec![Arc::new(Trim)]
}
