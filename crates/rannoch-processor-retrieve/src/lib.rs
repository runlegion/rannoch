#![warn(missing_docs)]
//! A flow action that queries one of the app's outside sources, through a trait the app
//! implements; the source's result is checked against the source's declared gives.
//!
//! The app implements [`Source`] for each source it reaches, and wraps it in a [`Retrieve`]
//! with the [`Sig`] it declares for the source in its signatures export. It hands the action to
//! `Processor::new` with its other actions, and a step calls it by the `Sig`'s `action`, as in
//! `{"do": "lookup"}`. `Retrieve` does not check the input against the `Sig`'s takes; `check`
//! does that when the app builds.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use rannoch_processor::{Action, Sig};
//! use rannoch_processor_retrieve::{Retrieve, Source};
//! use serde_json::{Map, Value, json};
//!
//! /// The app's source; this one makes each user's email from their name.
//! struct Directory;
//!
//! impl Source for Directory {
//!     fn get(&self, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
//!         let user = input.get("user").and_then(Value::as_str).ok_or("no user")?;
//!         Ok(Map::from_iter([("email".to_owned(), json!(format!("{user}@example.com")))]))
//!     }
//! }
//!
//! let sig: Sig = serde_json::from_value(json!({ "action": "lookup", "job": true,
//!     "takes": { "user": "string" }, "gives": { "email": "string" } }))?;
//! let lookup = Retrieve::new(sig, Arc::new(Directory));
//!
//! let input = Map::from_iter([("user".to_owned(), json!("sam"))]);
//! assert_eq!(lookup.run(&json!({}), &input)?["email"], json!("sam@example.com"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::sync::Arc;

use rannoch_processor::{Sig, type_of};
use serde_json::{Map, Value};

/// What the app implements to reach its outside sources; rannoch reaches none on its own.
pub trait Source: Send + Sync {
    /// Gets from the source for `input`, the step's input; the result is checked against the
    /// source's gives.
    /// # Errors
    /// The failure to get, which fails the step.
    fn get(&self, input: &Map<String, Value>) -> Result<Map<String, Value>, String>;
}

/// One source as a flow action: a step names it by its Sig's action.
#[expect(
    missing_debug_implementations,
    reason = "the issue's shape; a Source is not Debug"
)]
pub struct Retrieve {
    sig: Sig,
    source: Arc<dyn Source>,
}

impl Retrieve {
    /// The action querying `source`, with `sig` the one the app declares for the source in its
    /// signatures export. `check` reads that export, so its `job` decides whether a flow that
    /// queries the source runs as a job.
    #[must_use]
    pub fn new(sig: Sig, source: Arc<dyn Source>) -> Self {
        Self { sig, source }
    }
}

impl rannoch_processor::Action for Retrieve {
    fn sig(&self) -> &Sig {
        &self.sig
    }

    /// Gets, and checks the result against `sig.gives`: `<field> as <type>` for a field of the
    /// wrong type, `no <field>` for a missing one.
    fn run(
        &self,
        _options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String> {
        let out = self.source.get(input)?;
        for (field, ty) in &self.sig.gives {
            match out.get(field).map(type_of) {
                Some(t) if t == ty => {}
                Some(t) => return Err(format!("{field} as {t}")),
                None => return Err(format!("no {field}")),
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rannoch_processor::Action;
    use serde_json::json;

    struct Stub(Value);
    impl Source for Stub {
        fn get(&self, _: &Map<String, Value>) -> Result<Map<String, Value>, String> {
            Ok(self.0.as_object().cloned().unwrap_or_default())
        }
    }

    fn retrieve(gave: Value) -> Retrieve {
        let gives = [("email".to_owned(), "string".to_owned())].into();
        let sig = Sig {
            action: "lookup".into(),
            gives,
            job: true,
            ..Sig::default()
        };
        Retrieve::new(sig, Arc::new(Stub(gave)))
    }

    #[tokio::test(start_paused = true)]
    async fn a_field_of_the_wrong_type_fails_naming_it() {
        let got = retrieve(json!({ "email": 7 })).run(&json!({}), &Map::new());
        assert_eq!(got, Err("email as number".to_owned()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_fitting_result_passes_through() {
        let gave = json!({ "email": "a@b.c", "name": "a" });
        let got = retrieve(gave.clone()).run(&json!({}), &Map::new());
        assert_eq!(got.map(Value::Object), Ok(gave));
    }
}
