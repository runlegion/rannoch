#![warn(missing_docs)]
//! A flow step that calls an agent the app reaches through [`Agent`]: the step's input is
//! checked against the agent's declared takes before the call, its answer against its gives
//! after.
//!
//! The app implements [`Agent`] for each agent it reaches, and wraps it in an [`AgentAction`]
//! with the [`Sig`] it declares for the agent in its signatures export. It hands the action to
//! `Processor::new` with its other actions, and a step calls it by the `Sig`'s `action`, as in
//! `{"do": "rate"}`.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use rannoch_processor::{Action, Sig};
//! use rannoch_processor_agent::{Agent, AgentAction};
//! use serde_json::{Map, Value, json};
//!
//! /// The app's agent; this one rates every text 2.
//! struct Rater;
//!
//! impl Agent for Rater {
//!     fn ask(&self, _input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
//!         Ok(Map::from_iter([("severity".to_owned(), json!(2))]))
//!     }
//! }
//!
//! let sig: Sig = serde_json::from_value(json!({ "action": "rate", "job": true,
//!     "takes": { "text": "string" }, "gives": { "severity": "number" } }))?;
//! let rate = AgentAction::new(sig, Arc::new(Rater));
//!
//! let input = Map::from_iter([("text".to_owned(), json!("it crashed"))]);
//! assert_eq!(rate.run(&json!({}), &input)?["severity"], json!(2));
//!
//! // Input that does not fit the agent's takes fails the step without asking the agent.
//! let input = Map::from_iter([("text".to_owned(), json!(5))]);
//! let failure = "agent rate input gave text as number, it declares string";
//! assert_eq!(rate.run(&json!({}), &input), Err(failure.to_owned()));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::sync::Arc;

use rannoch_processor::{Action, Shape, Sig, type_of};
use serde_json::{Map, Value};

/// What the app implements to reach its agents; rannoch reaches none on its own.
pub trait Agent: Send + Sync {
    /// Asks the agent with `input`, the step's input once it fits the agent's takes; the answer
    /// is checked against its gives.
    /// # Errors
    /// The agent's failure, which fails the step.
    fn ask(&self, input: &Map<String, Value>) -> Result<Map<String, Value>, String>;
}

/// One agent as a flow action: a step names it by its Sig's action.
#[expect(
    missing_debug_implementations,
    reason = "the agent is the app's and need not be Debug"
)]
pub struct AgentAction {
    sig: Sig,
    agent: Arc<dyn Agent>,
}

impl AgentAction {
    /// The action calling `agent`, with `sig` the one the app declares for the agent in its
    /// signatures export. `check` reads that export, so its `job` decides whether a flow that
    /// calls the agent runs as a job.
    #[must_use]
    pub fn new(sig: Sig, agent: Arc<dyn Agent>) -> Self {
        Self { sig, agent }
    }
}

/// The first field of `shape` that `got` lacks or holds as another type: "<what> <missing>
/// <field>", or "<what> gave <field> as <type>, it declares <type>".
fn fit(got: &Map<String, Value>, shape: &Shape, what: &str, missing: &str) -> Result<(), String> {
    for (field, ty) in shape {
        match got.get(field).map(type_of) {
            Some(t) if t == ty => {}
            Some(t) => return Err(format!("{what} gave {field} as {t}, it declares {ty}")),
            None => return Err(format!("{what} {missing} {field}")),
        }
    }
    Ok(())
}

impl Action for AgentAction {
    fn sig(&self) -> &Sig {
        &self.sig
    }

    /// Checks `input` against `sig.takes` (`agent <name> input gave <field> as <type>, it declares
    /// <type>`, or `agent <name> input has no <field>`) and returns that error without calling
    /// the agent; else asks, and checks the answer against `sig.gives` (`agent <name> gave no
    /// <field>`, or `agent <name> gave <field> as <type>, it declares <type>`).
    fn run(&self, _: &Value, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let name = format!("agent {}", self.sig.action);
        fit(input, &self.sig.takes, &format!("{name} input"), "has no")?;
        let answer = self.agent.ask(input)?;
        fit(&answer, &self.sig.gives, &name, "gave no")?;
        Ok(answer)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rannoch_processor::{Action, Sig};
    use serde_json::{Map, Value, json};

    use super::{Agent, AgentAction};

    /// A stub agent that counts its calls and answers `answer`.
    struct Stub(Value, AtomicUsize);
    impl Agent for Stub {
        fn ask(&self, _: &Map<String, Value>) -> Result<Map<String, Value>, String> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(self.0.as_object().cloned().unwrap_or_default())
        }
    }

    /// classify, taking text as a string and giving severity as a number, over a stub answering
    /// `answer`; returns its run on `input` and the stub's call count.
    fn classify(answer: &Value, input: &Value) -> (Result<Map<String, Value>, String>, usize) {
        let sig: Sig = serde_json::from_value(json!({ "action": "classify", "job": true,
            "takes": { "text": "string" }, "gives": { "severity": "number" } }))
        .unwrap();
        let stub = Arc::new(Stub(answer.clone(), AtomicUsize::new(0)));
        let action = AgentAction::new(sig, stub.clone());
        let input = input.as_object().cloned().unwrap_or_default();
        (
            action.run(&json!({}), &input),
            stub.1.load(Ordering::SeqCst),
        )
    }

    #[test]
    fn input_of_the_wrong_type_returns_the_input_error_and_does_not_call_the_agent() {
        let (ran, calls) = classify(&json!({ "severity": 2 }), &json!({ "text": 5 }));
        let error = "agent classify input gave text as number, it declares string";
        assert_eq!(ran.unwrap_err(), error);
        assert_eq!(calls, 0);
    }

    #[test]
    fn a_missing_given_field_returns_gave_no_field() {
        let (ran, calls) = classify(&json!({}), &json!({ "text": "it crashed" }));
        assert_eq!(ran.unwrap_err(), "agent classify gave no severity");
        assert_eq!(calls, 1);
    }

    #[test]
    fn a_fitting_answer_passes_through() {
        let (ran, _) = classify(&json!({ "severity": 2 }), &json!({ "text": "it crashed" }));
        assert_eq!(
            ran.unwrap(),
            json!({ "severity": 2 }).as_object().cloned().unwrap()
        );
    }
}
