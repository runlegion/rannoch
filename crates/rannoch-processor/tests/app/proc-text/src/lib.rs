//! A test processor crate, separate from the app: text actions, with their signatures exported as
//! data for the app's build to gather.

use std::sync::Arc;

use rannoch_processor::{Action, Signature, gather};
use serde_json::{Map, Value, json};

/// Every action's signature: options, takes, gives, job. The app's build.rs gathers it.
pub const SIGNATURES: &str = include_str!("../signatures.json");

/// One text action, by its signature's name.
struct Text(Signature);

impl Action for Text {
    fn signature(&self) -> &Signature {
        &self.0
    }

    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String> {
        let text = input.get("text").and_then(Value::as_str).unwrap_or_default();
        let out = match self.0.action.as_str() {
            "trim" => json!({ "text": text.trim() }),
            "upper" => json!({ "text": text.to_uppercase() }),
            "greet" => {
                let hi = options["salutation"].as_str().unwrap_or_default();
                json!({ "greeting": format!("{hi} {text}") })
            }
            "limit" => {
                let max = options["max"].as_u64().unwrap_or_default();
                let len = text.len();
                if u64::try_from(len).unwrap_or(u64::MAX) > max {
                    return Err(format!("limit: text is {len} long, max {max}"));
                }
                json!({})
            }
            other => return Err(format!("no action named {other}")),
        };
        Ok(match out {
            Value::Object(map) => map,
            _ => Map::new(),
        })
    }
}

/// This crate's actions, one per exported signature.
#[must_use]
pub fn actions() -> Vec<Arc<dyn Action>> {
    gather(&[SIGNATURES])
        .unwrap_or_default()
        .into_iter()
        .map(|sig| Arc::new(Text(sig)) as Arc<dyn Action>)
        .collect()
}
