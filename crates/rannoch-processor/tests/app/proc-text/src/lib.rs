//! A processor crate separate from the app: text actions, their signatures exported as data.

use std::sync::Arc;

use rannoch_processor::{Action, Sig};
use serde_json::{Map, Value, json};

/// Every action's signature; the app's build gathers it.
#[cfg(not(feature = "renamed"))]
pub const SIGNATURES: &str = include_str!("../signatures.json");
/// Every action's signature, greet giving `salutation` in place of `greeting`.
#[cfg(feature = "renamed")]
pub const SIGNATURES: &str = include_str!("../signatures-renamed.json");

struct Text(Sig);

impl Action for Text {
    fn sig(&self) -> &Sig {
        &self.0
    }

    fn run(
        &self,
        options: &Value,
        input: &Map<String, Value>,
    ) -> Result<Map<String, Value>, String> {
        let text = input
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let (hi, max) = (options["salutation"].as_str(), options["max"].as_u64());
        let out = match self.0.action.as_str() {
            "trim" => json!({ "text": text.trim() }),
            "upper" => json!({ "text": text.to_uppercase() }),
            "greet" => json!({ "greeting": format!("{} {text}", hi.unwrap_or_default()) }),
            "limit" if text.len() as u64 > max.unwrap_or_default() => {
                return Err(format!(
                    "limit: text is {} long, max {}",
                    text.len(),
                    options["max"]
                ));
            }
            _ => json!({}),
        };
        Ok(out.as_object().cloned().unwrap_or_default())
    }
}

/// The crate's actions, one per signature.
#[must_use]
pub fn actions() -> Vec<Arc<dyn Action>> {
    let sigs: Vec<Sig> = serde_json::from_str(SIGNATURES).unwrap_or_default();
    sigs.into_iter()
        .map(|sig| Arc::new(Text(sig)) as Arc<dyn Action>)
        .collect()
}
