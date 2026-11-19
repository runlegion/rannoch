//! The flow doc: a flow names the `ty` it runs on and its steps in order; a step is a dot chain
//! of actions, with options per link, an optional `as` and an optional next.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// What a flow takes, or an action gives: each field's type, in the signature vocabulary.
pub type Shape = BTreeMap<String, String>;

/// One flow doc.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlowDoc {
    /// The `ty` of the events the flow runs.
    pub ty: u16,
    /// What the flow takes.
    #[serde(default)]
    pub takes: Shape,
    /// The steps, run in order on one event.
    #[serde(default)]
    pub steps: Vec<Step>,
    /// Whether any step uses a job action: set by the app's build from the signatures.
    #[serde(default)]
    pub job: bool,
}

/// One step: the chain it runs, then its next.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// The links, in dot notation, as `trim.upper.greet`.
    #[serde(rename = "do")]
    pub chain: String,
    /// Each link's own options, by the link's action.
    #[serde(default)]
    pub with: BTreeMap<String, Value>,
    /// The field the step's result goes under, beside the input; with none, it is merged in.
    #[serde(default, rename = "as")]
    pub under: Option<String>,
    /// The send that hands the step's result on; with none, the step is the end.
    #[serde(default)]
    pub next: Option<Next>,
}

/// A next: a new event of `ty`, its data the step's result shaped by `shape`, due after
/// `delay_ms`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Next {
    /// The `ty` of the work it hands to.
    pub ty: u16,
    /// Each field of the next's data, as an expression over the options.
    #[serde(default)]
    pub shape: BTreeMap<String, Value>,
    /// How long after the run the next is due, in milliseconds.
    #[serde(default)]
    pub delay_ms: u64,
}

/// The value of a shape expression over `vals`: `"$field"` reads a field; `{"$concat": [..]}`
/// joins its parts into a string; `{"$": expr, "or": default}` gives `default` when `expr` is
/// absent; anything else is itself.
pub(crate) fn eval(expr: &Value, vals: &Map<String, Value>) -> Value {
    match expr {
        Value::String(field) if field.starts_with('$') => {
            vals.get(&field[1..]).cloned().unwrap_or(Value::Null)
        }
        Value::Object(o) if o.contains_key("$concat") => {
            let parts = o["$concat"].as_array().map_or_default(Vec::as_slice);
            Value::String(
                parts
                    .iter()
                    .map(|part| match eval(part, vals) {
                        Value::String(s) => s,
                        other => other.to_string(),
                    })
                    .collect(),
            )
        }
        Value::Object(o) if o.contains_key("$") => match eval(&o["$"], vals) {
            Value::Null => o.get("or").cloned().unwrap_or(Value::Null),
            value => value,
        },
        other => other.clone(),
    }
}

/// The data a next's `shape` gives over `vals`.
pub(crate) fn shaped(next: &Next, vals: &Map<String, Value>) -> Map<String, Value> {
    next.shape
        .iter()
        .map(|(field, expr)| (field.clone(), eval(expr, vals)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{FlowDoc, eval};
    use serde_json::{Map, Value, json};

    fn vals(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn a_ref_reads_its_field_and_an_absent_one_is_null() {
        let v = vals(json!({ "greeting": "hi SAM" }));

        assert_eq!(eval(&json!("$greeting"), &v), json!("hi SAM"));
        assert_eq!(eval(&json!("$nick"), &v), Value::Null);
        assert_eq!(eval(&json!("plain"), &v), json!("plain"));
    }

    #[test]
    fn concat_builds_a_string_from_its_parts() {
        let v = vals(json!({ "greeting": "hi SAM", "n": 3 }));

        assert_eq!(
            eval(&json!({ "$concat": ["$greeting", "!", "$n"] }), &v),
            json!("hi SAM!3")
        );
    }

    #[test]
    fn or_gives_an_absent_field_its_default_and_leaves_a_present_one() {
        let expr = json!({ "$": "$nick", "or": "friend" });

        assert_eq!(eval(&expr, &Map::new()), json!("friend"));
        assert_eq!(eval(&expr, &vals(json!({ "nick": "sam" }))), json!("sam"));
    }

    #[test]
    fn a_flow_doc_parses_with_its_as_and_next() {
        let doc: FlowDoc = serde_json::from_value(json!({
            "ty": 10,
            "takes": { "text": "string" },
            "steps": [{ "do": "trim.upper", "as": "shout", "next": { "ty": 20, "delay_ms": 5 } }]
        }))
        .unwrap();

        assert_eq!(doc.steps[0].chain, "trim.upper");
        assert_eq!(doc.steps[0].under.as_deref(), Some("shout"));
        let next = doc.steps[0].next.as_ref().unwrap();
        assert_eq!((next.ty, next.delay_ms), (20, 5));
    }
}
