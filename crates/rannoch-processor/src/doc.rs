use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Field name to type name: `"string"`, `"number"`, `"bool"`, `"list<string>"`,
/// `"list<number>"` or `"any"`, each the name [`type_of`] gives a value of that type.
pub type Shape = BTreeMap<String, String>;

/// A flow document: the steps that run for one event `ty`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Flow {
    /// The flow's name, by which `check` names a next to it.
    pub name: String,
    /// The event `ty` the flow runs; nexts and waits reach the flow by it.
    pub ty: u16,
    /// The fields the flow's data holds: the first step's scope, and what every next to this
    /// `ty` must give.
    pub takes: Shape,
    /// The steps, run in order.
    pub steps: Vec<Step>,
    /// Whether the flow runs as a job, on a thread of its own; `false` by default. `check` sets
    /// it, true when any step's action has a job `Sig`.
    #[serde(default)]
    pub job: bool,
}

/// A step: its links, their options, where its result goes and what it sends.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// `do`: the step's links, action names joined by `.`, run in order.
    #[serde(rename = "do")]
    pub chain: String,
    /// Each link's options, by its action name; a link with no entry is passed `{}`. Empty by
    /// default.
    #[serde(default)]
    pub with: BTreeMap<String, Value>,
    /// `as`: the field the step's result goes under as an object; by default none, and the
    /// links' outputs join the values.
    #[serde(default, rename = "as")]
    pub under: Option<String>,
    /// What the step sends; none by default.
    #[serde(default)]
    pub next: Option<Next>,
}

/// What a step sends, in one of three forms told apart by their fields.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Next {
    /// A next to one `ty`.
    To(To),
    /// `{"branch": {..}}`: the next of the first case whose `when` equals a field's value, else
    /// the branch's `else`.
    Branch {
        /// The branch.
        branch: Branch,
    },
    /// `{"wait": {..}}`: a wait for an event of the `ty` it names, with the same key.
    Wait {
        /// The wait.
        wait: Box<Wait>,
    },
}

/// A wait for an event of `for` with the same `key`: `next` on a match, else `timeout`, `late`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Wait {
    /// `for`: the `ty` of the event waited for.
    #[serde(rename = "for")]
    pub for_ty: u16,
    /// A shape expression; an event of `for` matches when it gives the same value over that
    /// event's data as over the values at the wait.
    pub key: Value,
    /// How long the wait lasts, in milliseconds.
    pub within_ms: u64,
    /// Sent on the first match before the wait times out.
    pub next: To,
    /// Sent when the wait times out with no match.
    pub timeout: To,
    /// Sent on the first match after the timeout; by default none, and a late match sends
    /// nothing.
    #[serde(default)]
    pub late: Option<To>,
}

/// A next to one ty: its data shaped from the values, due `delay_ms` after the run; with
/// `each`, one per item of that list field, the item read as `"$item"`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct To {
    /// The `ty` of the event sent.
    pub ty: u16,
    /// The event's data, by field, each value a shape expression; empty by default.
    #[serde(default)]
    pub shape: BTreeMap<String, Value>,
    /// How many milliseconds after the run the event comes due; 0 by default.
    #[serde(default)]
    pub delay_ms: u64,
    /// The list field to send one event per item of; none by default.
    #[serde(default)]
    pub each: Option<String>,
}

/// A branch on one field's value.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Branch {
    /// The name of the field to branch on; an absent field is null.
    pub on: String,
    /// The cases, in order; the first whose `when` equals the field's value sends its `next`.
    pub cases: Vec<Case>,
    /// `else`: sent when no case's `when` equals the field's value.
    #[serde(rename = "else")]
    pub otherwise: To,
}

/// One case of a branch.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// The JSON value the field must equal: the number `1` does not equal the string `"1"`.
    pub when: Value,
    /// Sent when the field equals `when`.
    pub next: To,
}

/// A shape expression's value over `vals`: `"$field"` reads the field, null when it is absent;
/// `{"$concat": [..]}` joins its parts as a string, a part that is not a string as its JSON
/// text; `{"$": <expression>, "or": <default>}` gives the default when the expression gives
/// null; anything else is itself.
#[must_use]
pub fn eval(expr: &Value, vals: &Map<String, Value>) -> Value {
    match expr {
        Value::String(r) if r.starts_with('$') => vals.get(&r[1..]).cloned().unwrap_or_default(),
        Value::Object(o) if o.contains_key("$concat") => {
            let mut text = String::new();
            for part in o["$concat"].as_array().cloned().unwrap_or_default() {
                let v = eval(&part, vals);
                text.push_str(&v.as_str().map_or(v.to_string(), str::to_owned));
            }
            Value::String(text)
        }
        Value::Object(o) if o.contains_key("$") => match eval(&o["$"], vals) {
            Value::Null => o.get("or").cloned().unwrap_or_default(),
            v => v,
        },
        v => v.clone(),
    }
}

/// A JSON value's type, in the Shape vocabulary: a string, number or bool is `string`, `number`
/// or `bool`; an array of at least one item, every item a number, is `list<number>`, and any
/// other array `list<string>`; an object or null is `any`.
#[must_use]
pub fn type_of(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "bool",
        Value::Array(a) if !a.is_empty() && a.iter().all(Value::is_number) => "list<number>",
        Value::Array(_) => "list<string>",
        _ => "any",
    }
}

/// The Shape type a shape expression gives against `scope`: a "$field" gives the field's type
/// or the error "$field names nothing in scope"; {"$concat": [..]} checks every part and gives
/// "string"; {"$": .., "or": default} gives the field's type, else the default's; a literal
/// gives `type_of`.
/// # Errors
/// "$field names nothing in scope", for the first field named that `scope` does not hold.
pub fn type_expr(expr: &Value, scope: &Shape) -> Result<String, String> {
    match expr {
        Value::String(r) if r.starts_with('$') => scope
            .get(&r[1..])
            .cloned()
            .ok_or(format!("{r} names nothing in scope")),
        Value::Object(o) if o.contains_key("$concat") => {
            for part in o["$concat"].as_array().into_iter().flatten() {
                type_expr(part, scope)?;
            }
            Ok("string".to_owned())
        }
        Value::Object(o) if o.contains_key("$") => match (type_expr(&o["$"], scope), o.get("or")) {
            (Err(_), Some(default)) => Ok(type_of(default).to_owned()),
            (got, _) => got,
        },
        v => Ok(type_of(v).to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value, json};

    fn vals(v: &Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn eval_reads_a_field_joins_a_concat_and_gives_or_for_an_absent_field() {
        let vals = vals(&json!({ "text": "SAM", "n": 2 }));
        assert_eq!(super::eval(&json!("$text"), &vals), json!("SAM"));
        let concat = json!({ "$concat": ["hi ", "$text", "!", "$n"] });
        assert_eq!(super::eval(&concat, &vals), json!("hi SAM!2"));
        let or = json!({ "$": "$nick", "or": "friend" });
        assert_eq!(super::eval(&or, &vals), json!("friend"));
        assert_eq!(super::eval(&json!(7), &vals), json!(7));
    }

    #[test]
    fn type_expr_types_a_field_a_concat_an_or_and_a_literal_and_names_a_missing_field() {
        let scope = [("text", "string"), ("n", "number")]
            .map(|(f, t)| (f.to_owned(), t.to_owned()))
            .into();
        let typed = |e: Value| super::type_expr(&e, &scope);
        assert_eq!(typed(json!("$n")), Ok("number".to_owned()));
        assert_eq!(
            typed(json!({ "$concat": ["$n", "!"] })),
            Ok("string".to_owned())
        );
        assert_eq!(
            typed(json!({ "$": "$nick", "or": 1 })),
            Ok("number".to_owned())
        );
        assert_eq!(
            typed(json!({ "$": "$text", "or": 1 })),
            Ok("string".to_owned())
        );
        assert_eq!(typed(json!(true)), Ok("bool".to_owned()));
        let missing = Err("$greeting names nothing in scope".to_owned());
        assert_eq!(typed(json!({ "$concat": ["$greeting", "!"] })), missing);
    }

    #[test]
    fn type_of_names_each_shape_type() {
        let named: Vec<&str> = [
            json!("a"),
            json!(1),
            json!(true),
            json!(["a"]),
            json!([1, 2]),
            json!({}),
        ]
        .iter()
        .map(super::type_of)
        .collect();
        let shape = [
            "string",
            "number",
            "bool",
            "list<string>",
            "list<number>",
            "any",
        ];
        assert_eq!(named, shape);
    }
}
