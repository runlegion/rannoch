use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Field name to type: "string", "number", "bool", "list<string>", "list<number>" or "any".
pub type Shape = BTreeMap<String, String>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Flow {
    pub name: String,
    pub ty: u16,
    pub takes: Shape,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub job: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    #[serde(rename = "do")]
    pub chain: String,
    #[serde(default)]
    pub with: BTreeMap<String, Value>,
    #[serde(default, rename = "as")]
    pub under: Option<String>,
    #[serde(default)]
    pub next: Option<Next>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Next {
    To(To),
    Branch { branch: Branch },
}

/// A next to one ty: its data shaped from the options so far, due `delay_ms` after the run;
/// with `each`, one per item of that list field, the item read as "$item".
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct To {
    pub ty: u16,
    #[serde(default)]
    pub shape: BTreeMap<String, Value>,
    #[serde(default)]
    pub delay_ms: u64,
    #[serde(default)]
    pub each: Option<String>,
}

/// The next of the first case whose `when` equals the field `on` by JSON value, else `otherwise`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Branch {
    pub on: String,
    pub cases: Vec<Case>,
    #[serde(rename = "else")]
    pub otherwise: To,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub when: Value,
    pub next: To,
}

/// A shape expression's value: "$field" reads the options so far; {"$concat": [..]} joins its
/// parts as a string; {"$": "$field", "or": default} gives the default when the field is absent;
/// anything else is itself.
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

/// A JSON value's type, in the Shape vocabulary.
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
