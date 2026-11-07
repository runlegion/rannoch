//! Each event type's data schema, and a checker for the small part of JSON Schema they use:
//! `type` (one name or a list), `required`, `properties`, `items` and `enum`. A type with no
//! schema fails, so nothing passes by being unknown.

use std::collections::HashMap;

use serde_json::Value;

/// Every type's schema, from `schemas.json`.
#[derive(Debug)]
pub struct Schemas(HashMap<String, Value>);

impl Schemas {
    /// # Errors
    /// The file is not a JSON object of schemas.
    pub fn load(text: &str) -> Result<Schemas, String> {
        let Value::Object(all) = serde_json::from_str(text).map_err(|e| e.to_string())? else {
            return Err("schemas.json is not an object".into());
        };
        Ok(Schemas(all.into_iter().collect()))
    }

    /// Checks `data` against the schema of `ty`.
    ///
    /// # Errors
    /// The type has no schema, or the data does not pass it, with where it failed.
    pub fn check(&self, ty: &str, data: &Value) -> Result<(), String> {
        let schema = self.0.get(ty).ok_or_else(|| format!("no schema for {ty}"))?;
        check(schema, data, "data")
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn knows(&self, ty: &str) -> bool {
        self.0.contains_key(ty)
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn check(schema: &Value, value: &Value, at: &str) -> Result<(), String> {
    if let Some(want) = schema.get("type") {
        let names: Vec<&str> = match want {
            Value::String(s) => vec![s.as_str()],
            Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        let got = type_name(value);
        let fits = names.iter().any(|n| *n == got || (*n == "number" && got == "integer"));
        if !fits {
            return Err(format!("{at} is {got}, not {}", names.join(" or ")));
        }
    }
    if let Some(Value::Array(allowed)) = schema.get("enum")
        && !allowed.contains(value)
    {
        return Err(format!("{at} is not one of {allowed:?}"));
    }
    if let Value::Object(object) = value {
        if let Some(Value::Array(required)) = schema.get("required") {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    return Err(format!("{at}.{name} is missing"));
                }
            }
        }
        if let Some(Value::Object(properties)) = schema.get("properties") {
            for (name, sub) in properties {
                if let Some(v) = object.get(name) {
                    check(sub, v, &format!("{at}.{name}"))?;
                }
            }
        }
    }
    if let (Value::Array(items), Some(sub)) = (value, schema.get("items")) {
        for (i, item) in items.iter().enumerate() {
            check(sub, item, &format!("{at}[{i}]"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_schema_refuses_what_it_does_not_describe_and_an_unknown_type_fails() {
        let schemas = Schemas::load(r#"{"t": {"type": "object", "required": ["a"], "properties": {"a": {"type": "integer"}, "l": {"type": "array", "items": {"type": "string"}}}}}"#).unwrap();
        assert!(schemas.check("t", &serde_json::json!({"a": 1, "l": ["x"]})).is_ok());
        assert!(schemas.check("t", &serde_json::json!({"a": "1"})).unwrap_err().contains("data.a"));
        assert!(schemas.check("t", &serde_json::json!({"l": ["x"]})).unwrap_err().contains("missing"));
        assert!(schemas.check("t", &serde_json::json!({"a": 1, "l": [2]})).unwrap_err().contains("l[0]"));
        assert!(schemas.check("u", &serde_json::json!({})).unwrap_err().contains("no schema"));
    }
}
