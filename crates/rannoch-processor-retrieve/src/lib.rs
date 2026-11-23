//! A flow action that queries one of the app's outside sources, through a trait the app
//! implements; the source's result is checked against the source's declared gives.

use std::sync::Arc;

use rannoch_processor::{Sig, type_of};
use serde_json::{Map, Value};

/// What the app implements to reach its outside sources; rannoch reaches none on its own.
pub trait Source: Send + Sync {
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
    /// The source's Sig is the one the app declares for it in its signatures.json, with job true.
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

    #[test]
    fn a_field_of_the_wrong_type_fails_naming_it() {
        let got = retrieve(json!({ "email": 7 })).run(&json!({}), &Map::new());
        assert_eq!(got, Err("email as number".to_owned()));
    }

    #[test]
    fn a_fitting_result_passes_through() {
        let gave = json!({ "email": "a@b.c", "name": "a" });
        let got = retrieve(gave.clone()).run(&json!({}), &Map::new());
        assert_eq!(got.map(Value::Object), Ok(gave));
    }
}
