//! The check at the app's build: the app's build.rs gathers the signatures every processor crate
//! it uses exports, its own included, and checks every step of its flow docs against them. Any
//! error fails the build, and every error is listed, each on a line of its own that starts with
//! its kind, `E1` to `E7`:
//!
//! - E1: a step names an action that does not exist;
//! - E2: the options for an action do not parse;
//! - E3: a link takes a field nothing before it gives;
//! - E4: a link takes a field given with the wrong type;
//! - E5: a next is missing a field the receiving flow takes, or one of its templates names a
//!   field nothing in scope gives;
//! - E6: a next gives a field with the wrong type;
//! - E7: a next goes to a `ty` no flow takes.
//!
//! A flow doc that is not a flow doc at all is `E0`.

use serde_json::{Map, Value};

use crate::doc::{FlowDoc, Next, Shape};
use crate::{Signature, type_of};

/// The signatures the processor crates export, each export a JSON list of signatures.
///
/// # Errors
///
/// The export that does not parse, and why.
pub fn gather(exports: &[&str]) -> Result<Vec<Signature>, String> {
    let mut signatures = Vec::new();
    for export in exports {
        let some: Vec<Signature> = serde_json::from_str(export)
            .map_err(|err| format!("signatures do not parse: {err}"))?;
        signatures.extend(some);
    }
    Ok(signatures)
}

/// Checks the flow docs `docs`, each a file name and its JSON text, against `signatures`: the
/// docs to compile in, each with its job flag set, in the order given.
///
/// # Errors
///
/// Every error in every doc, in order.
pub fn check(
    docs: &[(String, String)],
    signatures: &[Signature],
) -> Result<Vec<String>, Vec<String>> {
    let mut errors = Vec::new();
    let mut flows = Vec::new();
    for (file, text) in docs {
        match serde_json::from_str::<FlowDoc>(text) {
            Ok(doc) => flows.push((file.as_str(), doc)),
            Err(err) => errors.push(format!("E0 {file}: not a flow doc: {err}")),
        }
    }
    for (file, flow) in &flows {
        errors.extend(flow_errors(file, flow, &flows, signatures));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    flows
        .into_iter()
        .map(|(_, mut flow)| {
            flow.job = is_job(&flow, signatures);
            serde_json::to_string(&flow).map_err(|err| vec![err.to_string()])
        })
        .collect()
}

/// Whether any step of `flow` uses a job action.
fn is_job(flow: &FlowDoc, signatures: &[Signature]) -> bool {
    flow.steps.iter().any(|step| {
        step.chain
            .split('.')
            .any(|link| signatures.iter().any(|s| s.action == link && s.job))
    })
}

/// Every error in `flow`, from the file `file`, among `flows`.
fn flow_errors(
    file: &str,
    flow: &FlowDoc,
    flows: &[(&str, FlowDoc)],
    signatures: &[Signature],
) -> Vec<String> {
    let mut errors = Vec::new();
    let mut scope = flow.takes.clone();
    for (index, step) in flow.steps.iter().enumerate() {
        let mut given = scope.clone();
        for link in step.chain.split('.') {
            let at = format!("{file} step {index} link {link}");
            let options = step
                .with
                .get(link)
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new()));
            match signature(signatures, link, &options) {
                Err((kind, message)) => errors.push(format!("E{kind} {at}: {message}")),
                Ok(sig) => {
                    for (field, takes) in &sig.takes {
                        match given.get(field) {
                            None => errors.push(format!(
                                "E3 {at}: takes {field}, which nothing before it gives"
                            )),
                            Some(is) if !fits(is, takes) => {
                                errors
                                    .push(format!("E4 {at}: takes {field} as {takes}, it is {is}"));
                            }
                            Some(_) => {}
                        }
                    }
                    given.extend(sig.gives.clone());
                }
            }
        }
        match &step.under {
            Some(field) => {
                scope.insert(field.clone(), "any".to_owned());
            }
            None => scope = given,
        }
        if let Some(next) = &step.next {
            let at = format!("{file} step {index} next");
            errors.extend(next_errors(&at, next, &scope, flows));
        }
    }
    errors
}

/// Every error in `next`, sent with the fields `scope` holds, among `flows`.
fn next_errors(at: &str, next: &Next, scope: &Shape, flows: &[(&str, FlowDoc)]) -> Vec<String> {
    let Some((to, receiving)) = flows.iter().find(|(_, f)| f.ty == next.ty) else {
        return vec![format!(
            "E7 {at}: next to ty {}, which no flow takes",
            next.ty
        )];
    };
    let mut errors = Vec::new();
    let mut shaped = Shape::new();
    for (field, expr) in &next.shape {
        match type_expr(expr, scope) {
            Ok(ty) => {
                shaped.insert(field.clone(), ty);
            }
            Err(message) => errors.push(format!("E5 {at}: next to {to} field {field}: {message}")),
        }
    }
    for (field, takes) in &receiving.takes {
        match (next.shape.contains_key(field), shaped.get(field)) {
            (false, _) => errors.push(format!(
                "E5 {at}: next to {to} gives no {field}, which it takes"
            )),
            (true, Some(gives)) if !fits(gives, takes) => errors.push(format!(
                "E6 {at}: next to {to} gives {field} as {gives}, it takes {takes}"
            )),
            (true, _) => {}
        }
    }
    errors
}

/// The signature of `action`, once `options` meet its declared options exactly; otherwise the
/// error's kind and message.
fn signature<'a>(
    signatures: &'a [Signature],
    action: &str,
    options: &Value,
) -> Result<&'a Signature, (u8, String)> {
    let sig = signatures
        .iter()
        .find(|s| s.action == action)
        .ok_or_else(|| (1, format!("no action named {action}")))?;
    let fail = |why: String| (2, format!("options for {action} do not parse: {why}"));
    let given = options
        .as_object()
        .ok_or_else(|| fail("not an object".to_owned()))?;
    if let Some(unknown) = given.keys().find(|k| !sig.options.contains_key(*k)) {
        return Err(fail(format!("unknown field `{unknown}`")));
    }
    for (option, declared) in &sig.options {
        match given.get(option) {
            None => return Err(fail(format!("missing field `{option}`"))),
            Some(value) if type_of(value) != declared => {
                return Err(fail(format!(
                    "{option} is {}, not {declared}",
                    type_of(value)
                )));
            }
            Some(_) => {}
        }
    }
    Ok(sig)
}

/// Whether a field of type `given` fits where `takes` is wanted: `any` takes every type.
fn fits(given: &str, takes: &str) -> bool {
    takes == "any" || given == takes
}

/// The type a shape expression gives over the fields `scope` holds, or why it gives none: every
/// field a `"$field"`, a `$concat` part or an `or` names is checked.
fn type_expr(expr: &Value, scope: &Shape) -> Result<String, String> {
    match expr {
        Value::String(field) if field.starts_with('$') => scope
            .get(&field[1..])
            .cloned()
            .ok_or_else(|| format!("{field} names nothing in scope")),
        Value::Object(o) if o.contains_key("$concat") => {
            for part in o["$concat"].as_array().into_iter().flatten() {
                type_expr(part, scope)?;
            }
            Ok("string".to_owned())
        }
        Value::Object(o) if o.contains_key("$") => match (type_expr(&o["$"], scope), o.get("or")) {
            (Ok(ty), _) => Ok(ty),
            (Err(_), Some(default)) => Ok(type_of(default).to_owned()),
            (Err(message), None) => Err(message),
        },
        other => Ok(type_of(other).to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{check, gather, type_expr};
    use crate::{FlowDoc, Signature};

    /// The test processor crate's signatures: trim, upper, greet, limit, and classify, a job.
    fn signatures() -> Vec<Signature> {
        gather(&[&json!([
            { "action": "trim", "takes": { "text": "string" }, "gives": { "text": "string" } },
            { "action": "upper", "takes": { "text": "string" }, "gives": { "text": "string" } },
            { "action": "greet", "options": { "salutation": "string" },
              "takes": { "text": "string" }, "gives": { "greeting": "string" } },
            { "action": "limit", "options": { "max": "number" }, "takes": { "text": "string" } },
            { "action": "classify", "job": true,
              "takes": { "text": "string" }, "gives": { "category": "string" } }
        ])
        .to_string()])
        .unwrap()
    }

    fn docs(list: &[(&str, Value)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(file, doc)| ((*file).to_owned(), doc.to_string()))
            .collect()
    }

    fn notice() -> (&'static str, Value) {
        (
            "20-notice.json",
            json!({ "ty": 20, "takes": { "msg": "string", "who": "string" } }),
        )
    }

    /// The errors checking `flow` beside the notice flow lists.
    fn errors(flow: Value) -> Vec<String> {
        check(&docs(&[("a.json", flow), notice()]), &signatures())
            .err()
            .unwrap_or_default()
    }

    #[test]
    fn a_valid_set_builds_with_each_flows_job_flag_set() {
        let set = docs(&[
            (
                "10-greet.json",
                json!({ "ty": 10, "takes": { "text": "string" }, "steps": [{
                    "do": "trim.upper.greet", "with": { "greet": { "salutation": "hi" } },
                    "next": { "ty": 20, "delay_ms": 500, "shape": {
                        "msg": { "$concat": ["$greeting", "!"] },
                        "who": { "$": "$nick", "or": "friend" } } } }] }),
            ),
            (
                "40-two.json",
                json!({ "ty": 40, "takes": { "text": "string" }, "steps": [
                    { "do": "trim", "next": { "ty": 20, "shape": { "msg": "$text", "who": "two" } } },
                    { "do": "classify" }] }),
            ),
            notice(),
        ]);

        let compiled = check(&set, &signatures()).unwrap();

        let jobs: Vec<(u16, bool)> = compiled
            .iter()
            .map(|text| serde_json::from_str::<FlowDoc>(text).unwrap())
            .map(|doc| (doc.ty, doc.job))
            .collect();
        assert_eq!(jobs, [(10, false), (40, true), (20, false)]);
    }

    #[test]
    fn a_step_naming_no_action_is_e1_and_emit_and_forget_name_none() {
        for name in ["nope", "emit", "forget"] {
            let found = errors(json!({ "ty": 1, "takes": { "text": "string" },
                "steps": [{ "do": format!("trim.{name}") }] }));

            assert_eq!(
                found,
                [format!(
                    "E1 a.json step 0 link {name}: no action named {name}"
                )]
            );
        }
    }

    #[test]
    fn options_that_do_not_meet_the_declared_options_are_e2() {
        for (with, why) in [
            (json!({ "salut": "hi" }), "unknown field `salut`"),
            (json!({}), "missing field `salutation`"),
            (
                json!({ "salutation": 1 }),
                "salutation is number, not string",
            ),
            (json!("hi"), "not an object"),
        ] {
            let found = errors(json!({ "ty": 1, "takes": { "text": "string" },
                "steps": [{ "do": "greet", "with": { "greet": with } }] }));

            assert_eq!(
                found,
                [format!(
                    "E2 a.json step 0 link greet: options for greet do not parse: {why}"
                )]
            );
        }
    }

    #[test]
    fn a_link_taking_what_nothing_before_it_gives_is_e3_and_the_wrong_type_e4() {
        assert_eq!(
            errors(json!({ "ty": 1, "takes": { "x": "number" }, "steps": [{ "do": "upper" }] })),
            ["E3 a.json step 0 link upper: takes text, which nothing before it gives"]
        );
        assert_eq!(
            errors(json!({ "ty": 1, "takes": { "text": "number" }, "steps": [{ "do": "upper" }] })),
            ["E4 a.json step 0 link upper: takes text as string, it is number"]
        );
    }

    #[test]
    fn a_link_takes_what_a_link_before_it_gives() {
        let found = errors(json!({ "ty": 1, "takes": {}, "steps": [
            { "do": "greet", "with": { "greet": { "salutation": "hi" } } }] }));
        assert_eq!(
            found,
            ["E3 a.json step 0 link greet: takes text, which nothing before it gives"]
        );

        let signatures = gather(&[&json!([
            { "action": "source", "gives": { "text": "string" } },
            { "action": "upper", "takes": { "text": "string" }, "gives": { "text": "string" } }
        ])
        .to_string()])
        .unwrap();
        let set = docs(&[(
            "a.json",
            json!({ "ty": 1, "steps": [{ "do": "source.upper" }] }),
        )]);
        assert!(check(&set, &signatures).is_ok());
    }

    #[test]
    fn a_next_missing_a_field_is_e5_the_wrong_type_e6_and_no_flow_e7() {
        let next = |shape: Value, ty: u16| {
            errors(json!({ "ty": 1, "takes": { "text": "string" },
                "steps": [{ "do": "upper", "next": { "ty": ty, "shape": shape } }] }))
        };

        assert_eq!(
            next(json!({ "msg": "$text" }), 20),
            ["E5 a.json step 0 next: next to 20-notice.json gives no who, which it takes"]
        );
        assert_eq!(
            next(json!({ "msg": "$text", "who": 5 }), 20),
            ["E6 a.json step 0 next: next to 20-notice.json gives who as number, it takes string"]
        );
        assert_eq!(
            next(json!({}), 99),
            ["E7 a.json step 0 next: next to ty 99, which no flow takes"]
        );
    }

    #[test]
    fn every_field_a_template_names_is_checked() {
        let found = errors(json!({ "ty": 10, "takes": { "text": "string" }, "steps": [{
            "do": "upper",
            "next": { "ty": 20, "shape": {
                "msg": { "$concat": ["$greeting", "!"] }, "who": "x", "extra": "$nick" } } }] }));

        assert_eq!(
            found,
            [
                "E5 a.json step 0 next: next to 20-notice.json field extra: $nick names nothing in scope",
                "E5 a.json step 0 next: next to 20-notice.json field msg: $greeting names nothing in scope",
            ]
        );
    }

    #[test]
    fn or_gives_its_defaults_type_when_its_field_is_absent_and_the_fields_when_present() {
        let scope = [("n".to_owned(), "number".to_owned())]
            .into_iter()
            .collect();

        assert_eq!(
            type_expr(&json!({ "$": "$nick", "or": "friend" }), &scope),
            Ok("string".to_owned())
        );
        assert_eq!(
            type_expr(&json!({ "$": "$n", "or": "friend" }), &scope),
            Ok("number".to_owned())
        );
        assert_eq!(
            type_expr(&json!({ "$": "$nick" }), &scope),
            Err("$nick names nothing in scope".to_owned())
        );
        assert_eq!(
            type_expr(&json!({ "$concat": ["$n", "!"] }), &scope),
            Ok("string".to_owned())
        );
    }

    #[test]
    fn as_puts_the_steps_result_under_its_field_beside_the_input() {
        let flow = |shape: Value| {
            json!({ "ty": 12, "takes": { "text": "string" }, "steps": [{
                "do": "trim.greet", "with": { "greet": { "salutation": "hi" } }, "as": "hello",
                "next": { "ty": 22, "shape": shape } }] })
        };
        let shouted = (
            "22-shouted.json",
            json!({ "ty": 22, "takes": { "hello": "any", "text": "string" } }),
        );

        let ok = docs(&[
            (
                "12.json",
                flow(json!({ "hello": "$hello", "text": "$text" })),
            ),
            shouted.clone(),
        ]);
        assert!(check(&ok, &signatures()).is_ok());

        let bad = docs(&[
            (
                "12.json",
                flow(json!({ "hello": "$greeting", "text": "$text" })),
            ),
            shouted,
        ]);
        assert_eq!(
            check(&bad, &signatures()).err().unwrap_or_default(),
            [
                "E5 12.json step 0 next: next to 22-shouted.json field hello: $greeting names nothing in scope"
            ]
        );
    }

    #[test]
    fn every_error_in_every_doc_is_listed() {
        let set = docs(&[
            (
                "a.json",
                json!({ "ty": 1, "takes": { "text": "string" },
                    "steps": [{ "do": "trim.nope", "next": { "ty": 99 } }] }),
            ),
            (
                "b.json",
                json!({ "ty": 2, "takes": { "text": "number" }, "steps": [{ "do": "upper" }] }),
            ),
            ("c.json", json!({ "steps": "none" })),
        ]);

        let found = check(&set, &signatures()).err().unwrap_or_default();

        let kinds: Vec<&str> = found.iter().map(|e| &e[..2]).collect();
        assert_eq!(kinds, ["E0", "E1", "E7", "E4"], "{found:?}");
    }

    #[test]
    fn signatures_that_do_not_parse_are_refused() {
        assert!(gather(&["[]", &json!([{ "action": "a" }]).to_string()]).is_ok());
        assert!(
            gather(&[&json!([{ "action": "a", "gave": {} }]).to_string()])
                .unwrap_err()
                .starts_with("signatures do not parse")
        );
    }
}
