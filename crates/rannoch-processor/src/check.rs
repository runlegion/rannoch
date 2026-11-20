use std::path::Path;

use serde_json::{Map, Value};

use crate::{FlowDoc, Next, Sig, type_expr, type_of};

/// Reads every flow doc in `dir`, parses `signatures` (each a processor crate's SIGNATURES,
/// the app's own included) and checks the docs. On success writes the flows, each with `job`
/// set, as one JSON array to `$OUT_DIR/flows.json` for the app to `include_str!`. On failure
/// prints every error to stderr, one per line, and panics, which fails the build.
/// # Panics
/// On any error in the docs, and on a doc, a signatures export or `dir` that cannot be read.
pub fn compile(dir: &Path, signatures: &[&str]) {
    println!("cargo:rerun-if-changed={}", dir.display());
    let mut sigs = Vec::new();
    for export in signatures {
        let some: Vec<Sig> =
            serde_json::from_str(export).unwrap_or_else(|e| panic!("signatures: {e}"));
        sigs.extend(some);
    }
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    let mut paths: Vec<_> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    let mut docs = Vec::new();
    for path in paths
        .iter()
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
    {
        let at = path.display();
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{at}: {e}"));
        let doc = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{at}: {e}"));
        let file = path.file_name().unwrap_or_default().to_string_lossy();
        docs.push((file.into_owned(), doc));
    }
    match check(&docs, &sigs) {
        Ok(flows) => {
            let out = std::env::var("OUT_DIR").unwrap_or_else(|e| panic!("OUT_DIR: {e}"));
            let out = Path::new(&out).join("flows.json");
            std::fs::write(&out, flows).unwrap_or_else(|e| panic!("{}: {e}", out.display()));
        }
        Err(errors) => {
            for error in &errors {
                eprintln!("{error}");
            }
            panic!(
                "{} errors in the flow docs in {}",
                errors.len(),
                dir.display()
            );
        }
    }
}

/// The flows as the JSON array to compile in, or every error.
/// # Errors
/// Every error in every doc, one line each, `E<n> <file> step <i> <link name | next>: <what>`.
#[expect(clippy::too_many_lines, reason = "every check of a doc, in its order")]
pub fn check(docs: &[(String, FlowDoc)], sigs: &[Sig]) -> Result<String, Vec<String>> {
    let (mut errors, mut flows, empty) = (Vec::new(), Vec::new(), Value::Object(Map::new()));
    for (file, flow) in docs {
        let mut flow = FlowDoc {
            job: false,
            ..flow.clone()
        };
        let mut scope = flow.takes.clone();
        for (i, step) in flow.steps.iter().enumerate() {
            let mut seen = scope.clone();
            for link in step.chain.split('.') {
                let at = format!("{file} step {i} {link}");
                let Some(sig) = sigs.iter().find(|s| s.action == link) else {
                    errors.push(format!("E1 {at}: no action named {link}"));
                    continue;
                };
                flow.job |= sig.job;
                let with = step.with.get(link).unwrap_or(&empty).as_object();
                let why = with.map_or(Some("not an object".to_owned()), |with| {
                    let unknown = with.keys().find(|k| !sig.options.contains_key(*k));
                    let unknown = unknown.map(|k| format!("unknown field `{k}`"));
                    unknown.or_else(|| {
                        sig.options
                            .iter()
                            .find_map(|(k, ty)| match with.get(k).map(type_of) {
                                None => Some(format!("missing field `{k}`")),
                                Some(is) if is != ty => Some(format!("{k} is {is}, not {ty}")),
                                Some(_) => None,
                            })
                    })
                });
                if let Some(why) = why {
                    errors.push(format!(
                        "E2 {at}: the options for {link} do not parse: {why}"
                    ));
                }
                for (field, ty) in &sig.takes {
                    match seen.get(field) {
                        None => errors.push(format!(
                            "E3 {at}: {link} takes {field}, which nothing before it gives"
                        )),
                        Some(is) if is != ty => errors
                            .push(format!("E4 {at}: {link} takes {field} as {ty}, it is {is}")),
                        Some(_) => {}
                    }
                }
                seen.extend(sig.gives.clone());
            }
            match &step.under {
                Some(field) => drop(scope.insert(field.clone(), "any".to_owned())),
                None => scope = seen,
            }
            let at = format!("{file} step {i} next");
            let tos = match &step.next {
                None => continue,
                Some(Next::To(to)) => vec![(to, None)],
                Some(Next::Branch { branch }) => {
                    let (on, cases) = (&branch.on, &branch.cases);
                    let ty = scope.get(on).map_or("", String::as_str);
                    if ty.is_empty() {
                        errors.push(format!("E3 {at}: branch on {on}, which nothing gives"));
                    }
                    let bad = cases.iter().map(|c| &c.when);
                    let bad = bad.filter(|w| !ty.is_empty() && type_of(w) != ty);
                    let e4 = |w| format!("E4 {at}: branch on {on} as {ty}, a case when {w}");
                    errors.extend(bad.map(e4));
                    let tos = cases.iter().map(|c| &c.next);
                    tos.chain([&branch.otherwise]).map(|t| (t, None)).collect()
                }
                Some(Next::Wait { wait }) => {
                    let (ty, key) = (wait.for_ty, &wait.key);
                    let got = docs.iter().find(|d| d.1.ty == ty).map(|d| &d.1.takes);
                    let e7 = format!("E7 {at}: wait for ty {ty}, which no flow takes");
                    match got.map(|g| (type_expr(key, &scope), type_expr(key, g))) {
                        None => errors.push(e7),
                        Some((Ok(a), Ok(b))) if a == b => {}
                        Some((a, b)) => errors.push(format!(
                            "E3 {at}: wait key {key} does not render the same on both sides \
                            ({a:?} / {b:?})"
                        )),
                    }
                    let late = wait.late.iter().map(|l| (l, got));
                    let tos = [(&wait.next, got), (&wait.timeout, None)];
                    tos.into_iter().chain(late).collect()
                }
            };
            for (to, got) in tos {
                let mut scope = scope.clone();
                let got = got.into_iter().flatten();
                scope.extend(got.map(|(f, t)| (format!("got.{f}"), t.clone())));
                if let Some(l) = &to.each {
                    let ty = scope.get(l).cloned().unwrap_or_default();
                    match ty.strip_prefix("list<").and_then(|t| t.strip_suffix('>')) {
                        Some(item) => drop(scope.insert("item".to_owned(), item.to_owned())),
                        None => errors.push(format!("E4 {at}: each over {l}, which is not a list")),
                    }
                }
                let Some((_, to_flow)) = docs.iter().find(|(_, f)| f.ty == to.ty) else {
                    errors.push(format!(
                        "E7 {at}: the next goes to ty {}, which no flow takes",
                        to.ty
                    ));
                    continue;
                };
                let name = &to_flow.name;
                for (field, takes) in &to_flow.takes {
                    match to.shape.get(field).map(|e| type_expr(e, &scope)) {
                        None => errors.push(format!(
                            "E5 {at}: the next to {name} gives no {field}, which it takes"
                        )),
                        Some(Err(why)) => errors.push(format!("E5 {at}: {why}")),
                        Some(Ok(gives)) if &gives != takes => errors.push(format!(
                            "E6 {at}: the next to {name} gives {field} as {gives}, it takes {takes}"
                        )),
                        Some(Ok(_)) => {}
                    }
                }
            }
        }
        flows.push(flow);
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    serde_json::to_string(&flows).map_err(|e| vec![e.to_string()])
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::{FlowDoc, Sig};

    fn sigs() -> Vec<Sig> {
        serde_json::from_value(json!([
            { "action": "trim", "takes": { "text": "string" }, "gives": { "text": "string" } },
            { "action": "upper", "takes": { "text": "string" }, "gives": { "text": "string" } },
            { "action": "greet", "options": { "salutation": "string" },
              "takes": { "text": "string" }, "gives": { "greeting": "string" } },
            { "action": "limit", "options": { "max": "number" }, "takes": { "text": "string" } },
            { "action": "index", "takes": { "text": "string" }, "job": true },
        ]))
        .unwrap()
    }

    fn notice() -> Value {
        json!({ "name": "notice", "ty": 20, "takes": { "msg": "string", "who": "string" },
            "steps": [] })
    }

    fn flow(takes: &Value, steps: &Value) -> Value {
        json!({ "name": "a", "ty": 1, "takes": takes, "steps": steps })
    }

    fn check(docs: &[(&str, Value)]) -> Result<String, Vec<String>> {
        let docs: Vec<(String, FlowDoc)> = docs
            .iter()
            .map(|(f, d)| ((*f).to_owned(), serde_json::from_value(d.clone()).unwrap()))
            .collect();
        super::check(&docs, &sigs())
    }

    /// The errors of a.json, taking text as `text` and running `steps`, beside notice.
    fn errors(text: &str, steps: &Value) -> Vec<String> {
        let a = flow(&json!({ "text": text }), steps);
        check(&[("a.json", a), ("n.json", notice())]).unwrap_err()
    }

    #[test]
    fn e1_a_link_naming_no_action() {
        let errors = errors("string", &json!([{ "do": "trim.nope" }]));
        assert_eq!(errors, ["E1 a.json step 0 nope: no action named nope"]);
    }

    #[test]
    fn e2_options_with_an_unknown_field() {
        let steps = json!([{ "do": "greet", "with": { "greet": { "salut": "hi" } } }]);
        let want =
            "E2 a.json step 0 greet: the options for greet do not parse: unknown field `salut`";
        assert_eq!(errors("string", &steps), [want]);
    }

    #[test]
    fn e3_a_link_taking_a_field_nothing_gives() {
        let a = flow(&json!({ "x": "number" }), &json!([{ "do": "upper" }]));
        let want = "E3 a.json step 0 upper: upper takes text, which nothing before it gives";
        assert_eq!(check(&[("a.json", a)]).unwrap_err(), [want]);
    }

    #[test]
    fn e4_a_link_taking_a_field_of_another_type() {
        let want = "E4 a.json step 0 upper: upper takes text as string, it is number";
        assert_eq!(errors("number", &json!([{ "do": "upper" }])), [want]);
    }

    #[test]
    fn e5_a_next_missing_a_field_the_flow_takes() {
        let steps = json!([{ "do": "upper", "next": { "ty": 20, "shape": { "msg": "$text" } } }]);
        let want = "E5 a.json step 0 next: the next to notice gives no who, which it takes";
        assert_eq!(errors("string", &steps), [want]);
    }

    #[test]
    fn e5_a_concat_naming_a_field_nothing_in_scope_gives() {
        let shape = json!({ "msg": { "$concat": ["$greeting", "!"] }, "who": "x" });
        let steps = json!([{ "do": "upper", "next": { "ty": 20, "shape": shape } }]);
        let want = "E5 a.json step 0 next: $greeting names nothing in scope";
        assert_eq!(errors("string", &steps), [want]);
    }

    #[test]
    fn e6_a_next_giving_a_field_of_another_type() {
        let shape = json!({ "msg": "$text", "who": 5 });
        let steps = json!([{ "do": "upper", "next": { "ty": 20, "shape": shape } }]);
        let want = "E6 a.json step 0 next: the next to notice gives who as number, it takes string";
        assert_eq!(errors("string", &steps), [want]);
    }

    #[test]
    fn e7_a_next_to_a_ty_no_flow_takes() {
        let steps = json!([{ "do": "upper", "next": { "ty": 99 } }]);
        let want = "E7 a.json step 0 next: the next goes to ty 99, which no flow takes";
        assert_eq!(errors("string", &steps), [want]);
    }

    #[test]
    fn three_errors_in_two_docs_are_all_returned() {
        let a = flow(
            &json!({ "text": "string" }),
            &json!([{ "do": "trim.nope", "next": { "ty": 99 } }]),
        );
        let b = json!({ "name": "b", "ty": 2, "takes": { "text": "number" },
            "steps": [{ "do": "upper" }] });
        let want = [
            "E1 a.json step 0 nope: no action named nope",
            "E7 a.json step 0 next: the next goes to ty 99, which no flow takes",
            "E4 b.json step 0 upper: upper takes text as string, it is number",
        ];
        assert_eq!(check(&[("a.json", a), ("b.json", b)]).unwrap_err(), want);
    }

    #[test]
    fn a_valid_set_compiles_with_job_set_on_the_flow_using_a_job_action() {
        let shape = json!({ "msg": { "$concat": ["$greeting", "!"] },
            "who": { "$": "$nick", "or": "friend" } });
        let greet = json!([{ "do": "trim.upper.greet", "with": { "greet": { "salutation": "hi" } },
            "next": { "ty": 20, "shape": shape } }]);
        let index = json!({ "name": "index", "ty": 30, "takes": { "text": "string" },
            "steps": [{ "do": "trim.index", "as": "got" }] });
        let docs = [
            ("a.json", flow(&json!({ "text": "string" }), &greet)),
            ("n.json", notice()),
            ("i.json", index),
        ];
        let flows: Vec<Value> = serde_json::from_str(&check(&docs).unwrap()).unwrap();
        let jobs: Vec<String> = flows
            .iter()
            .map(|f| format!("{} {}", f["ty"], f["job"]))
            .collect();
        assert_eq!(jobs, ["1 false", "20 false", "30 true"]);
    }
    #[test]
    fn e3_a_branch_on_a_field_nothing_gives() {
        let to = json!({ "ty": 20, "shape": { "msg": "$text", "who": "x" } });
        let branch = json!({ "on": "level", "cases": [], "else": to });
        let steps = json!([{ "do": "upper", "next": { "branch": branch } }]);
        let want = "E3 a.json step 0 next: branch on level, which nothing gives";
        assert_eq!(errors("string", &steps), [want]);
    }

    #[test]
    fn e4_each_over_a_non_list() {
        let next = json!({ "ty": 20, "each": "text", "shape": { "msg": "$text", "who": "x" } });
        let steps = json!([{ "do": "upper", "next": next }]);
        let want = "E4 a.json step 0 next: each over text, which is not a list";
        assert_eq!(errors("string", &steps), [want]);
    }
}
