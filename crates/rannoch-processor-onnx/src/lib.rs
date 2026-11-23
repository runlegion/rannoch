//! Flow steps that run a local ONNX model: a numeric model, a text embedding and a text
//! classifier. Every model runs through ort, or through tract when the `tract` feature is on.

use std::collections::HashMap;
use std::path::Path;

use rannoch_processor::{Action, Sig};
use serde_json::{Map, Value, json};

/// One one-dimensional input tensor.
enum Input {
    F32(Vec<f32>),
    I64(Vec<i64>),
}

/// The outputs of one run, by name, each flattened.
type Outputs = Vec<(String, Vec<f32>)>;

/// A loaded model file.
#[derive(Debug)]
pub struct Model {
    #[cfg(not(feature = "tract"))]
    session: std::sync::Mutex<ort::session::Session>,
    #[cfg(feature = "tract")]
    plan: tract_onnx::prelude::TypedRunnableModel<tract_onnx::prelude::TypedModel>,
    inputs: Vec<String>,
}

impl Model {
    /// # Errors
    /// The runtime's error, when the file does not load as a model.
    #[cfg(not(feature = "tract"))]
    pub fn open(path: &Path) -> Result<Self, String> {
        let builder = ort::session::Session::builder();
        let session = builder
            .and_then(|b| b.commit_from_file(path))
            .map_err(err)?;
        let inputs = session.inputs.iter().map(|i| i.name.clone()).collect();
        let session = std::sync::Mutex::new(session);
        Ok(Self { session, inputs })
    }

    /// # Errors
    /// The runtime's error, when the file does not load as a model.
    #[cfg(feature = "tract")]
    pub fn open(path: &Path) -> Result<Self, String> {
        use tract_onnx::prelude::*;
        let model = tract_onnx::onnx().model_for_path(path);
        let plan = model
            .and_then(InferenceModel::into_optimized)
            .and_then(TypedModel::into_runnable)
            .map_err(|e| format!("{e:?}"))?;
        let outlets = plan.model().input_outlets().map_err(|e| format!("{e:?}"))?;
        let node = |o: &OutletId| plan.model().node(o.node).name.clone();
        let inputs = outlets.iter().map(node).collect();
        Ok(Self { plan, inputs })
    }

    #[cfg(not(feature = "tract"))]
    fn run(&self, inputs: Vec<(String, Input)>) -> Result<Outputs, String> {
        use ort::value::Tensor;
        let tensor = |input| match input {
            Input::F32(v) => Tensor::from_array(([v.len()], v)).map(Tensor::into_dyn),
            Input::I64(v) => Tensor::from_array(([v.len()], v)).map(Tensor::into_dyn),
        };
        let values = inputs.into_iter().map(|(n, i)| tensor(i).map(|t| (n, t)));
        let values = values.collect::<ort::Result<Vec<_>>>().map_err(err)?;
        let mut session = self.session.lock().map_err(err)?;
        let outputs = session.run(values).map_err(err)?;
        let flat = |(name, value): (&str, ort::value::ValueRef<'_>)| {
            let (_, v) = value.try_extract_tensor::<f32>()?;
            Ok((name.to_owned(), v.to_vec()))
        };
        outputs
            .iter()
            .map(flat)
            .collect::<ort::Result<_>>()
            .map_err(err)
    }

    #[cfg(feature = "tract")]
    fn run(&self, inputs: Vec<(String, Input)>) -> Result<Outputs, String> {
        use tract_onnx::prelude::*;
        let model = self.plan.model();
        let mut by_name: HashMap<String, Input> = inputs.into_iter().collect();
        let mut values = TVec::new();
        for name in &self.inputs {
            let tensor = match by_name.remove(name).ok_or(format!("no input {name}"))? {
                Input::F32(v) => tract_ndarray::Array1::from(v).into_tensor(),
                Input::I64(v) => tract_ndarray::Array1::from(v).into_tensor(),
            };
            values.push(tensor.into());
        }
        let run = || -> TractResult<Outputs> {
            let outputs = self.plan.run(values)?;
            let outlets = model.output_outlets()?;
            let flat = |(outlet, value): (&OutletId, &TValue)| -> TractResult<_> {
                let label = model.outlet_label(*outlet);
                let name = label.unwrap_or(&model.node(outlet.node).name).to_owned();
                let v = value.to_array_view::<f32>()?.iter().copied().collect();
                Ok((name, v))
            };
            outlets.iter().zip(outputs.iter()).map(flat).collect()
        };
        run().map_err(|e| format!("{e:?}"))
    }
}

/// A numeric model as an action: each `takes` field (a number) goes in as a one-element f32
/// tensor of the same name; each `gives` field is the first element of the output of that name.
#[derive(Debug)]
pub struct Infer {
    sig: Sig,
    model: Model,
}

impl Infer {
    #[must_use]
    pub fn new(sig: Sig, model: Model) -> Self {
        Self { sig, model }
    }
}

impl Action for Infer {
    fn sig(&self) -> &Sig {
        &self.sig
    }

    fn run(&self, _: &Value, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let action = &self.sig.action;
        let mut inputs = Vec::new();
        for field in self.sig.takes.keys() {
            let x = input.get(field).and_then(Value::as_f64);
            let x = x.ok_or(format!("{action} takes {field}, a number"))?;
            #[expect(clippy::cast_possible_truncation, reason = "the model's input is f32")]
            inputs.push((field.clone(), Input::F32(vec![x as f32])));
        }
        let outputs = self.model.run(inputs)?;
        let first = |field: &String| {
            let output = outputs.iter().find(|(name, _)| name == field);
            let x = output.and_then(|(_, v)| v.first());
            let x = x.ok_or(format!("{action}'s model gives no output {field}"))?;
            Ok((field.clone(), json!(f64::from(*x))))
        };
        self.sig.gives.keys().map(first).collect()
    }
}

/// takes text, gives vector (`list<number>`): the model2vec embedding, L2-normalized.
#[derive(Debug)]
pub struct Embed {
    sig: Sig,
    model: Model,
    vocab: HashMap<String, i64>,
}

impl Embed {
    /// # Errors
    /// When `tokenizer_json` does not parse or holds no `model.vocab`.
    pub fn new(sig: Sig, model: Model, tokenizer_json: &str) -> Result<Self, String> {
        let tokenizer: Value = serde_json::from_str(tokenizer_json).map_err(err)?;
        let vocab = tokenizer["model"]["vocab"].as_object();
        let vocab = vocab.ok_or("tokenizer.json holds no model.vocab")?;
        let id = |(k, v): (&String, &Value)| Some((k.clone(), v.as_i64()?));
        let vocab = vocab.iter().filter_map(id).collect();
        Ok(Self { sig, model, vocab })
    }

    /// The embedding of `text`: `input_ids`, and `offsets` [0] when the model declares it.
    fn embed(&self, text: &str) -> Result<Vec<f64>, String> {
        let ids = Input::I64(tokenize(&self.vocab, text));
        let mut inputs = vec![("input_ids".to_owned(), ids)];
        if self.model.inputs.iter().any(|i| i == "offsets") {
            inputs.push(("offsets".to_owned(), Input::I64(vec![0])));
        }
        let outputs = self.model.run(inputs)?;
        let first = outputs.into_iter().next();
        Ok(normalize(&first.ok_or("the model gives no output")?.1))
    }
}

impl Action for Embed {
    fn sig(&self) -> &Sig {
        &self.sig
    }

    fn run(&self, _: &Value, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let vector = self.embed(text(input))?;
        Ok(Map::from_iter([("vector".to_owned(), json!(vector))]))
    }
}

/// takes text, gives label (string): the label whose description's embedding is nearest by cosine.
#[derive(Debug)]
pub struct Classify {
    sig: Sig,
    embed: Embed,
    labels: Vec<(String, Vec<f64>)>,
}

impl Classify {
    /// # Errors
    /// The failure to embed a label's description.
    pub fn new(sig: Sig, embed: Embed, labels: &[(&str, &str)]) -> Result<Self, String> {
        let label = |(label, about): &(&str, &str)| Ok(((*label).to_owned(), embed.embed(about)?));
        let labels = labels.iter().map(label).collect::<Result<_, String>>()?;
        Ok(Self { sig, embed, labels })
    }
}

impl Action for Classify {
    fn sig(&self) -> &Sig {
        &self.sig
    }

    fn run(&self, _: &Value, input: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let vector = self.embed.embed(text(input))?;
        let label = nearest(&vector, &self.labels);
        Ok(Map::from_iter([("label".to_owned(), json!(label))]))
    }
}

fn text(input: &Map<String, Value>) -> &str {
    input.get("text").and_then(Value::as_str).unwrap_or("")
}

#[expect(clippy::needless_pass_by_value, reason = "the shape map_err passes")]
fn err(e: impl ToString) -> String {
    e.to_string()
}

/// The label whose vector is nearest `v` by cosine.
fn nearest<'a>(v: &[f64], labels: &'a [(String, Vec<f64>)]) -> &'a str {
    let norm = |x: &[f64]| x.iter().map(|a| a * a).sum::<f64>().sqrt();
    let cos = |l: &[f64]| v.iter().zip(l).map(|(a, b)| a * b).sum::<f64>() / (norm(v) * norm(l));
    let best = labels.iter().max_by(|a, b| cos(&a.1).total_cmp(&cos(&b.1)));
    best.map_or("", |(label, _)| label)
}

fn normalize(v: &[f32]) -> Vec<f64> {
    let n = v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>();
    let n = n.sqrt().max(1e-12);
    v.iter().map(|x| f64::from(*x) / n).collect()
}

/// Lowercase, split on whitespace and punctuation, word pieces over `vocab`, unknowns dropped,
/// no special tokens, as model2vec feeds its static model.
fn tokenize(vocab: &HashMap<String, i64>, text: &str) -> Vec<i64> {
    let mut words = vec![String::new()];
    for ch in text.to_lowercase().chars() {
        if ch.is_ascii_punctuation() {
            words.extend([ch.to_string(), String::new()]);
        } else if ch.is_whitespace() {
            words.push(String::new());
        } else if let Some(word) = words.last_mut() {
            word.push(ch);
        }
    }
    let mut ids = Vec::new();
    for word in words {
        let chars: Vec<char> = word.chars().collect();
        let (mut start, mut pieces) = (0, Vec::new());
        while start < chars.len() {
            let piece = (start + 1..=chars.len()).rev().find_map(|end| {
                let s: String = chars[start..end].iter().collect();
                let s = if start > 0 { format!("##{s}") } else { s };
                vocab.get(&s).map(|id| (*id, end))
            });
            let Some((id, end)) = piece else {
                pieces.clear();
                break;
            };
            pieces.push(id);
            start = end;
        }
        ids.extend(pieces);
    }
    ids
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::path::Path;

    use rannoch_processor::{Action, Sig};
    use serde_json::{Map, json};

    use super::{Infer, Model, nearest, tokenize};

    #[test]
    fn tokenize_splits_punctuation_and_word_pieces_and_drops_unknowns() {
        let vocab = [
            ("the", 1),
            ("app", 2),
            ("crash", 3),
            ("##es", 4),
            (",", 5),
            ("ok", 6),
        ];
        let vocab: HashMap<String, i64> = vocab.iter().map(|(k, v)| ((*k).into(), *v)).collect();
        assert_eq!(
            tokenize(&vocab, "The app crashes, ok? zzz"),
            [1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn nearest_picks_the_label_nearest_by_cosine() {
        let labels = [("bug", vec![3.0, 0.0]), ("question", vec![0.0, 1.0])];
        let labels: Vec<(String, Vec<f64>)> =
            labels.into_iter().map(|(l, v)| (l.into(), v)).collect();
        assert_eq!(nearest(&[10.0, 1.0], &labels), "bug");
        assert_eq!(nearest(&[1.0, 1.2], &labels), "question");
    }

    #[test]
    fn infer_on_add_gives_the_sum() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/models/add.onnx");
        let number = |f: &str| (f.to_owned(), "number".to_owned());
        let sig = Sig {
            action: "add".into(),
            takes: BTreeMap::from([number("a"), number("b")]),
            gives: BTreeMap::from([number("y")]),
            ..Sig::default()
        };
        let add = Infer::new(sig, Model::open(&path).unwrap());
        let out = add.run(
            &json!({}),
            &Map::from_iter([("a".into(), json!(2.5)), ("b".into(), json!(4.0))]),
        );
        assert_eq!(out.unwrap()["y"], json!(6.5));
    }
}
