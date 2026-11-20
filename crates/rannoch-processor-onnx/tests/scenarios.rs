//! FR-RANNOCH-121's scenarios (`@criterion-FR-RANNOCH-121-c1` to `-c6`) over the test app of
//! `tests/app`, its flows run by rannoch-processor over the real store and queue. c3 and c6 run
//! with the `tract` feature, c5 without it; potion-base-8M is fetched into the target dir.

use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc, mpsc::Sender};
use std::time::{Duration, Instant};
use std::{fs, process::Command, thread};

use rannoch_core::forgr::_events;
use rannoch_core::{Append, Config, Rannoch};
use rannoch_events::Events;
use rannoch_processor::{Action, Processor, Sig};
use rannoch_processor_onnx::{Classify, Embed, Infer, Model};
use rannoch_queue::{Flows, Queue, Settings};
use rannoch_store::{Opened, Store};
use rusqlite::Connection;
use serde_json::{Value, json};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const ADD: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/models/add.onnx");
const TEXTS: [&str; 5] = [
    "the app crashes when I click save",
    "how do I export my notes?",
    "thank you, this tool is wonderful",
    "Rannoch keeps every event forever.",
    "a b c",
];
const LABELS: [(&str, &str); 3] = [
    ("bug", "the app crashed with an error and is broken"),
    ("question", "how do I do this, can you explain"),
    ("praise", "thank you, this is great and I love it"),
];

fn tmp(name: &str) -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR")).join(name)
}

fn ready(path: &Path) -> Result<Connection> {
    let Opened::Ready(conn) = rannoch_store::open(path, &rannoch_core::migrator())? else {
        return Err("migrations pending".into());
    };
    Ok(conn)
}

fn sig(action: &str) -> Result<Sig> {
    let sigs: Vec<Sig> = serde_json::from_str(include_str!("app/signatures.json"))?;
    let sig = sigs.into_iter().find(|s| s.action == action);
    sig.ok_or(format!("no sig {action}").into())
}

/// The test app's flows, checked against its `signatures.json` as its build checks them.
fn flows() -> Result<String> {
    let mut docs = Vec::new();
    for entry in fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/app/flows"))? {
        let entry = entry?;
        let doc = serde_json::from_str(&fs::read_to_string(entry.path())?)?;
        docs.push((entry.file_name().to_string_lossy().into_owned(), doc));
    }
    docs.sort_by(|a, b| a.0.cmp(&b.0));
    let sigs: Vec<Sig> = serde_json::from_str(include_str!("app/signatures.json"))?;
    rannoch_processor::check(&docs, &sigs).map_err(|e| e.join("\n").into())
}

/// The app: its database, the queue's signal, rannoch, the queue, and rannoch-processor.
struct App(
    Connection,
    Sender<()>,
    Rannoch<u16, Value>,
    Queue,
    Arc<Processor<u16, Value>>,
);

impl App {
    /// The app over a fresh store, every migration applied by opening rannoch once.
    fn start(name: &str, actions: Vec<Arc<dyn Action>>) -> Result<Self> {
        drop(fs::remove_dir_all(tmp(name)));
        fs::create_dir_all(tmp(name))?;
        let path = tmp(name).join("store.db");
        let store = Arc::new(Store::new(path.clone(), rannoch_core::migrator()));
        let config = |append| Config {
            store: Some(store.clone()),
            append,
            ..Config::default()
        };
        Rannoch::<u16, Value>::open(config(None))?.shutdown();
        let processor = Arc::new(Processor::new(&flows()?, actions, ready(&path)?)?);
        let (signal, wake) = mpsc::channel();
        let events = Events::new(ready(&path)?, signal.clone());
        let rannoch = Rannoch::open(config(Some(Arc::new(events) as Arc<dyn Append<_, _>>)))?;
        let (flows, settings) = (processor.clone(), Settings::new(Duration::from_millis(100)));
        let queue = Queue::start(ready(&path)?, wake, rannoch.sender(), flows, settings);
        Ok(App(ready(&path)?, signal, rannoch, queue, processor))
    }

    /// The (ty, data) of every next sent about an event of `ty` with `data`, from the app's
    /// forgr factory, once its flow has sent its 2.
    fn hand(&self, ty: i64, data: &Value) -> Result<Vec<(i64, Value)>> {
        let id = Uuid::now_v7().to_string();
        let row = _events::factory().id(id.clone()).ty(ty);
        row.data(data.to_string()).status(0).insert(&self.0)?;
        self.1.send(())?;
        let sql = "SELECT ty, status, data FROM _events WHERE source = ?1 ORDER BY id";
        let (mut select, start) = (self.0.prepare(sql)?, Instant::now());
        loop {
            let rows = select.query_map([&id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            let rows: Vec<(i64, i64, String)> = rows.collect::<rusqlite::Result<_>>()?;
            if rows.iter().any(|r| r.1 == 2) {
                let next = |r: (i64, i64, String)| Ok((r.0, serde_json::from_str(&r.2)?));
                return rows.into_iter().filter(|r| r.1 == 0).map(next).collect();
            }
            assert!(start.elapsed() < Duration::from_secs(10), "{ty} {data} ran");
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// The `field` of the one next the flow for `ty` sends about `text`.
    fn ask(&self, ty: i64, text: &str, field: &str) -> Result<Value> {
        let next = self.hand(ty, &json!({ "text": text }))?;
        Ok(next.first().map_or(Value::Null, |n| n.1[field].clone()))
    }

    fn stop(self) {
        drop(self.1);
        self.2.shutdown();
        self.3.join();
    }
}

/// The app's embed and classify, each over the model file `model`.
fn text_actions(model: &Path, tokenizer: &str) -> Result<Vec<Arc<dyn Action>>> {
    let embed = || -> Result<_> { Ok(Embed::new(sig("embed")?, Model::open(model)?, tokenizer)?) };
    let classify = Classify::new(sig("classify")?, embed()?, &LABELS)?;
    Ok(vec![Arc::new(embed()?), Arc::new(classify)])
}

/// The potion-base-8M files, fetched from Hugging Face into the target dir once.
fn potion() -> Result<PathBuf> {
    fs::create_dir_all(tmp("potion/onnx"))?;
    for file in ["model.safetensors", "tokenizer.json", "onnx/model.onnx"] {
        let (to, part) = (tmp("potion").join(file), tmp("potion.part"));
        let url = format!("https://huggingface.co/minishlab/potion-base-8M/resolve/main/{file}");
        let args = ["-sSfL", "-o", &part.display().to_string(), &url];
        if !to.exists() && !Command::new("curl").args(args).status()?.success() {
            return Err(format!("could not fetch {url}").into());
        } else if !to.exists() {
            fs::rename(&part, to)?;
        }
    }
    Ok(tmp("potion"))
}

/// Runs cargo with `args` on the test app of `tests/app`, the workspace's Cargo.lock copied in:
/// its stdout, or its stderr as the error.
fn cargo(args: &[&str]) -> Result<String> {
    let app = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/app");
    fs::copy(app.join("../../../../Cargo.lock"), app.join("Cargo.lock"))?;
    let mut cargo = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    cargo.current_dir(app).args(args);
    let out = cargo.env("CARGO_TARGET_DIR", tmp("app-target")).output()?;
    if !out.status.success() {
        return Err(String::from_utf8(out.stderr)?.into());
    }
    Ok(String::from_utf8(out.stdout)?)
}

#[test]
fn a_flow_step_runs_a_local_model() -> Result<()> {
    // Given an app that does not enable the onnx feature
    // When the app is built
    // Then rannoch-processor-onnx is not built
    let tree = |more: &[&str]| cargo(&[&["tree", "-p", "app", "-e", "normal"], more].concat());
    assert!(!tree(&[])?.contains("rannoch-processor-onnx v"));
    let control = tree(&["--features", "onnx"])?;
    assert!(control.contains("rannoch-processor-onnx v"), "{control}");

    // Given an app that enables the onnx feature, with a flow whose steps name embed and
    // classify on text
    // When the app is built
    // Then the build succeeds, and the flow reports a job step
    cargo(&["build", "-q", "-p", "app", "--features", "onnx"])?;
    let ran = String::from_utf8(Command::new(tmp("app-target/debug/app")).output()?.stdout)?;
    assert!(ran.lines().any(|l| l == "40 true"), "{ran}");

    // Given an app that enables the onnx feature, built with a flow whose step runs the
    // one-node Add model, kept as a local file
    // Then the flow reports a job step
    let add = Infer::new(sig("add")?, Model::open(Path::new(ADD))?);
    let app = App::start("runs_a_local_model", vec![Arc::new(add)])?;
    assert!(app.4.flow(10).is_some_and(|f| f.job));

    // When the queue hands rannoch-processor an event whose inputs are 2.5 and 4.0
    // Then the step's next carries the sum 6.5
    let nexts = app.hand(10, &json!({ "a": 2.5, "b": 4.0 }))?;
    assert_eq!(nexts, [(11, json!({ "sum": 6.5 }))]);
    app.stop();
    Ok(())
}

#[cfg(not(feature = "tract"))]
#[test]
fn a_flow_embeds_and_classifies_text_with_a_local_model() -> Result<()> {
    // Given an app that enables the onnx feature, built with flows whose steps embed and
    // classify text with the potion-base-8M model, kept as a local file, classify's labels
    // being bug, question and praise, each with a description
    let potion = potion()?;
    let tokenizer = fs::read_to_string(potion.join("tokenizer.json"))?;
    let actions = text_actions(&potion.join("onnx/model.onnx"), &tokenizer)?;
    let app = App::start("embeds_and_classifies", actions)?;

    // When the queue hands rannoch-processor an event to embed a text, twice
    // Then each next carries a vector of 256 numbers, identical both times
    // (the processor checks each next's vector is a list<number>, as embed's sig gives)
    let once = app.ask(20, TEXTS[0], "vector")?;
    assert_eq!(once.as_array().map(Vec::len), Some(256));
    assert_eq!(app.ask(20, TEXTS[0], "vector")?, once);

    // When the queue hands rannoch-processor an event to embed a different text
    // Then its vector differs
    assert_ne!(app.ask(20, TEXTS[1], "vector")?, once);

    // When the queue hands rannoch-processor events to classify 'the app crashes when I click
    // save', 'how do I export my notes?' and 'thank you, this tool is wonderful'
    // Then the nexts carry bug, question and praise
    let label = |t: &&str| app.ask(30, t, "label");
    let labels: Vec<Value> = TEXTS[..3].iter().map(label).collect::<Result<_>>()?;
    assert_eq!(labels, ["bug", "question", "praise"]);
    app.stop();
    Ok(())
}

#[cfg(feature = "tract")]
#[test]
fn a_local_model_gives_the_same_answers_through_tract_as_through_ort() -> Result<()> {
    // Given an app that enables the onnx feature with the tract feature, built with a flow
    // whose step runs the one-node Add model, kept as a local file
    let add = Infer::new(sig("add")?, Model::open(Path::new(ADD))?);
    let app = App::start("same_answers", vec![Arc::new(add)])?;

    // When the queue hands rannoch-processor events whose inputs are (2.5, 4.0), (-1.0, 0.25),
    // (1e6, 3.0) and (0, 0)
    // Then each step's next carries the sum of its two inputs, the same answer the ort build
    // gives
    for (a, b) in [(2.5f32, 4.0f32), (-1.0, 0.25), (1e6, 3.0), (0.0, 0.0)] {
        let nexts = app.hand(10, &json!({ "a": a, "b": b }))?;
        let by_ort = ort(Path::new(ADD), vec![("a", vec![a]), ("b", vec![b])])?[0];
        assert_eq!(by_ort, a + b, "{a} + {b} through ort");
        assert_eq!(nexts, [(11, json!({ "sum": by_ort }))], "{a} + {b}");
    }
    app.stop();
    Ok(())
}

/// The first output of `model` through ort on `inputs`: what the onnx feature's build gives.
#[cfg(feature = "tract")]
fn ort<T: ort::tensor::PrimitiveTensorElementType + std::fmt::Debug + Clone + 'static>(
    model: &Path,
    inputs: Vec<(&'static str, Vec<T>)>,
) -> Result<Vec<f32>> {
    let mut session = ort::session::Session::builder()?.commit_from_file(model)?;
    let tensor = |(k, v): (_, Vec<T>)| Ok((k, ort::value::Tensor::from_array(([v.len()], v))?));
    let inputs: ort::Result<Vec<_>> = inputs.into_iter().map(tensor).collect();
    let outputs = session.run(inputs?)?;
    Ok(outputs[0].try_extract_tensor::<f32>()?.1.to_vec())
}

/// potion-base-8M as a Loop-free export, written from `model.safetensors` with tract-onnx's
/// protobuf types: Gather of the [29528, 256] matrix by `input_ids`, `ReduceMean` over the
/// tokens, then `ReduceL2` and Div, as its config's normalize asks; opset 13.
#[cfg(feature = "tract")]
#[expect(clippy::field_reassign_with_default)]
fn export(potion: &Path) -> Result<PathBuf> {
    use tract_onnx::pb::tensor_shape_proto::{Dimension, dimension::Value::DimParam};
    use tract_onnx::pb::{attribute_proto::AttributeType, tensor_proto::DataType, *};
    let bytes = fs::read(potion.join("model.safetensors"))?;
    let n = 8 + usize::try_from(u64::from_le_bytes(bytes[..8].try_into()?))?;
    let header: Value = serde_json::from_slice(&bytes[8..n])?;
    let ints = |k: &str| serde_json::from_value::<Vec<i64>>(header["embeddings"][k].clone());
    let (dims, at) = (ints("shape")?, ints("data_offsets")?);
    let at = |i: usize| usize::try_from(at[i]).map(|o| n + o);
    // Gather's axis 0 and the reductions' keepdims 1 are ONNX's defaults.
    let axes = |axis: i64| AttributeProto {
        name: "axes".into(),
        ints: vec![axis],
        r#type: AttributeType::Ints as i32,
        ..AttributeProto::default()
    };
    let node = |op: &str, input: &str, out: &str, attribute| NodeProto {
        input: input.split(' ').map(str::to_owned).collect(),
        output: vec![out.into()],
        op_type: op.into(),
        attribute,
        ..NodeProto::default()
    };
    let mut dim = Dimension::default();
    dim.value = Some(DimParam("n".into()));
    let shape = Some(TensorShapeProto { dim: vec![dim] });
    let elem_type = DataType::Int64 as i32;
    let tensor = type_proto::Tensor { elem_type, shape };
    let mut ty = TypeProto::default();
    ty.value = Some(type_proto::Value::TensorType(tensor));
    let input_ids = ValueInfoProto {
        name: "input_ids".into(),
        r#type: Some(ty),
        ..ValueInfoProto::default()
    };
    let mut y = ValueInfoProto::default();
    y.name = "y".into();
    let graph = GraphProto {
        node: vec![
            node("Gather", "E input_ids", "g", vec![]),
            node("ReduceMean", "g", "m", vec![axes(0)]),
            node("ReduceL2", "m", "nrm", vec![axes(1)]),
            node("Div", "m nrm", "y", vec![]),
        ],
        initializer: vec![TensorProto {
            dims,
            data_type: DataType::Float as i32,
            name: "E".into(),
            raw_data: bytes[at(0)?..at(1)?].to_vec(),
            ..TensorProto::default()
        }],
        input: vec![input_ids],
        output: vec![y],
        ..GraphProto::default()
    };
    let mut opset = OperatorSetIdProto::default();
    opset.version = 13;
    let model = ModelProto {
        ir_version: 8,
        opset_import: vec![opset],
        graph: Some(graph),
        ..ModelProto::default()
    };
    let path = potion.join("export.onnx");
    fs::write(&path, prost::Message::encode_to_vec(&model))?;
    Ok(path)
}

/// The word-piece ids of `text`, as the onnx feature's build feeds them, for the ort reference.
#[cfg(feature = "tract")]
fn ids(vocab: &Value, text: &str) -> Vec<i64> {
    let pad = |c: char| {
        let space = c.is_ascii_punctuation().then_some(' ');
        [space, Some(c), space].into_iter().flatten()
    };
    let (spaced, mut ids): (String, _) =
        (text.to_lowercase().chars().flat_map(pad).collect(), vec![]);
    for word in spaced.split_whitespace() {
        let chars: Vec<char> = word.chars().collect();
        let (mut start, mut pieces) = (0, Vec::new());
        while start < chars.len() {
            let piece = (start + 1..=chars.len()).rev().find_map(|end| {
                let s: String = chars[start..end].iter().collect();
                let s = if start > 0 { format!("##{s}") } else { s };
                vocab[s.as_str()].as_i64().map(|id| (id, end))
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

#[cfg(feature = "tract")]
#[test]
fn a_tract_build_runs_the_model_shipped_as_an_export_tract_can_load() -> Result<()> {
    // Given the potion-base-8M model shipped as a Loop-free export, Gather and ReduceMean with
    // the L2 step its config's normalize asks for, kept as a local file
    let potion = potion()?;
    let export = export(&potion)?;
    // And one app built with the onnx feature and one built with the tract feature, each with
    // flows that embed and classify text with that export
    let tokenizer = fs::read_to_string(potion.join("tokenizer.json"))?;
    let app = App::start("tract_build", text_actions(&export, &tokenizer)?)?;
    let vocab = serde_json::from_str::<Value>(&tokenizer)?["model"]["vocab"].take();
    let unit = |v: Vec<f32>| {
        let n = v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        v.into_iter().map(|x| f64::from(x) / n).collect::<Vec<_>>()
    };
    let by_ort = |text: &str| ort(&export, vec![("input_ids", ids(&vocab, text))]).map(unit);
    let close = |a: &[f64], b: &[f64]| {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-5)
    };

    // When each is handed the same five texts to embed
    // Then the two builds' vectors give the same answers, as the original model gives through ort
    for text in TEXTS {
        let tract: Vec<f64> = serde_json::from_value(app.ask(20, text, "vector")?)?;
        let original = vec![("input_ids", ids(&vocab, text)), ("offsets", vec![0])];
        let original = unit(ort(&potion.join("onnx/model.onnx"), original)?);
        assert_eq!(tract.len(), 256);
        assert!(close(&tract, &by_ort(text)?), "{text}: tract and ort");
        assert!(close(&tract, &original), "{text}: the original through ort");
    }

    // When each is handed 'the app crashes when I click save', 'how do I export my notes?' and
    // 'thank you, this tool is wonderful' to classify
    // Then both give bug, question and praise
    let label = |t: &&str| app.ask(30, t, "label");
    let labels: Vec<Value> = TEXTS[..3].iter().map(label).collect::<Result<_>>()?;
    assert_eq!(labels, ["bug", "question", "praise"]);
    let about = LABELS.map(|(l, d)| by_ort(d).map(|v| (l, v)));
    let about = about.into_iter().collect::<Result<Vec<_>>>()?;
    for (text, want) in TEXTS[..3].iter().zip(["bug", "question", "praise"]) {
        let v = by_ort(text)?;
        let cos = |l: &[f64]| v.iter().zip(l).map(|(a, b)| a * b).sum::<f64>();
        let best = about.iter().max_by(|a, b| cos(&a.1).total_cmp(&cos(&b.1)));
        assert_eq!(best.map(|b| b.0), Some(want), "the ort build on {text}");
    }
    app.stop();
    Ok(())
}
