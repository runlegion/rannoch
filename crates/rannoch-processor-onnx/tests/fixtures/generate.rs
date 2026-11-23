//! Writes the fixture model2vec model that the crate's scenarios embed and classify with into
//! `tests/fixtures/model2vec/`: `tokenizer.json`, `config.json`, `model.safetensors` and
//! `onnx/model.onnx`. Run it after changing it, and commit what it writes:
//! `cargo run -p rannoch-processor-onnx --features tract --example fixture`.
//!
//! The vocabulary is the words of the scenarios' texts and label descriptions, `rannoch` left
//! out so that one word is dropped. Each word's row of the [41, 8] embeddings matrix is a small
//! pseudo-random vector, plus 1 in dimension 0, 1 or 2 for a word that speaks of a bug, a
//! question or praise, so each of the scenarios' three texts is nearest its label's description.
//! `onnx/model.onnx` takes `input_ids` and `offsets` as a model2vec export does, for one text,
//! and gives the mean of the rows of `input_ids` from `offsets[0]` on.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::num::TryFromIntError;
use std::path::Path;

use prost::Message;
use serde_json::json;
use tract_onnx::pb::tensor_shape_proto::{Dimension, dimension::Value::DimParam};
use tract_onnx::pb::{AttributeProto, GraphProto, ModelProto, NodeProto, OperatorSetIdProto};
use tract_onnx::pb::{TensorProto, TensorShapeProto, TypeProto, ValueInfoProto, type_proto};
use tract_onnx::pb::{attribute_proto::AttributeType, tensor_proto::DataType};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/model2vec");
/// Each word, and the dimension it speaks to: 0 a bug, 1 a question, 2 praise.
const WORDS: [(&str, Option<usize>); 41] = [
    ("[UNK]", None),
    ("crash", Some(0)),
    ("##es", Some(0)),
    ("##ed", Some(0)),
    ("error", Some(0)),
    ("broken", Some(0)),
    ("how", Some(1)),
    ("do", Some(1)),
    ("?", Some(1)),
    ("can", Some(1)),
    ("explain", Some(1)),
    ("thank", Some(2)),
    ("wonderful", Some(2)),
    ("great", Some(2)),
    ("love", Some(2)),
    ("the", None),
    ("app", None),
    ("when", None),
    ("i", None),
    ("click", None),
    ("save", None),
    ("export", None),
    ("my", None),
    ("notes", None),
    ("you", None),
    (",", None),
    ("this", None),
    ("tool", None),
    ("is", None),
    ("keeps", None),
    ("every", None),
    ("event", None),
    ("forever", None),
    (".", None),
    ("a", None),
    ("b", None),
    ("c", None),
    ("with", None),
    ("an", None),
    ("and", None),
    ("it", None),
];
const DIM: usize = 8;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = Path::new(FIXTURE);
    fs::create_dir_all(dir.join("onnx"))?;
    // Keys in sorted order, so serde_json writes the same file with or without preserve_order.
    let vocab: BTreeMap<&str, usize> = WORDS.iter().enumerate().map(|(i, w)| (w.0, i)).collect();
    let tokenizer = json!({ "model": {
        "continuing_subword_prefix": "##",
        "max_input_chars_per_word": 100,
        "type": "WordPiece",
        "unk_token": "[UNK]",
        "vocab": vocab,
    } });
    let config = json!({ "hidden_dim": DIM, "model_type": "model2vec", "normalize": true });
    for (file, value) in [("tokenizer.json", tokenizer), ("config.json", config)] {
        fs::write(dir.join(file), serde_json::to_string_pretty(&value)? + "\n")?;
    }
    let data = embeddings()?;
    fs::write(dir.join("model.safetensors"), safetensors(&data)?)?;
    fs::write(dir.join("onnx/model.onnx"), onnx(data)?.encode_to_vec())?;
    Ok(())
}

/// The embeddings matrix as little-endian f32s, row by row: each draw of a 64-bit LCG, 16 bits a
/// draw, scaled into [-0.1, 0.1), plus 1 in the dimension the row's word speaks to.
fn embeddings() -> Result<Vec<u8>, TryFromIntError> {
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut data = Vec::new();
    for (_, about) in WORDS {
        for d in 0..DIM {
            state = state.wrapping_mul(6_364_136_223_846_793_005);
            state = state.wrapping_add(1_442_695_040_888_963_407);
            let noise = (f32::from(u16::try_from(state >> 48)?) / 65_536.0 - 0.5) * 0.2;
            let x = noise + if about == Some(d) { 1.0 } else { 0.0 };
            data.extend(x.to_le_bytes());
        }
    }
    Ok(data)
}

/// `data` as a safetensors file of one F32 tensor, `embeddings`, its header padded to 8 bytes.
fn safetensors(data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    let header = json!({ "embeddings": {
        "data_offsets": [0, data.len()],
        "dtype": "F32",
        "shape": [WORDS.len(), DIM],
    } });
    let mut header = serde_json::to_string(&header)?;
    while header.len() % 8 != 0 {
        header.push(' ');
    }
    let mut file = u64::try_from(header.len())?.to_le_bytes().to_vec();
    file.extend(header.as_bytes());
    file.extend(data);
    Ok(file)
}

/// The model over the embeddings `data`: Slice of `input_ids` from `offsets[0]`, Gather of
/// those rows, and `ReduceMean` over them; opset 13.
fn onnx(data: Vec<u8>) -> Result<ModelProto, TryFromIntError> {
    let ids = |name: &str, dim: &str| ValueInfoProto {
        name: name.into(),
        r#type: Some(TypeProto {
            value: Some(type_proto::Value::TensorType(type_proto::Tensor {
                elem_type: DataType::Int64 as i32,
                shape: Some(TensorShapeProto {
                    dim: vec![Dimension {
                        value: Some(DimParam(dim.into())),
                        ..Dimension::default()
                    }],
                }),
            })),
            ..TypeProto::default()
        }),
        ..ValueInfoProto::default()
    };
    let node = |op: &str, input: &[&str], out: &str, attribute| NodeProto {
        input: input.iter().map(|i| (*i).to_owned()).collect(),
        output: vec![out.into()],
        op_type: op.into(),
        attribute,
        ..NodeProto::default()
    };
    // Gather's axis 0, Slice's axes and steps, and ReduceMean's keepdims 1 are ONNX's defaults.
    let axes = AttributeProto {
        name: "axes".into(),
        ints: vec![0],
        r#type: AttributeType::Ints as i32,
        ..AttributeProto::default()
    };
    let graph = GraphProto {
        node: vec![
            node("Slice", &["input_ids", "offsets", "end"], "ids", vec![]),
            node("Gather", &["E", "ids"], "rows", vec![]),
            node("ReduceMean", &["rows"], "embeddings", vec![axes]),
        ],
        initializer: vec![
            TensorProto {
                dims: vec![i64::try_from(WORDS.len())?, i64::try_from(DIM)?],
                data_type: DataType::Float as i32,
                name: "E".into(),
                raw_data: data,
                ..TensorProto::default()
            },
            TensorProto {
                dims: vec![1],
                data_type: DataType::Int64 as i32,
                name: "end".into(),
                int64_data: vec![i64::MAX],
                ..TensorProto::default()
            },
        ],
        input: vec![ids("input_ids", "n"), ids("offsets", "texts")],
        output: vec![ValueInfoProto {
            name: "embeddings".into(),
            ..ValueInfoProto::default()
        }],
        ..GraphProto::default()
    };
    Ok(ModelProto {
        ir_version: 8,
        opset_import: vec![OperatorSetIdProto {
            version: 13,
            ..OperatorSetIdProto::default()
        }],
        graph: Some(graph),
        ..ModelProto::default()
    })
}
