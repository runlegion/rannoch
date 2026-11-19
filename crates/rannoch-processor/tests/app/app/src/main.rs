//! The test app: its flows compiled in by its build.rs, run over its own actions and the
//! separate processor crate's. `jobs` prints each flow's `ty` and whether the flow the queue reads
//! has a job step; `run <ty> <data>` runs that `ty`'s flow on one event and prints the event's id,
//! then every event the flow sends, one JSON line each.

use std::error::Error;
use std::sync::Arc;

use rannoch_core::{Append, Config, Event, Rannoch, Status};
use rannoch_processor::{Action, Processor, Signature, gather};
use rannoch_queue::{Flows, Row};
use serde_json::{Map, Value, json};
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/flows.rs"));

/// The app's own processor crate: its actions' signatures, as data.
const SIGNATURES: &str = include_str!("../signatures.json");

/// One of the app's own actions: note gives nothing; classify gives a category.
struct Own(Signature);

impl Action for Own {
    fn signature(&self) -> &Signature {
        &self.0
    }

    fn run(&self, _: &Value, _: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let mut out = Map::new();
        if self.0.action == "classify" {
            out.insert("category".to_owned(), json!("bug"));
        }
        Ok(out)
    }
}

/// Prints every event rannoch hands over.
#[derive(Debug)]
struct Print;

impl Append<u16, Value> for Print {
    fn append(&self, batch: &[Event<u16, Value>]) -> Result<(), rannoch_core::Error> {
        for event in batch {
            let line = json!({
                "ty": event.ty,
                "status": event.status as i8,
                "source": event.source.map(|s| s.to_string()),
                "data": event.data,
                "run_at": event.run_at.map(|t| t.timestamp_millis()),
            });
            println!("{line}");
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut actions = proc_text::actions();
    actions.extend(
        gather(&[SIGNATURES])?
            .into_iter()
            .map(|sig| Arc::new(Own(sig)) as Arc<dyn Action>),
    );
    let processor: Processor<u16, Value> = Processor::new(FLOWS, actions)?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [jobs] if jobs == "jobs" => {
            for doc in FLOWS {
                let ty = serde_json::from_str::<Value>(doc)?["ty"]
                    .as_u64()
                    .and_then(|ty| u16::try_from(ty).ok())
                    .ok_or("a compiled flow has no ty")?;
                let flow = processor.flow(ty).ok_or("a compiled flow is not run")?;
                println!("{ty} {}", flow.job);
            }
        }
        [run, ty, data] if run == "run" => {
            let ty: u16 = ty.parse()?;
            let flow = processor.flow(ty).ok_or("no flow for that ty")?;
            let event: Row = Event {
                id: Uuid::now_v7(),
                source: None,
                ty,
                data: data.clone(),
                run_at: None,
                status: Status::InProgress,
            };
            println!("{}", event.id);
            let rannoch = Rannoch::open(Config {
                append: Some(Arc::new(Print) as Arc<dyn Append<u16, Value>>),
                ..Config::default()
            })
            .map_err(|err| err.to_string())?;
            let ran = (flow.run)(&event, &event, &rannoch.sender());
            rannoch.shutdown();
            ran?;
        }
        _ => return Err("usage: jobs | run <ty> <data>".into()),
    }
    Ok(())
}
