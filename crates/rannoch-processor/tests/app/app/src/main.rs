//! The app: loads the flows its build compiled in and prints each flow's ty and job.

use rannoch_processor::{FlowDoc, Processor};
use rannoch_queue::Flows;
use serde_json::Value;

const FLOWS: &str = include_str!(concat!(env!("OUT_DIR"), "/flows.json"));

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let events = rusqlite::Connection::open_in_memory()?;
    let processor: Processor<u16, Value> = Processor::new(FLOWS, proc_text::actions(), events)?;
    for doc in serde_json::from_str::<Vec<FlowDoc>>(FLOWS)? {
        let job = processor.flow(doc.ty).is_some_and(|flow| flow.job);
        println!("{} {job}", doc.ty);
    }
    Ok(())
}
