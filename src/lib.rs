//! rannoch: one graph for the Rust projects in our ecosystem.
//!
//! Every change to a graph is an event; the tables are state built from the events; the graph
//! presents as an ontology. An app opens a graph file, declares its ontology on the graph's own
//! stream, writes whole units, and reads by capped walk, exact count or find. Every write is a
//! hearing at a point on the time axis, identified by its content under the app's own key.
//!
//! ```
//! use rannoch::{Class, Date, Declaration, Relation, Unit, ValidTime, Write};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let key = [7; 32]; // the app's content-identity key: never stored in the graph's files
//! let mut g = rannoch::open(dir.path().join("graph.rannoch"), Some(&key))?;
//! g.declare(&Declaration::Class(Class::new("author", [] as [&str; 0])))?;
//! g.declare(&Declaration::Class(Class::new("book", ["title"])))?;
//! g.declare(&Declaration::Relation(Relation::new("wrote", "author", "book", false)))?;
//!
//! let when = ValidTime::On(Date::month(2019, 10));
//! let events = g.write(
//!     &Write::new("swtor")
//!         .unit(Unit::new("author", "a1", [] as [(&str, &str); 0], when.clone()))
//!         .unit(Unit::new("book", "b1", [("title", "hi")], when.clone())
//!             .link("wrote", "a1", "b1", when)),
//! )?;
//! assert_eq!(events.len(), 1);
//! assert_eq!(g.find("book", "b1")?.map(|n| n.source), Some("swtor".to_string()));
//! # Ok(())
//! # }
//! ```

mod events;
mod history;
#[cfg(test)]
mod measure;
mod open;
mod queue;
mod read;
mod refs;
mod write;

pub use events::{
    Change, Cursor, Event, EventId, Events, Fact, LinkFact, NodeFact, Snapshot, WrongKind, rebuild,
};
/// The content-identity key the unit tests open their graphs under.
#[cfg(test)]
pub(crate) const TEST_KEY: [u8; 32] = *b"rannoch unit tests' identity key";

pub use history::{End, Hearing};
pub use open::{Class, Graph, HistoryKey, Ontology, Relation, Value, ValueType, open};
pub use queue::{Failure, Queue, Ticket};
pub use read::{
    Count, Description, Filter, GroupBy, Link, Node, NodeRef, OpenLink, PartOf, Step, Tally, Walk,
};
pub use refs::Fate;
pub use write::{Date, Declaration, Error, Unit, ValidTime, Write, WriteError};
