//! rannoch: one graph for the Rust projects in our ecosystem.
//!
//! Every change to a graph is an event; the tables are state built from the events; the graph
//! presents as an ontology. An app opens a graph file, declares its ontology on the graph's own
//! stream, writes whole units, and reads by capped walk, exact count or find.
//!
//! ```
//! use rannoch::{Class, Date, Declaration, Relation, Unit, ValidTime, Write};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let mut g = rannoch::open(dir.path().join("graph.rannoch"))?;
//! g.declare(&Declaration::Class(Class::new("voice", [] as [&str; 0])))?;
//! g.declare(&Declaration::Class(Class::new("statement", ["text"])))?;
//! g.declare(&Declaration::Relation(Relation::new("said", "voice", "statement", false)))?;
//!
//! let when = ValidTime::On(Date::month(2019, 10));
//! let events = g.write(
//!     &Write::new("swtor")
//!         .unit(Unit::new("voice", "v1", [] as [(&str, &str); 0], when.clone()))
//!         .unit(Unit::new("statement", "s1", [("text", "hi")], when.clone())
//!             .link("said", "v1", "s1", when)),
//! )?;
//! assert_eq!(events.len(), 1);
//! assert_eq!(g.find("statement", "s1")?.map(|n| n.source), Some("swtor".to_string()));
//! # Ok(())
//! # }
//! ```

mod events;
mod open;
mod read;
mod write;

pub use events::{Change, Cursor, Event, EventId, Events, Fact, LinkFact, NodeFact, WrongKind};
pub use open::{Class, Graph, Ontology, Relation, open};
pub use read::{Count, Filter, GroupBy, Link, Node, NodeRef, OpenLink, Step, Tally};
pub use write::{Date, Declaration, Error, PIECE_CHANGES, Unit, ValidTime, Write, WriteError};
