//! What a committed `CloudEvent` says the store must fill, read through `rannoch-cloudevents` and
//! checked against the ontology: a node of a declared class whose properties fit it and that
//! carries the key its history is ordered by, or a link of a declared relation between its
//! classes.

pub(crate) use rannoch_cloudevents::Fact;
use rannoch_cloudevents::{Conclusion, LinkFact, NodeRef};
use rannoch_ontology::{HistoryKey, Ontology, Relation};

use crate::Error;

impl From<rannoch_cloudevents::Error> for Error {
    fn from(error: rannoch_cloudevents::Error) -> Error {
        match error {
            rannoch_cloudevents::Error::Refused(why) => Error::Refused(why),
            rannoch_cloudevents::Error::Encoding(error) => Error::Encoding(error),
        }
    }
}

/// Reads `body` as a `CloudEvent` and checks what it says against `ontology`: its format, its class
/// or relation, the history key its class is ordered by, its values' types and a link's ends. A
/// body that is not a `CloudEvents` 1.0 event, or a node or link the ontology does not allow, is
/// refused with the reason.
///
/// # Errors
/// [`Error::Refused`] with the reason, or [`Error::Encoding`] when the body is not JSON.
pub fn check(body: &str, ontology: &Ontology) -> Result<Fact, Error> {
    let fact = rannoch_cloudevents::read(body)?;
    match &fact {
        Fact::Node(node) => {
            let class = ontology.class(&node.class)?;
            let missing = match class.history {
                HistoryKey::Time if node.time.is_none() => Some("a time"),
                HistoryKey::Integer if node.integer.is_none() => Some("an integer key"),
                _ => None,
            };
            if let Some(missing) = missing {
                return Err(Error::Refused(format!(
                    "class {} is ordered by {missing} the writer supplies; node {} carries none",
                    class.name, node.key
                )));
            }
            class.check(&node.properties)?;
        }
        Fact::Link(LinkFact {
            relation, from, to, ..
        })
        | Fact::Concluded(Conclusion {
            relation, from, to, ..
        }) => ends(ontology.relation(relation)?, from, to)?,
        Fact::Amended(amended) => {
            let class = ontology.class(&amended.class)?;
            if amended.from == amended.to {
                return Err(Error::Refused(format!(
                    "amending {} {} to its own key changes nothing; a new version under the same \
                     key is a write",
                    class.name, amended.from
                )));
            }
        }
        Fact::Declared | Fact::None => {}
    }
    Ok(fact)
}

/// Refuses a link whose ends are not of its relation's classes.
fn ends(relation: &Relation, from: &NodeRef, to: &NodeRef) -> Result<(), Error> {
    Ok(relation.holds(&from.class, &to.class)?)
}
