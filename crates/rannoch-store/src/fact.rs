//! What a committed `CloudEvent` says the store must fill, read through `rannoch-cloudevents` and
//! checked against the ontology: a node of a declared class whose properties fit it, or a link
//! of a declared relation between its classes, each carrying the point its class or relation
//! declares.

pub(crate) use rannoch_cloudevents::Fact;
use rannoch_cloudevents::{Axis, Conclusion, LinkFact, NodeRef};
use rannoch_ontology::{Ontology, Relation};

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
/// or relation, its point on their declared axis, its values' types and a link's ends. A
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
            point(&class.axis, &node.axis, &format!("class {}", class.name))?;
            class.check(&node.properties)?;
        }
        Fact::Link(LinkFact {
            relation,
            from,
            to,
            axis,
            ..
        })
        | Fact::Concluded(Conclusion {
            relation,
            from,
            to,
            ended: Some((axis, _)),
            ..
        }) => {
            let relation = ontology.relation(relation)?;
            point(&relation.axis, axis, &format!("relation {}", relation.name))?;
            ends(relation, from, to)?;
        }
        Fact::Concluded(Conclusion {
            relation, from, to, ..
        }) => ends(ontology.relation(relation)?, from, to)?,
        Fact::Declared | Fact::None => {}
    }
    Ok(fact)
}

/// Refuses a link whose ends are not of its relation's classes.
fn ends(relation: &Relation, from: &NodeRef, to: &NodeRef) -> Result<(), Error> {
    Ok(relation.holds(&from.class, &to.class)?)
}

/// The point `given` names on the axis `declared` (of `owner`, a class or relation), its parts in
/// declared order; empty when the axis is undeclared, so the point is the fact's known-at.
///
/// # Errors
/// A declared part `given` leaves out, or a part it names that is not declared.
pub(crate) fn point(declared: &[String], given: &Axis, owner: &str) -> Result<Vec<i64>, Error> {
    if let Some(extra) = given.keys().find(|part| !declared.contains(part)) {
        return Err(Error::Refused(format!(
            "{owner} declares axis {declared:?}; the event names part {extra}, which it does not"
        )));
    }
    declared
        .iter()
        .map(|part| {
            given.get(part).copied().ok_or_else(|| {
                Error::Refused(format!(
                    "{owner} declares axis {declared:?}; the event's data.axis leaves out {part}"
                ))
            })
        })
        .collect()
}
