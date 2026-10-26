//! Purging: an app's forgetting rule removes a fact, a stream, everything about a node or a link,
//! or one source's word, from the store and from every snapshot the store made, and leaves nothing
//! of it: no event body, no hearing, no content identity, and no event id that names nothing left.
//! A purge runs in one transaction with `secure_delete` on and checkpoints the write-ahead log
//! after, so no purged byte stays in a free page or in the log.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, ToSql, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::events::{
    Change, Cursor, Event, EventId, Fact, LinkFact, Stored, append, drop_derived, naming_key,
    remove, unlink,
};
use crate::history::{Key, Of, link_entity, newest, node_entity, see};
use crate::open::{CLASS, Graph, Ontology, RELATION, class_table, open_keyed, relation_table};
use crate::read::NodeRef;
use crate::write::{Error, Piece, Stamp, node_id};

/// What a purge names: a node, or a link by its relation and both of its ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Target {
    Node(NodeRef),
    Link {
        relation: String,
        from: NodeRef,
        to: NodeRef,
    },
}

/// What an app's forgetting rule purges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purge {
    /// One hearing, by its content identity.
    Fact { identity: [u8; 32] },
    /// Every hearing, ending and correction written under the write source `source`.
    Stream { source: String },
    /// Everything about one node and every node transitively part of it, with their links,
    /// hearings, endings and corrections; or a link's hearings, endings and corrections.
    Entity { target: Target },
    /// The node's or link's hearings, endings and corrections from the hearing source `source`.
    EntityFromSource { target: Target, source: String },
    /// Every hearing, ending and correction from the hearing source `source`, on every node and
    /// link.
    Source { source: String },
}

impl Purge {
    /// The purge with each of its targets as its keyed match value under `key`, as its event
    /// records it: it names what it matched and holds none of it.
    pub(crate) fn matched(&self, key: &Key) -> Purge {
        let text = |text: &str| key.matched(text.as_bytes()).to_hex().to_string();
        let node = |n: &NodeRef| matched_node(key, n);
        let target = |t: &Target| match t {
            Target::Node(n) => Target::Node(node(n)),
            Target::Link { relation, from, to } => Target::Link {
                relation: text(relation),
                from: node(from),
                to: node(to),
            },
        };
        match self {
            Purge::Fact { identity } => Purge::Fact {
                identity: *key.matched(identity).as_bytes(),
            },
            Purge::Stream { source } => Purge::Stream {
                source: text(source),
            },
            Purge::Entity { target: t } => Purge::Entity { target: target(t) },
            Purge::EntityFromSource { target: t, source } => Purge::EntityFromSource {
                target: target(t),
                source: text(source),
            },
            Purge::Source { source } => Purge::Source {
                source: text(source),
            },
        }
    }
}

/// Each purge's standing rule: its rule, its kind, its targets as keyed match values
/// (`match_b` empty for a kind with one), and the purge event. A hearing that arrives is looked
/// up by (`kind`, `match_a`, `match_b`), which holds one rule: the first kept for it.
pub(crate) const PURGE_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _purge_rules(
        rule TEXT NOT NULL, kind TEXT NOT NULL, match_a BLOB NOT NULL, match_b BLOB NOT NULL,
        event TEXT NOT NULL);
    CREATE UNIQUE INDEX IF NOT EXISTS _purge_rules_one
        ON _purge_rules(kind, match_a, match_b);";

/// The kind and the values `match_a` and `match_b` a standing rule is kept and looked up under,
/// from a purge whose targets are keyed match values: a node is its class's and its key's, a link
/// its relation's and its two keys', end to end.
fn rule_of(matched: &Purge) -> Result<(&'static str, Vec<u8>, Vec<u8>), Error> {
    let value = |hex: &str| {
        blake3::Hash::from_hex(hex)
            .map(|hash| hash.as_bytes().to_vec())
            .map_err(|_| Error::Refused(format!("the purge's match value {hex} is not a hash")))
    };
    let target = |target: &Target| {
        Ok::<_, Error>(match target {
            Target::Node(node) => [value(&node.class)?, value(&node.key)?].concat(),
            Target::Link { relation, from, to } => {
                [value(relation)?, value(&from.key)?, value(&to.key)?].concat()
            }
        })
    };
    Ok(match matched {
        Purge::Fact { identity } => ("fact", identity.to_vec(), Vec::new()),
        Purge::Stream { source } => ("stream", value(source)?, Vec::new()),
        Purge::Source { source } => ("source", value(source)?, Vec::new()),
        Purge::Entity { target: t } => ("entity", target(t)?, Vec::new()),
        Purge::EntityFromSource { target: t, source } => {
            ("entity_from_source", target(t)?, value(source)?)
        }
    })
}

/// Keeps `matched`, a purge whose targets are keyed match values, as the standing rule `rule`,
/// named by the purge event `event`.
pub(crate) fn stand(
    tx: &Connection,
    rule: &str,
    matched: &Purge,
    event: EventId,
) -> Result<(), Error> {
    keep_rule(tx, rule, matched, &event.to_string())
}

/// Keeps `matched`, a purge whose targets are keyed match values, under `rule`, named by the
/// purge event whose id is `event`.
fn keep_rule(tx: &Connection, rule: &str, matched: &Purge, event: &str) -> Result<(), Error> {
    let (kind, a, b) = rule_of(matched)?;
    tx.prepare_cached(
        "INSERT OR IGNORE INTO _purge_rules(rule, kind, match_a, match_b, event) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )?
    .execute(params![rule, kind, a, b, event])?;
    Ok(())
}

/// Keeps a legacy forget of `node` under `rule`, named by the forget event `event`, as that
/// node's purge rule.
pub(crate) fn stand_forgot(
    tx: &Connection,
    key: &Key,
    rule: &str,
    node: &NodeRef,
    event: EventId,
) -> Result<(), Error> {
    let purge = Purge::Entity {
        target: Target::Node(node.clone()),
    };
    stand(tx, rule, &purge.matched(key), event)
}

/// Keeps a purge event's rules: `matched`, and each of the `parts` it removed with the whole,
/// all as keyed match values.
pub(crate) fn stand_purged(
    tx: &Connection,
    rule: &str,
    (matched, parts): (&Purge, &[NodeRef]),
    event: EventId,
) -> Result<(), Error> {
    stand(tx, rule, matched, event)?;
    for part in parts {
        let part = Purge::Entity {
            target: Target::Node(part.clone()),
        };
        stand(tx, rule, &part, event)?;
    }
    Ok(())
}

/// Whether the store keeps any standing rule.
pub(crate) fn standing(tx: &Connection) -> Result<bool, Error> {
    Ok(tx
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM _purge_rules)")?
        .query_row([], |row| row.get(0))?)
}

/// The standing rule (its rule and purge event) the first of `candidates`, purges whose targets
/// are plain, matches; `None` when none does.
fn rule_matching(
    tx: &Connection,
    key: &Key,
    candidates: Vec<Purge>,
) -> Result<Option<(String, String)>, Error> {
    for candidate in candidates {
        let (kind, a, b) = rule_of(&candidate.matched(key))?;
        let found = tx
            .prepare_cached(
                "SELECT rule, event FROM _purge_rules WHERE kind = ?1 AND match_a = ?2 AND match_b = ?3 LIMIT 1",
            )?
            .query_row(params![kind, a, b], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

/// The purges that match the link of `relation` from `from` to `to` from `source`: its write
/// source, its hearing source, the link, the link from `source`, and everything about either end.
fn link_candidates(relation: &str, from: &NodeRef, to: &NodeRef, source: &str) -> Vec<Purge> {
    let link = Target::Link {
        relation: relation.to_string(),
        from: from.clone(),
        to: to.clone(),
    };
    vec![
        Purge::Stream {
            source: source.to_string(),
        },
        Purge::Source {
            source: source.to_string(),
        },
        Purge::Entity {
            target: Target::Node(from.clone()),
        },
        Purge::Entity {
            target: Target::Node(to.clone()),
        },
        Purge::EntityFromSource {
            target: link.clone(),
            source: source.to_string(),
        },
        Purge::Entity { target: link },
    ]
}

/// Whether a standing rule matches the hearing `of`, written and heard from `source`: by its
/// identity, its write source, its hearing source, its node or link, that from `source`, or, for
/// a link, everything about either of its ends.
pub(crate) fn purged(tx: &Connection, key: &Key, of: &Of, source: &str) -> Result<bool, Error> {
    let mut candidates = vec![Purge::Fact {
        identity: of.identity(key, source)?,
    }];
    match of {
        Of::Node(node) => {
            let target = Target::Node(NodeRef::new(&node.class, &node.key));
            candidates.extend([
                Purge::Stream {
                    source: source.to_string(),
                },
                Purge::Source {
                    source: source.to_string(),
                },
                Purge::EntityFromSource {
                    target: target.clone(),
                    source: source.to_string(),
                },
                Purge::Entity { target },
            ]);
        }
        Of::Link(link) => {
            candidates.extend(link_candidates(
                &link.relation,
                &link.from,
                &link.to,
                source,
            ));
        }
    }
    Ok(rule_matching(tx, key, candidates)?.is_some())
}

/// The rule that matches an ending or a correction of the link of `relation` from `from` to `to`
/// from `source`, if a standing rule does.
pub(crate) fn concluded_by(
    tx: &Connection,
    key: &Key,
    (relation, from, to): (&str, &NodeRef, &NodeRef),
    source: &str,
) -> Result<Option<String>, Error> {
    Ok(rule_matching(tx, key, link_candidates(relation, from, to, source))?.map(|(rule, _)| rule))
}

/// Whether `node` arrives as a part of a whole a standing rule purges everything about: one of
/// `links` is a containment link from it to such a whole, or to a part of one that arrived
/// before. It then stands as a part under that whole's rule, so its own parts follow it.
pub(crate) fn part_of_purged<'l>(
    tx: &Connection,
    key: &Key,
    ontology: &Ontology,
    node: &NodeRef,
    links: impl IntoIterator<Item = &'l LinkFact>,
) -> Result<bool, Error> {
    for link in links {
        let contained = ontology
            .relations
            .get(&link.relation)
            .is_some_and(|r| r.containment);
        if !contained || link.from != *node {
            continue;
        }
        let whole = Purge::Entity {
            target: Target::Node(link.to.clone()),
        };
        if let Some((rule, event)) = rule_matching(tx, key, vec![whole])? {
            let part = Purge::Entity {
                target: Target::Node(node.clone()),
            };
            keep_rule(tx, &rule, &part.matched(key), &event)?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// `change` as it is kept on arrival: a write without each hearing a standing rule matches (a
/// node arriving as part of a purged whole with it), or `None` when nothing is left of it, or
/// when it is an ending or a correction a rule matches.
pub(crate) fn arriving(
    tx: &Connection,
    key: &Key,
    ontology: &Ontology,
    change: &Change,
) -> Result<Option<Change>, Error> {
    if !standing(tx)? {
        return Ok(Some(change.clone()));
    }
    let (source, added, wrong_kind, check) = match change {
        Change::Wrote {
            source,
            added,
            wrong_kind,
            check,
        } => (source, added, wrong_kind, check),
        Change::Ended {
            source,
            relation,
            from,
            to,
            ..
        }
        | Change::Corrected {
            source,
            relation,
            from,
            to,
        } => {
            let rule = concluded_by(tx, key, (relation, from, to), source)?;
            return Ok(rule.is_none().then(|| change.clone()));
        }
        _ => return Ok(Some(change.clone())),
    };
    let links: Vec<&LinkFact> = added
        .iter()
        .filter_map(|fact| match fact {
            Fact::Link(link) | Fact::Opened(link) => Some(link),
            Fact::Node(_) => None,
        })
        .collect();
    let mut kept = Vec::new();
    for fact in added {
        let gone = purged(tx, key, &Of::fact(fact), source)?
            || match fact {
                Fact::Node(node) => {
                    let node = NodeRef::new(&node.class, &node.key);
                    part_of_purged(tx, key, ontology, &node, links.iter().copied())?
                }
                _ => false,
            };
        if !gone {
            kept.push(fact.clone());
        }
    }
    if kept.len() == added.len() {
        return Ok(Some(change.clone()));
    }
    Ok(
        (!kept.is_empty() || !wrong_kind.is_empty()).then(|| Change::Wrote {
            source: source.clone(),
            added: kept,
            wrong_kind: wrong_kind.clone(),
            check: check.clone(),
        }),
    )
}

/// Keeps a standing rule for each purge and each legacy forget in the log of a store that holds
/// none, as one transaction: a store that purged before its rules were kept.
pub(crate) fn stand_all(conn: &mut Connection, key: &Key) -> Result<(), Error> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if standing(&tx)? {
        return Ok(());
    }
    let mut events: BTreeMap<i64, Stored> = naming_key(&tx, "purged")?.into_iter().collect();
    events.extend(naming_key(&tx, "forgot")?);
    for stored in events.into_values() {
        match &stored.change {
            Change::Purged {
                rule, purge, parts, ..
            } => stand_purged(&tx, rule, (purge, parts), stored.id)?,
            Change::Forgot { rule, nodes, .. } => {
                for node in nodes {
                    stand_forgot(&tx, key, rule, node, stored.id)?;
                }
            }
            _ => {}
        }
    }
    tx.commit()?;
    Ok(())
}

/// `node` with its class and key as their keyed match values under `key`.
fn matched_node(key: &Key, node: &NodeRef) -> NodeRef {
    let text = |text: &str| key.matched(text.as_bytes()).to_hex().to_string();
    NodeRef::new(text(&node.class), text(&node.key))
}

/// A node or a link as the hearings name it: its entity, its key and, for a link, its target's.
type Entity = (String, String, Option<String>);

/// A hearing a purge removes: its identity, what it is of, and the event it is named by, with
/// that event's place in the log.
struct Heard {
    identity: [u8; 32],
    of: Entity,
    event: String,
    seq: i64,
}

/// What a purge matched.
#[derive(Default)]
struct Chosen {
    hearings: BTreeMap<[u8; 32], Heard>,
    /// The ending and correction events, with the link each concluded.
    concluded: BTreeMap<String, Entity>,
    /// The nodes that leave whole, with their links.
    nodes: BTreeSet<NodeRef>,
    /// The links that leave whole: relation, from key, to key.
    links: BTreeSet<(String, String, String)>,
}

impl Chosen {
    fn hear(&mut self, tx: &Connection, filter: &str, args: &[&dyn ToSql]) -> Result<(), Error> {
        let sql = format!(
            "SELECT identity, entity, key, to_key, event, seq FROM _hearings WHERE {filter}"
        );
        let mut statement = tx.prepare_cached(&sql)?;
        let mut rows = statement.query(args)?;
        while let Some(row) = rows.next()? {
            let identity: [u8; 32] = row.get(0)?;
            self.hearings.insert(
                identity,
                Heard {
                    identity,
                    of: (row.get(1)?, row.get(2)?, row.get(3)?),
                    event: row.get(4)?,
                    seq: row.get(5)?,
                },
            );
        }
        Ok(())
    }

    fn conclude(
        &mut self,
        tx: &Connection,
        filter: &str,
        args: &[&dyn ToSql],
    ) -> Result<(), Error> {
        let sql = format!(
            "SELECT event, entity, key, to_key FROM _ends WHERE {filter} \
             UNION SELECT event, entity, key, to_key FROM _endings WHERE {filter}"
        );
        let mut statement = tx.prepare_cached(&sql)?;
        let mut rows = statement.query(args)?;
        while let Some(row) = rows.next()? {
            self.concluded
                .insert(row.get(0)?, (row.get(1)?, row.get(2)?, row.get(3)?));
        }
        Ok(())
    }

    /// Every hearing, ending and correction written under the write source `source`.
    fn stream(&mut self, tx: &Connection, key: &Key, source: &str) -> Result<(), Error> {
        for (_, stored) in naming_key(tx, source)? {
            match &stored.change {
                Change::Wrote {
                    source: s, added, ..
                } if s == source => {
                    for fact in added {
                        let of = Of::fact(fact);
                        let identity = of.identity(key, s)?;
                        self.hear(tx, "identity = ?1", params![identity])?;
                    }
                }
                Change::Ended {
                    source: s,
                    relation,
                    from,
                    to,
                    ..
                }
                | Change::Corrected {
                    source: s,
                    relation,
                    from,
                    to,
                } if s == source => {
                    let of = (
                        link_entity(relation),
                        from.key.clone(),
                        Some(to.key.clone()),
                    );
                    self.concluded.insert(stored.id.to_string(), of);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Everything about `whole` and every node part of it: their hearings, and their links'
    /// hearings, endings and corrections; each of them leaves whole.
    fn whole_node(
        &mut self,
        tx: &Connection,
        ontology: &Ontology,
        whole: &NodeRef,
    ) -> Result<(), Error> {
        for node in with_parts(tx, ontology, whole)? {
            self.hear(
                tx,
                "entity = ?1 AND key = ?2 AND to_key IS NULL",
                params![node_entity(&node.class), node.key],
            )?;
            for relation in ontology.relations.values() {
                for (end, column) in [(&relation.from, "key"), (&relation.to, "to_key")] {
                    if *end != node.class {
                        continue;
                    }
                    let filter = format!("entity = ?1 AND {column} = ?2");
                    let args = params![link_entity(&relation.name), node.key];
                    self.hear(tx, &filter, args)?;
                    self.conclude(tx, &filter, args)?;
                }
            }
            self.nodes.insert(node);
        }
        let links: Vec<Entity> = self
            .hearings
            .values()
            .map(|h| h.of.clone())
            .chain(self.concluded.values().cloned())
            .filter(|(_, _, to)| to.is_some())
            .collect();
        for (entity, from, to) in links {
            self.links.insert(link_key(&(entity, from, to)));
        }
        Ok(())
    }

    /// Whether a fact names a node or a link that leaves whole.
    fn names(&self, fact: &Fact) -> bool {
        match fact {
            Fact::Node(node) => self.nodes.contains(&NodeRef::new(&node.class, &node.key)),
            Fact::Link(link) | Fact::Opened(link) => self.whole_link(link),
        }
    }

    fn whole_link(&self, link: &LinkFact) -> bool {
        self.nodes.contains(&link.from)
            || self.nodes.contains(&link.to)
            || self.links.contains(&(
                link.relation.clone(),
                link.from.key.clone(),
                link.to.key.clone(),
            ))
    }
}

/// A link as the hearings name it, as its relation, from key and to key.
fn link_key((entity, from, to): &Entity) -> (String, String, String) {
    let relation = entity.split_once(':').map_or(entity.as_str(), |(_, r)| r);
    (
        relation.to_string(),
        from.clone(),
        to.clone().unwrap_or_default(),
    )
}

/// The node or link `target` names, as its hearings name it.
fn entity(ontology: &Ontology, target: &Target) -> Result<Entity, Error> {
    Ok(match target {
        Target::Node(node) => (
            node_entity(&ontology.class(&node.class)?.name),
            node.key.clone(),
            None,
        ),
        Target::Link { relation, from, to } => (
            link_entity(&ontology.relation(relation)?.name),
            from.key.clone(),
            Some(to.key.clone()),
        ),
    })
}

/// `node` and every node transitively part of it, following inbound containment links at any
/// depth, as a description walks them.
fn with_parts(tx: &Connection, ontology: &Ontology, node: &NodeRef) -> Result<Vec<NodeRef>, Error> {
    let root = NodeRef::new(&ontology.class(&node.class)?.name, &node.key);
    let mut all = vec![root.clone()];
    let mut frontier = vec![root];
    while let Some(whole) = frontier.pop() {
        for relation in ontology.relations.values() {
            if !relation.containment || relation.to != whole.class {
                continue;
            }
            let sql = format!(
                "SELECT p.key FROM {} r JOIN {} p ON p.id = r.src JOIN {} w ON w.id = r.dst \
                 WHERE w.key = ?1",
                relation_table(&relation.name),
                class_table(&relation.from),
                class_table(&relation.to)
            );
            let parts: Vec<String> = tx
                .prepare_cached(&sql)?
                .query_map(params![whole.key], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            for key in parts {
                let part = NodeRef::new(&relation.from, key);
                if !all.contains(&part) {
                    all.push(part.clone());
                    frontier.push(part);
                }
            }
        }
    }
    Ok(all)
}

/// What `purge` matches in the store `tx` holds.
fn choose(tx: &Connection, ontology: &Ontology, key: &Key, purge: &Purge) -> Result<Chosen, Error> {
    let mut chosen = Chosen::default();
    match purge {
        Purge::Fact { identity } => chosen.hear(tx, "identity = ?1", params![identity])?,
        Purge::Source { source } => {
            chosen.hear(tx, "source = ?1", params![source])?;
            chosen.conclude(tx, "source = ?1", params![source])?;
        }
        Purge::Stream { source } => chosen.stream(tx, key, source)?,
        Purge::EntityFromSource { target, source } => {
            let (entity, from, to) = entity(ontology, target)?;
            let args = params![entity, from, to, source];
            chosen.hear(
                tx,
                "entity = ?1 AND key = ?2 AND to_key IS ?3 AND source = ?4",
                args,
            )?;
            chosen.conclude(
                tx,
                "entity = ?1 AND key = ?2 AND to_key IS ?3 AND source = ?4",
                args,
            )?;
        }
        Purge::Entity {
            target: target @ Target::Link { .. },
        } => {
            let (entity, from, to) = entity(ontology, target)?;
            let args = params![entity, from, to];
            chosen.hear(tx, "entity = ?1 AND key = ?2 AND to_key IS ?3", args)?;
            chosen.conclude(tx, "entity = ?1 AND key = ?2 AND to_key IS ?3", args)?;
            chosen.links.insert(link_key(&(entity, from, to)));
        }
        Purge::Entity {
            target: Target::Node(node),
        } => chosen.whole_node(tx, ontology, node)?,
    }
    Ok(chosen)
}

/// Removes the link of `relation` from `from` to `to` from live state: from its relation, or
/// from the open links.
pub(crate) fn drop_link(
    tx: &Connection,
    ontology: &Ontology,
    (relation, from, to): (&str, &str, &str),
) -> Result<(), Error> {
    let relation = ontology.relation(relation)?;
    tx.prepare_cached(&format!(
        "DELETE FROM {} WHERE src = (SELECT id FROM {} WHERE key = ?1) \
         AND dst = (SELECT id FROM {} WHERE key = ?2)",
        relation_table(&relation.name),
        class_table(&relation.from),
        class_table(&relation.to)
    ))?
    .execute(params![from, to])?;
    tx.prepare_cached("DELETE FROM _open WHERE rel = ?1 AND from_key = ?2 AND to_key = ?3")?
        .execute(params![relation.name, from, to])?;
    drop_derived(tx, (&relation.name, from, to))?;
    Ok(())
}

/// What is left of a touched node or link once its purged hearings are gone.
#[derive(Default)]
pub(crate) struct Left {
    /// Nodes with no hearing left, which left live state; their links wait open.
    nodes: BTreeSet<NodeRef>,
    /// Links with no hearing left, which left live state.
    links: BTreeSet<(String, String, String)>,
}

/// Puts the current state of `of` back from its newest version left, as every write chooses it;
/// a node or link with no version left leaves live state, and a node's links that still have
/// hearings wait open for it.
pub(crate) fn current(
    tx: &Connection,
    ontology: &Ontology,
    key: &Key,
    (entity, at, to): &Entity,
    left: &mut Left,
) -> Result<(), Error> {
    let version = newest(tx, (entity, at, to.as_deref()))?;
    let stamp = |recorded| Stamp {
        id: EventId::now(),
        cursor: Cursor::START,
        recorded,
        arrived: true,
    };
    let (kind, name) = entity.split_once(':').unwrap_or_default();
    if kind == CLASS {
        let class = ontology.class(name)?;
        if let Some((v, recorded, _)) = version {
            let piece = Piece::new(tx, ontology, key, &v.source, stamp(recorded));
            piece.put(class, at, &v.source, &v.valid, recorded, &v.properties)?;
            return Ok(());
        }
        let node = NodeRef::new(&class.name, at.as_str());
        let Some(id) = node_id(tx, &class.name, at)? else {
            return Ok(());
        };
        for link in unlink(tx, ontology, &node, id)? {
            let entity = link_entity(&link.relation);
            let of = (
                entity.as_str(),
                link.from.key.as_str(),
                Some(link.to.key.as_str()),
            );
            if let Some((v, ..)) = newest(tx, of)? {
                tx.prepare_cached(
                    "INSERT OR REPLACE INTO _open(rel, from_key, to_key, source, valid) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?
                .execute(params![
                    link.relation,
                    link.from.key,
                    link.to.key,
                    v.source,
                    v.valid.to_sql()
                ])?;
            }
        }
        tx.prepare_cached(&format!(
            "DELETE FROM {} WHERE id = ?1",
            class_table(&class.name)
        ))?
        .execute(params![id])?;
        drop_derived(tx, (&class.name, at, ""))?;
        left.nodes.insert(node);
    } else if kind == RELATION {
        let relation = ontology.relation(name)?;
        let to = to.clone().unwrap_or_default();
        let Some((v, recorded, _)) = version else {
            drop_link(tx, ontology, (&relation.name, at, &to))?;
            left.links.insert((relation.name.clone(), at.clone(), to));
            return Ok(());
        };
        let fact = LinkFact {
            relation: relation.name.clone(),
            from: NodeRef::new(&relation.from, at.as_str()),
            to: NodeRef::new(&relation.to, to.as_str()),
            valid: v.valid.clone(),
            derived_from: Vec::new(),
        };
        let ends = node_id(tx, &relation.from, at)?.zip(node_id(tx, &relation.to, &to)?);
        let piece = Piece::new(tx, ontology, key, &v.source, stamp(recorded));
        piece.current_link(&fact, ends, &v.source, &v.valid)?;
    }
    Ok(())
}

/// `change` without what the purge removed, or `None` when nothing is left of it. A link whose
/// node left with no hearing waits open in every event that wrote it.
fn without(
    change: &Change,
    stripped: Option<&BTreeSet<[u8; 32]>>,
    key: &Key,
    (chosen, left): (&Chosen, &Left),
) -> Result<Option<Change>, Error> {
    let left_link = |link: &LinkFact| {
        left.nodes.contains(&link.from)
            || left.nodes.contains(&link.to)
            || left.links.contains(&(
                link.relation.clone(),
                link.from.key.clone(),
                link.to.key.clone(),
            ))
    };
    Ok(match change {
        Change::Wrote {
            source,
            added,
            wrong_kind,
            check,
        } => {
            let mut kept = Vec::new();
            for fact in added {
                let of = Of::fact(fact);
                let gone = match stripped {
                    Some(stripped) => stripped.contains(&of.identity(key, source)?),
                    None => false,
                };
                if gone || chosen.names(fact) {
                    continue;
                }
                kept.push(match fact {
                    Fact::Link(link)
                        if left.nodes.contains(&link.from) || left.nodes.contains(&link.to) =>
                    {
                        Fact::Opened(link.clone())
                    }
                    other => other.clone(),
                });
            }
            let wrong_kind: Vec<_> = wrong_kind
                .iter()
                .filter(|w| !chosen.nodes.contains(&w.arrived) && !chosen.whole_link(&w.waiting))
                .cloned()
                .collect();
            (!kept.is_empty() || !wrong_kind.is_empty()).then(|| Change::Wrote {
                source: source.clone(),
                added: kept,
                wrong_kind,
                check: check.clone(),
            })
        }
        Change::Closed(link) => {
            (!chosen.whole_link(link) && !left_link(link)).then(|| change.clone())
        }
        Change::Replaced { class, old, new } => (!chosen.nodes.contains(&NodeRef::new(class, old))
            && !chosen.nodes.contains(&NodeRef::new(class, new)))
        .then(|| change.clone()),
        other => Some(other.clone()),
    })
}

/// Removes what `purge` matches from the store `tx` holds: the hearings and conclusions it
/// matched, the nodes and links that leave whole, what every event body holds of them, and each
/// event id that then names nothing left; then puts back what is seen of every node and link it
/// touched and their current state.
/// Returns each node removed as a part of the whole `purge` names.
fn purge_in(
    tx: &Connection,
    ontology: &Ontology,
    key: &Key,
    purge: &Purge,
) -> Result<Vec<NodeRef>, Error> {
    let chosen = choose(tx, ontology, key, purge)?;
    // Every id each purged hearing goes by, with the identities that event's body loses, and the
    // events to read again: by place where the hearing names it, by id otherwise.
    let mut stripped: BTreeMap<String, BTreeSet<[u8; 32]>> = BTreeMap::new();
    let mut by_id: BTreeSet<String> = chosen.concluded.keys().cloned().collect();
    let mut places: BTreeSet<i64> = BTreeSet::new();
    for heard in chosen.hearings.values() {
        stripped
            .entry(heard.event.clone())
            .or_default()
            .insert(heard.identity);
        places.insert(heard.seq);
        let aliases: Vec<String> = tx
            .prepare_cached("SELECT alias FROM _aliases WHERE identity = ?1")?
            .query_map(params![heard.identity], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        for alias in aliases {
            stripped
                .entry(alias.clone())
                .or_default()
                .insert(heard.identity);
            by_id.insert(alias);
        }
    }
    for node in &chosen.nodes {
        remove(tx, ontology, node)?;
        tx.prepare_cached(
            "DELETE FROM _fates WHERE class = ?1 AND (key = ?2 OR replaced_by = ?2)",
        )?
        .execute(params![node.class, node.key])?;
    }
    for (relation, from, to) in &chosen.links {
        drop_link(tx, ontology, (relation, from, to))?;
    }
    for identity in chosen.hearings.keys() {
        tx.prepare_cached("DELETE FROM _aliases WHERE identity = ?1")?
            .execute(params![identity])?;
        tx.prepare_cached("DELETE FROM _hearings WHERE identity = ?1")?
            .execute(params![identity])?;
    }
    for event in chosen.concluded.keys() {
        for table in ["_ends", "_endings"] {
            tx.prepare_cached(&format!("DELETE FROM {table} WHERE event = ?1"))?
                .execute(params![event])?;
        }
    }
    let touched: BTreeSet<&Entity> = chosen
        .hearings
        .values()
        .map(|h| &h.of)
        .chain(chosen.concluded.values())
        .collect();
    let mut left = Left::default();
    for of in touched {
        see(tx, (&of.0, &of.1, of.2.as_deref()))?;
        let (kind, name) = of.0.split_once(':').unwrap_or_default();
        let whole = if kind == CLASS {
            chosen.nodes.contains(&NodeRef::new(name, of.1.as_str()))
        } else {
            chosen.links.contains(&link_key(of))
        };
        if !whole {
            current(tx, ontology, key, of, &mut left)?;
        }
    }
    rewrite(tx, key, (&chosen, &left), &stripped, (places, by_id))?;
    let whole = match purge {
        Purge::Entity {
            target: Target::Node(node),
        } => Some(NodeRef::new(&ontology.class(&node.class)?.name, &node.key)),
        _ => None,
    };
    Ok(chosen
        .nodes
        .into_iter()
        .filter(|node| Some(node) != whole.as_ref())
        .collect())
}

/// Rewrites each event body the purge reaches without what it removed: each event a purged
/// hearing or conclusion goes by (`places` in the log, or ids in `by_id`), and each event naming a
/// node or link that left. An event left with nothing is deleted, and its id leaves every
/// derived-from list.
fn rewrite(
    tx: &Connection,
    key: &Key,
    (chosen, left): (&Chosen, &Left),
    stripped: &BTreeMap<String, BTreeSet<[u8; 32]>>,
    (places, by_id): (BTreeSet<i64>, BTreeSet<String>),
) -> Result<(), Error> {
    let mut events: BTreeMap<i64, Stored> = BTreeMap::new();
    for seq in places {
        let body: Option<String> = tx
            .prepare_cached("SELECT body FROM _events WHERE seq = ?1")?
            .query_row(params![seq], |row| row.get(0))
            .optional()?;
        if let Some(body) = body {
            events.insert(seq, serde_json::from_str(&body)?);
        }
    }
    let keys = by_id
        .iter()
        .cloned()
        .chain(
            chosen
                .nodes
                .iter()
                .chain(&left.nodes)
                .map(|n| n.key.clone()),
        )
        .chain(
            chosen
                .links
                .iter()
                .chain(&left.links)
                .map(|(_, from, _)| from.clone()),
        );
    for named in keys {
        events.extend(naming_key(tx, &named)?);
    }
    let mut removed = Vec::new();
    for (seq, stored) in events {
        let id = stored.id.to_string();
        let kept = match &stored.change {
            Change::Ended { .. } | Change::Corrected { .. }
                if chosen.concluded.contains_key(&id) =>
            {
                None
            }
            change => without(change, stripped.get(&id), key, (chosen, left))?,
        };
        match kept {
            Some(change) if change == stored.change => {}
            Some(change) => {
                let body = serde_json::to_string(&Stored {
                    id: stored.id,
                    change,
                })?;
                tx.prepare_cached("UPDATE _events SET body = ?1 WHERE seq = ?2")?
                    .execute(params![body, seq])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM _events WHERE seq = ?1")?
                    .execute(params![seq])?;
                removed.push(id);
            }
        }
    }
    for id in removed {
        underive(tx, &id)?;
        tx.prepare_cached("DELETE FROM _derived WHERE event = ?1")?
            .execute(params![id])?;
    }
    Ok(())
}

/// Drops the removed event `id` from the derived-from list of every node and link fact in the
/// log that names it. `_derived` names each fact that lists `id`; that fact's hearings and their
/// aliases name the events whose bodies carry it.
fn underive(tx: &Connection, id: &str) -> Result<(), Error> {
    let facts: Vec<(String, String, String)> = tx
        .prepare_cached("SELECT class, key, to_key FROM _derived WHERE event = ?1")?
        .query_map(params![id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<Result<_, _>>()?;
    for (class, key, to_key) in facts {
        let (entity, to) = if to_key.is_empty() {
            (node_entity(&class), None)
        } else {
            (link_entity(&class), Some(to_key))
        };
        let bodies: Vec<(i64, String)> = tx
            .prepare_cached(
                "SELECT seq, body FROM _events WHERE id IN ( \
                 SELECT event FROM _hearings WHERE entity = ?1 AND key = ?2 AND to_key IS ?3 \
                 UNION SELECT a.alias FROM _aliases a JOIN _hearings h \
                 ON h.identity = a.identity \
                 WHERE h.entity = ?1 AND h.key = ?2 AND h.to_key IS ?3)",
            )?
            .query_map(params![entity, key, to], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<_, _>>()?;
        for (seq, body) in bodies {
            let mut stored: Stored = serde_json::from_str(&body)?;
            let Change::Wrote { added, .. } = &mut stored.change else {
                continue;
            };
            let mut changed = false;
            for fact in added {
                let derived_from = match fact {
                    Fact::Node(node) => &mut node.derived_from,
                    Fact::Link(link) | Fact::Opened(link) => &mut link.derived_from,
                };
                let before = derived_from.len();
                derived_from.retain(|event| event.to_string() != id);
                changed |= derived_from.len() != before;
            }
            if changed {
                tx.prepare_cached("UPDATE _events SET body = ?1 WHERE seq = ?2")?
                    .execute(params![serde_json::to_string(&stored)?, seq])?;
            }
        }
    }
    Ok(())
}

/// Runs `purge` on `conn` with `secure_delete` on, so every page it frees or rewrites is zeroed,
/// and restores the setting after.
fn secure<T>(
    conn: &mut Connection,
    purge: impl FnOnce(&mut Connection) -> Result<T, Error>,
) -> Result<T, Error> {
    let before: i64 = conn.query_row("PRAGMA secure_delete", [], |row| row.get(0))?;
    conn.pragma_update(None, "secure_delete", 1)?;
    let purged = purge(conn);
    conn.pragma_update(None, "secure_delete", before)?;
    purged
}

/// Checkpoints the write-ahead log of `conn`, truncating it, so no purged byte stays in it.
///
/// # Errors
/// [`Error::Refused`] when a reader still open kept the checkpoint from truncating the log once
/// the busy handler gave up: the purge committed, and the log holds purged bytes until a
/// checkpoint runs after the readers close.
fn truncate_log(conn: &Connection) -> Result<(), Error> {
    let busy: i64 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    if busy != 0 {
        return Err(Error::Refused(format!(
            "the purge committed, but the write-ahead log of {} still holds purged bytes: a \
             reader is open, so the log could not be truncated; running the checkpoint again \
             once the readers close clears it",
            conn.path().unwrap_or("the graph file")
        )));
    }
    Ok(())
}

/// Purges the snapshot file at `path` as the store was purged by the purge event `purged`, keeps
/// that event's rules in it in the same transaction, so a rebuild from the snapshot alone keeps
/// purging, and truncates its log.
fn purge_snapshot(path: &Path, key: &Key, target: &Purge, purged: &Event) -> Result<(), Error> {
    let mut snapshot = open_keyed(path, key)?;
    secure(&mut snapshot.conn, |conn| {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        purge_in(&tx, &Ontology::load(&tx)?, key, target)?;
        if let Change::Purged {
            rule, purge, parts, ..
        } = &purged.change
        {
            stand_purged(&tx, rule, (purge, parts), purged.id)?;
        }
        tx.commit()?;
        Ok(())
    })?;
    truncate_log(&snapshot.conn)
}

impl Graph {
    /// Purges `target` under the app's own `rule`, from `source`: what it matches leaves the
    /// store and every snapshot the store made, with nothing of it left. Returns the purge event,
    /// which names the rule and the kind with each target as its keyed match value; a purge that
    /// matches nothing still writes it, as a standing rule. The store's removal lands in one
    /// transaction or not at all; each snapshot that still exists is purged after it, even when
    /// the store's log could not be truncated, and one recorded that no longer exists is skipped.
    ///
    /// # Errors
    /// [`Error::KeyMissing`] without a key; [`Error::Refused`] when the target's class or
    /// relation is not declared; [`Error::Refused`] when the purge committed but a reader still
    /// open on the store kept its write-ahead log from being truncated, so the log holds purged
    /// bytes until a checkpoint runs after the readers close; [`Error::Refused`] naming the
    /// snapshot file when a snapshot could not be purged or its log truncated, after every other
    /// snapshot was tried; the graph file cannot be read or written.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the interface #85 names takes it by value"
    )]
    pub fn purge(&mut self, target: Purge, rule: &str, source: &str) -> Result<Event, Error> {
        let key = self.key.clone().ok_or(Error::KeyMissing)?;
        let (event, snapshots) = secure(&mut self.conn, |conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let parts = purge_in(&tx, &Ontology::load(&tx)?, &key, &target)?;
            let matched = target.matched(&key);
            let parts: Vec<NodeRef> = parts.iter().map(|part| matched_node(&key, part)).collect();
            let event = append(
                &tx,
                Change::Purged {
                    source: source.to_string(),
                    rule: rule.to_string(),
                    purge: matched.clone(),
                    parts: parts.clone(),
                },
            )?;
            stand_purged(&tx, rule, (&matched, &parts), event.id)?;
            let snapshots: Vec<String> = tx
                .prepare_cached("SELECT path FROM _snapshots ORDER BY path")?
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            tx.commit()?;
            Ok((event, snapshots))
        })?;
        // The purge committed: every snapshot is purged whether or not the store's log could be
        // truncated, and the store's error comes first.
        let store = truncate_log(&self.conn);
        let mut failed = None;
        for path in snapshots
            .into_iter()
            .map(PathBuf::from)
            .filter(|p| p.exists())
        {
            if let Err(error) = purge_snapshot(&path, &key, &target, &event) {
                failed.get_or_insert_with(|| {
                    Error::Refused(format!(
                        "the snapshot {} was not purged: {error}",
                        path.display()
                    ))
                });
            }
        }
        store?;
        failed.map_or(Ok(event), Err)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Change, Class, Cursor, Declaration, Error, Event, EventId, Fact, LinkFact, NodeFact,
        NodeRef, Purge, Relation, Target, Unit, ValidTime, Write, open, rebuild,
    };

    /// A graph of people who know people, and rooms part of houses and shelves part of rooms.
    fn people(path: &std::path::Path) -> Result<crate::Graph, Error> {
        let mut g = open(path, Some(&crate::TEST_KEY))?;
        for d in [
            Declaration::Class(Class::new("person", [] as [&str; 0])),
            Declaration::Class(Class::new("house", [] as [&str; 0])),
            Declaration::Class(Class::new("room", [] as [&str; 0])),
            Declaration::Class(Class::new("shelf", [] as [&str; 0])),
            Declaration::Relation(Relation::new("knows", "person", "person", false)),
            Declaration::Relation(Relation::new("in_house", "room", "house", true)),
            Declaration::Relation(Relation::new("in_room", "shelf", "room", true)),
        ] {
            g.declare(&d)?;
        }
        Ok(g)
    }

    fn bare(class: &str, key: &str) -> Unit {
        Unit::new(class, key, [] as [(&str, &str); 0], ValidTime::Unknown)
    }

    /// `change` as the next event after `events`.
    fn after(events: &[Event], change: Change) -> Event {
        Event {
            id: EventId::now(),
            cursor: Cursor(events.last().map_or(0, |e| e.cursor.0) + 1),
            change,
        }
    }

    #[test]
    fn an_ending_or_correction_a_rule_matches_is_refused_and_dropped_on_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = people(&dir.path().join("graph.rannoch"))?;
        let knows = |p: &str| bare("person", p).link("knows", p, "p2", ValidTime::Unknown);
        g.write(&Write::new("a").unit(bare("person", "p2")).unit(knows("p1")))?;
        g.write(&Write::new("b").unit(knows("p1")))?;
        let link = Target::Link {
            relation: "knows".into(),
            from: NodeRef::new("person", "p1"),
            to: NodeRef::new("person", "p2"),
        };
        g.purge(
            Purge::EntityFromSource {
                target: link,
                source: "b".into(),
            },
            "b-forgets",
            "app",
        )?;
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(9);
        for refused in [
            g.end_link("knows", "p1", "p2", at, ValidTime::Unknown, "b"),
            g.correct_link("knows", "p1", "p2", "b"),
        ] {
            let Err(Error::Refused(why)) = refused else {
                panic!("a conclusion a rule matches was recorded: {refused:?}");
            };
            assert!(
                why.contains("b-forgets") && why.contains("nothing was recorded"),
                "{why}"
            );
        }
        assert_eq!(g.link_end("knows", "p1", "p2")?, None);
        g.end_link("knows", "p1", "p2", at, ValidTime::Unknown, "a")?;

        let mut events = g.events().since(Cursor::START)?;
        let (from, to) = (NodeRef::new("person", "p1"), NodeRef::new("person", "p2"));
        let ended = Change::Ended {
            source: "b".into(),
            relation: "knows".into(),
            from: from.clone(),
            to: to.clone(),
            at: 10_000,
            valid: ValidTime::Unknown,
        };
        events.push(after(&events, ended));
        let corrected = Change::Corrected {
            source: "b".into(),
            relation: "knows".into(),
            from,
            to,
        };
        events.push(after(&events, corrected));
        let rebuilt = rebuild(
            &events,
            dir.path().join("rebuilt.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        assert_eq!(
            rebuilt.events().since(Cursor::START)?.len(),
            events.len() - 2,
            "the matched ending and correction are not stored"
        );
        assert_eq!(
            rebuilt.link_end("knows", "p1", "p2")?,
            g.link_end("knows", "p1", "p2")?
        );
        Ok(())
    }

    #[test]
    fn a_part_of_a_purged_whole_is_purged_on_arrival_at_any_depth()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = people(&dir.path().join("graph.rannoch"))?;
        g.write(
            &Write::new("app")
                .unit(bare("house", "h1"))
                .unit(bare("house", "h2")),
        )?;
        g.purge(
            Purge::Entity {
                target: Target::Node(NodeRef::new("house", "h1")),
            },
            "h1-gone",
            "app",
        )?;
        let room = bare("room", "r1").link("in_house", "r1", "h1", ValidTime::Unknown);
        let shelf = bare("shelf", "s1").link("in_room", "s1", "r1", ValidTime::Unknown);
        assert_eq!(g.write(&Write::new("app").unit(room))?, vec![]);
        assert_eq!(g.write(&Write::new("app").unit(shelf))?, vec![]);
        let kept = bare("room", "r2").link("in_house", "r2", "h2", ValidTime::Unknown);
        assert_eq!(g.write(&Write::new("app").unit(kept))?.len(), 1);
        assert_eq!(g.find("room", "r1")?, None);
        assert_eq!(g.find("shelf", "s1")?, None);
        assert!(g.find("room", "r2")?.is_some());

        // On replay, as on the write path.
        let mut events = g.events().since(Cursor::START)?;
        let unit = |class: &str, key: &str, relation: &str, whole: (&str, &str)| Change::Wrote {
            source: "app".into(),
            added: vec![
                Fact::Node(NodeFact {
                    class: class.into(),
                    key: key.into(),
                    properties: std::collections::BTreeMap::new(),
                    valid: ValidTime::Unknown,
                    time: None,
                    integer: None,
                    derived_from: Vec::new(),
                }),
                Fact::Opened(LinkFact {
                    relation: relation.into(),
                    from: NodeRef::new(class, key),
                    to: NodeRef::new(whole.0, whole.1),
                    valid: ValidTime::Unknown,
                    derived_from: Vec::new(),
                }),
            ],
            wrong_kind: Vec::new(),
            check: None,
        };
        events.push(after(
            &events,
            unit("room", "r3", "in_house", ("house", "h1")),
        ));
        events.push(after(
            &events,
            unit("shelf", "s3", "in_room", ("room", "r3")),
        ));
        let rebuilt = rebuild(
            &events,
            dir.path().join("rebuilt.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        assert_eq!(rebuilt.find("room", "r3")?, None);
        assert_eq!(rebuilt.find("shelf", "s3")?, None);
        assert_eq!(
            rebuilt.events().since(Cursor::START)?.len(),
            events.len() - 2
        );
        Ok(())
    }

    #[test]
    fn a_part_a_purge_removed_stays_purged_when_it_comes_back_and_after_a_rebuild()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = people(&dir.path().join("graph.rannoch"))?;
        g.write(&Write::new("app").unit(bare("house", "h1")))?;
        g.write(&Write::new("app").unit(bare("room", "r1").link(
            "in_house",
            "r1",
            "h1",
            ValidTime::Unknown,
        )))?;
        g.write(&Write::new("app").unit(bare("shelf", "s1").link(
            "in_room",
            "s1",
            "r1",
            ValidTime::Unknown,
        )))?;
        let event = g.purge(
            Purge::Entity {
                target: Target::Node(NodeRef::new("house", "h1")),
            },
            "h1-gone",
            "app",
        )?;
        let Change::Purged { parts, .. } = &event.change else {
            panic!("{event:?}");
        };
        assert_eq!(parts.len(), 2, "the room and the shelf, as match values");
        let body = serde_json::to_string(&event.change)?;
        assert!(
            !body.contains("\"r1\"") && !body.contains("\"s1\""),
            "{body}"
        );

        let back = |g: &mut crate::Graph| -> Result<(), Box<dyn std::error::Error>> {
            let shelf = bare("shelf", "s1").link("in_room", "s1", "r1", ValidTime::Unknown);
            assert_eq!(g.write(&Write::new("app").unit(shelf))?, vec![]);
            assert_eq!(
                g.write(&Write::new("app").unit(bare("room", "r1")))?,
                vec![]
            );
            assert_eq!(g.find("shelf", "s1")?, None);
            assert_eq!(g.find("room", "r1")?, None);
            Ok(())
        };
        let events = g.events().since(Cursor::START)?;
        back(&mut g)?;
        let mut rebuilt = rebuild(
            &events,
            dir.path().join("rebuilt.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        back(&mut rebuilt)?;
        Ok(())
    }

    #[test]
    fn a_snapshot_taken_before_a_purge_and_rebuilt_alone_keeps_purging()
    -> Result<(), Box<dyn std::error::Error>> {
        const LATE: &str = "zq-late-copy-88";
        let dir = tempfile::tempdir()?;
        let mut g = people(&dir.path().join("graph.rannoch"))?;
        let copy = || bare("house", LATE);
        g.write(&Write::new("app").unit(copy()).unit(bare("house", "h2")))?;
        let snapshot = g.snapshot(g.events().committed()?, dir.path().join("snapshot.rannoch"))?;
        g.purge(
            Purge::Entity {
                target: Target::Node(NodeRef::new("house", LATE)),
            },
            "late-gone",
            "app",
        )?;
        let mut rebuilt = snapshot.rebuild(
            &[],
            dir.path().join("rebuilt.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        assert_eq!(rebuilt.write(&Write::new("app").unit(copy()))?, vec![]);
        assert_eq!(rebuilt.find("house", LATE)?, None);
        assert!(rebuilt.hearings(&NodeRef::new("house", LATE))?.is_empty());
        for event in rebuilt.events().since(Cursor::START)? {
            let body = serde_json::to_string(&event.change)?;
            assert!(!body.contains(LATE), "{body}");
        }
        assert!(rebuilt.find("house", "h2")?.is_some());
        Ok(())
    }

    #[test]
    fn a_purged_snapshot_rebuilt_with_the_purge_event_keeps_one_row_per_rule()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut g = people(&dir.path().join("graph.rannoch"))?;
        g.write(&Write::new("app").unit(bare("house", "h1")))?;
        let room = bare("room", "r1").link("in_house", "r1", "h1", ValidTime::Unknown);
        g.write(&Write::new("app").unit(room))?;
        let at = g.events().committed()?;
        let snapshot = g.snapshot(at, dir.path().join("snapshot.rannoch"))?;
        let target = Target::Node(NodeRef::new("house", "h1"));
        g.purge(Purge::Entity { target }, "h1-gone", "app")?;
        // The purged snapshot holds the purge's rules, and the purge event replays them again.
        let later: Vec<Event> = g
            .events()
            .since(at)?
            .into_iter()
            .filter(|e| e.cursor > at)
            .collect();
        assert_eq!(later.len(), 1);
        let rebuilt = snapshot.rebuild(
            &later,
            dir.path().join("rebuilt.rannoch"),
            Some(&crate::TEST_KEY),
        )?;
        let rows = |g: &crate::Graph| -> rusqlite::Result<i64> {
            g.conn
                .query_row("SELECT count(*) FROM _purge_rules", [], |row| row.get(0))
        };
        assert_eq!(rows(&g)?, 2, "the house's rule and the room's as a part");
        assert_eq!(rows(&rebuilt)?, 2);
        Ok(())
    }

    #[test]
    fn a_store_that_purged_before_rules_were_kept_takes_them_at_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        {
            let mut g = people(&path)?;
            g.write(&Write::new("app").unit(bare("person", "p1")))?;
            crate::events::forget_as_before(&mut g, &NodeRef::new("person", "p1"))?;
            g.write(&Write::new("app").unit(bare("house", "h1")))?;
            let target = Target::Node(NodeRef::new("house", "h1"));
            g.purge(Purge::Entity { target }, "h1-gone", "app")?;
            // As a store written before its rules were kept.
            g.conn.execute_batch("DELETE FROM _purge_rules")?;
        }
        let mut g = open(&path, Some(&crate::TEST_KEY))?;
        assert_eq!(
            g.write(&Write::new("app").unit(bare("house", "h1")))?,
            vec![]
        );
        assert_eq!(
            g.write(&Write::new("app").unit(bare("person", "p1")))?,
            vec![]
        );
        assert_eq!(g.find("house", "h1")?, None);
        assert_eq!(g.find("person", "p1")?, None);
        Ok(())
    }

    const TOKEN: &[u8] = b"zq-reader-token";

    #[test]
    fn a_purge_with_a_reader_open_says_the_log_still_holds_purged_bytes_and_purges_snapshots()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        let mut g = open(&path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("note", ["text"])))?;
        g.write(&Write::new("app").unit(Unit::new(
            "note",
            "n1",
            [("text", "zq-reader-token")],
            ValidTime::Unknown,
        )))?;
        let snapshot_path = dir.path().join("snapshot.rannoch");
        g.snapshot(g.events().committed()?, &snapshot_path)?;
        let reader = rusqlite::Connection::open(&path)?;
        reader.execute_batch("BEGIN; SELECT count(*) FROM _events;")?;
        let target = Target::Node(NodeRef::new("note", "n1"));
        let Err(Error::Refused(why)) = g.purge(Purge::Entity { target }, "rule", "app") else {
            panic!("a purge with a reader open reported that the log was truncated");
        };
        assert!(
            why.contains("the purge committed") && why.contains("still holds purged bytes"),
            "{why}"
        );
        assert_eq!(g.find("note", "n1")?, None, "the purge itself committed");
        reader.execute_batch("COMMIT")?;
        let snapshot = std::fs::read(&snapshot_path)?;
        assert!(
            !snapshot.windows(TOKEN.len()).any(|w| w == TOKEN),
            "the snapshot still holds the purged token"
        );
        let wal = std::fs::read(format!("{}-wal", snapshot_path.display())).unwrap_or_default();
        assert!(!wal.windows(TOKEN.len()).any(|w| w == TOKEN));
        Ok(())
    }

    #[test]
    fn a_purged_event_leaves_no_copy_of_its_id_in_the_facts_derived_from_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.rannoch");
        let mut g = open(&path, Some(&crate::TEST_KEY))?;
        g.declare(&Declaration::Class(Class::new("note", ["text"])))?;
        g.declare(&Declaration::Relation(Relation::new(
            "cites", "note", "note", false,
        )))?;
        let note = |key: &str| Unit::new("note", key, [("text", "words")], ValidTime::Unknown);
        let source = g.write(&Write::new("app").unit(note("e")))?[0].id;
        g.write(
            &Write::new("app")
                .unit(note("n2"))
                .unit(note("n1").link("cites", "n1", "n2", ValidTime::Unknown))
                .derived_from([source]),
        )?;
        let snapshot_path = dir.path().join("snapshot.rannoch");
        g.snapshot(g.events().committed()?, &snapshot_path)?;
        let target = Target::Node(NodeRef::new("note", "e"));
        g.purge(Purge::Entity { target }, "retention", "app")?;

        let id = source.to_string();
        for file in [
            path.clone(),
            dir.path().join("graph.rannoch-wal"),
            snapshot_path.clone(),
            dir.path().join("snapshot.rannoch-wal"),
        ] {
            let bytes = std::fs::read(&file).unwrap_or_default();
            assert!(
                !bytes.windows(id.len()).any(|w| w == id.as_bytes()),
                "{} still holds the purged event's id",
                file.display()
            );
        }
        for graph in [&g, &open(&snapshot_path, Some(&crate::TEST_KEY))?] {
            let found = graph.find("note", "n1")?.ok_or("n1 was purged")?;
            assert!(found.derived_from.is_empty());
            let links: Vec<LinkFact> = graph
                .events()
                .since(Cursor::START)?
                .into_iter()
                .flat_map(|e| match e.change {
                    Change::Wrote { added, .. } => added,
                    _ => Vec::new(),
                })
                .filter_map(|fact| match fact {
                    Fact::Link(link) => Some(link),
                    _ => None,
                })
                .collect();
            assert_eq!(links.len(), 1, "the link remains");
            assert!(links[0].derived_from.is_empty());
        }
        Ok(())
    }
}
