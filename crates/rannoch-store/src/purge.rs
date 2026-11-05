//! Purging: erasure of a fact from the store, rare and selective, and the rule that stands after
//! it. What a purge matches leaves the log and every table kept from it, and each later event the
//! rule matches is never stored. The same words decide both: what an event says about itself, each
//! as a keyed match value, which the purge compares with its targets and a standing rule with what
//! arrives.

use std::collections::BTreeSet;

use rannoch_cloudevents::{EventId, Fact, attributes, read, source_back};
use rannoch_ontology::{NodeRef, Ontology};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::handles::{Handles, Run};
use crate::history::{
    Key, Of, Subject, amendment_target, conclude, derive, keep_noted, link_ends, link_entity,
    merge, naming, newest, node_entity, renames_in, take_noted,
};
use crate::inbox::{AMENDED, CLOSED_PREFIX, DECLARED_PREFIX, REFILLED};
use crate::log::{Cursor, Event, Events, event_at, store};
use crate::ontology::load;
use crate::queries::{
    CONCLUDED_BY, Catalogue, DROP_ALIASES, DROP_AMENDMENT, DROP_CORRECTIONS, DROP_CORRECTIONS_AT,
    DROP_ENDINGS, DROP_ENDINGS_AT, DROP_ENDS, DROP_ENDS_AT, DROP_ENDS_OF, DROP_EVENT,
    DROP_HEARINGS, DROP_RENAMES, DROP_WAITING, DROP_WORK, HEARINGS_AT, IDENTITY_AT,
    LATEST_CONCLUSION, LINKS_AT, PLACES_OF_IDENTITY, RENAMES_BY, Sql, TARGET_OF, TRUNCATE_LOG,
    UNCONCLUDED, part_keys,
};
use crate::{Error, Graph, refresh};

/// The type of the event a purge writes: its rule, its kind and its targets as keyed match values.
pub const PURGED: &str = "rannoch.purged";

/// What a purge names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// One fact, by its content identity. An amended version's identity names the event it was
    /// heard from: that event and every amendment of it leave.
    Fact { identity: [u8; 32] },
    /// A write source's stream: every event with this source, an app's own events and amendments
    /// among them.
    Stream { source: String },
    /// A node under every key it was renamed from or to, with every node part of it and all their
    /// links.
    Node(NodeRef),
    /// A link, under every key its ends were renamed from or to.
    Link {
        relation: String,
        from: String,
        to: String,
    },
    /// One source's word about a node: its hearings of it.
    NodeWord { node: NodeRef, source: String },
    /// One source's word about a link: its hearings, endings and corrections of it.
    LinkWord {
        relation: String,
        from: String,
        to: String,
        source: String,
    },
    /// One source's word everywhere: its hearings, endings and corrections of every node and link.
    Source { source: String },
}

/// What kind of thing a match value names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Fact,
    Stream,
    Node,
    Link,
    NodeWord,
    LinkWord,
    Source,
    Event,
}

impl Kind {
    /// Its name, as a purge event and the rules table hold it and its match value is hashed under.
    fn as_str(self) -> &'static str {
        match self {
            Kind::Fact => "fact",
            Kind::Stream => "stream",
            Kind::Node => "node",
            Kind::Link => "link",
            Kind::NodeWord => "node_word",
            Kind::LinkWord => "link_word",
            Kind::Source => "source",
            Kind::Event => "event",
        }
    }
}

/// One keyed match value: what kind of thing it names, and the value.
type Word = (Kind, [u8; 32]);

/// A rule's match value as a purge event holds it.
#[derive(Serialize, Deserialize)]
struct Matched {
    kind: Kind,
    #[serde(rename = "match")]
    value: String,
}

/// What a purge event's data holds: nothing but the rule, the kind, and match values: the target,
/// and what else the rule stands against (a part, another name of a key, an event that left).
#[derive(Serialize, Deserialize)]
struct Purged {
    rule: String,
    kind: Kind,
    #[serde(rename = "match")]
    value: String,
    also: Vec<Matched>,
}

/// How many events a purge reads from the log at a time.
const CHUNK: usize = 4096;

fn hex(value: &[u8; 32]) -> String {
    blake3::Hash::from_bytes(*value).to_hex().to_string()
}

fn node_word(key: &Key, kind: Kind, class: &str, name: &str, by: &[&str]) -> Word {
    let parts: Vec<&str> = [class, name]
        .into_iter()
        .chain(by.iter().copied())
        .collect();
    (kind, key.matched(kind.as_str(), &parts))
}

fn link_word(key: &Key, kind: Kind, relation: &str, (from, to): (&str, &str), by: &[&str]) -> Word {
    let parts: Vec<&str> = [relation, from, to]
        .into_iter()
        .chain(by.iter().copied())
        .collect();
    (kind, key.matched(kind.as_str(), &parts))
}

fn source_word(key: &Key, kind: Kind, source: &str) -> Word {
    (kind, key.matched(kind.as_str(), &[source]))
}

fn event_word(key: &Key, source: &str, id: &str) -> Word {
    (
        Kind::Event,
        key.matched(Kind::Event.as_str(), &[source, id]),
    )
}

fn fact_word(key: &Key, identity: &[u8; 32]) -> Word {
    (
        Kind::Fact,
        key.matched(Kind::Fact.as_str(), &[&hex(identity)]),
    )
}

/// Whether events of type `ty` are the store's own record, which no rule matches: a declaration,
/// the rule event itself, or what fill appended.
fn own(ty: &str) -> bool {
    ty == PURGED
        || ty == REFILLED
        || ty.starts_with(DECLARED_PREFIX)
        || ty.starts_with(CLOSED_PREFIX)
}

/// What `fact` says about itself.
fn fact_words(key: &Key, fact: &Fact, words: &mut Vec<Word>) -> Result<(), Error> {
    match fact {
        Fact::Node(node) => {
            words.push(fact_word(key, &Of::Node(node).identity(key)?));
            words.push(source_word(key, Kind::Source, &node.source));
            words.push(node_word(key, Kind::Node, &node.class, &node.key, &[]));
            words.push(node_word(
                key,
                Kind::NodeWord,
                &node.class,
                &node.key,
                &[&node.source],
            ));
        }
        Fact::Link(link) => {
            words.push(fact_word(key, &Of::Link(link).identity(key)?));
            link_words(
                key,
                (&link.relation, &link.from, &link.to),
                &link.source,
                words,
            );
        }
        Fact::Concluded(conclusion) => link_words(
            key,
            (&conclusion.relation, &conclusion.from, &conclusion.to),
            &conclusion.source,
            words,
        ),
        Fact::Declared | Fact::None => {}
    }
    Ok(())
}

fn link_words(
    key: &Key,
    (relation, from, to): (&str, &NodeRef, &NodeRef),
    source: &str,
    words: &mut Vec<Word>,
) {
    let ends = (from.key.as_str(), to.key.as_str());
    words.push(source_word(key, Kind::Source, source));
    words.push(link_word(key, Kind::Link, relation, ends, &[]));
    words.push(link_word(key, Kind::LinkWord, relation, ends, &[source]));
    words.push(node_word(key, Kind::Node, &from.class, &from.key, &[]));
    words.push(node_word(key, Kind::Node, &to.class, &to.key, &[]));
}

/// What the event `body` says about itself, each as a keyed match value: its own name and source,
/// and its fact; for an amendment, the event it names and that event's fact as patched. A body
/// that is no event, or that is the store's own record, says nothing.
fn words(tx: &impl Run, key: &Key, body: &str) -> Result<Vec<Word>, Error> {
    let Ok(event) = attributes(body) else {
        return Ok(Vec::new());
    };
    if own(&event.ty) {
        return Ok(Vec::new());
    }
    let source = source_back(&event.source).unwrap_or_else(|_| event.source.clone());
    let mut words = vec![
        event_word(key, &event.source, &event.id),
        source_word(key, Kind::Stream, &source),
    ];
    if event.ty == AMENDED {
        amendment_words(tx, key, body, &mut words)?;
    } else if let Ok(fact) = read(body) {
        fact_words(key, &fact, &mut words)?;
    }
    Ok(words)
}

/// What the amendment `body` names: its target, and its target's fact with its patch applied, so
/// a patch cannot carry a fact back onto what a rule refuses.
fn amendment_words(
    tx: &impl Run,
    key: &Key,
    body: &str,
    words: &mut Vec<Word>,
) -> Result<(), Error> {
    let amendment: serde_json::Value = serde_json::from_str(body)?;
    let Some((source, id)) = amendment_target(&amendment) else {
        return Ok(());
    };
    words.push(event_word(key, source, id));
    let written: Option<String> = tx.with(Sql::EventByCloudevent, |s| {
        s.query_row(params![source, id], |row| row.get(1))
            .optional()
    })?;
    let patch = amendment.get("data").and_then(|data| data.get("patch"));
    let (Some(written), Some(patch)) = (written, patch) else {
        return Ok(());
    };
    let mut patched: serde_json::Value = serde_json::from_str(&written)?;
    merge(&mut patched, patch);
    if let Ok(fact) = read(&patched.to_string()) {
        fact_words(key, &fact, words)?;
    }
    Ok(())
}

/// Keeps the rule `rule` against `word`. The first rule kept for a word stays.
fn stand(tx: &impl Run, rule: &str, word: &Word) -> Result<(), Error> {
    tx.with(Sql::StandRule, |s| {
        s.execute(params![word.0.as_str(), word.1.as_slice(), rule])
    })?;
    Ok(())
}

/// The standing rule that matches the event `body`, if one does.
fn standing(tx: &impl Run, key: &Key, body: &str) -> Result<Option<String>, Error> {
    for (kind, value) in words(tx, key, body)? {
        let rule = tx.with(Sql::RuleOf, |s| {
            s.query_row(params![kind.as_str(), value.as_slice()], |row| row.get(0))
                .optional()
        })?;
        if rule.is_some() {
            return Ok(rule);
        }
    }
    Ok(None)
}

/// The rule that refuses `body`, arriving now, if one does. An amendment of it is refused from
/// here on under the same rule, and any that already waits for it goes, so no patch of what was
/// never stored is kept.
pub(crate) fn refused(tx: &impl Run, key: &Key, body: &str) -> Result<Option<String>, Error> {
    let Some(rule) = standing(tx, key, body)? else {
        return Ok(None);
    };
    if let Ok(event) = attributes(body) {
        stand(tx, &rule, &event_word(key, &event.source, &event.id))?;
        let named = params![event.source, event.id];
        let waiting: Vec<i64> = tx.with(Sql::WaitingFor, |s| {
            s.query_map(named, |row| row.get(0))?.collect()
        })?;
        tx.with(Sql::StopWaiting, |s| s.execute(named))?;
        for seq in waiting {
            remove_event(tx, seq)?;
        }
    }
    Ok(Some(rule))
}

/// Takes the rules of `body` when it is a purge event, as a store does that replays a log. Says
/// whether it was one. It keeps rules and removes nothing: what another store already holds is the
/// app's to purge there.
pub(crate) fn heard_rule(tx: &impl Run, body: &str) -> Result<bool, Error> {
    #[derive(Deserialize)]
    struct Body {
        data: Purged,
    }
    if !attributes(body).is_ok_and(|event| event.ty == PURGED) {
        return Ok(false);
    }
    let Body { data } = serde_json::from_str(body)?;
    let primary = Matched {
        kind: data.kind,
        value: data.value,
    };
    for matched in data.also.iter().chain([&primary]) {
        let value = blake3::Hash::from_hex(&matched.value).map_err(|_| {
            Error::Refused(format!("purge rule {} holds a bad match value", data.rule))
        })?;
        stand(tx, &data.rule, &(matched.kind, *value.as_bytes()))?;
    }
    Ok(true)
}

/// Every key the node `key` of `class` went by: every key a rename connects it to, in either
/// direction.
fn names_of(tx: &impl Run, class: &str, key: &str) -> Result<Vec<String>, Error> {
    let renames = renames_in(tx, &node_entity(class))?;
    let mut names = BTreeSet::from([key.to_string()]);
    let mut frontier = vec![key.to_string()];
    while let Some(name) = frontier.pop() {
        for (old, new, ..) in &renames {
            let other = if *old == name {
                new
            } else if *new == name {
                old
            } else {
                continue;
            };
            if names.insert(other.clone()) {
                frontier.push(other.clone());
            }
        }
    }
    Ok(names.into_iter().collect())
}

/// `whole` and every node part of it at any depth, each under every key it went by. A part is a
/// node a containment link in its relation's table, or still waiting open, ties to the whole.
fn with_parts(
    run: &Handles<'_>,
    ontology: &Ontology,
    whole: &NodeRef,
) -> Result<BTreeSet<NodeRef>, Error> {
    let mut all = BTreeSet::new();
    let mut frontier = vec![whole.clone()];
    while let Some(node) = frontier.pop() {
        for key in names_of(run, &node.class, &node.key)? {
            let named = NodeRef::new(&node.class, &key);
            if !all.insert(named) {
                continue;
            }
            for relation in ontology.relations.values() {
                if !relation.containment || relation.to != node.class {
                    continue;
                }
                let held: Vec<String> = run
                    .conn()
                    .prepare_cached(&part_keys(&relation.name, &relation.from, &relation.to))?
                    .query_map([&key], |row| row.get(0))?
                    .collect::<Result<_, _>>()?;
                let open: Vec<String> = run.with(Sql::WaitingOnTarget, |s| {
                    s.query_map(params![relation.name, key], |row| row.get(0))?
                        .collect()
                })?;
                frontier.extend(
                    held.into_iter()
                        .chain(open)
                        .map(|part| NodeRef::new(&relation.from, part)),
                );
            }
        }
    }
    Ok(all)
}

/// The target's match value as the rule event records it, and every word it stands
/// against in this store: the target under each key it went by, its parts, and for an amended
/// version's identity, the fact and event it was heard from.
fn matching(
    run: &Handles<'_>,
    ontology: &Ontology,
    key: &Key,
    target: &Target,
) -> Result<(Word, BTreeSet<Word>), Error> {
    let link_ends =
        |relation: &str, from: &str, to: &str| -> Result<(Vec<String>, Vec<String>), Error> {
            let relation = ontology.relation(relation)?;
            Ok((
                names_of(run, &relation.from, from)?,
                names_of(run, &relation.to, to)?,
            ))
        };
    let mut set = BTreeSet::new();
    let primary = match target {
        Target::Fact { identity } => {
            let word = fact_word(key, identity);
            set.insert(word);
            let places: Vec<i64> = run
                .conn()
                .prepare_cached(PLACES_OF_IDENTITY)?
                .query_map(params![identity], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            for seq in places {
                if let Some(event) = event_at(run, Cursor(seq))?
                    && let Ok(attributes) = attributes(&event.body)
                {
                    set.insert(event_word(key, &attributes.source, &attributes.id));
                }
                let own: Option<[u8; 32]> = run
                    .conn()
                    .prepare_cached(IDENTITY_AT)?
                    .query_row(params![seq], |row| row.get(0))
                    .optional()?;
                set.extend(own.map(|own| fact_word(key, &own)));
            }
            word
        }
        Target::Stream { source } => {
            let word = source_word(key, Kind::Stream, source);
            set.insert(word);
            word
        }
        Target::Source { source } => {
            let word = source_word(key, Kind::Source, source);
            set.insert(word);
            word
        }
        Target::Node(node) => {
            ontology.class(&node.class)?;
            for part in with_parts(run, ontology, node)? {
                set.insert(node_word(key, Kind::Node, &part.class, &part.key, &[]));
            }
            node_word(key, Kind::Node, &node.class, &node.key, &[])
        }
        Target::NodeWord { node, source } => {
            ontology.class(&node.class)?;
            for name in names_of(run, &node.class, &node.key)? {
                set.insert(node_word(
                    key,
                    Kind::NodeWord,
                    &node.class,
                    &name,
                    &[source],
                ));
            }
            node_word(key, Kind::NodeWord, &node.class, &node.key, &[source])
        }
        Target::Link { relation, from, to } => {
            let (froms, tos) = link_ends(relation, from, to)?;
            for from in &froms {
                for to in &tos {
                    set.insert(link_word(key, Kind::Link, relation, (from, to), &[]));
                }
            }
            link_word(key, Kind::Link, relation, (from, to), &[])
        }
        Target::LinkWord {
            relation,
            from,
            to,
            source,
        } => {
            let (froms, tos) = link_ends(relation, from, to)?;
            for from in &froms {
                for to in &tos {
                    set.insert(link_word(
                        key,
                        Kind::LinkWord,
                        relation,
                        (from, to),
                        &[source],
                    ));
                }
            }
            link_word(key, Kind::LinkWord, relation, (from, to), &[source])
        }
    };
    Ok((primary, set))
}

/// Removes the event at `seq` from the log, with the worker rows that name it, so `next` never
/// hands back a place that holds nothing and no worker holds the processed mark behind it.
pub(crate) fn remove_event(tx: &impl Run, seq: i64) -> Result<(), Error> {
    let conn = tx.conn();
    conn.prepare_cached(DROP_WORK)?.execute([seq])?;
    conn.prepare_cached(DROP_EVENT)?.execute([seq])?;
    Ok(())
}

/// Every event in the log, in order, a chunk at a time.
fn each_event(
    run: &Handles<'_>,
    mut f: impl FnMut(&Event) -> Result<(), Error>,
) -> Result<(), Error> {
    let log = Events { conn: run.conn() };
    let mut from = Cursor::START;
    loop {
        let events = log.chunk(from, Cursor(i64::MAX), CHUNK)?;
        let Some(last) = events.last() else {
            return Ok(());
        };
        from = last.cursor;
        for event in &events {
            f(event)?;
        }
    }
}

/// The places of what `set` matches, growing it with each event that leaves: first every event but
/// amendments, then the amendments, which an event leaving takes with it, whichever is first in
/// the log.
fn chosen(run: &Handles<'_>, key: &Key, set: &mut BTreeSet<Word>) -> Result<BTreeSet<i64>, Error> {
    let mut places = BTreeSet::new();
    for amendments in [false, true] {
        each_event(run, |event| {
            let amendment = attributes(&event.body).is_ok_and(|e| e.ty == AMENDED);
            if amendment != amendments {
                return Ok(());
            }
            let said = words(run, key, &event.body)?;
            if said.iter().any(|word| set.contains(word)) {
                places.insert(event.cursor.0);
                if let Ok(event) = attributes(&event.body) {
                    set.insert(event_word(key, &event.source, &event.id));
                }
            }
            Ok(())
        })?;
    }
    Ok(places)
}

/// What removing events leaves to put right.
#[derive(Default)]
struct Left {
    /// The nodes and links whose facts changed.
    touched: Vec<Subject>,
    /// The links that lost a conclusion: their latest is chosen again.
    concluded: BTreeSet<Subject>,
    /// The kept events an amendment that left had patched.
    amended: BTreeSet<i64>,
    /// The renames that left: entity, old key, new key.
    renames: Vec<(String, String, String)>,
}

/// Removes the event at `seq` and every row kept from it.
fn remove_with_rows(run: &Handles<'_>, seq: i64, left: &mut Left) -> Result<(), Error> {
    let conn = run.conn();
    let Some(event) = event_at(run, Cursor(seq))? else {
        return Ok(());
    };
    let heard: Vec<([u8; 32], Subject)> = conn
        .prepare_cached(HEARINGS_AT)?
        .query_map([seq], |row| {
            Ok((row.get(0)?, (row.get(1)?, row.get(2)?, row.get(3)?)))
        })?
        .collect::<Result<_, _>>()?;
    for (identity, of) in heard {
        conn.prepare_cached(DROP_ALIASES)?
            .execute(params![identity])?;
        left.touched.push(of);
    }
    conn.prepare_cached(DROP_HEARINGS)?.execute([seq])?;
    // A fact's rows are named by the uuid of its CloudEvent id, in the form the id parses to.
    let id = attributes(&event.body).map(|e| e.id).unwrap_or_default();
    let uuid = id.split(':').next().unwrap_or_default();
    let by = uuid
        .parse::<EventId>()
        .map_or_else(|_| uuid.to_string(), |parsed| parsed.to_string());
    let by = by.as_str();
    let concluded: Vec<Subject> = conn
        .prepare_cached(CONCLUDED_BY)?
        .query_map([by], |row| {
            Ok((row.get(0)?, row.get(1)?, Some(row.get::<_, String>(2)?)))
        })?
        .collect::<Result<_, _>>()?;
    for of in concluded {
        left.touched.push(of.clone());
        left.concluded.insert(of);
    }
    for statement in [DROP_ENDS, DROP_ENDINGS, DROP_CORRECTIONS] {
        conn.prepare_cached(statement)?.execute([by])?;
    }
    let targets: Vec<i64> = conn
        .prepare_cached(TARGET_OF)?
        .query_map([seq], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    left.amended.extend(targets);
    conn.prepare_cached(DROP_AMENDMENT)?.execute([seq])?;
    conn.prepare_cached(DROP_WAITING)?.execute([seq])?;
    let renames: Vec<(String, String, String)> = conn
        .prepare_cached(RENAMES_BY)?
        .query_map([by], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    left.renames.extend(renames);
    conn.prepare_cached(DROP_RENAMES)?.execute([by])?;
    remove_event(run, seq)
}

/// Undoes each rename that left, for what stays: a conclusion copied onto the new key goes, every
/// conclusion that names either key is kept again through the renames still held, and every event
/// that names either key is heard again.
fn undo_renames(run: &Handles<'_>, ontology: &Ontology, left: &mut Left) -> Result<(), Error> {
    if left.renames.is_empty() {
        return Ok(());
    }
    let conn = run.conn();
    let mut keys: BTreeSet<(String, String)> = BTreeSet::new();
    for (entity, old, new) in std::mem::take(&mut left.renames) {
        for (of, target) in link_ends(ontology, &entity) {
            let subjects: Vec<Subject> = conn
                .prepare_cached(UNCONCLUDED)?
                .query_map(params![of, new, target], |row| {
                    Ok((row.get(0)?, row.get(1)?, Some(row.get::<_, String>(2)?)))
                })?
                .collect::<Result<_, _>>()?;
            for of in subjects {
                left.touched.push(of.clone());
                left.concluded.insert(of);
            }
            for statement in [DROP_ENDS_AT, DROP_ENDINGS_AT, DROP_CORRECTIONS_AT] {
                conn.prepare_cached(statement)?
                    .execute(params![of, new, target])?;
            }
        }
        for name in [&old, &new] {
            left.amended.extend(naming(run, ontology, &entity, name)?);
            keys.insert((entity.clone(), name.clone()));
        }
    }
    let named = |end: &NodeRef| keys.contains(&(node_entity(&end.class), end.key.clone()));
    let mut concluded: Vec<Subject> = Vec::new();
    each_event(run, |event| {
        if let Ok(Fact::Concluded(conclusion)) = read(&event.body)
            && (named(&conclusion.from) || named(&conclusion.to))
        {
            conclude(run, ontology, &conclusion, true)?;
            concluded.push((
                link_entity(&conclusion.relation),
                conclusion.from.key,
                Some(conclusion.to.key),
            ));
        }
        Ok(())
    })?;
    left.touched.extend(concluded.iter().cloned());
    left.concluded.extend(concluded);
    Ok(())
}

/// Lets go of every link of each touched node that has no version left: its row leaves its
/// relation's table, and a link with hearings of its own is touched, so it waits open for the node.
fn unlink(run: &Handles<'_>, ontology: &Ontology, left: &mut Left) -> Result<(), Error> {
    let gone: Vec<Subject> = left
        .touched
        .iter()
        .filter(|(.., to)| to.is_none())
        .cloned()
        .collect();
    for (entity, key, _) in gone {
        if newest(run, (&entity, &key, None))?.is_some() {
            continue;
        }
        for (of, target) in link_ends(ontology, &entity) {
            let links: Vec<(String, String)> = run
                .conn()
                .prepare_cached(LINKS_AT)?
                .query_map(params![of, key, target], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?
                .collect::<Result<_, _>>()?;
            let relation = of.trim_start_matches(&link_entity(""));
            let remove = run.sql.relation(relation)?.remove.stmt();
            for (from, to) in links {
                run.with(remove, |s| s.execute([&from, &to]))?;
                left.touched.push((of.clone(), from, Some(to)));
            }
        }
    }
    Ok(())
}

/// Removes what fill appended about what left: a closing of a link that has no version left, or
/// with an end that has none, and a retry naming a place that holds nothing now.
fn drop_appended(
    run: &Handles<'_>,
    touched: &[Subject],
    places: &BTreeSet<i64>,
) -> Result<(), Error> {
    #[derive(Deserialize)]
    struct Closed {
        from: NodeRef,
        to: NodeRef,
    }
    #[derive(Deserialize)]
    struct Retried {
        cursor: i64,
    }
    let touched: BTreeSet<&Subject> = touched.iter().collect();
    let no_version = |of: Subject| -> Result<bool, Error> {
        Ok(touched.contains(&of) && newest(run, (&of.0, &of.1, of.2.as_deref()))?.is_none())
    };
    let mut appended = Vec::new();
    each_event(run, |event| {
        let Ok(attributes) = attributes(&event.body) else {
            return Ok(());
        };
        let data = || -> serde_json::Result<serde_json::Value> {
            Ok(serde_json::from_str::<serde_json::Value>(&event.body)?
                .get("data")
                .cloned()
                .unwrap_or_default())
        };
        let gone = if let Some(relation) = attributes.ty.strip_prefix(CLOSED_PREFIX) {
            let closed: Closed = serde_json::from_value(data()?)?;
            no_version((
                link_entity(relation),
                closed.from.key.clone(),
                Some(closed.to.key.clone()),
            ))? || no_version((node_entity(&closed.from.class), closed.from.key, None))?
                || no_version((node_entity(&closed.to.class), closed.to.key, None))?
        } else if attributes.ty == REFILLED {
            let retried: Retried = serde_json::from_value(data()?)?;
            places.contains(&retried.cursor)
        } else {
            false
        };
        if gone {
            appended.push(event.cursor.0);
        }
        Ok(())
    })?;
    for seq in appended {
        remove_event(run, seq)?;
    }
    Ok(())
}

impl Graph {
    /// Purges `target` under the app's own `rule`, from `source`: what it matches leaves the store
    /// with no event body, hearing row, content identity or orphaned event id left, in one
    /// immediate transaction under the connection's `secure_delete`, and the write-ahead log is
    /// then truncated. An amendment naming a purged event leaves with it. Returns the purge event,
    /// `rannoch.purged`, which holds its rule and kind plainly and each target only as a keyed
    /// match value; a purge that matches nothing still writes it, and the rule stands. From then
    /// on a matching event is never stored, on the write path or in fill.
    ///
    /// Copies outside the store (`copy_to`, replicas, backups) are the app's to purge. An event
    /// of an app's own that mentions a purged value, a logged answer among them, leaves only with
    /// its stream. With a queue open on the store, purge through it (`Queue::purge`): this handle
    /// cannot reach what a queue has read ahead.
    ///
    /// # Errors
    /// [`Error::Refused`] when the target's class or relation is not declared; [`Error::Refused`]
    /// when the purge committed but a reader still open kept the write-ahead log from truncating,
    /// so it holds purged bytes until a checkpoint runs after the readers close; the graph file
    /// cannot be read or written, and nothing is purged.
    pub fn purge(&mut self, target: &Target, rule: &str, source: &str) -> Result<Event, Error> {
        // So every part and rename the log holds is in the tables the purge reads them from.
        self.fill()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ontology = load(&tx)?;
        let sql = Catalogue::new(&ontology);
        let run = Handles::new(&tx, &sql);
        take_noted();
        let (primary, mut set) = matching(&run, &ontology, &self.key, target)?;
        let places = chosen(&run, &self.key, &mut set)?;
        let mut left = Left::default();
        for seq in &places {
            remove_with_rows(&run, *seq, &mut left)?;
        }
        undo_renames(&run, &ontology, &mut left)?;
        for seq in std::mem::take(&mut left.amended) {
            if !places.contains(&seq) {
                left.touched
                    .extend(derive(&run, (&ontology, &self.key), seq)?);
            }
        }
        for (entity, key, to) in std::mem::take(&mut left.concluded) {
            let to = to.unwrap_or_default();
            tx.prepare_cached(DROP_ENDS_OF)?
                .execute(params![entity, key, to])?;
            tx.prepare_cached(LATEST_CONCLUSION)?
                .execute(params![entity, key, to])?;
        }
        unlink(&run, &ontology, &mut left)?;
        left.touched.sort();
        left.touched.dedup();
        refresh(&run, &ontology, left.touched.clone())?;
        drop_appended(&run, &left.touched, &places)?;
        let also = set
            .iter()
            .filter(|word| **word != primary)
            .map(|(kind, value)| Matched {
                kind: *kind,
                value: hex(value),
            })
            .collect();
        let id = EventId::now();
        let data = Purged {
            rule: rule.to_string(),
            kind: primary.0,
            value: hex(&primary.1),
            also,
        };
        let body = serde_json::json!({
            "specversion": "1.0",
            "id": id.to_string(),
            "source": source,
            "type": PURGED,
            "data": data,
        })
        .to_string();
        let event = store(&run, id, None, &body)?;
        heard_rule(&run, &body)?;
        keep_noted(&run)?;
        drop(run);
        tx.commit()?;
        truncate_log(&self.conn)?;
        Ok(event)
    }
}

/// Checkpoints the write-ahead log, truncating it, so no purged byte stays in it.
fn truncate_log(conn: &rusqlite::Connection) -> Result<(), Error> {
    let busy: i64 = conn.query_row(TRUNCATE_LOG, [], |row| row.get(0))?;
    if busy != 0 {
        return Err(Error::Refused(
            "the purge committed, but a reader kept the write-ahead log from being truncated: it \
             holds purged bytes until a checkpoint runs after the readers close"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rannoch_cloudevents::EventId;
    use rannoch_ontology::{Class, Declaration, NodeRef, Relation};
    use serde_json::json;

    use super::{PURGED, Target};
    use crate::{Cursor, Error, Graph, View, open};

    const KEY: [u8; 32] = [9; 32];
    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    /// A store with authors, books and chapters, a chapter part of its book.
    fn store(dir: &tempfile::TempDir, name: &str) -> Result<Graph> {
        let mut g = open(dir.path().join(name), &KEY)?;
        let declared = |name: &str| {
            json!({"specversion": "1.0", "id": format!("d-{name}"), "source": "rannoch:shop",
                   "type": format!("rannoch.declared.{name}")})
            .to_string()
        };
        for class in ["author", "book", "chapter"] {
            g.declare(
                &Declaration::Class(Class::new(class, ["title"])),
                &declared(class),
            )?;
        }
        for relation in [
            Relation::new("wrote", "author", "book", false),
            Relation::new("in", "chapter", "book", true),
        ] {
            let name = relation.name.clone();
            g.declare(&Declaration::Relation(relation), &declared(&name))?;
        }
        Ok(g)
    }

    fn id() -> String {
        format!("{}:0", EventId::now())
    }

    fn node_from(source: &str, class: &str, key: &str, title: &str) -> String {
        json!({"specversion": "1.0", "id": id(), "source": format!("rannoch:{source}"),
               "type": format!("rannoch.node.{class}"), "subject": key,
               "data": {"properties": {"title": title}}})
        .to_string()
    }

    fn book(key: &str, title: &str) -> String {
        node_from("shop", "book", key, title)
    }

    fn fact_from(
        source: &str,
        kind: &str,
        relation: &str,
        from: (&str, &str),
        to: (&str, &str),
    ) -> String {
        json!({"specversion": "1.0", "id": id(), "source": format!("rannoch:{source}"),
               "type": format!("rannoch.{kind}.{relation}"),
               "data": {"from": {"class": from.0, "key": from.1},
                        "to": {"class": to.0, "key": to.1}}})
        .to_string()
    }

    fn wrote(source: &str, from: &str, to: &str) -> String {
        fact_from(source, "link", "wrote", ("author", from), ("book", to))
    }

    fn amended(target: &str, patch: &serde_json::Value, source: &str) -> Result<String> {
        let target: serde_json::Value = serde_json::from_str(target)?;
        Ok(json!({"specversion": "1.0", "id": id(), "source": source,
                  "type": "rannoch.amended",
                  "data": {"target": {"source": target["source"], "id": target["id"]},
                           "patch": patch}})
        .to_string())
    }

    fn land(g: &mut Graph, bodies: &[&String]) -> Result {
        let bodies: Vec<&str> = bodies.iter().map(|body| body.as_str()).collect();
        for landed in g.commit(&bodies, &[])? {
            landed?;
        }
        g.fill()?;
        Ok(())
    }

    fn node(class: &str, key: &str) -> NodeRef {
        NodeRef::new(class, key)
    }

    fn title(g: &Graph, class: &str, key: &str) -> Result<Option<crate::Value>> {
        Ok(g.find(class, key)?
            .and_then(|found| found.properties.get("title").cloned()))
    }

    fn rows(g: &Graph, table: &str) -> Result<i64> {
        Ok(g.conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?)
    }

    fn logged(g: &Graph) -> Result<Vec<String>> {
        Ok(g.events()
            .since(Cursor::START)?
            .into_iter()
            .map(|event| event.body)
            .collect())
    }

    /// The store's file and its write-ahead log as bytes.
    fn file_bytes(dir: &tempfile::TempDir, name: &str) -> Result<Vec<u8>> {
        let mut bytes = std::fs::read(dir.path().join(name))?;
        if let Ok(wal) = std::fs::read(dir.path().join(format!("{name}-wal"))) {
            bytes.extend(wal);
        }
        Ok(bytes)
    }

    fn holds(bytes: &[u8], text: &[u8]) -> bool {
        bytes.windows(text.len()).any(|window| window == text)
    }

    #[test]
    fn a_purge_leaves_no_byte_of_what_it_matched() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let gone = book("secret-key", "a-secret-title");
        let again = amended(
            &gone,
            &json!({"data": {"properties": {"title": "patched-secret"}}}),
            "rannoch:shop",
        )?;
        land(&mut g, &[&gone, &book("kept", "a-kept-title"), &again])?;
        let identity = g.hearings(&node("book", "secret-key"))?[0].identity;
        let event = rannoch_cloudevents::attributes(&gone)?.id;
        assert!(holds(&file_bytes(&dir, "s")?, b"a-secret-title"), "written");
        g.purge(&Target::Node(node("book", "secret-key")), "erasure", "app")?;
        assert_eq!(
            g.conn
                .query_row("PRAGMA secure_delete", [], |row| row.get::<_, i64>(0))?,
            1
        );
        let bytes = file_bytes(&dir, "s")?;
        for text in [
            b"a-secret-title".as_slice(),
            b"patched-secret",
            b"secret-key",
            event.as_bytes(),
            identity.as_slice(),
        ] {
            assert!(!holds(&bytes, text), "{}", String::from_utf8_lossy(text));
        }
        assert!(holds(&bytes, b"a-kept-title"));
        assert_eq!(
            std::fs::metadata(dir.path().join("s-wal"))?.len(),
            0,
            "the log is truncated"
        );
        assert_eq!(title(&g, "book", "secret-key")?, None);
        assert!(g.hearings(&node("book", "secret-key"))?.is_empty());
        assert_eq!(title(&g, "book", "kept")?, Some("a-kept-title".into()));
        Ok(())
    }

    #[test]
    fn the_purge_event_holds_its_rule_and_kind_and_no_target() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let event = g.purge(
            &Target::NodeWord {
                node: node("book", "plain-key"),
                source: "plain-source".to_string(),
            },
            "erasure-7",
            "app",
        )?;
        let body: serde_json::Value = serde_json::from_str(&event.body)?;
        assert_eq!(body["type"], PURGED);
        assert_eq!(body["data"]["rule"], "erasure-7");
        assert_eq!(body["data"]["kind"], "node_word");
        assert!(!event.body.contains("plain-key"));
        assert!(!event.body.contains("plain-source"));
        assert_eq!(body["data"]["match"].as_str().map(str::len), Some(64));
        Ok(())
    }

    #[test]
    fn a_fact_by_its_identity_leaves_alone() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        // The same content under a second id is one hearing: both events go.
        let first = book("b1", "one");
        let mut second: serde_json::Value = serde_json::from_str(&first)?;
        second["id"] = json!(id());
        let second = second.to_string();
        land(&mut g, &[&first, &second, &book("b1", "two")])?;
        let hearings = g.hearings(&node("book", "b1"))?;
        assert_eq!(hearings.len(), 2);
        let one = hearings
            .iter()
            .find(|h| h.aliases.len() == 1)
            .ok_or("the hearing with its alias")?;
        g.purge(
            &Target::Fact {
                identity: one.identity,
            },
            "r",
            "app",
        )?;
        assert_eq!(g.hearings(&node("book", "b1"))?.len(), 1);
        assert_eq!(rows(&g, "_aliases")?, 0);
        assert_eq!(title(&g, "book", "b1")?, Some("two".into()));
        assert!(logged(&g)?.iter().all(|body| !body.contains("\"one\"")));
        Ok(())
    }

    #[test]
    fn a_stream_takes_every_event_of_its_source_and_a_source_only_its_word() -> Result {
        let app_event = |source: &str| {
            json!({"specversion": "1.0", "id": id(), "source": format!("rannoch:{source}"),
                   "type": "app.note", "data": {"note": format!("note-of-{source}")}})
            .to_string()
        };
        for (target, note_stays) in [
            (
                Target::Stream {
                    source: "feed".to_string(),
                },
                false,
            ),
            (
                Target::Source {
                    source: "feed".to_string(),
                },
                true,
            ),
        ] {
            let dir = tempfile::tempdir()?;
            let mut g = store(&dir, "s")?;
            let kept = book("b1", "kept");
            land(
                &mut g,
                &[
                    &node_from("feed", "book", "b2", "fed"),
                    &node_from("feed", "book", "b1", "fed-too"),
                    &kept,
                    &app_event("feed"),
                ],
            )?;
            g.purge(&target, "r", "app")?;
            assert_eq!(title(&g, "book", "b2")?, None);
            assert_eq!(title(&g, "book", "b1")?, Some("kept".into()));
            assert_eq!(
                logged(&g)?.iter().any(|body| body.contains("note-of-feed")),
                note_stays,
                "{target:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_node_leaves_with_its_parts_and_all_their_links() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let ended = fact_from("shop", "ended", "wrote", ("author", "a1"), ("book", "b1"));
        land(
            &mut g,
            &[
                &book("b1", "whole"),
                &book("b2", "other"),
                &node_from("shop", "author", "a1", "ann"),
                &node_from("shop", "chapter", "c1", "part"),
                &node_from("shop", "chapter", "c2", "not-part"),
                &wrote("shop", "a1", "b1"),
                &wrote("shop", "a1", "b2"),
                &fact_from("shop", "link", "in", ("chapter", "c1"), ("book", "b1")),
                &fact_from("shop", "link", "in", ("chapter", "c2"), ("book", "b2")),
                &ended,
            ],
        )?;
        g.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        assert_eq!(title(&g, "book", "b1")?, None);
        assert_eq!(title(&g, "chapter", "c1")?, None, "its part");
        assert_eq!(title(&g, "chapter", "c2")?, Some("not-part".into()));
        assert_eq!(title(&g, "author", "a1")?, Some("ann".into()));
        assert!(g.link_hearings("wrote", "a1", "b1")?.is_empty());
        assert!(g.link_end("wrote", "a1", "b1")?.is_none(), "its ending too");
        assert_eq!(g.link_hearings("wrote", "a1", "b2")?.len(), 1);
        assert_eq!(rows(&g, "_endings")?, 0);
        assert_eq!(rows(&g, "_ends")?, 0);
        // The part's rule stands too.
        let refused = g.commit(&[&node_from("shop", "chapter", "c1", "back")], &[])?;
        assert!(matches!(&refused[0], Err(Error::Purged { rule }) if rule == "r"));
        Ok(())
    }

    #[test]
    fn a_link_leaves_alone_and_its_closing_with_it() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        land(
            &mut g,
            &[
                &node_from("shop", "author", "a1", "ann"),
                &wrote("shop", "a1", "b1"),
                &book("b1", "one"),
            ],
        )?;
        assert!(
            logged(&g)?
                .iter()
                .any(|body| body.contains("rannoch.closed.wrote"))
        );
        g.purge(
            &Target::Link {
                relation: "wrote".to_string(),
                from: "a1".to_string(),
                to: "b1".to_string(),
            },
            "r",
            "app",
        )?;
        assert!(g.link_hearings("wrote", "a1", "b1")?.is_empty());
        assert!(
            logged(&g)?
                .iter()
                .all(|body| !body.contains("rannoch.closed"))
        );
        assert_eq!(title(&g, "book", "b1")?, Some("one".into()));
        assert_eq!(title(&g, "author", "a1")?, Some("ann".into()));
        Ok(())
    }

    #[test]
    fn one_sources_word_leaves_and_the_others_stays() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        land(
            &mut g,
            &[
                &node_from("a", "book", "b1", "older"),
                &node_from("b", "book", "b1", "newer"),
                &node_from("b", "author", "a1", "ann"),
                &node_from("b", "book", "b2", "only-b"),
                &wrote("a", "a1", "b2"),
            ],
        )?;
        g.purge(
            &Target::NodeWord {
                node: node("book", "b1"),
                source: "b".to_string(),
            },
            "r",
            "app",
        )?;
        assert_eq!(title(&g, "book", "b1")?, Some("older".into()), "falls back");
        // Its node has no word left, and the link's own word waits open for it.
        g.purge(
            &Target::NodeWord {
                node: node("book", "b2"),
                source: "b".to_string(),
            },
            "r",
            "app",
        )?;
        assert_eq!(title(&g, "book", "b2")?, None);
        assert_eq!(g.link_hearings("wrote", "a1", "b2")?.len(), 1);
        assert_eq!(rows(&g, "_open")?, 1);
        g.purge(
            &Target::LinkWord {
                relation: "wrote".to_string(),
                from: "a1".to_string(),
                to: "b2".to_string(),
                source: "a".to_string(),
            },
            "r",
            "app",
        )?;
        assert!(g.link_hearings("wrote", "a1", "b2")?.is_empty());
        assert_eq!(rows(&g, "_open")?, 0);
        Ok(())
    }

    #[test]
    fn the_rule_refuses_what_arrives_after_it() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let target = book("b1", "one");
        let author = node_from("shop", "author", "a1", "ann");
        land(&mut g, &[&target, &author, &book("b2", "two")])?;
        g.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        let ending = fact_from("shop", "ended", "wrote", ("author", "a1"), ("book", "b1"));
        let late_amendment = amended(
            &target,
            &json!({"data": {"properties": {"title": "x"}}}),
            "rannoch:shop",
        )?;
        let onto_purged = amended(
            &author,
            &json!({"type": "rannoch.node.book", "subject": "b1"}),
            "rannoch:shop",
        )?;
        let before = g.events().count(Cursor::START, Cursor(i64::MAX))?;
        for body in [
            book("b1", "again"),
            wrote("shop", "a1", "b1"),
            ending,
            late_amendment,
            onto_purged,
        ] {
            let answer = g.commit(&[&body], &[])?;
            assert!(matches!(&answer[0], Err(Error::Purged { .. })), "{body}");
        }
        assert_eq!(g.events().count(Cursor::START, Cursor(i64::MAX))?, before);
        g.fill()?;
        assert_eq!(title(&g, "book", "b1")?, None);
        assert!(g.stopped()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_producer_carries_on_past_a_body_a_rule_refuses() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        g.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        let refused = book("b1", "one");
        let kept = book("b2", "two");
        let answer = g.commit(
            &[&refused, &kept],
            &[(0, "p", "line 1"), (1, "p", "line 2")],
        )?;
        assert!(matches!(&answer[0], Err(Error::Purged { .. })));
        assert!(answer[1].is_ok(), "the later body lands");
        assert_eq!(g.position("p")?.as_deref(), Some("line 2"));
        let answer = g.commit(&[&refused], &[(0, "q", "line 9")])?;
        assert!(answer[0].is_err());
        assert_eq!(g.position("q")?.as_deref(), Some("line 9"), "moved past it");
        Ok(())
    }

    #[test]
    fn an_amendment_waiting_for_what_a_rule_refuses_goes_with_it() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        g.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        let refused = book("b1", "one");
        let patch = json!({"data": {"properties": {"title": "a-waiting-patch"}}});
        let waiting = amended(&refused, &patch, "rannoch:shop")?;
        land(&mut g, &[&waiting])?;
        assert_eq!(rows(&g, "_waiting")?, 1);
        assert!(g.commit(&[&refused], &[])?[0].is_err());
        assert_eq!(rows(&g, "_waiting")?, 0);
        assert!(
            logged(&g)?
                .iter()
                .all(|body| !body.contains("a-waiting-patch"))
        );
        let again = amended(&refused, &patch, "rannoch:shop")?;
        assert!(g.commit(&[&again], &[])?[0].is_err(), "and any after it");
        Ok(())
    }

    #[test]
    fn a_store_replaying_a_log_holds_its_rules_and_fill_never_stores_a_match() -> Result {
        let dir = tempfile::tempdir()?;
        let mut origin = store(&dir, "origin")?;
        let purge = origin.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        let mut replica = store(&dir, "replica")?;
        // In one batch: the rule stands from where it lands in the batch.
        let late = book("b1", "a-late-title");
        let kept = book("b2", "kept");
        replica.commit(&[&purge.body, &late, &kept], &[])?;
        assert!(
            !holds(&file_bytes(&dir, "replica")?, b"a-late-title"),
            "no byte of it reaches the file or its log"
        );
        replica.fill()?;
        assert_eq!(title(&replica, "book", "b1")?, None);
        assert_eq!(title(&replica, "book", "b2")?, Some("kept".into()));
        assert!(
            logged(&replica)?
                .iter()
                .all(|body| !body.contains("a-late-title"))
        );
        assert!(replica.commit(&[&book("b1", "later")], &[])?[0].is_err());
        // Ahead of the rule in a log not yet filled: fill never stores it, and takes it out.
        let mut unfilled = store(&dir, "unfilled")?;
        unfilled.commit(&[&book("b1", "ahead-of-rule"), &purge.body], &[])?;
        unfilled.fill()?;
        assert_eq!(title(&unfilled, "book", "b1")?, None);
        assert!(
            logged(&unfilled)?
                .iter()
                .all(|body| !body.contains("ahead-of-rule"))
        );
        // A rule event says what stands and removes nothing another store already holds.
        let mut holder = store(&dir, "holder")?;
        land(&mut holder, &[&book("b1", "held")])?;
        land(&mut holder, &[&purge.body.clone()])?;
        assert_eq!(title(&holder, "book", "b1")?, Some("held".into()));
        Ok(())
    }

    #[test]
    fn a_copy_holds_the_rules() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        g.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        let at = dir.path().join("copy");
        g.copy_to(&at)?;
        let mut copy = open(&at, &KEY)?;
        assert!(copy.commit(&[&book("b1", "back")], &[])?[0].is_err());
        Ok(())
    }

    #[test]
    fn purging_a_kept_events_amendment_reads_it_as_written() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let target = book("b1", "written");
        let patch = amended(
            &target,
            &json!({"data": {"properties": {"title": "patched"}}}),
            "rannoch:fixer",
        )?;
        land(&mut g, &[&target, &patch])?;
        assert_eq!(title(&g, "book", "b1")?, Some("patched".into()));
        g.purge(
            &Target::Stream {
                source: "fixer".to_string(),
            },
            "r",
            "app",
        )?;
        assert_eq!(title(&g, "book", "b1")?, Some("written".into()));
        assert_eq!(rows(&g, "_amendments")?, 0);
        assert!(logged(&g)?.iter().all(|body| !body.contains("patched")));
        Ok(())
    }

    #[test]
    fn a_purge_of_an_event_takes_every_amendment_naming_it_wherever_it_waits() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let target = book("b1", "written");
        let early = amended(
            &target,
            &json!({"data": {"properties": {"title": "early-patch"}}}),
            "rannoch:shop",
        )?;
        land(&mut g, &[&early])?;
        assert_eq!(rows(&g, "_waiting")?, 1);
        land(&mut g, &[&target])?;
        let later = amended(
            &target,
            &json!({"data": {"properties": {"title": "later-patch"}}}),
            "rannoch:shop",
        )?;
        land(&mut g, &[&later])?;
        g.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        let bytes = file_bytes(&dir, "s")?;
        assert!(!holds(&bytes, b"early-patch"));
        assert!(!holds(&bytes, b"later-patch"));
        assert_eq!(rows(&g, "_amendments")?, 0);
        assert_eq!(rows(&g, "_waiting")?, 0);
        Ok(())
    }

    #[test]
    fn a_node_is_purged_under_every_key_it_went_by() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let target = book("old-key", "one");
        let author = node_from("shop", "author", "a1", "ann");
        let link = wrote("shop", "a1", "old-key");
        let renamed = amended(&target, &json!({"subject": "new-key"}), "rannoch:shop")?;
        land(&mut g, &[&target, &author, &link, &renamed])?;
        assert_eq!(title(&g, "book", "new-key")?, Some("one".into()));
        // Named by its new key, its old key's events go, and either key is refused after.
        g.purge(&Target::Node(node("book", "new-key")), "r", "app")?;
        assert!(g.hearings(&node("book", "old-key"))?.is_empty());
        assert!(g.hearings(&node("book", "new-key"))?.is_empty());
        assert!(g.link_hearings("wrote", "a1", "new-key")?.is_empty());
        assert_eq!(rows(&g, "_renames")?, 0);
        let bytes = file_bytes(&dir, "s")?;
        assert!(!holds(&bytes, b"old-key") && !holds(&bytes, b"new-key"));
        assert!(g.commit(&[&book("old-key", "back")], &[])?[0].is_err());
        assert!(g.commit(&[&book("new-key", "back")], &[])?[0].is_err());
        Ok(())
    }

    #[test]
    fn a_rename_that_leaves_is_undone_for_what_stays() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let target = book("b1", "one");
        let author = node_from("shop", "author", "a1", "ann");
        let link = wrote("shop", "a1", "b1");
        let ended = fact_from("shop", "ended", "wrote", ("author", "a1"), ("book", "b1"));
        let renamed = amended(&target, &json!({"subject": "b9"}), "rannoch:fixer")?;
        land(&mut g, &[&target, &author, &link, &ended, &renamed])?;
        assert_eq!(title(&g, "book", "b9")?, Some("one".into()));
        assert!(g.link_end("wrote", "a1", "b9")?.is_some());
        g.purge(
            &Target::Stream {
                source: "fixer".to_string(),
            },
            "r",
            "app",
        )?;
        assert_eq!(title(&g, "book", "b1")?, Some("one".into()));
        assert_eq!(title(&g, "book", "b9")?, None);
        assert_eq!(rows(&g, "_renames")?, 0);
        assert!(
            g.link_end("wrote", "a1", "b1")?.is_some(),
            "the ending stays"
        );
        assert!(g.link_end("wrote", "a1", "b9")?.is_none(), "its copy goes");
        assert!(g.link_hearings("wrote", "a1", "b9")?.is_empty());
        assert_eq!(g.link_hearings("wrote", "a1", "b1")?.len(), 1);
        let view = View::default();
        assert!(g.state_at(&node("book", "b1"), &view)?.is_some());
        Ok(())
    }

    /// `body` with the uuid of its id in upper case: it parses as the same id, which a fact's rows
    /// are kept under in its canonical form.
    fn upper_id(body: &str) -> Result<String> {
        let mut event: serde_json::Value = serde_json::from_str(body)?;
        let id = event["id"].as_str().ok_or("an id")?.to_uppercase();
        event["id"] = json!(id);
        Ok(event.to_string())
    }

    #[test]
    fn a_conclusion_or_a_rename_under_an_upper_case_id_leaves_its_rows() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        let target = book("b1", "one");
        let renamed = upper_id(&amended(
            &target,
            &json!({"subject": "b9"}),
            "rannoch:feed",
        )?)?;
        let ended = upper_id(&fact_from(
            "feed",
            "ended",
            "wrote",
            ("author", "a1"),
            ("book", "b1"),
        ))?;
        let corrected = upper_id(&fact_from(
            "feed",
            "corrected",
            "wrote",
            ("author", "a1"),
            ("book", "b1"),
        ))?;
        land(
            &mut g,
            &[
                &target,
                &node_from("shop", "author", "a1", "ann"),
                &wrote("shop", "a1", "b1"),
                &ended,
                &corrected,
                &renamed,
            ],
        )?;
        assert!(rows(&g, "_renames")? > 0 && rows(&g, "_endings")? > 0);
        g.purge(
            &Target::Stream {
                source: "feed".to_string(),
            },
            "r",
            "app",
        )?;
        for table in ["_renames", "_ends", "_endings", "_corrections"] {
            assert_eq!(rows(&g, table)?, 0, "{table}");
        }
        assert_eq!(title(&g, "book", "b1")?, Some("one".into()));
        Ok(())
    }

    #[test]
    fn a_conclusion_that_leaves_brings_back_the_one_before_it() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        land(
            &mut g,
            &[
                &book("b1", "one"),
                &node_from("shop", "author", "a1", "ann"),
                &wrote("shop", "a1", "b1"),
                &fact_from(
                    "shop",
                    "corrected",
                    "wrote",
                    ("author", "a1"),
                    ("book", "b1"),
                ),
                &fact_from("other", "ended", "wrote", ("author", "a1"), ("book", "b1")),
            ],
        )?;
        assert!(matches!(
            g.link_end("wrote", "a1", "b1")?,
            Some(crate::End::Ended { .. })
        ));
        g.purge(
            &Target::Source {
                source: "other".to_string(),
            },
            "r",
            "app",
        )?;
        assert!(matches!(
            g.link_end("wrote", "a1", "b1")?,
            Some(crate::End::Corrected { .. })
        ));
        assert_eq!(rows(&g, "_ends")?, 1);
        Ok(())
    }

    #[test]
    fn bodies_not_yet_filled_or_that_failed_fill_are_purged_too() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        // Of a class not declared, so it fails fill and has no hearing to find it by.
        let failed = node_from("feed", "ghost", "g1", "failed-body");
        let unfilled = node_from("feed", "book", "b1", "unfilled-body");
        g.commit(&[&failed], &[])?;
        g.fill()?;
        assert_eq!(rows(&g, "_work")?, 1);
        g.commit(&[&unfilled], &[])?;
        g.purge(
            &Target::Stream {
                source: "feed".to_string(),
            },
            "r",
            "app",
        )?;
        let bytes = file_bytes(&dir, "s")?;
        assert!(!holds(&bytes, b"failed-body") && !holds(&bytes, b"unfilled-body"));
        assert_eq!(rows(&g, "_work")?, 0, "nor its failure");
        Ok(())
    }

    #[test]
    fn a_worker_sees_only_the_rule_event() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        land(
            &mut g,
            &[
                &book("b1", "first"),
                &book("b2", "second"),
                &book("b3", "third"),
            ],
        )?;
        let until = g.events().committed()?;
        let mut handed = Vec::new();
        // In progress on the book that leaves, as a worker that stopped leaves it.
        while let Some(event) = g.next("w", until)? {
            let at_book = event.body.contains("rannoch.node.book");
            handed.push(event);
            if at_book {
                break;
            }
        }
        let open = handed.last().ok_or("handed")?.cursor;
        for event in &handed[..handed.len() - 1] {
            g.done("w", event.cursor)?;
        }
        g.purge(&Target::Node(node("book", "b1")), "r", "app")?;
        g.purge(&Target::Node(node("book", "b2")), "r", "app")?;
        assert!(
            g.processed()? >= open,
            "the mark is not held behind what left"
        );
        g.handed.clear();
        let until = g.events().committed()?;
        let mut seen = Vec::new();
        while let Some(event) = g.next("w", until)? {
            g.done("w", event.cursor)?;
            seen.push(event.body);
        }
        assert_eq!(seen.iter().filter(|body| body.contains(PURGED)).count(), 2);
        assert!(seen.iter().any(|body| body.contains("third")));
        assert!(
            seen.iter()
                .all(|body| !body.contains("first") && !body.contains("second"))
        );
        Ok(())
    }

    #[test]
    fn a_purge_that_matches_nothing_still_stands_and_an_undeclared_target_is_refused() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        g.purge(
            &Target::Source {
                source: "nobody".to_string(),
            },
            "r",
            "app",
        )?;
        let body = node_from("nobody", "book", "b1", "x");
        assert!(g.commit(&[&body], &[])?[0].is_err());
        let refused = g.purge(&Target::Node(node("ghost", "g1")), "r", "app");
        assert!(matches!(refused, Err(Error::Refused(_))));
        Ok(())
    }

    #[test]
    fn a_reader_holding_the_log_open_is_said_after_the_purge_commits() -> Result {
        let dir = tempfile::tempdir()?;
        let mut g = store(&dir, "s")?;
        land(&mut g, &[&book("b1", "one")])?;
        let reader = rusqlite::Connection::open(dir.path().join("s"))?;
        reader.execute_batch("BEGIN; SELECT count(*) FROM _events;")?;
        let answer = g.purge(&Target::Node(node("book", "b1")), "r", "app");
        assert!(matches!(answer, Err(Error::Refused(why)) if why.contains("write-ahead log")));
        reader.execute_batch("COMMIT;")?;
        assert_eq!(title(&g, "book", "b1")?, None, "the purge committed");
        Ok(())
    }
}
