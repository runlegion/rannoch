//! Opening a graph file, and the ontology its own stream declared.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::events::{Events, OWN_TABLES};
use crate::write::Error;

/// A class: a kind of node, with its natural key and a set of properties.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub name: String,
    pub properties: BTreeSet<String>,
}

impl Class {
    pub fn new<P: Into<String>>(
        name: impl Into<String>,
        properties: impl IntoIterator<Item = P>,
    ) -> Self {
        Class {
            name: name.into(),
            properties: properties.into_iter().map(Into::into).collect(),
        }
    }
}

/// A relation: links from a node of one class to a node of another. A containment relation
/// says its source is part of its target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub from: String,
    pub to: String,
    pub containment: bool,
}

impl Relation {
    pub fn new(
        name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        containment: bool,
    ) -> Self {
        Relation {
            name: name.into(),
            from: from.into(),
            to: to.into(),
            containment,
        }
    }
}

/// A graph's classes and relations, as its stream declared them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ontology {
    pub classes: BTreeMap<String, Class>,
    pub relations: BTreeMap<String, Relation>,
}

#[cfg(test)]
thread_local! {
    /// How many times `Ontology::load` ran on this thread, so a test can see a reload.
    pub(crate) static LOADS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Ontology {
    pub(crate) fn load(conn: &Connection) -> Result<Ontology, Error> {
        #[cfg(test)]
        LOADS.with(|loads| loads.set(loads.get() + 1));
        let mut ontology = Ontology::default();
        let mut statement = conn.prepare_cached("SELECT kind, body FROM _ontology")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let body: String = row.get(1)?;
            if row.get::<_, String>(0)? == CLASS {
                let class: Class = serde_json::from_str(&body)?;
                ontology.classes.insert(class.name.clone(), class);
            } else {
                let relation: Relation = serde_json::from_str(&body)?;
                ontology.relations.insert(relation.name.clone(), relation);
            }
        }
        Ok(ontology)
    }

    pub(crate) fn class(&self, name: &str) -> Result<&Class, Error> {
        self.classes
            .get(name)
            .ok_or_else(|| Error::Refused(format!("class {name} is not declared")))
    }

    pub(crate) fn relation(&self, name: &str) -> Result<&Relation, Error> {
        self.relations
            .get(name)
            .ok_or_else(|| Error::Refused(format!("relation {name} is not declared")))
    }
}

pub(crate) const CLASS: &str = "class";
pub(crate) const RELATION: &str = "relation";

/// Each item formatted by `each` and joined into one string.
pub(crate) fn concat<T>(items: impl IntoIterator<Item = T>, each: impl Fn(T) -> String) -> String {
    items.into_iter().map(each).collect::<Vec<_>>().concat()
}

/// A quoted SQL identifier.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The table that holds a class's nodes. Its name differs from the class's own name, so no
/// answer in ontology terms ever names it.
pub(crate) fn class_table(class: &str) -> String {
    ident(&format!("{CLASS}:{class}"))
}

/// The table that holds a relation's links.
pub(crate) fn relation_table(relation: &str) -> String {
    ident(&format!("{RELATION}:{relation}"))
}

/// The index over a relation's links by their target.
pub(crate) fn relation_index(relation: &str) -> String {
    ident(&format!("{RELATION}:{relation}:to"))
}

/// The column that holds a property. Prefixed, so a property never meets a column of rannoch's.
pub(crate) fn property_column(property: &str) -> String {
    ident(&format!("p:{property}"))
}

/// While another connection holds the file's write lock, a connection retries every millisecond,
/// for the same five seconds the default handler waits, so it takes the lock in the gap a write
/// leaves between pieces. The default handler backs off to 100 ms and misses that gap.
fn retry_lock(attempts: i32) -> bool {
    std::thread::sleep(Duration::from_millis(1));
    attempts < 5_000
}

/// The file's `PRAGMA schema_version`. SQLite raises it only when the schema changes, and every
/// declaration that is not identical creates a table or adds a column, so an unchanged version
/// means an unchanged ontology. Read it before loading the ontology it stands for, or inside the
/// same transaction, so a declaration landing in between costs a reload, never a stale ontology.
pub(crate) fn schema_version(conn: &Connection) -> Result<i64, Error> {
    Ok(conn
        .prepare_cached("PRAGMA schema_version")?
        .query_row([], |row| row.get(0))?)
}

/// One graph file.
#[derive(Debug)]
pub struct Graph {
    pub(crate) conn: Connection,
    /// As of the last load by `open` or a `&mut self` method; what `ontology()` returns.
    pub(crate) ontology: Ontology,
    /// What reads use: the ontology and the `PRAGMA schema_version` it was loaded at.
    /// `Graph` is already `!Sync` through `Connection`, so a `RefCell` costs nothing; `Arc`
    /// (not `Rc`) keeps `Graph` `Send`, as it is today.
    pub(crate) fresh: RefCell<Fresh>,
    /// Other graph files this graph's references can name, by the name each was mounted under.
    pub(crate) mounts: BTreeMap<String, Graph>,
}

/// An ontology and the schema version of the file it was loaded at.
#[derive(Debug)]
pub(crate) struct Fresh {
    pub(crate) schema_version: i64,
    pub(crate) ontology: Arc<Ontology>,
}

/// Opens the graph the file at `path` holds, with the ontology its own stream declared. A path
/// that holds no graph yet starts an empty graph.
///
/// # Errors
/// The file cannot be opened as a graph file.
pub fn open(path: impl AsRef<Path>) -> Result<Graph, Error> {
    let conn = Connection::open(path)?;
    conn.busy_handler(Some(retry_lock))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;",
    )?;
    conn.set_prepared_statement_cache_capacity(256);
    conn.execute_batch(OWN_TABLES)?;
    let schema_version = schema_version(&conn)?;
    let ontology = Ontology::load(&conn)?;
    let fresh = RefCell::new(Fresh {
        schema_version,
        ontology: Arc::new(ontology.clone()),
    });
    Ok(Graph {
        conn,
        ontology,
        fresh,
        mounts: BTreeMap::new(),
    })
}

impl Graph {
    /// The graph's ontology as of this handle's last load, by `open` or by one of its own
    /// declarations or writes: every class and relation declared on its stream by then. A
    /// declaration another handle made since is not in it; reads see it all the same.
    #[must_use]
    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    /// The ontology the file holds now. Another handle may have declared since this one last
    /// loaded, and a read must answer from the file's own stream; checking the schema version
    /// first keeps a plain data write elsewhere from costing a reload.
    pub(crate) fn current_ontology(&self) -> Result<Arc<Ontology>, Error> {
        let version = schema_version(&self.conn)?;
        {
            let fresh = self.fresh.borrow();
            if fresh.schema_version == version {
                return Ok(Arc::clone(&fresh.ontology));
            }
        }
        let ontology = Arc::new(Ontology::load(&self.conn)?);
        *self.fresh.borrow_mut() = Fresh {
            schema_version: version,
            ontology: Arc::clone(&ontology),
        };
        Ok(ontology)
    }

    /// The graph's event log.
    #[must_use]
    pub fn events(&self) -> Events<'_> {
        Events { conn: &self.conn }
    }
}

#[cfg(test)]
mod tests {
    use super::LOADS;
    use crate::{Class, Date, Declaration, Unit, ValidTime, Write, open};

    fn loads() -> usize {
        LOADS.with(std::cell::Cell::get)
    }

    fn temp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rannoch-{name}-{}.db", uuid::Uuid::now_v7()))
    }

    fn unit(class: &str, key: &str) -> Unit {
        Unit::new(
            class,
            key,
            [] as [(&str, &str); 0],
            ValidTime::On(Date::day(2026, 1, 1)),
        )
    }

    #[test]
    fn a_graph_is_still_send() {
        fn send<T: Send>() {}
        send::<crate::Graph>();
    }

    #[test]
    fn a_data_write_elsewhere_costs_a_read_no_reload() -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("no-reload");
        let mut a = open(&path)?;
        a.declare(&Declaration::Class(Class::new("crate", [] as [&str; 0])))?;
        let b = open(&path)?;
        a.write(&Write::new("a").unit(unit("crate", "k")))?;

        let before = loads();
        let found = b.find("crate", "k")?;
        assert_eq!(
            loads(),
            before,
            "a plain data write must not reload the ontology"
        );
        assert!(found.is_some());
        Ok(())
    }

    #[test]
    fn a_declaration_elsewhere_reloads_once_on_the_first_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temp("reload-once");
        let mut a = open(&path)?;
        let b = open(&path)?;
        a.declare(&Declaration::Class(Class::new("late", [] as [&str; 0])))?;
        a.write(&Write::new("a").unit(unit("late", "k")))?;

        let before = loads();
        assert!(b.find("late", "k")?.is_some());
        assert_eq!(loads(), before + 1, "the first read reloads once");
        assert!(b.find("late", "k")?.is_some());
        assert_eq!(loads(), before + 1, "the second read does not reload");
        Ok(())
    }
}
