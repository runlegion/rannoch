//! Snapshots: a graph's state at a cursor in a file of its own, and rebuilding a graph from one
//! plus the events after its cursor (oracle `v0.4.0.a` `src/events.rs`, FR-RANNOCH-062).
//!
//! A snapshot holds every event up to its cursor under the event's own id and place, so the
//! state filled from them holds every version, hearing and alias, and a stored cursor names the
//! same event in it as in the source. Declarations carry no declaration in their body, so the
//! ontology is copied from the source's, as the source holds it when the snapshot is taken.

use std::path::{Path, PathBuf};

use rannoch_ontology::{Declaration, Ontology};
use rusqlite::{Connection, params};

use crate::log::{Cursor, Event, Events, store};
use crate::ontology::build;
use crate::queries::{
    ALL_PLACES, ALL_POSITIONS, ALL_WORK, COPY_INTO, LOWER_PLACES, PUT_WORK, RAISE_HIGH_WATER,
    RECORD_SNAPSHOT, SET_HIGH_WATER, SET_PLACE, SET_POSITION,
};
use crate::{Error, Graph, open, open_with};

/// A graph's state at a cursor, in a file of its own. A new reader or a rebuild starts from it
/// and takes only the events after its cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    cursor: Cursor,
    path: PathBuf,
}

/// The inbox's rows, as one graph holds them: worker places, worker events open or failed, and
/// producer positions.
type Inbox = (
    Vec<(String, i64)>,
    Vec<(String, i64, Option<String>)>,
    Vec<(String, String)>,
);

impl Snapshot {
    /// The place in the source graph's log the snapshot was taken at.
    #[must_use]
    pub fn cursor(&self) -> Cursor {
        self.cursor
    }

    /// Rebuilds a graph at `empty_path` from the snapshot and `later`, the source's events after
    /// the snapshot's cursor, each kept under its own id and place, under the content-identity
    /// `key`. A worker's place past what the rebuilt graph holds is lowered to its watermark.
    /// The snapshot file is left as it was.
    ///
    /// # Errors
    /// [`Error::KeyDiffers`] when the snapshot was taken under another key; [`Error::Refused`]
    /// when `empty_path` is not UTF-8; the snapshot cannot be copied, or an event cannot be kept
    /// or filled.
    pub fn rebuild(
        &self,
        later: &[Event],
        empty_path: impl AsRef<Path>,
        key: &[u8; 32],
    ) -> Result<Graph, Error> {
        Connection::open(&self.path)?.execute(COPY_INTO, params![text(empty_path.as_ref())?])?;
        let mut graph = open(empty_path, key)?;
        let tx = graph.conn.transaction()?;
        for event in later {
            store(&tx, event.id, Some(event.cursor), &event.body)?;
        }
        tx.commit()?;
        graph.fill()?;
        let committed = graph.events().committed()?;
        graph
            .conn
            .prepare_cached(LOWER_PLACES)?
            .execute(params![committed.0])?;
        Ok(graph)
    }
}

impl Graph {
    /// Takes a snapshot of the graph's state at `cursor` into a new file at `empty_path`: the
    /// ontology, every event up to the cursor under its own id and place, the state filled from
    /// them, the workers' places and events, and the producers' positions. Its watermark is at
    /// least `cursor`. It reads this graph in one read transaction and deletes nothing; the one
    /// write it makes here, once the new file is whole, records the snapshot so a purge reaches
    /// it.
    ///
    /// # Errors
    /// [`Error::Refused`] when `empty_path` is not UTF-8; the graph cannot be read, or the new
    /// file cannot be opened, written or filled.
    pub fn snapshot(
        &self,
        cursor: Cursor,
        empty_path: impl AsRef<Path>,
    ) -> Result<Snapshot, Error> {
        let read = self.conn.unchecked_transaction()?;
        let events = Events { conn: &read }.since_until(Cursor::START, cursor)?;
        let ontology = crate::ontology::load(&read)?;
        let inbox = inbox(&read)?;
        drop(read);
        let mut fresh = open_with(&empty_path, self.key.clone())?;
        // Nothing reads the new file until this returns, so it is filled under a rollback
        // journal, which writes each page once, and goes back to WAL at the end.
        fresh
            .conn
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                row.get::<_, String>(0)
            })?;
        let tx = fresh.conn.transaction()?;
        declare_all(&tx, &ontology)?;
        for event in &events {
            store(&tx, event.id, Some(event.cursor), &event.body)?;
        }
        put_inbox(&tx, &inbox)?;
        // The watermark is at least the cursor, even when no event sits at that place.
        let raised = tx.execute(RAISE_HIGH_WATER, params![cursor.0])?;
        if raised == 0 && (Events { conn: &tx }).committed()? < cursor {
            tx.execute(SET_HIGH_WATER, params![cursor.0])?;
        }
        tx.commit()?;
        fresh.ontology = crate::ontology::load(&fresh.conn)?;
        fresh.fill()?;
        fresh
            .conn
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))?;
        drop(fresh);
        let recorded = std::fs::canonicalize(&empty_path)
            .unwrap_or_else(|_| empty_path.as_ref().to_path_buf());
        self.conn
            .prepare_cached(RECORD_SNAPSHOT)?
            .execute(params![text(&recorded)?, cursor.0])?;
        Ok(Snapshot {
            cursor,
            path: empty_path.as_ref().to_path_buf(),
        })
    }
}

/// Declares every class, then every relation, of `ontology` in `tx`.
fn declare_all(tx: &Connection, ontology: &Ontology) -> Result<(), Error> {
    let mut so_far = Ontology::default();
    for class in ontology.classes.values() {
        build(tx, &so_far, &Declaration::Class(class.clone()))?;
        so_far.classes.insert(class.name.clone(), class.clone());
    }
    for relation in ontology.relations.values() {
        build(tx, &so_far, &Declaration::Relation(relation.clone()))?;
        so_far
            .relations
            .insert(relation.name.clone(), relation.clone());
    }
    Ok(())
}

fn inbox(conn: &Connection) -> Result<Inbox, Error> {
    let places = conn
        .prepare_cached(ALL_PLACES)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    let work = conn
        .prepare_cached(ALL_WORK)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    let positions = conn
        .prepare_cached(ALL_POSITIONS)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    Ok((places, work, positions))
}

fn put_inbox(tx: &Connection, (places, work, positions): &Inbox) -> Result<(), Error> {
    for (worker, cursor) in places {
        tx.prepare_cached(SET_PLACE)?
            .execute(params![worker, cursor])?;
    }
    for (worker, seq, reason) in work {
        tx.prepare_cached(PUT_WORK)?
            .execute(params![worker, seq, reason])?;
    }
    for (producer, position) in positions {
        tx.prepare_cached(SET_POSITION)?
            .execute(params![producer, position])?;
    }
    Ok(())
}

/// `path` as text, refused when it is not UTF-8.
fn text(path: &Path) -> Result<&str, Error> {
    path.to_str()
        .ok_or_else(|| Error::Refused(format!("the path {} is not UTF-8", path.display())))
}
