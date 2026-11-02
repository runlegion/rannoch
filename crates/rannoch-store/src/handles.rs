//! Statement handles. A read looks each statement up in the connection's statement cache by its
//! prebuilt text; a fill transaction takes each handle from the cache once and keeps it by slot
//! for every event in the batch, so no event formats or hashes a statement's text.

use std::cell::RefCell;

use rusqlite::{CachedStatement, Connection, Statement};

use crate::queries::{Catalogue, Stmt};

/// Where statements run: a connection, which looks each one up by its text, or a fill
/// transaction's [`Handles`].
pub(crate) trait Run {
    /// The connection itself, for what runs once per transaction or batch.
    fn conn(&self) -> &Connection;

    /// Runs `f` on the statement `stmt`.
    ///
    /// # Errors
    /// The statement cannot be prepared, or `f` fails.
    fn with<'s, T>(
        &self,
        stmt: impl Into<Stmt<'s>>,
        f: impl FnOnce(&mut Statement<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T>;
}

impl Run for Connection {
    fn conn(&self) -> &Connection {
        self
    }

    fn with<'s, T>(
        &self,
        stmt: impl Into<Stmt<'s>>,
        f: impl FnOnce(&mut Statement<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut statement = self.prepare_cached(stmt.into().text)?;
        f(&mut statement)
    }
}

/// A fill transaction's statements: the ontology's [`Catalogue`], and each statement's handle,
/// taken from the connection's cache on first use and kept by its slot until the transaction
/// commits, when the handles go back to the cache.
pub(crate) struct Handles<'t> {
    tx: &'t Connection,
    pub(crate) sql: &'t Catalogue,
    held: RefCell<Vec<Option<CachedStatement<'t>>>>,
}

impl<'t> Handles<'t> {
    pub(crate) fn new(tx: &'t Connection, sql: &'t Catalogue) -> Handles<'t> {
        Handles {
            tx,
            sql,
            held: RefCell::new(std::iter::repeat_with(|| None).take(sql.slots).collect()),
        }
    }
}

impl Run for Handles<'_> {
    fn conn(&self) -> &Connection {
        self.tx
    }

    fn with<'s, T>(
        &self,
        stmt: impl Into<Stmt<'s>>,
        f: impl FnOnce(&mut Statement<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let stmt = stmt.into();
        // A statement run while another's handle is in use takes its own from the cache.
        let Ok(mut held) = self.held.try_borrow_mut() else {
            return self.tx.with(stmt, f);
        };
        let handle = match &mut held[stmt.slot] {
            Some(handle) => handle,
            empty => empty.insert(self.tx.prepare_cached(stmt.text)?),
        };
        f(handle)
    }
}
