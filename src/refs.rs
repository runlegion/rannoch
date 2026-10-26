//! References into other graph files: an app mounts another graph file by name and resolves a
//! node in it by class and key, from that file's own facts.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

use crate::events::{Change, naming_key};
use crate::open::{Graph, open_file};
use crate::read::NodeRef;
use crate::write::Error;

/// What a reference's target is, as the target file's own facts say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fate {
    /// The target is in its file, and nothing replaced it.
    Live,
    /// The target was forgotten under the app's `rule`.
    Forgotten { rule: String },
    /// The target was replaced by the node `by`.
    Replaced { by: NodeRef },
}

impl Graph {
    /// Mounts the graph file at `path` under `name`, so this graph's references can name it.
    ///
    /// # Errors
    /// The file cannot be opened as a graph file.
    pub fn mount(&mut self, name: &str, path: impl AsRef<Path>) -> Result<(), Error> {
        let mounted = open_file(path)?;
        self.mounts.insert(name.to_string(), mounted);
        Ok(())
    }

    /// Resolves a reference to `target` in the graph file mounted as `mount`, from that file's
    /// own facts: replaced (and by what), forgotten (and by which rule), or live. Resolving
    /// changes neither file.
    ///
    /// # Errors
    /// [`Error::Refused`] when no graph file is mounted as `mount`; [`Error::NotFound`] when the
    /// mounted file holds no node, live or forgotten, of the class under the key; the target's
    /// class is not declared in the mounted file; the mounted file cannot be read.
    pub fn resolve(&self, mount: &str, target: &NodeRef) -> Result<Fate, Error> {
        let mounted = self
            .mounts
            .get(mount)
            .ok_or_else(|| Error::Refused(format!("no graph file is mounted as {mount}")))?;
        let replaced_by = mounted
            .conn
            .prepare_cached("SELECT replaced_by FROM _fates WHERE class = ?1 AND key = ?2")?
            .query_row(params![target.class, target.key], |row| {
                row.get::<_, String>(0)
            })
            .optional()?;
        if let Some(key) = replaced_by {
            return Ok(Fate::Replaced {
                by: NodeRef::new(&target.class, key),
            });
        }
        if mounted.find(&target.class, &target.key)?.is_some() {
            return Ok(Fate::Live);
        }
        match forgotten_by(&mounted.conn, target)? {
            Some(rule) => Ok(Fate::Forgotten { rule }),
            None => Err(Error::NotFound {
                class: target.class.clone(),
                key: target.key.clone(),
            }),
        }
    }
}

/// The rule of the latest forget event that named `target`, if one did.
fn forgotten_by(conn: &Connection, target: &NodeRef) -> Result<Option<String>, Error> {
    for (_, stored) in naming_key(conn, &target.key)?.into_iter().rev() {
        if let Change::Forgot { rule, nodes, .. } = stored.change
            && nodes.contains(target)
        {
            return Ok(Some(rule));
        }
    }
    Ok(None)
}
