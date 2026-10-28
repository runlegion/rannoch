//! Every SQL statement and fragment the store runs. No other module holds a string that begins a
//! statement; [`ident`] is the only function that quotes an identifier, and every value reaches
//! SQL as a bound parameter.

use crate::ontology::{CLASS, RELATION, concat};

/// How a connection is opened. Freed space is zeroed on every deletion and page split, so no copy
/// of a removed body is left in free space by a split made before the removal. A commit is
/// durable: on macOS bundled SQLite issues `F_FULLFSYNC` only with `fullfsync` on.
pub(crate) const PRAGMAS: &str = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
     PRAGMA fullfsync=ON; PRAGMA foreign_keys=ON; PRAGMA secure_delete=ON;";

/// rannoch's own tables: the event log and the ontology. An event's place is never reused, so a
/// cursor stays a place in the log.
pub(crate) const OWN_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _events(
        seq INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL, id TEXT);
    CREATE UNIQUE INDEX IF NOT EXISTS _events_id ON _events(id);
    CREATE TABLE IF NOT EXISTS _ontology(
        kind TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL,
        PRIMARY KEY(kind, name)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _open(
        rel TEXT NOT NULL, from_key TEXT NOT NULL, to_key TEXT NOT NULL,
        source TEXT NOT NULL, valid TEXT,
        PRIMARY KEY(rel, from_key, to_key)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _open_to ON _open(rel, to_key);
    CREATE TABLE IF NOT EXISTS _filled(one INTEGER PRIMARY KEY CHECK (one = 1),
        cursor INTEGER NOT NULL);
    INSERT OR IGNORE INTO _filled(one, cursor) VALUES (1, 0);";

/// rannoch's history tables (oracle history.rs:21-54). The hearings, one version row each, with
/// no valid-to: `arrived` counts up as this store records them, `integer` is set only in an
/// integer-keyed class, and `properties` holds a node's. Then the other ids each arrived under,
/// each an occurrence of its hearing at its own point; the latest conclusion to arrive for each
/// link that ended or was found wrong (`point` null for a correction, which masks its endings);
/// every ending of a link, at its point (`valid` null when unknown); what is kept of when each
/// node and link was seen; and the check value of the graph's key.
///
/// Every history table carries the line it belongs to in its key. Every history read and write
/// is on the line `'main'` until a second line exists.
pub(crate) const HISTORY_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _hearings(
        arrived INTEGER PRIMARY KEY, line TEXT NOT NULL DEFAULT 'main', identity BLOB NOT NULL,
        entity TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT, event TEXT NOT NULL,
        seq INTEGER NOT NULL, source TEXT NOT NULL, valid TEXT, point INTEGER NOT NULL,
        integer INTEGER, recorded INTEGER NOT NULL, properties TEXT, UNIQUE(line, identity));
    CREATE INDEX IF NOT EXISTS _hearings_at
        ON _hearings(entity, line, key, to_key, integer, point, event);
    CREATE TABLE IF NOT EXISTS _aliases(
        line TEXT NOT NULL DEFAULT 'main', identity BLOB NOT NULL, alias TEXT NOT NULL,
        point INTEGER, PRIMARY KEY(line, identity, alias)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _aliases_at ON _aliases(line, identity, point);
    CREATE INDEX IF NOT EXISTS _aliases_alias ON _aliases(alias);
    CREATE TABLE IF NOT EXISTS _ends(
        entity TEXT NOT NULL, line TEXT NOT NULL DEFAULT 'main', key TEXT NOT NULL,
        to_key TEXT NOT NULL, event TEXT NOT NULL, source TEXT NOT NULL, point INTEGER,
        valid TEXT, PRIMARY KEY(entity, line, key, to_key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _endings(
        id INTEGER PRIMARY KEY, entity TEXT NOT NULL, line TEXT NOT NULL DEFAULT 'main',
        key TEXT NOT NULL, to_key TEXT NOT NULL, point INTEGER NOT NULL, event TEXT NOT NULL,
        source TEXT NOT NULL, valid TEXT, UNIQUE(entity, line, key, to_key, point, event));
    CREATE TABLE IF NOT EXISTS _seen(
        entity TEXT NOT NULL, line TEXT NOT NULL DEFAULT 'main', key TEXT NOT NULL,
        to_key TEXT NOT NULL, first_hearing INTEGER, first_ending INTEGER,
        last_hearing INTEGER, last_ending INTEGER, recorded_hearing INTEGER,
        recorded_ending INTEGER, first_true TEXT, count INTEGER NOT NULL,
        PRIMARY KEY(entity, line, key, to_key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _check(value TEXT NOT NULL);";

pub(crate) const CHECK: &str = "SELECT value FROM _check";

pub(crate) const SET_CHECK: &str = "INSERT INTO _check(value) VALUES (?1)";

/// The hearing whose content identity is `?1`: the event it is named by and that event's place.
pub(crate) const HELD: &str =
    "SELECT event, seq FROM _hearings WHERE line = 'main' AND identity = ?1";

pub(crate) const HEAR: &str = "INSERT INTO _hearings(identity, entity, key, to_key, event, seq, \
     source, valid, point, integer, recorded, properties) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";

/// The order versions are chosen by, `DESC` putting the newest first: by integer key (only an
/// integer-keyed class's rows hold one), then point, then event id.
pub(crate) fn order(direction: &str) -> String {
    format!("integer {direction}, point {direction}, event {direction}")
}

/// Every occurrence of the hearings `filter` picks, over their `_hearings` row `h`: its own, at
/// its row's point under its event, and each alias's, at the alias's point under the alias. A
/// row carries its hearing's columns, `name` the event the hearing is named by, and the
/// occurrence's `point` and `event`, which [`order`] picks by.
pub(crate) fn occurrences(filter: &str) -> String {
    let hearing = "h.arrived, h.identity, h.event AS name, h.source, h.valid, h.integer, \
                   h.recorded, h.properties";
    format!(
        "(SELECT {hearing}, h.point AS point, h.event AS event FROM _hearings h WHERE {filter} \
         UNION ALL SELECT {hearing}, a.point, a.alias FROM _hearings h \
         JOIN _aliases a ON a.line = h.line AND a.identity = h.identity WHERE {filter})"
    )
}

/// The occurrences of the node or link `?1`, `?2`, `?3` (entity, key, target key).
pub(crate) fn occurrences_of() -> String {
    occurrences("h.entity = ?1 AND h.line = 'main' AND h.key = ?2 AND h.to_key IS ?3")
}

/// One version row of the node or link `?1`, `?2`, `?3` among its hearings' occurrences, as
/// `tail` picks it: event, source, valid, point, integer key, properties, recorded, identity.
pub(crate) fn version_row(tail: &str) -> String {
    format!(
        "SELECT name, source, valid, point, integer, properties, recorded, identity \
         FROM {} WHERE true {tail}",
        occurrences_of()
    )
}

/// The `column` of the newest occurrence of the hearing whose `arrived` is `hearing` (SQL).
pub(crate) fn newest_occurrence(hearing: &str, column: &str) -> String {
    format!(
        "(SELECT {column} FROM {} ORDER BY {} LIMIT 1)",
        occurrences(&format!("h.arrived = {hearing}")),
        order("DESC")
    )
}

/// A valid time in `column` as `YYYY-MM-DD`: a month (7 characters) stands for its first day,
/// and an unknown valid time is NULL, which no comparison holds for.
pub(crate) fn valid_day(column: &str) -> String {
    format!("(CASE WHEN length({column}) = 7 THEN {column} || '-01' ELSE {column} END)")
}

/// The kept seen row of `?1`, `?2`, `?3` (its target key `''` for a node), with the facts it
/// names as first and last: hearing, ending, integer key, point and event of each.
pub(crate) fn seen_kept() -> String {
    format!(
        "SELECT s.first_hearing, s.first_ending, fh.integer, coalesce(fh.point, fe.point), \
         coalesce(fh.event, fe.event), s.last_hearing, s.last_ending, lh.integer, \
         coalesce({}, le.point), coalesce({}, le.event) FROM _seen s \
         LEFT JOIN _hearings fh ON fh.arrived = s.first_hearing \
         LEFT JOIN _endings fe ON fe.id = s.first_ending \
         LEFT JOIN _hearings lh ON lh.arrived = s.last_hearing \
         LEFT JOIN _endings le ON le.id = s.last_ending \
         WHERE s.entity = ?1 AND s.line = 'main' AND s.key = ?2 AND s.to_key = ?3",
        newest_occurrence("s.last_hearing", "point"),
        newest_occurrence("s.last_hearing", "event")
    )
}

/// A fact of `?1`, `?2`, `?3` other than the hearing `?4` or the ending `?5`: hearing, ending,
/// integer key, point, event and valid time.
pub(crate) const SEEN_OTHER: &str = "SELECT * FROM (SELECT arrived, NULL, integer, point, event, \
     valid FROM _hearings WHERE entity = ?1 AND line = 'main' AND key = ?2 \
     AND to_key IS nullif(?3, '') AND arrived IS NOT ?4 UNION ALL SELECT NULL, id, NULL, point, \
     event, valid FROM _endings WHERE entity = ?1 AND line = 'main' AND key = ?2 \
     AND to_key = ?3 AND id IS NOT ?5) LIMIT 1";

/// Starts the seen row of `?1`, `?2`, `?3` from its fact `?4` (a hearing) or `?5` (an ending),
/// with the valid time `?6`.
pub(crate) const SEEN_START: &str = "INSERT INTO _seen(entity, key, to_key, first_hearing, \
     first_ending, last_hearing, last_ending, recorded_hearing, recorded_ending, first_true, \
     count) VALUES (?1, ?2, ?3, ?4, ?5, ?4, ?5, ?4, ?5, ?6, 1)";

/// Keeps `?4`/`?5` as first and `?6`/`?7` as last of `?1`, `?2`, `?3`, adds `?8` to its count and
/// takes the valid time `?9` as its first true when earlier.
pub(crate) fn seen_keep() -> String {
    let earlier = format!("{} < {}", valid_day("?9"), valid_day("first_true"));
    format!(
        "UPDATE _seen SET first_hearing = ?4, first_ending = ?5, last_hearing = ?6, \
         last_ending = ?7, count = count + ?8, first_true = CASE WHEN ?9 IS NOT NULL \
         AND (first_true IS NULL OR {earlier}) THEN ?9 ELSE first_true END \
         WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3"
    )
}

/// The SQL deciding a link's ending, the `_endings` row `e`, of the link `(entity, from, to)` (SQL
/// expressions): that it is the link's ending by the point `known` when given, and that its
/// valid time is on or before `day`, NULL unknown. Its ending by then is the earliest ending,
/// by point and then event id, after its last live hearing by then; there is none once a live
/// hearing comes after every ending, or while a correction is its latest conclusion. Only the
/// endings after that hearing are read, never its history.
pub(crate) fn ending_sql(
    e: &str,
    (entity, from, to): (&str, &str, &str),
    known: Option<&str>,
    day: &str,
) -> (String, String) {
    let link = |t: &str| {
        format!(
            "{t}.entity = {entity} AND {t}.line = 'main' AND {t}.key = {from} AND {t}.to_key = {to}"
        )
    };
    let by = |t: &str| known.map_or(String::new(), |known| format!(" AND {t}.point <= {known}"));
    // The last live hearing's point and event, by its occurrences, or below every ending when
    // there is none.
    let live = |column: &str, none: &str| {
        format!(
            "coalesce((SELECT {column} FROM {} WHERE true{} ORDER BY {} LIMIT 1), {none})",
            occurrences(&format!("{} AND h.integer IS NULL", link("h"))),
            known.map_or(String::new(), |known| format!(" AND point <= {known}")),
            order("DESC")
        )
    };
    let after = live("point", &i64::MIN.to_string());
    (
        format!(
            "{e}.id = (SELECT x.id FROM _endings x \
             WHERE {link_x}{by_x} AND x.point >= {after} AND (x.point, x.event) > ({after}, {}) \
             ORDER BY x.point, x.event LIMIT 1) \
             AND NOT EXISTS (SELECT 1 FROM _ends c WHERE {link_c} AND c.point IS NULL)",
            live("event", "''"),
            link_x = link("x"),
            by_x = by("x"),
            link_c = link("c"),
        ),
        format!("{} <= {day}", valid_day(&format!("{e}.valid"))),
    )
}

/// The one read of what is seen of `?1`, `?2`, `?3` (its target key `''` for a node): its kept
/// row, with the facts it names, each a hearing joined by when it arrived or an ending by its
/// id; or, for a node or link with one fact, which keeps no row, that fact. Then when a link
/// ended: count, first true, when it ended, then first heard, last heard and first recorded as
/// event, source, valid time, point, integer key, properties and whether an ending.
pub(crate) fn seen_sql() -> String {
    let fact = |p: &str, point: &str| {
        format!(
            "coalesce({p}h.event, {p}e.event), coalesce({p}h.source, {p}e.source), \
             CASE WHEN {p}h.arrived IS NULL THEN {p}e.valid ELSE {p}h.valid END, \
             coalesce({point}, {p}e.point), {p}h.integer, {p}h.properties, \
             {p}h.arrived IS NULL"
        )
    };
    let join = |p: &str, column: &str| {
        format!(
            "LEFT JOIN _hearings {p}h ON {p}h.arrived = s.{column}_hearing \
             LEFT JOIN _endings {p}e ON {p}e.id = s.{column}_ending"
        )
    };
    let (ending, _) = ending_sql("n", ("?1", "?2", "?3"), None, "NULL");
    // A node or link with one fact, a hearing or an ending, keeps no row: that fact is read.
    let one = |fact: &str, from: &str, to: &str| {
        format!(
            "UNION ALL SELECT 1, valid, (SELECT n.point FROM _endings n WHERE {ending}), \
             {fact}, {fact}, {fact} FROM {from} WHERE entity = ?1 AND line = 'main' AND key = ?2 \
             AND {to} AND NOT EXISTS (SELECT 1 FROM _seen WHERE entity = ?1 AND line = 'main' \
             AND key = ?2 AND to_key = ?3)"
        )
    };
    format!(
        "SELECT s.count, s.first_true, (SELECT n.point FROM _endings n WHERE {ending}), \
         {}, {}, {} FROM _seen s {} {} {} WHERE s.entity = ?1 AND s.line = 'main' \
         AND s.key = ?2 AND s.to_key = ?3 {} {} LIMIT 1",
        fact("f", "fh.point"),
        // A hearing heard again is last heard at its newest occurrence.
        fact("l", &newest_occurrence("s.last_hearing", "point")),
        fact("r", "rh.point"),
        join("f", "first"),
        join("l", "last"),
        join("r", "recorded"),
        one(
            "event, source, valid, point, integer, properties, 0",
            "_hearings",
            "to_key IS nullif(?3, '')"
        ),
        one(
            "event, source, valid, point, NULL, NULL, 1",
            "_endings",
            "to_key = ?3"
        ),
    )
}

/// Picks the newest version row at or before the point `?4`, in a class not ordered by an
/// integer key (and for a link, which is never ordered by one).
pub(crate) fn by_point() -> String {
    format!(
        "AND integer IS NULL AND point <= ?4 ORDER BY {} LIMIT 1",
        order("DESC")
    )
}

/// Picks the newest version row at or before the integer key `?4`.
pub(crate) fn by_integer_key() -> String {
    format!("AND integer <= ?4 ORDER BY {} LIMIT 1", order("DESC"))
}

/// Picks the version row valid on or before the day `?4` (and, given `column`, at or before
/// `?5` on it), the greatest by valid time and then by its class's order.
pub(crate) fn as_of(column: Option<&str>) -> String {
    let valid = valid_day("valid");
    let known = column.map_or(String::new(), |column| format!(" AND {column} <= ?5"));
    format!(
        "AND {valid} <= ?4{known} ORDER BY {valid} DESC, {} LIMIT 1",
        order("DESC")
    )
}

/// The newest of the live hearings and endings of the link `?1`, `?2`, `?3` at or before the
/// point `?4`: event, source, valid, point, integer key, properties, whether an ending.
pub(crate) fn link_state() -> String {
    format!(
        "SELECT event, source, valid, point, integer, properties, ending FROM ( \
         SELECT event, source, valid, point, integer, properties, 0 AS ending FROM _hearings \
         WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3 AND integer IS NULL \
         AND point <= ?4 UNION ALL SELECT event, source, valid, point, NULL, NULL, 1 \
         FROM _endings WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3 \
         AND point <= ?4) ORDER BY {} LIMIT 1",
        order("DESC")
    )
}

/// Whether the link `?1`, `?2`, `?3` had ended by the point `?4`, and as of the valid time `?5`
/// when given (NULL when its ending's valid time is unknown); no row when it had not ended.
pub(crate) fn ended_sql() -> String {
    let (ending, valid_by) = ending_sql("e", ("?1", "?2", "?3"), Some("?4"), "?5");
    format!("SELECT ?5 IS NULL OR {valid_by} FROM _endings e WHERE {ending}")
}

/// Records the latest conclusion to arrive for the link `?1`, `?2`, `?3`: the event `?4` from
/// `?5`, at the point `?6` with the valid time `?7`, both NULL for a correction.
pub(crate) const CONCLUDE: &str = "INSERT INTO _ends(entity, key, to_key, event, source, point, \
     valid) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(entity, line, key, to_key) \
     DO UPDATE SET event = excluded.event, source = excluded.source, point = excluded.point, \
     valid = excluded.valid";

/// Keeps the ending of the link `?1`, `?2`, `?3` at the point `?4`, in the event `?5` from `?6`,
/// with the valid time `?7`.
pub(crate) const ENDING: &str = "INSERT OR IGNORE INTO _endings(entity, key, to_key, point, \
     event, source, valid) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// The correction of the link `?1`, `?2`, `?3`, when one is its latest conclusion: event, source.
pub(crate) const CORRECTED: &str = "SELECT event, source FROM _ends \
     WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3 AND point IS NULL";

/// The ending of the link `?1`, `?2`, `?3`: event, source, point, valid.
pub(crate) fn link_ending() -> String {
    let (ending, _) = ending_sql("e", ("?1", "?2", "?3"), None, "NULL");
    format!("SELECT event, source, point, valid FROM _endings e WHERE {ending}")
}

/// Names the hearing `?1` by the event `?2` at the place `?3`, at the point `?4`.
pub(crate) const RENAME: &str =
    "UPDATE _hearings SET event = ?2, seq = ?3, point = ?4 WHERE line = 'main' AND identity = ?1";

/// Holds `?2` as an alias of the hearing `?1`, at the point `?3`.
pub(crate) const ALIAS: &str =
    "INSERT OR IGNORE INTO _aliases(identity, alias, point) VALUES (?1, ?2, ?3)";

/// The aliases of the hearing `?1`, in order.
pub(crate) const ALIASES_OF: &str =
    "SELECT alias FROM _aliases WHERE line = 'main' AND identity = ?1 ORDER BY alias";

/// What the hearing `?1` is of: entity, key, target key.
pub(crate) const HEARING_OF: &str =
    "SELECT entity, key, to_key FROM _hearings WHERE line = 'main' AND identity = ?1";

/// The first (`ASC`) or last (`DESC`) occurrence of a hearing of `?1`, `?2`, `?3`: hearing,
/// ending (NULL), integer key, point, event.
pub(crate) fn edge_hearing(direction: &str) -> String {
    format!(
        "SELECT arrived, NULL, integer, point, event FROM {} ORDER BY {} LIMIT 1",
        occurrences_of(),
        order(direction)
    )
}

/// The first (`ASC`) or last (`DESC`) ending of the link `?1`, `?2`, `?3`: hearing (NULL),
/// ending, integer key (NULL), point, event.
pub(crate) fn edge_ending(direction: &str) -> String {
    format!(
        "SELECT NULL, id, NULL, point, event FROM _endings \
         WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3 \
         ORDER BY point {direction}, event {direction} LIMIT 1"
    )
}

/// Every hearing of `?1`, `?2`, `?3` in order on its axis: identity, event, source, valid,
/// point, integer key.
pub(crate) fn hearings() -> String {
    format!(
        "SELECT identity, event, source, valid, point, integer FROM _hearings \
         WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key IS ?3 ORDER BY {}",
        order("ASC")
    )
}

pub(crate) const FILLED: &str = "SELECT cursor FROM _filled WHERE one = 1";

pub(crate) const SET_FILLED: &str = "UPDATE _filled SET cursor = ?1 WHERE one = 1";

/// Puts a link that waits for an end into `_open`, or updates the one already waiting.
pub(crate) const OPEN_LINK: &str = "INSERT INTO _open(rel, from_key, to_key, source, valid) \
     VALUES (?1, ?2, ?3, ?4, ?5) \
     ON CONFLICT(rel, from_key, to_key) DO UPDATE SET \
     source = excluded.source, valid = excluded.valid \
     WHERE source IS NOT excluded.source OR valid IS NOT excluded.valid";

pub(crate) const CLOSE_OPEN: &str =
    "DELETE FROM _open WHERE rel = ?1 AND from_key = ?2 AND to_key = ?3";

pub(crate) const SAVEPOINT: &str = "SAVEPOINT event";

pub(crate) const RELEASE: &str = "RELEASE event";

pub(crate) const ROLLBACK_TO: &str = "ROLLBACK TO event; RELEASE event";

pub(crate) const TABLE_EXISTS: &str =
    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)";

pub(crate) const SCHEMA_VERSION: &str = "PRAGMA schema_version";

pub(crate) const LOAD_ONTOLOGY: &str = "SELECT kind, body FROM _ontology";

pub(crate) const SAVE_ONTOLOGY: &str =
    "INSERT OR REPLACE INTO _ontology(kind, name, body) VALUES (?1, ?2, ?3)";

pub(crate) const APPEND_EVENT: &str = "INSERT INTO _events(seq, body, id) VALUES (?1, ?2, ?3)";

pub(crate) const HIGH_WATER: &str =
    "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = '_events'), 0)";

pub(crate) const EVENT_AT: &str = "SELECT id, body FROM _events WHERE seq = ?1";

pub(crate) const EVENTS_BETWEEN: &str =
    "SELECT seq, id, body FROM _events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq";

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

/// Creates a class's table, with one column per property (`columns` is each `, <column> <type>`).
pub(crate) fn create_class_table(class: &str, columns: &str) -> String {
    format!(
        "CREATE TABLE {}(id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE, \
         source TEXT NOT NULL, valid TEXT, recorded INTEGER NOT NULL{columns});",
        class_table(class)
    )
}

/// Creates a relation's table between its two classes' tables, and its index by target.
pub(crate) fn create_relation_table(relation: &str, from: &str, to: &str) -> String {
    let table = relation_table(relation);
    format!(
        "CREATE TABLE {table}(src INTEGER NOT NULL REFERENCES {}(id), \
         dst INTEGER NOT NULL REFERENCES {}(id), source TEXT NOT NULL, valid TEXT, \
         recorded INTEGER NOT NULL, PRIMARY KEY(src, dst)) WITHOUT ROWID; \
         CREATE INDEX {} ON {table}(dst, src);",
        class_table(from),
        class_table(to),
        relation_index(relation)
    )
}

/// Writes a node into its class's table under its key: a new row, or the same row with the
/// given properties, source, valid time and record time replaced. Binds `?1` key, `?2` source,
/// `?3` valid, `?4` recorded, then one value per property in `properties`' order.
pub(crate) fn upsert_node(class: &str, properties: &[&str]) -> String {
    let columns = concat(properties, |p| format!(", {}", property_column(p)));
    let holes = concat(0..properties.len(), |i| format!(", ?{}", i + 5));
    let updates = concat(properties, |p| {
        let column = property_column(p);
        format!(", {column} = excluded.{column}")
    });
    format!(
        "INSERT INTO {}(key, source, valid, recorded{columns}) VALUES (?1, ?2, ?3, ?4{holes}) \
         ON CONFLICT(key) DO UPDATE SET source = excluded.source, valid = excluded.valid, \
         recorded = excluded.recorded{updates}",
        class_table(class)
    )
}

/// Writes a link into its relation's table between the nodes with the from-key `?1` and the
/// to-key `?2`; `?3` source, `?4` valid, `?5` recorded. A missing end leaves its id NULL, which
/// the table refuses.
pub(crate) fn upsert_link(relation: &str, from: &str, to: &str) -> String {
    format!(
        "INSERT INTO {}(src, dst, source, valid, recorded) \
         VALUES ((SELECT id FROM {} WHERE key = ?1), (SELECT id FROM {} WHERE key = ?2), ?3, ?4, ?5) \
         ON CONFLICT(src, dst) DO UPDATE SET source = excluded.source, valid = excluded.valid, \
         recorded = excluded.recorded",
        relation_table(relation),
        class_table(from),
        class_table(to)
    )
}

/// Reads the node keyed `?1` from its class's table: its source, valid time, record time, then
/// one column per property in `properties`' order.
pub(crate) fn select_node(class: &str, properties: &[&str]) -> String {
    let columns = concat(properties, |p| format!(", {}", property_column(p)));
    format!(
        "SELECT source, valid, recorded{columns} FROM {} WHERE key = ?1",
        class_table(class)
    )
}

/// The id of the node keyed `?1` in its class's table.
pub(crate) fn node_id(class: &str) -> String {
    format!("SELECT id FROM {} WHERE key = ?1", class_table(class))
}

/// The open links of the relation `?1` waiting on the key `?2` at the end `column`
/// (`from_key` or `to_key`).
pub(crate) fn waiting(column: &str) -> String {
    format!("SELECT from_key, to_key, source, valid FROM _open WHERE rel = ?1 AND {column} = ?2")
}

/// Adds a property's column to its class's table.
pub(crate) fn add_property_column(class: &str, property: &str, column_type: &str) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN {} {column_type};",
        class_table(class),
        property_column(property)
    )
}
