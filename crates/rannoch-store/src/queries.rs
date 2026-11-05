//! Every SQL statement and fragment the store runs. No other module holds a string that begins a
//! statement; [`ident`] is the only function that quotes an identifier, and every value reaches
//! SQL as a bound parameter.

use std::collections::BTreeMap;

use rannoch_ontology::Ontology;

use crate::Error;
use crate::ontology::{CLASS, RELATION};

/// A statement's text built on first use and kept for the life of the process, so no call
/// formats it again.
macro_rules! built {
    ($text:expr) => {{
        static TEXT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| $text);
        TEXT.as_str()
    }};
}

/// How a connection is opened. Freed space is zeroed on every deletion and page split, so no copy
/// of a removed body is left in free space by a split made before the removal. A commit is
/// durable: on macOS bundled SQLite issues `F_FULLFSYNC` only with `fullfsync` on.
pub(crate) const PRAGMAS: &str = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
     PRAGMA fullfsync=ON; PRAGMA foreign_keys=ON; PRAGMA secure_delete=ON;";

/// The memory every connection works in: a 64 MB page cache, so fill keeps the pages it touches
/// until commit instead of reading them back and spilling them mid-transaction; temporary tables
/// in memory; and a checkpoint every 10,000 WAL pages. None of it changes what a commit syncs.
pub(crate) const CACHE: &str =
    "PRAGMA cache_size=-65536; PRAGMA temp_store=MEMORY; PRAGMA wal_autocheckpoint=10000;";

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
    CREATE TABLE IF NOT EXISTS _fill(one INTEGER PRIMARY KEY CHECK (one = 1),
        try_limit INTEGER NOT NULL);
    INSERT OR IGNORE INTO _fill(one, try_limit) VALUES (1, 3);
    CREATE TABLE IF NOT EXISTS _positions(
        producer TEXT PRIMARY KEY, position TEXT NOT NULL) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _places(
        worker TEXT PRIMARY KEY, cursor INTEGER NOT NULL) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _work(
        worker TEXT NOT NULL, seq INTEGER NOT NULL, reason TEXT,
        tries INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(worker, seq)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _kept(
        worker TEXT PRIMARY KEY, state TEXT NOT NULL) WITHOUT ROWID;";

/// Sets producer `?1`'s position in its source to `?2`.
pub(crate) const SET_POSITION: &str = "INSERT INTO _positions(producer, position) VALUES (?1, ?2) \
     ON CONFLICT(producer) DO UPDATE SET position = excluded.position";

pub(crate) const POSITION: &str = "SELECT position FROM _positions WHERE producer = ?1";

/// Worker `?1`'s place: the last cursor handed out to it.
pub(crate) const PLACE: &str = "SELECT cursor FROM _places WHERE worker = ?1";

pub(crate) const SET_PLACE: &str = "INSERT INTO _places(worker, cursor) VALUES (?1, ?2) \
     ON CONFLICT(worker) DO UPDATE SET cursor = excluded.cursor";

/// The first event after `?1` and at or below `?2`.
pub(crate) const NEXT_EVENT: &str =
    "SELECT seq, id, body FROM _events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT 1";

/// Worker `?1` joins at the start of the log, if it has no place yet.
pub(crate) const JOIN_WORKER: &str = "INSERT OR IGNORE INTO _places(worker, cursor) VALUES (?1, 0)";

/// The processed mark: for each worker, the point before its oldest event in progress or else
/// its place; the lowest of those. Null when there is no worker.
pub(crate) const PROCESSED: &str = "SELECT MIN(COALESCE((SELECT MIN(w.seq) - 1 FROM _work w \
     WHERE w.worker = p.worker AND w.reason IS NULL), p.cursor)) FROM _places p";

pub(crate) const REMOVE_PLACE: &str = "DELETE FROM _places WHERE worker = ?1";

pub(crate) const REMOVE_WORK: &str = "DELETE FROM _work WHERE worker = ?1";

pub(crate) const REMOVE_KEPT: &str = "DELETE FROM _kept WHERE worker = ?1";

/// Every worker but `?1`: where it resumes (the point before its oldest event in progress, or else
/// its place), the state it keeps, and its last event closed as failed.
pub(crate) const PLACES: &str = "SELECT p.worker, COALESCE((SELECT MIN(w.seq) - 1 FROM _work w \
     WHERE w.worker = p.worker AND w.reason IS NULL), p.cursor), k.state, \
     (SELECT MAX(w.seq) FROM _work w WHERE w.worker = p.worker AND w.reason IS NOT NULL) \
     FROM _places p LEFT JOIN _kept k ON k.worker = p.worker WHERE p.worker <> ?1";

/// Sets the state worker `?1` keeps with its place to `?2`.
pub(crate) const SET_KEPT: &str = "INSERT INTO _kept(worker, state) VALUES (?1, ?2) \
     ON CONFLICT(worker) DO UPDATE SET state = excluded.state";

/// Drops worker `?1`'s events in progress: a place saved by the queue already stands before them.
pub(crate) const DROP_OPEN_WORK: &str = "DELETE FROM _work WHERE worker = ?1 AND reason IS NULL";

/// Records event `?2` as failed for worker `?1` with reason `?3`, over any earlier reason.
pub(crate) const FAILED_WORK: &str = "INSERT INTO _work(worker, seq, reason) VALUES (?1, ?2, ?3) \
     ON CONFLICT(worker, seq) DO UPDATE SET reason = excluded.reason";

/// Worker `?1`'s events still in progress, oldest first.
pub(crate) const OPEN_WORK: &str =
    "SELECT seq FROM _work WHERE worker = ?1 AND reason IS NULL ORDER BY seq";

/// Marks event `?2` in progress for worker `?1`.
pub(crate) const START_WORK: &str = "INSERT INTO _work(worker, seq, reason) VALUES (?1, ?2, NULL)";

/// The row of event `?2` for worker `?1`: present while in progress (reason null) or failed.
pub(crate) const WORK: &str = "SELECT reason FROM _work WHERE worker = ?1 AND seq = ?2";

/// Closes event `?2` in progress for worker `?1` as done.
pub(crate) const DONE_WORK: &str =
    "DELETE FROM _work WHERE worker = ?1 AND seq = ?2 AND reason IS NULL";

/// Closes event `?2` in progress for worker `?1` as failed with reason `?3`.
pub(crate) const FAIL_WORK: &str =
    "UPDATE _work SET reason = ?3 WHERE worker = ?1 AND seq = ?2 AND reason IS NULL";

/// The store's try limit: how many times fill tries an event before it stops.
pub(crate) const TRY_LIMIT: &str = "SELECT try_limit FROM _fill WHERE one = 1";

pub(crate) const SET_TRY_LIMIT: &str = "UPDATE _fill SET try_limit = ?1 WHERE one = 1";

/// Worker `?1`'s failed events tried fewer than `?2` times, oldest first.
pub(crate) const RETRY_WORK: &str = "SELECT seq FROM _work \
     WHERE worker = ?1 AND reason IS NOT NULL AND tries < ?2 ORDER BY seq";

/// Worker `?1`'s failed events tried `?2` times or more, oldest first, with their last reason.
pub(crate) const STOPPED_WORK: &str = "SELECT seq, reason FROM _work \
     WHERE worker = ?1 AND reason IS NOT NULL AND tries >= ?2 ORDER BY seq";

/// Event `?2` failed for worker `?1` with reason `?3`: its first try, or one more.
pub(crate) const TRIED_WORK: &str = "INSERT INTO _work(worker, seq, reason, tries) \
     VALUES (?1, ?2, ?3, 1) \
     ON CONFLICT(worker, seq) DO UPDATE SET reason = excluded.reason, tries = tries + 1";

/// Closes event `?2`, failed for worker `?1`, as done.
pub(crate) const DONE_RETRY: &str =
    "DELETE FROM _work WHERE worker = ?1 AND seq = ?2 AND reason IS NOT NULL";

/// Sets the tries of event `?2`, failed for worker `?1`, back to none.
pub(crate) const RESET_TRIES: &str =
    "UPDATE _work SET tries = 0 WHERE worker = ?1 AND seq = ?2 AND reason IS NOT NULL";

/// rannoch's history tables (oracle history.rs:21-54). The hearings, one version row each, with
/// no valid-to: `arrived` counts up as this store records them, `axis` is its point on its
/// class's or relation's axis (its parts encoded so the bytes compare as the parts do; its
/// known-at when no axis is declared), `known` when it became known, and `properties` holds a
/// node's. Then the other ids each arrived under, each an occurrence of its hearing at its own
/// known-at; the latest conclusion by known-at for each link that ended or was found wrong
/// (`axis` null for a correction); every ending of a link, at its point (`valid` null when
/// unknown); every correction of a link, seeded from the conclusions when a store first has this
/// table; what is kept of when each node and link was seen; the
/// amendments waiting for their target; the check value of the graph's key; and the highest
/// known-at ever filled, a high-water mark seeded by one scan when a store first has none kept.
///
/// Every history table carries the line it belongs to in its key. Every history read and write
/// is on the line `'main'` until a second line exists.
pub(crate) const HISTORY_TABLES: &str = "
    CREATE TABLE IF NOT EXISTS _hearings(
        arrived INTEGER PRIMARY KEY, line TEXT NOT NULL DEFAULT 'main', identity BLOB NOT NULL,
        entity TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT, event TEXT NOT NULL,
        seq INTEGER NOT NULL, source TEXT NOT NULL, valid TEXT, axis BLOB NOT NULL,
        known INTEGER NOT NULL, recorded INTEGER NOT NULL, properties TEXT,
        until INTEGER, amended INTEGER, UNIQUE(line, identity));
    CREATE INDEX IF NOT EXISTS _hearings_seq ON _hearings(seq);
    CREATE TABLE IF NOT EXISTS _amendments(
        target INTEGER NOT NULL, known INTEGER NOT NULL, event TEXT NOT NULL,
        seq INTEGER NOT NULL, PRIMARY KEY(target, known, event)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _renames(
        entity TEXT NOT NULL, old TEXT NOT NULL, new TEXT NOT NULL, known INTEGER NOT NULL,
        event TEXT NOT NULL, PRIMARY KEY(entity, old, known, event)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _hearings_at
        ON _hearings(entity, line, key, to_key, axis, known, event);
    CREATE INDEX IF NOT EXISTS _hearings_to ON _hearings(entity, line, to_key, key);
    CREATE TABLE IF NOT EXISTS _aliases(
        line TEXT NOT NULL DEFAULT 'main', identity BLOB NOT NULL, alias TEXT NOT NULL,
        axis BLOB NOT NULL, known INTEGER NOT NULL,
        PRIMARY KEY(line, identity, alias)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS _aliases_at ON _aliases(line, identity, axis, known);
    CREATE INDEX IF NOT EXISTS _aliases_alias ON _aliases(alias);
    CREATE TABLE IF NOT EXISTS _ends(
        entity TEXT NOT NULL, line TEXT NOT NULL DEFAULT 'main', key TEXT NOT NULL,
        to_key TEXT NOT NULL, event TEXT NOT NULL, source TEXT NOT NULL, axis BLOB,
        known INTEGER NOT NULL, valid TEXT,
        PRIMARY KEY(entity, line, key, to_key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _endings(
        id INTEGER PRIMARY KEY, entity TEXT NOT NULL, line TEXT NOT NULL DEFAULT 'main',
        key TEXT NOT NULL, to_key TEXT NOT NULL, axis BLOB NOT NULL, known INTEGER NOT NULL,
        event TEXT NOT NULL, source TEXT NOT NULL, valid TEXT,
        UNIQUE(entity, line, key, to_key, axis, event));
    CREATE TABLE IF NOT EXISTS _corrections(
        entity TEXT NOT NULL, line TEXT NOT NULL DEFAULT 'main', key TEXT NOT NULL,
        to_key TEXT NOT NULL, known INTEGER NOT NULL, event TEXT NOT NULL, source TEXT NOT NULL,
        PRIMARY KEY(entity, line, key, to_key, known, event)) WITHOUT ROWID;
    INSERT OR IGNORE INTO _corrections(entity, line, key, to_key, known, event, source)
        SELECT entity, line, key, to_key, known, event, source FROM _ends
        WHERE axis IS NULL AND NOT EXISTS (SELECT 1 FROM _corrections);
    CREATE TABLE IF NOT EXISTS _seen(
        entity TEXT NOT NULL, line TEXT NOT NULL DEFAULT 'main', key TEXT NOT NULL,
        to_key TEXT NOT NULL, first_hearing INTEGER, first_ending INTEGER,
        last_hearing INTEGER, last_ending INTEGER, recorded_hearing INTEGER,
        recorded_ending INTEGER, first_true TEXT, count INTEGER NOT NULL,
        PRIMARY KEY(entity, line, key, to_key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _waiting(
        source TEXT NOT NULL, id TEXT NOT NULL, known INTEGER NOT NULL, event TEXT NOT NULL,
        seq INTEGER NOT NULL, PRIMARY KEY(source, id, known, event)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _purge_unfinished(
        one INTEGER PRIMARY KEY CHECK (one = 1));
    CREATE TABLE IF NOT EXISTS _purge_rules(
        kind TEXT NOT NULL, value BLOB NOT NULL, rule TEXT NOT NULL,
        PRIMARY KEY(kind, value)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS _check(value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS _newest(one INTEGER PRIMARY KEY CHECK (one = 1), known INTEGER);
    INSERT OR IGNORE INTO _newest(one, known) SELECT 1, (SELECT max(k) FROM (
        SELECT max(known) AS k FROM _hearings UNION ALL SELECT max(until) FROM _hearings
        UNION ALL SELECT max(known) FROM _aliases UNION ALL SELECT max(known) FROM _endings
        UNION ALL SELECT max(known) FROM _ends))
        WHERE NOT EXISTS (SELECT 1 FROM _newest);";

pub(crate) const CHECK: &str = "SELECT value FROM _check";

pub(crate) const SET_CHECK: &str = "INSERT INTO _check(value) VALUES (?1)";

/// The hearing whose content identity is `?1`: the event it is named by and that event's place.
pub(crate) const HELD: &str =
    "SELECT event, seq FROM _hearings WHERE line = 'main' AND identity = ?1";

pub(crate) const HEAR: &str = "INSERT INTO _hearings(identity, entity, key, to_key, event, seq, \
     source, valid, axis, known, recorded, properties) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";

/// The first event the log holds whose `CloudEvent` source is `?1` and id `?2`: its place and
/// body. It reads every body, so only an amendment, which is rare, runs it; nothing on the write
/// path pays for it.
pub(crate) const EVENT_BY_CLOUDEVENT: &str = "SELECT seq, body FROM _events \
     WHERE CASE WHEN json_valid(body) THEN json_extract(body, '$.id') = ?2 \
     AND json_extract(body, '$.source') = ?1 ELSE 0 END ORDER BY seq LIMIT 1";

/// Keeps the amendment `?4` at the place `?5` (known at `?3`) waiting for the event whose
/// `CloudEvent` source is `?1` and id `?2`.
pub(crate) const WAIT: &str = "INSERT OR IGNORE INTO _waiting(source, id, known, event, seq) \
     VALUES (?1, ?2, ?3, ?4, ?5)";

/// The `CloudEvent` id of every target an amendment waits for.
pub(crate) const WAITED: &str = "SELECT DISTINCT id FROM _waiting";

/// The places of the amendments waiting for the event whose source is `?1` and id `?2`, by
/// known-at and then event id.
pub(crate) const WAITING_FOR: &str =
    "SELECT seq FROM _waiting WHERE source = ?1 AND id = ?2 ORDER BY known, event";

pub(crate) const STOP_WAITING: &str = "DELETE FROM _waiting WHERE source = ?1 AND id = ?2";

/// The hearing the event at the place `?1` made itself: arrived, entity, key, target key, event
/// it is named by, axis point and known-at.
pub(crate) const MADE_BY: &str = "SELECT arrived, entity, key, to_key, event, axis, known \
     FROM _hearings WHERE line = 'main' AND seq = ?1 AND amended IS NULL";

/// Keeps the amendment `?3` (its known-at `?2`, its place `?4`) of the event at the place `?1`.
pub(crate) const AMENDMENT: &str =
    "INSERT OR IGNORE INTO _amendments(target, known, event, seq) VALUES (?1, ?2, ?3, ?4)";

/// Every amendment of the event at the place `?1` in point order, then event id: its point,
/// event and body.
pub(crate) const AMENDMENTS_OF: &str = "SELECT a.known, a.event, e.body FROM _amendments a \
     JOIN _events e ON e.seq = a.seq WHERE a.target = ?1 ORDER BY a.known, a.event";

/// What the hearings amendments made of the event at the place `?1` are of.
pub(crate) const AMENDED_OF: &str = "SELECT entity, key, to_key FROM _hearings \
     WHERE line = 'main' AND amended = ?1";

pub(crate) const DROP_AMENDED: &str = "DELETE FROM _hearings WHERE line = 'main' AND amended = ?1";

/// Makes the hearing `?1` known again with no end, before its amendments are applied again.
pub(crate) const KNOWN_AGAIN: &str = "UPDATE _hearings SET until = NULL WHERE arrived = ?1";

/// Keeps the rename of the node `?2` of the class entity `?1` to `?3`, known from the point
/// `?4`, made by the amendment `?5`.
pub(crate) const RENAMED: &str =
    "INSERT OR IGNORE INTO _renames(entity, old, new, known, event) VALUES (?1, ?2, ?3, ?4, ?5)";

/// Every rename in the class entity `?1`, in point order and then event id: old key, new key,
/// point and event.
pub(crate) const RENAMES_IN: &str =
    "SELECT old, new, known, event FROM _renames WHERE entity = ?1 ORDER BY known, event";

/// Whether the store holds any rename.
pub(crate) const ANY_RENAME: &str = "SELECT EXISTS (SELECT 1 FROM _renames)";

/// Whether the key `?2` of the class entity `?1` was ever renamed.
pub(crate) const WAS_RENAMED: &str =
    "SELECT EXISTS (SELECT 1 FROM _renames WHERE entity = ?1 AND old = ?2)";

/// The places of the events with a hearing of the entity `?1` keyed `?2` (as a link's target
/// key when `?3` is true).
/// Copies the endings of the links of `?1` whose start (`?4` false) or end (`?4` true) is keyed
/// `?2` onto the key `?3`, so a renamed node's links keep their endings under its new key, known
/// there from the rename's known-at `?5` at the earliest.
pub(crate) const RENAME_ENDINGS: &str = "INSERT OR IGNORE INTO _endings(entity, line, key, \
     to_key, axis, known, event, source, valid) SELECT entity, line, \
     CASE WHEN ?4 THEN key ELSE ?3 END, CASE WHEN ?4 THEN ?3 ELSE to_key END, axis, \
     max(known, ?5), event, source, valid FROM _endings \
     WHERE entity = ?1 AND line = 'main' AND CASE WHEN ?4 THEN to_key = ?2 ELSE key = ?2 END";

/// Copies the conclusions of the links of `?1` keyed `?2` at one end onto `?3`, as
/// [`RENAME_ENDINGS`] does for endings.
pub(crate) const RENAME_ENDS: &str = "INSERT OR IGNORE INTO _ends(entity, line, key, to_key, \
     event, source, axis, known, valid) SELECT entity, line, CASE WHEN ?4 THEN key ELSE ?3 END, \
     CASE WHEN ?4 THEN ?3 ELSE to_key END, event, source, axis, known, valid FROM _ends \
     WHERE entity = ?1 AND line = 'main' AND CASE WHEN ?4 THEN to_key = ?2 ELSE key = ?2 END";

/// Copies the corrections of the links of `?1` keyed `?2` at one end onto `?3`, as
/// [`RENAME_ENDINGS`] does for endings.
pub(crate) const RENAME_CORRECTIONS: &str = "INSERT OR IGNORE INTO _corrections(entity, line, \
     key, to_key, known, event, source) SELECT entity, line, CASE WHEN ?4 THEN key ELSE ?3 END, \
     CASE WHEN ?4 THEN ?3 ELSE to_key END, known, event, source FROM _corrections \
     WHERE entity = ?1 AND line = 'main' AND CASE WHEN ?4 THEN to_key = ?2 ELSE key = ?2 END";

pub(crate) const PLACES_NAMING: &str = "SELECT DISTINCT seq FROM _hearings WHERE line = 'main' \
     AND entity = ?1 AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END";

/// How many facts the node or link `?1`, `?2`, `?3` (its target key `''` for a node) has known
/// now: its hearings and a link's endings.
pub(crate) const FACTS_KNOWN: &str = "SELECT (SELECT count(*) FROM _hearings WHERE entity = ?1 \
     AND line = 'main' AND key = ?2 AND to_key IS nullif(?3, '') AND until IS NULL) \
     + (SELECT count(*) FROM _endings WHERE entity = ?1 AND line = 'main' AND key = ?2 \
     AND to_key = ?3)";

/// The first hearing this store recorded of `?1`, `?2`, `?3` among those known now, else its
/// first ending, and the earliest valid time any of its facts was given.
pub(crate) fn seen_recount() -> &'static str {
    built!({
        format!(
            "SELECT (SELECT min(arrived) FROM _hearings WHERE {h}), \
             (SELECT min(id) FROM _endings WHERE {e}), \
             (SELECT valid FROM (SELECT valid FROM _hearings WHERE {h} UNION ALL \
             SELECT valid FROM _endings WHERE {e}) WHERE valid IS NOT NULL ORDER BY {} LIMIT 1)",
            valid_day("valid"),
            h = "entity = ?1 AND line = 'main' AND key = ?2 AND to_key IS nullif(?3, '') \
                 AND until IS NULL",
            e = "entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3",
        )
    })
}

/// Puts the seen row of `?1`, `?2`, `?3` whole: first and last facts, first recorded, first
/// true and count.
pub(crate) const SEEN_PUT: &str = "INSERT OR REPLACE INTO _seen(entity, line, key, to_key, \
     first_hearing, first_ending, last_hearing, last_ending, recorded_hearing, recorded_ending, \
     first_true, count) VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

/// Sets when the hearing `?1` stopped being known: the known-at `?2`, or NULL while it is.
pub(crate) const KNOWN_UNTIL: &str = "UPDATE _hearings SET until = ?2 WHERE arrived = ?1";

/// A hearing made by an amendment of the event at the place `?6`: at the axis point `?9`, known
/// from `?10`.
pub(crate) const HEAR_AMENDED: &str = "INSERT INTO _hearings(identity, entity, key, to_key, \
     event, seq, source, valid, axis, known, recorded, properties, amended) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?6)";

pub(crate) const UNSEEN: &str = "DELETE FROM _seen WHERE entity = ?1 AND line = 'main' \
     AND key = ?2 AND to_key = ?3";

/// Whether the store keeps any purge rule.
pub(crate) const ANY_RULE: &str = "SELECT EXISTS (SELECT 1 FROM _purge_rules)";

/// Keeps the rule `?3` against the match value `?2` of the kind `?1`; the first kept stays.
pub(crate) const STAND_RULE: &str =
    "INSERT OR IGNORE INTO _purge_rules(kind, value, rule) VALUES (?1, ?2, ?3)";

/// The rule standing against the match value `?2` of the kind `?1`.
pub(crate) const RULE_OF: &str = "SELECT rule FROM _purge_rules WHERE kind = ?1 AND value = ?2";

/// The places of the hearing whose identity is `?1`.
pub(crate) const PLACES_OF_IDENTITY: &str =
    "SELECT seq FROM _hearings WHERE line = 'main' AND identity = ?1";

/// The identity of the hearing the event at the place `?1` made itself.
pub(crate) const IDENTITY_AT: &str =
    "SELECT identity FROM _hearings WHERE line = 'main' AND seq = ?1 AND amended IS NULL";

/// Every hearing the event at the place `?1` is named on: identity, entity, key, target key.
pub(crate) const HEARINGS_AT: &str =
    "SELECT identity, entity, key, to_key FROM _hearings WHERE line = 'main' AND seq = ?1";

pub(crate) const DROP_ALIASES: &str = "DELETE FROM _aliases WHERE line = 'main' AND identity = ?1";

pub(crate) const DROP_HEARINGS: &str = "DELETE FROM _hearings WHERE line = 'main' AND seq = ?1";

/// Every link with a conclusion made by the event `?1`.
pub(crate) const CONCLUDED_BY: &str = "SELECT entity, key, to_key FROM _ends WHERE event = ?1 \
     UNION SELECT entity, key, to_key FROM _endings WHERE event = ?1 \
     UNION SELECT entity, key, to_key FROM _corrections WHERE event = ?1";

pub(crate) const DROP_ENDS: &str = "DELETE FROM _ends WHERE event = ?1";

pub(crate) const DROP_ENDINGS: &str = "DELETE FROM _endings WHERE event = ?1";

pub(crate) const DROP_CORRECTIONS: &str = "DELETE FROM _corrections WHERE event = ?1";

/// Every link of `?1` with a conclusion, keyed `?2` at its start (`?3` false) or its end.
pub(crate) const UNCONCLUDED: &str = "SELECT entity, key, to_key FROM _ends \
     WHERE entity = ?1 AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END \
     UNION SELECT entity, key, to_key FROM _endings \
     WHERE entity = ?1 AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END \
     UNION SELECT entity, key, to_key FROM _corrections \
     WHERE entity = ?1 AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END";

pub(crate) const DROP_ENDS_AT: &str = "DELETE FROM _ends WHERE entity = ?1 \
     AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END";

pub(crate) const DROP_ENDINGS_AT: &str = "DELETE FROM _endings WHERE entity = ?1 \
     AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END";

pub(crate) const DROP_CORRECTIONS_AT: &str = "DELETE FROM _corrections WHERE entity = ?1 \
     AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END";

pub(crate) const DROP_ENDS_OF: &str =
    "DELETE FROM _ends WHERE entity = ?1 AND key = ?2 AND to_key = ?3";

/// Keeps the latest conclusion left of the link `?1`, `?2`, `?3`, by known-at and then event id.
pub(crate) const LATEST_CONCLUSION: &str = "INSERT INTO _ends(entity, key, to_key, event, source, \
     axis, valid, known) SELECT ?1, ?2, ?3, event, source, axis, valid, known FROM ( \
     SELECT event, source, axis, valid, known FROM _endings \
     WHERE entity = ?1 AND key = ?2 AND to_key = ?3 \
     UNION ALL SELECT event, source, NULL, NULL, known FROM _corrections \
     WHERE entity = ?1 AND key = ?2 AND to_key = ?3) ORDER BY known DESC, event DESC LIMIT 1";

/// The place of the event the amendment at the place `?1` patched.
pub(crate) const TARGET_OF: &str = "SELECT target FROM _amendments WHERE seq = ?1";

pub(crate) const DROP_AMENDMENT: &str = "DELETE FROM _amendments WHERE seq = ?1";

pub(crate) const DROP_WAITING: &str = "DELETE FROM _waiting WHERE seq = ?1";

/// The renames the amendment `?1` made: entity, old key, new key.
pub(crate) const RENAMES_BY: &str = "SELECT entity, old, new FROM _renames WHERE event = ?1";

pub(crate) const DROP_RENAMES: &str = "DELETE FROM _renames WHERE event = ?1";

pub(crate) const DROP_WORK: &str = "DELETE FROM _work WHERE seq = ?1";

pub(crate) const DROP_EVENT: &str = "DELETE FROM _events WHERE seq = ?1";

/// Every link of `?1` heard with the key `?2` at its start (`?3` false) or its end.
pub(crate) const LINKS_AT: &str = "SELECT DISTINCT key, to_key FROM _hearings WHERE line = 'main' \
     AND entity = ?1 AND CASE WHEN ?3 THEN to_key = ?2 ELSE key = ?2 END";

/// Moves every page of the write-ahead log into the file and truncates it to nothing. Its first
/// column says whether a reader kept it from finishing.
pub(crate) const TRUNCATE_LOG: &str = "PRAGMA wal_checkpoint(TRUNCATE)";
/// A purge's transaction marks it unfinished until its write-ahead log has been truncated.
pub(crate) const PURGE_UNFINISHED: &str = "INSERT OR IGNORE INTO _purge_unfinished(one) VALUES (1)";
pub(crate) const IS_PURGE_UNFINISHED: &str = "SELECT EXISTS (SELECT 1 FROM _purge_unfinished)";
pub(crate) const PURGE_FINISHED: &str = "DELETE FROM _purge_unfinished";

/// The order versions are chosen by, `DESC` putting the newest first: by axis point, then
/// known-at, so a correction at the same point follows what it corrects, then event id.
pub(crate) fn order(direction: &str) -> String {
    format!("axis {direction}, known {direction}, event {direction}")
}

/// Every occurrence of the hearings `filter` picks, over their `_hearings` row `h`: its own, at
/// its row's axis point and known-at under its event, and each alias's, at the alias's under the
/// alias. A row carries its hearing's columns, `name` the event the hearing is named by, and the
/// occurrence's `axis`, `known` and `event`, which [`order`] picks by.
pub(crate) fn occurrences(filter: &str) -> String {
    known_occurrences(&format!("{filter} AND h.until IS NULL"))
}

/// Every occurrence of the hearings `filter` picks, whether known now or only before an
/// amendment superseded them: each row also carries its hearing's `until`. `CROSS JOIN` keeps
/// the hearing first, so each alias is a seek on its line and identity, not a scan of its line.
fn known_occurrences(filter: &str) -> String {
    let hearing = "h.arrived, h.identity, h.key, h.to_key, h.event AS name, h.source, h.valid, \
                   h.recorded, h.properties, h.until";
    format!(
        "(SELECT {hearing}, h.axis AS axis, h.known AS known, h.event AS event FROM _hearings h \
         WHERE {filter} \
         UNION ALL SELECT {hearing}, a.axis, a.known, a.alias FROM _hearings h \
         CROSS JOIN _aliases a ON a.line = h.line AND a.identity = h.identity WHERE {filter})"
    )
}

/// The node or link `?1`, `?2`, `?3` (entity, key, target key), over its `_hearings` row `h`.
const OF: &str = "h.entity = ?1 AND h.line = 'main' AND h.key = ?2 AND h.to_key IS ?3";

/// The occurrences of the node or link `?1`, `?2`, `?3` (entity, key, target key).
pub(crate) fn occurrences_of() -> String {
    occurrences(OF)
}

/// The newest version row of the node or link `?1`, `?2`, `?3` among its hearings'
/// occurrences, by its class's order: event, source, valid, axis, known-at, properties,
/// recorded, identity.
pub(crate) fn newest_version() -> &'static str {
    built!(format!(
        "SELECT name, source, valid, axis, known, properties, recorded, identity \
         FROM {} WHERE true ORDER BY {}",
        occurrences_of(),
        order("DESC")
    ))
}

/// The occurrences of the hearings `filter` picks as the view of the axis bound `axis` and the
/// known-at `known` sees them (SQL expressions, each NULL for no bound, which is now): each
/// hearing is known from its `known` until the `until` of the amendment that superseded it.
fn in_view(filter: &str, (axis, known): (&str, &str)) -> String {
    format!(
        "(SELECT * FROM {} WHERE ({known} IS NULL OR known <= {known}) \
         AND ({axis} IS NULL OR axis <= {axis}))",
        known_occurrences(&format!(
            "{filter} AND (h.until IS NULL OR h.until > coalesce({known}, {}))",
            i64::MAX
        ))
    )
}

/// One version row of the node or link `?1`, `?2`, `?3` among its hearings in the view of the
/// axis bound `?4` and the known-at `?5`, as `tail` picks it: see [`newest_version`].
fn view_row(tail: &str) -> String {
    format!(
        "SELECT name, source, valid, axis, known, properties, recorded, identity \
         FROM {} WHERE true {tail}",
        in_view(OF, ("?4", "?5"))
    )
}

/// The newest version row in the view `?4`, `?5`: see [`view_row`].
pub(crate) fn newest_in_view() -> &'static str {
    built!(view_row(&format!("ORDER BY {} LIMIT 1", order("DESC"))))
}

/// The version row in the view `?4`, `?5` valid on or before the day `?6`, the greatest by valid
/// time and then by its class's order: see [`view_row`].
pub(crate) fn as_of() -> &'static str {
    built!({
        let valid = valid_day("valid");
        view_row(&format!(
            "AND {valid} <= ?6 ORDER BY {valid} DESC, {} LIMIT 1",
            order("DESC")
        ))
    })
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
/// names as first and last: hearing, ending, axis point, known-at and event of each.
pub(crate) fn seen_kept() -> &'static str {
    built!({
        format!(
            "SELECT s.first_hearing, s.first_ending, coalesce(fh.axis, fe.axis), \
             coalesce(fh.known, fe.known), coalesce(fh.event, fe.event), s.last_hearing, \
             s.last_ending, coalesce({}, le.axis), coalesce({}, le.known), coalesce({}, le.event) \
             FROM _seen s \
             LEFT JOIN _hearings fh ON fh.arrived = s.first_hearing \
             LEFT JOIN _endings fe ON fe.id = s.first_ending \
             LEFT JOIN _hearings lh ON lh.arrived = s.last_hearing \
             LEFT JOIN _endings le ON le.id = s.last_ending \
             WHERE s.entity = ?1 AND s.line = 'main' AND s.key = ?2 AND s.to_key = ?3",
            newest_occurrence("s.last_hearing", "axis"),
            newest_occurrence("s.last_hearing", "known"),
            newest_occurrence("s.last_hearing", "event")
        )
    })
}

/// A fact of `?1`, `?2`, `?3` other than the hearing `?4` or the ending `?5`: hearing, ending,
/// axis point, known-at, event and valid time.
pub(crate) const SEEN_OTHER: &str = "SELECT * FROM (SELECT arrived, NULL, axis, known, event, \
     valid FROM _hearings WHERE entity = ?1 AND line = 'main' AND key = ?2 \
     AND to_key IS nullif(?3, '') AND until IS NULL AND arrived IS NOT ?4 UNION ALL SELECT NULL, id, axis, known, \
     event, valid FROM _endings WHERE entity = ?1 AND line = 'main' AND key = ?2 \
     AND to_key = ?3 AND id IS NOT ?5) LIMIT 1";

/// Starts the seen row of `?1`, `?2`, `?3` from its fact `?4` (a hearing) or `?5` (an ending),
/// with the valid time `?6`.
pub(crate) const SEEN_START: &str = "INSERT INTO _seen(entity, key, to_key, first_hearing, \
     first_ending, last_hearing, last_ending, recorded_hearing, recorded_ending, first_true, \
     count) VALUES (?1, ?2, ?3, ?4, ?5, ?4, ?5, ?4, ?5, ?6, 1)";

/// Keeps `?4`/`?5` as first and `?6`/`?7` as last of `?1`, `?2`, `?3`, adds `?8` to its count and
/// takes the valid time `?9` as its first true when earlier.
pub(crate) fn seen_keep() -> &'static str {
    built!({
        let earlier = format!("{} < {}", valid_day("?9"), valid_day("first_true"));
        format!(
            "UPDATE _seen SET first_hearing = ?4, first_ending = ?5, last_hearing = ?6, \
             last_ending = ?7, count = count + ?8, first_true = CASE WHEN ?9 IS NOT NULL \
             AND (first_true IS NULL OR {earlier}) THEN ?9 ELSE first_true END \
             WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3"
        )
    })
}

/// The SQL deciding a link's ending, the `_endings` row `e`, of the link `(entity, from, to)` (SQL
/// expressions): that it is the link's ending in the view of the axis bound and known-at `view`
/// (SQL expressions, each NULL for no bound) when given, and that its valid time is on or before
/// `day`, NULL unknown. Its ending in that view is the earliest ending, by axis point, known-at
/// and then event id, after its last live hearing in it; there is none once a live hearing comes
/// after every ending. Only the endings after that hearing are read, never its history.
pub(crate) fn ending_sql(
    e: &str,
    (entity, from, to): (&str, &str, &str),
    view: Option<(&str, &str)>,
    day: &str,
) -> (String, String) {
    let link = |t: &str| {
        format!(
            "{t}.entity = {entity} AND {t}.line = 'main' AND {t}.key = {from} AND {t}.to_key = {to}"
        )
    };
    let by = |t: &str| {
        view.map_or(String::new(), |(axis, known)| {
            format!(
                " AND ({axis} IS NULL OR {t}.axis <= {axis}) \
                 AND ({known} IS NULL OR {t}.known <= {known})"
            )
        })
    };
    // The last live hearing's axis point, known-at and event, by its occurrences, or below every
    // ending when there is none.
    let hearings = view.map_or_else(|| occurrences(&link("h")), |view| in_view(&link("h"), view));
    let live = |column: &str, none: &str| {
        format!(
            "coalesce((SELECT {column} FROM {hearings} ORDER BY {} LIMIT 1), {none})",
            order("DESC")
        )
    };
    (
        format!(
            "{e}.id = (SELECT x.id FROM _endings x \
             WHERE {link_x}{by_x} AND (x.axis, x.known, x.event) > ({}, {}, {}) \
             ORDER BY x.axis, x.known, x.event LIMIT 1)",
            live("axis", "X''"),
            live("known", &i64::MIN.to_string()),
            live("event", "''"),
            link_x = link("x"),
            by_x = by("x"),
        ),
        format!("{} <= {day}", valid_day(&format!("{e}.valid"))),
    )
}

/// The one read of what is seen of `?1`, `?2`, `?3` (its target key `''` for a node): its kept
/// row, with the facts it names, each a hearing joined by when it arrived or an ending by its
/// id; or, for a node or link with one fact, which keeps no row, that fact. Then when a link
/// ended, which it has not while a correction is its latest conclusion: count, first true, the
/// axis point it ended at, then first heard, last heard and first
/// recorded as event, source, valid time, axis point, known-at, properties and whether an ending.
pub(crate) fn seen_sql() -> &'static str {
    built!({
        let fact = |p: &str, (axis, known): (&str, &str)| {
            format!(
                "coalesce({p}h.event, {p}e.event), coalesce({p}h.source, {p}e.source), \
                 CASE WHEN {p}h.arrived IS NULL THEN {p}e.valid ELSE {p}h.valid END, \
                 coalesce({axis}, {p}e.axis), coalesce({known}, {p}e.known), {p}h.properties, \
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
        let ending = format!(
            "{ending} AND NOT EXISTS (SELECT 1 FROM _ends c WHERE c.entity = ?1 \
             AND c.line = 'main' AND c.key = ?2 AND c.to_key = ?3 AND c.axis IS NULL)"
        );
        // A node or link with one fact, a hearing or an ending, keeps no row: that fact is read.
        let one = |fact: &str, from: &str, to: &str| {
            format!(
                "UNION ALL SELECT 1, valid, (SELECT n.axis FROM _endings n WHERE {ending}), \
                 {fact}, {fact}, {fact} FROM {from} WHERE entity = ?1 AND line = 'main' AND key = ?2 \
                 AND {to} AND NOT EXISTS (SELECT 1 FROM _seen WHERE entity = ?1 AND line = 'main' \
                 AND key = ?2 AND to_key = ?3)"
            )
        };
        let (last_axis, last_known) = (
            newest_occurrence("s.last_hearing", "axis"),
            newest_occurrence("s.last_hearing", "known"),
        );
        format!(
            "SELECT s.count, s.first_true, (SELECT n.axis FROM _endings n WHERE {ending}), \
             {}, {}, {} FROM _seen s {} {} {} WHERE s.entity = ?1 AND s.line = 'main' \
             AND s.key = ?2 AND s.to_key = ?3 {} {} LIMIT 1",
            fact("f", ("fh.axis", "fh.known")),
            // A hearing heard again is last heard at its newest occurrence.
            fact("l", (&last_axis, &last_known)),
            fact("r", ("rh.axis", "rh.known")),
            join("f", "first"),
            join("l", "last"),
            join("r", "recorded"),
            one(
                "event, source, valid, axis, known, properties, 0",
                "_hearings",
                "to_key IS nullif(?3, '') AND until IS NULL"
            ),
            one(
                "event, source, valid, axis, known, NULL, 1",
                "_endings",
                "to_key = ?3"
            ),
        )
    })
}

/// The hearings in the view of the axis bound `?2` and the known-at `?3` (each NULL for now), as
/// columns of `_hearings` unqualified. A node or link is in the view exactly when one of its
/// hearings' own rows is: a hearing is named by its smallest id, so no alias of it is known
/// before it.
const IN_VIEW: &str = "line = 'main' AND (?2 IS NULL OR axis <= ?2) \
     AND (?3 IS NULL OR known <= ?3) \
     AND (until IS NULL OR until > coalesce(?3, 9223372036854775807))";

/// The view `?2`, `?3` as bounds on the occurrences [`in_view`] reads.
const VIEW: (&str, &str) = ("?2", "?3");

/// The newest of the rows `facts` holds for each `of` (a column), by their class's order, with
/// `columns` read from it.
fn newest_each(facts: &str, of: &str, columns: &str) -> String {
    format!(
        "SELECT {columns} FROM (SELECT *, row_number() OVER (PARTITION BY {of} ORDER BY {}) AS n \
         FROM {facts}) WHERE n = 1",
        order("DESC")
    )
}

/// The version of each node of the class entity `?1` in the view `?2`, `?3`, in key order, its
/// key first, then the columns [`view_row`] reads; `keys` (SQL) narrows the keys read, or is
/// empty for every key.
fn nodes_in_view(keys: &str) -> String {
    let filter = if keys.is_empty() {
        "h.entity = ?1 AND h.line = 'main'".to_string()
    } else {
        format!("h.entity = ?1 AND h.line = 'main' AND h.key IN ({keys})")
    };
    format!(
        "{} ORDER BY key",
        newest_each(
            &in_view(&filter, VIEW),
            "key",
            "key, name, source, valid, axis, known, properties"
        )
    )
}

/// Every node of the class entity `?1` in the view `?2`, `?3`: see [`nodes_in_view`].
pub(crate) fn all_nodes_in_view() -> &'static str {
    built!(nodes_in_view(""))
}

/// The keys of the class `class` with a hearing in the view `?2`, `?3` whose `property` held the
/// value whose JSON is `?4`: read through the property's declared index.
fn keys_by_property(class: &str, property: &str) -> String {
    format!(
        "SELECT key FROM _hearings WHERE entity = {} AND {} = json_extract(?4, '$') \
         AND {IN_VIEW}",
        literal(&format!("{CLASS}:{class}")),
        property_value(property)
    )
}

/// The links of the relation entity `?1` at the node keyed `?4`, from it (`out`) or to it, that
/// hold in the view `?2`, `?3`, in the other end's key order: the other end's key, then the
/// columns of the newest of each one's live hearings and endings, as [`link_state`] reads it.
/// A link holds when its newest fact in the view is a live hearing and no correction known in
/// the view follows that hearing's event.
pub(crate) fn links_in_view(out: bool) -> &'static str {
    if out {
        built!(holding("key = ?4", "to_key", "key = ?4"))
    } else {
        // Endings have no index by target: those of the links heard to the node are sought.
        built!(holding(
            "to_key = ?4",
            "key",
            "to_key = ?4 AND key IN (SELECT key FROM _hearings \
             WHERE entity = ?1 AND line = 'main' AND to_key = ?4)",
        ))
    }
}

/// How many links [`links_in_view`] reads.
pub(crate) fn links_counted(out: bool) -> &'static str {
    if out {
        built!(format!("SELECT count(*) FROM ({})", links_in_view(true)))
    } else {
        built!(format!("SELECT count(*) FROM ({})", links_in_view(false)))
    }
}

/// How many links of the relation entity `?1` hold in the view `?2`, `?3`, as
/// [`links_in_view`] decides: those heard in it, less those it reads as ended or found wrong,
/// which only a link with an ending or a correction in the view can be.
pub(crate) fn link_count() -> &'static str {
    built!({
        let ended = "IN (SELECT key, to_key FROM _endings WHERE entity = ?1 AND line = 'main' \
             AND (?2 IS NULL OR axis <= ?2) AND (?3 IS NULL OR known <= ?3) \
             UNION SELECT key, to_key FROM _corrections WHERE entity = ?1 AND line = 'main' \
             AND (?3 IS NULL OR known <= ?3))";
        format!(
            "SELECT (SELECT count(*) FROM (SELECT DISTINCT key, to_key FROM _hearings \
             WHERE entity = ?1 AND {IN_VIEW})) - (SELECT count(*) FROM ({}) f \
             WHERE (ending = 1 AND {}) OR (ending = 0 AND {CORRECTED_IN_VIEW}))",
            newest_each(
                &link_facts(
                    &format!("(h.key, h.to_key) {ended}"),
                    &format!("(key, to_key) {ended}")
                ),
                "key, to_key",
                "key, to_key, event, ending"
            ),
            heard_in_view()
        )
    })
}

/// Whether a correction known in the view follows the event of the row `f`, the newest fact in
/// the view of a link of `?1`. A correction has no axis point: it holds across the whole axis
/// from its known-at, until a newer hearing's event follows it.
const CORRECTED_IN_VIEW: &str = "EXISTS (SELECT 1 FROM _corrections c WHERE c.entity = ?1 \
     AND c.line = 'main' AND c.key = f.key AND c.to_key = f.to_key \
     AND (?3 IS NULL OR c.known <= ?3) AND c.event > f.event)";

/// Whether the link of `?1` named by the row `f` has a hearing in the view `?2`, `?3`.
fn heard_in_view() -> String {
    format!(
        "EXISTS (SELECT 1 FROM _hearings WHERE entity = ?1 AND {IN_VIEW} AND key = f.key \
         AND to_key = f.to_key)"
    )
}

/// The live hearings' occurrences (`ending` 0) and the endings (`ending` 1) in the view `?2`,
/// `?3` of the links of `?1` whose hearings `heard` picks (SQL over `_hearings h`) and whose
/// endings `ends` picks (over `_endings`), each with its link's ends and the columns
/// [`link_state`] reads.
fn link_facts(heard: &str, ends: &str) -> String {
    format!(
        "(SELECT key, to_key, name, source, valid, axis, known, properties, event, 0 AS ending \
         FROM {} UNION ALL SELECT key, to_key, event, source, valid, axis, known, NULL, event, 1 \
         FROM _endings WHERE entity = ?1 AND line = 'main' AND {ends} \
         AND (?2 IS NULL OR axis <= ?2) AND (?3 IS NULL OR known <= ?3))",
        in_view(
            &format!("h.entity = ?1 AND h.line = 'main' AND h.to_key IS NOT NULL AND {heard}"),
            VIEW
        )
    )
}

/// The links of `?1` whose hearings `heard` picks (SQL over `_hearings`) and whose endings `ends`
/// picks (over `_endings`) that hold in the view `?2`, `?3`, each named by `other`, newest fact
/// first: see [`links_in_view`].
fn holding(heard: &str, other: &str, ends: &str) -> String {
    format!(
        "SELECT {other}, name, source, valid, axis, known, properties FROM ({}) f \
         WHERE ending = 0 AND NOT {CORRECTED_IN_VIEW} ORDER BY {other}",
        newest_each(
            &link_facts(&format!("h.{heard}"), ends),
            "key, to_key",
            "key, to_key, name, source, valid, axis, known, properties, event, ending"
        )
    )
}

/// How many keys of the class entity `?1` have a version in the view `?2`, `?3`.
pub(crate) fn node_count() -> &'static str {
    built!({ format!("SELECT count(DISTINCT key) FROM _hearings WHERE entity = ?1 AND {IN_VIEW}") })
}

/// The highest known-at ever filled: of a hearing, the amendment that superseded one, an alias,
/// an ending or a conclusion, kept by fill per batch. It can exceed the greatest the tables hold
/// now, after a later-filled amendment makes an earlier one redundant; a read known at either
/// reads the same state, all the store knows.
pub(crate) const KNOWN_NOW: &str = "SELECT known FROM _newest";

/// Raises the highest known-at ever filled to `?1`.
pub(crate) const KEEP_NEWEST: &str =
    "UPDATE _newest SET known = ?1 WHERE known IS NULL OR known < ?1";

/// The newest of the live hearings and endings of the link `?1`, `?2`, `?3` in the view of the
/// axis bound `?4` and the known-at `?5`: event, source, valid, axis point, known-at, properties,
/// whether an ending, whether a correction known in the view follows that fact's own event.
pub(crate) fn link_state() -> &'static str {
    built!({
        format!(
            "SELECT event, source, valid, axis, known, properties, ending, \
             EXISTS (SELECT 1 FROM _corrections c WHERE c.entity = ?1 AND c.line = 'main' \
             AND c.key = ?2 AND c.to_key = ?3 AND (?5 IS NULL OR c.known <= ?5) \
             AND c.event > occurrence) FROM ( \
             SELECT name AS event, source, valid, axis, known, properties, 0 AS ending, \
             event AS occurrence FROM {} \
             UNION ALL SELECT event, source, valid, axis, known, NULL, 1, event \
             FROM _endings WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3 \
             AND (?4 IS NULL OR axis <= ?4) AND (?5 IS NULL OR known <= ?5)) \
             ORDER BY {}, occurrence DESC LIMIT 1",
            in_view(OF, ("?4", "?5")),
            order("DESC")
        )
    })
}

/// Whether the link `?1`, `?2`, `?3` had ended in the view of the axis bound `?4` and the
/// known-at `?5`, and as of the valid time `?6` when given (NULL when its ending's valid time is
/// unknown); no row when it had not ended.
pub(crate) fn ended_sql() -> &'static str {
    built!({
        let (ending, valid_by) = ending_sql("e", ("?1", "?2", "?3"), Some(("?4", "?5")), "?6");
        format!("SELECT ?6 IS NULL OR {valid_by} FROM _endings e WHERE {ending}")
    })
}

/// Records the conclusion of the link `?1`, `?2`, `?3` in the event `?4` from `?5`, at the axis
/// point `?6` with the valid time `?7` (both NULL for a correction), known at `?8`, when it is the
/// latest by known-at and then event id: never by arrival.
pub(crate) const CONCLUDE: &str = "INSERT INTO _ends(entity, key, to_key, event, source, axis, \
     valid, known) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(entity, line, key, to_key) \
     DO UPDATE SET event = excluded.event, source = excluded.source, axis = excluded.axis, \
     valid = excluded.valid, known = excluded.known \
     WHERE (excluded.known, excluded.event) > (_ends.known, _ends.event)";

/// Keeps the ending of the link `?1`, `?2`, `?3` at the axis point `?4`, known at `?8`, in the
/// event `?5` from `?6`, with the valid time `?7`.
pub(crate) const ENDING: &str = "INSERT OR IGNORE INTO _endings(entity, key, to_key, axis, \
     event, source, valid, known) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";

/// Keeps the correction of the link `?1`, `?2`, `?3`, known at `?4`, in the event `?5` from `?6`.
pub(crate) const CORRECTION: &str = "INSERT OR IGNORE INTO _corrections(entity, key, to_key, \
     known, event, source) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

/// The newest correction of the link `?1`, `?2`, `?3` that no newer live hearing's event follows:
/// event, source, whether a correction is its latest conclusion.
pub(crate) fn link_correction() -> &'static str {
    built!(format!(
        "SELECT c.event, c.source, EXISTS (SELECT 1 FROM _ends e WHERE e.entity = ?1 \
         AND e.line = 'main' AND e.key = ?2 AND e.to_key = ?3 AND e.axis IS NULL) \
         FROM _corrections c WHERE c.entity = ?1 AND c.line = 'main' AND c.key = ?2 \
         AND c.to_key = ?3 AND c.event > coalesce((SELECT event FROM {} WHERE true \
         ORDER BY {} LIMIT 1), '') ORDER BY c.known DESC, c.event DESC LIMIT 1",
        occurrences_of(),
        order("DESC")
    ))
}

/// The ending of the link `?1`, `?2`, `?3`: event, source, axis point, known-at, valid.
pub(crate) fn link_ending() -> &'static str {
    built!({
        let (ending, _) = ending_sql("e", ("?1", "?2", "?3"), None, "NULL");
        format!("SELECT event, source, axis, known, valid FROM _endings e WHERE {ending}")
    })
}

/// Names the hearing `?1` by the event `?2` at the place `?3`, at the axis point `?4` and the
/// known-at `?5`.
pub(crate) const RENAME: &str = "UPDATE _hearings SET event = ?2, seq = ?3, axis = ?4, known = ?5 \
     WHERE line = 'main' AND identity = ?1";

/// Holds `?2` as an alias of the hearing `?1`, at the axis point `?3` and the known-at `?4`.
pub(crate) const ALIAS: &str =
    "INSERT OR IGNORE INTO _aliases(identity, alias, axis, known) VALUES (?1, ?2, ?3, ?4)";

/// The aliases of the hearing `?1`, in order.
pub(crate) const ALIASES_OF: &str =
    "SELECT alias FROM _aliases WHERE line = 'main' AND identity = ?1 ORDER BY alias";

/// What the hearing `?1` is of: entity, key, target key.
pub(crate) const HEARING_OF: &str =
    "SELECT entity, key, to_key FROM _hearings WHERE line = 'main' AND identity = ?1";

/// The first or the last occurrence of a hearing of `?1`, `?2`, `?3`: hearing, ending (NULL),
/// axis point, known-at, event.
fn edge_hearing(direction: &str) -> String {
    format!(
        "SELECT arrived, NULL, axis, known, event FROM {} ORDER BY {} LIMIT 1",
        occurrences_of(),
        order(direction)
    )
}

/// The first or the last ending of the link `?1`, `?2`, `?3`: hearing (NULL), ending, axis
/// point, known-at, event.
fn edge_ending(direction: &str) -> String {
    format!(
        "SELECT NULL, id, axis, known, event FROM _endings \
         WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key = ?3 \
         ORDER BY {} LIMIT 1",
        order(direction)
    )
}

/// Every hearing of `?1`, `?2`, `?3` in order on its axis: identity, event, source, valid,
/// axis point, known-at.
pub(crate) fn hearings() -> &'static str {
    built!({
        format!(
            "SELECT identity, event, source, valid, axis, known FROM _hearings \
             WHERE entity = ?1 AND line = 'main' AND key = ?2 AND to_key IS ?3 AND until IS NULL \
             ORDER BY {}",
            order("ASC")
        )
    })
}

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

pub(crate) const SCHEMA_VERSION: &str = "PRAGMA schema_version";

/// Moves `PRAGMA schema_version` without leaving anything behind.
pub(crate) const TOUCH_SCHEMA: &str = "CREATE TABLE _touched(x); DROP TABLE _touched;";

pub(crate) const LOAD_ONTOLOGY: &str = "SELECT kind, body FROM _ontology";

pub(crate) const SAVE_ONTOLOGY: &str =
    "INSERT OR REPLACE INTO _ontology(kind, name, body) VALUES (?1, ?2, ?3)";

pub(crate) const APPEND_EVENT: &str = "INSERT INTO _events(seq, body, id) VALUES (?1, ?2, ?3)";

pub(crate) const HIGH_WATER: &str =
    "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = '_events'), 0)";

/// Writes a copy of the database into the empty file at `?1`, read in one read transaction.
pub(crate) const COPY_INTO: &str = "VACUUM INTO ?1";

pub(crate) const EVENT_AT: &str = "SELECT id, body FROM _events WHERE seq = ?1";

pub(crate) const EVENTS_BETWEEN: &str =
    "SELECT seq, id, body FROM _events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq";

/// At most `?3` of the events after `?1` and up to `?2`.
pub(crate) const EVENTS_CHUNK: &str =
    "SELECT seq, id, body FROM _events WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?3";

pub(crate) const COUNT_BETWEEN: &str = "SELECT COUNT(*) FROM _events WHERE seq > ?1 AND seq <= ?2";

/// A quoted SQL identifier.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A quoted SQL string literal, for the one place a value cannot be bound: an index's
/// definition, and the query that must name the same text to read through it.
fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A node hearing's value of `property`, as its declared index holds it.
fn property_value(property: &str) -> String {
    format!(
        "json_extract(properties, {})",
        literal(&format!("$.\"{property}\""))
    )
}

/// Indexes the hearings of `class`'s nodes by the value of `property`, led by the entity so a
/// read seeks that class's. No index is partial: a `WHERE` on a column a statement binds makes
/// SQLite prepare that statement again whenever the bound value changes.
pub(crate) fn create_property_index(class: &str, property: &str) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON _hearings(entity, {}, key);",
        ident(&format!("_by:{}:{class}:{property}", class.len())),
        property_value(property)
    )
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

/// Writes a node into its class's table under its key: a new row, or the same row with its
/// source, valid time and record time replaced and each property the version carries. Binds `?1`
/// key, `?2` source, `?3` valid, `?4` recorded, then for each of `properties` in order its value
/// and whether the version carries it; one it does not carry is left as the row holds it.
fn upsert_node<'a>(class: &str, properties: impl Iterator<Item = &'a String> + Clone) -> String {
    let columns = concat(properties.clone(), |p| format!(", {}", property_column(p)));
    let holes = concat(properties.clone().enumerate(), |(i, _)| {
        format!(", ?{}", 2 * i + 5)
    });
    let updates = concat(properties.enumerate(), |(i, p)| {
        let column = property_column(p);
        format!(
            ", {column} = CASE WHEN ?{} THEN excluded.{column} ELSE {column} END",
            2 * i + 6
        )
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
/// the table refuses as a NOT NULL constraint, so the insert itself says whether both ends are
/// present. One row of values, not a select, so SQLite needs no statement journal for it.
fn upsert_link(relation: &str, from: &str, to: &str) -> String {
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

/// Reads the node keyed `?1` from its class's table: its source, valid time and record time.
fn select_node(class: &str) -> String {
    format!(
        "SELECT source, valid, recorded FROM {} WHERE key = ?1",
        class_table(class)
    )
}

/// Removes the node keyed `?1` from its class's table.
fn delete_node(class: &str) -> String {
    format!("DELETE FROM {} WHERE key = ?1", class_table(class))
}

/// Removes the link from the node keyed `?1` to the node keyed `?2` from its relation's table.
fn delete_link(relation: &str, from: &str, to: &str) -> String {
    format!(
        "DELETE FROM {} WHERE src = (SELECT id FROM {} WHERE key = ?1) \
         AND dst = (SELECT id FROM {} WHERE key = ?2)",
        relation_table(relation),
        class_table(from),
        class_table(to)
    )
}

/// The open links of the relation `?1` waiting on the key `?2` at their target end (`to`), or at
/// their source end.
fn waiting(column: &str) -> String {
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

/// Each item formatted by `each` and joined into one string.
pub(crate) fn concat<T>(items: impl IntoIterator<Item = T>, each: impl Fn(T) -> String) -> String {
    items.into_iter().map(each).collect::<Vec<_>>().concat()
}

/// A statement by its text and its slot: where a fill transaction keeps its handle.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Stmt<'a> {
    pub(crate) slot: usize,
    pub(crate) text: &'a str,
}

/// A statement whose text is one per class or relation, built when the ontology loads.
#[derive(Debug)]
pub(crate) struct Owned {
    slot: usize,
    text: String,
}

impl Owned {
    pub(crate) fn stmt(&self) -> Stmt<'_> {
        Stmt {
            slot: self.slot,
            text: &self.text,
        }
    }
}

/// The statements of one class: writing and removing a node, which fill runs; reading its row,
/// and its nodes by each indexed property, which the reads run.
#[derive(Debug)]
pub(crate) struct ClassSql {
    /// Its declared properties, in the order `put` binds them.
    pub(crate) properties: Vec<String>,
    pub(crate) put: Owned,
    pub(crate) remove: Owned,
    pub(crate) row: String,
    pub(crate) by: BTreeMap<String, String>,
}

/// The statements of one relation that fill runs: writing and removing a link.
#[derive(Debug)]
pub(crate) struct RelationSql {
    pub(crate) put: Owned,
    pub(crate) remove: Owned,
}

/// Every statement of an ontology's classes and relations, each text built once when the
/// ontology loads.
#[derive(Debug)]
pub(crate) struct Catalogue {
    pub(crate) classes: BTreeMap<String, ClassSql>,
    pub(crate) relations: BTreeMap<String, RelationSql>,
    /// How many slots its statements and the fixed ones take.
    pub(crate) slots: usize,
}

impl Catalogue {
    /// The statements of the class `name`.
    pub(crate) fn class(&self, name: &str) -> Result<&ClassSql, Error> {
        self.classes
            .get(name)
            .ok_or_else(|| Error::Refused(format!("class {name} is not declared")))
    }

    /// The statements of the relation `name`.
    pub(crate) fn relation(&self, name: &str) -> Result<&RelationSql, Error> {
        self.relations
            .get(name)
            .ok_or_else(|| Error::Refused(format!("relation {name} is not declared")))
    }

    pub(crate) fn new(ontology: &Ontology) -> Catalogue {
        let mut slots = Sql::COUNT;
        let mut owned = |text: String| {
            slots += 1;
            Owned {
                slot: slots - 1,
                text,
            }
        };
        let classes = ontology
            .classes
            .values()
            .map(|class| {
                let sql = ClassSql {
                    properties: class.properties.keys().cloned().collect(),
                    put: owned(upsert_node(&class.name, class.properties.keys())),
                    remove: owned(delete_node(&class.name)),
                    row: select_node(&class.name),
                    by: class
                        .indexed
                        .iter()
                        .map(|p| {
                            let keys = keys_by_property(&class.name, p);
                            (p.clone(), nodes_in_view(&keys))
                        })
                        .collect(),
                };
                (class.name.clone(), sql)
            })
            .collect();
        let relations = ontology
            .relations
            .values()
            .map(|r| {
                let sql = RelationSql {
                    put: owned(upsert_link(&r.name, &r.from, &r.to)),
                    remove: owned(delete_link(&r.name, &r.from, &r.to)),
                };
                (r.name.clone(), sql)
            })
            .collect();
        Catalogue {
            classes,
            relations,
            slots,
        }
    }
}

/// The statements whose text is the same for every ontology, numbered by their slots.
macro_rules! fixed {
    ($($name:ident => $text:expr,)*) => {
        /// A statement whose text is the same for every ontology.
        #[derive(Debug, Clone, Copy)]
        pub(crate) enum Sql {
            $($name,)*
        }

        impl Sql {
            pub(crate) const COUNT: usize = [$(Sql::$name),*].len();

            pub(crate) fn text(self) -> &'static str {
                match self {
                    $(Sql::$name => $text,)*
                }
            }
        }
    };
}

impl From<Sql> for Stmt<'static> {
    fn from(sql: Sql) -> Self {
        Stmt {
            slot: sql as usize,
            text: sql.text(),
        }
    }
}

fixed! {
    Held => HELD,
    Hear => HEAR,
    SeenKept => seen_kept(),
    SeenOther => SEEN_OTHER,
    SeenStart => SEEN_START,
    SeenKeep => seen_keep(),
    Rename => RENAME,
    Alias => ALIAS,
    HearingOf => HEARING_OF,
    FirstHearing => built!(edge_hearing("ASC")),
    LastHearing => built!(edge_hearing("DESC")),
    FirstEnding => built!(edge_ending("ASC")),
    LastEnding => built!(edge_ending("DESC")),
    Conclude => CONCLUDE,
    Ending => ENDING,
    Correction => CORRECTION,
    EventByCloudevent => EVENT_BY_CLOUDEVENT,
    Wait => WAIT,
    Amendment => AMENDMENT,
    Renamed => RENAMED,
    MadeBy => MADE_BY,
    RenamesIn => RENAMES_IN,
    WasRenamed => WAS_RENAMED,
    AmendmentsOf => AMENDMENTS_OF,
    PlacesNaming => PLACES_NAMING,
    RenameEndings => RENAME_ENDINGS,
    RenameEnds => RENAME_ENDS,
    RenameCorrections => RENAME_CORRECTIONS,
    EventAt => EVENT_AT,
    AmendedOf => AMENDED_OF,
    DropAmended => DROP_AMENDED,
    KnownAgain => KNOWN_AGAIN,
    KnownUntil => KNOWN_UNTIL,
    HearAmended => HEAR_AMENDED,
    FactsKnown => FACTS_KNOWN,
    Unseen => UNSEEN,
    SeenRecount => seen_recount(),
    SeenPut => SEEN_PUT,
    NewestVersion => newest_version(),
    NewestInView => newest_in_view(),
    AsOf => as_of(),
    KeepNewest => KEEP_NEWEST,
    TriedWork => TRIED_WORK,
    DoneRetry => DONE_RETRY,
    Waited => WAITED,
    AnyRename => ANY_RENAME,
    WaitingFor => WAITING_FOR,
    StopWaiting => STOP_WAITING,
    CloseOpen => CLOSE_OPEN,
    OpenLink => OPEN_LINK,
    WaitingOnTarget => built!(waiting("to_key")),
    WaitingOnSource => built!(waiting("from_key")),
    AppendEvent => APPEND_EVENT,
    Place => PLACE,
    AnyRule => ANY_RULE,
    StandRule => STAND_RULE,
    RuleOf => RULE_OF,
}
