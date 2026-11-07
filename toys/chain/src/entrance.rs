//! The one entrance. An event's `CloudEvents` attributes are read here, once, and from here on it
//! travels as type, source, subject and body: no handler reads its JSON again.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;
use serde_json::Value;

/// How many times an event's attributes have been read: once per event through [`admit`].
/// Counts every attribute read.
pub static READS: AtomicU64 = AtomicU64::new(0);

/// An event as it travels past the entrance.
#[derive(Debug, Clone, PartialEq, serde::Serialize, Deserialize)]
pub struct Event {
    /// Its `CloudEvent` id: a `UUIDv7` whose millisecond is its event time.
    pub id: String,
    pub ty: String,
    pub source: String,
    pub subject: Option<String>,
    /// Event time, in milliseconds since the Unix epoch: inside the event, never the clock's.
    pub time: i64,
    pub data: Value,
    /// The id of the event that caused it, as the `causation` extension attribute.
    pub causation: Option<String>,
    /// Its place in the order the runtime released it, set on release.
    pub position: u64,
}

#[derive(Deserialize)]
struct Wire {
    specversion: String,
    id: String,
    source: String,
    #[serde(rename = "type")]
    ty: String,
    subject: Option<String>,
    time: String,
    #[serde(default)]
    data: Value,
    causation: Option<String>,
}

/// Reads one `CloudEvent` from its JSON, at `position` in its log. The one attribute read.
///
/// # Errors
/// The line is not a `CloudEvents` 1.0 event with a time.
pub fn admit(line: &str, position: u64) -> Result<Event, String> {
    READS.fetch_add(1, Ordering::Relaxed);
    let wire: Wire = serde_json::from_str(line).map_err(|e| e.to_string())?;
    if wire.specversion != "1.0" {
        return Err(format!("specversion {} is not 1.0", wire.specversion));
    }
    Ok(Event {
        time: parse_time(&wire.time)?,
        id: wire.id,
        ty: wire.ty,
        source: wire.source,
        subject: wire.subject,
        data: wire.data,
        causation: wire.causation,
        position,
    })
}

/// The event as a `CloudEvent`'s JSON.
#[must_use]
pub fn to_json(event: &Event) -> String {
    let mut object = serde_json::json!({
        "specversion": "1.0",
        "id": event.id,
        "source": event.source,
        "type": event.ty,
        "time": format_time(event.time),
        "data": event.data,
    });
    if let Some(subject) = &event.subject {
        object["subject"] = Value::String(subject.clone());
    }
    if let Some(causation) = &event.causation {
        object["causation"] = Value::String(causation.clone());
    }
    object.to_string()
}

/// A `UUIDv7` whose millisecond is `time` and whose random bits come from `seed`, so the same
/// event always gets the same id: its identity is its content, source and position.
#[must_use]
pub fn mint(time: i64, seed: &[u8]) -> String {
    let hash = blake3::hash(seed);
    let bytes = hash.as_bytes();
    let mut random = [0u8; 16];
    random.copy_from_slice(&bytes[..16]);
    let random = u128::from_be_bytes(random);
    let ms = u128::from(u64::try_from(time).unwrap_or(0)) & ((1 << 48) - 1);
    let rand_a = (random >> 64) & 0xfff;
    let rand_b = random & ((1 << 62) - 1);
    let value = (ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b;
    uuid::Uuid::from_u128(value).to_string()
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
#[must_use]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Milliseconds of `YYYY-MM-DD` at 09:00 UTC, the hour Westwind's generator stamps every row.
///
/// # Errors
/// The text is not a date.
pub fn date_ms(date: &str) -> Result<i64, String> {
    let part = |range: std::ops::Range<usize>| -> Result<i64, String> {
        date.get(range)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| format!("{date} is not YYYY-MM-DD"))
    };
    let days = days_from_civil(part(0..4)?, part(5..7)?, part(8..10)?);
    Ok(days * 86_400_000 + 9 * 3_600_000)
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ`.
#[must_use]
pub fn format_time(ms: i64) -> String {
    let (year, month, day) = civil_from_days(ms.div_euclid(86_400_000));
    let rest = ms.rem_euclid(86_400_000);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3_600_000,
        rest / 60_000 % 60,
        rest / 1000 % 60,
        rest % 1000
    )
}

fn parse_time(text: &str) -> Result<i64, String> {
    let bad = || format!("time {text} is not YYYY-MM-DDTHH:MM:SS.mmmZ");
    let num = |range: std::ops::Range<usize>| -> Result<i64, String> {
        text.get(range).and_then(|s| s.parse().ok()).ok_or_else(bad)
    };
    if text.len() != 24 {
        return Err(bad());
    }
    let days = days_from_civil(num(0..4)?, num(5..7)?, num(8..10)?);
    Ok(days * 86_400_000 + num(11..13)? * 3_600_000 + num(14..16)? * 60_000 + num(17..19)? * 1000 + num(20..23)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_round_trips_and_counts_one_read() {
        let event = Event {
            id: mint(1_736_154_000_000, b"x"),
            ty: "westwind.order.placed".into(),
            source: "westwind/seed".into(),
            subject: Some("o1".into()),
            time: 1_736_154_000_000,
            data: serde_json::json!({"id": "o1"}),
            causation: Some("c".into()),
            position: 4,
        };
        let before = READS.load(Ordering::Relaxed);
        let back = admit(&to_json(&event), 4).unwrap();
        assert_eq!(back, event);
        assert!(READS.load(Ordering::Relaxed) > before);
    }

    #[test]
    fn a_minted_id_carries_its_time_and_is_the_same_every_time() {
        let id = mint(1_736_154_000_000, b"seed");
        assert_eq!(id, mint(1_736_154_000_000, b"seed"));
        let uuid = uuid::Uuid::parse_str(&id).unwrap();
        assert_eq!(uuid.get_version_num(), 7);
        let (secs, _) = uuid.get_timestamp().unwrap().to_unix();
        assert_eq!(secs, 1_736_154_000);
    }

    #[test]
    fn dates_read_at_nine() {
        assert_eq!(format_time(date_ms("2025-01-06").unwrap()), "2025-01-06T09:00:00.000Z");
    }
}
