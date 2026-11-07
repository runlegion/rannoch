//! A build stream shaped like kessel's, from a fixed seed: objects of about 3 KB of JSON, in six
//! builds. Build 1 writes every object; each later build sends every object again, 2 to 5% of
//! them changed. Build 4 is followed by a fix pass that rewrites 1% of the objects at build 4.
//! The build is the declared axis. No kessel data is used.

use serde_json::{Map, Value, json};

use crate::entrance::{Event, mint, to_json};
use crate::rows::Rng;
use crate::world::{DAY, HOUR};

pub const BUILDS: i64 = 6;
pub const FIX_BUILD: i64 = 4;

/// The stream's plan: which version of each object each build sends.
#[derive(Debug)]
pub struct Plan {
    pub objects: usize,
    pub base: i64,
    /// For each object, its versions: (build, version number, known-at), in order.
    pub history: Vec<Vec<(i64, u32, i64)>>,
    /// For each build, the objects that changed in it; for the fix pass, those it rewrote.
    pub changed: Vec<Vec<usize>>,
    pub fixed: Vec<usize>,
}

/// An object's key.
#[must_use]
pub fn key(i: usize) -> String {
    format!("obj-{i:06}")
}

/// Object `i` at version `v`: about 3 KB of JSON, the same every time.
#[must_use]
pub fn body(i: usize, v: u32) -> Value {
    let mut rng = Rng::new(0x4B45_5353 ^ (i as u64) << 20 ^ u64::from(v));
    let mut fields = Map::new();
    for f in 0..28 {
        let text: String = (0..96).map(|_| char::from(b'a' + u8::try_from(rng.below(26)).unwrap_or(0))).collect();
        fields.insert(format!("f{f:02}"), Value::String(text));
    }
    json!({"kind": "item", "version": v, "fields": fields})
}

impl Plan {
    #[must_use]
    pub fn new(objects: usize) -> Plan {
        let base = crate::entrance::date_ms("2026-01-05").unwrap_or(0) - 9 * HOUR;
        let mut rng = Rng::new(0x0B11_D5EE_D);
        let mut version = vec![0u32; objects];
        let mut history: Vec<Vec<(i64, u32, i64)>> = (0..objects).map(|i| vec![(1, 0, base + DAY + i as i64)]).collect();
        let mut changed = vec![(0..objects).collect::<Vec<_>>()];
        let mut fixed = Vec::new();
        for b in 2..=BUILDS {
            let rate = 2 + rng.below(4);
            let count = objects * usize::try_from(rate).unwrap_or(2) / 100;
            let mut picked = std::collections::BTreeSet::new();
            while picked.len() < count {
                picked.insert(usize::try_from(rng.below(objects as u64)).unwrap_or(0));
            }
            for &i in &picked {
                version[i] += 1;
                history[i].push((b, version[i], base + b * DAY + i as i64));
            }
            changed.push(picked.into_iter().collect());
            if b == FIX_BUILD {
                let mut picked = std::collections::BTreeSet::new();
                while picked.len() < objects / 100 {
                    picked.insert(usize::try_from(rng.below(objects as u64)).unwrap_or(0));
                }
                for &i in &picked {
                    version[i] += 1;
                    history[i].push((b, version[i], Self::fix_time(base, i)));
                }
                fixed = picked.into_iter().collect();
            }
        }
        Plan { objects, base, history, changed, fixed }
    }

    fn fix_time(base: i64, i: usize) -> i64 {
        base + FIX_BUILD * DAY + 12 * HOUR + i as i64
    }

    /// The fix pass's known-at for object `i`.
    #[must_use]
    pub fn fixed_at(&self, i: usize) -> i64 {
        Self::fix_time(self.base, i)
    }

    /// The version object `i` has at build `b`, as known at `known`.
    #[must_use]
    pub fn version_at(&self, i: usize, b: i64, known: i64) -> u32 {
        self.history[i].iter().filter(|(hb, _, k)| *hb <= b && *k <= known).map(|(_, v, _)| *v).last().unwrap_or(0)
    }

    /// Every arrival of the stream, in order: each build's objects, the fix pass after build 4.
    #[must_use]
    pub fn order(&self) -> Vec<(i64, usize, bool)> {
        let mut out = Vec::new();
        for b in 1..=BUILDS {
            out.extend((0..self.objects).map(|i| (b, i, false)));
            if b == FIX_BUILD {
                out.extend(self.fixed.iter().map(|&i| (b, i, true)));
            }
        }
        out
    }

    /// One arrival as its CloudEvent line: object `i` as build `b` sends it, or as the fix pass
    /// rewrites it.
    #[must_use]
    pub fn line(&self, b: i64, i: usize, fix: bool) -> String {
        let (time, seed) = if fix { (self.fixed_at(i), "fix".to_string()) } else { (self.base + b * DAY + i as i64, format!("build {b}")) };
        let v = self.version_at(i, b, time);
        let e = Event {
            id: mint(time, format!("{seed}\n{i}\n{v}").as_bytes()),
            ty: "build.object.written".into(),
            source: "build/kessel".into(),
            subject: Some(key(i)),
            time,
            data: json!({"id": key(i), "build": b, "body": body(i, v)}),
            causation: None,
            position: 0,
        };
        to_json(&e)
    }

    /// The JSON bytes of the objects that changed in build `b` (with the fix pass for build 4).
    #[must_use]
    pub fn changed_bytes(&self, b: i64) -> usize {
        let mut total: usize = self.changed[usize::try_from(b - 1).unwrap_or(0)]
            .iter()
            .map(|&i| body(i, self.version_at(i, b, self.base + b * DAY + i as i64)).to_string().len())
            .sum();
        if b == FIX_BUILD {
            total += self.fixed.iter().map(|&i| body(i, self.version_at(i, b, i64::MAX)).to_string().len()).sum::<usize>();
        }
        total
    }
}
