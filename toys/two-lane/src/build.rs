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

/// The object object `i` depends on, the same at every version: a link.
#[must_use]
pub fn depends_on(i: usize) -> String {
    key((i * 7919 + 13) % 200_000)
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
    json!({"kind": "item", "version": v, "depends_on": depends_on(i), "fields": fields})
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

    /// The time and id of object `i`'s event in build `b`, or in the fix pass.
    #[must_use]
    pub fn id_of(&self, b: i64, i: usize, fix: bool) -> (i64, String) {
        let (time, seed) = if fix { (self.fixed_at(i), "fix".to_string()) } else { (self.base + b * DAY + i as i64, format!("build {b}")) };
        let v = self.version_at(i, b, time);
        (time, mint(time, format!("{seed}\n{i}\n{v}").as_bytes()))
    }

    /// One arrival as its CloudEvent line: object `i` as build `b` sends it, or as the fix pass
    /// rewrites it.
    #[must_use]
    pub fn line(&self, b: i64, i: usize, fix: bool) -> String {
        let (time, id) = self.id_of(b, i, fix);
        let v = self.version_at(i, b, time);
        let e = Event {
            id,
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

/// What one added event is.
#[derive(Debug, Clone)]
pub enum Extra {
    /// An amendment of object `i`'s event in build `b`, changing one property to `value`.
    Property { b: i64, i: usize, value: String },
    /// An amendment of object `i`'s event in build `b`, changing its subject to `new`.
    Rename { b: i64, i: usize, new: String },
    /// Object `i`'s link to what it depends on was wrong.
    Corrected { i: usize },
    /// Object `i`'s link to what it depends on stopped holding.
    Ended { i: usize },
    /// Object `i`'s event in build `b` again, the same content under a new id.
    Alias { b: i64, i: usize },
}

/// The added events: each with its arrival slot (after the base arrival at that index), its line,
/// its id and its known-at.
#[derive(Debug, Default)]
pub struct Extras {
    pub items: Vec<(usize, Extra, String, String, i64)>,
}

/// The same object's event, patched: the amendment's data is a JSON Merge Patch of the event.
fn amendment(id: &str, time: i64, target: &str, patch: &Value) -> String {
    json!({"specversion": "1.0", "id": id, "source": "build/fixer", "type": "rannoch.amended", "subject": target,
           "time": crate::entrance::format_time(time), "data": patch})
    .to_string()
}

impl Plan {
    /// The seeded set of amendments, corrections, endings and aliases, spread across the builds:
    /// `props`, `renames`, `corrected`, `ended` and `aliases` of each, on distinct objects.
    #[must_use]
    pub fn extras(&self, props: usize, renames: usize, corrected: usize, ended: usize, aliases: usize) -> Extras {
        let mut rng = Rng::new(0xA3E7_D5);
        let fixed: std::collections::BTreeSet<usize> = self.fixed.iter().copied().collect();
        let mut used = std::collections::BTreeSet::new();
        let per_build = self.objects + 0;
        let mut out = Extras::default();
        // Base arrivals: build b's objects start at (b - 1) * objects, plus the fix pass after 4.
        let start_of = |b: i64| usize::try_from(b - 1).unwrap_or(0) * per_build + if b > FIX_BUILD { self.fixed.len() } else { 0 };
        let slot_of = |b: i64, i: usize, rng: &mut Rng| -> usize {
            let start = start_of(b);
            let at = start + i + 1 + usize::try_from(rng.below(3000)).unwrap_or(0);
            at.min(start + per_build)
        };
        let pick = |b: i64, rng: &mut Rng, used: &mut std::collections::BTreeSet<usize>| -> usize {
            let pool = &self.changed[usize::try_from(b - 1).unwrap_or(0)];
            loop {
                let i = pool[usize::try_from(rng.below(pool.len() as u64)).unwrap_or(0)];
                if !fixed.contains(&i) && used.insert(i) {
                    return i;
                }
            }
        };
        let mut n = 0usize;
        for (count, what) in [(props, 0), (renames, 1), (corrected, 2), (ended, 3), (aliases, 4)] {
            for _ in 0..count {
                n += 1;
                let b = 1 + i64::try_from(rng.below(BUILDS as u64)).unwrap_or(0);
                let i = pick(b, &mut rng, &mut used);
                let slot = slot_of(b, i, &mut rng);
                let (target_time, target) = self.id_of(b, i, false);
                // It arrives just after the base arrival before its slot, at that arrival's time
                // plus one millisecond: after the event it names, and in time order.
                let time = self.base + b * DAY + i64::try_from(slot - start_of(b)).unwrap_or(0);
                let id = mint(time, format!("extra\n{n}").as_bytes());
                let (extra, line) = match what {
                    0 => {
                        let value = format!("amended-{n}");
                        let patch = json!({"data": {"body": {"fields": {"f05": value}}}});
                        (Extra::Property { b, i, value }, amendment(&id, time, &target, &patch))
                    }
                    1 => {
                        let new = format!("obj-renamed-{n:05}");
                        let patch = json!({"subject": new, "data": {"id": new}});
                        (Extra::Rename { b, i, new }, amendment(&id, time, &target, &patch))
                    }
                    2 | 3 => {
                        let ty = if what == 2 { "rannoch.corrected.depends_on" } else { "rannoch.ended.depends_on" };
                        let line = json!({"specversion": "1.0", "id": id, "source": "build/fixer", "type": ty,
                            "time": crate::entrance::format_time(time),
                            "data": {"from": {"class": "build_object", "key": key(i)}, "to": {"class": "build_object", "key": depends_on(i)}}})
                        .to_string();
                        (if what == 2 { Extra::Corrected { i } } else { Extra::Ended { i } }, line)
                    }
                    _ => {
                        let original: Value = serde_json::from_str(&self.line(b, i, false)).unwrap_or(Value::Null);
                        let mut alias = original;
                        alias["id"] = Value::String(id.clone());
                        (Extra::Alias { b, i }, alias.to_string())
                    }
                };
                let known = if what == 4 { target_time } else { time };
                out.items.push((slot, extra, line, id, known));
            }
        }
        out.items.sort_by_key(|x| x.0);
        out
    }
}
