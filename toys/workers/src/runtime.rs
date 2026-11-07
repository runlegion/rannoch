//! The router, a supervisor per handler, and the loop that joins them.
//!
//! The router is a table from type pattern to handlers, built from what each handler registers.
//! Each handler runs under its own supervisor, one for one: a crash restarts that handler alone,
//! from its durable place, and the events it had been handed since are handed again (at least
//! once). An event that keeps crashing it goes to that handler's bin, and the rest carry on.
//! Everything runs on one thread, in log order; an event a handler emits is routed before the
//! next event from the log.

use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use crate::entrance::Event;

/// One handler of routed events.
pub trait Handler {
    /// Handles one event; what it emits goes in `out`.
    ///
    /// # Errors
    /// The event could not be handled: the supervisor treats it as a crash.
    fn handle(&mut self, event: &Event, out: &mut Vec<Event>) -> Result<(), String>;

    /// Makes what it has done durable and returns its place: the last log position it is done
    /// with for good, or `None` while it has none.
    ///
    /// # Errors
    /// What it has done cannot be made durable.
    fn checkpoint(&mut self) -> Result<Option<u64>, String>;

    /// Its durable place as it started.
    fn place(&self) -> Option<u64>;

    /// Its state as text, for a handler whose state is worth comparing.
    fn snapshot(&self) -> Option<String> {
        None
    }
}

/// Starts a handler from its durable place.
pub type Start = Box<dyn FnMut() -> Result<Box<dyn Handler>, String>>;

/// A table from type pattern to handlers: a pattern is a type, or a prefix ending in `*`.
#[derive(Debug, Default)]
pub struct Router {
    exact: HashMap<String, Vec<usize>>,
    prefixes: Vec<(String, usize)>,
    resolved: HashMap<String, Vec<usize>>,
}

impl Router {
    pub fn register(&mut self, pattern: &str, handler: usize) {
        if let Some(prefix) = pattern.strip_suffix('*') {
            self.prefixes.push((prefix.to_string(), handler));
        } else {
            self.exact.entry(pattern.to_string()).or_default().push(handler);
        }
        self.resolved.clear();
    }

    /// The handlers an event of type `ty` goes to, in the order they registered.
    pub fn route(&mut self, ty: &str) -> &[usize] {
        if !self.resolved.contains_key(ty) {
            let mut handlers: Vec<usize> = self.exact.get(ty).cloned().unwrap_or_default();
            for (prefix, handler) in &self.prefixes {
                if ty.starts_with(prefix.as_str()) && !handlers.contains(handler) {
                    handlers.push(*handler);
                }
            }
            handlers.sort_unstable();
            self.resolved.insert(ty.to_string(), handlers);
        }
        self.resolved.get(ty).map_or(&[], Vec::as_slice)
    }
}

/// One handler's supervisor.
pub struct Supervisor {
    pub name: String,
    start: Start,
    handler: Box<dyn Handler>,
    place: Option<u64>,
    /// Handed since the durable place, so a restart can hand them again.
    pending: VecDeque<Rc<Event>>,
    /// Crashes per event, by its id and position.
    crashes: HashMap<(String, u64), u32>,
    binned: HashSet<(String, u64)>,
    limit: u32,
    /// Events that kept crashing the handler, each with its last reason.
    pub bin: Vec<(Rc<Event>, String)>,
    pub restarts: u64,
    pub handed: u64,
}

impl std::fmt::Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Supervisor {
    /// Starts the handler, which crashes on an event up to `limit` times before it is binned.
    ///
    /// # Errors
    /// The handler cannot start.
    pub fn new(name: &str, mut start: Start, limit: u32) -> Result<Supervisor, String> {
        let handler = start()?;
        let place = handler.place();
        Ok(Supervisor {
            name: name.to_string(),
            start,
            handler,
            place,
            pending: VecDeque::new(),
            crashes: HashMap::new(),
            binned: HashSet::new(),
            limit,
            bin: Vec::new(),
            restarts: 0,
            handed: 0,
        })
    }

    /// Hands `event` to the handler, restarting it as often as it crashes.
    ///
    /// # Errors
    /// The handler cannot be restarted.
    pub fn deliver(&mut self, event: Rc<Event>, out: &mut Vec<Event>) -> Result<(), String> {
        if self.place.is_some_and(|place| event.position <= place) {
            return Ok(());
        }
        self.pending.push_back(event);
        let mut index = self.pending.len() - 1;
        while index < self.pending.len() {
            let event = Rc::clone(&self.pending[index]);
            self.handed += 1;
            let handler = &mut self.handler;
            let outcome = catch_unwind(AssertUnwindSafe(|| handler.handle(&event, out)));
            let reason = match outcome {
                Ok(Ok(())) => {
                    index += 1;
                    continue;
                }
                Ok(Err(reason)) => reason,
                Err(panic) => panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
                    .unwrap_or_else(|| "panicked".to_string()),
            };
            let at = (event.id.clone(), event.position);
            let crashes = self.crashes.entry(at.clone()).or_insert(0);
            *crashes += 1;
            if *crashes >= self.limit {
                self.binned.insert(at);
                self.bin.push((Rc::clone(&event), reason));
            }
            self.restart()?;
            index = 0;
        }
        Ok(())
    }

    fn restart(&mut self) -> Result<(), String> {
        self.restarts += 1;
        self.handler = (self.start)()?;
        self.place = self.handler.place();
        let place = self.place;
        let binned = &self.binned;
        self.pending.retain(|event| {
            place.is_none_or(|place| event.position > place)
                && !binned.contains(&(event.id.clone(), event.position))
        });
        Ok(())
    }

    /// The handler's state as text, when it has one to compare.
    #[must_use]
    pub fn snapshot(&self) -> Option<String> {
        self.handler.snapshot()
    }

    /// Checkpoints the handler and forgets what it no longer needs to hand again.
    ///
    /// # Errors
    /// The checkpoint failed; nothing is forgotten.
    pub fn checkpoint(&mut self) -> Result<(), String> {
        let handler = &mut self.handler;
        match catch_unwind(AssertUnwindSafe(|| handler.checkpoint())) {
            Ok(Ok(place)) => {
                if place.is_some() {
                    self.place = place;
                }
                let place = self.place;
                self.pending
                    .retain(|event| place.is_none_or(|place| event.position > place));
                Ok(())
            }
            Ok(Err(reason)) => Err(reason),
            Err(_) => {
                self.restart()?;
                Ok(())
            }
        }
    }
}

/// The router and its supervised handlers.
#[derive(Debug, Default)]
pub struct Runtime {
    pub router: Router,
    pub handlers: Vec<Supervisor>,
    queue: VecDeque<Rc<Event>>,
    pub routed: u64,
}

impl Runtime {
    /// Adds a supervised handler for the type patterns it registers.
    ///
    /// # Errors
    /// The handler cannot start.
    pub fn add(
        &mut self,
        name: &str,
        patterns: &[String],
        start: Start,
        limit: u32,
    ) -> Result<usize, String> {
        let index = self.handlers.len();
        self.handlers.push(Supervisor::new(name, start, limit)?);
        for pattern in patterns {
            self.router.register(pattern, index);
        }
        Ok(index)
    }

    /// Routes `event` and everything it causes to be emitted.
    ///
    /// # Errors
    /// A handler cannot be restarted.
    pub fn push(&mut self, event: Event) -> Result<(), String> {
        self.queue.push_back(Rc::new(event));
        let mut out = Vec::new();
        while let Some(event) = self.queue.pop_front() {
            self.routed += 1;
            for &handler in self.router.route(&event.ty) {
                self.handlers[handler].deliver(Rc::clone(&event), &mut out)?;
            }
            for mut emitted in out.drain(..) {
                emitted.position = event.position;
                self.queue.push_back(Rc::new(emitted));
            }
        }
        Ok(())
    }

    /// Checkpoints every handler.
    ///
    /// # Errors
    /// A handler's checkpoint failed.
    pub fn checkpoint(&mut self) -> Result<(), String> {
        for handler in &mut self.handlers {
            handler.checkpoint()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    fn event(ty: &str, position: u64) -> Event {
        Event {
            id: format!("e{position}"),
            ty: ty.into(),
            source: "test".into(),
            subject: None,
            time: 0,
            data: serde_json::Value::Null,
            position,
        }
    }

    /// Records what it handles; crashes on each position in `crash_on` every time.
    struct Flaky {
        seen: Rc<RefCell<Vec<u64>>>,
        crash_on: Vec<u64>,
        durable: Rc<RefCell<Option<u64>>>,
        last: Option<u64>,
    }

    impl Handler for Flaky {
        fn handle(&mut self, event: &Event, _: &mut Vec<Event>) -> Result<(), String> {
            assert!(!self.crash_on.contains(&event.position), "crash at {}", event.position);
            self.seen.borrow_mut().push(event.position);
            self.last = Some(event.position);
            Ok(())
        }
        fn checkpoint(&mut self) -> Result<Option<u64>, String> {
            *self.durable.borrow_mut() = self.last;
            Ok(self.last)
        }
        fn place(&self) -> Option<u64> {
            *self.durable.borrow()
        }
    }

    fn flaky(crash_on: Vec<u64>) -> (Start, Rc<RefCell<Vec<u64>>>) {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let durable = Rc::new(RefCell::new(None));
        let shared = Rc::clone(&seen);
        let start: Start = Box::new(move || {
            Ok(Box::new(Flaky {
                seen: Rc::clone(&shared),
                crash_on: crash_on.clone(),
                durable: Rc::clone(&durable),
                last: *durable.borrow(),
            }) as Box<dyn Handler>)
        });
        (start, seen)
    }

    #[test]
    fn a_type_goes_to_exactly_the_handlers_that_registered_it() {
        let mut router = Router::default();
        router.register("westwind.order.placed", 0);
        router.register("westwind.order.*", 1);
        router.register("westwind.order_line.added", 2);
        assert_eq!(router.route("westwind.order.placed"), &[0, 1]);
        assert_eq!(router.route("westwind.order.shipped"), &[1]);
        assert_eq!(router.route("westwind.order_line.added"), &[2]);
        assert!(router.route("westwind.customer.joined").is_empty());
    }

    #[test]
    fn an_event_that_keeps_crashing_one_handler_is_binned_and_the_rest_carry_on() {
        std::panic::set_hook(Box::new(|_| {}));
        let mut runtime = Runtime::default();
        let (bad, bad_seen) = flaky(vec![3]);
        let (good, good_seen) = flaky(vec![]);
        runtime.add("bad", &["t".into()], bad, 3).unwrap();
        runtime.add("good", &["t".into()], good, 3).unwrap();
        for position in 1..=5 {
            runtime.push(event("t", position)).unwrap();
            if position == 2 {
                runtime.checkpoint().unwrap();
            }
        }
        let _ = std::panic::take_hook();
        assert_eq!(*good_seen.borrow(), vec![1, 2, 3, 4, 5]);
        let bad_seen = bad_seen.borrow();
        assert!(!bad_seen.contains(&3));
        assert!(bad_seen.contains(&4) && bad_seen.contains(&5));
        let bad = &runtime.handlers[0];
        assert_eq!(bad.bin.len(), 1);
        assert_eq!(bad.bin[0].0.position, 3);
        assert_eq!(bad.restarts, 3);
        assert_eq!(runtime.handlers[1].restarts, 0);
    }

    #[test]
    fn a_restart_hands_again_what_came_after_the_durable_place() {
        std::panic::set_hook(Box::new(|_| {}));
        let mut runtime = Runtime::default();
        let (bad, seen) = flaky(vec![4]);
        runtime.add("bad", &["t".into()], bad, 1).unwrap();
        for position in 1..=4 {
            runtime.push(event("t", position)).unwrap();
            if position == 2 {
                runtime.checkpoint().unwrap();
            }
        }
        let _ = std::panic::take_hook();
        // 3 was handled, then 4 crashed: the restart from place 2 hands 3 again, at least once.
        assert_eq!(*seen.borrow(), vec![1, 2, 3, 3]);
    }
}
