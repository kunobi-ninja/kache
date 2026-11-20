//! One walk of a tree for every caller that asks while it is still to start.
//!
//! The tree guard stamps a whole workspace for each unit, and a daemon runs
//! many units of one workspace at once, so dozens of threads would list and
//! stat the same directories side by side. A caller here never takes a walk
//! that was already under way when it asked: what it gets started after it
//! arrived, so it saw every change made before the caller asked, as its own
//! walk would have. Callers that arrive while a walk runs share the next
//! one, so a root has at most one walk running and one waiting.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Condvar, Mutex, PoisonError};

/// Walks keyed by what decides their result.
pub(crate) struct NextFlight<K, V> {
    slots: Mutex<HashMap<K, Arc<Slot<V>>>>,
}

struct Slot<V> {
    state: Mutex<SlotState<V>>,
    finished: Condvar,
    /// Callers holding this slot, counted under the map's lock: the last one
    /// out removes it.
    callers: Mutex<usize>,
}

struct SlotState<V> {
    /// Generation of the newest walk started; walks count from 1.
    started: u64,
    /// Generation whose result `result` holds.
    finished: u64,
    running: bool,
    result: Option<V>,
    /// Callers parked on `finished`, so tests can wait until they are.
    #[cfg(test)]
    parked: usize,
}

impl<K: Eq + Hash + Clone, V: Clone> NextFlight<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// The result of a `walk` for `key` that started after this call did:
    /// this caller's own, or one it shares with callers that asked while the
    /// previous walk ran.
    pub(crate) fn run(&self, key: &K, walk: impl FnOnce() -> V) -> V {
        let caller = Caller {
            flights: self,
            key,
            slot: self.enter(key),
        };
        Self::await_or_walk(&caller.slot, walk)
    }

    fn enter(&self, key: &K) -> Arc<Slot<V>> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = Arc::clone(slots.entry(key.clone()).or_insert_with(|| {
            Arc::new(Slot {
                state: Mutex::new(SlotState {
                    started: 0,
                    finished: 0,
                    running: false,
                    result: None,
                    #[cfg(test)]
                    parked: 0,
                }),
                finished: Condvar::new(),
                callers: Mutex::new(0),
            })
        }));
        *slot.callers.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        slot
    }

    fn leave(&self, key: &K, slot: &Arc<Slot<V>>) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let mut callers = slot.callers.lock().unwrap_or_else(PoisonError::into_inner);
        *callers -= 1;
        if *callers == 0 {
            slots.remove(key);
        }
    }

    fn await_or_walk(slot: &Slot<V>, walk: impl FnOnce() -> V) -> V {
        let mut state = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
        // A walk running now started before this call; only a later one will do.
        let wanted = state.started + 1;
        loop {
            if state.finished >= wanted
                && let Some(result) = &state.result
            {
                return result.clone();
            }
            if !state.running {
                state.started += 1;
                state.running = true;
                let generation = state.started;
                drop(state);
                let mut walking = Walking { slot, done: false };
                let result = walk();
                let mut state = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.result = Some(result.clone());
                state.finished = generation;
                state.running = false;
                walking.done = true;
                drop(state);
                slot.finished.notify_all();
                return result;
            }
            // Only a running walk ends a wait: with none, nothing would wake us.
            debug_assert!(state.running, "waiting with no walk running");
            #[cfg(test)]
            park(&mut state);
            state = slot
                .finished
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
            #[cfg(test)]
            unpark(&mut state);
        }
    }
}

/// One run of a computation for the callers that ask while it runs: a
/// caller arriving mid-run waits for that run, and takes its result when the
/// result is one that may be shared. Only for a key that fixes such a
/// result, as a tree's settled stamp fixes a digest the memo recorded;
/// [`NextFlight`] is for a walk whose answer depends on when it started.
/// Nothing is kept once the last caller leaves, and a run that unwinds hands
/// the work to a waiter.
pub(crate) struct JoinFlight<K, V> {
    slots: Mutex<HashMap<K, Arc<JoinSlot<V>>>>,
}

struct JoinSlot<V> {
    state: Mutex<JoinState<V>>,
    finished: Condvar,
}

enum JoinState<V> {
    Running,
    Done(V),
    Abandoned,
}

impl<K: Eq + Hash + Clone, V: Clone> JoinFlight<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// `compute()` for `key`, or the result of a run of it that was under
    /// way when this call arrived, when `shared` says a caller that did not
    /// make that result may take it. A caller whose leader's result may not
    /// be taken computes its own, alone.
    pub(crate) fn run(
        &self,
        key: &K,
        compute: impl FnOnce() -> V,
        shared: impl Fn(&V) -> bool,
    ) -> V {
        loop {
            let (slot, leader) = {
                let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
                match slots.get(key) {
                    Some(slot) => (Arc::clone(slot), false),
                    None => {
                        let slot = Arc::new(JoinSlot {
                            state: Mutex::new(JoinState::Running),
                            finished: Condvar::new(),
                        });
                        slots.insert(key.clone(), Arc::clone(&slot));
                        (slot, true)
                    }
                }
            };
            if leader {
                let mut running = Leading {
                    flights: self,
                    key,
                    slot: &slot,
                    done: false,
                };
                let result = compute();
                *slot.state.lock().unwrap_or_else(PoisonError::into_inner) =
                    JoinState::Done(result.clone());
                running.done = true;
                return result;
            }
            let mut state = slot.state.lock().unwrap_or_else(PoisonError::into_inner);
            while matches!(*state, JoinState::Running) {
                state = slot
                    .finished
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            let finished = match &*state {
                JoinState::Done(result) => Some(result.clone()),
                JoinState::Abandoned | JoinState::Running => None,
            };
            // `shared` is the caller's code: not under the slot's lock.
            drop(state);
            match finished {
                Some(result) if shared(&result) => return result,
                Some(_) => break,
                // The leader unwound: run again, as a leader if nobody else is.
                None => {}
            }
        }
        compute()
    }

    /// Callers in the run for `key`: the one computing and those waiting.
    #[cfg(test)]
    pub(crate) fn callers(&self, key: &K) -> usize {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .map_or(0, |slot| Arc::strong_count(slot) - 1)
    }
}

/// Publishes the leader's outcome, abandoned if it unwinds, and removes the
/// slot so the next caller after the run computes afresh.
struct Leading<'a, K: Eq + Hash + Clone, V> {
    flights: &'a JoinFlight<K, V>,
    key: &'a K,
    slot: &'a Arc<JoinSlot<V>>,
    done: bool,
}

impl<K: Eq + Hash + Clone, V> Drop for Leading<'_, K, V> {
    fn drop(&mut self) {
        let mut slots = self
            .flights
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slots
            .get(self.key)
            .is_some_and(|slot| Arc::ptr_eq(slot, self.slot))
        {
            slots.remove(self.key);
        }
        drop(slots);
        if !self.done {
            *self
                .slot
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = JoinState::Abandoned;
        }
        self.slot.finished.notify_all();
    }
}

#[cfg(test)]
fn park<V>(state: &mut SlotState<V>) {
    state.parked += 1;
}

#[cfg(test)]
fn unpark<V>(state: &mut SlotState<V>) {
    state.parked -= 1;
}

/// Leaves the slot however the call ends.
struct Caller<'a, K: Eq + Hash + Clone, V: Clone> {
    flights: &'a NextFlight<K, V>,
    key: &'a K,
    slot: Arc<Slot<V>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Drop for Caller<'_, K, V> {
    fn drop(&mut self) {
        self.flights.leave(self.key, &self.slot);
    }
}

/// Clears `running` if a walk unwinds, so a waiter walks in its place.
struct Walking<'a, V> {
    slot: &'a Slot<V>,
    done: bool,
}

impl<V> Drop for Walking<'_, V> {
    fn drop(&mut self) {
        if !self.done {
            self.slot
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .running = false;
            self.slot.finished.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, mpsc};
    use std::time::Duration;

    fn wait_for_parked<V>(flights: &Arc<NextFlight<u32, V>>, key: &u32, parked: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let slot = Arc::clone(&flights.slots.lock().unwrap()[key]);
            if slot.state.lock().unwrap().parked == parked {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "callers never parked");
            std::thread::yield_now();
        }
    }

    #[test]
    fn a_lone_caller_walks_and_leaves_no_slot() {
        let flights = NextFlight::<u32, u32>::new();
        assert_eq!(flights.run(&7, || 41), 41);
        assert_eq!(flights.run(&7, || 42), 42, "a later caller walks again");
        assert!(flights.slots.lock().unwrap().is_empty());
    }

    #[test]
    fn different_keys_never_share_a_walk() {
        let flights = Arc::new(NextFlight::<u32, u32>::new());
        let (entered, wait_entered) = mpsc::channel();
        let (release, wait_release) = mpsc::channel::<()>();
        let first = {
            let flights = Arc::clone(&flights);
            std::thread::spawn(move || {
                flights.run(&1, || {
                    entered.send(()).unwrap();
                    wait_release.recv().unwrap();
                    1
                })
            })
        };
        wait_entered.recv().unwrap();
        // Key 2 walks at once although key 1 is mid-walk.
        assert_eq!(flights.run(&2, || 2), 2);
        release.send(()).unwrap();
        assert_eq!(first.join().unwrap(), 1);
    }

    /// Callers that ask while a walk runs never get that walk's result, and
    /// all of them share one walk after it.
    #[test]
    fn callers_during_a_walk_share_the_next_one() {
        const WAITERS: usize = 6;
        let flights = Arc::new(NextFlight::<u32, usize>::new());
        let walks = Arc::new(AtomicUsize::new(0));
        let (entered, wait_entered) = mpsc::channel();
        let (release, wait_release) = mpsc::channel::<()>();
        let first = {
            let (flights, walks) = (Arc::clone(&flights), Arc::clone(&walks));
            std::thread::spawn(move || {
                flights.run(&1, || {
                    entered.send(()).unwrap();
                    wait_release.recv().unwrap();
                    walks.fetch_add(1, Ordering::SeqCst)
                })
            })
        };
        wait_entered.recv().unwrap();
        let ready = Arc::new(Barrier::new(WAITERS + 1));
        let waiters: Vec<_> = (0..WAITERS)
            .map(|_| {
                let (flights, walks, ready) =
                    (Arc::clone(&flights), Arc::clone(&walks), Arc::clone(&ready));
                std::thread::spawn(move || {
                    ready.wait();
                    flights.run(&1, || walks.fetch_add(1, Ordering::SeqCst))
                })
            })
            .collect();
        ready.wait();
        // Let every waiter park on the first walk before it ends. Counting
        // callers is not enough: one counted but not yet parked could take a
        // later walk than the others.
        wait_for_parked(&flights, &1, WAITERS);
        release.send(()).unwrap();
        assert_eq!(first.join().unwrap(), 0, "the first walk");
        for waiter in waiters {
            assert_eq!(waiter.join().unwrap(), 1, "the walk after the first");
        }
        assert_eq!(walks.load(Ordering::SeqCst), 2);
        assert!(flights.slots.lock().unwrap().is_empty());
    }

    #[test]
    fn a_walk_that_panics_hands_the_walk_to_a_waiter() {
        let flights = Arc::new(NextFlight::<u32, u32>::new());
        let (entered, wait_entered) = mpsc::channel();
        let (release, wait_release) = mpsc::channel::<()>();
        let panicking = {
            let flights = Arc::clone(&flights);
            std::thread::spawn(move || {
                flights.run(&1, || {
                    entered.send(()).unwrap();
                    wait_release.recv().unwrap();
                    panic!("walk failed")
                })
            })
        };
        wait_entered.recv().unwrap();
        let (walked, wait_walked) = mpsc::channel();
        {
            let flights = Arc::clone(&flights);
            std::thread::spawn(move || walked.send(flights.run(&1, || 9)));
        }
        wait_for_parked(&flights, &1, 1);
        release.send(()).unwrap();
        assert!(panicking.join().is_err());
        // A timeout, not a join: a waiter left parked must fail the test, not
        // hang it.
        assert_eq!(
            wait_walked.recv_timeout(Duration::from_secs(10)),
            Ok(9),
            "the waiter walks once the panicking walk unwinds"
        );
        assert!(flights.slots.lock().unwrap().is_empty());
    }

    fn wait_for_callers(flights: &JoinFlight<u8, u32>, key: u8, callers: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while flights.callers(&key) != callers {
            assert!(std::time::Instant::now() < deadline, "callers never joined");
            std::thread::yield_now();
        }
    }

    /// A leader that computes `led` once released, and `followers` callers
    /// that arrive while it runs and would each compute 9. Gives the leader's
    /// result, the followers' results and how many computations ran.
    fn led_and_followed(
        flights: &Arc<JoinFlight<u8, u32>>,
        followers: usize,
        shared: fn(&u32) -> bool,
    ) -> (u32, Vec<u32>, usize) {
        let runs = Arc::new(AtomicUsize::new(0));
        let (started, wait_started) = mpsc::channel();
        let (release, wait_release) = mpsc::channel::<()>();
        let leader = {
            let (flights, runs) = (Arc::clone(flights), Arc::clone(&runs));
            std::thread::spawn(move || {
                flights.run(
                    &1,
                    || {
                        runs.fetch_add(1, Ordering::SeqCst);
                        started.send(()).unwrap();
                        wait_release.recv().unwrap();
                        7
                    },
                    shared,
                )
            })
        };
        wait_started.recv().unwrap();
        let (followed, wait_followed) = mpsc::channel();
        for _ in 0..followers {
            let (flights, runs, followed) =
                (Arc::clone(flights), Arc::clone(&runs), followed.clone());
            std::thread::spawn(move || {
                followed.send(flights.run(
                    &1,
                    || {
                        runs.fetch_add(1, Ordering::SeqCst);
                        9
                    },
                    shared,
                ))
            });
        }
        wait_for_callers(flights, 1, followers + 1);
        release.send(()).unwrap();
        let led = leader.join().unwrap();
        // A timeout, not a join: a caller left waiting must fail the test,
        // not hang it.
        let followed = (0..followers)
            .map(|_| {
                wait_followed
                    .recv_timeout(Duration::from_secs(10))
                    .expect("a caller was left waiting")
            })
            .collect();
        (led, followed, runs.load(Ordering::SeqCst))
    }

    #[test]
    fn callers_during_a_join_flight_share_its_one_run() {
        let flights = Arc::new(JoinFlight::<u8, u32>::new());
        let (led, followed, runs) = led_and_followed(&flights, 4, |_| true);
        assert_eq!((led, followed, runs), (7, vec![7; 4], 1));
        assert!(flights.slots.lock().unwrap().is_empty());
        assert_eq!(
            flights.run(&1, || 3, |_| true),
            3,
            "a later caller computes afresh"
        );
    }

    /// A result its leader may not share is the leader's alone: each caller
    /// that waited for it computes its own.
    #[test]
    fn callers_compute_their_own_when_the_run_may_not_be_shared() {
        let flights = Arc::new(JoinFlight::<u8, u32>::new());
        let (led, followed, runs) = led_and_followed(&flights, 3, |_| false);
        assert_eq!((led, followed, runs), (7, vec![9; 3], 4));
        assert!(flights.slots.lock().unwrap().is_empty());
    }

    #[test]
    fn a_join_flight_that_unwinds_hands_the_run_to_a_waiting_caller() {
        let flights = Arc::new(JoinFlight::<u8, u32>::new());
        let (started, wait_started) = mpsc::channel();
        let (release, wait_release) = mpsc::channel::<()>();
        let unwinding = {
            let flights = Arc::clone(&flights);
            std::thread::spawn(move || {
                flights.run(
                    &2,
                    || {
                        started.send(()).unwrap();
                        wait_release.recv().unwrap();
                        panic!("fold failed")
                    },
                    |_| true,
                )
            })
        };
        wait_started.recv().unwrap();
        let (computed, wait_computed) = mpsc::channel();
        {
            let flights = Arc::clone(&flights);
            std::thread::spawn(move || computed.send(flights.run(&2, || 5, |_| true)));
        }
        wait_for_callers(&flights, 2, 2);
        release.send(()).unwrap();
        assert!(unwinding.join().is_err());
        // A timeout, not a join: a caller left waiting must fail the test,
        // not hang it.
        assert_eq!(
            wait_computed.recv_timeout(Duration::from_secs(10)),
            Ok(5),
            "the waiting caller computes once the run unwinds"
        );
        assert!(flights.slots.lock().unwrap().is_empty());
    }
}
