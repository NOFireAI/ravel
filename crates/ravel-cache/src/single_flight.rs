//! Hand-written request coalescing (ADR-0046 decision 5). No coalescing
//! primitive exists in the workspace: semaphores bound concurrency but
//! never deduplicate work, so today two concurrent queries over one
//! segment issue two identical GETs. This collapses concurrent calls for
//! the same key into one, built from `parking_lot::Mutex` and
//! `tokio::sync::watch`, both already workspace dependencies.
//!
//! One call per key becomes the leader and runs the fetch; every other
//! concurrent call for that key is a follower that waits on a `watch`
//! channel for the leader's result instead of calling fetch itself. The
//! leader deposits one of three outcomes on that channel, and
//! [`LeaderGuard`]'s `Drop` is what guarantees it does so even when the
//! leader never reaches the end of `fetch`:
//!
//! - A finished fetch (`Ok`, or an `Err` the follower receives as
//!   [`SingleFlightError::Upstream`]) is deposited by the leader itself, so
//!   a failing fetch reaches every follower as that error rather than
//!   leaving any of them parked.
//! - A cancelled leader, its future dropped before `fetch` resolved,
//!   deposits a cancellation signal. A drop outside any unwind reads
//!   `std::thread::panicking()` as false, so [`LeaderGuard`]'s `Drop`
//!   records the cancellation. A follower that receives it does not fail:
//!   it re-enters the slot for the key, becoming the new leader if the slot
//!   is now vacant (running its own `fetch` under its own cancellation) or
//!   following whoever leads it otherwise. The reason is issue #4174: a
//!   query torn down for its memory budget drops the fetch it was leading,
//!   and healthy followers in other queries must not inherit a 503 from a
//!   store that never failed. Re-entry is bounded by
//!   `MAX_CANCELLED_LEADER_REENTRIES`; a follower still following when the
//!   bound is exhausted returns [`SingleFlightError::LeaderLost`].
//! - A leader whose fetch panicked is dropped during that unwind, so `Drop`
//!   reads `std::thread::panicking()` as true and records a panic. Followers
//!   of a panicking leader receive [`SingleFlightError::LeaderLost`] and do
//!   NOT re-enter, because a deterministic panic would otherwise repeat once
//!   per follower. `panicking()` is true for any drop during an unwind on
//!   that thread, so a cancelled leader dropped while an unrelated panic
//!   unwinds the same thread is also recorded as a panic; its followers then
//!   get `LeaderLost` instead of re-entering.
//!
//! A cancelled or panicking leader removes its slot before it deposits the
//! outcome. A follower can only subscribe to a slot it finds in the map, so
//! a follower woken by a cancellation never finds that leader's slot again
//! and counts one re-entry per cancelled leader it followed.
//!
//! The cancelled/panicked distinction is carried on the channel through an
//! internal `LeaderOutcome` signal, not through [`SingleFlightError`], so
//! the public error enum is unchanged.
//!
//! Role and the collapse counter stay truthful across re-entry.
//! [`SingleFlight::run`] reports [`Role::Leader`] for the call that ran
//! `fetch` and [`Role::Follower`] for a call that rode another's result, so
//! a re-entering follower that takes over leadership is counted as a leader,
//! not as a collapse. [`SingleFlight::run_with_reentries`] additionally reports
//! how many times the call re-entered after a cancelled leader, which the cache
//! records on a counter separate from the collapse counter.
//!
//! A completed leader removes its own slot before returning, and does so
//! only if the slot still belongs to that leader's attempt (matched by a
//! monotonic id), so a later, separate miss on the same key always starts
//! a new leader rather than reusing a stale in-flight slot.

use std::collections::HashMap;
use std::collections::hash_map::Entry as MapEntry;
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::watch;

/// Whether a `SingleFlight::run` call ran the fetch itself or joined an
/// in-flight one. `Cache::get_or_fetch` uses this to decide whether to
/// count a collapse (follower only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

/// Error a `SingleFlight::run` call resolves to. `Upstream` carries the
/// fetch's own error, cloned to every waiter. `LeaderLost` is synthesized
/// when the leader unwinds from a panic before producing a result, or when a
/// follower whose leader was cancelled has re-entered the slot
/// `MAX_CANCELLED_LEADER_REENTRIES` times without any attempt resolving. A
/// leader that is merely cancelled (its future dropped) does not reach a
/// follower as `LeaderLost` on its own: the follower re-enters and runs the
/// fetch instead (issue #4174).
#[derive(Debug, Clone)]
pub enum SingleFlightError<E> {
    Upstream(E),
    LeaderLost,
}

type Outcome<V, E> = Result<V, SingleFlightError<E>>;

/// How many cancelled leaders one follower follows before giving up: the
/// follower re-enters the slot after each of its leaders is cancelled, and
/// once it has re-entered this many times the next cancellation returns
/// [`SingleFlightError::LeaderLost`]. It bounds how long one follower keeps
/// waiting through a chain of cancellations, not how many fetches run: each
/// caller runs its own fetch at most once whatever the value. A cancelled
/// leader is almost always a single query torn down for its memory budget, so
/// the common path re-enters at most once.
pub(crate) const MAX_CANCELLED_LEADER_REENTRIES: u32 = 8;

/// What a leader deposits on its slot's `watch` channel for followers. Kept
/// internal so the cancelled/panicked distinction never widens the public
/// [`SingleFlightError`].
enum LeaderOutcome<V, E> {
    /// The leader finished and produced a result: `Ok`, or an upstream `Err`
    /// the follower receives as [`SingleFlightError::Upstream`].
    Resolved(Outcome<V, E>),
    /// The leader's future was dropped before producing a result. A follower
    /// re-enters the slot rather than failing.
    Cancelled,
    /// The leader unwound from a panic before producing a result. A follower
    /// returns [`SingleFlightError::LeaderLost`] and does not re-enter.
    Panicked,
}

// `watch::Receiver::borrow_and_update().clone()` clones the stored value, so
// the channel payload must be `Clone`; a manual impl avoids a `V: Clone + E:
// Clone` bound being demanded everywhere the type name appears, as a derive
// would.
impl<V, E> Clone for LeaderOutcome<V, E>
where
    V: Clone,
    E: Clone,
{
    fn clone(&self) -> Self {
        match self {
            LeaderOutcome::Resolved(result) => LeaderOutcome::Resolved(result.clone()),
            LeaderOutcome::Cancelled => LeaderOutcome::Cancelled,
            LeaderOutcome::Panicked => LeaderOutcome::Panicked,
        }
    }
}

/// What a follower's [`SingleFlight::wait`] decided the leader's deposited
/// outcome means for this follower.
enum FollowerWait<V, E> {
    /// The leader produced a result; return it.
    Resolved(Outcome<V, E>),
    /// The leader was cancelled; re-enter the slot (subject to the bound).
    Retry,
    /// The leader panicked, or its sender dropped without depositing an
    /// outcome; return [`SingleFlightError::LeaderLost`] and do not retry.
    Lost,
}

struct Slot<V, E> {
    id: u64,
    tx: watch::Sender<Option<LeaderOutcome<V, E>>>,
}

enum LockOutcome<V, E> {
    Follower(watch::Receiver<Option<LeaderOutcome<V, E>>>),
    Leader(u64, watch::Sender<Option<LeaderOutcome<V, E>>>),
}

/// A request-coalescing map keyed by `K`, producing values `V` or errors
/// `E`. See the [module docs](self).
pub struct SingleFlight<K, V, E> {
    inflight: Mutex<HashMap<K, Slot<V, E>>>,
    next_id: AtomicU64,
}

impl<K, V, E> Default for SingleFlight<K, V, E> {
    fn default() -> Self {
        SingleFlight {
            inflight: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }
}

impl<K, V, E> SingleFlight<K, V, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `fetch` for `key`, collapsing concurrent calls for the same key
    /// into one invocation. Returns the result and which [`Role`] this call
    /// played. The arity-stable entry point for callers that do not record the
    /// re-entry count ([`run_with_reentries`](Self::run_with_reentries) is the
    /// variant the RAM and tiered caches use to feed their counter); the
    /// cancelled-leader retry behaviour is identical through both.
    pub async fn run<F, Fut>(&self, key: K, fetch: F) -> (Outcome<V, E>, Role)
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<V, E>> + Send,
    {
        let (result, role, _reentries) = self.run_with_reentries(key, fetch).await;
        (result, role)
    }

    /// [`run`](Self::run), also reporting how many times this call lost a
    /// cancelled leader and re-entered the slot (0 on the common path, at most
    /// `MAX_CANCELLED_LEADER_REENTRIES`). The caches record that count on a
    /// counter separate from the collapse counter.
    ///
    /// `fetch` is `FnOnce` and runs at most once: only the branch that becomes
    /// leader consumes it, and that branch returns, so a re-entry after a
    /// cancelled leader never re-runs a `fetch` that already produced a result.
    pub async fn run_with_reentries<F, Fut>(&self, key: K, fetch: F) -> (Outcome<V, E>, Role, u32)
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<V, E>> + Send,
    {
        let mut reentries: u32 = 0;
        loop {
            let outcome = {
                let mut inflight = self.inflight.lock();
                match inflight.entry(key.clone()) {
                    MapEntry::Occupied(slot) => LockOutcome::Follower(slot.get().tx.subscribe()),
                    MapEntry::Vacant(vacant) => {
                        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                        let (tx, _rx) = watch::channel(None);
                        vacant.insert(Slot { id, tx: tx.clone() });
                        LockOutcome::Leader(id, tx)
                    }
                }
            };

            match outcome {
                // Moving `fetch` is sound inside the loop: this branch diverges
                // (`return`), so the loop's only back-edge is the follower
                // `continue` below, which never touches `fetch`.
                LockOutcome::Leader(id, tx) => {
                    let (result, role) = self.run_as_leader(key, id, tx, fetch).await;
                    return (result, role, reentries);
                }
                LockOutcome::Follower(mut rx) => match Self::wait(&mut rx).await {
                    FollowerWait::Resolved(result) => return (result, Role::Follower, reentries),
                    FollowerWait::Lost => {
                        return (
                            Err(SingleFlightError::LeaderLost),
                            Role::Follower,
                            reentries,
                        );
                    }
                    FollowerWait::Retry => {
                        if reentries >= MAX_CANCELLED_LEADER_REENTRIES {
                            return (
                                Err(SingleFlightError::LeaderLost),
                                Role::Follower,
                                reentries,
                            );
                        }
                        reentries += 1;
                        continue;
                    }
                },
            }
        }
    }

    /// How many followers are parked on the in-flight call for `key`; 0 when
    /// none is in flight.
    pub fn waiters(&self, key: &K) -> usize {
        self.inflight
            .lock()
            .get(key)
            .map_or(0, |slot| slot.tx.receiver_count())
    }

    /// Whether a call for `key` is in flight, followers or not.
    pub fn is_in_flight(&self, key: &K) -> bool {
        self.inflight.lock().contains_key(key)
    }

    async fn wait(rx: &mut watch::Receiver<Option<LeaderOutcome<V, E>>>) -> FollowerWait<V, E> {
        loop {
            let current = rx.borrow_and_update().clone();
            match current {
                Some(LeaderOutcome::Resolved(result)) => return FollowerWait::Resolved(result),
                Some(LeaderOutcome::Cancelled) => return FollowerWait::Retry,
                Some(LeaderOutcome::Panicked) => return FollowerWait::Lost,
                None => {
                    if rx.changed().await.is_err() {
                        // The sender was dropped without ever depositing an
                        // outcome: the leader's guard fired without one (should
                        // not happen; the guard always deposits before dropping)
                        // or was dropped before installing one. Either way, do
                        // not hang, and do not retry on a signal that may not be
                        // a clean cancellation.
                        return FollowerWait::Lost;
                    }
                }
            }
        }
    }

    async fn run_as_leader<F, Fut>(
        &self,
        key: K,
        id: u64,
        tx: watch::Sender<Option<LeaderOutcome<V, E>>>,
        fetch: F,
    ) -> (Outcome<V, E>, Role)
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<V, E>> + Send,
    {
        let mut guard = LeaderGuard {
            flight: self,
            key: key.clone(),
            id,
            tx,
            done: false,
        };

        let result = fetch().await.map_err(SingleFlightError::Upstream);
        guard.done = true;
        // `send_replace`, never `send`. `watch::Sender::send` returns `Err`
        // and stores nothing when the receiver count is zero, and the leader's
        // own receiver was dropped when it released the mutex. A leader that
        // finishes before any follower subscribes would therefore leave the
        // channel holding `None`; a follower that subscribes in the window
        // between this line and `remove_if_current` sees `None`, then sees
        // every sender drop, and reports `LeaderLost` for a fetch that
        // succeeded. `send_replace` stores the value regardless of receivers.
        // Depositing before the removal is safe on this path: a caller that
        // finds the slot in between reads `Resolved` and returns it without
        // re-entering.
        guard
            .tx
            .send_replace(Some(LeaderOutcome::Resolved(result.clone())));
        self.remove_if_current(&key, id);

        (result, Role::Leader)
    }

    fn remove_if_current(&self, key: &K, id: u64) {
        let mut inflight = self.inflight.lock();
        if let Some(slot) = inflight.get(key)
            && slot.id == id
        {
            inflight.remove(key);
        }
    }
}

/// Unblocks every follower if the leader never gets to deposit a result: a
/// cancelled leader future is dropped, and a panicking one is dropped during
/// the unwind, and either way this `Drop` runs. It distinguishes the two with
/// `std::thread::panicking()` -- false for a drop outside any unwind, true for
/// a drop during one -- and deposits `LeaderOutcome::Cancelled` or
/// `LeaderOutcome::Panicked` accordingly, so a follower retries a cancelled
/// leader but surfaces [`SingleFlightError::LeaderLost`] for a panicking one.
/// A leader that finishes normally sets `done` first, so this is then a no-op.
struct LeaderGuard<'a, K, V, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    flight: &'a SingleFlight<K, V, E>,
    key: K,
    id: u64,
    tx: watch::Sender<Option<LeaderOutcome<V, E>>>,
    done: bool,
}

impl<K, V, E> Drop for LeaderGuard<'_, K, V, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.done {
            // Cancellation (a plain drop) and a panic reach here the same way,
            // but a follower must treat them differently: retry a cancelled
            // leader, give up on a panicking one. `std::thread::panicking()` is
            // the only signal available in a destructor that tells them apart.
            // It is true for any drop during an unwind on this thread: a leader
            // whose own fetch panicked reports `Panicked`, and so does a
            // cancelled leader dropped during an unrelated unwind, which only
            // costs its followers the retry.
            let outcome = if std::thread::panicking() {
                LeaderOutcome::Panicked
            } else {
                LeaderOutcome::Cancelled
            };
            // `send_replace` for the same reason as the success path: it stores
            // the value regardless of the receiver count. A follower that misses
            // it falls through to the sender-dropped branch, which returns
            // `LeaderLost` without retrying -- the conservative answer when the
            // cancel/panic signal was lost.
            //
            // Remove the slot before depositing. A follower woken by the outcome
            // re-enters the map; if this slot were still there it would
            // subscribe to it again, read the same outcome at once, and spend a
            // re-entry per lap on one cancellation. Followers already subscribed
            // still receive the outcome, because `self.tx` lives until the end
            // of this function.
            self.flight.remove_if_current(&self.key, self.id);
            self.tx.send_replace(Some(outcome));
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod send_replace_regression {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A successful fetch must never reach a follower as `LeaderLost`.
    ///
    /// `watch::Sender::send` returns `Err` and stores nothing when the receiver
    /// count is zero. The leader drops its own receiver when it releases the
    /// mutex, so a leader that finishes before any follower subscribes used to
    /// leave the channel holding `None`. A follower subscribing in the window
    /// between the send and the slot removal then saw `None`, saw every sender
    /// drop, and reported `LeaderLost` for a fetch that had succeeded. That is
    /// a store read turned into a query error by timing alone, which
    /// ADR-0046 decision 4 forbids.
    ///
    /// This test must run on a multi-thread runtime: the `#[tokio::test]`
    /// default is current-thread, where nothing can preempt between those two
    /// statements, so the defect is invisible there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn successful_fetch_is_never_reported_as_leader_lost() {
        let flight: Arc<SingleFlight<u64, u64, &'static str>> = Arc::new(SingleFlight::new());
        let spurious = Arc::new(AtomicUsize::new(0));

        for round in 0..2_000u64 {
            let mut handles = Vec::new();
            for _ in 0..4 {
                let flight = Arc::clone(&flight);
                let spurious = Arc::clone(&spurious);
                handles.push(tokio::spawn(async move {
                    let (result, _role) = flight
                        .run(round, || async move { Ok::<u64, &'static str>(round) })
                        .await;
                    match result {
                        Ok(value) => assert_eq!(value, round),
                        Err(SingleFlightError::LeaderLost) => {
                            spurious.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(other) => panic!("unexpected error: {other:?}"),
                    }
                }));
            }
            for handle in handles {
                handle.await.expect("waiter task joins");
            }
        }

        assert_eq!(
            spurious.load(Ordering::Relaxed),
            0,
            "a fetch that returned Ok was reported to a follower as LeaderLost"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod cancelled_leader_drop_order {
    use std::hash::Hasher;
    use std::panic::AssertUnwindSafe;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;

    type ProbeRx = watch::Receiver<Option<LeaderOutcome<u64, &'static str>>>;

    /// Records, at the moment a dropping leader looks its slot up to remove it,
    /// whether that leader's outcome is already visible on the channel.
    #[derive(Default)]
    struct Probe {
        armed: AtomicBool,
        rx: Mutex<Option<ProbeRx>>,
        outcome_visible_at_removal: Mutex<Option<bool>>,
    }

    /// A key whose `Hash` runs the probe once after it is armed. The first
    /// hash of the key after arming is the guard's own lookup in
    /// `remove_if_current`, made under the map lock.
    #[derive(Clone)]
    struct ProbeKey {
        id: u64,
        probe: Arc<Probe>,
    }

    impl PartialEq for ProbeKey {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    impl Eq for ProbeKey {}

    impl Hash for ProbeKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.id.hash(state);
            if self.probe.armed.swap(false, Ordering::SeqCst) {
                let visible = self
                    .probe
                    .rx
                    .lock()
                    .as_ref()
                    .map(|rx| rx.borrow().is_some());
                *self.probe.outcome_visible_at_removal.lock() = visible;
            }
        }
    }

    fn poll_now<F: Future + ?Sized>(future: &mut Pin<Box<F>>) -> Poll<F::Output> {
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }

    fn subscribe_and_arm(flight: &SingleFlight<ProbeKey, u64, &'static str>, key: &ProbeKey) {
        let rx = flight
            .inflight
            .lock()
            .get(key)
            .map(|slot| slot.tx.subscribe());
        assert!(rx.is_some(), "the parked leader holds the slot");
        *key.probe.rx.lock() = rx;
        key.probe.armed.store(true, Ordering::SeqCst);
    }

    /// A cancelled leader's slot is gone from the map before its `Cancelled`
    /// becomes visible, so no follower woken by it can find and re-subscribe to
    /// that slot. Fails on the deposit-then-remove order: the probe then sees
    /// the outcome already deposited while the guard is still removing.
    #[test]
    fn cancelled_leader_removes_its_slot_before_the_outcome_is_visible() {
        let flight = SingleFlight::<ProbeKey, u64, &'static str>::new();
        let key = ProbeKey {
            id: 1,
            probe: Arc::new(Probe::default()),
        };
        let mut leader = Box::pin(flight.run(key.clone(), || {
            std::future::pending::<Result<u64, &'static str>>()
        }));
        assert!(poll_now(&mut leader).is_pending());
        subscribe_and_arm(&flight, &key);

        drop(leader);

        assert_eq!(
            *key.probe.outcome_visible_at_removal.lock(),
            Some(false),
            "the cancelled leader's outcome was visible while its slot was still in the map"
        );
        assert!(!flight.is_in_flight(&key));
        let rx = key
            .probe
            .rx
            .lock()
            .take()
            .expect("the probe holds a receiver");
        assert!(matches!(*rx.borrow(), Some(LeaderOutcome::Cancelled)));
    }

    /// The panic path shares the cancelled path's order: the slot is removed
    /// before `Panicked` is visible, so only callers that subscribed while the
    /// leader was in flight inherit the panic.
    #[test]
    fn panicking_leader_removes_its_slot_before_the_outcome_is_visible() {
        let flight = SingleFlight::<ProbeKey, u64, &'static str>::new();
        let key = ProbeKey {
            id: 2,
            probe: Arc::new(Probe::default()),
        };
        let (go_tx, go_rx) = oneshot::channel::<()>();
        let mut leader = Box::pin(flight.run(key.clone(), move || async move {
            let _ = go_rx.await;
            panic!("leader fetch panics after the follower subscribed");
        }));
        assert!(poll_now(&mut leader).is_pending());
        subscribe_and_arm(&flight, &key);

        go_tx
            .send(())
            .expect("the leader is parked on the receiver");
        let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| poll_now(&mut leader)));
        assert!(unwound.is_err(), "the leader's fetch panicked");

        assert_eq!(
            *key.probe.outcome_visible_at_removal.lock(),
            Some(false),
            "the panicking leader's outcome was visible while its slot was still in the map"
        );
        assert!(!flight.is_in_flight(&key));
        let rx = key
            .probe
            .rx
            .lock()
            .take()
            .expect("the probe holds a receiver");
        assert!(matches!(*rx.borrow(), Some(LeaderOutcome::Panicked)));
    }

    /// One cancelled leader costs its follower exactly one re-entry, and the
    /// follower resolves with its own fetch rather than `LeaderLost`. The
    /// follower is woken on another worker while the cancelled leader's guard
    /// is still running, so the drop order is raced for real. On the path
    /// where the follower re-enters and finds the key vacant it leads at once,
    /// and the count is still 1: one per cancelled leader followed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_cancellation_costs_a_follower_exactly_one_reentry() {
        let flight: Arc<SingleFlight<u64, u64, &'static str>> = Arc::new(SingleFlight::new());

        for round in 0..2_000u64 {
            let (entered_tx, entered_rx) = oneshot::channel::<()>();
            let leader = {
                let flight = Arc::clone(&flight);
                tokio::spawn(async move {
                    flight
                        .run(round, move || async move {
                            let _ = entered_tx.send(());
                            std::future::pending::<Result<u64, &'static str>>().await
                        })
                        .await
                })
            };
            entered_rx.await.expect("the leader enters its fetch");

            let follower = {
                let flight = Arc::clone(&flight);
                tokio::spawn(async move {
                    flight
                        .run_with_reentries(round, move || async move {
                            Ok::<u64, &'static str>(round + 1)
                        })
                        .await
                })
            };
            // The timeout only guards against a hang.
            timeout(Duration::from_secs(10), async {
                while flight.waiters(&round) < 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the follower subscribes to the parked leader");

            leader.abort();

            let (result, role, reentries) = timeout(Duration::from_secs(10), follower)
                .await
                .expect("the follower does not hang")
                .expect("the follower task joins");
            match result {
                Ok(value) => assert_eq!(value, round + 1, "round {round}"),
                Err(SingleFlightError::LeaderLost) => {
                    panic!("round {round}: one cancellation reached the follower as LeaderLost")
                }
                Err(other) => panic!("round {round}: unexpected error {other:?}"),
            }
            assert_eq!(role, Role::Leader, "round {round}");
            assert_eq!(
                reentries, 1,
                "round {round}: one cancellation must cost exactly one re-entry"
            );
        }
    }
}
