//! Hand-written request coalescing (ADR-0046 decision 5). No coalescing
//! primitive exists in the workspace: semaphores bound concurrency but
//! never deduplicate work, so today two concurrent queries over one
//! segment issue two identical GETs. This collapses concurrent calls for
//! the same key into one, built from `parking_lot::Mutex` and
//! `tokio::sync::watch`, both already workspace dependencies.
//!
//! One call per key becomes the leader and runs the fetch; every other
//! concurrent call for that key is a follower that waits on a `watch`
//! channel for the leader's result instead of calling fetch itself. Two
//! failure modes are easy to get wrong here and are both handled by
//! [`LeaderGuard`]:
//!
//! - A failing fetch must reach every follower as an error rather than
//!   leaving any of them parked forever. The leader sends its `Err`
//!   result on the watch channel like any other result.
//! - A leader whose task is cancelled or that panics before `fetch`
//!   resolves must not leave followers waiting forever either. Both cases
//!   unwind the leader's stack (a cancelled future is dropped, a panic
//!   unwinds), and Rust runs destructors during unwinding, so
//!   [`LeaderGuard`]'s `Drop` fires exactly when the leader never got to
//!   send a real result. It sends [`SingleFlightError::LeaderLost`] and
//!   removes the in-flight slot so the next call for that key starts a
//!   fresh leader rather than joining a dead one.
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
/// admit the result (leader only) and whether to count a collapse
/// (follower only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

/// Error a `SingleFlight::run` call resolves to. `Upstream` carries the
/// fetch's own error, cloned to every waiter. `LeaderLost` is synthesized
/// when the leader's future is dropped (cancelled) or unwinds from a
/// panic before it produces a result.
#[derive(Debug, Clone)]
pub enum SingleFlightError<E> {
    Upstream(E),
    LeaderLost,
}

type Outcome<V, E> = Result<V, SingleFlightError<E>>;

struct Slot<V, E> {
    id: u64,
    tx: watch::Sender<Option<Outcome<V, E>>>,
}

enum LockOutcome<V, E> {
    Follower(watch::Receiver<Option<Outcome<V, E>>>),
    Leader(u64, watch::Sender<Option<Outcome<V, E>>>),
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

    /// Run `fetch` for `key`, collapsing concurrent calls for the same
    /// key into one invocation. Returns the result and which [`Role`]
    /// this call played.
    pub async fn run<F, Fut>(&self, key: K, fetch: F) -> (Outcome<V, E>, Role)
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<V, E>> + Send,
    {
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
            LockOutcome::Follower(mut rx) => (Self::wait(&mut rx).await, Role::Follower),
            LockOutcome::Leader(id, tx) => self.run_as_leader(key, id, tx, fetch).await,
        }
    }

    async fn wait(rx: &mut watch::Receiver<Option<Outcome<V, E>>>) -> Outcome<V, E> {
        loop {
            let current = rx.borrow_and_update().clone();
            if let Some(result) = current {
                return result;
            }
            if rx.changed().await.is_err() {
                // The sender was dropped without ever sending: the
                // leader's guard fired without a result to send (should
                // not happen; the guard always sends before dropping) or
                // was dropped before installing one. Either way, do not
                // hang.
                return Err(SingleFlightError::LeaderLost);
            }
        }
    }

    async fn run_as_leader<F, Fut>(
        &self,
        key: K,
        id: u64,
        tx: watch::Sender<Option<Outcome<V, E>>>,
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
        guard.tx.send_replace(Some(result.clone()));
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

/// Unblocks every follower if the leader never gets to send a result: a
/// cancelled leader future is dropped, and a panicking one unwinds, and
/// either way this `Drop` runs and sends [`SingleFlightError::LeaderLost`]
/// on its way out. A leader that finishes normally sets `done` first, so
/// this is then a no-op.
struct LeaderGuard<'a, K, V, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    flight: &'a SingleFlight<K, V, E>,
    key: K,
    id: u64,
    tx: watch::Sender<Option<Outcome<V, E>>>,
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
            // `send_replace` for the same reason as the success path. Here a
            // discarded value would be benign, since a follower that missed it
            // falls through to the sender-dropped branch and reports
            // `LeaderLost` anyway, which is the correct answer. Kept identical
            // so the two paths cannot drift.
            self.tx
                .send_replace(Some(Err(SingleFlightError::LeaderLost)));
            self.flight.remove_if_current(&self.key, self.id);
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
