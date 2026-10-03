//! Shared test scaffolding for the off-worker / blocking-pool tests across
//! the workspace (ADR-1702): a parking clock that announces its first
//! post-arm call and then blocks until released, and a watchdog that turns a
//! wedged runtime into a deterministic panic instead of an indefinitely hung
//! test binary.
//!
//! This crate is dev-only. It depends on no other Ravel crate, so a caller
//! whose own clock trait the parking clock has to satisfy implements that
//! trait for [`ParkOnFirstArmedCall`] on its own side, forwarding to the
//! inherent [`ParkOnFirstArmedCall::now_ns`] here.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use parking_lot::Mutex;

/// A clock that parks the calling thread inside the first `now_ns` call
/// made after [`arm`](Self::arm), announcing beforehand that the call has
/// begun, then blocking on `release_rx` until the test releases it.
///
/// The announcement is generic over its channel type via a boxed closure, and
/// which sender a call site passes turns on WHICH THREAD the park lands on,
/// not on whether a runtime is present -- every call site here runs under a
/// `current_thread` runtime.
///
/// When the parked call happens on a task the runtime must keep polling, the
/// waiter has to yield, so the announce goes through a tokio sender and the
/// waiter `recv().await`s it: a blocking wait there would stop the runtime
/// from ever polling the task it is waiting for.
///
/// When the parked call happens on a different OS thread -- the blocking
/// pool, which is exactly what the disk-tier tests exist to prove disk I/O
/// reaches -- a blocking wait on the runtime thread cannot deadlock it, so a
/// `std::sync::mpsc::SyncSender` and a plain `recv()` are fine. That is the
/// shape in `disk_tier_get_and_insert_run_on_the_blocking_pool`, which
/// `recv()`s inside `rt.block_on` while the park sits on the blocking pool.
///
/// Picking by "is there a runtime" rather than by "where does the park land"
/// is how a later test blocks the runtime thread on a park that landed on
/// that same thread, then reads the hang as a production regression.
///
/// Either way, `release_rx` is always a rendezvous channel: nothing here ever
/// waits on, measures, or compares a duration, only on an explicit release.
///
/// Arming is separate from construction because a fresh `DiskCache`
/// (`ravel_cache::DiskCache::new_with_clock`) and a test's own setup
/// `insert` both call the clock before the operation under test does; the
/// park must land on the first call *after* arming, not the first call ever.
pub struct ParkOnFirstArmedCall {
    armed: AtomicBool,
    parked: AtomicBool,
    announce: Box<dyn Fn() + Send + Sync>,
    release_rx: Mutex<Receiver<()>>,
}

impl ParkOnFirstArmedCall {
    pub fn new(announce: impl Fn() + Send + Sync + 'static, release_rx: Receiver<()>) -> Self {
        Self {
            armed: AtomicBool::new(false),
            parked: AtomicBool::new(false),
            announce: Box::new(announce),
            release_rx: Mutex::new(release_rx),
        }
    }

    /// Start parking on the next `now_ns` call. Calls made before this are
    /// no-ops (setup writes, the disk tier's own startup scan).
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// The parking read itself. A caller wires this into its own injected
    /// clock trait with a forwarding impl on its side of the dependency edge.
    pub fn now_ns(&self) -> u64 {
        if self.armed.load(Ordering::SeqCst) && !self.parked.swap(true, Ordering::SeqCst) {
            (self.announce)();
            self.release_rx.lock().recv().unwrap();
        }
        0
    }
}

/// Runs `body` on a dedicated OS thread and bounds how long the caller waits
/// for it with a watchdog: if `body` has not signalled completion within
/// `bound`, the watchdog panics with `timeout_message()` instead of hanging
/// the test binary forever. Nothing here compares a duration against a
/// measured one; `bound` only stops a wedged runtime from hanging past a
/// point a human is waiting on.
///
/// A panic raised inside `body` on the worker thread is propagated to the
/// caller's thread via `std::panic::resume_unwind`, so a genuine assertion
/// failure inside the body still fails the test with its own message, not
/// the watchdog's.
/// `#[track_caller]` so a timeout panic reports the test that wedged rather
/// than this shared helper: consolidating four copies into one otherwise
/// points every hang in a CI log at the same line here.
#[track_caller]
pub fn run_with_watchdog<F>(bound: Duration, timeout_message: impl FnOnce() -> String, body: F)
where
    F: FnOnce() + Send + 'static,
{
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

    let worker = std::thread::spawn(move || {
        body();
        let _ = done_tx.send(());
    });

    match done_rx.recv_timeout(bound) {
        Ok(()) => worker.join().unwrap(),
        Err(RecvTimeoutError::Timeout) => {
            panic!("{}", timeout_message());
        }
        Err(RecvTimeoutError::Disconnected) => match worker.join() {
            Ok(()) => panic!("worker thread exited without a result"),
            Err(panic) => std::panic::resume_unwind(panic),
        },
    }
}
