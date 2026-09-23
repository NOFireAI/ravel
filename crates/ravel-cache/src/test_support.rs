//! Shared test scaffolding for the off-worker / blocking-pool tests in
//! [`tiered`](crate::tiered) and [`disk`](crate::disk): a parking [`Clock`]
//! that announces its first post-arm call and then blocks until released,
//! and a watchdog that turns a wedged runtime into a deterministic panic
//! instead of an indefinitely hung test binary.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use parking_lot::Mutex;

use crate::clock::Clock;

/// A [`Clock`] that parks the calling thread inside the first `now_ns` call
/// made after [`arm`](Self::arm), announcing beforehand that the call has
/// begun, then blocking on `release_rx` until the test releases it.
///
/// The announcement is generic over its channel type via a boxed closure:
/// callers on a `current_thread` tokio runtime pass a tokio channel sender so
/// the awaiting task yields the runtime instead of blocking it (a
/// `recv().await` is what lets the parked call's own task be polled at all);
/// a caller with no such task to unblock (a plain OS thread waiting on the
/// parked clock, with no runtime in between) can pass a
/// `std::sync::mpsc::SyncSender` instead. Either way, `release_rx` is always
/// a rendezvous channel: nothing here ever waits on, measures, or compares a
/// duration, only on an explicit release.
///
/// Arming is separate from construction because a fresh [`DiskCache`]
/// (`crate::disk::DiskCache::new_with_clock`) and a test's own setup
/// `insert` both call the clock before the operation under test does; the
/// park must land on the first call *after* arming, not the first call ever.
pub(crate) struct ParkOnFirstArmedCall {
    armed: AtomicBool,
    parked: AtomicBool,
    announce: Box<dyn Fn() + Send + Sync>,
    release_rx: Mutex<Receiver<()>>,
}

impl ParkOnFirstArmedCall {
    pub(crate) fn new(
        announce: impl Fn() + Send + Sync + 'static,
        release_rx: Receiver<()>,
    ) -> Self {
        Self {
            armed: AtomicBool::new(false),
            parked: AtomicBool::new(false),
            announce: Box::new(announce),
            release_rx: Mutex::new(release_rx),
        }
    }

    /// Start parking on the next `now_ns` call. Calls made before this are
    /// no-ops (setup writes, the disk tier's own startup scan).
    pub(crate) fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl Clock for ParkOnFirstArmedCall {
    fn now_ns(&self) -> u64 {
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
pub(crate) fn run_with_watchdog<F>(
    bound: Duration,
    timeout_message: impl FnOnce() -> String,
    body: F,
) where
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
