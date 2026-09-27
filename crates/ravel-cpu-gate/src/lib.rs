//! The CPU gate: a capped `spawn_blocking` for codec work (ADR-1702).
//!
//! [`CpuGate::run`] takes an owned permit from a fixed-size semaphore, then
//! runs the job on tokio's blocking pool. The permit moves into the blocking
//! closure and is released only when the job returns, so a caller that stops
//! waiting cannot free a permit while its job still holds a core. A job whose
//! size is below the gate's inline floor runs on the calling thread and takes
//! no permit (decision 4).
//!
//! Cancellation (decision 5): a waiter dropped before it holds a permit never
//! runs its job. A job that has started runs to completion, because a bulk
//! codec call cannot be interrupted; if its waiter is gone when it returns,
//! the result is discarded and the job is counted as abandoned.
//!
//! The gate does no byte accounting. Permits count jobs; a caller's memory
//! `Reservation` moves into the job closure with its buffer (decision 6).
//!
//! Counters (decision 11): the permit count, jobs running and waiters queued
//! right now, the wait and run time sums and counts, abandoned jobs, and per
//! call site the jobs dispatched and the jobs run inline. Times come from an
//! injected [`MonotonicClock`].

mod clock;
mod site;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub use crate::clock::{InstantClock, MonotonicClock};
pub use crate::site::{GateKind, GateSite, ReadSite, WriteSite};

/// Units below this many uncompressed bytes run inline (decision 4).
pub const DEFAULT_INLINE_FLOOR_BYTES: u64 = 256 * 1024;

/// PromQL evaluations below this many samples run inline (decision 1).
pub const DEFAULT_EVAL_FLOOR_SAMPLES: u64 = 100_000;

/// Default read gate permits for a host with `cores` usable cores:
/// `max(1, cores - 1)` (decision 3).
pub fn default_read_permits(cores: usize) -> usize {
    cores.saturating_sub(1).max(1)
}

/// Default write gate permits for a host with `cores` usable cores:
/// `max(1, cores / 2)` (decision 3).
pub fn default_write_permits(cores: usize) -> usize {
    (cores / 2).max(1)
}

/// How much work a job is, compared against the matching inline floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSize {
    /// Uncompressed bytes of a codec unit, against
    /// [`CpuGateConfig::inline_floor_bytes`].
    Bytes(u64),
    /// Samples of a PromQL evaluation, against
    /// [`CpuGateConfig::eval_floor_samples`].
    Samples(u64),
}

/// One gate's settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuGateConfig {
    /// Jobs that may run at once. Floored at 1 by [`CpuGate::new`]: a
    /// zero-permit gate would never run a job.
    pub permits: usize,
    /// A [`JobSize::Bytes`] job below this runs inline. Tests set it to 0 so
    /// every job goes through the gate.
    pub inline_floor_bytes: u64,
    /// A [`JobSize::Samples`] job below this runs inline.
    pub eval_floor_samples: u64,
}

impl CpuGateConfig {
    /// `permits` with both inline floors at their defaults.
    pub fn with_permits(permits: usize) -> Self {
        CpuGateConfig {
            permits,
            inline_floor_bytes: DEFAULT_INLINE_FLOOR_BYTES,
            eval_floor_samples: DEFAULT_EVAL_FLOOR_SAMPLES,
        }
    }

    fn runs_inline(&self, size: JobSize) -> bool {
        match size {
            JobSize::Bytes(bytes) => bytes < self.inline_floor_bytes,
            JobSize::Samples(samples) => samples < self.eval_floor_samples,
        }
    }
}

/// Why a gated job produced no result. A caller maps this to its own decode
/// error; the gate never panics on a job's behalf.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CpuGateError {
    /// The job panicked on the blocking pool.
    #[error("CPU gate job panicked")]
    Panicked,
    /// The runtime dropped the job before it ran, which happens only while it
    /// shuts down.
    #[error("CPU gate job was cancelled before it ran")]
    Cancelled,
    /// The gate's semaphore was closed. Nothing closes it today.
    #[error("CPU gate is closed")]
    Closed,
}

#[derive(Debug, Default)]
struct Counters {
    running: AtomicU64,
    queued: AtomicU64,
    wait_nanos_sum: AtomicU64,
    wait_count: AtomicU64,
    run_nanos_sum: AtomicU64,
    run_count: AtomicU64,
    abandoned: AtomicU64,
}

#[derive(Debug, Default)]
struct SiteCounters {
    jobs: AtomicU64,
    inline: AtomicU64,
}

/// A capped `spawn_blocking` for the sites of one [`GateSite`] type.
pub struct CpuGate<S: GateSite> {
    semaphore: Arc<Semaphore>,
    permits: usize,
    config: CpuGateConfig,
    clock: Arc<dyn MonotonicClock>,
    counters: Arc<Counters>,
    sites: Box<[SiteCounters]>,
    _site: PhantomData<fn(S)>,
}

/// The read gate: query, catalog and maintenance decode.
pub type ReadGate = CpuGate<ReadSite>;
/// The write gate: flush encode and ingest payload decode.
pub type WriteGate = CpuGate<WriteSite>;

impl<S: GateSite> CpuGate<S> {
    /// A gate with `config.permits` permits (at least 1, at most tokio's
    /// semaphore maximum), measuring with `clock`.
    pub fn new(config: CpuGateConfig, clock: Arc<dyn MonotonicClock>) -> Self {
        let permits = config.permits.clamp(1, Semaphore::MAX_PERMITS);
        CpuGate {
            semaphore: Arc::new(Semaphore::new(permits)),
            permits,
            config,
            clock,
            counters: Arc::new(Counters::default()),
            sites: S::ALL.iter().map(|_| SiteCounters::default()).collect(),
            _site: PhantomData,
        }
    }

    /// Which gate this is.
    pub fn kind(&self) -> GateKind {
        S::GATE
    }

    /// The permit count the gate was built with.
    pub fn permits(&self) -> usize {
        self.permits
    }

    /// Jobs holding a permit right now, including a job whose waiter is gone.
    pub fn running(&self) -> u64 {
        self.counters.running.load(Ordering::Acquire)
    }

    /// Waiters queued for a permit right now.
    pub fn queued(&self) -> u64 {
        self.counters.queued.load(Ordering::Acquire)
    }

    /// Started jobs whose waiter was gone when they returned.
    pub fn abandoned(&self) -> u64 {
        self.counters.abandoned.load(Ordering::Acquire)
    }

    /// Runs `job` for `site`: inline when `size` is below the matching floor,
    /// otherwise on the blocking pool once a permit is free.
    pub async fn run<F, R>(&self, site: S, size: JobSize, job: F) -> Result<R, CpuGateError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let site_counters = self.sites.get(site.index());
        if self.config.runs_inline(size) {
            if let Some(counters) = site_counters {
                counters.inline.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(job());
        }

        let wait_started = self.clock.now_nanos();
        let queued = QueuedGuard::enter(&self.counters);
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .map_err(|_| CpuGateError::Closed)?;
        drop(queued);
        let waited = self.clock.now_nanos().saturating_sub(wait_started);
        self.counters
            .wait_nanos_sum
            .fetch_add(waited, Ordering::Relaxed);
        self.counters.wait_count.fetch_add(1, Ordering::Relaxed);
        if let Some(counters) = site_counters {
            counters.jobs.fetch_add(1, Ordering::Relaxed);
        }

        let handoff = Arc::new(AtomicU8::new(0));
        let running = JobGuard::start(
            &self.counters,
            Arc::clone(&self.clock),
            Arc::clone(&handoff),
            permit,
        );
        let handle = tokio::task::spawn_blocking(move || {
            let mut running = running;
            running.started_at = Some(running.clock.now_nanos());
            job()
        });
        let waiter = WaiterGuard {
            handoff,
            counters: Arc::clone(&self.counters),
            armed: true,
        };
        let joined = handle.await;
        waiter.disarm();
        joined.map_err(|err| {
            if err.is_panic() {
                CpuGateError::Panicked
            } else {
                CpuGateError::Cancelled
            }
        })
    }

    /// Every counter, read once.
    pub fn snapshot(&self) -> CpuGateSnapshot<S> {
        let counters = &self.counters;
        CpuGateSnapshot {
            gate: S::GATE,
            permits: self.permits,
            running: counters.running.load(Ordering::Acquire),
            queued: counters.queued.load(Ordering::Acquire),
            wait_nanos_sum: counters.wait_nanos_sum.load(Ordering::Relaxed),
            wait_count: counters.wait_count.load(Ordering::Relaxed),
            run_nanos_sum: counters.run_nanos_sum.load(Ordering::Relaxed),
            run_count: counters.run_count.load(Ordering::Relaxed),
            abandoned: counters.abandoned.load(Ordering::Acquire),
            sites: S::ALL
                .iter()
                .zip(self.sites.iter())
                .map(|(&site, counts)| SiteSnapshot {
                    site,
                    jobs: counts.jobs.load(Ordering::Relaxed),
                    inline: counts.inline.load(Ordering::Relaxed),
                })
                .collect(),
        }
    }
}

/// One gate's counters at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuGateSnapshot<S> {
    pub gate: GateKind,
    pub permits: usize,
    pub running: u64,
    pub queued: u64,
    /// Nanoseconds spent waiting for a permit, over waits that got one.
    pub wait_nanos_sum: u64,
    pub wait_count: u64,
    /// Nanoseconds jobs spent running on the blocking pool, panics included.
    pub run_nanos_sum: u64,
    pub run_count: u64,
    pub abandoned: u64,
    /// One entry per site, in [`GateSite::ALL`] order.
    pub sites: Vec<SiteSnapshot<S>>,
}

/// One call site's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SiteSnapshot<S> {
    pub site: S,
    /// Jobs that took a permit and were dispatched to the blocking pool.
    pub jobs: u64,
    /// Jobs below the inline floor, run on the calling thread.
    pub inline: u64,
}

/// Holds one `queued` count for as long as a waiter is waiting, including a
/// waiter dropped mid-wait.
struct QueuedGuard<'a> {
    counters: &'a Counters,
}

impl<'a> QueuedGuard<'a> {
    fn enter(counters: &'a Counters) -> Self {
        counters.queued.fetch_add(1, Ordering::AcqRel);
        QueuedGuard { counters }
    }
}

impl Drop for QueuedGuard<'_> {
    fn drop(&mut self) {
        self.counters.queued.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Set by the job side when a started job returns or unwinds.
const JOB_FINISHED: u8 = 1;
/// Set by the waiter side when it is dropped without taking the result.
const WAITER_GONE: u8 = 2;

/// Owns a running job's permit and `running` count. It lives in the blocking
/// closure, so both are released only when the job returns or unwinds.
/// Whichever of this guard and the [`WaiterGuard`] sets its bit second sees
/// the other's, which is how an abandoned job is counted exactly once.
struct JobGuard {
    counters: Arc<Counters>,
    clock: Arc<dyn MonotonicClock>,
    handoff: Arc<AtomicU8>,
    started_at: Option<u64>,
    _permit: OwnedSemaphorePermit,
}

impl JobGuard {
    fn start(
        counters: &Arc<Counters>,
        clock: Arc<dyn MonotonicClock>,
        handoff: Arc<AtomicU8>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        counters.running.fetch_add(1, Ordering::AcqRel);
        JobGuard {
            counters: Arc::clone(counters),
            clock,
            handoff,
            started_at: None,
            _permit: permit,
        }
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        if let Some(started_at) = self.started_at {
            let ran = self.clock.now_nanos().saturating_sub(started_at);
            self.counters
                .run_nanos_sum
                .fetch_add(ran, Ordering::Relaxed);
            self.counters.run_count.fetch_add(1, Ordering::Relaxed);
            let seen = self.handoff.fetch_or(JOB_FINISHED, Ordering::AcqRel);
            if seen & WAITER_GONE != 0 {
                self.counters.abandoned.fetch_add(1, Ordering::AcqRel);
            }
        }
        // The permit field drops after this body, so `running` falls before
        // the next waiter can take the permit.
        self.counters.running.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The waiter's half of the abandonment handshake.
struct WaiterGuard {
    handoff: Arc<AtomicU8>,
    counters: Arc<Counters>,
    armed: bool,
}

impl WaiterGuard {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let seen = self.handoff.fetch_or(WAITER_GONE, Ordering::AcqRel);
        if seen & JOB_FINISHED != 0 {
            self.counters.abandoned.fetch_add(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
