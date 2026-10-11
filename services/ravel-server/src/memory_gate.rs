//! The memory gate's server side (ADR-2633 task 2, issue #2730, amended for
//! issue #2806): startup resolution and its stamp, the `ravel-memory-gate`
//! sampler thread, and the figures `/metrics` renders for it.
//!
//! The gate closes on the host's available memory, never on the process's
//! own resident size or on the budget: a warm server whose read caches are
//! full sits far above any mark on its own memory while the host still has
//! memory to spare, and the one quantity that separates a server the kernel
//! kills from one it does not is host available memory.
//!
//! The gate state itself lives in [`ravel_memory::MemoryBudget`]; this module
//! is its only writer. When the gate is off the sampler is never started, so
//! [`ravel_memory::MemoryBudget::set_host_memory_gate`] is never called and
//! the budget's gate stays open with a reading and floor of 0.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ravel_memory::MemoryBudget;

use crate::config::{Cli, HostProfile, Mode, PERF_SOURCE_FALLBACK, ResolvedPerformanceDefaults};

const MIB: u64 = 1 << 20;

/// The lowest `--memory-gate-min-available-bytes` accepted: 64 MiB.
pub const MIN_AVAILABLE_FLOOR_BYTES: u64 = 64 * MIB;
/// The default close floor is the larger of this, 512 MiB, and
/// [`DEFAULT_MIN_AVAILABLE_PERCENT`] of host memory.
pub const DEFAULT_MIN_AVAILABLE_BYTES: u64 = 512 * MIB;
/// The share of host memory the default close floor takes when it is above
/// [`DEFAULT_MIN_AVAILABLE_BYTES`].
pub const DEFAULT_MIN_AVAILABLE_PERCENT: u8 = 2;
/// Default `--memory-gate-purge-min-interval-ms`.
pub const DEFAULT_PURGE_MIN_INTERVAL_MS: u64 = 5000;
/// The accepted range of `--memory-gate-purge-min-interval-ms`.
pub const MIN_PURGE_MIN_INTERVAL_MS: u64 = 100;
pub const MAX_PURGE_MIN_INTERVAL_MS: u64 = 600_000;
/// Default `--memory-gate-interval-ms`.
pub const DEFAULT_INTERVAL_MS: u64 = 100;
/// Default `--memory-gate-wait-ms`.
pub const DEFAULT_WAIT_MS: u64 = 2000;

/// Stamp source: the gate is on at the default floors.
pub const SOURCE_DEFAULT: &str = "default";
/// Stamp source: the gate is on and `--memory-gate-min-available-bytes` or
/// `--memory-gate-purge-available-bytes` set a floor.
pub const SOURCE_FLAG: &str = "flag";
/// Stamp source: off, because `--mode gateway` claims no memory budget.
pub const SOURCE_DISABLED_GATEWAY: &str = "disabled-gateway";
/// Stamp source: off, because the budget resolved from the fallback.
pub const SOURCE_DISABLED_FALLBACK_BUDGET: &str = "disabled-fallback-budget";
/// Stamp source: off, because `--disable-memory-gate` was passed.
pub const SOURCE_DISABLED_FLAG: &str = "disabled-flag";
/// Stamp source: off, because this build does not run under jemalloc.
pub const SOURCE_NOT_JEMALLOC: &str = "not-jemalloc";
/// Stamp source: off, because neither `/proc/meminfo` nor the cgroup memory
/// files could be read, so there is no host memory to take a floor of and
/// nothing for the sampler to read.
pub const SOURCE_DISABLED_NO_HOST_MEMORY: &str = "disabled-no-host-memory";

/// The memory gate flags, as parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGateFlags {
    pub min_available_bytes: Option<u64>,
    pub purge_available_bytes: Option<u64>,
    pub purge_min_interval_ms: u64,
    pub interval_ms: u64,
    pub wait_ms: u64,
    pub disabled: bool,
}

/// Where the sampler reads the host's available memory from, decided once
/// at startup from the same host profile the budget derivation reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostMemorySource {
    /// `MemAvailable` from `/proc/meminfo`.
    MemInfo,
    /// The cgroup memory limit minus the cgroup's current usage: v2
    /// `memory.max` minus `memory.current`, else v1 `memory.limit_in_bytes`
    /// minus `memory.usage_in_bytes`. Chosen when the process runs under a
    /// finite cgroup limit, the branch the budget derivation takes too.
    Cgroup,
}

/// The sampler's two floors and the purge spacing, the part of a resolved
/// gate each sample decides on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floors {
    /// The gate is closed while the final available reading of a sample is
    /// below this.
    pub min_available: u64,
    /// A forced purge runs when the available reading is below this and the
    /// previous purge started at least `purge_min_interval` ago.
    pub purge_available: u64,
    pub purge_min_interval: Duration,
}

/// The resolved memory gate, stamped once at startup by [`MemoryGate::emit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGate {
    pub enabled: bool,
    /// Host memory as the performance defaults resolve it: `MemTotal` capped
    /// by the cgroup memory limit. 0 when unknown.
    pub host_memory_bytes: u64,
    /// The close floor; 0 when off, and never passed to the budget then.
    pub min_available_bytes: u64,
    /// The purge floor; 0 when off.
    pub purge_available_bytes: u64,
    pub purge_min_interval_ms: u64,
    pub interval_ms: u64,
    pub wait_ms: u64,
    /// The resolved `memory_budget_bytes`, stamped beside the floors so a
    /// reader can see what the gate no longer derives from.
    pub memory_budget_bytes: u64,
    /// Where the sampler reads available memory.
    pub host_memory_source: HostMemorySource,
    /// One of the `SOURCE_*` constants: who set the floors, or why the gate
    /// is off.
    pub source: &'static str,
}

impl MemoryGate {
    /// The startup stamp, beside the `performance default resolved` lines.
    pub fn emit(&self) {
        tracing::info!(
            enabled = self.enabled,
            host_memory_bytes = self.host_memory_bytes,
            min_available_bytes = self.min_available_bytes,
            purge_available_bytes = self.purge_available_bytes,
            purge_min_interval_ms = self.purge_min_interval_ms,
            interval_ms = self.interval_ms,
            wait_ms = self.wait_ms,
            memory_budget_bytes = self.memory_budget_bytes,
            source = self.source,
            "memory gate resolved"
        );
    }

    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    pub fn floors(&self) -> Floors {
        Floors {
            min_available: self.min_available_bytes,
            purge_available: self.purge_available_bytes,
            purge_min_interval: Duration::from_millis(self.purge_min_interval_ms),
        }
    }
}

/// Startup refusal: the floors cannot work as given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryGateFloorError {
    /// The purge floor is not above the close floor, so the purge would
    /// never run before the gate closed.
    PurgeFloorNotAboveCloseFloor {
        purge_available_bytes: u64,
        min_available_bytes: u64,
    },
    /// The close floor is at or above host memory, so the gate could never
    /// open.
    CloseFloorReachesHostMemory {
        min_available_bytes: u64,
        host_memory_bytes: u64,
    },
}

impl std::fmt::Display for MemoryGateFloorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PurgeFloorNotAboveCloseFloor {
                purge_available_bytes,
                min_available_bytes,
            } => write!(
                f,
                "--memory-gate-purge-available-bytes ({purge_available_bytes}) must be above \
                 --memory-gate-min-available-bytes ({min_available_bytes}): the forced purge \
                 runs at the purge floor so it can reclaim memory before the gate closes"
            ),
            Self::CloseFloorReachesHostMemory {
                min_available_bytes,
                host_memory_bytes,
            } => write!(
                f,
                "--memory-gate-min-available-bytes ({min_available_bytes}) reaches host memory \
                 ({host_memory_bytes} bytes): available memory can never be at or above it, so \
                 the gate would never open. Lower the floor or pass --disable-memory-gate"
            ),
        }
    }
}

impl std::error::Error for MemoryGateFloorError {}

/// `percent`% of `total`, rounded down.
fn percent_of(total: u64, percent: u8) -> u64 {
    let share = u128::from(total) * u128::from(percent) / 100;
    u64::try_from(share).unwrap_or(u64::MAX)
}

/// The default close floor for `host_memory_bytes` of host memory:
/// `max(512 MiB, 2 % of host memory)`.
pub fn default_min_available_bytes(host_memory_bytes: u64) -> u64 {
    DEFAULT_MIN_AVAILABLE_BYTES.max(percent_of(host_memory_bytes, DEFAULT_MIN_AVAILABLE_PERCENT))
}

/// Resolves the gate from its flags and the facts that turn it off. The off
/// reasons are checked in the order the stamp's sources list them: gateway
/// mode, a fallback budget, `--disable-memory-gate`, a build without
/// jemalloc, a host whose memory could not be read. An enabled gate's floors
/// are then checked: the purge floor must be above the close floor, and the
/// close floor below host memory.
pub fn resolve(
    flags: MemoryGateFlags,
    mode: Mode,
    performance: &ResolvedPerformanceDefaults,
    host: HostProfile,
    jemalloc: bool,
) -> Result<MemoryGate, MemoryGateFloorError> {
    let off = if !mode.uses_memory_budget() {
        Some(SOURCE_DISABLED_GATEWAY)
    } else if performance.sources.memory_budget_bytes == PERF_SOURCE_FALLBACK {
        Some(SOURCE_DISABLED_FALLBACK_BUDGET)
    } else if flags.disabled {
        Some(SOURCE_DISABLED_FLAG)
    } else if !jemalloc {
        Some(SOURCE_NOT_JEMALLOC)
    } else if host.mem_total_bytes.is_none() {
        Some(SOURCE_DISABLED_NO_HOST_MEMORY)
    } else {
        None
    };
    let mut gate = MemoryGate {
        enabled: false,
        host_memory_bytes: host.mem_total_bytes.unwrap_or(0),
        min_available_bytes: 0,
        purge_available_bytes: 0,
        purge_min_interval_ms: flags.purge_min_interval_ms,
        interval_ms: flags.interval_ms,
        wait_ms: flags.wait_ms,
        memory_budget_bytes: performance.memory_budget_bytes,
        host_memory_source: if host.cgroup_memory_limit_bytes.is_some() {
            HostMemorySource::Cgroup
        } else {
            HostMemorySource::MemInfo
        },
        source: SOURCE_DEFAULT,
    };
    if let Some(source) = off {
        gate.source = source;
        return Ok(gate);
    }

    let min_available_bytes = flags
        .min_available_bytes
        .unwrap_or_else(|| default_min_available_bytes(gate.host_memory_bytes));
    let purge_available_bytes = flags
        .purge_available_bytes
        .unwrap_or_else(|| min_available_bytes.saturating_mul(2));
    if purge_available_bytes <= min_available_bytes {
        return Err(MemoryGateFloorError::PurgeFloorNotAboveCloseFloor {
            purge_available_bytes,
            min_available_bytes,
        });
    }
    if min_available_bytes >= gate.host_memory_bytes {
        return Err(MemoryGateFloorError::CloseFloorReachesHostMemory {
            min_available_bytes,
            host_memory_bytes: gate.host_memory_bytes,
        });
    }
    gate.enabled = true;
    gate.min_available_bytes = min_available_bytes;
    gate.purge_available_bytes = purge_available_bytes;
    gate.source = if flags.min_available_bytes.is_some() || flags.purge_available_bytes.is_some() {
        SOURCE_FLAG
    } else {
        SOURCE_DEFAULT
    };
    Ok(gate)
}

impl Cli {
    /// [`resolve`] from this command line, the resolved performance defaults
    /// and the host profile they were resolved from, for this build's
    /// allocator.
    pub fn resolve_memory_gate(
        &self,
        performance: &ResolvedPerformanceDefaults,
        host: HostProfile,
    ) -> anyhow::Result<MemoryGate> {
        let flags = MemoryGateFlags {
            min_available_bytes: self.memory_gate_min_available_bytes,
            purge_available_bytes: self.memory_gate_purge_available_bytes,
            purge_min_interval_ms: self.memory_gate_purge_min_interval_ms,
            interval_ms: self.memory_gate_interval_ms,
            wait_ms: self.memory_gate_wait_ms,
            disabled: self.disable_memory_gate,
        };
        Ok(resolve(
            flags,
            self.mode,
            performance,
            host,
            crate::mem_stats::JEMALLOC,
        )?)
    }
}

/// The host's available memory right now, from `source`. `None` when the
/// files behind it cannot be read or parsed; the sampler then keeps the
/// gate's last state.
pub fn read_available(source: HostMemorySource) -> Option<u64> {
    match source {
        HostMemorySource::MemInfo => HostProfile::detect_mem_available_bytes(),
        HostMemorySource::Cgroup => {
            let limit = HostProfile::detect_cgroup_memory_limit_bytes()?;
            let current = HostProfile::detect_cgroup_memory_current_bytes()?;
            Some(limit.saturating_sub(current))
        }
    }
}

/// Upper bounds, in microseconds, of the `ravel_memory_gate_purge_seconds`
/// buckets. 50 ms is the ADR-2633 acceptance bound on the p99.
pub const PURGE_BUCKET_BOUNDS_MICROS: [u64; 12] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 1_000_000,
];
/// One bucket per bound plus the overflow bucket.
pub const PURGE_BUCKET_COUNT: usize = PURGE_BUCKET_BOUNDS_MICROS.len() + 1;

/// The sampler's running totals and its two gauges that do not live on the
/// budget: the last resident reading and the purge floor. The process's one
/// instance is [`STATS`]; tests build their own.
#[derive(Debug)]
pub struct MemoryGateStats {
    samples: AtomicU64,
    epoch_refresh_nanos: AtomicU64,
    purges: AtomicU64,
    purge_nanos: AtomicU64,
    /// Non-cumulative: bucket `i` counts purges that landed in it alone.
    purge_buckets: [AtomicU64; PURGE_BUCKET_COUNT],
    resident_bytes: AtomicU64,
    purge_floor_bytes: AtomicU64,
}

/// A scrape-time copy of [`MemoryGateStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryGateStatsSnapshot {
    /// Samples that got an available-memory reading.
    pub samples: u64,
    /// Wall time spent in epoch refresh plus the available and
    /// `stats.resident` reads, the re-read after a purge included.
    pub epoch_refresh_nanos: u64,
    pub purges: u64,
    /// Wall time spent purging.
    pub purge_nanos: u64,
    /// Non-cumulative per-bucket purge counts.
    pub purge_buckets: [u64; PURGE_BUCKET_COUNT],
    /// jemalloc `stats.resident` as the sampler last read it, after the
    /// purge when one ran; 0 before the first reading.
    pub resident_bytes: u64,
    /// The purge floor the sampler runs with; 0 when the gate is off.
    pub purge_floor_bytes: u64,
}

impl MemoryGateStats {
    pub const fn new() -> Self {
        Self {
            samples: AtomicU64::new(0),
            epoch_refresh_nanos: AtomicU64::new(0),
            purges: AtomicU64::new(0),
            purge_nanos: AtomicU64::new(0),
            purge_buckets: [const { AtomicU64::new(0) }; PURGE_BUCKET_COUNT],
            resident_bytes: AtomicU64::new(0),
            purge_floor_bytes: AtomicU64::new(0),
        }
    }

    fn record_epoch_refresh(&self, elapsed: Duration) {
        self.epoch_refresh_nanos
            .fetch_add(duration_nanos(elapsed), Ordering::Relaxed);
    }

    fn record_purge(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let bucket = PURGE_BUCKET_BOUNDS_MICROS
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(PURGE_BUCKET_COUNT - 1);
        if let Some(slot) = self.purge_buckets.get(bucket) {
            slot.fetch_add(1, Ordering::Relaxed);
        }
        self.purge_nanos
            .fetch_add(duration_nanos(elapsed), Ordering::Relaxed);
        self.purges.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MemoryGateStatsSnapshot {
        let mut purge_buckets = [0; PURGE_BUCKET_COUNT];
        for (slot, bucket) in purge_buckets.iter_mut().zip(&self.purge_buckets) {
            *slot = bucket.load(Ordering::Relaxed);
        }
        MemoryGateStatsSnapshot {
            samples: self.samples.load(Ordering::Relaxed),
            epoch_refresh_nanos: self.epoch_refresh_nanos.load(Ordering::Relaxed),
            purges: self.purges.load(Ordering::Relaxed),
            purge_nanos: self.purge_nanos.load(Ordering::Relaxed),
            purge_buckets,
            resident_bytes: self.resident_bytes.load(Ordering::Relaxed),
            purge_floor_bytes: self.purge_floor_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Default for MemoryGateStats {
    fn default() -> Self {
        Self::new()
    }
}

fn duration_nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

/// The process's sampler totals, rendered on `/metrics`.
pub static STATS: MemoryGateStats = MemoryGateStats::new();

/// What one sample did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sample {
    /// The gate was set from `available`, the sample's final reading (the
    /// post-purge re-read when a purge ran). `resident` is the matching
    /// `stats.resident` reading, `None` when it could not be read.
    Sampled {
        available: u64,
        resident: Option<u64>,
        purged: bool,
        closed: bool,
    },
    /// No available-memory reading; the gate keeps its previous state and
    /// nothing is written.
    Unread,
}

/// One sampler pass (ADR-2633 sections 1 and 2, as amended for issue #2806):
/// read the host's available memory and `stats.resident`; below the purge
/// floor, purge once if the previous purge started at least the minimum
/// interval ago, and re-read both; then set the gate from the final
/// available reading, closed below the close floor. A sample below the
/// close floor whose purge is rate-limited closes on the reading it has.
/// When a re-read fails the figure from before the purge stands.
///
/// `now` is the injected clock: every instant this pass needs, the purge
/// spacing included, comes from it and never from `Instant::now()`.
#[allow(clippy::too_many_arguments)]
pub fn sample_once(
    budget: &MemoryBudget,
    stats: &MemoryGateStats,
    floors: Floors,
    now: &mut impl FnMut() -> Instant,
    last_purge: &mut Option<Instant>,
    read_available: &mut impl FnMut() -> Option<u64>,
    read_resident: &mut impl FnMut() -> Option<u64>,
    purge: &mut impl FnMut(),
) -> Sample {
    let started = now();
    let first_available = read_available();
    let mut resident = read_resident();
    stats.record_epoch_refresh(now().duration_since(started));
    let Some(mut available) = first_available else {
        return Sample::Unread;
    };
    stats.samples.fetch_add(1, Ordering::Relaxed);

    let mut purged = false;
    if available < floors.purge_available {
        let purge_started = now();
        let due = last_purge
            .is_none_or(|last| purge_started.duration_since(last) >= floors.purge_min_interval);
        if due {
            purge();
            stats.record_purge(now().duration_since(purge_started));
            *last_purge = Some(purge_started);
            purged = true;
            let reread_started = now();
            let reread_available = read_available();
            let reread_resident = read_resident();
            stats.record_epoch_refresh(now().duration_since(reread_started));
            if let Some(reread) = reread_available {
                available = reread;
            }
            if reread_resident.is_some() {
                resident = reread_resident;
            }
        }
    }

    if let Some(resident) = resident {
        stats.resident_bytes.store(resident, Ordering::Relaxed);
    }
    budget.set_host_memory_gate(available, floors.min_available);
    Sample::Sampled {
        available,
        resident,
        purged,
        closed: available < floors.min_available,
    }
}

static SAMPLER_STARTED: AtomicBool = AtomicBool::new(false);

/// Starts the `ravel-memory-gate` thread for the process's life when `gate`
/// is enabled, at most once per process. Returns whether this call started
/// it: an off gate starts nothing, so the budget's setter is never called.
pub fn start_sampler(gate: &MemoryGate, budget: Arc<MemoryBudget>) -> std::io::Result<bool> {
    if !gate.enabled {
        return Ok(false);
    }
    if SAMPLER_STARTED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(false);
    }
    STATS
        .purge_floor_bytes
        .store(gate.purge_available_bytes, Ordering::Relaxed);
    let floors = gate.floors();
    let interval = gate.interval();
    let source = gate.host_memory_source;
    let spawned = std::thread::Builder::new()
        .name("ravel-memory-gate".to_string())
        .spawn(move || run_sampler(&budget, floors, interval, source));
    match spawned {
        Ok(_) => Ok(true),
        Err(err) => {
            SAMPLER_STARTED.store(false, Ordering::Release);
            Err(err)
        }
    }
}

fn run_sampler(
    budget: &MemoryBudget,
    floors: Floors,
    interval: Duration,
    source: HostMemorySource,
) {
    let mut now = Instant::now;
    let mut last_purge = None;
    let mut read_available = move || read_available(source);
    let mut read_resident = crate::mem_stats::read_resident;
    let mut purge = || {
        let report = crate::mem_stats::purge_arenas();
        if report.arenas_purged == 0 {
            tracing::warn!(
                arenas_skipped = report.arenas_skipped,
                "memory gate purge found no initialised arena to purge, so resident will not \
                 fall; the purge still counts in ravel_memory_gate_purges_total"
            );
        }
    };
    let mut warned_unread = false;
    let mut warned_resident = false;
    loop {
        let started = Instant::now();
        let sample = sample_once(
            budget,
            &STATS,
            floors,
            &mut now,
            &mut last_purge,
            &mut read_available,
            &mut read_resident,
            &mut purge,
        );
        match sample {
            Sample::Unread if !warned_unread => {
                warned_unread = true;
                tracing::warn!(
                    "memory gate could not read the host's available memory; the gate keeps its \
                     last state until a reading succeeds"
                );
            }
            Sample::Sampled { resident: None, .. } if !warned_resident => {
                warned_resident = true;
                tracing::warn!(
                    "memory gate could not read jemalloc stats.resident; \
                     ravel_memory_gate_resident_bytes keeps its last reading"
                );
            }
            _ => {}
        }
        std::thread::sleep(next_sleep(interval, started.elapsed()));
    }
}

/// How long the sampler sleeps after a pass that took `elapsed`: the rest of
/// the interval, or half an interval when the pass used it all (a long purge
/// at the 10 ms minimum), so purges never run back to back and the duty
/// cycle ADR-2633 section 2 budgets holds.
fn next_sleep(interval: Duration, elapsed: Duration) -> Duration {
    if elapsed >= interval {
        interval / 2
    } else {
        interval - elapsed
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::PERF_SOURCE_FLAG;

    const GIB: u64 = 1 << 30;
    const GB: u64 = 1_000_000_000;
    /// The default floors on a 32 GiB host: `max(512 MiB, 2 %)` and twice
    /// that.
    const FLOOR_32G: u64 = 687_194_767;
    const PURGE_FLOOR_32G: u64 = 1_374_389_534;

    fn flags() -> MemoryGateFlags {
        MemoryGateFlags {
            min_available_bytes: None,
            purge_available_bytes: None,
            purge_min_interval_ms: DEFAULT_PURGE_MIN_INTERVAL_MS,
            interval_ms: DEFAULT_INTERVAL_MS,
            wait_ms: DEFAULT_WAIT_MS,
            disabled: false,
        }
    }

    /// The default floors on the 32 GiB host.
    fn floors() -> Floors {
        Floors {
            min_available: FLOOR_32G,
            purge_available: PURGE_FLOOR_32G,
            purge_min_interval: Duration::from_millis(DEFAULT_PURGE_MIN_INTERVAL_MS),
        }
    }

    /// Resolved defaults for a `--memory-budget-bytes` of `budget` on
    /// `host`.
    fn performance(mode: Mode, budget: u64, host: HostProfile) -> ResolvedPerformanceDefaults {
        let cli = <Cli as clap::Parser>::try_parse_from([
            "ravel-server".to_string(),
            "--mode".to_string(),
            mode_arg(mode).to_string(),
            "--memory-budget-bytes".to_string(),
            budget.to_string(),
        ])
        .expect("flags parse");
        cli.resolve_performance(host).expect("performance resolves")
    }

    /// A 32 GiB host with no cgroup limit.
    fn host() -> HostProfile {
        HostProfile::new(8, Some(32 * GIB), Some(32 * GIB), None, None, None)
    }

    fn mode_arg(mode: Mode) -> &'static str {
        match mode {
            Mode::All => "all",
            Mode::Gateway => "gateway",
            Mode::Query => "query",
            Mode::Maintain => "maintain",
        }
    }

    /// A clock that returns `at` until the test moves it, so a sample's own
    /// timing reads 0 and the spacing between samples is the test's.
    struct TestClock {
        at: std::cell::Cell<Instant>,
    }

    impl TestClock {
        fn new() -> Self {
            Self {
                at: std::cell::Cell::new(Instant::now()),
            }
        }

        fn advance(&self, by: Duration) {
            self.at.set(self.at.get() + by);
        }

        fn now(&self) -> impl FnMut() -> Instant {
            || self.at.get()
        }
    }

    /// One sample with fixed readings and a purge that reclaims nothing.
    fn run_sample(
        budget: &MemoryBudget,
        stats: &MemoryGateStats,
        clock: &TestClock,
        last_purge: &mut Option<Instant>,
        available: u64,
        resident: u64,
    ) -> Sample {
        sample_once(
            budget,
            stats,
            floors(),
            &mut clock.now(),
            last_purge,
            &mut || Some(available),
            &mut || Some(resident),
            &mut || {},
        )
    }

    /// Issue #2806: a warm server with full caches on a host that still has
    /// memory to spare admits work. Available 1.0 GB on the 32 GiB host is
    /// above the 687 MB close floor, so resident 29 GB decides nothing.
    ///
    /// FLIP: close on `resident >= 19.4 GB` (70 % of the 27.8 GB stock
    /// budget, the mark before this fix) or on `resident >= 28.8 GB` (90 %
    /// of a 32 GB host) and the gate is closed here.
    #[test]
    fn a_warm_server_with_free_host_memory_stays_open() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let clock = TestClock::new();
        let mut last_purge = None;
        let sample = run_sample(&budget, &stats, &clock, &mut last_purge, GB, 29 * GB);
        assert_eq!(
            sample,
            Sample::Sampled {
                available: GB,
                resident: Some(29 * GB),
                purged: true,
                closed: false,
            }
        );
        assert!(budget.gate_open());
        budget.try_reserve(1).expect("an open gate admits");
        assert_eq!(budget.gate_available(), GB);
        assert_eq!(budget.gate_floor(), FLOOR_32G);
        assert_eq!(stats.snapshot().resident_bytes, 29 * GB);
    }

    /// Available memory falling 1.0, 0.8, 0.6 GB across three samples, with
    /// a purge that reclaims nothing: the gate closes at 0.6 GB, below the
    /// 687 MB floor, and the refusal carries the host-memory cause. The
    /// second and third samples are below the purge floor but their purge
    /// is rate-limited, so they decide on the reading they have.
    ///
    /// FLIP: never close (write `available.max(floors.min_available)` to
    /// the budget) and the third sample leaves the gate open.
    #[test]
    fn the_gate_closes_before_the_kill() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let clock = TestClock::new();
        let mut last_purge = None;
        let mut purges = 0;
        let mut run = |available: u64| {
            clock.advance(Duration::from_millis(DEFAULT_INTERVAL_MS));
            sample_once(
                &budget,
                &stats,
                floors(),
                &mut clock.now(),
                &mut last_purge,
                &mut || Some(available),
                &mut || Some(29 * GB),
                &mut || purges += 1,
            )
        };
        let first = run(GB);
        assert!(matches!(
            first,
            Sample::Sampled {
                purged: true,
                closed: false,
                ..
            }
        ));
        assert!(budget.gate_open());

        let second = run(800_000_000);
        assert_eq!(
            second,
            Sample::Sampled {
                available: 800_000_000,
                resident: Some(29 * GB),
                purged: false,
                closed: false,
            }
        );
        assert!(budget.gate_open());

        let third = run(600_000_000);
        assert_eq!(
            third,
            Sample::Sampled {
                available: 600_000_000,
                resident: Some(29 * GB),
                purged: false,
                closed: true,
            }
        );
        assert!(!budget.gate_open());
        assert_eq!(purges, 1);
        let refused = budget
            .try_reserve(1)
            .expect_err("a closed gate refuses one byte");
        assert_eq!(
            refused.cause,
            ravel_memory::ExhaustionCause::HostMemory {
                available: 600_000_000,
                floor: FLOOR_32G,
            }
        );
        assert_eq!(
            refused.to_string(),
            format!(
                "memory exhausted: host available memory 600000000 bytes is below the \
                 {FLOOR_32G} byte floor (requested 1)"
            )
        );
    }

    /// Available memory back at 1.0 GB reopens the gate on the next sample.
    #[test]
    fn the_gate_reopens_when_available_memory_returns() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let clock = TestClock::new();
        let mut last_purge = None;
        run_sample(
            &budget,
            &stats,
            &clock,
            &mut last_purge,
            600_000_000,
            29 * GB,
        );
        assert!(!budget.gate_open());
        clock.advance(Duration::from_millis(DEFAULT_INTERVAL_MS));
        let sample = run_sample(&budget, &stats, &clock, &mut last_purge, GB, 29 * GB);
        assert!(matches!(sample, Sample::Sampled { closed: false, .. }));
        assert!(budget.gate_open());
        budget.try_reserve(1).expect("a reopened gate admits");
    }

    /// Available 1.2 GB is below the 1.37 GB purge floor and above the
    /// close floor: the purge runs and the gate stays open.
    ///
    /// FLIP: close at the purge floor (`closed: available <
    /// floors.purge_available`) and this sample closes the gate.
    #[test]
    fn a_reading_below_the_purge_floor_purges_and_stays_open() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let clock = TestClock::new();
        let mut last_purge = None;
        let sample = run_sample(
            &budget,
            &stats,
            &clock,
            &mut last_purge,
            1_200_000_000,
            20 * GB,
        );
        assert_eq!(
            sample,
            Sample::Sampled {
                available: 1_200_000_000,
                resident: Some(20 * GB),
                purged: true,
                closed: false,
            }
        );
        assert!(budget.gate_open());
        assert_eq!(stats.snapshot().purges, 1);
        assert!(last_purge.is_some());

        // At the purge floor no purge runs.
        clock.advance(Duration::from_secs(10));
        let sample = run_sample(
            &budget,
            &stats,
            &clock,
            &mut last_purge,
            PURGE_FLOOR_32G,
            20 * GB,
        );
        assert!(matches!(
            sample,
            Sample::Sampled {
                purged: false,
                closed: false,
                ..
            }
        ));
        assert_eq!(stats.snapshot().purges, 1);
    }

    /// With the injected clock, 100 samples below the purge floor 100 ms
    /// apart run exactly 2 purges: one at the first sample and one at the
    /// first sample 5 s or more later, `floor(9.9 s / 5 s) + 1`.
    ///
    /// FLIP: purge on every sample below the purge floor (drop the
    /// `last_purge` spacing) and the count reads 100.
    #[test]
    fn purges_are_spaced_by_the_minimum_interval() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let clock = TestClock::new();
        let mut last_purge = None;
        let mut purges = 0;
        for index in 0..100 {
            if index > 0 {
                clock.advance(Duration::from_millis(100));
            }
            let sample = sample_once(
                &budget,
                &stats,
                floors(),
                &mut clock.now(),
                &mut last_purge,
                &mut || Some(1_200_000_000),
                &mut || Some(20 * GB),
                &mut || purges += 1,
            );
            assert!(matches!(sample, Sample::Sampled { closed: false, .. }));
        }
        assert_eq!(purges, 2);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.purges, 2);
        assert_eq!(snapshot.purge_buckets.iter().sum::<u64>(), 2);
        assert_eq!(snapshot.samples, 100);
    }

    /// An unreadable available memory leaves the gate in its last state and
    /// never reaches the setter, closed or open.
    #[test]
    fn an_unreadable_available_reading_leaves_the_gate_alone() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let clock = TestClock::new();
        let mut last_purge = None;
        let mut purges = 0;
        let mut unread = |budget: &MemoryBudget, last_purge: &mut Option<Instant>| {
            sample_once(
                budget,
                &stats,
                floors(),
                &mut clock.now(),
                last_purge,
                &mut || None,
                &mut || Some(20 * GB),
                &mut || purges += 1,
            )
        };
        assert_eq!(unread(&budget, &mut last_purge), Sample::Unread);
        assert!(budget.gate_open());
        assert_eq!(budget.gate_available(), 0);
        assert_eq!(budget.gate_floor(), 0, "the setter was not called");

        run_sample(
            &budget,
            &stats,
            &clock,
            &mut last_purge,
            600_000_000,
            20 * GB,
        );
        assert!(!budget.gate_open());
        assert_eq!(unread(&budget, &mut last_purge), Sample::Unread);
        assert!(!budget.gate_open(), "a closed gate stays closed");
        assert_eq!(budget.gate_available(), 600_000_000);
        assert_eq!(purges, 0, "an unread sample purges nothing");
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.purges, 1, "only the readable sample purged");
        assert_eq!(snapshot.samples, 1);
    }

    /// The resident gauge holds the post-purge re-read, and a failed re-read
    /// leaves the pre-purge figures standing.
    #[test]
    fn the_resident_gauge_holds_the_post_purge_reread() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let clock = TestClock::new();
        let mut last_purge = None;
        let mut availables = [GB, 2 * GB].into_iter();
        let mut residents = [29 * GB, 25 * GB].into_iter();
        let sample = sample_once(
            &budget,
            &stats,
            floors(),
            &mut clock.now(),
            &mut last_purge,
            &mut || availables.next(),
            &mut || residents.next(),
            &mut || {},
        );
        assert_eq!(
            sample,
            Sample::Sampled {
                available: 2 * GB,
                resident: Some(25 * GB),
                purged: true,
                closed: false,
            }
        );
        assert_eq!(budget.gate_available(), 2 * GB);
        assert_eq!(stats.snapshot().resident_bytes, 25 * GB);

        clock.advance(Duration::from_secs(10));
        let mut availables = [600_000_000, 0].into_iter();
        let mut residents = [29 * GB].into_iter();
        let sample = sample_once(
            &budget,
            &stats,
            floors(),
            &mut clock.now(),
            &mut last_purge,
            &mut || availables.next().filter(|reading| *reading > 0),
            &mut || residents.next(),
            &mut || {},
        );
        assert_eq!(
            sample,
            Sample::Sampled {
                available: 600_000_000,
                resident: Some(29 * GB),
                purged: true,
                closed: true,
            }
        );
        assert_eq!(stats.snapshot().resident_bytes, 29 * GB);
    }

    #[test]
    fn the_default_floors_on_a_32_gib_host_are_687_mb_and_twice_that() {
        assert_eq!(default_min_available_bytes(32 * GIB), FLOOR_32G);
        // Below 25.6 GiB the 512 MiB floor wins over 2 %.
        assert_eq!(default_min_available_bytes(8 * GIB), 512 * MIB);
        assert_eq!(default_min_available_bytes(0), 512 * MIB);

        let on = performance(Mode::Query, 27 * GB, host());
        assert_eq!(on.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
        let gate = resolve(flags(), Mode::Query, &on, host(), true).expect("resolves");
        assert!(gate.enabled);
        assert_eq!(gate.source, SOURCE_DEFAULT);
        assert_eq!(gate.host_memory_bytes, 32 * GIB);
        assert_eq!(gate.min_available_bytes, FLOOR_32G);
        assert_eq!(gate.purge_available_bytes, PURGE_FLOOR_32G);
        assert_eq!(gate.purge_min_interval_ms, 5000);
        assert_eq!(gate.memory_budget_bytes, 27 * GB);
        assert_eq!(gate.host_memory_source, HostMemorySource::MemInfo);
        assert_eq!(gate.floors(), floors());
    }

    #[test]
    fn the_floors_are_validated_against_each_other_and_host_memory() {
        let on = performance(Mode::Query, 27 * GB, host());

        // A purge floor at or below the close floor is refused.
        let refused = resolve(
            MemoryGateFlags {
                purge_available_bytes: Some(FLOOR_32G),
                ..flags()
            },
            Mode::Query,
            &on,
            host(),
            true,
        )
        .expect_err("purge floor equal to the close floor");
        assert_eq!(
            refused,
            MemoryGateFloorError::PurgeFloorNotAboveCloseFloor {
                purge_available_bytes: FLOOR_32G,
                min_available_bytes: FLOOR_32G,
            }
        );
        let message = refused.to_string();
        assert!(
            message.contains(&format!(
                "--memory-gate-purge-available-bytes ({FLOOR_32G})"
            )),
            "{message}"
        );
        assert!(
            message.contains(&format!("--memory-gate-min-available-bytes ({FLOOR_32G})")),
            "{message}"
        );
        resolve(
            MemoryGateFlags {
                purge_available_bytes: Some(FLOOR_32G + 1),
                ..flags()
            },
            Mode::Query,
            &on,
            host(),
            true,
        )
        .expect("one byte above the close floor");

        // A close floor at or above host memory is refused; the purge floor
        // follows it as twice its value.
        let refused = resolve(
            MemoryGateFlags {
                min_available_bytes: Some(32 * GIB),
                ..flags()
            },
            Mode::Query,
            &on,
            host(),
            true,
        )
        .expect_err("close floor at host memory");
        assert_eq!(
            refused,
            MemoryGateFloorError::CloseFloorReachesHostMemory {
                min_available_bytes: 32 * GIB,
                host_memory_bytes: 32 * GIB,
            }
        );
        assert!(
            refused
                .to_string()
                .contains(&format!("reaches host memory ({} bytes)", 32 * GIB)),
            "{refused}"
        );
        let flagged = resolve(
            MemoryGateFlags {
                min_available_bytes: Some(32 * GIB - 1),
                purge_available_bytes: Some(32 * GIB),
                ..flags()
            },
            Mode::Query,
            &on,
            host(),
            true,
        )
        .expect("one byte under host memory");
        assert_eq!(flagged.source, SOURCE_FLAG);
        assert_eq!(flagged.min_available_bytes, 32 * GIB - 1);
        assert_eq!(flagged.purge_available_bytes, 32 * GIB);

        // An off gate checks nothing.
        let off = resolve(
            MemoryGateFlags {
                min_available_bytes: Some(32 * GIB),
                disabled: true,
                ..flags()
            },
            Mode::Query,
            &on,
            host(),
            true,
        )
        .expect("off gate is not checked");
        assert!(!off.enabled);
        assert_eq!(off.min_available_bytes, 0);
    }

    #[test]
    fn the_stamp_reports_every_source() {
        let on = performance(Mode::Query, 27 * GB, host());
        assert_eq!(on.sources.memory_budget_bytes, PERF_SOURCE_FLAG);

        let default = resolve(flags(), Mode::Query, &on, host(), true).expect("resolves");
        assert_eq!(default.source, SOURCE_DEFAULT);
        assert!(default.enabled);

        for flagged in [
            MemoryGateFlags {
                min_available_bytes: Some(GB),
                ..flags()
            },
            MemoryGateFlags {
                purge_available_bytes: Some(3 * GB),
                ..flags()
            },
        ] {
            let gate = resolve(flagged, Mode::Query, &on, host(), true).expect("resolves");
            assert_eq!(gate.source, SOURCE_FLAG);
            assert!(gate.enabled);
        }
        let gate = resolve(
            MemoryGateFlags {
                min_available_bytes: Some(GB),
                ..flags()
            },
            Mode::Query,
            &on,
            host(),
            true,
        )
        .expect("resolves");
        assert_eq!(gate.min_available_bytes, GB);
        assert_eq!(
            gate.purge_available_bytes,
            2 * GB,
            "twice the flagged close floor"
        );

        let gateway = performance(Mode::Gateway, 27 * GB, host());
        let off = resolve(flags(), Mode::Gateway, &gateway, host(), true).expect("resolves");
        assert_eq!(off.source, SOURCE_DISABLED_GATEWAY);
        assert!(!off.enabled);
        assert_eq!(off.min_available_bytes, 0);
        assert_eq!(off.purge_available_bytes, 0);

        let mut fallback = on;
        fallback.sources.memory_budget_bytes = PERF_SOURCE_FALLBACK;
        let off = resolve(flags(), Mode::Query, &fallback, host(), true).expect("resolves");
        assert_eq!(off.source, SOURCE_DISABLED_FALLBACK_BUDGET);
        assert!(!off.enabled);

        let off = resolve(
            MemoryGateFlags {
                disabled: true,
                ..flags()
            },
            Mode::Query,
            &on,
            host(),
            true,
        )
        .expect("resolves");
        assert_eq!(off.source, SOURCE_DISABLED_FLAG);
        assert!(!off.enabled);

        let off = resolve(flags(), Mode::Query, &on, host(), false).expect("resolves");
        assert_eq!(off.source, SOURCE_NOT_JEMALLOC);
        assert!(!off.enabled);

        // A host whose memory could not be read, with the budget set by
        // flag so the fallback-budget reason does not fire first.
        let unknown = HostProfile::new(8, None, None, None, None, None);
        let on_unknown = performance(Mode::Query, 27 * GB, unknown);
        assert_eq!(on_unknown.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
        let off = resolve(flags(), Mode::Query, &on_unknown, unknown, true).expect("resolves");
        assert_eq!(off.source, SOURCE_DISABLED_NO_HOST_MEMORY);
        assert!(!off.enabled);
        assert_eq!(off.host_memory_bytes, 0);

        // Under a cgroup limit the sampler reads the cgroup files.
        let limited = HostProfile::new(
            8,
            Some(16 * GIB),
            Some(32 * GIB),
            Some(16 * GIB),
            None,
            None,
        );
        let on_limited = performance(Mode::Query, 8 * GB, limited);
        let gate = resolve(flags(), Mode::Query, &on_limited, limited, true).expect("resolves");
        assert!(gate.enabled);
        assert_eq!(gate.host_memory_source, HostMemorySource::Cgroup);
        assert_eq!(gate.host_memory_bytes, 16 * GIB);
        assert_eq!(gate.min_available_bytes, 512 * MIB);
    }

    #[test]
    fn a_short_pass_sleeps_the_rest_of_the_interval() {
        let interval = Duration::from_millis(100);
        assert_eq!(
            next_sleep(interval, Duration::from_millis(10)),
            Duration::from_millis(90)
        );
        // Past half the interval the sleep is still the remainder, not a
        // floor: a 99 ms pass keeps the 100 ms period.
        assert_eq!(
            next_sleep(interval, Duration::from_millis(99)),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn a_pass_that_uses_the_whole_interval_still_sleeps_half_of_it() {
        let interval = Duration::from_millis(10);
        assert_eq!(
            next_sleep(interval, Duration::from_millis(10)),
            Duration::from_millis(5)
        );
        assert_eq!(
            next_sleep(interval, Duration::from_millis(15)),
            Duration::from_millis(5)
        );
    }

    #[test]
    fn an_off_gate_starts_no_sampler_and_never_sets_the_gate() {
        for (mode, fallback) in [(Mode::Gateway, false), (Mode::Query, true)] {
            let mut performance = performance(mode, 27 * GB, host());
            if fallback {
                performance.sources.memory_budget_bytes = PERF_SOURCE_FALLBACK;
            }
            let gate = resolve(flags(), mode, &performance, host(), true).expect("resolves");
            assert!(!gate.enabled);
            let budget = Arc::new(MemoryBudget::new(GIB));
            assert!(!start_sampler(&gate, budget.clone()).expect("no spawn to fail"));
            std::thread::sleep(Duration::from_millis(3 * DEFAULT_INTERVAL_MS));
            assert!(budget.gate_open());
            assert_eq!(budget.gate_available(), 0);
            assert_eq!(budget.gate_floor(), 0);
            assert_eq!(STATS.snapshot().purge_floor_bytes, 0);
            budget.try_reserve(1).expect("an open gate admits");
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLog(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("log buffer lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The stamp is one INFO line carrying every resolved field once.
    #[test]
    fn the_stamp_logs_every_field_once() {
        let gate = resolve(
            flags(),
            Mode::Query,
            &performance(Mode::Query, 27 * GB, host()),
            host(),
            true,
        )
        .expect("resolves");
        let log = CapturedLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || gate.emit());

        let logged = String::from_utf8(log.0.lock().expect("log buffer lock").clone())
            .expect("log bytes are utf-8");
        let lines: Vec<&str> = logged
            .lines()
            .filter(|line| line.contains("memory gate resolved"))
            .collect();
        assert_eq!(lines.len(), 1, "{logged}");
        for field in [
            "INFO",
            "enabled=true",
            &format!("host_memory_bytes={}", 32 * GIB),
            &format!("min_available_bytes={FLOOR_32G}"),
            &format!("purge_available_bytes={PURGE_FLOOR_32G}"),
            "purge_min_interval_ms=5000",
            "interval_ms=100",
            "wait_ms=2000",
            &format!("memory_budget_bytes={}", 27 * GB),
            "source=\"default\"",
        ] {
            assert_eq!(
                lines[0].matches(field).count(),
                1,
                "{field} in {}",
                lines[0]
            );
        }
    }
}
