//! The resident memory gate's server side (ADR-2633 task 2, issue #2730):
//! startup resolution and its stamp, the `ravel-memory-gate` sampler thread,
//! and the figures `/metrics` renders for it.
//!
//! The gate state itself lives in [`ravel_memory::MemoryBudget`]; this module
//! is its only writer. When the gate is off the sampler is never started, so
//! [`ravel_memory::MemoryBudget::set_resident_gate`] is never called and the
//! budget's gate stays open with a reading and mark of 0.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ravel_ingest::IngestByteBudgetLimit;
use ravel_memory::MemoryBudget;

use crate::config::{Cli, Mode, PERF_SOURCE_FALLBACK, ResolvedPerformanceDefaults};

/// Default `--memory-gate-high-water-percent`.
pub const DEFAULT_HIGH_WATER_PERCENT: u8 = 70;
/// Default `--memory-gate-interval-ms`.
pub const DEFAULT_INTERVAL_MS: u64 = 100;
/// Default `--memory-gate-wait-ms`.
pub const DEFAULT_WAIT_MS: u64 = 2000;

/// Stamp source: the gate is on at the default mark.
pub const SOURCE_DEFAULT: &str = "default";
/// Stamp source: the gate is on at `--memory-gate-high-water-percent`.
pub const SOURCE_FLAG: &str = "flag";
/// Stamp source: off, because `--mode gateway` claims no memory budget.
pub const SOURCE_DISABLED_GATEWAY: &str = "disabled-gateway";
/// Stamp source: off, because the budget resolved from the fallback and is
/// not a figure to take a fraction of.
pub const SOURCE_DISABLED_FALLBACK_BUDGET: &str = "disabled-fallback-budget";
/// Stamp source: off, because `--disable-memory-gate` was passed.
pub const SOURCE_DISABLED_FLAG: &str = "disabled-flag";
/// Stamp source: off, because this build does not run under jemalloc.
pub const SOURCE_NOT_JEMALLOC: &str = "not-jemalloc";

/// The four memory gate flags, as parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGateFlags {
    pub high_water_percent: Option<u8>,
    pub interval_ms: u64,
    pub wait_ms: u64,
    pub disabled: bool,
}

/// The resolved memory gate, stamped once at startup by [`MemoryGate::emit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGate {
    pub enabled: bool,
    /// The mark the sampler compares `stats.resident` against; 0 when off,
    /// and never passed to the budget then.
    pub high_water_bytes: u64,
    pub high_water_percent: u8,
    /// The resolved `memory_budget_bytes` the mark is a percentage of.
    pub memory_budget_bytes: u64,
    pub interval_ms: u64,
    pub wait_ms: u64,
    /// One of the `SOURCE_*` constants: who set the mark, or why the gate is
    /// off.
    pub source: &'static str,
}

impl MemoryGate {
    /// The startup stamp, beside the `performance default resolved` lines.
    pub fn emit(&self) {
        tracing::info!(
            enabled = self.enabled,
            high_water_bytes = self.high_water_bytes,
            high_water_percent = self.high_water_percent,
            memory_budget_bytes = self.memory_budget_bytes,
            interval_ms = self.interval_ms,
            wait_ms = self.wait_ms,
            source = self.source,
            "memory gate resolved"
        );
    }

    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }
}

/// Startup refusal: the memory that sits inside `stats.resident` whenever the
/// caches and the ingest buffer are full already reaches the mark, so the
/// gate would never open (ADR-2633 section 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGateMarkReached {
    /// `memory_hard_caps_bytes`: the two resolved cache ceilings.
    pub hard_caps_bytes: u64,
    /// The bounded `--max-ingest-buffer-bytes` in `all` mode, else 0.
    pub ingest_buffer_bytes: u64,
    pub high_water_bytes: u64,
    pub high_water_percent: u8,
    pub memory_budget_bytes: u64,
}

impl std::fmt::Display for MemoryGateMarkReached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the cache hard caps ({} bytes) plus the ingest buffer ceiling ({} bytes) total {} \
             bytes, which reaches the memory gate's high-water mark ({} bytes, {}% of \
             memory_budget_bytes {}): with the caches and the ingest buffer full the gate would \
             never open. Lower --cache-max-bytes, --catalog-cache-max-bytes or \
             --max-ingest-buffer-bytes, raise --memory-gate-high-water-percent, or pass \
             --disable-memory-gate",
            self.hard_caps_bytes,
            self.ingest_buffer_bytes,
            self.hard_caps_bytes
                .saturating_add(self.ingest_buffer_bytes),
            self.high_water_bytes,
            self.high_water_percent,
            self.memory_budget_bytes,
        )
    }
}

impl std::error::Error for MemoryGateMarkReached {}

/// `percent`% of `budget`, rounded down.
fn percent_of(budget: u64, percent: u8) -> u64 {
    let mark = u128::from(budget) * u128::from(percent) / 100;
    u64::try_from(mark).unwrap_or(u64::MAX)
}

/// Resolves the gate from its flags and the facts that turn it off. The off
/// reasons are checked in the order the stamp's sources list them: gateway
/// mode, a fallback budget, `--disable-memory-gate`, a build without
/// jemalloc. An enabled gate whose mark the hard caps reach is refused.
pub fn resolve(
    flags: MemoryGateFlags,
    mode: Mode,
    performance: &ResolvedPerformanceDefaults,
    ingest_buffer: IngestByteBudgetLimit,
    jemalloc: bool,
) -> Result<MemoryGate, MemoryGateMarkReached> {
    let high_water_percent = flags
        .high_water_percent
        .unwrap_or(DEFAULT_HIGH_WATER_PERCENT);
    let off = if !mode.uses_memory_budget() {
        Some(SOURCE_DISABLED_GATEWAY)
    } else if performance.sources.memory_budget_bytes == PERF_SOURCE_FALLBACK {
        Some(SOURCE_DISABLED_FALLBACK_BUDGET)
    } else if flags.disabled {
        Some(SOURCE_DISABLED_FLAG)
    } else if !jemalloc {
        Some(SOURCE_NOT_JEMALLOC)
    } else {
        None
    };
    let mut gate = MemoryGate {
        enabled: false,
        high_water_bytes: 0,
        high_water_percent,
        memory_budget_bytes: performance.memory_budget_bytes,
        interval_ms: flags.interval_ms,
        wait_ms: flags.wait_ms,
        source: SOURCE_DEFAULT,
    };
    if let Some(source) = off {
        gate.source = source;
        return Ok(gate);
    }

    let high_water_bytes = percent_of(performance.memory_budget_bytes, high_water_percent);
    let ingest_buffer_bytes = match (mode, ingest_buffer) {
        (Mode::All, IngestByteBudgetLimit::Bounded(bytes)) => bytes,
        _ => 0,
    };
    let held = performance
        .memory_hard_caps_bytes
        .saturating_add(ingest_buffer_bytes);
    if held >= high_water_bytes {
        return Err(MemoryGateMarkReached {
            hard_caps_bytes: performance.memory_hard_caps_bytes,
            ingest_buffer_bytes,
            high_water_bytes,
            high_water_percent,
            memory_budget_bytes: performance.memory_budget_bytes,
        });
    }
    gate.enabled = true;
    gate.high_water_bytes = high_water_bytes;
    gate.source = if flags.high_water_percent.is_some() {
        SOURCE_FLAG
    } else {
        SOURCE_DEFAULT
    };
    Ok(gate)
}

impl Cli {
    /// [`resolve`] from this command line and the resolved performance
    /// defaults, for this build's allocator.
    pub fn resolve_memory_gate(
        &self,
        performance: &ResolvedPerformanceDefaults,
    ) -> anyhow::Result<MemoryGate> {
        let flags = MemoryGateFlags {
            high_water_percent: self.memory_gate_high_water_percent,
            interval_ms: self.memory_gate_interval_ms,
            wait_ms: self.memory_gate_wait_ms,
            disabled: self.disable_memory_gate,
        };
        Ok(resolve(
            flags,
            self.mode,
            performance,
            self.parse_ingest_buffer_budget()?,
            crate::mem_stats::JEMALLOC,
        )?)
    }
}

/// Upper bounds, in microseconds, of the `ravel_memory_gate_purge_seconds`
/// buckets. 50 ms is the ADR-2633 acceptance bound on the p99.
pub const PURGE_BUCKET_BOUNDS_MICROS: [u64; 12] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 1_000_000,
];
/// One bucket per bound plus the overflow bucket.
pub const PURGE_BUCKET_COUNT: usize = PURGE_BUCKET_BOUNDS_MICROS.len() + 1;

/// The sampler's running totals. The process's one instance is [`STATS`];
/// tests build their own.
#[derive(Debug)]
pub struct MemoryGateStats {
    samples: AtomicU64,
    epoch_refresh_nanos: AtomicU64,
    purges: AtomicU64,
    purge_nanos: AtomicU64,
    /// Non-cumulative: bucket `i` counts purges that landed in it alone.
    purge_buckets: [AtomicU64; PURGE_BUCKET_COUNT],
}

/// A scrape-time copy of [`MemoryGateStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryGateStatsSnapshot {
    /// Samples that got a reading.
    pub samples: u64,
    /// Wall time spent in epoch refresh plus `stats.resident` reads, the
    /// re-read after a purge included.
    pub epoch_refresh_nanos: u64,
    pub purges: u64,
    /// Wall time spent purging.
    pub purge_nanos: u64,
    /// Non-cumulative per-bucket purge counts.
    pub purge_buckets: [u64; PURGE_BUCKET_COUNT],
}

impl MemoryGateStats {
    pub const fn new() -> Self {
        Self {
            samples: AtomicU64::new(0),
            epoch_refresh_nanos: AtomicU64::new(0),
            purges: AtomicU64::new(0),
            purge_nanos: AtomicU64::new(0),
            purge_buckets: [const { AtomicU64::new(0) }; PURGE_BUCKET_COUNT],
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
    /// The reading was below the mark; the gate is open.
    Open { resident: u64 },
    /// The reading was at or above the mark, a purge ran, and the gate was
    /// set from the post-purge reading.
    Purged { resident: u64, closed: bool },
    /// No reading; the gate keeps its previous state.
    Unread,
}

/// One sampler pass (ADR-2633 sections 1 and 2): read `resident`; at or above
/// `high_water`, purge once and re-read; then set the gate from the last
/// reading. When the re-read fails the gate is set from the first reading.
pub fn sample_once(
    budget: &MemoryBudget,
    stats: &MemoryGateStats,
    high_water: u64,
    read_resident: &mut impl FnMut() -> Option<u64>,
    purge: &mut impl FnMut(),
) -> Sample {
    let mut timed_read = || {
        let started = Instant::now();
        let reading = read_resident();
        stats.record_epoch_refresh(started.elapsed());
        reading
    };
    let Some(first) = timed_read() else {
        return Sample::Unread;
    };
    stats.samples.fetch_add(1, Ordering::Relaxed);
    if first < high_water {
        budget.set_resident_gate(first, high_water);
        return Sample::Open { resident: first };
    }
    let started = Instant::now();
    purge();
    stats.record_purge(started.elapsed());
    let resident = timed_read().unwrap_or(first);
    budget.set_resident_gate(resident, high_water);
    Sample::Purged {
        resident,
        closed: resident >= high_water,
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
    let high_water = gate.high_water_bytes;
    let interval = gate.interval();
    let spawned = std::thread::Builder::new()
        .name("ravel-memory-gate".to_string())
        .spawn(move || run_sampler(&budget, high_water, interval));
    match spawned {
        Ok(_) => Ok(true),
        Err(err) => {
            SAMPLER_STARTED.store(false, Ordering::Release);
            Err(err)
        }
    }
}

fn run_sampler(budget: &MemoryBudget, high_water: u64, interval: Duration) {
    let mut read_resident = crate::mem_stats::read_resident;
    let mut purge = || {
        crate::mem_stats::purge_arenas();
    };
    let mut warned_unread = false;
    loop {
        let started = Instant::now();
        let sample = sample_once(budget, &STATS, high_water, &mut read_resident, &mut purge);
        if sample == Sample::Unread && !warned_unread {
            warned_unread = true;
            tracing::warn!(
                "memory gate could not read jemalloc stats.resident; the gate keeps its last state \
                 until a reading succeeds"
            );
        }
        std::thread::sleep(interval.saturating_sub(started.elapsed()));
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::{HostProfile, PERF_SOURCE_FLAG};

    const GIB: u64 = 1 << 30;

    fn flags() -> MemoryGateFlags {
        MemoryGateFlags {
            high_water_percent: None,
            interval_ms: DEFAULT_INTERVAL_MS,
            wait_ms: DEFAULT_WAIT_MS,
            disabled: false,
        }
    }

    /// Resolved defaults for a `--memory-budget-bytes` of `budget` with both
    /// cache ceilings set by flag.
    fn performance(
        mode: Mode,
        budget: u64,
        cache: u64,
        catalog: u64,
    ) -> ResolvedPerformanceDefaults {
        let cli = <Cli as clap::Parser>::try_parse_from([
            "ravel-server".to_string(),
            "--mode".to_string(),
            mode_arg(mode).to_string(),
            "--memory-budget-bytes".to_string(),
            budget.to_string(),
            "--cache-max-bytes".to_string(),
            cache.to_string(),
            "--catalog-cache-max-bytes".to_string(),
            catalog.to_string(),
        ])
        .expect("flags parse");
        cli.resolve_performance(host())
            .expect("performance resolves")
    }

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

    #[test]
    fn the_stamp_reports_every_source() {
        let on = performance(Mode::Query, 10 * GIB, GIB, GIB);
        assert_eq!(on.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
        let ingest = IngestByteBudgetLimit::Bounded(GIB / 2);

        let default = resolve(flags(), Mode::Query, &on, ingest, true).expect("resolves");
        assert_eq!(default.source, SOURCE_DEFAULT);
        assert!(default.enabled);
        assert_eq!(default.high_water_percent, 70);
        assert_eq!(default.high_water_bytes, 7 * GIB);

        let flagged = resolve(
            MemoryGateFlags {
                high_water_percent: Some(50),
                ..flags()
            },
            Mode::Query,
            &on,
            ingest,
            true,
        )
        .expect("resolves");
        assert_eq!(flagged.source, SOURCE_FLAG);
        assert_eq!(flagged.high_water_bytes, 5 * GIB);

        let gateway = performance(Mode::Gateway, 10 * GIB, GIB, GIB);
        let off = resolve(flags(), Mode::Gateway, &gateway, ingest, true).expect("resolves");
        assert_eq!(off.source, SOURCE_DISABLED_GATEWAY);
        assert!(!off.enabled);
        assert_eq!(off.high_water_bytes, 0);

        let mut fallback = on;
        fallback.sources.memory_budget_bytes = PERF_SOURCE_FALLBACK;
        let off = resolve(flags(), Mode::Query, &fallback, ingest, true).expect("resolves");
        assert_eq!(off.source, SOURCE_DISABLED_FALLBACK_BUDGET);
        assert!(!off.enabled);

        let off = resolve(
            MemoryGateFlags {
                disabled: true,
                ..flags()
            },
            Mode::Query,
            &on,
            ingest,
            true,
        )
        .expect("resolves");
        assert_eq!(off.source, SOURCE_DISABLED_FLAG);
        assert!(!off.enabled);

        let off = resolve(flags(), Mode::Query, &on, ingest, false).expect("resolves");
        assert_eq!(off.source, SOURCE_NOT_JEMALLOC);
        assert!(!off.enabled);
        assert_eq!(off.high_water_bytes, 0);
    }

    #[test]
    fn hard_caps_and_the_ingest_ceiling_reaching_the_mark_are_refused() {
        // Mark at 70% of 10 GiB = 7 GiB. Caps 3 GiB + ingest 4 GiB = 7 GiB.
        let all = performance(Mode::All, 10 * GIB, 2 * GIB, GIB);
        let refused = resolve(
            flags(),
            Mode::All,
            &all,
            IngestByteBudgetLimit::Bounded(4 * GIB),
            true,
        )
        .expect_err("caps plus ingest reach the mark");
        assert_eq!(refused.hard_caps_bytes, 3 * GIB);
        assert_eq!(refused.ingest_buffer_bytes, 4 * GIB);
        assert_eq!(refused.high_water_bytes, 7 * GIB);
        let message = refused.to_string();
        assert!(
            message.contains(&format!("total {} bytes", 7 * GIB)),
            "{message}"
        );
        assert!(
            message.contains(&format!("high-water mark ({} bytes", 7 * GIB)),
            "{message}"
        );

        // One byte under the mark is accepted.
        resolve(
            flags(),
            Mode::All,
            &all,
            IngestByteBudgetLimit::Bounded(4 * GIB - 1),
            true,
        )
        .expect("one byte under the mark");

        // Outside `all` mode the ingest ceiling does not count.
        let query = performance(Mode::Query, 10 * GIB, 2 * GIB, GIB);
        resolve(
            flags(),
            Mode::Query,
            &query,
            IngestByteBudgetLimit::Bounded(4 * GIB),
            true,
        )
        .expect("query mode holds no ingest buffer");

        // The caps alone reaching the mark are refused too.
        let tight = performance(Mode::Query, 10 * GIB, 4 * GIB, 3 * GIB);
        let refused = resolve(
            flags(),
            Mode::Query,
            &tight,
            IngestByteBudgetLimit::Unlimited,
            true,
        )
        .expect_err("caps reach the mark");
        assert_eq!(refused.ingest_buffer_bytes, 0);

        // An off gate checks nothing.
        resolve(
            MemoryGateFlags {
                disabled: true,
                ..flags()
            },
            Mode::Query,
            &tight,
            IngestByteBudgetLimit::Unlimited,
            true,
        )
        .expect("off gate is not checked");
    }

    #[test]
    fn a_reading_at_the_mark_closes_the_gate_and_one_below_opens_it() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let mut purge = || {};

        let sample = sample_once(&budget, &stats, 100, &mut || Some(100), &mut purge);
        assert_eq!(
            sample,
            Sample::Purged {
                resident: 100,
                closed: true
            }
        );
        assert!(!budget.gate_open());

        let sample = sample_once(&budget, &stats, 100, &mut || Some(99), &mut purge);
        assert_eq!(sample, Sample::Open { resident: 99 });
        assert!(budget.gate_open());
        assert_eq!(budget.gate_high_water(), 100);
    }

    #[test]
    fn the_purge_counter_counts_each_purging_sample() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let mut purges = 0;
        let mut purge = || purges += 1;
        sample_once(&budget, &stats, 100, &mut || Some(50), &mut purge);
        sample_once(&budget, &stats, 100, &mut || Some(150), &mut purge);
        sample_once(&budget, &stats, 100, &mut || Some(200), &mut purge);
        let snapshot = stats.snapshot();
        assert_eq!(purges, 2);
        assert_eq!(snapshot.purges, 2);
        assert_eq!(snapshot.purge_buckets.iter().sum::<u64>(), 2);
        assert_eq!(snapshot.samples, 3);
    }

    #[test]
    fn the_gauge_holds_the_post_purge_reread() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let mut readings = [150, 60].into_iter();
        let mut read = || readings.next();
        let sample = sample_once(&budget, &stats, 100, &mut read, &mut || {});
        assert_eq!(
            sample,
            Sample::Purged {
                resident: 60,
                closed: false
            }
        );
        assert_eq!(budget.gate_resident(), 60);
        assert!(budget.gate_open());
    }

    #[test]
    fn an_unread_sample_leaves_the_gate_alone() {
        let budget = MemoryBudget::new(u64::MAX);
        let stats = MemoryGateStats::new();
        let mut purges = 0;
        let sample = sample_once(&budget, &stats, 100, &mut || None, &mut || purges += 1);
        assert_eq!(sample, Sample::Unread);
        assert_eq!(purges, 0);
        assert_eq!(budget.gate_high_water(), 0);
        assert!(budget.gate_open());
        assert_eq!(stats.snapshot().samples, 0);
    }

    #[test]
    fn an_off_gate_starts_no_sampler_and_never_sets_the_gate() {
        for (mode, fallback) in [(Mode::Gateway, false), (Mode::Query, true)] {
            let mut performance = performance(mode, 10 * GIB, GIB, GIB);
            if fallback {
                performance.sources.memory_budget_bytes = PERF_SOURCE_FALLBACK;
            }
            let gate = resolve(
                flags(),
                mode,
                &performance,
                IngestByteBudgetLimit::Unlimited,
                true,
            )
            .expect("resolves");
            assert!(!gate.enabled);
            let budget = Arc::new(MemoryBudget::new(GIB));
            assert!(!start_sampler(&gate, budget.clone()).expect("no spawn to fail"));
            std::thread::sleep(Duration::from_millis(3 * DEFAULT_INTERVAL_MS));
            assert!(budget.gate_open());
            assert_eq!(budget.gate_resident(), 0);
            assert_eq!(budget.gate_high_water(), 0);
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

    /// The stamp is one INFO line carrying every resolved field.
    #[test]
    fn the_stamp_logs_every_field_once() {
        let gate = resolve(
            flags(),
            Mode::Query,
            &performance(Mode::Query, 10 * GIB, GIB, GIB),
            IngestByteBudgetLimit::Unlimited,
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
            &format!("high_water_bytes={}", 7 * GIB),
            "high_water_percent=70",
            &format!("memory_budget_bytes={}", 10 * GIB),
            "interval_ms=100",
            "wait_ms=2000",
            "source=\"default\"",
        ] {
            assert!(lines[0].contains(field), "{field} in {}", lines[0]);
        }
    }
}
