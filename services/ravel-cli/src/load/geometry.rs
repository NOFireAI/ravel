//! Default object geometry (issue #2753): the flush target and pipeline depth
//! a load derives when `--target-bytes` and `--pipeline-depth` are unset, the
//! depth-triggered flush that keeps a derived window moving, and the
//! serializable per-load flush report every signal prints.
//!
//! The router's size trigger compares a shard buffer's estimated uncompressed
//! object content against `target_bytes`, so a row-count target has to be
//! translated into that unit. The loader reads it off the first decoded
//! batch: `ravel_ingest`'s `est_*_object_bytes` functions are the estimates
//! the shard actors themselves sum, so `DEFAULT_OBJECT_ROWS` rows of the same
//! shape reach the derived target exactly when the size trigger fires.

use std::sync::Arc;

use tokio::sync::Semaphore;

use super::*;

/// Rows per shard object a load aims for when `--target-bytes` is unset.
pub const DEFAULT_OBJECT_ROWS: usize = 250_000;

/// `--max-flush-delay` when the flag is unset, on every signal. Long enough
/// that the depth flush and the size trigger, not the age trigger, decide a
/// bulk load's object layout.
pub const DEFAULT_MAX_FLUSH_DELAY: Duration = Duration::from_secs(60);

/// `--target-bytes 1`: every Strict write flushes inside its own
/// `handle_write`, so one batch is one object per involved shard. The layout
/// every signal produced before issue #2753 made the target derived.
///
/// Any target at or below one batch's per-shard slice gives the same layout:
/// the size check runs once per write, after the whole slice has merged, so
/// the first write into an empty buffer already exceeds it. See
/// [`target_bytes_no_effect_warning`].
pub const PER_BATCH_TARGET_BYTES: usize = 1;

/// What the caller asked for before the first batch is decoded: each lever
/// `None` is derived by [`resolve_geometry`], and the memory request bounds a
/// derived depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeometryRequest {
    pub target_bytes: Option<usize>,
    pub pipeline_depth: Option<usize>,
    pub load_memory: LoadMemoryRequest,
}

impl GeometryRequest {
    /// Both levers given, no memory bound: the library entry points'
    /// geometry.
    pub fn explicit(target_bytes: usize, pipeline_depth: usize) -> Self {
        Self {
            target_bytes: Some(target_bytes),
            pipeline_depth: Some(pipeline_depth),
            load_memory: LoadMemoryRequest::Unbudgeted,
        }
    }

    /// Reject a lever given as `0`, which cannot express what the flag means.
    pub(super) fn validate(&self) -> Result<(), LoadError> {
        if self.pipeline_depth == Some(0) {
            return Err(LoadError::Setup(
                "--pipeline-depth must be at least 1 (the number of concurrent in-flight \
                 writes); 0 was given"
                    .to_string(),
            ));
        }
        if self.target_bytes == Some(0) {
            return Err(LoadError::Setup(
                "--target-bytes must be at least 1 (1 flushes every batch as its own object); 0 \
                 was given"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// The memory budget a metrics or spans load bounds a derived depth by:
    /// one read cursor and `concurrent_flushes` flushes outside it. Those
    /// loads charge nothing to it.
    pub(super) fn sequential_memory_budget(
        &self,
        concurrent_flushes: u64,
    ) -> Result<Option<u64>, LoadError> {
        LoadMemory::resolve(self.load_memory, 1, concurrent_flushes)
            .map(|memory| memory.map(|m| m.budget_bytes))
            .map_err(LoadError::Setup)
    }
}

/// Where a load's flush target came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetSource {
    /// `--target-bytes` as given.
    #[default]
    Flag,
    /// [`DEFAULT_OBJECT_ROWS`] times the first batch's estimated bytes per row.
    Derived,
}

/// What decided a load's pipeline depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepthBound {
    /// `--pipeline-depth` as given.
    #[default]
    Flag,
    /// `ceil(DEFAULT_OBJECT_ROWS * shards / batch_rows)`: enough batches in
    /// flight for every shard to fill one target.
    Rows,
    /// The memory budget divided by one batch's estimated bytes, which was
    /// below the rows bound.
    Memory,
}

/// The flush target and pipeline depth a load ran with, and how each was
/// chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LoadGeometry {
    pub target_bytes: usize,
    pub target_source: TargetSource,
    /// The first decoded batch's estimated object bytes per row, rounded up.
    pub est_bytes_per_row: u64,
    pub pipeline_depth: usize,
    pub depth_bound: DepthBound,
    /// `ceil(DEFAULT_OBJECT_ROWS * shards / batch_rows)`, at least 1.
    pub rows_depth: usize,
    /// `budget / (batch_rows * est_bytes_per_row)`, at least 1. `None` when
    /// the load had no memory budget.
    pub memory_depth: Option<usize>,
}

impl LoadGeometry {
    /// Whether the loader flushes every shard each time `pipeline_depth`
    /// batches have been submitted since the last such flush. Off at
    /// [`PER_BATCH_TARGET_BYTES`] or below, where every write already flushes
    /// on its own.
    pub fn depth_flush_enabled(&self) -> bool {
        self.target_bytes > PER_BATCH_TARGET_BYTES
    }
}

/// Resolve the target and depth for a load once its first non-empty batch is
/// decoded: `first_rows` source rows whose records the router estimates at
/// `first_est_bytes`. An explicit value is kept as given; a missing one is
/// derived as the [`DEFAULT_OBJECT_ROWS`] geometry describes.
pub fn resolve_geometry(
    target_bytes: Option<usize>,
    pipeline_depth: Option<usize>,
    shards: u32,
    batch_rows: usize,
    first_rows: usize,
    first_est_bytes: usize,
    memory_budget: Option<u64>,
) -> LoadGeometry {
    let first_rows = first_rows.max(1) as u128;
    let est = first_est_bytes as u128;
    let est_bytes_per_row = u64::try_from(est.div_ceil(first_rows))
        .unwrap_or(u64::MAX)
        .max(1);
    let (target_bytes, target_source) = match target_bytes {
        Some(t) => (t, TargetSource::Flag),
        None => {
            let derived = (DEFAULT_OBJECT_ROWS as u128).saturating_mul(est) / first_rows;
            (
                usize::try_from(derived).unwrap_or(usize::MAX).max(1),
                TargetSource::Derived,
            )
        }
    };
    let batch_rows_u = batch_rows.max(1);
    let rows_depth = DEFAULT_OBJECT_ROWS
        .saturating_mul(shards.max(1) as usize)
        .div_ceil(batch_rows_u)
        .max(1);
    let memory_depth = memory_budget.map(|budget| {
        let batch_bytes = (batch_rows_u as u128).saturating_mul(est_bytes_per_row as u128);
        usize::try_from(budget as u128 / batch_bytes.max(1))
            .unwrap_or(usize::MAX)
            .max(1)
    });
    let (pipeline_depth, depth_bound) = match (pipeline_depth, memory_depth) {
        (Some(d), _) => (d, DepthBound::Flag),
        (None, Some(m)) if m < rows_depth => (m, DepthBound::Memory),
        (None, _) => (rows_depth, DepthBound::Rows),
    };
    LoadGeometry {
        target_bytes,
        target_source,
        est_bytes_per_row,
        pipeline_depth,
        depth_bound,
        rows_depth,
        memory_depth,
    }
}

/// Counts submits that have not yet reached their shards' channels, so the
/// depth flush can wait until every submitted write is enqueued ahead of the
/// `FlushNow` it sends. Each shard's channel is FIFO, so a write enqueued
/// before the flush request is merged into the buffer that request drains.
pub(super) struct EnqueueTracker {
    enqueued: Arc<Semaphore>,
    outstanding: u32,
}

/// Held by a write task until its submit returns, successfully or not.
pub(super) struct EnqueueGuard(Arc<Semaphore>);

impl Drop for EnqueueGuard {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

impl EnqueueTracker {
    pub(super) fn new() -> Self {
        Self {
            enqueued: Arc::new(Semaphore::new(0)),
            outstanding: 0,
        }
    }

    /// A guard for one write about to be submitted.
    pub(super) fn guard(&mut self) -> EnqueueGuard {
        self.outstanding = self.outstanding.saturating_add(1);
        EnqueueGuard(Arc::clone(&self.enqueued))
    }

    /// Wait until every write handed a guard so far has been submitted.
    pub(super) async fn wait_enqueued(&mut self) {
        if self.outstanding == 0 {
            return;
        }
        // The semaphore is never closed, so acquisition cannot fail.
        if let Ok(permits) = self.enqueued.acquire_many(self.outstanding).await {
            permits.forget();
        }
        self.outstanding = 0;
    }
}

/// Counts batches since the last depth flush. Every `pipeline_depth`
/// submitted batches the loader flushes every shard: at the rows-bound depth
/// that is one target's worth of rows per shard, so a shard the size trigger
/// has not released yet is released here rather than by the age trigger,
/// and every write still in the window is answered by the time the flush
/// returns.
pub(super) struct DepthWindow {
    depth: usize,
    enabled: bool,
    since_flush: usize,
}

impl DepthWindow {
    pub(super) fn new(geometry: &LoadGeometry) -> Self {
        Self {
            depth: geometry.pipeline_depth.max(1),
            enabled: geometry.depth_flush_enabled(),
            since_flush: 0,
        }
    }

    /// Record one submitted batch; `true` when the depth flush is due now.
    pub(super) fn submitted(&mut self) -> bool {
        if !self.enabled {
            return false;
        }
        self.since_flush += 1;
        if self.since_flush >= self.depth {
            self.since_flush = 0;
            true
        } else {
            false
        }
    }
}

/// Flush counts split by trigger cause, as a load reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushMixCounts {
    /// Flushes opened because a shard buffer reached the flush target (or
    /// its memory backstop).
    pub size: u64,
    /// Flushes opened because a shard buffer aged past `--max-flush-delay`.
    pub age: u64,
    /// Flushes the loader opened because `pipeline_depth` batches had been
    /// submitted since its last such flush.
    pub depth: u64,
    /// Every other loader-requested flush: the final drain at load close and
    /// the logs loader's budget-stall flushes.
    pub manual: u64,
}

impl FlushMixCounts {
    /// The flushes-opened count, the sum of the four disjoint causes. On a
    /// load that abandons nothing this equals the objects written.
    pub fn total(&self) -> u64 {
        self.size + self.age + self.depth + self.manual
    }

    /// The trigger that opened the most flushes, `None` when nothing flushed.
    /// Ties resolve in the order size, age, depth, manual.
    pub fn dominant(&self) -> Option<&'static str> {
        let causes = [
            ("size", self.size),
            ("age", self.age),
            ("depth", self.depth),
            ("manual", self.manual),
        ];
        let mut best: Option<(&'static str, u64)> = None;
        for (name, count) in causes {
            if count > best.map_or(0, |(_, c)| c) {
                best = Some((name, count));
            }
        }
        best.map(|(name, _)| name)
    }
}

/// One shard's flush trigger mix, keyed by shard index (issue #983).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFlushMix {
    pub shard: u32,
    #[serde(flatten)]
    pub counts: FlushMixCounts,
}

/// A load's flush trigger mix (issue #983). The logs router counts per shard,
/// so a logs load carries one row per shard that flushed; the metrics and
/// spans routers count per router, so theirs carries totals only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushMixReport {
    /// One row per shard that flushed, sorted by shard index. Empty on the
    /// metrics and spans paths.
    pub shards: Vec<ShardFlushMix>,
    /// The counts summed across every shard.
    pub totals: FlushMixCounts,
}

/// The machine-readable layout report every signal's summary ends with: the
/// geometry the load ran with, the trigger mix that produced its objects, and
/// the `--target-bytes` no-effect warning when it applies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadLayoutReport {
    pub signal: String,
    pub objects_written: usize,
    /// `None` when a metrics or spans load decoded no rows, so no router was
    /// built and nothing was resolved.
    pub geometry: Option<LoadGeometry>,
    pub flush_mix: FlushMixReport,
    pub target_bytes_no_effect_warning: Option<String>,
}

/// The prefix of the summary line carrying [`LoadLayoutReport`] as JSON.
pub const LOAD_REPORT_JSON_PREFIX: &str = "load report json : ";

/// The layout lines of an end-of-load summary: the target and depth with
/// what chose each, the trigger mix (per shard where the router counts per
/// shard), the no-effect warning when it applies, and the whole report as
/// one JSON line after [`LOAD_REPORT_JSON_PREFIX`].
pub fn render_layout_summary(report: &LoadLayoutReport) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    if let Some(g) = &report.geometry {
        let target_source = match g.target_source {
            TargetSource::Flag => "--target-bytes".to_string(),
            TargetSource::Derived => format!(
                "derived: {DEFAULT_OBJECT_ROWS} rows x {} estimated bytes per row",
                g.est_bytes_per_row
            ),
        };
        let _ = writeln!(
            out,
            "  flush target     : {} bytes ({target_source})",
            g.target_bytes
        );
        let memory_depth = g
            .memory_depth
            .map_or_else(|| "no budget".to_string(), |d| d.to_string());
        let depth_bound = match g.depth_bound {
            DepthBound::Flag => "--pipeline-depth".to_string(),
            DepthBound::Rows => format!(
                "rows bound decided it: rows bound {}, memory bound {memory_depth}",
                g.rows_depth
            ),
            DepthBound::Memory => format!(
                "memory bound decided it: memory bound {memory_depth}, rows bound {}",
                g.rows_depth
            ),
        };
        let _ = writeln!(
            out,
            "  pipeline depth   : {} ({depth_bound})",
            g.pipeline_depth
        );
    }
    let t = &report.flush_mix.totals;
    let _ = writeln!(
        out,
        "  flush triggers   : size {}, age {}, depth {}, manual {} (total {})",
        t.size,
        t.age,
        t.depth,
        t.manual,
        t.total(),
    );
    for s in &report.flush_mix.shards {
        let _ = writeln!(
            out,
            "    shard {:>3}      : size {}, age {}, depth {}, manual {} (total {})",
            s.shard,
            s.counts.size,
            s.counts.age,
            s.counts.depth,
            s.counts.manual,
            s.counts.total(),
        );
    }
    if let Some(warning) = &report.target_bytes_no_effect_warning {
        let _ = writeln!(out, "  target warning   : {warning}");
    }
    match serde_json::to_string(report) {
        Ok(json) => {
            let _ = writeln!(out, "{LOAD_REPORT_JSON_PREFIX}{json}");
        }
        Err(e) => {
            let _ = writeln!(out, "{LOAD_REPORT_JSON_PREFIX}unavailable: {e}");
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn derived_target_is_default_object_rows_of_the_first_batch() {
        let g = resolve_geometry(None, None, 1, 10_000, 10_000, 1_230_000, None);
        assert_eq!(g.target_bytes, 250_000 * 123);
        assert_eq!(g.target_source, TargetSource::Derived);
        assert_eq!(g.est_bytes_per_row, 123);
        assert_eq!(g.rows_depth, 25);
        assert_eq!(g.pipeline_depth, 25);
        assert_eq!(g.depth_bound, DepthBound::Rows);
        assert_eq!(g.memory_depth, None);
    }

    #[test]
    fn rows_depth_rounds_up_over_every_shard() {
        let g = resolve_geometry(None, None, 4, 30_000, 30_000, 30_000, None);
        // ceil(250_000 * 4 / 30_000) = ceil(33.33) = 34.
        assert_eq!(g.rows_depth, 34);
        let g = resolve_geometry(None, None, 1, 1_000_000, 10, 10, None);
        assert_eq!(g.rows_depth, 1);
    }

    #[test]
    fn memory_bound_wins_only_below_the_rows_bound() {
        // One batch is 10_000 rows * 100 bytes = 1_000_000 bytes.
        let g = resolve_geometry(None, None, 1, 10_000, 10_000, 1_000_000, Some(5_500_000));
        assert_eq!(g.memory_depth, Some(5));
        assert_eq!(g.pipeline_depth, 5);
        assert_eq!(g.depth_bound, DepthBound::Memory);
        let g = resolve_geometry(None, None, 1, 10_000, 10_000, 1_000_000, Some(1 << 40));
        assert_eq!(g.pipeline_depth, 25);
        assert_eq!(g.depth_bound, DepthBound::Rows);
        let g = resolve_geometry(None, None, 1, 10_000, 10_000, 1_000_000, Some(1));
        assert_eq!(g.memory_depth, Some(1));
        assert_eq!(g.pipeline_depth, 1);
    }

    #[test]
    fn explicit_values_are_kept_and_named() {
        let g = resolve_geometry(Some(1), Some(3), 4, 10_000, 10_000, 1_000_000, Some(1));
        assert_eq!(g.target_bytes, 1);
        assert_eq!(g.target_source, TargetSource::Flag);
        assert_eq!(g.pipeline_depth, 3);
        assert_eq!(g.depth_bound, DepthBound::Flag);
        assert!(!g.depth_flush_enabled());
    }

    #[test]
    fn depth_window_fires_every_depth_batches_and_never_per_batch() {
        let mut g = resolve_geometry(Some(1_000), Some(3), 1, 10, 10, 10, None);
        let mut w = DepthWindow::new(&g);
        let fired: Vec<bool> = (0..7).map(|_| w.submitted()).collect();
        assert_eq!(fired, [false, false, true, false, false, true, false]);
        g.target_bytes = PER_BATCH_TARGET_BYTES;
        let mut w = DepthWindow::new(&g);
        assert!((0..7).all(|_| !w.submitted()));
    }

    #[test]
    fn dominant_trigger_breaks_ties_in_declared_order() {
        let c = FlushMixCounts {
            size: 2,
            age: 0,
            depth: 2,
            manual: 1,
        };
        assert_eq!(c.dominant(), Some("size"));
        assert_eq!(FlushMixCounts::default().dominant(), None);
    }

    #[tokio::test]
    async fn wait_enqueued_returns_once_every_guard_dropped() {
        let mut t = EnqueueTracker::new();
        let a = t.guard();
        let b = t.guard();
        let task = tokio::spawn(async move {
            drop(a);
            tokio::task::yield_now().await;
            drop(b);
        });
        t.wait_enqueued().await;
        task.await.unwrap();
        // A second wait with nothing outstanding returns at once.
        t.wait_enqueued().await;
    }
}
