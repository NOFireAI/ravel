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

use std::collections::BTreeMap;
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

/// The largest pipeline depth a load derives. A depth is a count of writes
/// held in flight at once, each with its own task and ack, so a tiny
/// `--batch-rows` on many shards must not turn the rows bound into tens of
/// thousands of them. `--pipeline-depth` is not capped.
pub const MAX_DERIVED_PIPELINE_DEPTH: usize = 1_024;

/// Called once a load's geometry is resolved, before its first write is
/// submitted, so a caller can report it while the load runs.
pub type GeometryHook = Arc<dyn Fn(&LoadGeometry) + Send + Sync>;

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
    /// [`MAX_DERIVED_PIPELINE_DEPTH`], which was below both bounds.
    Cap,
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
    /// Whether the loader runs the depth flush (see [`DepthWindow`]). Only a
    /// derived target turns it on: with an explicit `--target-bytes` the
    /// buffers close on the size and age triggers and the loader's own
    /// flushes alone, whatever `--pipeline-depth` says.
    pub fn depth_flush_enabled(&self) -> bool {
        self.target_source == TargetSource::Derived && self.target_bytes > PER_BATCH_TARGET_BYTES
    }
}

/// Resolve the target and depth for a load once its first non-empty batch is
/// decoded: `first_rows` rows of the object (log records, spans or metric
/// points) that the router estimates at `first_est_bytes`. An explicit value is kept as given; a missing one is
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
        (None, Some(m)) if m < rows_depth && m <= MAX_DERIVED_PIPELINE_DEPTH => {
            (m, DepthBound::Memory)
        }
        (None, _) if rows_depth <= MAX_DERIVED_PIPELINE_DEPTH => (rows_depth, DepthBound::Rows),
        (None, _) => (MAX_DERIVED_PIPELINE_DEPTH, DepthBound::Cap),
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

/// How long, on the loader's clock, [`settle_shards`] waits for the shard
/// actors to handle every enqueued write before the depth window gives up
/// realigning on size flushes.
pub(super) const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The shard skew figures [`settle_shards`] reads, after the test override
/// when one is in scope.
#[cfg(test)]
pub(super) type SkewOverride = Arc<
    dyn Fn(Vec<(u32, ravel_ingest::ShardSkewStats)>) -> Vec<(u32, ravel_ingest::ShardSkewStats)>
        + Send
        + Sync,
>;

#[cfg(test)]
tokio::task_local! {
    /// Rewrites the figures [`settle_shards`] reads, so a test can hold a
    /// shard short of handling its writes.
    pub(super) static SKEW_OVERRIDE: SkewOverride;
}

/// Wait until every shard actor has handled every write enqueued so far, and
/// return each shard's enqueued count. `None` when that has not happened
/// within [`SETTLE_TIMEOUT`] on `clock`.
///
/// A shard actor counts a write processed only after `handle_write` returns,
/// and a write's size flush is opened (and counted) inside `handle_write`, so
/// once processed has caught up with enqueued the router's size-flush
/// counters cover every write submitted so far ([`DepthWindow`] covers the
/// hand-back flushes this does not wait for). The wait is one batch's merge on the
/// shard actors, not a flush: the flush runs in a task of its own.
pub(super) async fn settle_shards(
    clock: &dyn Clock,
    skew: impl Fn() -> Vec<(u32, ravel_ingest::ShardSkewStats)>,
) -> Option<BTreeMap<u32, u64>> {
    let timeout_ns = i64::try_from(SETTLE_TIMEOUT.as_nanos()).unwrap_or(i64::MAX);
    let deadline_ns = clock.now_ns().saturating_add(timeout_ns);
    let mut round = 0u32;
    loop {
        let stats = skew();
        #[cfg(test)]
        let stats = match SKEW_OVERRIDE.try_with(Arc::clone) {
            Ok(rewrite) => rewrite(stats),
            Err(_) => stats,
        };
        if stats
            .iter()
            .all(|(_, s)| s.messages_processed >= s.messages_enqueued)
        {
            return Some(
                stats
                    .into_iter()
                    .map(|(shard, s)| (shard, s.messages_enqueued))
                    .collect(),
            );
        }
        if clock.now_ns() >= deadline_ns {
            return None;
        }
        round = round.saturating_add(1);
        if round < 64 {
            tokio::task::yield_now().await;
        } else {
            clock.sleep(Duration::from_millis(1)).await;
        }
    }
}

/// The router state the depth window reads after each submit, once
/// [`settle_shards`] returned: each shard's enqueued writes and the size
/// flushes it has opened so far.
pub(super) struct ShardObservation {
    pub(super) enqueued: BTreeMap<u32, u64>,
    pub(super) size_flushes: BTreeMap<u32, u64>,
}

/// Decides when the loader flushes every shard. A shard's buffer is pending
/// from the first batch that reached it after its last flush; once the oldest
/// pending batch on any shard is `pipeline_depth` batches old, the loader
/// flushes every shard. At the rows-bound depth that is one target's worth of
/// rows per shard, so a buffer the size trigger has not closed by then (the
/// first batch over-estimated the rest) is closed here rather than by the age
/// trigger. It also runs no later than the loop's wait on the oldest
/// in-flight write, which is `pipeline_depth` batches back, so that wait never
/// sits on a buffer nothing will flush.
///
/// A size flush clears its own shard only, so that shard's next window starts
/// at its next batch while every other shard keeps its oldest pending batch:
/// the depth flush never closes a buffer the size trigger has just emptied.
/// A write's size flush is counted before the write is, so one counted after
/// a batch was handled covers that batch. A hand-back from another shard
/// generation (`handle_hand_back`) also opens size flushes, and is not
/// counted as enqueued, so settling does not wait for it; that arises only
/// when the tenant's shard generation changes during the load. A hand-back
/// flush observed after a later batch merged restarts that shard's window
/// one batch late, and because the depth flush and the loop's oldest-ack
/// wait are in lockstep (one batch of slack is the whole margin), the loop
/// then waits on an ack that only the age trigger answers: the load stalls
/// once for up to `--max-flush-delay`, and nothing in the summary says so.
/// Age and stall flushes clear nothing.
///
/// When the shards have not handled a batch within [`SETTLE_TIMEOUT`] the
/// window gives up realigning for the rest of the load: it stops observing
/// the router, size flushes no longer clear anything, and the depth flush
/// may come early, never late. The load reports that it gave up
/// ([`DepthWindow::realign_gave_up`]).
pub(super) struct DepthWindow {
    depth: u64,
    enabled: bool,
    submits: u64,
    oldest_pending: BTreeMap<u32, u64>,
    enqueued: BTreeMap<u32, u64>,
    size_per_shard: BTreeMap<u32, u64>,
    /// Cleared once an observation is missing, after which size flushes no
    /// longer clear anything.
    realign: bool,
    realign_gave_up: u64,
}

/// The `oldest_pending` key for batches whose shards are unknown.
const ANY_SHARD: u32 = u32::MAX;

impl DepthWindow {
    pub(super) fn new(geometry: &LoadGeometry) -> Self {
        Self {
            depth: geometry.pipeline_depth.max(1) as u64,
            enabled: geometry.depth_flush_enabled(),
            submits: 0,
            oldest_pending: BTreeMap::new(),
            enqueued: BTreeMap::new(),
            size_per_shard: BTreeMap::new(),
            realign: true,
            realign_gave_up: 0,
        }
    }

    /// Whether the depth flush runs at all.
    pub(super) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Whether the loader still settles the shards and observes the router
    /// after each submit. `false` once the window gave up realigning.
    pub(super) fn realigning(&self) -> bool {
        self.enabled && self.realign
    }

    /// How many times the window gave up realigning on size flushes: 0, or 1
    /// once [`settle_shards`] timed out, since the loader stops settling then.
    pub(super) fn realign_gave_up(&self) -> u64 {
        self.realign_gave_up
    }

    /// Record one submitted batch and the router state once it was handled
    /// (`None` when [`settle_shards`] timed out or was not run); `true` when
    /// the depth flush is due now.
    pub(super) fn submitted(&mut self, observed: Option<ShardObservation>) -> bool {
        if !self.enabled {
            return false;
        }
        self.submits += 1;
        let batch = self.submits;
        match observed {
            Some(observed) if self.realign => self.realign_to(batch, observed),
            _ => {
                if std::mem::replace(&mut self.realign, false) {
                    self.realign_gave_up += 1;
                }
                self.oldest_pending.entry(ANY_SHARD).or_insert(batch);
            }
        }
        let due = self
            .oldest_pending
            .values()
            .min()
            .is_some_and(|&oldest| batch - oldest + 1 >= self.depth);
        if due {
            self.oldest_pending.clear();
        }
        due
    }

    fn realign_to(&mut self, batch: u64, observed: ShardObservation) {
        for (&shard, &count) in &observed.enqueued {
            if count > self.enqueued.get(&shard).copied().unwrap_or(0) {
                self.oldest_pending.entry(shard).or_insert(batch);
            }
        }
        self.enqueued = observed.enqueued;
        for (&shard, &count) in &observed.size_flushes {
            if count > self.size_per_shard.get(&shard).copied().unwrap_or(0) {
                self.oldest_pending.remove(&shard);
            }
        }
        self.size_per_shard = observed.size_flushes;
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
    /// Flushes opened by a strict-arrival write (`FlushTrigger::Arrival`),
    /// kept apart from `age`. Always 0 on a load: the loader writes
    /// `WriteMode::Strict` only. Carried so the report shape matches the
    /// ingest counters.
    pub arrival: u64,
    /// Flushes the loader opened because the oldest batch pending on some
    /// shard was `pipeline_depth` batches old ([`DepthWindow`]).
    pub depth: u64,
    /// Every other loader-requested flush: the final drain at load close and
    /// the logs loader's budget-stall flushes.
    pub manual: u64,
}

impl FlushMixCounts {
    /// The flushes-opened count, the sum of the five disjoint causes. On a
    /// load that abandons nothing this equals the objects written.
    pub fn total(&self) -> u64 {
        self.size + self.age + self.arrival + self.depth + self.manual
    }

    /// The trigger that opened the most flushes, `None` when nothing flushed.
    /// Ties resolve in the order size, age, arrival, depth, manual.
    pub fn dominant(&self) -> Option<&'static str> {
        let causes = [
            ("size", self.size),
            ("age", self.age),
            ("arrival", self.arrival),
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
    /// `None` when the load decoded no rows, on any signal, so nothing was
    /// resolved.
    pub geometry: Option<LoadGeometry>,
    pub flush_mix: FlushMixReport,
    pub target_bytes_no_effect_warning: Option<String>,
    /// 1 when the depth window gave up realigning on size flushes because the
    /// shards had not handled a batch within the settle timeout (30 s on the
    /// loader's clock), 0 otherwise.
    #[serde(default)]
    pub realign_gave_up: u64,
}

/// The prefix of the summary line carrying [`LoadLayoutReport`] as JSON.
pub const LOAD_REPORT_JSON_PREFIX: &str = "load report json : ";

/// What chose the flush target, as the summary and the geometry line print
/// it. A derived target prints its per-row figure as the exact decimal
/// `target_bytes / DEFAULT_OBJECT_ROWS`, so the printed product is the
/// printed target.
fn target_source_text(g: &LoadGeometry) -> String {
    match g.target_source {
        TargetSource::Flag => "--target-bytes".to_string(),
        TargetSource::Derived => {
            let rows = DEFAULT_OBJECT_ROWS;
            let whole = g.target_bytes / rows;
            // 1,000,000 / DEFAULT_OBJECT_ROWS is a whole number, so six
            // decimal places hold the remainder exactly.
            let millionths = (g.target_bytes % rows) * (1_000_000 / rows);
            let per_row = if millionths == 0 {
                whole.to_string()
            } else {
                let frac = format!("{millionths:06}");
                format!("{whole}.{}", frac.trim_end_matches('0'))
            };
            format!("derived: {rows} rows x {per_row} estimated bytes per row")
        }
    }
}

/// What decided the pipeline depth, naming every bound that was weighed.
fn depth_bound_text(g: &LoadGeometry) -> String {
    let memory_depth = g
        .memory_depth
        .map_or_else(|| "no budget".to_string(), |d| d.to_string());
    match g.depth_bound {
        DepthBound::Flag => "--pipeline-depth".to_string(),
        DepthBound::Rows => format!(
            "rows bound decided it: rows bound {}, memory bound {memory_depth}",
            g.rows_depth
        ),
        DepthBound::Memory => format!(
            "memory bound decided it: memory bound {memory_depth}, rows bound {}",
            g.rows_depth
        ),
        DepthBound::Cap => format!(
            "cap decided it: cap {MAX_DERIVED_PIPELINE_DEPTH}, rows bound {}, memory bound \
             {memory_depth}",
            g.rows_depth
        ),
    }
}

/// Whether the depth window kept realigning on size flushes, as the summary
/// prints it.
fn realign_text(gave_up: u64) -> String {
    if gave_up == 0 {
        "per shard on size flushes".to_string()
    } else {
        format!(
            "gave up {gave_up} time(s): the shards had not handled a batch within {} s, so \
             later depth flushes counted batches without size flushes and may have closed \
             objects early",
            SETTLE_TIMEOUT.as_secs()
        )
    }
}

/// The prefix of the line a load prints on stderr once its geometry is
/// resolved, before its first write.
pub const GEOMETRY_LINE_PREFIX: &str = "load geometry: ";

/// The one-line geometry report printed as soon as the geometry is resolved:
/// the target and the depth, with what decided each.
pub fn render_geometry_line(signal: &str, g: &LoadGeometry) -> String {
    format!(
        "{GEOMETRY_LINE_PREFIX}{signal}: flush target {} bytes ({}), pipeline depth {} ({})",
        g.target_bytes,
        target_source_text(g),
        g.pipeline_depth,
        depth_bound_text(g),
    )
}

/// The layout lines of an end-of-load summary: the target and depth with
/// what chose each, the trigger mix (per shard where the router counts per
/// shard), the no-effect warning when it applies, and the whole report as
/// one JSON line after [`LOAD_REPORT_JSON_PREFIX`].
pub fn render_layout_summary(report: &LoadLayoutReport) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    if let Some(g) = &report.geometry {
        let _ = writeln!(
            out,
            "  flush target     : {} bytes ({})",
            g.target_bytes,
            target_source_text(g)
        );
        let _ = writeln!(
            out,
            "  pipeline depth   : {} ({})",
            g.pipeline_depth,
            depth_bound_text(g)
        );
        if g.depth_flush_enabled() {
            let _ = writeln!(
                out,
                "  depth realign    : {}",
                realign_text(report.realign_gave_up)
            );
        }
    }
    let t = &report.flush_mix.totals;
    let _ = writeln!(
        out,
        "  flush triggers   : size {}, age {}, arrival {}, depth {}, manual {} (total {})",
        t.size,
        t.age,
        t.arrival,
        t.depth,
        t.manual,
        t.total(),
    );
    for s in &report.flush_mix.shards {
        let _ = writeln!(
            out,
            "    shard {:>3}      : size {}, age {}, arrival {}, depth {}, manual {} (total {})",
            s.shard,
            s.counts.size,
            s.counts.age,
            s.counts.arrival,
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

    /// One shard that received `enqueued` writes and opened `size` size
    /// flushes so far.
    fn one_shard(enqueued: u64, size: u64) -> Option<ShardObservation> {
        Some(ShardObservation {
            enqueued: BTreeMap::from([(0, enqueued)]),
            size_flushes: BTreeMap::from([(0, size)]),
        })
    }

    #[test]
    fn depth_window_fires_every_depth_batches_without_size_flushes() {
        // Derived target, depth 3.
        let g = resolve_geometry(None, Some(3), 1, 10, 10, 10, None);
        let mut w = DepthWindow::new(&g);
        let fired: Vec<bool> = (1..=7).map(|n| w.submitted(one_shard(n, 0))).collect();
        assert_eq!(fired, [false, false, true, false, false, true, false]);
    }

    #[test]
    fn depth_window_is_off_for_an_explicit_target() {
        for target in [PER_BATCH_TARGET_BYTES, 1_000, 1_850_000_000] {
            let g = resolve_geometry(Some(target), Some(3), 1, 10, 10, 10, None);
            assert!(!g.depth_flush_enabled());
            let mut w = DepthWindow::new(&g);
            assert!(!w.enabled());
            assert!((1..=7).all(|n| !w.submitted(one_shard(n, 0))));
        }
    }

    #[test]
    fn a_size_flush_restarts_the_window_at_the_next_batch() {
        let g = resolve_geometry(None, Some(3), 1, 10, 10, 10, None);
        let mut w = DepthWindow::new(&g);
        // The size trigger closes the buffer on batch 2, so batch 3 starts
        // the next window and batch 5 is its third batch.
        let size = [0, 1, 1, 1, 1, 1];
        let fired: Vec<bool> = (1..=6)
            .map(|n| w.submitted(one_shard(n, size[n as usize - 1])))
            .collect();
        assert_eq!(fired, [false, false, false, false, true, false]);
    }

    #[test]
    fn a_size_flush_restarts_only_its_own_shard() {
        let g = resolve_geometry(None, Some(3), 2, 10, 10, 10, None);
        let both = |enqueued: u64, size0: u64| {
            Some(ShardObservation {
                enqueued: BTreeMap::from([(0, enqueued), (1, enqueued)]),
                size_flushes: BTreeMap::from([(0, size0)]),
            })
        };
        // Shard 0 size-flushes on batch 2; shard 1 is still pending from
        // batch 1, so the window fires on batch 3, not on batch 4.
        let mut w = DepthWindow::new(&g);
        let fired: Vec<bool> = [(1, 0), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]
            .into_iter()
            .map(|(n, size)| w.submitted(both(n, size)))
            .collect();
        assert_eq!(fired, [false, false, true, false, false, true]);
    }

    #[test]
    fn a_missing_observation_falls_back_to_counting_batches() {
        let g = resolve_geometry(None, Some(3), 1, 10, 10, 10, None);
        let mut w = DepthWindow::new(&g);
        assert!(!w.submitted(one_shard(1, 0)));
        assert!(w.realigning());
        assert!(!w.submitted(None));
        assert!(!w.realigning());
        assert_eq!(w.realign_gave_up(), 1);
        // Size flushes no longer clear anything once one was missed, and the
        // give-up is counted once.
        assert!(w.submitted(one_shard(3, 1)));
        assert!(!w.submitted(None));
        assert_eq!(w.realign_gave_up(), 1);
    }

    #[tokio::test]
    async fn settle_gives_up_on_the_injected_clock() {
        use super::super::test_support::{NOW_NS, TestClock, yield_until_router_is_quiet};
        let clock = TestClock::new(NOW_NS);
        let stuck = || {
            vec![(
                0,
                ravel_ingest::ShardSkewStats {
                    messages_enqueued: 1,
                    ..Default::default()
                },
            )]
        };
        let settle = settle_shards(clock.as_ref(), stuck);
        tokio::pin!(settle);
        tokio::select! {
            biased;
            _ = &mut settle => panic!("a shard that never handles its write cannot settle"),
            () = yield_until_router_is_quiet(&clock) => {}
        }
        clock.advance_ns(i64::try_from(SETTLE_TIMEOUT.as_nanos()).unwrap());
        assert_eq!(settle.await, None);
    }

    #[test]
    fn a_derived_depth_is_capped() {
        // ceil(250,000 x 4 / 1) = 1,000,000 batches.
        let g = resolve_geometry(None, None, 4, 1, 1, 100, Some(1 << 40));
        assert_eq!(g.rows_depth, 1_000_000);
        assert_eq!(g.pipeline_depth, MAX_DERIVED_PIPELINE_DEPTH);
        assert_eq!(g.depth_bound, DepthBound::Cap);
        assert!(
            render_geometry_line("logs", &g).contains(
                "pipeline depth 1024 (cap decided it: cap 1024, rows bound 1000000, memory \
                 bound"
            ),
            "{}",
            render_geometry_line("logs", &g)
        );
        // An explicit depth is kept above the cap.
        let g = resolve_geometry(None, Some(4_096), 4, 1, 1, 100, None);
        assert_eq!(g.pipeline_depth, 4_096);
    }

    #[test]
    fn the_printed_per_row_figure_times_the_rows_is_the_target() {
        // 3 rows estimated at 1,000 bytes: 333.333... per row, target
        // floor(250,000 x 1,000 / 3) = 83,333,333.
        let g = resolve_geometry(None, None, 1, 3, 3, 1_000, None);
        assert_eq!(g.target_bytes, 83_333_333);
        let line = render_geometry_line("logs", &g);
        assert!(
            line.contains(
                "flush target 83333333 bytes (derived: 250000 rows x 333.333332 \
                 estimated bytes per row)"
            ),
            "{line}"
        );
        let g = resolve_geometry(None, None, 1, 10_000, 10_000, 1_230_000, None);
        assert!(
            render_geometry_line("logs", &g).contains("250000 rows x 123 estimated"),
            "{}",
            render_geometry_line("logs", &g)
        );
    }

    #[test]
    fn dominant_trigger_breaks_ties_in_declared_order() {
        let c = FlushMixCounts {
            size: 2,
            age: 0,
            arrival: 0,
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
