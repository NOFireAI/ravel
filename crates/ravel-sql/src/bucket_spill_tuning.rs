//! The bucket spill tuning a refusal retry derives from its refused first
//! attempt (ADR-3565 amendment 2, item 4, and ADR-4143 decision 1).
//!
//! The retry plans with [`BucketedFinalAggregateExec`] at a placeholder
//! tuning, reads `P`, `C`, the routed column widths and the scan's `N` from
//! that plan ([`BucketSpillPlanShape::read`]), and derives `bits` and
//! `routed_chunk_rows` from them and the first attempt's figures
//! ([`derive_bucket_spill_tuning`]). With:
//!
//! - `T = A x N / n`, the projected table bytes;
//! - `S = (L - H) / C`, the per-stream share, `H` the spill-mode headroom;
//! - `table(b) = ceil(2 x T / (P x 2^b))`, one bucket's table with a
//!   margin of 2;
//! - `b(c)`, the bytes a routed chunk of `c` rows is charged (each column's
//!   values capacity rounded up to 64 bytes, plus its validity bitmap);
//! - `bits`, the least `b` with `2 x T / (P x 2^b) <= 0.75 x S` and
//!   `table(b) + (2^b + 1) x b(1024) <= S`;
//! - `routed_chunk_rows`, the most rows `c` up to 8192 with
//!   `(2^bits + 1) x b(c) <= S - table(bits)`: the open chunks and the
//!   read-back chunk take what the table leaves.
//!
//! A route with a string or binary column (ADR-4143 decision 3) budgets
//! its chunks in bytes instead: `b(1024)` above is `B_min`, 16 KiB, and the
//! derivation returns `routed_chunk_bytes`, `B = floor((S - table(bits)) /
//! (2^bits + 1))`, with `routed_chunk_rows` at 8192, the row count at which
//! such a chunk seals whatever its bytes.
//!
//! Any condition in [`NoTuning`] keeps the retry's sort-merge plan.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::buffer::BufferExec;
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use crate::bucketed_final_aggregate::{
    BUCKETED_FINAL_AGGREGATE_MAX_BITS, BucketedFinalAggregateExec, RoutedLayout, routed_types,
};
use crate::config::BucketedFinalAggregation;
use crate::logs_scan::LogsScanExec;
use crate::memory::spill_mode_headroom;
use crate::spill::{MAX_SPILL_SHARES, SPILL_SHARE_UNIT_BYTES};

/// The largest `routed_chunk_rows` the derivation returns.
pub const BUCKET_SPILL_MAX_CHUNK_ROWS: usize = 8192;

/// The smallest `routed_chunk_rows` the derivation returns; below it the
/// retry keeps its sort-merge plan.
pub const BUCKET_SPILL_MIN_CHUNK_ROWS: usize = 1024;

/// `B_min`: the smallest byte budget the derivation gives a chunk on a route
/// with a string or binary column (ADR-4143 decision 3). It stands in for
/// `b(1024)` in the `bits` condition.
pub const BUCKET_SPILL_MIN_CHUNK_BYTES: usize = 16 * 1024;

/// The first attempt's and the retry plan's figures the derivation reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSpillInputs {
    /// `A`: the first attempt's peak asked intermediate bytes at its refusal.
    pub asked_bytes: u64,
    /// `N`: the scan's total input rows, before any filter; `None` when the
    /// scan's `sample_count` sum overflowed.
    pub total_rows: Option<u64>,
    /// `n`: the input rows the scan had consumed at the refusal, on `N`'s
    /// basis; `None` when the first attempt reported no counter.
    pub consumed_rows: Option<u64>,
    /// `P`: the retry's final partition count.
    pub partitions: usize,
    /// `C`: the spill-capable consumers the retry plan registers.
    pub spill_consumers: usize,
    /// `L`: the per-query memory limit.
    pub query_limit: usize,
    /// The routed columns' fixed widths in bytes, keys then state; `w` is
    /// their sum. A string or binary column counts its 16-byte view.
    pub column_widths: Vec<usize>,
    /// Whether a routed column is a string or binary type, so the chunks are
    /// budgeted in bytes.
    pub variable_width: bool,
    /// The soft `RLIMIT_NOFILE` read once at server start.
    pub nofile_soft: u64,
    /// `slots`: how many retries can hold scratch at once
    /// ([`spill_slots`]).
    pub slots: u64,
}

/// A derived bucket spill tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketSpillTuning {
    pub bits: u8,
    pub routed_chunk_rows: usize,
    /// `B`, each chunk's byte budget, on a route with a string or binary
    /// column; `None` on a fixed-width route.
    pub routed_chunk_bytes: Option<usize>,
    /// `2 x P x 2^bits + P`, the descriptors one retry can hold open.
    pub descriptor_need: u64,
}

/// Why the derivation returned no tuning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoTuning {
    /// `n` is zero, or the first attempt reported no consumed-rows counter.
    NoConsumedRows,
    /// The plan holds no bucketed final aggregate, more than one, or its
    /// input holds other than one scan.
    NotOneScan,
    /// The one scan is not a [`LogsScanExec`].
    NotLogsScan,
    /// The scan's `sample_count` sum overflowed: there is no exact `N`.
    NoExactTotal,
    /// The plan holds a node the `C` table does not list.
    UnlistedNode(String),
    /// A routed column is neither fixed width nor a string or binary type.
    NotFixedWidth,
    /// A figure is zero or does not fit the arithmetic (`P`, `C`, `w`, or a
    /// share `L - H` of nothing).
    Degenerate,
    /// No `bits` up to [`BUCKETED_FINAL_AGGREGATE_MAX_BITS`] both fits the
    /// table and leaves room for chunks of [`BUCKET_SPILL_MIN_CHUNK_ROWS`].
    TooManyBits,
    /// A routed chunk below [`BUCKET_SPILL_MIN_CHUNK_ROWS`]. The derivation no
    /// longer returns it (ADR-4143 decision 1): a `bits` that leaves no room
    /// for floor-sized chunks is not taken, and past the last one the result
    /// is [`NoTuning::TooManyBits`].
    ChunkTooSmall,
    /// `2 x P x 2^bits + P` exceeds `floor(nofile_soft / (2 x slots))`.
    DescriptorBudget,
}

impl NoTuning {
    /// The condition's name in the retry's INFO line.
    pub fn reason(&self) -> &'static str {
        match self {
            NoTuning::NoConsumedRows => "no_consumed_rows",
            NoTuning::NotOneScan => "not_one_scan",
            NoTuning::NotLogsScan => "not_logs_scan",
            NoTuning::NoExactTotal => "no_exact_total",
            NoTuning::UnlistedNode(_) => "unlisted_node",
            NoTuning::NotFixedWidth => "not_fixed_width",
            NoTuning::Degenerate => "degenerate",
            NoTuning::TooManyBits => "too_many_bits",
            NoTuning::ChunkTooSmall => "chunk_too_small",
            NoTuning::DescriptorBudget => "descriptor_budget",
        }
    }
}

/// The soft `RLIMIT_NOFILE` an executor assumes until its server installs
/// the one it read at start (`SqlExecutor::with_nofile_soft`): Linux's
/// default soft limit.
pub const DEFAULT_NOFILE_SOFT: u64 = 1024;

/// The smallest `2 x P x 2^bits + P` any tuning needs at `partitions`, the
/// retry's final exchange partition count: `3 x P` at `bits = 0`. A
/// descriptor budget below it leaves the retry sort-merge only.
pub fn bucket_spill_min_descriptor_need(partitions: usize) -> u64 {
    (partitions.max(1) as u64).saturating_mul(3)
}

/// `F = floor(nofile_soft / (2 x slots))`, the descriptors one bucket retry
/// may hold open.
pub fn bucket_spill_descriptor_budget(nofile_soft: u64, slots: u64) -> u64 {
    nofile_soft / slots.max(1).saturating_mul(2)
}

/// `clamp(floor(spill_max_bytes / 8 GiB), 1, 4)`, as `SpillBudget::slots`
/// gives it.
pub fn spill_slots(spill_max_bytes: u64) -> u64 {
    (spill_max_bytes / SPILL_SHARE_UNIT_BYTES).clamp(1, MAX_SPILL_SHARES)
}

/// The capacities `OpenChunk::allocate` gives one routed column of `width`
/// bytes per value for `rows` rows: its values buffer and its validity
/// bitmap, each rounded up to 64 bytes.
pub(crate) fn routed_column_capacities(width: usize, rows: usize) -> Option<(usize, usize)> {
    let values = rows.checked_mul(width)?.checked_next_multiple_of(64)?;
    let validity = rows.div_ceil(8).checked_next_multiple_of(64)?;
    Some((values, validity))
}

/// The bytes one routed chunk of `rows` rows is charged: the sum over its
/// columns of [`routed_column_capacities`].
pub fn routed_chunk_charge(column_widths: &[usize], rows: usize) -> Option<usize> {
    column_widths.iter().try_fold(0usize, |acc, width| {
        let (values, validity) = routed_column_capacities(*width, rows)?;
        acc.checked_add(values)?.checked_add(validity)
    })
}

/// The largest row count up to [`BUCKET_SPILL_MAX_CHUNK_ROWS`] whose
/// [`routed_chunk_charge`] is at most `per_chunk`; 0 when not even one row
/// fits.
fn largest_chunk_rows(column_widths: &[usize], per_chunk: usize) -> usize {
    let mut rows = 0;
    let (mut low, mut high) = (1usize, BUCKET_SPILL_MAX_CHUNK_ROWS);
    while low <= high {
        let mid = low + (high - low) / 2;
        match routed_chunk_charge(column_widths, mid) {
            Some(charge) if charge <= per_chunk => {
                rows = mid;
                low = mid + 1;
            }
            _ => high = mid - 1,
        }
    }
    rows
}

/// Derive the retry's `bits` and `routed_chunk_rows`, or say why not.
pub fn derive_bucket_spill_tuning(
    inputs: &BucketSpillInputs,
) -> Result<BucketSpillTuning, NoTuning> {
    let consumed = match inputs.consumed_rows {
        Some(n) if n > 0 => n,
        _ => return Err(NoTuning::NoConsumedRows),
    };
    let total = inputs.total_rows.ok_or(NoTuning::NoExactTotal)?;
    if inputs.partitions == 0
        || inputs.spill_consumers == 0
        || inputs.column_widths.is_empty()
        || inputs.column_widths.contains(&0)
    {
        return Err(NoTuning::Degenerate);
    }
    let headroom = spill_mode_headroom(inputs.query_limit);
    let share = inputs
        .query_limit
        .checked_sub(headroom)
        .ok_or(NoTuning::Degenerate)?
        / inputs.spill_consumers;
    if share == 0 {
        return Err(NoTuning::Degenerate);
    }

    // 2 x T / (P x 0.75 x S) = 8 x A x N / (3 x n x P x S), in exact
    // integers: the table fits at b when 8 x A x N <= 2^b x 3 x n x P x S.
    let numerator = 8u128
        .checked_mul(u128::from(inputs.asked_bytes))
        .and_then(|product| product.checked_mul(u128::from(total)))
        .ok_or(NoTuning::Degenerate)?;
    let unit = 3u128
        .checked_mul(u128::from(consumed))
        .and_then(|product| product.checked_mul(inputs.partitions as u128))
        .and_then(|product| product.checked_mul(share as u128))
        .ok_or(NoTuning::Degenerate)?;
    // `table(b) = ceil(2 x T / (P x 2^b)) = ceil(ceil(2 x A x N / (n x P)) / 2^b)`.
    let tables = u128::from(consumed)
        .checked_mul(inputs.partitions as u128)
        .map(|divisor| (numerator / 4).div_ceil(divisor))
        .ok_or(NoTuning::Degenerate)?;
    let floor_chunk = if inputs.variable_width {
        Some(BUCKET_SPILL_MIN_CHUNK_BYTES as u128)
    } else {
        routed_chunk_charge(&inputs.column_widths, BUCKET_SPILL_MIN_CHUNK_ROWS)
            .map(|charge| charge as u128)
    };
    let mut bits: u8 = 0;
    let table = loop {
        // A `2^b x unit` past u128 exceeds every u128 numerator: the table fits.
        let table_fits = unit
            .checked_mul(1u128 << bits)
            .is_none_or(|scaled| numerator <= scaled);
        if table_fits {
            let table = tables.div_ceil(1u128 << bits);
            let floor_chunks = floor_chunk
                .and_then(|charge| charge.checked_mul((1u128 << bits) + 1))
                .and_then(|chunks| chunks.checked_add(table));
            if floor_chunks.is_some_and(|need| need <= share as u128) {
                break table as usize;
            }
        }
        if bits >= BUCKETED_FINAL_AGGREGATE_MAX_BITS {
            return Err(NoTuning::TooManyBits);
        }
        bits += 1;
    };

    let per_chunk = (share - table) / ((1usize << bits) + 1);
    // A byte-budgeted chunk seals on `per_chunk` bytes, so its row cap is
    // only the 8,192-row ceiling.
    let rows = if inputs.variable_width {
        BUCKET_SPILL_MAX_CHUNK_ROWS
    } else {
        largest_chunk_rows(&inputs.column_widths, per_chunk)
    };

    let partitions = inputs.partitions as u64;
    let descriptor_need = partitions
        .checked_mul(2u64 << bits)
        .and_then(|need| need.checked_add(partitions))
        .ok_or(NoTuning::Degenerate)?;
    if descriptor_need > bucket_spill_descriptor_budget(inputs.nofile_soft, inputs.slots) {
        return Err(NoTuning::DescriptorBudget);
    }
    Ok(BucketSpillTuning {
        bits,
        routed_chunk_rows: rows,
        routed_chunk_bytes: inputs.variable_width.then_some(per_chunk),
        descriptor_need,
    })
}

/// What the derivation reads from the retry's placeholder plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSpillPlanShape {
    /// `P`: the bucketed exec's output partitions.
    pub partitions: usize,
    /// `C`, counted from the `C` table over every node.
    pub spill_consumers: usize,
    /// The routed columns' widths, keys then state; 16 for a string or
    /// binary column's view.
    pub column_widths: Vec<usize>,
    /// Whether a routed column is a string or binary type.
    pub variable_width: bool,
    /// `N`: the checked `sample_count` sum of the one `LogsScanExec`.
    pub total_rows: Option<u64>,
}

impl BucketSpillPlanShape {
    /// Read `P`, `C`, the routed widths and `N` from `plan`, which must hold
    /// exactly one [`BucketedFinalAggregateExec`] whose input holds exactly
    /// one scan, a [`LogsScanExec`].
    pub fn read(plan: &Arc<dyn ExecutionPlan>) -> Result<Self, NoTuning> {
        let spill_consumers = spill_consumers(plan)?;
        let mut bucketed = Vec::new();
        collect_bucketed(plan, &mut bucketed);
        let [exec] = bucketed.as_slice() else {
            return Err(NoTuning::NotOneScan);
        };
        let partitions = exec.properties().partitioning.partition_count();
        let input = exec.aggregate().input();
        // A routed type that does not resolve has no width either.
        let layouts = routed_types(exec.aggregate())
            .ok()
            .and_then(Result::ok)
            .ok_or(NoTuning::NotFixedWidth)?;
        let variable_width = layouts
            .iter()
            .any(|(_, layout)| !matches!(layout, RoutedLayout::Fixed(_)));
        let column_widths = layouts
            .into_iter()
            .map(|(_, layout)| match layout {
                RoutedLayout::Fixed(width) => width,
                RoutedLayout::Offsets(_) | RoutedLayout::Views => 16,
            })
            .collect();
        let mut leaves = Vec::new();
        collect_leaves(input, &mut leaves);
        let [leaf] = leaves.as_slice() else {
            return Err(NoTuning::NotOneScan);
        };
        let scan = leaf
            .downcast_ref::<LogsScanExec>()
            .ok_or(NoTuning::NotLogsScan)?;
        Ok(BucketSpillPlanShape {
            partitions,
            spill_consumers,
            column_widths,
            variable_width,
            total_rows: scan.sample_count_total(),
        })
    }
}

/// The retry's reason when its candidate plan keeps a `FinalPartitioned`
/// aggregate the bucketed rule does not admit, or holds no bucketed exec.
pub const RETRY_NOT_ADMITTED: &str = "not_admitted";

/// The retry's reason when its candidate plan holds an order-preserving
/// `RepartitionExec`.
pub const RETRY_ORDER_PRESERVING_EXCHANGE: &str = "order_preserving_exchange";

/// What the refused first attempt and the process give the bucket retry's
/// derivation, beside what [`BucketSpillPlanShape::read`] takes from the
/// plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketRetryFigures {
    /// `A`, [`BucketSpillInputs::asked_bytes`].
    pub asked_bytes: u64,
    /// `n`, [`BucketSpillInputs::consumed_rows`].
    pub consumed_rows: Option<u64>,
    /// `L`, [`BucketSpillInputs::query_limit`].
    pub query_limit: usize,
    /// [`BucketSpillInputs::nofile_soft`].
    pub nofile_soft: u64,
    /// [`BucketSpillInputs::slots`].
    pub slots: u64,
}

/// Whether the refusal retry keeps `plan`, its candidate planned at the
/// placeholder tuning, and at which tuning (ADR-0954, parallel bucket spill
/// item 1): every `FinalPartitioned` aggregate was replaced by a
/// [`BucketedFinalAggregateExec`], no `RepartitionExec` preserves order, and
/// the derivation returns a tuning. The error is the condition that keeps
/// the sort-merge retry, as the retry's INFO line names it.
pub fn bucket_retry_tuning(
    plan: &Arc<dyn ExecutionPlan>,
    figures: BucketRetryFigures,
) -> Result<BucketSpillTuning, &'static str> {
    bucket_retry_derivation(plan, figures).0
}

/// What [`bucket_retry_derivation`] read from the candidate plan, for the
/// retry's INFO line; each `None` when the derivation stopped before it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketRetryShape {
    /// `N`, [`BucketSpillPlanShape::total_rows`].
    pub total_rows: Option<u64>,
    /// `C`, [`BucketSpillPlanShape::spill_consumers`].
    pub spill_consumers: Option<usize>,
    /// `P`, [`BucketSpillPlanShape::partitions`].
    pub partitions: Option<usize>,
    /// [`BucketSpillTuning::descriptor_need`], when a tuning was derived.
    pub descriptor_need: Option<u64>,
}

/// [`bucket_retry_tuning`], with the figures it read from `plan`.
pub fn bucket_retry_derivation(
    plan: &Arc<dyn ExecutionPlan>,
    figures: BucketRetryFigures,
) -> (Result<BucketSpillTuning, &'static str>, BucketRetryShape) {
    fn walk(plan: &Arc<dyn ExecutionPlan>, bucketed: &mut usize) -> Result<(), &'static str> {
        if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
            && *aggregate.mode() == AggregateMode::FinalPartitioned
        {
            return Err(RETRY_NOT_ADMITTED);
        }
        if plan
            .downcast_ref::<RepartitionExec>()
            .is_some_and(RepartitionExec::preserve_order)
        {
            return Err(RETRY_ORDER_PRESERVING_EXCHANGE);
        }
        if plan.downcast_ref::<BucketedFinalAggregateExec>().is_some() {
            *bucketed += 1;
        }
        plan.children()
            .into_iter()
            .try_for_each(|child| walk(child, bucketed))
    }
    let mut bucketed = 0;
    if let Err(reason) = walk(plan, &mut bucketed) {
        return (Err(reason), BucketRetryShape::default());
    }
    if bucketed == 0 {
        return (Err(RETRY_NOT_ADMITTED), BucketRetryShape::default());
    }
    let shape = match BucketSpillPlanShape::read(plan) {
        Ok(shape) => shape,
        Err(why) => return (Err(why.reason()), BucketRetryShape::default()),
    };
    let mut read = BucketRetryShape {
        total_rows: shape.total_rows,
        spill_consumers: Some(shape.spill_consumers),
        partitions: Some(shape.partitions),
        descriptor_need: None,
    };
    let tuning = derive_bucket_spill_tuning(&BucketSpillInputs {
        asked_bytes: figures.asked_bytes,
        total_rows: shape.total_rows,
        consumed_rows: figures.consumed_rows,
        partitions: shape.partitions,
        spill_consumers: shape.spill_consumers,
        query_limit: figures.query_limit,
        column_widths: shape.column_widths,
        variable_width: shape.variable_width,
        nofile_soft: figures.nofile_soft,
        slots: figures.slots,
    })
    .map_err(|why| why.reason());
    read.descriptor_need = tuning.as_ref().ok().map(|tuning| tuning.descriptor_need);
    (tuning, read)
}

/// `plan` with every [`BucketedFinalAggregateExec`] rebuilt at `tuning`'s
/// `bits`, `routed_chunk_rows` and `routed_chunk_bytes`, with spill on, keeping its
/// `engage_groups` and per-bucket top-k.
pub fn with_bucket_spill_tuning(
    plan: Arc<dyn ExecutionPlan>,
    tuning: BucketSpillTuning,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(exec) = node.downcast_ref::<BucketedFinalAggregateExec>() else {
            return Ok(Transformed::no(node));
        };
        let config = BucketedFinalAggregation {
            bits: tuning.bits,
            routed_chunk_rows: tuning.routed_chunk_rows,
            routed_chunk_bytes: tuning
                .routed_chunk_bytes
                .unwrap_or(exec.config().routed_chunk_bytes),
            ..exec.config()
        };
        let tuned = BucketedFinalAggregateExec::new(Arc::clone(exec.aggregate()), config)
            .with_topk(exec.topk().cloned())
            .with_spill(true);
        Ok(Transformed::yes(Arc::new(tuned) as Arc<dyn ExecutionPlan>))
    })
    .map(|transformed| transformed.data)
}

fn collect_bucketed<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    out: &mut Vec<&'a BucketedFinalAggregateExec>,
) {
    if let Some(exec) = plan.downcast_ref::<BucketedFinalAggregateExec>() {
        out.push(exec);
    }
    for child in plan.children() {
        collect_bucketed(child, out);
    }
}

fn collect_leaves<'a>(plan: &'a Arc<dyn ExecutionPlan>, out: &mut Vec<&'a Arc<dyn ExecutionPlan>>) {
    let children = plan.children();
    if children.is_empty() {
        out.push(plan);
    }
    for child in children {
        collect_leaves(child, out);
    }
}

/// `C`: the spill-capable consumers `plan` registers, from the closed table
/// of ADR-3565 amendment 2 item 4. A node the table does not list is
/// [`NoTuning::UnlistedNode`].
pub fn spill_consumers(plan: &Arc<dyn ExecutionPlan>) -> Result<usize, NoTuning> {
    let own = spill_consumers_of(plan.as_ref())?;
    plan.children().into_iter().try_fold(own, |acc, child| {
        Ok(acc.saturating_add(spill_consumers(child)?))
    })
}

fn spill_consumers_of(plan: &dyn ExecutionPlan) -> Result<usize, NoTuning> {
    let partitions = plan.properties().partitioning.partition_count();
    if plan.is::<AggregateExec>()
        || plan.is::<RepartitionExec>()
        || plan.is::<BucketedFinalAggregateExec>()
    {
        return Ok(partitions);
    }
    if let Some(sort) = plan.downcast_ref::<SortExec>() {
        return Ok(if sort.fetch().is_some() {
            0
        } else {
            partitions
        });
    }
    if registers_none(plan) {
        return Ok(0);
    }
    Err(NoTuning::UnlistedNode(plan.name().to_owned()))
}

#[allow(deprecated)] // CoalesceBatchesExec still appears in physical plans.
fn registers_none(any: &dyn ExecutionPlan) -> bool {
    any.is::<SortPreservingMergeExec>()
        || any.is::<ProjectionExec>()
        || any.is::<FilterExec>()
        || any.is::<CoalesceBatchesExec>()
        || any.is::<CoalescePartitionsExec>()
        || any.is::<GlobalLimitExec>()
        || any.is::<LocalLimitExec>()
        || any.is::<DataSourceExec>()
        || any.is::<EmptyExec>()
        || any.is::<PlaceholderRowExec>()
        || any.is::<CooperativeExec>()
        || any.is::<BufferExec>()
        || any.is::<LogsScanExec>()
        || any.is::<crate::scan::RsegScanExec>()
        || any.is::<crate::alerts_scan::AlertsScanExec>()
        || any.is::<crate::audit_scan::AuditScanExec>()
        || any.is::<crate::spans_scan::SpansScanExec>()
        || any.is::<crate::dedup::RsegDedupExec>()
        || any.is::<crate::compact_slices::CompactSlicesExec>()
        || any.is::<crate::top_keys::TopKeysAggregateExec>()
        || any.is::<crate::first_groups::FirstGroupsExec>()
        || any.is::<crate::late_materialization::LogsRowFetchExec>()
        || any.is::<crate::metadata_agg::MetadataOnlyExec>()
        || registers_none_distributed(any)
}

#[cfg(feature = "flight-sql")]
fn registers_none_distributed(any: &dyn ExecutionPlan) -> bool {
    any.is::<crate::distributed::DistributedScanExec>()
        || any.is::<crate::distributed_rlog::DistributedSliceScanExec>()
}

#[cfg(not(feature = "flight-sql"))]
fn registers_none_distributed(_any: &dyn ExecutionPlan) -> bool {
    false
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// q33's routed columns on the ClickBench corpus: 8 + 8 B of keys,
    /// 8 + 16 + 16 + 8 B of state, the 64 B row M8 measured (ADR-3565
    /// amendment 3).
    const Q33_WIDTHS: [usize; 6] = [8, 8, 8, 16, 16, 8];
    /// The stock final's asked bytes, the projection `T` (amendment 1).
    const Q33_ASKED: u64 = 9_480_000_000;
    const Q33_ROWS: u64 = 99_997_497;

    struct Box6a {
        query_limit: usize,
        spill_max: u64,
        partial: usize,
    }

    const LARGE: Box6a = Box6a {
        query_limit: 1_448_258_643,
        spill_max: 9_328_558_080,
        partial: 8,
    };
    const XLARGE: Box6a = Box6a {
        query_limit: 3_378_149_703,
        spill_max: 21_759_418_368,
        partial: 8,
    };
    const XXLARGE: Box6a = Box6a {
        query_limit: 8_186_333_184,
        spill_max: 54_294_740_992,
        partial: 16,
    };

    /// q33 on `box6a` with the scan finished (`n = N`, so `T = A`).
    fn q33(box6a: &Box6a, nofile_soft: u64) -> BucketSpillInputs {
        BucketSpillInputs {
            asked_bytes: Q33_ASKED,
            total_rows: Some(Q33_ROWS),
            consumed_rows: Some(Q33_ROWS),
            partitions: 16,
            spill_consumers: box6a.partial + 16 + 16,
            query_limit: box6a.query_limit,
            column_widths: Q33_WIDTHS.to_vec(),
            variable_width: false,
            nofile_soft,
            slots: spill_slots(box6a.spill_max),
        }
    }

    #[test]
    fn bucket_spill_derivation_q33_c6a_2xlarge() {
        let inputs = q33(&XXLARGE, 524_288);
        assert_eq!(inputs.slots, 4);
        assert_eq!(inputs.spill_consumers, 48);
        assert_eq!(
            derive_bucket_spill_tuning(&inputs),
            Ok(BucketSpillTuning {
                bits: 4,
                routed_chunk_rows: 8192,
                routed_chunk_bytes: None,
                descriptor_need: 528,
            })
        );
    }

    #[test]
    fn bucket_spill_derivation_q33_c6a_xlarge() {
        let inputs = q33(&XLARGE, 524_288);
        assert_eq!(inputs.slots, 2);
        assert_eq!(
            derive_bucket_spill_tuning(&inputs),
            Ok(BucketSpillTuning {
                bits: 5,
                routed_chunk_rows: 8192,
                routed_chunk_bytes: None,
                descriptor_need: 1040,
            })
        );
    }

    /// The only q33 column whose chunk rows fall below 8192: 65 chunks of
    /// `routed_chunk_charge(.., rows)` within what `table(6)`,
    /// `ceil(2 x 9.48 GB / (16 x 64))`, leaves of `S`, 32,585,819 B.
    #[test]
    fn bucket_spill_derivation_q33_c6a_large() {
        let inputs = q33(&LARGE, 524_288);
        assert_eq!(inputs.slots, 1);
        let tuning = derive_bucket_spill_tuning(&inputs).expect("tunes");
        assert_eq!(
            tuning,
            BucketSpillTuning {
                bits: 6,
                routed_chunk_rows: 3336,
                routed_chunk_bytes: None,
                descriptor_need: 2064,
            }
        );
        let budget = (32_585_819 - 18_515_625) / 65;
        assert_eq!(routed_chunk_charge(&Q33_WIDTHS, 3336), Some(216_192));
        assert!(216_192 <= budget);
        assert!(routed_chunk_charge(&Q33_WIDTHS, 3337).expect("fits") > budget);
        // At `bits` 7 a fixed quarter of the same `S` held 972 rows (ADR-4143).
        let quarter = 32_585_819 / 4 / 129;
        assert!(routed_chunk_charge(&Q33_WIDTHS, 972).expect("fits") <= quarter);
        assert!(routed_chunk_charge(&Q33_WIDTHS, 973).expect("fits") > quarter);
    }

    /// The c6a.large inputs M8 logged for q33 (ADR-3565 amendment 3, #4072):
    /// `T = A x N / n` is 12.65 GB, `S` 32,874,379 B and `table(7)`
    /// 12,354,622 B, which leaves 2,452 rows to each of 129 chunks. A fixed
    /// quarter of `S` held 980, under the floor.
    #[test]
    fn bucket_spill_derivation_q33_c6a_large_measured_t() {
        let inputs = BucketSpillInputs {
            asked_bytes: 1_099_716_576,
            total_rows: Some(Q33_ROWS),
            consumed_rows: Some(8_692_416),
            partitions: 16,
            spill_consumers: 40,
            query_limit: 1_461_083_554,
            column_widths: Q33_WIDTHS.to_vec(),
            variable_width: false,
            nofile_soft: 8320,
            slots: 1,
        };
        assert_eq!(
            derive_bucket_spill_tuning(&inputs),
            Ok(BucketSpillTuning {
                bits: 7,
                routed_chunk_rows: 2452,
                routed_chunk_bytes: None,
                descriptor_need: 4112,
            })
        );
        let budget = (32_874_379 - 12_354_622) / 129;
        assert!(routed_chunk_charge(&Q33_WIDTHS, 2452).expect("fits") <= budget);
        assert!(routed_chunk_charge(&Q33_WIDTHS, 2453).expect("fits") > budget);
        assert!(routed_chunk_charge(&Q33_WIDTHS, 980).expect("fits") <= 32_874_379 / 4 / 129);
        assert!(routed_chunk_charge(&Q33_WIDTHS, 981).expect("fits") > 32_874_379 / 4 / 129);
    }

    /// The second `bits` condition: at the M8 c6a.large inputs with `A`
    /// raised so `T` is 25.09 GB, `table(7)` is 24,500,000 B, within
    /// `0.75 x S` (24,655,784 B), but 129 chunks of 1,024 rows (66,304 B
    /// each) need 8,553,216 B and `S - table(7)` is 8,374,379 B. The
    /// derivation takes `bits` 8, where `table(8)` is 12,250,000 B and 257
    /// chunks get 1,232 rows each.
    #[test]
    fn bucket_spill_derivation_takes_a_bit_for_floor_chunks() {
        let inputs = BucketSpillInputs {
            asked_bytes: 2_180_807_911,
            total_rows: Some(Q33_ROWS),
            consumed_rows: Some(8_692_416),
            partitions: 16,
            spill_consumers: 40,
            query_limit: 1_461_083_554,
            column_widths: Q33_WIDTHS.to_vec(),
            variable_width: false,
            nofile_soft: 524_288,
            slots: 1,
        };
        let share: u128 = 32_874_379;
        // The inputs sit inside the band: table(7) fits 0.75 x S and leaves
        // no room for floor-sized chunks.
        let two_t = 2 * u128::from(inputs.asked_bytes) * u128::from(Q33_ROWS);
        let consumed_partitions = 8_692_416u128 * 16;
        let table7 = two_t.div_ceil(consumed_partitions * 128);
        assert_eq!(table7, 24_500_000);
        assert!(4 * two_t <= 3 * share * consumed_partitions * 128);
        assert!(4 * two_t > 3 * share * consumed_partitions * 64);
        let floor_chunk = routed_chunk_charge(&Q33_WIDTHS, 1024).expect("fits") as u128;
        assert_eq!(floor_chunk, 66_304);
        assert!(table7 + 129 * floor_chunk > share);
        assert_eq!(
            derive_bucket_spill_tuning(&inputs),
            Ok(BucketSpillTuning {
                bits: 8,
                routed_chunk_rows: 1232,
                routed_chunk_bytes: None,
                descriptor_need: 8208,
            })
        );
    }

    /// The largest `T` that tunes q33 on c6a.large at item 4's `S`,
    /// 32,585,819 B: at `bits` 7 `table(7)` may take `S` less 129 floor
    /// chunks of 66,304 B, 24,032,603 B, so `T` up to 24,609,385,472 B
    /// (about 24.6 GB) with a soft `nofile` of at least 8,224; at `bits` 8 it
    /// may take `S` less 257 of them, 15,545,691 B, so `T` up to
    /// 31,837,575,168 B (about 31.8 GB) with at least 16,416. Past that no
    /// `bits` up to 16 passes both conditions.
    #[test]
    fn bucket_spill_derivation_q33_c6a_large_largest_t() {
        let at = |asked_bytes, nofile_soft| {
            let mut inputs = q33(&LARGE, nofile_soft);
            inputs.asked_bytes = asked_bytes;
            derive_bucket_spill_tuning(&inputs)
        };
        let (bits7, bits8) = (24_609_385_472, 31_837_575_168);
        assert_eq!(
            at(bits7, 8224),
            Ok(BucketSpillTuning {
                bits: 7,
                routed_chunk_rows: 1024,
                routed_chunk_bytes: None,
                descriptor_need: 4112,
            })
        );
        assert_eq!(at(bits7, 8223), Err(NoTuning::DescriptorBudget));
        assert_eq!(at(bits7 + 1, 524_288).map(|tuning| tuning.bits), Ok(8));
        assert_eq!(at(bits7 + 1, 16_415), Err(NoTuning::DescriptorBudget));
        assert_eq!(
            at(bits8, 16_416),
            Ok(BucketSpillTuning {
                bits: 8,
                routed_chunk_rows: 1024,
                routed_chunk_bytes: None,
                descriptor_need: 8208,
            })
        );
        assert_eq!(at(bits8 + 1, 524_288), Err(NoTuning::TooManyBits));
    }

    /// `table(b)` rounds up, in both divisions: `ceil(2 x A x N / (n x P))`
    /// and its `ceil(.. / 2^b)`. In each case the rounded-up table leaves
    /// one byte too few for a larger tuning, so a table rounded down changes
    /// the tuning. With one 8 B column at `C = 1` and `L` 64 MiB plus `S`, so
    /// `S = L - 64 MiB`:
    ///
    /// - `S` 200,000 B, `P` 3: `table(0) = ceil(399,554 / 3)` is 133,185 B,
    ///   and two chunks of 33,407 B fit 4,096 rows. Rounded down, 133,184 B
    ///   leaves 33,408 B each, the charge of 4,104 rows.
    /// - `S` 400,000 B, `P` 1: `table(2) = ceil(931,842 / 4)` is 232,961 B,
    ///   and five chunks of 33,407 B fit 4,096 rows. Rounded down, 232,960 B
    ///   leaves 33,408 B each and 4,104 rows.
    /// - `S` 56,639 B, `P` 3: `table(0) = ceil(119,998 / 3)` is 40,000 B, and
    ///   two floor chunks of 8,320 B each overshoot `S` by one byte, so `bits`
    ///   is 1: `table(1)` is 20,000 B and three chunks of 12,213 B fit 1,496
    ///   rows. Rounded down, `bits` is 0 with 1,024 rows.
    #[test]
    fn bucket_spill_derivation_rounds_the_table_up() {
        let derive = |asked_bytes, partitions, share: usize| {
            derive_bucket_spill_tuning(&BucketSpillInputs {
                asked_bytes,
                total_rows: Some(1),
                consumed_rows: Some(1),
                partitions,
                spill_consumers: 1,
                query_limit: (64 << 20) + share,
                column_widths: vec![8],
                variable_width: false,
                nofile_soft: 524_288,
                slots: 1,
            })
        };
        assert_eq!(routed_chunk_charge(&[8], 4096), Some(33_280));
        assert_eq!(routed_chunk_charge(&[8], 4104), Some(33_408));
        assert_eq!(routed_chunk_charge(&[8], 4105), Some(33_472));
        assert_eq!(routed_chunk_charge(&[8], 1024), Some(8320));
        assert_eq!(routed_chunk_charge(&[8], 1496), Some(12_160));
        assert_eq!(routed_chunk_charge(&[8], 1497), Some(12_224));
        assert_eq!(
            derive(199_777, 3, 200_000),
            Ok(BucketSpillTuning {
                bits: 0,
                routed_chunk_rows: 4096,
                routed_chunk_bytes: None,
                descriptor_need: 9,
            })
        );
        assert_eq!(
            derive(465_921, 1, 400_000),
            Ok(BucketSpillTuning {
                bits: 2,
                routed_chunk_rows: 4096,
                routed_chunk_bytes: None,
                descriptor_need: 9,
            })
        );
        assert_eq!(
            derive(59_999, 3, 56_639),
            Ok(BucketSpillTuning {
                bits: 1,
                routed_chunk_rows: 1496,
                routed_chunk_bytes: None,
                descriptor_need: 15,
            })
        );
    }

    /// ADR-4143 decision 3: a route with a string or binary column gets
    /// `B = floor((S - table(bits)) / (2^bits + 1))` and the 8,192-row cap,
    /// and `B_min` (16,384 B) stands in for `b(1024)` in the `bits` floor
    /// condition. With `N = n = 1`, `P = 1` and `S` the share, `table(b)` is
    /// `ceil(2 x A / 2^b)` and the table fits at `b` when `8 x A <= 2^b x 3 x S`.
    /// The routed widths are an `Int64` and a string's 16, whose `b(1024)` is
    /// 24,832 B:
    /// - `A` 100,000, `S` 1,000,000: the table, 200,000 B, fits at `bits` 0
    ///   beside two floors, and `B` is 800,000 / 2 = 400,000.
    /// - `A` 30,000, `S` 100,000: the table, 60,000 B, fits at `bits` 0
    ///   (240,000 <= 300,000) beside two `B_min` floors (92,768 B), and `B`
    ///   is 40,000 / 2 = 20,000. Two `b(1024)` floors would need 109,664 B,
    ///   and every later `bits` adds more floor than it takes off the table,
    ///   so the same widths on a fixed-width route get `too_many_bits`.
    /// - `A` 30,000, `S` 85,000: two `B_min` floors beside the table need
    ///   92,768 B, so `bits` is 1: 30,000 B and three floors are 79,152 B,
    ///   and `B` is 55,000 / 3 = 18,333.
    /// - `S` 30,000: two `B_min` floors alone pass it, at any `bits`.
    #[test]
    fn bucket_spill_derivation_budgets_string_routes_in_bytes() {
        let derive = |asked_bytes, share: usize, variable_width| {
            derive_bucket_spill_tuning(&BucketSpillInputs {
                asked_bytes,
                total_rows: Some(1),
                consumed_rows: Some(1),
                partitions: 1,
                spill_consumers: 1,
                query_limit: (64 << 20) + share,
                column_widths: vec![8, 16],
                variable_width,
                nofile_soft: 524_288,
                slots: 1,
            })
        };
        assert_eq!(routed_chunk_charge(&[8, 16], 1024), Some(24_832));
        let string_route = |bits, bytes, descriptor_need| {
            Ok(BucketSpillTuning {
                bits,
                routed_chunk_rows: 8192,
                routed_chunk_bytes: Some(bytes),
                descriptor_need,
            })
        };
        assert_eq!(derive(100_000, 1_000_000, true), string_route(0, 400_000, 3));
        assert_eq!(derive(30_000, 100_000, true), string_route(0, 20_000, 3));
        assert_eq!(
            derive(30_000, 100_000, false),
            Err(NoTuning::TooManyBits)
        );
        assert_eq!(derive(30_000, 85_000, true), string_route(1, 18_333, 5));
        assert_eq!(derive(1, 30_000, true), Err(NoTuning::TooManyBits));
    }

    /// `C` divides the share: dividing by `P` (16) instead of `C` (48) on
    /// c6a.2xlarge triples `S` and drops `bits` from 4 to 2.
    #[test]
    fn bucket_spill_derivation_divides_by_consumers() {
        let mut inputs = q33(&XXLARGE, 524_288);
        inputs.spill_consumers = inputs.partitions;
        assert_eq!(
            derive_bucket_spill_tuning(&inputs).map(|tuning| tuning.bits),
            Ok(2)
        );
    }

    /// `n` is on the scan's input basis: a refusal halfway through the input
    /// at half q33's asked bytes projects q33's `T` and its `bits` 4. Fed
    /// the same prefix's output rows behind a 10%-selective filter, `N / n`
    /// is ten times too large and the derivation asks for 7.
    #[test]
    fn bucket_spill_derivation_projects_on_input_rows() {
        let mut inputs = q33(&XXLARGE, 524_288);
        inputs.asked_bytes = Q33_ASKED / 2;
        inputs.consumed_rows = Some(Q33_ROWS / 2);
        assert_eq!(
            derive_bucket_spill_tuning(&inputs).map(|tuning| tuning.bits),
            Ok(4)
        );
        inputs.consumed_rows = Some(Q33_ROWS / 20);
        assert_eq!(
            derive_bucket_spill_tuning(&inputs).map(|tuning| tuning.bits),
            Ok(7)
        );
    }

    #[test]
    fn bucket_spill_derivation_refusals() {
        let base = q33(&XXLARGE, 524_288);
        let refused = |edit: &dyn Fn(&mut BucketSpillInputs)| {
            let mut inputs = base.clone();
            edit(&mut inputs);
            derive_bucket_spill_tuning(&inputs).expect_err("no tuning")
        };
        assert_eq!(
            refused(&|i| i.consumed_rows = Some(0)),
            NoTuning::NoConsumedRows
        );
        assert_eq!(
            refused(&|i| i.consumed_rows = None),
            NoTuning::NoConsumedRows
        );
        assert_eq!(refused(&|i| i.total_rows = None), NoTuning::NoExactTotal);
        assert_eq!(
            refused(&|i| i.consumed_rows = Some(1)),
            NoTuning::TooManyBits
        );
        // At 37.92 GB on c6a.large the table fits from bits 8, and from
        // there to bits 16 every table and its floor-sized chunks together
        // exceed `S`: no bits passes both conditions.
        assert_eq!(
            refused(&|i| {
                i.query_limit = LARGE.query_limit;
                i.spill_consumers = 40;
                i.asked_bytes = Q33_ASKED * 4;
            }),
            NoTuning::TooManyBits
        );
        assert_eq!(
            refused(&|i| i.nofile_soft = 1024),
            NoTuning::DescriptorBudget
        );
        assert_eq!(refused(&|i| i.partitions = 0), NoTuning::Degenerate);
        assert_eq!(refused(&|i| i.spill_consumers = 0), NoTuning::Degenerate);
        assert_eq!(refused(&|i| i.column_widths.clear()), NoTuning::Degenerate);
    }

    /// Every product the derivation forms is checked: inputs at their type
    /// maxima return `Degenerate` instead of wrapping or panicking.
    #[test]
    fn bucket_spill_derivation_overflow_is_degenerate() {
        let max = BucketSpillInputs {
            asked_bytes: u64::MAX,
            total_rows: Some(u64::MAX),
            consumed_rows: Some(u64::MAX),
            partitions: usize::MAX,
            spill_consumers: 1,
            query_limit: usize::MAX,
            column_widths: vec![usize::MAX],
            variable_width: false,
            nofile_soft: u64::MAX,
            slots: u64::MAX,
        };
        assert_eq!(derive_bucket_spill_tuning(&max), Err(NoTuning::Degenerate));
        let base = q33(&XXLARGE, u64::MAX);
        let refused = |edit: &dyn Fn(&mut BucketSpillInputs)| {
            let mut inputs = base.clone();
            edit(&mut inputs);
            derive_bucket_spill_tuning(&inputs)
        };
        // 8 x A x N past u128.
        assert_eq!(
            refused(&|i| {
                i.asked_bytes = u64::MAX;
                i.total_rows = Some(u64::MAX);
            }),
            Err(NoTuning::Degenerate)
        );
        // 3 x n x P x S past u128.
        assert_eq!(
            refused(&|i| {
                i.consumed_rows = Some(u64::MAX);
                i.partitions = usize::MAX;
            }),
            Err(NoTuning::Degenerate)
        );
        // `2 x P x 2^bits + P` past u64 at bits 0.
        assert_eq!(
            refused(&|i| i.partitions = 1 << 63),
            Err(NoTuning::Degenerate)
        );
        // `2 x slots` past u64 saturates, leaving a budget of zero, as the
        // startup line's `bucket_spill_descriptor_budget` computes it.
        assert_eq!(
            refused(&|i| i.slots = u64::MAX),
            Err(NoTuning::DescriptorBudget)
        );
        // The largest `slots` that doubles exactly leaves a budget of one.
        assert_eq!(
            refused(&|i| i.slots = u64::MAX / 2),
            Err(NoTuning::DescriptorBudget)
        );
    }

    #[test]
    fn bucket_spill_descriptor_budget() {
        let tuned = |nofile_soft| derive_bucket_spill_tuning(&q33(&XXLARGE, nofile_soft));
        assert_eq!(tuned(1024), Err(NoTuning::DescriptorBudget));
        assert_eq!(tuned(524_288).map(|tuning| tuning.bits), Ok(4));
        assert_eq!(tuned(4224).map(|tuning| tuning.descriptor_need), Ok(528));
        assert_eq!(tuned(4223), Err(NoTuning::DescriptorBudget));
        // The least limit on each box is `2 x slots x need`.
        let least = |box6a: &Box6a| {
            let tuning = derive_bucket_spill_tuning(&q33(box6a, u64::MAX)).expect("tunes");
            2 * spill_slots(box6a.spill_max) * tuning.descriptor_need
        };
        assert_eq!(least(&LARGE), 4128);
        assert_eq!(least(&XLARGE), 4160);
        assert_eq!(least(&XXLARGE), 4224);
        for box6a in [&LARGE, &XLARGE, &XXLARGE] {
            let limit = least(box6a);
            assert!(derive_bucket_spill_tuning(&q33(box6a, limit)).is_ok());
            assert_eq!(
                derive_bucket_spill_tuning(&q33(box6a, limit - 1)),
                Err(NoTuning::DescriptorBudget)
            );
        }
    }

    /// The least need is `3 x P` at the inputs' partition count, the need of
    /// a `bits = 0` tuning: at 16 partitions over four slots a soft limit of
    /// `2 x 4 x 48` tunes and one below it does not.
    ///
    /// Prove-the-test: return 3 whatever the partitions and the minimum reads
    /// 3 against a tuning that needs 48.
    #[test]
    fn the_least_descriptor_need_is_three_per_partition() {
        assert_eq!(bucket_spill_min_descriptor_need(16), 48);
        let at = |nofile_soft| {
            let mut inputs = q33(&XXLARGE, nofile_soft);
            inputs.asked_bytes = 1;
            derive_bucket_spill_tuning(&inputs)
        };
        let least = 2 * 4 * bucket_spill_min_descriptor_need(16);
        assert_eq!(
            at(least).map(|tuning| (tuning.bits, tuning.descriptor_need)),
            Ok((0, bucket_spill_min_descriptor_need(16)))
        );
        assert_eq!(at(least - 1), Err(NoTuning::DescriptorBudget));
    }

    #[test]
    fn spill_consumers_refuses_an_unlisted_node() {
        use datafusion::arrow::datatypes::Schema;
        use datafusion::physical_plan::union::UnionExec;
        let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        assert_eq!(spill_consumers(&empty), Ok(0));
        let union = UnionExec::try_new(vec![Arc::clone(&empty), empty]).expect("union");
        assert_eq!(
            spill_consumers(&union),
            Err(NoTuning::UnlistedNode("UnionExec".to_owned()))
        );
    }
}
