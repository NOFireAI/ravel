//! The bucket spill tuning a refusal retry derives from its refused first
//! attempt (ADR-3565 amendment 2, item 4).
//!
//! The retry plans with [`BucketedFinalAggregateExec`] at a placeholder
//! tuning, reads `P`, `C`, the routed column widths and the scan's `N` from
//! that plan ([`BucketSpillPlanShape::read`]), and derives `bits` and
//! `routed_chunk_rows` from them and the first attempt's figures
//! ([`derive_bucket_spill_tuning`]). With:
//!
//! - `T = A x N / n`, the projected table bytes;
//! - `S = (L - H) / C`, the per-stream share, `H` the spill-mode headroom;
//! - `bits = max(0, ceil(log2(2 x T / (P x 0.75 x S))))`;
//! - `routed_chunk_rows`, the most rows up to 8192 whose `2^bits + 1`
//!   chunks, charged as a routed chunk is charged (each column's values
//!   capacity rounded up to 64 bytes, plus its validity bitmap), fit in
//!   `floor(0.25 x S)`.
//!
//! Any condition in [`NoTuning`] keeps the retry's sort-merge plan.

use std::sync::Arc;

use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
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
    BUCKETED_FINAL_AGGREGATE_MAX_BITS, BucketedFinalAggregateExec, routed_types,
};
use crate::logs_scan::LogsScanExec;
use crate::memory::spill_mode_headroom;
use crate::spill::{MAX_SPILL_SHARES, SPILL_SHARE_UNIT_BYTES};

/// The largest `routed_chunk_rows` the derivation returns.
pub const BUCKET_SPILL_MAX_CHUNK_ROWS: usize = 8192;

/// The smallest `routed_chunk_rows` the derivation returns; below it the
/// retry keeps its sort-merge plan.
pub const BUCKET_SPILL_MIN_CHUNK_ROWS: usize = 1024;

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
    /// their sum.
    pub column_widths: Vec<usize>,
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
    /// A routed column is not fixed width.
    NotFixedWidth,
    /// A figure is zero or does not fit the arithmetic (`P`, `C`, `w`, or a
    /// share `L - H` of nothing).
    Degenerate,
    /// `bits` would exceed [`BUCKETED_FINAL_AGGREGATE_MAX_BITS`].
    TooManyBits,
    /// `routed_chunk_rows` would fall below [`BUCKET_SPILL_MIN_CHUNK_ROWS`].
    ChunkTooSmall,
    /// `2 x P x 2^bits + P` exceeds `floor(nofile_soft / (2 x slots))`.
    DescriptorBudget,
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
    // integers: bits is the least b with 8 x A x N <= 2^b x 3 x n x P x S.
    let numerator = 8u128
        .checked_mul(u128::from(inputs.asked_bytes))
        .and_then(|product| product.checked_mul(u128::from(total)))
        .ok_or(NoTuning::Degenerate)?;
    let unit = 3u128
        .checked_mul(u128::from(consumed))
        .and_then(|product| product.checked_mul(inputs.partitions as u128))
        .and_then(|product| product.checked_mul(share as u128))
        .ok_or(NoTuning::Degenerate)?;
    let mut bits: u8 = 0;
    loop {
        // A `2^b x unit` past u128 exceeds every u128 numerator: b fits.
        match unit.checked_mul(1u128 << bits) {
            Some(scaled) if numerator > scaled => {}
            _ => break,
        }
        if bits >= BUCKETED_FINAL_AGGREGATE_MAX_BITS {
            return Err(NoTuning::TooManyBits);
        }
        bits += 1;
    }

    let per_chunk = (share / 4) / ((1usize << bits) + 1);
    let mut rows = 0;
    let (mut low, mut high) = (1usize, BUCKET_SPILL_MAX_CHUNK_ROWS);
    while low <= high {
        let mid = low + (high - low) / 2;
        match routed_chunk_charge(&inputs.column_widths, mid) {
            Some(charge) if charge <= per_chunk => {
                rows = mid;
                low = mid + 1;
            }
            _ => high = mid - 1,
        }
    }
    if rows < BUCKET_SPILL_MIN_CHUNK_ROWS {
        return Err(NoTuning::ChunkTooSmall);
    }

    let partitions = inputs.partitions as u64;
    let descriptor_need = partitions
        .checked_mul(2u64 << bits)
        .and_then(|need| need.checked_add(partitions))
        .ok_or(NoTuning::Degenerate)?;
    let budget = inputs.nofile_soft
        / inputs
            .slots
            .max(1)
            .checked_mul(2)
            .ok_or(NoTuning::Degenerate)?;
    if descriptor_need > budget {
        return Err(NoTuning::DescriptorBudget);
    }
    Ok(BucketSpillTuning {
        bits,
        routed_chunk_rows: rows,
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
    /// The routed columns' widths, keys then state.
    pub column_widths: Vec<usize>,
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
        let column_widths = routed_types(exec.aggregate())
            .ok()
            .and_then(Result::ok)
            .ok_or(NoTuning::NotFixedWidth)?
            .into_iter()
            .map(|(_, width)| width)
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
            total_rows: scan.sample_count_total(),
        })
    }
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

    /// q33's routed columns: 8 + 4 B of keys, 8 + 8 + 16 + 8 B of state.
    const Q33_WIDTHS: [usize; 6] = [8, 4, 8, 8, 16, 8];
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
                descriptor_need: 1040,
            })
        );
    }

    /// The only q33 column whose chunk rows fall below 8192: 65 chunks of
    /// `routed_chunk_charge(.., rows)` within `floor(0.25 x 32,585,819)`,
    /// which counts each column's validity and 64-byte rounding, where
    /// `52 x rows` alone would give 2,410.
    #[test]
    fn bucket_spill_derivation_q33_c6a_large() {
        let inputs = q33(&LARGE, 524_288);
        assert_eq!(inputs.slots, 1);
        let tuning = derive_bucket_spill_tuning(&inputs).expect("tunes");
        assert_eq!(
            tuning,
            BucketSpillTuning {
                bits: 6,
                routed_chunk_rows: 2368,
                descriptor_need: 2064,
            }
        );
        let budget = 32_585_819 / 4 / 65;
        assert_eq!(routed_chunk_charge(&Q33_WIDTHS, 2368), Some(125_056));
        assert!(125_056 <= budget);
        assert!(routed_chunk_charge(&Q33_WIDTHS, 2369).expect("fits") > budget);
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
        // At bits 6 on c6a.large `S / 4 / 65` holds 2,368 rows of q33; at 7
        // it holds 1,188, and at 8 it holds under 600, below the 1024 floor.
        assert_eq!(
            refused(&|i| {
                i.query_limit = LARGE.query_limit;
                i.spill_consumers = 40;
                i.asked_bytes = Q33_ASKED * 4;
            }),
            NoTuning::ChunkTooSmall
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
        // `2 x slots` past u64.
        assert_eq!(refused(&|i| i.slots = u64::MAX), Err(NoTuning::Degenerate));
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
