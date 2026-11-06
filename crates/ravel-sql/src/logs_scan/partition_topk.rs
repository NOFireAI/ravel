use super::*;

use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};

/// Inserts a per-partition TopK under the global TopK of a row-filtered
/// [`LogsScanExec`] (ADR-0093, exact not-equal amendment).
///
/// Ravel plans with `repartition_sorts` off, so `ORDER BY <col> LIMIT k` over
/// a scan is one `SortExec` with a fetch over a `CoalescePartitionsExec`. A
/// scan that evaluates its row filter itself emits narrowed batches that still
/// carry their block's full dictionary values, and that one TopK would read
/// every one of them. This rule matches a `SortExec` with `fetch: Some(k)` and
/// `preserve_partitioning: false` over a fetch-less `CoalescePartitionsExec`,
/// below which only `ProjectionExec` and `CooperativeExec` lead to a scan with
/// a non-empty row filter and no active TopK threshold, and puts a `SortExec`
/// with the same sort expressions, the same fetch and `preserve_partitioning:
/// true` directly under the coalesce. The global `SortExec` stays: the top `k`
/// of the union is the top `k` of every partition's top `k`. Any other shape
/// is returned unchanged. Pinned by
/// `a_row_filtered_scan_under_a_global_topk_gets_a_partition_topk` and its
/// neighbours in `tests/logs_partition_topk.rs`.
#[derive(Debug, Default)]
pub(crate) struct LogsPartitionTopK;

/// The name [`LogsPartitionTopK`] reports as a physical optimizer rule.
const LOGS_PARTITION_TOPK_RULE: &str = "logs_partition_topk";

impl PhysicalOptimizerRule for LogsPartitionTopK {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| match partition_topk(&node)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(node)),
        })
        .data()
    }

    fn name(&self) -> &str {
        LOGS_PARTITION_TOPK_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `node` with a partition TopK inserted under its coalesce, when `node` is
/// the global TopK of a matching shape.
fn partition_topk(node: &Arc<dyn ExecutionPlan>) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let Some(sort) = node.downcast_ref::<SortExec>() else {
        return Ok(None);
    };
    let Some(fetch) = sort.fetch() else {
        return Ok(None);
    };
    if sort.preserve_partitioning() {
        return Ok(None);
    }
    let coalesce = sort.input();
    let Some(coalesce_exec) = coalesce.downcast_ref::<CoalescePartitionsExec>() else {
        return Ok(None);
    };
    if coalesce_exec.fetch().is_some() || !reaches_row_filtered_scan(coalesce_exec.input()) {
        return Ok(None);
    }
    let partition_sort = SortExec::new(sort.expr().clone(), Arc::clone(coalesce_exec.input()))
        .with_fetch(Some(fetch))
        .with_preserve_partitioning(true);
    let recompute = || ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute);
    let coalesce =
        Arc::clone(coalesce).replace_children(vec![Arc::new(partition_sort)], recompute())?;
    Arc::clone(node)
        .replace_children(vec![coalesce], recompute())
        .map(Some)
}

/// Whether `plan` is, through `ProjectionExec` and `CooperativeExec` only, a
/// scan with a row filter and no active TopK threshold.
fn reaches_row_filtered_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut plan = plan;
    loop {
        if let Some(scan) = plan.downcast_ref::<LogsScanExec>() {
            return !scan.row_filters.is_empty() && scan.active_topk().is_none();
        }
        if !(plan.is::<ProjectionExec>() || plan.is::<CooperativeExec>()) {
            return false;
        }
        match plan.children().as_slice() {
            [child] => plan = child,
            _ => return false,
        }
    }
}
