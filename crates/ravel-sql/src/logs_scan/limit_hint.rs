use super::*;

use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};

/// Installs a non-truncating limit hint on each [`LogsScanExec`] that sits
/// under a `FilterExec` with a fetch, through pass-through nodes only, with no
/// TopK above (ADR-3326, serve-if-fits amendment).
///
/// A `LIKE` is never exact, so a `FilterExec` stays above its scan and
/// DataFusion's `LimitPushdown` stops there: `FilterExec` takes the fetch and
/// does not push it further. The scan's own `fetch` is an exact per-partition
/// cap and cannot be set below an inexact filter, so this rule copies the
/// filter's fetch into the scan's separate `limit_hint`, which never truncates
/// output and only spares the whole-segment fast path its verdict.
///
/// Pass-through nodes are those that neither reorder, drop nor aggregate
/// rows: `ProjectionExec`, `CooperativeExec`, and a round-robin
/// `RepartitionExec`. Any other node between the filter and the scan
/// installs nothing, and a `SortExec` with a fetch (a TopK) anywhere above
/// the scan installs nothing below it. A scan this rule does not reach with a
/// hint has its hint cleared. Appended after DataFusion's default rules, so
/// after `LimitPushdown`. Which statements get a hint is pinned by
/// `only_a_filter_fetch_over_pass_through_nodes_installs_the_hint` in
/// `tests/logs_limit_hint.rs`.
#[derive(Debug, Default)]
pub(crate) struct LogsLimitHint;

/// The name [`LogsLimitHint`] reports as a physical optimizer rule.
const LOGS_LIMIT_HINT_RULE: &str = "logs_limit_hint";

impl PhysicalOptimizerRule for LogsLimitHint {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        install(plan, None)
    }

    fn name(&self) -> &str {
        LOGS_LIMIT_HINT_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `plan` with every scan's hint set from `hint`, the fetch of the nearest
/// `FilterExec` above it when every node between them passes rows through
/// and no TopK sits above that filter.
fn install(plan: Arc<dyn ExecutionPlan>, hint: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = plan.downcast_ref::<LogsScanExec>() {
        if scan.limit_hint == hint {
            return Ok(plan);
        }
        return Ok(Arc::new(
            scan.sibling(scan.fetch, scan.topk.clone())
                .with_limit_hint(hint),
        ));
    }
    if plan
        .downcast_ref::<SortExec>()
        .is_some_and(|sort| sort.fetch().is_some())
    {
        return clear_below(plan);
    }
    let child_hint = if plan.is::<FilterExec>() {
        plan.fetch()
    } else if passes_rows_through(plan.as_ref()) {
        hint
    } else {
        None
    };
    let children = plan.children();
    let mut changed = false;
    let mut rebuilt = Vec::with_capacity(children.len());
    for child in children {
        let new = install(Arc::clone(child), child_hint)?;
        changed |= !Arc::ptr_eq(&new, child);
        rebuilt.push(new);
    }
    if changed {
        plan.replace_children(
            rebuilt,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    } else {
        Ok(plan)
    }
}

/// `plan` with no hint on any scan below it: the subtree of a TopK.
fn clear_below(plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(scan) = node.downcast_ref::<LogsScanExec>() else {
            return Ok(Transformed::no(node));
        };
        if scan.limit_hint.is_none() {
            return Ok(Transformed::no(node));
        }
        let cleared = scan
            .sibling(scan.fetch, scan.topk.clone())
            .with_limit_hint(None);
        Ok(Transformed::yes(Arc::new(cleared) as Arc<dyn ExecutionPlan>))
    })
    .data()
}

/// Whether `plan` emits every input row of a partition, in order, and nothing
/// else, so a filter fetch of `n` above it needs no more than it would
/// directly above the scan.
fn passes_rows_through(plan: &dyn ExecutionPlan) -> bool {
    if plan.is::<ProjectionExec>() || plan.is::<CooperativeExec>() {
        return true;
    }
    plan.downcast_ref::<RepartitionExec>()
        .is_some_and(|repartition| {
            matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_))
        })
}
