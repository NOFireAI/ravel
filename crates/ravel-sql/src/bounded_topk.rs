//! Bounded top-k grouped aggregation (issue #1402).
//!
//! # The problem
//!
//! `GROUP BY <high-cardinality key> ... ORDER BY <aggregate> DESC LIMIT k`
//! materialises one accumulator per distinct key even though the client asked
//! for `k` rows. The measured instance is ClickBench q33 (`GROUP BY
//! "WatchID","ClientIP"`, roughly 10^8 nearly-unique groups), whose
//! intermediate state reaches 10.09 GB to return 10 rows. ADR-0013 forbids the
//! obvious escape: budget exhaustion is an error, never a partial result, and
//! spilling stays disabled. The only admissible route is to stop building the
//! state in the first place.
//!
//! # What can and cannot be bounded exactly
//!
//! The pinned DataFusion (54.1) already ships the operator: an `AggregateExec`
//! carrying `LimitOptions` executes as `GroupedTopKAggregateStream`, a bounded
//! priority map of `k` groups instead of an unbounded group table. This rule
//! decides *when* ravel lets that happen; it does not reimplement it.
//!
//! The gate is narrower than issue #1402 proposed, because the property the
//! issue named ("the ordering aggregate is monotone non-decreasing in its
//! inputs, so a group evicted below the k-th cannot re-enter") is false. Take
//! `ORDER BY count(*) DESC LIMIT 1` over the row sequence `A, B, B, B`. After
//! row 1 the map holds `{A: 1}`. Row 2 opens group `B` with a running count of
//! `1`, which does not beat the retained `1`, so `B` is evicted; rows 3 and 4
//! evict it again. The bounded answer is `A` with 1, the true answer is `B`
//! with 3. Monotonicity says a group's running value is a LOWER bound on its
//! final value, and a lower bound is the wrong direction for pruning: nothing
//! in the stream bounds a group's future contribution from ABOVE, so an
//! accumulating aggregate (`count`, `sum`) cannot be pruned without either
//! approximation (forbidden: exact semantics by default) or a second pass.
//! `count`/`sum` are therefore NOT admitted here, and
//! `count_ordering_is_refused_and_still_returns_the_true_top_k` in
//! `tests/bounded_topk_aggregate.rs` is the pin that reddens if the gate is
//! ever widened to them.
//!
//! What *is* exact is a value-selective ordering aggregate: `max` under a
//! descending sort and `min` under an ascending one. Their per-group result is
//! the extreme of independently contributed row values rather than a running
//! accumulation, which is what makes eviction safe. See
//! [`BoundedTopKAggregate`]'s doc comment for the argument.
//!
//! # Why the gate is ravel's and not DataFusion's default
//!
//! `datafusion.optimizer.enable_topk_aggregation` defaults to `true`, so
//! DataFusion's own `TopKAggregation` rule was live in every ravel session,
//! ungated. Two of the shapes it admits are ones ravel must not take:
//!
//! - **Float `min`/`max`.** ADR-0023 gives ravel its own total-order min/max
//!   UDAF whose float comparison is `f64::total_cmp`, so `-0.0` and NaN
//!   payloads order deterministically. The priority map does its own
//!   comparisons and never constructs that accumulator, so a float `min`/`max`
//!   routed through it answers under DataFusion's ordering, not ravel's.
//!   [`crate::minmax::is_float`] inputs are refused here.
//! - **An unbounded `LIMIT`.** The bound this rule buys is proportional to the
//!   limit; at a large enough `k` the priority map is the group table with
//!   extra steps. [`crate::SqlConfig::bounded_topk_max_limit`] is the ceiling.
//!
//! So [`crate::session_config`] turns `enable_topk_aggregation` off for every
//! query and this rule re-admits exactly the vetted shape. `None` for the
//! config threshold does not install the rule at all, which is the operator
//! opt-out and the rule-off side of every test in
//! `tests/bounded_topk_aggregate.rs`.

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, LimitOptions, topk_types_supported};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;

use crate::minmax::is_float;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const BOUNDED_TOPK_AGGREGATE_RULE: &str = "bounded_topk_aggregate";

/// Bounds the intermediate state of a grouped aggregate whose only consumer is
/// a top-k sort, by handing DataFusion's `GroupedTopKAggregateStream` a limit
/// (issue #1402). See the module docs for the gate and for the aggregates it
/// refuses.
///
/// **Exactness.** The rule fires only for `max` under a descending sort and
/// `min` under an ascending one, where a group's result is the extreme of its
/// rows' independently contributed values: the priority map drops a group only
/// when its best value so far loses to the current k-th best, and that k-th
/// best only ever tightens as more rows arrive. A group whose true extreme
/// beats the FINAL k-th therefore also beat every earlier, weaker threshold, so
/// it was admitted at the moment that value arrived and carries exactly that
/// value, which is why no correct answer can be evicted and no evicted group
/// can have belonged in the answer.
#[derive(Debug)]
pub struct BoundedTopKAggregate {
    /// The largest `LIMIT` this rule will bound an aggregate for. See
    /// [`crate::SqlConfig::bounded_topk_max_limit`].
    max_limit: usize,
}

impl BoundedTopKAggregate {
    /// The rule with `max_limit` as its `LIMIT` ceiling.
    pub fn new(max_limit: usize) -> Self {
        BoundedTopKAggregate { max_limit }
    }
}

impl PhysicalOptimizerRule for BoundedTopKAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| self.rewrite(node)).data()
    }

    fn name(&self) -> &str {
        BOUNDED_TOPK_AGGREGATE_RULE
    }

    fn schema_check(&self) -> bool {
        // The rewrite sets an execution hint and rebuilds no schema, so let
        // DataFusion assert that: a rule that changed a column type here would
        // be changing a client-visible result.
        true
    }
}

impl BoundedTopKAggregate {
    /// Try to rewrite one node. Returns it unchanged unless it is a qualifying
    /// top-k sort over a qualifying grouped aggregate.
    fn rewrite(
        &self,
        node: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(sort) = node.downcast_ref::<SortExec>() else {
            return Ok(Transformed::no(node));
        };
        // No `fetch` is no top-k: the sort materialises every group whatever
        // this rule does, and there is no `k` to bound the aggregate by.
        let Some(limit) = sort.fetch() else {
            return Ok(Transformed::no(node));
        };
        if limit > self.max_limit {
            return Ok(Transformed::no(node));
        }
        // A per-partition sort keeps `fetch` rows PER partition; the bound the
        // doc comment argues is written for one stream of at most `k` rows.
        if sort.preserve_partitioning() {
            return Ok(Transformed::no(node));
        }
        // Exactly one ordering expression, and it must name a column: the gate
        // compares it against one aggregate's output field by name, and a
        // second ordering key would decide ties the priority map does not know
        // about.
        let ordering: &[_] = sort.expr();
        let [order] = ordering else {
            return Ok(Transformed::no(node));
        };
        let Some(order_column) = order.expr.downcast_ref::<Column>() else {
            return Ok(Transformed::no(node));
        };
        let descending = order.options.descending;

        // Walk down to every aggregate stage feeding this sort, tracking the
        // ordering column through renaming projections the way DataFusion's own
        // rule does. `refused` latches: once an unrecognised node is seen,
        // nothing below it is touched.
        let mut order_name = order_column.name().to_string();
        let mut refused = false;
        let mut bounded = false;
        let input = Arc::clone(sort.input())
            .transform_down(|inner| {
                if refused {
                    return Ok(Transformed::no(inner));
                }
                if let Some(aggregate) = inner.downcast_ref::<AggregateExec>() {
                    match bound_aggregate(aggregate, &order_name, descending, limit) {
                        Some(rewritten) => {
                            bounded = true;
                            return Ok(Transformed::yes(Arc::new(rewritten) as _));
                        }
                        None => refused = true,
                    }
                } else if let Some(projection) = inner.downcast_ref::<ProjectionExec>() {
                    for expr in projection.expr() {
                        if expr.alias == order_name
                            && let Some(source) = expr.expr.downcast_ref::<Column>()
                        {
                            order_name = source.name().to_string();
                        }
                    }
                } else if !is_pass_through(&inner) {
                    refused = true;
                }
                Ok(Transformed::no(inner))
            })
            .data()?;

        if !bounded {
            return Ok(Transformed::no(node));
        }
        let rewritten = SortExec::new(sort.expr().clone(), input)
            .with_fetch(sort.fetch())
            .with_preserve_partitioning(sort.preserve_partitioning());
        Ok(Transformed::yes(Arc::new(rewritten)))
    }
}

/// A node between the top-k sort and the aggregate that neither drops rows nor
/// renames the ordering column.
///
/// The allowlist is the mechanism, not a fallback: anything outside it refuses
/// the rewrite. A `FilterExec` is deliberately absent, and that is the `HAVING`
/// case -- a post-aggregation filter decides which groups reach the sort, so
/// keeping only `k` groups underneath it would discard groups the filter would
/// have let through. A node carrying its own `fetch` is refused for the same
/// reason.
fn is_pass_through(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.fetch().is_some() {
        return false;
    }
    // `CoalescePartitionsExec` collapses the partial stages under the final
    // one; `CooperativeExec` is DataFusion 54's yield wrapper. A
    // `RepartitionExec` of any partitioning preserves every row: it moves rows
    // between partitions, and the sort above it still takes the global top `k`
    // from whatever each partition kept.
    plan.is::<CoalescePartitionsExec>()
        || plan.is::<CooperativeExec>()
        || plan.is::<RepartitionExec>()
}

/// The gate, one conjunct per `return None`. `Some` is the aggregate rebuilt to
/// execute as a bounded priority map of `limit` groups.
fn bound_aggregate(
    aggregate: &AggregateExec,
    order_name: &str,
    descending: bool,
    limit: usize,
) -> Option<AggregateExec> {
    // Already carrying a limit: leave whoever set it alone.
    if aggregate.limit_options().is_some() {
        return None;
    }
    // The priority map is keyed by a single group value. A grouping set or a
    // null expression is a different group-key shape entirely.
    let group_by = aggregate.group_expr();
    if group_by.has_grouping_set() || !group_by.null_expr().is_empty() {
        return None;
    }
    let [(group_key, _)] = group_by.expr() else {
        return None;
    };
    // A `FILTER` clause makes the aggregate's value a function of rows the
    // priority map never sees it reject.
    if aggregate.filter_expr().iter().any(Option::is_some) {
        return None;
    }
    // The ordering aggregate must be the only aggregate, must be
    // value-selective (`min`/`max`, which is what `get_minmax_desc` reports),
    // must be sorted in the direction its own extreme runs, and must be the
    // column the sort actually orders by. An accumulating aggregate (`count`,
    // `sum`) reports nothing here and is refused; the module docs give the
    // counterexample that makes that a correctness gate rather than a
    // conservative default.
    let (field, aggregate_descending) = aggregate.get_minmax_desc()?;
    if aggregate_descending != descending || field.name() != order_name {
        return None;
    }
    // ADR-0023: ravel's float min/max is a total order the priority map does
    // not implement. Refusing float input keeps the answer ravel's.
    if is_float(field.data_type()) {
        return None;
    }
    let key_type = group_key.data_type(&aggregate.input().schema()).ok()?;
    if !topk_types_supported(&key_type, field.data_type()) {
        return None;
    }
    Some(AggregateExec::with_new_limit_options(
        aggregate,
        Some(LimitOptions::new_with_order(limit, descending)),
    ))
}
