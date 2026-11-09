//! Widen the hash exchange under a final grouped aggregate to a partition
//! floor (issue #4014, ADR-1195 amendment of 2026-11-09).
//!
//! A high-cardinality grouped aggregate builds one final hash table per
//! output partition of the exchange below it, and the process budget holds
//! the next doubling of the widest of them in reserve. More final partitions
//! make each table smaller, so the statement answers in memory under a
//! smaller budget. Every other operator, the scans and the partial aggregate
//! included, keeps the session's `target_partitions`.
//!
//! [`FinalExchangeFloor`] rewrites exactly one shape: an `AggregateExec` in
//! mode `FinalPartitioned` whose direct child is a `RepartitionExec` with
//! `Partitioning::Hash(exprs, n)` and `n` below the floor. The exchange is
//! rebuilt as `Hash(exprs, floor)` over the same input, keeping
//! `preserve_order`, and the aggregate is rebuilt over it. It never inserts an
//! exchange: a statement with no hash exchange (one whose aggregates are not
//! exact-typed, ADR-0094, or a spill-enabled attempt, ADR-0954) keeps its plan.
//! A floor of 0 turns it off.
//!
//! The answer does not depend on the count: the exchange still routes every
//! row of a group to one partition, by the same hash expressions, and a
//! `FinalPartitioned` aggregate merges the partial states of each group it
//! receives whatever the partition count. The operators above the aggregate
//! read its output partitioning from the rebuilt node.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result as DFResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, Partitioning, ReplaceChildrenOptions,
};

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const FINAL_EXCHANGE_FLOOR_RULE: &str = "ravel_final_exchange_floor";

/// Raises the partition count of the hash exchange feeding a
/// `FinalPartitioned` aggregate to `floor`. See the module docs.
#[derive(Debug)]
pub struct FinalExchangeFloor {
    /// See [`crate::SqlConfig::first_attempt_min_partitions`].
    floor: usize,
}

impl FinalExchangeFloor {
    /// The rule that widens such an exchange to at least `floor` partitions.
    pub fn new(floor: usize) -> Self {
        FinalExchangeFloor { floor }
    }

    fn rewrite(
        &self,
        node: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(aggregate) = node.downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(node));
        };
        if *aggregate.mode() != AggregateMode::FinalPartitioned {
            return Ok(Transformed::no(node));
        }
        let Some(exchange) = aggregate.input().downcast_ref::<RepartitionExec>() else {
            return Ok(Transformed::no(node));
        };
        let Partitioning::Hash(exprs, n) = exchange.partitioning() else {
            return Ok(Transformed::no(node));
        };
        if *n >= self.floor {
            return Ok(Transformed::no(node));
        }
        let widened = RepartitionExec::try_new(
            Arc::clone(exchange.input()),
            Partitioning::Hash(exprs.clone(), self.floor),
        )?;
        let widened = if exchange.preserve_order() {
            widened.with_preserve_order()
        } else {
            widened
        };
        let rebuilt = Arc::clone(&node).replace_children(
            vec![Arc::new(widened)],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        Ok(Transformed::yes(rebuilt))
    }
}

impl PhysicalOptimizerRule for FinalExchangeFloor {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if self.floor == 0 {
            return Ok(plan);
        }
        plan.transform_up(|node| self.rewrite(node)).data()
    }

    fn name(&self) -> &str {
        FINAL_EXCHANGE_FLOOR_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::{ExecutionPlanProperties, displayable};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    /// An empty `partitions`-partition source, sorted on `k` when `sorted`.
    fn scan(partitions: usize, sorted: bool) -> Arc<dyn ExecutionPlan> {
        let empty = vec![Vec::new(); partitions];
        let source = MemorySourceConfig::try_new(&empty, schema(), None).expect("source");
        let source = if sorted {
            let ordering = LexOrdering::new([PhysicalSortExpr::new_default(
                col("k", &schema()).expect("k"),
            )])
            .expect("one sort key");
            source
                .try_with_sort_information(vec![ordering])
                .expect("sorted source")
        } else {
            source
        };
        DataSourceExec::from_data_source(source)
    }

    fn count_v(input_schema: &SchemaRef) -> Vec<Arc<AggregateFunctionExpr>> {
        vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("v", input_schema).expect("v")])
                .schema(Arc::clone(input_schema))
                .alias("count(v)")
                .build()
                .expect("count"),
        )]
    }

    fn group_by_k(schema: &SchemaRef) -> PhysicalGroupBy {
        PhysicalGroupBy::new_single(vec![(col("k", schema).expect("k"), "k".to_string())])
    }

    fn aggregate(
        mode: AggregateMode,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Arc<dyn ExecutionPlan> {
        let group_by = group_by_k(&input.schema());
        Arc::new(
            AggregateExec::try_new(
                mode,
                group_by,
                count_v(&input_schema),
                vec![None],
                input,
                input_schema,
            )
            .expect("aggregate"),
        )
    }

    /// `Hash([k], n)` over `input`, order-preserving when `preserve_order`.
    fn exchange(
        input: Arc<dyn ExecutionPlan>,
        n: usize,
        preserve_order: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let keys = vec![col("k", &input.schema()).expect("k")];
        let exchange =
            RepartitionExec::try_new(input, Partitioning::Hash(keys, n)).expect("repartition");
        Arc::new(if preserve_order {
            exchange.with_preserve_order()
        } else {
            exchange
        })
    }

    /// `AggregateExec(Partial) -> Hash([k], n) -> AggregateExec(FinalPartitioned)`
    /// over a 4-partition source: the plan `EnforceDistribution` builds for an
    /// exact-typed `GROUP BY k` at `n` target partitions.
    fn final_over_hash(n: usize, sorted: bool) -> Arc<dyn ExecutionPlan> {
        let source = scan(4, sorted);
        let input_schema = source.schema();
        let partial = aggregate(AggregateMode::Partial, source, Arc::clone(&input_schema));
        aggregate(
            AggregateMode::FinalPartitioned,
            exchange(partial, n, sorted),
            input_schema,
        )
    }

    fn optimize(floor: usize, plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        FinalExchangeFloor::new(floor)
            .optimize(plan, &ConfigOptions::default())
            .expect("rule runs")
    }

    fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    /// The direct child of `plan`, which must be a `RepartitionExec`.
    fn child_exchange(plan: &Arc<dyn ExecutionPlan>) -> &RepartitionExec {
        plan.children()[0]
            .downcast_ref::<RepartitionExec>()
            .expect("the child is an exchange")
    }

    /// The exchange under a `FinalPartitioned` aggregate goes from 8 to 16
    /// partitions, on the same keys, and the aggregate reports 16.
    ///
    /// FLIP: return `Transformed::no` before rebuilding and both counts stay 8.
    #[test]
    fn the_exchange_under_a_final_partitioned_aggregate_is_widened_to_the_floor() {
        let after = optimize(16, final_over_hash(8, false));
        let widened = child_exchange(&after);
        let Partitioning::Hash(exprs, n) = widened.partitioning() else {
            panic!("still a hash exchange:\n{}", plan_text(&after));
        };
        assert_eq!(*n, 16, "{}", plan_text(&after));
        assert_eq!(
            format!("{exprs:?}"),
            format!("{:?}", [col("k", &schema()).expect("k")])
        );
        assert_eq!(after.output_partitioning().partition_count(), 16);
        assert_eq!(
            widened.input().output_partitioning().partition_count(),
            4,
            "the partial aggregate keeps its source's partitions"
        );
    }

    /// An order-preserving exchange stays order-preserving when widened.
    ///
    /// FLIP: drop the `with_preserve_order` branch in `rewrite` and the
    /// widened exchange no longer preserves order.
    #[test]
    fn a_widened_exchange_keeps_preserve_order() {
        let before = final_over_hash(8, true);
        assert!(
            child_exchange(&before).preserve_order(),
            "fixture is ordered"
        );
        let after = optimize(16, before);
        let widened = child_exchange(&after);
        assert_eq!(widened.partitioning().partition_count(), 16);
        assert!(widened.preserve_order(), "{}", plan_text(&after));
    }

    /// A hash exchange whose parent is a `Partial` aggregate, or no aggregate
    /// at all, is left as it is.
    ///
    /// FLIP: drop the mode check (rewrite under any aggregate) and the first
    /// plan changes; rewrite every hash `RepartitionExec` and both do.
    #[test]
    fn an_exchange_under_another_parent_is_left_alone() {
        let source = scan(4, false);
        let input_schema = source.schema();
        let under_partial = aggregate(
            AggregateMode::Partial,
            exchange(source, 8, false),
            input_schema,
        );
        let under_nothing = exchange(scan(4, false), 8, false);
        for plan in [under_partial, under_nothing] {
            let before = plan_text(&plan);
            assert_eq!(plan_text(&optimize(16, plan)), before);
        }
    }

    /// A floor of 0, and an exchange already at or above the floor, leave the
    /// plan as it is.
    ///
    /// FLIP: drop the floor-0 return and the `n >= floor` check, so `n` is
    /// always set to the floor, and the 0 case no longer reads 8 and the 32
    /// case narrows to 16.
    #[test]
    fn the_rule_is_a_no_op_at_floor_zero_and_above_the_floor() {
        for (floor, n) in [(0, 8), (16, 32), (16, 16)] {
            let plan = final_over_hash(n, false);
            let before = plan_text(&plan);
            let after = optimize(floor, plan);
            assert_eq!(plan_text(&after), before, "floor {floor}, n {n}");
            assert_eq!(child_exchange(&after).partitioning().partition_count(), n);
        }
    }
}
