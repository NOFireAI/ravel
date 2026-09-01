//! Keep dictionary-encoded string group keys off DataFusion's `RowConverter`
//! aggregation path (issue #737).
//!
//! # The path a `GROUP BY` over a declared `Str` column takes
//!
//! A declared `Str` attribute column reaches Arrow as `Dictionary(Int32,
//! Utf8)` (ADR-0099 decision 5, [`crate::declared::DeclaredType::Str`]). That
//! type is a wire contract for what a client receives, and DataFusion 54 has
//! no specialized group-value table for it: `Dictionary` appears in neither
//! the single-column dispatch in
//! `datafusion_physical_plan::aggregates::group_values::new_group_values` nor
//! the `supported_type` list its multi-column `GroupValuesColumn` is built
//! from. Both fall through to `GroupValuesRows`, which encodes every group key
//! into arrow's comparable row format and decodes it back on emit.
//!
//! The decode is the problem. `GroupValuesRows::emit(EmitTo::All)` hands the
//! whole table to `RowConverter::convert_rows`, and for a `Utf8` inner type
//! that lands in `arrow_row::variable::decode_binary::<i32>`, which appends
//! `i32` offsets into a single values buffer. One `Utf8` array cannot hold
//! more than `i32::MAX` bytes, and the group table is decoded as one array,
//! not in `batch_size` slices: the slicing in
//! `aggregates::row_hash::GroupedHashAggregateStream` happens after the emit,
//! on the batch the emit already built. A tenant with roughly ten million
//! distinct URLs averaging 215 bytes crosses the limit and arrow panics with
//! `offset overflow`. Nothing bounds it first. The memory pool cannot: the
//! reservation is released before the emit. `EmitTo::First` would, but it is
//! reachable only under a `GroupOrdering` or the early-emit path.
//!
//! # The rewrite
//!
//! [`DictionaryGroupKeysAsViews`] is a physical optimizer rule that casts
//! every `Dictionary(_, Utf8 | LargeUtf8)` group key to `Utf8View` on the
//! aggregate that first reads it, and casts the emitted group column straight
//! back to its declared type in a projection directly above the aggregate that
//! produces the final values. `Utf8View` is in both dispatch tables, so the
//! aggregate uses `GroupValuesBytesView`, whose emitted array keeps its values
//! in a list of 2 MiB blocks addressed by a per-view buffer index. There is no
//! single offset buffer to overflow.
//!
//! `Utf8` would also leave the `RowConverter` path, and it is deliberately not
//! what this rule casts to: `GroupValuesBytes::<i32>` has the same `i32`
//! offsets in one values buffer, so it moves the panic rather than removing
//! it.
//!
//! Both casts are cheap. Arrow special-cases `Dictionary(_, Utf8)` to
//! `Utf8View` (`arrow_cast::cast::dictionary::dictionary_cast`) by reusing the
//! dictionary's values buffer and building views over it, with no string data
//! copied. The cast back runs on the emitted group column, which the aggregate
//! stream has already sliced to `batch_size` rows, so the `Utf8` values buffer
//! it packs is bounded by one output batch.
//!
//! # What the rule leaves alone
//!
//! - Grouping sets (`ROLLUP`, `CUBE`, `GROUPING SETS`). Their per-set NULL
//!   placeholder expressions are typed to match each group expression, and
//!   rewriting one side without the other would desynchronize them.
//! - Any aggregate that already has an output ordering. Such an aggregate runs
//!   under a `GroupOrdering`, which reaches `EmitTo::First` and so is already
//!   bounded, and a cast in a projection above it is not guaranteed to carry
//!   the ordering equivalence its parent may require.
//!
//! Everything else is unconditional: correctness of the rewrite does not
//! depend on how many distinct keys a query actually has, so there is no
//! threshold to get wrong.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateOutputMode, PhysicalGroupBy};
use datafusion::physical_plan::projection::ProjectionExec;

/// The in-memory type a dictionary-encoded string group key is grouped as.
/// Nothing outside the aggregate observes it: the projection this rule inserts
/// restores the aggregate's declared output type in the same plan.
const GROUP_KEY_TYPE: DataType = DataType::Utf8View;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const DICTIONARY_GROUP_KEYS_RULE: &str = "dictionary_group_keys_as_views";

/// Rewrites dictionary-encoded string group keys to `Utf8View` for the
/// duration of an aggregation. See the module docs for why.
#[derive(Debug, Default)]
pub struct DictionaryGroupKeysAsViews;

impl PhysicalOptimizerRule for DictionaryGroupKeysAsViews {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(rewrite_aggregate).data()
    }

    fn name(&self) -> &str {
        DICTIONARY_GROUP_KEYS_RULE
    }

    fn schema_check(&self) -> bool {
        // The rule is required to be schema-preserving end to end, so let
        // DataFusion assert it: a projection that failed to restore a declared
        // column's type is a client-visible regression, not an internal detail.
        true
    }
}

/// True for the dictionary-encoded string types this rule redirects.
fn is_dictionary_string(ty: &DataType) -> bool {
    matches!(ty, DataType::Dictionary(_, value)
        if matches!(**value, DataType::Utf8 | DataType::LargeUtf8))
}

/// Rewrite one plan node, bottom-up.
///
/// Two things happen here, and both are driven by the *current* input schema
/// rather than by what this rule did to a lower node. That is what keeps a
/// two-stage aggregation consistent: if the `Partial` stage was left alone,
/// the `Final` stage above it sees a dictionary input and is left alone too.
///
/// 1. Cast dictionary-string group keys to `Utf8View`.
/// 2. Rebuild the aggregate whenever its recorded schema disagrees with what
///    its group expressions now produce. `AggregateExec::with_new_children`,
///    which the tree walk used to install the rewritten child, carries the old
///    schema forward verbatim; a `Final` stage over a rewritten `Partial`
///    would otherwise claim `Dictionary` while its group values are
///    `Utf8View`.
fn rewrite_aggregate(
    node: Arc<dyn ExecutionPlan>,
) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(agg) = node.downcast_ref::<AggregateExec>() else {
        return Ok(Transformed::no(node));
    };
    let group_by = agg.group_expr();
    if group_by.has_grouping_set() || !group_by.null_expr().is_empty() {
        return Ok(Transformed::no(node));
    }

    let input_schema = agg.input().schema();
    let declared_schema = agg.schema();

    // Only cast where the aggregate reads a dictionary column directly. A
    // `Final` stage's group expressions are plain column references into a
    // rewritten `Partial` output and are already `Utf8View` here, so this
    // finds nothing and only the stale-schema rebuild below applies.
    let ordered = agg.properties().output_ordering().is_some();
    let mut cast_any = false;
    let mut exprs = Vec::with_capacity(group_by.expr().len());
    for (expr, name) in group_by.expr() {
        let ty = expr.data_type(&input_schema)?;
        if !ordered && is_dictionary_string(&ty) {
            let cast = CastExpr::new(Arc::clone(expr), GROUP_KEY_TYPE, None);
            exprs.push((Arc::new(cast) as Arc<dyn PhysicalExpr>, name.clone()));
            cast_any = true;
        } else {
            exprs.push((Arc::clone(expr), name.clone()));
        }
    }

    if !cast_any && !group_schema_is_stale(group_by, &input_schema, &declared_schema)? {
        return Ok(Transformed::no(node));
    }

    let rebuilt = AggregateExec::try_new(
        *agg.mode(),
        PhysicalGroupBy::new(exprs, Vec::new(), group_by.groups().to_vec(), false),
        agg.aggr_expr().to_vec(),
        agg.filter_expr().to_vec(),
        Arc::clone(agg.input()),
        agg.input_schema(),
    )?
    .with_limit_options(agg.limit_options());
    let rebuilt: Arc<dyn ExecutionPlan> = Arc::new(rebuilt);

    if agg.mode().output_mode() == AggregateOutputMode::Partial {
        // A partial stage's group columns are consumed only by the stage above
        // it, which this same walk reaches next. Leave them as views so that
        // stage groups on views too.
        return Ok(Transformed::yes(rebuilt));
    }
    Ok(Transformed::yes(restore_schema(rebuilt, &declared_schema)?))
}

/// True when at least one group column of `declared_schema` no longer matches
/// the type its group expression produces over `input_schema`.
fn group_schema_is_stale(
    group_by: &PhysicalGroupBy,
    input_schema: &SchemaRef,
    declared_schema: &SchemaRef,
) -> DFResult<bool> {
    for (idx, (expr, _)) in group_by.expr().iter().enumerate() {
        // A schema narrower than its own group-by list is not something this
        // rule can repair; leave the node alone rather than index past the end.
        let Some(field) = declared_schema.fields().get(idx) else {
            return Ok(false);
        };
        if expr.data_type(input_schema)? != *field.data_type() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Project `plan` back onto `target`, casting every column whose type this
/// rule moved and passing the rest through by reference.
///
/// The projection is positional and name-preserving, so every expression above
/// it resolves the same column at the same index with the same type: the
/// rewrite is invisible to the rest of the plan and to the client.
fn restore_schema(
    plan: Arc<dyn ExecutionPlan>,
    target: &SchemaRef,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let current = plan.schema();
    if current.fields() == target.fields() {
        return Ok(plan);
    }
    // Rebuilding an aggregate changes its group column types and nothing else,
    // so the widths always agree. Say so as an error rather than as an index:
    // a future DataFusion whose `create_schema` adds a column would otherwise
    // panic here, in the code whose whole point is that a query does not.
    if current.fields().len() != target.fields().len() {
        return Err(datafusion::error::DataFusionError::Internal(format!(
            "{DICTIONARY_GROUP_KEYS_RULE}: rebuilt aggregate has {} columns, expected {}",
            current.fields().len(),
            target.fields().len()
        )));
    }
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(target.fields().len());
    for (idx, field) in target.fields().iter().enumerate() {
        let column = Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>;
        let expr = if current.field(idx).data_type() == field.data_type() {
            column
        } else {
            Arc::new(CastExpr::new(column, field.data_type().clone(), None))
                as Arc<dyn PhysicalExpr>
        };
        exprs.push((expr, field.name().clone()));
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{ArrayRef, DictionaryArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Int32Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    fn dict_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    /// A session over one batch with a `Dictionary(Int32, Utf8)` key column and
    /// an `Int64` payload. `with_rule` decides whether the rule under test is
    /// installed, so the two halves of the differential differ in nothing else.
    fn dict_context(with_rule: bool) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", dict_type(), true),
            Field::new("v", DataType::Int64, false),
        ]));
        let keys: DictionaryArray<Int32Type> =
            vec![Some("a"), Some("b"), Some("a")].into_iter().collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(keys) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .expect("fixture batch builds");
        let ctx = context(with_rule);
        ctx.register_batch("t", batch).expect("table registers");
        ctx
    }

    /// A default session, optionally carrying the rule, built the same way
    /// `crate::session::build_session` does.
    fn context(with_rule: bool) -> SessionContext {
        let mut builder = SessionStateBuilder::new().with_default_features();
        if with_rule {
            builder = builder.with_physical_optimizer_rule(Arc::new(DictionaryGroupKeysAsViews));
        }
        SessionContext::new_with_state(builder.build())
    }

    fn collect_aggregates(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
        if plan.is::<AggregateExec>() {
            out.push(Arc::clone(plan));
        }
        for child in plan.children() {
            collect_aggregates(child, out);
        }
    }

    #[tokio::test]
    async fn dictionary_group_key_groups_on_views_and_reads_back_as_a_dictionary() {
        let ctx = dict_context(true);
        let df = ctx
            .sql("SELECT k, sum(v) AS total FROM t GROUP BY k")
            .await
            .expect("query plans");
        let logical_schema = df.schema().as_arrow().clone();
        let plan = df.create_physical_plan().await.expect("physical plan");

        // The plan's output type is untouched: a caller still sees the
        // declared dictionary column, and it still agrees with the logical
        // schema DataFusion promised at plan time.
        assert_eq!(plan.schema().field(0).data_type(), &dict_type());
        assert_eq!(logical_schema.field(0).data_type(), &dict_type());

        // ...and every aggregate inside it groups on views.
        let mut aggregates = Vec::new();
        collect_aggregates(&plan, &mut aggregates);
        assert!(!aggregates.is_empty(), "the plan must contain an aggregate");
        for agg in &aggregates {
            assert_eq!(
                agg.schema().field(0).data_type(),
                &DataType::Utf8View,
                "an aggregate still groups on a dictionary: {agg:?}"
            );
        }
    }

    #[tokio::test]
    async fn results_and_types_match_the_unrewritten_plan() {
        let sql = "SELECT k, sum(v) AS total FROM t GROUP BY k ORDER BY k";

        let baseline = dict_context(false).sql(sql).await.expect("baseline plans");
        let before_schema = baseline.schema().as_arrow().clone();
        let before = baseline.collect().await.expect("baseline runs");

        let rewritten = dict_context(true).sql(sql).await.expect("rewritten plans");
        let after_schema = rewritten.schema().as_arrow().clone();
        let after = rewritten.collect().await.expect("rewritten runs");

        assert_eq!(before_schema, after_schema);
        assert_eq!(before, after);
        // The baseline really is the dictionary path this rule exists to avoid.
        assert_eq!(before_schema.field(0).data_type(), &dict_type());
    }

    #[tokio::test]
    async fn a_non_dictionary_group_key_is_untouched() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .expect("fixture batch builds");
        let ctx = context(true);
        ctx.register_batch("t", batch).expect("table registers");

        let plan = ctx
            .sql("SELECT k, sum(v) FROM t GROUP BY k")
            .await
            .expect("query plans")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let mut aggregates = Vec::new();
        collect_aggregates(&plan, &mut aggregates);
        assert!(!aggregates.is_empty());
        for agg in &aggregates {
            assert_eq!(agg.schema().field(0).data_type(), &DataType::Utf8);
        }
        // No projection was inserted above the aggregate: the rule declined.
        assert_eq!(plan.schema().field(0).data_type(), &DataType::Utf8);
    }
}
