//! Keep dictionary-encoded string group keys off DataFusion's `RowConverter`
//! aggregation path (issue #737).
//!
//! # The path a `GROUP BY` over a declared `Str` column takes
//!
//! A declared `Str` attribute column reaches Arrow as `Dictionary(Int32,
//! Utf8)` (ADR-0099 decision 5, [`crate::declared::DeclaredType::Str`]). That
//! type is a wire contract for what a client receives, and DataFusion 55 has
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
//! not in `batch_size` slices: DataFusion 55's `MaterializedAggregateOutput`
//! slices after the emit, the batch the emit already built. A tenant with
//! roughly ten million distinct URLs averaging 215 bytes crosses the limit and
//! arrow panics with `offset overflow`. Nothing bounds it first. The memory
//! pool cannot: the decode allocates before the stream next updates its
//! reservation. `EmitTo::First` would, but it is
//! reachable only under a `GroupOrdering` or the early-emit path.
//!
//! # The rewrite
//!
//! [`DictionaryGroupKeysAsViews`] is a physical optimizer rule that casts
//! every `Dictionary(_, Utf8 | LargeUtf8)` group key to `Utf8View` on the
//! aggregate that first reads it, carries the emitted `Utf8View` column up
//! through the operators above that aggregate whose every expression reading
//! it is the bare column, and casts it back to its declared type in one
//! projection:
//! under the first ancestor outside that allowlist, or above the plan root
//! when there is none (see "Where the cast back runs" below). `Utf8View` is in
//! both dispatch tables, so the aggregate uses `GroupValuesBytesView`, whose
//! emitted array keeps its values in a list of 2 MiB blocks addressed by a
//! per-view buffer index. There is no single offset buffer to overflow.
//!
//! `Utf8` would also leave the `RowConverter` path, and it is deliberately not
//! what this rule casts to: `GroupValuesBytes::<i32>` has the same `i32`
//! offsets in one values buffer, so it moves the panic rather than removing
//! it.
//!
//! Both casts are cheap. Arrow special-cases `Dictionary(_, Utf8)` to
//! `Utf8View` (`arrow_cast::cast::dictionary::dictionary_cast`) by reusing the
//! dictionary's values buffer and building views over it, with no string data
//! copied. The cast back runs on the batches the topmost carried operator
//! emits, one batch at a time, so the `Utf8` values buffer it packs is bounded
//! by the rows of one such batch. Under `ORDER BY <aggregate> ... LIMIT k`,
//! when every sort key that reads the group key is the bare column, the
//! topmost carried operator is the TopK or the merge above it, so the cast
//! runs on the `k` rows the statement returns, not on one row per distinct
//! group.
//!
//! # Where the cast back runs
//!
//! The rule walks the plan bottom-up. An aggregate it rebuilt reports its
//! group column as `Utf8View`, and so does every operator above it that
//! carries the column on. The parent of each such node decides: an operator in
//! the allowlist below is rebuilt over the carried input so that its own
//! schema says `Utf8View` too, and the decision moves up to its parent; any
//! other operator gets one projection restoring the declared types placed
//! directly over that input, so a `HAVING` filter, a window, a union or a join
//! that does not carry (see "Joins" below) reads the declared type. Above the
//! plan root the same projection goes in,
//! so the plan still produces the schema DataFusion promised at plan time,
//! which the rule's `schema_check` asserts.
//!
//! An operator that evaluates expressions over its input carries the views
//! only when every one of those expressions that reads a carried column,
//! anywhere in its tree, is that bare column. A computed expression was
//! planned against the declared type: `k = 'a'` on a dictionary column
//! compares with a dictionary literal, and over views arrow refuses it at
//! execution (`Invalid comparison operation: Utf8View == Utf8`). Under such an
//! operator the restore goes directly over its input, as under any operator
//! outside the allowlist.
//!
//! The allowlist:
//!
//! - `CoalescePartitionsExec`, `GlobalLimitExec`, `LocalLimitExec` and
//!   `CooperativeExec`, with or without a fetch: they evaluate no expression
//!   and output their input's columns as they are.
//! - `SortPreservingMergeExec`, when every expression it orders by that reads
//!   a carried column is that bare column.
//! - `SortExec`, with or without a fetch, under the same condition on its
//!   ordering, and except a TopK (a sort with a fetch) that orders by a carried
//!   column and by no value an aggregate computed. A TopK publishes a dynamic
//!   filter over the columns it orders by; one that reads only group keys
//!   could be pushed below the aggregate to the scan, and this rule does not
//!   show that a threshold read off the views compares correctly with the
//!   scan's dictionary column there. A filter that also reads a computed value
//!   (`ORDER BY count(*) DESC`, with or without the key as a tie-breaker)
//!   cannot be evaluated below the aggregate, so it stays above it.
//! - `RepartitionExec`, when every expression it hash-partitions by and, for
//!   one that preserves order, every expression of its input's ordering that
//!   reads a carried column is that bare column; and except one that
//!   hash-partitions on a carried column under a parent that is not an
//!   aggregate. This rule does not show that a
//!   view and a dictionary of the same string hash alike, and a join needs
//!   both of its inputs partitioned by the same hash; an aggregate above the
//!   exchange sees every row through the same one.
//! - `ProjectionExec`, when every expression that reads a carried column is
//!   that bare column. It is rebuilt with `ProjectionExec::try_new`:
//!   `with_new_children` keeps the output schema it computed at plan time,
//!   which would claim `Dictionary` over batches of views.
//! - `AggregateExec`, when it reads the carried columns only as bare-column
//!   group keys, as the outer level of a `COUNT(DISTINCT ..)` planned as two
//!   aggregates does. It is rebuilt like any other aggregate and groups on the
//!   views directly, with no second cast.
//! - `HashJoinExec` in `PartitionMode::Partitioned`, under the conditions in
//!   "Joins" below, which it checks for both inputs at once.
//!
//! Any rebuilt operator other than a projection, an aggregate or a hash join
//! must report its input's schema as its own; a rebuilt hash join must report
//! the join schema of its new inputs. One that does not gets the restore below
//! it instead.
//!
//! # Joins
//!
//! A hash join in `Partitioned` mode requires both inputs hash-partitioned on
//! its keys into the same partitions. The exchange below a grouped input
//! hashes the views it carries, and this rule does not show that a view and a
//! dictionary of the same string hash alike, so the two inputs must hash the
//! same type on every key that reads a carried column. A cast in a projection
//! also drops its input's hash partitioning when it casts a partitioning
//! column, so a restore directly under the join would leave that input with
//! unknown partitioning, which the join's executable-plan invariant refuses.
//! A plan takes one of two paths.
//!
//! - The join carries the views when, for every key that reads a carried
//!   column, both inputs carry views and the key is that bare column on both
//!   sides; its join filter reads no carried column; and no node below it
//!   holds a dynamic filter the join publishes, which would compare the views'
//!   bounds and hashes with a column that may be a dictionary below an
//!   aggregate. Both inputs then hash and match views, through exchanges that
//!   carry them, and the one restore runs above the join.
//! - Otherwise the join does not carry, even when one input carries views.
//!   Each carrying input is restored at the first operator above its aggregate
//!   that does not carry, as anywhere else, and when that leaves the join's
//!   input with a partitioning other than the hash partitioning it had, the
//!   restored input is exchanged again with the same hash expressions and
//!   partition count. That exchange hashes the declared type, as the other
//!   input does, so both inputs hash alike. The exchange preserves order when
//!   its input is ordered, since the join reports its probe input's ordering
//!   as its own. The exchange between a partial and a final aggregate carries
//!   the views, its parent being an aggregate, so outside strict mode the
//!   restore lands above the final aggregate, not between it and that
//!   exchange.
//!
//! A hash join in `CollectLeft` mode, the mode `SqlExecutor`'s session plans
//! its hash joins in (it turns `repartition_joins` off), and
//! `SortMergeJoinExec`, `NestedLoopJoinExec` and `CrossJoinExec` are not in
//! the allowlist: each carrying input is restored at or below the join's
//! input, as before, and no exchange is added.
//!
//! # What the rule leaves alone
//!
//! - Grouping sets (`ROLLUP`, `CUBE`, `GROUPING SETS`). Their per-set NULL
//!   placeholder expressions are typed to match each group expression, and
//!   rewriting one side without the other would desynchronize them.
//! - Any aggregate that already has an output ordering. Such an aggregate runs
//!   under a `GroupOrdering`, which reaches `EmitTo::First` and so is already
//!   bounded, and a cast in a projection above it is not guaranteed to carry
//!   the ordering equivalence its parent may require. Such an aggregate gets
//!   no cast of its own. One whose input already carries views from a lower
//!   aggregate, and that reads them only as group keys, can group on those
//!   views with no projection between it and its input (strict mode, below,
//!   is when it does not).
//!
//! The carried placement respects that caveat. No cast sits between the
//! operators that carry the views, so every sort and merge in the chain orders
//! the views themselves and declares that ordering on them. The restore cast
//! is where a declared ordering on the key is lost, and above the plan root no
//! operator reads one: the rows leave in the order the chain produced them.
//! Under any other parent, a carried input whose output is ordered on a
//! carried column is rewritten again in strict mode, where no operator ordered
//! on a carried column carries it. The restore then goes below that operator,
//! which orders the declared type as it did before views were carried. A
//! window partitioned by the key over a grouped subquery needs this: at
//! execution it requires its input declared ordered on the key.
//!
//! Everything else is unconditional: correctness of the rewrite does not
//! depend on how many distinct keys a query actually has, so there is no
//! threshold to get wrong.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinSide, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateInputMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::utils::build_join_schema;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, Partitioning, ReplaceChildrenOptions,
};

/// The in-memory type a dictionary-encoded string group key is grouped as.
/// The operators between the aggregate and the restore projection see it too,
/// and are rebuilt so their schemas say so; nothing above that projection, and
/// no client, observes it.
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
        let rewritten = rewrite(&plan, false, false)?;
        restore_schema(rewritten, &plan.schema())
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

/// Rewrite `node` and its subtree, bottom-up.
///
/// The returned plan produces the same rows as `node`, and its schema is
/// `node`'s except that a column this rule moved from a dictionary string to
/// `Utf8View` may still be `Utf8View`: it is *carried*, and the caller decides
/// whether to carry it further or restore it. `parent_is_aggregate` is whether
/// `node`'s parent is an `AggregateExec`, which is all [`carries`] needs to
/// know about it.
///
/// `strict` rewrites a subtree whose restore lands under a parent other than
/// the plan root. The restore cast does not carry an ordering on the column
/// it casts (a window over it then finds its `PARTITION BY` input unordered),
/// so in strict mode no operator whose output is ordered on a carried column
/// carries it: the restore goes below that operator, which then orders the
/// declared type itself.
fn rewrite(
    node: &Arc<dyn ExecutionPlan>,
    parent_is_aggregate: bool,
    strict: bool,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let is_aggregate = node.is::<AggregateExec>();
    let children = node.children();
    let mut rewritten = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in &children {
        let new_child = rewrite(child, is_aggregate, strict)?;
        changed |= !Arc::ptr_eq(child, &new_child);
        let carried = carried_columns(&child.schema(), &new_child.schema());
        let views = only_views(&child.schema(), &new_child.schema(), &carried);
        rewritten.push((new_child, carried, views));
    }
    // A join decides for both inputs at once: it hashes and matches its keys
    // on both sides, so one input carrying views on a key is not enough.
    let join_carries = node.downcast_ref::<HashJoinExec>().map(|join| {
        let [(_, left, left_views), (_, right, right_views)] = rewritten.as_slice() else {
            return false;
        };
        *left_views && *right_views && join_carries(node, join, left, right)
    });

    let mut new_children = Vec::with_capacity(children.len());
    let mut carrying = false;
    for (child, (mut new_child, carried, views)) in children.iter().zip(rewritten) {
        if !carried.is_empty() {
            let carries = join_carries
                .unwrap_or_else(|| views && carries(node, &carried, parent_is_aggregate));
            if carries {
                carrying = true;
            } else {
                if !strict && orders_on(&new_child, &carried) {
                    new_child = rewrite(child, is_aggregate, true)?;
                }
                new_child = restore_schema(new_child, &child.schema())?;
            }
        }
        new_children.push(keep_join_partitioning(node, child, new_child)?);
    }

    let rebuilt = rebuild(node, new_children.clone(), changed, carrying)?;
    if strict
        && carrying
        && orders_on(
            &rebuilt,
            &carried_columns(&node.schema(), &rebuilt.schema()),
        )
    {
        let restored = restore_children(node, &children, new_children)?;
        return rebuild(node, restored, true, false);
    }
    Ok(rebuilt)
}

/// True when every one of the `carried` columns moved from a dictionary string
/// in `original` to `Utf8View` in `rewritten`.
fn only_views(original: &SchemaRef, rewritten: &SchemaRef, carried: &[usize]) -> bool {
    carried.iter().all(|&idx| {
        is_dictionary_string(original.field(idx).data_type())
            && *rewritten.field(idx).data_type() == GROUP_KEY_TYPE
    })
}

/// `node` over its rewritten `children`. `changed` is whether any child is a
/// different plan from the one `node` holds, and `carrying` whether any of
/// them carries a column `node` agreed to carry.
fn rebuild(
    node: &Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
    changed: bool,
    carrying: bool,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = node.downcast_ref::<AggregateExec>() {
        return rewrite_aggregate(node, agg, children, changed);
    }
    if !changed {
        return Ok(Arc::clone(node));
    }
    if !carrying {
        return replace_children(node, children);
    }
    if let Some(projection) = node.downcast_ref::<ProjectionExec>()
        && let [input] = children.as_slice()
    {
        // `with_new_children` keeps the projection's precomputed output
        // schema, which still says `Dictionary` for a column whose batches
        // now hold views.
        return Ok(Arc::new(ProjectionExec::try_new(
            projection.expr().to_vec(),
            Arc::clone(input),
        )?));
    }
    let rebuilt = replace_children(node, children.clone())?;
    if let [input] = children.as_slice()
        && rebuilt.schema().fields() == input.schema().fields()
    {
        return Ok(rebuilt);
    }
    if let Some(join) = rebuilt.downcast_ref::<HashJoinExec>()
        && join_schema_follows_inputs(join)
    {
        return Ok(rebuilt);
    }
    // A pass-through operator that did not take its schema from the new input
    // would claim the declared type over views. Restore below it instead of
    // trusting it.
    let restored = restore_children(node, &node.children(), children)?;
    replace_children(node, restored)
}

/// Each of `rewritten` restored under `parent` onto the `original` child at
/// the same position, as [`restore_input`] does.
fn restore_children(
    parent: &Arc<dyn ExecutionPlan>,
    original: &[&Arc<dyn ExecutionPlan>],
    rewritten: Vec<Arc<dyn ExecutionPlan>>,
) -> DFResult<Vec<Arc<dyn ExecutionPlan>>> {
    original
        .iter()
        .zip(rewritten)
        .map(|(child, new_child)| restore_input(parent, child, new_child))
        .collect()
}

/// `rewritten` projected back onto the schema of `original`, the child of
/// `parent` it replaces, and exchanged again as [`keep_join_partitioning`]
/// says.
fn restore_input(
    parent: &Arc<dyn ExecutionPlan>,
    original: &Arc<dyn ExecutionPlan>,
    rewritten: Arc<dyn ExecutionPlan>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let restored = restore_schema(rewritten, &original.schema())?;
    keep_join_partitioning(parent, original, restored)
}

/// `input`, the rewritten `original` child of `parent`, with `original`'s
/// hash partitioning put back when `parent` is a hash join in `Partitioned`
/// mode and the rewrite lost it.
///
/// The join requires both inputs hash-partitioned on its keys. A restore cast
/// on a column `original` was partitioned on, at any depth between the join
/// and the exchange below it, leaves `input`'s partitioning unknown. That
/// exchange hashed views, and the join does not carry them, so the other
/// input hashes the declared type: `input`'s restored rows are exchanged again
/// on `original`'s hash expressions, now over the declared type.
fn keep_join_partitioning(
    parent: &Arc<dyn ExecutionPlan>,
    original: &Arc<dyn ExecutionPlan>,
    input: Arc<dyn ExecutionPlan>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let partitioned_join = parent
        .downcast_ref::<HashJoinExec>()
        .is_some_and(|join| *join.partition_mode() == PartitionMode::Partitioned);
    let partitioning = original.properties().output_partitioning();
    if !partitioned_join
        || !matches!(partitioning, Partitioning::Hash(..))
        || input.properties().output_partitioning() == partitioning
    {
        return Ok(input);
    }
    let ordered = input.properties().output_ordering().is_some();
    let exchange = RepartitionExec::try_new(input, partitioning.clone())?;
    // A join reports its probe input's ordering as its own, and an operator
    // above it may rely on that.
    Ok(Arc::new(if ordered {
        exchange.with_preserve_order()
    } else {
        exchange
    }))
}

/// True when a hash join can take inputs whose `left` and `right` carried
/// columns are `Utf8View`, and so hash and match them as views on both sides.
///
/// It must be in `Partitioned` mode; every join key that reads a carried
/// column must be that bare column on both sides, each carried; its filter
/// must read no carried column; and no node below it may hold the dynamic
/// filter it publishes over its keys, which would compare the views' bounds
/// and hashes with a column below an aggregate that may still be a
/// dictionary.
fn join_carries(
    node: &Arc<dyn ExecutionPlan>,
    join: &HashJoinExec,
    left: &[usize],
    right: &[usize],
) -> bool {
    if *join.partition_mode() != PartitionMode::Partitioned {
        return false;
    }
    let carried_column = |expr: &Arc<dyn PhysicalExpr>, carried: &[usize]| {
        expr.downcast_ref::<Column>()
            .is_some_and(|column| carried.contains(&column.index()))
    };
    let reads = |expr: &Arc<dyn PhysicalExpr>, carried: &[usize]| {
        collect_columns(expr)
            .iter()
            .any(|column| carried.contains(&column.index()))
    };
    let keys = join.on().iter().all(|(l, r)| {
        !(reads(l, left) || reads(r, right))
            || (carried_column(l, left) && carried_column(r, right))
    });
    let filter = join.filter().is_none_or(|filter| {
        !filter
            .column_indices()
            .iter()
            .any(|column| match column.side {
                JoinSide::Left => left.contains(&column.index),
                JoinSide::Right => right.contains(&column.index),
                JoinSide::None => false,
            })
    });
    keys && filter && !dynamic_filter_consumed(node)
}

/// True when a node below `node` holds a dynamic filter `node` publishes, or
/// when that cannot be told: a filter without an id, or an expression walk
/// that fails.
fn dynamic_filter_consumed(node: &Arc<dyn ExecutionPlan>) -> bool {
    let produced = node.dynamic_expressions_produced();
    let ids: Vec<u64> = produced
        .iter()
        .filter_map(|filter| filter.expression_id())
        .collect();
    if ids.len() != produced.len() {
        return true;
    }
    !ids.is_empty()
        && node
            .children()
            .iter()
            .any(|child| holds_filter(child, &ids))
}

/// True when `plan` or a node below it holds, in its own expressions, a
/// dynamic filter whose id is one of `ids`, or when its expressions cannot be
/// walked.
fn holds_filter(plan: &Arc<dyn ExecutionPlan>, ids: &[u64]) -> bool {
    let mut found = false;
    let walked = plan.apply_expressions(&mut |expr| {
        expr.apply(|sub| {
            if sub
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .and_then(|filter| filter.expression_id())
                .is_some_and(|id| ids.contains(&id))
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    });
    walked.is_err() || found || plan.children().iter().any(|child| holds_filter(child, ids))
}

/// True when `join`'s schema is the one its current inputs produce, so a
/// carried column it outputs says `Utf8View` rather than the type it was
/// planned with.
fn join_schema_follows_inputs(join: &HashJoinExec) -> bool {
    let (schema, _) = build_join_schema(
        &join.left().schema(),
        &join.right().schema(),
        join.join_type(),
    );
    let expected: Vec<&DataType> = match &join.projection {
        Some(projection) => projection
            .iter()
            .filter_map(|&idx| schema.fields().get(idx).map(|field| field.data_type()))
            .collect(),
        None => schema.fields().iter().map(|f| f.data_type()).collect(),
    };
    let actual = join.schema();
    expected.len() == actual.fields().len()
        && expected
            .iter()
            .zip(actual.fields().iter())
            .all(|(ty, field)| *ty == field.data_type())
}

/// True when `plan`'s output ordering reads any of the `columns` of its own
/// output.
fn orders_on(plan: &Arc<dyn ExecutionPlan>, columns: &[usize]) -> bool {
    !columns.is_empty()
        && plan.properties().output_ordering().is_some_and(|ordering| {
            ordering.iter().any(|sort| {
                collect_columns(&sort.expr)
                    .iter()
                    .any(|column| columns.contains(&column.index()))
            })
        })
}

/// `node` over `children`, with its plan properties recomputed from them.
fn replace_children(
    node: &Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    Arc::clone(node).replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

/// The indices of the columns whose type differs between a node's original
/// schema and its rewritten one.
fn carried_columns(original: &SchemaRef, rewritten: &SchemaRef) -> Vec<usize> {
    original
        .fields()
        .iter()
        .zip(rewritten.fields().iter())
        .enumerate()
        .filter(|(_, (before, after))| before.data_type() != after.data_type())
        .map(|(idx, _)| idx)
        .collect()
}

/// True when `node` can take an input whose `carried` columns are `Utf8View`
/// instead of their declared dictionary type, and be rebuilt so that its own
/// schema says so, without changing a row it produces. This is the allowlist
/// the module docs describe; anything not named here gets the restore
/// projection directly over its input.
///
/// An operator that evaluates expressions over its input (a projection, a
/// sort or merge through its ordering, an exchange through its partitioning
/// or the ordering it preserves) carries only when every one of them that
/// reads a carried column, anywhere in its tree, is that bare column: a
/// computed expression such as `k = 'a'` was planned against the declared
/// type and fails or changes meaning over views.
fn carries(node: &Arc<dyn ExecutionPlan>, carried: &[usize], parent_is_aggregate: bool) -> bool {
    let reads = |expr: &Arc<dyn PhysicalExpr>| {
        collect_columns(expr)
            .iter()
            .any(|column| carried.contains(&column.index()))
    };
    let bare =
        |expr: &Arc<dyn PhysicalExpr>| !reads(expr) || expr.downcast_ref::<Column>().is_some();
    if let Some(sort) = node.downcast_ref::<SortExec>() {
        // A TopK publishes a dynamic filter over every column it orders by. A
        // filter that also reads a value an aggregate computed cannot be
        // pushed below that aggregate, so its thresholds never meet the scan's
        // dictionary column; one that reads only group keys could be.
        return sort.expr().iter().all(|e| bare(&e.expr))
            && (sort.fetch().is_none()
                || !sort.expr().iter().any(|e| reads(&e.expr))
                || sort.expr().iter().any(|e| {
                    e.expr
                        .downcast_ref::<Column>()
                        .is_some_and(|column| is_aggregate_value(sort.input(), column.index()))
                }));
    }
    if let Some(merge) = node.downcast_ref::<SortPreservingMergeExec>() {
        return merge.expr().iter().all(|e| bare(&e.expr));
    }
    if let Some(repartition) = node.downcast_ref::<RepartitionExec>() {
        let ordering_bare = !repartition.preserve_order()
            || repartition
                .input()
                .properties()
                .output_ordering()
                .is_none_or(|ordering| ordering.iter().all(|e| bare(&e.expr)));
        return ordering_bare
            && match repartition.partitioning() {
                Partitioning::Hash(exprs, _) => {
                    exprs.iter().all(bare) && (parent_is_aggregate || !exprs.iter().any(reads))
                }
                _ => true,
            };
    }
    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        return projection.expr().iter().all(|e| bare(&e.expr));
    }
    if let Some(agg) = node.downcast_ref::<AggregateExec>() {
        return aggregate_carries(agg, carried, &reads);
    }
    passes_columns_through(node)
}

/// True for an operator that evaluates no expression over its input's columns
/// and outputs them as they are: it moves or limits rows.
fn passes_columns_through(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.is::<CoalescePartitionsExec>()
        || node.is::<GlobalLimitExec>()
        || node.is::<LocalLimitExec>()
        || node.is::<CooperativeExec>()
}

/// True when column `index` of `plan`'s output is, through renaming
/// projections and operators that pass rows through, an aggregate's computed
/// value rather than one of its group keys. False for anything it cannot
/// trace.
fn is_aggregate_value(plan: &Arc<dyn ExecutionPlan>, index: usize) -> bool {
    let mut plan = Arc::clone(plan);
    let mut index = index;
    loop {
        if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
            return !agg.group_expr().has_grouping_set() && index >= agg.group_expr().expr().len();
        }
        let next = if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            let Some(column) = projection
                .expr()
                .get(index)
                .and_then(|e| e.expr.downcast_ref::<Column>())
            else {
                return false;
            };
            index = column.index();
            Arc::clone(projection.input())
        } else if plan.is::<SortExec>()
            || plan.is::<SortPreservingMergeExec>()
            || plan.is::<RepartitionExec>()
            || passes_columns_through(&plan)
        {
            let [input] = plan.children()[..] else {
                return false;
            };
            Arc::clone(input)
        } else {
            return false;
        };
        plan = next;
    }
}

/// True when `agg` reads its input's `carried` columns only as bare-column
/// group keys, so it can group on the views itself.
fn aggregate_carries(
    agg: &AggregateExec,
    carried: &[usize],
    reads: &dyn Fn(&Arc<dyn PhysicalExpr>) -> bool,
) -> bool {
    let group_by = agg.group_expr();
    if group_by.has_grouping_set() || !group_by.null_expr().is_empty() {
        return false;
    }
    if group_by
        .expr()
        .iter()
        .any(|(expr, _)| reads(expr) && expr.downcast_ref::<Column>().is_none())
    {
        return false;
    }
    if agg.mode().input_mode() == AggregateInputMode::Raw {
        let aggregate_reads = agg.aggr_expr().iter().any(|aggregate| {
            aggregate.expressions().iter().any(reads)
                || aggregate.order_bys().iter().any(|sort| reads(&sort.expr))
        });
        let filter_reads = agg.filter_expr().iter().flatten().any(reads);
        !aggregate_reads && !filter_reads
    } else {
        // A merging stage reads its accumulator state positionally rather
        // than through expressions, after the group columns. Only those group
        // columns may be carried into it.
        carried.iter().all(|&idx| idx < group_by.expr().len())
    }
}

/// Rewrite one aggregate over its rewritten input.
///
/// Two things happen here, and both are driven by the *current* input schema
/// rather than by what this rule did to a lower node. That is what keeps a
/// two-stage aggregation consistent: if the `Partial` stage was left alone,
/// the stage above it sees the dictionary input it always saw.
///
/// 1. Cast dictionary-string group keys to `Utf8View`.
/// 2. Rebuild the aggregate whenever its recorded schema disagrees with what
///    its group expressions now produce. `AggregateExec::with_new_children`
///    carries the old schema forward verbatim; a `Final` stage over a
///    rewritten `Partial` would otherwise claim `Dictionary` while its group
///    values are `Utf8View`.
///
/// The rebuilt aggregate is returned as it is, whatever its mode: its parent
/// decides in [`rewrite`] whether the `Utf8View` group column goes further up.
fn rewrite_aggregate(
    node: &Arc<dyn ExecutionPlan>,
    agg: &AggregateExec,
    new_children: Vec<Arc<dyn ExecutionPlan>>,
    changed: bool,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let unchanged = || -> DFResult<Arc<dyn ExecutionPlan>> {
        if changed {
            replace_children(node, new_children.clone())
        } else {
            Ok(Arc::clone(node))
        }
    };
    let group_by = agg.group_expr();
    if group_by.has_grouping_set() || !group_by.null_expr().is_empty() {
        return unchanged();
    }
    let [input] = new_children.as_slice() else {
        return unchanged();
    };

    let input_schema = input.schema();
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
        return unchanged();
    }

    // A stage over raw rows records its own input's schema; a merging stage
    // records the schema of the raw input below its partial stage.
    let raw_schema = if agg.mode().input_mode() == AggregateInputMode::Raw {
        Arc::clone(&input_schema)
    } else {
        agg.input_schema()
    };
    let rebuilt = AggregateExec::try_new(
        *agg.mode(),
        PhysicalGroupBy::new(exprs, Vec::new(), group_by.groups().to_vec(), false),
        agg.aggr_expr().to_vec(),
        agg.filter_expr().to_vec(),
        Arc::clone(input),
        raw_schema,
    )?
    .with_limit_options(agg.limit_options());
    Ok(Arc::new(rebuilt))
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
    // The rewrite changes column types and nothing else, so the widths always
    // agree. Say so as an error rather than as an index: a future DataFusion
    // whose `create_schema` adds a column would otherwise panic here, in the
    // code whose whole point is that a query does not.
    if current.fields().len() != target.fields().len() {
        return Err(datafusion::error::DataFusionError::Internal(format!(
            "{DICTIONARY_GROUP_KEYS_RULE}: rewritten plan has {} columns, expected {}",
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
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::tree_node::Transformed;
    use datafusion::common::{JoinType, NullEquality, ScalarValue};
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion::physical_plan::aggregates::AggregateOutputMode;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn dict_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    /// A session over one batch with a `Dictionary(Int32, Utf8)` key column and
    /// an `Int64` payload. `with_rule` decides whether the rule under test is
    /// installed, so the two halves of the differential differ in nothing else.
    ///
    /// The groups are NULL, `a`, `b` and `c`, with row counts 5, 4, 3, 2,
    /// distinct `v` counts 5, 4, 2, 1 and sums 60, 10, 4, 14, so every
    /// `ORDER BY` the tests use has one answer, ties included.
    fn dict_context(with_rule: bool) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", dict_type(), true),
            Field::new("v", DataType::Int64, false),
        ]));
        let rows: [(Option<&str>, i64); 14] = [
            (None, 10),
            (Some("a"), 1),
            (Some("b"), 1),
            (None, 11),
            (Some("c"), 7),
            (Some("a"), 2),
            (None, 12),
            (Some("b"), 1),
            (Some("a"), 3),
            (None, 13),
            (Some("c"), 7),
            (Some("b"), 2),
            (Some("a"), 4),
            (None, 14),
        ];
        let keys: DictionaryArray<Int32Type> = rows.iter().map(|(k, _)| *k).collect();
        let values: Int64Array = rows.iter().map(|(_, v)| Some(*v)).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
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

    /// The statements the differential below runs. Each has one correct row
    /// order over the `dict_context` fixture.
    const DIFFERENTIAL: [&str; 11] = [
        "SELECT k, sum(v) AS total FROM t GROUP BY k ORDER BY k",
        // q34's shape: a TopK over the grouped counts, restored above it.
        "SELECT k, count(*) AS c FROM t GROUP BY k ORDER BY c DESC LIMIT 2",
        // q14's shape: one COUNT(DISTINCT) planned as two aggregate levels.
        "SELECT k, count(DISTINCT v) AS u FROM t GROUP BY k ORDER BY u DESC LIMIT 10",
        // A TopK ordering by the key itself, which keeps the restore below it.
        "SELECT k, count(*) AS c FROM t GROUP BY k ORDER BY k LIMIT 2",
        // A HAVING filter, which keeps the restore directly above the aggregate.
        "SELECT k, count(*) AS c FROM t GROUP BY k HAVING count(*) > 2 ORDER BY c DESC",
        // A window ordered by the key, over a sort that carries the views.
        "SELECT k, count(*) AS c, row_number() OVER (ORDER BY k) AS r \
         FROM t GROUP BY k ORDER BY k",
        // A window partitioned by the key over a grouped subquery: the sort
        // it needs orders the declared type, below the restore.
        "SELECT k, c, rank() OVER (PARTITION BY k ORDER BY c) AS r \
         FROM (SELECT k, count(*) AS c FROM t GROUP BY k) ORDER BY k",
        // Two grouped inputs hash-joined on the key.
        "SELECT a.k, a.c, b.total \
         FROM (SELECT k, count(*) AS c FROM t GROUP BY k) a \
         JOIN (SELECT k, sum(v) AS total FROM t GROUP BY k) b ON a.k = b.k \
         ORDER BY a.c DESC",
        // Sort keys that compute on the key: the comparison against a
        // dictionary literal, and a CASE over it, are evaluated on the sort's
        // input, so that input must hold the declared type.
        "SELECT k, count(*) AS c FROM t GROUP BY k ORDER BY k = 'a', c",
        "SELECT k, count(*) AS c FROM t GROUP BY k ORDER BY c DESC, k = 'x' LIMIT 10",
        "SELECT k, count(*) AS c FROM t GROUP BY k \
         ORDER BY CASE k WHEN 'a' THEN 0 ELSE 1 END, c",
    ];

    /// Run `sql` on a fresh fixture session, returning the schema the plan
    /// promised and the rows as a table, so two runs compare by type, value
    /// and order whatever their batch boundaries or dictionary layouts.
    async fn run(with_rule: bool, sql: &str) -> (Schema, String) {
        let df = dict_context(with_rule).sql(sql).await.expect("query plans");
        let logical = df.schema().as_arrow().clone();
        let batches = df.collect().await.expect("query runs");
        for batch in &batches {
            assert_eq!(batch.schema().fields(), logical.fields(), "{sql}");
        }
        let table = pretty_format_batches(&batches)
            .expect("batches format")
            .to_string();
        (logical, table)
    }

    #[tokio::test]
    async fn results_and_types_match_the_unrewritten_plan() {
        for sql in DIFFERENTIAL {
            let (before_schema, before) = run(false, sql).await;
            let (after_schema, after) = run(true, sql).await;

            assert_eq!(before_schema, after_schema, "{sql}");
            assert_eq!(before, after, "{sql}");
            // The baseline really is the dictionary path this rule exists to
            // avoid.
            assert_eq!(before_schema.field(0).data_type(), &dict_type(), "{sql}");
            assert!(before.lines().count() > 4, "{sql} returned no rows");
        }
        // The TopK shape returns its top two groups, so the comparison above
        // is not over an empty or truncated answer.
        let (_, top) = run(true, DIFFERENTIAL[1]).await;
        let expected = [
            "+---+---+",
            "| k | c |",
            "+---+---+",
            "|   | 5 |",
            "| a | 4 |",
            "+---+---+",
        ];
        assert_eq!(top.lines().collect::<Vec<_>>(), expected);
    }

    /// A `HAVING` filter is outside the allowlist: the restore projection sits
    /// directly above the aggregate that produces final values, as it did
    /// before views were carried, and the filter reads the declared type.
    #[tokio::test]
    async fn having_on_an_aggregate_restores_below_the_filter() {
        let sql = DIFFERENTIAL[4];
        let plan = dict_context(true)
            .sql(sql)
            .await
            .expect("query plans")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let mut filters = Vec::new();
        find(&plan, &|node| node.is::<FilterExec>(), &mut filters);
        let [filter] = filters.as_slice() else {
            panic!("expected one FilterExec: {filters:?}");
        };
        let [restore] = filter.children()[..] else {
            panic!("a FilterExec has one input");
        };
        let projection = restore
            .downcast_ref::<ProjectionExec>()
            .expect("the filter's input is the restore projection");
        assert!(
            projection
                .expr()
                .iter()
                .any(|e| casts_to(&e.expr, &dict_type())),
            "the projection under the filter restores the dictionary type"
        );
        assert_eq!(restore.schema().field(0).data_type(), &dict_type());
        let [aggregate] = restore.children()[..] else {
            panic!("a ProjectionExec has one input");
        };
        let aggregate = aggregate
            .downcast_ref::<AggregateExec>()
            .expect("the restore projection sits directly on the aggregate");
        assert_ne!(
            aggregate.mode().output_mode(),
            AggregateOutputMode::Partial,
            "the restore sits on the stage that produces final values"
        );
        assert_eq!(aggregate.schema().field(0).data_type(), &DataType::Utf8View);

        let (before_schema, before) = run(false, sql).await;
        let (after_schema, after) = run(true, sql).await;
        assert_eq!(before_schema, after_schema);
        assert_eq!(before, after);
    }

    /// Every node of `plan` for which `pred` holds, in pre-order.
    fn find(
        plan: &Arc<dyn ExecutionPlan>,
        pred: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool,
        out: &mut Vec<Arc<dyn ExecutionPlan>>,
    ) {
        if pred(plan) {
            out.push(Arc::clone(plan));
        }
        for child in plan.children() {
            find(child, pred, out);
        }
    }

    /// True when `expr` is a `CastExpr` to `ty`.
    fn casts_to(expr: &Arc<dyn PhysicalExpr>, ty: &DataType) -> bool {
        expr.downcast_ref::<CastExpr>()
            .is_some_and(|cast| cast.cast_type() == ty)
    }

    /// The two inputs and the keys of a hash join planned, without the rule,
    /// over two inputs grouped on the dictionary key `k`, in a session that
    /// plans it `Partitioned`: `repartition_joins` on and no input small
    /// enough to collect. Each input is the final stage of a two-stage
    /// aggregate, hash-partitioned on `k` by the exchange below it.
    async fn grouped_join_inputs() -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>, JoinOn) {
        let partitions = 4;
        let schema = Arc::new(Schema::new(vec![Field::new("k", dict_type(), true)]));
        let keys = ["a", "b", "c", "a", "x", "b", "a", "c"];
        let batches: Vec<Vec<RecordBatch>> = keys
            .chunks(keys.len() / partitions)
            .map(|chunk| {
                let dict: DictionaryArray<Int32Type> = chunk.iter().map(|k| Some(*k)).collect();
                vec![
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dict) as ArrayRef])
                        .expect("key batch builds"),
                ]
            })
            .collect();
        let mut config = SessionConfig::new()
            .with_target_partitions(partitions)
            .with_repartition_joins(true);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let table = MemTable::try_new(schema, batches).expect("mem table builds");
        ctx.register_table("t", Arc::new(table))
            .expect("table registers");
        let plan = ctx
            .sql(
                "SELECT a.k, a.c, b.c \
                 FROM (SELECT k, count(*) AS c FROM t GROUP BY k) a \
                 JOIN (SELECT k, count(*) AS c FROM t GROUP BY k) b ON a.k = b.k",
            )
            .await
            .expect("query plans")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let mut joins = Vec::new();
        find(&plan, &|node| node.is::<HashJoinExec>(), &mut joins);
        let [join] = joins.as_slice() else {
            panic!("expected one HashJoinExec: {joins:?}");
        };
        let join = join.downcast_ref::<HashJoinExec>().expect("is a HashJoinExec");
        assert_eq!(*join.partition_mode(), PartitionMode::Partitioned);
        // Each input is the subquery's renaming projection over its final
        // aggregate; the join is rebuilt directly on the aggregates, whose `k`
        // is at the same index.
        let aggregate = |input: &Arc<dyn ExecutionPlan>| {
            let projection = input
                .downcast_ref::<ProjectionExec>()
                .expect("a join input is the subquery's projection");
            let agg = Arc::clone(projection.input());
            let mode = agg
                .downcast_ref::<AggregateExec>()
                .expect("the projection is over the grouped aggregate")
                .mode()
                .output_mode();
            assert_eq!(mode, AggregateOutputMode::Final);
            assert_eq!(agg.schema().field(0).data_type(), &dict_type());
            assert!(matches!(
                agg.properties().output_partitioning(),
                Partitioning::Hash(..)
            ));
            agg
        };
        (
            aggregate(join.left()),
            aggregate(join.right()),
            join.on().to_vec(),
        )
    }

    /// A `Partitioned` inner hash join of `left` and `right` on `on`.
    fn hash_join(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        on: &JoinOn,
        filter: Option<JoinFilter>,
    ) -> HashJoinExec {
        HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            on.clone(),
            filter,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .expect("hash join builds")
    }

    /// The plan the rule makes of `join`, and the one `HashJoinExec` in it.
    fn optimize_join(join: HashJoinExec) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
        let plan = DictionaryGroupKeysAsViews
            .optimize(Arc::new(join), &ConfigOptions::default())
            .expect("rule runs");
        let mut joins = Vec::new();
        find(&plan, &|node| node.is::<HashJoinExec>(), &mut joins);
        let [join] = joins.as_slice() else {
            panic!("expected one HashJoinExec: {joins:?}");
        };
        let join = Arc::clone(join);
        (plan, join)
    }

    /// The type of `k`, the first column, on each of `join`'s inputs.
    fn input_key_types(join: &Arc<dyn ExecutionPlan>) -> Vec<DataType> {
        join.children()
            .iter()
            .map(|input| input.schema().field(0).data_type().clone())
            .collect()
    }

    /// The join-filter and dynamic-filter conditions of `join_carries`, on a
    /// hand-built `Partitioned` join whose inputs both carry the views, so
    /// neither condition is pre-empted by an operator between the join and
    /// its aggregates. Without either filter the join carries; with a join
    /// filter reading the key, or with a dynamic filter it publishes held
    /// below its probe input's aggregate, each input is restored below it.
    #[tokio::test]
    async fn partitioned_join_filter_on_a_carried_key_restores_below_the_join() {
        let (left, right, on) = grouped_join_inputs().await;

        let (plan, join) = optimize_join(hash_join(&left, &right, &on, None));
        assert_eq!(
            input_key_types(&join),
            [DataType::Utf8View, DataType::Utf8View],
            "the join without a filter carries the views: {plan:?}"
        );
        assert_eq!(join.schema().field(0).data_type(), &DataType::Utf8View);
        let restore = plan
            .downcast_ref::<ProjectionExec>()
            .expect("the restore projection is above the join");
        assert!(Arc::ptr_eq(restore.input(), &join), "{plan:?}");
        assert_eq!(plan.schema().field(0).data_type(), &dict_type());

        // `k = 'x'` on the left key, evaluated by the join itself.
        let filter_schema = Arc::new(Schema::new(vec![Field::new("k", dict_type(), true)]));
        let x = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("x".to_owned()))),
        );
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("k", 0)),
            Operator::Eq,
            Arc::new(Literal::new(x)),
        )) as Arc<dyn PhysicalExpr>;
        let filter = JoinFilter::new(
            expression,
            vec![ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            }],
            filter_schema,
        );
        let (plan, join) = optimize_join(hash_join(&left, &right, &on, Some(filter)));
        assert_restored_below(&plan, &join);

        // A dynamic filter the join publishes on the probe key, held by a
        // filter under the probe input's partial aggregate, where `k` is still
        // the scan's dictionary column.
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&on[0].1)],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        let probe = Arc::clone(&right)
            .transform_down(|node| {
                let partial = node
                    .downcast_ref::<AggregateExec>()
                    .is_some_and(|agg| agg.mode().output_mode() == AggregateOutputMode::Partial);
                let [scan] = node.children()[..] else {
                    return Ok(Transformed::no(node));
                };
                if !partial {
                    return Ok(Transformed::no(node));
                }
                let held = FilterExec::try_new(
                    Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>,
                    Arc::clone(scan),
                )?;
                let rebuilt = replace_children(&node, vec![Arc::new(held)])?;
                Ok(Transformed::yes(rebuilt))
            })
            .expect("probe input rebuilds")
            .data;
        let mut held = Vec::new();
        find(&probe, &|node| node.is::<FilterExec>(), &mut held);
        assert_eq!(held.len(), 1, "the dynamic filter is held once: {probe:?}");
        let join = hash_join(&left, &probe, &on, None)
            .with_dynamic_filter_expr(dynamic)
            .expect("dynamic filter attaches");
        let (plan, join) = optimize_join(join);
        assert_restored_below(&plan, &join);
    }

    /// `join` does not carry: both its inputs hold the declared type, and it
    /// is the plan root, with no restore above it.
    fn assert_restored_below(plan: &Arc<dyn ExecutionPlan>, join: &Arc<dyn ExecutionPlan>) {
        assert_eq!(
            input_key_types(join),
            [dict_type(), dict_type()],
            "the join carries the views: {plan:?}"
        );
        assert!(Arc::ptr_eq(plan, join), "{plan:?}");
        assert_eq!(plan.schema().field(0).data_type(), &dict_type());
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
