//! Project `attrs['k']` as per-key columns instead of rebuilding the merged
//! map (issue #1768).
//!
//! # What it costs to not do this
//!
//! Asking a question through `attrs['attr_3']` and through a declared column
//! `"attr_3"` over the same data moves identical bytes over the wire under the
//! stock whole-object read policy, yet the map form costs 50x-220x the CPU: on
//! an in-process store, 40 objects, 200,000 rows, 33 record attributes, the
//! equality statement was 1070.7 ms versus 4.8 ms cold and decoded 84,691
//! versus 3,531 stored page bytes. Three things cost that, and all three are
//! about materializing the whole map when the query reads one key: the reader
//! selects every FIELD_DIR column's pages, [`crate::logs_scan`] rebuilds a
//! `Vec<(String, AttrValue)>` per row, and it materializes a `Map(Utf8, Utf8)`
//! column that `get_field` then reads one key out of.
//!
//! This is a CPU saving, not an I/O one. The stock fetch policy reads whole
//! objects, so narrowing the *decode* selection changes no wire byte and no GET
//! (the fetch layer is out of scope, #1769); only the pages decoded and the
//! per-row work fall.
//!
//! # The rewrite
//!
//! [`crate::map_field_planner`] lowers `attrs['k']` to `get_field(attrs, 'k')`
//! at plan time. Physically that is a `get_field` over the scan's whole
//! `attrs` map column ([`LOG_COL_ATTRS`]). This rule walks the linear chain of
//! plan nodes above a [`LogsScanExec`], collects every literal key the `attrs`
//! column is read with through `get_field`, and -- only when the whole map is
//! never used any other way -- rewrites the scan to materialize one synthetic
//! `Utf8` column per key (via [`LogsScanExec::reproject_attr_keys`]) in place of
//! the map, and rewrites each `get_field(attrs, 'k')` above it into a reference
//! to that key's column. The scan then decodes only those keys' FIELD_DIR
//! columns plus `attrs_raw` and stays on the columnar fast path, exactly as a
//! declared column already does.
//!
//! ## Why it is not a rename
//!
//! `attrs['k']` and a declared column `"k"` diverge by design when a record
//! holds a non-`Str` value under a `Str` declaration: the map renders `7`, the
//! declared column reads NULL (ADR-0090 decision 6). The per-key column
//! reproduces the MAP rendering -- record-wins over resource/scope, every
//! variant rendered as text, NULL only for a genuinely absent key -- so a query
//! returns the same rows it did before. Aliasing the declared column instead
//! would change results. That rendering lives in [`crate::logs_scan`]'s
//! `build_attr_key_columnar_array`/`attr_key_column_array`; this rule only wires
//! the columns into the plan.
//!
//! ## When it does nothing
//!
//! When the projection needs the whole map (`SELECT attrs`, `SELECT *`, a
//! comparison of two subscripts folded to a bare map reference, an unrecognized
//! node between the map's user and the scan, a pending erasure, a scan that
//! already emits row refs or per-key columns), the walk finds a bare use or an
//! unsupported shape and returns the plan untouched: those stay on the existing
//! row path, which is correct, just slower. The rule is fail-safe by
//! construction -- every refusal leaves a correct plan.
//!
//! ## Why the residual still runs
//!
//! An `attrs['k'] = 'v'` predicate is `Inexact` pushdown, so DataFusion keeps a
//! `FilterExec` above the scan (crate::logs_provider). This rule rewrites the
//! `get_field` inside that filter's predicate into a column reference; the
//! `FilterExec` still sits above the scan and still evaluates the equality, now
//! over the per-key column rather than the map. The scan reads no fewer rows and
//! the residual is still the sole exactness mechanism.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::scalar::ScalarValue;

use crate::logs_scan::LogsScanExec;
use crate::logs_schema::{LOG_COL_ATTRS, attr_key_field_name};

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const ATTRS_PER_KEY_RULE: &str = "attrs_per_key_projection";

/// Rewrites a `logs` scan whose `attrs` map is read only through literal-key
/// `get_field` subscripts into one that materializes those keys as per-key
/// `Utf8` columns (issue #1768). See the module docs.
#[derive(Debug, Default)]
pub struct AttrsPerKeyProjection;

impl PhysicalOptimizerRule for AttrsPerKeyProjection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match try_rewrite(&plan)? {
            Some(rewritten) => Ok(rewritten),
            None => Ok(plan),
        }
    }

    fn name(&self) -> &str {
        ATTRS_PER_KEY_RULE
    }

    fn schema_check(&self) -> bool {
        // The rewrite is required to be invisible to the client: it changes an
        // inner `get_field` into a column reference of the same `Utf8` type and
        // leaves every output name, order, and type untouched. Let DataFusion
        // assert that rather than trusting it.
        true
    }
}

/// The role a chain node plays while the `attrs` column is still live below it.
enum Flow {
    /// A schema-preserving node that may read `attrs` (a residual filter, a
    /// sort key): the column stays live above it.
    StayLive,
    /// A node that consumes `attrs` into its output (a projection computing
    /// `attrs['k']`, an aggregate grouping on it): the column is not live above
    /// it.
    Consume,
    /// An unsupported or whole-map use: refuse the rewrite.
    Bail,
}

/// Try to rewrite `root`'s subtree, or return `None` to leave it unchanged.
fn try_rewrite(root: &Arc<dyn ExecutionPlan>) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    // The linear chain from the root down to the one `LogsScanExec` leaf. A
    // branching node (a join, a union) before the scan means this is not the
    // single-scan `logs` shape the rule targets; leave it alone.
    let mut chain: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    let mut cur = Arc::clone(root);
    loop {
        if cur.is::<LogsScanExec>() {
            break;
        }
        let children = cur.children();
        if children.len() != 1 {
            return Ok(None);
        }
        let child = Arc::clone(children[0]);
        chain.push(Arc::clone(&cur));
        cur = child;
    }
    let Some(scan) = cur.downcast_ref::<LogsScanExec>() else {
        // The loop only breaks on a `LogsScanExec`; this is unreachable, but
        // leaving the plan unchanged is the fail-safe answer.
        return Ok(None);
    };

    // Only a scan that could otherwise take the columnar path, and that this
    // rule has not already rewritten. `late_materialization_candidate` is
    // exactly "no pending erasure and not itself a row-ref phase 1", both of
    // which this rewrite also requires.
    if scan.has_attr_keys() || !scan.late_materialization_candidate() {
        return Ok(None);
    }

    // The `attrs` map must be projected exactly once. If it is not projected at
    // all there is nothing to replace; if it somehow appears twice the position
    // arithmetic below would be ambiguous.
    let attrs_positions: Vec<usize> = scan
        .projection()
        .iter()
        .enumerate()
        .filter(|(_, v)| **v == LOG_COL_ATTRS)
        .map(|(i, _)| i)
        .collect();
    if attrs_positions.len() != 1 {
        return Ok(None);
    }
    let attrs_idx = attrs_positions[0];
    let full_len = scan.full_schema_len();
    let width = scan.projection().len();

    // Pass 1: walk bottom-up collecting every literal key the `attrs` column is
    // read with, and refusing on any bare (whole-map) use or unsupported node.
    let mut keys: Vec<String> = Vec::new();
    let mut attrs_live = true;
    for node in chain.iter().rev() {
        if !attrs_live {
            // Above the node that consumed `attrs`, the map is out of scope: its
            // input schema no longer carries the column, so nothing here can
            // reference it and there is nothing to validate.
            continue;
        }
        match classify_and_collect(node, attrs_idx, &mut keys) {
            Flow::StayLive => {}
            Flow::Consume => attrs_live = false,
            Flow::Bail => return Ok(None),
        }
    }
    if keys.is_empty() {
        return Ok(None);
    }

    // The new projection replaces the map slot with the first key's synthetic
    // column (keeping every other output index fixed) and appends the rest past
    // the projected width, so no existing column index shifts. `slots` records
    // each key's output index and field name for the `get_field` rewrite.
    let mut slots: HashMap<String, (usize, String)> = HashMap::new();
    let mut new_projection = scan.projection().to_vec();
    new_projection[attrs_idx] = full_len;
    slots.insert(keys[0].clone(), (attrs_idx, attr_key_field_name(&keys[0])));
    for (j, key) in keys.iter().enumerate().skip(1) {
        new_projection.push(full_len + j);
        slots.insert(key.clone(), (width + (j - 1), attr_key_field_name(key)));
    }

    let new_scan = scan.reproject_attr_keys(new_projection, keys.clone())?;

    // Pass 2: rebuild the chain bottom-up over the new scan, rewriting the
    // `get_field(attrs, 'k')` in every node that referenced the map into a
    // reference to the key's per-key column, tracking `attrs_live` exactly as
    // pass 1 did.
    let mut child: Arc<dyn ExecutionPlan> = Arc::new(new_scan);
    let mut attrs_live = true;
    for node in chain.iter().rev() {
        if !attrs_live {
            child = Arc::clone(node).with_new_children(vec![child])?;
            continue;
        }
        let (rebuilt, still_live) = rebuild_node(node, child, attrs_idx, &slots)?;
        child = rebuilt;
        attrs_live = still_live;
    }
    Ok(Some(child))
}

/// Classify one chain node and collect the `attrs` keys it reads. `keys`
/// accumulates in first-seen order across the whole chain.
fn classify_and_collect(
    node: &Arc<dyn ExecutionPlan>,
    attrs_idx: usize,
    keys: &mut Vec<String>,
) -> Flow {
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        // A projected filter reshapes the columns flowing up out of it, which
        // would desync the indices this rule keeps fixed. Refuse rather than
        // reason about two remaps at once.
        if filter.projection().is_some() {
            return Flow::Bail;
        }
        let mut bare = false;
        analyze_expr(filter.predicate(), attrs_idx, keys, &mut bare);
        return if bare { Flow::Bail } else { Flow::StayLive };
    }
    if let Some(sort) = node.downcast_ref::<SortExec>() {
        let mut bare = false;
        for se in sort.expr().iter() {
            analyze_expr(&se.expr, attrs_idx, keys, &mut bare);
        }
        return if bare { Flow::Bail } else { Flow::StayLive };
    }
    if let Some(proj) = node.downcast_ref::<ProjectionExec>() {
        let mut bare = false;
        for pe in proj.expr().iter() {
            analyze_expr(&pe.expr, attrs_idx, keys, &mut bare);
        }
        return if bare { Flow::Bail } else { Flow::Consume };
    }
    if let Some(agg) = node.downcast_ref::<AggregateExec>() {
        let group = agg.group_expr();
        if group.has_grouping_set() || !group.null_expr().is_empty() {
            return Flow::Bail;
        }
        let mut bare = false;
        for (e, _) in group.expr().iter() {
            analyze_expr(e, attrs_idx, keys, &mut bare);
        }
        if bare {
            return Flow::Bail;
        }
        // Only group keys are rewritten. An aggregate argument or a per-aggregate
        // filter that reads `attrs` (`COUNT(attrs['k'])`, a `FILTER (WHERE
        // attrs['k'] = ...)`) has no rewrite here, so refuse rather than leave a
        // stale `get_field` over what is now a `Utf8` column.
        let mut other_keys: Vec<String> = Vec::new();
        let mut other_bare = false;
        for af in agg.aggr_expr().iter() {
            for e in af.expressions() {
                analyze_expr(&e, attrs_idx, &mut other_keys, &mut other_bare);
            }
        }
        for fe in agg.filter_expr().iter().flatten() {
            analyze_expr(fe, attrs_idx, &mut other_keys, &mut other_bare);
        }
        if other_bare || !other_keys.is_empty() {
            return Flow::Bail;
        }
        return Flow::Consume;
    }
    if is_opaque_passthrough(node) {
        return Flow::StayLive;
    }
    Flow::Bail
}

/// Rebuild one chain node over `child`, rewriting the `attrs` `get_field`s it
/// carries. Returns the rebuilt node and whether the `attrs` column is still
/// live above it.
fn rebuild_node(
    node: &Arc<dyn ExecutionPlan>,
    child: Arc<dyn ExecutionPlan>,
    attrs_idx: usize,
    slots: &HashMap<String, (usize, String)>,
) -> DFResult<(Arc<dyn ExecutionPlan>, bool)> {
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        let predicate = rewrite_expr(filter.predicate(), attrs_idx, slots)?;
        let rebuilt = FilterExec::try_new(predicate, child)?
            .with_default_selectivity(filter.default_selectivity())?;
        return Ok((Arc::new(rebuilt), true));
    }
    if let Some(sort) = node.downcast_ref::<SortExec>() {
        let mut exprs = Vec::with_capacity(sort.expr().len());
        for se in sort.expr().iter() {
            let mut remapped = se.clone();
            remapped.expr = rewrite_expr(&se.expr, attrs_idx, slots)?;
            exprs.push(remapped);
        }
        let ordering = LexOrdering::new(exprs).ok_or_else(|| {
            DataFusionError::Internal(format!("{ATTRS_PER_KEY_RULE}: rewritten sort is empty"))
        })?;
        let rebuilt = SortExec::new(ordering, child)
            .with_fetch(sort.fetch())
            .with_preserve_partitioning(sort.preserve_partitioning());
        return Ok((Arc::new(rebuilt), true));
    }
    if let Some(proj) = node.downcast_ref::<ProjectionExec>() {
        let mut exprs: Vec<ProjectionExpr> = Vec::with_capacity(proj.expr().len());
        for pe in proj.expr().iter() {
            exprs.push(ProjectionExpr {
                expr: rewrite_expr(&pe.expr, attrs_idx, slots)?,
                alias: pe.alias.clone(),
            });
        }
        let rebuilt = ProjectionExec::try_new(exprs, child)?;
        return Ok((Arc::new(rebuilt), false));
    }
    if let Some(agg) = node.downcast_ref::<AggregateExec>() {
        let group = agg.group_expr();
        let mut gexpr = Vec::with_capacity(group.expr().len());
        for (e, name) in group.expr().iter() {
            gexpr.push((rewrite_expr(e, attrs_idx, slots)?, name.clone()));
        }
        let group_by = PhysicalGroupBy::new(
            gexpr,
            group.null_expr().to_vec(),
            group.groups().to_vec(),
            group.has_grouping_set(),
        );
        let rebuilt = AggregateExec::try_new(
            *agg.mode(),
            group_by,
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            child,
            agg.input_schema(),
        )?
        .with_limit_options(agg.limit_options());
        return Ok((Arc::new(rebuilt), false));
    }
    // Opaque pass-through: rebuild with the new child, nothing to remap.
    Ok((Arc::clone(node).with_new_children(vec![child])?, true))
}

/// A node between the `attrs` map's user and the scan that neither reads a
/// column expression nor changes the schema, so the map flows through it with
/// its index unchanged.
#[allow(deprecated)] // CoalesceBatchesExec still appears in physical plans here.
fn is_opaque_passthrough(node: &Arc<dyn ExecutionPlan>) -> bool {
    if node.is::<CoalescePartitionsExec>()
        || node.is::<CoalesceBatchesExec>()
        || node.is::<CooperativeExec>()
        || node.is::<GlobalLimitExec>()
        || node.is::<LocalLimitExec>()
    {
        return true;
    }
    if let Some(repartition) = node.downcast_ref::<RepartitionExec>() {
        // A hash repartition carries column expressions; round-robin and
        // unknown carry none. A hash on the `attrs` column itself is not a
        // shape this rule rewrites, so only the exprless partitionings are
        // opaque.
        return matches!(
            repartition.partitioning(),
            Partitioning::RoundRobinBatch(_) | Partitioning::UnknownPartitioning(_)
        );
    }
    false
}

/// The literal `Utf8` key of a `get_field` subscript, if that is what `lit` is.
fn literal_key(lit: &Literal) -> Option<&str> {
    match lit.value() {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s),
        ScalarValue::Utf8View(Some(s)) => Some(s),
        _ => None,
    }
}

/// If `expr` is `get_field(Column(attrs_idx), '<key>')`, return the key.
fn attrs_get_field(expr: &Arc<dyn PhysicalExpr>, attrs_idx: usize) -> Option<&str> {
    let sf = expr.downcast_ref::<ScalarFunctionExpr>()?;
    if sf.fun().name() != "get_field" {
        return None;
    }
    let args = sf.args();
    if args.len() != 2 {
        return None;
    }
    let column = args[0].downcast_ref::<Column>()?;
    if column.index() != attrs_idx {
        return None;
    }
    let lit = args[1].downcast_ref::<Literal>()?;
    literal_key(lit)
}

/// Walk `expr`, pushing every `attrs['k']` key into `keys` (first-seen order,
/// deduplicated) and setting `bare` if the `attrs` column is referenced any
/// other way. A `get_field` on the map is consumed without recursing into the
/// column argument, so it does not count as a bare use.
fn analyze_expr(
    expr: &Arc<dyn PhysicalExpr>,
    attrs_idx: usize,
    keys: &mut Vec<String>,
    bare: &mut bool,
) {
    if let Some(key) = attrs_get_field(expr, attrs_idx) {
        if !keys.iter().any(|k| k == key) {
            keys.push(key.to_string());
        }
        return;
    }
    if let Some(column) = expr.downcast_ref::<Column>() {
        if column.index() == attrs_idx {
            *bare = true;
        }
        return;
    }
    for child in expr.children() {
        analyze_expr(child, attrs_idx, keys, bare);
    }
}

/// `expr` with every `get_field(Column(attrs_idx), '<key>')` replaced by a
/// reference to that key's per-key column.
fn rewrite_expr(
    expr: &Arc<dyn PhysicalExpr>,
    attrs_idx: usize,
    slots: &HashMap<String, (usize, String)>,
) -> DFResult<Arc<dyn PhysicalExpr>> {
    Arc::clone(expr)
        .transform_down(|node| {
            if let Some(key) = attrs_get_field(&node, attrs_idx)
                && let Some((slot, name)) = slots.get(key)
            {
                let column = Arc::new(Column::new(name, *slot)) as Arc<dyn PhysicalExpr>;
                // The subtree below was the map column plus a literal; there is
                // nothing left in it to rewrite.
                return Ok(Transformed::new(column, true, TreeNodeRecursion::Jump));
            }
            Ok(Transformed::no(node))
        })
        .data()
}
