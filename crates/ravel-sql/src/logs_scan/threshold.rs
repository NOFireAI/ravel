use super::*;

/// The `ts` threshold of an `ORDER BY ts [, ...] LIMIT k` TopK above a logs
/// scan (ADR-2677 decision 6).
///
/// DataFusion's TopK publishes its heap's current worst row as a
/// [`DynamicFilterPhysicalExpr`]: `lit(true)` until the heap holds `k` rows,
/// then `ts < t`, or `ts < t OR (ts = t AND ...)` with further sort keys, and
/// only tighter after that. [`Self::ceiling`] reads `t` off the filter's
/// current expression: no row with `ts > t` can still enter the heap. A block
/// or segment whose stored minimum ts is strictly above `t` holds no such row
/// and is skipped; one whose minimum equals `t` is read, because its rows can
/// tie on `ts` and win on a later key.
///
/// Kept only when the filter's leading key is the bare `ts` column, named by
/// name so a rebuild that moves column indices keeps it valid.
///
/// The filter carries no sort direction, and a filter from a descending or
/// nulls-first sort, an aggregate, or a join can also lead with `ts`. So a
/// kept filter starts unconfirmed and changes nothing: no skip, snapshot visit
/// order, prefetch as without it. [`ConfirmTopKThreshold`] confirms it once it
/// finds the `SortExec` that publishes it and that sort's leading key is
/// ascending with nulls last, and drops it otherwise.
#[derive(Clone, Debug)]
pub(crate) struct TopKThreshold {
    pub(super) filter: Arc<DynamicFilterPhysicalExpr>,
    pub(super) confirmed: bool,
}

/// The logs table's event-timestamp column name.
const TS_COLUMN: &str = "ts";

impl TopKThreshold {
    /// The unconfirmed threshold `filter` carries, when its leading key is the
    /// bare `ts` column.
    pub(super) fn from_filter(filter: &Arc<DynamicFilterPhysicalExpr>) -> Option<Self> {
        let leading = filter.children().first().map(|c| Arc::clone(c))?;
        is_ts_column(leading.as_ref()).then(|| Self {
            filter: Arc::clone(filter),
            confirmed: false,
        })
    }

    /// The id DataFusion gives the filter's shared state, the same in every
    /// copy remapped through a projection, so it names the producing sort.
    pub(super) fn expression_id(&self) -> Option<u64> {
        self.filter.expression_id()
    }

    /// `Some(t)` once the TopK's current filter bounds `ts` from above: no
    /// row with `ts > t` can still be accepted. `None` while the heap is not
    /// full (the filter is `lit(true)`), and for any filter shape
    /// [`ts_ceiling`] does not recognize, which skips nothing.
    fn ceiling(&self) -> Option<i64> {
        let current = self.filter.current().ok()?;
        ts_ceiling(current.as_ref())
    }
}

/// Whether `expr` is the bare `ts` column.
fn is_ts_column(expr: &dyn PhysicalExpr) -> bool {
    expr.downcast_ref::<Column>()
        .is_some_and(|c| c.name() == TS_COLUMN)
}

/// The largest `ts` a row satisfying `expr` can have, or `None` when `expr`
/// does not bound `ts` from above in a shape read here.
///
/// `ts < t`, `ts <= t` and `ts = t` bound it by `t`. A conjunction is bounded
/// by its tighter bounded side; a disjunction only when both sides are, by
/// the looser. Anything else, `lit(true)` included, bounds nothing. The bound
/// is never tighter than the predicate, so skipping a block whose minimum is
/// above it never drops a row the predicate accepts.
fn ts_ceiling(expr: &dyn PhysicalExpr) -> Option<i64> {
    let binary = expr.downcast_ref::<BinaryExpr>()?;
    match binary.op() {
        Operator::And => {
            match (
                ts_ceiling(binary.left().as_ref()),
                ts_ceiling(binary.right().as_ref()),
            ) {
                (Some(l), Some(r)) => Some(l.min(r)),
                (l, r) => l.or(r),
            }
        }
        Operator::Or => {
            let l = ts_ceiling(binary.left().as_ref())?;
            let r = ts_ceiling(binary.right().as_ref())?;
            Some(l.max(r))
        }
        Operator::Lt | Operator::LtEq | Operator::Eq => {
            if !is_ts_column(binary.left().as_ref()) {
                return None;
            }
            match binary.right().downcast_ref::<Literal>()?.value() {
                ScalarValue::TimestampNanosecond(Some(t), _) => Some(*t),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The current ceiling of `topk`, if any; see [`TopKThreshold::ceiling`].
pub(super) fn topk_ceiling(topk: Option<&TopKThreshold>) -> Option<i64> {
    topk.and_then(TopKThreshold::ceiling)
}

/// The `expression_id` of `sort`'s dynamic filter when `sort` is a TopK whose
/// leading key is ascending with nulls last: the one filter shape
/// [`TopKThreshold::ceiling`] reads a `ts` bound from (`ts < t`). A nulls-first
/// key publishes `ts IS NULL OR ts < t`, which bounds nothing.
fn ascending_topk_filter_id(sort: &SortExec) -> Option<u64> {
    sort.fetch()?;
    let options = sort.expr().first().options;
    if options.descending || options.nulls_first {
        return None;
    }
    sort.dynamic_expressions_produced().first()?.expression_id()
}

/// Confirms or drops each [`LogsScanExec`]'s kept TopK filter (ADR-2677
/// decision 6) by matching it to the sort that publishes it.
///
/// A scan keeps the first pushed dynamic filter whose leading child is `ts`,
/// and the filter alone does not say whether its producer is an ascending
/// TopK. This rule collects the filter ids of every TopK whose leading key is
/// ascending with nulls last, confirms a scan whose kept filter has one of
/// those ids, and drops any other kept filter, so a scan under `ORDER BY ts
/// DESC LIMIT k` keeps its snapshot visit order and its prefetch. Appended
/// after every rule that installs a filter: DataFusion's post-optimization
/// filter pushdown and [`crate::late_materialization::TopKLateMaterialization`].
/// A session without this rule confirms nothing, so every scan skips nothing.
#[derive(Debug, Default)]
pub(crate) struct ConfirmTopKThreshold;

/// The name [`ConfirmTopKThreshold`] reports as a physical optimizer rule.
const CONFIRM_TOPK_THRESHOLD_RULE: &str = "confirm_logs_topk_threshold";

impl PhysicalOptimizerRule for ConfirmTopKThreshold {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut ascending = HashSet::new();
        plan.apply(|node| {
            if let Some(id) = node
                .downcast_ref::<SortExec>()
                .and_then(ascending_topk_filter_id)
            {
                ascending.insert(id);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        plan.transform_up(|node| {
            let Some(scan) = node.downcast_ref::<LogsScanExec>() else {
                return Ok(Transformed::no(node));
            };
            Ok(match scan.confirm_topk(&ascending) {
                Some(confirmed) => Transformed::yes(Arc::new(confirmed) as Arc<dyn ExecutionPlan>),
                None => Transformed::no(node),
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        CONFIRM_TOPK_THRESHOLD_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Skips the blocks at the front of `scan` whose stored minimum ts is above
/// `ceiling`, before any of their columns is decoded, and returns how many.
///
/// Every candidate block is checked, not only up to the first one that
/// survives: block minima are not ordered within a segment (several streams
/// interleave), so a later block can be skippable after an earlier one is
/// read. The caller checks again before each decode, so the threshold it
/// reads is the latest the TopK has published.
pub(super) fn skip_blocks_above(scan: &mut LogSegmentScan, ceiling: i64) -> usize {
    let mut skipped = 0;
    while let Some((min, max)) = scan.next_block_ts_bounds() {
        if !above_ceiling(min, max, ceiling) || !scan.skip_next_block() {
            break;
        }
        skipped += 1;
    }
    skipped
}

/// Whether a block or segment whose stored ts bounds are `min..=max` can be
/// skipped under `ceiling`: only when its minimum is strictly above it, and
/// never on inverted bounds (`min > max`), which prove nothing.
pub(super) fn above_ceiling(min: i64, max: i64, ceiling: i64) -> bool {
    min <= max && min > ceiling
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod topk_threshold_tests {
    //! ADR-2677 decision 6: which TopK filters bound `ts`, and which stored
    //! bounds a block or segment may be skipped on.

    use super::*;
    use datafusion::physical_expr::expressions::lit;

    const T: i64 = 1_700_000_000_000_000_000;

    fn col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn ts_lit(t: i64) -> Arc<dyn PhysicalExpr> {
        lit(ScalarValue::TimestampNanosecond(Some(t), None))
    }

    fn bin(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    }

    #[test]
    fn ts_ceiling_reads_the_topk_filter_shapes() {
        let ts = || col("ts", 0);
        assert_eq!(ts_ceiling(lit(true).as_ref()), None, "an unfilled heap");
        for op in [Operator::Lt, Operator::LtEq, Operator::Eq] {
            assert_eq!(ts_ceiling(bin(ts(), op, ts_lit(T)).as_ref()), Some(T));
        }
        // `ts < t OR (ts = t AND body < b)`: the two-key filter.
        let tie = bin(
            bin(ts(), Operator::Eq, ts_lit(T)),
            Operator::And,
            bin(col("body", 1), Operator::Lt, lit("m")),
        );
        let two_key = bin(bin(ts(), Operator::Lt, ts_lit(T)), Operator::Or, tie);
        assert_eq!(ts_ceiling(two_key.as_ref()), Some(T));
        let both = bin(
            bin(ts(), Operator::Lt, ts_lit(T + 5)),
            Operator::And,
            bin(ts(), Operator::Lt, ts_lit(T)),
        );
        assert_eq!(ts_ceiling(both.as_ref()), Some(T), "the tighter side");
        let either = bin(
            bin(ts(), Operator::Lt, ts_lit(T + 5)),
            Operator::Or,
            bin(ts(), Operator::Lt, ts_lit(T)),
        );
        assert_eq!(ts_ceiling(either.as_ref()), Some(T + 5), "the looser side");
    }

    #[test]
    fn ts_ceiling_refuses_what_does_not_bound_ts_from_above() {
        let open_or = bin(
            bin(col("ts", 0), Operator::Lt, ts_lit(T)),
            Operator::Or,
            bin(col("body", 1), Operator::Lt, lit("m")),
        );
        for expr in [
            bin(col("ts", 0), Operator::Gt, ts_lit(T)),
            bin(col("ts", 0), Operator::GtEq, ts_lit(T)),
            bin(col("observed_ts", 1), Operator::Lt, ts_lit(T)),
            bin(col("ts", 0), Operator::Lt, lit(5i64)),
            bin(
                lit(ScalarValue::TimestampNanosecond(None, None)),
                Operator::Gt,
                col("ts", 0),
            ),
            bin(
                col("ts", 0),
                Operator::Lt,
                lit(ScalarValue::TimestampNanosecond(None, None)),
            ),
            open_or,
        ] {
            assert_eq!(ts_ceiling(expr.as_ref()), None, "{expr}");
        }
    }

    #[test]
    fn above_ceiling_is_strict_and_refuses_inverted_bounds() {
        assert!(above_ceiling(T + 1, T + 9, T));
        assert!(!above_ceiling(T, T + 9, T), "a tie is read");
        assert!(!above_ceiling(T + 1, T, T), "inverted bounds prove nothing");
        assert!(!above_ceiling(0, 0, T), "an empty block reads min_ts 0");
    }

    #[test]
    fn threshold_needs_a_leading_bare_ts_and_a_full_heap() {
        let keys = |lead: Arc<dyn PhysicalExpr>| vec![lead, col("body", 1)];
        for lead in [
            col("body", 1),
            col("observed_ts", 2),
            bin(col("ts", 0), Operator::Plus, ts_lit(1)),
        ] {
            let filter = Arc::new(DynamicFilterPhysicalExpr::new(keys(lead), lit(true)));
            assert!(TopKThreshold::from_filter(&filter).is_none());
        }
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            keys(col("ts", 0)),
            lit(true),
        ));
        let threshold = TopKThreshold::from_filter(&filter).expect("a leading ts key");
        assert_eq!(threshold.ceiling(), None, "no bound until the heap is full");
        filter
            .update(bin(col("ts", 0), Operator::Lt, ts_lit(T)))
            .expect("update");
        assert_eq!(threshold.ceiling(), Some(T));
        filter
            .update(bin(col("ts", 0), Operator::Lt, ts_lit(T - 3)))
            .expect("update");
        assert_eq!(threshold.ceiling(), Some(T - 3), "the latest bound is read");
    }
}
