//! Filter pushdown for the `spans` table, under the same pruning-soundness
//! invariant the `logs` and metrics pushdowns obey (crate::logs_pushdown,
//! crate::pushdown, docs/arrow-datafusion-plan.md section 2 "Filter
//! pushdown"): pruning may only ever *widen* the read set relative to the
//! query's true need, never narrow it. `SpansTableProvider::
//! supports_filters_pushdown` returns `Inexact` for every filter, so
//! DataFusion always re-applies the originals above the scan; exactness comes
//! from that residual.
//!
//! Six shapes are recognized (everything else contributes nothing and widens):
//!
//! - **ts window**: top-level AND conjuncts of the bare `start_ts` or `end_ts`
//!   column compared to a literal timestamp (`>=`, `>`, `<`, `<=`, `=`), and
//!   `BETWEEN`. These collapse into the single inclusive window `[ts_min,
//!   ts_max]` that [`ravel_rspan::SpanQuery`] carries, which the reader prunes
//!   blocks against by interval overlap. See "Why one window covers both
//!   endpoints" below for why folding both columns into one window is a widen,
//!   never a narrow.
//! - **trace_id equality** `trace_id = <literal>`: the RSPAN-specific fast
//!   path (ADR-0041's whole point). A single trace-id equality is extracted
//!   into [`SpansPushdown::trace_id`] and turned into a
//!   [`ravel_rspan::SpanQuery::trace`] lookup, which the reader's skip index
//!   uses to drop every block whose `[min_trace_id, max_trace_id]` range
//!   excludes the target -- a bounded single-trace scan instead of a full
//!   time-window scan. Pushing it is sound for the same reason the `logs`
//!   `has_word` push is: the reader applies `trace_id ==` as an exact per-row
//!   filter, removing exactly the rows the `Inexact` residual would remove, so
//!   no needed row is ever dropped.
//! - **duration_ns window** (ADR-0045 decision 5): top-level AND conjuncts of
//!   the bare `duration_ns` computed column compared to a literal integer,
//!   folding into `[duration_lo, duration_hi]` exactly the way the ts window
//!   folds `start_ts`/`end_ts` -- same bound-tightening, same `checked_add`/
//!   `checked_sub` handling of strict `>`/`<`.
//! - **status_code equality** `status_code = <literal>`: each conjunct maps to
//!   one `ravel_rspan::skip_index` status bit, and multiple conjuncts
//!   AND-intersect into [`SpansPushdown::status_mask`].
//! - **service_name equality** `service_name = <literal>`: last-writer-wins,
//!   mirroring `trace_id`. Feeds a `COL_SERVICE_NAME` bloom probe (ADR-0054):
//!   a block whose bloom proves the token absent is skipped before decode.
//! - **name equality** `name = <literal>`: last-writer-wins, the span-name
//!   sibling of `service_name`. Feeds a `COL_NAME` bloom probe (ADR-0054), the
//!   other field the v3 per-block bloom is built over.
//!
//! Five of the six are conjunctive-only (ADR-0045 decision 5): a disjunction
//! anywhere in a conjunct's own subtree ([`contains_or`]) drops that whole
//! conjunct rather than being soundly pushed, since refusing to push is
//! always widen-safe (the `Inexact` residual re-applies it).
//!
//! The `status_code`/`duration_ns` axes are the exception (issue #519, first
//! increment): a **same-axis** disjunction -- every disjunct constrains the
//! *one* axis, nothing else -- is recognized and pushed as the *union* of the
//! per-disjunct constraints, not the AND-intersection ordinary sibling
//! conjuncts use. `status_code IN (1, 2)`, `status_code = 1 OR status_code =
//! 2`, and a duration range union all take this path; see
//! [`same_axis_status_union`] and [`same_axis_duration_union`]. This is sound
//! because the union covers every disjunct: a block is dropped only when it
//! proves no-match against the *combined* constraint, which means it proves
//! no-match against each individual disjunct too. A **cross-axis**
//! disjunction (`status_code = 2 OR duration_ns > 5e8`) is still refused: no
//! disjunct's failure to name a single shared axis is treated as "not this
//! increment's shape" and falls back to the ordinary widen-safe refusal
//! ([`contains_or`]'s doc). `trace_id` is not part of this increment: RSPAN's
//! reader only ever exposes a single-point trace lookup
//! ([`ravel_rspan::SpanQuery::trace`]) with no range-based skip-index
//! primitive to union against, so a `trace_id =` disjunction is refused the
//! same as any other cross-axis case (a follow-up would need a new
//! `ravel_rspan` primitive first, out of this crate's reach).
//!
//! # Why one window covers both `start_ts` and `end_ts`
//!
//! [`ravel_rspan::SpanQuery`] carries one window `[ts_min, ts_max]`, and the
//! reader keeps a record iff its `[start_ts, end_ts]` interval overlaps it
//! (`start_ts <= ts_max && end_ts >= ts_min`). Extraction must guarantee every
//! record satisfying all SQL conjuncts overlaps the window. It does:
//!
//! - A lower-bound conjunct on either column (`start_ts >= L`, `start_ts > L`,
//!   `end_ts >= L`, `end_ts > L`) implies `end_ts >= L`, because
//!   `end_ts >= start_ts`. Taking `ts_min = max` of all lower bounds gives
//!   `end_ts >= ts_min` for every surviving record.
//! - An upper-bound conjunct on either column (`... <= U`, `... < U`) implies
//!   `start_ts <= U`, because `start_ts <= end_ts`. Taking `ts_max = min` of
//!   all upper bounds gives `start_ts <= ts_max`.
//!
//! So the overlap test holds for every record the SQL would keep: the window
//! is a widen. An unsatisfiable pair (`start_ts >= 100 AND end_ts <= 50`)
//! collapses to `ts_min > ts_max`, which the reader treats as the empty scan
//! -- correct, since no record satisfies it. `= V` on either column sets both
//! bounds to `V` (an exact point overlaps `[V, V]`).

use ravel_rspan::SpanQuery;
use ravel_rspan::skip_index::{STATUS_BIT_ERROR, STATUS_BIT_OK, STATUS_BIT_UNSET};

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// Byte width of a trace id: the fixed length a `trace_id =` literal must have
/// to be a valid RSPAN trace key (`ravel_rspan::record::TRACE_ID_WIDTH`).
const TRACE_ID_WIDTH: usize = 16;

/// Everything the extractor pulled out of a `spans` filter set, all widen-only.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SpansPushdown {
    /// Inclusive lower bound on the ts window in nanoseconds, if provably
    /// required. Folded from `start_ts`/`end_ts` lower-bound conjuncts.
    pub ts_lo: Option<i64>,
    /// Inclusive upper bound on the ts window in nanoseconds, if provably
    /// required.
    pub ts_hi: Option<i64>,
    /// The single trace id an exact `trace_id = <literal>` equality pinned, if
    /// any: the RSPAN fast-path key.
    pub trace_id: Option<[u8; 16]>,
    /// Inclusive lower bound on `duration_ns`, if provably required. Folded
    /// from `duration_ns` lower-bound conjuncts the same way `ts_lo` folds
    /// `start_ts`/`end_ts` bounds (ADR-0045 decision 5).
    pub duration_lo: Option<i64>,
    /// Inclusive upper bound on `duration_ns`, if provably required.
    pub duration_hi: Option<i64>,
    /// The set of `status_code` values a query's conjuncts can still match,
    /// as a bitmask over `ravel_rspan::skip_index`'s `STATUS_BIT_*` bits.
    /// `None` means unconstrained (every status code still possible) -- the
    /// only value that means "no constraint". Never `Some(0)`: that would
    /// claim no status code can match and prune every block, which is sound
    /// only when the conjuncts truly are unsatisfiable (see
    /// [`status_code_equality`]'s doc).
    pub status_mask: Option<u8>,
    /// The single service name an exact `service_name = <literal>` equality
    /// pinned, if any (last writer wins, mirroring [`Self::trace_id`]). Drives a
    /// `ravel_rspan::record::COL_SERVICE_NAME` bloom probe (ADR-0054).
    pub service_name: Option<String>,
    /// The single span name an exact `name = <literal>` equality pinned, if any
    /// (last writer wins, mirroring [`Self::service_name`]). Drives a
    /// `ravel_rspan::record::COL_NAME` bloom probe (ADR-0054).
    pub name: Option<String>,
}

impl SpansPushdown {
    /// The inclusive lower ts bound for the [`SpanQuery`] window: the extracted
    /// bound, or `i64::MIN` when none was proven (scan the whole time range).
    pub fn ts_min(&self) -> i64 {
        self.ts_lo.unwrap_or(i64::MIN)
    }

    /// The inclusive upper ts bound for the [`SpanQuery`] window: the extracted
    /// bound, or `i64::MAX` when none was proven.
    pub fn ts_max(&self) -> i64 {
        self.ts_hi.unwrap_or(i64::MAX)
    }

    /// The [`SpanQuery`] this pushdown maps to. When a `trace_id =` equality
    /// was extracted this is the cheap [`SpanQuery::trace`] single-trace lookup
    /// (ADR-0041's trace_id-keyed routing); otherwise it is a bare
    /// [`SpanQuery::ts_range`] window scan. Both carry the same ts window.
    pub fn span_query(&self) -> SpanQuery {
        match self.trace_id {
            Some(tid) => SpanQuery::trace(tid, self.ts_min(), self.ts_max()),
            None => SpanQuery::ts_range(self.ts_min(), self.ts_max()),
        }
    }

    /// The inclusive `duration_ns` window to prune
    /// `ravel_rspan::skip_index::BlockEntry`s against, in the shape
    /// `candidate_blocks`'s `duration_ns: Option<(i64, i64)>` parameter takes:
    /// `None` when no duration bound was proven (every block's duration
    /// range must be kept), `Some((lo, hi))` otherwise, defaulting whichever
    /// side is unset to the full-range bound so the pair is always a real
    /// window.
    pub fn duration_window(&self) -> Option<(i64, i64)> {
        if self.duration_lo.is_none() && self.duration_hi.is_none() {
            return None;
        }
        Some((
            self.duration_lo.unwrap_or(i64::MIN),
            self.duration_hi.unwrap_or(i64::MAX),
        ))
    }
}

/// Extract all sound pushdown from a `spans` filter set. Each element of
/// `filters` is an implicit top-level AND conjunct; nested `AND`s are split too.
pub fn extract_spans(filters: &[Expr]) -> SpansPushdown {
    let mut out = SpansPushdown::default();
    for f in filters {
        walk_conjunct(f, &mut out);
    }
    out
}

fn walk_conjunct(expr: &Expr, out: &mut SpansPushdown) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
        && *op == Operator::And
    {
        walk_conjunct(left, out);
        walk_conjunct(right, out);
        return;
    }
    handle_leaf(expr, out);
}

fn handle_leaf(expr: &Expr, out: &mut SpansPushdown) {
    // An `Or`-rooted conjunct (the only place an `Or` can sit once
    // `walk_conjunct` has split every top-level `And`, see `contains_or`'s
    // doc) may still be soundly pushed when every disjunct constrains the
    // same single axis (issue #519's same-axis increment): the union of the
    // disjuncts' constraints is folded in as this conjunct's contribution,
    // exactly as a single equality would be. A cross-axis or otherwise
    // unrecognized `Or` shape falls through to the blanket refusal below.
    if is_or(expr) {
        if let Some(mask) = same_axis_status_union(expr) {
            fold_status_mask(out, mask);
            return;
        }
        if let Some((lo, hi)) = same_axis_duration_union(expr) {
            if let Some(lo) = lo {
                tighten_duration_lo(out, lo);
            }
            if let Some(hi) = hi {
                tighten_duration_hi(out, hi);
            }
            return;
        }
    }
    // A disjunction anywhere in this conjunct's subtree means the conjunct
    // as a whole cannot be soundly narrowed on any axis: see `contains_or`'s
    // doc for why refusing is the only safe move. Checked once here, up
    // front, rather than threaded into every shape matcher below, so a
    // future shape added to `handle_binary` can't forget the check.
    if contains_or(expr) {
        return;
    }
    match expr {
        Expr::BinaryExpr(be) => handle_binary(be, out),
        // BETWEEN low AND high desugars to `col >= low AND col <= high`. A
        // negated BETWEEN is an OR of two ranges: not sound to narrow.
        Expr::Between(bt) if !bt.negated && is_ts_col(&bt.expr) => {
            if let Some(lo) = lit_ts_ns(&bt.low) {
                tighten_lo(out, lo);
            }
            if let Some(hi) = lit_ts_ns(&bt.high) {
                tighten_hi(out, hi);
            }
        }
        // `status_code IN (v1, v2, ...)`: DataFusion's SQL parser lowers `IN`
        // to `Expr::InList`, not a chain of `Or`s, so this shape never
        // reaches `same_axis_status_union` above -- it needs its own arm.
        // Sound for the same reason the `Or` union is: the pushed mask is the
        // union of the list's bits, so a block is dropped only when it can
        // hold none of the listed values. `negated` (`NOT IN`) is an AND of
        // inequalities, a different shape this increment does not attempt.
        Expr::InList(il) if !il.negated && is_status_code_col(&il.expr) => {
            if let Some(mask) = status_mask_union(il.list.iter()) {
                fold_status_mask(out, mask);
            }
        }
        _ => {}
    }
}

fn is_or(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::Or,
            ..
        })
    )
}

/// AND-fold `mask` into `out.status_mask`, the same combinator
/// [`handle_binary`]'s plain-equality arm uses: sibling top-level conjuncts
/// (whether a single equality, an `IN` list, or a same-axis `Or`) always
/// intersect, since every one of them independently must hold.
fn fold_status_mask(out: &mut SpansPushdown, mask: u8) {
    out.status_mask = Some(match out.status_mask {
        Some(cur) => cur & mask,
        None => mask,
    });
}

/// The union of the `status_code` bits named by a list of literal exprs (an
/// `IN` list's values, or one same-axis-`Or` disjunct's values), or `None` if
/// any member is not a literal this extractor can read (an unknown value
/// might match a real row via that member, so the whole union is unsound to
/// compute -- refuse rather than guess). A member whose literal *is* known
/// but names a status code outside `0..=2` contributes no bit: it can never
/// equal a real row's `status_code` either way, so folding it out of the
/// union only omits an always-empty disjunct, never a reachable one.
fn status_mask_union<'a>(values: impl Iterator<Item = &'a Expr>) -> Option<u8> {
    let mut mask = 0u8;
    for v in values {
        let value = lit_i64(v)?;
        mask |= status_bit(value).unwrap_or(0);
    }
    Some(mask)
}

/// Recognize an `Or`-rooted conjunct as a same-axis `status_code`
/// disjunction and return the union mask, or `None` if any disjunct touches
/// something other than a `status_code` equality/`IN` list (cross-axis, or a
/// shape this increment does not parse) -- the caller then falls back to the
/// ordinary widen-safe refusal.
fn same_axis_status_union(expr: &Expr) -> Option<u8> {
    let mut disjuncts = Vec::new();
    flatten_or(expr, &mut disjuncts);
    if disjuncts.len() < 2 {
        return None;
    }
    let mut mask = 0u8;
    for d in disjuncts {
        mask |= disjunct_status_mask(d)?;
    }
    Some(mask)
}

/// One disjunct's own `status_code` mask: a bare equality, an `IN` list, or
/// an `And` of such (AND-intersected, matching ordinary conjunct semantics
/// within the one disjunct). `None` for anything else, including a further
/// nested `Or` -- this increment's same-axis recognizer is one level deep.
fn disjunct_status_mask(expr: &Expr) -> Option<u8> {
    match expr {
        Expr::BinaryExpr(be) if be.op == Operator::And => {
            Some(disjunct_status_mask(&be.left)? & disjunct_status_mask(&be.right)?)
        }
        Expr::BinaryExpr(be) => status_code_equality(be),
        Expr::InList(il) if !il.negated && is_status_code_col(&il.expr) => {
            status_mask_union(il.list.iter())
        }
        _ => None,
    }
}

/// Recognize an `Or`-rooted conjunct as a same-axis `duration_ns`
/// disjunction and return the union window as `(lo, hi)`, each `None` when
/// unbounded on that side (a disjunct with no lower/upper bound makes the
/// *union*'s corresponding side unbounded too, since that disjunct alone can
/// already match an arbitrarily low/high duration). `None` overall if any
/// disjunct touches something other than `duration_ns` comparisons.
fn same_axis_duration_union(expr: &Expr) -> Option<(Option<i64>, Option<i64>)> {
    let mut disjuncts = Vec::new();
    flatten_or(expr, &mut disjuncts);
    if disjuncts.len() < 2 {
        return None;
    }
    let mut los = Vec::with_capacity(disjuncts.len());
    let mut his = Vec::with_capacity(disjuncts.len());
    for d in disjuncts {
        let (lo, hi) = disjunct_duration_bounds(d)?;
        los.push(lo);
        his.push(hi);
    }
    let lo = if los.iter().any(Option::is_none) {
        None
    } else {
        los.into_iter().flatten().min()
    };
    let hi = if his.iter().any(Option::is_none) {
        None
    } else {
        his.into_iter().flatten().max()
    };
    Some((lo, hi))
}

/// One disjunct's own `duration_ns` window, as `(lo, hi)` (each `None` when
/// unbounded on that side). Walks the disjunct's own `And` structure with the
/// same bound-tightening [`apply_duration_bound`] uses for top-level
/// conjuncts -- an intersection *within* the disjunct, which is ordinary AND
/// semantics, not the union this function's caller folds disjuncts with.
/// `None` for anything that is not a plain `duration_ns` comparison chained
/// by `And` (a different column, a further `Or`, an unparseable literal).
fn disjunct_duration_bounds(expr: &Expr) -> Option<(Option<i64>, Option<i64>)> {
    let mut local = SpansPushdown::default();
    walk_duration_only(expr, &mut local)?;
    Some((local.duration_lo, local.duration_hi))
}

fn walk_duration_only(expr: &Expr, out: &mut SpansPushdown) -> Option<()> {
    let Expr::BinaryExpr(be) = expr else {
        return None;
    };
    if be.op == Operator::And {
        walk_duration_only(&be.left, out)?;
        walk_duration_only(&be.right, out)?;
        return Some(());
    }
    let (op, ns) = duration_comparison(be)?;
    apply_duration_bound(out, op, ns);
    Some(())
}

/// Flatten an `Or` tree into its leaf disjuncts, at any depth (`(a OR b) OR
/// c` yields `[a, b, c]`). A non-`Or` expr yields itself as the sole element,
/// so callers can feed in an already-flat conjunct uniformly.
fn flatten_or<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
        && *op == Operator::Or
    {
        flatten_or(left, out);
        flatten_or(right, out);
    } else {
        out.push(expr);
    }
}

/// True if `expr`'s subtree contains a disjunction anywhere: an `Or`
/// `BinaryExpr` at any depth, or a negated `BETWEEN` (`NOT (col BETWEEN lo
/// AND hi)` desugars to `col < lo OR col > hi`, a disjunction in substance
/// even though it has no `Expr::BinaryExpr(Or)` node).
///
/// This is the fallback refusal path: [`handle_leaf`] tries
/// [`same_axis_status_union`]/[`same_axis_duration_union`] on an `Or`-rooted
/// conjunct first (issue #519's same-axis increment), and only reaches this
/// blanket check when neither recognizes the shape (cross-axis, a negated
/// `BETWEEN`, an `Or` mixed with an unrelated column, or an `Or` nested
/// somewhere other than this conjunct's root). Refusing is always widen-safe:
/// DataFusion's `Inexact` residual re-applies the original predicate above
/// the scan, so dropping the whole conjunct only ever costs pruning, never
/// correctness.
fn contains_or(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            *op == Operator::Or || contains_or(left) || contains_or(right)
        }
        Expr::Between(bt) => {
            bt.negated || contains_or(&bt.expr) || contains_or(&bt.low) || contains_or(&bt.high)
        }
        _ => false,
    }
}

fn handle_binary(be: &BinaryExpr, out: &mut SpansPushdown) {
    // A ts (start_ts/end_ts) vs literal-timestamp comparison folds into the
    // window; a `duration_ns` comparison folds into the duration window; a
    // `trace_id =`, `status_code =`, `service_name =`, or `name =` equality
    // pins its respective axis. Every other binary shape contributes nothing
    // and widens. Column names are disjoint, so at most one branch ever matches.
    if let Some((op, ts_ns)) = ts_comparison(be) {
        apply_ts_bound(out, op, ts_ns);
    } else if let Some((op, dur_ns)) = duration_comparison(be) {
        apply_duration_bound(out, op, dur_ns);
    } else if let Some(tid) = trace_id_equality(be) {
        // Last writer wins; two different trace ids ANDed are unsatisfiable, and
        // pinning either still drops no needed row (no record has both).
        out.trace_id = Some(tid);
    } else if let Some(bit) = status_code_equality(be) {
        // AND-intersection, not overwrite: unlike trace_id (whose bytes
        // cannot be ANDed into a tighter value), each `status_code = v`
        // conjunct rules out every other status, so intersecting bitmasks
        // gives the tightest sound mask across however many conjuncts a
        // query supplies. A contradictory pair (`= Ok AND = Error`)
        // intersects to a `0` mask, which the reader's skip index treats as
        // "no block can match" -- correct, since no row can satisfy both
        // equalities either. [`fold_status_mask`] is the same combinator a
        // same-axis `Or`/`IN` conjunct folds its own union mask through.
        fold_status_mask(out, bit);
    } else if let Some(service) = service_name_equality(be) {
        // Last writer wins, mirroring trace_id: two different service names
        // ANDed are unsatisfiable, and pinning either still drops no needed row.
        out.service_name = Some(service);
    } else if let Some(name) = name_equality(be) {
        // Last writer wins, mirroring service_name.
        out.name = Some(name);
    }
}

// --- ts window extraction (mirrors crate::logs_pushdown's ts logic) ---

fn ts_comparison(be: &BinaryExpr) -> Option<(Operator, i64)> {
    let (op, ns) = if is_ts_col(&be.left) {
        (be.op, lit_ts_ns(&be.right)?)
    } else if is_ts_col(&be.right) {
        (flip_op(be.op)?, lit_ts_ns(&be.left)?)
    } else {
        return None;
    };
    match op {
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq => {
            Some((op, ns))
        }
        _ => None,
    }
}

fn apply_ts_bound(out: &mut SpansPushdown, op: Operator, ns: i64) {
    match op {
        Operator::GtEq => tighten_lo(out, ns),
        // `col > L` is `col >= L+1` in integer ns; on overflow drop the bound.
        Operator::Gt => {
            if let Some(lo) = ns.checked_add(1) {
                tighten_lo(out, lo);
            }
        }
        Operator::LtEq => tighten_hi(out, ns),
        // `col < U` is `col <= U-1`; on underflow drop the bound.
        Operator::Lt => {
            if let Some(hi) = ns.checked_sub(1) {
                tighten_hi(out, hi);
            }
        }
        Operator::Eq => {
            tighten_lo(out, ns);
            tighten_hi(out, ns);
        }
        _ => {}
    }
}

fn tighten_lo(out: &mut SpansPushdown, candidate: i64) {
    out.ts_lo = Some(match out.ts_lo {
        Some(cur) => cur.max(candidate),
        None => candidate,
    });
}

fn tighten_hi(out: &mut SpansPushdown, candidate: i64) {
    out.ts_hi = Some(match out.ts_hi {
        Some(cur) => cur.min(candidate),
        None => candidate,
    });
}

/// True for the bare `start_ts` or `end_ts` column. Both fold into the one
/// [`SpanQuery`] window (see the module doc's soundness argument).
fn is_ts_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "start_ts" || c.name == "end_ts")
}

// --- duration_ns window extraction (mirrors the ts logic above) ---

fn duration_comparison(be: &BinaryExpr) -> Option<(Operator, i64)> {
    let (op, ns) = if is_duration_col(&be.left) {
        (be.op, lit_i64(&be.right)?)
    } else if is_duration_col(&be.right) {
        (flip_op(be.op)?, lit_i64(&be.left)?)
    } else {
        return None;
    };
    match op {
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq => {
            Some((op, ns))
        }
        _ => None,
    }
}

fn apply_duration_bound(out: &mut SpansPushdown, op: Operator, ns: i64) {
    match op {
        Operator::GtEq => tighten_duration_lo(out, ns),
        // `duration_ns > L` is `duration_ns >= L+1`; on overflow drop the
        // bound (never a bare `+ 1`, which would wrap silently and narrow).
        Operator::Gt => {
            if let Some(lo) = ns.checked_add(1) {
                tighten_duration_lo(out, lo);
            }
        }
        Operator::LtEq => tighten_duration_hi(out, ns),
        // `duration_ns < U` is `duration_ns <= U-1`; on underflow drop the bound.
        Operator::Lt => {
            if let Some(hi) = ns.checked_sub(1) {
                tighten_duration_hi(out, hi);
            }
        }
        Operator::Eq => {
            tighten_duration_lo(out, ns);
            tighten_duration_hi(out, ns);
        }
        _ => {}
    }
}

fn tighten_duration_lo(out: &mut SpansPushdown, candidate: i64) {
    out.duration_lo = Some(match out.duration_lo {
        Some(cur) => cur.max(candidate),
        None => candidate,
    });
}

fn tighten_duration_hi(out: &mut SpansPushdown, candidate: i64) {
    out.duration_hi = Some(match out.duration_hi {
        Some(cur) => cur.min(candidate),
        None => candidate,
    });
}

/// True for the bare `duration_ns` column.
fn is_duration_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "duration_ns")
}

/// A literal integer in nanoseconds, or `None` for any non-integer or
/// non-literal expression. Accepts every integer `ScalarValue` width
/// DataFusion's type coercion might produce for a `duration_ns` (`Int64`)
/// comparison, not just `Int64`, since the coercion shape for an integer
/// column compared to a SQL integer literal has no precedent elsewhere in
/// this crate's pushdown extractors to copy. On overflow converting to `i64`
/// the literal is rejected (widen, never a wrapped/truncated bound).
fn lit_i64(e: &Expr) -> Option<i64> {
    let sv = match e {
        Expr::Literal(sv, _) => sv,
        _ => return None,
    };
    match sv {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        ScalarValue::UInt32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i64::from(*v)),
        _ => None,
    }
}

/// A literal timestamp in nanoseconds, or `None` for any non-timestamp or
/// non-literal expression. Integer literals are rejected (ambiguous scale, a
/// mis-scaled bound would narrow). Scaling is exact; on overflow the literal is
/// rejected (widen). Identical to the `logs`/metrics extractors.
fn lit_ts_ns(e: &Expr) -> Option<i64> {
    let sv = match e {
        Expr::Literal(sv, _) => sv,
        _ => return None,
    };
    match sv {
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => v.checked_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => v.checked_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(v), _) => v.checked_mul(1_000_000_000),
        _ => None,
    }
}

fn flip_op(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Eq => Operator::Eq,
        _ => return None,
    })
}

// --- trace_id fast-path extraction ---

/// A `trace_id = <literal>` equality (either operand order) -> the 16-byte
/// trace id. The literal may be a fixed/variable binary of exactly 16 bytes or
/// a 32-char lowercase/uppercase hex string; anything else (wrong width, bad
/// hex, non-`trace_id` column, non-`Eq` operator) yields `None` and widens.
fn trace_id_equality(be: &BinaryExpr) -> Option<[u8; 16]> {
    if be.op != Operator::Eq {
        return None;
    }
    if is_trace_id_col(&be.left) {
        lit_trace_id(&be.right)
    } else if is_trace_id_col(&be.right) {
        lit_trace_id(&be.left)
    } else {
        None
    }
}

fn is_trace_id_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "trace_id")
}

/// Decode a `trace_id` literal to its 16 raw bytes. Accepts a binary literal of
/// exactly 16 bytes (the column's native `FixedSizeBinary(16)` shape, and the
/// `Binary`/`LargeBinary`/`BinaryView` a `X'..'` literal may lower to) or a
/// 32-character hex string. A wrong length or malformed hex returns `None`,
/// which drops the fast path and falls back to a full window scan (a widen,
/// never a wrong prune).
fn lit_trace_id(e: &Expr) -> Option<[u8; 16]> {
    let sv = match e {
        Expr::Literal(sv, _) => sv,
        _ => return None,
    };
    match sv {
        ScalarValue::FixedSizeBinary(w, Some(b)) if *w == TRACE_ID_WIDTH as i32 => fixed_16(b),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b)) => fixed_16(b),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => hex_16(s),
        _ => None,
    }
}

/// A byte slice as a `[u8; 16]`, or `None` if it is not exactly 16 bytes.
fn fixed_16(b: &[u8]) -> Option<[u8; 16]> {
    <[u8; 16]>::try_from(b).ok()
}

/// A 32-character hex string as `[u8; 16]`, or `None` on any non-hex byte or a
/// length other than 32. Both nibble cases are accepted.
fn hex_16(s: &str) -> Option<[u8; 16]> {
    let bytes = s.as_bytes();
    if bytes.len() != TRACE_ID_WIDTH * 2 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = hex_nibble(bytes[i * 2])?;
        let lo = hex_nibble(bytes[i * 2 + 1])?;
        *slot = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// --- status_code equality extraction ---

/// A `status_code = <literal>` equality (either operand order) -> the
/// matching `ravel_rspan::skip_index` status bit. Anything else (wrong
/// operator, non-`status_code` column, a value outside `0..=2`) yields `None`
/// and widens.
///
/// The literal's value, not its `ScalarValue` width, decides the bit: DataFusion
/// may coerce a bare SQL integer literal to whichever integer type the
/// `status_code` (`UInt8`) column has, or leave it wider, and no other
/// integer-column pushdown exists yet in this crate to confirm which -- so
/// [`lit_i64`] accepts every plausible width defensively.
fn status_code_equality(be: &BinaryExpr) -> Option<u8> {
    if be.op != Operator::Eq {
        return None;
    }
    let value = if is_status_code_col(&be.left) {
        lit_i64(&be.right)?
    } else if is_status_code_col(&be.right) {
        lit_i64(&be.left)?
    } else {
        return None;
    };
    status_bit(value)
}

fn is_status_code_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "status_code")
}

/// The `ravel_rspan::skip_index` status bit for a `StatusCode` byte value
/// (`0=Unset`, `1=Ok`, `2=Error`), or `None` for anything outside that range
/// (widen: an out-of-range status_code can never match any real row, but
/// this extractor's job is only to narrow the *read set*, not to prove a
/// predicate unsatisfiable, so it contributes no bound rather than a `0`
/// mask here).
fn status_bit(value: i64) -> Option<u8> {
    match value {
        0 => Some(STATUS_BIT_UNSET),
        1 => Some(STATUS_BIT_OK),
        2 => Some(STATUS_BIT_ERROR),
        _ => None,
    }
}

// --- service_name equality extraction ---

/// A `service_name = <literal>` equality (either operand order) -> the
/// service name string. Anything else (wrong operator, non-`service_name`
/// column, non-string literal) yields `None` and widens.
fn service_name_equality(be: &BinaryExpr) -> Option<String> {
    if be.op != Operator::Eq {
        return None;
    }
    if is_service_name_col(&be.left) {
        lit_utf8(&be.right)
    } else if is_service_name_col(&be.right) {
        lit_utf8(&be.left)
    } else {
        None
    }
}

fn is_service_name_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "service_name")
}

// --- name equality extraction (mirrors service_name) ---

/// A `name = <literal>` equality (either operand order) -> the span name
/// string. Anything else (wrong operator, non-`name` column, non-string
/// literal) yields `None` and widens.
fn name_equality(be: &BinaryExpr) -> Option<String> {
    if be.op != Operator::Eq {
        return None;
    }
    if is_name_col(&be.left) {
        lit_utf8(&be.right)
    } else if is_name_col(&be.right) {
        lit_utf8(&be.left)
    } else {
        None
    }
}

fn is_name_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "name")
}

fn lit_utf8(e: &Expr) -> Option<String> {
    let sv = match e {
        Expr::Literal(sv, _) => sv,
        _ => return None,
    };
    match sv {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::logical_expr::{BinaryExpr, and, or};
    use datafusion::prelude::{col, lit};
    use ravel_rspan::skip_index::{BlockEntry, SkipIndex};

    use super::*;

    fn ts_lit(v: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(v), None))
    }

    fn fsb(bytes: [u8; 16]) -> Expr {
        lit(ScalarValue::FixedSizeBinary(16, Some(bytes.to_vec())))
    }

    /// A block whose trace_id/ts/duration/status ranges are maximally wide,
    /// so a test can narrow only the one field it cares about and be sure no
    /// other axis is what caused a survive/prune result.
    fn full_range_block() -> BlockEntry {
        BlockEntry {
            block_offset: 0,
            block_len: 0,
            block_crc32c: 0,
            record_count: 1,
            min_trace_id: [0u8; 16],
            max_trace_id: [0xffu8; 16],
            min_start_ts: i64::MIN,
            max_end_ts: i64::MAX,
            min_duration_ns: i64::MIN,
            max_duration_ns: i64::MAX,
            status_mask: STATUS_BIT_UNSET | STATUS_BIT_OK | STATUS_BIT_ERROR,
        }
    }

    #[test]
    fn start_and_end_ts_bounds_fold_into_one_window() {
        // Lower bound from start_ts, upper bound from end_ts: both fold in.
        let p = extract_spans(&[
            col("start_ts").gt_eq(ts_lit(100)),
            col("end_ts").lt(ts_lit(200)),
        ]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(100), Some(199)));
        assert_eq!((p.ts_min(), p.ts_max()), (100, 199));
        assert_eq!(p.trace_id, None);
    }

    #[test]
    fn between_on_either_ts_column() {
        let e = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("end_ts")),
            negated: false,
            low: Box::new(ts_lit(10)),
            high: Box::new(ts_lit(20)),
        });
        let p = extract_spans(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(10), Some(20)));
    }

    #[test]
    fn no_bound_widens_to_the_full_window() {
        let p = SpansPushdown::default();
        assert_eq!((p.ts_min(), p.ts_max()), (i64::MIN, i64::MAX));

        // An OR anywhere at the top level contributes no bound.
        let range = and(
            col("start_ts").gt_eq(ts_lit(100)),
            col("end_ts").lt(ts_lit(200)),
        );
        let mixed = or(range, col("name").eq(lit("x")));
        let p = extract_spans(&[mixed]);
        assert_eq!((p.ts_lo, p.ts_hi, p.trace_id), (None, None, None));
    }

    #[test]
    fn trace_id_equality_selects_the_trace_fast_path() {
        let tid = [7u8; 16];
        let p = extract_spans(&[fsb(tid).eq(col("trace_id"))]);
        assert_eq!(p.trace_id, Some(tid));
        // The resulting SpanQuery is the cheap single-trace lookup, not a bare
        // ts_range scan.
        assert_eq!(p.span_query(), SpanQuery::trace(tid, i64::MIN, i64::MAX));

        // Combined with a ts window, the window rides along on the trace query.
        let p = extract_spans(&[
            col("trace_id").eq(fsb(tid)),
            col("start_ts").gt_eq(ts_lit(5)),
            col("end_ts").lt_eq(ts_lit(9)),
        ]);
        assert_eq!(p.span_query(), SpanQuery::trace(tid, 5, 9));
    }

    #[test]
    fn no_trace_id_yields_a_bare_ts_range_query() {
        let p = extract_spans(&[col("start_ts").gt_eq(ts_lit(5))]);
        assert_eq!(p.span_query(), SpanQuery::ts_range(5, i64::MAX));
    }

    #[test]
    fn trace_id_accepts_hex_and_binary_literals_and_rejects_bad_ones() {
        let tid = [0xabu8; 16];
        let hex = "ab".repeat(16); // 32 chars
        let p = extract_spans(&[col("trace_id").eq(lit(hex.clone()))]);
        assert_eq!(p.trace_id, Some(tid));

        // Uppercase hex decodes to the same bytes.
        let p = extract_spans(&[col("trace_id").eq(lit(hex.to_uppercase()))]);
        assert_eq!(p.trace_id, Some(tid));

        // A binary literal of the wrong width is rejected (widen).
        let short = lit(ScalarValue::Binary(Some(vec![1, 2, 3])));
        let p = extract_spans(&[col("trace_id").eq(short)]);
        assert_eq!(p.trace_id, None);

        // A 31-char (odd/short) hex string is rejected.
        let p = extract_spans(&[col("trace_id").eq(lit("ab".repeat(15) + "a"))]);
        assert_eq!(p.trace_id, None);

        // Non-hex characters are rejected.
        let p = extract_spans(&[col("trace_id").eq(lit("z".repeat(32)))]);
        assert_eq!(p.trace_id, None);
    }

    #[test]
    fn duration_bounds_fold_into_a_window_with_checked_add_on_strict_gt() {
        let p = extract_spans(&[
            col("duration_ns").gt(lit(500_000_000i64)),
            col("duration_ns").lt_eq(lit(900_000_000i64)),
        ]);
        assert_eq!(p.duration_lo, Some(500_000_001));
        assert_eq!(p.duration_hi, Some(900_000_000));
        assert_eq!(p.duration_window(), Some((500_000_001, 900_000_000)));
    }

    #[test]
    fn no_duration_bound_is_unconstrained_not_a_zero_window() {
        let p = SpansPushdown::default();
        assert_eq!(p.duration_window(), None);
    }

    #[test]
    fn status_code_equality_maps_to_the_matching_bit() {
        let p = extract_spans(&[col("status_code").eq(lit(2i64))]);
        assert_eq!(p.status_mask, Some(STATUS_BIT_ERROR));

        let p = extract_spans(&[col("status_code").eq(lit(0i64))]);
        assert_eq!(p.status_mask, Some(STATUS_BIT_UNSET));

        // Out of range: contributes nothing rather than an unsound bit.
        let p = extract_spans(&[col("status_code").eq(lit(9i64))]);
        assert_eq!(p.status_mask, None);
    }

    #[test]
    fn two_status_code_equalities_and_intersect() {
        // status_code = Ok AND status_code = Error is unsatisfiable; the
        // intersected mask is 0, which correctly prunes every block since no
        // row can ever satisfy both equalities.
        let p = extract_spans(&[
            col("status_code").eq(lit(1i64)),
            col("status_code").eq(lit(2i64)),
        ]);
        assert_eq!(p.status_mask, Some(0));
    }

    #[test]
    fn service_name_equality_is_last_writer_wins() {
        let p = extract_spans(&[col("service_name").eq(lit("checkout"))]);
        assert_eq!(p.service_name, Some("checkout".to_string()));

        // Last writer wins across two conjuncts, mirroring trace_id.
        let p = extract_spans(&[
            col("service_name").eq(lit("checkout")),
            col("service_name").eq(lit("payments")),
        ]);
        assert_eq!(p.service_name, Some("payments".to_string()));
    }

    #[test]
    fn name_equality_is_extracted_last_writer_wins() {
        // Either operand order.
        let p = extract_spans(&[col("name").eq(lit("GET /cart"))]);
        assert_eq!(p.name, Some("GET /cart".to_string()));
        let p = extract_spans(&[lit("GET /cart").eq(col("name"))]);
        assert_eq!(p.name, Some("GET /cart".to_string()));

        // Last writer wins; service_name and name are independent axes.
        let p = extract_spans(&[
            col("name").eq(lit("a")),
            col("name").eq(lit("b")),
            col("service_name").eq(lit("checkout")),
        ]);
        assert_eq!(p.name, Some("b".to_string()));
        assert_eq!(p.service_name, Some("checkout".to_string()));

        // A non-string literal is rejected (widen).
        let p = extract_spans(&[col("name").eq(lit(7i64))]);
        assert_eq!(p.name, None);
    }

    #[test]
    fn a_disjunction_in_one_conjunct_drops_only_that_conjunct() {
        // duration_ns > 5 stays; the OR'd conjunct on status_code/service_name
        // contributes nothing, on either axis it touches.
        let p = extract_spans(&[
            col("duration_ns").gt(lit(5i64)),
            or(
                col("status_code").eq(lit(1i64)),
                col("service_name").eq(lit("x")),
            ),
        ]);
        assert_eq!(p.duration_lo, Some(6));
        assert_eq!(p.status_mask, None);
        assert_eq!(p.service_name, None);
    }

    #[test]
    fn or_nested_under_and_still_drops_the_whole_conjunct() {
        // `status_code = 1 OR (status_code = 2 AND duration_ns > 5)`: the top
        // node is Or, so duration_ns > 5 is not always true (only in the
        // second disjunct) and must not be pushed even though it is itself an
        // AND conjunct one level down.
        let inner = and(
            col("status_code").eq(lit(2i64)),
            col("duration_ns").gt(lit(5i64)),
        );
        let mixed = or(col("status_code").eq(lit(1i64)), inner);
        let p = extract_spans(&[mixed]);
        assert_eq!(p, SpansPushdown::default());
    }

    #[test]
    fn negated_between_is_treated_as_a_disjunction_and_contributes_nothing() {
        let e = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("end_ts")),
            negated: true,
            low: Box::new(ts_lit(10)),
            high: Box::new(ts_lit(20)),
        });
        let p = extract_spans(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (None, None));
    }

    // --- widen-only soundness: extracted bounds fed into the real
    // ravel_rspan::skip_index::candidate_blocks, proving a block containing a
    // matching row always survives (not just "some blocks get dropped") ---

    #[test]
    fn duration_gt_boundary_excludes_the_threshold_row_but_keeps_the_next_one() {
        // `duration_ns > 500_000_000`: a row of exactly 500_000_000 does not
        // match (strict), a row of 500_000_001 does.
        let p = extract_spans(&[col("duration_ns").gt(lit(500_000_000i64))]);
        let window = p.duration_window().expect("duration bound extracted");

        let at_threshold = BlockEntry {
            min_duration_ns: 500_000_000,
            max_duration_ns: 500_000_000,
            ..full_range_block()
        };
        let one_above = BlockEntry {
            min_duration_ns: 500_000_001,
            max_duration_ns: 500_000_001,
            ..full_range_block()
        };
        let index = SkipIndex::new(vec![at_threshold, one_above]);
        let kept = index.candidate_blocks(None, i64::MIN, i64::MAX, Some(window), None);
        assert!(
            kept.contains(&1),
            "a block whose only row is one ns past a strict > threshold must survive"
        );
    }

    #[test]
    fn duration_gteq_boundary_keeps_the_exact_threshold_row() {
        // `duration_ns >= 500_000_000`: a row of exactly 500_000_000 matches.
        let p = extract_spans(&[col("duration_ns").gt_eq(lit(500_000_000i64))]);
        let window = p.duration_window().expect("duration bound extracted");

        let at_threshold = BlockEntry {
            min_duration_ns: 500_000_000,
            max_duration_ns: 500_000_000,
            ..full_range_block()
        };
        let index = SkipIndex::new(vec![at_threshold]);
        let kept = index.candidate_blocks(None, i64::MIN, i64::MAX, Some(window), None);
        assert_eq!(
            kept,
            vec![0],
            "a row exactly at a >= threshold must survive pruning"
        );
    }

    #[test]
    fn status_mask_soundness_a_block_with_the_matching_status_survives() {
        let p = extract_spans(&[col("status_code").eq(lit(2i64))]);
        let mask = p.status_mask.expect("status bit extracted");

        let has_error = BlockEntry {
            status_mask: STATUS_BIT_ERROR,
            ..full_range_block()
        };
        let no_error = BlockEntry {
            status_mask: STATUS_BIT_OK | STATUS_BIT_UNSET,
            ..full_range_block()
        };
        let index = SkipIndex::new(vec![has_error, no_error]);
        let kept = index.candidate_blocks(None, i64::MIN, i64::MAX, None, Some(mask));
        assert!(
            kept.contains(&0) && !kept.contains(&1),
            "only the block that can hold an Error row may survive"
        );
    }

    // --- issue #519 first increment: same-axis disjunction ---

    #[test]
    fn same_axis_status_or_unions_the_masks() {
        // `status_code = 1 OR status_code = 2` (issue #519's headline
        // example): the union `OK | ERROR`, not the old widen-to-nothing.
        let disjunctive = or(
            col("status_code").eq(lit(1i64)),
            col("status_code").eq(lit(2i64)),
        );
        let p = extract_spans(&[disjunctive]);
        assert_eq!(
            p.status_mask,
            Some(STATUS_BIT_OK | STATUS_BIT_ERROR),
            "a same-axis status OR must push the union mask"
        );
    }

    #[test]
    fn status_in_list_unions_the_masks() {
        // The common target (deliverable item 3): `status_code IN (1, 2)`
        // lowers to the identical union mask a same-axis `OR` produces.
        let in_list = col("status_code").in_list(vec![lit(1i64), lit(2i64)], false);
        let p = extract_spans(&[in_list]);
        assert_eq!(p.status_mask, Some(STATUS_BIT_OK | STATUS_BIT_ERROR));

        // NOT IN is a different (AND-of-inequalities) shape, not attempted.
        let not_in = col("status_code").in_list(vec![lit(1i64)], true);
        let p = extract_spans(&[not_in]);
        assert_eq!(p.status_mask, None);
    }

    #[test]
    fn same_axis_status_union_still_and_intersects_with_sibling_conjuncts() {
        // `status_code = 1 AND (status_code = 1 OR status_code = 2)`: the
        // OR's union (OK|ERROR) intersects with the sibling equality's OK
        // bit, same top-level AND semantics as two plain equalities.
        let p = extract_spans(&[
            col("status_code").eq(lit(1i64)),
            or(
                col("status_code").eq(lit(1i64)),
                col("status_code").eq(lit(2i64)),
            ),
        ]);
        assert_eq!(p.status_mask, Some(STATUS_BIT_OK));
    }

    /// Soundness proof: a block that can hold ONLY the status named by one
    /// branch of a same-axis OR (not the other) must still survive pruning.
    /// If the union in `same_axis_status_union` were an intersection instead
    /// (flip the `mask |= disjunct_status_mask(d)?;` fold in
    /// `same_axis_status_union` to `mask &= ...`, seeded from `!0` instead of
    /// `0`), this test fails: `STATUS_BIT_OK & STATUS_BIT_ERROR == 0`, and a
    /// block whose only rows are `Ok` would be wrongly pruned by a
    /// `status_code = 1 OR status_code = 2` predicate even though every one
    /// of its rows satisfies the `= 1` branch.
    #[test]
    fn same_axis_status_or_soundness_a_block_matching_one_branch_survives() {
        let disjunctive = or(
            col("status_code").eq(lit(1i64)),
            col("status_code").eq(lit(2i64)),
        );
        let p = extract_spans(&[disjunctive]);
        let mask = p.status_mask.expect("same-axis OR extracts a union mask");

        let only_ok = BlockEntry {
            status_mask: STATUS_BIT_OK,
            ..full_range_block()
        };
        let only_unset = BlockEntry {
            status_mask: STATUS_BIT_UNSET,
            ..full_range_block()
        };
        let index = SkipIndex::new(vec![only_ok, only_unset]);
        let kept = index.candidate_blocks(None, i64::MIN, i64::MAX, None, Some(mask));
        assert_eq!(
            kept,
            vec![0],
            "the Ok-only block matches the `= 1` branch and must survive; \
             the Unset-only block matches neither branch and may be pruned"
        );
    }

    #[test]
    fn same_axis_duration_or_unions_the_ranges() {
        // `duration_ns < 100 OR duration_ns > 1000`: the union is
        // unrepresentable as one contiguous range without widening through
        // the gap, so both sides come out unbounded -- correct (a
        // single-range parameter can only over-approximate two disjoint
        // half-open rays), and still strictly better than dropping the whole
        // conjunct only when at least one side stays bounded (see the
        // two-sided-bound case below).
        let p = extract_spans(&[or(
            col("duration_ns").lt(lit(100i64)),
            col("duration_ns").gt(lit(1000i64)),
        )]);
        assert_eq!(p.duration_window(), None);

        // Two closed ranges union into their hull: `[100, 200] u [500, 600]`
        // -> `[100, 600]`. A block whose duration falls in the gap (e.g.
        // 300) is not soundly prunable from a single range, so the hull
        // over-approximates there -- still sound, just not maximally tight.
        let p = extract_spans(&[or(
            and(
                col("duration_ns").gt_eq(lit(100i64)),
                col("duration_ns").lt_eq(lit(200i64)),
            ),
            and(
                col("duration_ns").gt_eq(lit(500i64)),
                col("duration_ns").lt_eq(lit(600i64)),
            ),
        )]);
        assert_eq!(p.duration_window(), Some((100, 600)));
    }

    /// Soundness proof, duration axis: a block matching only the low branch
    /// of a same-axis duration OR must survive. If the hull hi hi/lo fold
    /// used `los.into_iter().flatten().max()`/`his...min()` (intersection
    /// shape) instead of the `min`/`max` this file actually uses, the hull
    /// for `[100,200] OR [500,600]` would collapse to `(500, 200)` (lo > hi,
    /// the empty range), which `duration_window()` still returns as `Some`
    /// and the skip index would prune every block -- including the
    /// `[100,200]`-only block a real row in that range must survive in.
    #[test]
    fn same_axis_duration_or_soundness_a_block_matching_one_branch_survives() {
        let p = extract_spans(&[or(
            and(
                col("duration_ns").gt_eq(lit(100i64)),
                col("duration_ns").lt_eq(lit(200i64)),
            ),
            and(
                col("duration_ns").gt_eq(lit(500i64)),
                col("duration_ns").lt_eq(lit(600i64)),
            ),
        )]);
        let window = p.duration_window().expect("same-axis OR extracts a window");

        let low_branch_only = BlockEntry {
            min_duration_ns: 150,
            max_duration_ns: 150,
            ..full_range_block()
        };
        let index = SkipIndex::new(vec![low_branch_only]);
        let kept = index.candidate_blocks(None, i64::MIN, i64::MAX, Some(window), None);
        assert_eq!(
            kept,
            vec![0],
            "a block matching only the [100,200] branch must survive pruning"
        );
    }

    #[test]
    fn cross_axis_disjunction_still_bails_widen_safe() {
        // `status_code = 2 OR duration_ns > 5e8`: neither axis alone covers
        // every disjunct (a row can satisfy the predicate via either
        // disjunct, so no single-axis bound is sound), so this stays
        // refused exactly like before this increment: no bound on either
        // axis, and DataFusion's `Inexact` residual re-applies the original
        // predicate above the scan.
        let mixed = or(
            col("status_code").eq(lit(2i64)),
            col("duration_ns").gt(lit(500_000_000i64)),
        );
        let p = extract_spans(&[mixed]);
        assert_eq!(p, SpansPushdown::default());
    }

    /// Differential/equivalence test (prove-the-test): a corpus of blocks
    /// spanning every `status_code` value, pruned by the same-axis union a
    /// `status_code = 1 OR status_code = 2` predicate extracts. Proves two
    /// things at once: pruning actually engaged (fewer candidates than the
    /// full corpus, via a real counter on the returned Vec, not just that
    /// extraction produced a mask), and every row the unpushed predicate
    /// would keep is still reachable afterward (the widen-safe equivalence:
    /// re-evaluating the original OR over each surviving block's exact
    /// per-row status set -- modeled here by each block's singleton
    /// `status_mask`, so "block's status" and "row's status" coincide --
    /// never finds a match among the pruned-away blocks).
    #[test]
    fn same_axis_status_union_pruning_engages_and_stays_sound_over_a_corpus() {
        let disjunctive = or(
            col("status_code").eq(lit(1i64)),
            col("status_code").eq(lit(2i64)),
        );
        let p = extract_spans(&[disjunctive]);
        let mask = p.status_mask.expect("same-axis OR extracts a union mask");

        // One block per status value, 40 of each, so the corpus is large
        // enough that "pruning happened" is not a one-block coincidence.
        let statuses = [STATUS_BIT_UNSET, STATUS_BIT_OK, STATUS_BIT_ERROR];
        let blocks: Vec<BlockEntry> = (0..120u32)
            .map(|i| BlockEntry {
                status_mask: statuses[(i % 3) as usize],
                ..full_range_block()
            })
            .collect();
        let want_survive_count = blocks
            .iter()
            .filter(|b| b.status_mask != STATUS_BIT_UNSET)
            .count();
        let index = SkipIndex::new(blocks.clone());

        let kept = index.candidate_blocks(None, i64::MIN, i64::MAX, None, Some(mask));

        // Pruning engaged: a real counter, not a mask-was-extracted proxy.
        assert!(
            kept.len() < blocks.len(),
            "the Unset-only blocks must be pruned: kept {} of {}",
            kept.len(),
            blocks.len()
        );
        assert_eq!(
            kept.len(),
            want_survive_count,
            "exactly the Ok/Error blocks survive, none of the Unset ones"
        );
        // Widen-safe equivalence: every kept block's own status really can
        // satisfy the original OR (re-evaluated exactly, per-row status ==
        // the block's singleton mask here), so pushdown dropped nothing the
        // unpushed predicate would have kept.
        for &i in &kept {
            let s = blocks[i].status_mask;
            assert!(
                s == STATUS_BIT_OK || s == STATUS_BIT_ERROR,
                "block {i} survived pruning but satisfies neither OR branch"
            );
        }
    }

    #[test]
    fn unrecognized_shapes_contribute_nothing() {
        // A binary on a column no shape recognizes (`span_id` has no pushdown).
        let be = BinaryExpr::new(Box::new(col("span_id")), Operator::Eq, Box::new(lit("x")));
        let p = extract_spans(&[Expr::BinaryExpr(be)]);
        assert_eq!(p, SpansPushdown::default());
    }
}
