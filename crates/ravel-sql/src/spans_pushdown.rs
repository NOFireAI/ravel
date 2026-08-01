//! Filter pushdown for the `spans` table, under the same pruning-soundness
//! invariant the `logs` and metrics pushdowns obey (crate::logs_pushdown,
//! crate::pushdown, docs/arrow-datafusion-plan.md section 2 "Filter
//! pushdown"): pruning may only ever *widen* the read set relative to the
//! query's true need, never narrow it. `SpansTableProvider::
//! supports_filters_pushdown` returns `Inexact` for every filter, so
//! DataFusion always re-applies the originals above the scan; exactness comes
//! from that residual.
//!
//! Two shapes are recognized (everything else contributes nothing and widens):
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

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// Byte width of a trace id: the fixed length a `trace_id =` literal must have
/// to be a valid RSPAN trace key (`ravel_rspan::record::TRACE_ID_WIDTH`).
const TRACE_ID_WIDTH: usize = 16;

/// Everything the extractor pulled out of a `spans` filter set, all widen-only.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
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
        _ => {}
    }
}

fn handle_binary(be: &BinaryExpr, out: &mut SpansPushdown) {
    // A ts (start_ts/end_ts) vs literal-timestamp comparison folds into the
    // window; a `trace_id = <literal>` equality pins the fast-path key. Every
    // other binary shape contributes nothing and widens.
    if let Some((op, ts_ns)) = ts_comparison(be) {
        apply_ts_bound(out, op, ts_ns);
    } else if let Some(tid) = trace_id_equality(be) {
        // Last writer wins; two different trace ids ANDed are unsatisfiable, and
        // pinning either still drops no needed row (no record has both).
        out.trace_id = Some(tid);
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::logical_expr::{BinaryExpr, and, or};
    use datafusion::prelude::{col, lit};

    use super::*;

    fn ts_lit(v: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(v), None))
    }

    fn fsb(bytes: [u8; 16]) -> Expr {
        lit(ScalarValue::FixedSizeBinary(16, Some(bytes.to_vec())))
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
    fn unrecognized_shapes_contribute_nothing() {
        // A binary that is neither a ts comparison nor a trace_id equality.
        let be = BinaryExpr::new(Box::new(col("name")), Operator::Eq, Box::new(lit("x")));
        let p = extract_spans(&[Expr::BinaryExpr(be)]);
        assert_eq!(p, SpansPushdown::default());
    }
}
