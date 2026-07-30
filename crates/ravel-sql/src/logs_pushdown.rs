//! Filter pushdown for the `logs` table, under the same pruning-soundness
//! invariant the metrics pushdown obeys (crate::pushdown,
//! docs/arrow-datafusion-plan.md section 2 "Filter pushdown"): pruning may only
//! ever *widen* the read set relative to the query's true need, never narrow
//! it. `LogsTableProvider::supports_filters_pushdown` returns `Inexact` for
//! every filter, so DataFusion always re-applies the originals above the scan;
//! exactness comes from that residual plus the scan re-applying nothing
//! destructive.
//!
//! Recognized shapes (everything else contributes nothing and widens):
//!
//! - **ts range**: top-level AND conjuncts of the bare `ts` column compared to
//!   a literal timestamp (`>=`, `>`, `<`, `<=`, `=`), and `BETWEEN`. Feeds
//!   segment-level pruning and the fetch's [`LogQuery`] ts bounds, identical to
//!   the metrics `ts_lo`/`ts_hi` shape.
//! - **word/phrase search** `has_word(col, 'literal')` and the plain
//!   `col LIKE '%word%'` substring shape: see below. Only `has_word` is pushed;
//!   `LIKE` is deliberately not, for soundness.
//!
//! # Why `LIKE '%word%'` is recognized but not pushed
//!
//! ADR-0033 lists "literal extraction from plain `LIKE '%word%'` patterns...
//! feeding `Predicate::HasWord`". Implementing that literally is **unsound**
//! here, and soundness wins (the invariant is never a valid trade-off):
//! `RlogReader::scan` applies a pushed `HasWord` as an *exact per-row filter*
//! (reader.rs, `phrase_match`), not merely a bloom prune, and token/phrase
//! matching is not a superset of SQL substring `LIKE`. The tokenizer lowercases
//! and splits on non-alphanumerics, so `body LIKE '%time%'` matches `"timeout"`
//! and `"TIME"` while `HasWord{word:"time"}` matches neither — pushing it would
//! drop rows the query needs, which no residual can recover. So `LIKE` is
//! recognized (for a future exact-substring predicate) but contributes no
//! pushed predicate today; `has_word`, whose SQL semantics are defined to equal
//! `HasWord` exactly (crate::logs_udf), is the sound text-pruning path.
//!
//! # Why stream-attribute equalities are not pushed
//!
//! `attrs['k'] = 'v'` (lowered to `get_field(attrs, 'k') = 'v'`) is **not**
//! extracted. It is tempting to resolve it against STREAM_DIR into a
//! `Predicate::StreamIn` prune (ADR-0033 first described exactly this), but that
//! is unsound under the ADR-0033 amendment's merged `attrs` column. `attrs`
//! merges resource + scope + record attributes with the record winning on a key
//! collision, so a record's `attrs['k']` can differ from its stream-identifying
//! resource/scope attributes. A `StreamIn` built from stream-level attributes
//! therefore drops a record whose match lives only in its per-record dynamic
//! attributes (resource `service.name = worker`, record attribute
//! `service.name = api`, query `= 'api'`) — a genuine narrowing, not a widen, so
//! it fails the pruning-soundness invariant and no residual can recover the lost
//! rows. Because there is no sound stream-level prune for this predicate, it is
//! not pushed at all: it is evaluated entirely by DataFusion's `Inexact`
//! residual against the merged `attrs` column ([`crate::logs_scan`]). Making it a
//! sound prune would need a record-attribute-aware index, an ADR-0033 follow-up.
//!
//! (Separately, the `attrs['k']` subscript *syntax* does not plan on this crate's
//! DataFusion build — `features = ["sql"]` registers no nested-expression
//! `ExprPlanner`, so the subscript fails query planning loudly rather than being
//! silently mis-evaluated; wiring it is a gate item for issue #240.)

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use ravel_logseg::{FieldSel, Predicate};

use crate::logs_udf::HAS_WORD_UDF;

/// Everything the extractor pulled out of a `logs` filter set, all widen-only.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct LogsPushdown {
    /// Inclusive lower bound on `ts` in nanoseconds, if provably required.
    pub ts_lo: Option<i64>,
    /// Inclusive upper bound on `ts` in nanoseconds, if provably required.
    pub ts_hi: Option<i64>,
    /// Content predicates handed straight to `RlogReader::scan`. Only shapes
    /// whose SQL semantics equal the reader's exact filter are pushed (today:
    /// `has_word`).
    pub content: Vec<Predicate>,
}

impl LogsPushdown {
    /// The inclusive lower ts bound for the fetch's [`LogQuery`]: the extracted
    /// bound, or `i64::MIN` when none was proven (scan everything).
    pub fn ts_min(&self) -> i64 {
        self.ts_lo.unwrap_or(i64::MIN)
    }

    /// The inclusive upper ts bound for the fetch's [`LogQuery`]: the extracted
    /// bound, or `i64::MAX` when none was proven.
    pub fn ts_max(&self) -> i64 {
        self.ts_hi.unwrap_or(i64::MAX)
    }
}

/// Extract all sound pushdown from a `logs` filter set. Each element of
/// `filters` is an implicit top-level AND conjunct; nested `AND`s are split too.
pub fn extract_logs(filters: &[Expr]) -> LogsPushdown {
    let mut out = LogsPushdown::default();
    for f in filters {
        walk_conjunct(f, &mut out);
    }
    out
}

fn walk_conjunct(expr: &Expr, out: &mut LogsPushdown) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
        && *op == Operator::And
    {
        walk_conjunct(left, out);
        walk_conjunct(right, out);
        return;
    }
    handle_leaf(expr, out);
}

fn handle_leaf(expr: &Expr, out: &mut LogsPushdown) {
    match expr {
        Expr::BinaryExpr(be) => handle_binary(be, out),
        Expr::Between(bt) if !bt.negated => {
            // BETWEEN low AND high desugars to `ts >= low AND ts <= high`. A
            // negated BETWEEN is an OR of two ranges: not sound to narrow.
            if is_ts_col(&bt.expr) {
                if let Some(lo) = lit_ts_ns(&bt.low) {
                    tighten_lo(out, lo);
                }
                if let Some(hi) = lit_ts_ns(&bt.high) {
                    tighten_hi(out, hi);
                }
            }
        }
        // A bare `has_word(col, 'literal')` boolean predicate.
        Expr::ScalarFunction(sf) if sf.func.name() == HAS_WORD_UDF => {
            if let Some(p) = has_word_predicate(&sf.args) {
                out.content.push(p);
            }
        }
        _ => {}
    }
}

fn handle_binary(be: &BinaryExpr, out: &mut LogsPushdown) {
    // ts vs literal timestamp comparison (either operand order). Stream-attribute
    // equalities (`get_field(attrs, 'k') = 'v'`) are deliberately not extracted;
    // see the module doc ("Why stream-attribute equalities are not pushed").
    if let Some((op, ts_ns)) = ts_comparison(be) {
        apply_ts_bound(out, op, ts_ns);
    }
}

// --- ts bound extraction (mirrors crate::pushdown's metrics logic) ---

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

fn apply_ts_bound(out: &mut LogsPushdown, op: Operator, ns: i64) {
    match op {
        Operator::GtEq => tighten_lo(out, ns),
        // `ts > L` is `ts >= L+1` in integer ns; on overflow drop the bound.
        Operator::Gt => {
            if let Some(lo) = ns.checked_add(1) {
                tighten_lo(out, lo);
            }
        }
        Operator::LtEq => tighten_hi(out, ns),
        // `ts < U` is `ts <= U-1`; on underflow drop the bound.
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

fn tighten_lo(out: &mut LogsPushdown, candidate: i64) {
    out.ts_lo = Some(match out.ts_lo {
        Some(cur) => cur.max(candidate),
        None => candidate,
    });
}

fn tighten_hi(out: &mut LogsPushdown, candidate: i64) {
    out.ts_hi = Some(match out.ts_hi {
        Some(cur) => cur.min(candidate),
        None => candidate,
    });
}

fn is_ts_col(e: &Expr) -> bool {
    matches!(e, Expr::Column(c) if c.name == "ts")
}

/// A literal timestamp in nanoseconds, or `None` for any non-timestamp or
/// non-literal expression. Integer literals are rejected (ambiguous scale), a
/// mis-scaled bound would narrow. Scaling is exact; on overflow the literal is
/// rejected (widen). Identical to the metrics extractor.
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

// --- content predicate extraction ---

/// `has_word(col, 'literal')` -> a [`Predicate::HasWord`] over the recognized
/// text field. Only `body` and `severity_text` map to a field selector; any
/// other first argument contributes nothing (widen). The word literal is passed
/// through verbatim; the reader tokenizes it, exactly as the UDF does.
fn has_word_predicate(args: &[Expr]) -> Option<Predicate> {
    if args.len() != 2 {
        return None;
    }
    let field = text_field_sel(&args[0])?;
    let word = lit_utf8(&args[1])?;
    Some(Predicate::HasWord { field, word })
}

fn text_field_sel(e: &Expr) -> Option<FieldSel> {
    match e {
        Expr::Column(c) if c.name == "body" => Some(FieldSel::Body),
        Expr::Column(c) if c.name == "severity_text" => Some(FieldSel::SeverityText),
        _ => None,
    }
}

fn lit_utf8(e: &Expr) -> Option<String> {
    match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::logical_expr::{BinaryExpr, and, or};
    use datafusion::prelude::{col, lit};

    use super::*;
    use crate::logs_udf::has_word_udf;

    fn ts_lit(v: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(v), None))
    }

    #[test]
    fn ts_bounds_and_between() {
        let p = extract_logs(&[col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200))]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(100), Some(199)));
        assert_eq!((p.ts_min(), p.ts_max()), (100, 199));

        let e = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("ts")),
            negated: false,
            low: Box::new(ts_lit(10)),
            high: Box::new(ts_lit(20)),
        });
        let p = extract_logs(&[e]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(10), Some(20)));
    }

    #[test]
    fn no_ts_bound_widens_to_everything() {
        let p = LogsPushdown::default();
        assert_eq!((p.ts_min(), p.ts_max()), (i64::MIN, i64::MAX));

        // An OR anywhere at the top level contributes no bound.
        let range = and(col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200)));
        let mixed = or(range, col("severity_num").gt(lit(5)));
        let p = extract_logs(&[mixed]);
        assert_eq!((p.ts_lo, p.ts_hi), (None, None));
    }

    #[test]
    fn has_word_becomes_content_predicate() {
        let e = has_word_udf().call(vec![col("body"), lit("timeout")]);
        let p = extract_logs(&[e]);
        assert_eq!(
            p.content,
            vec![Predicate::HasWord {
                field: FieldSel::Body,
                word: "timeout".into()
            }]
        );

        // has_word over a non-text/unrecognized column contributes nothing.
        let e = has_word_udf().call(vec![col("attrs"), lit("timeout")]);
        assert!(extract_logs(&[e]).content.is_empty());
    }

    #[test]
    fn like_is_not_pushed() {
        // `body LIKE '%time%'` is recognized syntactically but must contribute
        // no pushed predicate (soundness; see module doc).
        let like = Expr::Like(datafusion::logical_expr::Like {
            negated: false,
            expr: Box::new(col("body")),
            pattern: Box::new(lit("%time%")),
            escape_char: None,
            case_insensitive: false,
        });
        let p = extract_logs(&[like]);
        assert!(p.content.is_empty());
    }

    #[test]
    fn unrecognized_shapes_contribute_nothing() {
        // A binary that is neither a ts comparison nor an attrs equality.
        let be = BinaryExpr::new(Box::new(col("flags")), Operator::Eq, Box::new(lit(1u32)));
        let p = extract_logs(&[Expr::BinaryExpr(be)]);
        assert_eq!(p, LogsPushdown::default());
    }
}
