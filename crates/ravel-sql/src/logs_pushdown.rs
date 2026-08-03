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
//! # Why attribute equalities are still not pushed (issue #510)
//!
//! `attrs['k'] = 'v'` (lowered by the map-field planner to
//! `get_field(attrs, 'k') = 'v'`, issue #507) and `attrs['k'] IN (...)` are
//! **not** extracted into `content`, and this is a soundness decision, not an
//! oversight. The reasoning has two layers, and ADR-0049's POSTINGS section
//! removes only the first.
//!
//! **Layer 1 — no stream-level prune.** It is tempting to resolve the equality
//! against STREAM_DIR into a `Predicate::StreamIn` prune (ADR-0033 first
//! described exactly this), but that is unsound under the ADR-0033 amendment's
//! merged `attrs` column. `attrs` merges resource + scope + record attributes
//! with the record winning on a key collision, so a record's `attrs['k']` can
//! differ from its stream-identifying resource/scope attributes. A `StreamIn`
//! built from stream-level attributes drops a record whose match lives only in
//! its per-record dynamic attributes (resource `service.name = worker`, record
//! attribute `service.name = api`, query `= 'api'`) — a narrowing, not a widen.
//! ADR-0049's POSTINGS section is the "record-attribute-aware index" ADR-0033
//! named as the fix for this layer: it indexes the per-record dynamic columns at
//! block granularity, and `RlogReader::scan` already probes it per
//! `Predicate::Equals` arm (reader.rs), skipping any field it has not indexed
//! (`Ok(None)`), so the *pruning* it drives is widen-only.
//!
//! **Layer 2 — the reader's content channel is an exact per-record filter, and
//! this is the blocker.** The `content: Vec<Predicate>` this extractor feeds is
//! not a prune-only hint. Every arm is AND-combined into the single predicate
//! `RlogReader::scan` evaluates *exactly, per row*, and `eval` resolves a
//! `Predicate::Equals { field: FieldSel::Attr(k), .. }` against the record's
//! own dynamic column plus `attrs_raw` overflow only — never against its
//! resource/scope stream attributes (reader.rs `equals`; the write path keeps
//! resource/scope attrs out of the per-record columns, see
//! `ravel_otlp::logs_normalize` and `ravel_logseg::writer::resolve_row`). So the
//! reader's `Equals{Attr}` is the *per-record* predicate, a strict subset of the
//! merged `attrs['k'] = 'v'`: it matches `per-record k = v`, never
//! `resource/scope k = v` where the record carries no own `k`.
//!
//! Pushing it would therefore make the scan return a **subset** of the rows the
//! query needs. Pushdown here is always `Inexact`, which requires the scan to
//! return a *superset* (DataFusion re-applies the original above it to reach
//! exactness); a subset is a data-loss bug the residual cannot repair, because
//! the residual only ever removes emitted rows, never adds a dropped one. The
//! concrete case is the resource-only match
//! (`crate::logs_provider::tests::attrs_subscript_plans_and_filters_correctly`'s
//! `ts=4`: resource `service.name = api`, no per-record attribute): the merged
//! residual keeps it, the reader's per-record `Equals` drops it. This is the
//! exact mirror of the layer-1 record-only case — the merged predicate is a
//! *union* over two attribute layers, and neither a stream-level nor a
//! per-record-level exact filter is a sound over-approximation of it.
//!
//! So the equality is evaluated entirely by DataFusion's `Inexact` residual over
//! the merged `attrs` column ([`crate::logs_scan`]), exactly as before. Wiring
//! the POSTINGS prune from SQL needs a `ravel-logseg` change this crate cannot
//! make: the reader must expose the postings-eligible `Equals{Attr}` arms as a
//! **prune-only** channel (used for block pruning, not row filtering, leaving
//! exactness to the merged residual), or `eval` must resolve `Equals{Attr}`
//! against the merged resource+scope+record view. Until one of those lands,
//! extracting the equality is unsound. See the issue #510 report.

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
    // ts vs literal timestamp comparison (either operand order). Attribute
    // equalities (`get_field(attrs, 'k') = 'v'`) and `attrs['k'] IN (...)` are
    // deliberately not extracted; see the module doc ("Why attribute equalities
    // are still not pushed") for why pushing them into the reader's exact
    // per-record `content` channel would drop resource/scope-only matches.
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
    use datafusion::functions::core::expr_fn::get_field;
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

    /// `attrs['k'] = 'v'` (planned to `get_field(attrs, 'k') = 'v'`) and
    /// `attrs['k'] IN (...)` are deliberately NOT extracted into `content`. The
    /// reader would apply an extracted `Predicate::Equals { field: Attr(..) }`
    /// as an exact per-record row filter, which under-returns resource/scope-
    /// only matches the merged `attrs` residual must keep (module doc, "Why
    /// attribute equalities are still not pushed"). This locks that decision:
    /// no `Predicate` is emitted for either shape, so the residual stays the
    /// sole, exact evaluator. Correct end-to-end results for these shapes,
    /// including the resource-only match a naive push drops, are proven over
    /// the full SQL path in `crate::logs_provider::tests`.
    #[test]
    fn attribute_equality_and_in_are_not_extracted() {
        let attr = || get_field(col("attrs"), "service.name");

        // Equality, both operand orders.
        assert!(extract_logs(&[attr().eq(lit("api"))]).content.is_empty());
        assert!(extract_logs(&[lit("api").eq(attr())]).content.is_empty());

        // `IN` list: an OR of equalities; the reader intersects `Equals` arms
        // (a conjunction), so one arm per element would be doubly unsound.
        // Extract nothing (issue #519 tracks a sound disjunctive form).
        let in_list = attr().in_list(vec![lit("api"), lit("worker")], false);
        assert!(extract_logs(&[in_list]).content.is_empty());

        // The equality does not disturb a ts bound extracted from the same AND.
        let p = extract_logs(&[col("ts").gt_eq(ts_lit(100)), attr().eq(lit("api"))]);
        assert_eq!((p.ts_lo, p.content.len()), (Some(100), 0));
    }

    /// Shapes that must never be extracted even once a sound attribute prune
    /// exists: an inequality, an `OR` across different keys, a `NOT`, and a
    /// comparison against a non-literal. None contributes a `content`
    /// predicate today (nothing does), so this pins the invariant that they
    /// stay non-extractable and are left to the residual.
    #[test]
    fn non_extractable_attribute_shapes_contribute_nothing() {
        let sn = || get_field(col("attrs"), "service.name");
        let region = || get_field(col("attrs"), "region");

        // Inequality.
        assert!(extract_logs(&[sn().not_eq(lit("api"))]).content.is_empty());
        // OR across different keys.
        assert!(
            extract_logs(&[or(sn().eq(lit("api")), region().eq(lit("eu")))])
                .content
                .is_empty()
        );
        // NOT around an equality.
        assert!(extract_logs(&[!sn().eq(lit("api"))]).content.is_empty());
        // Comparison against a non-literal (attr vs attr).
        assert!(extract_logs(&[sn().eq(region())]).content.is_empty());
    }
}
