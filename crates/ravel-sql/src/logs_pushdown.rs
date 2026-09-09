//! Filter pushdown for the `logs` table, under the same pruning-soundness
//! invariant the metrics pushdown obeys (crate::pushdown): pruning may only
//! ever *widen* the read set relative to the query's true need, never narrow
//! it. `LogsTableProvider::supports_filters_pushdown` returns `Inexact` for
//! every filter except one it can prove the reader re-verifies per row
//! ([`filter_is_exact`]: a pure `ts` bound and/or `has_word` call), so
//! DataFusion re-applies every other original above the scan; exactness comes
//! from that residual plus the scan re-applying nothing destructive.
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
//! # How attribute equalities are pushed (prune-only)
//!
//! `attrs['k'] = 'v'` (lowered to `get_field(attrs, 'k') = 'v'`, either operand
//! order) is extracted into the prune-only [`LogsPushdown::prune`] channel, not
//! into `content`. `attrs` is the merged view: resource + scope + per-record
//! attributes, the record winning on a key collision ([`crate::rlog_attrs`]).
//! Two layers of the old gap are now closed:
//!
//! - Layer 1, the index. ADR-0049's POSTINGS section gives exact block-level
//!   pruning on an indexed per-record attribute value.
//! - Layer 2, the coupling. The prune channel drives that pruning without
//!   feeding the reader's exact per-row filter. `RlogReader::scan_pruned` treats
//!   a prune arm as a block prune only and never evaluates it per row. This
//!   matters because the reader's per-record `Equals` resolves against a
//!   record's own dynamic column and `attrs_raw` overflow only, never the
//!   resource/scope blob, so it is a strict subset of the merged equality.
//!   Pushing it as an exact filter drops a record whose match lives only in its
//!   resource/scope attributes (resource `service.name = worker`, record
//!   attribute `service.name = api`, query `= 'api'`); a prune cannot, because a
//!   field the POSTINGS index does not cover prunes nothing (widen-only,
//!   ADR-0013).
//!
//! The equality is therefore pruned but still evaluated exactly by DataFusion's
//! `Inexact` residual over the merged `attrs` column ([`crate::logs_scan`]),
//! which stays the sole exact evaluator. Nothing changes about which rows the
//! query returns; only which blocks the fetch must read.
//!
//! `attrs['k'] IN (...)` stays unextracted. An `IN` list is a disjunction, and
//! the prune channel intersects its arms, so a sound disjunctive prune needs a
//! different shape. It contributes nothing to either channel.
//!
//! The `attrs['k']` subscript *syntax* plans through the hand-written
//! `crate::map_field_planner` `ExprPlanner`; the older note that it
//! failed planning under `features = ["sql"]` is closed.
//!
//! # How declared typed columns are pushed (prune-only, ADR-0093)
//!
//! A tenant may *declare* an attribute key as a native typed column (ADR-0090),
//! so a query reads `status_code = 500` against a real `Int64`/`Boolean`/
//! `Binary`/dictionary Arrow column rather than `attrs['status_code'] = '500'`
//! over the stringified map. A bare `Expr::Column` whose name is not one of the
//! nine fixed `logs` columns ([`crate::logs_schema`]) is resolved against the
//! tenant's declared vocabulary ([`crate::declared`]) and, on a match, dispatched
//! by its [`DeclaredType`] into the same two prune primitives the `attrs`/`ts`
//! shapes already feed. This is an **allowlist**, not a best-effort translation:
//! only the shapes below are extracted, and every other shape declines exactly
//! as an undeclared column would (declining to prune is always widen-safe).
//!
//! - `I64`/`Bool` + `<`,`<=`,`>`,`>=`,`=`, or `BETWEEN`, against a literal whose
//!   `ScalarValue` type exactly matches the declared type -> a
//!   [`Predicate::NumRange`] with bit-pattern bounds ([`ravel_logseg::block`]'s
//!   `NumStat` encoding: an `i64` as its two's-complement `u64`, a `bool` as
//!   `0`/`1`). This drives `SkipIndex::candidate_blocks` (#331); it carries no
//!   POSTINGS version gate.
//! - `I64` + `IN (v1, v2, ...)` -> ONE envelope `NumRange` spanning
//!   `[min(vs), max(vs)]`, never one arm per value. `Predicate` has no
//!   disjunction and the reader intersects arms, so a per-value arm would prove
//!   some block disjoint that the full `IN` does not, silently dropping rows. The
//!   envelope is deliberately coarser (it also spans values between the listed
//!   ones); the `Inexact` residual excludes those, never the prune. The same
//!   envelope is recognized in two forms: a literal `Expr::InList` (a large list)
//!   and a same-column `col = v1 OR col = v2 OR ...` disjunction, which is what
//!   DataFusion's simplifier rewrites a *small* `IN` into before the scan ever
//!   sees it. A disjunction across different columns, or of any non-equality
//!   shape, is not a representable single range and declines (widen-safe).
//! - `Str`/`Bytes` + `=`, against a matching-type literal ->
//!   [`Predicate::Equals`] on `FieldSel::Attr`, byte-identical to the
//!   `attrs['k'] = 'v'` predicate, feeding POSTINGS. `Str`/`Bytes` `IN (...)` is
//!   not extracted (a disjunction, as above).
//! - Anything else -- `!=`, `NOT`, a negated `BETWEEN`, `IS [NOT] NULL`, a
//!   general `OR` (anything but the same-column I64 equality union above), a
//!   range operator on a `Str`/`Bytes` column, or a literal whose type does not
//!   match the declared type (including a `Cast`-wrapped operand DataFusion's own
//!   coercion may have inserted, which is no longer a bare `Expr::Column` and so
//!   fails resolution) -- is not extracted. A mismatched literal is never coerced
//!   to fit.
//!
//! Two soundness caveats are inherited unchanged, not introduced here. The
//! equality half inherits the POSTINGS-section-version decline (a section written
//! before the #333 fix declines all equality pruning) and the over-conservative
//! "a name that also carries a non-`Str` column declines entirely" rule from the
//! existing `attrs['k']='v'` path. The `NumRange` half has neither: under
//! ADR-0095's single-version RLOG regime every stat is merged-view-correct, and
//! an absent stat is "no information", never "no match" (ADR-0013), so an object
//! predating the column's declaration falls through and is scanned, not pruned.
//!
//! F64 is out of scope (no `DeclaredType::F64` exists yet). The dispatch is one
//! `match` on [`DeclaredType`], so F64 is a one-arm addition once its plumbing
//! lands -- but that arm MUST honor [`Predicate::NumRange`]'s float contract
//! (widen a zero-including range across both `0.0`/`-0.0` bit patterns, never
//! build a bound from a NaN literal); the I64/Bool code here does not, because
//! neither case is reachable for an integer or boolean literal.

use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use ravel_logseg::{AttrValue, FieldSel, FieldType, Predicate};

use crate::declared::{DeclaredColumn, DeclaredType};
use crate::logs_udf::HAS_WORD_UDF;

/// The scalar UDF name an `attrs['k']` subscript lowers to
/// ([`crate::map_field_planner`]).
const GET_FIELD_UDF: &str = "get_field";

/// Everything the extractor pulled out of a `logs` filter set, all widen-only.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct LogsPushdown {
    /// Inclusive lower bound on `ts` in nanoseconds, if provably required.
    pub ts_lo: Option<i64>,
    /// Inclusive upper bound on `ts` in nanoseconds, if provably required.
    pub ts_hi: Option<i64>,
    /// Content predicates handed straight to `RlogReader::scan` as the exact
    /// per-row filter. Only shapes whose SQL semantics equal the reader's exact
    /// filter are pushed (today: `has_word`).
    pub content: Vec<Predicate>,
    /// Prune-only attribute equalities (`attrs['k'] = 'v'`), each a
    /// `Predicate::Equals` on `FieldSel::Attr`. These drive POSTINGS block
    /// pruning in `RlogReader::scan_pruned` and nothing else. They are never fed
    /// to `content`: the reader's per-record `Equals` is a strict subset of the
    /// merged-view SQL equality, so evaluating one exactly would drop a
    /// resource/scope-only match. The merged residual stays the sole exact
    /// evaluator, so pushdown stays `Inexact`. See the module doc.
    pub prune: Vec<Predicate>,
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
///
/// `declared` is the tenant's declared typed attribute columns (ADR-0090),
/// resolved once per plan by `SqlExecutor` and threaded down. An empty slice
/// reproduces the pre-ADR-0093 behavior exactly: a bare `Expr::Column` resolves
/// against nothing, so only the `ts`/`attrs`/`has_word` shapes are extracted.
pub fn extract_logs(filters: &[Expr], declared: &[DeclaredColumn]) -> LogsPushdown {
    let mut out = LogsPushdown::default();
    for f in filters {
        walk_conjunct(f, &mut out, declared);
    }
    out
}

/// Whether `filter` is captured EXACTLY by the ts and content channels, with
/// nothing left over for a residual to evaluate.
///
/// [`crate::logs_provider::LogsTableProvider::supports_filters_pushdown`]
/// reports `TableProviderFilterPushDown::Exact` for exactly the filters this
/// answers `true` for, which deletes them from the plan. So it fails closed:
/// every conjunct of `filter` must be a shape whose pushed predicate is
/// *equivalent* to it, not merely implied by it.
///
/// Two shapes qualify, and only because the reader re-verifies both per row
/// (`ravel_logseg::reader`'s `eval`, its `Predicate::TsRange` and
/// `Predicate::HasWord` arms both read the row's own value): a `ts` bound and a
/// `has_word(col, 'literal')` call. Everything routed to the prune-only
/// [`LogsPushdown::prune`] channel, and everything not recognized at all,
/// answers `false`.
pub fn filter_is_exact(filter: &Expr, declared: &[DeclaredColumn]) -> bool {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter
        && *op == Operator::And
    {
        return filter_is_exact(left, declared) && filter_is_exact(right, declared);
    }
    leaf_is_exact(filter, declared)
}

/// [`filter_is_exact`] for one non-`AND` conjunct. The extraction is
/// [`handle_leaf`] itself and the shapes are recognized with the same primitives
/// it dispatches on, so the two cannot disagree about what is recognized. What
/// is added here is per-shape *completeness*: which channel field the shape must
/// have populated for the pushed predicate to mean the whole leaf. A bound the
/// extractor dropped (an overflowing `ts > <max literal>`, a `BETWEEN` with a
/// non-timestamp edge) leaves its field `None`, so this answers `false` and the
/// filter stays `Inexact`. A shape `handle_leaf` learns to recognize later also
/// answers `false` until it is added here.
fn leaf_is_exact(expr: &Expr, declared: &[DeclaredColumn]) -> bool {
    let mut out = LogsPushdown::default();
    handle_leaf(expr, &mut out, declared);
    if !out.prune.is_empty() {
        return false;
    }
    match expr {
        // A `ts` comparison denotes one bound, or two for `=`; all of them must
        // have survived `apply_ts_bound`.
        Expr::BinaryExpr(be) => match ts_comparison(be) {
            Some((Operator::Eq, _)) => out.ts_lo.is_some() && out.ts_hi.is_some(),
            Some((Operator::Gt | Operator::GtEq, _)) => out.ts_lo.is_some(),
            Some((Operator::Lt | Operator::LtEq, _)) => out.ts_hi.is_some(),
            _ => false,
        },
        Expr::Between(bt) if !bt.negated && is_ts_col(&bt.expr) => {
            out.ts_lo.is_some() && out.ts_hi.is_some()
        }
        Expr::ScalarFunction(sf) if sf.func.name() == HAS_WORD_UDF => !out.content.is_empty(),
        _ => false,
    }
}

fn walk_conjunct(expr: &Expr, out: &mut LogsPushdown, declared: &[DeclaredColumn]) {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
        && *op == Operator::And
    {
        walk_conjunct(left, out, declared);
        walk_conjunct(right, out, declared);
        return;
    }
    handle_leaf(expr, out, declared);
}

fn handle_leaf(expr: &Expr, out: &mut LogsPushdown, declared: &[DeclaredColumn]) {
    match expr {
        Expr::BinaryExpr(be) => handle_binary(be, out, declared),
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
            } else if let Some(p) = declared_between_predicate(bt, declared) {
                out.prune.push(p);
            }
        }
        // `col IN (v1, v2, ...)` is an `Expr::InList`, not a `BinaryExpr`, so it
        // never flows through `handle_binary`; a declared I64 column's `IN` gets
        // its own envelope-range arm here. The `attrs['k'] IN (...)` shape has a
        // `get_field` (not a bare `Expr::Column`) subscript and so declines.
        Expr::InList(il) if !il.negated => {
            if let Some(p) = declared_in_list_predicate(il, declared) {
                out.prune.push(p);
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

fn handle_binary(be: &BinaryExpr, out: &mut LogsPushdown, declared: &[DeclaredColumn]) {
    // ts vs literal timestamp comparison (either operand order).
    if let Some((op, ts_ns)) = ts_comparison(be) {
        apply_ts_bound(out, op, ts_ns);
        return;
    }
    // A merged-attribute equality `attrs['k'] = 'v'` feeds the prune-only
    // channel. It never becomes a content predicate: the reader would evaluate
    // it against per-record attributes only, a strict subset of the merged view,
    // and drop resource/scope-only matches (see the module doc).
    if let Some(p) = attr_equality_predicate(be) {
        out.prune.push(p);
        return;
    }
    // A comparison against a declared typed column (ADR-0093). Resolved only for
    // a non-fixed column name; dispatched by declared type into the same
    // prune-only NumRange/Equals primitives.
    if let Some(p) = declared_comparison_predicate(be, declared) {
        out.prune.push(p);
        return;
    }
    // A same-column I64 disjunction of equalities: how DataFusion's simplifier
    // delivers a small `col IN (v1, v2, ...)`, expanded to
    // `col = v1 OR col = v2 OR ...` before the scan sees it. It reduces to the
    // same one envelope range the `Expr::InList` arm builds (see the module
    // doc's IN note). A disjunct on any other column or of any other shape
    // declines the whole OR (widen-safe).
    if let Some(p) = declared_i64_or_envelope(be, declared) {
        out.prune.push(p);
    }
}

// --- declared typed column extraction (ADR-0093) ------------------------------

/// The nine fixed `logs` schema columns ([`crate::logs_schema::logs_schema`]).
/// A bare `Expr::Column` naming one of these is handled by the fixed-column path
/// (only `ts` extracts anything today) and is NEVER resolved against declared
/// columns, even if a tenant also happens to declare a column of the same name:
/// the fixed schema always wins, per ADR-0093's resolution-order guard.
const FIXED_LOG_COLUMNS: [&str; 9] = [
    "ts",
    "observed_ts",
    "severity_num",
    "severity_text",
    "body",
    "trace_id",
    "span_id",
    "flags",
    "attrs",
];

fn is_fixed_log_column(name: &str) -> bool {
    FIXED_LOG_COLUMNS.contains(&name)
}

/// The declared column a bare, non-fixed `Expr::Column` resolves to, or `None`
/// for a fixed name, a non-column expression (including a `Cast`-wrapped
/// column), or a name outside the tenant's declared vocabulary.
fn resolve_declared<'a>(e: &Expr, declared: &'a [DeclaredColumn]) -> Option<&'a DeclaredColumn> {
    let Expr::Column(c) = e else {
        return None;
    };
    if is_fixed_log_column(&c.name) {
        return None;
    }
    declared.iter().find(|d| d.key == c.name)
}

/// A comparison (`<`, `<=`, `>`, `>=`, `=`, either operand order) against a
/// declared typed column, resolved to a prune-only [`Predicate`]. Range
/// operators on `Str`/`Bytes`, any negation, and a type-mismatched literal all
/// decline (return `None`), per ADR-0093's allowlist.
fn declared_comparison_predicate(
    be: &BinaryExpr,
    declared: &[DeclaredColumn],
) -> Option<Predicate> {
    // Orient the comparison so `op`/`lit` describe `column op literal`.
    let (dc, op, lit) = if let Some(dc) = resolve_declared(&be.left, declared) {
        (dc, be.op, be.right.as_ref())
    } else {
        let dc = resolve_declared(&be.right, declared)?;
        (dc, flip_op(be.op)?, be.left.as_ref())
    };
    match dc.ty {
        DeclaredType::I64 => {
            let v = lit_i64(lit)?;
            let (min, max) = int_range_bounds(op, v)?;
            Some(num_range(&dc.key, FieldType::I64, min, max))
        }
        DeclaredType::Bool => {
            let b = lit_bool(lit)?;
            let (min, max) = int_range_bounds(op, b as i64)?;
            Some(num_range(&dc.key, FieldType::Bool, min, max))
        }
        DeclaredType::Str => {
            let value = str_eq_value(op, lit)?;
            Some(Predicate::Equals {
                field: FieldSel::Attr(dc.key.clone()),
                value,
            })
        }
        DeclaredType::Bytes => {
            let value = bytes_eq_value(op, lit)?;
            Some(Predicate::Equals {
                field: FieldSel::Attr(dc.key.clone()),
                value,
            })
        }
    }
}

/// `col BETWEEN low AND high` (non-negated) against a declared I64/Bool column ->
/// an inclusive [`Predicate::NumRange`]. `Str`/`Bytes` decline (POSTINGS is an
/// equality index, no ordered range). Both bounds must be matching-type literals,
/// or the whole shape declines.
fn declared_between_predicate(bt: &Between, declared: &[DeclaredColumn]) -> Option<Predicate> {
    let dc = resolve_declared(&bt.expr, declared)?;
    let (ty, lo, hi) = match dc.ty {
        DeclaredType::I64 => (FieldType::I64, lit_i64(&bt.low)?, lit_i64(&bt.high)?),
        DeclaredType::Bool => (
            FieldType::Bool,
            lit_bool(&bt.low)? as i64,
            lit_bool(&bt.high)? as i64,
        ),
        DeclaredType::Str | DeclaredType::Bytes => return None,
    };
    Some(num_range(&dc.key, ty, Some(lo), Some(hi)))
}

/// `col IN (v1, v2, ...)` (non-negated) against a declared I64 column -> ONE
/// envelope [`Predicate::NumRange`] spanning `[min(vs), max(vs)]`. Bool/Str/Bytes
/// `IN` and an empty list decline; any non-`Int64` member declines the whole
/// shape (an unreadable member could match a real row, so a partial envelope
/// would be unsound). See the module doc for why this is one range, not one per
/// value.
fn declared_in_list_predicate(il: &InList, declared: &[DeclaredColumn]) -> Option<Predicate> {
    let dc = resolve_declared(&il.expr, declared)?;
    if dc.ty != DeclaredType::I64 || il.list.is_empty() {
        return None;
    }
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    for e in &il.list {
        let v = lit_i64(e)?;
        lo = lo.min(v);
        hi = hi.max(v);
    }
    Some(num_range(&dc.key, FieldType::I64, Some(lo), Some(hi)))
}

/// An `OR`-rooted conjunct recognized as `col = v1 OR col = v2 OR ...` on ONE
/// declared I64 column -> the same envelope [`Predicate::NumRange`] the
/// `Expr::InList` arm builds. This is the shape DataFusion's simplifier rewrites
/// a small `col IN (...)` into before the scan. `None` unless the expr is an
/// `OR` whose every disjunct is an equality on the *same* declared I64 column
/// against an exact `Int64` literal: a cross-column or non-equality disjunct
/// makes the union unrepresentable as one range, so the whole OR declines
/// (declining to prune is widen-safe).
fn declared_i64_or_envelope(be: &BinaryExpr, declared: &[DeclaredColumn]) -> Option<Predicate> {
    if be.op != Operator::Or {
        return None;
    }
    let mut disjuncts = Vec::new();
    flatten_or(&be.left, &mut disjuncts);
    flatten_or(&be.right, &mut disjuncts);

    let mut name: Option<&str> = None;
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    for d in disjuncts {
        let (dc, v) = eq_i64_on_declared(d, declared)?;
        match name {
            Some(n) if n != dc.key => return None,
            _ => name = Some(&dc.key),
        }
        lo = lo.min(v);
        hi = hi.max(v);
    }
    let name = name?;
    Some(num_range(name, FieldType::I64, Some(lo), Some(hi)))
}

/// Collect the leaf disjuncts of a (possibly nested) `OR` tree into `out`.
fn flatten_or<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryExpr(be) = e
        && be.op == Operator::Or
    {
        flatten_or(&be.left, out);
        flatten_or(&be.right, out);
    } else {
        out.push(e);
    }
}

/// `col = <Int64 literal>` (either operand order) on a declared I64 column ->
/// the column and value, or `None` for any other shape.
fn eq_i64_on_declared<'a>(
    e: &Expr,
    declared: &'a [DeclaredColumn],
) -> Option<(&'a DeclaredColumn, i64)> {
    let Expr::BinaryExpr(be) = e else {
        return None;
    };
    if be.op != Operator::Eq {
        return None;
    }
    let (dc, v) = if let Some(dc) = resolve_declared(&be.left, declared) {
        (dc, lit_i64(&be.right)?)
    } else {
        let dc = resolve_declared(&be.right, declared)?;
        (dc, lit_i64(&be.left)?)
    };
    if dc.ty != DeclaredType::I64 {
        return None;
    }
    Some((dc, v))
}

/// The inclusive `[min, max]` bounds an integer comparison denotes, in i64
/// space, or `None` for an operator outside the allowlist or a bound that would
/// overflow (declining is widen-safe). Applies to Bool too, via its `0`/`1`
/// mapping.
fn int_range_bounds(op: Operator, v: i64) -> Option<(Option<i64>, Option<i64>)> {
    Some(match op {
        Operator::Eq => (Some(v), Some(v)),
        Operator::GtEq => (Some(v), None),
        Operator::Gt => (Some(v.checked_add(1)?), None),
        Operator::LtEq => (None, Some(v)),
        Operator::Lt => (None, Some(v.checked_sub(1)?)),
        _ => return None,
    })
}

/// Build a prune-only [`Predicate::NumRange`] on `name`, encoding the inclusive
/// i64-space bounds as the two's-complement `u64` bit pattern
/// [`ravel_logseg::block`]'s `NumStat` stores (for `Bool`, the `0`/`1` mapping is
/// already the identity under this cast).
fn num_range(name: &str, ty: FieldType, min: Option<i64>, max: Option<i64>) -> Predicate {
    Predicate::NumRange {
        field: FieldSel::Attr(name.to_string()),
        ty,
        min: min.map(|v| v as u64),
        max: max.map(|v| v as u64),
    }
}

/// The `AttrValue::Str` value of an `= 'literal'` comparison on a declared Str
/// column, or `None` for any non-`=` operator or non-string literal. A declared
/// Str column is Arrow `Dictionary(Int32, Utf8)` (ADR-0099), so DataFusion's
/// type coercion wraps the compared literal in a `Dictionary` scalar; unwrap it
/// to the same UTF-8 the POSTINGS `Equals` predicate keys on.
fn str_eq_value(op: Operator, lit: &Expr) -> Option<AttrValue> {
    if op != Operator::Eq {
        return None;
    }
    let Expr::Literal(sv, _) = lit else {
        return None;
    };
    Some(AttrValue::Str(scalar_utf8(sv)?))
}

/// The UTF-8 string a scalar carries, unwrapping a `Dictionary` scalar to its
/// value (a declared Str column's coerced literal), or `None` for a non-string
/// scalar.
fn scalar_utf8(sv: &ScalarValue) -> Option<String> {
    match sv {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        ScalarValue::Dictionary(_, inner) => scalar_utf8(inner),
        _ => None,
    }
}

/// The `AttrValue::Bytes` value of an `= X'..'` comparison on a declared Bytes
/// column, or `None` for any non-`=` operator or non-binary literal.
fn bytes_eq_value(op: Operator, lit: &Expr) -> Option<AttrValue> {
    if op != Operator::Eq {
        return None;
    }
    Some(AttrValue::Bytes(lit_bytes(lit)?))
}

/// An i64 literal, exactly. Only `ScalarValue::Int64` matches: a narrower or
/// wider integer type, or a float, is a type mismatch that declines rather than
/// coerces (ADR-0093).
fn lit_i64(e: &Expr) -> Option<i64> {
    match e {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(*v),
        _ => None,
    }
}

/// A boolean literal, exactly.
fn lit_bool(e: &Expr) -> Option<bool> {
    match e {
        Expr::Literal(ScalarValue::Boolean(Some(b)), _) => Some(*b),
        _ => None,
    }
}

/// A binary literal, in any of DataFusion's binary scalar shapes.
fn lit_bytes(e: &Expr) -> Option<Vec<u8>> {
    match e {
        Expr::Literal(ScalarValue::Binary(Some(b)), _)
        | Expr::Literal(ScalarValue::LargeBinary(Some(b)), _)
        | Expr::Literal(ScalarValue::BinaryView(Some(b)), _) => Some(b.clone()),
        _ => None,
    }
}

/// `get_field(attrs, 'k') = 'v'` (either operand order) -> a prune-only
/// [`Predicate::Equals`] on `FieldSel::Attr("k")` with a string value. Only a
/// string-literal comparison value is recognized; any other value contributes
/// nothing. `attrs['k'] IN (...)` is an `Expr::InList`, not a `BinaryExpr`, so
/// it never reaches here and stays unextracted (a disjunction the intersecting
/// prune channel cannot soundly represent).
fn attr_equality_predicate(be: &BinaryExpr) -> Option<Predicate> {
    if be.op != Operator::Eq {
        return None;
    }
    let (key, value) = match attr_subscript_key(&be.left) {
        Some(k) => (k, lit_utf8(&be.right)?),
        None => {
            let k = attr_subscript_key(&be.right)?;
            (k, lit_utf8(&be.left)?)
        }
    };
    Some(Predicate::Equals {
        field: FieldSel::Attr(key),
        value: AttrValue::Str(value),
    })
}

/// The key literal `k` of an `attrs['k']` subscript, planned as
/// `get_field(attrs, 'k')`. `None` for any other expression, including
/// `get_field` over a column other than `attrs` or with a non-literal key.
fn attr_subscript_key(e: &Expr) -> Option<String> {
    let Expr::ScalarFunction(sf) = e else {
        return None;
    };
    if sf.func.name() != GET_FIELD_UDF || sf.args.len() != 2 {
        return None;
    }
    if !matches!(&sf.args[0], Expr::Column(c) if c.name == "attrs") {
        return None;
    }
    lit_utf8(&sf.args[1])
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
        let p = extract_logs(
            &[col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200))],
            &[],
        );
        assert_eq!((p.ts_lo, p.ts_hi), (Some(100), Some(199)));
        assert_eq!((p.ts_min(), p.ts_max()), (100, 199));

        let e = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("ts")),
            negated: false,
            low: Box::new(ts_lit(10)),
            high: Box::new(ts_lit(20)),
        });
        let p = extract_logs(&[e], &[]);
        assert_eq!((p.ts_lo, p.ts_hi), (Some(10), Some(20)));
    }

    #[test]
    fn no_ts_bound_widens_to_everything() {
        let p = LogsPushdown::default();
        assert_eq!((p.ts_min(), p.ts_max()), (i64::MIN, i64::MAX));

        // An OR anywhere at the top level contributes no bound.
        let range = and(col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(200)));
        let mixed = or(range, col("severity_num").gt(lit(5)));
        let p = extract_logs(&[mixed], &[]);
        assert_eq!((p.ts_lo, p.ts_hi), (None, None));
    }

    #[test]
    fn has_word_becomes_content_predicate() {
        let e = has_word_udf().call(vec![col("body"), lit("timeout")]);
        let p = extract_logs(&[e], &[]);
        assert_eq!(
            p.content,
            vec![Predicate::HasWord {
                field: FieldSel::Body,
                word: "timeout".into()
            }]
        );

        // has_word over a non-text/unrecognized column contributes nothing.
        let e = has_word_udf().call(vec![col("attrs"), lit("timeout")]);
        assert!(extract_logs(&[e], &[]).content.is_empty());
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
        let p = extract_logs(&[like], &[]);
        assert!(p.content.is_empty());
    }

    #[test]
    fn unrecognized_shapes_contribute_nothing() {
        // A binary that is neither a ts comparison nor an attrs equality.
        let be = BinaryExpr::new(Box::new(col("flags")), Operator::Eq, Box::new(lit(1u32)));
        let p = extract_logs(&[Expr::BinaryExpr(be)], &[]);
        assert_eq!(p, LogsPushdown::default());
    }

    #[test]
    fn attribute_equality_and_in_are_not_extracted() {
        // `attrs['service.name'] = 'api'` (lowered to
        // `get_field(attrs, 'service.name') = 'api'`) now goes to the prune-only
        // channel, never to content: the reader would evaluate it against
        // per-record attributes only and drop resource/scope-only matches.
        let expected = vec![Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("api".into()),
        }];

        let eq = get_field(col("attrs"), "service.name").eq(lit("api"));
        let p = extract_logs(&[eq], &[]);
        assert_eq!(p.prune, expected);
        assert!(p.content.is_empty(), "the equality must not become content");
        assert_eq!((p.ts_lo, p.ts_hi), (None, None));

        // The value on the left extracts the identical predicate.
        let eq_flipped = lit("api").eq(get_field(col("attrs"), "service.name"));
        let p = extract_logs(&[eq_flipped], &[]);
        assert_eq!(p.prune, expected);
        assert!(p.content.is_empty());

        // `attrs['k'] IN (...)` is a disjunction: it contributes nothing to
        // either channel. The intersecting prune channel cannot represent it
        // soundly; a disjunctive form is not yet supported.
        let in_list = get_field(col("attrs"), "k").in_list(vec![lit("a"), lit("b")], false);
        let p = extract_logs(&[in_list], &[]);
        assert!(p.content.is_empty());
        assert!(p.prune.is_empty(), "IN must not populate the prune channel");
    }

    #[test]
    fn get_field_over_non_attrs_column_is_not_pushed() {
        // Only the `attrs` map is prunable; a subscript on any other column
        // contributes nothing to either channel.
        let eq = get_field(col("resource"), "service.name").eq(lit("api"));
        let p = extract_logs(&[eq], &[]);
        assert!(p.prune.is_empty());
        assert!(p.content.is_empty());
    }

    // --- declared typed column extraction (ADR-0093) -------------------------

    fn i64_decl() -> Vec<DeclaredColumn> {
        vec![DeclaredColumn::new("status_code", DeclaredType::I64)]
    }

    fn num_range_arm(ty: FieldType, min: Option<u64>, max: Option<u64>) -> Predicate {
        Predicate::NumRange {
            field: FieldSel::Attr("status_code".into()),
            ty,
            min,
            max,
        }
    }

    /// Every I64 comparison operator, either operand order, maps to the inclusive
    /// bit-pattern range it denotes. `>`/`<` shift by one in integer space; a
    /// flipped operand order flips the operator.
    #[test]
    fn i64_comparisons_map_to_num_range() {
        let d = i64_decl();
        let cases = [
            (col("status_code").eq(lit(500i64)), Some(500), Some(500)),
            (col("status_code").gt_eq(lit(500i64)), Some(500), None),
            (col("status_code").gt(lit(500i64)), Some(501), None),
            (col("status_code").lt_eq(lit(500i64)), None, Some(500)),
            (col("status_code").lt(lit(500i64)), None, Some(499)),
            // Flipped operand order: `500 < status_code` is `status_code > 500`.
            (lit(500i64).lt(col("status_code")), Some(501), None),
        ];
        for (expr, min, max) in cases {
            let p = extract_logs(std::slice::from_ref(&expr), &d);
            assert_eq!(
                p.prune,
                vec![num_range_arm(FieldType::I64, min, max)],
                "unexpected range for {expr:?}"
            );
            assert!(p.content.is_empty());
            assert_eq!((p.ts_lo, p.ts_hi), (None, None));
        }
    }

    /// A declared Bool column: `= true`/`= false` degenerate to the `1`/`0`
    /// point range `bool_stat` folds under, sharing the I64 extraction path.
    #[test]
    fn bool_equality_maps_to_bit_point_range() {
        let d = vec![DeclaredColumn::new("is_active", DeclaredType::Bool)];
        let p = extract_logs(&[col("is_active").eq(lit(true))], &d);
        assert_eq!(
            p.prune,
            vec![Predicate::NumRange {
                field: FieldSel::Attr("is_active".into()),
                ty: FieldType::Bool,
                min: Some(1),
                max: Some(1),
            }]
        );
        let p = extract_logs(&[col("is_active").eq(lit(false))], &d);
        assert_eq!(
            p.prune,
            vec![Predicate::NumRange {
                field: FieldSel::Attr("is_active".into()),
                ty: FieldType::Bool,
                min: Some(0),
                max: Some(0),
            }]
        );
    }

    /// A declared I64 `BETWEEN low AND high` is one inclusive range; a negated
    /// BETWEEN (an OR of two half-lines) is not extracted.
    #[test]
    fn i64_between_maps_to_inclusive_range() {
        let d = i64_decl();
        let between = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("status_code")),
            negated: false,
            low: Box::new(lit(200i64)),
            high: Box::new(lit(499i64)),
        });
        let p = extract_logs(&[between], &d);
        assert_eq!(
            p.prune,
            vec![num_range_arm(FieldType::I64, Some(200), Some(499))]
        );

        let negated = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("status_code")),
            negated: true,
            low: Box::new(lit(200i64)),
            high: Box::new(lit(499i64)),
        });
        let p = extract_logs(&[negated], &d);
        assert!(
            p.prune.is_empty(),
            "a negated BETWEEN must not be extracted"
        );
    }

    /// A declared I64 `IN (v1, v2, v3)` collapses to ONE envelope range spanning
    /// the set's min and max, never one arm per value.
    #[test]
    fn i64_in_list_maps_to_one_envelope_range() {
        let d = i64_decl();
        let in_list =
            col("status_code").in_list(vec![lit(200i64), lit(404i64), lit(500i64)], false);
        let p = extract_logs(&[in_list], &d);
        assert_eq!(
            p.prune,
            vec![num_range_arm(FieldType::I64, Some(200), Some(500))],
            "IN must produce a single [min, max] envelope, not one arm per value"
        );
    }

    /// A small `col IN (...)` reaches the scan as `col = v1 OR col = v2 OR ...`
    /// (DataFusion's simplifier expands it). That same-column I64 disjunction
    /// collapses to the SAME envelope range the `Expr::InList` arm builds.
    #[test]
    fn same_column_i64_or_disjunction_maps_to_envelope() {
        let d = i64_decl();
        let or_expr = or(
            or(
                col("status_code").eq(lit(200i64)),
                col("status_code").eq(lit(404i64)),
            ),
            col("status_code").eq(lit(500i64)),
        );
        let p = extract_logs(&[or_expr], &d);
        assert_eq!(
            p.prune,
            vec![num_range_arm(FieldType::I64, Some(200), Some(500))],
            "a same-column equality OR is the small-IN envelope"
        );
    }

    /// A cross-column OR (or any non-equality disjunct) is not a representable
    /// single range and must not extract: one disjunct's range would prune a
    /// block the full disjunction keeps.
    #[test]
    fn cross_column_or_is_not_extracted() {
        let d = vec![
            DeclaredColumn::new("status_code", DeclaredType::I64),
            DeclaredColumn::new("shard", DeclaredType::I64),
        ];
        // Different declared columns.
        let mixed = or(
            col("status_code").eq(lit(200i64)),
            col("shard").eq(lit(3i64)),
        );
        assert!(extract_logs(&[mixed], &d).prune.is_empty());

        // A non-equality disjunct on the same column.
        let ranged = or(
            col("status_code").eq(lit(200i64)),
            col("status_code").gt(lit(400i64)),
        );
        assert!(extract_logs(&[ranged], &d).prune.is_empty());

        // A disjunct on an undeclared column.
        let undeclared = or(
            col("status_code").eq(lit(200i64)),
            col("body").eq(lit("zzz")),
        );
        assert!(extract_logs(&[undeclared], &d).prune.is_empty());
    }

    /// A `NOT IN` is an AND of inequalities, a different shape; not extracted.
    #[test]
    fn i64_negated_in_list_is_not_extracted() {
        let d = i64_decl();
        let in_list = col("status_code").in_list(vec![lit(200i64), lit(404i64)], true);
        let p = extract_logs(&[in_list], &d);
        assert!(p.prune.is_empty());
    }

    /// A declared Str column's `=` maps to the same `Equals` predicate the
    /// `attrs['k'] = 'v'` shape builds, feeding POSTINGS.
    #[test]
    fn str_equality_maps_to_attr_equals() {
        let d = vec![DeclaredColumn::new("region", DeclaredType::Str)];
        let p = extract_logs(&[col("region").eq(lit("eu"))], &d);
        assert_eq!(
            p.prune,
            vec![Predicate::Equals {
                field: FieldSel::Attr("region".into()),
                value: AttrValue::Str("eu".into()),
            }]
        );
        // A range operator on a Str column has no ordered index; declines.
        let p = extract_logs(&[col("region").gt(lit("eu"))], &d);
        assert!(
            p.prune.is_empty(),
            "a Str range operator must not be extracted"
        );
        // Str IN is a disjunction POSTINGS cannot represent; declines.
        let p = extract_logs(
            &[col("region").in_list(vec![lit("eu"), lit("us")], false)],
            &d,
        );
        assert!(p.prune.is_empty(), "a Str IN must not be extracted");
    }

    /// TEST 6: `!=` and a `NOT`-wrapped equality on a declared column produce no
    /// prune arm at all. `NumRange` is a single contiguous range and cannot
    /// express the complement of a point (ADR-0093).
    #[test]
    fn not_equal_and_negation_produce_no_prune_arm() {
        let d = i64_decl();

        let ne = extract_logs(&[col("status_code").not_eq(lit(500i64))], &d);
        assert!(ne.prune.is_empty(), "!= must produce no prune arm");

        let negated = extract_logs(
            &[Expr::Not(Box::new(col("status_code").eq(lit(500i64))))],
            &d,
        );
        assert!(
            negated.prune.is_empty(),
            "a NOT-wrapped equality must produce no prune arm"
        );

        // Bool's point-complement coincidence must NOT be special-cased either.
        let bd = vec![DeclaredColumn::new("is_active", DeclaredType::Bool)];
        let bne = extract_logs(&[col("is_active").not_eq(lit(true))], &bd);
        assert!(bne.prune.is_empty(), "bool != must produce no prune arm");
    }

    /// TEST 7: a comparison against a type-mismatched literal (a float against a
    /// declared I64 column) produces no prune arm. A mismatched literal is never
    /// coerced to fit; extraction declines.
    #[test]
    fn type_mismatched_literal_produces_no_prune_arm() {
        let d = i64_decl();
        let p = extract_logs(&[col("status_code").gt(lit(2.5f64))], &d);
        assert!(
            p.prune.is_empty(),
            "a float literal against an I64 column must not extract"
        );
        // A string literal against an I64 column likewise declines.
        let p = extract_logs(&[col("status_code").eq(lit("500"))], &d);
        assert!(p.prune.is_empty());
    }

    /// The resolution-order guard: a fixed schema name is never resolved against
    /// declared columns, even when a tenant declares one of the same name. A
    /// declared `severity_num` must not turn the fixed `severity_num = 5` into a
    /// prune arm (the fixed-column path owns that name).
    #[test]
    fn a_fixed_name_is_never_resolved_as_declared() {
        let d = vec![DeclaredColumn::new("severity_num", DeclaredType::I64)];
        let p = extract_logs(&[col("severity_num").eq(lit(5i64))], &d);
        assert!(
            p.prune.is_empty(),
            "a fixed schema name must take the fixed-column path, not declared resolution"
        );
    }

    /// A bare column that matches no declared column falls through unchanged:
    /// scanned, not pruned, exactly as before this ADR.
    #[test]
    fn undeclared_column_falls_through() {
        let p = extract_logs(&[col("status_code").eq(lit(500i64))], &[]);
        assert_eq!(p, LogsPushdown::default());
    }
}
