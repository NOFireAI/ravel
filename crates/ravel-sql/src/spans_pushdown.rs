//! Filter pushdown for the `spans` table, under the same pruning-soundness
//! invariant the `logs` and metrics pushdowns obey (crate::logs_pushdown,
//! crate::pushdown, docs/arrow-datafusion-plan.md section 2 "Filter
//! pushdown"): pruning may only ever *widen* the read set relative to the
//! query's true need, never narrow it. `SpansTableProvider::
//! supports_filters_pushdown` returns `Inexact` for every filter, so
//! DataFusion always re-applies the originals above the scan; exactness comes
//! from that residual.
//!
//! Five shapes are recognized (everything else contributes nothing and widens):
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
//!   mirroring `trace_id`.
//!
//! All five are conjunctive-only (ADR-0045 decision 5): a disjunction anywhere
//! in a conjunct's own subtree ([`contains_or`]) drops that whole conjunct
//! rather than being soundly pushed, since refusing to push is always
//! widen-safe (the `Inexact` residual re-applies it). This crate does not
//! attempt a sound OR pushdown; see [`contains_or`]'s doc for what one would
//! require.
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
    /// pinned, if any (last writer wins, mirroring [`Self::trace_id`]).
    pub service_name: Option<String>,
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
        _ => {}
    }
}

/// True if `expr`'s subtree contains a disjunction anywhere: an `Or`
/// `BinaryExpr` at any depth, or a negated `BETWEEN` (`NOT (col BETWEEN lo
/// AND hi)` desugars to `col < lo OR col > hi`, a disjunction in substance
/// even though it has no `Expr::BinaryExpr(Or)` node).
///
/// No shape in this file attempts to push a disjunction soundly; a burned
/// prior attempt is the reason ("do not attempt to handle OR"). Refusing is
/// always widen-safe: DataFusion's `Inexact` residual re-applies the
/// original predicate above the scan, so dropping the whole conjunct only
/// ever costs pruning, never correctness.
///
/// A sound disjunctive pushdown would need a different algorithm, not an
/// extension of this one: each disjunct would have to be extracted into its
/// own bound, all disjuncts would have to constrain the *same* single axis
/// (a mixed `duration_ns > 5 OR status_code = 1` cannot produce one bound on
/// either axis, since a row can satisfy the predicate via either disjunct
/// alone), and the per-disjunct bounds would then have to be combined by
/// *union* (widest span / broadest mask covering every disjunct) rather than
/// this file's AND-intersection (narrowest span / tightest mask). Detecting
/// "all disjuncts hit the same column" and building the union bound is the
/// unimplemented part; this function only ever detects and refuses.
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
    // `trace_id =`, `status_code =`, or `service_name =` equality pins its
    // respective axis. Every other binary shape contributes nothing and
    // widens. Column names are disjoint, so at most one branch ever matches.
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
        // equalities either.
        out.status_mask = Some(match out.status_mask {
            Some(cur) => cur & bit,
            None => bit,
        });
    } else if let Some(name) = service_name_equality(be) {
        // Last writer wins, mirroring trace_id: two different names ANDed
        // are unsatisfiable, and pinning either still drops no needed row.
        out.service_name = Some(name);
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

    #[test]
    fn unrecognized_shapes_contribute_nothing() {
        // A binary that is neither a ts comparison nor a trace_id equality.
        let be = BinaryExpr::new(Box::new(col("name")), Operator::Eq, Box::new(lit("x")));
        let p = extract_spans(&[Expr::BinaryExpr(be)]);
        assert_eq!(p, SpansPushdown::default());
    }
}
