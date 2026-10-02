//! Result comparison for the ClickBench Parquet lane (ADR-2040 section D7):
//! normalize reference and subject rows to a type-width-agnostic [`Cell`],
//! then compare them as a multiset with boundary-key tie reduction at an
//! ORDER BY + LIMIT cut, since two conformant engines may pick different
//! rows among a tie straddling that cut.
//!
//! Floats compare bit-exact ([`Cell::Float`] holds the `f64` bit pattern, so
//! bit-identical NaNs compare equal and differing NaN payloads do not). A
//! float difference inside an otherwise-matching row is recorded as a
//! [`FloatMismatch`] rather than a [`RowMismatch`]: informational, not fatal
//! on its own.

use std::collections::HashSet;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::sqlparser::ast::{
    Expr, LimitClause, OrderBy, OrderByKind, SelectItem, SetExpr, Statement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// A normalized cell value: the common currency both Arrow batches and a
/// JSON reference are reduced to before comparison.
///
/// Integer width is deliberately erased (`Int8`..`UInt64` all become
/// `Int(i128)`): the comparator's job is checking query correctness, not
/// column-type fidelity, and DataFusion is free to widen an aggregate's
/// output type across engines. `Str` and `Bytes` are kept distinct even
/// though both wrap bytes, because `binary_as_string` is an explicit,
/// visible opt-in (ADR-2040 D5) and a column that silently compares equal
/// whether or not it fired would hide exactly the bug that option exists to
/// make visible.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Cell {
    Null,
    Bool(bool),
    /// Any signed or unsigned integer width, widened to `i128`.
    Int(i128),
    /// The `f64` bit pattern (a `Float32` is widened to `f64` first, then
    /// its bits taken), so NaN payloads and -0.0 compare by bits, never `==`.
    Float(u64),
    Str(String),
    Bytes(Vec<u8>),
    /// Days since the Unix epoch (`Date32` natively; `Date64` converted).
    Date(i32),
    /// Nanoseconds since the Unix epoch. The time zone, if any, is dropped:
    /// D7 compares wall-clock instants, not zone-qualified ones.
    Ts(i64),
    /// Unscaled value and scale, e.g. `Decimal(12345, 2)` for `123.45`.
    Decimal(i128, i8),
}

/// The kind a column normalizes to, used to type a JSON reference cell
/// against its subject column (JSON has no native integer/float/date/
/// timestamp distinction, so the subject's Arrow type supplies it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Bool,
    Int,
    Float,
    Str,
    Bytes,
    Date,
    Ts,
    Decimal(i8),
}

/// Everything comparison can fail on before it can even start: a shape the
/// comparator does not understand, not a mismatch within an understood one.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ComparatorError {
    #[error("reference and subject rows have different column counts: {reference} != {subject}")]
    ColumnCountMismatch { reference: usize, subject: usize },
    #[error("column {index}: unsupported Arrow type {data_type:?}")]
    UnsupportedArrowType { index: usize, data_type: DataType },
    #[error("column {index}: JSON value {value} cannot be read as {kind:?}")]
    InvalidJsonCell {
        index: usize,
        value: String,
        kind: ColumnKind,
    },
    #[error("column {index}: invalid date {value:?}: {reason}")]
    InvalidDate {
        index: usize,
        value: String,
        reason: String,
    },
    #[error("column {index}: invalid timestamp {value:?}: {reason}")]
    InvalidTimestamp {
        index: usize,
        value: String,
        reason: String,
    },
    #[error("could not parse SQL: {0}")]
    SqlParse(String),
    #[error(
        "ORDER BY key {expr:?} does not resolve to a projection column (no alias match, no bare \
         column match, no unaliased expression text match) and suite.toml carries no order_key \
         override for statement {statement_number}"
    )]
    UnresolvedOrderKey { statement_number: u32, expr: String },
}

/// The tie-breaking key and truncation shape ADR-2040 D7 compares under: the
/// 0-indexed projection columns a statement's ORDER BY resolves to, its
/// LIMIT (`None` when the statement has no LIMIT), and its OFFSET (`0` when
/// absent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieSpec {
    pub key: Vec<usize>,
    pub limit: Option<u64>,
    pub offset: u64,
}

/// Final judgement a comparison reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Every row (after boundary-tie reduction) matched.
    Pass,
    /// The statement has a LIMIT but no resolvable ORDER BY key, so row
    /// identity is unconstrained; only row and column counts were checked.
    CardinalityOnly,
    /// A row mismatch survived boundary-tie reduction and float tolerance.
    Fail,
}

/// A float cell that differed between a matched reference/subject row pair.
/// Informational: present alongside a `Pass` verdict as long as every other
/// cell in the row matched.
#[derive(Debug, Clone, PartialEq)]
pub struct FloatMismatch {
    pub column: usize,
    /// The matched row's cells (with the mismatching float cell included),
    /// so a reader can identify which logical row the mismatch is in.
    pub row_key: Vec<Cell>,
    pub reference_bits: u64,
    pub subject_bits: u64,
    pub reference_f64: f64,
    pub subject_f64: f64,
}

/// Rows present on one side and absent from the other, after boundary-tie
/// reduction, capped at 20 rows each so a report never grows unbounded on a
/// badly broken comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowMismatch {
    /// In reference, not (after float-tolerant pairing) in subject.
    pub missing: Vec<Vec<Cell>>,
    /// In subject, not in reference.
    pub extra: Vec<Vec<Cell>>,
}

/// The full result of comparing a reference result set against a subject
/// one.
#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonReport {
    pub verdict: Verdict,
    pub float_mismatches: Vec<FloatMismatch>,
    pub row_mismatch: RowMismatch,
    /// Rows exempted from exact comparison because their ORDER BY key tied
    /// with the first or last reference row (summed across both sides).
    pub tie_rows_reduced: u64,
    /// Float cells actually inspected (cells in rows that reached content
    /// comparison; boundary-exempted rows are never inspected).
    pub float_cells_compared: u64,
}

const MAX_MISMATCH_ROWS: usize = 20;

fn column_kind(data_type: &DataType, index: usize) -> Result<ColumnKind, ComparatorError> {
    use DataType::*;
    match data_type {
        Boolean => Ok(ColumnKind::Bool),
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Ok(ColumnKind::Int),
        Float32 | Float64 => Ok(ColumnKind::Float),
        Utf8 | LargeUtf8 | Utf8View => Ok(ColumnKind::Str),
        Binary | LargeBinary | BinaryView | FixedSizeBinary(_) => Ok(ColumnKind::Bytes),
        Date32 | Date64 => Ok(ColumnKind::Date),
        Timestamp(_, _) => Ok(ColumnKind::Ts),
        Decimal128(_, scale) => Ok(ColumnKind::Decimal(*scale)),
        other => Err(ComparatorError::UnsupportedArrowType {
            index,
            data_type: other.clone(),
        }),
    }
}

/// Read row `row` of `array` into a [`Cell`], failing only for an Arrow type
/// this comparator does not understand.
fn cell_from_array(array: &dyn Array, row: usize, index: usize) -> Result<Cell, ComparatorError> {
    if array.is_null(row) {
        return Ok(Cell::Null);
    }
    use DataType::*;
    Ok(match array.data_type() {
        Boolean => Cell::Bool(array.as_boolean().value(row)),
        Int8 => Cell::Int(array.as_primitive::<Int8Type>().value(row) as i128),
        Int16 => Cell::Int(array.as_primitive::<Int16Type>().value(row) as i128),
        Int32 => Cell::Int(array.as_primitive::<Int32Type>().value(row) as i128),
        Int64 => Cell::Int(array.as_primitive::<Int64Type>().value(row) as i128),
        UInt8 => Cell::Int(array.as_primitive::<UInt8Type>().value(row) as i128),
        UInt16 => Cell::Int(array.as_primitive::<UInt16Type>().value(row) as i128),
        UInt32 => Cell::Int(array.as_primitive::<UInt32Type>().value(row) as i128),
        UInt64 => Cell::Int(array.as_primitive::<UInt64Type>().value(row) as i128),
        Float32 => Cell::Float((array.as_primitive::<Float32Type>().value(row) as f64).to_bits()),
        Float64 => Cell::Float(array.as_primitive::<Float64Type>().value(row).to_bits()),
        Utf8 => Cell::Str(array.as_string::<i32>().value(row).to_string()),
        LargeUtf8 => Cell::Str(array.as_string::<i64>().value(row).to_string()),
        Utf8View => Cell::Str(array.as_string_view().value(row).to_string()),
        Binary => Cell::Bytes(array.as_binary::<i32>().value(row).to_vec()),
        LargeBinary => Cell::Bytes(array.as_binary::<i64>().value(row).to_vec()),
        BinaryView => Cell::Bytes(array.as_binary_view().value(row).to_vec()),
        Date32 => Cell::Date(array.as_primitive::<Date32Type>().value(row)),
        // Date64 is milliseconds since the epoch; narrowed to whole days.
        Date64 => Cell::Date((array.as_primitive::<Date64Type>().value(row) / 86_400_000) as i32),
        Timestamp(unit, _tz) => {
            let ns = match unit {
                TimeUnit::Second => {
                    array.as_primitive::<TimestampSecondType>().value(row) * 1_000_000_000
                }
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row) * 1_000_000
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row) * 1_000
                }
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            };
            Cell::Ts(ns)
        }
        Decimal128(_, scale) => {
            Cell::Decimal(array.as_primitive::<Decimal128Type>().value(row), *scale)
        }
        other => {
            return Err(ComparatorError::UnsupportedArrowType {
                index,
                data_type: other.clone(),
            });
        }
    })
}

/// Normalize every row of `batches` into `Vec<Cell>` rows, in batch then
/// row order.
pub fn rows_from_arrow(batches: &[RecordBatch]) -> Result<Vec<Vec<Cell>>, ComparatorError> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for (index, column) in batch.columns().iter().enumerate() {
                cells.push(cell_from_array(column.as_ref(), row, index)?);
            }
            rows.push(cells);
        }
    }
    Ok(rows)
}

/// Howard Hinnant's `days_from_civil`: days since the Unix epoch for a
/// proleptic-Gregorian calendar date. No dependency on a calendar crate
/// (`chrono` is not a workspace dependency); this is the standard
/// constant-time algorithm, valid for every `i64` year.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn parse_date_to_days(index: usize, value: &str) -> Result<i32, ComparatorError> {
    let fail = |reason: &str| ComparatorError::InvalidDate {
        index,
        value: value.to_string(),
        reason: reason.to_string(),
    };
    let parts: Vec<&str> = value.splitn(3, '-').collect();
    if parts.len() != 3 {
        return Err(fail("expected YYYY-MM-DD"));
    }
    let y: i64 = parts[0].parse().map_err(|_| fail("non-numeric year"))?;
    let m: i64 = parts[1].parse().map_err(|_| fail("non-numeric month"))?;
    let d: i64 = parts[2].parse().map_err(|_| fail("non-numeric day"))?;
    Ok(days_from_civil(y, m, d) as i32)
}

/// Strip a trailing `Z`/`z` or a `+00:00`/`-00:00` offset. D7 compares
/// wall-clock instants with the zone dropped, and every timestamp this
/// corpus produces is already zone-naive (`to_timestamp_seconds`), so any
/// non-zero offset is refused rather than silently converted.
fn strip_zero_offset(index: usize, time_part: &str) -> Result<&str, ComparatorError> {
    if let Some(stripped) = time_part.strip_suffix(['Z', 'z']) {
        return Ok(stripped);
    }
    if let Some(pos) = time_part.rfind(['+', '-']) {
        let offset = &time_part[pos..];
        if offset == "+00:00" || offset == "-00:00" {
            return Ok(&time_part[..pos]);
        }
        return Err(ComparatorError::InvalidTimestamp {
            index,
            value: time_part.to_string(),
            reason: format!("non-zero time zone offset {offset} is not supported"),
        });
    }
    Ok(time_part)
}

fn parse_rfc3339_to_ns(index: usize, value: &str) -> Result<i64, ComparatorError> {
    let fail = |reason: &str| ComparatorError::InvalidTimestamp {
        index,
        value: value.to_string(),
        reason: reason.to_string(),
    };
    let t_pos = value.find(['T', 't']).ok_or_else(|| fail("missing T"))?;
    let (date_part, rest) = value.split_at(t_pos);
    let time_part = strip_zero_offset(index, &rest[1..])?;
    let days = parse_date_to_days(index, date_part)?;
    let (hms, nanos) = match time_part.split_once('.') {
        Some((hms, frac)) => {
            let digits = &frac[..frac.len().min(9)];
            let padded = format!("{digits:0<9}");
            (hms, padded.parse::<i64>().map_err(|_| fail("bad fraction"))?)
        }
        None => (time_part, 0),
    };
    let parts: Vec<&str> = hms.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(fail("expected HH:MM:SS"));
    }
    let h: i64 = parts[0].parse().map_err(|_| fail("bad hour"))?;
    let mi: i64 = parts[1].parse().map_err(|_| fail("bad minute"))?;
    let s: i64 = parts[2].parse().map_err(|_| fail("bad second"))?;
    let day_ns = days as i64 * 86_400_000_000_000;
    let time_ns = (h * 3600 + mi * 60 + s) * 1_000_000_000 + nanos;
    Ok(day_ns + time_ns)
}

/// Read one JSON reference cell, typed per `kind` (the corresponding
/// subject column's normalized kind): a JSON number becomes `Float` when
/// `kind` is `Float`, else `Int`; a `YYYY-MM-DD` string becomes `Date`; an
/// RFC 3339 string becomes `Ts`.
pub fn json_cell(
    index: usize,
    value: &serde_json::Value,
    kind: ColumnKind,
) -> Result<Cell, ComparatorError> {
    if value.is_null() {
        return Ok(Cell::Null);
    }
    let invalid = || ComparatorError::InvalidJsonCell {
        index,
        value: value.to_string(),
        kind,
    };
    match kind {
        ColumnKind::Bool => value.as_bool().map(Cell::Bool).ok_or_else(invalid),
        ColumnKind::Int => {
            if let Some(i) = value.as_i64() {
                Ok(Cell::Int(i as i128))
            } else {
                value
                    .as_str()
                    .and_then(|s| s.parse::<i128>().ok())
                    .map(Cell::Int)
                    .ok_or_else(invalid)
            }
        }
        ColumnKind::Float => value
            .as_f64()
            .map(|f| Cell::Float(f.to_bits()))
            .ok_or_else(invalid),
        ColumnKind::Str => value
            .as_str()
            .map(|s| Cell::Str(s.to_string()))
            .ok_or_else(invalid),
        ColumnKind::Bytes => value
            .as_str()
            .map(|s| Cell::Bytes(s.as_bytes().to_vec()))
            .ok_or_else(invalid),
        ColumnKind::Date => value
            .as_str()
            .ok_or_else(invalid)
            .and_then(|s| parse_date_to_days(index, s))
            .map(Cell::Date),
        ColumnKind::Ts => value
            .as_str()
            .ok_or_else(invalid)
            .and_then(|s| parse_rfc3339_to_ns(index, s))
            .map(Cell::Ts),
        ColumnKind::Decimal(scale) => value.as_f64().ok_or_else(invalid).map(|f| {
            Cell::Decimal((f * 10f64.powi(scale as i32)).round() as i128, scale)
        }),
    }
}

/// Normalize a JSON reference (an array of arrays, one inner array of
/// column values per row) using `subject_kinds` to type each column.
pub fn rows_from_json(
    reference: &[serde_json::Value],
    subject_kinds: &[ColumnKind],
) -> Result<Vec<Vec<Cell>>, ComparatorError> {
    reference
        .iter()
        .map(|row| {
            let cells = row.as_array().ok_or_else(|| ComparatorError::InvalidJsonCell {
                index: 0,
                value: row.to_string(),
                kind: subject_kinds.first().copied().unwrap_or(ColumnKind::Str),
            })?;
            if cells.len() != subject_kinds.len() {
                return Err(ComparatorError::ColumnCountMismatch {
                    reference: cells.len(),
                    subject: subject_kinds.len(),
                });
            }
            cells
                .iter()
                .zip(subject_kinds)
                .enumerate()
                .map(|(index, (value, kind))| json_cell(index, value, *kind))
                .collect()
        })
        .collect()
}

fn select_projection(body: &SetExpr) -> Result<Vec<SelectItem>, ComparatorError> {
    match body {
        SetExpr::Select(select) => Ok(select.projection.clone()),
        SetExpr::Query(query) => select_projection(&query.body),
        other => Err(ComparatorError::SqlParse(format!(
            "unsupported query body shape: {other:?}"
        ))),
    }
}

fn order_by_exprs(order_by: &OrderBy) -> Result<Vec<Expr>, ComparatorError> {
    match &order_by.kind {
        OrderByKind::Expressions(exprs) => Ok(exprs.iter().map(|e| e.expr.clone()).collect()),
        OrderByKind::All(_) => Err(ComparatorError::SqlParse(
            "ORDER BY ALL is not supported".to_string(),
        )),
    }
}

fn identifier_tail(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// Resolve one ORDER BY expression to a 0-indexed projection column, per
/// D7's three textual rules in order: (a) alias match, case-insensitive;
/// (b) bare column match against an unaliased projection item, case-
/// sensitive (column identity is case-sensitive); (c) unaliased expression
/// text match (for an un-aliased aggregate like `COUNT(*)` repeated
/// verbatim in ORDER BY).
fn resolve_projection_index(expr: &Expr, projection: &[SelectItem]) -> Option<usize> {
    if let Some(order_name) = identifier_tail(expr) {
        for (i, item) in projection.iter().enumerate() {
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                if alias.value.eq_ignore_ascii_case(order_name) {
                    return Some(i);
                }
            }
        }
        for (i, item) in projection.iter().enumerate() {
            if let SelectItem::UnnamedExpr(pexpr) = item {
                if identifier_tail(pexpr) == Some(order_name) {
                    return Some(i);
                }
            }
        }
    }
    let order_text = expr.to_string();
    for (i, item) in projection.iter().enumerate() {
        if let SelectItem::UnnamedExpr(pexpr) = item {
            if pexpr.to_string() == order_text {
                return Some(i);
            }
        }
    }
    None
}

fn parse_u64_expr(expr: &Expr) -> Result<u64, ComparatorError> {
    expr.to_string()
        .parse::<u64>()
        .map_err(|_| ComparatorError::SqlParse(format!("not a plain integer literal: {expr}")))
}

fn parse_limit_clause(limit_clause: Option<&LimitClause>) -> Result<(Option<u64>, u64), ComparatorError> {
    match limit_clause {
        None => Ok((None, 0)),
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            let limit = limit.as_ref().map(parse_u64_expr).transpose()?;
            let offset = offset
                .as_ref()
                .map(|o| parse_u64_expr(&o.value))
                .transpose()?
                .unwrap_or(0);
            Ok((limit, offset))
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            Ok((Some(parse_u64_expr(limit)?), parse_u64_expr(offset)?))
        }
    }
}

/// Resolve `sql`'s `TieSpec`: its ORDER BY key (by D7's textual rules, or
/// `override_key` when given), its LIMIT, and its OFFSET. `statement_number`
/// is used only to name the statement in [`ComparatorError::UnresolvedOrderKey`].
pub fn resolve_tie_spec(
    statement_number: u32,
    sql: &str,
    override_key: Option<&[usize]>,
) -> Result<TieSpec, ComparatorError> {
    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| ComparatorError::SqlParse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(ComparatorError::SqlParse(format!(
            "expected exactly one statement, found {}",
            statements.len()
        )));
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(ComparatorError::SqlParse(
            "statement is not a SELECT query".to_string(),
        ));
    };
    let (limit, offset) = parse_limit_clause(query.limit_clause.as_ref())?;
    let Some(order_by) = &query.order_by else {
        return Ok(TieSpec {
            key: Vec::new(),
            limit,
            offset,
        });
    };
    let projection = select_projection(&query.body)?;
    let exprs = order_by_exprs(order_by)?;
    let mut key = Vec::with_capacity(exprs.len());
    let mut all_resolved = true;
    for expr in &exprs {
        match resolve_projection_index(expr, &projection) {
            Some(idx) => key.push(idx),
            None => {
                all_resolved = false;
                break;
            }
        }
    }
    if all_resolved {
        return Ok(TieSpec { key, limit, offset });
    }
    match override_key {
        Some(k) => Ok(TieSpec {
            key: k.to_vec(),
            limit,
            offset,
        }),
        None => Err(ComparatorError::UnresolvedOrderKey {
            statement_number,
            expr: exprs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        }),
    }
}

fn project(row: &[Cell], key: &[usize]) -> Vec<Cell> {
    key.iter().map(|&i| row[i].clone()).collect()
}

fn rows_match_except_float(a: &[Cell], b: &[Cell]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            matches!((x, y), (Cell::Float(_), Cell::Float(_))) || x == y
        })
}

fn count_float_cells(row: &[Cell]) -> u64 {
    row.iter().filter(|c| matches!(c, Cell::Float(_))).count() as u64
}

/// Compare `reference` against `subject` under `tie`, applying D7's
/// boundary-key tie reduction. Both must already be normalized (see
/// [`rows_from_arrow`] / [`rows_from_json`]).
pub fn compare(
    reference: &[Vec<Cell>],
    subject: &[Vec<Cell>],
    tie: &TieSpec,
) -> Result<ComparisonReport, ComparatorError> {
    let width = reference
        .first()
        .or_else(|| subject.first())
        .map(Vec::len);
    if let Some(width) = width {
        for row in reference.iter().chain(subject.iter()) {
            if row.len() != width {
                return Err(ComparatorError::ColumnCountMismatch {
                    reference: width,
                    subject: row.len(),
                });
            }
        }
    }

    // A LIMIT with no resolvable ORDER BY key: row identity is genuinely
    // unconstrained (any N rows may come back), so only cardinality can be
    // asserted.
    if tie.limit.is_some() && tie.key.is_empty() {
        let verdict = if reference.len() == subject.len() {
            Verdict::CardinalityOnly
        } else {
            Verdict::Fail
        };
        return Ok(ComparisonReport {
            verdict,
            float_mismatches: Vec::new(),
            row_mismatch: RowMismatch::default(),
            tie_rows_reduced: 0,
            float_cells_compared: 0,
        });
    }

    // Only a LIMIT truncates the result (an OFFSET alone, with no LIMIT, is
    // not representable by this corpus's grammar; sqlparser requires a
    // LIMIT for an OffsetCommaLimit and ADR-2040's statements never use
    // OFFSET without LIMIT), so the boundary set is populated only when a
    // LIMIT is present: without one, both reference and subject are the
    // full deterministic multiset and no row sits at an arbitrary cut.
    let boundary: HashSet<Vec<Cell>> = if tie.limit.is_some() && !tie.key.is_empty() {
        let mut b = HashSet::new();
        if let Some(first) = reference.first() {
            b.insert(project(first, &tie.key));
        }
        if let Some(last) = reference.last() {
            b.insert(project(last, &tie.key));
        }
        b
    } else {
        HashSet::new()
    };

    let is_boundary = |row: &[Cell]| -> bool { !boundary.is_empty() && boundary.contains(&project(row, &tie.key)) };

    let mut tie_rows_reduced = 0u64;
    let mut interior_ref = Vec::new();
    for row in reference {
        if is_boundary(row) {
            tie_rows_reduced += 1;
        } else {
            interior_ref.push(row.clone());
        }
    }
    let mut interior_subj = Vec::new();
    for row in subject {
        if is_boundary(row) {
            tie_rows_reduced += 1;
        } else {
            interior_subj.push(row.clone());
        }
    }

    let float_cells_compared: u64 = interior_ref
        .iter()
        .chain(interior_subj.iter())
        .map(|r| count_float_cells(r))
        .sum();

    let mut counts: std::collections::HashMap<Vec<Cell>, i64> = std::collections::HashMap::new();
    for row in &interior_ref {
        *counts.entry(row.clone()).or_insert(0) += 1;
    }
    for row in &interior_subj {
        *counts.entry(row.clone()).or_insert(0) -= 1;
    }
    let mut missing = Vec::new();
    let mut extra = Vec::new();
    for (row, delta) in counts {
        if delta > 0 {
            for _ in 0..delta {
                missing.push(row.clone());
            }
        } else if delta < 0 {
            for _ in 0..(-delta) {
                extra.push(row.clone());
            }
        }
    }

    let mut float_mismatches = Vec::new();
    let mut remaining_missing = Vec::new();
    let mut used_extra = vec![false; extra.len()];
    for m in missing {
        let mut matched = false;
        for (i, e) in extra.iter().enumerate() {
            if used_extra[i] {
                continue;
            }
            if rows_match_except_float(&m, e) {
                used_extra[i] = true;
                matched = true;
                for (col, (mc, ec)) in m.iter().zip(e.iter()).enumerate() {
                    if let (Cell::Float(mb), Cell::Float(eb)) = (mc, ec) {
                        if mb != eb {
                            float_mismatches.push(FloatMismatch {
                                column: col,
                                row_key: m.clone(),
                                reference_bits: *mb,
                                subject_bits: *eb,
                                reference_f64: f64::from_bits(*mb),
                                subject_f64: f64::from_bits(*eb),
                            });
                        }
                    }
                }
                break;
            }
        }
        if !matched {
            remaining_missing.push(m);
        }
    }
    let remaining_extra: Vec<Vec<Cell>> = extra
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !used_extra[*i])
        .map(|(_, r)| r)
        .collect();

    let verdict = if remaining_missing.is_empty() && remaining_extra.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail
    };

    Ok(ComparisonReport {
        verdict,
        float_mismatches,
        row_mismatch: RowMismatch {
            missing: remaining_missing.into_iter().take(MAX_MISMATCH_ROWS).collect(),
            extra: remaining_extra.into_iter().take(MAX_MISMATCH_ROWS).collect(),
        },
        tie_rows_reduced,
        float_cells_compared,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tie(key: Vec<usize>, limit: Option<u64>, offset: u64) -> TieSpec {
        TieSpec { key, limit, offset }
    }

    /// Required test: tie cut by LIMIT passes when subject picked different
    /// tied rows at the boundary (a GROUP BY ... ORDER BY c DESC LIMIT N
    /// shape, where several groups share the smallest included count).
    #[test]
    fn boundary_tie_with_different_rows_passes() {
        // key = column 0 (the count), rows ordered descending by it.
        // reference's last row (count=1, id="a") ties with other id="b"/"c"
        // rows a real engine could have picked instead (not present in
        // either truncated result, since this reference/subject pair only
        // show what each engine actually returned).
        let reference = vec![
            vec![Cell::Int(5), Cell::Str("x".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            vec![Cell::Int(5), Cell::Str("x".into())],
            vec![Cell::Int(1), Cell::Str("b".into())],
        ];
        let report = compare(&reference, &subject, &tie(vec![0], Some(2), 0)).expect("compare");
        assert_eq!(report.verdict, Verdict::Pass);
        assert_eq!(report.tie_rows_reduced, 2);
    }

    /// Required test: fails when subject picked a row outside the tie (its
    /// replacement row's key does not match the boundary key at all, so it
    /// cannot be a legitimate tie-break variation).
    #[test]
    fn boundary_tie_with_row_outside_tie_fails() {
        let reference = vec![
            vec![Cell::Int(5), Cell::Str("x".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            vec![Cell::Int(5), Cell::Str("x".into())],
            // key (count) is 9, not 1: not a member of the boundary tie.
            vec![Cell::Int(9), Cell::Str("z".into())],
        ];
        let report = compare(&reference, &subject, &tie(vec![0], Some(2), 0)).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
        assert_eq!(report.row_mismatch.missing.len(), 1);
        assert_eq!(report.row_mismatch.extra.len(), 1);
    }

    /// Distinguishing test for wrong implementation (a): reducing EVERY row
    /// to its key tuple (not only boundary rows) would let an interior tie
    /// (away from the first/last reference row) mask a real content bug.
    /// Here rows at positions 1 and 2 (middle of a 4-row, no-LIMIT result)
    /// share key=2 legitimately, but the subject's row for id "p" wrongly
    /// carries a different payload than reference's. Since this tie is NOT
    /// at the boundary (no LIMIT at all: tie.limit is None, so the boundary
    /// set is empty for every row), the correct implementation compares
    /// every row exactly and catches it.
    #[test]
    fn interior_tie_away_from_boundary_is_compared_exactly() {
        let reference = vec![
            vec![Cell::Int(3), Cell::Str("n".into())],
            vec![Cell::Int(2), Cell::Str("p".into())],
            vec![Cell::Int(2), Cell::Str("q".into())],
            vec![Cell::Int(1), Cell::Str("r".into())],
        ];
        let subject = vec![
            vec![Cell::Int(3), Cell::Str("n".into())],
            // Wrong: "p"'s row swapped for a row with a different id at the
            // same key, inside the interior (tie.limit is None here, so
            // nothing is ever exempted). A buggy comparator that projects
            // every row down to its key (ignoring boundary scope entirely)
            // would wrongly call this a Pass.
            vec![Cell::Int(2), Cell::Str("WRONG".into())],
            vec![Cell::Int(2), Cell::Str("q".into())],
            vec![Cell::Int(1), Cell::Str("r".into())],
        ];
        let report = compare(&reference, &subject, &tie(vec![0], None, 0)).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail, "an interior content bug away from any LIMIT boundary must still be caught");
        assert_eq!(report.tie_rows_reduced, 0, "no LIMIT means no boundary rows are ever exempted");
    }

    /// Required test: a float differing in its last bit is listed with
    /// both bit patterns.
    #[test]
    fn float_last_bit_difference_is_listed() {
        let a = 1.0_f64;
        let b = f64::from_bits(a.to_bits() + 1);
        let reference = vec![vec![Cell::Int(1), Cell::Float(a.to_bits())]];
        let subject = vec![vec![Cell::Int(1), Cell::Float(b.to_bits())]];
        let report = compare(&reference, &subject, &tie(vec![], None, 0)).expect("compare");
        assert_eq!(report.verdict, Verdict::Pass, "a float mismatch alone is informational, not fatal");
        assert_eq!(report.float_mismatches.len(), 1);
        let fm = &report.float_mismatches[0];
        assert_eq!(fm.reference_bits, a.to_bits());
        assert_eq!(fm.subject_bits, b.to_bits());
        assert_eq!(report.float_cells_compared, 2);
    }

    /// Required test, and distinguishing test for wrong implementation (b):
    /// a bit-identical NaN must compare equal. Comparing floats with `==`
    /// on `f64` instead of by bits would make this fail, since `NaN == NaN`
    /// is always false under IEEE 754, even for the same bit pattern.
    #[test]
    fn nan_compares_by_bits() {
        let nan_bits = f64::NAN.to_bits();
        let reference = vec![vec![Cell::Float(nan_bits)]];
        let subject = vec![vec![Cell::Float(nan_bits)]];
        let report = compare(&reference, &subject, &tie(vec![], None, 0)).expect("compare");
        assert_eq!(report.verdict, Verdict::Pass);
        assert!(
            report.float_mismatches.is_empty(),
            "a bit-identical NaN must not be reported as a float mismatch: {:?}",
            report.float_mismatches
        );
    }

    /// Required test: Q43's `ORDER BY DATE_TRUNC('minute', M)` does not
    /// resolve under the textual rules (the alias is `M`, not the full
    /// `DATE_TRUNC('minute', M)` text; it is not a bare column; the
    /// unaliased projection text is `DATE_TRUNC('minute',
    /// to_timestamp_seconds("EventTime"))`, not `DATE_TRUNC('minute', M)`),
    /// so the suite.toml override is what makes it resolve.
    #[test]
    fn q43_override_is_applied() {
        let suite = crate::clickbench_parquet::suite::load_default().expect("suite loads");
        let statement = suite
            .statements
            .iter()
            .find(|s| s.number == 43)
            .expect("statement 43 present");
        let over = suite.override_for(43).expect("Q43 override present");

        let without_override = resolve_tie_spec(43, &statement.sql, None);
        assert!(
            matches!(without_override, Err(ComparatorError::UnresolvedOrderKey { .. })),
            "Q43 must not resolve without the override: {without_override:?}"
        );

        let with_override = resolve_tie_spec(43, &statement.sql, Some(&over.order_key))
            .expect("Q43 resolves with its override");
        assert_eq!(with_override.key, vec![0]);
        assert_eq!(with_override.limit, Some(10));
        assert_eq!(with_override.offset, 1000);
    }

    /// Required test: an unresolvable ORDER BY key without an override is
    /// refused, not silently given some default key.
    #[test]
    fn unresolvable_order_key_without_override_is_refused() {
        let sql = r#"SELECT "a", "b" FROM t ORDER BY "c" LIMIT 5"#;
        let err = resolve_tie_spec(999, sql, None).expect_err("c is neither selected nor aliased");
        assert!(matches!(err, ComparatorError::UnresolvedOrderKey { .. }));
    }

    /// Required test, and distinguishing test for wrong implementation (c):
    /// Int16 vs Int64 passes (integer width is erased by normalization),
    /// but Utf8 vs Binary fails (treating them as the same kind would make
    /// `binary_as_string` silently unobservable).
    #[test]
    fn int_width_agnostic_but_str_vs_bytes_distinct() {
        let reference = vec![vec![Cell::Int(5)]];
        let subject = vec![vec![Cell::Int(5)]];
        let report = compare(&reference, &subject, &tie(vec![], None, 0)).expect("compare");
        assert_eq!(report.verdict, Verdict::Pass);

        let reference = vec![vec![Cell::Str("x".to_string())]];
        let subject = vec![vec![Cell::Bytes(b"x".to_vec())]];
        let report = compare(&reference, &subject, &tie(vec![], None, 0)).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail, "Utf8 and Binary must not compare equal");
    }

    /// Required test: a JSON reference row parses to the same cells as the
    /// equivalent Arrow batch, across every kind a JSON number/string can be
    /// typed as.
    #[test]
    fn json_reference_matches_equivalent_batch() {
        use datafusion::arrow::array::{
            BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array,
            TimestampSecondArray,
        };
        use datafusion::arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("i", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("y", DataType::Binary, false),
            Field::new("d", DataType::Date32, false),
            Field::new("t", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![1.5])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["hi"])),
                Arc::new(BinaryArray::from(vec![b"hi".as_slice()])),
                Arc::new(Date32Array::from(vec![days_from_civil(2013, 7, 15) as i32])),
                Arc::new(TimestampSecondArray::from(vec![
                    days_from_civil(2013, 7, 15) * 86_400 + 3723,
                ])),
            ],
        )
        .expect("build batch");

        let arrow_rows = rows_from_arrow(std::slice::from_ref(&batch)).expect("normalize arrow");
        let kinds = [
            ColumnKind::Bool,
            ColumnKind::Int,
            ColumnKind::Float,
            ColumnKind::Str,
            ColumnKind::Bytes,
            ColumnKind::Date,
            ColumnKind::Ts,
        ];
        let json_rows: Vec<serde_json::Value> = vec![serde_json::json!([
            true, 42, 1.5, "hi", "hi", "2013-07-15", "2013-07-15T01:02:03Z"
        ])];
        let json_normalized = rows_from_json(&json_rows, &kinds).expect("normalize json");
        assert_eq!(arrow_rows, json_normalized);
    }
}
