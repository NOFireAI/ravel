//! Result comparison for the ClickBench Parquet lane (ADR-2040 section D7):
//! normalize reference and subject rows to a type-width-agnostic [`Cell`],
//! then compare them as a multiset with boundary-key tie reduction at an
//! ORDER BY + LIMIT cut, since two conformant engines may pick different
//! rows among a tie straddling that cut.
//!
//! Floats compare bit-exact ([`Cell::Float`] holds the `f64` bit pattern, so
//! bit-identical NaNs compare equal and differing NaN payloads do not). A
//! float difference inside an otherwise-matching row is recorded as a
//! [`FloatMismatch`] rather than a [`RowMismatch`]. By default any such
//! mismatch fails the comparison; a statement may declare a
//! [`FloatTolerance`] (`suite.toml`'s `float_reason`/`float_max_ulps`), and a
//! mismatch within that many ULPs (same sign, both finite) is "Explained"
//! instead: listed, but not fatal. The one expected source is Ravel's
//! sequential-fold `avg` (ADR-0022), though no statement declares a
//! tolerance yet.

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
    #[error(
        "order_key_columns names column {column:?}, which is not present in the {side} output \
         schema"
    )]
    OrderKeyColumnNotFound { column: String, side: &'static str },
    #[error(
        "order_key_columns column {column:?} resolves to position {subject_index} in the \
         subject output but position {reference_index} in the reference output"
    )]
    OrderKeyColumnPositionMismatch {
        column: String,
        subject_index: usize,
        reference_index: usize,
    },
    #[error("ORDER BY key column index {index} is out of range for a {width}-column result")]
    OrderKeyIndexOutOfRange { index: usize, width: usize },
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
    /// Set when `suite.toml` declares `compare = "cardinality"` for this
    /// statement: `compare` skips key resolution and boundary-tie reduction
    /// entirely and asserts only row and column counts, carrying this
    /// reason into [`Verdict::CardinalityOnly`]. `None` leaves the ordinary
    /// D7 rules (including the LIMIT-without-ORDER-BY cardinality-only
    /// case) to decide the verdict from `key`/`limit` instead.
    pub cardinality_reason: Option<String>,
}

/// Final judgement a comparison reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every row (after boundary-tie reduction) matched.
    Pass,
    /// Row identity could not be asserted, so only row and column counts
    /// were checked: either the statement has a LIMIT but no resolvable
    /// ORDER BY key (`None`), or `suite.toml` declares `compare =
    /// "cardinality"` for it, carrying the declared reason (`Some`).
    CardinalityOnly(Option<String>),
    /// A row mismatch survived boundary-tie reduction and float tolerance.
    Fail,
}

/// A float cell that differed between a matched reference/subject row pair.
/// Fatal (fails the comparison) unless `explanation` is set, which happens
/// only when the statement declared a [`FloatTolerance`] and this mismatch
/// falls within it.
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
    /// The declared `float_reason` when this mismatch is within the
    /// statement's declared `float_max_ulps` (both values finite, same
    /// sign); `None` when the mismatch is fatal, either because no
    /// tolerance was declared for this statement or because the mismatch
    /// falls outside it.
    pub explanation: Option<String>,
}

/// A statement's declared float tolerance (`suite.toml`'s
/// `float_reason`/`float_max_ulps`): a mismatch within `max_ulps` of the
/// ordered bit representation, same sign, both values finite, is listed as
/// "Explained" (with `reason`) instead of failing the comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloatTolerance {
    pub reason: String,
    pub max_ulps: u32,
}

const SIGN_MASK: u64 = 0x8000_0000_0000_0000;

/// Map an `f64` bit pattern to a monotonically ordered `i64`: for
/// non-negative values the bits already sort the same as the value: for
/// negative values (sign bit set), flipping every bit restores the
/// sort-matches-value property. Standard IEEE-754 total-ordering trick.
fn ordered_bits(bits: u64) -> i64 {
    if bits & SIGN_MASK == 0 {
        bits as i64
    } else {
        !bits as i64
    }
}

/// Whether a reference/subject float bit-pattern pair is within
/// `tolerance`: both finite, same sign, and no more than `max_ulps` apart in
/// ordered bit representation. Returns the declared reason when so, `None`
/// otherwise (including when no tolerance was declared at all).
fn float_explanation(
    reference_bits: u64,
    subject_bits: u64,
    tolerance: Option<&FloatTolerance>,
) -> Option<String> {
    let tolerance = tolerance?;
    if !f64::from_bits(reference_bits).is_finite() || !f64::from_bits(subject_bits).is_finite() {
        return None;
    }
    if (reference_bits & SIGN_MASK) != (subject_bits & SIGN_MASK) {
        return None;
    }
    let distance = ordered_bits(reference_bits).abs_diff(ordered_bits(subject_bits));
    (distance <= tolerance.max_ulps as u64).then(|| tolerance.reason.clone())
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
    /// Float cells actually paired and inspected, counted on both sides of
    /// each pair (a boundary-cut row's float cells are inspected too: see
    /// [`FloatMismatch`]).
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
        Date64 => {
            // Date64 is milliseconds since the epoch; narrowed to whole days
            // by floor division (`div_euclid`, not `/`, so a negative
            // millisecond value narrows toward the earlier day rather than
            // toward zero).
            let ms = array.as_primitive::<Date64Type>().value(row);
            let days = ms.div_euclid(86_400_000);
            let days32 = i32::try_from(days).map_err(|_| ComparatorError::InvalidDate {
                index,
                value: ms.to_string(),
                reason: "day count out of range for a 32-bit day index".to_string(),
            })?;
            Cell::Date(days32)
        }
        Timestamp(unit, _tz) => {
            let (raw, factor): (i64, i64) = match unit {
                TimeUnit::Second => (
                    array.as_primitive::<TimestampSecondType>().value(row),
                    1_000_000_000,
                ),
                TimeUnit::Millisecond => (
                    array.as_primitive::<TimestampMillisecondType>().value(row),
                    1_000_000,
                ),
                TimeUnit::Microsecond => (
                    array.as_primitive::<TimestampMicrosecondType>().value(row),
                    1_000,
                ),
                TimeUnit::Nanosecond => {
                    (array.as_primitive::<TimestampNanosecondType>().value(row), 1)
                }
            };
            let ns = raw
                .checked_mul(factor)
                .ok_or_else(|| ComparatorError::InvalidTimestamp {
                    index,
                    value: raw.to_string(),
                    reason: "scaling to nanoseconds overflowed i64".to_string(),
                })?;
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

/// The [`ColumnKind`] of every column in `batch`'s schema, in order. Used to
/// type a JSON reference's cells against the subject batch they are compared
/// to (see [`rows_from_json`]).
pub fn schema_kinds(batch: &RecordBatch) -> Result<Vec<ColumnKind>, ComparatorError> {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| column_kind(field.data_type(), index))
        .collect()
}

/// Howard Hinnant's `days_from_civil`: days since the Unix epoch for a
/// proleptic-Gregorian calendar date. No dependency on a calendar crate
/// (`chrono` is not a workspace dependency); this is the standard
/// constant-time algorithm. Every intermediate step is checked: a year far
/// enough from the epoch makes the era or day-of-era multiplication overflow
/// `i64`, and that is reported as `None` rather than wrapping, so the caller
/// can turn it into a typed error instead of returning a wrong day count.
fn days_from_civil_checked(y: i64, m: i64, d: i64) -> Option<i64> {
    let y_adj = if m <= 2 { y.checked_sub(1)? } else { y };
    let era_base = if y_adj >= 0 {
        y_adj
    } else {
        y_adj.checked_sub(399)?
    };
    let era = era_base / 400;
    let yoe = y_adj.checked_sub(era.checked_mul(400)?)?;
    let mp = (m + 9) % 12;
    let doy = (153i64.checked_mul(mp)?.checked_add(2)?) / 5 + d - 1;
    let doe = yoe
        .checked_mul(365)?
        .checked_add(yoe / 4)?
        .checked_sub(yoe / 100)?
        .checked_add(doy)?;
    era.checked_mul(146097)?.checked_add(doe)?.checked_sub(719468)
}

const DAYS_IN_MONTH: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
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
    if !(1..=12).contains(&m) {
        return Err(fail("month out of range 1..=12"));
    }
    let max_day = if m == 2 && is_leap_year(y) {
        29
    } else {
        DAYS_IN_MONTH[(m - 1) as usize]
    };
    if d < 1 || d > max_day {
        return Err(fail("day out of range for its month"));
    }
    let days =
        days_from_civil_checked(y, m, d).ok_or_else(|| fail("date arithmetic overflowed i64"))?;
    i32::try_from(days).map_err(|_| fail("day count out of range for a 32-bit day index"))
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
        if offset == "+00:00" || offset == "-00:00" || offset == "+0000" || offset == "-0000" {
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
            // Validate every byte is an ASCII digit before slicing: frac may
            // legitimately carry more than 9 digits (truncated to ns below),
            // but a non-ASCII byte in it must never be sliced through (that
            // can land mid-character and panic), and trailing non-digit
            // junk must be refused rather than silently dropped by the
            // truncation.
            if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return Err(fail("fraction must be one or more ASCII digits"));
            }
            let digits = &frac[..frac.len().min(9)];
            let padded = format!("{digits:0<9}");
            (
                hms,
                padded.parse::<i64>().map_err(|_| fail("bad fraction"))?,
            )
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
    if !(0..=23).contains(&h) {
        return Err(fail("hour out of range 0..=23"));
    }
    if !(0..=59).contains(&mi) {
        return Err(fail("minute out of range 0..=59"));
    }
    if !(0..=59).contains(&s) {
        return Err(fail("second out of range 0..=59"));
    }
    let day_ns = (days as i64)
        .checked_mul(86_400_000_000_000)
        .ok_or_else(|| fail("date too far from the epoch to represent in nanoseconds"))?;
    let time_ns = (h * 3600 + mi * 60 + s)
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nanos))
        .ok_or_else(|| fail("time-of-day arithmetic overflowed i64"))?;
    day_ns
        .checked_add(time_ns)
        .ok_or_else(|| fail("timestamp arithmetic overflowed i64"))
}

/// Parse decimal text (`[-]digits[.digits]`) directly into an unscaled
/// `i128` at `scale`, never through `f64`: an `f64` round-trip loses
/// precision past about 17 significant digits, which a decimal reference
/// value can exceed. The text's fractional part must fit within `scale`
/// digits (padded with trailing zeros if shorter); more than `scale`
/// fractional digits is a precision loss this comparator refuses rather
/// than silently rounding.
fn parse_decimal_text(index: usize, text: &str, scale: i8) -> Result<i128, ComparatorError> {
    let fail = || ComparatorError::InvalidJsonCell {
        index,
        value: text.to_string(),
        kind: ColumnKind::Decimal(scale),
    };
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((i, f)) => (i, f),
        None => (unsigned, ""),
    };
    if int_part.is_empty() || !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(fail());
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(fail());
    }
    let scale_digits = scale as usize;
    if frac_part.len() > scale_digits {
        return Err(fail());
    }
    let mut digits = String::with_capacity(int_part.len() + scale_digits);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in frac_part.len()..scale_digits {
        digits.push('0');
    }
    let magnitude: i128 = digits.parse().map_err(|_| fail())?;
    Ok(if negative { -magnitude } else { magnitude })
}

/// Read one JSON reference cell, typed per `kind` (the corresponding
/// subject column's normalized kind): a JSON number becomes `Float` when
/// `kind` is `Float`, else `Int`; a `YYYY-MM-DD` string becomes `Date`; an
/// RFC 3339 string becomes `Ts`. A JSON string is never read as `Bytes`:
/// JSON has no byte-string literal, so a `Binary` subject column paired
/// with a JSON string reference is refused rather than silently encoding
/// the string's UTF-8 bytes, which is how a `binary_as_string` option that
/// did not fire on the subject side would otherwise stay invisible.
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
        // A JSON reference has no way to express raw bytes: a JSON string is
        // always text, never the Binary subject column's byte string. See
        // the function doc comment for why this must fail rather than
        // coerce.
        ColumnKind::Bytes => Err(invalid()),
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
        ColumnKind::Decimal(scale) => value
            .as_str()
            .ok_or_else(invalid)
            .and_then(|s| parse_decimal_text(index, s, scale))
            .map(|unscaled| Cell::Decimal(unscaled, scale)),
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
            let cells = row
                .as_array()
                .ok_or_else(|| ComparatorError::InvalidJsonCell {
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
            if let SelectItem::ExprWithAlias { alias, .. } = item
                && alias.value.eq_ignore_ascii_case(order_name)
            {
                return Some(i);
            }
        }
        for (i, item) in projection.iter().enumerate() {
            if let SelectItem::UnnamedExpr(pexpr) = item
                && identifier_tail(pexpr) == Some(order_name)
            {
                return Some(i);
            }
        }
    }
    let order_text = expr.to_string();
    for (i, item) in projection.iter().enumerate() {
        if let SelectItem::UnnamedExpr(pexpr) = item
            && pexpr.to_string() == order_text
        {
            return Some(i);
        }
    }
    None
}

fn parse_u64_expr(expr: &Expr) -> Result<u64, ComparatorError> {
    expr.to_string()
        .parse::<u64>()
        .map_err(|_| ComparatorError::SqlParse(format!("not a plain integer literal: {expr}")))
}

fn parse_limit_clause(
    limit_clause: Option<&LimitClause>,
) -> Result<(Option<u64>, u64), ComparatorError> {
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
///
/// `cardinality_reason`, when `Some`, is `suite.toml`'s declared `compare =
/// "cardinality"` reason for this statement: ORDER BY key resolution is
/// skipped entirely (there is no row identity to resolve a key for) and the
/// returned `TieSpec` carries the reason in
/// [`TieSpec::cardinality_reason`], with an empty `key`. LIMIT/OFFSET are
/// still parsed, since a cardinality-only statement's row count still
/// matters to [`compare`].
pub fn resolve_tie_spec(
    statement_number: u32,
    sql: &str,
    override_key: Option<&[usize]>,
    cardinality_reason: Option<&str>,
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
    if let Some(reason) = cardinality_reason {
        return Ok(TieSpec {
            key: Vec::new(),
            limit,
            offset,
            cardinality_reason: Some(reason.to_string()),
        });
    }
    let Some(order_by) = &query.order_by else {
        return Ok(TieSpec {
            key: Vec::new(),
            limit,
            offset,
            cardinality_reason: None,
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
        return Ok(TieSpec {
            key,
            limit,
            offset,
            cardinality_reason: None,
        });
    }
    match override_key {
        Some(k) => Ok(TieSpec {
            key: k.to_vec(),
            limit,
            offset,
            cardinality_reason: None,
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

/// Resolve `names` (`suite.toml`'s `order_key_columns`) to projection
/// indices by matching each name against both `subject_columns` and
/// `reference_columns` (the engines' actual output column names, in
/// result-column order). Used when a statement's ORDER BY key cannot be
/// found in the SQL text itself (e.g. Q24's `SELECT *`, where the
/// projection is a wildcard rather than a list of named expressions), so
/// the comparator resolves the name against each side's real result schema
/// instead. Both sides must carry the name, and it must land at the same
/// position on both: a name present only on one side, or that resolves to
/// different positions on each, is a typed error rather than a silently
/// wrong key.
pub fn resolve_order_key_columns(
    names: &[String],
    subject_columns: &[String],
    reference_columns: &[String],
) -> Result<Vec<usize>, ComparatorError> {
    names
        .iter()
        .map(|name| {
            let subject_index =
                subject_columns
                    .iter()
                    .position(|c| c == name)
                    .ok_or_else(|| ComparatorError::OrderKeyColumnNotFound {
                        column: name.clone(),
                        side: "subject",
                    })?;
            let reference_index = reference_columns
                .iter()
                .position(|c| c == name)
                .ok_or_else(|| ComparatorError::OrderKeyColumnNotFound {
                    column: name.clone(),
                    side: "reference",
                })?;
            if subject_index != reference_index {
                return Err(ComparatorError::OrderKeyColumnPositionMismatch {
                    column: name.clone(),
                    subject_index,
                    reference_index,
                });
            }
            Ok(subject_index)
        })
        .collect()
}

fn project(row: &[Cell], key: &[usize]) -> Vec<Cell> {
    key.iter().map(|&i| row[i].clone()).collect()
}

/// Whether two key tuples sit at the same cut boundary, ignoring float bit
/// differences: a float cell inside an ORDER BY key must still be
/// recognized as the cut (its value is reported as a [`FloatMismatch`] by
/// [`compare_multiset`], never silently misrouted into an exact-equality
/// interior-row Fail because its bits happened to differ).
fn keys_match_structurally(a: &[Cell], b: &[Cell]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| matches!((x, y), (Cell::Float(_), Cell::Float(_))) || x == y)
}

/// Split a row into its non-float "shape" (every non-float cell, with each
/// float cell position replaced by `None`) and the float bit patterns it
/// held, in column order.
fn split_row(row: &[Cell]) -> (Vec<Option<Cell>>, Vec<u64>) {
    let mut shape = Vec::with_capacity(row.len());
    let mut floats = Vec::new();
    for cell in row {
        match cell {
            Cell::Float(bits) => {
                shape.push(None);
                floats.push(*bits);
            }
            other => shape.push(Some(other.clone())),
        }
    }
    (shape, floats)
}

/// Rows sharing one non-float shape: every row in `ref_rows`/`subj_rows`
/// differs from every other only in its float cells, if any. Each entry
/// keeps the float bit-pattern tuple (the sort/pairing key) alongside the
/// original full row, so a surplus or mismatched row can be reported
/// without reconstructing it.
struct ShapeGroup {
    /// Original column index each `None` slot in the group's shape stands
    /// for, in the same order as each float-bit-pattern tuple below.
    float_columns: Vec<usize>,
    ref_rows: Vec<(Vec<u64>, Vec<Cell>)>,
    subj_rows: Vec<(Vec<u64>, Vec<Cell>)>,
}

fn float_columns_of(shape: &[Option<Cell>]) -> Vec<usize> {
    shape
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_none())
        .map(|(i, _)| i)
        .collect()
}

/// Compare two multisets of rows, pairing float-only differences
/// deterministically rather than relying on hash-map iteration order: rows
/// are grouped by their non-float shape, in an order fixed by that shape's
/// `Debug` text (a `BTreeMap` key, never a hash), and within a group each
/// side's float-cell tuples are sorted by raw bit pattern before pairing
/// positionally. A count surplus within a group becomes a missing/extra row
/// for the excess, same as an ordinary multiset difference. With no float
/// cells present this degenerates to exact multiset equality, which is
/// what D1's boundary-tuple comparison and the no-float interior case both
/// need from the same function.
///
/// Returns the row mismatch (uncapped; callers cap to [`MAX_MISMATCH_ROWS`]
/// if the result is surfaced to a report), every float mismatch found
/// (`explanation` set when `tolerance` covers it), and the number of float
/// cells paired and inspected (counted on both sides of each pair).
fn compare_multiset(
    reference: Vec<Vec<Cell>>,
    subject: Vec<Vec<Cell>>,
    tolerance: Option<&FloatTolerance>,
) -> (RowMismatch, Vec<FloatMismatch>, u64) {
    let mut groups: std::collections::BTreeMap<String, ShapeGroup> =
        std::collections::BTreeMap::new();
    for row in reference {
        let (shape, floats) = split_row(&row);
        let key = format!("{shape:?}");
        let group = groups.entry(key).or_insert_with(|| ShapeGroup {
            float_columns: float_columns_of(&shape),
            ref_rows: Vec::new(),
            subj_rows: Vec::new(),
        });
        group.ref_rows.push((floats, row));
    }
    for row in subject {
        let (shape, floats) = split_row(&row);
        let key = format!("{shape:?}");
        let group = groups.entry(key).or_insert_with(|| ShapeGroup {
            float_columns: float_columns_of(&shape),
            ref_rows: Vec::new(),
            subj_rows: Vec::new(),
        });
        group.subj_rows.push((floats, row));
    }

    let mut missing = Vec::new();
    let mut extra = Vec::new();
    let mut float_mismatches = Vec::new();
    let mut float_cells_compared = 0u64;

    for (_, mut group) in groups {
        group.ref_rows.sort_by(|a, b| a.0.cmp(&b.0));
        group.subj_rows.sort_by(|a, b| a.0.cmp(&b.0));
        let paired = group.ref_rows.len().min(group.subj_rows.len());
        for i in 0..paired {
            let (ref_floats, ref_row) = &group.ref_rows[i];
            let (subj_floats, _) = &group.subj_rows[i];
            float_cells_compared += (ref_floats.len() + subj_floats.len()) as u64;
            for (slot, (&rb, &sb)) in ref_floats.iter().zip(subj_floats.iter()).enumerate() {
                if rb != sb {
                    float_mismatches.push(FloatMismatch {
                        column: group.float_columns[slot],
                        row_key: ref_row.clone(),
                        reference_bits: rb,
                        subject_bits: sb,
                        reference_f64: f64::from_bits(rb),
                        subject_f64: f64::from_bits(sb),
                        explanation: float_explanation(rb, sb, tolerance),
                    });
                }
            }
        }
        for (_, row) in &group.ref_rows[paired..] {
            missing.push(row.clone());
        }
        for (_, row) in &group.subj_rows[paired..] {
            extra.push(row.clone());
        }
    }

    (
        RowMismatch { missing, extra },
        float_mismatches,
        float_cells_compared,
    )
}

/// Compare `reference` against `subject` under `tie`, applying D7's
/// boundary-key tie reduction. Both must already be normalized (see
/// [`rows_from_arrow`] / [`rows_from_json`]). `float_tolerance` is the
/// statement's declared [`FloatTolerance`] (`suite.toml`'s
/// `float_reason`/`float_max_ulps`), or `None` when the statement declares
/// none, in which case any float mismatch is fatal.
pub fn compare(
    reference: &[Vec<Cell>],
    subject: &[Vec<Cell>],
    tie: &TieSpec,
    float_tolerance: Option<&FloatTolerance>,
) -> Result<ComparisonReport, ComparatorError> {
    let width = reference.first().or_else(|| subject.first()).map(Vec::len);
    if let Some(width) = width {
        for row in reference.iter().chain(subject.iter()) {
            if row.len() != width {
                return Err(ComparatorError::ColumnCountMismatch {
                    reference: width,
                    subject: row.len(),
                });
            }
        }
        if let Some(&index) = tie.key.iter().find(|&&i| i >= width) {
            return Err(ComparatorError::OrderKeyIndexOutOfRange { index, width });
        }
    }

    // Row counts must agree in every verdict mode (D7 rule 1a), including
    // CardinalityOnly: a row-count mismatch is always a Fail, checked
    // before anything else can mask it.
    if reference.len() != subject.len() {
        return Ok(ComparisonReport {
            verdict: Verdict::Fail,
            float_mismatches: Vec::new(),
            row_mismatch: RowMismatch::default(),
            tie_rows_reduced: 0,
            float_cells_compared: 0,
        });
    }

    // suite.toml declared this statement's row identity unrecoverable from
    // its output (e.g. an ORDER BY column that is not projected): skip key
    // resolution and boundary-tie reduction entirely, carrying the declared
    // reason into the verdict.
    if let Some(reason) = &tie.cardinality_reason {
        return Ok(ComparisonReport {
            verdict: Verdict::CardinalityOnly(Some(reason.clone())),
            float_mismatches: Vec::new(),
            row_mismatch: RowMismatch::default(),
            tie_rows_reduced: 0,
            float_cells_compared: 0,
        });
    }

    // A LIMIT with no resolvable ORDER BY key: row identity is genuinely
    // unconstrained (any N rows may come back), so only cardinality can be
    // asserted (already proven equal above).
    if tie.limit.is_some() && tie.key.is_empty() {
        return Ok(ComparisonReport {
            verdict: Verdict::CardinalityOnly(None),
            float_mismatches: Vec::new(),
            row_mismatch: RowMismatch::default(),
            tie_rows_reduced: 0,
            float_cells_compared: 0,
        });
    }

    // A top cut exists only when OFFSET > 0 (rule 1b): with no OFFSET, the
    // reference's first row is not sitting at an arbitrary point in a tie,
    // it is simply the first row, and must match exactly. A bottom cut
    // exists only when the statement has a LIMIT AND the reference actually
    // returned exactly LIMIT rows: true truncation. A result shorter than
    // its LIMIT was never truncated and gets no reduction at all.
    let top_cut_key = if tie.offset > 0 && !tie.key.is_empty() {
        reference.first().map(|row| project(row, &tie.key))
    } else {
        None
    };
    let bottom_cut_key = if !tie.key.is_empty()
        && tie
            .limit
            .is_some_and(|limit| reference.len() as u64 == limit)
    {
        reference.last().map(|row| project(row, &tie.key))
    } else {
        None
    };

    let is_cut_key = |k: &Vec<Cell>| {
        top_cut_key
            .as_ref()
            .is_some_and(|c| keys_match_structurally(k, c))
            || bottom_cut_key
                .as_ref()
                .is_some_and(|c| keys_match_structurally(k, c))
    };

    // Rows whose key tuple equals a cut's key tuple reduce to the key
    // tuple (rule 1c); all other rows are compared in full. The reduced
    // tuples from both sides go into their own multiset, which must be
    // equal (counted, never silently dropped), not merely tallied.
    let mut tie_rows_reduced = 0u64;
    let mut interior_ref = Vec::new();
    let mut reduced_ref = Vec::new();
    for row in reference {
        let key = project(row, &tie.key);
        if is_cut_key(&key) {
            tie_rows_reduced += 1;
            reduced_ref.push(key);
        } else {
            interior_ref.push(row.clone());
        }
    }
    let mut interior_subj = Vec::new();
    let mut reduced_subj = Vec::new();
    for row in subject {
        let key = project(row, &tie.key);
        if is_cut_key(&key) {
            tie_rows_reduced += 1;
            reduced_subj.push(key);
        } else {
            interior_subj.push(row.clone());
        }
    }

    // The reduced (cut) side is compared only to decide whether it matches
    // (D1 rule 1c): its own missing/extra rows are partial key tuples, not
    // full rows, and are never surfaced in the report's `row_mismatch`. Its
    // float mismatches ARE surfaced (D2c): a float cell inside the ORDER BY
    // key still gets reported, with its column remapped from an index into
    // the key tuple back to the row's real column index.
    let (reduced_result, mut reduced_float_mismatches, reduced_float_cells) =
        compare_multiset(reduced_ref, reduced_subj, float_tolerance);
    for m in &mut reduced_float_mismatches {
        m.column = tie.key[m.column];
    }
    let reduced_has_mismatch =
        !reduced_result.missing.is_empty() || !reduced_result.extra.is_empty();

    let (interior_result, interior_float_mismatches, interior_float_cells) =
        compare_multiset(interior_ref, interior_subj, float_tolerance);

    let mut float_mismatches = reduced_float_mismatches;
    float_mismatches.extend(interior_float_mismatches);
    let float_cells_compared = reduced_float_cells + interior_float_cells;
    let has_unexplained_float_mismatch = float_mismatches.iter().any(|m| m.explanation.is_none());

    let verdict = if !reduced_has_mismatch
        && interior_result.missing.is_empty()
        && interior_result.extra.is_empty()
        && !has_unexplained_float_mismatch
    {
        Verdict::Pass
    } else {
        Verdict::Fail
    };

    Ok(ComparisonReport {
        verdict,
        float_mismatches,
        row_mismatch: RowMismatch {
            missing: interior_result
                .missing
                .into_iter()
                .take(MAX_MISMATCH_ROWS)
                .collect(),
            extra: interior_result
                .extra
                .into_iter()
                .take(MAX_MISMATCH_ROWS)
                .collect(),
        },
        tie_rows_reduced,
        float_cells_compared,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn tie(key: Vec<usize>, limit: Option<u64>, offset: u64) -> TieSpec {
        TieSpec {
            key,
            limit,
            offset,
            cardinality_reason: None,
        }
    }

    /// Required test: tie cut by LIMIT passes when subject picked a
    /// different tied row at the bottom boundary (a GROUP BY ... ORDER BY c
    /// DESC LIMIT N shape, where several groups share the smallest included
    /// count).
    ///
    /// key = column 0, rows ordered descending by it. With OFFSET 0 there
    /// is no top cut (rule 1b), so the top row (key=10) must match exactly
    /// and is kept identical on both sides; the bottom row (key=1) is the
    /// true-truncation bottom cut (reference returned exactly LIMIT rows),
    /// so it may legally differ in content. The interior rows (key=8,
    /// key=5) are neither cut and must still match exactly.
    #[test]
    fn boundary_tie_with_different_rows_passes() {
        let reference = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            // bottom row: different content, but key=1 is the bottom cut.
            vec![Cell::Int(1), Cell::Str("b".into())],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0], Some(4), 0), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Pass);
        // The bottom cut row on both sides: 1 (reference) + 1 (subject).
        assert_eq!(report.tie_rows_reduced, 2);
    }

    /// Required test: fails when subject picked a row outside the tie (its
    /// replacement row's key does not match the bottom cut's key tuple at
    /// all, so it cannot be a legitimate tie-break variation of the bottom
    /// edge). OFFSET is 0, so the top row carries no cut and is kept
    /// identical on both sides to isolate the bottom-edge behavior.
    #[test]
    fn boundary_tie_with_row_outside_tie_fails() {
        let reference = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            // key=9 matches neither cut key tuple (10 is not a cut at
            // OFFSET 0, and 1 is the bottom cut): not a legitimate
            // tie-break variation, an interior row instead.
            vec![Cell::Int(9), Cell::Str("z".into())],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0], Some(4), 0), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
        assert_eq!(report.row_mismatch.missing.len(), 0);
        assert_eq!(report.row_mismatch.extra.len(), 1);
        assert_eq!(report.row_mismatch.extra[0][1], Cell::Str("z".into()));
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
        let report = compare(&reference, &subject, &tie(vec![0], None, 0), None).expect("compare");
        assert_eq!(
            report.verdict,
            Verdict::Fail,
            "an interior content bug away from any LIMIT boundary must still be caught"
        );
        assert_eq!(
            report.tie_rows_reduced, 0,
            "no LIMIT means no boundary rows are ever exempted"
        );
    }

    /// Required test: a float differing in its last bit is listed with
    /// both bit patterns.
    #[test]
    fn float_last_bit_difference_is_listed() {
        let a = 1.0_f64;
        let b = f64::from_bits(a.to_bits() + 1);
        let reference = vec![vec![Cell::Int(1), Cell::Float(a.to_bits())]];
        let subject = vec![vec![Cell::Int(1), Cell::Float(b.to_bits())]];
        let report = compare(&reference, &subject, &tie(vec![], None, 0), None).expect("compare");
        assert_eq!(
            report.verdict,
            Verdict::Fail,
            "an undeclared float mismatch is fatal by default (D2 rule a)"
        );
        assert_eq!(report.float_mismatches.len(), 1);
        let fm = &report.float_mismatches[0];
        assert_eq!(fm.reference_bits, a.to_bits());
        assert_eq!(fm.subject_bits, b.to_bits());
        assert_eq!(fm.explanation, None);
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
        let report = compare(&reference, &subject, &tie(vec![], None, 0), None).expect("compare");
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

        let without_override = resolve_tie_spec(43, &statement.sql, None, None);
        assert!(
            matches!(
                without_override,
                Err(ComparatorError::UnresolvedOrderKey { .. })
            ),
            "Q43 must not resolve without the override: {without_override:?}"
        );

        let order_key = over.order_key.as_deref().expect("Q43 declares order_key");
        let with_override = resolve_tie_spec(43, &statement.sql, Some(order_key), None)
            .expect("Q43 resolves with its override");
        assert_eq!(with_override.key, vec![0]);
        assert_eq!(with_override.limit, Some(10));
        assert_eq!(with_override.offset, 1000);
        assert_eq!(with_override.cardinality_reason, None);
    }

    /// Required test: an unresolvable ORDER BY key without an override is
    /// refused, not silently given some default key.
    #[test]
    fn unresolvable_order_key_without_override_is_refused() {
        let sql = r#"SELECT "a", "b" FROM t ORDER BY "c" LIMIT 5"#;
        let err =
            resolve_tie_spec(999, sql, None, None).expect_err("c is neither selected nor aliased");
        assert!(matches!(err, ComparatorError::UnresolvedOrderKey { .. }));
    }

    /// `resolve_order_key_columns` resolves a name present at the same
    /// position on both sides.
    #[test]
    fn order_key_columns_resolve_by_name() {
        let subject = vec!["a".to_string(), "EventTime".to_string()];
        let reference = subject.clone();
        let resolved = resolve_order_key_columns(&["EventTime".to_string()], &subject, &reference)
            .expect("EventTime is present on both sides at the same position");
        assert_eq!(resolved, vec![1]);
    }

    /// A name absent from one side is a typed error, not a silently wrong
    /// (or default) index.
    #[test]
    fn order_key_columns_missing_name_is_refused() {
        let subject = vec!["a".to_string(), "b".to_string()];
        let reference = subject.clone();
        let err = resolve_order_key_columns(&["EventTime".to_string()], &subject, &reference)
            .expect_err("EventTime is not present on either side");
        assert!(matches!(
            err,
            ComparatorError::OrderKeyColumnNotFound {
                side: "subject",
                ..
            }
        ));
    }

    /// A name present on both sides but at different positions is a typed
    /// error: the comparator cannot tell which position is the real key.
    #[test]
    fn order_key_columns_position_mismatch_is_refused() {
        let subject = vec!["EventTime".to_string(), "a".to_string()];
        let reference = vec!["a".to_string(), "EventTime".to_string()];
        let err = resolve_order_key_columns(&["EventTime".to_string()], &subject, &reference)
            .expect_err("EventTime resolves to different positions on each side");
        assert!(matches!(
            err,
            ComparatorError::OrderKeyColumnPositionMismatch {
                subject_index: 0,
                reference_index: 1,
                ..
            }
        ));
    }

    /// Required test, and distinguishing test for wrong implementation (c):
    /// Int16 vs Int64 passes (integer width is erased by normalization), but
    /// Utf8 vs Binary fails (treating them as the same kind would make
    /// `binary_as_string` silently unobservable). Built from real Arrow
    /// arrays of the two actual widths/types (not hand-built `Cell`
    /// literals), so the assertion exercises `cell_from_array`'s own
    /// width-erasure and Str/Bytes split, not just `Cell`'s `PartialEq`.
    #[test]
    fn int_width_agnostic_but_str_vs_bytes_distinct() {
        use datafusion::arrow::array::{BinaryArray, Int16Array, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let narrow_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int16, false)]));
        let narrow_batch = RecordBatch::try_new(
            narrow_schema,
            vec![Arc::new(Int16Array::from(vec![5])) as Arc<dyn Array>],
        )
        .expect("build Int16 batch");
        let wide_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let wide_batch = RecordBatch::try_new(
            wide_schema,
            vec![Arc::new(Int64Array::from(vec![5])) as Arc<dyn Array>],
        )
        .expect("build Int64 batch");
        let reference = rows_from_arrow(std::slice::from_ref(&narrow_batch)).expect("normalize");
        let subject = rows_from_arrow(std::slice::from_ref(&wide_batch)).expect("normalize");
        assert_eq!(reference, vec![vec![Cell::Int(5)]]);
        assert_eq!(subject, vec![vec![Cell::Int(5)]]);
        let report = compare(&reference, &subject, &tie(vec![], None, 0), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Pass);

        let str_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let str_batch = RecordBatch::try_new(
            str_schema,
            vec![Arc::new(StringArray::from(vec!["x"])) as Arc<dyn Array>],
        )
        .expect("build Utf8 batch");
        let bin_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Binary, false)]));
        let bin_batch = RecordBatch::try_new(
            bin_schema,
            vec![Arc::new(BinaryArray::from(vec![b"x".as_slice()])) as Arc<dyn Array>],
        )
        .expect("build Binary batch");
        let reference = rows_from_arrow(std::slice::from_ref(&str_batch)).expect("normalize");
        let subject = rows_from_arrow(std::slice::from_ref(&bin_batch)).expect("normalize");
        assert_eq!(reference, vec![vec![Cell::Str("x".to_string())]]);
        assert_eq!(subject, vec![vec![Cell::Bytes(b"x".to_vec())]]);
        let report = compare(&reference, &subject, &tie(vec![], None, 0), None).expect("compare");
        assert_eq!(
            report.verdict,
            Verdict::Fail,
            "Utf8 and Binary must not compare equal"
        );
    }

    /// Distinguishing test for deliverable 4a: a `Binary` subject column
    /// paired with a JSON string reference must fail as a typed error, not
    /// silently succeed by encoding the string as bytes. Red against the
    /// pre-fix `json_cell`, which converted any JSON string into
    /// `Cell::Bytes` whenever `kind` was `Bytes`, so a `binary_as_string`
    /// option that never fired on the subject side (leaving it `Binary`)
    /// stayed invisible: both sides would normalize to the identical
    /// `Cell::Bytes`.
    #[test]
    fn json_string_against_binary_subject_is_refused() {
        let err = json_cell(0, &serde_json::json!("hello"), ColumnKind::Bytes)
            .expect_err("a JSON string can never be read as Bytes");
        assert!(matches!(
            err,
            ComparatorError::InvalidJsonCell {
                kind: ColumnKind::Bytes,
                ..
            }
        ));
    }

    /// Distinguishing test for deliverable 4c: a fraction containing a
    /// multi-byte UTF-8 character whose bytes straddle the 9-byte
    /// truncation point must never panic. Red against the pre-fix
    /// `parse_rfc3339_to_ns`, which sliced `frac[..frac.len().min(9)]` by
    /// byte index without checking char boundaries: 8 ASCII digits followed
    /// by the 2-byte character `é` puts a char boundary at byte 8 and
    /// another at byte 10, so slicing at byte 9 lands inside `é` and panics
    /// ("byte index 9 is not a char boundary").
    #[test]
    fn multibyte_fraction_is_typed_error_not_panic() {
        let err = parse_rfc3339_to_ns(0, "2013-07-15T01:02:03.12345678é9")
            .expect_err("a non-ASCII fraction must be refused, not sliced through");
        assert!(matches!(err, ComparatorError::InvalidTimestamp { .. }));
    }

    /// Distinguishing test for deliverable 4c: trailing non-digit junk
    /// beyond the 9th fraction digit was silently dropped by the pre-fix
    /// truncation (`frac[..frac.len().min(9)]` keeps only the first 9
    /// bytes, discarding anything after). Must now be a typed error.
    #[test]
    fn trailing_junk_after_fraction_is_rejected() {
        let err = parse_rfc3339_to_ns(0, "2013-07-15T01:02:03.123456789XYZ")
            .expect_err("trailing non-digit characters after the fraction must be refused");
        assert!(matches!(err, ComparatorError::InvalidTimestamp { .. }));
    }

    /// Required test (4e): year 99999 is a plausible, in-range date and must
    /// produce the correct day count via checked arithmetic, not an error.
    #[test]
    fn year_99999_produces_correct_day_count() {
        let days = parse_date_to_days(0, "99999-01-01").expect("year 99999 is in range");
        assert_eq!(
            days,
            days_from_civil_checked(99999, 1, 1).expect("checked arithmetic fits i64") as i32
        );
    }

    /// Required test (4e): an hour of 99 is not a valid time of day.
    #[test]
    fn hour_99_is_typed_error() {
        let err = parse_rfc3339_to_ns(0, "2013-07-15T99:02:03Z")
            .expect_err("hour 99 is out of range 0..=23");
        assert!(matches!(err, ComparatorError::InvalidTimestamp { .. }));
    }

    /// Distinguishing test for deliverable 4b: an `i64::MAX` year overflows
    /// the day-of-era/era arithmetic in `i64`. Red against the pre-fix
    /// `days_from_civil`, which used plain (wrapping-in-release,
    /// panicking-in-debug) arithmetic and either produced a wrong value or
    /// aborted the process instead of returning a typed error.
    #[test]
    fn i64_max_year_is_typed_error() {
        let err = parse_date_to_days(0, &format!("{}-01-01", i64::MAX))
            .expect_err("i64::MAX year overflows the date arithmetic");
        assert!(matches!(err, ComparatorError::InvalidDate { .. }));
    }

    /// Required test (4e): month 13 is invalid.
    #[test]
    fn month_13_is_typed_error() {
        let err =
            parse_date_to_days(0, "2013-13-01").expect_err("month 13 is out of range 1..=12");
        assert!(matches!(err, ComparatorError::InvalidDate { .. }));
    }

    /// Required test (4e): day 45 is invalid for any month.
    #[test]
    fn day_45_is_typed_error() {
        let err = parse_date_to_days(0, "2013-07-45").expect_err("day 45 is out of range");
        assert!(matches!(err, ComparatorError::InvalidDate { .. }));
    }

    /// Required test (4e): a 10-digit year produces a day count far beyond
    /// what fits in a 32-bit day index (`Cell::Date` is `i32`); the i64
    /// arithmetic itself does not overflow, but the final cast must.
    #[test]
    fn year_9999999999_is_typed_error() {
        let err = parse_date_to_days(0, "9999999999-01-01")
            .expect_err("day count for this year does not fit in i32");
        assert!(matches!(err, ComparatorError::InvalidDate { .. }));
    }

    /// Required test (4e): `TimestampSecond(i64::MAX / 2)` overflows when
    /// scaled to nanoseconds (`* 1_000_000_000`). Red against the pre-fix
    /// `cell_from_array`, which multiplied with a plain `*` and would wrap
    /// (release) or panic (debug) instead of returning a typed error.
    #[test]
    fn timestamp_second_i64_max_half_is_typed_error_not_panic() {
        use datafusion::arrow::array::TimestampSecondArray;
        use datafusion::arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampSecondArray::from(vec![i64::MAX / 2])) as Arc<dyn Array>],
        )
        .expect("build batch");
        let err = rows_from_arrow(std::slice::from_ref(&batch))
            .expect_err("scaling i64::MAX / 2 seconds to nanoseconds overflows i64");
        assert!(matches!(err, ComparatorError::InvalidTimestamp { .. }));
    }

    /// Required test (4e): a bare numeric `+0000` offset (no colon) is
    /// accepted as UTC, same as `+00:00` and `Z`.
    #[test]
    fn plus_zero_zero_zero_zero_offset_accepted() {
        let ns = parse_rfc3339_to_ns(0, "2013-07-15T01:02:03+0000").expect("+0000 is UTC");
        let z_ns = parse_rfc3339_to_ns(0, "2013-07-15T01:02:03Z").expect("Z is UTC");
        assert_eq!(ns, z_ns);
    }

    /// Required test (4e): a non-zero offset is still refused, naming the
    /// offset in the error.
    #[test]
    fn non_zero_offset_is_typed_error_naming_offset() {
        let err = parse_rfc3339_to_ns(0, "2013-07-15T01:02:03+05:00")
            .expect_err("a non-zero offset is not supported");
        match err {
            ComparatorError::InvalidTimestamp { reason, .. } => {
                assert!(
                    reason.contains("+05:00"),
                    "error must name the offset: {reason}"
                );
            }
            other => panic!("expected InvalidTimestamp, got {other:?}"),
        }
    }

    /// Required test (4e)/distinguishing test for deliverable 4d: a decimal
    /// with 20 significant digits, well past `f64`'s ~17-digit precision,
    /// parses exactly. Red against the pre-fix implementation, which
    /// rounded through `f64` and would not reproduce this exact unscaled
    /// value.
    #[test]
    fn large_decimal_parses_exact() {
        let cell = json_cell(
            0,
            &serde_json::json!("12345678901234567890.12"),
            ColumnKind::Decimal(2),
        )
        .expect("exact decimal parse");
        assert_eq!(cell, Cell::Decimal(1234567890123456789012, 2));
    }

    /// Required test (4e): `Date64` of -1 ms narrows to day -1 (floor
    /// division), not day 0 (truncation toward zero). Red against the
    /// pre-fix `cell_from_array`, which used `/` (truncating) instead of
    /// `div_euclid` (flooring).
    #[test]
    fn date64_negative_one_ms_is_day_negative_one() {
        use datafusion::arrow::array::Date64Array;
        use datafusion::arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date64Array::from(vec![-1])) as Arc<dyn Array>],
        )
        .expect("build batch");
        let rows = rows_from_arrow(std::slice::from_ref(&batch)).expect("normalize");
        assert_eq!(rows, vec![vec![Cell::Date(-1)]]);
    }

    /// Required test (D7 rule 1a): a subject missing the last row of a
    /// LIMIT result fails. The missing row's key would sit at the bottom
    /// cut, but a row-count mismatch must be caught regardless of where the
    /// missing row's key falls.
    #[test]
    fn missing_last_limit_row_fails() {
        let reference = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0], Some(4), 0), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
    }

    /// Required test (D7 rule 1a): a subject returning LIMIT+2 rows whose
    /// extras tie on the last key still fails, since row counts must be
    /// equal regardless of tie reduction.
    #[test]
    fn extra_rows_tied_on_last_key_fails() {
        let reference = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
            vec![Cell::Int(1), Cell::Str("extra1".into())],
            vec![Cell::Int(1), Cell::Str("extra2".into())],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0], Some(4), 0), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
    }

    /// Required test (D7 rule 1b): with OFFSET 0, there is no top cut, so
    /// wrong content in the top row fails even though it shares its key
    /// with the reference's first row.
    #[test]
    fn offset_zero_wrong_top_row_content_fails() {
        let reference = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            // Same key (10) as reference's top row, but wrong content.
            // With OFFSET 0 there is no top cut (rule 1b), so this row must
            // be compared exactly, not exempted.
            vec![Cell::Int(10), Cell::Str("WRONG".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("b".into())],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0], Some(4), 0), None).expect("compare");
        assert_eq!(
            report.verdict,
            Verdict::Fail,
            "OFFSET 0 means no top cut; the top row's wrong content must be caught"
        );
    }

    /// Required test (D7 rule 1b): a result shorter than its LIMIT was
    /// never truncated, so it gets no bottom-cut reduction at all; a wrong
    /// last row must still be caught.
    #[test]
    fn short_of_limit_gets_no_reduction() {
        let reference = vec![
            vec![Cell::Int(3), Cell::Str("n".into())],
            vec![Cell::Int(2), Cell::Str("p".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            vec![Cell::Int(3), Cell::Str("n".into())],
            vec![Cell::Int(2), Cell::Str("p".into())],
            // Wrong content on what would be the "last row"; since the
            // reference returned only 3 rows against a LIMIT of 10, there
            // was no true truncation and this row is not a bottom cut.
            vec![Cell::Int(1), Cell::Str("WRONG".into())],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0], Some(10), 0), None).expect("compare");
        assert_eq!(
            report.verdict,
            Verdict::Fail,
            "a result shorter than its LIMIT was not truncated; no row is exempt"
        );
        assert_eq!(report.tie_rows_reduced, 0);
    }

    /// Required test (D7 rule 1b/1c): with OFFSET > 0, different rows tied
    /// on the first key pass, since the top cut is exempt from exact
    /// comparison.
    #[test]
    fn offset_positive_top_tie_with_different_rows_passes() {
        let reference = vec![
            vec![Cell::Int(10), Cell::Str("u".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            vec![Cell::Int(1), Cell::Str("a".into())],
        ];
        let subject = vec![
            // Different content, but key=10 is the top cut key (OFFSET>0).
            vec![Cell::Int(10), Cell::Str("different".into())],
            vec![Cell::Int(8), Cell::Str("v".into())],
            vec![Cell::Int(5), Cell::Str("w".into())],
            // Different content, but key=1 is the bottom cut key (true
            // truncation: reference.len() == limit).
            vec![Cell::Int(1), Cell::Str("also-different".into())],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0], Some(4), 2), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Pass);
        assert_eq!(report.tie_rows_reduced, 4);
    }

    /// Required test (D7 rule 1a): `CardinalityOnly` (a LIMIT with no
    /// resolvable ORDER BY key) still fails on a row-count mismatch rather
    /// than ignoring it.
    #[test]
    fn cardinality_only_with_different_row_counts_fails() {
        let reference = vec![vec![Cell::Int(1)], vec![Cell::Int(2)], vec![Cell::Int(3)]];
        let subject = vec![vec![Cell::Int(1)], vec![Cell::Int(2)]];
        let report =
            compare(&reference, &subject, &tie(vec![], Some(3), 0), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
    }

    /// Required (D3c): an out-of-range key index (from any override source:
    /// a numeric `order_key`, a resolved `order_key_columns`, or a bad
    /// textual resolution) is a typed error, never a panic through
    /// `project`'s `row[i]` indexing.
    #[test]
    fn out_of_range_order_key_index_is_typed_error_not_panic() {
        let reference = vec![vec![Cell::Int(1), Cell::Int(2)]];
        let subject = vec![vec![Cell::Int(1), Cell::Int(2)]];
        let err = compare(&reference, &subject, &tie(vec![5], Some(1), 0), None)
            .expect_err("index 5 is out of range for a 2-column result");
        assert_eq!(
            err,
            ComparatorError::OrderKeyIndexOutOfRange { index: 5, width: 2 }
        );
    }

    /// Required test: a JSON reference row parses to the same cells as the
    /// equivalent Arrow batch, across every kind a JSON number/string can be
    /// typed as.
    #[test]
    fn json_reference_matches_equivalent_batch() {
        use datafusion::arrow::array::{
            BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, TimestampSecondArray,
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
                Arc::new(Date32Array::from(vec![
                    days_from_civil_checked(2013, 7, 15).expect("valid date") as i32,
                ])),
                Arc::new(TimestampSecondArray::from(vec![
                    days_from_civil_checked(2013, 7, 15).expect("valid date") * 86_400 + 3723,
                ])),
            ],
        )
        .expect("build batch");

        let arrow_rows = rows_from_arrow(std::slice::from_ref(&batch)).expect("normalize arrow");
        let kinds = schema_kinds(&batch).expect("derive kinds from schema");
        assert_eq!(
            kinds,
            vec![
                ColumnKind::Bool,
                ColumnKind::Int,
                ColumnKind::Float,
                ColumnKind::Str,
                ColumnKind::Bytes,
                ColumnKind::Date,
                ColumnKind::Ts,
            ]
        );
        let json_rows: Vec<serde_json::Value> = vec![serde_json::json!([
            true,
            42,
            1.5,
            "hi",
            "hi",
            "2013-07-15",
            "2013-07-15T01:02:03Z"
        ])];
        let json_normalized = rows_from_json(&json_rows, &kinds).expect("normalize json");
        assert_eq!(arrow_rows, json_normalized);
    }

    /// Required test, distinguishing: an AVG-shaped float difference this
    /// large (5.0 vs 500.0, the sequential-fold-vs-tree-fold shape ADR-0022
    /// warns about) fails with no tolerance declared. Red against any
    /// implementation that treats a float mismatch as informational-only by
    /// default; green here because `compare` defaults to `Fail` unless an
    /// explicit `FloatTolerance` is passed and covers the mismatch.
    #[test]
    fn large_float_mismatch_fails_without_tolerance() {
        let reference = vec![vec![Cell::Int(1), Cell::Float(5.0_f64.to_bits())]];
        let subject = vec![vec![Cell::Int(1), Cell::Float(500.0_f64.to_bits())]];
        let report = compare(&reference, &subject, &tie(vec![], None, 0), None).expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
        assert_eq!(report.float_mismatches.len(), 1);
        assert_eq!(report.float_mismatches[0].explanation, None);
    }

    /// Required test: a 1-ulp mismatch on a statement that declares
    /// `float_max_ulps = 1` is "Explained" (listed, not fatal).
    #[test]
    fn one_ulp_mismatch_explained_when_declared() {
        let a = 1.0_f64;
        let b = f64::from_bits(a.to_bits() + 1);
        let reference = vec![vec![Cell::Float(a.to_bits())]];
        let subject = vec![vec![Cell::Float(b.to_bits())]];
        let tolerance = FloatTolerance {
            reason: "sequential-fold avg rounding (ADR-0022)".to_string(),
            max_ulps: 1,
        };
        let report = compare(
            &reference,
            &subject,
            &tie(vec![], None, 0),
            Some(&tolerance),
        )
        .expect("compare");
        assert_eq!(report.verdict, Verdict::Pass);
        assert_eq!(report.float_mismatches.len(), 1);
        assert_eq!(
            report.float_mismatches[0].explanation.as_deref(),
            Some("sequential-fold avg rounding (ADR-0022)")
        );
    }

    /// Required test: a 2-ulp mismatch against a declared `float_max_ulps =
    /// 1` still fails; the bound is exclusive-not-elastic.
    #[test]
    fn two_ulp_mismatch_fails_declared_one_ulp_tolerance() {
        let a = 1.0_f64;
        let b = f64::from_bits(a.to_bits() + 2);
        let reference = vec![vec![Cell::Float(a.to_bits())]];
        let subject = vec![vec![Cell::Float(b.to_bits())]];
        let tolerance = FloatTolerance {
            reason: "test".to_string(),
            max_ulps: 1,
        };
        let report = compare(
            &reference,
            &subject,
            &tie(vec![], None, 0),
            Some(&tolerance),
        )
        .expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
        assert_eq!(report.float_mismatches[0].explanation, None);
    }

    /// Required test: 0.0 vs -0.0 is listed as a float mismatch (different
    /// bit patterns) and is never "Explained", regardless of a declared
    /// tolerance, since the two sides have different signs.
    #[test]
    fn positive_and_negative_zero_is_listed_and_never_explained() {
        let reference = vec![vec![Cell::Float(0.0_f64.to_bits())]];
        let subject = vec![vec![Cell::Float((-0.0_f64).to_bits())]];
        let tolerance = FloatTolerance {
            reason: "test".to_string(),
            max_ulps: u32::MAX,
        };
        let report = compare(
            &reference,
            &subject,
            &tie(vec![], None, 0),
            Some(&tolerance),
        )
        .expect("compare");
        assert_eq!(report.verdict, Verdict::Fail);
        assert_eq!(report.float_mismatches.len(), 1);
        assert_eq!(report.float_mismatches[0].explanation, None);
    }

    /// Required test: deterministic pairing. Two rows share a non-float
    /// shape but carry different float bit patterns on each side; which
    /// reference float pairs with which subject float must not depend on
    /// hash-map iteration order. Run the comparison 20 times and require
    /// the exact same report every time.
    #[test]
    fn float_pairing_is_deterministic_across_runs() {
        let reference = vec![
            vec![Cell::Int(1), Cell::Float(3.0_f64.to_bits())],
            vec![Cell::Int(1), Cell::Float(1.0_f64.to_bits())],
            vec![Cell::Int(1), Cell::Float(2.0_f64.to_bits())],
        ];
        let subject = vec![
            vec![Cell::Int(1), Cell::Float(1.0_f64.to_bits())],
            vec![Cell::Int(1), Cell::Float(2.5_f64.to_bits())],
            vec![Cell::Int(1), Cell::Float(3.5_f64.to_bits())],
        ];
        let first = compare(&reference, &subject, &tie(vec![], None, 0), None).expect("compare");
        for _ in 0..19 {
            let report =
                compare(&reference, &subject, &tie(vec![], None, 0), None).expect("compare");
            assert_eq!(report, first, "pairing must not vary across runs");
        }
    }

    /// Required test (D2c): a float cell inside a two-column ORDER BY key
    /// (a non-float tie-breaker plus a float, e.g. `ORDER BY GroupId,
    /// AvgValue`) at the bottom cut is paired and reported as a
    /// `FloatMismatch`, with its column remapped back to the row's real
    /// index, rather than silently failing or passing as a plain row
    /// mismatch.
    #[test]
    fn float_in_order_key_bottom_cut_is_listed_as_mismatch() {
        let reference = vec![
            vec![
                Cell::Int(1),
                Cell::Float(10.0_f64.to_bits()),
                Cell::Str("a".into()),
            ],
            vec![
                Cell::Int(2),
                Cell::Float(20.0_f64.to_bits()),
                Cell::Str("b".into()),
            ],
        ];
        let bumped = f64::from_bits(20.0_f64.to_bits() + 1);
        let subject = vec![
            vec![
                Cell::Int(1),
                Cell::Float(10.0_f64.to_bits()),
                Cell::Str("a".into()),
            ],
            vec![
                Cell::Int(2),
                Cell::Float(bumped.to_bits()),
                Cell::Str("b".into()),
            ],
        ];
        let report =
            compare(&reference, &subject, &tie(vec![0, 1], Some(2), 0), None).expect("compare");
        assert_eq!(
            report.verdict,
            Verdict::Fail,
            "no tolerance declared, so the key float mismatch is fatal"
        );
        assert_eq!(report.float_mismatches.len(), 1);
        let fm = &report.float_mismatches[0];
        assert_eq!(
            fm.column, 1,
            "must name the row's real float column, not the key-tuple index"
        );
        assert_eq!(fm.reference_bits, 20.0_f64.to_bits());
        assert_eq!(fm.subject_bits, bumped.to_bits());
    }

    /// D3d: build every one of the frozen corpus's 43 statements' `TieSpec`
    /// the way a real comparison run would (suite.toml's `order_key` /
    /// `order_key_columns` / `compare = "cardinality"` overrides, falling
    /// back to the textual ORDER BY rules when a statement has none), and
    /// classify each by what it can assert: a resolved key (full exact
    /// comparison, D1/D2's boundary-tie rules apply), a LIMIT with no key
    /// and no declared reason (Q18's existing cardinality-only rule), or a
    /// declared cardinality reason (Q25/Q27). Asserts none of the 43
    /// statements still returns `UnresolvedOrderKey`, and that exactly Q18,
    /// Q25, and Q27 classify as cardinality-only.
    ///
    /// Q24's `SELECT *` needs a real output column list to resolve
    /// `order_key_columns` against. `HITS_COLUMNS` is a stand-in for the
    /// engines' actual result schema: no SQL engine is wired into this
    /// crate yet (see `engine.rs`), so there is no live schema to resolve
    /// against. Only `EventTime`'s presence and position matter to this
    /// test; the rest of the list is illustrative, not asserted elsewhere.
    #[test]
    fn every_statement_resolves_and_only_q18_q25_q27_are_cardinality_only() {
        const HITS_COLUMNS: &[&str] = &[
            "WatchID",
            "UserID",
            "URLHash",
            "RefererHash",
            "CounterID",
            "RegionID",
            "ClientIP",
            "AdvEngineID",
            "ResolutionWidth",
            "MobilePhone",
            "SearchEngineID",
            "TraficSourceID",
            "IsRefresh",
            "IsLink",
            "IsDownload",
            "DontCountHits",
            "WindowClientWidth",
            "WindowClientHeight",
            "EventTime",
            "EventDate",
            "URL",
            "Title",
            "Referer",
            "SearchPhrase",
            "MobilePhoneModel",
        ];
        let columns: Vec<String> = HITS_COLUMNS.iter().map(|s| s.to_string()).collect();

        let suite = crate::clickbench_parquet::suite::load_default().expect("suite loads");
        assert_eq!(
            suite.statements.len(),
            crate::clickbench_parquet::suite::STATEMENT_COUNT
        );

        let mut cardinality_only = Vec::new();
        for statement in &suite.statements {
            let over = suite.override_for(statement.number);

            let tie = if let Some(over) = over.filter(|o| o.is_cardinality_only()) {
                resolve_tie_spec(
                    statement.number,
                    &statement.sql,
                    None,
                    over.reason.as_deref(),
                )
            } else if let Some(names) = over.and_then(|o| o.order_key_columns.as_deref()) {
                let resolved = resolve_order_key_columns(names, &columns, &columns)
                    .unwrap_or_else(|e| panic!("statement {}: {e}", statement.number));
                resolve_tie_spec(statement.number, &statement.sql, Some(&resolved), None)
            } else {
                let numeric = over.and_then(|o| o.order_key.as_deref());
                resolve_tie_spec(statement.number, &statement.sql, numeric, None)
            };
            let tie = tie
                .unwrap_or_else(|e| panic!("statement {} did not resolve: {e}", statement.number));

            let is_cardinality_only =
                tie.cardinality_reason.is_some() || (tie.limit.is_some() && tie.key.is_empty());
            if is_cardinality_only {
                cardinality_only.push(statement.number);
            }
        }

        assert_eq!(cardinality_only, vec![18, 25, 27]);
    }
}
