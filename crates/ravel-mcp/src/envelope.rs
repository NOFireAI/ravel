//! The D4 result envelope (ADR-1374).
//!
//! [`Envelope`] is the one shape every MCP tool returns. [`Envelope::fit`]
//! implements the byte-cap algorithm verbatim: drop rows from the end of
//! `data.rows` until the envelope's serialized size is at or under the
//! effective cap, and if a single remaining row still does not fit, keep it
//! and shorten its oversized cells instead of dropping it -- a page always
//! keeps its first row.
//!
//! # Two things the ADR's own example literal settles that a first read of
//! the surrounding prose might not
//!
//! The D4 prose says "Integers and timestamps are JSON strings, because
//! nanosecond epochs exceed 2^53", which read alone could suggest every
//! integer field in the envelope (`data.row_count`, `presentation.max_rows`,
//! and so on) is string-encoded. The ADR's own literal envelope example
//! shows those fields as plain JSON numbers (`"max_rows": 200`,
//! `"row_count": 0`, `"effective_max_response_bytes": 524288`): the rule is
//! about [`Cell::Int`] and [`Cell::Timestamp`] values inside `data.rows` (and
//! any other field that carries a query-produced value that can exceed
//! 2^53), not about the envelope's own bookkeeping counters, which this
//! module keeps as plain `u32`/`u64`/`bool` fields to match that example.
//!
//! `float_to_json` here mirrors `ravel_sql::output`'s private function of the
//! same name and behavior (NaN/+Inf/-Inf as strings, finite floats as JSON
//! numbers with the sign of zero preserved) rather than reusing it: the
//! function is not `pub`, and this crate has no path to it that would not
//! also require exporting it from `ravel-sql`, which is out of this task's
//! file scope.

use std::borrow::Cow;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

/// The smallest `max_response_bytes` the server honors; a smaller request is
/// raised to this floor. Re-exported from [`crate::budget`] so the two
/// modules cannot disagree about the number.
pub use crate::budget::MAX_RESPONSE_BYTES_FLOOR;

/// A projection wider than this many columns is an `invalid_argument`
/// failure (D4).
pub const MAX_PROJECTION_COLUMNS: usize = 256;

const MAX_COLUMNS: usize = 256;
const MAX_PREDICATES_APPLIED: usize = 16;
const MAX_ORDER_BY: usize = 16;
const MAX_MIN_COMMIT_TOKENS: usize = 64;
const MAX_FRAGMENTS: usize = 64;
const MAX_UNINDEXED_PREDICATES: usize = 16;
const MAX_WARNINGS: usize = 16;
const MAX_NEXT_STEPS: usize = 8;
const MAX_EVIDENCE: usize = 16;

/// Byte-serialized cell payload of one table row. Every variant follows the
/// D4 precision rules: [`Cell::Int`] and [`Cell::Timestamp`] serialize as
/// JSON strings (nanosecond epochs exceed 2^53); [`Cell::Float`] follows
/// `float_to_json`; [`Cell::HexId`] is a hex string and, like `Bool`, `Int`,
/// and `Timestamp`, never exceeds 64 B and is never shortened by
/// [`Envelope::fit`]; [`Cell::Map`] serializes as a JSON object when it fits
/// under the per-cell budget, and is re-serialized to truncated JSON text
/// (as a string) only when it does not.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Timestamp(i64),
    Float(f64),
    HexId(String),
    Str(String),
    Map(Map<String, Value>),
}

impl Cell {
    fn to_value(&self) -> Value {
        match self {
            Cell::Null => Value::Null,
            Cell::Bool(b) => Value::Bool(*b),
            Cell::Int(n) => Value::String(n.to_string()),
            Cell::Timestamp(n) => Value::String(n.to_string()),
            Cell::Float(f) => float_to_json(*f),
            Cell::HexId(s) => Value::String(s.clone()),
            Cell::Str(s) => Value::String(s.clone()),
            Cell::Map(m) => Value::Object(m.clone()),
        }
    }
}

/// Mirrors `ravel_sql::output::float_to_json` (private, not reusable from
/// this crate): NaN/+Inf/-Inf as strings, finite floats as JSON numbers,
/// sign of zero preserved.
fn float_to_json(value: f64) -> Value {
    if value.is_nan() {
        Value::String("NaN".to_string())
    } else if value == f64::INFINITY {
        Value::String("+Inf".to_string())
    } else if value == f64::NEG_INFINITY {
        Value::String("-Inf".to_string())
    } else {
        serde_json::json!(value)
    }
}

impl Serialize for Cell {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_value().serialize(serializer)
    }
}

impl JsonSchema for Cell {
    fn schema_name() -> Cow<'static, str> {
        "Cell".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::Cell").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        // A cell's JSON shape depends on its variant (string, number,
        // boolean, object, or null); an unconstrained schema is the honest
        // one rather than a misleading single-type schema.
        json_schema!({})
    }
}

/// A JSON value of unspecified shape, for envelope fields (the `budget`
/// block's `effective`/`actual`/`estimate` sub-objects) whose concrete shape
/// is defined elsewhere (ADR-1374 D6) and not reconstructed here. Also reused
/// by [`crate::catalog`] for tool-input fields whose shape is caller-defined
/// (`ravel_search_logs`'s `predicates`), hence the `Deserialize` derive
/// alongside the manual `Serialize` impl.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct AnyJson(pub Value);

impl Serialize for AnyJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl JsonSchema for AnyJson {
    fn schema_name() -> Cow<'static, str> {
        "AnyJson".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::AnyJson").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({})
    }
}

pub type Row = Vec<Cell>;

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Column {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Data {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub row_count: u64,
}

/// Also reused by [`crate::catalog`] as a tool-input field type, hence the
/// `Deserialize` derive here alongside `Serialize`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct TimeRange {
    pub start_ns: String,
    pub end_ns: String,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Scope {
    pub signal: String,
    pub table: String,
    pub time_range: Option<TimeRange>,
    pub predicates_applied: Vec<String>,
    pub order_by: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Ids {
    pub query_id: String,
    pub audit_ref: String,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Visibility {
    pub snapshot_id: String,
    pub watermark_hour: String,
    pub pinned: bool,
    pub min_commit_tokens_applied: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Coverage {
    pub complete: bool,
    pub partial: bool,
    pub fragments: Vec<String>,
    pub unindexed_predicates: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Accuracy {
    pub exact: bool,
    pub approximation: Option<String>,
    pub lower_bound_count: bool,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Presentation {
    pub max_rows: u32,
    pub row_cap_hit: bool,
    pub bytes_cap_hit: bool,
    pub rows_omitted: u64,
    pub cells_truncated: u64,
    pub metadata_elided: u64,
    pub effective_max_response_bytes: u64,
    pub floor_applied: bool,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Budget {
    pub effective: AnyJson,
    pub actual: AnyJson,
    pub estimate: AnyJson,
    pub estimate_is_upper_envelope: bool,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct EvidenceEntry {
    pub r#ref: String,
    pub covers: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct NextStep {
    pub action: String,
    pub detail: String,
}

/// The four D4 status values. `Ok` is a complete query, including a query
/// with zero matches or an unfilled `LIMIT`. `OkBounded` means the row cap
/// stopped the result and no cursor exists because the statement has no
/// total order. `OkPage` means a cap stopped the result and a cursor exists.
/// `Error` carries a non-null [`Failure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    Ok,
    OkBounded,
    OkPage,
    Error,
}

/// The D4 failure classes, verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Unauthorized,
    InvalidArgument,
    MissingArgument,
    Validation,
    Unsupported,
    BudgetEstimateExceedsCeiling,
    BudgetExceeded,
    Deadline,
    Unavailable,
    SnapshotInvalidated,
    CursorExpired,
    CursorInvalid,
    Internal,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct Failure {
    pub class: FailureClass,
    pub message: String,
    /// The counter that tripped, for `budget_exceeded`; absent otherwise.
    pub counter: Option<String>,
}

/// A projection wider than [`MAX_PROJECTION_COLUMNS`] is an
/// `invalid_argument` failure whose `next_steps` says to name the columns
/// (D4).
pub fn validate_projection(columns: &[Column]) -> Result<(), Failure> {
    if columns.len() > MAX_PROJECTION_COLUMNS {
        Err(Failure {
            class: FailureClass::InvalidArgument,
            message: format!(
                "projection has {} columns, more than the {} column limit",
                columns.len(),
                MAX_PROJECTION_COLUMNS
            ),
            counter: None,
        })
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct Envelope {
    pub status: Status,
    pub failure: Option<Failure>,
    pub data: Data,
    /// The plan text `ravel_explain_query` returns; `null` on every other
    /// tool.
    pub plan: Option<String>,
    pub scope: Scope,
    pub ids: Ids,
    pub visibility: Visibility,
    pub coverage: Coverage,
    pub accuracy: Accuracy,
    pub presentation: Presentation,
    pub budget: Budget,
    pub evidence: Vec<EvidenceEntry>,
    pub warnings: Vec<String>,
    pub next_steps: Vec<NextStep>,
}

fn serialized_len(envelope: &Envelope) -> usize {
    serde_json::to_vec(envelope)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

fn truncate_vec<T>(items: &mut Vec<T>, max: usize) -> u64 {
    if items.len() > max {
        let dropped = (items.len() - max) as u64;
        items.truncate(max);
        dropped
    } else {
        0
    }
}

const TRUNCATION_MARKER: &str = "...[truncated]";

/// Cuts `s` so that its serialized JSON string form (including the
/// surrounding quotes and the trailing marker) is at most `budget` bytes.
/// Assumes `s` needs no JSON escaping in its kept prefix, which holds for
/// the row content this function is applied to (query row cells); a cell
/// containing characters that need escaping would only ever make the kept
/// prefix shorter than this estimate, never longer, so the result still
/// fits under `budget`.
fn truncate_to_budget(s: &str, budget: usize) -> String {
    let quote_overhead = 2usize;
    let available = budget.saturating_sub(quote_overhead);
    let content_budget = available.saturating_sub(TRUNCATION_MARKER.len());
    let mut end = content_budget.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &s[..end], TRUNCATION_MARKER)
}

fn cell_json_len(cell: &Cell) -> usize {
    serde_json::to_vec(&cell.to_value())
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

/// Shortens every oversized cell in `row` to fit `budget_per_cell`, per the
/// D4 rule: a string cell over budget is cut to the budget including the
/// trailing marker; a map cell over budget is serialized to JSON text first,
/// then cut the same way and returned as a string. Numbers, timestamps,
/// booleans, and hex ids never exceed 64 B and are left untouched. Returns
/// the count of cells actually shortened.
fn shorten_row(row: &mut Row, budget_per_cell: usize) -> u64 {
    let mut truncated = 0u64;
    for cell in row.iter_mut() {
        let is_candidate = matches!(cell, Cell::Str(_) | Cell::Map(_));
        if !is_candidate {
            continue;
        }
        if cell_json_len(cell) <= budget_per_cell {
            continue;
        }
        let text = match cell {
            Cell::Str(s) => s.clone(),
            Cell::Map(m) => serde_json::to_string(&Value::Object(m.clone())).unwrap_or_default(),
            _ => unreachable!("is_candidate already restricted to Str and Map"),
        };
        *cell = Cell::Str(truncate_to_budget(&text, budget_per_cell));
        truncated += 1;
    }
    truncated
}

impl Envelope {
    /// Caps every variable-length metadata list to its D4 bound, keeping the
    /// first entries and reporting the number dropped.
    fn cap_metadata_lists(&mut self) -> u64 {
        truncate_vec(&mut self.data.columns, MAX_COLUMNS)
            + truncate_vec(&mut self.scope.predicates_applied, MAX_PREDICATES_APPLIED)
            + truncate_vec(&mut self.scope.order_by, MAX_ORDER_BY)
            + truncate_vec(
                &mut self.visibility.min_commit_tokens_applied,
                MAX_MIN_COMMIT_TOKENS,
            )
            + truncate_vec(&mut self.coverage.fragments, MAX_FRAGMENTS)
            + truncate_vec(
                &mut self.coverage.unindexed_predicates,
                MAX_UNINDEXED_PREDICATES,
            )
            + truncate_vec(&mut self.warnings, MAX_WARNINGS)
            + truncate_vec(&mut self.next_steps, MAX_NEXT_STEPS)
            + truncate_vec(&mut self.evidence, MAX_EVIDENCE)
    }

    /// The D4 byte-cap algorithm. Floors `max_response_bytes` at
    /// [`MAX_RESPONSE_BYTES_FLOOR`], caps every metadata list to its bound,
    /// then drops rows from the end of `data.rows` until the envelope fits.
    /// If a single remaining row still does not fit, keeps it and shortens
    /// its oversized cells instead of dropping it, so `data.rows` is never
    /// empty while `rows_omitted` is positive and a retained row always
    /// fits.
    pub fn fit(mut self, requested_max_response_bytes: u64) -> Envelope {
        self.presentation.metadata_elided = self.cap_metadata_lists();

        let effective_cap = requested_max_response_bytes.max(MAX_RESPONSE_BYTES_FLOOR);
        self.presentation.effective_max_response_bytes = effective_cap;
        self.presentation.floor_applied = requested_max_response_bytes < MAX_RESPONSE_BYTES_FLOOR;
        let cap = effective_cap as usize;

        if serialized_len(&self) <= cap {
            self.presentation.bytes_cap_hit = false;
            self.presentation.rows_omitted = 0;
            self.presentation.cells_truncated = 0;
            return self;
        }
        self.presentation.bytes_cap_hit = true;

        let mut rows_omitted = 0u64;
        while self.data.rows.len() > 1 && serialized_len(&self) > cap {
            self.data.rows.pop();
            rows_omitted += 1;
        }
        self.presentation.rows_omitted = rows_omitted;

        let mut cells_truncated = 0u64;
        if serialized_len(&self) > cap {
            let saved_rows = std::mem::take(&mut self.data.rows);
            let fixed_part = serialized_len(&self) as u64;
            self.data.rows = saved_rows;
            let column_count = self.data.columns.len().max(1) as u64;
            // Reserve a few bytes for the row's own array brackets and the
            // commas between cells, which `fixed_part` (computed with
            // `data.rows` emptied to `[]`) does not itself account for.
            const ROW_STRUCTURE_OVERHEAD: u64 = 8;
            let available = (cap as u64)
                .saturating_sub(fixed_part)
                .saturating_sub(ROW_STRUCTURE_OVERHEAD);
            let budget_per_cell = available.checked_div(column_count).unwrap_or(0).max(256) as usize;
            if let Some(row) = self.data.rows.first_mut() {
                cells_truncated = shorten_row(row, budget_per_cell);
            }
        }
        self.presentation.cells_truncated = cells_truncated;
        self
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn envelope_with_rows(row_count: usize, cell: impl Fn(usize) -> Row) -> Envelope {
        let mut envelope = Envelope::default();
        envelope.data.columns = vec![Column {
            name: "value".to_string(),
            r#type: "string".to_string(),
        }];
        envelope.data.rows = (0..row_count).map(cell).collect();
        envelope.data.row_count = row_count as u64;
        envelope
    }

    /// 10 rows, each over half the floor cap on its own (so even 2 rows
    /// together never fit): the drop-from-the-end loop must go all the way
    /// down to exactly 1 kept row (never 0, per the D4 first-row guarantee)
    /// and report exactly 9 omitted.
    #[test]
    fn byte_cap_drops_rows_from_the_end_and_reports_the_count() {
        let row_text = "x".repeat(150_000);
        let envelope = envelope_with_rows(10, |_| vec![Cell::Str(row_text.clone())]);

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1, "one row must always be kept");
        assert_eq!(fitted.presentation.rows_omitted, 9);
        assert!(fitted.presentation.bytes_cap_hit);
        assert_eq!(fitted.presentation.cells_truncated, 0);
        assert!(serialized_len(&fitted) <= MAX_RESPONSE_BYTES_FLOOR as usize);
    }

    /// A single row with a 1 MiB string cell: dropping is not an option (it
    /// is already the only row), so the row is kept and its one oversized
    /// cell is shortened.
    #[test]
    fn oversized_first_row_string_is_kept_and_fits() {
        let big = "y".repeat(1024 * 1024);
        let envelope = envelope_with_rows(1, |_| vec![Cell::Str(big.clone())]);

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.presentation.cells_truncated, 1);
        let size = serialized_len(&fitted);
        assert!(
            size <= MAX_RESPONSE_BYTES_FLOOR as usize,
            "serialized size {size} exceeds cap"
        );
    }

    /// A single row with one ordinary short column and one 1 MiB map
    /// column: only the map cell exceeds the per-cell budget, so exactly
    /// one cell is truncated and the row is kept.
    #[test]
    fn oversized_first_row_map_is_kept_and_fits() {
        let mut big_map = Map::new();
        big_map.insert("body".to_string(), Value::String("z".repeat(1024 * 1024)));
        let mut envelope = Envelope::default();
        envelope.data.columns = vec![
            Column {
                name: "id".to_string(),
                r#type: "int64".to_string(),
            },
            Column {
                name: "attrs".to_string(),
                r#type: "map".to_string(),
            },
        ];
        envelope.data.rows = vec![vec![Cell::Int(42), Cell::Map(big_map)]];
        envelope.data.row_count = 1;

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.presentation.cells_truncated, 1);
        let size = serialized_len(&fitted);
        assert!(
            size <= MAX_RESPONSE_BYTES_FLOOR as usize,
            "serialized size {size} exceeds cap"
        );
    }

    /// Zero rows, every metadata list at its D4 bound: the fixed part alone
    /// must serialize under 106,496 B.
    #[test]
    fn zero_row_envelope_with_maximal_metadata_fits_under_the_floor() {
        let mut envelope = Envelope::default();
        envelope.data.columns = (0..MAX_COLUMNS)
            .map(|i| Column {
                name: format!("{:0>4}{}", i, "n".repeat(100)),
                r#type: "t".repeat(30),
            })
            .collect();
        envelope.data.row_count = 0;
        envelope.scope.signal = "logs".to_string();
        envelope.scope.table = "logs".to_string();
        envelope.scope.time_range = Some(TimeRange {
            start_ns: "1700000000000000000".to_string(),
            end_ns: "1700000003600000000000".to_string(),
        });
        envelope.scope.predicates_applied = (0..MAX_PREDICATES_APPLIED)
            .map(|i| format!("predicate_{i}_{}", "p".repeat(480)))
            .collect();
        envelope.scope.order_by = (0..MAX_ORDER_BY)
            .map(|i| format!("order_{i}_{}", "o".repeat(480)))
            .collect();
        envelope.ids.query_id = "q".repeat(64);
        envelope.ids.audit_ref = "a".repeat(64);
        envelope.visibility.snapshot_id = "s".repeat(64);
        envelope.visibility.watermark_hour = "2026090800".to_string();
        envelope.visibility.pinned = true;
        envelope.visibility.min_commit_tokens_applied = (0..MAX_MIN_COMMIT_TOKENS)
            .map(|i| format!("tok_{i}_{}", "m".repeat(100)))
            .collect();
        envelope.coverage.complete = true;
        envelope.coverage.fragments = (0..MAX_FRAGMENTS)
            .map(|i| format!("frag_{i}_{}", "f".repeat(130)))
            .collect();
        envelope.coverage.unindexed_predicates = (0..MAX_UNINDEXED_PREDICATES)
            .map(|i| format!("unindexed_{i}_{}", "u".repeat(220)))
            .collect();
        envelope.accuracy.exact = true;
        envelope.presentation.max_rows = 200;
        envelope.presentation.cursor = Some("c".repeat(200));
        envelope.warnings = (0..MAX_WARNINGS)
            .map(|i| format!("warning_{i}_{}", "w".repeat(480)))
            .collect();
        envelope.next_steps = (0..MAX_NEXT_STEPS)
            .map(|i| NextStep {
                action: format!("action_{i}"),
                detail: "d".repeat(480),
            })
            .collect();
        envelope.evidence = (0..MAX_EVIDENCE)
            .map(|i| EvidenceEntry {
                r#ref: format!("ref_{i}_{}", "r".repeat(400)),
                covers: "data.rows".to_string(),
                sha256: "0".repeat(64),
            })
            .collect();

        let elided = envelope.cap_metadata_lists();
        assert_eq!(elided, 0, "every list is already at its bound, not over it");

        let size = serialized_len(&envelope);
        assert!(
            size < 106_496,
            "maximal-metadata envelope serialized to {size} bytes, must be < 106496"
        );
    }

    /// 2^53 + 1 does not round-trip through `f64`; the wire form must be a
    /// JSON string so the exact integer survives. A nanosecond timestamp is
    /// the same rule applied to the same representation.
    #[test]
    fn integers_and_timestamps_serialize_as_strings() {
        let big_int: i64 = (1i64 << 53) + 1;
        let ts_ns: i64 = 1_700_000_000_123_456_789;
        let row = vec![Cell::Int(big_int), Cell::Timestamp(ts_ns)];

        let value = serde_json::to_value(&row).expect("row serializes");
        let cells = value.as_array().expect("row is a JSON array");

        let int_cell = cells[0].as_str().expect("int cell must be a JSON string");
        assert_eq!(int_cell.parse::<i64>().expect("round-trips"), big_int);

        let ts_cell = cells[1].as_str().expect("timestamp cell must be a JSON string");
        assert_eq!(ts_cell.parse::<i64>().expect("round-trips"), ts_ns);
    }
}
