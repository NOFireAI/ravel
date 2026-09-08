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

/// The D4 per-entry serialized-size bounds. Each is a bound on one entry's
/// own serialized JSON -- a string entry's quotes and escapes, or a struct
/// entry's braces, keys, and separators -- not on its source characters, so
/// the count bound above times the bound here is the field's real wire
/// ceiling. Their sum with the scalar allowance is the 102 KiB the ADR
/// states, which is what keeps the fixed part under half the 256 KiB floor.
const COLUMN_ENTRY_BOUND: usize = 160;
const PREDICATE_ENTRY_BOUND: usize = 512;
const ORDER_BY_ENTRY_BOUND: usize = 512;
const MIN_COMMIT_TOKEN_ENTRY_BOUND: usize = 128;
const FRAGMENT_ENTRY_BOUND: usize = 160;
const UNINDEXED_PREDICATE_ENTRY_BOUND: usize = 256;
const WARNING_ENTRY_BOUND: usize = 512;
const NEXT_STEP_ENTRY_BOUND: usize = 512;
const EVIDENCE_ENTRY_BOUND: usize = 512;

/// The D4 per-cell floor: a cell is never cut below this serialized size,
/// even when the whole envelope still does not fit.
const MIN_CELL_BUDGET: usize = 256;

/// How many times [`Envelope::shorten_first_row_to_fit`] re-cuts the kept row
/// under a smaller per-cell budget before it stops. Two passes are enough for
/// every shape measured (the first pass's residual is the row's own array
/// structure and its under-budget cells); the rest is headroom so that
/// termination is a property of the loop, not of the shrink step.
const MAX_SHORTEN_PASSES: usize = 16;

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
    /// Metadata entries dropped because their list was over its count bound.
    pub metadata_elided: u64,
    /// Metadata entries kept but cut because the entry was over its own
    /// per-entry serialized-size bound. Distinct from `metadata_elided`: a
    /// dropped entry is gone, a cut one is still there and still says so
    /// through its truncation marker.
    pub entries_truncated: u64,
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

/// Serialized size of one entry as JSON, whatever its shape.
fn entry_serialized_len<T: Serialize>(entry: &T) -> usize {
    serde_json::to_string(entry)
        .map(|text| text.len())
        .unwrap_or(usize::MAX)
}

/// Cuts every entry of a list of plain strings to `bound` serialized bytes,
/// and returns how many were cut.
fn bound_string_entries(items: &mut [String], bound: usize) -> u64 {
    let mut truncated = 0u64;
    for item in items.iter_mut() {
        if serialized_str_len(item) > bound {
            *item = truncate_to_budget(item, bound);
            truncated += 1;
        }
    }
    truncated
}

/// Cuts a struct entry's variable-length string fields until the entry's own
/// serialized size fits `bound`, and reports whether anything was cut.
///
/// `overhead` is the entry's serialized size minus the serialized size of
/// those fields: the braces, the keys, and the separators, all of which are
/// fixed by the type. So `overhead + sum(fields)` is the entry's exact
/// serialized size, and cutting the longest field by the overshoot lands the
/// entry on the bound in one pass. The loop exists for the case where the
/// longest field alone cannot cover the overshoot; it ends as soon as a pass
/// stops making progress, so no field is cut below the marker.
fn bound_entry_fields(fields: &mut [&mut String], overhead: usize, bound: usize) -> bool {
    let mut truncated = false;
    loop {
        let fields_len: usize = fields.iter().map(|field| serialized_str_len(field)).sum();
        let total = overhead.saturating_add(fields_len);
        if total <= bound {
            return truncated;
        }
        let over = total - bound;
        let Some(index) = (0..fields.len()).max_by_key(|&i| serialized_str_len(fields[i])) else {
            return truncated;
        };
        let longest = serialized_str_len(fields[index]);
        if longest <= MARKER_SERIALIZED_LEN {
            return truncated;
        }
        let budget = longest.saturating_sub(over).max(MARKER_SERIALIZED_LEN);
        *fields[index] = truncate_to_budget(fields[index], budget);
        truncated = true;
    }
}

fn bound_columns(columns: &mut [Column]) -> u64 {
    let mut truncated = 0u64;
    for column in columns.iter_mut() {
        let overhead = entry_serialized_len(column)
            .saturating_sub(serialized_str_len(&column.name) + serialized_str_len(&column.r#type));
        let Column { name, r#type } = column;
        if bound_entry_fields(&mut [name, r#type], overhead, COLUMN_ENTRY_BOUND) {
            truncated += 1;
        }
    }
    truncated
}

fn bound_next_steps(steps: &mut [NextStep]) -> u64 {
    let mut truncated = 0u64;
    for step in steps.iter_mut() {
        let overhead = entry_serialized_len(step)
            .saturating_sub(serialized_str_len(&step.action) + serialized_str_len(&step.detail));
        let NextStep { action, detail } = step;
        if bound_entry_fields(&mut [action, detail], overhead, NEXT_STEP_ENTRY_BOUND) {
            truncated += 1;
        }
    }
    truncated
}

fn bound_evidence(entries: &mut [EvidenceEntry]) -> u64 {
    let mut truncated = 0u64;
    for entry in entries.iter_mut() {
        let overhead = entry_serialized_len(entry).saturating_sub(
            serialized_str_len(&entry.r#ref)
                + serialized_str_len(&entry.covers)
                + serialized_str_len(&entry.sha256),
        );
        let EvidenceEntry {
            r#ref,
            covers,
            sha256,
        } = entry;
        if bound_entry_fields(&mut [r#ref, covers, sha256], overhead, EVIDENCE_ENTRY_BOUND) {
            truncated += 1;
        }
    }
    truncated
}

const TRUNCATION_MARKER: &str = "...[truncated]";

/// Serialized size of the truncation marker alone as a JSON string: the two
/// quotes plus the marker, which needs no escaping. No cut can produce a
/// value smaller than this.
const MARKER_SERIALIZED_LEN: usize = TRUNCATION_MARKER.len() + 2;

/// Serialized size of `s` as a JSON string value, quotes and every escape
/// sequence included. This is the number every budget in this module is
/// measured in: a source byte count is not it, because one source byte can
/// serialize to two (`"`, `\`, `\n`) or six (` `) bytes.
fn serialized_str_len(s: &str) -> usize {
    escaped_len(s) + 2
}

/// Serialized length of `s` inside a JSON string, without the quotes.
fn escaped_len(s: &str) -> usize {
    s.chars().map(escaped_char_len).sum()
}

/// Serialized length of one character inside a JSON string, matching
/// `serde_json`'s escaping exactly (see the test that compares the two over
/// every character it can reach).
fn escaped_char_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\u{8}' | '\u{9}' | '\u{a}' | '\u{c}' | '\u{d}' => 2,
        c if (c as u32) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// Cuts `s` so that its serialized JSON string form -- the surrounding
/// quotes, every escape sequence in the kept prefix, and the trailing marker
/// -- is at most `budget` bytes.
///
/// The kept prefix is measured in serialized bytes, never in source bytes: a
/// body of quotes serializes to two bytes per source byte and a body of
/// control characters to six, so a source-byte cut overruns the budget by
/// that factor. When `budget` is smaller than [`MARKER_SERIALIZED_LEN`] the
/// result is the marker alone, which is the smallest value a cut can produce.
fn truncate_to_budget(s: &str, budget: usize) -> String {
    let allowance = budget.saturating_sub(MARKER_SERIALIZED_LEN);
    let mut used = 0usize;
    let mut end = s.len();
    for (idx, c) in s.char_indices() {
        let next = used + escaped_char_len(c);
        if next > allowance {
            end = idx;
            break;
        }
        used = next;
    }
    format!("{}{}", &s[..end], TRUNCATION_MARKER)
}

/// Shortens every oversized cell in `row` to fit `budget_per_cell`, per the
/// D4 rule: a string cell whose serialized form is over budget is cut to the
/// budget including the trailing marker; a map cell over budget is serialized
/// to JSON text first, then cut the same way and returned as a string.
/// Numbers, timestamps, booleans, and hex ids never exceed 64 B and are left
/// untouched. Every comparison is against the cell's serialized size, so an
/// escape-heavy cell is sized by what goes on the wire. Returns the count of
/// cells actually shortened.
fn shorten_row(row: &mut Row, budget_per_cell: usize) -> u64 {
    let mut truncated = 0u64;
    for cell in row.iter_mut() {
        let text = match cell {
            Cell::Str(s) => {
                if serialized_str_len(s) <= budget_per_cell {
                    continue;
                }
                std::mem::take(s)
            }
            Cell::Map(m) => {
                let text = serde_json::to_string(&Value::Object(m.clone())).unwrap_or_default();
                if text.len() <= budget_per_cell {
                    continue;
                }
                text
            }
            _ => continue,
        };
        *cell = Cell::Str(truncate_to_budget(&text, budget_per_cell));
        truncated += 1;
    }
    truncated
}

/// What [`Envelope::cap_metadata_lists`] did: the two D4 bounds are separate
/// facts and are counted separately.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MetadataCaps {
    /// Entries dropped because a list was over its count bound.
    elided: u64,
    /// Entries kept but cut because they were over their per-entry bound.
    entries_truncated: u64,
}

impl Envelope {
    /// Applies both D4 bounds to every variable-length field outside
    /// `data.rows`: the count bound (keep the first entries, report the
    /// number dropped) and the per-entry serialized-size bound (cut the
    /// over-long entry, report the number cut).
    ///
    /// The count bound runs first, so an over-long entry that is about to be
    /// dropped anyway is never cut.
    fn cap_metadata_lists(&mut self) -> MetadataCaps {
        let elided = truncate_vec(&mut self.data.columns, MAX_COLUMNS)
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
            + truncate_vec(&mut self.evidence, MAX_EVIDENCE);

        let entries_truncated = bound_columns(&mut self.data.columns)
            + bound_string_entries(&mut self.scope.predicates_applied, PREDICATE_ENTRY_BOUND)
            + bound_string_entries(&mut self.scope.order_by, ORDER_BY_ENTRY_BOUND)
            + bound_string_entries(
                &mut self.visibility.min_commit_tokens_applied,
                MIN_COMMIT_TOKEN_ENTRY_BOUND,
            )
            + bound_string_entries(&mut self.coverage.fragments, FRAGMENT_ENTRY_BOUND)
            + bound_string_entries(
                &mut self.coverage.unindexed_predicates,
                UNINDEXED_PREDICATE_ENTRY_BOUND,
            )
            + bound_string_entries(&mut self.warnings, WARNING_ENTRY_BOUND)
            + bound_next_steps(&mut self.next_steps)
            + bound_evidence(&mut self.evidence);

        MetadataCaps {
            elided,
            entries_truncated,
        }
    }

    /// The D4 byte-cap algorithm. Floors `max_response_bytes` at
    /// [`MAX_RESPONSE_BYTES_FLOOR`], caps every metadata list to its bound,
    /// then drops rows from the end of `data.rows` until the envelope fits.
    /// If a single remaining row still does not fit, keeps it and shortens
    /// its oversized cells instead of dropping it, so `data.rows` is never
    /// empty while `rows_omitted` is positive and a retained row always
    /// fits.
    pub fn fit(mut self, requested_max_response_bytes: u64) -> Envelope {
        let caps = self.cap_metadata_lists();
        self.presentation.metadata_elided = caps.elided;
        self.presentation.entries_truncated = caps.entries_truncated;

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

        self.presentation.cells_truncated = self.shorten_first_row_to_fit(cap);
        self
    }

    /// Cuts the cells of the one kept row until the whole envelope fits
    /// `cap`, and returns the number of cells cut.
    ///
    /// The D4 per-cell budget is `max(256 B, (cap - fixed_part) /
    /// column_count)`, where `fixed_part` is the envelope serialized without
    /// `data.rows`. That budget is an estimate of what one cell may spend,
    /// not a measurement of the envelope: it does not account for the row's
    /// own array structure, nor for the cells that are already under budget
    /// and keep their full size. So each pass re-cuts the pristine row under
    /// a smaller budget and re-measures the whole envelope, until it fits or
    /// the budget is at the D4 256 B floor.
    fn shorten_first_row_to_fit(&mut self, cap: usize) -> u64 {
        if serialized_len(self) <= cap {
            return 0;
        }
        let Some(pristine_row) = self.data.rows.first().cloned() else {
            return 0;
        };

        let saved_rows = std::mem::take(&mut self.data.rows);
        let fixed_part = serialized_len(self) as u64;
        self.data.rows = saved_rows;
        let column_count = self.data.columns.len().max(1) as u64;
        // Reserve a few bytes for the row's own array brackets and the commas
        // between cells, which `fixed_part` (computed with `data.rows` emptied
        // to `[]`) does not itself account for.
        const ROW_STRUCTURE_OVERHEAD: u64 = 8;
        let available = (cap as u64)
            .saturating_sub(fixed_part)
            .saturating_sub(ROW_STRUCTURE_OVERHEAD);
        let mut budget_per_cell = available
            .checked_div(column_count)
            .unwrap_or(0)
            .max(MIN_CELL_BUDGET as u64) as usize;

        let mut truncated = 0u64;
        let mut previous_size = usize::MAX;
        // Bounded so termination never depends on the shrink step making
        // progress: a pass that does not shrink the envelope drops straight
        // to the floor budget, and the floor budget ends the loop.
        for _ in 0..MAX_SHORTEN_PASSES {
            let mut row = pristine_row.clone();
            truncated = shorten_row(&mut row, budget_per_cell);
            if let Some(first) = self.data.rows.first_mut() {
                *first = row;
            }
            let size = serialized_len(self);
            if size <= cap || truncated == 0 || budget_per_cell <= MIN_CELL_BUDGET {
                break;
            }
            let over = (size - cap) as u64;
            budget_per_cell = if size >= previous_size {
                MIN_CELL_BUDGET
            } else {
                budget_per_cell
                    .saturating_sub(over.div_ceil(truncated).max(1) as usize)
                    .max(MIN_CELL_BUDGET)
            };
            previous_size = size;
        }
        truncated
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

    /// The single-column envelopes below all resolve to the same per-cell
    /// budget (one column, the same fixed part), so the kept cell serializes
    /// to the same exact size in each: `cap - fixed_part - 8` rounded down at
    /// a character boundary of the source body. Pinning it is what makes the
    /// three tests detect a cut measured in source bytes: such a cut either
    /// overruns the cap or, once the re-measure loop has clamped it, lands on
    /// the 256 B floor instead of this figure.
    const KEPT_CELL_SERIALIZED_LEN: usize = 261_270;
    /// The whole envelope's exact serialized size for those same three cases.
    const FITTED_ENVELOPE_SERIALIZED_LEN: usize = 262_138;

    /// Serialized size of a zero-row envelope with every metadata field at
    /// both its D4 bounds: the largest fixed part the bounds permit. The ADR
    /// requires this to be under 106,496 B, which is what leaves a retained
    /// row its 154 KiB under the 256 KiB floor.
    const MAXIMAL_METADATA_ENVELOPE_LEN: usize = 102_116;
    const _: () = assert!(
        MAXIMAL_METADATA_ENVELOPE_LEN < 106_496,
        "ADR-1374 D4 requires the maximal fixed part under 106,496 B"
    );

    fn cell_len(envelope: &Envelope) -> usize {
        let row = envelope.data.rows.first().expect("one row");
        serde_json::to_string(&row[0])
            .expect("cell serializes")
            .len()
    }

    /// Every cell budget in this module is a serialized-byte budget, so the
    /// per-character escape sizes it sums must be `serde_json`'s own. Checked
    /// over every character with a distinct escaping rule (the C0 range, the
    /// two escaped ASCII punctuation characters, the DEL boundary) plus one
    /// character per UTF-8 length.
    #[test]
    fn escaped_char_len_matches_serde_json() {
        let checked: Vec<char> = (0u32..0x300)
            .chain([0x7f, 0x2028, 0x1F600, 0x10FFFF])
            .filter_map(char::from_u32)
            .collect();
        assert_eq!(checked.len(), 768 + 4, "every probe must be a valid char");
        for c in checked {
            let serialized = serde_json::to_string(&c.to_string()).expect("char serializes");
            assert_eq!(
                escaped_char_len(c) + 2,
                serialized.len(),
                "escaped length of U+{:04X} disagrees with serde_json ({serialized})",
                c as u32
            );
        }
    }

    /// A 1 MiB body of `"` characters: every source byte serializes to two
    /// (`\"`), so a cut measured in source bytes overruns the cap by 2x. The
    /// cut must be measured in serialized bytes instead.
    #[test]
    fn oversized_first_row_of_quotes_fits() {
        let quotes = "\"".repeat(1024 * 1024);
        let envelope = envelope_with_rows(1, |_| vec![Cell::Str(quotes.clone())]);

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.presentation.cells_truncated, 1);
        assert!(fitted.presentation.bytes_cap_hit);
        assert_eq!(cell_len(&fitted), KEPT_CELL_SERIALIZED_LEN);
        let size = serialized_len(&fitted);
        assert_eq!(size, FITTED_ENVELOPE_SERIALIZED_LEN);
        assert!(
            size <= MAX_RESPONSE_BYTES_FLOOR as usize,
            "serialized size {size} exceeds cap {MAX_RESPONSE_BYTES_FLOOR}"
        );
    }

    /// A 1 MiB body of `\n` and spaces: `\n` serializes to two bytes, so the
    /// body's serialized size is 1.5x its source size. The same rule as the
    /// quote case, at a different expansion factor, and with a character that
    /// is not the escape character itself.
    #[test]
    fn oversized_first_row_of_control_characters_fits() {
        let body = "\n ".repeat(512 * 1024);
        assert_eq!(body.len(), 1024 * 1024);
        let envelope = envelope_with_rows(1, |_| vec![Cell::Str(body.clone())]);

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.presentation.cells_truncated, 1);
        assert!(fitted.presentation.bytes_cap_hit);
        assert_eq!(cell_len(&fitted), KEPT_CELL_SERIALIZED_LEN);
        let size = serialized_len(&fitted);
        assert_eq!(size, FITTED_ENVELOPE_SERIALIZED_LEN);
        assert!(
            size <= MAX_RESPONSE_BYTES_FLOOR as usize,
            "serialized size {size} exceeds cap {MAX_RESPONSE_BYTES_FLOOR}"
        );
    }

    /// A map cell whose values are quotes: the map is serialized to JSON text
    /// first (which escapes each quote once), and that text is then cut as a
    /// string (which escapes the text's own quotes again). Both levels count
    /// against the cap.
    #[test]
    fn oversized_first_row_map_of_quotes_fits() {
        let mut big_map = Map::new();
        big_map.insert("body".to_string(), Value::String("\"".repeat(1024 * 1024)));
        big_map.insert("service".to_string(), Value::String("\"api\"".to_string()));
        let envelope = envelope_with_rows(1, |_| vec![Cell::Map(big_map.clone())]);

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.presentation.cells_truncated, 1);
        assert!(fitted.presentation.bytes_cap_hit);
        assert_eq!(cell_len(&fitted), KEPT_CELL_SERIALIZED_LEN);
        let size = serialized_len(&fitted);
        assert_eq!(size, FITTED_ENVELOPE_SERIALIZED_LEN);
        assert!(
            size <= MAX_RESPONSE_BYTES_FLOOR as usize,
            "serialized size {size} exceeds cap {MAX_RESPONSE_BYTES_FLOOR}"
        );
    }

    /// The per-cell budget is computed from the envelope with `data.rows`
    /// emptied, so it accounts for neither the row's own array structure nor
    /// the cells already under budget. With 64 columns the commas alone
    /// overrun the cap, which only a re-measurement of the whole envelope
    /// after the cut can see.
    #[test]
    fn oversized_first_row_of_many_columns_fits() {
        const COLUMNS: usize = 64;
        let big = "q".repeat(64 * 1024);
        let mut envelope = Envelope::default();
        envelope.data.columns = (0..COLUMNS)
            .map(|i| Column {
                name: format!("c{i}"),
                r#type: "string".to_string(),
            })
            .collect();
        envelope.data.rows = vec![(0..COLUMNS).map(|_| Cell::Str(big.clone())).collect()];
        envelope.data.row_count = 1;

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.presentation.cells_truncated, COLUMNS as u64);
        let size = serialized_len(&fitted);
        assert!(
            size <= MAX_RESPONSE_BYTES_FLOOR as usize,
            "serialized size {size} exceeds cap {MAX_RESPONSE_BYTES_FLOOR}"
        );
    }

    /// A warning over its 512 B per-entry bound is cut to exactly the bound
    /// and counted, while the warning next to it is left alone. The body is
    /// quotes, so the bound has to be measured in serialized bytes: 248
    /// quotes plus the marker is 512 serialized bytes, 262 source bytes.
    #[test]
    fn oversized_warning_is_cut_to_its_entry_bound() {
        let mut envelope = Envelope {
            warnings: vec!["\"".repeat(10_000), "short warning".to_string()],
            ..Default::default()
        };

        let caps = envelope.cap_metadata_lists();

        assert_eq!(caps.elided, 0, "two warnings are under the count bound");
        assert_eq!(caps.entries_truncated, 1);
        assert_eq!(envelope.warnings.len(), 2);
        let cut = &envelope.warnings[0];
        assert_eq!(serialized_str_len(cut), WARNING_ENTRY_BOUND);
        assert_eq!(cut.chars().filter(|c| *c == '"').count(), 248);
        assert_eq!(cut.len(), 248 + TRUNCATION_MARKER.len());
        assert!(cut.ends_with(TRUNCATION_MARKER));
        assert_eq!(envelope.warnings[1], "short warning");
    }

    /// A column name over the 160 B per-entry bound is cut so the whole entry
    /// (both fields, the keys, the braces, the separators) lands on exactly
    /// the bound. Only the longest field is cut: the type is untouched.
    #[test]
    fn oversized_column_name_is_cut() {
        let mut envelope = Envelope::default();
        envelope.data.columns = vec![
            Column {
                name: "n".repeat(10_000),
                r#type: "map<string,string>".to_string(),
            },
            Column {
                name: "ts".to_string(),
                r#type: "timestamp".to_string(),
            },
        ];

        let caps = envelope.cap_metadata_lists();

        assert_eq!(caps.elided, 0, "two columns are under the count bound");
        assert_eq!(caps.entries_truncated, 1);
        let cut = &envelope.data.columns[0];
        assert_eq!(entry_serialized_len(cut), COLUMN_ENTRY_BOUND);
        assert_eq!(cut.name.len(), 107 + TRUNCATION_MARKER.len());
        assert!(cut.name.ends_with(TRUNCATION_MARKER));
        assert_eq!(cut.r#type, "map<string,string>");
        assert_eq!(entry_serialized_len(&envelope.data.columns[1]), 32);
    }

    /// Zero rows, every metadata list at its count bound and every entry at
    /// its per-entry bound: the fixed part alone must serialize to exactly
    /// [`MAXIMAL_METADATA_ENVELOPE_LEN`], which the ADR requires to be under
    /// 106,496 B.
    ///
    /// Each entry below is sized to land on its bound exactly, so the
    /// envelope this builds is the largest one the D4 bounds permit, and
    /// `cap_metadata_lists` must find nothing to do. A single 10 MiB warning
    /// fed in afterwards is cut back to the same size, so no metadata a
    /// caller or the engine can produce moves this figure.
    #[test]
    fn zero_row_envelope_with_maximal_metadata_fits_under_the_floor() {
        let mut envelope = Envelope::default();
        envelope.data.columns = (0..MAX_COLUMNS)
            .map(|i| Column {
                name: format!("{:0>4}{}", i, "n".repeat(105)),
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
            .map(|i| format!("{i:0>2}_{}", "p".repeat(507)))
            .collect();
        envelope.scope.order_by = (0..MAX_ORDER_BY)
            .map(|i| format!("{i:0>2}_{}", "o".repeat(507)))
            .collect();
        envelope.ids.query_id = "q".repeat(64);
        envelope.ids.audit_ref = "a".repeat(64);
        envelope.visibility.snapshot_id = "s".repeat(64);
        envelope.visibility.watermark_hour = "2026090800".to_string();
        envelope.visibility.pinned = true;
        envelope.visibility.min_commit_tokens_applied = (0..MAX_MIN_COMMIT_TOKENS)
            .map(|i| format!("{i:0>2}_{}", "m".repeat(123)))
            .collect();
        envelope.coverage.complete = true;
        envelope.coverage.fragments = (0..MAX_FRAGMENTS)
            .map(|i| format!("{i:0>2}_{}", "f".repeat(155)))
            .collect();
        envelope.coverage.unindexed_predicates = (0..MAX_UNINDEXED_PREDICATES)
            .map(|i| format!("{i:0>2}_{}", "u".repeat(251)))
            .collect();
        envelope.accuracy.exact = true;
        envelope.presentation.max_rows = 200;
        envelope.presentation.cursor = Some("c".repeat(200));
        envelope.warnings = (0..MAX_WARNINGS)
            .map(|i| format!("{i:0>2}_{}", "w".repeat(507)))
            .collect();
        envelope.next_steps = (0..MAX_NEXT_STEPS)
            .map(|i| NextStep {
                action: format!("action_{i}"),
                detail: "d".repeat(479),
            })
            .collect();
        envelope.evidence = (0..MAX_EVIDENCE)
            .map(|i| EvidenceEntry {
                r#ref: format!("{i:0>2}_{}", "r".repeat(402)),
                covers: "data.rows".to_string(),
                sha256: "0".repeat(64),
            })
            .collect();

        for column in &envelope.data.columns {
            assert_eq!(entry_serialized_len(column), COLUMN_ENTRY_BOUND);
        }
        for predicate in &envelope.scope.predicates_applied {
            assert_eq!(serialized_str_len(predicate), PREDICATE_ENTRY_BOUND);
        }
        for order in &envelope.scope.order_by {
            assert_eq!(serialized_str_len(order), ORDER_BY_ENTRY_BOUND);
        }
        for token in &envelope.visibility.min_commit_tokens_applied {
            assert_eq!(serialized_str_len(token), MIN_COMMIT_TOKEN_ENTRY_BOUND);
        }
        for fragment in &envelope.coverage.fragments {
            assert_eq!(serialized_str_len(fragment), FRAGMENT_ENTRY_BOUND);
        }
        for predicate in &envelope.coverage.unindexed_predicates {
            assert_eq!(
                serialized_str_len(predicate),
                UNINDEXED_PREDICATE_ENTRY_BOUND
            );
        }
        for warning in &envelope.warnings {
            assert_eq!(serialized_str_len(warning), WARNING_ENTRY_BOUND);
        }
        for step in &envelope.next_steps {
            assert_eq!(entry_serialized_len(step), NEXT_STEP_ENTRY_BOUND);
        }
        for entry in &envelope.evidence {
            assert_eq!(entry_serialized_len(entry), EVIDENCE_ENTRY_BOUND);
        }

        let caps = envelope.cap_metadata_lists();
        assert_eq!(
            caps,
            MetadataCaps::default(),
            "every list is already at its bound, not over it"
        );

        assert_eq!(serialized_len(&envelope), MAXIMAL_METADATA_ENVELOPE_LEN);

        envelope.warnings[0] = "w".repeat(10 * 1024 * 1024);
        let caps = envelope.cap_metadata_lists();
        assert_eq!(
            caps,
            MetadataCaps {
                elided: 0,
                entries_truncated: 1,
            }
        );
        assert_eq!(
            serialized_str_len(&envelope.warnings[0]),
            WARNING_ENTRY_BOUND
        );
        assert_eq!(serialized_len(&envelope), MAXIMAL_METADATA_ENVELOPE_LEN);
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

        let ts_cell = cells[1]
            .as_str()
            .expect("timestamp cell must be a JSON string");
        assert_eq!(ts_cell.parse::<i64>().expect("round-trips"), ts_ns);
    }
}
