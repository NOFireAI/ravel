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

/// D4 bounds "the scalar fields together" at 4 KiB. This is that allowance,
/// measured the same way every other bound in this module is: the serialized
/// JSON size of the payloads, not their source characters.
///
/// It covers every non-list, non-row payload a caller or the engine supplies:
/// `plan`, `failure.message`, `failure.counter`, the three `budget` values,
/// `accuracy.approximation`, both `ids` fields, both `visibility` strings,
/// `scope.signal`, `scope.table`, and both `scope.time_range` bounds. The
/// envelope's own bookkeeping (the booleans, the counters, the status word)
/// is skeleton rather than payload and is covered by [`SKELETON_SLACK`]
/// instead. `presentation.cursor` has [`CURSOR_BOUND`] of its own.
const SCALAR_ALLOWANCE: usize = 4096;

/// The cursor's own bound, separate from [`SCALAR_ALLOWANCE`] (the 2026-09-09
/// amendment to ADR-1374 D4).
///
/// Sharing the scalar allowance made the two compete: a caller's own `plan`
/// text could push a cursor out of the envelope, and the only way to stay
/// under the shared bound was to drop the cursor whole, since a MAC'd token
/// cut to length is a corrupt token. Pagination then ended for a reason that
/// had nothing to do with pagination.
///
/// With its own bound nothing else can crowd it out, and the only way to
/// exceed it is for this process to mint a cursor over its own limit, which
/// is a server defect. On an otherwise-successful envelope that defect is the
/// `internal` failure. On one that already carries a failure the class stays
/// what it was, and the defect is reported as a counted drop plus a warning:
/// see [`Envelope::fit`].
///
/// Measured the same way the scalar allowance is: the serialized JSON size of
/// the token string, not its source characters. Distinct from
/// [`crate::cursor::MAX_TOKEN_BYTES`], which is the hostile-input cap the
/// codec refuses to even look at a longer token past; this is what an
/// envelope will carry.
const CURSOR_BOUND: usize = 4096;

/// The per-scalar sub-bounds. Every scalar with a natural size has one, so
/// no single field can eat the whole allowance and no cut is needed on a
/// field that is already the size its content implies.
const SIGNAL_BOUND: usize = 64;
const TABLE_BOUND: usize = 128;
const TIME_RANGE_BOUND: usize = 32;
const QUERY_ID_BOUND: usize = 128;
const AUDIT_REF_BOUND: usize = 128;
const SNAPSHOT_ID_BOUND: usize = 128;
const INGEST_WATERMARK_HOUR_BOUND: usize = 32;
const APPROXIMATION_BOUND: usize = 256;
const FAILURE_COUNTER_BOUND: usize = 128;

/// What the sub-bounded scalars can occupy together. `presentation.cursor`
/// has no entry here: it is not one of the scalars this allowance covers, and
/// has [`CURSOR_BOUND`] of its own.
const FIXED_SCALAR_BOUNDS: usize = SIGNAL_BOUND
    + TABLE_BOUND
    + 2 * TIME_RANGE_BOUND
    + QUERY_ID_BOUND
    + AUDIT_REF_BOUND
    + SNAPSHOT_ID_BOUND
    + INGEST_WATERMARK_HOUR_BOUND
    + APPROXIMATION_BOUND
    + FAILURE_COUNTER_BOUND;

/// The rest of the allowance, for the five scalars with no natural size:
/// `plan`, `failure.message`, and the three `budget` values.
/// [`Envelope::cap_scalars`] cuts them in that order.
const VARIABLE_SCALAR_ALLOWANCE: usize = SCALAR_ALLOWANCE - FIXED_SCALAR_BOUNDS;

/// Cutting the five variable scalars to their floor always lands the scalars
/// inside the allowance, which is why `cap_scalars` needs no failure channel:
/// its last stage cannot leave the envelope over the bound.
const _: () = assert!(
    5 * MARKER_SERIALIZED_LEN <= VARIABLE_SCALAR_ALLOWANCE,
    "the five variable scalars must fit the allowance at their marker floor"
);

/// The D4 per-cell floor: a cell is never cut below this serialized size,
/// even when the whole envelope still does not fit.
const MIN_CELL_BUDGET: usize = 256;

/// How many times [`Envelope::shorten_first_row_to_fit`] re-cuts the kept row
/// under a smaller per-cell budget before it stops. Two passes are enough for
/// every shape measured (the first pass's residual is the row's own array
/// structure and its under-budget cells); the rest is headroom so that
/// termination is a property of the loop, not of the shrink step.
const MAX_SHORTEN_PASSES: usize = 16;

/// Serialized size of an envelope with no rows, no list entries, and every
/// scalar empty: the keys, the braces, the separators, the shortest status
/// word, and the zeroed counters. Pinned by
/// `zero_row_envelope_with_maximal_metadata_fits_under_the_floor`, which
/// fails if a field is added or renamed without revisiting the arithmetic
/// below.
const EMPTY_ENVELOPE_SERIALIZED_LEN: usize = 859;

/// Room for the skeleton parts that grow without being payload: the counters
/// widening from `0` to their full decimal form, `status` from `ok` to
/// `ok_bounded`, and the `failure` and `time_range` blocks appearing with
/// their own keys and class instead of `null`. Measured at 195 B for the
/// widest shape; the rest is headroom.
const SKELETON_SLACK: usize = 512;

/// What every metadata list can occupy together: each list at its count
/// bound, each entry at its per-entry bound, plus one separator per entry.
const LIST_ALLOWANCE: usize = MAX_PROJECTION_COLUMNS * (COLUMN_ENTRY_BOUND + 1)
    + MAX_PREDICATES_APPLIED * (PREDICATE_ENTRY_BOUND + 1)
    + MAX_ORDER_BY * (ORDER_BY_ENTRY_BOUND + 1)
    + MAX_MIN_COMMIT_TOKENS * (MIN_COMMIT_TOKEN_ENTRY_BOUND + 1)
    + MAX_FRAGMENTS * (FRAGMENT_ENTRY_BOUND + 1)
    + MAX_UNINDEXED_PREDICATES * (UNINDEXED_PREDICATE_ENTRY_BOUND + 1)
    + MAX_WARNINGS * (WARNING_ENTRY_BOUND + 1)
    + MAX_NEXT_STEPS * (NEXT_STEP_ENTRY_BOUND + 1)
    + MAX_EVIDENCE * (EVIDENCE_ENTRY_BOUND + 1);

/// The largest fixed part -- the whole envelope but `data.rows` -- that can
/// survive [`Envelope::cap_metadata_lists`] and [`Envelope::cap_scalars`],
/// plus the [`CURSOR_BOUND`] the cursor holds outside the scalar allowance,
/// plus the room [`Envelope::finish`] needs afterward for the
/// [`IDENTITY_WARNINGS_ALLOWANCE`] it can still add. `finish` runs after
/// `fit`, so a row-packed envelope that `fit` measured as exactly at its cap
/// has zero slack of its own; folding the identity-warning allowance in here
/// is what lets `fit` reserve that room up front (see [`Envelope::fit`]'s
/// `cap` calculation) rather than leaving `finish` to add bytes `fit` never
/// accounted for.
const MAXIMAL_FIXED_PART: usize = EMPTY_ENVELOPE_SERIALIZED_LEN
    + SKELETON_SLACK
    + LIST_ALLOWANCE
    + SCALAR_ALLOWANCE
    + CURSOR_BOUND
    + IDENTITY_WARNINGS_ALLOWANCE;

/// What makes [`Envelope::fit`] total. `fit` floors its cap at
/// [`MAX_RESPONSE_BYTES_FLOOR`] and, as a last resort, empties `data.rows`;
/// the result is the fixed part alone, which the caps hold under
/// [`MAXIMAL_FIXED_PART`]. So a zero-row envelope over the cap is
/// arithmetically impossible and `fit` needs no failure channel, even once
/// `finish` adds the identity warnings [`MAXIMAL_FIXED_PART`] now reserves
/// room for.
const _: () = assert!(
    MAXIMAL_FIXED_PART < MAX_RESPONSE_BYTES_FLOOR as usize,
    "a zero-row envelope must fit the smallest cap fit can be given"
);

/// Largest a hex id may be, in characters: a 32-byte trace id is 64
/// characters of hex, the widest id any signal carries. D4 states a hex id
/// never exceeds 64 B, and [`HexId`] is what makes that true of every value
/// that reaches an envelope.
pub const MAX_HEX_ID_LEN: usize = 64;

/// Why a string is not a hex id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HexIdError {
    /// Longer than [`MAX_HEX_ID_LEN`] characters.
    #[error("hex id length {len} exceeds the {max}-character cap")]
    TooLong { len: usize, max: usize },
    /// Not lowercase hex. Reports the first offending character, which is one
    /// the caller supplied.
    #[error("hex id contains {found:?}, which is not lowercase hex")]
    NotLowercaseHex { found: char },
}

/// The payload of a [`Cell::HexId`]: at most [`MAX_HEX_ID_LEN`] characters of
/// lowercase hex, checked once at construction.
///
/// The bound is a construction-time invariant and not a `fit`-time cut
/// because [`Envelope::fit`] skips hex id cells: D4 lists them with the
/// booleans and the numbers as cells small enough that shortening one is
/// never what makes an envelope fit. That skip is only sound if no oversized
/// hex id can exist, so the inner string is private and [`HexId::new`] is the
/// one way to make one. A `Cell::HexId(String)` variant, or a
/// `From<String>`, would leave `fit` skipping a cell of unbounded size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HexId(String);

impl HexId {
    /// Checks the length and the alphabet, and returns a typed error rather
    /// than a silently cut or lossy value.
    pub fn new(id: impl Into<String>) -> Result<HexId, HexIdError> {
        let id = id.into();
        // Every accepted character is one ASCII byte, so `chars().count()`
        // and the byte length agree and either bound is the other.
        if id.len() > MAX_HEX_ID_LEN {
            return Err(HexIdError::TooLong {
                len: id.len(),
                max: MAX_HEX_ID_LEN,
            });
        }
        if let Some(found) = id.chars().find(|c| !matches!(c, '0'..='9' | 'a'..='f')) {
            return Err(HexIdError::NotLowercaseHex { found });
        }
        Ok(HexId(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Byte-serialized cell payload of one table row. Every variant follows the
/// D4 precision rules: [`Cell::Int`] and [`Cell::Timestamp`] serialize as
/// JSON strings (nanosecond epochs exceed 2^53); [`Cell::Float`] follows
/// `float_to_json`; [`Cell::HexId`] is a hex string bounded at construction
/// (see [`HexId`]) and, like `Bool`, `Int`, and `Timestamp`, never exceeds
/// 64 B and is never shortened by [`Envelope::fit`]; [`Cell::Map`] serializes
/// as a JSON object when it fits under the per-cell budget, and is
/// re-serialized to truncated JSON text (as a string) only when it does not.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    /// A carrier wide enough for the union of the signed and unsigned 64-bit
    /// ranges, not a general 128-bit integer: every value that reaches it
    /// came from an `i64` or a `u64`, so it lies in
    /// `i64::MIN..=u64::MAX`. The width exists so a `u64` above
    /// `i64::MAX` stays an integer instead of being widened to a float or
    /// carried as a string.
    Int(i128),
    Timestamp(i64),
    Float(f64),
    HexId(HexId),
    Str(String),
    Map(Map<String, Value>),
}

impl Cell {
    /// Builds a [`Cell::HexId`], refusing anything [`HexId::new`] refuses.
    /// This is the only way to construct the variant.
    pub fn hex_id(id: impl Into<String>) -> Result<Cell, HexIdError> {
        Ok(Cell::HexId(HexId::new(id)?))
    }

    fn to_value(&self) -> Value {
        match self {
            Cell::Null => Value::Null,
            Cell::Bool(b) => Value::Bool(*b),
            Cell::Int(n) => Value::String(n.to_string()),
            Cell::Timestamp(n) => Value::String(n.to_string()),
            Cell::Float(f) => float_to_json(*f),
            Cell::HexId(id) => Value::String(id.0.clone()),
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
    /// Freshness: the greatest ingest hour among the segments this operation
    /// resolved, as the decimal unix hour in a JSON string (D4's precision
    /// rule carries integers as strings). It is not `YYYYMMDDHH`, and it is
    /// not the catalog's `YYYYMMDDTHH` key text.
    ///
    /// Deliberately not spelled `watermark_hour`: that name is the FOLD
    /// watermark everywhere else in this system (docs/catalog-and-mvcc.md
    /// makes HEAD's `watermark_hour` authoritative, and ravel-catalog and
    /// ravel-maintain carry it through `SnapshotPartRef` and `PartHeader`).
    /// This is a different quantity, and a reader who conflates the two reads
    /// a cost boundary as a freshness claim.
    pub ingest_watermark_hour: String,
    pub pinned: bool,
    pub min_commit_tokens_applied: Vec<String>,
    /// Set by an operation that resolved a snapshot and got no segments back,
    /// which is the one case where [`Self::ingest_watermark_hour`] has no
    /// value to report: it is the greatest ingest hour among the resolved
    /// segments, and there are none.
    ///
    /// Not a wire field. It selects which warning
    /// [`Envelope::warn_unreported_identity`] emits for the empty watermark,
    /// so a caller can tell an empty resolve from an operation that never
    /// measures the field at all. Both leave the string empty, and the
    /// distinction is not recoverable from the envelope without it.
    #[serde(skip)]
    pub resolved_no_segments: bool,
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
    /// Metadata entries dropped because their list was over its count bound,
    /// plus the cursor when it was dropped for being over [`CURSOR_BOUND`] on
    /// an envelope that already carried a failure. Both are the same fact: a
    /// field the response bounds would not carry is gone from it. The cursor
    /// case cannot be reported as a failure class there without displacing the
    /// one that says why the call failed, so this counter and a warning are
    /// what make it observable.
    pub metadata_elided: u64,
    /// Metadata entries kept but cut because the entry was over its own
    /// per-entry serialized-size bound. Distinct from `metadata_elided`: a
    /// dropped entry is gone, a cut one is still there and still says so
    /// through its truncation marker.
    pub entries_truncated: u64,
    /// Scalar fields cut or replaced by the truncation marker because the
    /// scalars together were over the D4 4 KiB allowance. The cursor is not
    /// among them: it holds its own [`CURSOR_BOUND`] outside that allowance,
    /// and a token over that bound is an internal failure rather than
    /// something to cut, since a cut would corrupt its MAC. Counted apart
    /// from `entries_truncated` because a scalar is not a list entry: one
    /// oversized `plan` says something different about a result than sixteen
    /// cut warnings do.
    pub scalars_truncated: u64,
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
    /// Lowercase hex BLAKE3-256 digest of the canonical bytes `covers`
    /// names. The field says which function produced it: this crate hashes
    /// with BLAKE3 everywhere (`ravel_sql::flight_ticket` does too), and a
    /// field called `sha256` carrying a BLAKE3 digest cannot be verified by
    /// anyone who believes the name.
    pub blake3_256: String,
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
    // Terminates: every pass either returns or cuts the longest field strictly
    // shorter, and the marker length is a floor no field can go below.
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
                + serialized_str_len(&entry.blake3_256),
        );
        let EvidenceEntry {
            r#ref,
            covers,
            blake3_256,
        } = entry;
        if bound_entry_fields(
            &mut [r#ref, covers, blake3_256],
            overhead,
            EVIDENCE_ENTRY_BOUND,
        ) {
            truncated += 1;
        }
    }
    truncated
}

/// Cuts one scalar to its own sub-bound, and reports whether it was cut.
fn bound_scalar(field: &mut String, bound: usize) -> bool {
    if serialized_str_len(field) <= bound {
        return false;
    }
    *field = truncate_to_budget(field, bound);
    true
}

/// Cuts one variable scalar by `over` serialized bytes, down to the marker
/// but never below it, and reports whether it was cut. A field already at the
/// marker has nothing left to give and is left alone.
fn shrink_scalar(field: &mut String, over: usize) -> bool {
    let len = serialized_str_len(field);
    if len <= MARKER_SERIALIZED_LEN {
        return false;
    }
    let budget = len.saturating_sub(over).max(MARKER_SERIALIZED_LEN);
    *field = truncate_to_budget(field, budget);
    true
}

const TRUNCATION_MARKER: &str = "...[truncated]";

/// Serialized size of the truncation marker alone as a JSON string: the two
/// quotes plus the marker, which needs no escaping. No cut can produce a
/// value smaller than this.
const MARKER_SERIALIZED_LEN: usize = TRUNCATION_MARKER.len() + 2;

/// The four identity fields D4 declares as strings, in the order
/// [`Envelope::warn_unreported_identity`] warns about them.
const IDENTITY_FIELDS: [&str; 4] = [
    "visibility.snapshot_id",
    "visibility.ingest_watermark_hour",
    "ids.query_id",
    "ids.audit_ref",
];

/// The fixed text [`Envelope::warn_unreported_identity`] appends to each
/// field name in [`IDENTITY_FIELDS`].
const IDENTITY_WARNING_SUFFIX: &str = " is not reported by this operation";

/// What [`Envelope::warn_unreported_identity`] appends instead, for the one
/// field and the one case where the operation did measure and the quantity
/// has no value: a resolve that returned no segments has no greatest ingest
/// hour among them.
///
/// Different words from [`IDENTITY_WARNING_SUFFIX`] on purpose. That one is a
/// statement about the operation ("this tool never reports the field"), and a
/// caller reading it on an empty resolve would conclude the freshness of its
/// own tenant is unknowable through this tool rather than that the window it
/// asked about is empty. `ravel_describe_data` exists to answer exactly that
/// question, so the two cases have to be told apart in the text.
const EMPTY_RESOLVE_WARNING_SUFFIX: &str = " is absent: this operation resolved no segments";

/// Index in [`IDENTITY_FIELDS`] of the one field
/// [`EMPTY_RESOLVE_WARNING_SUFFIX`] can apply to.
const INGEST_WATERMARK_HOUR_IDENTITY_INDEX: usize = 1;

const _: () = assert!(
    ascii_eq(
        IDENTITY_FIELDS[INGEST_WATERMARK_HOUR_IDENTITY_INDEX],
        "visibility.ingest_watermark_hour"
    ),
    "the empty-resolve warning must point at the ingest watermark field"
);

/// Whether two ASCII strings are equal, for the const assertions above:
/// `str` has no `const` equality on stable Rust.
const fn ascii_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// The larger of two lengths, for the const arithmetic below: `usize::max` is
/// not `const` on stable Rust.
const fn max_len(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

/// Serialized JSON string length of one byte inside a string, mirroring
/// [`escaped_char_len`] for the ASCII range: every byte
/// [`IDENTITY_FIELDS`] and [`IDENTITY_WARNING_SUFFIX`] can contain today.
/// Kept separate from `escaped_char_len` because a `const fn` cannot decode
/// UTF-8 through `str::chars` on stable Rust, and these two string sources
/// are ASCII by construction (field names and fixed English prose).
const fn ascii_escaped_byte_len(b: u8) -> usize {
    match b {
        b'"' | b'\\' => 2,
        b if b < 0x20 => 6,
        _ => 1,
    }
}

/// Exact serialized JSON string length of `a` immediately followed by `b`,
/// as one string value: the two surrounding quotes plus every byte of both
/// pieces escaped the way `serialized_str_len` measures every other entry
/// in this module.
const fn ascii_pair_serialized_len(a: &str, b: &str) -> usize {
    let mut len = 2; // the surrounding quotes
    let a = a.as_bytes();
    let mut i = 0;
    while i < a.len() {
        len += ascii_escaped_byte_len(a[i]);
        i += 1;
    }
    let b = b.as_bytes();
    i = 0;
    while i < b.len() {
        len += ascii_escaped_byte_len(b[i]);
        i += 1;
    }
    len
}

/// Worst-case serialized cost of the four
/// [`Envelope::warn_unreported_identity`] entries together, as `warnings`
/// list entries: each field's own message (its name from [`IDENTITY_FIELDS`]
/// plus [`IDENTITY_WARNING_SUFFIX`]), plus one list separator per entry, the
/// same `+ 1` every other per-entry term in [`LIST_ALLOWANCE`] carries.
/// Derived from the strings themselves so a changed field name or a changed
/// wording cannot drift silently from what [`Envelope::fit`] reserves room
/// for.
///
/// The ingest watermark field takes whichever of its two messages is longer:
/// one envelope carries either the not-reported wording or the
/// [`EMPTY_RESOLVE_WARNING_SUFFIX`] one for that field, never both.
const fn identity_warnings_allowance() -> usize {
    let mut total = 0usize;
    let mut i = 0usize;
    while i < IDENTITY_FIELDS.len() {
        let mut entry = ascii_pair_serialized_len(IDENTITY_FIELDS[i], IDENTITY_WARNING_SUFFIX);
        if i == INGEST_WATERMARK_HOUR_IDENTITY_INDEX {
            entry = max_len(
                entry,
                ascii_pair_serialized_len(IDENTITY_FIELDS[i], EMPTY_RESOLVE_WARNING_SUFFIX),
            );
        }
        total += entry + 1;
        i += 1;
    }
    total
}

/// See [`identity_warnings_allowance`].
const IDENTITY_WARNINGS_ALLOWANCE: usize = identity_warnings_allowance();

/// Serialized size of `s` as a JSON string value, quotes and every escape
/// sequence included. This is the number every budget in this module is
/// measured in: a source byte count is not it, because one source byte can
/// serialize to two (`"`, `\`, `\n`) or six (a NUL, which serde_json writes
/// as a backslash, a `u`, and four hex digits). That escape is spelled out
/// here rather than written literally: a raw NUL byte in a source file makes
/// every text tool, `grep` and `git diff` included, treat it as binary.
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
/// untouched -- for hex ids that holds because [`HexId::new`] is the only way
/// to build one and refuses anything longer. Every comparison is against the
/// cell's serialized size, so an escape-heavy cell is sized by what goes on
/// the wire. Returns the count of cells actually shortened.
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
            // Null, Bool, Int, Timestamp, Float, and HexId are all bounded
            // by their own types at 64 B or less, so there is nothing here a
            // cut could reclaim.
            Cell::Null
            | Cell::Bool(_)
            | Cell::Int(_)
            | Cell::Timestamp(_)
            | Cell::Float(_)
            | Cell::HexId(_) => continue,
        };
        *cell = Cell::Str(truncate_to_budget(&text, budget_per_cell));
        truncated += 1;
    }
    truncated
}

/// How many rows, counted from the front, fit under `cap` once combined
/// with `fixed` (the envelope's serialized size with `data.rows` emptied).
/// `row_lens` is each row's own serialized JSON length; every row after the
/// first adds one more byte for the comma `serde_json`'s compact array form
/// places between it and the row before it. Never returns less than 1 when
/// `row_lens` is non-empty: dropping the first row is never on the table
/// (D4's first-row guarantee), so a first row that alone exceeds the
/// remaining room is still kept and left for
/// [`Envelope::shorten_first_row_to_fit`] to shrink instead.
///
/// This is the whole point of the model: computing it costs one
/// `serialized_len` call for the fixed part and one per row, not one per
/// *dropped* row re-serializing the whole envelope, which is what made the
/// previous version of this function quadratic in the row count.
fn rows_fitting_prefix(fixed: usize, cap: usize, row_lens: &[usize]) -> usize {
    let mut total = fixed;
    let mut kept = 0usize;
    for (index, &len) in row_lens.iter().enumerate() {
        let separator = usize::from(index > 0);
        let next_total = total.saturating_add(len).saturating_add(separator);
        if next_total > cap && kept >= 1 {
            break;
        }
        total = next_total;
        kept = index + 1;
    }
    kept
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
        let elided = truncate_vec(&mut self.data.columns, MAX_PROJECTION_COLUMNS)
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

    /// Serialized size of the scalar fields the [`SCALAR_ALLOWANCE`] covers.
    ///
    /// The three `budget` values are JSON of a shape D6 owns, so they are
    /// measured as serialized JSON rather than as strings; every other scalar
    /// is a string and is measured with its quotes and escapes.
    fn scalar_serialized_len(&self) -> usize {
        let mut total = serialized_str_len(&self.scope.signal)
            + serialized_str_len(&self.scope.table)
            + serialized_str_len(&self.ids.query_id)
            + serialized_str_len(&self.ids.audit_ref)
            + serialized_str_len(&self.visibility.snapshot_id)
            + serialized_str_len(&self.visibility.ingest_watermark_hour)
            + entry_serialized_len(&self.budget.effective)
            + entry_serialized_len(&self.budget.actual)
            + entry_serialized_len(&self.budget.estimate);
        if let Some(range) = &self.scope.time_range {
            total += serialized_str_len(&range.start_ns) + serialized_str_len(&range.end_ns);
        }
        if let Some(approximation) = &self.accuracy.approximation {
            total += serialized_str_len(approximation);
        }
        if let Some(failure) = &self.failure {
            total += serialized_str_len(&failure.message);
            if let Some(counter) = &failure.counter {
                total += serialized_str_len(counter);
            }
        }
        if let Some(plan) = &self.plan {
            total += serialized_str_len(plan);
        }
        total
    }

    /// Serialized size of `presentation.cursor`, which [`CURSOR_BOUND`] covers
    /// on its own rather than through [`SCALAR_ALLOWANCE`].
    fn cursor_serialized_len(&self) -> usize {
        self.presentation
            .cursor
            .as_ref()
            .map_or(0, |cursor| serialized_str_len(cursor))
    }

    /// Applies the D4 scalar bound, and returns how many scalars it cut.
    ///
    /// Two stages. Every scalar with a natural size is cut to its own
    /// sub-bound first, so one field cannot eat the allowance. Then, if the
    /// scalars are still over it together, the five with no natural size are
    /// cut in the D4 order -- `plan`, `failure.message`, then the three
    /// `budget` values -- each by the current overshoot and none below the
    /// truncation marker.
    ///
    /// `presentation.cursor` is not one of them. It has [`CURSOR_BOUND`] of
    /// its own, so nothing a caller supplies can crowd it out and nothing
    /// here touches it: a MAC'd token cut to length is not a shorter cursor
    /// but a corrupt one, and a token over its own bound is a defect in the
    /// process that minted it rather than something to negotiate away against
    /// a caller's `plan` text (see [`Envelope::fit`]).
    ///
    /// The last stage cannot leave the scalars over the allowance: with every
    /// variable scalar at the marker the total is at most
    /// `FIXED_SCALAR_BOUNDS + 5 * MARKER_SERIALIZED_LEN`, which the const
    /// assertion beside [`VARIABLE_SCALAR_ALLOWANCE`] holds under the bound.
    fn cap_scalars(&mut self) -> u64 {
        let mut cut = 0u64;
        cut += u64::from(bound_scalar(&mut self.scope.signal, SIGNAL_BOUND));
        cut += u64::from(bound_scalar(&mut self.scope.table, TABLE_BOUND));
        if let Some(range) = &mut self.scope.time_range {
            cut += u64::from(bound_scalar(&mut range.start_ns, TIME_RANGE_BOUND));
            cut += u64::from(bound_scalar(&mut range.end_ns, TIME_RANGE_BOUND));
        }
        cut += u64::from(bound_scalar(&mut self.ids.query_id, QUERY_ID_BOUND));
        cut += u64::from(bound_scalar(&mut self.ids.audit_ref, AUDIT_REF_BOUND));
        cut += u64::from(bound_scalar(
            &mut self.visibility.snapshot_id,
            SNAPSHOT_ID_BOUND,
        ));
        cut += u64::from(bound_scalar(
            &mut self.visibility.ingest_watermark_hour,
            INGEST_WATERMARK_HOUR_BOUND,
        ));
        if let Some(approximation) = &mut self.accuracy.approximation {
            cut += u64::from(bound_scalar(approximation, APPROXIMATION_BOUND));
        }
        if let Some(failure) = &mut self.failure
            && let Some(counter) = &mut failure.counter
        {
            cut += u64::from(bound_scalar(counter, FAILURE_COUNTER_BOUND));
        }
        let over = self
            .scalar_serialized_len()
            .saturating_sub(SCALAR_ALLOWANCE);
        if over > 0
            && let Some(plan) = &mut self.plan
        {
            cut += u64::from(shrink_scalar(plan, over));
        }
        let over = self
            .scalar_serialized_len()
            .saturating_sub(SCALAR_ALLOWANCE);
        if over > 0
            && let Some(failure) = &mut self.failure
        {
            cut += u64::from(shrink_scalar(&mut failure.message, over));
        }
        // A budget value is JSON of a shape this module does not own, so an
        // over-allowance one is replaced whole by the marker string rather
        // than cut into JSON that no longer parses. A value already smaller
        // than the marker is left alone: replacing it would grow the
        // envelope.
        for index in 0..3 {
            if self.scalar_serialized_len() <= SCALAR_ALLOWANCE {
                break;
            }
            let value = match index {
                0 => &mut self.budget.effective,
                1 => &mut self.budget.actual,
                _ => &mut self.budget.estimate,
            };
            if entry_serialized_len(value) > MARKER_SERIALIZED_LEN {
                *value = AnyJson(Value::String(TRUNCATION_MARKER.to_string()));
                cut += 1;
            }
        }
        cut
    }

    /// The D4 byte-cap algorithm. Floors `max_response_bytes` at
    /// [`MAX_RESPONSE_BYTES_FLOOR`], caps the scalars to their allowance,
    /// caps every metadata list to its bound, then drops rows from the end
    /// of `data.rows` until the envelope fits. If a single remaining row
    /// still does not fit, keeps it and shortens its oversized cells instead
    /// of dropping it, so `data.rows` is never empty while `rows_omitted` is
    /// positive and a retained row always fits.
    ///
    /// A cursor over [`CURSOR_BOUND`] is checked before any of that, and is
    /// the one input this method answers with a failure rather than a cut.
    /// The bound is the cursor's own, so nothing a caller asked for can push
    /// a token past it: only this process minting one over its own limit can,
    /// which is a defect, and an `internal` failure is what says so. Cutting
    /// a MAC'd token is not an option (a cut cursor redeems as invalid), and
    /// dropping it silently would return a page the caller cannot turn while
    /// reporting nothing wrong. It only writes that failure when the envelope
    /// carries none already: an envelope that arrives here reporting a
    /// `budget_exceeded` or an `unavailable` is reporting why the call failed,
    /// and replacing that with `internal` would tell the caller to file a bug
    /// about this process instead of retrying or narrowing. The cursor is
    /// dropped either way, so no unturnable page goes out.
    ///
    /// On that second path the defect is still reported, through the two
    /// channels that displace nothing: `presentation.metadata_elided` counts
    /// the dropped cursor, and a warning names it. A drop that wrote neither
    /// would leave the one input this method treats as a defect invisible on
    /// every envelope that already carries a failure, which is the reverse of
    /// what the bound is for. The warning goes in at the front of `warnings`
    /// so the bound on that list cannot be what drops it.
    ///
    /// Scalars are capped after that check because the failure message it
    /// writes is one of the scalars the allowance covers.
    ///
    /// The one case that overrides the first-row guarantee is a row nothing
    /// can shorten enough: a row of cells that are all at their own type's
    /// floor (numbers, booleans, hex ids) can still be wider than the cap,
    /// and no cut reclaims a byte of it. Rather than return an envelope over
    /// the cap, `fit` drops that row too and counts it. This is what makes
    /// `fit` total, and it needs no failure channel because the fixed part
    /// that remains is under [`MAXIMAL_FIXED_PART`], which is under the
    /// smallest cap `fit` can be given.
    ///
    /// Every decision below (the already-fits check, the row-packing target,
    /// the first-row shortening) is made against `cap`, which is the
    /// caller's resolved [`Envelope::presentation`]`.effective_max_response_bytes`
    /// minus [`IDENTITY_WARNINGS_ALLOWANCE`], not that figure itself. A
    /// row-packed envelope this method measures as sitting exactly at the
    /// reported cap has no slack of its own left for
    /// [`Envelope::finish`](Self::finish) to spend on the identity warnings
    /// it adds afterward; reserving that room here, once, is what keeps the
    /// envelope `finish` returns inside the cap it reports, without `finish`
    /// having to re-measure or re-cut anything.
    pub fn fit(mut self, requested_max_response_bytes: u64) -> Envelope {
        let cursor_len = self.cursor_serialized_len();
        let mut cursor_dropped_over_bound = false;
        if cursor_len > CURSOR_BOUND {
            self.presentation.cursor = None;
            self.status = Status::Error;
            if self.failure.is_none() {
                self.failure = Some(Failure {
                    class: FailureClass::Internal,
                    message: format!(
                        "cursor serializes to {cursor_len} B, over its {CURSOR_BOUND} B bound"
                    ),
                    counter: None,
                });
            } else {
                cursor_dropped_over_bound = true;
                self.warnings.insert(
                    0,
                    format!(
                        "cursor dropped: it serializes to {cursor_len} B, over its \
                         {CURSOR_BOUND} B bound; this is a server defect"
                    ),
                );
            }
        }

        self.presentation.scalars_truncated = self.cap_scalars();
        let caps = self.cap_metadata_lists();
        self.presentation.metadata_elided = caps.elided + u64::from(cursor_dropped_over_bound);
        self.presentation.entries_truncated = caps.entries_truncated;

        let effective_cap = requested_max_response_bytes.max(MAX_RESPONSE_BYTES_FLOOR);
        self.presentation.effective_max_response_bytes = effective_cap;
        self.presentation.floor_applied = requested_max_response_bytes < MAX_RESPONSE_BYTES_FLOOR;
        let cap = (effective_cap as usize).saturating_sub(IDENTITY_WARNINGS_ALLOWANCE);

        if serialized_len(&self) <= cap {
            self.presentation.bytes_cap_hit = false;
            self.presentation.rows_omitted = 0;
            self.presentation.cells_truncated = 0;
            return self;
        }
        self.presentation.bytes_cap_hit = true;

        // Measure the fixed part and every row's own serialized size once
        // each, rather than re-serializing the whole envelope on every
        // dropped row: `rows_fitting_prefix` turns those measurements into
        // the longest kept prefix with one linear scan.
        let row_lens: Vec<usize> = self.data.rows.iter().map(entry_serialized_len).collect();
        let saved_rows = std::mem::take(&mut self.data.rows);
        let fixed = serialized_len(&self);
        self.data.rows = saved_rows;

        let kept = rows_fitting_prefix(fixed, cap, &row_lens);
        let mut rows_omitted = (row_lens.len() - kept) as u64;
        self.data.rows.truncate(kept);
        self.presentation.rows_omitted = rows_omitted;

        // `rows_fitting_prefix`'s model is exact for this envelope: `rows`
        // serializes as a plain JSON array with one comma between adjacent
        // elements and no trailing separator, and no other field's
        // serialized size depends on how many rows are kept (`row_count` is
        // set independently by the caller, not derived here). So this
        // confirming call should never find the envelope still over `cap`;
        // it exists as a defensive check, and the existing shorten-then-clear
        // path below is what would absorb the difference if the model's
        // separator accounting were ever wrong.
        self.presentation.cells_truncated = self.shorten_first_row_to_fit(cap);

        if serialized_len(&self) > cap {
            rows_omitted += self.data.rows.len() as u64;
            self.data.rows.clear();
            self.presentation.rows_omitted = rows_omitted;
            // The cut cells left with the row they were in, so nothing in
            // what is returned was truncated.
            self.presentation.cells_truncated = 0;
        }
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

    /// Names every D4 identity field this envelope did not measure.
    ///
    /// D4 types `visibility.snapshot_id`, `visibility.ingest_watermark_hour`,
    /// `ids.query_id`, and `ids.audit_ref` as strings, so an operation that
    /// never resolved one serializes `""`. An empty string is a value: a
    /// caller cannot tell it apart from an id that really is empty, and a
    /// reader collecting snapshot ids across calls would collect blanks as if
    /// they were measurements. Saying in `warnings` that the field is not
    /// reported by this operation is the honest form.
    ///
    /// One empty field has a second reason, and it gets its own wording. The
    /// ingest watermark is the greatest ingest hour among the segments the
    /// operation resolved, so a resolve that returned none has nothing to
    /// report even though it measured. `visibility.resolved_no_segments` is
    /// how the operation says so, and it swaps
    /// [`IDENTITY_WARNING_SUFFIX`] for [`EMPTY_RESOLVE_WARNING_SUFFIX`] on
    /// that field alone. The two never both appear for it: the operation
    /// either reports the field or does not, and if it does not, exactly one
    /// of the two reasons holds.
    ///
    /// Every envelope that reaches a caller goes through [`finish`](Self::finish),
    /// which is what calls this: a crate that built its own envelope and
    /// called `fit` directly used to ship these four fields as silent empty
    /// strings with no warning, because the warning lived only in the one
    /// call path that ran `fit` and this check as separate steps.
    ///
    /// Inserted at the front, not appended: `finish` runs this after `fit`
    /// has already applied the D4 count bound to `warnings`, and re-applies
    /// that same bound afterward (see `finish`'s own doc comment). Putting
    /// the identity warnings first means they are what survives that second
    /// pass when a caller's own warnings already filled the list.
    fn warn_unreported_identity(&mut self) {
        let reported = [
            !self.visibility.snapshot_id.is_empty(),
            !self.visibility.ingest_watermark_hour.is_empty(),
            !self.ids.query_id.is_empty(),
            !self.ids.audit_ref.is_empty(),
        ];
        let empty_resolve = self.visibility.resolved_no_segments;
        let mut identity_warnings: Vec<String> = IDENTITY_FIELDS
            .iter()
            .enumerate()
            .zip(reported)
            .filter(|(_, reported)| !reported)
            .map(|((index, field), _)| {
                let suffix = if index == INGEST_WATERMARK_HOUR_IDENTITY_INDEX && empty_resolve {
                    EMPTY_RESOLVE_WARNING_SUFFIX
                } else {
                    IDENTITY_WARNING_SUFFIX
                };
                format!("{field}{suffix}")
            })
            .collect();
        if !identity_warnings.is_empty() {
            identity_warnings.append(&mut self.warnings);
            self.warnings = identity_warnings;
        }
    }

    /// Resolves `status` from what the caps and the cursor actually did, the
    /// last step before a result is returned.
    ///
    /// D4 defines three success values and they are not interchangeable
    /// (ADR-1374 D4, docs/reference/mcp.md#the-envelope):
    ///
    /// - `ok_page`: a cap stopped the result and a cursor exists, so the
    ///   caller can ask for the next page.
    /// - `ok_bounded`: a cap stopped the result and no cursor exists, so
    ///   more rows match than were returned and there is no way to reach
    ///   them but a narrower request.
    /// - `ok`: no cap stopped the result. A zero-row match and an unfilled
    ///   `LIMIT` are both complete results and both `ok`.
    ///
    /// "A cap stopped the result" means the cap took something away, not that
    /// a cap was consulted. `bytes_cap_hit` says the envelope was over the
    /// byte cap when `fit` measured it; if the caps that followed dropped no
    /// row and cut no cell, nothing is missing and the result is `ok`.
    /// Reporting `ok_bounded` there would tell the caller more rows match
    /// than came back, which is a claim about the data and would be false.
    ///
    /// `has_total_order` is the statement-level fact that decides whether a
    /// cursor may be handed out at all: D5 mints one only when the ordering
    /// plus its tiebreak is a total order, because a cursor over a partial
    /// order can skip or repeat rows at the page boundary. A caller of this
    /// method that has set `presentation.cursor` on a statement without a
    /// total order gets the cursor dropped and `ok_bounded`, not `ok_page`:
    /// the status and the cursor cannot disagree, and dropping is the safe
    /// direction. A cursor set when no cap stopped the result is dropped for
    /// the same reason -- there is no next page to point at.
    ///
    /// An `Error` envelope keeps its status and loses its cursor: a failure
    /// is never a page. Since a failed operation never measured anything,
    /// [`warn_unreported_identity`](Self::warn_unreported_identity) does not
    /// run on that path either.
    ///
    /// Every unmeasured identity field is named in `warnings` here, so every
    /// crate that builds an envelope and calls this method gets the warning
    /// once rather than each having to remember to ask for it. This runs
    /// after [`fit`](Self::fit) in every call site that uses both, so the
    /// bytes it adds are not run back through `fit`'s own caps; two things
    /// make that sound rather than an exception to them. `fit` reserves
    /// [`IDENTITY_WARNINGS_ALLOWANCE`] bytes of headroom below its cap for
    /// exactly this addition (see [`fit`](Self::fit)'s own doc comment), so
    /// the byte cap still holds once these warnings are in. And `warnings`
    /// itself is re-bounded to [`MAX_WARNINGS`] right here, the same
    /// [`truncate_vec`] call `fit`'s own metadata capping uses, with
    /// anything that still does not fit counted into
    /// `presentation.metadata_elided` exactly the way that capping counts
    /// every other over-the-bound entry; the identity warnings are inserted
    /// first, so they are what survives that bound when a caller's own
    /// warnings already filled the list.
    pub fn finish(mut self, has_total_order: bool) -> Envelope {
        if self.status == Status::Error || self.failure.is_some() {
            self.status = Status::Error;
            self.presentation.cursor = None;
            return self;
        }

        self.warn_unreported_identity();
        self.presentation.metadata_elided += truncate_vec(&mut self.warnings, MAX_WARNINGS);

        let bytes_cap_took_something =
            self.presentation.rows_omitted > 0 || self.presentation.cells_truncated > 0;
        let capped = self.presentation.row_cap_hit
            || (self.presentation.bytes_cap_hit && bytes_cap_took_something);
        if !has_total_order || !capped {
            self.presentation.cursor = None;
        }

        self.status = if self.presentation.cursor.is_some() {
            Status::OkPage
        } else if capped {
            Status::OkBounded
        } else {
            Status::Ok
        };
        self
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use proptest::prelude::*;

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

    /// Four rows of known serialized size (150,004 B each: a `Cell::Str` of
    /// 150,000 plain ASCII bytes is two bytes of array brackets plus two of
    /// quotes wider than its body). The fixed part here is 863 B, not the
    /// 859 B of a bare default envelope: `presentation.effective_max_response_bytes`
    /// and `presentation.bytes_cap_hit` are both set (to the requested cap
    /// and to `true`) before the row scan runs, and a 6-digit cap widens the
    /// first from 1 digit to 6 while `bytes_cap_hit` narrows from `false` to
    /// `true`, a net +4 B. From that fixed part the running total is 150,867
    /// after the first row and 300,872 after the second (one more byte for
    /// the comma between them). `fit` packs rows against its cap minus
    /// [`IDENTITY_WARNINGS_ALLOWANCE`] (the room it reserves for
    /// [`Envelope::finish`]'s identity warnings), so the requested cap here
    /// is that total plus the allowance: internally `fit` targets exactly
    /// 300,872 again, keeps exactly those two rows and omits the other two,
    /// and the fitted envelope's real serialized size lands on that same
    /// total, unaffected by the reservation because `finish` is never called
    /// in this test.
    #[test]
    fn row_prefix_is_chosen_from_measured_sizes() {
        let sizes = [150_000usize, 150_000, 150_000, 150_000];
        let mut envelope = Envelope::default();
        envelope.data.rows = sizes
            .iter()
            .map(|&n| vec![Cell::Str("a".repeat(n))])
            .collect();

        let fitted = envelope.fit(300_872 + IDENTITY_WARNINGS_ALLOWANCE as u64);

        assert_eq!(fitted.presentation.rows_omitted, 2);
        assert_eq!(fitted.data.rows.len(), 2);
        assert_eq!(fitted.presentation.cells_truncated, 0);
        assert_eq!(serialized_len(&fitted), 300_872);
    }

    /// 5,000 one-cell rows (the D4 `MAX_ROWS_CEILING`), each a small fixed
    /// size, under a cap that admits a precomputed count: with the same
    /// 863 B fixed part as above (the 256 KiB floor is a 6-digit cap too)
    /// and 204 B rows (a 200-byte `Cell::Str` plus its two bytes of
    /// brackets and two of quotes), the running total after `k` rows is
    /// `862 + 205*k`. `fit` packs rows against the floor minus
    /// [`IDENTITY_WARNINGS_ALLOWANCE`], not the floor itself, so the
    /// crossing point is one row earlier than it would be without that
    /// reservation: between the 1,273rd and 1,274th row -- except
    /// `rows_omitted` (3,727, four digits) is itself part of the returned
    /// envelope and widens it by 3 B over the one-digit `0` it held while
    /// `fixed` above was measured, so the returned envelope's real size is
    /// 3 B over that running total. This is the boundedness test the task
    /// calls for in place of counting `serialized_len` calls directly: there
    /// is no call counter to hook, so this instead pins the one thing a
    /// quadratic re-serialize-per-drop implementation could still get wrong
    /// at this row count -- the exact kept prefix -- without any wall-clock
    /// timing.
    #[test]
    fn row_prefix_scan_is_exact_at_the_row_count_ceiling() {
        let mut envelope = Envelope::default();
        envelope.data.rows = (0..5_000)
            .map(|_| vec![Cell::Str("b".repeat(200))])
            .collect();

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 1_273);
        assert_eq!(fitted.presentation.rows_omitted, 5_000 - 1_273);
        assert_eq!(fitted.presentation.cells_truncated, 0);
        assert_eq!(serialized_len(&fitted), 862 + 205 * 1_273 + 3);
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
    const KEPT_CELL_SERIALIZED_LEN: usize = 261_000;
    /// The whole envelope's exact serialized size for those same cases.
    const FITTED_ENVELOPE_SERIALIZED_LEN: usize = 261_897;

    /// `\n ` alternates a character that serializes to two bytes with one
    /// that serializes to one, and that is what makes the kept cell one byte
    /// wider here than for the pure quote body above. A cut lands on a source
    /// character boundary, so a body of nothing but two-byte escapes can only
    /// reach an even serialized length; a mixed body can also reach the odd
    /// one just below the budget. The two constants below are not required to
    /// agree, and today they do not.
    const KEPT_CONTROL_CELL_SERIALIZED_LEN: usize = 261_001;
    const FITTED_CONTROL_ENVELOPE_SERIALIZED_LEN: usize = 261_898;

    /// Serialized size of a 200-row page of one hex id column, every id at
    /// the 64-character bound: 13,801 B of rows (200 cells of 68 B, their
    /// commas, and the array brackets) plus 896 B of envelope fields, far
    /// under the 256 KiB floor. Pinned so a hex id that grew past its bound
    /// shows up here as a size change rather than as a cell `fit` silently
    /// skipped.
    const HEX_ID_PAGE_SERIALIZED_LEN: usize = 14_697;

    /// Serialized size of a zero-row envelope with every metadata field at
    /// both its D4 bounds, every scalar filling the D4 scalar allowance, and
    /// the cursor at the [`CURSOR_BOUND`] it now holds on its own: the largest
    /// fixed part the bounds permit.
    ///
    /// The 110,592 B tripwire below is this test's own, not a figure D4
    /// states: it is 108 KiB, the round number just above what the bounds
    /// measure to, so a bound that grows shows up as a failure here rather
    /// than as a quiet climb toward the floor. What D4 constrains is the
    /// per-list and per-entry bounds and the 4 KiB scalar allowance, whose
    /// sum with the cursor bound is 106 KiB. What actually guards the 256 KiB
    /// floor is the [`MAXIMAL_FIXED_PART`] assertion beside that constant,
    /// which adds the skeleton, its slack, and the identity-warning
    /// allowance on top of the documented bounds.
    const MAXIMAL_METADATA_ENVELOPE_LEN: usize = 109_909;
    const _: () = assert!(
        MAXIMAL_METADATA_ENVELOPE_LEN < 110_592,
        "the maximal fixed part must stay under this test's 108 KiB tripwire"
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
        assert_eq!(cell_len(&fitted), KEPT_CONTROL_CELL_SERIALIZED_LEN);
        let size = serialized_len(&fitted);
        assert_eq!(size, FITTED_CONTROL_ENVELOPE_SERIALIZED_LEN);
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

    /// A hex id is bounded at construction rather than cut at `fit` time, so
    /// anything over 64 characters is refused with a typed error and never
    /// becomes a cell. The alphabet is checked too: an uppercase or non-hex
    /// character is not a hex id, and admitting one would leave the 64 B
    /// claim resting on what the caller happened to pass.
    #[test]
    fn hex_id_longer_than_64_bytes_is_refused() {
        assert_eq!(MAX_HEX_ID_LEN, 64);
        let at_bound = "a".repeat(MAX_HEX_ID_LEN);
        let id = HexId::new(at_bound.clone()).expect("64 characters of hex is a hex id");
        assert_eq!(id.as_str(), at_bound);
        assert_eq!(serialized_str_len(id.as_str()), 66);

        assert_eq!(
            HexId::new("a".repeat(MAX_HEX_ID_LEN + 1)),
            Err(HexIdError::TooLong { len: 65, max: 64 })
        );
        assert_eq!(
            Cell::hex_id("f".repeat(1024 * 1024)),
            Err(HexIdError::TooLong {
                len: 1_048_576,
                max: 64,
            })
        );
        assert_eq!(
            HexId::new("00ab7F"),
            Err(HexIdError::NotLowercaseHex { found: 'F' })
        );
        assert_eq!(
            HexId::new("00ab 7f"),
            Err(HexIdError::NotLowercaseHex { found: ' ' })
        );
        assert_eq!(
            HexId::new("00ab\n7f"),
            Err(HexIdError::NotLowercaseHex { found: '\n' })
        );
    }

    /// `fit` skipping hex id cells is sound only because no oversized hex id
    /// can exist: through the public API there is no path from a hostile
    /// 1 MiB "id" to a cell, and a full 200-row page of hex ids at the bound
    /// stays under the floor cap with nothing dropped and nothing cut.
    #[test]
    fn oversized_hex_id_cannot_reach_the_envelope() {
        let hostile = "b".repeat(1024 * 1024);
        assert!(
            Cell::hex_id(hostile).is_err(),
            "no cell may carry an oversized hex id"
        );

        let maximal = Cell::hex_id("c".repeat(MAX_HEX_ID_LEN)).expect("at the bound");
        let envelope = envelope_with_rows(200, |_| vec![maximal.clone()]);

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.data.rows.len(), 200);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.presentation.cells_truncated, 0);
        assert!(!fitted.presentation.bytes_cap_hit);
        assert_eq!(serialized_len(&fitted), HEX_ID_PAGE_SERIALIZED_LEN);
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

    /// The D4 scalar allowance, over the fields that have no natural size.
    ///
    /// Every scalar with a natural size is cut to its own sub-bound first
    /// (nine of the ten checked). A 1 MiB `plan` is then cut by the whole
    /// remaining overshoot, which lands it on the truncation marker, and the
    /// failure message beside it absorbs what is still over. The cursor takes
    /// no part in any of it: it has its own bound and is not one of the
    /// scalars this allowance covers.
    #[test]
    fn oversized_plan_is_cut_to_the_scalar_allowance() {
        let mut envelope = Envelope {
            plan: Some("p".repeat(1024 * 1024)),
            failure: Some(Failure {
                class: FailureClass::Internal,
                message: "m".repeat(4096),
                counter: Some("c".repeat(4096)),
            }),
            ..Default::default()
        };
        envelope.scope.signal = "s".repeat(4096);
        envelope.scope.table = "t".repeat(4096);
        envelope.scope.time_range = Some(TimeRange {
            start_ns: "1".repeat(100),
            end_ns: "2".repeat(100),
        });
        envelope.ids.query_id = "q".repeat(4096);
        envelope.ids.audit_ref = "a".repeat(4096);
        envelope.visibility.snapshot_id = "v".repeat(4096);
        // The decimal unix hour, which is what this field carries: 496896 is
        // 2026-09-08T00:00:00Z. Not YYYYMMDDHH, and not the catalog's
        // YYYYMMDDTHH key text.
        envelope.visibility.ingest_watermark_hour = "496896".to_string();
        envelope.accuracy.approximation = Some("x".repeat(4096));
        envelope.presentation.cursor = Some("k".repeat(3 * 1024));
        envelope.budget.effective = AnyJson(Value::String("b".repeat(1000)));
        envelope.budget.estimate = AnyJson(Value::Number(7.into()));

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        // Nine scalars cut to their own sub-bound, the plan cut to the
        // marker, and the failure message cut to what is left of the
        // allowance. The cursor is untouched and uncounted.
        assert_eq!(fitted.presentation.scalars_truncated, 11);
        assert_eq!(fitted.scalar_serialized_len(), SCALAR_ALLOWANCE);

        assert_eq!(fitted.plan.as_deref(), Some(TRUNCATION_MARKER));
        assert_eq!(
            fitted.presentation.cursor.as_deref(),
            Some("k".repeat(3 * 1024).as_str())
        );
        assert!(fitted.warnings.is_empty());
        assert!(fitted.next_steps.is_empty());
        let failure = fitted.failure.as_ref().expect("the failure is kept");
        // Cut, but not to the marker: with the cursor no longer counted
        // against the allowance there is room left for the message's own
        // text once the plan is at its floor.
        assert!(failure.message.ends_with(TRUNCATION_MARKER));
        assert_eq!(serialized_str_len(&failure.message), 2_041);
        assert_eq!(
            serialized_str_len(failure.counter.as_deref().expect("a counter")),
            FAILURE_COUNTER_BOUND
        );
        assert_eq!(serialized_str_len(&fitted.scope.signal), SIGNAL_BOUND);
        assert_eq!(serialized_str_len(&fitted.scope.table), TABLE_BOUND);
        let range = fitted.scope.time_range.as_ref().expect("a time range");
        assert_eq!(serialized_str_len(&range.start_ns), TIME_RANGE_BOUND);
        assert_eq!(serialized_str_len(&range.end_ns), TIME_RANGE_BOUND);
        assert_eq!(serialized_str_len(&fitted.ids.query_id), QUERY_ID_BOUND);
        assert_eq!(serialized_str_len(&fitted.ids.audit_ref), AUDIT_REF_BOUND);
        assert_eq!(
            serialized_str_len(&fitted.visibility.snapshot_id),
            SNAPSHOT_ID_BOUND
        );
        assert_eq!(
            serialized_str_len(
                fitted
                    .accuracy
                    .approximation
                    .as_deref()
                    .expect("an approximation")
            ),
            APPROXIMATION_BOUND
        );
        // Already inside their bounds, so untouched by the cut.
        assert_eq!(fitted.visibility.ingest_watermark_hour, "496896");
        // The stage before it brought the scalars inside the allowance, so
        // the budget values are never reached.
        assert_eq!(
            fitted.budget.effective,
            AnyJson(Value::String("b".repeat(1000)))
        );
        assert_eq!(fitted.budget.estimate, AnyJson(Value::Number(7.into())));
    }

    /// The cursor's bound is its own, so a caller's own scalars cannot crowd
    /// it out: a 3 KiB cursor next to a `plan` large enough to fill the whole
    /// scalar allowance survives untouched, the plan absorbs the entire
    /// overshoot, and the page finishes as `ok_page`.
    #[test]
    fn a_cursor_within_its_bound_survives_a_full_scalar_allowance() {
        let cursor = "k".repeat(3 * 1024);
        let mut envelope = Envelope {
            plan: Some("p".repeat(8 * 1024)),
            ..Default::default()
        };
        envelope.presentation.cursor = Some(cursor.clone());
        envelope.presentation.row_cap_hit = true;

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(
            fitted.presentation.scalars_truncated, 1,
            "the plan alone was cut"
        );
        let plan = fitted.plan.as_deref().expect("the plan is kept");
        assert!(plan.ends_with(TRUNCATION_MARKER));
        // The whole allowance less the other scalars of a default envelope.
        // A cursor counted against the allowance would take 3,074 B off this.
        assert_eq!(serialized_str_len(plan), 4_072);
        assert_eq!(
            fitted.scalar_serialized_len(),
            SCALAR_ALLOWANCE,
            "the scalars sit exactly on their allowance"
        );
        assert_eq!(fitted.presentation.cursor.as_deref(), Some(cursor.as_str()));
        assert_eq!(fitted.cursor_serialized_len(), 3 * 1024 + 2);
        assert!(fitted.failure.is_none());
        assert!(fitted.warnings.is_empty());
        assert!(fitted.next_steps.is_empty());

        let finished = fitted.finish(true);
        assert_eq!(finished.status, Status::OkPage);
        assert_eq!(
            finished.presentation.cursor.as_deref(),
            Some(cursor.as_str())
        );
    }

    /// A cursor over its own bound is not something a caller asked for and
    /// not something a cut can fix: only this process minting a token past
    /// its own limit produces one, so it is an `internal` failure. The cursor
    /// is dropped (a corrupt or over-bound token is worse than none) and the
    /// envelope finishes as an error rather than as a page.
    ///
    /// One byte over is enough: the check is on the bound, not on a margin.
    #[test]
    fn an_over_bound_cursor_is_an_internal_failure() {
        // Two of the serialized bytes are the JSON quotes, so this is exactly
        // one byte over CURSOR_BOUND.
        let cursor = "k".repeat(CURSOR_BOUND - 1);
        let mut envelope = Envelope::default();
        envelope.presentation.cursor = Some(cursor);
        envelope.presentation.row_cap_hit = true;

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.presentation.cursor, None);
        assert_eq!(fitted.cursor_serialized_len(), 0);
        let failure = fitted.failure.as_ref().expect("an internal failure");
        assert_eq!(failure.class, FailureClass::Internal);
        assert_eq!(
            failure.message,
            "cursor serializes to 4097 B, over its 4096 B bound"
        );
        assert_eq!(failure.counter, None);
        assert!(
            fitted.warnings.is_empty(),
            "an over-bound cursor is a failure, not a warning"
        );
        assert!(fitted.next_steps.is_empty());

        let finished = fitted.finish(true);
        assert_eq!(finished.status, Status::Error);
        assert_eq!(finished.presentation.cursor, None);
    }

    /// The same over-bound cursor on an envelope that is already reporting a
    /// failure. The cursor is still dropped, but the existing failure's class,
    /// message, and counter survive: a `budget_exceeded` that turned into an
    /// `internal` would tell the caller to file a bug about this process
    /// instead of narrowing the query that actually tripped, and the counter
    /// naming which budget tripped would be gone with it.
    ///
    /// Keeping that class is not licence to drop the cursor quietly. The
    /// defect is still reported through the two channels that displace nothing:
    /// `metadata_elided` counts the drop and a warning names it. Without them
    /// this envelope would be indistinguishable from one that never had a
    /// cursor, which is the state the bound exists to make visible.
    #[test]
    fn an_over_bound_cursor_on_a_failed_envelope_is_still_observable() {
        let cursor = "k".repeat(CURSOR_BOUND - 1);
        let mut envelope = Envelope::default();
        envelope.presentation.cursor = Some(cursor);
        envelope.presentation.row_cap_hit = true;
        envelope.status = Status::Error;
        envelope.failure = Some(Failure {
            class: FailureClass::BudgetExceeded,
            message: "scanned 4 GiB, over the 1 GiB max_bytes_scanned".to_string(),
            counter: Some("bytes_scanned".to_string()),
        });

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.presentation.cursor, None);
        assert_eq!(fitted.cursor_serialized_len(), 0);
        let failure = fitted.failure.as_ref().expect("the original failure");
        assert_eq!(failure.class, FailureClass::BudgetExceeded);
        assert_eq!(
            failure.message,
            "scanned 4 GiB, over the 1 GiB max_bytes_scanned"
        );
        assert_eq!(failure.counter.as_deref(), Some("bytes_scanned"));
        assert_eq!(fitted.status, Status::Error);
        assert_eq!(fitted.presentation.scalars_truncated, 0);

        assert_eq!(
            fitted.presentation.metadata_elided, 1,
            "the dropped cursor is the one elided field"
        );
        assert_eq!(fitted.warnings.len(), 1);
        assert_eq!(
            fitted.warnings[0],
            "cursor dropped: it serializes to 4097 B, over its 4096 B bound; \
             this is a server defect"
        );

        // `finish` returns an error envelope untouched, so both channels are
        // still there when the caller reads the result.
        let finished = fitted.finish(true);
        assert_eq!(finished.presentation.metadata_elided, 1);
        assert_eq!(finished.warnings.len(), 1);
    }

    /// The last byte that is not over: a cursor serializing to exactly
    /// [`CURSOR_BOUND`] is carried, so the refusal above is attributable to
    /// the bound rather than to being merely large.
    #[test]
    fn a_cursor_exactly_at_its_bound_is_carried() {
        let cursor = "k".repeat(CURSOR_BOUND - 2);
        let mut envelope = Envelope::default();
        envelope.presentation.cursor = Some(cursor.clone());
        envelope.presentation.row_cap_hit = true;

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.cursor_serialized_len(), CURSOR_BOUND);
        assert_eq!(fitted.presentation.cursor.as_deref(), Some(cursor.as_str()));
        assert!(fitted.failure.is_none());
        assert_eq!(fitted.presentation.scalars_truncated, 0);
        assert_eq!(fitted.finish(true).status, Status::OkPage);
    }

    /// The last stage of the scalar cut: three budget values that are over
    /// the allowance together are replaced whole by the marker string, in
    /// order, and only until the scalars fit. A budget value is JSON of a
    /// shape D6 owns, so cutting it as text would produce something that no
    /// longer parses.
    #[test]
    fn oversized_budget_values_are_replaced_by_the_marker() {
        let mut envelope = Envelope::default();
        let big = AnyJson(serde_json::json!({ "scanned_bytes": "b".repeat(4096) }));
        envelope.budget.effective = big.clone();
        envelope.budget.actual = big.clone();
        envelope.budget.estimate = big;

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);

        assert_eq!(fitted.presentation.scalars_truncated, 3);
        let marker = AnyJson(Value::String(TRUNCATION_MARKER.to_string()));
        assert_eq!(fitted.budget.effective, marker);
        assert_eq!(fitted.budget.actual, marker);
        assert_eq!(fitted.budget.estimate, marker);
        assert_eq!(fitted.scalar_serialized_len(), 60);
    }

    /// Zero rows, every metadata list at its count bound, every entry at its
    /// per-entry bound, every scalar filling the D4 scalar allowance exactly,
    /// and the cursor at [`CURSOR_BOUND`]: the fixed part alone must serialize
    /// to exactly [`MAXIMAL_METADATA_ENVELOPE_LEN`], which must stay under
    /// 110,592 B.
    ///
    /// Each entry below is sized to land on its bound exactly, so the
    /// envelope this builds is the largest one the D4 bounds permit, and
    /// neither `cap_metadata_lists` nor `cap_scalars` may find anything to
    /// do. A 10 MiB warning and a 1 MiB plan fed in afterwards are cut back,
    /// so nothing a caller or the engine can produce moves this figure up.
    #[test]
    fn zero_row_envelope_with_maximal_metadata_fits_under_the_floor() {
        assert_eq!(
            serialized_len(&Envelope::default()),
            EMPTY_ENVELOPE_SERIALIZED_LEN
        );

        let mut envelope = Envelope::default();
        envelope.data.columns = (0..MAX_PROJECTION_COLUMNS)
            .map(|i| Column {
                name: format!("{:0>4}{}", i, "n".repeat(105)),
                r#type: "t".repeat(30),
            })
            .collect();
        envelope.data.row_count = 0;
        envelope.scope.signal = "s".repeat(SIGNAL_BOUND - 2);
        envelope.scope.table = "t".repeat(TABLE_BOUND - 2);
        envelope.scope.time_range = Some(TimeRange {
            start_ns: "1".repeat(TIME_RANGE_BOUND - 2),
            end_ns: "2".repeat(TIME_RANGE_BOUND - 2),
        });
        envelope.scope.predicates_applied = (0..MAX_PREDICATES_APPLIED)
            .map(|i| format!("{i:0>2}_{}", "p".repeat(507)))
            .collect();
        envelope.scope.order_by = (0..MAX_ORDER_BY)
            .map(|i| format!("{i:0>2}_{}", "o".repeat(507)))
            .collect();
        envelope.ids.query_id = "q".repeat(QUERY_ID_BOUND - 2);
        envelope.ids.audit_ref = "a".repeat(AUDIT_REF_BOUND - 2);
        envelope.visibility.snapshot_id = "v".repeat(SNAPSHOT_ID_BOUND - 2);
        envelope.visibility.ingest_watermark_hour = "h".repeat(INGEST_WATERMARK_HOUR_BOUND - 2);
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
        envelope.accuracy.approximation = Some("x".repeat(APPROXIMATION_BOUND - 2));
        envelope.presentation.max_rows = 200;
        // The cursor is at CURSOR_BOUND, which it occupies on its own rather
        // than out of the scalar allowance.
        envelope.presentation.cursor = Some("k".repeat(CURSOR_BOUND - 2));
        // The longest failure class, so the block's own keys and value are at
        // their widest too.
        envelope.failure = Some(Failure {
            class: FailureClass::BudgetEstimateExceedsCeiling,
            message: "m".repeat(298),
            counter: Some("c".repeat(FAILURE_COUNTER_BOUND - 2)),
        });
        // The plan takes up the slack the cursor left when it moved out of the
        // scalar allowance, so the scalars still land on it exactly.
        envelope.plan = Some("p".repeat(2546));
        envelope.budget.effective = AnyJson(Value::String("b".repeat(62)));
        envelope.budget.actual = AnyJson(Value::String("a".repeat(62)));
        envelope.budget.estimate = AnyJson(Value::String("e".repeat(62)));
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
                r#ref: format!("{i:0>2}_{}", "r".repeat(398)),
                covers: "data.rows".to_string(),
                blake3_256: "0".repeat(64),
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

        assert_eq!(
            envelope.scalar_serialized_len(),
            SCALAR_ALLOWANCE,
            "the scalars fill the allowance exactly"
        );
        assert_eq!(
            envelope.cursor_serialized_len(),
            CURSOR_BOUND,
            "the cursor fills its own bound exactly"
        );

        let caps = envelope.cap_metadata_lists();
        assert_eq!(
            caps,
            MetadataCaps::default(),
            "every list is already at its bound, not over it"
        );
        assert_eq!(
            envelope.cap_scalars(),
            0,
            "every scalar is already at its bound, not over it"
        );

        assert_eq!(serialized_len(&envelope), MAXIMAL_METADATA_ENVELOPE_LEN);
        const {
            assert!(MAXIMAL_METADATA_ENVELOPE_LEN <= MAXIMAL_FIXED_PART);
        }

        envelope.warnings[0] = "w".repeat(10 * 1024 * 1024);
        envelope.plan = Some("p".repeat(1024 * 1024));
        let caps = envelope.cap_metadata_lists();
        assert_eq!(
            caps,
            MetadataCaps {
                elided: 0,
                entries_truncated: 1,
            }
        );
        assert_eq!(envelope.cap_scalars(), 1, "the plan alone was over");
        assert_eq!(
            serialized_str_len(&envelope.warnings[0]),
            WARNING_ENTRY_BOUND
        );
        let plan = envelope.plan.as_deref().expect("the plan is kept");
        assert!(plan.ends_with(TRUNCATION_MARKER));
        assert_eq!(serialized_str_len(plan), 2_548);
        // The plan is cut by exactly the overshoot, so it lands back on the
        // 2,548 B it occupied before: neither the 10 MiB warning nor the 1 MiB
        // plan moves the figure at all.
        assert_eq!(serialized_len(&envelope), MAXIMAL_METADATA_ENVELOPE_LEN);
    }

    /// One cell of every kind D4 defines, at sizes from empty to far past the
    /// cap. The three string shapes are the three escaping regimes: a plain
    /// body, a body of quotes (two serialized bytes per source byte), and a
    /// body of control characters (six).
    fn cell_strategy() -> impl Strategy<Value = Cell> {
        prop_oneof![
            Just(Cell::Null),
            any::<bool>().prop_map(Cell::Bool),
            // Both halves of the carrier's range, since a u64 above i64::MAX
            // is the case that made it wider than an i64.
            any::<i64>().prop_map(|n| Cell::Int(i128::from(n))),
            any::<u64>().prop_map(|n| Cell::Int(i128::from(n))),
            any::<i64>().prop_map(Cell::Timestamp),
            any::<f64>().prop_map(Cell::Float),
            (0usize..=MAX_HEX_ID_LEN)
                .prop_map(|len| Cell::HexId(HexId::new("a".repeat(len)).expect("a hex id"))),
            (0usize..3, 0usize..30_000).prop_map(|(shape, len)| {
                let body = match shape {
                    0 => "x",
                    1 => "\"",
                    _ => "\u{1}",
                };
                Cell::Str(body.repeat(len))
            }),
            (0usize..8, 0usize..8_000).prop_map(|(keys, len)| {
                let mut map = Map::new();
                for key in 0..keys {
                    map.insert(format!("k{key}"), Value::String("\"".repeat(len)));
                }
                Cell::Map(map)
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// `fit` is total: whatever the one row holds, the envelope it
        /// returns is inside the cap it reports. The generated row includes
        /// the shape no cut can reach -- a run of integer cells, each already
        /// at its own type's floor -- which is over the cap by width alone
        /// and can only be fitted by dropping the row.
        #[test]
        fn fit_never_returns_over_cap_for_any_single_row(
            cells in prop::collection::vec(cell_strategy(), 0..8),
            uncuttable in 0usize..30_000,
            requested_cap in 0u64..(400 * 1024),
        ) {
            let mut row: Row = cells;
            row.extend((0..uncuttable).map(|i| Cell::Int(i as i128 * 1_000_000_009)));
            let mut envelope = Envelope::default();
            envelope.data.columns = (0..row.len().min(MAX_PROJECTION_COLUMNS))
                .map(|i| Column {
                    name: format!("c{i}"),
                    r#type: "string".to_string(),
                })
                .collect();
            envelope.data.rows = vec![row];
            envelope.data.row_count = 1;

            let fitted = envelope.fit(requested_cap);

            let cap = fitted.presentation.effective_max_response_bytes as usize;
            prop_assert!(cap >= MAX_RESPONSE_BYTES_FLOOR as usize);
            prop_assert!(fitted.data.rows.len() <= 1);
            let size = serialized_len(&fitted);
            prop_assert!(size <= cap, "serialized size {size} exceeds cap {cap}");
        }
    }

    /// `floor_applied` reports whether the floor changed the caller's cap, so
    /// it is true only strictly below the floor. At the floor exactly, and at
    /// the 512 KiB default above it, nothing was raised and the flag is false.
    #[test]
    fn floor_applied_is_true_only_when_the_floor_raised_the_cap() {
        let fitted = Envelope::default().fit(1_024);
        assert!(fitted.presentation.floor_applied);
        assert_eq!(
            fitted.presentation.effective_max_response_bytes,
            MAX_RESPONSE_BYTES_FLOOR
        );
        assert_eq!(fitted.presentation.effective_max_response_bytes, 256 * 1024);

        let fitted = Envelope::default().fit(MAX_RESPONSE_BYTES_FLOOR - 1);
        assert!(fitted.presentation.floor_applied);
        assert_eq!(
            fitted.presentation.effective_max_response_bytes,
            MAX_RESPONSE_BYTES_FLOOR
        );

        let fitted = Envelope::default().fit(MAX_RESPONSE_BYTES_FLOOR);
        assert!(!fitted.presentation.floor_applied);
        assert_eq!(
            fitted.presentation.effective_max_response_bytes,
            MAX_RESPONSE_BYTES_FLOOR
        );

        let fitted = Envelope::default().fit(512 * 1024);
        assert!(!fitted.presentation.floor_applied);
        assert_eq!(fitted.presentation.effective_max_response_bytes, 512 * 1024);

        let fitted = Envelope::default().fit(4 * 1024 * 1024);
        assert!(!fitted.presentation.floor_applied);
        assert_eq!(
            fitted.presentation.effective_max_response_bytes,
            4 * 1024 * 1024
        );
    }

    /// 2^53 + 1 does not round-trip through `f64`; the wire form must be a
    /// JSON string so the exact integer survives. A nanosecond timestamp is
    /// the same rule applied to the same representation.
    ///
    /// Every digit string is asserted literally. Parsing the cell back into
    /// an integer and comparing would agree with any serialization the same
    /// parser accepts, including one that dropped or added a digit the
    /// parser then re-derived, and it cannot check a value the parser's own
    /// type has no room for.
    #[test]
    fn integers_and_timestamps_serialize_as_strings() {
        let row = vec![
            Cell::Int(i128::from((1u64 << 53) + 1)),
            Cell::Timestamp(1_700_000_000_123_456_789),
            // Both ends of the carrier's range, and the first value above
            // i64::MAX, which is what a u64 column can reach.
            Cell::Int(i128::from(u64::MAX)),
            Cell::Int(i128::from(i64::MAX as u64 + 1)),
            Cell::Int(i128::from(i64::MIN)),
            // A small integer travels the identical path, so the string form
            // is the rule for every magnitude and not a large-value escape.
            Cell::Int(1),
        ];

        let value = serde_json::to_value(&row).expect("row serializes");
        let cells = value.as_array().expect("row is a JSON array");
        let text: Vec<&str> = cells
            .iter()
            .map(|cell| cell.as_str().expect("every cell must be a JSON string"))
            .collect();

        assert_eq!(
            text,
            vec![
                "9007199254740993",
                "1700000000123456789",
                "18446744073709551615",
                "9223372036854775808",
                "-9223372036854775808",
                "1",
            ]
        );
    }

    /// The widest integer the carrier holds is still a number, so `fit` never
    /// cuts it, however small the per-cell budget gets. A string cell of the
    /// same serialized size under the same budget is cut, which is what makes
    /// this a claim about the variant rather than about the size.
    #[test]
    fn an_oversized_integer_cell_is_never_shortened() {
        let widest = i128::from(u64::MAX);
        let equally_long = "1".repeat(u64::MAX.to_string().len());
        assert_eq!(
            serialized_str_len(&widest.to_string()),
            serialized_str_len(&equally_long)
        );
        let mut row = vec![Cell::Int(widest), Cell::Str(equally_long)];

        // One byte under the 22 B both cells serialize to.
        let truncated = shorten_row(&mut row, 21);

        assert_eq!(truncated, 1, "the string alone was cut");
        assert_eq!(row[0], Cell::Int(widest));
        assert_eq!(row[1], Cell::Str(format!("11111{TRUNCATION_MARKER}")));
    }

    /// JSON has no NaN or infinity, so the three non-finite floats travel
    /// as the strings PromQL already uses for them, and a NaN payload
    /// collapses to the same `"NaN"` rather than to null. `-0.0` is the
    /// opposite case: it is a finite number and must stay one, with its
    /// sign, because `-0.0` and `0.0` are distinct values in storage and
    /// dedup paths and the wire form is where the sign is lost.
    #[test]
    fn float_to_json_keeps_nan_inf_and_negative_zero() {
        assert_eq!(float_to_json(f64::NAN), Value::String("NaN".to_string()));
        assert_eq!(
            float_to_json(f64::from_bits(0x7ff8_0000_0000_0001)),
            Value::String("NaN".to_string())
        );
        assert_eq!(
            float_to_json(f64::INFINITY),
            Value::String("+Inf".to_string())
        );
        assert_eq!(
            float_to_json(f64::NEG_INFINITY),
            Value::String("-Inf".to_string())
        );

        let negative_zero = float_to_json(-0.0);
        let number = negative_zero
            .as_f64()
            .expect("-0.0 must stay a JSON number");
        assert_eq!(number.to_bits(), (-0.0f64).to_bits());
        assert_ne!(number.to_bits(), 0.0f64.to_bits());

        let row = vec![
            Cell::Float(f64::NAN),
            Cell::Float(f64::INFINITY),
            Cell::Float(f64::NEG_INFINITY),
            Cell::Float(-0.0),
            Cell::Float(0.0),
        ];
        assert_eq!(
            serde_json::to_string(&row).expect("row serializes"),
            r#"["NaN","+Inf","-Inf",-0.0,0.0]"#
        );
    }

    /// An envelope no cap stopped is `ok`, and it stays `ok` with zero rows:
    /// D4 counts a zero-row match and an unfilled `LIMIT` as complete
    /// results, not as bounded ones.
    #[test]
    fn finish_is_ok_when_no_cap_stopped_the_result() {
        let finished = Envelope::default().finish(true);
        assert_eq!(finished.status, Status::Ok);
        assert_eq!(finished.presentation.cursor, None);

        let mut envelope = envelope_with_rows(3, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.max_rows = 200;
        let finished = envelope.finish(true);
        assert_eq!(finished.status, Status::Ok);
        assert_eq!(finished.data.rows.len(), 3);
    }

    /// The row cap and the byte cap each stop a result on their own, and
    /// either one without a cursor is `ok_bounded`: more rows match than
    /// came back and nothing points at them.
    #[test]
    fn finish_is_ok_bounded_when_a_cap_stopped_the_result_without_a_cursor() {
        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.row_cap_hit = true;
        let finished = envelope.finish(false);
        assert_eq!(finished.status, Status::OkBounded);
        assert_eq!(finished.presentation.cursor, None);

        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.bytes_cap_hit = true;
        envelope.presentation.rows_omitted = 7;
        let finished = envelope.finish(false);
        assert_eq!(finished.status, Status::OkBounded);
        assert_eq!(finished.presentation.rows_omitted, 7);
    }

    /// A cap stopped the result, the statement has a total order, and a
    /// cursor was minted: that is the one combination that is `ok_page`, and
    /// the cursor survives for the caller to redeem.
    #[test]
    fn finish_is_ok_page_when_a_cap_stopped_the_result_and_a_cursor_exists() {
        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.row_cap_hit = true;
        envelope.presentation.cursor = Some("cursor-token".to_string());

        let finished = envelope.finish(true);

        assert_eq!(finished.status, Status::OkPage);
        assert_eq!(
            finished.presentation.cursor.as_deref(),
            Some("cursor-token")
        );
    }

    /// D5 mints a cursor only over a total order, because a cursor over a
    /// partial order can skip or repeat rows at the page boundary. A cursor
    /// handed to `finish` for a statement without one is dropped and the
    /// status degrades to `ok_bounded`; the status and the cursor never
    /// disagree.
    #[test]
    fn finish_drops_a_cursor_when_the_ordering_is_not_total() {
        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.row_cap_hit = true;
        envelope.presentation.cursor = Some("cursor-token".to_string());

        let finished = envelope.finish(false);

        assert_eq!(finished.status, Status::OkBounded);
        assert_eq!(finished.presentation.cursor, None);
    }

    /// A cursor with no cap behind it points at no next page, so it is
    /// dropped and the result is the plain `ok` it is.
    #[test]
    fn finish_drops_a_cursor_when_no_cap_stopped_the_result() {
        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.cursor = Some("cursor-token".to_string());

        let finished = envelope.finish(true);

        assert_eq!(finished.status, Status::Ok);
        assert_eq!(finished.presentation.cursor, None);
    }

    /// `bytes_cap_hit` alone is not a bounded result. The flag says the
    /// envelope was over the byte cap when `fit` measured it; when the caps
    /// that followed dropped no row and cut no cell, every matching row came
    /// back and the status is `ok`. A cursor set on such a result points at
    /// no next page and is dropped with it.
    ///
    /// One dropped row or one cut cell is enough to make the same envelope
    /// bounded, which is what keeps the flag from being decoration.
    #[test]
    fn finish_is_ok_when_the_cap_was_hit_but_nothing_was_dropped() {
        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.bytes_cap_hit = true;
        envelope.presentation.cursor = Some("cursor-token".to_string());

        let finished = envelope.finish(true);

        assert_eq!(finished.status, Status::Ok);
        assert_eq!(finished.presentation.cursor, None);

        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.bytes_cap_hit = true;
        envelope.presentation.rows_omitted = 1;
        assert_eq!(envelope.finish(false).status, Status::OkBounded);

        let mut envelope = envelope_with_rows(2, |i| vec![Cell::Int(i as i128)]);
        envelope.presentation.bytes_cap_hit = true;
        envelope.presentation.cells_truncated = 1;
        assert_eq!(envelope.finish(false).status, Status::OkBounded);
    }

    /// Every envelope that reaches a caller goes through `finish`, so the
    /// four D4 identity fields it left unmeasured are named in `warnings`
    /// there, whole and in order, regardless of which crate built the
    /// envelope or whether that crate calls a server-side wrapper around
    /// `finish` at all. A field the caller did measure is excluded, so the
    /// warning list is exactly the fields still missing.
    #[test]
    fn finish_warns_about_unmeasured_identity_fields() {
        let envelope = envelope_with_rows(1, |i| vec![Cell::Int(i as i128)]);

        let finished = envelope.finish(false);

        assert_eq!(
            finished.warnings,
            vec![
                "visibility.snapshot_id is not reported by this operation".to_string(),
                "visibility.ingest_watermark_hour is not reported by this operation".to_string(),
                "ids.query_id is not reported by this operation".to_string(),
                "ids.audit_ref is not reported by this operation".to_string(),
            ]
        );

        let mut measured = envelope_with_rows(1, |i| vec![Cell::Int(i as i128)]);
        measured.visibility.snapshot_id = "snap-7".to_string();

        let finished = measured.finish(false);

        assert_eq!(
            finished.warnings,
            vec![
                "visibility.ingest_watermark_hour is not reported by this operation".to_string(),
                "ids.query_id is not reported by this operation".to_string(),
                "ids.audit_ref is not reported by this operation".to_string(),
            ]
        );
    }

    /// An operation that resolved and got no segments back is not an
    /// operation that does not report freshness. The ingest watermark is the
    /// greatest ingest hour among the segments resolved, so an empty resolve
    /// leaves it with no value while having measured perfectly well, and that
    /// is precisely the state `ravel_describe_data` exists to report. A
    /// caller that reads the not-reported wording there concludes its
    /// tenant's freshness is unknowable through this tool rather than that
    /// the window it asked about is empty.
    ///
    /// Both strings are pinned whole, and asserted to differ: the agent
    /// corpus D8 requires keys on telling these two apart, so the wording is
    /// a contract rather than incidental prose.
    #[test]
    fn an_empty_resolve_warns_in_different_words_than_an_unmeasured_watermark() {
        const NOT_REPORTED: &str =
            "visibility.ingest_watermark_hour is not reported by this operation";
        const EMPTY_RESOLVE: &str =
            "visibility.ingest_watermark_hour is absent: this operation resolved no segments";
        assert_ne!(NOT_REPORTED, EMPTY_RESOLVE);

        let unmeasured = envelope_with_rows(1, |i| vec![Cell::Int(i as i128)]).finish(false);
        assert!(unmeasured.warnings.contains(&NOT_REPORTED.to_string()));
        assert!(!unmeasured.warnings.contains(&EMPTY_RESOLVE.to_string()));

        let mut empty = envelope_with_rows(0, |i| vec![Cell::Int(i as i128)]);
        empty.visibility.resolved_no_segments = true;
        let empty = empty.finish(false);
        assert_eq!(
            empty.warnings,
            vec![
                "visibility.snapshot_id is not reported by this operation".to_string(),
                EMPTY_RESOLVE.to_string(),
                "ids.query_id is not reported by this operation".to_string(),
                "ids.audit_ref is not reported by this operation".to_string(),
            ]
        );

        // A resolve that found segments reports the hour, and neither wording
        // applies: the flag only ever selects between the two reasons a field
        // is absent, and never suppresses one that is present.
        let mut measured = envelope_with_rows(1, |i| vec![Cell::Int(i as i128)]);
        measured.visibility.ingest_watermark_hour = "496896".to_string();
        measured.visibility.resolved_no_segments = true;
        let measured = measured.finish(false);
        assert!(!measured.warnings.contains(&NOT_REPORTED.to_string()));
        assert!(!measured.warnings.contains(&EMPTY_RESOLVE.to_string()));
    }

    /// An envelope that already carries `MAX_WARNINGS` warnings, each at the
    /// per-entry bound, and was fitted at the smallest cap `fit` can be
    /// given: adding the four identity warnings in `finish` must neither
    /// push `warnings` past its D4 count bound nor push the envelope's
    /// serialized size past the cap it was fitted to, even though nothing
    /// about this envelope has any headroom left on either dimension before
    /// `finish` runs.
    #[test]
    fn identity_warnings_are_inside_the_cap_and_the_count_bound() {
        let mut envelope = envelope_with_rows(1, |i| vec![Cell::Int(i as i128)]);
        envelope.warnings = (0..MAX_WARNINGS)
            .map(|i| format!("{i:0>2}_{}", "w".repeat(507)))
            .collect();
        for warning in &envelope.warnings {
            assert_eq!(serialized_str_len(warning), WARNING_ENTRY_BOUND);
        }

        let fitted = envelope.fit(MAX_RESPONSE_BYTES_FLOOR);
        assert_eq!(
            fitted.warnings.len(),
            MAX_WARNINGS,
            "nothing was elided yet"
        );

        let finished = fitted.finish(false);

        assert_eq!(finished.warnings.len(), MAX_WARNINGS);
        for field in IDENTITY_FIELDS {
            let expected = format!("{field}{IDENTITY_WARNING_SUFFIX}");
            assert!(
                finished.warnings.contains(&expected),
                "{expected:?} is missing from {:?}",
                finished.warnings
            );
        }
        assert!(serialized_len(&finished) <= MAX_RESPONSE_BYTES_FLOOR as usize);

        // The empty-resolve wording is the longer of the two messages the
        // watermark field can carry, so it is the one the allowance has to
        // cover. Same envelope, same cap, that message instead.
        let mut envelope = envelope_with_rows(1, |i| vec![Cell::Int(i as i128)]);
        envelope.warnings = (0..MAX_WARNINGS)
            .map(|i| format!("{i:0>2}_{}", "w".repeat(507)))
            .collect();
        envelope.visibility.resolved_no_segments = true;

        let finished = envelope.fit(MAX_RESPONSE_BYTES_FLOOR).finish(false);

        assert_eq!(finished.warnings.len(), MAX_WARNINGS);
        let expected = format!(
            "{}{EMPTY_RESOLVE_WARNING_SUFFIX}",
            IDENTITY_FIELDS[INGEST_WATERMARK_HOUR_IDENTITY_INDEX]
        );
        assert!(
            finished.warnings.contains(&expected),
            "{expected:?} is missing from {:?}",
            finished.warnings
        );
        assert!(serialized_len(&finished) <= MAX_RESPONSE_BYTES_FLOOR as usize);
    }

    /// A failure is never a page: an `Error` envelope keeps its status even
    /// with both caps set, and loses any cursor on it.
    #[test]
    fn finish_preserves_an_error_status_and_drops_its_cursor() {
        let mut envelope = envelope_with_rows(1, |i| vec![Cell::Int(i as i128)]);
        envelope.status = Status::Error;
        envelope.failure = Some(Failure {
            class: FailureClass::BudgetExceeded,
            message: "over budget".to_string(),
            counter: None,
        });
        envelope.presentation.row_cap_hit = true;
        envelope.presentation.bytes_cap_hit = true;
        envelope.presentation.cursor = Some("cursor-token".to_string());

        let finished = envelope.finish(true);

        assert_eq!(finished.status, Status::Error);
        assert_eq!(finished.presentation.cursor, None);
    }
}
