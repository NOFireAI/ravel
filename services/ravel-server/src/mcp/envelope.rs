//! One query service outcome, as a D4 envelope.
//!
//! [`crate::service::QueryService`] returns each operation's native outcome
//! type; the [`ravel_mcp::service::QueryBackend`] port returns the envelope.
//! This module is that conversion, once per operation shape, so
//! [`super::service_impl`] holds only the call itself.
//!
//! Two rules govern every function here. Rows carry the values the operation
//! actually produced, never a rendering that invents one: a native-histogram
//! element reports its histogram and a null float rather than the `0.0`
//! placeholder its in-memory form carries (ADR-0108). And every figure in the
//! `budget` block names its own basis: `bytes_scanned` is wire bytes as
//! transferred, the same counter the `max_bytes_scanned` ceiling is enforced
//! against, and `store_requests` is request count including retries and range
//! reads, which is what `max_store_requests` bounds.

use ravel_mcp::budget::McpEffectiveBudgets;
use ravel_mcp::envelope::{
    AnyJson, Budget, Cell, Column, Data, Envelope, Failure, FailureClass, Row, TimeRange,
};
use ravel_promql::{FloatHistogram, HistogramAwareMatrix, InstantVector, RangeMatrix};
use ravel_query::{PhaseAccountingSnapshot, QueryPhase};
use ravel_types::accounting::{CostEstimate, QueryAccountingSnapshot};
use ravel_types::{CommitToken, LabelSet, SeriesId};
use serde_json::{Map, Value, json};

use crate::service::{ServiceError, ServiceErrorKind};

/// What one operation spent, and whether its estimate is an upper envelope.
///
/// Both figures ride together because the envelope's `budget` block reports
/// them together and a reader comparing them has to know they came from the
/// same attempt.
pub(crate) struct Spend<'a> {
    pub accounting: &'a QueryAccountingSnapshot,
    pub estimate: &'a CostEstimate,
    /// The same requests and bytes `accounting` pools, split across the four
    /// phases that issued them. `None` for an operation whose outcome type
    /// carries no split, which is every metadata and exemplar operation.
    pub phases: Option<&'a PhaseAccountingSnapshot>,
    pub estimate_is_upper_envelope: bool,
}

/// A fresh envelope for one operation: its scope, the effective ceilings the
/// call resolved to, and nothing else filled in.
pub(crate) fn base(signal: &str, table: &str, budgets: &McpEffectiveBudgets) -> Envelope {
    let mut envelope = Envelope::default();
    envelope.scope.signal = signal.to_string();
    envelope.scope.table = table.to_string();
    envelope.presentation.max_rows = budgets.max_rows;
    envelope.accuracy.exact = true;
    envelope.coverage.complete = true;
    envelope.budget = Budget {
        effective: AnyJson(ceilings(budgets)),
        actual: AnyJson(Value::Object(Map::new())),
        estimate: AnyJson(Value::Object(Map::new())),
        estimate_is_upper_envelope: false,
    };
    envelope
}

/// The `[start_ns, end_ns]` window the operation ran over.
pub(crate) fn window(envelope: &mut Envelope, start_ns: i64, end_ns: i64) {
    envelope.scope.time_range = Some(TimeRange {
        start_ns: start_ns.to_string(),
        end_ns: end_ns.to_string(),
    });
}

/// The commit tokens the caller required the snapshot to include.
pub(crate) fn min_tokens(envelope: &mut Envelope, tokens: &[CommitToken]) {
    envelope.visibility.min_commit_tokens_applied =
        tokens.iter().map(CommitToken::encode).collect();
}

/// Coverage and the operation's own non-fatal diagnostics.
pub(crate) fn coverage(envelope: &mut Envelope, partial: bool, warnings: Vec<String>) {
    envelope.coverage.complete = !partial;
    envelope.coverage.partial = partial;
    envelope.warnings.extend(warnings);
}

/// The `budget` block: the ceilings the call resolved to, what it spent, and
/// what it estimated before it spent anything.
pub(crate) fn spend(envelope: &mut Envelope, budgets: &McpEffectiveBudgets, spend: &Spend<'_>) {
    let mut actual = actual(spend.accounting);
    if let (Some(phases), Value::Object(map)) = (spend.phases, &mut actual) {
        map.insert("phases".to_string(), phase_split(phases));
    }
    envelope.budget = Budget {
        effective: AnyJson(ceilings(budgets)),
        actual: AnyJson(actual),
        estimate: AnyJson(estimated(spend.estimate)),
        estimate_is_upper_envelope: spend.estimate_is_upper_envelope,
    };
}

/// The `budget` block of an operation whose outcome reports its spend in the
/// exemplars surface's own JSON spelling rather than as a
/// [`QueryAccountingSnapshot`].
///
/// `stats` is the serialized outcome stats. The counters are read out by the
/// names that surface publishes and reported in the same spelling every other
/// tool uses, so one collector reads them all. If any expected name is absent
/// (the surface renamed a field), the raw object is reported unchanged and a
/// warning says so: a missing name would otherwise read as a real zero.
pub(crate) fn spend_from_stats_json(
    envelope: &mut Envelope,
    budgets: &McpEffectiveBudgets,
    stats: &Value,
) {
    let actual = match published_accounting(stats) {
        Some(actual) => actual,
        None => {
            envelope.warnings.push(
                "cost figures are reported in the query surface's own field names: this build \
                 could not read them under the names it expected"
                    .to_string(),
            );
            stats.get("accounting").cloned().unwrap_or(Value::Null)
        }
    };
    envelope.budget = Budget {
        effective: AnyJson(ceilings(budgets)),
        actual: AnyJson(actual),
        // This surface computes no estimate at all (it resolves without one),
        // so there is nothing to report rather than a zero that would read as
        // an estimate of zero.
        estimate: AnyJson(Value::Null),
        estimate_is_upper_envelope: false,
    };
}

/// The six published counter names the exemplars surface's `accounting`
/// object carries, summed into the three figures the ceilings bound.
fn published_accounting(stats: &Value) -> Option<Value> {
    let accounting = stats.get("accounting")?;
    let counter = |name: &str| accounting.get(name).and_then(Value::as_u64);
    let requests = counter("s3GetRequests")?
        .saturating_add(counter("s3ListRequests")?)
        .saturating_add(counter("s3HeadRequests")?);
    let bytes = counter("s3GetBytes")?
        .saturating_add(counter("s3ListBytes")?)
        .saturating_add(counter("s3HeadBytes")?);
    Some(json!({
        "bytes_scanned": bytes,
        "store_requests": requests,
        "segments_read": counter("segmentsOpened")?,
        "decompressed_bytes": counter("decompressedBytes")?,
    }))
}

/// Apply the two D6 caps and resolve the status: the row cap first, so the
/// byte cap measures the envelope the caller will actually receive, then
/// D4's byte-cap algorithm.
///
/// `Envelope::fit` reports only what its own byte cap dropped (it resets
/// `rows_omitted` when the envelope already fits), so the row cap's own
/// count is folded back in afterwards. No operation here has a total order
/// to page over, so no cursor is minted and the capped status is
/// `ok_bounded`.
///
/// Two figures are carried through rather than recomputed from what survived.
/// `data.row_count` is the count the operation produced, which is what D4
/// makes it and what `Envelope::fit` deliberately leaves alone: it is read
/// off the envelope the caller passes in, before either cap runs, and
/// `presentation.rows_omitted` beside it is what the caps took. And
/// `presentation.row_cap_hit` is folded in rather than assigned, because a
/// caller may already have set it from a cap of its own: the SQL executor
/// stops its own stream at a row cap this layer never sees.
///
/// Every identity field the operation left unmeasured is named in `warnings`
/// by [`Envelope::finish`](ravel_mcp::envelope::Envelope::finish) itself, so
/// the empty string D4's typing forces there is never read as a value. That
/// warning lives on `finish` rather than here so every envelope reaching a
/// caller carries it, including one a crate builds and calls `fit`/`finish`
/// on directly without going through this wrapper (`ravel_capabilities` is
/// exactly that case).
pub(crate) fn finish(mut envelope: Envelope, budgets: &McpEffectiveBudgets) -> Envelope {
    let produced_rows = envelope.data.row_count;
    let max_rows = budgets.max_rows as usize;
    let row_cap_omitted = envelope.data.rows.len().saturating_sub(max_rows) as u64;
    envelope.data.rows.truncate(max_rows);

    let mut fitted = envelope.fit(budgets.max_response_bytes);
    fitted.presentation.rows_omitted += row_cap_omitted;
    fitted.presentation.row_cap_hit |= row_cap_omitted > 0;
    fitted.data.row_count = produced_rows;
    fitted.finish(false)
}

/// The D4 failure class of a service error. Every service kind has one, so
/// the mapping is total and no failure arrives as a protocol error: D4
/// reserves those for malformed JSON-RPC and unknown tool names.
pub(crate) fn failure(error: &ServiceError) -> Failure {
    let class = match error.kind {
        ServiceErrorKind::Unauthorized => FailureClass::Unauthorized,
        ServiceErrorKind::InvalidArgument => FailureClass::InvalidArgument,
        ServiceErrorKind::Validation => FailureClass::Validation,
        ServiceErrorKind::Unsupported => FailureClass::Unsupported,
        ServiceErrorKind::BudgetExceeded => FailureClass::BudgetExceeded,
        ServiceErrorKind::Deadline => FailureClass::Deadline,
        ServiceErrorKind::Unavailable => FailureClass::Unavailable,
        ServiceErrorKind::SnapshotInvalidated => FailureClass::SnapshotInvalidated,
        ServiceErrorKind::Internal => FailureClass::Internal,
    };
    Failure {
        class,
        message: error.message.clone(),
        // The service layer does not name which counter tripped, so a budget
        // failure reports the class alone rather than a guessed counter.
        counter: None,
    }
}

/// A request this adapter could not build from the tool's arguments. The
/// operation never ran, so there is nothing to report but the class.
pub(crate) fn invalid_argument(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}

/// A result this adapter produced but could not encode. The operation ran, so
/// the caller is told the failure is on this side rather than in the request.
pub(crate) fn internal(message: String) -> Failure {
    Failure {
        class: FailureClass::Internal,
        message,
        counter: None,
    }
}

/// The `budget` block of an operation that reports an estimate but no spend of
/// its own.
///
/// `actual` stays empty rather than zero: an operation whose outcome type does
/// not carry its accounting snapshot spent store requests it cannot report,
/// and a zero there would read as a query that touched nothing.
pub(crate) fn estimate_only(
    envelope: &mut Envelope,
    budgets: &McpEffectiveBudgets,
    estimate: &CostEstimate,
    estimate_is_upper_envelope: bool,
) {
    envelope.budget = Budget {
        effective: AnyJson(ceilings(budgets)),
        actual: AnyJson(Value::Object(Map::new())),
        estimate: AnyJson(estimated(estimate)),
        estimate_is_upper_envelope,
    };
}

/// The effective ceilings, in the spelling `ravel_capabilities` reports and
/// the D6 `budget.effective` block uses.
fn ceilings(budgets: &McpEffectiveBudgets) -> Value {
    json!({
        "max_rows": budgets.max_rows,
        "max_response_bytes": budgets.max_response_bytes,
        "deadline_ms": budgets.deadline.as_millis() as u64,
        "max_bytes_scanned": byte_limit(budgets.query.max_bytes_scanned),
        "max_store_requests": request_limit(budgets.query.max_store_requests),
        "max_segments": budgets.query.max_segments,
    })
}

fn byte_limit(limit: ravel_query::ByteLimit) -> Value {
    match limit {
        ravel_query::ByteLimit::Bounded(bytes) => json!(bytes),
        ravel_query::ByteLimit::Unlimited => Value::Null,
    }
}

fn request_limit(limit: ravel_query::RequestLimit) -> Value {
    match limit {
        ravel_query::RequestLimit::Bounded(requests) => json!(requests),
        ravel_query::RequestLimit::Unlimited => Value::Null,
    }
}

/// What the operation spent, per phase as well as pooled. The three pooled
/// figures are the ones the ceilings above bound; the per-phase split is what
/// makes a cost attributable to the phase that issued it.
fn actual(accounting: &QueryAccountingSnapshot) -> Value {
    json!({
        "bytes_scanned": accounting.total_s3_bytes(),
        "store_requests": accounting.total_s3_requests(),
        "segments_read": accounting.segments_opened,
        "decompressed_bytes": accounting.decompressed_bytes,
    })
}

fn estimated(estimate: &CostEstimate) -> Value {
    json!({
        "bytes_scanned": estimate.estimated_store_bytes,
        "store_requests": estimate.estimated_requests,
        "segments_read": estimate.segments,
        "decompressed_bytes": estimate.estimated_decompressed_bytes,
    })
}

/// The per-phase split of one operation's requests and wire bytes, every
/// phase exactly once. Additive to the pooled figures beside it: a pooled
/// request count cannot say whether the query spent its requests resolving
/// the catalog snapshot or scanning pages.
fn phase_split(phases: &PhaseAccountingSnapshot) -> Value {
    let mut map = Map::new();
    for (phase, requests, wire_bytes) in phase_figures(phases) {
        map.insert(
            phase.name().to_string(),
            json!({ "store_requests": requests, "wire_bytes": wire_bytes }),
        );
    }
    Value::Object(map)
}

/// The per-phase wire bytes and requests of one operation, in
/// [`QueryPhase::ALL`] order and each phase exactly once. The figures the
/// progress notifications carry.
pub(crate) fn phase_figures(phases: &PhaseAccountingSnapshot) -> Vec<(QueryPhase, u64, u64)> {
    QueryPhase::ALL
        .iter()
        .map(|&phase| {
            let accounting = phases.phase(phase);
            (
                phase,
                accounting.total_s3_requests(),
                accounting.total_s3_bytes(),
            )
        })
        .collect()
}

/// One instant vector: one row per matched element.
///
/// A native-histogram element reports `null` in the float column and its
/// histogram in the `histogram` column. Rendering its in-memory `0.0`
/// placeholder as a float would be the silent zero ADR-0108 forbids.
pub(crate) fn instant_vector_data(vector: InstantVector) -> Data {
    let rows = vector
        .into_iter()
        .map(|sample| {
            vec![
                labels_cell(&sample.labels),
                Cell::Timestamp(sample.ts_ns),
                match &sample.histogram {
                    Some(_) => Cell::Null,
                    None => Cell::Float(sample.value),
                },
                histogram_cell(sample.histogram.as_ref()),
            ]
        })
        .collect();
    sample_data(rows)
}

/// One range matrix: one row per `(series, step)` pair, in the evaluation's
/// own series and step order.
pub(crate) fn matrix_data(matrix: RangeMatrix) -> Data {
    let mut rows: Vec<Row> = Vec::new();
    for (labels, samples) in matrix {
        let series = labels_cell(&labels);
        for sample in samples {
            rows.push(vec![
                series.clone(),
                Cell::Timestamp(sample.ts_ns),
                Cell::Float(sample.value),
                Cell::Null,
            ]);
        }
    }
    sample_data(rows)
}

/// One histogram-aware range matrix: one row per `(series, step)` pair, with
/// a histogram step reporting `null` in the float column for the same
/// ADR-0108 reason as [`instant_vector_data`]. This is the shape a range
/// evaluation actually returns; [`matrix_data`] takes the float-only
/// projection an instant evaluation's `Matrix` value carries.
pub(crate) fn histogram_aware_matrix_data(matrix: HistogramAwareMatrix) -> Data {
    let mut rows: Vec<Row> = Vec::new();
    for (labels, samples) in matrix {
        let series = labels_cell(&labels);
        for sample in samples {
            rows.push(vec![
                series.clone(),
                Cell::Timestamp(sample.ts_ns),
                match &sample.histogram {
                    Some(_) => Cell::Null,
                    None => Cell::Float(sample.value),
                },
                histogram_cell(sample.histogram.as_ref()),
            ]);
        }
    }
    sample_data(rows)
}

fn sample_data(rows: Vec<Row>) -> Data {
    Data {
        columns: vec![
            column("series", "json"),
            column("ts_ns", "timestamp"),
            column("value", "double"),
            column("histogram", "json"),
        ],
        row_count: rows.len() as u64,
        rows,
    }
}

/// A top-level scalar or string result: one row, one column.
pub(crate) fn scalar_data(cell: Cell, r#type: &str) -> Data {
    Data {
        columns: vec![column("value", r#type)],
        rows: vec![vec![cell]],
        row_count: 1,
    }
}

/// A list of names or values: one row each, one column.
pub(crate) fn string_list_data(name: &str, values: Vec<String>) -> Data {
    let rows: Vec<Row> = values.into_iter().map(|v| vec![Cell::Str(v)]).collect();
    Data {
        columns: vec![column(name, "string")],
        row_count: rows.len() as u64,
        rows,
    }
}

/// The matched series of a metadata query: the canonical series id and the
/// label set behind it.
pub(crate) fn series_data(series: Vec<(SeriesId, LabelSet)>) -> Data {
    let rows: Vec<Row> = series
        .into_iter()
        .map(|(id, labels)| vec![series_id_cell(id), labels_cell(&labels)])
        .collect();
    Data {
        columns: vec![column("series_id", "hex_id"), column("series", "json")],
        row_count: rows.len() as u64,
        rows,
    }
}

/// A `(property, value)` table, for an operation whose answer is a set of
/// named figures rather than a result set.
pub(crate) fn property_data(properties: Vec<(&str, Value)>) -> Data {
    let rows: Vec<Row> = properties
        .into_iter()
        .map(|(name, value)| vec![Cell::Str(name.to_string()), json_cell(value)])
        .collect();
    Data {
        columns: vec![column("property", "string"), column("value", "json")],
        row_count: rows.len() as u64,
        rows,
    }
}

/// One row per JSON object, under a single column of that name. The shape a
/// surface whose answer is already JSON (exemplars) arrives in.
pub(crate) fn json_rows_data(name: &str, values: Vec<Value>) -> Data {
    let rows: Vec<Row> = values.into_iter().map(|v| vec![json_cell(v)]).collect();
    Data {
        columns: vec![column(name, "json")],
        row_count: rows.len() as u64,
        rows,
    }
}

/// A SQL result, from the `{"columns": [{name, type}], "rows": [[cell]]}`
/// encoding `ravel_sql::QueryOutput::to_json` produces. Going through that
/// encoding rather than the record batches keeps one Arrow-to-JSON mapping in
/// the process: a second one would be a second place for a type to be
/// rendered differently.
pub(crate) fn sql_data(output: &Value) -> Data {
    let columns = output
        .get("columns")
        .and_then(Value::as_array)
        .map(|columns| {
            columns
                .iter()
                .map(|column| Column {
                    name: string_field(column, "name"),
                    r#type: string_field(column, "type"),
                })
                .collect()
        })
        .unwrap_or_default();
    let rows: Vec<Row> = output
        .get("rows")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.as_array()
                        .map(|cells| cells.iter().map(json_to_cell).collect())
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default();
    Data {
        row_count: rows.len() as u64,
        columns,
        rows,
    }
}

fn string_field(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// One already-JSON cell, as the D4 cell whose precision rules match its
/// type. An integer keeps its exact value as a string (D4: a nanosecond
/// epoch exceeds 2^53), a float follows the float rules, and an array has no
/// cell variant of its own so it keeps its JSON text.
///
/// `serde_json` parses a JSON integer above `i64::MAX` as a `u64`, and
/// `Number::as_f64` succeeds on it with precision loss. `Cell` has no
/// unsigned variant, so a number that fails `as_i64` only becomes a float
/// when it actually is one (`is_f64`); otherwise its exact digits go out as
/// `Cell::Str`, matching the string-precision rule integers already get.
fn json_to_cell(value: &Value) -> Cell {
    match value {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(*b),
        Value::Number(number) => match number.as_i64() {
            Some(n) => Cell::Int(n),
            None if number.is_f64() => match number.as_f64() {
                Some(f) => Cell::Float(f),
                None => Cell::Str(number.to_string()),
            },
            None => Cell::Str(number.to_string()),
        },
        Value::String(s) => Cell::Str(s.clone()),
        Value::Object(map) => Cell::Map(map.clone()),
        Value::Array(_) => Cell::Str(value.to_string()),
    }
}

fn json_cell(value: Value) -> Cell {
    match value {
        Value::Object(map) => Cell::Map(map),
        other => json_to_cell(&other),
    }
}

fn labels_cell(labels: &LabelSet) -> Cell {
    let mut map = Map::new();
    for label in labels.iter() {
        map.insert(label.name.clone(), Value::String(label.value.clone()));
    }
    Cell::Map(map)
}

/// The canonical series id as a hex cell. `HexId` bounds its input, and a
/// 128-bit id is far inside that bound, so the fallback is unreachable; it
/// exists because the constructor is fallible and this path has no failure
/// channel of its own.
fn series_id_cell(id: SeriesId) -> Cell {
    let hex = id.to_hex();
    Cell::hex_id(hex.clone()).unwrap_or(Cell::Str(hex))
}

/// A native histogram, as the fields a caller can act on. The bucket spans
/// themselves are not rendered: they are unbounded in length and D4 bounds
/// every cell, and a histogram cut mid-bucket is not a smaller histogram but
/// a wrong one.
fn histogram_cell(histogram: Option<&FloatHistogram>) -> Cell {
    match histogram {
        None => Cell::Null,
        Some(histogram) => {
            let mut map = Map::new();
            map.insert("count".to_string(), json!(histogram.count));
            map.insert("sum".to_string(), json!(histogram.sum));
            map.insert("scale".to_string(), json!(histogram.scale));
            map.insert("zero_count".to_string(), json!(histogram.zero_count));
            map.insert(
                "zero_threshold".to_string(),
                json!(histogram.zero_threshold),
            );
            map.insert(
                "positive_bucket_count".to_string(),
                json!(histogram.positive_buckets.len()),
            );
            map.insert(
                "negative_bucket_count".to_string(),
                json!(histogram.negative_buckets.len()),
            );
            Cell::Map(map)
        }
    }
}

fn column(name: &str, r#type: &str) -> Column {
    Column {
        name: name.to_string(),
        r#type: r#type.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_mcp::budget::{McpBudgetConfig, McpRequestBudgets};
    use ravel_mcp::envelope::Status;
    use ravel_promql::{InstantSample, ResetHint};
    use ravel_query::EngineConfig;
    use ravel_types::{Label, Sample};

    fn budgets() -> McpEffectiveBudgets {
        McpRequestBudgets::default().clamp(&EngineConfig::default(), &McpBudgetConfig::default())
    }

    fn labels(name: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: name.to_string(),
        }])
        .expect("one label is a valid set")
    }

    /// A histogram element reports its histogram and a null float. The `0.0`
    /// its in-memory form carries is a placeholder, and rendering it as a
    /// float would be a silent zero (ADR-0108).
    #[test]
    fn a_histogram_element_reports_no_float_value() {
        let histogram = FloatHistogram {
            counter_reset_hint: ResetHint::Unknown,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 7.0,
            sum: 21.0,
            positive_spans: Vec::new(),
            negative_spans: Vec::new(),
            positive_buckets: Vec::new(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        };
        let vector = vec![InstantSample {
            labels: labels("latency"),
            ts_ns: 5,
            orig_sample_ts_ns: 5,
            value: 0.0,
            histogram: Some(histogram),
        }];

        let data = instant_vector_data(vector);

        assert_eq!(data.row_count, 1);
        assert_eq!(data.rows[0][2], Cell::Null);
        let Cell::Map(map) = &data.rows[0][3] else {
            panic!("the histogram column is an object: {:?}", data.rows[0][3]);
        };
        assert_eq!(map["count"], json!(7.0));
        assert_eq!(map["sum"], json!(21.0));
    }

    /// A matrix flattens to one row per `(series, step)` pair, so a two-series
    /// matrix of three steps each is exactly six rows.
    #[test]
    fn a_matrix_is_one_row_per_series_and_step() {
        let samples: Vec<Sample> = (0..3)
            .map(|i| Sample {
                ts_ns: i,
                value: i as f64,
            })
            .collect();
        let matrix = vec![
            (labels("up"), samples.clone()),
            (labels("down"), samples.clone()),
        ];

        let data = matrix_data(matrix);

        assert_eq!(data.row_count, 6);
        assert_eq!(data.rows.len(), 6);
    }

    /// The row cap drops the rows past it, reports exactly how many it
    /// dropped, and leaves the status `ok_bounded`: more rows matched than
    /// came back and no cursor can reach them. `row_count` stays the produced
    /// count, so the returned rows plus the omitted ones account for it.
    #[test]
    fn the_row_cap_reports_the_exact_number_it_dropped() {
        let budgets = budgets();
        let rows = budgets.max_rows as usize + 17;
        let mut envelope = base("metrics", "metrics", &budgets);
        envelope.data = string_list_data("label", (0..rows).map(|i| i.to_string()).collect());

        let fitted = finish(envelope, &budgets);

        assert_eq!(fitted.data.rows.len(), budgets.max_rows as usize);
        assert_eq!(fitted.data.row_count, u64::from(budgets.max_rows) + 17);
        assert_eq!(fitted.presentation.rows_omitted, 17);
        assert!(fitted.presentation.row_cap_hit);
        assert_eq!(fitted.status, Status::OkBounded);
        assert!(fitted.presentation.cursor.is_none());
    }

    /// A cap the caller reported from its own outcome survives `finish`. The
    /// SQL executor stops its stream at a row cap this layer never sees, so
    /// `sql_execute` sets `row_cap_hit` from `outcome.stats.row_cap_hit`
    /// before calling here. Overwriting it with the D6 row cap's own verdict
    /// would report `row_cap_hit: false` and degrade the status to `ok`, which
    /// D4 defines as a complete query.
    #[test]
    fn row_cap_hit_from_the_outcome_survives_finish() {
        let budgets = budgets();
        let mut envelope = base("mixed", "sql", &budgets);
        envelope.data = string_list_data("label", vec!["up".to_string(), "down".to_string()]);
        envelope.presentation.row_cap_hit = true;

        let fitted = finish(envelope, &budgets);

        assert!(fitted.presentation.row_cap_hit);
        assert_eq!(fitted.status, Status::OkBounded);
        // The D6 row cap itself dropped nothing: two rows are far under the
        // 200-row ceiling, so the surviving `row_cap_hit` is the outcome's.
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert_eq!(fitted.data.rows.len(), 2);
        assert_eq!(fitted.data.row_count, 2);
    }

    /// `data.row_count` is the count the query produced, not the length of
    /// what survived the byte cap (D4: "data.row_count keeps the count of rows
    /// that the query produced"). A caller reading `row_count` alone must not
    /// see a complete-looking result whose tail the cap took; the dropped
    /// count is in `rows_omitted` beside it, and the two account for the
    /// produced total exactly.
    #[test]
    fn row_count_keeps_the_produced_count_after_a_byte_cap_drop() {
        let budgets = budgets();
        // 150 rows of 8 KiB, so the payload is about 1.2 MiB against the
        // 512 KiB default cap, and the count stays under the 200-row ceiling:
        // the byte cap is the only cap that fires here.
        let produced = 150u64;
        let mut envelope = base("logs", "logs", &budgets);
        envelope.data = string_list_data(
            "body",
            (0..produced).map(|i| format!("{i:*<8192}")).collect(),
        );

        let fitted = finish(envelope, &budgets);

        assert!(fitted.presentation.bytes_cap_hit);
        assert!(!fitted.presentation.row_cap_hit);
        assert_eq!(fitted.data.row_count, produced);
        assert_eq!(fitted.presentation.rows_omitted, 87);
        assert_eq!(fitted.data.rows.len(), 63);
        // The two account for the produced total exactly: nothing is lost
        // between what came back and what the cap took.
        assert_eq!(
            fitted.data.rows.len() as u64 + fitted.presentation.rows_omitted,
            fitted.data.row_count
        );
        assert_eq!(fitted.presentation.cells_truncated, 0);
        assert_eq!(fitted.status, Status::OkBounded);
    }

    /// A result inside both caps is `ok`, with nothing omitted.
    #[test]
    fn a_result_inside_both_caps_is_ok() {
        let budgets = budgets();
        let mut envelope = base("metrics", "labels", &budgets);
        envelope.data = string_list_data("label", vec!["up".to_string(), "down".to_string()]);

        let fitted = finish(envelope, &budgets);

        assert_eq!(fitted.status, Status::Ok);
        assert_eq!(fitted.data.row_count, 2);
        assert_eq!(fitted.presentation.rows_omitted, 0);
        assert!(!fitted.presentation.row_cap_hit);
    }

    /// A metadata operation measures none of the four identity fields, so all
    /// four are named in `warnings` rather than emitted as empty strings.
    ///
    /// The list is asserted whole and in order: a field dropped from it would
    /// go back to serializing `""`, which reads as a measured value, and this
    /// is the one place that says so.
    #[test]
    fn unmeasured_identity_fields_are_warned_not_empty() {
        let budgets = budgets();
        let mut envelope = base("metrics", "labels", &budgets);
        window(&mut envelope, 0, 3_600_000_000_000);
        envelope.data = string_list_data("label", vec!["up".to_string()]);
        coverage(&mut envelope, false, Vec::new());

        let fitted = finish(envelope, &budgets);

        assert_eq!(
            fitted.warnings,
            vec![
                "visibility.snapshot_id is not reported by this operation".to_string(),
                "visibility.watermark_hour is not reported by this operation".to_string(),
                "ids.query_id is not reported by this operation".to_string(),
                "ids.audit_ref is not reported by this operation".to_string(),
            ]
        );
        // The fields themselves are still the empty strings D4's typing
        // forces; the warnings are what makes them readable as absent.
        assert!(fitted.visibility.snapshot_id.is_empty());
        assert!(fitted.visibility.watermark_hour.is_empty());
        assert!(fitted.ids.query_id.is_empty());
        assert!(fitted.ids.audit_ref.is_empty());
    }

    /// An identity field the operation did measure is reported and not warned
    /// about, so the warning list is exactly the fields still missing.
    #[test]
    fn a_measured_identity_field_is_not_warned_about() {
        let budgets = budgets();
        let mut envelope = base("metrics", "labels", &budgets);
        envelope.visibility.snapshot_id = "snap-7".to_string();
        envelope.data = string_list_data("label", vec!["up".to_string()]);

        let fitted = finish(envelope, &budgets);

        assert_eq!(fitted.visibility.snapshot_id, "snap-7");
        assert_eq!(
            fitted.warnings,
            vec![
                "visibility.watermark_hour is not reported by this operation".to_string(),
                "ids.query_id is not reported by this operation".to_string(),
                "ids.audit_ref is not reported by this operation".to_string(),
            ]
        );
    }

    /// A service failure carries its D4 class and the service layer's
    /// already-redacted message, not a message this layer composed.
    #[test]
    fn a_service_failure_becomes_its_d4_class() {
        let error = ServiceError::unauthorized();

        let failure = failure(&error);

        assert_eq!(failure.class, FailureClass::Unauthorized);
        assert_eq!(failure.message, error.message);
        assert!(failure.counter.is_none());
    }

    /// The pooled figures and the per-phase split are both reported, and the
    /// split names every phase exactly once.
    #[test]
    fn the_budget_block_reports_the_phase_split_beside_the_pooled_figures() {
        let budgets = budgets();
        let accounting = QueryAccountingSnapshot::default();
        let estimate = ravel_query::http::service::zero_estimate();
        let phases = PhaseAccountingSnapshot::default();
        let mut envelope = base("metrics", "metrics", &budgets);

        spend(
            &mut envelope,
            &budgets,
            &Spend {
                accounting: &accounting,
                estimate: &estimate,
                phases: Some(&phases),
                estimate_is_upper_envelope: true,
            },
        );

        let actual = &envelope.budget.actual.0;
        assert_eq!(actual["store_requests"], json!(0));
        assert_eq!(actual["bytes_scanned"], json!(0));
        let split = actual["phases"]
            .as_object()
            .expect("the split is an object");
        assert_eq!(split.len(), 4);
        for phase in QueryPhase::ALL {
            assert_eq!(split[phase.name()]["store_requests"], json!(0));
            assert_eq!(split[phase.name()]["wire_bytes"], json!(0));
        }
        assert!(envelope.budget.estimate_is_upper_envelope);
    }

    /// An operation that reports its spend in the exemplars surface's own
    /// JSON spelling still reports it under the names every other tool uses.
    /// The counters serialize with every field present, so this also fails if
    /// one of those published names is ever renamed.
    #[test]
    fn published_counter_names_are_read_into_the_shared_spelling() {
        let budgets = budgets();
        let stats = serde_json::to_value(crate::exemplars::QueryStatsJson::default())
            .expect("the stats serialize");
        let mut envelope = base("metrics", "exemplars", &budgets);

        spend_from_stats_json(&mut envelope, &budgets, &stats);

        let actual = &envelope.budget.actual.0;
        assert_eq!(actual["store_requests"], json!(0));
        assert_eq!(actual["bytes_scanned"], json!(0));
        assert_eq!(actual["segments_read"], json!(0));
        assert_eq!(actual["decompressed_bytes"], json!(0));
        assert_eq!(envelope.budget.estimate.0, Value::Null);
        assert!(envelope.warnings.is_empty());
    }

    /// A counter object without the expected names is reported raw with a
    /// warning rather than as a zero a reader would take for a real figure.
    #[test]
    fn unreadable_counters_are_reported_raw_with_a_warning() {
        let budgets = budgets();
        let stats = json!({ "accounting": { "somethingElse": 3 } });
        let mut envelope = base("metrics", "exemplars", &budgets);

        spend_from_stats_json(&mut envelope, &budgets, &stats);

        assert_eq!(envelope.budget.actual.0, json!({ "somethingElse": 3 }));
        assert_eq!(envelope.warnings.len(), 1);
    }

    /// The SQL encoding's integers keep their exact value as strings and its
    /// nulls stay null: a cell rendered through the JSON number type would
    /// lose an id above 2^53.
    #[test]
    fn sql_cells_keep_their_exact_integer_values() {
        let output = json!({
            "columns": [{"name": "ts_ns", "type": "Int64"}, {"name": "value", "type": "Float64"}],
            "rows": [["9007199254740993", null], [9007199254740993i64, 1.5]],
        });

        let data = sql_data(&output);

        assert_eq!(data.row_count, 2);
        assert_eq!(data.columns[0].name, "ts_ns");
        assert_eq!(data.columns[0].r#type, "Int64");
        assert_eq!(data.rows[0][0], Cell::Str("9007199254740993".to_string()));
        assert_eq!(data.rows[0][1], Cell::Null);
        assert_eq!(data.rows[1][0], Cell::Int(9007199254740993));
        assert_eq!(data.rows[1][1], Cell::Float(1.5));
    }

    /// A JSON integer above `i64::MAX` parses as a `u64`, and `Cell` has no
    /// unsigned variant. Rendering it through `as_f64` would lose precision
    /// (D4's exact-semantics-by-default rule), so it must come out as the
    /// exact digits in `Cell::Str`, the same way an out-of-range `i64` does.
    /// An ordinary float is unaffected and still becomes `Cell::Float`.
    #[test]
    fn a_large_unsigned_integer_is_not_downgraded_to_a_float() {
        assert_eq!(
            json_to_cell(&json!(u64::MAX)),
            Cell::Str(u64::MAX.to_string())
        );
        assert_eq!(
            json_to_cell(&json!(i64::MAX as u64 + 1)),
            Cell::Str((i64::MAX as u64 + 1).to_string())
        );
        assert_eq!(json_to_cell(&json!(1.5f64)), Cell::Float(1.5));
    }

    /// The per-phase figures are every phase exactly once, in
    /// `QueryPhase::ALL` order, so a reader cannot mistake one phase's
    /// requests for another's.
    #[test]
    fn phase_figures_report_every_phase_exactly_once() {
        let phases = PhaseAccountingSnapshot::default();

        let figures = phase_figures(&phases);

        assert_eq!(figures.len(), 4);
        let names: Vec<&str> = figures.iter().map(|(phase, _, _)| phase.name()).collect();
        assert_eq!(names, vec!["resolve", "plan", "probe", "scan"]);
    }
}
