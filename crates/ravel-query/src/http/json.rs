//! Prometheus-compatible JSON response envelopes.

use std::collections::HashMap;

use ravel_promql::{FloatHistogram, RangeSample, RangeValue, Value, ms_to_ns};
use ravel_types::accounting::AccountedOp;
use ravel_types::{LabelSet, SeriesId};
use serde::{Serialize, Serializer};

use crate::http::error::ApiError;
use crate::phase_accounting::{PhaseAccountingSnapshot, QueryPhase};

#[derive(Serialize)]
#[serde(tag = "status")]
pub enum ApiResponse<T> {
    #[serde(rename = "success")]
    Success {
        data: T,
        /// Coverage marker (ADR-0071 "partial results are consent-gated and
        /// envelope-visible" amendment, decision 2): a required top-level
        /// sibling of `status`/`data`/`warnings`, `false` on complete coverage
        /// and `true` on partial. Unlike `warnings`/`infos` it is NOT omitted
        /// when its value is the "empty" case: a strict deserializer modelling a
        /// read-endpoint envelope must always see it, so a naive client cannot
        /// mistake a partial answer for a complete one.
        ///
        /// `Option` only so the stateless compatibility routes (buildinfo,
        /// metadata) that build a bare success envelope via [`Self::success`]
        /// omit it entirely rather than assert a coverage claim they never
        /// evaluate; the five read endpoints always pass `Some(_)` through
        /// [`Self::success_with_annotations`], so on those endpoints the field is
        /// unconditionally present.
        #[serde(skip_serializing_if = "Option::is_none")]
        partial: Option<bool>,
        /// Prometheus' top-level `warnings` array: non-fatal diagnostics that
        /// very likely indicate a result the caller did not intend. Omitted
        /// when empty (Prometheus' own `omitempty`), which most responses
        /// are; the labels/series/label-values endpoints always pass empty.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
        /// Prometheus' top-level `infos` array: non-fatal diagnostics worth
        /// surfacing but usually benign. Kept separate from `warnings` (the
        /// two are distinct fields in Prometheus and in [`ravel_promql`]'s
        /// `Annotations`). Omitted when empty.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        infos: Vec<String>,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(rename = "errorType")]
        error_type: &'static str,
        error: String,
    },
}

impl<T> ApiResponse<T> {
    /// A bare success envelope with no annotations and no coverage marker, for
    /// the stateless compatibility routes (buildinfo, metadata) that run no
    /// query and so make no coverage claim. The `partial` field is omitted
    /// here; the five read endpoints use [`Self::success_with_annotations`],
    /// which always renders it.
    pub fn success(data: T) -> Self {
        ApiResponse::Success {
            data,
            partial: None,
            warnings: Vec::new(),
            infos: Vec::new(),
        }
    }

    /// A success envelope for a read endpoint: carries the query's evaluation
    /// annotations and the always-present top-level `partial` coverage marker
    /// (ADR-0071 amendment decision 2).
    pub fn success_with_annotations(
        data: T,
        partial: bool,
        warnings: Vec<String>,
        infos: Vec<String>,
    ) -> Self {
        ApiResponse::Success {
            data,
            partial: Some(partial),
            warnings,
            infos,
        }
    }
}

/// Segment counters for the query that produced a response
///, rendered under the Prometheus response
/// envelope's `data` object alongside `resultType`/`result`. Prometheus'
/// own API has no standardized shape for this (ravel-query previously
/// carried no query-level stats at all),
/// so the field names here are ravel's own.
#[derive(Debug, Serialize)]
pub struct QueryStatsJson {
    #[serde(rename = "segmentsFetched")]
    pub segments_fetched: u64,
    #[serde(rename = "segmentsPruned")]
    pub segments_pruned: u64,
    pub accounting: QueryAccountingJson,
    /// The same GETs and bytes `accounting` pools, split across the four
    /// phases of a query's object-store cost (issue #935, ADR-0927 decision 7;
    /// the split itself is issue #796's `PhaseAccounting`, which until now
    /// reached no reader outside the engine's own unit tests). One entry per
    /// [`QueryPhase`], in [`QueryPhase::ALL`] order, each phase exactly once:
    /// `resolve`, `plan`, `probe`, `scan`.
    ///
    /// Additive to `accounting`, never a replacement: every per-phase counter
    /// here sums back to its pooled counterpart there, so a consumer reading
    /// only `accounting` sees exactly the numbers it saw before this field
    /// existed. The phases are what make a cost attributable -- a pooled GET
    /// count cannot say whether a query spent its requests resolving the
    /// catalog snapshot or scanning pages.
    pub phases: Vec<PhaseCostJson>,
    pub estimate: CostEstimateJson,
    /// True when at least one federated remote cluster was skipped because it
    /// was unavailable and its `skip_unavailable` opt-in was set
    /// (ADR-0071), so the query's coverage is partial. Always `false` for a
    /// fully cluster-local query. A client can read this marker to tell a
    /// partial federated result apart from a complete one; the skipped
    /// cluster(s) are named in the top-level `warnings` array.
    pub partial: bool,
    /// The federation fan-out's Prometheus-compatible `warnings` (one per
    /// skipped cluster). Also merged into the response envelope's top-level
    /// `warnings` array by the handler; carried here too so the stats block is
    /// self-describing. Each names the operator-facing cluster only, never its
    /// endpoint or transport error detail (redacted at the federation seam).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl From<crate::QueryStats> for QueryStatsJson {
    fn from(stats: crate::QueryStats) -> Self {
        QueryStatsJson {
            segments_fetched: stats.segments_fetched,
            segments_pruned: stats.segments_pruned,
            accounting: QueryAccountingJson::from_snapshot(&stats.accounting, &stats.page_stats),
            phases: phase_costs(&stats.phase_accounting),
            estimate: stats.estimate.into(),
            partial: stats.partial,
            warnings: stats.warnings.clone(),
        }
    }
}

/// Actual per-query counters (ADR-0044 decision 1), rendered under
/// `stats.accounting`. Field names are ravel's own; Prometheus has no
/// standard shape for this. `raw_f64_pages`/`raw_f64_bytes` are the
/// pre-existing per-segment `FetchStats`: a narrower count
/// of `ValPageKind::RawF64` pages specifically, kept distinct from
/// `decompressed_bytes` (the typed-output footprint of every decoded
/// sample, any encoding). Has no `segmentsPruned` field: `QueryAccounting`
/// carries a `segments_pruned` counter, but nothing in `ravel-query` or
/// `ravel-catalog` ever calls `add_segments_pruned`, so it would always
/// render 0 next to the correctly-populated `QueryStatsJson.segments_pruned`
/// (sourced from `Catalog::resolve`'s own count). Sibling field, sole
/// source of truth; do not reintroduce a second, always-zero copy here.
#[derive(Debug, Serialize)]
pub struct QueryAccountingJson {
    #[serde(rename = "s3GetRequests")]
    pub s3_get_requests: u64,
    #[serde(rename = "s3GetBytes")]
    pub s3_get_bytes: u64,
    #[serde(rename = "s3ListRequests")]
    pub s3_list_requests: u64,
    #[serde(rename = "s3ListBytes")]
    pub s3_list_bytes: u64,
    #[serde(rename = "s3HeadRequests")]
    pub s3_head_requests: u64,
    #[serde(rename = "s3HeadBytes")]
    pub s3_head_bytes: u64,
    #[serde(rename = "cacheHits")]
    pub cache_hits: u64,
    #[serde(rename = "cacheMisses")]
    pub cache_misses: u64,
    #[serde(rename = "cacheBytes")]
    pub cache_bytes: u64,
    #[serde(rename = "decompressedBytes")]
    pub decompressed_bytes: u64,
    #[serde(rename = "segmentsOpened")]
    pub segments_opened: u64,
    #[serde(rename = "seriesMatched")]
    pub series_matched: u64,
    #[serde(rename = "bytesReused")]
    pub bytes_reused: u64,
    #[serde(rename = "peakIntermediateBytes")]
    pub peak_intermediate_bytes: u64,
    #[serde(rename = "rawF64Pages")]
    pub raw_f64_pages: u64,
    #[serde(rename = "rawF64Bytes")]
    pub raw_f64_bytes: u64,
}

impl QueryAccountingJson {
    fn from_snapshot(
        snapshot: &ravel_types::accounting::QueryAccountingSnapshot,
        page_stats: &crate::fetcher::FetchStats,
    ) -> Self {
        QueryAccountingJson {
            s3_get_requests: snapshot.s3_requests(AccountedOp::Get),
            s3_get_bytes: snapshot.s3_bytes(AccountedOp::Get),
            s3_list_requests: snapshot.s3_requests(AccountedOp::List),
            s3_list_bytes: snapshot.s3_bytes(AccountedOp::List),
            s3_head_requests: snapshot.s3_requests(AccountedOp::Head),
            s3_head_bytes: snapshot.s3_bytes(AccountedOp::Head),
            cache_hits: snapshot.cache_hits,
            cache_misses: snapshot.cache_misses,
            cache_bytes: snapshot.cache_bytes,
            decompressed_bytes: snapshot.decompressed_bytes,
            segments_opened: snapshot.segments_opened,
            series_matched: snapshot.series_matched,
            bytes_reused: snapshot.bytes_reused,
            peak_intermediate_bytes: snapshot.peak_intermediate_bytes,
            raw_f64_pages: page_stats.raw_f64_pages,
            raw_f64_bytes: page_stats.raw_f64_bytes,
        }
    }
}

/// One [`QueryPhase`]'s share of a query's cost, rendered as one element of
/// `stats.phases` (issue #935).
///
/// # Which bytes each byte figure counts
///
/// Every byte field names its own kind, because figures of different kinds
/// cannot be compared with each other or summed together: wire bytes are what
/// the network moved, cache-served bytes never crossed the wire at all, and
/// decompressed bytes are a decode-time output size that no request paid for.
/// Each field's doc comment below names its kind, whether retries and range
/// reads are included, and its pooled counterpart under `stats.accounting`.
///
/// # Which counters are here, and which are deliberately absent
///
/// `stats.accounting` renders four counters this per-phase block does not,
/// because they are structurally dead on a PromQL query and a per-phase zero
/// would read as a measurement rather than as an absence (ADR-0927 decision
/// 7): `s3HeadRequests`/`s3HeadBytes` (`AccountedOp::Head` is recorded
/// nowhere on this path), `s3ListBytes` (`Catalog::guarded_list_all` counts
/// the LIST request but no bytes for it), and `peakIntermediateBytes` (SQL
/// executor only). They keep their pooled fields; they gain no phase fields.
#[derive(Debug, Serialize)]
pub struct PhaseCostJson {
    /// The phase, as [`QueryPhase::name`] spells it: `resolve` (catalog
    /// snapshot resolve), `plan` (footer and skip-index probing to build a
    /// fetch plan), `probe` (segment catalog fetch), or `scan` (block, page,
    /// and chunk data reads). See `crate::phase_accounting`'s module docs for
    /// the full phase boundaries.
    pub phase: &'static str,
    /// GET requests this phase issued. A LOGICAL call count, not a billed
    /// request count: `object_store` retries beneath `InstrumentedStore`, so
    /// one GET that retried nine times counts once here while S3 bills ten
    /// (ADR-0927 decision 8). Pooled counterpart: `accounting.s3GetRequests`.
    #[serde(rename = "s3GetRequests")]
    pub s3_get_requests: u64,
    /// WIRE bytes this phase's GETs transferred: the byte length of each
    /// successful response body, summed. Range reads are included and count
    /// only the bytes their range returned; a coalesced run's unwanted
    /// in-between bytes are included, because the wire carried them. A retried
    /// GET's failed attempts are NOT included -- the counter is written once,
    /// from the response that succeeded. Never stored bytes, never
    /// decompressed bytes, never cache-served bytes. Pooled counterpart:
    /// `accounting.s3GetBytes`.
    #[serde(rename = "s3GetWireBytes")]
    pub s3_get_wire_bytes: u64,
    /// LIST requests this phase issued, on the same logical-call basis as
    /// [`Self::s3_get_requests`]. Pooled counterpart:
    /// `accounting.s3ListRequests`. There is deliberately no per-phase LIST
    /// byte figure: nothing records LIST bytes.
    #[serde(rename = "s3ListRequests")]
    pub s3_list_requests: u64,
    /// In-process cache lookups this phase served from cache.
    #[serde(rename = "cacheHits")]
    pub cache_hits: u64,
    /// In-process cache lookups this phase had to fetch.
    #[serde(rename = "cacheMisses")]
    pub cache_misses: u64,
    /// Bytes this phase's cache hits served. These bytes NEVER crossed the
    /// wire, so they are not part of [`Self::s3_get_wire_bytes`] and must not
    /// be added to it: a warm query moves cache-served bytes and no wire
    /// bytes. Pooled counterpart: `accounting.cacheBytes`.
    #[serde(rename = "cacheServedBytes")]
    pub cache_served_bytes: u64,
    /// DECOMPRESSED bytes: the typed-output footprint of every sample this
    /// phase decoded, whatever the encoding. A decode-time output size that no
    /// request paid for, so it is comparable neither with
    /// [`Self::s3_get_wire_bytes`] nor with [`Self::cache_served_bytes`]. By
    /// convention this lands on the `scan` phase, since decode always follows
    /// a scan in this crate's fetch pipelines. Pooled counterpart:
    /// `accounting.decompressedBytes`.
    #[serde(rename = "decompressedOutputBytes")]
    pub decompressed_output_bytes: u64,
    /// Bytes this phase served from a region the fetcher had ALREADY fetched,
    /// without issuing a second GET. Wire bytes that were avoided, not wire
    /// bytes that were moved: they are absent from
    /// [`Self::s3_get_wire_bytes`] by construction (no request carried them)
    /// and adding the two would double-count the first fetch. Pooled
    /// counterpart: `accounting.bytesReused`.
    #[serde(rename = "reusedRegionBytes")]
    pub reused_region_bytes: u64,
    /// Segments this phase opened.
    #[serde(rename = "segmentsOpened")]
    pub segments_opened: u64,
    /// Series this phase's catalog decode matched.
    #[serde(rename = "seriesMatched")]
    pub series_matched: u64,
}

/// One entry per [`QueryPhase`], in [`QueryPhase::ALL`] order, each phase
/// exactly once. Driven off `ALL` rather than a hand-written list of four so a
/// phase added to the enum cannot be silently dropped from the response, the
/// same reason `ravel-bench`'s `sql_latency::phase_wire_bytes` is built this
/// way (issue #913).
fn phase_costs(snapshot: &PhaseAccountingSnapshot) -> Vec<PhaseCostJson> {
    QueryPhase::ALL
        .iter()
        .map(|phase| {
            let s = snapshot.phase(*phase);
            PhaseCostJson {
                phase: phase.name(),
                s3_get_requests: s.s3_requests(AccountedOp::Get),
                s3_get_wire_bytes: s.s3_bytes(AccountedOp::Get),
                s3_list_requests: s.s3_requests(AccountedOp::List),
                cache_hits: s.cache_hits,
                cache_misses: s.cache_misses,
                cache_served_bytes: s.cache_bytes,
                decompressed_output_bytes: s.decompressed_bytes,
                reused_region_bytes: s.bytes_reused,
                segments_opened: s.segments_opened,
                series_matched: s.series_matched,
            }
        })
        .collect()
}

/// Upper-envelope cost estimate (ADR-0044 decision 3), rendered under
/// `stats.estimate`, computed after snapshot resolution and before any page
/// fetch. Recorded alongside `stats.accounting`'s actuals so the estimate's
/// accuracy is a measurable quantity from outside the process.
#[derive(Debug, Serialize)]
pub struct CostEstimateJson {
    #[serde(rename = "estimatedRequests")]
    pub estimated_requests: u64,
    #[serde(rename = "estimatedStoreBytes")]
    pub estimated_store_bytes: u64,
    #[serde(rename = "estimatedDecompressedBytes")]
    pub estimated_decompressed_bytes: u64,
    pub segments: u64,
    pub series: u64,
}

impl From<ravel_types::accounting::CostEstimate> for CostEstimateJson {
    fn from(estimate: ravel_types::accounting::CostEstimate) -> Self {
        CostEstimateJson {
            estimated_requests: estimate.estimated_requests,
            estimated_store_bytes: estimate.estimated_store_bytes,
            estimated_decompressed_bytes: estimate.estimated_decompressed_bytes,
            segments: estimate.segments,
            series: estimate.series,
        }
    }
}

/// `/api/v1/query` and `/api/v1/query_range` response `data` object: the
/// Prometheus-shaped result, flattened alongside this query's segment
/// stats.
#[derive(Debug, Serialize)]
pub struct QueryResponseData {
    #[serde(flatten)]
    pub result: QueryData,
    pub stats: QueryStatsJson,
}

pub fn with_stats(result: QueryData, stats: crate::QueryStats) -> QueryResponseData {
    QueryResponseData {
        result,
        stats: stats.into(),
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "resultType", content = "result")]
pub enum QueryData {
    #[serde(rename = "vector")]
    Vector(Vec<VectorResult>),
    #[serde(rename = "matrix")]
    Matrix(Vec<MatrixResult>),
    #[serde(rename = "scalar")]
    Scalar((Timestamp, String)),
    #[serde(rename = "string")]
    String((Timestamp, String)),
}

/// One instant-vector element. Prometheus renders a float sample under a
/// `value` field and a native-histogram sample under a `histogram` field;
/// exactly one is present per element (a series is scalar or histogram in
/// storage, never both), so both are `Option` with `omitempty`, matching
/// Prometheus' `web/api/v1` sample shape.
#[derive(Debug, Serialize)]
pub struct VectorResult {
    pub metric: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<(Timestamp, String)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub histogram: Option<(Timestamp, HistogramJson)>,
}

/// One range-vector (matrix) series. Prometheus renders float steps under a
/// `values` array and native-histogram steps under a `histograms` array, per
/// element type per timestamp; a series may carry either or both across its
/// grid (a series that switches representation mid-range is float at some
/// steps and histogram at others). Both are `omitempty`, matching
/// Prometheus' `web/api/v1` matrix sample shape, so a float-only series omits
/// `histograms` and a histogram-only series omits `values`.
#[derive(Debug, Serialize)]
pub struct MatrixResult {
    pub metric: HashMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<(Timestamp, String)>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub histograms: Vec<(Timestamp, HistogramJson)>,
}

/// Prometheus' `model.SampleHistogram` JSON shape for a native (exponential)
/// histogram value: `count` and `sum` as strings (same
/// full-precision string encoding as a float sample value), and `buckets` an
/// array of `[boundaries, lower, upper, count]` tuples in cumulative ascending
/// order. Field names match Prometheus so the differential comparator (a
/// follow-up task) can compare against real Prometheus output.
#[derive(Debug, Serialize)]
pub struct HistogramJson {
    pub count: String,
    pub sum: String,
    pub buckets: Vec<HistogramBucketJson>,
}

/// One native-histogram bucket, rendered exactly as Prometheus renders it: a
/// 4-element JSON array `[boundaries, lower, upper, count]`. `boundaries`
/// encodes interval openness (Prometheus' convention): 0 left-open/right-closed
/// `(lower, upper]`, 1 left-closed/right-open `[lower, upper)`, 2 open both,
/// 3 closed both.
#[derive(Debug, Serialize)]
pub struct HistogramBucketJson(pub i32, pub String, pub String, pub String);

/// A query result timestamp, rendered in JSON as Prometheus' Go encoder
/// renders it: a whole-second value is a bare integer (`1700000150`), not
/// serde_json's default `f64` encoding, which always keeps a fractional
/// part (`1700000150.0`) to disambiguate an `f64` from a JSON integer on
/// deserialize. Matching Prometheus' actual wire text, not just its
/// numeric value, is the bit-exact HTTP-API parity ADR-0021 requires.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timestamp(pub f64);

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_finite() && self.0.fract() == 0.0 && self.0.abs() < 2f64.powi(53) {
            serializer.serialize_i64(self.0 as i64)
        } else {
            serializer.serialize_f64(self.0)
        }
    }
}

/// Prometheus renders a sample value as a JSON string, not a number, so
/// full f64 precision survives round-tripping through any JSON library.
pub fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    }
}

fn labels_to_map(labels: &LabelSet) -> HashMap<String, String> {
    labels
        .iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

/// Reported sample timestamps from the evaluator are always exact
/// multiples of 1ms in nanoseconds (docs/query-engine.md, `ravel_promql`
/// eval module doc), so this floor-division never loses precision.
fn ts_ns_to_seconds(ts_ns: i64) -> Timestamp {
    Timestamp((ts_ns as f64) / 1_000_000_000.0)
}

/// Render one instant-vector element: a native-histogram element
/// (`histogram: Some`) becomes Prometheus' `histogram` field, a float element
/// its `value` field. The evaluator leaves `value` at `0.0` on a histogram
/// element, so the two are never both meaningful.
fn vector_result(s: ravel_promql::InstantSample) -> VectorResult {
    let metric = labels_to_map(&s.labels);
    let ts = ts_ns_to_seconds(s.ts_ns);
    match s.histogram {
        Some(h) => VectorResult {
            metric,
            value: None,
            histogram: Some((ts, histogram_to_json(&h))),
        },
        None => VectorResult {
            metric,
            value: Some((ts, format_value(s.value))),
            histogram: None,
        },
    }
}

/// Convert a [`FloatHistogram`] to Prometheus' `model.SampleHistogram` JSON
/// shape: `count`/`sum` as strings, and every populated bucket in
/// cumulative ascending order (the order `FloatHistogram::all_buckets` yields,
/// itself matching Prometheus' `AllFloatBucketIterator`). Zero-count buckets
/// are skipped, exactly as Prometheus' marshaler skips them.
fn histogram_to_json(h: &FloatHistogram) -> HistogramJson {
    let buckets = h
        .all_buckets()
        .into_iter()
        .filter(|b| b.count != 0.0)
        .map(|b| {
            HistogramBucketJson(
                bucket_boundaries(b.lower, b.upper),
                format_value(b.lower),
                format_value(b.upper),
                format_value(b.count),
            )
        })
        .collect();
    HistogramJson {
        count: format_value(h.count),
        sum: format_value(h.sum),
        buckets,
    }
}

/// Prometheus' interval-openness code for a bucket, computed exactly as
/// Prometheus does (`histogram.Bucket`: `LowerInclusive = lower < 0`,
/// `UpperInclusive = upper > 0`): 0 left-open/right-closed, 1
/// left-closed/right-open, 2 open both, 3 closed both. Positive buckets are
/// `(lower, upper]` (0), negative buckets `[lower, upper)` (1), the zero bucket
/// `[lower, upper]` (3).
fn bucket_boundaries(lower: f64, upper: f64) -> i32 {
    let lower_inclusive = lower < 0.0;
    let upper_inclusive = upper > 0.0;
    match (lower_inclusive, upper_inclusive) {
        (true, true) => 3,
        (true, false) => 1,
        (false, true) => 0,
        (false, false) => 2,
    }
}

/// Render an [`Evaluator::eval_instant`] result as a Prometheus-shaped
/// `/api/v1/query` payload. A top-level range vector is a valid instant
/// result: Prometheus renders `resultType: matrix` for an instant query
/// whose top-level expression is itself a range vector (e.g. a bare
/// subquery, or `max_over_time(rate(...)[5m:1m])[10m:2m]`), and Ravel now
/// matches that. Unlike the scalar/string case in `range_value_to_json`,
/// which repeats a constant across the query-range grid, a matrix from an
/// instant query already carries its own per-series timestamps from the
/// evaluator, so no grid bounds are needed to render it.
pub fn instant_value_to_json(value: Value, time_ms: i64) -> Result<QueryData, ApiError> {
    match value {
        Value::Vector(vector) => Ok(QueryData::Vector(
            vector.into_iter().map(vector_result).collect(),
        )),
        Value::Scalar(v) => Ok(QueryData::Scalar((
            ms_to_seconds(time_ms)?,
            format_value(v),
        ))),
        Value::String(s) => Ok(QueryData::String((ms_to_seconds(time_ms)?, s))),
        // An instant query's top-level `Value::Matrix` is float-only by
        // construction: the histogram-aware channel is the range endpoint's
        // `RangeValue` (ADR-0108), and the two instant shapes that reach this
        // arm both keep histograms out of it -- a subquery over native
        // histograms refuses upstream (`eval_subquery_matrix`), and a bare
        // matrix selector matching histogram data refuses in
        // `eval_expr`'s `MatrixSelector` arm (issue #643). So `histograms` is
        // always empty here; a histogram-carrying instant range vector is a
        // typed 422, never a silent HTTP 200 with the histograms dropped.
        Value::Matrix(matrix) => Ok(QueryData::Matrix(
            matrix
                .into_iter()
                .map(|(labels, samples)| MatrixResult {
                    metric: labels_to_map(&labels),
                    values: samples
                        .into_iter()
                        .map(|s| (ts_ns_to_seconds(s.ts_ns), format_value(s.value)))
                        .collect(),
                    histograms: Vec::new(),
                })
                .collect(),
        )),
    }
}

/// Render an [`Evaluator::eval_range`] result as a Prometheus-shaped
/// `/api/v1/query_range` payload. A scalar or string top-level result is
/// constant across the whole grid (`eval_range`'s own doc comment), so
/// Prometheus renders it as a `matrix` with one synthetic empty-labeled
/// series repeating that value at every evaluated step; this materializes
/// that repetition, which `eval_range` deliberately defers to the wire
/// format layer.
pub fn range_value_to_json(
    value: RangeValue,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<QueryData, ApiError> {
    match value {
        RangeValue::Matrix(matrix) => Ok(QueryData::Matrix(
            matrix
                .into_iter()
                .map(|(labels, samples)| range_series_to_matrix_result(labels, samples))
                .collect(),
        )),
        RangeValue::Scalar(v) => Ok(QueryData::Matrix(vec![repeated_matrix_result(
            start_ms,
            end_ms,
            step_ms,
            format_value(v),
        )?])),
        RangeValue::String(s) => Ok(QueryData::Matrix(vec![repeated_matrix_result(
            start_ms, end_ms, step_ms, s,
        )?])),
    }
}

/// Split one range-vector series' per-step [`RangeSample`]s into Prometheus'
/// two per-element-type arrays: float steps under `values`, native-histogram
/// steps under `histograms`. A histogram step's placeholder `0.0` float
/// (`RangeSample::histogram`) is never emitted as a float; it renders as a
/// histogram element through the same [`histogram_to_json`] encoder the
/// instant path uses, keeping the two endpoints byte-compatible.
fn range_series_to_matrix_result(labels: LabelSet, samples: Vec<RangeSample>) -> MatrixResult {
    let mut values = Vec::new();
    let mut histograms = Vec::new();
    for s in samples {
        let ts = ts_ns_to_seconds(s.ts_ns);
        match s.histogram {
            Some(h) => histograms.push((ts, histogram_to_json(&h))),
            None => values.push((ts, format_value(s.value))),
        }
    }
    MatrixResult {
        metric: labels_to_map(&labels),
        values,
        histograms,
    }
}

fn ms_to_seconds(ms: i64) -> Result<Timestamp, ApiError> {
    Ok(ts_ns_to_seconds(
        ms_to_ns(ms).map_err(|e| ApiError::BadData(e.to_string()))?,
    ))
}

/// Builds the repeated-value series for a scalar/string range result, over
/// the exact same evaluation grid `eval_range` used internally: `start`,
/// `start + step`, ..., stopping at the last instant `<= end`.
fn repeated_matrix_result(
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    value: String,
) -> Result<MatrixResult, ApiError> {
    let start_ns = ms_to_ns(start_ms).map_err(|e| ApiError::BadData(e.to_string()))?;
    let end_ns = ms_to_ns(end_ms).map_err(|e| ApiError::BadData(e.to_string()))?;
    let step_ns = ms_to_ns(step_ms).map_err(|e| ApiError::BadData(e.to_string()))?;

    let mut values = Vec::new();
    let mut t = start_ns;
    while t <= end_ns {
        values.push((ts_ns_to_seconds(t), value.clone()));
        t += step_ns;
    }
    Ok(MatrixResult {
        metric: HashMap::new(),
        values,
        histograms: Vec::new(),
    })
}

pub fn series_to_json(series: Vec<(SeriesId, LabelSet)>) -> Vec<HashMap<String, String>> {
    series
        .into_iter()
        .map(|(_, labels)| labels_to_map(&labels))
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn matrix_result(data: QueryData) -> Vec<MatrixResult> {
        match data {
            QueryData::Matrix(m) => m,
            _ => panic!("expected matrix result, got a differently-shaped QueryData"),
        }
    }

    #[test]
    fn instant_scalar_renders_as_scalar_tuple_at_the_query_time() {
        let data =
            instant_value_to_json(Value::Scalar(42.5), 1_700_000_000_000).expect("scalar renders");
        match data {
            QueryData::Scalar((ts, value)) => {
                assert_eq!(ts, Timestamp(1_700_000_000.0));
                assert_eq!(value, "42.5");
            }
            _ => panic!("expected scalar result, got a differently-shaped QueryData"),
        }
    }

    #[test]
    fn instant_string_renders_as_string_tuple_at_the_query_time() {
        let data = instant_value_to_json(Value::String("up".to_string()), 1_700_000_000_000)
            .expect("string renders");
        match data {
            QueryData::String((ts, value)) => {
                assert_eq!(ts, Timestamp(1_700_000_000.0));
                assert_eq!(value, "up");
            }
            _ => panic!("expected string result, got a differently-shaped QueryData"),
        }
    }

    #[test]
    fn instant_matrix_renders_as_matrix_with_its_own_timestamps() {
        // An instant query whose top-level expression is itself a range
        // vector (e.g. a bare subquery, or a range function nested inside an
        // outer subquery) evaluates to a Value::Matrix. Prometheus renders
        // that as resultType: matrix for /api/v1/query, using the per-series
        // timestamps the evaluator already produced, not the single query
        // time. Ravel matches that here.
        use ravel_types::{Label, LabelSet, Sample};

        let labels = LabelSet::new(vec![Label {
            name: "job".to_string(),
            value: "api".to_string(),
        }])
        .expect("valid label set");
        let matrix = vec![(
            labels,
            vec![
                Sample {
                    ts_ns: 1_700_000_000_000_000_000,
                    value: 1.0,
                },
                Sample {
                    ts_ns: 1_700_000_120_000_000_000,
                    value: 2.5,
                },
            ],
        )];

        // time_ms is deliberately unrelated to the sample timestamps: the
        // matrix arm must ignore it and use each sample's own ts_ns.
        let data = instant_value_to_json(Value::Matrix(matrix), 9_999_999_999_000)
            .expect("matrix renders");
        let result = matrix_result(data);
        assert_eq!(result.len(), 1, "one series");
        assert_eq!(result[0].metric.get("job").map(String::as_str), Some("api"));
        assert_eq!(
            result[0].values,
            vec![
                (Timestamp(1_700_000_000.0), "1".to_string()),
                (Timestamp(1_700_000_120.0), "2.5".to_string()),
            ],
        );
    }

    #[test]
    fn range_scalar_repeats_the_value_across_the_whole_grid() {
        let data = range_value_to_json(
            RangeValue::Scalar(3.0),
            1_700_000_000_000,
            1_700_000_002_000,
            1_000,
        )
        .expect("scalar range renders");
        let matrix = matrix_result(data);
        assert_eq!(matrix.len(), 1, "one synthetic series");
        assert!(
            matrix[0].metric.is_empty(),
            "synthetic series has no labels"
        );
        let values: Vec<&str> = matrix[0].values.iter().map(|(_, v)| v.as_str()).collect();
        assert_eq!(values, vec!["3", "3", "3"], "one repeat per grid step");
    }

    #[test]
    fn range_string_repeats_the_value_across_the_whole_grid() {
        let data = range_value_to_json(
            RangeValue::String("idle".to_string()),
            1_700_000_000_000,
            1_700_000_001_000,
            1_000,
        )
        .expect("string range renders");
        let matrix = matrix_result(data);
        assert_eq!(matrix.len(), 1);
        let values: Vec<&str> = matrix[0].values.iter().map(|(_, v)| v.as_str()).collect();
        assert_eq!(values, vec!["idle", "idle"]);
    }

    #[test]
    fn whole_second_timestamp_renders_without_a_fractional_part() {
        // Prometheus' Go encoder emits a whole-second timestamp as a bare
        // JSON integer; serde_json's default f64 encoding would emit
        // `1700000150.0` instead, which is a real value this differential
        // test corpus caught against a pinned Prometheus binary.
        let json = serde_json::to_string(&Timestamp(1_700_000_150.0)).expect("serializes");
        assert_eq!(json, "1700000150");
    }

    #[test]
    fn fractional_timestamp_still_renders_its_fractional_part() {
        let json = serde_json::to_string(&Timestamp(1_700_000_150.5)).expect("serializes");
        assert_eq!(json, "1700000150.5");
    }

    #[test]
    fn success_envelope_carries_warnings_and_infos() {
        // Both annotation channels render as top-level arrays, matching
        // Prometheus' `warnings`/`infos` fields.
        let resp = ApiResponse::success_with_annotations(
            "data-goes-here",
            false,
            vec!["quantile value should be between 0 and 1, got 1.5".to_string()],
            vec!["needed to be fixed for monotonicity".to_string()],
        );
        let json = serde_json::to_value(&resp).expect("serializes");
        assert_eq!(json["status"], "success");
        assert_eq!(
            json["warnings"],
            serde_json::json!(["quantile value should be between 0 and 1, got 1.5"])
        );
        assert_eq!(
            json["infos"],
            serde_json::json!(["needed to be fixed for monotonicity"])
        );
    }

    #[test]
    fn success_envelope_omits_empty_annotation_arrays() {
        // Prometheus' `omitempty`: a query with no annotations carries
        // neither field, so the comparator's presence check reads them as
        // absent rather than present-but-empty.
        let resp = ApiResponse::success("data-goes-here");
        let json = serde_json::to_value(&resp).expect("serializes");
        assert!(json.get("warnings").is_none(), "empty warnings omitted");
        assert!(json.get("infos").is_none(), "empty infos omitted");
    }

    #[test]
    fn read_endpoint_envelope_always_carries_partial_even_when_false() {
        // ADR-0071 amendment decision 2: the top-level `partial` field is
        // present on every read-endpoint response, false on complete coverage
        // (NOT omitted like the annotation arrays). A strict client that models
        // status+data+partial then cannot deserialize a complete response
        // without acknowledging the coverage contract.
        //
        // Flip-line proof: switching `partial` to `skip_serializing_if` (the
        // omitempty treatment the ADR rejects) drops the field here and this
        // assertion fails.
        let complete =
            ApiResponse::success_with_annotations("data-goes-here", false, Vec::new(), Vec::new());
        let json = serde_json::to_value(&complete).expect("serializes");
        assert_eq!(json["status"], "success");
        assert_eq!(
            json["partial"],
            serde_json::json!(false),
            "partial:false must be present on a complete read response, not omitted"
        );

        let partial =
            ApiResponse::success_with_annotations("data-goes-here", true, Vec::new(), Vec::new());
        let json = serde_json::to_value(&partial).expect("serializes");
        assert_eq!(json["partial"], serde_json::json!(true));
    }

    #[test]
    fn bare_success_envelope_omits_partial() {
        // The stateless compatibility routes (buildinfo, metadata) make no
        // coverage claim, so their bare success envelope omits `partial`
        // entirely rather than assert `false`. This keeps the field off
        // responses Grafana's datasource parses for version detection, where an
        // unrelated coverage marker would be noise.
        let resp = ApiResponse::success("data-goes-here");
        let json = serde_json::to_value(&resp).expect("serializes");
        assert!(
            json.get("partial").is_none(),
            "bare success envelope must not carry a coverage marker"
        );
    }

    #[test]
    fn stats_json_carries_partial_and_warnings() {
        // BLOCK 1 (ADR-0071): the partial-coverage marker and the
        // per-skipped-cluster warnings must survive `QueryStats ->
        // QueryStatsJson`, never be silently dropped.
        //
        // Flip-line proof: drop `partial`/`warnings` from the `From` impl and
        // both assertions below fail (partial reads false, warnings empty).
        let stats = crate::QueryStats {
            partial: true,
            warnings: vec!["remote cluster eu-west unavailable; results are partial".to_string()],
            ..Default::default()
        };
        let json = QueryStatsJson::from(stats);
        assert!(json.partial, "partial marker carried through");
        assert_eq!(
            json.warnings,
            vec!["remote cluster eu-west unavailable; results are partial".to_string()],
            "skipped-cluster warnings carried through"
        );

        let value = serde_json::to_value(&json).expect("serializes");
        assert_eq!(value["partial"], serde_json::json!(true));
        assert_eq!(
            value["warnings"],
            serde_json::json!(["remote cluster eu-west unavailable; results are partial"])
        );
    }

    #[test]
    fn stats_json_omits_empty_warnings_and_defaults_partial_false() {
        let json = QueryStatsJson::from(crate::QueryStats::default());
        assert!(!json.partial, "a cluster-local query is never partial");
        let value = serde_json::to_value(&json).expect("serializes");
        assert_eq!(value["partial"], serde_json::json!(false));
        assert!(
            value.get("warnings").is_none(),
            "empty warnings array omitted"
        );
    }

    /// One phase's expected rendered figures, named rather than positional so a
    /// row of eleven numbers cannot be read against the wrong field.
    struct ExpectedPhase {
        phase: &'static str,
        gets: u64,
        wire_bytes: u64,
        lists: u64,
        hits: u64,
        misses: u64,
        cache_bytes: u64,
        decompressed: u64,
        reused: u64,
        opened: u64,
        matched: u64,
    }

    /// A `QueryStats` whose four phase handles carry a distinct, exactly known
    /// figure in every counter `stats.phases` renders, so a test can name the
    /// expected number per phase per field and a swapped or pooled phase is a
    /// failing assertion rather than a plausible-looking total.
    ///
    /// Each phase gets its own value for every field, and no two phases share
    /// a value in the same field, so an off-by-one in `QueryPhase::index` or a
    /// mis-wired `snapshot.phase(..)` lookup cannot pass.
    fn stats_with_distinct_phase_counters() -> crate::QueryStats {
        use crate::phase_accounting::PhaseAccounting;
        use ravel_types::accounting::AccountedOp;

        let phases = PhaseAccounting::new();

        // resolve: catalog LISTs and commit-record GETs.
        for _ in 0..3 {
            phases.resolve().record_s3_request(AccountedOp::Get);
        }
        phases.resolve().add_s3_bytes(AccountedOp::Get, 1_000);
        for _ in 0..2 {
            phases.resolve().record_s3_request(AccountedOp::List);
        }
        phases.resolve().record_cache_hit();
        for _ in 0..2 {
            phases.resolve().record_cache_miss();
        }
        phases.resolve().add_cache_bytes(11);

        // plan: footer and skip-index probes.
        for _ in 0..5 {
            phases.plan().record_s3_request(AccountedOp::Get);
        }
        phases.plan().add_s3_bytes(AccountedOp::Get, 2_000);
        for _ in 0..3 {
            phases.plan().record_cache_hit();
        }
        for _ in 0..4 {
            phases.plan().record_cache_miss();
        }
        phases.plan().add_cache_bytes(22);
        phases.plan().add_segments_opened(6);

        // probe: segment catalog fetch.
        for _ in 0..7 {
            phases.probe().record_s3_request(AccountedOp::Get);
        }
        phases.probe().add_s3_bytes(AccountedOp::Get, 4_000);
        for _ in 0..8 {
            phases.probe().record_cache_hit();
        }
        for _ in 0..9 {
            phases.probe().record_cache_miss();
        }
        phases.probe().add_cache_bytes(33);
        phases.probe().add_series_matched(12);

        // scan: block and page data reads, plus decode's own byte figures.
        for _ in 0..11 {
            phases.scan().record_s3_request(AccountedOp::Get);
        }
        phases.scan().add_s3_bytes(AccountedOp::Get, 8_000);
        for _ in 0..13 {
            phases.scan().record_cache_hit();
        }
        for _ in 0..14 {
            phases.scan().record_cache_miss();
        }
        phases.scan().add_cache_bytes(44);
        phases.scan().add_decompressed_bytes(5_000);
        phases.scan().add_bytes_reused(640);
        phases.scan().add_segments_opened(15);
        phases.scan().add_series_matched(16);

        let snapshot = phases.snapshot();
        crate::QueryStats {
            // `accounting` is the pooled sum in production
            // (`QueryStats::new`); keep that relationship here so the
            // sums-back-to-pooled assertions test the renderer, not a
            // hand-built inconsistency.
            accounting: snapshot.pooled(),
            phase_accounting: snapshot,
            ..Default::default()
        }
    }

    #[test]
    fn phase_accounting_renders_all_four_phases_exactly_once() {
        // ACCEPTANCE TEST (issue #935). `PhaseAccounting` splits every
        // `QueryAccounting` counter four ways on every PromQL query, and until
        // this field existed the split reached no reader outside the engine's
        // own unit tests. This pins the rendered shape: the exact phase names,
        // in `QueryPhase::ALL` order, each appearing exactly once, with every
        // figure at its exact expected value.
        //
        // Flip-line proof: in `phase_costs`, change `QueryPhase::ALL.iter()` to
        // `QueryPhase::ALL.iter().take(3)` and the count/name assertions fail;
        // hand the mapping `snapshot.phase(QueryPhase::Resolve)` instead of
        // `snapshot.phase(*phase)` and the per-phase value assertions fail.
        let json = QueryStatsJson::from(stats_with_distinct_phase_counters());
        let value = serde_json::to_value(&json).expect("serializes");

        let phases = value["phases"].as_array().expect("phases is an array");
        assert_eq!(phases.len(), 4, "four phases, no more and no fewer");

        // The exact names, in order. Not a set comparison: the order is
        // `QueryPhase::ALL`'s, and a report that pairs a phase name with the
        // wrong row is exactly what an unordered check would miss.
        let names: Vec<&str> = phases
            .iter()
            .map(|p| p["phase"].as_str().expect("phase name is a string"))
            .collect();
        assert_eq!(names, vec!["resolve", "plan", "probe", "scan"]);

        // Each name appears exactly once. A figure emitted twice is as much a
        // defect as one emitted wrong: a consumer summing the array would
        // double-count that phase.
        for want in ["resolve", "plan", "probe", "scan"] {
            assert_eq!(
                names.iter().filter(|n| **n == want).count(),
                1,
                "phase {want} must appear exactly once"
            );
        }

        // Every figure, per phase, at its exact expected value.
        let expected = [
            ExpectedPhase {
                phase: "resolve",
                gets: 3,
                wire_bytes: 1_000,
                lists: 2,
                hits: 1,
                misses: 2,
                cache_bytes: 11,
                decompressed: 0,
                reused: 0,
                opened: 0,
                matched: 0,
            },
            ExpectedPhase {
                phase: "plan",
                gets: 5,
                wire_bytes: 2_000,
                lists: 0,
                hits: 3,
                misses: 4,
                cache_bytes: 22,
                decompressed: 0,
                reused: 0,
                opened: 6,
                matched: 0,
            },
            ExpectedPhase {
                phase: "probe",
                gets: 7,
                wire_bytes: 4_000,
                lists: 0,
                hits: 8,
                misses: 9,
                cache_bytes: 33,
                decompressed: 0,
                reused: 0,
                opened: 0,
                matched: 12,
            },
            ExpectedPhase {
                phase: "scan",
                gets: 11,
                wire_bytes: 8_000,
                lists: 0,
                hits: 13,
                misses: 14,
                cache_bytes: 44,
                decompressed: 5_000,
                reused: 640,
                opened: 15,
                matched: 16,
            },
        ];
        for (got, want) in phases.iter().zip(expected) {
            let name = want.phase;
            assert_eq!(got["phase"], name);
            assert_eq!(got["s3GetRequests"], want.gets, "{name} s3GetRequests");
            assert_eq!(
                got["s3GetWireBytes"], want.wire_bytes,
                "{name} s3GetWireBytes"
            );
            assert_eq!(got["s3ListRequests"], want.lists, "{name} s3ListRequests");
            assert_eq!(got["cacheHits"], want.hits, "{name} cacheHits");
            assert_eq!(got["cacheMisses"], want.misses, "{name} cacheMisses");
            assert_eq!(
                got["cacheServedBytes"], want.cache_bytes,
                "{name} cacheServedBytes"
            );
            assert_eq!(
                got["decompressedOutputBytes"], want.decompressed,
                "{name} decompressedOutputBytes"
            );
            assert_eq!(
                got["reusedRegionBytes"], want.reused,
                "{name} reusedRegionBytes"
            );
            assert_eq!(got["segmentsOpened"], want.opened, "{name} segmentsOpened");
            assert_eq!(got["seriesMatched"], want.matched, "{name} seriesMatched");
        }
    }

    #[test]
    fn phase_figures_sum_back_to_the_pooled_accounting_block() {
        // The split is additive to `stats.accounting`, not a replacement: a
        // consumer reading only the pooled block sees the same numbers as
        // before `phases` existed. Asserted as exact equality per counter, so a
        // phase whose bytes land nowhere (or twice) fails here even if the
        // per-phase assertions above pass.
        let json = QueryStatsJson::from(stats_with_distinct_phase_counters());
        let value = serde_json::to_value(&json).expect("serializes");
        let phases = value["phases"].as_array().expect("phases is an array");

        let sum = |field: &str| -> u64 {
            phases
                .iter()
                .map(|p| p[field].as_u64().expect("figure is an unsigned integer"))
                .sum()
        };

        assert_eq!(sum("s3GetRequests"), 3 + 5 + 7 + 11);
        assert_eq!(value["accounting"]["s3GetRequests"], 26);
        assert_eq!(sum("s3GetWireBytes"), 1_000 + 2_000 + 4_000 + 8_000);
        assert_eq!(value["accounting"]["s3GetBytes"], 15_000);
        assert_eq!(sum("s3ListRequests"), 2);
        assert_eq!(value["accounting"]["s3ListRequests"], 2);
        assert_eq!(sum("cacheHits"), 1 + 3 + 8 + 13);
        assert_eq!(value["accounting"]["cacheHits"], 25);
        assert_eq!(sum("cacheMisses"), 2 + 4 + 9 + 14);
        assert_eq!(value["accounting"]["cacheMisses"], 29);
        assert_eq!(sum("cacheServedBytes"), 11 + 22 + 33 + 44);
        assert_eq!(value["accounting"]["cacheBytes"], 110);
        assert_eq!(sum("decompressedOutputBytes"), 5_000);
        assert_eq!(value["accounting"]["decompressedBytes"], 5_000);
        assert_eq!(sum("reusedRegionBytes"), 640);
        assert_eq!(value["accounting"]["bytesReused"], 640);
        assert_eq!(sum("segmentsOpened"), 6 + 15);
        assert_eq!(value["accounting"]["segmentsOpened"], 21);
        assert_eq!(sum("seriesMatched"), 12 + 16);
        assert_eq!(value["accounting"]["seriesMatched"], 28);
    }

    #[test]
    fn phases_render_every_phase_queryphase_all_names() {
        // The enforcement for "a phase added to `QueryPhase::ALL` cannot be
        // silently dropped": the renderer iterates `ALL`, and this test derives
        // its expectation from `ALL` too, so a fifth variant added to the enum
        // and to `ALL` must appear in the response or this fails. A test that
        // hard-coded four names could not tell a dropped fifth phase from a
        // correct render.
        use crate::phase_accounting::QueryPhase;

        let json = QueryStatsJson::from(crate::QueryStats::default());
        let value = serde_json::to_value(&json).expect("serializes");
        let phases = value["phases"].as_array().expect("phases is an array");

        assert_eq!(
            phases.len(),
            QueryPhase::ALL.len(),
            "one rendered entry per QueryPhase::ALL variant"
        );
        let names: Vec<&str> = phases
            .iter()
            .map(|p| p["phase"].as_str().expect("phase name is a string"))
            .collect();
        let want: Vec<&str> = QueryPhase::ALL.iter().map(|p| p.name()).collect();
        assert_eq!(names, want, "rendered names are QueryPhase::ALL, in order");
    }

    #[test]
    fn phases_carry_no_field_for_a_structurally_dead_counter() {
        // ADR-0927 decision 7: `AccountedOp::Head`, `s3_bytes[List]`,
        // `segments_pruned`, and `peak_intermediate_bytes` are never recorded
        // on a PromQL query. A per-phase zero for one of them would read as a
        // measurement, so the per-phase block has no field for it. The pooled
        // `accounting` block keeps its own pre-existing fields untouched, which
        // the second half of this test pins.
        let json = QueryStatsJson::from(stats_with_distinct_phase_counters());
        let value = serde_json::to_value(&json).expect("serializes");

        for phase in value["phases"].as_array().expect("phases is an array") {
            for dead in [
                "s3HeadRequests",
                "s3HeadBytes",
                "s3ListBytes",
                "peakIntermediateBytes",
                "segmentsPruned",
            ] {
                assert!(
                    phase.get(dead).is_none(),
                    "per-phase block must not render the dead counter {dead}"
                );
            }
        }

        // The pooled block is unchanged: every field it rendered before this
        // change is still there, dead ones included.
        for kept in [
            "s3GetRequests",
            "s3GetBytes",
            "s3ListRequests",
            "s3ListBytes",
            "s3HeadRequests",
            "s3HeadBytes",
            "cacheHits",
            "cacheMisses",
            "cacheBytes",
            "decompressedBytes",
            "segmentsOpened",
            "seriesMatched",
            "bytesReused",
            "peakIntermediateBytes",
            "rawF64Pages",
            "rawF64Bytes",
        ] {
            assert!(
                value["accounting"].get(kept).is_some(),
                "pooled accounting field {kept} must not be removed"
            );
        }
        assert!(
            value.get("segmentsFetched").is_some(),
            "segmentsFetched must not be removed"
        );
        assert!(
            value.get("segmentsPruned").is_some(),
            "segmentsPruned must not be removed"
        );
        assert!(
            value.get("estimate").is_some(),
            "estimate must not be removed"
        );
    }

    #[test]
    fn instant_vector_renders_native_histogram_element() {
        // A native-histogram vector element renders under Prometheus'
        // `histogram` field (not `value`), with count/sum as strings and each
        // populated bucket a [boundaries, lower, upper, count] array in
        // cumulative ascending order. This value has a negative bucket
        // (-2,-1] (boundaries 1), a zero bucket (-0.5,0.5] (boundaries 3), and
        // a positive bucket (1,2] (boundaries 0).
        use ravel_promql::{FloatHistogram, InstantSample, ResetHint, Span};
        use ravel_types::{Label, LabelSet};

        let h = FloatHistogram {
            counter_reset_hint: ResetHint::Unknown,
            scale: 0,
            zero_threshold: 0.5,
            zero_count: 4.0,
            count: 9.0,
            sum: 4.5,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            positive_buckets: vec![2.0],
            negative_buckets: vec![3.0],
            custom_values: Vec::new(),
        };
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "req_latency".to_string(),
        }])
        .expect("valid labels");
        let sample = InstantSample {
            labels,
            ts_ns: 1_700_000_000_000_000_000,
            orig_sample_ts_ns: 1_700_000_000_000_000_000,
            value: 0.0,
            histogram: Some(h),
        };

        let data = instant_value_to_json(Value::Vector(vec![sample]), 1_700_000_000_000)
            .expect("histogram vector renders");
        let json = serde_json::to_value(&data).expect("serializes");
        assert_eq!(json["resultType"], "vector");
        let elem = &json["result"][0];
        assert_eq!(elem["metric"]["__name__"], "req_latency");
        // Float `value` field is absent on a histogram element.
        assert!(elem.get("value").is_none(), "no value field on a histogram");
        let hist = &elem["histogram"];
        assert_eq!(hist[0], 1_700_000_000, "histogram carries the timestamp");
        assert_eq!(hist[1]["count"], "9");
        assert_eq!(hist[1]["sum"], "4.5");
        assert_eq!(
            hist[1]["buckets"],
            serde_json::json!([
                [1, "-2", "-1", "3"],
                [3, "-0.5", "0.5", "4"],
                [0, "1", "2", "2"],
            ])
        );
    }

    #[test]
    fn range_result_renders_native_histogram_field() {
        // A range-vector series whose steps are native histograms renders each
        // step under Prometheus' `histograms` array (not `values`), shaped
        // `[ts, {count, sum, buckets}]` with the same per-element encoding the
        // instant path uses. The float `values` array is absent (omitempty) on
        // a histogram-only series. This histogram matches
        // `instant_vector_renders_native_histogram_element`: a negative bucket
        // (-2,-1] (boundaries 1), a zero bucket (-0.5,0.5] (boundaries 3), and
        // a positive bucket (1,2] (boundaries 0).
        use ravel_promql::{FloatHistogram, ResetHint, Span};
        use ravel_types::{Label, LabelSet};

        let make_hist = |sum: f64| FloatHistogram {
            counter_reset_hint: ResetHint::Unknown,
            scale: 0,
            zero_threshold: 0.5,
            zero_count: 4.0,
            count: 9.0,
            sum,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            positive_buckets: vec![2.0],
            negative_buckets: vec![3.0],
            custom_values: Vec::new(),
        };
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "req_latency".to_string(),
        }])
        .expect("valid labels");
        let matrix = vec![(
            labels,
            vec![
                RangeSample {
                    ts_ns: 1_700_000_000_000_000_000,
                    value: 0.0,
                    histogram: Some(make_hist(4.5)),
                },
                RangeSample {
                    ts_ns: 1_700_000_060_000_000_000,
                    value: 0.0,
                    histogram: Some(make_hist(5.5)),
                },
            ],
        )];

        let data = range_value_to_json(
            RangeValue::Matrix(matrix),
            1_700_000_000_000,
            1_700_000_060_000,
            60_000,
        )
        .expect("histogram matrix renders");
        let json = serde_json::to_value(&data).expect("serializes");
        assert_eq!(json["resultType"], "matrix");
        let elem = &json["result"][0];
        assert_eq!(elem["metric"]["__name__"], "req_latency");
        // A histogram-only series carries no float `values` array.
        assert!(
            elem.get("values").is_none(),
            "no values field on a histogram-only series"
        );
        let hists = &elem["histograms"];
        assert!(hists.is_array(), "histograms is an array");
        assert_eq!(hists.as_array().expect("array").len(), 2, "one per step");
        // First step: timestamp, count/sum as strings, buckets exact.
        assert_eq!(hists[0][0], 1_700_000_000, "first step timestamp");
        assert_eq!(hists[0][1]["count"], "9");
        assert_eq!(hists[0][1]["sum"], "4.5");
        assert_eq!(
            hists[0][1]["buckets"],
            serde_json::json!([
                [1, "-2", "-1", "3"],
                [3, "-0.5", "0.5", "4"],
                [0, "1", "2", "2"],
            ])
        );
        // Second step carries its own timestamp and sum.
        assert_eq!(hists[1][0], 1_700_000_060, "second step timestamp");
        assert_eq!(hists[1][1]["sum"], "5.5");
        assert_eq!(
            hists[1][1]["buckets"],
            serde_json::json!([
                [1, "-2", "-1", "3"],
                [3, "-0.5", "0.5", "4"],
                [0, "1", "2", "2"],
            ])
        );
    }

    #[test]
    fn instant_vector_renders_float_element_under_value_field() {
        // A plain float vector element keeps the `value` field and carries no
        // `histogram` field, so the two element shapes stay disjoint.
        use ravel_promql::InstantSample;
        use ravel_types::{Label, LabelSet};

        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "up".to_string(),
        }])
        .expect("valid labels");
        let sample = InstantSample {
            labels,
            ts_ns: 1_700_000_000_000_000_000,
            orig_sample_ts_ns: 1_700_000_000_000_000_000,
            value: 1.0,
            histogram: None,
        };
        let data = instant_value_to_json(Value::Vector(vec![sample]), 1_700_000_000_000)
            .expect("float vector renders");
        let json = serde_json::to_value(&data).expect("serializes");
        let elem = &json["result"][0];
        assert_eq!(elem["value"], serde_json::json!([1_700_000_000, "1"]));
        assert!(
            elem.get("histogram").is_none(),
            "no histogram field on a float element"
        );
    }
}
