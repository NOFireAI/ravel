//! Normalization from the version-blind [`ResolvedRequest`] into Ravel's
//! canonical metric point representation (ADR-0015, docs/ingest-breadth-plan.md
//! section 2.1).
//!
//! Unlike `ravel_otlp::normalize`, this module never sanitizes a label name
//! or metric name: Remote Write payloads are already in the Prometheus data
//! model, and mutating them would silently alias a series relative to what
//! the sender believes it wrote. Every check here is validation, not
//! mutation.
//!
//! Per RW convention, a label with an empty value is treated as absent and
//! dropped before duplicate-name and length checks run; this is expected
//! Prometheus behavior (used by classic senders to signal "no value"), not
//! a leniency this crate invented.
//!
//! Values pass through as raw `f64` bit patterns, so the Prometheus stale
//! marker (a specific NaN payload) round-trips unchanged: to the storage
//! layer it is an ordinary sample.
//!
//! Native `Histogram` messages are admitted since RSEG v5 gave them durable
//! storage (ADR-0017, docs/rseg-v3-plan.md phase C8). Their wire shape is
//! close to the storage model's but not identical: integer bucket sides
//! arrive as deltas and are accumulated to the absolute counts storage
//! holds, and every structural rule the RSEG v5 writer or reader enforces is
//! re-checked here so an accepted point can never fail either (see
//! [`build_histogram_value`]).

use ravel_otlp::normalize::{NormalizedHistogramPoint, NormalizedPoint};
use ravel_otlp::{IngestLimits, Rejection};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ResetHint as SegResetHint,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};

use crate::resolved::{
    ResolvedCount, ResolvedHistogram, ResolvedRequest, ResolvedSample, ResolvedSeries, ResolvedSpan,
};

/// Why a Remote Write series or sample was not admitted.
///
/// `Otlp` wraps the semantics [`ravel_otlp::Rejection`] already defines
/// (label/timestamp/limit validation is identical to OTLP's) rather than
/// redefining them; `count` carries how many resolved data points (samples
/// plus native-histogram entries) this rejection accounts for, since a
/// single RW series can hold many points behind one label-validation
/// failure, unlike `Rejection`'s own per-variant count fields, which are
/// sized for OTLP's per-metric or per-resource grouping.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RwRejection {
    #[error("{reason} (rejecting {count} point(s))")]
    Otlp { reason: Rejection, count: usize },

    #[error("label name is empty (rejecting {count} point(s))")]
    EmptyLabelName { count: usize },

    #[error(
        "native histogram schema {schema} is below the -53 custom-buckets sentinel, which no bucket layout defines"
    )]
    NativeHistogramSchemaUnsupported { schema: i32 },

    #[error(
        "native histogram count and zero_count disagree about integer vs float: a histogram sample is all-integer or all-float, never mixed"
    )]
    NativeHistogramCountKindMixed,

    #[error(
        "native histogram carries {kind} counts but the bucket arrays for the other count kind are populated"
    )]
    NativeHistogramBucketEncodingMixed { kind: &'static str },

    #[error("native histogram bucket span has zero length")]
    NativeHistogramSpanLengthZero,

    #[error(
        "native histogram has {buckets} bucket count(s) on the {side} side but its spans cover {expected}"
    )]
    NativeHistogramBucketCountLenMismatch {
        side: &'static str,
        buckets: usize,
        expected: u64,
    },

    #[error("native histogram bucket delta sequence leaves an absolute count outside u64")]
    NativeHistogramDeltaOutOfRange,

    #[error(
        "native histogram custom_values must be non-empty and strictly ascending when schema == -53, and absent otherwise"
    )]
    NativeHistogramCustomValuesMismatch,

    #[error(
        "native histogram count is smaller than its zero_count or than the sum of its bucket counts, which the segment format's reader would reject as corrupted"
    )]
    NativeHistogramCountInconsistent,

    #[error("sample timestamp in milliseconds overflows nanosecond conversion")]
    TimestampOverflow,
}

impl RwRejection {
    /// Number of underlying resolved data points this rejection accounts
    /// for; mirrors [`Rejection::rejected_count`] for the RW surface.
    pub fn rejected_count(&self) -> usize {
        match self {
            RwRejection::Otlp { count, .. } => *count,
            RwRejection::EmptyLabelName { count } => *count,
            RwRejection::NativeHistogramSchemaUnsupported { .. }
            | RwRejection::NativeHistogramCountKindMixed
            | RwRejection::NativeHistogramBucketEncodingMixed { .. }
            | RwRejection::NativeHistogramSpanLengthZero
            | RwRejection::NativeHistogramBucketCountLenMismatch { .. }
            | RwRejection::NativeHistogramDeltaOutOfRange
            | RwRejection::NativeHistogramCustomValuesMismatch
            | RwRejection::NativeHistogramCountInconsistent
            | RwRejection::TimestampOverflow => 1,
        }
    }
}

/// Result of normalizing one resolved Remote Write request.
#[derive(Debug, Clone, PartialEq)]
pub struct RwNormalizeOutput {
    pub points: Vec<NormalizedPoint>,
    /// Native histogram samples admitted as [`NormalizedHistogramPoint`]s,
    /// carried separately from scalar `points` (matching the OTLP surface's
    /// split). Its length equals `histograms_written`, which the RW2
    /// histograms-written stats header reports.
    pub histogram_points: Vec<NormalizedHistogramPoint>,
    pub rejected: Vec<RwRejection>,
    /// Native histogram samples admitted and written, which is what the RW2
    /// histograms-written stats header reports
    /// (docs/ingest-breadth-plan.md section 2.1). Nonzero since RSEG v5 gave
    /// native histograms durable storage (docs/rseg-v3-plan.md phase C8);
    /// before that every native histogram landed in `histograms_dropped`.
    pub histograms_written: usize,
    /// Native histogram samples seen and rejected (a subset of `rejected`'s
    /// point count; broken out because the RW stats surface reports it as
    /// its own header, distinct from the generic rejected-sample count).
    pub histograms_dropped: usize,
    /// Exemplars accepted-and-dropped (ADR-0017 deferral): every exemplar
    /// attached to an otherwise-processed series counts here, whether or
    /// not that series' own points were admitted.
    pub exemplars_dropped: usize,
    /// Metric metadata entries accepted-and-dropped: Ravel has no
    /// metric-metadata store yet (ADR-0015).
    pub metadata_dropped: usize,
    /// Created/start timestamps accepted-and-dropped. Always 0 for RW1:
    /// `prometheus.Sample` has no such field on the wire; RW2's
    /// `Sample.start_timestamp` and `Histogram.start_timestamp` populate
    /// [`ResolvedRequest::created_timestamps_count`], which this mirrors.
    pub created_timestamps_dropped: usize,
}

/// Normalize a resolved Remote Write request into Ravel canonical points.
///
/// `ingest_ts_ns` is the receiver's clock reading at admission time, used to
/// bound event-time skew identically to the OTLP surface (ADR-0010 section
/// 8). Nothing here panics for malformed or oversized input: every problem
/// becomes an [`RwRejection`].
pub fn normalize_resolved(
    tenant: &TenantId,
    resolved: ResolvedRequest,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
) -> RwNormalizeOutput {
    // Native histograms count toward the per-request data-point limit now
    // that they are admitted rather than rejected: one histogram sample is
    // one stored data point, the same as one scalar sample.
    let total_points: usize = resolved
        .series
        .iter()
        .map(|s| s.samples.len() + s.histograms.len())
        .sum();
    if total_points > limits.max_data_points_per_request {
        return RwNormalizeOutput {
            points: Vec::new(),
            histogram_points: Vec::new(),
            rejected: vec![RwRejection::Otlp {
                reason: Rejection::TooManyDataPoints {
                    count: total_points,
                    max: limits.max_data_points_per_request,
                },
                count: total_points,
            }],
            histograms_written: 0,
            histograms_dropped: 0,
            exemplars_dropped: 0,
            metadata_dropped: resolved.metadata_count,
            created_timestamps_dropped: resolved.created_timestamps_count,
        };
    }

    let mut points = Vec::new();
    let mut histogram_points = Vec::new();
    let mut rejected = Vec::new();
    let mut counts = HistogramTallies::default();
    let mut exemplars_dropped = 0usize;

    for series in &resolved.series {
        exemplars_dropped += series.exemplar_count;
        normalize_series(
            tenant,
            series,
            limits,
            ingest_ts_ns,
            &mut points,
            &mut histogram_points,
            &mut rejected,
            &mut counts,
        );
    }

    RwNormalizeOutput {
        points,
        histogram_points,
        rejected,
        histograms_written: counts.written,
        histograms_dropped: counts.dropped,
        exemplars_dropped,
        metadata_dropped: resolved.metadata_count,
        created_timestamps_dropped: resolved.created_timestamps_count,
    }
}

/// Running native-histogram tallies for one request.
#[derive(Debug, Default)]
struct HistogramTallies {
    written: usize,
    dropped: usize,
}

#[allow(clippy::too_many_arguments)]
fn normalize_series(
    tenant: &TenantId,
    series: &ResolvedSeries,
    limits: &IngestLimits,
    ingest_ts_ns: i64,
    points: &mut Vec<NormalizedPoint>,
    histogram_points: &mut Vec<NormalizedHistogramPoint>,
    rejected: &mut Vec<RwRejection>,
    counts: &mut HistogramTallies,
) {
    let point_count = series.samples.len() + series.histograms.len();

    let (metric_name, label_set) = match resolve_series_identity(series, limits, point_count) {
        Ok(v) => v,
        Err(rejection) => {
            rejected.push(rejection);
            return;
        }
    };

    let series_id = match SeriesId::compute(tenant, &metric_name, &label_set) {
        Ok(id) => id,
        Err(_) => {
            rejected.push(RwRejection::Otlp {
                reason: Rejection::OversizedSeriesComponent,
                count: point_count,
            });
            return;
        }
    };

    for histogram in &series.histograms {
        match build_histogram_sample(histogram, ingest_ts_ns, limits) {
            Ok(sample) => {
                counts.written += 1;
                histogram_points.push(NormalizedHistogramPoint {
                    series_id,
                    labels: label_set.clone(),
                    sample,
                });
            }
            Err(reason) => {
                counts.dropped += 1;
                rejected.push(reason);
            }
        }
    }

    for sample in &series.samples {
        match build_sample(sample, ingest_ts_ns, limits) {
            Ok(sample) => points.push(NormalizedPoint {
                series_id,
                labels: label_set.clone(),
                sample,
                // RW1's metadata is a flat request-level list with no
                // per-series correlation (ADR-0015), so whether this
                // series' underlying metric is a monotonic sum is not
                // knowable here; conservatively false, matching the
                // metadata-is-dropped scope of this phase.
                is_monotonic_sum: false,
            }),
            Err(reason) => rejected.push(reason),
        }
    }
}

/// Resolve one series' `__name__` and remaining labels into a metric name
/// and validated [`LabelSet`], per docs/ingest-breadth-plan.md section 2.1:
/// `__name__` must be present and non-empty; an empty label *name* is
/// rejected as malformed regardless of its value (ADR-0031), which is what
/// RW2 already enforces at decode (`Rw2DecodeError::EmptyLabelName`) and RW1
/// did not; enforcing it here makes both decoders agree on the resolved
/// series and keeps an empty-named label out of `SeriesId::compute`.
/// Empty-*value* labels are dropped as absent (ADR-0031, unchanged);
/// duplicate names (including a duplicated `__name__`) reject; existing
/// `IngestLimits` length/count limits apply unchanged, with
/// `max_attributes_per_point` counted over labels excluding `__name__` to
/// match the OTLP surface, where the metric name is a separate proto field.
fn resolve_series_identity(
    series: &ResolvedSeries,
    limits: &IngestLimits,
    point_count: usize,
) -> Result<(String, LabelSet), RwRejection> {
    let otlp = |reason| RwRejection::Otlp {
        reason,
        count: point_count,
    };

    let mut name_labels = series.labels.iter().filter(|l| l.name == METRIC_NAME_LABEL);
    let name_label = name_labels.next();
    if name_labels.next().is_some() {
        return Err(otlp(Rejection::DuplicateLabelName(
            METRIC_NAME_LABEL.to_string(),
        )));
    }
    let metric_name = match name_label {
        None => return Err(otlp(Rejection::EmptyMetricName { count: point_count })),
        Some(l) if l.value.is_empty() => {
            return Err(otlp(Rejection::EmptyMetricName { count: point_count }));
        }
        Some(l) => l.value.clone(),
    };
    if metric_name.len() > limits.max_metric_name_len {
        return Err(otlp(Rejection::MetricNameTooLong {
            len: metric_name.len(),
            max: limits.max_metric_name_len,
            count: point_count,
        }));
    }

    let mut labels = Vec::with_capacity(series.labels.len());
    for l in &series.labels {
        // Reject an empty label name before the empty-value drop below:
        // ADR-0031 rejects an empty name independent of its value, so a
        // `{name: "", value: ""}` label must not slip through as "absent".
        if l.name.is_empty() {
            return Err(RwRejection::EmptyLabelName { count: point_count });
        }
        if l.name == METRIC_NAME_LABEL || l.value.is_empty() {
            continue;
        }
        if l.name.len() > limits.max_label_name_len {
            return Err(otlp(Rejection::LabelNameTooLong {
                len: l.name.len(),
                max: limits.max_label_name_len,
            }));
        }
        if l.value.len() > limits.max_label_value_len {
            return Err(otlp(Rejection::LabelValueTooLong {
                len: l.value.len(),
                max: limits.max_label_value_len,
            }));
        }
        labels.push(l.clone());
    }
    if labels.len() > limits.max_attributes_per_point {
        return Err(otlp(Rejection::TooManyAttributes {
            attribute_count: labels.len(),
            max: limits.max_attributes_per_point,
        }));
    }
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.clone(),
    });

    let label_set = LabelSet::new(labels).map_err(|err| match err {
        ravel_types::TypeError::DuplicateLabelName(name) => {
            otlp(Rejection::DuplicateLabelName(name))
        }
        // LabelSet::new only ever returns DuplicateLabelName; this arm
        // exists so a future TypeError variant can't silently pass through
        // as an accepted series.
        _ => otlp(Rejection::DuplicateLabelName(String::new())),
    })?;

    Ok((metric_name, label_set))
}

/// Convert one resolved sample's millisecond timestamp to nanoseconds and
/// apply the same skew/lag admission bounds as OTLP (ADR-0010 section 8).
fn build_sample(
    sample: &ResolvedSample,
    ingest_ts_ns: i64,
    limits: &IngestLimits,
) -> Result<Sample, RwRejection> {
    Ok(Sample {
        ts_ns: checked_event_ts(sample.ts_ms, ingest_ts_ns, limits)?,
        value: sample.value,
    })
}

/// Convert one resolved native histogram into a storage-side
/// [`HistogramSample`], under the same timestamp bounds a scalar sample gets.
fn build_histogram_sample(
    histogram: &ResolvedHistogram,
    ingest_ts_ns: i64,
    limits: &IngestLimits,
) -> Result<HistogramSample, RwRejection> {
    Ok(HistogramSample {
        ts_ns: checked_event_ts(histogram.ts_ms, ingest_ts_ns, limits)?,
        value: build_histogram_value(histogram)?,
    })
}

/// Build the storage-side [`HistogramValue`] from one wire native histogram
/// (ADR-0017, docs/rseg-v3-plan.md section 2/3.5).
///
/// Every check here mirrors one the RSEG v5 writer or reader performs, and
/// exists so that no accepted point can later fail either. That matters in
/// both directions. A value the *writer* rejects fails the whole flush for
/// the whole tenant buffer it landed in, not just the offending point, so one
/// malformed histogram would deny writes to unrelated series. A value the
/// *reader* rejects would be written durably and then reported corrupt at
/// every query, forever, since data objects are immutable.
///
/// The count-consistency rule is the reader's own (`count` at least
/// `zero_count`, and `count` at least the sum of the bucket counts), rather
/// than the stricter `count >= zero_count + bucket_sum` the OTLP surface
/// applies. Prometheus float histograms carry counts produced by arithmetic
/// on other histograms, so the sum rule rejects payloads no reader would
/// object to, and admission must reject exactly what a reader would: no
/// less, so nothing unreadable is ever stored, and no more, so no legitimate
/// series is silently dropped.
///
/// Custom-bucket histograms (`schema == -53`) are admitted, not rejected:
/// the wire format specifies them, the storage model represents their
/// boundaries losslessly, and this normalizer validates them to exactly the
/// writer's rule (non-empty, strictly ascending, present if and only if the
/// schema is the sentinel). The OTLP surface keeps rejecting `scale == -53`,
/// which is not an inconsistency: OTLP has no field to carry boundaries at
/// all, so there the sentinel can only ever arrive unbacked.
fn build_histogram_value(h: &ResolvedHistogram) -> Result<HistogramValue, RwRejection> {
    if h.schema < -53 {
        return Err(RwRejection::NativeHistogramSchemaUnsupported { schema: h.schema });
    }

    let custom_values = if h.schema == -53 {
        if h.custom_values.is_empty() || !h.custom_values.windows(2).all(|w| w[0] < w[1]) {
            return Err(RwRejection::NativeHistogramCustomValuesMismatch);
        }
        Some(h.custom_values.clone())
    } else {
        if !h.custom_values.is_empty() {
            return Err(RwRejection::NativeHistogramCustomValuesMismatch);
        }
        None
    };

    let positive_spans = build_spans(&h.positive_spans)?;
    let negative_spans = build_spans(&h.negative_spans)?;
    let positive_expected = spans_bucket_count(&positive_spans);
    let negative_expected = spans_bucket_count(&negative_spans);

    // The `count` oneof alone decides whether this sample is integer- or
    // float-valued, matching Prometheus's own `Histogram.IsFloatHistogram`.
    // An unset oneof reads as integer 0, which is what a `GetCountInt()`
    // reader sees; `zero_count` must then agree, since the storage model has
    // no mixed-kind representation and silently coercing one side would
    // change a sender's numbers.
    let counts = match (h.count, h.zero_count) {
        (Some(ResolvedCount::Float(count)), zero) => {
            let zero_count = match zero {
                Some(ResolvedCount::Float(v)) => v,
                None => 0.0,
                Some(ResolvedCount::Int(_)) => {
                    return Err(RwRejection::NativeHistogramCountKindMixed);
                }
            };
            if !h.positive_deltas.is_empty() || !h.negative_deltas.is_empty() {
                return Err(RwRejection::NativeHistogramBucketEncodingMixed { kind: "float" });
            }
            let positive =
                float_side_counts(&h.positive_counts, positive_expected, "positive")?.to_vec();
            let negative =
                float_side_counts(&h.negative_counts, negative_expected, "negative")?.to_vec();
            HistogramCounts::Float {
                zero_count,
                count,
                positive,
                negative,
            }
        }
        (count, zero) => {
            let count = match count {
                Some(ResolvedCount::Int(v)) => v,
                None => 0,
                // Unreachable: the float arm above matched every
                // `Some(Float)`.
                Some(ResolvedCount::Float(_)) => {
                    return Err(RwRejection::NativeHistogramCountKindMixed);
                }
            };
            let zero_count = match zero {
                Some(ResolvedCount::Int(v)) => v,
                None => 0,
                Some(ResolvedCount::Float(_)) => {
                    return Err(RwRejection::NativeHistogramCountKindMixed);
                }
            };
            if !h.positive_counts.is_empty() || !h.negative_counts.is_empty() {
                return Err(RwRejection::NativeHistogramBucketEncodingMixed { kind: "integer" });
            }
            HistogramCounts::Int {
                zero_count,
                count,
                positive: int_side_counts(&h.positive_deltas, positive_expected, "positive")?,
                negative: int_side_counts(&h.negative_deltas, negative_expected, "negative")?,
            }
        }
    };

    validate_counts_against_reader(&counts)?;

    Ok(HistogramValue {
        scale: h.schema,
        zero_threshold: h.zero_threshold,
        // RW's `sum` is a plain `double` with no presence on either wire
        // version, so a native histogram always carries one.
        sum: Some(h.sum),
        custom_values,
        positive_spans,
        negative_spans,
        counts,
        reset_hint: reset_hint_from_wire(h.reset_hint),
    })
}

/// Map wire spans to storage spans. A zero-length span is rejected here
/// because the writer rejects it (docs/rseg-v3-plan.md section 3.5): spans
/// describe *populated* runs, so an empty run has no meaning.
fn build_spans(spans: &[ResolvedSpan]) -> Result<Vec<HistogramSpan>, RwRejection> {
    spans
        .iter()
        .map(|s| {
            if s.length == 0 {
                return Err(RwRejection::NativeHistogramSpanLengthZero);
            }
            Ok(HistogramSpan {
                offset: s.offset,
                length: s.length,
            })
        })
        .collect()
}

/// Total buckets one side's spans cover. Saturating rather than checked: a
/// `u64` sum of `u32` lengths cannot overflow before the per-side bucket
/// array, which is bounded by the decoded message size, fails to match it.
fn spans_bucket_count(spans: &[HistogramSpan]) -> u64 {
    spans.iter().map(|s| u64::from(s.length)).sum()
}

/// Convert one integer side's wire deltas into the absolute counts storage
/// holds. Prometheus encodes each bucket as a delta against the previous
/// bucket on the same side, starting from zero, so a sequence whose running
/// total leaves `u64` (negative, or past `u64::MAX`) describes no valid
/// bucket population.
fn int_side_counts(
    deltas: &[i64],
    expected: u64,
    side: &'static str,
) -> Result<Vec<u64>, RwRejection> {
    if deltas.len() as u64 != expected {
        return Err(RwRejection::NativeHistogramBucketCountLenMismatch {
            side,
            buckets: deltas.len(),
            expected,
        });
    }
    let mut absolute = Vec::with_capacity(deltas.len());
    let mut running: u64 = 0;
    for delta in deltas {
        running = if delta.is_negative() {
            running
                .checked_sub(delta.unsigned_abs())
                .ok_or(RwRejection::NativeHistogramDeltaOutOfRange)?
        } else {
            running
                .checked_add(delta.unsigned_abs())
                .ok_or(RwRejection::NativeHistogramDeltaOutOfRange)?
        };
        absolute.push(running);
    }
    Ok(absolute)
}

/// Float sides carry absolute counts already, so only the length has to
/// agree with the spans.
fn float_side_counts<'a>(
    counts: &'a [f64],
    expected: u64,
    side: &'static str,
) -> Result<&'a [f64], RwRejection> {
    if counts.len() as u64 != expected {
        return Err(RwRejection::NativeHistogramBucketCountLenMismatch {
            side,
            buckets: counts.len(),
            expected,
        });
    }
    Ok(counts)
}

/// The RSEG v5 reader's own count-consistency rule, applied at admission so
/// nothing that fails it is ever written (see [`build_histogram_value`]).
fn validate_counts_against_reader(counts: &HistogramCounts) -> Result<(), RwRejection> {
    match counts {
        HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative,
        } => {
            let bucket_sum = positive
                .iter()
                .chain(negative)
                .try_fold(0u64, |acc, v| acc.checked_add(*v))
                .ok_or(RwRejection::NativeHistogramCountInconsistent)?;
            if count < zero_count || *count < bucket_sum {
                return Err(RwRejection::NativeHistogramCountInconsistent);
            }
        }
        HistogramCounts::Float {
            zero_count,
            count,
            positive,
            negative,
        } => {
            let bucket_sum: f64 = positive.iter().chain(negative).sum();
            // NaN compares false against everything, so a NaN count or
            // bucket passes here exactly as it passes in the reader: the
            // float path treats NaN as an ordinary payload, not corruption.
            if count < zero_count || *count < bucket_sum {
                return Err(RwRejection::NativeHistogramCountInconsistent);
            }
        }
    }
    Ok(())
}

/// Map the wire `reset_hint` discriminant. RW1 and RW2 number the four
/// states identically, and any unknown number degrades to `Unknown`, the
/// "test for a counter reset explicitly" state, which is the safe reading of
/// a hint a future sender adds.
fn reset_hint_from_wire(reset_hint: i32) -> SegResetHint {
    match reset_hint {
        1 => SegResetHint::Yes,
        2 => SegResetHint::No,
        3 => SegResetHint::Gauge,
        _ => SegResetHint::Unknown,
    }
}

/// Convert a millisecond wire timestamp to nanoseconds and apply the skew
/// and lag admission bounds (ADR-0010 section 8), shared by scalar samples
/// and native histograms.
fn checked_event_ts(
    ts_ms: i64,
    ingest_ts_ns: i64,
    limits: &IngestLimits,
) -> Result<i64, RwRejection> {
    let ts_ns = ts_ms
        .checked_mul(1_000_000)
        .ok_or(RwRejection::TimestampOverflow)?;
    if ts_ns == 0 {
        return Err(RwRejection::Otlp {
            reason: Rejection::ZeroTimestamp,
            count: 1,
        });
    }

    // Convention (ADR-0010 section 8): the bound itself passes, only
    // strictly exceeding it rejects.
    let skew_ns = ts_ns.saturating_sub(ingest_ts_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(RwRejection::Otlp {
            reason: Rejection::FutureSkew {
                skew_ns,
                max_ns: limits.max_future_skew_ns,
            },
            count: 1,
        });
    }
    let lag_ns = ingest_ts_ns.saturating_sub(ts_ns);
    if lag_ns > limits.max_ingest_lag_ns {
        return Err(RwRejection::Otlp {
            reason: Rejection::TooOld {
                lag_ns,
                max_ns: limits.max_ingest_lag_ns,
            },
            count: 1,
        });
    }

    Ok(ts_ns)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn tenant() -> TenantId {
        TenantId::new("acme")
    }

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn series(labels: Vec<Label>, samples: Vec<ResolvedSample>) -> ResolvedSeries {
        ResolvedSeries {
            labels,
            samples,
            histograms: vec![],
            exemplar_count: 0,
        }
    }

    /// A minimal well-formed integer native histogram: one populated
    /// positive span of three buckets whose deltas accumulate to 2, 5, 6,
    /// plus a zero bucket, all covered by `count`.
    fn histogram(ts_ms: i64) -> ResolvedHistogram {
        ResolvedHistogram {
            ts_ms,
            schema: 2,
            zero_threshold: 1e-9,
            sum: 42.5,
            count: Some(ResolvedCount::Int(14)),
            zero_count: Some(ResolvedCount::Int(1)),
            positive_spans: vec![ResolvedSpan {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            positive_deltas: vec![2, 3, 1],
            negative_deltas: vec![],
            positive_counts: vec![],
            negative_counts: vec![],
            reset_hint: 1,
            custom_values: vec![],
        }
    }

    /// The single admitted histogram sample from a one-series request.
    fn expect_histogram(out: &RwNormalizeOutput) -> &HistogramSample {
        assert_eq!(out.histogram_points.len(), 1, "{:?}", out.rejected);
        &out.histogram_points[0].sample
    }

    /// Normalize one series holding exactly `histograms` and no scalar
    /// samples, under default limits.
    fn normalize_histograms(histograms: Vec<ResolvedHistogram>) -> RwNormalizeOutput {
        let mut s = series(vec![label("__name__", "req_latency")], vec![]);
        s.histograms = histograms;
        normalize_resolved(
            &tenant(),
            request(vec![s]),
            &IngestLimits::default(),
            1_000_000,
        )
    }

    /// Assert `histogram` is rejected with exactly `expected`, and that
    /// nothing was admitted or counted as written.
    fn assert_histogram_rejected(histogram: ResolvedHistogram, expected: RwRejection) {
        let out = normalize_histograms(vec![histogram]);
        assert!(
            out.histogram_points.is_empty(),
            "{:?}",
            out.histogram_points
        );
        assert_eq!(out.histograms_written, 0);
        assert_eq!(out.histograms_dropped, 1);
        assert_eq!(out.rejected, vec![expected]);
    }

    fn sample(ts_ms: i64, value: f64) -> ResolvedSample {
        ResolvedSample { ts_ms, value }
    }

    fn request(series: Vec<ResolvedSeries>) -> ResolvedRequest {
        ResolvedRequest {
            series,
            metadata_count: 0,
            created_timestamps_count: 0,
        }
    }

    // --- happy path ---

    #[test]
    fn admits_a_well_formed_series() {
        let req = request(vec![series(
            vec![label("__name__", "up"), label("job", "svc")],
            vec![sample(1_700_000_000_000, 1.0)],
        )]);
        let out = normalize_resolved(
            &tenant(),
            req,
            &IngestLimits::default(),
            1_700_000_001_000_000_000,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 1);
        assert_eq!(out.points[0].labels.get("__name__"), Some("up"));
        assert_eq!(out.points[0].labels.get("job"), Some("svc"));
        assert_eq!(out.points[0].sample.ts_ns, 1_700_000_000_000_000_000);
        assert_eq!(out.points[0].sample.value, 1.0);
        assert!(!out.points[0].is_monotonic_sum);
    }

    #[test]
    fn no_sanitization_dotted_label_name_passes_through_unchanged() {
        // Unlike OTLP, RW labels are never mutated: a dotted name (invalid
        // as Prometheus label syntax, but this crate validates, not
        // sanitizes) is carried verbatim rather than rewritten to
        // underscores.
        let req = request(vec![series(
            vec![label("__name__", "up"), label("weird.name", "v")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].labels.get("weird.name"), Some("v"));
    }

    // --- empty-value labels dropped as absent ---

    #[test]
    fn empty_value_label_is_dropped_not_rejected() {
        let req = request(vec![series(
            vec![label("__name__", "up"), label("optional", "")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].labels.get("optional"), None);
    }

    // --- __name__ rules ---

    #[test]
    fn missing_name_label_rejects_whole_series() {
        let req = request(vec![series(
            vec![label("job", "svc")],
            vec![sample(1_000, 1.0), sample(2_000, 2.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::EmptyMetricName { count: 2 },
                count: 2,
            }]
        );
    }

    #[test]
    fn empty_name_value_rejects_whole_series() {
        let req = request(vec![series(
            vec![label("__name__", "")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::EmptyMetricName { count: 1 },
                count: 1,
            }]
        );
    }

    #[test]
    fn duplicate_name_label_rejects_series() {
        let req = request(vec![series(
            vec![label("__name__", "up"), label("__name__", "down")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::DuplicateLabelName("__name__".to_string()),
                count: 1,
            }]
        );
    }

    #[test]
    fn metric_name_too_long_rejects_series() {
        let limits = IngestLimits {
            max_metric_name_len: 3,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "toolong")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::MetricNameTooLong {
                    len: 7,
                    max: 3,
                    count: 1,
                },
                count: 1,
            }]
        );
    }

    // --- duplicate and oversized labels ---

    #[test]
    fn duplicate_label_name_rejects_series() {
        let req = request(vec![series(
            vec![
                label("__name__", "up"),
                label("job", "a"),
                label("job", "b"),
            ],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::DuplicateLabelName("job".to_string()),
                count: 1,
            }]
        );
    }

    #[test]
    fn empty_label_name_rejects_series() {
        // RW2 rejects this at decode (Rw2DecodeError::EmptyLabelName); the
        // normalizer must reject it too so RW1, which copies the empty name
        // verbatim, agrees and the empty-named label never reaches
        // SeriesId::compute (ADR-0031, issue #204).
        let req = request(vec![series(
            vec![label("__name__", "up"), label("", "surprise")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![RwRejection::EmptyLabelName { count: 1 }]);
    }

    #[test]
    fn empty_label_name_rejects_even_with_empty_value() {
        // ADR-0031 rejects an empty name independent of its value: the
        // empty-value "treat as absent" drop must not swallow it first.
        let req = request(vec![series(
            vec![label("__name__", "up"), label("", "")],
            vec![sample(1_000, 1.0), sample(2_000, 2.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![RwRejection::EmptyLabelName { count: 2 }]);
    }

    #[test]
    fn too_many_attributes_excludes_name_label() {
        let limits = IngestLimits {
            max_attributes_per_point: 1,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up"), label("a", "1"), label("b", "2")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::TooManyAttributes {
                    attribute_count: 2,
                    max: 1,
                },
                count: 1,
            }]
        );
    }

    #[test]
    fn label_name_too_long_rejects_series() {
        let limits = IngestLimits {
            max_label_name_len: 3,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up"), label("toolong", "v")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::LabelNameTooLong { len: 7, max: 3 },
                count: 1,
            }]
        );
    }

    #[test]
    fn label_value_too_long_rejects_series() {
        let limits = IngestLimits {
            max_label_value_len: 3,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up"), label("k", "toolong")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::LabelValueTooLong { len: 7, max: 3 },
                count: 1,
            }]
        );
    }

    // --- timestamps ---

    #[test]
    fn zero_timestamp_rejected() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(0, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::ZeroTimestamp,
                count: 1,
            }]
        );
    }

    #[test]
    fn ms_to_ns_overflow_rejected_typed() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(i64::MAX, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(out.rejected, vec![RwRejection::TimestampOverflow]);
    }

    #[test]
    fn future_skew_exactly_at_bound_passes() {
        let limits = IngestLimits {
            max_future_skew_ns: 100,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000;
        let event_ts_ns = ingest_ts + 100;
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(event_ts_ns / 1_000_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, ingest_ts);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    }

    #[test]
    fn future_skew_one_ns_past_bound_fails() {
        let limits = IngestLimits {
            max_future_skew_ns: 0,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000_000;
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(ingest_ts / 1_000_000 + 1, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, ingest_ts);
        assert!(out.points.is_empty());
        assert!(matches!(
            out.rejected[0],
            RwRejection::Otlp {
                reason: Rejection::FutureSkew { .. },
                ..
            }
        ));
    }

    #[test]
    fn too_old_one_ns_past_bound_fails() {
        let limits = IngestLimits {
            max_ingest_lag_ns: 0,
            ..IngestLimits::default()
        };
        let ingest_ts = 1_000_000_000;
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(ingest_ts / 1_000_000 - 1, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, ingest_ts);
        assert!(out.points.is_empty());
        assert!(matches!(
            out.rejected[0],
            RwRejection::Otlp {
                reason: Rejection::TooOld { .. },
                ..
            }
        ));
    }

    // --- stale marker bit-pattern round trip ---

    #[test]
    fn stale_marker_nan_bit_pattern_round_trips() {
        const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
        let stale = f64::from_bits(STALE_NAN_BITS);
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, stale)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value.to_bits(), STALE_NAN_BITS);
    }

    #[test]
    fn negative_zero_bit_pattern_round_trips() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, -0.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points[0].sample.value.to_bits(), (-0.0f64).to_bits());
    }

    // --- native histograms: admitted since RSEG v5 (phase C8) ---

    #[test]
    fn native_histogram_is_admitted_and_counted_as_written() {
        let out = normalize_histograms(vec![histogram(1_000), histogram(2_000)]);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        // Histograms never land in the scalar `points` vector.
        assert!(out.points.is_empty());
        assert_eq!(out.histogram_points.len(), 2);
        assert_eq!(
            out.histograms_written, 2,
            "the RW2 histograms-written stats header reports this"
        );
        assert_eq!(out.histograms_dropped, 0);
    }

    /// Every field of the wire shape has to reach storage: the integer
    /// bucket deltas accumulated into absolute counts, `sum` present, the
    /// reset hint mapped, and the timestamp converted from milliseconds.
    #[test]
    fn admitted_native_histogram_maps_every_field_to_the_storage_model() {
        let out = normalize_histograms(vec![histogram(1_000)]);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        let sample = expect_histogram(&out);
        assert_eq!(sample.ts_ns, 1_000_000_000);
        assert_eq!(sample.value.scale, 2);
        assert_eq!(sample.value.zero_threshold.to_bits(), 1e-9f64.to_bits());
        assert_eq!(sample.value.sum.map(f64::to_bits), Some(42.5f64.to_bits()));
        assert_eq!(sample.value.custom_values, None);
        assert_eq!(
            sample.value.positive_spans,
            vec![HistogramSpan {
                offset: 0,
                length: 3
            }]
        );
        assert!(sample.value.negative_spans.is_empty());
        assert_eq!(
            sample.value.counts,
            HistogramCounts::Int {
                zero_count: 1,
                count: 14,
                // Wire deltas 2, 3, 1 accumulate to these absolute counts.
                positive: vec![2, 5, 6],
                negative: vec![],
            }
        );
        assert_eq!(sample.value.reset_hint, SegResetHint::Yes);
    }

    /// Negative deltas are how Prometheus encodes a bucket count that drops
    /// relative to its predecessor; only a running total that leaves `u64`
    /// is malformed.
    #[test]
    fn negative_bucket_deltas_accumulate_rather_than_reject() {
        let mut h = histogram(1_000);
        h.positive_deltas = vec![5, -3, 2];
        h.count = Some(ResolvedCount::Int(20));
        let out = normalize_histograms(vec![h]);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(
            expect_histogram(&out).value.counts,
            HistogramCounts::Int {
                zero_count: 1,
                count: 20,
                positive: vec![5, 2, 4],
                negative: vec![],
            }
        );
    }

    #[test]
    fn bucket_delta_sequence_going_below_zero_is_rejected() {
        let mut h = histogram(1_000);
        h.positive_deltas = vec![1, -5, 1];
        assert_histogram_rejected(h, RwRejection::NativeHistogramDeltaOutOfRange);
    }

    /// A float histogram is signalled by the `count` oneof and carries
    /// absolute bucket counts, not deltas.
    #[test]
    fn float_native_histogram_is_admitted_with_absolute_counts() {
        let mut h = histogram(1_000);
        h.count = Some(ResolvedCount::Float(14.5));
        h.zero_count = Some(ResolvedCount::Float(1.5));
        h.positive_deltas = vec![];
        h.positive_counts = vec![2.0, 5.0, 6.0];
        let out = normalize_histograms(vec![h]);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(
            expect_histogram(&out).value.counts,
            HistogramCounts::Float {
                zero_count: 1.5,
                count: 14.5,
                positive: vec![2.0, 5.0, 6.0],
                negative: vec![],
            }
        );
    }

    /// A count/zero_count kind disagreement has no storage representation:
    /// the storage model is all-integer or all-float per sample.
    #[test]
    fn count_and_zero_count_kind_disagreement_is_rejected() {
        let mut h = histogram(1_000);
        h.count = Some(ResolvedCount::Float(14.0));
        h.zero_count = Some(ResolvedCount::Int(1));
        h.positive_deltas = vec![];
        h.positive_counts = vec![2.0, 5.0, 6.0];
        assert_histogram_rejected(h, RwRejection::NativeHistogramCountKindMixed);
    }

    /// A float-kind histogram whose delta arrays are populated mixes the two
    /// bucket encodings, which no reader interprets.
    #[test]
    fn float_histogram_with_populated_delta_arrays_is_rejected() {
        let mut h = histogram(1_000);
        h.count = Some(ResolvedCount::Float(14.0));
        h.zero_count = Some(ResolvedCount::Float(1.0));
        h.positive_counts = vec![2.0, 5.0, 6.0];
        assert_histogram_rejected(
            h,
            RwRejection::NativeHistogramBucketEncodingMixed { kind: "float" },
        );
    }

    /// An unset `count` oneof reads as integer zero, which is what a
    /// Prometheus `GetCountInt()` reader sees. An empty histogram (no spans,
    /// no buckets, zero counts) is well-formed under that reading.
    #[test]
    fn empty_native_histogram_with_unset_count_oneof_is_admitted_as_integer_zero() {
        let mut h = histogram(1_000);
        h.count = None;
        h.zero_count = None;
        h.positive_spans = vec![];
        h.positive_deltas = vec![];
        let out = normalize_histograms(vec![h]);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(
            expect_histogram(&out).value.counts,
            HistogramCounts::Int {
                zero_count: 0,
                count: 0,
                positive: vec![],
                negative: vec![],
            }
        );
    }

    #[test]
    fn zero_length_bucket_span_is_rejected() {
        let mut h = histogram(1_000);
        h.positive_spans = vec![ResolvedSpan {
            offset: 0,
            length: 0,
        }];
        h.positive_deltas = vec![];
        assert_histogram_rejected(h, RwRejection::NativeHistogramSpanLengthZero);
    }

    #[test]
    fn bucket_counts_not_matching_span_coverage_are_rejected() {
        let mut h = histogram(1_000);
        h.positive_deltas = vec![2, 3];
        assert_histogram_rejected(
            h,
            RwRejection::NativeHistogramBucketCountLenMismatch {
                side: "positive",
                buckets: 2,
                expected: 3,
            },
        );
    }

    #[test]
    fn schema_below_the_custom_buckets_sentinel_is_rejected() {
        let mut h = histogram(1_000);
        h.schema = -54;
        assert_histogram_rejected(
            h,
            RwRejection::NativeHistogramSchemaUnsupported { schema: -54 },
        );
    }

    /// Custom-bucket histograms are admitted on this surface (the phase C8
    /// decision recorded in `build_histogram_value`): the RW wire format
    /// specifies `schema == -53` with a `custom_values` boundary list, and
    /// storage represents it losslessly.
    #[test]
    fn custom_bucket_histogram_is_admitted_with_its_boundaries() {
        let mut h = histogram(1_000);
        h.schema = -53;
        h.custom_values = vec![0.5, 1.0, 2.5];
        let out = normalize_histograms(vec![h]);
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        let sample = expect_histogram(&out);
        assert_eq!(sample.value.scale, -53);
        assert_eq!(sample.value.custom_values, Some(vec![0.5, 1.0, 2.5]));
        assert_eq!(out.histograms_written, 1);
    }

    #[test]
    fn custom_values_out_of_order_is_rejected() {
        let mut h = histogram(1_000);
        h.schema = -53;
        h.custom_values = vec![1.0, 0.5];
        assert_histogram_rejected(h, RwRejection::NativeHistogramCustomValuesMismatch);
    }

    #[test]
    fn custom_values_without_the_sentinel_schema_is_rejected() {
        let mut h = histogram(1_000);
        h.custom_values = vec![0.5, 1.0];
        assert_histogram_rejected(h, RwRejection::NativeHistogramCustomValuesMismatch);
    }

    #[test]
    fn sentinel_schema_without_custom_values_is_rejected() {
        let mut h = histogram(1_000);
        h.schema = -53;
        assert_histogram_rejected(h, RwRejection::NativeHistogramCustomValuesMismatch);
    }

    /// The RSEG v5 reader rejects a record whose `count` is below its
    /// `zero_count` or below its bucket total as corrupted. Admitting one
    /// would write bytes that every later query reports unreadable, so it
    /// rejects here instead.
    #[test]
    fn count_below_its_bucket_total_is_rejected() {
        let mut h = histogram(1_000);
        h.count = Some(ResolvedCount::Int(5));
        assert_histogram_rejected(h, RwRejection::NativeHistogramCountInconsistent);
    }

    #[test]
    fn count_below_zero_count_is_rejected() {
        let mut h = histogram(1_000);
        h.zero_count = Some(ResolvedCount::Int(100));
        assert_histogram_rejected(h, RwRejection::NativeHistogramCountInconsistent);
    }

    #[test]
    fn histogram_timestamp_shares_the_scalar_skew_bounds() {
        let limits = IngestLimits::default();
        let mut s = series(vec![label("__name__", "h")], vec![]);
        // Far enough in the future to exceed max_future_skew_ns at an
        // ingest clock of 0.
        s.histograms = vec![histogram(i64::from(i32::MAX))];
        let out = normalize_resolved(&tenant(), request(vec![s]), &limits, 0);
        assert!(out.histogram_points.is_empty());
        assert_eq!(out.histograms_written, 0);
        assert_eq!(out.histograms_dropped, 1);
        assert!(
            matches!(
                out.rejected.as_slice(),
                [RwRejection::Otlp {
                    reason: Rejection::FutureSkew { .. },
                    ..
                }]
            ),
            "{:?}",
            out.rejected
        );
    }

    /// One malformed histogram must not take the series' other points with
    /// it: admission is per point, like scalar samples.
    #[test]
    fn a_rejected_histogram_leaves_the_rest_of_its_series_admitted() {
        let mut bad = histogram(2_000);
        bad.positive_deltas = vec![1, -5, 1];
        let mut s = series(vec![label("__name__", "mixed")], vec![sample(1_000, 1.0)]);
        s.histograms = vec![histogram(1_000), bad];
        let out = normalize_resolved(
            &tenant(),
            request(vec![s]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert_eq!(out.points.len(), 1, "the scalar sample");
        assert_eq!(out.histogram_points.len(), 1, "the well-formed histogram");
        assert_eq!(out.histograms_written, 1);
        assert_eq!(out.histograms_dropped, 1);
        assert_eq!(
            out.rejected,
            vec![RwRejection::NativeHistogramDeltaOutOfRange]
        );
    }

    /// A histogram and a scalar sample carrying the same label set are the
    /// same series identity on the wire; both points therefore resolve to
    /// one `SeriesId`. Storage resolves the resulting value-kind conflict
    /// downstream (crates/ravel-ingest), not here.
    #[test]
    fn histogram_and_scalar_points_in_one_series_share_its_series_id() {
        let mut s = series(vec![label("__name__", "mixed")], vec![sample(1_000, 1.0)]);
        s.histograms = vec![histogram(2_000)];
        let out = normalize_resolved(
            &tenant(),
            request(vec![s]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert_eq!(out.points.len(), 1);
        assert_eq!(out.histogram_points.len(), 1);
        assert_eq!(out.points[0].series_id, out.histogram_points[0].series_id);
    }

    /// Native histograms count toward the per-request data-point limit: one
    /// histogram sample is one stored point, the same as one scalar sample.
    #[test]
    fn histograms_count_toward_the_data_point_limit() {
        let limits = IngestLimits {
            max_data_points_per_request: 2,
            ..IngestLimits::default()
        };
        let mut s = series(vec![label("__name__", "h")], vec![sample(1_000, 1.0)]);
        s.histograms = vec![histogram(1_000), histogram(2_000)];
        let out = normalize_resolved(&tenant(), request(vec![s]), &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert!(out.histogram_points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::TooManyDataPoints { count: 3, max: 2 },
                count: 3,
            }]
        );
    }

    #[test]
    fn invalid_labels_reject_histogram_entries_too() {
        // No __name__: the whole series (its scalar samples AND its
        // histogram entries) is unidentifiable, so both reject together
        // rather than the histogram counter firing independently.
        let mut s = series(vec![label("job", "svc")], vec![sample(1_000, 1.0)]);
        s.histograms = vec![histogram(1_000), histogram(2_000), histogram(3_000)];
        let out = normalize_resolved(
            &tenant(),
            request(vec![s]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert!(out.points.is_empty());
        assert!(out.histogram_points.is_empty());
        assert_eq!(out.histograms_dropped, 0);
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::EmptyMetricName { count: 4 },
                count: 4,
            }]
        );
    }

    // --- exemplars and metadata: accepted-and-dropped, counted ---

    #[test]
    fn exemplars_counted_regardless_of_series_admission() {
        let mut admitted = series(vec![label("__name__", "up")], vec![sample(1_000, 1.0)]);
        admitted.exemplar_count = 2;
        let mut rejected_series = series(vec![label("job", "svc")], vec![sample(1_000, 1.0)]);
        rejected_series.exemplar_count = 1;

        let out = normalize_resolved(
            &tenant(),
            request(vec![admitted, rejected_series]),
            &IngestLimits::default(),
            1_000_000,
        );
        assert_eq!(out.exemplars_dropped, 3);
    }

    #[test]
    fn metadata_dropped_is_request_level_tally() {
        let mut req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0)],
        )]);
        req.metadata_count = 5;
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert_eq!(out.metadata_dropped, 5);
    }

    #[test]
    fn created_timestamps_dropped_is_always_zero_for_rw1() {
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert_eq!(out.created_timestamps_dropped, 0);
    }

    #[test]
    fn created_timestamps_dropped_mirrors_resolved_tally() {
        let mut req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0)],
        )]);
        req.created_timestamps_count = 3;
        let out = normalize_resolved(&tenant(), req, &IngestLimits::default(), 1_000_000);
        assert_eq!(out.created_timestamps_dropped, 3);
    }

    // --- request-level cap ---

    #[test]
    fn too_many_data_points_rejects_whole_request() {
        let limits = IngestLimits {
            max_data_points_per_request: 2,
            ..IngestLimits::default()
        };
        let req = request(vec![series(
            vec![label("__name__", "up")],
            vec![sample(1_000, 1.0), sample(2_000, 2.0), sample(3_000, 3.0)],
        )]);
        let out = normalize_resolved(&tenant(), req, &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::TooManyDataPoints { count: 3, max: 2 },
                count: 3,
            }]
        );
    }

    #[test]
    fn histogram_only_request_counts_toward_the_request_level_cap() {
        // No scalar samples at all: the request-level total must still
        // count native histogram entries (matching the per-series formula
        // in `normalize_series`), or a histogram-heavy request slips past
        // the cap while `samples.len()` alone stays at 0.
        let limits = IngestLimits {
            max_data_points_per_request: 2,
            ..IngestLimits::default()
        };
        let mut s = series(vec![label("__name__", "h")], vec![]);
        s.histograms = vec![histogram(1_000), histogram(2_000), histogram(3_000)];
        let out = normalize_resolved(&tenant(), request(vec![s]), &limits, 1_000_000);
        assert!(out.points.is_empty());
        assert_eq!(
            out.rejected,
            vec![RwRejection::Otlp {
                reason: Rejection::TooManyDataPoints { count: 3, max: 2 },
                count: 3,
            }]
        );
    }

    // --- CH-1 cross-protocol identity vector (plan section 4.1) ---

    #[test]
    fn ch1_classic_histogram_series_match_expected_series_ids_and_values() {
        let tenant = TenantId::new("t-fixture");
        let ts_ms = 1_700_000_000_000;
        let base_labels = |extra: &[(&str, &str)]| -> Vec<Label> {
            let mut labels = vec![
                label("__name__", ""), // overwritten per-series below
                label("job", "svc"),
                label("instance", "i-1"),
            ];
            labels.remove(0);
            for (n, v) in extra {
                labels.push(label(n, v));
            }
            labels
        };

        let mut series_list = Vec::new();
        for (le, value) in [("0.1", 1.0), ("1", 3.0), ("10", 6.0), ("+Inf", 10.0)] {
            let mut labels = base_labels(&[("le", le)]);
            labels.push(label("__name__", "http_request_duration_seconds_bucket"));
            series_list.push(series(labels, vec![sample(ts_ms, value)]));
        }
        for (suffix, value) in [("_sum", 42.5), ("_count", 10.0)] {
            let mut labels = base_labels(&[]);
            labels.push(label(
                "__name__",
                &format!("http_request_duration_seconds{suffix}"),
            ));
            series_list.push(series(labels, vec![sample(ts_ms, value)]));
        }

        let out = normalize_resolved(
            &tenant,
            request(series_list),
            &IngestLimits::default(),
            ts_ms * 1_000_000 + 1,
        );
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.points.len(), 6);

        let expect_id = |metric_name: &str, extra: &[(&str, &str)]| {
            let mut labels = vec![label("job", "svc"), label("instance", "i-1")];
            for (n, v) in extra {
                labels.push(label(n, v));
            }
            SeriesId::compute(&tenant, metric_name, &LabelSet::new(labels).expect("valid"))
                .expect("id")
        };

        let bucket_ids: Vec<_> = [("0.1", 1.0), ("1", 3.0), ("10", 6.0), ("+Inf", 10.0)]
            .iter()
            .map(|(le, _)| expect_id("http_request_duration_seconds_bucket", &[("le", le)]))
            .collect();
        let sum_id = expect_id("http_request_duration_seconds_sum", &[]);
        let count_id = expect_id("http_request_duration_seconds_count", &[]);

        for (i, (_, expected_value)) in [("0.1", 1.0), ("1", 3.0), ("10", 6.0), ("+Inf", 10.0)]
            .iter()
            .enumerate()
        {
            assert_eq!(out.points[i].series_id, bucket_ids[i]);
            assert_eq!(out.points[i].sample.value, *expected_value);
        }
        assert_eq!(out.points[4].series_id, sum_id);
        assert_eq!(out.points[4].sample.value, 42.5);
        assert_eq!(out.points[5].series_id, count_id);
        assert_eq!(out.points[5].sample.value, 10.0);
    }
}
