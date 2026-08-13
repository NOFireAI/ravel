//! Admission limits and typed rejection reasons for OTLP trace normalization
//! (ADR-0041, docs/span-segment-format.md).
//!
//! Ordering and cost. These limits are enforced inside
//! [`crate::traces_normalize::normalize_traces`], which runs only after the
//! transport layer has already decoded the whole `ExportTraceServiceRequest`
//! into memory, exactly like [`crate::logs_limits`]' log equivalent: the HTTP
//! path decodes the full body in `services/ravel-server` before calling in,
//! and the gRPC path does the same through tonic. `max_spans_per_request` and
//! the rest therefore bound per-span allocation and what reaches the shard
//! buffer; they do not bound decode-time allocation. The only bound on the
//! work a hostile or misconfigured sender can force before any check here runs
//! is the transport body/message limit, which lives in the services crate.
//!
//! Rejections are typed rather than bare errors: the OTLP partial-success
//! response reports a rejected-span count, and [`SpanRejection::rejected_count`]
//! gives the multiplier for a rejection that covers more than one span. A
//! rejection that drops one attribute of an otherwise-stored span returns 0
//! from [`SpanRejection::rejected_count`], because no span was lost (#364); it
//! is still reported to the sender through the partial-success `error_message`,
//! just not counted toward `rejected_spans`. A rejection that applies
//! identically to every span under one `ResourceSpans` or `ScopeSpans` uses
//! [`SpanRejection::Grouped`] to carry the shared reason plus the span count it
//! covers, instead of materializing one clone per span. This mirrors
//! [`crate::logs_limits::LogRejection::Grouped`].

/// Admission limits checked at OTLP trace ingest, before allocating per-span
/// attribute structures.
///
/// The ceilings deliberately track [`crate::LogIngestLimits`]' where the two
/// signals carry the same kind of data, and diverge only where a span's field
/// is a different shape from a log's:
///
/// - `max_spans_per_request`, `max_attributes_per_span`,
///   `max_attribute_key_len`, `max_attribute_value_len`,
///   `max_resource_attributes`, and `max_scope_attributes` are the log values
///   verbatim. A span attribute carries the same kind of payload a log
///   attribute does (`db.statement`, `http.url`, `exception.message`), so it
///   gets the same 8 KiB ceiling rather than a metric label's much smaller one.
/// - `max_name_len` is far *below* the log body's ceiling: a span name is a
///   low-cardinality operation name (`GET /checkout`), not a payload, and
///   OTel's own guidance is that it must not carry high-cardinality data.
/// - `max_status_message_len` sits between the two: an error description is
///   payload-shaped but is not a full log body.
/// - `max_raw_blob_len` bounds the serialized events/links blobs
///   ([`crate::traces_normalize`]), the only span field that can hold an
///   arbitrary nested payload, so it gets the log body's ceiling.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanIngestLimits {
    /// Total spans across a single `ExportTraceServiceRequest`, counted from
    /// span vector lengths only (no per-span allocation happens before this
    /// check).
    pub max_spans_per_request: usize,
    /// Attributes on a single span, counted before the resource and scope
    /// attributes are merged in.
    pub max_attributes_per_span: usize,
    /// Bytes in an attribute key.
    pub max_attribute_key_len: usize,
    /// Bytes in an attribute value after conversion to its stored string form.
    pub max_attribute_value_len: usize,
    /// Bytes in a span name.
    pub max_name_len: usize,
    /// Bytes in a span status message.
    pub max_status_message_len: usize,
    /// Attributes on a Resource. Unlike logs, resource attributes are not part
    /// of any identity here (ADR-0041 routes by `trace_id`); this bounds how
    /// much gets merged into every span under the resource.
    pub max_resource_attributes: usize,
    /// Attributes on an instrumentation scope, bounding the same merge.
    pub max_scope_attributes: usize,
    /// Bytes in the serialized events or links blob, measured before hex
    /// encoding. Applied to each blob independently.
    pub max_raw_blob_len: usize,
    /// Nanoseconds a span's resolved `end_ts_ns` may lead ingest time
    /// (ADR-0051 §4). Default 10 minutes, the same value the metrics and
    /// log paths use: the catalog listing window is one shared
    /// `max_ingest_lag_ns`, not per-signal, so the admission bounds that
    /// make it sound are shared too. The end timestamp is the bounded one
    /// because the commit record advertises `max end_ts`.
    pub max_future_skew_ns: i64,
    /// Nanoseconds a span's resolved `end_ts_ns` may lag ingest time
    /// (ADR-0051 §4 as superseded by the 2026-08-13 amendment). Default 2
    /// hours, shared with metrics and logs; the end timestamp is the bounded
    /// one (the amendment moved the anchor from start to end) because the
    /// listing window stays sound as long as the end is in window.
    /// Consequence: a span reported more than this after it *ended* is
    /// rejected as late data, but a long-running span that started earlier and
    /// ended in window is admitted. Raising it for a tenant is legal only
    /// together with the catalog-side window config.
    pub max_ingest_lag_ns: i64,
}

const SECOND_NANOS: i64 = 1_000_000_000;
const MINUTE_NANOS: i64 = 60 * SECOND_NANOS;
const HOUR_NANOS: i64 = 60 * MINUTE_NANOS;

impl Default for SpanIngestLimits {
    fn default() -> Self {
        SpanIngestLimits {
            max_spans_per_request: 100_000,
            max_attributes_per_span: 128,
            max_attribute_key_len: 256,
            max_attribute_value_len: 8192,
            max_name_len: 1024,
            max_status_message_len: 4096,
            max_resource_attributes: 128,
            max_scope_attributes: 64,
            max_raw_blob_len: 65_536,
            max_future_skew_ns: 10 * MINUTE_NANOS,
            max_ingest_lag_ns: 2 * HOUR_NANOS,
        }
    }
}

/// Why a single OTLP span (or a group of them, or one attribute of one span)
/// was not admitted. Every variant is meant to be reported back to the sender
/// via the OTLP partial-success mechanism, never just logged and dropped.
///
/// In the three `TooMany*Attributes` variants, `count` is an *attribute*
/// count: it says how far over the limit the offending attribute set was. The
/// number of spans a rejection accounts for is never read from those fields;
/// it comes from [`SpanRejection::rejected_count`], which reads
/// [`SpanRejection::Grouped`]'s own count for the scope-wide cases.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpanRejection {
    #[error("request has {count} spans, more than the per-request limit of {max}")]
    TooManySpans { count: usize, max: usize },

    #[error("span has {count} attributes, more than the per-span limit of {max}")]
    TooManyAttributes { count: usize, max: usize },

    #[error("attribute key is {len} bytes, more than the limit of {max}")]
    AttributeKeyTooLong { len: usize, max: usize },

    #[error("attribute value is {len} bytes, more than the limit of {max}")]
    AttributeValueTooLong { len: usize, max: usize },

    #[error("span name is {len} bytes, more than the limit of {max}")]
    NameTooLong { len: usize, max: usize },

    #[error("status message is {len} bytes, more than the limit of {max}")]
    StatusMessageTooLong { len: usize, max: usize },

    #[error("resource has {count} attributes, more than the limit of {max}")]
    TooManyResourceAttributes { count: usize, max: usize },

    #[error("scope has {count} attributes, more than the limit of {max}")]
    TooManyScopeAttributes { count: usize, max: usize },

    /// A span's `trace_id` was not exactly 16 bytes. Rejects the whole span:
    /// `trace_id` is RSPAN's sort and lookup key (ADR-0041), and padding or
    /// truncating one would file the span under a trace that never existed.
    #[error("trace_id is {len} bytes, not the required 16")]
    InvalidTraceId { len: usize },

    /// A span's `span_id` was not exactly 8 bytes. Rejects the whole span for
    /// the same reason as [`SpanRejection::InvalidTraceId`].
    #[error("span_id is {len} bytes, not the required 8")]
    InvalidSpanId { len: usize },

    /// A span's `parent_span_id` was present but not 8 bytes. Unlike the two
    /// ids above this does not reject the span, whose own identity is intact;
    /// the parent edge is stored as absent and the problem reported, rather
    /// than a fabricated 8-byte parent being invented.
    #[error("parent_span_id is {len} bytes, not the required 8; stored as no parent")]
    InvalidParentSpanId { len: usize },

    /// `end_time_unix_nano` preceded `start_time_unix_nano`. Rejects the span:
    /// RSPAN's skip index prunes by interval overlap over `[start, end]`, and
    /// an inverted interval cannot be pruned soundly.
    #[error("span ends at {end_ts_ns} before it starts at {start_ts_ns}")]
    InvalidTimeRange { start_ts_ns: i64, end_ts_ns: i64 },

    /// The span's resolved `end_ts_ns` leads ingest time by more than the
    /// admission bound (ADR-0051 §4). Rejected rather than clamped:
    /// rewriting a sender's timestamps would be silent data corruption, and
    /// a span past this bound would be stored but invisible to every
    /// listing-window query and its hour bucket unexpirable. The end
    /// timestamp is the checked one because the commit record advertises
    /// `max end_ts`. Mirrors [`crate::limits::Rejection::FutureSkew`].
    #[error(
        "span end timestamp is {skew_ns} ns ahead of ingest time, more than the max future skew of {max_ns} ns"
    )]
    FutureSkew { skew_ns: i64, max_ns: i64 },

    /// The span's resolved `end_ts_ns` lags ingest time by more than the
    /// admission bound (ADR-0051 §4 as superseded by the 2026-08-13
    /// amendment). The end timestamp is the checked one because the listing
    /// window is sound as long as the end is in window; a span reported more
    /// than the lag after it *ended* is late data and lands here, while a
    /// long-running span that started earlier but ended in window is admitted.
    /// Mirrors [`crate::limits::Rejection::TooOld`].
    #[error(
        "span end timestamp is {lag_ns} ns behind ingest time, more than the max ingest lag of {max_ns} ns"
    )]
    TooOld { lag_ns: i64, max_ns: i64 },

    /// The serialized span-events blob exceeded `max_raw_blob_len`. The span
    /// itself is admitted without the blob; the loss is reported rather than
    /// silent (ADR-0041 keeps events visible-but-opaque in v1).
    #[error("serialized span events are {len} bytes, more than the limit of {max}")]
    EventsBlobTooLong { len: usize, max: usize },

    /// The serialized span-links blob exceeded `max_raw_blob_len`. Handled
    /// exactly like [`SpanRejection::EventsBlobTooLong`].
    #[error("serialized span links are {len} bytes, more than the limit of {max}")]
    LinksBlobTooLong { len: usize, max: usize },

    /// An attribute arrived with its `value` field unset. Dropped as a single
    /// attribute, not as the whole span, and reported rather than silently
    /// discarded.
    #[error("attribute {key} has no value set")]
    MissingAttributeValue { key: String },

    /// An attribute value is a string-table reference (`strindex`), which
    /// carries no value of its own. Dropped as a single attribute, like
    /// [`SpanRejection::MissingAttributeValue`].
    #[error("attribute {key} is a string-table reference (strindex) with no value of its own")]
    UnsupportedAttributeValue { key: String },

    /// An attribute value is an array or kvlist. RSPAN v1 stores attributes as
    /// `Map<Utf8, Utf8>` (ADR-0041), which has no nested representation, and
    /// inventing a stringification would be a frozen-format decision made by
    /// accident. Dropped as a single attribute and reported, the same choice
    /// [`crate::logs_limits::LogRejection::UnsupportedBodyKind`] makes for a
    /// structured log body.
    #[error("attribute {key} is a nested array or map, which RSPAN v1 cannot store")]
    UnsupportedAttributeKind { key: String },

    /// `reason` applied identically to `count` spans that share one resource
    /// or scope (a resource or scope whose attribute set exceeded its limit).
    /// Represents the same information as `count` clones of `reason` without
    /// materializing them, mirroring
    /// [`crate::logs_limits::LogRejection::Grouped`].
    #[error("{reason} (rejecting {count} spans under it)")]
    Grouped {
        reason: Box<SpanRejection>,
        count: usize,
    },
}

impl SpanRejection {
    /// Number of underlying OTLP spans this rejection accounts for. Summing
    /// this over [`crate::traces_normalize::SpanNormalizeOutput::rejected`]
    /// gives the count to report in an OTLP `rejected_spans` field. Mirrors
    /// [`crate::logs_limits::LogRejection::rejected_count`].
    ///
    /// Attribute-level rejections return 0: they drop one attribute (or the
    /// events/links blob, or a malformed `parent_span_id` edge) of a span that
    /// is still stored, so counting them as a rejected *span* over-reports how
    /// many spans a sender's export lost (#364). They remain visible to the
    /// sender through the partial-success `error_message`; only their
    /// contribution to `rejected_spans` is zero. This mirrors the metrics
    /// path's [`crate::limits::Rejection::rejected_count`], where an admitted
    /// data point that dropped an informational field also returns 0.
    pub fn rejected_count(&self) -> usize {
        match self {
            SpanRejection::TooManySpans { count, .. } | SpanRejection::Grouped { count, .. } => {
                *count
            }
            // The span still lands; only one attribute (or blob, or the parent
            // edge) was dropped. These must never inflate `rejected_spans`.
            SpanRejection::AttributeKeyTooLong { .. }
            | SpanRejection::AttributeValueTooLong { .. }
            | SpanRejection::InvalidParentSpanId { .. }
            | SpanRejection::EventsBlobTooLong { .. }
            | SpanRejection::LinksBlobTooLong { .. }
            | SpanRejection::MissingAttributeValue { .. }
            | SpanRejection::UnsupportedAttributeValue { .. }
            | SpanRejection::UnsupportedAttributeKind { .. } => 0,
            // Whole-span rejections: the span never reached storage.
            _ => 1,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_sizing_table() {
        let limits = SpanIngestLimits::default();
        assert_eq!(limits.max_spans_per_request, 100_000);
        assert_eq!(limits.max_attributes_per_span, 128);
        assert_eq!(limits.max_attribute_key_len, 256);
        assert_eq!(limits.max_attribute_value_len, 8192);
        assert_eq!(limits.max_name_len, 1024);
        assert_eq!(limits.max_status_message_len, 4096);
        assert_eq!(limits.max_resource_attributes, 128);
        assert_eq!(limits.max_scope_attributes, 64);
        assert_eq!(limits.max_raw_blob_len, 65_536);
        assert_eq!(limits.max_future_skew_ns, 600_000_000_000);
        assert_eq!(limits.max_ingest_lag_ns, 7_200_000_000_000);
    }

    /// The skew bounds are the metrics ones verbatim (ADR-0051 §4): the
    /// catalog listing window is one shared value, so the admission bounds
    /// that make it sound cannot differ per signal. Pinned so a per-signal
    /// "tuning" edit has to argue with a failing test.
    #[test]
    fn skew_bounds_equal_the_metric_ones() {
        let spans = SpanIngestLimits::default();
        let metrics = crate::IngestLimits::default();
        assert_eq!(spans.max_future_skew_ns, metrics.max_future_skew_ns);
        assert_eq!(spans.max_ingest_lag_ns, metrics.max_ingest_lag_ns);
    }

    /// The shared ceilings are the log ones verbatim, and the span-specific
    /// ones sit where the type's doc comment says they do. Pinned so a future
    /// "consistency" edit has to argue with a failing test rather than a
    /// comment.
    #[test]
    fn span_ceilings_track_the_log_ones_where_the_data_is_the_same_shape() {
        let spans = SpanIngestLimits::default();
        let logs = crate::LogIngestLimits::default();
        assert_eq!(spans.max_spans_per_request, logs.max_records_per_request);
        assert_eq!(
            spans.max_attributes_per_span,
            logs.max_attributes_per_record
        );
        assert_eq!(spans.max_attribute_key_len, logs.max_attribute_key_len);
        assert_eq!(spans.max_attribute_value_len, logs.max_attribute_value_len);
        assert_eq!(spans.max_resource_attributes, logs.max_resource_attributes);
        assert_eq!(spans.max_scope_attributes, logs.max_scope_attributes);
        // A span name is an operation name, not a payload.
        assert!(spans.max_name_len < logs.max_body_len);
        assert!(spans.max_status_message_len < logs.max_body_len);
        // The events/links blob is the one arbitrary-payload span field.
        assert_eq!(spans.max_raw_blob_len, logs.max_body_len);
    }

    #[test]
    fn rejected_count_uses_batch_count_for_too_many_spans() {
        let r = SpanRejection::TooManySpans {
            count: 250_000,
            max: 100_000,
        };
        assert_eq!(r.rejected_count(), 250_000);
    }

    /// Whole-span rejections each account for exactly one lost span. The span
    /// never reached storage, so it counts toward `rejected_spans`.
    #[test]
    fn rejected_count_defaults_to_one_for_whole_span_reasons() {
        for r in [
            SpanRejection::TooManyAttributes {
                count: 200,
                max: 128,
            },
            SpanRejection::NameTooLong {
                len: 2000,
                max: 1024,
            },
            SpanRejection::StatusMessageTooLong {
                len: 5000,
                max: 4096,
            },
            SpanRejection::TooManyResourceAttributes {
                count: 200,
                max: 128,
            },
            SpanRejection::TooManyScopeAttributes {
                count: 100,
                max: 64,
            },
            SpanRejection::InvalidTraceId { len: 3 },
            SpanRejection::InvalidSpanId { len: 7 },
            SpanRejection::InvalidTimeRange {
                start_ts_ns: 20,
                end_ts_ns: 10,
            },
            SpanRejection::FutureSkew {
                skew_ns: 700_000_000_000,
                max_ns: 600_000_000_000,
            },
            SpanRejection::TooOld {
                lag_ns: 8_000_000_000_000,
                max_ns: 7_200_000_000_000,
            },
        ] {
            assert_eq!(r.rejected_count(), 1, "{r}");
        }
    }

    /// Attribute-level rejections drop one attribute (or blob, or the parent
    /// edge) of a span that is still stored, so they contribute zero to
    /// `rejected_spans` (#364). Pinned so a future edit that reintroduces the
    /// old catch-all `_ => 1` has to argue with a failing test.
    #[test]
    fn rejected_count_is_zero_for_attribute_level_reasons() {
        for r in [
            SpanRejection::AttributeKeyTooLong { len: 300, max: 256 },
            SpanRejection::AttributeValueTooLong {
                len: 9000,
                max: 8192,
            },
            SpanRejection::InvalidParentSpanId { len: 5 },
            SpanRejection::EventsBlobTooLong {
                len: 70_000,
                max: 65_536,
            },
            SpanRejection::LinksBlobTooLong {
                len: 70_000,
                max: 65_536,
            },
            SpanRejection::MissingAttributeValue { key: "k".into() },
            SpanRejection::UnsupportedAttributeValue { key: "k".into() },
            SpanRejection::UnsupportedAttributeKind { key: "k".into() },
        ] {
            assert_eq!(r.rejected_count(), 0, "{r}");
        }
    }

    #[test]
    fn grouped_rejected_count_is_the_carried_count_not_one() {
        let r = SpanRejection::Grouped {
            reason: Box::new(SpanRejection::TooManyResourceAttributes {
                count: 200,
                max: 128,
            }),
            count: 5_000,
        };
        assert_eq!(r.rejected_count(), 5_000);
        let msg = r.to_string();
        assert!(msg.contains("5000"), "{msg}");
        assert!(msg.contains("resource has 200 attributes"), "{msg}");
    }
}
