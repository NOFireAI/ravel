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
//! rejection that applies identically to every span under one `ResourceSpans`
//! or `ScopeSpans` uses [`SpanRejection::Grouped`] to carry the shared reason
//! plus the span count it covers, instead of materializing one clone per span.
//! This mirrors [`crate::logs_limits::LogRejection::Grouped`].

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
}

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
    pub fn rejected_count(&self) -> usize {
        match self {
            SpanRejection::TooManySpans { count, .. } | SpanRejection::Grouped { count, .. } => {
                *count
            }
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

    #[test]
    fn rejected_count_defaults_to_one_for_span_scoped_reasons() {
        for r in [
            SpanRejection::TooManyAttributes {
                count: 200,
                max: 128,
            },
            SpanRejection::AttributeKeyTooLong { len: 300, max: 256 },
            SpanRejection::AttributeValueTooLong {
                len: 9000,
                max: 8192,
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
            SpanRejection::InvalidParentSpanId { len: 5 },
            SpanRejection::InvalidTimeRange {
                start_ts_ns: 20,
                end_ts_ns: 10,
            },
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
            assert_eq!(r.rejected_count(), 1, "{r}");
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
