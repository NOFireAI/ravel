//! Admission limits and typed rejection reasons for OTLP log normalization
//! (ADR-0029).
//!
//! Ordering and cost. These limits are enforced inside
//! [`crate::logs_normalize::normalize_logs`], which runs only after the
//! transport layer has already decoded the whole `ExportLogsServiceRequest`
//! into memory, exactly like [`crate::limits`]' metrics equivalent: the HTTP
//! path decodes the full body in `services/ravel-server` before calling in,
//! and the gRPC path does the same through tonic. `max_records_per_request`
//! and the rest therefore bound per-record allocation and what reaches the
//! shard buffer; they do not bound decode-time allocation. The only bound on
//! the work a hostile or misconfigured sender can force before any check here
//! runs is the transport body/message limit, which lives in the services
//! crate.
//!
//! Rejections are typed rather than bare errors: the OTLP partial-success
//! response reports a rejected-record count, and
//! [`LogRejection::rejected_count`] gives the multiplier for a rejection that
//! covers more than one record. A rejection that applies identically to every
//! record under one `ResourceLogs` or `ScopeLogs` uses
//! [`LogRejection::Grouped`] to carry the shared reason plus the record count
//! it covers, instead of materializing one clone per record. This mirrors
//! [`crate::limits::Rejection::Grouped`] on the metrics path.

/// Admission limits checked at OTLP log ingest, before allocating per-record
/// attribute structures.
///
/// The ceilings here are deliberately wider than [`crate::IngestLimits`]'
/// equivalents: a log body and a log attribute value routinely carry a
/// message, a stack trace, or a structured payload, where a metric label
/// value carries an identifier. The asymmetry is intentional, not an
/// oversight to be "fixed" into agreement.
#[derive(Debug, Clone, PartialEq)]
pub struct LogIngestLimits {
    /// Total log records across a single `ExportLogsServiceRequest`, counted
    /// from record vector lengths only (no per-record allocation happens
    /// before this check).
    pub max_records_per_request: usize,
    /// Attributes on a single log record.
    pub max_attributes_per_record: usize,
    /// Bytes in an attribute key.
    pub max_attribute_key_len: usize,
    /// Payload bytes in an attribute value: its own string or bytes payload,
    /// plus nested entries for a list or map value (see
    /// `logs_normalize::attr_value_len`).
    pub max_attribute_value_len: usize,
    /// Bytes in a record body after normalization to a string.
    pub max_body_len: usize,
    /// Attributes on a Resource. Resource attributes are part of log stream
    /// identity (ADR-0029), so this bounds the identity preimage too.
    pub max_resource_attributes: usize,
    /// Attributes on an instrumentation scope. Also part of stream identity.
    pub max_scope_attributes: usize,
    /// Nanoseconds a record's resolved event time may lead ingest time
    /// (ADR-0051 §4). Default 10 minutes, the same value the metrics path
    /// uses ([`crate::IngestLimits::max_future_skew_ns`]): the catalog
    /// listing window is one shared `max_ingest_lag_ns`, not per-signal, so
    /// the admission bounds that make it sound are shared too.
    pub max_future_skew_ns: i64,
    /// Nanoseconds a record's resolved event time may lag ingest time
    /// (ADR-0051 §4). Default 2 hours, shared with metrics for the same
    /// reason as [`LogIngestLimits::max_future_skew_ns`]. Raising it for a
    /// tenant is legal only together with the catalog-side window config.
    pub max_ingest_lag_ns: i64,
}

const SECOND_NANOS: i64 = 1_000_000_000;
const MINUTE_NANOS: i64 = 60 * SECOND_NANOS;
const HOUR_NANOS: i64 = 60 * MINUTE_NANOS;

impl Default for LogIngestLimits {
    fn default() -> Self {
        LogIngestLimits {
            max_records_per_request: 100_000,
            max_attributes_per_record: 128,
            max_attribute_key_len: 256,
            max_attribute_value_len: 8192,
            max_body_len: 65_536,
            max_resource_attributes: 128,
            max_scope_attributes: 64,
            max_future_skew_ns: 10 * MINUTE_NANOS,
            max_ingest_lag_ns: 2 * HOUR_NANOS,
        }
    }
}

/// Why a single OTLP log record (or a group of them, or one attribute of one
/// record) was not admitted. Every variant is meant to be reported back to
/// the sender via the OTLP partial-success mechanism, never just logged and
/// dropped.
///
/// In the three `TooMany*Attributes` variants, `count` is an *attribute*
/// count: it says how far over the limit the offending attribute set was.
/// The number of log records a rejection accounts for is never read from
/// those fields; it comes from [`LogRejection::rejected_count`], which reads
/// [`LogRejection::Grouped`]'s own count for the scope-wide cases.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LogRejection {
    #[error("request has {count} log records, more than the per-request limit of {max}")]
    TooManyRecords { count: usize, max: usize },

    #[error("record has {count} attributes, more than the per-record limit of {max}")]
    TooManyAttributes { count: usize, max: usize },

    #[error("attribute key is {len} bytes, more than the limit of {max}")]
    AttributeKeyTooLong { len: usize, max: usize },

    #[error("attribute value is {len} bytes, more than the limit of {max}")]
    AttributeValueTooLong { len: usize, max: usize },

    #[error("body is {len} bytes, more than the limit of {max}")]
    BodyTooLong { len: usize, max: usize },

    #[error("resource has {count} attributes, more than the limit of {max}")]
    TooManyResourceAttributes { count: usize, max: usize },

    #[error("scope has {count} attributes, more than the limit of {max}")]
    TooManyScopeAttributes { count: usize, max: usize },

    #[error("record body is not a supported AnyValue kind")]
    UnsupportedBodyKind,

    /// The record's resolved event time (`ts_ns`, after the observed-time and
    /// ingest-time fallbacks) leads ingest time by more than the admission
    /// bound (ADR-0051 §4). Rejected rather than clamped: rewriting a
    /// sender's event time would be silent data corruption, and a record
    /// past this bound would be stored but invisible to every listing-window
    /// query and its hour bucket unexpirable. Mirrors
    /// [`crate::limits::Rejection::FutureSkew`].
    #[error(
        "record timestamp is {skew_ns} ns ahead of ingest time, more than the max future skew of {max_ns} ns"
    )]
    FutureSkew { skew_ns: i64, max_ns: i64 },

    /// The record's resolved event time lags ingest time by more than the
    /// admission bound (ADR-0051 §4). Mirrors
    /// [`crate::limits::Rejection::TooOld`].
    #[error(
        "record timestamp is {lag_ns} ns behind ingest time, more than the max ingest lag of {max_ns} ns"
    )]
    TooOld { lag_ns: i64, max_ns: i64 },

    /// An attribute arrived with its `value` field unset. Dropped as a single
    /// attribute, not as the whole record, and reported rather than silently
    /// discarded.
    #[error("attribute {key} has no value set")]
    MissingAttributeValue { key: String },

    /// An attribute value is a string-table reference (`strindex`), which
    /// carries no value of its own and has no canonical `AttrValue`
    /// representation. Dropped as a single attribute, like
    /// [`LogRejection::MissingAttributeValue`].
    #[error("attribute {key} is a string-table reference (strindex) with no value of its own")]
    UnsupportedAttributeValue { key: String },

    /// An array or kvlist attribute value nests deeper than
    /// [`crate::logs_normalize::MAX_ATTRIBUTE_NESTING_DEPTH`] levels. Rejected
    /// rather than converted so the recursive
    /// [`crate::logs_normalize`] converter cannot be driven past a bounded
    /// depth by a hostile or malformed payload, independent of the decoder's
    /// own recursion limit. Dropped as a single attribute when it sits on a
    /// record, like [`LogRejection::MissingAttributeValue`]; a resource or
    /// scope attribute that trips it rejects that group instead, the same as
    /// any other conversion failure there.
    #[error("attribute {key} nests more than {max} levels deep")]
    AttributeTooDeeplyNested { key: String, max: usize },

    /// `reason` applied identically to `count` log records that share one
    /// resource or scope (a resource or scope whose attribute set exceeded
    /// its limit, so nothing under it can be given a stream identity).
    /// Represents the same information as `count` clones of `reason` without
    /// materializing them, mirroring [`crate::limits::Rejection::Grouped`].
    #[error("{reason} (rejecting {count} log records under it)")]
    Grouped {
        reason: Box<LogRejection>,
        count: usize,
    },
}

impl LogRejection {
    /// The admission `reason` this rejection is counted under (ADR-0051
    /// section 3 layer 3), or `None` when it costs the sender no log record.
    /// Mirrors [`crate::limits::Rejection::admission_class`], and is
    /// exhaustive for the same reason: a new variant does not compile until it
    /// has been classified.
    ///
    /// The five per-attribute variants class as `None`, not `structural`,
    /// when they stand on their own: they drop one attribute of a record that
    /// is still stored (their [`LogRejection::rejected_count`] is 0), so they
    /// cost the sender no record and must not move a rejected counter. This
    /// mirrors the traces path, where a stored span's dropped attributes are
    /// classed `None` for the same reason.
    ///
    /// Those same five variants also appear inside a [`LogRejection::Grouped`]
    /// when a resource- or scope-level attribute set cannot be converted,
    /// where they do cost whole records (the attributes carry stream identity,
    /// so nothing under the resource or scope can be admitted). That context
    /// lives on `Grouped`, which classes `structural` for the group rather
    /// than delegating to the reason: at record scope the same reason costs
    /// nothing, so delegating would let a real whole-resource loss go
    /// uncounted.
    pub fn admission_class(&self) -> Option<crate::limits::AdmissionClass> {
        use crate::limits::AdmissionClass;
        match self {
            LogRejection::FutureSkew { .. } | LogRejection::TooOld { .. } => {
                Some(AdmissionClass::Skew)
            }
            LogRejection::TooManyRecords { .. }
            | LogRejection::TooManyAttributes { .. }
            | LogRejection::BodyTooLong { .. }
            | LogRejection::TooManyResourceAttributes { .. }
            | LogRejection::TooManyScopeAttributes { .. }
            | LogRejection::UnsupportedBodyKind => Some(AdmissionClass::Structural),
            // Per-attribute drops on an otherwise-stored record: the record
            // still lands, so these cost the sender nothing and must not move
            // a rejected counter (their `rejected_count` is 0 for the same
            // reason). When one of these instead rejects a whole resource or
            // scope it is wrapped in `Grouped`, whose arm below classes the
            // group, not the reason.
            LogRejection::AttributeKeyTooLong { .. }
            | LogRejection::AttributeValueTooLong { .. }
            | LogRejection::MissingAttributeValue { .. }
            | LogRejection::UnsupportedAttributeValue { .. }
            | LogRejection::AttributeTooDeeplyNested { .. } => None,
            // A grouped rejection is always a whole-group structural loss: its
            // reason is either a too-many-attributes breach or an attribute
            // that could not be converted at resource or scope scope, and skew
            // is resolved per record after the group check, so it never
            // groups. Classify by the group rather than by the reason, since
            // the attribute-conversion reasons class `None` on their own.
            LogRejection::Grouped { .. } => Some(AdmissionClass::Structural),
        }
    }

    /// Number of underlying OTLP log records this rejection accounts for.
    /// Summing this over [`crate::logs_normalize::LogNormalizeOutput::rejected`]
    /// gives the count to report in an OTLP `rejected_log_records` field.
    /// Mirrors [`crate::limits::Rejection::rejected_count`].
    ///
    /// The five per-attribute variants return 0: they drop one attribute of a
    /// record that is still stored, so counting them as a rejected record
    /// over-reports how many records a sender's export lost. They remain
    /// visible to the sender through the partial-success `error_message`; only
    /// their contribution to `rejected_log_records` is zero. When one of them
    /// rejects a whole resource or scope it is carried in
    /// [`LogRejection::Grouped`], whose own `count` (not the reason's) is read
    /// here, so a real whole-group loss still counts. This mirrors the traces
    /// path's [`crate::traces_limits::SpanRejection::rejected_count`].
    pub fn rejected_count(&self) -> usize {
        match self {
            LogRejection::TooManyRecords { count, .. } | LogRejection::Grouped { count, .. } => {
                *count
            }
            // The record still lands; only one attribute was dropped. These
            // must never inflate `rejected_log_records`.
            LogRejection::AttributeKeyTooLong { .. }
            | LogRejection::AttributeValueTooLong { .. }
            | LogRejection::MissingAttributeValue { .. }
            | LogRejection::UnsupportedAttributeValue { .. }
            | LogRejection::AttributeTooDeeplyNested { .. } => 0,
            // Whole-record rejections: the record never reached storage.
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
        let limits = LogIngestLimits::default();
        assert_eq!(limits.max_records_per_request, 100_000);
        assert_eq!(limits.max_attributes_per_record, 128);
        assert_eq!(limits.max_attribute_key_len, 256);
        assert_eq!(limits.max_attribute_value_len, 8192);
        assert_eq!(limits.max_body_len, 65_536);
        assert_eq!(limits.max_resource_attributes, 128);
        assert_eq!(limits.max_scope_attributes, 64);
        assert_eq!(limits.max_future_skew_ns, 600_000_000_000);
        assert_eq!(limits.max_ingest_lag_ns, 7_200_000_000_000);
    }

    /// The skew bounds are the metrics ones verbatim (ADR-0051 §4): the
    /// catalog listing window is one shared value, so the admission bounds
    /// that make it sound cannot differ per signal. Pinned so a per-signal
    /// "tuning" edit has to argue with a failing test.
    #[test]
    fn skew_bounds_equal_the_metric_ones() {
        let logs = LogIngestLimits::default();
        let metrics = crate::IngestLimits::default();
        assert_eq!(logs.max_future_skew_ns, metrics.max_future_skew_ns);
        assert_eq!(logs.max_ingest_lag_ns, metrics.max_ingest_lag_ns);
    }

    #[test]
    fn log_body_and_value_ceilings_exceed_the_metric_equivalents() {
        // The asymmetry with IngestLimits is deliberate (see the type's doc
        // comment); pin it so a future "consistency" edit has to argue with
        // a failing test rather than a comment.
        let logs = LogIngestLimits::default();
        let metrics = crate::IngestLimits::default();
        assert!(logs.max_attribute_value_len > metrics.max_label_value_len);
        assert!(logs.max_attributes_per_record > metrics.max_attributes_per_point);
    }

    #[test]
    fn rejected_count_uses_batch_count_for_too_many_records() {
        let r = LogRejection::TooManyRecords {
            count: 250_000,
            max: 100_000,
        };
        assert_eq!(r.rejected_count(), 250_000);
    }

    #[test]
    fn rejected_count_defaults_to_one_for_record_scoped_reasons() {
        assert_eq!(LogRejection::UnsupportedBodyKind.rejected_count(), 1);
        assert_eq!(
            LogRejection::TooManyAttributes {
                count: 200,
                max: 128
            }
            .rejected_count(),
            1
        );
        assert_eq!(
            LogRejection::BodyTooLong {
                len: 70_000,
                max: 65_536
            }
            .rejected_count(),
            1
        );
        assert_eq!(
            LogRejection::TooManyResourceAttributes {
                count: 200,
                max: 128
            }
            .rejected_count(),
            1
        );
        assert_eq!(
            LogRejection::TooManyScopeAttributes {
                count: 100,
                max: 64
            }
            .rejected_count(),
            1
        );
        assert_eq!(
            LogRejection::FutureSkew {
                skew_ns: 700_000_000_000,
                max_ns: 600_000_000_000,
            }
            .rejected_count(),
            1
        );
        assert_eq!(
            LogRejection::TooOld {
                lag_ns: 8_000_000_000_000,
                max_ns: 7_200_000_000_000,
            }
            .rejected_count(),
            1
        );
    }

    /// The five per-attribute variants drop one attribute of a record that is
    /// still stored, so on their own they cost the sender no record: their
    /// `rejected_count` is 0 and they carry no admission class. Mirrors the
    /// traces path's attribute-level rejections.
    #[test]
    fn per_attribute_variants_cost_no_record_on_their_own() {
        let variants = [
            LogRejection::AttributeKeyTooLong { len: 300, max: 256 },
            LogRejection::AttributeValueTooLong {
                len: 9000,
                max: 8192,
            },
            LogRejection::MissingAttributeValue { key: "k".into() },
            LogRejection::UnsupportedAttributeValue { key: "k".into() },
            LogRejection::AttributeTooDeeplyNested {
                key: "k".into(),
                max: 100,
            },
        ];
        for v in &variants {
            assert_eq!(v.rejected_count(), 0, "{v:?}");
            assert_eq!(v.admission_class(), None, "{v:?}");
        }
    }

    /// A whole resource or scope lost to an attribute that could not be
    /// converted is carried as a `Grouped`, and must still count as
    /// `structural` for its full record count. The reason inside classes
    /// `None` on its own (previous test), so this pins that the group context
    /// on `Grouped`, not delegation to the reason, is what counts.
    #[test]
    fn grouped_attribute_conversion_failure_counts_as_structural() {
        let r = LogRejection::Grouped {
            reason: Box::new(LogRejection::MissingAttributeValue { key: "svc".into() }),
            count: 7,
        };
        assert_eq!(
            r.admission_class(),
            Some(crate::limits::AdmissionClass::Structural)
        );
        assert_eq!(r.rejected_count(), 7);
    }

    #[test]
    fn grouped_rejected_count_is_the_carried_count_not_one() {
        let r = LogRejection::Grouped {
            reason: Box::new(LogRejection::TooManyResourceAttributes {
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
