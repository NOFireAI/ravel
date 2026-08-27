//! Normalization from a decoded OTLP `ExportLogsServiceRequest` into Ravel's
//! canonical log record representation (ADR-0029,
//! docs/log-segment-format.md).
//!
//! Log stream identity is the canonical hash of the OTLP resource attributes
//! plus the scope name, version, and attributes; per-record attributes never
//! enter identity. Identity is computed once per `ScopeLogs` and reused by
//! every record under it, which is also where the record's `stream_attrs`
//! preimage comes from: both [`ravel_types::logstream::log_stream_id`] and
//! [`ravel_logseg::stream_attrs_bytes`] are called with the same
//! already-converted attribute vectors, so
//! `stream_id == blake3("ravel-logstream-v1" || stream_attrs)[..16]` holds by
//! construction rather than by two similar computations happening to agree.
//!
//! Rejection granularity has three levels, mirroring
//! [`crate::normalize`]'s: request-wide (`TooManyRecords` short-circuits
//! before any record is processed), scope-wide (a resource or scope whose
//! attributes are over the limit or cannot be converted rejects every record
//! under it through one [`LogRejection::Grouped`]), and per-record. Attribute
//! problems inside an admitted record drop that one attribute and report it,
//! never the whole record.
//!
//! Nothing here panics for malformed or oversized input: every problem
//! becomes a [`LogRejection`] so the caller can build an OTLP partial-success
//! response. This includes deeply nested array/kvlist attribute values:
//! [`convert_value`] recurses, so it enforces its own
//! [`MAX_ATTRIBUTE_NESTING_DEPTH`] bound and rejects anything past it. That
//! bound is what makes the no-overflow guarantee hold on its own terms; it
//! does not depend on prost's decode-time recursion limit (an upstream
//! default this crate neither sets nor controls) still being in force.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use ravel_logseg::stream_attrs_bytes;
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};

use crate::logs_limits::{LogIngestLimits, LogRejection};
use crate::promcompat::format_float;

/// One admitted OTLP log record, normalized to Ravel's canonical shape.
/// Not yet a [`ravel_logseg::LogRecord`] (that needs a stream reference,
/// assigned only at flush time by the writer); this is the OTLP-independent
/// shape the ingest router and shard actor buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedLogRecord {
    pub stream_id: LogStreamId,
    /// The exact bytes [`ravel_logseg::stream_attrs_bytes`] produced for this
    /// record's resource+scope: `stream_id ==
    /// blake3("ravel-logstream-v1" || stream_attrs)[..16]`. Carried on every
    /// record, not just once per stream, because the shard buffer and
    /// `RlogWriter::finish()` are the only places positioned to detect a
    /// stream-id collision (two different resource+scope inputs hashing to
    /// the same `stream_id`): `finish()` does that by comparing this field
    /// across every record sharing a `stream_id`.
    pub stream_attrs: Vec<u8>,
    pub ts_ns: i64,
    pub observed_ts_ns: i64,
    pub severity_num: u8,
    pub severity_text: String,
    pub body: String,
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub flags: u32,
    pub attrs: Vec<(String, AttrValue)>,
}

/// Result of normalizing one `ExportLogsServiceRequest`.
#[derive(Debug, Clone, PartialEq)]
pub struct LogNormalizeOutput {
    pub records: Vec<NormalizedLogRecord>,
    pub rejected: Vec<LogRejection>,
}

/// Decode and normalize log records from `req`.
///
/// `ingest_ts_ns` is the receiver's clock reading at admission time, used as
/// the last-resort timestamp for a record that carries neither an event nor
/// an observed timestamp (both are optional in OTLP). Mirrors
/// [`crate::normalize::normalize_metrics`]'s contract: nothing here panics
/// for malformed or oversized input, every problem becomes a
/// [`LogRejection`].
pub fn normalize_logs(
    req: ExportLogsServiceRequest,
    limits: &LogIngestLimits,
    ingest_ts_ns: i64,
) -> LogNormalizeOutput {
    let total_records: usize = req.resource_logs.iter().map(resource_record_count).sum();
    if total_records > limits.max_records_per_request {
        return LogNormalizeOutput {
            records: Vec::new(),
            rejected: vec![LogRejection::TooManyRecords {
                count: total_records,
                max: limits.max_records_per_request,
            }],
        };
    }

    let mut records = Vec::new();
    let mut rejected = Vec::new();
    for rl in &req.resource_logs {
        normalize_resource(rl, limits, ingest_ts_ns, &mut records, &mut rejected);
    }

    LogNormalizeOutput { records, rejected }
}

fn resource_record_count(rl: &ResourceLogs) -> usize {
    rl.scope_logs.iter().map(|sl| sl.log_records.len()).sum()
}

fn normalize_resource(
    rl: &ResourceLogs,
    limits: &LogIngestLimits,
    ingest_ts_ns: i64,
    records: &mut Vec<NormalizedLogRecord>,
    rejected: &mut Vec<LogRejection>,
) {
    let resource_record_count = resource_record_count(rl);
    if resource_record_count == 0 {
        return;
    }

    let resource_attributes = rl
        .resource
        .as_ref()
        .map(|r| r.attributes.as_slice())
        .unwrap_or(&[]);
    if resource_attributes.len() > limits.max_resource_attributes {
        rejected.push(LogRejection::Grouped {
            reason: Box::new(LogRejection::TooManyResourceAttributes {
                count: resource_attributes.len(),
                max: limits.max_resource_attributes,
            }),
            count: resource_record_count,
        });
        return;
    }

    // A resource attribute that cannot be converted rejects every record
    // under the resource: resource attributes are part of stream identity,
    // so admitting the records without it would file them under an identity
    // that silently differs from the one the sender described.
    let resource_attrs = match convert_attrs(resource_attributes, limits) {
        Ok(attrs) => attrs,
        Err(reason) => {
            rejected.push(LogRejection::Grouped {
                reason: Box::new(reason),
                count: resource_record_count,
            });
            return;
        }
    };

    for sl in &rl.scope_logs {
        normalize_scope(sl, &resource_attrs, limits, ingest_ts_ns, records, rejected);
    }
}

fn normalize_scope(
    sl: &ScopeLogs,
    resource_attrs: &[(String, AttrValue)],
    limits: &LogIngestLimits,
    ingest_ts_ns: i64,
    records: &mut Vec<NormalizedLogRecord>,
    rejected: &mut Vec<LogRejection>,
) {
    let scope_record_count = sl.log_records.len();
    if scope_record_count == 0 {
        return;
    }

    let scope = sl.scope.as_ref();
    let scope_name = scope.map(|s| s.name.as_str()).unwrap_or("");
    let scope_version = scope.map(|s| s.version.as_str()).unwrap_or("");
    let scope_attributes = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);

    if scope_attributes.len() > limits.max_scope_attributes {
        rejected.push(LogRejection::Grouped {
            reason: Box::new(LogRejection::TooManyScopeAttributes {
                count: scope_attributes.len(),
                max: limits.max_scope_attributes,
            }),
            count: scope_record_count,
        });
        return;
    }

    let scope_attrs = match convert_attrs(scope_attributes, limits) {
        Ok(attrs) => attrs,
        Err(reason) => {
            rejected.push(LogRejection::Grouped {
                reason: Box::new(reason),
                count: scope_record_count,
            });
            return;
        }
    };

    // Identity and its preimage are computed once per ScopeLogs, from the
    // same converted attribute vectors, and reused for every record below.
    let stream_id = log_stream_id(resource_attrs, scope_name, scope_version, &scope_attrs);
    let stream_attrs = stream_attrs_bytes(resource_attrs, scope_name, scope_version, &scope_attrs);

    for record in &sl.log_records {
        match normalize_record(record, stream_id, &stream_attrs, limits, ingest_ts_ns) {
            Ok((normalized, dropped_attrs)) => {
                records.push(normalized);
                rejected.extend(dropped_attrs);
            }
            Err(reason) => rejected.push(reason),
        }
    }
}

/// Normalize one record. `Err` drops the whole record; `Ok`'s second element
/// carries per-attribute rejections for attributes dropped from an otherwise
/// admitted record.
fn normalize_record(
    record: &LogRecord,
    stream_id: LogStreamId,
    stream_attrs: &[u8],
    limits: &LogIngestLimits,
    ingest_ts_ns: i64,
) -> Result<(NormalizedLogRecord, Vec<LogRejection>), LogRejection> {
    if record.attributes.len() > limits.max_attributes_per_record {
        return Err(LogRejection::TooManyAttributes {
            count: record.attributes.len(),
            max: limits.max_attributes_per_record,
        });
    }

    let body = normalize_body(record.body.as_ref())?;
    if body.len() > limits.max_body_len {
        return Err(LogRejection::BodyTooLong {
            len: body.len(),
            max: limits.max_body_len,
        });
    }

    let observed_ts_ns = match to_i64_ns(record.observed_time_unix_nano) {
        0 => ingest_ts_ns,
        v => v,
    };
    // An OTLP record with neither timestamp set is legal. Fall back to the
    // observed time, then to ingest time, rather than storing a bare zero.
    let ts_ns = checked_record_ts(
        match to_i64_ns(record.time_unix_nano) {
            0 => observed_ts_ns,
            v => v,
        },
        ingest_ts_ns,
        limits,
    )?;

    let mut attrs = Vec::with_capacity(record.attributes.len());
    let mut dropped = Vec::new();
    for kv in &record.attributes {
        match convert_attr(kv, limits) {
            Ok(attr) => attrs.push(attr),
            Err(reason) => dropped.push(reason),
        }
    }

    Ok((
        NormalizedLogRecord {
            stream_id,
            stream_attrs: stream_attrs.to_vec(),
            ts_ns,
            observed_ts_ns,
            // OTLP defines severity_number over 0..=24. A value outside u8
            // is a protocol violation with no meaningful severity; it
            // normalizes to 0 (UNSPECIFIED) rather than truncating into an
            // unrelated severity the sender never sent.
            severity_num: u8::try_from(record.severity_number).unwrap_or(0),
            severity_text: record.severity_text.clone(),
            body,
            // A trace or span id whose length does not match exactly is
            // dropped, never padded or truncated: padding would fabricate an
            // id that never existed.
            trace_id: <[u8; 16]>::try_from(record.trace_id.as_slice()).ok(),
            span_id: <[u8; 8]>::try_from(record.span_id.as_slice()).ok(),
            flags: record.flags,
            attrs,
        },
        dropped,
    ))
}

/// OTLP timestamps are `u64` nanoseconds; real ones fit comfortably in `i64`,
/// and a value that does not is far outside any admissible window, so it
/// saturates rather than wrapping negative.
fn to_i64_ns(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Bound a record's resolved event time to `[ingest_ts_ns -
/// max_ingest_lag_ns, ingest_ts_ns + max_future_skew_ns]` (ADR-0051 §4),
/// mirroring the metrics path's `checked_event_ts`. The bound itself passes;
/// only strictly exceeding it rejects: `ts == ingest_ts + max_future_skew`
/// is accepted, `ts == ingest_ts + max_future_skew + 1` is not. Applied to
/// the *resolved* timestamp, after the observed-time and ingest-time
/// fallbacks, so a record whose only timestamp is a skewed observed time is
/// bounded too; the metrics path's zero-rejection arm has no counterpart
/// here because a zero already fell back to an in-bounds ingest time.
///
/// The catalog listing window is provably complete only under these bounds
/// (docs/consistency-model.md "Late and skewed data"): an out-of-window
/// record would be stored and acked but invisible to every listing-window
/// query, and retention anchors expiry on max event time, so one far-future
/// record would make its hour bucket unexpirable. Rejecting, never clamping:
/// rewriting a sender's event time is silent corruption of the
/// plausible-wrong-result class; a typed rejection is visible and countable.
fn checked_record_ts(
    ts_ns: i64,
    ingest_ts_ns: i64,
    limits: &LogIngestLimits,
) -> Result<i64, LogRejection> {
    let skew_ns = ts_ns.saturating_sub(ingest_ts_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(LogRejection::FutureSkew {
            skew_ns,
            max_ns: limits.max_future_skew_ns,
        });
    }
    let lag_ns = ingest_ts_ns.saturating_sub(ts_ns);
    if lag_ns > limits.max_ingest_lag_ns {
        return Err(LogRejection::TooOld {
            lag_ns,
            max_ns: limits.max_ingest_lag_ns,
        });
    }
    Ok(ts_ns)
}

/// Normalize a record body to the string RLOG stores. A string body maps
/// directly; scalar bodies take their canonical string form; a bytes body
/// becomes lowercase hex. Array and kvlist bodies are rejected rather than
/// stringified: a structured body needs a real storage decision, and a
/// rejection makes the gap visible instead of inventing lossy semantics.
/// An absent body (or an `AnyValue` with no variant set) is the empty string,
/// which is what `ravel_logseg::LogRecord` uses for an absent field.
fn normalize_body(body: Option<&AnyValue>) -> Result<String, LogRejection> {
    match body.and_then(|v| v.value.as_ref()) {
        None => Ok(String::new()),
        Some(AnyValueVariant::StringValue(s)) => Ok(s.clone()),
        Some(AnyValueVariant::BoolValue(b)) => Ok(b.to_string()),
        Some(AnyValueVariant::IntValue(i)) => Ok(i.to_string()),
        Some(AnyValueVariant::DoubleValue(d)) => Ok(format_float(*d)),
        Some(AnyValueVariant::BytesValue(b)) => Ok(hex::encode(b)),
        Some(AnyValueVariant::ArrayValue(_))
        | Some(AnyValueVariant::KvlistValue(_))
        | Some(AnyValueVariant::StringValueStrindex(_)) => Err(LogRejection::UnsupportedBodyKind),
    }
}

/// Convert an attribute set whole: the first failure rejects the set. Used
/// for resource and scope attributes, where a dropped attribute would change
/// stream identity; per-record attributes use [`convert_attr`] directly and
/// drop only the offending attribute.
fn convert_attrs(
    attributes: &[KeyValue],
    limits: &LogIngestLimits,
) -> Result<Vec<(String, AttrValue)>, LogRejection> {
    attributes
        .iter()
        .map(|kv| convert_attr(kv, limits))
        .collect()
}

fn convert_attr(
    kv: &KeyValue,
    limits: &LogIngestLimits,
) -> Result<(String, AttrValue), LogRejection> {
    if kv.key.len() > limits.max_attribute_key_len {
        return Err(LogRejection::AttributeKeyTooLong {
            len: kv.key.len(),
            max: limits.max_attribute_key_len,
        });
    }
    let value = convert_value(&kv.key, kv.value.as_ref(), 1)?;
    let len = attr_value_len(&value);
    if len > limits.max_attribute_value_len {
        return Err(LogRejection::AttributeValueTooLong {
            len,
            max: limits.max_attribute_value_len,
        });
    }
    Ok((kv.key.clone(), value))
}

/// Maximum nesting depth [`convert_value`] will follow through array and
/// kvlist attribute values before rejecting, counting the top-level attribute
/// value as level 1 and each enclosing array or kvlist as one more level.
///
/// Set to 100 to match prost's default message-decode recursion limit
/// (`prost::Message::decode` caps nested-message depth at 100), which is the
/// only thing that bounds this converter today: a decoded `AnyValue` cannot
/// arrive nested past that limit, so no legitimate input reaches this depth.
/// Asserting the same bound here makes the "nothing here overflows the stack"
/// guarantee stand on this crate's own check rather than on an upstream
/// default it neither sets nor controls; a value nested deeper has no
/// meaningful attribute semantics and is rejected rather than recursed
/// through. This mirrors ravel-promql's parser complexity guard (#529), which
/// likewise refuses to depend on an upstream library surviving unbounded
/// recursion.
pub const MAX_ATTRIBUTE_NESTING_DEPTH: usize = 100;

/// Map one OTLP `AnyValue` to the canonical [`AttrValue`]. Lists and maps
/// recurse through the same mapping. `key` is carried only for the rejection
/// message, and it is always the enclosing top-level attribute's key, never
/// the innermost list index or map entry key: the converter drops the whole
/// attribute either way, so the rejection needs the name of the thing that
/// was dropped, not an inner map entry that no longer exists as an
/// independent value. Both recursive arms pass `key` through unchanged
/// (issue #808).
///
/// `depth` is the current nesting level (1 for a top-level attribute value,
/// one more per enclosing array or kvlist). Exceeding
/// [`MAX_ATTRIBUTE_NESTING_DEPTH`] rejects the value rather than recursing
/// further, so a malformed or hostile payload cannot drive this recursion
/// past a bounded depth.
fn convert_value(
    key: &str,
    value: Option<&AnyValue>,
    depth: usize,
) -> Result<AttrValue, LogRejection> {
    if depth > MAX_ATTRIBUTE_NESTING_DEPTH {
        return Err(LogRejection::AttributeTooDeeplyNested {
            key: key.to_string(),
            max: MAX_ATTRIBUTE_NESTING_DEPTH,
        });
    }
    match value.and_then(|v| v.value.as_ref()) {
        None => Err(LogRejection::MissingAttributeValue {
            key: key.to_string(),
        }),
        Some(AnyValueVariant::StringValue(s)) => Ok(AttrValue::Str(s.clone())),
        Some(AnyValueVariant::BoolValue(b)) => Ok(AttrValue::Bool(*b)),
        Some(AnyValueVariant::IntValue(i)) => Ok(AttrValue::I64(*i)),
        Some(AnyValueVariant::DoubleValue(d)) => Ok(AttrValue::F64(*d)),
        Some(AnyValueVariant::BytesValue(b)) => Ok(AttrValue::Bytes(b.clone())),
        Some(AnyValueVariant::ArrayValue(array)) => {
            let items = array
                .values
                .iter()
                .map(|v| convert_value(key, Some(v), depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AttrValue::List(items))
        }
        Some(AnyValueVariant::KvlistValue(kvlist)) => {
            let entries = kvlist
                .values
                .iter()
                .map(|kv| {
                    convert_value(key, kv.value.as_ref(), depth + 1).map(|v| (kv.key.clone(), v))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AttrValue::Map(entries))
        }
        Some(AnyValueVariant::StringValueStrindex(_)) => {
            Err(LogRejection::UnsupportedAttributeValue {
                key: key.to_string(),
            })
        }
    }
}

/// Payload bytes in an attribute value: its own string or bytes payload, plus
/// nested entries (and their keys) for lists and maps. Scalars count as their
/// stored width. This is what `max_attribute_value_len` bounds; it is a size
/// measure over the value, not the exact length of the canonical encoding,
/// which additionally carries type tags and length prefixes.
fn attr_value_len(value: &AttrValue) -> usize {
    match value {
        AttrValue::Str(s) => s.len(),
        AttrValue::Bytes(b) => b.len(),
        AttrValue::I64(_) | AttrValue::F64(_) => 8,
        AttrValue::Bool(_) => 1,
        AttrValue::List(items) => items.iter().map(attr_value_len).sum(),
        AttrValue::Map(entries) => entries
            .iter()
            .map(|(k, v)| k.len() + attr_value_len(v))
            .sum(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, InstrumentationScope, KeyValueList};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn any(value: AnyValueVariant) -> AnyValue {
        AnyValue { value: Some(value) }
    }

    fn kv(key: &str, value: AnyValueVariant) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(any(value)),
            ..Default::default()
        }
    }

    fn string_kv(key: &str, value: &str) -> KeyValue {
        kv(key, AnyValueVariant::StringValue(value.to_string()))
    }

    fn record(body: Option<AnyValue>, attrs: Vec<KeyValue>, ts_ns: u64) -> LogRecord {
        LogRecord {
            time_unix_nano: ts_ns,
            observed_time_unix_nano: ts_ns,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body,
            attributes: attrs,
            flags: 0,
            ..Default::default()
        }
    }

    fn scope_logs(name: &str, version: &str, records: Vec<LogRecord>) -> ScopeLogs {
        ScopeLogs {
            scope: Some(InstrumentationScope {
                name: name.to_string(),
                version: version.to_string(),
                ..Default::default()
            }),
            log_records: records,
            ..Default::default()
        }
    }

    fn resource_logs(resource_attrs: Vec<KeyValue>, scopes: Vec<ScopeLogs>) -> ResourceLogs {
        ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attrs,
                ..Default::default()
            }),
            scope_logs: scopes,
            ..Default::default()
        }
    }

    fn request(resources: Vec<ResourceLogs>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: resources,
        }
    }

    fn normalize(req: ExportLogsServiceRequest) -> LogNormalizeOutput {
        normalize_logs(req, &LogIngestLimits::default(), 5_000)
    }

    #[test]
    fn empty_request_yields_empty_output() {
        let out = normalize(request(vec![]));
        assert!(out.records.is_empty());
        assert!(out.rejected.is_empty());
    }

    #[test]
    fn string_body_record_normalizes_field_for_field() {
        let mut rec = record(
            Some(any(AnyValueVariant::StringValue("hello".into()))),
            vec![
                string_kv("http.method", "GET"),
                kv("http.status", AnyValueVariant::IntValue(200)),
            ],
            1_700,
        );
        rec.trace_id = vec![7u8; 16];
        rec.span_id = vec![3u8; 8];
        rec.flags = 1;
        let out = normalize(request(vec![resource_logs(
            vec![string_kv("service.name", "api")],
            vec![scope_logs("lib", "1.0", vec![rec])],
        )]));

        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.records.len(), 1);
        let r = &out.records[0];
        assert_eq!(r.body, "hello");
        assert_eq!(r.ts_ns, 1_700);
        assert_eq!(r.observed_ts_ns, 1_700);
        assert_eq!(r.severity_num, 9);
        assert_eq!(r.severity_text, "INFO");
        assert_eq!(r.trace_id, Some([7u8; 16]));
        assert_eq!(r.span_id, Some([3u8; 8]));
        assert_eq!(r.flags, 1);
        assert_eq!(
            r.attrs,
            vec![
                ("http.method".to_string(), AttrValue::Str("GET".into())),
                ("http.status".to_string(), AttrValue::I64(200)),
            ]
        );
        assert_eq!(
            r.stream_id,
            log_stream_id(
                &[("service.name".to_string(), AttrValue::Str("api".into()))],
                "lib",
                "1.0",
                &[]
            )
        );
    }

    #[test]
    fn records_under_one_scope_share_a_stream_id() {
        let out = normalize(request(vec![resource_logs(
            vec![string_kv("service.name", "api")],
            vec![scope_logs(
                "lib",
                "1.0",
                vec![
                    record(
                        Some(any(AnyValueVariant::StringValue("a".into()))),
                        vec![],
                        1,
                    ),
                    record(
                        Some(any(AnyValueVariant::StringValue("b".into()))),
                        vec![],
                        2,
                    ),
                ],
            )],
        )]));
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].stream_id, out.records[1].stream_id);
        assert_eq!(out.records[0].stream_attrs, out.records[1].stream_attrs);
    }

    #[test]
    fn different_scope_names_produce_different_stream_ids() {
        let rec = || {
            record(
                Some(any(AnyValueVariant::StringValue("x".into()))),
                vec![],
                1,
            )
        };
        let out = normalize(request(vec![resource_logs(
            vec![string_kv("service.name", "api")],
            vec![
                scope_logs("lib-a", "1.0", vec![rec()]),
                scope_logs("lib-b", "1.0", vec![rec()]),
            ],
        )]));
        assert_eq!(out.records.len(), 2);
        assert_ne!(out.records[0].stream_id, out.records[1].stream_id);
    }

    #[test]
    fn stream_attrs_is_the_stream_id_preimage_for_every_record() {
        // Proves stream_attrs comes from the same inputs log_stream_id used,
        // not from a coincidentally-similar computation: the domain-prefixed
        // hash of the carried preimage must reproduce the carried id.
        let out = normalize(request(vec![
            resource_logs(
                vec![
                    string_kv("service.name", "api"),
                    kv("replicas", AnyValueVariant::IntValue(3)),
                ],
                vec![scope_logs(
                    "lib",
                    "1.0",
                    vec![record(
                        Some(any(AnyValueVariant::StringValue("a".into()))),
                        vec![],
                        1,
                    )],
                )],
            ),
            resource_logs(
                vec![],
                vec![scope_logs(
                    "",
                    "",
                    vec![record(
                        Some(any(AnyValueVariant::StringValue("b".into()))),
                        vec![],
                        2,
                    )],
                )],
            ),
        ]));
        assert_eq!(out.records.len(), 2);
        for r in &out.records {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"ravel-logstream-v1");
            hasher.update(&r.stream_attrs);
            assert_eq!(&hasher.finalize().as_bytes()[..16], &r.stream_id.0[..]);
        }
    }

    #[test]
    fn absent_body_normalizes_to_empty_string() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs("lib", "1", vec![record(None, vec![], 1)])],
        )]));
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.records[0].body, "");
    }

    #[test]
    fn double_body_normalizes_via_format_float() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![
                    record(Some(any(AnyValueVariant::DoubleValue(1.5))), vec![], 1),
                    record(
                        Some(any(AnyValueVariant::DoubleValue(f64::INFINITY))),
                        vec![],
                        2,
                    ),
                ],
            )],
        )]));
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.records[0].body, format_float(1.5));
        assert_eq!(out.records[1].body, "+Inf");
    }

    #[test]
    fn bytes_body_normalizes_to_hex() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::BytesValue(vec![0xde, 0xad]))),
                    vec![],
                    1,
                )],
            )],
        )]));
        assert_eq!(out.records[0].body, "dead");
    }

    #[test]
    fn array_body_rejects_the_record() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![
                    record(
                        Some(any(AnyValueVariant::ArrayValue(ArrayValue {
                            values: vec![any(AnyValueVariant::IntValue(1))],
                        }))),
                        vec![],
                        1,
                    ),
                    record(
                        Some(any(AnyValueVariant::StringValue("ok".into()))),
                        vec![],
                        2,
                    ),
                ],
            )],
        )]));
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].body, "ok");
        assert_eq!(out.rejected, vec![LogRejection::UnsupportedBodyKind]);
        assert_eq!(out.rejected[0].rejected_count(), 1);
    }

    #[test]
    fn oversized_attribute_value_drops_the_attribute_not_the_record() {
        let limits = LogIngestLimits::default();
        let big = "x".repeat(limits.max_attribute_value_len + 1);
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![string_kv("small", "v"), string_kv("big", &big)],
                    1,
                )],
            )],
        )]));
        assert_eq!(out.records.len(), 1);
        assert_eq!(
            out.records[0].attrs,
            vec![("small".to_string(), AttrValue::Str("v".into()))]
        );
        assert_eq!(
            out.rejected,
            vec![LogRejection::AttributeValueTooLong {
                len: limits.max_attribute_value_len + 1,
                max: limits.max_attribute_value_len,
            }]
        );
    }

    #[test]
    fn attribute_without_a_value_is_reported_and_the_record_kept() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![KeyValue {
                        key: "novalue".to_string(),
                        value: None,
                        ..Default::default()
                    }],
                    1,
                )],
            )],
        )]));
        assert_eq!(out.records.len(), 1);
        assert!(out.records[0].attrs.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::MissingAttributeValue {
                key: "novalue".to_string()
            }]
        );
    }

    #[test]
    fn nested_list_and_map_attributes_map_through_the_same_variants() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![
                        kv(
                            "list",
                            AnyValueVariant::ArrayValue(ArrayValue {
                                values: vec![
                                    any(AnyValueVariant::StringValue("a".into())),
                                    any(AnyValueVariant::BoolValue(true)),
                                ],
                            }),
                        ),
                        kv(
                            "map",
                            AnyValueVariant::KvlistValue(KeyValueList {
                                values: vec![kv("inner", AnyValueVariant::DoubleValue(2.5))],
                            }),
                        ),
                    ],
                    1,
                )],
            )],
        )]));
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(
            out.records[0].attrs,
            vec![
                (
                    "list".to_string(),
                    AttrValue::List(vec![AttrValue::Str("a".into()), AttrValue::Bool(true)])
                ),
                (
                    "map".to_string(),
                    AttrValue::Map(vec![("inner".to_string(), AttrValue::F64(2.5))])
                ),
            ]
        );
    }

    /// Wrap `leaf` in `layers` nested single-element `ArrayValue`s. The leaf
    /// then sits at [`convert_value`] nesting level `layers + 1` (the
    /// outermost array is level 1).
    fn nested_array(layers: usize, leaf: AnyValueVariant) -> AnyValue {
        let mut value = any(leaf);
        for _ in 0..layers {
            value = any(AnyValueVariant::ArrayValue(ArrayValue {
                values: vec![value],
            }));
        }
        value
    }

    fn attr_kv(key: &str, value: AnyValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(value),
            ..Default::default()
        }
    }

    #[test]
    fn attribute_nested_exactly_at_the_depth_limit_converts() {
        // MAX - 1 array layers put the I64 leaf at nesting level
        // MAX_ATTRIBUTE_NESTING_DEPTH: the deepest level that still converts.
        let deep = nested_array(
            MAX_ATTRIBUTE_NESTING_DEPTH - 1,
            AnyValueVariant::IntValue(7),
        );
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv("deep", deep)],
                    1,
                )],
            )],
        )]));
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.records.len(), 1);
        let (key, value) = &out.records[0].attrs[0];
        assert_eq!(key, "deep");
        // MAX - 1 single-element List layers around the I64(7) leaf.
        let mut cur = value;
        for _ in 0..(MAX_ATTRIBUTE_NESTING_DEPTH - 1) {
            match cur {
                AttrValue::List(items) => {
                    assert_eq!(items.len(), 1);
                    cur = &items[0];
                }
                other => panic!("expected List layer, got {other:?}"),
            }
        }
        assert_eq!(cur, &AttrValue::I64(7));
    }

    #[test]
    fn attribute_nested_one_past_the_limit_is_rejected_with_typed_error() {
        // MAX array layers put the leaf at level MAX + 1, one past the limit.
        // The over-nested attribute is dropped and reported; the record and
        // its sibling attribute survive (per-record partial-failure contract).
        let deep = nested_array(MAX_ATTRIBUTE_NESTING_DEPTH, AnyValueVariant::IntValue(7));
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv("deep", deep), string_kv("ok", "1")],
                    1,
                )],
            )],
        )]));
        assert_eq!(out.records.len(), 1);
        assert_eq!(
            out.records[0].attrs,
            vec![("ok".to_string(), AttrValue::Str("1".into()))]
        );
        assert_eq!(
            out.rejected,
            vec![LogRejection::AttributeTooDeeplyNested {
                key: "deep".to_string(),
                max: MAX_ATTRIBUTE_NESTING_DEPTH,
            }]
        );
        assert_eq!(
            out.rejected[0].to_string(),
            "attribute deep nests more than 100 levels deep"
        );
    }

    /// The rejection is per record: a batch with one over-nested record and
    /// one clean record admits both. The module's rejection-granularity
    /// contract (see the file-level doc) says an attribute problem inside an
    /// admitted record drops that one attribute and reports it, never the
    /// whole record and never a sibling.
    #[test]
    fn over_nested_attribute_rejection_does_not_spread_to_a_sibling_record() {
        let deep = nested_array(MAX_ATTRIBUTE_NESTING_DEPTH, AnyValueVariant::IntValue(7));
        let bad = record(
            Some(any(AnyValueVariant::StringValue("a".into()))),
            vec![attr_kv("deep", deep)],
            1,
        );
        let good = record(
            Some(any(AnyValueVariant::StringValue("b".into()))),
            vec![string_kv("x", "y")],
            2,
        );
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs("lib", "1", vec![bad, good])],
        )]));
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].body, "a");
        assert!(out.records[0].attrs.is_empty());
        assert_eq!(out.records[1].body, "b");
        assert_eq!(
            out.records[1].attrs,
            vec![("x".to_string(), AttrValue::Str("y".into()))]
        );
        assert_eq!(
            out.rejected,
            vec![LogRejection::AttributeTooDeeplyNested {
                key: "deep".to_string(),
                max: MAX_ATTRIBUTE_NESTING_DEPTH,
            }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 1);
    }

    // --- convert_value key identity (#808): both recursive arms must report
    // the enclosing top-level attribute's key, never an inner list index or
    // map entry key, since the converter drops the whole attribute either
    // way and an inner key may not even be unique.

    #[test]
    fn value_rejected_inside_a_list_reports_the_enclosing_attribute_key() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv(
                        "outer",
                        any(AnyValueVariant::ArrayValue(ArrayValue {
                            values: vec![AnyValue { value: None }],
                        })),
                    )],
                    1,
                )],
            )],
        )]));
        assert_eq!(
            out.rejected,
            vec![LogRejection::MissingAttributeValue {
                key: "outer".to_string()
            }]
        );
    }

    #[test]
    fn value_rejected_inside_a_kvlist_reports_the_enclosing_attribute_key() {
        // The changing case: before #808 this arm rebound `key` to the map
        // entry's own key ("inner") instead of the enclosing attribute's.
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv(
                        "outer",
                        any(AnyValueVariant::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "inner".to_string(),
                                value: None,
                                ..Default::default()
                            }],
                        })),
                    )],
                    1,
                )],
            )],
        )]));
        assert_eq!(
            out.rejected,
            vec![LogRejection::MissingAttributeValue {
                key: "outer".to_string()
            }]
        );
    }

    #[test]
    fn value_rejected_inside_a_kvlist_nested_in_a_list_reports_the_enclosing_attribute_key() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv(
                        "outer",
                        any(AnyValueVariant::ArrayValue(ArrayValue {
                            values: vec![any(AnyValueVariant::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key: "inner".to_string(),
                                    value: None,
                                    ..Default::default()
                                }],
                            }))],
                        })),
                    )],
                    1,
                )],
            )],
        )]));
        assert_eq!(
            out.rejected,
            vec![LogRejection::MissingAttributeValue {
                key: "outer".to_string()
            }]
        );
    }

    #[test]
    fn value_rejected_inside_a_list_nested_in_a_kvlist_reports_the_enclosing_attribute_key() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv(
                        "outer",
                        any(AnyValueVariant::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "inner".to_string(),
                                value: Some(any(AnyValueVariant::ArrayValue(ArrayValue {
                                    values: vec![AnyValue { value: None }],
                                }))),
                                ..Default::default()
                            }],
                        })),
                    )],
                    1,
                )],
            )],
        )]));
        assert_eq!(
            out.rejected,
            vec![LogRejection::MissingAttributeValue {
                key: "outer".to_string()
            }]
        );
    }

    #[test]
    fn unsupported_attribute_value_inside_a_kvlist_reports_the_enclosing_attribute_key() {
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv(
                        "outer",
                        any(AnyValueVariant::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "inner".to_string(),
                                value: Some(any(AnyValueVariant::StringValueStrindex(5))),
                                ..Default::default()
                            }],
                        })),
                    )],
                    1,
                )],
            )],
        )]));
        assert_eq!(
            out.rejected,
            vec![LogRejection::UnsupportedAttributeValue {
                key: "outer".to_string()
            }]
        );
    }

    #[test]
    fn attribute_too_deeply_nested_inside_a_kvlist_reports_the_enclosing_attribute_key() {
        // The depth guard reaches its rejection through the `depth > MAX`
        // check at the top of convert_value, a different route than the
        // Missing/Unsupported arms below it, so it needs its own coverage
        // of the same key-identity claim.
        let deep = nested_array(MAX_ATTRIBUTE_NESTING_DEPTH, AnyValueVariant::IntValue(7));
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("body".into()))),
                    vec![attr_kv(
                        "outer",
                        any(AnyValueVariant::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "inner".to_string(),
                                value: Some(deep),
                                ..Default::default()
                            }],
                        })),
                    )],
                    1,
                )],
            )],
        )]));
        assert_eq!(
            out.rejected,
            vec![LogRejection::AttributeTooDeeplyNested {
                key: "outer".to_string(),
                max: MAX_ATTRIBUTE_NESTING_DEPTH,
            }]
        );
    }

    #[test]
    fn too_many_records_short_circuits_the_whole_request() {
        let limits = LogIngestLimits {
            max_records_per_request: 2,
            ..LogIngestLimits::default()
        };
        let recs = (0..3)
            .map(|i| {
                record(
                    Some(any(AnyValueVariant::StringValue("x".into()))),
                    vec![],
                    i + 1,
                )
            })
            .collect();
        let out = normalize_logs(
            request(vec![resource_logs(
                vec![],
                vec![scope_logs("lib", "1", recs)],
            )]),
            &limits,
            5_000,
        );
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::TooManyRecords { count: 3, max: 2 }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 3);
    }

    #[test]
    fn malformed_trace_id_length_normalizes_to_none() {
        let mut rec = record(
            Some(any(AnyValueVariant::StringValue("x".into()))),
            vec![],
            1,
        );
        rec.trace_id = vec![1, 2, 3];
        rec.span_id = vec![1; 7];
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs("lib", "1", vec![rec])],
        )]));
        assert_eq!(out.records[0].trace_id, None);
        assert_eq!(out.records[0].span_id, None);
    }

    #[test]
    fn missing_timestamps_fall_back_to_observed_then_ingest_time() {
        let mut only_observed = record(
            Some(any(AnyValueVariant::StringValue("a".into()))),
            vec![],
            0,
        );
        only_observed.observed_time_unix_nano = 900;
        let neither = record(
            Some(any(AnyValueVariant::StringValue("b".into()))),
            vec![],
            0,
        );
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs("lib", "1", vec![only_observed, neither])],
        )]));
        assert_eq!(out.records[0].ts_ns, 900);
        assert_eq!(out.records[0].observed_ts_ns, 900);
        assert_eq!(out.records[1].ts_ns, 5_000);
        assert_eq!(out.records[1].observed_ts_ns, 5_000);
    }

    #[test]
    fn oversized_resource_attributes_reject_every_record_under_the_resource() {
        let limits = LogIngestLimits {
            max_resource_attributes: 1,
            ..LogIngestLimits::default()
        };
        let recs: Vec<LogRecord> = (0..4)
            .map(|i| {
                record(
                    Some(any(AnyValueVariant::StringValue("x".into()))),
                    vec![],
                    i + 1,
                )
            })
            .collect();
        let out = normalize_logs(
            request(vec![resource_logs(
                vec![string_kv("a", "1"), string_kv("b", "2")],
                vec![scope_logs("lib", "1", recs)],
            )]),
            &limits,
            5_000,
        );
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::Grouped {
                reason: Box::new(LogRejection::TooManyResourceAttributes { count: 2, max: 1 }),
                count: 4,
            }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 4);
    }

    #[test]
    fn oversized_scope_attributes_reject_only_that_scope() {
        let limits = LogIngestLimits {
            max_scope_attributes: 0,
            ..LogIngestLimits::default()
        };
        let rec = || {
            record(
                Some(any(AnyValueVariant::StringValue("x".into()))),
                vec![],
                1,
            )
        };
        let mut bad_scope = scope_logs("lib-b", "1", vec![rec(), rec()]);
        if let Some(scope) = bad_scope.scope.as_mut() {
            scope.attributes = vec![string_kv("k", "v")];
        }
        let out = normalize_logs(
            request(vec![resource_logs(
                vec![],
                vec![scope_logs("lib-a", "1", vec![rec()]), bad_scope],
            )]),
            &limits,
            5_000,
        );
        assert_eq!(out.records.len(), 1);
        assert_eq!(
            out.rejected,
            vec![LogRejection::Grouped {
                reason: Box::new(LogRejection::TooManyScopeAttributes { count: 1, max: 0 }),
                count: 2,
            }]
        );
    }

    #[test]
    fn unconvertible_resource_attribute_rejects_the_resource_as_one_group() {
        // A resource attribute with no value cannot be dropped like a record
        // attribute: it is part of stream identity, so the whole resource's
        // records are rejected through one Grouped entry rather than one
        // rejection per record.
        let out = normalize(request(vec![resource_logs(
            vec![KeyValue {
                key: "service.name".to_string(),
                value: None,
                ..Default::default()
            }],
            vec![scope_logs(
                "lib",
                "1",
                vec![
                    record(
                        Some(any(AnyValueVariant::StringValue("a".into()))),
                        vec![],
                        1,
                    ),
                    record(
                        Some(any(AnyValueVariant::StringValue("b".into()))),
                        vec![],
                        2,
                    ),
                ],
            )],
        )]));
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::Grouped {
                reason: Box::new(LogRejection::MissingAttributeValue {
                    key: "service.name".to_string()
                }),
                count: 2,
            }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 2);
    }

    #[test]
    fn oversized_body_rejects_the_record() {
        let limits = LogIngestLimits {
            max_body_len: 4,
            ..LogIngestLimits::default()
        };
        let out = normalize_logs(
            request(vec![resource_logs(
                vec![],
                vec![scope_logs(
                    "lib",
                    "1",
                    vec![record(
                        Some(any(AnyValueVariant::StringValue("toolong".into()))),
                        vec![],
                        1,
                    )],
                )],
            )]),
            &limits,
            5_000,
        );
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::BodyTooLong { len: 7, max: 4 }]
        );
    }

    #[test]
    fn too_many_record_attributes_rejects_the_record() {
        let limits = LogIngestLimits {
            max_attributes_per_record: 1,
            ..LogIngestLimits::default()
        };
        let out = normalize_logs(
            request(vec![resource_logs(
                vec![],
                vec![scope_logs(
                    "lib",
                    "1",
                    vec![record(
                        Some(any(AnyValueVariant::StringValue("x".into()))),
                        vec![string_kv("a", "1"), string_kv("b", "2")],
                        1,
                    )],
                )],
            )]),
            &limits,
            5_000,
        );
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::TooManyAttributes { count: 2, max: 1 }]
        );
    }

    // --- event-time skew bounds (ADR-0051 §4) ---
    // Convention, shared with the metrics path: the bound itself passes; one
    // ns past it fails.

    /// A realistic ingest clock reading, so the skew arithmetic runs on
    /// full-size nanosecond timestamps rather than tiny test integers.
    const INGEST_TS: i64 = 1_754_000_000_000_000_000;

    fn one_record_at(ts_ns: u64) -> ExportLogsServiceRequest {
        request(vec![resource_logs(
            vec![string_kv("service.name", "api")],
            vec![scope_logs(
                "lib",
                "1",
                vec![record(
                    Some(any(AnyValueVariant::StringValue("x".into()))),
                    vec![],
                    ts_ns,
                )],
            )],
        )])
    }

    #[test]
    fn rejects_future_skewed_record() {
        let limits = LogIngestLimits::default();
        let ts = INGEST_TS + limits.max_future_skew_ns + 1;
        let out = normalize_logs(one_record_at(ts as u64), &limits, INGEST_TS);
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::FutureSkew {
                skew_ns: limits.max_future_skew_ns + 1,
                max_ns: limits.max_future_skew_ns,
            }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 1);
    }

    #[test]
    fn rejects_record_older_than_max_ingest_lag() {
        let limits = LogIngestLimits::default();
        let ts = INGEST_TS - limits.max_ingest_lag_ns - 1;
        let out = normalize_logs(one_record_at(ts as u64), &limits, INGEST_TS);
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::TooOld {
                lag_ns: limits.max_ingest_lag_ns + 1,
                max_ns: limits.max_ingest_lag_ns,
            }]
        );
    }

    #[test]
    fn record_exactly_at_either_bound_is_accepted() {
        let limits = LogIngestLimits::default();
        for ts in [
            INGEST_TS + limits.max_future_skew_ns,
            INGEST_TS - limits.max_ingest_lag_ns,
        ] {
            let out = normalize_logs(one_record_at(ts as u64), &limits, INGEST_TS);
            assert!(out.rejected.is_empty(), "{:?}", out.rejected);
            assert_eq!(out.records.len(), 1);
            assert_eq!(out.records[0].ts_ns, ts);
        }
    }

    /// The bound applies to the *resolved* timestamp: a record with no event
    /// time falls back to its observed time, and a skewed observed time must
    /// not smuggle an out-of-window timestamp past the check.
    #[test]
    fn skewed_observed_time_fallback_is_rejected_too() {
        let limits = LogIngestLimits::default();
        let mut rec = record(
            Some(any(AnyValueVariant::StringValue("x".into()))),
            vec![],
            0,
        );
        rec.observed_time_unix_nano = (INGEST_TS + limits.max_future_skew_ns + 1) as u64;
        let out = normalize_logs(
            request(vec![resource_logs(
                vec![],
                vec![scope_logs("lib", "1", vec![rec])],
            )]),
            &limits,
            INGEST_TS,
        );
        assert!(out.records.is_empty());
        assert_eq!(
            out.rejected,
            vec![LogRejection::FutureSkew {
                skew_ns: limits.max_future_skew_ns + 1,
                max_ns: limits.max_future_skew_ns,
            }]
        );
    }

    /// A u64 timestamp past i64::MAX saturates and is rejected as future
    /// skew rather than wrapping negative into the admissible window.
    #[test]
    fn timestamp_past_i64_max_is_rejected_not_wrapped() {
        let out = normalize_logs(
            one_record_at(u64::MAX),
            &LogIngestLimits::default(),
            INGEST_TS,
        );
        assert!(out.records.is_empty());
        assert!(
            matches!(out.rejected.as_slice(), [LogRejection::FutureSkew { .. }]),
            "{:?}",
            out.rejected
        );
    }

    proptest::proptest! {
        /// Over arbitrary timestamps, a record is admitted exactly when its
        /// resolved event time lies in
        /// `[ingest - max_ingest_lag, ingest + max_future_skew]`; nothing is
        /// ever both admitted and rejected, and nothing panics.
        #[test]
        fn skew_bounds_partition_admission(
            ts_ns in 1u64..=u64::MAX,
            offset_ns in -4_000_000_000_000i64..=4_000_000_000_000i64,
        ) {
            let limits = LogIngestLimits::default();
            let ingest_ts = INGEST_TS + offset_ns;
            let out = normalize_logs(one_record_at(ts_ns), &limits, ingest_ts);
            let resolved = i64::try_from(ts_ns).unwrap_or(i64::MAX);
            let in_bounds = resolved >= ingest_ts - limits.max_ingest_lag_ns
                && resolved <= ingest_ts + limits.max_future_skew_ns;
            if in_bounds {
                proptest::prop_assert_eq!(out.records.len(), 1);
                proptest::prop_assert!(out.rejected.is_empty());
                proptest::prop_assert_eq!(out.records[0].ts_ns, resolved);
            } else {
                proptest::prop_assert!(out.records.is_empty());
                proptest::prop_assert_eq!(out.rejected.len(), 1);
                let is_skew_rejection = matches!(
                    out.rejected[0],
                    LogRejection::FutureSkew { .. } | LogRejection::TooOld { .. }
                );
                proptest::prop_assert!(is_skew_rejection, "{:?}", out.rejected);
            }
        }
    }

    #[test]
    fn out_of_range_severity_number_normalizes_to_unspecified() {
        let mut rec = record(
            Some(any(AnyValueVariant::StringValue("x".into()))),
            vec![],
            1,
        );
        rec.severity_number = 300;
        let out = normalize(request(vec![resource_logs(
            vec![],
            vec![scope_logs("lib", "1", vec![rec])],
        )]));
        assert_eq!(out.records[0].severity_num, 0);
    }
}
