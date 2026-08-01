//! Normalization from a decoded OTLP `ExportTraceServiceRequest` into Ravel's
//! canonical span representation (ADR-0041, docs/span-segment-format.md).
//!
//! Spans have no stream identity, which is the one structural difference from
//! [`crate::logs_normalize`]. RLOG derives a `stream_id` from the resource and
//! scope and keeps their attributes in a separate identity blob; RSPAN routes
//! and sorts by `trace_id` (ADR-0041 decision 2) and stores exactly one merged
//! `attrs` map per span. So the resource and scope attribute sets are converted
//! once per `ScopeSpans` and merged into every span under it through
//! [`ravel_rspan::merge_attrs`], following the same
//! resource-beats-scope-beats-record precedence `docs/log-segment-format.md`
//! documents for logs.
//!
//! That difference also changes what a bad resource attribute costs. In
//! [`crate::logs_normalize`] a resource attribute that cannot be converted
//! rejects every record under the resource, because dropping it would file the
//! records under an identity that silently differs from the one the sender
//! described. Here nothing derives identity from resource attributes, so an
//! unconvertible one is dropped and reported on its own and the spans are still
//! admitted. That matters in practice: OTel SDKs routinely set array-valued
//! resource attributes (`process.command_args`), and rejecting every span from
//! any such process would be a far worse failure than storing the rest of its
//! attributes.
//!
//! Rejection granularity otherwise mirrors the logs path: request-wide
//! (`TooManySpans` short-circuits before any span is processed), scope-wide (a
//! resource or scope with too many attributes rejects every span under it
//! through one [`SpanRejection::Grouped`]), and per-span. Attribute problems
//! inside an admitted span drop that one attribute and report it.
//!
//! # Fields with no RSPAN column
//!
//! ADR-0041's record shape has no column for span kind, trace state, span
//! flags, events, or links. None of them is silently dropped: each is stored in
//! the merged `attrs` map under a reserved underscore-prefixed key
//! ([`ATTR_SPAN_KIND`] and friends), and the reserved keys are applied after the
//! merge, so they win over a sender's own attribute of the same name. Events
//! and links keep ADR-0041's "opaque blob, visibly present, unindexed" shape:
//! their concatenated length-delimited protobuf encodings, lowercase-hex.
//!
//! The three `dropped_*_count` fields are the deliberate exception and are not
//! stored: they describe the sender's own SDK-side drops, not the span, and
//! carry no meaning once the span is at rest.
//!
//! Nothing here panics for malformed or oversized input: every problem becomes
//! a [`SpanRejection`] so the caller can build an OTLP partial-success response.

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use ravel_rspan::{StatusCode, merge_attrs};

use crate::promcompat::format_float;
use crate::traces_limits::{SpanIngestLimits, SpanRejection};

/// Reserved `attrs` key holding the span kind (see [`span_kind_name`]).
pub const ATTR_SPAN_KIND: &str = "_kind";
/// Reserved `attrs` key holding the W3C `tracestate` header value.
pub const ATTR_TRACE_STATE: &str = "_trace_state";
/// Reserved `attrs` key holding the decimal W3C trace flags, when non-zero.
pub const ATTR_SPAN_FLAGS: &str = "_flags";
/// Reserved `attrs` key holding the span's events: the concatenated
/// length-delimited protobuf encodings of every
/// `opentelemetry.proto.trace.v1.Span.Event`, lowercase-hex. Opaque and
/// unindexed in RSPAN v1 (ADR-0041 decision 4).
pub const ATTR_EVENTS_RAW: &str = "_events_raw";
/// Reserved `attrs` key holding the span's links, encoded exactly like
/// [`ATTR_EVENTS_RAW`] but over `opentelemetry.proto.trace.v1.Span.Link`.
pub const ATTR_LINKS_RAW: &str = "_links_raw";

/// Every reserved `attrs` key, in one place so stripping a sender's own
/// attribute of the same name (see [`normalize_span`]) cannot drift from the
/// set [`reserved_attrs`] actually populates.
const RESERVED_ATTR_KEYS: [&str; 5] = [
    ATTR_SPAN_KIND,
    ATTR_TRACE_STATE,
    ATTR_SPAN_FLAGS,
    ATTR_EVENTS_RAW,
    ATTR_LINKS_RAW,
];

fn is_reserved_key(key: &str) -> bool {
    RESERVED_ATTR_KEYS.contains(&key)
}

/// One admitted OTLP span, normalized to Ravel's canonical shape.
///
/// Not yet a [`ravel_rspan::SpanRecord`]: that is the writer's type and is
/// built at flush time. This is the OTLP-independent shape the ingest router
/// and shard actor buffer, the span-pipeline counterpart of
/// [`crate::logs_normalize::NormalizedLogRecord`]. Every field maps one to one
/// onto `SpanRecord`, including the already-merged `attrs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// `None` for a root span, and also for a span whose `parent_span_id` was
    /// present but not 8 bytes (reported as
    /// [`SpanRejection::InvalidParentSpanId`] rather than fabricated).
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub status_code: StatusCode,
    pub status_message: Option<String>,
    /// Resource + scope + span attributes already merged and canonicalized by
    /// [`ravel_rspan::merge_attrs`], plus the reserved keys documented on this
    /// module.
    pub attrs: Vec<(String, String)>,
}

/// Result of normalizing one `ExportTraceServiceRequest`.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanNormalizeOutput {
    pub spans: Vec<NormalizedSpan>,
    pub rejected: Vec<SpanRejection>,
}

/// Decode and normalize spans from `req`.
///
/// `ingest_ts_ns` is the receiver's clock reading at admission time, used as
/// the last-resort start timestamp for a span that carries none (OTLP marks
/// both timestamps required, but a zero is what an under-instrumented sender
/// actually emits, and storing a bare zero would place the span at the Unix
/// epoch). Mirrors [`crate::logs_normalize::normalize_logs`]'s contract:
/// nothing here panics for malformed or oversized input, every problem becomes
/// a [`SpanRejection`].
pub fn normalize_traces(
    req: ExportTraceServiceRequest,
    limits: &SpanIngestLimits,
    ingest_ts_ns: i64,
) -> SpanNormalizeOutput {
    let total_spans: usize = req.resource_spans.iter().map(resource_span_count).sum();
    if total_spans > limits.max_spans_per_request {
        return SpanNormalizeOutput {
            spans: Vec::new(),
            rejected: vec![SpanRejection::TooManySpans {
                count: total_spans,
                max: limits.max_spans_per_request,
            }],
        };
    }

    let mut spans = Vec::new();
    let mut rejected = Vec::new();
    for rs in &req.resource_spans {
        normalize_resource(rs, limits, ingest_ts_ns, &mut spans, &mut rejected);
    }

    SpanNormalizeOutput { spans, rejected }
}

fn resource_span_count(rs: &ResourceSpans) -> usize {
    rs.scope_spans.iter().map(|ss| ss.spans.len()).sum()
}

fn normalize_resource(
    rs: &ResourceSpans,
    limits: &SpanIngestLimits,
    ingest_ts_ns: i64,
    spans: &mut Vec<NormalizedSpan>,
    rejected: &mut Vec<SpanRejection>,
) {
    let resource_span_count = resource_span_count(rs);
    if resource_span_count == 0 {
        return;
    }

    let resource_attributes = rs
        .resource
        .as_ref()
        .map(|r| r.attributes.as_slice())
        .unwrap_or(&[]);
    if resource_attributes.len() > limits.max_resource_attributes {
        rejected.push(SpanRejection::Grouped {
            reason: Box::new(SpanRejection::TooManyResourceAttributes {
                count: resource_attributes.len(),
                max: limits.max_resource_attributes,
            }),
            count: resource_span_count,
        });
        return;
    }

    // Converted once per resource and reported once, not once per span under
    // it: see the module docs on why an unconvertible resource attribute does
    // not reject the spans the way the logs path does.
    let resource_attrs = convert_attrs_lossy(resource_attributes, limits, rejected);

    for ss in &rs.scope_spans {
        normalize_scope(ss, &resource_attrs, limits, ingest_ts_ns, spans, rejected);
    }
}

fn normalize_scope(
    ss: &ScopeSpans,
    resource_attrs: &[(String, String)],
    limits: &SpanIngestLimits,
    ingest_ts_ns: i64,
    spans: &mut Vec<NormalizedSpan>,
    rejected: &mut Vec<SpanRejection>,
) {
    let scope_span_count = ss.spans.len();
    if scope_span_count == 0 {
        return;
    }

    let scope = ss.scope.as_ref();
    let scope_attributes = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
    if scope_attributes.len() > limits.max_scope_attributes {
        rejected.push(SpanRejection::Grouped {
            reason: Box::new(SpanRejection::TooManyScopeAttributes {
                count: scope_attributes.len(),
                max: limits.max_scope_attributes,
            }),
            count: scope_span_count,
        });
        return;
    }

    let mut scope_attrs = convert_attrs_lossy(scope_attributes, limits, rejected);
    // The instrumentation scope's own name and version are attributes here
    // rather than part of an identity blob (spans have no stream identity), so
    // they follow OTel's own `otel.scope.*` convention. Empty values are not
    // stored: an absent scope and an empty-named one are the same thing.
    if let Some(scope) = scope {
        if !scope.name.is_empty() {
            scope_attrs.push(("otel.scope.name".to_string(), scope.name.clone()));
        }
        if !scope.version.is_empty() {
            scope_attrs.push(("otel.scope.version".to_string(), scope.version.clone()));
        }
    }

    for span in &ss.spans {
        match normalize_span(span, resource_attrs, &scope_attrs, limits, ingest_ts_ns) {
            Ok((normalized, dropped)) => {
                spans.push(normalized);
                rejected.extend(dropped);
            }
            Err(reason) => rejected.push(reason),
        }
    }
}

/// Normalize one span. `Err` drops the whole span; `Ok`'s second element
/// carries rejections for attributes (or blobs) dropped from an otherwise
/// admitted span.
fn normalize_span(
    span: &Span,
    resource_attrs: &[(String, String)],
    scope_attrs: &[(String, String)],
    limits: &SpanIngestLimits,
    ingest_ts_ns: i64,
) -> Result<(NormalizedSpan, Vec<SpanRejection>), SpanRejection> {
    if span.attributes.len() > limits.max_attributes_per_span {
        return Err(SpanRejection::TooManyAttributes {
            count: span.attributes.len(),
            max: limits.max_attributes_per_span,
        });
    }
    if span.name.len() > limits.max_name_len {
        return Err(SpanRejection::NameTooLong {
            len: span.name.len(),
            max: limits.max_name_len,
        });
    }

    // trace_id and span_id are the record's identity and RSPAN's sort key; a
    // length mismatch cannot be padded or truncated without fabricating an id.
    let trace_id = <[u8; 16]>::try_from(span.trace_id.as_slice()).map_err(|_| {
        SpanRejection::InvalidTraceId {
            len: span.trace_id.len(),
        }
    })?;
    let span_id =
        <[u8; 8]>::try_from(span.span_id.as_slice()).map_err(|_| SpanRejection::InvalidSpanId {
            len: span.span_id.len(),
        })?;

    let mut dropped = Vec::new();
    let parent_span_id = if span.parent_span_id.is_empty() {
        None
    } else {
        match <[u8; 8]>::try_from(span.parent_span_id.as_slice()) {
            Ok(id) => Some(id),
            Err(_) => {
                dropped.push(SpanRejection::InvalidParentSpanId {
                    len: span.parent_span_id.len(),
                });
                None
            }
        }
    };

    let start_ts_ns = match to_i64_ns(span.start_time_unix_nano) {
        0 => ingest_ts_ns,
        v => v,
    };
    // An end with no start of its own belongs at the start: a zero would put
    // the span's interval at the epoch and make every window overlap it.
    let end_ts_ns = match to_i64_ns(span.end_time_unix_nano) {
        0 => start_ts_ns,
        v => v,
    };
    if end_ts_ns < start_ts_ns {
        return Err(SpanRejection::InvalidTimeRange {
            start_ts_ns,
            end_ts_ns,
        });
    }

    let (status_code, status_message) = match span.status.as_ref() {
        None => (StatusCode::Unset, None),
        Some(status) => {
            if status.message.len() > limits.max_status_message_len {
                return Err(SpanRejection::StatusMessageTooLong {
                    len: status.message.len(),
                    max: limits.max_status_message_len,
                });
            }
            let message = if status.message.is_empty() {
                None
            } else {
                Some(status.message.clone())
            };
            (status_code_from_i32(status.code), message)
        }
    };

    let span_attrs = convert_attrs_lossy(&span.attributes, limits, &mut dropped);
    let mut merged = merge_attrs(resource_attrs, scope_attrs, &span_attrs);
    // A sender's own attribute under one of the reserved keys must never
    // survive, whether or not the span's real field is present: leaving it
    // when the real field is absent lets a sender spoof `_events_raw` etc, and
    // RSPAN objects are immutable, so the provenance is unrecoverable once
    // stored. Strip unconditionally before applying the real reserved values.
    merged.retain(|(k, _)| !is_reserved_key(k));
    let reserved = reserved_attrs(span, limits, &mut dropped);
    // Reserved keys are applied as the highest-precedence set, so a sender's
    // own `_kind` cannot shadow the span's real kind.
    let attrs = if reserved.is_empty() {
        merged
    } else {
        merge_attrs(&reserved, &[], &merged)
    };

    Ok((
        NormalizedSpan {
            trace_id,
            span_id,
            parent_span_id,
            name: span.name.clone(),
            start_ts_ns,
            end_ts_ns,
            status_code,
            status_message,
            attrs,
        },
        dropped,
    ))
}

/// The attributes carrying span fields RSPAN v1 has no column for (see the
/// module docs). Only non-default values are stored, so an ordinary span pays
/// nothing for fields it never set.
fn reserved_attrs(
    span: &Span,
    limits: &SpanIngestLimits,
    dropped: &mut Vec<SpanRejection>,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(kind) = span_kind_name(span.kind) {
        out.push((ATTR_SPAN_KIND.to_string(), kind));
    }
    if !span.trace_state.is_empty() {
        out.push((ATTR_TRACE_STATE.to_string(), span.trace_state.clone()));
    }
    if span.flags != 0 {
        out.push((ATTR_SPAN_FLAGS.to_string(), span.flags.to_string()));
    }
    if !span.events.is_empty() {
        match encode_blob(&span.events, limits.max_raw_blob_len) {
            Ok(hex) => out.push((ATTR_EVENTS_RAW.to_string(), hex)),
            Err(len) => dropped.push(SpanRejection::EventsBlobTooLong {
                len,
                max: limits.max_raw_blob_len,
            }),
        }
    }
    if !span.links.is_empty() {
        match encode_blob(&span.links, limits.max_raw_blob_len) {
            Ok(hex) => out.push((ATTR_LINKS_RAW.to_string(), hex)),
            Err(len) => dropped.push(SpanRejection::LinksBlobTooLong {
                len,
                max: limits.max_raw_blob_len,
            }),
        }
    }
    out
}

/// Serialize `items` as concatenated length-delimited protobuf messages and
/// hex-encode the result. `Err(len)` carries the serialized length when it
/// exceeds `max_len`, so the caller can report how far over the blob was. The
/// length is checked on the serialized bytes, before hex doubles them.
fn encode_blob<T: Message>(items: &[T], max_len: usize) -> Result<String, usize> {
    let mut raw = Vec::new();
    for item in items {
        // `encode_length_delimited` only fails when the target buffer cannot
        // grow, which a Vec does not; the error is still handled rather than
        // unwrapped, and treats an over-long blob as the failure it would be.
        if item.encode_length_delimited(&mut raw).is_err() {
            return Err(raw.len());
        }
    }
    if raw.len() > max_len {
        return Err(raw.len());
    }
    Ok(hex::encode(&raw))
}

/// Canonical stored name for an OTLP `SpanKind`, or `None` for
/// `SPAN_KIND_UNSPECIFIED` (which carries no information and is not stored).
/// A value outside the enum is stored as its decimal form rather than mapped
/// to a kind the sender never sent.
pub fn span_kind_name(kind: i32) -> Option<String> {
    Some(match kind {
        0 => return None,
        1 => "internal".to_string(),
        2 => "server".to_string(),
        3 => "client".to_string(),
        4 => "producer".to_string(),
        5 => "consumer".to_string(),
        other => other.to_string(),
    })
}

/// Map an OTLP status code to RSPAN's. A value outside `0..=2` is a protocol
/// violation with no meaningful status; it normalizes to `Unset` rather than
/// being coerced into an unrelated status the sender never sent (the same
/// choice [`crate::logs_normalize`] makes for an out-of-range severity).
fn status_code_from_i32(code: i32) -> StatusCode {
    match code {
        1 => StatusCode::Ok,
        2 => StatusCode::Error,
        _ => StatusCode::Unset,
    }
}

/// OTLP timestamps are `u64` nanoseconds; real ones fit comfortably in `i64`,
/// and a value that does not is far outside any admissible window, so it
/// saturates rather than wrapping negative.
fn to_i64_ns(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Convert an attribute set, dropping and reporting the ones that cannot be
/// represented rather than failing the whole set. Used for resource, scope,
/// and span attributes alike: unlike logs, no attribute set here feeds an
/// identity, so a single bad attribute never has to reject its neighbours.
fn convert_attrs_lossy(
    attributes: &[KeyValue],
    limits: &SpanIngestLimits,
    rejected: &mut Vec<SpanRejection>,
) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(attributes.len());
    for kv in attributes {
        match convert_attr(kv, limits) {
            Ok(attr) => out.push(attr),
            Err(reason) => rejected.push(reason),
        }
    }
    out
}

fn convert_attr(
    kv: &KeyValue,
    limits: &SpanIngestLimits,
) -> Result<(String, String), SpanRejection> {
    if kv.key.len() > limits.max_attribute_key_len {
        return Err(SpanRejection::AttributeKeyTooLong {
            len: kv.key.len(),
            max: limits.max_attribute_key_len,
        });
    }
    let value = convert_value(&kv.key, kv.value.as_ref())?;
    if value.len() > limits.max_attribute_value_len {
        return Err(SpanRejection::AttributeValueTooLong {
            len: value.len(),
            max: limits.max_attribute_value_len,
        });
    }
    Ok((kv.key.clone(), value))
}

/// Map one OTLP `AnyValue` to the `Utf8` string RSPAN stores. Scalars take
/// their canonical string form and a bytes value becomes lowercase hex, the
/// same mapping [`crate::logs_normalize`] uses for a scalar log body. Arrays
/// and kvlists are rejected: RSPAN v1's `attrs` is `Map<Utf8, Utf8>` with no
/// nested representation, and picking a stringification for them here would
/// make a frozen-format decision by accident.
fn convert_value(key: &str, value: Option<&AnyValue>) -> Result<String, SpanRejection> {
    match value.and_then(|v| v.value.as_ref()) {
        None => Err(SpanRejection::MissingAttributeValue {
            key: key.to_string(),
        }),
        Some(AnyValueVariant::StringValue(s)) => Ok(s.clone()),
        Some(AnyValueVariant::BoolValue(b)) => Ok(b.to_string()),
        Some(AnyValueVariant::IntValue(i)) => Ok(i.to_string()),
        Some(AnyValueVariant::DoubleValue(d)) => Ok(format_float(*d)),
        Some(AnyValueVariant::BytesValue(b)) => Ok(hex::encode(b)),
        Some(AnyValueVariant::ArrayValue(_)) | Some(AnyValueVariant::KvlistValue(_)) => {
            Err(SpanRejection::UnsupportedAttributeKind {
                key: key.to_string(),
            })
        }
        Some(AnyValueVariant::StringValueStrindex(_)) => {
            Err(SpanRejection::UnsupportedAttributeValue {
                key: key.to_string(),
            })
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, InstrumentationScope, KeyValueList};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::Status;
    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};

    const TRACE: [u8; 16] = [7u8; 16];
    const SPAN: [u8; 8] = [3u8; 8];

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

    fn span(name: &str, start: u64, end: u64, attrs: Vec<KeyValue>) -> Span {
        Span {
            trace_id: TRACE.to_vec(),
            span_id: SPAN.to_vec(),
            name: name.to_string(),
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            attributes: attrs,
            ..Default::default()
        }
    }

    fn scope_spans(name: &str, version: &str, spans: Vec<Span>) -> ScopeSpans {
        ScopeSpans {
            scope: Some(InstrumentationScope {
                name: name.to_string(),
                version: version.to_string(),
                ..Default::default()
            }),
            spans,
            ..Default::default()
        }
    }

    fn resource_spans(resource_attrs: Vec<KeyValue>, scopes: Vec<ScopeSpans>) -> ResourceSpans {
        ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attrs,
                ..Default::default()
            }),
            scope_spans: scopes,
            ..Default::default()
        }
    }

    fn request(resources: Vec<ResourceSpans>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: resources,
        }
    }

    fn normalize(req: ExportTraceServiceRequest) -> SpanNormalizeOutput {
        normalize_traces(req, &SpanIngestLimits::default(), 5_000)
    }

    fn lookup<'a>(attrs: &'a [(String, String)], key: &str) -> Option<&'a str> {
        attrs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn empty_request_yields_empty_output() {
        let out = normalize(request(vec![]));
        assert!(out.spans.is_empty());
        assert!(out.rejected.is_empty());
    }

    #[test]
    fn span_normalizes_field_for_field() {
        let mut s = span(
            "GET /checkout",
            1_700,
            2_700,
            vec![
                string_kv("http.method", "GET"),
                kv("http.status", AnyValueVariant::IntValue(200)),
            ],
        );
        s.parent_span_id = vec![9u8; 8];
        s.kind = 2;
        s.status = Some(Status {
            code: 2,
            message: "boom".to_string(),
        });
        let out = normalize(request(vec![resource_spans(
            vec![string_kv("service.name", "checkout")],
            vec![scope_spans("lib", "1.0", vec![s])],
        )]));

        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        assert_eq!(out.spans.len(), 1);
        let sp = &out.spans[0];
        assert_eq!(sp.trace_id, TRACE);
        assert_eq!(sp.span_id, SPAN);
        assert_eq!(sp.parent_span_id, Some([9u8; 8]));
        assert_eq!(sp.name, "GET /checkout");
        assert_eq!(sp.start_ts_ns, 1_700);
        assert_eq!(sp.end_ts_ns, 2_700);
        assert_eq!(sp.status_code, StatusCode::Error);
        assert_eq!(sp.status_message.as_deref(), Some("boom"));
        assert_eq!(lookup(&sp.attrs, "http.method"), Some("GET"));
        assert_eq!(lookup(&sp.attrs, "http.status"), Some("200"));
        assert_eq!(lookup(&sp.attrs, "service.name"), Some("checkout"));
        assert_eq!(lookup(&sp.attrs, "otel.scope.name"), Some("lib"));
        assert_eq!(lookup(&sp.attrs, "otel.scope.version"), Some("1.0"));
        assert_eq!(lookup(&sp.attrs, ATTR_SPAN_KIND), Some("server"));
        // Sorted ascending by key, as merge_attrs canonicalizes.
        let keys: Vec<&str> = sp.attrs.iter().map(|(k, _)| k.as_str()).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn resource_and_scope_attributes_win_over_span_attributes() {
        let s = span("op", 1, 2, vec![string_kv("k", "span")]);
        let mut scope = scope_spans("lib", "1.0", vec![s]);
        if let Some(sc) = scope.scope.as_mut() {
            sc.attributes = vec![string_kv("k", "scope"), string_kv("only.scope", "s")];
        }
        let out = normalize(request(vec![resource_spans(
            vec![string_kv("k", "resource")],
            vec![scope],
        )]));
        let attrs = &out.spans[0].attrs;
        assert_eq!(lookup(attrs, "k"), Some("resource"));
        assert_eq!(lookup(attrs, "only.scope"), Some("s"));
    }

    #[test]
    fn root_span_has_no_parent_and_a_bad_parent_is_reported_not_fabricated() {
        let root = span("root", 1, 2, vec![]);
        let mut bad_parent = span("child", 1, 2, vec![]);
        bad_parent.parent_span_id = vec![1, 2, 3];
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans("lib", "1", vec![root, bad_parent])],
        )]));
        assert_eq!(out.spans.len(), 2, "neither span is dropped");
        assert_eq!(out.spans[0].parent_span_id, None);
        assert_eq!(out.spans[1].parent_span_id, None);
        assert_eq!(
            out.rejected,
            vec![SpanRejection::InvalidParentSpanId { len: 3 }]
        );
    }

    #[test]
    fn malformed_trace_id_or_span_id_rejects_the_span() {
        let mut bad_trace = span("a", 1, 2, vec![]);
        bad_trace.trace_id = vec![1, 2, 3];
        let mut bad_span = span("b", 1, 2, vec![]);
        bad_span.span_id = vec![1; 7];
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans("lib", "1", vec![bad_trace, bad_span])],
        )]));
        assert!(out.spans.is_empty());
        assert_eq!(
            out.rejected,
            vec![
                SpanRejection::InvalidTraceId { len: 3 },
                SpanRejection::InvalidSpanId { len: 7 },
            ]
        );
    }

    #[test]
    fn missing_timestamps_fall_back_to_ingest_time_and_a_zero_end_to_the_start() {
        let no_times = span("a", 0, 0, vec![]);
        let no_end = span("b", 900, 0, vec![]);
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans("lib", "1", vec![no_times, no_end])],
        )]));
        assert_eq!(out.spans[0].start_ts_ns, 5_000);
        assert_eq!(out.spans[0].end_ts_ns, 5_000);
        assert_eq!(out.spans[1].start_ts_ns, 900);
        assert_eq!(out.spans[1].end_ts_ns, 900);
    }

    #[test]
    fn end_before_start_rejects_the_span() {
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans(
                "lib",
                "1",
                vec![
                    span("inverted", 2_000, 1_000, vec![]),
                    span("ok", 1, 2, vec![]),
                ],
            )],
        )]));
        assert_eq!(out.spans.len(), 1);
        assert_eq!(out.spans[0].name, "ok");
        assert_eq!(
            out.rejected,
            vec![SpanRejection::InvalidTimeRange {
                start_ts_ns: 2_000,
                end_ts_ns: 1_000,
            }]
        );
    }

    #[test]
    fn status_codes_map_and_an_out_of_range_code_is_unset() {
        let mut ok = span("ok", 1, 2, vec![]);
        ok.status = Some(Status {
            code: 1,
            message: String::new(),
        });
        let mut weird = span("weird", 1, 2, vec![]);
        weird.status = Some(Status {
            code: 99,
            message: String::new(),
        });
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans(
                "lib",
                "1",
                vec![ok, weird, span("none", 1, 2, vec![])],
            )],
        )]));
        assert_eq!(out.spans[0].status_code, StatusCode::Ok);
        assert_eq!(out.spans[0].status_message, None);
        assert_eq!(out.spans[1].status_code, StatusCode::Unset);
        assert_eq!(out.spans[2].status_code, StatusCode::Unset);
    }

    #[test]
    fn events_and_links_round_trip_through_the_opaque_blob() {
        let mut s = span("op", 1, 2, vec![]);
        s.events = vec![Event {
            time_unix_nano: 1_500,
            name: "exception".to_string(),
            attributes: vec![string_kv("exception.type", "IoError")],
            dropped_attributes_count: 0,
        }];
        s.links = vec![Link {
            trace_id: vec![1u8; 16],
            span_id: vec![2u8; 8],
            ..Default::default()
        }];
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans("lib", "1", vec![s])],
        )]));
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);

        let attrs = &out.spans[0].attrs;
        let events_hex = lookup(attrs, ATTR_EVENTS_RAW).expect("events blob stored");
        let raw = hex::decode(events_hex).expect("blob is hex");
        let decoded = Event::decode_length_delimited(raw.as_slice()).expect("decodes as an Event");
        assert_eq!(decoded.name, "exception");
        assert_eq!(decoded.time_unix_nano, 1_500);

        let links_hex = lookup(attrs, ATTR_LINKS_RAW).expect("links blob stored");
        let raw = hex::decode(links_hex).expect("blob is hex");
        let decoded = Link::decode_length_delimited(raw.as_slice()).expect("decodes as a Link");
        assert_eq!(decoded.trace_id, vec![1u8; 16]);
    }

    #[test]
    fn oversized_events_blob_is_reported_and_the_span_still_lands() {
        let limits = SpanIngestLimits {
            max_raw_blob_len: 16,
            ..SpanIngestLimits::default()
        };
        let mut s = span("op", 1, 2, vec![]);
        s.events = vec![Event {
            name: "x".repeat(64),
            ..Default::default()
        }];
        let out = normalize_traces(
            request(vec![resource_spans(
                vec![],
                vec![scope_spans("lib", "1", vec![s])],
            )]),
            &limits,
            5_000,
        );
        assert_eq!(out.spans.len(), 1, "the span itself is still admitted");
        assert_eq!(lookup(&out.spans[0].attrs, ATTR_EVENTS_RAW), None);
        assert!(
            matches!(
                out.rejected.as_slice(),
                [SpanRejection::EventsBlobTooLong { max: 16, .. }]
            ),
            "{:?}",
            out.rejected
        );
    }

    #[test]
    fn reserved_keys_win_over_a_sender_attribute_of_the_same_name() {
        let mut s = span("op", 1, 2, vec![string_kv(ATTR_SPAN_KIND, "spoofed")]);
        s.kind = 3;
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans("lib", "1", vec![s])],
        )]));
        assert_eq!(lookup(&out.spans[0].attrs, ATTR_SPAN_KIND), Some("client"));
    }

    #[test]
    fn unspecified_kind_and_empty_trace_state_store_nothing() {
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans("lib", "1", vec![span("op", 1, 2, vec![])])],
        )]));
        let attrs = &out.spans[0].attrs;
        assert_eq!(lookup(attrs, ATTR_SPAN_KIND), None);
        assert_eq!(lookup(attrs, ATTR_TRACE_STATE), None);
        assert_eq!(lookup(attrs, ATTR_SPAN_FLAGS), None);
        assert_eq!(lookup(attrs, ATTR_EVENTS_RAW), None);
        assert_eq!(lookup(attrs, ATTR_LINKS_RAW), None);
    }

    /// A sender's own attribute under a reserved key must be stripped even
    /// when the span carries no real value for that field - not just
    /// shadowed by a real value, which `reserved_keys_win_over_a_sender_
    /// attribute_of_the_same_name` already covers with `kind = 3`. A span
    /// with `kind = 0` (unspecified) sets no `_kind` of its own, so without
    /// this a sender-supplied `_kind`/`_events_raw`/etc would land verbatim -
    /// silently spoofing metadata a future reader trusts as real, in an
    /// object that's immutable once written.
    #[test]
    fn sender_attribute_under_a_reserved_key_is_stripped_even_when_the_real_field_is_absent() {
        let mut s = span(
            "op",
            1,
            2,
            vec![
                string_kv(ATTR_SPAN_KIND, "spoofed-kind"),
                string_kv(ATTR_TRACE_STATE, "spoofed-state"),
                string_kv(ATTR_SPAN_FLAGS, "999"),
                string_kv(ATTR_EVENTS_RAW, "deadbeef"),
                string_kv(ATTR_LINKS_RAW, "cafebabe"),
            ],
        );
        s.kind = 0; // unspecified: reserved_attrs() produces no _kind of its own
        s.trace_state = String::new();
        s.flags = 0;
        s.events = vec![];
        s.links = vec![];
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans("lib", "1", vec![s])],
        )]));
        let attrs = &out.spans[0].attrs;
        assert_eq!(lookup(attrs, ATTR_SPAN_KIND), None);
        assert_eq!(lookup(attrs, ATTR_TRACE_STATE), None);
        assert_eq!(lookup(attrs, ATTR_SPAN_FLAGS), None);
        assert_eq!(lookup(attrs, ATTR_EVENTS_RAW), None);
        assert_eq!(lookup(attrs, ATTR_LINKS_RAW), None);
    }

    #[test]
    fn scalar_attribute_values_take_their_canonical_string_form() {
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans(
                "lib",
                "1",
                vec![span(
                    "op",
                    1,
                    2,
                    vec![
                        kv("b", AnyValueVariant::BoolValue(true)),
                        kv("i", AnyValueVariant::IntValue(-3)),
                        kv("f", AnyValueVariant::DoubleValue(1.5)),
                        kv("inf", AnyValueVariant::DoubleValue(f64::INFINITY)),
                        kv("bytes", AnyValueVariant::BytesValue(vec![0xde, 0xad])),
                    ],
                )],
            )],
        )]));
        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
        let attrs = &out.spans[0].attrs;
        assert_eq!(lookup(attrs, "b"), Some("true"));
        assert_eq!(lookup(attrs, "i"), Some("-3"));
        assert_eq!(lookup(attrs, "f"), Some(format_float(1.5).as_str()));
        assert_eq!(lookup(attrs, "inf"), Some("+Inf"));
        assert_eq!(lookup(attrs, "bytes"), Some("dead"));
    }

    #[test]
    fn nested_attribute_values_are_dropped_and_reported_not_stringified() {
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans(
                "lib",
                "1",
                vec![span(
                    "op",
                    1,
                    2,
                    vec![
                        kv(
                            "list",
                            AnyValueVariant::ArrayValue(ArrayValue {
                                values: vec![any(AnyValueVariant::IntValue(1))],
                            }),
                        ),
                        kv(
                            "map",
                            AnyValueVariant::KvlistValue(KeyValueList {
                                values: vec![kv("inner", AnyValueVariant::DoubleValue(2.5))],
                            }),
                        ),
                        string_kv("kept", "v"),
                    ],
                )],
            )],
        )]));
        assert_eq!(out.spans.len(), 1);
        assert_eq!(lookup(&out.spans[0].attrs, "kept"), Some("v"));
        assert_eq!(
            out.rejected,
            vec![
                SpanRejection::UnsupportedAttributeKind { key: "list".into() },
                SpanRejection::UnsupportedAttributeKind { key: "map".into() },
            ]
        );
    }

    #[test]
    fn an_unconvertible_resource_attribute_drops_only_itself() {
        // The deliberate departure from logs_normalize: resource attributes
        // are data here, not identity, so an array-valued one (an OTel SDK
        // emits `process.command_args` that way) costs one attribute, not
        // every span from that process.
        let out = normalize(request(vec![resource_spans(
            vec![
                kv(
                    "process.command_args",
                    AnyValueVariant::ArrayValue(ArrayValue {
                        values: vec![any(AnyValueVariant::StringValue("--serve".into()))],
                    }),
                ),
                string_kv("service.name", "checkout"),
            ],
            vec![scope_spans(
                "lib",
                "1",
                vec![span("a", 1, 2, vec![]), span("b", 1, 2, vec![])],
            )],
        )]));
        assert_eq!(out.spans.len(), 2, "both spans survive");
        assert_eq!(
            lookup(&out.spans[0].attrs, "service.name"),
            Some("checkout")
        );
        assert_eq!(
            out.rejected,
            vec![SpanRejection::UnsupportedAttributeKind {
                key: "process.command_args".into()
            }],
            "reported once for the resource, not once per span"
        );
    }

    #[test]
    fn oversized_attribute_value_drops_the_attribute_not_the_span() {
        let limits = SpanIngestLimits::default();
        let big = "x".repeat(limits.max_attribute_value_len + 1);
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans(
                "lib",
                "1",
                vec![span(
                    "op",
                    1,
                    2,
                    vec![string_kv("small", "v"), string_kv("big", &big)],
                )],
            )],
        )]));
        assert_eq!(out.spans.len(), 1);
        assert_eq!(lookup(&out.spans[0].attrs, "small"), Some("v"));
        assert_eq!(lookup(&out.spans[0].attrs, "big"), None);
        assert_eq!(
            out.rejected,
            vec![SpanRejection::AttributeValueTooLong {
                len: limits.max_attribute_value_len + 1,
                max: limits.max_attribute_value_len,
            }]
        );
    }

    #[test]
    fn oversized_attribute_key_drops_the_attribute() {
        let limits = SpanIngestLimits::default();
        let key = "k".repeat(limits.max_attribute_key_len + 1);
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans(
                "lib",
                "1",
                vec![span("op", 1, 2, vec![string_kv(&key, "v")])],
            )],
        )]));
        assert_eq!(out.spans.len(), 1);
        assert_eq!(
            out.rejected,
            vec![SpanRejection::AttributeKeyTooLong {
                len: limits.max_attribute_key_len + 1,
                max: limits.max_attribute_key_len,
            }]
        );
    }

    #[test]
    fn attribute_without_a_value_is_reported_and_the_span_kept() {
        let out = normalize(request(vec![resource_spans(
            vec![],
            vec![scope_spans(
                "lib",
                "1",
                vec![span(
                    "op",
                    1,
                    2,
                    vec![KeyValue {
                        key: "novalue".to_string(),
                        value: None,
                        ..Default::default()
                    }],
                )],
            )],
        )]));
        assert_eq!(out.spans.len(), 1);
        assert_eq!(
            out.rejected,
            vec![SpanRejection::MissingAttributeValue {
                key: "novalue".to_string()
            }]
        );
    }

    #[test]
    fn too_many_spans_short_circuits_the_whole_request() {
        let limits = SpanIngestLimits {
            max_spans_per_request: 2,
            ..SpanIngestLimits::default()
        };
        let spans = (0..3).map(|i| span("op", i + 1, i + 2, vec![])).collect();
        let out = normalize_traces(
            request(vec![resource_spans(
                vec![],
                vec![scope_spans("lib", "1", spans)],
            )]),
            &limits,
            5_000,
        );
        assert!(out.spans.is_empty());
        assert_eq!(
            out.rejected,
            vec![SpanRejection::TooManySpans { count: 3, max: 2 }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 3);
    }

    #[test]
    fn too_many_span_attributes_rejects_the_span() {
        let limits = SpanIngestLimits {
            max_attributes_per_span: 1,
            ..SpanIngestLimits::default()
        };
        let out = normalize_traces(
            request(vec![resource_spans(
                vec![],
                vec![scope_spans(
                    "lib",
                    "1",
                    vec![span(
                        "op",
                        1,
                        2,
                        vec![string_kv("a", "1"), string_kv("b", "2")],
                    )],
                )],
            )]),
            &limits,
            5_000,
        );
        assert!(out.spans.is_empty());
        assert_eq!(
            out.rejected,
            vec![SpanRejection::TooManyAttributes { count: 2, max: 1 }]
        );
    }

    #[test]
    fn oversized_name_rejects_the_span() {
        let limits = SpanIngestLimits {
            max_name_len: 4,
            ..SpanIngestLimits::default()
        };
        let out = normalize_traces(
            request(vec![resource_spans(
                vec![],
                vec![scope_spans("lib", "1", vec![span("toolong", 1, 2, vec![])])],
            )]),
            &limits,
            5_000,
        );
        assert!(out.spans.is_empty());
        assert_eq!(
            out.rejected,
            vec![SpanRejection::NameTooLong { len: 7, max: 4 }]
        );
    }

    #[test]
    fn oversized_status_message_rejects_the_span() {
        let limits = SpanIngestLimits {
            max_status_message_len: 4,
            ..SpanIngestLimits::default()
        };
        let mut s = span("op", 1, 2, vec![]);
        s.status = Some(Status {
            code: 2,
            message: "toolong".to_string(),
        });
        let out = normalize_traces(
            request(vec![resource_spans(
                vec![],
                vec![scope_spans("lib", "1", vec![s])],
            )]),
            &limits,
            5_000,
        );
        assert!(out.spans.is_empty());
        assert_eq!(
            out.rejected,
            vec![SpanRejection::StatusMessageTooLong { len: 7, max: 4 }]
        );
    }

    #[test]
    fn oversized_resource_attributes_reject_every_span_under_the_resource() {
        let limits = SpanIngestLimits {
            max_resource_attributes: 1,
            ..SpanIngestLimits::default()
        };
        let spans: Vec<Span> = (0..4).map(|i| span("op", i + 1, i + 2, vec![])).collect();
        let out = normalize_traces(
            request(vec![resource_spans(
                vec![string_kv("a", "1"), string_kv("b", "2")],
                vec![scope_spans("lib", "1", spans)],
            )]),
            &limits,
            5_000,
        );
        assert!(out.spans.is_empty());
        assert_eq!(
            out.rejected,
            vec![SpanRejection::Grouped {
                reason: Box::new(SpanRejection::TooManyResourceAttributes { count: 2, max: 1 }),
                count: 4,
            }]
        );
        assert_eq!(out.rejected[0].rejected_count(), 4);
    }

    #[test]
    fn oversized_scope_attributes_reject_only_that_scope() {
        let limits = SpanIngestLimits {
            max_scope_attributes: 0,
            ..SpanIngestLimits::default()
        };
        let mut bad_scope = scope_spans(
            "lib-b",
            "1",
            vec![span("a", 1, 2, vec![]), span("b", 1, 2, vec![])],
        );
        if let Some(scope) = bad_scope.scope.as_mut() {
            scope.attributes = vec![string_kv("k", "v")];
        }
        let out = normalize_traces(
            request(vec![resource_spans(
                vec![],
                vec![
                    scope_spans("lib-a", "1", vec![span("c", 1, 2, vec![])]),
                    bad_scope,
                ],
            )]),
            &limits,
            5_000,
        );
        assert_eq!(out.spans.len(), 1);
        assert_eq!(
            out.rejected,
            vec![SpanRejection::Grouped {
                reason: Box::new(SpanRejection::TooManyScopeAttributes { count: 1, max: 0 }),
                count: 2,
            }]
        );
    }

    #[test]
    fn span_kind_names_cover_the_enum_and_fall_back_to_the_number() {
        assert_eq!(span_kind_name(0), None);
        assert_eq!(span_kind_name(1).as_deref(), Some("internal"));
        assert_eq!(span_kind_name(2).as_deref(), Some("server"));
        assert_eq!(span_kind_name(3).as_deref(), Some("client"));
        assert_eq!(span_kind_name(4).as_deref(), Some("producer"));
        assert_eq!(span_kind_name(5).as_deref(), Some("consumer"));
        assert_eq!(span_kind_name(42).as_deref(), Some("42"));
    }
}
