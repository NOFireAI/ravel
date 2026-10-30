//! The logs normalizer as it was before it moved values out of the request
//! and folded the projection into its pass (ADR-3492 D3a, issue #3496), kept
//! as the oracle `normalize_output_is_unchanged` compares the live one to.
//! It clones every string it keeps and walks the request once to project and
//! once to normalize. The helpers that change did not (canonical JSON, the
//! timestamp window, IPCrypt, the storage-shape checks) are shared with the
//! live code rather than copied.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use ravel_ipcrypt::rules::TenantRules;
use ravel_ipcrypt::tally::IpcryptTally;
use ravel_logseg::{MAX_ATTR_ENTRIES, attr_value_fits_storage, stream_attrs_bytes};
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};

use super::{
    BODY_KEY, LogNormalizeOutput, MAX_ATTRIBUTE_NESTING_DEPTH, NormalizedBody,
    NormalizedLogRecord, apply_ipcrypt_attr, attr_value_len, canonical_json, checked_record_ts,
    has_container_past_entry_cap, to_i64_ns, too_deeply_nested,
};
use crate::label_projection::{
    encoded_attrs_len, encoded_bytes_len, exact_capacity, log_attrs_bytes,
};
use crate::logs_limits::{LogIngestLimits, LogRejection};
use crate::promcompat::format_float;

pub(super) fn project(
    req: &ExportLogsServiceRequest,
    limits: &LogIngestLimits,
    rules: &TenantRules,
) -> usize {
    let mut total = 0usize;
    for rl in &req.resource_logs {
        let resource_attributes = rl
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        if resource_attributes.len() > limits.max_resource_attributes {
            continue;
        }
        total = total.saturating_add(log_attrs_bytes(resource_attributes, rules));
        let resource_encoded = encoded_attrs_len(resource_attributes, rules);
        for sl in &rl.scope_logs {
            let scope = sl.scope.as_ref();
            let scope_attributes = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
            if scope_attributes.len() > limits.max_scope_attributes {
                continue;
            }
            let scope_name = scope.map_or(0, |s| s.name.len());
            let scope_version = scope.map_or(0, |s| s.version.len());
            let stream_encoded = resource_encoded
                .saturating_add(encoded_bytes_len(scope_name))
                .saturating_add(encoded_bytes_len(scope_version))
                .saturating_add(encoded_attrs_len(scope_attributes, rules));
            total = total
                .saturating_add(log_attrs_bytes(scope_attributes, rules))
                .saturating_add(stream_encoded);
            for record in &sl.log_records {
                if record.attributes.len() > limits.max_attributes_per_record {
                    continue;
                }
                total = total
                    .saturating_add(stream_encoded)
                    .saturating_add(log_attrs_bytes(&record.attributes, rules));
            }
        }
    }
    total
}

pub(super) fn normalize_logs(
    req: ExportLogsServiceRequest,
    limits: &LogIngestLimits,
    rules: &TenantRules,
    ingest_ts_ns: i64,
) -> LogNormalizeOutput {
    let projected = project(&req, limits, rules);
    normalize_logs_projected(req, limits, rules, ingest_ts_ns, projected)
}

pub(super) fn normalize_logs_projected(
    req: ExportLogsServiceRequest,
    limits: &LogIngestLimits,
    rules: &TenantRules,
    ingest_ts_ns: i64,
    projected: usize,
) -> LogNormalizeOutput {
    let total_records: usize = req.resource_logs.iter().map(resource_record_count).sum();
    if total_records > limits.max_records_per_request {
        return LogNormalizeOutput {
            records: Vec::new(),
            rejected: vec![LogRejection::TooManyRecords {
                count: total_records,
                max: limits.max_records_per_request,
            }],
            body_conversions: 0,
            ipcrypt_tally: IpcryptTally::new(),
        };
    }
    if projected > limits.max_resolved_label_bytes_per_request {
        return LogNormalizeOutput {
            records: Vec::new(),
            rejected: vec![LogRejection::ResolvedLabelBytesExceeded {
                projected,
                max: limits.max_resolved_label_bytes_per_request,
                count: total_records,
            }],
            body_conversions: 0,
            ipcrypt_tally: IpcryptTally::new(),
        };
    }

    let mut records = Vec::new();
    let mut rejected = Vec::new();
    let mut body_conversions = 0;
    let mut tally = IpcryptTally::new();
    for rl in &req.resource_logs {
        normalize_resource(
            rl,
            limits,
            rules,
            ingest_ts_ns,
            &mut records,
            &mut rejected,
            &mut body_conversions,
            &mut tally,
        );
    }
    LogNormalizeOutput {
        records,
        rejected,
        body_conversions,
        ipcrypt_tally: tally,
    }
}

fn resource_record_count(rl: &ResourceLogs) -> usize {
    rl.scope_logs.iter().map(|sl| sl.log_records.len()).sum()
}

#[allow(clippy::too_many_arguments)]
fn normalize_resource(
    rl: &ResourceLogs,
    limits: &LogIngestLimits,
    rules: &TenantRules,
    ingest_ts_ns: i64,
    records: &mut Vec<NormalizedLogRecord>,
    rejected: &mut Vec<LogRejection>,
    body_conversions: &mut usize,
    tally: &mut IpcryptTally,
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
    let mut resource_tally = IpcryptTally::new();
    let resource_attrs =
        match convert_attrs(resource_attributes, limits, rules, &mut resource_tally) {
            Ok(attrs) => attrs,
            Err(reason) => {
                rejected.push(LogRejection::Grouped {
                    reason: Box::new(reason),
                    count: resource_record_count,
                });
                return;
            }
        };
    let stored_before = records.len();
    for sl in &rl.scope_logs {
        normalize_scope(
            sl,
            &resource_attrs,
            limits,
            rules,
            ingest_ts_ns,
            records,
            rejected,
            body_conversions,
            tally,
        );
    }
    if records.len() > stored_before {
        tally.merge(&resource_tally);
    }
}

#[allow(clippy::too_many_arguments)]
fn normalize_scope(
    sl: &ScopeLogs,
    resource_attrs: &[(String, AttrValue)],
    limits: &LogIngestLimits,
    rules: &TenantRules,
    ingest_ts_ns: i64,
    records: &mut Vec<NormalizedLogRecord>,
    rejected: &mut Vec<LogRejection>,
    body_conversions: &mut usize,
    tally: &mut IpcryptTally,
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
    let mut scope_tally = IpcryptTally::new();
    let scope_attrs = match convert_attrs(scope_attributes, limits, rules, &mut scope_tally) {
        Ok(attrs) => attrs,
        Err(reason) => {
            rejected.push(LogRejection::Grouped {
                reason: Box::new(reason),
                count: scope_record_count,
            });
            return;
        }
    };
    let stream_id = log_stream_id(resource_attrs, scope_name, scope_version, &scope_attrs);
    let stream_attrs = stream_attrs_bytes(resource_attrs, scope_name, scope_version, &scope_attrs);
    let stored_before = records.len();
    for record in &sl.log_records {
        match normalize_record(
            record,
            stream_id,
            &stream_attrs,
            limits,
            rules,
            ingest_ts_ns,
            tally,
        ) {
            Ok((normalized, dropped_attrs, converted_body)) => {
                records.push(normalized);
                rejected.extend(dropped_attrs);
                if converted_body {
                    *body_conversions += 1;
                }
            }
            Err(reason) => rejected.push(reason),
        }
    }
    if records.len() > stored_before {
        tally.merge(&scope_tally);
    }
}

#[allow(clippy::too_many_arguments)]
fn normalize_record(
    record: &LogRecord,
    stream_id: LogStreamId,
    stream_attrs: &[u8],
    limits: &LogIngestLimits,
    rules: &TenantRules,
    ingest_ts_ns: i64,
    tally: &mut IpcryptTally,
) -> Result<(NormalizedLogRecord, Vec<LogRejection>, bool), LogRejection> {
    if record.attributes.len() > limits.max_attributes_per_record {
        return Err(LogRejection::TooManyAttributes {
            count: record.attributes.len(),
            max: limits.max_attributes_per_record,
        });
    }
    let NormalizedBody {
        text: body,
        converted: converted_body,
    } = normalize_body(record.body.as_ref(), limits.max_body_len)?;
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
        match convert_attr(kv, limits, rules, tally) {
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
            severity_num: u8::try_from(record.severity_number).unwrap_or(0),
            severity_text: record.severity_text.clone(),
            body,
            trace_id: <[u8; 16]>::try_from(record.trace_id.as_slice()).ok(),
            span_id: <[u8; 8]>::try_from(record.span_id.as_slice()).ok(),
            flags: record.flags,
            attrs,
        },
        dropped,
        converted_body,
    ))
}

fn normalize_body(
    body: Option<&AnyValue>,
    max_body_len: usize,
) -> Result<NormalizedBody, LogRejection> {
    let plain = |text: String| {
        Ok(NormalizedBody {
            text,
            converted: false,
        })
    };
    match body.and_then(|v| v.value.as_ref()) {
        None => plain(String::new()),
        Some(AnyValueVariant::StringValue(s)) => plain(s.clone()),
        Some(AnyValueVariant::BoolValue(b)) => plain(b.to_string()),
        Some(AnyValueVariant::IntValue(i)) => plain(i.to_string()),
        Some(AnyValueVariant::DoubleValue(d)) => plain(format_float(*d)),
        Some(AnyValueVariant::BytesValue(b)) => plain(hex::encode(b)),
        Some(AnyValueVariant::ArrayValue(_)) | Some(AnyValueVariant::KvlistValue(_)) => {
            let value =
                convert_value(BODY_KEY, body, 1).map_err(|_| LogRejection::UnsupportedBodyKind)?;
            Ok(NormalizedBody {
                text: canonical_json(&value, max_body_len)?,
                converted: true,
            })
        }
        Some(AnyValueVariant::StringValueStrindex(_)) => Err(LogRejection::UnsupportedBodyKind),
    }
}

fn convert_attrs(
    attributes: &[KeyValue],
    limits: &LogIngestLimits,
    rules: &TenantRules,
    tally: &mut IpcryptTally,
) -> Result<Vec<(String, AttrValue)>, LogRejection> {
    let mut attrs = attributes
        .iter()
        .map(|kv| convert_attr(kv, limits, rules, tally))
        .collect::<Result<Vec<_>, _>>()?;
    attrs.shrink_to_fit();
    Ok(attrs)
}

fn convert_attr(
    kv: &KeyValue,
    limits: &LogIngestLimits,
    rules: &TenantRules,
    tally: &mut IpcryptTally,
) -> Result<(String, AttrValue), LogRejection> {
    if kv.key.len() > limits.max_attribute_key_len {
        return Err(LogRejection::AttributeKeyTooLong {
            len: kv.key.len(),
            max: limits.max_attribute_key_len,
        });
    }
    let mut value = convert_value(&kv.key, kv.value.as_ref(), 1)?;
    let mut attr_tally = IpcryptTally::new();
    apply_ipcrypt_attr(rules, &mut attr_tally, &kv.key, &mut value);
    if !attr_value_fits_storage(&value) {
        return Err(if has_container_past_entry_cap(&value) {
            LogRejection::AttributeTooManyEntries {
                key: kv.key.clone(),
                max: MAX_ATTR_ENTRIES as usize,
            }
        } else {
            too_deeply_nested(&kv.key)
        });
    }
    let len = attr_value_len(&value);
    if len > limits.max_attribute_value_len {
        return Err(LogRejection::AttributeValueTooLong {
            len,
            max: limits.max_attribute_value_len,
        });
    }
    tally.merge(&attr_tally);
    Ok((exact_capacity(kv.key.clone()), value))
}

fn convert_value(
    key: &str,
    value: Option<&AnyValue>,
    depth: usize,
) -> Result<AttrValue, LogRejection> {
    if depth > MAX_ATTRIBUTE_NESTING_DEPTH {
        return Err(too_deeply_nested(key));
    }
    match value.and_then(|v| v.value.as_ref()) {
        None => Err(LogRejection::MissingAttributeValue {
            key: key.to_string(),
        }),
        Some(AnyValueVariant::StringValue(s)) => Ok(AttrValue::Str(exact_capacity(s.clone()))),
        Some(AnyValueVariant::BoolValue(b)) => Ok(AttrValue::Bool(*b)),
        Some(AnyValueVariant::IntValue(i)) => Ok(AttrValue::I64(*i)),
        Some(AnyValueVariant::DoubleValue(d)) => Ok(AttrValue::F64(*d)),
        Some(AnyValueVariant::BytesValue(b)) => Ok(AttrValue::Bytes(b.clone())),
        Some(AnyValueVariant::ArrayValue(array)) => {
            let mut items = array
                .values
                .iter()
                .map(|v| convert_value(key, Some(v), depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            items.shrink_to_fit();
            Ok(AttrValue::List(items))
        }
        Some(AnyValueVariant::KvlistValue(kvlist)) => {
            let mut entries = kvlist
                .values
                .iter()
                .map(|kv| {
                    convert_value(key, kv.value.as_ref(), depth + 1)
                        .map(|v| (exact_capacity(kv.key.clone()), v))
                })
                .collect::<Result<Vec<_>, _>>()?;
            entries.shrink_to_fit();
            Ok(AttrValue::Map(entries))
        }
        Some(AnyValueVariant::StringValueStrindex(_)) => {
            Err(LogRejection::UnsupportedAttributeValue {
                key: key.to_string(),
            })
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, InstrumentationScope, KeyValueList};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use proptest::prelude::*;

    use super::*;
    use crate::label_projection::capacity_cases::rules_on;

    /// 2026-01-01T00:00:00Z.
    const INGEST_TS_NS: i64 = 1_767_225_600_000_000_000;
    const HOUR_NS: u64 = 3_600_000_000_000;

    /// The rule-matched key in [`key`]'s pool.
    const RULED_KEY: &str = "client.ip";

    fn key() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("k"),
            Just(RULED_KEY),
            Just("svc"),
            Just(""),
            Just("key.past.twelve.bytes"),
        ]
        .prop_map(String::from)
    }

    fn text() -> impl Strategy<Value = String> {
        prop_oneof![
            Just(String::new()),
            Just("10.1.2.3".to_string()),
            Just("2001:db8::7".to_string()),
            Just("not-an-address".to_string()),
            Just("a value past the 24-byte value cap".to_string()),
            "[a-z]{1,6}",
        ]
    }

    fn leaf() -> impl Strategy<Value = Option<AnyValueVariant>> {
        prop_oneof![
            Just(None),
            text().prop_map(|s| Some(AnyValueVariant::StringValue(s))),
            any::<bool>().prop_map(|b| Some(AnyValueVariant::BoolValue(b))),
            any::<i64>().prop_map(|i| Some(AnyValueVariant::IntValue(i))),
            prop_oneof![
                any::<f64>(),
                Just(f64::NAN),
                Just(-0.0),
                Just(f64::INFINITY)
            ]
            .prop_map(|d| Some(AnyValueVariant::DoubleValue(d))),
            proptest::collection::vec(any::<u8>(), 0..4)
                .prop_map(|b| Some(AnyValueVariant::BytesValue(b))),
            any::<i32>().prop_map(|i| Some(AnyValueVariant::StringValueStrindex(i))),
        ]
    }

    /// Every `AnyValue` kind, an unset one included, nested up to three deep.
    fn variant() -> impl Strategy<Value = Option<AnyValueVariant>> {
        leaf().prop_recursive(3, 16, 3, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..3).prop_map(|values| {
                    Some(AnyValueVariant::ArrayValue(ArrayValue {
                        values: values.into_iter().map(|value| AnyValue { value }).collect(),
                    }))
                }),
                proptest::collection::vec((key(), inner), 0..3).prop_map(|entries| {
                    Some(AnyValueVariant::KvlistValue(KeyValueList {
                        values: entries
                            .into_iter()
                            .map(|(key, value)| KeyValue {
                                key,
                                value: Some(AnyValue { value }),
                                ..Default::default()
                            })
                            .collect(),
                    }))
                }),
            ]
        })
    }

    /// An absent `AnyValue` or one holding [`variant`].
    fn any_value() -> impl Strategy<Value = Option<AnyValue>> {
        prop_oneof![
            1 => Just(None),
            6 => variant().prop_map(|value| Some(AnyValue { value })),
        ]
    }

    fn attributes(max: usize) -> impl Strategy<Value = Vec<KeyValue>> {
        proptest::collection::vec(
            (key(), any_value()).prop_map(|(key, value)| KeyValue {
                key,
                value,
                ..Default::default()
            }),
            0..max,
        )
    }

    fn timestamp() -> impl Strategy<Value = u64> {
        let now = INGEST_TS_NS as u64;
        prop_oneof![
            Just(0),
            Just(now),
            Just(now - 1),
            Just(now - 3 * HOUR_NS),
            Just(now + HOUR_NS),
            Just(u64::MAX),
        ]
    }

    fn record() -> impl Strategy<Value = LogRecord> {
        (
            any_value(),
            attributes(6),
            prop_oneof![Just(""), Just("INFO"), Just("a longer severity text")],
            prop_oneof![0..25i32, any::<i32>()],
            timestamp(),
            timestamp(),
            prop_oneof![Just(vec![]), Just(vec![1u8; 16]), Just(vec![2u8; 3])],
            prop_oneof![Just(vec![]), Just(vec![3u8; 8]), Just(vec![4u8; 5])],
            any::<u32>(),
        )
            .prop_map(
                |(body, attributes, severity, severity_number, ts, observed, trace, span, flags)| {
                    LogRecord {
                        time_unix_nano: ts,
                        observed_time_unix_nano: observed,
                        severity_number,
                        severity_text: severity.to_string(),
                        body,
                        attributes,
                        flags,
                        trace_id: trace,
                        span_id: span,
                        ..Default::default()
                    }
                },
            )
    }

    fn scope_logs() -> impl Strategy<Value = ScopeLogs> {
        (
            proptest::option::weighted(
                0.8,
                (
                    prop_oneof![Just(""), Just("lib"), Just("other.lib")],
                    prop_oneof![Just(""), Just("1.0")],
                    attributes(4),
                ),
            ),
            proptest::collection::vec(record(), 0..4),
        )
            .prop_map(|(scope, log_records)| ScopeLogs {
                scope: scope.map(|(name, version, attributes)| InstrumentationScope {
                    name: name.to_string(),
                    version: version.to_string(),
                    attributes,
                    ..Default::default()
                }),
                log_records,
                ..Default::default()
            })
    }

    fn request() -> impl Strategy<Value = ExportLogsServiceRequest> {
        proptest::collection::vec(
            (
                proptest::option::weighted(0.8, attributes(5)),
                proptest::collection::vec(scope_logs(), 0..4),
            )
                .prop_map(|(resource, scope_logs)| ResourceLogs {
                    resource: resource.map(|attributes| Resource {
                        attributes,
                        ..Default::default()
                    }),
                    scope_logs,
                    ..Default::default()
                }),
            0..4,
        )
        .prop_map(|resource_logs| ExportLogsServiceRequest { resource_logs })
    }

    /// Limits small enough that every per-attribute, per-scope, per-resource
    /// and request-wide refusal is reachable from [`request`].
    fn limits() -> impl Strategy<Value = LogIngestLimits> {
        (
            prop_oneof![3 => Just(100_000usize), 1 => 0..10usize],
            prop_oneof![Just(65_536usize), 0..40usize],
            prop_oneof![2 => Just(usize::MAX), 1 => 0..4_000usize],
        )
            .prop_map(
                |(max_records, max_body_len, max_resolved)| LogIngestLimits {
                    max_records_per_request: max_records,
                    max_attributes_per_record: 4,
                    max_attribute_key_len: 12,
                    max_attribute_value_len: 24,
                    max_body_len,
                    max_resource_attributes: 3,
                    max_scope_attributes: 2,
                    max_resolved_label_bytes_per_request: max_resolved,
                    ..LogIngestLimits::default()
                },
            )
    }

    /// An attribute value with each float as its bit pattern, so a NaN
    /// compares equal to itself and `-0.0` unequal to `0.0`.
    #[derive(Debug, Clone, PartialEq)]
    enum Bits {
        Str(String),
        I64(i64),
        F64(u64),
        Bool(bool),
        Bytes(Vec<u8>),
        List(Vec<Bits>),
        Map(Vec<(String, Bits)>),
    }

    fn bits(value: &AttrValue) -> Bits {
        match value {
            AttrValue::Str(s) => Bits::Str(s.clone()),
            AttrValue::I64(i) => Bits::I64(*i),
            AttrValue::F64(d) => Bits::F64(d.to_bits()),
            AttrValue::Bool(b) => Bits::Bool(*b),
            AttrValue::Bytes(b) => Bits::Bytes(b.clone()),
            AttrValue::List(items) => Bits::List(items.iter().map(bits).collect()),
            AttrValue::Map(entries) => {
                Bits::Map(entries.iter().map(|(k, v)| (k.clone(), bits(v))).collect())
            }
        }
    }

    type ComparableRecord = (
        LogStreamId,
        Vec<u8>,
        i64,
        i64,
        u8,
        String,
        String,
        Option<[u8; 16]>,
        Option<[u8; 8]>,
        u32,
        Vec<(String, Bits)>,
    );

    /// Every field of the output, floats by bit pattern.
    fn comparable(
        out: &LogNormalizeOutput,
    ) -> (Vec<ComparableRecord>, Vec<LogRejection>, usize, IpcryptTally) {
        let records = out
            .records
            .iter()
            .map(|r| {
                (
                    r.stream_id,
                    r.stream_attrs.clone(),
                    r.ts_ns,
                    r.observed_ts_ns,
                    r.severity_num,
                    r.severity_text.clone(),
                    r.body.clone(),
                    r.trace_id,
                    r.span_id,
                    r.flags,
                    r.attrs.iter().map(|(k, v)| (k.clone(), bits(v))).collect(),
                )
            })
            .collect();
        (
            records,
            out.rejected.clone(),
            out.body_conversions,
            out.ipcrypt_tally.clone(),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        /// Moving values out of the request, sharing the scope's blob and
        /// folding the projection into the pass change nothing a caller sees:
        /// both entry points produce exactly the oracle's records, refusals,
        /// body-conversion count and IPCrypt tally, and the projection the
        /// server charges is the oracle's.
        #[test]
        fn normalize_output_is_unchanged(
            req in request(),
            limits in limits(),
            ruled in any::<bool>(),
        ) {
            let rules = if ruled { rules_on(&[RULED_KEY]) } else { TenantRules::none() };
            let projected =
                crate::label_projection::project_log_resolved_label_bytes(&req, &limits, &rules);
            prop_assert_eq!(projected.get(), project(&req, &limits, &rules));

            let expected = comparable(&normalize_logs(req.clone(), &limits, &rules, INGEST_TS_NS));
            let folded = comparable(&super::super::normalize_logs(
                req.clone(),
                &limits,
                &rules,
                INGEST_TS_NS,
            ));
            prop_assert_eq!(&folded, &expected);
            let given = comparable(&super::super::normalize_logs_projected(
                req,
                &limits,
                &rules,
                INGEST_TS_NS,
                projected,
            ));
            prop_assert_eq!(&given, &expected);
        }
    }
}
