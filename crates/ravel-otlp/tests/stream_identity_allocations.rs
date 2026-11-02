//! Allocations of stream identity per `ScopeLogs` (ADR-3492 baseline
//! amendment cut (b), issue #3644).
//!
//! The allocation measurement wraps a `stats_alloc::Region` around the global
//! allocator, so it holds `LOCK` and the other test in this binary takes it
//! too: nothing else may allocate while the region is open.
//!
//! Identity encodes the resource attributes once per `ScopeLogs` (into the
//! stored preimage; the id is hashed from that blob), into one scratch buffer
//! per attribute set instead of one buffer per attribute. The measurement
//! differences two request sizes under one `ResourceLogs`, so per-request and
//! per-resource allocations cancel, and compares 10 against 40 resource
//! attributes: the per-scope count must not depend on the attribute count.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Mutex;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_ipcrypt::rules::TenantRules;
use ravel_logseg::stream_attrs_bytes;
use ravel_otlp::{LogIngestLimits, normalize_logs};
use ravel_types::logstream::{AttrValue, log_stream_id, log_stream_id_from_preimage};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

static LOCK: Mutex<()> = Mutex::new(());

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;

/// Allocations per `ScopeLogs` at either attribute count, measured below.
const PER_SCOPE_ALLOCATIONS: usize = 0;

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// One resource with `attrs` string attributes and `scopes` scopes, each with
/// one record, round-tripped through prost so strings have decoded capacity.
fn request(attrs: usize, scopes: usize) -> ExportLogsServiceRequest {
    let built = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: (0..attrs)
                    .map(|i| string_kv(format!("resource.{i:03}"), format!("resource-value-{i:03}")))
                    .collect(),
                ..Default::default()
            }),
            scope_logs: (0..scopes)
                .map(|s| ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: format!("scope.{s:04}"),
                        version: "1.2.3".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: INGEST_TS_NS as u64,
                        observed_time_unix_nano: INGEST_TS_NS as u64,
                        severity_number: 9,
                        severity_text: "INFORMATION".to_string(),
                        body: Some(AnyValue {
                            value: Some(AnyValueVariant::StringValue(format!(
                                "log line number {s:08}"
                            ))),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
    };
    ExportLogsServiceRequest::decode(built.encode_to_vec().as_slice()).expect("round trip")
}

fn allocations(attrs: usize, scopes: usize) -> usize {
    let limits = LogIngestLimits::default();
    let rules = TenantRules::none();
    let req = request(attrs, scopes);
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let out = normalize_logs(req, &limits, &rules, INGEST_TS_NS);
    let change = region.change();
    assert_eq!(out.records.len(), scopes, "rejected: {:?}", out.rejected);
    assert!(out.rejected.is_empty(), "rejected: {:?}", out.rejected);
    drop(out);
    change.allocations
}

#[test]
fn stream_identity_allocations_do_not_scale_with_attribute_count() {
    let _guard = LOCK.lock().unwrap();
    const FEW: usize = 8;
    const MANY: usize = 24;
    let mut per_scope = Vec::new();
    for attrs in [10usize, 40] {
        let few = allocations(attrs, FEW);
        let many = allocations(attrs, MANY);
        eprintln!("{attrs} resource attributes: {few} allocations at {FEW} scopes, {many} at {MANY}");
        assert_eq!((many - few) % (MANY - FEW), 0, "not a whole count per scope");
        per_scope.push((many - few) / (MANY - FEW));
    }
    eprintln!("allocations per ScopeLogs at 10 and 40 attributes: {per_scope:?}");
    assert_eq!(
        per_scope,
        [PER_SCOPE_ALLOCATIONS, PER_SCOPE_ALLOCATIONS],
        "per-scope allocations must be the same fixed figure at 10 and 40 attributes"
    );
}

#[test]
fn preimage_id_equals_log_stream_id_through_stream_attrs_bytes() {
    let _guard = LOCK.lock().unwrap();
    let resource: Vec<(String, AttrValue)> = vec![
        ("service.name".into(), AttrValue::Str("api".into())),
        ("host.name".into(), AttrValue::Str(String::new())),
        ("n".into(), AttrValue::I64(-3)),
        ("f".into(), AttrValue::F64(-0.0)),
        (
            "m".into(),
            AttrValue::Map(vec![
                ("b".into(), AttrValue::List(vec![AttrValue::Bool(true)])),
                ("a".into(), AttrValue::Bytes(vec![1, 2])),
            ]),
        ),
    ];
    let scope_attrs = vec![("k".into(), AttrValue::Str("v".into()))];
    for (name, version, scope) in [
        ("", "", &[][..]),
        ("scope", "1.0", &[][..]),
        ("scope", "1.0", &scope_attrs[..]),
    ] {
        let blob = stream_attrs_bytes(&resource, name, version, scope);
        assert_eq!(
            log_stream_id_from_preimage(&blob),
            log_stream_id(&resource, name, version, scope)
        );
    }
}
