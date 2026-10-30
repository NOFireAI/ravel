//! Allocations per log record in OTLP logs normalization (ADR-3492 D3a,
//! issue #3496).
//!
//! This file contains EXACTLY ONE test on purpose: the measurement wraps a
//! `stats_alloc::Region` around the global allocator, so another test running
//! concurrently in this binary would land in the count. The same pattern is in
//! `tests/normalize_allocations.rs`.
//!
//! Normalization owns the decoded request, so every string it stores (keys,
//! string values, severity text, body) is moved out of it, never copied. What
//! a record still allocates is its own attribute `Vec` and its copy of the
//! stream's attribute blob (`NormalizedLogRecord::stream_attrs` is a `Vec<u8>`
//! per record, so it cannot be shared yet): two allocations per record,
//! whatever the attribute count. The measurement differences two request
//! sizes under one scope, so the per-request and per-scope allocations cancel.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_ipcrypt::rules::TenantRules;
use ravel_otlp::{
    LogIngestLimits, normalize_logs, normalize_logs_projected, project_log_resolved_label_bytes,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;

/// String attributes per record.
const ATTRS: usize = 40;

/// The moved-value floor: the record's attribute `Vec` and its copy of the
/// stream blob.
const FLOOR_PER_RECORD: usize = 2;

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// One resource, one scope, `records` records of `ATTRS` string attributes
/// each, with a non-empty severity text and string body, put through a prost
/// encode and decode so every string has the capacity a decoded request's
/// does (its length, for the 8 bytes or more every string here holds).
fn request(records: usize) -> ExportLogsServiceRequest {
    let built = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: (0..5)
                    .map(|i| string_kv(format!("resource.{i}"), format!("resource-value-{i}")))
                    .collect(),
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "scope.name".to_string(),
                    version: "1.2.3".to_string(),
                    ..Default::default()
                }),
                log_records: (0..records)
                    .map(|r| LogRecord {
                        time_unix_nano: INGEST_TS_NS as u64,
                        observed_time_unix_nano: INGEST_TS_NS as u64,
                        severity_number: 9,
                        severity_text: "INFORMATION".to_string(),
                        body: Some(AnyValue {
                            value: Some(AnyValueVariant::StringValue(format!(
                                "log line number {r:08}"
                            ))),
                        }),
                        attributes: (0..ATTRS)
                            .map(|a| {
                                string_kv(
                                    format!("attribute.key.{a:03}"),
                                    format!("attribute-value-{r:06}-{a:03}"),
                                )
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    ExportLogsServiceRequest::decode(built.encode_to_vec().as_slice()).expect("round trip")
}

#[derive(Debug, Clone, Copy)]
enum Entry {
    /// The server's path: projected outside the region, then normalized.
    Projected,
    /// The single-pass entry point.
    Plain,
}

fn allocations(entry: Entry, records: usize) -> (usize, usize) {
    let limits = LogIngestLimits::default();
    let rules = TenantRules::none();
    let req = request(records);
    let projected = project_log_resolved_label_bytes(&req, &limits, &rules);
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let out = match entry {
        Entry::Projected => normalize_logs_projected(req, &limits, &rules, INGEST_TS_NS, projected),
        Entry::Plain => normalize_logs(req, &limits, &rules, INGEST_TS_NS),
    };
    let change = region.change();
    assert_eq!(out.records.len(), records, "rejected: {:?}", out.rejected);
    assert!(out.rejected.is_empty(), "rejected: {:?}", out.rejected);
    assert!(out.records.iter().all(|r| r.attrs.len() == ATTRS));
    drop(out);
    (change.allocations, change.reallocations)
}

#[test]
fn log_record_allocations_are_the_moved_value_floor() {
    const FEW: usize = 64;
    const MANY: usize = 128;
    for entry in [Entry::Projected, Entry::Plain] {
        let (few, few_realloc) = allocations(entry, FEW);
        let (many, many_realloc) = allocations(entry, MANY);
        let per_record = (many - few) as f64 / (MANY - FEW) as f64;
        eprintln!(
            "{entry:?}: {few} allocations ({few_realloc} reallocations) at {FEW} records, \
             {many} ({many_realloc}) at {MANY}: {per_record} allocations per record"
        );
        assert_eq!(
            many - few,
            FLOOR_PER_RECORD * (MANY - FEW),
            "{entry:?}: {per_record} allocations per record with {ATTRS} string \
             attributes, against a moved-value floor of {FLOOR_PER_RECORD}"
        );
        // A moved string already at its length is not shrunk again; the one
        // reallocation allowed is the output `Vec` doubling past 64 records.
        assert!(
            many_realloc - few_realloc <= 1,
            "{entry:?}: {few_realloc} reallocations at {FEW} records, {many_realloc} at {MANY}"
        );
    }
}
