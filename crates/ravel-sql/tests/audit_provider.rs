//! Integration tests for [`ravel_sql::AuditTableProvider`] (ADR-0040, issue
//! #383), the `audit` SQL table over an already-resolved `Signal::Audit`
//! snapshot.
//!
//! Audit records ride RLOG v1 verbatim (ADR-0040 decision 2), so these fixtures
//! are built directly with `ravel-logseg`'s `RlogWriter`. The `audit` table is
//! deliberately generic: it promotes no record-specific field into a typed
//! column, exposing only `ts_ns`, `severity_text`, `body`, and the whole merged
//! attribute map. The fixtures therefore mix a legal-hold record (the one kind
//! shipped so far) with a generic query-audit record (a later kind, ADR-0042) to
//! prove the table is not legal-hold-specific.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, MapArray, StringArray, StructArray, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher};
use ravel_sql::AuditTableProvider;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        // Matches the TenantHash([7u8; 16]) this test fetches as; issue #612
        // added a footer tenant_hash check on the RLOG read path, so a footer
        // naming a different tenant than the fetch now fails closed.
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// A generic audit record: event time, severity, body, and an arbitrary set of
/// structured `attrs` (whatever the record kind carries). The audit stream is a
/// single synthetic stream (empty resource + a fixed `audit` scope); the
/// record-specific fields live in per-record `attrs`.
fn audit_record(ts: i64, severity: &str, body: &str, attrs: &[(&str, &str)]) -> LogRecord {
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&[], "audit", "1", &[]),
        stream_attrs: stream_attrs_bytes(&[], "audit", "1", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 0,
        severity_text: severity.to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs
            .iter()
            .map(|(k, v)| (k.to_string(), AttrValue::Str(v.to_string())))
            .collect(),
    }
}

/// [`audit_record`] whose stream carries `resource` attributes in its
/// `stream_attrs` (the RESOURCE position) rather than only per-record `attrs`.
/// Used to prove selective erasure catches a subject named only at the merged
/// resource/scope level (issue #928).
fn audit_record_on_resource(
    resource: &[(String, AttrValue)],
    ts: i64,
    severity: &str,
    body: &str,
) -> LogRecord {
    let mut r = audit_record(ts, severity, body, &[]);
    r.stream_id = ravel_types::logstream::log_stream_id(resource, "audit", "1", &[]);
    r.stream_attrs = stream_attrs_bytes(resource, "audit", "1", &[]);
    r
}

async fn write_object(store: &MemoryStore, key: &str, records: &[LogRecord]) -> SegmentRef {
    let mut w = RlogWriter::new(small_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

fn ts_lit(v: i64) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::TimestampNanosecond(Some(v), None))
}

/// One generic audit row: `(ts, severity_text, body)`.
type Row = (i64, String, String);

fn batches_to_rows(batches: &[RecordBatch]) -> BTreeSet<Row> {
    let mut out = BTreeSet::new();
    for batch in batches {
        assert_eq!(
            batch.schema(),
            ravel_sql::audit_schema(),
            "public audit schema"
        );
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let sev = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("severity col");
        let body = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body col");
        for i in 0..batch.num_rows() {
            out.insert((
                ts.value(i),
                sev.value(i).to_string(),
                body.value(i).to_string(),
            ));
        }
    }
    out
}

async fn collect_plan(plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

fn provider(store: MemoryStore, segments: Vec<SegmentRef>) -> AuditTableProvider {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    AuditTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    )
}

/// A ts range returns exactly the surviving generic rows across several objects,
/// checked against an independent oracle. The records deliberately span two
/// different audit kinds (legal-hold and query-audit).
#[tokio::test]
async fn scan_prunes_by_ts_returns_exact_generic_rows() {
    let store = MemoryStore::new();

    // Object A: ts 100..=104, legal-hold records (kind=legal_hold).
    let obj_a: Vec<LogRecord> = (100..=104)
        .map(|ts| {
            audit_record(
                ts,
                "NOTICE",
                &format!("legal hold placed {ts}"),
                &[
                    ("kind", "legal_hold"),
                    ("hold.op", "place"),
                    ("hold.scope", "tenant/acme"),
                    ("hold.reason", "litigation"),
                ],
            )
        })
        .collect();
    // Object B: ts 5000..=5004, fully out of range (pruned before GET).
    let obj_b: Vec<LogRecord> = (5000..=5004)
        .map(|ts| audit_record(ts, "INFO", "future audit", &[("kind", "query_audit")]))
        .collect();
    // Object C: ts 200..=203, generic query-audit records (a later kind).
    let obj_c: Vec<LogRecord> = (200..=203)
        .map(|ts| {
            audit_record(
                ts,
                "INFO",
                &format!("query executed {ts}"),
                &[
                    ("kind", "query_audit"),
                    ("actor", "api-key/xyz"),
                    ("action", "query"),
                    ("result", "ok"),
                ],
            )
        })
        .collect();

    let ref_a = write_object(&store, "audit/a.rlog", &obj_a).await;
    let ref_b = write_object(&store, "audit/b.rlog", &obj_b).await;
    let ref_c = write_object(&store, "audit/c.rlog", &obj_c).await;

    let provider = provider(store, vec![ref_a, ref_b, ref_c]);

    // WHERE ts_ns >= 100 AND ts_ns <= 300
    let (lo, hi) = (100i64, 300i64);
    let filters = vec![
        col("ts_ns").gt_eq(ts_lit(lo)),
        col("ts_ns").lt_eq(ts_lit(hi)),
    ];
    let plan = provider.plan_filters(4, &filters).expect("build plan");
    let got = batches_to_rows(&collect_plan(plan).await);

    let mut want = BTreeSet::new();
    for records in [&obj_a, &obj_b, &obj_c] {
        for r in records {
            if r.ts_ns >= lo && r.ts_ns <= hi {
                want.insert((r.ts_ns, r.severity_text.clone(), r.body.clone()));
            }
        }
    }
    // Sanity: the out-of-range object contributes nothing.
    assert!(!want.iter().any(|(_, _, body)| body == "future audit"));
    assert_eq!(got, want, "scan output must equal the oracle exactly");
}

/// The `attrs` column exposes every record-specific field generically, for both
/// a legal-hold record and a query-audit record read from the same table. This
/// is the whole point of the generic schema: a new audit kind needs no column.
#[tokio::test]
async fn attrs_map_is_generic_across_kinds() {
    let store = MemoryStore::new();
    let hold = audit_record(
        1,
        "NOTICE",
        "legal hold",
        &[
            ("kind", "legal_hold"),
            ("hold.op", "place"),
            ("hold.scope", "tenant/acme"),
        ],
    );
    let query = audit_record(
        2,
        "INFO",
        "query executed",
        &[
            ("kind", "query_audit"),
            ("actor", "svc/a"),
            ("action", "query"),
        ],
    );
    let seg = write_object(&store, "audit/mixed.rlog", &[hold, query]).await;
    let provider = provider(store, vec![seg]);

    let batches = collect_plan(provider.plan(1).expect("build plan")).await;
    // Records are emitted ts-ascending, so row 0 is the legal-hold, row 1 the query.
    let hold_map = attrs_map(&batches[0], 0);
    assert_eq!(map_get(&hold_map, "kind"), Some("legal_hold".to_string()));
    assert_eq!(map_get(&hold_map, "hold.op"), Some("place".to_string()));
    assert_eq!(
        map_get(&hold_map, "hold.scope"),
        Some("tenant/acme".to_string())
    );

    let query_map = attrs_map(&batches[0], 1);
    assert_eq!(map_get(&query_map, "kind"), Some("query_audit".to_string()));
    assert_eq!(map_get(&query_map, "actor"), Some("svc/a".to_string()));
    assert_eq!(map_get(&query_map, "action"), Some("query".to_string()));
    // The generic record carries no legal-hold fields; the schema still holds it.
    assert_eq!(map_get(&query_map, "hold.op"), None);
}

/// Issue #829 (ADR-0064 decision 3, EJ-T3 #753): a pending selective-erasure
/// request on the resolved snapshot excludes matching rows through the real
/// `AuditTableProvider` scan path, the one the SQL `audit` table uses in
/// production. Covers `AuditTableProvider::build_scan` passing the
/// snapshot-derived predicates into `AuditScanExec` (audit_provider.rs) and
/// `AuditScanExec` calling `LogQuery::with_erasure` before fetch
/// (audit_scan.rs); reverting `.with_erasure((*erasure).clone())` back to a
/// bare `LogQuery::new(ts_min, ts_max)` in audit_scan.rs makes the erased row
/// reappear.
#[tokio::test]
async fn pending_erasure_excludes_matching_rows() {
    let store = MemoryStore::new();
    let hold = audit_record(1, "NOTICE", "erase me", &[("actor", "svc/erase")]);
    let query = audit_record(2, "INFO", "keep me", &[("actor", "svc/keep")]);
    let seg = write_object(&store, "audit/erasure.rlog", &[hold, query]).await;

    let request = ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "actor".to_string(),
            value: "svc/erase".to_string(),
        }],
        ..Default::default()
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: vec![request],
    };
    let provider = AuditTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    );

    let batches = collect_plan(provider.plan(1).expect("build plan")).await;
    let got = batches_to_rows(&batches);
    assert_eq!(
        got,
        BTreeSet::from([(2, "INFO".to_string(), "keep me".to_string())]),
        "the erased row is excluded; the other row survives"
    );
}

/// Issue #928 (ADR-0064): a subject named ONLY in a RESOURCE/scope
/// (`stream_attrs`) attribute must also be excluded from the `audit` table. The
/// `attrs` column materializes the merged resource + scope + record view, so
/// `actor` is queryable, yet the fetcher-level filter matches per-record
/// attributes alone and never sees it. Before the scan-layer `retain_unerased`
/// in `audit_scan.rs::prepare_partition`, this row leaked; removing that call
/// reintroduces the leak.
#[tokio::test]
async fn pending_erasure_excludes_resource_attribute_rows() {
    let store = MemoryStore::new();
    // `actor` lives in the RESOURCE position (stream_attrs), not per-record.
    let erase = audit_record_on_resource(
        &[("actor".to_string(), AttrValue::Str("svc/erase".to_string()))],
        1,
        "NOTICE",
        "erase me",
    );
    let keep = audit_record_on_resource(
        &[("actor".to_string(), AttrValue::Str("svc/keep".to_string()))],
        2,
        "INFO",
        "keep me",
    );
    let seg = write_object(&store, "audit/erasure-resource.rlog", &[erase, keep]).await;

    let request = ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "actor".to_string(),
            value: "svc/erase".to_string(),
        }],
        ..Default::default()
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: vec![request],
    };
    let provider = AuditTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    );

    let batches = collect_plan(provider.plan(1).expect("build plan")).await;
    let got = batches_to_rows(&batches);
    assert_eq!(
        got,
        BTreeSet::from([(2, "INFO".to_string(), "keep me".to_string())]),
        "the resource-only subject svc/erase is excluded; the other row survives"
    );
}

fn attrs_map(batch: &RecordBatch, row: usize) -> Vec<(String, String)> {
    let map = batch
        .column(3)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("attrs map col");
    let entries = map.value(row);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map entries struct");
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keys");
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("vals");
    (0..keys.len())
        .map(|j| (keys.value(j).to_string(), vals.value(j).to_string()))
        .collect()
}

fn map_get(map: &[(String, String)], key: &str) -> Option<String> {
    map.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}
