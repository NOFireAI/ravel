//! ADR-0087 reachability gate: a full-table analytical query over a wide,
//! large `logs` table completes inside a small SQL memory pool, driven through
//! the real `POST /api/v1/sql` handler.
//!
//! This is the test that fails against the pre-ADR-0087 scan, and it has to
//! live here rather than in `ravel-sql` because the failure it pins is a
//! property of the whole request path: the endpoint installs the per-query
//! `TenantDelegatingPool` (`SqlConfig::query_pool`), the executor drains the
//! plan's stream into a response, and only that combination shows a scan's
//! memory behaviour as a client-visible success or failure.
//!
//! The fixture is deliberately hostile in exactly the way the ADR describes:
//! [`ATTRS_PER_RECORD`] dynamic attributes on every one of [`TOTAL_RECORDS`]
//! records. Against the old collect-then-emit scan both queries fail: every
//! column was decoded regardless of projection, the whole partition was
//! collected in row form before any batch was emitted (a stage that was never
//! charged against the pool at all), and the per-batch `try_grow` only ever
//! grew, so the cumulative charge crossed an 8 MiB pool within the first few
//! thousand rows. Against the streaming, column-projecting scan neither query
//! decodes an attribute column at all, and the reservation tracks one block
//! plus one batch.
//!
//! Both a `COUNT(*)` (no projected column at all) and a `GROUP BY` over a
//! projected fixed column are exercised: the first is the degenerate case the
//! ADR's context paragraph names, the second proves a real aggregate over a
//! projected column still works under the same ceiling.
//!
//! Each fixture object is built column by column as one `ColumnarLogBatch`
//! and written through `RlogWriter::push_columnar`, the call `ravel-ingest`'s
//! log shard flush makes, with the objects encoded on parallel threads.
//! [`columnar_fixture_object_equals_the_row_path_object`] pins, on a
//! 1,000-record object, that this writes the same bytes as pushing the same
//! records one at a time.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, Bitmap, ColumnarLogBatch, DynCells, DynColumn, FieldType, LogRecord, RlogConfig,
    RlogWriter, StrColumnDict, VarBytes, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_server::sql::{SqlState, router};
use ravel_sql::{SqlConfig, SqlExecutor};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small on purpose: `Catalog::resolve` issues one LIST per (shard,
/// ingest-hour) pair across the window.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// Dynamic attribute columns on every record. The ADR's motivating figure is
/// "roughly 100 dynamic attributes per record"; this sits just above it.
const ATTRS_PER_RECORD: usize = 100;
/// RLOG objects in the fixture.
const OBJECTS: usize = 30;
/// Records per object. Kept modest because each object is built as one
/// in-memory batch before `finish()`, so this, not `TOTAL_RECORDS`, bounds what
/// each build thread (see [`build_wide_objects`]) holds.
const RECORDS_PER_OBJECT: usize = 10_000;
/// Total records in the `logs` table under test.
const TOTAL_RECORDS: usize = OBJECTS * RECORDS_PER_OBJECT;

/// Severity texts cycled across records, so the `GROUP BY` has several real
/// groups with known counts rather than one.
const SEVERITIES: &[&str] = &["INFO", "WARN", "ERROR", "DEBUG"];

/// The per-query DataFusion memory-pool ceiling for this test: 8 MiB, a
/// thirty-second of the 256 MiB default. The fixture's cumulative emitted
/// bytes are about two orders of magnitude past this: the `attrs` map column
/// alone is 2,504 bytes per row in Arrow form (100 entries of an 8-byte key
/// and a 9-byte value, each with a 4-byte offset, plus a 4-byte map offset),
/// about 716 MiB over the table, some 90 times the pool. So a scan whose
/// reservation tracks cumulative output cannot pass however many batches it
/// splits into.
const SMALL_POOL_BYTES: usize = 8 * 1024 * 1024;
/// The tenant-wide ceiling behind the per-query pool. Above the per-query
/// ceiling (it is shared across a tenant's concurrent queries) but still small,
/// so it cannot silently become the thing under test.
const SMALL_TENANT_BYTES: usize = 32 * 1024 * 1024;

/// A clock frozen at [`NOW_NS`].
struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn identity(index: usize, tenant_hash: [u8; 16], writer_id: Uuid) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
    }
}

/// One RLOG object of `count` wide records starting at `first_ts`, as one
/// [`ColumnarLogBatch`] written through [`RlogWriter::push_columnar`], the
/// call `ravel-ingest`'s log shard flush makes.
///
/// Every record has a fixed stream, `severity_num` 9, a severity cycling
/// through [`SEVERITIES`] by timestamp, a per-record body, no trace or span
/// id, and the same [`ATTRS_PER_RECORD`] string attributes `attr.NNN =
/// value-NNN`. The attribute *values* are shared across records on purpose
/// (they compress away, keeping the objects small); what matters to this test
/// is the number of distinct attribute *columns*, since that is what an
/// unprojected decode has to materialize per row. The attribute columns are
/// filled directly rather than from per-record `LogRecord`s, which would clone
/// every attribute pair of every record. Each also carries its one-entry
/// dictionary (`dyn_col_dicts`), with which the writer pays string encoding
/// and token bloom per distinct value rather than per row. The log shard
/// flush leaves `dyn_col_dicts` empty; the object written here is still byte
/// for byte the row path's, checked at 1,000 records by
/// [`columnar_fixture_object_equals_the_row_path_object`].
fn wide_object_bytes(identity: ObjectIdentity, first_ts: i64, count: usize) -> Vec<u8> {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let ts: Vec<i64> = (0..count).map(|i| first_ts + i as i64).collect();
    let mut severity_text = VarBytes::new();
    let mut body = VarBytes::new();
    for &t in &ts {
        severity_text.push(SEVERITIES[(t as usize) % SEVERITIES.len()].as_bytes());
        body.push(format!("request {t} completed").as_bytes());
    }
    let mut absent = Bitmap::with_capacity(count);
    absent.pad_to(count);
    let mut present = Bitmap::with_capacity(count);
    for _ in 0..count {
        present.push(true);
    }
    let mut dyn_col_dicts = Vec::with_capacity(ATTRS_PER_RECORD);
    let dyn_columns = (0..ATTRS_PER_RECORD)
        .map(|i| {
            let value = format!("value-{i:03}");
            let mut cells = VarBytes::with_capacity(count, count * value.len());
            for _ in 0..count {
                cells.push(value.as_bytes());
            }
            dyn_col_dicts.push(Some(StrColumnDict {
                distinct: vec![value.clone().into_bytes()],
                ids: vec![0; count],
            }));
            DynColumn {
                name: format!("attr.{i:03}"),
                field_type: FieldType::Str,
                cells: DynCells::Str(cells),
                validity: present.clone(),
            }
        })
        .collect();
    let batch = ColumnarLogBatch {
        num_rows: count,
        observed_ts_ns: ts.clone(),
        ts_ns: ts,
        severity_num: vec![9; count],
        flags: vec![0; count],
        severity_text,
        body,
        trace_id: Vec::new(),
        trace_id_validity: absent.clone(),
        span_id: Vec::new(),
        span_id_validity: absent,
        stream_refs: vec![0; count],
        stream_ids: vec![stream_id],
        stream_attrs: vec![stream_attrs],
        dyn_columns,
        dyn_col_dicts,
        residual_attrs: vec![Vec::new(); count],
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    writer.push_columnar(batch).expect("push columnar batch");
    writer.finish().expect("finish rlog object")
}

/// [`wide_object_bytes`]'s records built one `LogRecord` at a time and
/// written through the row path ([`RlogWriter::push`]), the reference
/// [`columnar_fixture_object_equals_the_row_path_object`] compares against.
fn wide_object_bytes_rows(identity: ObjectIdentity, first_ts: i64, count: usize) -> Vec<u8> {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    let attrs: Vec<(String, AttrValue)> = (0..ATTRS_PER_RECORD)
        .map(|i| {
            (
                format!("attr.{i:03}"),
                AttrValue::Str(format!("value-{i:03}")),
            )
        })
        .collect();
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for i in 0..count {
        let ts = first_ts + i as i64;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: SEVERITIES[(ts as usize) % SEVERITIES.len()].to_string(),
                body: format!("request {ts} completed"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: attrs.clone(),
            })
            .expect("push");
    }
    writer.finish().expect("finish")
}

/// The bytes of all [`OBJECTS`] fixture objects of [`RECORDS_PER_OBJECT`]
/// records each, object `index` starting at `first_ts(index)`, in index order.
/// Objects are built on up to `available_parallelism` threads at once, since
/// encoding them is nearly all of the test's wall clock.
fn build_wide_objects(
    tenant: &TenantId,
    first_ts: impl Fn(usize) -> i64 + Sync,
) -> Vec<(usize, Vec<u8>)> {
    let tenant_hash = tenant.hash().0;
    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(OBJECTS);
    let first_ts = &first_ts;
    let mut objects: Vec<(usize, Vec<u8>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                scope.spawn(move || {
                    (thread..OBJECTS)
                        .step_by(threads)
                        .map(|index| {
                            let id = identity(index, tenant_hash, writer_id(index));
                            (
                                index,
                                wide_object_bytes(id, first_ts(index), RECORDS_PER_OBJECT),
                            )
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("fixture build thread"))
            .collect()
    });
    objects.sort_by_key(|(index, _)| *index);
    objects
}

/// The writer id of fixture object `index`.
fn writer_id(index: usize) -> Uuid {
    Uuid::from_u128(9_000 + index as u128)
}

/// Build and publish one RLOG object of `count` wide records starting at
/// `first_ts` (see [`wide_object_bytes`] and [`publish_object`]).
async fn publish_wide_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    first_ts: i64,
    count: usize,
) {
    let id = identity(index, tenant.hash().0, writer_id(index));
    let bytes = wide_object_bytes(id, first_ts, count);
    publish_object(store, tenant, index, first_ts, count, bytes).await;
}

/// Publish `bytes`, fixture object `index` of `count` records starting at
/// `first_ts`, plus its `Signal::Logs` commit record, as `ravel-ingest`'s log
/// shard actor does.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    first_ts: i64,
    count: usize,
    bytes: Vec<u8>,
) {
    let tenant_hash = tenant.hash();
    let writer_id = writer_id(index);
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let min_event_ts_ns = first_ts;
    let max_event_ts_ns = first_ts + count as i64 - 1;

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: count as u64,
        // One stream: every fixture record shares the same resource attributes.
        series_count: 1,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        // The version the RlogWriter above actually stamped. A commit record
        // that names another one is refused before any GET is issued
        // (ADR-0892 decision 4), so this cannot be a placeholder.
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10 + index as i64,
        ingest_hour_bucket: 0,
    })
    .expect("valid log commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
}

/// The real `/api/v1/sql` router, with the per-query memory pool capped at
/// [`SMALL_POOL_BYTES`]. This is the whole point of driving the test through
/// the endpoint: `SqlConfig::max_query_bytes` is consumed where the endpoint
/// builds the query's `RuntimeEnv`, so the ceiling under test is the one
/// production installs.
fn build_router(store: Arc<dyn ObjectStoreBackend>, tokens: HashMap<String, TenantId>) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let config = SqlConfig {
        max_query_bytes: SMALL_POOL_BYTES,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        config,
        SMALL_TENANT_BYTES,
    );
    router(SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        // Generous: this test is about memory, and a debug-build scan over
        // 300k records must not be failed by a wall clock instead.
        max_deadline: Duration::from_secs(600),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

async fn post_json(app: &Router, token: &str, query: &str) -> (StatusCode, Value) {
    let payload = serde_json::json!({
        "query": query,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
        "timeout": 540.0,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(payload))
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body")
        .to_vec();
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

/// The index of `name` in the response's column list.
fn column_index(value: &Value, name: &str) -> usize {
    value["data"]["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns in {value}"))
        .iter()
        .position(|c| c["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("no column {name} in {value}"))
}

/// The rows of a successful response, as positional JSON arrays.
fn rows(value: &Value) -> &Vec<Value> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"))
}

/// The query's own reported peak reserved bytes (ADR-0044
/// `peak_intermediate_bytes`), the high-water mark of the per-query pool.
fn peak_intermediate_bytes(value: &Value) -> u64 {
    value["stats"]["accounting"]["peakIntermediateBytes"]
        .as_u64()
        .unwrap_or_else(|| panic!("no peakIntermediateBytes in {value}"))
}

/// The acceptance test (ADR-0087): a full-table `COUNT(*)` and a full-table
/// `GROUP BY` over 300,000 records carrying 100 dynamic attributes each both
/// return correct results through `POST /api/v1/sql` under an 8 MiB per-query
/// memory pool.
///
/// Failing this means the scan is holding, or charging for, memory proportional
/// to the table rather than to a block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wide_full_table_aggregates_complete_within_a_small_memory_pool() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let first_ts = |index: usize| (index * RECORDS_PER_OBJECT) as i64 + 1;
    for (index, bytes) in build_wide_objects(&tenant, first_ts) {
        publish_object(
            store.as_ref(),
            &tenant,
            index,
            first_ts(index),
            RECORDS_PER_OBJECT,
            bytes,
        )
        .await;
    }
    let app = build_router(
        store,
        HashMap::from([("acme-token".to_string(), tenant.clone())]),
    );

    // 1. `COUNT(*)`: no column projected at all. The pre-change scan still
    //    received (and materialized) every column including the 100-attribute
    //    `attrs` map, which is precisely the case the ADR's context paragraph
    //    describes as crossing the pool "well under 100,000 rows".
    let (status, value) = post_json(&app, "acme-token", "SELECT COUNT(*) FROM logs").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "COUNT(*) must complete inside the {SMALL_POOL_BYTES}-byte pool: {value}"
    );
    assert_eq!(value["status"], "success", "{value}");
    let count_rows = rows(&value);
    assert_eq!(count_rows.len(), 1, "COUNT(*) returns one row: {value}");
    assert_eq!(
        count_rows[0][0].as_i64(),
        Some(TOTAL_RECORDS as i64),
        "every record must be counted: {value}"
    );
    // The query's own high-water mark, not just "it did not fail": a scan that
    // charged cumulative output would have reported far more than the pool.
    let peak = peak_intermediate_bytes(&value);
    assert!(
        peak < SMALL_POOL_BYTES as u64,
        "peak reserved bytes {peak} must stay under the {SMALL_POOL_BYTES}-byte          pool, not merely avoid tripping it: {value}"
    );

    // 2. A real aggregate over a projected fixed column. Four groups of known
    //    size, so a partially-drained scan shows up as wrong counts rather than
    //    merely as a different error.
    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT severity_text, COUNT(*) AS n FROM logs GROUP BY severity_text",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GROUP BY must complete inside the {SMALL_POOL_BYTES}-byte pool: {value}"
    );
    assert_eq!(value["status"], "success", "{value}");

    let severity_col = column_index(&value, "severity_text");
    let n_col = column_index(&value, "n");
    let group_rows = rows(&value);
    assert_eq!(
        group_rows.len(),
        SEVERITIES.len(),
        "one row per severity: {value}"
    );
    let mut total = 0i64;
    let mut seen: Vec<String> = Vec::new();
    for row in group_rows {
        let severity = row[severity_col].as_str().expect("severity_text");
        let n = row[n_col].as_i64().expect("count");
        assert!(
            SEVERITIES.contains(&severity),
            "unexpected severity {severity} in {value}"
        );
        seen.push(severity.to_string());
        total += n;
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), SEVERITIES.len(), "duplicate groups in {value}");
    assert_eq!(
        total, TOTAL_RECORDS as i64,
        "the groups must account for every record: {value}"
    );
    let peak = peak_intermediate_bytes(&value);
    assert!(
        peak < SMALL_POOL_BYTES as u64,
        "peak reserved bytes {peak} must stay under the {SMALL_POOL_BYTES}-byte          pool: {value}"
    );
}

/// The `/sql` stats carry `peakAskedIntermediateBytes`, the high-water mark
/// of what this query's memory consumers asked for, live, before a migrated
/// hash aggregate stream's reservations are charged ahead of that ask
/// (`AggregateReserveAhead`), beside the charged `peakIntermediateBytes`. A
/// consumer is never charged less than it asked for, so the asked figure is
/// at most the charged one; this fixture is too small to also pin a lower
/// bound on the ratio honestly (`crates/ravel-sql/tests/peak_asked_bytes.rs`
/// does that at a scale where the bound holds).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_stats_json_carries_peak_asked_intermediate_bytes() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_wide_object(store.as_ref(), &tenant, 0, 1, 4_000).await;
    let app = build_router(
        store,
        HashMap::from([("acme-token".to_string(), tenant.clone())]),
    );
    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT severity_text, COUNT(*) AS n FROM logs GROUP BY severity_text",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let charged = peak_intermediate_bytes(&value);
    let asked = value["stats"]["accounting"]["peakAskedIntermediateBytes"]
        .as_u64()
        .unwrap_or_else(|| panic!("no peakAskedIntermediateBytes in {value}"));
    assert!(asked > 0, "a GROUP BY reserves memory: {value}");
    assert!(
        asked <= charged,
        "asked {asked} > charged {charged}: {value}"
    );
}

/// On a 1,000-record object, the columnar fixture build writes byte for byte
/// the object the same records pushed one at a time through the row path
/// write, so the scan reads it exactly as it reads the row-built one.
#[test]
fn columnar_fixture_object_equals_the_row_path_object() {
    let tenant = TenantId::new("acme".to_string());
    let id = || identity(0, tenant.hash().0, writer_id(0));
    let columnar = wide_object_bytes(id(), 1, 1_000);
    let rows = wide_object_bytes_rows(id(), 1, 1_000);
    assert!(
        columnar == rows,
        "columnar build wrote {} bytes, row path {} bytes, and they differ",
        columnar.len(),
        rows.len()
    );
}
