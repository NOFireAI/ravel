//! Logs SQL scan batch-building throughput (ADR-0099 decision 7, issue #415).
//!
//! Both benchmarks drain [`ravel_sql::LogsScanExec`] itself over one RLOG object
//! held in a `MemoryStore`, so what they time is fetch/decode plus the
//! per-block batch construction this ADR added, with no object-store latency in
//! the way. The two fixtures are the *same corpus*; they differ only in the
//! projection, which is what decides the batch-building path:
//!
//! - `fast_path_fixed_columns` projects only fixed columns (`ts`, `body`), so
//!   the scan is columnar-eligible and every spill-free block is turned into an
//!   Arrow batch straight from the `ColumnarBlockView`, with no `LogRecord` and
//!   no `merged_attrs`;
//! - `row_path_attrs_map` projects the merged `attrs` map, which makes the query
//!   ineligible ([`columnar_static_eligible`] rejects any `attrs` reference), so
//!   the unchanged row path rebuilds a `LogRecord` per row and merges its
//!   attributes.
//!
//! Reporting both over one corpus makes the comparison like-for-like: the only
//! variable is the path, so the throughput gap is the path's, not the fixture's.
//! Throughput is reported in records per second (`Throughput::Elements`).
//! Allocation counting lives in `tests/logs_scan_allocations.rs`, which needs a
//! one-test binary to keep the count deterministic.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{LOG_COL_ATTRS, LogsScanExec, logs_schema_with_declared};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
/// Records written into the one RLOG object. With the default block target
/// (8192 records) this is several full blocks, so both paths run their per-block
/// loop rather than a single block.
const RECORDS: usize = 40_000;

const WORD_VOCAB: &[&str] = &["timeout", "connection", "error", "ok", "retry"];

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// One record. Carries a resource attribute and one dynamic attribute so the
/// row path's `merged_attrs` has real work to do, matching a realistic `attrs`
/// projection; the fast path never touches either.
fn record(i: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc-{}", i % 8)),
    )];
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: i as i64,
        observed_ts_ns: i as i64,
        severity_num: (i % 24) as u8,
        severity_text: "INFO".to_string(),
        body: format!(
            "{} {}",
            WORD_VOCAB[i % WORD_VOCAB.len()],
            WORD_VOCAB[(i + 2) % WORD_VOCAB.len()]
        ),
        trace_id: Some([(i % 251) as u8; 16]),
        span_id: Some([(i % 251) as u8; 8]),
        flags: i as u32,
        attrs: vec![("user_id".to_string(), AttrValue::Str(format!("u{i}")))],
    }
}

/// Write `RECORDS` records into one RLOG object and return its `SegmentRef`.
async fn build() -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
    let store = MemoryStore::new();
    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for i in 0..RECORDS {
        let r = record(i);
        min = min.min(r.ts_ns);
        max = max.max(r.ts_ns);
        w.push(r).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    let key = "logs/bench.rlog";
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    let seg = SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: RECORDS as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    };
    (Arc::new(store), vec![seg])
}

/// Drain a `LogsScanExec` over `segments` with `projection`, returning the rows
/// it emitted.
async fn drain(
    store: Arc<dyn ObjectStoreBackend>,
    segments: &[SegmentRef],
    projection: Vec<usize>,
) -> usize {
    let scan = LogsScanExec::new(
        TENANT,
        LogSegmentFetcher::new(store),
        segments,
        1,
        i64::MIN,
        i64::MAX,
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Some(&projection),
        QueryAccounting::new(),
        logs_schema_with_declared(&[]),
        Arc::new(Vec::new()),
    )
    .expect("build scan");
    let mut stream = scan
        .execute(0, Arc::new(TaskContext::default()))
        .expect("execute scan");
    let mut rows = 0;
    while let Some(next) = stream.next().await {
        rows += next.expect("batch").num_rows();
    }
    rows
}

fn bench_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (store, segments) = rt.block_on(build());

    // ts, body: fixed columns only, so the scan takes the columnar fast path.
    let fast = vec![0usize, 4];
    // ts, body, attrs: the merged map makes the query columnar-ineligible, so
    // the same corpus drains the row path.
    let row = vec![0usize, 4, LOG_COL_ATTRS];

    for (name, projection) in [
        ("fast_path_fixed_columns", fast),
        ("row_path_attrs_map", row),
    ] {
        let rows = rt.block_on(drain(Arc::clone(&store), &segments, projection.clone()));

        let mut group = c.benchmark_group("logs_scan");
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                let emitted = rt.block_on(drain(Arc::clone(&store), &segments, projection.clone()));
                std::hint::black_box(emitted);
            });
        });
        group.finish();
    }
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
