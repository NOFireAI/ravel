//! ADR-2677 decision 6 (issue #2694) through the real `POST /api/v1/sql`
//! handler: `ORDER BY ts LIMIT k` over `logs` returns the same rows as a scan
//! that skips nothing, reads every block that can hold one of the answer's
//! rows, reports `stats.pruning` figures that add up, and plans with the
//! TopK's `ts` threshold armed on the scan.
//!
//! The fixture is eight published RLOG objects of two streams each, 64-record
//! blocks, eight blocks per object. Object `o` has rank `(o + 1) % 8`, and rank
//! `r` holds event times `1 + 8 * (40r + m) + r` for `m` in `0..512` (stream
//! `m % 2`), so the ranks' spans overlap, every `ts` is unique, and the object
//! published last holds the smallest `ts`. Within an object the two streams'
//! blocks interleave in `ts`, so block minima are not monotone.
//!
//! The server exposes no switch for the skip, so the unskipped answer comes
//! from two places: the fixture itself, and the same statement led by
//! `observed_ts`, which every record sets equal to `ts` and which the skip
//! does not read.
//!
//! How many blocks a run skips is not fixed here. The plan is a single TopK
//! over a `CoalescePartitionsExec` over eight scan partitions, so each
//! partition decodes on its own task and buffers into the coalesce channel
//! while the TopK has not yet seen a row: a partition can drain its whole
//! segment before the threshold exists, and the partitions that start later
//! may then skip theirs whole. This test asserts what holds on every
//! schedule; `crates/ravel-sql/tests/topk_threshold_skip_order.rs` drives the
//! threshold in a fixed order and pins the exact skip.
//!
//! What does hold on every schedule is that the production session arms the
//! skip at all. The endpoint refuses `EXPLAIN`, so the test explains the
//! statement through the `SqlExecutor` the router serves, which plans it with
//! the same session builder, and asserts the scan line carries
//! `topk_threshold=ts`.

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
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_server::sql::{SqlState, router};
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId, TimeRange};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;

const OBJECTS: usize = 8;
const STREAMS: usize = 2;
const PER_OBJECT: usize = 512;
const RECORDS_PER_BLOCK: usize = 64;
const BLOCKS_PER_OBJECT: usize = PER_OBJECT / RECORDS_PER_BLOCK;
const TOTAL_BLOCKS: usize = OBJECTS * BLOCKS_PER_OBJECT;

struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn rank(object: usize) -> usize {
    (object + 1) % OBJECTS
}

fn ts_of(rank: usize, m: usize) -> i64 {
    1 + (8 * (40 * rank + m) + rank) as i64
}

fn body_of(rank: usize, m: usize) -> String {
    format!("r{rank}m{m}")
}

/// Publish object `index` and its `Signal::Logs` commit record, as the log
/// shard actor does, and return each of its blocks' minimum `ts` as the
/// written skip index records it.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
) -> Vec<i64> {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(7_000 + index as u128);
    let cfg = RlogConfig {
        block_target_records: RECORDS_PER_BLOCK,
        ..RlogConfig::default()
    };
    let mut writer = RlogWriter::new(
        cfg,
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: index as u64 + 1,
        },
    );
    let r = rank(index);
    for m in 0..PER_OBJECT {
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str(format!("svc{}", m % STREAMS)),
        )];
        writer
            .push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts_of(r, m),
                observed_ts_ns: ts_of(r, m),
                severity_num: 9,
                severity_text: "INFO".into(),
                body: body_of(r, m),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let block_mins: Vec<i64> = RlogReader::new(&bytes, &cfg)
        .expect("read back rlog object")
        .directories()
        .skip_index()
        .l0
        .iter()
        .map(|block| block.min_ts)
        .collect();
    assert_eq!(block_mins.len(), BLOCKS_PER_OBJECT, "object {index}");
    let min_event_ts_ns = ts_of(r, 0);
    let max_event_ts_ns = ts_of(r, PER_OBJECT - 1);
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: PER_OBJECT as u64,
        series_count: STREAMS as u64,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
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
    block_mins
}

/// The endpoint's router and the executor it serves.
fn build_router(
    store: Arc<dyn ObjectStoreBackend>,
    tokens: HashMap<String, TenantId>,
) -> (Router, Arc<SqlExecutor>) {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = Arc::new(SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    ));
    let app = router(SqlState {
        executor: Arc::clone(&executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(120),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    });
    (app, executor)
}

async fn post_json(app: &Router, query: &str) -> Value {
    let payload = serde_json::json!({
        "query": query,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
        "timeout": 60.0,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer acme-token")
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
    let value: Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    assert_eq!(status, StatusCode::OK, "{query}: {value}");
    assert_eq!(value["status"], "success", "{query}: {value}");
    value
}

/// The `body` column of a response, in row order.
fn bodies(value: &Value) -> Vec<String> {
    let col = value["data"]["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns in {value}"))
        .iter()
        .position(|c| c["name"].as_str() == Some("body"))
        .unwrap_or_else(|| panic!("no body column in {value}"));
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"))
        .iter()
        .map(|row| row[col].as_str().expect("body is a string").to_string())
        .collect()
}

fn pruning(value: &Value, key: &str) -> u64 {
    value["stats"]["pruning"][key]
        .as_u64()
        .unwrap_or_else(|| panic!("no stats.pruning.{key} in {value}"))
}

/// The fixture's rows as `(ts, body)`, in `ts` order.
fn sorted_rows() -> Vec<(i64, String)> {
    let mut all: Vec<(i64, String)> = (0..OBJECTS)
        .flat_map(|o| {
            let r = rank(o);
            (0..PER_OBJECT).map(move |m| (ts_of(r, m), body_of(r, m)))
        })
        .collect();
    all.sort();
    all
}

/// The `k` smallest-`ts` bodies of the fixture, in `ts` order.
fn expected(k: usize) -> Vec<String> {
    sorted_rows().into_iter().take(k).map(|(_, b)| b).collect()
}

/// Whether the scan in the plan the executor builds for `sql`, over the
/// window and clock `post_json` sends through the endpoint, carries the
/// TopK's `ts` threshold.
async fn threshold_armed(executor: &SqlExecutor, tenant: &TenantId, sql: &str) -> bool {
    let req = SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(60),
        row_window: false,
        max_rows: None,
        budgets: None,
    };
    let plan = executor
        .explain(tenant.hash(), &req)
        .await
        .unwrap_or_else(|e| panic!("{sql}: explain failed: {e}"))
        .plan_text;
    let scan_line = plan
        .lines()
        .find(|line| line.trim_start().starts_with("LogsScanExec:"))
        .unwrap_or_else(|| panic!("{sql}: no LogsScanExec in\n{plan}"));
    eprintln!("{sql}: {}", scan_line.trim());
    scan_line.contains("topk_threshold=ts")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_ts_limit_reads_the_must_read_set_and_arms_the_threshold() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let mut block_mins = Vec::with_capacity(OBJECTS);
    for index in 0..OBJECTS {
        block_mins.push(publish_object(store.as_ref(), &tenant, index).await);
    }
    let rows = sorted_rows();
    assert!(
        rows.windows(2).all(|w| w[0].0 < w[1].0),
        "every ts is unique"
    );
    assert_eq!(
        rows[0].0,
        ts_of(rank(OBJECTS - 1), 0),
        "the object published last holds the smallest ts"
    );
    for (index, mins) in block_mins.iter().enumerate() {
        assert!(
            mins.windows(2).any(|w| w[0] > w[1]),
            "object {index}: block minima are not monotone: {mins:?}"
        );
    }
    let (app, executor) = build_router(
        store,
        HashMap::from([("acme-token".to_string(), tenant.clone())]),
    );

    // The unskipped answer: led by `observed_ts`, which equals `ts` on every
    // record, so the order is the same and the skip reads nothing.
    let oracle_sql = "SELECT ts, body FROM logs ORDER BY observed_ts LIMIT 10";
    assert!(
        !threshold_armed(&executor, &tenant, oracle_sql).await,
        "a TopK led by observed_ts arms no ts threshold"
    );
    let oracle = post_json(&app, oracle_sql).await;
    assert_eq!(bodies(&oracle), expected(10), "{oracle}");
    assert_eq!(pruning(&oracle, "blocksSkippedByThreshold"), 0, "{oracle}");
    assert_eq!(
        pruning(&oracle, "segmentsSkippedByThreshold"),
        0,
        "{oracle}"
    );
    assert_eq!(
        pruning(&oracle, "blocksScanned"),
        TOTAL_BLOCKS as u64,
        "{oracle}"
    );

    // The TopK's threshold never drops below the oracle's 10th `ts`, so no
    // block or object whose minimum is at or below it can be skipped. Those
    // objects are always opened and those blocks always scanned.
    let tenth_ts = sorted_rows()[9].0;
    let must_read = block_mins
        .iter()
        .flatten()
        .filter(|&&min| min <= tenth_ts)
        .count() as u64;
    let must_open = block_mins
        .iter()
        .filter(|mins| mins.iter().any(|&min| min <= tenth_ts))
        .count() as u64;
    assert!(must_read > 0 && must_read < TOTAL_BLOCKS as u64);

    for sql in [
        "SELECT ts, body FROM logs ORDER BY ts LIMIT 10",
        "SELECT ts, body FROM logs ORDER BY ts, body LIMIT 10",
    ] {
        assert!(
            threshold_armed(&executor, &tenant, sql).await,
            "{sql}: the production session arms the TopK threshold on the scan"
        );

        let value = post_json(&app, sql).await;
        assert_eq!(bodies(&value), bodies(&oracle), "{sql}: {value}");
        let scanned = pruning(&value, "blocksScanned");
        let total = pruning(&value, "blocksTotal");
        let blocks_skipped = pruning(&value, "blocksSkippedByThreshold");
        let segments_skipped = pruning(&value, "segmentsSkippedByThreshold");
        assert_eq!(
            pruning(&value, "segments"),
            OBJECTS as u64,
            "{sql}: {value}"
        );
        // Every opened object's blocks are either scanned or skipped, and every
        // object is either opened or skipped whole.
        assert_eq!(scanned + blocks_skipped, total, "{sql}: {value}");
        assert_eq!(
            total + segments_skipped * BLOCKS_PER_OBJECT as u64,
            TOTAL_BLOCKS as u64,
            "{sql}: {value}"
        );
        assert!(
            segments_skipped <= OBJECTS as u64 - must_open,
            "{sql}: an object holding a row at or below ts {tenth_ts} was skipped: {value}"
        );
        // Inside the opened objects, only blocks whose minimum is above the
        // oracle's 10th `ts` can have been skipped.
        assert!(
            scanned >= must_read && blocks_skipped <= total - must_read,
            "{sql}: a block with minimum at or below ts {tenth_ts} was skipped \
             ({must_read} such blocks): {value}"
        );
        eprintln!(
            "{sql}: pruning fired: {}; blocks scanned {scanned} of {total} opened, \
             skipped by threshold {blocks_skipped}; objects skipped whole \
             {segments_skipped} of {OBJECTS}",
            blocks_skipped + segments_skipped > 0
        );
    }
}
