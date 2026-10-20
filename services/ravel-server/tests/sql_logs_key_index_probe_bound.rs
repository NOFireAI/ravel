//! Issue #3117 diagnosis repro (temporary, not for commit): which logs objects
//! a declared-i64 equality reaches without the KEY_IDX probe.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::key_index::KeyType;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_server::sql::{SqlState, router};
use ravel_sql::{
    DeclaredColumn, DeclaredType, KeyIndexDeclaration, SqlConfig, SqlExecutor,
    StaticDeclaredColumns,
};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;
const TOKEN: &str = "acme-token";
const RECORDS_PER_BLOCK: usize = 16;
const BLOCKS: usize = 8;
const RECORDS: usize = RECORDS_PER_BLOCK * BLOCKS;
const TARGET: i64 = 435_090_932_899_640_449;
const HOLDER: usize = 0;

struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

struct RecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    gets: Mutex<Vec<(String, GetRange)>>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets
            .lock()
            .expect("get log")
            .push((key.to_string(), range));
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

/// A distinct i64 per (object, record), never equal to TARGET.
fn decoy(object: usize, i: usize) -> i64 {
    1_000_000 + (object * RECORDS + i) as i64
}

fn record_of(object: usize, i: usize, pad_bytes: usize) -> LogRecord {
    let user = if object == HOLDER && i == 50 {
        TARGET
    } else {
        decoy(object, i)
    };
    let mut hasher = blake3::Hasher::new();
    hasher.update(&((object * RECORDS + i) as u64).to_le_bytes());
    let mut pad = vec![0u8; pad_bytes];
    hasher.finalize_xof().fill(&mut pad);
    let body: String = pad.iter().map(|b| format!("{b:02x}")).collect();
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let ts = 1_000 + (i * 4 + object) as i64;
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("o{object}r{i:03}|{body}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("user_id".to_string(), AttrValue::I64(user))],
    }
}

struct Config {
    label: &'static str,
    /// Per object, the random pad per record (object size knob).
    pads: Vec<usize>,
    target_partitions: usize,
    /// `with_projection_break_even_bytes` (moves `plan_whole_object_bound`).
    break_even: Option<u64>,
}

struct Outcome {
    sizes: Vec<u64>,
    bound: u64,
    rows: usize,
    pruned: u64,
    key_gets: u64,
    plan_gets: u64,
    scan_gets: u64,
    scan_wire: u64,
    blocks_scanned: u64,
    probed_objects: usize,
    whole_read_objects: usize,
    pruning: Value,
}

async fn run(cfg: &Config) -> Outcome {
    let recorder = Arc::new(RecordingStore {
        inner: Arc::new(MemoryStore::new()),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let tenant_hash = tenant().hash();
    let mut sizes = Vec::new();
    let mut objs: Vec<(String, u64, Option<std::ops::Range<u64>>)> = Vec::new();
    for (seq, pad) in cfg.pads.iter().enumerate() {
        let writer_id = Uuid::from_u128(3_117_000 + seq as u128);
        let mut writer = RlogWriter::new(
            RlogConfig {
                block_target_records: RECORDS_PER_BLOCK,
                ..RlogConfig::default()
            },
            ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: seq as u64 + 1,
            },
        )
        .with_key_index_fields(vec![("user_id".to_string(), KeyType::I64)]);
        let records: Vec<LogRecord> = (0..RECORDS).map(|i| record_of(seq, i, *pad)).collect();
        let min = records.iter().map(|r| r.ts_ns).min().expect("records");
        let max = records.iter().map(|r| r.ts_ns).max().expect("records");
        for r in records {
            writer.push(r).expect("push");
        }
        let bytes = writer.finish().expect("finish");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: RECORDS as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: 10 + seq as i64,
            ingest_hour_bucket: 0,
        })
        .expect("record");
        let key = keys::reconstruct_data_key(&rec).expect("key");
        let footer = ravel_logseg::footer::open(&bytes).expect("footer");
        let key_idx = footer
            .section(ravel_logseg::footer::kind::KEY_IDX)
            .map(|d| d.offset..d.offset + d.len);
        sizes.push(bytes.len() as u64);
        objs.push((key.clone(), bytes.len() as u64, key_idx));
        recorder
            .put(&key, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        publish::publish(recorder.as_ref(), &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let logs = LogSegmentFetcher::new(store.clone())
        .with_suffix_len(1024)
        .with_request_cost_bytes(1)
        .with_block_range_threshold(0)
        .with_projection_break_even_bytes(cfg.break_even);
    let bound = logs.plan_whole_object_bound();
    let sql_config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(cfg.target_partitions),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        logs,
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        sql_config,
        1 << 30,
    )
    .with_declared_column_source(Arc::new(
        StaticDeclaredColumns::new(vec![DeclaredColumn::new("user_id", DeclaredType::I64)])
            .with_key_index(KeyIndexDeclaration {
                columns: vec!["user_id".to_string()],
                trace_id: false,
            }),
    ));
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant());
    let app: Router = router(SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store: store.clone(),
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(60),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    });

    recorder.gets.lock().expect("log").clear();
    let sql = format!("SELECT user_id FROM logs WHERE user_id = {TARGET}");
    let payload = serde_json::json!({
        "query": sql,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
        "timeout": 60.0,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("request");
    let response = app.oneshot(request).await.expect("oneshot");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let v: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(v["status"], "success", "{v}");

    // An object counts as probed when any GET it served overlaps KEY_IDX.
    let gets = recorder.gets.lock().expect("log").clone();
    let probed_objects = objs
        .iter()
        .filter(|(key, size, key_idx)| {
            let Some(k) = key_idx else { return false };
            gets.iter().filter(|(g, _)| g == key).any(|(_, r)| {
                let (s, e) = match *r {
                    GetRange::Full => (0, *size),
                    GetRange::Range(s, e) => (s, e),
                    GetRange::Suffix(n) => (size.saturating_sub(n), *size),
                };
                // A whole-object read covers KEY_IDX without probing it.
                !matches!(r, GetRange::Full) && s < k.end && k.start < e && e - s < *size
            })
        })
        .count();
    let whole_read_objects = objs
        .iter()
        .filter(|(key, _, _)| {
            gets.iter()
                .any(|(g, r)| g == key && matches!(r, GetRange::Full))
        })
        .count();
    let phase = |name: &str, field: &str| -> u64 {
        v["stats"]["phases"]
            .as_array()
            .expect("phases")
            .iter()
            .find(|p| p["phase"] == name)
            .map_or(0, |p| p[field].as_u64().unwrap_or(0))
    };
    let pruning = v["stats"]["pruning"].clone();
    Outcome {
        sizes,
        bound,
        rows: v["data"]["rows"].as_array().map_or(0, Vec::len),
        pruned: pruning["segmentsPrunedByKeyIndex"].as_u64().unwrap_or(0),
        key_gets: phase("keyIndex", "s3GetRequests"),
        plan_gets: phase("plan", "s3GetRequests"),
        scan_gets: phase("scan", "s3GetRequests"),
        scan_wire: phase("scan", "s3GetWireBytes"),
        blocks_scanned: pruning["blocksScanned"].as_u64().unwrap_or(0),
        probed_objects,
        whole_read_objects,
        pruning,
    }
}

const BIG: usize = 6 * 1024;
const SMALL: usize = 2 * 1024;

#[tokio::test]
async fn t3117_which_objects_skip_the_key_probe() {
    let n = 24usize;
    let mixed = |small: usize| -> Vec<usize> {
        // The holder (object 0) is always big; `small` of the rest are small.
        (0..n)
            .map(|i| if i >= 1 && i <= small { SMALL } else { BIG })
            .collect()
    };
    let configs = vec![
        Config {
            label: "A all-big tp4",
            pads: vec![BIG; n],
            target_partitions: 4,
            break_even: None,
        },
        Config {
            label: "B all-big tp16",
            pads: vec![BIG; n],
            target_partitions: 16,
            break_even: None,
        },
        Config {
            label: "C mixed16small tp4",
            pads: mixed(16),
            target_partitions: 4,
            break_even: Some(512 << 10),
        },
        Config {
            label: "D mixed16small tp16",
            pads: mixed(16),
            target_partitions: 16,
            break_even: Some(512 << 10),
        },
        Config {
            label: "E mixed16small tp32",
            pads: mixed(16),
            target_partitions: 32,
            break_even: Some(512 << 10),
        },
        Config {
            label: "F mixed5small tp16",
            pads: mixed(5),
            target_partitions: 16,
            break_even: Some(512 << 10),
        },
        Config {
            label: "G all-big bound2MB tp4",
            pads: vec![BIG; n],
            target_partitions: 4,
            break_even: Some(2 << 20),
        },
        Config {
            label: "H all-big bound2MB tp32",
            pads: vec![BIG; n],
            target_partitions: 32,
            break_even: Some(2 << 20),
        },
        Config {
            label: "I mixed16small tp2",
            pads: mixed(16),
            target_partitions: 2,
            break_even: Some(512 << 10),
        },
        Config {
            label: "J mixed16small b0 tp4",
            pads: mixed(16),
            target_partitions: 4,
            break_even: Some(0),
        },
    ];
    let mut failures = Vec::new();
    for cfg in &configs {
        let o = run(cfg).await;
        let above = o.sizes.iter().filter(|&&s| s > o.bound).count();
        println!(
            "{:<26} objects={} bound={} above_bound={} min_size={} max_size={} rows={} \
             probed={} segmentsPrunedByKeyIndex={} unprobed={} keyIndexGETs={} planGETs={} \
             scanGETs={} scanWire={} blocksScanned={} wholeReadObjects={}",
            cfg.label,
            o.sizes.len(),
            o.bound,
            above,
            o.sizes.iter().min().unwrap(),
            o.sizes.iter().max().unwrap(),
            o.rows,
            o.probed_objects,
            o.pruned,
            o.sizes.len() - o.probed_objects,
            o.key_gets,
            o.plan_gets,
            o.scan_gets,
            o.scan_wire,
            o.blocks_scanned,
            o.whole_read_objects,
        );
        println!("    pruning={}", o.pruning);
        assert_eq!(o.rows, 1, "{}", cfg.label);
        if o.pruned != (o.sizes.len() - 1) as u64 {
            failures.push(format!(
                "{}: segmentsPrunedByKeyIndex {} != objects - 1 = {}",
                cfg.label,
                o.pruned,
                o.sizes.len() - 1
            ));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
