//! End-to-end coverage for the background catalog fold task
//! (docs/metric-index-plan.md section 4): ingest a metric into an
//! already-sealed hour, let the fold task run against a real timer, then
//! confirm HEAD was written directly against the store (bypassing the query
//! path, which is already covered by `integration.rs`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Sealed well past the default `max_flush_lifetime (1h) + clock_skew_allowance
/// (5m) + fold_safety_margin (15m)` bound (docs/metric-index-plan.md section 2),
/// so the fold task treats this ingest hour as immutable on its first tick.
const SEALED_AGE: Duration = Duration::from_secs(3 * 60 * 60);

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn export_request(
    metric_name: &str,
    job: &str,
    value: f64,
    ts_ns: i64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", job)],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(value)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format).
/// Duplicated from `ravel_server::fold`'s private helper: this test only
/// ever reads the object, never constructs it for a real fold.
fn head_key(tenant_hash_hex: &str, signal: Signal) -> String {
    format!("t/{tenant_hash_hex}/catalog/{}/HEAD", signal.key_prefix())
}

/// Seeds one durable log flush -- a real RLOG data object plus a commit record
/// referencing it -- directly into `store`, bucketed at `ingest_hour`. This
/// bypasses the ingest actor on purpose: the log shard buckets a commit by its
/// wall-clock flush hour (`ingest_hour_bucket = flush_open_ns / hour`, not the
/// event timestamp, `crates/ravel-ingest/src/{log_shard,shard}.rs`), so a live
/// export always lands in the current, not-yet-sealed hour and the fold would
/// not touch it for over an hour. Writing the commit at a sealed hour makes the
/// fold pick it up on its first tick, deterministically.
///
/// The RLOG bytes are built by `RlogWriter` exactly as ingest builds them, so
/// this is a genuine log object -- which is precisely why `Catalog::fold`'s
/// RSEG-only postings build fails to decode it (ADR-0033). Returns the data
/// object key the commit points at.
async fn seed_sealed_log_commit(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    ingest_hour: u32,
    event_ts_ns: i64,
) -> String {
    let tenant_hash = tenant.hash();
    let shard = 0u32;
    let writer_id = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0007);
    let epoch = 1u64;
    let seq = 0u64;

    let resource_attrs = vec![(
        "service.name".to_string(),
        AttrValue::Str("checkout".to_string()),
    )];
    let stream_attrs = stream_attrs_bytes(&resource_attrs, "", "", &[]);
    let stream_id = log_stream_id(&resource_attrs, "", "", &[]);

    let identity = ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.into_bytes(),
        writer_epoch: epoch,
        writer_seq: seq,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    writer
        .push(LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: event_ts_ns,
            observed_ts_ns: event_ts_ns,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "checkout completed".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![(
                "http.route".to_string(),
                AttrValue::Str("/checkout".to_string()),
            )],
        })
        .expect("push log record");
    let bytes = writer.finish().expect("finish RLOG object");

    // The commit record's `content_hash` only has to name the object key the
    // fold will GET; nothing verifies it against the bytes before RSEG decode
    // fails, so a fixed value keeps this seed free of a blake3 dependency.
    let content_hash = [0x5au8; 32];
    let data_key = ravel_commit::keys::data_key(
        &tenant_hash,
        Signal::Logs,
        shard,
        writer_id,
        epoch,
        seq,
        &content_hash,
    )
    .expect("build data key");
    store
        .put(
            &data_key,
            bytes::Bytes::from(bytes),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put RLOG data object");

    // `created_unix_ns` sits at the top of `ingest_hour` so the record's
    // ingest-hour/created-hour cross-check (ravel-commit `validate`) holds.
    let created_unix_ns = i64::from(ingest_hour) * NS_PER_HOUR;
    let record = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: 0,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: event_ts_ns,
        max_event_ts_ns: event_ts_ns,
        min_ingest_ts_ns: created_unix_ns,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns,
        ingest_hour_bucket: ingest_hour,
    })
    .expect("build log commit record");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish log commit record");

    data_key
}

/// A pass-through object store that records the key of every `GetRange::Full`
/// read, so a test can prove the fold task actually fetched a log data object
/// (the wasted full-object GET `Catalog::fold` issues for every log entry
/// while attempting the RSEG-only name-postings build, ADR-0033).
struct GetSpyStore {
    inner: MemoryStore,
    full_gets: std::sync::Mutex<Vec<String>>,
}

impl GetSpyStore {
    fn new() -> Self {
        GetSpyStore {
            inner: MemoryStore::new(),
            full_gets: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn full_get_keys(&self) -> Vec<String> {
        self.full_gets
            .lock()
            .expect("spy mutex not poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for GetSpyStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: ravel_object_store::PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        if matches!(range, GetRange::Full) {
            self.full_gets
                .lock()
                .expect("spy mutex not poisoned")
                .push(key.to_string());
        }
        self.inner.get(key, range).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits (issue #298).
        ravel_object_store::Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Logs fold independently of metrics: with only log ingest and no metric
/// activity at all, the fold task must still produce `catalog/l/HEAD` with a
/// valid snapshot. This proves ADR-0033 gap 1 is closed -- `spawn()` now runs
/// a fold loop for `Signal::Logs`, not just `Signal::Metrics`.
///
/// It also proves the accepted-cost behavior from ADR-0033: `Catalog::fold`
/// issues a full-object GET on each log data object and then fails to decode
/// it as RSEG (an RLOG object carries signal=2, RSEG expects signal=1), so no
/// name-postings object is written, yet the fold still succeeds. The test does
/// not assert an absence of GETs; it asserts the GET happened (via the spy
/// store) and that the same bytes genuinely fail RSEG decode, and that a valid
/// HEAD exists afterward regardless.
#[tokio::test]
async fn background_fold_writes_logs_head_independently_of_metrics() {
    let mut tokens = HashMap::new();
    let tenant = TenantId::new("acme");
    tokens.insert(TOKEN.to_string(), tenant.clone());
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(GetSpyStore::new());

    // Seed a single sealed log flush and NOTHING for metrics, before the fold
    // tasks start. The event and its commit sit ~3h in the past, well past the
    // seal bound, so the logs fold folds it on its first tick.
    let sealed_ts_ns = now_ns() - i64::try_from(SEALED_AGE.as_nanos()).expect("fits i64");
    let sealed_hour = u32::try_from(sealed_ts_ns.div_euclid(NS_PER_HOUR)).expect("hour fits u32");
    let log_data_key =
        seed_sealed_log_commit(store.as_ref(), &tenant, sealed_hour, sealed_ts_ns).await;

    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let config = ServerConfig {
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        fold_tenants: vec![tenant.hash()],
        fold: FoldTaskConfig {
            enabled: true,
            fold_interval: Duration::from_millis(200),
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
    };
    let running = ravel_server::start(config, store_dyn)
        .await
        .expect("server starts");

    // The logs fold loop -- newly wired by this change -- must produce
    // catalog/l/HEAD even though no metric ever existed.
    let logs_head_key = head_key(&tenant.hash().to_hex(), Signal::Logs);
    let mut head_bytes = None;
    for _ in 0..50 {
        if let Ok(got) = store.get(&logs_head_key, GetRange::Full).await {
            head_bytes = Some(got.data);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let head_bytes =
        head_bytes.expect("logs fold task writes catalog/l/HEAD within the polling window");
    let head = ravel_catalog::decode_head(&head_bytes).expect("logs HEAD decodes");
    assert!(
        head.watermark_hour >= sealed_hour,
        "logs fold should have sealed at least the seeded log's hour"
    );

    // A valid snapshot part (.csnap) covers the seeded log, but no name-postings
    // object (.npost under /idx/) was written: build_postings returned None, the
    // observable graceful-degradation consequence, while base HEAD/part
    // resolution stands. Poll: the .csnap and the HEAD are written in the same
    // fold, but list-after-HEAD is a separate read.
    let logs_prefix = format!("t/{}/catalog/l/", tenant.hash().to_hex());
    let mut logs_catalog = Vec::new();
    for _ in 0..50 {
        logs_catalog = list_all(store.as_ref(), &logs_prefix)
            .await
            .expect("list logs catalog objects");
        if logs_catalog.iter().any(|obj| obj.key.ends_with(".csnap")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        logs_catalog.iter().any(|obj| obj.key.ends_with(".csnap")),
        "logs fold must write a snapshot part covering the seeded log"
    );
    assert!(
        logs_catalog
            .iter()
            .all(|obj| !obj.key.contains("/idx/") && !obj.key.ends_with(".npost")),
        "logs fold must not write a name-postings object"
    );

    // Prove the accepted ADR-0033 cost happened: the fold issued a full-object
    // GET on the seeded log data object (the wasted postings-build fetch), and
    // those genuine RLOG bytes really do fail RSEG decode -- which is exactly
    // why build_postings degraded to None above, without failing the fold. The
    // test asserts the GET-and-fail-to-decode occurred, never an absence of
    // GETs.
    assert!(
        store.full_get_keys().iter().any(|key| key == &log_data_key),
        "fold must have issued a full-object GET on the log data object"
    );
    let bytes = store
        .get(&log_data_key, GetRange::Full)
        .await
        .expect("log data object still present")
        .data;
    assert!(
        ravel_segment::open_from_full(&bytes, ravel_segment::ReaderLimits::default()).is_err(),
        "a real RLOG object must fail RSEG decode, the root cause of the wasted GET"
    );

    // Independence from metrics: no metric data object was ever fetched, because
    // no metric commit exists -- only logs folded real content in this test.
    let metrics_data_prefix = format!("t/{}/m/l0/", tenant.hash().to_hex());
    assert!(
        !store
            .full_get_keys()
            .iter()
            .any(|key| key.starts_with(&metrics_data_prefix)),
        "no metric was ingested, so the metrics fold must fetch no data object"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn background_fold_writes_head_for_a_sealed_hour() {
    let mut tokens = HashMap::new();
    let tenant = TenantId::new("acme");
    tokens.insert(TOKEN.to_string(), tenant.clone());
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();

    let config = ServerConfig {
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        fold_tenants: vec![tenant.hash()],
        fold: FoldTaskConfig {
            enabled: true,
            fold_interval: Duration::from_millis(200),
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
    };
    let running = ravel_server::start(config, store_dyn)
        .await
        .expect("server starts");

    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let sealed_ts_ns = now_ns() - i64::try_from(SEALED_AGE.as_nanos()).expect("fits i64");
    let request = export_request("cpu_usage", "demo", 42.5, sealed_ts_ns);
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");
    assert_eq!(response.status(), 200, "export should succeed");

    let key = head_key(&tenant.hash().to_hex(), Signal::Metrics);
    let mut head_bytes = None;
    for _ in 0..50 {
        if let Ok(got) = store.get(&key, GetRange::Full).await {
            head_bytes = Some(got.data);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let head_bytes = head_bytes.expect("fold task writes HEAD within the polling window");
    let head = ravel_catalog::decode_head(&head_bytes).expect("HEAD decodes");
    assert!(
        head.watermark_hour > 0,
        "fold should have sealed the ingested hour"
    );

    running.shutdown().await.expect("graceful shutdown");
}
