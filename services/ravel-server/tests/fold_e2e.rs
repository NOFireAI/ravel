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

// EH-T6 (#744) additions. The multi-part fold acceptance test drives the real
// durable object layout (commit records, compaction records, retention
// tombstones) through the same publish/record/key APIs the ingest flush and
// the maintain compaction/retention tasks use, then exercises the real
// `Catalog::fold` and `Catalog::resolve` entry points the server's fold task
// and query engine call.
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, DEFAULT_MAX_SNAPSHOT_PART_BYTES, PartLimits, SegmentLevel,
    SegmentRef, Snapshot, decode_head, decode_part,
};
use ravel_commit::{keys, signal};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord, RetentionTombstone,
};
use ravel_types::{TenantHash, TimeRange};

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
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: FoldTaskConfig {
            enabled: true,
            fold_interval: Duration::from_millis(200),
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let running = ravel_server::start(
        config,
        store_dyn,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
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
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: FoldTaskConfig {
            enabled: true,
            fold_interval: Duration::from_millis(200),
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let running = ravel_server::start(
        config,
        store_dyn,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    let sealed_ts_ns = now_ns() - i64::try_from(SEALED_AGE.as_nanos()).expect("fits i64");
    let sealed_hour = u32::try_from(sealed_ts_ns.div_euclid(NS_PER_HOUR)).expect("hour fits u32");

    // Storage-derived discovery (ADR-0048 decision 3) only folds a tenant once
    // it has some durable footprint under `t/<hash>/`; the metric export below
    // only ever reaches the ingest shard's in-memory buffer within this test's
    // polling window (`ravel-ingest` flushes on its own age/size timer, not on
    // export), exactly the buffered-flush gap `seed_sealed_log_commit` already
    // works around for the sibling test above. Seeding one sealed log commit
    // first makes the tenant discoverable on fold's very first tick, so the
    // metrics fold loop can run and seal the (otherwise dataless) hour.
    seed_sealed_log_commit(store.as_ref(), &tenant, sealed_hour, sealed_ts_ns).await;

    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
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

// ---- EH-T6 (#744): multi-part parallel fold end-to-end reachability ----
//
// ADR-0063 section 8 (acceptance S3-E2/E3). Drives a tenant's sealed segment
// count past a scaled-down single-part ceiling and asserts, through the real
// `Catalog` fold and resolve entry points, that: the fold produces multiple
// parts under one HEAD; a whole-range query returns exactly what the
// listing-path (never-folded) resolve returns; a late compaction supersession
// and a retention tombstone landing in already-folded hours are reflected
// after a reconcile pass; and every fold step keeps every part under the
// per-part decode cap (no oversized single-part blowup).

/// Seal margin under default catalog config: an ingest hour `H` is sealed once
/// `now >= end(H) + max_flush_lifetime + clock_skew_allowance +
/// fold_safety_margin` (docs/catalog-and-mvcc.md, ADR-0020).
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// A scaled-down single-part ceiling (ADR-0063 section 8: "cap 100 entries"
/// down here to 3): a modest, hour-partitioned segment count crosses it and
/// forces the fold to split into several parts. Splits are always at hour
/// boundaries, so one segment per hour makes each part hold exactly `PART_CAP`
/// hours until the tail.
const PART_CAP: usize = 3;

/// `now_ns` at which ingest hour `hour` has exactly sealed under default
/// margins. Mirrors the catalog fold unit tests' `now_at_seal`, kept local so
/// this e2e test drives `Catalog::fold` with a deterministic injected clock
/// (the crate never reads a wall clock; only the server's fold *task* does).
fn seal_now(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

/// Publish one real durable L0 metric segment into every store in `stores`,
/// bucketed at sealed ingest `hour`, via the exact publish path the ingest
/// shard's flush uses to make a segment durable (`put_data_object` for the
/// data object, then `publish::publish` for the commit record). The payload is
/// an opaque blob rather than a real RSEG: `Catalog::resolve` never decodes
/// segment bytes (it resolves commit records into `SegmentRef`s), and the
/// metrics fold's postings build tolerates an undecodable object by skipping
/// the postings ref, so an opaque payload exercises exactly the fold/resolve
/// paths under test. Returns the commit record so a later compaction can name
/// its writer identity as a superseded input.
async fn seed_metric_segment(
    stores: &[&MemoryStore],
    tenant: &TenantHash,
    shard: u32,
    writer_id: Uuid,
    seq: u64,
    hour: u32,
) -> CommitRecord {
    // Mid-hour keeps `created_hour_bucket == ingest_hour_bucket`, satisfying
    // the commit record's ingest-hour/created-hour cross-check (ravel-commit
    // `validate`).
    let created_unix_ns = i64::from(hour) * NS_PER_HOUR + NS_PER_HOUR / 2;
    let payload = format!("seg-{shard}-{writer_id}-{seq}-{hour}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let record = record::build(NewCommitRecord {
        tenant_hash: *tenant,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket: hour,
    })
    .expect("valid metric commit record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    for store in stores {
        publish::put_data_object(*store, &data_key, bytes::Bytes::from(payload.clone()))
            .await
            .expect("put data object");
        publish::publish(*store, &record, &RetryPolicy::default())
            .await
            .expect("publish commit record");
    }
    record
}

/// Publish a compaction record into `(shard, hour)` that supersedes the L0
/// `inputs` with one L1 part, written at its reconstructed
/// `l1.<hash>.cmt` key into every store, exactly as the maintain compaction
/// task publishes it. Only the record object is written: both the fold and
/// resolve read the compaction record, never the L1 part bytes (they
/// reconstruct the part key from the record).
async fn seed_compaction(
    stores: &[&MemoryStore],
    tenant: &TenantHash,
    shard: u32,
    hour: u32,
    inputs: &[&CommitRecord],
    created_unix_ns: i64,
) -> CompactionRecord {
    let input_ids: Vec<CompactionInputIdentity> = inputs
        .iter()
        .map(|r| CompactionInputIdentity {
            writer_id: r.writer_id.clone(),
            writer_epoch: r.writer_epoch,
            writer_seq: r.writer_seq,
        })
        .collect();
    let mut hasher = blake3::Hasher::new();
    for id in &input_ids {
        hasher.update(id.writer_id.as_bytes());
        hasher.update(&id.writer_epoch.to_le_bytes());
        hasher.update(&id.writer_seq.to_le_bytes());
    }
    let input_set_hash = *hasher.finalize().as_bytes();
    let part_payload = format!("l1-{shard}-{hour}").into_bytes();
    let part_content_hash = *blake3::hash(&part_payload).as_bytes();
    let part = CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xffu8; 16],
        content_hash: part_content_hash.to_vec(),
        object_size: part_payload.len() as u64,
        sample_count: 1,
        series_count: 1,
        run_count: 1,
        min_event_ts_ns: i64::from(hour) * NS_PER_HOUR + 1,
        max_event_ts_ns: i64::from(hour) * NS_PER_HOUR + NS_PER_HOUR / 2,
        segment_format_version: 3,
    };
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics).into(),
        shard,
        ingest_hour_bucket: hour,
        level: 1,
        inputs: input_ids,
        input_set_hash: input_set_hash.to_vec(),
        parts: vec![part],
        created_unix_ns,
    };
    let key = keys::compaction_record_key_for(&record).expect("compaction key");
    for store in stores {
        store
            .put(
                &key,
                bytes::Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put compaction record");
    }
    record
}

/// Publish a retention tombstone for `(shard, hour)` into every store, at its
/// reconstructed `retire.tmb` key, exactly as the maintain retention task
/// writes it. Both fold and resolve classify a bucket as tombstoned from the
/// key shape alone, so a minimal valid record body suffices.
async fn seed_tombstone(stores: &[&MemoryStore], tenant: &TenantHash, shard: u32, hour: u32) {
    let record = RetentionTombstone {
        format_version: 1,
        tenant_hash: tenant.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics).into(),
        shard,
        ingest_hour_bucket: hour,
        retired_at_ns: 0,
        retention_window_ns: 0,
        record_count_observed: 0,
    };
    let key = keys::retention_tombstone_key_for(&record).expect("tombstone key");
    for store in stores {
        store
            .put(
                &key,
                bytes::Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put tombstone");
    }
}

/// The resolved segments of a snapshot, sorted by data-object key, so two
/// resolves computed by different code paths can be compared as sets without
/// depending on internal iteration order.
fn sorted_segments(snap: &Snapshot) -> Vec<SegmentRef> {
    let mut segs = snap.segments.clone();
    segs.sort_by(|a, b| a.data_object_key.cmp(&b.data_object_key));
    segs
}

/// Assert the HEAD's parts are all decodable under the default per-part decode
/// cap (`DEFAULT_MAX_SNAPSHOT_PART_BYTES`, 256 MiB): a successful `decode_part`
/// under `PartLimits::default()` is exactly the resolve-time bound, so this
/// proves no fold step produced an oversized single-part blowup. Returns the
/// part count for the caller's multi-part assertions.
async fn assert_parts_under_decode_cap(store: &MemoryStore, tenant: &TenantHash) -> usize {
    let head_key = head_key(&tenant.to_hex(), Signal::Metrics);
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("HEAD present")
        .data;
    let head = decode_head(&head_bytes).expect("HEAD decodes");
    for part in &head.parts {
        assert!(
            part.size <= DEFAULT_MAX_SNAPSHOT_PART_BYTES,
            "part {} encoded size {} exceeds the decode cap {}",
            part.key,
            part.size,
            DEFAULT_MAX_SNAPSHOT_PART_BYTES
        );
        let part_bytes = store
            .get(&part.key, GetRange::Full)
            .await
            .expect("part object present")
            .data;
        decode_part(&part_bytes, &PartLimits::default()).unwrap_or_else(|err| {
            panic!(
                "part {} must decode under the per-part cap, got {err:?}",
                part.key
            )
        });
    }
    head.parts.len()
}

/// The blake3 of the HEAD part that covers `hour`, or `None` if no part does.
async fn covering_part_blake3(
    store: &MemoryStore,
    tenant: &TenantHash,
    hour: u32,
) -> Option<Vec<u8>> {
    let head_key = head_key(&tenant.to_hex(), Signal::Metrics);
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("HEAD present")
        .data;
    let head = decode_head(&head_bytes).expect("HEAD decodes");
    head.parts
        .iter()
        .find(|p| p.min_hour <= hour && hour <= p.watermark_hour)
        .map(|p| p.blake3.clone())
}

#[tokio::test]
async fn multi_part_ceiling_crossing_folds_queries_and_compacts() {
    let tenant = TenantId::new("eht6-multipart").hash();
    let shard = 0u32;

    // Two independent stores holding byte-identical durable records. `folded`
    // is folded into a multi-part snapshot; `listing` is never folded, so its
    // `Catalog::resolve` always runs the Phase 1 full-listing path. It is the
    // independent oracle for every query assertion below: the listing path
    // enumerates commit/compaction/tombstone records directly, while the
    // folded path unions decoded snapshot-part entries and range-scopes them.
    // Two genuinely different code paths over the same inputs, so their
    // agreeing is a real cross-check, never circular.
    let folded_store = Arc::new(MemoryStore::new());
    let listing_store = Arc::new(MemoryStore::new());
    let both: [&MemoryStore; 2] = [folded_store.as_ref(), listing_store.as_ref()];

    let cfg = CatalogConfig {
        shard_count: 1,
        snapshot_part_max_entries: PART_CAP,
        ..CatalogConfig::default()
    };
    // The folding catalog is long-lived (it runs both folds and holds a warm
    // HEAD/part cache across them, exactly as the server's fold task does).
    // Every *query* resolve, by contrast, uses a fresh catalog built on the
    // same store: object storage is the source of truth and compute is
    // disposable, so a query node reads the current durable HEAD with a cold
    // cache. Without this, a resolve issued microseconds after a fold could
    // serve the pre-fold HEAD from a warm cache (the accepted <=TTL staleness),
    // masking the reconcile under test on a sub-second test clock.
    let folded = Catalog::new(folded_store.clone(), cfg).expect("folding catalog");
    let query_folded = || Catalog::new(folded_store.clone(), cfg).expect("query catalog");
    let listing = Catalog::new(listing_store.clone(), cfg).expect("listing catalog");

    // --- Phase A: drive the sealed segment count past the single-part ceiling.
    // One segment per hour across eight sealed hours [30, 37]. With PART_CAP=3
    // the fold seals parts at hour boundaries: [30,32], [33,35], and a tail.
    const FIRST_HOUR: u32 = 30;
    const LAST_HOUR: u32 = 37;
    let mut records: HashMap<u32, CommitRecord> = HashMap::new();
    for (i, hour) in (FIRST_HOUR..=LAST_HOUR).enumerate() {
        let record =
            seed_metric_segment(&both, &tenant, shard, Uuid::new_v4(), i as u64 + 1, hour).await;
        records.insert(hour, record);
    }

    let whole_range = TimeRange {
        start_ns: 0,
        end_ns: seal_now(LAST_HOUR),
    };

    // Baseline: the listing-path resolve over the whole range, computed on the
    // never-folded store. This is the answer the fold must reproduce exactly.
    let now_a = seal_now(LAST_HOUR);
    let listing_a = listing
        .resolve(&tenant, Signal::Metrics, whole_range, &[], now_a)
        .await
        .expect("listing-path resolve (pre-fold)");
    assert_eq!(
        listing_a.segments.len(),
        (LAST_HOUR - FIRST_HOUR + 1) as usize,
        "one L0 per sealed hour"
    );

    // Fold: crosses the ceiling, producing a multi-part HEAD.
    let report_a = folded
        .fold(&tenant, Signal::Metrics, Uuid::new_v4(), now_a, &[])
        .await
        .expect("first fold");
    assert!(!report_a.no_op, "sealed hours must fold");
    assert_eq!(report_a.entry_count, (LAST_HOUR - FIRST_HOUR + 1) as u64);
    assert!(
        report_a.parts_total >= 2,
        "crossing the ceiling must split into multiple parts, got {}",
        report_a.parts_total
    );

    // Multiple parts, all under one HEAD, all under the per-part decode cap.
    let part_count_a = assert_parts_under_decode_cap(folded_store.as_ref(), &tenant).await;
    assert_eq!(
        part_count_a as u64, report_a.parts_total,
        "HEAD part count matches the fold report"
    );
    assert!(part_count_a >= 2, "HEAD names multiple parts");

    // Query equivalence: the folded (multi-part snapshot) resolve returns
    // exactly the listing-path result.
    let folded_a = query_folded()
        .resolve(&tenant, Signal::Metrics, whole_range, &[], now_a)
        .await
        .expect("folded resolve (post-fold)");
    assert_eq!(
        sorted_segments(&folded_a),
        sorted_segments(&listing_a),
        "multi-part fold resolve must equal the listing-path resolve"
    );

    // --- Phase B: a compaction supersession and a retention tombstone land in
    // hours that Phase A already sealed and folded (hour 32 lives in the first
    // sealed part, hour 34 in the second). Neither is in the tail, so only the
    // reconcile pass -- not the incremental step -- can apply them.
    const COMPACTED_HOUR: u32 = 32;
    const TOMBSTONED_HOUR: u32 = 34;
    let superseded = records
        .get(&COMPACTED_HOUR)
        .expect("hour 32 segment")
        .clone();
    let seg_before = covering_part_blake3(folded_store.as_ref(), &tenant, COMPACTED_HOUR).await;

    seed_compaction(
        &both,
        &tenant,
        shard,
        COMPACTED_HOUR,
        &[&superseded],
        i64::from(COMPACTED_HOUR) * NS_PER_HOUR + NS_PER_HOUR / 2 + 1_000,
    )
    .await;
    seed_tombstone(&both, &tenant, shard, TOMBSTONED_HOUR).await;

    let now_b = seal_now(LAST_HOUR + 2);
    let whole_range_b = TimeRange {
        start_ns: 0,
        end_ns: now_b,
    };

    // The listing oracle now reflects both mutations directly.
    let listing_b = listing
        .resolve(&tenant, Signal::Metrics, whole_range_b, &[], now_b)
        .await
        .expect("listing-path resolve (post-mutation)");

    // Before the reconcile fold, the folded snapshot is deliberately stale:
    // hours <= watermark are served exclusively from the (not-yet-reconciled)
    // parts, so it still shows the pre-compaction L0 and the not-yet-tombstoned
    // hour. This is what makes the post-reconcile agreement below meaningful
    // rather than trivially always-equal.
    let folded_stale = query_folded()
        .resolve(&tenant, Signal::Metrics, whole_range_b, &[], now_b)
        .await
        .expect("folded resolve (pre-reconcile)");
    assert!(
        folded_stale
            .segments
            .iter()
            .any(|s| superseded.content_hash == s.content_hash),
        "pre-reconcile snapshot still names the superseded L0"
    );
    assert!(
        folded_stale
            .segments
            .iter()
            .any(|s| s.ingest_hour_bucket == TOMBSTONED_HOUR),
        "pre-reconcile snapshot still names the soon-to-be-tombstoned hour"
    );
    assert_ne!(
        sorted_segments(&folded_stale),
        sorted_segments(&listing_b),
        "the stale snapshot must differ from the post-mutation listing oracle"
    );

    // Reconcile fold: discovers the late compaction and tombstone within its
    // window and rewrites only the covering sealed parts.
    let report_b = folded
        .fold(&tenant, Signal::Metrics, Uuid::new_v4(), now_b, &[])
        .await
        .expect("reconcile fold");
    assert!(!report_b.no_op, "the watermark advanced, so not a no-op");
    assert!(
        !report_b.rebuilt,
        "an incremental fold with a reconcile pass, not a full rebuild"
    );

    // Every fold step still keeps every part under the decode cap.
    assert!(
        assert_parts_under_decode_cap(folded_store.as_ref(), &tenant).await >= 2,
        "the reconciled HEAD still names multiple parts, all under the cap"
    );

    // The sealed part covering hour 32 was actually rewritten (its content
    // changed), not carried forward by reference.
    let seg_after = covering_part_blake3(folded_store.as_ref(), &tenant, COMPACTED_HOUR).await;
    assert!(
        seg_before.is_some() && seg_after.is_some() && seg_before != seg_after,
        "the reconcile pass must re-PUT the sealed part covering the compacted hour"
    );

    // Query equivalence holds again after reconcile: folded snapshot resolve
    // equals the listing-path oracle, both now reflecting the mutations.
    let folded_b = query_folded()
        .resolve(&tenant, Signal::Metrics, whole_range_b, &[], now_b)
        .await
        .expect("folded resolve (post-reconcile)");
    assert_eq!(
        sorted_segments(&folded_b),
        sorted_segments(&listing_b),
        "post-reconcile fold resolve must equal the listing-path resolve"
    );

    // And the specific supersession and tombstone facts, read straight off the
    // folded snapshot resolve:
    assert!(
        !folded_b
            .segments
            .iter()
            .any(|s| superseded.content_hash == s.content_hash),
        "the superseded L0 must be gone from the reconciled snapshot"
    );
    let compacted: Vec<&SegmentRef> = folded_b
        .segments
        .iter()
        .filter(|s| s.ingest_hour_bucket == COMPACTED_HOUR)
        .collect();
    assert_eq!(compacted.len(), 1, "hour 32 now holds exactly the L1 part");
    assert!(
        matches!(compacted[0].level, SegmentLevel::L1 { .. }),
        "hour 32's surviving segment is the compaction L1 part"
    );
    assert!(
        !folded_b
            .segments
            .iter()
            .any(|s| s.ingest_hour_bucket == TOMBSTONED_HOUR),
        "the tombstoned hour must contribute nothing to the reconciled snapshot"
    );
}
