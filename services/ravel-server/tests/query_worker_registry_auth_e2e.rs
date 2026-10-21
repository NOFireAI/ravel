//! ADR-1918 through the server's own startup path: `Cli` flags,
//! `Cli::parse_distrib_settings`, and `ravel_server::start`, for two processes
//! that read one `--fragment-key-file` and find each other through the real
//! query-worker heartbeat under `sys/query/workers/`.
//!
//! The worker starts first. A forged record, self-consistent JSON with no MAC
//! (what a query-role principal without the key file can PUT), is then written
//! at a fresh UUID whose `fragment_endpoint` is a TCP listener this test owns.
//! The coordinator starts after that, so its first heartbeat read lists the
//! forged record. The data sits on the one shard whose rendezvous ranking puts
//! the forged UUID first over all three ids and the worker ahead of the
//! coordinator, so a coordinator that admitted the forged record would dial
//! the listener first for the query's only slice.
//!
//! The coordinator's store refuses every GET of the published data object, so
//! the query succeeds only when the worker, in the other process, answered the
//! slice; that proves the two processes admitted each other's signed records.
//! The listener must have accepted no connection, and the coordinator's real
//! `/metrics` output must count the forged record under `missing_mac`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/fragment_tls.rs"]
mod fragment_tls;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_fleet::query_workers::{QUERY_WORKERS_PREFIX, QueryWorkerRecord, query_worker_key};
use ravel_fleet::worker_set;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::config::{Cli, DistribSettings};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const METRIC: &str = "cpu";
const FRAGMENT_KEY: &str = "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";
const SQL_TICKET_KEY: &str = "5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c";
/// Enough shards that one of them ranks the forged id first and the worker
/// ahead of the coordinator (a 1-in-6 chance per shard).
const SHARDS: u32 = 64;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

/// How long each wait on the other process gets. The first heartbeat of each
/// process writes its record and reads the live set immediately, so this is
/// only the time for spawned tasks to run.
const DEADLINE: Duration = Duration::from_secs(20);

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

fn temp_file(contents: &str) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("temp file");
    std::fs::write(file.path(), contents).expect("write");
    file
}

/// The distributed settings a real `ravel-server` process builds from its
/// flags, reading `fragment_key_file`, with the cost gate forced open.
fn distrib_settings(fragment_key_file: &tempfile::NamedTempFile) -> DistribSettings {
    let sql_keys = temp_file(&format!("{SQL_TICKET_KEY}\n"));
    let cert = temp_file(fragment_tls::TEST_FRAGMENT_CERT_PEM);
    let key = temp_file(fragment_tls::TEST_FRAGMENT_KEY_PEM);
    let ca = temp_file(fragment_tls::TEST_FRAGMENT_CA_PEM);
    let cli = Cli::try_parse_from([
        "ravel-server",
        "--mode",
        "all",
        "--listen-http",
        "127.0.0.1:8080",
        "--listen-grpc",
        "127.0.0.1:4317",
        "--distributed-query",
        "--fragment-key-file",
        fragment_key_file.path().to_str().expect("utf8"),
        "--sql-ticket-key-file",
        sql_keys.path().to_str().expect("utf8"),
        "--fragment-listener",
        "127.0.0.1:0",
        "--fragment-tls-cert",
        cert.path().to_str().expect("utf8"),
        "--fragment-tls-key",
        key.path().to_str().expect("utf8"),
        "--fragment-tls-ca",
        ca.path().to_str().expect("utf8"),
        "--distribute-bytes-threshold",
        "0",
        "--distribute-segments-threshold",
        "0",
    ])
    .expect("flags parse");
    cli.validate().expect("flags validate");
    cli.parse_distrib_settings()
        .expect("distributed settings parse")
        .expect("--distributed-query is on")
}

/// A store over one shared [`MemoryStore`] that counts GETs of the published
/// data object and, when `refuse` is set, fails them. The data key is set once
/// the test has chosen the shard to publish on.
struct DataStore {
    inner: Arc<MemoryStore>,
    data_key: Arc<OnceLock<String>>,
    refuse: bool,
    data_gets: AtomicUsize,
}

impl DataStore {
    fn new(inner: Arc<MemoryStore>, data_key: Arc<OnceLock<String>>, refuse: bool) -> Arc<Self> {
        Arc::new(DataStore {
            inner,
            data_key,
            refuse,
            data_gets: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for DataStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if self.data_key.get().is_some_and(|data| data == key) {
            self.data_gets.fetch_add(1, Ordering::SeqCst);
            if self.refuse {
                return Err(StoreError::AccessDenied(format!(
                    "the coordinator may not read {key}"
                )));
            }
        }
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
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

/// Publish one segment of [`METRIC`] on `shard` with `samples` and return its
/// data key.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    shard: u32,
    samples: &[(i64, f64)],
) -> String {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: samples
            .iter()
            .map(|&(ts_ns, value)| Sample { ts_ns, value })
            .collect(),
    }];
    let writer_id = Uuid::from_u128(5_000 + u128::from(shard));
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");
    let base_ns = samples[0].0;
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: base_ns + 4 * NS_PER_MIN,
        ingest_hour_bucket: u32::try_from(base_ns / NS_PER_HOUR).expect("hour bucket fits u32"),
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    data_key
}

async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    distrib: DistribSettings,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: SHARDS,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: Some(distrib),
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// The process id of every key under `sys/query/workers/`.
async fn registered_ids(store: &MemoryStore) -> Vec<Uuid> {
    list_all(store, QUERY_WORKERS_PREFIX)
        .await
        .expect("list query workers")
        .into_iter()
        .filter_map(|meta| {
            meta.key
                .strip_prefix(QUERY_WORKERS_PREFIX)
                .and_then(|id| Uuid::parse_str(id).ok())
        })
        .collect()
}

/// Wait until the shared store holds `count` query-worker records.
async fn await_records(store: &MemoryStore, count: usize) -> Vec<Uuid> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let ids = registered_ids(store).await;
        if ids.len() >= count {
            return ids;
        }
        assert!(
            Instant::now() < deadline,
            "{count} query-worker records never appeared within {DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One `query_range` over HTTP; the error carries the status and body so the
/// caller can retry while the coordinator's roster fills.
async fn query_range(http: std::net::SocketAddr, start: i64, end: i64) -> Result<String, String> {
    let response = reqwest::Client::new()
        .get(format!("http://{http}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", METRIC.to_string()),
            ("start", start.to_string()),
            ("end", end.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .map_err(|e| format!("send: {e}"))?;
    let status = response.status();
    let body = response.text().await.map_err(|e| format!("body: {e}"))?;
    if status != 200 {
        return Err(format!("{status}: {body}"));
    }
    Ok(body)
}

async fn scrape_metrics(http: std::net::SocketAddr) -> String {
    reqwest::Client::new()
        .get(format!("http://{http}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// The value of the one sample line that starts with `series`.
fn sample(metrics: &str, series: &str) -> u64 {
    let lines: Vec<&str> = metrics
        .lines()
        .filter(|line| line.starts_with(series))
        .collect();
    assert_eq!(lines.len(), 1, "exactly one `{series}` sample:\n{metrics}");
    lines[0]
        .rsplit(' ')
        .next()
        .and_then(|value| value.parse().ok())
        .expect("sample value is an integer")
}

/// Acceptance (ADR-1918): two processes started through `ravel_server::start`
/// from one fragment key file admit each other, and a forged unsigned record
/// that would own the query's only slice is never dialed and is counted under
/// `missing_mac` on the coordinator's `/metrics`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forged_query_worker_record_is_never_dialed_and_signed_siblings_still_meet() {
    let shared = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let fragment_keys = temp_file(&format!("# cluster fragment key\n{FRAGMENT_KEY}\n"));
    let data_key = Arc::new(OnceLock::new());

    // The worker, then the forged record, then the coordinator.
    let worker_store = DataStore::new(shared.clone(), data_key.clone(), false);
    let worker = start_server(worker_store.clone(), distrib_settings(&fragment_keys)).await;
    let worker_id = await_records(&shared, 1).await[0];

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the forged endpoint");
    let forged_endpoint = listener.local_addr().expect("listener address");
    let accepted = Arc::new(AtomicUsize::new(0));
    let accept_task = {
        let accepted = accepted.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                drop(socket);
            }
        })
    };
    let forged_id = Uuid::new_v4();
    let forged = QueryWorkerRecord {
        process_id: forged_id.to_string(),
        fragment_endpoint: forged_endpoint.to_string(),
        protocol_version: ravel_query::distrib::codec::PROTOCOL_VERSION,
        started_unix_ns: now_ns(),
    };
    shared
        .put(
            &query_worker_key(&forged.process_id),
            forged.encode().expect("encode forged").into(),
            PutOptions::default(),
        )
        .await
        .expect("put forged record");

    let coordinator_store = DataStore::new(shared.clone(), data_key.clone(), true);
    let coordinator =
        start_server(coordinator_store.clone(), distrib_settings(&fragment_keys)).await;
    let ids = await_records(&shared, 3).await;
    let coordinator_id = ids
        .iter()
        .copied()
        .find(|id| *id != worker_id && *id != forged_id)
        .expect("the coordinator's record");

    // The shard whose unit the forged id would win over all three, with the
    // worker ahead of the coordinator once the forged id is out.
    let shard = (0..SHARDS)
        .find(|&shard| {
            let unit = worker_set::unit_key(&tenant.hash(), Signal::Metrics, shard);
            worker_set::owner(&unit, &[coordinator_id, worker_id, forged_id]) == Some(forged_id)
                && worker_set::owner(&unit, &[coordinator_id, worker_id]) == Some(worker_id)
        })
        .expect("some shard ranks the forged id first and the worker second");
    let base = now_ns() - 10 * NS_PER_MIN;
    let published = publish_segment(
        shared.as_ref(),
        &tenant,
        shard,
        &[(base, 1.5), (base + 3 * NS_PER_MIN, 2.5)],
    )
    .await;
    data_key.set(published).expect("data key set once");
    let (start, end) = ((base - 5 * NS_PER_MIN) / NS_PER_SEC, now_ns() / NS_PER_SEC);

    // The coordinator cannot read the data, so the query succeeds only once its
    // heartbeat read has admitted the worker and the slice went there. That
    // read came after the forged record was written.
    let deadline = Instant::now() + DEADLINE;
    let body = loop {
        match query_range(coordinator.http_addr, start, end).await {
            Ok(body) => break body,
            Err(last) if Instant::now() >= deadline => panic!(
                "the cross-process query never succeeded within {DEADLINE:?}; last error: {last}"
            ),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    assert!(
        body.contains("\"1.5\"") && body.contains("\"2.5\""),
        "both samples come back: {body}"
    );
    assert!(
        worker_store.data_gets.load(Ordering::SeqCst) > 0,
        "the worker process read the segment"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        0,
        "the coordinator dialed the forged record's endpoint"
    );

    let metrics = scrape_metrics(coordinator.http_addr).await;
    assert!(
        sample(
            &metrics,
            "ravel_query_worker_record_rejects_total{mode=\"all\",reason=\"missing_mac\"} "
        ) > 0,
        "the forged record is counted under missing_mac:\n{metrics}"
    );
    assert_eq!(
        sample(
            &metrics,
            "ravel_query_worker_record_rejects_total{mode=\"all\",reason=\"bad_mac\"} "
        ),
        0,
        "the worker's signed record verified:\n{metrics}"
    );

    accept_task.abort();
    coordinator
        .shutdown()
        .await
        .expect("coordinator shuts down");
    worker.shutdown().await.expect("worker shuts down");
}
