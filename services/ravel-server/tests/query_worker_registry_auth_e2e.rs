//! Query-worker registry authentication (ADR-1918) across two real
//! `ravel_server::start`ed processes over one shared `MemoryStore`.
//!
//! Worker A and coordinator C share a fragment key, so each admits the other's
//! signed heartbeat record. The test then writes an unsigned record at a fresh
//! UUID F, chosen with `worker_set::owner` so that F would own the queried
//! unit if C admitted it, pointing at a plain TCP listener. C's heartbeat reads
//! the live set after the injection, and a PromQL query on C that only A can
//! answer (C's store refuses every `.rseg` read) must succeed without a single
//! connection reaching the forged endpoint, while C's `/metrics` counts the
//! forged record under `reason="missing_mac"`.
//!
//! The wait is event-driven: C's store holds every listing of the registry
//! prefix shut until the forged record is written, so C's first heartbeat
//! read (which runs at startup, not one heartbeat interval later) is the one
//! that sees it, and the test waits until C has read both sibling records.
//!
//! Both processes run the same start path, so a start path that derived its
//! record keys from something other than the fragment key file would still
//! let A and C admit each other. The test therefore also reads the shared
//! store with its own `QueryWorkers`, keyed by `RecordKeys::from_file_keys`
//! over the fixture's fragment key alone, and requires it to admit both A's
//! and C's records. Remove the MAC check, and C admits F and dials the
//! listener.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/fragment_tls.rs"]
mod fragment_tls;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_fleet::query_workers::{
    QUERY_WORKERS_PREFIX, QueryWorkerRecord, QueryWorkers, RecordKeys, RecordReject,
    query_worker_key,
};
use ravel_fleet::worker_set;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::distrib::partition::DistribThresholds;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::config::DistribSettings;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const METRIC: &str = "m";
const FRAGMENT_KEY: [u8; 32] = [0x5a; 32];
const SQL_TICKET_KEY: [u8; 32] = [0x5c; 32];
const SHARD_COUNT: u32 = 16;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

/// How long each event-driven wait may take before the test fails. Every
/// awaited event happens at startup or right after a release, so this only
/// bounds scheduling of a few spawned tasks.
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

/// Publish one RSEG segment and its commit record for `tenant` on `shard`.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    shard: u32,
    base_ns: i64,
) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: vec![
            Sample {
                ts_ns: base_ns,
                value: 1.0,
            },
            Sample {
                ts_ns: base_ns + 3 * NS_PER_MIN,
                value: 2.5,
            },
        ],
    }];

    let writer_id = Uuid::from_u128(4_000);
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
}

fn distrib_settings() -> DistribSettings {
    DistribSettings {
        fragment_keys: vec![FRAGMENT_KEY],
        sql_ticket_keys: Some(vec![SQL_TICKET_KEY]),
        max_inflight_fragments: 32,
        max_inflight_federated_resolves: 8,
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
        fragment_listener: fragment_tls::listener_settings(),
        advertise_endpoint: None,
    }
}

async fn start_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
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
        shard_count: SHARD_COUNT,
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
        distrib: Some(distrib_settings()),
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
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// The coordinator's view of the shared store. It refuses every `.rseg` read,
/// so only a remote worker can answer a query; it holds every listing of the
/// registry prefix shut while armed; and it records which registry objects
/// were written and read.
struct CoordinatorStore {
    inner: Arc<MemoryStore>,
    armed: AtomicBool,
    blocked: AtomicUsize,
    release: tokio::sync::Notify,
    registry_puts: Mutex<Vec<String>>,
    registry_gets: Mutex<HashMap<String, usize>>,
}

impl CoordinatorStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
            blocked: AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
            registry_puts: Mutex::new(Vec::new()),
            registry_gets: Mutex::new(HashMap::new()),
        }
    }

    fn blocked(&self) -> usize {
        self.blocked.load(Ordering::SeqCst)
    }

    fn release_all(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.release.notify_waiters();
    }

    fn registry_puts(&self) -> Vec<String> {
        self.registry_puts.lock().clone()
    }

    fn has_read(&self, key: &str) -> bool {
        self.reads_of(key) > 0
    }

    fn reads_of(&self, key: &str) -> usize {
        self.registry_gets.lock().get(key).copied().unwrap_or(0)
    }

    async fn gate(&self, prefix: &str) {
        if !prefix.starts_with(QUERY_WORKERS_PREFIX) {
            return;
        }
        loop {
            if !self.armed.load(Ordering::SeqCst) {
                return;
            }
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.armed.load(Ordering::SeqCst) {
                return;
            }
            self.blocked.fetch_add(1, Ordering::SeqCst);
            notified.await;
            self.blocked.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for CoordinatorStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        if key.starts_with(QUERY_WORKERS_PREFIX) {
            self.registry_puts.lock().push(key.to_string());
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if key.ends_with(".rseg") {
            return Err(StoreError::AccessDenied(
                "the coordinator never reads segment data in this test".to_string(),
            ));
        }
        let outcome = self.inner.get(key, range).await;
        if key.starts_with(QUERY_WORKERS_PREFIX) {
            *self
                .registry_gets
                .lock()
                .entry(key.to_string())
                .or_default() += 1;
        }
        outcome
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
        self.gate(prefix).await;
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.gate(prefix).await;
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Poll `ready` until it holds, failing the test after [`DEADLINE`].
async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

fn process_id_of(key: &str) -> Uuid {
    key.strip_prefix(QUERY_WORKERS_PREFIX)
        .and_then(|id| Uuid::parse_str(id).ok())
        .unwrap_or_else(|| panic!("{key} is a query-worker record key"))
}

async fn registry_ids(store: &MemoryStore) -> Vec<Uuid> {
    ravel_object_store::list_all(store, QUERY_WORKERS_PREFIX)
        .await
        .expect("list registry")
        .iter()
        .map(|meta| process_id_of(&meta.key))
        .collect()
}

/// One `query_range` on `base`; `Ok` with the body on 200, `Err` with the
/// status and body otherwise.
async fn query_range(base: &str, start: i64, end: i64) -> Result<serde_json::Value, String> {
    let response = reqwest::Client::new()
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", METRIC.to_string()),
            ("start", start.to_string()),
            ("end", end.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("query request completes");
    let code = response.status();
    let body = response.text().await.expect("response body");
    if code == 200 {
        Ok(serde_json::from_str(&body).expect("response body is JSON"))
    } else {
        Err(format!("{code}: {body}"))
    }
}

async fn scrape_metrics(base: &str) -> String {
    reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// The value of the one `ravel_query_worker_record_rejects_total` sample whose
/// label set carries `reason="<reason>"`.
fn record_rejects(metrics: &str, reason: &str) -> f64 {
    let label = format!("reason=\"{reason}\"");
    let samples: Vec<f64> = metrics
        .lines()
        .filter(|line| line.starts_with("ravel_query_worker_record_rejects_total{"))
        .filter(|line| line.contains(&label))
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("unparseable sample: {line}"))
        })
        .collect();
    assert_eq!(
        samples.len(),
        1,
        "exactly one {label} sample in:\n{metrics}"
    );
    samples[0]
}

#[tokio::test]
async fn forged_unsigned_registry_record_is_never_dialed() {
    let shared = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let tenant_hash = tenant.hash();

    // Worker A: a pass-through view of the shared store. Its first heartbeat
    // writes its signed record at startup.
    let worker = start_server(shared.clone()).await;
    let a = tokio::time::timeout(DEADLINE, async {
        loop {
            if let [a] = registry_ids(&shared).await[..] {
                break a;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed out waiting for the worker's registry record");

    // Coordinator C: its first heartbeat writes its record and then parks on
    // the registry listing until the forged record is in place.
    let coordinator_store = Arc::new(CoordinatorStore::new(shared.clone()));
    let coordinator = start_server(coordinator_store.clone()).await;
    wait_for("the coordinator's record and parked listing", || {
        !coordinator_store.registry_puts().is_empty() && coordinator_store.blocked() > 0
    })
    .await;
    let c = process_id_of(&coordinator_store.registry_puts()[0]);
    assert_ne!(a, c);

    // A shard whose unit A owns between A and C, so a correct coordinator
    // dispatches it to A; and a forged id F that would own it among A, C, F.
    let (shard, unit) = (0..SHARD_COUNT)
        .map(|shard| {
            (
                shard,
                worker_set::unit_key(&tenant_hash, Signal::Metrics, shard),
            )
        })
        .find(|(_, unit)| worker_set::owner(unit, &[a, c]) == Some(a))
        .expect("A owns some shard's unit against C");
    let forged = std::iter::repeat_with(Uuid::new_v4)
        .find(|f| worker_set::owner(&unit, &[a, c, *f]) == Some(*f))
        .expect("a forged id that owns the unit");

    let now = now_ns();
    publish_segment(shared.as_ref(), &tenant, shard, now - 10 * NS_PER_MIN).await;

    // The forged endpoint: a plain TCP listener that counts every connection.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind forged endpoint");
    let forged_endpoint = listener.local_addr().expect("listener addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let accept_task = tokio::spawn({
        let accepted = accepted.clone();
        async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        }
    });

    // A self-consistent record with no MAC, as a writer holding only the
    // prefix's PutObject grant would write it.
    let forged_record = QueryWorkerRecord {
        process_id: forged.to_string(),
        fragment_endpoint: forged_endpoint.to_string(),
        protocol_version: ravel_query::distrib::codec::PROTOCOL_VERSION,
        started_unix_ns: now,
    };
    let forged_key = query_worker_key(&forged_record.process_id);
    shared
        .put(
            &forged_key,
            serde_json::to_vec(&forged_record)
                .expect("encode record")
                .into(),
            PutOptions::default(),
        )
        .await
        .expect("put forged record");

    // Release the listing: C's first live-set read now sees A and F.
    coordinator_store.release_all();
    let a_key = query_worker_key(&a.to_string());
    wait_for("the coordinator to read both sibling records", || {
        coordinator_store.has_read(&a_key) && coordinator_store.has_read(&forged_key)
    })
    .await;

    // A reader keyed by the fixture's fragment key file and nothing else
    // admits both processes' records, so the start path signs under keys
    // derived from that file. It rejects the forged record as missing_mac.
    let file_keyed = QueryWorkers::with_defaults(
        "127.0.0.1:1",
        ravel_query::distrib::codec::PROTOCOL_VERSION,
        RecordKeys::from_file_keys(&[FRAGMENT_KEY]).expect("one key"),
    );
    let live: Vec<String> = file_keyed
        .live_set(shared.as_ref(), now_ns())
        .await
        .expect("file-keyed live set")
        .into_iter()
        .map(|record| record.process_id)
        .collect();
    assert!(
        live.contains(&a.to_string()),
        "a reader keyed by the fragment key file admits A's record: {live:?}"
    );
    assert!(
        live.contains(&c.to_string()),
        "a reader keyed by the fragment key file admits C's record: {live:?}"
    );
    assert!(!live.contains(&forged.to_string()));
    assert_eq!(
        file_keyed.record_rejects().by_reason(),
        RecordReject::ALL.map(|reason| (reason, u64::from(reason == RecordReject::MissingMac))),
        "the file-keyed reader rejects only the forged record"
    );

    // Only A can answer: C refuses its own segment reads. Two queries, so a
    // live set published just after the first one routes the second.
    let (start_s, end_s) = ((now - 15 * NS_PER_MIN) / NS_PER_SEC, now / NS_PER_SEC);
    let base = format!("http://{}", coordinator.http_addr);
    for round in 0..2 {
        let mut last_error = String::new();
        let body = tokio::time::timeout(DEADLINE, async {
            loop {
                match query_range(&base, start_s, end_s).await {
                    Ok(body) => break body,
                    Err(error) => last_error = error,
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("query round {round} never succeeded through worker A: {last_error}")
        });
        let series = body["data"]["result"]
            .as_array()
            .expect("matrix result")
            .len();
        assert_eq!(series, 1, "round {round}: the one series, from worker A");
    }

    assert_eq!(
        accepted.load(Ordering::SeqCst),
        0,
        "the forged endpoint must never be dialed"
    );
    // C's heartbeat reads the live set once at startup and then sleeps a
    // full heartbeat interval (60 s), so the one read the test released is
    // the only one: it fetched the forged record once and counted it once.
    assert_eq!(
        coordinator_store.reads_of(&forged_key),
        1,
        "exactly one coordinator live-set read fetched the forged record"
    );
    let metrics = scrape_metrics(&base).await;
    assert_eq!(
        record_rejects(&metrics, "missing_mac"),
        1.0,
        "the forged record is counted once, as missing_mac"
    );
    assert_eq!(
        record_rejects(&metrics, "bad_mac"),
        0.0,
        "A's record verifies at C"
    );

    accept_task.abort();
    coordinator
        .shutdown()
        .await
        .expect("coordinator shuts down");
    worker.shutdown().await.expect("worker shuts down");
}
