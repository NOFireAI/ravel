//! ADR-3130 reachability: two real `Mode::Maintain` processes built by
//! [`ravel_server::start`], keyed from a `--maintain-key-file` parsed by the
//! production `Cli`, admit each other's signed registry records and refuse an
//! unsigned one.
//!
//! The forged records are well-formed `WorkerHeartbeat`s with no MAC: one at
//! format version 2, and one at version 1, the unsigned shape every pre-3130
//! writer produced. Each sits at a fresh process id chosen so that, were it
//! admitted, it would be the rendezvous owner of the one unit holding data.
//! The test refreshes them on every poll, so they never age out of the
//! liveness window and every registry read in the window sees them.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use prost::Message;
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_fleet::record_auth::RecordReject;
use ravel_maintain::worker_set::{
    MaintainMembership, MaintainRecordKeys, WorkerSet, heartbeat_key, owner, unit_key,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, list_all};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{Cli, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const SHARD_COUNT: u32 = 8;
/// A 3 s liveness window: wide against scheduler delay, short enough that
/// convergence costs a few seconds.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const WORKERS_LIVE: &str = "ravel_maintain_workers_live{mode=\"maintain\"}";
const UNITS_OWNED: &str = "ravel_maintain_units_owned{mode=\"maintain\"}";
const MISSING_MAC: &str =
    "ravel_maintain_worker_record_rejects_total{mode=\"maintain\",reason=\"missing_mac\"}";

/// The maintain record keys exactly as `main` reads them: `--maintain-key-file`
/// through `Cli`, validated, then parsed.
fn keys_from_cli(path: &str) -> MaintainRecordKeys {
    let cli = Cli::try_parse_from([
        "ravel-server",
        "--mode",
        "maintain",
        "--maintain-key-file",
        path,
    ])
    .expect("flags parse");
    cli.validate().expect("maintain with a key file validates");
    cli.parse_maintain_record_keys()
        .expect("key file parses")
        .expect("the flag yields keys")
}

fn maintain_config(tenant: &TenantId, keys: MaintainRecordKeys) -> ServerConfig {
    ServerConfig {
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
        mode: Mode::Maintain,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: SHARD_COUNT,
        tenant_resolver: ravel_server::tenant::build_resolver(Default::default(), false),
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: ravel_server::FoldTaskConfig::default(),
        maintain: ravel_server::MaintenanceTaskConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            shard_count: SHARD_COUNT,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            worker_record_keys: Some(keys),
            ..ravel_server::MaintenanceTaskConfig::default()
        },
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
        scrub_period: Duration::from_secs(60),
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
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// One segment and its commit record in `(tenant, Metrics, shard 0)`, so tenant
/// discovery finds the tenant and that unit holds data. Returns the data key.
async fn publish_segment(store: &MemoryStore, tenant: &TenantId) -> String {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(7);
    let created_unix_ns = 500_000 * NS_PER_HOUR;
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "cpu".to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant, "cpu", &labels).expect("series id");
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let (min_ingest_ts_ns, max_ingest_ts_ns) = (created_unix_ns - 1_000, created_unix_ns);
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: created_unix_ns,
                value: 1.0,
            }],
        }],
        identity,
        IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        },
    )
    .expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns,
        max_ingest_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket: 500_000,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit record");
    data_key
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos() as i64
}

/// The trailing integer of the one line starting with `line_prefix`.
fn sample_value(body: &str, line_prefix: &str) -> Option<u64> {
    body.lines()
        .find(|line| line.starts_with(line_prefix))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|v| v.parse::<u64>().ok())
}

async fn scrape(client: &reqwest::Client, base: &str) -> String {
    client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("scrape /metrics")
        .text()
        .await
        .expect("metrics body")
}

/// The process ids every record under the registry prefix is keyed by.
async fn registered(store: &MemoryStore) -> BTreeSet<Uuid> {
    list_all(store, "sys/maintain/workers/")
        .await
        .expect("list registry")
        .iter()
        .filter_map(|meta| meta.key.rsplit('/').next()?.parse().ok())
        .collect()
}

/// A well-formed record at `format_version` for `forged`, stamped now, with no
/// MAC.
async fn put_unsigned(store: &MemoryStore, forged: Uuid, format_version: u32) {
    let now = now_ns();
    let unsigned = ravel_proto::sys::v1::WorkerHeartbeat {
        format_version,
        process_id: forged.as_bytes().to_vec(),
        started_unix_ns: now,
        heartbeat_unix_ns: now,
        mac: Vec::new(),
    };
    store
        .put(
            &heartbeat_key(&forged),
            unsigned.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::Overwrite,
                checksum: None,
            },
        )
        .await
        .expect("put unsigned record");
}

/// A fresh process id that would be the rendezvous owner of `unit` if it
/// joined `members`.
fn fresh_owner_of(unit: &[u8], members: &BTreeSet<Uuid>) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        let mut with: Vec<Uuid> = members.iter().copied().collect();
        with.push(candidate);
        if owner(unit, &with) == Some(candidate) {
            return candidate;
        }
    }
}

/// How many of `tenant`'s maintained units each of `live` owns, sorted.
fn unit_split(tenant: &TenantId, live: &BTreeSet<Uuid>) -> Vec<u64> {
    let members: Vec<Uuid> = live.iter().copied().collect();
    let mut split: Vec<u64> = members
        .iter()
        .map(|id| {
            let mut owned = 0;
            for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
                for shard in 0..SHARD_COUNT {
                    if owner(&unit_key(&tenant.hash(), signal, shard), &members) == Some(*id) {
                        owned += 1;
                    }
                }
            }
            owned
        })
        .collect();
    split.sort_unstable();
    split
}

/// Fails with the heartbeat task reading the registry without verifying it, or
/// admitting a version-1 record unsigned as every reader before ADR-3130 did:
/// a forged member is admitted, `workers_live` reads more than 2 within one
/// heartbeat tick, and the unit split moves. Scrub's own read feeds neither
/// gauge; the next test covers it. On the dispatch ref it does not build:
/// `Cli` has no `--maintain-key-file` and the reject family does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsigned_registry_record_is_refused_by_both_maintain_processes() {
    let tenant = TenantId::new("registry-auth-e2e");
    let store = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), &tenant).await;

    let key_file = tempfile::NamedTempFile::new().expect("temp key file");
    std::fs::write(key_file.path(), format!("{}\n", "5a".repeat(32))).expect("write key");
    let key_path = key_file.path().to_str().expect("utf8");

    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let start = |keys| {
        ravel_server::start(
            maintain_config(&tenant, keys),
            backend.clone(),
            backend.clone(),
            Arc::new(ravel_object_store::StoreMetrics::default()),
            None,
        )
    };
    let a = start(keys_from_cli(key_path))
        .await
        .expect("worker a starts");
    let b = start(keys_from_cli(key_path))
        .await
        .expect("worker b starts");
    let client = reqwest::Client::new();
    let bases = [
        format!("http://{}", a.http_addr),
        format!("http://{}", b.http_addr),
    ];

    // Each admits the other: both report two live workers.
    let mut converged = false;
    for _ in 0..100 {
        let mut live = Vec::new();
        for base in &bases {
            live.push(sample_value(&scrape(&client, base).await, WORKERS_LIVE));
        }
        if live == [Some(2), Some(2)] {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        converged,
        "the two maintain processes admit each other within 20 s"
    );

    // A reader keyed by the same file admits both real records.
    let real = registered(store.as_ref()).await;
    assert_eq!(real.len(), 2, "two real registry records: {real:?}");
    let reader = WorkerSet::with_defaults(now_ns());
    let reader_membership = MaintainMembership::new(keys_from_cli(key_path));
    let read = reader
        .live_set(store.as_ref(), &reader_membership, now_ns())
        .await
        .expect("live set reads");
    let mut expected: BTreeSet<Uuid> = real.clone();
    expected.insert(reader.process_id());
    assert_eq!(read.iter().copied().collect::<BTreeSet<_>>(), expected);

    // Fresh ids that would each own the data unit if admitted.
    let data_unit = unit_key(&tenant.hash(), Signal::Metrics, 0);
    let forged = fresh_owner_of(&data_unit, &real);
    let forged_v1 = fresh_owner_of(&data_unit, &real);
    let expected_split = unit_split(&tenant, &real);

    // Both processes count the forged records as missing_mac and never admit
    // them. Every scrape in the window must read two live workers.
    let mut counted_at = None;
    let mut settled = false;
    let mut owned = Vec::new();
    for i in 0..150 {
        put_unsigned(store.as_ref(), forged, 2).await;
        put_unsigned(store.as_ref(), forged_v1, 1).await;
        let mut bodies = Vec::new();
        for base in &bases {
            bodies.push(scrape(&client, base).await);
        }
        for (name, body) in ["a", "b"].iter().zip(&bodies) {
            assert_eq!(
                sample_value(body, WORKERS_LIVE),
                Some(2),
                "worker {name} admitted the unsigned record at iteration {i}"
            );
        }
        // Each heartbeat read sees both forged records, so two per read.
        let counted = bodies
            .iter()
            .all(|body| sample_value(body, MISSING_MAC).is_some_and(|n| n >= 2));
        if counted && counted_at.is_none() {
            counted_at = Some(i);
        }
        // Three seconds past the count (three heartbeat ticks and maintenance
        // intervals), the units split between the two real processes exactly
        // as rendezvous over their two ids alone assigns them.
        if counted_at.is_some_and(|at| i >= at + 15) {
            owned = bodies
                .iter()
                .map(|body| sample_value(body, UNITS_OWNED).unwrap_or(0))
                .collect();
            owned.sort_unstable();
            if owned == expected_split {
                settled = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        counted_at.is_some(),
        "both processes count both unsigned records as missing_mac within 30 s"
    );
    assert!(
        settled,
        "units_owned {owned:?} never matched the two-member split {expected_split:?} \
         while the forged records were fresh"
    );

    // The test-side reader refuses both the same way.
    put_unsigned(store.as_ref(), forged, 2).await;
    put_unsigned(store.as_ref(), forged_v1, 1).await;
    let read = reader
        .live_set(store.as_ref(), &reader_membership, now_ns())
        .await
        .expect("live set reads");
    assert!(!read.contains(&forged), "the reader admitted {forged}");
    assert!(
        !read.contains(&forged_v1),
        "the reader admitted {forged_v1}"
    );
    assert_eq!(read.len(), 3, "the reader admitted {read:?}");
    assert_eq!(
        reader_membership
            .record_rejects()
            .get(RecordReject::MissingMac),
        2
    );

    a.shutdown().await.expect("graceful shutdown a");
    b.shutdown().await.expect("graceful shutdown b");
}

/// A pass-through store that records which tokio task issued each PUT under
/// the registry prefix and each GET of one watched key. The heartbeat task
/// writes the process's record and reads the registry from the same task, so
/// a watched read from any task that never wrote a record is another reader's.
struct RegistrySpyStore {
    inner: MemoryStore,
    writers: Mutex<Vec<Option<tokio::task::Id>>>,
    watched: Mutex<Option<String>>,
    watched_reads: Mutex<Vec<Option<tokio::task::Id>>>,
}

impl RegistrySpyStore {
    fn new() -> Self {
        RegistrySpyStore {
            inner: MemoryStore::new(),
            writers: Mutex::new(Vec::new()),
            watched: Mutex::new(None),
            watched_reads: Mutex::new(Vec::new()),
        }
    }

    fn watch(&self, key: String) {
        *self.watched.lock().expect("spy mutex") = Some(key);
    }

    fn writers(&self) -> Vec<Option<tokio::task::Id>> {
        self.writers.lock().expect("spy mutex").clone()
    }

    /// Reads of the watched key as (from a registry-writing task, from any
    /// other task).
    fn watched_reads(&self) -> (u64, u64) {
        let writers = self.writers();
        let reads = self.watched_reads.lock().expect("spy mutex");
        let from_writer = reads.iter().filter(|id| writers.contains(id)).count() as u64;
        (from_writer, reads.len() as u64 - from_writer)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RegistrySpyStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        if key.starts_with("sys/maintain/workers/") {
            self.writers
                .lock()
                .expect("spy mutex")
                .push(tokio::task::try_id());
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        if self.watched.lock().expect("spy mutex").as_deref() == Some(key) {
            self.watched_reads
                .lock()
                .expect("spy mutex")
                .push(tokio::task::try_id());
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
        // double inherits.
        ravel_object_store::Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// `missing_mac` and the spy's watched-read counts, taken while no watched
/// read lands between the two spy reads around the scrape, and only once the
/// counter has caught up with the reads (a reject is counted after its GET
/// returns). `None` when that never holds within 5 s.
async fn settled_counts(
    spy: &RegistrySpyStore,
    client: &reqwest::Client,
    base: &str,
) -> Option<(u64, u64, u64)> {
    for _ in 0..50 {
        let before = spy.watched_reads();
        let missing_mac = sample_value(&scrape(client, base).await, MISSING_MAC).unwrap_or(0);
        if spy.watched_reads() == before && missing_mac == before.0 + before.1 {
            return Some((before.0, before.1, missing_mac));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Scrub gates its shards on its own live-set read, separate from the
/// heartbeat task's. A lone maintain process with a corrupted object in the
/// data unit, and an unsigned record at an id that would own that unit if
/// admitted: scrub reads that record itself and counts it as `missing_mac`,
/// still scrubs the unit, and reports the corruption.
///
/// The two readers share one reject counter, so the test tells them apart at
/// the store: the spy tags each GET of the forged key with its tokio task, and
/// the heartbeat task is the one that wrote the registry record. With a 60 s
/// heartbeat it makes one pass in the window, and every `missing_mac` past
/// that pass is one of scrub's reads.
///
/// Fails with scrub admitting the record unverified while the heartbeat task
/// still verifies: `missing_mac` stops matching the reads, scrub cedes the
/// unit, and the mismatch never surfaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scrub_does_not_cede_a_unit_to_an_unsigned_registry_record() {
    let tenant = TenantId::new("registry-auth-scrub-e2e");
    let spy = Arc::new(RegistrySpyStore::new());
    let store = &spy.inner;
    let data_key = publish_segment(store, &tenant).await;
    let existing = store
        .get(&data_key, GetRange::Full)
        .await
        .expect("get object");
    let mut corrupted = existing.data.to_vec();
    corrupted[0] ^= 0x01;
    store
        .put(&data_key, corrupted.into(), PutOptions::default())
        .await
        .expect("overwrite corrupted object");

    let key_file = tempfile::NamedTempFile::new().expect("temp key file");
    std::fs::write(key_file.path(), format!("{}\n", "5a".repeat(32))).expect("write key");
    let mut config = maintain_config(
        &tenant,
        keys_from_cli(key_file.path().to_str().expect("utf8")),
    );
    // One shard, scrubbed on a 1 s tick; the maintenance loop only heartbeats
    // inside this test's window.
    config.shard_count = 1;
    config.maintain.shard_count = 1;
    config.maintain.interval = Duration::from_secs(3600);
    config.maintain.heartbeat_interval = Duration::from_secs(60);
    config.scrub_period = Duration::from_secs(1);
    let backend: Arc<dyn ObjectStoreBackend> = spy.clone();
    let running = ravel_server::start(
        config,
        backend.clone(),
        backend,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("maintain server starts");

    let mut own = BTreeSet::new();
    for _ in 0..100 {
        own = registered(store).await;
        if !own.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(own.len(), 1, "the process registers within 500 ms");
    let data_unit = unit_key(&tenant.hash(), Signal::Metrics, 0);
    let forged = fresh_owner_of(&data_unit, &own);
    spy.watch(heartbeat_key(&forged));
    put_unsigned(store, forged, 2).await;

    // Scrub has read the forged record at least once, and kept the unit.
    let client = reqwest::Client::new();
    let base = format!("http://{}", running.http_addr);
    let mismatch =
        "ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"metrics\",level=\"l0\"}";
    let mut detected = false;
    for _ in 0..60 {
        let body = scrape(&client, &base).await;
        if sample_value(&body, mismatch).is_some_and(|n| n >= 1) && spy.watched_reads().1 >= 1 {
            detected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        detected,
        "scrub must read the forged record, keep the data unit, and report its corruption, \
         within 12 s"
    );

    // One registry write, so one heartbeat pass, and every read of the forged
    // record, from either task, counted once as missing_mac.
    let writers = spy.writers();
    assert_eq!(writers.len(), 1, "one heartbeat write in the window");
    assert!(writers[0].is_some(), "the heartbeat write ran in a task");
    let (heartbeat_reads, scrub_reads, missing_mac) = settled_counts(&spy, &client, &base)
        .await
        .unwrap_or_else(|| {
            panic!(
                "missing_mac never matched the forged record's reads {:?}",
                spy.watched_reads()
            )
        });
    assert!(heartbeat_reads <= 1, "{heartbeat_reads} heartbeat reads");
    assert!(scrub_reads >= 1, "{scrub_reads} scrub reads");

    // The next scrub tick alone moves missing_mac, by exactly its reads.
    let mut next = false;
    for _ in 0..50 {
        if spy.watched_reads().1 > scrub_reads {
            next = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(next, "scrub reads the forged record again within 5 s");
    let (heartbeat_after, scrub_after, missing_mac_after) = settled_counts(&spy, &client, &base)
        .await
        .unwrap_or_else(|| {
            panic!(
                "missing_mac never matched the forged record's reads {:?}",
                spy.watched_reads()
            )
        });
    assert_eq!(heartbeat_after, heartbeat_reads, "no second heartbeat pass");
    assert_eq!(
        missing_mac_after - missing_mac,
        scrub_after - scrub_reads,
        "missing_mac grows by exactly scrub's reads of the forged record"
    );

    running.shutdown().await.expect("graceful shutdown");
}
