//! ADR-1693: the scheduled catalog fold runs in the maintain role, partitioned
//! by the maintain live set.
//!
//! The acceptance test drives two maintain processes over one `MemoryStore`
//! with a live set of two and one tick each, and asserts the exact partition:
//! every `(tenant, metrics)` pair is folded exactly once, and for a pair it
//! does not own a process issues no store request at all. The only request a
//! tick makes that does not name a pair the process owns is the one
//! `LISTD t/` that discovers which tenants exist, which is per signal and per
//! tick rather than per tenant; the test asserts that exact remaining list.
//!
//! The remaining tests cover the hand-over bound the ADR states (a pair moves
//! to a new owner when the previous owner leaves the live set), the wiring of
//! the scheduled fold in a running server (a maintain process folds the live
//! set its heartbeat publishes, an `all` process folds every unit), and the
//! two modes that stop folding on a schedule.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, decode_head,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::worker_set::{DEFAULT_LIVENESS_FACTOR, DEFAULT_UNIT_CONCURRENCY};
use ravel_maintain::{FixedClock, RetentionConfig, WorkerSet};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_server::fold::{self, FOLD_UNIT_SHARD, FoldTaskConfig};
use ravel_server::{Mode, ServerConfig};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const TOKEN: &str = "testtoken";

/// Seal margin under default catalog config (docs/catalog-and-mvcc.md): an
/// ingest hour `H` is sealed once
/// `now >= end(H) + max_flush_lifetime + clock_skew_allowance +
/// fold_safety_margin`.
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// The sealed ingest hour every seeded tenant holds one segment in.
const SEALED_HOUR: u32 = 40;

/// `now_ns` at which [`SEALED_HOUR`] has exactly sealed.
const SEAL_NOW_NS: i64 = (SEALED_HOUR as i64 + 1) * NS_PER_HOUR + MARGIN_NS;

/// The heartbeat interval `H` both workers run on.
const HEARTBEAT: Duration = Duration::from_secs(60);

/// One fold tick's freshness interval. A HEAD younger than this is skipped, so
/// the second tick over an already-folded pair is a peek and nothing more.
const FOLD_INTERVAL: Duration = Duration::from_secs(300);

/// Tenants seeded for the acceptance test. Six is enough that the rendezvous
/// hash splits them across two process ids without either side being empty,
/// which the test asserts rather than assumes.
const TENANTS: [&str; 6] = [
    "fold-own-alpha",
    "fold-own-bravo",
    "fold-own-charlie",
    "fold-own-delta",
    "fold-own-echo",
    "fold-own-foxtrot",
];

/// Fixed process ids so rendezvous ownership is a pure function of the test's
/// own inputs (`WorkerSet::with_process_id`'s stated purpose): a fresh UUID per
/// run would make the asserted partition change run to run.
const PROCESS_A: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00a1);
const PROCESS_B: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00b2);

/// A pass-through store that records every request it makes of the shared
/// inner store, as `"<OP> <key>"`. One per simulated process, all over the same
/// `MemoryStore`, so "what did this process ask the store for" is exactly
/// answerable per process.
struct RequestLogStore {
    inner: Arc<MemoryStore>,
    requests: Mutex<Vec<String>>,
}

impl RequestLogStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        RequestLogStore {
            inner,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, entry: String) {
        self.requests
            .lock()
            .expect("request log mutex not poisoned")
            .push(entry);
    }

    fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("request log mutex not poisoned")
            .clone()
    }

    /// Every recorded request whose key names `tenant`, in order.
    fn requests_naming(&self, tenant: &TenantHash) -> Vec<String> {
        let needle = format!("t/{}/", tenant.to_hex());
        self.requests()
            .into_iter()
            .filter(|entry| entry.contains(&needle))
            .collect()
    }

    fn clear(&self) {
        self.requests
            .lock()
            .expect("request log mutex not poisoned")
            .clear();
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RequestLogStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.record(format!("PUT {key}"));
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.record(format!("GET {key}"));
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.record(format!("HEAD {key}"));
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.record(format!("LIST {prefix}"));
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.record(format!("LISTD {prefix}"));
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.record(format!("DELETE {key}"));
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Publish one durable L0 metric segment for `tenant` into the sealed hour,
/// through the exact publish path the ingest shard's flush uses. The payload is
/// opaque: the fold resolves commit records into entries and never decodes
/// segment bytes, and its postings build tolerates an undecodable object.
async fn seed_sealed_metric_segment(store: &MemoryStore, tenant: &TenantHash) {
    // Mid-hour keeps `created_hour_bucket == ingest_hour_bucket`, satisfying
    // the commit record's cross-check (ravel-commit `validate`).
    let created_unix_ns = i64::from(SEALED_HOUR) * NS_PER_HOUR + NS_PER_HOUR / 2;
    let payload = format!("seg-{}", tenant.to_hex()).into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let commit = record::build(NewCommitRecord {
        tenant_hash: *tenant,
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0007),
        writer_epoch: 1,
        writer_seq: 1,
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
        ingest_hour_bucket: SEALED_HOUR,
    })
    .expect("valid metric commit record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, bytes::Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish commit record");
}

fn catalog_config() -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        ..CatalogConfig::default()
    }
}

/// One simulated maintain process: its own request-logging view of the shared
/// store, its own `Catalog` (a process's catalog cache is per process), and its
/// own `WorkerSet` identity.
struct Process {
    store: Arc<RequestLogStore>,
    catalog: Catalog,
    worker: WorkerSet,
    folder_id: Uuid,
}

impl Process {
    fn new(inner: &Arc<MemoryStore>, process_id: Uuid) -> Self {
        let store = Arc::new(RequestLogStore::new(inner.clone()));
        let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
        let catalog = Catalog::new(store_dyn, catalog_config()).expect("catalog builds");
        let worker = WorkerSet::new(
            SEAL_NOW_NS,
            HEARTBEAT,
            DEFAULT_LIVENESS_FACTOR,
            DEFAULT_UNIT_CONCURRENCY,
        )
        .with_process_id(process_id);
        Process {
            store,
            catalog,
            worker,
            folder_id: process_id,
        }
    }

    /// One scheduled fold tick for `Signal::Metrics` under `live_set`, at
    /// `now_ns` on an injected clock.
    async fn tick(&self, live_set: &[Uuid], now_ns: i64) -> fold::FoldTickReport {
        let retention = RetentionConfig::default();
        let clock = FixedClock::new(now_ns);
        fold::run_tick(
            &self.catalog,
            self.store.as_ref(),
            Signal::Metrics,
            None,
            self.folder_id,
            FOLD_INTERVAL,
            &retention,
            &self.worker,
            live_set,
            &clock,
        )
        .await
        .expect("tenant discovery succeeds")
    }
}

/// Sorted hex tenant hashes, so two partitions computed by different code
/// paths compare as sets without depending on iteration order.
fn sorted_hex(tenants: &[TenantHash]) -> Vec<String> {
    let mut hex: Vec<String> = tenants.iter().map(|t| t.to_hex()).collect();
    hex.sort();
    hex
}

/// THE ADR-1693 acceptance test. Two maintain processes, one live set of two,
/// one tick each: every `(tenant, metrics)` pair folds exactly once, and the
/// non-owner of a pair issues no request naming that tenant at all.
#[tokio::test]
async fn two_maintain_workers_fold_each_unit_exactly_once_and_the_non_owner_issues_no_requests() {
    let inner = Arc::new(MemoryStore::new());
    let tenants: Vec<TenantHash> = TENANTS.iter().map(|t| TenantId::new(*t).hash()).collect();
    for tenant in &tenants {
        seed_sealed_metric_segment(inner.as_ref(), tenant).await;
    }

    let a = Process::new(&inner, PROCESS_A);
    let b = Process::new(&inner, PROCESS_B);
    let live_set = vec![PROCESS_A, PROCESS_B];

    // The partition the rendezvous hash defines, computed independently of the
    // fold through `WorkerSet::owns_unit` on shard 0 of each pair. This is the
    // expectation the tick must reproduce, not a reading of what it did.
    let expected_a: Vec<TenantHash> = tenants
        .iter()
        .copied()
        .filter(|t| a.worker.owns_unit(&live_set, t, Signal::Metrics, 0))
        .collect();
    let expected_b: Vec<TenantHash> = tenants
        .iter()
        .copied()
        .filter(|t| b.worker.owns_unit(&live_set, t, Signal::Metrics, 0))
        .collect();
    assert_eq!(
        expected_a.len() + expected_b.len(),
        TENANTS.len(),
        "rendezvous ownership is total: every pair has exactly one owner"
    );
    assert!(
        !expected_a.is_empty() && !expected_b.is_empty(),
        "the fixed process ids must split the tenants, got {} and {}",
        expected_a.len(),
        expected_b.len()
    );

    let report_a = a.tick(&live_set, SEAL_NOW_NS).await;
    let report_b = b.tick(&live_set, SEAL_NOW_NS).await;

    // Both processes discover every tenant: discovery is per-process and never
    // gated on ownership (ADR-0065 decision 2). The lifecycle restriction runs
    // only over the pairs a process owns, so `maintained` counts those and
    // nothing else.
    assert_eq!(report_a.discovered, TENANTS.len());
    assert_eq!(report_b.discovered, TENANTS.len());
    assert_eq!(report_a.maintained, expected_a.len());
    assert_eq!(report_b.maintained, expected_b.len());
    assert_eq!(report_a.excluded, 0);
    assert_eq!(report_b.excluded, 0);

    // Each process folded exactly the pairs it owns, and nothing else.
    assert_eq!(sorted_hex(&report_a.owned), sorted_hex(&expected_a));
    assert_eq!(sorted_hex(&report_b.owned), sorted_hex(&expected_b));
    assert_eq!(sorted_hex(&report_a.folded), sorted_hex(&expected_a));
    assert_eq!(sorted_hex(&report_b.folded), sorted_hex(&expected_b));
    assert_eq!(report_a.failed, Vec::<TenantHash>::new());
    assert_eq!(report_b.failed, Vec::<TenantHash>::new());
    assert_eq!(report_a.skipped_fresh, Vec::<TenantHash>::new());
    assert_eq!(report_b.skipped_fresh, Vec::<TenantHash>::new());

    // Exactly once across the fleet: the two folded sets are disjoint and their
    // union is every tenant.
    let mut folded_all: Vec<TenantHash> = report_a.folded.clone();
    folded_all.extend(report_b.folded.iter().copied());
    assert_eq!(
        sorted_hex(&folded_all),
        sorted_hex(&tenants),
        "every (tenant, metrics) pair is folded exactly once across the two processes"
    );

    // And the durable result: every tenant has a HEAD whose watermark covers
    // the sealed hour, written by exactly one folder id.
    let mut folder_ids: HashMap<String, Uuid> = HashMap::new();
    for tenant in &tenants {
        let key = format!("t/{}/catalog/m/HEAD", tenant.to_hex());
        let bytes = inner
            .get(&key, GetRange::Full)
            .await
            .expect("every tenant has a folded HEAD")
            .data;
        let head = decode_head(&bytes).expect("HEAD decodes");
        assert_eq!(
            head.watermark_hour, SEALED_HOUR,
            "the fold sealed exactly the seeded hour"
        );
        let folder = Uuid::from_slice(&head.folder_id).expect("folder id is a uuid");
        folder_ids.insert(tenant.to_hex(), folder);
    }
    for tenant in &expected_a {
        assert_eq!(
            folder_ids.get(&tenant.to_hex()),
            Some(&PROCESS_A),
            "a pair owned by A carries A's folder id"
        );
    }
    for tenant in &expected_b {
        assert_eq!(
            folder_ids.get(&tenant.to_hex()),
            Some(&PROCESS_B),
            "a pair owned by B carries B's folder id"
        );
    }

    // The cost claim, counted on the instrumented store. For a pair it does not
    // own, a process issues no request naming that tenant: no lifecycle config
    // read, no `catalog/m/HEAD` peek, no LIST of the commit prefix, no PUT.
    for (process, store, owned) in [("A", &a.store, &expected_a), ("B", &b.store, &expected_b)] {
        for tenant in &tenants {
            if owned.contains(tenant) {
                continue;
            }
            assert_eq!(
                store.requests_naming(tenant),
                Vec::<String>::new(),
                "process {process} must issue no request at all for a pair it does not own"
            );
        }

        // And the exact remaining list: every other request the tick made is
        // the single delimited listing that discovers which tenants exist,
        // which is per signal and per tick, not per tenant.
        let owned_hex: Vec<String> = owned.iter().map(|t| format!("t/{}/", t.to_hex())).collect();
        let unowned_requests: Vec<String> = store
            .requests()
            .into_iter()
            .filter(|entry| !owned_hex.iter().any(|prefix| entry.contains(prefix)))
            .collect();
        assert_eq!(
            unowned_requests,
            vec!["LISTD t/".to_string()],
            "process {process}'s only request that does not name a pair it owns is the \
             per-tick tenant discovery listing"
        );
    }

    // The owner, by contrast, really did pay for the fold: the HEAD peek, the
    // commit-prefix LIST, and the part and HEAD PUTs.
    let owner_requests = a.store.requests_naming(&expected_a[0]);
    let owned_hex = expected_a[0].to_hex();
    assert!(
        owner_requests.contains(&format!("GET t/{owned_hex}/catalog/m/HEAD")),
        "the owner peeks at HEAD before folding, got {owner_requests:?}"
    );
    assert!(
        owner_requests
            .iter()
            .any(|entry| entry.starts_with(&format!("LIST t/{owned_hex}/m/"))),
        "the owner lists the pair's commit prefix, got {owner_requests:?}"
    );
    assert!(
        owner_requests
            .iter()
            .any(|entry| entry == &format!("PUT t/{owned_hex}/catalog/m/HEAD")),
        "the owner publishes the new HEAD, got {owner_requests:?}"
    );
}

/// ADR-1693's hand-over bound, made observable: a pair moves to a new owner
/// when the previous owner leaves the live set, with no coordination beyond
/// membership. The overlap the ADR bounds at `3 * H` plus one heartbeat is the
/// window in which both processes still see the departed peer; here the live
/// set has already converged, which is the state after that window.
#[tokio::test]
async fn a_pair_hands_over_to_the_surviving_worker_when_its_owner_leaves_the_live_set() {
    let inner = Arc::new(MemoryStore::new());
    let tenants: Vec<TenantHash> = TENANTS.iter().map(|t| TenantId::new(*t).hash()).collect();
    for tenant in &tenants {
        seed_sealed_metric_segment(inner.as_ref(), tenant).await;
    }

    let a = Process::new(&inner, PROCESS_A);
    let b = Process::new(&inner, PROCESS_B);
    let both = vec![PROCESS_A, PROCESS_B];

    // Round 1, both live: each folds only what it owns.
    let round1_a = a.tick(&both, SEAL_NOW_NS).await;
    let round1_b = b.tick(&both, SEAL_NOW_NS).await;
    let a_owned = round1_a.folded.clone();
    assert!(!a_owned.is_empty(), "A owns at least one pair");
    assert_eq!(
        round1_b.folded.len(),
        TENANTS.len() - a_owned.len(),
        "B folds exactly the rest"
    );

    // A leaves. B's next live-set read returns `{B}` alone, so B owns every
    // pair, including the ones A was folding.
    let solo = vec![PROCESS_B];
    for tenant in &a_owned {
        assert!(
            b.worker.owns_unit(&solo, tenant, Signal::Metrics, 0),
            "once A is gone, B owns every pair A held"
        );
    }

    // A new sealed hour arrives, so the pairs A used to fold have work again.
    // B's tick must pick them all up, with no hand-over protocol beyond the
    // live set it already reads.
    b.store.clear();
    let later_ns = SEAL_NOW_NS + NS_PER_HOUR;
    let round2_b = b.tick(&solo, later_ns).await;
    assert_eq!(
        sorted_hex(&round2_b.owned),
        sorted_hex(&tenants),
        "B owns every pair once it is the only live worker"
    );
    assert_eq!(
        round2_b.failed,
        Vec::<TenantHash>::new(),
        "no hand-over failure: the HEADs A published are B's starting point"
    );
    assert_eq!(
        sorted_hex(&round2_b.folded),
        sorted_hex(&tenants),
        "B folds every pair, its own and the ones it took over"
    );
    // Every pair A previously folded is now folded by B, from A's HEAD: the
    // HEAD CAS is the only serialization the hand-over needs.
    for tenant in &a_owned {
        assert!(
            round2_b.folded.contains(tenant),
            "B takes over a pair A used to fold"
        );
        let key = format!("t/{}/catalog/m/HEAD", tenant.to_hex());
        let bytes = inner
            .get(&key, GetRange::Full)
            .await
            .expect("HEAD present")
            .data;
        let head = decode_head(&bytes).expect("HEAD decodes");
        assert_eq!(
            Uuid::from_slice(&head.folder_id).expect("folder id is a uuid"),
            PROCESS_B,
            "the handed-over pair's HEAD now carries B's folder id"
        );
    }
}

/// A test server over an instrumented store, so "did the scheduled fold run"
/// is answered by the requests the process made rather than by a timer.
fn test_config(mode: Mode, tenant: &TenantId, fold_interval: Duration) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant.clone());
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: FoldTaskConfig {
            enabled: true,
            fold_interval,
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
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

/// A foreign maintain worker that only ever exists as a heartbeat record: the
/// live set a running server computes has to contain it, and the partition has
/// to move accordingly.
const FOREIGN_WORKER: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0f01);

/// Real wall-clock nanoseconds. The heartbeat a running server writes and
/// judges liveness against is on the wall clock (`maintain::WallClock`), so a
/// heartbeat seeded for it must be too.
fn wall_now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos(),
    )
    .expect("wall clock fits in i64 nanoseconds")
}

/// Publish one heartbeat for `process_id`, dated now, through the same
/// `WorkerSet` path a real maintain process uses. With the default 60s
/// heartbeat interval the liveness window is 180s, so this record stays live
/// for the whole of a test.
async fn seed_worker_heartbeat(store: &MemoryStore, process_id: Uuid) {
    let now_ns = wall_now_ns();
    WorkerSet::new(
        now_ns,
        HEARTBEAT,
        DEFAULT_LIVENESS_FACTOR,
        DEFAULT_UNIT_CONCURRENCY,
    )
    .with_process_id(process_id)
    .write_heartbeat(store, now_ns)
    .await
    .expect("seed a foreign worker heartbeat");
}

/// Every process id with a heartbeat record, once there are `expected` of them.
/// A running maintain process mints its own id, so this is how a test learns
/// which id the partition it is about to assert is keyed on.
async fn worker_ids(store: &MemoryStore, expected: usize) -> Vec<Uuid> {
    for _ in 0..200 {
        let page = store
            .list("sys/maintain/workers/", None)
            .await
            .expect("list the workers prefix");
        if page.objects.len() == expected {
            return page
                .objects
                .iter()
                .map(|meta| {
                    let raw = meta
                        .key
                        .strip_prefix("sys/maintain/workers/")
                        .expect("a workers-prefix key");
                    Uuid::parse_str(raw).expect("the heartbeat key names a uuid")
                })
                .collect();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no {expected} heartbeat records appeared under sys/maintain/workers/");
}

/// The ceiling on every bounded wait below. Generous, because the figure that
/// matters is the state each wait names, not how long it took to appear: a
/// healthy run reaches it in well under a second and a loaded machine is
/// allowed to be slow. Passing this is a failure, never a flake to retry.
const WAIT_BUDGET: Duration = Duration::from_secs(30);

/// How often a bounded wait re-reads the state it is waiting on.
const WAIT_POLL: Duration = Duration::from_millis(25);

/// One `/metrics` scrape of a running server.
async fn scrape(running: &ravel_server::Running) -> String {
    reqwest::Client::new()
        .get(format!("http://{}/metrics", running.http_addr))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// Waits until this maintain server's own heartbeat task has computed and
/// published a live set of `expected` workers.
///
/// `ravel_maintain_workers_live` is set from the computed set on the statement
/// before the one that publishes it on the `watch` channel the fold reads, so
/// a scrape reporting `expected` proves the fold's next tick partitions
/// against that set. That is the state the test needs; a heartbeat RECORD
/// existing in the store is a weaker fact, since it is written before the
/// listing that computes the set.
///
/// Bounded and driven by that state rather than by a fixed sleep: a sleep
/// sized to a healthy machine asserts nothing the state had not already
/// reached, and flakes on a loaded one.
async fn wait_for_published_live_set(running: &ravel_server::Running, expected: usize) {
    let want = format!("ravel_maintain_workers_live{{mode=\"maintain\"}} {expected}");
    let deadline = std::time::Instant::now() + WAIT_BUDGET;
    loop {
        if scrape(running).await.lines().any(|line| line == want) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAIT_BUDGET:?} waiting for the heartbeat task to publish a live \
             set of {expected} workers (no `{want}` sample)"
        );
        tokio::time::sleep(WAIT_POLL).await;
    }
}

/// Waits until every named tenant's metrics `HEAD` exists, the durable state a
/// completed fold of that pair leaves behind. Bounded, and a timeout names the
/// tenants still missing rather than failing later on a bare `get` error.
async fn wait_for_folded_metrics_heads(store: &MemoryStore, tenants: &[TenantHash]) {
    let deadline = std::time::Instant::now() + WAIT_BUDGET;
    loop {
        let mut missing = Vec::new();
        for tenant in tenants {
            let key = format!("t/{}/catalog/m/HEAD", tenant.to_hex());
            if store.get(&key, GetRange::Full).await.is_err() {
                missing.push(tenant.to_hex());
            }
        }
        if missing.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAIT_BUDGET:?} waiting for the scheduled fold to publish a HEAD \
             for every pair this process owns; still unfolded: {missing:?}"
        );
        tokio::time::sleep(WAIT_POLL).await;
    }
}

/// Assert a HEAD a running server's scheduled fold published covers exactly
/// the one segment [`seed_sealed_metric_segment`] seeded, and return its
/// folder id.
///
/// The watermark is only bounded below: unlike the [`fold::run_tick`] tests
/// above, a running server folds on the wall clock, so its sealed frontier is
/// the current hour rather than [`SEALED_HOUR`]. What is exact is the work the
/// fold captured, which is the one seeded commit.
fn assert_folded_the_seeded_segment(head_bytes: &[u8]) -> Uuid {
    let head = decode_head(head_bytes).expect("HEAD decodes");
    assert_eq!(
        head.parts.len(),
        1,
        "one part covers the one seeded segment"
    );
    assert_eq!(
        head.parts[0].entry_count, 1,
        "the one seeded commit is the one folded entry"
    );
    assert!(
        head.watermark_hour >= SEALED_HOUR,
        "the wall-clock sealed frontier covers the seeded hour, got {}",
        head.watermark_hour
    );
    Uuid::from_slice(&head.folder_id).expect("folder id is a uuid")
}

/// A `Mode::Maintain` server whose heartbeat runs and whose scheduled fold
/// ticks fast, with no tenant restriction (an empty `fold_tenants` means every
/// discovered tenant is maintained). The maintenance sweep interval is long
/// enough that only the fold acts within a test.
fn maintain_test_config(tenant: &TenantId, fold_interval: Duration) -> ServerConfig {
    let mut config = test_config(Mode::Maintain, tenant, fold_interval);
    config.fold_tenants = Vec::new();
    config.maintain = ravel_server::MaintenanceTaskConfig {
        enabled: true,
        interval: Duration::from_secs(3_600),
        shard_count: 1,
        heartbeat_interval: HEARTBEAT,
        ..Default::default()
    };
    config
}

/// ADR-1693 decision 1, wired end to end rather than by calling [`fold::run_tick`]
/// with a hand-made live set: a running `Mode::Maintain` server folds exactly
/// the pairs it owns under the live set its OWN heartbeat loop published, and
/// leaves the rest to the peer whose heartbeat it read.
///
/// The tenants are chosen after the server's process id is known, so both
/// sides of the partition are non-empty by construction rather than by luck.
/// Every tenant left unfolded here is one this process WOULD own under its
/// solo set, which is what makes this a test of the published live set and not
/// of ownership in general.
#[tokio::test]
async fn a_maintain_server_folds_the_partition_its_published_live_set_defines() {
    let inner = Arc::new(MemoryStore::new());
    seed_worker_heartbeat(inner.as_ref(), FOREIGN_WORKER).await;

    let store_dyn: Arc<dyn ObjectStoreBackend> = inner.clone();
    let config = maintain_test_config(&TenantId::new("fold-maintain"), Duration::from_millis(250));
    let running = ravel_server::start(
        config,
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    // The server's own id, learned from the heartbeat it just wrote. The
    // record exists before the listing that computes the live set, so the
    // wait below is on the published set rather than on the record.
    let ids = worker_ids(inner.as_ref(), 2).await;
    let own = *ids
        .iter()
        .find(|id| **id != FOREIGN_WORKER)
        .expect("the server minted its own process id");
    wait_for_published_live_set(&running, 2).await;

    let worker = WorkerSet::new(
        wall_now_ns(),
        HEARTBEAT,
        DEFAULT_LIVENESS_FACTOR,
        DEFAULT_UNIT_CONCURRENCY,
    )
    .with_process_id(own);
    let live_set = vec![own, FOREIGN_WORKER];

    // Three tenants on each side of the rendezvous split, walked in a fixed
    // order so the choice is a pure function of the server's own id.
    let mut mine: Vec<TenantHash> = Vec::new();
    let mut theirs: Vec<TenantHash> = Vec::new();
    for i in 0..1_000 {
        if mine.len() == 3 && theirs.len() == 3 {
            break;
        }
        let tenant = TenantId::new(format!("fold-live-{i}")).hash();
        if worker.owns_unit(&live_set, &tenant, Signal::Metrics, FOLD_UNIT_SHARD) {
            if mine.len() < 3 {
                mine.push(tenant);
            }
        } else if theirs.len() < 3 {
            theirs.push(tenant);
        }
    }
    assert_eq!(mine.len(), 3, "three tenants this process owns");
    assert_eq!(theirs.len(), 3, "three tenants the foreign worker owns");
    for tenant in &theirs {
        assert!(
            worker.owns_unit(&[own], tenant, Signal::Metrics, FOLD_UNIT_SHARD),
            "a process that ignored the published live set would fold this pair too"
        );
    }

    for tenant in mine.iter().chain(theirs.iter()) {
        seed_sealed_metric_segment(inner.as_ref(), tenant).await;
    }
    // Every pair this process owns has been folded. The pairs the foreign
    // worker owns are asserted unfolded below, and they stay so for as long as
    // this process runs: ownership is decided before any per-tenant read, so
    // there is no later moment at which this process would fold one.
    wait_for_folded_metrics_heads(inner.as_ref(), &mine).await;
    running.shutdown().await.expect("graceful shutdown");

    let mut folders: Vec<Uuid> = Vec::new();
    for tenant in &mine {
        let key = format!("t/{}/catalog/m/HEAD", tenant.to_hex());
        let bytes = inner
            .get(&key, GetRange::Full)
            .await
            .unwrap_or_else(|err| panic!("the maintain server folds a pair it owns: {err}"))
            .data;
        folders.push(assert_folded_the_seeded_segment(&bytes));
    }
    folders.dedup();
    assert_eq!(
        folders.len(),
        1,
        "one folder id (one process start) folded every pair this process owns"
    );
    for tenant in &theirs {
        let key = format!("t/{}/catalog/m/HEAD", tenant.to_hex());
        assert!(
            inner.get(&key, GetRange::Full).await.is_err(),
            "a pair the published live set gives to the foreign worker is left alone"
        );
    }
}

/// ADR-1693 decision 3: a `Mode::All` process publishes no heartbeat and keeps
/// its solo live set for its whole lifetime, so it owns and folds every unit.
/// A foreign heartbeat sits in the store throughout: an `all` process that
/// partitioned against what it found there would fold only about half of these
/// tenants.
#[tokio::test]
async fn an_all_mode_server_folds_every_unit() {
    let inner = Arc::new(MemoryStore::new());
    seed_worker_heartbeat(inner.as_ref(), FOREIGN_WORKER).await;
    let tenants: Vec<TenantHash> = TENANTS.iter().map(|t| TenantId::new(*t).hash()).collect();
    for tenant in &tenants {
        seed_sealed_metric_segment(inner.as_ref(), tenant).await;
    }

    let store_dyn: Arc<dyn ObjectStoreBackend> = inner.clone();
    let mut config = test_config(
        Mode::All,
        &TenantId::new("fold-all"),
        Duration::from_millis(250),
    );
    config.fold_tenants = Vec::new();
    let running = ravel_server::start(
        config,
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    wait_for_folded_metrics_heads(inner.as_ref(), &tenants).await;
    running.shutdown().await.expect("graceful shutdown");

    // No heartbeat of its own, so nothing ever replaces the solo set the watch
    // channel starts on. This is what makes the partition inert here rather
    // than merely unobserved.
    assert_eq!(
        worker_ids(inner.as_ref(), 1).await,
        vec![FOREIGN_WORKER],
        "an all-mode process publishes no heartbeat"
    );

    // Exactly one folder id across every tenant, and it is not the foreign
    // worker: one process folded the whole set.
    let mut folders: Vec<Uuid> = Vec::new();
    for tenant in &tenants {
        let key = format!("t/{}/catalog/m/HEAD", tenant.to_hex());
        let bytes = inner
            .get(&key, GetRange::Full)
            .await
            .unwrap_or_else(|err| panic!("an all-mode process folds every unit: {err}"))
            .data;
        folders.push(assert_folded_the_seeded_segment(&bytes));
    }
    folders.dedup();
    assert_eq!(folders.len(), 1, "one process folded every tenant");
    assert_ne!(
        folders[0], FOREIGN_WORKER,
        "the folder is the running process, not the heartbeat record it ignored"
    );
}

/// ADR-1693 decision 2: a gateway process runs no scheduled fold. Counted, not
/// timed out: over a window covering many fold intervals the process never
/// peeks at the pair's HEAD, which is the first request a fold tick makes.
#[tokio::test]
async fn a_gateway_mode_process_spawns_no_scheduled_fold() {
    let inner = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("fold-gateway");
    seed_sealed_metric_segment(inner.as_ref(), &tenant.hash()).await;

    let store = Arc::new(RequestLogStore::new(inner.clone()));
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let config = test_config(Mode::Gateway, &tenant, Duration::from_millis(50));
    assert!(
        !config.folds_in_process(),
        "a gateway process folds by neither route"
    );
    let running = ravel_server::start(
        config,
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    // Twenty fold intervals: a scheduled fold would have ticked many times.
    tokio::time::sleep(Duration::from_secs(1)).await;
    running.shutdown().await.expect("graceful shutdown");

    let head_key = format!("t/{}/catalog/m/HEAD", tenant.hash().to_hex());
    let peeks: Vec<String> = store
        .requests()
        .into_iter()
        .filter(|entry| entry.ends_with(&head_key))
        .collect();
    assert_eq!(
        peeks,
        Vec::<String>::new(),
        "a gateway process must never peek at or write a catalog HEAD on a schedule"
    );
    assert!(
        inner.get(&head_key, GetRange::Full).await.is_err(),
        "no HEAD exists: nothing folded this tenant"
    );
}

/// ADR-1693 decision 2, the other half: a query process runs no scheduled fold
/// either, but the on-demand `POST /api/v1/admin/fold` route stays exactly
/// where it is mounted today and still folds.
#[tokio::test]
async fn a_query_mode_process_folds_only_through_the_on_demand_route() {
    let inner = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("fold-query");
    seed_sealed_metric_segment(inner.as_ref(), &tenant.hash()).await;

    let store = Arc::new(RequestLogStore::new(inner.clone()));
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let config = test_config(Mode::Query, &tenant, Duration::from_millis(50));
    let running = ravel_server::start(
        config,
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    tokio::time::sleep(Duration::from_secs(1)).await;
    let head_key = format!("t/{}/catalog/m/HEAD", tenant.hash().to_hex());
    assert!(
        inner.get(&head_key, GetRange::Full).await.is_err(),
        "a query process must not fold on a schedule"
    );

    // The operator trigger is untouched.
    let base = format!("http://{}", running.http_addr);
    let response = reqwest::Client::new()
        .post(format!("{base}/api/v1/admin/fold"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "signal": "metrics" }))
        .send()
        .await
        .expect("on-demand fold request succeeds");
    assert_eq!(response.status(), 200, "the on-demand route stays mounted");
    let body: serde_json::Value = response.json().await.expect("json body");
    assert_eq!(
        body["status"], "published",
        "the on-demand fold publishes a snapshot, got {body}"
    );
    assert_eq!(
        body["entry_count"], 1,
        "the one seeded commit is the one folded entry, got {body}"
    );

    let bytes = inner
        .get(&head_key, GetRange::Full)
        .await
        .expect("the on-demand fold wrote a HEAD")
        .data;
    let head = decode_head(&bytes).expect("HEAD decodes");
    assert!(
        head.watermark_hour >= SEALED_HOUR,
        "the on-demand fold runs on the wall clock, so its watermark covers the seeded hour"
    );

    running.shutdown().await.expect("graceful shutdown");
}
