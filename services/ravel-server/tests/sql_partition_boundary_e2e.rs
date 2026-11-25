//! A logs `LIKE '%lit%'` statement's wire bytes do not jump when the SQL
//! partition count crosses the number of objects it scans (issue #4208).
//!
//! One tenant holds two logs objects, both above the logs plan bound. At two
//! partitions the scan takes the whole-segment fast path, which has no plan
//! phase; at three it takes the striped route, whose plan phase opens each
//! object. Since #4205 that plan reads each object's footer and four
//! directories through the suffix probe and nothing else, so the striped
//! statement moves no more wire bytes than the fast path's plus at most one
//! metadata read per object.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState};
use ravel_logseg::footer::{TRAILER_LEN, kind, open as open_footer};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_query::{LogSegmentFetcher, ReadCache};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const KEY: &str = "svc";

const OBJECTS: usize = 2;
/// Rows per export. Each row's body is 160 pseudo-random hex characters, so
/// one export writes one object of a few hundred KiB.
const ROWS: usize = 2_500;
const BLOCK_RANGE_THRESHOLD: u64 = 64 * 1024;
const REQUEST_COST_BYTES: u64 = 16 * 1024;
const CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// Partition counts on either side of the object count.
const FAST_PATH_PARTITIONS: usize = OBJECTS;
const STRIPED_PARTITIONS: usize = OBJECTS + 1;

const LIKE_COUNT: &str = "SELECT count(*) FROM logs WHERE svc LIKE '%7%'";
const LIKE_WIDE: &str = "SELECT * FROM logs WHERE svc LIKE '%7%' ORDER BY ts";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// A store whose tenant listing is empty, so a server's startup cache warm-up
/// finds no tenant and every run starts from an empty read cache. Queries
/// address the tenant's keys directly and never list `t/`.
struct NoTenantListing(Arc<MemoryStore>);

#[async_trait]
impl ObjectStoreBackend for NoTenantListing {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.0.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.0.get(key, range).await
    }
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.0.get_pinned(key, range, pin).await
    }
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.0.get_with_pin(key, range).await
    }
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.0.pin_of(key).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.0.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.0.list(prefix, page).await
    }
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.0.list_after(prefix, start_after, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        if prefix == "t/" {
            return Ok(DelimitedList {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                unaddressable: Vec::new(),
                unaddressable_prefixes: Vec::new(),
            });
        }
        self.0.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.0.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// `ROWS` records starting at `base_ts_ns`, with high-entropy bodies so the
/// object stays a few hundred KiB after compression.
fn export(seed: u64, base_ts_ns: i64) -> ExportLogsServiceRequest {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let log_records = (0..ROWS)
        .map(|i| {
            let ts = (base_ts_ns + i as i64) as u64;
            let body: String = (0..10).map(|_| format!("{:016x}", next())).collect();
            LogRecord {
                time_unix_nano: ts,
                observed_time_unix_nano: ts,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(body)),
                }),
                attributes: vec![kv(KEY, &format!("s{}", i % 97))],
                ..Default::default()
            }
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "web")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            ..Default::default()
        }],
    }
}

async fn data_keys(memory: &MemoryStore) -> BTreeSet<String> {
    let prefix = format!("t/{}/l/l0/", tenant_hash().to_hex());
    list_all(memory, &prefix)
        .await
        .expect("list log data objects")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

/// One data object's size, the bytes of its footer, trailer and four
/// directories, and the length of the suffix probe that reads them.
#[derive(Debug)]
struct Layout {
    size: u64,
    footer_and_dirs: u64,
    probe: u64,
}

struct Tenant {
    store: Arc<NoTenantListing>,
    base_ts_ns: i64,
    layouts: Vec<Layout>,
}

/// Writes the tenant's two objects through a cacheless server, one export
/// each, then stops that server.
async fn ingest_tenant() -> Tenant {
    let memory = Arc::new(MemoryStore::new());
    let config = TenantConfig {
        typed_attr_columns: Some(vec![DeclaredTypedColumn {
            key: KEY.to_string(),
            ty: DeclaredColumnType::Str,
        }]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(memory.as_ref(), &tenant_hash(), &config, now_ns())
        .await
        .expect("tenant config");
    let store = Arc::new(NoTenantListing(memory.clone()));

    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let running = ravel_server::start(
        server_config(FAST_PATH_PARTITIONS),
        dyn_store.clone(),
        dyn_store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("ingest server starts");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let base_ts_ns = now_ns();
    for i in 0..OBJECTS {
        let before = data_keys(&memory).await;
        let response = client
            .post(format!("{base}/v1/logs"))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/x-protobuf")
            .body(export(i as u64 + 1, base_ts_ns + i as i64 * 1_000_000).encode_to_vec())
            .send()
            .await
            .expect("export sent");
        assert_eq!(response.status(), 200, "export accepted");
        assert_eq!(
            data_keys(&memory).await.difference(&before).count(),
            1,
            "one export, one data object"
        );
    }
    running.shutdown().await.expect("graceful shutdown");

    let plan_bound = LogSegmentFetcher::new(memory.clone())
        .with_block_range_threshold(BLOCK_RANGE_THRESHOLD)
        .with_request_cost_bytes(REQUEST_COST_BYTES)
        .plan_whole_object_bound();
    let mut layouts = Vec::new();
    for key in data_keys(&memory).await {
        let bytes = memory
            .get(&key, GetRange::Full)
            .await
            .expect("get the data object")
            .data;
        let size = bytes.len() as u64;
        assert!(
            size > plan_bound,
            "every object is above the logs plan bound {plan_bound}: {size}"
        );
        let trailer = &bytes[bytes.len() - TRAILER_LEN..];
        let footer_len =
            u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]) as u64;
        let footer = open_footer(&bytes).expect("open footer");
        let dirs: Vec<(u64, u64)> = [
            kind::STREAM_DIR,
            kind::FIELD_DIR,
            kind::SKIP_IDX,
            kind::PAGE_DIR,
        ]
        .into_iter()
        .map(|k| {
            let desc = footer.section(k).expect("a directory section");
            (desc.offset, desc.len)
        })
        .collect();
        let probe = ravel_query::derive_suffix_len(size).min(size);
        assert!(
            probe < size && dirs.iter().all(|&(offset, _)| offset >= size - probe),
            "the suffix probe holds the footer and all four directories and not the whole \
             object: size {size}, probe {probe}, directories {dirs:?}"
        );
        layouts.push(Layout {
            size,
            footer_and_dirs: footer_len
                + TRAILER_LEN as u64
                + dirs.iter().map(|&(_, len)| len).sum::<u64>(),
            probe,
        });
    }
    assert_eq!(layouts.len(), OBJECTS);
    Tenant {
        store,
        base_ts_ns,
        layouts,
    }
}

/// One statement's rows and stats, on a freshly started server with an empty
/// read cache.
struct Run {
    rows: serde_json::Value,
    stats: serde_json::Value,
}

impl Tenant {
    async fn run(&self, partitions: usize, query: &str) -> Run {
        let cache = Arc::new(Cache::new(CacheLimits::new(
            CACHE_BYTES,
            16_384,
            CACHE_BYTES,
        )));
        let store: Arc<dyn ObjectStoreBackend> = self.store.clone();
        let running = ravel_server::start(
            server_config(partitions),
            store.clone(),
            store,
            Arc::new(ravel_object_store::StoreMetrics::default()),
            Some(ReadCache::Ram(cache.clone())),
        )
        .await
        .expect("server starts");
        assert_eq!(cache.len(), 0, "the read cache starts empty");
        let base = format!("http://{}", running.http_addr);
        let start = (self.base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
        let end = (self.base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
        let response = reqwest::Client::new()
            .post(format!("{base}/api/v1/sql"))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "query": query, "start": start, "end": end }).to_string())
            .send()
            .await
            .expect("sql request sent");
        let status = response.status();
        let value: serde_json::Value =
            serde_json::from_str(&response.text().await.expect("sql response body"))
                .expect("sql response is JSON");
        assert_eq!(status, 200, "{value}");
        running.shutdown().await.expect("graceful shutdown");
        Run {
            rows: value["data"]["rows"].clone(),
            stats: value["stats"].clone(),
        }
    }
}

/// `stats.phases[phase].field`, checked to be present exactly once.
fn phase_figure(stats: &serde_json::Value, phase: &str, field: &str) -> u64 {
    let matching: Vec<&serde_json::Value> = stats["phases"]
        .as_array()
        .unwrap_or_else(|| panic!("stats.phases: {stats}"))
        .iter()
        .filter(|p| p["phase"] == phase)
        .collect();
    assert_eq!(matching.len(), 1, "one {phase} phase: {stats}");
    matching[0][field]
        .as_u64()
        .unwrap_or_else(|| panic!("{phase}.{field}: {stats}"))
}

/// Wire bytes the statement's data-object reads moved: every phase but
/// `resolve`, whose GETs read the catalog.
fn data_wire_bytes(stats: &serde_json::Value) -> u64 {
    let phases = stats["phases"]
        .as_array()
        .unwrap_or_else(|| panic!("stats.phases: {stats}"));
    assert!(
        phases.iter().any(|p| p["phase"] == "resolve"),
        "a resolve phase: {stats}"
    );
    phases
        .iter()
        .filter(|p| p["phase"] != "resolve")
        .map(|p| {
            p["s3GetWireBytes"]
                .as_u64()
                .unwrap_or_else(|| panic!("s3GetWireBytes: {p}"))
        })
        .sum()
}

/// How the striped run's data wire bytes relate to the fast path's.
#[derive(Clone, Copy, Debug)]
enum Band {
    /// Within the footer, trailer and directory bytes of every object: the
    /// narrow scan opens each object from the probe the plan left in the read
    /// cache, so the routes differ by no more than the metadata itself.
    FooterAndDirectories,
    /// Exactly one suffix probe per object more: the fast path reads each
    /// object whole in one GET, and the striped scan does the same after its
    /// plan has fetched the probe under another cache key, so the probe's
    /// bytes cross the wire twice on that route.
    OneProbePerObject,
}

/// For a `LIKE` count and a `LIKE` with every column projected, the run at
/// three partitions (striped) returns the fast path's rows at two, and its
/// plan phase reads each object's footer and four directories through one
/// suffix probe and nothing else. The striped run's data wire bytes stay
/// within the objects' footer and directory bytes of the fast path's for the
/// count, and for the wide projection equal the object sizes plus one probe
/// per object, where the fast path's equal the object sizes ([`Band`]).
///
/// The route is read from the plan phase's counters (`segmentsOpened`,
/// `s3GetRequests`, `s3GetWireBytes`), not from
/// `stats.timings.planningWaitMaxMs` as `sql_like_contains_e2e.rs` does: the
/// counters are exact figures a run either matches or not, while
/// `planningWaitMaxMs` is a wall-clock duration, and asserting it above 0 on
/// the striped route depends on the plan barrier taking measurable time.
///
/// Seen failing with #4205 reverted: the striped plan also fetched every
/// column of each object's candidate blocks, four GETs and about 682,000
/// plan bytes against two probes of 262,144 bytes in all, and with that
/// assertion lifted the count's striped total sat about 420,000 bytes above
/// the fast path's against a band of under 1 KiB. Seen failing with the
/// coverage crossover off (`DEFAULT_LOG_COVERAGE_THRESHOLD` 2.0): the striped
/// wide scan read by range, 674,584 bytes against 682,008, which a band of one
/// probe per object either side of the fast path's would have accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn like_scan_bytes_are_continuous_across_the_object_count_boundary() {
    let tenant = ingest_tenant().await;
    let sizes: u64 = tenant.layouts.iter().map(|l| l.size).sum();
    let probes: u64 = tenant.layouts.iter().map(|l| l.probe).sum();
    let footer_and_dirs: u64 = tenant.layouts.iter().map(|l| l.footer_and_dirs).sum();

    for (name, query, band) in [
        ("count", LIKE_COUNT, Band::FooterAndDirectories),
        ("wide", LIKE_WIDE, Band::OneProbePerObject),
    ] {
        let fast = tenant.run(FAST_PATH_PARTITIONS, query).await;
        let striped = tenant.run(STRIPED_PARTITIONS, query).await;

        assert_eq!(striped.rows, fast.rows, "{name}: same rows on both routes");
        assert!(
            fast.rows.as_array().is_some_and(|rows| !rows.is_empty()),
            "{name}: rows: {}",
            fast.rows
        );

        assert_eq!(
            (
                phase_figure(&fast.stats, "plan", "segmentsOpened"),
                phase_figure(&fast.stats, "plan", "s3GetRequests"),
                phase_figure(&fast.stats, "plan", "s3GetWireBytes"),
            ),
            (0, 0, 0),
            "{name}: two partitions take the fast path, which has no plan phase: {}",
            fast.stats
        );
        assert_eq!(
            (
                phase_figure(&striped.stats, "plan", "segmentsOpened"),
                phase_figure(&striped.stats, "plan", "s3GetRequests"),
                phase_figure(&striped.stats, "plan", "s3GetWireBytes"),
            ),
            (OBJECTS as u64, OBJECTS as u64, probes),
            "{name}: three partitions take the striped route, whose plan phase reads one \
             suffix probe per object, the footer and directories, and nothing else: {}",
            striped.stats
        );

        let fast_bytes = data_wire_bytes(&fast.stats);
        let striped_bytes = data_wire_bytes(&striped.stats);
        match band {
            Band::FooterAndDirectories => assert!(
                striped_bytes.abs_diff(fast_bytes) <= footer_and_dirs,
                "{name}: striped data wire bytes {striped_bytes} within {footer_and_dirs} \
                 ({band:?}) of the fast path's {fast_bytes}\nfast {}\nstriped {}",
                fast.stats,
                striped.stats
            ),
            Band::OneProbePerObject => {
                assert_eq!(
                    fast_bytes, sizes,
                    "{name}: the fast path reads each object whole: {}",
                    fast.stats
                );
                assert_eq!(
                    striped_bytes,
                    sizes + probes,
                    "{name}: the striped route reads each object whole plus its probe \
                     ({band:?})\nfast {}\nstriped {}",
                    fast.stats,
                    striped.stats
                );
            }
        }
    }
}

fn server_config(partitions: usize) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        emit_declared_stat_aggregates: true,
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: ravel_server::QueryBudgets {
            sql_partition_count: partitions,
            logs_block_range_threshold: Some(BLOCK_RANGE_THRESHOLD),
            logs_request_cost_bytes: Some(REQUEST_COST_BYTES),
            ..Default::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(50),
        max_flush_delay_idle: Duration::from_millis(50),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![tenant_hash()],
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
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
        disable_cache: true,
        cache_max_bytes: CACHE_BYTES,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        ingest_hold: Default::default(),
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
