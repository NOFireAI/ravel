//! The startup warm-up's logs metadata pass (ADR-4203 decision 8, issue
//! #4218): a restarted server serves the first statement's footer and
//! directory reads for every logs object in the warm window from the read
//! cache, on the fast path and on the striped route alike.
//!
//! One tenant holds [`OBJECTS`] logs objects, more than the whole-part warm's
//! cap of 4, every one above the logs plan bound the server is configured
//! with, so both SQL routes read each object's footer and directories
//! through the suffix probe and then its column pages by range. A counting
//! store classifies every GET of a data object against that object's own
//! footer: a read reaching the last byte is a footer read, one overlapping
//! STREAM_DIR, FIELD_DIR, SKIP_IDX or PAGE_DIR a directory read, anything
//! else a column-page read.
//!
//! Each statement runs on its own freshly started server, once warmed and
//! once unwarmed. The unwarmed baseline hides the store's tenant listing
//! from the warm-up while the server starts, so the warm-up finds no tenant
//! and leaves the cache empty.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
use ravel_logseg::footer::{kind, open as open_footer};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_query::{CacheFetchError, ReadCache};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const KEY: &str = "svc";

/// More objects than the whole-part warm's per-signal cap of 4.
const OBJECTS: usize = 6;
/// Rows per export. Each row's body is 160 pseudo-random hex characters, so
/// one export writes one object of a few hundred KiB.
const ROWS: usize = 2_500;
/// The logs plan bound the query servers run with: every fixture object is
/// above it, so both routes read it by range.
const BLOCK_RANGE_THRESHOLD: u64 = 64 * 1024;
const REQUEST_COST_BYTES: u64 = 16 * 1024;
/// A read cache large enough that nothing is evicted.
const ROOMY_CACHE_BYTES: u64 = 256 * 1024 * 1024;

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// Every GET the wrapped store was asked for, with its range, a switch that
/// hides the tenant listing the warm-up discovers tenants from, and one key
/// whose GETs fail while it is set.
struct Counting {
    inner: Arc<MemoryStore>,
    gets: Mutex<Vec<(String, GetRange)>>,
    hide_tenants: AtomicBool,
    failing: Mutex<Option<String>>,
}

impl Counting {
    fn note(&self, key: &str, range: GetRange) -> Result<(), StoreError> {
        self.gets
            .lock()
            .expect("get log")
            .push((key.to_string(), range));
        if self.failing.lock().expect("failing key").as_deref() == Some(key) {
            return Err(StoreError::Permanent(format!(
                "injected GET failure: {key}"
            )));
        }
        Ok(())
    }

    fn take_gets(&self) -> Vec<(String, GetRange)> {
        std::mem::take(&mut *self.gets.lock().expect("get log"))
    }
}

#[async_trait]
impl ObjectStoreBackend for Counting {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.note(key, range)?;
        self.inner.get(key, range).await
    }
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.note(key, range)?;
        self.inner.get_pinned(key, range, pin).await
    }
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.note(key, range)?;
        self.inner.get_with_pin(key, range).await
    }
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.inner.pin_of(key).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        if prefix == "t/" && self.hide_tenants.load(Ordering::SeqCst) {
            return Ok(DelimitedList {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                unaddressable: Vec::new(),
                unaddressable_prefixes: Vec::new(),
            });
        }
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// One data object's size and the byte ranges of its four directories, in the
/// order STREAM_DIR, FIELD_DIR, SKIP_IDX, PAGE_DIR.
struct Layout {
    size: u64,
    dirs: Vec<(u64, u64)>,
}

/// GETs of data objects, by class, per object key: the whole object, a read
/// reaching the last byte (the footer), one overlapping a directory, and
/// anything else (column pages).
#[derive(Debug, Default, PartialEq)]
struct Gets {
    whole: BTreeMap<String, usize>,
    footer: BTreeMap<String, usize>,
    dir: BTreeMap<String, usize>,
    page: BTreeMap<String, usize>,
}

impl Gets {
    fn whole_total(&self) -> usize {
        self.whole.values().sum()
    }
    fn footer_total(&self) -> usize {
        self.footer.values().sum()
    }
    fn dir_total(&self) -> usize {
        self.dir.values().sum()
    }
    fn page_total(&self) -> usize {
        self.page.values().sum()
    }
}

struct Tenant {
    store: Arc<Counting>,
    /// Data object keys, most recent first: the order the warm-up visits them.
    keys: Vec<String>,
    layouts: BTreeMap<String, Layout>,
    base_ts_ns: i64,
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

/// Streams per export in [`wide_tail_export`], each named by 128 pseudo-random
/// hex characters, so STREAM_DIR is larger than the 128 KiB probe.
const WIDE_TAIL_STREAMS: usize = 3_000;
/// Per-record attribute keys in [`wide_tail_export`], each [`FIELD_KEY_LEN`]
/// pseudo-random characters of a 64-symbol alphabet, so FIELD_DIR also starts
/// before the probe. Under the 1000-column cap together with the declared
/// `svc` column.
const WIDE_TAIL_FIELDS: usize = 900;
/// Under the logs ingest limit of 256 bytes per attribute key.
const FIELD_KEY_LEN: usize = 250;
const FIELD_KEY_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.";
/// Objects in the [`wide_tail_tenant`] fixture.
const WIDE_TAIL_OBJECTS: usize = 2;

/// `WIDE_TAIL_STREAMS` streams of two records each, starting at `base_ts_ns`.
/// Record `i` also carries attribute key `i % WIDE_TAIL_FIELDS`.
fn wide_tail_export(seed: u64, base_ts_ns: i64) -> ExportLogsServiceRequest {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let fields: Vec<String> = (0..WIDE_TAIL_FIELDS)
        .map(|_| {
            (0..FIELD_KEY_LEN)
                .map(|_| FIELD_KEY_ALPHABET[(next() % 64) as usize] as char)
                .collect()
        })
        .collect();
    let resource_logs = (0..WIDE_TAIL_STREAMS)
        .map(|s| {
            let host: String = (0..8).map(|_| format!("{:016x}", next())).collect();
            let log_records = (0..2)
                .map(|r| {
                    let i = 2 * s + r;
                    let ts = (base_ts_ns + i as i64) as u64;
                    LogRecord {
                        time_unix_nano: ts,
                        observed_time_unix_nano: ts,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(AnyValueVariant::StringValue(format!("row {s} {r}"))),
                        }),
                        attributes: vec![
                            kv(KEY, &format!("s{}", i % 97)),
                            kv(&fields[i % WIDE_TAIL_FIELDS], "v"),
                        ],
                        ..Default::default()
                    }
                })
                .collect();
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "web"), kv("host.id", &host)],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records,
                    schema_url: String::new(),
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportLogsServiceRequest { resource_logs }
}

/// Writes the tenant's `OBJECTS` objects through a cacheless server, one
/// export each, then stops that server. Every directory of every object lies
/// inside its suffix probe.
async fn ingest_tenant() -> Tenant {
    let tenant = ingest(OBJECTS, export).await;
    for layout in tenant.layouts.values() {
        let probe = probe_len(layout.size);
        assert!(
            layout.size <= 512 * 1024,
            "every object stays a whole-object read for the default-config whole-part warm: {}",
            layout.size
        );
        assert!(
            layout
                .dirs
                .iter()
                .all(|&(start, _)| start >= layout.size - probe),
            "the suffix probe covers all four directories"
        );
    }
    tenant
}

/// Writes [`WIDE_TAIL_OBJECTS`] objects of [`wide_tail_export`]: each one's
/// FIELD_DIR starts before its suffix probe, so all four directories, which
/// precede it in the order SKIP_IDX, PAGE_DIR, STREAM_DIR, FIELD_DIR, are only
/// partly resident in the probe or not at all.
async fn wide_tail_tenant() -> Tenant {
    let tenant = ingest(WIDE_TAIL_OBJECTS, wide_tail_export).await;
    for layout in tenant.layouts.values() {
        let probe = probe_len(layout.size);
        let [stream_dir, field_dir, skip_idx, page_dir] = [0, 1, 2, 3].map(|i| layout.dirs[i]);
        assert!(
            skip_idx.1 <= page_dir.0 && page_dir.1 <= stream_dir.0 && stream_dir.1 <= field_dir.0,
            "directory order: {:?}",
            layout.dirs
        );
        assert!(
            field_dir.0 < layout.size - probe,
            "FIELD_DIR at {} starts before the {probe}-byte probe of a {}-byte object",
            field_dir.0,
            layout.size
        );
    }
    tenant
}

/// Writes `objects` objects through a cacheless server, one `export` each,
/// then stops that server.
async fn ingest(objects: usize, export: fn(u64, i64) -> ExportLogsServiceRequest) -> Tenant {
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
    let store = Arc::new(Counting {
        inner: memory.clone(),
        gets: Mutex::new(Vec::new()),
        hide_tenants: AtomicBool::new(false),
        failing: Mutex::new(None),
    });

    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let running = ravel_server::start(
        server_config(2, ROOMY_CACHE_BYTES),
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
    let mut keys = Vec::new();
    for i in 0..objects {
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
        let after = data_keys(&memory).await;
        let new: Vec<String> = after.difference(&before).cloned().collect();
        assert_eq!(new.len(), 1, "one export, one data object");
        keys.extend(new);
    }
    running.shutdown().await.expect("graceful shutdown");
    keys.reverse();

    let mut layouts = BTreeMap::new();
    for key in &keys {
        let bytes = memory
            .get(key, GetRange::Full)
            .await
            .expect("get the data object")
            .data;
        let footer = open_footer(&bytes).expect("open footer");
        let dirs = [
            kind::STREAM_DIR,
            kind::FIELD_DIR,
            kind::SKIP_IDX,
            kind::PAGE_DIR,
        ]
        .into_iter()
        .map(|k| {
            let desc = footer.section(k).expect("a directory section");
            (desc.offset, desc.offset + desc.len)
        })
        .collect();
        let size = bytes.len() as u64;
        layouts.insert(key.clone(), Layout { size, dirs });
    }
    for layout in layouts.values() {
        let probe = probe_len(layout.size);
        assert!(
            layout.size > BLOCK_RANGE_THRESHOLD && layout.size > probe,
            "every object is above the plan bound and larger than its probe: {}",
            layout.size
        );
    }
    store.take_gets();
    Tenant {
        store,
        keys,
        layouts,
        base_ts_ns,
    }
}

/// The suffix probe's length for an object of `size` bytes.
fn probe_len(size: u64) -> u64 {
    ravel_query::derive_suffix_len(size).min(size).max(1)
}

impl Tenant {
    fn classify(&self, gets: Vec<(String, GetRange)>) -> Gets {
        let mut out = Gets::default();
        for (key, range) in gets {
            let Some(layout) = self.layouts.get(&key) else {
                continue;
            };
            let (start, end) = match range {
                GetRange::Full => (0, layout.size),
                GetRange::Range(start, end) => (start, end),
                GetRange::Suffix(n) => (layout.size.saturating_sub(n), layout.size),
            };
            let class = if start == 0 && end >= layout.size {
                &mut out.whole
            } else if end >= layout.size {
                &mut out.footer
            } else if layout.dirs.iter().any(|&(s, e)| start < e && s < end) {
                &mut out.dir
            } else {
                &mut out.page
            };
            *class.entry(key).or_default() += 1;
        }
        out
    }

    /// Starts a server over the tenant with a fresh read cache of
    /// `cache_bytes`, warmed unless `warm` is false, and waits for `/readyz`.
    async fn restart(&self, partitions: usize, cache_bytes: u64, warm: bool) -> Server {
        let cache = Arc::new(Cache::new(CacheLimits::new(
            cache_bytes,
            16_384,
            cache_bytes,
        )));
        self.store.hide_tenants.store(!warm, Ordering::SeqCst);
        let store: Arc<dyn ObjectStoreBackend> = self.store.clone();
        let running = ravel_server::start(
            server_config(partitions, cache_bytes),
            store.clone(),
            store,
            Arc::new(ravel_object_store::StoreMetrics::default()),
            Some(ReadCache::Ram(cache.clone())),
        )
        .await
        .expect("server starts");
        self.store.hide_tenants.store(false, Ordering::SeqCst);
        let base = format!("http://{}", running.http_addr);
        let client = reqwest::Client::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let ready = client
                .get(format!("{base}/readyz"))
                .send()
                .await
                .is_ok_and(|r| r.status() == 200);
            if ready {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "/readyz latches");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let warm_gets = self.classify(self.store.take_gets());
        Server {
            running,
            base,
            client,
            cache,
            warm_gets,
        }
    }
}

struct Server {
    running: ravel_server::Running,
    base: String,
    client: reqwest::Client,
    cache: Arc<Cache<CacheFetchError>>,
    /// The data-object GETs the server issued while starting.
    warm_gets: Gets,
}

/// One statement's rows, its data-object GETs and the read cache's hits and
/// misses while it ran.
struct Run {
    rows: serde_json::Value,
    gets: Gets,
    cache_hits: u64,
    cache_misses: u64,
    stats: serde_json::Value,
}

impl Server {
    async fn sql(&self, tenant: &Tenant, query: &str) -> Run {
        let start = (tenant.base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
        let end = (tenant.base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
        let before = self.cache.metrics().snapshot();
        tenant.store.take_gets();
        let response = self
            .client
            .post(format!("{}/api/v1/sql", self.base))
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
        let gets = tenant.classify(tenant.store.take_gets());
        let after = self.cache.metrics().snapshot();
        Run {
            rows: value["data"]["rows"].clone(),
            gets,
            cache_hits: after.hits - before.hits,
            cache_misses: after.misses - before.misses,
            stats: value["stats"].clone(),
        }
    }

    async fn stop(self) {
        self.running.shutdown().await.expect("graceful shutdown");
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

/// The two routes: a narrow-column scan the whole-segment fast path takes
/// (two partitions over six objects), and a `LIKE` whose pushed `Contains`
/// arm plans from the directories on the striped route (more partitions
/// than objects).
const FAST_PATH: (&str, usize) = (
    "SELECT count(*) FROM logs WHERE svc LIKE '%7%' ESCAPE '#'",
    2,
);
const STRIPED: (&str, usize) = ("SELECT count(*) FROM logs WHERE svc LIKE '%7%'", 16);

/// On a server restarted over [`OBJECTS`] logs objects, the first statement
/// of either route issues no footer GET and no directory GET: the warm-up's
/// metadata pass put every object's probe in the cache under the key the
/// statement reads. The phase that first opens each object (scan on the fast
/// path, plan on the striped route) takes exactly one cache hit per object,
/// where the unwarmed run takes none. Column-page GETs equal the unwarmed
/// run's, and over the whole statement the read cache serves exactly one more
/// hit and one fewer miss per object than unwarmed: the probe read the
/// unwarmed run missed. While the server started, each object's probe was
/// read once, no column page or lone directory was, and only the whole-part
/// warm's 4 objects were read whole.
///
/// Seen failing against three alternative warms: the 4-part cap kept on the
/// metadata pass (4 first-open cache hits, not 6), a probe of another length
/// (0, not 6), and a pass that also reads every object's columns (6 whole
/// reads during startup, not 4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_server_serves_the_first_scan_footers_from_the_warmed_cache() {
    let tenant = ingest_tenant().await;

    for (route, (query, partitions)) in [("fast path", FAST_PATH), ("striped", STRIPED)] {
        let cold = tenant.restart(partitions, ROOMY_CACHE_BYTES, false).await;
        assert_eq!(
            cold.warm_gets,
            Gets::default(),
            "{route}: the unwarmed start reads no data object"
        );
        assert_eq!(cold.cache.len(), 0, "{route}: the unwarmed cache is empty");
        let unwarmed = cold.sql(&tenant, query).await;
        cold.stop().await;

        let mut warm = tenant.restart(partitions, ROOMY_CACHE_BYTES, true).await;
        let warmed = warm.sql(&tenant, query).await;
        let warm_gets = std::mem::take(&mut warm.warm_gets);
        warm.stop().await;

        // The route: only the striped route opens objects in the plan phase,
        // and the phase that first opens an object reads its probe.
        let planned = if partitions > OBJECTS { OBJECTS } else { 0 };
        let first_open = if planned > 0 { "plan" } else { "scan" };
        for run in [&unwarmed, &warmed] {
            assert_eq!(
                phase_figure(&run.stats, "plan", "segmentsOpened"),
                planned as u64,
                "{route}: objects opened in the plan phase: {}",
                run.stats
            );
        }
        assert_eq!(
            (
                phase_figure(&unwarmed.stats, first_open, "s3GetRequests") > 0,
                phase_figure(&unwarmed.stats, first_open, "cacheHits"),
            ),
            (true, 0),
            "{route}: unwarmed, the {first_open} phase fetches every probe: {}",
            unwarmed.stats
        );
        assert_eq!(
            phase_figure(&warmed.stats, first_open, "cacheHits"),
            OBJECTS as u64,
            "{route}: warmed, the {first_open} phase hits the cache once per object: {}",
            warmed.stats
        );
        if planned > 0 {
            assert_eq!(
                phase_figure(&warmed.stats, "plan", "s3GetRequests"),
                0,
                "{route}: warmed, the plan phase issues no GET: {}",
                warmed.stats
            );
        }

        assert_eq!(warmed.rows, unwarmed.rows, "{route}: same rows");
        assert_eq!(
            unwarmed.gets.footer_total(),
            OBJECTS,
            "{route}: unwarmed, the statement reads every footer"
        );
        assert_eq!(
            (warmed.gets.footer_total(), warmed.gets.dir_total()),
            (0, 0),
            "{route}: warmed, no footer GET and no directory GET: {:?}",
            warmed.gets
        );
        assert!(
            unwarmed.gets.page_total() > 0,
            "{route}: the statement reads column pages"
        );
        assert_eq!(
            warmed.gets.page, unwarmed.gets.page,
            "{route}: the same column-page GETs as unwarmed"
        );
        assert_eq!(
            warmed.cache_hits - unwarmed.cache_hits,
            OBJECTS as u64,
            "{route}: one more cache hit per object than unwarmed"
        );
        assert_eq!(
            unwarmed.cache_misses - warmed.cache_misses,
            OBJECTS as u64,
            "{route}: one fewer cache miss per object than unwarmed"
        );
        assert_eq!(
            (
                warm_gets.footer_total(),
                warm_gets.footer.len(),
                warm_gets.dir_total(),
                warm_gets.page_total(),
                warm_gets.whole_total(),
            ),
            (OBJECTS, OBJECTS, 0, 0, 4),
            "{route}: the warm-up read one probe per object, no column page, and only the \
             whole-part warm's 4 objects whole: {warm_gets:?}"
        );
    }
}

/// Objects whose directories reach past the suffix probe: the striped route's
/// plan reads each one's probe, then one coalesced range GET for SKIP_IDX and
/// PAGE_DIR and one for STREAM_DIR and FIELD_DIR. On a warmed server the first
/// statement issues none of those GETs, because the metadata pass read the
/// same three extents under the same keys while the server started.
///
/// Seen failing against a pass that reads only the probe
/// (`fetch_footer` in place of `fetch_plan_directories`): the warmed
/// statement's directory GETs are 2 per object, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_warm_covers_directories_beyond_the_suffix_probe() {
    let tenant = wide_tail_tenant().await;
    let (query, partitions) = STRIPED;
    assert!(partitions > WIDE_TAIL_OBJECTS, "the striped route");

    let cold = tenant.restart(partitions, ROOMY_CACHE_BYTES, false).await;
    let unwarmed = cold.sql(&tenant, query).await;
    cold.stop().await;
    let mut warm = tenant.restart(partitions, ROOMY_CACHE_BYTES, true).await;
    let warmed = warm.sql(&tenant, query).await;
    let warm_gets = std::mem::take(&mut warm.warm_gets);
    warm.stop().await;

    assert_eq!(warmed.rows, unwarmed.rows, "same rows");
    for run in [&unwarmed, &warmed] {
        assert_eq!(
            phase_figure(&run.stats, "plan", "segmentsOpened"),
            WIDE_TAIL_OBJECTS as u64,
            "the plan phase opens every object: {}",
            run.stats
        );
    }
    assert_eq!(
        (unwarmed.gets.footer_total(), unwarmed.gets.dir_total()),
        (WIDE_TAIL_OBJECTS, 2 * WIDE_TAIL_OBJECTS),
        "unwarmed, the statement reads each object's probe and two directory runs: {:?}",
        unwarmed.gets
    );
    assert_eq!(
        (warmed.gets.footer_total(), warmed.gets.dir_total()),
        (0, 0),
        "warmed, no footer GET and no directory GET: {:?}",
        warmed.gets
    );
    assert_eq!(
        (
            warm_gets.footer_total(),
            warm_gets.dir_total(),
            warm_gets.page_total()
        ),
        (WIDE_TAIL_OBJECTS, 2 * WIDE_TAIL_OBJECTS, 0),
        "the warm-up read each object's probe and two directory runs: {warm_gets:?}"
    );
    assert_eq!(
        (
            phase_figure(&warmed.stats, "plan", "s3GetRequests"),
            phase_figure(&warmed.stats, "plan", "cacheHits"),
        ),
        (0, 3 * WIDE_TAIL_OBJECTS as u64),
        "warmed, the plan phase hits the probe and both runs of every object: {}",
        warmed.stats
    );
    assert_eq!(
        warmed.gets.page, unwarmed.gets.page,
        "the same column-page GETs as unwarmed"
    );
}

/// A read cache too small for the whole tenant: startup still succeeds, the
/// metadata pass stops at its share of the budget, and the first statement
/// reads exactly the footers of the objects the pass did not reach.
///
/// The whole-part warm leaves the 4 newest objects resident first, `R`
/// bytes. The pass then admits objects most recent first while resident
/// bytes plus the admitted probes stay within half the cache, so a cache of
/// `2 * (R + 2 * P) + P` (`P` one probe) admits exactly 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_metadata_warm_stops_at_the_cache_budget() {
    let tenant = ingest_tenant().await;
    let resident: u64 = tenant
        .keys
        .iter()
        .take(4)
        .map(|key| tenant.layouts[key].size)
        .sum();
    let probes: Vec<u64> = tenant
        .keys
        .iter()
        .map(|key| probe_len(tenant.layouts[key].size))
        .collect();
    let probe = probes[0];
    assert!(
        probes.iter().all(|&p| p == probe),
        "one probe length: {probes:?}"
    );
    let cache_bytes = 2 * (resident + 2 * probe) + probe;
    const ADMITTED: usize = 2;

    let server = tenant.restart(FAST_PATH.1, cache_bytes, true).await;
    assert_eq!(
        server.cache.total_bytes(),
        resident + ADMITTED as u64 * probe,
        "the 4 whole parts and the admitted probes are resident"
    );
    let run = server.sql(&tenant, FAST_PATH.0).await;
    server.stop().await;

    let not_warmed: BTreeMap<String, usize> = tenant
        .keys
        .iter()
        .skip(ADMITTED)
        .map(|key| (key.clone(), 1))
        .collect();
    assert_eq!(
        run.gets.footer, not_warmed,
        "footer GETs are exactly the objects the pass did not warm"
    );
    assert_eq!(run.gets.footer_total(), OBJECTS - ADMITTED);
}

/// A metadata read that fails while the server starts skips that object
/// only: startup succeeds and the first statement reads exactly that one
/// object's footer. The failing object is the oldest, which only the
/// metadata pass reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_metadata_read_skips_only_that_object() {
    let tenant = ingest_tenant().await;
    let failed = tenant.keys[OBJECTS - 1].clone();

    *tenant.store.failing.lock().expect("failing key") = Some(failed.clone());
    let server = tenant.restart(FAST_PATH.1, ROOMY_CACHE_BYTES, true).await;
    *tenant.store.failing.lock().expect("failing key") = None;
    assert_eq!(
        server.warm_gets.footer.get(&failed),
        Some(&1),
        "the pass tried the failing object once"
    );
    let run = server.sql(&tenant, FAST_PATH.0).await;
    server.stop().await;

    assert_eq!(
        run.gets.footer,
        BTreeMap::from([(failed, 1)]),
        "the statement reads only the failed object's footer"
    );
    assert_eq!(run.gets.dir_total(), 0);
}

fn server_config(partitions: usize, cache_max_bytes: u64) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
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
        cache_max_bytes,
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
