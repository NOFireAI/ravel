//! ADR-1170 issue #3244 amendment reachability: the catalog a started server
//! builds charges its snapshot part and name-postings decodes to the server's
//! own process memory budget, the one `Running::process_memory_budget` hands
//! out and the query engine and SQL executor hold.
//!
//! The fixture folds one hour into a single snapshot part whose entry body is
//! larger than the one segment a query for `probe_gauge` fetches: the other
//! segments in the hour carry a different metric at event times outside the
//! query's lookback window. A process budget smaller than the part's entry
//! body then refuses the part decode before any segment fetch, while a
//! catalog that does not charge that budget answers the same query 200.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ravel_catalog::{
    Catalog, CatalogConfig, PartLimits, PostingsLimits, decode_head, decode_part, decode_postings,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreMetrics};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
/// Hours before the current one the seeded segments sit in: far enough back
/// that the fold seals the hour on its first run against the wall clock.
const SEALED_HOURS_AGO: i64 = 4;
/// Segments of `filler_gauge` beside the one `probe_gauge` segment. They
/// grow the part's entry body; the query never fetches them.
const FILLERS: u64 = 200;
const PROBE: &str = "probe_gauge";
const FILLER: &str = "filler_gauge";
/// Minutes into the hour the probe's samples start; the fillers' one sample
/// sits at minute 1, outside a five-minute lookback from the probe's samples.
const PROBE_MINUTE: i64 = 30;
const LIMIT: u64 = 1 << 30;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// Publishes one real RSEG metric segment holding one `metric` series with
/// `samples` one-second samples from `first_ts_ns`, through the ingest
/// flush's two puts.
async fn seed_segment(
    store: &MemoryStore,
    tenant: &TenantId,
    seq: u64,
    hour: u32,
    metric: &str,
    first_ts_ns: i64,
    samples: i64,
) {
    let tenant_hash = tenant.hash();
    let labels = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, metric, &labels).expect("series id"),
        labels,
        samples: (0..samples)
            .map(|i| Sample {
                ts_ns: first_ts_ns + i * NS_PER_SEC,
                value: i as f64,
            })
            .collect(),
    }];
    let writer_id = Uuid::from_u128(0x3244_0000 + u128::from(seq));
    let written = SegmentWriter::write(
        series,
        SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: seq,
        },
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: i64::from(hour) * NS_PER_HOUR + 59 * 60 * NS_PER_SEC,
        ingest_hour_bucket: hour,
    })
    .expect("valid metric commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit record");
}

/// The folded fixture's figures.
struct Folded {
    /// The query time, in seconds, an instant query for [`PROBE`] uses.
    query_time_s: i64,
    /// The one part's `entries_uncompressed_len`: the part decode's charge.
    part_bytes: u64,
    /// The postings object's `body_uncompressed_len`: the postings decode's
    /// charge.
    postings_bytes: u64,
    /// The probe segment's object size, the one segment the query fetches.
    probe_segment_bytes: u64,
}

async fn fold_fixture(store: &Arc<MemoryStore>) -> Folded {
    let tenant_id = TenantId::new(TENANT);
    let tenant = tenant_id.hash();
    let now = now_ns();
    let hour = u32::try_from(now / NS_PER_HOUR - SEALED_HOURS_AGO).expect("hour fits u32");
    let hour_start = i64::from(hour) * NS_PER_HOUR;
    for seq in 1..=FILLERS {
        seed_segment(
            store,
            &tenant_id,
            seq,
            hour,
            FILLER,
            hour_start + 60 * NS_PER_SEC,
            1,
        )
        .await;
    }
    let probe_first = hour_start + PROBE_MINUTE * 60 * NS_PER_SEC;
    seed_segment(store, &tenant_id, FILLERS + 1, hour, PROBE, probe_first, 60).await;

    let report = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("folding catalog")
    .fold(&tenant, Signal::Metrics, Uuid::new_v4(), now, &[], None)
    .await
    .expect("fold");
    assert_eq!(
        report.entry_count,
        FILLERS + 1,
        "every seeded segment folds"
    );

    let (part_bytes, postings_bytes) = decode_charges(store, &tenant).await;
    let probe_segment_bytes = probe_segment_size(store, &tenant).await;
    assert!(
        probe_segment_bytes < part_bytes - 1,
        "the probe segment ({probe_segment_bytes} bytes) must fit in one byte less than the \
         part's entry body ({part_bytes} bytes), or a budget that refuses the part would also \
         refuse the fetch"
    );
    Folded {
        query_time_s: (probe_first + 59 * NS_PER_SEC) / NS_PER_SEC,
        part_bytes,
        postings_bytes,
        probe_segment_bytes,
    }
}

/// The metrics HEAD's one part `entries_uncompressed_len` and its postings
/// object's `body_uncompressed_len`.
async fn decode_charges(store: &MemoryStore, tenant: &TenantHash) -> (u64, u64) {
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("HEAD present")
        .data;
    let head = decode_head(&head_bytes).expect("HEAD decodes");
    assert_eq!(head.parts.len(), 1, "one part: {:?}", head.parts);
    let part = &head.parts[0];
    let part_bytes = store
        .get(&part.key, GetRange::Full)
        .await
        .expect("part present")
        .data;
    let entries = decode_part(&part_bytes, &PartLimits::default())
        .expect("part decodes")
        .header
        .entries_uncompressed_len;
    let postings_ref = head.postings.expect("the fold built postings");
    let postings_bytes = store
        .get(&postings_ref.key, GetRange::Full)
        .await
        .expect("postings present")
        .data;
    let part_blake3: [u8; 32] = part.blake3.as_slice().try_into().expect("32-byte blake3");
    let body = decode_postings(&postings_bytes, &PostingsLimits::default(), &[part_blake3])
        .expect("postings decode")
        .header
        .body_uncompressed_len;
    (entries, body)
}

/// The object size of the probe segment, the one with the highest writer seq.
async fn probe_segment_size(store: &MemoryStore, tenant: &TenantHash) -> u64 {
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("HEAD present")
        .data;
    let head = decode_head(&head_bytes).expect("HEAD decodes");
    let part_bytes = store
        .get(&head.parts[0].key, GetRange::Full)
        .await
        .expect("part present")
        .data;
    let part = decode_part(&part_bytes, &PartLimits::default()).expect("part decodes");
    part.entries
        .iter()
        .find(|entry| entry.writer_seq == FILLERS + 1)
        .expect("the probe entry")
        .object_size
}

async fn start_query_server(
    store: Arc<MemoryStore>,
    process_memory_budget_bytes: u64,
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
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Query,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
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
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: ravel_server::config::CpuGatePermits {
            read: 2,
            write: 1,
            ..Default::default()
        },
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let backend: Arc<dyn ObjectStoreBackend> = store;
    ravel_server::start(
        config,
        backend.clone(),
        backend,
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// An instant PromQL query for [`PROBE`] at `time_s`: the status and the
/// parsed body.
async fn query_probe(running: &ravel_server::Running, time_s: i64) -> (u16, serde_json::Value) {
    let response = reqwest::Client::new()
        .get(format!("http://{}/api/v1/query", running.http_addr))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", PROBE), ("time", time_s.to_string().as_str())])
        .send()
        .await
        .expect("query request completes");
    let status = response.status().as_u16();
    let body = response.json().await.expect("JSON body");
    (status, body)
}

/// Asserts a 200 whose one series is the probe's.
fn assert_probe_answered(status: u16, body: &serde_json::Value) {
    assert_eq!(status, 200, "{body}");
    let result = body["data"]["result"].as_array().expect("result array");
    assert_eq!(result.len(), 1, "{body}");
    assert_eq!(result[0]["metric"]["__name__"], PROBE, "{body}");
}

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

/// The value of the one sample named `series` (name and labels, exactly as
/// rendered), asserting it renders exactly once.
fn sample(body: &str, series: &str) -> u64 {
    let prefix = format!("{series} ");
    let samples: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .collect();
    assert_eq!(samples.len(), 1, "{series:?} must render once:\n{body}");
    samples[0].parse().expect("gauge value")
}

/// The HELP text of `family`, asserting it renders exactly once.
fn help<'a>(body: &'a str, family: &str) -> &'a str {
    let prefix = format!("# HELP {family} ");
    let helps: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .collect();
    assert_eq!(helps.len(), 1, "{family:?} HELP must render once:\n{body}");
    helps[0]
}

const FETCH_RESERVED: &str = r#"ravel_memory_reserved_bytes{mode="query",component="fetch"}"#;
const PART_CACHE: &str = r#"ravel_catalog_decoded_cache_reserved_bytes{mode="query",cache="part"}"#;
const POSTINGS_CACHE: &str =
    r#"ravel_catalog_decoded_cache_reserved_bytes{mode="query",cache="postings"}"#;
const RETRIES: &str = "ravel_catalog_decode_reserve_retries_total";
const EVICTIONS: &str = "ravel_catalog_decoded_cache_memory_evictions_total";
const BOUND_EVICTIONS: &str = "ravel_catalog_decoded_cache_byte_bound_evictions_total";
const BOUND_REFUSALS: &str = "ravel_catalog_decoded_cache_byte_bound_refusals_total";
/// A process budget whose 1% (the server's decoded-cache byte bound) is
/// below the fixture's part while the whole budget holds it many times over.
const SMALL_LIMIT: u64 = 1_000_000;

/// A process budget one byte smaller than the part's entry body refuses the
/// part decode: 422 `execution` with the budget's own figures. The part is
/// the resolve's first charged decode, so nothing is reserved when it is
/// refused and nothing is left reserved after.
///
/// FLIP: drop `.with_memory_budget(process_memory_budget.clone())` from the
/// catalog build in `ravel_server::start` and the query answers 200, so the
/// status assertion fails with 200 against 422.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_decode_over_the_process_budget_answers_422() {
    let store = Arc::new(MemoryStore::new());
    let folded = fold_fixture(&store).await;
    let limit = folded.part_bytes - 1;
    let running = start_query_server(store.clone(), limit).await;

    let (status, body) = query_probe(&running, folded.query_time_s).await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(body["errorType"], "execution", "{body}");
    assert_eq!(
        body["error"].as_str().expect("error string"),
        format!(
            "catalog decode refused: memory exhausted: requested {} bytes, 0 of {limit} byte \
             limit already reserved",
            folded.part_bytes
        ),
        "{body}"
    );
    assert_eq!(running.process_memory_budget().reserved(), 0);

    running.shutdown().await.expect("graceful shutdown");
}

/// The catalog charges the server's own budget, not a copy of its size: a
/// reservation held on `Running::process_memory_budget` leaves one byte less
/// room than the part needs while the limit itself would hold it many times
/// over, and the part decode is refused. Releasing the held bytes lets the
/// same query answer 200 on the same server.
///
/// FLIP: build the catalog in `ravel_server::start` with
/// `.with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(config.process_memory_budget_bytes)))`
/// and the first query answers 200, so the status assertion fails with 200
/// against 422.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_catalog_charges_the_servers_own_budget() {
    let store = Arc::new(MemoryStore::new());
    let folded = fold_fixture(&store).await;
    let running = start_query_server(store.clone(), LIMIT).await;
    let budget = running.process_memory_budget();
    let held_bytes = LIMIT - (folded.part_bytes - 1);
    let held = budget.reserve(held_bytes).expect("room for the held bytes");

    let (status, body) = query_probe(&running, folded.query_time_s).await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        body["error"].as_str().expect("error string"),
        format!(
            "catalog decode refused: memory exhausted: requested {} bytes, {held_bytes} of \
             {LIMIT} byte limit already reserved",
            folded.part_bytes
        ),
        "{body}"
    );
    assert_eq!(budget.reserved(), held_bytes, "the refusal charged nothing");
    // The decoded caches were empty, so no eviction could admit the part and
    // the catalog kept its first refusal without a pass or a retry.
    let refused = scrape(&running).await;
    assert_eq!(
        sample(&refused, &format!(r#"{RETRIES}{{mode="query"}}"#)),
        0
    );
    assert_eq!(
        sample(&refused, &format!(r#"{EVICTIONS}{{mode="query"}}"#)),
        0
    );

    drop(held);
    let (status, body) = query_probe(&running, folded.query_time_s).await;
    assert_probe_answered(status, &body);

    running.shutdown().await.expect("graceful shutdown");
}

/// After a query, the catalog's cached part and postings decodes are bytes
/// the server's budget holds: `ravel_memory_reserved_bytes{component="fetch"}`
/// is exactly their sum (the segment fetch's reservation is released with
/// the query), and the decoded-cache gauge splits the same bytes by cache.
///
/// FLIP: drop `.with_memory_budget(process_memory_budget.clone())` from the
/// catalog build in `ravel_server::start` and the decodes charge the
/// catalog's private budget, so the `FETCH_RESERVED` assertion fails with 0
/// against the sum while the decoded-cache gauge still reads the same bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fetch_gauge_counts_the_catalogs_held_decodes() {
    let store = Arc::new(MemoryStore::new());
    let folded = fold_fixture(&store).await;
    let running = start_query_server(store.clone(), LIMIT).await;

    let before = scrape(&running).await;
    assert_eq!(sample(&before, FETCH_RESERVED), 0);

    let (status, body) = query_probe(&running, folded.query_time_s).await;
    assert_probe_answered(status, &body);

    let after = scrape(&running).await;
    assert_eq!(sample(&after, PART_CACHE), folded.part_bytes, "{after}");
    assert_eq!(
        sample(&after, POSTINGS_CACHE),
        folded.postings_bytes,
        "{after}"
    );
    assert_eq!(
        sample(&after, FETCH_RESERVED),
        folded.part_bytes + folded.postings_bytes,
        "{after}"
    );
    assert_eq!(
        running.process_memory_budget().fetch_reserved(),
        folded.part_bytes + folded.postings_bytes
    );
    assert!(
        help(&after, "ravel_memory_reserved_bytes")
            .contains("the catalog's snapshot-part and name-postings decodes"),
        "{after}"
    );
    for counter in [RETRIES, EVICTIONS] {
        assert!(
            help(&after, counter).contains(
                "moves only when the shared ADR-1170 process memory budget refuses a catalog \
                 decode reservation that dropping cached entries could admit"
            ),
            "{after}"
        );
    }
    eprintln!(
        "pinned figures: part {} bytes, postings {} bytes, probe segment {} bytes",
        folded.part_bytes, folded.postings_bytes, folded.probe_segment_bytes
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// A finite process budget bounds the catalog's decoded caches at 1% of it.
/// With a part larger than that bound and a postings object below it, each
/// query answers 200, the part is decoded and not cached (one byte-bound
/// refusal per query, its reservation released with the query), and the
/// postings object is cached and held by the budget. Nothing is evicted.
///
/// FLIP: drop `.with_decoded_cache_max_bytes(..)` from the catalog build in
/// `ravel_server::start`, or pass it `process_memory_budget.limit()` instead
/// of the 1% figure, and the part is cached, so the `BOUND_REFUSALS`
/// assertion after the first query fails with 0 against 1. Export the two
/// counters' values swapped and the same assertion fails with 0 against 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_over_one_percent_of_the_budget_is_not_cached() {
    let store = Arc::new(MemoryStore::new());
    let folded = fold_fixture(&store).await;
    let bound = SMALL_LIMIT / 100;
    assert!(
        folded.postings_bytes <= bound && bound < folded.part_bytes,
        "the fixture must put the postings ({} bytes) within and the part ({} bytes) over the \
         {bound}-byte bound",
        folded.postings_bytes,
        folded.part_bytes
    );
    assert!(
        folded.part_bytes + folded.postings_bytes + folded.probe_segment_bytes < SMALL_LIMIT,
        "the whole budget must hold one query's decodes and fetch"
    );
    let running = start_query_server(store.clone(), SMALL_LIMIT).await;
    let budget = running.process_memory_budget();
    assert_eq!(budget.reserved(), 0);

    for query in 1..=2u64 {
        let (status, body) = query_probe(&running, folded.query_time_s).await;
        assert_probe_answered(status, &body);

        let after = scrape(&running).await;
        assert_eq!(
            sample(&after, &format!(r#"{BOUND_REFUSALS}{{mode="query"}}"#)),
            query,
            "{after}"
        );
        assert_eq!(sample(&after, PART_CACHE), 0, "{after}");
        assert_eq!(
            sample(&after, POSTINGS_CACHE),
            folded.postings_bytes,
            "{after}"
        );
        assert_eq!(
            sample(&after, &format!(r#"{BOUND_EVICTIONS}{{mode="query"}}"#)),
            0,
            "{after}"
        );
        assert_eq!(
            sample(&after, &format!(r#"{EVICTIONS}{{mode="query"}}"#)),
            0,
            "{after}"
        );
        // The refused part's charge leaves with the query; only the cached
        // postings stay reserved.
        assert_eq!(budget.reserved(), folded.postings_bytes);
        if query == 1 {
            assert!(
                help(&after, BOUND_EVICTIONS).contains("to stay within the caches' byte bound"),
                "{after}"
            );
            assert!(
                help(&after, BOUND_REFUSALS).contains("each is decoded again by the next query"),
                "{after}"
            );
            for family in [BOUND_EVICTIONS, BOUND_REFUSALS] {
                assert!(
                    help(&after, family).contains("reads 0 where that budget is unlimited")
                        || help(&after, family).contains("Reads 0 where that budget is unlimited"),
                    "{after}"
                );
            }
        }
    }

    running.shutdown().await.expect("graceful shutdown");
}
