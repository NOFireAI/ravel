//! ADR-0064 EJ-T8 end-to-end selective-subject erasure reachability acceptance
//! (issue #756, epic EJ #460).
//!
//! Where the per-crate unit tests each prove one link of the erasure pipeline in
//! isolation, this module drives the WHOLE shipped chain together, for every
//! signal, through the real surfaces a deployment exposes:
//!
//! 1. Ingest a subject's records plus a bystander's into a sealed bucket.
//! 2. Submit a `.dreq` erasure request exactly as `ravel-cli erase submit` does.
//! 3. Immediate query exclusion, including from a WARM read cache: the subject's
//!    bytes are pulled into the ADR-0046 read cache by a pre-erase query, and the
//!    post-erase query -- reusing that same warm cache -- still excludes them,
//!    because the ADR-0064 scan-time filter runs post-cache.
//! 4. Real maintenance ticks ([`crate::maintain::run_tick_with_clock`] with an
//!    injected clock) publish a `RewriteRecord` for the subject's bucket while
//!    the bystander survives bit-exact.
//! 5. Past `protection_horizon`, the request's `.done` marker exists and its
//!    `.dreq` is swept.
//! 6. Physical absence: the pre-rewrite input object (which held the subject's
//!    bytes) is superseded and swept from durable storage; survivors remain.
//!
//! Plus a `FaultStore` failure-path variant proving a fault during the rewrite
//! publish leaves the system in a safe state (no partial deletion, no `.dreq`
//! removed without its `.done`, and the erasure completes on retry).
//!
//! **Signal coverage.** Metrics and logs are exercised through the real
//! ravel-server HTTP surfaces (`/api/v1/query` and `/api/v1/sql`), including the
//! warm-cache clause. Spans have no ravel-server query endpoint (the SQL
//! executor routes only `logs` and `samples`; `FROM spans` is unreachable
//! through the server -- a pre-existing, documented limitation, ADR-0041 phase
//! 5, NOT an erasure gap), so the span query-exclusion link is driven through
//! the two functions the shipped span scan itself calls --
//! `ravel_query::snapshot_erasure_predicates` over a snapshot the real
//! `Catalog::resolve` produced (which reads the `.dreq`), then
//! `ravel_query::erasure::is_erased_span` per decoded RSPAN row. Because that
//! path has no in-memory read-cache hook, its immediate-exclusion clause is
//! proven in the equivalent pre-rewrite state the cache clause guards: the
//! subject is excluded while its bytes are still physically present in the
//! durable RSPAN object. The physical rewrite/sweep/absence link runs through
//! the same production `run_tick` for all three signals.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::list_all;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

use crate::Cli;
use crate::maintain::{MaintenanceOwnershipMetrics, MaintenanceSafetyMetrics, run_tick_with_clock};
use crate::query::{build_app_state, build_catalog};
use crate::store::build_cache;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// The injected "now" every tick runs at: hour 10_000 (1971), far past the seal
/// margin for the ingest-hour-0 buckets these tests publish, so the rewrite pass
/// sees a sealed bucket with no wall-clock dependence.
const TEST_NOW_NS: i64 = 10_000 * NS_PER_HOUR;

/// The label/attribute key whose value names the erasure subject.
const SUBJECT_KEY: &str = "user_id";
/// The subject to erase, and the bystander that must survive untouched.
const ERASED_SUBJECT: &str = "u123";
const SURVIVING_SUBJECT: &str = "u999";

const METRIC_NAME: &str = "http_requests";
/// The bystander's sample value. A non-integer f64 so a bit-exact survival check
/// (`to_bits`) after the rewrite is meaningful, not satisfiable by a zeroed or
/// truncated payload.
const SURVIVOR_VALUE: f64 = 1234.5;
/// The subject's sample value, distinct so a stray survival is unambiguous.
const SUBJECT_VALUE: f64 = 42.25;
/// The event time of every metric sample: one second past the epoch, inside
/// ingest-hour bucket 0.
const SAMPLE_TS_NS: i64 = 1_000_000_000;

/// [`crate::maintain::DEFAULT_STALLED_AFTER_INTERVALS`] is private to that
/// module; the value is not load-bearing for these tests (no unit ever stalls),
/// so a local copy of the same default keeps the ownership metric constructible.
const STALLED_AFTER_INTERVALS: u32 = 3;

// --- shared maintenance-tick driver ---------------------------------------

/// The full per-tenant fixture a real tick needs, built once and reused across
/// ticks so the memo persists exactly as the running service's does.
struct TickHarness {
    compactor: ravel_maintain::CompactorConfig,
    retention: ravel_maintain::RetentionConfig,
    memo: ravel_maintain::scan::MaintainMemo,
    safety: MaintenanceSafetyMetrics,
    ownership: MaintenanceOwnershipMetrics,
    worker: ravel_maintain::WorkerSet,
}

impl TickHarness {
    fn new() -> Self {
        Self {
            compactor: ravel_maintain::CompactorConfig::default(),
            retention: ravel_maintain::RetentionConfig::default(),
            memo: ravel_maintain::scan::MaintainMemo::with_default_interval(),
            safety: MaintenanceSafetyMetrics::default(),
            ownership: MaintenanceOwnershipMetrics::new(STALLED_AFTER_INTERVALS),
            worker: ravel_maintain::WorkerSet::with_defaults(0),
        }
    }

    fn protection_horizon_ns(&self) -> i64 {
        self.compactor.protection_horizon_ns
    }

    /// Drive one real maintenance tick at `now_ns` over `shard_count` shards --
    /// the same `run_tick_with_clock` the running service calls, only with the
    /// clock injected so the `.dreq` sweep's `protection_horizon` wait is
    /// deterministic (CLAUDE.md: time is injected).
    async fn tick(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        now_ns: i64,
        shard_count: u32,
    ) {
        let clock = ravel_maintain::FixedClock::new(now_ns);
        let live_set = self.worker.solo_live_set();
        run_tick_with_clock(
            &clock,
            store,
            tenant,
            &self.compactor,
            &self.retention,
            shard_count,
            &mut self.memo,
            &self.safety,
            &self.ownership,
            &self.worker,
            &live_set,
        )
        .await;
    }
}

// --- durable-store erasure oracles ----------------------------------------

/// Every `RewriteRecord` present in `(tenant, signal, shard, hour)`, decoded, in
/// key order. A published rewrite is the durable evidence the erasure pass acted
/// on the bucket.
async fn rewrite_records(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    hour: u32,
) -> Vec<ravel_proto::commit::v1::RewriteRecord> {
    let prefix =
        keys::commit_shard_hour_prefix(tenant, signal, shard, hour).expect("bucket prefix");
    let mut found: Vec<String> = list_all(store, &prefix)
        .await
        .expect("list bucket")
        .into_iter()
        .map(|meta| meta.key)
        .filter(|key| key.contains("/rw."))
        .collect();
    found.sort();
    let mut out = Vec::with_capacity(found.len());
    for key in found {
        let got = store.get(&key, GetRange::Full).await.expect("get rewrite");
        out.push(ravel_commit::erasure::decode_rewrite(&got.data).expect("decode rewrite"));
    }
    out
}

/// The label sets a metrics rewrite record's output parts actually hold, read
/// back out of the published segment objects: the "survivors are intact" oracle
/// that reads what a query would read, not what the record claims.
async fn rewritten_metric_labels(
    store: &dyn ObjectStoreBackend,
    record: &ravel_proto::commit::v1::RewriteRecord,
) -> Vec<LabelSet> {
    use ravel_segment::{ReaderLimits, decode_catalog_v5, open_from_full};

    let limits = ReaderLimits::default();
    let mut out = Vec::new();
    for part in &record.parts {
        let key = keys::reconstruct_rewrite_part_key(record, part).expect("part key");
        let got = store.get(&key, GetRange::Full).await.expect("get part");
        let object = got.data;
        let loc = open_from_full(&object, limits).expect("open part");
        for entry in decode_catalog_v5(&loc.footer, &object, limits).expect("decode catalog") {
            out.push(entry.entry.labels);
        }
    }
    out
}

/// Whether `data_key` is still a durable object in the store.
async fn object_exists(store: &dyn ObjectStoreBackend, data_key: &str) -> bool {
    store.get(data_key, GetRange::Full).await.is_ok()
}

// --- erasure request submission (the EJ-T6 `erase submit` write) -----------

/// Submit one erasure request exactly as `ravel-cli erase submit` does: a
/// validated [`ravel_proto::commit::v1::ErasureRequest`] written `CreateIfAbsent`
/// to its `.dreq` key. Returns the key.
async fn submit_erasure_request(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    request_id: Uuid,
    subject: &str,
    now_ns: i64,
) -> String {
    let request = ravel_proto::commit::v1::ErasureRequest {
        format_version: ravel_commit::erasure::FORMAT_VERSION,
        tenant_hash: tenant.0.to_vec(),
        signal: ravel_commit::signal::to_proto(signal) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: now_ns,
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: SUBJECT_KEY.to_string(),
            value: subject.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: "dsar".to_string(),
    };
    ravel_commit::erasure::validate_request(&request).expect("valid request");
    let key = keys::erasure_request_key(tenant, signal, request_id).expect("dreq key");
    store
        .put(
            &key,
            ravel_commit::erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("submit .dreq");
    key
}

// === Metrics: full HTTP e2e chain =========================================

fn metric_labels(subject: &str, disc: &str) -> LabelSet {
    LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: METRIC_NAME.to_string(),
        },
        Label {
            name: SUBJECT_KEY.to_string(),
            value: subject.to_string(),
        },
        // A per-bucket discriminator so the subject can appear as a distinct
        // series in more than one shard (multi-shard coverage) without two
        // segments claiming the same series id.
        Label {
            name: "host".to_string(),
            value: disc.to_string(),
        },
    ])
    .expect("valid labels")
}

/// Publish one sealed metrics bucket at ingest hour 0 in `shard`, holding the
/// erasure subject and one bystander (one sample each), tagged with `disc`. A
/// single L0 input keeps the bucket below `min_compaction_inputs`, so the tick's
/// compaction pass leaves the record set as raw L0 and the erasure rewrite is
/// what acts on it. Returns the input data object's key (the physical-absence
/// oracle target).
async fn publish_metrics_bucket(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    shard: u32,
    disc: &str,
) -> String {
    let tenant_hash = tenant.hash();
    let mut series: Vec<SeriesInput> = [
        (ERASED_SUBJECT, SUBJECT_VALUE),
        (SURVIVING_SUBJECT, SURVIVOR_VALUE),
    ]
    .into_iter()
    .map(|(subject, value)| {
        let labels = metric_labels(subject, disc);
        SeriesInput {
            series_id: SeriesId::compute(tenant, METRIC_NAME, &labels).expect("series id"),
            labels,
            samples: vec![Sample {
                ts_ns: SAMPLE_TS_NS,
                value,
            }],
        }
    })
    .collect();
    series.sort_by_key(|s| s.series_id);

    let writer_id = Uuid::from_u128(0x9100 + u128::from(shard));
    let written = SegmentWriter::write(
        series,
        SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
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
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
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

/// Build a PromQL query app whose read cache is `cache`, so two queries against
/// it share one warm cache.
fn build_metrics_app(
    store: Arc<dyn ObjectStoreBackend>,
    cache: Arc<ravel_cache::Cache<ravel_query::CacheFetchError>>,
    tenant: &TenantId,
    shard_count: u32,
) -> Router {
    let cli = Cli::try_parse_from(["ravel-server"]).expect("default flags parse");
    let catalog = build_catalog(
        Arc::clone(&store),
        shard_count,
        cli.disable_cache,
        cli.cache_max_bytes,
    )
    .expect("catalog");
    let mut tokens = std::collections::HashMap::new();
    tokens.insert("acme-token".to_string(), tenant.clone());
    let state = build_app_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache),
        ravel_query::EngineConfig::default(),
        Arc::new(crate::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        None,
    );
    ravel_query::http::router(state)
}

/// Run an instant PromQL query for [`METRIC_NAME`] at [`SAMPLE_TS_NS`] and return
/// the `(user_id, value)` pairs it yields.
async fn query_metric_subjects(app: &Router) -> Vec<(String, f64)> {
    let uri = format!(
        "/api/v1/query?query={METRIC_NAME}&time={}",
        SAMPLE_TS_NS / 1_000_000_000
    );
    let request = Request::builder()
        .method("GET")
        .uri(&uri)
        .header("authorization", "Bearer acme-token")
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    assert_eq!(response.status(), StatusCode::OK, "query must succeed");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&bytes).expect("parse json");
    assert_eq!(json["status"], "success", "query status: {json}");
    let mut out = Vec::new();
    for entry in json["data"]["result"]
        .as_array()
        .expect("result is an array")
    {
        let user_id = entry["metric"][SUBJECT_KEY]
            .as_str()
            .expect("user_id label")
            .to_string();
        let value: f64 = entry["value"][1]
            .as_str()
            .expect("value string")
            .parse()
            .expect("value parses as f64");
        out.push((user_id, value));
    }
    out
}

fn user_ids(pairs: &[(String, f64)]) -> Vec<String> {
    let mut ids: Vec<String> = pairs.iter().map(|(id, _)| id.clone()).collect();
    ids.sort();
    ids.dedup();
    ids
}

/// The metrics end-to-end acceptance: warm-cache immediate exclusion, a real
/// tick's rewrite with a bit-exact bystander, `.dreq` sweep past the horizon,
/// and physical absence of the input from durable storage -- across TWO shards,
/// proving one `(tenant, signal)`-scoped request reaches every shard.
///
/// Non-vacuous flip: delete the `submit_erasure_request` call. Then the
/// post-erase query returns `u123` again (the exclusion assertion fires), and no
/// input is ever superseded so the physical-absence assertion fires too. Both
/// assertions therefore depend on the pipeline, not on a query that returns
/// nothing.
#[tokio::test]
async fn metrics_erasure_reaches_query_cache_rewrite_and_physical_absence() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant_id = TenantId::new("acme");
    let tenant = tenant_id.hash();
    const SHARD_COUNT: u32 = 2;

    // Two sealed buckets: the subject appears as a distinct series in shard 0
    // and shard 1. One (tenant, Metrics) erasure request must reach both.
    let data_key_s0 = publish_metrics_bucket(store.as_ref(), &tenant_id, 0, "a").await;
    let data_key_s1 = publish_metrics_bucket(store.as_ref(), &tenant_id, 1, "b").await;

    let cache = build_cache(&Cli::try_parse_from(["ravel-server"]).expect("cli"))
        .expect("cache enabled by default");
    let app = build_metrics_app(
        Arc::clone(&store),
        Arc::clone(&cache),
        &tenant_id,
        SHARD_COUNT,
    );

    // Pre-erase: both subjects reachable in both shards, and the query warms the
    // read cache with the segment bytes (which include the subject's).
    let before = query_metric_subjects(&app).await;
    assert_eq!(
        user_ids(&before),
        vec![ERASED_SUBJECT.to_string(), SURVIVING_SUBJECT.to_string()],
        "pre-erase, both the subject and the bystander are reachable"
    );
    assert_eq!(
        before.iter().filter(|(id, _)| id == ERASED_SUBJECT).count(),
        2,
        "the subject is present as a distinct series in both shards pre-erase"
    );
    assert!(
        !cache.is_empty(),
        "the pre-erase query must have warmed the read cache with the segment bytes"
    );

    // Submit the erasure request (the EJ-T6 write). THIS is the flip line: remove
    // it and the two post-erase exclusion assertions below both fail.
    let request_id = Uuid::from_u128(0xE7A5);
    let dreq_key = submit_erasure_request(
        store.as_ref(),
        &tenant,
        Signal::Metrics,
        request_id,
        ERASED_SUBJECT,
        TEST_NOW_NS,
    )
    .await;
    let done_key =
        keys::erasure_completion_key(&tenant, Signal::Metrics, request_id).expect("done key");

    // Immediate exclusion from the WARM cache: the scan-time filter runs
    // post-cache, so the subject is gone even though its bytes are still cached
    // AND still physically present in the durable input objects.
    //
    // Snapshot the cache's hit counter before the post-erase query so the
    // assertion after it proves the exclusion held ON A CACHE HIT: the query
    // served the segment bytes from the warm cache (not a re-fetch from store)
    // and STILL filtered the subject, direct evidence the ADR-0064 scan-time
    // filter runs after the cache, not that the cache was merely warm.
    let hits_before = cache.metrics().snapshot().hits;
    let after = query_metric_subjects(&app).await;
    assert!(
        cache.metrics().snapshot().hits > hits_before,
        "the post-erase query must be served FROM the warm cache (a cache hit), not re-fetched \
         from store: only then does its exclusion of the subject prove the scan-time filter runs \
         post-cache"
    );
    assert_eq!(
        user_ids(&after),
        vec![SURVIVING_SUBJECT.to_string()],
        "post-erase, a query resolving after the request ack excludes the subject from the warm cache"
    );
    assert!(
        object_exists(store.as_ref(), &data_key_s0).await
            && object_exists(store.as_ref(), &data_key_s1).await,
        "exclusion is immediate and logical: the durable input objects still hold the subject's \
         bytes at this point, so the query filtered them, it did not read an emptied store"
    );

    // A real tick: the rewrite runs in both shards and the request completes
    // (every in-scope bucket now carries a rewrite naming it).
    let mut harness = TickHarness::new();
    harness
        .tick(store.as_ref(), &tenant, TEST_NOW_NS, SHARD_COUNT)
        .await;

    for shard in 0..SHARD_COUNT {
        let records = rewrite_records(store.as_ref(), &tenant, Signal::Metrics, shard, 0).await;
        assert_eq!(
            records.len(),
            1,
            "shard {shard} must carry exactly one RewriteRecord after the tick"
        );
        let record = &records[0];
        assert_eq!(record.drops.len(), 1, "the one pending request is applied");
        assert_eq!(record.drops[0].request_id, request_id.to_string());
        assert_eq!(
            record.drops[0].dropped_count, 1,
            "exactly the subject's one sample is dropped in shard {shard}"
        );
        let survivors = rewritten_metric_labels(store.as_ref(), record).await;
        assert_eq!(
            survivors,
            vec![metric_labels(
                SURVIVING_SUBJECT,
                if shard == 0 { "a" } else { "b" }
            )],
            "only the bystander series survives the rewrite in shard {shard}"
        );
    }

    assert!(
        object_exists(store.as_ref(), &done_key).await,
        "every in-scope bucket carries the request, so the tick writes .done"
    );
    assert!(
        object_exists(store.as_ref(), &dreq_key).await,
        ".dreq survives until protection_horizon has elapsed past .done"
    );

    // Past the horizon: the `.dreq` (which carries the subject identifier) is
    // swept, and the superseded input objects are physically deleted.
    let past_horizon = TEST_NOW_NS + harness.protection_horizon_ns() + 1;
    harness
        .tick(store.as_ref(), &tenant, past_horizon, SHARD_COUNT)
        .await;

    assert!(
        object_exists(store.as_ref(), &done_key).await,
        ".done is permanent erasure evidence"
    );
    assert!(
        !object_exists(store.as_ref(), &dreq_key).await,
        ".dreq must be swept once its .done is older than protection_horizon"
    );
    assert!(
        !object_exists(store.as_ref(), &data_key_s0).await
            && !object_exists(store.as_ref(), &data_key_s1).await,
        "physical absence: the pre-rewrite input objects holding the subject's bytes are \
         superseded and swept from durable storage"
    );

    // Post-sweep, the `.dreq` is gone, so exclusion now rests PURELY on the
    // physical rewrite: a fresh query still excludes the subject and returns the
    // bystander's sample value bit-exact.
    let cold_cache =
        build_cache(&Cli::try_parse_from(["ravel-server"]).expect("cli")).expect("cache");
    let fresh_app = build_metrics_app(Arc::clone(&store), cold_cache, &tenant_id, SHARD_COUNT);
    let post_sweep = query_metric_subjects(&fresh_app).await;
    assert_eq!(
        user_ids(&post_sweep),
        vec![SURVIVING_SUBJECT.to_string()],
        "after the .dreq is swept, the subject stays excluded on the strength of the physical \
         rewrite alone"
    );
    for (_, value) in post_sweep.iter().filter(|(id, _)| id == SURVIVING_SUBJECT) {
        assert_eq!(
            value.to_bits(),
            SURVIVOR_VALUE.to_bits(),
            "the bystander's sample payload survives the rewrite bit-exact"
        );
    }
}

// === Logs: full HTTP e2e chain (SQL surface) ==============================

#[cfg(feature = "sql")]
mod logs {
    use super::*;
    use axum::http::header;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
    use ravel_types::logstream::log_stream_id;

    /// The subject's and bystander's log bodies, the query-visible witnesses.
    const SUBJECT_BODY: &str = "subject-log-line";
    const SURVIVOR_BODY: &str = "bystander-log-line";
    /// Every log event's timestamp: inside ingest-hour bucket 0 and inside the
    /// query window below.
    const LOG_TS_NS: i64 = 100;
    /// The query clock, frozen four hours past the epoch so the `start: 0`
    /// window resolves to an ordinary span rather than tripping the issue #635
    /// window-cost ceiling (mirrors `tests/cache_attachment_logs.rs`). Wholly
    /// independent of the tick clock, which runs at [`TEST_NOW_NS`].
    const LOG_QUERY_NOW_NS: i64 = 4 * NS_PER_HOUR;

    struct FixedQueryClock;
    impl ravel_ingest::Clock for FixedQueryClock {
        fn now_ns(&self) -> i64 {
            LOG_QUERY_NOW_NS
        }
    }

    fn log_record(user_id: &str, body: &str) -> LogRecord {
        // One shared stream (same resource) so the subject and bystander land in
        // the same bucket; the subject is named by a PER-RECORD attribute, which
        // is what the logs erasure filter matches against the merged view.
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("checkout".to_string()),
        )];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: LOG_TS_NS,
            observed_ts_ns: LOG_TS_NS,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: body.into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![(SUBJECT_KEY.to_string(), AttrValue::Str(user_id.to_string()))],
        }
    }

    /// Publish one sealed RLOG bucket (ingest hour 0, shard 0) holding the
    /// subject and bystander records. Returns the input data object key.
    async fn publish_logs_bucket(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> String {
        let tenant_hash = tenant.hash();
        let records = [
            log_record(ERASED_SUBJECT, SUBJECT_BODY),
            log_record(SURVIVING_SUBJECT, SURVIVOR_BODY),
        ];
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut streams = std::collections::HashSet::new();
        for rec in &records {
            min_ts = min_ts.min(rec.ts_ns);
            max_ts = max_ts.max(rec.ts_ns);
            streams.insert(rec.stream_id);
        }

        let writer_id = Uuid::from_u128(0x1_0000);
        let mut writer = RlogWriter::new(
            RlogConfig::default(),
            ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for rec in &records {
            writer.push(rec.clone()).expect("push log record");
        }
        let bytes = writer.finish().expect("finish rlog object");
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: streams.len() as u64,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid log commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put log data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish log commit");
        data_key
    }

    fn build_logs_app(
        store: Arc<dyn ObjectStoreBackend>,
        cache: Arc<ravel_cache::Cache<ravel_query::CacheFetchError>>,
        tenant: &TenantId,
    ) -> Router {
        let cli = Cli::try_parse_from(["ravel-server"]).expect("default flags parse");
        let catalog = build_catalog(
            Arc::clone(&store),
            1,
            cli.disable_cache,
            cli.cache_max_bytes,
        )
        .expect("catalog");
        let mut tokens = std::collections::HashMap::new();
        tokens.insert("acme-token".to_string(), tenant.clone());
        let mut sql_state = crate::query::build_sql_state(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(tokens)),
            Some(cache),
            ravel_query::EngineConfig::default(),
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            ravel_query::QueryAdmissionController::shared(
                ravel_query::QueryConcurrencyLimit::Unlimited,
            ),
        )
        .expect("sql state");
        sql_state.clock = Arc::new(FixedQueryClock);
        crate::sql::router(sql_state)
    }

    /// `SELECT body FROM logs`, returning the bodies it yields, sorted.
    async fn query_log_bodies(app: &Router) -> Vec<String> {
        let payload = serde_json::json!({
            "query": "SELECT body FROM logs ORDER BY body",
            "start": 0.0,
            "end": LOG_QUERY_NOW_NS as f64 / 1_000_000_000.0,
        })
        .to_string();
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/sql")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer acme-token")
            .body(Body::from(payload))
            .expect("build request");
        let response = app.clone().oneshot(request).await.expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK, "sql query must succeed");
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: Value = serde_json::from_slice(&bytes).expect("parse json");
        assert_eq!(json["status"], "success", "sql status: {json}");
        let mut bodies: Vec<String> = json["data"]["rows"]
            .as_array()
            .expect("rows is an array")
            .iter()
            .map(|row| row[0].as_str().expect("body string").to_string())
            .collect();
        bodies.sort();
        bodies
    }

    /// The logs end-to-end acceptance, through the real `/api/v1/sql` surface:
    /// warm-cache immediate exclusion, a real tick's rewrite, `.dreq` sweep past
    /// the horizon, and physical absence of the input from durable storage.
    ///
    /// Non-vacuous flip: delete the `submit_erasure_request` call. The post-erase
    /// query then returns `SUBJECT_BODY` again (the exclusion assertion fires) and
    /// the input is never superseded (the physical-absence assertion fires).
    #[tokio::test]
    async fn logs_erasure_reaches_query_cache_rewrite_and_physical_absence() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        let data_key = publish_logs_bucket(store.as_ref(), &tenant_id).await;

        let cache = build_cache(&Cli::try_parse_from(["ravel-server"]).expect("cli"))
            .expect("cache enabled by default");
        let app = build_logs_app(Arc::clone(&store), Arc::clone(&cache), &tenant_id);

        // Pre-erase: both bodies reachable; the query warms the read cache.
        let before = query_log_bodies(&app).await;
        assert_eq!(
            before,
            vec![SURVIVOR_BODY.to_string(), SUBJECT_BODY.to_string()],
            "pre-erase, both the subject and the bystander log lines are reachable"
        );
        assert!(
            !cache.is_empty(),
            "the pre-erase logs query must have warmed the read cache with the RLOG bytes"
        );

        // Submit the erasure request. THIS is the flip line.
        let request_id = Uuid::from_u128(0xE7B0);
        let dreq_key = submit_erasure_request(
            store.as_ref(),
            &tenant,
            Signal::Logs,
            request_id,
            ERASED_SUBJECT,
            TEST_NOW_NS,
        )
        .await;
        let done_key =
            keys::erasure_completion_key(&tenant, Signal::Logs, request_id).expect("done key");

        // Immediate exclusion from the warm cache while the input still holds
        // the subject's bytes. Snapshot the hit counter first so the assertion
        // proves the exclusion held ON A CACHE HIT: the post-erase SQL query
        // served the RLOG bytes from the warm cache (not a re-fetch) and STILL
        // filtered the subject, direct evidence the scan-time filter runs
        // post-cache.
        let hits_before = cache.metrics().snapshot().hits;
        let after = query_log_bodies(&app).await;
        assert!(
            cache.metrics().snapshot().hits > hits_before,
            "the post-erase SQL query must be served FROM the warm cache (a cache hit), not \
             re-fetched from store: only then does its exclusion of the subject prove the \
             scan-time filter runs post-cache"
        );
        assert_eq!(
            after,
            vec![SURVIVOR_BODY.to_string()],
            "post-erase, the SQL query excludes the subject's log line from the warm cache"
        );
        assert!(
            object_exists(store.as_ref(), &data_key).await,
            "the durable RLOG input still holds the subject's bytes: exclusion was logical"
        );

        // A real tick: the logs bucket is rewritten and the request completes.
        let mut harness = TickHarness::new();
        harness.tick(store.as_ref(), &tenant, TEST_NOW_NS, 1).await;

        let records = rewrite_records(store.as_ref(), &tenant, Signal::Logs, 0, 0).await;
        assert_eq!(records.len(), 1, "the tick publishes one RewriteRecord");
        assert_eq!(
            records[0].drops.len(),
            1,
            "the one pending request is applied"
        );
        assert_eq!(records[0].drops[0].request_id, request_id.to_string());
        assert_eq!(
            records[0].drops[0].dropped_count, 1,
            "exactly the subject's one log record is dropped"
        );
        assert!(
            object_exists(store.as_ref(), &done_key).await,
            "every in-scope bucket carries the request, so the tick writes .done"
        );
        assert!(
            object_exists(store.as_ref(), &dreq_key).await,
            ".dreq survives until protection_horizon has elapsed"
        );

        // Past the horizon: `.dreq` swept, input physically gone.
        let past_horizon = TEST_NOW_NS + harness.protection_horizon_ns() + 1;
        harness.tick(store.as_ref(), &tenant, past_horizon, 1).await;

        assert!(
            object_exists(store.as_ref(), &done_key).await,
            ".done is permanent erasure evidence"
        );
        assert!(
            !object_exists(store.as_ref(), &dreq_key).await,
            ".dreq must be swept once its .done is older than protection_horizon"
        );
        assert!(
            !object_exists(store.as_ref(), &data_key).await,
            "physical absence: the pre-rewrite RLOG input is superseded and swept"
        );

        // Post-sweep, `.dreq` gone: exclusion rests purely on the physical
        // rewrite, and the bystander's line survives.
        let cold_cache =
            build_cache(&Cli::try_parse_from(["ravel-server"]).expect("cli")).expect("cache");
        let fresh_app = build_logs_app(Arc::clone(&store), cold_cache, &tenant_id);
        let post_sweep = query_log_bodies(&fresh_app).await;
        assert_eq!(
            post_sweep,
            vec![SURVIVOR_BODY.to_string()],
            "after the .dreq is swept, the subject stays excluded on the physical rewrite alone \
             and the bystander survives"
        );
    }
}

// === Spans: scan-layer query exclusion + full physical rewrite/sweep ======
//
// Spans have no ravel-server query endpoint (the SQL executor routes only
// `logs` and `samples`), so the immediate-exclusion link is driven through the
// two functions the shipped span scan (`ravel_sql::spans_scan::prepare_partition`)
// itself calls -- `ravel_query::snapshot_erasure_predicates` over a snapshot the
// real `Catalog::resolve` produced (which reads the `.dreq`), then
// `ravel_query::erasure::is_erased_span` per decoded RSPAN row -- rather than
// through DataFusion, which would require adding datafusion as a dev-dependency
// only to re-run the same two functions. The physical rewrite/sweep/absence link
// runs through the real production `run_tick`, exactly as for metrics and logs.
mod spans {
    use super::*;
    use ravel_rspan::{RspanConfig, RspanReader, RspanWriter, SpanQuery, SpanRecord, StatusCode};
    use ravel_types::TimeRange;

    const SUBJECT_NAME: &str = "subject-span";
    const SURVIVOR_NAME: &str = "bystander-span";
    const TRACE_ID: [u8; 16] = [0x11; 16];

    fn span_record(span_id: u8, name: &str, user_id: &str, start: i64) -> SpanRecord {
        SpanRecord {
            trace_id: TRACE_ID,
            span_id: [span_id; 8],
            parent_span_id: None,
            name: name.to_string(),
            start_ts_ns: start,
            end_ts_ns: start + 10,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![(SUBJECT_KEY.to_string(), user_id.to_string())],
        }
    }

    /// Publish one sealed RSPAN bucket (ingest hour 0, shard 0) holding the
    /// subject and bystander spans. Returns the input data object key.
    async fn publish_spans_bucket(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> String {
        let tenant_hash = tenant.hash();
        let records = [
            span_record(0, SUBJECT_NAME, ERASED_SUBJECT, 100),
            span_record(1, SURVIVOR_NAME, SURVIVING_SUBJECT, 200),
        ];
        let min_ts = records
            .iter()
            .map(|r| r.start_ts_ns)
            .min()
            .expect("nonempty");
        let max_ts = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");

        let writer_id = Uuid::from_u128(0x2_0000);
        let mut writer = RspanWriter::new(
            RspanConfig::default(),
            ravel_rspan::ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for rec in &records {
            writer.push(rec.clone());
        }
        let bytes = writer.finish().expect("finish rspan object");
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Spans,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: 1,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid span commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put span data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish span commit");
        data_key
    }

    /// The span names a query would return, computed exactly as the shipped span
    /// scan does: resolve the `Signal::Spans` snapshot through the real catalog
    /// (which reads the `.dreq` into `pending_erasure`), derive the scan
    /// predicates with `snapshot_erasure_predicates`, decode every RSPAN row in
    /// the snapshot's segments, and drop the rows `is_erased_span` matches.
    async fn query_span_names(
        store: &Arc<dyn ObjectStoreBackend>,
        tenant: &TenantHash,
    ) -> Vec<String> {
        let cli = Cli::try_parse_from(["ravel-server"]).expect("cli");
        let catalog = build_catalog(Arc::clone(store), 1, cli.disable_cache, cli.cache_max_bytes)
            .expect("catalog");
        let snapshot = catalog
            .resolve(
                tenant,
                Signal::Spans,
                TimeRange {
                    start_ns: 0,
                    end_ns: TEST_NOW_NS,
                },
                &[],
                TEST_NOW_NS,
            )
            .await
            .expect("resolve spans snapshot");
        let erasure = ravel_query::snapshot_erasure_predicates(&snapshot);

        let mut names = Vec::new();
        for segment in &snapshot.segments {
            let bytes = store
                .get(&segment.data_object_key, GetRange::Full)
                .await
                .expect("get span object")
                .data;
            let reader =
                RspanReader::new(&bytes, &RspanConfig::default()).expect("open rspan object");
            let (rows, _stats) = reader
                .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
                .expect("scan");
            for row in rows {
                if !ravel_query::erasure::is_erased_span(&row.attrs, row.start_ts_ns, &erasure) {
                    names.push(row.name);
                }
            }
        }
        names.sort();
        names
    }

    /// The spans end-to-end acceptance: the shipped scan-decision functions
    /// exclude the subject the instant its `.dreq` is durable (while the durable
    /// RSPAN input still holds its bytes), and a real tick then rewrites, sweeps
    /// the `.dreq`, and physically removes the input.
    ///
    /// Non-vacuous flip: delete the `submit_erasure_request` call. The post-erase
    /// resolve then carries no pending erasure, so `is_erased_span` matches
    /// nothing and the subject span reappears (the exclusion assertion fires),
    /// and no input is superseded (the physical-absence assertion fires).
    #[tokio::test]
    async fn spans_erasure_reaches_scan_exclusion_rewrite_and_physical_absence() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        let data_key = publish_spans_bucket(store.as_ref(), &tenant_id).await;

        // Pre-erase: both spans reachable through the real resolve + scan
        // decision.
        let before = query_span_names(&store, &tenant).await;
        assert_eq!(
            before,
            vec![SURVIVOR_NAME.to_string(), SUBJECT_NAME.to_string()],
            "pre-erase, both the subject and the bystander span are reachable"
        );

        // Submit the erasure request. THIS is the flip line.
        let request_id = Uuid::from_u128(0xE7C0);
        let dreq_key = submit_erasure_request(
            store.as_ref(),
            &tenant,
            Signal::Spans,
            request_id,
            ERASED_SUBJECT,
            TEST_NOW_NS,
        )
        .await;
        let done_key =
            keys::erasure_completion_key(&tenant, Signal::Spans, request_id).expect("done key");

        // Immediate exclusion: the resolve reads the `.dreq`, the scan drops the
        // subject, yet the durable RSPAN input still holds its bytes.
        let after = query_span_names(&store, &tenant).await;
        assert_eq!(
            after,
            vec![SURVIVOR_NAME.to_string()],
            "post-erase, the span scan excludes the subject the instant its .dreq is durable"
        );
        assert!(
            object_exists(store.as_ref(), &data_key).await,
            "the durable RSPAN input still holds the subject's bytes: exclusion was logical"
        );

        // A real tick: the spans bucket is rewritten and the request completes.
        let mut harness = TickHarness::new();
        harness.tick(store.as_ref(), &tenant, TEST_NOW_NS, 1).await;

        let records = rewrite_records(store.as_ref(), &tenant, Signal::Spans, 0, 0).await;
        assert_eq!(
            records.len(),
            1,
            "the tick publishes one RewriteRecord for spans"
        );
        assert_eq!(
            records[0].drops.len(),
            1,
            "the one pending request is applied"
        );
        assert_eq!(records[0].drops[0].request_id, request_id.to_string());
        assert_eq!(
            records[0].drops[0].dropped_count, 1,
            "exactly the subject's one span is dropped"
        );
        assert!(
            object_exists(store.as_ref(), &done_key).await,
            "every in-scope bucket carries the request, so the tick writes .done"
        );
        assert!(
            object_exists(store.as_ref(), &dreq_key).await,
            ".dreq survives until protection_horizon has elapsed"
        );

        // Past the horizon: `.dreq` swept, input physically gone.
        let past_horizon = TEST_NOW_NS + harness.protection_horizon_ns() + 1;
        harness.tick(store.as_ref(), &tenant, past_horizon, 1).await;

        assert!(
            object_exists(store.as_ref(), &done_key).await,
            ".done is permanent erasure evidence"
        );
        assert!(
            !object_exists(store.as_ref(), &dreq_key).await,
            ".dreq must be swept once its .done is older than protection_horizon"
        );
        assert!(
            !object_exists(store.as_ref(), &data_key).await,
            "physical absence: the pre-rewrite RSPAN input is superseded and swept"
        );

        // Post-sweep, `.dreq` gone: the resolve carries no pending erasure, so
        // exclusion now rests purely on the physical rewrite, and the bystander
        // span survives in the rewrite output.
        let post_sweep = query_span_names(&store, &tenant).await;
        assert_eq!(
            post_sweep,
            vec![SURVIVOR_NAME.to_string()],
            "after the .dreq is swept, the subject stays excluded on the physical rewrite alone \
             and the bystander survives"
        );
    }
}

// === Failure path: a faulted rewrite-record publish is safe ===============

mod fault {
    use super::*;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };

    /// A fault during the rewrite-record publish must leave the system in a safe
    /// state: the rewrite does not complete, so no `.done` is written and the
    /// `.dreq` is kept (never removed without its `.done`), and -- critically for
    /// an irreversible operation -- the pre-rewrite input is NOT deleted (no
    /// partial erasure), so the subject's data is fully recoverable. A later
    /// fault-free tick completes the erasure and, past the horizon, physically
    /// removes the input.
    ///
    /// The fault targets the FIRST `Put` whose key contains `/rw.` -- the
    /// selective-erasure rewrite record. Its output parts may land, but with no
    /// live rewrite record naming them the input stays live and the request stays
    /// pending, exactly the state the ordering guarantee promises.
    #[tokio::test]
    async fn a_faulted_rewrite_publish_deletes_nothing_and_completes_on_retry() {
        let inner = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        let data_key = publish_metrics_bucket(&inner, &tenant_id, 0, "a").await;

        let request_id = Uuid::from_u128(0xE7D0);
        let dreq_key = submit_erasure_request(
            &inner,
            &tenant,
            Signal::Metrics,
            request_id,
            ERASED_SUBJECT,
            TEST_NOW_NS,
        )
        .await;
        let done_key =
            keys::erasure_completion_key(&tenant, Signal::Metrics, request_id).expect("done key");

        // Fault the FIRST publish of a rewrite record (`/rw.`) and nothing else,
        // so the recovery ticks run against an otherwise untouched store.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Transient("rewrite record publish unavailable".into()),
            )
            .with_key_contains("/rw.")
            .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);

        let mut harness = TickHarness::new();

        // Tick 1 under the fault: the rewrite publish fails.
        harness.tick(&store, &tenant, TEST_NOW_NS, 1).await;

        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Transient),
            1,
            "the injected rewrite-publish fault must actually have fired"
        );
        assert!(
            rewrite_records(&store, &tenant, Signal::Metrics, 0, 0)
                .await
                .is_empty(),
            "a faulted rewrite publishes no live rewrite record"
        );
        assert!(
            !object_exists(&store, &done_key).await,
            "no rewrite completed, so no .done may be written"
        );
        assert!(
            object_exists(&store, &dreq_key).await,
            "the .dreq is kept: it is never removed without its .done"
        );
        assert!(
            object_exists(&store, &data_key).await,
            "no partial deletion: the pre-rewrite input still holds the subject's bytes, so the \
             erasure is fully recoverable after the fault"
        );

        // Tick 2, past the one-shot fault: the rewrite completes.
        harness.tick(&store, &tenant, TEST_NOW_NS, 1).await;
        assert_eq!(
            rewrite_records(&store, &tenant, Signal::Metrics, 0, 0)
                .await
                .len(),
            1,
            "the fault-free retry publishes the rewrite record"
        );
        assert!(
            object_exists(&store, &done_key).await,
            "the fault-free retry completes the request"
        );
        assert!(
            object_exists(&store, &dreq_key).await,
            ".dreq is kept until protection_horizon elapses past .done"
        );

        // Tick 3, past the horizon: the request is swept and the input physically
        // removed -- the erasure the fault deferred is now durably complete.
        let past_horizon = TEST_NOW_NS + harness.protection_horizon_ns() + 1;
        harness.tick(&store, &tenant, past_horizon, 1).await;
        assert!(
            object_exists(&store, &done_key).await,
            ".done is permanent erasure evidence"
        );
        assert!(
            !object_exists(&store, &dreq_key).await,
            ".dreq is swept once its .done is older than protection_horizon"
        );
        assert!(
            !object_exists(&store, &data_key).await,
            "physical absence: the input the fault preserved is now superseded and swept"
        );
    }
}
