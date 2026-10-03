//! Prove-the-test coverage for ADR-0073 (recent-hours read path):
//! the sealed-set segment cap and the S3 request budget replace the single
//! whole-snapshot `max_segments` check that made a hot tenant's newest 1-2
//! hours unqueryable and could make `resolve_min_token` violate
//! read-your-write (ADR-0073 "Context").
//!
//! Every test names the exact line whose reversion makes it fail:
//! `crates/ravel-query/src/engine.rs`'s `resolve_bounded`, which used to
//! compare `snapshot.segments.len()` against `self.config.max_segments` and
//! now calls `segment_admission::admit(&snapshot, &origins, &self.config)`,
//! counting only `origins.sealed_count`.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_promql::{LabelMatcher, Value};
use ravel_query::{EngineConfig, QueryEngine, QueryError, RequestLimit};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash,
    TenantId, TimeRange,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// First instant at which `hour` is sealed, mirroring `postings_pruning.rs`
/// and `bytes_scanned_budget.rs`'s fixtures.
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn tenant(id: &str) -> TenantId {
    TenantId::new(id.to_string())
}

fn catalog(store: Arc<dyn ObjectStoreBackend>) -> Catalog {
    Catalog::new(store, CatalogConfig::default()).expect("catalog")
}

/// Writes one real RSEG v6 segment carrying a single sample and publishes
/// its commit record, returning the read-your-write token and the segment
/// data-object key. Distinct `writer_seq` values keep segments distinct data
/// objects and commit records even when they share `ingest_hour_bucket`.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    metric: &str,
    ts_ns: i64,
    value: f64,
) -> (CommitToken, String) {
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value }],
    };
    let written = SegmentWriter::write(vec![input], identity, bounds).expect("write v6 segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 0,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

fn name_matcher(metric: &str) -> Vec<LabelMatcher> {
    vec![LabelMatcher::equal(METRIC_NAME_LABEL, metric)]
}

/// Sorted `(metric_name, ts_ns, value_bits)` triples of a range vector
/// (`Value::Matrix`), for order-independent, exact (bit-level) comparisons.
fn matrix_bits(value: &Value) -> Vec<(String, i64, u64)> {
    match value {
        Value::Matrix(m) => {
            let mut out: Vec<(String, i64, u64)> = m
                .iter()
                .flat_map(|(labels, samples)| {
                    let name = labels.get(METRIC_NAME_LABEL).unwrap_or("").to_string();
                    samples
                        .iter()
                        .map(move |s| (name.clone(), s.ts_ns, s.value.to_bits()))
                })
                .collect();
            out.sort();
            out
        }
        other => panic!("expected range vector (matrix), got {other:?}"),
    }
}

/// Sealed (below-watermark) segments still count against `max_segments`;
/// recent (above-watermark, never folded) segments must not (ADR-0073
/// decision 2). One sealed segment plus five recent segments, `max_segments:
/// 1`: `resolve_bounded` counts only `origins.sealed_count` (1), not the
/// whole snapshot's 6 segments, so the query is admitted.
#[tokio::test]
async fn recent_hours_exempt_from_segment_cap() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let sealed_hour = 9_001u32;
    let recent_hour = sealed_hour + 1;
    // now_at_seal(sealed_hour) is exactly MARGIN_NS into recent_hour: sealing
    // sealed_hour and (separately) making recent_hour's listing window live
    // both fall out of the same instant.
    let now = now_at_seal(sealed_hour);

    publish_segment(
        store.as_ref(),
        &tid,
        th,
        1,
        sealed_hour,
        "m",
        i64::from(sealed_hour) * NS_PER_HOUR + 10 * 60 * NS_PER_SEC,
        1.0,
    )
    .await;

    let cat = catalog(store.clone());
    cat.fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold seals sealed_hour");

    for seq in 2..7u64 {
        publish_segment(
            store.as_ref(),
            &tid,
            th,
            seq,
            recent_hour,
            "m",
            i64::from(recent_hour) * NS_PER_HOUR + (seq as i64) * 60 * NS_PER_SEC,
            seq as f64,
        )
        .await;
    }

    let config = EngineConfig {
        max_segments: 1,
        ..EngineConfig::default()
    };
    let engine = QueryEngine::new(Arc::new(cat), store, config);

    let window = TimeRange {
        start_ns: i64::from(sealed_hour) * NS_PER_HOUR,
        end_ns: i64::from(recent_hour + 1) * NS_PER_HOUR,
    };
    let (series, stats) = engine
        .resolve_series_with_stats(
            th,
            &name_matcher("m"),
            window,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect("1 sealed + 5 recent segments must be admitted under max_segments=1");
    assert_eq!(series.len(), 1, "one series id shared by all 6 segments");
    assert_eq!(
        stats.segments_fetched, 6,
        "all 6 segments were fetched, unpruned"
    );
}

/// A segment resolved only via an explicit `min_commit_token` -- never
/// listed (its hour is outside the query window), never folded -- must be
/// admitted regardless of `max_segments`, even set to 0 (ADR-0073 decision
/// 2's other exemption, and the read-your-write fix ADR-0073's Context
/// describes: `resolve_min_token` used to insert into the same counted map).
/// Pre-fix, the lone token-resolved segment alone (count 1) exceeded
/// `max_segments: 0` and the query failed with `TooManySegments`.
#[tokio::test]
async fn token_segments_always_admitted() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    // Far outside the query window below, and never folded: the only path
    // to this segment is the explicit token.
    let token_hour = 20_000u32;
    let ts_ns = i64::from(token_hour) * NS_PER_HOUR + 100 * NS_PER_SEC;
    let (token, _key) =
        publish_segment(store.as_ref(), &tid, th, 1, token_hour, "m", ts_ns, 7.0).await;

    let cat = catalog(store.clone());
    let config = EngineConfig {
        max_segments: 0,
        ..EngineConfig::default()
    };
    let engine = QueryEngine::new(Arc::new(cat), store, config);

    // Window covers hour 0 only; the token segment's hour (20,000) is never
    // listed. `now` sits inside hour 0, keeping the listing window small.
    let window = TimeRange {
        start_ns: 0,
        end_ns: NS_PER_HOUR,
    };
    let now = 30 * 60 * NS_PER_SEC;
    let (series, stats) = engine
        .resolve_series_with_stats(
            th,
            &name_matcher("m"),
            window,
            &[token],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect("token-resolved segment must be admitted despite max_segments=0");
    assert_eq!(series.len(), 1);
    assert_eq!(
        stats.segments_fetched, 1,
        "the token-resolved segment, nothing listed"
    );
}

/// Sealed-set semantics unchanged (regression proof): four sealed segments
/// sharing one metric (so postings pruning excludes none of them) against
/// `max_segments: 2` must still be refused with `TooManySegments`, carrying
/// the sealed count. ADR-0073 decision 2 exempts recent/token-resolved
/// segments only; it must not accidentally widen the sealed-set cap itself.
#[tokio::test]
async fn sealed_set_still_capped_when_oversized() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 9_101u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;

    for seq in 1..5u64 {
        publish_segment(store.as_ref(), &tid, th, seq, hour, "m", ts, seq as f64).await;
    }
    let cat = catalog(store.clone());
    cat.fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");

    let config = EngineConfig {
        max_segments: 2,
        ..EngineConfig::default()
    };
    let engine = QueryEngine::new(Arc::new(cat), store, config);
    let window = TimeRange {
        start_ns: i64::from(hour) * NS_PER_HOUR,
        end_ns: i64::from(hour + 1) * NS_PER_HOUR,
    };
    let err = engine
        .resolve_series(
            th,
            &name_matcher("m"),
            window,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect_err("4 sealed segments over max_segments=2 must still be refused");
    match err {
        QueryError::TooManySegments { count, max } => {
            assert_eq!(count, 4);
            assert_eq!(max, 2);
        }
        other => panic!("expected TooManySegments, got {other:?}"),
    }
}

/// Exactness (ADR-0073 decision 5): the same underlying samples, queried
/// once while the newer hour is still recent (listed live) and once more
/// after a second fold seals that hour too, must return bit-identical
/// results. Sealing only moves a segment's catalog representation from
/// listed-live to extracted-from-a-folded-part; it must never change what a
/// query sees. Two independent catalog/engine instances over the same store
/// rule out any head-cache carryover from the first resolve.
#[tokio::test]
async fn exactness_stable_across_fold() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let sealed_hour = 9_201u32;
    let recent_hour = sealed_hour + 1;
    let now1 = now_at_seal(sealed_hour);

    let ts_sealed = i64::from(sealed_hour) * NS_PER_HOUR + 10 * 60 * NS_PER_SEC;
    let ts_recent_a = i64::from(recent_hour) * NS_PER_HOUR + 5 * 60 * NS_PER_SEC;
    let ts_recent_b = i64::from(recent_hour) * NS_PER_HOUR + 40 * 60 * NS_PER_SEC;

    publish_segment(
        store.as_ref(),
        &tid,
        th,
        1,
        sealed_hour,
        "m",
        ts_sealed,
        1.0,
    )
    .await;
    publish_segment(
        store.as_ref(),
        &tid,
        th,
        2,
        recent_hour,
        "m",
        ts_recent_a,
        2.0,
    )
    .await;
    publish_segment(
        store.as_ref(),
        &tid,
        th,
        3,
        recent_hour,
        "m",
        ts_recent_b,
        3.0,
    )
    .await;

    let config = EngineConfig::default();
    let eval_ns = i64::from(recent_hour + 1) * NS_PER_HOUR;
    let eval_t_ms = eval_ns / 1_000_000;

    let cat_a = catalog(store.clone());
    cat_a
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now1, &[], None)
        .await
        .expect("fold seals sealed_hour only");
    let engine_a = QueryEngine::new(Arc::new(cat_a), store.clone(), config);
    // A bare range-vector selector ("m[3h]") returns every raw sample in the
    // lookback window unresampled, so this is a direct read of the mixed
    // sealed+recent snapshot's contents.
    let (before, _stats) = engine_a
        .instant_with_stats(th, "m[3h]", eval_t_ms, &[], now1, Duration::from_secs(5))
        .await
        .expect("mixed sealed+recent query");

    let now2 = now_at_seal(recent_hour);
    let cat_b = catalog(store.clone());
    cat_b
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now2, &[], None)
        .await
        .expect("fold seals recent_hour too");
    let engine_b = QueryEngine::new(Arc::new(cat_b), store, config);
    let (after, _stats) = engine_b
        .instant_with_stats(th, "m[3h]", eval_t_ms, &[], now2, Duration::from_secs(5))
        .await
        .expect("same query once recent_hour is sealed too");

    assert_eq!(
        matrix_bits(&before),
        matrix_bits(&after),
        "sealing the recent hour must not change query results"
    );
}

// --- Request budget (ADR-0073 decision 3) ---

/// Wraps a `MemoryStore` and counts GETs issued for any key in `data_keys`
/// (the segment data objects), mirroring `bytes_scanned_budget.rs`'s
/// `GetCountingStore`.
struct GetCountingStore {
    inner: MemoryStore,
    data_keys: HashSet<String>,
    gets: Arc<AtomicUsize>,
}

#[async_trait]
impl ObjectStoreBackend for GetCountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if self.data_keys.contains(key) {
            self.gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, range).await
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
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

const REQUEST_BUDGET_SEGMENTS: usize = 4;
const REQUEST_BUDGET_METRIC: &str = "m";
const REQUEST_BUDGET_TS_NS: i64 = 1_000 * NS_PER_SEC;

/// Publishes `REQUEST_BUDGET_SEGMENTS` token-resolved segments (hour 0, kept
/// outside any listing window so only the token makes each visible -- the
/// exemption ADR-0073 decision 2 grants is exercised here too, isolating
/// the request budget from the sealed-set cap) and wraps the store in a
/// GET-counting double.
async fn setup(
    max_s3_requests: RequestLimit,
) -> (QueryEngine, Arc<AtomicUsize>, Vec<CommitToken>, TenantHash) {
    let tenant_id = tenant("tenant-a");
    let tenant_hash = tenant_id.hash();

    let inner = MemoryStore::new();
    let mut tokens = Vec::with_capacity(REQUEST_BUDGET_SEGMENTS);
    let mut data_keys = HashSet::with_capacity(REQUEST_BUDGET_SEGMENTS);
    for seq in 0..REQUEST_BUDGET_SEGMENTS as u64 {
        let (token, data_key) = publish_segment(
            &inner,
            &tenant_id,
            tenant_hash,
            seq,
            0,
            REQUEST_BUDGET_METRIC,
            REQUEST_BUDGET_TS_NS,
            1.0,
        )
        .await;
        tokens.push(token);
        data_keys.insert(data_key);
    }

    let gets = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(GetCountingStore {
        inner,
        data_keys,
        gets: Arc::clone(&gets),
    });
    let backend: Arc<dyn ObjectStoreBackend> = store;

    let cat = Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let config = EngineConfig {
        max_s3_requests,
        fetch_concurrency: 1,
        ..Default::default()
    };
    let engine = QueryEngine::new(cat, backend, config);
    (engine, gets, tokens, tenant_hash)
}

/// The request budget trips as a typed `RequestBudgetExceeded`, mirroring
/// `bytes_scanned_budget.rs`'s proof for `TooManyBytesScanned`: with
/// `fetch_concurrency: 1` against a synchronous backend, the first pushed
/// segment fetch runs to completion before any other is polled, so a budget
/// this low trips at the very next checkpoint and cancels the rest. Pre-fix,
/// there was no `max_s3_requests` check at all in `fetch_all_series`; the
/// fix's mirrored `segment_admission::request_budget_exceeded` checkpoint is
/// what this test exercises.
#[tokio::test]
async fn request_budget_trips_typed() {
    let (engine, gets, tokens, tenant_hash) = setup(RequestLimit::Bounded(1)).await;

    let t_ms = REQUEST_BUDGET_TS_NS / 1_000_000;
    let result = engine
        .instant(
            tenant_hash,
            REQUEST_BUDGET_METRIC,
            t_ms,
            &tokens,
            REQUEST_BUDGET_TS_NS,
            Duration::from_secs(30),
        )
        .await;

    assert!(
        matches!(result, Err(QueryError::RequestBudgetExceeded { .. })),
        "expected RequestBudgetExceeded, got {result:?}"
    );
    let issued = gets.load(Ordering::SeqCst);
    assert!(
        issued < REQUEST_BUDGET_SEGMENTS,
        "the tripped budget must cancel the remaining segment fetches: issued {issued} GETs, \
         snapshot has {REQUEST_BUDGET_SEGMENTS} segments"
    );
}

/// No-regression companion: an unlimited request budget fetches every
/// segment, exactly the pre-ADR-0073 behavior for a query that never opts
/// into the new cap.
#[tokio::test]
async fn unlimited_request_budget_fetches_every_segment() {
    let (engine, gets, tokens, tenant_hash) = setup(RequestLimit::Unlimited).await;

    let t_ms = REQUEST_BUDGET_TS_NS / 1_000_000;
    let result = engine
        .instant(
            tenant_hash,
            REQUEST_BUDGET_METRIC,
            t_ms,
            &tokens,
            REQUEST_BUDGET_TS_NS,
            Duration::from_secs(30),
        )
        .await;

    assert!(
        result.is_ok(),
        "unlimited request budget must not reject: {result:?}"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        REQUEST_BUDGET_SEGMENTS,
        "an unlimited request budget fetches every segment exactly once"
    );
}
