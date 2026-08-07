//! Acceptance proof for the relocated per-tenant bytes-scanned budget
//! (ADR-0061 decision 1, issue #721; ADR-0061 amendment 2026-08-07).
//!
//! The budget's enforcement point was moved out of the three cross-segment
//! merge functions and into the two fetch fan-outs that own segment
//! concurrency (`fetch_all_series`, `fetch_all_samples_and_histograms`).
//! The former merge-level check ran only *after* `.collect()` had already
//! drained every segment's fetch, so it could detect an overage but never
//! cancel one: every byte was fetched and paid for before the check fired.
//!
//! These tests prove the new location actually cancels mid-scan. Each drives a
//! real query through the full stack (catalog -> fetcher -> engine) over a
//! multi-segment snapshot, against an object store that counts the GETs issued
//! for the segment data objects. When the budget trips, the GET count is
//! strictly below the segment count -- the remaining segments' fetches were
//! never issued because returning early dropped the fetch stream. With the
//! budget `Unlimited`, every segment is fetched, so the GET count equals the
//! segment count (no regression to prior behavior).
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_promql::LabelMatcher;
use ravel_query::{ByteLimit, EngineConfig, QueryEngine, QueryError};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash,
    TenantId, TimeRange,
};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const SAMPLE_TS_NS: i64 = 1_000 * NS;
const METRIC: &str = "m";
/// How many segments each test's snapshot resolves to. Chosen > 1 so "GETs <
/// segments" is a meaningful short-circuit assertion.
const SEGMENTS: usize = 4;

/// Wraps a `MemoryStore` and counts the GETs issued for any key in
/// `data_keys` (the segment data objects), passing every operation --
/// including the counted ones -- straight through. Catalog commit-record reads
/// hit other keys and are never counted, so `gets` is exactly the number of
/// segment fetches the engine issued.
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
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits (issue #298).
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Writes one real RSEG segment carrying a single `METRIC` sample and
/// publishes its commit record onto `store`, returning the read-your-write
/// token and the segment data-object key. `writer_seq` distinguishes the N
/// segments so each is a separate data object and a separate commit record.
/// `ingest_hour_bucket` 0 keeps the segment outside any real-time listing
/// window, so only the token makes it visible: catalog resolution reads the
/// commit record, never the data object, before the fetch runs.
async fn publish_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    writer_seq: u64,
) -> (CommitToken, String) {
    let writer_id = Uuid::from_u128(7);
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
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, METRIC, &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        // Every segment carries the same series at the same timestamp: a
        // distinct `writer_seq` (below) keeps them separate segments and
        // separate data objects, while the shared event time makes all N
        // overlap a single-instant query window so the snapshot resolves to
        // all of them. Cross-segment duplicates merge to one sample, which is
        // irrelevant here -- what matters is that all N are fetched.
        samples: vec![Sample {
            ts_ns: SAMPLE_TS_NS,
            value: 1.0,
        }],
    };
    let written: WrittenSegment =
        SegmentWriter::write(vec![input], identity, bounds).expect("write segment");

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
        segment_format_version: 1,
        created_unix_ns: 42,
        ingest_hour_bucket: 0,
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

/// Publishes `SEGMENTS` segments onto a fresh store, then wraps that store in a
/// GET-counting double. Returns the engine, the wrapper's GET counter, and the
/// per-segment commit tokens. `fetch_concurrency` is 1 so segments are fetched
/// one at a time: a tripped budget then short-circuits deterministically after
/// exactly one data-object GET, and the assertion "GETs < segments" is exact
/// rather than racing the buffered fan-out.
async fn setup(
    max_bytes_scanned: ByteLimit,
) -> (QueryEngine, Arc<AtomicUsize>, Vec<CommitToken>, TenantHash) {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();

    let inner = MemoryStore::new();
    let mut tokens = Vec::with_capacity(SEGMENTS);
    let mut data_keys = HashSet::with_capacity(SEGMENTS);
    for seq in 0..SEGMENTS as u64 {
        let (token, data_key) = publish_segment(&inner, &tenant_id, tenant_hash, seq).await;
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

    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let config = EngineConfig {
        max_bytes_scanned,
        fetch_concurrency: 1,
        ..Default::default()
    };
    let engine = QueryEngine::new(catalog, backend, config);
    (engine, gets, tokens, tenant_hash)
}

/// The bytes-scanned budget trips mid-scan on a real instant query: the query
/// returns `TooManyBytesScanned`, and the counting store recorded strictly
/// fewer GETs than the snapshot has segments -- proving the remaining
/// segments' fetches were cancelled, not merely that the query failed after
/// fetching everything.
#[tokio::test]
async fn bytes_budget_trips_and_cancels_remaining_segment_fetches() {
    // A one-byte budget trips the moment the first segment's bytes are
    // accounted, so exactly one segment is fetched before the fan-out is
    // dropped.
    let (engine, gets, tokens, tenant_hash) = setup(ByteLimit::Bounded(1)).await;

    let t_ms = SAMPLE_TS_NS / 1_000_000;
    let result = engine
        .instant(
            tenant_hash,
            METRIC,
            t_ms,
            &tokens,
            SAMPLE_TS_NS,
            Duration::from_secs(30),
        )
        .await;

    assert!(
        matches!(result, Err(QueryError::TooManyBytesScanned { .. })),
        "expected TooManyBytesScanned, got {result:?}"
    );
    let issued = gets.load(Ordering::SeqCst);
    assert!(
        issued < SEGMENTS,
        "the tripped budget must cancel the remaining segment fetches: issued {issued} GETs, \
         snapshot has {SEGMENTS} segments"
    );
    assert_eq!(
        issued, 1,
        "with fetch_concurrency=1 the budget trips after the first segment, so exactly one \
         data-object GET is issued (got {issued})"
    );
}

/// No-regression companion: with the budget `Unlimited` the identical query
/// fetches every segment, so the GET count equals the segment count and the
/// query succeeds. This is the pre-ADR-0061 behavior, unchanged.
#[tokio::test]
async fn unlimited_budget_fetches_every_segment() {
    let (engine, gets, tokens, tenant_hash) = setup(ByteLimit::Unlimited).await;

    let t_ms = SAMPLE_TS_NS / 1_000_000;
    let result = engine
        .instant(
            tenant_hash,
            METRIC,
            t_ms,
            &tokens,
            SAMPLE_TS_NS,
            Duration::from_secs(30),
        )
        .await;

    assert!(
        result.is_ok(),
        "unlimited budget must not reject: {result:?}"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        SEGMENTS,
        "an unlimited budget fetches every segment exactly once"
    );
}

/// The empty-input gap (review finding 4): a labels/series query whose matchers
/// resolve to a non-empty snapshot but match zero series still evaluates the
/// budget, because the check now lives in `fetch_all_series` rather than in the
/// `build_series_by_id` loop body -- which never runs when nothing matches. The
/// bounded run trips `TooManyBytesScanned`; the unlimited run proves the query
/// genuinely matches zero series (an empty result), so the trip is enforcement
/// on a truly empty result, not on matched data.
#[tokio::test]
async fn empty_matching_labels_query_still_trips_budget() {
    let window = TimeRange {
        start_ns: 0,
        end_ns: SAMPLE_TS_NS * 2,
    };
    // `__name__ = m` keeps every segment in the resolved snapshot (postings
    // pruning matches on the metric name); the second matcher is a label no
    // series carries, so every fetched segment yields zero matching series.
    let matchers = vec![
        LabelMatcher::equal(METRIC_NAME_LABEL, METRIC),
        LabelMatcher::equal("missing", "nope"),
    ];

    // Unlimited: proves the query really does match zero series.
    let (engine, gets, tokens, tenant_hash) = setup(ByteLimit::Unlimited).await;
    let series = engine
        .resolve_series(
            tenant_hash,
            &matchers,
            window,
            &tokens,
            SAMPLE_TS_NS * 2,
            Duration::from_secs(30),
        )
        .await
        .expect("resolve_series must succeed");
    assert!(
        series.is_empty(),
        "the matchers must resolve to zero series, got {}",
        series.len()
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        SEGMENTS,
        "an unlimited labels query still fetches every segment to check matchers"
    );

    // Bounded: the budget trips even though zero series match, because it is
    // checked in `fetch_all_series` after each segment fetch.
    let (engine, gets, tokens, tenant_hash) = setup(ByteLimit::Bounded(1)).await;
    let result = engine
        .resolve_series(
            tenant_hash,
            &matchers,
            window,
            &tokens,
            SAMPLE_TS_NS * 2,
            Duration::from_secs(30),
        )
        .await;
    assert!(
        matches!(result, Err(QueryError::TooManyBytesScanned { .. })),
        "the empty-matching labels query must still trip the byte budget, got {result:?}"
    );
    assert!(
        gets.load(Ordering::SeqCst) < SEGMENTS,
        "the tripped budget must cancel the remaining segment fetches on the labels path too"
    );
}
