//! Regression coverage for issue #811: the RSEG footer read used to be a
//! `GetRange::Suffix`, which `guarded_get`'s `cacheable_range` match never
//! routes through the cache (a suffix has no absolute start, so it cannot be
//! `CacheKey`-addressed by content hash plus offset/len). `open_segment` now
//! converts that read into an absolute `Range(object_size - suffix,
//! object_size)` whenever `seg_ref.object_size` is known, which is every real
//! `SegmentRef` (the field is a plain `u64`, populated from the commit or
//! compaction record's own `object_size` before a segment is ever committed).
//!
//! GET counting uses two independent oracles, per the "a pooled counter once
//! hid an extra round trip" caution (`ravel-query/src/engine.rs`
//! `STORE_BYTES_SAFETY_FACTOR`'s doc comment): `FaultStore`'s `Sequence`
//! progress counter, which advances once per `Op::Get` the store itself
//! receives and knows nothing about `ravel-query`'s own accounting; and a
//! fresh `QueryAccounting` handle per call, whose `s3_requests`/`cache_hits`
//! fields are `guarded_get`'s own per-call bookkeeping. Both must agree.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::SegmentRef;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{CacheFetchError, FetchedSeries, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{Label, LabelSet, TenantHash, TenantId};
use uuid::Uuid;

fn labels(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series(metric: &str, samples: &[(i64, f64)]) -> SeriesInput {
    let label_set = labels(metric);
    let tenant_id = TenantId::new("t".to_string());
    let series_id =
        ravel_types::SeriesId::compute(&tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| ravel_types::Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }
}

const SEG_KEY: &str = "test/segment.rseg";

/// Six series of eight samples each: small enough that the default 64 KiB
/// suffix window covers the whole object (footer, catalog, and pages) in one
/// read, so a cold fetch costs exactly one GET regardless of which `GetRange`
/// variant carries it.
fn build_multi_series_segment() -> (Bytes, TenantHash, SegmentRef) {
    let tenant_hash = TenantHash([7u8; 16]);
    let writer_id = Uuid::from_u128(1);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    const NS: i64 = 1_000_000_000;
    let inputs: Vec<SeriesInput> = (0..6)
        .map(|i| {
            let metric = format!("metric_{i}");
            let samples: Vec<(i64, f64)> = (0..8)
                .map(|j| ((1_000 + j) * NS, (i as f64) + (j as f64) * 0.5))
                .collect();
            series(&metric, &samples)
        })
        .collect();
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");

    let seg_ref = SegmentRef {
        data_object_key: SEG_KEY.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 0,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard: 0,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 42,
        level: ravel_catalog::SegmentLevel::L0,
    };
    (written.bytes, tenant_hash, seg_ref)
}

/// A `FaultStore` whose only rule is a long run of pass-throughs on the
/// segment key: it changes no behavior but makes `sequence_progress(0)` a
/// faithful, `ravel-query`-independent count of the GETs a fetch issued.
async fn faulted_store_with_segment(bytes: Bytes) -> Arc<FaultStore<MemoryStore>> {
    let inner = MemoryStore::new();
    inner
        .put(SEG_KEY, bytes, PutOptions::default())
        .await
        .expect("put segment object");
    let mut seq = Sequence::new(Op::Get).with_key_contains(SEG_KEY);
    for _ in 0..64 {
        seq = seq.then_passthrough();
    }
    Arc::new(FaultStore::new(
        inner,
        FaultPlan::empty().with_sequence(seq),
    ))
}

fn sort_series(mut v: Vec<FetchedSeries>) -> Vec<FetchedSeries> {
    v.sort_by_key(|s| s.series_id.0);
    v
}

fn assert_same_series(a: &[FetchedSeries], b: &[FetchedSeries]) {
    assert_eq!(a.len(), b.len(), "series count must match");
    for (x, y) in a.iter().zip(b.iter()) {
        assert_eq!(x.series_id, y.series_id);
        assert_eq!(x.labels, y.labels);
        assert_eq!(x.samples.len(), y.samples.len());
        for (sx, sy) in x.samples.iter().zip(y.samples.iter()) {
            assert_eq!(sx.ts_ns, sy.ts_ns);
            assert_eq!(sx.value.to_bits(), sy.value.to_bits());
        }
    }
}

/// The primary issue #811 regression: a second fetch of the same segment,
/// through the same cache, issues exactly zero additional store GETs for the
/// footer. `with_whole_object_threshold(0)` forces the range/suffix branch
/// unconditionally (never `GetRange::Full`), so this exercises the same
/// branch a large above-threshold segment takes in production; the fixture
/// itself is small only so the test runs fast, and the default 64 KiB
/// suffix window still covers it in a single read.
#[tokio::test]
async fn second_fetch_reuses_cached_footer_with_zero_additional_gets() {
    let (bytes, tenant_hash, seg_ref) = build_multi_series_segment();
    let store = faulted_store_with_segment(bytes).await;
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(limits));
    let fetcher = SegmentFetcher::new(backend)
        .with_whole_object_threshold(0)
        .with_cache(cache);

    // Cold fetch: exactly one store GET (footer + catalog, both inside the
    // default suffix window).
    let cold_accounting = QueryAccounting::new();
    let cold = fetcher
        .fetch_series_accounted(tenant_hash, &seg_ref, &[], &cold_accounting)
        .await
        .expect("cold fetch_series");
    assert_eq!(cold.len(), 6, "cold fetch must decode all six series");
    assert_eq!(
        store.sequence_progress(0),
        1,
        "cold fetch must issue exactly one store GET"
    );
    let cold_snapshot = cold_accounting.snapshot();
    assert_eq!(
        cold_snapshot.s3_requests(AccountedOp::Get),
        1,
        "cold accounting must show exactly one store GET"
    );
    assert_eq!(
        cold_snapshot.cache_misses, 1,
        "cold accounting must show exactly one cache miss"
    );
    assert_eq!(
        cold_snapshot.cache_hits, 0,
        "cold accounting must show zero cache hits"
    );

    // Warm fetch: same fetcher, same cache, same store. Before this fix the
    // footer read stayed a `GetRange::Suffix`, which `guarded_get` never
    // cache-routes, so this second fetch would have added a second store GET
    // here (`store.sequence_progress(0)` == 2, `s3_requests(Get)` == 1). It
    // is now an absolute `Range`, which is cache-eligible.
    let warm_accounting = QueryAccounting::new();
    let warm = fetcher
        .fetch_series_accounted(tenant_hash, &seg_ref, &[], &warm_accounting)
        .await
        .expect("warm fetch_series");
    assert_eq!(warm.len(), 6, "warm fetch must decode all six series");
    assert_eq!(
        store.sequence_progress(0),
        1,
        "warm fetch must issue zero additional store GETs (still 1 total, not 2)"
    );
    let warm_snapshot = warm_accounting.snapshot();
    assert_eq!(
        warm_snapshot.s3_requests(AccountedOp::Get),
        0,
        "warm accounting must show zero store GETs for the footer"
    );
    assert_eq!(
        warm_snapshot.s3_bytes(AccountedOp::Get),
        0,
        "warm accounting must show zero store bytes for the footer"
    );
    assert_eq!(
        warm_snapshot.cache_hits, 1,
        "warm accounting must show exactly one cache hit"
    );
    assert_eq!(
        warm_snapshot.cache_misses, 0,
        "warm accounting must show zero cache misses"
    );
    assert_eq!(
        warm_snapshot.cache_bytes,
        seg_ref.object_size,
        "warm accounting's cache-served bytes must equal the cached footer read's own length"
    );
}

/// The `object_size == 0` defensive fallback (`open_segment`, `fetcher.rs`):
/// no real `SegmentRef` carries this value, but the suffix path it selects
/// must still work, including its `NeedRange` chase. This pins `object_size`
/// absent/unknown as still-correct, not merely still-compiling.
#[tokio::test]
async fn object_size_absent_falls_back_to_suffix_and_needrange_chase_still_decodes() {
    let (bytes, tenant_hash, seg_ref) = build_multi_series_segment();

    // Reference: real object_size, tiny suffix forces the footer NeedRange
    // chase down the normal (now Range-based) path.
    let ref_store = faulted_store_with_segment(bytes.clone()).await;
    let ref_backend: Arc<dyn ObjectStoreBackend> = ref_store.clone();
    let reference = sort_series(
        SegmentFetcher::new(ref_backend)
            .with_whole_object_threshold(0)
            .with_suffix_len(16)
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("reference fetch"),
    );
    assert!(
        ref_store.sequence_progress(0) >= 2,
        "reference fetch must chase NeedRange for the footer, got {}",
        ref_store.sequence_progress(0)
    );

    // object_size unknown/absent (0): `open_segment` must keep taking the
    // `GetRange::Suffix` fallback (a computed absolute range would be wrong
    // without a real size) and its NeedRange chase must still recover the
    // exact same series.
    let mut unknown_size_ref = seg_ref.clone();
    unknown_size_ref.object_size = 0;
    let store = faulted_store_with_segment(bytes).await;
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let fetched = sort_series(
        SegmentFetcher::new(backend)
            .with_whole_object_threshold(0)
            .with_suffix_len(16)
            .fetch(tenant_hash, &unknown_size_ref, &[])
            .await
            .expect("object_size-absent fetch"),
    );
    assert!(
        store.sequence_progress(0) >= 2,
        "the suffix fallback must still chase NeedRange for the footer, got {}",
        store.sequence_progress(0)
    );
    assert_same_series(&reference, &fetched);
}
