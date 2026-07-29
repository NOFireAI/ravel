//! Regression coverage for the `SegmentFetcher` multi-GET path (audit A5,
//! finding a5-F01; docs/reviews/2026-07-27-storage-engine-quality-audit/
//! a5-fetch-object-store.md). Every segment in the pre-existing test corpus
//! fits inside the default 64 KiB suffix read, so the footer `NeedRange`
//! chase (`fetcher.rs` `open_segment`), `ensure_ranges` issuing additional
//! GETs, byte-range coalescing, and both `EtagChanged` snapshot guards were
//! unreachable. These tests shrink the suffix window below the object so the
//! multi-GET plan runs for real.
//!
//! GET counting uses the `FaultStore` sequencing API (issue #92): a `Sequence`
//! of pass-throughs on the segment key advances one step per matching `get`,
//! so `sequence_progress` reports exactly how many GETs a fetch issued (the
//! step budget is set well above any fetch's GET count, so the cursor never
//! saturates). The `EtagChanged` tests script `[Passthrough, .., EtagChange]`:
//! the first read pins an etag, a later read returns fresh bytes under a new
//! etag, and the fetcher must abort rather than mix bytes across versions.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::SegmentRef;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{FetchError, FetchedSeries, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
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

/// Builds a real RSEG segment with six series of eight samples each, so the
/// LABEL_DICT / SERIES_TABLE / TS_PAGES / VAL_PAGES sections all sit well
/// before the footer and the planned page ranges span more than one region.
/// Returns the raw bytes plus a matching `SegmentRef`; callers put the bytes
/// on whichever store they need (plain, or `FaultStore`-wrapped).
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

async fn memory_store_with_segment(bytes: Bytes) -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::new());
    store
        .put(SEG_KEY, bytes, PutOptions::default())
        .await
        .expect("put segment object");
    store
}

/// A `FaultStore` whose only rule is a long run of pass-throughs on the
/// segment key: it changes no behavior but makes `sequence_progress(0)` a
/// faithful count of the GETs a fetch issued.
async fn get_counting_store(bytes: Bytes) -> Arc<FaultStore<MemoryStore>> {
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

/// a5-F01, deliverable 1: a suffix smaller than the trailer+footer forces
/// `open_segment` down the `NeedRange` branch and `ensure_ranges` into extra
/// GETs. The default-suffix fetch reads the whole object in one GET; the tiny
/// suffix issues several. Both decode to identical series, proving the footer
/// chase and the additional page GETs are correct, not just exercised.
#[tokio::test]
async fn needrange_chase_issues_a_second_get_and_matches_full_suffix() {
    let (bytes, tenant_hash, seg_ref) = build_multi_series_segment();

    // Reference: default 64 KiB suffix returns the whole object in one GET.
    let ref_store = get_counting_store(bytes.clone()).await;
    let ref_backend: Arc<dyn ObjectStoreBackend> = ref_store.clone();
    let reference = sort_series(
        SegmentFetcher::new(ref_backend)
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("reference fetch (default suffix)"),
    );
    assert_eq!(
        ref_store.sequence_progress(0),
        1,
        "default suffix must satisfy the whole read in a single GET"
    );

    // Tiny suffix (16 bytes = trailer only) forces the footer NeedRange chase
    // plus separate catalog/page GETs.
    let multi_store = get_counting_store(bytes).await;
    let multi_backend: Arc<dyn ObjectStoreBackend> = multi_store.clone();
    let multi_get = sort_series(
        SegmentFetcher::new(multi_backend)
            .with_suffix_len(16)
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("multi-GET fetch (tiny suffix)"),
    );
    let gets = multi_store.sequence_progress(0);
    assert!(
        gets >= 2,
        "tiny suffix must issue at least a second GET for the footer chase, got {gets}"
    );

    assert_same_series(&reference, &multi_get);
}

/// a5-F01, deliverable 2: the coalesce gap changes only how planned ranges are
/// grouped into GETs, never the decoded result. A zero gap fetches each
/// non-touching range separately (the maximal GET set); a huge gap merges
/// adjacent ranges (the minimal GET set). Data is identical, and widening the
/// gap never increases the GET count.
#[tokio::test]
async fn coalesce_gap_changes_get_count_but_not_data() {
    let (bytes, tenant_hash, seg_ref) = build_multi_series_segment();

    let no_coalesce_store = get_counting_store(bytes.clone()).await;
    let no_coalesce_backend: Arc<dyn ObjectStoreBackend> = no_coalesce_store.clone();
    let no_coalesce = sort_series(
        SegmentFetcher::new(no_coalesce_backend)
            .with_suffix_len(16)
            .with_coalesce_gap(0)
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch with no coalescing"),
    );
    let gets_no_coalesce = no_coalesce_store.sequence_progress(0);

    let wide_store = get_counting_store(bytes).await;
    let wide_backend: Arc<dyn ObjectStoreBackend> = wide_store.clone();
    let wide_coalesce = sort_series(
        SegmentFetcher::new(wide_backend)
            .with_suffix_len(16)
            .with_coalesce_gap(1 << 20)
            .fetch(tenant_hash, &seg_ref, &[])
            .await
            .expect("fetch with wide coalescing"),
    );
    let gets_wide = wide_store.sequence_progress(0);

    assert_same_series(&no_coalesce, &wide_coalesce);
    assert!(
        gets_wide >= 2,
        "the multi-GET path must be active, got {gets_wide}"
    );
    assert!(
        gets_no_coalesce >= gets_wide,
        "coalescing must never increase the GET count: no_coalesce={gets_no_coalesce}, wide={gets_wide}"
    );
}

/// a5-F01, deliverable 3: the footer `NeedRange` etag guard
/// (`open_segment`). The suffix read pins etag A; the scripted footer read
/// returns fresh bytes under etag B, so the fetch must surface `EtagChanged`
/// rather than decode a footer from a different object version. A control run
/// with no fault plan proves the same tiny-suffix path otherwise succeeds, so
/// the failure is attributable to the etag change.
#[tokio::test]
async fn etag_change_on_footer_chase_surfaces_etag_changed() {
    let (bytes, tenant_hash, seg_ref) = build_multi_series_segment();

    // Control: tiny suffix, no fault -> the NeedRange chase succeeds.
    let control = memory_store_with_segment(bytes.clone()).await;
    let control_backend: Arc<dyn ObjectStoreBackend> = control;
    SegmentFetcher::new(control_backend)
        .with_suffix_len(16)
        .fetch(tenant_hash, &seg_ref, &[])
        .await
        .expect("control fetch (no fault) must succeed");

    // First get (suffix) passes through; second get (the footer NeedRange
    // read) returns real bytes under a changed etag.
    let inner = MemoryStore::new();
    inner
        .put(SEG_KEY, bytes, PutOptions::default())
        .await
        .expect("put segment object");
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Get)
            .with_key_contains(SEG_KEY)
            .then_passthrough()
            .then_fault(ScriptedFault::EtagChange),
    );
    let store = Arc::new(FaultStore::new(inner, plan));
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();

    let result = SegmentFetcher::new(backend)
        .with_suffix_len(16)
        .fetch(tenant_hash, &seg_ref, &[])
        .await;
    assert!(
        matches!(result, Err(FetchError::EtagChanged { .. })),
        "expected EtagChanged, got {result:?}"
    );
    assert_eq!(
        store.sequence_progress(0),
        2,
        "both the suffix and the etag-changed footer read must have fired"
    );
    assert_eq!(store.fault_count(Op::Get, FaultKind::EtagChange), 1);
}

/// a5-F01, deliverable 3: the `ensure_ranges` etag guard. The suffix and the
/// footer NeedRange read pass through consistently; the first catalog range
/// GET returns fresh bytes under a changed etag, so the fetch surfaces
/// `EtagChanged` before decoding any catalog bytes from the wrong version.
#[tokio::test]
async fn etag_change_on_range_get_surfaces_etag_changed() {
    let (bytes, tenant_hash, seg_ref) = build_multi_series_segment();

    let inner = MemoryStore::new();
    inner
        .put(SEG_KEY, bytes, PutOptions::default())
        .await
        .expect("put segment object");
    // Suffix (pass), footer NeedRange (pass, same etag), then the first
    // ensure_ranges GET (etag changed).
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Get)
            .with_key_contains(SEG_KEY)
            .then_passthrough()
            .then_passthrough()
            .then_fault(ScriptedFault::EtagChange),
    );
    let store = Arc::new(FaultStore::new(inner, plan));
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();

    let result = SegmentFetcher::new(backend)
        .with_suffix_len(16)
        .fetch(tenant_hash, &seg_ref, &[])
        .await;
    assert!(
        matches!(result, Err(FetchError::EtagChanged { .. })),
        "expected EtagChanged, got {result:?}"
    );
    assert_eq!(
        store.sequence_progress(0),
        3,
        "suffix, footer, and the etag-changed range GET must all have fired"
    );
    assert_eq!(store.fault_count(Op::Get, FaultKind::EtagChange), 1);
}
