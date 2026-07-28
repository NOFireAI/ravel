//! Ingest-then-read-back over `MemoryStore` for directly-constructed
//! histogram `IngestPoint`s (docs/rseg-v3-plan.md phase C7, section 7):
//! proves `IngestRouter::write_values` -> shard buffer -> `SegmentWriter::
//! write_v3` plumbing end to end, since native histograms are rejected at
//! wire admission until phase C8 and so never reach this path through OTLP
//! or remote-write decode. Read-back uses `ravel-segment`'s reader API
//! directly (mirrors crates/ravel-segment/tests/reader_v3.rs), because
//! `ravel-query`'s fetcher does not yet decode RSEG v3/HIST_PAGES.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, build_labels, tenant};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{IngestConfig, IngestPoint, IngestRouter, IngestValue, WriteMode};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ReaderLimits, ResetHint,
    SeriesEntry, ValueKind,
};
use ravel_types::{METRIC_NAME_LABEL, Sample, SeriesId, Signal};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

fn flush_on_first_point(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

fn histogram_point(tenant_id: &ravel_types::TenantId, metric: &str, ts_ns: i64) -> IngestPoint {
    let labels = build_labels(&[(METRIC_NAME_LABEL, metric)]);
    let series_id = SeriesId::compute(tenant_id, metric, &labels).expect("series id");
    IngestPoint {
        series_id,
        labels,
        value: IngestValue::Histogram(HistogramSample {
            ts_ns,
            value: HistogramValue {
                scale: 2,
                zero_threshold: 1e-9,
                sum: Some(42.5),
                custom_values: None,
                positive_spans: vec![HistogramSpan {
                    offset: 0,
                    length: 3,
                }],
                negative_spans: vec![],
                counts: HistogramCounts::Int {
                    zero_count: 1,
                    count: 7,
                    positive: vec![2, 3, 1],
                    negative: vec![],
                },
                reset_hint: ResetHint::Yes,
            },
        }),
    }
}

fn scalar_point(
    tenant_id: &ravel_types::TenantId,
    metric: &str,
    ts_ns: i64,
    value: f64,
) -> IngestPoint {
    let labels = build_labels(&[(METRIC_NAME_LABEL, metric)]);
    let series_id = SeriesId::compute(tenant_id, metric, &labels).expect("series id");
    IngestPoint {
        series_id,
        labels,
        value: IngestValue::Scalar(Sample { ts_ns, value }),
    }
}

/// A flush buffer with any histogram-kind series must produce RSEG v3, even
/// with a scalar series mixed into the same batch (docs/rseg-v3-plan.md
/// section 3.2). Reads the object back through `ravel-segment`'s own reader
/// API (not `ravel-query`, which doesn't decode v3 yet) and checks both
/// series' content round-trips exactly.
#[tokio::test]
async fn histogram_and_scalar_points_ingest_and_read_back_as_v3() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first_point(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let hist = histogram_point(&tenant, "req_latency", 1_000);
    let scalar = scalar_point(&tenant, "cpu_usage", 1_000, 3.5);

    let receipt = router
        .write_values(
            tenant.clone(),
            vec![hist.clone(), scalar.clone()],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("histogram batch is accepted");
    assert_eq!(receipt.tokens.len(), 1);

    let commit_key =
        keys::commit_key_for_token(&tenant.hash(), Signal::Metrics, &receipt.tokens[0])
            .expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded_record = record::decode(&commit_bytes).expect("decode commit record");
    assert_eq!(
        decoded_record.segment_format_version, 3,
        "a flush containing a histogram series must stamp v3"
    );
    assert_eq!(decoded_record.series_count, 2);
    assert_eq!(decoded_record.sample_count, 2);

    let data_bytes = store
        .get(&decoded_record.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data;
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&data_bytes, limits).expect("opens segment");
    assert_eq!(loc.version, 3);

    let section = |kind: u32| -> &[u8] {
        let s = loc
            .footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .expect("section present");
        let start = s.offset as usize;
        &data_bytes[start..start + s.len as usize]
    };
    const LABEL_DICT: u32 = 1;
    const SERIES_IDS: u32 = 5;
    const SERIES_META: u32 = 6;

    let dict = section(LABEL_DICT);
    let ids = section(SERIES_IDS);
    let meta = section(SERIES_META);
    let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
        .expect("decodes catalog");
    assert_eq!(entries.len(), 2);

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v3(&loc.footer, &selected).expect("plans ranges");

    let mut saw_scalar = false;
    let mut saw_histogram = false;
    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_start = range.ts_range.0 as usize;
        let ts_bytes = &data_bytes[ts_start..ts_start + range.ts_range.1 as usize];
        match entry.value_kind {
            ValueKind::Scalar => {
                let val_start = range.val_range.0 as usize;
                let val_bytes = &data_bytes[val_start..val_start + range.val_range.1 as usize];
                let samples = ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits)
                    .expect("decodes");
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].ts_ns, 1_000);
                assert_eq!(samples[0].value.to_bits(), 3.5_f64.to_bits());
                saw_scalar = true;
            }
            ValueKind::Histogram => {
                let hist_start = range.hist_range.0 as usize;
                let hist_bytes = &data_bytes[hist_start..hist_start + range.hist_range.1 as usize];
                let samples =
                    ravel_segment::decode_histogram_pages(entry, ts_bytes, hist_bytes, limits)
                        .expect("decodes");
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].ts_ns, 1_000);
                let want = match hist.value {
                    IngestValue::Histogram(ref h) => &h.value,
                    IngestValue::Scalar(_) => unreachable!(),
                };
                assert_eq!(samples[0].value.scale, want.scale);
                assert_eq!(
                    samples[0].value.zero_threshold.to_bits(),
                    want.zero_threshold.to_bits()
                );
                assert_eq!(
                    samples[0].value.sum.map(f64::to_bits),
                    want.sum.map(f64::to_bits)
                );
                saw_histogram = true;
            }
        }
    }
    assert!(saw_scalar, "expected the scalar series to round-trip");
    assert!(saw_histogram, "expected the histogram series to round-trip");

    router.shutdown().await;
}

/// Two histogram points for the same series merge into one series with both
/// samples, exactly like the scalar `equal_label_sets_still_merge` case in
/// series_id_collision.rs -- the generalized accumulation path must not
/// special-case histograms out of normal same-series merging.
#[tokio::test]
async fn multiple_histogram_points_for_one_series_merge() {
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(ravel_object_store::memory::MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first_point(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let first = histogram_point(&tenant, "req_latency", 1_000);
    let second = histogram_point(&tenant, "req_latency", 2_000);
    assert_eq!(first.series_id, second.series_id);

    let receipt = router
        .write_values(
            tenant.clone(),
            vec![first, second],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("merging histogram batch is accepted");
    assert_eq!(receipt.tokens.len(), 1);

    let commit_key =
        keys::commit_key_for_token(&tenant.hash(), Signal::Metrics, &receipt.tokens[0])
            .expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded_record = record::decode(&commit_bytes).expect("decode commit record");
    assert_eq!(decoded_record.series_count, 1, "one series");
    assert_eq!(decoded_record.sample_count, 2, "both samples merged");

    router.shutdown().await;
}
