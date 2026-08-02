//! Ingest-then-read-back over `MemoryStore` for directly-constructed
//! histogram `IngestPoint`s: proves `IngestRouter::write_values` -> shard
//! buffer -> `SegmentWriter::write_histograms` (the v5 raw-sample adapter)
//! plumbing end to end, since native histograms are rejected at wire
//! admission and so never reach this path through OTLP or remote-write
//! decode. Read-back uses `ravel-segment`'s v5 reader API directly (the
//! single-run v5 grammar), because `ravel-query`'s fetcher decodes only
//! scalar series.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, build_labels, tenant};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{
    IngestConfig, IngestPoint, IngestRouter, IngestValue, SEGMENT_FORMAT_VERSION, WriteMode,
};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ReaderLimits, ResetHint,
    SeriesEntryV4, ValueKind,
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

/// A flush buffer with a histogram series and a scalar series mixed into the
/// same batch produces one RSEG v5 object with a histogram run and a scalar
/// run. Reads it back through `ravel-segment`'s own v5 reader API (not
/// `ravel-query`, which decodes only scalar series) and checks both series'
/// content round-trips exactly.
#[tokio::test]
async fn histogram_and_scalar_points_ingest_and_read_back_as_v5() {
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
        decoded_record.segment_format_version,
        u32::from(SEGMENT_FORMAT_VERSION),
        "every flush stamps v5"
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
    assert_eq!(loc.version, SEGMENT_FORMAT_VERSION);

    let entries = ravel_segment::decode_catalog_v5(&loc.footer, &data_bytes, limits)
        .expect("decodes catalog");
    assert_eq!(entries.len(), 2);

    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v4(&loc.footer, &selected).expect("plans ranges");

    let mut saw_scalar = false;
    let mut saw_histogram = false;
    for entry in &entries {
        let run = &entry.runs[0];
        let range = ranges
            .iter()
            .find(|r| r.series_id == entry.entry.series_id && r.run_index == 0)
            .expect("planned range for run 0");
        let ts_start = range.ts_range.0 as usize;
        let ts_bytes = &data_bytes[ts_start..ts_start + range.ts_range.1 as usize];
        match entry.entry.value_kind {
            ValueKind::Scalar => {
                let val_start = range.val_range.0 as usize;
                let val_bytes = &data_bytes[val_start..val_start + range.val_range.1 as usize];
                let mut scratch = Vec::new();
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                ravel_segment::decode_run_pages_soa(
                    &entry.entry.series_id,
                    run,
                    ts_bytes,
                    val_bytes,
                    limits,
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .expect("decodes");
                assert_eq!(timestamps.len(), 1);
                assert_eq!(timestamps[0], 1_000);
                assert_eq!(values[0].to_bits(), 3.5_f64.to_bits());
                saw_scalar = true;
            }
            ValueKind::Histogram => {
                let hist_start = range.hist_range.0 as usize;
                let hist_bytes = &data_bytes[hist_start..hist_start + range.hist_range.1 as usize];
                let samples = ravel_segment::decode_run_histogram_pages(
                    &entry.entry.series_id,
                    run,
                    ts_bytes,
                    hist_bytes,
                    limits,
                )
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
