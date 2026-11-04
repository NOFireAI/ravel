//! The RSEG compaction merge's peak resident bytes, split by component
//! (issue #3817).
//!
//! The fixture has the shape the issue measured: a bucket of `S` series in
//! which each of `N` L0 inputs carries a slice of `M` of them, one sample per
//! series per input, so every series appears in `N * M / S` inputs. The merge
//! runs with a [`MergeMemoryTracker`] installed and the test reads back each
//! component's high-water: the planning catalogs (series and run plans, and
//! their label sets), the group-by index built from them, the raw fetched
//! window, one series' decoded merge state, and the writer term. Two input
//! counts with the same `M` and `S` show which components grow with `N * M`.
//! Run with `--nocapture` to see the table.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::read::label_heap_bytes;
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, MergeMemoryTracker, MergePhasePeaks,
    compact_bucket,
};
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// Series each input carries.
const SERIES_PER_INPUT: usize = 256;
/// Distinct series in the bucket; every series appears in `N / 8` inputs.
const BUCKET_SERIES: usize = 8 * SERIES_PER_INPUT;

struct Measured {
    inputs: usize,
    peaks: MergePhasePeaks,
    resident: u64,
    writer: u64,
    parts: usize,
    total_samples: u64,
}

/// The label pairs of bucket series `s`: eight labels of the lengths a
/// node-exporter style series carries, so the label term has a realistic
/// weight against the fixed-size plan structs.
fn series_labels(s: usize) -> Vec<(String, String)> {
    vec![
        ("cluster".into(), "prod-eu-west-1-primary".into()),
        ("container".into(), format!("app-container-{:03}", s % 64)),
        (
            "instance".into(),
            format!("10.20.{}.{}:9100", s / 256, s % 256),
        ),
        ("job".into(), "kubernetes-service-endpoints".into()),
        ("namespace".into(), format!("team-namespace-{:02}", s % 16)),
        ("pod".into(), format!("app-deployment-7f9c8d6b5-{s:05}")),
        ("region".into(), "europe-west".into()),
        ("service".into(), format!("checkout-service-{:02}", s % 32)),
    ]
}

async fn measure(inputs: usize) -> Measured {
    let templates: Vec<Vec<(String, String)>> = (0..BUCKET_SERIES).map(series_labels).collect();
    let store = MemoryStore::new();
    for i in 0..inputs {
        let series: Vec<RawSeries> = (0..SERIES_PER_INPUT)
            .map(|j| {
                let s = (i * SERIES_PER_INPUT + j) % BUCKET_SERIES;
                let pairs: Vec<(&str, &str)> = templates[s]
                    .iter()
                    .map(|(n, v)| (n.as_str(), v.as_str()))
                    .collect();
                raw_series(
                    "http_requests_total",
                    &pairs,
                    &[(1_000 + i as i64, i as f64)],
                )
            })
            .collect();
        let spec = InputSpec::new(Uuid::from_u128(i as u128 + 1), 1, i as u64 + 1, series);
        seed_input(&store, &spec).await;
    }

    let tracker = MergeMemoryTracker::new();
    let config = CompactorConfig {
        merge_memory_tracker: Some(tracker.clone()),
        ..CompactorConfig::default()
    };
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("compact rseg bucket");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    let record = fetch_compaction_record(&store, &bucket).await;
    assert_eq!(record.inputs.len(), inputs);
    Measured {
        inputs,
        peaks: tracker.phase_peaks(),
        resident: tracker.peak_resident_bytes(),
        writer: tracker.peak_writer_bytes(),
        parts: record.parts.len(),
        total_samples: record.parts.iter().map(|p| p.sample_count).sum(),
    }
}

fn report(m: &Measured) {
    let p = &m.peaks;
    println!(
        "[rseg-merge-memory] N={} M={SERIES_PER_INPUT} S={BUCKET_SERIES} parts={} samples={} | \
         catalog_plan_heap={} catalog_label_heap={} plan_index_heap={} \
         fetched_raw={} decoded_heap={} writer={} resident={}",
        m.inputs,
        m.parts,
        m.total_samples,
        p.catalog_plan_heap_bytes,
        p.catalog_label_heap_bytes,
        p.plan_index_heap_bytes,
        p.fetched_raw_bytes,
        p.decoded_heap_bytes,
        m.writer,
        m.resident,
    );
}

/// The exact label heap one copy of every bucket series' label set holds,
/// as [`label_heap_bytes`] charges it. Depends on `S` and the label lengths
/// only, never on `N`.
fn bucket_label_heap_bytes() -> u64 {
    (0..BUCKET_SERIES)
        .map(|s| {
            let labels = series_labels(s);
            let pairs: Vec<(&str, &str)> = labels
                .iter()
                .map(|(n, v)| (n.as_str(), v.as_str()))
                .collect();
            let (_, ls, _) = raw_series("http_requests_total", &pairs, &[]);
            label_heap_bytes(&ls)
        })
        .sum()
}

/// Which components grow with `N * M` (the number of (input, series) catalog
/// entries) and which do not. The ratios are asserted, not only printed: the
/// plan and index terms are a fixed cost per (input, series) entry, so 8x the
/// inputs at the same `M` is close to 8x their bytes, while the label term is
/// one shared copy per bucket series and the decoded, fetched and writer terms
/// track one series, one window or one part.
#[tokio::test]
async fn rseg_merge_peak_resident_bytes_by_component() {
    let small = measure(64).await;
    let large = measure(512).await;
    report(&small);
    report(&large);

    for m in [&small, &large] {
        let p = &m.peaks;
        assert_eq!(m.total_samples, (m.inputs * SERIES_PER_INPUT) as u64);
        // Every component the RSEG merge accounts was driven.
        assert!(p.catalog_plan_heap_bytes > 0);
        assert!(p.catalog_label_heap_bytes > 0);
        assert!(p.plan_index_heap_bytes > 0);
        assert!(p.fetched_raw_bytes > 0);
        assert!(p.decoded_heap_bytes > 0);
        assert!(m.writer > 0);
    }

    let ratio = |a: u64, b: u64| b as f64 / a as f64;
    let plan_ratio = ratio(
        small.peaks.catalog_plan_heap_bytes,
        large.peaks.catalog_plan_heap_bytes,
    );
    let index_ratio = ratio(
        small.peaks.plan_index_heap_bytes,
        large.peaks.plan_index_heap_bytes,
    );
    println!(
        "[rseg-merge-memory] N 64 -> 512: catalog plan x{plan_ratio:.2} index x{index_ratio:.2}"
    );
    // The plan structs and the index derived from them hold one entry per
    // (input, series): 8x the inputs at the same M is 8x the bytes, within the
    // fixed per-input and per-series overheads.
    assert!(
        (7.5..=8.5).contains(&plan_ratio),
        "catalog plan term grows with N * M: x{plan_ratio:.2}"
    );
    assert!(
        (5.0..=8.5).contains(&index_ratio),
        "index term grows with N * M: x{index_ratio:.2}"
    );
}

/// The bound issue #3817 adds: the planning catalogs retain one label set per
/// bucket series, shared by every input that carries it, so the label term is
/// exactly one copy of the bucket's label sets at any input count.
#[tokio::test]
async fn rseg_merge_label_heap_is_independent_of_input_count() {
    let bound = bucket_label_heap_bytes();
    for inputs in [64, 512] {
        let m = measure(inputs).await;
        report(&m);
        assert_eq!(
            m.peaks.catalog_label_heap_bytes, bound,
            "N={inputs}: the catalogs must retain one label set per bucket series"
        );
    }
}
