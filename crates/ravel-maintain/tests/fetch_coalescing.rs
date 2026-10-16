//! Page-fetch mechanics of the RSEG compactor:
//! coalescing adjacent page ranges from the same input into one GET, and
//! keeping the buffered page bytes bounded to one part under the concurrent
//! fetch. These assert the *mechanics* only; the differential-equivalence of
//! the compaction output (same samples, same part bytes) is pinned by
//! `end_to_end.rs` and `determinism.rs` and must stay unchanged, so each test
//! here also re-checks the samples round-trip to prove the new fetch path did
//! not perturb what gets written.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::{CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::InstrumentedStore;
use ravel_object_store::instrument::StoreOp;
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// Build `inputs` L0 flushes, each carrying the same `series_per_input` hot
/// series (one run each). In the merge every input contributes one run to each
/// output series, and within one input object those runs' TS pages are
/// contiguous in the TS section and their VAL pages contiguous in the VAL
/// section, so coalescing collapses each input's page fetch to two GETs.
fn specs(inputs: usize, series_per_input: usize) -> Vec<InputSpec> {
    (0..inputs)
        .map(|i| {
            let series: Vec<RawSeries> = (0..series_per_input)
                .map(|s| {
                    raw_series(
                        "hot",
                        &[("series", &format!("{s:03}"))],
                        &[
                            (1_000 + i as i64, i as f64),
                            (2_000 + i as i64, (i * 3) as f64),
                        ],
                    )
                })
                .collect();
            InputSpec::new(Uuid::from_u128(i as u128 + 1), 1, i as u64 + 1, series)
        })
        .collect()
}

/// Coalescing turns the naive ~2R sequential page GETs into a couple of GETs
/// per input object. With `INPUTS` inputs each carrying `SERIES` series (one
/// run each), the merge has `R = INPUTS * SERIES` runs, so the naive path would
/// issue `2R` page GETs alone. This asserts the *total* store GET count
/// (record + footer + catalog reads plus every page GET) stays below `R`,
/// which `2R` page GETs could never do: the only way under `R` total GETs is
/// coalescing many runs' pages into shared GETs.
#[tokio::test]
async fn coalescing_cuts_page_gets_far_below_two_per_run() {
    const INPUTS: usize = 4;
    const SERIES: usize = 25;
    const RUNS: u64 = (INPUTS * SERIES) as u64; // 100 runs -> naive 200 page GETs

    let store = InstrumentedStore::new(MemoryStore::new());
    for s in &specs(INPUTS, SERIES) {
        seed_input(&store, s).await;
    }

    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    // Default target (256 MiB) >> this bucket, so all SERIES land in one part /
    // one fetch batch: every input's runs coalesce to a TS GET + a VAL GET.
    let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

    let gets = store.metrics().snapshot().op(StoreOp::Get).calls;
    println!(
        "[coalesce] inputs={INPUTS} series={SERIES} runs={RUNS} \
         total store GETs={gets} (naive page GETs alone = {})",
        2 * RUNS
    );
    // Naive fetch: 2 GETs per run = 2R = 200 page GETs, before any of the
    // record/footer/catalog reads. Coalesced: ~2 page GETs per input object
    // (8) plus the read-phase GETs, comfortably under R.
    assert!(
        gets < RUNS,
        "expected far fewer than {RUNS} GETs with coalescing, got {gets}"
    );

    // The page mechanics changed, the output must not: samples round-trip.
    let record = fetch_compaction_record(&store, &bucket).await;
    let got = read_record_samples(&store, &record).await;
    assert_eq!(got, expected_samples(&specs(INPUTS, SERIES)));
}

/// Adjacent runs in the *same* object share coalesced GETs: a single input
/// carrying many series (one run each) is fetched with a bounded, tiny number
/// of page GETs rather than two per run. Isolates the "same object" claim by
/// counting GETs against a one-input bucket compacted with `min_compaction_inputs = 1`.
#[tokio::test]
async fn adjacent_runs_in_one_object_coalesce() {
    const SERIES: usize = 40;

    let store = InstrumentedStore::new(MemoryStore::new());
    // One input, many series. min_compaction_inputs=1 makes the single input a
    // valid (degenerate) compaction so the build path still runs.
    let spec = &specs(1, SERIES)[0];
    seed_input(&store, spec).await;

    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    let config = CompactorConfig {
        min_compaction_inputs: 1,
        ..CompactorConfig::default()
    };
    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

    let gets = store.metrics().snapshot().op(StoreOp::Get).calls;
    let series = SERIES as u64;
    println!(
        "[coalesce-1obj] series={SERIES} total store GETs={gets} (naive page GETs = {})",
        2 * series
    );
    // The one object's SERIES runs (naive: 2*SERIES = 80 page GETs) collapse to
    // a couple of coalesced GETs; the whole compaction, read phase included,
    // stays well under the naive page-GET count for this single object.
    assert!(
        gets < series,
        "one object's {SERIES} adjacent runs should coalesce well under {SERIES} GETs, got {gets}"
    );
}

/// The stored-size target still splits parts under the concurrent, coalesced
/// fetch: with a target small enough to force many parts, the builder splits on
/// series boundaries rather than accumulating the bucket into one part. Proven
/// by (a) the output splitting into several parts and (b) the samples still
/// round-tripping exactly across those parts.
///
/// This is about where parts split, not about a ceiling on the builder's heap:
/// finished parts are retained until publish, and one series is materialized
/// whole, neither of which any config knob sizes (`build.rs` header comment).
#[tokio::test]
async fn small_part_target_splits_output_under_coalesced_fetch() {
    const INPUTS: usize = 6;
    const SERIES: usize = 30;

    let store = MemoryStore::new();
    let specs = specs(INPUTS, SERIES);
    for s in &specs {
        seed_input(&store, s).await;
    }

    // The target is set to a few series' worth of stored bytes, forcing many
    // part flushes (each flush releases the buffered part). One output series
    // here is INPUTS runs of small pages merged into one run.
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    // A deliberately tiny target: far below the whole bucket's stored bytes, so
    // the builder must flush many parts. If the fetch path buffered the whole
    // bucket (ignoring the target) this would still pass functionally, so the
    // multi-part assertion below is what ties correctness to the target: parts
    // only appear when the part's stored-size estimate crosses it mid-merge.
    let config = CompactorConfig {
        max_l1_part_bytes: 1024,
        ..CompactorConfig::default()
    };
    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

    let record = fetch_compaction_record(&store, &bucket).await;
    println!(
        "[mem-bound] inputs={INPUTS} series={SERIES} target=1024B parts={}",
        record.parts.len()
    );
    assert!(
        record.parts.len() > 1,
        "a sub-bucket stored-size target must split output into multiple parts, got {}",
        record.parts.len()
    );

    // Splitting must not change the data: every sample round-trips across the
    // parts (the differential-equivalence invariant, under the new fetch path).
    let got = read_record_samples(&store, &record).await;
    assert_eq!(got, expected_samples(&specs));
}
