//! End-to-end compaction against the `MemoryStore` oracle, including listing
//! pagination via `with_page_size(2)`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, PublishOutcome, compact_bucket,
};
use ravel_object_store::memory::MemoryStore;
use ravel_segment::VERSION_V7;
use uuid::Uuid;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

fn three_inputs() -> Vec<InputSpec> {
    let w1 = Uuid::from_u128(1);
    let w2 = Uuid::from_u128(2);
    let w3 = Uuid::from_u128(3);
    vec![
        InputSpec::new(
            w1,
            10,
            1,
            vec![
                raw_series(
                    "http_requests",
                    &[("job", "a")],
                    &[(1_000, 1.0), (2_000, 2.0)],
                ),
                raw_series("http_requests", &[("job", "b")], &[(1_000, 10.0)]),
            ],
        ),
        InputSpec::new(
            w2,
            10,
            2,
            vec![
                // Same series as w1's job=a, later flush: a second run.
                raw_series("http_requests", &[("job", "a")], &[(3_000, 3.0)]),
                raw_series("cpu_seconds", &[("core", "0")], &[(1_500, -0.0)]),
            ],
        ),
        InputSpec::new(
            w3,
            10,
            3,
            vec![raw_series(
                "cpu_seconds",
                &[("core", "0")],
                &[(2_500, f64::NAN)],
            )],
        ),
    ]
}

#[tokio::test]
async fn compacts_bucket_and_preserves_all_samples() {
    let store = MemoryStore::new();
    let specs = three_inputs();
    for s in &specs {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    let outcome = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("compact");
    match outcome {
        CompactionOutcome::Compacted { parts, publish } => {
            assert!(parts >= 1);
            assert_eq!(publish, PublishOutcome::Published);
        }
        other => panic!("expected Compacted, got {other:?}"),
    }

    let record = fetch_compaction_record(&store, &bucket).await;
    // Inputs recorded in canonical order (writer_id, epoch, seq).
    assert_eq!(record.inputs.len(), 3);
    assert_eq!(record.level, 1);
    let ids: Vec<(&str, u64, u64)> = record
        .inputs
        .iter()
        .map(|i| (i.writer_id.as_str(), i.writer_epoch, i.writer_seq))
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "inputs must be canonically ordered");
    for p in &record.parts {
        assert_eq!(
            p.segment_format_version,
            u32::from(VERSION_V7),
            "v6 output (ADR-0026/0027)"
        );
    }

    // Every input sample survives, byte-for-byte (union view; dedup is a
    // query-time concern).
    let got = read_record_samples(&store, &record).await;
    let expected = expected_samples(&specs);
    assert_eq!(got, expected);
}

/// The epic's reachability test (ADR-0092, issue #315): drive a real bucket
/// through the SHIPPING compaction path, prove the L1 object holds exactly one
/// run per series (the whole point of run-merged L1), and confirm the samples
/// read back through the real segment decode path match the inputs bit-for-bit.
///
/// The input set has a series (`http_requests{job=a}`) present in two L0 flushes
/// with an overlapping timestamp, so it takes the multi-run merge path and its
/// two runs must collapse to one carrying per-sample provenance. The engine-level
/// dedup equivalence (that a query over the merged L1 picks the same winners as a
/// query over the inputs) is proven separately and rigorously by
/// `crates/ravel-query/tests/differential_compaction.rs`.
#[tokio::test]
async fn compacted_bucket_holds_one_run_per_series_and_query_matches_inputs() {
    let store = MemoryStore::new();
    let specs = three_inputs();
    for s in &specs {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    let outcome = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "expected Compacted, got {outcome:?}"
    );

    let record = fetch_compaction_record(&store, &bucket).await;
    for p in &record.parts {
        assert_eq!(p.segment_format_version, u32::from(VERSION_V7));
    }

    // Exactly one run per series after merging: the run-merged L1 invariant.
    let run_counts = record_run_counts(&store, &record).await;
    assert!(
        !run_counts.is_empty(),
        "the compacted bucket must carry series"
    );
    for (id, count) in &run_counts {
        assert_eq!(
            *count,
            1,
            "series {:02x?} must hold exactly one merged run, got {count}",
            &id[..8]
        );
    }

    // Query the L1 through the real decode path and compare to the union of the
    // inputs, bit-for-bit (compaction preserves every input sample; dedup is a
    // query-time concern, exercised by the differential test).
    let got = read_record_samples(&store, &record).await;
    let expected = expected_samples(&specs);
    assert_eq!(
        got, expected,
        "L1 samples must match the inputs bit-for-bit"
    );
}

#[tokio::test]
async fn dry_run_reports_the_plan_but_writes_nothing() {
    // Dry-run: config.dry_run computes the identical eligible set and part plan a
    // real run would, but skips every store.put. The reported outcome reflects
    // what a real run *would* have written; the store is left untouched.
    let store = MemoryStore::new();
    let specs = three_inputs();
    for s in &specs {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    let mut config = cfg();
    config.dry_run = true;
    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("dry-run compact");
    match outcome {
        CompactionOutcome::Compacted { parts, publish } => {
            assert!(parts >= 1, "dry run must report the parts it would write");
            assert_eq!(publish, PublishOutcome::Published);
        }
        other => panic!("expected Compacted, got {other:?}"),
    }

    // Nothing persisted: no l1/ part object anywhere.
    let all = ravel_object_store::list_all(&store, "t/")
        .await
        .expect("list");
    for meta in &all {
        assert!(
            !meta.key.contains("/l1/"),
            "dry run wrote an L1 part: {}",
            meta.key
        );
    }

    // A subsequent real run still sees the bucket as uncompacted, proving the
    // dry run published no compaction record.
    let real = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("real compact");
    assert!(
        matches!(real, CompactionOutcome::Compacted { .. }),
        "dry run must not have persisted a record: {real:?}"
    );
}

#[tokio::test]
async fn not_sealed_is_skipped() {
    let store = MemoryStore::new();
    for s in &three_inputs() {
        seed_input(&store, s).await;
    }
    // Now is before the seal margin.
    let clock = FixedClock::new((i64::from(HOUR) + 1) * NS_PER_HOUR);
    let outcome = compact_bucket(&store, &clock, &cfg(), &bucket())
        .await
        .expect("compact");
    assert_eq!(outcome, CompactionOutcome::NotSealed);
}

#[tokio::test]
async fn single_input_is_below_min() {
    let store = MemoryStore::new();
    let spec = InputSpec::new(
        Uuid::from_u128(1),
        10,
        1,
        vec![raw_series("m", &[], &[(1, 1.0)])],
    );
    seed_input(&store, &spec).await;
    let clock = FixedClock::new(sealed_now_ns());
    let outcome = compact_bucket(&store, &clock, &cfg(), &bucket())
        .await
        .expect("compact");
    assert_eq!(outcome, CompactionOutcome::BelowMinInputs { count: 1 });
}

#[tokio::test]
async fn second_run_sees_already_compacted() {
    let store = MemoryStore::new();
    for s in &three_inputs() {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("first");
    let second = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("second");
    assert_eq!(second, CompactionOutcome::AlreadyCompacted);
}

#[tokio::test]
async fn listing_pagination_is_handled() {
    // page_size 2 forces multi-page listing of the bucket's records.
    let store = MemoryStore::with_page_size(2);
    let mut specs = Vec::new();
    for i in 0..7u128 {
        specs.push(InputSpec::new(
            Uuid::from_u128(i + 1),
            10,
            (i + 1) as u64,
            vec![raw_series(
                "m",
                &[("i", &i.to_string())],
                &[(1_000, i as f64)],
            )],
        ));
    }
    for s in &specs {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    let outcome = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    let record = fetch_compaction_record(&store, &bucket).await;
    assert_eq!(record.inputs.len(), 7);
    let got = read_record_samples(&store, &record).await;
    assert_eq!(got, expected_samples(&specs));
}

#[tokio::test]
async fn part_splitting_keeps_series_whole_and_disjoint() {
    let store = MemoryStore::new();
    // Many distinct series so a tiny stored-size target forces several parts.
    let mut series = Vec::new();
    for i in 0..40u32 {
        series.push(raw_series(
            "m",
            &[("id", &format!("{i:03}"))],
            &[(1_000, i as f64), (2_000, (i * 2) as f64)],
        ));
    }
    let specs = vec![
        InputSpec::new(Uuid::from_u128(1), 10, 1, series.clone()),
        InputSpec::new(Uuid::from_u128(2), 10, 2, series),
    ];
    for s in &specs {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    let mut config = cfg();
    config.max_l1_part_bytes = 512; // force splits on series boundaries

    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

    let record = fetch_compaction_record(&store, &bucket).await;
    assert!(record.parts.len() >= 2, "tiny target must split into parts");

    // Part id ranges are disjoint and ascending (series never straddle).
    let mut prev_last: Option<[u8; 16]> = None;
    for p in &record.parts {
        let first: [u8; 16] = p.first_series_id.as_slice().try_into().unwrap();
        let last: [u8; 16] = p.last_series_id.as_slice().try_into().unwrap();
        assert!(first <= last);
        if let Some(pl) = prev_last {
            assert!(pl < first, "part id ranges must be disjoint and ordered");
        }
        prev_last = Some(last);
    }

    // Content still complete.
    let got = read_record_samples(&store, &record).await;
    assert_eq!(got, expected_samples(&specs));
}
