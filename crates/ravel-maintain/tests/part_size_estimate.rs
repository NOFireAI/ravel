//! The RSEG stored-size target is charged against every section that grows
//! with the data, not the TS/VAL pages alone.
//!
//! `max_l1_part_bytes` is named for the bytes of the object the builder is
//! about to write. Counting only the page payload left LABEL_DICT, the
//! per-sample provenance columns, SERIES_IDS and the SERIES_META cells out of
//! the figure the split rule compares, so a bucket whose weight sits in those
//! sections ran past the target while the estimate stayed under it, and the
//! excess accumulated from one series to the next.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::{CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// Padding bytes in each series' fat label value. The padding is pseudo-random
/// alphanumerics, not a repeated character: LABEL_DICT is zstd-compressed as a
/// whole section, so a compressible pad would leave the object far smaller than
/// the dictionary bytes the estimate charges and the object-size assertion
/// below would hold whatever the estimate counted.
const PAD_BYTES: usize = 400;

/// Distinct series in the bucket, each with its own fat label value.
const SERIES: usize = 60;

/// L0 inputs, each carrying every series. Every output series therefore merges
/// `INPUTS` runs into one and carries the per-sample provenance columns.
const INPUTS: usize = 4;

/// Samples one input contributes per series. Deliberately tiny: the page
/// payload must be a small share of the object so the sections the estimate
/// used to ignore are what decides where parts split.
const SAMPLES_PER_INPUT: usize = 2;

/// The stored-size target the split assertion runs under.
const STORED_TARGET: u64 = 8 * 1024;

/// Deterministic pseudo-random alphanumerics, `n` bytes, distinct per `seed`.
/// An LCG, so the fixture is reproducible and no series' label value is a copy
/// of another's.
fn incompressible_pad(seed: u64, n: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut out = String::with_capacity(n);
    for _ in 0..n {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        out.push(ALPHABET[(state >> 33) as usize % ALPHABET.len()] as char);
    }
    out
}

/// `INPUTS` L0 flushes, each carrying all `SERIES` series at disjoint
/// timestamps, so the merge interleaves every input's samples into one run per
/// series and every sample carries an explicit dedup key.
fn specs() -> Vec<InputSpec> {
    (0..INPUTS)
        .map(|i| {
            let series: Vec<RawSeries> = (0..SERIES)
                .map(|s| {
                    let pad = incompressible_pad(s as u64, PAD_BYTES);
                    let samples: Vec<(i64, f64)> = (0..SAMPLES_PER_INPUT)
                        .map(|k| {
                            (
                                1_000 + (k * INPUTS + i) as i64,
                                (s * 100 + k * INPUTS + i) as f64,
                            )
                        })
                        .collect();
                    raw_series("fat", &[("id", &pad)], &samples)
                })
                .collect();
            InputSpec::new(Uuid::from_u128(i as u128 + 1), 1, i as u64 + 1, series)
        })
        .collect()
}

async fn compact_with(target: u64) -> (MemoryStore, ravel_proto::commit::v1::CompactionRecord) {
    let store = MemoryStore::new();
    for s in &specs() {
        seed_input(&store, s).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    let config = CompactorConfig {
        max_l1_part_bytes: target,
        ..CompactorConfig::default()
    };
    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    let record = fetch_compaction_record(&store, &bucket).await;
    (store, record)
}

/// A part's LABEL_DICT and per-sample provenance columns are charged against
/// the stored-size target, so a bucket whose weight sits in those sections
/// splits on them rather than running past the target.
///
/// Both assertions are magnitudes proportional to the fixture, not floors. The
/// whole bucket in one object is more than twice the target (so a builder that
/// did not charge these sections could only emit that one object and fail),
/// and every part written under the target stays within twice it.
///
/// Demonstrated red by charging the pages only: reducing
/// `PartSizeEstimate::push_series` to `run_page_bytes` alone and
/// `STORED_PART_FIXED_BYTES` to 0 -- the shape this test exists to catch --
/// leaves this bucket's whole page payload under the 8_192-byte target, so the
/// builder emits a single 18_975-byte part, 2.3x the target, failing the size
/// assertion and then the part-count assertion at one part. A flat floor low
/// enough to be safe would have passed at both.
#[tokio::test]
async fn fat_dictionary_and_provenance_charge_against_the_stored_target() {
    // The fixture's own shape, measured rather than assumed: compacted with the
    // target far above the bucket, the whole thing is one object, and the fat
    // label values alone are most of its bytes. That is what makes this bucket
    // dictionary-dominated rather than page-dominated.
    let (_store, whole) = compact_with(u64::MAX).await;
    assert_eq!(whole.parts.len(), 1, "the default target must not split");
    let whole_bytes = whole.parts[0].object_size;
    let dict_bytes = (SERIES * PAD_BYTES) as u64;
    println!(
        "[part-size] whole object={whole_bytes}B fat label values={dict_bytes}B \
         target={STORED_TARGET}B"
    );
    assert!(
        dict_bytes * 2 > whole_bytes,
        "fixture must be dictionary-dominated: {dict_bytes}B of label values in a \
         {whole_bytes}B object"
    );
    assert!(
        whole_bytes > 2 * STORED_TARGET,
        "fixture must be worth splitting: {whole_bytes}B against a {STORED_TARGET}B target"
    );

    let (store, record) = compact_with(STORED_TARGET).await;
    println!(
        "[part-size] parts={} sizes={:?}",
        record.parts.len(),
        record
            .parts
            .iter()
            .map(|p| p.object_size)
            .collect::<Vec<_>>()
    );
    // The estimate is only worth having if it tracks the object it names.
    for p in &record.parts {
        assert!(
            p.object_size < 2 * STORED_TARGET,
            "part of {} bytes exceeded twice the {STORED_TARGET}-byte stored target",
            p.object_size
        );
    }
    assert!(
        record.parts.len() > 1,
        "the stored-size target must split this bucket, got {} part(s)",
        record.parts.len()
    );

    // Splitting must not change the data.
    let got = read_record_samples(&store, &record).await;
    assert_eq!(got, expected_samples(&specs()));
}
