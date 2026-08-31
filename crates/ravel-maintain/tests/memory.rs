//! Peak-memory measurement on a synthetic 3,600-input bucket.
//!
//! Method: seed 3,600 L0 inputs (each a small flush of the same hot series)
//! into a `MemoryStore`, record the process resident set (VmRSS) and peak
//! (VmHWM) from `/proc/self/status` after seeding, then compact and record
//! them again. The compactor's incremental peak is `VmHWM(after) -
//! VmRSS(after seeding)`: the extra resident memory the build/publish pass
//! needed on top of the already-resident input objects. The design bound is
//! catalog metadata for all inputs plus one in-flight part buffer, since
//! page bytes are fetched by range during the merge, not held.
//!
//! This is a measurement with a deliberately loose upper-bound assertion, not
//! a tight gate; the printed numbers are what the report cites. Run with
//! `--nocapture` to see them.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, MergeMemoryTracker, compact_bucket,
};
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// (VmRSS, VmHWM) in kibibytes from /proc/self/status, if available.
fn mem_kib() -> Option<(u64, u64)> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut rss = None;
    let mut hwm = None;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss = rest.split_whitespace().next().and_then(|v| v.parse().ok());
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            hwm = rest.split_whitespace().next().and_then(|v| v.parse().ok());
        }
    }
    Some((rss?, hwm?))
}

#[tokio::test]
async fn peak_memory_on_3600_input_bucket() {
    const INPUTS: usize = 3_600;
    const HOT_SERIES: usize = 10;

    let store = MemoryStore::new();
    // Each input is one flush of the same HOT_SERIES series with 2 samples. Under
    // run-merged L1 compaction (ADR-0092 decision 1) every series' 3,600
    // contributing runs are decoded, merged in timestamp order, and re-encoded
    // into ONE run, so the output holds exactly HOT_SERIES runs. Decoding those
    // 3,600 runs' samples one series at a time is the memory shape this test
    // pins: it must stay under the ceiling even though the decoded samples now
    // live alongside the fetch buffer.
    let mut input_bytes: u64 = 0;
    for i in 0..INPUTS {
        let series: Vec<RawSeries> = (0..HOT_SERIES)
            .map(|s| {
                raw_series(
                    "hot",
                    &[("series", &format!("{s:02}"))],
                    &[
                        (1_000 + i as i64, i as f64),
                        (2_000 + i as i64, (i * 2) as f64),
                    ],
                )
            })
            .collect();
        let spec = InputSpec::new(Uuid::from_u128(i as u128 + 1), 1, i as u64 + 1, series);
        let written = build_v5_l0(
            &spec.series,
            spec.hour,
            (spec.created_unix_ns, spec.epoch, spec.seq),
        );
        input_bytes += written.bytes.len() as u64;
        seed_input(&store, &spec).await;
    }

    let after_seed = mem_kib();
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact 3600-input bucket");
    let after_compact = mem_kib();

    let record = fetch_compaction_record(&store, &bucket).await;
    let total_runs: u64 = record.parts.iter().map(|p| p.run_count).sum();
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    assert_eq!(record.inputs.len(), INPUTS);
    // One run per series after merging, not one per (input, series).
    assert_eq!(total_runs, HOT_SERIES as u64);

    match (after_seed, after_compact) {
        (Some((rss_seed, hwm_seed)), Some((rss_c, hwm_c))) => {
            let incremental = hwm_c.saturating_sub(rss_seed);
            println!(
                "[memory] inputs={INPUTS} hot_series={HOT_SERIES} \
                 input_bytes={input_bytes} parts={} total_runs={total_runs}",
                record.parts.len()
            );
            println!(
                "[memory] after_seed: VmRSS={rss_seed} KiB VmHWM={hwm_seed} KiB | \
                 after_compact: VmRSS={rss_c} KiB VmHWM={hwm_c} KiB"
            );
            println!(
                "[memory] compaction incremental peak (VmHWM_after - VmRSS_after_seed) = {incremental} KiB"
            );
            // Loose ceiling: the compactor must not blow up to gigabytes on a
            // few-MB bucket. Catalog metadata + one part buffer is single-digit
            // to low-double-digit MiB at this shape.
            assert!(
                incremental < 512 * 1024,
                "compaction incremental peak {incremental} KiB exceeded 512 MiB ceiling"
            );
        }
        _ => {
            println!("[memory] /proc/self/status unavailable; skipping RSS assertion");
        }
    }
}

/// The RLOG analogue: the ranged reader means the merge holds only
/// per-input directories plus one stream's blocks at a time, so the compactor's
/// incremental peak stays bounded as the input count grows rather than scaling
/// with the whole bucket's raw bytes (the pre-#275 merge held every input
/// object whole and resident at once). Same method and loose ceiling as the
/// RSEG test above.
#[tokio::test]
async fn peak_memory_on_rlog_input_bucket() {
    const INPUTS: usize = 3_600;
    const HOT_STREAMS: u32 = 10;

    let store = MemoryStore::new();
    // Each input is one flush of the same HOT_STREAMS log streams with 2 records
    // each, so the merge produces HOT_STREAMS output streams each with
    // INPUTS * 2 records (the hot-stream shape).
    for i in 0..INPUTS {
        let records: Vec<_> = (0..HOT_STREAMS)
            .flat_map(|s| {
                [
                    log_record(s, 1_000 + i as i64, "hot log line one"),
                    log_record(s, 2_000 + i as i64, "hot log line two"),
                ]
            })
            .collect();
        seed_rlog_input(
            &store,
            Uuid::from_u128(i as u128 + 1),
            1,
            i as u64 + 1,
            &records,
        )
        .await;
    }

    let after_seed = mem_kib();
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = logs_bucket();
    let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact 3600-input rlog bucket");
    let after_compact = mem_kib();

    let record = fetch_compaction_record(&store, &bucket).await;
    let total_samples: u64 = record.parts.iter().map(|p| p.sample_count).sum();
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    assert_eq!(record.inputs.len(), INPUTS);
    assert_eq!(total_samples, (INPUTS as u64) * u64::from(HOT_STREAMS) * 2);

    match (after_seed, after_compact) {
        (Some((rss_seed, hwm_seed)), Some((rss_c, hwm_c))) => {
            let incremental = hwm_c.saturating_sub(rss_seed);
            println!(
                "[memory:rlog] inputs={INPUTS} hot_streams={HOT_STREAMS} \
                 parts={} total_samples={total_samples}",
                record.parts.len()
            );
            println!(
                "[memory:rlog] after_seed: VmRSS={rss_seed} KiB VmHWM={hwm_seed} KiB | \
                 after_compact: VmRSS={rss_c} KiB VmHWM={hwm_c} KiB"
            );
            println!(
                "[memory:rlog] compaction incremental peak (VmHWM_after - VmRSS_after_seed) = {incremental} KiB"
            );
            assert!(
                incremental < 512 * 1024,
                "rlog compaction incremental peak {incremental} KiB exceeded 512 MiB ceiling"
            );
        }
        _ => {
            println!("[memory:rlog] /proc/self/status unavailable; skipping RSS assertion");
        }
    }
}

/// Issue #977: the retained closed parts are attributed to their OWN phase term
/// and are NOT folded into the writer term.
///
/// A logs compaction under a small stored-size target closes many L1 parts.
/// Every closed part is PUT but its encoded bytes stay resident in
/// `PartSink::parts` until publish; that retained residency is the plateau a
/// large-bucket compaction sits at, and before this ticket it was invisible in
/// the [`MergeMemoryTracker`]. This drives a compaction that closes at least
/// three parts and asserts the tracker's retained-parts high-water equals the
/// EXACT sum of those parts' encoded object sizes (read back from the published
/// record's `object_size`, which `finalize_part` sets to the same bytes it
/// retains). The value is derivable, so it is pinned exactly, not bounded.
///
/// Non-vacuity / flip proof: the retained bytes are charged at `rlog.rs`'s
/// `PartSink::flush`, in the line `t.add_retained_part_bytes(retained);`.
/// Deleting that line (attributing nothing to the retained phase, i.e. leaving
/// the bytes accounted only through the writer term that `set_writer_bytes`
/// drives) makes `peak_retained_part_bytes()` stay 0, and the exact-equality
/// assertion below fails `0 != <sum>`. Rerouting it to `t.set_writer_bytes(
/// retained)` (folding the retained bytes into the writer phase, the bug this
/// test exists to catch) fails the same way. Verified failing both ways before
/// restoring.
#[tokio::test]
async fn retained_parts_are_attributed_to_their_own_phase() {
    const INPUTS: usize = 24;
    const STREAMS: u32 = 4;
    const RECORDS_PER_STREAM: usize = 4;

    let store = MemoryStore::new();
    // A body long enough that a handful of records exceeds the tiny stored-size
    // target below, so the merge closes many parts rather than one.
    let body: String = "log-line-".repeat(24);
    for i in 0..INPUTS {
        let mut records = Vec::new();
        for s in 0..STREAMS {
            for r in 0..RECORDS_PER_STREAM {
                let ts = 1_000 + (i * RECORDS_PER_STREAM + r) as i64;
                records.push(log_record(s, ts, &format!("{body}-{s}-{i}-{r}")));
            }
        }
        seed_rlog_input(
            &store,
            Uuid::from_u128(i as u128 + 1),
            1,
            i as u64 + 1,
            &records,
        )
        .await;
    }

    let tracker = MergeMemoryTracker::new();
    let config = CompactorConfig {
        // Small stored-size target so parts close often; this changes only where
        // records split into parts, never their bytes.
        max_l1_part_bytes: 2048,
        merge_memory_tracker: Some(tracker.clone()),
        ..CompactorConfig::default()
    };
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = logs_bucket();
    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("compact rlog bucket");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

    let record = fetch_compaction_record(&store, &bucket).await;
    assert!(
        record.parts.len() >= 3,
        "the small stored target must close at least three parts, got {}",
        record.parts.len()
    );

    // The retained-parts phase term equals the exact encoded size of every part
    // the merge held: each `object_size` is the byte length `finalize_part`
    // retained in `PartSink::parts`, and every closed part is retained until
    // publish, so the high-water is their sum.
    let expected_retained: u64 = record.parts.iter().map(|p| p.object_size).sum();
    assert_eq!(
        tracker.peak_retained_part_bytes(),
        expected_retained,
        "retained-parts high-water must equal the exact sum of the parts' encoded sizes"
    );

    // The retained term is its own phase, not the writer term. The writer term
    // is decoded Rust heap of one in-progress part at a time; the retained term
    // is the encoded bytes of every closed part at once. Read them back from the
    // phase split and confirm the split reports the retained sum under the
    // retained field, and the writer field is a different (heap) quantity that
    // does not carry the retained bytes.
    let peaks = tracker.phase_peaks();
    assert_eq!(
        peaks.retained_part_encoded_bytes, expected_retained,
        "phase_peaks must carry the retained sum in its retained field"
    );
    assert_ne!(
        peaks.retained_part_encoded_bytes, peaks.writer_heap_bytes,
        "retained (encoded, all parts) must not be folded into writer (heap, one part)"
    );
    // Sanity: the merge really exercised the other phases too.
    assert!(
        peaks.writer_heap_bytes > 0,
        "the in-progress writer term must have been driven"
    );
    assert!(
        peaks.catalog_directory_decoded_bytes > 0,
        "the catalog-load term must have been driven"
    );
    assert!(
        peaks.publish_record_encoded_bytes > 0,
        "the finish/publish term must have been driven"
    );
    println!(
        "[memory:rlog:977] parts={} retained_part_encoded_bytes={} writer_heap_bytes={} \
         cursor_bytes={} catalog_directory_decoded_bytes={} publish_record_encoded_bytes={}",
        record.parts.len(),
        peaks.retained_part_encoded_bytes,
        peaks.writer_heap_bytes,
        peaks.cursor_bytes,
        peaks.catalog_directory_decoded_bytes,
        peaks.publish_record_encoded_bytes,
    );
}
