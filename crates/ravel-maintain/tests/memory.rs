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

/// ADR-0979 decision 3: on the RLOG compaction path the retained-parts memory
/// term is ZERO, because each closed part's encoded bytes are released the
/// moment its PUT succeeds rather than held in `PartSink::parts` until publish.
///
/// This is the update to the #977 test that used to pin the retained-parts
/// high-water to the EXACT sum of the parts' encoded object sizes: that sum was
/// the plateau a large-bucket compaction sat at, and D3 removes it. The new
/// truth is the exact figure the charge site now produces. `finalize_part`
/// releases `built.bytes` at PUT under `retain_bytes = false` (compaction), so
/// `PartSink::flush` reads `built.bytes.as_ref().map_or(0, ..)` == 0 and charges
/// `t.add_retained_part_bytes(0)` for every part; the high-water never leaves
/// zero. It is pinned exactly (== 0), not bounded, and derived from the charge
/// site, not the fixture's byte totals -- the parts still carry real bytes
/// (their `object_size` is nonzero), and the point is that none of those bytes
/// stay resident.
///
/// Non-vacuity / flip proof: against the pre-D3 code (where `finalize_part`
/// returned `bytes: object` and never released it, and `PartSink::flush` charged
/// `built.bytes.len()`), `peak_retained_part_bytes()` equals the nonzero sum of
/// the parts' `object_size`, so the `== 0` assertion below fails
/// `<sum> != 0`. The flip is the exact behaviour D3 changes: retention at PUT
/// vs release at PUT. The other phases are still asserted driven, so a merge
/// that silently did nothing would fail here too.
#[tokio::test]
async fn compaction_retains_no_part_bytes() {
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
    // The parts really do carry bytes; the point is none of them stay resident.
    let part_bytes_sum: u64 = record.parts.iter().map(|p| p.object_size).sum();
    assert!(
        part_bytes_sum > 0,
        "the parts must carry real encoded bytes for the release to be meaningful"
    );

    // The exact new truth: the retained-parts high-water is zero, because every
    // part's bytes were released at PUT (D3), not held until publish.
    assert_eq!(
        tracker.peak_retained_part_bytes(),
        0,
        "compaction must retain no part bytes; each is released at PUT (ADR-0979 D3)"
    );

    let peaks = tracker.phase_peaks();
    assert_eq!(
        peaks.retained_part_encoded_bytes, 0,
        "phase_peaks must report zero retained bytes on the compaction path"
    );
    // Sanity: the merge really exercised the other phases too, so the zero above
    // is a real release, not an inert run.
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
        "[memory:rlog:979] parts={} part_bytes_sum={part_bytes_sum} \
         retained_part_encoded_bytes={} writer_heap_bytes={} \
         cursor_bytes={} catalog_directory_decoded_bytes={} publish_record_encoded_bytes={}",
        record.parts.len(),
        peaks.retained_part_encoded_bytes,
        peaks.writer_heap_bytes,
        peaks.cursor_bytes,
        peaks.catalog_directory_decoded_bytes,
        peaks.publish_record_encoded_bytes,
    );
}

/// A tracker left installed across SERIAL runs reports each run's own peaks:
/// `rewrite_and_publish` calls `reset_for_run` before any accounting, so the
/// second bucket's figures are its own. Pinned here at the tracker level
/// (accumulate, reset, accumulate less, read exactly the smaller figure);
/// demonstrated failing by dropping the `reset_for_run` call, which reads
/// 700, not 300.
#[test]
#[allow(clippy::expect_used)]
fn reset_for_run_scopes_peaks_to_one_run() {
    use ravel_maintain::MergeMemoryTracker;
    let t = MergeMemoryTracker::new();
    t.add_retained_part_bytes(700);
    t.add_catalog_directory_bytes(50);
    let first = t.phase_peaks();
    assert_eq!(first.retained_part_encoded_bytes, 700);
    assert_eq!(first.catalog_directory_decoded_bytes, 50);

    t.reset_for_run();
    let cleared = t.phase_peaks();
    assert_eq!(cleared.retained_part_encoded_bytes, 0, "reset clears");
    assert_eq!(cleared.catalog_directory_decoded_bytes, 0, "reset clears");

    t.add_retained_part_bytes(300);
    let second = t.phase_peaks();
    assert_eq!(
        second.retained_part_encoded_bytes, 300,
        "the second run reports only its own bytes; without the reset this reads 700"
    );
    assert_eq!(second.catalog_directory_decoded_bytes, 0);
}
