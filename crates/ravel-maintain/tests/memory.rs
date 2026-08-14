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
use ravel_maintain::{CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
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
    // Each input is one flush of the same HOT_SERIES series with 2 samples,
    // so the merge produces HOT_SERIES output series each with 3,600 runs
    // (the §9 hot-series shape).
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
    assert_eq!(total_runs, (INPUTS * HOT_SERIES) as u64);

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
