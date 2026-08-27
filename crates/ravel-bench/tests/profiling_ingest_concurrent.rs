//! Regression guard for issue #616's re-opened framing: the pprof sampler
//! must survive a real ingest run at the concurrency a fast-load
//! configuration actually produces, in a single long-lived session, with no
//! repeated `--runs`.
//!
//! Field evidence (two loads on the same box, same corpus, same branch head,
//! `--pipeline-depth 4 --max-inflight-flushes 4`, differing only in whether a
//! sampler was armed): the profiled load segfaulted (exit 139, signal 11) at
//! 720s with 4.94 cores busy; the unprofiled load completed cleanly at 1,519s.
//! A separate profiled load that stayed at 2.33 cores busy survived 4,466s.
//! The variable is thread count, not run count: `ingest_bench`'s own write
//! loop (`ravel_bench::ingest::run`) spawns one tokio task per batch as soon
//! as pacing allows, with no cap on how many `IngestRouter::write` calls run
//! concurrently, so raising `--points-per-sec` relative to `--batch-size` (or
//! raising `--max-inflight-flushes`, which lets more flushes run at once
//! per shard) raises the number of OS threads with a `SIGPROF`-interruptible
//! stack, not the run count `runs_supported_with_profiling` guards. That
//! guard (in `sql_latency_bench`) still matters for repeated statements, but
//! it does not cover this axis, and `ingest_bench` has no `--runs` flag to
//! guard on in the first place.
//!
//! This test reproduces that shape with the real ingest code path (not a
//! synthetic hammer): an in-memory `IngestRouter` driven by
//! `ravel_bench::ingest::run_with_profile` at `--shards 4
//! --max-inflight-flushes 4`, a small batch size against a high points/sec
//! rate so thousands of write tasks are in flight concurrently, under a live
//! `ProfileSession`. If the sampler is unsafe at this concurrency the process
//! dies with SIGSEGV and the test binary exits non-zero; a clean run is the
//! regression guard for the sampling rate this fix lands on. It cannot prove
//! the hazard absent on every host (the crash is probabilistic and varies
//! with architecture, core count, and glibc), so a pass here means "did not
//! reproduce on this host," not "provably safe" -- see the module doc on
//! `ravel_bench::profiling` for why the fix is a lowered sampling rate plus an
//! external-profiler path, not a claim of safety.
#![cfg(feature = "profiling")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::ingest::{IngestBenchArgs, run_with_profile};
use ravel_bench::profiling::ProfileSession;

/// `worker_threads(8)` rather than the default (core-count-dependent): the
/// hazard scales with concurrent `SIGPROF`-interruptible threads, so the test
/// must not go quiet on a small host. Real ingest work (encode, route, flush,
/// object-store PUT) against `MemoryStore`, not a sleep loop, so every worker
/// thread actually spends time on-CPU for the sampler to interrupt.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sampler_survives_a_real_high_concurrency_ingest_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ingest-concurrent.svg");

    // Small batches at a high points/sec rate keep the pacing interval
    // (batch_size / points_per_sec) tiny, so `run`'s dispatch loop spawns
    // thousands of write tasks with almost no gap between them: the same
    // "many tasks in flight" shape the field evidence's high-concurrency load
    // produced, reached here with `--shards 4 --max-inflight-flushes 4`
    // instead of `--pipeline-depth`.
    let args = IngestBenchArgs::try_parse_from([
        "ingest_bench",
        "--store",
        "memory",
        "--shards",
        "4",
        "--target-series",
        "500",
        "--points-per-sec",
        "500000",
        "--duration-secs",
        "1",
        "--batch-size",
        "50",
        "--max-inflight-flushes",
        "4",
    ])
    .expect("parse args");
    args.validate().expect("args valid");
    let config = args.to_config();

    let report = run_with_profile(&config, || {
        ProfileSession::to_path("ingest_bench", path.clone())
    })
    .await;

    assert!(
        report.accepted_points > 0,
        "the run under test must actually accept points, or it proves nothing about \
         concurrent write-path CPU"
    );

    let bytes = std::fs::read(&path).expect("read written flamegraph");
    assert!(!bytes.is_empty(), "flamegraph SVG must not be empty");
    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
    assert!(
        head.contains("<svg") || head.contains("<?xml"),
        "written file should be an SVG document, got: {head}"
    );
}
