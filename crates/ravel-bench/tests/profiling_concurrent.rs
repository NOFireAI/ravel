//! Regression guard for issue #680: the profiling sampler must survive a
//! concurrent, allocation-heavy, syscall-heavy workload.
//!
//! On 2026-08-25 `sql_latency_bench` with `RAVEL_BENCH_PROFILE_SVG` set
//! segfaulted (exit 139, no Rust panic) the first time the logs scan lane ran
//! its segment prunes and scan partitions concurrently. The single-threaded
//! plan phase had survived a 600 s profiled run; concurrency across many worker
//! threads is what crosses the line. The mechanism is the one the crate's own
//! module doc already names: `pprof`'s `cpp` unwinder goes through libgcc's
//! `_Unwind_Backtrace`, which is not async-signal-safe, and a `SIGPROF` tick
//! that lands while a thread is mid-unwind or holds the loader lock can fault.
//! More concurrent unwinds per unit of wall-clock raise the odds of one tick
//! landing in the unsafe window.
//!
//! This test reproduces that shape: a 16-worker tokio runtime whose tasks churn
//! the malloc arena (build and drop `Vec<u8>` of random sizes) and hit the
//! kernel and loader (`std::fs::metadata`) in a tight loop, all under a live
//! `ProfileSession`, repeated five times. If the sampler is unsafe the process
//! dies with SIGSEGV and the test binary exits non-zero; a clean run is the
//! regression guard for whatever configuration the fix lands on. It cannot
//! prove the hazard absent on every host (the crash is probabilistic and varies
//! with architecture, core count, and glibc), so a pass here means "did not
//! reproduce on this host", not "provably safe".
#![cfg(feature = "profiling")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::{Duration, Instant};

use rand::{RngExt, SeedableRng};
use ravel_bench::profiling::ProfileSession;

/// Drive a 16-worker tokio runtime doing allocation-heavy work interleaved with
/// blocking syscalls for roughly `dur`. Mixes the malloc arena, the loader, and
/// the kernel the way the concurrent logs scan does.
fn hammer(dur: Duration) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    rt.block_on(async move {
        let deadline = Instant::now() + dur;
        let mut handles = Vec::new();
        for task in 0..16u64 {
            handles.push(tokio::spawn(async move {
                let mut rng = rand::rngs::StdRng::seed_from_u64(0x5eed ^ task);
                let mut sink: u64 = 0;
                while Instant::now() < deadline {
                    // Allocation churn of varying sizes exercises the malloc
                    // arena and the loader on arena growth.
                    let n = rng.random_range(1..=64 * 1024usize);
                    let mut v = vec![0u8; n];
                    for (i, b) in v.iter_mut().enumerate() {
                        *b = (i as u8).wrapping_mul(31);
                    }
                    sink = sink.wrapping_add(v.iter().map(|&b| b as u64).sum::<u64>());
                    drop(v);
                    // A real blocking syscall on every iteration: this is the
                    // kernel/loader boundary a SIGPROF tick can interrupt.
                    let _ = std::fs::metadata(".");
                }
                sink
            }));
        }
        let mut total: u64 = 0;
        for h in handles {
            total = total.wrapping_add(h.await.unwrap_or(0));
        }
        std::hint::black_box(total);
    });
}

/// Five profiled rounds of the concurrent workload. The assertion the test
/// really makes is negative: the process must not SIGSEGV. When `finish` does
/// write an SVG it must be a real, non-empty document.
#[test]
fn sampler_survives_concurrent_allocation_and_syscalls() {
    let dir = tempfile::tempdir().expect("tempdir");
    for round in 0..5 {
        let path = dir.path().join(format!("concurrent-{round}.svg"));
        let session = ProfileSession::to_path("concurrent-test", path.clone());
        hammer(Duration::from_secs(3));
        if let Some(written) = session.finish() {
            let bytes = std::fs::read(&written).expect("read written flamegraph");
            assert!(!bytes.is_empty(), "flamegraph SVG must not be empty");
            let head = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
            assert!(
                head.contains("<svg") || head.contains("<?xml"),
                "written file should be an SVG document, got: {head}"
            );
        }
    }
}
