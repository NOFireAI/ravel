//! Optional CPU flamegraph lane for the ingest and query benchmark cores.
//!
//! This is the workspace's first CPU attribution of any kind: before it, every
//! claim about where ingest or query CPU goes was an argument from code shape,
//! not a measurement (issue #365). It is measurement infrastructure with no
//! production caller: the only thing that starts a `ProfileSession` is a human
//! (or CI) running one of the bench binaries with the environment variable set.
//! No production ingest/query path changes.
//!
//! The lane is gated behind the `profiling` cargo feature so a default build
//! never links `pprof`. When the feature is off this module is a set of no-op
//! stubs, so the bench cores can call it unconditionally.
//!
//! # Usage
//!
//! Set [`PROFILE_ENV`] to the SVG path you want written, then run a bench bin
//! built with `--features profiling`. The sampler brackets only the measured
//! region (it is started after fixture generation and stopped before the report
//! is assembled), so the flamegraph reflects the code under test rather than
//! workload construction. For a dense CPU picture, drive the ingest bench with
//! a high `--points-per-sec` (the total point count is `points-per-sec *
//! duration-secs`, so a value of `0` produces an empty workload and an empty
//! profile). CPU sampling only accrues on-CPU time, so the pacing this implies
//! does not dilute the flamegraph.
//!
//! # The profiler perturbs the run it profiles
//!
//! This is a signal-based sampler: `pprof` arms `ITIMER_PROF`, and each
//! `SIGPROF` delivery unwinds the stack of the running thread. That unwind is
//! not free and it fires on the same threads doing the ingest/query work, so a
//! profiled run is measurably slower than an unprofiled one. The latency
//! figures a bench core reports while a profile is active (for example the ack
//! p50/p95/p99 in `ingest.rs`) are therefore inflated by the sampler and are
//! worse than the real latencies. Read the flamegraph for CPU *attribution*
//! only; quote latency numbers from an unprofiled run, never from a profiled
//! one.
//!
//! # Known instability under repeated execution
//!
//! A single [`ProfileSession`] holds one `pprof::ProfilerGuard` live across the
//! whole measured region. When that region runs each statement more than once
//! (`sql_latency_bench --runs N` with `N > 1`, where the guard brackets every
//! run of every corpus entry), the profiled process has been observed to
//! segfault; `--runs 1`, and any run with profiling off, is stable (issue
//! #616).
//!
//! The cause is upstream in the sampler, not in this crate's measurement loop
//! or in the SQL engine under test. `pprof` is a signal sampler: each `SIGPROF`
//! delivery unwinds the interrupted thread's stack from inside the signal
//! handler, and that unwind goes through the platform unwinder
//! (`backtrace::trace_unsynchronized`, i.e. libgcc's `_Unwind_Backtrace`),
//! which is not async-signal-safe. `pprof`'s own documentation carries the
//! gperftools warning verbatim: libgcc's unwind "is not safe to use from signal
//! handlers", and a tick that lands while a thread is mid-unwind or holds the
//! loader lock can deadlock or fault. The blocklist passed to the guard
//! mitigates but cannot close this: it only suppresses samples whose
//! interrupted instruction pointer is already inside a blocklisted segment, not
//! one that faults while unwinding out of application code.
//!
//! Why `--runs 3` crosses a line `--runs 1` does not: the plan, the data, and
//! the per-statement work are identical between the two. The only thing more
//! runs change is how long the guard stays armed, and therefore how many
//! async-signal-unsafe unwinds occur. The crash is probabilistic in that count,
//! so more executions raise the odds of one tick landing in the unsafe window.
//! That also means it is not deterministic on every host: it did not reproduce
//! under this crate's own repeated attempts on an aarch64 Linux box (default
//! and widened corpora, `--runs` up to 10, sampling frequency raised well past
//! the default), which is consistent with a probability that varies with
//! architecture, core count, and glibc rather than a fixed trigger. Do not read
//! a clean profiled `--runs 3` on one host as evidence the hazard is absent.
//!
//! Operational guidance: for a profiled pass, use `--runs 1`. One execution per
//! statement already produces a dense CPU flamegraph (the sampler accrues a
//! stack every ~1 ms of on-CPU time), and the latency numbers from a profiled
//! run are unusable anyway (see the section above), so the extra runs buy the
//! profile nothing while adding the exposure that risks the crash. Take latency
//! from a separate unprofiled multi-run pass.
//!
//! `sql_latency_bench` enforces this rather than trusting the operator to
//! remember it: a run with the profiler armed (`PROFILE_ENV` set and the
//! `profiling` feature compiled in) and `--runs > 1` is refused up front by
//! [`runs_supported_with_profiling`] before the guard is built, so the failure
//! is a clear error instead of a segfault partway through the first statement.
//! The profiler alone (`--runs 1`) and repeated runs alone (profiler off) are
//! both unaffected.
//!
//! # Concurrent-query segfault and the query-lane sampling rate (issue #680)
//!
//! The same hazard fires without repeated runs once the measured region goes
//! concurrent. On 2026-08-25 `sql_latency_bench` with `RAVEL_BENCH_PROFILE_SVG`
//! set died with exit 139 (SIGSEGV, no Rust panic) after 42 s, the first time
//! the logs scan lane ran eight segment prunes and eight scan partitions at
//! once; the same sampler had survived a 600 s run while the plan phase was
//! sequential, and the unprofiled run is fine. The mechanism is unchanged: each
//! `SIGPROF` unwind goes through libgcc's async-signal-unsafe path, and the
//! crash probability scales with the number of unwinds landing in the unsafe
//! window per unit of wall-clock. Concurrency multiplies that count without
//! multiplying the guard's lifetime, which is why sequential runs at the same
//! sampling rate survived.
//!
//! The fix lowers the sampling frequency for the query lanes only, from 997 Hz
//! to 199 Hz (`QUERY_SAMPLE_HZ`). This is the lever pprof's own documentation
//! makes available: the signal frequency directly sets how many unsafe unwinds
//! occur per second of on-CPU time, so cutting it about fivefold cuts the
//! exposure by the same factor, while 199 Hz (still twice pprof's default of
//! 99 Hz) keeps the flamegraph dense. The ingest lane keeps 997 Hz because its
//! measured region was sequential when the crash was observed.
//!
//! Two heavier options were considered and rejected for this task. Switching
//! pprof to its `frame-pointer` unwinder would sidestep libgcc entirely, but
//! pprof's README documents that unwinder as nightly-only, requiring
//! `cargo +nightly -Z build-std` to rebuild the standard library with correct
//! frame pointers, and even then "it's also not possible to ensure the safety
//! [...] the program will panic" on a bad frame pointer. That needs a
//! `.cargo/config` and toolchain change outside this crate, and pprof does not
//! document it as signal-safe, so it is not adopted here. Widening the blocklist
//! only suppresses samples whose interrupted instruction pointer already sits in
//! a blocklisted segment; it cannot suppress a fault while unwinding out of
//! application code, and the crash produced no Rust stack to point at new
//! libraries to add.
//!
//! When a flamegraph is needed for a workload that still faults under sampling,
//! `perf record --call-graph dwarf` on the host is the fallback: it unwinds out
//! of process and does not run inside the target's signal handler.

/// Environment variable naming the flamegraph SVG output path. When it is set
/// and the crate was built with the `profiling` feature, a bench core wraps its
/// measured region in a `pprof` CPU sampler and writes the flamegraph there. If
/// it is unset, no profiler is started even when the feature is compiled in.
pub const PROFILE_ENV: &str = "RAVEL_BENCH_PROFILE_SVG";

/// Refuse a profiled run that would execute each statement more than once
/// (issue #616). `pprof`'s signal sampler unwinds through libgcc's
/// async-signal-unsafe path on every `SIGPROF` tick, and the crash is
/// probabilistic in the number of unwinds; more runs of every statement raise
/// that count and the profiled process has been observed to segfault (see the
/// "Known instability under repeated execution" section of this module). One
/// guard around the whole measured region does not close the hazard, so rather
/// than segfault partway through a run this refuses the combination up front:
/// take the flamegraph from `--runs 1` (one execution already fills it) and the
/// latencies from a separate unprofiled multi-run pass.
///
/// Pure so the decision is testable without touching the process environment or
/// arming a real sampler: `profile_requested` is whether a sampler would run
/// (the `profiling` feature is on and [`PROFILE_ENV`] names a path), which
/// [`profile_requested`] resolves for the live path.
pub fn runs_supported_with_profiling(profile_requested: bool, runs: usize) -> Result<(), String> {
    if profile_requested && runs > 1 {
        return Err(format!(
            "--runs > 1 is not supported with {PROFILE_ENV}; the profiler and repeated execution \
             crash together, issue #616"
        ));
    }
    Ok(())
}

/// Whether a CPU sampler would actually run for this process: the `profiling`
/// feature is compiled in and [`PROFILE_ENV`] names a non-empty path. This is
/// the condition [`runs_supported_with_profiling`] gates on. With the feature
/// off no `pprof` guard is ever built, so the env var alone cannot crash a run
/// and this is always `false`.
#[cfg(feature = "profiling")]
pub fn profile_requested() -> bool {
    std::env::var_os(PROFILE_ENV).is_some_and(|v| !v.is_empty())
}

/// See the `profiling`-on variant. No sampler is linked in a default build, so
/// a set [`PROFILE_ENV`] cannot crash a repeated run and nothing is refused.
#[cfg(not(feature = "profiling"))]
pub fn profile_requested() -> bool {
    false
}

/// Sampling frequency in Hz for the ingest lane. 997 (a prime near 1 kHz)
/// avoids aliasing against any periodic timer in the workload.
#[cfg(feature = "profiling")]
const SAMPLE_HZ: std::os::raw::c_int = 997;

/// Sampling frequency in Hz for the query lanes (`sql_latency_bench`,
/// `query_latency_bench`). Deliberately lower than the ingest lane; see the
/// "Concurrent-query segfault" section of the module doc (issue #680). 199 is a
/// prime near 200 Hz and still twice pprof's own default frequency of 99 Hz, so
/// the flamegraph stays dense while the count of async-signal-unsafe unwinds per
/// second of on-CPU time drops about fivefold.
#[cfg(feature = "profiling")]
const QUERY_SAMPLE_HZ: std::os::raw::c_int = 199;

#[cfg(feature = "profiling")]
mod imp {
    use std::path::PathBuf;

    use super::{PROFILE_ENV, QUERY_SAMPLE_HZ, SAMPLE_HZ};

    /// The query lanes run their statements across many worker threads
    /// concurrently, and each `SIGPROF` tick unwinds through libgcc, which is
    /// not async-signal-safe, so more concurrent unwinds per second of
    /// wall-clock raise the odds of a fault (issue #680). Sample those lanes
    /// slower. The ingest lane, whose measured region was sequential when the
    /// crash was observed, keeps the higher rate.
    fn sample_hz_for(label: &str) -> std::os::raw::c_int {
        if label.contains("sql") || label.contains("query") {
            QUERY_SAMPLE_HZ
        } else {
            SAMPLE_HZ
        }
    }

    /// An active (or inert) profiling session. Created via
    /// [`ProfileSession::from_env`] or [`ProfileSession::to_path`]; call
    /// [`ProfileSession::finish`] once the measured region ends to write the
    /// SVG. Holds a `pprof::ProfilerGuard` only while sampling.
    pub struct ProfileSession {
        active: Option<Active>,
    }

    struct Active {
        guard: pprof::ProfilerGuard<'static>,
        path: PathBuf,
        label: String,
    }

    impl ProfileSession {
        /// Starts a session iff [`PROFILE_ENV`] names a path; otherwise returns
        /// an inert session whose `finish` does nothing.
        ///
        /// See issue #680: the sampler must survive a concurrent query plan.
        pub fn from_env(label: &str) -> Self {
            match std::env::var_os(PROFILE_ENV) {
                Some(p) if !p.is_empty() => Self::to_path(label, PathBuf::from(p)),
                _ => ProfileSession { active: None },
            }
        }

        /// Whether a sampler is currently running (a session that resolved a
        /// path and built a guard). Bench cores read this to switch off their
        /// own concurrent instrumentation while profiling: pprof samples every
        /// on-CPU thread for the life of the guard, so a poller or sampler
        /// running alongside the code under test lands in the same flamegraph.
        pub fn is_active(&self) -> bool {
            self.active.is_some()
        }

        /// Starts a session writing to an explicit path, ignoring the
        /// environment. Used by the crate's own test and by any caller that has
        /// already resolved an output path.
        pub fn to_path(label: &str, path: PathBuf) -> Self {
            let hz = sample_hz_for(label);
            match pprof::ProfilerGuardBuilder::default()
                .frequency(hz)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
            {
                Ok(guard) => {
                    eprintln!("profiling: sampling '{label}' at {hz} Hz");
                    ProfileSession {
                        active: Some(Active {
                            guard,
                            path,
                            label: label.to_string(),
                        }),
                    }
                }
                Err(err) => {
                    eprintln!("profiling: could not start sampler: {err}");
                    ProfileSession { active: None }
                }
            }
        }

        /// Stops sampling and writes the flamegraph SVG. Errors are reported to
        /// stderr and swallowed rather than propagated, so a profiling failure
        /// never discards the run's own report. Returns the written path when a
        /// session was active and the write succeeded.
        pub fn finish(self) -> Option<PathBuf> {
            let active = self.active?;
            let report = match active.guard.report().build() {
                Ok(report) => report,
                Err(err) => {
                    eprintln!("profiling: could not build report: {err}");
                    return None;
                }
            };
            let file = match std::fs::File::create(&active.path) {
                Ok(file) => file,
                Err(err) => {
                    eprintln!(
                        "profiling: could not create {}: {err}",
                        active.path.display()
                    );
                    return None;
                }
            };
            match report.flamegraph(file) {
                Ok(()) => {
                    eprintln!(
                        "profiling: wrote '{}' flamegraph to {}",
                        active.label,
                        active.path.display()
                    );
                    Some(active.path)
                }
                Err(err) => {
                    eprintln!("profiling: could not write flamegraph: {err}");
                    None
                }
            }
        }
    }
}

#[cfg(not(feature = "profiling"))]
mod imp {
    use std::path::PathBuf;

    use super::PROFILE_ENV;

    /// No-op stub used when the `profiling` feature is off, so the bench cores
    /// can call the profiling API unconditionally without linking `pprof`.
    pub struct ProfileSession;

    impl ProfileSession {
        pub fn from_env(_label: &str) -> Self {
            if std::env::var_os(PROFILE_ENV).is_some_and(|v| !v.is_empty()) {
                eprintln!(
                    "profiling: {PROFILE_ENV} is set but this binary was built without the \
                     `profiling` feature; no flamegraph will be written. Rebuild with \
                     `--features profiling`."
                );
            }
            ProfileSession
        }

        pub fn to_path(_label: &str, _path: PathBuf) -> Self {
            ProfileSession
        }

        pub fn is_active(&self) -> bool {
            false
        }

        pub fn finish(self) -> Option<PathBuf> {
            None
        }
    }
}

pub use imp::ProfileSession;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod manifest_tests {
    /// Guards that `pprof` cannot leak into a default build: it must be an
    /// `optional` dependency, and the only feature that activates it must be
    /// `profiling`. A mechanical read of this crate's own manifest, so it runs
    /// under the default `cargo test` (which does not link `pprof`) and needs
    /// no `cargo tree` subprocess. Equivalent to the `cargo tree -p
    /// ravel-bench` check the task calls for, but enforced in CI on every run.
    #[test]
    fn pprof_is_optional_and_only_behind_the_profiling_feature() {
        let manifest = include_str!("../Cargo.toml");

        let dep_line = manifest
            .lines()
            .find(|l| l.trim_start().starts_with("pprof ="))
            .expect("pprof dependency line present in Cargo.toml");
        assert!(
            dep_line.contains("optional = true"),
            "pprof must be an optional dependency so a default build never links it; got: {dep_line}"
        );

        // No feature other than `profiling` may activate pprof (as `dep:pprof`
        // or by bare name). Scan the [features] table line by line.
        let mut in_features = false;
        let mut current_feature = String::new();
        for line in manifest.lines() {
            let trimmed = line.trim();
            if trimmed == "[features]" {
                in_features = true;
                continue;
            }
            if in_features && trimmed.starts_with('[') {
                break; // left the [features] table
            }
            if !in_features {
                continue;
            }
            if let Some((name, _)) = trimmed.split_once('=') {
                let name = name.trim();
                if !name.is_empty() && !name.starts_with('#') {
                    current_feature = name.to_string();
                }
            }
            if trimmed.contains("pprof") && !trimmed.starts_with('#') {
                assert_eq!(
                    current_feature, "profiling",
                    "pprof may only be activated by the `profiling` feature, found under `{current_feature}`: {trimmed}"
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod refusal_tests {
    use super::{PROFILE_ENV, runs_supported_with_profiling};

    /// A profiled run (`RAVEL_BENCH_PROFILE_SVG` set, sampler linked) with
    /// `--runs > 1` is refused up front with the exact message, rather than
    /// segfaulting partway through the run (issue #616). Reverting the guard to
    /// an unconditional `Ok(())` makes the first assertion fail. The decision is
    /// pure so it needs no live sampler or process-env mutation.
    #[test]
    fn repeated_runs_refused_only_when_profiling() {
        // The offending combination: profiler on and more than one run.
        let err = runs_supported_with_profiling(true, 3)
            .expect_err("a profiled multi-run pass must be refused");
        assert_eq!(
            err,
            format!(
                "--runs > 1 is not supported with {PROFILE_ENV}; the profiler and repeated \
                 execution crash together, issue #616"
            )
        );

        // Either alone is fine: a single profiled run is the supported profiling
        // path, and repeated runs with the profiler off are stable.
        assert!(
            runs_supported_with_profiling(true, 1).is_ok(),
            "one profiled run is the supported flamegraph path"
        );
        assert!(
            runs_supported_with_profiling(false, 3).is_ok(),
            "repeated runs are stable when no sampler is armed"
        );
        // A boundary: exactly one run is never refused, profiler or not.
        assert!(runs_supported_with_profiling(false, 1).is_ok());
    }
}

#[cfg(all(test, feature = "profiling"))]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The lane produces a non-empty SVG flamegraph for a short CPU-bound run.
    /// Only compiled with the `profiling` feature, so the default `cargo test`
    /// neither runs it nor links `pprof`.
    #[test]
    fn profiling_lane_writes_non_empty_svg() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "ravel-bench-profiling-test-{}.svg",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let session = ProfileSession::to_path("unit-test", path.clone());
        // Busy-spin so the CPU sampler collects at least a few stacks. This has
        // to burn real on-CPU time; a sleep would accrue no ITIMER_PROF ticks.
        let mut acc: u64 = 0;
        for i in 0..50_000_000u64 {
            acc = acc.wrapping_add(i).wrapping_mul(2_654_435_761);
        }
        std::hint::black_box(acc);
        let written = session.finish();

        assert_eq!(written.as_deref(), Some(path.as_path()));
        let bytes = std::fs::read(&path).expect("read written flamegraph");
        assert!(!bytes.is_empty(), "flamegraph SVG must not be empty");
        let head = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
        assert!(
            head.contains("<svg") || head.contains("<?xml"),
            "written file should be an SVG document, got: {head}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
