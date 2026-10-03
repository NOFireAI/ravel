//! Optional CPU flamegraph lane for `load --parquet` (issue #365's pattern),
//! mirroring `crates/ravel-bench/src/profiling.rs`.
//!
//! This is measurement infrastructure with no production caller: the only
//! thing that starts a [`ProfileSession`] is a human running `ravel-cli load`
//! with the environment variable set. No production ingest path changes.
//!
//! The lane is gated behind the `profiling` cargo feature so a default build
//! never links `pprof`. When the feature is off this module is a no-op stub,
//! so `main.rs` can call it unconditionally.
//!
//! # Usage
//!
//! Set [`PROFILE_ENV`] to the SVG path you want written, then run `ravel-cli`
//! built with `--features profiling`. The sampler brackets only `load`'s
//! measured region, so the flamegraph reflects the load path rather than CLI
//! argument parsing or store construction.
//!
//! # The profiler perturbs the run it profiles
//!
//! Signal-based sampling (`pprof` arms `ITIMER_PROF`) is not free, so a
//! profiled load is measurably slower than an unprofiled one. Read the
//! flamegraph for CPU *attribution* only; quote load-time numbers from an
//! unprofiled run.

/// Environment variable naming the flamegraph SVG output path. When set and
/// `ravel-cli` was built with the `profiling` feature, `load --parquet` wraps
/// its measured region in a `pprof` CPU sampler and writes the flamegraph
/// there. Unset means no profiler starts even when the feature is compiled in.
pub const PROFILE_ENV: &str = "RAVEL_CLI_PROFILE_SVG";

/// Sampling frequency in Hz. 997 (a prime near 1 kHz), matching
/// `ravel-bench`'s choice, avoids aliasing against any periodic timer in the
/// load loop (e.g. the ack-deadline wait).
#[cfg(feature = "profiling")]
const SAMPLE_HZ: std::os::raw::c_int = 997;

#[cfg(feature = "profiling")]
mod imp {
    use std::path::PathBuf;

    use super::{PROFILE_ENV, SAMPLE_HZ};

    /// An active (or inert) profiling session. Created via
    /// [`ProfileSession::from_env`]; call [`ProfileSession::finish`] once the
    /// measured region ends to write the SVG. Holds a `pprof::ProfilerGuard`
    /// only while sampling.
    pub struct ProfileSession {
        active: Option<Active>,
    }

    struct Active {
        guard: pprof::ProfilerGuard<'static>,
        path: PathBuf,
        label: String,
    }

    impl ProfileSession {
        /// Starts a session iff [`PROFILE_ENV`] names a path; otherwise
        /// returns an inert session whose `finish` does nothing.
        pub fn from_env(label: &str) -> Self {
            match std::env::var_os(PROFILE_ENV) {
                Some(p) if !p.is_empty() => {
                    let path = PathBuf::from(p);
                    match pprof::ProfilerGuardBuilder::default()
                        .frequency(SAMPLE_HZ)
                        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                        .build()
                    {
                        Ok(guard) => {
                            eprintln!("profiling: sampling '{label}' at {SAMPLE_HZ} Hz");
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
                _ => ProfileSession { active: None },
            }
        }

        /// Stops sampling and writes the flamegraph SVG. Errors are reported
        /// to stderr and swallowed rather than propagated, so a profiling
        /// failure never discards the load's own report or error. Returns the
        /// written path when a session was active and the write succeeded.
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

    /// No-op stub used when the `profiling` feature is off, so `main.rs` can
    /// call the profiling API unconditionally without linking `pprof`.
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
    /// no `cargo tree` subprocess. Mirrors
    /// `crates/ravel-bench/src/profiling.rs`'s own guard.
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
