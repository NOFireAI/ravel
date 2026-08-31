//! Benchmark harness for Ravel. Report-only: this crate never changes library
//! behavior, it only measures it.

pub mod bench_env;
pub mod codecs;
#[cfg(feature = "parquet-baseline")]
pub mod columnar_load;
pub mod concurrent;
pub mod distrib_crossover;
pub mod e2e;
pub mod generator;
#[cfg(feature = "sql-latency")]
pub mod groupby_scaling;
pub mod harness;
pub mod ingest;
#[cfg(feature = "sql-latency")]
pub mod logs_scan_scaling;
pub mod metrics_gen;
pub mod metrics_workload;
pub mod profiling;
pub mod promql_corpus;
pub mod pushdown_crossover;
pub mod query_latency;
#[cfg(feature = "parquet-baseline")]
pub mod read_accounting;
pub mod report;
pub mod report_schema;
pub mod section_accounting;
pub mod segment_support;
#[cfg(feature = "sql-latency")]
pub mod spans_scan;
#[cfg(feature = "sql-latency")]
pub mod sql_corpus;
#[cfg(feature = "sql-latency")]
pub mod sql_latency;
pub mod value_shapes;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    /// Builds the shipping `ingest_bench` binary with the SAME feature set as
    /// this test and returns the exact path cargo reports for it.
    ///
    /// An integration test would get `CARGO_BIN_EXE_ingest_bench`, but this is a
    /// lib unit test (its path must be `ravel_bench::tests::...`), which does
    /// not. Deriving the path from the test executable's own directory instead
    /// (the previous approach) resolves whatever `ingest_bench` binary already
    /// sits in `target/<profile>/` -- and `cargo test --lib` does not rebuild
    /// the bin at all, so a binary left there by an earlier `--features
    /// stage-timing` build lingers and gets spawned by a default-feature run of
    /// this test. Its `stage_breakdown` field then makes the feature-off
    /// assertion below fail against a binary whose feature set never matched
    /// this test's (issue #925). Building the bin here, forwarding this test's
    /// own `stage-timing` state, binds the spawned binary's feature set to the
    /// assertions that check it.
    fn ingest_bench_bin() -> PathBuf {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut cmd = Command::new(cargo);
        cmd.args([
            "build",
            "--quiet",
            "-p",
            "ravel-bench",
            "--bin",
            "ingest_bench",
            "--message-format=json-render-diagnostics",
        ]);
        if cfg!(feature = "stage-timing") {
            cmd.args(["--features", "stage-timing"]);
        }
        let output = cmd.output().expect("spawn cargo build ingest_bench");
        assert!(
            output.status.success(),
            "cargo build ingest_bench failed: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        // The JSON artifact stream carries one `compiler-artifact` message per
        // built target; the `ingest_bench` bin's `executable` field is its path.
        serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<serde_json::Value>()
            .filter_map(Result::ok)
            .find_map(|msg| {
                if msg.get("reason").and_then(|v| v.as_str()) != Some("compiler-artifact") {
                    return None;
                }
                if msg
                    .get("target")
                    .and_then(|t| t.get("name"))
                    .and_then(|n| n.as_str())
                    != Some("ingest_bench")
                {
                    return None;
                }
                msg.get("executable")
                    .and_then(|e| e.as_str())
                    .map(PathBuf::from)
            })
            .expect("cargo build reported no ingest_bench executable path")
    }

    /// Runs the real `ingest_bench` binary against an in-process `MemoryStore`
    /// and returns the JSON report it prints. The bin prints the pretty JSON
    /// document first and a human table after it, so this reads the first JSON
    /// value off stdout and ignores the trailing text.
    fn run_ingest_bench() -> serde_json::Value {
        let output = Command::new(ingest_bench_bin())
            .args([
                "--store",
                "memory",
                "--shards",
                "2",
                "--target-series",
                "40",
                "--points-per-sec",
                "4000",
                "--duration-secs",
                "1",
                "--batch-size",
                "40",
                "--ack-timeout-secs",
                "20",
            ])
            .output()
            .expect("spawn ingest_bench");
        assert!(
            output.status.success(),
            "ingest_bench exited non-zero: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<serde_json::Value>()
            .next()
            .expect("some JSON on stdout")
            .expect("valid JSON report")
    }

    #[cfg(feature = "stage-timing")]
    fn total_ns(group: &serde_json::Value, stage: &str) -> u64 {
        group
            .get(stage)
            .and_then(|s| s.get("total_ns"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| panic!("stage {stage} missing total_ns"))
    }

    /// Acceptance test (ADR-0104 decision 2b, epic #365 T3): the SHIPPING
    /// `ingest_bench` binary emits the per-stage breakdown end to end when built
    /// with the `stage-timing` feature, and emits valid JSON lacking the field
    /// when built without it. Drives the real binary as a subprocess, not an
    /// in-process stand-in, so it proves the breakdown is reachable from the
    /// binary a feature-lane build actually ships, not merely from library code.
    #[test]
    fn ingest_bench_emits_stage_breakdown_in_feature_lane() {
        let report = run_ingest_bench();
        assert!(report.is_object(), "report must be a JSON object");
        // Base fields are present regardless of the feature: the breakdown is
        // additive, it does not replace the report.
        assert!(
            report.get("accepted_points").is_some(),
            "base report field accepted_points must be present"
        );
        assert!(
            report.get("config").is_some(),
            "base report field config must be present"
        );

        #[cfg(feature = "stage-timing")]
        {
            use std::collections::BTreeSet;

            let breakdown = report
                .get("stage_breakdown")
                .expect("stage_breakdown present in the feature build");

            // Harness-timed stages: the key set is EXACTLY {decode, normalize}.
            // A distinct group from the seam stages, so a harness measurement
            // can never be misread as a seam-reported one.
            let harness = breakdown
                .get("harness_stages")
                .expect("harness_stages present")
                .as_object()
                .expect("harness_stages is an object");
            let harness_keys: BTreeSet<&str> = harness.keys().map(String::as_str).collect();
            assert_eq!(
                harness_keys,
                BTreeSet::from(["decode", "normalize"]),
                "harness stage set must be exactly {{decode, normalize}}"
            );

            // Seam-reported stages: the key set is EXACTLY {admit, route, merge,
            // encode}. Pinned both ways: every wired stage present, nothing
            // extra.
            let seam = breakdown
                .get("seam_stages")
                .expect("seam_stages present")
                .as_object()
                .expect("seam_stages is an object");
            let seam_keys: BTreeSet<&str> = seam.keys().map(String::as_str).collect();
            assert_eq!(
                seam_keys,
                BTreeSet::from(["admit", "route", "merge", "encode"]),
                "seam stage set must be exactly {{admit, route, merge, encode}}"
            );

            // Every stage recorded a nonzero duration. A missing or unreached
            // stage shows as a zero here and this names it, unlike a
            // non-empty-map check that passes with most stages silently
            // unreported.
            let harness_val = breakdown.get("harness_stages").expect("harness_stages");
            for stage in ["decode", "normalize"] {
                assert!(
                    total_ns(harness_val, stage) > 0,
                    "harness stage {stage} recorded a zero duration"
                );
            }
            let seam_val = breakdown.get("seam_stages").expect("seam_stages");
            let seam_ns: Vec<(&str, u64)> = ["admit", "route", "merge", "encode"]
                .into_iter()
                .map(|s| (s, total_ns(seam_val, s)))
                .collect();
            for (stage, ns) in &seam_ns {
                assert!(
                    *ns > 0,
                    "seam stage {stage} recorded a zero duration; it is unwired or unreached"
                );
            }

            // Each seam stage is measured independently over its own boundary,
            // so the four nanosecond totals are pairwise distinct. Misattributing
            // one stage as another (emitting route's duration under the admit
            // key) leaves the key set and count valid but makes two totals
            // identical; this catches exactly that, which an exact-key-set check
            // alone cannot.
            for i in 0..seam_ns.len() {
                for j in (i + 1)..seam_ns.len() {
                    assert_ne!(
                        seam_ns[i].1, seam_ns[j].1,
                        "seam stages {} and {} report identical total_ns ({}); a stage is misattributed",
                        seam_ns[i].0, seam_ns[j].0, seam_ns[i].1
                    );
                }
            }
        }

        #[cfg(not(feature = "stage-timing"))]
        {
            assert!(
                report.get("stage_breakdown").is_none(),
                "feature-off build must not emit stage_breakdown; the field is optional for \
                 consumers and absent without the feature"
            );
        }
    }
}
