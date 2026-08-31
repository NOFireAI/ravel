//! Reachability tests for the reconciled MetricsBench report schema (ADR-0927,
//! issue #936): a malformed artifact must fail through the shipping
//! `metricsbench_report` binary, not only through a direct
//! `report_schema::validate` call. The unit tests in `report_schema` prove the
//! validator; these prove the binary that runs it is the fail-closed gate a
//! consumer actually hits.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::PathBuf;
use std::process::{Command, Output};

use ravel_bench::promql_corpus::CostClass;
use ravel_bench::report_schema::{
    Backend, Comparator, Figure, Hardware, Measurement, MetricsBenchReport, Provenance,
    ResultStatus, SCHEMA_VERSION,
};

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_metricsbench_report"))
}

fn valid_report() -> MetricsBenchReport {
    MetricsBenchReport {
        provenance: Provenance {
            schema_version: SCHEMA_VERSION,
            ravel_git_commit: "9fc85f421590d360e7979ee167eb38e166b45462".to_string(),
            toolchain: "rustc 1.90.0".to_string(),
            protocol: "remote_write_1.0".to_string(),
            hardware: Hardware {
                os: "Linux 6.8.0 x86_64".to_string(),
                cpu_model: "AMD EPYC 7R13".to_string(),
                logical_cores: 8,
                instance_type: None,
            },
            backend: Backend {
                store_backend: "s3".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "n/a".to_string(),
                backend_bills_requests: true,
            },
            comparators: vec![Comparator {
                name: "prometheus".to_string(),
                version: "3.13.1".to_string(),
                image_digest:
                    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_string(),
            }],
            generator_digest: "blake3:1111".to_string(),
            corpus_digest: "blake3:2222".to_string(),
            config: vec![],
            allocator: ravel_bench::allocator::ALLOCATOR_SYSTEM.to_string(),
        },
        measurements: vec![Measurement {
            id: "mb_fanout_total_rate".to_string(),
            class: CostClass::HighFanOut,
            status: ResultStatus::Ok,
            figures: vec![
                Figure {
                    name: "min_ms".to_string(),
                    value: 10.0,
                },
                Figure {
                    name: "median_ms".to_string(),
                    value: 12.5,
                },
                Figure {
                    name: "max_ms".to_string(),
                    value: 20.0,
                },
            ],
        }],
        geomean_ms: Some(12.5),
    }
}

/// Write a report artifact and run `metricsbench_report render` over it.
fn render_report(dir: &tempfile::TempDir, name: &str, report: &MetricsBenchReport) -> Output {
    let path = dir.path().join(name);
    std::fs::write(
        &path,
        serde_json::to_string_pretty(report).expect("serialize"),
    )
    .expect("write");
    Command::new(bin())
        .args(["render", "--report", path.to_str().expect("utf8 path")])
        .output()
        .expect("spawn metricsbench_report")
}

#[test]
fn a_valid_report_renders_through_the_binary_with_the_retry_caveat() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = render_report(&dir, "valid.json", &valid_report());
    assert!(
        out.status.success(),
        "a valid report must render: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    // The table is derived from the artifact.
    assert!(stdout.contains("mb_fanout_total_rate"), "{stdout}");
    assert!(stdout.contains("high_fan_out"), "{stdout}");
    assert!(
        stdout.contains("12.500"),
        "the median figure is rendered: {stdout}"
    );
    // The retry caveat is in the rendered output, not only in a doc.
    assert!(
        stdout.contains("logical-call counts") && stdout.contains("#928"),
        "the retry caveat must be rendered: {stdout}"
    );
    // A gap with no pre-registered band is a loud SKIP.
    assert!(
        stdout.contains("SKIP"),
        "an unregistered band must render a loud SKIP: {stdout}"
    );
}

#[test]
fn a_report_missing_a_provenance_field_fails_through_the_binary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut report = valid_report();
    report.provenance.ravel_git_commit = String::new();
    let out = render_report(&dir, "missing_field.json", &report);
    assert!(
        !out.status.success(),
        "a blank git commit must fail through the binary"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("ravel_git_commit"),
        "the binary must name the missing field: {stderr}"
    );
}

#[test]
fn a_non_finite_figure_fails_through_the_binary() {
    // serde_json cannot represent a non-finite number: it serializes NaN as
    // `null` and rejects an out-of-range literal like `1e999` at parse time. So
    // a non-finite figure can never reach the validator through a parsed
    // artifact -- the parser is itself fail-closed against it, which is the
    // property this proves through the binary. (The validator's own
    // `NonFiniteFigure` path, reachable when a producer builds the struct
    // in-process before serializing, is pinned by the `report_schema` unit
    // test.)
    let dir = tempfile::tempdir().expect("tempdir");
    let mut report = valid_report();
    report.geomean_ms = None;
    // A distinctive sentinel so the textual swap below hits exactly the median
    // figure and nothing else.
    report.measurements[0].figures[1].value = 424_242.0;
    let json = serde_json::to_string_pretty(&report)
        .expect("serialize")
        .replace("424242.0", "1e999");
    let path = dir.path().join("non_finite.json");
    std::fs::write(&path, json).expect("write");
    let out = Command::new(bin())
        .args(["render", "--report", path.to_str().expect("utf8 path")])
        .output()
        .expect("spawn");
    assert!(
        !out.status.success(),
        "a non-finite figure must fail through the binary"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("not a valid MetricsBench report"),
        "the binary must reject the out-of-range figure at parse: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn an_unparseable_artifact_fails_through_the_binary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("garbage.json");
    std::fs::write(&path, "{ this is not json ]").expect("write");
    let out = Command::new(bin())
        .args(["render", "--report", path.to_str().expect("utf8 path")])
        .output()
        .expect("spawn");
    assert!(!out.status.success(), "an unparseable artifact must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("not a valid MetricsBench report"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_provenance_subcommand_stamps_a_block_a_report_accepts() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Any two files: the subcommand digests their contents.
    let workload = dir.path().join("workload.json");
    let corpus = dir.path().join("corpus.json");
    std::fs::write(&workload, r#"{"version":1}"#).expect("write");
    std::fs::write(&corpus, r#"{"version":1,"entries":[]}"#).expect("write");
    let out = Command::new(bin())
        .args([
            "provenance",
            "--workload",
            workload.to_str().expect("utf8 path"),
            "--corpus",
            corpus.to_str().expect("utf8 path"),
            "--store-backend",
            "s3",
            "--region",
            "us-east-1",
            "--bills-requests",
            "--config",
            "max_flush_delay_ms=2000",
        ])
        .output()
        .expect("spawn");
    assert!(
        out.status.success(),
        "provenance must gather and self-validate: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let p: Provenance = serde_json::from_slice(&out.stdout).expect("emitted provenance is JSON");
    assert_eq!(p.schema_version, SCHEMA_VERSION);
    assert_eq!(p.backend.store_backend, "s3");
    assert!(p.backend.backend_bills_requests);
    assert!(p.generator_digest.starts_with("blake3:"));
    assert!(p.corpus_digest.starts_with("blake3:"));
    assert_eq!(p.config.len(), 1);
    assert_eq!(p.config[0].key, "max_flush_delay_ms");
    // A different corpus content is a different digest, so the stamp is not
    // ignoring its inputs.
    std::fs::write(&corpus, r#"{"version":1,"entries":[1]}"#).expect("rewrite");
    let out2 = Command::new(bin())
        .args([
            "provenance",
            "--workload",
            workload.to_str().expect("utf8 path"),
            "--corpus",
            corpus.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("spawn");
    let p2: Provenance = serde_json::from_slice(&out2.stdout).expect("JSON");
    assert_ne!(p.corpus_digest, p2.corpus_digest);
}
