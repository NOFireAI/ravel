//! Smoke test for the `cost_regression_check` binary (ADR-0996 task 996-7):
//! the gate is proven reachable from the shipping bin a caller runs, over two
//! fixture reports, not only from the library unit tests.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::process::Command;

use ravel_bench::cost_regression::{CostReport, Figure, FigureClass, ProfileStamp};
use ravel_types::cost_profile::StoreCostProfile;

fn fig(name: &str, class: FigureClass, value: Option<f64>, unit: Option<&str>) -> Figure {
    Figure {
        name: name.to_string(),
        class,
        value,
        unit: unit.map(str::to_string),
    }
}

fn baseline_report() -> CostReport {
    CostReport {
        profile: ProfileStamp {
            requested: StoreCostProfile::reference(),
            effective: Some(StoreCostProfile::reference()),
        },
        effective_policy: None,
        figures: vec![
            fig("object_count", FigureClass::ObjectCount, Some(3469.0), None),
            fig(
                "write_class_requests",
                FigureClass::WriteClassRequests,
                Some(14500.0),
                None,
            ),
            fig("data_gets", FigureClass::DataGets, Some(149167.0), None),
            fig(
                "modeled_request_cost",
                FigureClass::ModeledRequestCost,
                Some(59667.0),
                None,
            ),
            fig("latency_p95", FigureClass::LatencyP95, Some(100.0), None),
            fig("latency_p50", FigureClass::LatencyP50, Some(10.0), None),
            fig(
                "bytes_read",
                FigureClass::Bytes,
                Some(1_000_000.0),
                Some("wire"),
            ),
        ],
    }
}

fn write_report(dir: &Path, name: &str, report: &CostReport) -> std::path::PathBuf {
    let path = dir.join(name);
    let json = serde_json::to_string_pretty(report).expect("serialize report");
    std::fs::write(&path, json).expect("write fixture");
    path
}

fn run(baseline: &Path, candidate: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cost_regression_check"))
        .arg(baseline)
        .arg(candidate)
        .output()
        .expect("run cost_regression_check")
}

fn run_with_bands(baseline: &Path, candidate: &Path, bands: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cost_regression_check"))
        .arg(baseline)
        .arg(candidate)
        .arg("--bands")
        .arg(bands)
        .output()
        .expect("run cost_regression_check --bands")
}

#[test]
fn identical_reports_exit_zero_and_list_every_figure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = write_report(dir.path(), "base.json", &baseline_report());
    let cand = write_report(dir.path(), "cand.json", &baseline_report());

    let out = run(&base, &cand);
    assert!(out.status.success(), "identical reports pass: {out:?}");
    let table = String::from_utf8(out.stdout).expect("utf8 table");
    // Deliverable 3: the table lists every compared figure, pass included.
    for figure in [
        "object_count",
        "write_class_requests",
        "data_gets",
        "modeled_request_cost",
        "latency_p95",
    ] {
        assert!(table.contains(figure), "table lists `{figure}`:\n{table}");
    }
}

#[test]
fn a_regressed_figure_exits_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = write_report(dir.path(), "base.json", &baseline_report());
    let mut regressed = baseline_report();
    regressed
        .figures
        .iter_mut()
        .find(|f| f.name == "data_gets")
        .expect("data_gets")
        .value = Some(149168.0); // exact band, +1 GET
    let cand = write_report(dir.path(), "cand.json", &regressed);

    let out = run(&base, &cand);
    assert_eq!(out.status.code(), Some(1), "a regression exits 1: {out:?}");
    let table = String::from_utf8(out.stdout).expect("utf8 table");
    assert!(
        table.contains("FAIL"),
        "the table marks the failing row:\n{table}"
    );
}

#[test]
fn differing_effective_profiles_exit_two_naming_both() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = write_report(dir.path(), "base.json", &baseline_report());
    let mut other_profile = StoreCostProfile::reference();
    other_profile.name = "gcs-multi-region-2026".to_string();
    let mut cand_report = baseline_report();
    cand_report.profile.effective = Some(other_profile);
    let cand = write_report(dir.path(), "cand.json", &cand_report);

    let out = run(&base, &cand);
    assert_eq!(out.status.code(), Some(2), "a refusal exits 2: {out:?}");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("s3-intra-region-2026"),
        "names the baseline profile: {stderr}"
    );
    assert!(
        stderr.contains("gcs-multi-region-2026"),
        "names the candidate profile: {stderr}"
    );
}

#[test]
fn a_legacy_report_missing_the_request_surface_exits_two() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = write_report(dir.path(), "base.json", &baseline_report());
    // A report with no data-GET figure: the legacy shape.
    let legacy = CostReport {
        profile: ProfileStamp {
            requested: StoreCostProfile::reference(),
            effective: Some(StoreCostProfile::reference()),
        },
        effective_policy: None,
        figures: vec![fig(
            "latency_p95",
            FigureClass::LatencyP95,
            Some(100.0),
            None,
        )],
    };
    let cand = write_report(dir.path(), "legacy.json", &legacy);

    let out = run(&base, &cand);
    assert_eq!(
        out.status.code(),
        Some(2),
        "a legacy report is a typed refusal, not a vacuous pass: {out:?}"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("request surface"),
        "names the missing surface: {stderr}"
    );
}

#[test]
fn a_malformed_report_exits_two_without_crashing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = write_report(dir.path(), "base.json", &baseline_report());
    let bad = dir.path().join("bad.json");
    std::fs::write(&bad, "{ not json").expect("write malformed fixture");

    let out = run(&base, &bad);
    assert_eq!(
        out.status.code(),
        Some(2),
        "a malformed report is a typed refusal: {out:?}"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("candidate: malformed report"),
        "names which report failed to parse and why: {stderr}"
    );
    // A refusal is not a regression verdict, so no table was printed.
    assert!(
        String::from_utf8(out.stdout)
            .expect("utf8 stdout")
            .is_empty(),
        "a refusal prints no comparison table"
    );
}

#[test]
fn the_checked_in_bands_toml_is_loadable_through_the_cli() {
    // The `--bands` surface is what makes a threshold configuration rather
    // than a constant, and the checked-in defaults document must be a file
    // the shipping binary actually accepts.
    let dir = tempfile::tempdir().expect("tempdir");
    let base = write_report(dir.path(), "base.json", &baseline_report());
    let mut raised = baseline_report();
    raised
        .figures
        .iter_mut()
        .find(|f| f.name == "data_gets")
        .expect("data_gets")
        .value = Some(149167.0 * 1.02); // +2%
    let cand = write_report(dir.path(), "cand.json", &raised);

    let defaults = dir.path().join("defaults.toml");
    std::fs::write(&defaults, include_str!("../cost_regression_bands.toml"))
        .expect("write the checked-in bands");
    let out = run_with_bands(&base, &cand, &defaults);
    assert_eq!(
        out.status.code(),
        Some(1),
        "the checked-in defaults keep data_gets exact, so +2% regresses: {out:?}"
    );

    // The same pair under a loosened band passes, proving the CLI reads the
    // document rather than ignoring it and falling back to the defaults.
    let loose = dir.path().join("loose.toml");
    std::fs::write(
        &loose,
        "[data_gets]\nkind = \"percent\"\nallowance = 10.0\n",
    )
    .expect("write the override");
    let out = run_with_bands(&base, &cand, &loose);
    assert!(
        out.status.success(),
        "a 10% data_gets band admits the same +2%: {out:?}"
    );
}

/// A shipping `BenchReport`, built from the crate's own report types and
/// serialized by the same serde surface `bench_report --out` writes, drives
/// the bin end to end under `--format bench-report`. This is the reachability
/// case: the gate reads what the bench actually emits, not only its own
/// fixture schema.
#[test]
fn bench_report_format_compares_a_real_report_end_to_end() {
    use ravel_bench::report::{
        BenchReport, BytesSection, Environment, IngestSection, LatencyReport, ModeledCost,
        QuerySection, RequestCounts, WorkloadShape,
    };

    let latency = |p50: f64, p95: f64| LatencyReport {
        p50,
        p95,
        p99: p95,
        max: p95,
        count: 10,
    };
    let requests = RequestCounts {
        backend_bills_requests: true,
        put: 11,
        get: 23,
        list: 7,
        put_attempts: Some(13),
        get_attempts: Some(29),
        list_attempts: Some(7),
        put_retry_overhead: Some(2),
        get_retry_overhead: Some(6),
        list_retry_overhead: Some(0),
    };
    let report = BenchReport {
        environment: Environment {
            store_backend: "memory".to_string(),
            region: "n/a-memory".to_string(),
            shard_count: 2,
            max_flush_delay_ms: 500,
            workload: WorkloadShape {
                target_series: 20,
                points_per_sec: 4_000,
                duration_secs: 1,
                batch_size: 50,
                query: "bench_gauge".to_string(),
                warm_query_count: 5,
            },
            git_commit: "0".repeat(40),
            toolchain: "rustc 0.0.0-test".to_string(),
            store_cost_profile_requested: StoreCostProfile::reference(),
            store_cost_profile_effective: Some(StoreCostProfile::reference()),
        },
        ingest: IngestSection {
            strict_ack_latency_ms: latency(10.0, 100.0),
            accepted_points: 4_000,
            accepted_points_per_sec: 4_000.0,
            write_amplification: 1.5,
        },
        query: QuerySection {
            cold_latency_ms: latency(20.0, 20.0),
            warm_latency_ms: latency(2.0, 4.0),
            matched_series: 20,
        },
        modeled_cost: ModeledCost::model(
            &StoreCostProfile::reference(),
            requests.put_class_attempts(),
            requests.get_attempts,
            48_000,
            48_000,
        ),
        s3_requests: requests,
        bytes: BytesSection {
            written: 96_000,
            read: 48_000,
        },
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bench.json");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&report).expect("serialize BenchReport"),
    )
    .expect("write BenchReport fixture");

    let out = Command::new(env!("CARGO_BIN_EXE_cost_regression_check"))
        .arg(&path)
        .arg(&path)
        .arg("--format")
        .arg("bench-report")
        .output()
        .expect("run cost_regression_check --format bench-report");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "an identical real BenchReport compares clean: {out:?}"
    );
    assert!(
        stdout.contains("source format:"),
        "the header names the source format: {stdout}"
    );
    assert!(
        stdout.contains("NOTE:"),
        "the header names the classes a BenchReport cannot enforce: {stdout}"
    );
}
