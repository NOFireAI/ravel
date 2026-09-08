//! Reachability smoke test for the Remote Write 1.0 ingest lane (ADR-0927,
//! issue #937, M5): the shipping `metricsbench_ingest` binary replays the
//! MetricsBench stream into the in-process Ravel path and prints a valid report
//! whose Ravel row is durable-on-ack with the sample accounting closed. Drives
//! the real binary as a subprocess, so it proves the lane is reachable from the
//! bin a caller runs, not only from the lib unit tests.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::Command;

/// Run the bin over the `ci` profile against an in-process `MemoryStore`, a few
/// steps, no comparator endpoints, and return the JSON report it prints.
fn run_lane() -> serde_json::Value {
    let bin = env!("CARGO_BIN_EXE_metricsbench_ingest");
    let output = Command::new(bin)
        // One step, and a batch large enough to flush the whole step in a
        // couple of strict writes: strict mode flushes durably per batch, so a
        // small batch over the `ci` profile's series count is many sequential
        // flushes. This keeps the reachability smoke fast.
        .args([
            "--profile",
            "ci",
            "--store",
            "memory",
            "--steps",
            "1",
            "--shards",
            "2",
            "--batch-size",
            "4096",
        ])
        .output()
        .expect("spawn metricsbench_ingest");
    assert!(
        output.status.success(),
        "metricsbench_ingest exited non-zero: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout is a JSON report")
}

/// Run the bin over the `ci` profile for exactly `steps` scrapes (no
/// assumption that `steps` covers the profile's full declared step count).
fn run_lane_with_steps(steps: u64) -> serde_json::Value {
    let bin = env!("CARGO_BIN_EXE_metricsbench_ingest");
    let output = Command::new(bin)
        .args([
            "--profile",
            "ci",
            "--store",
            "memory",
            "--steps",
            &steps.to_string(),
            "--shards",
            "2",
            "--batch-size",
            "4096",
        ])
        .output()
        .expect("spawn metricsbench_ingest");
    assert!(
        output.status.success(),
        "metricsbench_ingest exited non-zero: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout is a JSON report")
}

#[test]
fn the_ingest_lane_is_reachable_from_the_bin_and_ravel_is_durable_on_ack() {
    let report = run_lane();
    let systems = report["systems"].as_array().expect("systems array");

    // With no comparator endpoints supplied, exactly the Ravel row is present.
    assert_eq!(
        systems.len(),
        1,
        "only the in-process Ravel row without endpoints"
    );
    let ravel = &systems[0];
    assert_eq!(ravel["system"], "ravel");
    assert_eq!(
        ravel["ack_semantics"], "durable_on_ack",
        "Ravel's strict Remote Write surface is durable-on-ack"
    );

    // The Ravel row carries its commit tokens (a strict ack mints them) and the
    // diagnostic storage accounting the in-process path can see.
    assert!(
        ravel["commit_tokens"]
            .as_array()
            .is_some_and(|t| !t.is_empty()),
        "the durable-on-ack row records at least one commit token"
    );
    assert!(
        ravel["storage"].is_object(),
        "the Ravel row carries storage accounting"
    );

    // The accounting closes and something was actually ingested.
    let ing = &ravel["ingest"];
    let accepted = ing["accepted_samples"].as_u64().expect("accepted");
    let rejected = ing["rejected_samples"].as_u64().expect("rejected");
    let dropped = ing["dropped_samples"].as_u64().expect("dropped");
    let offered = ing["offered_samples"].as_u64().expect("offered");
    assert!(accepted > 0, "the run ingested a non-zero sample count");
    assert_eq!(
        accepted + rejected + dropped,
        offered,
        "the sample accounting must close (ADR-0927 band 4)"
    );

    // FIX 1 (issue #937 review finding 1): the binary runs the post-ingest
    // read-your-write query phase and records it as its own row, separate from
    // ingest (ADR-0927 decision 9). The row is token-bound: it carries the
    // min_commit_token set it read against, and matches the series just written
    // without sleeping past the flush delay (decision 3).
    //
    // TO SEE THIS FAIL against the pre-fix binary: drop the `.with_query(query)`
    // in `metricsbench_ingest`'s `run`; `queries` is then absent and the
    // assertions below fail.
    let queries = report["queries"].as_array().expect("queries array present");
    let ravel_query = queries
        .iter()
        .find(|q| q["system"] == "ravel")
        .expect("a ravel query-phase row exists");
    assert!(
        ravel_query["min_commit_tokens"]
            .as_array()
            .is_some_and(|t| !t.is_empty()),
        "the query phase is token-bound: it carries the min_commit_token set"
    );
    assert!(
        ravel_query["matched_series"]
            .as_u64()
            .is_some_and(|m| m > 0),
        "the token-bound read-your-write query matched the just-written series"
    );
    assert!(
        ravel_query["eval_ts_ms"].as_i64().is_some(),
        "the query records the instant it evaluated at (the replay's newest sample)"
    );
}

/// ADR-0927 decision 11: the artifact carries a top-level `profile` block
/// naming the `ci` profile as non-comparable, with its reason, the
/// pre-registered declared figures, and (under `run`) the generator-exact
/// figures for the steps this run actually generated -- not a set of numbers
/// a reader could mistake for a comparable result.
///
/// The `ci` profile's full declared run (120 steps, `samples_per_series`)
/// measures ~64s unoptimized on this host, over the 60s this test must stay
/// under, so this drives the bin over a short, deliberately truncated
/// `--steps` count instead and asserts `run`'s figures against the
/// generator's own [`ravel_bench::metrics_gen::Generator::total_series_created`]
/// for that exact step count -- never a re-typed formula -- while the
/// declared figures are still asserted against the manifest.
///
/// TO SEE THIS FAIL against the pre-fix binary: drop `.with_profile(profile_record)`
/// in `metricsbench_ingest`'s `run`; the top-level `profile` key is then absent
/// and the first assertion below fails.
#[test]
fn ci_profile_artifact_is_marked_non_comparable_and_carries_the_profile_figures() {
    let workload = ravel_bench::metrics_workload::load_workload(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/metrics/workload.json"
    )))
    .expect("load the same workload manifest the binary loads");
    let expected = workload
        .profile("ci")
        .expect("manifest declares a `ci` profile");
    let expected_label_cardinalities = workload.label_cardinalities(expected);

    // A deliberate truncation: far under the profile's declared
    // `samples_per_series` (120), so the run finishes fast regardless of host
    // load.
    const STEPS: u64 = 5;
    let generator = ravel_bench::metrics_gen::Generator::new(&workload, "ci", 0)
        .expect("generator builds for the ci profile");
    let expected_total_series_created = generator.total_series_created(STEPS);

    let report = run_lane_with_steps(STEPS);
    let profile = &report["profile"];

    assert_eq!(
        profile["comparable"].as_bool(),
        Some(false),
        "the `ci` profile is never comparable (ADR-0927 decision 11)"
    );
    let reason = profile["comparability_reason"]
        .as_str()
        .expect("comparability_reason is a string");
    assert!(
        !reason.is_empty(),
        "a non-comparable profile must state why, not leave the reason blank"
    );
    assert_eq!(profile["name"].as_str(), Some("ci"));
    assert_eq!(
        profile["active_series"].as_u64(),
        Some(expected.active_series)
    );
    assert_eq!(
        profile["samples_per_series"].as_u64(),
        Some(expected.samples_per_series)
    );
    assert_eq!(
        profile["scrape_interval_secs"].as_u64(),
        Some(expected.scrape_interval_secs)
    );
    assert_eq!(
        profile["duration_secs"].as_u64(),
        Some(expected.duration_secs)
    );
    assert_eq!(
        profile["total_samples"].as_u64(),
        Some(expected.total_samples)
    );
    assert_eq!(
        profile["churn_basis_points_per_hour"].as_u64(),
        Some(expected.churn_basis_points_per_hour)
    );
    // A deliberately truncated run: steps_run is this test's short STEPS, not
    // the profile's full declared step count.
    assert_eq!(
        profile["steps_declared"].as_u64(),
        Some(expected.samples_per_series)
    );
    assert_eq!(profile["steps_run"].as_u64(), Some(STEPS));
    assert!(
        STEPS < expected.samples_per_series,
        "STEPS must actually truncate the profile for this test to exercise that path"
    );
    let run = &profile["run"];
    assert_eq!(
        run["steps"].as_u64(),
        profile["steps_run"].as_u64(),
        "run.steps must name the same basis as steps_run"
    );
    assert_eq!(
        run["total_series_created"].as_u64(),
        Some(expected_total_series_created),
        "total series created must match the generator's own count for this run's steps"
    );
    assert!(
        run["logical_input_bytes"].as_u64().is_some_and(|b| b > 0),
        "the generator produced a non-empty logical input stream"
    );
    assert!(
        run["total_samples_generated"]
            .as_u64()
            .is_some_and(|s| s > 0),
        "the generator emitted a non-empty sample count"
    );
    let label_cardinalities = profile["label_cardinalities"]
        .as_object()
        .expect("label_cardinalities is present");
    assert_eq!(
        label_cardinalities.len(),
        expected_label_cardinalities.len(),
        "every label dimension the manifest declares must be reported"
    );
    for (name, count) in &expected_label_cardinalities {
        assert_eq!(
            label_cardinalities.get(name).and_then(|v| v.as_u64()),
            Some(*count),
            "label dimension `{name}`'s distinct-value count must match the manifest"
        );
    }

    // The substrate block: an in-process MemoryStore never bills.
    let substrate = &report["substrate"];
    assert_eq!(substrate["store_backend"].as_str(), Some("memory"));
    assert_eq!(substrate["backend_bills_requests"].as_bool(), Some(false));
}
