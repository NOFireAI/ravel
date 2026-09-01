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
