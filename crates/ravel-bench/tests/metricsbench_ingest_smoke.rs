//! Reachability smoke test for the Remote Write 1.0 ingest lane (ADR-0927,
//! issue #937, M5): the shipping `metricsbench_ingest` binary replays the
//! MetricsBench stream into the in-process Ravel path and prints a valid report
//! whose Ravel row is durable-on-ack with the sample accounting closed. Drives
//! the real binary as a subprocess, so it proves the lane is reachable from the
//! bin a caller runs, not only from the lib unit tests.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::process::Command;
use std::sync::Mutex;

use ravel_bench::metrics_gen::Generator;
use ravel_bench::metrics_ingest::{
    GeneratedStream, LogicalSample, SampleBatches, StreamRecord, classify_line,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// The allocator counts every thread, so each test holds this for its whole
/// body and a measurement never sees another test's allocations.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn ci_workload() -> ravel_bench::metrics_workload::WorkloadFile {
    ravel_bench::metrics_workload::load_workload(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/metrics/workload.json"
    )))
    .expect("load the same workload manifest the binary loads")
}

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
/// assumption that `steps` covers the profile's full declared step count),
/// with the first step at `anchor_ms`.
fn run_lane_with_steps(steps: u64, anchor_ms: i64) -> serde_json::Value {
    let bin = env!("CARGO_BIN_EXE_metricsbench_ingest");
    let output = Command::new(bin)
        .args([
            "--profile",
            "ci",
            "--store",
            "memory",
            "--steps",
            &steps.to_string(),
            "--anchor-ms",
            &anchor_ms.to_string(),
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
    let _serial = serial();
    let report = run_lane();
    assert_eq!(
        report["profile"]["run"]["anchor_source"], "clock",
        "without --anchor-ms the anchor is read from the clock, and the report says so"
    );
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
    let _serial = serial();
    let workload = ci_workload();
    let expected = workload
        .profile("ci")
        .expect("manifest declares a `ci` profile");
    let expected_label_cardinalities = workload.label_cardinalities();

    // A deliberate truncation: far under the profile's declared
    // `samples_per_series` (120), so the run finishes fast regardless of host
    // load.
    const STEPS: u64 = 5;
    let generator = ravel_bench::metrics_gen::Generator::new(&workload, "ci", 0)
        .expect("generator builds for the ci profile");
    let expected_total_series_created = generator.total_series_created(STEPS);

    // A now-based anchor, so the strict write lands in a plausible ingest
    // window, passed explicitly so the test can regenerate the same stream.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64;
    let anchor_ms = now_ms - STEPS as i64 * expected.scrape_interval_secs as i64 * 1_000;
    let (_, gen_report) = Generator::new(&workload, "ci", anchor_ms)
        .expect("generator builds")
        .generate_bytes(STEPS)
        .expect("generates");

    let report = run_lane_with_steps(STEPS, anchor_ms);
    let profile = &report["profile"];

    assert_eq!(profile["run"]["anchor_source"], "explicit");
    assert_eq!(profile["run"]["anchor_ms"].as_i64(), Some(anchor_ms));
    assert_eq!(
        report["systems"][0]["ingest"]["elapsed_basis"], "timed_segments",
        "the artifact says what elapsed_secs measures, so a reader can tell it from a \
         wall-time report written before the streaming replay"
    );
    assert_eq!(
        profile["run"]["total_samples_generated"].as_u64(),
        Some(gen_report.emitted_samples),
        "the bin generated the stream this test regenerated from the same anchor"
    );

    // Every row's stream counts are the generator's per-type figures, and the
    // scalar accounting names the two excluded kinds.
    let ravel = &report["systems"][0];
    let counts = &ravel["ingest"]["stream_counts"];
    assert_eq!(
        counts["generated_lines"].as_u64(),
        Some(gen_report.emitted_samples)
    );
    assert_eq!(counts["scalar"].as_u64(), Some(gen_report.float_samples));
    assert_eq!(
        counts["native_histogram"].as_u64(),
        Some(gen_report.native_histogram_samples)
    );
    assert_eq!(
        counts["stale_marker"].as_u64(),
        Some(gen_report.stale_markers)
    );
    assert_eq!(counts["malformed"].as_u64(), Some(0));
    assert_eq!(
        ravel["ingest"]["offered_samples"].as_u64(),
        Some(gen_report.float_samples),
        "the replay offers exactly the scalar lines"
    );
    let excluded_native = ravel["ingest"]["excluded_native_histogram"]
        .as_u64()
        .expect("excluded_native_histogram");
    let excluded_stale = ravel["ingest"]["excluded_stale_marker"]
        .as_u64()
        .expect("excluded_stale_marker");
    assert_eq!(excluded_native, gen_report.native_histogram_samples);
    assert_eq!(excluded_stale, gen_report.stale_markers);
    assert!(
        excluded_native > 0 && excluded_stale > 0,
        "the ci stream carries both excluded kinds at {STEPS} steps"
    );

    // This run truncates the profile (STEPS < samples_per_series, asserted
    // below), so this only exercises the truncation branch of
    // `build_profile_record`, not the `ci` profile's own non-comparable
    // verdict -- a full, untruncated `ci` run is covered separately by
    // `metrics_ingest::tests::profile_record_is_non_comparable_for_the_profiles_own_reason_when_not_truncated`.
    assert_eq!(
        profile["comparable"].as_bool(),
        Some(false),
        "a truncated run is never comparable, regardless of the profile's own verdict"
    );
    let reason = profile["comparability_reason"]
        .as_str()
        .expect("comparability_reason is a string");
    assert!(
        !reason.is_empty(),
        "a non-comparable profile must state why, not leave the reason blank"
    );
    assert!(
        reason.contains("steps"),
        "a truncated run's reason must state the truncation, not the profile's own verdict: {reason}"
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
    assert!(
        !label_cardinalities.contains_key(&workload.generator.scaling_label),
        "the scaling label is not a fixed dimension and must not appear in label_cardinalities"
    );

    let scaling_label = &profile["scaling_label"];
    assert_eq!(
        scaling_label["name"].as_str(),
        Some(workload.generator.scaling_label.as_str())
    );
    assert_eq!(
        scaling_label["cardinality_declared"].as_u64(),
        Some(generator.scaling_label_cardinality(expected.samples_per_series)),
        "cardinality_declared must match the generator's own figure at the declared step count"
    );
    assert_eq!(
        run["scaling_label_cardinality"].as_u64(),
        Some(generator.scaling_label_cardinality(STEPS)),
        "run.scaling_label_cardinality must match the generator's own figure at steps_run"
    );

    // The substrate block: an in-process MemoryStore never bills.
    let substrate = &report["substrate"];
    assert_eq!(substrate["store_backend"].as_str(), Some("memory"));
    assert_eq!(substrate["backend_bills_requests"].as_bool(), Some(false));
}

/// The replay batch size these tests pass explicitly (`run_lane` and
/// `run_lane_with_steps` hand it to `--batch-size`). The bin's own default
/// is 500; a larger batch is a looser bound, so the ratio assertions below
/// hold at the default too.
const MEMORY_BATCH: usize = 4096;

/// Bytes the global allocator holds now that it did not hold when `region`
/// was opened. `stats_alloc` already books a realloc's growth or shrink under
/// `bytes_allocated` or `bytes_deallocated`, so `bytes_reallocated` is not
/// added again.
fn live_bytes(region: &Region<'_, System>) -> i64 {
    let change = region.change();
    change.bytes_allocated as i64 - change.bytes_deallocated as i64
}

/// Peak live bytes while draining a `ci` stream of `steps` scrapes batch by
/// batch, sampled with each batch held, the way a replay holds it.
fn streaming_peak(steps: u64) -> i64 {
    let workload = ci_workload();
    let region = Region::new(GLOBAL);
    let mut stream = GeneratedStream::new(
        Generator::new(&workload, "ci", 1_700_000_000_000).expect("generator builds"),
        steps,
    );
    let mut peak = live_bytes(&region);
    let mut offered = 0u64;
    while let Some(batch) = stream.next_batch(MEMORY_BATCH).expect("generates") {
        peak = peak.max(live_bytes(&region));
        offered += batch.len() as u64;
    }
    let (_, counts) = stream.finish().expect("drained stream closes");
    assert_eq!(offered, counts.scalar);
    peak
}

/// The negative control: the whole stream materialised, bytes then every
/// scalar sample, as the pre-streaming lane held it.
fn materialised_live(steps: u64) -> i64 {
    let workload = ci_workload();
    let region = Region::new(GLOBAL);
    let (bytes, _) = Generator::new(&workload, "ci", 1_700_000_000_000)
        .expect("generator builds")
        .generate_bytes(steps)
        .expect("generates");
    let samples: Vec<LogicalSample> = std::str::from_utf8(&bytes)
        .expect("utf8")
        .lines()
        .enumerate()
        .filter_map(|(i, line)| match classify_line(i as u64 + 1, line) {
            StreamRecord::Scalar(sample) => Some(sample),
            _ => None,
        })
        .collect();
    let live = live_bytes(&region);
    drop((bytes, samples));
    live
}

/// Required test (iii): streaming replay memory is bounded by batch size and
/// step width, not by history length. Four times the steps leaves the
/// streaming peak within 1.5x, while the materialised stream grows past 3x,
/// which shows the measurement can see history-proportional growth.
#[test]
fn streaming_replay_memory_does_not_grow_with_history_length() {
    let _serial = serial();
    let (short, long) = (120, 480);
    let stream_short = streaming_peak(short);
    let stream_long = streaming_peak(long);
    let held_short = materialised_live(short);
    let held_long = materialised_live(long);
    let streaming_ratio = stream_long as f64 / stream_short as f64;
    let materialised_ratio = held_long as f64 / held_short as f64;
    eprintln!(
        "streaming peak {stream_short} B at {short} steps, {stream_long} B at {long} \
         (ratio {streaming_ratio:.3}); materialised {held_short} B, {held_long} B \
         (ratio {materialised_ratio:.3})"
    );
    assert!(stream_short > 0 && held_short > 0);
    assert!(
        streaming_ratio <= 1.5,
        "streaming peak grew {streaming_ratio:.3}x for 4x the steps"
    );
    assert!(
        materialised_ratio > 3.0,
        "the materialised control grew only {materialised_ratio:.3}x for 4x the steps"
    );
}
