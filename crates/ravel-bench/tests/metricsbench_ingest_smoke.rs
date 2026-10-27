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

/// The `ci` profile's scrape interval in milliseconds, the grid every anchor
/// sits on.
fn ci_interval_ms() -> i64 {
    let secs = ci_workload()
        .profile("ci")
        .expect("manifest declares a `ci` profile")
        .scrape_interval_secs;
    secs as i64 * 1_000
}

/// The fixed anchor every smoke run replays from (2025-10-09T08:53:30Z), so a
/// run does not depend on the wall clock.
const ANCHOR_MS: i64 = 1_760_000_010_000;

/// [`ANCHOR_MS`], checked to sit on the `ci` profile's scrape grid.
fn on_grid_anchor() -> i64 {
    assert_eq!(ANCHOR_MS.rem_euclid(ci_interval_ms()), 0, "anchor off grid");
    ANCHOR_MS
}

/// Run the bin over the `ci` profile against an in-process `MemoryStore`, a few
/// steps, no comparator endpoints, and return the JSON report it prints.
fn run_lane() -> serde_json::Value {
    // One step, and a batch large enough to flush the whole step in a couple
    // of strict writes: strict mode flushes durably per batch, so a small batch
    // over the `ci` profile's series count is many sequential flushes. This
    // keeps the reachability smoke fast.
    run_lane_with_steps(1, on_grid_anchor())
}

/// Run the bin over the `ci` profile, one step, with `extra` arguments in
/// place of the anchor, and return its exit code and stderr.
fn run_refused(extra: &[&str]) -> (Option<i32>, String) {
    let bin = env!("CARGO_BIN_EXE_metricsbench_ingest");
    let output = Command::new(bin)
        .args(["--profile", "ci", "--store", "memory", "--steps", "1"])
        .args(extra)
        .output()
        .expect("spawn metricsbench_ingest");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
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
    assert!(
        report["profile"]["run"].get("anchor_source").is_none(),
        "the anchor is always the flag's, so there is no source to record"
    );
    let systems = report["systems"].as_array().expect("systems array");

    // With no comparator endpoints supplied, exactly the Ravel row is present.
    assert_eq!(
        systems.len(),
        1,
        "only the in-process Ravel row without endpoints"
    );
    let ravel = &systems[0];
    assert_eq!(ravel["system"], "ravel-in-process");
    assert_eq!(
        ravel["endpoint"], "in-process",
        "the in-process row is never read as the portable Ravel row"
    );
    assert!(
        ravel.get("last_commit_token").is_none(),
        "only the portable Ravel row records a last commit token"
    );
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
        .find(|q| q["system"] == "ravel-in-process")
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

    // A fixed on-grid anchor, so every run replays the same stream and the
    // test can regenerate it.
    let anchor_ms = on_grid_anchor();
    let (_, gen_report) = Generator::new(&workload, "ci", anchor_ms)
        .expect("generator builds")
        .generate_bytes(STEPS)
        .expect("generates");

    let report = run_lane_with_steps(STEPS, anchor_ms);
    let profile = &report["profile"];

    assert_eq!(profile["run"]["anchor_ms"].as_i64(), Some(anchor_ms));
    assert_eq!(
        profile["run"]["stream_digest"].as_str(),
        Some(gen_report.digest.as_str()),
        "the recorded stream digest is the regenerated stream's"
    );
    assert_eq!(
        profile["run"]["workload_version"].as_u64(),
        Some(u64::from(workload.version))
    );
    assert_eq!(profile["run"]["seed"].as_u64(), Some(workload.seed));
    let manifest_bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/metrics/workload.json"
    ))
    .expect("read the manifest");
    assert_eq!(
        profile["run"]["workload_digest"].as_str(),
        Some(blake3::hash(&manifest_bytes).to_hex().as_str()),
        "workload_digest is BLAKE3 over the manifest file's bytes"
    );
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

    // Every row's stream counts are the generator's per-type figures, and
    // every type is offered: stale markers as samples, native histograms as
    // their own class, both accepted through the in-process `IngestPoint`.
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
    let ingest = &ravel["ingest"];
    assert_eq!(
        ingest["offered_samples"].as_u64(),
        Some(gen_report.float_samples + gen_report.stale_markers),
        "the replay offers every scalar and every stale marker as a sample"
    );
    assert_eq!(
        ingest["accepted_samples"].as_u64(),
        ingest["offered_samples"].as_u64()
    );
    assert_eq!(
        ingest["offered_histograms"].as_u64(),
        Some(gen_report.native_histogram_samples),
        "native histograms are their own offered class"
    );
    assert_eq!(
        ingest["accepted_histograms"].as_u64(),
        Some(gen_report.native_histogram_samples),
        "the in-process path accepts every native histogram"
    );
    assert_eq!(ingest["excluded_native_histogram"].as_u64(), Some(0));
    assert_eq!(ingest["excluded_stale_marker"].as_u64(), Some(0));
    assert!(
        gen_report.native_histogram_samples > 0 && gen_report.stale_markers > 0,
        "the ci stream carries both kinds at {STEPS} steps"
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

/// The anchor is a run parameter: omitted, the bin exits 64 naming the flag
/// and why no clock default exists; off the scrape grid, it exits 64 showing
/// the remainder. Both refuse before anything is generated.
#[test]
fn a_missing_or_off_grid_anchor_exits_64() {
    let _serial = serial();
    let (code, stderr) = run_refused(&[]);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(stderr.contains("--anchor-ms is required"), "{stderr}");
    assert!(
        stderr.contains(
            "an anchor read from the clock would make each run replay a different stream"
        ),
        "{stderr}"
    );

    let off_grid = (on_grid_anchor() + 1).to_string();
    let (code, stderr) = run_refused(&["--anchor-ms", &off_grid]);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(
        stderr.contains(&format!("{off_grid} mod {} = 1", ci_interval_ms())),
        "{stderr}"
    );

    let (code, stderr) = run_refused(&["--anchor-ms", &(i64::MAX - 1).to_string()]);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(stderr.contains("9223372036854 ms"), "{stderr}");
}

/// The portable Ravel row's token comes from the environment variable the
/// flag names: an unset variable, or the flag without `--ravel-endpoint`,
/// exits 64 before anything is generated.
#[test]
fn a_bearer_token_env_that_cannot_be_read_exits_64() {
    let _serial = serial();
    let anchor = on_grid_anchor().to_string();
    let var = "METRICSBENCH_SMOKE_TOKEN_NEVER_SET";
    let (code, stderr) = run_refused(&[
        "--anchor-ms",
        &anchor,
        "--ravel-endpoint",
        "http://127.0.0.1:9/api/v1/write",
        "--ravel-bearer-token-env",
        var,
    ]);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(stderr.contains(var), "{stderr}");

    let (code, stderr) = run_refused(&["--anchor-ms", &anchor, "--ravel-bearer-token-env", var]);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(stderr.contains("needs --ravel-endpoint"), "{stderr}");
}

/// The portable Ravel row records its tenant: `--ravel-endpoint` without
/// `--ravel-tenant`, or a `--ravel-tenant` other than `--tenant`, exits 64
/// before anything is generated.
#[test]
fn a_missing_or_mismatched_ravel_tenant_exits_64() {
    let _serial = serial();
    let anchor = on_grid_anchor().to_string();
    let endpoint = ["--ravel-endpoint", "http://127.0.0.1:9/api/v1/write"];
    let query = ["--ravel-query-url", "http://127.0.0.1:9"];

    let mut missing = vec!["--anchor-ms", anchor.as_str()];
    missing.extend_from_slice(&endpoint);
    missing.extend_from_slice(&query);
    let (code, stderr) = run_refused(&missing);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(stderr.contains("needs --ravel-tenant"), "{stderr}");

    let mut differs = missing.clone();
    differs.extend_from_slice(&["--tenant", "a", "--ravel-tenant", "b"]);
    let (code, stderr) = run_refused(&differs);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(
        stderr.contains("--ravel-tenant `b` differs from --tenant `a`"),
        "{stderr}"
    );
}

/// The same anchor replays the same stream and offers the same tuples; a
/// different anchor changes both digests.
#[test]
fn the_anchor_alone_determines_the_stream_and_offered_digests() {
    let _serial = serial();
    let anchor = on_grid_anchor();
    let digests = |report: &serde_json::Value| {
        (
            report["profile"]["run"]["stream_digest"]
                .as_str()
                .expect("stream_digest")
                .to_string(),
            report["systems"][0]["ingest"]["offered_digest"]
                .as_str()
                .expect("offered_digest")
                .to_string(),
        )
    };
    let first = digests(&run_lane_with_steps(1, anchor));
    let second = digests(&run_lane_with_steps(1, anchor));
    assert_eq!(first, second, "same anchor, same stream and offered tuples");
    assert!(!first.1.is_empty());

    let other = digests(&run_lane_with_steps(1, anchor - ci_interval_ms()));
    assert_ne!(other.0, first.0, "a different anchor is a different stream");
    assert_ne!(other.1, first.1, "and offers different tuples");
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
    assert_eq!(
        offered,
        counts.scalar + counts.stale_marker + counts.native_histogram
    );
    peak
}

/// The negative control: the whole stream materialised, bytes then every
/// scalar sample, as the pre-streaming lane held it. Scalars alone already
/// grow with history, which is all the control has to show.
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

/// One point a [`StubTsdb`] stored: a float's bits, or a native histogram.
#[derive(Clone, Copy)]
enum StubPoint {
    Float(u64),
    Histogram,
}

/// Every series a stub receiver stored, keyed by its sorted label pairs
/// (`__name__` included), each holding `(ts_ms, point)`.
type StubTsdb =
    std::sync::Arc<Mutex<std::collections::BTreeMap<Vec<(String, String)>, Vec<(i64, StubPoint)>>>>;

/// A stub Remote Write 1.0 receiver with just enough of the Prometheus query
/// API for the lane's read-back: it stores what it is sent and answers
/// `/api/v1/series` over a `__name__=~` alternation, `count_over_time` over an
/// exact selector, and an instant exact selector honouring stale markers.
fn stub_receiver(db: StubTsdb) -> axum::Router {
    use ravel_remote_write::proto::prometheus::WriteRequest;

    let write_db = db.clone();
    let series_db = db.clone();
    axum::Router::new()
        .route(
            "/api/v1/write",
            axum::routing::post(move |body: axum::body::Bytes| {
                let db = write_db.clone();
                async move {
                    let raw = snap::raw::Decoder::new()
                        .decompress_vec(&body)
                        .expect("snappy body");
                    let request = <WriteRequest as prost::Message>::decode(raw.as_slice())
                        .expect("WriteRequest");
                    let mut db = db.lock().unwrap();
                    for ts in request.timeseries {
                        let mut labels: Vec<(String, String)> =
                            ts.labels.into_iter().map(|l| (l.name, l.value)).collect();
                        labels.sort();
                        let points = db.entry(labels).or_default();
                        for s in ts.samples {
                            points.push((s.timestamp, StubPoint::Float(s.value.to_bits())));
                        }
                        for h in ts.histograms {
                            points.push((h.timestamp, StubPoint::Histogram));
                        }
                    }
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        )
        .route(
            "/api/v1/series",
            axum::routing::get(
                move |axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>| {
                    let db = series_db.clone();
                    async move {
                        let param = |k: &str| {
                            params
                                .iter()
                                .find(|(name, _)| name == k)
                                .map(|(_, v)| v.clone())
                                .expect("series parameter")
                        };
                        let names = name_alternation(&param("match[]"));
                        let start = seconds_to_ms(&param("start"));
                        let end = seconds_to_ms(&param("end"));
                        let db = db.lock().unwrap();
                        let data: Vec<serde_json::Value> = db
                            .iter()
                            .filter(|(labels, points)| {
                                labels
                                    .iter()
                                    .any(|(k, v)| k == "__name__" && names.contains(v))
                                    && points.iter().any(|(ts, _)| (start..=end).contains(ts))
                            })
                            .map(|(labels, _)| {
                                serde_json::Value::Object(
                                    labels
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone().into()))
                                        .collect(),
                                )
                            })
                            .collect();
                        axum::Json(serde_json::json!({"status": "success", "data": data}))
                    }
                },
            ),
        )
        .route(
            "/api/v1/query",
            axum::routing::get(
                move |axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>| {
                    let db = db.clone();
                    async move {
                        let param = |k: &str| {
                            params
                                .iter()
                                .find(|(name, _)| name == k)
                                .map(|(_, v)| v.clone())
                                .expect("query parameter")
                        };
                        let query = param("query");
                        let time = param("time");
                        let at = seconds_to_ms(&time);
                        let db = db.lock().unwrap();
                        let result = if let Some(inner) = query
                            .strip_prefix("count_over_time(")
                            .and_then(|q| q.strip_suffix("ms])"))
                        {
                            let (selector, range) = inner.rsplit_once('[').expect("range");
                            let range: i64 = range.parse().expect("range ms");
                            let count = db.get(&exact_selector(selector)).map_or(0, |points| {
                                points
                                    .iter()
                                    .filter(|(ts, p)| {
                                        *ts > at - range
                                            && *ts <= at
                                            && !matches!(p, StubPoint::Float(b) if *b == STALE_MARKER_BITS)
                                    })
                                    .count()
                            });
                            if count == 0 {
                                Vec::new()
                            } else {
                                vec![serde_json::json!({"metric": {}, "value": [time, count.to_string()]})]
                            }
                        } else {
                            let latest = db.get(&exact_selector(&query)).and_then(|points| {
                                points
                                    .iter()
                                    .filter(|(ts, _)| *ts <= at && *ts > at - 300_000)
                                    .max_by_key(|(ts, _)| *ts)
                                    .copied()
                            });
                            match latest {
                                Some((_, StubPoint::Float(b))) if b != STALE_MARKER_BITS => {
                                    vec![serde_json::json!({"metric": {}, "value": [time, "1"]})]
                                }
                                Some((_, StubPoint::Histogram)) => {
                                    vec![serde_json::json!({"metric": {}, "value": [time, "1"]})]
                                }
                                _ => Vec::new(),
                            }
                        };
                        axum::Json(serde_json::json!({
                            "status": "success",
                            "data": {"resultType": "vector", "result": result},
                        }))
                    }
                },
            ),
        )
}

use ravel_promql_difftest::generator::STALE_MARKER_BITS;

/// The API's decimal seconds as milliseconds.
fn seconds_to_ms(text: &str) -> i64 {
    let (whole, frac) = text.split_once('.').unwrap_or((text, "0"));
    let ms: i64 = format!("{frac:0<3}")[..3].parse().expect("fraction");
    let whole: i64 = whole.parse().expect("seconds");
    if text.starts_with('-') {
        whole * 1_000 - ms
    } else {
        whole * 1_000 + ms
    }
}

/// Undo PromQL string escaping.
fn unescape_promql(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some(other) => out.push(other),
                None => {}
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The metric names of a `{__name__=~"a|b"}` matcher of escaped literals.
fn name_alternation(matcher: &str) -> Vec<String> {
    let regex = matcher
        .strip_prefix("{__name__=~\"")
        .and_then(|m| m.strip_suffix("\"}"))
        .expect("a __name__ alternation");
    let regex = unescape_promql(regex);
    let mut names = vec![String::new()];
    let mut chars = regex.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => names.last_mut().expect("name").extend(chars.next()),
            '|' => names.push(String::new()),
            other => names.last_mut().expect("name").push(other),
        }
    }
    names
}

/// The sorted label pairs of an exact `{k="v",...}` selector.
fn exact_selector(selector: &str) -> Vec<(String, String)> {
    let body = selector
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .expect("a braced selector");
    let mut labels = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let (name, after) = rest.split_once("=\"").expect("an exact matcher");
        let mut value = String::new();
        let mut chars = after.char_indices();
        let mut end = after.len();
        while let Some((i, c)) = chars.next() {
            match c {
                '\\' => value.extend(chars.next().map(|(_, c)| c)),
                '"' => {
                    end = i + 1;
                    break;
                }
                other => value.push(other),
            }
        }
        labels.push((name.to_string(), value));
        rest = after[end..].strip_prefix(',').unwrap_or(&after[end..]);
    }
    labels.sort();
    labels
}

/// `--no-in-process-row` with two stub portable receivers: the run opens no
/// store, the report carries no `ravel-in-process` row, no query phase and no
/// substrate, says `in_process_row: "skipped"`, and still closes band 6 on
/// each portable row, whose expected digests agree. The in-process run's
/// expected digests equal theirs, so the portable rows replayed the stream
/// the in-process row would have.
#[test]
fn without_the_in_process_row_the_portable_rows_still_close_band_6() {
    let _serial = serial();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let mut urls = Vec::new();
    for _ in 0..2 {
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind stub");
        let addr = listener.local_addr().expect("addr");
        let app = stub_receiver(StubTsdb::default());
        runtime.spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        urls.push(format!("http://{addr}"));
    }
    let anchor = on_grid_anchor().to_string();
    let prometheus_write = format!("{}/api/v1/write", urls[0]);
    let mimir_write = format!("{}/api/v1/write", urls[1]);
    let bin = env!("CARGO_BIN_EXE_metricsbench_ingest");
    let output = Command::new(bin)
        .args([
            "--profile",
            "ci",
            "--steps",
            "2",
            "--anchor-ms",
            &anchor,
            "--no-in-process-row",
            "--store",
            "s3",
            "--http-batch-size",
            "4096",
            "--prometheus-endpoint",
            &prometheus_write,
            "--prometheus-query-url",
            &urls[0],
            "--mimir-endpoint",
            &mimir_write,
            "--mimir-query-url",
            &urls[1],
        ])
        // An S3 store on a port nothing listens on: a run that opened and
        // wrote to it would fail.
        .env("RAVEL_S3_ENDPOINT", "http://127.0.0.1:9")
        .env("RAVEL_S3_ALLOW_HTTP", "true")
        .env("RAVEL_S3_BUCKET", "unreachable")
        .env("RAVEL_S3_REGION", "us-east-1")
        .env("RAVEL_S3_ACCESS_KEY_ID", "unused")
        .env("RAVEL_S3_SECRET_ACCESS_KEY", "unused")
        .output()
        .expect("spawn metricsbench_ingest");
    assert!(
        output.status.success(),
        "status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is a JSON report");
    assert_eq!(report["in_process_row"], "skipped");
    assert!(report.get("substrate").is_none(), "no store was opened");
    assert!(report.get("queries").is_none(), "no in-process query phase");
    let systems = report["systems"].as_array().expect("systems");
    let names: Vec<&str> = systems
        .iter()
        .map(|r| r["system"].as_str().expect("system"))
        .collect();
    assert_eq!(names, ["prometheus", "mimir"]);
    for row in systems {
        assert_eq!(
            row["ingest"]["expected_digests"], row["ingest"]["accepted_digests"],
            "band 6 on {}",
            row["system"]
        );
        assert_eq!(row["readback"]["matches_expected"], true, "{row}");
    }
    assert_eq!(
        systems[0]["ingest"]["expected_digests"],
        systems[1]["ingest"]["expected_digests"]
    );
    assert!(
        report["profile"]["run"]["stream_digest"].is_string(),
        "the profile record is built from the first portable row's stream"
    );

    let in_process = run_lane_with_steps(2, on_grid_anchor());
    assert_eq!(in_process["in_process_row"], "present");
    assert_eq!(
        in_process["systems"][0]["ingest"]["expected_digests"],
        systems[0]["ingest"]["expected_digests"]
    );
    assert_eq!(
        in_process["profile"]["run"]["stream_digest"],
        report["profile"]["run"]["stream_digest"]
    );
}

/// `--no-in-process-row` with no portable endpoint would measure nothing, so
/// it exits 64 before anything is generated.
#[test]
fn skipping_the_in_process_row_with_no_endpoint_exits_64() {
    let _serial = serial();
    let anchor = on_grid_anchor().to_string();
    let (code, stderr) = run_refused(&["--anchor-ms", &anchor, "--no-in-process-row"]);
    assert_eq!(code, Some(64), "{stderr}");
    assert!(
        stderr.contains("--no-in-process-row needs at least one portable endpoint"),
        "{stderr}"
    );
}
