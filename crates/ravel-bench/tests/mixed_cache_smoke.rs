//! Smoke test for the mixed scan-and-interactive cache admission mode
//! (ADR-3072 track P5, issue #3084), behind the `sql-latency` feature. It
//! drives `run_mixed` end to end on a tiny generated corpus against a
//! `MemoryStore` and pins the report contract the bin exits on:
//!
//! - every required figure is present exactly once per arm run and class, in
//!   the JSON and as a column of the runs table, and both classes completed
//!   statements;
//! - every statement returned identical rows in every arm run;
//! - dropping any one figure from the serialized report fails the same check
//!   (the mutation the test exists to catch).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_bench::mixed_cache::{
    Arm, MixedConfig, REQUIRED_FIGURES, check_report, check_report_json, check_runs_table,
    render_report, render_runs_table, run_mixed,
};
use ravel_bench::sql_latency::ExecutorSettings;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;

/// Arm B's trigger: above the two-object foreground window's bytes and below
/// the 40-object corpus, so the scans read as bulk and the foreground stays
/// interactive. The test asserts both sides of that bracket on the report.
const ARM_B_MIN_BYTES: u64 = 32 * 1024;

fn tiny_config(store: Arc<dyn ObjectStoreBackend>, foreground: usize) -> MixedConfig {
    MixedConfig {
        store,
        store_backend: "memory".to_string(),
        records: 2_000,
        records_per_object: 50,
        extra_attrs: 1,
        body_bytes: 32,
        // Above the two-object foreground window, below the corpus, so the
        // scans stream through it while the window fits.
        cache_bytes: 96 * 1024,
        cache_bulk_share: 0.5,
        duration: Duration::from_millis(1500),
        background: 1,
        foreground,
        arms: vec![
            Arm {
                label: "A".to_string(),
                bulk_scan_min_bytes: None,
            },
            Arm {
                label: "B".to_string(),
                bulk_scan_min_bytes: Some(ARM_B_MIN_BYTES),
            },
        ],
        repetitions: 1,
        window_objects: 2,
        background_ids: Vec::new(),
        deadline: Duration::from_secs(60),
        settings: ExecutorSettings {
            fetch_concurrency: 2,
            ..ExecutorSettings::default()
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_mixed_mode_reports_every_figure_once_and_both_classes_complete_statements() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = run_mixed(&tiny_config(store, 1)).await.expect("mixed run");

    // The contract the bin exits on, on the complete report.
    check_report(&report).expect("every figure present, both classes completed statements");
    let table = render_runs_table(&report);
    check_runs_table(&table, &report).expect("table carries each figure once per run and class");
    assert_eq!(report.runs.len(), 2, "one run per arm");
    for run in &report.runs {
        assert!(
            run.foreground.statements_completed > 0,
            "{} foreground",
            run.arm
        );
        assert!(
            run.background.statements_completed > 0,
            "{} background",
            run.arm
        );
        // Every statement of both classes ran at least once and read through
        // the cache: the figures come from stats.cache, so a run with no
        // cache attached cannot get here.
        for s in &run.statements {
            assert!(s.completed > 0, "{} {}", run.arm, s.id);
            assert!(
                s.digest_stable,
                "{} {} rows changed between executions",
                run.arm, s.id
            );
        }
        assert!(run.background.object_store_gets + run.background.cache_hits > 0);
    }
    // The trigger brackets the two read sets: the whole corpus resolves for
    // the scans, above it, and the two-object window below it. So arm B reads
    // every scan as a bulk scan and the foreground stays interactive; unset,
    // only the predicate-free aggregate is a bulk scan.
    assert_eq!(report.dataset.object_count, 40);
    assert!(
        report.dataset.stored_bytes >= ARM_B_MIN_BYTES,
        "corpus {} bytes is under the trigger",
        report.dataset.stored_bytes
    );
    assert!(
        report.interactive_window.resolved_bytes < ARM_B_MIN_BYTES,
        "window {} bytes reaches the trigger",
        report.interactive_window.resolved_bytes
    );
    assert!(report.interactive_window.resolved_bytes < report.provenance.cache_bytes);
    let b = report.runs.iter().find(|r| r.arm == "B").unwrap();
    assert_eq!(
        b.background.intents.interactive, 0,
        "arm B scans are all bulk"
    );
    assert!(b.background.intents.bulk_scan > 0);
    let a = report.runs.iter().find(|r| r.arm == "A").unwrap();
    assert!(
        a.background.intents.interactive > 0,
        "arm A filtered scans stay interactive"
    );
    assert_eq!(
        a.foreground.intents.bulk_scan, 0,
        "foreground stays interactive"
    );
    assert_eq!(
        b.foreground.intents.bulk_scan, 0,
        "foreground stays interactive"
    );

    // Identical rows per statement across the arms.
    assert!(
        report.results.identical,
        "differing: {:?}",
        report.results.differing
    );
    let bands = &report.bands;
    assert!(
        bands
            .iter()
            .any(|b| b.id == "results_identical" && b.verdict == "HIT")
    );
    assert!(
        bands
            .iter()
            .any(|b| b.id == "mixed_foreground_served_share" && b.arm == "B")
    );
    assert_eq!(report.medians.len(), 2);

    // The full table renders every band row and the results line.
    let rendered = render_report(&report);
    assert!(rendered.contains("results_identical"));
    assert!(rendered.contains("identical rows per statement"));

    // Mutation: drop one figure from a class of the serialized report and the
    // check the bin exits on fails, naming the figure. The JSON is what a
    // consumer reads, so the check runs over it, not over the struct.
    for figure in REQUIRED_FIGURES {
        let mut json = serde_json::to_value(&report).unwrap();
        let class = json["runs"][0]["foreground"].as_object_mut().unwrap();
        assert!(class.remove(figure).is_some(), "{figure} is in the report");
        let err = check_report_json(&json).expect_err("a dropped figure fails the check");
        assert!(err.contains(figure), "{err} names {figure}");
    }
    // And a class that completed nothing fails it too.
    let mut json = serde_json::to_value(&report).unwrap();
    json["runs"][1]["background"]["statements_completed"] = serde_json::json!(0);
    let err = check_report_json(&json).expect_err("zero completed fails the check");
    assert!(err.contains("completed zero statements"), "{err}");
}

/// The scans-only shape (no foreground connections) reports the scans-only
/// band rows and skips the empty class.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_scans_only_shape_reports_the_scans_only_band() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = run_mixed(&tiny_config(store, 0))
        .await
        .expect("scans-only run");
    check_report(&report).expect("the empty foreground class is skipped");
    for run in &report.runs {
        assert_eq!(run.foreground.connections, 0);
        assert_eq!(run.foreground.statements_completed, 0);
        assert!(run.background.statements_completed > 0);
    }
    assert!(
        report
            .bands
            .iter()
            .any(|b| b.id == "scans_only_background_served_share" && b.arm == "B")
    );
    assert!(!report.bands.iter().any(|b| b.id.starts_with("mixed_")));
    let table = render_runs_table(&report);
    assert!(
        !table.contains("foreground"),
        "no row for a class with no connections"
    );
    check_runs_table(&table, &report).expect("one background row per run");
}

/// A cache of zero bytes is refused before anything is generated.
#[tokio::test]
async fn a_zero_cache_is_refused() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut cfg = tiny_config(store, 1);
    cfg.cache_bytes = 0;
    let err = run_mixed(&cfg).await.expect_err("no cache is refused");
    assert!(err.to_string().contains("--cache-bytes"), "{err}");
}
