//! Gate tests for the checked-in MetricsBench artifacts (ADR-0927, issue #933)
//! under `benchmarks/metrics/`, plus the reachability proof the task requires:
//! a corrupt artifact fails through the shipping `metricsbench_gen` binary, not
//! only through a direct `gate_corpus` call.
//!
//! Everything here is an exact expected value. Where a figure would move with
//! an encoding change it is bounded against something known (per profile, per
//! family), never against a flat floor.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

use ravel_bench::metrics_gen::Generator;
use ravel_bench::metrics_workload::{
    Comparability, FamilyKind, NON_COMPARABLE_PROFILES, REQUIRED_PROFILES, load_workload,
};
use ravel_bench::promql_corpus::{
    CostClass, EvalKind, class_counts, known_construct_names, load_corpus,
};

/// The checked-in artifacts, relative to this crate's manifest dir.
fn workload_path() -> PathBuf {
    PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/metrics/workload.json"
    ))
}

fn corpus_path() -> PathBuf {
    PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/metrics/metrics.corpus.json"
    ))
}

/// The corpus holds three queries in each of the eight ADR-0927 cost classes.
const ENTRIES_PER_CLASS: usize = 3;
/// Eight classes at three each.
const CORPUS_SIZE: usize = 24;

#[test]
fn the_checked_in_artifacts_load_and_pass_their_gates() {
    let workload = load_workload(workload_path()).expect("the workload manifest loads and gates");
    let entries = load_corpus(corpus_path()).expect("the query corpus loads and gates");
    assert_eq!(workload.version, 1);
    assert_eq!(workload.seed, 927_000_933);
    assert_eq!(entries.len(), CORPUS_SIZE);
}

#[test]
fn the_manifest_declares_exactly_the_four_adr_0927_profiles_with_their_exact_figures() {
    let workload = load_workload(workload_path()).expect("manifest loads");
    let names: Vec<&str> = workload.profiles.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, REQUIRED_PROFILES);

    // ADR-0927 decision 11's table, restated as exact values. Each row:
    // (name, active series, samples/series, scrape, duration, churn bp/h,
    // total samples).
    let expected: &[(&str, u64, u64, u64, u64, u64, u64)] = &[
        ("cardinality", 1_000_000, 360, 15, 5_400, 0, 360_000_000),
        ("history", 10_000, 172_800, 15, 2_592_000, 0, 1_728_000_000),
        ("churn", 50_000, 8_640, 15, 129_600, 2_000, 432_000_000),
        ("ci", 1_000, 120, 15, 1_800, 0, 120_000),
    ];
    for (name, active, samples, scrape, duration, churn_bp, total) in expected {
        let p = workload.profile(name).expect("profile present");
        assert_eq!(p.active_series, *active, "{name} active_series");
        assert_eq!(p.samples_per_series, *samples, "{name} samples_per_series");
        assert_eq!(p.scrape_interval_secs, *scrape, "{name} scrape_interval");
        assert_eq!(p.duration_secs, *duration, "{name} duration");
        assert_eq!(
            p.churn_basis_points_per_hour, *churn_bp,
            "{name} churn rate"
        );
        assert_eq!(p.total_samples, *total, "{name} total_samples");
    }
}

#[test]
fn only_the_ci_profile_is_marked_non_comparable_and_it_states_why() {
    let workload = load_workload(workload_path()).expect("manifest loads");
    for profile in &workload.profiles {
        let must_refuse = NON_COMPARABLE_PROFILES.contains(&profile.name.as_str());
        assert_eq!(
            profile.is_publishable(),
            !must_refuse,
            "profile `{}` publishability disagrees with ADR-0927 decision 11",
            profile.name
        );
        match &profile.comparability {
            Comparability::Comparable => assert!(!must_refuse),
            Comparability::NonComparable { reason } => {
                assert!(must_refuse);
                // The refusal has to be usable: a reader shows this string.
                assert!(
                    reason.contains("ADR-0927"),
                    "the ci profile's refusal must cite the rule: {reason}"
                );
                assert!(reason.len() > 80, "the refusal must be a real reason");
            }
        }
    }
    assert_eq!(
        workload
            .profiles
            .iter()
            .filter(|p| !p.is_publishable())
            .count(),
        1,
        "exactly one profile is non-comparable"
    );
}

#[test]
fn the_family_shares_partition_every_profiles_active_series_exactly() {
    let workload = load_workload(workload_path()).expect("manifest loads");
    assert_eq!(workload.families.len(), 5);
    assert_eq!(workload.label_dimensions.len(), 5);

    // The declared shares, in permille, summing to 1000.
    let expected: &[(&str, FamilyKind, u64, usize)] = &[
        ("metricsbench_gauge_cpu_percent", FamilyKind::Gauge, 400, 2),
        ("metricsbench_requests_total", FamilyKind::Counter, 300, 3),
        (
            "metricsbench_request_duration_seconds",
            FamilyKind::ClassicHistogram,
            150,
            1,
        ),
        (
            "metricsbench_latency_native",
            FamilyKind::NativeHistogram,
            100,
            1,
        ),
        ("metricsbench_build_info", FamilyKind::Gauge, 50, 2),
    ];
    for (i, (name, kind, permille, labels)) in expected.iter().enumerate() {
        let f = &workload.families[i];
        assert_eq!(&f.name, name);
        assert_eq!(f.kind, *kind, "{name} kind");
        assert_eq!(f.series_permille, *permille, "{name} share");
        assert_eq!(f.labels.len(), *labels, "{name} label count");
    }
    assert_eq!(
        workload
            .families
            .iter()
            .map(|f| f.series_permille)
            .sum::<u64>(),
        1000
    );

    // Seven declared bounds plus +Inf, _sum and _count: ten series per classic
    // histogram instance.
    assert_eq!(
        workload.series_per_instance(FamilyKind::ClassicHistogram),
        10
    );

    // Per profile, the per-family series counts sum to exactly the profile's
    // active-series count, and each divides into whole instances. Bounded
    // against the profile's own figure rather than a flat floor.
    for profile in &workload.profiles {
        let mut total = 0u64;
        for family in &workload.families {
            let series = workload.family_series(profile, family);
            let per_instance = workload.series_per_instance(family.kind);
            assert_eq!(
                series % per_instance,
                0,
                "family `{}` under `{}` does not divide into whole instances",
                family.name,
                profile.name
            );
            assert_eq!(
                workload.family_instances(profile, family) * per_instance,
                series
            );
            total += series;
        }
        assert_eq!(
            total, profile.active_series,
            "the family shares must partition profile `{}`'s active series",
            profile.name
        );
    }
}

#[test]
fn churn_figures_are_exact_per_profile_and_the_generator_agrees_with_the_profile() {
    let workload = load_workload(workload_path()).expect("manifest loads");
    let expected: &[(&str, u64, u64, u64)] = &[
        // (profile, epochs, churned series per epoch, total series created)
        ("cardinality", 2, 0, 1_000_000),
        ("history", 720, 0, 10_000),
        ("churn", 36, 10_000, 400_000),
        ("ci", 1, 0, 1_000),
    ];
    for (name, epochs, churned, created) in expected {
        let p = workload.profile(name).expect("profile present");
        assert_eq!(p.churn_epochs(), *epochs, "{name} epochs");
        assert_eq!(p.churned_series_per_epoch(), *churned, "{name} cohort");
        assert_eq!(p.total_series_created(), *created, "{name} series created");

        // The generator computes the same figure from its own per-family plans.
        // Arithmetic only: nothing is generated here, so the 1.7 billion-sample
        // profile costs nothing to check.
        let generator = Generator::new(&workload, name, 0).expect("generator builds");
        assert_eq!(
            generator.total_series_created(p.samples_per_series),
            *created,
            "the generator's plans and profile `{name}`'s own arithmetic disagree"
        );
    }
}

#[test]
fn every_declared_anomaly_rate_is_actually_delivered_by_the_run() {
    // A declared anomaly rate that the run never realizes is an inert claim: the
    // ci profile once declared 500 bp of churn over a 30-minute run that spans
    // one churn epoch, so `churn_epochs()` was 1, no cohort was ever retired,
    // and the profile claimed coverage it could not deliver. This pins declared
    // == delivered for churn (arithmetic over every profile) and for every
    // generator-level anomaly rate (a full ci run).
    let workload = load_workload(workload_path()).expect("manifest loads");

    // Churn: a nonzero rate must retire at least one cohort over the profile's
    // own duration, and a zero rate must retire none.
    for p in &workload.profiles {
        let declares_churn = p.churn_basis_points_per_hour > 0;
        let delivers_churn = p.total_series_created() > p.active_series;
        assert_eq!(
            declares_churn,
            delivers_churn,
            "profile `{}` declares {} bp of churn but {} retire a cohort: a nonzero rate over a \
             run that never crosses an epoch boundary is an inert anomaly",
            p.name,
            p.churn_basis_points_per_hour,
            if delivers_churn { "does" } else { "does not" }
        );
    }

    // Every generator anomaly rate: a nonzero rate delivers events on the full
    // ci run, a zero rate delivers none.
    let ci = workload.profile("ci").expect("ci profile");
    let mut generator = Generator::new(&workload, "ci", 0).expect("generator builds");
    let (_, report) = generator
        .generate_bytes(ci.samples_per_series)
        .expect("the ci profile generates");
    let anomalies = workload.generator.anomalies;
    for (label, rate, delivered) in [
        (
            "missing samples",
            anomalies.missing_sample_one_in,
            report.omitted_missing_samples,
        ),
        (
            "stale markers",
            anomalies.stale_marker_one_in,
            report.stale_markers,
        ),
        (
            "counter resets",
            anomalies.counter_reset_one_in,
            report.counter_reset_events,
        ),
        (
            "out-of-order samples",
            anomalies.out_of_order_one_in,
            report.out_of_order_samples,
        ),
    ] {
        if rate > 0 {
            assert!(
                delivered > 0,
                "{label}: rate one-in-{rate} is declared but the run delivered none"
            );
        } else {
            assert_eq!(
                delivered, 0,
                "{label}: rate is 0 but the run delivered {delivered}"
            );
        }
    }
}

#[test]
fn every_corpus_entry_is_classed_and_the_classes_are_evenly_covered() {
    let entries = load_corpus(corpus_path()).expect("corpus loads");
    let counts = class_counts(&entries);
    assert_eq!(counts.len(), 8, "eight cost classes");
    for (class, n) in &counts {
        assert_eq!(
            *n,
            ENTRIES_PER_CLASS,
            "cost class `{}` holds {n} entries, expected {ENTRIES_PER_CLASS}",
            class.slug()
        );
    }
    assert_eq!(
        counts.iter().map(|(_, n)| *n).sum::<usize>(),
        CORPUS_SIZE,
        "every entry is classed exactly once"
    );

    // Ids are unique (the gate proves it) and stable-looking: every entry is
    // prefixed `mb_`, so a report row cannot be confused with a SQL corpus row.
    let ids: BTreeSet<&str> = entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids.len(), CORPUS_SIZE);
    for id in &ids {
        assert!(id.starts_with("mb_"), "unexpected id shape: {id}");
    }
}

#[test]
fn the_corpus_splits_exactly_six_range_queries_from_eighteen_instant_ones() {
    let entries = load_corpus(corpus_path()).expect("corpus loads");
    let range = entries.iter().filter(|e| e.eval == EvalKind::Range).count();
    let instant = entries
        .iter()
        .filter(|e| e.eval == EvalKind::Instant)
        .count();
    assert_eq!(range, 6);
    assert_eq!(instant, 18);
    assert_eq!(range + instant, CORPUS_SIZE);
}

#[test]
fn only_the_long_range_entries_restrict_themselves_to_a_profile() {
    let entries = load_corpus(corpus_path()).expect("corpus loads");
    let restricted: Vec<&str> = entries
        .iter()
        .filter(|e| !e.profiles.is_empty())
        .map(|e| e.id.as_str())
        .collect();
    // Exactly the three long-range entries: only `history` (30 days) and, for
    // the 6-hour window, `churn` (36 hours) generate enough data for them.
    assert_eq!(
        restricted,
        vec![
            "mb_long_range_7d_avg",
            "mb_long_range_subquery_max",
            "mb_long_range_predict_linear"
        ]
    );
    for entry in entries.iter().filter(|e| !e.profiles.is_empty()) {
        assert_eq!(entry.class, Some(CostClass::LongRange), "{}", entry.id);
        assert!(entry.runs_under("history"), "{}", entry.id);
        assert!(!entry.runs_under("cardinality"), "{}", entry.id);
    }

    // Per profile, the exact number of entries that run.
    let counted = |profile: &str| entries.iter().filter(|e| e.runs_under(profile)).count();
    assert_eq!(counted("history"), 24);
    assert_eq!(counted("churn"), 22);
    assert_eq!(counted("cardinality"), 21);
    assert_eq!(counted("ci"), 21);
}

#[test]
fn every_named_construct_is_in_the_promql_registry_and_the_corpus_names_a_known_number_of_them() {
    let entries = load_corpus(corpus_path()).expect("corpus loads");
    let known = known_construct_names();
    let named: BTreeSet<&str> = entries
        .iter()
        .flat_map(|e| e.constructs.iter().map(String::as_str))
        .collect();
    for construct in &named {
        assert!(
            known.contains(construct),
            "construct `{construct}` is not in the PromQL conformance registry"
        );
    }
    // The corpus is a workload sample, not a conformance suite: it names 34 of
    // the registry's 133 constructs. Pinned exactly so adding an entry that
    // names nothing new, or dropping the only entry that names a construct, is
    // visible.
    assert_eq!(named.len(), 34);
    assert_eq!(known.len(), 133);

    // Every entry names at least two constructs, so no entry passes the
    // non-empty check on a single generic name.
    for entry in &entries {
        assert!(
            entry.constructs.len() >= 2,
            "entry `{}` names only {} construct(s)",
            entry.id,
            entry.constructs.len()
        );
    }
}

#[test]
fn every_metric_and_label_literal_the_corpus_selects_exists_in_the_workload() {
    let workload = load_workload(workload_path()).expect("manifest loads");
    let entries = load_corpus(corpus_path()).expect("corpus loads");
    let emitted = workload.emitted_metric_names();
    // Five families: three suffixed series for the classic histogram, one name
    // each for the other four.
    assert_eq!(emitted.len(), 7);

    let absent = &workload.generator.absent_metric_name;
    let mut selected: BTreeSet<String> = BTreeSet::new();
    for entry in &entries {
        for word in entry
            .promql
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        {
            if word.starts_with("metricsbench_") {
                selected.insert(word.to_string());
            }
        }
    }
    for metric in &selected {
        assert!(
            metric == absent || emitted.contains(metric),
            "corpus selects `{metric}`, which the workload does not emit"
        );
    }
    // Every emitted metric except the two the corpus has no query for is
    // actually selected; pinned as an exact set so a family added to the
    // manifest without a query is visible.
    let expected_selected: BTreeSet<String> = [
        "metricsbench_absent_metric",
        "metricsbench_build_info",
        "metricsbench_gauge_cpu_percent",
        "metricsbench_latency_native",
        "metricsbench_request_duration_seconds_bucket",
        "metricsbench_requests_total",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(selected, expected_selected);

    // Label literals the corpus selects on must be values the manifest
    // declares, or every one of those queries matches nothing.
    for (label, value) in [
        ("job", "metricsbench-api"),
        ("region", "eu-west-1"),
        ("method", "GET"),
        ("status", "200"),
    ] {
        let dim = workload.dimension(label).expect("dimension declared");
        assert!(
            dim.values.iter().any(|v| v == value),
            "corpus selects {label}=\"{value}\", which the manifest does not declare"
        );
    }
    assert_eq!(
        workload.generator.scaling_label_value_prefix, "metricsbench-instance-",
        "the single-series entries select an instance literal built from this prefix"
    );
}

#[test]
fn the_ci_profile_generates_its_exact_declared_sample_count() {
    let workload = load_workload(workload_path()).expect("manifest loads");
    let ci = workload.profile("ci").expect("ci profile");
    let mut generator = Generator::new(&workload, "ci", 0).expect("generator builds");
    let (bytes, report) = generator
        .generate_bytes(ci.samples_per_series)
        .expect("the ci profile generates");

    assert_eq!(report.nominal_samples, ci.total_samples);
    assert_eq!(report.nominal_samples, 120_000);
    assert_eq!(
        report.emitted_samples + report.omitted_missing_samples,
        report.nominal_samples,
        "generated must equal emitted plus explicitly reported omissions"
    );
    assert_eq!(report.active_series, 1_000);
    assert_eq!(report.total_series_created, 1_000);
    assert!(!report.publishable);
    assert!(report.non_comparable_reason.is_some());

    // One line per emitted sample, and the report's byte count is the stream's.
    assert_eq!(
        bytes.iter().filter(|b| **b == b'\n').count() as u64,
        report.emitted_samples
    );
    assert_eq!(report.bytes, bytes.len() as u64);

    // Native-histogram samples: exactly one per native instance per step. 100
    // instances (100 permille of 1,000 series, one series each) over 120 steps,
    // less those whose scrape was dropped. Bounded against the family's own
    // nominal count rather than a flat floor.
    let native_nominal = 100 * ci.samples_per_series;
    assert!(
        report.native_histogram_samples <= native_nominal,
        "{} native samples exceeds the family's nominal {native_nominal}",
        report.native_histogram_samples
    );
    assert_eq!(
        report.float_samples + report.stale_markers + report.native_histogram_samples,
        report.emitted_samples
    );
    // The injected anomalies all fired at their designed rate: a run that
    // produced one marker where the profile designs for dozens would pass every
    // count above while generating a workload the ADR does not describe. The
    // generator is deterministic on this seed and profile, so each figure is
    // exact. Each is also checked against the nominal count its own one-in rate
    // implies, so a constant that stops tracking the design is visible as more
    // than a changed number.
    let anomalies = workload.generator.anomalies;
    let steps = ci.samples_per_series;
    // Instances scraped per step, from the family shares of the 1,000 active
    // series: 400 cpu gauge, 300 counter, 15 classic histogram (10 series
    // each), 100 native, 50 build_info gauge. Staleness applies to gauges only;
    // resets apply to counters, classic histograms and native histograms, which
    // all carry counter semantics.
    let gauge_scrapes = 450 * steps;
    let counter_scrapes = (300 + 15 + 100) * steps;
    let instance_scrapes = 865 * steps;
    // The omission and out-of-order figures count series rather than instances,
    // so their nominals are instance-rate approximations; a factor of two
    // absorbs the per-instance series weighting without admitting a fraction of
    // the designed rate.
    for (label, observed, nominal) in [
        (
            "stale markers",
            report.stale_markers,
            gauge_scrapes / anomalies.stale_marker_one_in,
        ),
        (
            "missing samples",
            report.omitted_missing_samples,
            instance_scrapes / anomalies.missing_sample_one_in,
        ),
        (
            "counter resets",
            report.counter_reset_events,
            counter_scrapes / anomalies.counter_reset_one_in,
        ),
        (
            "out-of-order samples",
            report.out_of_order_samples,
            instance_scrapes / anomalies.out_of_order_one_in,
        ),
    ] {
        assert!(
            observed >= nominal / 2 && observed <= nominal * 2,
            "{label}: {observed} is not within a factor of two of the {nominal} \
             the profile's own rate designs for"
        );
    }
    // The exact figures this seed produces.
    assert_eq!(report.stale_markers, 62, "staleness markers");
    assert_eq!(report.omitted_missing_samples, 225, "missing samples");
    assert_eq!(report.counter_reset_events, 43, "counter resets");
    assert_eq!(report.out_of_order_samples, 182, "out-of-order samples");
}

/// The `metricsbench_gen` binary, resolved the way an integration test can.
fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_metricsbench_gen"))
}

fn run_bin(args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .args(args)
        .output()
        .expect("spawn metricsbench_gen")
}

#[test]
fn the_shipping_binary_gates_the_checked_in_artifacts_and_reports_exact_figures() {
    let out = run_bin(&["--profile", "ci", "--steps", "20"]);
    assert!(
        out.status.success(),
        "metricsbench_gen failed on the checked-in artifacts: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let document: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("the bin prints one JSON document");
    assert_eq!(document["corpus"]["entries"], 24);
    assert_eq!(document["corpus"]["entries_in_profile"], 21);
    assert_eq!(document["workload"]["seed"], 927_000_933u64);
    assert_eq!(document["workload"]["families"], 5);
    assert_eq!(document["generation"]["profile"], "ci");
    assert_eq!(document["generation"]["nominal_samples"], 20_000);
    assert_eq!(document["generation"]["publishable"], false);
    assert_eq!(document["run"]["covers_whole_profile"], false);
    let classes = document["corpus"]["cost_classes"]
        .as_array()
        .expect("cost class table");
    assert_eq!(classes.len(), 8);
    for row in classes {
        assert_eq!(row["entries"], 3, "class row {row}");
    }
}

#[test]
fn the_binary_refuses_to_publish_a_non_comparable_profile_or_a_truncated_run() {
    // The `ci` profile carries its refusal in the artifact, and the binary acts
    // on that field rather than on a hardcoded profile name.
    let out = run_bin(&["--profile", "ci", "--steps", "5", "--require-comparable"]);
    assert!(!out.status.success(), "a non-comparable profile must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not comparable") && stderr.contains("ADR-0927"),
        "the refusal must quote the artifact's reason: {stderr}"
    );

    // A comparable profile still refuses when the run does not cover it.
    let out = run_bin(&[
        "--profile",
        "cardinality",
        "--steps",
        "1",
        "--require-comparable",
    ]);
    assert!(!out.status.success(), "a truncated run must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("1 of profile `cardinality`'s 360 steps"),
        "the refusal must name both step counts: {stderr}"
    );
}

/// Copy the checked-in corpus with one JSON mutation applied, so the corruption
/// is the only difference from an artifact that passes.
fn corrupt_corpus(
    dir: &tempfile::TempDir,
    name: &str,
    mutate: impl Fn(&mut serde_json::Value),
) -> PathBuf {
    let text = std::fs::read_to_string(corpus_path()).expect("read the real corpus");
    let mut doc: serde_json::Value = serde_json::from_str(&text).expect("the real corpus is JSON");
    mutate(&mut doc);
    let path = dir.path().join(name);
    std::fs::write(&path, doc.to_string()).expect("write the corrupt corpus");
    path
}

#[test]
fn a_corrupt_corpus_fails_through_the_binary_not_only_through_gate_corpus() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workload = workload_path();
    let workload = workload.to_str().expect("utf8 path").to_string();

    // 1. An unclassified entry in an otherwise classed corpus: the acceptance
    //    test's check, reached through the shipping binary.
    let path = corrupt_corpus(&dir, "unclassified.json", |doc| {
        doc["entries"][3]
            .as_object_mut()
            .expect("entry object")
            .remove("class");
    });
    let out = run_bin(&[
        "--workload",
        &workload,
        "--corpus",
        path.to_str().expect("utf8 path"),
        "--profile",
        "ci",
        "--steps",
        "1",
    ]);
    assert!(!out.status.success(), "an unclassified entry must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("mb_single_gauge_instant") && stderr.contains("no cost class"),
        "the binary must name the unclassified entry: {stderr}"
    );

    // 2. A construct the registry does not carry.
    let path = corrupt_corpus(&dir, "unknown_construct.json", |doc| {
        doc["entries"][0]["constructs"][1] = serde_json::json!("absent_over_tyme");
    });
    let out = run_bin(&[
        "--workload",
        &workload,
        "--corpus",
        path.to_str().expect("utf8 path"),
        "--profile",
        "ci",
        "--steps",
        "1",
    ]);
    assert!(!out.status.success(), "an unknown construct must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("absent_over_tyme") && stderr.contains("REGISTRY"),
        "the binary must name the unknown construct: {stderr}"
    );

    // 3. A misspelled KEY, which a typed class enum alone cannot catch.
    let path = corrupt_corpus(&dir, "typo_key.json", |doc| {
        let entry = doc["entries"][0].as_object_mut().expect("entry object");
        let class = entry.remove("class").expect("class present");
        entry.insert("clas".to_string(), class);
    });
    let out = run_bin(&[
        "--workload",
        &workload,
        "--corpus",
        path.to_str().expect("utf8 path"),
        "--profile",
        "ci",
        "--steps",
        "1",
    ]);
    assert!(!out.status.success(), "a misspelled class key must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("clas"),
        "the binary must name the offending field: {stderr}"
    );

    // 4. A duplicate id.
    let path = corrupt_corpus(&dir, "duplicate.json", |doc| {
        let first = doc["entries"][0].clone();
        doc["entries"]
            .as_array_mut()
            .expect("entry list")
            .push(first);
    });
    let out = run_bin(&[
        "--workload",
        &workload,
        "--corpus",
        path.to_str().expect("utf8 path"),
        "--profile",
        "ci",
        "--steps",
        "1",
    ]);
    assert!(!out.status.success(), "a duplicate id must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("appears more than once"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 5. A query selecting a metric the workload does not emit. This one is not
    //    a corpus-gate check at all: it is the band the binary adds on top, and
    //    it is only reachable through the binary.
    let path = corrupt_corpus(&dir, "unknown_metric.json", |doc| {
        doc["entries"][9]["promql"] = serde_json::json!("sum(rate(metricsbench_ghost_total[5m]))");
    });
    let out = run_bin(&[
        "--workload",
        &workload,
        "--corpus",
        path.to_str().expect("utf8 path"),
        "--profile",
        "ci",
        "--steps",
        "1",
    ]);
    assert!(!out.status.success(), "an unknown metric must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("metricsbench_ghost_total"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_corrupt_workload_manifest_fails_through_the_binary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let text = std::fs::read_to_string(workload_path()).expect("read the real manifest");
    let mut doc: serde_json::Value =
        serde_json::from_str(&text).expect("the real manifest is JSON");
    // Promote the ci profile to comparable: the exact edit ADR-0927 decision 11
    // forbids, and the one a reader would make to publish a CI number.
    doc["profiles"][3]["comparability"] = serde_json::json!("comparable");
    let path = dir.path().join("comparable_ci.json");
    std::fs::write(&path, doc.to_string()).expect("write the corrupt manifest");

    let out = run_bin(&[
        "--workload",
        path.to_str().expect("utf8 path"),
        "--profile",
        "ci",
        "--steps",
        "1",
    ]);
    assert!(!out.status.success(), "a comparable ci profile must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("refuse to publish"),
        "the binary must refuse a manifest that promotes ci: {stderr}"
    );
}

#[test]
fn the_binary_refuses_an_unknown_profile_rather_than_defaulting() {
    let out = run_bin(&["--profile", "smoke", "--steps", "1"]);
    assert!(!out.status.success(), "an unknown profile must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("declares no profile `smoke`"), "{stderr}");

    // And `--profile` has no default at all: the choice selects which data the
    // run touches, so it cannot be silent.
    let out = run_bin(&["--steps", "1"]);
    assert!(!out.status.success(), "a missing --profile must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--profile"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_binary_writes_the_same_stream_twice_for_the_same_seed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = dir.path().join("first.stream");
    let second = dir.path().join("second.stream");
    let digest = |out: &std::process::Output| -> String {
        let document: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("JSON document");
        document["generation"]["digest"]
            .as_str()
            .expect("digest field")
            .to_string()
    };

    let a = run_bin(&[
        "--profile",
        "ci",
        "--steps",
        "10",
        "--base-ts-ms",
        "1700000000000",
        "--out",
        first.to_str().expect("utf8 path"),
    ]);
    assert!(a.status.success(), "{}", String::from_utf8_lossy(&a.stderr));
    let b = run_bin(&[
        "--profile",
        "ci",
        "--steps",
        "10",
        "--base-ts-ms",
        "1700000000000",
        "--out",
        second.to_str().expect("utf8 path"),
    ]);
    assert!(b.status.success(), "{}", String::from_utf8_lossy(&b.stderr));

    let first_bytes = std::fs::read(&first).expect("read first stream");
    let second_bytes = std::fs::read(&second).expect("read second stream");
    assert_eq!(
        first_bytes, second_bytes,
        "the shipping binary must write byte-identical output for one seed"
    );
    assert_eq!(digest(&a), digest(&b));
    assert!(!first_bytes.is_empty());
    // 10 steps over 1,000 active series, less the dropped scrapes.
    let lines = first_bytes.iter().filter(|b| **b == b'\n').count();
    assert!(
        lines <= 10_000 && lines > 9_000,
        "{lines} lines is outside the 10-step nominal 10,000 less its dropped scrapes"
    );

    // A different base timestamp is a different stream, so the equality above
    // is not a stream that ignores its inputs.
    let shifted = dir.path().join("shifted.stream");
    let c = run_bin(&[
        "--profile",
        "ci",
        "--steps",
        "10",
        "--base-ts-ms",
        "1700000015000",
        "--out",
        shifted.to_str().expect("utf8 path"),
    ]);
    assert!(c.status.success(), "{}", String::from_utf8_lossy(&c.stderr));
    assert_ne!(
        std::fs::read(&shifted).expect("read shifted stream"),
        first_bytes
    );
}
