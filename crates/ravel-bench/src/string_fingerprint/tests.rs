#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;

use super::spec::{Kind, PredicateSpec, SubjectBands, SubjectSpec};
use super::*;

fn wide_bands() -> SubjectBands {
    SubjectBands {
        lines: [0.0, 1e9],
        blocks: [0.0, 1e9],
        compressed_bytes: [0.0, 1e9],
        zstd_cpu_s: [0.0, 1e6],
        build_cpu_over_zstd: [0.0, 1e6],
        summary_bytes_share: [0.0, 1.0],
        seen_equals_false_positive_share: [0.0, 1.0],
        seen_like_false_positive_share: [0.0, 1.0],
        unseen_equals_false_positive_share: [0.0, 1.0],
        unseen_like_false_positive_share: [0.0, 1.0],
        bloom_equals_blocks_kept_share: [0.0, 1.0],
        contains_like_cells_tested_share: [0.0, 1.0],
        saturation_false_positive_share: [0.0, 1.0],
    }
}

fn pred(id: &str, kind: Kind, literal: &str) -> PredicateSpec {
    PredicateSpec {
        id: id.to_string(),
        kind,
        literal: literal.to_string(),
        reason: "test".to_string(),
    }
}

fn tiny_spec() -> SubjectSpec {
    SubjectSpec {
        name: "s1".to_string(),
        corpus: "test".to_string(),
        source: "inline".to_string(),
        column: "value".to_string(),
        block_records: 2,
        gram_len: 3,
        df_threshold: 0.5,
        budget_share: 0.5,
        bands: wide_bands(),
        seen: vec![
            pred("e1", Kind::Equals, "gamma"),
            pred("l1", Kind::Like, "needle"),
        ],
        unseen: vec![
            pred("e2", Kind::Equals, "other"),
            pred("l2", Kind::Like, "trace"),
        ],
    }
}

fn tiny_blocks() -> Vec<BlockValues> {
    vec![
        vec![b"alpha needle beta".to_vec(), b"gamma".to_vec()],
        vec![b"delta".to_vec(), b"epsilon stack trace".to_vec()],
    ]
}

fn predicate_input(id: &str, kind: Kind, literal: &str) -> PredicateInput {
    PredicateInput {
        id: id.to_string(),
        kind,
        literal: literal.as_bytes().to_vec(),
        bloom_blocks_kept: if kind == Kind::Equals { Some(1) } else { None },
        contains_cells_tested: if kind == Kind::Like { Some(3) } else { None },
    }
}

#[test]
fn measure_runs_end_to_end_on_a_small_fixture() {
    let spec = tiny_spec();
    let blocks = tiny_blocks();
    let seen = vec![
        predicate_input("e1", Kind::Equals, "gamma"),
        predicate_input("l1", Kind::Like, "needle"),
    ];
    let unseen = vec![
        predicate_input("e2", Kind::Equals, "other"),
        predicate_input("l2", Kind::Like, "trace"),
    ];
    let input = SubjectInput {
        lines: 4,
        blocks: &blocks,
        zstd_level: 3,
        seen: &seen,
        unseen: &unseen,
    };
    let report =
        measure(&spec, &input).expect("measure should not hard-error on a conservative run");

    assert_eq!(report.subject, "s1");
    let by_key: BTreeMap<&str, f64> = report
        .figures
        .iter()
        .map(|f| (f.key.as_str(), f.value))
        .collect();
    assert_eq!(by_key["s1/lines"], 4.0);
    assert_eq!(by_key["s1/blocks"], 2.0);
    assert!(by_key["s1/compressed_bytes"] > 0.0);
    // An equals literal outside the vocabulary always survives, by
    // construction (summary module doc); "other" truly holds in no block,
    // so every surviving block is a false positive.
    assert_eq!(by_key["s1/unseen/equals/false_positive_share"], 1.0);
    assert_eq!(by_key["s1/bloom/equals/blocks_kept_share"], 0.5);
    assert_eq!(by_key["s1/contains/like/cells_tested_share"], 3.0 / 4.0);
    // No block holds a value over 1024 bytes, so the saturation arm is
    // vacuously 0.0, not simply absent.
    assert_eq!(by_key["s1/saturation/false_positive_share"], 0.0);

    let structural_problems: Vec<&String> = report
        .problems
        .iter()
        .filter(|p| !p.contains("cpu"))
        .collect();
    assert!(structural_problems.is_empty(), "{structural_problems:?}");
    assert!(!report.decision.line.is_empty());
}

#[test]
fn measure_never_drops_a_block_that_truly_holds_a_predicate() {
    // "needle" is a seen like predicate present in block 0; a correct
    // measure() must find it surviving there (an error means the summary
    // dropped a true holder, which must never happen).
    let spec = tiny_spec();
    let blocks = tiny_blocks();
    let seen = vec![
        predicate_input("e1", Kind::Equals, "gamma"),
        predicate_input("l1", Kind::Like, "needle"),
    ];
    let unseen = vec![
        predicate_input("e2", Kind::Equals, "other"),
        predicate_input("l2", Kind::Like, "trace"),
    ];
    let input = SubjectInput {
        lines: 4,
        blocks: &blocks,
        zstd_level: 3,
        seen: &seen,
        unseen: &unseen,
    };
    assert!(measure(&spec, &input).is_ok());
}

#[test]
fn check_figures_reports_missing_duplicate_out_of_band_and_unbanded() {
    let bands: BTreeMap<String, Band> = [
        ("a".to_string(), [0.0, 1.0]),
        ("b".to_string(), [0.0, 1.0]),
        ("c".to_string(), [0.0, 1.0]),
    ]
    .into_iter()
    .collect();
    let figures = vec![
        Figure {
            key: "b".to_string(),
            value: 0.5,
        },
        Figure {
            key: "b".to_string(),
            value: 0.6,
        },
        Figure {
            key: "c".to_string(),
            value: 5.0,
        },
        Figure {
            key: "d".to_string(),
            value: 1.0,
        },
    ];
    let problems = check_figures(&figures, &bands);
    assert!(
        problems.iter().any(|p| p.starts_with("a: missing")),
        "{problems:?}"
    );
    assert!(
        problems.iter().any(|p| p.contains("b: present 2 times")),
        "{problems:?}"
    );
    assert!(
        problems
            .iter()
            .any(|p| p.contains("c: 5") && p.contains("outside band")),
        "{problems:?}"
    );
    assert!(
        problems.iter().any(|p| p.starts_with("d: no band")),
        "{problems:?}"
    );
}

#[test]
fn check_figures_is_clean_when_every_figure_matches_its_band_exactly_once() {
    let bands: BTreeMap<String, Band> = [("a".to_string(), [0.0, 1.0])].into_iter().collect();
    let figures = vec![Figure {
        key: "a".to_string(),
        value: 1.0,
    }];
    assert!(check_figures(&figures, &bands).is_empty());
}

fn full_decision_input() -> DecisionInput {
    DecisionInput {
        subject: "s1".to_string(),
        unseen_equals_fp: Some(0.05),
        unseen_like_fp: Some(0.05),
        summary_bytes_share: Some(0.005),
        build_cpu_over_zstd: Some(1.5),
        saturation_fp: Some(0.1),
    }
}

#[test]
fn decide_is_go_when_every_leg_clears_its_bar() {
    let d = decide(&full_decision_input(), true);
    assert!(d.go, "{}", d.line);
}

#[test]
fn decide_is_no_go_when_unseen_equals_exceeds_the_bar() {
    let mut input = full_decision_input();
    input.unseen_equals_fp = Some(GO_FALSE_POSITIVE_BAR + 0.01);
    let d = decide(&input, true);
    assert!(!d.go);
    assert!(d.line.contains("unseen_equals"), "{}", d.line);
}

#[test]
fn decide_is_no_go_when_unseen_like_exceeds_the_bar() {
    let mut input = full_decision_input();
    input.unseen_like_fp = Some(GO_FALSE_POSITIVE_BAR + 0.01);
    let d = decide(&input, true);
    assert!(!d.go);
    assert!(d.line.contains("unseen_like"), "{}", d.line);
}

#[test]
fn decide_is_no_go_when_summary_bytes_exceeds_the_bar() {
    let mut input = full_decision_input();
    input.summary_bytes_share = Some(GO_BYTES_BAR + 0.001);
    let d = decide(&input, true);
    assert!(!d.go);
    assert!(d.line.contains("bytes"), "{}", d.line);
}

#[test]
fn decide_is_no_go_when_build_cpu_exceeds_the_bar() {
    let mut input = full_decision_input();
    input.build_cpu_over_zstd = Some(GO_CPU_BAR + 0.1);
    let d = decide(&input, true);
    assert!(!d.go);
    assert!(d.line.contains("cpu"), "{}", d.line);
}

#[test]
fn decide_is_no_go_when_saturation_exceeds_the_bar_and_eligible() {
    let mut input = full_decision_input();
    input.saturation_fp = Some(GO_SATURATION_BAR + 0.01);
    let d = decide(&input, true);
    assert!(!d.go);
    assert!(d.line.contains("saturation"), "{}", d.line);
}

#[test]
fn decide_passes_saturation_vacuously_when_the_subject_is_body_ineligible() {
    let mut input = full_decision_input();
    input.saturation_fp = None;
    let d = decide(&input, false);
    assert!(d.go, "{}", d.line);
    assert!(d.line.contains("body-ineligible"), "{}", d.line);
}

#[test]
fn a_missing_figure_is_never_silently_in_band() {
    // Every gating leg must read as a failure when its figure was never
    // reported, never default to a pass.
    let empty = DecisionInput {
        subject: "s1".to_string(),
        unseen_equals_fp: None,
        unseen_like_fp: None,
        summary_bytes_share: None,
        build_cpu_over_zstd: None,
        saturation_fp: None,
    };
    let d = decide(&empty, true);
    assert!(!d.go, "{}", d.line);
    for leg in ["unseen_equals", "unseen_like", "bytes", "cpu", "saturation"] {
        assert!(d.line.contains(leg), "{}: {}", leg, d.line);
    }
}

fn saved(subject: &str, go: bool, problems: usize) -> SavedDecision {
    SavedDecision {
        decision: Decision {
            subject: subject.to_string(),
            go,
            line: format!(
                "string_fingerprint decision {subject}: {}",
                if go { "GO" } else { "NO-GO" }
            ),
        },
        problems,
    }
}

#[test]
fn overall_is_go_only_when_every_named_subject_is_a_clean_go() {
    let subjects = vec!["s1".to_string(), "s2".to_string()];
    let d = overall(&subjects, &[saved("s1", true, 0), saved("s2", true, 0)]);
    assert!(d.go, "{}", d.line);
}

#[test]
fn overall_is_no_go_when_a_subject_is_missing() {
    let subjects = vec!["s1".to_string(), "s2".to_string()];
    let d = overall(&subjects, &[saved("s1", true, 0)]);
    assert!(!d.go);
    assert!(d.line.contains("s2=missing"), "{}", d.line);
}

#[test]
fn overall_is_no_go_when_a_go_report_has_figure_problems() {
    let subjects = vec!["s1".to_string()];
    let d = overall(&subjects, &[saved("s1", true, 2)]);
    assert!(!d.go);
    assert!(d.line.contains("not a result"), "{}", d.line);
}

#[test]
fn overall_is_no_go_when_a_subject_has_more_than_one_report() {
    let subjects = vec!["s1".to_string()];
    let d = overall(&subjects, &[saved("s1", true, 0), saved("s1", true, 0)]);
    assert!(!d.go);
    assert!(d.line.contains("2 reports"), "{}", d.line);
}

#[test]
fn overall_is_no_go_when_a_saved_decision_is_not_pre_registered() {
    let subjects = vec!["s1".to_string()];
    let d = overall(&subjects, &[saved("s1", true, 0), saved("s9", true, 0)]);
    assert!(!d.go);
    assert!(d.line.contains("s9=not pre-registered"), "{}", d.line);
}

#[test]
fn overall_is_no_go_when_any_named_subject_is_a_no_go() {
    let subjects = vec!["s1".to_string(), "s2".to_string()];
    let d = overall(&subjects, &[saved("s1", true, 0), saved("s2", false, 0)]);
    assert!(!d.go);
}
