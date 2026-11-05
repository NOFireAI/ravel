#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;

use super::spec::{Expect, SubjectSpec};
use super::workload::{self, Workload};
use super::*;

/// Production answers computed from the values themselves, with switches
/// that make it wrong in the ways `measure` must refuse.
struct Truth<'a> {
    blocks: &'a [BlockValues],
    equals_short_by_one: bool,
    contains_drops_a_row: bool,
}

impl Production for Truth<'_> {
    fn equals_blocks_kept(&self, literal: &str) -> Result<u64, String> {
        let held = self
            .blocks
            .iter()
            .filter(|b| b.iter().any(|v| v.as_slice() == literal.as_bytes()))
            .count() as u64;
        Ok(if self.equals_short_by_one {
            held.saturating_sub(1)
        } else {
            held
        })
    }

    fn contains(&self, literal: &str, keep: Option<&[bool]>) -> Result<ContainsRun, String> {
        let mut cells_tested = 0;
        let mut rows = Vec::new();
        for (i, b) in self.blocks.iter().enumerate() {
            if keep.is_some_and(|k| !k[i]) {
                rows.push(0);
                continue;
            }
            cells_tested += b.len() as u64;
            let mut n = b
                .iter()
                .filter(|v| String::from_utf8_lossy(v).contains(literal))
                .count() as u64;
            if self.contains_drops_a_row && n > 0 {
                n = 0;
            }
            rows.push(n);
        }
        Ok(ContainsRun { cells_tested, rows })
    }
}

fn blocks(n: usize) -> Vec<BlockValues> {
    (0..n)
        .map(|b| {
            (0..6)
                .map(|r| {
                    format!("host{b} request {r} served path /a{b}x{r} by worker-{b}").into_bytes()
                })
                .collect()
        })
        .collect()
}

fn subject(figures: BTreeMap<String, Expect>) -> SubjectSpec {
    SubjectSpec {
        name: "s1".into(),
        corpus: "test".into(),
        source: "inline".into(),
        column: "value".into(),
        block_records: 6,
        object_records: 12,
        max_lines: 48,
        zstd_level: 3,
        gram_len: 3,
        budget_share: 1.0,
        fallback_min_bits: 64,
        seed: 5,
        literals_per_kind: 4,
        eq_max_len: 64,
        like_min_len: 4,
        like_max_len: 9,
        figures,
    }
}

/// Every figure unpredicted, except the saturation figures a corpus with no
/// long value cannot report.
fn unpredicted() -> BTreeMap<String, Expect> {
    FIGURES
        .iter()
        .map(|f| {
            let e = if f.starts_with("saturation/") && *f != "saturation/long_values" {
                Expect::Absent("no value over 1024 bytes".into())
            } else {
                Expect::Unpredicted("test".into())
            };
            (f.to_string(), e)
        })
        .collect()
}

fn input<'a>(b: &'a [BlockValues], t: &'a [String], w: &'a Workload) -> SubjectInput<'a> {
    SubjectInput {
        lines: b.iter().map(|x| x.len() as u64).sum(),
        blocks: b,
        texts: t,
        workload: w,
    }
}

fn by_key(r: &SubjectReport) -> BTreeMap<String, f64> {
    r.figures
        .iter()
        .map(|f| (f.key.trim_start_matches("s1/").to_string(), f.value))
        .collect()
}

#[test]
fn measure_reports_each_figure_once_and_prunes_unseen_literals() {
    let b = blocks(8);
    let t = block_texts(&b).expect("texts");
    let spec = subject(unpredicted());
    let w = workload::draw(&b, &t, &spec.workload_params()).expect("draw");
    let truth = Truth {
        blocks: &b,
        equals_short_by_one: false,
        contains_drops_a_row: false,
    };
    let r = measure(&spec, &input(&b, &t, &w), &truth).expect("measure");
    assert!(r.checked.problems.is_empty(), "{:?}", r.checked);
    let f = by_key(&r);
    assert_eq!(f["lines"], 48.0);
    assert_eq!(f["blocks"], 8.0);
    // Two present and two absent evaluation equalities, all under 64 bytes.
    assert_eq!(f["eval_equals_bloom_testable"], 4.0);
    // The truth production keeps exactly the holders, so it has no false
    // positive; the summary is judged against that.
    assert_eq!(f["production/equals/false_positive_share"], 0.0);
    assert_eq!(f["bloom/equals/false_positive_share"], 0.0);
    // Evaluation literals were never seen by the build, and the summary
    // still prunes some of their non-holding blocks.
    for k in [
        "unseen/equals/false_positive_share",
        "unseen/like/false_positive_share",
    ] {
        assert!(f[k] < 1.0, "{k} = {}", f[k]);
    }
    assert!(f["summary/like/cells_tested_share"] < 1.0);
    assert!(f["contains/like/cells_tested_per_row"] > 0.0);
    assert_eq!(f["saturation/long_values"], 0.0);
    assert!(!f.contains_key("saturation/blocks"));
    assert_eq!(r.decision.saturation, Saturation::Untested);
}

/// The truth, except that equality keeps one block beyond the holders.
struct OneExtraEquals<'a>(Truth<'a>);

impl Production for OneExtraEquals<'_> {
    fn equals_blocks_kept(&self, literal: &str) -> Result<u64, String> {
        Ok(self.0.equals_blocks_kept(literal)? + 1)
    }

    fn contains(&self, literal: &str, keep: Option<&[bool]>) -> Result<ContainsRun, String> {
        self.0.contains(literal, keep)
    }
}

fn lit(kind: Kind, present: bool, text: &str) -> Literal {
    Literal {
        kind,
        present,
        text: text.into(),
    }
}

#[test]
fn measure_reports_hand_computed_shares() {
    // 3-grams per block:
    //   b0 alp lph pha ome meg ega
    //   b1 alp lph pha hab abe bet hom ome meg egs
    //   b2 ome meg ega
    //   b3 del elt lta ome meg ega
    // The training grams are alp lph pha hab abe bet ome meg ega. ome and meg
    // are in every block, so they are not learned and never prune; the other
    // seven are learned. Every literal below uses only training grams, so a
    // block keeps it exactly when the block holds all of its grams.
    let b: Vec<BlockValues> = [
        &["alpha", "omega"][..],
        &["alphabet", "home", "megs"],
        &["omega"],
        &["delta", "omeg", "mega"],
    ]
    .iter()
    .map(|vs| vs.iter().map(|v| v.as_bytes().to_vec()).collect())
    .collect();
    let t = block_texts(&b).expect("texts");
    let w = Workload {
        training: vec![
            // Holder b1; kept b1; non-holders b0 b2 b3 dropped: 0 of 3.
            lit(Kind::Equals, true, "alphabet"),
            // Holder b0; kept b0 b1; non-holders b1 kept, b2 b3 dropped: 1 of 3.
            lit(Kind::Equals, true, "alpha"),
            // Holders b0 b2; kept b0 b2 b3; non-holders b1 dropped, b3 kept: 1 of 2.
            lit(Kind::Like, true, "omega"),
        ],
        evaluation: vec![
            // Holder b3; kept b0 b2 b3; non-holders b0 b2 kept, b1 dropped: 2 of 3.
            lit(Kind::Equals, true, "mega"),
            // No holder; kept b1: 1 of 4.
            lit(Kind::Equals, false, "lphabe"),
            // Holders b0 b2 b3; kept all; non-holder b1 kept: 1 of 1.
            lit(Kind::Like, true, "omeg"),
            // Holder b1; kept b1; non-holders b0 b2 b3 dropped: 0 of 3.
            lit(Kind::Like, true, "phab"),
        ],
    };
    let spec = subject(unpredicted());
    let production = OneExtraEquals(Truth {
        blocks: &b,
        equals_short_by_one: false,
        contains_drops_a_row: false,
    });
    let r = measure(&spec, &input(&b, &t, &w), &production).expect("measure");
    assert!(r.checked.problems.is_empty(), "{:?}", r.checked);
    let f = by_key(&r);
    assert_eq!(f["learned_bits"], 7.0);
    // Over non-holder pairs only: a holder pair is never in the denominator.
    assert_eq!(f["seen/equals/false_positive_share"], 1.0 / 6.0);
    assert_eq!(f["seen/like/false_positive_share"], 1.0 / 2.0);
    assert_eq!(f["unseen/equals/false_positive_share"], 3.0 / 7.0);
    assert_eq!(f["unseen/like/false_positive_share"], 1.0 / 4.0);
    assert_eq!(f["unseen/equals_absent/false_positive_share"], 1.0 / 4.0);
    assert_eq!(
        f["summary/equals_bloom_testable/false_positive_share"],
        3.0 / 7.0
    );
    // Production keeps one non-holder per equality literal: "mega" has 3
    // non-holders, "lphabe" 4, so 2 of 7 (not 2 of the 8 literal-block pairs).
    assert_eq!(f["production/equals/false_positive_share"], 2.0 / 7.0);
    assert_eq!(f["bloom/equals/false_positive_share"], 2.0 / 7.0);
    assert_eq!(f["eval_equals_bloom_testable"], 2.0);
    // Cells: 9 values. "omeg" scans all 9 under both; "phab" 9 in full and
    // b1's 3 under the summary.
    assert_eq!(f["summary/like/cells_tested_share"], 12.0 / 18.0);
    assert_eq!(f["contains/like/cells_tested_per_row"], 1.0);
}

#[test]
fn measure_learns_grams_only_from_the_training_literals() {
    let b = blocks(8);
    let t = block_texts(&b).expect("texts");
    let spec = subject(unpredicted());
    let w = workload::draw(&b, &t, &spec.workload_params()).expect("draw");
    let truth = Truth {
        blocks: &b,
        equals_short_by_one: false,
        contains_drops_a_row: false,
    };
    let grams_of = |set: &[Literal]| {
        let mut out = std::collections::BTreeSet::new();
        for l in set {
            summary::for_each_gram(l.text.as_bytes(), spec.gram_len, |k| {
                out.insert(k);
            });
        }
        out
    };
    let training = grams_of(&w.training);
    let evaluation_only: Vec<u64> = grams_of(&w.evaluation)
        .difference(&training)
        .copied()
        .collect();
    // An evaluation-only gram absent from some block would be learned if the
    // evaluation literals fed the build.
    assert!(
        evaluation_only.iter().any(|&k| b.iter().any(|block| {
            let mut has = false;
            for v in block {
                summary::for_each_gram(v, spec.gram_len, |g| has |= g == k);
            }
            !has
        })),
        "the fixture cannot tell training from evaluation grams"
    );
    let r = measure(&spec, &input(&b, &t, &w), &truth).expect("measure");
    assert!(!r.layout.learned.is_empty());
    for k in &r.layout.learned {
        assert!(
            training.contains(k),
            "learned gram {k:#x} is not a training gram"
        );
    }
    assert_eq!(by_key(&r)["candidate_grams"], training.len() as f64);
}

#[test]
fn measure_refuses_a_contains_baseline_that_disagrees_with_the_truth() {
    let b = blocks(8);
    let t = block_texts(&b).expect("texts");
    let spec = subject(unpredicted());
    let w = workload::draw(&b, &t, &spec.workload_params()).expect("draw");
    let truth = Truth {
        blocks: &b,
        equals_short_by_one: false,
        contains_drops_a_row: true,
    };
    let err = measure(&spec, &input(&b, &t, &w), &truth).unwrap_err();
    assert!(err.contains("disagrees"), "{err}");
}

#[test]
fn measure_refuses_an_equality_baseline_below_the_holders() {
    let b = blocks(8);
    let t = block_texts(&b).expect("texts");
    let spec = subject(unpredicted());
    let w = workload::draw(&b, &t, &spec.workload_params()).expect("draw");
    let truth = Truth {
        blocks: &b,
        equals_short_by_one: true,
        contains_drops_a_row: false,
    };
    let err = measure(&spec, &input(&b, &t, &w), &truth).unwrap_err();
    assert!(err.contains("fewer than"), "{err}");
}

#[test]
fn measure_runs_the_saturation_arm_on_long_values() {
    let mut b = blocks(8);
    let long = format!("long body {} end", "request served ".repeat(80));
    assert!(long.len() > SATURATION_BYTES);
    b[3].push(long.into_bytes());
    let t = block_texts(&b).expect("texts");
    let mut figures = unpredicted();
    for f in FIGURES.iter().filter(|f| f.starts_with("saturation/")) {
        figures.insert(f.to_string(), Expect::Unpredicted("test".into()));
    }
    let spec = subject(figures);
    let w = workload::draw(&b, &t, &spec.workload_params()).expect("draw");
    let truth = Truth {
        blocks: &b,
        equals_short_by_one: false,
        contains_drops_a_row: false,
    };
    let r = measure(&spec, &input(&b, &t, &w), &truth).expect("measure");
    assert!(r.checked.problems.is_empty(), "{:?}", r.checked);
    let f = by_key(&r);
    assert_eq!(f["saturation/long_values"], 1.0);
    assert_eq!(f["saturation/blocks"], 1.0);
    assert_ne!(r.decision.saturation, Saturation::Untested);
}

#[test]
fn long_value_blocks_cut_by_records_and_bytes() {
    let long = vec![b'x'; SATURATION_BYTES + 1];
    let b: Vec<BlockValues> = vec![
        vec![long.clone(), b"short".to_vec(), long.clone()],
        vec![long.clone(), long.clone()],
    ];
    let cut = long_value_blocks(&b, 3);
    assert_eq!(cut.iter().map(Vec::len).collect::<Vec<_>>(), vec![3, 1]);
    let huge = vec![b'y'; BLOCK_MAX_BYTES / 2 + 1];
    let cut = long_value_blocks(&[vec![huge.clone(), huge]], 100);
    assert_eq!(cut.len(), 2);
}

#[test]
fn check_figures_separates_problems_from_misses() {
    let mut expect = BTreeMap::new();
    expect.insert(
        "s/a".to_string(),
        Expect::Predicted {
            predict: 1.0,
            band: [0.0, 2.0],
        },
    );
    expect.insert(
        "s/b".to_string(),
        Expect::Predicted {
            predict: 1.0,
            band: [0.0, 2.0],
        },
    );
    expect.insert("s/c".to_string(), Expect::Absent("why".into()));
    expect.insert("s/d".to_string(), Expect::Unpredicted("why".into()));
    expect.insert("s/e".to_string(), Expect::Unpredicted("why".into()));
    let fig = |k: &str, v: f64| Figure {
        key: k.into(),
        value: v,
    };
    let figures = vec![
        fig("s/a", 1.5),
        fig("s/b", 3.0),
        fig("s/c", 0.0),
        fig("s/d", 9.0),
        fig("s/d", 9.0),
        fig("s/z", 1.0),
    ];
    let c = check_figures(&figures, &expect);
    assert_eq!(c.misses.len(), 1);
    assert!(c.misses[0].starts_with("s/b"), "{:?}", c.misses);
    let joined = c.problems.join("|");
    for want in [
        "s/c: reported",
        "s/d: present 2 times",
        "s/e: missing",
        "s/z: not pre-registered",
    ] {
        assert!(joined.contains(want), "{want} not in {joined}");
    }
    // A NaN figure is outside every band.
    let c = check_figures(&[fig("s/a", f64::NAN)], &expect);
    assert_eq!(c.misses.len(), 1);
}

fn decision_input_with(
    eq: f64,
    like: f64,
    bytes: f64,
    cpu: f64,
    sat: Option<f64>,
) -> DecisionInput {
    DecisionInput {
        subject: "s".into(),
        unseen_equals_fp: Some(eq),
        unseen_like_fp: Some(like),
        summary_bytes_share: Some(bytes),
        build_cpu_over_zstd: Some(cpu),
        saturation_fp: sat,
    }
}

#[test]
fn decide_applies_every_bar_and_keeps_saturation_separate() {
    let go = decide(&decision_input_with(0.1, 0.05, 0.01, 2.0, Some(0.9)));
    assert!(go.go, "{}", go.line);
    assert_eq!(go.saturation, Saturation::BodyIneligible);
    for d in [
        decision_input_with(0.11, 0.0, 0.0, 0.0, None),
        decision_input_with(0.0, 0.11, 0.0, 0.0, None),
        decision_input_with(0.0, 0.0, 0.011, 0.0, None),
        decision_input_with(0.0, 0.0, 0.0, 2.1, None),
        decision_input_with(f64::NAN, 0.0, 0.0, 0.0, None),
    ] {
        let out = decide(&d);
        assert!(!out.go, "{}", out.line);
        assert_eq!(out.saturation, Saturation::Untested);
    }
    let missing = DecisionInput {
        unseen_like_fp: None,
        ..decision_input_with(0.0, 0.0, 0.0, 0.0, Some(0.3))
    };
    let out = decide(&missing);
    assert!(!out.go);
    assert!(out.line.contains("unseen_like_fp=missing"), "{}", out.line);
    assert_eq!(out.saturation, Saturation::BodyEligible);
}

#[test]
fn decision_input_reads_only_figures_reported_once() {
    let fig = |k: &str, v: f64| Figure {
        key: format!("s/{k}"),
        value: v,
    };
    let figures = vec![
        fig("unseen/equals/false_positive_share", 0.0),
        fig("unseen/like/false_positive_share", 0.0),
        fig("unseen/like/false_positive_share", 0.0),
        fig("summary_bytes_share", 0.0),
    ];
    let d = decision_input("s", &figures, None);
    assert_eq!(d.unseen_equals_fp, Some(0.0));
    assert_eq!(d.unseen_like_fp, None);
    assert_eq!(d.build_cpu_over_zstd, None);
}

#[test]
fn overall_needs_every_subject_once_go_and_without_problems() {
    let saved = |s: &str, go: bool, problems: usize| SavedDecision {
        decision: Decision {
            subject: s.into(),
            go,
            saturation: Saturation::Untested,
            line: String::new(),
        },
        problems,
    };
    let names = vec!["a".to_string(), "b".to_string()];
    assert!(overall(&names, &[saved("a", true, 0), saved("b", true, 0)]).go);
    for set in [
        vec![saved("a", true, 0)],
        vec![saved("a", true, 0), saved("b", false, 0)],
        vec![saved("a", true, 0), saved("b", true, 1)],
        vec![
            saved("a", true, 0),
            saved("b", true, 0),
            saved("b", true, 0),
        ],
        vec![
            saved("a", true, 0),
            saved("b", true, 0),
            saved("c", true, 0),
        ],
    ] {
        assert!(!overall(&names, &set).go);
    }
    assert!(!overall(&[], &[]).go);
}

#[test]
fn saved_decision_reads_back_a_written_report() {
    let b = blocks(8);
    let t = block_texts(&b).expect("texts");
    let spec = subject(unpredicted());
    let w = workload::draw(&b, &t, &spec.workload_params()).expect("draw");
    let truth = Truth {
        blocks: &b,
        equals_short_by_one: false,
        contains_drops_a_row: false,
    };
    let r = measure(&spec, &input(&b, &t, &w), &truth).expect("measure");
    let json = serde_json::to_string(&r).expect("json");
    let back = saved_decision(&json).expect("read back");
    assert_eq!(back.decision, r.decision);
    assert_eq!(back.problems, 0);
}
