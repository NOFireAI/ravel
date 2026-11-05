#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::spec::{self, Band, ColumnSpec};
use super::*;
use crate::logbody::grams::{BlockText, GramIndex, Params, Pattern};

const BLOCKS: usize = 10;
const LINES_PER_BLOCK: usize = 12;

const BACKGROUND: [&str; 12] = [
    "ivy", "oak", "elm", "fox", "cat", "dog", "owl", "bee", "ant", "cow", "pig", "hen",
];

/// `(value, blocks it is planted in)`. No two entries share a block, so each
/// block gets at most one overwritten row.
const PLANTED: [(&str, &[usize]); 4] = [
    ("redwood", &[2, 5]),
    ("moonlight", &[1, 7]),
    ("saturn", &[3]),
    ("midnight", &[4, 6]),
];

const SPEC: &str = r#"
schema = 1

[[corpus]]
name = "syn"
source = "string_fingerprint::tests"
block_records = 12

[[corpus.column]]
name = "col"
max_gram = 6
df_threshold = 0.5
budget_share = 0.2
sample_size = 2
long_value_bytes = 5

[corpus.column.bands]
blocks = [10, 10]
compressed_bytes = [1, 1e9]
zstd_cpu_s = [0, 10]
fingerprint_index_bytes = [0, 1e6]
fingerprint_index_bytes_share = [0, 1]
fingerprint_build_cpu_s = [0, 10]
fingerprint_build_cpu_over_zstd = [0, 1e12]
fingerprint_grams_selected = [0, 1000]
saturation_blocks_long = [7, 7]
saturation_false_positive_share = [0, 1]

[[corpus.column.predicate]]
id = "p_redwood"
kind = "equals"
set = "seen"
pattern = "redwood"
reason = "trains the summary on an equals literal"
bands = { true_holders = [2, 2], fingerprint = { blocks_surviving = [2, 10], survival_share = [0.2, 1], blocks_missed = [0, 0], false_positive_share = [0, 1] }, bloom = { blocks_surviving = [0, 10], survival_share = [0, 1], blocks_missed = [0, 10], false_positive_share = [0, 1] } }

[[corpus.column.predicate]]
id = "p_moon"
kind = "contains"
set = "seen"
pattern = "moon"
reason = "trains the summary on a contains literal embedded in a longer token"
bands = { true_holders = [2, 2], fingerprint = { blocks_surviving = [2, 10], survival_share = [0.2, 1], blocks_missed = [0, 0], false_positive_share = [0, 1] }, bloom = { blocks_surviving = [0, 10], survival_share = [0, 1], blocks_missed = [0, 10], false_positive_share = [0, 1] }, contains = { cells_tested = [0, 120], cells_total = [120, 120], cells_tested_share = [0, 1] } }

[[corpus.column.predicate]]
id = "p_saturn"
kind = "equals"
set = "unseen"
pattern = "saturn"
reason = "out-of-sample equals literal never seen while training"
bands = { true_holders = [1, 1], fingerprint = { blocks_surviving = [1, 10], survival_share = [0.1, 1], blocks_missed = [0, 0], false_positive_share = [0, 1] }, bloom = { blocks_surviving = [0, 10], survival_share = [0, 1], blocks_missed = [0, 10], false_positive_share = [0, 1] } }

[[corpus.column.predicate]]
id = "p_night"
kind = "contains"
set = "unseen"
pattern = "night"
reason = "out-of-sample contains literal embedded in a longer token, never seen while training"
bands = { true_holders = [2, 2], fingerprint = { blocks_surviving = [2, 10], survival_share = [0.2, 1], blocks_missed = [0, 0], false_positive_share = [0, 1] }, bloom = { blocks_surviving = [0, 10], survival_share = [0, 1], blocks_missed = [0, 10], false_positive_share = [0, 1] }, contains = { cells_tested = [0, 120], cells_total = [120, 120], cells_tested_share = [0, 1] } }

[[corpus.column.predicate]]
id = "p_absent"
kind = "equals"
set = "unseen"
pattern = "zzzzznotfound"
reason = "never occurs anywhere; exercises the zero-holder path for both arms"
bands = { true_holders = [0, 0], fingerprint = { blocks_surviving = [0, 10], survival_share = [0, 1], blocks_missed = [0, 0], false_positive_share = [0, 1] }, bloom = { blocks_surviving = [0, 10], survival_share = [0, 1], blocks_missed = [0, 0], false_positive_share = [0, 1] } }
"#;

fn synthetic_blocks() -> Vec<BlockText> {
    (0..BLOCKS)
        .map(|b| {
            let mut lines: Vec<String> = (0..LINES_PER_BLOCK)
                .map(|i| BACKGROUND[i % BACKGROUND.len()].to_string())
                .collect();
            let mut at = BACKGROUND.len() - 1;
            for (value, holders) in PLANTED {
                if holders.contains(&b) {
                    lines[at] = value.to_string();
                    at = at.saturating_sub(1);
                }
            }
            BlockText::from_lines(&lines).expect("no 0x0A")
        })
        .collect()
}

fn synthetic_spec() -> ColumnSpec {
    let s = spec::parse(SPEC).expect("spec");
    s.corpus[0].column[0].clone()
}

fn raw_bytes(blocks: &[BlockText]) -> u64 {
    blocks.iter().map(|b| b.bytes().len() as u64).sum()
}

fn synthetic_report() -> ColumnReport {
    let blocks = synthetic_blocks();
    let input = ColumnInput {
        blocks: &blocks,
        compressed_bytes: raw_bytes(&blocks),
        zstd_level: 3,
    };
    measure("syn", &synthetic_spec(), &input).expect("measure")
}

fn value(r: &ColumnReport, key: &str) -> f64 {
    let hits: Vec<f64> = r
        .figures
        .iter()
        .filter(|f| f.key == format!("syn/col/{key}"))
        .map(|f| f.value)
        .collect();
    assert_eq!(hits.len(), 1, "{key}: {:?}", r.figures);
    hits[0]
}

#[test]
fn report_figures_present_once_and_banded() {
    let r = synthetic_report();
    assert!(r.problems.is_empty(), "{:?}", r.problems);
    let keys: std::collections::BTreeSet<&str> = r.figures.iter().map(|f| f.key.as_str()).collect();
    assert_eq!(keys.len(), r.figures.len());
    assert_eq!(keys, r.bands.keys().map(String::as_str).collect());
    assert!(check_figures(&r.figures, &r.bands).is_empty());

    let key = "syn/col/p_redwood/fingerprint/blocks_surviving";
    let removed: Vec<Figure> = r.figures.iter().filter(|f| f.key != key).cloned().collect();
    assert_eq!(check_figures(&removed, &r.bands).len(), 1);

    let mut duplicated = r.figures.clone();
    duplicated.push(Figure {
        key: key.to_string(),
        value: 0.0,
    });
    assert_eq!(
        check_figures(&duplicated, &r.bands),
        [format!("{key}: present 2 times")]
    );

    let mut unbanded = r.figures.clone();
    unbanded.push(Figure {
        key: "syn/col/stray".to_string(),
        value: 0.0,
    });
    assert_eq!(
        check_figures(&unbanded, &r.bands),
        ["syn/col/stray: no band".to_string()]
    );

    let lines = r.lines();
    assert_eq!(lines.len(), r.figures.len() + 2);
    assert!(
        lines[0].starts_with("string_fingerprint params "),
        "{}",
        lines[0]
    );
    assert!(lines.iter().all(|l| !l.ends_with("MISS")), "{lines:?}");
    assert!(
        lines
            .last()
            .unwrap()
            .starts_with("string_fingerprint decision syn/col: ")
    );
}

#[test]
fn ground_truth_matches_the_planted_rows() {
    let r = synthetic_report();
    assert_eq!(value(&r, "p_redwood/true_holders"), 2.0);
    assert_eq!(value(&r, "p_moon/true_holders"), 2.0);
    assert_eq!(value(&r, "p_saturn/true_holders"), 1.0);
    assert_eq!(value(&r, "p_night/true_holders"), 2.0);
    assert_eq!(value(&r, "p_absent/true_holders"), 0.0);
    assert_eq!(value(&r, "saturation/blocks_long"), 7.0);
}

#[test]
fn fingerprint_arm_never_drops_a_true_holder() {
    let r = synthetic_report();
    for id in ["p_redwood", "p_moon", "p_saturn", "p_night", "p_absent"] {
        assert_eq!(
            value(&r, &format!("{id}/fingerprint/blocks_missed")),
            0.0,
            "{id}"
        );
    }
}

#[test]
fn bloom_arm_misses_a_literal_embedded_in_a_longer_token() {
    // "moon" and "night" occur only inside "moonlight"/"midnight", which
    // tokenize to one whole token each; production's word-token bloom never
    // sees "moon" or "night" as a token on their own, so it must miss every
    // block that only holds them this way.
    let r = synthetic_report();
    assert_eq!(value(&r, "p_moon/bloom/blocks_missed"), 2.0);
    assert_eq!(value(&r, "p_night/bloom/blocks_missed"), 2.0);
    // An equals predicate's literal is the whole row: the bloom never misses it.
    assert_eq!(value(&r, "p_redwood/bloom/blocks_missed"), 0.0);
    assert_eq!(value(&r, "p_saturn/bloom/blocks_missed"), 0.0);
}

#[test]
fn contains_baseline_is_a_pruned_or_full_scan_proxy() {
    let r = synthetic_report();
    let total = value(&r, "p_moon/contains/cells_total");
    assert_eq!(total, (BLOCKS * LINES_PER_BLOCK) as f64);
    let tested = value(&r, "p_moon/contains/cells_tested");
    assert!(tested <= total, "{tested} > {total}");
    assert_eq!(
        value(&r, "p_moon/contains/cells_tested_share"),
        tested / total
    );
}

#[test]
fn measure_refuses_a_bloom_applicability_mismatch() {
    // "night time" is two tokens: no longer a single-token pattern, but the
    // pre-registration still carries bloom bands for it.
    let bad = SPEC.replace("pattern = \"night\"", "pattern = \"night time\"");
    let bad = bad.replace(
        "reason = \"out-of-sample contains literal embedded in a longer token, never seen while training\"",
        "reason = \"two words, still >= 3 literal bytes\"",
    );
    let s = spec::parse(&bad).expect("spec");
    let col = s.corpus[0].column[0].clone();
    let blocks = synthetic_blocks();
    let input = ColumnInput {
        blocks: &blocks,
        compressed_bytes: raw_bytes(&blocks),
        zstd_level: 3,
    };
    let err = measure("syn", &col, &input).unwrap_err();
    assert!(err.contains("p_night"), "{err}");
    assert!(err.contains("not a single token"), "{err}");
}

#[test]
fn measure_refuses_an_unpackable_gram_length() {
    let mut col = synthetic_spec();
    col.max_gram = 8;
    let blocks = synthetic_blocks();
    let input = ColumnInput {
        blocks: &blocks,
        compressed_bytes: raw_bytes(&blocks),
        zstd_level: 3,
    };
    assert!(measure("syn", &col, &input).is_err());
}

#[test]
fn single_token_bytes_only_accepts_one_whole_alnum_token() {
    assert_eq!(single_token_bytes("redwood"), Some(b"redwood".to_vec()));
    assert_eq!(single_token_bytes("RedWood"), Some(b"redwood".to_vec()));
    assert_eq!(single_token_bytes("red wood"), None);
    assert_eq!(single_token_bytes("red-wood"), None);
    // Truncation at the tokenizer's internal byte cap breaks the equality
    // check without this module needing to know the cap's value.
    assert_eq!(single_token_bytes(&"a".repeat(200)), None);
}

fn input(unseen: &[(&str, Option<f64>)], bytes: Option<f64>, cpu: Option<f64>) -> DecisionInput {
    DecisionInput {
        corpus: "syn".to_string(),
        column: "col".to_string(),
        unseen_false_positive_share: unseen.iter().map(|(id, s)| (id.to_string(), *s)).collect(),
        index_bytes_share: bytes,
        build_cpu_over_zstd: cpu,
        saturation_false_positive_share: Some(0.0),
    }
}

#[test]
fn decision_go_on_constructed_figures() {
    let d = decide(&input(
        &[("u1", Some(0.01)), ("u2", Some(0.05))],
        Some(0.005),
        Some(1.5),
    ));
    assert!(d.go, "{}", d.line);
    assert!(
        d.line
            .starts_with("string_fingerprint decision syn/col: GO; ")
    );
    assert!(d.line.ends_with("fails: none"), "{}", d.line);
}

#[test]
fn decision_no_go_on_survival_mean() {
    let d = decide(&input(
        &[("u1", Some(0.01)), ("u2", Some(0.5))],
        Some(0.005),
        Some(1.5),
    ));
    assert!(!d.go);
    assert!(d.line.ends_with("fails: survival"), "{}", d.line);
}

#[test]
fn decision_no_go_on_bytes() {
    let d = decide(&input(&[("u1", Some(0.0))], Some(0.02), Some(1.0)));
    assert!(!d.go);
    assert!(d.line.ends_with("fails: bytes"), "{}", d.line);
}

#[test]
fn decision_no_go_on_cpu() {
    let d = decide(&input(&[("u1", Some(0.0))], Some(0.005), Some(2.1)));
    assert!(!d.go);
    assert!(d.line.ends_with("fails: cpu"), "{}", d.line);
}

#[test]
fn decision_reports_body_eligible_informationally() {
    let mut d = input(&[("u1", Some(0.0))], Some(0.005), Some(1.0));
    d.saturation_false_positive_share = Some(0.9);
    let dec = decide(&d);
    // A saturated summary still can go, body_eligible only annotates the line.
    assert!(dec.go, "{}", dec.line);
    assert!(dec.line.contains("body_eligible=false"), "{}", dec.line);
}

/// The wrong implementation: a missing figure reads as 0, clearing every bar.
fn check_defaulting_to_zero(figures: &[Figure], bands: &BTreeMap<String, Band>) -> Vec<String> {
    bands
        .iter()
        .filter_map(|(key, band)| {
            let v = figures
                .iter()
                .find(|f| &f.key == key)
                .map_or(0.0, |f| f.value);
            (!(band[0] <= v && v <= band[1])).then(|| format!("{key}: {v} outside band"))
        })
        .collect()
}

#[test]
fn a_missing_figure_does_not_default_to_zero() {
    let r = synthetic_report();
    let key = "syn/col/p_redwood/fingerprint/false_positive_share";
    let removed: Vec<Figure> = r.figures.iter().filter(|f| f.key != key).cloned().collect();
    assert!(check_defaulting_to_zero(&removed, &r.bands).is_empty());
    assert_eq!(check_figures(&removed, &r.bands).len(), 1);

    // In the decision a missing unseen figure fails the leg, it does not clear.
    let missing = input(&[("u1", None), ("u2", Some(0.0))], Some(0.005), Some(1.0));
    let mut defaulted = missing.clone();
    for (_, s) in &mut defaulted.unseen_false_positive_share {
        *s = Some(s.unwrap_or(0.0));
    }
    assert!(decide(&defaulted).go);
    let real = decide(&missing);
    assert!(!real.go);
    assert!(real.line.contains("u1=missing"), "{}", real.line);
}

#[test]
fn decision_input_reads_only_unseen_predicates() {
    let r = synthetic_report();
    let d = decision_input("syn", &synthetic_spec(), &r.figures);
    let ids: Vec<&str> = d
        .unseen_false_positive_share
        .iter()
        .map(|(id, _)| id.as_str())
        .collect();
    assert_eq!(ids, ["p_saturn", "p_night", "p_absent"]);
    assert!(
        d.unseen_false_positive_share
            .iter()
            .all(|(_, s)| s.is_some())
    );
}

fn saved(corpus: &str, go: bool, problems: usize) -> SavedDecision {
    SavedDecision {
        decision: Decision {
            corpus: corpus.to_string(),
            go,
            line: String::new(),
        },
        problems,
    }
}

#[test]
fn overall_needs_every_pre_registered_column_go_and_clean() {
    let names = ["a/x".to_string(), "b/y".to_string()];
    let d = overall(&names, &[saved("a/x", true, 0), saved("b/y", true, 0)]);
    assert!(d.go);

    let d = overall(&names, &[saved("a/x", true, 0)]);
    assert!(!d.go);
    assert!(d.line.contains("b/y=missing"), "{}", d.line);

    let d = overall(&names, &[saved("a/x", true, 0), saved("b/y", false, 0)]);
    assert!(!d.go);

    let d = overall(&names, &[saved("a/x", true, 0), saved("b/y", true, 1)]);
    assert!(!d.go);
    assert!(d.line.contains("figure problems"), "{}", d.line);

    let d = overall(
        &names,
        &[
            saved("a/x", true, 0),
            saved("a/x", true, 0),
            saved("b/y", true, 0),
        ],
    );
    assert!(!d.go);
    assert!(d.line.contains("a/x=2 reports"));

    let d = overall(
        &names,
        &[
            saved("a/x", true, 0),
            saved("b/y", true, 0),
            saved("z/z", true, 0),
        ],
    );
    assert!(!d.go);
    assert!(d.line.ends_with("z/z=not pre-registered"));

    assert!(!overall(&[], &[]).go);
}

#[test]
fn saved_decision_reads_a_written_report() {
    #[derive(serde::Serialize)]
    struct Written<'a> {
        string_fingerprint: &'a ColumnReport,
    }
    let r = synthetic_report();
    let json = serde_json::to_string(&Written {
        string_fingerprint: &r,
    })
    .unwrap();
    let s = saved_decision(&json).unwrap();
    assert_eq!(s.decision, r.decision);
    assert_eq!(s.problems, 0);
    assert!(saved_decision("{}").is_err());
}

#[test]
fn spec_validation_refuses_a_bad_pre_registration() {
    assert!(spec::parse(&SPEC.replace("schema = 1", "schema = 2")).is_err());
    assert!(spec::parse(&SPEC.replace("max_gram = 6", "max_gram = 8")).is_err());
    assert!(spec::parse(&SPEC.replace("df_threshold = 0.5", "df_threshold = 0")).is_err());
    assert!(spec::parse(&SPEC.replace("budget_share = 0.2", "budget_share = 1.5")).is_err());
    assert!(spec::parse(&SPEC.replace("sample_size = 2", "sample_size = 0")).is_err());
    assert!(spec::parse(&SPEC.replace("long_value_bytes = 5", "long_value_bytes = 0")).is_err());
    assert!(
        spec::parse(&SPEC.replace(
            "reason = \"trains the summary on an equals literal\"",
            "reason = \" \""
        ))
        .is_err()
    );
    assert!(spec::parse(&SPEC.replace("pattern = \"saturn\"", "pattern = \"sa\"")).is_err());
    assert!(spec::parse(&SPEC.replace("id = \"p_absent\"", "id = \"p_saturn\"")).is_err());
    assert!(spec::parse(&SPEC.replace("id = \"p_absent\"", "id = \"p/absent\"")).is_err());
    // Every seen or unseen predicate is required.
    assert!(
        spec::parse(
            &SPEC.replace("set = \"seen\"", "set = \"unseen\"").replacen(
                "set = \"unseen\"",
                "set = \"unseen\"",
                1
            )
        )
        .is_err()
    );
    // Contains bands must track the predicate's kind.
    assert!(
        spec::parse(&SPEC.replace(
            "kind = \"contains\"\nset = \"seen\"",
            "kind = \"equals\"\nset = \"seen\""
        ))
        .is_err()
    );
    assert!(
        spec::parse(&SPEC.replace(
            "reason = \"trains the summary on an equals literal\"",
            "reason = \"trains the summary on an equals literal\"\nextra = 1"
        ))
        .is_err()
    );
}

fn ascii_token() -> impl Strategy<Value = String> {
    "[a-z]{3,8}"
}

fn block_rows() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(ascii_token(), 1..=6)
}

proptest! {
    /// A block that truly holds a predicate (ground truth, by direct
    /// substring/equality check) is never dropped by the fingerprint arm,
    /// whether or not the predicate trained the index (an unseen query is
    /// checked the identical way: by which of its own literal substrings the
    /// index kept, never by whether the index has seen the query itself).
    #[test]
    fn fingerprint_arm_never_drops_a_block_it_truly_holds(
        blocks_rows in prop::collection::vec(block_rows(), 1..=8),
        workload_literals in prop::collection::vec(ascii_token(), 1..=3),
        query_literal in ascii_token(),
    ) {
        let blocks: Vec<BlockText> = blocks_rows
            .iter()
            .map(|rows| BlockText::from_lines(rows).expect("no 0x0A"))
            .collect();
        let workload: Vec<Pattern> = workload_literals
            .iter()
            .filter_map(|l| Pattern::parse(l))
            .collect();
        let query = Pattern::parse(&query_literal).expect("3..=8 lowercase ascii has a literal run");
        let index = GramIndex::build(&blocks, &workload, u64::MAX, &Params::default());
        let survivors = index.survivors(&query);
        for (b, block) in blocks.iter().enumerate() {
            if query.matches_block(block) {
                prop_assert!(survivors[b], "block {b} truly holds {query_literal:?} but was dropped");
            }
        }
    }
}

#[test]
fn string_fingerprint_inline_spec_is_a_valid_pre_registration() {
    let s = spec::parse(SPEC).expect("SPEC");
    assert_eq!(s.columns(), ["syn/col".to_string()]);
}

#[test]
fn needles_fixture_is_a_valid_pre_registration() {
    let text = include_str!("../../fixtures/string_fingerprint/needles.toml");
    let s = spec::parse(text).expect("needles.toml");
    let names: Vec<&str> = s.corpus.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["hdfs", "bgl", "ssh", "apache", "clickbench"]);
    assert_eq!(
        s.columns(),
        [
            "hdfs/body",
            "bgl/body",
            "ssh/body",
            "apache/body",
            "clickbench/URL",
            "clickbench/Title",
            "clickbench/SearchPhrase",
            "clickbench/Referer",
        ]
    );
    for c in &s.corpus {
        // LogHub corpora exercise 2 seen + 2 unseen (one single-token
        // bloom-applicable, one multi-word bloom-inapplicable, per split);
        // ClickBench's columns exercise 1 seen + 1 unseen each.
        let want = if c.name == "clickbench" { 1 } else { 2 };
        for col in &c.column {
            let seen = col
                .predicate
                .iter()
                .filter(|p| p.set == spec::Split::Seen)
                .count();
            let unseen = col
                .predicate
                .iter()
                .filter(|p| p.set == spec::Split::Unseen)
                .count();
            assert_eq!(seen, want, "{}/{} seen predicates", c.name, col.name);
            assert_eq!(unseen, want, "{}/{} unseen predicates", c.name, col.name);
        }
    }
}
