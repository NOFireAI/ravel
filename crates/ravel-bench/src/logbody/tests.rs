#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::grams::{self, BlockText, GramIndex, Params, Pattern, TrigramSets};
use super::spec::{self, CorpusSpec};
use super::*;

const BLOCKS: usize = 20;
const LINES_PER_BLOCK: usize = 40;

/// Every trigram of `Exception writing block` and every 3-to-6-byte piece of
/// `7f3a9c2e` is in every block; the needle lines are planted.
const BACKGROUND: [&str; 8] = [
    "Exception in receive",
    "begin writing",
    "sending block",
    "x 7f3a9c y",
    "x f3a9c2 y",
    "x 3a9c2e y",
    "served request ok",
    "heartbeat ok",
];

const PLANTED: [(&str, &[usize]); 6] = [
    ("Exception writing block", &[3, 11]),
    ("Deleting stale replica now", &[5]),
    (
        "Verification succeeded for blk",
        &[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ],
    ),
    ("lost heartbeat from peer 4", &[7]),
    ("disk quota exceeded", &[9, 10]),
    ("id 7f3a9c2e seen", &[13]),
];

const SPEC: &str = r#"
schema = 1

[[corpus]]
name = "syn"
file = "synthetic"
source = "logbody::tests"
block_records = 40

[corpus.bands]
lines = [800, 800]
blocks = [20, 20]
compressed_bytes = [1, 1e9]
zstd_cpu_s = [0, 10]

[corpus.bands.trigram]
index_bytes = [1, 1e9]
index_bytes_share = [0, 100]
build_cpu_s = [0, 10]
build_cpu_over_zstd = [0, 1e12]

[corpus.bands.selected]
index_bytes = [1, 1e9]
index_bytes_share = [0, 0.05]
build_cpu_s = [0, 10]
build_cpu_over_zstd = [0, 1e12]
grams_selected = [1, 1000]

[corpus.bands.template.trigram]
blocks_surviving = [0, 20]
survival_share = [0, 1]
false_positives = [0, 20]
false_positive_share = [0, 1]

[corpus.bands.template.selected]
blocks_surviving = [0, 20]
survival_share = [0, 1]
false_positives = [0, 20]
false_positive_share = [0, 1]

[corpus.bands.id.trigram]
blocks_surviving = [20, 20]
survival_share = [1, 1]
false_positives = [19, 19]
false_positive_share = [0.9, 1]

[corpus.bands.id.selected]
blocks_surviving = [20, 20]
survival_share = [1, 1]
false_positives = [19, 19]
false_positive_share = [0.9, 1]

[[corpus.needle]]
id = "t1"
kind = "template"
pattern = "Exception writing block"
reason = "every trigram is in every block; only longer grams prune"
bands = { literal_run_bytes = [23, 23], true_holders = [2, 2], selected = { blocks_surviving = [2, 2], survival_share = [0, 0.1] } }

[[corpus.needle]]
id = "t2"
kind = "template"
pattern = "Deleting stale replica"
reason = "rare"
bands = { literal_run_bytes = [22, 22], true_holders = [1, 1] }

[[corpus.needle]]
id = "t3"
kind = "template"
pattern = "Verification succeeded"
reason = "in every block"
bands = { literal_run_bytes = [22, 22], true_holders = [20, 20] }

[[corpus.needle]]
id = "t4"
kind = "template"
pattern = "lost heartbeat .* peer"
reason = "a gap"
bands = { literal_run_bytes = [15, 15], true_holders = [1, 1] }

[[corpus.needle]]
id = "t5"
kind = "template"
pattern = "disk quota exceeded"
reason = "two blocks"
bands = { literal_run_bytes = [19, 19], true_holders = [2, 2] }

[[corpus.needle]]
id = "i1"
kind = "id"
pattern = "7f3a9c2e"
reason = "every piece of it is in every block"
bands = { literal_run_bytes = [8, 8], true_holders = [1, 1] }
"#;

fn synthetic_blocks() -> Vec<BlockText> {
    (0..BLOCKS)
        .map(|b| {
            let mut lines: Vec<String> = (0..LINES_PER_BLOCK)
                .map(|i| BACKGROUND[i % BACKGROUND.len()].to_string())
                .collect();
            let mut at = BACKGROUND.len();
            for (line, holders) in PLANTED {
                if holders.contains(&b) {
                    lines[at] = line.to_string();
                    at += 1;
                }
            }
            BlockText::from_lines(&lines).expect("no 0x0A")
        })
        .collect()
}

fn synthetic_spec() -> CorpusSpec {
    spec::parse(SPEC).expect("spec").corpus.remove(0)
}

fn raw_bytes(blocks: &[BlockText]) -> u64 {
    blocks.iter().map(|b| b.bytes().len() as u64).sum()
}

fn synthetic_report() -> ReiReport {
    let blocks = synthetic_blocks();
    let input = ReiInput {
        blocks: &blocks,
        lines: (BLOCKS * LINES_PER_BLOCK) as u64,
        compressed_bytes: raw_bytes(&blocks),
        zstd_level: 3,
    };
    measure(&synthetic_spec(), &input, &Params::default()).expect("measure")
}

fn value(r: &ReiReport, key: &str) -> f64 {
    let hits: Vec<f64> = r
        .figures
        .iter()
        .filter(|f| f.key == key)
        .map(|f| f.value)
        .collect();
    assert_eq!(hits.len(), 1, "{key}");
    hits[0]
}

#[test]
fn rei_report_figures_present_once_and_banded() {
    let r = synthetic_report();
    assert!(r.problems.is_empty(), "{:?}", r.problems);
    // Every band has its figure and every figure its band.
    let keys: std::collections::BTreeSet<&str> = r.figures.iter().map(|f| f.key.as_str()).collect();
    assert_eq!(keys.len(), r.figures.len());
    assert_eq!(keys, r.bands.keys().map(String::as_str).collect());
    // 4 corpus, 2 x 4 arm, 1 grams_selected, 6 needles x (2 + 2 x 4).
    assert_eq!(r.figures.len(), 4 + 8 + 1 + 6 * 10);
    assert!(check_figures(&r.figures, &r.bands).is_empty());

    let key = "syn/t1/selected/blocks_surviving";

    let removed: Vec<Figure> = r.figures.iter().filter(|f| f.key != key).cloned().collect();
    assert_eq!(
        check_figures(&removed, &r.bands),
        [format!("{key}: missing (band [2.0, 2.0])")]
    );

    let mut duplicated = r.figures.clone();
    duplicated.push(Figure {
        key: key.to_string(),
        value: 2.0,
    });
    assert_eq!(
        check_figures(&duplicated, &r.bands),
        [format!("{key}: present 2 times")]
    );

    let mut out_of_band = r.figures.clone();
    for f in &mut out_of_band {
        if f.key == key {
            f.value = 3.0;
        }
    }
    assert_eq!(
        check_figures(&out_of_band, &r.bands),
        [format!("{key}: 3 outside band [2.0, 2.0]")]
    );

    let mut nan = r.figures.clone();
    for f in &mut nan {
        if f.key == key {
            f.value = f64::NAN;
        }
    }
    assert_eq!(check_figures(&nan, &r.bands).len(), 1);

    let mut unbanded = r.figures.clone();
    unbanded.push(Figure {
        key: "syn/stray".to_string(),
        value: 0.0,
    });
    assert_eq!(
        check_figures(&unbanded, &r.bands),
        ["syn/stray: no band".to_string()]
    );

    // One printed line per figure, then the decision.
    let lines = r.lines();
    assert_eq!(lines.len(), r.figures.len() + 1);
    assert!(lines.iter().all(|l| !l.ends_with("MISS")));
    assert!(lines.last().unwrap().starts_with("rei decision syn: "));
}

#[test]
fn selected_grams_prune_more_than_trigrams_on_a_templated_needle() {
    let r = synthetic_report();
    assert_eq!(value(&r, "syn/t1/true_holders"), 2.0);
    assert_eq!(value(&r, "syn/t1/trigram/blocks_surviving"), BLOCKS as f64);
    assert_eq!(value(&r, "syn/t1/selected/blocks_surviving"), 2.0);
    assert_eq!(value(&r, "syn/t1/selected/survival_share"), 0.1);
    assert_eq!(value(&r, "syn/t1/selected/false_positives"), 0.0);
    assert_eq!(value(&r, "syn/t1/trigram/false_positive_share"), 0.9);

    // The same through the arms directly.
    let blocks = synthetic_blocks();
    let p = Pattern::parse("Exception writing block").unwrap();
    let tri = TrigramSets::build(&blocks);
    let sel = GramIndex::build(&blocks, &[p.clone()], u64::MAX, &Params::default());
    assert!(tri.survivors(&p).iter().all(|&k| k));
    let kept: Vec<usize> = (0..BLOCKS).filter(|&b| sel.survivors(&p)[b]).collect();
    assert_eq!(kept, [3, 11]);
}

#[test]
fn selected_grams_prune_nothing_on_an_id() {
    let r = synthetic_report();
    assert_eq!(value(&r, "syn/i1/true_holders"), 1.0);
    for arm in ARMS {
        assert_eq!(
            value(&r, &format!("syn/i1/{arm}/blocks_surviving")),
            BLOCKS as f64
        );
    }
    let blocks = synthetic_blocks();
    let p = Pattern::parse("7f3a9c2e").unwrap();
    let sel = GramIndex::build(&blocks, &[p.clone()], u64::MAX, &Params::default());
    // Every candidate is in every block, above the frequency threshold.
    assert_eq!(sel.candidates, 6 + 5 + 4 + 3);
    assert!(sel.grams.is_empty());
    assert!(sel.survivors(&p).iter().all(|&k| k));
}

#[test]
fn arms_never_drop_a_holder_and_agree_with_the_residual() {
    let r = synthetic_report();
    for n in &synthetic_spec().needle {
        let held = value(&r, &format!("syn/{}/true_holders", n.id));
        for arm in ARMS {
            let s = value(&r, &format!("syn/{}/{arm}/blocks_surviving", n.id));
            let fp = value(&r, &format!("syn/{}/{arm}/false_positives", n.id));
            assert_eq!(s - fp, held, "{} {arm}", n.id);
        }
    }
    let blocks = synthetic_blocks();
    let p = Pattern::parse("lost heartbeat .* peer").unwrap();
    assert!(p.matches_line(b"lost heartbeat from peer 4"));
    assert!(!p.matches_line(b"peer lost heartbeat"));
    assert_eq!(blocks.iter().filter(|b| p.matches_block(b)).count(), 1);
}

#[test]
fn frequency_rule_redundancy_rule_and_byte_cap() {
    let blocks = synthetic_blocks();
    let p = Pattern::parse("Exception writing block").unwrap();
    let sel = GramIndex::build(&blocks, &[p.clone()], u64::MAX, &Params::default());
    let max_df = BLOCKS / 10;
    for (g, list) in &sel.grams {
        assert!(list.len() <= max_df, "{g:?}");
        // No kept proper substring has the same blocks.
        for (h, other) in &sel.grams {
            if h.len() < g.len() && g.windows(h.len()).any(|w| w == h.as_slice()) {
                assert_ne!(other, list, "{g:?} is redundant with {h:?}");
            }
        }
    }
    assert!(sel.grams.iter().any(|(g, _)| g == b"on w"));
    assert!(!sel.grams.iter().any(|(g, _)| g == b"on wr"));

    // A cap below one entry keeps nothing and so prunes nothing.
    let tight = GramIndex::build(&blocks, &[p.clone()], 100, &Params::default());
    assert!(tight.bytes() <= 5);
    assert!(tight.survivors(&p).iter().all(|&k| k));
    // The cap holds at every size.
    for compressed in [200u64, 400, 800, 1600, 3200] {
        let i = GramIndex::build(&blocks, &[p.clone()], compressed, &Params::default());
        assert!(i.bytes() <= compressed / 20, "{compressed}: {}", i.bytes());
    }
}

#[test]
fn index_encoding_round_trips() {
    let blocks = synthetic_blocks();
    let workload: Vec<Pattern> = synthetic_spec()
        .needle
        .iter()
        .map(|n| Pattern::parse(&n.pattern).unwrap())
        .collect();
    let sel = GramIndex::build(&blocks, &workload, u64::MAX, &Params::default());
    assert!(!sel.grams.is_empty());
    let (n, back) = grams::decode(&sel.encoded).expect("decode");
    assert_eq!(n, BLOCKS as u32);
    assert_eq!(back, sel.grams);
    assert!(grams::decode(&sel.encoded[..sel.encoded.len() - 1]).is_none());
}

fn input(survival: [Option<f64>; 5], bytes: Option<f64>, cpu: Option<f64>) -> DecisionInput {
    DecisionInput {
        corpus: "c".to_string(),
        template_survival: survival
            .iter()
            .enumerate()
            .map(|(i, s)| (format!("t{i}"), *s))
            .collect(),
        index_bytes_share: bytes,
        build_cpu_over_zstd: cpu,
    }
}

#[test]
fn decision_go_on_constructed_figures() {
    let d = decide(&input(
        [Some(0.01), Some(0.10), Some(0.08), Some(0.5), Some(0.9)],
        Some(0.05),
        Some(2.0),
    ));
    assert!(d.go, "{}", d.line);
    assert_eq!(
        d.line,
        "rei decision c: GO; selected survival_share <= 0.1 on 3 of 5 template needles, need 3 \
         (t0=0.0100 t1=0.1000 t2=0.0800 t3=0.5000 t4=0.9000); index_bytes_share=0.0500 \
         (bar 0.05); build_cpu_over_zstd=2.0000 (bar 2); fails: none"
    );
}

#[test]
fn decision_no_go_on_survival() {
    let d = decide(&input(
        [Some(0.01), Some(0.11), Some(0.08), Some(0.5), Some(0.9)],
        Some(0.01),
        Some(1.0),
    ));
    assert!(!d.go);
    assert!(d.line.starts_with("rei decision c: NO-GO; "), "{}", d.line);
    assert!(d.line.contains("on 2 of 5"), "{}", d.line);
    assert!(d.line.ends_with("fails: survival"), "{}", d.line);
}

#[test]
fn decision_no_go_on_bytes() {
    let d = decide(&input(
        [Some(0.01), Some(0.01), Some(0.01), Some(0.01), Some(0.01)],
        Some(0.0501),
        Some(1.0),
    ));
    assert!(!d.go);
    assert!(d.line.ends_with("fails: bytes"), "{}", d.line);
}

#[test]
fn decision_no_go_on_cpu() {
    let d = decide(&input(
        [Some(0.01), Some(0.01), Some(0.01), Some(0.01), Some(0.01)],
        Some(0.01),
        Some(2.01),
    ));
    assert!(!d.go);
    assert!(d.line.ends_with("fails: cpu"), "{}", d.line);
}

/// The wrong implementation: a figure not reported reads as 0.
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
    let key = "syn/t2/selected/false_positives";
    assert_eq!(r.bands[key], [0.0, 20.0]);
    let removed: Vec<Figure> = r.figures.iter().filter(|f| f.key != key).cloned().collect();
    // The wrong check passes the gap; the real one names it.
    assert!(check_defaulting_to_zero(&removed, &r.bands).is_empty());
    assert_eq!(check_figures(&removed, &r.bands).len(), 1);

    // In the decision a missing survival figure is a fail, not a clear.
    let d = input(
        [Some(0.01), Some(0.01), None, Some(0.5), Some(0.9)],
        Some(0.01),
        Some(1.0),
    );
    let mut defaulted = d.clone();
    for (_, s) in &mut defaulted.template_survival {
        *s = Some(s.unwrap_or(0.0));
    }
    assert!(decide(&defaulted).go);
    let real = decide(&d);
    assert!(!real.go);
    assert!(real.line.contains("t2=missing"), "{}", real.line);
    let sel_bytes = input([Some(0.01); 5], None, Some(1.0));
    assert!(
        decide(&sel_bytes)
            .line
            .contains("index_bytes_share=missing")
    );
    assert!(!decide(&sel_bytes).go);
}

#[test]
fn survival_is_over_all_blocks_not_surviving_blocks() {
    let r = synthetic_report();
    let key = "syn/t1/selected/survival_share";
    let surviving = value(&r, "syn/t1/selected/blocks_surviving");
    // The wrong denominator: the surviving blocks themselves.
    let wrong: Vec<Figure> = r
        .figures
        .iter()
        .map(|f| Figure {
            key: f.key.clone(),
            value: if f.key == key {
                surviving / surviving
            } else {
                f.value
            },
        })
        .collect();
    assert_eq!(
        check_figures(&wrong, &r.bands),
        [format!("{key}: 1 outside band [0.0, 0.1]")]
    );
    assert_eq!(value(&r, key), survival_share(2, BLOCKS as u64));
    let spec = synthetic_spec();
    let right = decision_input(&spec, &r.figures);
    let bent = decision_input(&spec, &wrong);
    assert_eq!(right.template_survival[0], ("t1".to_string(), Some(0.1)));
    assert_eq!(bent.template_survival[0], ("t1".to_string(), Some(1.0)));
}

#[test]
fn synthetic_decision_reads_its_own_figures() {
    let r = synthetic_report();
    // t1, t2, t4 and t5 survive in at most 2 of 20 blocks; t3 is everywhere.
    let line = &r.decision.line;
    assert!(line.contains("on 4 of 5"), "{line}");
    let fails = line.rsplit("fails: ").next().unwrap();
    assert!(
        !fails.contains("survival") && !fails.contains("bytes"),
        "{line}"
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
fn overall_needs_every_pre_registered_corpus_go_and_clean() {
    let names = ["a".to_string(), "b".to_string()];
    let d = overall(&names, &[saved("a", true, 0), saved("b", true, 0)]);
    assert!(d.go);
    assert_eq!(d.line, "rei decision overall: GO; a=GO, b=GO");

    let d = overall(&names, &[saved("a", true, 0)]);
    assert!(!d.go);
    assert_eq!(d.line, "rei decision overall: NO-GO; a=GO, b=missing");

    let d = overall(&names, &[saved("a", true, 0), saved("b", false, 0)]);
    assert_eq!(d.line, "rei decision overall: NO-GO; a=GO, b=NO-GO");

    let d = overall(&names, &[saved("a", true, 0), saved("b", true, 2)]);
    assert!(!d.go);
    assert!(d.line.contains("b=GO but 2 figure problems (not a result)"));

    let d = overall(
        &names,
        &[
            saved("a", true, 0),
            saved("a", true, 0),
            saved("b", true, 0),
        ],
    );
    assert!(!d.go);
    assert!(d.line.contains("a=2 reports"));

    let d = overall(
        &names,
        &[
            saved("a", true, 0),
            saved("b", true, 0),
            saved("z", true, 0),
        ],
    );
    assert!(!d.go);
    assert!(d.line.ends_with("z=not pre-registered"));

    assert!(!overall(&[], &[]).go);
}

#[test]
fn saved_decision_reads_a_written_report() {
    #[derive(serde::Serialize)]
    struct Written<'a> {
        rei: &'a ReiReport,
    }
    let r = synthetic_report();
    let json = serde_json::to_string(&Written { rei: &r }).unwrap();
    let s = saved_decision(&json).unwrap();
    assert_eq!(s.decision, r.decision);
    assert_eq!(s.problems, 0);
    assert!(saved_decision("{}").is_err());
}

#[test]
fn spec_validation_refuses_a_bad_pre_registration() {
    assert!(spec::parse(&SPEC.replace("schema = 1", "schema = 2")).is_err());
    // Four template needles.
    assert!(
        spec::parse(&SPEC.replace(
            "id = \"t5\"\nkind = \"template\"",
            "id = \"t5\"\nkind = \"id\""
        ))
        .is_err()
    );
    assert!(spec::parse(&SPEC.replace("reason = \"rare\"", "reason = \" \"")).is_err());
    assert!(spec::parse(&SPEC.replace("lines = [800, 800]", "lines = [801, 800]")).is_err());
    assert!(
        spec::parse(&SPEC.replace("pattern = \"disk quota exceeded\"", "pattern = \"di.*sk\""))
            .is_err()
    );
    assert!(spec::parse(&SPEC.replace("id = \"t5\"", "id = \"t4\"")).is_err());
    assert!(spec::parse(&SPEC.replace("id = \"t5\"", "id = \"selected\"")).is_err());
    assert!(
        spec::parse(&SPEC.replace("reason = \"rare\"", "reason = \"rare\"\nextra = 1")).is_err()
    );
}

#[test]
fn measure_refuses_an_unpackable_gram_length() {
    let blocks = synthetic_blocks();
    let input = ReiInput {
        blocks: &blocks,
        lines: 800,
        compressed_bytes: 1,
        zstd_level: 3,
    };
    let params = Params {
        max_gram: 8,
        ..Params::default()
    };
    assert!(measure(&synthetic_spec(), &input, &params).is_err());
}

#[test]
fn rei_needles_fixture_is_a_valid_pre_registration() {
    let text = include_str!("../../fixtures/logbody/rei_needles.toml");
    let s = spec::parse(text).expect("rei_needles.toml");
    let names: Vec<&str> = s.corpus.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["hdfs", "bgl", "ssh", "apache"]);
    for c in &s.corpus {
        for n in &c.needle {
            let p = Pattern::parse(&n.pattern).unwrap();
            // Every needle's literal-run band is its exact length.
            let len = p.literal_run_bytes() as f64;
            assert_eq!(n.bands.literal_run_bytes, [len, len], "{} {}", c.name, n.id);
            assert!(p.literal_run_bytes() >= grams::MIN_GRAM);
        }
    }
    assert_eq!(s.corpus("apache").unwrap().block_records, 1024);
}
