//! Gate test for the checked-in ClickBench `hits` corpus (issue #430,
//! ADR-0100 decision 3), at `benchmarks/clickbench/hits.corpus.json`.
//!
//! It asserts two things that together stop the corpus rotting:
//!
//! 1. The corpus FILE parses and passes the construct gate. `load_external_corpus`
//!    parses the document and runs the same gate the harness runs before the
//!    first query, so an unsupported construct, a duplicate id, an empty
//!    modification reason, or a malformed document is a loud typed error naming
//!    the fault -- not a silently skipped entry.
//! 2. Every one of ClickBench's 43 upstream statements (queries.sql, Q1..Q43) is
//!    accounted for: present in the corpus, or listed in [`KNOWN_GAPS`] with the
//!    single unsupported construct that keeps it out. A statement dropped without
//!    a gap entry fails the accounting; a gap claimed for a construct that is
//!    actually supported fails the last test.
//!
//! The gap list here mirrors the runbook (`docs/guides/clickbench.md`) and the
//! capability issues it references; this test is what keeps the two honest.
#![cfg(feature = "sql-latency")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::path::PathBuf;

use ravel_bench::sql_corpus::{CostClass, load_external_corpus, supported_construct_names};

/// The checked-in corpus, relative to this crate's manifest dir
/// (`crates/ravel-bench`) up to the repo root.
fn corpus_path() -> PathBuf {
    PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/clickbench/hits.corpus.json"
    ))
}

/// ClickBench maintains 43 statements upstream (queries.sql, Q1..Q43).
const UPSTREAM_COUNT: usize = 43;

/// The ClickBench statements the corpus construct-gate cannot admit, each paired
/// with the single construct that blocks it. One row per rejected statement.
///
/// Now empty: every ClickBench statement Q1..Q43 is in the corpus file. Q21-Q24
/// (`LIKE` pattern matching, issue #479) moved into the corpus once `LIKE` was
/// registered as a named construct; Q28/Q29 (`length`, issue #480) moved earlier
/// once `length` was registered.
///
/// Each named construct MUST be absent from [`supported_construct_names`]
/// (asserted by [`each_known_gap_names_a_genuinely_unsupported_construct`]): a
/// gap cannot be claimed for a construct that is actually supported, and if a
/// construct here later becomes supported this test fails, which is the signal to
/// move that statement into the corpus file.
const KNOWN_GAPS: &[(&str, &str)] = &[];

#[test]
fn checked_in_clickbench_corpus_parses_and_passes_the_construct_gate() {
    let entries = load_external_corpus(corpus_path())
        .expect("checked-in ClickBench corpus parses and passes the construct gate");
    assert!(!entries.is_empty(), "corpus is empty");
    for e in &entries {
        assert!(
            e.upstream_id.is_some(),
            "corpus entry `{}` has no upstream_id; every ClickBench statement must carry \
             the id it is diffed against",
            e.id
        );
        // A modified statement without a reason is already refused by the gate;
        // this makes the disclosure obligation explicit at the corpus boundary.
        if e.modified.is_modified() {
            assert!(
                e.modified.reason().is_some_and(|r| !r.trim().is_empty()),
                "corpus entry `{}` is modified but states no reason",
                e.id
            );
        }
    }
}

#[test]
fn every_clickbench_statement_is_run_or_a_named_gap() {
    let entries = load_external_corpus(corpus_path()).expect("corpus loads");

    let mut accounted: BTreeSet<String> = BTreeSet::new();
    for e in &entries {
        let up = e
            .upstream_id
            .clone()
            .expect("every entry carries an upstream_id");
        assert!(
            accounted.insert(up.clone()),
            "upstream id `{up}` appears twice across the corpus"
        );
    }
    for (up, _) in KNOWN_GAPS {
        assert!(
            accounted.insert((*up).to_string()),
            "upstream id `{up}` is both in the corpus and listed as a known gap"
        );
    }

    let expected: BTreeSet<String> = (1..=UPSTREAM_COUNT).map(|i| format!("Q{i}")).collect();
    assert_eq!(
        accounted, expected,
        "every ClickBench statement Q1..Q{UPSTREAM_COUNT} must be either in the corpus or a \
         named gap: neither silently absent nor invented"
    );
}

/// The `q<NN>` prefix of a corpus entry id, e.g. `q07` from
/// `q07_min_max_eventdate`. Membership is matched on this, not on full id text.
fn q_prefix(id: &str) -> &str {
    id.split('_').next().unwrap_or(id)
}

/// The metadata-decomposable (M) statements, by `q<NN>` prefix (epic #913).
const CLASS_M: &[&str] = &["q01", "q02", "q07", "q08"];
/// The selective (S) statements, by `q<NN>` prefix (epic #913).
const CLASS_S: &[&str] = &[
    "q20", "q21", "q22", "q23", "q24", "q37", "q38", "q39", "q40", "q41", "q42", "q43",
];

/// The class a `q<NN>` prefix belongs to, derived from the M/S lists; every
/// prefix not in either is full-value (F). Independent of what the JSON says, so
/// a mislabelled entry disagrees with this and fails the membership test.
fn expected_class(q: &str) -> CostClass {
    if CLASS_M.contains(&q) {
        CostClass::MetadataDecomposable
    } else if CLASS_S.contains(&q) {
        CostClass::Selective
    } else {
        CostClass::FullValue
    }
}

#[test]
fn every_clickbench_statement_carries_a_cost_class() {
    let entries = load_external_corpus(corpus_path()).expect("corpus loads");
    assert_eq!(
        entries.len(),
        UPSTREAM_COUNT,
        "the corpus must hold all {UPSTREAM_COUNT} statements"
    );
    for e in &entries {
        assert!(
            e.class.is_some(),
            "corpus entry `{}` carries no cost class; every ClickBench statement must be \
             classed (epic #913)",
            e.id
        );
    }
}

#[test]
fn cost_class_counts_are_exactly_four_twelve_and_the_remainder() {
    let entries = load_external_corpus(corpus_path()).expect("corpus loads");
    let (mut m, mut s, mut f) = (0usize, 0usize, 0usize);
    for e in &entries {
        match e.class.expect("every entry is classed") {
            CostClass::MetadataDecomposable => m += 1,
            CostClass::Selective => s += 1,
            CostClass::FullValue => f += 1,
        }
    }
    assert_eq!(
        m, 4,
        "expected exactly 4 metadata-decomposable (M) statements"
    );
    assert_eq!(s, 12, "expected exactly 12 selective (S) statements");
    assert_eq!(
        f,
        UPSTREAM_COUNT - 16,
        "expected the remaining {} statements to be full-value (F)",
        UPSTREAM_COUNT - 16
    );
    assert_eq!(
        m + s + f,
        UPSTREAM_COUNT,
        "every statement is classed exactly once"
    );
}

#[test]
fn cost_class_membership_is_pinned_per_statement() {
    let entries = load_external_corpus(corpus_path()).expect("corpus loads");
    // Every entry's label must equal the class its q-prefix belongs to. A test
    // that only counted would pass with the labels shuffled; this fails the
    // moment any single statement is mislabelled.
    for e in &entries {
        let q = q_prefix(&e.id);
        assert_eq!(
            e.class.expect("classed"),
            expected_class(q),
            "corpus entry `{}` carries the wrong cost class",
            e.id
        );
    }
    // Named spot checks from the task, so the pin is legible without expanding
    // the loop in your head.
    let class_of = |q: &str| -> CostClass {
        entries
            .iter()
            .find(|e| q_prefix(&e.id) == q)
            .and_then(|e| e.class)
            .unwrap_or_else(|| panic!("statement {q} is present and classed"))
    };
    for q in ["q01", "q02", "q07", "q08"] {
        assert_eq!(
            class_of(q),
            CostClass::MetadataDecomposable,
            "{q} must be M"
        );
    }
    assert_eq!(class_of("q20"), CostClass::Selective, "q20 must be S");
    assert_eq!(class_of("q43"), CostClass::Selective, "q43 must be S");
    assert_eq!(class_of("q03"), CostClass::FullValue, "q03 must be F");
}

#[test]
fn each_known_gap_names_a_genuinely_unsupported_construct() {
    let supported = supported_construct_names();
    for (up, construct) in KNOWN_GAPS {
        assert!(
            !supported.contains(*construct),
            "known gap {up} names construct `{construct}`, but the conformance registry \
             classifies it as supported; if it is now supported, move {up} into the corpus \
             file instead of listing it as a gap"
        );
    }
}
