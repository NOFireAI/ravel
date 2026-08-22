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

use ravel_bench::sql_corpus::{load_external_corpus, supported_construct_names};

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
/// with the single construct that blocks it. One row per rejected statement; the
/// distinct missing capabilities are two (`LIKE` pattern matching, and `length`
/// not being enumerated as a named construct in the conformance registry) -- see
/// the runbook's gap list and the issues it references.
///
/// Each named construct MUST be absent from [`supported_construct_names`]
/// (asserted by [`each_known_gap_names_a_genuinely_unsupported_construct`]): a
/// gap cannot be claimed for a construct that is actually supported, and if a
/// construct here later becomes supported this test fails, which is the signal to
/// move that statement into the corpus file.
const KNOWN_GAPS: &[(&str, &str)] = &[
    ("Q21", "LIKE pattern match"),
    ("Q22", "LIKE pattern match"),
    ("Q23", "LIKE pattern match"),
    ("Q24", "LIKE pattern match"),
    ("Q28", "length"),
    ("Q29", "length"),
];

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
