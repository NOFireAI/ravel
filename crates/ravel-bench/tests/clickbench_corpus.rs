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
//!
//! [`parquet_lane_runs_the_upstream_suite_verbatim`] is the ClickBench Parquet
//! lane's acceptance test (ADR-2040 D7, issue #2055): the 43 upstream
//! statements from `benchmarks/clickbench/parquet/queries.sql`, run unedited
//! through Ravel's in-process `SqlExecutor` over a prefix of four files (arm A)
//! and over one combined file (arm B), each compared against plain DataFusion
//! over the same layout.
#![cfg(feature = "sql-latency")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use datafusion::arrow::record_batch::RecordBatch;
use ravel_bench::clickbench_parquet::comparator::{
    self, ColumnMatch, ComparisonReport, Verdict, resolve_order_key_columns, resolve_tie_spec,
};
use ravel_bench::clickbench_parquet::engine::{
    EngineError, InProcessEngine, ReferenceEngine, SuiteEngine,
};
use ravel_bench::clickbench_parquet::fixture;
use ravel_bench::clickbench_parquet::suite::{self, Statement, StatementOverride, Suite};
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

/// Seed the fixture is generated from; any fixed value keeps the run
/// deterministic.
const FIXTURE_SEED: u64 = 2055;

/// Statements the comparator can only check by row and column count: Q18 has
/// a LIMIT and no ORDER BY, and suite.toml declares Q25 and Q27
/// `compare = "cardinality"`. Every other statement must Pass, except one
/// declaring `ci_expected_error`, which [`check_statement`] never compares.
fn expected_verdict(suite: &Suite, number: u32) -> Verdict {
    match number {
        18 => Verdict::CardinalityOnly(None),
        25 | 27 => Verdict::CardinalityOnly(Some(
            suite
                .override_for(number)
                .and_then(|o| o.reason.clone())
                .unwrap_or_else(|| {
                    panic!("Q{number} must declare compare = \"cardinality\" with a reason")
                }),
        )),
        _ => Verdict::Pass,
    }
}

/// Statements that return no rows on the fixture, so their Pass compares
/// nothing: Q28 and Q29 keep only groups with `HAVING COUNT(*) > 100000`,
/// more rows than the whole fixture holds. [`check_row_counts`] asserts both
/// engines really return zero rows for these on both arms, so a fixture
/// resize that makes them non-empty is noticed, and that every other
/// statement (except one declaring `ci_expected_error`) returns at least one
/// row on the reference.
const ZERO_ROWS_ON_FIXTURE: &[u32] = &[28, 29];
const _: () = assert!(
    fixture::TOTAL_ROWS <= 100_000,
    "a fixture over 100,000 rows can give Q28/Q29 a group past their HAVING"
);

type StatementResult = Result<Vec<RecordBatch>, EngineError>;

fn row_count(result: &StatementResult) -> Result<usize, String> {
    result
        .as_ref()
        .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
        .map_err(ToString::to_string)
}

/// Failure messages for every statement whose row counts break
/// [`ZERO_ROWS_ON_FIXTURE`]'s contract on `arm`.
fn check_row_counts(
    suite: &Suite,
    arm: &str,
    reference: &[StatementResult],
    subject: &[StatementResult],
) -> Vec<String> {
    let mut failures = Vec::new();
    for (i, statement) in suite.statements.iter().enumerate() {
        let number = statement.number;
        let reference_rows = row_count(&reference[i]);
        if ZERO_ROWS_ON_FIXTURE.contains(&number) {
            let subject_rows = row_count(&subject[i]);
            if reference_rows != Ok(0) || subject_rows != Ok(0) {
                failures.push(format!(
                    "Q{number} arm {arm}: listed in ZERO_ROWS_ON_FIXTURE, but reference \
                     returned {reference_rows:?} and ravel {subject_rows:?} rows"
                ));
            }
            continue;
        }
        let expects_error = suite
            .override_for(number)
            .is_some_and(|o| o.ci_expected_error.is_some());
        if !expects_error && !matches!(reference_rows, Ok(n) if n > 0) {
            failures.push(format!(
                "Q{number} arm {arm}: reference returned {reference_rows:?} rows; only \
                 ZERO_ROWS_ON_FIXTURE statements may return none"
            ));
        }
    }
    failures
}

async fn run_suite(engine: &dyn SuiteEngine, suite: &Suite) -> Vec<StatementResult> {
    let mut results = Vec::with_capacity(suite.statements.len());
    for statement in &suite.statements {
        results.push(engine.query(&statement.sql).await);
    }
    results
}

fn column_names(batches: &[RecordBatch]) -> Option<Vec<String>> {
    batches.first().map(|b| {
        b.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    })
}

/// Compare one statement's reference and Ravel results the way suite.toml
/// says to. `Err` names whichever engine failed, with both sides' errors,
/// or the comparator's own error.
fn judge(
    statement: &Statement,
    over: Option<&StatementOverride>,
    reference: &StatementResult,
    subject: &StatementResult,
) -> Result<ComparisonReport, String> {
    let (reference, subject) = match (reference, subject) {
        (Ok(r), Ok(s)) => (r, s),
        _ => {
            return Err(format!(
                "reference: {}; ravel: {}",
                reference
                    .as_ref()
                    .map_or_else(ToString::to_string, |_| "ok".to_string()),
                subject
                    .as_ref()
                    .map_or_else(ToString::to_string, |_| "ok".to_string()),
            ));
        }
    };
    let number = statement.number;
    let sql = &statement.sql;
    let column_match = over.map_or(ColumnMatch::Positional, StatementOverride::column_match);
    let (reference_columns, subject_columns) = match column_match {
        ColumnMatch::Positional => (Vec::new(), Vec::new()),
        ColumnMatch::ByName => (
            column_names(reference).ok_or("reference returned no batches")?,
            column_names(subject).ok_or("ravel returned no batches")?,
        ),
    };
    let tie = if let Some(over) = over.filter(|o| o.is_cardinality_only()) {
        resolve_tie_spec(number, sql, None, over.reason.as_deref())
    } else if let Some(names) = over.and_then(|o| o.order_key_columns.as_deref()) {
        let reference_order = column_names(reference).ok_or("reference returned no batches")?;
        // Under by-name matching the subject's columns are compared in the
        // reference's order, so the key resolves against that order on both
        // sides.
        let subject_order = match column_match {
            ColumnMatch::Positional => column_names(subject).ok_or("ravel returned no batches")?,
            ColumnMatch::ByName => reference_order.clone(),
        };
        let key = resolve_order_key_columns(names, &subject_order, &reference_order)
            .map_err(|e| format!("order_key_columns: {e}"))?;
        resolve_tie_spec(number, sql, Some(&key), None)
    } else {
        resolve_tie_spec(number, sql, over.and_then(|o| o.order_key.as_deref()), None)
    }
    .map_err(|e| format!("tie spec: {e}"))?;
    let reference_rows =
        comparator::rows_from_arrow(reference).map_err(|e| format!("reference rows: {e}"))?;
    let subject_rows =
        comparator::rows_from_arrow(subject).map_err(|e| format!("ravel rows: {e}"))?;
    let tolerance = over.and_then(StatementOverride::float_tolerance);
    comparator::compare_with_columns(
        &reference_rows,
        &reference_columns,
        &subject_rows,
        &subject_columns,
        column_match,
        &tie,
        tolerance.as_ref(),
    )
    .map_err(|e| format!("comparator: {e}"))
}

/// The largest bit distance between any mismatching float pair in `report`,
/// 0 when every float cell matched. For a same-sign pair, the only kind a
/// declared tolerance can explain, this is the comparator's ULP distance.
fn max_ulp_distance(report: &ComparisonReport) -> u64 {
    report
        .float_mismatches
        .iter()
        .map(|m| m.reference_bits.abs_diff(m.subject_bits))
        .max()
        .unwrap_or(0)
}

/// One line of the verdict table, and a failure message when the line is
/// not what [`expected_verdict`] says it must be. Every compared statement
/// also appends `(arm, number, max ulp distance)` to `ulps`.
fn check_statement(
    suite: &Suite,
    arm: &str,
    statement: &Statement,
    reference: &StatementResult,
    subject: &StatementResult,
    totals: &mut (u64, u64),
    ulps: &mut Vec<(String, u32, u64)>,
) -> (String, Option<String>) {
    let number = statement.number;
    let over = suite.override_for(number);
    if let Some(expected) = over.and_then(|o| o.ci_expected_error.as_deref()) {
        let reference = reference
            .as_ref()
            .map_or_else(ToString::to_string, |_| "ok".to_string());
        return match subject {
            Err(e) if e.to_string().contains(expected) => (
                format!("Q{number} arm {arm}: expected error (reference: {reference})"),
                None,
            ),
            Err(e) => (
                format!("Q{number} arm {arm}: ERROR"),
                Some(format!(
                    "Q{number} arm {arm}: ravel error {e} does not contain the declared \
                     ci_expected_error {expected:?}; reference: {reference}"
                )),
            ),
            Ok(_) => (
                format!("Q{number} arm {arm}: ok"),
                Some(format!(
                    "Q{number} arm {arm}: ravel answered, but suite.toml declares \
                     ci_expected_error {expected:?}; reference: {reference}"
                )),
            ),
        };
    }
    let expected = expected_verdict(suite, number);
    match judge(statement, over, reference, subject) {
        Err(problem) => (
            format!("Q{number} arm {arm}: ERROR"),
            Some(format!("Q{number} arm {arm}: {problem}")),
        ),
        Ok(report) => {
            totals.0 += report.tie_rows_reduced;
            totals.1 += report.float_cells_compared;
            ulps.push((arm.to_string(), number, max_ulp_distance(&report)));
            let line = format!(
                "Q{number} arm {arm}: {:?} (tie_rows_reduced {}, float_cells_compared {}, \
                 explained float mismatches {}, max ulp distance {})",
                report.verdict,
                report.tie_rows_reduced,
                report.float_cells_compared,
                report
                    .float_mismatches
                    .iter()
                    .filter(|m| m.explanation.is_some())
                    .count(),
                ulps.last().map_or(0, |u| u.2),
            );
            let failure = (report.verdict != expected).then(|| {
                format!(
                    "Q{number} arm {arm}: verdict {:?}, expected {expected:?}; missing rows {:?}; \
                     extra rows {:?}; float mismatches {:?}",
                    report.verdict,
                    report.row_mismatch.missing,
                    report.row_mismatch.extra,
                    report.float_mismatches,
                )
            });
            (line, failure)
        }
    }
}

fn copy_parts(fixture_dir: &Path, parts_dir: &Path) {
    std::fs::create_dir_all(parts_dir).expect("create parts dir");
    for part in 0..fixture::PART_COUNT {
        let name = format!("hits_{part}.parquet");
        std::fs::copy(fixture_dir.join(&name), parts_dir.join(&name)).expect("copy part");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_lane_runs_the_upstream_suite_verbatim() {
    let suite = suite::load_default().expect("suite loads");
    assert_eq!(suite.statements.len(), suite::STATEMENT_COUNT);

    let tmp = tempfile::tempdir().expect("tempdir");
    let fixture_dir = tmp.path().join("fixture");
    std::fs::create_dir_all(&fixture_dir).expect("create fixture dir");
    fixture::write_hits(&fixture_dir, FIXTURE_SEED).expect("fixture writes");
    // The reference's arm A layout: the four parts alone, without the
    // combined file sitting beside them in `fixture_dir`.
    let parts_dir = tmp.path().join("parts");
    copy_parts(&fixture_dir, &parts_dir);

    let ravel = InProcessEngine::new(&fixture_dir)
        .await
        .expect("in-process engine builds");

    let created = ravel
        .ddl(&suite.table.render("s3://clickbench/hits/"))
        .await
        .expect("arm A CREATE EXTERNAL TABLE");
    assert_eq!(created.outcome, "created");
    assert_eq!(created.files, Some(4), "arm A mounts the four parts");
    let arm_a = run_suite(&ravel, &suite).await;

    let dropped = ravel.ddl("DROP TABLE hits").await.expect("DROP TABLE hits");
    assert_eq!(dropped.outcome, "dropped");
    let created = ravel
        .ddl(&suite.table.render("s3://clickbench/hits.parquet"))
        .await
        .expect("arm B CREATE EXTERNAL TABLE");
    assert_eq!(created.outcome, "created");
    assert_eq!(created.files, Some(1), "arm B mounts the combined file");
    let arm_b = run_suite(&ravel, &suite).await;

    let reference_a = ReferenceEngine::new(&parts_dir)
        .await
        .expect("arm A reference engine builds");
    let reference_a = run_suite(&reference_a, &suite).await;
    let reference_b = ReferenceEngine::new(&fixture_dir.join("hits.parquet"))
        .await
        .expect("arm B reference engine builds");
    let reference_b = run_suite(&reference_b, &suite).await;

    let mut table = Vec::new();
    let mut failures = Vec::new();
    let mut totals = (0u64, 0u64);
    let mut ulps = Vec::new();
    for (arm, subject, reference) in [("A", &arm_a, &reference_a), ("B", &arm_b, &reference_b)] {
        for (i, statement) in suite.statements.iter().enumerate() {
            let (line, failure) = check_statement(
                &suite,
                arm,
                statement,
                &reference[i],
                &subject[i],
                &mut totals,
                &mut ulps,
            );
            table.push(line);
            failures.extend(failure);
        }
        failures.extend(check_row_counts(&suite, arm, reference, subject));
    }
    println!("{}", table.join("\n"));
    println!(
        "summed tie_rows_reduced {}, summed float_cells_compared {}",
        totals.0, totals.1
    );

    assert!(
        failures.is_empty(),
        "{} statement check(s) failed (verdict or row count):\n{}",
        failures.len(),
        failures.join("\n")
    );
    // These figures are exact because everything they depend on is fixed: the
    // fixture (FIXTURE_SEED) and both engines' plan partition count
    // (engine::PLAN_PARTITIONS), which sets how many partial aggregates the
    // floating-point sums are combined from. A comparator that stops reducing
    // ties or comparing floats on any statement-arm changes the totals.
    let q4_ulps: Vec<(&str, u64)> = ulps
        .iter()
        .filter(|(_, number, _)| *number == 4)
        .map(|(arm, _, distance)| (arm.as_str(), *distance))
        .collect();
    assert_eq!(
        q4_ulps,
        [("A", 1), ("B", 2)],
        "Q4's ulp distance per arm changed; suite.toml's float_max_ulps for Q4 is \
         declared against these"
    );
    assert_eq!(
        totals.0, 588,
        "summed tie_rows_reduced across both arms changed"
    );
    assert_eq!(
        totals.1, 44,
        "summed float_cells_compared across both arms changed"
    );
}
