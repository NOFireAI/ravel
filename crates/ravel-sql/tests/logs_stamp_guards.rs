//! The physical checks that guard the stamp-first statistics readers of
//! ADR-4628 (issue #4684). A stamp answer needs no `.cstat` load, so the
//! executor's load gate refuses nothing for it: every refusal below rests on
//! a check in the metadata-only rewrite (`crate::metadata_agg`) or on the scan
//! (`metadata_rewrite_stats_are_exact`). Each test names the refusal
//! inventory row it pins, runs over stamps carrying fields 6 to 8 with no
//! `.cstat`, and compares with a forced scan of the same objects (fields
//! stripped, no `.cstat`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use datafusion::arrow::array::{Float64Array, Int64Array};
use ravel_logseg::AttrValue;
use ravel_sql::{DeclaredType, KeyIndexDeclaration};
use ravel_types::declared_stats::DeclaredStatValue;

use crate::logs_stamp_readers::{
    Corpus, Fields, Row, assert_answered, assert_same_answer, assert_scanned, columns, pretty, row,
    stamp_of, two_parts,
};

/// Each statement scans over the stamps and equals the forced scan.
async fn assert_each_scans(corpus: &Corpus, sqls: &[&str]) {
    for sql in sqls {
        let run = corpus.stamped(sql).await;
        assert_scanned(&run, sql);
        assert_same_answer(&run, &corpus.forced(sql).await, sql);
    }
}

/// Each statement answers from the stamps and equals a scan of the same
/// objects with no stamp at all.
async fn assert_each_answers(corpus: &Corpus, sqls: &[&str]) {
    for sql in sqls {
        let run = corpus.stamped(sql).await;
        assert_answered(&run, sql);
        let truth = corpus.truth(sql).await;
        assert_scanned(&truth, sql);
        assert_same_answer(&run, &truth, sql);
    }
}

/// `two_parts`' rows as one object: one scan partition, so `LimitPushdown`
/// leaves a subquery's `LIMIT` on the scan alone.
async fn one_object() -> Corpus {
    let rows: Vec<Row> = two_parts().into_iter().flatten().collect();
    Corpus::build(&[rows]).await
}

/// Row 2: a column that is not declared has no stamp to read.
#[tokio::test]
async fn row02_an_undeclared_argument_scans() {
    let corpus = Corpus::build(&two_parts()).await;
    assert_each_scans(
        &corpus,
        &[
            "SELECT SUM(severity_num) FROM logs",
            "SELECT COUNT(*) FROM logs WHERE severity_num <> 9",
        ],
    )
    .await;
}

/// Row 3: a content predicate, an attribute-map equality, a prune arm other
/// than `NumNotEq`, and a ts bound that clips a segment, written plain and as
/// a `TIMESTAMP` literal.
#[tokio::test]
async fn row03_content_prune_and_clipping_bounds_scan() {
    let corpus = Corpus::build(&two_parts()).await;
    assert_each_scans(
        &corpus,
        &[
            "SELECT SUM(a) FROM logs WHERE body LIKE '%row at 1%'",
            "SELECT COUNT(*) FROM logs WHERE a <> 0 AND body LIKE '%row at 1%'",
            "SELECT SUM(a) FROM logs WHERE attrs['user'] = 'subject-s'",
            "SELECT COUNT(*) FROM logs WHERE a <> 0 AND attrs['user'] = 'subject-s'",
            "SELECT SUM(a) FROM logs WHERE a > 1",
            "SELECT AVG(w) FROM logs WHERE ts < TIMESTAMP '1970-01-01 00:00:00.000000025'",
            "SELECT COUNT(*) FROM logs WHERE a <> 0 AND ts < TIMESTAMP '1970-01-01 00:00:00.000000025'",
            "SELECT SUM(a) FROM logs WHERE ts < '1970-01-01 00:00:00.000000025'",
            "SELECT COUNT(*) FROM logs WHERE a <> 0 AND ts < '1970-01-01 00:00:00.000000025'",
        ],
    )
    .await;
}

/// Row 4: two subqueries, one answerable and one refused: the first answers
/// from the stamps and the second scans, and the statement equals the forced
/// scan.
#[tokio::test]
async fn row04_only_the_answerable_subquery_answers() {
    let corpus = Corpus::build(&two_parts()).await;
    let sql = "SELECT (SELECT SUM(a) FROM logs) AS s, \
               (SELECT SUM(a) FROM logs WHERE body LIKE '%row at 1%') AS c";
    let run = corpus.stamped(sql).await;
    assert!(
        run.plan.contains("MetadataOnlyExec") && run.plan.contains("LogsScanExec"),
        "one subquery answers, the other scans; plan was:\n{}",
        run.plan
    );
    let forced = corpus.forced(sql).await;
    assert!(!forced.plan.contains("MetadataOnlyExec"), "{}", forced.plan);
    assert_same_answer(&run, &forced, sql);
}

/// Row 5: with no `.cstat` loaded, as when the executor's load gate admits
/// no aggregate, the physical rule alone decides: a shape it refuses scans.
#[tokio::test]
async fn row05_with_no_load_the_physical_rule_decides() {
    let corpus = Corpus::build(&two_parts()).await;
    assert_each_scans(&corpus, &["SELECT MIN(w), SUM(a) FROM logs"]).await;
}

/// Row 6: a fetch on any node of the walk, the scan included, for each limit
/// shape of `logs_metadata_agg_multi.rs`.
#[tokio::test]
async fn row06_a_fetch_on_the_walk_scans() {
    let corpus = one_object().await;
    for sql in [
        "SELECT SUM(a), COUNT(*) FROM (SELECT a FROM logs LIMIT 3)",
        "SELECT SUM(a) FROM (SELECT a FROM logs LIMIT 3)",
        "SELECT AVG(a) FROM (SELECT a FROM logs LIMIT 3)",
        "SELECT COUNT(*) FROM (SELECT a FROM logs LIMIT 3) WHERE a <> 0",
        "SELECT a, COUNT(*) FROM (SELECT a FROM logs LIMIT 3) GROUP BY a ORDER BY a",
        "SELECT MIN(a), MAX(a) FROM (SELECT a FROM logs LIMIT 3)",
    ] {
        let run = corpus.stamped(sql).await;
        assert!(
            !run.plan.contains("MetadataOnlyExec"),
            "{sql}:\n{}",
            run.plan
        );
        assert!(run.gets > 0, "{sql}: the limited rows are read");
        assert_same_answer(&run, &corpus.forced(sql).await, sql);
    }
    // The limited rows are a = 0, 2, NULL: 2, not 17.
    let run = corpus
        .stamped("SELECT SUM(a) FROM (SELECT a FROM logs LIMIT 3)")
        .await;
    let sum = columns(&run)[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(sum, 2);
}

/// Row 7: an `OFFSET` with no `LIMIT` carries no fetch; only the walk's
/// allowlist refuses it. One scan partition.
#[tokio::test]
async fn row07_an_offset_without_a_limit_scans() {
    let corpus = one_object().await;
    assert_each_scans(
        &corpus,
        &[
            "SELECT SUM(a) FROM (SELECT a FROM logs OFFSET 2)",
            "SELECT COUNT(*) FROM (SELECT a FROM logs OFFSET 2) WHERE a <> 0",
        ],
    )
    .await;
}

/// Row 8: a sort with no fetch, a window, a join, a semi join, a union, a
/// `DISTINCT` and an inner aggregate between the aggregate and the scan. Each
/// equals the forced scan; the ones whose plan keeps such a node scan.
#[tokio::test]
async fn row08_nodes_outside_the_allowlist_match_the_scan() {
    let corpus = Corpus::build(&two_parts()).await;
    for sql in [
        "SELECT SUM(a) FROM (SELECT a FROM logs ORDER BY w)",
        "SELECT SUM(x) FROM (SELECT LAG(a) OVER (ORDER BY ts) AS x FROM logs)",
        "SELECT SUM(l.a) FROM logs l JOIN logs r ON l.a = r.w",
        "SELECT SUM(a) FROM logs WHERE a IN (SELECT w - 1365 FROM logs)",
        "SELECT SUM(a) FROM (SELECT a FROM logs UNION ALL SELECT a FROM logs)",
        "SELECT SUM(a) FROM (SELECT DISTINCT a FROM logs)",
        "SELECT SUM(m) FROM (SELECT MAX(a) AS m FROM logs GROUP BY w)",
        "SELECT COUNT(*) FROM (SELECT DISTINCT a FROM logs) WHERE a <> 0",
    ] {
        let run = corpus.stamped(sql).await;
        let forced = corpus.forced(sql).await;
        assert_same_answer(&run, &forced, sql);
        if !sql.contains("ORDER BY w)") {
            assert_scanned(&run, sql);
        }
    }
}

/// Row 9: a projection between the aggregate and the scan is outside the
/// walk's allowlist, an alias included: under `SUM` DataFusion keeps
/// `a AS x` as a `ProjectionExec`, so the alias scans like the computed
/// `a * 2`. Under q02's filter the alias is gone by the time the rule runs,
/// and the statement answers as `a <> 0`.
#[tokio::test]
async fn row09_a_projection_on_the_walk_scans() {
    let corpus = Corpus::build(&two_parts()).await;
    assert_each_answers(
        &corpus,
        &["SELECT COUNT(*) FROM (SELECT a AS x FROM logs) WHERE x <> 0"],
    )
    .await;
    assert_each_scans(
        &corpus,
        &[
            "SELECT SUM(x) FROM (SELECT a AS x FROM logs)",
            "SELECT SUM(x) FROM (SELECT a * 2 AS x FROM logs)",
            "SELECT COUNT(*) FROM (SELECT a * 2 AS x FROM logs) WHERE x <> 0",
        ],
    )
    .await;
}

/// Row 10: a residual conjunct, a row filter (`Str <> ''`, `I64 =`), a key
/// equality and a key `IN`.
#[tokio::test]
async fn row10_row_filters_and_key_predicates_scan() {
    let parts: Vec<Vec<Row>> = two_parts()
        .into_iter()
        .map(|rows| {
            rows.into_iter()
                .enumerate()
                .map(|(i, r)| {
                    if i % 2 == 0 {
                        r.with("s", AttrValue::Str(format!("v{i}")))
                    } else {
                        r.with("s", AttrValue::Str(String::new()))
                    }
                })
                .collect()
        })
        .collect();
    let corpus = Corpus::build(&parts).await;
    assert_each_scans(
        &corpus,
        &[
            "SELECT SUM(a) FROM logs WHERE s <> ''",
            "SELECT COUNT(*) FROM logs WHERE a <> 0 AND s <> ''",
            "SELECT SUM(a) FROM logs WHERE w = 7",
            "SELECT COUNT(*) FROM logs WHERE a <> 0 AND w = 7",
            "SELECT SUM(a) FROM logs WHERE a + w > 10",
        ],
    )
    .await;
    for sql in [
        "SELECT SUM(w) FROM logs WHERE a = 2",
        "SELECT SUM(w) FROM logs WHERE a IN (2, 5)",
        "SELECT COUNT(*) FROM logs WHERE w <> 7 AND a = 2",
        "SELECT COUNT(*) FROM logs WHERE w <> 7 AND a IN (2, 5)",
    ] {
        let mut keyed = corpus.setup(Fields::With);
        keyed.key_index = Some(KeyIndexDeclaration {
            columns: vec!["a".to_string()],
            trace_id: false,
        });
        let run = corpus.run(sql, keyed).await;
        assert_scanned(&run, sql);
        let mut forced = corpus.setup(Fields::Without);
        forced.key_index = Some(KeyIndexDeclaration {
            columns: vec!["a".to_string()],
            trace_id: false,
        });
        assert_same_answer(&run, &corpus.run(sql, forced).await, sql);
    }
}

/// Row 11: an aggregate the rule does not classify, and grouped shapes.
#[tokio::test]
async fn row11_unclassified_aggregates_and_grouped_shapes_scan() {
    let corpus = Corpus::build(&two_parts()).await;
    assert_each_scans(
        &corpus,
        &[
            "SELECT SUM(DISTINCT a) FROM logs",
            "SELECT SUM(a) FILTER (WHERE w > 1000) FROM logs",
            "SELECT SUM(a), MIN(w) FROM logs",
            "SELECT COUNT(NULL), SUM(a) FROM logs",
            "SELECT SUM(a + NULL) FROM logs",
            "SELECT SUM(CAST(a AS DOUBLE)) FROM logs",
            "SELECT AVG(CAST(a AS DOUBLE)) FROM logs",
            "SELECT SUM(a + 1.5) FROM logs",
            "SELECT w, SUM(a) FROM logs GROUP BY w ORDER BY w",
            "SELECT w, AVG(a) FROM logs GROUP BY w ORDER BY w",
            "SELECT w, COUNT(*) FROM logs WHERE a <> 0 GROUP BY w ORDER BY w",
        ],
    )
    .await;
}

/// Row 13: q02 with no not-equal conjunct, two of them, or a `Str` one; and
/// a literal the stamp's three cases do not cover (mutant d: the zero count
/// is the count of zeros only).
#[tokio::test]
async fn row13_other_not_equal_shapes_scan() {
    let parts: Vec<Vec<Row>> = two_parts()
        .into_iter()
        .map(|rows| {
            rows.into_iter()
                .map(|r| r.with("s", AttrValue::Str("x".into())))
                .collect()
        })
        .collect();
    let corpus = Corpus::build(&parts).await;
    assert_each_scans(
        &corpus,
        &[
            "SELECT COUNT(*) FROM logs WHERE a < 3",
            "SELECT COUNT(*) FROM logs WHERE a <> 0 AND w <> 7",
            "SELECT COUNT(*) FROM logs WHERE s <> 'x'",
            "SELECT COUNT(*) FROM logs WHERE a <> 2",
            "SELECT COUNT(*) FROM logs WHERE w <> 1368",
        ],
    )
    .await;
    // Covered literals: outside every object's range, and the one-valued
    // second object's `w` with the first object's range excluding it.
    assert_each_answers(
        &corpus,
        &[
            "SELECT COUNT(*) FROM logs WHERE a <> 100",
            "SELECT COUNT(*) FROM logs WHERE a <> -1",
            "SELECT COUNT(*) FROM logs WHERE w <> 7",
        ],
    )
    .await;
}

/// Rows 12 and 16 (mutant g): a stamp whose type is not the column's
/// declared type answers nothing. `a` is declared I64, its rows hold BOOL
/// values (NULL to the declaration), and the stamp is a BOOL one with a zero
/// count.
#[tokio::test]
async fn row16_a_stamp_of_another_type_scans() {
    let rows: Vec<Row> = vec![
        Row {
            ts: 0,
            attrs: vec![("a", AttrValue::Bool(false))],
        },
        Row {
            ts: 10,
            attrs: vec![("a", AttrValue::Bool(true))],
        },
        Row {
            ts: 20,
            attrs: vec![("a", AttrValue::Bool(false))],
        },
    ];
    let corpus = Corpus::build(std::slice::from_ref(&rows)).await;
    let bool_stamp = stamp_of("a", DeclaredType::Bool, &rows, Fields::With).expect("stamp");
    assert_eq!(bool_stamp.zero_count(), Some(2));
    for sql in [
        "SELECT COUNT(*) FROM logs WHERE a <> 0",
        "SELECT SUM(a) FROM logs",
    ] {
        let mut setup = corpus.setup(Fields::With);
        setup.segs = vec![corpus.seg_with(0, std::slice::from_ref(&bool_stamp))];
        let run = corpus.run(sql, setup).await;
        assert_scanned(&run, sql);
        assert_same_answer(&run, &corpus.forced(sql).await, sql);
    }
}

/// The BOOL not-equal shapes reach the rule as no not-equal at all:
/// DataFusion simplifies `f <> false` to the filter `f` and `f <> true` to
/// `NOT f`, so both scan and equal the scan of unstamped objects.
#[tokio::test]
async fn bool_not_equal_shapes_match_the_scan() {
    let parts = vec![
        vec![
            row(0, Some(1), None).with("f", AttrValue::Bool(false)),
            row(10, Some(1), None).with("f", AttrValue::Bool(true)),
            row(20, Some(1), None),
        ],
        vec![
            row(30, Some(1), None).with("f", AttrValue::Bool(true)),
            row(40, Some(1), None).with("f", AttrValue::Bool(true)),
        ],
    ];
    let corpus = Corpus::build(&parts).await;
    for (sql, filter) in [
        (
            "SELECT COUNT(*) FROM logs WHERE f <> false",
            "FilterExec: f@0, projection=[]",
        ),
        (
            "SELECT COUNT(*) FROM logs WHERE f <> true",
            "FilterExec: NOT f@0, projection=[]",
        ),
    ] {
        let run = corpus.stamped(sql).await;
        assert!(run.plan.contains(filter), "{sql}: plan was:\n{}", run.plan);
        assert_scanned(&run, sql);
        assert_same_answer(&run, &corpus.truth(sql).await, sql);
    }
    let run = corpus
        .stamped("SELECT COUNT(*) FROM logs WHERE f <> false")
        .await;
    assert!(pretty(&run).contains("| 3 "), "{}", pretty(&run));
}

/// The one value of a one-row, one-column `Int64` answer.
fn int64(run: &crate::logs_stamp_readers::Run) -> i64 {
    columns(run)[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0)
}

/// The one value of a one-row, one-column `Float64` answer, as bits.
fn float64_bits(run: &crate::logs_stamp_readers::Run) -> u64 {
    columns(run)[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64")
        .value(0)
        .to_bits()
}

/// Row 3's prune-arm refusal has no statement of its own: every pushed
/// predicate that adds a prune arm other than `NumNotEq` also leaves a
/// residual `FilterExec` above the scan (the arm is block-only, so the
/// provider reports the filter `Inexact`), or, for a declared I64 equality,
/// a row filter on the scan. Each candidate below scans and equals the forced
/// scan; the arm alone is pinned in `logs_scan`'s `figure_resolver_tests`
/// (`a_prune_arm_other_than_not_equal_refuses_the_stamp_answer`).
#[tokio::test]
async fn every_prune_arm_statement_carries_another_refusal() {
    let parts: Vec<Vec<Row>> = two_parts()
        .into_iter()
        .map(|rows| {
            rows.into_iter()
                .enumerate()
                .map(|(i, r)| r.with("s", AttrValue::Str(format!("v{}", i % 2))))
                .collect()
        })
        .collect();
    let corpus = Corpus::build(&parts).await;
    for sql in [
        "SELECT SUM(a) FROM logs WHERE attrs['user'] = 'subject-s'",
        "SELECT COUNT(*) FROM logs WHERE a <> 0 AND attrs['user'] = 'subject-s'",
        "SELECT SUM(a) FROM logs WHERE w = 7",
        "SELECT COUNT(*) FROM logs WHERE a <> 0 AND w = 7",
        "SELECT SUM(a) FROM logs WHERE w BETWEEN 1 AND 1000",
        "SELECT SUM(a) FROM logs WHERE w > 1",
        "SELECT COUNT(*) FROM logs WHERE a <> 0 AND w > 1",
        "SELECT SUM(a) FROM logs WHERE w IN (7, 800)",
        "SELECT SUM(a) FROM logs WHERE s = 'v0'",
        "SELECT COUNT(*) FROM logs WHERE a <> 0 AND s = 'v0'",
    ] {
        let run = corpus.stamped(sql).await;
        assert!(
            run.plan.contains("FilterExec") || run.plan.contains("row_filters=["),
            "{sql}: a residual filter or a row filter beside the prune arm; plan was:\n{}",
            run.plan
        );
        assert_scanned(&run, sql);
        assert_same_answer(&run, &corpus.forced(sql).await, sql);
    }
}

/// Two objects with disjoint ts spans (0 to 40 and 100 to 130); `ts >= 50`
/// excludes the first entirely and contains the second, so the resolved scan
/// holds only the second, which the bound does not clip: the statement
/// answers from its stamp, with no data-object GET, and equals the scan of
/// unstamped objects. The figures differ from the unbounded statement's, so
/// counting the excluded object would change them. Written once as a
/// `TIMESTAMP` literal and once plain.
///
/// Prove-the-test: make `LogsTableProvider::pruned_segments` keep every
/// segment; the first object then fails the containment check and the
/// statement scans, failing `assert_answered`.
#[tokio::test]
async fn a_ts_bound_excluding_one_object_and_containing_the_other_answers() {
    let corpus = Corpus::build(&two_parts()).await;
    for bound in [
        "ts >= TIMESTAMP '1970-01-01 00:00:00.000000050'",
        "ts >= '1970-01-01 00:00:00.000000050'",
    ] {
        let sum = format!("SELECT SUM(a) FROM logs WHERE {bound}");
        let avg = format!("SELECT AVG(a) FROM logs WHERE {bound}");
        let ne = format!("SELECT COUNT(*) FROM logs WHERE a <> 0 AND {bound}");
        for sql in [&sum, &avg, &ne] {
            let run = corpus.stamped(sql).await;
            assert_answered(&run, sql);
            let truth = corpus.truth(sql).await;
            assert_scanned(&truth, sql);
            assert_same_answer(&run, &truth, sql);
        }
        // The second object's a: 3, NULL, 0, 7.
        assert_eq!(int64(&corpus.stamped(&sum).await), 10, "{sum}");
        assert_eq!(
            float64_bits(&corpus.stamped(&avg).await),
            (10.0f64 / 3.0).to_bits(),
            "{avg}"
        );
        assert_eq!(int64(&corpus.stamped(&ne).await), 2, "{ne}");
    }
    // Unbounded, the first object's 0, 2, 0, 5 count too.
    assert_eq!(int64(&corpus.stamped("SELECT SUM(a) FROM logs").await), 17);
    assert_eq!(
        float64_bits(&corpus.stamped("SELECT AVG(a) FROM logs").await),
        (17.0f64 / 7.0).to_bits()
    );
    assert_eq!(
        int64(
            &corpus
                .stamped("SELECT COUNT(*) FROM logs WHERE a <> 0")
                .await
        ),
        4
    );
}

/// Literals the not-equal reader must not take: past `i64`, fractional, a
/// float equal to an integer, and the column cast to `DOUBLE`. DataFusion
/// casts the column to the literal's type (`CAST(a@0 AS Decimal128(20, 0))`,
/// `CAST(a@0 AS Float64)`), so none reaches the rule as a bare column against
/// an `Int64` literal; each scans and equals the scan of unstamped objects.
/// `CAST(a AS BIGINT)` and `-0` reach it as `a <> 0`: the optimizer removes
/// the no-op cast and folds `-0` to 0, and both answer from the stamps with
/// the scan's count.
#[tokio::test]
async fn not_equal_literals_of_another_type_scan() {
    let corpus = Corpus::build(&two_parts()).await;
    for (sql, filter, count) in [
        (
            "SELECT COUNT(*) FROM logs WHERE a <> 9223372036854775808",
            "FilterExec: CAST(a@0 AS Decimal128(20, 0)) != 9223372036854775808",
            7,
        ),
        (
            "SELECT COUNT(*) FROM logs WHERE a <> 0.5",
            "FilterExec: CAST(a@0 AS Float64) != 0.5",
            7,
        ),
        (
            "SELECT COUNT(*) FROM logs WHERE a <> 100.0",
            "FilterExec: CAST(a@0 AS Float64) != 100,",
            7,
        ),
        (
            "SELECT COUNT(*) FROM logs WHERE CAST(a AS DOUBLE) <> 0",
            "FilterExec: CAST(a@0 AS Float64) != 0,",
            4,
        ),
    ] {
        let run = corpus.stamped(sql).await;
        assert!(run.plan.contains(filter), "{sql}: plan was:\n{}", run.plan);
        assert_scanned(&run, sql);
        assert_same_answer(&run, &corpus.truth(sql).await, sql);
        assert_eq!(int64(&run), count, "{sql}");
    }
    for sql in [
        "SELECT COUNT(*) FROM logs WHERE CAST(a AS BIGINT) <> 0",
        "SELECT COUNT(*) FROM logs WHERE a <> -0",
    ] {
        let run = corpus.stamped(sql).await;
        assert_answered(&run, sql);
        let truth = corpus.truth(sql).await;
        assert_scanned(&truth, sql);
        assert_same_answer(&run, &truth, sql);
        assert_eq!(int64(&run), 4, "{sql}: a is 2, 5, 3, 7 on four rows");
    }
}

/// Two stamps whose zero count contradicts their extrema over an object
/// whose minimum `a` is 0 and whose other non-null values are 3 and 5: a
/// zero count of 0, and a zero count of 3 (every non-null row). Neither can
/// describe its object. Decode drops each (issue #4672), so `COUNT(*) WHERE
/// a <> 0` scans and equals the forced scan, which counts the two non-zero
/// rows.
///
/// Prove-the-test: remove the two zero-count clauses from `aggregate_defect`
/// and the drop count fails; remove the `stamp_equal_count` checks as well
/// and the statement answers from the stamp, failing `assert_scanned`.
#[tokio::test]
async fn a_zero_count_that_contradicts_the_extrema_scans() {
    let rows = vec![
        row(0, Some(0), None),
        row(10, Some(3), None),
        row(20, None, None),
        row(30, Some(5), None),
    ];
    let corpus = Corpus::build(std::slice::from_ref(&rows)).await;
    let true_stamp = stamp_of("a", DeclaredType::I64, &rows, Fields::Without).expect("stamp");
    assert_eq!(
        (true_stamp.min(), true_stamp.max(), true_stamp.null_count()),
        (
            Some(DeclaredStatValue::I64(0)),
            Some(DeclaredStatValue::I64(5)),
            1
        )
    );
    let sql = "SELECT COUNT(*) FROM logs WHERE a <> 0";
    let forced = corpus.forced(sql).await;
    for zero_count in [0, 3] {
        let lying = true_stamp
            .clone()
            .with_zero_count(zero_count)
            .expect("zero inside the range");
        let (seg, dropped) = corpus.seg_read(0, std::slice::from_ref(&lying));
        assert_eq!(dropped, 1, "zero count {zero_count}: decode drops the stamp");
        let mut setup = corpus.setup(Fields::With);
        setup.segs = vec![seg];
        let run = corpus.run(sql, setup).await;
        assert_scanned(&run, sql);
        assert_same_answer(&run, &forced, sql);
        assert_eq!(int64(&run), 2, "3 and 5");
    }
}
