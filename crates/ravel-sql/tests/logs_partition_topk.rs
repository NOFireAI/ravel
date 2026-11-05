//! ADR-0093, exact not-equal amendment: a logs scan that evaluates a pushed
//! declared-`Str` `<>` conjunct itself, under a global `ORDER BY ... LIMIT k`
//! TopK over a `CoalescePartitionsExec`, gets a per-partition TopK between the
//! coalesce and the scan (`LogsPartitionTopK`).
//!
//! Every statement runs through the production session over real RLOG objects
//! in a `MemoryStore`, one object per partition. The reference answer is the
//! same statement on a provider with the row filter off
//! (`LogsTableProvider::with_row_filter_pushdown(false)`), whose plan keeps a
//! `FilterExec` above the scan and so gets no partition TopK.
//!
//! - `a_row_filtered_scan_under_a_global_topk_gets_a_partition_topk`: the q26
//!   shape plans the inserted sort with its fetch and returns the reference
//!   answer on three fixtures.
//! - `other_topk_shapes_plan_as_before`: the `ORDER BY ts` shapes, an
//!   unfiltered `ORDER BY` a `Str` column, the q13 aggregate shape and a
//!   `FilterExec` between the coalesce and the scan get no inserted sort.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::physical_plan::{ExecutionPlan, displayable};
use ravel_catalog::SegmentRef;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig};
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{DeclaredColumn, DeclaredType};

use crate::logs_str_not_equal_row_filter::{
    Stream, column_texts, encode, nodes, partitioned_session, pretty, provider, put, record, run, s,
};

/// The session's `target_partitions`, and every fixture's object count: an
/// uncached scan plans one partition per object up to `target_partitions`.
const PARTITIONS: usize = 4;

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("SearchPhrase", DeclaredType::Str),
        DeclaredColumn::new("URL", DeclaredType::Str),
    ]
}

/// One object per entry of `objects`, each row holding its `SearchPhrase`.
async fn fixture(objects: [&[&str]; PARTITIONS]) -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (object, phrases) in objects.iter().enumerate() {
        let records: Vec<LogRecord> = phrases
            .iter()
            .enumerate()
            .map(|(i, phrase)| {
                let ts = object as i64 * 100 + i as i64;
                record(
                    ts,
                    Stream::Bare,
                    &[
                        ("SearchPhrase", s(phrase)),
                        ("URL", AttrValue::Str(format!("https://google.com/{i}"))),
                    ],
                )
            })
            .collect();
        let bytes = encode(&records, RlogConfig::default());
        segments.push(put(&store, &format!("logs/p-{object}.rlog"), bytes, &records).await);
    }
    (store, segments)
}

/// `plan`'s EXPLAIN lines, each without the TopK's dynamic `filter=[...]`,
/// whose text depends on what execution has seen so far.
fn shape(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    displayable(plan.as_ref())
        .indent(true)
        .to_string()
        .lines()
        .map(|line| match line.find(", filter=[") {
            Some(at) => line[..at].to_string(),
            None => line.to_string(),
        })
        .collect()
}

/// The q26 shape with `k = 3`.
const Q26: &str = "SELECT \"SearchPhrase\" FROM logs WHERE \"SearchPhrase\" <> '' \
                   ORDER BY \"SearchPhrase\" LIMIT 3";

/// The q26 shape plans as a global `SortExec` with the fetch over the
/// coalesce, over a `SortExec` with the same keys and fetch that preserves
/// partitioning, over the scan; the reference plan keeps its `FilterExec` and
/// has no such sort. Each fixture's answer equals the reference answer and the
/// pinned sorted values:
///
/// - the 3 smallest values all in one partition;
/// - ties on `b` in every partition, so which `b` rows survive is not
///   asserted, only the values;
/// - the global top 3 spread over three partitions.
///
/// The answer is asserted before the plan, so a rule that replaced the global
/// `SortExec` with the partition one, which returns the partitions' top 3
/// rows unmerged, fails "the reference answer".
#[tokio::test]
async fn a_row_filtered_scan_under_a_global_topk_gets_a_partition_topk() {
    let fixtures: [([&[&str]; PARTITIONS], [&str; 3]); 3] = [
        (
            [
                &["m", "", "n", "o"],
                &["p", "", "q", "r"],
                &["c", "", "a", "b"],
                &["s", "", "t", "u"],
            ],
            ["a", "b", "c"],
        ),
        (
            [
                &["m", "b", ""],
                &["b", "z", ""],
                &["b", "", "x"],
                &["", "a", "b"],
            ],
            ["a", "b", "b"],
        ),
        (
            [
                &["a", "", "e"],
                &["", "f", "b"],
                &["g", "c", ""],
                &["h", "", "d"],
            ],
            ["a", "b", "c"],
        ),
    ];
    let expected_shape = [
        "SortExec: TopK(fetch=3), expr=[SearchPhrase@0 ASC NULLS LAST], \
         preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    SortExec: TopK(fetch=3), expr=[SearchPhrase@0 ASC NULLS LAST], \
         preserve_partitioning=[true]",
        "      LogsScanExec: partitions=4, content=0, prune=0, projection=[SearchPhrase], \
         row_filters=[SearchPhrase != ]",
    ];
    for (objects, values) in fixtures {
        let (store, segments) = fixture(objects).await;
        let on = partitioned_session(provider(&store, segments.clone(), &declared()), PARTITIONS);
        let off = partitioned_session(
            provider(&store, segments, &declared()).with_row_filter_pushdown(false),
            PARTITIONS,
        );
        let (plan, got) = run(&on, Q26).await;
        let (reference_plan, reference) = run(&off, Q26).await;
        assert_eq!(pretty(&got), pretty(&reference), "the reference answer");
        let expected: Vec<Option<String>> = values.iter().map(|v| Some(v.to_string())).collect();
        assert_eq!(
            column_texts(&got, "SearchPhrase"),
            expected,
            "the sorted values"
        );
        assert_eq!(
            shape(&plan),
            expected_shape,
            "the plan with a partition TopK"
        );
        assert_eq!(
            nodes(&plan, "LogsScanExec")[0]
                .properties()
                .partitioning
                .partition_count(),
            PARTITIONS
        );
        assert_eq!(
            nodes(&reference_plan, "SortExec(TopK)").len(),
            1,
            "the reference plan has no partition TopK: {:?}",
            shape(&reference_plan)
        );
        assert_eq!(nodes(&reference_plan, "FilterExec").len(), 1);
    }
}

/// Every other TopK shape plans as it did before the rule, pinned by its
/// EXPLAIN: `ORDER BY ts` (q25) and `ORDER BY ts, "SearchPhrase"` (q27) with
/// the filter, whose scans carry a confirmed `ts` threshold; `ORDER BY` a
/// `Str` column with no filter; the q13 shape, whose sort sits above an
/// aggregate; and a `FilterExec` holding a `LIKE` between the coalesce and
/// the scan. Each answer equals the reference answer.
#[tokio::test]
async fn other_topk_shapes_plan_as_before() {
    let (store, segments) = fixture([
        &["d", "", "b", "c"],
        &["a", "", "e", "b"],
        &["g", "", "h", "a"],
        &["j", "", "b", "l"],
    ])
    .await;
    let on = partitioned_session(provider(&store, segments.clone(), &declared()), PARTITIONS);
    let off = partitioned_session(
        provider(&store, segments, &declared()).with_row_filter_pushdown(false),
        PARTITIONS,
    );
    let cases: [(&str, &[&str]); 5] = [
        (
            "SELECT \"SearchPhrase\" FROM logs WHERE \"SearchPhrase\" <> '' ORDER BY ts LIMIT 3",
            &[
                "ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase]",
                "  SortExec: TopK(fetch=3), expr=[ts@1 ASC NULLS LAST], \
                 preserve_partitioning=[false]",
                "    CoalescePartitionsExec",
                "      ProjectionExec: expr=[SearchPhrase@1 as SearchPhrase, ts@0 as ts]",
                "        LogsScanExec: partitions=4, content=0, prune=0, \
                 projection=[ts, SearchPhrase], row_filters=[SearchPhrase != ], \
                 topk_threshold=ts",
            ],
        ),
        (
            "SELECT \"SearchPhrase\" FROM logs WHERE \"SearchPhrase\" <> '' \
             ORDER BY ts, \"SearchPhrase\" LIMIT 3",
            &[
                "ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase]",
                "  SortExec: TopK(fetch=3), expr=[ts@1 ASC NULLS LAST, \
                 SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "    CoalescePartitionsExec",
                "      ProjectionExec: expr=[SearchPhrase@1 as SearchPhrase, ts@0 as ts]",
                "        LogsScanExec: partitions=4, content=0, prune=0, \
                 projection=[ts, SearchPhrase], row_filters=[SearchPhrase != ], \
                 topk_threshold=ts",
            ],
        ),
        (
            "SELECT \"SearchPhrase\" FROM logs ORDER BY \"SearchPhrase\" LIMIT 3",
            &[
                "SortExec: TopK(fetch=3), expr=[SearchPhrase@0 ASC NULLS LAST], \
                 preserve_partitioning=[false]",
                "  CoalescePartitionsExec",
                "    LogsScanExec: partitions=4, content=0, prune=0, projection=[SearchPhrase]",
            ],
        ),
        (
            "SELECT \"SearchPhrase\", COUNT(*) AS c FROM logs WHERE \"SearchPhrase\" <> '' \
             GROUP BY \"SearchPhrase\" ORDER BY c DESC, \"SearchPhrase\" LIMIT 3",
            &[
                "ProjectionExec: expr=[CAST(SearchPhrase@0 AS Dictionary(Int32, Utf8)) \
                 as SearchPhrase, c@1 as c]",
                "  ProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, \
                 count(Int64(1))@1 as c]",
                "    SortExec: TopK(fetch=3), expr=[count(Int64(1))@1 DESC, \
                 SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "      CompactSlicesExec: topk=[count(Int64(1))@1 DESC, \
                 SearchPhrase@0 ASC NULLS LAST], fetch=3",
                "        AggregateExec: mode=Final, gby=[SearchPhrase@0 as SearchPhrase], \
                 aggr=[count(Int64(1))]",
                "          CoalescePartitionsExec",
                "            AggregateExec: mode=Partial, \
                 gby=[CAST(null_entries_as_null_keys(SearchPhrase@0) AS Utf8View) \
                 as SearchPhrase], aggr=[count(Int64(1))]",
                "              LogsScanExec: partitions=4, content=0, prune=0, \
                 projection=[SearchPhrase], row_filters=[SearchPhrase != ]",
            ],
        ),
        (
            "SELECT \"SearchPhrase\" FROM logs WHERE \"SearchPhrase\" <> '' \
             AND \"URL\" LIKE '%google%' ORDER BY \"SearchPhrase\" LIMIT 3",
            &[
                "SortExec: TopK(fetch=3), expr=[SearchPhrase@0 ASC NULLS LAST], \
                 preserve_partitioning=[false]",
                "  CoalescePartitionsExec",
                "    FilterExec: like(URL@1, %google%), projection=[SearchPhrase@0]",
                "      LogsScanExec: partitions=4, content=1, prune=0, \
                 projection=[SearchPhrase, URL], row_filters=[SearchPhrase != ]",
            ],
        ),
    ];
    for (sql, expected_shape) in cases {
        let (plan, got) = run(&on, sql).await;
        let (_, reference) = run(&off, sql).await;
        assert_eq!(shape(&plan), expected_shape, "{sql}: the plan as before");
        assert_eq!(
            pretty(&got),
            pretty(&reference),
            "{sql}: the reference answer"
        );
    }
}
