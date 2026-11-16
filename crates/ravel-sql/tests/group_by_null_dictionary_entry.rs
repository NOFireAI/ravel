//! Issue #3521 through the real logs scan: a declared `Str` column whose page
//! dictionary holds an entry that is not UTF-8, referenced by some rows,
//! beside genuine empty strings and rows that do not set the key.
//!
//! The scan reads a non-UTF-8 entry as no value (`StrCursor::dict_key` in
//! `logs_scan/batch.rs` returns no key for it), so no row it emits has a valid
//! key over a NULL entry, and `GROUP BY` agrees with the row path. This file
//! pins both: the emitted arrays never key a NULL entry, and the grouped,
//! filtered, distinct and ordered answers are exact on the fast path and the
//! row path alike. The planner-level reproduction of the wrong answer, with a
//! hand-built dictionary that does key a NULL entry, is in
//! `group_keys::tests::a_null_dictionary_entry_groups_with_null`.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, DictionaryArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_codec::encoding::Enc;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

use crate::logs_columnar::{column_page_encs, rewrite_str_column_cells, str_column_id};

const TENANT: TenantHash = TenantHash([53u8; 16]);

/// The declared `url` cell of each row, in row order: 4 `api`, 5 over the
/// non-UTF-8 entry `[b'b', 0xff]`, 3 genuine `""`, 2 `worker`, 3 absent. The
/// resource does not set `url`, so an absent or non-UTF-8 cell is NULL.
fn cells() -> Vec<Option<Vec<u8>>> {
    let mut cells = Vec::new();
    for (cell, rows) in [
        (Some(b"api".to_vec()), 4),
        (Some(vec![b'b', 0xff]), 5),
        (Some(Vec::new()), 3),
        (Some(b"worker".to_vec()), 2),
        (None, 3),
    ] {
        cells.extend(std::iter::repeat_n(cell, rows));
    }
    cells
}

/// `GROUP BY url` from first principles: the 3 absent rows and the 5 over the
/// non-UTF-8 entry are NULL, and `""` counts only the 3 genuine empty strings.
fn want_groups() -> Vec<(Option<String>, i64)> {
    vec![
        (None, 8),
        (Some(String::new()), 3),
        (Some("api".to_owned()), 4),
        (Some("worker".to_owned()), 2),
    ]
}

fn record(ts: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("row at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        // Every record sets `url` so the writer's FIELD_DIR, which the rewrite
        // copies, carries the `Str` column; the rewrite decides each cell.
        attrs: vec![("url".to_string(), AttrValue::Str("seed".to_string()))],
    }
}

fn encode(records: &[LogRecord], cfg: RlogConfig) -> Vec<u8> {
    let mut w = RlogWriter::new(
        cfg,
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [7u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

async fn fixture() -> (Arc<MemoryStore>, SegmentRef) {
    let cfg = RlogConfig {
        block_target_records: 64,
        ..RlogConfig::default()
    };
    let cells = cells();
    let records: Vec<LogRecord> = (1..=cells.len() as i64).map(record).collect();
    let clean = encode(&records, cfg);
    let column_id = str_column_id(&clean, cfg, "url");
    let patched = rewrite_str_column_cells(&clean, cfg, &records, column_id, &cells);
    assert_eq!(
        column_page_encs(&patched, cfg, column_id),
        [Enc::Bitmap, Enc::Dict],
        "the present cells are stored as a page dictionary"
    );

    let store = Arc::new(MemoryStore::new());
    let key = "logs/null-dictionary-entry.rlog".to_string();
    let content_hash = *blake3::hash(&patched).as_bytes();
    let object_size = patched.len() as u64;
    store
        .put(&key, bytes::Bytes::from(patched), PutOptions::default())
        .await
        .expect("put");
    let segment = SegmentRef {
        data_object_key: key,
        object_size,
        min_event_ts_ns: 1,
        max_event_ts_ns: records.len() as i64,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(7),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    (store, segment)
}

/// The fast path, or with `row_path` the row path, forced by a pending
/// erasure that matches nothing.
fn snapshot(segment: &SegmentRef, row_path: bool) -> Snapshot {
    let pending_erasure = if row_path {
        vec![ravel_proto::commit::v1::ErasureRequest {
            predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                key: "__does_not_exist__".to_string(),
                value: "__nope__".to_string(),
            }],
            ..Default::default()
        }]
    } else {
        Vec::new()
    };
    Snapshot {
        segments: vec![segment.clone()],
        segments_pruned: 0,
        pending_erasure,
    }
}

/// One statement's batches, the scan's `columnar_batches` and
/// `rowpath_batches` counts (zero each when no scan ran), and whether an
/// aggregate in the plan groups on `Utf8View`, the rule's rewrite.
struct Run {
    batches: Vec<RecordBatch>,
    columnar: usize,
    rowpath: usize,
    groups_on_views: bool,
}

/// `sql` run through `SqlExecutor`'s session over `snapshot`.
async fn execute(store: &Arc<MemoryStore>, snapshot: Snapshot, sql: &str) -> Run {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&backend)),
        LogSegmentFetcher::new(Arc::clone(&backend)),
        SpanSegmentFetcher::new(Arc::clone(&backend)),
        SqlConfig::default(),
        1 << 30,
    );
    let declared = [DeclaredColumn::new("url", DeclaredType::Str)];
    let pinned = executor
        .plan_pinned(TENANT, snapshot, sql, &QueryAccounting::new(), &declared)
        .await
        .expect("plan");
    let plan = pinned.create_physical_plan().await.expect("physical plan");
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    Run {
        batches,
        columnar: scan_metric(&plan, "columnar_batches"),
        rowpath: scan_metric(&plan, "rowpath_batches"),
        groups_on_views: groups_on_views(&plan),
    }
}

fn groups_on_views(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let own = plan.downcast_ref::<AggregateExec>().is_some_and(|agg| {
        agg.group_expr()
            .expr()
            .iter()
            .any(|(expr, _)| expr.data_type(&agg.input().schema()).ok() == Some(DataType::Utf8View))
    });
    own || plan.children().iter().any(|child| groups_on_views(child))
}

fn scan_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let own = if plan.name() == "LogsScanExec" {
        plan.metrics()
            .map(|metrics| {
                metrics
                    .iter()
                    .filter(|m| m.value().name() == name)
                    .map(|m| m.value().as_usize())
                    .sum()
            })
            .unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .iter()
        .map(|child| scan_metric(child, name))
        .sum::<usize>()
}

/// Column 0 of every row, read through arrow's `take`-based cast to `Utf8`,
/// which keeps a NULL dictionary entry NULL.
fn texts(batches: &[RecordBatch]) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = datafusion::arrow::compute::cast(batch.column(0), &DataType::Utf8)
                .expect("reads as Utf8");
            let column = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8")
                .clone();
            (0..column.len())
                .map(|i| column.is_valid(i).then(|| column.value(i).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn int(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64")
                .clone();
            values.values().to_vec()
        })
        .collect()
}

/// Step 1a of issue #3521: the scan never hands the group key a valid key over
/// a NULL entry, so `GROUP BY url`, `url IS NULL`, `count(DISTINCT url)` and
/// `ORDER BY url LIMIT` are exact on both paths. The fast path's arrays do
/// carry the NULL entry (the non-UTF-8 one) in their values; no row keys it.
#[tokio::test]
async fn a_non_utf8_dictionary_entry_groups_with_null_through_the_scan() {
    let (store, segment) = fixture().await;

    for row_path in [false, true] {
        let path = if row_path { "row path" } else { "fast path" };
        let Run {
            batches: rows,
            columnar,
            rowpath,
            ..
        } = execute(&store, snapshot(&segment, row_path), "SELECT url FROM logs").await;
        assert_eq!(
            (columnar > 0, rowpath > 0),
            (!row_path, row_path),
            "{path}: columnar {columnar}, rowpath {rowpath}"
        );
        let mut null_entries = 0;
        for batch in &rows {
            let dict = batch
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("url is Dictionary(Int32, Utf8)");
            null_entries += dict.values().null_count();
            for key in dict.keys().iter().flatten() {
                let at = usize::try_from(key).expect("non-negative key");
                assert!(
                    dict.values().is_valid(at),
                    "{path}: a row keys NULL entry {at}"
                );
            }
        }
        assert_eq!(
            null_entries > 0,
            !row_path,
            "{path}: the fast path's values hold the non-UTF-8 entry as NULL"
        );
        let mut all = texts(&rows);
        all.sort();
        assert_eq!(all.len(), 17, "{path}");
        assert_eq!(all.iter().filter(|t| t.is_none()).count(), 8, "{path}");

        let grouped = execute(
            &store,
            snapshot(&segment, row_path),
            "SELECT url, count(*) AS c FROM logs GROUP BY url",
        )
        .await;
        assert!(grouped.groups_on_views, "{path}: the rule groups on views");
        let grouped = grouped.batches;
        let mut groups: Vec<(Option<String>, i64)> =
            texts(&grouped).into_iter().zip(int(&grouped, 1)).collect();
        groups.sort();
        assert_eq!(groups, want_groups(), "{path}: GROUP BY url");

        let nulls = execute(
            &store,
            snapshot(&segment, row_path),
            "SELECT count(*) FROM logs WHERE url IS NULL",
        )
        .await;
        assert_eq!(int(&nulls.batches, 0), [8], "{path}: url IS NULL");

        let distinct = execute(
            &store,
            snapshot(&segment, row_path),
            "SELECT count(DISTINCT url) FROM logs",
        )
        .await;
        assert_eq!(
            int(&distinct.batches, 0),
            [3],
            "{path}: count(DISTINCT url)"
        );

        let ordered = execute(
            &store,
            snapshot(&segment, row_path),
            "SELECT url FROM logs ORDER BY url LIMIT 5",
        )
        .await;
        assert_eq!(
            texts(&ordered.batches),
            [
                Some(String::new()),
                Some(String::new()),
                Some(String::new()),
                Some("api".to_owned()),
                Some("api".to_owned()),
            ],
            "{path}: ORDER BY url LIMIT 5"
        );
    }
}
