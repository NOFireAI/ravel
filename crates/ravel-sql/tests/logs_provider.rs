//! Integration tests for [`ravel_sql::LogsTableProvider`] (ADR-0033, issue
//! #239), the `logs` SQL table over an already-resolved `Signal::Logs`
//! snapshot.
//!
//! Two properties are pinned:
//!
//! - `scan_prunes_by_ts_and_word_returns_exact_rows` (the epic's acceptance
//!   test for this task): a ts range + `has_word` combination returns exactly
//!   the records that should survive across several objects, with no false
//!   positives and no false negatives. This is the pruning-soundness property:
//!   segment/ts pruning and content pushdown may only ever widen, and the
//!   scan's output still matches an independent record-by-record oracle.
//! - `scan_reverifies_stream_attr_over_approximation`: the scan re-applies a
//!   stream-attribute equality against each record's genuine top-level
//!   resource/scope attributes, excluding a record whose stream only matches
//!   because the fetcher's byte-containment prefilter cannot tell a nested
//!   `Map` value from a real top-level attribute (issue #238's documented
//!   over-approximation). A control fetch proves the fetcher really does return
//!   the false positive, so the scan's exclusion is doing real work.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogQuery, LogSegmentFetcher, StreamAttrEquals};
use ravel_sql::{LogsPushdown, LogsTableProvider, has_word_udf};
use uuid::Uuid;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 3 records so the small test objects still have several
/// blocks and pruning has something real to act on.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// A record on the single-`service.name` stream `name`.
fn record(name: &str, ts: i64, body: &str) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str(name.to_string()))];
    record_with_resource(&resource, ts, body)
}

/// A record on the stream identified by an arbitrary resource attribute set.
fn record_with_resource(resource: &[(String, AttrValue)], ts: i64, body: &str) -> LogRecord {
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Write one RLOG object from `records`, put it at `key`, and return a matching
/// L0 [`SegmentRef`] carrying the object's true ts span.
async fn write_object(store: &MemoryStore, key: &str, records: &[LogRecord]) -> SegmentRef {
    let mut w = RlogWriter::new(small_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

fn ts_lit(v: i64) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::TimestampNanosecond(Some(v), None))
}

/// The independent oracle for `has_word`: `word` tokenizes to an in-order
/// contiguous run present in the tokenized `body`. Mirrors the reader/UDF, so
/// the expected set is computed with no shared code path with the scan.
fn body_has_word(body: &str, word: &str) -> bool {
    let query = tokens(word);
    if query.is_empty() {
        return true;
    }
    let toks = tokens(body);
    toks.windows(query.len()).any(|w| w == query.as_slice())
}

/// Reduce output batches to the set of `(ts, body)` pairs they contain.
fn batches_to_rows(batches: &[RecordBatch]) -> BTreeSet<(i64, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        assert_eq!(
            batch.schema(),
            ravel_sql::logs_schema(),
            "public logs schema"
        );
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let body = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body col");
        for i in 0..batch.num_rows() {
            out.insert((ts.value(i), body.value(i).to_string()));
        }
    }
    out
}

async fn collect_plan(plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

/// The epic's acceptance test: a ts range plus a `has_word` predicate returns
/// exactly the surviving records across several objects, checked against an
/// independent oracle (no false positives, no false negatives).
#[tokio::test]
async fn scan_prunes_by_ts_and_word_returns_exact_rows() {
    let store = MemoryStore::new();

    // Object A: stream "api", ts 100..=110. "connection timeout" at 105.
    let obj_a: Vec<LogRecord> = (100..=110)
        .map(|ts| {
            record(
                "api",
                ts,
                if ts == 105 {
                    "connection timeout"
                } else {
                    "ok"
                },
            )
        })
        .collect();
    // Object B: stream "worker", ts 1000..=1010, entirely outside the query ts
    // range (must be pruned before any GET). "request timeout" at 1005.
    let obj_b: Vec<LogRecord> = (1000..=1010)
        .map(|ts| {
            record(
                "worker",
                ts,
                if ts == 1005 { "request timeout" } else { "ok" },
            )
        })
        .collect();
    // Object C: stream "api", ts 200..=205. "timeout" at 202, and a decoy
    // "timed out" at 204 that must NOT match the word "timeout".
    let obj_c: Vec<LogRecord> = (200..=205)
        .map(|ts| {
            let body = match ts {
                202 => "gateway timeout",
                204 => "timed out",
                _ => "ok",
            };
            record("api", ts, body)
        })
        .collect();

    let ref_a = write_object(&store, "logs/a.rlog", &obj_a).await;
    let ref_b = write_object(&store, "logs/b.rlog", &obj_b).await;
    let ref_c = write_object(&store, "logs/c.rlog", &obj_c).await;

    let snapshot = Snapshot {
        segments: vec![ref_a, ref_b, ref_c],
        segments_pruned: 0,
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let provider = LogsTableProvider::new(snapshot, fetcher, EngineConfig::default());

    // WHERE ts >= 100 AND ts <= 250 AND has_word(body, 'timeout')
    let (lo, hi) = (100i64, 250i64);
    let filters = vec![
        col("ts").gt_eq(ts_lit(lo)),
        col("ts").lt_eq(ts_lit(hi)),
        has_word_udf().call(vec![col("body"), lit("timeout")]),
    ];
    let plan = provider.plan_filters(4, &filters).expect("build plan");
    let batches = collect_plan(plan).await;
    let got = batches_to_rows(&batches);

    // Independent oracle: every source record whose ts is in [lo, hi] and whose
    // body token-contains "timeout".
    let mut want = BTreeSet::new();
    for records in [&obj_a, &obj_b, &obj_c] {
        for r in records {
            if r.ts_ns >= lo && r.ts_ns <= hi && body_has_word(&r.body, "timeout") {
                want.insert((r.ts_ns, r.body.clone()));
            }
        }
    }

    // The oracle should pick exactly the two "timeout" rows in range.
    assert_eq!(
        want,
        BTreeSet::from([
            (105, "connection timeout".to_string()),
            (202, "gateway timeout".to_string()),
        ]),
        "oracle sanity"
    );
    assert_eq!(got, want, "scan output must equal the oracle exactly");
}

/// The mandatory stream-attribute re-verification test (issue #239): the
/// scan excludes a record whose stream only matches `service.name = 'api'`
/// because the pair is nested inside a `Map` value, not a genuine top-level
/// resource attribute. The fetcher over-approximates and returns it; the scan
/// must not.
#[tokio::test]
async fn scan_reverifies_stream_attr_over_approximation() {
    let store = MemoryStore::new();

    // Stream (plain): a genuine top-level `service.name = "api"`.
    let plain = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    // Stream (nested): no top-level `service.name`; the pair only appears nested
    // inside a `k8s.labels` map attribute value. A distinct stream.
    let nested = vec![(
        "k8s.labels".to_string(),
        AttrValue::Map(vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )]),
    )];

    let records = vec![
        record_with_resource(&plain, 1, "hello from plain"),
        record_with_resource(&nested, 2, "hello from nested"),
    ];
    let seg = write_object(&store, "logs/nested.rlog", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));

    // Control: the fetcher itself, given the same stream-attribute equality,
    // returns BOTH records — the nested-map stream is a false positive. This
    // proves the scan's exclusion below is doing real work.
    let query = LogQuery::new(i64::MIN, i64::MAX).with_stream_attr(StreamAttrEquals::new(
        "service.name",
        AttrValue::Str("api".into()),
    ));
    let control = fetcher
        .fetch(&seg, &query)
        .await
        .expect("fetch")
        .expect("in range");
    let control_ts: BTreeSet<i64> = control.records.iter().map(|r| r.ts_ns).collect();
    assert_eq!(
        control_ts,
        BTreeSet::from([1, 2]),
        "the fetcher over-approximates and returns the nested-map false positive"
    );

    // The scan re-verifies and keeps only the genuine top-level match (ts=1).
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
    };
    let provider = LogsTableProvider::new(snapshot, fetcher, EngineConfig::default());
    let pushdown = LogsPushdown {
        stream_attrs: vec![StreamAttrEquals::new(
            "service.name",
            AttrValue::Str("api".into()),
        )],
        ..LogsPushdown::default()
    };
    let plan = provider.plan_pushdown(2, &pushdown).expect("build plan");
    let batches = collect_plan(plan).await;
    let got = batches_to_rows(&batches);

    assert_eq!(
        got,
        BTreeSet::from([(1, "hello from plain".to_string())]),
        "scan must exclude the nested-map false positive (ts=2) and keep only \
         the genuine top-level attribute record (ts=1)"
    );
}
