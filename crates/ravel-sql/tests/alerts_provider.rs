//! Integration tests for [`ravel_sql::AlertsTableProvider`] (ADR-0040), the `alerts` SQL table over an already-resolved `Signal::Alerts`
//! snapshot.
//!
//! Alert records ride RLOG v1 verbatim (ADR-0040 decision 2), so these fixtures
//! are built directly with `ravel-logseg`'s `RlogWriter` -- the same way the
//! `logs` table's acceptance test builds its objects -- rather than through a
//! live ingest path. Each alert record carries the ADR-0040 attribute
//! convention in its per-record `attrs`: `alert_id` (a hex stable id), `rule_id`,
//! `state`, `generation` (an `I64`), and `label.<name>` entries.
//!
//! Two properties are pinned:
//!
//! - `scan_prunes_by_ts_returns_exact_promoted_rows`: a ts range returns exactly
//!   the surviving records across several objects, with the four scalar fields
//!   promoted into typed columns, checked against an independent record-by-record
//!   oracle (no false positives, no false negatives).
//! - `alert_id_and_rule_id_equality_fast_paths`: `alert_id = '<hex>'` and
//!   `rule_id = '<name>'` are pushed as exact per-record `Predicate::Equals`
//!   content predicates. The scan leaf is executed directly (no DataFusion
//!   residual `FilterExec`), so the returned rows are filtered *entirely* by the
//!   pushed predicate -- proving the fast path is both sound and actually
//!   applied.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Int64Array, MapArray, StringArray, StructArray, TimestampNanosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::AlertsTableProvider;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        // Matches the TenantHash([7u8; 16]) this test fetches as; the RLOG
        // read path enforces a footer tenant_hash check, so a footer naming a
        // different tenant than the fetch fails closed.
        tenant_hash: [7u8; 16],
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

/// The single synthetic alert stream: alert records carry their structured
/// fields in per-record `attrs`, so the stream identity itself is empty
/// resource + a fixed `alerts` scope. Every record here shares it (the read
/// path does not require one-stream-per-rule; that is a write-path convention).
fn alert_record(
    ts: i64,
    alert_id: &str,
    rule_id: &str,
    state: &str,
    generation: i64,
    labels: &[(&str, &str)],
) -> LogRecord {
    let mut attrs = vec![
        ("alert_id".to_string(), AttrValue::Str(alert_id.to_string())),
        ("rule_id".to_string(), AttrValue::Str(rule_id.to_string())),
        ("state".to_string(), AttrValue::Str(state.to_string())),
        ("generation".to_string(), AttrValue::I64(generation)),
    ];
    for (k, v) in labels {
        attrs.push((format!("label.{k}"), AttrValue::Str(v.to_string())));
    }
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&[], "alerts", "1", &[]),
        stream_attrs: stream_attrs_bytes(&[], "alerts", "1", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 0,
        severity_text: state.to_string(),
        body: format!("alert {alert_id} {state}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// [`alert_record`] whose alert stream carries `resource` attributes in its
/// `stream_attrs` (the RESOURCE position) rather than only per-record `attrs`.
/// Used to prove selective erasure catches a subject named only at the merged
/// resource/scope level.
fn alert_record_on_resource(
    resource: &[(String, AttrValue)],
    ts: i64,
    alert_id: &str,
    rule_id: &str,
    state: &str,
    generation: i64,
) -> LogRecord {
    let mut r = alert_record(ts, alert_id, rule_id, state, generation, &[]);
    r.stream_id = ravel_types::logstream::log_stream_id(resource, "alerts", "1", &[]);
    r.stream_attrs = stream_attrs_bytes(resource, "alerts", "1", &[]);
    r
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
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    }
}

fn ts_lit(v: i64) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::TimestampNanosecond(Some(v), None))
}

/// One promoted alert row: `(ts, alert_id, rule_id, state, generation)`.
type Row = (
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

fn batches_to_rows(batches: &[RecordBatch]) -> BTreeSet<Row> {
    let mut out = BTreeSet::new();
    for batch in batches {
        assert_eq!(
            batch.schema(),
            ravel_sql::alerts_schema(),
            "public alerts schema"
        );
        let ts = col_ts(batch, 0);
        let alert_id = col_str(batch, 1);
        let rule_id = col_str(batch, 2);
        let state = col_str(batch, 3);
        let generation = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("generation col");
        for i in 0..batch.num_rows() {
            let gen_val = if generation.is_null(i) {
                None
            } else {
                Some(generation.value(i))
            };
            out.insert((
                ts.value(i),
                str_at(alert_id, i),
                str_at(rule_id, i),
                str_at(state, i),
                gen_val,
            ));
        }
    }
    out
}

fn col_ts(batch: &RecordBatch, i: usize) -> &TimestampNanosecondArray {
    batch
        .column(i)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("timestamp col")
}

fn col_str(batch: &RecordBatch, i: usize) -> &StringArray {
    batch
        .column(i)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string col")
}

fn str_at(arr: &StringArray, i: usize) -> Option<String> {
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i).to_string())
    }
}

async fn collect_plan(plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

fn provider(store: MemoryStore, segments: Vec<SegmentRef>) -> AlertsTableProvider {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    AlertsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    )
}

/// A ts range returns exactly the surviving records across several objects, with
/// the four scalar fields promoted into typed columns, checked against an
/// independent oracle.
#[tokio::test]
async fn scan_prunes_by_ts_returns_exact_promoted_rows() {
    let store = MemoryStore::new();

    // Object A: ts 100..=110, rule "cpu-high", one alert cycling firing/resolved.
    let obj_a: Vec<LogRecord> = (100..=110)
        .map(|ts| {
            let (state, gen_val) = if ts % 2 == 0 {
                ("firing", 1)
            } else {
                ("resolved", 2)
            };
            alert_record(ts, "aa01", "cpu-high", state, gen_val, &[("env", "prod")])
        })
        .collect();
    // Object B: ts 1000..=1010, entirely outside the query range (pruned before GET).
    let obj_b: Vec<LogRecord> = (1000..=1010)
        .map(|ts| alert_record(ts, "bb02", "mem-high", "firing", 1, &[("env", "stage")]))
        .collect();
    // Object C: ts 200..=205, rule "latency", a suppressed alert (generation 3).
    let obj_c: Vec<LogRecord> = (200..=205)
        .map(|ts| alert_record(ts, "cc03", "latency", "suppressed", 3, &[]))
        .collect();

    let ref_a = write_object(&store, "alerts/a.rlog", &obj_a).await;
    let ref_b = write_object(&store, "alerts/b.rlog", &obj_b).await;
    let ref_c = write_object(&store, "alerts/c.rlog", &obj_c).await;

    let provider = provider(store, vec![ref_a, ref_b, ref_c]);

    // WHERE ts_ns >= 100 AND ts_ns <= 250
    let (lo, hi) = (100i64, 250i64);
    let filters = vec![
        col("ts_ns").gt_eq(ts_lit(lo)),
        col("ts_ns").lt_eq(ts_lit(hi)),
    ];
    let plan = provider.plan_filters(4, &filters).expect("build plan");
    let got = batches_to_rows(&collect_plan(plan).await);

    // Independent oracle: every source record whose ts is in [lo, hi], with its
    // promoted fields read straight off the constructed record.
    let mut want = BTreeSet::new();
    for records in [&obj_a, &obj_b, &obj_c] {
        for r in records {
            if r.ts_ns >= lo && r.ts_ns <= hi {
                want.insert(expected_row(r));
            }
        }
    }
    // Sanity: object B is fully out of range, so nothing from "mem-high".
    assert!(
        want.iter()
            .all(|(_, _, rule, _, _)| rule.as_deref() != Some("mem-high"))
    );
    assert_eq!(got, want, "scan output must equal the oracle exactly");
}

/// The promoted-row an alert record must decode to, read directly from its
/// constructed `attrs` (the independent oracle).
fn expected_row(r: &LogRecord) -> Row {
    let find = |k: &str| {
        r.attrs
            .iter()
            .find(|(ak, _)| ak == k)
            .map(|(_, v)| v.clone())
    };
    let as_str = |k: &str| match find(k) {
        Some(AttrValue::Str(s)) => Some(s),
        _ => None,
    };
    let gen_val = match find("generation") {
        Some(AttrValue::I64(v)) => Some(v),
        _ => None,
    };
    (
        r.ts_ns,
        as_str("alert_id"),
        as_str("rule_id"),
        as_str("state"),
        gen_val,
    )
}

/// `alert_id = '<hex>'` and `rule_id = '<name>'` are pushed as exact per-record
/// `Predicate::Equals`. Executing the scan leaf directly runs no residual
/// `FilterExec`, so the rows returned are filtered entirely by the pushed
/// predicate: an exact match proves the fast path both fires and is sound.
#[tokio::test]
async fn alert_id_and_rule_id_equality_fast_paths() {
    let store = MemoryStore::new();

    // Two rules, two alert ids, interleaved across one object's ts span.
    let mut records = Vec::new();
    for ts in 0..12i64 {
        let rec = match ts % 3 {
            0 => alert_record(ts, "id-a", "cpu-high", "firing", 1, &[]),
            1 => alert_record(ts, "id-b", "cpu-high", "resolved", 2, &[]),
            _ => alert_record(ts, "id-c", "mem-high", "firing", 1, &[]),
        };
        records.push(rec);
    }
    let seg = write_object(&store, "alerts/mixed.rlog", &records).await;
    let provider = provider(store, vec![seg]);

    // alert_id = 'id-b' -> only the four `id-b` records (ts % 3 == 1).
    let plan = provider
        .plan_filters(4, &[col("alert_id").eq(lit("id-b"))])
        .expect("build plan");
    let got = batches_to_rows(&collect_plan(plan).await);
    let want: BTreeSet<Row> = records
        .iter()
        .filter(|r| r.ts_ns % 3 == 1)
        .map(expected_row)
        .collect();
    assert!(!want.is_empty(), "oracle must select some rows");
    assert_eq!(
        got, want,
        "alert_id equality fast path must return exactly the matches"
    );
    assert!(
        got.iter()
            .all(|(_, aid, _, _, _)| aid.as_deref() == Some("id-b")),
        "no non-'id-b' rows may leak through the pushed predicate"
    );

    // rule_id = 'mem-high' -> only the "mem-high" records (ts % 3 == 2).
    let plan = provider
        .plan_filters(4, &[col("rule_id").eq(lit("mem-high"))])
        .expect("build plan");
    let got = batches_to_rows(&collect_plan(plan).await);
    let want: BTreeSet<Row> = records
        .iter()
        .filter(|r| r.ts_ns % 3 == 2)
        .map(expected_row)
        .collect();
    assert!(!want.is_empty(), "oracle must select some rows");
    assert_eq!(
        got, want,
        "rule_id equality fast path must return exactly the matches"
    );
    assert!(
        got.iter()
            .all(|(_, _, rule, _, _)| rule.as_deref() == Some("mem-high")),
        "no non-'mem-high' rows may leak through the pushed predicate"
    );

    // A non-existent id returns nothing (the pushed predicate is exact, not a
    // widen-only hint that would leak rows).
    let plan = provider
        .plan_filters(4, &[col("alert_id").eq(lit("nope"))])
        .expect("build plan");
    assert!(batches_to_rows(&collect_plan(plan).await).is_empty());
}

/// The `attrs` column carries the whole record attribute map (the promoted keys
/// plus every `label.<name>`), stringified into `Map(Utf8, Utf8)`.
#[tokio::test]
async fn attrs_map_exposes_labels_and_promoted_keys() {
    let store = MemoryStore::new();
    let rec = alert_record(
        5,
        "id-x",
        "disk-full",
        "firing",
        4,
        &[("env", "prod"), ("team", "sre")],
    );
    let seg = write_object(&store, "alerts/one.rlog", &[rec]).await;
    let provider = provider(store, vec![seg]);

    let plan = provider.plan(1).expect("build plan");
    let batches = collect_plan(plan).await;
    let map = attrs_map(&batches[0], 0);

    assert_eq!(map_get(&map, "alert_id"), Some("id-x".to_string()));
    assert_eq!(map_get(&map, "rule_id"), Some("disk-full".to_string()));
    assert_eq!(map_get(&map, "state"), Some("firing".to_string()));
    // The I64 generation renders to its decimal text in the string-valued map.
    assert_eq!(map_get(&map, "generation"), Some("4".to_string()));
    assert_eq!(map_get(&map, "label.env"), Some("prod".to_string()));
    assert_eq!(map_get(&map, "label.team"), Some("sre".to_string()));
}

/// This table's fixtures elsewhere in this file (`alert_record`) hand-encode
/// RLOG attrs matching the ADR-0040 convention this crate assumes -
/// `ravel-alerting` was not available when this table was first built, so
/// nothing here had actually been checked against its real writer. This test
/// closes that gap: it builds a record through `ravel-alerting`'s own
/// `build_transition_record`/`encode_record_object` (the real path a live
/// evaluator would use), and confirms this table extracts identical values
/// for every promoted column and every attrs-map entry. A future rename of an
/// attr key or a change to `generation`'s stored type in `ravel-alerting`
/// would silently break this table without ever failing a test in either
/// crate alone; this is the one place that drift is checked.
#[tokio::test]
async fn real_ravel_alerting_record_decodes_identically() {
    use ravel_alerting::{
        AlertState, Rule, RuleCondition, RuleQuery, ThresholdOp, build_transition_record,
        encode_record_object,
    };

    let rule = Rule {
        rule_id: "disk-full".to_string(),
        query: RuleQuery::Promql("disk_free_bytes < 1e9".to_string()),
        condition: RuleCondition::Threshold {
            op: ThresholdOp::Lt,
            threshold: 1e9,
        },
        labels: vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "sre".to_string()),
        ],
        annotations: vec![],
        for_duration: None,
        max_alert_generation: None,
        repeat_interval: None,
    };
    let record = build_transition_record(&rule, AlertState::Firing, 4, 5);
    let object = encode_record_object(&record, small_blocks(), identity()).expect("encode");

    let store = MemoryStore::new();
    let key = "alerts/real.rlog";
    let size = object.len() as u64;
    store
        .put(key, bytes::Bytes::from(object), PutOptions::default())
        .await
        .expect("put object");
    let seg = SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: 5,
        max_event_ts_ns: 5,
        ingest_hour_bucket: 0,
        sample_count: 1,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    };

    let provider = provider(store, vec![seg]);
    let plan = provider.plan(1).expect("build plan");
    let batches = collect_plan(plan).await;
    assert_eq!(batches[0].num_rows(), 1);

    assert_eq!(
        str_at(col_str(&batches[0], 1), 0),
        Some(record.alert_id.to_hex()),
        "alert_id column matches the real crate's hex encoding"
    );
    assert_eq!(
        str_at(col_str(&batches[0], 2), 0),
        Some("disk-full".to_string())
    );
    assert_eq!(
        str_at(col_str(&batches[0], 3), 0),
        Some("firing".to_string())
    );

    let map = attrs_map(&batches[0], 0);
    assert_eq!(map_get(&map, "alert_id"), Some(record.alert_id.to_hex()));
    assert_eq!(map_get(&map, "rule_id"), Some("disk-full".to_string()));
    assert_eq!(map_get(&map, "state"), Some("firing".to_string()));
    assert_eq!(map_get(&map, "generation"), Some("4".to_string()));
    assert_eq!(map_get(&map, "label.env"), Some("prod".to_string()));
    assert_eq!(map_get(&map, "label.team"), Some("sre".to_string()));
}

/// (ADR-0064 decision 3): a pending selective-erasure
/// request on the resolved snapshot excludes matching rows through the real
/// `AlertsTableProvider` scan path, the one the SQL `alerts` table uses in
/// production. Covers `AlertsTableProvider::build_scan` passing the
/// snapshot-derived predicates into `AlertsScanExec` (alerts_provider.rs) and
/// `AlertsScanExec` calling `LogQuery::with_erasure` before fetch
/// (alerts_scan.rs); reverting `.with_erasure((*erasure).clone())` back to a
/// bare `LogQuery::new(ts_min, ts_max)` in alerts_scan.rs makes the erased row
/// reappear.
#[tokio::test]
async fn pending_erasure_excludes_matching_rows() {
    let store = MemoryStore::new();
    let keep = alert_record(1, "id-keep", "cpu-high", "firing", 1, &[("env", "prod")]);
    let erase = alert_record(2, "id-erase", "cpu-high", "firing", 1, &[("env", "prod")]);
    let seg = write_object(&store, "alerts/erasure.rlog", &[keep.clone(), erase]).await;

    let request = ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "alert_id".to_string(),
            value: "id-erase".to_string(),
        }],
        ..Default::default()
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: vec![request],
    };
    let provider = AlertsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

    let got = batches_to_rows(&collect_plan(provider.plan(1).expect("build plan")).await);
    assert_eq!(
        got,
        BTreeSet::from([expected_row(&keep)]),
        "the erased alert_id is excluded; the other row survives"
    );
}

/// (ADR-0064): a subject named ONLY in a RESOURCE/scope
/// (`stream_attrs`) attribute must also be excluded from the `alerts` table.
/// The `attrs` column materializes the merged resource + scope + record view, so
/// `user_id` is queryable, yet the fetcher-level filter matches per-record
/// attributes alone and never sees it. Before the scan-layer `retain_unerased`
/// in `alerts_scan.rs::prepare_partition`, this row leaked; removing that call
/// reintroduces the leak.
#[tokio::test]
async fn pending_erasure_excludes_resource_attribute_rows() {
    let store = MemoryStore::new();
    // `user_id` lives in the RESOURCE position (stream_attrs), not per-record.
    let keep = alert_record_on_resource(
        &[("user_id".to_string(), AttrValue::Str("u2".to_string()))],
        1,
        "id-keep",
        "cpu-high",
        "firing",
        1,
    );
    let erase = alert_record_on_resource(
        &[("user_id".to_string(), AttrValue::Str("u1".to_string()))],
        2,
        "id-erase",
        "cpu-high",
        "firing",
        1,
    );
    let seg = write_object(
        &store,
        "alerts/erasure-resource.rlog",
        &[keep.clone(), erase],
    )
    .await;

    let request = ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: "u1".to_string(),
        }],
        ..Default::default()
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: vec![request],
    };
    let provider = AlertsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

    let got = batches_to_rows(&collect_plan(provider.plan(1).expect("build plan")).await);
    assert_eq!(
        got,
        BTreeSet::from([expected_row(&keep)]),
        "the resource-only subject u1 is excluded; u2's row survives"
    );
}

/// The `(key, value)` pairs of one row's `attrs` map cell.
fn attrs_map(batch: &RecordBatch, row: usize) -> Vec<(String, String)> {
    let map = batch
        .column(5)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("attrs map col");
    let entries = map.value(row);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map entries struct");
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keys");
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("vals");
    (0..keys.len())
        .map(|j| (keys.value(j).to_string(), vals.value(j).to_string()))
        .collect()
}

fn map_get(map: &[(String, String)], key: &str) -> Option<String> {
    map.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}
