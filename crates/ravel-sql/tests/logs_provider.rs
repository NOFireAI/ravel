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
//! - `stream_attr_equality_is_resolved_by_the_residual`: `attrs['k']='v'` is not
//!   pushed as a fetch prune (a stream-level prune is unsound against the merged
//!   `attrs` column, ADR-0033 amendment); it is evaluated by DataFusion's
//!   residual over the merged column. The test pins both directions that a
//!   stream-level prune would get wrong: a record matching only via a nested
//!   `Map` value is excluded, and a record matching only via a per-record
//!   attribute that overrides its resource attribute is kept.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, MapArray, StringArray, StringBuilder, StructArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher};
use ravel_sql::{LogsTableProvider, has_word_udf};
use ravel_types::accounting::QueryAccounting;
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

/// A record on the stream identified by an arbitrary resource attribute set,
/// with no per-record attributes.
fn record_with_resource(resource: &[(String, AttrValue)], ts: i64, body: &str) -> LogRecord {
    record_with_resource_and_attrs(resource, ts, body, &[])
}

/// A record on the stream identified by `resource`, carrying per-record dynamic
/// `attrs` (which win over resource/scope attributes on a key collision in the
/// merged `attrs` column).
fn record_with_resource_and_attrs(
    resource: &[(String, AttrValue)],
    ts: i64,
    body: &str,
    attrs: &[(String, AttrValue)],
) -> LogRecord {
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
        attrs: attrs.to_vec(),
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
    let provider =
        LogsTableProvider::new(snapshot, fetcher, EngineConfig::default(), QueryAccounting::new());

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

/// `attrs['service.name'] = 'api'` is resolved entirely by DataFusion's residual
/// over the merged `attrs` column, not by any stream-level fetch prune (ADR-0033
/// amendment). This pins both directions a stream-level prune gets wrong:
///
/// - ts=2's stream matches `service.name = 'api'` only via a value nested inside
///   a `Map` attribute, not a genuine top-level attribute. The merged column
///   omits nested values, so the residual excludes it. (The fetcher's
///   byte-containment STREAM_DIR match would have treated it as a positive.)
/// - ts=3's stream carries a genuine top-level `service.name = 'worker'`, but the
///   record overrides it with a per-record `service.name = 'api'`. The merge
///   resolves the key to the record's value (record wins), so the residual keeps
///   it. A `Predicate::StreamIn` built from stream-level attributes would have
///   dropped it before DataFusion ever ran — the data-loss bug this fix closes.
#[tokio::test]
async fn stream_attr_equality_is_resolved_by_the_residual() {
    let store = MemoryStore::new();

    // ts=1: genuine top-level `service.name = "api"` -> kept.
    let plain = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    // ts=2: no top-level `service.name`; the pair only appears nested inside a
    // `k8s.labels` map value. Merged column omits nested values -> excluded.
    let nested = vec![(
        "k8s.labels".to_string(),
        AttrValue::Map(vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )]),
    )];
    // ts=3: resource `service.name = "worker"`, overridden by a per-record
    // `service.name = "api"` (record wins in the merge) -> kept.
    let overridden_resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("worker".to_string()),
    )];

    let records = vec![
        record_with_resource(&plain, 1, "plain"),
        record_with_resource(&nested, 2, "nested"),
        record_with_resource_and_attrs(
            &overridden_resource,
            3,
            "override",
            &[(
                "service.name".to_string(),
                AttrValue::Str("api".to_string()),
            )],
        ),
    ];
    let seg = write_object(&store, "logs/attrs.rlog", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));

    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
    };
    let provider =
        LogsTableProvider::new(snapshot, fetcher, EngineConfig::default(), QueryAccounting::new());

    // A full SessionContext query so DataFusion's residual FilterExec actually
    // runs the `attrs['service.name'] = 'api'` equality (via the `get_field` UDF)
    // over the merged column, which is the sole correctness mechanism here.
    let ctx = SessionContext::new();
    let get_field = ScalarUDF::from(GetField::new());
    ctx.register_udf(get_field.clone());
    ctx.register_table("logs", Arc::new(provider))
        .expect("register table");
    let df = ctx
        .table("logs")
        .await
        .expect("table")
        .filter(
            get_field
                .call(vec![col("attrs"), lit("service.name")])
                .eq(lit("api")),
        )
        .expect("filter");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let batches = collect_plan(plan).await;
    let got = batches_to_rows(&batches);

    assert_eq!(
        got,
        BTreeSet::from([(1, "plain".to_string()), (3, "override".to_string())]),
        "residual must keep the top-level match (ts=1) and the record-override \
         match (ts=3), and exclude the nested-map non-match (ts=2)"
    );
}

/// A minimal, functional `get_field(map, 'key') -> Utf8` over a
/// `Map(Utf8, Utf8)` column: the value stored for `key` in each row's map, or
/// NULL when the key is absent.
///
/// Named `get_field` so [`ravel_sql`]'s pushdown extractor recognizes the
/// `attrs['k']` shape exactly as it would DataFusion's own subscript lowering,
/// while giving the residual `FilterExec` a real evaluator to run. The crate's
/// DataFusion build registers no nested-expression planner (`features =
/// ["sql"]`), which is why the `attrs['k']` SQL *text* cannot plan today
/// (tracked as a gate item for #240); this test therefore builds the equivalent
/// expression programmatically.
#[derive(Debug, PartialEq, Eq, Hash)]
struct GetField {
    signature: Signature,
}

impl GetField {
    fn new() -> Self {
        GetField {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GetField {
    fn name(&self) -> &str {
        "get_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    /// Accept the argument types as given (a `Map` first arg and a `Utf8` key);
    /// `Signature::user_defined` delegates coercion here.
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let key = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(k))) => k.clone(),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "get_field test udf: key must be a Utf8 literal, got {other:?}"
                )));
            }
        };
        let map_arr = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let map = map_arr
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("get_field test udf: first arg must be a Map column");
        let mut out = StringBuilder::new();
        for i in 0..map.len() {
            if map.is_null(i) {
                out.append_null();
                continue;
            }
            let entries = map.value(i);
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("map entries struct");
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("map keys utf8");
            let vals = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("map values utf8");
            let mut hit = None;
            for j in 0..keys.len() {
                if !keys.is_null(j) && keys.value(j) == key {
                    hit = Some(j);
                    break;
                }
            }
            match hit {
                Some(j) if !vals.is_null(j) => out.append_value(vals.value(j)),
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Regression test for the issue #239 data-loss bug: DataFusion's mandatory
/// `Inexact` residual re-applies `attrs['service.name'] = 'api'` against the
/// `attrs` column, so that column must carry resource/scope data merged with the
/// record's own attributes. If `attrs` holds only per-record dynamic attributes,
/// a record whose `service.name` is a genuine *resource* attribute (the normal
/// OTLP shape) is silently dropped by the residual — the bug the merged column
/// closed. The residual is the sole correctness mechanism (no fetch prune or
/// scan re-check narrows the attribute set; ADR-0033 amendment, issue #241).
///
/// This drives a REAL `TableProvider::scan` inside a `SessionContext` (unlike
/// `scan_prunes_by_ts_and_word_returns_exact_rows`, which executes the scan leaf
/// directly and so never runs the residual `FilterExec` that exposes this bug).
///
/// Records:
/// - ts=1: `service.name = "api"` ONLY as a resource attribute, no dynamic
///   attrs. The record-only `attrs` column omitted it -> the dropped row.
/// - ts=2: a different resource `service.name = "worker"` (non-matching stream).
/// - ts=3: `service.name = "api"` as a genuine per-record dynamic attribute, in
///   a stream that ALSO carries resource `service.name = "api"` (exercises the
///   merge collision rule: record value wins, one key, still `"api"`).
#[tokio::test]
async fn residual_recheck_keeps_resource_only_stream_attr_match() {
    let store = MemoryStore::new();

    let rec1 = record_with_resource(
        &[("service.name".into(), AttrValue::Str("api".into()))],
        1,
        "resource only",
    );
    let rec2 = record_with_resource(
        &[("service.name".into(), AttrValue::Str("worker".into()))],
        2,
        "other stream",
    );
    let mut rec3 = record_with_resource(
        &[("service.name".into(), AttrValue::Str("api".into()))],
        3,
        "record attr",
    );
    rec3.attrs = vec![("service.name".into(), AttrValue::Str("api".into()))];

    let seg = write_object(&store, "logs/resource-attr.rlog", &[rec1, rec2, rec3]).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
    };
    let provider =
        LogsTableProvider::new(snapshot, fetcher, EngineConfig::default(), QueryAccounting::new());

    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(GetField::new()));
    ctx.register_table("logs", Arc::new(provider))
        .expect("register table");

    // WHERE get_field(attrs, 'service.name') = 'api' -- the programmatic form of
    // `attrs['service.name'] = 'api'`, so this drives the real
    // supports_filters_pushdown -> scan -> residual FilterExec path.
    let get_field = ScalarUDF::from(GetField::new());
    let filter = get_field
        .call(vec![col("attrs"), lit("service.name")])
        .eq(lit("api"));

    let df = ctx
        .table("logs")
        .await
        .expect("table")
        .filter(filter)
        .expect("filter");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let batches = collect_plan(plan).await;

    let mut got = BTreeSet::new();
    for batch in &batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        for i in 0..batch.num_rows() {
            got.insert(ts.value(i));
        }
    }

    assert_eq!(
        got,
        BTreeSet::from([1, 3]),
        "residual must keep ts=1 (resource-only match) and ts=3 (record attr), \
         and exclude ts=2 (non-matching stream); got {got:?}"
    );
}
