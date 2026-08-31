//! Integration tests for [`ravel_sql::LogsTableProvider`] (ADR-0033), the `logs` SQL table over an already-resolved `Signal::Logs`
//! snapshot.
//!
//! Two properties are pinned:
//!
//! - `scan_prunes_by_ts_and_word_returns_exact_rows` (the acceptance test):
//!   a ts range + `has_word` combination returns exactly
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
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{DeclaredColumn, DeclaredType, LogsTableProvider, has_word_udf};
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
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
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
        pending_erasure: Vec::new(),
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

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
        pending_erasure: Vec::new(),
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

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
/// (nested-expression planning is not yet wired); this test therefore builds the equivalent
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

/// Regression test for a data-loss bug: DataFusion's mandatory
/// `Inexact` residual re-applies `attrs['service.name'] = 'api'` against the
/// `attrs` column, so that column must carry resource/scope data merged with the
/// record's own attributes. If `attrs` holds only per-record dynamic attributes,
/// a record whose `service.name` is a genuine *resource* attribute (the normal
/// OTLP shape) is silently dropped by the residual — the bug the merged column
/// closed. The residual is the sole correctness mechanism (no fetch prune or
/// scan re-check narrows the attribute set; ADR-0033 amendment).
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
        pending_erasure: Vec::new(),
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

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

// ---------------------------------------------------------------------------
// ADR-0087: streaming, column-projecting scan
// ---------------------------------------------------------------------------

/// A `SessionContext` built through the real production path
/// (`ravel_sql::build_session`), the same one `/api/v1/sql` and Flight SQL use,
/// with `provider` registered as `logs`.
fn logs_session(
    provider: LogsTableProvider,
) -> datafusion::error::Result<datafusion::prelude::SessionContext> {
    let config = ravel_sql::SqlConfig::default();
    let tenant = ravel_sql::TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    ravel_sql::build_session(
        &config,
        pool,
        ravel_sql::SessionTable::Logs(Arc::new(provider)),
        false,
        ravel_sql::SpillDecision::Disabled,
    )
}

/// The `LogsScanExec` leaf of a physical plan. The plan above it is whatever
/// the optimizer built, so the leaf is found by walking rather than by shape.
fn find_by_name(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    name: &str,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    if plan.name() == name {
        return Some(Arc::clone(plan));
    }
    plan.children().iter().find_map(|c| find_by_name(c, name))
}

/// Two streams whose records interleave in time, written to two objects, so
/// neither the reader's `(stream_ref, ts)` grouping within an object nor the
/// segment order across the partition is `ts` ascending. This is what makes the
/// `ORDER BY ts` assertion below non-vacuous.
fn interleaved_objects() -> (Vec<LogRecord>, Vec<LogRecord>) {
    // Object A: "api" at even ts 0..40, "worker" at odd ts 1..41. The writer
    // sorts by (stream_ref, ts), so A emits all of one stream then all of the
    // other: not ts ascending.
    let a: Vec<LogRecord> = (0..20)
        .flat_map(|i| {
            [
                record("api", i * 2, &format!("api {}", i * 2)),
                record("worker", i * 2 + 1, &format!("worker {}", i * 2 + 1)),
            ]
        })
        .collect();
    // Object B covers a ts band that *precedes* part of A, so concatenating the
    // partition's segments in order is not ts ascending either.
    let b: Vec<LogRecord> = (0..20)
        .map(|i| record("db", i * 2 + 100, &format!("db {}", i * 2 + 100)))
        .collect();
    (a, b)
}

/// ADR-0087 decision 1's must-not-regress case: the leaf declares no ordering,
/// and `ORDER BY ts` is nonetheless exactly sorted because DataFusion inserts a
/// sort above it.
///
/// Three things are asserted together, and all three are needed. The rows are
/// exactly the reference multiset (nothing lost or duplicated); the emitted
/// sequence is `ts` ascending (the ordering itself); and the plan contains a
/// `SortExec` above the `LogsScanExec` while the leaf's own
/// `PlanProperties` declare no output ordering (the ordering comes from the
/// inserted sort, not from a leaf claim).
///
/// Against a naive streaming implementation that keeps the old
/// `EquivalenceProperties::new_with_orderings(... ts asc ...)` in
/// `LogsScanExec::compute_properties` and simply drops the partition sort,
/// DataFusion trusts the leaf, inserts no `SortExec`, and the ts-ascending
/// assertion fails on the very first out-of-order pair.
#[tokio::test]
async fn order_by_ts_is_sorted_by_an_inserted_sort_not_by_a_leaf_claim() {
    let store = MemoryStore::new();
    let (obj_a, obj_b) = interleaved_objects();
    let ref_a = write_object(&store, "logs/order-a.rlog", &obj_a).await;
    let ref_b = write_object(&store, "logs/order-b.rlog", &obj_b).await;
    let snapshot = Snapshot {
        segments: vec![ref_a, ref_b],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        LogSegmentFetcher::new(backend),
        QueryAccounting::new(),
    );

    // The leaf itself declares nothing. Read it off the unsorted plan so this
    // is a statement about `LogsScanExec`, not about whatever the optimizer
    // wrapped it in.
    let bare = provider.plan(4).expect("plan");
    assert!(
        bare.properties().output_ordering().is_none(),
        "the logs scan must declare no output ordering (ADR-0087 decision 1)"
    );

    let ctx = logs_session(provider).expect("session");

    // Unordered: prove the scan really does emit out of ts order, so the
    // ordered assertion below is not satisfied by accident.
    let unordered = ctx
        .sql("SELECT ts, body FROM logs")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let emitted = ts_sequence(&unordered);
    assert!(
        emitted.windows(2).any(|w| w[0] > w[1]),
        "fixture must not already be ts-ascending out of the scan, else the \
         ORDER BY assertion proves nothing"
    );

    let plan = ctx
        .sql("SELECT ts, body FROM logs ORDER BY ts")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    assert!(
        find_by_name(&plan, "SortExec").is_some(),
        "ORDER BY ts must be answered by a sort operator above the scan"
    );
    assert!(
        find_by_name(&plan, "LogsScanExec").is_some(),
        "the plan must still be over the logs scan"
    );

    let ordered = collect_plan(plan).await;
    let got = ts_sequence(&ordered);
    let mut want = emitted;
    want.sort_unstable();
    assert_eq!(
        got, want,
        "ORDER BY ts must return every scanned row exactly once, ts ascending"
    );
    assert!(
        got.windows(2).all(|w| w[0] <= w[1]),
        "ORDER BY ts output must be ascending"
    );
}

/// The `ts` values of `batches`, in emission order (column 0 of `SELECT ts, ...`).
fn ts_sequence(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        for i in 0..batch.num_rows() {
            out.push(ts.value(i));
        }
    }
    out
}

/// Number of dynamic attribute columns each record in the projection fixture
/// carries. Comfortably over the "100+ attributes" the ADR's motivating case
/// describes.
const WIDE_ATTRS: usize = 120;

/// One record carrying `WIDE_ATTRS` dynamic attributes. `a007` and `a042` vary
/// per record (so an erasure predicate can name one specific row); the rest are
/// constant, which changes nothing about how many *columns* exist.
fn wide_record(ts: i64) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let attrs: Vec<(String, AttrValue)> = (0..WIDE_ATTRS)
        .map(|i| {
            let key = format!("a{i:03}");
            let value = match i {
                7 => format!("r{ts}"),
                42 => format!("s{ts}"),
                _ => format!("v{i}"),
            };
            (key, AttrValue::Str(value))
        })
        .collect();
    record_with_resource_and_attrs(&resource, ts, &format!("body {ts}"), &attrs)
}

/// Sum a named `LogsScanExec` metric over the executed plan.
fn scan_metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> usize {
    find_by_name(plan, "LogsScanExec")
        .expect("a LogsScanExec leaf")
        .metrics()
        .expect("the scan publishes metrics")
        .sum_by_name(name)
        .map(|v| v.as_usize())
        .unwrap_or_else(|| panic!("metric {name} missing"))
}

/// ADR-0087 decision 3: a query that references two of a hundred-and-twenty
/// attributes decodes exactly those two columns' pages and walks past the rest.
///
/// The two referenced attributes here are named by *pending erasure
/// predicates*, not by the projection. That is deliberate and it is the only
/// shape the ADR leaves room for: the SQL surface exposes attributes as one
/// merged `attrs` Map column, so any query naming `attrs` at all resolves to
/// every dynamic column (per-key `attrs['k']` projection is explicitly out of
/// scope). Erasure predicates name individual attribute keys, so they exercise
/// the per-key resolution path that does exist -- and they are the case where
/// getting the column set wrong is a correctness bug, not just a performance
/// one: an erasure key the reader never decodes makes the erased row reappear.
///
/// Both halves are asserted: the page counts (projection reached the page
/// level) and the rows (the two erasures still bite, so the columns that *were*
/// decoded are the right ones).
#[tokio::test]
async fn column_projection_decodes_only_the_referenced_attribute_pages() {
    let store = MemoryStore::new();
    let records: Vec<LogRecord> = (0..12).map(wide_record).collect();
    let seg = write_object(&store, "logs/wide.rlog", &records).await;
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // Two pending erasure requests, each naming one attribute key: `a007` on
    // the ts=3 row and `a042` on the ts=5 row.
    let erasure = |key: &str, value: &str| ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: key.to_string(),
            value: value.to_string(),
        }],
        ..Default::default()
    };
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: vec![erasure("a007", "r3"), erasure("a042", "s5")],
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        LogSegmentFetcher::new(backend),
        QueryAccounting::new(),
    );
    let ctx = logs_session(provider).expect("session");

    let plan = ctx
        .sql("SELECT ts, body FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let batches = collect_plan(Arc::clone(&plan)).await;

    // Rows: everything except the two erased ones. This proves the two decoded
    // attribute columns are genuinely the erasure keys' columns -- had the
    // projection dropped them, the erasure would have found nothing to match
    // and both rows would be back.
    let got: BTreeSet<i64> = ts_sequence(&batches).into_iter().collect();
    let want: BTreeSet<i64> = (0..12).filter(|ts| *ts != 3 && *ts != 5).collect();
    assert_eq!(
        got, want,
        "the two erasure predicates must still exclude their rows"
    );

    let blocks = scan_metric(&plan, "blocks_scanned");
    let decoded = scan_metric(&plan, "pages_decoded");
    let skipped = scan_metric(&plan, "pages_skipped");
    assert!(blocks > 0, "the fixture must actually decode blocks");

    // Per block: `ts`, `stream_ref` (always), `body` (projected), and the two
    // erasure-named attribute columns. Nothing else. `attrs_raw` is in the
    // resolved set but occupies no page here (no record overflowed), and an
    // absent column costs no page either way.
    assert_eq!(
        decoded,
        5 * blocks,
        "expected 5 pages per block (ts, stream_ref, body, a007, a042), got \
         {decoded} over {blocks} blocks"
    );
    // Everything else in the block: the remaining 118 dynamic columns plus the
    // four unprojected always-present fixed columns (observed_ts, severity_num,
    // flags, severity_text).
    assert_eq!(
        skipped,
        (WIDE_ATTRS - 2 + 4) * blocks,
        "every unreferenced column's pages must be skipped, got {skipped} over \
         {blocks} blocks"
    );
    assert!(
        skipped > 20 * decoded,
        "the whole point: {skipped} pages skipped vs {decoded} decoded"
    );
}

/// The counterpart to the test above: a query that *does* reference `attrs`
/// gets every dynamic column, because the merged map's contract is that every
/// key is present (ADR-0087 decision 3, per-key projection out of scope).
/// Without this, "we skipped pages" could be true while `SELECT *` silently
/// returned a truncated map.
#[tokio::test]
async fn referencing_attrs_decodes_every_dynamic_column() {
    let store = MemoryStore::new();
    let records: Vec<LogRecord> = (0..6).map(wide_record).collect();
    let seg = write_object(&store, "logs/wide-attrs.rlog", &records).await;
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        LogSegmentFetcher::new(backend),
        QueryAccounting::new(),
    );
    let ctx = logs_session(provider).expect("session");

    let plan = ctx
        .sql("SELECT ts, attrs FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let batches = collect_plan(Arc::clone(&plan)).await;

    // Every record's map carries all WIDE_ATTRS dynamic keys plus the one
    // resource attribute.
    let mut rows = 0usize;
    for batch in &batches {
        let map = batch
            .column(1)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("attrs map col");
        for i in 0..batch.num_rows() {
            let entries = map.value(i);
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("map entries");
            assert_eq!(
                entries.len(),
                WIDE_ATTRS + 1,
                "a query referencing attrs must see every dynamic key plus the \
                 resource attribute"
            );
            rows += 1;
        }
    }
    assert_eq!(rows, 6);

    let blocks = scan_metric(&plan, "blocks_scanned");
    let skipped = scan_metric(&plan, "pages_skipped");
    // Only the five unprojected fixed columns (observed_ts, severity_num,
    // flags, severity_text, body) are skipped; every dynamic column is decoded.
    assert_eq!(
        skipped,
        5 * blocks,
        "referencing attrs must skip only the unprojected fixed columns"
    );
}

/// ADR-0099 decision 5 types a declared `Str` column `Dictionary(Int32, Utf8)`.
/// `has_word` must reach that dictionary column with NO materializing cast: an
/// exact `create_udf([Utf8, Utf8])` signature makes DataFusion insert
/// `CAST(name AS Utf8)` over the dictionary argument, which both hydrates the
/// column back to one value per row (defeating the per-distinct-value shape
/// #479 builds on) and leaves the dictionary arm of `has_word_impl` unreachable
/// by any plan. This asserts the optimized physical plan carries no `CAST`, and
/// that the query still returns exactly the matching rows -- so the dictionary
/// arm actually executes end to end, which a direct `has_word_impl` unit call
/// cannot prove.
#[tokio::test]
async fn has_word_over_declared_str_column_has_no_cast_and_returns_exact_rows() {
    let store = MemoryStore::new();
    // Low-cardinality declared `name` values so the page is a dict page, the
    // shape decision 5 targets. `name` matches "timeout" on the even rows.
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    let records: Vec<LogRecord> = (0..6)
        .map(|i| {
            let name = if i % 2 == 0 {
                "connection timeout"
            } else {
                "ok"
            };
            record_with_resource_and_attrs(
                &resource,
                i,
                "b",
                &[("name".to_string(), AttrValue::Str(name.to_string()))],
            )
        })
        .collect();
    let seg = write_object(&store, "logs/declared.rlog", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new("name", DeclaredType::Str)]);

    let ctx = SessionContext::new();
    ctx.register_udf(has_word_udf());
    ctx.register_table("logs", Arc::new(provider))
        .expect("register table");
    let df = ctx
        .table("logs")
        .await
        .expect("table")
        .filter(has_word_udf().call(vec![col("name"), lit("timeout")]))
        .expect("filter");
    let plan = df.create_physical_plan().await.expect("physical plan");

    // The only expression in the plan is the `has_word` residual over the
    // declared column; a `CAST` anywhere is the coercion the exact signature
    // would have inserted over the dictionary argument.
    let text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        !text.contains("CAST"),
        "has_word must take the dictionary column with no coercing CAST; plan was:\n{text}"
    );
    assert!(
        text.contains("has_word(name@"),
        "has_word's first argument must be the bare declared column, not a CAST; \
         plan was:\n{text}"
    );

    // End to end: the dictionary arm evaluates and returns exactly the even
    // rows, whose declared `name` token-contains "timeout".
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
        BTreeSet::from([0i64, 2, 4]),
        "has_word over the declared dictionary column must match exactly the \
         'connection timeout' rows"
    );
}

/// Probe matrix for `has_word`'s first-argument coercion. The user-defined
/// signature must leave a declared `Str` column's `Dictionary(Int32, Utf8)`
/// intact (no coercing CAST, so the per-distinct-value dictionary arm stays
/// reachable) while still accepting every first-argument type the original
/// `create_udf([Utf8, Utf8])` exact signature accepted. A prior fix over-narrowed
/// the signature to reject anything but `Utf8`/`Dictionary(Int32, Utf8)`, turning
/// ordinary queries -- notably `CAST(body AS VARCHAR)`, which DataFusion types as
/// `Utf8View` -- into planning errors. This pins each type end to end: it must
/// plan AND return the expected rows.
#[tokio::test]
async fn has_word_first_argument_coercion_matrix() {
    let store = MemoryStore::new();
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    // `body` and the declared `name` carry the same low-cardinality text, so
    // `name` encodes as a dict page (the shape decision 5 targets) and both
    // columns token-contain "timeout" on the same two rows (ts 0 and 2).
    let rows_spec = [
        (0i64, "connection timeout"),
        (1, "ok"),
        (2, "request timeout"),
        (3, "fine"),
    ];
    let records: Vec<LogRecord> = rows_spec
        .iter()
        .map(|(ts, text)| {
            record_with_resource_and_attrs(
                &resource,
                *ts,
                text,
                &[("name".to_string(), AttrValue::Str((*text).to_string()))],
            )
        })
        .collect();
    let seg = write_object(&store, "logs/matrix.rlog", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new("name", DeclaredType::Str)]);

    let ctx = SessionContext::new();
    ctx.register_udf(has_word_udf());
    ctx.register_table("logs", Arc::new(provider))
        .expect("register table");

    // (label, first-argument SQL expression, expected surviving ts values in
    // order). Each planned and answered under the old exact signature; the fix
    // must keep them planning and answering identically.
    let cases: &[(&str, &str, &[i64])] = &[
        ("Utf8 body", "body", &[0, 2]),
        ("declared Str dictionary", "name", &[0, 2]),
        (
            "SQL CAST(body AS VARCHAR) -> Utf8View",
            "CAST(body AS VARCHAR)",
            &[0, 2],
        ),
        ("LargeUtf8", "arrow_cast(body, 'LargeUtf8')", &[0, 2]),
        ("Utf8View", "arrow_cast(body, 'Utf8View')", &[0, 2]),
        (
            "Dictionary(Int8, Utf8)",
            "arrow_cast(name, 'Dictionary(Int8, Utf8)')",
            &[0, 2],
        ),
        // Int64 casts to its decimal text, which token-contains no "timeout".
        ("Int64", "arrow_cast(ts, 'Int64')", &[]),
        // A NULL first argument is never a match.
        ("NULL literal", "NULL", &[]),
        // A constant-true string literal matches every row.
        ("string literal", "'connection timeout'", &[0, 1, 2, 3]),
    ];

    for (label, first_arg, expected) in cases {
        let sql = format!("SELECT ts FROM logs WHERE has_word({first_arg}, 'timeout') ORDER BY ts");
        let df = ctx
            .sql(&sql)
            .await
            .unwrap_or_else(|e| panic!("[{label}] planning failed: {e}"));
        let batches = df
            .collect()
            .await
            .unwrap_or_else(|e| panic!("[{label}] execution failed: {e}"));
        let mut got = Vec::new();
        for batch in &batches {
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts col");
            for i in 0..batch.num_rows() {
                got.push(ts.value(i));
            }
        }
        assert_eq!(
            got, *expected,
            "[{label}] has_word({first_arg}, 'timeout') returned unexpected rows"
        );
    }

    // The dictionary case must additionally carry NO CAST in the optimized
    // physical plan: leaving `Dictionary(Int32, Utf8)` intact is the whole point
    // of the user-defined signature (kept from the earlier dedicated test).
    let dict_plan = ctx
        .sql("SELECT ts FROM logs WHERE has_word(name, 'timeout')")
        .await
        .expect("plan dict case")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let text = displayable(dict_plan.as_ref()).indent(true).to_string();
    assert!(
        !text.contains("CAST"),
        "has_word over the declared dictionary column must carry no coercing CAST; \
         plan was:\n{text}"
    );
    assert!(
        text.contains("has_word(name@"),
        "has_word's first argument must be the bare dictionary column; plan was:\n{text}"
    );
}

/// The #479 acceptance test: `col LIKE 'pattern'` over the `logs` table plans
/// with NO coercing CAST on a declared `Str` column (its `Dictionary(Int32,
/// Utf8)` reaches the Ravel `like` UDF intact, matched once per distinct value),
/// while still planning and answering correctly for every other first-argument
/// type DataFusion's built-in `LIKE` accepted. The second argument (the pattern)
/// is coerced to `Utf8` too, so a `LargeUtf8`/`Utf8View` pattern plans rather
/// than failing at execution.
///
/// This runs through the real `logs` session (`build_session`, via
/// [`logs_session`]), which is where the `LIKE` -> `like` function rewrite is
/// registered: a bare `SessionContext` would not rewrite `Expr::Like` at all, so
/// the dictionary fast path and its no-CAST plan would never be exercised. A
/// unit call into `like_impl` cannot catch the CAST class either -- it is handed
/// whatever array type the test picks, which is exactly what DataFusion's
/// coercion was silently changing.
#[tokio::test]
async fn like_matrix_plans_without_cast_and_matches_rows() {
    let store = MemoryStore::new();
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    // `body` (fixed Utf8) and the declared `name` (dictionary) carry the same
    // low-cardinality text; both substring-contain "timeout" on ts 0 and 2.
    let rows_spec = [
        (0i64, "connection timeout"),
        (1, "ok"),
        (2, "request timeout"),
        (3, "fine"),
    ];
    let records: Vec<LogRecord> = rows_spec
        .iter()
        .map(|(ts, text)| {
            record_with_resource_and_attrs(
                &resource,
                *ts,
                text,
                &[("name".to_string(), AttrValue::Str((*text).to_string()))],
            )
        })
        .collect();
    let seg = write_object(&store, "logs/like-matrix.rlog", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new("name", DeclaredType::Str)]);

    // The real production `logs` session: this is what registers the LIKE
    // rewrite. `arrow_cast` is an admitted scalar, so the cast probes plan.
    let ctx = logs_session(provider).expect("session");

    // (label, WHERE predicate, expected surviving ts values in order).
    let cases: &[(&str, &str, &[i64])] = &[
        // --- first-argument (matched column) coercion ---
        ("Utf8 body", "body LIKE '%timeout%'", &[0, 2]),
        ("declared Str dictionary", "name LIKE '%timeout%'", &[0, 2]),
        (
            "SQL CAST(body AS VARCHAR) -> Utf8View",
            "CAST(body AS VARCHAR) LIKE '%timeout%'",
            &[0, 2],
        ),
        (
            "LargeUtf8",
            "arrow_cast(body, 'LargeUtf8') LIKE '%timeout%'",
            &[0, 2],
        ),
        (
            "Utf8View",
            "arrow_cast(body, 'Utf8View') LIKE '%timeout%'",
            &[0, 2],
        ),
        (
            "Dictionary(Int8, Utf8)",
            "arrow_cast(name, 'Dictionary(Int8, Utf8)') LIKE '%timeout%'",
            &[0, 2],
        ),
        // Int64 casts to its decimal text ("0".."3"), which contains no
        // "timeout" substring.
        ("Int64", "arrow_cast(ts, 'Int64') LIKE '%timeout%'", &[]),
        // A NULL first argument is never a match (NULL LIKE x is NULL).
        ("NULL literal", "NULL LIKE '%timeout%'", &[]),
        // A constant-matching string literal matches every row.
        (
            "string literal",
            "'connection timeout' LIKE '%timeout%'",
            &[0, 1, 2, 3],
        ),
        // --- second-argument (pattern) coercion, which the impl must survive ---
        (
            "LargeUtf8 pattern",
            "body LIKE arrow_cast('%timeout%', 'LargeUtf8')",
            &[0, 2],
        ),
        (
            "Utf8View pattern",
            "body LIKE arrow_cast('%timeout%', 'Utf8View')",
            &[0, 2],
        ),
        // A NULL pattern makes every comparison NULL: no surviving rows.
        ("NULL pattern", "body LIKE NULL", &[]),
        // NOT LIKE inverts the match (and keeps NULL NULL): the two non-matching
        // rows survive.
        ("NOT LIKE", "body NOT LIKE '%timeout%'", &[1, 3]),
        // Case sensitivity is load-bearing (ClickBench Q23): lowercase pattern
        // over capitalized text matches nothing.
        ("case sensitive", "body LIKE '%TIMEOUT%'", &[]),
    ];

    for (label, predicate, expected) in cases {
        let sql = format!("SELECT ts FROM logs WHERE {predicate} ORDER BY ts");
        let df = ctx
            .sql(&sql)
            .await
            .unwrap_or_else(|e| panic!("[{label}] planning failed: {e}"));
        let batches = df
            .collect()
            .await
            .unwrap_or_else(|e| panic!("[{label}] execution failed: {e}"));
        let got = ts_sequence(&batches);
        assert_eq!(
            got, *expected,
            "[{label}] `{predicate}` returned unexpected rows"
        );
    }

    // The dictionary case must carry NO CAST in the optimized physical plan:
    // leaving `Dictionary(Int32, Utf8)` intact into the `like` UDF is the whole
    // point. A `CAST` here is the coercion DataFusion's built-in LIKE would have
    // inserted over the dictionary argument, defeating the per-distinct-value arm
    // and hydrating the column to one value per row.
    let dict_plan = ctx
        .sql("SELECT ts FROM logs WHERE name LIKE '%timeout%'")
        .await
        .expect("plan dict case")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let text = displayable(dict_plan.as_ref()).indent(true).to_string();
    assert!(
        !text.contains("CAST"),
        "LIKE over the declared dictionary column must carry no coercing CAST; plan was:\n{text}"
    );
    assert!(
        text.contains("like(name@"),
        "LIKE's matched argument must be the bare dictionary column, not a CAST; plan was:\n{text}"
    );
}

/// Reachability of the RLOG block-range fetcher (ADR-0107) from a real `logs`
/// SQL query: the same ts + `has_word` scan as
/// `scan_prunes_by_ts_and_word_returns_exact_rows`, but with a
/// `LogSegmentFetcher` whose block-range path is forced on (threshold 0) and
/// configured to genuinely range-fetch (a tiny probe suffix so the front
/// metadata and the candidate blocks are fetched by range, pruned blocks left as
/// zeroed gaps, coverage crossover disabled). The scan's output must still equal
/// the record-by-record oracle exactly, proving the block-range assembly decodes
/// correctly through the whole SQL scan pipeline, not only a unit test.
#[tokio::test]
async fn logs_scan_over_block_range_fetcher_returns_exact_rows() {
    let store = MemoryStore::new();

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
    let obj_b: Vec<LogRecord> = (1000..=1010)
        .map(|ts| {
            record(
                "worker",
                ts,
                if ts == 1005 { "request timeout" } else { "ok" },
            )
        })
        .collect();
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
        pending_erasure: Vec::new(),
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // Force the block-range path on for every object, with a tiny probe suffix
    // (genuine range fetches, zeroed gaps for pruned blocks) and the coverage
    // crossover disabled.
    let block_range = ravel_query::BlockRangeFetcher::new(Arc::clone(&store))
        .with_whole_object_threshold(0)
        .with_suffix_len(64)
        .with_coverage_threshold(2.0);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(block_range);

    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

    let (lo, hi) = (100i64, 250i64);
    let filters = vec![
        col("ts").gt_eq(ts_lit(lo)),
        col("ts").lt_eq(ts_lit(hi)),
        has_word_udf().call(vec![col("body"), lit("timeout")]),
    ];
    let plan = provider.plan_filters(4, &filters).expect("build plan");
    let batches = collect_plan(plan).await;
    let got = batches_to_rows(&batches);

    let mut want = BTreeSet::new();
    for records in [&obj_a, &obj_b, &obj_c] {
        for r in records {
            if r.ts_ns >= lo && r.ts_ns <= hi && body_has_word(&r.body, "timeout") {
                want.insert((r.ts_ns, r.body.clone()));
            }
        }
    }
    assert_eq!(
        want,
        BTreeSet::from([
            (105, "connection timeout".to_string()),
            (202, "gateway timeout".to_string()),
        ]),
        "oracle sanity"
    );
    assert_eq!(
        got, want,
        "block-range SQL scan output must equal the oracle exactly"
    );
}
