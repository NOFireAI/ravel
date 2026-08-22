//! Integration tests for typed attribute columns on the `logs` SQL table
//! (ADR-0090), driven through a real [`ravel_sql::SqlExecutor`] built with a
//! [`ravel_sql::StaticDeclaredColumns`] source against a `MemoryStore`.
//!
//! Unlike a test that calls `build_batch`/`resolve_columns` directly, these go
//! through the executor's real validate -> resolve -> plan -> execute path (the
//! same one `/api/v1/sql` uses), so they exercise the whole wiring: the
//! declared columns resolved once per plan, the widened schema, the projection
//! reaching the scan, and the native typed arrays `build_batch` produces.
//!
//! Two properties are pinned:
//!
//! - `declared_columns_project_as_native_typed_arrays_null_on_mismatch`: a
//!   `StaticDeclaredColumns` set with one column of each of the four declared
//!   types, over a dataset with matching and mismatching attribute values,
//!   yields native typed results (not strings), NULL on every mismatch and
//!   absent case, and `attrs` still carries the same keys (ADR-0090 decisions
//!   5-7).
//! - `declared_bytes_column_normalizes_list_map_across_storage_locations`: the
//!   identical logical `List` value stored once as a dynamic `Bytes` column and
//!   once overflowed into `attrs_raw` reads the same canonical bytes through a
//!   declared `bytes` column (ADR-0090 decision 7's one normalization rule).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, DictionaryArray, Int64Array, MapArray, StringArray,
    StructArray,
};
use datafusion::arrow::datatypes::Int32Type;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::record::canonical_value_bytes;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

fn tenant() -> TenantId {
    // A real TenantId: the RLOG object is written under its hash and the
    // executor queries the same hash, so the footer tenant check passes.
    TenantId::new("declared-columns-301".to_string())
}

fn identity(tenant_hash: [u8; 16]) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn s(v: &str) -> AttrValue {
    AttrValue::Str(v.to_string())
}

/// A record on a fixed single-`service.name` stream carrying per-record `attrs`.
fn record(ts: i64, body: &str, attrs: &[(String, AttrValue)]) -> LogRecord {
    let resource = vec![("service.name".to_string(), s("api"))];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
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

/// Write one RLOG object from `records` and publish its `Signal::Logs` commit
/// record, so a real `Catalog::resolve` finds the segment. Returns nothing; the
/// executor resolves the snapshot itself.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let writer_id = Uuid::from_u128(9_301);
    let mut w = RlogWriter::new(RlogConfig::default(), identity(tenant.hash().0));
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// Build an executor over `store` with `declared` typed attribute columns
/// installed via a `StaticDeclaredColumns` source.
fn executor_with(store: Arc<dyn ObjectStoreBackend>, declared: Vec<DeclaredColumn>) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("cat"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared)))
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: 1_000_000,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
    }
}

/// ADR-0090 decisions 5-7 end to end: declared columns of each of the four
/// types project as native typed Arrow arrays through a real `SqlExecutor`,
/// read NULL on a type mismatch or an absent key, and leave `attrs` carrying
/// the same keys.
#[tokio::test]
async fn declared_columns_project_as_native_typed_arrays_null_on_mismatch() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let t = tenant();

    // Three records:
    // ts=1: every declared key carries a value of the matching type.
    // ts=2: every declared key carries a value of the WRONG type (a `Str` where
    //       i64/bool/bytes are declared, and an i64 where str is declared).
    // ts=3: none of the declared keys are present at all.
    let records = vec![
        record(
            1,
            "match",
            &[
                ("dur".to_string(), AttrValue::I64(42)),
                ("name".to_string(), s("checkout")),
                ("ok".to_string(), AttrValue::Bool(true)),
                ("blob".to_string(), AttrValue::Bytes(vec![1, 2, 3])),
            ],
        ),
        record(
            2,
            "mismatch",
            &[
                ("dur".to_string(), s("not-an-int")),
                ("name".to_string(), AttrValue::I64(7)),
                ("ok".to_string(), s("not-a-bool")),
                ("blob".to_string(), AttrValue::I64(9)),
            ],
        ),
        record(3, "absent", &[("unrelated".to_string(), s("x"))]),
    ];
    publish_logs(store.as_ref(), &t, &records).await;

    let declared = vec![
        DeclaredColumn::new("dur", DeclaredType::I64),
        DeclaredColumn::new("name", DeclaredType::Str),
        DeclaredColumn::new("ok", DeclaredType::Bool),
        DeclaredColumn::new("blob", DeclaredType::Bytes),
    ];
    let executor = executor_with(Arc::clone(&store), declared);

    // Typed columns project as their native Arrow types. `attrs` is selected
    // too, so the "declared keys stay in the map" property is checked in the
    // same query. Order by ts so rows are deterministic.
    let outcome = executor
        .execute(
            t.hash(),
            &request("SELECT ts, dur, name, ok, blob, attrs FROM logs ORDER BY ts"),
        )
        .await
        .expect("query executes");
    let batches = outcome.output.batches();
    assert_eq!(outcome.output.num_rows(), 3);

    // Collect the single batch (3 rows fit in one) for column-typed asserts.
    let batch = batches.iter().find(|b| b.num_rows() == 3).expect("batch");

    let dur = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("dur is a native Int64 column, not a string");
    // A declared Str column is dictionary-encoded (ADR-0099 decision 5), so it
    // downcasts to `Dictionary(Int32, Utf8)`, not a plain `StringArray`.
    let name_dict = batch
        .column(2)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("name is a native Dictionary(Int32, Utf8) column");
    let name_values = name_dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name dictionary values are Utf8");
    let name: Vec<Option<String>> = (0..name_dict.len())
        .map(|i| {
            if name_dict.is_null(i) {
                None
            } else {
                let k = name_dict.keys().value(i) as usize;
                (!name_values.is_null(k)).then(|| name_values.value(k).to_string())
            }
        })
        .collect();
    let ok = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("ok is a native Boolean column");
    let blob = batch
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("blob is a native Binary column");

    // Row 0 (ts=1): matching values, native and non-null.
    assert!(!dur.is_null(0) && dur.value(0) == 42);
    assert!(name[0].as_deref() == Some("checkout"));
    assert!(!ok.is_null(0) && ok.value(0));
    assert!(!blob.is_null(0) && blob.value(0) == [1u8, 2, 3]);

    // Row 1 (ts=2): every value is the wrong variant -> NULL, never a cast.
    assert!(dur.is_null(1), "a Str under an i64 column must read NULL");
    assert!(name[1].is_none(), "an I64 under a str column must read NULL");
    assert!(ok.is_null(1), "a Str under a bool column must read NULL");
    assert!(
        blob.is_null(1),
        "an I64 under a bytes column must read NULL"
    );

    // Row 2 (ts=3): keys absent -> NULL.
    assert!(dur.is_null(2) && name[2].is_none() && ok.is_null(2) && blob.is_null(2));

    // Decision 6: declared keys still appear in the `attrs` map exactly as they
    // would undeclared. Row 0 carries dur/name/ok/blob plus the resource key.
    let attrs = batch
        .column(5)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("attrs is still a Map column");
    let entries = attrs.value(0);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map entries");
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map keys");
    let present: std::collections::BTreeSet<&str> =
        (0..keys.len()).map(|i| keys.value(i)).collect();
    for k in ["dur", "name", "ok", "blob", "service.name"] {
        assert!(
            present.contains(k),
            "declared key {k} must remain in the attrs map (row 0 keys: {present:?})"
        );
    }

    // Typed comparison over a declared i64 column, impossible over a stringified
    // map: only ts=1's dur=42 is > 40.
    let ranged = executor
        .execute(t.hash(), &request("SELECT ts FROM logs WHERE dur > 40"))
        .await
        .expect("typed range executes");
    assert_eq!(ranged.output.num_rows(), 1);

    // Typed aggregate over the declared i64 column: only ts=1 contributes 42.
    let agg = executor
        .execute(t.hash(), &request("SELECT sum(dur) FROM logs"))
        .await
        .expect("typed aggregate executes");
    let agg_batch = agg.output.batches().first().expect("agg batch");
    let sum = agg_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum(i64) is Int64");
    assert_eq!(sum.value(0), 42, "sum ignores the mismatch and absent rows");
}

/// ADR-0090 decision 7's one normalization rule: the identical logical `List`
/// value reads the same canonical bytes through a declared `bytes` column
/// whether it was stored as a dynamic `Bytes` column (fit the budget) or
/// overflowed into `attrs_raw` (decoded back as `List`).
///
/// Both storage locations are produced from the SAME writer by controlling the
/// per-object dynamic-column budget: a tight budget forces the `List` value to
/// overflow into `attrs_raw`; a roomy budget stores it as a `Bytes` column. The
/// declared `bytes` column must read the identical canonical bytes in both.
#[tokio::test]
async fn declared_bytes_column_normalizes_list_map_across_storage_locations() {
    // The one logical value under test: a nested List.
    let list_value = AttrValue::List(vec![s("a"), s("b"), AttrValue::I64(7)]);
    let expected = canonical_value_bytes(&list_value);

    // Case A: a roomy budget, so `tags` is stored as a dynamic Bytes column.
    let stored = read_declared_bytes(&list_value, RlogConfig::default(), 0).await;
    // Case B: a budget of 1 dynamic column, consumed by an alphabetically
    // earlier `filler` key, so `tags` overflows into `attrs_raw` and decodes
    // back as a `List`.
    let cfg_tight = RlogConfig {
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };
    let overflowed = read_declared_bytes(&list_value, cfg_tight, 20).await;

    assert_eq!(
        stored.as_deref(),
        Some(expected.as_slice()),
        "a value stored as a dynamic Bytes column reads its canonical bytes"
    );
    assert_eq!(
        overflowed.as_deref(),
        Some(expected.as_slice()),
        "the same value overflowed into attrs_raw reads the identical canonical bytes"
    );
    assert_eq!(
        stored, overflowed,
        "both storage locations must produce the identical declared bytes value"
    );
}

/// Write one record whose `tags` attribute is `value`, plus `filler` unrelated
/// dynamic keys to control the dynamic-column budget, then query the declared
/// `bytes` column `tags` through a real executor and return row 0's bytes.
async fn read_declared_bytes(value: &AttrValue, cfg: RlogConfig, filler: usize) -> Option<Vec<u8>> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let t = tenant();

    let mut attrs = vec![("tags".to_string(), value.clone())];
    for i in 0..filler {
        attrs.push((format!("filler{i}"), AttrValue::Str(format!("v{i}"))));
    }
    let records = vec![record(1, "row", &attrs)];

    // Write with the explicit config controlling the dynamic-column budget.
    let writer_id = Uuid::from_u128(9_302);
    let mut w = RlogWriter::new(cfg, identity(t.hash().0));
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let new_record = NewCommitRecord {
        tenant_hash: t.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [8u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: 1,
        max_event_ts_ns: 1,
        min_ingest_ts_ns: 1,
        max_ingest_ts_ns: 1,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");
    publish::publish(store.as_ref(), &rec, &RetryPolicy::default())
        .await
        .expect("publish");

    let executor = executor_with(
        Arc::clone(&store),
        vec![DeclaredColumn::new("tags", DeclaredType::Bytes)],
    );
    let outcome = executor
        .execute(t.hash(), &request("SELECT tags FROM logs"))
        .await
        .expect("query executes");
    let batch = outcome
        .output
        .batches()
        .iter()
        .find(|b| b.num_rows() == 1)
        .cloned()
        .expect("one row");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("tags is a native Binary column")
        .clone();
    if col.is_null(0) {
        None
    } else {
        Some(col.value(0).to_vec())
    }
}
