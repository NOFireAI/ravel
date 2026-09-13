//! Semantic agreement between `attrs['k']` map access and a declared typed
//! column `"k"` over the same RLOG data, through a real [`SqlExecutor`] on a
//! `MemoryStore` (issue #913, read-ahead and attribute-extraction study).
//!
//! The two access forms are what the bench compares for cost, so this pins,
//! row by row, exactly where they agree and where they are defined to differ:
//!
//! - a `Str` record value reads identically through both forms;
//! - a resource-level key with no record value reads identically through both
//!   (declared columns fall back to the stream attributes);
//! - a record value overrides a resource value under both forms;
//! - an absent key is NULL under both forms;
//! - an `I64` record value compares equal once the map form is cast
//!   (`TRY_CAST(attrs['k'] AS BIGINT)`), and a `Str` value under an `i64`
//!   declaration is NULL under both (the cast fails to NULL, the declared
//!   column reads NULL on a variant mismatch);
//! - a wrong-variant value under a `str` declaration is the one defined
//!   divergence: the map form renders the value as text, the declared column
//!   reads NULL (ADR-0090 decision 6);
//! - a key that spilled into `attrs_raw` (the writer's dynamic-column budget
//!   exhausted by an unrelated key) reads identically through both forms, on
//!   the row path the overflow forces;
//! - a key containing a dot is a literal key under both forms, never a path.
//!
//! Every assertion compares result contents as ordered rows (the statements
//! carry `ORDER BY ts`), not row counts.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, DictionaryArray, Int64Array, MapArray, StringArray};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::record_batch::RecordBatch;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
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
    TenantId::new("attrs-map-vs-declared-913".to_string())
}

fn identity(tenant_hash: [u8; 16]) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: [3u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn s(v: &str) -> AttrValue {
    AttrValue::Str(v.to_string())
}

/// The stream's resource attributes: `k_res` exists only here, `k_str` also
/// exists here so a record value can be shown to win over it.
fn resource() -> Vec<(String, AttrValue)> {
    vec![
        ("service.name".to_string(), s("api")),
        ("k_res".to_string(), s("from-resource")),
        ("k_str".to_string(), s("resource-level")),
    ]
}

fn record(ts: i64, attrs: &[(&str, AttrValue)]) -> LogRecord {
    let resource = resource();
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("row {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    }
}

/// The fixture: one record per case listed in the module doc.
fn records() -> Vec<LogRecord> {
    vec![
        // 1: plain Str value, plus an I64 and a dotted literal key.
        record(
            1_000,
            &[
                ("k_str", s("alpha")),
                ("k_i64", AttrValue::I64(5)),
                ("a.b", s("dotted")),
            ],
        ),
        // 2: wrong variants: I64 under the str key, Str under the i64 key.
        record(
            2_000,
            &[("k_str", AttrValue::I64(7)), ("k_i64", s("not-a-number"))],
        ),
        // 3: no record attrs at all: k_res and k_str come from the resource.
        record(3_000, &[]),
        // 4: k_str present again with another Str, k_i64 negative.
        record(
            4_000,
            &[("k_str", s("beta")), ("k_i64", AttrValue::I64(-11))],
        ),
    ]
}

/// Write one RLOG object with `cfg` and publish its commit record.
async fn publish_logs(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    records: &[LogRecord],
    cfg: RlogConfig,
) {
    let writer_id = Uuid::from_u128(9_913);
    let mut w = RlogWriter::new(cfg, identity(tenant.hash().0));
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
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
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

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("k_str", DeclaredType::Str),
        DeclaredColumn::new("k_i64", DeclaredType::I64),
        DeclaredColumn::new("k_res", DeclaredType::Str),
        DeclaredColumn::new("a.b", DeclaredType::Str),
    ]
}

fn executor(store: Arc<dyn ObjectStoreBackend>) -> SqlExecutor {
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
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())))
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
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Column 1 of every batch as `Option<String>`, concatenated in batch order,
/// accepting both the map form's `Utf8` and the declared form's
/// `Dictionary(Int32, Utf8)`.
fn strings(batches: &[RecordBatch]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(1);
        if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i).to_string())));
        } else if let Some(d) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let values = d
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 dictionary values");
            out.extend((0..d.len()).map(|i| {
                d.is_valid(i)
                    .then(|| values.value(d.keys().value(i) as usize).to_string())
            }));
        } else {
            panic!("unexpected column type {:?}", col.data_type());
        }
    }
    out
}

/// The value of `key` in the `attrs` map column (column 1) per row, in batch
/// order, or `None` where the row's map has no such key. Used to read the whole
/// merged map back through the public surface, the path the per-key projection
/// rewrite must NOT change.
fn map_values(batches: &[RecordBatch], key: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let m = b
            .column(1)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("attrs map column");
        for i in 0..m.len() {
            let entries = m.value(i);
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("map keys are Utf8");
            let vals = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("map values are Utf8");
            let mut found = None;
            for j in 0..keys.len() {
                if keys.value(j) == key {
                    found = Some(vals.value(j).to_string());
                    break;
                }
            }
            out.push(found);
        }
    }
    out
}

fn ints(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let a = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i))));
    }
    out
}

async fn run(ex: &SqlExecutor, sql: &str) -> Vec<RecordBatch> {
    let t = tenant();
    ex.execute(t.hash(), &request(sql))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .output
        .batches()
        .to_vec()
}

/// Runs the whole comparison over a store written with `cfg`, so the same
/// expectations are checked once on the columnar layout and once with an
/// `attrs_raw` overflow forcing the row path.
async fn compare_all(cfg: RlogConfig) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs(store.as_ref(), &tenant(), &records(), cfg).await;
    let ex = executor(store);

    // k_str: Str values agree; the resource value shows where no record value
    // exists; the I64-under-str row is the defined divergence.
    let map = strings(&run(&ex, "SELECT ts, attrs['k_str'] AS v FROM logs ORDER BY ts").await);
    let dec = strings(&run(&ex, "SELECT ts, \"k_str\" AS v FROM logs ORDER BY ts").await);
    assert_eq!(
        map,
        vec![
            Some("alpha".into()),
            Some("7".into()),
            Some("resource-level".into()),
            Some("beta".into()),
        ]
    );
    assert_eq!(
        dec,
        vec![
            Some("alpha".into()),
            None,
            Some("resource-level".into()),
            Some("beta".into()),
        ]
    );
    // Exactly one row differs, and it is the wrong-variant row.
    let differing: Vec<usize> = (0..4).filter(|&i| map[i] != dec[i]).collect();
    assert_eq!(differing, vec![1]);

    // k_i64: with TRY_CAST the map form equals the declared column on every
    // row, including the Str-under-i64 row (NULL on both sides).
    let map = ints(
        &run(
            &ex,
            "SELECT ts, TRY_CAST(attrs['k_i64'] AS BIGINT) AS v FROM logs ORDER BY ts",
        )
        .await,
    );
    let dec = ints(&run(&ex, "SELECT ts, \"k_i64\" AS v FROM logs ORDER BY ts").await);
    assert_eq!(map, vec![Some(5), None, None, Some(-11)]);
    assert_eq!(map, dec);

    // k_res: only ever at resource level; both forms read it on every row.
    let map = strings(&run(&ex, "SELECT ts, attrs['k_res'] AS v FROM logs ORDER BY ts").await);
    let dec = strings(&run(&ex, "SELECT ts, \"k_res\" AS v FROM logs ORDER BY ts").await);
    assert_eq!(map, vec![Some("from-resource".into()); 4]);
    assert_eq!(map, dec);

    // a.b: a literal dotted key on row 1, absent (NULL) elsewhere, both forms.
    let map = strings(&run(&ex, "SELECT ts, attrs['a.b'] AS v FROM logs ORDER BY ts").await);
    let dec = strings(&run(&ex, "SELECT ts, \"a.b\" AS v FROM logs ORDER BY ts").await);
    assert_eq!(map, vec![Some("dotted".into()), None, None, None]);
    assert_eq!(map, dec);

    // Filtering agrees where the value is a Str: the equality predicate over
    // the map and over the declared column select the same rows.
    let map = ints(
        &run(
            &ex,
            "SELECT ts, CAST(ts AS BIGINT) AS v FROM logs WHERE attrs['k_str'] = 'beta' ORDER BY ts",
        )
        .await,
    );
    let dec = ints(
        &run(
            &ex,
            "SELECT ts, CAST(ts AS BIGINT) AS v FROM logs WHERE \"k_str\" = 'beta' ORDER BY ts",
        )
        .await,
    );
    assert_eq!(map, vec![Some(4_000)]);
    assert_eq!(map, dec);

    // The per-key projection `attrs['k_str']` (issue #1768) must render exactly
    // what reading the whole `attrs` map back does, key for key, INCLUDING the
    // divergence row where the record holds a non-Str value under `k_str`: both
    // render `7`, unlike the declared column which reads NULL. `SELECT attrs`
    // takes the untouched whole-map path; `attrs['k_str']` takes the rewritten
    // per-key path. They must agree.
    let per_key = strings(&run(&ex, "SELECT ts, attrs['k_str'] AS v FROM logs ORDER BY ts").await);
    let whole = map_values(
        &run(&ex, "SELECT ts, attrs AS a FROM logs ORDER BY ts").await,
        "k_str",
    );
    assert_eq!(
        whole,
        vec![
            Some("alpha".into()),
            Some("7".into()),
            Some("resource-level".into()),
            Some("beta".into()),
        ],
        "the whole attrs map renders the non-Str divergence value as text"
    );
    assert_eq!(
        per_key, whole,
        "the per-key projection reproduces the whole map's rendering exactly"
    );
}

/// The default writer layout: every key gets its own dynamic column, so the
/// declared form takes the columnar path and the map form the row path.
#[tokio::test]
async fn map_and_declared_forms_agree_on_the_columnar_layout() {
    compare_all(RlogConfig::default()).await;
}

/// `max_dynamic_columns = 1`: the writer assigns its single dynamic column to
/// the first `(name, type)` pair in byte order (`a.b`), and every other key
/// (`k_i64`, `k_str`) spills into `attrs_raw`, forcing the row path for both
/// forms. The same expectations must hold, including the divergence row.
///
/// To watch this fail against a reader that ignored `attrs_raw` for declared
/// columns: the `k_i64` comparison would read `[None, None, None, None]` for
/// the declared side and the `assert_eq!(map, dec)` after it would trip.
#[tokio::test]
async fn map_and_declared_forms_agree_under_attrs_raw_overflow() {
    let cfg = RlogConfig {
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };
    compare_all(cfg).await;
}
