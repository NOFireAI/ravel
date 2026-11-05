//! Issue #3838: an integer `sum` whose total leaves its result type fails the
//! query with `SqlError::IntegerSumOverflow` instead of returning a wrapped
//! value.
//!
//! These tests drive the real `SqlExecutor::execute` path over a `logs` table
//! with declared `I64` columns, spread over several objects so the scan has
//! several partitions and the aggregate merges partial states, with
//! `parallel_final_aggregation` on and off. The typed error must reach the
//! caller with its own class and client message, not as a value and not
//! redacted to the generic execution failure.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::physical_plan::displayable;
use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, ErrorClass, SpanSegmentFetcher, SqlConfig, SqlError,
    SqlExecutor, SqlRequest, StaticDeclaredColumns,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// RLOG objects the fixture publishes; also the scan's fan-out at
/// `fetch_concurrency == OBJECTS`, so the plan has this many partitions.
const OBJECTS: usize = 4;

/// Log records per object.
const RECORDS_PER_OBJECT: usize = 4;

fn tenant() -> TenantId {
    TenantId::new("issue-3838-integer-sum-overflow".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("a", DeclaredType::I64),
        DeclaredColumn::new("b", DeclaredType::I64),
        DeclaredColumn::new("c", DeclaredType::I64),
        DeclaredColumn::new("g", DeclaredType::I64),
    ]
}

/// One fixture row.
#[derive(Debug, Clone, Copy)]
struct Row {
    a: i64,
    b: i64,
    c: i64,
    g: i64,
}

/// Record 0 of objects 0, 1 and 2 carries the limit values; every other
/// record carries zeros. `a` totals `i64::MAX + 1` (objects 0 and 1), `c`
/// totals `i64::MIN - 1`, and `b` totals `i64::MAX` through `i64::MAX + 1`
/// in objects 0 and 1 and `-1` in object 2. Record 0 of every object is in
/// group `g = 0`; group 1 holds only zeros.
fn fixture_row(object: usize, index: usize) -> Row {
    let limit = |values: [i64; 3]| {
        if index == 0 && object < 3 {
            values[object]
        } else {
            0
        }
    };
    Row {
        a: limit([i64::MAX, 1, 0]),
        b: limit([i64::MAX, 1, -1]),
        c: limit([i64::MIN, -1, 0]),
        g: (index % 2) as i64,
    }
}

fn record(ts: i64, row: Row) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("web".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: String::new(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("a".to_string(), AttrValue::I64(row.a)),
            ("b".to_string(), AttrValue::I64(row.b)),
            ("c".to_string(), AttrValue::I64(row.c)),
            ("g".to_string(), AttrValue::I64(row.g)),
        ],
    }
}

async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, object: usize) {
    let base_ts = object as i64 * 100_000;
    let records: Vec<LogRecord> = (0..RECORDS_PER_OBJECT)
        .map(|i| record(base_ts + i as i64, fixture_row(object, i)))
        .collect();

    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: *Uuid::from_u128(0x3838_0000 + object as u128).as_bytes(),
        writer_epoch: 1,
        writer_seq: object as u64 + 1,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(0x3838_0000 + object as u128),
        writer_epoch: 1,
        writer_seq: object as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: [object as u8; 32],
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

struct Fixture {
    executor: SqlExecutor,
    catalog: Arc<Catalog>,
}

impl Fixture {
    async fn build(parallel_final_aggregation: bool) -> Self {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let t = tenant();
        for object in 0..OBJECTS {
            publish_object(store.as_ref(), &t, object).await;
        }
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let config = SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: OBJECTS,
                ..EngineConfig::default()
            },
            parallel_final_aggregation,
            ..SqlConfig::default()
        };
        let executor = SqlExecutor::new(
            Arc::clone(&catalog),
            SegmentFetcher::new(Arc::clone(&store)),
            LogSegmentFetcher::new(Arc::clone(&store)),
            SpanSegmentFetcher::new(Arc::clone(&store)),
            config,
            1 << 30,
        )
        .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())));
        Fixture { executor, catalog }
    }

    async fn snapshot(&self) -> Snapshot {
        self.catalog
            .resolve(
                &tenant().hash(),
                Signal::Logs,
                TimeRange {
                    start_ns: 0,
                    end_ns: 1_000_000,
                },
                &[],
                1_000_000,
            )
            .await
            .expect("resolve logs snapshot")
    }

    /// The indented physical plan for `sql` through `plan_pinned`.
    async fn plan(&self, sql: &str) -> String {
        let accounting = QueryAccounting::new();
        let planned = self
            .executor
            .plan_pinned(
                tenant().hash(),
                self.snapshot().await,
                sql,
                &accounting,
                &declared(),
            )
            .await
            .expect("plan_pinned");
        let plan = planned
            .create_physical_plan()
            .await
            .expect("physical plan builds");
        format!("{}", displayable(plan.as_ref()).indent(true))
    }

    /// The last column of every row `sql` returns, in result order.
    async fn sums(&self, sql: &str) -> Result<Vec<Option<i64>>, SqlError> {
        let outcome = self.executor.execute(tenant().hash(), &request(sql)).await?;
        let mut out = Vec::new();
        for batch in outcome.output.batches() {
            let column = batch
                .column(batch.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an Int64 sum column");
            out.extend((0..column.len()).map(|i| column.is_valid(i).then(|| column.value(i))));
        }
        Ok(out)
    }
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

/// `result` is the typed overflow for `aggregate`, classed `Unsupported` (the
/// HTTP surface's 422), with a client message that names the aggregate and the
/// result type.
fn assert_overflow(sql: &str, result: Result<Vec<Option<i64>>, SqlError>, aggregate: &str) {
    let err = match result {
        Ok(values) => panic!("{sql}: expected an integer sum overflow, got {values:?}"),
        Err(err) => err,
    };
    assert_eq!(err.class(), ErrorClass::Unsupported, "{sql}: {err:?}");
    assert_eq!(
        err.client_message(),
        format!("integer sum overflow in {aggregate}: the sum overflowed its result type Int64"),
        "{sql}: {err:?}"
    );
}

#[tokio::test]
async fn an_overflowing_integer_sum_fails_on_the_executor() {
    for parallel in [false, true] {
        let fixture = Fixture::build(parallel).await;
        for (sql, aggregate) in [
            ("SELECT sum(a) FROM logs", "sum(logs.a)"),
            ("SELECT sum(a) FROM logs WHERE g >= 0", "sum(logs.a)"),
            ("SELECT g, sum(a) FROM logs GROUP BY g ORDER BY g", "sum(logs.a)"),
            ("SELECT sum(c) FROM logs", "sum(logs.c)"),
            ("SELECT g, sum(c) FROM logs GROUP BY g ORDER BY g", "sum(logs.c)"),
        ] {
            let result = fixture.sums(sql).await;
            assert_overflow(sql, result, aggregate);
        }
    }
}

/// `b`'s partial for objects 0 and 1 is `i64::MAX + 1`, outside `Int64`, and
/// its total is `i64::MAX`: the exact total comes back.
#[tokio::test]
async fn a_sum_whose_total_fits_returns_it_on_the_executor() {
    for parallel in [false, true] {
        let fixture = Fixture::build(parallel).await;
        assert_eq!(
            fixture.sums("SELECT sum(b) FROM logs").await.expect("sum"),
            vec![Some(i64::MAX)]
        );
        assert_eq!(
            fixture
                .sums("SELECT sum(b) FROM logs WHERE g >= 0")
                .await
                .expect("sum"),
            vec![Some(i64::MAX)]
        );
        assert_eq!(
            fixture
                .sums("SELECT g, sum(b) FROM logs GROUP BY g ORDER BY g")
                .await
                .expect("grouped sum"),
            vec![Some(i64::MAX), Some(0)]
        );
    }
}

/// The fixture's scan has several partitions, so the grouped and filtered
/// statements merge partial states: the overflow tests above cover the merge.
#[tokio::test]
async fn the_fixture_aggregates_in_partial_and_final_phases() {
    let fixture = Fixture::build(false).await;
    for sql in [
        "SELECT sum(a) FROM logs WHERE g >= 0",
        "SELECT g, sum(a) FROM logs GROUP BY g ORDER BY g",
    ] {
        let plan = fixture.plan(sql).await;
        assert!(plan.contains("mode=Partial"), "{sql}:\n{plan}");
    }
}
