//! End-to-end memory-pool byte accounting, exercising the
//! `TenantDelegatingPool` through a real DataFusion query rather than a
//! hand-built `TaskContext`.
//!
//! The pool unit tests (tests/pushdown_memory.rs) prove the pool's own arithmetic and
//! that a dropped mid-scan stream returns the tenant's bytes. What they
//! cannot prove is the property that actually matters: that a query
//! which pushes **many** batches through the pool releases all of them.
//!
//! The distinction matters. `RsegScanExec` grows its reservation once per
//! fetched/decoded segment and again per emitted batch (see
//! crate::scan module doc), and releases the whole thing when the stream
//! drops, so a single-batch test passes for either a correct implementation
//! or one whose release path only ever covers the last batch. Every test
//! here therefore forces more than one batch (over `RsegScanExec`'s
//! 8192-row batch size), checks that the reserved figure actually grew
//! *across* batches, and only then asserts the return to zero. A per-batch
//! `shrink` that leaked everything but the final batch would show a
//! non-zero residual here and nowhere else.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    LogsTableProvider, RavelTableProvider, SessionTable, SqlConfig, TenantMemoryAccountant,
    build_session,
};
use ravel_types::TenantId;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};
use uuid::Uuid;

/// Comfortably more than `RsegScanExec`'s 8192-row batch size, so the scan
/// emits several batches from one partition.
const SAMPLES: i64 = 30_000;

fn big_segment() -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "m",
            (0..SAMPLES).map(|i| (i, i as f64)).collect(),
        )],
    )]
}

/// Drain one query through a pool the test owns, returning
/// (batch count, reserved after the first batch, peak reserved).
async fn drain(
    fixture: &Fixture,
    tenant: &TenantId,
    pool: Arc<dyn MemoryPool>,
    sql: &str,
) -> (usize, usize, usize) {
    let snapshot = fixture.snapshot(tenant).await;
    let provider = Arc::new(RavelTableProvider::new(
        snapshot,
        tenant.hash(),
        SegmentFetcher::new(Arc::clone(&fixture.store)),
        SqlConfig::default(),
        QueryAccounting::new(),
    ));
    let ctx = build_session(
        &SqlConfig::default(),
        Arc::clone(&pool),
        SessionTable::Metrics(provider),
        false,
    )
    .expect("session");

    let mut stream = ctx
        .sql(sql)
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("execute");

    let mut batches = 0usize;
    let mut after_first = 0usize;
    let mut peak = 0usize;
    while let Some(next) = stream.next().await {
        let _batch = next.expect("batch");
        batches += 1;
        let reserved = pool.reserved();
        if batches == 1 {
            after_first = reserved;
        }
        peak = peak.max(reserved);
    }
    // Dropping the stream is what releases every `MemoryReservation`; the
    // pool forwards each drop-time shrink to the tenant accountant (see the
    // pool module docs).
    drop(stream);
    (batches, after_first, peak)
}

#[tokio::test]
async fn a_multi_batch_query_returns_every_reserved_byte() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) =
        SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());
    assert_eq!(pool.reserved(), 0, "a fresh pool starts empty");

    let (batches, after_first, peak) = drain(
        &fixture,
        &tenant,
        Arc::clone(&pool),
        "SELECT ts, value FROM samples",
    )
    .await;

    assert!(
        batches > 1,
        "the test is meaningless with one batch; got {batches}"
    );
    assert!(
        after_first > 0,
        "the scan must reserve bytes for its first batch"
    );
    assert!(
        peak > after_first,
        "reserved bytes must accumulate across batches \
         (first={after_first}, peak={peak}); otherwise this test would pass \
         for an implementation that only ever tracks one batch"
    );

    assert_eq!(
        pool.reserved(),
        0,
        "the query pool must return to zero once the streams drop"
    );
    assert_eq!(
        accountant.reserved(),
        0,
        "the tenant accountant must return to zero once the streams drop"
    );
}

/// An aggregating query puts DataFusion's own accumulator state through the
/// same pool, on top of the scan's batches. It must also return to zero.
#[tokio::test]
async fn a_multi_batch_aggregate_query_returns_every_reserved_byte() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) =
        SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());

    let (_batches, _first, _peak) = drain(
        &fixture,
        &tenant,
        Arc::clone(&pool),
        "SELECT series_id, count(value), sum(value) FROM samples GROUP BY series_id",
    )
    .await;

    assert_eq!(pool.reserved(), 0);
    assert_eq!(accountant.reserved(), 0);
}

/// Three consecutive queries against one tenant accountant. A leak of even
/// one batch per query would accumulate and show here, which a single-query
/// test cannot see.
#[tokio::test]
async fn repeated_multi_batch_queries_do_not_accumulate_tenant_bytes() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    for round in 0..3 {
        let (pool, _breach) =
            SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());
        let (batches, _first, _peak) = drain(
            &fixture,
            &tenant,
            pool,
            "SELECT ts, value FROM samples ORDER BY ts",
        )
        .await;
        assert!(batches >= 1);
        assert_eq!(
            accountant.reserved(),
            0,
            "tenant bytes must be back to zero after round {round}"
        );
    }
}

/// The same property through the real endpoint driver, which owns its pool
/// internally: after `SqlExecutor::execute` returns, the tenant accountant
/// it used is back to zero. This is the assertion an operator cares about,
/// since nothing outside the executor can drop those streams by hand.
#[tokio::test]
async fn the_executor_returns_tenant_bytes_after_a_multi_batch_query() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let budget = fixture.executor.tenant_budget(tenant.hash());
    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("query");

    assert!(
        outcome.output.num_batches() > 1,
        "expected a multi-batch result, got {}",
        outcome.output.num_batches()
    );
    assert_eq!(outcome.output.num_rows(), SAMPLES as usize);
    assert_eq!(
        budget.reserved(),
        0,
        "the executor must leave the tenant accountant at zero"
    );
}

/// A per-query byte ceiling smaller than the working set trips with the
/// pool's typed error, and still leaves the tenant accountant at zero: a
/// failed query must not leak either budget.
///
/// The reservation's first growth moved from the batch phase into
/// the fetch/decode phase (`prepare_partition`): the single segment's decoded
/// SoA alone (30,000 samples, one i64 timestamp and one f64 value each =
/// ~469 KiB, plus the segment's fetched raw-f64 page bytes) already exceeds
/// this test's 400 KB ceiling, so the trip happens before the scan ever
/// builds its first `RecordBatch`. That is the intended, earlier rejection
/// the issue asked for, not a regression: zero batches is the correct
/// outcome for a byte ceiling this far below the decoded input size.
///
/// The ceiling moved with the unit. It was 1.4 MiB while the fetch/decode
/// charge was `rows * size_of::<ScanRow>()` (64 bytes per sample); ADR-0099
/// decision 6 deleted that row struct, and the same live bytes are now the
/// SoA buffers the merge holds, 16 bytes per sample.
#[tokio::test]
async fn a_query_that_outgrows_its_pool_still_releases_tenant_bytes() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 400_000,
        parallel_final_aggregation: false,
    };
    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(Arc::clone(&accountant), QueryAccounting::new());

    let snapshot = fixture.snapshot(&tenant).await;
    let provider = Arc::new(RavelTableProvider::new(
        snapshot,
        tenant.hash(),
        SegmentFetcher::new(Arc::clone(&fixture.store)),
        config,
        QueryAccounting::new(),
    ));
    let ctx = build_session(
        &config,
        Arc::clone(&pool),
        SessionTable::Metrics(provider),
        false,
    )
    .expect("session");
    let mut stream = ctx
        .sql("SELECT ts, value FROM samples")
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("execute");

    let mut failed = false;
    let mut batches = 0usize;
    while let Some(next) = stream.next().await {
        match next {
            Ok(_) => batches += 1,
            Err(e) => {
                assert!(
                    e.to_string().contains("memory pool exhausted"),
                    "expected the query pool's own error, got: {e}"
                );
                failed = true;
                break;
            }
        }
    }
    drop(stream);

    assert!(failed, "the query pool must trip");
    assert_eq!(
        batches, 0,
        "the fetch/decode-phase charge must reject this query \
         before its one segment's decoded run ever reaches the batch phase"
    );
    assert_eq!(
        accountant.reserved(),
        0,
        "a query that failed on its own budget must not leak tenant bytes"
    );
}

// ---------------------------------------------------------------------------
// The `logs` columnar fast path (ADR-0099 decision 2, issue #415)
// ---------------------------------------------------------------------------
//
// The tests above all drive the `samples` (metrics) scan, whose reservation
// holds decoded SoA plus one row-built batch. They never touch the `logs` scan,
// so before this section the return-to-zero property was unproven for what the
// columnar fast path holds instead: a block's *pre-built* `RecordBatch`es
// (`hold_batches`/`emit_next_columnar_batch`), charged once and relabelled from
// `held` to `emitted` rather than regrown. That release path is arithmetically
// different from the row path's, and a leak in it would have shown nowhere.
//
// This test drives it end to end: a fixed-column `logs` query is columnar-
// eligible, and asserting `columnar_batches > 0` proves the fast path actually
// ran (so the test is not silently reduced to the row path), while the peak /
// return-to-zero assertions prove the columnar release path balances.

/// Records for the logs fixture. Several full blocks at RLOG's default 8192-row
/// block target, so the columnar path holds and releases more than one block and
/// the reserved figure accumulates across them.
const LOG_RECORDS: usize = 40_000;

/// One log record with a resource attribute and a dynamic attribute. The
/// fixed-column fast path touches neither, but they are present so the corpus is
/// realistic and a regression that folded `attrs` into the projection would
/// change the reserved figures.
fn log_record(i: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc-{}", i % 8)),
    )];
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: i as i64,
        observed_ts_ns: i as i64,
        severity_num: (i % 24) as u8,
        severity_text: "INFO".to_string(),
        body: format!("request {} completed", i % 997),
        trace_id: None,
        span_id: None,
        flags: i as u32,
        attrs: vec![("user_id".to_string(), AttrValue::Str(format!("u{i}")))],
    }
}

/// Write one RLOG object of `LOG_RECORDS` records into `store` and return a
/// snapshot over it.
async fn write_logs(store: &Arc<dyn ObjectStoreBackend>) -> Snapshot {
    let identity = ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for i in 0..LOG_RECORDS {
        w.push(log_record(i)).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    let key = "logs/accounting.rlog";
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    Snapshot {
        segments: vec![SegmentRef {
            data_object_key: key.to_string(),
            object_size: size,
            min_event_ts_ns: 0,
            max_event_ts_ns: (LOG_RECORDS - 1) as i64,
            ingest_hour_bucket: 0,
            sample_count: LOG_RECORDS as u64,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        }],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The `LogsScanExec` leaf of a physical plan, found by walking rather than by
/// shape: the optimizer inserts operators above it.
fn find_logs_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == "LogsScanExec" {
        return Some(Arc::clone(plan));
    }
    plan.children().iter().find_map(|c| find_logs_scan(c))
}

/// The outcome of one columnar `logs` drain: how many batches it emitted, the
/// peak bytes the pool held mid-stream, and the two path metrics from the
/// `LogsScanExec` leaf that prove which batch-building path ran.
struct LogsDrain {
    batches: usize,
    peak: usize,
    columnar: usize,
    rowpath: usize,
}

/// Drain one `logs` query through a pool the test owns.
async fn drain_logs(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
    pool: Arc<dyn MemoryPool>,
    sql: &str,
) -> LogsDrain {
    let snapshot = write_logs(&store).await;
    let provider = Arc::new(LogsTableProvider::new(
        snapshot,
        tenant.hash(),
        LogSegmentFetcher::new(store),
        QueryAccounting::new(),
    ));
    let ctx = build_session(
        &SqlConfig::default(),
        Arc::clone(&pool),
        SessionTable::Logs(provider),
        false,
    )
    .expect("session");

    let plan = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let scan = find_logs_scan(&plan).expect("a LogsScanExec leaf");
    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx()).expect("execute");

    let mut batches = 0usize;
    let mut peak = 0usize;
    while let Some(next) = stream.next().await {
        let _batch = next.expect("batch");
        batches += 1;
        peak = peak.max(pool.reserved());
    }
    drop(stream);

    let metrics = scan.metrics().expect("scan metrics");
    let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    LogsDrain {
        batches,
        peak,
        columnar: count("columnar_batches"),
        rowpath: count("rowpath_batches"),
    }
}

/// A fixed-column `logs` query takes the columnar fast path, holds real bytes
/// while streaming, and returns every one of them once the stream drops -- three
/// times over against one tenant accountant, so a leak of even one block's
/// batches per query would accumulate and show (the pattern
/// `repeated_multi_batch_queries_do_not_accumulate_tenant_bytes` uses for the
/// metrics scan).
///
/// This is the return-to-zero property for what the fast path holds: a block's
/// pre-built `RecordBatch`es (`hold_batches`/`emit_next_columnar_batch`), charged
/// once and relabelled from `held` to `emitted`, not a `Vec<LogRecord>`. The
/// metrics-scan tests above never touch the logs scan, so before this case that
/// release path was unexercised here.
///
/// There is deliberately no "reserved bytes grew across batches" assertion like
/// the metrics scan's: the logs scan is block-at-a-time (ADR-0087) and releases
/// each block before decoding the next, so its reserved figure is bounded by one
/// block, not cumulative. Non-vacuity comes from `columnar > 0` (the fast path
/// actually ran), `peak > 0` (it reserved real bytes), and the cross-round
/// accountant check (a surviving leak accumulates), not from growth.
#[tokio::test]
async fn repeated_logs_columnar_queries_return_every_reserved_byte() {
    let tenant = tenant_id("acme");

    let accountant = TenantMemoryAccountant::new(1 << 30);
    for round in 0..3 {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let (pool, _breach) =
            SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());
        assert_eq!(
            pool.reserved(),
            0,
            "round {round}: a fresh pool starts empty"
        );

        let run = drain_logs(
            Arc::clone(&store),
            &tenant,
            Arc::clone(&pool),
            "SELECT ts, body FROM logs",
        )
        .await;

        assert!(
            run.batches > 1,
            "round {round}: the test is meaningless with one batch; got {}",
            run.batches
        );
        assert!(
            run.columnar > 0,
            "round {round}: the query must take the columnar fast path, else this \
             case asserts nothing about it; columnar={}, rowpath={}",
            run.columnar,
            run.rowpath
        );
        assert_eq!(
            run.rowpath, 0,
            "round {round}: a fixed-column, spill-free query must not fall back \
             to the row path"
        );
        assert!(
            run.peak > 0,
            "round {round}: the fast path must reserve real bytes for its held \
             block's batches"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "round {round}: the query pool must return to zero once the columnar \
             stream drops"
        );
        assert_eq!(
            accountant.reserved(),
            0,
            "round {round}: the tenant accountant must be back to zero; a leak of \
             one query's blocks would accumulate across rounds"
        );
    }
}
