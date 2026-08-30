//! Bounded ephemeral spill for eligible SQL operators (ADR-0954, issue #954).
//!
//! Every test drives the real `SqlExecutor::execute` path over a real
//! `MemoryStore`-backed catalog, so the disk-manager wiring, the eligibility
//! predicate, the typed spill errors, and cleanup are all exercised end to end
//! rather than through a hand-built session.
//!
//! The spilling tests use the `logs` table on purpose: `LogsScanExec` streams
//! block by block and releases each block's reservation before the next (its
//! reservation "never covers two blocks at once"), so the scan's resident
//! footprint stays small and the high-cardinality aggregate is the consumer
//! that reaches the memory ceiling and spills. The `samples` scan instead
//! reserves a whole segment up front and holds it, so on that path the
//! non-spillable scan, not the aggregate, is what a small budget refuses; the
//! `samples` table is therefore used only for the refusal tests, where that is
//! exactly the point.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Int64Array, TimestampNanosecondArray};
use futures::StreamExt;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    QueryOutput, SpanSegmentFetcher, SpillConfig, SqlConfig, SqlError, SqlExecutor, SqlRequest,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use util::{Fixture, SegSpec, SeriesSpec, request as samples_request, tenant_id};
use uuid::Uuid;

/// Distinct-group count for the spilling logs fixture. Every record carries a
/// distinct ts, so the final aggregate holds `GROUPS` entries; at roughly 64
/// bytes per Int64 group the settled state is well above [`SPILL_MEMORY_BUDGET`],
/// so the final aggregate spills. The logs scan streams block by block, so its
/// own footprint stays a small fraction of the budget.
const GROUPS: i64 = 200_000;

/// The per-query memory budget for the spilling tests. Small enough that the
/// 200k-group final aggregate overruns it, large enough that the streaming logs
/// scan and the EmitTo::All spill path's transient sort reservation both fit.
const SPILL_MEMORY_BUDGET: usize = 4 * 1024 * 1024;

/// A generous scratch quota for the success case.
const AMPLE_SPILL_QUOTA: usize = 256 * 1024 * 1024;

/// The transient-headroom allowance the spilling aggregate's observed peak
/// memory is permitted over the configured cap (ADR-0954). The pool's
/// `try_grow` bounds reserved bytes at the cap, and `peak_intermediate_bytes`
/// records only granted reservations, so the observed peak is expected at or
/// below the cap; this allowance is slack for a single in-flight grow, not a
/// license to hide an overrun. The test prints the real peak and ratio so a
/// true overrun is reported, not masked. DataFusion's grouped-hash spill path
/// materializes all current groups with `EmitTo::All` before sorting
/// (DataFusion #24072), so the process-resident peak can exceed the cap even
/// when the pool-reserved figure `peak_intermediate_bytes` measures does not.
const SPILL_PEAK_MAX_OVERHEAD_NUMERATOR: u64 = 2;
const SPILL_PEAK_MAX_OVERHEAD_DENOMINATOR: u64 = 1;

const COUNT_SQL: &str = "SELECT ts, count(*) AS n FROM logs GROUP BY ts";

// ---------------------------------------------------------------------------
// Spill directory helpers
// ---------------------------------------------------------------------------

/// A unique, fresh spill directory under the OS temp dir.
fn fresh_spill_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ravel-sql-spill-{}", Uuid::new_v4()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create spill dir");
    dir
}

/// The entries in `dir` (its immediate children). DataFusion creates a
/// `datafusion-XXXX` subdirectory when the disk manager is enabled and removes
/// it when the query's `RuntimeEnv` drops, so a clean run leaves the root empty.
fn dir_entries(dir: &std::path::Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .expect("read spill dir")
        .map(|e| e.expect("dir entry").path())
        .collect()
}

/// Assert `dir` becomes empty within a short window. Cleanup runs when the
/// query's session and its DataFusion background tasks drop, which for the
/// error and cancellation paths can lag the return of `execute`/`drop` by a
/// poll, so this polls rather than checking once.
async fn assert_dir_eventually_empty(dir: &std::path::Path, context: &str) {
    for _ in 0..50 {
        if dir_entries(dir).is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "{context}: spill directory still not empty: {:?}",
        dir_entries(dir)
    );
}

// ---------------------------------------------------------------------------
// Logs fixture (streaming scan; the aggregate is the spilling consumer)
// ---------------------------------------------------------------------------

fn logs_tenant() -> TenantId {
    TenantId::new("spill-954".to_string())
}

/// The request window for the logs fixture: records carry ts in `0..GROUPS`, so
/// a `[0, GROUPS]` window at `now_ns = GROUPS` covers them with a single
/// ingest-hour LIST.
fn logs_request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: GROUPS,
        },
        min_tokens: Vec::new(),
        now_ns: GROUPS,
        deadline: Duration::from_secs(120),
    }
}

/// Publish one RLOG object holding `GROUPS` records with distinct ts (a minimal
/// body and no attrs, to keep each decoded block small), so a real
/// `Catalog::resolve` finds it and `GROUP BY ts` yields `GROUPS` groups.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let resource = vec![(
        "service.name".to_string(),
        ravel_logseg::AttrValue::Str("api".to_string()),
    )];
    let stream_id = ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    // Small blocks keep the streaming logs scan's per-block reservation
    // negligible against the memory budget, so the high-cardinality aggregate
    // -- not the scan -- is the consumer that reaches the ceiling and spills.
    // With default 8192-record blocks the scan's single-block reservation is a
    // large fraction of a small budget and competes with the aggregate for the
    // pool (see the module doc).
    let rlog_config = RlogConfig {
        block_target_records: 512,
        ..RlogConfig::default()
    };
    let mut writer = RlogWriter::new(rlog_config, identity);
    for ts in 0..GROUPS {
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "r".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(954),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: GROUPS as u64,
        series_count: 1,
        min_event_ts_ns: 0,
        max_event_ts_ns: GROUPS - 1,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: GROUPS - 1,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// A `SqlConfig` with a single-partition, simple aggregate plan shape, the given
/// memory budget, and optional spill configuration.
fn config(max_query_bytes: usize, spill: Option<SpillConfig>) -> SqlConfig {
    SqlConfig {
        engine: util::engine_config(),
        max_query_bytes,
        parallel_final_aggregation: false,
        // ON (the shipped default): the tightened skip-partial probe caps the
        // PARTIAL stage (it forwards raw rows after 8192 instead of holding a
        // full group table), so the partial does not compete for the pool and
        // the FINAL aggregate is the one consumer that accumulates every group
        // and reaches the ceiling. With it OFF the partial holds a full group
        // table too and starves the final's EmitTo::All spill-sort reservation.
        // The streaming logs scan's per-block reservation is small (see
        // publish_logs), so the final has the whole budget to spill within.
        skip_partial_aggregation: true,
        late_materialization_extra_columns: None,
        spill,
    }
}

async fn logs_executor(cfg: SqlConfig) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs(store.as_ref(), &logs_tenant()).await;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        cfg,
        1 << 40,
    )
}

/// Extract `(ts, count)` rows from a `SELECT ts, count(*) ... GROUP BY ts`
/// result, sorted by ts, so two runs compare byte for byte regardless of the
/// order the aggregate emitted groups.
fn ts_count_rows(output: &QueryOutput) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in output.batches() {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts is Timestamp(Nanosecond)");
        let count = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) is Int64");
        for i in 0..batch.num_rows() {
            rows.push((ts.value(i), count.value(i)));
        }
    }
    rows.sort_unstable();
    rows
}

// ---------------------------------------------------------------------------
// Samples fixture (whole-segment scan; used only for the refusal tests)
// ---------------------------------------------------------------------------

/// One segment carrying `rows` samples in a single series (ts == value == i).
fn samples_specs(rows: i64) -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        0,
        vec![SeriesSpec::new(
            "m",
            (0..rows).map(|i| (i, i as f64)).collect(),
        )],
    )]
}

/// A samples fixture over a small budget, for the refusal tests. 300k rows over
/// a 16 MiB budget is the existing over-budget-aggregation fixture's shape.
async fn samples_fixture(spill: Option<SpillConfig>) -> Fixture {
    let tenant = tenant_id("acme");
    Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &samples_specs(300_000))],
        config(16 * 1024 * 1024, spill),
        1 << 40,
    )
    .await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// An eligible high-cardinality aggregation that exceeds the memory budget
/// completes via spill and returns results byte-identical to the same query run
/// with a budget large enough to avoid spilling. Identical VALUES is the
/// assertion that matters, not merely the row count.
#[tokio::test]
async fn eligible_aggregation_over_budget_completes_via_spill_with_identical_results() {
    let tenant = logs_tenant();
    let spill_dir = fresh_spill_dir();

    let spilling = logs_executor(config(
        SPILL_MEMORY_BUDGET,
        Some(SpillConfig {
            dir: spill_dir.clone(),
            max_bytes: AMPLE_SPILL_QUOTA,
        }),
    ))
    .await;
    let spilled = spilling
        .execute(tenant.hash(), &logs_request(COUNT_SQL))
        .await
        .expect("an eligible aggregation must complete via spill, not fail");

    assert!(
        spilled.stats.spill_files > 0,
        "the aggregation must have spilled (files={}, bytes_written={}, rows={}); \
         if zero, the budget is too large to force a spill",
        spilled.stats.spill_files,
        spilled.stats.spill_bytes_written,
        spilled.stats.spill_rows,
    );
    assert!(
        spilled.stats.spill_bytes_written > 0 && spilled.stats.spill_rows > 0,
        "spill accounting must report the bytes and rows written: {:?}",
        (spilled.stats.spill_bytes_written, spilled.stats.spill_rows),
    );

    let ample = logs_executor(config(1 << 34, None)).await;
    let reference = ample
        .execute(tenant.hash(), &logs_request(COUNT_SQL))
        .await
        .expect("the large-budget reference run must complete");
    assert_eq!(
        reference.stats.spill_files, 0,
        "the reference run must not spill"
    );

    let spilled_rows = ts_count_rows(&spilled.output);
    let reference_rows = ts_count_rows(&reference.output);
    assert_eq!(
        spilled_rows.len(),
        GROUPS as usize,
        "every distinct ts must be a group"
    );
    assert_eq!(
        spilled_rows, reference_rows,
        "the spilled result must be byte-identical to the non-spilled result"
    );

    let peak = spilled.accounting.peak_intermediate_bytes;
    let cap = SPILL_MEMORY_BUDGET as u64;
    println!(
        "spill peak_intermediate_bytes = {peak} against cap {cap} (ratio {:.3}); \
         spill files={} bytes_written={} rows={}",
        peak as f64 / cap as f64,
        spilled.stats.spill_files,
        spilled.stats.spill_bytes_written,
        spilled.stats.spill_rows,
    );
    let allowed = cap * SPILL_PEAK_MAX_OVERHEAD_NUMERATOR / SPILL_PEAK_MAX_OVERHEAD_DENOMINATOR;
    assert!(
        peak <= allowed,
        "observed peak {peak} exceeds the declared allowance {allowed} \
         ({SPILL_PEAK_MAX_OVERHEAD_NUMERATOR}/{SPILL_PEAK_MAX_OVERHEAD_DENOMINATOR} of cap {cap}); \
         report the real number rather than raising the cap"
    );

    assert_dir_eventually_empty(&spill_dir, "after a completed query").await;
    let _ = std::fs::remove_dir_all(&spill_dir);
}

/// The same aggregation with a deliberately insufficient spill quota returns
/// the typed `SpillBudgetExhausted` error, not a partial result and not a panic.
#[tokio::test]
async fn insufficient_spill_quota_returns_spill_budget_exhausted() {
    let tenant = logs_tenant();
    let spill_dir = fresh_spill_dir();

    let executor = logs_executor(config(
        SPILL_MEMORY_BUDGET,
        Some(SpillConfig {
            dir: spill_dir.clone(),
            max_bytes: 64 * 1024,
        }),
    ))
    .await;

    let err = executor
        .execute(tenant.hash(), &logs_request(COUNT_SQL))
        .await
        .expect_err("an insufficient spill quota must fail typed");

    assert!(
        matches!(err, SqlError::SpillBudgetExhausted(_)),
        "an exceeded spill quota must be a typed SpillBudgetExhausted, \
         not a partial result or a generic error; got {err:?}"
    );

    assert_dir_eventually_empty(&spill_dir, "after a budget-exhausted query").await;
    let _ = std::fs::remove_dir_all(&spill_dir);
}

/// A missing/unwritable spill directory returns `SpillUnavailable`. The path
/// names a nonexistent parent, so the disk manager's eager directory creation
/// fails at session build regardless of the running user (a non-recursive
/// `create_dir` cannot create a directory whose parent is absent).
#[tokio::test]
async fn unwritable_spill_directory_returns_spill_unavailable() {
    let tenant = logs_tenant();
    let bad_dir = PathBuf::from(format!(
        "/nonexistent-ravel-spill-{}/scratch",
        Uuid::new_v4()
    ));

    let executor = logs_executor(config(
        SPILL_MEMORY_BUDGET,
        Some(SpillConfig {
            dir: bad_dir,
            max_bytes: AMPLE_SPILL_QUOTA,
        }),
    ))
    .await;

    let err = executor
        .execute(tenant.hash(), &logs_request(COUNT_SQL))
        .await
        .expect_err("an unusable spill directory must fail typed");

    assert!(
        matches!(err, SqlError::SpillUnavailable(_)),
        "a missing/unwritable spill directory must be a typed SpillUnavailable; got {err:?}"
    );
    assert!(
        !err.client_message().contains("/nonexistent-ravel-spill-"),
        "the client message must not leak the configured spill path: {}",
        err.client_message()
    );
}

/// An INELIGIBLE plan (order-dependent float aggregation) over budget is still
/// refused with today's typed `ResourcesExhausted` error, and does NOT spill:
/// no scratch file is created, because the disk manager stays disabled for an
/// ineligible query even when spill is configured. Uses the `samples` table for
/// its float `value` column.
#[tokio::test]
async fn ineligible_float_aggregation_over_budget_is_refused_and_does_not_spill() {
    let tenant = tenant_id("acme");
    let spill_dir = fresh_spill_dir();

    let fixture = samples_fixture(Some(SpillConfig {
        dir: spill_dir.clone(),
        max_bytes: AMPLE_SPILL_QUOTA,
    }))
    .await;

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &samples_request("SELECT ts, sum(value) AS s FROM samples GROUP BY ts"),
        )
        .await
        .expect_err("a float sum over budget must be refused, not spilled");

    assert!(
        matches!(err, SqlError::ResourcesExhausted(_)),
        "an ineligible float aggregation must fail with today's ResourcesExhausted, \
         not a spill error; got {err:?}"
    );
    assert!(
        !matches!(
            err,
            SqlError::SpillBudgetExhausted(_) | SqlError::SpillUnavailable(_)
        ),
        "an ineligible query must never surface a spill-family error; got {err:?}"
    );

    // No spill file was ever created: the disk manager stayed disabled, so it
    // never touched the configured directory.
    assert!(
        dir_entries(&spill_dir).is_empty(),
        "an ineligible query must not create any spill file: {:?}",
        dir_entries(&spill_dir)
    );
    let _ = std::fs::remove_dir_all(&spill_dir);
}

/// Spill off (unset config) reproduces today's behaviour exactly: a
/// high-cardinality aggregation over budget is the typed `ResourcesExhausted`,
/// never a spill, and never DataFusion's raw disabled-disk wording.
#[tokio::test]
async fn spill_off_reproduces_todays_disabled_behaviour() {
    let tenant = tenant_id("acme");
    let fixture = samples_fixture(None).await;

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &samples_request("SELECT ts, count(*) AS n FROM samples GROUP BY ts"),
        )
        .await
        .expect_err("with spill off, an over-budget aggregation must fail typed");

    assert!(
        matches!(err, SqlError::ResourcesExhausted(_)),
        "spill off must reproduce today's ResourcesExhausted; got {err:?}"
    );
    assert!(
        !matches!(
            err,
            SqlError::SpillBudgetExhausted(_) | SqlError::SpillUnavailable(_)
        ),
        "spill off must never surface a spill-family error; got {err:?}"
    );
    assert!(
        err.client_message().contains("budget"),
        "the client message must name the budget; got {:?}",
        err.client_message()
    );
    let SqlError::ResourcesExhausted(msg) = &err else {
        unreachable!("asserted above");
    };
    assert!(
        !msg.contains("DiskManager is disabled"),
        "DataFusion's own disabled-disk wording must not leak through: {msg:?}"
    );
}

/// Cleanup after a cancelled stream: a query that starts spilling and is then
/// dropped mid-flight leaves no scratch behind, because dropping the stream
/// drops the session, its `RuntimeEnv`, and the disk manager's temp directory.
#[tokio::test]
async fn cancelled_spilling_stream_leaves_no_files() {
    let tenant = logs_tenant();
    let spill_dir = fresh_spill_dir();

    let executor = logs_executor(config(
        SPILL_MEMORY_BUDGET,
        Some(SpillConfig {
            dir: spill_dir.clone(),
            max_bytes: AMPLE_SPILL_QUOTA,
        }),
    ))
    .await;

    let accounting = QueryAccounting::new();
    let (snapshot, _estimate) = executor
        .resolve_snapshot(tenant.hash(), &logs_request(COUNT_SQL), &accounting)
        .await
        .expect("resolve the logs snapshot");
    {
        let planned = executor
            .plan_pinned(tenant.hash(), snapshot, COUNT_SQL, &accounting, &[])
            .await
            .expect("plan the spilling query");
        let mut stream = planned.execute().await.expect("start the stream");
        // Drive one poll: the aggregate is blocking, so this consumes its input
        // and creates spill files, then drop the stream without draining it.
        let _ = stream.next().await;
        drop(stream);
    }

    assert_dir_eventually_empty(&spill_dir, "after a cancelled query").await;
    let _ = std::fs::remove_dir_all(&spill_dir);
}
