//! SQL/PromQL admission parity (ADR-0073 decision 4):
//! the SQL executor's whole-snapshot check in
//! `crates/ravel-sql/src/executor.rs`'s `resolve` now calls the same
//! `ravel_query::admit` seam PromQL's `QueryEngine::resolve_bounded` already
//! used. Every test here runs the identical published
//! segments and `EngineConfig` through both engines and asserts they reach
//! the same admission verdict.
//!
//! The line whose reversion breaks this: `crates/ravel-sql/src/executor.rs`'s
//! `resolve`, `admit(&snapshot, &origins, &self.config.engine)
//! .map_err(admission_error_to_sql)?`. Revert that to a bare
//! `snapshot.segments.len() > self.config.engine.max_segments` check (the
//! shape) and `sql_and_promql_agree_recent_hours_are_exempt` below
//! starts failing: SQL would count all 6 segments (1 sealed + 5 recent)
//! against `max_segments: 1` and refuse a query PromQL still admits.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_promql::LabelMatcher;
use ravel_query::{EngineConfig, LogSegmentFetcher, QueryEngine, QueryError, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_sql::{SqlConfig, SqlError, SqlExecutor, SqlRequest};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash,
    TenantId, TimeRange,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// First instant at which `hour` is sealed, mirroring
/// `ravel-query/tests/recent_hours_admission.rs`.
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn tenant(id: &str) -> TenantId {
    TenantId::new(id.to_string())
}

/// Writes one real RSEG v6 segment carrying a single sample and publishes
/// its commit record, mirroring
/// `ravel-query/tests/recent_hours_admission.rs`'s helper of the same name so
/// both sides of the parity check build identical bytes.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    metric: &str,
    ts_ns: i64,
    value: f64,
) -> (CommitToken, String) {
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value }],
    };
    let written = SegmentWriter::write(vec![input], identity, bounds).expect("write v6 segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 0,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

fn sql_request(sql: &str, window: TimeRange, now_ns: i64) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window,
        min_tokens: Vec::new(),
        now_ns,
        deadline: Duration::from_secs(30),
    }
}

/// One sealed segment plus five recent segments under `max_segments: 1`:
/// PromQL already proves this admits
/// (`recent_hours_exempt_from_segment_cap`). SQL must reach the identical
/// verdict now that `resolve` shares the same seam.
#[tokio::test]
async fn sql_and_promql_agree_recent_hours_are_exempt() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let sealed_hour = 9_001u32;
    let recent_hour = sealed_hour + 1;
    let now = now_at_seal(sealed_hour);

    publish_segment(
        store.as_ref(),
        &tid,
        th,
        1,
        sealed_hour,
        "m",
        i64::from(sealed_hour) * NS_PER_HOUR + 10 * 60 * NS_PER_SEC,
        1.0,
    )
    .await;

    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    cat.fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold seals sealed_hour");

    for seq in 2..7u64 {
        publish_segment(
            store.as_ref(),
            &tid,
            th,
            seq,
            recent_hour,
            "m",
            i64::from(recent_hour) * NS_PER_HOUR + (seq as i64) * 60 * NS_PER_SEC,
            seq as f64,
        )
        .await;
    }

    let engine_config = EngineConfig {
        max_segments: 1,
        ..EngineConfig::default()
    };
    let window = TimeRange {
        start_ns: i64::from(sealed_hour) * NS_PER_HOUR,
        end_ns: i64::from(recent_hour + 1) * NS_PER_HOUR,
    };

    let engine = QueryEngine::new(Arc::clone(&cat), store.clone(), engine_config);
    let (series, stats) = engine
        .resolve_series_with_stats(
            th,
            &[LabelMatcher::equal(METRIC_NAME_LABEL, "m")],
            window,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect("PromQL: 1 sealed + 5 recent segments admitted under max_segments=1");
    assert_eq!(series.len(), 1);
    assert_eq!(stats.segments_fetched, 6);

    let sql_config = SqlConfig {
        engine: engine_config,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        Arc::clone(&cat),
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        sql_config,
        1 << 30,
    );
    let outcome = executor
        .execute(
            th,
            &sql_request(
                "SELECT ts, value FROM samples WHERE label(labels, '__name__') = 'm'",
                window,
                now,
            ),
        )
        .await
        .expect("SQL: 1 sealed + 5 recent segments admitted under max_segments=1");
    assert_eq!(
        outcome.stats.segments, 6,
        "SQL must see the same 6-segment snapshot PromQL did"
    );
}

/// Four sealed segments under `max_segments: 2` must still be refused by
/// both engines, with matching `count`/`max` (PromQL:
/// `sealed_set_still_capped_when_oversized`). The recent-hours exemption
/// must not widen the sealed-set cap itself, on either surface.
#[tokio::test]
async fn sql_and_promql_agree_sealed_set_still_capped() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 9_101u32;
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;

    for seq in 1..5u64 {
        publish_segment(store.as_ref(), &tid, th, seq, hour, "m", ts, seq as f64).await;
    }
    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    cat.fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");

    let engine_config = EngineConfig {
        max_segments: 2,
        ..EngineConfig::default()
    };
    let window = TimeRange {
        start_ns: i64::from(hour) * NS_PER_HOUR,
        end_ns: i64::from(hour + 1) * NS_PER_HOUR,
    };

    let engine = QueryEngine::new(Arc::clone(&cat), store.clone(), engine_config);
    let promql_err = engine
        .resolve_series(
            th,
            &[LabelMatcher::equal(METRIC_NAME_LABEL, "m")],
            window,
            &[],
            now,
            Duration::from_secs(5),
        )
        .await
        .expect_err("PromQL: 4 sealed segments over max_segments=2 must be refused");
    let (promql_count, promql_max) = match promql_err {
        QueryError::TooManySegments { count, max } => (count, max),
        other => panic!("expected TooManySegments, got {other:?}"),
    };
    assert_eq!((promql_count, promql_max), (4, 2));

    let sql_config = SqlConfig {
        engine: engine_config,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        Arc::clone(&cat),
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        sql_config,
        1 << 30,
    );
    let sql_err = executor
        .execute(
            th,
            &sql_request(
                "SELECT ts, value FROM samples WHERE label(labels, '__name__') = 'm'",
                window,
                now,
            ),
        )
        .await
        .expect_err("SQL: 4 sealed segments over max_segments=2 must be refused");
    match sql_err {
        SqlError::TooManySegments { count, max } => {
            assert_eq!(
                (count, max),
                (promql_count, promql_max),
                "SQL and PromQL must refuse with the identical sealed count and cap"
            );
        }
        other => panic!("expected TooManySegments, got {other:?}"),
    }
}

/// SQL-only recent-hour exemption: five never-folded (thus
/// `SegmentOrigin::Recent`) segments must be admitted through
/// `SqlExecutor::execute` even under `max_segments: 0`, mirroring PromQL's
/// `token_segments_always_admitted`/`recent_hours_exempt_from_segment_cap`
/// exemption (ADR-0073 decision 2). Previously, the SQL executor's own
/// whole-snapshot count would have refused this with `TooManySegments { count:
/// 5, max: 0 }`.
#[tokio::test]
async fn sql_recent_hour_segments_are_exempt_from_max_segments() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let recent_hour = 9_301u32;
    let now = now_at_seal(recent_hour);

    for seq in 1..6u64 {
        publish_segment(
            store.as_ref(),
            &tid,
            th,
            seq,
            recent_hour,
            "m",
            i64::from(recent_hour) * NS_PER_HOUR + (seq as i64) * 60 * NS_PER_SEC,
            seq as f64,
        )
        .await;
    }
    // No fold: every segment above stays classified `Recent`.
    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));

    let engine_config = EngineConfig {
        max_segments: 0,
        ..EngineConfig::default()
    };
    let sql_config = SqlConfig {
        engine: engine_config,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        cat,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        sql_config,
        1 << 30,
    );
    let window = TimeRange {
        start_ns: i64::from(recent_hour) * NS_PER_HOUR,
        end_ns: i64::from(recent_hour + 1) * NS_PER_HOUR,
    };
    let outcome = executor
        .execute(
            th,
            &sql_request(
                "SELECT ts, value FROM samples WHERE label(labels, '__name__') = 'm'",
                window,
                now,
            ),
        )
        .await
        .expect("5 never-folded recent segments must be admitted despite max_segments=0");
    assert_eq!(outcome.stats.segments, 5);
    assert_eq!(outcome.output.num_rows(), 5);
}

/// No-op regression: with no recent-hour data (every
/// segment folded/sealed) and the default, effectively-unbounded budgets
/// (`EngineConfig::default()`: `max_segments` 1024, `max_s3_requests` bounded
/// at 25,000), admission must behave exactly as it did before this
/// migration -- every sealed segment fetched, nothing refused.
#[tokio::test]
async fn no_recent_hour_data_default_config_is_unaffected() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 9_401u32;
    let now = now_at_seal(hour);

    for seq in 1..4u64 {
        publish_segment(
            store.as_ref(),
            &tid,
            th,
            seq,
            hour,
            "m",
            i64::from(hour) * NS_PER_HOUR + (seq as i64) * 60 * NS_PER_SEC,
            seq as f64,
        )
        .await;
    }
    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    cat.fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold seals hour, leaving no recent-hour data");

    let sql_config = SqlConfig {
        engine: EngineConfig::default(),
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        cat,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        sql_config,
        1 << 30,
    );
    let window = TimeRange {
        start_ns: i64::from(hour) * NS_PER_HOUR,
        end_ns: i64::from(hour + 1) * NS_PER_HOUR,
    };
    let outcome = executor
        .execute(
            th,
            &sql_request(
                "SELECT ts, value FROM samples WHERE label(labels, '__name__') = 'm'",
                window,
                now,
            ),
        )
        .await
        .expect("default budgets over 3 sealed segments must admit, unchanged from before RH-T2");
    assert_eq!(outcome.stats.segments, 3);
    assert_eq!(outcome.output.num_rows(), 3);
}
