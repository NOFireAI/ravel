//! Traceability row `PartialReportingMatchesSignal`
//! (crates/ravel-ingest/src/log_router.rs, `await_strict_acks`).
//! docs/consistency-model.md "Partial multi-shard commits": a multi-shard
//! Strict write where at least one shard fails but one or more siblings
//! already committed durably reports a retryable error that also carries
//! the commit tokens of every shard that did commit, in shard order --
//! "this holds for all three signals: metrics, logs, and spans alike."
//! Three tests, one per signal, each over the same three-shard shape: shard
//! 1's data-object PUT is permanently failed, shards 0 and 2 commit.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::keys;
use ravel_ingest::{
    IngestConfig, IngestRouter, LogIngestRouter, LogWriteError, SpanIngestRouter, WriteError,
    WriteMode, shard_for_span,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantHash, shard_for, shard_for_log};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first point/record/span (`target_bytes: 1`) and never on
/// age, so a strict write drives one complete flush inline.
fn flush_on_first(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// A `FaultPlan` that fails every data-object PUT for one shard permanently,
/// so that shard's flush is abandoned on the first attempt, deterministically
/// and with no clock advance.
fn fail_shard_data_puts(shard: u32) -> FaultPlan {
    let key = format!("/l0/{shard:04}/");
    FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
        )
        .with_key_contains(key)
        .with_occurrence(Occurrence::Always),
    )
}

async fn assert_commit_record_present(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    token: &CommitToken,
) {
    let commit_key = keys::commit_key_for_token(tenant_hash, signal, token).expect("commit key");
    let outcome = store
        .get(&commit_key, ravel_object_store::GetRange::Full)
        .await;
    assert!(
        outcome.is_ok(),
        "durable token's commit record must exist at {commit_key}"
    );
}

fn point_on_shard(
    tenant: &ravel_types::TenantId,
    want_shard: u32,
    shard_count: u32,
    ts_ns: i64,
) -> ravel_otlp::NormalizedPoint {
    for i in 0..100_000u32 {
        let point = make_point(tenant, "cpu_usage", &[("host", &i.to_string())], ts_ns, 1.0);
        if shard_for(&point.series_id, shard_count) == want_shard {
            return point;
        }
    }
    panic!("no series found for shard {want_shard} of {shard_count}");
}

fn span_on_shard(want_shard: u32, shard_count: u32, start_ns: i64) -> NormalizedSpan {
    for i in 0..100_000u64 {
        let mut trace_id = [0u8; 16];
        trace_id[..8].copy_from_slice(&i.to_le_bytes());
        if shard_for_span(&trace_id, shard_count) == want_shard {
            return NormalizedSpan {
                trace_id,
                span_id: [1u8; 8],
                parent_span_id: None,
                name: "handle".to_string(),
                start_ts_ns: start_ns,
                end_ts_ns: start_ns + 100,
                status_code: StatusCode::Unset,
                status_message: None,
                attrs: vec![("service.name".to_string(), "checkout".to_string())],
            };
        }
    }
    panic!("no trace routes to shard {want_shard} of {shard_count}");
}

fn norm_record(resource: &[(&str, &str)], ts_ns: i64, body: &str) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = resource
        .iter()
        .map(|(k, v)| ((*k).to_string(), AttrValue::Str((*v).to_string())))
        .collect();
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&res, "scope", "", &scope_attrs);
    let stream_attrs = stream_attrs_bytes(&res, "scope", "", &scope_attrs);
    NormalizedLogRecord {
        stream_id,
        stream_attrs,
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn record_on_shard(
    want_shard: u32,
    shard_count: u32,
    ts_ns: i64,
    body: &str,
) -> NormalizedLogRecord {
    for i in 0..100_000u32 {
        let host = i.to_string();
        let rec = norm_record(&[("service.name", "api"), ("host", &host)], ts_ns, body);
        if shard_for_log(&rec.stream_id, shard_count) == want_shard {
            return rec;
        }
    }
    panic!("no stream found for shard {want_shard} of {shard_count}");
}

/// Metrics: shard 1's flush is abandoned while shards 0 and 2 commit. The
/// failure carries exactly the two surviving tokens, in shard order, each
/// naming a commit record that actually exists.
#[tokio::test]
async fn partial_multi_shard_commit_reports_durable_tokens_metrics() {
    let shard_count = 3;
    let store = Arc::new(FaultStore::new(MemoryStore::new(), fail_shard_data_puts(1)));
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first(shard_count),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let points = vec![
        point_on_shard(&tenant, 0, shard_count, 1_000),
        point_on_shard(&tenant, 1, shard_count, 2_000),
        point_on_shard(&tenant, 2, shard_count, 3_000),
    ];

    let err = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("shard 1's flush was abandoned, so the write is a failure");

    let durable = match &err {
        WriteError::PartialWrite { durable, .. } => durable,
        other => panic!("expected PartialWrite, got {other:?}"),
    };
    assert!(err.is_retryable());
    assert_eq!(
        durable.len(),
        2,
        "exactly the two surviving shards' tokens are recovered, got {durable:?}"
    );
    assert_eq!(durable[0].shard, 0, "tokens are in ascending shard order");
    assert_eq!(durable[1].shard, 2, "tokens are in ascending shard order");
    for token in durable {
        assert_commit_record_present(store.as_ref(), &tenant.hash(), Signal::Metrics, token).await;
    }
    router.shutdown().await;
}

/// Logs: same three-shard shape, over `LogIngestRouter`.
#[tokio::test]
async fn partial_multi_shard_commit_reports_durable_tokens_logs() {
    let shard_count = 3;
    let store = Arc::new(FaultStore::new(MemoryStore::new(), fail_shard_data_puts(1)));
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(
        flush_on_first(shard_count),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let records = vec![
        record_on_shard(0, shard_count, 1_000, "shard0"),
        record_on_shard(1, shard_count, 2_000, "shard1"),
        record_on_shard(2, shard_count, 3_000, "shard2"),
    ];

    let err = router
        .write(
            tenant.clone(),
            records,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("shard 1's flush was abandoned, so the write is a failure");

    let durable = match &err {
        LogWriteError::PartialWrite { durable, .. } => durable,
        other => panic!("expected PartialWrite, got {other:?}"),
    };
    assert!(err.is_retryable());
    assert_eq!(
        durable.len(),
        2,
        "exactly the two surviving shards' tokens are recovered, got {durable:?}"
    );
    assert_eq!(durable[0].shard, 0, "tokens are in ascending shard order");
    assert_eq!(durable[1].shard, 2, "tokens are in ascending shard order");
    for token in durable {
        assert_commit_record_present(store.as_ref(), &tenant.hash(), Signal::Logs, token).await;
    }
    router.shutdown().await;
}

/// Spans: same three-shard shape, over `SpanIngestRouter`.
#[tokio::test]
async fn partial_multi_shard_commit_reports_durable_tokens_spans() {
    let shard_count = 3;
    let store = Arc::new(FaultStore::new(MemoryStore::new(), fail_shard_data_puts(1)));
    let clock = TestClock::new(BASE_NS);
    let router = SpanIngestRouter::new(
        flush_on_first(shard_count),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let spans = vec![
        span_on_shard(0, shard_count, 1_000),
        span_on_shard(1, shard_count, 2_000),
        span_on_shard(2, shard_count, 3_000),
    ];

    let err = router
        .write(
            tenant.clone(),
            spans,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("shard 1's flush was abandoned, so the write is a failure");

    let durable = match &err {
        ravel_ingest::SpanWriteError::PartialWrite { durable, .. } => durable,
        other => panic!("expected PartialWrite, got {other:?}"),
    };
    assert!(err.is_retryable());
    assert_eq!(
        durable.len(),
        2,
        "exactly the two surviving shards' tokens are recovered, got {durable:?}"
    );
    assert_eq!(durable[0].shard, 0, "tokens are in ascending shard order");
    assert_eq!(durable[1].shard, 2, "tokens are in ascending shard order");
    for token in durable {
        assert_commit_record_present(store.as_ref(), &tenant.hash(), Signal::Spans, token).await;
    }
    router.shutdown().await;
}
