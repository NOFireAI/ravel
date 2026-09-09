//! Issue #1130: a multi-shard Strict write where some shards commit durably
//! and at least one shard fails in the same `write()` call must report a
//! partial commit that carries every durable sibling token, on the metrics
//! and span routers, exactly as the log router already does. All-shards-fail
//! stays the plain (non-partial) error, and an ack round that times out
//! carries no tokens at all.
//!
//! docs/consistency-model.md "Partial multi-shard commits" is normative for
//! all three signals.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::keys;
use ravel_ingest::{
    IngestConfig, IngestRouter, SpanIngestRouter, WriteError, WriteMode, shard_for_span,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::NormalizedPoint;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::{Signal, TenantHash, shard_for};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first point/span (`target_bytes: 1`) and never on age, so a
/// strict write drives one complete flush inline and returns its outcome.
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

/// First `host` label value whose point routes to `want_shard`.
fn point_on_shard(
    tenant: &ravel_types::TenantId,
    want_shard: u32,
    shard_count: u32,
    ts_ns: i64,
) -> NormalizedPoint {
    for i in 0..100_000u32 {
        let point = make_point(tenant, "cpu_usage", &[("host", &i.to_string())], ts_ns, 1.0);
        if shard_for(&point.series_id, shard_count) == want_shard {
            return point;
        }
    }
    panic!("no series found for shard {want_shard} of {shard_count}");
}

/// A span whose `trace_id` (an incrementing little-endian counter) routes to
/// `want_shard`. `shard_for_span` hashes the whole id, so scanning counter
/// values finds a representative for every shard.
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

/// Asserts the store holds exactly one commit record for `token`, proving the
/// recovered token names a shard that actually committed.
async fn assert_commit_record_present(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    token: &ravel_types::CommitToken,
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

/// A `FaultPlan` that fails every data-object PUT for one shard permanently.
/// A non-retryable store error abandons that flush on the first attempt with
/// no backoff, so the outcome is deterministic and needs no clock advance.
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

/// 5a: a metrics Strict write over three shards where shard 1's flush is
/// abandoned while shards 0 and 2 commit durably. The failure surfaces as
/// `PartialWrite` carrying exactly the two surviving tokens in shard order.
///
/// Non-vacuity: the failing shard (shard 1) sorts before shard 2 in the
/// router's ascending-shard ack loop, so the pre-fix `tokens.push(inner?)`
/// returned at shard 1 and dropped both siblings' tokens. Against that unfixed
/// loop this test fails at the `PartialWrite` match below, because the error
/// is the bare abandonment and carries no recovered tokens at all. The
/// injected fault is asserted via the `FaultStore` counter so the abandonment
/// is proven to have fired.
#[tokio::test]
async fn metrics_partial_commit_reports_surviving_tokens_in_shard_order() {
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

    let (inner, durable) = match &err {
        WriteError::PartialWrite { inner, durable } => (inner, durable),
        other => panic!("expected PartialWrite, got {other:?}"),
    };
    assert!(
        err.is_retryable(),
        "an abandoned-flush partial write delegates retryability to its (retryable) inner, got {inner}"
    );
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

    assert_eq!(
        router.metrics().snapshot().partial_writes,
        1,
        "the partial write is counted exactly once"
    );
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the permanent data-object PUT fault fired exactly once (shard 1, no retry)"
    );

    router.shutdown().await;
}

/// 5b: the span-router counterpart of 5a. Same three-shard shape, shard 1
/// abandoned, shards 0 and 2 durable, `SpanWriteError::PartialWrite` carrying
/// the two tokens in shard order. Non-vacuity is identical to 5a.
#[tokio::test]
async fn spans_partial_commit_reports_surviving_tokens_in_shard_order() {
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
    assert!(
        err.is_retryable(),
        "an abandoned-flush partial write delegates retryability to its (retryable) inner, got {err}"
    );
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

    assert_eq!(
        router.metrics().snapshot().partial_writes,
        1,
        "the partial write is counted exactly once"
    );
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the permanent data-object PUT fault fired exactly once (shard 1, no retry)"
    );

    router.shutdown().await;
}

/// 5c (metrics): when every involved shard fails, no shard committed durably,
/// so the error is the plain abandonment, never `PartialWrite`. Two shards,
/// both abandoned via a fault on every data-object PUT.
///
/// Non-vacuity: the fix only wraps a failure in `PartialWrite` when the durable
/// set is non-empty; a regression that wrapped unconditionally would produce
/// `PartialWrite` here and fail the `matches!` assertion.
#[tokio::test]
async fn metrics_all_shards_fail_is_plain_error_not_partial() {
    let shard_count = 2;
    // Fail every data-object PUT (`/l0/`) for every shard.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
        )
        .with_key_contains("/l0/")
        .with_occurrence(Occurrence::Always),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
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
    ];

    let err = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("every shard's flush was abandoned");
    assert!(
        !matches!(err, WriteError::PartialWrite { .. }),
        "no shard committed, so the error is the plain abandonment, got {err:?}"
    );
    assert_eq!(
        err.durable_tokens().len(),
        0,
        "an all-failed write recovers no tokens"
    );
    assert_eq!(
        router.metrics().snapshot().partial_writes,
        0,
        "an all-failed write is not counted as a partial commit"
    );
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Permanent),
        2,
        "both shards' data PUTs were rejected once each"
    );

    router.shutdown().await;
}

/// 5c (spans): the span-router counterpart of the all-shards-fail case.
#[tokio::test]
async fn spans_all_shards_fail_is_plain_error_not_partial() {
    let shard_count = 2;
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
        )
        .with_key_contains("/l0/")
        .with_occurrence(Occurrence::Always),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
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
    ];

    let err = router
        .write(
            tenant.clone(),
            spans,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("every shard's flush was abandoned");
    assert!(
        !matches!(err, ravel_ingest::SpanWriteError::PartialWrite { .. }),
        "no shard committed, so the error is the plain abandonment, got {err:?}"
    );
    assert_eq!(
        err.durable_tokens().len(),
        0,
        "an all-failed write recovers no tokens"
    );
    assert_eq!(
        router.metrics().snapshot().partial_writes,
        0,
        "an all-failed write is not counted as a partial commit"
    );
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Permanent),
        2,
        "both shards' data PUTs were rejected once each"
    );

    router.shutdown().await;
}

/// 5e: an ack round whose only shard's flush is held open past the ack
/// deadline returns `AckTimeout` with no recovered tokens (a sibling that
/// commits inside the elapsed window is unknowable to the caller). After the
/// hold is released the flush completes: the shard's commit record is present
/// in the store at its exact key, exactly the crash-matrix row that says a
/// commit can land after the client already received the retryable timeout.
///
/// Non-vacuity: before the fix, `AckTimeout` was returned identically, so this
/// pins the invariant the fix must not regress (no tokens on timeout) and the
/// documented after-timeout durability, rather than the token-recovery change.
#[tokio::test]
async fn ack_timeout_recovers_no_tokens_but_commit_lands_after_release() {
    let shard_count = 1;
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    // Hold shard 0's data-object PUT so its flush blocks before the commit
    // record is written; the ack never resolves within the deadline.
    let gate = store.hold(Op::Put, Some("/l0/0000/".to_string()), Occurrence::Always);
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_first(shard_count),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let point = point_on_shard(&tenant, 0, shard_count, 1_000);

    // A short ack deadline: the held flush cannot resolve, so the whole ack
    // round times out.
    let err = router
        .write(
            tenant.clone(),
            vec![point],
            WriteMode::Strict,
            Duration::from_millis(200),
        )
        .await
        .expect_err("the held flush cannot ack before the deadline");
    assert!(
        matches!(err, WriteError::AckTimeout),
        "a whole-round timeout is AckTimeout, got {err:?}"
    );
    assert_eq!(
        err.durable_tokens().len(),
        0,
        "a timed-out ack round recovers no tokens"
    );

    // Release the held PUT; the shard actor completes the abandoned-caller
    // flush in the background and its commit record lands.
    gate.wait_until_held(1).await;
    for id in gate.held() {
        assert!(gate.release(id), "held call {id} is released");
    }

    // Shutdown joins every in-flight flush, so once it returns the released
    // shard's commit is durable or was abandoned: one LIST decides, with no
    // wall-clock band.
    router.shutdown().await;
    let commit_prefix =
        keys::commit_shard_prefix(&tenant.hash(), Signal::Metrics, 0).expect("commit shard prefix");
    let commit_keys: Vec<String> = list_all(store.as_ref(), &commit_prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|o| o.key)
        .filter(|k| k.ends_with(".cmt"))
        .collect();
    assert_eq!(
        commit_keys.len(),
        1,
        "the held shard's commit lands after release, at exactly one key: {commit_keys:?}"
    );
}
