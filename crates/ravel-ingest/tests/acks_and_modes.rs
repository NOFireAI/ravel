//! Strict vs buffered ack semantics, and multi-shard receipts with
//! read-your-write via `Catalog::resolve` (docs/consistency-model.md).
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter,
    LogWriteError, PendingAckBudget, WriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TimeRange};

fn norm_log_record(ts_ns: i64, body: &str) -> NormalizedLogRecord {
    let resource: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&resource, "scope", "", &scope_attrs);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "", &scope_attrs);
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

fn config(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(50),
        ..IngestConfig::default()
    }
}

#[tokio::test]
async fn strict_ack_only_after_data_and_commit_exist() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    // `flush_all` only flushes tenants already buffered, so it must run
    // concurrently with (not after) the write, and after (not before) the
    // write's own enqueue; joining the two futures interleaves them
    // correctly without a timing-dependent sleep.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commit_objects = objects.iter().filter(|o| o.key.contains("/c/")).count();
    assert_eq!(data_objects, 1, "data object must exist once ack fires");
    assert_eq!(commit_objects, 1, "commit record must exist once ack fires");

    router.shutdown().await;
}

#[tokio::test]
async fn buffered_mode_acks_immediately_then_flush_lands_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let receipt = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write succeeds");
    assert!(receipt.tokens.is_empty());

    let before = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        before.is_empty(),
        "buffered ack must not wait on durability"
    );

    router.flush_all().await;
    let after = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(after.iter().any(|o| o.key.contains("/l0/")));
    assert!(after.iter().any(|o| o.key.contains("/c/")));

    router.shutdown().await;
}

/// `submit` must return as soon as the point is enqueued, without waiting on
/// any shard ack (ADR-2793 decision 4): nothing triggers a flush yet
/// (`max_flush_delay` is an hour and `flush_all` has not run), so a `submit`
/// that folds the ack wait back in -- the old `write` body left inline,
/// awaiting `ack_rxs` before returning `Submitted` instead of after -- would
/// hang past the timeout below instead of returning. `acks()`, joined with
/// `flush_all`, must then produce the same receipt and durability `write`
/// does.
#[tokio::test]
async fn submit_then_acks_matches_write() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let submitted = tokio::time::timeout(
        Duration::from_millis(200),
        router.submit(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        ),
    )
    .await
    .expect("submit must return without waiting on any shard ack")
    .expect("submit enqueues");

    let (acks_result, ()) = tokio::join!(submitted.acks(), router.flush_all());
    let receipt = acks_result.expect("acks resolves once the flush commits");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commit_objects = objects.iter().filter(|o| o.key.contains("/c/")).count();
    assert_eq!(
        data_objects, 1,
        "data object must exist once acks() resolves"
    );
    assert_eq!(
        commit_objects, 1,
        "commit record must exist once acks() resolves"
    );

    router.shutdown().await;
}

/// The bound-plus-one strict `submit` must refuse once the budget's limit is
/// already held (ADR-2793 decision 4), and the permit must come from before
/// any shard is touched: taking it after the byte charge or after the shard
/// send instead (flip the order in `submit_points`) does not change this
/// test's outcome by itself, since nothing else contends for the budget here
/// -- the ordering itself is what `BufferBudgetExceeded`'s existing
/// before-routing placement in the same function already pins; this test
/// pins the bound.
#[tokio::test]
async fn pending_ack_budget_refuses_the_bound_plus_one() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let budget = Arc::new(PendingAckBudget::new(2));
    let byte_budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::clone(&budget),
    )
    .with_budget(Arc::clone(&byte_budget));
    let tenant = tenant("acme");
    let point = |host: &str| {
        vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", host)],
            1_000,
            1.0,
        )]
    };

    let first = router
        .submit(
            tenant.clone(),
            point("a"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first submit within budget");
    let second = router
        .submit(
            tenant.clone(),
            point("b"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("second submit within budget");
    let charged_after_second = byte_budget.in_flight_bytes();

    let third = router
        .submit(
            tenant.clone(),
            point("c"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    assert!(
        matches!(&third, Err(WriteError::PendingAckLimitReached)),
        "the bound-plus-one submit must refuse, got {:?}",
        third.err()
    );
    assert_eq!(budget.shed_total(), 1);
    assert_eq!(budget.held(), 2);
    assert_eq!(
        byte_budget.in_flight_bytes(),
        charged_after_second,
        "a submit refused for the pending-ack budget must hold no byte charge: the refusal \
         happens before the byte budget is touched"
    );

    drop(first);
    drop(second);
    router.shutdown().await;
}

/// The pending-ack budget is one `Arc` shared by construction across every
/// pipeline (ADR-2793 decision 4): a per-router budget instead -- each
/// constructor given its own `PendingAckBudget::new` rather than a clone of
/// one shared instance -- would count the metrics pipeline's held permit in a
/// counter the log pipeline never reads, and the refusal below would not
/// happen.
#[tokio::test]
async fn pending_ack_budget_is_shared_across_pipelines() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let budget = Arc::new(PendingAckBudget::new(1));
    let metrics_router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::clone(&budget),
    );
    let log_router = LogIngestRouter::new(
        config(1),
        Arc::clone(&store),
        clock.clone(),
        Arc::clone(&budget),
    );

    let tenant = tenant("acme");
    let point = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];
    let held = metrics_router
        .submit(
            tenant.clone(),
            point,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("metrics submit takes the only permit");

    let record = vec![norm_log_record(1_000, "boot")];
    let refused = log_router
        .submit(
            tenant.clone(),
            record,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    assert!(
        matches!(&refused, Err(LogWriteError::PendingAckLimitReached)),
        "the log pipeline must see the metrics pipeline's held permit, got {:?}",
        refused.err()
    );
    assert_eq!(budget.shed_total(), 1);

    drop(held);
    metrics_router.shutdown().await;
    log_router.shutdown().await;
}

/// `Submitted` holds the pending-ack permit via RAII and must release it on
/// drop even when `acks()` is never called (ADR-2793 decision 4): releasing
/// only inside `acks()` instead -- in its `Ok`/`Err` arms rather than in
/// `PendingAckPermit`'s `Drop` -- would leave the permit held forever here,
/// and the second submit below would refuse instead of succeeding.
#[tokio::test]
async fn dropping_submitted_unawaited_releases_the_permit() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::clone(&budget),
    );
    let tenant = tenant("acme");
    let point = |host: &str| {
        vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", host)],
            1_000,
            1.0,
        )]
    };

    let submitted = router
        .submit(
            tenant.clone(),
            point("a"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("submit takes the only permit");
    assert_eq!(budget.held(), 1);

    drop(submitted);
    assert_eq!(
        budget.held(),
        0,
        "dropping Submitted unawaited must release the permit"
    );

    let second = router
        .submit(
            tenant.clone(),
            point("b"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    assert!(second.is_ok(), "permit must be free again after the drop");

    drop(second);
    router.shutdown().await;
}

#[tokio::test]
async fn multi_shard_write_returns_one_token_per_shard_and_is_read_your_write() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let shard_count = 4;
    let router = IngestRouter::new(
        config(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );

    let tenant = tenant("acme");
    // Enough distinct series that, with 4 shards, more than one shard is hit.
    let points: Vec<_> = (0..16)
        .map(|i| {
            make_point(
                &tenant,
                "cpu_usage",
                &[("host", &i.to_string())],
                1_000,
                i as f64,
            )
        })
        .collect();

    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert!(
        receipt.tokens.len() > 1,
        "expected points to spread across multiple shards, got {} token(s)",
        receipt.tokens.len()
    );

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog config valid");

    let snapshot = catalog
        .resolve(
            &tenant.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: 0,
                end_ns: 2_000,
            },
            &receipt.tokens,
            clock.now(),
        )
        .await
        .expect("resolve succeeds");
    assert_eq!(
        snapshot.segments.len(),
        receipt.tokens.len(),
        "read-your-write must surface every flushed segment via min_tokens"
    );

    router.shutdown().await;
}
