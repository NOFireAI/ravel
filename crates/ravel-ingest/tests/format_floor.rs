//! The ingest write path's format-floor check (ADR-1746 decisions 4 and 5).
//!
//! A tenant's provisioning record is written and its floor raised with the
//! catalog's own functions, one above the version each pipeline writes. A
//! write into that bucket, strict or buffered, is refused at admission on the
//! record the router's generation switch cached for routing, before any shard
//! buffers it: it is answered with `BelowFormatFloor`, the refusal is counted,
//! and the store sees no PUT. Rows buffered before a refresh raised the floor
//! are refused at flush open instead, dropped and counted. The clock is
//! injected and never advanced except where a case says so, so every flush
//! here is a strict-arrival flush opened inside its write or one a case drives
//! with `flush_all`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_catalog::{AbsentPolicy, FloorBasis, raise_format_floor, validate_or_adopt};
use ravel_ingest::{
    BelowFormatFloor, FormatFamily, FormatFloorMetrics, FormatFloorPolicy, IngestByteBudget,
    IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter, LogWriteError,
    PendingAckBudget, SpanIngestRouter, SpanWriteError, WriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Past the generation switch's 60 s refresh interval, so the next write
/// re-reads the provisioning record before it routes.
const PAST_REFRESH_NS: i64 = 61_000_000_000;

const ACK_DEADLINE: Duration = Duration::from_secs(60);

/// One shard, with age clocks no case reaches: only a strict-arrival write
/// opens a flush.
fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    }
}

fn unlimited() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)
}

/// An instrumented store holding `tenant`'s provisioning record for `signal`,
/// with `floor` raised for `family` when given.
async fn provisioned(
    tenant: &TenantId,
    signal: Signal,
    floor: Option<(FormatFamily, u32)>,
) -> Arc<InstrumentedStore<MemoryStore>> {
    let store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        signal,
        1,
        BASE_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    if let Some((family, version)) = floor {
        raise_floor(store.as_ref(), tenant, signal, family, version).await;
    }
    store
}

async fn raise_floor(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
    family: FormatFamily,
    version: u32,
) {
    raise_format_floor(
        store,
        &tenant.hash(),
        signal,
        family.as_str(),
        version,
        FloorBasis {
            observed_entries: 0,
            observed_newest_created_unix_ns: 0,
            observed_shards: 1,
        },
        "test",
        BASE_NS,
    )
    .await
    .expect("raise floor");
}

fn puts(store: &InstrumentedStore<MemoryStore>) -> u64 {
    store.metrics().snapshot().put.calls
}

fn refusing(metrics: &Arc<FormatFloorMetrics>) -> FormatFloorPolicy {
    FormatFloorPolicy::new(false, Arc::clone(metrics))
}

fn metrics_router(
    store: &Arc<InstrumentedStore<MemoryStore>>,
    clock: &Arc<TestClock>,
    policy: FormatFloorPolicy,
) -> IngestRouter {
    metrics_router_with(store, clock, policy, unlimited())
}

fn metrics_router_with(
    store: &Arc<InstrumentedStore<MemoryStore>>,
    clock: &Arc<TestClock>,
    policy: FormatFloorPolicy,
    budget: Arc<IngestByteBudget>,
) -> IngestRouter {
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    IngestRouter::new(
        config(),
        store,
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(budget)
    .with_format_floor_policy(policy)
}

fn log_router(
    store: &Arc<InstrumentedStore<MemoryStore>>,
    clock: &Arc<TestClock>,
    policy: FormatFloorPolicy,
    budget: Arc<IngestByteBudget>,
) -> LogIngestRouter {
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    LogIngestRouter::new(
        config(),
        store,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(budget)
    .with_format_floor_policy(policy)
}

fn span_router(
    store: &Arc<InstrumentedStore<MemoryStore>>,
    clock: &Arc<TestClock>,
    policy: FormatFloorPolicy,
    budget: Arc<IngestByteBudget>,
) -> SpanIngestRouter {
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    SpanIngestRouter::new(
        config(),
        store,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(budget)
    .with_format_floor_policy(policy)
}

/// A finite budget far above anything a case here buffers, so every charge
/// is admitted and the gauge reads exactly what is held.
fn bounded() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1 << 30))
}

async fn write_metric(
    router: &IngestRouter,
    tenant: &TenantId,
    i: usize,
) -> Result<ravel_ingest::WriteReceipt, WriteError> {
    write_metric_in(router, tenant, i, WriteMode::StrictArrival).await
}

async fn write_metric_in(
    router: &IngestRouter,
    tenant: &TenantId,
    i: usize,
    mode: WriteMode,
) -> Result<ravel_ingest::WriteReceipt, WriteError> {
    let host = format!("h{i}");
    let point = make_point(tenant, "cpu_usage", &[("host", &host)], 1_000, 1.0);
    router
        .write(tenant.clone(), vec![point], mode, ACK_DEADLINE)
        .await
}

fn log_record() -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns: 1_000,
        observed_ts_ns: 1_000,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "line".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn span() -> NormalizedSpan {
    NormalizedSpan {
        trace_id: [1; 16],
        span_id: [1; 8],
        parent_span_id: None,
        name: "handle".to_string(),
        start_ts_ns: 1_000,
        end_ts_ns: 1_100,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

fn above(family: FormatFamily) -> BelowFormatFloor {
    BelowFormatFloor {
        family,
        floor: family.writable_version() + 1,
        writing: family.writable_version(),
    }
}

/// A strict metrics write into a bucket whose RSEG floor is one above the
/// writer's version is refused before any PUT, and gets the typed refusal.
#[tokio::test]
async fn a_strict_metrics_write_below_the_floor_is_refused_with_no_put() {
    let acme = tenant("acme");
    let family = FormatFamily::Rseg;
    let store = provisioned(
        &acme,
        Signal::Metrics,
        Some((family, family.writable_version() + 1)),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let router = metrics_router(&store, &clock, refusing(&metrics));
    let before = puts(&store);

    let err = write_metric(&router, &acme, 0)
        .await
        .expect_err("a flush below the floor is refused");

    assert!(
        matches!(err, WriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert!(err.is_retryable());
    assert_eq!(puts(&store), before, "a refused write PUTs nothing");
    assert_eq!(metrics.refused(family), 1);
    assert_eq!(metrics.writes_below(family), 0);
    assert_eq!(router.metrics().snapshot().flushes_by_arrival, 0);
}

/// The log pipeline refuses below its RLOG floor the same way, with a
/// retryable error.
#[tokio::test]
async fn a_strict_log_write_below_the_floor_is_refused_with_no_put() {
    let acme = tenant("acme");
    let family = FormatFamily::Rlog;
    let store = provisioned(
        &acme,
        Signal::Logs,
        Some((family, family.writable_version() + 1)),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let router = log_router(&store, &clock, refusing(&metrics), unlimited());
    let before = puts(&store);

    let err = router
        .write(
            acme.clone(),
            vec![log_record()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect_err("a flush below the floor is refused");

    assert!(
        matches!(err, LogWriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert!(err.is_retryable(), "a floor refusal is retryable");
    assert_eq!(puts(&store), before, "a refused write PUTs nothing");
    assert_eq!(metrics.refused(family), 1);
}

/// The span pipeline refuses below its RSPAN floor the same way, with a
/// retryable error.
#[tokio::test]
async fn a_strict_span_write_below_the_floor_is_refused_with_no_put() {
    let acme = tenant("acme");
    let family = FormatFamily::Rspan;
    let store = provisioned(
        &acme,
        Signal::Spans,
        Some((family, family.writable_version() + 1)),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let router = span_router(&store, &clock, refusing(&metrics), unlimited());
    let before = puts(&store);

    let err = router
        .write(
            acme.clone(),
            vec![span()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect_err("a flush below the floor is refused");

    assert!(
        matches!(err, SpanWriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert!(err.is_retryable(), "a floor refusal is retryable");
    assert_eq!(puts(&store), before, "a refused write PUTs nothing");
    assert_eq!(metrics.refused(family), 1);
}

/// A floor at the writer's own version refuses nothing: the check is
/// strictly below.
#[tokio::test]
async fn a_floor_at_the_writers_version_admits_the_flush() {
    let acme = tenant("acme");
    let family = FormatFamily::Rseg;
    let store = provisioned(
        &acme,
        Signal::Metrics,
        Some((family, family.writable_version())),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let router = metrics_router(&store, &clock, refusing(&metrics));
    let before = puts(&store);

    let receipt = write_metric(&router, &acme, 0)
        .await
        .expect("a floor at the version admits");

    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(
        puts(&store),
        before + 2,
        "one data object, one commit record"
    );
    assert_eq!(metrics.refused(family), 0);
}

/// The refusal follows the cached record: a floor raised while the router
/// runs reaches it on the next re-read, which a write past the 60 s refresh
/// interval makes before it routes, and the next flush is refused. The first
/// flush, before the raise, lands.
#[tokio::test]
async fn the_refusal_survives_a_refresh_that_raises_the_floor_mid_run() {
    let acme = tenant("acme");
    let family = FormatFamily::Rseg;
    let store = provisioned(&acme, Signal::Metrics, None).await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let router = metrics_router(&store, &clock, refusing(&metrics));

    write_metric(&router, &acme, 0)
        .await
        .expect("no floor yet: the flush lands");
    let after_first = puts(&store);

    raise_floor(
        store.as_ref(),
        &acme,
        Signal::Metrics,
        family,
        family.writable_version() + 1,
    )
    .await;
    let after_raise = puts(&store);
    assert_eq!(after_raise, after_first + 1, "the raise is one record PUT");

    // Inside the refresh interval the cached view has no floor yet.
    write_metric(&router, &acme, 1)
        .await
        .expect("the cached view predates the raise");
    let after_cached = puts(&store);
    assert_eq!(after_cached, after_raise + 2);

    clock.advance_ns(PAST_REFRESH_NS);
    let err = write_metric(&router, &acme, 2)
        .await
        .expect_err("the refreshed view carries the floor");
    assert!(
        matches!(err, WriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert_eq!(puts(&store), after_cached, "the refused write PUTs nothing");
    assert_eq!(metrics.refused(family), 1);
}

/// Under `--write-below-format-floor` the flush lands and is counted once.
#[tokio::test]
async fn the_override_writes_below_the_floor_and_counts_it() {
    let acme = tenant("acme");
    let family = FormatFamily::Rseg;
    let store = provisioned(
        &acme,
        Signal::Metrics,
        Some((family, family.writable_version() + 1)),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let router = metrics_router(
        &store,
        &clock,
        FormatFloorPolicy::new(true, Arc::clone(&metrics)),
    );
    let before = puts(&store);

    let receipt = write_metric(&router, &acme, 0)
        .await
        .expect("the override writes below the floor");

    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(puts(&store), before + 2);
    assert_eq!(metrics.writes_below(family), 1);
    assert_eq!(metrics.refused(family), 0);
}

/// A buffered metrics write into a floored bucket is refused at admission,
/// before any shard buffers it: it gets the retryable refusal instead of the
/// 2xx a buffered write gets at enqueue, it is counted once, and nothing is
/// left buffered, so the byte gauge reads 0 and a later `flush_all` finds no
/// rows to refuse or PUT.
#[tokio::test]
async fn a_buffered_metrics_write_below_the_floor_is_refused_and_nothing_is_buffered() {
    let acme = tenant("acme");
    let family = FormatFamily::Rseg;
    let store = provisioned(
        &acme,
        Signal::Metrics,
        Some((family, family.writable_version() + 1)),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let budget = unlimited();
    let router = metrics_router_with(&store, &clock, refusing(&metrics), budget.clone());
    let before = puts(&store);

    let err = write_metric_in(&router, &acme, 0, WriteMode::Buffered)
        .await
        .expect_err("a buffered write below the floor is refused, not acknowledged");

    assert!(
        matches!(err, WriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert!(err.is_retryable());
    assert_eq!(budget.in_flight_bytes(), 0, "nothing is buffered");
    assert_eq!(metrics.refused(family), 1);
    router.flush_all().await;
    assert_eq!(metrics.refused(family), 1, "no buffered rows reach a flush");
    assert_eq!(puts(&store), before);
}

/// The log pipeline refuses a buffered write at admission the same way.
#[tokio::test]
async fn a_buffered_log_write_below_the_floor_is_refused_and_nothing_is_buffered() {
    let acme = tenant("acme");
    let family = FormatFamily::Rlog;
    let store = provisioned(
        &acme,
        Signal::Logs,
        Some((family, family.writable_version() + 1)),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let budget = unlimited();
    let router = log_router(&store, &clock, refusing(&metrics), budget.clone());
    let before = puts(&store);

    let err = router
        .write(
            acme.clone(),
            vec![log_record()],
            WriteMode::Buffered,
            ACK_DEADLINE,
        )
        .await
        .expect_err("a buffered write below the floor is refused, not acknowledged");

    assert!(
        matches!(err, LogWriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert_eq!(budget.in_flight_bytes(), 0, "nothing is buffered");
    assert_eq!(metrics.refused(family), 1);
    router.flush_all().await;
    assert_eq!(metrics.refused(family), 1, "no buffered rows reach a flush");
    assert_eq!(puts(&store), before);
}

/// The span pipeline refuses a buffered write at admission the same way.
#[tokio::test]
async fn a_buffered_span_write_below_the_floor_is_refused_and_nothing_is_buffered() {
    let acme = tenant("acme");
    let family = FormatFamily::Rspan;
    let store = provisioned(
        &acme,
        Signal::Spans,
        Some((family, family.writable_version() + 1)),
    )
    .await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let budget = unlimited();
    let router = span_router(&store, &clock, refusing(&metrics), budget.clone());
    let before = puts(&store);

    let err = router
        .write(
            acme.clone(),
            vec![span()],
            WriteMode::Buffered,
            ACK_DEADLINE,
        )
        .await
        .expect_err("a buffered write below the floor is refused, not acknowledged");

    assert!(
        matches!(err, SpanWriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert_eq!(budget.in_flight_bytes(), 0, "nothing is buffered");
    assert_eq!(metrics.refused(family), 1);
    router.flush_all().await;
    assert_eq!(metrics.refused(family), 1, "no buffered rows reach a flush");
    assert_eq!(puts(&store), before);
}

/// Rows buffered before a refresh raised the floor are refused when their
/// flush opens: they are dropped, counted, PUT nothing, and the flush hands
/// its byte-budget charge back, so a finite budget's gauge returns to its
/// pre-write value. The write that refreshed the view is itself refused at
/// admission, so the counter reads two.
#[tokio::test]
async fn rows_buffered_before_the_floor_rose_are_dropped_counted_and_release_their_charge() {
    let acme = tenant("acme");
    let family = FormatFamily::Rseg;
    let store = provisioned(&acme, Signal::Metrics, None).await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let budget = bounded();
    let router = metrics_router_with(&store, &clock, refusing(&metrics), budget.clone());
    let gauge_before = budget.in_flight_bytes();

    write_metric_in(&router, &acme, 0, WriteMode::Buffered)
        .await
        .expect("no floor yet: buffered");
    assert!(budget.in_flight_bytes() > gauge_before, "the rows are held");
    raise_floor(
        store.as_ref(),
        &acme,
        Signal::Metrics,
        family,
        family.writable_version() + 1,
    )
    .await;
    let after_raise = puts(&store);

    clock.advance_ns(PAST_REFRESH_NS);
    let err = write_metric_in(&router, &acme, 1, WriteMode::Buffered)
        .await
        .expect_err("the refreshed view carries the floor");
    assert!(
        matches!(err, WriteError::BelowFormatFloor(refused) if refused == above(family)),
        "{err:?}"
    );
    assert_eq!(metrics.refused(family), 1, "the refreshing write");

    router.flush_all().await;

    assert_eq!(metrics.refused(family), 2, "the buffered rows' flush");
    assert_eq!(puts(&store), after_raise, "the refused flush PUTs nothing");
    assert_eq!(
        budget.in_flight_bytes(),
        gauge_before,
        "the refused flush released its charge"
    );
}

/// The log pipeline's flush-open backstop drops and counts rows buffered
/// before the floor rose, the same way.
#[tokio::test]
async fn log_rows_buffered_before_the_floor_rose_are_dropped_and_counted() {
    let acme = tenant("acme");
    let family = FormatFamily::Rlog;
    let store = provisioned(&acme, Signal::Logs, None).await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let budget = bounded();
    let router = log_router(&store, &clock, refusing(&metrics), budget.clone());
    let write = |mode| router.write(acme.clone(), vec![log_record()], mode, ACK_DEADLINE);

    write(WriteMode::Buffered).await.expect("no floor yet");
    assert!(budget.in_flight_bytes() > 0);
    raise_floor(
        store.as_ref(),
        &acme,
        Signal::Logs,
        family,
        family.writable_version() + 1,
    )
    .await;
    let after_raise = puts(&store);
    clock.advance_ns(PAST_REFRESH_NS);
    write(WriteMode::Buffered)
        .await
        .expect_err("the refreshed view carries the floor");

    router.flush_all().await;

    assert_eq!(metrics.refused(family), 2);
    assert_eq!(puts(&store), after_raise);
    assert_eq!(budget.in_flight_bytes(), 0);
}

/// The span pipeline's flush-open backstop drops and counts rows buffered
/// before the floor rose, the same way.
#[tokio::test]
async fn span_rows_buffered_before_the_floor_rose_are_dropped_and_counted() {
    let acme = tenant("acme");
    let family = FormatFamily::Rspan;
    let store = provisioned(&acme, Signal::Spans, None).await;
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let budget = bounded();
    let router = span_router(&store, &clock, refusing(&metrics), budget.clone());
    let write = |mode| router.write(acme.clone(), vec![span()], mode, ACK_DEADLINE);

    write(WriteMode::Buffered).await.expect("no floor yet");
    assert!(budget.in_flight_bytes() > 0);
    raise_floor(
        store.as_ref(),
        &acme,
        Signal::Spans,
        family,
        family.writable_version() + 1,
    )
    .await;
    let after_raise = puts(&store);
    clock.advance_ns(PAST_REFRESH_NS);
    write(WriteMode::Buffered)
        .await
        .expect_err("the refreshed view carries the floor");

    router.flush_all().await;

    assert_eq!(metrics.refused(family), 2);
    assert_eq!(puts(&store), after_raise);
    assert_eq!(budget.in_flight_bytes(), 0);
}

/// A flush refused at flush open does not consume a sequence number: the
/// shard's next successful flush, for a tenant with no floor on the same
/// shard, carries the `writer_seq` right after the one before the refusal.
#[tokio::test]
async fn a_refused_flush_does_not_advance_the_shards_sequence() {
    let acme = tenant("acme");
    let bob = tenant("bob");
    let family = FormatFamily::Rseg;
    let store = provisioned(&acme, Signal::Metrics, None).await;
    validate_or_adopt(
        store.as_ref(),
        &bob.hash(),
        Signal::Metrics,
        1,
        BASE_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision bob");
    let clock = TestClock::new(BASE_NS);
    let metrics = Arc::new(FormatFloorMetrics::default());
    let router = metrics_router(&store, &clock, refusing(&metrics));

    let first = write_metric(&router, &bob, 0).await.expect("bob lands");
    write_metric_in(&router, &acme, 1, WriteMode::Buffered)
        .await
        .expect("no floor yet: buffered");
    raise_floor(
        store.as_ref(),
        &acme,
        Signal::Metrics,
        family,
        family.writable_version() + 1,
    )
    .await;
    clock.advance_ns(PAST_REFRESH_NS);
    write_metric_in(&router, &acme, 2, WriteMode::Buffered)
        .await
        .expect_err("the refreshed view carries the floor");
    router.flush_all().await;
    assert_eq!(metrics.refused(family), 2, "the flush itself was refused");
    let second = write_metric(&router, &bob, 3).await.expect("bob lands");

    assert_eq!(first.tokens.len(), 1);
    assert_eq!(second.tokens.len(), 1);
    assert_eq!(
        second.tokens[0].seq,
        first.tokens[0].seq + 1,
        "the refused flush pinned no seq"
    );
}
