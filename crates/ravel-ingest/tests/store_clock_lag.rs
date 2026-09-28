//! Writer clock lag against the object store's observed clock (ADR-1685).
//!
//! A fold seals an ingest hour from the folder's clock, and a writer stamps
//! its bucket from its own. A writer whose clock lags far enough would publish
//! an acknowledged commit record into an hour a token-less resolve no longer
//! lists. At flush open each shard actor now compares its raw clock reading
//! with the store's observed clock (the latest response `Date`, a lower bound)
//! and refuses the flush, retryably, when the reading lags by more than
//! `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`. With no observation the flush proceeds
//! and is counted as unchecked.
//!
//! Every case drives a real write through the router the server builds, over
//! `MemoryStore` with `set_observed_store_time_ns` standing in for the S3
//! adapter's `Date` observation.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, catalog, make_point, tenant};
use ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
use ravel_commit::keys;
use ravel_ingest::{
    IngestConfig, IngestRouter, LogIngestRouter, LogWriteError, SpanIngestRouter, SpanWriteError,
    WriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId, TimeRange};

/// The store's clock in every case.
const STORE_NS: i64 = 1_700_000_000_000_000_000;
const TWO_HOURS_NS: i64 = 2 * 3_600_000_000_000;
const LAG_MESSAGE: &str = "lags the object store's observed clock";

/// One shard, a one-byte target so each write size-triggers its own flush,
/// and an age tick and delays longer than the two-hour convergence step, so no
/// tick fires during a test and nothing retries a re-buffered flush behind the
/// test's back: every flush counted is one a write caused.
fn flush_per_write_config() -> IngestConfig {
    let day = Duration::from_secs(24 * 3600);
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: day,
        max_flush_delay_idle: day,
        flush_tick: day,
        ..IngestConfig::default()
    }
}

fn observed_store(ns: Option<i64>) -> (Arc<MemoryStore>, Arc<dyn ObjectStoreBackend>) {
    let memory = Arc::new(MemoryStore::new());
    memory.set_observed_store_time_ns(ns);
    let store: Arc<dyn ObjectStoreBackend> = memory.clone();
    (memory, store)
}

/// Every commit record under the tenant's shard-0 commit prefix for `signal`.
async fn commit_records(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> Vec<String> {
    let prefix = keys::commit_shard_prefix(&tenant.hash(), signal, 0).expect("commit prefix");
    list_all(store, &prefix)
        .await
        .expect("list commit prefix")
        .into_iter()
        .map(|o| o.key)
        .collect()
}

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

fn norm_span(start_ns: i64) -> NormalizedSpan {
    NormalizedSpan {
        trace_id: [7u8; 16],
        span_id: [1u8; 8],
        parent_span_id: None,
        name: "handle".to_string(),
        start_ts_ns: start_ns,
        end_ts_ns: start_ns + 100,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

async fn write_metric(
    router: &IngestRouter,
    tenant: &TenantId,
    ts_ns: i64,
) -> Result<ravel_ingest::WriteReceipt, WriteError> {
    router
        .write(
            tenant.clone(),
            vec![make_point(
                tenant,
                "cpu_usage",
                &[("host", "a")],
                ts_ns,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
}

/// The ADR-1685 acceptance case. The writer's clock is two hours behind the
/// store's, past the one-hour-twenty seal margin, so without the check the
/// flush would stamp an hour a fold running on the store's clock has sealed.
/// The strict write fails with the retryable `Abandoned`, nothing is
/// published, and the refusal is counted once. Once the host clock converges
/// the retried data is published and a token-less resolve sees it.
#[tokio::test]
async fn writer_clock_lagging_the_store_refuses_the_flush() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    let err = write_metric(&router, &tenant, 1_000)
        .await
        .expect_err("a writer two hours behind the store must not publish");
    assert!(
        matches!(err, WriteError::Abandoned(_)),
        "a lag refusal is the retryable Abandoned variant; got: {err:?}"
    );
    assert!(err.is_retryable(), "got: {err:?}");
    assert!(
        err.to_string().contains(LAG_MESSAGE),
        "expected the lag refusal message, got: {err}"
    );
    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Metrics).await,
        Vec::<String>::new(),
        "a refused flush publishes no commit record"
    );
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_refused, 1, "one refused flush, one count");
    assert_eq!(snap.clock_lag_unchecked, 0);
    assert_eq!(
        snap.clock_regressions_refused, 0,
        "the lag check runs before the ADR-1307 floor"
    );

    // NTP converges the host onto the store's clock and the client retries.
    clock.set_ns(STORE_NS);
    let receipt = write_metric(&router, &tenant, 1_000)
        .await
        .expect("the retried write publishes once the clock converges");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Metrics)
            .await
            .len(),
        1,
        "the re-buffered row and the retry publish in one flush"
    );
    assert_eq!(router.metrics().snapshot().clock_lag_refused, 1);

    let snapshot = catalog(Arc::clone(&store), 1)
        .resolve(
            &tenant.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: 0,
                end_ns: 2_000,
            },
            &[],
            STORE_NS,
        )
        .await
        .expect("token-less resolve");
    assert_eq!(
        snapshot.segments.len(),
        1,
        "a token-less resolve on the store's clock sees the retried data"
    );

    router.shutdown().await;
}

/// The bound is inclusive: a reading exactly `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`
/// behind the store publishes, one nanosecond further behind is refused.
#[tokio::test]
async fn lag_exactly_at_the_allowance_passes_and_one_ns_past_is_refused() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - DEFAULT_CLOCK_SKEW_ALLOWANCE_NS);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let at = tenant("at-bound");
    write_metric(&router, &at, 1_000)
        .await
        .expect("a lag of exactly the allowance is not refused");
    assert_eq!(router.metrics().snapshot().clock_lag_refused, 0);
    router.shutdown().await;

    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - DEFAULT_CLOCK_SKEW_ALLOWANCE_NS - 1);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let past = tenant("past-bound");
    let err = write_metric(&router, &past, 1_000)
        .await
        .expect_err("one nanosecond past the allowance is refused");
    assert!(matches!(err, WriteError::Abandoned(_)), "got: {err:?}");
    assert_eq!(router.metrics().snapshot().clock_lag_refused, 1);
    assert!(
        commit_records(store.as_ref(), &past, Signal::Metrics)
            .await
            .is_empty()
    );
    router.shutdown().await;
}

/// The check compares the raw reading, not the floor-raised stamp. An earlier
/// flush arms the ADR-1307 floor at `STORE_NS`; the clock then steps back by
/// half the hold bound (absorbed, so the stamp would be `STORE_NS`) while the
/// store's observed clock sits three quarters of the allowance ahead. The
/// stamp lags by less than the allowance and the raw reading by more, so only
/// a check on the raw reading refuses.
#[tokio::test]
async fn lag_is_measured_from_the_raw_reading_not_the_floor() {
    let (memory, store) = observed_store(None);
    let clock = TestClock::new(STORE_NS);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");
    write_metric(&router, &tenant, 1_000)
        .await
        .expect("baseline flush arms the floor");

    memory.set_observed_store_time_ns(Some(STORE_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS * 3 / 4));
    clock.set_ns(STORE_NS - DEFAULT_CLOCK_SKEW_ALLOWANCE_NS / 2);
    let err = write_metric(&router, &tenant, 2_000)
        .await
        .expect_err("the raw reading lags by more than the allowance");
    assert!(matches!(err, WriteError::Abandoned(_)), "got: {err:?}");
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_refused, 1);
    assert_eq!(
        snap.clock_regressions, 0,
        "refused before the floor was consulted, so nothing was absorbed"
    );
    router.shutdown().await;
}

/// With no store-clock observation the flush proceeds, and each flush moves
/// `clock_lag_unchecked` by exactly one, even with a clock far behind true
/// time: refusing on `None` would deadlock a process whose flush is its first
/// store response.
#[tokio::test]
async fn no_observation_publishes_and_counts_each_flush_unchecked() {
    let (_memory, store) = observed_store(None);
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");
    for (i, ts_ns) in [1_000, 2_000, 3_000].into_iter().enumerate() {
        write_metric(&router, &tenant, ts_ns)
            .await
            .expect("an unobserved store never refuses");
        let snap = router.metrics().snapshot();
        assert_eq!(snap.clock_lag_unchecked, i as u64 + 1);
        assert_eq!(snap.clock_lag_refused, 0);
    }
    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Metrics)
            .await
            .len(),
        3
    );
    router.shutdown().await;
}

/// A checked flush within the allowance counts neither refused nor unchecked.
#[tokio::test]
async fn observed_clock_within_the_allowance_counts_nothing() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    write_metric(&router, &tenant("acme"), 1_000)
        .await
        .expect("an in-sync writer publishes");
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_refused, 0);
    assert_eq!(snap.clock_lag_unchecked, 0);
    router.shutdown().await;
}

async fn write_log(
    router: &LogIngestRouter,
    tenant: &TenantId,
    ts_ns: i64,
) -> Result<(), LogWriteError> {
    router
        .write(
            tenant.clone(),
            vec![norm_log_record(ts_ns, "line")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .map(|_| ())
}

/// Logs: the log shard actor applies the same check at its flush-open site.
#[tokio::test]
async fn logs_writer_clock_lagging_the_store_refuses_the_flush() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    let err = write_log(&router, &tenant, 1_000)
        .await
        .expect_err("a log writer two hours behind the store must not publish");
    assert!(
        matches!(err, LogWriteError::Abandoned(_)),
        "a lag refusal is the retryable Abandoned variant; got: {err:?}"
    );
    assert!(err.to_string().contains(LAG_MESSAGE), "got: {err}");
    assert!(
        commit_records(store.as_ref(), &tenant, Signal::Logs)
            .await
            .is_empty()
    );
    assert_eq!(router.metrics().snapshot().clock_lag_refused, 1);

    clock.set_ns(STORE_NS);
    write_log(&router, &tenant, 2_000)
        .await
        .expect("the retried log write publishes once the clock converges");
    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Logs)
            .await
            .len(),
        1
    );
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_refused, 1);
    assert_eq!(snap.clock_lag_unchecked, 0);
    router.shutdown().await;
}

#[tokio::test]
async fn logs_no_observation_counts_each_flush_unchecked() {
    let (_memory, store) = observed_store(None);
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    for (i, ts_ns) in [1_000, 2_000].into_iter().enumerate() {
        write_log(&router, &tenant, ts_ns)
            .await
            .expect("an unobserved store never refuses");
        let snap = router.metrics().snapshot();
        assert_eq!(snap.clock_lag_unchecked, i as u64 + 1);
        assert_eq!(snap.clock_lag_refused, 0);
    }
    router.shutdown().await;
}

async fn write_span(
    router: &SpanIngestRouter,
    tenant: &TenantId,
    start_ns: i64,
) -> Result<(), SpanWriteError> {
    router
        .write(
            tenant.clone(),
            vec![norm_span(start_ns)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .map(|_| ())
}

/// Spans: the span shard actor applies the same check at its flush-open site.
#[tokio::test]
async fn spans_writer_clock_lagging_the_store_refuses_the_flush() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    let err = write_span(&router, &tenant, 1_000)
        .await
        .expect_err("a span writer two hours behind the store must not publish");
    assert!(
        matches!(err, SpanWriteError::Abandoned(_)),
        "a lag refusal is the retryable Abandoned variant; got: {err:?}"
    );
    assert!(err.to_string().contains(LAG_MESSAGE), "got: {err}");
    assert!(
        commit_records(store.as_ref(), &tenant, Signal::Spans)
            .await
            .is_empty()
    );
    assert_eq!(router.metrics().snapshot().clock_lag_refused, 1);

    clock.set_ns(STORE_NS);
    write_span(&router, &tenant, 2_000)
        .await
        .expect("the retried span write publishes once the clock converges");
    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Spans)
            .await
            .len(),
        1
    );
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_refused, 1);
    assert_eq!(snap.clock_lag_unchecked, 0);
    router.shutdown().await;
}

#[tokio::test]
async fn spans_no_observation_counts_each_flush_unchecked() {
    let (_memory, store) = observed_store(None);
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    for (i, start_ns) in [1_000, 2_000].into_iter().enumerate() {
        write_span(&router, &tenant, start_ns)
            .await
            .expect("an unobserved store never refuses");
        let snap = router.metrics().snapshot();
        assert_eq!(snap.clock_lag_unchecked, i as u64 + 1);
        assert_eq!(snap.clock_lag_refused, 0);
    }
    router.shutdown().await;
}
