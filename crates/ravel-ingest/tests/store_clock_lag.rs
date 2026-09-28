//! Writer clock lag against the object store's observed clock (ADR-1685).
//!
//! A fold seals an ingest hour from the folder's clock, and a writer stamps
//! its bucket from its own. A writer whose clock lags far enough would publish
//! an acknowledged commit record into an hour a token-less resolve no longer
//! lists. At flush open each shard actor now compares its raw clock reading
//! with the store's observed clock (the latest response `Date`, a lower bound)
//! and refuses the flush, retryably, when the reading lags by more than
//! `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS`. With no observation the flush proceeds
//! and is counted as unchecked. The one exception is a teardown drain's bypass
//! passes, where durability wins: the check is bypassed so acknowledged
//! buffered-mode rows publish rather than being dropped (the ADR-1685 teardown
//! amendment). The ADR-1307 floor still applies on a bypass pass, so the drain
//! makes those passes in a bounded loop: the first one is where a backwards
//! step the lag check had hidden from the floor surfaces, and the pass after it
//! publishes.
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
use ravel_commit::{keys, record};
use ravel_ingest::{
    IngestConfig, IngestRouter, LogIngestRouter, LogWriteError, MAX_FLUSH_ALL_PASSES,
    SpanIngestRouter, SpanWriteError, WriteError, WriteMode,
};
use ravel_logseg::{Predicate, RlogConfig, RlogReader, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::{RspanConfig, RspanReader, SpanQuery, StatusCode};
use ravel_segment::{ReaderLimits, SeriesEntryV4};
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

/// The bytes of the data object named by the one commit record under the
/// tenant's shard-0 commit prefix for `signal`. Panics unless exactly one
/// commit record exists, and follows that record's own `object_key` rather
/// than guessing a data key, so the bytes are the ones this commit published.
async fn sole_published_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> bytes::Bytes {
    let commits = commit_records(store, tenant, signal).await;
    assert_eq!(commits.len(), 1, "expected exactly one commit record");
    let commit_bytes = store
        .get(&commits[0], GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded = record::decode(&commit_bytes).expect("decode commit record");
    store
        .get(&decoded.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data
}

/// Every scalar sample in the tenant's sole published metrics segment, as
/// `(ts_ns, value bit pattern)` sorted by timestamp. Bit patterns, not `==`,
/// because -0.0 and NaN payloads are significant in this storage path.
async fn published_samples(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<(i64, u64)> {
    let data = sole_published_object(store, tenant, Signal::Metrics).await;
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&data, limits).expect("opens segment");
    let entries =
        ravel_segment::decode_catalog_v5(&loc.footer, &data, limits).expect("decodes catalog");
    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v4(&loc.footer, &selected).expect("plans ranges");
    let mut out = Vec::new();
    for entry in &entries {
        for (run_index, run) in entry.runs.iter().enumerate() {
            let range = ranges
                .iter()
                .find(|r| r.series_id == entry.entry.series_id && r.run_index == run_index)
                .expect("planned range for this run");
            let ts_start = range.ts_range.0 as usize;
            let ts_bytes = &data[ts_start..ts_start + range.ts_range.1 as usize];
            let val_start = range.val_range.0 as usize;
            let val_bytes = &data[val_start..val_start + range.val_range.1 as usize];
            let mut scratch = Vec::new();
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            ravel_segment::decode_run_pages_soa(
                &entry.entry.series_id,
                run,
                ts_bytes,
                val_bytes,
                limits,
                &mut scratch,
                &mut timestamps,
                &mut values,
            )
            .expect("decodes run");
            for (ts, value) in timestamps.iter().zip(values.iter()) {
                out.push((*ts, value.to_bits()));
            }
        }
    }
    out.sort_unstable();
    out
}

/// Every record body in the tenant's sole published log segment, sorted.
async fn published_log_bodies(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<String> {
    let data = sole_published_object(store, tenant, Signal::Logs).await;
    let reader = RlogReader::new(&data, &RlogConfig::default()).expect("open rlog");
    let (records, _stats) = reader
        .scan(&Predicate::And(Vec::new()))
        .expect("unfiltered scan");
    let mut bodies: Vec<String> = records.into_iter().map(|r| r.body).collect();
    bodies.sort();
    bodies
}

/// Every span start timestamp in the tenant's sole published span segment,
/// sorted. Every span this suite writes shares one trace id, so one
/// trace-scoped scan sees them all.
async fn published_span_starts(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<i64> {
    let data = sole_published_object(store, tenant, Signal::Spans).await;
    let reader = RspanReader::new(&data, &RspanConfig::default()).expect("open rspan");
    let (spans, _stats) = reader
        .scan(&SpanQuery::trace([7u8; 16], i64::MIN, i64::MAX))
        .expect("trace scan");
    let mut starts: Vec<i64> = spans.into_iter().map(|s| s.start_ts_ns).collect();
    starts.sort_unstable();
    starts
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

/// One span of the suite's single trace. `span_id` is derived from `start_ns`
/// so two spans written at different starts are two distinct records rather
/// than the same span twice.
fn norm_span(start_ns: i64) -> NormalizedSpan {
    NormalizedSpan {
        trace_id: [7u8; 16],
        span_id: start_ns.to_be_bytes(),
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
///
/// The retry uses a second timestamp so the published segment distinguishes
/// the re-buffered row from the retried one: the single flush carries BOTH
/// samples, which is what "the refusal costs availability, not durability"
/// means. A refuse arm that dropped the buffer instead of re-inserting it
/// would publish one commit record holding only ts 2_000 and still pass every
/// count-only assertion.
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

    // NTP converges the host onto the store's clock and the client retries,
    // at a second timestamp so the re-buffered row is distinguishable.
    clock.set_ns(STORE_NS);
    let receipt = write_metric(&router, &tenant, 2_000)
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
    assert_eq!(
        published_samples(store.as_ref(), &tenant).await,
        vec![(1_000, 1.0_f64.to_bits()), (2_000, 1.0_f64.to_bits())],
        "that one flush carries the re-buffered row AND the retry"
    );
    assert_eq!(router.metrics().snapshot().clock_lag_refused, 1);

    let snapshot = catalog(Arc::clone(&store), 1)
        .resolve(
            &tenant.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: 0,
                end_ns: 3_000,
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
    write_log_body(router, tenant, ts_ns, "line").await
}

async fn write_log_body(
    router: &LogIngestRouter,
    tenant: &TenantId,
    ts_ns: i64,
    body: &str,
) -> Result<(), LogWriteError> {
    router
        .write(
            tenant.clone(),
            vec![norm_log_record(ts_ns, body)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .map(|_| ())
}

/// Logs: the log shard actor applies the same check at its flush-open site,
/// and the retry's single flush carries the re-buffered record too.
#[tokio::test]
async fn logs_writer_clock_lagging_the_store_refuses_the_flush() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    let err = write_log_body(&router, &tenant, 1_000, "refused")
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
    write_log_body(&router, &tenant, 2_000, "retried")
        .await
        .expect("the retried log write publishes once the clock converges");
    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Logs)
            .await
            .len(),
        1
    );
    assert_eq!(
        published_log_bodies(store.as_ref(), &tenant).await,
        vec!["refused".to_string(), "retried".to_string()],
        "that one flush carries the re-buffered record AND the retry"
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

/// Spans: the span shard actor applies the same check at its flush-open site,
/// and the retry's single flush carries the re-buffered span too.
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
    assert_eq!(
        published_span_starts(store.as_ref(), &tenant).await,
        vec![1_000, 2_000],
        "that one flush carries the re-buffered span AND the retry"
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

/// Logs: the check compares the raw reading, not the floor-raised stamp.
/// Mirror of `lag_is_measured_from_the_raw_reading_not_the_floor`: an earlier
/// flush arms the floor at `STORE_NS`, the clock then steps back half the hold
/// bound (absorbed, so the stamp would be `STORE_NS`) while the observation
/// sits three quarters of the allowance ahead. Only a check on the raw reading
/// refuses.
#[tokio::test]
async fn logs_lag_is_measured_from_the_raw_reading_not_the_floor() {
    let (memory, store) = observed_store(None);
    let clock = TestClock::new(STORE_NS);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    write_log(&router, &tenant, 1_000)
        .await
        .expect("baseline flush arms the floor");

    memory.set_observed_store_time_ns(Some(STORE_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS * 3 / 4));
    clock.set_ns(STORE_NS - DEFAULT_CLOCK_SKEW_ALLOWANCE_NS / 2);
    let err = write_log(&router, &tenant, 2_000)
        .await
        .expect_err("the raw reading lags by more than the allowance");
    assert!(matches!(err, LogWriteError::Abandoned(_)), "got: {err:?}");
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_refused, 1);
    assert_eq!(
        snap.clock_regressions, 0,
        "refused before the floor was consulted, so nothing was absorbed"
    );
    router.shutdown().await;
}

/// Spans: the same raw-reading-versus-stamp case.
#[tokio::test]
async fn spans_lag_is_measured_from_the_raw_reading_not_the_floor() {
    let (memory, store) = observed_store(None);
    let clock = TestClock::new(STORE_NS);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    write_span(&router, &tenant, 1_000)
        .await
        .expect("baseline flush arms the floor");

    memory.set_observed_store_time_ns(Some(STORE_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS * 3 / 4));
    clock.set_ns(STORE_NS - DEFAULT_CLOCK_SKEW_ALLOWANCE_NS / 2);
    let err = write_span(&router, &tenant, 2_000)
        .await
        .expect_err("the raw reading lags by more than the allowance");
    assert!(matches!(err, SpanWriteError::Abandoned(_)), "got: {err:?}");
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_refused, 1);
    assert_eq!(
        snap.clock_regressions, 0,
        "refused before the floor was consulted, so nothing was absorbed"
    );
    router.shutdown().await;
}

/// A buffer nothing flushes until a drain asks: no size trigger (the target is
/// far above one row), no age trigger, no tick. So every refusal a drain case
/// counts is a drain pass's, which makes the counts exact rather than a race
/// against the actor's own triggers.
fn buffer_until_drain_config() -> IngestConfig {
    let day = Duration::from_secs(24 * 3600);
    IngestConfig {
        shard_count: 1,
        target_bytes: 64 * 1024 * 1024,
        min_flush_bytes: 0,
        max_flush_delay: day,
        max_flush_delay_idle: day,
        flush_tick: day,
        ..IngestConfig::default()
    }
}

/// How many refusals a drain counts under [`buffer_until_drain_config`] with a
/// lagging clock: one per enforced pass, because a lag refusal re-anchors
/// nothing and every pass reads the same lag (see `MAX_FLUSH_ALL_PASSES`).
/// Bypass passes add nothing here: they do not run the check.
const DRAIN_REFUSALS: u64 = MAX_FLUSH_ALL_PASSES as u64;

/// The ADR-1685 teardown amendment. Buffered mode acknowledged these rows, and
/// a lag refusal changes neither the monotonic floor nor the store's
/// observation, so every enforced drain pass refuses identically and the pass
/// cap would have reported them as residue and dropped them. Durability wins at
/// teardown: the drain's bypass passes skip the check, and here the floor is
/// unarmed, so the first of them stamps the raw reading, publishes, and counts
/// the bypass once.
///
/// Deleting the teardown bypass passes from all three `flush_all`s fails this
/// on its first assertion, the commit-record count: the drain publishes nothing
/// (left 0, right 1). The twelve cases above this one still pass under that
/// deletion, so none of them rests on the bypass.
#[tokio::test]
async fn shutdown_publishes_what_the_lag_check_refused_and_counts_the_bypass() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = IngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    // Buffered mode: the write is acknowledged now, so losing these rows on a
    // graceful drain would be a loss of an acknowledged write.
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("a buffered write is acknowledged without waiting for its flush");

    // `shutdown` consumes the router, so keep the counters alive past it.
    let metrics = router.metrics_handle();
    router.shutdown().await;

    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Metrics)
            .await
            .len(),
        1,
        "the teardown drain's first bypass pass publishes the acknowledged rows"
    );
    assert_eq!(
        published_samples(store.as_ref(), &tenant).await,
        vec![(1_000, 1.0_f64.to_bits())],
        "and that commit's segment holds exactly the buffered row"
    );
    let snap = metrics.snapshot();
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "nothing was left buffered, so nothing is residue"
    );
    assert_eq!(
        snap.clock_lag_bypassed_at_shutdown, 1,
        "one flush published with the check bypassed"
    );
    assert_eq!(
        snap.clock_lag_refused, DRAIN_REFUSALS,
        "every enforced drain pass refused before the bypass pass ran"
    );
    assert_eq!(
        snap.clock_regressions_refused, 0,
        "the bypass changes the lag check only; the floor rules did not fire"
    );
}

/// The bypass is teardown-only. `flush_all` on a live router is the `FlushNow`
/// drain, whose actor keeps running, so a lagging flush is still refused and
/// its rows stay buffered for a later trigger.
#[tokio::test]
async fn flush_now_is_still_refused_with_a_lagging_clock() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = IngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write acknowledged");

    router.flush_all().await;

    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Metrics).await,
        Vec::<String>::new(),
        "FlushNow never bypasses the lag check"
    );
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_bypassed_at_shutdown, 0);
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "a Retryable drain's residue is a WARN, not a durability-defect bump"
    );
    assert_eq!(
        snap.clock_lag_refused, DRAIN_REFUSALS,
        "every pass of the FlushNow drain refused, and none was bypassed"
    );

    // Still recoverable: the rows were re-buffered, not dropped, so they
    // publish once the host clock converges.
    clock.set_ns(STORE_NS);
    router.flush_all().await;
    assert_eq!(
        published_samples(store.as_ref(), &tenant).await,
        vec![(1_000, 1.0_f64.to_bits())]
    );
    assert_eq!(
        router.metrics().snapshot().clock_lag_bypassed_at_shutdown,
        0
    );

    router.shutdown().await;
}

/// Logs: the same teardown bypass at the log shard's drain.
#[tokio::test]
async fn logs_shutdown_publishes_what_the_lag_check_refused() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = LogIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "acked")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered log write acknowledged");

    let metrics = router.metrics_handle();
    router.shutdown().await;

    assert_eq!(
        published_log_bodies(store.as_ref(), &tenant).await,
        vec!["acked".to_string()],
        "the teardown drain publishes the acknowledged record"
    );
    let snap = metrics.snapshot();
    assert_eq!(snap.flush_all_residue_tenants, 0);
    assert_eq!(snap.clock_lag_bypassed_at_shutdown, 1);
    assert_eq!(snap.clock_lag_refused, DRAIN_REFUSALS);
}

/// Spans: the same teardown bypass at the span shard's drain.
#[tokio::test]
async fn spans_shutdown_publishes_what_the_lag_check_refused() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = SpanIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered span write acknowledged");

    let metrics = router.metrics_handle();
    router.shutdown().await;

    assert_eq!(
        published_span_starts(store.as_ref(), &tenant).await,
        vec![1_000],
        "the teardown drain publishes the acknowledged span"
    );
    let snap = metrics.snapshot();
    assert_eq!(snap.flush_all_residue_tenants, 0);
    assert_eq!(snap.clock_lag_bypassed_at_shutdown, 1);
    assert_eq!(snap.clock_lag_refused, DRAIN_REFUSALS);
}

/// The tenant whose flush arms the shard's ADR-1307 monotonic floor at the
/// store's time before the lagging tenant buffers its row. One shard, so both
/// tenants share the floor.
const FLOOR_TENANT: &str = "floor-arm";

/// The bypass pass reads the floor the enforced passes never did, so a lag
/// refusal can hide a backwards step big enough to be refused there.
///
/// An earlier flush in this process stamped the store's time, arming the floor.
/// The clock then steps two hours back, which is both past the lag allowance
/// and past `MAX_FLUSH_CLOCK_HOLD_NS` below that floor. Every enforced pass
/// returns `LagRefused` before the floor is consulted, so nothing re-anchors;
/// the first bypass pass reaches the floor and is refused there. With a single
/// bypass pass that was the end of the drain and these acknowledged rows became
/// residue, where the same backwards step without the lag is refused once,
/// re-anchors, and publishes on the next pass. The drain now keeps making
/// bounded bypass passes, so the pass after the re-anchor stamps the raw
/// reading and publishes.
///
/// Reverting `flush_all` in shard.rs to the single
/// `self.flush_all_pass(trigger, LagCheck::BypassedAtTeardown).await;` fails
/// this on the commit-record count (left 0, right 1), with
/// `flush_all_residue_tenants` 1: the acknowledged row is lost.
#[tokio::test]
async fn shutdown_publishes_when_a_lag_refusal_hid_a_backwards_step() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS);
    let router = IngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    // Arm the floor: this flush's reading is the store's own time, so it is
    // within the allowance, publishes, and leaves the shard's floor at
    // `STORE_NS`.
    let floor_tenant = tenant(FLOOR_TENANT);
    router
        .write(
            floor_tenant.clone(),
            vec![make_point(
                &floor_tenant,
                "cpu_usage",
                &[("host", "floor")],
                500,
                2.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write acknowledged");
    router.flush_all().await;
    assert_eq!(
        commit_records(store.as_ref(), &floor_tenant, Signal::Metrics)
            .await
            .len(),
        1,
        "the arming flush published, so the shard's floor is now the store's time"
    );

    // Two hours back: beyond the lag allowance, and beyond the hold bound
    // below the floor the arming flush just set.
    clock.set_ns(STORE_NS - TWO_HOURS_NS);
    let tenant = tenant("acme");
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("a buffered write is acknowledged without waiting for its flush");

    let metrics = router.metrics_handle();
    router.shutdown().await;

    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Metrics)
            .await
            .len(),
        1,
        "the teardown drain publishes the acknowledged rows after the floor re-anchors"
    );
    assert_eq!(
        published_samples(store.as_ref(), &tenant).await,
        vec![(1_000, 1.0_f64.to_bits())],
        "and that commit's segment holds exactly the buffered row"
    );
    let snap = metrics.snapshot();
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "nothing was left buffered, so nothing is residue"
    );
    assert_eq!(
        snap.clock_lag_refused, DRAIN_REFUSALS,
        "every enforced drain pass refused on the lag, before the floor"
    );
    assert_eq!(
        snap.clock_regressions_refused, 1,
        "the first bypass pass reached the floor and was refused there once"
    );
    assert_eq!(
        snap.clock_lag_bypassed_at_shutdown, 2,
        "two bypass passes: the one the floor refused, and the one that published"
    );
}

/// Logs: the same hole, at the log shard's drain.
#[tokio::test]
async fn logs_shutdown_publishes_when_a_lag_refusal_hid_a_backwards_step() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS);
    let router = LogIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );

    let floor_tenant = tenant(FLOOR_TENANT);
    router
        .write(
            floor_tenant.clone(),
            vec![norm_log_record(500, "arming")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered log write acknowledged");
    router.flush_all().await;
    assert_eq!(
        commit_records(store.as_ref(), &floor_tenant, Signal::Logs)
            .await
            .len(),
        1,
        "the arming flush published, so the shard's floor is now the store's time"
    );

    clock.set_ns(STORE_NS - TWO_HOURS_NS);
    let tenant = tenant("acme");
    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "acked")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered log write acknowledged");

    let metrics = router.metrics_handle();
    router.shutdown().await;

    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Logs)
            .await
            .len(),
        1,
        "the teardown drain publishes the acknowledged record after the floor re-anchors"
    );
    assert_eq!(
        published_log_bodies(store.as_ref(), &tenant).await,
        vec!["acked".to_string()],
        "and that commit's segment holds exactly the buffered record"
    );
    let snap = metrics.snapshot();
    assert_eq!(snap.flush_all_residue_tenants, 0);
    assert_eq!(snap.clock_lag_refused, DRAIN_REFUSALS);
    assert_eq!(snap.clock_regressions_refused, 1);
    assert_eq!(snap.clock_lag_bypassed_at_shutdown, 2);
}

/// Spans: the same hole, at the span shard's drain.
#[tokio::test]
async fn spans_shutdown_publishes_when_a_lag_refusal_hid_a_backwards_step() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS);
    let router = SpanIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );

    let floor_tenant = tenant(FLOOR_TENANT);
    router
        .write(
            floor_tenant.clone(),
            vec![norm_span(500)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered span write acknowledged");
    router.flush_all().await;
    assert_eq!(
        commit_records(store.as_ref(), &floor_tenant, Signal::Spans)
            .await
            .len(),
        1,
        "the arming flush published, so the shard's floor is now the store's time"
    );

    clock.set_ns(STORE_NS - TWO_HOURS_NS);
    let tenant = tenant("acme");
    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered span write acknowledged");

    let metrics = router.metrics_handle();
    router.shutdown().await;

    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Spans)
            .await
            .len(),
        1,
        "the teardown drain publishes the acknowledged span after the floor re-anchors"
    );
    assert_eq!(
        published_span_starts(store.as_ref(), &tenant).await,
        vec![1_000],
        "and that commit's segment holds exactly the buffered span"
    );
    let snap = metrics.snapshot();
    assert_eq!(snap.flush_all_residue_tenants, 0);
    assert_eq!(snap.clock_lag_refused, DRAIN_REFUSALS);
    assert_eq!(snap.clock_regressions_refused, 1);
    assert_eq!(snap.clock_lag_bypassed_at_shutdown, 2);
}

/// Logs: the bypass is teardown-only there too. Extending it to the
/// `FlushNow` drain (`DrainIntent::Retryable`) in log_shard.rs fails this on
/// the commit-record assertion: the drain publishes, where its actor is still
/// running and a later trigger would have published on a converged clock.
#[tokio::test]
async fn logs_flush_now_is_still_refused_with_a_lagging_clock() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = LogIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "acked")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered log write acknowledged");

    router.flush_all().await;

    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Logs).await,
        Vec::<String>::new(),
        "FlushNow never bypasses the lag check"
    );
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_bypassed_at_shutdown, 0);
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "a Retryable drain's residue is a WARN, not a durability-defect bump"
    );
    assert_eq!(snap.clock_lag_refused, DRAIN_REFUSALS);

    // Re-buffered, not dropped: the record publishes once the clock converges.
    clock.set_ns(STORE_NS);
    router.flush_all().await;
    assert_eq!(
        published_log_bodies(store.as_ref(), &tenant).await,
        vec!["acked".to_string()]
    );
    assert_eq!(
        router.metrics().snapshot().clock_lag_bypassed_at_shutdown,
        0
    );

    router.shutdown().await;
}

/// Spans: the same, at the span shard's `FlushNow` drain. Extending the bypass
/// to `DrainIntent::Retryable` in span_shard.rs fails this the same way.
#[tokio::test]
async fn spans_flush_now_is_still_refused_with_a_lagging_clock() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = SpanIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered span write acknowledged");

    router.flush_all().await;

    assert_eq!(
        commit_records(store.as_ref(), &tenant, Signal::Spans).await,
        Vec::<String>::new(),
        "FlushNow never bypasses the lag check"
    );
    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_lag_bypassed_at_shutdown, 0);
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "a Retryable drain's residue is a WARN, not a durability-defect bump"
    );
    assert_eq!(snap.clock_lag_refused, DRAIN_REFUSALS);

    clock.set_ns(STORE_NS);
    router.flush_all().await;
    assert_eq!(
        published_span_starts(store.as_ref(), &tenant).await,
        vec![1_000]
    );
    assert_eq!(
        router.metrics().snapshot().clock_lag_bypassed_at_shutdown,
        0
    );

    router.shutdown().await;
}

/// The other teardown path: the router is dropped without `shutdown()`, which
/// production reaches whenever `Arc::try_unwrap` fails or `Running::shutdown`
/// returns early (services/ravel-server/src/lib.rs). The channel-close arm is a
/// teardown too, so it bypasses the lag check rather than drop rows buffered
/// mode already acknowledged.
///
/// Changing that arm's `DrainIntent::Teardown` to `DrainIntent::Retryable`
/// (shard.rs, the `None` arm's `flush_all` call at line 1098) fails this: no
/// bypass pass runs, nothing is published, and the commit-record assertion
/// below reports an empty store (left 0, right 1).
#[tokio::test]
async fn channel_close_publishes_what_the_lag_check_refused() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = IngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("a buffered write is acknowledged without waiting for its flush");

    // A buffered ack fires at enqueue, so wait until the actor has actually
    // buffered the point before dropping the router; otherwise the drop could
    // race the actor and prove nothing.
    let metrics = router.metrics_handle();
    for _ in 0..100 {
        if metrics.snapshot().buffered_points_total == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        metrics.snapshot().buffered_points_total,
        1,
        "the point must be buffered in the actor before the router is dropped"
    );

    drop(router);

    // The detached actor notices the closed channel, drains, and stops. Wait
    // for the commit record rather than a flat sleep: a slow host takes
    // longer instead of failing, and a host that never writes one fails on
    // the assertion below.
    let mut commits = Vec::new();
    for _ in 0..2_000 {
        commits = commit_records(store.as_ref(), &tenant, Signal::Metrics).await;
        if !commits.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert_eq!(
        commits.len(),
        1,
        "the channel-close drain publishes the acknowledged rows"
    );
    assert_eq!(
        published_samples(store.as_ref(), &tenant).await,
        vec![(1_000, 1.0_f64.to_bits())],
        "and that commit's segment holds exactly the buffered row"
    );
    let snap = metrics.snapshot();
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "nothing was left buffered, so nothing is residue"
    );
    assert_eq!(
        snap.clock_lag_bypassed_at_shutdown, 1,
        "one flush published with the check bypassed"
    );
    assert_eq!(
        snap.clock_lag_refused, DRAIN_REFUSALS,
        "every enforced pass of the channel-close drain refused first"
    );
}

/// Logs: the same channel-close teardown. `LogIngestRouter` spawns its shard
/// actors detached, so dropping the router without `shutdown()` closes their
/// mailboxes and the `None` arm drains.
///
/// Changing that arm's `DrainIntent::Teardown` to `DrainIntent::Retryable`
/// (log_shard.rs, the `None` arm's `flush_all` call at line 1144) fails this:
/// no bypass pass runs, so the poll below times out with an empty store and the
/// body assertion panics inside `sole_published_object` with "expected exactly
/// one commit record", left 0, right 1.
#[tokio::test]
async fn logs_channel_close_publishes_what_the_lag_check_refused() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = LogIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "acked")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered log write acknowledged");

    // A buffered ack fires at enqueue, so wait until the actor has actually
    // buffered the record before dropping the router; otherwise the drop could
    // race the actor and prove nothing.
    let metrics = router.metrics_handle();
    for _ in 0..100 {
        if metrics.snapshot().buffered_records_total == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        metrics.snapshot().buffered_records_total,
        1,
        "the record must be buffered in the actor before the router is dropped"
    );

    drop(router);

    // Wait for the commit record rather than a flat sleep: a slow host takes
    // longer instead of failing, and a host that never writes one fails on the
    // assertions below.
    for _ in 0..2_000 {
        if !commit_records(store.as_ref(), &tenant, Signal::Logs)
            .await
            .is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert_eq!(
        published_log_bodies(store.as_ref(), &tenant).await,
        vec!["acked".to_string()],
        "the channel-close drain publishes the acknowledged record"
    );
    let snap = metrics.snapshot();
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "nothing was left buffered, so nothing is residue"
    );
    assert_eq!(
        snap.clock_lag_bypassed_at_shutdown, 1,
        "one flush published with the check bypassed"
    );
    assert_eq!(
        snap.clock_lag_refused, DRAIN_REFUSALS,
        "every enforced pass of the channel-close drain refused first"
    );
}

/// Spans: the same channel-close teardown at the span shard's `None` arm.
///
/// Changing that arm's `DrainIntent::Teardown` to `DrainIntent::Retryable`
/// (span_shard.rs, the `None` arm's `flush_all` call at line 713) fails this:
/// no bypass pass runs, so the poll below times out with an empty store and the
/// start-timestamp assertion panics inside `sole_published_object` with
/// "expected exactly one commit record", left 0, right 1.
#[tokio::test]
async fn spans_channel_close_publishes_what_the_lag_check_refused() {
    let (_memory, store) = observed_store(Some(STORE_NS));
    let clock = TestClock::new(STORE_NS - TWO_HOURS_NS);
    let router = SpanIngestRouter::new(
        buffer_until_drain_config(),
        Arc::clone(&store),
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered span write acknowledged");

    let metrics = router.metrics_handle();
    for _ in 0..100 {
        if metrics.snapshot().buffered_spans_total == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        metrics.snapshot().buffered_spans_total,
        1,
        "the span must be buffered in the actor before the router is dropped"
    );

    drop(router);

    for _ in 0..2_000 {
        if !commit_records(store.as_ref(), &tenant, Signal::Spans)
            .await
            .is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert_eq!(
        published_span_starts(store.as_ref(), &tenant).await,
        vec![1_000],
        "the channel-close drain publishes the acknowledged span"
    );
    let snap = metrics.snapshot();
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "nothing was left buffered, so nothing is residue"
    );
    assert_eq!(
        snap.clock_lag_bypassed_at_shutdown, 1,
        "one flush published with the check bypassed"
    );
    assert_eq!(
        snap.clock_lag_refused, DRAIN_REFUSALS,
        "every enforced pass of the channel-close drain refused first"
    );
}
