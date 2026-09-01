//! Size-triggered and age-triggered adaptive flush (docs/ingest.md "Shard
//! actor").
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
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
use ravel_types::Signal;
use ravel_types::logstream::{AttrValue, log_stream_id};

#[tokio::test]
async fn size_triggered_flush_lands_without_waiting_on_age() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

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
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("write flushes on size before the deadline");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(objects.iter().any(|o| o.key.contains("/l0/")));
    assert!(objects.iter().any(|o| o.key.contains("/c/")));

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.flushes_by_size, 1);
    assert_eq!(snapshot.flushes_by_age, 0);

    router.shutdown().await;
}

#[tokio::test]
async fn age_triggered_flush_lands_below_size_threshold() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    // The shard actor's flush tick now runs on the injected `TestClock`,
    // so the two clocks that used to be raced with a real
    // `tokio::time::sleep` are one. Wait cooperatively until the point is
    // actually buffered in the actor, then advance the injected clock past
    // `max_flush_delay`; that advance deterministically wakes the tick and
    // fires the age flush, with no wall-clock sleep.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        async {
            while router.metrics().snapshot().buffered_points_total < 1 {
                tokio::task::yield_now().await;
            }
            clock.advance_ns(100_000_000);
        },
    );
    let receipt = write_result.expect("write flushes on age before the deadline");
    assert_eq!(receipt.tokens.len(), 1);

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.flushes_by_size, 0);
    assert_eq!(snapshot.flushes_by_age, 1);

    router.shutdown().await;
}

fn norm_log_record(ts_ns: i64) -> NormalizedLogRecord {
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
        body: "boot".to_string(),
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

/// ADR-0051 section 7: a non-positive flush clock reading can never
/// be a valid hour bucket. The old `unwrap_or(0)` silently mapped it to
/// bucket 0 instead of surfacing the problem; each shard actor must now fail
/// the flush with a typed `SegmentBuild` error, ack every waiter with it, and
/// write no object. Covers all three signals, since each shard actor
/// (`shard.rs`, `log_shard.rs`, `span_shard.rs`) carries its own copy of the
/// hour-bucket computation.
#[tokio::test]
async fn nonpositive_flush_clock_fails_loud() {
    // Metrics: target_bytes: 1 forces a size-triggered flush on the very
    // first point, so flush_open_ns is read while the clock is still 0.
    {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(0);
        let config = IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(20),
            ..IngestConfig::default()
        };
        let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());
        let tenant = tenant("acme");
        let points = vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", "a")],
            1_000,
            1.0,
        )];

        let err = router
            .write(tenant, points, WriteMode::Strict, Duration::from_secs(5))
            .await
            .expect_err("a nonpositive flush clock must fail loud, not fall back to bucket 0");
        assert!(
            matches!(err, WriteError::SegmentBuild(_)),
            "expected SegmentBuild, got {err:?}"
        );

        let snapshot = router.metrics().snapshot();
        assert_eq!(snapshot.abandoned_input_rejected, 1);
        assert_eq!(snapshot.acks_err, 1);
        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            objects.is_empty(),
            "no object may be written when the hour bucket check fails"
        );

        router.shutdown().await;
    }

    // Logs.
    {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(0);
        let config = IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(20),
            ..IngestConfig::default()
        };
        let router = LogIngestRouter::new(config, Arc::clone(&store), clock.clone());
        let tenant = tenant("acme");
        let records = vec![norm_log_record(1_000)];

        let err = router
            .write(tenant, records, WriteMode::Strict, Duration::from_secs(5))
            .await
            .expect_err("a nonpositive flush clock must fail loud, not fall back to bucket 0");
        assert!(
            matches!(err, LogWriteError::SegmentBuild(_)),
            "expected SegmentBuild, got {err:?}"
        );

        let snapshot = router.metrics().snapshot();
        assert_eq!(snapshot.abandoned_input_rejected, 1);
        assert_eq!(snapshot.acks_err, 1);
        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            objects.is_empty(),
            "no object may be written when the hour bucket check fails"
        );

        router.shutdown().await;
    }

    // Spans.
    {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(0);
        let config = IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(20),
            ..IngestConfig::default()
        };
        let router = SpanIngestRouter::new(config, Arc::clone(&store), clock.clone());
        let tenant = tenant("acme");
        let spans = vec![norm_span(1_000)];

        let err = router
            .write(tenant, spans, WriteMode::Strict, Duration::from_secs(5))
            .await
            .expect_err("a nonpositive flush clock must fail loud, not fall back to bucket 0");
        assert!(
            matches!(err, SpanWriteError::SegmentBuild(_)),
            "expected SegmentBuild, got {err:?}"
        );

        let snapshot = router.metrics().snapshot();
        assert_eq!(snapshot.abandoned_input_rejected, 1);
        assert_eq!(snapshot.acks_err, 1);
        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            objects.is_empty(),
            "no object may be written when the hour bucket check fails"
        );

        router.shutdown().await;
    }
}
