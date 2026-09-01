//! Per-tenant PUT attribution wired through the live shard actors (ADR-0076
//! decision 2). Proves the attribution structure is fed at flush completion
//! across all three signals, keyed by the flushing tenant, with the ADR's
//! 2-PUTs-per-flush cost model. The eviction/ordering correctness of the
//! structure itself is unit-tested in `src/attribution.rs`; these tests cover
//! the end-to-end wiring the unit tests cannot.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{
    IngestConfig, IngestRouter, LogIngestRouter, PUTS_PER_FLUSH, SpanIngestRouter, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::Signal;
use ravel_types::logstream::{AttrValue, log_stream_id};

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

/// A completed metrics flush attributes exactly [`PUTS_PER_FLUSH`] PUTs to the
/// flushing tenant, and a tenant that never flushed has no entry.
#[tokio::test]
async fn metrics_flush_attributes_puts_to_the_tenant() {
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

    let acme = tenant("acme");
    let points = vec![make_point(&acme, "cpu_usage", &[("host", "a")], 1_000, 1.0)];
    router
        .write(
            acme.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("size-triggered flush lands");

    // The object store actually holds the data object and the commit record:
    // the two PUTs the attribution charges are real.
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(objects.iter().any(|o| o.key.contains("/l0/")));
    assert!(objects.iter().any(|o| o.key.contains("/c/")));

    let top = router.metrics().tenant_put_attribution().top_n(10);
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].tenant, acme.hash());
    assert_eq!(top[0].puts, PUTS_PER_FLUSH);
    assert_eq!(top[0].error, 0);

    // A tenant that never flushed is absent, not present with zero.
    let other = tenant("never");
    assert!(top.iter().all(|t| t.tenant != other.hash()));

    router.shutdown().await;
}

/// Two flushes on the same tenant sum to two flushes' worth of PUTs.
#[tokio::test]
async fn repeated_metrics_flushes_sum_for_one_tenant() {
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
    let acme = tenant("acme");

    for i in 0..3 {
        let points = vec![make_point(
            &acme,
            "cpu_usage",
            &[("host", "a")],
            1_000 + i,
            1.0,
        )];
        router
            .write(
                acme.clone(),
                points,
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
            .expect("flush lands");
    }

    let top = router.metrics().tenant_put_attribution().top_n(10);
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].tenant, acme.hash());
    assert_eq!(top[0].puts, 3 * PUTS_PER_FLUSH);

    router.shutdown().await;
}

/// Distinct tenants on the same signal are attributed independently, and
/// `top_n` orders them by PUT volume.
#[tokio::test]
async fn distinct_tenants_are_attributed_independently() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 4,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let heavy = tenant("heavy");
    let light = tenant("light");
    for i in 0..3 {
        let points = vec![make_point(
            &heavy,
            "cpu_usage",
            &[("host", "a")],
            1_000 + i,
            1.0,
        )];
        router
            .write(
                heavy.clone(),
                points,
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
            .expect("flush lands");
    }
    let points = vec![make_point(
        &light,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];
    router
        .write(
            light.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("flush lands");

    let top = router.metrics().tenant_put_attribution().top_n(10);
    assert_eq!(top.len(), 2);
    assert_eq!(top[0].tenant, heavy.hash());
    assert_eq!(top[0].puts, 3 * PUTS_PER_FLUSH);
    assert_eq!(top[1].tenant, light.hash());
    assert_eq!(top[1].puts, PUTS_PER_FLUSH);

    router.shutdown().await;
}

/// The log pipeline feeds its own attribution at flush completion.
#[tokio::test]
async fn log_flush_attributes_puts_to_the_tenant() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    };
    let router = LogIngestRouter::new(config, Arc::clone(&store), clock.clone());
    let acme = tenant("acme");
    let records = vec![norm_log_record(1_000), norm_log_record(2_000)];
    router
        .write(
            acme.clone(),
            records,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("log flush lands");

    let top = router.metrics().tenant_put_attribution().top_n(10);
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].tenant, acme.hash());
    assert_eq!(top[0].puts, PUTS_PER_FLUSH);

    router.shutdown().await;
}

/// The span pipeline feeds its own attribution at flush completion.
#[tokio::test]
async fn span_flush_attributes_puts_to_the_tenant() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    };
    let router = SpanIngestRouter::new(config, Arc::clone(&store), clock.clone());
    let acme = tenant("acme");
    let spans = vec![norm_span(1_000), norm_span(2_000)];
    router
        .write(
            acme.clone(),
            spans,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("span flush lands");

    let top = router.metrics().tenant_put_attribution().top_n(10);
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].tenant, acme.hash());
    assert_eq!(top[0].puts, PUTS_PER_FLUSH);

    router.shutdown().await;
}
