//! Crash-matrix row 3 (ack lost after commit) and duplicate OTLP delivery
//! (docs/consistency-model.md "Duplicates and idempotency"; ADR-0010 SS5).
//! At-least-once delivery means both copies are stored; PromQL dedups to a
//! single value per (series, ts) using the documented total order.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data as MetricData,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::{IngestLimits, normalize_metrics};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::Signal;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

/// Crash-matrix row 3: the commit lands and is durable, but the ack never
/// reaches the client. The client, having no ack, retries the identical
/// payload. Both copies are stored under separate commits; the query layer
/// still returns exactly one value at that timestamp.
#[tokio::test]
async fn ack_lost_after_commit_then_client_retry_dedups_to_one_value() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let event_ts = BASE_NS - NS_PER_MIN;
    let build_points = || {
        vec![make_point(
            &tid,
            "requests_total",
            &[("route", "checkout")],
            event_ts,
            42.0,
        )]
    };

    let first_receipt = router
        .write(
            tid.clone(),
            build_points(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first write succeeds and commits durably");
    // The client never observes `first_receipt`'s ack (simulated ack loss)
    // and retries the exact same payload.
    let retry_receipt = router
        .write(
            tid.clone(),
            build_points(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("retried write also succeeds");
    assert_ne!(
        first_receipt.tokens, retry_receipt.tokens,
        "a client-level retry is a brand new flush with its own commit, not deduped at ingest"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let commit_count = objects.iter().filter(|o| o.key.contains("/c/")).count();
    let data_count = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    assert_eq!(
        commit_count, 2,
        "both the original and the retried commit must be durable"
    );
    assert_eq!(
        data_count, 2,
        "both copies of the data are stored, as at-least-once delivery promises"
    );

    router.shutdown().await;

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    let result = engine
        .instant(
            tid.hash(),
            "requests_total",
            event_ts / 1_000_000,
            &[],
            clock.now(),
            Duration::from_secs(5),
        )
        .await
        .expect("query");
    assert_eq!(
        result.len(),
        1,
        "duplicate samples must not double-count as two series values"
    );
    assert_eq!(result[0].value, 42.0);
}

fn any_value_str(v: &str) -> AnyValue {
    AnyValue {
        value: Some(AnyValueVariant::StringValue(v.to_string())),
    }
}

fn number_point(ts_ns: i64, value: f64) -> NumberDataPoint {
    NumberDataPoint {
        attributes: vec![],
        time_unix_nano: ts_ns as u64,
        value: Some(NumberValue::AsDouble(value)),
        ..Default::default()
    }
}

fn gauge_request(metric_name: &str, ts_ns: i64, value: f64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "service.name".to_string(),
                    value: Some(any_value_str("checkout")),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![number_point(ts_ns, value)],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Same OTLP batch normalized twice (once per flush that re-received it,
/// e.g. a collector retrying an unacked export), rather than a client-level
/// retry of the ingest API. The two normalization passes are independent
/// (fresh series/points each time, as a second delivery attempt would
/// produce), landing in two separate flushes; the queryable value must
/// still be correct and not double-counted at the single (series, ts) the
/// evaluator reports.
#[tokio::test]
async fn duplicate_otlp_delivery_normalized_twice_does_not_double_count() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let event_ts = BASE_NS - NS_PER_MIN;
    let limits = IngestLimits::default();
    let req = gauge_request("otlp_gauge", event_ts, 7.0);

    let first = normalize_metrics(&tid, req.clone(), &limits, BASE_NS);
    assert!(first.rejected.is_empty());
    let second = normalize_metrics(&tid, req, &limits, BASE_NS);
    assert!(second.rejected.is_empty());

    router
        .write(
            tid.clone(),
            first.points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first flush");
    router
        .write(
            tid.clone(),
            second.points,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("duplicate flush");
    router.shutdown().await;

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    let result = engine
        .instant(
            tid.hash(),
            "otlp_gauge",
            event_ts / 1_000_000,
            &[],
            clock.now(),
            Duration::from_secs(5),
        )
        .await
        .expect("query");
    assert_eq!(result.len(), 1, "one series, not two, despite two ingests");
    assert_eq!(result[0].value, 7.0);
}
