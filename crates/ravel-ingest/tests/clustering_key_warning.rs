//! An unresolved clustering key warns once per overlay refresh, not once per
//! flush (ADR-2135, issue #2142). The only test in its binary: a thread-local
//! capture subscriber misses events when other tests in the same process hit
//! the callsite concurrently.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use ravel_catalog::config_key;
use ravel_ingest::{IngestConfig, LogIngestRouter, TenantCount, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_proto::sys::v1::{
    ClusteringBucketWidth, ClusteringKeyConfig, TenantConfigRecord, TenantLifecycleState,
    TypedAttrColumn, TypedAttrColumnConfig, TypedAttrColumnType,
};
use ravel_types::TenantId;
use ravel_types::logstream::{AttrValue, log_stream_id};
use tracing_subscriber::layer::SubscriberExt;

mod common;
use common::TestClock;

const BASE_NS: i64 = 1_700_000_000_000_000_000;
/// Past the overlay's 60 s refresh horizon.
const PAST_HORIZON_NS: i64 = 61_000_000_000;

/// Collects the message of every WARN event.
#[derive(Clone, Default)]
struct WarnCapture {
    messages: Arc<Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        self.messages.lock().expect("lock").push(visitor.0);
    }
}

#[derive(Default)]
struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl WarnCapture {
    fn clustering_warnings(&self) -> usize {
        self.messages
            .lock()
            .expect("lock")
            .iter()
            .filter(|m| m.contains("clustering key or bloom scope does not resolve"))
            .count()
    }
}

fn record(ts_ns: i64) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &[]),
        stream_attrs: ravel_logseg::stream_attrs_bytes(&res, "scope", "", &[]),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "row".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("region".to_string(), AttrValue::Str("west".to_string()))],
    }
}

#[tokio::test]
async fn an_unresolved_key_warns_once_per_refresh() {
    let capture = WarnCapture::default();
    let _default =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));

    let tenant = TenantId::new("clustered");
    // `missing` is not a declared typed column.
    let config = TenantConfigRecord {
        format_version: 3,
        tenant_hash: tenant.hash().0.to_vec(),
        lifecycle_state: TenantLifecycleState::Active as i32,
        typed_attr_columns: Some(TypedAttrColumnConfig {
            columns: vec![TypedAttrColumn {
                key: "region".to_string(),
                r#type: TypedAttrColumnType::Str as i32,
            }],
        }),
        clustering_key: Some(ClusteringKeyConfig {
            columns: vec!["missing".to_string()],
            bucket_width: ClusteringBucketWidth::OneHour as i32,
            generation: 4,
        }),
        created_unix_ns: 1,
        updated_unix_ns: 1,
        ..Default::default()
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    store
        .put(
            &config_key(&tenant.hash()),
            config.encode_to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("put config record");
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(
        IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(20),
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        clock.clone(),
    );
    let write = || async {
        router
            .write(
                tenant.clone(),
                vec![record(BASE_NS)],
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
            .expect("strict write flushes");
    };

    write().await;
    write().await;
    assert_eq!(capture.clustering_warnings(), 1, "two flushes, one refresh");

    clock.advance_ns(PAST_HORIZON_NS);
    write().await;
    assert_eq!(
        capture.clustering_warnings(),
        2,
        "a second refresh warns again"
    );
    assert_eq!(
        router.metrics().clustering_key_unresolved_by_tenant(),
        vec![TenantCount {
            tenant: tenant.hash(),
            count: 3,
            error: 0,
        }]
    );
    router.shutdown().await;
}
