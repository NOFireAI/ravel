//! The mTLS listener's query service layer authenticates with the mTLS
//! resolver (ADR-0050 section 1, ADR-1374 decision 3).
//!
//! The two listeners share one admission controller, one cost recorder, one
//! usage sink, and one engine, but never one tenant resolver: the public
//! listener derives the tenant from a bearer token and the mTLS listener from
//! the peer certificate its terminator presents. A single service instance
//! reachable from both would authenticate a certificate-identified caller
//! against the bearer-token resolver.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "public-token";
const TENANT: &str = "acme";
const MTLS_TOKEN: &str = "mtls-token";
const MTLS_TENANT: &str = "beta";

fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
    );
    headers
}

/// A query-serving process with both listeners bound, each on its own resolver
/// over a disjoint token set, so a credential that works on one is invalid on
/// the other.
async fn start_test_server() -> ravel_server::Running {
    start_test_server_over(Arc::new(MemoryStore::new())).await
}

/// [`start_test_server`] over a store the caller keeps a handle to.
async fn start_test_server_over(store: Arc<MemoryStore>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);

    let mut mtls_tokens = HashMap::new();
    mtls_tokens.insert(MTLS_TOKEN.to_string(), TenantId::new(MTLS_TENANT));
    let mtls_resolver = ravel_server::tenant::build_resolver(mtls_tokens, false);

    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: Some(ravel_server::MtlsListenerConfig {
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            resolver: mtls_resolver,
        }),
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

#[tokio::test]
async fn mtls_router_service_authenticates_with_the_mtls_resolver() {
    let running = start_test_server().await;

    let mtls_service = running
        .mtls_query_service
        .as_ref()
        .expect("an mTLS listener was configured on a query-serving mode");
    let public_service = running
        .query_service
        .as_ref()
        .expect("a query-serving mode builds the public listener's service");

    // The two instances are not the same one: each accepts its own listener's
    // credential and rejects the other's.
    assert_eq!(
        mtls_service
            .authenticate(&bearer(MTLS_TOKEN))
            .expect("the mTLS resolver knows the mTLS credential"),
        TenantId::new(MTLS_TENANT).hash(),
    );
    assert_eq!(
        public_service
            .authenticate(&bearer(TOKEN))
            .expect("the public resolver knows the public credential"),
        TenantId::new(TENANT).hash(),
    );

    let err = mtls_service
        .authenticate(&bearer(TOKEN))
        .expect_err("the public listener's credential is not an mTLS identity");
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    let err = public_service
        .authenticate(&bearer(MTLS_TOKEN))
        .expect_err("the mTLS credential is not a public-listener identity");
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);

    running.shutdown().await.expect("clean shutdown");
}

/// `describe_data` answers on both listeners' query services as `start` builds
/// them, and reads families through the metadata cache `/api/v1/metadata`
/// serves from rather than one of its own: the HTTP route fills the cache with
/// the first record, the record is then replaced in the store, and both
/// services still page the cached families inside the refresh horizon. FLIP:
/// hand either `with_describe` a fresh `MetadataCache` over the same store and
/// that service reads the replacement's `family_new`. FLIP: drop either
/// construction site's `with_describe` and that call is refused as
/// `Unsupported`.
#[cfg(feature = "sql")]
#[tokio::test]
async fn describe_data_is_served_on_both_listeners_from_the_routes_metadata_cache() {
    use ravel_catalog::{MetricKind, MetricMetadataEntry, write_metrics_meta};
    use ravel_server::service::describe::DescribeDataRequest;

    const NOW_NS: i64 = 1_700_000_000_000_000_000;
    let entry = |name: &str| MetricMetadataEntry {
        family_name: name.to_string(),
        kind: MetricKind::Counter,
        help: format!("{name} help"),
        unit: String::new(),
        updated_unix_ns: NOW_NS,
    };

    let store = Arc::new(MemoryStore::new());
    let tenant_hash = TenantId::new(TENANT).hash();
    let first = write_metrics_meta(store.as_ref(), &tenant_hash, &[entry("family_old")], None)
        .await
        .expect("write the first record");
    let running = start_test_server_over(Arc::clone(&store)).await;

    let metadata: serde_json::Value = reqwest::Client::new()
        .get(format!("http://{}/api/v1/metadata", running.http_addr))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("metadata request")
        .json()
        .await
        .expect("metadata json");
    assert!(
        metadata["data"]["family_old"].is_array(),
        "the route filled the cache with the first record: {metadata}"
    );

    write_metrics_meta(
        store.as_ref(),
        &tenant_hash,
        &[entry("family_new")],
        Some(first),
    )
    .await
    .expect("replace the record");

    let request = DescribeDataRequest {
        signal: "metrics".to_string(),
        after_name: None,
        limit: None,
        now_ns: NOW_NS,
        deadline: std::time::Duration::from_secs(30),
    };
    for (listener, service) in [
        ("public", running.query_service.as_ref()),
        ("mtls", running.mtls_query_service.as_ref()),
    ] {
        let data = service
            .expect("a query-serving mode builds both services")
            .describe_data(tenant_hash, &request)
            .await
            .unwrap_or_else(|err| panic!("{listener}: {err:?}"));
        let names: Vec<_> = data.families.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["family_old"], "{listener}");
        assert_eq!(data.families_total, 1, "{listener}");
        assert_eq!(data.families[0].help, "family_old help", "{listener}");
        assert_eq!(data.ingest_watermark_hour, None, "{listener}");
    }

    running.shutdown().await.expect("clean shutdown");
}
