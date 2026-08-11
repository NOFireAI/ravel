//! Regression test for issue #60 / finding a7-F03: the metadata endpoints
//! (`/series`, `/labels`, `/label/{name}/values`) must apply one wall
//! deadline to the whole request, not grant the full deadline afresh to each
//! `match[]` selector.
//!
//! Adapted from the `#[ignore]`d a7-F03 reproducer on branch `audit/repro-a7`
//! (`many_selectors_are_all_processed`). That reproducer only asserted that
//! all selectors are accepted (HTTP 200) against an instant in-memory store,
//! which cannot distinguish a shared budget from a per-selector one. This
//! version wraps the store so every listing costs a fixed delay, sets a short
//! client `timeout`, and asserts the request as a whole is bounded by that
//! single budget: it fails with a `504 timeout` well before N per-selector
//! budgets would elapse.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_server::{FoldTaskConfig, Mode, Running, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// Wraps an inner store and sleeps a fixed amount before every listing, so
/// each snapshot resolution (one per `match[]` selector) costs measurable
/// wall-clock time. Reads/writes are delegated unchanged; only `list` is
/// slowed, which is the operation the catalog snapshot resolver drives per
/// selector (crates/ravel-catalog `resolve` -> `list_hour_bucket`).
struct SlowListStore {
    inner: Arc<MemoryStore>,
    list_delay: Duration,
}

#[async_trait]
impl ObjectStoreBackend for SlowListStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        tokio::time::sleep(self.list_delay).await;
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits (issue #298).
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

async fn start_test_server(store: Arc<dyn ObjectStoreBackend>) -> Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// issue #60 / a7-F03: a `/series` request carrying many `match[]` selectors
/// shares one wall deadline. Each selector drives one snapshot resolution,
/// which lists a handful of catalog buckets; slowing each list by
/// `LIST_DELAY` makes one selector cost roughly `PER_SELECTOR`. The client
/// sets a `timeout` a few selectors' worth long, far below the N-selector
/// total. If the deadline were granted per selector (the defect), every
/// selector would resolve within its own fresh budget and the request would
/// return 200 after roughly `N * PER_SELECTOR`. With one shared budget the
/// request instead trips the deadline and returns `504 timeout` after
/// roughly a single `timeout`, never the N-fold total.
///
/// The status assertion (200 vs 504) is the timing-independent
/// discriminator; the elapsed bound backs the "one deadline, not N" claim.
#[tokio::test]
async fn metadata_request_shares_one_wall_deadline_across_selectors() {
    const N: usize = 12;
    // One selector lists ~4 catalog buckets; at 25 ms each that is ~100 ms of
    // work, comfortably below REQUEST_TIMEOUT so a single selector never
    // trips on its own, while N of them (>= ~1.2 s) dwarf the shared budget.
    const LIST_DELAY: Duration = Duration::from_millis(25);
    // Shared budget: a few selectors' worth, well under the N-selector total.
    const REQUEST_TIMEOUT: Duration = Duration::from_millis(300);

    let store = Arc::new(SlowListStore {
        inner: Arc::new(MemoryStore::new()),
        list_delay: LIST_DELAY,
    });
    let store_dyn: Arc<dyn ObjectStoreBackend> = store;
    let running = start_test_server(store_dyn).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let mut query: Vec<(String, String)> = Vec::new();
    for i in 0..N {
        query.push(("match[]".to_string(), format!("series_{i}")));
    }
    query.push((
        "timeout".to_string(),
        format!("{}", REQUEST_TIMEOUT.as_secs_f64()),
    ));

    let started = Instant::now();
    let response = client
        .get(format!("{base}/api/v1/series"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&query)
        .send()
        .await
        .expect("series request completes");
    let status = response.status();
    let elapsed = started.elapsed();

    // The whole request is bounded by one wall deadline: it deadline-fails
    // (504 timeout) rather than succeeding after N independent budgets.
    assert_eq!(
        status, 504,
        "request should trip the shared wall deadline (504), not grant each \
         selector its own budget; got {status}"
    );

    // And it does so within roughly one budget, not N. The tripping selector
    // returns as soon as the shared budget is spent, so a fixed request lands
    // near REQUEST_TIMEOUT plus one selector's cost; a per-selector grant
    // would instead run all N (>= ~1.2 s). Bound at 3x the budget: independent
    // of the exact per-selector list count, well above one deadline and well
    // below the N-fold total.
    let bound = REQUEST_TIMEOUT * 3;
    assert!(
        elapsed < bound,
        "request took {elapsed:?}, at or above {bound:?} (3x the shared \
         budget); the deadline is not shared across the {N} selectors"
    );

    running.shutdown().await.expect("graceful shutdown");
}
