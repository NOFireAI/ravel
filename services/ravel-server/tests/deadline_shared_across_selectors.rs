//! The metadata endpoints
//! (`/series`, `/labels`, `/label/{name}/values`) must apply one wall
//! deadline to the whole request, not grant the full deadline afresh to each
//! `match[]` selector.
//!
//! The store is wrapped so every listing costs a fixed delay. A `/series`
//! request carrying N selectors resolves them one at a time, each drawing down
//! the same request budget (`resolve_matched_series` in ravel-query computes
//! one absolute `request_deadline` and hands each selector only the time still
//! left). With N selectors' worth of listing far exceeding a short client
//! `timeout`, the request trips the shared deadline and returns `504 timeout`
//! after only a prefix of the selectors have resolved.
//!
//! Determinism (#706, #680, #731): the earlier version asserted `elapsed <
//! 900 ms` (3x the 300 ms budget) to back the "one deadline, not N" claim.
//! That is a wall-clock race against the scheduler: under host load the
//! deadline still fires but the cancelled futures are not polled promptly, so
//! the measured wall time overran 900 ms and the test failed (observed at
//! 964 ms, twice, in the flight-sql lane). The change under gate cannot reach
//! that assertion; it is a pure timing flake.
//!
//! The fix keeps the load-robust `504` discriminator and replaces the wall
//! bound with a count. `Catalog::resolve` issues exactly one
//! `t/<th>/<sig>/del/` LIST per snapshot resolution (ADR-0064 decision 2), so
//! counting those LISTs counts the selectors that ran a full resolution. Under
//! one shared budget the deadline cuts the request off after only a prefix of
//! the N selectors resolve, so that count is strictly below N; a per-selector
//! budget would resolve all N. Host load only makes fewer selectors fit the
//! budget, never more, so `resolutions < N` is an upper bound that holds
//! regardless of how loaded the box is -- the deterministic replacement for
//! the old timing assertion. Mirrors the "observe the shared bound, don't race
//! it" fix #717 applied to the query-engine deadline tests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
/// wall-clock time and N of them dwarf a short request budget. Reads/writes
/// are delegated unchanged; only `list` is slowed, which is the operation the
/// catalog snapshot resolver drives per selector (crates/ravel-catalog
/// `resolve` -> `list_hour_bucket`).
///
/// `resolutions` counts the `del/` LISTs. `Catalog::resolve` issues exactly one
/// `t/<th>/<sig>/del/` LIST per snapshot resolution (ADR-0064 decision 2), so
/// this counter is the number of selectors that ran a full resolution before
/// the request ended -- the deterministic signal the test asserts on.
struct SlowListStore {
    inner: Arc<MemoryStore>,
    list_delay: Duration,
    resolutions: Arc<AtomicUsize>,
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
        // One `del/` LIST marks one completed snapshot resolution (ADR-0064
        // decision 2). Count before the delay so a resolution that reaches the
        // del LIST is counted even if the deadline fires during the sleep.
        if prefix.contains("/del/") {
            self.resolutions.fetch_add(1, Ordering::SeqCst);
        }
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
        // double inherits.
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
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        cache_dir: None,
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
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// A `/series` request carrying N `match[]` selectors shares one wall deadline.
/// Each selector drives one snapshot resolution, which lists a handful of
/// catalog buckets; slowing each list by `LIST_DELAY` makes N selectors' worth
/// of listing far exceed the short client `timeout`. The request trips the
/// shared deadline and returns `504 timeout`.
///
/// Two assertions carry the claim, neither of them a wall-clock measurement:
///
/// * `504` is the timing-independent discriminator. N selectors need
///   `>= ~N * PER_SELECTOR` of real listing, far above the budget, so the
///   deadline fires regardless of host speed. A per-selector budget would let
///   every selector resolve inside its own fresh budget and return `200`.
/// * `resolutions < N` shows the budget is *shared*: the deadline cut the
///   request off after only a prefix of the N selectors ran a full resolution
///   (one `del/` LIST each, ADR-0064 decision 2). A per-selector budget would
///   resolve all N (`resolutions == N`). Load only lowers this count, so the
///   `< N` bound never flakes upward -- it replaces the old `elapsed < 3x`
///   bound that did.
#[tokio::test]
async fn metadata_request_shares_one_wall_deadline_across_selectors() {
    const N: usize = 12;
    // One selector lists a handful of catalog buckets plus its `del/` LIST; at
    // 25 ms each that is tens of ms of work, well below REQUEST_TIMEOUT so a
    // single selector never trips on its own, while N of them dwarf the shared
    // budget.
    const LIST_DELAY: Duration = Duration::from_millis(25);
    // Shared budget: a few selectors' worth, well under the N-selector total.
    const REQUEST_TIMEOUT: Duration = Duration::from_millis(300);

    let resolutions = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(SlowListStore {
        inner: Arc::new(MemoryStore::new()),
        list_delay: LIST_DELAY,
        resolutions: Arc::clone(&resolutions),
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

    let response = client
        .get(format!("{base}/api/v1/series"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&query)
        .send()
        .await
        .expect("series request completes");
    let status = response.status();
    let body = response.text().await.expect("series response body");

    // The whole request is bounded by one wall deadline: it deadline-fails
    // (504 timeout) rather than succeeding after N independent budgets.
    assert_eq!(
        status, 504,
        "request should trip the shared wall deadline (504), not grant each \
         selector its own budget; got {status} with body {body}"
    );
    // ...and it is the typed deadline error, not some other 504. The envelope
    // tags the class `timeout` and the message names the deadline.
    assert!(
        body.contains("\"errorType\":\"timeout\""),
        "expected the typed timeout error envelope, got {body}"
    );
    assert!(
        body.contains("deadline"),
        "expected the deadline error message, got {body}"
    );

    // One shared budget, not N: the deadline cut the request off after only a
    // prefix of the N selectors ran a full snapshot resolution. A per-selector
    // budget would have resolved all N (resolutions == N). Host load can only
    // lower this count (fewer selectors fit the budget), so the bound holds
    // regardless of how loaded the box is.
    let resolved = resolutions.load(Ordering::SeqCst);
    assert!(
        resolved < N,
        "the {N} selectors must share one wall budget: only a prefix of them \
         should resolve before the shared deadline fires, but {resolved} of \
         {N} resolved (a per-selector budget would resolve all {N})"
    );

    running.shutdown().await.expect("graceful shutdown");
}
