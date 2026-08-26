//! The metadata endpoints
//! (`/series`, `/labels`, `/label/{name}/values`) must apply one wall
//! deadline to the whole request, not grant the full deadline afresh to each
//! `match[]` selector.
//!
//! A `/series` request carrying N selectors resolves them one at a time, each
//! drawing down the same request budget (`resolve_matched_series` in
//! ravel-query computes one absolute `request_deadline` and hands each
//! selector only the time still left). The test drives that budget with a
//! `FaultStore` hold gate on the `t/<th>/<sig>/del/` LIST every snapshot
//! resolution issues (ADR-0064 decision 2), so each step is an event the test
//! waits for rather than a duration it hopes for: the first `K - 1` selectors
//! are parked in the store for a fixed `HOLD` each and then released, and
//! selector `K` is parked and never released, so the shared deadline fires
//! while its resolve sits inside the store.
//!
//! Determinism (#757, #743, #731, #680): both earlier versions discriminated
//! on wall-clock bands, and on 2026-08-26 the two failed in opposite
//! directions on the same 16-core hosts, on branches that touch nothing on the
//! metadata path. `elapsed < 900 ms` (3x the 300 ms budget) failed high when
//! the lane was loaded: the deadline fired but the cancelled futures were not
//! polled promptly, so the measured wall time overran. Its replacement,
//! `resolutions < N`, failed low when the box was quiet: once #730 cut the
//! resolve's LISTs from 25 to 9, all 12 selectors fit inside the 300 ms budget
//! and nothing was cut off. GitHub's runners happen to sit between the two
//! bands, which is why CI stayed green while local runs flaked either way.
//!
//! Nothing here measures how long anything took. The hold gate supplies the
//! ordering, and the two claims are pinned by an exact count and by the budget
//! the server itself reports:
//!
//! * `initiated == K` and `completed == K - 1`: the request reached exactly
//!   the first `K` selectors' resolutions, finished `K - 1` of them, and never
//!   started selector `K + 1`. Load cannot move an exact count.
//! * the deadline named in the `504` body is the budget selector `K` was
//!   granted. Under one shared budget that is the time *remaining* after the
//!   first `K - 1` selectors burned `HOLD` each, so it is at most
//!   `REQUEST_TIMEOUT - (K - 1) * HOLD`. A per-selector budget would grant
//!   selector `K` the full `REQUEST_TIMEOUT`. `tokio::time::sleep` never
//!   returns early, so the bound holds from below by construction, and host
//!   load only makes the reported budget smaller -- it can never push the
//!   value up through the threshold.
//!
//! The only durations left are `HOLD` (budget the test deliberately spends,
//! not a duration it measures) and a 30 s sanity ceiling on each wait, which
//! exists only so a regression that hangs fails as a test failure instead of a
//! stuck lane.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_server::{FoldTaskConfig, Mode, Running, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// Counts the `t/<th>/<sig>/del/` LISTs that enter and leave the store.
/// `Catalog::resolve` issues exactly one of them per snapshot resolution
/// (ADR-0064 decision 2), and one `match[]` selector drives one resolution, so
/// `initiated` counts the selectors whose resolution reached the store and
/// `completed` counts the ones that finished. A resolution the deadline
/// cancelled while it was parked in the hold gate below is counted by
/// `initiated` and not by `completed`, which is the difference the test reads.
///
/// This wraps the [`FaultStore`] rather than the other way round so a held
/// call is counted as initiated at the moment it is held.
struct DelListCounter {
    inner: Arc<FaultStore<MemoryStore>>,
    initiated: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

impl DelListCounter {
    fn is_pending_erasure_list(prefix: &str) -> bool {
        prefix.contains("/del/")
    }
}

#[async_trait]
impl ObjectStoreBackend for DelListCounter {
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
        if !Self::is_pending_erasure_list(prefix) {
            return self.inner.list(prefix, page).await;
        }
        self.initiated.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.list(prefix, page).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        result
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
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

/// The budget the server reports in `query exceeded its deadline of {d:?}`,
/// which for a cancelled selector is the budget that selector was granted.
/// `Duration`'s `Debug` writes one number and one unit (`3s`, `1.5s`,
/// `999.8ms`, `12µs`), so the value is the digits and dot run after the
/// marker and the unit is the letters that follow it.
fn granted_deadline(body: &str) -> Duration {
    let marker = "deadline of ";
    let start = body
        .find(marker)
        .map(|at| at + marker.len())
        .unwrap_or_else(|| panic!("body should name the deadline that was exceeded, got {body}"));
    let rest = &body[start..];
    let split = rest
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or_else(|| panic!("body should give the deadline a numeric value, got {body}"));
    let (value, tail) = rest.split_at(split);
    let unit: String = tail.chars().take_while(|c| c.is_alphabetic()).collect();
    let value: f64 = value
        .parse()
        .unwrap_or_else(|e| panic!("deadline value {value} in {body} should parse: {e}"));
    let seconds = match unit.as_str() {
        "s" => value,
        "ms" => value / 1e3,
        "µs" | "us" => value / 1e6,
        "ns" => value / 1e9,
        other => panic!("unexpected deadline unit {other} in {body}"),
    };
    Duration::from_secs_f64(seconds)
}

/// A `/series` request carrying N `match[]` selectors shares one wall
/// deadline. Each selector drives one snapshot resolution, and a hold gate on
/// the `del/` LIST that resolution issues lets the test spend the budget on
/// purpose: the first `K - 1` selectors are parked for `HOLD` each and
/// released, selector `K` is parked and left there, so the shared deadline
/// fires with selector `K`'s resolve still inside the store.
///
/// Three assertions carry the claim, none of them a wall-clock measurement:
///
/// * `504 timeout` is the discriminator that the request died on the deadline
///   rather than answering.
/// * `initiated == K` with `completed == K - 1`: exactly the prefix before the
///   held selector resolved, the held selector's resolution reached the store
///   and never finished, and no selector after it ever started. An exact count
///   cannot drift with host speed.
/// * the budget named in the body is at most `REQUEST_TIMEOUT - (K - 1) *
///   HOLD`. That is the *shared* claim: selector `K` was granted only what the
///   earlier selectors left, not a fresh full deadline. Rebuilding the
///   deadline per selector reports the full `REQUEST_TIMEOUT` here.
#[tokio::test]
async fn metadata_request_shares_one_wall_deadline_across_selectors() {
    const N: usize = 12;
    // The selector whose resolution is held open until after the response.
    // The first K - 1 selectors resolve; nothing past K ever starts.
    const K: usize = 3;
    // Budget each of the first K - 1 selectors spends before being released.
    // Well under REQUEST_TIMEOUT so those selectors resolve rather than trip
    // the deadline themselves, and `sleep` never returns early, so the
    // remaining budget at selector K is at most REQUEST_TIMEOUT - 2 * HOLD.
    const HOLD: Duration = Duration::from_millis(500);
    // The one shared budget for the whole request.
    const REQUEST_TIMEOUT: Duration = Duration::from_millis(2_500);
    // Not a bound on anything the test asserts: it only turns a hang into a
    // failure instead of a stuck lane.
    const SANITY_CEILING: Duration = Duration::from_secs(30);

    let faulty = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    // Hold every `del/` LIST. The request drives them one at a time (one per
    // selector, in selector order), so the test releases them one at a time.
    let gate = faulty.hold(Op::List, Some("/del/".to_string()), Occurrence::Always);
    let initiated = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(DelListCounter {
        inner: Arc::clone(&faulty),
        initiated: Arc::clone(&initiated),
        completed: Arc::clone(&completed),
    });
    let running = start_test_server(store).await;
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

    // In flight while the test drives the gate, so the holds below park the
    // request rather than the test.
    let request = tokio::spawn(async move {
        let response = client
            .get(format!("{base}/api/v1/series"))
            .header("authorization", format!("Bearer {TOKEN}"))
            .query(&query)
            .send()
            .await
            .expect("series request completes");
        let status = response.status();
        let body = response.text().await.expect("series response body");
        (status, body)
    });

    // Spend the shared budget: each of the first K - 1 selectors is parked in
    // the store for HOLD and then released, so it resolves and the request
    // moves on with that much less budget left.
    for selector in 1..K {
        tokio::time::timeout(SANITY_CEILING, gate.wait_until_held(1))
            .await
            .unwrap_or_else(|_| panic!("selector {selector}'s del/ LIST should reach the store"));
        let held = gate.held();
        assert_eq!(
            held.len(),
            1,
            "selectors resolve one at a time, so exactly one del/ LIST is held \
             at selector {selector}"
        );
        assert_eq!(
            initiated.load(Ordering::SeqCst),
            selector,
            "one del/ LIST per selector resolution (ADR-0064 decision 2)"
        );
        tokio::time::sleep(HOLD).await;
        assert!(
            gate.release(held[0]),
            "selector {selector}'s del/ LIST should still be held when released"
        );
    }

    // Selector K parks in the store and stays there: the shared deadline has
    // to fire on it.
    tokio::time::timeout(SANITY_CEILING, gate.wait_until_held(1))
        .await
        .expect("selector K's del/ LIST should reach the store");
    let held = gate.held();
    assert_eq!(held.len(), 1, "exactly selector {K}'s del/ LIST is held");

    let (status, body) = tokio::time::timeout(SANITY_CEILING, request)
        .await
        .expect("the shared deadline should end the request while selector K is held")
        .expect("request task joins");

    // The whole request is bounded by one wall deadline: it deadline-fails
    // (504 timeout) rather than answering after N independent budgets.
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

    // The request died with selector K's resolution still parked in the store:
    // the deadline cut that resolution off, it did not finish and hand back.
    assert_eq!(
        gate.held_count(),
        1,
        "selector {K}'s del/ LIST must still be held when the 504 arrives"
    );
    // Exactly the prefix before the held selector resolved, and no selector
    // after K ever started. A per-selector budget would have waited out
    // selector K's own fresh budget and gone on resolving.
    assert_eq!(
        initiated.load(Ordering::SeqCst),
        K,
        "the request must reach exactly {K} selector resolutions: the {} that \
         resolved plus the held one, and none of the {} after it",
        K - 1,
        N - K
    );
    assert_eq!(
        completed.load(Ordering::SeqCst),
        K - 1,
        "exactly the {} selectors before the held one may finish resolving",
        K - 1
    );

    // The budget selector K was granted is what the earlier selectors left of
    // the shared one, never a fresh full deadline. Each of the K - 1 releases
    // above waited a full HOLD (`sleep` does not return early), so the ceiling
    // holds by construction; host load only pushes the reported budget further
    // below it.
    let granted = granted_deadline(&body);
    let ceiling = REQUEST_TIMEOUT - HOLD * (K as u32 - 1);
    assert!(
        granted <= ceiling,
        "selector {K} must be granted only the budget left over from the \
         shared {REQUEST_TIMEOUT:?} (at most {ceiling:?} after {} selectors \
         spent {HOLD:?} each), but the server reported {granted:?}: that is \
         the full request budget granted afresh per selector",
        K - 1
    );

    // Releasing the held call now proves it was still held (the request ended
    // on the deadline rather than after this resolution came back), and
    // nothing resumes: the deadline dropped that resolve's future, so the
    // release finds no receiver. Nothing may resolve after the response
    // either.
    assert!(
        gate.release(held[0]),
        "selector {K}'s del/ LIST was still held when the 504 arrived"
    );
    tokio::time::sleep(HOLD).await;
    assert_eq!(
        initiated.load(Ordering::SeqCst),
        K,
        "no selector may start resolving after the request has answered"
    );
    assert_eq!(
        completed.load(Ordering::SeqCst),
        K - 1,
        "the cancelled resolution must not finish after the request has answered"
    );

    running.shutdown().await.expect("graceful shutdown");
}
