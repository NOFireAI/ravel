//! The metadata endpoints
//! (`/series`, `/labels`, `/label/{name}/values`) must apply one wall
//! deadline to the whole request, not grant the full deadline afresh to each
//! `match[]` selector.
//!
//! A `/series` request carrying N selectors resolves them one at a time, each
//! drawing down the same request budget (`resolve_matched_series` in
//! ravel-query computes one absolute `request_deadline` and hands each
//! selector only the time still left). The store is a `FaultStore` with a hold
//! gate armed on the `t/<th>/<sig>/del/` LIST that the K-th selector's
//! resolution issues (one per resolve, ADR-0064 decision 2). The K-th selector
//! parks inside the store and the request cannot progress past it, so the
//! shared deadline fires while it is still held: the request returns `504
//! timeout` with exactly K - 1 selectors resolved, and the deadline the error
//! reports is the budget that was left after those K - 1, not the configured
//! one.
//!
//! Determinism (#757, #743, #731, #680): every earlier version of this test
//! discriminated on wall-clock bands, and the two that survived failed in
//! opposite directions on the same 16-core hosts, on branches that touch
//! nothing on the metadata path. `elapsed < 900 ms` (3x the 300 ms budget)
//! failed under load at 964 ms, because a fired deadline does not mean the
//! cancelled futures are polled promptly. `resolutions < N` then failed on a
//! quiet, fast host: once #730 cut a resolve from 25 LISTs to 9, all 12
//! selectors fit inside the 300 ms budget, so nothing was cut off and the
//! prefix the assertion described never formed. Both bands are gone. What
//! stops the request now is an event the test holds open, not a race the test
//! hopes to win: the held selector never finishes at any host speed, so the
//! prefix is exactly K - 1 rather than "some number below N", and the only
//! duration left in the file is a 30 s ceiling that distinguishes a hang from
//! a failure. Mirrors the "observe the shared bound, don't race it" fix #717
//! applied to the query-engine deadline tests.

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

/// Wraps an inner store, counting and slowing the `t/<th>/<sig>/del/` LIST
/// that `Catalog::resolve` issues exactly once per snapshot resolution
/// (ADR-0064 decision 2). One `del/` LIST is therefore one selector's
/// completed resolution: `resolutions` is the number of selectors that
/// finished resolving, counted after the dwell and before the call reaches the
/// backend, so a resolution cut off part way is not counted as one.
///
/// `dwell` is what each resolved selector spends out of the shared request
/// budget. It is not what stops the request (the hold gate is), so nothing
/// here is a race: the request must survive `dwell` on each of the K - 1
/// selectors before the held one, which it does with the budget over three
/// times their total. Overshoot under load only takes more of the shared
/// budget, which is the direction the assertions already allow for.
struct CatalogResolveProbe {
    inner: MemoryStore,
    dwell: Duration,
    resolutions: Arc<AtomicUsize>,
}

#[async_trait]
impl ObjectStoreBackend for CatalogResolveProbe {
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
        if prefix.contains("/del/") {
            tokio::time::sleep(self.dwell).await;
            self.resolutions.fetch_add(1, Ordering::SeqCst);
        }
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

/// The `Duration` the timeout envelope reports, parsed back out of
/// `QueryError::DeadlineExceeded`'s message ("query exceeded its deadline of
/// 2.199843211s", `Duration`'s own `Debug` rendering). This is the budget the
/// engine was handed for the selector that tripped the deadline, and it is the
/// only place the size of that budget is visible from outside the process.
fn reported_deadline(body: &str) -> Duration {
    let tail = body
        .split("deadline of ")
        .nth(1)
        .unwrap_or_else(|| panic!("expected the deadline error message, got {body}"));
    let split = tail
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or_else(|| panic!("expected a duration after \"deadline of \", got {tail}"));
    let (value, unit) = tail.split_at(split);
    let value: f64 = value
        .parse()
        .unwrap_or_else(|e| panic!("expected a numeric duration, got {value}: {e}"));
    let seconds = if unit.starts_with("ns") {
        value / 1e9
    } else if unit.starts_with("\u{b5}s") {
        value / 1e6
    } else if unit.starts_with("ms") {
        value / 1e3
    } else if unit.starts_with('s') {
        value
    } else {
        panic!("unrecognized duration unit in {tail}")
    };
    Duration::from_secs_f64(seconds)
}

/// A `/series` request carrying N `match[]` selectors shares one wall deadline.
/// The store holds the K-th selector's catalog `del/` LIST open, so the
/// request is parked inside selector K when the shared deadline fires, and
/// three assertions carry the claim, none of them a wall-clock band:
///
/// * `504 timeout` arrives while the hold is still held. A request whose only
///   in-flight work is parked forever can only end on a deadline, at any host
///   speed.
/// * Exactly `K - 1` selectors resolved. The selectors before the held one
///   completed, the held one never reached the backend, and no selector after
///   it started, because the loop cannot get past the held one. This is an
///   exact count, not a bound: nothing about it moves when the host is fast,
///   slow, or loaded.
/// * The deadline the error reports is at most the budget left after the
///   K - 1 dwells, so the selector that tripped it was handed the *remaining*
///   budget. A per-selector budget reports the configured deadline instead,
///   because it never subtracts what earlier selectors spent.
#[tokio::test]
async fn metadata_request_shares_one_wall_deadline_across_selectors() {
    const SELECTORS: usize = 12;
    // Held selector (1-indexed): far enough in that a prefix exists before it,
    // near enough that the dwells before it stay a small share of the budget.
    const HELD_SELECTOR: usize = 3;
    // What each resolved selector takes out of the shared budget.
    const DWELL: Duration = Duration::from_millis(400);
    // The whole request's budget, over three times the two dwells that precede
    // the held selector.
    const SHARED_BUDGET: Duration = Duration::from_secs(3);
    // Not a bound on anything the test measures: a ceiling that turns a hang
    // (a deadline that never fires, a response that never arrives) into a
    // named failure instead of a stuck run.
    const HANG_CEILING: Duration = Duration::from_secs(30);

    let resolutions = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(FaultStore::new(
        CatalogResolveProbe {
            inner: MemoryStore::new(),
            dwell: DWELL,
            resolutions: Arc::clone(&resolutions),
        },
        FaultPlan::empty(),
    ));
    // Park the K-th `del/` LIST, and only that one: the selectors before it
    // resolve normally, and the ones after it never issue theirs.
    let gate = store.hold(
        Op::List,
        Some("/del/".to_string()),
        Occurrence::Nth(HELD_SELECTOR as u64),
    );
    let store_dyn: Arc<dyn ObjectStoreBackend> = store;
    let running = start_test_server(store_dyn).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let mut query: Vec<(String, String)> = Vec::new();
    for i in 0..SELECTORS {
        query.push(("match[]".to_string(), format!("series_{i}")));
    }
    query.push((
        "timeout".to_string(),
        format!("{}", SHARED_BUDGET.as_secs_f64()),
    ));

    let mut request = tokio::spawn(async move {
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

    // The request reaches selector K and parks there. Racing the response
    // against the hold rather than just awaiting the hold keeps a request that
    // ended early (a rejected parameter, a deadline that fired before the
    // prefix resolved) from showing up as a 30 s stall with no explanation.
    let ended_early = tokio::time::timeout(HANG_CEILING, async {
        tokio::select! {
            () = gate.wait_until_held(1) => None,
            finished = &mut request => Some(finished),
        }
    })
    .await
    .expect("selector K's catalog LIST must reach the store within the hang ceiling");
    assert!(
        ended_early.is_none(),
        "the request ended before selector {HELD_SELECTOR} parked in the store, so nothing \
         was holding it open when it finished: {ended_early:?}"
    );

    // Nothing releases the hold until after this returns, so whatever ends the
    // request ends it while selector K is still parked mid-resolve.
    let (status, body) = tokio::time::timeout(HANG_CEILING, &mut request)
        .await
        .expect("the shared deadline must end the request while selector K is held")
        .expect("request task joins");

    assert_eq!(
        status, 504,
        "a request parked in selector {HELD_SELECTOR} can only end on a deadline; \
         got {status} with body {body}"
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

    // The held call is still held: the response was produced while selector
    // K's resolution was in flight, not after it completed.
    let held = gate.held_details();
    assert_eq!(
        held.len(),
        1,
        "selector {HELD_SELECTOR}'s catalog LIST must still be held when the request \
         returns, but the store holds {held:?}"
    );
    assert!(
        held[0].2.contains("/del/"),
        "the held call must be the catalog `del/` LIST of selector {HELD_SELECTOR}, got {held:?}"
    );

    // One shared budget, not N: the selectors before the held one resolved,
    // the held one never reached the backend, and the loop never got to the
    // ones after it. Exact, because the held selector cannot finish at any
    // host speed.
    let resolved = resolutions.load(Ordering::SeqCst);
    assert_eq!(
        resolved,
        HELD_SELECTOR - 1,
        "the {SELECTORS} selectors must share one wall budget: exactly the \
         {} before the held one should resolve, but {resolved} did (a per-selector \
         budget resolves the held one's successors too)",
        HELD_SELECTOR - 1
    );

    // The budget the tripped selector was handed is what the earlier selectors
    // left of the request's, so it is at most the configured budget minus
    // their dwells. A deadline rebuilt per selector reports the configured
    // budget in full.
    let granted = reported_deadline(&body);
    let spent_by_prefix = DWELL * (HELD_SELECTOR as u32 - 1);
    let ceiling = SHARED_BUDGET - spent_by_prefix;
    assert!(
        granted <= ceiling,
        "selector {HELD_SELECTOR} must be handed what is left of the request's \
         {SHARED_BUDGET:?} after the {} selectors before it spent {spent_by_prefix:?}, \
         so at most {ceiling:?}; the error reports {granted:?}, which is a budget \
         granted afresh rather than shared",
        HELD_SELECTOR - 1
    );

    // Releasing the held call drives no further resolution: the deadline
    // already cancelled the parked resolve, and the request it belonged to is
    // over, so no selector after the held one ever runs.
    assert!(
        gate.release(held[0].0),
        "the held call must still be in the store's held set when the request returns"
    );
    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(
        gate.held_count(),
        0,
        "the released call must leave the held set"
    );
    assert_eq!(
        resolutions.load(Ordering::SeqCst),
        HELD_SELECTOR - 1,
        "releasing the held call must not resolve another selector: the request \
         ended on the shared deadline and does not resume"
    );
}
