//! Crate-level integration tests (ADR-0080 decision 3, T6a deliverable 8).
//!
//! All tests use an injectable EndpointSlice source (synthetic slices fed
//! straight into [`EndpointStore::apply_event`]) and, where a proxy hop is
//! exercised, a local axum backend standing in for a gateway pod. No real
//! cluster or Kubernetes API server is needed.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use kube_runtime::watcher::Event;
use ravel_affinity::{ReplicaId, rank};

use crate::clock::Clock;
use crate::endpoints::EndpointStore;
use crate::endpoints::test_support::{TestEndpoint, slice, sync_one_slice};
use crate::key::KeyResolver;
use crate::proxy::build_app;
use crate::router::RouterState;
use crate::select::RoundRobin;

// --- test harness ------------------------------------------------------------

/// A fixed clock, so the round-robin idle logic is deterministic in tests.
struct FakeClock(i64);
impl Clock for FakeClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

type Hits = Arc<Mutex<Vec<String>>>;

/// A local backend standing in for a gateway pod. Records `"<host> <path>"` for
/// every request it receives, so a test can prove which pod address the router
/// actually dialed.
struct Backend {
    addr: SocketAddr,
    hits: Hits,
}

impl Backend {
    fn recorded(&self) -> Vec<String> {
        self.hits.lock().unwrap().clone()
    }
}

async fn record(State(hits): State<Hits>, req: Request) -> &'static str {
    let host = req
        .headers()
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let path = req.uri().path().to_string();
    hits.lock().unwrap().push(format!("{host} {path}"));
    "from-backend"
}

async fn spawn_backend() -> Backend {
    let hits: Hits = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new().fallback(record).with_state(hits.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Backend { addr, hits }
}

async fn spawn_router(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

fn state(
    store: Arc<EndpointStore>,
    key_resolver: KeyResolver,
    subset_size: usize,
) -> Arc<RouterState> {
    Arc::new(RouterState {
        endpoints: store,
        key_resolver,
        subset_size,
        round_robin: RoundRobin::new(1024, 60_000_000_000),
        clock: Arc::new(FakeClock(0)),
        http: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
    })
}

/// Feed a full relist of several slices, the shape the watcher emits on connect.
fn sync_slices(store: &EndpointStore, slices: Vec<k8s_openapi::api::discovery::v1::EndpointSlice>) {
    store.apply_event(Event::Init);
    for s in slices {
        store.apply_event(Event::InitApply(s));
    }
    store.apply_event(Event::InitDone);
}

// --- deliverable 8 tests -----------------------------------------------------

#[tokio::test]
async fn request_is_proxied_to_hrw_selected_pod_endpoint_not_service_vip() {
    // Two real backends on distinct loopback ports stand in for two gateway
    // pods. Each pod is injected as a Ready endpoint whose dial address IS that
    // backend's real bound address; the router knows no Service name or VIP,
    // only these pod addresses.
    let backend_a = spawn_backend().await;
    let backend_b = spawn_backend().await;

    let store = Arc::new(EndpointStore::new(None));
    // One slice per backend, so each pod carries its own port.
    let slice_a = slice(
        "gw-a",
        "gw",
        backend_a.addr.port() as i32,
        &[TestEndpoint {
            uid: "uid-a",
            ip: "127.0.0.1",
            ready: true,
        }],
    );
    let slice_b = slice(
        "gw-b",
        "gw",
        backend_b.addr.port() as i32,
        &[TestEndpoint {
            uid: "uid-b",
            ip: "127.0.0.1",
            ready: true,
        }],
    );
    sync_slices(&store, vec![slice_a, slice_b]);

    // Compute the HRW winner for the tenant key independently of the router, so
    // the assertion is against `ravel_affinity::rank`'s own answer, not just
    // "some backend replied".
    let key = b"Bearer test-token";
    let id_a = ReplicaId::new(b"uid-a".to_vec());
    let id_b = ReplicaId::new(b"uid-b".to_vec());
    let reps = [id_a.clone(), id_b.clone()];
    let ranked = rank(key, &reps);
    let winner = ranked[0].clone();
    let (winner_backend, loser_backend, winner_addr) = if winner == id_a {
        (&backend_a, &backend_b, backend_a.addr)
    } else {
        (&backend_b, &backend_a, backend_b.addr)
    };

    // subset_size 1: the subset is exactly the HRW winner, so the winner must
    // receive the request.
    let state = state(store, KeyResolver::Header(AUTHORIZATION), 1);
    let router_addr = spawn_router(build_app(state)).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{router_addr}/v1/metrics"))
        .header(AUTHORIZATION, "Bearer test-token")
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "from-backend");

    // The HRW-selected pod received the request, at its own address...
    let winner_hits = winner_backend.recorded();
    assert_eq!(
        winner_hits.len(),
        1,
        "winner pod received exactly one request"
    );
    assert_eq!(
        winner_hits[0],
        format!("{winner_addr} /v1/metrics"),
        "request was dialed directly to the pod address (Host == pod IP:port), not a Service VIP"
    );
    // ...and the other pod received nothing: the router did not spray across
    // endpoints the way a Service VIP would.
    assert!(
        loser_backend.recorded().is_empty(),
        "the non-selected pod must receive no traffic"
    );
}

#[tokio::test]
async fn unready_subset_member_falls_through_to_next_ranked_replica() {
    // The HRW-ranked top member is marked NotReady (absent from the Ready dial
    // set). With subset_size 1, the subset is just that top member, so the
    // request must fall through to the next-ranked replica rather than 503.
    let backend = spawn_backend().await;

    let key = b"Bearer test-token";
    let id_a = ReplicaId::new(b"uid-a".to_vec());
    let id_b = ReplicaId::new(b"uid-b".to_vec());
    let reps = [id_a.clone(), id_b.clone()];
    let ranked = rank(key, &reps);
    let top = ranked[0].clone();
    let next = ranked[1].clone();

    // Build one slice on the backend's port. The rank-top endpoint is NotReady;
    // the next-ranked endpoint is Ready and points at the backend.
    let uid_top: &'static str = if top == id_a { "uid-a" } else { "uid-b" };
    let uid_next: &'static str = if next == id_a { "uid-a" } else { "uid-b" };
    let s = slice(
        "gw",
        "gw",
        backend.addr.port() as i32,
        &[
            TestEndpoint {
                uid: uid_top,
                ip: "127.0.0.1",
                ready: false,
            },
            TestEndpoint {
                uid: uid_next,
                ip: "127.0.0.1",
                ready: true,
            },
        ],
    );
    let store = Arc::new(EndpointStore::new(None));
    sync_one_slice(&store, s);

    // Sanity: the top member ranks first but is excluded from the dial set.
    let snap = store.snapshot();
    assert!(
        snap.replicas.contains(&top),
        "unready top member still ranks"
    );
    assert!(
        !snap.ready_addr.contains_key(&top),
        "unready top member is not dialable"
    );

    let state = state(store, KeyResolver::Header(AUTHORIZATION), 1);
    let router_addr = spawn_router(build_app(state)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{router_addr}/v1/metrics"))
        .header(AUTHORIZATION, "Bearer test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "the request falls through to the next-ranked Ready replica, not 503"
    );
    let hits = backend.recorded();
    assert_eq!(
        hits.len(),
        1,
        "the next-ranked replica received the request"
    );
    assert_eq!(hits[0], format!("{} /v1/metrics", backend.addr));
}

#[tokio::test]
async fn canonical_tenant_resolution_failure_rejects_request_fail_closed() {
    // A backend is registered and Ready, so if the router ever proxied, this
    // backend would record it. It must not: canonical-tenant resolution failure
    // is fail-closed (401), never a proxy attempt.
    let backend = spawn_backend().await;
    let store = Arc::new(EndpointStore::new(None));
    let s = slice(
        "gw",
        "gw",
        backend.addr.port() as i32,
        &[TestEndpoint {
            uid: "uid-a",
            ip: "127.0.0.1",
            ready: true,
        }],
    );
    sync_one_slice(&store, s);

    // A resolver that always fails (no configured credential matches).
    struct AlwaysFail;
    impl ravel_tenant_resolve::TenantResolver for AlwaysFail {
        fn resolve(
            &self,
            _headers: &axum::http::HeaderMap,
        ) -> Result<ravel_types::TenantId, ravel_tenant_resolve::AuthError> {
            Err(ravel_tenant_resolve::AuthError)
        }
    }

    let state = state(store, KeyResolver::Canonical(Arc::new(AlwaysFail)), 2);
    let router_addr = spawn_router(build_app(state)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{router_addr}/v1/metrics"))
        .header(AUTHORIZATION, "Bearer whatever")
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        401,
        "canonical-tenant resolution failure must reject with 401"
    );
    assert!(
        backend.recorded().is_empty(),
        "fail-closed: no request may reach any backend on resolution failure"
    );
}

#[tokio::test]
async fn router_rejects_traffic_before_first_watcher_sync() {
    // A backend exists but the store has NOT completed a sync (no InitDone).
    let backend = spawn_backend().await;
    let store = Arc::new(EndpointStore::new(None));
    assert!(!store.is_synced());

    let state = state(store, KeyResolver::Header(AUTHORIZATION), 2);
    let router_addr = spawn_router(build_app(state)).await;
    let client = reqwest::Client::new();

    // /readyz reports not-ready before the first sync.
    let readyz = client
        .get(format!("http://{router_addr}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(readyz.status(), StatusCode::SERVICE_UNAVAILABLE);

    // A proxied request is 503 (not-synced), and no backend is dialed.
    let response = client
        .post(format!("http://{router_addr}/v1/metrics"))
        .header(AUTHORIZATION, "Bearer test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        backend.recorded().is_empty(),
        "no proxy attempt may happen against an unsynced (empty) subset"
    );
}

#[tokio::test]
async fn readyz_flips_to_ready_after_first_sync() {
    // Companion to the cold-start test: /readyz is 503 before sync and 200 once
    // the injected source completes a relist.
    let store = Arc::new(EndpointStore::new(None));
    let state = state(store.clone(), KeyResolver::Header(AUTHORIZATION), 2);
    let router_addr = spawn_router(build_app(state)).await;
    let client = reqwest::Client::new();

    let before = client
        .get(format!("http://{router_addr}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), StatusCode::SERVICE_UNAVAILABLE);

    let s = slice(
        "gw",
        "gw",
        8080,
        &[TestEndpoint {
            uid: "uid-a",
            ip: "10.0.0.1",
            ready: true,
        }],
    );
    sync_one_slice(&store, s);

    let after = client
        .get(format!("http://{router_addr}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(after.status(), StatusCode::OK);
}

#[tokio::test]
async fn different_header_values_route_to_different_subsets() {
    // The authorization-header/header/mtls-subject sources hash the raw header
    // bytes, so distinct values pick distinct pods. Use many pods and two keys
    // that HRW-rank different winners, and prove each key lands on its own
    // winner.
    let store = Arc::new(EndpointStore::new(None));
    let mut backends = Vec::new();
    let mut slices = Vec::new();
    for i in 0..6u8 {
        let backend = spawn_backend().await;
        let uid: &'static str = Box::leak(format!("uid-{i}").into_boxed_str());
        slices.push(slice(
            Box::leak(format!("gw-{i}").into_boxed_str()),
            "gw",
            backend.addr.port() as i32,
            &[TestEndpoint {
                uid,
                ip: "127.0.0.1",
                ready: true,
            }],
        ));
        backends.push((uid, backend));
    }
    sync_slices(&store, slices);

    let reps: Vec<ReplicaId> = backends
        .iter()
        .map(|(uid, _)| ReplicaId::new(uid.as_bytes()))
        .collect();

    // Find two header values whose HRW winners differ.
    let mut chosen: Option<(String, String, ReplicaId, ReplicaId)> = None;
    for a in 0..50 {
        for b in (a + 1)..50 {
            let ka = format!("tenant-{a}");
            let kb = format!("tenant-{b}");
            let wa = rank(ka.as_bytes(), &reps)[0].clone();
            let wb = rank(kb.as_bytes(), &reps)[0].clone();
            if wa != wb {
                chosen = Some((ka, kb, wa, wb));
                break;
            }
        }
        if chosen.is_some() {
            break;
        }
    }
    let (ka, kb, wa, wb) = chosen.expect("two keys with distinct HRW winners exist");

    let state = state(
        store,
        KeyResolver::Header(axum::http::HeaderName::from_static("x-tenant")),
        1,
    );
    let router_addr = spawn_router(build_app(state)).await;
    let client = reqwest::Client::new();

    for (key, winner) in [(ka, wa), (kb, wb)] {
        client
            .post(format!("http://{router_addr}/v1/metrics"))
            .header("x-tenant", &key)
            .send()
            .await
            .unwrap();
        let winner_uid = std::str::from_utf8(winner.as_bytes()).unwrap();
        let (_, backend) = backends.iter().find(|(uid, _)| *uid == winner_uid).unwrap();
        assert_eq!(
            backend.recorded().len(),
            1,
            "key {key} must land on its HRW winner {winner_uid}"
        );
    }
}

#[tokio::test]
async fn tenant_key_bytes_never_appear_in_an_error_body() {
    // authorization-header source with a secret token, forced down an error
    // path (Exhausted: synced but no Ready endpoint). The 503 body must not
    // contain the token bytes.
    let secret = "super-secret-token-value";
    let store = Arc::new(EndpointStore::new(None));
    let s = slice(
        "gw",
        "gw",
        8080,
        &[TestEndpoint {
            uid: "uid-a",
            ip: "10.0.0.1",
            ready: false, // synced, but nothing Ready -> Exhausted
        }],
    );
    sync_one_slice(&store, s);

    let state = state(store, KeyResolver::Header(AUTHORIZATION), 2);
    let router_addr = spawn_router(build_app(state)).await;

    let response = reqwest::Client::new()
        .post(format!("http://{router_addr}/v1/metrics"))
        .header(AUTHORIZATION, format!("Bearer {secret}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response.text().await.unwrap();
    assert!(
        !body.contains(secret),
        "the tenant key (bearer token) must never appear in an error body; got {body:?}"
    );
}
