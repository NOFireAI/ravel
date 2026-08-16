//! Ravel-native subset-affinity ingest router (ADR-0080 decision 3, T6a).
//!
//! A horizontally-scalable HTTP reverse proxy that pins each tenant's ingest
//! traffic to a stable subset-of-`S` gateway pods, computed with rendezvous
//! (HRW) hashing over the pods' own `EndpointSlice` identities. Requests are
//! dialed **directly to the chosen pod address**, never through the gateway
//! Service's cluster IP, so kube-proxy's own load balancing cannot undo the
//! subset selection.
//!
//! # Request path
//!
//! Every request flows deliverable 3 -> 4 -> 5:
//!
//! 1. [`key`]: resolve the raw tenant-key bytes from the request headers. Under
//!    `canonical-tenant` this runs the shared [`ravel_tenant_resolve`] chain and
//!    is **fail-closed** (a resolution failure is a 401, never a fallback to a
//!    weaker key or to unpinned routing).
//! 2. [`select`]: [`ravel_affinity::rank`] the current endpoint snapshot, take
//!    the top `subset_size` as the working subset, pick one member by bounded
//!    per-tenant round-robin, and fall through the HRW order past position `S`
//!    for any member not present in the Ready snapshot.
//! 3. [`proxy`]: reverse-proxy the request to the selected pod's dial address.
//!
//! The protocol-agnostic core of steps 1-2 is
//! [`router::RouterState::resolve_and_select`]; the follow-up gRPC task (#184)
//! calls it directly rather than reimplementing selection.
//!
//! # Cold start
//!
//! The endpoint snapshot starts empty and the watcher's first sync takes an
//! observable moment after process start. [`router::RouterState::resolve_and_select`]
//! rejects with [`select::RouteError::NotSynced`] (a 503) until the first
//! [`kube_runtime::watcher`] sync completes, and the same latch backs the
//! `/readyz` probe, so a rolling deploy never serves from an empty subset.

pub mod config;

pub(crate) mod auth;
pub(crate) mod clock;
pub(crate) mod endpoints;
pub(crate) mod key;
pub(crate) mod proxy;
pub(crate) mod router;
pub(crate) mod select;
pub(crate) mod watch;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use anyhow::Context;
use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::config::{KeyConfig, RouterConfig};

/// Wire the watcher, resolver, selector, and HTTP proxy into one running
/// process and serve until the listener closes, or exit with an error if the
/// EndpointSlice watcher task returns or panics (deliverable 6).
///
/// The stages are wired through [`router::RouterState::resolve_and_select`], the
/// same boundary a unit test drives directly, so "reachable" here means the real
/// binary and the tests exercise one code path, not two.
pub async fn run(config: RouterConfig) -> anyhow::Result<()> {
    let idle_ttl_ns = i64::try_from(config.round_robin_idle_ttl.as_nanos()).unwrap_or(i64::MAX);

    // EndpointSlice watcher: build the shared store and drive it from the kube
    // watch stream on a background task. The store starts not-synced, so the
    // proxy and `/readyz` reject until the first sync completes.
    let store = Arc::new(endpoints::EndpointStore::new(
        config.gateway_port_name.clone(),
    ));
    let client = kube::Client::try_default().await?;
    let api: kube::Api<EndpointSlice> =
        kube::Api::namespaced(client, &config.gateway_service_namespace);
    let watch_config = watch::watch_config(&config.gateway_service_name);
    let watch_handle = tokio::spawn(watch::run(api, watch_config, store.clone()));

    // Key resolution (deliverable 3). Resolver wiring is built only for the
    // canonical-tenant key source.
    let key_resolver = match config.key {
        KeyConfig::Header(name) => key::KeyResolver::Header(name),
        KeyConfig::CanonicalTenant(settings) => {
            let built = auth::build(&settings)?;
            if let Some(refresh) = built.oidc_refresh {
                auth::spawn_jwks_refresh(refresh);
            }
            key::KeyResolver::Canonical(built.resolver)
        }
    };

    // The reverse-proxy client. Redirects are passed back to the client, never
    // followed here: a proxy that chased a redirect would leave the pinned pod.
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let state = Arc::new(router::RouterState {
        endpoints: store,
        key_resolver,
        subset_size: config.subset_size,
        round_robin: select::RoundRobin::new(config.round_robin_max_entries, idle_ttl_ns),
        clock: Arc::new(clock::SystemClock),
        http,
    });

    // Bound the round-robin map over time: sweep entries idle past the TTL on
    // that same interval (the idle-eviction shape of ADR-0069).
    spawn_round_robin_sweep(state.clone(), config.round_robin_idle_ttl);

    let listener = tokio::net::TcpListener::bind(config.listen_http).await?;
    tracing::info!(addr = %config.listen_http, "ravel-ingest-router listening");

    // The watcher normally never returns (kube_runtime::watcher turns errors
    // into stream items and retries with backoff internally, per watch::run's
    // own doc comment). If its task does return or panic, the process must not
    // keep serving on an endpoint snapshot that will never update again: a
    // silently-dead watcher would route tenant ingest to an increasingly stale
    // (and, after enough pod churn, wrong) set of addresses while /readyz kept
    // reporting healthy. Race it against the HTTP server and exit on either.
    tokio::select! {
        result = axum::serve(listener, proxy::build_app(state)) => {
            result?;
        }
        watch_result = watch_handle => {
            match watch_result {
                Ok(()) => anyhow::bail!("endpointslice watcher task ended unexpectedly"),
                Err(join_error) => {
                    return Err(anyhow::Error::from(join_error))
                        .context("endpointslice watcher task panicked");
                }
            }
        }
    }
    Ok(())
}

/// Periodically evict idle round-robin entries. A swept entry restarts at
/// offset 0 on the tenant's next request, so this only bounds memory and never
/// affects correctness.
fn spawn_round_robin_sweep(state: Arc<router::RouterState>, interval: std::time::Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // consume the immediate first tick
        loop {
            ticker.tick().await;
            let evicted = state.round_robin.evict_idle(state.clock.now_ns());
            if evicted > 0 {
                tracing::debug!(evicted, "swept idle round-robin entries");
            }
        }
    });
}
