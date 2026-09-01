//! The shared selection core (deliverables 3-4 wired together) plus its
//! status-code mapping.
//!
//! [`RouterState::resolve_and_select`] is the protocol-agnostic boundary #184
//! calls to reuse selection for the gRPC listener: it takes a `&HeaderMap`
//! (gRPC metadata maps to the same type) and returns the chosen
//! [`Selection`](crate::select::Selection) or a [`RouteError`], with no HTTP
//! specifics. The HTTP proxy ([`crate::proxy`]) is the only HTTP-aware layer on
//! top of it.

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};

use crate::clock::Clock;
use crate::endpoints::EndpointStore;
use crate::key::KeyResolver;
use crate::select::{RoundRobin, RouteError, Selection, pick};

/// Everything the request path needs, shared behind an `Arc`.
pub(crate) struct RouterState {
    pub endpoints: Arc<EndpointStore>,
    pub key_resolver: KeyResolver,
    pub subset_size: usize,
    pub round_robin: RoundRobin,
    pub clock: Arc<dyn Clock>,
    pub http: reqwest::Client,
    /// The forwarding client for the gRPC listener ([`crate::grpc`]). Separate
    /// from `http` because it is built with `http2_prior_knowledge()`, which
    /// forces every request onto cleartext HTTP/2 (h2c) and so cannot serve the
    /// HTTP/1 proxy path.
    pub grpc_http: reqwest::Client,
}

impl RouterState {
    /// Resolve the tenant key and select the pod to dial (deliverables 3-4).
    ///
    /// This is the reuse boundary for #184: it is protocol-agnostic (headers in,
    /// a [`Selection`] out) and performs the full cold-start gate, fail-closed
    /// canonical-tenant resolution, HRW ranking, bounded round-robin pick, and
    /// unready-member fallback. The HTTP proxy and the future gRPC proxy both
    /// call it rather than reimplementing any of it.
    pub(crate) fn resolve_and_select(&self, headers: &HeaderMap) -> Result<Selection, RouteError> {
        // Cold start: reject before the first watcher sync rather than after
        // failing to find a replica in an empty subset.
        if !self.endpoints.is_synced() {
            return Err(RouteError::NotSynced);
        }

        // Deliverable 3. Fail-closed under canonical-tenant.
        let key = self.key_resolver.resolve(headers)?;

        // Deliverable 4. Clone the snapshot Arc, then rank/select with no lock
        // held.
        let snapshot = self.endpoints.snapshot();
        if snapshot.replicas.is_empty() {
            return Err(RouteError::Exhausted);
        }
        let ranked = ravel_affinity::rank(key.as_bytes(), &snapshot.replicas);
        let offset = self.round_robin.tick(key.hash(), self.clock.now_ns());
        pick(&ranked, &snapshot.ready_addr, self.subset_size, offset)
    }
}

impl RouteError {
    /// The HTTP status a [`RouteError`] maps to. The body carries no key bytes
    /// (the status alone is returned).
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            RouteError::NotSynced => StatusCode::SERVICE_UNAVAILABLE,
            RouteError::Unauthenticated => StatusCode::UNAUTHORIZED,
            RouteError::Exhausted => StatusCode::SERVICE_UNAVAILABLE,
            RouteError::Upstream => StatusCode::BAD_GATEWAY,
        }
    }

    /// The gRPC status code a [`RouteError`] maps to (deliverable 2 of #184).
    ///
    /// This is the same enum's second status-code projection, alongside
    /// [`RouteError::status`], for the gRPC transport ([`crate::grpc`]). The
    /// mapping is not identical to the HTTP one: a cold-start/not-ready or
    /// exhausted-subset condition and an upstream forwarding failure are all
    /// retryable *availability* problems to a gRPC caller, so they map to
    /// `Unavailable` (the HTTP projection distinguishes 503 from a 502 bad
    /// gateway, a distinction gRPC's code space does not carry). A resolution
    /// failure is `Unauthenticated`, matching the 401 projection and ADR-0080's
    /// fail-closed rule. Like `status`, it carries no key bytes.
    pub(crate) fn grpc_code(&self) -> tonic::Code {
        match self {
            RouteError::NotSynced => tonic::Code::Unavailable,
            RouteError::Unauthenticated => tonic::Code::Unauthenticated,
            RouteError::Exhausted => tonic::Code::Unavailable,
            RouteError::Upstream => tonic::Code::Unavailable,
        }
    }
}
