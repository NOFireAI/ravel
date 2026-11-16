//! Tenant-key resolution (deliverable 3): turn a request's headers into the raw
//! key bytes fed to [`ravel_affinity::rank`].
//!
//! # Secrecy
//!
//! Under `authorization-header` the key *is* the raw bearer token. [`TenantKey`]
//! therefore never exposes its bytes through `Debug`/`Display`, and nothing in
//! this crate logs them or places them in a response or error body. The bounded
//! round-robin map is keyed by [`TenantKey::hash`] (a blake3 digest), not the
//! raw bytes, so no raw token is ever held as a map key either.

use axum::http::HeaderMap;
use axum::http::header::HeaderName;

use ravel_tenant_resolve::TenantResolver;
use std::sync::Arc;

use crate::select::RouteError;

/// Raw tenant-key bytes. Deliberately opaque: its `Debug` redacts the contents
/// so a key can never leak into a log line through a `?`-formatted struct.
#[derive(Clone)]
pub(crate) struct TenantKey(Vec<u8>);

impl TenantKey {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        TenantKey(bytes)
    }

    /// The raw bytes, for [`ravel_affinity::rank`] only.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// A blake3 digest of the key, used as the round-robin map key so the raw
    /// bytes (a bearer token under `authorization-header`) are never stored.
    pub(crate) fn hash(&self) -> [u8; 32] {
        *blake3::hash(&self.0).as_bytes()
    }
}

impl std::fmt::Debug for TenantKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the bytes: this is the load-bearing no-leak property.
        write!(f, "TenantKey(<redacted {} bytes>)", self.0.len())
    }
}

/// How a request's headers map to key bytes.
pub(crate) enum KeyResolver {
    /// Hash the raw value bytes of this header. A missing header yields an empty
    /// key (deterministic; matches nginx `upstream-hash-by` on an absent
    /// variable). Not fail-closed: only `canonical-tenant` fails closed.
    Header(HeaderName),
    /// Resolve the canonical tenant via the shared chain; on failure reject the
    /// request (401), never falling back to a weaker key. This is the
    /// security-critical fail-closed path (ADR-0080 decision 3).
    Canonical(Arc<dyn TenantResolver>),
}

impl KeyResolver {
    /// Resolve the routing key, or a [`RouteError`] the proxy turns into a
    /// status code.
    pub(crate) fn resolve(&self, headers: &HeaderMap) -> Result<TenantKey, RouteError> {
        match self {
            KeyResolver::Header(name) => {
                let bytes = headers
                    .get(name)
                    .map(|v| v.as_bytes().to_vec())
                    .unwrap_or_default();
                Ok(TenantKey::new(bytes))
            }
            KeyResolver::Canonical(resolver) => match resolver.resolve(headers) {
                Ok(tenant) => Ok(TenantKey::new(tenant.as_str().as_bytes().to_vec())),
                // Fail-closed: no fallback to a header key or to unpinned
                // routing. The error carries no key bytes.
                Err(_) => Err(RouteError::Unauthenticated),
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_tenant_resolve::AuthError;
    use ravel_types::TenantId;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                HeaderName::try_from(*k).expect("valid header name"),
                v.parse().expect("valid header value"),
            );
        }
        map
    }

    #[test]
    fn header_source_reads_raw_value_bytes() {
        let resolver = KeyResolver::Header(HeaderName::from_static("authorization"));
        let key = resolver
            .resolve(&headers(&[("authorization", "Bearer secret-token")]))
            .expect("header present");
        assert_eq!(key.as_bytes(), b"Bearer secret-token");
    }

    #[test]
    fn header_source_missing_header_is_empty_key_not_error() {
        let resolver = KeyResolver::Header(HeaderName::from_static("x-tenant"));
        let key = resolver
            .resolve(&HeaderMap::new())
            .expect("a missing header is an empty key, not an error");
        assert!(key.as_bytes().is_empty());
    }

    #[test]
    fn different_header_values_produce_different_keys() {
        let resolver = KeyResolver::Header(HeaderName::from_static("x-tenant"));
        let a = resolver.resolve(&headers(&[("x-tenant", "a")])).expect("a");
        let b = resolver.resolve(&headers(&[("x-tenant", "b")])).expect("b");
        assert_ne!(a.as_bytes(), b.as_bytes());
        assert_ne!(a.hash(), b.hash());
    }

    struct AlwaysFail;
    impl TenantResolver for AlwaysFail {
        fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
            Err(AuthError)
        }
    }

    struct AlwaysTenant(&'static str);
    impl TenantResolver for AlwaysTenant {
        fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
            Ok(TenantId::new(self.0))
        }
    }

    #[test]
    fn canonical_success_uses_tenant_bytes() {
        let resolver = KeyResolver::Canonical(Arc::new(AlwaysTenant("acme")));
        let key = resolver.resolve(&HeaderMap::new()).expect("resolves");
        assert_eq!(key.as_bytes(), b"acme");
    }

    #[test]
    fn canonical_failure_is_unauthenticated_fail_closed() {
        let resolver = KeyResolver::Canonical(Arc::new(AlwaysFail));
        let err = resolver
            .resolve(&HeaderMap::new())
            .expect_err("resolution failure rejects");
        assert!(matches!(err, RouteError::Unauthenticated));
    }

    #[test]
    fn debug_never_prints_key_bytes() {
        let key = TenantKey::new(b"super-secret-token".to_vec());
        let rendered = format!("{key:?}");
        assert!(
            !rendered.contains("super-secret-token"),
            "TenantKey Debug must never render the raw bytes"
        );
    }
}
