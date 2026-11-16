//! Tenant resolution for the HTTP layer.
//!
//! The implementation moved to the `ravel-tenant-resolve` crate (ADR-0080
//! decision 3) as a mechanical, behavior-preserving extraction, so
//! `ravel-server` and the future `ravel-ingest-router` depend on one resolver
//! implementation rather than two copies that can drift. This module stays as
//! ravel-query's own public re-export surface: every existing
//! `ravel_query::http::X` (and `ravel_query::http::tenant::X`) path keeps
//! resolving unchanged.
//!
//! There is deliberately no default resolver: callers must construct one of
//! the impls (or their own), so an unauthenticated deployment is a conscious
//! choice, not an oversight (docs/query-engine.md "default deny").

pub use ravel_tenant_resolve::{
    AuthError, DevHeaderTenantResolver, MtlsResolver, OidcError, OidcJwksCache, OidcResolver,
    StaticBearerTokenResolver, TenantResolver,
};
