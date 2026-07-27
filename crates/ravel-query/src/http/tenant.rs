//! Tenant resolution for the HTTP layer. There is deliberately no default
//! resolver: callers must construct one of the impls below (or their own),
//! so an unauthenticated deployment is a conscious choice, not an oversight
//! (docs/query-engine.md "default deny").

use axum::http::HeaderMap;
use std::collections::HashMap;

use ravel_types::TenantId;

/// A request could not be attributed to a tenant.
#[derive(Debug, thiserror::Error)]
#[error("unauthenticated request")]
pub struct AuthError;

/// Resolves the tenant for an incoming request from its headers.
pub trait TenantResolver: Send + Sync {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError>;
}

/// Resolves a tenant from a static `Authorization: Bearer <token>` map.
pub struct StaticBearerTokenResolver {
    tokens: HashMap<String, TenantId>,
}

impl StaticBearerTokenResolver {
    pub fn new(tokens: HashMap<String, TenantId>) -> Self {
        StaticBearerTokenResolver { tokens }
    }
}

impl TenantResolver for StaticBearerTokenResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let raw = headers
            .get(axum::http::header::AUTHORIZATION)
            .ok_or(AuthError)?;
        let raw = raw.to_str().map_err(|_| AuthError)?;
        let token = raw.strip_prefix("Bearer ").ok_or(AuthError)?;
        self.tokens.get(token).cloned().ok_or(AuthError)
    }
}

/// Resolves a tenant straight from a header value, for development and
/// tests only. Must be constructed explicitly; never used as a default.
pub struct DevHeaderTenantResolver {
    header_name: String,
}

impl DevHeaderTenantResolver {
    pub fn new(header_name: impl Into<String>) -> Self {
        DevHeaderTenantResolver {
            header_name: header_name.into(),
        }
    }
}

impl Default for DevHeaderTenantResolver {
    fn default() -> Self {
        DevHeaderTenantResolver::new("x-ravel-tenant")
    }
}

impl TenantResolver for DevHeaderTenantResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let raw = headers.get(self.header_name.as_str()).ok_or(AuthError)?;
        let value = raw.to_str().map_err(|_| AuthError)?;
        if value.is_empty() {
            return Err(AuthError);
        }
        Ok(TenantId::new(value.to_string()))
    }
}
