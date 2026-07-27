//! Shared tenant resolution for both the OTLP ingest path and the query API.

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::HeaderMap;
use ravel_query::http::{
    AuthError, DevHeaderTenantResolver, StaticBearerTokenResolver, TenantResolver,
};
use ravel_types::TenantId;

/// Tries each resolver in order, returning the first successful match.
pub struct FallbackResolver {
    resolvers: Vec<Arc<dyn TenantResolver>>,
}

impl FallbackResolver {
    pub fn new(resolvers: Vec<Arc<dyn TenantResolver>>) -> Self {
        FallbackResolver { resolvers }
    }
}

impl TenantResolver for FallbackResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        for resolver in &self.resolvers {
            if let Ok(tenant) = resolver.resolve(headers) {
                return Ok(tenant);
            }
        }
        Err(AuthError)
    }
}

pub fn build_resolver(
    tokens: HashMap<String, TenantId>,
    dev_header: bool,
) -> Arc<dyn TenantResolver> {
    let bearer: Arc<dyn TenantResolver> = Arc::new(StaticBearerTokenResolver::new(tokens));
    if dev_header {
        Arc::new(FallbackResolver::new(vec![
            bearer,
            Arc::new(DevHeaderTenantResolver::default()),
        ]))
    } else {
        bearer
    }
}
