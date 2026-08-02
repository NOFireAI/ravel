//! Flight SQL registration on the gRPC listener, compiled only under the
//! `flight-sql` feature.
//!
//! Ticket C1d (issue #152) replaces C1a's `UnimplementedFlightSqlService` stub
//! with the real service from `ravel_sql::flight`. This module is only the
//! wiring: it adapts the two deployment-owned things ravel-sql states as
//! traits -- the authoritative request identity and the injected clock -- and
//! hands them to [`RavelFlightSqlService`] together with the same
//! `SqlExecutor`, clock, deadline ceiling, and object store `SqlState` already
//! carries for `POST /api/v1/sql`. Sharing the store is what lets the Flight
//! path write the same query-audit record the HTTP endpoint does (issue #395),
//! so a tenant's query activity is durably logged on both transports.
//!
//! Sharing the executor is the point, not an optimization: the per-tenant
//! memory accountants live inside it, so a tenant that runs one query over
//! HTTP and one over Flight accounts both against one tenant budget. Two
//! executors would give the same tenant two independent budgets.
//!
//! Nothing here decides anything about the protocol. Tenant resolution reuses
//! [`crate::flight_auth`], which reuses the OTLP gRPC path's
//! metadata-to-header translation, so all three transports reject the same
//! inputs the same way.

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use ravel_ingest::Clock;
use ravel_query::http::TenantResolver;
use ravel_sql::{FlightAuth, FlightClock, FlightSqlConfig, RavelFlightSqlService};
use ravel_types::{CommitToken, TenantHash};
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::flight_auth;
use crate::sql::SqlState;

/// Resolves the authoritative tenant and read-your-write tokens for a Flight
/// SQL request from its gRPC metadata.
///
/// The deployment's [`TenantResolver`] is the only authority. ravel-sql never
/// sees a credential and never reads a tenant from a ticket.
pub struct ResolverFlightAuth {
    tenant_resolver: Arc<dyn TenantResolver>,
}

impl FlightAuth for ResolverFlightAuth {
    fn tenant(&self, metadata: &MetadataMap) -> Result<TenantHash, Status> {
        flight_auth::resolve_tenant(self.tenant_resolver.as_ref(), metadata)
    }

    fn min_commit_tokens(&self, metadata: &MetadataMap) -> Result<Vec<CommitToken>, Status> {
        flight_auth::min_commit_tokens(metadata)
    }
}

/// Adapts the process's [`ravel_ingest::Clock`] to ravel-sql's
/// [`FlightClock`].
///
/// ravel-sql states the one-method clock contract itself rather than linking
/// ravel-ingest across the ADR-0013 boundary, so the deployment bridges the
/// two here, once.
pub struct IngestClock {
    clock: Arc<dyn Clock>,
}

impl FlightClock for IngestClock {
    fn now_ns(&self) -> i64 {
        self.clock.now_ns()
    }
}

/// Builds the Flight SQL service to register on the gRPC server.
///
/// `max_deadline` comes from `SqlState`, so a Flight query and an HTTP query
/// are bounded by the same server ceiling; the GC protection horizon and the
/// default event-time window take ravel-sql's documented defaults.
pub fn service(state: &SqlState) -> FlightServiceServer<RavelFlightSqlService> {
    let auth = Arc::new(ResolverFlightAuth {
        tenant_resolver: Arc::clone(&state.tenant_resolver),
    });
    let clock = Arc::new(IngestClock {
        clock: Arc::clone(&state.clock),
    });
    let config = FlightSqlConfig {
        max_deadline: state.max_deadline,
        ..FlightSqlConfig::default()
    };
    FlightServiceServer::new(RavelFlightSqlService::new(
        Arc::clone(&state.executor),
        auth,
        clock,
        config,
        // The same object store `SqlState` writes the HTTP endpoint's
        // query-audit record to, so Flight statements are audited through the
        // same store (issue #395).
        Arc::clone(&state.store),
    ))
}
