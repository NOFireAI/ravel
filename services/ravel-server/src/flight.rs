//! Flight SQL registration on the gRPC listener, compiled only under the
//! `flight-sql` feature.
//!
//! Ticket C1d replaces C1a's `UnimplementedFlightSqlService` stub
//! with the real service from `ravel_sql::flight`. This module is only the
//! wiring: it adapts the two deployment-owned things ravel-sql states as
//! traits -- the authoritative request identity and the injected clock -- and
//! hands them to [`RavelFlightSqlService`] together with the same
//! `SqlExecutor`, clock, deadline ceiling, and object store `SqlState` already
//! carries for `POST /api/v1/sql`. Sharing the store is what lets the Flight
//! path write the same query-audit record the HTTP endpoint does,
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
use ravel_sql::flight::AuditSubject;
use ravel_sql::{
    DistributedFlightConfig, FlightAuth, FlightClock, FlightListenerRole, FlightSqlConfig,
    MinCommitToken, RavelFlightSqlService, SliceRejectCounters, SqlTicketKeys,
};
use ravel_types::TenantHash;
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::flight_auth;
use crate::flight_deadline::DeadlineBoundedFlightService;
use crate::sql::SqlState;

/// Resolves the authoritative tenant and read-your-write tokens for a Flight
/// SQL request from its gRPC metadata.
///
/// For every client request the deployment's [`TenantResolver`] is the only
/// authority, and ravel-sql never sees a credential. A slice `DoGet`
/// (ADR-1689 decision 2) is the one exception: on a listener that serves
/// slices (the dedicated fragment listener, or the combined public listener of
/// a process without one), a ticket that verifies under the slice key runs
/// under the tenant it carries, and this resolver is never consulted for it.
pub struct ResolverFlightAuth {
    tenant_resolver: Arc<dyn TenantResolver>,
}

impl FlightAuth for ResolverFlightAuth {
    fn tenant(&self, metadata: &MetadataMap) -> Result<TenantHash, Status> {
        flight_auth::resolve_tenant(self.tenant_resolver.as_ref(), metadata)
    }

    fn min_commit_tokens(&self, metadata: &MetadataMap) -> Result<Vec<MinCommitToken>, Status> {
        flight_auth::min_commit_tokens(metadata)
    }

    fn audit_subject(&self, metadata: &MetadataMap) -> Result<Option<AuditSubject>, Status> {
        let headers = crate::otlp_grpc::metadata_to_headers(metadata);
        let principal = self
            .tenant_resolver
            .resolve_principal(&headers)
            .map_err(|_| Status::unauthenticated("invalid or missing tenant credentials"))?;
        if principal.tenant.hash() != self.tenant(metadata)? {
            return Ok(None);
        }
        Ok(ravel_query::http::audit_subject(&principal))
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
/// are bounded by the same server ceiling. `gc_protection_horizon` is
/// `gc_ticket_ceiling`, sourced by the caller from the durable `sys/gc` object
/// (`protection_horizon - grace`, ADR-0050 section 4, EC4) rather than
/// ravel-sql's conservative hardcoded default: this is where the ticket's GC
/// ceiling becomes the single durable authority the flight_ticket.rs docs
/// anticipate. The default event-time window still takes ravel-sql's default.
///
/// The service runs every statement whole-set on this process and serves the
/// client surface only (`ClientOnly`): it holds no distributed scan config, so
/// it never dials a worker. The server's own listeners are assembled with
/// [`service_on_listener`].
pub fn service(
    state: &SqlState,
    gc_ticket_ceiling: std::time::Duration,
) -> FlightServiceServer<DeadlineBoundedFlightService<RavelFlightSqlService>> {
    service_on_listener(
        state,
        gc_ticket_ceiling,
        None,
        None,
        FlightMount {
            role: FlightListenerRole::ClientOnly,
            slice_client_tls: None,
            slice_rejects: None,
            slice_tls_dials: None,
        },
    )
}

/// Which listener a Flight SQL service is mounted on and how its coordinator
/// dials worker slices (ADR-1689 decision 1).
#[derive(Clone)]
pub struct FlightMount {
    /// `ClientOnly` on the public gRPC listener and `SliceOnly` on the
    /// dedicated fragment listener.
    pub role: FlightListenerRole,
    /// The pinned-CA client configuration the coordinator dials worker slices
    /// with ([`crate::distrib::fragment_client_tls`]). `None` only for a
    /// service that dials no worker: one without a distributed scan config, or
    /// the `SliceOnly` service.
    pub slice_client_tls: Option<tonic::transport::ClientTlsConfig>,
    /// Counters shared by every service of one process, so its refusals are
    /// counted once whichever listener refused them. `None` keeps the
    /// service's own.
    pub slice_rejects: Option<SliceRejectCounters>,
    /// The counter slice fetches dialed over `slice_client_tls` are recorded
    /// in. `None` keeps the service's own.
    pub slice_tls_dials: Option<ravel_sql::SliceTlsDialCounter>,
}

/// The Flight SQL service, mounted as `mount` describes. `distributed`, when
/// `Some`, is the ADR-0071 coordinator-side scan config: the fleet worker
/// roster plus the cost gate the process installs under `--distributed-query`.
/// `ticket_keys` are the keys read from `--sql-ticket-key-file` (ADR-1689
/// decision 2); `None` keeps the service's own per-process random keys.
pub fn service_on_listener(
    state: &SqlState,
    gc_ticket_ceiling: std::time::Duration,
    distributed: Option<DistributedFlightConfig>,
    ticket_keys: Option<SqlTicketKeys>,
    mount: FlightMount,
) -> FlightServiceServer<DeadlineBoundedFlightService<RavelFlightSqlService>> {
    let auth = Arc::new(ResolverFlightAuth {
        tenant_resolver: Arc::clone(&state.tenant_resolver),
    });
    let clock = Arc::new(IngestClock {
        clock: Arc::clone(&state.clock),
    });
    let config = FlightSqlConfig {
        max_deadline: state.max_deadline,
        gc_protection_horizon: gc_ticket_ceiling,
        ..FlightSqlConfig::default()
    };
    let mut service = RavelFlightSqlService::new(
        Arc::clone(&state.executor),
        auth,
        clock,
        config,
        // The same per-query cost aggregator every other read surface folds
        // into, cloned out of `SqlState`, so Flight SQL cost reaches the one
        // process `ravel_query_*` family (ADR-0044 section 4).
        Arc::clone(&state.query_accounting) as Arc<dyn ravel_types::accounting::QueryCostRecorder>,
        // The one shared fleet-global query concurrency controller (ADR-0061
        // decision 2), cloned out of `SqlState`, so Flight SQL
        // `GetFlightInfo` admits against the same process-wide in-flight
        // count the PromQL and HTTP SQL surfaces do.
        Arc::clone(&state.query_admission),
    )
    // The same evidential audit sink the HTTP SQL endpoint submits through
    // (ADR-0062 §2a). Flight SQL audits at stream completion through it,
    // so both transports' query-audit trails land through one
    // seam.
    .with_audit_sink(Arc::clone(&state.audit_sink));
    // ADR-0071: install the coordinator-side distributed scan when
    // the deployment built one (`--distributed-query` on a query-serving mode).
    // Absent it, the service is byte-identical to the pre-distribution build.
    if let Some(config) = distributed {
        service = service.with_distributed_scan(config);
    }
    if let Some(keys) = ticket_keys {
        service = service.with_ticket_keys(keys);
    }
    service = service.with_listener_role(mount.role);
    if let Some(tls) = mount.slice_client_tls {
        service = service.with_slice_client_tls(tls);
    }
    if let Some(counters) = mount.slice_rejects {
        service = service.with_slice_reject_counters(counters);
    }
    if let Some(counter) = mount.slice_tls_dials {
        service = service.with_slice_tls_dial_counter(counter);
    }
    // Bound every DoGet stream by the server ceiling so a client that opens the
    // stream and then reads nothing cannot pin its query-concurrency permit
    // indefinitely. `max_deadline` is the same bound the per-ticket
    // effective deadline is already clamped against, so this never shortens a
    // within-ticket consumer's budget; it only breaks the transport-level stall
    // the per-batch deadline check inside the stream cannot reach.
    FlightServiceServer::new(DeadlineBoundedFlightService::new(
        service,
        state.max_deadline,
    ))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use axum::http::HeaderMap;
    use ravel_tenant_resolve::{AuthError, Principal, Subject, SubjectSource};
    use ravel_types::TenantId;

    /// Admits `admitted` and resolves a principal of `principal` carrying the
    /// subject `alice`, so the two resolutions `ResolverFlightAuth` makes can
    /// disagree.
    struct SplitResolver {
        admitted: TenantId,
        principal: TenantId,
    }

    impl TenantResolver for SplitResolver {
        fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
            Ok(self.admitted.clone())
        }

        fn resolve_principal(&self, _headers: &HeaderMap) -> Result<Principal, AuthError> {
            let mut principal = Principal::tenant_only(self.principal.clone());
            principal.subject =
                Some(Subject::new(SubjectSource::StaticToken, "alice").expect("a valid subject"));
            Ok(principal)
        }
    }

    fn audit_subject_for(admitted: &str, principal: &str) -> Option<AuditSubject> {
        let auth = ResolverFlightAuth {
            tenant_resolver: Arc::new(SplitResolver {
                admitted: TenantId::new(admitted),
                principal: TenantId::new(principal),
            }),
        };
        auth.audit_subject(&MetadataMap::new())
            .expect("both resolutions succeed")
    }

    #[test]
    fn audit_subject_names_the_principal_of_the_admitted_tenant() {
        assert_eq!(
            audit_subject_for("acme", "acme"),
            Some(AuditSubject {
                value: "alice".to_string(),
                source: "static_token".to_string(),
            })
        );
    }

    #[test]
    fn audit_subject_is_none_when_the_principal_is_another_tenants() {
        assert_eq!(audit_subject_for("acme", "globex"), None);
    }
}
