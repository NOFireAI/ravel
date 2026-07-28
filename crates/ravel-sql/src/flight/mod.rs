//! Flight SQL transport for the SQL execution path (ticket C1d, issue #152),
//! compiled only under the `flight-sql` feature.
//!
//! The service here answers the same queries `POST /api/v1/sql` answers,
//! through the same [`SqlExecutor`](crate::SqlExecutor): the same validation
//! gate, the same `Catalog::resolve`, the same fresh per-query single-tenant
//! `SessionContext`, the same memory pool, the same snapshot retry contract.
//! Only the transport differs. Anything that made the two paths merely
//! *resemble* each other would be a bug waiting to happen, so both go through
//! [`SqlExecutor::plan_pinned`](crate::SqlExecutor::plan_pinned) and neither
//! builds a session of its own.
//!
//! # The two-RPC problem, and the pin (review F18)
//!
//! Flight SQL splits one query across two RPCs. `GetFlightInfo` plans it and
//! hands back an opaque ticket; `DoGet` redeems the ticket and streams the
//! results, possibly minutes later and possibly on another process. A `DoGet`
//! that re-resolved the snapshot would observe a different committed segment
//! set than `GetFlightInfo` planned against, so one query could return two
//! different answers across its own two RPCs.
//!
//! So `GetFlightInfo` resolves exactly once and pins the resolved snapshot
//! into the ticket ([`crate::FlightTicket`]), and `DoGet` rebuilds that exact
//! snapshot from the ticket. There is no second `Catalog::resolve` anywhere on
//! the redemption path.
//!
//! # Tenancy is never taken from the ticket
//!
//! The ticket carries a tenant field, but it is not a trust boundary: a ticket
//! is bytes a client holds and can replay. Every method resolves the
//! authoritative tenant from the caller's gRPC metadata through
//! [`FlightAuth`], and `DoGet` additionally compares that tenant against the
//! ticket's embedded tenant and rejects a mismatch with `PERMISSION_DENIED`
//! before touching the pinned snapshot. A ticket minted for tenant A and
//! presented with tenant B's credentials reads nothing.
//!
//! The catalog/metadata methods resolve the tenant too, before returning
//! anything. There is exactly one logical table (`samples`) per tenant and the
//! metadata answers are therefore identical for every tenant, but "the answer
//! happens to be constant" is not a reason to answer an unauthenticated
//! caller: default-deny is the invariant, not a consequence of the data
//! (review F17).
//!
//! # Deadline and the GC protection horizon
//!
//! A ticket's `deadline_ns` bounds the *whole* two-RPC flow: it is set at
//! `GetFlightInfo` to `now_ns + min(request deadline, GC protection horizon)`
//! and is not extended at `DoGet`. Two consequences, both intended:
//!
//! - A client cannot enlarge its wall budget by sitting on the ticket. `DoGet`
//!   runs with whatever is left, and a slow consumer that crosses the deadline
//!   mid-stream fails `SnapshotInvalidated` rather than receiving batches read
//!   under a pin the GC no longer protects.
//! - The pin never outlives the guarantee that its segments still exist. A
//!   pinned segment is protected until the GC protection horizon
//!   (`protection_horizon >= max_query_duration + grace`,
//!   docs/consistency-model.md "Deletion and GC"), so a ticket redeemed before
//!   its deadline never races superseded-input or retention GC.
//!
//! # Scope: prepared statements are not implemented
//!
//! Every prepared-statement method answers `UNIMPLEMENTED`
//! (docs/arrow-datafusion-plan.md Phase C). Statement execution and the
//! catalog/metadata surface are the v1 Flight SQL contract.

mod metadata;
mod request;
mod service;
mod stream;

use std::sync::Arc;
use std::time::Duration;

use ravel_types::{CommitToken, TenantHash};
use tonic::Status;
use tonic::metadata::MetadataMap;

pub use metadata::{CATALOG_NAME, SCHEMA_NAME, TABLE_TYPE};
pub use request::{END_KEY, START_KEY, TIMEOUT_KEY};
pub use service::RavelFlightSqlService;

/// The injected wall clock the Flight path reads.
///
/// Library logic never calls `SystemTime::now()` (CLAUDE.md "Time is
/// injected"); the service reads this once per RPC and threads the value
/// through resolve, ticket minting, and expiry checks, so a test can drive
/// ticket expiry deterministically instead of sleeping.
///
/// This restates the one-method contract `ravel_ingest::Clock` already
/// defines rather than depending on it: ravel-sql is the datafusion isolation
/// boundary (ADR-0013) and linking the ingest crate into it to reach a
/// single-method trait would trade a real structural property for a saved
/// six lines. services/ravel-server adapts its `ravel_ingest::Clock` to this
/// in one impl.
pub trait FlightClock: Send + Sync + 'static {
    /// Wall-clock nanoseconds since the Unix epoch.
    fn now_ns(&self) -> i64;
}

/// Authoritative request identity, resolved from gRPC metadata.
///
/// Implemented by the deployment (services/ravel-server), which owns the
/// tenant resolver and the metadata-to-header translation. ravel-sql states
/// only what it needs and never sees a credential.
pub trait FlightAuth: Send + Sync + 'static {
    /// The tenant this request authenticates as, or a rejection.
    ///
    /// This is the only tenant any Flight method may act on. Returning `Err`
    /// must deny the request; there is no anonymous fallback.
    fn tenant(&self, metadata: &MetadataMap) -> Result<TenantHash, Status>;

    /// The read-your-write commit tokens attached to the request, in the
    /// order given. Zero tokens is the normal case.
    fn min_commit_tokens(&self, metadata: &MetadataMap) -> Result<Vec<CommitToken>, Status>;
}

/// Default cap on how long a Flight ticket may stay valid, standing in for
/// the deployment's GC protection horizon.
///
/// docs/consistency-model.md requires `protection_horizon >=
/// max_query_duration + grace` with a 24 h default `grace`, and there is no
/// process-wide GC configuration in the codebase yet to read the real figure
/// from. 24 h is therefore a conservative default *ceiling*, not a tuned
/// value: it is only ever an upper bound on a ticket's life, and the request
/// deadline (seconds) is what actually decides it in every normal case. A
/// deployment that configures a shorter horizon must lower
/// [`FlightSqlConfig::gc_protection_horizon`] to match, or a very long
/// per-request deadline could mint a ticket outliving its pin's protection.
pub const DEFAULT_GC_PROTECTION_HORIZON: Duration = Duration::from_secs(24 * 60 * 60);

/// Deployment knobs for the Flight SQL service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlightSqlConfig {
    /// Server wall-deadline ceiling. A request's own timeout may only lower
    /// it, never raise it (the rule finding a7-F01 established for the HTTP
    /// endpoint, applied to this transport).
    pub max_deadline: Duration,
    /// Upper bound on a ticket's validity, the deployment's GC protection
    /// horizon. See [`DEFAULT_GC_PROTECTION_HORIZON`].
    pub gc_protection_horizon: Duration,
    /// Event-time window used when the request names none. Bounds which
    /// commits are listed; every predicate is still re-applied above the
    /// scan, so this never changes a row's value, only which segments are
    /// candidates.
    pub default_window: Duration,
}

impl Default for FlightSqlConfig {
    fn default() -> Self {
        FlightSqlConfig {
            max_deadline: ravel_query::EngineConfig::default().deadline,
            gc_protection_horizon: DEFAULT_GC_PROTECTION_HORIZON,
            default_window: Duration::from_secs(60 * 60),
        }
    }
}

impl FlightSqlConfig {
    /// The absolute deadline a ticket minted at `now_ns` for a request whose
    /// (already clamped) wall deadline is `deadline` must carry.
    ///
    /// `min` of the two bounds: the request deadline is the budget, the GC
    /// horizon is the hard ceiling past which the pin is no longer protected.
    /// Saturating arithmetic so an absurd clock reading cannot wrap the
    /// deadline into the past or the far future.
    fn ticket_deadline_ns(&self, now_ns: i64, deadline: Duration) -> i64 {
        let ttl = deadline.min(self.gc_protection_horizon);
        let ttl_ns = i64::try_from(ttl.as_nanos()).unwrap_or(i64::MAX);
        now_ns.saturating_add(ttl_ns)
    }
}

/// Convenience alias for the shared clock handle the service holds.
type ClockRef = Arc<dyn FlightClock>;

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn the_ticket_deadline_is_the_lesser_of_the_request_budget_and_the_gc_horizon() {
        let config = FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            gc_protection_horizon: Duration::from_secs(10),
            default_window: Duration::from_secs(3600),
        };
        // Request budget below the horizon: the budget decides.
        assert_eq!(
            config.ticket_deadline_ns(1_000, Duration::from_secs(5)),
            1_000 + 5_000_000_000
        );
        // Request budget above the horizon: the horizon decides, so no
        // ticket can outlive the protection its pin depends on.
        assert_eq!(
            config.ticket_deadline_ns(1_000, Duration::from_secs(30)),
            1_000 + 10_000_000_000
        );
    }

    #[test]
    fn an_extreme_clock_reading_cannot_wrap_the_ticket_deadline() {
        let config = FlightSqlConfig::default();
        assert_eq!(
            config.ticket_deadline_ns(i64::MAX, Duration::from_secs(5)),
            i64::MAX
        );
    }
}
