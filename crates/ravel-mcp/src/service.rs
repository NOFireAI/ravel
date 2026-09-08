//! The D1 port trait `ravel-server` implements in #1381, mirroring
//! `services/ravel-server`'s `QueryService` operations.
//!
//! `QueryService` is a concrete struct there, not a trait, and two of its
//! nine operations (`analytics`, `exemplars`) take request/outcome types
//! (`AnalyticsRequest`, `AnalyticsOutcome`, `ExemplarsRequest`,
//! `ExemplarsOutcome`) defined in that same crate. `ravel-mcp` cannot import
//! any of the four: #1381 makes `ravel-server` depend on `ravel-mcp`, so the
//! reverse import would cycle. [`QueryBackend`] is the port side of that
//! adapter pair instead: one trait, one method per `QueryService` operation,
//! named and ordered identically, that #1381 implements for the real
//! `QueryService`. Seven of the nine methods take the exact request type
//! `QueryService` takes -- all seven already live in `ravel_query`, an
//! existing dependency of this crate. `analytics` and `exemplars` take
//! [`crate::envelope::AnyJson`] instead, since their concrete request shape
//! is the one unreachable pair; #1381's adapter is where that JSON becomes a
//! real `AnalyticsRequest`/`ExemplarsRequest`.
//!
//! Every method returns the D4 envelope directly (`Result<Envelope,
//! Failure>`) rather than each operation's native outcome type. Turning a
//! native outcome into an envelope is exactly what every tool body (#1380,
//! #1382) needs regardless of which operation answered it, so returning nine
//! different outcome types here would only move that same conversion to a
//! later, harder-to-name place; the port trait states the shape the tool
//! layer actually consumes.
//!
//! No method has a body: this module only names the shape #1381's adapter
//! must implement and #1380/#1382's tool bodies must call.

use ravel_query::http::{InstantRequest, MetadataRequest, RangeRequest};
use ravel_types::TenantHash;

use crate::envelope::{AnyJson, Envelope, Failure};

/// The nine `QueryService` operations, as the tool layer needs them.
///
/// The lint allow is intentional: this trait is a crate-internal seam with
/// exactly one production implementer (#1381's adapter over the real
/// `QueryService`), called directly and never behind `dyn`, so it needs no
/// `Send`-bounded return future -- the only thing `async_fn_in_trait` warns
/// about. `crates/ravel-maintain/src/codec.rs`'s `SegmentCodec` documents the
/// same reasoning for the same shape of trait.
#[allow(async_fn_in_trait)]
pub trait QueryBackend {
    /// Mirrors `QueryService::promql_instant`.
    async fn promql_instant(
        &self,
        tenant_hash: TenantHash,
        request: &InstantRequest,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::promql_range`.
    async fn promql_range(
        &self,
        tenant_hash: TenantHash,
        request: &RangeRequest,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::labels`.
    async fn labels(&self, tenant_hash: TenantHash, request: &MetadataRequest) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::label_values`.
    async fn label_values(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
        name: &str,
        include_log_metric_names: bool,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::series`.
    async fn series(&self, tenant_hash: TenantHash, request: &MetadataRequest) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::analytics`. Its native `AnalyticsRequest` is
    /// defined in `services/ravel-server`, unreachable from this crate (see
    /// the module doc); the caller-defined shape crosses as opaque JSON
    /// instead, for #1381's adapter to convert.
    async fn analytics(&self, tenant_hash: TenantHash, request: &AnyJson) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::exemplars`. Same reachability note as
    /// `analytics`.
    async fn exemplars(&self, tenant_hash: TenantHash, request: &AnyJson) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::sql_execute`.
    async fn sql_execute(&self, tenant_hash: TenantHash, request: &ravel_sql::SqlRequest) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::sql_explain`.
    async fn sql_explain(&self, tenant_hash: TenantHash, request: &ravel_sql::SqlRequest) -> Result<Envelope, Failure>;
}
