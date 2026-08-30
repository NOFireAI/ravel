//! Acceptance proof for request-budget parity: the cross-cluster federation
//! coordinator path (`QueryEngine::federate_discovery`, ADR-0071/ADR-0073)
//! re-enforces the `max_s3_requests` request budget over the folded
//! local+remote spend, the same way `federate_scalar` and the distributed
//! sample-fetch path do.
//!
//! Flip-line proof: `crates/ravel-query/src/engine.rs`'s `federate_discovery`
//! calls `bytes_scanned_exceeded(...)` right after folding the federation
//! outcome into `accounting`, and immediately follows it with
//! `segment_admission::request_budget_exceeded(...)`. Deleting that second
//! block makes
//! `federated_series_over_request_budget_trips_typed_error` below fail: the
//! query that must return `QueryError::RequestBudgetExceeded` instead returns
//! `Ok` with an empty series list, because only the bytes budget was checked.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_promql::LabelMatcher;
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::distrib::client::{DistribError, SliceFetcher, SliceResponse};
use ravel_query::distrib::{Federation, RemoteCluster};
use ravel_query::{
    EngineConfig, PhaseAccountingSnapshot, QueryEngine, QueryError, QueryPhase, RequestLimit,
};
use ravel_types::{METRIC_NAME_LABEL, TenantId, TimeRange};

const NS: i64 = 1_000_000_000;
const WINDOW_END_NS: i64 = 1_000 * NS;

/// A remote that always answers `Ok` with no series but a crafted accounting
/// snapshot: `requests` S3 requests and `bytes` S3 bytes. Lets the test drive
/// the coordinator's post-fold budget re-check independent of what a real
/// remote resolve would cost.
struct FixedCostFetcher {
    requests: u64,
    bytes: u64,
}

#[async_trait]
impl SliceFetcher for FixedCostFetcher {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        // A remote's spend arrives split by phase (issue #959). This double
        // reports it all on `scan`, the phase a segment data read lands on;
        // the budget re-check under test reads `pooled()`, so which phase
        // carries it does not change what is enforced.
        let mut phase_accounting = PhaseAccountingSnapshot::default();
        let scan = phase_accounting.phase_mut(QueryPhase::Scan);
        scan.s3_requests[0] = self.requests;
        scan.s3_bytes[0] = self.bytes;
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            phase_accounting,
            stats: Default::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::Ok,
            status_message: String::new(),
        })
    }
}

/// Builds an engine over a tenant with no local data (so the local resolve
/// and `fetch_all_series` fan-out contribute a negligible, near-zero request
/// count of their own) and one federated remote reporting `remote_requests`
/// S3 requests and `remote_bytes` S3 bytes. `max_s3_requests` is the only
/// bounded budget: `max_bytes_scanned` stays at its `Unlimited` default so a
/// trip can only come from the request-budget re-check under test.
fn engine_with_remote(
    remote_requests: u64,
    remote_bytes: u64,
    max_s3_requests: RequestLimit,
) -> QueryEngine {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let config = EngineConfig {
        max_s3_requests,
        ..Default::default()
    };
    let federation = Federation::new(vec![RemoteCluster {
        name: "eu-west".to_string(),
        fetcher: Arc::new(FixedCostFetcher {
            requests: remote_requests,
            bytes: remote_bytes,
        }),
        skip_unavailable: false,
        soft_timeout: Duration::from_secs(5),
    }]);
    QueryEngine::new(catalog, store, config).with_federation(Arc::new(federation))
}

/// A federated `/api/v1/series`-style discovery query whose merged local+remote
/// `total_s3_requests()` exceeds `max_s3_requests`, while `total_s3_bytes()`
/// stays far under the (unlimited) bytes budget, must return the typed
/// `RequestBudgetExceeded` the base (non-federated) path already returns for
/// the same overage -- not succeed because only the bytes budget was
/// re-checked on this coordinator path.
#[tokio::test]
async fn federated_series_over_request_budget_trips_typed_error() {
    // The remote alone reports 50 requests; the local empty-tenant resolve
    // adds at most a handful more. Either way the merged total clears a
    // budget of 1, and the remote's reported bytes (10) stay far under the
    // default-unlimited bytes budget, so a bytes-only re-check would let this
    // query through.
    let engine = engine_with_remote(50, 10, RequestLimit::Bounded(1));

    let tenant_hash = TenantId::new("tenant-936".to_string()).hash();
    let matchers = vec![LabelMatcher::equal(METRIC_NAME_LABEL, "m")];
    let window = TimeRange {
        start_ns: 0,
        end_ns: WINDOW_END_NS,
    };

    let result = engine
        .resolve_series(
            tenant_hash,
            &matchers,
            window,
            &[],
            WINDOW_END_NS,
            Duration::from_secs(30),
        )
        .await;

    match result {
        Err(QueryError::RequestBudgetExceeded { requests, max }) => {
            assert!(
                requests > max,
                "typed error must name an overage: requests {requests} <= max {max}"
            );
            assert_eq!(max, 1, "max must be the configured max_s3_requests");
        }
        other => panic!(
            "expected QueryError::RequestBudgetExceeded (before this fix, \
             federate_discovery only re-checked the bytes budget and this returned {other:?})"
        ),
    }
}

/// Companion no-regression case: the identical federated query under a
/// request budget the merged local+remote spend does not exceed succeeds
/// normally. Proves the new re-check does not change results for an
/// under-budget federated query.
#[tokio::test]
async fn federated_series_under_request_budget_succeeds() {
    let engine = engine_with_remote(50, 10, RequestLimit::Bounded(10_000));

    let tenant_hash = TenantId::new("tenant-936-ok".to_string()).hash();
    let matchers = vec![LabelMatcher::equal(METRIC_NAME_LABEL, "m")];
    let window = TimeRange {
        start_ns: 0,
        end_ns: WINDOW_END_NS,
    };

    let result = engine
        .resolve_series(
            tenant_hash,
            &matchers,
            window,
            &[],
            WINDOW_END_NS,
            Duration::from_secs(30),
        )
        .await;

    assert!(
        result.is_ok(),
        "an under-budget federated query must not be rejected: {result:?}"
    );
    let (series, _coverage) = result.expect("checked is_ok above");
    assert!(
        series.is_empty(),
        "the remote and the empty local tenant both report zero series"
    );
}
