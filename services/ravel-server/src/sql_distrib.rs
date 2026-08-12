//! ADR-0071 SQL-lane server wiring (issue #868): the deployment's
//! implementation of ravel-sql's [`WorkerEndpoints`] trait over the ravel-fleet
//! query-worker registry, and the [`DistributedFlightConfig`] a coordinator
//! would install on the Flight SQL service under `--distributed-query`.
//!
//! ravel-sql states only what it needs ([`WorkerEndpoints`]); real membership
//! lives here, in the server crate. [`FleetWorkerEndpoints`] reads the same live
//! query-worker set the PromQL distributed lane's [`crate::distrib`] router
//! reads (written by the heartbeat loop under `sys/query/workers/`), filtered to
//! the coordinator's protocol version, and returns each live worker's endpoint
//! as a Flight location.
//!
//! # Why the fragment endpoint is the Flight location
//!
//! A worker's [`QueryWorkerRecord::fragment_endpoint`] is the `host:port` of its
//! cluster-internal gRPC listener. That one listener hosts BOTH the queryfrag
//! `SeriesFetch` service (the PromQL distributed lane) AND the Flight SQL service
//! (see the gRPC server builder in `lib.rs`, where both are added to the same
//! `tonic::transport::Server`). So the registry's fragment endpoint is exactly
//! where a worker's Flight `DoGet` also lives; the SQL lane needs no separate
//! endpoint field.
//!
//! # Install seam
//!
//! The [`DistributedFlightConfig`] this module builds is installed on the
//! Flight SQL service through `RavelFlightSqlService::with_distributed_scan`
//! (ADR-0071, issue #868): the server registration site
//! ([`crate::flight::service`]) passes the config built here when
//! `--distributed-query` is on in a query-serving mode. On a positive cost
//! gate, `do_get_statement` mints slice tickets and fans the samples scan out
//! to the workers this roster resolves. Absent the config, the service runs
//! every statement whole-set on the coordinator, byte-identical to before.

use std::sync::Arc;

use parking_lot::RwLock;
use ravel_fleet::query_workers::QueryWorkerRecord;
use ravel_query::distrib::codec::PROTOCOL_VERSION;
use ravel_sql::{DistributedFlightConfig, WorkerEndpoints};

/// The shared live query-worker set, refreshed by the heartbeat loop. The same
/// handle [`crate::distrib::RoutingSliceFetcher`] reads for the PromQL lane.
type LiveWorkers = Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>>;

/// A [`WorkerEndpoints`] over the ravel-fleet query-worker registry (ADR-0071
/// SQL lane, issue #868). Returns the Flight location of every live,
/// protocol-matched worker, in the registry's order. An empty result means no
/// workers are available, and ravel-sql runs the query fully local (a single
/// self-endpoint over the whole pinned set), which is always correct.
pub struct FleetWorkerEndpoints {
    live_workers: LiveWorkers,
}

impl FleetWorkerEndpoints {
    /// Build over the shared live-worker set (the same one the PromQL router and
    /// the heartbeat loop share).
    pub fn new(live_workers: LiveWorkers) -> Self {
        FleetWorkerEndpoints { live_workers }
    }

    /// The Flight location for a worker record: its cluster-internal gRPC
    /// listener, where the Flight SQL service is mounted alongside the queryfrag
    /// surface. The `http://` scheme matches the plaintext channel the PromQL
    /// coordinator dials the same listener over.
    fn location(record: &QueryWorkerRecord) -> String {
        format!("http://{}", record.fragment_endpoint)
    }
}

impl WorkerEndpoints for FleetWorkerEndpoints {
    fn endpoints(&self) -> Vec<String> {
        let live = Arc::clone(&self.live_workers.read());
        live.iter()
            // A version-skewed worker is dropped here, exactly as the PromQL
            // router drops it at routing time: dispatching a slice to a worker
            // that speaks a different protocol would fail the fetch.
            .filter(|record| record.protocol_version == PROTOCOL_VERSION)
            .map(Self::location)
            .collect()
    }
}

/// Build the [`DistributedFlightConfig`] a coordinator installs on the Flight
/// SQL service under `--distributed-query`: the fleet-backed worker roster plus
/// the same cost gate/fan-out width the PromQL lane uses, so both lanes gate
/// distribution on identical estimate semantics. [`crate::flight::service`]
/// installs the returned value through
/// `RavelFlightSqlService::with_distributed_scan`.
///
/// `auth_token` is the cluster-internal fragment secret every process in the
/// deployment already shares (`DistribSettings::auth_token`). The Flight ticket
/// MAC key is derived from it (ADR-0071, issue #868) so a coordinator's slice
/// ticket verifies on the worker process that redeems it; without a shared key,
/// cross-process slice fan-out would fail every ticket MAC.
pub fn distributed_flight_config(
    live_workers: LiveWorkers,
    thresholds: ravel_query::distrib::partition::DistribThresholds,
    auth_token: &str,
) -> DistributedFlightConfig {
    DistributedFlightConfig {
        workers: Arc::new(FleetWorkerEndpoints::new(live_workers)),
        thresholds,
        shared_ticket_key: Some(ravel_sql::derive_ticket_key(auth_token.as_bytes())),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn record(process_id: &str, endpoint: &str, protocol_version: u32) -> QueryWorkerRecord {
        QueryWorkerRecord {
            process_id: process_id.to_string(),
            fragment_endpoint: endpoint.to_string(),
            protocol_version,
            started_unix_ns: 0,
        }
    }

    /// The endpoints reflect the live set, in order, and drop version-skewed
    /// workers, mirroring the PromQL router's routing-time version filter.
    #[test]
    fn endpoints_reflect_live_set_and_drop_version_skew() {
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(Vec::new())));
        let endpoints = FleetWorkerEndpoints::new(live.clone());

        // Empty registry: no workers, ravel-sql runs local.
        assert!(endpoints.endpoints().is_empty());

        let current = PROTOCOL_VERSION;
        *live.write() = Arc::new(vec![
            record("a", "10.0.0.1:9000", current),
            record("b", "10.0.0.2:9000", current.wrapping_add(1)),
            record("c", "10.0.0.3:9000", current),
        ]);

        assert_eq!(
            endpoints.endpoints(),
            vec![
                "http://10.0.0.1:9000".to_string(),
                "http://10.0.0.3:9000".to_string(),
            ],
            "only version-matched workers, in registry order, as Flight locations"
        );

        // A membership change is reflected on the next read (the roster is
        // resolved per query, not snapshotted at construction).
        *live.write() = Arc::new(vec![record("a", "10.0.0.1:9000", current)]);
        assert_eq!(
            endpoints.endpoints(),
            vec!["http://10.0.0.1:9000".to_string()]
        );
    }

    /// The config builder carries the fleet roster and the supplied thresholds
    /// through unchanged.
    #[test]
    fn config_builder_carries_roster_and_thresholds() {
        let current = PROTOCOL_VERSION;
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![record(
            "a",
            "10.0.0.1:9000",
            current,
        )])));
        let thresholds = ravel_query::distrib::partition::DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 4,
        };
        let config = distributed_flight_config(live, thresholds, "cluster-secret");
        assert_eq!(
            config.workers.endpoints(),
            vec!["http://10.0.0.1:9000".to_string()]
        );
        assert_eq!(config.thresholds.max_parallel_slices, 4);
        // The ticket key is derived from the shared secret, deterministically.
        assert_eq!(
            config.shared_ticket_key,
            Some(ravel_sql::derive_ticket_key(b"cluster-secret")),
            "distributed config must carry the derived shared ticket key"
        );
    }
}
