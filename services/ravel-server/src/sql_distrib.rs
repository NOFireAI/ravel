//! ADR-0071 SQL-lane server wiring: the deployment's
//! implementation of ravel-sql's [`WorkerEndpoints`] trait over the ravel-fleet
//! query-worker registry, and the [`DistributedFlightConfig`] a coordinator
//! would install on the Flight SQL service under `--distributed-query`.
//!
//! ravel-sql states only what it needs ([`WorkerEndpoints`]); real membership
//! lives here, in the server crate. [`FleetWorkerEndpoints`] reads the same live
//! query-worker set the PromQL distributed lane's [`crate::distrib`] router
//! reads (written by the heartbeat loop under `sys/query/workers/`), filtered to
//! the coordinator's protocol version and with the coordinator itself removed,
//! and returns each remaining live worker's slice `DoGet` location.
//!
//! # Where a slice `DoGet` goes
//!
//! The [`QueryWorkerRecord`] carries two addresses, and which one the SQL lane
//! dials depends on this process's [`SliceTransport`]:
//!
//! - With `--fragment-listener` ([`SliceTransport::FragmentTls`], ADR-1689
//!   decision 1), [`QueryWorkerRecord::fragment_endpoint`] names the dedicated
//!   TLS listener, which mounts the Flight service in the `SliceOnly` role
//!   beside `SeriesFetch`. The lane dials `https://{fragment_endpoint}` with the
//!   same pinned-CA client configuration the PromQL lane builds
//!   ([`crate::distrib::fragment_client_tls`]). The worker's public gRPC
//!   listener refuses a slice ticket in this layout, so
//!   [`QueryWorkerRecord::flight_sql_endpoint`] is never dialed for a slice.
//! - Without it ([`SliceTransport::PublicPlaintext`], release A only), the
//!   public gRPC listener serves both the client surface and slices
//!   (`Combined`), and the lane dials `http://{flight_sql_endpoint}` in
//!   plaintext.
//!
//! A record that carries no address for the transport in use is not dialed:
//! its slices run on the remaining workers, or coordinator-local.
//!
//! # Install seam
//!
//! The [`DistributedFlightConfig`] this module builds is installed on the
//! Flight SQL service through `RavelFlightSqlService::with_distributed_scan`
//! (ADR-0071): the server registration site
//! ([`crate::flight::service`]) passes the config built here when
//! `--distributed-query` is on in a query-serving mode. On a positive cost
//! gate, `do_get_statement` mints slice tickets and fans the samples scan out
//! to the workers this roster resolves. Absent the config, the service runs
//! every statement whole-set on the coordinator, byte-identical to before.

use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;
use ravel_fleet::query_workers::QueryWorkerRecord;
use ravel_query::distrib::codec::PROTOCOL_VERSION;
use ravel_sql::{DistributedFlightConfig, SqlTicketKeys, WorkerEndpoints};
use uuid::Uuid;

use crate::config::DistribSettings;

/// The shared live query-worker set, refreshed by the heartbeat loop. The same
/// handle [`crate::distrib::RoutingSliceFetcher`] reads for the PromQL lane.
type LiveWorkers = Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>>;

/// This process's own query-worker id, published once the gRPC listener is bound
/// and the heartbeat identity exists. The same cell
/// [`crate::distrib::RoutingSliceFetcher`] reads to recognize a self-mapped
/// slice; empty until then.
type SelfId = Arc<OnceLock<Uuid>>;

/// How this coordinator reaches a worker's slice `DoGet` (ADR-1689 decision
/// 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SliceTransport {
    /// No `--fragment-listener`: plaintext to the worker's public gRPC listener
    /// ([`QueryWorkerRecord::flight_sql_endpoint`]), which serves slices in the
    /// `Combined` role. Removed in release B (ADR-1689 decision 4).
    PublicPlaintext,
    /// `--fragment-listener`: TLS to the worker's dedicated fragment listener
    /// ([`QueryWorkerRecord::fragment_endpoint`]), which serves slices in the
    /// `SliceOnly` role.
    FragmentTls,
}

impl SliceTransport {
    /// The transport a process with `settings` dials slices over.
    pub fn for_settings(settings: &DistribSettings) -> Self {
        if settings.fragment_listener.is_some() {
            SliceTransport::FragmentTls
        } else {
            SliceTransport::PublicPlaintext
        }
    }

    /// The address in `record` this transport dials, empty when the record
    /// carries none.
    fn address(self, record: &QueryWorkerRecord) -> &str {
        match self {
            SliceTransport::PublicPlaintext => &record.flight_sql_endpoint,
            SliceTransport::FragmentTls => &record.fragment_endpoint,
        }
    }

    fn scheme(self) -> &'static str {
        match self {
            SliceTransport::PublicPlaintext => "http",
            SliceTransport::FragmentTls => "https",
        }
    }
}

/// A [`WorkerEndpoints`] over the ravel-fleet query-worker registry (ADR-0071
/// SQL lane). Returns the slice `DoGet` location of every live,
/// protocol-matched worker OTHER than this coordinator, in the registry's
/// order. An empty result means no workers are available, and ravel-sql runs
/// the query fully local (a single self-endpoint over the whole pinned set),
/// which is always correct.
pub struct FleetWorkerEndpoints {
    live_workers: LiveWorkers,
    self_id: SelfId,
    transport: SliceTransport,
}

impl FleetWorkerEndpoints {
    /// Build over the shared live-worker set and the shared self-id cell (the
    /// same two the PromQL router and the heartbeat loop share), dialing over
    /// `transport`.
    pub fn new(live_workers: LiveWorkers, self_id: SelfId, transport: SliceTransport) -> Self {
        FleetWorkerEndpoints {
            live_workers,
            self_id,
            transport,
        }
    }

    /// The slice location for a worker record under this transport.
    fn location(&self, record: &QueryWorkerRecord) -> String {
        format!(
            "{}://{}",
            self.transport.scheme(),
            self.transport.address(record)
        )
    }
}

impl WorkerEndpoints for FleetWorkerEndpoints {
    fn endpoints(&self) -> Vec<String> {
        let live = Arc::clone(&self.live_workers.read());
        // `QueryWorkers::live_set` always includes this process ("a process
        // never disowns itself"), so the coordinator's own record is in the
        // roster unless it is dropped here.
        let own = self.self_id.get().map(Uuid::to_string);
        live.iter()
            // A version-skewed worker is dropped here, exactly as the PromQL
            // router drops it at routing time: a worker on another protocol
            // version may serve slices on another listener (a version 4 worker
            // refuses nothing on its public listener and serves no slices on its
            // dedicated one), so its slices run coordinator-local instead.
            .filter(|record| record.protocol_version == PROTOCOL_VERSION)
            // A record with no address for this transport is not dispatchable
            // (`flight_sql_endpoint` is `#[serde(default)]`, so an older
            // writer's record decodes with an empty string); dialing "http://"
            // or "https://" would only fail the slice.
            .filter(|record| !self.transport.address(record).is_empty())
            // The coordinator serves its own slices through the local path
            // (ravel_sql::distributed::CoordinatorSliceReader, the last step of
            // the fan-out's failure sequence), so dispatching one to itself over
            // Flight is a wasted hop: an extra connection, an extra ticket MAC,
            // and an extra encode/decode round for bytes it can read directly.
            // Before the self-id cell is populated (the identity exists only
            // once the gRPC listener has bound) nothing is excluded, which is
            // the pre-existing behavior and is correct, just one hop slower.
            .filter(|record| own.as_deref() != Some(record.process_id.as_str()))
            .map(|record| self.location(record))
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
/// `self_id` is the shared cell holding this process's own query-worker id (the
/// one the PromQL router reads to recognize a self-mapped slice). It is what
/// keeps the coordinator out of its own SQL roster.
///
/// `transport` picks which address of each worker record a slice dials
/// ([`SliceTransport`]).
///
/// `legacy_secret` is the release A fallback (ADR-1689 decision 4) for a
/// process without `--sql-ticket-key-file`: the cluster secret every process
/// already shares ([`DistribSettings::sql_ticket_secret`], derived from the
/// first fragment key), from which the Flight ticket MAC key is derived so a
/// coordinator's slice ticket verifies on the worker that redeems it. `None`
/// derives nothing: the caller installs keys from the SQL ticket key file
/// instead (see [`distributed_flight_setup`]).
pub fn distributed_flight_config(
    live_workers: LiveWorkers,
    self_id: SelfId,
    transport: SliceTransport,
    thresholds: ravel_query::distrib::partition::DistribThresholds,
    legacy_secret: Option<&str>,
) -> DistributedFlightConfig {
    DistributedFlightConfig {
        workers: Arc::new(FleetWorkerEndpoints::new(live_workers, self_id, transport)),
        thresholds,
        shared_ticket_key: legacy_secret
            .map(|secret| ravel_sql::derive_ticket_key(secret.as_bytes())),
    }
}

/// The SQL lane's distributed config plus the ticket keys to install on the
/// Flight SQL service, from this process's [`DistribSettings`]. The roster
/// dials over [`SliceTransport::for_settings`].
///
/// With `--sql-ticket-key-file` (ADR-1689 decision 2) the keys are
/// [`SqlTicketKeys::from_file_keys`] over every key in the file and nothing is
/// derived from the fragment keys. Without it (release A only) the keys are
/// `None` and the config carries the key derived from
/// [`DistribSettings::sql_ticket_secret`], so a fleet rolling onto this release
/// keeps agreeing on one ticket key.
///
/// An empty key list is an error rather than `None`: `None` would leave the
/// service on its per-process random keys, and every slice ticket would then
/// fail every other process's MAC.
pub fn distributed_flight_setup(
    live_workers: LiveWorkers,
    self_id: SelfId,
    settings: &DistribSettings,
) -> anyhow::Result<(DistributedFlightConfig, Option<SqlTicketKeys>)> {
    let transport = SliceTransport::for_settings(settings);
    match settings.sql_ticket_keys.as_deref() {
        Some(file_keys) => {
            let keys = SqlTicketKeys::from_file_keys(file_keys).ok_or_else(|| {
                anyhow::anyhow!("--sql-ticket-key-file resolved to no keys; it needs at least one")
            })?;
            Ok((
                distributed_flight_config(
                    live_workers,
                    self_id,
                    transport,
                    settings.thresholds,
                    None,
                ),
                Some(keys),
            ))
        }
        None => Ok((
            distributed_flight_config(
                live_workers,
                self_id,
                transport,
                settings.thresholds,
                Some(&settings.sql_ticket_secret()),
            ),
            None,
        )),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A record whose Flight SQL endpoint is `endpoint` and whose
    /// `fragment_endpoint` is a DIFFERENT address, so a test that expects the SQL
    /// location fails if `location` ever reads `fragment_endpoint` again (the
    /// #1296 defect). The fragment endpoint is spelled as a TLS-only port to
    /// mirror the `--fragment-listener` layout that produced the bug.
    fn record(process_id: &str, endpoint: &str, protocol_version: u32) -> QueryWorkerRecord {
        QueryWorkerRecord {
            process_id: process_id.to_string(),
            fragment_endpoint: format!("{endpoint}-fragment-tls"),
            flight_sql_endpoint: endpoint.to_string(),
            protocol_version,
            started_unix_ns: 0,
        }
    }

    /// An empty self-id cell, for the cases that are not about self-exclusion.
    fn no_self() -> SelfId {
        Arc::new(OnceLock::new())
    }

    /// A populated self-id cell.
    fn self_id(id: Uuid) -> SelfId {
        let cell: SelfId = Arc::new(OnceLock::new());
        cell.set(id).expect("set self id");
        cell
    }

    /// The endpoints reflect the live set, in order, and drop version-skewed
    /// workers, mirroring the PromQL router's routing-time version filter.
    #[test]
    fn endpoints_reflect_live_set_and_drop_version_skew() {
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(Vec::new())));
        let endpoints =
            FleetWorkerEndpoints::new(live.clone(), no_self(), SliceTransport::PublicPlaintext);

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

    /// The coordinator's own record is dropped from its SQL roster: it serves
    /// its own slices through the local path, so a slice dispatched to itself
    /// over Flight would be a wasted hop. `QueryWorkers::live_set` always puts
    /// this process in the live set, so without this filter the coordinator is
    /// always in the roster it fans out to.
    #[test]
    fn endpoints_exclude_the_coordinator_itself() {
        let current = PROTOCOL_VERSION;
        let me = Uuid::from_u128(1);
        let sibling = Uuid::from_u128(2);
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![
            record(&me.to_string(), "10.0.0.1:9000", current),
            record(&sibling.to_string(), "10.0.0.2:9000", current),
        ])));

        // With the self-id cell populated, only the sibling is dispatchable.
        let endpoints =
            FleetWorkerEndpoints::new(live.clone(), self_id(me), SliceTransport::PublicPlaintext);
        assert_eq!(
            endpoints.endpoints(),
            vec!["http://10.0.0.2:9000".to_string()],
            "the coordinator's own endpoint is not a fan-out target"
        );

        // A single-node cluster fans out to nothing at all, so
        // `plan_distributed_slices` returns None and the statement runs
        // whole-set locally instead of dispatching every slice to itself.
        *live.write() = Arc::new(vec![record(&me.to_string(), "10.0.0.1:9000", current)]);
        assert!(
            endpoints.endpoints().is_empty(),
            "a lone coordinator advertises no workers"
        );

        // Before the identity exists (the cell is filled once the gRPC listener
        // binds), nothing is excluded: correct, one hop slower.
        let early =
            FleetWorkerEndpoints::new(live.clone(), no_self(), SliceTransport::PublicPlaintext);
        assert_eq!(
            early.endpoints(),
            vec!["http://10.0.0.1:9000".to_string()],
            "an unpopulated self-id cell excludes nothing"
        );
    }

    /// A record whose two endpoints differ pins which field each transport
    /// reads. With `--fragment-listener` the slice goes to the dedicated TLS
    /// listener over `https`, never to the public gRPC listener, which refuses
    /// slice tickets in that layout (ADR-1689 decision 1). Without it the slice
    /// goes to the public gRPC listener over plaintext, never to a
    /// `fragment_endpoint` that may name a TLS-only port (the #1296 defect).
    #[test]
    fn each_transport_dials_its_own_endpoint() {
        let current = PROTOCOL_VERSION;
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
            process_id: "a".to_string(),
            fragment_endpoint: "10.0.0.1:9443".to_string(),
            flight_sql_endpoint: "10.0.0.1:9000".to_string(),
            protocol_version: current,
            started_unix_ns: 0,
        }])));
        let tls = FleetWorkerEndpoints::new(live.clone(), no_self(), SliceTransport::FragmentTls);
        assert_eq!(
            tls.endpoints(),
            vec!["https://10.0.0.1:9443".to_string()],
            "with --fragment-listener the SQL lane dials the dedicated listener over TLS"
        );
        let plaintext = FleetWorkerEndpoints::new(live, no_self(), SliceTransport::PublicPlaintext);
        assert_eq!(
            plaintext.endpoints(),
            vec!["http://10.0.0.1:9000".to_string()],
            "without it the SQL lane dials the public gRPC listener"
        );
    }

    /// The transport follows `--fragment-listener`, and the setup's roster
    /// dials over it.
    #[test]
    fn setup_dials_the_fragment_endpoint_when_a_fragment_listener_is_configured() {
        let mut with_listener = settings(Some(vec![NEW_KEY]));
        with_listener.fragment_listener = Some(crate::config::FragmentListenerSettings {
            addr: "127.0.0.1:0".parse().expect("addr"),
            tls_cert_pem: Vec::new(),
            tls_key_pem: Vec::new(),
            tls_ca_pem: Vec::new(),
        });
        assert_eq!(
            SliceTransport::for_settings(&with_listener),
            SliceTransport::FragmentTls
        );
        assert_eq!(
            SliceTransport::for_settings(&settings(Some(vec![NEW_KEY]))),
            SliceTransport::PublicPlaintext
        );
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![record(
            "a",
            "10.0.0.1:9000",
            PROTOCOL_VERSION,
        )])));
        let (config, _) = distributed_flight_setup(live, no_self(), &with_listener).expect("setup");
        assert_eq!(
            config.workers.endpoints(),
            vec!["https://10.0.0.1:9000-fragment-tls".to_string()]
        );
    }

    /// A worker whose record names no fragment endpoint is not dialed for
    /// slices under `--fragment-listener`: its slices run elsewhere, or
    /// coordinator-local.
    #[test]
    fn worker_without_fragment_endpoint_is_dropped_under_the_tls_transport() {
        let current = PROTOCOL_VERSION;
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![
            QueryWorkerRecord {
                process_id: "no-fragment".to_string(),
                fragment_endpoint: String::new(),
                flight_sql_endpoint: "10.0.0.9:9000".to_string(),
                protocol_version: current,
                started_unix_ns: 0,
            },
            record("new", "10.0.0.2:9000", current),
        ])));
        let endpoints = FleetWorkerEndpoints::new(live, no_self(), SliceTransport::FragmentTls);
        assert_eq!(
            endpoints.endpoints(),
            vec!["https://10.0.0.2:9000-fragment-tls".to_string()],
            "the record without a fragment endpoint is not a slice target"
        );
    }

    /// ADR-1689 decision 3: a version 5 coordinator routes no slice to a
    /// version 4 worker on either transport. A version 4 worker serves slices
    /// only on its public listener, which a version 5 coordinator with
    /// `--fragment-listener` does not dial, so its slices run coordinator-local.
    #[test]
    fn a_version_five_coordinator_routes_no_slice_to_a_version_four_worker() {
        assert_eq!(PROTOCOL_VERSION, 5);
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![
            record("v4", "10.0.0.4:9000", 4),
            record("v5", "10.0.0.5:9000", 5),
        ])));
        for (transport, want) in [
            (
                SliceTransport::FragmentTls,
                "https://10.0.0.5:9000-fragment-tls",
            ),
            (SliceTransport::PublicPlaintext, "http://10.0.0.5:9000"),
        ] {
            let endpoints = FleetWorkerEndpoints::new(live.clone(), no_self(), transport);
            assert_eq!(
                endpoints.endpoints(),
                vec![want.to_string()],
                "{transport:?}: only the version 5 worker is a slice target"
            );
        }
    }

    /// A worker that advertises no Flight SQL endpoint (a pre-amendment record,
    /// `flight_sql_endpoint` defaulted empty) is dropped from the SQL roster
    /// rather than dialed as `http://`.
    #[test]
    fn worker_without_flight_sql_endpoint_is_dropped() {
        let current = PROTOCOL_VERSION;
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![
            QueryWorkerRecord {
                process_id: "old".to_string(),
                fragment_endpoint: "10.0.0.9:9443".to_string(),
                flight_sql_endpoint: String::new(),
                protocol_version: current,
                started_unix_ns: 0,
            },
            record("new", "10.0.0.2:9000", current),
        ])));
        let endpoints = FleetWorkerEndpoints::new(live, no_self(), SliceTransport::PublicPlaintext);
        assert_eq!(
            endpoints.endpoints(),
            vec!["http://10.0.0.2:9000".to_string()],
            "only the worker advertising a Flight SQL endpoint is dispatchable"
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
        let config = distributed_flight_config(
            live,
            no_self(),
            SliceTransport::PublicPlaintext,
            thresholds,
            Some("cluster-secret"),
        );
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

    const FRAGMENT_KEY: [u8; 32] = [0xab; 32];
    const OLD_KEY: [u8; 32] = [0x01; 32];
    const NEW_KEY: [u8; 32] = [0x02; 32];

    fn settings(sql_ticket_keys: Option<Vec<[u8; 32]>>) -> DistribSettings {
        DistribSettings {
            fragment_keys: vec![FRAGMENT_KEY],
            sql_ticket_keys,
            max_inflight_fragments: 1,
            max_inflight_federated_resolves: 1,
            thresholds: ravel_query::distrib::partition::DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 4,
            },
            fragment_listener: None,
            advertise_endpoint: None,
        }
    }

    fn slice_ticket() -> ravel_sql::FlightTicket {
        ravel_sql::FlightTicket {
            tenant: ravel_types::TenantId::new("acme").hash(),
            statement: "SELECT 1".to_string(),
            segments: Vec::new(),
            min_commit_tokens: Vec::new(),
            now_ns: 1,
            deadline_ns: 2,
            slice_index: 0,
            slice_count: 2,
            pending_erasure: Vec::new(),
            declared_columns: Vec::new(),
            parquet_tables: Vec::new(),
            budgets: None,
        }
    }

    /// With `--sql-ticket-key-file` the service gets keys over every file key
    /// and the config derives nothing from the fragment secret (ADR-1689
    /// decision 2); a `[new, old]` file mints under new and verifies a ticket
    /// minted under `[old]`, which `[new]` alone refuses.
    #[test]
    fn setup_takes_ticket_keys_from_the_sql_ticket_key_file() {
        use ravel_sql::TicketSurface;

        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(Vec::new())));
        let (config, keys) = distributed_flight_setup(
            live.clone(),
            no_self(),
            &settings(Some(vec![NEW_KEY, OLD_KEY])),
        )
        .expect("setup");
        assert_eq!(
            config.shared_ticket_key, None,
            "nothing derived from the fragment secret"
        );
        assert_eq!(config.thresholds.max_parallel_slices, 4);
        let keys = keys.expect("keys from the SQL ticket key file");
        assert_eq!(
            keys.mint_key(TicketSurface::Slice),
            &ravel_sql::derive_surface_key(&NEW_KEY, TicketSurface::Slice),
            "the first file key mints"
        );

        let minted_under_old = SqlTicketKeys::from_file_key(&OLD_KEY)
            .encode(&slice_ticket(), TicketSurface::Slice)
            .expect("encode");
        assert_eq!(
            keys.decode(&minted_under_old, TicketSurface::Slice)
                .expect("[new, old] verifies a ticket minted under old"),
            slice_ticket()
        );
        assert!(
            SqlTicketKeys::from_file_key(&NEW_KEY)
                .decode(&minted_under_old, TicketSurface::Slice)
                .is_err(),
            "[new] alone refuses it, so the old key is what verified it"
        );
    }

    /// Without the flag (release A) the service keeps its keys and the config
    /// carries the key derived from the first fragment key, as before.
    #[test]
    fn setup_without_the_sql_ticket_key_file_keeps_the_release_a_derivation() {
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(Vec::new())));
        let (config, keys) =
            distributed_flight_setup(live, no_self(), &settings(None)).expect("setup");
        assert!(keys.is_none());
        assert_eq!(
            config.shared_ticket_key,
            Some(ravel_sql::derive_ticket_key(
                hex::encode(FRAGMENT_KEY).as_bytes()
            ))
        );
    }

    /// A key file that resolved to no keys refuses startup instead of leaving
    /// the service on per-process random keys.
    #[test]
    fn setup_refuses_an_empty_sql_ticket_key_list() {
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(Vec::new())));
        let err = distributed_flight_setup(live, no_self(), &settings(Some(Vec::new())))
            .err()
            .expect("an empty key list is an error");
        assert!(
            err.to_string().contains("--sql-ticket-key-file"),
            "names the flag: {err}"
        );
    }

    /// The BLAKE3 context `ravel_sql::derive_ticket_key` uses, spelled here as
    /// the deployment guide spells it for `b3sum --derive-key`.
    const DOCUMENTED_DERIVATION_CONTEXT: &str =
        "ravel-sql flight ticket MAC key 2026-08 (RFT1 v4, ADR-0071)";

    /// The gapless switch onto `--sql-ticket-key-file` the deployment guide
    /// documents: a file holding only the key a release A node derives from its
    /// first fragment key makes the file node and the derived node agree on
    /// both surfaces, in both directions. The guide's recipe for that key
    /// (BLAKE3 derive-key over the first fragment key's lowercase hex) is
    /// pinned too.
    #[test]
    fn a_file_of_the_derived_key_agrees_with_the_release_a_derivation() {
        use ravel_sql::TicketSurface;

        let derived_key = ravel_sql::derive_ticket_key(hex::encode(FRAGMENT_KEY).as_bytes());
        assert_eq!(
            derived_key,
            blake3::derive_key(
                DOCUMENTED_DERIVATION_CONTEXT,
                hex::encode(FRAGMENT_KEY).as_bytes()
            ),
            "the documented recipe computes the key a release A node derives"
        );
        // `printf '%s' abab...ab | b3sum --derive-key "<context>" --no-names`,
        // run with b3sum 1.8.7 over this test's fragment key.
        assert_eq!(
            hex::encode(derived_key),
            "eb8273766cadffb0a66e51bd709785eb7bcf9811ec4a6968952791bccc83c691",
            "the guide's b3sum command prints the key file line"
        );

        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(Vec::new())));
        let (_, file_keys) =
            distributed_flight_setup(live.clone(), no_self(), &settings(Some(vec![derived_key])))
                .expect("setup");
        let file_node = file_keys.expect("keys from the file");
        let (derived_config, none) =
            distributed_flight_setup(live, no_self(), &settings(None)).expect("setup");
        assert!(none.is_none());
        // What `with_distributed_scan` installs from the config's key.
        let derived_node = SqlTicketKeys::from_file_key(
            &derived_config
                .shared_ticket_key
                .expect("release A carries the derived key"),
        );
        let unrelated = SqlTicketKeys::from_file_key(&NEW_KEY);

        let mut ticket = slice_ticket();
        for surface in [TicketSurface::Client, TicketSurface::Slice] {
            ticket.slice_count = if surface == TicketSurface::Slice {
                2
            } else {
                1
            };
            for (minter, verifier, direction) in [
                (&file_node, &derived_node, "file to derived"),
                (&derived_node, &file_node, "derived to file"),
            ] {
                let bytes = minter.encode(&ticket, surface).expect("encode");
                assert_eq!(
                    verifier
                        .decode(&bytes, surface)
                        .unwrap_or_else(|e| panic!("{surface:?} {direction}: {e}")),
                    ticket,
                    "{surface:?} {direction}"
                );
                assert!(
                    unrelated.decode(&bytes, surface).is_err(),
                    "{surface:?} {direction}: a different key refuses it"
                );
            }
        }
    }
}
