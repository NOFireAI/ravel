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
//! and returns each remaining live worker's Flight SQL endpoint as a Flight
//! location.
//!
//! # Why the SQL lane dials `flight_sql_endpoint`, not `fragment_endpoint`
//!
//! The two distributed lanes reach a worker over two different listeners, so the
//! [`QueryWorkerRecord`] carries two addresses:
//!
//! - [`QueryWorkerRecord::fragment_endpoint`] is the queryfrag `SeriesFetch`
//!   surface the PromQL lane dials. Under the ADR-0071 amendment (dedicated
//!   fragment listener) it is the dedicated TLS listener, which serves the
//!   `Pinned` `SeriesFetch` surface only and terminates TLS in-process.
//! - [`QueryWorkerRecord::flight_sql_endpoint`] is the public gRPC listener,
//!   which mounts Flight SQL (and OTLP) and is reached plaintext. This is where a
//!   worker's Flight `DoGet` lives, so it is the location the SQL lane must dial.
//!
//! Before the amendment both surfaces shared one listener, so the SQL lane could
//! reuse `fragment_endpoint`. With `--fragment-listener` set that stopped being
//! true: `fragment_endpoint` names a TLS-only port that serves no Flight service,
//! and dialing it plaintext for a Flight `DoGet` failed twice over (wrong scheme,
//! and no service behind the port). The SQL lane therefore reads the separate
//! `flight_sql_endpoint`.
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

/// A [`WorkerEndpoints`] over the ravel-fleet query-worker registry (ADR-0071
/// SQL lane). Returns the Flight location of every live, protocol-matched
/// worker OTHER than this coordinator, in the registry's order. An empty result
/// means no workers are available, and ravel-sql runs the query fully local (a
/// single self-endpoint over the whole pinned set), which is always correct.
pub struct FleetWorkerEndpoints {
    live_workers: LiveWorkers,
    self_id: SelfId,
}

impl FleetWorkerEndpoints {
    /// Build over the shared live-worker set and the shared self-id cell (the
    /// same two the PromQL router and the heartbeat loop share).
    pub fn new(live_workers: LiveWorkers, self_id: SelfId) -> Self {
        FleetWorkerEndpoints {
            live_workers,
            self_id,
        }
    }

    /// The Flight location for a worker record: its public gRPC listener
    /// ([`QueryWorkerRecord::flight_sql_endpoint`]), where the Flight SQL `DoGet`
    /// surface is mounted. Plaintext `http://`: the public gRPC listener does not
    /// terminate TLS (only the dedicated fragment listener does, and it serves no
    /// Flight service). NOT `fragment_endpoint`, which under `--fragment-listener`
    /// is the TLS-only `SeriesFetch` port.
    fn location(record: &QueryWorkerRecord) -> String {
        format!("http://{}", record.flight_sql_endpoint)
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
            // router drops it at routing time: dispatching a slice to a worker
            // that speaks a different protocol would fail the fetch.
            .filter(|record| record.protocol_version == PROTOCOL_VERSION)
            // A record that advertises no Flight SQL endpoint is not a
            // dispatchable SQL worker. This is a pre-amendment record (the field
            // is `#[serde(default)]`, so an old writer's object decodes with an
            // empty string): its Flight SQL lives on a public gRPC address this
            // record does not carry, so dropping it here runs the query on the
            // remaining workers, or fully local, rather than dialing "http://"
            // and failing the slice.
            .filter(|record| !record.flight_sql_endpoint.is_empty())
            // The coordinator serves its own slices through the local path
            // (ravel_sql::distributed::CoordinatorSliceReader, the last step of
            // the fan-out's failure sequence), so dispatching one to itself over
            // Flight is a wasted hop: an extra connection, an extra ticket MAC,
            // and an extra encode/decode round for bytes it can read directly.
            // Before the self-id cell is populated (the identity exists only
            // once the gRPC listener has bound) nothing is excluded, which is
            // the pre-existing behavior and is correct, just one hop slower.
            .filter(|record| own.as_deref() != Some(record.process_id.as_str()))
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
/// `self_id` is the shared cell holding this process's own query-worker id (the
/// one the PromQL router reads to recognize a self-mapped slice). It is what
/// keeps the coordinator out of its own SQL roster.
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
    thresholds: ravel_query::distrib::partition::DistribThresholds,
    legacy_secret: Option<&str>,
) -> DistributedFlightConfig {
    DistributedFlightConfig {
        workers: Arc::new(FleetWorkerEndpoints::new(live_workers, self_id)),
        thresholds,
        shared_ticket_key: legacy_secret
            .map(|secret| ravel_sql::derive_ticket_key(secret.as_bytes())),
    }
}

/// The SQL lane's distributed config plus the ticket keys to install on the
/// Flight SQL service, from this process's [`DistribSettings`].
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
    match settings.sql_ticket_keys.as_deref() {
        Some(file_keys) => {
            let keys = SqlTicketKeys::from_file_keys(file_keys).ok_or_else(|| {
                anyhow::anyhow!("--sql-ticket-key-file resolved to no keys; it needs at least one")
            })?;
            Ok((
                distributed_flight_config(live_workers, self_id, settings.thresholds, None),
                Some(keys),
            ))
        }
        None => Ok((
            distributed_flight_config(
                live_workers,
                self_id,
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
        let endpoints = FleetWorkerEndpoints::new(live.clone(), no_self());

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
        let endpoints = FleetWorkerEndpoints::new(live.clone(), self_id(me));
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
        let early = FleetWorkerEndpoints::new(live.clone(), no_self());
        assert_eq!(
            early.endpoints(),
            vec!["http://10.0.0.1:9000".to_string()],
            "an unpopulated self-id cell excludes nothing"
        );
    }

    /// Regression for #1296: the SQL location is the worker's Flight SQL
    /// endpoint (public gRPC), never its `fragment_endpoint` (the dedicated TLS
    /// `SeriesFetch` listener under `--fragment-listener`, which serves no Flight
    /// service). A record whose two endpoints differ pins which field is read.
    #[test]
    fn location_is_the_flight_sql_endpoint_not_the_fragment_endpoint() {
        let current = PROTOCOL_VERSION;
        let live: LiveWorkers = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
            process_id: "a".to_string(),
            // A dedicated TLS fragment listener address: TLS-only, no Flight.
            fragment_endpoint: "10.0.0.1:9443".to_string(),
            // The public gRPC listener where Flight SQL DoGet actually lives.
            flight_sql_endpoint: "10.0.0.1:9000".to_string(),
            protocol_version: current,
            started_unix_ns: 0,
        }])));
        let endpoints = FleetWorkerEndpoints::new(live, no_self());
        assert_eq!(
            endpoints.endpoints(),
            vec!["http://10.0.0.1:9000".to_string()],
            "the SQL lane must dial the Flight SQL endpoint, not the fragment listener"
        );
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
        let endpoints = FleetWorkerEndpoints::new(live, no_self());
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
        let config = distributed_flight_config(live, no_self(), thresholds, Some("cluster-secret"));
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
