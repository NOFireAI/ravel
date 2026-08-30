//! Smoke tests for `sql_latency_bench`'s Flight SQL lane (issue #680), behind
//! the `flight-lane` feature.
//!
//! The lane exists because the in-process lanes measure the SQL executor as a
//! library, and a ClickBench number a user would see goes through
//! `ravel-server` over Flight SQL. So these tests refuse to stub the
//! transport: they stand up ravel-sql's real [`RavelFlightSqlService`] on a
//! real `tonic` listener on `127.0.0.1:0` over a `MemoryStore` tenant, and
//! drive the lane's own client against it. (ravel-sql's own `Harness` calls
//! the service's trait methods in process and lives in `ravel-sql/tests/util`,
//! which is not reachable from this crate; a listener is both available and
//! the stronger check, since the lane's whole point is the wire.)
//!
//! What they pin:
//!
//! - the lane reports the rows the server actually returned, `scan` absent,
//!   `source == "flight"`, one entry per corpus statement, and one progress
//!   line per finished statement;
//! - a transport failure (a closed port) is the statement's error: recorded in
//!   `failed` under `--continue-on-error`, and fatal without it.
#![cfg(feature = "flight-lane")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_bench::sql_corpus::{CorpusEntry, Modification};
use ravel_bench::sql_latency::{
    FlightTarget, GenerateConfig, TenantConfigInput, run_generated, run_tenant,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{
    DEFAULT_FETCH_CONCURRENCY, LogSegmentFetcher, QueryAdmissionController, QueryConcurrencyLimit,
    SegmentFetcher,
};
use ravel_sql::{
    DEFAULT_MAX_QUERY_BYTES, FlightAuth, FlightClock, FlightSqlConfig, RavelFlightSqlService,
    SpanSegmentFetcher, SqlConfig, SqlExecutor,
};
use ravel_types::accounting::NoopQueryCostRecorder;
use ravel_types::{CommitToken, TenantHash, TenantId, TimeRange};
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

/// The frozen query clock the generated lane writes against: its records land
/// in ingest-hour bucket 0 near the epoch, so a resolve over `[0, NOW_NS]`
/// stays bounded.
const NOW_NS: i64 = 4 * 3_600_000_000_000;

/// The bearer credential the served tenant answers to.
const TOKEN: &str = "flight-lane-token";

/// Records written, split across three RLOG objects.
const RECORDS: usize = 60;
const RECORDS_PER_OBJECT: usize = 20;

// ---------------------------------------------------------------------------
// The two deployment-owned traits, as fixed test doubles
// ---------------------------------------------------------------------------

/// A tenant resolver over a fixed `Bearer <token>` map, deny by default.
struct FixedAuth(HashMap<String, TenantHash>);

impl FlightAuth for FixedAuth {
    fn tenant(&self, metadata: &MetadataMap) -> Result<TenantHash, tonic::Status> {
        let raw = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| tonic::Status::unauthenticated("missing tenant credentials"))?;
        self.0
            .get(raw)
            .copied()
            .ok_or_else(|| tonic::Status::unauthenticated("invalid tenant credentials"))
    }

    fn min_commit_tokens(
        &self,
        _metadata: &MetadataMap,
    ) -> Result<Vec<CommitToken>, tonic::Status> {
        Ok(Vec::new())
    }
}

/// A clock frozen at the generated dataset's query clock, so ticket expiry and
/// the resolve's `now_ns` are deterministic.
struct FrozenClock(i64);

impl FlightClock for FrozenClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// The corpus this suite measures: two statements, each projecting one column
/// of every row, so `rows_returned` is exactly the rows written. Neither
/// requires a declared column, so neither can be skipped.
fn two_statement_corpus() -> Vec<CorpusEntry> {
    ["SELECT body FROM logs", "SELECT severity_text FROM logs"]
        .iter()
        .enumerate()
        .map(|(i, sql)| CorpusEntry {
            id: format!("flight_scan_{i}"),
            sql: (*sql).to_string(),
            constructs: vec!["select".to_string()],
            expected_rows: Some(RECORDS),
            upstream_id: None,
            modified: Modification::Verbatim,
            required_declarations: Vec::new(),
            class: None,
        })
        .collect()
}

/// Publish a three-object logs dataset for one tenant by running the
/// generated lane over a throwaway one-statement corpus, and return the tenant
/// it minted.
async fn publish_dataset(store: Arc<dyn ObjectStoreBackend>) -> TenantId {
    let report = run_generated(&GenerateConfig {
        store,
        store_backend: "memory".to_string(),
        region: "n/a".to_string(),
        endpoint: "n/a".to_string(),
        entries: vec![CorpusEntry {
            id: "seed".to_string(),
            sql: "SELECT count(*) FROM logs".to_string(),
            constructs: vec!["count".to_string()],
            expected_rows: Some(1),
            upstream_id: None,
            modified: Modification::Verbatim,
            required_declarations: Vec::new(),
            class: None,
        }],
        runs: 1,
        records: RECORDS,
        records_per_object: RECORDS_PER_OBJECT,
        extra_attrs: 2,
        max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
        cache_bytes: 0,
        deadline: Duration::from_secs(30),
        continue_on_error: false,
        fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
        progress_jsonl: None,
        tenant_max_bytes: ravel_bench::sql_latency::DEFAULT_TENANT_MAX_BYTES,
        parallel_final_aggregation: false,
        max_segments: ravel_query::DEFAULT_MAX_SEGMENTS,
        explain_dir: None,
        warm_catalog: false,
        logs_suffix_len: None,
    })
    .await
    .expect("generated lane publishes the dataset");
    assert_eq!(
        report.dataset.object_count, 3,
        "{RECORDS} records at {RECORDS_PER_OBJECT} per object is 3 objects"
    );
    assert_eq!(
        report.dataset.rows, RECORDS as u64,
        "every record is durable"
    );
    TenantId::new(report.provenance.dataset_id)
}

/// Serve ravel-sql's Flight SQL service over `store` for `tenant` on an
/// ephemeral loopback port. Returns the bound address; the server task is
/// detached and dies with the test process.
async fn serve_flight(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
) -> std::net::SocketAddr {
    let catalog = Arc::new(
        ravel_catalog::Catalog::new(Arc::clone(&store), ravel_catalog::CatalogConfig::default())
            .expect("catalog"),
    );
    let executor = Arc::new(SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    ));
    let mut tenants = HashMap::new();
    tenants.insert(format!("Bearer {TOKEN}"), tenant.hash());
    let service = RavelFlightSqlService::new(
        executor,
        Arc::new(FixedAuth(tenants)),
        Arc::new(FrozenClock(NOW_NS)),
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            ..FlightSqlConfig::default()
        },
        Arc::new(NoopQueryCostRecorder),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
    );

    let incoming =
        TcpIncoming::bind("127.0.0.1:0".parse().expect("loopback addr")).expect("bind listener");
    let addr = incoming.local_addr().expect("listener local addr");
    tokio::spawn(async move {
        Server::builder()
            .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .expect("serve Flight SQL");
    });
    addr
}

/// A tenant-lane config pointed at `flight`, over the dataset in `store`.
fn flight_cfg(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
    flight: FlightTarget,
    runs: usize,
    continue_on_error: bool,
    progress_jsonl: Option<std::path::PathBuf>,
) -> TenantConfigInput {
    TenantConfigInput {
        store,
        store_backend: "memory".to_string(),
        region: "n/a".to_string(),
        endpoint: "n/a".to_string(),
        tenant: tenant.as_str().to_string(),
        entries: two_statement_corpus(),
        runs,
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        now_ns: NOW_NS,
        compaction: None,
        max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
        // The generated lane writes shard 0 only and leaves no provisioning
        // record, so the shard count has to be named.
        shards: Some(1),
        cache_bytes: 0,
        deadline: Duration::from_secs(30),
        continue_on_error,
        fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
        progress_jsonl,
        tenant_max_bytes: ravel_bench::sql_latency::DEFAULT_TENANT_MAX_BYTES,
        parallel_final_aggregation: false,
        max_segments: ravel_query::DEFAULT_MAX_SEGMENTS,
        explain_dir: None,
        flight: Some(flight),
        warm_catalog: false,
        logs_suffix_len: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flight_lane_measures_every_statement_over_the_wire_without_scan_diagnostics() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = publish_dataset(Arc::clone(&store)).await;
    let addr = serve_flight(Arc::clone(&store), &tenant).await;

    let progress = tempfile::NamedTempFile::new().expect("progress file");
    let report = run_tenant(&flight_cfg(
        Arc::clone(&store),
        &tenant,
        FlightTarget {
            endpoint: addr.to_string(),
            token: Some(TOKEN.to_string()),
        },
        2,
        false,
        Some(progress.path().to_path_buf()),
    ))
    .await
    .expect("flight lane runs");

    assert!(
        report.failed.is_empty(),
        "no statement failed: {:?}",
        report.failed
    );
    assert!(
        report.skipped.is_empty(),
        "neither statement declares a dependency"
    );
    assert_eq!(report.entries.len(), 2, "one entry per corpus statement");

    // The rows come back over the wire, not from a local executor. Pinned to
    // the exact count the dataset holds: `> 0` would hold just as well if the
    // lane drained only the first batch of the first endpoint, which is the
    // way an IPC-draining bug actually presents.
    //
    // Prove-the-test: this is the assertion that fires when the drain is
    // defeated. Changing `rows += batches.iter().map(|batch|
    // batch.num_rows()).sum::<usize>();` in `execute_over_flight`
    // (src/sql_latency.rs) to `rows += batches.first().map(|batch|
    // batch.num_rows()).unwrap_or(0);` still returns a plausible non-zero
    // number and still passes every other assertion here, and fails this one.
    for entry in &report.entries {
        assert_eq!(
            entry.rows_returned, RECORDS,
            "entry `{}` must report every row the server returned",
            entry.id
        );
        assert!(entry.cold_ms > 0.0, "entry `{}` cold time > 0", entry.id);
        assert!(
            entry.min_ms <= entry.median_ms && entry.median_ms <= entry.max_ms,
            "entry `{}` violates min<=median<=max",
            entry.id
        );
        // Executor-side counters are not on the wire, so the lane must report
        // their absence rather than zeros that would read as "scanned
        // nothing".
        assert!(
            entry.scan.is_none(),
            "entry `{}` must carry no scan diagnostics over Flight",
            entry.id
        );
    }

    assert_eq!(report.provenance.source, "flight");
    assert_eq!(
        report.provenance.flight_endpoint.as_deref(),
        Some(addr.to_string().as_str()),
        "the provenance names the Flight endpoint, not the object store's"
    );
    assert_eq!(
        report.provenance.endpoint, "n/a",
        "the object-store endpoint field is left alone"
    );
    // The Flight lane does not send `parallel_final_aggregation` to the server,
    // so the report must record the local CLI value only as *requested*
    // (`flight_cfg` sets it false), and the *effective* value as unknown
    // (`None`): the server's own config governed, and this process cannot know
    // it. Recording the local value as effective (the bug this fixes) would have
    // `parallel_final_aggregation_effective == Some(false)` here.
    assert!(
        !report.provenance.parallel_final_aggregation_requested,
        "the requested field carries this run's CLI value (false)"
    );
    assert_eq!(
        report.provenance.parallel_final_aggregation_effective, None,
        "a Flight run does not send the setting, so the effective value is unknown"
    );
    // Same shape for the per-query pool ceiling (issue #615): `--sql-max-query-bytes`
    // is not a Flight header either, and `ExecutorSettings` is passed only on the
    // in-process arm, so the server's own ceiling governed. Echoing the local CLI
    // value as effective would let two Flight tables taken at different
    // `--sql-max-query-bytes` values look comparable while having run under
    // identical server ceilings.
    assert_eq!(
        report.provenance.sql_max_query_bytes_requested, DEFAULT_MAX_QUERY_BYTES,
        "the requested field carries this run's CLI value (flight_cfg's default)"
    );
    assert_eq!(
        report.provenance.sql_max_query_bytes_effective, None,
        "a Flight run does not send the ceiling, so the effective value is unknown"
    );
    // The dataset stanza is still resolved from the store directly: a Flight
    // client cannot read the catalog.
    assert_eq!(report.dataset.object_count, 3);
    assert_eq!(report.dataset.rows, RECORDS as u64);

    // `scan` is omitted from the JSON rather than serialized as null, and the
    // report still round-trips.
    let json = serde_json::to_string(&report).expect("report serializes");
    assert!(!json.contains("\"scan\""), "absent scan is omitted: {json}");
    serde_json::from_str::<ravel_bench::sql_latency::SqlLatencyReport>(&json)
        .expect("report round-trips");

    let lines = std::fs::read_to_string(progress.path()).expect("progress file readable");
    assert_eq!(
        lines.lines().count(),
        2,
        "one progress line per finished statement: {lines}"
    );
}

#[tokio::test]
async fn a_closed_port_fails_the_statement_rather_than_the_transport_silently_passing() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = publish_dataset(Arc::clone(&store)).await;

    // Bind and immediately drop, so the port is known-closed rather than
    // guessed. Nothing serves it.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr")
    };
    let target = FlightTarget {
        endpoint: closed.to_string(),
        token: Some(TOKEN.to_string()),
    };

    // With --continue-on-error the run completes and records the transport
    // error against each statement.
    //
    // Prove-the-test: the line this guards is `let mut client =
    // FlightSqlServiceClient::new(channel.clone().connect_lazy());` in
    // `measure_over_flight` (src/sql_latency.rs). Connecting eagerly instead
    // (`channel.clone().connect().await.map_err(...)?`) makes an unreachable
    // server abort the run with `connect: transport error` no matter what
    // `continue_on_error` says, so `run_tenant` below returns `Err` and this
    // `expect` fires. Verified by running it.
    let report = run_tenant(&flight_cfg(
        Arc::clone(&store),
        &tenant,
        target.clone(),
        2,
        true,
        None,
    ))
    .await
    .expect("the run completes when asked to continue past failures");
    assert!(
        report.entries.is_empty(),
        "an unreachable server produces no measurement: {:?}",
        report.entries
    );
    assert_eq!(
        report.failed.len(),
        2,
        "both statements are recorded failed"
    );
    for failure in &report.failed {
        assert_eq!(failure.run, 0, "the first run is the one that failed");
        assert!(
            failure.error.contains("GetFlightInfo"),
            "the recorded error names the RPC that could not connect: {}",
            failure.error
        );
    }

    // Without it, the first failure aborts the run.
    let err = run_tenant(&flight_cfg(
        Arc::clone(&store),
        &tenant,
        target,
        2,
        false,
        None,
    ))
    .await
    .expect_err("an unreachable server aborts a run that was not told to continue");
    assert!(
        err.to_string().contains("failed to execute"),
        "the abort names the statement: {err}"
    );
}
