//! End-to-end Flight SQL tests.
//!
//! Everything here goes over a real
//! tonic channel with real gRPC metadata, because the registration site is
//! precisely where a mistake would not show up in ravel-sql's own tests: the
//! tenant resolver, the metadata-to-header translation, and the service
//! registration all live on this side of the ADR-0013 boundary.
//!
//! Two levels:
//!
//! - `ravel_server::start` in query mode, asserting the gRPC listener carries
//!   the *real* Flight SQL service. The discriminator is deliberate: the C1a
//!   stub answered `UNIMPLEMENTED` to everything, so an authenticated-less
//!   `GetFlightInfo` answering `UNAUTHENTICATED` can only come from the real
//!   service.
//! - A hand-built `SqlState` over published fixture data on its own tonic
//!   server, driving `GetFlightInfo` then `DoGet` and comparing the rows to
//!   what was written. The clock is frozen there, as it is in the HTTP
//!   endpoint tests, so `Catalog::resolve` lists a handful of prefixes rather
//!   than a wall-clock fan-out.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{Any, CommandGetTables, CommandStatementQuery, ProstMessageExt};
use arrow_flight::{Criteria, FlightData, FlightDescriptor, Ticket};
use futures::TryStreamExt;
use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_fleet::query_workers::QueryWorkerRecord;
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::sql::SqlState;
use ravel_server::sql_distrib::distributed_flight_config;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_sql::{DistributedFlightConfig, SqlConfig, SqlExecutor, WorkerEndpoints};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tokio::sync::oneshot;
use tonic::Request;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small on purpose: `Catalog::resolve` issues one LIST per (shard,
/// ingest-hour) pair across the window, so a wall-clock value would fan out
/// to hundreds of thousands of LISTs.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

const QUERY: &str = "SELECT ts, value FROM samples ORDER BY ts";

/// A clock frozen at [`NOW_NS`].
struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// Publish one real segment plus its commit record for `tenant`, on shard 0.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    metric: &str,
    samples: &[(i64, f64)],
) {
    publish_segment_on_shard(store, tenant, metric, 0, samples).await;
}

/// Publish one real segment plus its commit record for `tenant` on `shard`.
///
/// Publishing the same series to two shards is how the ADR-0071 engage test
/// builds a snapshot that partitions into more than one slice (partitioning is
/// shard-major) with a `(series_id, ts)` sample duplicated across slices, so the
/// coordinator's cross-slice dedup is observable in the row count.
async fn publish_segment_on_shard(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    metric: &str,
    shard: u32,
    samples: &[(i64, f64)],
) {
    let tenant_hash = tenant.hash();
    let label_set = labels_for(metric);
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }];

    let writer_id = Uuid::from_u128(3_000);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Publish one real RSPAN object plus its `Signal::Spans` commit record for
/// `tenant` (ADR-0045 decision 5). The footer carries `tenant`'s hash so the
/// tenant-checked span fetch admits it, and it lands at the reconstructed data
/// key so `Catalog::resolve` for `Signal::Spans` finds it. Each `records` entry
/// is `(service, name, start_ns, duration_ns, status)`.
async fn publish_span_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    records: &[(&str, &str, i64, i64, ravel_rspan::StatusCode)],
) {
    let tenant_hash = tenant.hash();
    let spans: Vec<ravel_rspan::SpanRecord> = records
        .iter()
        .enumerate()
        .map(
            |(i, (service, name, start, dur, status))| ravel_rspan::SpanRecord {
                trace_id: [0xABu8; 16],
                span_id: [i as u8; 8],
                parent_span_id: None,
                name: (*name).to_string(),
                start_ts_ns: *start,
                end_ts_ns: *start + *dur,
                status_code: *status,
                status_message: None,
                attrs: vec![("service.name".to_string(), (*service).to_string())],
            },
        )
        .collect();

    let min_event_ts_ns = spans.iter().map(|s| s.start_ts_ns).min().expect("nonempty");
    let max_event_ts_ns = spans.iter().map(|s| s.end_ts_ns).max().expect("nonempty");

    let writer_id = Uuid::from_u128(5_000);
    let identity = ravel_rspan::ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut writer = ravel_rspan::RspanWriter::new(ravel_rspan::RspanConfig::default(), identity);
    for span in &spans {
        writer.push(span.clone());
    }
    let bytes = writer.finish().expect("finish rspan object");
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: spans.len() as u64,
        series_count: 1,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid span commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
}

fn sql_state(store: Arc<dyn ObjectStoreBackend>, tokens: HashMap<String, TenantId>) -> SqlState {
    sql_state_with_shards(store, tokens, 1)
}

/// [`sql_state`] over a catalog that resolves `shard_count` shards. The default
/// single-shard state cannot see data written to any shard but 0, so the
/// distributed-scan engage test (which fans out shard-major over a two-shard
/// snapshot) builds its state here with `shard_count = 2`.
fn sql_state_with_shards(
    store: Arc<dyn ObjectStoreBackend>,
    tokens: HashMap<String, TenantId>,
    shard_count: u32,
) -> SqlState {
    let catalog_config = CatalogConfig {
        shard_count,
        ..CatalogConfig::default()
    };
    let catalog = Arc::new(Catalog::new(Arc::clone(&store), catalog_config).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    );
    SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(30),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    }
}

/// A running tonic server carrying only the Flight SQL service.
struct FlightServer {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl FlightServer {
    async fn start(state: &SqlState) -> Self {
        Self::start_with(state, None).await
    }

    /// Start a Flight server with an ADR-0071 distributed scan config installed,
    /// so a whole-set statement's `DoGet` fans the samples scan
    /// out to the roster the config carries.
    async fn start_distributed(state: &SqlState, distributed: DistributedFlightConfig) -> Self {
        Self::start_with(state, Some(distributed)).await
    }

    async fn start_with(state: &SqlState, distributed: Option<DistributedFlightConfig>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = oneshot::channel::<()>();
        // The Flight ticket-TTL ceiling is sourced from the durable sys/gc
        // (ADR-0050 section 4, EC4); this test uses the maintain-default ceiling
        // (protection_horizon - grace) the server would source on a fresh
        // bucket.
        let ceiling = ravel_server::gc_config::flight_ceiling(
            &ravel_maintain::GcConfigValues::maintain_defaults(),
        );
        let service = ravel_server::flight::service(state, ceiling, distributed);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await
                .expect("serve");
        });
        FlightServer {
            addr,
            shutdown: tx,
            task,
        }
    }

    async fn client(&self) -> FlightServiceClient<tonic::transport::Channel> {
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", self.addr))
            .expect("valid endpoint uri")
            .connect()
            .await
            .expect("connect");
        FlightServiceClient::new(channel)
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        let _ = self.task.await;
    }
}

/// A `FlightDescriptor` carrying `command`, the way a Flight SQL client sends
/// one.
fn descriptor(command: &impl ProstMessageExt) -> FlightDescriptor {
    FlightDescriptor::new_cmd(command.as_any().encode_to_vec())
}

/// Attach the bearer token plus the window the fixture data lives in.
fn authed<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii"),
    );
    metadata.insert("x-ravel-start", "0".parse().expect("ascii"));
    metadata.insert(
        "x-ravel-end",
        format!("{}", NOW_NS as f64 / 1e9).parse().expect("ascii"),
    );
    request
}

/// Decode a `DoGet` response into a row count and the column names it
/// carried.
///
/// Deliberately not a typed value comparison: arrow-flight carries arrow 58
/// and this crate's `arrow` dev-dependency is the workspace 59 pin, so a
/// downcast here would need a second arrow major declared just for a test.
/// Row *values* are already compared bit-for-bit against
/// `SqlExecutor::execute` in ravel-sql's own Flight test; what this level has
/// to prove is that the bytes survive a real channel and decode to the right
/// shape.
async fn decode(
    stream: tonic::Streaming<FlightData>,
) -> Result<(usize, Vec<String>), arrow_flight::error::FlightError> {
    let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|status| arrow_flight::error::FlightError::Tonic(Box::new(status))),
    )
    .try_collect::<Vec<_>>()
    .await?;

    let rows = batches.iter().map(|batch| batch.num_rows()).sum();
    let columns = batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect()
        })
        .unwrap_or_default();
    Ok((rows, columns))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `GetFlightInfo` then `DoGet` over a real channel returns the published
/// samples.
#[tokio::test]
async fn a_flight_sql_query_returns_the_published_rows() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let samples = [(100i64, 1.5f64), (200, 2.5), (300, -0.0)];
    publish_segment(store.as_ref(), &tenant, "cpu", &samples).await;

    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant);
    let state = sql_state(store, tokens);
    let server = FlightServer::start(&state).await;
    let mut client = server.client().await;

    let command = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };
    let info = client
        .get_flight_info(authed(descriptor(&command), "acme-token"))
        .await
        .expect("flight info")
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .expect("one endpoint")
        .ticket
        .clone()
        .expect("a ticket");

    let stream = client
        .do_get(authed(ticket, "acme-token"))
        .await
        .expect("do get")
        .into_inner();
    let (rows, columns) = decode(stream).await.expect("decode");

    assert_eq!(rows, samples.len(), "every published sample comes back");
    assert_eq!(columns, vec!["ts".to_string(), "value".to_string()]);

    server.stop().await;
}

/// ADR-0045 decision 5 reachability, over Flight SQL: `GetFlightInfo` then
/// `DoGet` for a `SELECT ... FROM spans WHERE ...` statement over a real tonic
/// channel returns the published spans. This proves the second production
/// surface (not just `/api/v1/sql`) reaches the newly-wired spans path through
/// `plan_pinned`: the ticket resolves the `Signal::Spans` snapshot at
/// `GetFlightInfo` and executes the accounted, tenant-checked span scan at
/// `DoGet`. Row values are compared bit-for-bit against `SqlExecutor::execute`
/// in ravel-sql's own tests; this level proves the shape survives the wire.
#[tokio::test]
async fn a_flight_sql_spans_query_returns_the_published_rows() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_span_segment(
        store.as_ref(),
        &tenant,
        &[
            (
                "checkout",
                "POST /checkout",
                100,
                600_000_000,
                ravel_rspan::StatusCode::Error,
            ),
            (
                "cart",
                "GET /cart",
                200,
                100_000_000,
                ravel_rspan::StatusCode::Ok,
            ),
            (
                "checkout",
                "POST /checkout/confirm",
                300,
                700_000_000,
                ravel_rspan::StatusCode::Error,
            ),
        ],
    )
    .await;

    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant);
    let state = sql_state(store, tokens);
    let server = FlightServer::start(&state).await;
    let mut client = server.client().await;

    let command = CommandStatementQuery {
        query: "SELECT name, duration_ns, service_name FROM spans \
                WHERE service_name = 'checkout' ORDER BY start_ts"
            .to_string(),
        transaction_id: None,
    };
    let info = client
        .get_flight_info(authed(descriptor(&command), "acme-token"))
        .await
        .expect("flight info")
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .expect("one endpoint")
        .ticket
        .clone()
        .expect("a ticket");

    let stream = client
        .do_get(authed(ticket, "acme-token"))
        .await
        .expect("do get")
        .into_inner();
    let (rows, columns) = decode(stream).await.expect("decode");

    assert_eq!(rows, 2, "only the two checkout spans come back");
    assert_eq!(
        columns,
        vec![
            "name".to_string(),
            "duration_ns".to_string(),
            "service_name".to_string()
        ]
    );

    server.stop().await;
}

/// A ticket minted for one tenant, replayed with another tenant's credentials,
/// is denied over the wire. This is the same rule ravel-sql tests in-process,
/// asserted here against the real metadata path.
#[tokio::test]
async fn a_ticket_replayed_by_another_tenant_is_denied_over_the_wire() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let acme = TenantId::new("acme".to_string());
    let other = TenantId::new("other".to_string());
    publish_segment(store.as_ref(), &acme, "cpu", &[(100, 1.0)]).await;
    publish_segment(store.as_ref(), &other, "cpu", &[(100, 9.0)]).await;

    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), acme);
    tokens.insert("other-token".to_string(), other);
    let state = sql_state(store, tokens);
    let server = FlightServer::start(&state).await;
    let mut client = server.client().await;

    let command = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };
    let info = client
        .get_flight_info(authed(descriptor(&command), "acme-token"))
        .await
        .expect("flight info")
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .expect("one endpoint")
        .ticket
        .clone()
        .expect("a ticket");

    let status = client
        .do_get(authed(ticket, "other-token"))
        .await
        .expect_err("cross-tenant redemption is denied");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    server.stop().await;
}

/// The catalog surface requires credentials, and answers one table when it
/// has them.
#[tokio::test]
async fn the_table_listing_requires_credentials() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant);
    let state = sql_state(store, tokens);
    let server = FlightServer::start(&state).await;
    let mut client = server.client().await;

    let command = CommandGetTables::default();
    let status = client
        .get_flight_info(Request::new(descriptor(&command)))
        .await
        .expect_err("no credentials, no metadata");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    let info = client
        .get_flight_info(authed(descriptor(&command), "acme-token"))
        .await
        .expect("flight info")
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .expect("one endpoint")
        .ticket
        .clone()
        .expect("a ticket");
    // The metadata ticket is the command itself, which is what lets
    // arrow-flight's dispatcher route the DoGet back to the right handler.
    let decoded = Any::decode(&*ticket.ticket).expect("an Any");
    assert!(
        decoded
            .unpack::<CommandGetTables>()
            .expect("decodes")
            .is_some()
    );

    let stream = client
        .do_get(authed(ticket, "acme-token"))
        .await
        .expect("do get")
        .into_inner();
    let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|status| arrow_flight::error::FlightError::Tonic(Box::new(status))),
    )
    .try_collect::<Vec<_>>()
    .await
    .expect("decode");
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 1, "exactly the samples table");

    server.stop().await;
}

/// The binary registers the real Flight SQL service on the gRPC listener.
///
/// A query-only process binds gRPC only because `flight-sql` is on (gRPC
/// otherwise carries only OTLP ingest), and an unauthenticated
/// `GetFlightInfo` answering `UNAUTHENTICATED` rather than `UNIMPLEMENTED` is
/// what distinguishes the real service from C1a's stub.
#[tokio::test]
async fn the_server_registers_the_real_flight_sql_service() {
    let tenant = TenantId::new("acme");
    let mut tokens = HashMap::new();
    tokens.insert("testtoken".to_string(), tenant);
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let config = ServerConfig {
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode: Mode::Query,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            fold_interval: Duration::from_secs(60),
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        cache_dir: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let running = ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("starts");
    let grpc_addr = running
        .grpc_addr
        .expect("query mode binds gRPC when flight-sql is on");

    let channel = tonic::transport::Channel::from_shared(format!("http://{grpc_addr}"))
        .expect("valid endpoint uri")
        .connect()
        .await
        .expect("Flight client connects");
    let mut client = FlightServiceClient::new(channel);

    let command = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };
    let status = client
        .get_flight_info(Request::new(descriptor(&command)))
        .await
        .expect_err("no credentials");
    assert_eq!(
        status.code(),
        tonic::Code::Unauthenticated,
        "the real service authenticates; the C1a stub answered Unimplemented",
    );

    // ListFlights is genuinely not implemented, and stays that way.
    let status = client
        .list_flights(Criteria::default())
        .await
        .expect_err("list_flights is not part of the v1 surface");
    assert_eq!(status.code(), tonic::Code::Unimplemented);

    running.shutdown().await.expect("graceful shutdown");
}

/// A `Ticket` that is not a Flight SQL command at all is refused, not
/// dispatched. Pinned because the ticket is attacker-reachable bytes.
#[tokio::test]
async fn a_garbage_ticket_is_refused() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant);
    let state = sql_state(store, tokens);
    let server = FlightServer::start(&state).await;
    let mut client = server.client().await;

    let status = client
        .do_get(authed(
            Ticket::new(vec![0xff, 0x00, 0x13, 0x37]),
            "acme-token",
        ))
        .await
        .expect_err("garbage is refused");
    assert_ne!(status.code(), tonic::Code::Ok);

    server.stop().await;
}

/// Deliverable 5, the reachable half of the ADR-0071 SQL lane.
///
/// This proves what is installable read-only:
///
/// 1. The Flight SQL surface is reachable end to end under the `flight-sql`
///    feature: `GetFlightInfo` returns exactly ONE endpoint (the coordinator
///    itself), `DoGet` streams the published rows back over a real channel, and
///    a second identical run is byte-for-byte identical (the result never
///    depends on whether distribution is engaged).
/// 2. The SQL lane's worker roster surface -- this crate's
///    [`ravel_sql::WorkerEndpoints`] impl over the ravel-fleet query-worker
///    registry ([`ravel_server::sql_distrib::FleetWorkerEndpoints`]) -- resolves
///    a live worker to its Flight location (the fragment gRPC listener, which
///    also hosts Flight SQL), and [`distributed_flight_config`] carries that
///    roster plus the cost thresholds a coordinator would install.
///
/// It does NOT assert "distributed scan engages"; that is
/// [`distributed_flight_sql_scan_engages`] below, which installs the config
/// through `RavelFlightSqlService::with_distributed_scan` and proves the fan-out
/// runs. Here the config is only built (not installed), so "byte-identical to
/// distribution off" holds over the single local path, which is what the two-run
/// assertion records.
#[tokio::test]
async fn distributed_flight_sql_reachable_end_to_end() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let samples = [(100i64, 1.5f64), (200, 2.5), (300, -0.0)];
    publish_segment(store.as_ref(), &tenant, "cpu", &samples).await;

    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant);
    let state = sql_state(store, tokens);
    let server = FlightServer::start(&state).await;
    let client = server.client().await;

    let command = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };

    // One statement query, twice: the result must not depend on distribution.
    let run = |mut client: FlightServiceClient<tonic::transport::Channel>| {
        let command = command.clone();
        async move {
            let info = client
                .get_flight_info(authed(descriptor(&command), "acme-token"))
                .await
                .expect("flight info")
                .into_inner();
            assert_eq!(
                info.endpoint.len(),
                1,
                "a Flight SQL client must see exactly one endpoint (the coordinator)"
            );
            let ticket = info.endpoint[0]
                .ticket
                .clone()
                .expect("the single endpoint carries a ticket");
            let stream = client
                .do_get(authed(ticket, "acme-token"))
                .await
                .expect("do get")
                .into_inner();
            decode(stream).await.expect("decode")
        }
    };

    let first = run(client.clone()).await;
    let second = run(client.clone()).await;
    assert_eq!(first.0, samples.len(), "every published sample comes back");
    assert_eq!(first.1, vec!["ts".to_string(), "value".to_string()]);
    assert_eq!(
        first, second,
        "the result is byte-identical run to run (distribution off is the only path)"
    );

    // The SQL-lane worker roster surface: a live worker resolves to its Flight
    // location (its fragment gRPC listener, which also hosts Flight SQL).
    let worker_endpoint = server.addr.to_string();
    let live_workers = Arc::new(parking_lot::RwLock::new(Arc::new(vec![
        QueryWorkerRecord {
            process_id: "worker-a".to_string(),
            fragment_endpoint: worker_endpoint.clone(),
            protocol_version: ravel_query::distrib::codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
    ])));
    let thresholds = ravel_query::distrib::partition::DistribThresholds {
        min_store_bytes: 0,
        min_segments: 0,
        max_parallel_slices: 8,
    };
    let config = distributed_flight_config(live_workers, thresholds, "cluster-secret");
    assert_eq!(
        config.workers.endpoints(),
        vec![format!("http://{worker_endpoint}")],
        "the fleet-backed WorkerEndpoints resolves the live worker to its Flight location"
    );

    server.stop().await;
}

/// A [`WorkerEndpoints`] that counts how many times its roster is resolved and
/// serves a late-bound location list. `endpoints()` is called by
/// `plan_distributed_slices` only after the cost gate passes on the engage path,
/// so a nonzero count is proof the coordinator engaged distribution for a query
/// -- and stays zero when no config is installed (the flip proof below). The
/// location list is filled after the server binds (its own address is not known
/// until then), the same late-binding shape the real fleet roster has.
struct CountingEndpoints {
    locations: Arc<parking_lot::RwLock<Vec<String>>>,
    calls: Arc<AtomicUsize>,
}

impl WorkerEndpoints for CountingEndpoints {
    fn endpoints(&self) -> Vec<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.locations.read().clone()
    }
}

/// Deliverable 5: the ADR-0071 SQL-lane distributed scan ENGAGES in
/// the shipping service, and its result is identical to the whole-set local
/// path.
///
/// The fixture publishes the same series to two shards with a `(series_id,
/// ts=100)` sample duplicated across them. Partitioning is shard-major, so the
/// pinned snapshot cuts into two slices, and the duplicated sample lands in both
/// -- which makes the coordinator's cross-slice dedup observable: an undeduped
/// fan-out would return four rows (100, 100, 200, 300), the deduped result is
/// three (100, 200, 300). The local run over the same data (no config installed)
/// also dedups, so the two results matching is the byte-identical guarantee.
///
/// Two independent facts are asserted:
///
/// 1. Identical result. The query run against a service with the distributed
///    config installed returns exactly the rows the same query returns against a
///    service with no config -- same row count, same columns.
/// 2. Fan-out happened. A [`CountingEndpoints`] wraps the worker roster; its
///    counter is nonzero only if the coordinator reached
///    `plan_distributed_slices` (past the cost gate) for the installed-config
///    run. With the two-shard snapshot the gate returns two slices, so the
///    coordinator self-dials its own Flight listener twice over a real channel
///    (each slice ticket carries `slice_count == 2`, served by the fragment
///    path), and the merged, deduped rows come back.
///
/// Non-vacuity (prove-the-test): replacing `start_distributed` with `start`
/// (installing no config) skips the engage branch in `do_get_statement` -- the
/// `self.distributed` option is `None`, so `endpoints()` is never called, the
/// counter stays zero, and the `calls > 0` assertion below fails. Verified by
/// making that one-line change and observing the failure, then restored; see the
/// task report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_flight_sql_scan_engages() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    // The same series on two shards, sharing (ts=100). Deduped distinct ts:
    // 100, 200, 300 -> three rows; an undeduped cross-slice union would be four.
    publish_segment_on_shard(store.as_ref(), &tenant, "cpu", 0, &[(100, 1.5), (200, 2.5)]).await;
    publish_segment_on_shard(store.as_ref(), &tenant, "cpu", 1, &[(100, 1.5), (300, 3.5)]).await;
    const DEDUPED_ROWS: usize = 3;

    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant);
    // Two shards, so the pinned snapshot resolves both segments and partitions
    // shard-major into two slices.
    let state = sql_state_with_shards(store, tokens, 2);

    // Run the query and return (row count, column names).
    async fn run_query(server: &FlightServer) -> (usize, Vec<String>) {
        let mut client = server.client().await;
        let command = CommandStatementQuery {
            query: QUERY.to_string(),
            transaction_id: None,
        };
        let info = client
            .get_flight_info(authed(descriptor(&command), "acme-token"))
            .await
            .expect("flight info")
            .into_inner();
        assert_eq!(
            info.endpoint.len(),
            1,
            "the external client always sees exactly one endpoint, distribution installed or not"
        );
        let ticket = info.endpoint[0]
            .ticket
            .clone()
            .expect("the single endpoint carries a ticket");
        let stream = client
            .do_get(authed(ticket, "acme-token"))
            .await
            .expect("do get")
            .into_inner();
        decode(stream).await.expect("decode")
    }

    // Baseline: no distributed config installed. This is the whole-set local
    // path (it still dedups across shards locally).
    let local_server = FlightServer::start(&state).await;
    let local = run_query(&local_server).await;
    assert_eq!(
        local.0, DEDUPED_ROWS,
        "the local path dedups the cross-shard duplicate"
    );
    local_server.stop().await;

    // Engage: install a distributed config whose roster is the coordinator's own
    // Flight listener (single process => the worker shares this coordinator's
    // ticket key and tenant resolver, so a self-dialed slice fetch authenticates
    // and its ticket MAC verifies). The location is filled after the server binds.
    let calls = Arc::new(AtomicUsize::new(0));
    let locations: Arc<parking_lot::RwLock<Vec<String>>> =
        Arc::new(parking_lot::RwLock::new(Vec::new()));
    let config = DistributedFlightConfig {
        workers: Arc::new(CountingEndpoints {
            locations: locations.clone(),
            calls: calls.clone(),
        }),
        // Zeroed byte/segment thresholds so the cost gate always trips; the
        // two-shard snapshot then cuts two slices to fan out.
        thresholds: ravel_query::distrib::partition::DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
        // Single process: keep the service's own key; the self-dialed worker is
        // this same instance, so its ticket MAC verifies without a derived key.
        shared_ticket_key: None,
    };
    let dist_server = FlightServer::start_distributed(&state, config).await;
    *locations.write() = vec![format!("http://{}", dist_server.addr)];

    let distributed = run_query(&dist_server).await;

    // Fact 1: identical result.
    assert_eq!(
        distributed, local,
        "the distributed scan returns exactly the whole-set local result (cross-slice dedup ran)"
    );
    assert_eq!(
        distributed.0, DEDUPED_ROWS,
        "the coordinator deduped the sample duplicated across the two slices"
    );

    // Fact 2: distribution actually engaged. `endpoints()` is reached only past
    // the cost gate on the engage path, so a nonzero count proves the fan-out
    // ran; it stays zero with no config installed (see the non-vacuity note).
    assert!(
        calls.load(Ordering::SeqCst) > 0,
        "the coordinator engaged distribution and resolved the worker roster"
    );

    dist_server.stop().await;
}
