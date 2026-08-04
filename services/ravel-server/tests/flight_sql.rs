//! End-to-end Flight SQL tests (ticket C1d, issue #152).
//!
//! These replace C1a's startup smoke test. Everything here goes over a real
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
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::sql::SqlState;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_sql::{SqlConfig, SqlExecutor};
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

/// Publish one real segment plus its commit record for `tenant`.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    metric: &str,
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
        shard: 0,
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
        shard: 0,
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

fn sql_state(store: Arc<dyn ObjectStoreBackend>, tokens: HashMap<String, TenantId>) -> SqlState {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    );
    SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(30),
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = oneshot::channel::<()>();
        let service = ravel_server::flight::service(state);
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
    };
    let running = ravel_server::start(
        config,
        store,
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
