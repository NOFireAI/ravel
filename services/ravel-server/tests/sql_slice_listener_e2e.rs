//! SQL slice `DoGet` on the dedicated fragment listener (ADR-1689 decision 1),
//! through the server's own startup path: `Cli` flags,
//! `Cli::parse_distrib_settings`, and `ravel_server::start`, for two processes
//! that find each other through the real query-worker heartbeat under
//! `sys/query/workers/`.
//!
//! Both processes run `--fragment-listener` with the test CA and a
//! `ravel-fragment` leaf certificate, and share one `--sql-ticket-key-file`.
//! The coordinator's store refuses every GET of a published data object, so it
//! cannot answer any part of the query itself: the query succeeds only when the
//! worker, in the other process, served the slices. With `--fragment-listener`
//! set the worker's public gRPC listener refuses slice tickets, so the slices
//! can only have been served on its dedicated TLS listener.
//!
//! The refusal cases pin each side of the split: a slice ticket on the public
//! listener, a client Flight SQL method on the dedicated listener, and a
//! dedicated-listener `DoGet` from a peer that presents no client certificate.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use arrow_flight::{FlightData, Ticket};
use clap::Parser;
use futures::TryStreamExt;
use prost::Message;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::config::{Cli, DistribSettings};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_sql::{FlightTicket, SliceReject, SqlTicketKeys, TicketSurface};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tonic::Request;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const QUERY: &str = "SELECT ts, value FROM samples ORDER BY ts";

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

/// How long the coordinator gets to see the worker in its heartbeat live set.
const ROSTER_DEADLINE: Duration = Duration::from_secs(20);

const SQL_KEY: &str = "3333333333333333333333333333333333333333333333333333333333333333";
const FRAGMENT_KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// The fixed server name every fragment certificate carries.
const FRAGMENT_SERVER_NAME: &str = "ravel-fragment";

// Operator-provisioned test PEM material, generated offline (EC P-256, valid to
// 2126), the same material the dedicated-listener unit tests in `distrib.rs`
// use: a CA, and a `ravel-fragment` leaf it signed carrying both serverAuth and
// clientAuth, since one process is both the worker that serves the listener
// and the coordinator that dials it.
const TEST_FRAGMENT_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBiDCCAS6gAwIBAgIURqm7z2RSSm9YMcJrjixkBTS6iSAwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MTkyMTIx
MzVaGA8yMTI2MDgyNjIxMjEzNVowITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQt
dGVzdC1jYTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABOynQpfkkGc1dJv+181e
8I9uvBML0AvXJo95Z4dxje72IOA/Hhh4cpQ0EQfogGW4LtnbWS7NgilX1+RpC6gG
CVajQjBAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0GA1UdDgQW
BBT7C6f70yaChMoXGTIBoZSw+8p3TTAKBggqhkjOPQQDAgNIADBFAiBXCp1E6E6i
I3VH8wGfxQxywXkuQ86dVH5Z7FpTA9udVAIhAJT+wKFJo9hWpeKKbEmbtuuwfaok
5axPjJ9kiO1C6ZIu
-----END CERTIFICATE-----
";
const TEST_FRAGMENT_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIB2jCCAYCgAwIBAgIUX3yTIiYvkWMVICeYkAoWQ/cpCEYwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MTkyMTIx
MzVaGA8yMTI2MDgyNjIxMjEzNVowGTEXMBUGA1UEAwwOcmF2ZWwtZnJhZ21lbnQw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQHF0p+BdFVa6wOH4/e9vBYV2a3We8/
+XoQmKdUGN8vOrlnREuOj4pqI54CjnYZ1OLRQF3JRynJ5y/yWL+i3rFEo4GbMIGY
MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUF
BwMBBggrBgEFBQcDAjAZBgNVHREEEjAQgg5yYXZlbC1mcmFnbWVudDAdBgNVHQ4E
FgQUfJC6GQoihnxgaXOnWiJBAfwInPwwHwYDVR0jBBgwFoAU+wun+9MmgoTKFxky
AaGUsPvKd00wCgYIKoZIzj0EAwIDSAAwRQIgbEMg/jES94eo3dxOwEiM1FiHhY1v
hzdk6C9qmCCckI4CIQC/2tvVzC1VvE9eO0Y9eN2GDp63hSc+5YvKnvFm8P6I6Q==
-----END CERTIFICATE-----
";
const TEST_FRAGMENT_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgonxTB6rEt10ZCoQ+
L1ACQVzux8AvoQAB2A9c890hWPmhRANCAAQHF0p+BdFVa6wOH4/e9vBYV2a3We8/
+XoQmKdUGN8vOrlnREuOj4pqI54CjnYZ1OLRQF3JRynJ5y/yWL+i3rFE
-----END PRIVATE KEY-----
";

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

fn temp_file(contents: &str) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("temp file");
    std::fs::write(file.path(), contents).expect("write");
    file
}

/// The files a `--fragment-listener` process reads at startup. Held for the
/// test's duration so the paths stay valid.
struct Material {
    fragment_keys: tempfile::NamedTempFile,
    sql_keys: tempfile::NamedTempFile,
    cert: tempfile::NamedTempFile,
    key: tempfile::NamedTempFile,
    ca: tempfile::NamedTempFile,
}

impl Material {
    fn new() -> Self {
        Material {
            fragment_keys: temp_file(&format!("{FRAGMENT_KEY}\n")),
            sql_keys: temp_file(&format!("{SQL_KEY}\n")),
            cert: temp_file(TEST_FRAGMENT_CERT_PEM),
            key: temp_file(TEST_FRAGMENT_KEY_PEM),
            ca: temp_file(TEST_FRAGMENT_CA_PEM),
        }
    }

    /// The distributed settings a real `ravel-server` process builds from its
    /// flags, with `--fragment-listener` on an ephemeral loopback port and the
    /// cost gate forced open so every statement fans out. `--listen-http` and
    /// `--listen-grpc` name fixed ports only so validation sees distinct
    /// listeners; the server binds the ephemeral ones in [`ServerConfig`].
    fn distrib_settings(&self) -> DistribSettings {
        let path = |file: &tempfile::NamedTempFile| file.path().to_str().expect("utf8").to_owned();
        let cli = Cli::try_parse_from([
            "ravel-server".to_owned(),
            "--mode".to_owned(),
            "all".to_owned(),
            "--listen-http".to_owned(),
            "127.0.0.1:8080".to_owned(),
            "--listen-grpc".to_owned(),
            "127.0.0.1:4317".to_owned(),
            "--distributed-query".to_owned(),
            "--fragment-key-file".to_owned(),
            path(&self.fragment_keys),
            "--sql-ticket-key-file".to_owned(),
            path(&self.sql_keys),
            "--fragment-listener".to_owned(),
            "127.0.0.1:0".to_owned(),
            "--fragment-tls-cert".to_owned(),
            path(&self.cert),
            "--fragment-tls-key".to_owned(),
            path(&self.key),
            "--fragment-tls-ca".to_owned(),
            path(&self.ca),
            "--distribute-bytes-threshold".to_owned(),
            "0".to_owned(),
            "--distribute-segments-threshold".to_owned(),
            "0".to_owned(),
        ])
        .expect("flags parse");
        cli.validate().expect("flags validate");
        let settings = cli
            .parse_distrib_settings()
            .expect("distributed settings parse")
            .expect("--distributed-query is on");
        assert!(settings.fragment_listener.is_some());
        settings
    }
}

/// A store over one shared [`MemoryStore`] that counts GETs of the published
/// data objects and, when `refuse` is set, fails them.
struct DataStore {
    inner: Arc<MemoryStore>,
    data_keys: HashSet<String>,
    refuse: bool,
    data_gets: AtomicUsize,
}

impl DataStore {
    fn new(inner: Arc<MemoryStore>, data_keys: HashSet<String>, refuse: bool) -> Arc<Self> {
        Arc::new(DataStore {
            inner,
            data_keys,
            refuse,
            data_gets: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for DataStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if self.data_keys.contains(key) {
            self.data_gets.fetch_add(1, Ordering::SeqCst);
            if self.refuse {
                return Err(StoreError::AccessDenied(format!(
                    "the coordinator may not read {key}"
                )));
            }
        }
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Publish one segment of `cpu` on `shard` with `samples` and return its data
/// key.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    shard: u32,
    samples: &[(i64, f64)],
) -> String {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: "cpu".to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, "cpu", &label_set).expect("series id"),
        labels: label_set,
        samples: samples
            .iter()
            .map(|&(ts_ns, value)| Sample { ts_ns, value })
            .collect(),
    }];
    let writer_id = uuid::Uuid::from_u128(6_000 + u128::from(shard));
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
    let base_ns = samples[0].0;
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
        created_unix_ns: base_ns + 4 * NS_PER_MIN,
        ingest_hour_bucket: u32::try_from(base_ns / NS_PER_HOUR).expect("hour bucket fits u32"),
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
    data_key
}

async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    distrib: Option<DistribSettings>,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        // Two shards, so the pinned snapshot partitions shard-major into two
        // slices.
        shard_count: 2,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// Decode `DoGet`'s messages into this crate's arrow by re-framing them as an
/// IPC stream (arrow-flight carries a different arrow major, so its batches
/// cannot be compared with this crate's types directly).
fn to_batch(messages: &[FlightData]) -> arrow::record_batch::RecordBatch {
    let mut ipc: Vec<u8> = Vec::new();
    for message in messages {
        let mut metadata = message.data_header.to_vec();
        while metadata.len() % 8 != 0 {
            metadata.push(0);
        }
        ipc.extend_from_slice(&u32::MAX.to_le_bytes());
        ipc.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        ipc.extend_from_slice(&metadata);
        ipc.extend_from_slice(&message.data_body);
    }
    ipc.extend_from_slice(&u32::MAX.to_le_bytes());
    ipc.extend_from_slice(&0u32.to_le_bytes());
    let reader = arrow::ipc::reader::StreamReader::try_new(ipc.as_slice(), None).expect("ipc");
    let schema = reader.schema();
    let batches: Vec<_> = reader.map(|batch| batch.expect("batch")).collect();
    arrow::compute::concat_batches(&schema, &batches).expect("concat")
}

/// `message` with the tenant bearer token and the query window attached.
fn authed<T>(message: T, start_ns: i64, end_ns: i64) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii"),
    );
    metadata.insert(
        "x-ravel-start",
        (start_ns / NS_PER_SEC).to_string().parse().expect("ascii"),
    );
    metadata.insert(
        "x-ravel-end",
        (end_ns / NS_PER_SEC).to_string().parse().expect("ascii"),
    );
    request
}

fn statement_descriptor() -> arrow_flight::FlightDescriptor {
    let command = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };
    arrow_flight::FlightDescriptor::new_cmd(command.as_any().encode_to_vec())
}

async fn plaintext_client(grpc: std::net::SocketAddr) -> FlightServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{grpc}"))
        .expect("valid endpoint uri")
        .connect()
        .await
        .expect("connect to the public gRPC listener");
    FlightServiceClient::new(channel)
}

/// The pinned-CA TLS configuration for the dedicated listener, presenting the
/// fragment certificate as client identity when `with_identity` is set.
fn fragment_tls(with_identity: bool) -> ClientTlsConfig {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(TEST_FRAGMENT_CA_PEM))
        .domain_name(FRAGMENT_SERVER_NAME);
    if with_identity {
        tls.identity(Identity::from_pem(
            TEST_FRAGMENT_CERT_PEM,
            TEST_FRAGMENT_KEY_PEM,
        ))
    } else {
        tls
    }
}

/// Dial the dedicated listener at `addr` over TLS. Under TLS 1.3 a missing
/// client certificate may be refused at connect or on the first RPC, so the
/// connect error is returned rather than unwrapped.
async fn dedicated_client(
    addr: std::net::SocketAddr,
    with_identity: bool,
) -> Result<FlightServiceClient<Channel>, tonic::transport::Error> {
    let channel = Channel::from_shared(format!("https://{addr}"))
        .expect("valid endpoint uri")
        .tls_config(fragment_tls(with_identity))
        .expect("tls config")
        .connect()
        .await?;
    Ok(FlightServiceClient::new(channel))
}

/// One Flight SQL statement against `grpc`: `GetFlightInfo`, then `DoGet` on
/// its single endpoint. The status is returned rather than unwrapped so the
/// caller can retry while the coordinator's roster fills.
async fn run_query(
    grpc: std::net::SocketAddr,
    start_ns: i64,
    end_ns: i64,
) -> Result<arrow::record_batch::RecordBatch, String> {
    let mut client = plaintext_client(grpc).await;
    let info = client
        .get_flight_info(authed(statement_descriptor(), start_ns, end_ns))
        .await
        .map_err(|s| format!("get_flight_info: {s}"))?
        .into_inner();
    assert_eq!(info.endpoint.len(), 1, "a client sees exactly one endpoint");
    let ticket = info.endpoint[0].ticket.clone().expect("endpoint ticket");
    let messages: Vec<FlightData> = client
        .do_get(authed(ticket, start_ns, end_ns))
        .await
        .map_err(|s| format!("do_get: {s}"))?
        .into_inner()
        .try_collect()
        .await
        .map_err(|s| format!("do_get stream: {s}"))?;
    Ok(to_batch(&messages))
}

/// The `(ts, value bits)` rows of a `SELECT ts, value` result.
fn rows(batch: &arrow::record_batch::RecordBatch) -> Vec<(i64, u64)> {
    use arrow::array::{Array, Float64Array};
    let ts = arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Int64)
        .expect("ts casts to i64");
    let ts = ts
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("i64");
    let value = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("value is f64");
    (0..batch.num_rows())
        .map(|i| (ts.value(i), value.value(i).to_bits()))
        .collect()
}

/// How many query-worker records the shared store holds.
async fn heartbeat_records(store: &MemoryStore) -> usize {
    store
        .list(ravel_fleet::query_workers::QUERY_WORKERS_PREFIX, None)
        .await
        .expect("list query workers")
        .objects
        .len()
}

/// Wait until the shared store holds `count` query-worker records.
async fn await_heartbeat_records(store: &MemoryStore, count: usize) {
    let deadline = Instant::now() + ROSTER_DEADLINE;
    while heartbeat_records(store).await < count {
        assert!(
            Instant::now() < deadline,
            "{count} query-worker records never appeared within {ROSTER_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The two-shard fixture: the same series on two shards sharing one sample,
/// so the query has two slices and a cross-slice duplicate the coordinator
/// must dedup. -0.0 is kept distinct from 0.0 by the bit comparison.
async fn publish_fixture(shared: &MemoryStore) -> (HashSet<String>, i64, i64) {
    let tenant = TenantId::new(TENANT);
    let base = now_ns() - 10 * NS_PER_MIN;
    let data_keys = [
        publish_segment(shared, &tenant, 0, &[(base, 1.5), (base + NS_PER_MIN, 2.5)]).await,
        publish_segment(
            shared,
            &tenant,
            1,
            &[(base, 1.5), (base + 2 * NS_PER_MIN, -0.0)],
        )
        .await,
    ]
    .into_iter()
    .collect();
    (data_keys, base - 5 * NS_PER_MIN, now_ns())
}

/// A valid slice capability for `TENANT` over no segments, minted under the
/// test's SQL ticket key exactly as a coordinator mints one.
fn slice_capability() -> Vec<u8> {
    let key: [u8; 32] = hex::decode(SQL_KEY)
        .expect("hex")
        .try_into()
        .expect("32 bytes");
    let now = now_ns();
    let ticket = FlightTicket {
        tenant: TenantId::new(TENANT).hash(),
        statement: String::new(),
        segments: Vec::new(),
        min_commit_tokens: Vec::new(),
        now_ns: now,
        deadline_ns: now + 60 * NS_PER_SEC,
        slice_index: 0,
        slice_count: 2,
        pending_erasure: Vec::new(),
        declared_columns: Vec::new(),
    };
    SqlTicketKeys::from_file_key(&key)
        .encode(&ticket, TicketSurface::Slice)
        .expect("encode")
}

/// A slice `DoGet`: the capability as the statement handle, and no metadata.
fn slice_do_get(handle: Vec<u8>) -> Request<Ticket> {
    let query = TicketStatementQuery {
        statement_handle: handle.into(),
    };
    Request::new(Ticket::new(query.as_any().encode_to_vec()))
}

/// Acceptance (ADR-1689 decision 1): two processes with `--fragment-listener`
/// run a distributed SQL query whose slices cross processes over the
/// dedicated TLS listener. The coordinator dials the worker's fragment
/// endpoint over `https`, never its public gRPC address; the result equals the
/// single-process result bit for bit; the worker read the segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_slice_fetch_rides_the_dedicated_tls_listener() {
    let material = Material::new();
    let shared = Arc::new(MemoryStore::new());
    let (data_keys, start_ns, end_ns) = publish_fixture(&shared).await;

    let worker_store = DataStore::new(shared.clone(), data_keys.clone(), false);
    let worker = start_server(worker_store.clone(), Some(material.distrib_settings())).await;
    let worker_fragment = worker.fragment_addr.expect("the dedicated listener binds");
    let worker_grpc = worker.grpc_addr.expect("gRPC binds in All mode");
    await_heartbeat_records(&shared, 1).await;
    let coordinator_store = DataStore::new(shared.clone(), data_keys, true);
    let coordinator =
        start_server(coordinator_store.clone(), Some(material.distrib_settings())).await;
    let coordinator_grpc = coordinator.grpc_addr.expect("gRPC binds in All mode");
    await_heartbeat_records(&shared, 2).await;

    // The location the coordinator dials a slice at, resolved exactly as its
    // `DoGet` resolves it. Until its first heartbeat read lists the worker the
    // roster is empty.
    let roster = coordinator
        .sql_slice_workers
        .clone()
        .expect("--distributed-query builds the SQL roster");
    let deadline = Instant::now() + ROSTER_DEADLINE;
    let locations = loop {
        let locations = roster.endpoints();
        if !locations.is_empty() {
            break locations;
        }
        assert!(
            Instant::now() < deadline,
            "the coordinator never listed the worker within {ROSTER_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        locations,
        vec![format!("https://{worker_fragment}")],
        "the slice location is the worker's dedicated TLS listener"
    );
    assert!(
        !locations
            .iter()
            .any(|location| location.contains(&worker_grpc.to_string())),
        "never the worker's public gRPC address {worker_grpc}: {locations:?}"
    );

    let deadline = Instant::now() + ROSTER_DEADLINE;
    let distributed = loop {
        match run_query(coordinator_grpc, start_ns, end_ns).await {
            Ok(batch) => break batch,
            Err(last) if Instant::now() >= deadline => panic!(
                "the cross-process query never succeeded within {ROSTER_DEADLINE:?}; last error: \
                 {last}"
            ),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    assert!(
        worker_store.data_gets.load(Ordering::SeqCst) > 0,
        "the worker process read the segments"
    );
    let worker_rejects = worker.sql_slice_rejects.clone().expect("Flight SQL served");
    assert_eq!(
        worker_rejects.get(SliceReject::WrongSurface),
        0,
        "no slice reached the worker's public listener"
    );

    let local = start_server(shared.clone(), None).await;
    let single = run_query(
        local.grpc_addr.expect("gRPC binds in All mode"),
        start_ns,
        end_ns,
    )
    .await
    .expect("the single-process query succeeds");
    let (distributed, single) = (rows(&distributed), rows(&single));
    assert_eq!(
        single.len(),
        3,
        "the cross-shard duplicate dedups: {single:?}"
    );
    assert_eq!(
        distributed, single,
        "the cross-process result equals the single-process result"
    );

    local.shutdown().await.expect("local shuts down");
    coordinator
        .shutdown()
        .await
        .expect("coordinator shuts down");
    worker.shutdown().await.expect("worker shuts down");
}

/// The mirror of `PublicFederation` refusing `Pinned`: once a dedicated
/// listener is configured, a valid slice capability presented on the public
/// gRPC listener is refused outright, typed and counted as `wrong_surface`.
/// The same capability is served on the dedicated listener, so the refusal is
/// about the surface, not the ticket.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slice_ticket_on_the_public_listener_is_refused_once_a_fragment_listener_is_set() {
    let material = Material::new();
    let node = start_server(
        Arc::new(MemoryStore::new()),
        Some(material.distrib_settings()),
    )
    .await;
    let rejects = node.sql_slice_rejects.clone().expect("Flight SQL served");

    let status = plaintext_client(node.grpc_addr.expect("gRPC binds"))
        .await
        .do_get(slice_do_get(slice_capability()))
        .await
        .expect_err("the public listener refuses a slice ticket");
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
    assert_eq!(status.message(), "slice fetch rejected: wrong_surface");
    assert_eq!(rejects.get(SliceReject::WrongSurface), 1);

    let mut dedicated = dedicated_client(node.fragment_addr.expect("listener binds"), true)
        .await
        .expect("a client presenting the fragment identity completes the handshake");
    let served: Vec<FlightData> = dedicated
        .do_get(slice_do_get(slice_capability()))
        .await
        .expect("the dedicated listener serves the same capability")
        .into_inner()
        .try_collect()
        .await
        .expect("the slice stream completes");
    assert!(!served.is_empty(), "the slice stream carries its schema");
    assert_eq!(
        rejects.by_reason().iter().map(|(_, n)| n).sum::<u64>(),
        1,
        "only the public-listener refusal was counted: {:?}",
        rejects.by_reason()
    );

    node.shutdown().await.expect("node shuts down");
}

/// The dedicated listener serves slice `DoGet` only: a client Flight SQL
/// method is refused with `permission_denied`, credential or not, and so is a
/// client whole-set ticket presented there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_flight_sql_method_on_the_dedicated_listener_is_permission_denied() {
    let material = Material::new();
    let shared = Arc::new(MemoryStore::new());
    let (_, start_ns, end_ns) = publish_fixture(&shared).await;
    let node = start_server(shared, Some(material.distrib_settings())).await;
    let mut dedicated = dedicated_client(node.fragment_addr.expect("listener binds"), true)
        .await
        .expect("a client presenting the fragment identity completes the handshake");

    let status = dedicated
        .get_flight_info(authed(statement_descriptor(), start_ns, end_ns))
        .await
        .expect_err("GetFlightInfo is a client method");
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");

    // A whole-set ticket, minted by the public listener for the same tenant.
    let mut public = plaintext_client(node.grpc_addr.expect("gRPC binds")).await;
    let info = public
        .get_flight_info(authed(statement_descriptor(), start_ns, end_ns))
        .await
        .expect("the public listener serves GetFlightInfo")
        .into_inner();
    let ticket = info.endpoint[0].ticket.clone().expect("endpoint ticket");
    let status = dedicated
        .do_get(authed(ticket, start_ns, end_ns))
        .await
        .expect_err("a client ticket is not a slice capability");
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
    assert_eq!(status.message(), "slice fetch rejected: wrong_surface");

    node.shutdown().await.expect("node shuts down");
}

/// The dedicated listener requires a client certificate from the pinned CA: a
/// slice `DoGet` from a peer that presents none fails the TLS handshake and
/// never reaches the capability check, which the same capability over a
/// channel presenting the fragment identity passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dedicated_listener_do_get_without_a_client_certificate_fails_the_handshake() {
    let material = Material::new();
    let node = start_server(
        Arc::new(MemoryStore::new()),
        Some(material.distrib_settings()),
    )
    .await;
    let fragment = node.fragment_addr.expect("listener binds");
    let rejects = node.sql_slice_rejects.clone().expect("Flight SQL served");

    match dedicated_client(fragment, false).await {
        Err(_) => {}
        Ok(mut anonymous) => {
            let status = anonymous
                .do_get(slice_do_get(slice_capability()))
                .await
                .expect_err("a peer with no client certificate is not served");
            let detail = format!("{status:?}");
            assert!(
                detail.contains("CertificateRequired"),
                "the TLS layer's own alert for a missing client certificate: {detail}"
            );
        }
    }
    assert_eq!(
        rejects.by_reason().iter().map(|(_, n)| n).sum::<u64>(),
        0,
        "the request never reached the capability check"
    );

    let mut authenticated = dedicated_client(fragment, true)
        .await
        .expect("a client presenting the fragment identity completes the handshake");
    authenticated
        .do_get(slice_do_get(slice_capability()))
        .await
        .expect("the same capability is served with a client certificate");

    node.shutdown().await.expect("node shuts down");
}
