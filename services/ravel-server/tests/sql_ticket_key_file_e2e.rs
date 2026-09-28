//! `--sql-ticket-key-file` (ADR-1689 decision 2) through the server's own
//! startup path: `Cli` flags, `Cli::parse_distrib_settings`, and
//! `ravel_server::start`, for two processes that find each other through the
//! real query-worker heartbeat under `sys/query/workers/`.
//!
//! The two processes read one SQL ticket key file and DIFFERENT fragment key
//! files. The SQL ticket key the pre-ADR-1689 wiring derived from the first
//! fragment key would therefore differ between them, so every slice ticket the
//! coordinator mints would fail the worker's MAC. Only keys read from the SQL
//! ticket key file make the two agree.
//!
//! The coordinator's store refuses every GET of a published data object, so it
//! cannot answer any part of the query itself: neither a whole-set local run
//! nor the coordinator-local fallback a failed slice falls through to. The
//! query succeeds only when the worker, in the other process, verified the
//! slice tickets and read the segments; the worker's own data GET count is
//! asserted as well. The result is compared with a single-process server's
//! result over the same data, row for row and bit for bit.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_flight::FlightData;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
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
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tonic::Request;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const QUERY: &str = "SELECT ts, value FROM samples ORDER BY ts";

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

/// How long the coordinator gets to see the worker in its heartbeat live set.
/// The first heartbeat of each process writes its record and reads the live set
/// immediately, so this is only the time for two spawned tasks to run.
const ROSTER_DEADLINE: Duration = Duration::from_secs(20);

const OLD_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const NEW_KEY: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const FRAGMENT_KEY_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FRAGMENT_KEY_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

/// A key file holding `keys`, one per line, first line first.
fn key_file(keys: &[&str]) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("temp key file");
    std::fs::write(file.path(), format!("# test keys\n{}\n", keys.join("\n"))).expect("write");
    file
}

/// The distributed settings a real `ravel-server` process builds from its flags,
/// with the cost gate forced open so every statement fans out.
fn distrib_settings(
    fragment_key_file: &tempfile::NamedTempFile,
    sql_ticket_key_file: &tempfile::NamedTempFile,
) -> DistribSettings {
    let cli = Cli::try_parse_from([
        "ravel-server",
        "--mode",
        "all",
        "--listen-http",
        "127.0.0.1:0",
        "--listen-grpc",
        "127.0.0.1:0",
        "--distributed-query",
        "--fragment-key-file",
        fragment_key_file.path().to_str().expect("utf8"),
        "--sql-ticket-key-file",
        sql_ticket_key_file.path().to_str().expect("utf8"),
        "--distribute-bytes-threshold",
        "0",
        "--distribute-segments-threshold",
        "0",
    ])
    .expect("flags parse");
    cli.validate().expect("flags validate");
    cli.parse_distrib_settings()
        .expect("distributed settings parse")
        .expect("--distributed-query is on")
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
    let writer_id = uuid::Uuid::from_u128(5_000 + u128::from(shard));
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

/// One Flight SQL statement against `grpc`: `GetFlightInfo`, then `DoGet` on
/// its single endpoint. The status is returned rather than unwrapped so the
/// caller can retry while the coordinator's roster fills.
async fn run_query(
    grpc: std::net::SocketAddr,
    start_ns: i64,
    end_ns: i64,
) -> Result<arrow::record_batch::RecordBatch, String> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{grpc}"))
        .expect("valid endpoint uri")
        .connect()
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let mut client = FlightServiceClient::new(channel);
    let command = CommandStatementQuery {
        query: QUERY.to_string(),
        transaction_id: None,
    };
    let descriptor = arrow_flight::FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
    let info = client
        .get_flight_info(authed(descriptor, start_ns, end_ns))
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

/// A worker and a coordinator started on one shared store, with the published
/// data and the query window.
struct Pair {
    shared: Arc<MemoryStore>,
    worker: ravel_server::Running,
    worker_store: Arc<DataStore>,
    coordinator: ravel_server::Running,
    coordinator_grpc: std::net::SocketAddr,
    start_ns: i64,
    end_ns: i64,
}

impl Pair {
    async fn shutdown(self) {
        self.coordinator
            .shutdown()
            .await
            .expect("coordinator shuts down");
        self.worker.shutdown().await.expect("worker shuts down");
    }
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

/// Two processes on one shared store: a worker whose SQL ticket key file is
/// `worker_sql_keys` and a coordinator whose file is `coordinator_sql_keys`,
/// each with its own fragment key file. The worker's heartbeat record is in
/// the store before the coordinator starts, so the coordinator's first
/// heartbeat read lists it.
async fn start_pair(coordinator_sql_keys: &[&str], worker_sql_keys: &[&str]) -> Pair {
    let shared = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let base = now_ns() - 10 * NS_PER_MIN;
    // The same series on two shards sharing one sample: two slices, and a
    // cross-slice duplicate the coordinator must dedup. -0.0 is kept distinct
    // from 0.0 by the bit comparison.
    let data_keys: HashSet<String> = [
        publish_segment(
            shared.as_ref(),
            &tenant,
            0,
            &[(base, 1.5), (base + NS_PER_MIN, 2.5)],
        )
        .await,
        publish_segment(
            shared.as_ref(),
            &tenant,
            1,
            &[(base, 1.5), (base + 2 * NS_PER_MIN, -0.0)],
        )
        .await,
    ]
    .into_iter()
    .collect();
    let (start_ns, end_ns) = (base - 5 * NS_PER_MIN, now_ns());

    let coordinator_sql = key_file(coordinator_sql_keys);
    let worker_sql = key_file(worker_sql_keys);
    let coordinator_fragment = key_file(&[FRAGMENT_KEY_A]);
    let worker_fragment = key_file(&[FRAGMENT_KEY_B]);

    let worker_store = DataStore::new(shared.clone(), data_keys.clone(), false);
    let worker = start_server(
        worker_store.clone(),
        Some(distrib_settings(&worker_fragment, &worker_sql)),
    )
    .await;
    await_heartbeat_records(&shared, 1).await;
    let coordinator_store = DataStore::new(shared.clone(), data_keys.clone(), true);
    let coordinator = start_server(
        coordinator_store.clone(),
        Some(distrib_settings(&coordinator_fragment, &coordinator_sql)),
    )
    .await;
    let coordinator_grpc = coordinator.grpc_addr.expect("gRPC binds in All mode");
    await_heartbeat_records(&shared, 2).await;
    Pair {
        shared,
        worker,
        worker_store,
        coordinator,
        coordinator_grpc,
        start_ns,
        end_ns,
    }
}

/// Runs the statement on the coordinator of a [`start_pair`] and returns its
/// result next to a single-process server's result.
async fn cross_process_query(
    coordinator_sql_keys: &[&str],
    worker_sql_keys: &[&str],
) -> (Vec<(i64, u64)>, Vec<(i64, u64)>) {
    let pair = start_pair(coordinator_sql_keys, worker_sql_keys).await;

    // Until the coordinator's first heartbeat read lists the worker, it runs
    // the statement itself and its store refuses the data. Retry until the
    // roster holds the worker; a query that never succeeds fails with the last
    // error.
    let deadline = Instant::now() + ROSTER_DEADLINE;
    let distributed = loop {
        match run_query(pair.coordinator_grpc, pair.start_ns, pair.end_ns).await {
            Ok(batch) => break batch,
            Err(last) if Instant::now() >= deadline => panic!(
                "the cross-process query never succeeded within {ROSTER_DEADLINE:?}; last error: \
                 {last}"
            ),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    assert!(
        pair.worker_store.data_gets.load(Ordering::SeqCst) > 0,
        "the worker process read the segments"
    );

    let local = start_server(pair.shared.clone(), None).await;
    let single = run_query(
        local.grpc_addr.expect("gRPC binds in All mode"),
        pair.start_ns,
        pair.end_ns,
    )
    .await
    .expect("the single-process query succeeds");

    local.shutdown().await.expect("local shuts down");
    pair.shutdown().await;
    (rows(&distributed), rows(&single))
}

/// Acceptance (ADR-1689 decision 2): two processes sharing one
/// `--sql-ticket-key-file` and holding different `--fragment-key-file`
/// contents run a distributed SQL query whose slices cross processes, and the
/// result equals the single-process result exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_ticket_key_file_keys_slice_tickets_across_processes() {
    let (distributed, single) = cross_process_query(&[OLD_KEY], &[OLD_KEY]).await;
    assert_eq!(
        single.len(),
        3,
        "the cross-shard duplicate dedups: {single:?}"
    );
    assert_eq!(
        distributed, single,
        "the cross-process result equals the single-process result"
    );
}

/// Rotation: a worker whose key file is `[new, old]` verifies the slice tickets
/// of a coordinator still minting under `[old]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_ticket_key_file_rotation_verifies_tickets_minted_under_the_old_key() {
    let (distributed, single) = cross_process_query(&[OLD_KEY], &[NEW_KEY, OLD_KEY]).await;
    assert_eq!(
        single.len(),
        3,
        "the cross-shard duplicate dedups: {single:?}"
    );
    assert_eq!(
        distributed, single,
        "a ticket minted under the old key verifies under [new, old]"
    );
}

/// How long the negative control keeps querying once both heartbeat records
/// are in the store. The two positive cases above succeed well inside it.
const NEGATIVE_WINDOW: Duration = Duration::from_secs(5);

/// Negative control for the harness: a worker holding only `[new]` never
/// verifies the slice tickets of a coordinator minting under `[old]`, so with
/// the coordinator's own data reads refused the cross-process query fails on
/// every attempt and the worker reads nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_without_the_minting_key_fails_the_cross_process_query() {
    let pair = start_pair(&[OLD_KEY], &[NEW_KEY]).await;
    let deadline = Instant::now() + NEGATIVE_WINDOW;
    let mut attempts = 0;
    let mut last = String::new();
    while Instant::now() < deadline {
        match run_query(pair.coordinator_grpc, pair.start_ns, pair.end_ns).await {
            Ok(batch) => panic!(
                "a worker without the coordinator's minting key served the query: {:?}",
                rows(&batch)
            ),
            Err(err) => last = err,
        }
        attempts += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        attempts >= 5,
        "the window ran only {attempts} attempts; last error: {last}"
    );
    assert_eq!(
        pair.worker_store.data_gets.load(Ordering::SeqCst),
        0,
        "the worker read no segment for a ticket it could not verify"
    );
    pair.shutdown().await;
}
