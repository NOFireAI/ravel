//! End-to-end Flight SQL against a MinIO-backed `ravel-server` with a pinned
//! arrow-flight SQL client.
//!
//! Every other Flight test in this workspace runs over `MemoryStore`. This one
//! closes the last gap: object storage is the only durable backend (CLAUDE.md
//! invariant), so an e2e that never touches S3 never proves the two-RPC flow
//! survives a real bucket -- segment PUTs, commit-record CAS, catalog LIST
//! fan-out, and the pinned `DoGet` re-fetch all going over the network to
//! MinIO. The client is arrow-flight's own [`FlightSqlServiceClient`], the
//! same driver an external BI tool would use, so the ticket it receives from
//! `GetFlightInfo` and replays at `DoGet` is exercised exactly as a real client
//! exercises it.
//!
//! No new dependency: `arrow-flight` (with its `flight-sql` feature) is already
//! the server's Flight dependency, `ravel-object-store` already carries the S3
//! adapter, and `tonic`'s `channel` feature is already a dev-dependency for the
//! other Flight tests. This file adds only test code.
//!
//! `#[ignore]`d by default because it needs a Docker daemon reachable by the
//! executing user (in the `docker` group or root, a usable
//! `/var/run/docker.sock`) and the ability to pull or already have the pinned
//! `minio/minio` and `minio/mc` images. Run explicitly with:
//!
//! ```text
//! cargo test -p ravel-server --features flight-sql --test flight_sql_e2e -- --ignored
//! ```
//!
//! Like `remote_write_prometheus_e2e.rs`, it was authored but not executed in
//! the fleet environment it was written in: that host has no Docker socket
//! access and no egress to pull images. The in-process `flight_sql.rs` (real
//! tonic channel, `MemoryStore`) and ravel-sql's `flight_differential.rs` /
//! `flight_tenancy.rs` (bit-identical parity, tenancy) cover the behaviour; this
//! file substitutes a real object store and a real Flight SQL driver for the
//! in-memory doubles.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tonic::transport::Channel;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small on purpose: `Catalog::resolve` issues one LIST per (shard,
/// ingest-hour) pair across the window, so a wall-clock value would fan the
/// listing out to hundreds of thousands of round trips against MinIO.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

const QUERY: &str = "SELECT ts, value FROM samples ORDER BY ts";

// Pinned images so the test is reproducible across runs.
const MINIO_IMAGE: &str = "minio/minio:RELEASE.2024-10-13T13-34-11Z";
const MC_IMAGE: &str = "minio/mc:RELEASE.2024-10-08T09-37-31Z";
const MINIO_USER: &str = "minioadmin";
const MINIO_PASSWORD: &str = "minioadmin";
const BUCKET: &str = "ravel-flight-e2e";
/// MinIO S3 API port, off the default 9000 to avoid colliding with a local
/// MinIO a developer may already be running.
const MINIO_PORT: u16 = 19900;

const ACME_TOKEN: &str = "acme-token";
const OTHER_TOKEN: &str = "other-token";

// ---------------------------------------------------------------------------
// MinIO container lifecycle
// ---------------------------------------------------------------------------

/// A MinIO container plus the bucket the server writes into. Dropping it stops
/// the container so a panicking test does not leak it.
struct Minio {
    container: String,
    endpoint: String,
}

impl Minio {
    async fn start() -> Minio {
        let container = format!("ravel-flight-e2e-{}", std::process::id());
        let endpoint = format!("http://127.0.0.1:{MINIO_PORT}");

        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--network",
                "host",
                "--name",
                &container,
                "-e",
                &format!("MINIO_ROOT_USER={MINIO_USER}"),
                "-e",
                &format!("MINIO_ROOT_PASSWORD={MINIO_PASSWORD}"),
                MINIO_IMAGE,
                "server",
                "/data",
                "--address",
                &format!("127.0.0.1:{MINIO_PORT}"),
                "--console-address",
                &format!("127.0.0.1:{}", MINIO_PORT + 1),
            ])
            .status()
            .expect("docker must be runnable in an environment that executes this ignored test");
        assert!(status.success(), "docker run failed to start MinIO");

        let minio = Minio {
            container,
            endpoint,
        };
        minio.wait_ready().await;
        minio.make_bucket();
        minio
    }

    /// Poll MinIO's readiness endpoint until it answers or the timeout passes.
    async fn wait_ready(&self) {
        let url = format!("{}/minio/health/ready", self.endpoint);
        let client = reqwest::Client::new();
        for _ in 0..60 {
            let ready = client
                .get(&url)
                .send()
                .await
                .map(|resp| resp.status().is_success())
                .unwrap_or(false);
            if ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("MinIO did not become ready within 30s");
    }

    /// Create the bucket with a one-shot `mc` container. `mc mb -p` is
    /// idempotent, so a re-run against a surviving volume does not fail.
    fn make_bucket(&self) {
        let script = format!(
            "mc alias set local {} {MINIO_USER} {MINIO_PASSWORD} && mc mb -p local/{BUCKET}",
            self.endpoint,
        );
        let status = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "host",
                "--entrypoint",
                "/bin/sh",
                MC_IMAGE,
                "-c",
                &script,
            ])
            .status()
            .expect("docker run mc");
        assert!(status.success(), "mc failed to create the bucket");
    }

    fn config(&self) -> S3Config {
        S3Config {
            bucket: BUCKET.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(self.endpoint.clone()),
            access_key_id: MINIO_USER.to_string(),
            secret_access_key: MINIO_PASSWORD.to_string(),
            allow_http: true,
            force_path_style: true,
            kms_key_id: None,
            session_token: None,
            credentials_file: None,
        }
    }
}

impl Drop for Minio {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["stop", &self.container])
            .status();
    }
}

// ---------------------------------------------------------------------------
// Fixture data
// ---------------------------------------------------------------------------

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// Publish one real segment plus its commit record for `tenant` onto the
/// S3-backed store, the same way the ingest path would.
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

    let writer_id = Uuid::from_u128(4_000);
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

// ---------------------------------------------------------------------------
// Client helpers
// ---------------------------------------------------------------------------

/// A Flight SQL client whose credential and event-time window are pinned in the
/// request headers, the way an ADBC/arrow-flight driver configures a session.
async fn client(grpc: &std::net::SocketAddr, token: &str) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{grpc}"))
        .expect("valid endpoint uri")
        .connect()
        .await
        .expect("Flight client connects");
    let mut client = FlightSqlServiceClient::new(channel);
    client.set_header("authorization", format!("Bearer {token}"));
    // The Flight SQL command carries no window; the server reads it from
    // metadata exactly as the HTTP endpoint reads it from the JSON body.
    client.set_header("x-ravel-start", "0");
    client.set_header("x-ravel-end", format!("{}", NOW_NS as f64 / 1e9));
    client
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a Docker daemon reachable by this user and the pinned minio/minio and minio/mc images"]
async fn flight_sql_against_minio_returns_rows_and_isolates_tenants() {
    let minio = Minio::start().await;
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(S3Store::new(minio.config()).expect("S3 store"));

    let acme = TenantId::new("acme".to_string());
    let other = TenantId::new("other".to_string());
    let samples = [(100i64, 1.5f64), (200, 2.5), (300, -0.0)];
    publish_segment(store.as_ref(), &acme, "cpu", &samples).await;
    // A distinct segment for the other tenant, so a leak would be visible.
    publish_segment(store.as_ref(), &other, "cpu", &[(100, 900.0)]).await;

    let mut tokens = HashMap::new();
    tokens.insert(ACME_TOKEN.to_string(), acme.clone());
    tokens.insert(OTHER_TOKEN.to_string(), other);
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);

    let config = ServerConfig {
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
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
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
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
    .expect("server starts");
    let grpc = running
        .grpc_addr
        .expect("query mode binds gRPC when flight-sql is on");

    // A statement query as acme: GetFlightInfo (execute) then DoGet.
    let mut acme_client = client(&grpc, ACME_TOKEN).await;
    let info = acme_client
        .execute(QUERY.to_string(), None)
        .await
        .expect("GetFlightInfo");
    let ticket = info
        .endpoint
        .first()
        .expect("one endpoint")
        .ticket
        .clone()
        .expect("endpoint carries a ticket");

    let batches = acme_client
        .do_get(ticket.clone())
        .await
        .expect("DoGet")
        .try_collect::<Vec<_>>()
        .await
        .expect("decode batches");
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, samples.len(), "every published sample comes back");
    let columns: Vec<String> = batches
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
    assert_eq!(columns, vec!["ts".to_string(), "value".to_string()]);

    // The same ticket, replayed with the other tenant's credentials, is denied
    // before the pinned snapshot is touched: the ticket's tenant is checked,
    // never trusted.
    let mut other_client = client(&grpc, OTHER_TOKEN).await;
    let denied = other_client
        .do_get(ticket)
        .await
        .expect_err("cross-tenant redemption is denied over the wire");
    let status = flight_status(denied);
    assert_eq!(
        status.code(),
        tonic::Code::PermissionDenied,
        "a ticket minted for acme must not be redeemable by other"
    );

    running.shutdown().await.expect("graceful shutdown");
    drop(minio);
}

/// Unwrap the `tonic::Status` inside an arrow-flight client error, so the test
/// can assert on the gRPC code the server actually returned.
fn flight_status(err: arrow_flight::error::FlightError) -> tonic::Status {
    match err {
        arrow_flight::error::FlightError::Tonic(status) => *status,
        other => panic!("expected a tonic status, got: {other}"),
    }
}
