//! End-to-end reachability for the bucket-protection startup gate.
//!
//! `services/ravel-server/src/bucket_protection.rs`'s unit tests drive
//! `enforce` with fixture reports. These tests drive the path the binary runs:
//! a `Cli` parsed from command-line arguments, `store::build_store` on it, and
//! `bucket_protection::enforce_at_startup_on` with the pieces `main.rs` hands
//! it, against a fake S3 endpoint that answers the bucket's control-plane
//! GETs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use axum::response::IntoResponse;
use clap::Parser;
use ravel_object_store::instrument::ControlPlaneMetricsSnapshot;
use ravel_server::bucket_protection::{
    BucketProtectionGauges, BucketProtectionOutcome, bucket_protection_gauges,
};
use ravel_server::config::Cli;
use ravel_server::store::{BuiltStore, build_store};
use tokio::sync::Mutex;

/// The gauges are process-global; every test here moves them.
static GAUGE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// One enabled rule over the whole bucket carrying every sanctioned action.
const COMPLIANT_LIFECYCLE: &str = "<LifecycleConfiguration><Rule><ID>ravel</ID>\
     <Status>Enabled</Status><Filter/>\
     <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays>\
     </NoncurrentVersionExpiration>\
     <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
     <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation>\
     </AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>";

/// The same rule with no `AbortIncompleteMultipartUpload` action.
const NO_ABORT_LIFECYCLE: &str = "<LifecycleConfiguration><Rule><ID>ravel</ID>\
     <Status>Enabled</Status><Filter/>\
     <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays>\
     </NoncurrentVersionExpiration>\
     <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
     </Rule></LifecycleConfiguration>";

struct FakeBucket {
    endpoint: String,
    served: Arc<AtomicU64>,
    served_bytes: Arc<AtomicU64>,
}

/// A versioned, Object-Lock-enabled bucket answering `?lifecycle` with
/// `lifecycle`. Counts every request and body byte it serves.
async fn spawn_fake_bucket(lifecycle: &'static str) -> FakeBucket {
    let served = Arc::new(AtomicU64::new(0));
    let served_bytes = Arc::new(AtomicU64::new(0));
    let (requests, bytes) = (Arc::clone(&served), Arc::clone(&served_bytes));
    let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
        let (requests, bytes) = (Arc::clone(&requests), Arc::clone(&bytes));
        async move {
            requests.fetch_add(1, Ordering::Relaxed);
            let query = uri.query().unwrap_or("");
            let body = match query.split(['=', '&']).next().unwrap_or("") {
                "versioning" => {
                    "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                }
                "lifecycle" => lifecycle,
                "object-lock" => {
                    "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                     </ObjectLockConfiguration>"
                }
                _ => return (axum::http::StatusCode::NOT_IMPLEMENTED, "").into_response(),
            };
            bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
            body.into_response()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    FakeBucket {
        endpoint: format!("http://{addr}"),
        served,
        served_bytes,
    }
}

fn s3_args(endpoint: &str) -> Vec<String> {
    [
        "ravel-server",
        "--store",
        "s3",
        "--s3-bucket",
        "ravel-test",
        "--s3-endpoint",
        endpoint,
        "--s3-access-key",
        "test",
        "--s3-secret-key",
        "test",
        "--require-bucket-protection",
    ]
    .iter()
    .map(|arg| arg.to_string())
    .collect()
}

fn build(args: Vec<String>) -> (Cli, BuiltStore) {
    let cli = Cli::try_parse_from(args).expect("flags parse");
    let built = build_store(&cli, ravel_server::config::DEFAULT_CACHE_MAX_BYTES)
        .expect("the store builds without network access");
    (cli, built)
}

/// The statement `main.rs` runs, with the pieces it hands the gate.
async fn startup_gate(
    cli: &Cli,
    built: &BuiltStore,
) -> anyhow::Result<Option<BucketProtectionOutcome>> {
    ravel_server::bucket_protection::enforce_at_startup_on(
        cli.require_bucket_protection,
        built.s3.as_deref(),
        built.foreground.as_ref(),
    )
    .await
}

fn gauges(unknown: u64, conditions_failed: u64, conditions_unknown: u64) -> BucketProtectionGauges {
    BucketProtectionGauges {
        unknown,
        conditions_failed,
        conditions_unknown,
    }
}

/// Asserts a compliant bucket read through `args` starts clean, sends
/// exactly the three control-plane GETs, and counts them into the store's
/// metrics handle.
async fn assert_compliant_startup(bucket: &FakeBucket, args: Vec<String>) {
    let (cli, built) = build(args);
    let outcome = startup_gate(&cli, &built)
        .await
        .expect("a compliant bucket starts")
        .expect("the flag is on");
    assert_eq!(outcome, BucketProtectionOutcome::default());
    assert_eq!(bucket_protection_gauges(), gauges(0, 0, 0));
    assert_eq!(bucket.served.load(Ordering::Relaxed), 3);
    assert_eq!(
        built.metrics.control_plane(),
        ControlPlaneMetricsSnapshot {
            requests: 3,
            calls: 3,
            response_bytes: bucket.served_bytes.load(Ordering::Relaxed),
        }
    );
}

/// Flipped line: `built.s3.as_deref()` in `startup_gate` (the argument
/// `main.rs` passes). Passing `None`, so the gate reads the wrapped
/// `dyn ObjectStoreBackend`, sends no GET and reports all seven checked
/// conditions unknown.
#[tokio::test]
async fn real_startup_path_reads_the_s3_bucket() {
    let _guard = GAUGE_TEST_LOCK.lock().await;
    let bucket = spawn_fake_bucket(COMPLIANT_LIFECYCLE).await;
    assert_compliant_startup(&bucket, s3_args(&bucket.endpoint)).await;
}

/// Under `--tenant-kms-config` the gate still reads the base store's bucket,
/// the one every per-tenant store writes to.
#[tokio::test]
async fn real_startup_path_reads_the_base_bucket_under_tenant_kms_config() {
    use std::io::Write;

    let _guard = GAUGE_TEST_LOCK.lock().await;
    let bucket = spawn_fake_bucket(COMPLIANT_LIFECYCLE).await;
    let mut file = tempfile::NamedTempFile::new().expect("create temp tenant-kms-config file");
    file.write_all(b"[tenants]\nacme = \"arn:aws:kms:us-east-1:111122223333:key/acme\"\n")
        .expect("write temp file");
    let mut args = s3_args(&bucket.endpoint);
    args.push("--tenant-kms-config".to_string());
    args.push(
        file.path()
            .to_str()
            .expect("temp path is valid utf-8")
            .to_string(),
    );
    assert_compliant_startup(&bucket, args).await;
}

/// A real bucket with no multipart-abort rule refuses through the startup
/// path, naming the condition, with the gauges pinned. The missing action also
/// fails `rule-scope` (no covering rule carries it), which is counted but is
/// not fatal, so the refusal names only `abort-multipart`.
#[tokio::test]
async fn real_startup_path_refuses_a_bucket_without_the_abort_rule() {
    let _guard = GAUGE_TEST_LOCK.lock().await;
    let bucket = spawn_fake_bucket(NO_ABORT_LIFECYCLE).await;
    let (cli, built) = build(s3_args(&bucket.endpoint));
    let err = startup_gate(&cli, &built)
        .await
        .expect_err("no abort rule must refuse to start");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("bucket-protection contract check failed; refusing to start"),
        "{msg}"
    );
    assert!(msg.contains("abort-multipart: "), "{msg}");
    assert!(!msg.contains("rule-scope: "), "{msg}");
    assert_eq!(bucket_protection_gauges(), gauges(0, 2, 0));
    assert_eq!(bucket.served.load(Ordering::Relaxed), 3);
}

/// Any backend other than S3 reaches the gate only through the
/// `ObjectStoreBackend` contract: every checked condition is unknown, the
/// process starts, and the gauges say so.
#[tokio::test]
async fn real_startup_path_on_memory_warns_and_sets_the_gauges() {
    let _guard = GAUGE_TEST_LOCK.lock().await;
    let (cli, built) = build(
        [
            "ravel-server",
            "--store",
            "memory",
            "--require-bucket-protection",
        ]
        .iter()
        .map(|arg| arg.to_string())
        .collect(),
    );
    assert!(built.s3.is_none());
    let outcome = startup_gate(&cli, &built)
        .await
        .expect("unknown must not refuse to start")
        .expect("the flag is on");
    assert_eq!(
        outcome,
        BucketProtectionOutcome {
            conditions_failed: 0,
            conditions_unknown: 7,
        }
    );
    assert_eq!(bucket_protection_gauges(), gauges(1, 0, 7));
}
