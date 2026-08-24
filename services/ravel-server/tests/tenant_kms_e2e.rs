//! Reachability proof (ADR-0062 decision 1, ADR-0072 decision 2)
//! that `--tenant-kms-config` actually routes each tenant's data-object
//! PUTs through that tenant's own KMS key, end to end through the real
//! startup path (`build_store`) and the real ingest handler
//! (`handle_export_logs`), not just through `KmsRoutingStore`'s own unit
//! tests. `KmsRoutingStore`'s per-tenant builder always constructs a real
//! `S3Store` (crates/ravel-object-store/src/kms_routing.rs), so this runs a
//! minimal mock S3 HTTP server on loopback and inspects the SSE-KMS headers
//! the `object_store` crate's S3 client actually sends
//! (`x-amz-server-side-encryption-aws-kms-key-id`, confirmed against the
//! pinned `object_store` 0.14.1 source), rather than asserting against
//! `ravel-object-store` internals.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use clap::Parser;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_catalog::read_epochs_from_store;
use ravel_ingest::{
    AdmissionController, AdmissionLimits, IngestConfig, LogIngestRouter, SystemClock, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::LogIngestLimits;
use ravel_server::logs_ingest::{LogIngestState, handle_export_logs};
use ravel_server::store::build_store;
use ravel_server::tenant_kms::configure_tenant_kms;
use ravel_server::{Cli, StoreKind};
use ravel_types::TenantId;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const BUCKET: &str = "kms-e2e-bucket";
const SSE_KMS_KEY_HEADER: &str = "x-amz-server-side-encryption-aws-kms-key-id";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn export_request(body: &str, ts_ns: i64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns as u64,
                    observed_time_unix_nano: ts_ns as u64,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(body.to_string())),
                    }),
                    attributes: vec![string_kv("http.route", "/checkout")],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn log_state(
    store: Arc<dyn ObjectStoreBackend>,
    admission: Arc<AdmissionController>,
) -> LogIngestState {
    let router = Arc::new(LogIngestRouter::new(
        IngestConfig {
            shard_count: 1,
            ..IngestConfig::default()
        },
        store.clone(),
        Arc::new(SystemClock),
    ));
    LogIngestState {
        router,
        limits: LogIngestLimits::default(),
        ack_deadline: Duration::from_secs(5),
        admission,
        store,
        recovery: None,
        provisioning: None,
    }
}

async fn ingest_one(store: Arc<dyn ObjectStoreBackend>, tenant: &str, body: &str) {
    let admission = Arc::new(AdmissionController::new(
        Arc::new(SystemClock),
        AdmissionLimits::default(),
    ));
    let state = log_state(store, admission);
    handle_export_logs(
        &state,
        TenantId::new(tenant),
        WriteMode::Strict,
        export_request(body, now_ns()),
        now_ns(),
        None,
    )
    .await
    .expect("strict ingest commits durably");
}

// --- Mock S3 (loopback, path-style) -----------------------------------------
//
// Only what the real write path in this test actually exercises: object
// GET/HEAD and single-shot PUT with S3's conditional-write preconditions
// (`If-None-Match: *`, `If-Match: <etag>`) -- ravel-catalog's
// `record_key_epoch` bootstrap relies on real CAS semantics
// (crates/ravel-catalog/src/key_epoch.rs: `CreateIfAbsent` then
// `CasVersion`). No LIST is needed: nothing on the write path this test
// drives calls it (confirmed by grep over ravel-ingest, ravel-commit,
// ravel-catalog's key_epoch module).

#[derive(Debug, Clone)]
struct RecordedPut {
    key: String,
    sse_kms_key_id: Option<String>,
}

/// A multipart initiate (`POST /{bucket}/{key}?uploads`). SSE-KMS is a
/// property of the upload session, so the encryption header rides this request
/// and never the per-part PUTs; this is the request whose `sse_kms_key_id` the
/// multipart tests assert on, mirroring how [`RecordedPut`] captures it for a
/// single-shot PUT.
#[derive(Debug, Clone)]
struct RecordedInitiate {
    key: String,
    sse_kms_key_id: Option<String>,
}

#[derive(Default)]
struct MockS3State {
    objects: Mutex<HashMap<String, (Bytes, String)>>,
    puts: Mutex<Vec<RecordedPut>>,
    initiates: Mutex<Vec<RecordedInitiate>>,
    next_etag: AtomicU64,
    next_upload_id: AtomicU64,
}

impl MockS3State {
    fn new_etag(&self) -> String {
        format!("\"etag-{}\"", self.next_etag.fetch_add(1, Ordering::SeqCst))
    }

    fn new_upload_id(&self) -> String {
        format!(
            "upload-{}",
            self.next_upload_id.fetch_add(1, Ordering::SeqCst)
        )
    }
}

fn strip_quotes(s: &str) -> &str {
    s.trim_matches('"')
}

async fn mock_s3_handler(
    State(mock): State<Arc<MockS3State>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(key) = uri.path().strip_prefix(&format!("/{BUCKET}/")) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("build response");
    };
    let key = key.to_string();
    let query = uri.query().unwrap_or("");

    match method {
        // Multipart initiate: `POST /{bucket}/{key}?uploads`. The `uploads`
        // query key (distinct from `uploadId` on complete: "uploadId" has no
        // trailing `s`) marks it. SSE-KMS rides this request, so it is the one
        // whose encryption header the multipart tests assert on.
        Method::POST if query.contains("uploads") => {
            let sse_kms_key_id = headers
                .get(SSE_KMS_KEY_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            mock.initiates
                .lock()
                .expect("mock initiates lock")
                .push(RecordedInitiate {
                    key: key.clone(),
                    sse_kms_key_id,
                });
            let upload_id = mock.new_upload_id();
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <InitiateMultipartUploadResult>\
                 <Bucket>{BUCKET}</Bucket><Key>{key}</Key><UploadId>{upload_id}</UploadId>\
                 </InitiateMultipartUploadResult>"
            );
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .expect("build response")
        }
        // Multipart complete: `POST /{bucket}/{key}?uploadId={id}` with an XML
        // part list. The parts' bytes are not reassembled here; the tests read
        // no object back, only the initiate's encryption header.
        Method::POST => {
            let etag = mock.new_etag();
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <CompleteMultipartUploadResult>\
                 <Location>{base}/{BUCKET}/{key}</Location>\
                 <Bucket>{BUCKET}</Bucket><Key>{key}</Key><ETag>{etag}</ETag>\
                 </CompleteMultipartUploadResult>",
                base = "http://mock-s3",
            );
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .expect("build response")
        }
        // Multipart part upload: `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}`.
        // Per the real API this never carries the SSE-KMS header, so nothing to
        // record; only an ETag is owed back.
        Method::PUT if query.contains("uploadId") => Response::builder()
            .status(StatusCode::OK)
            .header("ETag", mock.new_etag())
            .body(Body::empty())
            .expect("build response"),
        Method::GET | Method::HEAD => {
            let objects = mock.objects.lock().expect("mock objects lock");
            match objects.get(&key) {
                Some((bytes, etag)) => {
                    let response_body = if method == Method::HEAD {
                        Bytes::new()
                    } else {
                        bytes.clone()
                    };
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("ETag", etag.clone())
                        .header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                        .header("Content-Length", response_body.len().to_string())
                        .body(Body::from(response_body))
                        .expect("build response")
                }
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("build response"),
            }
        }
        Method::PUT => {
            let if_none_match = headers
                .get("if-none-match")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let if_match = headers
                .get("if-match")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let sse_kms_key_id = headers
                .get(SSE_KMS_KEY_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);

            let mut objects = mock.objects.lock().expect("mock objects lock");
            let existing = objects.get(&key).cloned();

            if if_none_match.as_deref() == Some("*") && existing.is_some() {
                return Response::builder()
                    .status(StatusCode::PRECONDITION_FAILED)
                    .body(Body::empty())
                    .expect("build response");
            }
            if let Some(want_etag) = &if_match {
                let matches = existing
                    .as_ref()
                    .is_some_and(|(_, etag)| strip_quotes(etag) == strip_quotes(want_etag));
                if !matches {
                    return Response::builder()
                        .status(StatusCode::PRECONDITION_FAILED)
                        .body(Body::empty())
                        .expect("build response");
                }
            }

            let etag = mock.new_etag();
            objects.insert(key.clone(), (body, etag.clone()));
            drop(objects);
            mock.puts.lock().expect("mock puts lock").push(RecordedPut {
                key,
                sse_kms_key_id,
            });

            Response::builder()
                .status(StatusCode::OK)
                .header("ETag", etag)
                .body(Body::empty())
                .expect("build response")
        }
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .expect("build response"),
    }
}

struct MockS3Server {
    base_url: String,
    state: Arc<MockS3State>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl MockS3Server {
    async fn start() -> MockS3Server {
        let state = Arc::new(MockS3State::default());
        let app = Router::new()
            .fallback(mock_s3_handler)
            .with_state(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock S3 listener");
        let addr = listener.local_addr().expect("mock S3 addr");
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        MockS3Server {
            base_url: format!("http://{addr}"),
            state,
            shutdown: Some(tx),
            task,
        }
    }

    fn puts_for_prefix(&self, prefix: &str) -> Vec<RecordedPut> {
        self.state
            .puts
            .lock()
            .expect("mock puts lock")
            .iter()
            .filter(|p| p.key.starts_with(prefix))
            .cloned()
            .collect()
    }

    async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
    }
}

fn write_tenant_kms_file(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().expect("create temp tenant-kms-config file");
    file.write_all(contents.as_bytes())
        .expect("write temp tenant-kms-config file");
    file
}

fn s3_cli(endpoint: &str, tenant_kms_path: Option<&str>) -> Cli {
    let mut args = vec![
        "ravel-server".to_string(),
        "--store".to_string(),
        "s3".to_string(),
        "--s3-bucket".to_string(),
        BUCKET.to_string(),
        "--s3-endpoint".to_string(),
        endpoint.to_string(),
        "--s3-access-key".to_string(),
        "test".to_string(),
        "--s3-secret-key".to_string(),
        "test".to_string(),
    ];
    if let Some(path) = tenant_kms_path {
        args.push("--tenant-kms-config".to_string());
        args.push(path.to_string());
    }
    Cli::try_parse_from(args).expect("flags parse")
}

/// Same base flags as [`s3_cli`], but with `--s3-kms-key` instead of
/// `--tenant-kms-config`: the other, independent SSE-KMS mechanism
/// (ADR-0062 decision 1c, `S3Config::kms_key_id`) that `build_store` applies
/// unconditionally to the default `S3Store`, regardless of per-tenant
/// routing.
fn s3_cli_with_single_kms_key(endpoint: &str, kms_key: &str) -> Cli {
    let args = vec![
        "ravel-server".to_string(),
        "--store".to_string(),
        "s3".to_string(),
        "--s3-bucket".to_string(),
        BUCKET.to_string(),
        "--s3-endpoint".to_string(),
        endpoint.to_string(),
        "--s3-access-key".to_string(),
        "test".to_string(),
        "--s3-secret-key".to_string(),
        "test".to_string(),
        "--s3-kms-key".to_string(),
        kms_key.to_string(),
    ];
    Cli::try_parse_from(args).expect("flags parse")
}

/// The reachability proof: booting `ravel-server` with `--tenant-kms-config`
/// naming two tenants' keys routes each tenant's data-object PUTs to that
/// tenant's own key ARN, all the way through the real startup path
/// (`build_store`) and the real ingest handler (`handle_export_logs`) -- not
/// asserted against `KmsRoutingStore` directly.
#[tokio::test]
async fn tenant_kms_config_routes_each_tenants_puts_to_its_own_key() {
    let mock = MockS3Server::start().await;

    let acme_key = "arn:aws:kms:us-east-1:111122223333:key/acme-key";
    let other_key = "arn:aws:kms:us-east-1:111122223333:key/other-key";
    let kms_file = write_tenant_kms_file(&format!(
        "[tenants]\nacme = \"{acme_key}\"\nother = \"{other_key}\"\n"
    ));

    let cli = s3_cli(
        &mock.base_url,
        Some(kms_file.path().to_str().expect("temp path is utf-8")),
    );
    assert_eq!(cli.store, StoreKind::S3);

    let ravel_server::store::BuiltStore {
        foreground: store,
        kms,
        ..
    } = build_store(&cli).expect("build_store succeeds against the mock S3 endpoint");
    let kms = kms.expect("--tenant-kms-config must yield a KmsRoutingStore handle");

    let tenant_kms_config = cli
        .parse_tenant_kms_config()
        .expect("tenant-kms-config file parses");
    configure_tenant_kms(&kms, store.as_ref(), &tenant_kms_config, now_ns())
        .await
        .expect("tenant KMS bootstrap succeeds");

    ingest_one(store.clone(), "acme", "checkout completed").await;
    ingest_one(store.clone(), "other", "checkout completed").await;

    let acme_hash = TenantId::new("acme").hash();
    let other_hash = TenantId::new("other").hash();

    // The `t/<hash>/enc` key-epoch record is deliberately excluded from the
    // "carries this tenant's KMS key" assertion below: `configure_tenant_kms`
    // bootstraps the epoch history *before* calling `set_tenant_key`
    // (crates/ravel-catalog/src/key_epoch.rs's bootstrap contract -- record
    // the epoch, then start routing through it, never the other way around,
    // so a crash between the two never leaves data under a key with no
    // epoch record). `KmsRoutingStore` therefore has no route registered yet
    // when the `/enc` bootstrap PUTs happen, and they fall to the default
    // store. This is checked explicitly below, not glossed over.
    let is_data_object = |p: &&RecordedPut| !p.key.ends_with("/enc");

    let acme_puts = mock.puts_for_prefix(&format!("t/{}/", acme_hash.to_hex()));
    let other_puts = mock.puts_for_prefix(&format!("t/{}/", other_hash.to_hex()));

    let acme_data_puts: Vec<_> = acme_puts.iter().filter(is_data_object).collect();
    let other_data_puts: Vec<_> = other_puts.iter().filter(is_data_object).collect();

    assert!(
        !acme_data_puts.is_empty(),
        "tenant acme's ingest wrote no data objects"
    );
    assert!(
        !other_data_puts.is_empty(),
        "tenant other's ingest wrote no data objects"
    );
    assert!(
        acme_data_puts
            .iter()
            .all(|p| p.sse_kms_key_id.as_deref() == Some(acme_key)),
        "every data object under tenant acme's prefix must carry acme's KMS key ARN, got: {acme_puts:?}"
    );
    assert!(
        other_data_puts
            .iter()
            .all(|p| p.sse_kms_key_id.as_deref() == Some(other_key)),
        "every data object under tenant other's prefix must carry other's KMS key ARN, got: {other_puts:?}"
    );
    assert!(
        acme_puts
            .iter()
            .filter(|p| p.key.ends_with("/enc"))
            .all(|p| p.sse_kms_key_id.is_none()),
        "the key-epoch bootstrap record itself is written before set_tenant_key, so it must \
         carry no tenant KMS key: {acme_puts:?}"
    );

    let acme_epochs = read_epochs_from_store(store.as_ref(), &acme_hash)
        .await
        .expect("read acme key-epoch history")
        .expect("acme has a key-epoch history after configuration");
    assert_eq!(
        acme_epochs.len(),
        2,
        "bootstrap epoch 0 (deployment default) plus epoch 1 (the configured key): {acme_epochs:?}"
    );
    assert_eq!(acme_epochs[0].epoch, 0);
    assert_eq!(acme_epochs[0].key_arn, "");
    assert_eq!(acme_epochs[1].epoch, 1);
    assert_eq!(acme_epochs[1].key_arn, acme_key);
    assert!(
        acme_epochs[0].activated_ns <= acme_epochs[1].activated_ns,
        "epoch history must be monotonically activated: {acme_epochs:?}"
    );

    mock.stop().await;
}

/// Reachability proof for the OTHER SSE-KMS
/// mechanism `build_store` wires up -- `--s3-kms-key`, a single deployment-
/// wide key applied to the default `S3Store` unconditionally
/// (`services/ravel-server/src/store.rs`'s `kms_key_id: cli.s3_kms_key.clone()`),
/// independent of and reachable with no `--tenant-kms-config` at all. Before
/// this test, `--s3-kms-key` had unit coverage nowhere: `store.rs` has no
/// `#[cfg(test)]` module exercising the field, and this file's existing two
/// tests only ever drive `--tenant-kms-config`'s `KmsRoutingStore` path. A
/// typo turning `kms_key_id: cli.s3_kms_key.clone()` into
/// `kms_key_id: None` (or dropping the field from the built `S3Config`
/// entirely) would leave every real PUT unencrypted under this flag with no
/// test anywhere failing.
#[tokio::test]
async fn s3_kms_key_applies_to_every_put_with_no_tenant_kms_config() {
    let mock = MockS3Server::start().await;
    let key = "arn:aws:kms:us-east-1:111122223333:key/deployment-key";
    let cli = s3_cli_with_single_kms_key(&mock.base_url, key);
    assert_eq!(cli.store, StoreKind::S3);

    let ravel_server::store::BuiltStore {
        foreground: store,
        kms,
        ..
    } = build_store(&cli).expect("build_store succeeds against the mock S3 endpoint");
    assert!(
        kms.is_none(),
        "--s3-kms-key alone must not construct a KmsRoutingStore; it is a plain S3Config field"
    );

    ingest_one(store.clone(), "acme", "checkout completed").await;

    let all_puts = mock.state.puts.lock().expect("mock puts lock").clone();
    assert!(!all_puts.is_empty(), "the ingest wrote nothing");
    assert!(
        all_puts
            .iter()
            .all(|p| p.sse_kms_key_id.as_deref() == Some(key)),
        "every PUT this process makes, including the key-epoch bootstrap record, must carry \
         --s3-kms-key's ARN when no per-tenant routing overrides it: {all_puts:?}"
    );

    mock.stop().await;
}

/// The negative control every reachability claim needs: a server booted
/// without `--tenant-kms-config` builds exactly today's store (deliverable
/// #1's "off by default") and routes no PUT through any tenant KMS key.
#[tokio::test]
async fn no_tenant_kms_config_routes_no_puts_through_any_kms_key() {
    let mock = MockS3Server::start().await;
    let cli = s3_cli(&mock.base_url, None);

    let ravel_server::store::BuiltStore {
        foreground: store,
        kms,
        ..
    } = build_store(&cli).expect("build_store succeeds against the mock S3 endpoint");
    assert!(
        kms.is_none(),
        "without --tenant-kms-config, build_store must not construct a KmsRoutingStore"
    );

    ingest_one(store.clone(), "acme", "checkout completed").await;

    let all_puts = mock.state.puts.lock().expect("mock puts lock").clone();
    assert!(!all_puts.is_empty(), "the baseline ingest wrote nothing");
    assert!(
        all_puts.iter().all(|p| p.sse_kms_key_id.is_none()),
        "a non-KMS server must carry no SSE-KMS key header on any PUT: {all_puts:?}"
    );

    mock.stop().await;
}

/// The multipart companion to the single-shot PUT tests above, for
/// `--tenant-kms-config`. `KmsRoutingStore::put_multipart` already routes a
/// configured tenant's upload to that tenant's per-tenant `S3Store`; this
/// proves the SSE-KMS header actually reaches the wire on the multipart
/// *initiate* request (where the real S3 API carries it -- never the per-part
/// PUTs), the same shape the single-shot assertion checks. A single part is
/// always legal and final, so one `put_part` before `complete` is enough.
#[tokio::test]
async fn multipart_puts_carry_the_configured_tenant_kms_key_on_initiate() {
    let mock = MockS3Server::start().await;

    let acme_key = "arn:aws:kms:us-east-1:111122223333:key/acme-key";
    let other_key = "arn:aws:kms:us-east-1:111122223333:key/other-key";
    let kms_file = write_tenant_kms_file(&format!(
        "[tenants]\nacme = \"{acme_key}\"\nother = \"{other_key}\"\n"
    ));

    let cli = s3_cli(
        &mock.base_url,
        Some(kms_file.path().to_str().expect("temp path is utf-8")),
    );

    let ravel_server::store::BuiltStore {
        foreground: store,
        kms,
        ..
    } = build_store(&cli).expect("build_store succeeds against the mock S3 endpoint");
    let kms = kms.expect("--tenant-kms-config must yield a KmsRoutingStore handle");

    let tenant_kms_config = cli
        .parse_tenant_kms_config()
        .expect("tenant-kms-config file parses");
    configure_tenant_kms(&kms, store.as_ref(), &tenant_kms_config, now_ns())
        .await
        .expect("tenant KMS bootstrap succeeds");

    let acme_hash = TenantId::new("acme").hash();
    let key = format!("t/{}/multipart-object.rseg", acme_hash.to_hex());

    let mut upload = store
        .put_multipart(&key)
        .await
        .expect("multipart upload begins against acme's per-tenant store");
    upload
        .put_part(Bytes::from_static(b"multipart part payload"), None)
        .await
        .expect("single final part uploads");
    upload.complete().await.expect("multipart upload completes");

    let initiates = mock
        .state
        .initiates
        .lock()
        .expect("mock initiates lock")
        .clone();
    let for_key: Vec<_> = initiates.iter().filter(|i| i.key == key).collect();
    assert_eq!(
        for_key.len(),
        1,
        "exactly one multipart initiate expected for {key}, got: {initiates:?}"
    );
    assert_eq!(
        for_key[0].sse_kms_key_id.as_deref(),
        Some(acme_key),
        "the multipart initiate routed to acme's per-tenant store must carry acme's KMS key ARN"
    );

    mock.stop().await;
}

/// The multipart companion for the OTHER mechanism, `--s3-kms-key`: a single
/// deployment-wide key on the default `S3Store`, with no per-tenant routing.
/// The initiate must carry that key just as every single-shot PUT does.
#[tokio::test]
async fn multipart_puts_carry_the_single_s3_kms_key_on_initiate() {
    let mock = MockS3Server::start().await;
    let key_arn = "arn:aws:kms:us-east-1:111122223333:key/deployment-key";
    let cli = s3_cli_with_single_kms_key(&mock.base_url, key_arn);

    let ravel_server::store::BuiltStore {
        foreground: store,
        kms,
        ..
    } = build_store(&cli).expect("build_store succeeds against the mock S3 endpoint");
    assert!(
        kms.is_none(),
        "--s3-kms-key alone must not construct a KmsRoutingStore; it is a plain S3Config field"
    );

    let key = "t/deadbeef/multipart-object.rseg".to_string();
    let mut upload = store
        .put_multipart(&key)
        .await
        .expect("multipart upload begins against the default store");
    upload
        .put_part(Bytes::from_static(b"multipart part payload"), None)
        .await
        .expect("single final part uploads");
    upload.complete().await.expect("multipart upload completes");

    let initiates = mock
        .state
        .initiates
        .lock()
        .expect("mock initiates lock")
        .clone();
    let for_key: Vec<_> = initiates.iter().filter(|i| i.key == key).collect();
    assert_eq!(
        for_key.len(),
        1,
        "exactly one multipart initiate expected for {key}, got: {initiates:?}"
    );
    assert_eq!(
        for_key[0].sse_kms_key_id.as_deref(),
        Some(key_arn),
        "the multipart initiate must carry --s3-kms-key's deployment-wide ARN"
    );

    mock.stop().await;
}
