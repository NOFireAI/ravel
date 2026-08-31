//! HTTP-layer fault injection for [`S3Store`].
//!
//! `src/s3.rs`'s own unit tests pin error *classification* against synthetic
//! `object_store::Error` values: they prove `classify_generic` turns a "503"
//! text into `Throttled`, but nothing there proves `S3Store` ever issues a
//! second HTTP request after a 503, that the pause between attempts grows, or
//! that a multipart upload whose part fails leaves no object at the key. Those
//! are properties of the whole stack (adapter + `object_store` client +
//! transport), so they can only be observed from the other end of a socket.
//!
//! This file stands up a minimal fake S3 endpoint on 127.0.0.1 (axum, already a
//! workspace dependency), points `S3Store` at it through the *existing*
//! [`S3Config::endpoint`] override, and scripts per-request faults: 503, 429, an
//! S3 `SlowDown` error body (both as a 503 body and as the 200-with-error body
//! S3 documents for `CompleteMultipartUpload`), 403 `AccessDenied`, a connection
//! dropped mid-response, and a multipart sequence that fails after some parts
//! have already succeeded. Every request is timestamped, so the tests assert on
//! what the server *saw* (attempt counts, inter-attempt gaps) rather than only
//! on what the caller got back.
//!
//! The conformance target is docs/object-store-contract.md:
//!
//! - "Retry classification: `Throttled`, `Timeout`, `Transient` are retryable
//!   with jittered exponential backoff. ... `AccessDenied` is permanent."
//! - "Nothing is readable at `key` until `complete` returns `Ok`; an
//!   incomplete, aborted, dropped, or crashed upload never becomes a visible
//!   object, not even a truncated one."
//! - Handle poisoning: after a failed part, `complete` fails `Permanent` and
//!   `abort` stays callable.
//!
//! Retry policy itself lives in `object_store`'s client (the contract's
//! "Nothing retries internally beyond what `object_store`'s client already
//! does per request"), and `S3Config` deliberately exposes no knob for it, so
//! these tests run against its defaults: max 10 retries, decorrelated-jitter
//! backoff whose first pause is exactly `init_backoff` (100 ms) and whose later
//! pauses are drawn from `[init_backoff, 2 * previous)`. The backoff assertions
//! below are written against that shape without depending on any exact value;
//! see [`backoff_growth_verdict`].
//!
//! Integration-test binary, so `[lints] workspace = true` applies (including
//! `clippy::expect_used` as a hard error under `-D warnings`); the crate-level
//! allow matches `tests/contract.rs` and `tests/instrument.rs`.
#![allow(clippy::expect_used)]

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::Response;
use bytes::Bytes;
use parking_lot::Mutex;
use ravel_object_store::s3::{MULTIPART_THRESHOLD, S3Config, S3HttpConfig, S3Store};
use ravel_object_store::{
    GetRange, InstrumentedStore, ObjectStoreBackend, PutOptions, StoreError, StoreMetrics,
};

/// Bucket name the fake serves. Path-style requests put it in the first path
/// segment (`/{bucket}/{key}`), which is what `force_path_style: true` makes
/// `object_store` emit.
const BUCKET: &str = "ravel-fault-bucket";

/// Fixed `Last-Modified` for every response. `object_store`'s S3 header config
/// does not require it, but a real endpoint always sends one and parsing it is
/// part of the path under test.
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

/// Body cap for a request the fake reads. Above any part these tests upload
/// (8 MiB) with room to spare; a request over it is truncated to empty rather
/// than allocating without bound.
const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Fake S3 endpoint
// ---------------------------------------------------------------------------

/// The S3 request kinds this fake understands, which is exactly the set
/// [`S3Store`] issues. Faults are scripted per kind, so a test can fail
/// `UploadPart` while leaving `CreateMultipartUpload` and
/// `CompleteMultipartUpload` healthy.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum Op {
    Get,
    Put,
    Head,
    Delete,
    CreateMultipart,
    UploadPart,
    CompleteMultipart,
    AbortMultipart,
}

/// One scripted misbehavior. Each maps to a response a real S3-compatible
/// endpoint can produce; the comment on each names what the client is supposed
/// to do with it per docs/object-store-contract.md.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Fault {
    /// Not a fault: serve the request normally. Exists so a script can put a
    /// success *between* faults, which is how a test makes some parts of a
    /// multipart upload succeed before one fails.
    Pass,
    /// 503 with an `<Error><Code>ServiceUnavailable</Code>` body: retryable.
    ServiceUnavailable,
    /// 429 with an `<Error><Code>TooManyRequests</Code>` body: retryable
    /// throttling.
    TooManyRequests,
    /// 503 with an `<Error><Code>SlowDown</Code>` body: S3's explicit
    /// throttle signal, retryable.
    SlowDown,
    /// 200 whose *body* carries `SlowDown`. S3 documents this for
    /// `CompleteMultipartUpload`, and it is the case that makes SlowDown "a
    /// protocol signal, not a raw error": the status line says success.
    OkWithSlowDownBody,
    /// 403 `AccessDenied`: permanent, must not be retried at all.
    AccessDenied,
    /// A 200 whose body starts, then the connection dies before the declared
    /// `Content-Length` is delivered. The response headers already succeeded,
    /// so no retry layer covers this: it surfaces to the caller, and the
    /// contract requires it to surface as something retryable.
    DropMidResponse,
}

/// One request as the server saw it: which operation, which key, when, which
/// fault (if any) was served for it, and the byte range it asked for.
///
/// `range` is what makes "the adapter bounded this read" observable from the
/// server side rather than inferred from the bytes that came back.
#[derive(Clone, Debug)]
struct Seen {
    op: Op,
    key: String,
    at: Instant,
    fault: Option<Fault>,
    /// Inclusive `[start, end]` from the request's `Range` header, absent for
    /// an unranged request.
    range: Option<(u64, u64)>,
}

impl Seen {
    /// Bytes this request asked for, or `None` if it was unranged (i.e. asked
    /// for the whole object, however large that turns out to be).
    fn range_len(&self) -> Option<u64> {
        self.range.map(|(start, end)| end - start + 1)
    }
}

#[derive(Default)]
struct FakeState {
    /// Visible objects. A multipart upload only lands here on a successful
    /// `CompleteMultipartUpload`, which is what makes "no truncated object
    /// becomes visible" an observable property rather than an assumption.
    objects: Mutex<HashMap<String, Bytes>>,
    /// In-flight multipart uploads: upload id -> (part number, bytes).
    uploads: Mutex<HashMap<String, Vec<(u32, Bytes)>>>,
    /// Per-operation fault queue, consumed one entry per matching request.
    scripted: Mutex<HashMap<Op, VecDeque<Fault>>>,
    /// Per-operation fault applied to *every* matching request, after the
    /// scripted queue for that operation is empty.
    always: Mutex<HashMap<Op, Fault>>,
    log: Mutex<Vec<Seen>>,
    next_upload_id: Mutex<u64>,
}

impl FakeState {
    /// Pop the fault for this request: scripted queue first, then the
    /// per-operation persistent fault, else serve normally.
    fn take_fault(&self, op: Op) -> Option<Fault> {
        if let Some(queued) = self
            .scripted
            .lock()
            .get_mut(&op)
            .and_then(VecDeque::pop_front)
        {
            return Some(queued);
        }
        self.always.lock().get(&op).copied()
    }

    fn record(&self, op: Op, key: &str, fault: Option<Fault>, range: Option<(u64, u64)>) {
        self.log.lock().push(Seen {
            op,
            key: key.to_string(),
            at: Instant::now(),
            fault,
            range,
        });
    }
}

/// A running fake S3 endpoint plus the handles a test needs to script it and
/// to inspect what it saw.
struct FakeS3 {
    addr: SocketAddr,
    state: Arc<FakeState>,
}

impl FakeS3 {
    /// Bind an ephemeral port on the loopback interface and serve until the
    /// test's runtime shuts down. Ephemeral so tests never collide on a port.
    async fn start() -> FakeS3 {
        let state = Arc::new(FakeState::default());
        let app = Router::new()
            .fallback(handle)
            // The extractor-level 2 MiB default would reject the 8 MiB parts
            // `put()`'s multipart path sends; this handler reads the body
            // itself under MAX_REQUEST_BODY instead.
            .layer(DefaultBodyLimit::disable())
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the fake endpoint must bind a loopback port");
        let addr = listener
            .local_addr()
            .expect("a bound listener must report its address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        FakeS3 { addr, state }
    }

    /// An [`S3Store`] pointed at this endpoint through the existing
    /// [`S3Config::endpoint`] override. No new configuration surface: this is
    /// the same field a MinIO deployment sets.
    fn store(&self) -> S3Store {
        S3Store::new(self.config()).expect("a fake-endpoint S3Store must build")
    }

    /// An [`S3Store`] pointed at this endpoint that records billed HTTP requests
    /// (attempts, retries included) into `metrics` via its counting connector
    /// (issue #928). The same `Arc` is shared with an
    /// [`InstrumentedStore::with_metrics`] in the tests, so `attempts` and
    /// `calls` land in one snapshot.
    fn store_with_metrics(&self, metrics: Arc<StoreMetrics>) -> S3Store {
        S3Store::with_metrics(self.config(), metrics)
            .expect("a fake-endpoint S3Store must build with attempt metrics")
    }

    /// The same store under an explicit [`S3HttpConfig`]. Used by the bounded
    /// whole-object read tests: the read chunk is derived from
    /// `request_timeout`, so a shorter timeout gives a small enough chunk to
    /// exercise splitting on a test-sized object.
    fn store_with_http(&self, http: S3HttpConfig) -> S3Store {
        S3Store::with_http_config(self.config(), http).expect("a fake-endpoint S3Store must build")
    }

    fn config(&self) -> S3Config {
        S3Config {
            bucket: BUCKET.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(format!("http://{}", self.addr)),
            access_key_id: "fake-access-key".to_string(),
            secret_access_key: "fake-secret-key".to_string(),
            allow_http: true,
            force_path_style: true,
            kms_key_id: None,
            session_token: None,
            credentials_file: None,
            auth: Default::default(),
            instance_metadata_endpoint: None,
        }
    }

    /// Serve `faults` in order for the next N requests of `op`, then serve
    /// normally.
    fn script(&self, op: Op, faults: impl IntoIterator<Item = Fault>) {
        self.state
            .scripted
            .lock()
            .insert(op, faults.into_iter().collect());
    }

    /// Serve `fault` for every request of `op`, without limit.
    fn always(&self, op: Op, fault: Fault) {
        self.state.always.lock().insert(op, fault);
    }

    /// Put an object into the fake's storage without going through the client,
    /// so a GET test's request counts contain only the requests it made.
    fn seed(&self, key: &str, data: &[u8]) {
        self.state
            .objects
            .lock()
            .insert(key.to_string(), Bytes::copy_from_slice(data));
    }

    /// What is visible at `key` right now, server-side. `None` is the strong
    /// form of "no object became visible": it is checked in the fake's own
    /// storage, not only through a GET the client could have mis-served.
    fn object(&self, key: &str) -> Option<Bytes> {
        self.state.objects.lock().get(key).cloned()
    }

    fn requests(&self, op: Op) -> Vec<Seen> {
        self.state
            .log
            .lock()
            .iter()
            .filter(|seen| seen.op == op)
            .cloned()
            .collect()
    }

    fn count(&self, op: Op) -> usize {
        self.requests(op).len()
    }

    /// Wall-clock gaps between consecutive requests of `op`, as the server
    /// observed them. For a single client call these are the client's backoff
    /// pauses plus a sub-millisecond loopback round trip.
    fn gaps(&self, op: Op) -> Vec<Duration> {
        let seen = self.requests(op);
        seen.windows(2)
            .map(|pair| pair[1].at.saturating_duration_since(pair[0].at))
            .collect()
    }
}

/// Split `/{bucket}/{key...}` into the key. Ravel's keys are plain ASCII, which
/// `object_store::path::Path` round-trips unencoded.
fn key_of(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((_bucket, key)) => key.to_string(),
        None => String::new(),
    }
}

/// Query string to pairs. Values here (`uploadId`, `partNumber`) are plain
/// alphanumerics, so no percent-decoding is needed.
fn query_pairs(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((name, value)) => (name.to_string(), value.to_string()),
            None => (pair.to_string(), String::new()),
        })
        .collect()
}

fn classify(method: &Method, query: &HashMap<String, String>) -> Option<Op> {
    let has_upload_id = query.contains_key("uploadId");
    match *method {
        Method::POST if query.contains_key("uploads") => Some(Op::CreateMultipart),
        Method::POST if has_upload_id => Some(Op::CompleteMultipart),
        Method::PUT if has_upload_id => Some(Op::UploadPart),
        Method::DELETE if has_upload_id => Some(Op::AbortMultipart),
        Method::PUT => Some(Op::Put),
        Method::GET => Some(Op::Get),
        Method::HEAD => Some(Op::Head),
        Method::DELETE => Some(Op::Delete),
        _ => None,
    }
}

fn etag_of(data: &[u8]) -> String {
    format!("\"{:08x}\"", crc32c::crc32c(data))
}

fn s3_error_body(code: &str, message: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{message}</Message>\
         <RequestId>fake-request-id</RequestId></Error>"
    )
}

fn build(status: StatusCode, headers: Vec<(header::HeaderName, String)>, body: Body) -> Response {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder
        .body(body)
        .expect("the fake's responses are well formed")
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    build(
        status,
        vec![(header::CONTENT_TYPE, "application/xml".to_string())],
        Body::from(s3_error_body(code, message)),
    )
}

/// A response whose body starts and then stops short of the declared
/// `Content-Length`. hyper aborts the connection when the body stream errors,
/// so the client sees a message that ended early.
///
/// Answers a ranged request 206 with a matching `Content-Range`, exactly as a
/// real endpoint would. Serving 200 to a ranged request instead would be
/// rejected as a non-partial response before the body was ever read, so the
/// injected mid-body drop would never be what the test exercised.
fn drop_mid_response(headers: &HeaderMap) -> Response {
    let stream = futures::stream::iter(vec![
        Ok::<Bytes, std::io::Error>(Bytes::from_static(b"partial-object-prefix")),
        Err(std::io::Error::other(
            "injected mid-response connection drop",
        )),
    ]);
    let mut response_headers = vec![
        (header::ETAG, "\"dropped\"".to_string()),
        (header::LAST_MODIFIED, LAST_MODIFIED.to_string()),
    ];
    // Declared length is far more than the stream delivers in either case, so
    // the client's read ends early rather than looking like a complete short
    // object.
    let status = match requested_range(headers) {
        Some((start, end)) => {
            response_headers.push((
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", end + 1),
            ));
            response_headers.push((header::CONTENT_LENGTH, (end - start + 1).to_string()));
            StatusCode::PARTIAL_CONTENT
        }
        None => {
            response_headers.push((header::CONTENT_LENGTH, "65536".to_string()));
            StatusCode::OK
        }
    };
    build(status, response_headers, Body::from_stream(stream))
}

/// The inclusive `[start, end]` a `Range: bytes=start-end` header asks for.
/// `None` for an absent header or any form this fake does not need to parse
/// (an open-ended or suffix range), which the adapter's bounded reads never
/// send.
fn requested_range(headers: &HeaderMap) -> Option<(u64, u64)> {
    let spec = headers.get(header::RANGE)?.to_str().ok()?;
    let (start, end) = spec.strip_prefix("bytes=")?.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

/// Serve a `Range` header against `data`, or the whole object when absent.
/// Returns the response body slice plus the `Content-Range` header value for a
/// partial response.
fn apply_range(data: &Bytes, headers: &HeaderMap) -> Option<(Bytes, Option<String>)> {
    let Some(raw) = headers.get(header::RANGE) else {
        return Some((data.clone(), None));
    };
    let spec = raw.to_str().ok()?.strip_prefix("bytes=")?;
    let (start_text, end_text) = spec.split_once('-')?;
    let total = data.len() as u64;
    let (start, end_inclusive) = if start_text.is_empty() {
        // Suffix: the last N bytes.
        let suffix: u64 = end_text.parse().ok()?;
        (total.saturating_sub(suffix), total.saturating_sub(1))
    } else {
        let start: u64 = start_text.parse().ok()?;
        let end = match end_text.is_empty() {
            true => total.saturating_sub(1),
            false => end_text.parse::<u64>().ok()?.min(total.saturating_sub(1)),
        };
        (start, end)
    };
    if start > end_inclusive || start >= total {
        return None;
    }
    let slice = data.slice(start as usize..(end_inclusive as usize + 1));
    Some((
        slice,
        Some(format!("bytes {start}-{end_inclusive}/{total}")),
    ))
}

async fn handle(
    State(state): State<Arc<FakeState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let query = query_pairs(uri.query().unwrap_or_default());
    let key = key_of(uri.path());
    let Some(op) = classify(&method, &query) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "MethodNotAllowed",
            "the fake endpoint does not implement this request",
        );
    };
    // Read the request body before deciding anything: a fault response that
    // left an unread request body on the wire would look like a transport
    // failure to the client rather than the scripted status.
    let data = axum::body::to_bytes(body, MAX_REQUEST_BODY)
        .await
        .unwrap_or_default();

    let fault = state.take_fault(op);
    state.record(op, &key, fault, requested_range(&headers));

    match fault {
        Some(Fault::ServiceUnavailable) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "Reduce your request rate.",
        ),
        Some(Fault::TooManyRequests) => error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "TooManyRequests",
            "Too many requests.",
        ),
        Some(Fault::SlowDown) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "SlowDown",
            "Please reduce your request rate.",
        ),
        Some(Fault::OkWithSlowDownBody) => build(
            StatusCode::OK,
            vec![(header::CONTENT_TYPE, "application/xml".to_string())],
            Body::from(s3_error_body(
                "SlowDown",
                "Please reduce your request rate.",
            )),
        ),
        Some(Fault::AccessDenied) => error_response(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "Access Denied by the fake endpoint.",
        ),
        Some(Fault::DropMidResponse) => drop_mid_response(&headers),
        Some(Fault::Pass) | None => serve(&state, op, &key, &query, &headers, data),
    }
}

fn serve(
    state: &FakeState,
    op: Op,
    key: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    data: Bytes,
) -> Response {
    match op {
        Op::Put => {
            let etag = etag_of(&data);
            state.objects.lock().insert(key.to_string(), data);
            build(
                StatusCode::OK,
                vec![
                    (header::ETAG, etag),
                    (header::LAST_MODIFIED, LAST_MODIFIED.to_string()),
                ],
                Body::empty(),
            )
        }
        Op::Get => {
            let Some(object) = state.objects.lock().get(key).cloned() else {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    "The specified key does not exist.",
                );
            };
            let etag = etag_of(&object);
            let Some((body, content_range)) = apply_range(&object, headers) else {
                return error_response(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "InvalidRange",
                    "The requested range is not satisfiable.",
                );
            };
            let mut response_headers = vec![
                (header::ETAG, etag),
                (header::LAST_MODIFIED, LAST_MODIFIED.to_string()),
            ];
            let status = match &content_range {
                Some(value) => {
                    response_headers.push((header::CONTENT_RANGE, value.clone()));
                    StatusCode::PARTIAL_CONTENT
                }
                None => StatusCode::OK,
            };
            build(status, response_headers, Body::from(body))
        }
        Op::Head => {
            let Some(object) = state.objects.lock().get(key).cloned() else {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    "The specified key does not exist.",
                );
            };
            build(
                StatusCode::OK,
                vec![
                    (header::ETAG, etag_of(&object)),
                    (header::LAST_MODIFIED, LAST_MODIFIED.to_string()),
                    (header::CONTENT_LENGTH, object.len().to_string()),
                ],
                Body::empty(),
            )
        }
        Op::Delete => {
            state.objects.lock().remove(key);
            build(StatusCode::NO_CONTENT, vec![], Body::empty())
        }
        Op::CreateMultipart => {
            let upload_id = {
                let mut next = state.next_upload_id.lock();
                *next += 1;
                format!("fake-upload-{next}")
            };
            state.uploads.lock().insert(upload_id.clone(), Vec::new());
            build(
                StatusCode::OK,
                vec![(header::CONTENT_TYPE, "application/xml".to_string())],
                Body::from(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <InitiateMultipartUploadResult><Bucket>{BUCKET}</Bucket>\
                     <Key>{key}</Key><UploadId>{upload_id}</UploadId>\
                     </InitiateMultipartUploadResult>"
                )),
            )
        }
        Op::UploadPart => {
            let upload_id = query.get("uploadId").cloned().unwrap_or_default();
            let part_number: u32 = query
                .get("partNumber")
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let etag = etag_of(&data);
            let mut uploads = state.uploads.lock();
            let Some(parts) = uploads.get_mut(&upload_id) else {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                );
            };
            parts.push((part_number, data));
            build(StatusCode::OK, vec![(header::ETAG, etag)], Body::empty())
        }
        Op::CompleteMultipart => {
            let upload_id = query.get("uploadId").cloned().unwrap_or_default();
            // Read the parts without removing the upload: a retried complete
            // (the 200-with-SlowDown-body case) must find it again.
            let Some(mut parts) = state.uploads.lock().get(&upload_id).cloned() else {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                );
            };
            // Parts are assembled by part number, not by arrival order: an
            // implementation may upload them concurrently.
            parts.sort_by_key(|(number, _)| *number);
            let mut assembled = Vec::new();
            for (_, part) in &parts {
                assembled.extend_from_slice(part);
            }
            let assembled = Bytes::from(assembled);
            let etag = format!("\"{:08x}-{}\"", crc32c::crc32c(&assembled), parts.len());
            state.objects.lock().insert(key.to_string(), assembled);
            state.uploads.lock().remove(&upload_id);
            build(
                StatusCode::OK,
                vec![(header::CONTENT_TYPE, "application/xml".to_string())],
                Body::from(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <CompleteMultipartUploadResult><Bucket>{BUCKET}</Bucket>\
                     <Key>{key}</Key><ETag>{etag}</ETag>\
                     </CompleteMultipartUploadResult>"
                )),
            )
        }
        Op::AbortMultipart => {
            let upload_id = query.get("uploadId").cloned().unwrap_or_default();
            state.uploads.lock().remove(&upload_id);
            build(StatusCode::NO_CONTENT, vec![], Body::empty())
        }
    }
}

// ---------------------------------------------------------------------------
// Backoff-growth check
// ---------------------------------------------------------------------------

/// Lower bound every retry pause must clear. Well under `object_store`'s
/// 100 ms `init_backoff` (its smallest possible pause), and far above the
/// sub-millisecond gap a client that retried immediately would produce.
const MIN_BACKOFF: Duration = Duration::from_millis(60);

/// Decide whether a sequence of observed inter-attempt gaps looks like
/// jittered exponential backoff, as a `Result` so the property can itself be
/// tested against sequences that must be rejected (see
/// [`backoff_check_rejects_a_client_that_does_not_back_off`]).
///
/// No exact timing value is asserted; that would be flaky. What is asserted is
/// relative, and is chosen to be robust against `object_store`'s
/// *decorrelated* jitter, where each pause is drawn from
/// `[init_backoff, 2 * previous)` and so is **not** monotonically increasing:
///
/// 1. Every gap clears [`MIN_BACKOFF`]. A client that did not sleep between
///    attempts fails here.
/// 2. Some later gap exceeds the first one, and the mean of all gaps exceeds
///    the first one. The first pause is the scheme's floor (`init_backoff`
///    exactly, before any jitter is drawn) and every later pause is drawn from
///    a range whose lower end is that same floor, so "later pauses exceed the
///    first" is what growth means here. A fixed-delay client fails both.
fn backoff_growth_verdict(gaps: &[Duration]) -> Result<(), String> {
    if gaps.len() < 3 {
        return Err(format!(
            "need at least 3 observed retry gaps to judge growth, got {}",
            gaps.len()
        ));
    }
    for (index, gap) in gaps.iter().enumerate() {
        if *gap < MIN_BACKOFF {
            return Err(format!(
                "gap {index} was {gap:?}, under the {MIN_BACKOFF:?} floor: \
                 the client did not back off between attempts"
            ));
        }
    }
    let first = gaps[0];
    let largest_later = gaps[1..]
        .iter()
        .copied()
        .max()
        .unwrap_or(Duration::from_secs(0));
    if largest_later <= first {
        return Err(format!(
            "no later gap exceeded the first one ({first:?}); gaps were {gaps:?}: \
             the delay between attempts did not grow"
        ));
    }
    let total: Duration = gaps.iter().sum();
    let mean = total / gaps.len() as u32;
    if mean <= first {
        return Err(format!(
            "mean gap {mean:?} did not exceed the first gap {first:?}; gaps were {gaps:?}"
        ));
    }
    Ok(())
}

/// [`backoff_growth_verdict`] is not vacuous: it rejects the two shapes a
/// broken client produces. Without this, "the gaps grew" could be a property
/// no observation can fail.
#[test]
fn backoff_check_rejects_a_client_that_does_not_back_off() {
    // A client that retries immediately.
    let immediate = vec![Duration::from_micros(200); 6];
    assert!(
        backoff_growth_verdict(&immediate).is_err_and(|why| why.contains("did not back off")),
        "an immediate-retry client must be rejected"
    );

    // A client that sleeps a fixed interval: real delay, no growth.
    let fixed = vec![Duration::from_millis(100); 6];
    assert!(
        backoff_growth_verdict(&fixed).is_err_and(|why| why.contains("did not grow")),
        "a fixed-delay client must be rejected"
    );

    // Too few samples to judge.
    assert!(backoff_growth_verdict(&[Duration::from_millis(100)]).is_err());

    // A jittered, growing sequence of the shape object_store produces passes,
    // including a non-monotonic dip in the middle.
    let jittered = [100, 180, 140, 260, 300, 240]
        .map(Duration::from_millis)
        .to_vec();
    assert_eq!(backoff_growth_verdict(&jittered), Ok(()));
}

// ---------------------------------------------------------------------------
// Retry behavior over HTTP
// ---------------------------------------------------------------------------

/// A GET that meets a 503, a 429, and an S3 `SlowDown` body in turn still
/// returns the object: each is retryable per the contract's retry
/// classification, and the assertion is on the *server's* view (four GETs
/// arrived) so a client that never retried could not pass it.
#[tokio::test]
async fn get_retries_through_503_429_and_slow_down() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/get-retry", b"the object survives throttling");
    fake.script(
        Op::Get,
        [
            Fault::ServiceUnavailable,
            Fault::TooManyRequests,
            Fault::SlowDown,
        ],
    );

    let outcome = store
        .get("fault/get-retry", GetRange::Full)
        .await
        .expect("a retryable 503/429/SlowDown sequence must not fail the get");
    assert_eq!(&outcome.data[..], b"the object survives throttling");

    let attempts = fake.requests(Op::Get);
    assert_eq!(
        attempts.len(),
        4,
        "three retryable faults plus one success must be four GETs, saw {attempts:?}"
    );
    assert_eq!(
        attempts.iter().filter(|seen| seen.fault.is_some()).count(),
        3,
        "all three scripted faults must have been served"
    );
}

/// The same for a PUT: the payload is re-sent on each retry and the object
/// that finally lands is the one the caller offered, not a partial body from a
/// throttled attempt.
#[tokio::test]
async fn put_retries_through_503_and_slow_down() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.script(Op::Put, [Fault::ServiceUnavailable, Fault::SlowDown]);

    store
        .put(
            "fault/put-retry",
            Bytes::from_static(b"payload that outlives two throttles"),
            PutOptions::default(),
        )
        .await
        .expect("a retryable throttle sequence must not fail the put");

    assert_eq!(
        fake.count(Op::Put),
        3,
        "two retryable faults plus one success must be three PUTs"
    );
    assert_eq!(
        fake.object("fault/put-retry").as_deref(),
        Some(&b"payload that outlives two throttles"[..]),
        "the retried PUT must land the caller's exact bytes"
    );
}

/// `AccessDenied` is permanent per the contract, and the proof is that the
/// server sees exactly one request: the same request counter that reads 4 in
/// [`get_retries_through_503_429_and_slow_down`] reads 1 here, so neither
/// number can be an artifact of the harness.
#[tokio::test]
async fn access_denied_is_never_retried() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/forbidden", b"unreachable");
    fake.always(Op::Get, Fault::AccessDenied);

    let error = store
        .get("fault/forbidden", GetRange::Full)
        .await
        .expect_err("a 403 must fail the get");
    assert!(
        matches!(error, StoreError::AccessDenied(_)),
        "403 must map to AccessDenied, got {error:?}"
    );
    assert!(
        !error.is_retryable(),
        "AccessDenied is permanent: {error:?} must not be retryable"
    );
    assert_eq!(
        fake.count(Op::Get),
        1,
        "a permanent error must not be retried at all"
    );
}

/// An endpoint that throttles forever eventually gives up, and the error that
/// surfaces is retryable (so the caller's own backoff loop can take over)
/// rather than `Permanent`. The attempt count proves the client exhausted a
/// retry budget instead of failing on the first response.
///
/// **Retry classification per docs/object-store-contract.md.** The contract
/// classifies a throttle as `Throttled { retry_after_ms }`, and `s3.rs`'s
/// tier-2 heuristic has a branch for exactly that ("429"/"503"/"slow
/// down"/"throttl"). Over real HTTP the error text `classify_generic` sees is
/// `object_store`'s `RetryError` `Display`, which writes `", after {retries}
/// retries, max_retries: {n}, retry_timeout: {d} "` whenever `retries != 0`.
/// That literal `retry_timeout` substring once shadowed the throttle branch
/// (the bare `timeout` check ran first), classifying every exhausted-retry
/// throttle as `Timeout`; #1105 reordered the heuristic so the throttle branch
/// wins. The assertion below now pins `Throttled` specifically.
#[tokio::test]
async fn persistent_slow_down_surfaces_throttled_after_retrying() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/always-slow", b"never served");
    fake.always(Op::Get, Fault::SlowDown);

    let error = store
        .get("fault/always-slow", GetRange::Full)
        .await
        .expect_err("an endpoint that only ever throttles must eventually fail the get");
    assert!(
        matches!(error, StoreError::Throttled { .. }),
        "a persistent SlowDown must surface as Throttled per the contract \
         (#1105: retry_timeout no longer shadows the throttle branch), \
         got {error:?}"
    );
    assert!(
        error.is_retryable(),
        "a throttled endpoint must leave the caller a retryable error, got {error:?}"
    );
    // Bounded below rather than pinned: the exact budget is object_store's
    // default (10 retries), which a dependency bump may change; what must not
    // change is that a throttled request is retried many times before the
    // adapter gives up.
    assert!(
        fake.count(Op::Get) >= 4,
        "a throttled GET must be retried repeatedly, saw {} attempts",
        fake.count(Op::Get)
    );
}

/// Repeated throttling makes the pause between attempts grow. Asserted on the
/// gaps the *server* measured between consecutive GETs, through
/// [`backoff_growth_verdict`], which rejects both an immediate-retry client and
/// a fixed-delay one.
#[tokio::test]
async fn backoff_grows_between_throttled_attempts() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/backoff", b"served after six throttles");
    fake.script(Op::Get, [Fault::SlowDown; 6]);

    store
        .get("fault/backoff", GetRange::Full)
        .await
        .expect("the seventh attempt must succeed");

    let gaps = fake.gaps(Op::Get);
    assert_eq!(
        gaps.len(),
        6,
        "six faults plus one success must leave six inter-attempt gaps"
    );
    if let Err(why) = backoff_growth_verdict(&gaps) {
        panic!("observed retry gaps are not exponential backoff: {why}");
    }
}

/// A connection that dies mid-response is not covered by any retry layer (the
/// response headers already said 200), so it reaches the caller. The contract
/// requires it to arrive as a retryable error, which is what lets a caller's
/// own backoff loop recover; a `Permanent` here would strand a healthy object.
#[tokio::test]
async fn connection_dropped_mid_response_surfaces_retryable() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/truncated", b"body the client never fully receives");
    fake.always(Op::Get, Fault::DropMidResponse);

    let error = store
        .get("fault/truncated", GetRange::Full)
        .await
        .expect_err("a truncated response body must not be reported as a successful read");
    assert!(
        error.is_retryable(),
        "a transport failure must be retryable, got {error:?}"
    );
    assert!(
        matches!(
            error,
            StoreError::Transient(_) | StoreError::Timeout | StoreError::Throttled { .. }
        ),
        "a dropped connection must classify as a transport failure, got {error:?}"
    );
}

// ---------------------------------------------------------------------------
// Bounded whole-object reads
// ---------------------------------------------------------------------------

/// An [`S3HttpConfig`] whose read chunk is 1 MiB, so a test-sized object is
/// enough to exercise splitting.
///
/// 7 s of `request_timeout` minus the 6 s connect/TLS/first-byte allowance
/// leaves 1 s of transfer, which carries 625 000 bytes at the 5 Mbps floor the
/// bound is sized against; that is under the 1 MiB minimum chunk, so the bound
/// floors there. Asserted below rather than assumed, so a change to any of the
/// three constants surfaces here instead of silently changing what these tests
/// cover.
fn small_chunk_http() -> S3HttpConfig {
    S3HttpConfig {
        request_timeout: Duration::from_secs(7),
        ..Default::default()
    }
}

/// A distinguishable byte at every offset, so a read that concatenates its
/// pieces in the wrong order, drops one, or overlaps two fails on content and
/// not only on length.
fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// The property [`S3HttpConfig::request_timeout`]'s stated criterion rests on:
/// no single request carries more than the timeout can transfer at the floor
/// rate, *including* a whole-object read, whose size the data chooses rather
/// than this crate.
///
/// Before this was fixed, `GetRange::Full` mapped straight to an unranged GET,
/// so a 256 MiB L1 compaction part went out as one request against a 20 s
/// ceiling it needs ~410 s to satisfy at that floor rate: it could only time
/// out, and then spend the retry budget re-issuing a request that never fits.
/// The assertion is on what the *server* saw, so a client that still sent one
/// unranged GET cannot pass it.
#[tokio::test]
async fn whole_object_read_is_split_into_requests_the_timeout_can_carry() {
    let fake = FakeS3::start().await;
    let http = small_chunk_http();
    let bound = http.max_request_body_bytes();
    assert_eq!(
        bound,
        1024 * 1024,
        "this test's object size is written against a 1 MiB bound"
    );
    let store = fake.store_with_http(http);

    // Deliberately not a multiple of the bound: the final request is short, so
    // an off-by-one in the range arithmetic shows up as wrong bytes.
    let size = 5 * bound + 123;
    let object = patterned(size);
    fake.seed("fault/bounded-read", &object);

    let outcome = store
        .get("fault/bounded-read", GetRange::Full)
        .await
        .expect("a bounded whole-object read must return the object");

    // Complete-object semantics for the caller: same bytes, same total size.
    assert_eq!(outcome.data.len(), size, "every byte of the object arrived");
    assert_eq!(
        &outcome.data[..],
        &object[..],
        "bytes are exact and in order"
    );
    assert_eq!(outcome.total_size, size as u64);

    let gets = fake.requests(Op::Get);
    assert_eq!(
        gets.len(),
        size.div_ceil(bound),
        "the read must be issued as ceil(size / bound) requests, saw {gets:?}"
    );
    for seen in &gets {
        let len = seen
            .range_len()
            .expect("every request of a bounded read carries a Range header");
        assert!(
            len <= bound as u64,
            "a request asked for {len} bytes, above the {bound}-byte bound the \
             request timeout is sized for"
        );
    }
    // The ranges partition the object exactly: no gap, no overlap, so wire
    // bytes are the object's size and not more.
    let mut covered: Vec<(u64, u64)> = gets.iter().filter_map(|seen| seen.range).collect();
    covered.sort_unstable();
    let mut next = 0u64;
    for (start, end) in covered {
        assert_eq!(start, next, "ranges must be contiguous from 0");
        next = end + 1;
    }
    assert_eq!(
        next, size as u64,
        "the ranges together must cover exactly the object"
    );
}

/// The cost of the fix is bounded to objects that need it: an object at or
/// under the chunk bound is still exactly one request, as it was before.
/// Pinned because a request is the expensive unit on this store, and the
/// overwhelming majority of reads here (footers, index objects, commit
/// records) sit far below the bound.
#[tokio::test]
async fn whole_object_read_below_the_bound_stays_one_request() {
    let fake = FakeS3::start().await;
    let http = small_chunk_http();
    let bound = http.max_request_body_bytes();
    let store = fake.store_with_http(http);

    let object = patterned(bound);
    fake.seed("fault/exactly-one-chunk", &object);

    let outcome = store
        .get("fault/exactly-one-chunk", GetRange::Full)
        .await
        .expect("an object at the bound must read in one request");
    assert_eq!(&outcome.data[..], &object[..]);
    assert_eq!(
        fake.count(Op::Get),
        1,
        "an object at or below the bound costs exactly one request"
    );
}

/// A zero-byte object has no satisfiable range, so the bounded form cannot
/// express it and a real endpoint answers 416. The read must still succeed and
/// return an empty object, at the cost of one extra unranged request -- the one
/// case where the split path issues more requests than bytes require.
#[tokio::test]
async fn whole_object_read_of_an_empty_object_falls_back_to_unranged() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_http(small_chunk_http());
    fake.seed("fault/empty", b"");

    let outcome = store
        .get("fault/empty", GetRange::Full)
        .await
        .expect("a zero-byte object must read as an empty object, not an InvalidRange error");
    assert!(outcome.data.is_empty());
    assert_eq!(outcome.total_size, 0);

    let gets = fake.requests(Op::Get);
    assert_eq!(gets.len(), 2, "one refused ranged probe, then one unranged");
    assert!(
        gets[0].range.is_some() && gets[1].range.is_none(),
        "the fallback must be the unranged form, saw {gets:?}"
    );
}

/// A caller-supplied range is passed through untouched, however large. The
/// caller sized that request itself, and splitting it here would hide the cost
/// from the code that chose it; the bound exists for `GetRange::Full`, which
/// carries no caller-chosen size.
#[tokio::test]
async fn a_caller_supplied_range_is_never_split() {
    let fake = FakeS3::start().await;
    let http = small_chunk_http();
    let bound = http.max_request_body_bytes();
    let store = fake.store_with_http(http);

    let object = patterned(4 * bound);
    fake.seed("fault/caller-range", &object);

    let wanted = 3 * bound;
    let outcome = store
        .get("fault/caller-range", GetRange::Range(0, wanted as u64))
        .await
        .expect("a caller range must be served");
    assert_eq!(&outcome.data[..], &object[..wanted]);

    let gets = fake.requests(Op::Get);
    assert_eq!(
        gets.len(),
        1,
        "one caller range is one request, saw {gets:?}"
    );
    assert_eq!(gets[0].range_len(), Some(wanted as u64));
}

/// The bound is derived from `request_timeout`, so the criterion holds for a
/// configured value and not only for the default. Pins both ends of the clamp:
/// the default sits at the 8 MiB cap that the multipart part size sets, and a
/// very tight timeout floors at 1 MiB rather than shrinking without limit.
#[tokio::test]
async fn the_request_body_bound_tracks_the_configured_timeout() {
    let default = S3HttpConfig::default();
    assert_eq!(
        default.max_request_body_bytes(),
        8 * 1024 * 1024,
        "the default timeout's transfer budget is capped at the multipart part size"
    );

    let tight = S3HttpConfig {
        request_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    assert_eq!(
        tight.max_request_body_bytes(),
        1024 * 1024,
        "a timeout below the overhead allowance floors the bound rather than reaching zero"
    );

    let middling = S3HttpConfig {
        request_timeout: Duration::from_secs(14),
        ..Default::default()
    };
    assert_eq!(
        middling.max_request_body_bytes(),
        8 * 625_000,
        "8 s of transfer budget at the 5 Mbps floor is 5 MB, under the cap"
    );
}

// ---------------------------------------------------------------------------
// Multipart
// ---------------------------------------------------------------------------

/// One 5 MiB part: the contract's minimum for any part but the last, enforced
/// locally by `PartSequence`, so a multi-part upload must send at least this
/// much per non-final part.
fn part_bytes(fill: u8) -> Bytes {
    Bytes::from(vec![fill; ravel_object_store::MULTIPART_MIN_PART_SIZE])
}

/// A multipart upload whose second part fails after the first succeeded: the
/// caller gets `Permanent` from `complete`, `abort` still works, and no object
/// (least of all a one-part truncation of the intended two) ever becomes
/// visible at the key.
#[tokio::test]
async fn multipart_part_failure_yields_permanent_and_no_visible_object() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    // First part succeeds, second is refused permanently: the "fails after
    // some but not all parts have succeeded" sequence.
    let mut upload = store
        .put_multipart("fault/multipart-poison")
        .await
        .expect("CreateMultipartUpload must succeed");

    upload
        .put_part(part_bytes(1), None)
        .await
        .expect("the first part must upload");
    assert_eq!(fake.count(Op::UploadPart), 1);

    // Now fail the next part at the endpoint.
    fake.always(Op::UploadPart, Fault::AccessDenied);
    let failed = upload
        .put_part(part_bytes(2), None)
        .await
        .expect_err("a 403 on UploadPart must fail the part");
    assert!(
        matches!(failed, StoreError::AccessDenied(_)),
        "the first failure must surface the original cause, got {failed:?}"
    );
    assert_eq!(
        fake.count(Op::UploadPart),
        2,
        "a permanently refused part must not be retried on the wire"
    );
    assert!(
        fake.requests(Op::UploadPart)
            .iter()
            .all(|seen| seen.key == "fault/multipart-poison"),
        "every part must have been addressed to the upload's own key"
    );

    // The handle is poisoned: completing it is Permanent, never a publish of
    // the one part that did succeed.
    let completed = upload
        .complete()
        .await
        .expect_err("completing after a failed part must fail");
    assert!(
        matches!(completed, StoreError::Permanent(_)),
        "a poisoned handle must fail Permanent, got {completed:?}"
    );
    assert!(!completed.is_retryable());
    assert_eq!(
        fake.count(Op::CompleteMultipart),
        0,
        "no CompleteMultipartUpload may be issued for a poisoned upload"
    );

    // abort stays callable on a poisoned handle and reaches the endpoint.
    upload.abort().await.expect("abort must still work");
    assert_eq!(fake.count(Op::AbortMultipart), 1);

    // Nothing is visible at the key: not server-side, and not through a read.
    assert!(
        fake.object("fault/multipart-poison").is_none(),
        "a failed multipart upload must leave no object, not even a truncated one"
    );
    let read = store
        .get("fault/multipart-poison", GetRange::Full)
        .await
        .expect_err("the key must not be readable");
    assert!(matches!(read, StoreError::NotFound), "got {read:?}");
}

/// The same guarantee on the path production actually reaches: `S3Store::put`
/// above [`MULTIPART_THRESHOLD`] chunks internally, and a part refused by the
/// endpoint must abort the upload and surface the error with no object at the
/// key.
#[tokio::test]
async fn put_above_threshold_leaves_no_object_when_a_part_fails() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    // Three parts (8 MiB, 8 MiB, 1 byte), uploaded with bounded concurrency.
    // The first request to arrive is served, every later one is refused: some
    // parts succeed, the upload does not.
    fake.script(Op::UploadPart, [Fault::Pass]);
    fake.always(Op::UploadPart, Fault::AccessDenied);

    let payload = Bytes::from(vec![7u8; MULTIPART_THRESHOLD + 1]);
    let error = store
        .put("fault/big-object", payload, PutOptions::default())
        .await
        .expect_err("a refused part must fail the put");
    assert!(
        !error.is_retryable(),
        "a 403 on a part is permanent, got {error:?}"
    );

    assert_eq!(
        fake.count(Op::CreateMultipart),
        1,
        "the put must have taken the multipart path"
    );
    assert_eq!(
        fake.count(Op::CompleteMultipart),
        0,
        "a failed part must never be completed"
    );
    assert_eq!(
        fake.count(Op::AbortMultipart),
        1,
        "a failed multipart put must abort the upload so parts are not left billed"
    );
    assert!(
        fake.object("fault/big-object").is_none(),
        "no object may be visible at the key of a failed multipart put"
    );
}

/// When the best-effort abort of a failed multipart put *itself* fails, the
/// parts are orphaned on the bucket and billed until the
/// `AbortIncompleteMultipartUpload` lifecycle rule reaps them. #864 makes that
/// invisible failure countable: `multipart_abort_failures` must read exactly 1
/// (not `> 0`) for the one abort that erred, and `multipart_uploads_unreaped`
/// must also read 1 because the upload ended without a successful abort.
#[tokio::test]
async fn failed_multipart_abort_is_counted_exactly_once() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    // One part succeeds, the rest are refused: the put fails and must abort.
    fake.script(Op::UploadPart, [Fault::Pass]);
    fake.always(Op::UploadPart, Fault::AccessDenied);
    // And the abort request itself is refused, so the parts stay orphaned.
    // 403 is permanent, so object_store issues exactly one AbortMultipart.
    fake.always(Op::AbortMultipart, Fault::AccessDenied);

    let payload = Bytes::from(vec![9u8; MULTIPART_THRESHOLD + 1]);
    let error = store
        .put("fault/abort-fails", payload, PutOptions::default())
        .await
        .expect_err("a refused part must fail the put");
    assert!(
        !error.is_retryable(),
        "a 403 on a part is permanent, got {error:?}"
    );

    assert_eq!(
        fake.count(Op::AbortMultipart),
        1,
        "a failed multipart put must attempt exactly one abort"
    );
    assert_eq!(
        store.multipart_abort_failures(),
        1,
        "the one abort that returned an error must be counted exactly once"
    );
    assert_eq!(
        store.multipart_uploads_unreaped(),
        1,
        "an upload whose abort failed ended without a successful abort"
    );
}

/// The counter cannot be firing unconditionally: the same failed-part put with
/// a *healthy* abort endpoint leaves `multipart_abort_failures` at exactly 0.
/// The abort succeeds, so the upload was cleanly reaped and
/// `multipart_uploads_unreaped` is 0 too.
#[tokio::test]
async fn successful_multipart_abort_leaves_failure_counter_at_zero() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.script(Op::UploadPart, [Fault::Pass]);
    fake.always(Op::UploadPart, Fault::AccessDenied);
    // AbortMultipart is left healthy (default Pass), so the abort succeeds.

    let payload = Bytes::from(vec![9u8; MULTIPART_THRESHOLD + 1]);
    store
        .put("fault/abort-ok", payload, PutOptions::default())
        .await
        .expect_err("a refused part must fail the put");

    assert_eq!(
        fake.count(Op::AbortMultipart),
        1,
        "the failed put must still abort so parts are not left billed"
    );
    assert_eq!(
        store.multipart_abort_failures(),
        0,
        "a successful abort must not increment the failed-abort counter"
    );
    assert_eq!(
        store.multipart_uploads_unreaped(),
        0,
        "a cleanly aborted upload did not end without a successful abort"
    );
}

/// `SlowDown` inside a 200 response body is S3's documented behavior for
/// `CompleteMultipartUpload`, and it is a protocol signal rather than a
/// success: the client must retry it, and the upload must complete correctly
/// once the endpoint stops throttling.
#[tokio::test]
async fn slow_down_in_a_200_body_retries_complete_multipart() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.script(
        Op::CompleteMultipart,
        [Fault::OkWithSlowDownBody, Fault::OkWithSlowDownBody],
    );

    let mut upload = store
        .put_multipart("fault/slow-complete")
        .await
        .expect("CreateMultipartUpload must succeed");
    // A single part may be any non-empty size: it is the last one.
    upload
        .put_part(Bytes::from_static(b"one small part"), None)
        .await
        .expect("the only part must upload");
    upload
        .complete()
        .await
        .expect("complete must retry through the throttle and succeed");

    assert_eq!(
        fake.count(Op::CompleteMultipart),
        3,
        "two SlowDown bodies plus one success must be three completes"
    );
    assert_eq!(
        fake.object("fault/slow-complete").as_deref(),
        Some(&b"one small part"[..]),
        "the completed object must hold the uploaded part"
    );
}

// ---------------------------------------------------------------------------
// Billed-request (attempt) counting below the retry loop (issue #928)
// ---------------------------------------------------------------------------
//
// `InstrumentedStore` counts completed logical calls; below it, `object_store`
// retries each call, so one `get()` that retried is one `calls` but several
// billed HTTP requests. The counting HTTP connector (`S3Store::with_metrics`)
// records those requests as `attempts`. These tests drive `object_store`'s real
// retry loop over the fake endpoint and assert `attempts` against the count the
// *server* saw, so the figure is the true billed-request count, not an artifact.
//
// The fault-injection here is at the HTTP layer, not the `FaultStore`
// (`ObjectStoreBackend`) decorator the issue text names: `FaultStore` sits
// *above* `object_store`, so it returns one error per call and cannot exercise
// the retry loop *below* the trait boundary that creates the gap. This fake
// endpoint is the fault store for that layer, and `fake.count(op)` is its
// fired-fault counter (asserted below), the same role `FaultStore::fault_count`
// plays one layer up.

/// A GET retried three times records four billed attempts but one completed
/// call: `attempts - calls` is exactly the retry count, and it equals the
/// requests the server saw, so the counter is the true billed-request count.
///
/// To watch this assertion fail: change `record_attempt` in
/// `crates/ravel-object-store/src/s3/attempts.rs` from firing once per
/// `HttpService::call` to firing once per `connect` (or delete the call
/// entirely) and `attempts` collapses to 0 (or 1), so the
/// `attempts - calls == 3` line below fails.
#[tokio::test]
async fn attempts_exceed_calls_by_the_exact_retry_count() {
    let fake = FakeS3::start().await;
    let metrics = Arc::new(StoreMetrics::default());
    let store = InstrumentedStore::with_metrics(
        fake.store_with_metrics(Arc::clone(&metrics)),
        Arc::clone(&metrics),
    );
    fake.seed("attempts/get", b"served after three throttles");
    fake.script(
        Op::Get,
        [
            Fault::ServiceUnavailable,
            Fault::TooManyRequests,
            Fault::SlowDown,
        ],
    );

    store
        .get("attempts/get", GetRange::Full)
        .await
        .expect("three retryable faults must not fail the get");

    let snap = metrics.snapshot();
    // The fault store fired: three faults served, four GETs on the wire.
    assert_eq!(
        fake.count(Op::Get),
        4,
        "the endpoint must see one GET per attempt: three faulted, one served"
    );
    assert_eq!(
        fake.requests(Op::Get)
            .iter()
            .filter(|seen| seen.fault.is_some())
            .count(),
        3,
        "all three scripted faults must have fired"
    );
    // The new figure is the billed-request count, exact.
    assert_eq!(
        snap.get.attempts, 4,
        "attempts count every billed HTTP request, retries included"
    );
    assert_eq!(snap.get.calls, 1, "exactly one logical get completed");
    assert_eq!(
        snap.get.attempts - snap.get.calls,
        3,
        "attempts exceed calls by exactly the retry count, not merely by > 0"
    );
    assert_eq!(
        snap.get.attempts,
        fake.count(Op::Get) as u64,
        "attempts equal the server-observed billed requests"
    );
    // Per-operation attribution: a get's retries do not leak onto another op.
    assert_eq!(snap.put.attempts, 0, "no put was issued");
    assert_eq!(snap.head.attempts, 0, "no head was issued");
    assert_eq!(snap.delete.attempts, 0, "no delete was issued");
}

/// With no fault injected, one logical call is exactly one billed request:
/// `attempts == calls`, so the figure does not drift on a quiet path.
///
/// To watch this assertion fail: make `AttemptCountingService::call` record two
/// attempts per request (a stray double `record_attempt`) and the
/// `attempts == calls` lines below fail with 2 != 1.
#[tokio::test]
async fn attempts_equal_calls_with_no_faults() {
    let fake = FakeS3::start().await;
    let metrics = Arc::new(StoreMetrics::default());
    let store = InstrumentedStore::with_metrics(
        fake.store_with_metrics(Arc::clone(&metrics)),
        Arc::clone(&metrics),
    );
    fake.seed("attempts/quiet", b"served on the first try");

    store
        .get("attempts/quiet", GetRange::Full)
        .await
        .expect("a healthy get must succeed");
    store
        .head("attempts/quiet")
        .await
        .expect("a healthy head must succeed");

    let snap = metrics.snapshot();
    // No fault fired: exactly one request per op on the wire.
    assert_eq!(fake.count(Op::Get), 1, "a quiet get is one GET");
    assert_eq!(fake.count(Op::Head), 1, "a quiet head is one HEAD");
    assert_eq!(
        fake.requests(Op::Get)
            .iter()
            .filter(|seen| seen.fault.is_some())
            .count(),
        0,
        "no fault may have fired on the quiet path"
    );
    assert_eq!(snap.get.attempts, 1);
    assert_eq!(snap.get.calls, 1);
    assert_eq!(
        snap.get.attempts, snap.get.calls,
        "no retry: attempts equal calls on the get"
    );
    assert_eq!(snap.head.attempts, 1);
    assert_eq!(snap.head.calls, 1);
    assert_eq!(
        snap.head.attempts, snap.head.calls,
        "no retry: attempts equal calls on the head"
    );
}

/// Attribution is per operation kind (the axis `FaultStore` injects on): a
/// retried PUT charges its retries to `put`, by the exact fault count, and to no
/// other op.
///
/// To watch this assertion fail: drop the `attempts::scope(StoreOp::Put, ..)`
/// wrapper from `S3Store::put` so the connector sees no scoped op for the
/// request; `snap.put.attempts` then reads 0 and the `== 3` line fails.
#[tokio::test]
async fn attempts_are_attributed_to_the_issuing_operation() {
    let fake = FakeS3::start().await;
    let metrics = Arc::new(StoreMetrics::default());
    let store = InstrumentedStore::with_metrics(
        fake.store_with_metrics(Arc::clone(&metrics)),
        Arc::clone(&metrics),
    );
    fake.script(Op::Put, [Fault::ServiceUnavailable, Fault::SlowDown]);

    store
        .put(
            "attempts/put",
            Bytes::from_static(b"payload that outlives two throttles"),
            PutOptions::default(),
        )
        .await
        .expect("two retryable throttles must not fail the put");

    let snap = metrics.snapshot();
    assert_eq!(
        fake.count(Op::Put),
        3,
        "two faults + one success = three PUTs on the wire"
    );
    assert_eq!(
        fake.requests(Op::Put)
            .iter()
            .filter(|seen| seen.fault.is_some())
            .count(),
        2,
        "both scripted PUT faults must have fired"
    );
    assert_eq!(
        snap.put.attempts, 3,
        "billed PUT requests include the two retries"
    );
    assert_eq!(snap.put.calls, 1, "exactly one logical put completed");
    assert_eq!(
        snap.put.attempts - snap.put.calls,
        2,
        "attempts exceed calls by exactly the retry count"
    );
    assert_eq!(
        snap.get.attempts, 0,
        "a put's retries must not be charged to get"
    );
}
