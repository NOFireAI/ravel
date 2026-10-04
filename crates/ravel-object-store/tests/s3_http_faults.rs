//! HTTP-layer fault injection for [`S3Store`].
//!
//! `src/s3.rs`'s own unit tests pin error *classification* against synthetic
//! `object_store::Error` values: they prove `classify_generic` turns a "503
//! Service Unavailable" text into `Throttled`, but nothing there proves `S3Store` ever issues a
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use parking_lot::Mutex;
use ravel_object_store::s3::{
    MULTIPART_PART_SIZE, MULTIPART_THRESHOLD, S3Config, S3HttpConfig, S3Store, UploadIntegrity,
};
use ravel_object_store::scheduling::{ClassedStore, SchedulerConfig};
use ravel_object_store::{
    GetRange, InstrumentedStore, ObjectStoreBackend, Pin, PutOptions, StoreError, StoreMetrics,
};
use tokio::sync::Notify;

/// Bucket name the fake serves. Path-style requests put it in the first path
/// segment (`/{bucket}/{key}`), which is what `force_path_style: true` makes
/// `object_store` emit.
const BUCKET: &str = "ravel-fault-bucket";

/// Fixed `Last-Modified` for every response. `object_store`'s S3 header config
/// does not require it, but a real endpoint always sends one and parsing it is
/// part of the path under test.
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

/// Fixed `x-amz-version-id` on every GET and HEAD response, as a bucket with
/// versioning on reports one. It differs from every ETag the fake issues, so
/// `Pin::from_store` keeps it as a selector. The fake does not model versions
/// beyond the header: it serves the current object whatever `versionId` a
/// request names, and an overwrite keeps this same id, so a `versionId` is
/// observable in the request log and nowhere else.
const VERSION_ID: &str = "fake-version-1";

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
    /// `DeleteObjects` (`POST /?delete`), which is what `S3Store::delete`
    /// sends: `object_store` routes a single-key delete through its bulk
    /// path unless bulk delete is disabled, and Ravel never disables it.
    DeleteObjects,
    CreateMultipart,
    UploadPart,
    CompleteMultipart,
    AbortMultipart,
    /// `ListObjectsV2`, served as an empty listing: no test here asserts on
    /// listing contents, only on how the request was signed.
    List,
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
    /// `DeleteObjects` only: a 200 whose `DeleteResult` carries an `<Error>`
    /// with this code and message for every requested key. S3 reports a
    /// per-key refusal (a deny policy, a missing `s3:DeleteObject`) this way,
    /// not with an error status, so no retry layer sees it.
    DeleteKeyError {
        code: &'static str,
        message: &'static str,
    },
    /// A 200 whose body starts, then the connection dies before the declared
    /// `Content-Length` is delivered. The response headers already succeeded,
    /// so no retry layer covers this: it surfaces to the caller, and the
    /// contract requires it to surface as something retryable.
    DropMidResponse,
    /// A 409 with an `<Error><Code>ConditionalRequestConflict</Code>` body, the
    /// response S3 returns when two `If-None-Match: *` PUTs race the same key.
    /// `object_store` 0.14 maps a raw 409 to `AlreadyExists` and does not retry
    /// a create conflict, so the S3 adapter's HEAD disambiguation is what turns
    /// an absent-key 409 into a retryable `Transient` (#1302).
    ConditionalConflict,
    /// A whole-request 404 whose `<Error><Code>` is this code: `NoSuchBucket`
    /// from a bucket that does not exist or was deleted, `NoSuchKey` from a
    /// missing key. A HEAD response carries no body, so on a HEAD the code
    /// never reaches the client.
    NotFoundCode(&'static str),
    /// A 200 HEAD whose `Last-Modified` cannot be parsed. The status line says
    /// success, but `object_store` cannot turn the response into `ObjectMeta`,
    /// so the HEAD determined nothing about whether the key is present. That
    /// parse happens after `object_store`'s retry loop returns, so it is not
    /// retried internally: exactly one HEAD reaches the endpoint, and the
    /// adapter classifies it as a retryable `Transient`. This is the
    /// inconclusive-probe case the create-conflict disambiguation must surface
    /// as retryable rather than as a terminal `AlreadyExists` (#1302).
    InconclusiveHead,
    /// `get` only: a 200/206 whose body has one byte flipped, served with the
    /// `x-amz-checksum-crc64nvme` of the *stored* object. Models a bit flipped
    /// at rest or on the wire below a checksum S3 still reports honestly, which
    /// is exactly what ADR-1696's read-side verification must catch: the
    /// adapter recomputes the digest over what arrived and must refuse with
    /// `Corrupted` instead of handing the bytes to a decoder.
    CorruptGetBody,
    /// `get` only: the body is served correctly but no `x-amz-checksum-*`
    /// header comes back, as from an endpoint that stores no checksum or
    /// ignores `x-amz-checksum-mode`. ADR-1696 decision 3 serves it and counts
    /// it, so this is the fault that moves `ravel_store_get_unverified_total`.
    NoGetChecksum,
    /// Not a fault either: serve the request normally under this exact `Date`
    /// header, so a test can pin what the store's clock said (ADR-1685
    /// decision 1) instead of racing the host clock hyper would otherwise
    /// stamp. `"not-a-valid-date"` is how a response with no *usable* `Date`
    /// is scripted: hyper adds one of its own to any response that carries
    /// none, so an absent header cannot be produced from this side, and an
    /// unparseable one exercises the same branch of the connector.
    FixedDate(&'static str),
    /// `get` only, and not a fault on the request it answers: serve it
    /// normally, then replace the stored object with one of the same length
    /// whose last byte differs, so every later request sees a new ETag. This is
    /// the owner overwriting the key between a split read's first request and
    /// its continuations, placed deterministically.
    OverwriteAfterServing,
}

/// How a GET is served once faults have been resolved: the two ADR-1696 read
/// paths a test needs to script are variations of a *successful* response, not
/// error statuses, so they ride here rather than short-circuiting in `handle`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum GetBehavior {
    /// Correct body, with the stored object's CRC-64/NVME attached when the
    /// request is one a MinIO-style endpoint answers with a checksum.
    Normal,
    /// Correct checksum header, one byte of the body flipped.
    CorruptBody,
    /// Correct body, no checksum header at all.
    NoChecksum,
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
    /// The value of the `x-amz-checksum-{crc64nvme,sha256,...}` request header,
    /// if the client attached a server-verified upload checksum (#863). Absent
    /// under `UploadIntegrity::Off`. The `x-amz-checksum-algorithm` header is
    /// deliberately not what this captures: the value header is what carries the
    /// digest S3 verifies.
    checksum_header: Option<(String, String)>,
    /// The `x-amz-checksum-mode` request header's value, if the client asked S3
    /// to return the checksum it stored at upload (ADR-1696 decision 2).
    checksum_mode: Option<String>,
    /// The request carried an `x-amz-*` header missing from its SigV4
    /// `SignedHeaders`, and was refused 403 for it.
    unsigned_amz_header: bool,
    /// The `If-Match` request header's value, which is the precondition half of
    /// a [`Pin`] on the wire (ADR-2040 decision 1).
    if_match: Option<String>,
    /// The `versionId` query parameter, which is the selector half of a `Pin`
    /// on the wire. Ravel's own reads never send it; an external pinned read
    /// does when the grant recorded a version.
    version_id: Option<String>,
    /// The raw request body of a `DeleteObjects` request, which is where its
    /// keys and any `VersionId` go. `None` for every other operation.
    delete_body: Option<String>,
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
    /// Requests the handler is serving right now, and the most it has ever
    /// served at once.
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    /// Woken whenever `in_flight` changes.
    in_flight_changed: Notify,
    /// When set, a request of one of these ops waits before answering until
    /// this many requests have been in flight at once (now or earlier) or the
    /// grace period passes, so every request a client sends concurrently
    /// overlaps at the server.
    hold: Mutex<Option<(Vec<Op>, usize, Duration)>>,
}

/// One request's slot in [`FakeState::in_flight`], released on drop so a
/// handler future the client abandoned still leaves the count.
struct InFlight<'a>(&'a FakeState);

impl<'a> InFlight<'a> {
    fn enter(state: &'a FakeState) -> Self {
        let now = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        state.peak_in_flight.fetch_max(now, Ordering::SeqCst);
        state.in_flight_changed.notify_waiters();
        InFlight(state)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.0.in_flight_changed.notify_waiters();
    }
}

impl FakeState {
    /// Wait until `count` (`in_flight` or `peak_in_flight`) reads at least
    /// `target`.
    async fn wait_for(&self, count: &AtomicUsize, target: usize) {
        loop {
            let changed = self.in_flight_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if count.load(Ordering::SeqCst) >= target {
                return;
            }
            changed.await;
        }
    }

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

    /// Log one request as the server saw it, stamped with the time it arrived.
    fn record(
        &self,
        op: Op,
        key: &str,
        fault: Option<Fault>,
        headers: &HeaderMap,
        query: &HashMap<String, String>,
        body: &[u8],
    ) {
        self.log.lock().push(Seen {
            op,
            key: key.to_string(),
            at: Instant::now(),
            fault,
            range: requested_range(headers),
            checksum_header: checksum_header(headers),
            checksum_mode: headers
                .get("x-amz-checksum-mode")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            unsigned_amz_header: has_unsigned_amz_header(headers),
            if_match: headers
                .get(header::IF_MATCH)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            version_id: query.get("versionId").cloned(),
            delete_body: (op == Op::DeleteObjects)
                .then(|| String::from_utf8_lossy(body).into_owned()),
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
    /// the same field a RustFS deployment sets.
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

    /// [`Self::store_with_http`] reporting into `metrics`, for a split-read
    /// test that asserts on the metrics snapshot rather than on the store.
    fn store_with_http_and_metrics(
        &self,
        http: S3HttpConfig,
        metrics: Arc<StoreMetrics>,
    ) -> S3Store {
        S3Store::with_http_config_and_metrics(self.config(), http, metrics)
            .expect("a fake-endpoint S3Store must build with metrics")
    }

    /// The same store with a write-time upload-integrity mode configured
    /// (#863), so `put` attaches a server-verified `x-amz-checksum-*` header.
    fn store_with_upload_integrity(&self, mode: UploadIntegrity) -> S3Store {
        let http = S3HttpConfig {
            upload_integrity: mode,
            ..Default::default()
        };
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

    /// Hold every request of the `ops` until `target` requests have been in
    /// flight at once or `grace` passes, whichever comes first.
    fn hold(&self, ops: &[Op], target: usize, grace: Duration) {
        *self.state.hold.lock() = Some((ops.to_vec(), target, grace));
    }

    /// Wait until at least `target` requests are in flight.
    async fn wait_in_flight(&self, target: usize) {
        self.state.wait_for(&self.state.in_flight, target).await;
    }

    /// The most requests of any kind the endpoint has served at once.
    fn peak_in_flight(&self) -> usize {
        self.state.peak_in_flight.load(Ordering::SeqCst)
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
        Method::POST if query.contains_key("delete") => Some(Op::DeleteObjects),
        Method::POST if query.contains_key("uploads") => Some(Op::CreateMultipart),
        Method::POST if has_upload_id => Some(Op::CompleteMultipart),
        Method::PUT if has_upload_id => Some(Op::UploadPart),
        Method::DELETE if has_upload_id => Some(Op::AbortMultipart),
        Method::PUT => Some(Op::Put),
        Method::GET if query.contains_key("list-type") => Some(Op::List),
        Method::GET => Some(Op::Get),
        Method::HEAD => Some(Op::Head),
        Method::DELETE => Some(Op::Delete),
        _ => None,
    }
}

fn version_id_header() -> header::HeaderName {
    header::HeaderName::from_static("x-amz-version-id")
}

fn etag_of(data: &[u8]) -> String {
    format!("\"{:08x}\"", crc32c::crc32c(data))
}

/// CRC-64/NVME, the digest behind `x-amz-checksum-crc64nvme`, written here as
/// the plain bitwise loop: reflected polynomial `0x9a6c9329ac4bc9b5` (the bit
/// reverse of the catalogue's `0xad93d23594c93659`), all-ones init and final
/// xor.
///
/// Deliberately a second, independent implementation rather than a call into
/// the adapter's table-driven one. The fake endpoint is the other side of the
/// wire, and a test that computed the digest with the same code under test
/// would agree with it even if both were a different CRC than S3's. The
/// catalogue `check` vector is asserted below so this side is pinned too.
fn crc64_nvme(data: &[u8]) -> u64 {
    const POLY: u64 = 0x9a6c_9329_ac4b_c9b5;
    let mut crc = !0u64;
    for &byte in data {
        crc ^= u64::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
        }
    }
    !crc
}

/// The catalogue `check` value for CRC-64/NVME. Without this the fake could
/// serve a self-consistent wrong digest and every verification test would still
/// pass.
#[test]
fn the_fake_endpoints_crc64_matches_the_catalogue_check_vector() {
    assert_eq!(crc64_nvme(b"123456789"), 0xae8b_1486_0a79_9888);
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

/// The keys a `DeleteObjects` body names, in request order. Ravel's keys are
/// plain ASCII with no XML metacharacters, so no unescaping is needed.
fn delete_objects_keys(body: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(body);
    text.split("<Key>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</Key>").map(|(key, _)| key.to_string()))
        .collect()
}

/// The inner text of every `<Object>` element a `DeleteObjects` body carries,
/// in request order.
fn delete_objects_entries(body: &str) -> Vec<&str> {
    body.split("<Object>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</Object>").map(|(entry, _)| entry))
        .collect()
}

/// Whether `body` holds a `VersionId` element anywhere, inside or outside an
/// `Object`: an open tag, a self-closing one, or one carrying attributes.
fn has_version_id_element(body: &str) -> bool {
    body.split("<VersionId").skip(1).any(|rest| {
        rest.chars()
            .next()
            .is_some_and(|c| c == '>' || c == '/' || c.is_whitespace())
    })
}

/// A 200 `DeleteResult` carrying `entries` (`<Deleted>` and `<Error>`
/// elements), the shape S3 answers every `DeleteObjects` request with.
fn delete_result(entries: &str) -> Response {
    build(
        StatusCode::OK,
        vec![(header::CONTENT_TYPE, "application/xml".to_string())],
        Body::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             {entries}</DeleteResult>"
        )),
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

/// The upload-checksum value header the client attached, if any (#863):
/// `x-amz-checksum-crc64nvme` / `-sha256` / etc., returned as `(name, value)`.
/// Two `x-amz-checksum-*` headers are skipped because neither carries a digest:
/// `-algorithm` names an algorithm, and `-mode` is the read-side
/// `ENABLED` request flag the adapter sends on every request (ADR-1696
/// decision 2), which would otherwise read as an upload checksum here.
fn checksum_header(headers: &HeaderMap) -> Option<(String, String)> {
    headers.iter().find_map(|(name, value)| {
        let name = name.as_str();
        if name.starts_with("x-amz-checksum-")
            && name != "x-amz-checksum-algorithm"
            && name != "x-amz-checksum-mode"
        {
            Some((name.to_string(), value.to_str().ok()?.to_string()))
        } else {
            None
        }
    })
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
    let _in_flight = InFlight::enter(&state);
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
    // A DeleteObjects request names its keys in the body, not the path.
    let key = if op == Op::DeleteObjects {
        delete_objects_keys(&data).join(",")
    } else {
        key
    };

    let fault = state.take_fault(op);
    state.record(op, &key, fault, &headers, &query, &data);
    let hold = state.hold.lock().clone();
    if let Some((ops, target, grace)) = hold
        && ops.contains(&op)
    {
        let _ = tokio::time::timeout(grace, state.wait_for(&state.peak_in_flight, target)).await;
    }
    if has_unsigned_amz_header(&headers) {
        return error_response(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "There were headers present in the request which were not signed",
        );
    }

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
        Some(Fault::DeleteKeyError { code, message }) => {
            let errors: String = delete_objects_keys(&data)
                .iter()
                .map(|key| {
                    format!(
                        "<Error><Key>{key}</Key><Code>{code}</Code>\
                         <Message>{message}</Message></Error>"
                    )
                })
                .collect();
            delete_result(&errors)
        }
        Some(Fault::DropMidResponse) => drop_mid_response(&headers),
        Some(Fault::ConditionalConflict) => error_response(
            StatusCode::CONFLICT,
            "ConditionalRequestConflict",
            "The conditional request could not be satisfied.",
        ),
        Some(Fault::NotFoundCode(code)) => {
            error_response(StatusCode::NOT_FOUND, code, "The resource does not exist.")
        }
        Some(Fault::InconclusiveHead) => build(
            StatusCode::OK,
            vec![
                (header::ETAG, "\"inconclusive\"".to_string()),
                // An RFC2822 date object_store cannot parse: the 200 arrives
                // but header_meta fails, so the probe yields no verdict about
                // the key's presence.
                (header::LAST_MODIFIED, "not-a-valid-date".to_string()),
                (header::CONTENT_LENGTH, "7".to_string()),
            ],
            Body::empty(),
        ),
        Some(Fault::CorruptGetBody) => serve(
            &state,
            op,
            &key,
            &query,
            &headers,
            data,
            GetBehavior::CorruptBody,
        ),
        Some(Fault::NoGetChecksum) => serve(
            &state,
            op,
            &key,
            &query,
            &headers,
            data,
            GetBehavior::NoChecksum,
        ),
        Some(Fault::FixedDate(date)) => {
            let mut response = serve(
                &state,
                op,
                &key,
                &query,
                &headers,
                data,
                GetBehavior::Normal,
            );
            // hyper only stamps its own `Date` on a response that carries
            // none, so setting one here is what the client sees.
            response
                .headers_mut()
                .insert(header::DATE, HeaderValue::from_static(date));
            response
        }
        Some(Fault::OverwriteAfterServing) => {
            let response = serve(
                &state,
                op,
                &key,
                &query,
                &headers,
                data,
                GetBehavior::Normal,
            );
            if let Some(object) = state.objects.lock().get_mut(&key) {
                let mut replaced = object.to_vec();
                let last = replaced
                    .last_mut()
                    .expect("an overwrite fault needs a non-empty object to replace");
                *last ^= 0x01;
                *object = Bytes::from(replaced);
            }
            response
        }
        Some(Fault::Pass) | None => serve(
            &state,
            op,
            &key,
            &query,
            &headers,
            data,
            GetBehavior::Normal,
        ),
    }
}

fn serve(
    state: &FakeState,
    op: Op,
    key: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    data: Bytes,
    get_behavior: GetBehavior,
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
            // `If-Match` is evaluated by the endpoint, before any body is sent,
            // so a pin the adapter attached to the wrong request or to none at
            // all is visible here and not only in the request log.
            if let Some(expected) = headers.get(header::IF_MATCH)
                && expected.as_bytes() != etag.as_bytes()
            {
                return error_response(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                );
            }
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
                (version_id_header(), VERSION_ID.to_string()),
            ];
            // The stored whole-object checksum, returned the way MinIO (and the
            // MinIO-derived RustFS) returns it: only when the request asked with
            // `x-amz-checksum-mode: ENABLED` and carried no `Range` header. A
            // ranged GET gets no checksum at all, whatever it covers. Computed
            // over `object`, never over `body`, so a corrupted body is served
            // under an honest checksum.
            let checksum_mode_enabled = headers
                .get("x-amz-checksum-mode")
                .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"ENABLED"));
            let unranged = headers.get(header::RANGE).is_none();
            if get_behavior != GetBehavior::NoChecksum && checksum_mode_enabled && unranged {
                response_headers.push((
                    header::HeaderName::from_static("x-amz-checksum-crc64nvme"),
                    STANDARD.encode(crc64_nvme(&object).to_be_bytes()),
                ));
            }
            let body = match get_behavior {
                GetBehavior::CorruptBody => {
                    let mut flipped = body.to_vec();
                    let first = flipped
                        .first_mut()
                        .expect("a corrupt-body fault needs a non-empty body to corrupt");
                    *first ^= 0x01;
                    Bytes::from(flipped)
                }
                GetBehavior::Normal | GetBehavior::NoChecksum => body,
            };
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
                    (version_id_header(), VERSION_ID.to_string()),
                ],
                Body::empty(),
            )
        }
        Op::Delete => {
            state.objects.lock().remove(key);
            build(StatusCode::NO_CONTENT, vec![], Body::empty())
        }
        Op::DeleteObjects => {
            let mut deleted = String::new();
            let mut objects = state.objects.lock();
            for key in delete_objects_keys(&data) {
                objects.remove(&key);
                deleted.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
            }
            delete_result(&deleted)
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
        Op::List => build(
            StatusCode::OK,
            vec![(header::CONTENT_TYPE, "application/xml".to_string())],
            Body::from(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Name>{BUCKET}</Name><Prefix></Prefix><KeyCount>0</KeyCount>\
                 <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
                 </ListBucketResult>"
            )),
        ),
    }
}

/// Whether the request carries an `x-amz-*` header its SigV4 `Authorization`
/// does not list in `SignedHeaders`. S3 and RustFS refuse such a request with
/// 403 `AccessDenied` ("There were headers present in the request which were
/// not signed"), and so does this fake.
fn has_unsigned_amz_header(headers: &HeaderMap) -> bool {
    let signed: Vec<&str> = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|auth| auth.split("SignedHeaders=").nth(1))
        .and_then(|rest| rest.split(',').next())
        .map(|list| list.split(';').collect())
        .unwrap_or_default();
    headers.keys().any(|name| {
        let name = name.as_str();
        name.starts_with("x-amz-") && !signed.contains(&name)
    })
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

/// #863, on the wire: a store built with `UploadIntegrity::Off` attaches no
/// checksum header (today's behavior), and a store built with a non-`Off` mode
/// attaches the corresponding `x-amz-checksum-*` value header so S3 verifies-
/// or-rejects the write. The fake endpoint records the request headers it saw,
/// so this proves the attach reaches the wire and is not merely reflected in
/// `capabilities()`. It also exercises the SigV4 path: `object_store` signs the
/// checksum header, and a signing break would surface as a client error before
/// any request reached the fake (which does not itself verify signatures).
///
/// Reverting the `with_checksum_algorithm` wiring in `S3Store::builder` makes
/// the `Crc64Nvme`/`Sha256` cases send no header, failing this test.
#[tokio::test]
async fn put_attaches_server_verified_checksum_only_when_configured() {
    let fake = FakeS3::start().await;

    // Off: no checksum header on the wire.
    fake.store()
        .put(
            "checksum/off",
            Bytes::from_static(b"no integrity configured"),
            PutOptions::default(),
        )
        .await
        .expect("put must succeed with integrity off");
    let off = fake.requests(Op::Put);
    assert_eq!(off.len(), 1, "one PUT expected");
    assert!(
        off[0].checksum_header.is_none(),
        "UploadIntegrity::Off must attach no x-amz-checksum-* header, got {:?}",
        off[0].checksum_header
    );

    for (mode, expected) in [
        (UploadIntegrity::Crc64Nvme, "x-amz-checksum-crc64nvme"),
        (UploadIntegrity::Sha256, "x-amz-checksum-sha256"),
    ] {
        let fake = FakeS3::start().await;
        let store = fake.store_with_upload_integrity(mode);
        store
            .put(
                "checksum/on",
                Bytes::from_static(b"integrity configured payload"),
                PutOptions::default(),
            )
            .await
            .unwrap_or_else(|e| panic!("put with {mode:?} must succeed: {e:?}"));
        let puts = fake.requests(Op::Put);
        assert_eq!(puts.len(), 1, "one PUT expected for {mode:?}");
        let (name, value) = puts[0]
            .checksum_header
            .as_ref()
            .unwrap_or_else(|| panic!("{mode:?} must attach {expected}, saw no checksum header"));
        assert_eq!(name, expected, "wrong checksum header for {mode:?}");
        assert!(
            !value.is_empty(),
            "the checksum header must carry a base64 digest value"
        );
    }
}

/// ADR-1696 decision 2, on the wire: a full-object GET whose body comes back
/// with one byte flipped, under the `x-amz-checksum-crc64nvme` of the object as
/// stored, must fail with `Corrupted` rather than hand the bytes to the caller.
/// This is the whole mechanism end to end --- the signed request header, the
/// connector reading the response header below `object_store`'s retry loop, and
/// the digest recomputed over the body received --- observed from the far side
/// of a socket.
///
/// The three assertions are separable on purpose. The error pins the outcome;
/// the request-header assertion pins that the adapter actually *asked* for the
/// stored checksum (an endpoint only returns one under checksum mode, so
/// dropping the header would make this test pass for the wrong reason against
/// this fake and fail against real S3); and the unverified counter staying at 0
/// pins that this was a genuine mismatch, not a read that fell through to the
/// serve-and-count path.
///
/// The fake returns the checksum header only on an unranged GET, as MinIO and
/// RustFS do, so this also pins that the first request of a full-object read
/// is unranged. Against an adapter whose first request was
/// `Range: bytes=0-(chunk-1)`, the response carries no checksum, the read is
/// served, and the `expect_err` below fails with the flipped bytes in its
/// message. Removing the `stored.verify(...)` call in
/// `S3Store::verify_full_read` fails the same line.
#[tokio::test]
async fn a_flipped_byte_in_a_get_body_is_corrupted() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("verify/flipped", b"a commit record's worth of bytes");
    fake.always(Op::Get, Fault::CorruptGetBody);

    let error = store
        .get("verify/flipped", GetRange::Full)
        .await
        .expect_err("a body that fails its stored checksum must not be served");
    assert!(
        matches!(error, StoreError::Corrupted(_)),
        "a checksum mismatch must be Corrupted, got {error:?}"
    );

    let gets = fake.requests(Op::Get);
    assert_eq!(gets.len(), 1, "a Corrupted read must not be retried");
    assert_eq!(gets[0].range, None, "the verified request is unranged");
    assert_eq!(
        gets[0].checksum_mode.as_deref(),
        Some("ENABLED"),
        "the adapter must ask S3 for the checksum it stored at upload"
    );
    assert_eq!(
        store.get_unverified(),
        0,
        "a read that was verified and failed is not an unverified read"
    );
}

/// ADR-1696 decision 3: an endpoint that returns no `x-amz-checksum-*` header
/// (it stores no checksum, or ignored checksum mode) is served, not refused,
/// and the read is counted on `ravel_store_get_unverified_total`. Failing
/// closed here would make an upgrade an outage for every object written before
/// upload integrity was on, which is every object in every existing bucket.
///
/// The counter assertion is `by exactly 1`, measured across the call, because
/// "greater than zero" would not distinguish one logical read from one per HTTP
/// request --- and the adapter splits a large whole-object read into several.
/// The verified read at the end is the control: it proves the counter moves for
/// the missing header rather than for every get.
#[tokio::test]
async fn a_get_with_no_checksum_header_is_served_and_counted_unverified() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    let payload = b"served without a stored checksum";
    fake.seed("verify/unchecksummed", payload);
    fake.script(Op::Get, [Fault::NoGetChecksum]);

    let before = store.get_unverified();
    let got = store
        .get("verify/unchecksummed", GetRange::Full)
        .await
        .expect("a response with no checksum header must still be served");
    assert_eq!(
        &got.data[..],
        &payload[..],
        "the bytes must be served unchanged"
    );
    assert_eq!(
        store.get_unverified(),
        before + 1,
        "one full-object read with no stored checksum is exactly one unverified read"
    );

    // Control: the scripted fault is spent, so this read comes back with the
    // checksum header and must not move the counter.
    store
        .get("verify/unchecksummed", GetRange::Full)
        .await
        .expect("the verified read must succeed");
    assert_eq!(
        store.get_unverified(),
        before + 1,
        "a read the adapter verified must not count as unverified"
    );

    // Decision 4, on the wire: a ranged read the caller asked for is outside
    // the check by construction, so it is neither verified nor counted. An
    // endpoint returns no checksum on a ranged response, and a slice could not
    // be compared against the whole-object one anyway, so counting one here
    // would make every suffix read of every segment look like a gap in
    // coverage.
    let ranged = store
        .get("verify/unchecksummed", GetRange::Range(0, 6))
        .await
        .expect("a ranged read must succeed");
    assert_eq!(&ranged.data[..], b"served", "the ranged bytes come back");
    assert_eq!(
        store.get_unverified(),
        before + 1,
        "a caller-issued ranged read is outside the check, not an unverified read"
    );
}

/// A commit-record-sized full-object read is exactly one store request: one
/// unranged GET, no HEAD before it, verified against the checksum it carried.
/// The per-query request budget counts requests, so a HEAD to learn the size
/// first would double the cost of every record read.
#[tokio::test]
async fn a_commit_record_sized_full_read_is_one_unranged_verified_request() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    let record = patterned(200);
    fake.seed("verify/record", &record);

    let outcome = store
        .get("verify/record", GetRange::Full)
        .await
        .expect("a small full-object read must be served");
    assert_eq!(&outcome.data[..], &record[..]);

    let gets = fake.requests(Op::Get);
    assert_eq!(gets.len(), 1, "exactly one GET, saw {gets:?}");
    assert_eq!(gets[0].range, None, "the GET is unranged");
    assert_eq!(fake.count(Op::Head), 0, "no HEAD precedes the read");
    assert_eq!(store.get_unverified(), 0, "the read was verified");
}

/// `x-amz-checksum-mode` is never sent unsigned. `object_store` signs
/// `ClientOptions`' default headers onto PUT, GET and HEAD itself, but
/// not onto LIST, and a reqwest client built from the same options adds them
/// after signing; S3 and RustFS refuse any request with an unsigned `x-amz-*`
/// header, so that would fail every LIST with a 403. The fake refuses the same
/// way. Without the default headers stripped in the S3 connector, the `list`
/// below fails with that 403.
#[tokio::test]
async fn no_request_carries_an_unsigned_checksum_mode_header() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    store
        .put(
            "signed/k",
            Bytes::from_static(b"v"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put");
    store.head("signed/k").await.expect("head");
    store.get("signed/k", GetRange::Full).await.expect("get");
    let page = store
        .list("signed/", None)
        .await
        .expect("a LIST must not carry an unsigned header");
    assert!(page.objects.is_empty(), "the fake serves an empty listing");

    let log = fake.state.log.lock().clone();
    for op in [Op::Put, Op::Head, Op::Get, Op::List] {
        assert_eq!(
            log.iter().filter(|seen| seen.op == op).count(),
            1,
            "exactly one {op:?} request, saw {log:?}"
        );
    }
    for seen in &log {
        assert!(
            !seen.unsigned_amz_header,
            "a request carried an unsigned x-amz-* header: {seen:?}"
        );
    }
    let get = log.iter().find(|seen| seen.op == Op::Get).expect("the GET");
    assert_eq!(
        get.checksum_mode.as_deref(),
        Some("ENABLED"),
        "the GET still asks for the stored checksum, signed"
    );
}

/// `S3HttpConfig::request_stored_checksum = false` sends no
/// `x-amz-checksum-mode` header on any request. An endpoint then returns no
/// stored checksum, so a full-object read is served and counted unverified,
/// exactly once, where the default configuration verifies the same object.
#[tokio::test]
async fn disabling_checksum_mode_sends_no_header_and_counts_reads_unverified() {
    let fake = FakeS3::start().await;
    let payload = b"read without asking for the stored checksum";
    fake.seed("verify/mode-off", payload);

    let store = fake.store_with_http(S3HttpConfig {
        request_stored_checksum: false,
        ..Default::default()
    });
    store
        .put(
            "verify/mode-off-put",
            Bytes::from_static(b"x"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("a put under the disabled mode must succeed");
    let outcome = store
        .get("verify/mode-off", GetRange::Full)
        .await
        .expect("an unverified read is served, not refused");
    assert_eq!(&outcome.data[..], payload);
    assert_eq!(
        store.get_unverified(),
        1,
        "the one full-object read is one unverified read"
    );
    for op in [Op::Put, Op::Get] {
        for seen in fake.requests(op) {
            assert_eq!(
                seen.checksum_mode, None,
                "no request may carry x-amz-checksum-mode when disabled, saw {seen:?}"
            );
        }
    }

    // The default sends it, and the same object is verified.
    let default_store = fake.store();
    default_store
        .get("verify/mode-off", GetRange::Full)
        .await
        .expect("the default read is served");
    assert_eq!(default_store.get_unverified(), 0);
    assert_eq!(
        fake.requests(Op::Get)
            .last()
            .and_then(|seen| seen.checksum_mode.clone())
            .as_deref(),
        Some("ENABLED")
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

/// `S3Store::delete` is one `DeleteObjects` POST naming only the key: never a
/// path `DELETE`, and never a `versionId`, which is why a versioned bucket
/// answers it with a delete marker rather than removing a version.
#[tokio::test]
async fn a_delete_is_one_delete_objects_request_with_no_version_id() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/deleted", b"present");

    store
        .delete("fault/deleted")
        .await
        .expect("a delete the endpoint accepts must succeed");

    let bulk = fake.requests(Op::DeleteObjects);
    assert_eq!(
        bulk.len(),
        1,
        "one delete must be one DeleteObjects request"
    );
    let body = bulk[0]
        .delete_body
        .as_deref()
        .expect("a DeleteObjects request records its body");
    assert_eq!(
        delete_objects_entries(body),
        ["<Key>fault/deleted</Key>"],
        "the body must carry exactly one Object element naming only the key: {body}"
    );
    for planted in [
        "<Delete><VersionId/><Object><Key>k</Key></Object></Delete>",
        "<Delete><Object><Key>k</Key></Object><VersionId id=\"v\">v1</VersionId></Delete>",
        "<Delete><Object><Key>k</Key></Object><VersionId\n>v1</VersionId></Delete>",
    ] {
        assert!(
            has_version_id_element(planted),
            "the VersionId check must see a VersionId element outside Object: {planted}"
        );
    }
    assert!(
        !has_version_id_element("<Delete><Object><Key>k</Key></Object></Delete>"),
        "the VersionId check must pass a body with no VersionId element"
    );
    assert!(
        !has_version_id_element(body),
        "a delete must carry no VersionId element, open, self-closing or \
         attributed, inside or outside Object: {body}"
    );
    assert_eq!(
        bulk[0].version_id, None,
        "a delete must carry no versionId query parameter"
    );
    assert_eq!(fake.count(Op::Delete), 0, "no path DELETE may be sent");
    assert_eq!(fake.object("fault/deleted"), None);
}

/// S3 refuses one key of a `DeleteObjects` request inside a 200 response, and
/// `object_store` surfaces that as an untyped `Generic` error. A per-key
/// `AccessDenied` (a deny policy, a missing `s3:DeleteObject`) must still reach
/// the caller as `AccessDenied`, the class the sweep tolerates per chain,
/// rather than as a retryable error that fails the whole pass.
#[tokio::test]
async fn a_per_key_access_denied_in_delete_objects_is_access_denied() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/denied", b"kept");
    fake.always(
        Op::DeleteObjects,
        Fault::DeleteKeyError {
            code: "AccessDenied",
            message: "Access Denied",
        },
    );

    let error = store
        .delete("fault/denied")
        .await
        .expect_err("a per-key AccessDenied must fail the delete");
    assert!(
        matches!(error, StoreError::AccessDenied(_)),
        "a per-key AccessDenied must map to AccessDenied, got {error:?}"
    );
    assert!(!error.is_retryable(), "{error:?} must not be retryable");
    assert_eq!(fake.count(Op::DeleteObjects), 1, "a refusal is not retried");
    assert!(fake.object("fault/denied").is_some());
}

/// Every per-key code with a mapping, by the HTTP status S3 documents for it,
/// classified the way the single-request path classifies that status: 403 is
/// `AccessDenied`, 404 is the idempotent missing-key success, 412 is
/// `PreconditionFailed`, 503 is `Throttled`. A code with no mapping (here the
/// 400 `InvalidArgument`) keeps the `Generic` classification, `Transient`. A
/// per-key `SlowDown` never reaches the mapping: `object_store` retries the
/// request, so one scripted `SlowDown` costs a second request and succeeds.
#[tokio::test]
async fn per_key_delete_objects_codes_map_by_their_http_status() {
    let fake = FakeS3::start().await;
    let store = fake.store();

    fake.script(
        Op::DeleteObjects,
        [Fault::DeleteKeyError {
            code: "SlowDown",
            message: "Please reduce your request rate.",
        }],
    );
    store
        .delete("fault/codes")
        .await
        .expect("object_store retries a per-key SlowDown");
    assert_eq!(fake.count(Op::DeleteObjects), 2, "SlowDown then success");

    let denied = [
        "AccessDenied",
        "AllAccessDisabled",
        "AccountProblem",
        "InvalidAccessKeyId",
        "InvalidObjectState",
        "SignatureDoesNotMatch",
    ];
    for code in denied {
        fake.script(
            Op::DeleteObjects,
            [Fault::DeleteKeyError {
                code,
                message: "refused",
            }],
        );
        let error = store.delete("fault/codes").await.expect_err(code);
        assert!(
            matches!(error, StoreError::AccessDenied(_)),
            "{code} must map to AccessDenied, got {error:?}"
        );
    }

    fake.script(
        Op::DeleteObjects,
        [Fault::DeleteKeyError {
            code: "NoSuchKey",
            message: "The specified key does not exist.",
        }],
    );
    store
        .delete("fault/codes")
        .await
        .expect("a per-key NoSuchKey is an idempotent delete");

    let cases = [
        ("PreconditionFailed", "precondition"),
        ("ServiceUnavailable", "throttled"),
        ("InvalidArgument", "transient"),
    ];
    for (code, want) in cases {
        fake.script(
            Op::DeleteObjects,
            [Fault::DeleteKeyError {
                code,
                message: "refused",
            }],
        );
        let error = store.delete("fault/codes").await.expect_err(code);
        let got = match error {
            StoreError::PreconditionFailed => "precondition",
            StoreError::Throttled { .. } => "throttled",
            StoreError::Transient(_) => "transient",
            ref other => panic!("{code} mapped to {other:?}"),
        };
        assert_eq!(got, want, "{code} classified as {error:?}");
    }
}

/// A `DeleteObjects` against a bucket that does not exist is refused as a
/// whole: S3 answers 404 `NoSuchBucket` before it looks at any key. That must
/// fail the delete with a non-retryable `Permanent`, never read as the
/// idempotent missing-key success, or a sweep counts every key of a deleted
/// bucket as deleted.
#[tokio::test]
async fn a_delete_answered_no_such_bucket_fails_permanent() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/no-bucket", b"kept");
    fake.always(Op::DeleteObjects, Fault::NotFoundCode("NoSuchBucket"));

    let error = store
        .delete("fault/no-bucket")
        .await
        .expect_err("a delete answered NoSuchBucket must fail");
    assert!(
        matches!(error, StoreError::Permanent(_)),
        "NoSuchBucket on DeleteObjects must map to Permanent, got {error:?}"
    );
    assert!(!error.is_retryable(), "{error:?} must not be retryable");
    assert_eq!(fake.count(Op::DeleteObjects), 1, "a 404 is not retried");
    assert!(fake.object("fault/no-bucket").is_some());
}

/// The same refusal reported per key inside a 200 `DeleteResult`, which an
/// S3-compatible endpoint could send in place of the whole-request 404. A
/// missing bucket must not read as a deleted key on that path either.
#[tokio::test]
async fn a_per_key_no_such_bucket_in_delete_objects_fails_permanent() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.always(
        Op::DeleteObjects,
        Fault::DeleteKeyError {
            code: "NoSuchBucket",
            message: "The specified bucket does not exist",
        },
    );

    let error = store
        .delete("fault/no-bucket-per-key")
        .await
        .expect_err("a per-key NoSuchBucket must fail the delete");
    assert!(
        matches!(error, StoreError::Permanent(_)),
        "a per-key NoSuchBucket must map to Permanent, got {error:?}"
    );
}

/// A whole-request 404 `NoSuchKey` on a delete is still the idempotent
/// missing-key success, and on a get still `NotFound`: only the bucket code
/// changes class.
#[tokio::test]
async fn no_such_key_still_reads_as_not_found_on_delete_and_get() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/no-key", b"served only if the fault is skipped");
    fake.script(Op::DeleteObjects, [Fault::NotFoundCode("NoSuchKey")]);
    fake.script(Op::Get, [Fault::NotFoundCode("NoSuchKey")]);

    store
        .delete("fault/no-key")
        .await
        .expect("a delete answered NoSuchKey is an idempotent success");
    let error = store
        .get("fault/no-key", GetRange::Full)
        .await
        .expect_err("a get answered NoSuchKey must fail");
    assert!(
        matches!(error, StoreError::NotFound),
        "NoSuchKey on GET must map to NotFound, got {error:?}"
    );
    assert_eq!(fake.count(Op::DeleteObjects), 1);
    assert_eq!(fake.count(Op::Get), 1);
}

/// A get answered `NoSuchBucket` is not a missing object: the 404 body carries
/// the code, and the read fails `Permanent` instead of `NotFound`.
#[tokio::test]
async fn a_get_answered_no_such_bucket_is_not_a_missing_object() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/get-no-bucket", b"never served");
    fake.always(Op::Get, Fault::NotFoundCode("NoSuchBucket"));

    let error = store
        .get("fault/get-no-bucket", GetRange::Full)
        .await
        .expect_err("a get answered NoSuchBucket must fail");
    assert!(
        matches!(error, StoreError::Permanent(_)),
        "NoSuchBucket on GET must map to Permanent, got {error:?}"
    );
    assert_eq!(fake.count(Op::Get), 1, "a 404 is not retried");
}

/// A HEAD response has no body, so the `NoSuchBucket` code the endpoint sent
/// never reaches the client, and `head` and `pin_of` cannot tell a missing
/// bucket from a missing key without a second request. This pins that limit:
/// the fault was served, one HEAD went out, and the answer is `NotFound`.
#[tokio::test]
async fn a_head_answered_no_such_bucket_reads_as_not_found_for_lack_of_a_body() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/head-no-bucket", b"never served");
    fake.always(Op::Head, Fault::NotFoundCode("NoSuchBucket"));

    let head = store
        .head("fault/head-no-bucket")
        .await
        .expect_err("a head answered 404 must fail");
    assert!(
        matches!(head, StoreError::NotFound),
        "a bodiless 404 carries no code to classify, got {head:?}"
    );
    let pin = store
        .pin_of("fault/head-no-bucket")
        .await
        .expect_err("a pin_of answered 404 must fail");
    assert!(
        matches!(pin, StoreError::NotFound),
        "a bodiless 404 carries no code to classify, got {pin:?}"
    );
    let heads = fake.requests(Op::Head);
    assert_eq!(heads.len(), 2, "one HEAD per call, no follow-up probe");
    assert!(
        heads
            .iter()
            .all(|seen| seen.fault == Some(Fault::NotFoundCode("NoSuchBucket"))),
        "every HEAD must have been answered NoSuchBucket"
    );
}

/// Every other operation that reaches the bucket fails `Permanent` on
/// `NoSuchBucket`: `put` and multipart creation, part upload and completion
/// (where `object_store` reports the 404 as `NotFound`), and `list` and
/// `list_delimited` (where it reports the 404 as a `Generic` error that would
/// otherwise classify as a retryable `Transient`).
#[tokio::test]
async fn no_such_bucket_is_permanent_on_put_list_and_multipart() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    let permanent = |what: &str, error: StoreError| {
        assert!(
            matches!(error, StoreError::Permanent(_)),
            "NoSuchBucket on {what} must map to Permanent, got {error:?}"
        );
    };

    fake.script(Op::Put, [Fault::NotFoundCode("NoSuchBucket")]);
    let put = store
        .put(
            "fault/put-no-bucket",
            Bytes::from_static(b"payload"),
            PutOptions::default(),
        )
        .await
        .expect_err("put");
    permanent("PUT", put);

    fake.script(Op::List, [Fault::NotFoundCode("NoSuchBucket")]);
    permanent("LIST", store.list("fault/", None).await.expect_err("list"));
    fake.script(Op::List, [Fault::NotFoundCode("NoSuchBucket")]);
    permanent(
        "delimited LIST",
        store
            .list_delimited("fault/")
            .await
            .expect_err("list_delimited"),
    );

    fake.script(Op::CreateMultipart, [Fault::NotFoundCode("NoSuchBucket")]);
    let create = match store.put_multipart("fault/mp-no-bucket").await {
        Ok(_) => panic!("CreateMultipartUpload answered NoSuchBucket must fail"),
        Err(error) => error,
    };
    permanent("CreateMultipartUpload", create);

    fake.script(Op::UploadPart, [Fault::NotFoundCode("NoSuchBucket")]);
    let mut upload = store
        .put_multipart("fault/mp-no-bucket")
        .await
        .expect("CreateMultipartUpload must succeed");
    let part = upload
        .put_part(Bytes::from_static(b"one small part"), None)
        .await
        .expect_err("put_part");
    permanent("UploadPart", part);
    let _ = upload.abort().await;

    fake.script(Op::CompleteMultipart, [Fault::NotFoundCode("NoSuchBucket")]);
    let mut upload = store
        .put_multipart("fault/mp-no-bucket")
        .await
        .expect("CreateMultipartUpload must succeed");
    upload
        .put_part(Bytes::from_static(b"one small part"), None)
        .await
        .expect("the only part must upload");
    let complete = upload.complete().await.expect_err("complete");
    permanent("CompleteMultipartUpload", complete);

    assert_eq!(fake.count(Op::Put), 1, "a 404 is not retried");
    assert_eq!(fake.count(Op::List), 2, "a 404 is not retried");
    assert_eq!(fake.count(Op::CompleteMultipart), 1, "a 404 is not retried");
    assert_eq!(fake.object("fault/put-no-bucket"), None);
    assert_eq!(fake.object("fault/mp-no-bucket"), None);
}

/// An endpoint that throttles forever eventually gives up, and the error that
/// surfaces is retryable (so the caller's own backoff loop can take over)
/// rather than `Permanent`. The attempt count proves the client exhausted a
/// retry budget instead of failing on the first response.
///
/// **Retry classification per docs/object-store-contract.md.** The contract
/// classifies a throttle as `Throttled { retry_after_ms }`, and `s3.rs`'s
/// tier-2 heuristic has a branch for exactly that ("too many requests",
/// "service unavailable", "slow down", "throttl"). Over real HTTP the error
/// text `classify_generic` sees is
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
// Conditional-write 409 disambiguation (#1302)
// ---------------------------------------------------------------------------

/// The caller-side retry loop the contract names ("Ravel retries a transient
/// conflict"): re-issue a `CreateIfAbsent` PUT while the store returns a
/// retryable error, up to `max_attempts` total tries. Returns the final
/// outcome and the number of PUT calls the loop made. `StoreError::is_retryable`
/// is exactly what routes a `Transient` conditional-request conflict back here,
/// so this stands in for the ingest flush loop and the commit publish path
/// without pulling in either crate.
async fn create_with_retry(
    store: &S3Store,
    key: &str,
    bytes: &'static [u8],
    max_attempts: u32,
) -> (Result<(), StoreError>, u32) {
    let opts = PutOptions::create_if_absent();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match store
            .put(key, Bytes::from_static(bytes), opts.clone())
            .await
        {
            Ok(_) => return (Ok(()), attempts),
            Err(e) if e.is_retryable() && attempts < max_attempts => continue,
            Err(e) => return (Err(e), attempts),
        }
    }
}

/// A single 409 `ConditionalRequestConflict` on a `CreateIfAbsent` PUT to an
/// absent key is a transient conflict the AWS PutObject spec says to retry, not
/// a permanent `AlreadyExists`. The adapter's HEAD disambiguation sees the key
/// absent and returns a retryable `Transient`, so the caller loop retries and
/// the second PUT lands the object. The proof is on the server's view: exactly
/// two PUTs (the 409 and the retry) and exactly one HEAD.
///
/// Before the fix, `object_store`'s raw-409-to-`AlreadyExists` mapping surfaced
/// straight through `map_put_error` as `AlreadyExists`, which
/// `create_with_retry` would not retry: the loop would stop at one PUT and the
/// object would never be created.
#[tokio::test]
async fn single_409_on_create_if_absent_is_retried_and_the_put_returns_ok() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.script(Op::Put, [Fault::ConditionalConflict]);

    let (result, attempts) =
        create_with_retry(&store, "conflict/absent", b"created after one conflict", 5).await;
    result.expect("an absent-key 409 must be retried to success");
    assert_eq!(
        attempts, 2,
        "one conflict then one success is exactly two caller attempts"
    );
    assert_eq!(
        fake.count(Op::Put),
        2,
        "exactly two PUTs reached the endpoint: the 409 and the retry"
    );
    assert_eq!(
        fake.count(Op::Head),
        1,
        "exactly one HEAD disambiguated the single 409"
    );
    assert_eq!(
        fake.object("conflict/absent").as_deref(),
        Some(&b"created after one conflict"[..]),
        "the retried PUT landed the caller's exact bytes"
    );
}

/// A 409 on a key that really exists stays `AlreadyExists`: the HEAD finds the
/// key present, so the adapter must not downgrade a genuine collision to a
/// retryable `Transient`. This is the test that keeps the commit-path
/// split-brain guard and the compaction vanished-part guard alive: both rely on
/// a real already-exists surfacing as `AlreadyExists`. Exactly one HEAD, no
/// second PUT, and the stored bytes are the winner's (the split-brain-detection
/// input a caller would GET next is intact).
///
/// Before the fix this passed too (the mapper returned `AlreadyExists`
/// directly); mapping `AlreadyExists` to `Transient` unconditionally — the
/// tempting shortcut the fix deliberately avoids — makes this fail, because the
/// present-key collision would then be retried forever instead of surfacing.
#[tokio::test]
async fn a_409_on_a_key_that_really_exists_stays_already_exists() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("conflict/present", b"the winner's bytes");
    fake.always(Op::Put, Fault::ConditionalConflict);

    let (result, attempts) =
        create_with_retry(&store, "conflict/present", b"the loser's bytes", 5).await;
    let err = result.expect_err("a present-key 409 must surface, not retry to success");
    assert!(
        matches!(err, StoreError::AlreadyExists),
        "a genuine already-exists must stay AlreadyExists, got {err:?}"
    );
    assert!(
        !err.is_retryable(),
        "AlreadyExists is a protocol signal, not a retryable error: {err:?}"
    );
    assert_eq!(
        attempts, 1,
        "AlreadyExists stops the caller loop after exactly one attempt"
    );
    assert_eq!(
        fake.count(Op::Put),
        1,
        "no second PUT: a real collision is never retried"
    );
    assert_eq!(
        fake.count(Op::Head),
        1,
        "exactly one HEAD confirmed the key present"
    );
    assert_eq!(
        fake.object("conflict/present").as_deref(),
        Some(&b"the winner's bytes"[..]),
        "the winner's bytes are untouched, so a subsequent GET sees the collision"
    );
}

/// An endpoint stuck returning 409 for an absent key exhausts the caller's
/// retry budget and surfaces the last conflict as a retryable `Transient`, so
/// the caller's own backoff can take over rather than a permanent
/// `AlreadyExists` stranding the write. The attempt count is pinned exactly:
/// one PUT and one disambiguating HEAD per attempt, and no object ever visible.
#[tokio::test]
async fn persistent_409_surfaces_transient_after_the_retry_budget() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.always(Op::Put, Fault::ConditionalConflict);

    const MAX_ATTEMPTS: u32 = 4;
    let (result, attempts) =
        create_with_retry(&store, "conflict/persistent", b"never lands", MAX_ATTEMPTS).await;
    let err = result.expect_err("a persistent 409 must exhaust the budget and fail");
    assert!(
        matches!(err, StoreError::Transient(_)),
        "each absent-key 409 is a retryable Transient, got {err:?}"
    );
    assert!(
        err.is_retryable(),
        "the surfaced error must be retryable: {err:?}"
    );
    assert_eq!(
        attempts, MAX_ATTEMPTS,
        "the loop ran exactly its budget of attempts"
    );
    assert_eq!(
        fake.count(Op::Put),
        MAX_ATTEMPTS as usize,
        "exactly one PUT per attempt reached the endpoint"
    );
    assert_eq!(
        fake.count(Op::Head),
        MAX_ATTEMPTS as usize,
        "exactly one HEAD disambiguated each 409"
    );
    assert!(
        fake.object("conflict/persistent").is_none(),
        "no object ever became visible under a persistent conflict"
    );
}

/// A 409 whose disambiguating HEAD comes back inconclusive (a 200 the client
/// cannot parse) must surface as a retryable `Transient`, not a terminal
/// `AlreadyExists`: the probe determined nothing, so the key's state is
/// unknown, and the PUT already failed leaving nothing written. The caller
/// loop therefore retries rather than abandoning the write. Counts are pinned
/// exactly: the inconclusive HEAD is not retried inside `object_store` (the
/// header parse fails after its retry loop returns), so each attempt is
/// exactly one PUT and one HEAD.
///
/// This is the discriminating test for the fix. With the pre-fix catch-all
/// (`Err(_) => AlreadyExists`), the inconclusive HEAD's retryable error is
/// swallowed into a terminal `AlreadyExists`: the loop stops after one attempt
/// with a non-retryable error, so the `Transient`, the attempt count, and the
/// HEAD count all change.
#[tokio::test]
async fn a_409_whose_head_probe_is_inconclusive_is_retried_as_transient() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.always(Op::Put, Fault::ConditionalConflict);
    fake.always(Op::Head, Fault::InconclusiveHead);

    const MAX_ATTEMPTS: u32 = 3;
    let (result, attempts) = create_with_retry(
        &store,
        "conflict/inconclusive",
        b"never lands",
        MAX_ATTEMPTS,
    )
    .await;
    let err = result.expect_err("an inconclusive HEAD must not resolve the conflict to success");
    assert!(
        matches!(err, StoreError::Transient(_)),
        "an inconclusive probe leaves the key's state unknown, which is retryable Transient, \
         got {err:?}"
    );
    assert!(
        err.is_retryable(),
        "the surfaced error must be retryable so the caller retries: {err:?}"
    );
    assert_eq!(
        attempts, MAX_ATTEMPTS,
        "a retryable disambiguation must keep the caller loop retrying to its budget"
    );
    assert_eq!(
        fake.count(Op::Put),
        MAX_ATTEMPTS as usize,
        "exactly one PUT per attempt reached the endpoint"
    );
    assert_eq!(
        fake.count(Op::Head),
        MAX_ATTEMPTS as usize,
        "exactly one HEAD per attempt: an inconclusive HEAD is not retried inside object_store"
    );
    assert!(
        fake.object("conflict/inconclusive").is_none(),
        "no object ever became visible: every PUT was refused"
    );
}

/// A 409 whose disambiguating HEAD fails terminally (403 `AccessDenied`) stays
/// `AlreadyExists`: retrying cannot make the probe conclusive, so the
/// conservative terminal verdict is correct and the caller loop stops. The
/// terminal HEAD is not retried inside `object_store` either, so the counts
/// are exactly one PUT and one HEAD.
///
/// Unlike the inconclusive case above, this test also passes under the pre-fix
/// catch-all, which likewise returned `AlreadyExists` for a non-`NotFound`
/// HEAD error: it pins the terminal branch the fix deliberately keeps, so a
/// future change that made a terminal HEAD failure retryable would fail here.
#[tokio::test]
async fn a_409_whose_head_probe_fails_terminally_stays_already_exists() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.always(Op::Put, Fault::ConditionalConflict);
    fake.always(Op::Head, Fault::AccessDenied);

    let (result, attempts) =
        create_with_retry(&store, "conflict/head-denied", b"the loser's bytes", 5).await;
    let err = result.expect_err("a terminal HEAD failure must surface, not retry to success");
    assert!(
        matches!(err, StoreError::AlreadyExists),
        "a terminal HEAD failure cannot make the probe conclusive, so the conservative \
         AlreadyExists stands, got {err:?}"
    );
    assert!(
        !err.is_retryable(),
        "AlreadyExists is a protocol signal, not a retryable error: {err:?}"
    );
    assert_eq!(
        attempts, 1,
        "AlreadyExists stops the caller loop after exactly one attempt"
    );
    assert_eq!(
        fake.count(Op::Put),
        1,
        "no second PUT: a terminal disambiguation is not retried"
    );
    assert_eq!(
        fake.count(Op::Head),
        1,
        "exactly one HEAD: a 403 is not retried inside object_store"
    );
    assert!(
        fake.object("conflict/head-denied").is_none(),
        "no object ever became visible: the PUT was refused"
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
/// An unranged GET read to its end would carry a 256 MiB L1 compaction part as
/// one request against a 20 s ceiling it needs ~410 s to satisfy at that floor
/// rate: it could only time out, and then spend the retry budget re-issuing a
/// request that never fits. The first request is unranged (the only form an
/// endpoint answers with a stored checksum), so the bound is kept by reading
/// `bound` bytes of it and ranging the rest. The assertion is on what the
/// *server* saw: a client that read the unranged body to its end would issue
/// no ranged requests after it, and one that ranged its first request would
/// fail the unranged-first assertion.
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
    // The first request is unranged, so an endpoint that returns a stored
    // checksum only without a `Range` header can return it; the adapter reads
    // `bound` bytes of that body and drops the rest.
    assert_eq!(
        gets[0].range, None,
        "the first request of a whole-object read is unranged, saw {gets:?}"
    );
    for seen in &gets[1..] {
        let len = seen
            .range_len()
            .expect("every request after the first carries a Range header");
        assert!(
            len <= bound as u64,
            "a request asked for {len} bytes, above the {bound}-byte bound the \
             request timeout is sized for"
        );
    }
    // The ranges pick up exactly where the truncated first body stopped and
    // partition the rest of the object: no gap, no overlap.
    let mut covered: Vec<(u64, u64)> = gets.iter().filter_map(|seen| seen.range).collect();
    covered.sort_unstable();
    let mut next = bound as u64;
    for (start, end) in covered {
        assert_eq!(start, next, "ranges must be contiguous from the bound");
        next = end + 1;
    }
    assert_eq!(
        next, size as u64,
        "the first body and the ranges together must cover exactly the object"
    );
    // No single response carried the whole object, so nothing was verified.
    assert_eq!(
        store.get_unverified(),
        1,
        "a whole-object read split across responses is one unverified read"
    );
    // Unpinned: the continuation requests pin by ETag alone, although the first
    // response reported an `x-amz-version-id` they could have copied (see
    // `the_fake_reports_a_version_id_on_get_and_head`). A `versionId` would need
    // `s3:GetObjectVersion` on Ravel's own versioned bucket, which the shipped
    // IAM templates do not grant.
    for seen in &gets {
        assert_eq!(
            seen.version_id, None,
            "an unpinned whole-object read must send no versionId, saw {seen:?}"
        );
    }
    assert_eq!(
        gets[0].if_match, None,
        "the first request of an unpinned read has no ETag to pin to yet, saw {gets:?}"
    );
    let etag = etag_of(&object);
    for seen in &gets[1..] {
        assert_eq!(
            seen.if_match.as_deref(),
            Some(etag.as_str()),
            "every continuation request pins the first response's ETag, saw {seen:?}"
        );
    }
}

/// Pinning and checksum verification are two independent conditions on one
/// request, not two read paths. So the `get_pinned` form of the split read
/// above must put the caller's pin on *every* request it issues, the unranged
/// first one included: that request pays for a body, and a read whose first
/// request carried no `If-Match` would transfer bytes from whatever version the
/// key holds now and could only compare afterwards. The endpoint here evaluates
/// `If-Match` itself, so a dropped or wrong pin is a 412 on the wire, and the
/// request log pins which requests carried it. The pin comes from `pin_of`, and
/// the fake reports an `x-amz-version-id`, so it carries both halves: every
/// request must carry the ETag as `If-Match` and the version as `versionId`.
///
/// Mutation that fails it: passing `None` instead of `pin` for the first
/// request in `S3Store::get_whole_object` (`self.get_one(key, None, None,
/// Some(self.max_get_chunk))`) leaves `gets[0].if_match` at `None`, which the
/// per-request assertion below rejects on its first iteration.
#[tokio::test]
async fn a_caller_pinned_whole_object_read_pins_every_request_including_the_first() {
    let fake = FakeS3::start().await;
    let http = small_chunk_http();
    let bound = http.max_request_body_bytes();
    let metrics = Arc::new(StoreMetrics::default());
    let store = fake.store_with_http_and_metrics(http, Arc::clone(&metrics));

    // Three requests: the truncated unranged first plus two ranged ones, so the
    // assertion covers a continuation set with more than one member.
    let size = 2 * bound + 77;
    let object = patterned(size);
    fake.seed("fault/pinned-read", &object);

    let (_, pin) = store
        .pin_of("fault/pinned-read")
        .await
        .expect("the pin of a seeded object");
    let etag = etag_of(&object);
    assert_eq!(
        pin.etag, etag,
        "the pin must carry the endpoint's own ETag verbatim"
    );
    assert_eq!(
        pin.version.as_deref(),
        Some(VERSION_ID),
        "pin_of must record the endpoint's x-amz-version-id as the selector"
    );

    let read = store
        .get_pinned("fault/pinned-read", GetRange::Full, &pin)
        .await
        .expect("a matching pin serves the whole object");
    assert_eq!(
        &read.outcome.data[..],
        &object[..],
        "a pinned whole-object read returns every byte, in order"
    );
    assert_eq!(
        read.pin, pin,
        "the read reports the pin it was served under"
    );

    let gets = fake.requests(Op::Get);
    assert_eq!(
        gets.len(),
        size.div_ceil(bound),
        "the pinned read is split the same way an unpinned one is, saw {gets:?}"
    );
    assert_eq!(
        gets[0].range, None,
        "the first request stays unranged, so the endpoint can return its stored \
         checksum, saw {gets:?}"
    );
    for seen in &gets {
        assert_eq!(
            seen.if_match.as_deref(),
            Some(pin.etag.as_str()),
            "every request of a caller-pinned read carries the pin's ETag as \
             If-Match, saw {seen:?}"
        );
        assert_eq!(
            seen.version_id, pin.version,
            "every request of a caller-pinned read selects the pin's version, \
             saw {seen:?}"
        );
    }
    // `ravel_store_get_unverified_total` counts a full-object read once per
    // logical read when no single response carried the whole object under a
    // checksum this adapter can recompute: no checksum header, a digest it
    // cannot recompute, or a read split across bounded requests. A caller's
    // ranged read never counts, and neither does the HEAD behind `pin_of`. This
    // read is split: its unranged first response carries the stored checksum
    // but not the whole object, so the read counts exactly once, not once per
    // request and not zero times.
    assert_eq!(
        metrics.snapshot().get_unverified,
        1,
        "a split pinned read is one unverified read"
    );
}

/// The precondition the two split-read tests above rest on: the fake reports an
/// `x-amz-version-id` on a GET and on a HEAD, and the store surfaces it as a
/// pin's selector. Without it, "no request of an unpinned read carries a
/// `versionId`" would hold of a store that copies whatever version it saw.
#[tokio::test]
async fn the_fake_reports_a_version_id_on_get_and_head() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("fault/versioned", b"a small versioned object");

    let (_, pin) = store
        .pin_of("fault/versioned")
        .await
        .expect("HEAD of a seeded object");
    assert_eq!(
        pin.version.as_deref(),
        Some(VERSION_ID),
        "HEAD, got {pin:?}"
    );

    let read = store
        .get_with_pin("fault/versioned", GetRange::Full)
        .await
        .expect("GET of a seeded object");
    assert_eq!(
        read.pin.version.as_deref(),
        Some(VERSION_ID),
        "GET, got {:?}",
        read.pin
    );
}

/// An object `S3Store`'s split whole-object read uses: three requests at the
/// 1 MiB bound of [`small_chunk_http`], so a continuation set of two follows
/// the unranged first request.
fn seed_split_object(fake: &FakeS3, key: &str) -> Vec<u8> {
    let object = patterned(2 * small_chunk_http().max_request_body_bytes() + 77);
    fake.seed(key, &object);
    object
}

/// docs/object-store-contract.md: an object overwritten between a split read's
/// first request and its continuations fails the read instead of splicing two
/// versions, and for an *unpinned* read that failure is `Transient`, because a
/// fresh read sees one consistent version. The fake replaces the object right
/// after serving the first request, so every continuation's `If-Match` (the
/// first response's ETag) is refused with a 412.
#[tokio::test]
async fn an_unpinned_split_read_overwritten_mid_read_is_transient() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_http(small_chunk_http());
    let object = seed_split_object(&fake, "fault/overwritten-unpinned");
    fake.script(Op::Get, [Fault::OverwriteAfterServing]);

    let err = store
        .get("fault/overwritten-unpinned", GetRange::Full)
        .await
        .expect_err("a read that spans an overwrite must fail");
    assert!(matches!(err, StoreError::Transient(_)), "got {err:?}");

    let gets = fake.requests(Op::Get);
    assert_eq!(
        gets[0].fault,
        Some(Fault::OverwriteAfterServing),
        "the overwrite must have happened after the first request, saw {gets:?}"
    );
    assert_ne!(
        fake.object("fault/overwritten-unpinned").as_deref(),
        Some(&object[..]),
        "the fake must really hold a different object now"
    );
    let etag = etag_of(&object);
    assert!(
        gets.len() > 1
            && gets[1..]
                .iter()
                .all(|seen| seen.if_match.as_deref() == Some(etag.as_str())),
        "the refused continuations carried the first response's ETag, saw {gets:?}"
    );
}

/// The rule the continuation's error mapping carries: a refused precondition on
/// a *caller-pinned* read stays `PreconditionFailed`, never the `Transient` of
/// the unpinned case above. The caller named one identity, so a retry would
/// only be refused again. The pin is an ETag alone, the pin a grant records on
/// an unversioned bucket, for which the contract makes an overwrite
/// `PreconditionFailed`; this fake does not select by `versionId`, so a pin
/// with a version would get the same 412 here, where a versioned S3 bucket
/// would keep serving the pinned version.
///
/// Mutation that fails it: dropping the `if pin.is_none()` guard from the
/// continuation's `PreconditionFailed` arm in `S3Store::get_whole_object`
/// reports `Transient` here.
#[tokio::test]
async fn a_caller_pinned_split_read_overwritten_mid_read_stays_precondition_failed() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_http(small_chunk_http());
    let object = seed_split_object(&fake, "fault/overwritten-pinned");
    let pin = Pin::etag(etag_of(&object));
    fake.script(Op::Get, [Fault::OverwriteAfterServing]);

    let err = store
        .get_pinned("fault/overwritten-pinned", GetRange::Full, &pin)
        .await
        .expect_err("a pinned read that spans an overwrite must fail");
    assert!(matches!(err, StoreError::PreconditionFailed), "got {err:?}");

    let gets = fake.requests(Op::Get);
    assert_eq!(
        gets[0].fault,
        Some(Fault::OverwriteAfterServing),
        "the first request was served and the overwrite followed it, saw {gets:?}"
    );
    assert!(
        gets.len() > 1,
        "the refusal came from a continuation, not the first request, saw {gets:?}"
    );
}

/// A pin that does not match the object is refused by the first request, the
/// unranged one, before any body is paid for and before any continuation is
/// issued: exactly one GET reaches the endpoint, carrying the wrong ETag.
///
/// Mutation that fails it: passing `None` instead of `pin` for the first
/// request in `S3Store::get_whole_object` lets that request succeed, so the
/// refusal only comes from the continuations and the request count is 3.
#[tokio::test]
async fn a_wrong_etag_pinned_full_read_fails_on_the_first_request() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_http(small_chunk_http());
    seed_split_object(&fake, "fault/wrong-pin");
    let pin = Pin::etag("\"not-this-object\"");

    let err = store
        .get_pinned("fault/wrong-pin", GetRange::Full, &pin)
        .await
        .expect_err("a pin naming another object must be refused");
    assert!(matches!(err, StoreError::PreconditionFailed), "got {err:?}");

    let gets = fake.requests(Op::Get);
    assert_eq!(
        gets.len(),
        1,
        "one refused request and no more, saw {gets:?}"
    );
    assert_eq!(
        gets[0].range, None,
        "the refused request is the unranged first"
    );
    assert_eq!(gets[0].if_match.as_deref(), Some(pin.etag.as_str()));
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
    assert_eq!(
        store.get_unverified(),
        0,
        "an object that fits one unranged response is verified"
    );
}

/// A zero-byte object has no satisfiable range, and a ranged request for one is
/// a 416. The unranged first request needs no fallback for it: the empty body
/// is a legal 200, so the read is one request, and the stored checksum of the
/// empty object comes back with it and is verified.
#[tokio::test]
async fn whole_object_read_of_an_empty_object_is_one_unranged_request() {
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
    assert_eq!(gets.len(), 1, "one unranged request, saw {gets:?}");
    assert_eq!(gets[0].range, None, "the request must be unranged");
    assert_eq!(store.get_unverified(), 0, "the empty object was verified");
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
/// #993 review finding: with upload integrity enabled, an Overwrite put above
/// [`MULTIPART_THRESHOLD`] must NOT take the multipart path: the single-PUT
/// path covers every size to the 5 GiB ceiling, carries one whole-object
/// checksum, and costs one billed request where multipart costs parts + 2.
/// The op counters are the proof.
///
/// Demonstrated failing against the unguarded routing (dropping the
/// `!upload_integrity.is_enabled()` term from `put`): CreateMultipart then
/// counts 1 and the zero assertion fails.
#[tokio::test]
async fn integrity_enabled_put_above_threshold_stays_single_put() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_upload_integrity(UploadIntegrity::Crc64Nvme);
    let payload = Bytes::from(vec![9u8; MULTIPART_THRESHOLD + 1]);
    store
        .put("integrity/large", payload.clone(), PutOptions::default())
        .await
        .expect("a large put under integrity must succeed on the single-PUT path");
    assert_eq!(
        fake.count(Op::CreateMultipart),
        0,
        "under upload integrity the multipart path must not be taken"
    );
    assert_eq!(
        fake.count(Op::Put),
        1,
        "exactly one billed PUT request carries the whole object"
    );
    let got = fake
        .object("integrity/large")
        .expect("the object must be visible");
    assert!(got[..] == payload[..], "byte-identical stored object");
}

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

/// The happy path moves neither #864 counter: a multipart upload whose parts
/// and `CompleteMultipartUpload` all succeed is reaped by the completion, so no
/// abort is ever attempted and both counters read exactly 0. Without this, a
/// counter that also fired on success would still pass the two failure tests
/// above; this pins that a clean upload is silent, which is the baseline the
/// failure counts are read against.
#[tokio::test]
async fn successful_multipart_upload_moves_neither_counter() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    // No fault scripted anywhere: every UploadPart and the CompleteMultipart
    // succeed, so the upload is reaped by completion and never aborted.
    let payload = Bytes::from(vec![4u8; MULTIPART_THRESHOLD + 1]);
    store
        .put("multipart/clean", payload.clone(), PutOptions::default())
        .await
        .expect("a clean multipart put must succeed");

    assert_eq!(
        fake.count(Op::CreateMultipart),
        1,
        "the put must have taken the multipart path"
    );
    assert_eq!(
        fake.count(Op::CompleteMultipart),
        1,
        "a clean multipart put completes exactly once"
    );
    assert_eq!(
        fake.count(Op::AbortMultipart),
        0,
        "a completed upload issues no abort"
    );
    assert_eq!(
        store.multipart_abort_failures(),
        0,
        "no abort was attempted, so the failed-abort counter must be exactly 0"
    );
    assert_eq!(
        store.multipart_uploads_unreaped(),
        0,
        "a cleanly completed upload did not end without a successful reap"
    );
    assert_eq!(
        fake.object("multipart/clean").as_deref(),
        Some(&payload[..]),
        "the completed object must hold the uploaded bytes"
    );
}

/// A `put()` above the multipart threshold through a scheduled class handle
/// with upload integrity off keeps exactly as many requests in flight as the
/// permits it holds (issue #2327): one permit when the class has capacity 1,
/// two when the background class has two. The endpoint holds every part until
/// one more than the permits are in flight, or one second passes. A put within
/// its permits never reaches that target, so each held part waits the full
/// second and every part the store sends together is in flight at once; a put
/// that over-issues reaches it and shows the extra request in the peak.
#[tokio::test]
async fn scheduled_large_put_keeps_requests_within_its_permits() {
    // (scheduler sizing, use the background handle, permits the put can hold)
    for (config, background, permits) in [
        (SchedulerConfig::new(1, 1, 1), false, 1),
        (SchedulerConfig::new(8, 2, 1), true, 2),
    ] {
        let fake = FakeS3::start().await;
        fake.hold(&[Op::UploadPart], permits + 1, Duration::from_secs(1));
        let classed = ClassedStore::scheduled(Arc::new(fake.store()), config);
        let handle = if background {
            classed.background()
        } else {
            classed.foreground()
        };
        // Five parts: four of 8 MiB and one of a single byte.
        let payload = Bytes::from(vec![3u8; 4 * MULTIPART_PART_SIZE + 1]);
        handle
            .put("scheduled/large", payload.clone(), PutOptions::default())
            .await
            .expect("a scheduled multipart put must succeed");

        assert_eq!(
            fake.count(Op::CreateMultipart),
            1,
            "the put must have taken the multipart path"
        );
        assert_eq!(fake.count(Op::UploadPart), 5, "five parts uploaded");
        let peak = fake.peak_in_flight();
        assert_eq!(
            peak, permits,
            "{peak} requests were in flight at once for a put holding {permits} permit(s) \
             ({config:?}, background: {background})"
        );
        assert_eq!(
            fake.object("scheduled/large").as_deref(),
            Some(&payload[..]),
            "the completed object must hold the uploaded bytes"
        );
    }
}

/// With upload integrity on, `S3Store` sends a large overwrite as one PUT, so a
/// scheduled handle takes exactly one permit for it rather than the multipart
/// fan-out. In a class of five permits, four GETs are issued once the PUT
/// reaches the endpoint, which holds every PUT and GET until five requests have
/// been in flight together, so all five overlap there. A put holding more than one permit
/// leaves fewer than four for the GETs beside it, and after it ends only the
/// four GETs remain, so five are never in flight together.
#[tokio::test]
async fn scheduled_large_put_with_integrity_takes_one_permit() {
    const PERMITS: usize = 5;
    let fake = FakeS3::start().await;
    fake.seed("scheduled/read", b"read beside the put");
    fake.hold(&[Op::Put, Op::Get], PERMITS, Duration::from_secs(5));
    let classed = ClassedStore::scheduled(
        Arc::new(fake.store_with_upload_integrity(UploadIntegrity::Crc64Nvme)),
        SchedulerConfig::new(PERMITS, PERMITS, 1),
    );
    let handle = classed.foreground();
    let payload = Bytes::from(vec![6u8; 4 * MULTIPART_PART_SIZE + 1]);

    let put = handle.put(
        "scheduled/integrity",
        payload.clone(),
        PutOptions::default(),
    );
    let reads = async {
        fake.wait_in_flight(1).await;
        futures::future::join_all(
            (1..PERMITS).map(|_| handle.get("scheduled/read", GetRange::Full)),
        )
        .await
    };
    let (put, reads) = tokio::join!(put, reads);
    put.expect("a scheduled single-PUT put must succeed");
    for read in reads {
        read.expect("a GET beside the put must succeed");
    }

    assert_eq!(
        fake.count(Op::CreateMultipart),
        0,
        "integrity keeps one PUT"
    );
    assert_eq!(fake.count(Op::Put), 1, "exactly one PUT request");
    let peak = fake.peak_in_flight();
    assert_eq!(
        peak, PERMITS,
        "at most {peak} of {PERMITS} requests were ever in flight together, so the put held \
         permits its single PUT does not use"
    );
    assert_eq!(
        fake.object("scheduled/integrity").as_deref(),
        Some(&payload[..]),
        "the object must hold the uploaded bytes"
    );
}

/// A whole-object read above the request body bound, through a scheduled class
/// handle, keeps exactly as many requests in flight as the permits it holds
/// (issue #2493): one when the class has capacity 1, two when the background
/// class has two. The read is one truncated unranged GET and then five ranged
/// ones. The endpoint holds every GET until one more than the permits are in
/// flight, or one second passes, as the put test above does: a read within its
/// permits never reaches that target, so every request it sends together is in
/// flight at once; a read that over-issues reaches it and shows the extra
/// request in the peak.
#[tokio::test]
async fn scheduled_large_get_keeps_requests_within_its_permits() {
    // (scheduler sizing, use the background handle, permits the read can hold)
    for (config, background, permits) in [
        (SchedulerConfig::new(1, 1, 1), false, 1),
        (SchedulerConfig::new(8, 2, 1), true, 2),
    ] {
        let fake = FakeS3::start().await;
        let http = small_chunk_http();
        let bound = http.max_request_body_bytes();
        let object = patterned(5 * bound + 123);
        fake.seed("scheduled/large-read", &object);
        fake.hold(&[Op::Get], permits + 1, Duration::from_secs(1));
        let classed = ClassedStore::scheduled(Arc::new(fake.store_with_http(http)), config);
        let handle = if background {
            classed.background()
        } else {
            classed.foreground()
        };

        let outcome = handle
            .get("scheduled/large-read", GetRange::Full)
            .await
            .expect("a scheduled whole-object read must succeed");

        assert_eq!(
            &outcome.data[..],
            &object[..],
            "the read must return the object's exact bytes"
        );
        assert_eq!(
            fake.count(Op::Get),
            6,
            "the read must have been split into one unranged and five ranged requests"
        );
        let peak = fake.peak_in_flight();
        assert_eq!(
            peak, permits,
            "{peak} requests were in flight at once for a read holding {permits} permit(s) \
             ({config:?}, background: {background})"
        );
    }
}

/// A read whose size is known to be one request takes one permit: a
/// whole-object read that fits in one response, and a caller-supplied range or
/// suffix of any size, which is never split. In a class of three permits the
/// endpoint holds every GET until three are in flight together. Three such
/// reads issued at once each take one permit, so all three overlap there; a
/// read taking a second permit would leave the third waiting, and three would
/// never be in flight together.
#[tokio::test]
async fn scheduled_small_and_ranged_gets_take_one_permit() {
    const PERMITS: usize = 3;
    let fake = FakeS3::start().await;
    let http = small_chunk_http();
    let bound = http.max_request_body_bytes() as u64;
    fake.seed("scheduled/small", b"one response carries this object");
    fake.seed(
        "scheduled/large",
        &patterned(usize::try_from(5 * bound).expect("a 5 MiB size fits a usize")),
    );
    fake.hold(&[Op::Get], PERMITS, Duration::from_secs(5));
    let classed = ClassedStore::scheduled(
        Arc::new(fake.store_with_http(http)),
        SchedulerConfig::new(PERMITS, PERMITS, 1),
    );
    let handle = classed.foreground();

    let (small, ranged, suffix) = tokio::join!(
        handle.get("scheduled/small", GetRange::Full),
        handle.get("scheduled/large", GetRange::Range(0, 3 * bound)),
        handle.get("scheduled/large", GetRange::Suffix(2 * bound)),
    );
    small.expect("a small whole-object read must succeed");
    assert_eq!(
        ranged.expect("a ranged read must succeed").data.len() as u64,
        3 * bound
    );
    assert_eq!(
        suffix.expect("a suffix read must succeed").data.len() as u64,
        2 * bound
    );

    assert_eq!(fake.count(Op::Get), PERMITS, "one request per read");
    let peak = fake.peak_in_flight();
    assert_eq!(
        peak, PERMITS,
        "at most {peak} of {PERMITS} reads were ever in flight together, so a read held \
         permits its single request does not use"
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

// ---------------------------------------------------------------------------
// Observed store time (ADR-1685 decision 1)
// ---------------------------------------------------------------------------

/// The `Date` the fake stamps on the first GET, and its unix nanoseconds. The
/// nanoseconds are written out rather than computed from the string, so a
/// parser that drifted (a month off by one, seconds dropped) fails here instead
/// of agreeing with itself.
const STORE_DATE_NOON: &str = "Wed, 16 Sep 2026 12:00:00 GMT";
const STORE_DATE_NOON_NS: i64 = 1_789_560_000_000_000_000;

/// One hour earlier, for the response that arrives *after* the noon one.
const STORE_DATE_ELEVEN: &str = "Wed, 16 Sep 2026 11:00:00 GMT";
const STORE_DATE_ELEVEN_NS: i64 = 1_789_556_400_000_000_000;

/// A store that has issued no request has observed no store clock, so a caller
/// gets `None` rather than a fabricated reading (ADR-1685 decision 4: no
/// observation means no check).
#[tokio::test]
async fn a_store_that_has_issued_no_request_has_no_observation() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    assert_eq!(store.observed_store_time_ns(), None);
}

/// One GET against an endpoint whose response carries a known `Date` makes that
/// date, to the nanosecond, what the store reports.
#[tokio::test]
async fn one_response_makes_its_date_the_observed_store_time() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("clock/object", b"payload");
    fake.script(Op::Get, [Fault::FixedDate(STORE_DATE_NOON)]);

    store
        .get("clock/object", GetRange::Full)
        .await
        .expect("a healthy GET must succeed");

    assert_eq!(fake.count(Op::Get), 1, "a small object is exactly one GET");
    assert_eq!(
        store.observed_store_time_ns(),
        Some(STORE_DATE_NOON_NS),
        "the observation is the response's Date, in unix nanoseconds"
    );
}

/// The latest response wins, in both directions. A second response carrying an
/// *older* `Date` replaces the first: the observation is not a running maximum,
/// so one wrong header from a proxy is corrected by the next response rather
/// than latched for the life of the process (ADR-1685 decision 1).
///
/// To watch this fail: make `ObservedStoreTime::observe` keep the larger of the
/// stored value and `ns` (`crates/ravel-object-store/src/s3/connector.rs`,
/// `self.ns.store(ns, Ordering::Relaxed)` becomes
/// `self.ns.fetch_max(ns, Ordering::Relaxed)`). The first two assertions still
/// pass and the third reports the noon observation where the eleven o'clock one
/// belongs.
#[tokio::test]
async fn a_later_response_with_an_older_date_replaces_the_observation() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("clock/object", b"payload");
    fake.script(
        Op::Get,
        [
            Fault::FixedDate(STORE_DATE_NOON),
            Fault::FixedDate(STORE_DATE_ELEVEN),
        ],
    );

    store
        .get("clock/object", GetRange::Full)
        .await
        .expect("the first GET must succeed");
    assert_eq!(
        store.observed_store_time_ns(),
        Some(STORE_DATE_NOON_NS),
        "the first response is observed"
    );

    store
        .get("clock/object", GetRange::Full)
        .await
        .expect("the second GET must succeed");
    assert_eq!(
        store.observed_store_time_ns(),
        Some(STORE_DATE_ELEVEN_NS),
        "the latest response wins: an older Date replaces a newer one"
    );
}

/// A response whose `Date` cannot be read changes nothing: the previous
/// observation stands rather than being cleared, which would turn a momentary
/// bad header into "no observation" and silently skip a caller's check.
///
/// The header is unparseable rather than absent because hyper stamps its own
/// `Date` on any response that carries none, so the fake cannot omit one; the
/// connector treats both the same way, and the missing-header case is pinned by
/// `connector.rs`'s own unit tests.
#[tokio::test]
async fn an_unreadable_date_leaves_the_previous_observation_standing() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.seed("clock/object", b"payload");
    fake.script(
        Op::Get,
        [
            Fault::FixedDate(STORE_DATE_NOON),
            Fault::FixedDate("not-a-valid-date"),
        ],
    );

    store
        .get("clock/object", GetRange::Full)
        .await
        .expect("the first GET must succeed");
    store
        .get("clock/object", GetRange::Full)
        .await
        .expect("the second GET must succeed");

    assert_eq!(fake.count(Op::Get), 2, "both GETs reached the endpoint");
    assert_eq!(
        store.observed_store_time_ns(),
        Some(STORE_DATE_NOON_NS),
        "an unparseable Date leaves the previous observation alone"
    );
}

/// An error response carries the store's clock too, so a request that failed
/// still seeds the observation. This is what keeps a process that is being
/// throttled from falling back to "no observation" exactly when its writes are
/// retrying.
#[tokio::test]
async fn an_error_response_is_observed_like_any_other() {
    let fake = FakeS3::start().await;
    let store = fake.store();
    fake.always(Op::Get, Fault::AccessDenied);

    let error = store
        .get("clock/denied", GetRange::Full)
        .await
        .expect_err("403 AccessDenied is permanent");
    assert!(
        matches!(error, StoreError::AccessDenied(_)),
        "a 403 maps to AccessDenied, got {error:?}"
    );
    // hyper stamped this response's `Date` itself: the assertion is that *some*
    // observation exists after a failed request, not which instant it names.
    assert!(
        store.observed_store_time_ns().is_some(),
        "a 403 response still reports the store's clock"
    );
}
