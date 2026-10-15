//! The pending strict-ack permit is taken at the request head (ADR-2793,
//! head pending-ack amendment, issue #2805): OTLP HTTP, Remote Write and the
//! unary OTLP gRPC services take it once the in-flight permit is held and the
//! credential resolves, for a request the surface writes strictly, and hold
//! it until that request's durability acknowledgement.
//!
//! Every case runs against `ravel_server::start` with every logs data PUT
//! held by a `FaultStore` gate, so "an ack is pending" is a held PUT rather
//! than a timing window. A request parked between its head and its submit is
//! a raw HTTP/1.1 connection that sends its head with
//! `Expect: 100-continue` and no body: the server sends `100 Continue` only
//! once the admission middleware has run and the handler starts reading the
//! body, so receiving it proves the head was admitted, and the body is never
//! sent until the test says so.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use prost::Message;
use ravel_server::config::StrictFlushPolicy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "support/strict_flush.rs"]
mod strict_flush;

use strict_flush::{PolicyServer, Signal, TOKEN, process_sample, release_until};

/// Bounds every wait here, so a request that is never answered fails the
/// test instead of hanging it. No outcome is decided by it.
const WAIT: Duration = Duration::from_secs(15);

const SHED_BODY: &str = "process pending strict-acknowledgement limit reached";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn export_body(line: &str) -> Vec<u8> {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: now_ns() as u64,
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(line.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// Bytes no protobuf decoder accepts: a field tag of zero.
const UNDECODABLE: &[u8] = &[0x00, 0x00, 0x00, 0x00];

/// POSTs `body` to `path` with the bearer token and, when given, an
/// `x-ravel-ingest-mode` header.
fn spawn_post(
    server: &PolicyServer,
    path: &str,
    mode: Option<&str>,
    body: Vec<u8>,
) -> tokio::task::JoinHandle<reqwest::Response> {
    let mut request = server
        .client
        .post(format!("{}{path}", server.base))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body);
    if let Some(mode) = mode {
        request = request.header("x-ravel-ingest-mode", mode);
    }
    tokio::spawn(async move { request.send().await.expect("export completes") })
}

fn spawn_export(
    server: &PolicyServer,
    mode: Option<&str>,
    line: &str,
) -> tokio::task::JoinHandle<reqwest::Response> {
    spawn_post(server, "/v1/logs", mode, export_body(line))
}

/// Awaits `task`'s response, failing the test if a data PUT is issued first:
/// a request that should have been refused at its head was admitted and
/// written instead.
async fn answered_before_any_put<T>(server: &PolicyServer, task: tokio::task::JoinHandle<T>) -> T {
    let held_before = server.gate.held_count();
    tokio::time::timeout(WAIT, async {
        tokio::select! {
            out = task => out.expect("request task"),
            () = server.gate.wait_until_held(held_before + 1) => panic!(
                "a new data PUT was issued before the request was answered: \
                 it was admitted and written, not refused at its head"
            ),
        }
    })
    .await
    .expect("the request is answered")
}

/// A strict OTLP logs export parked between its admitted head and its
/// submit: the head is sent with `Expect: 100-continue`, the `100 Continue`
/// is read back, and the body is held until [`ParkedExport::finish`].
struct ParkedExport {
    stream: TcpStream,
    body: Vec<u8>,
}

impl ParkedExport {
    async fn open(addr: SocketAddr, line: &str) -> ParkedExport {
        let body = export_body(line);
        let head = format!(
            "POST /v1/logs HTTP/1.1\r\n\
             host: {addr}\r\n\
             authorization: Bearer {TOKEN}\r\n\
             content-type: application/x-protobuf\r\n\
             x-ravel-ingest-mode: strict\r\n\
             expect: 100-continue\r\n\
             content-length: {}\r\n\r\n",
            body.len()
        );
        let mut stream = TcpStream::connect(addr).await.expect("connect to listener");
        stream
            .write_all(head.as_bytes())
            .await
            .expect("write request head");
        let interim = read_response_head(&mut stream).await;
        assert!(
            interim.starts_with("HTTP/1.1 100"),
            "the parked export's head is admitted and its body awaited, got: {interim}"
        );
        ParkedExport { stream, body }
    }

    /// Sends the held body and returns the final response's status code.
    async fn finish(mut self) -> u16 {
        self.stream
            .write_all(&self.body)
            .await
            .expect("write request body");
        let head = read_response_head(&mut self.stream).await;
        head.split(' ')
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("unparsable response head: {head}"))
    }
}

/// Reads one response head, up to its blank line.
async fn read_response_head(stream: &mut TcpStream) -> String {
    let mut raw = Vec::new();
    tokio::time::timeout(WAIT, async {
        let mut byte = [0u8; 1];
        while !raw.ends_with(b"\r\n\r\n") {
            let n = stream.read(&mut byte).await.expect("read response");
            assert!(n > 0, "connection closed mid-head: {raw:?}");
            raw.push(byte[0]);
        }
    })
    .await
    .expect("a response head arrives");
    String::from_utf8_lossy(&raw).to_string()
}

async fn assert_pending_ack_shed(response: reqwest::Response) {
    assert_eq!(response.status(), 429, "refused for the pending-ack bound");
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    assert_eq!(response.text().await.expect("refusal body"), SHED_BODY);
}

async fn pending_strict_acks(server: &PolicyServer) -> u64 {
    process_sample(&server.scrape().await, "ravel_ingest_pending_strict_acks")
}

async fn pending_ack_shed_total(server: &PolicyServer) -> u64 {
    process_sample(
        &server.scrape().await,
        "ravel_ingest_pending_ack_shed_total",
    )
}

async fn start(max_pending_strict_acks: u64) -> PolicyServer {
    strict_flush::start_with(StrictFlushPolicy::Arrival, Signal::Logs, |config| {
        config.max_pending_strict_acks = max_pending_strict_acks;
    })
    .await
}

/// With a budget of 1 held by a strict export parked between its head and
/// its submit (head admitted, body not yet sent), a second strict export is
/// refused with the pending-ack 429 before any data PUT, and the parked
/// export still acks once its body arrives.
#[tokio::test]
async fn a_strict_export_parked_before_submit_holds_the_head_permit() {
    let server = start(1).await;

    let parked = ParkedExport::open(server.running.http_addr, "parked strict log line").await;
    let second = spawn_export(&server, Some("strict"), "second strict log line");
    assert_pending_ack_shed(answered_before_any_put(&server, second).await).await;
    assert_eq!(pending_ack_shed_total(&server).await, 1);

    let status = release_until(&server.gate, parked.finish()).await;
    assert_eq!(
        status, 200,
        "the parked export acks with the permit it took"
    );
    assert_eq!(pending_strict_acks(&server).await, 0);

    server.shutdown().await;
}

/// With a budget of 1 held by a strict export waiting on its held data PUT,
/// a strict export whose body does not decode is refused with the
/// pending-ack 429, not the decode 400: the refusal is decided before the
/// body is read.
#[tokio::test]
async fn an_undecodable_strict_export_over_the_bound_is_refused_429_not_400() {
    let server = start(1).await;

    let pending = spawn_export(&server, Some("strict"), "pending strict log line");
    server.wait_for_data_put().await;

    let refused = spawn_post(&server, "/v1/logs", Some("strict"), UNDECODABLE.to_vec())
        .await
        .expect("export task");
    assert_pending_ack_shed(refused).await;
    assert_eq!(pending_ack_shed_total(&server).await, 1);

    let pending = release_until(&server.gate, pending)
        .await
        .expect("export task");
    assert_eq!(pending.status(), 200);

    server.shutdown().await;
}

/// A strict export admitted at its head that ends without a write (its body
/// does not decode) returns its head permit with its response: the gauge
/// reads 0 afterwards and the next strict export under a budget of 1 is
/// admitted and acks.
#[tokio::test]
async fn a_strict_export_that_ends_without_a_write_returns_its_head_permit() {
    let server = start(1).await;

    let rejected = spawn_post(&server, "/v1/logs", Some("strict"), UNDECODABLE.to_vec())
        .await
        .expect("export task");
    assert_eq!(rejected.status(), 400, "the body does not decode");
    assert_eq!(pending_strict_acks(&server).await, 0);

    let next = spawn_export(&server, Some("strict"), "next strict log line");
    server.wait_for_data_put().await;
    let next = release_until(&server.gate, next)
        .await
        .expect("export task");
    assert_eq!(
        next.status(),
        200,
        "the returned permit admits the next one"
    );
    assert_eq!(pending_ack_shed_total(&server).await, 0);

    server.shutdown().await;
}

/// With a budget of 2 and the data PUT held, two concurrent strict exports
/// are both admitted and both wait on the PUT, the gauge reads 2 (one permit
/// each, taken once), and a third strict export is refused.
#[tokio::test]
async fn a_budget_of_two_admits_two_strict_exports_once_each_and_refuses_a_third() {
    let server = start(2).await;

    let mut first = spawn_export(&server, Some("strict"), "first strict log line");
    server.wait_for_buffered_while_pending(1, &mut first).await;
    let mut second = spawn_export(&server, Some("strict"), "second strict log line");
    server.wait_for_buffered_while_pending(2, &mut second).await;
    assert_eq!(
        pending_strict_acks(&server).await,
        2,
        "each admitted strict export holds exactly one permit until its ack"
    );

    let third = spawn_export(&server, Some("strict"), "third strict log line");
    assert_pending_ack_shed(answered_before_any_put(&server, third).await).await;
    assert_eq!(pending_ack_shed_total(&server).await, 1);

    let (first, second) = release_until(&server.gate, async {
        (
            first.await.expect("export task"),
            second.await.expect("export task"),
        )
    })
    .await;
    assert_eq!(first.status(), 200);
    assert_eq!(second.status(), 200);
    assert_eq!(pending_strict_acks(&server).await, 0);

    server.shutdown().await;
}

/// With a budget of 1 held by a strict export waiting on its held data PUT,
/// a buffered export is accepted on both transports: 200 over OTLP HTTP and
/// OK over OTLP gRPC, each reading its mode from its own
/// `x-ravel-ingest-mode`.
#[tokio::test]
async fn a_buffered_export_takes_no_pending_ack_permit_on_http_or_grpc() {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

    let server = start(1).await;
    let grpc_addr = server.running.grpc_addr.expect("gateway mode binds gRPC");

    let pending = spawn_export(&server, Some("strict"), "pending strict log line");
    server.wait_for_data_put().await;

    let http = spawn_export(&server, Some("buffered"), "buffered http log line")
        .await
        .expect("export task");
    assert_eq!(http.status(), 200, "a buffered HTTP export is admitted");

    let mut client = LogsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(
        ExportLogsServiceRequest::decode(export_body("buffered grpc log line").as_slice())
            .expect("decodes"),
    );
    let metadata = request.metadata_mut();
    metadata.insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    metadata.insert("x-ravel-ingest-mode", "buffered".parse().expect("ascii"));
    let grpc = tokio::time::timeout(WAIT, client.export(request))
        .await
        .expect("the gRPC export is answered");
    assert!(
        grpc.is_ok(),
        "a buffered gRPC export is admitted: {:?}",
        grpc.err()
    );
    assert_eq!(pending_ack_shed_total(&server).await, 0);
    assert_eq!(pending_strict_acks(&server).await, 1);

    let pending = release_until(&server.gate, pending)
        .await
        .expect("export task");
    assert_eq!(pending.status(), 200);

    server.shutdown().await;
}

/// Remote Write reads no mode header: with a budget of 1 held by a strict
/// export waiting on its held data PUT, a Remote Write carrying
/// `x-ravel-ingest-mode: buffered` is still refused with the pending-ack 429.
/// It negotiates protocol 0.1.0 and its body does not decode, so a head that
/// let the header make it buffered would answer the decode's 400 instead.
#[tokio::test]
async fn a_remote_write_marked_buffered_is_still_bounded_as_strict() {
    let server = start(1).await;

    let pending = spawn_export(&server, Some("strict"), "pending strict log line");
    server.wait_for_data_put().await;

    let refused = server
        .client
        .post(format!("{}/api/v1/write", server.base))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .header("x-ravel-ingest-mode", "buffered")
        .body(UNDECODABLE.to_vec())
        .send()
        .await
        .expect("remote write completes");
    assert_pending_ack_shed(refused).await;
    assert_eq!(pending_ack_shed_total(&server).await, 1);

    let pending = release_until(&server.gate, pending)
        .await
        .expect("export task");
    assert_eq!(pending.status(), 200);

    server.shutdown().await;
}

/// `ravel_ingest_pending_strict_acks` on the real `/metrics` counts strict
/// requests from head admission: with one strict export parked before its
/// submit and another waiting on its held data PUT, it reads 2, and 0 once
/// both have acked.
#[tokio::test]
async fn pending_strict_acks_gauge_counts_a_request_admitted_at_its_head() {
    let server = start(8).await;

    let mut held = spawn_export(&server, Some("strict"), "held strict log line");
    server.wait_for_buffered_while_pending(1, &mut held).await;
    let parked = ParkedExport::open(server.running.http_addr, "parked strict log line").await;

    assert_eq!(
        pending_strict_acks(&server).await,
        2,
        "the export still being read counts alongside the one awaiting its ack"
    );

    let (held, parked) = release_until(&server.gate, async {
        (held.await.expect("export task"), parked.finish().await)
    })
    .await;
    assert_eq!(held.status(), 200);
    assert_eq!(parked, 200);
    assert_eq!(pending_strict_acks(&server).await, 0);

    server.shutdown().await;
}
