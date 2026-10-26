//! `PrometheusClient` query deadline against local socket fixtures: a stalled
//! body is a `Timeout { phase: Body }` within the deadline, and the other
//! failure shapes (refused connect, short body, oversized body) are never
//! reported as a timeout.

#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use ravel_promql_difftest::prometheus_client::{
    PrometheusClient, PrometheusClientError, QueryOutcome, TimeoutPhase, TransportErrorKind,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DEADLINE: Duration = Duration::from_millis(500);
/// Generous upper bound on a call that must stop at `DEADLINE`.
const DEADLINE_BOUND: Duration = Duration::from_secs(2);

/// One scripted response step: wait, then write bytes.
struct Step {
    delay: Duration,
    bytes: Vec<u8>,
}

fn step(delay_ms: u64, bytes: impl Into<Vec<u8>>) -> Step {
    Step {
        delay: Duration::from_millis(delay_ms),
        bytes: bytes.into(),
    }
}

/// Serves one connection: reads the request head, plays `steps`, then either
/// closes or (with `hold`) keeps the socket open without writing.
async fn serve_once(steps: Vec<Step>, hold: bool) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        read_request_head(&mut sock).await;
        for s in steps {
            tokio::time::sleep(s.delay).await;
            if sock.write_all(&s.bytes).await.is_err() {
                return;
            }
        }
        if hold {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        drop(sock);
    });
    addr
}

async fn read_request_head(sock: &mut TcpStream) {
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = sock.read(&mut buf).await.expect("read request");
        if n == 0 {
            return;
        }
        head.extend_from_slice(&buf[..n]);
    }
}

fn headers(status: &str, content_length: usize) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {content_length}\r\n\r\n"
    )
}

fn client(addr: SocketAddr) -> PrometheusClient {
    PrometheusClient::with_deadline(format!("http://{addr}"), DEADLINE)
}

#[tokio::test]
async fn stalled_body_is_a_body_timeout_within_the_deadline() {
    let addr = serve_once(vec![step(0, headers("200 OK", 100_000))], true).await;
    let started = Instant::now();
    let outcome = client(addr)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    let wall = started.elapsed();
    eprintln!("stalled-body wall elapsed: {wall:?}, outcome: {outcome:?}");
    assert!(wall < DEADLINE_BOUND, "call took {wall:?}"); // allow-wall-clock: the deadline under test bounds real socket I/O; 4x upper bound only
    match outcome {
        QueryOutcome::Timeout {
            phase,
            elapsed,
            timing,
        } => {
            assert_eq!(phase, TimeoutPhase::Body);
            assert!(elapsed >= DEADLINE, "expired early: {elapsed:?}");
            assert!(elapsed < DEADLINE_BOUND, "expired late: {elapsed:?}");
            assert!(timing.dispatch_to_headers.is_some(), "{timing:?}");
            assert!(timing.headers_to_body_complete.is_none(), "{timing:?}");
            assert!(timing.parse.is_none(), "{timing:?}");
        }
        other => panic!("expected Timeout {{ phase: Body }}, got {other:?}"),
    }
}

#[tokio::test]
async fn stalled_body_through_the_wrapper_is_the_timeout_error() {
    let addr = serve_once(vec![step(0, headers("200 OK", 100_000))], true).await;
    let result = client(addr).instant_query("up", 0).await;
    assert!(
        matches!(
            result,
            Err(PrometheusClientError::Timeout {
                phase: TimeoutPhase::Body,
                ..
            })
        ),
        "{result:?}"
    );
}

/// The deadline covers the whole operation, not each phase: headers at
/// 300 ms and the body at 700 ms each fit 500 ms on their own.
#[tokio::test]
async fn deadline_is_shared_across_phases() {
    let body = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
    let addr = serve_once(
        vec![
            step(300, headers("200 OK", body.len())),
            step(400, body.as_bytes()),
        ],
        true,
    )
    .await;
    let outcome = client(addr)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    match outcome {
        QueryOutcome::Timeout { phase, timing, .. } => {
            assert_eq!(phase, TimeoutPhase::Body);
            assert!(timing.dispatch_to_headers.is_some(), "{timing:?}");
            assert!(timing.headers_to_body_complete.is_none(), "{timing:?}");
        }
        other => panic!("expected Timeout {{ phase: Body }}, got {other:?}"),
    }
}

#[tokio::test]
async fn refused_connect_is_transport_not_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let outcome = client(addr)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    match outcome {
        QueryOutcome::Transport { kind, timing, .. } => {
            assert_eq!(kind, TransportErrorKind::Connect);
            assert!(timing.dispatch_to_headers.is_none(), "{timing:?}");
        }
        other => panic!("expected Transport {{ kind: Connect }}, got {other:?}"),
    }
}

#[tokio::test]
async fn complete_body_in_time_is_an_envelope() {
    let body = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
    let mut response = headers("200 OK", body.len()).into_bytes();
    response.extend_from_slice(body.as_bytes());
    let addr = serve_once(vec![step(0, response)], false).await;
    let outcome = client(addr)
        .range_query_with_deadline("up", 0, 60_000, 15_000, DEADLINE)
        .await;
    match outcome {
        QueryOutcome::Envelope {
            http_status,
            body,
            timing,
        } => {
            assert_eq!(http_status, 200);
            assert_eq!(body["status"], "success");
            assert_eq!(body["data"]["resultType"], "vector");
            assert!(timing.dispatch_to_headers.is_some(), "{timing:?}");
            assert!(timing.headers_to_body_complete.is_some(), "{timing:?}");
            assert!(timing.parse.is_some(), "{timing:?}");
        }
        other => panic!("expected Envelope, got {other:?}"),
    }
}

/// A 4xx with a JSON body is an envelope: the error corpus reads
/// `errorType` from it.
#[tokio::test]
async fn error_status_with_json_body_is_an_envelope() {
    let body = r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#;
    let mut response = headers("400 Bad Request", body.len()).into_bytes();
    response.extend_from_slice(body.as_bytes());
    let addr = serve_once(vec![step(0, response)], false).await;
    let outcome = client(addr)
        .instant_query_with_deadline("up{", 0, DEADLINE)
        .await;
    match outcome {
        QueryOutcome::Envelope {
            http_status, body, ..
        } => {
            assert_eq!(http_status, 400);
            assert_eq!(body["errorType"], "bad_data");
        }
        other => panic!("expected Envelope, got {other:?}"),
    }
}

#[tokio::test]
async fn body_closed_short_is_transport_not_timeout() {
    let mut response = headers("200 OK", 10).into_bytes();
    response.extend_from_slice(b"{\"a\":");
    let addr = serve_once(vec![step(0, response)], false).await;
    let outcome = client(addr)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    match outcome {
        QueryOutcome::Transport { kind, timing, .. } => {
            assert_eq!(kind, TransportErrorKind::Body);
            assert!(timing.dispatch_to_headers.is_some(), "{timing:?}");
            assert!(timing.headers_to_body_complete.is_none(), "{timing:?}");
        }
        other => panic!("expected Transport {{ kind: Body }}, got {other:?}"),
    }
}

#[tokio::test]
async fn declared_body_over_the_cap_is_body_too_large() {
    let addr = serve_once(vec![step(0, headers("200 OK", 100_000))], true).await;
    let outcome = client(addr)
        .with_max_body_bytes(1024)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    assert!(
        matches!(outcome, QueryOutcome::BodyTooLarge { limit: 1024, .. }),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn received_body_over_the_cap_is_body_too_large() {
    let body = "x".repeat(4096);
    let mut response =
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n"
            .to_vec();
    response.extend_from_slice(format!("{:x}\r\n{body}\r\n0\r\n\r\n", body.len()).as_bytes());
    let addr = serve_once(vec![step(0, response)], false).await;
    let outcome = client(addr)
        .with_max_body_bytes(1024)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    assert!(
        matches!(outcome, QueryOutcome::BodyTooLarge { limit: 1024, .. }),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn non_json_body_is_a_decode_failure() {
    let body = "<html>bad gateway</html>";
    let response = format!(
        "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/html\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes();
    let addr = serve_once(vec![step(0, response)], false).await;
    let outcome = client(addr)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    assert!(
        matches!(
            outcome,
            QueryOutcome::Transport {
                kind: TransportErrorKind::Decode,
                ..
            }
        ),
        "{outcome:?}"
    );
}

/// A listener whose accept queue is full and never drained drops further
/// SYNs on Linux, so the connect stalls until the transport's connect timer
/// fires.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn stalled_connect_is_a_connect_timeout() {
    let socket = tokio::net::TcpSocket::new_v4().expect("socket");
    socket
        .bind("127.0.0.1:0".parse().expect("addr"))
        .expect("bind");
    let addr = socket.local_addr().expect("addr");
    let _listener = socket.listen(1).expect("listen");
    let mut fillers = Vec::new();
    for _ in 0..8 {
        if let Ok(Ok(s)) =
            tokio::time::timeout(Duration::from_millis(100), TcpStream::connect(addr)).await
        {
            fillers.push(s);
        }
    }
    let started = Instant::now();
    let outcome = client(addr)
        .instant_query_with_deadline("up", 0, DEADLINE)
        .await;
    let wall = started.elapsed();
    assert!(wall < DEADLINE_BOUND, "call took {wall:?}"); // allow-wall-clock: the deadline under test bounds real socket I/O; 4x upper bound only
    match outcome {
        QueryOutcome::Timeout { phase, timing, .. } => {
            assert_eq!(phase, TimeoutPhase::Connect);
            assert!(timing.dispatch_to_headers.is_none(), "{timing:?}");
        }
        other => panic!("expected Timeout {{ phase: Connect }}, got {other:?}"),
    }
}
