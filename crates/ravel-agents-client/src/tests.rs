//! Tests against a hand-rolled HTTP/1.1 server on a loopback port. Each test
//! hands the server the answers it should give, in order, and reads back the
//! requests it received.

use std::sync::{Arc, Mutex};

use ravel_agents_core::error::{ErrorKind, RetryHint};
use ravel_agents_core::wire::{
    AgentRef, CallRequest, EventsRequest, OperationStatus, ReconcileDecision, ReconcileRequest,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    target: String,
    authorization: Option<String>,
    body: Vec<u8>,
}

impl Seen {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("request body is JSON")
    }
}

struct Loopback {
    url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Loopback {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("lock").clone()
    }
}

/// Serves one canned `(status, body)` answer per connection, in order, then
/// stops accepting.
async fn serve(answers: Vec<(u16, String)>) -> Loopback {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        for (status, body) in answers {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let request = read_request(&mut socket).await;
            log.lock().expect("lock").push(request);
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write");
            socket.shutdown().await.expect("shutdown");
        }
    });
    Loopback {
        url: format!("http://{addr}"),
        seen,
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Seen {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = socket.read(&mut chunk).await.expect("read");
        assert!(n > 0, "client closed before the headers ended");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().expect("request line").split(' ');
    let method = first.next().expect("method").to_string();
    let target = first.next().expect("target").to_string();
    let mut length = 0usize;
    let mut authorization = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            if name == "content-length" {
                length = value.trim().parse().expect("content-length");
            } else if name == "authorization" {
                authorization = Some(value.trim().to_string());
            }
        }
    }
    while buf.len() < header_end + length {
        let n = socket.read(&mut chunk).await.expect("read body");
        assert!(n > 0, "client closed before the body ended");
        buf.extend_from_slice(&chunk[..n]);
    }
    Seen {
        method,
        target,
        authorization,
        body: buf[header_end..header_end + length].to_vec(),
    }
}

fn agent() -> AgentRef {
    AgentRef {
        kind: "assistant".into(),
        key: "user 1/a&b".into(),
    }
}

fn error_body(code: &str, retry: &str) -> String {
    json!({
        "code": code,
        "retry": retry,
        "reason": "the reason",
        "ids": {"operation": "op-token"}
    })
    .to_string()
}

#[tokio::test]
async fn every_error_kind_maps_from_its_status_and_body() {
    for kind in ErrorKind::ALL {
        let status = match kind {
            ErrorKind::AgentGone => 404,
            ErrorKind::OutcomeUnknown => 500,
            ErrorKind::MailboxFull => 429,
            ErrorKind::BudgetExceeded => 413,
            ErrorKind::StoreUnavailable => 503,
            ErrorKind::UnsupportedFormat => 422,
            _ => 409,
        };
        let server = serve(vec![(status, error_body(kind.code(), kind.retry().code()))]).await;
        let client = AgentsClient::new(&server.url, Some("tok".into())).expect("client");
        let error = client.operation("op-token").await.expect_err("refused");
        match error {
            ClientError::Agent {
                status: got,
                kind: k,
                retry,
                reason,
                ids,
            } => {
                assert_eq!(got, status);
                assert_eq!(k, kind);
                assert_eq!(retry, kind.retry());
                assert_eq!(reason, "the reason");
                assert_eq!(ids.operation.as_deref(), Some("op-token"));
            }
            other => panic!("{kind:?}: expected a typed agents error, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_retry_hint_is_the_bodys_not_the_kinds_default() {
    let server = serve(vec![(500, error_body("outcome_unknown", "never"))]).await;
    let client = AgentsClient::new(&server.url, None).expect("client");
    let error = client.cancel("op").await.expect_err("refused");
    assert_eq!(error.kind(), Some(ErrorKind::OutcomeUnknown));
    assert_eq!(error.retry(), Some(RetryHint::Never));
}

#[tokio::test]
async fn outcome_unknown_is_returned_after_one_request_with_no_retry() {
    // A second answer is queued; a client that retried would consume it and
    // succeed.
    let server = serve(vec![
        (500, error_body("outcome_unknown", "resolve_operation")),
        (
            200,
            json!({"operation": "op", "accept_seq": 1, "duplicate": false}).to_string(),
        ),
    ])
    .await;
    let client = AgentsClient::new(&server.url, None).expect("client");
    let error = client
        .send(&SendRequest {
            agent: agent(),
            handler: "chat".into(),
            request_id: "r1".into(),
            payload: json!({}),
        })
        .await
        .expect_err("refused");
    assert_eq!(error.retry(), Some(RetryHint::ResolveOperation));
    assert_eq!(server.seen().len(), 1);
}

#[tokio::test]
async fn unauthorized_and_forbidden_map_from_the_plain_shape() {
    let server = serve(vec![
        (
            401,
            json!({"status": "error", "errorType": "unauthorized", "error": "authentication required"})
                .to_string(),
        ),
        (
            403,
            json!({"status": "error", "errorType": "forbidden", "error": "this route requires the agents_admin capability"})
                .to_string(),
        ),
    ])
    .await;
    let client = AgentsClient::new(&server.url, Some("tok".into())).expect("client");
    let request = ReconcileRequest {
        effect: "eff".into(),
        attempt: 1,
        decision: ReconcileDecision::Retry,
    };
    match client.reconcile(&request).await.expect_err("401") {
        ClientError::Unauthorized(message) => assert_eq!(message, "authentication required"),
        other => panic!("expected Unauthorized, got {other:?}"),
    }
    match client.reconcile(&request).await.expect_err("403") {
        ClientError::Forbidden(message) => {
            assert_eq!(message, "this route requires the agents_admin capability")
        }
        other => panic!("expected Forbidden, got {other:?}"),
    }
    let seen = server.seen();
    assert_eq!(seen[0].authorization.as_deref(), Some("Bearer tok"));
    assert_eq!(seen[0].json()["decision"], "retry");
}

#[tokio::test]
async fn an_unknown_error_shape_keeps_its_status_and_body() {
    let server = serve(vec![(400, "not json".into())]).await;
    let client = AgentsClient::new(&server.url, None).expect("client");
    match client.operation("op").await.expect_err("refused") {
        ClientError::UnexpectedStatus { status, body } => {
            assert_eq!(status, 400);
            assert_eq!(body, "not json");
        }
        other => panic!("expected UnexpectedStatus, got {other:?}"),
    }
}

#[tokio::test]
async fn long_polls_are_clamped_to_twenty_seconds() {
    let empty = json!({"operation": "op", "result": null}).to_string();
    let call = json!({"operation": "op", "status": "running", "result": null}).to_string();
    let server = serve(vec![(200, empty.clone()), (200, empty), (200, call)]).await;
    let client = AgentsClient::new(&server.url, None).expect("client");
    let polled = client.result("op", 60_000).await.expect("poll");
    assert_eq!(polled.result, None);
    client.result("op", 1_500).await.expect("poll");
    let called = client
        .call(&CallRequest {
            agent: agent(),
            handler: "chat".into(),
            request_id: "r1".into(),
            payload: json!({"text": "hi"}),
            wait_ms: 45_000,
        })
        .await
        .expect("call");
    assert_eq!(called.status, OperationStatus::Running);
    let seen = server.seen();
    assert_eq!(seen[0].target, "/api/v1/agents/result?operation=op&wait_ms=20000");
    assert_eq!(seen[1].target, "/api/v1/agents/result?operation=op&wait_ms=1500");
    assert_eq!(seen[2].method, "POST");
    assert_eq!(seen[2].json()["wait_ms"], 20_000);
}

#[tokio::test]
async fn event_cursor_resumes_from_the_last_page_and_holds_on_an_empty_one() {
    let page = |seqs: &[u64], next: &str| {
        json!({
            "events": seqs.iter().map(|s| json!({
                "event_seq": s, "accept_seq": 1, "turn_id": "t",
                "event_type": "progress", "payload": null
            })).collect::<Vec<_>>(),
            "next_cursor": next,
        })
        .to_string()
    };
    let server = serve(vec![
        (200, page(&[1, 2], "2")),
        (200, page(&[], "2")),
        (200, page(&[3], "3")),
    ])
    .await;
    let client = AgentsClient::new(&server.url, None).expect("client");
    let mut cursor = EventCursor::new(agent(), None, 5_000);
    assert_eq!(cursor.next_page(&client).await.expect("p1").events.len(), 2);
    assert_eq!(cursor.position(), Some("2"));
    assert!(cursor.next_page(&client).await.expect("p2").events.is_empty());
    assert_eq!(cursor.position(), Some("2"));
    cursor.next_page(&client).await.expect("p3");
    assert_eq!(cursor.position(), Some("3"));

    let seen = server.seen();
    assert_eq!(seen[0].json()["cursor"], Value::Null);
    assert_eq!(seen[0].json()["limit"], MAX_EVENTS_PAGE);
    assert_eq!(seen[1].json()["cursor"], "2");
    assert_eq!(seen[2].json()["cursor"], "2");
    assert_eq!(seen[2].json()["agent"]["key"], "user 1/a&b");
}

#[tokio::test]
async fn an_events_limit_of_zero_is_raised_to_one() {
    let server = serve(vec![(
        200,
        json!({"events": [], "next_cursor": "0"}).to_string(),
    )])
    .await;
    let client = AgentsClient::new(&server.url, None).expect("client");
    client
        .events(&EventsRequest {
            agent: agent(),
            cursor: None,
            limit: 0,
        })
        .await
        .expect("page");
    assert_eq!(server.seen()[0].json()["limit"], 1);
}

#[tokio::test]
async fn query_values_and_path_segments_are_percent_encoded() {
    let effects = json!({"effects": []}).to_string();
    let inspect = json!({
        "effect": "e/1", "turn_id": "t", "tool_name": "x", "tool_version": "1",
        "policy": "manual", "status": "in_doubt", "attempt": 1,
        "step_key": "s", "result_attempt": null, "result": null,
        "attempts": [], "evidence": []
    })
    .to_string();
    let server = serve(vec![(200, effects), (200, inspect)]).await;
    let client = AgentsClient::new(&format!("{}/ignored/path?x=1", server.url), None)
        .expect("client");
    client.effects(&agent()).await.expect("effects");
    client.effect("e/1").await.expect("inspect");
    let seen = server.seen();
    assert_eq!(
        seen[0].target,
        "/api/v1/agents/effects?kind=assistant&key=user+1%2Fa%26b"
    );
    assert_eq!(seen[1].target, "/api/v1/agents/effects/e%2F1");
    assert_eq!(seen[1].authorization, None);
}

#[tokio::test]
async fn a_success_body_of_the_wrong_shape_is_a_decode_error() {
    let server = serve(vec![(200, json!({"nope": 1}).to_string())]).await;
    let client = AgentsClient::new(&server.url, None).expect("client");
    assert!(matches!(
        client.state(&agent()).await,
        Err(ClientError::Decode(_))
    ));
}

#[test]
fn a_base_url_that_cannot_address_routes_is_refused() {
    for bad in ["not a url", "mailto:a@b", "ftp://host"] {
        assert!(
            matches!(AgentsClient::new(bad, None), Err(ClientError::InvalidUrl(_))),
            "{bad}"
        );
    }
}
