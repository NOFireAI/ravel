//! ADR-1731's integration test: boot the listener `main` wires
//! (`controller::run` binds it and hands it to `controller::run_on`), drive
//! one reconcile through a kube `Client` pointed at a fake apiserver, and
//! assert `/healthz` answers 200 and
//! `ravel_operator_reconciles_total{result="ok"}` rises by exactly one.
//!
//! The fake apiserver is a loopback hyper server answering the handful of
//! requests one fresh-cluster reconcile makes: the watch lists, the shared
//! credentials Secret, a 404 for the qualify Job, the Job's server-side
//! apply, and the status patch. That pass holds for qualification and
//! returns `Ok`, which is the success the counter records.

#![allow(clippy::expect_used)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use kube::{Client, Config};
use ravel_operator::controller;
use ravel_operator::health;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const NAMESPACE: &str = "default";
const INSTANCE: &str = "probe";
const CREDENTIALS_SECRET: &str = "s3-credentials";

fn cluster() -> Value {
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": "00000000-0000-0000-0000-000000000001",
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": {
            "image": "ravel-server:test",
            "shards": 1,
            "storage": { "s3": {
                "bucket": "ravel",
                "region": "us-east-1",
                "endpoint": "https://s3.example.test",
                "credentialsSecretRef": { "name": CREDENTIALS_SECRET },
            }},
        },
    })
}

fn empty_list(kind: &str, api_version: &str) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": { "resourceVersion": "1" },
        "items": [],
    })
}

/// State the test controls: whether the `RavelCluster` list may be answered
/// yet (so the baseline scrape runs before any reconcile), and every request
/// the fake did not expect.
struct Fake {
    release_list: watch::Receiver<bool>,
    unexpected: Mutex<Vec<String>>,
}

fn json_response(status: StatusCode, body: &Value) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        "content-type",
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}

async fn apiserver(
    fake: Arc<Fake>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let is_watch = req
        .uri()
        .query()
        .is_some_and(|query| query.split('&').any(|pair| pair == "watch=true"));
    let body = req
        .into_body()
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .unwrap_or_default();

    // A watch that never delivers an event: the initial lists are all the
    // controller needs, and a stream that stays open adds no reconcile.
    if method == Method::GET && is_watch {
        std::future::pending::<()>().await;
    }

    let clusters = "/apis/ravel.nofire.ai/v1alpha1/ravelclusters";
    let secret = format!("/api/v1/namespaces/{NAMESPACE}/secrets/{CREDENTIALS_SECRET}");
    let job = format!("/apis/batch/v1/namespaces/{NAMESPACE}/jobs/{INSTANCE}-qualify");
    let status = format!(
        "/apis/ravel.nofire.ai/v1alpha1/namespaces/{NAMESPACE}/ravelclusters/{INSTANCE}/status"
    );

    let response = match (&method, path.as_str()) {
        (&Method::GET, p) if p == clusters => {
            let mut release = fake.release_list.clone();
            // The sender lives for the whole test; an error only means it
            // already finished, when answering straight away is fine.
            let _ = release.wait_for(|released| *released).await;
            json_response(
                StatusCode::OK,
                &json!({
                    "apiVersion": "ravel.nofire.ai/v1alpha1",
                    "kind": "RavelClusterList",
                    "metadata": { "resourceVersion": "1" },
                    "items": [cluster()],
                }),
            )
        }
        (&Method::GET, "/apis/apps/v1/deployments") => {
            json_response(StatusCode::OK, &empty_list("DeploymentList", "apps/v1"))
        }
        (&Method::GET, "/api/v1/services") => {
            json_response(StatusCode::OK, &empty_list("ServiceList", "v1"))
        }
        (&Method::GET, "/apis/networking.k8s.io/v1/ingresses") => json_response(
            StatusCode::OK,
            &empty_list("IngressList", "networking.k8s.io/v1"),
        ),
        (&Method::GET, p) if p == secret => json_response(
            StatusCode::OK,
            &json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": CREDENTIALS_SECRET,
                    "namespace": NAMESPACE,
                    "resourceVersion": "7",
                },
            }),
        ),
        (&Method::GET, p) if p == job => json_response(
            StatusCode::NOT_FOUND,
            &json!({
                "apiVersion": "v1",
                "kind": "Status",
                "metadata": {},
                "status": "Failure",
                "reason": "NotFound",
                "message": "jobs.batch not found",
                "code": 404,
            }),
        ),
        // Server-side apply: the applied object is the request body.
        (&Method::PATCH, p) if p == job => {
            let applied: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            json_response(StatusCode::OK, &applied)
        }
        (&Method::PATCH, p) if p == status => json_response(StatusCode::OK, &cluster()),
        _ => {
            if let Ok(mut unexpected) = fake.unexpected.lock() {
                unexpected.push(format!("{method} {path}"));
            }
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "metadata": {},
                    "status": "Failure",
                    "message": "unexpected request",
                    "code": 500,
                }),
            )
        }
    };
    Ok(response)
}

async fn serve_fake(listener: TcpListener, fake: Arc<Fake>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let fake = Arc::clone(&fake);
        tokio::spawn(async move {
            let service = service_fn(move |req| apiserver(Arc::clone(&fake), req));
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

/// One HTTP/1.1 GET against the health listener; returns the status code and
/// the body.
async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("health listener accepts");
    let request = format!("GET {path} HTTP/1.1\r\nHost: operator\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("request writes");
    let mut raw = String::new();
    stream
        .read_to_string(&mut raw)
        .await
        .expect("response reads");
    let (head, body) = raw.split_once("\r\n\r\n").expect("response has a head");
    let code = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line carries a code");
    (code, body.to_string())
}

/// The value of the one sample named exactly `series` in a `/metrics` body.
fn sample(body: &str, series: &str) -> f64 {
    let values: Vec<f64> = body
        .lines()
        .filter_map(|line| line.strip_prefix(series))
        .filter_map(|rest| rest.strip_prefix(' '))
        .map(|value| value.trim().parse().expect("sample value parses"))
        .collect();
    assert_eq!(values.len(), 1, "exactly one `{series}` sample in:\n{body}");
    values[0]
}

const OK_SERIES: &str = "ravel_operator_reconciles_total{result=\"ok\"}";
const ERROR_SERIES: &str = "ravel_operator_reconciles_total{result=\"error\"}";

#[tokio::test]
async fn one_reconcile_raises_the_ok_counter_by_one_and_healthz_answers_200() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (release, release_list) = watch::channel(false);
    let fake = Arc::new(Fake {
        release_list,
        unexpected: Mutex::new(Vec::new()),
    });
    let fake_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake apiserver binds");
    let fake_addr = fake_listener.local_addr().expect("fake apiserver address");
    tokio::spawn(serve_fake(fake_listener, Arc::clone(&fake)));

    let client = Client::try_from(Config::new(
        format!("http://{fake_addr}")
            .parse()
            .expect("fake apiserver URI"),
    ))
    .expect("client builds");

    let health_listener = health::bind("127.0.0.1:0".parse().expect("valid address"))
        .await
        .expect("health listener binds");
    let health_addr = health_listener
        .local_addr()
        .expect("health listener address");
    let operator = tokio::spawn(controller::run_on(health_listener, client, None));

    // Before the RavelCluster list is released: alive, not ready, no reconcile.
    assert_eq!(get(health_addr, "/healthz").await.0, 200);
    assert_eq!(get(health_addr, "/readyz").await.0, 503);
    let (code, before) = get(health_addr, "/metrics").await;
    assert_eq!(code, 200);
    assert_eq!(sample(&before, OK_SERIES), 0.0);
    assert_eq!(sample(&before, ERROR_SERIES), 0.0);

    release.send(true).expect("the fake holds the receiver");

    let mut after = String::new();
    for _ in 0..200 {
        after = get(health_addr, "/metrics").await.1;
        if sample(&after, OK_SERIES) + sample(&after, ERROR_SERIES) > 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let unexpected = fake.unexpected.lock().expect("lock").clone();
    assert!(
        unexpected.is_empty(),
        "fake apiserver got unexpected requests: {unexpected:?}"
    );
    assert_eq!(
        sample(&after, OK_SERIES) - sample(&before, OK_SERIES),
        1.0,
        "one reconcile raises result=\"ok\" by exactly one:\n{after}"
    );
    assert_eq!(sample(&after, ERROR_SERIES), 0.0);

    assert_eq!(get(health_addr, "/healthz").await.0, 200);
    // Readiness flips on its own task once the reflector marks the store
    // ready, which can land just after the reconcile the list triggered.
    let mut ready = 0;
    for _ in 0..200 {
        ready = get(health_addr, "/readyz").await.0;
        if ready == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(ready, 200, "/readyz answers 200 once the list arrived");
    assert!(!operator.is_finished(), "the controller is still running");
    operator.abort();
}
