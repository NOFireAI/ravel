//! The first-deploy ordering under `storage.s3.auth: podIdentity` (ADR-0034,
//! Pod Identity storage credentials amendment): the operator-owned
//! ServiceAccounts must exist before the qualify Job does. The gate creates
//! the Job and returns, and a Job pod naming a ServiceAccount that does not
//! exist is refused at admission on every creation attempt, so a cluster
//! whose accounts were applied after the gate would never qualify.
//!
//! One reconcile of a fresh podIdentity cluster runs against a loopback fake
//! apiserver that records every write in order. The fake answers no Secret
//! read, so the pass also proves that no credential Secret is read.

#![allow(clippy::expect_used)]

use std::convert::Infallible;
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
use tokio::net::TcpListener;

const NAMESPACE: &str = "default";
const INSTANCE: &str = "keyless";
const SIDECAR_IMAGE: &str = "ghcr.io/nofireai/ravel-operator:0.25.0";

fn cluster() -> Value {
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": "00000000-0000-0000-0000-000000000002",
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": {
            "image": "ravel-server:test",
            "shards": 1,
            "storage": { "s3": {
                "bucket": "ravel",
                "region": "us-east-2",
                "auth": "podIdentity",
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

/// Every write in arrival order (`PATCH <path>`), the applied Job, and every
/// request the fake did not expect.
#[derive(Default)]
struct Fake {
    writes: Mutex<Vec<String>>,
    job: Mutex<Option<Value>>,
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

fn not_found() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Failure",
        "reason": "NotFound",
        "code": 404,
    })
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
    if method == Method::GET && is_watch {
        std::future::pending::<()>().await;
    }

    let service_accounts = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts/");
    let job = format!("/apis/batch/v1/namespaces/{NAMESPACE}/jobs/{INSTANCE}-qualify");
    let status = format!(
        "/apis/ravel.nofire.ai/v1alpha1/namespaces/{NAMESPACE}/ravelclusters/{INSTANCE}/status"
    );
    let applied = || serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);

    let response = match (&method, path.as_str()) {
        (&Method::GET, "/apis/ravel.nofire.ai/v1alpha1/ravelclusters") => json_response(
            StatusCode::OK,
            &json!({
                "apiVersion": "ravel.nofire.ai/v1alpha1",
                "kind": "RavelClusterList",
                "metadata": { "resourceVersion": "1" },
                "items": [cluster()],
            }),
        ),
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
        (&Method::GET, p) if p == job => json_response(StatusCode::NOT_FOUND, &not_found()),
        (&Method::PATCH, p) if p.starts_with(&service_accounts) || p == job || p == status => {
            fake.writes
                .lock()
                .expect("lock")
                .push(format!("PATCH {path}"));
            if p == job {
                *fake.job.lock().expect("lock") = Some(applied());
            }
            if p == status {
                json_response(StatusCode::OK, &cluster())
            } else {
                json_response(StatusCode::OK, &applied())
            }
        }
        _ => {
            fake.unexpected
                .lock()
                .expect("lock")
                .push(format!("{method} {path}"));
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

#[tokio::test]
async fn service_accounts_are_applied_before_the_qualify_job() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let fake = Arc::new(Fake::default());
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
    let operator = tokio::spawn(controller::run_on(
        health_listener,
        client,
        None,
        Some(SIDECAR_IMAGE.to_string()),
    ));

    let status_write = format!(
        "PATCH /apis/ravel.nofire.ai/v1alpha1/namespaces/{NAMESPACE}/ravelclusters/{INSTANCE}/status"
    );
    let mut writes = Vec::new();
    for _ in 0..200 {
        writes = fake.writes.lock().expect("lock").clone();
        if writes.contains(&status_write) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    operator.abort();

    let unexpected = fake.unexpected.lock().expect("lock").clone();
    assert!(
        unexpected.is_empty(),
        "fake apiserver got unexpected requests (a Secret read among them would mean the \
         podIdentity pass read a credential Secret): {unexpected:?}"
    );
    let account = |component: &str| {
        format!("PATCH /api/v1/namespaces/{NAMESPACE}/serviceaccounts/{INSTANCE}-{component}")
    };
    let job_write = format!("PATCH /apis/batch/v1/namespaces/{NAMESPACE}/jobs/{INSTANCE}-qualify");
    assert_eq!(
        writes,
        vec![
            account("gateway"),
            account("query"),
            account("maintain"),
            account("qualify"),
            job_write,
            status_write,
        ],
        "the four ServiceAccounts are applied before the qualify Job is created"
    );

    let job = fake
        .job
        .lock()
        .expect("lock")
        .clone()
        .expect("the Job was applied");
    let pod = &job["spec"]["template"]["spec"];
    assert_eq!(pod["serviceAccountName"], format!("{INSTANCE}-qualify"));
    assert_eq!(pod["initContainers"][0]["name"], "pod-identity-credentials");
    assert_eq!(pod["initContainers"][0]["image"], SIDECAR_IMAGE);
    assert_eq!(pod["initContainers"][0]["restartPolicy"], "Always");
}
