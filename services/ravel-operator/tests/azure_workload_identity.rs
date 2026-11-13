//! The first-deploy ordering under `storage.azure` (issue #4165, ADR-4158):
//! the operator-owned ServiceAccounts carrying each role's workload identity
//! client ID must exist before the qualify Job does, for the same reason as
//! under Pod Identity: a Job pod naming a ServiceAccount that does not exist
//! is refused at admission on every creation attempt.
//!
//! One reconcile of a fresh Azure cluster runs against a loopback fake
//! apiserver that records every write in order. The fake answers no Secret
//! read, so the pass also proves that no credential Secret is read, and the
//! operator runs with no credentials sidecar image, which Azure never needs.

#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
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
const INSTANCE: &str = "blob";
const CLIENT_ID: &str = "00000000-0000-0000-0000-00000000c1d0";
const GATEWAY_CLIENT_ID: &str = "00000000-0000-0000-0000-0000000000a1";

fn cluster() -> Value {
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": "00000000-0000-0000-0000-000000000003",
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": {
            "image": "ravel-server:test",
            "shards": 1,
            "storage": { "azure": {
                "account": "ravelacct",
                "container": "ravel-data",
                "clientId": CLIENT_ID,
                "roleClientIds": { "gateway": GATEWAY_CLIENT_ID },
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

/// Every write in arrival order (`PATCH <path>`), the applied ServiceAccounts
/// by path, the applied Job, the last status patch, and every request the
/// fake did not expect.
#[derive(Default)]
struct Fake {
    writes: Mutex<Vec<String>>,
    service_accounts: Mutex<BTreeMap<String, Value>>,
    job: Mutex<Option<Value>>,
    status: Mutex<Option<Value>>,
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
            if p.starts_with(&service_accounts) {
                fake.service_accounts
                    .lock()
                    .expect("lock")
                    .insert(path.clone(), applied());
            }
            if p == job {
                *fake.job.lock().expect("lock") = Some(applied());
            }
            if p == status {
                *fake.status.lock().expect("lock") = Some(applied());
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

fn status_path() -> String {
    format!(
        "PATCH /apis/ravel.nofire.ai/v1alpha1/namespaces/{NAMESPACE}/ravelclusters/{INSTANCE}/status"
    )
}

fn account_path(component: &str) -> String {
    format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{INSTANCE}-{component}")
}

/// Run the operator, with no credentials sidecar image, against the fake
/// until its first status write, then stop it.
async fn one_pass() -> (Arc<Fake>, Vec<String>) {
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
    let operator = tokio::spawn(controller::run_on(health_listener, client, None, None));

    let status_write = status_path();
    let mut writes = Vec::new();
    for _ in 0..200 {
        writes = fake.writes.lock().expect("lock").clone();
        if writes.contains(&status_write) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    operator.abort();
    (fake, writes)
}

#[tokio::test]
async fn annotated_service_accounts_are_applied_before_the_qualify_job() {
    let (fake, writes) = one_pass().await;

    let unexpected = fake.unexpected.lock().expect("lock").clone();
    assert!(
        unexpected.is_empty(),
        "fake apiserver got unexpected requests (a Secret read among them would mean the \
         Azure pass read a credential Secret): {unexpected:?}"
    );
    let job_write = format!("PATCH /apis/batch/v1/namespaces/{NAMESPACE}/jobs/{INSTANCE}-qualify");
    assert_eq!(
        writes,
        vec![
            format!("PATCH {}", account_path("gateway")),
            format!("PATCH {}", account_path("query")),
            format!("PATCH {}", account_path("maintain")),
            format!("PATCH {}", account_path("qualify")),
            job_write,
            status_path(),
        ],
        "the four ServiceAccounts are applied before the qualify Job is created"
    );

    let accounts = fake.service_accounts.lock().expect("lock").clone();
    for (component, client_id) in [
        ("gateway", GATEWAY_CLIENT_ID),
        ("query", CLIENT_ID),
        ("maintain", CLIENT_ID),
        ("qualify", CLIENT_ID),
    ] {
        let account = accounts
            .get(&account_path(component))
            .expect("the account was applied");
        assert_eq!(
            account["metadata"]["annotations"]["azure.workload.identity/client-id"],
            client_id,
            "{component}"
        );
    }

    let job = fake
        .job
        .lock()
        .expect("lock")
        .clone()
        .expect("the Job was applied");
    let template = &job["spec"]["template"];
    assert_eq!(
        template["metadata"]["labels"]["azure.workload.identity/use"],
        "true"
    );
    let pod = &template["spec"];
    assert_eq!(pod["serviceAccountName"], format!("{INSTANCE}-qualify"));
    assert_eq!(pod["initContainers"], Value::Null, "no credentials sidecar");
    assert_eq!(
        pod["containers"][0]["args"],
        json!(["--store", "azure", "store", "qualify"])
    );
    assert_eq!(
        pod["containers"][0]["env"],
        json!([
            { "name": "RAVEL_AZURE_ACCOUNT", "value": "ravelacct" },
            { "name": "RAVEL_AZURE_CONTAINER", "value": "ravel-data" },
        ])
    );

    let status = fake
        .status
        .lock()
        .expect("lock")
        .clone()
        .expect("the status was patched");
    assert_eq!(
        status["status"]["podIdentityServiceAccounts"],
        json!(true),
        "the gate's status write records the applied ServiceAccounts"
    );
    let conditions = status["status"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let protection = conditions
        .iter()
        .find(|c| c["type"] == "BucketProtectionVerified")
        .expect("the gate's status write carries BucketProtectionVerified");
    assert_eq!(protection["status"], "False");
    assert_eq!(protection["reason"], "NotImplementedForAzure");
}
