//! Issue #3200 (ADR-1170 amendment): with `spec.query.memoryBudgetBytes` set,
//! a query memory limit below the budget plus the 256 MiB `--mode query` holds
//! outside it, or no limit at all, must leave the query Deployment exactly as
//! it is: no apply (which would roll pods that cannot hold their budget) and no
//! delete. The gateway and maintain tiers keep reconciling, the status names
//! the field, and `queryReadyReplicas` still reports the live Deployment. A
//! limit above that floor but below the headroom rolls the Deployment with the
//! flag and reports the low headroom.
//!
//! One reconcile pass runs against a loopback fake apiserver that records
//! every request, so the assertions are on the calls the operator made.

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
use ravel_operator::crd::RavelClusterSpec;
use ravel_operator::health;
use ravel_operator::reconcile::qualify_job_input_hash;
use serde_json::{Value, json};
use tokio::net::TcpListener;

const NAMESPACE: &str = "default";
const INSTANCE: &str = "rc";
const CREDENTIALS_RV: &str = "cred-1";
/// Base64 of 64 `0` characters: a well-formed audit-token key and maintain
/// key, so neither the query nor the maintain tier is held back for a
/// missing one.
const KEY_B64: &str =
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMA==";
/// The pinned budget: 1.5 GiB.
const BUDGET: u64 = 1_610_612_736;

/// One fake cluster: the `RavelCluster` the operator lists, and whether a
/// query Deployment is already running.
struct Scenario {
    /// `spec.query.resources.limits.memory`, or `None` for no limit.
    limit: Option<&'static str>,
    /// A query Deployment is running, with one ready replica.
    query_exists: bool,
}

fn spec(limit: Option<&str>) -> Value {
    let mut query = json!({ "memoryBudgetBytes": BUDGET });
    if let Some(limit) = limit {
        query["resources"] = json!({ "limits": { "memory": limit } });
    }
    json!({
        "image": "ravel-server:test",
        "shards": 1,
        "storage": { "s3": {
            "bucket": "ravel",
            "region": "us-east-2",
            "credentialsSecretRef": { "name": "ravel-s3" },
        }},
        "auditTokenKeySecretRef": { "name": "audit-key" },
        "query": query,
        "maintain": { "enabled": true, "keySecretRef": { "name": "maintain-keys" } },
    })
}

fn cluster(scenario: &Scenario) -> Value {
    let spec = spec(scenario.limit);
    let typed: RavelClusterSpec = serde_json::from_value(spec.clone()).expect("spec parses");
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": "00000000-0000-0000-0000-000000003200",
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": spec,
        // Already qualified for these inputs, so the pass goes past the
        // qualification gate to the Deployments.
        "status": {
            "storeQualifiedHash": qualify_job_input_hash(&typed, Some(CREDENTIALS_RV)),
        },
    })
}

fn secret(name: &str, data: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": NAMESPACE, "resourceVersion": "cred-1" },
        "data": data,
    })
}

/// The query Deployment a previous pass left running, without the budget.
fn live_query() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{INSTANCE}-query"),
            "namespace": NAMESPACE,
            "resourceVersion": "7",
            "generation": 1,
        },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app.kubernetes.io/component": "query" } },
            "template": {
                "metadata": { "labels": { "app.kubernetes.io/component": "query" } },
                "spec": { "containers": [{ "name": "ravel", "image": "ravel-server:old" }] },
            },
        },
        "status": {
            "observedGeneration": 1,
            "replicas": 1,
            "updatedReplicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
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

/// The scenario served, every request in arrival order (`METHOD <path>`), the
/// body of every query Deployment apply, and the last status patch.
struct Fake {
    scenario: Scenario,
    requests: Mutex<Vec<String>>,
    query_applies: Mutex<Vec<Value>>,
    status: Mutex<Option<Value>>,
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

fn deployment_path(component: &str) -> String {
    format!("/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{INSTANCE}-{component}")
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
    fake.requests
        .lock()
        .expect("lock")
        .push(format!("{method} {path}"));

    let secrets = format!("/api/v1/namespaces/{NAMESPACE}/secrets/");
    let query = deployment_path("query");
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
                "items": [cluster(&fake.scenario)],
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
        (&Method::GET, p) if p == format!("{secrets}ravel-s3") => json_response(
            StatusCode::OK,
            &secret(
                "ravel-s3",
                json!({ "accessKeyId": "YQ==", "secretAccessKey": "Yg==" }),
            ),
        ),
        (&Method::GET, p) if p == format!("{secrets}audit-key") => json_response(
            StatusCode::OK,
            &secret("audit-key", json!({ "key": KEY_B64 })),
        ),
        (&Method::GET, p) if p == format!("{secrets}maintain-keys") => json_response(
            StatusCode::OK,
            &secret("maintain-keys", json!({ "keys": KEY_B64 })),
        ),
        (&Method::GET, p) if p == query && fake.scenario.query_exists => {
            json_response(StatusCode::OK, &live_query())
        }
        (&Method::PATCH, p) if p == status => {
            *fake.status.lock().expect("lock") = Some(applied());
            json_response(StatusCode::OK, &cluster(&fake.scenario))
        }
        (&Method::PATCH, p) if p == query => {
            fake.query_applies.lock().expect("lock").push(applied());
            json_response(StatusCode::OK, &applied())
        }
        // Every server-side apply echoes the applied object back.
        (&Method::PATCH, _) => json_response(StatusCode::OK, &applied()),
        // Nothing else exists: every other read and every delete is a 404.
        _ => json_response(StatusCode::NOT_FOUND, &not_found()),
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

/// What one pass did: every request, the body of each query Deployment
/// apply, and the status patch.
struct Pass {
    requests: Vec<String>,
    query_applies: Vec<Value>,
    status: Value,
}

/// Run the operator against the fake until its first status write, then stop
/// it.
async fn one_pass(scenario: Scenario) -> Pass {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let fake = Arc::new(Fake {
        scenario,
        requests: Mutex::default(),
        query_applies: Mutex::default(),
        status: Mutex::default(),
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
    let operator = tokio::spawn(controller::run_on(health_listener, client, None, None));

    let mut status = None;
    for _ in 0..200 {
        status = fake.status.lock().expect("lock").clone();
        if status.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    operator.abort();
    Pass {
        requests: fake.requests.lock().expect("lock").clone(),
        query_applies: fake.query_applies.lock().expect("lock").clone(),
        status: status.expect("the pass wrote a status"),
    }
}

/// The pass's `Degraded=True` condition has `reason`, and its message
/// contains every one of `names`.
fn assert_degraded(status: &Value, reason: &str, names: &[&str]) {
    let condition = status["status"]["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == "Degraded")
        .expect("a Degraded condition");
    assert_eq!(condition["status"], "True", "{condition}");
    assert_eq!(condition["reason"], reason, "{condition}");
    let message = condition["message"].as_str().expect("a message");
    for name in names {
        assert!(message.contains(name), "{name}: {condition}");
    }
}

/// No write of any kind reached the query Deployment, while the gateway and
/// maintain Deployments were applied.
fn assert_only_query_withheld(requests: &[String]) {
    let query = deployment_path("query");
    for method in ["PATCH", "DELETE", "PUT", "POST"] {
        let call = format!("{method} {query}");
        assert!(
            !requests.contains(&call),
            "{call} was sent for a refused query memory budget: {requests:?}"
        );
    }
    for component in ["gateway", "maintain"] {
        let call = format!("PATCH {}", deployment_path(component));
        assert!(
            requests.contains(&call),
            "{call} missing: the {component} tier must keep reconciling: {requests:?}"
        );
    }
}

/// A running query Deployment and a limit below budget plus held memory: the
/// Deployment is neither applied nor deleted, its live readiness is still
/// reported, and the status names the field and the figures.
#[tokio::test]
async fn refused_budget_leaves_the_running_query_deployment_alone() {
    let pass = one_pass(Scenario {
        limit: Some("1Gi"),
        query_exists: true,
    })
    .await;
    assert_only_query_withheld(&pass.requests);
    assert_degraded(
        &pass.status,
        "QueryMemoryBudgetExceedsLimit",
        &[
            "spec.query.memoryBudgetBytes",
            "1610612736",
            "268435456",
            "1073741824",
        ],
    );
    assert_eq!(
        pass.status["status"]["queryReadyReplicas"],
        json!(1),
        "the untouched query Deployment's live readiness is reported: {}",
        pass.status
    );
}

/// A fresh cluster whose query container has no memory limit gets no query
/// Deployment at all.
#[tokio::test]
async fn refused_budget_creates_no_query_deployment_on_a_fresh_cluster() {
    let pass = one_pass(Scenario {
        limit: None,
        query_exists: false,
    })
    .await;
    assert_only_query_withheld(&pass.requests);
    assert_degraded(
        &pass.status,
        "QueryMemoryBudgetExceedsLimit",
        &["spec.query.memoryBudgetBytes", "no memory limit"],
    );
}

/// The stg-1 values: 3.5Gi clears the floor but not the default headroom. The
/// query Deployment is applied with the flag, and the status reports the three
/// figures and the override field.
#[tokio::test]
async fn low_headroom_rolls_the_query_deployment_with_the_flag() {
    let pass = one_pass(Scenario {
        limit: Some("3.5Gi"),
        query_exists: true,
    })
    .await;
    let [apply] = pass.query_applies.as_slice() else {
        panic!("one query Deployment apply expected: {:?}", pass.requests);
    };
    let args = apply["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array()
        .expect("container args");
    let at = args
        .iter()
        .position(|a| a == "--memory-budget-bytes")
        .expect("the flag renders");
    assert_eq!(args[at + 1], json!(BUDGET.to_string()));
    assert_degraded(
        &pass.status,
        "QueryMemoryHeadroomLow",
        &[
            "3758096384",
            "1879048192",
            "2182380259",
            "spec.query.memoryHeadroomBytes",
        ],
    );
}
