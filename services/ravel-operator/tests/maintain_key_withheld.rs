//! ADR-3130 decision 5: `ravel-server --mode maintain` refuses to start without
//! `--maintain-key-file`, and the operator renders that flag only from
//! `spec.maintain.keySecretRef`. A cluster with maintain enabled and no ref (or
//! a ref to a Secret that does not exist) must keep whatever maintain
//! Deployment it already runs, untouched: no apply (which would roll it onto
//! pods that cannot start) and no delete (which would stop maintenance
//! outright). On an existing cluster the gateway and query tiers keep
//! reconciling; on a fresh cluster with per-role credentials they wait for
//! maintain to bootstrap `sys/gc`, so nothing is created. Either way the status
//! names the missing field or Secret.
//!
//! One reconcile pass runs against a loopback fake apiserver that records
//! every request, so the assertions are on the calls the operator made.

#![allow(clippy::expect_used)]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

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
/// Base64 of 64 `0` characters: a well-formed audit-token key, so the query
/// tier is not held back for a missing one.
const AUDIT_KEY_B64: &str =
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMA==";
/// The per-role maintain credential Secret. Setting it puts the cluster under
/// per-role credentials, where gateway and query wait for maintain to create
/// `sys/gc`.
const MAINTAIN_CREDENTIALS: &str = "ravel-s3-maintain";
/// The Secret a `maintain.keySecretRef` names in the missing-Secret case. The
/// fake never serves it.
const MAINTAIN_KEYS: &str = "maintain-keys";

/// One fake cluster: the `RavelCluster` the operator lists, and the live state
/// it finds.
struct Scenario {
    /// The `RavelCluster` spec.
    spec: Value,
    /// A maintain Deployment from before ADR-3130 is running, with one ready
    /// replica.
    maintain_exists: bool,
    /// `status.gcBootstrapWaitingSince` the cluster already carries.
    waiting_since: Option<String>,
}

fn spec(per_role: bool, key_ref: Option<&str>) -> Value {
    let mut maintain = json!({ "enabled": true });
    if per_role {
        maintain["credentialsSecretRef"] = json!({ "name": MAINTAIN_CREDENTIALS });
    }
    if let Some(name) = key_ref {
        maintain["keySecretRef"] = json!({ "name": name });
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
        "maintain": maintain,
    })
}

fn cluster(scenario: &Scenario) -> Value {
    let typed: RavelClusterSpec =
        serde_json::from_value(scenario.spec.clone()).expect("spec parses");
    // Already qualified for these inputs, so the pass goes past the
    // qualification gate to the Deployments.
    let mut status = json!({
        "storeQualifiedHash": qualify_job_input_hash(&typed, Some(CREDENTIALS_RV)),
    });
    if let Some(since) = &scenario.waiting_since {
        status["gcBootstrapWaitingSince"] = json!(since);
    }
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": "00000000-0000-0000-0000-000000003130",
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": scenario.spec,
        "status": status,
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

/// The maintain Deployment a previous operator version left running.
fn live_maintain() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{INSTANCE}-maintain"),
            "namespace": NAMESPACE,
            "resourceVersion": "7",
        },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app.kubernetes.io/component": "maintain" } },
            "template": {
                "metadata": { "labels": { "app.kubernetes.io/component": "maintain" } },
                "spec": { "containers": [{ "name": "ravel", "image": "ravel-server:old" }] },
            },
        },
        "status": { "replicas": 1, "readyReplicas": 1 },
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

/// The scenario served, every request in arrival order (`METHOD <path>`), and
/// the last status patch.
struct Fake {
    scenario: Scenario,
    requests: Mutex<Vec<String>>,
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
    let maintain = format!("/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{INSTANCE}-maintain");
    let status = format!(
        "/apis/ravel.nofire.ai/v1alpha1/namespaces/{NAMESPACE}/ravelclusters/{INSTANCE}/status"
    );
    let applied = || serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let credentials = |name: &str| {
        secret(
            name,
            json!({ "accessKeyId": "YQ==", "secretAccessKey": "Yg==" }),
        )
    };

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
        (&Method::GET, p) if p == format!("{secrets}ravel-s3") => {
            json_response(StatusCode::OK, &credentials("ravel-s3"))
        }
        (&Method::GET, p) if p == format!("{secrets}{MAINTAIN_CREDENTIALS}") => {
            json_response(StatusCode::OK, &credentials(MAINTAIN_CREDENTIALS))
        }
        (&Method::GET, p) if p == format!("{secrets}audit-key") => json_response(
            StatusCode::OK,
            &secret("audit-key", json!({ "key": AUDIT_KEY_B64 })),
        ),
        (&Method::GET, p) if p == maintain && fake.scenario.maintain_exists => {
            json_response(StatusCode::OK, &live_maintain())
        }
        (&Method::PATCH, p) if p == status => {
            *fake.status.lock().expect("lock") = Some(applied());
            json_response(StatusCode::OK, &cluster(&fake.scenario))
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

fn deployment_path(component: &str) -> String {
    format!("/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{INSTANCE}-{component}")
}

/// Run the operator against the fake until its first status write, then stop
/// it. Returns every request and the status patch.
async fn one_pass(scenario: Scenario) -> (Vec<String>, Value) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let fake = Arc::new(Fake {
        scenario,
        requests: Mutex::default(),
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
    let requests = fake.requests.lock().expect("lock").clone();
    (requests, status.expect("the pass wrote a status"))
}

fn degraded(status: &Value) -> &Value {
    status["status"]["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == "Degraded")
        .expect("a Degraded condition")
}

/// No write of any kind reached the maintain Deployment.
fn assert_maintain_untouched(requests: &[String]) {
    let maintain = deployment_path("maintain");
    for method in ["PATCH", "DELETE", "PUT", "POST"] {
        let call = format!("{method} {maintain}");
        assert!(
            !requests.contains(&call),
            "{call} was sent for a withheld maintain tier: {requests:?}"
        );
    }
}

fn assert_request_serving_applied(requests: &[String]) {
    for component in ["gateway", "query"] {
        let call = format!("PATCH {}", deployment_path(component));
        assert!(
            requests.contains(&call),
            "{call} missing: the {component} tier must keep reconciling: {requests:?}"
        );
    }
}

/// The pass's `Degraded=True` condition has `reason` and a message containing
/// `names`.
fn assert_degraded(status: &Value, reason: &str, names: &str) {
    let condition = degraded(status);
    assert_eq!(condition["status"], "True", "{condition}");
    assert_eq!(condition["reason"], reason, "{condition}");
    assert!(
        condition["message"]
            .as_str()
            .is_some_and(|m| m.contains(names)),
        "{condition}"
    );
}

fn assert_withheld(requests: &[String], status: &Value) {
    assert_maintain_untouched(requests);
    assert_request_serving_applied(requests);
    assert_degraded(
        status,
        "MaintainKeySecretRefMissing",
        "spec.maintain.keySecretRef",
    );
}

/// An upgrade: a maintain Deployment from before ADR-3130 is running and the
/// CR has no `maintain.keySecretRef` yet. It is neither applied nor deleted,
/// and its live readiness is still reported.
#[tokio::test]
async fn existing_maintain_deployment_is_left_alone_without_the_key_ref() {
    let (requests, status) = one_pass(Scenario {
        spec: spec(false, None),
        maintain_exists: true,
        waiting_since: None,
    })
    .await;
    assert_withheld(&requests, &status);
    assert_eq!(
        status["status"]["maintainReadyReplicas"],
        json!(1),
        "the untouched maintain Deployment's live readiness is reported: {status}"
    );
}

/// A fresh cluster on one shared credential without `maintain.keySecretRef`
/// gets no maintain Deployment at all.
#[tokio::test]
async fn no_maintain_deployment_is_created_without_the_key_ref() {
    let (requests, status) = one_pass(Scenario {
        spec: spec(false, None),
        maintain_exists: false,
        waiting_since: None,
    })
    .await;
    assert_withheld(&requests, &status);
}

/// A fresh cluster on per-role credentials without `maintain.keySecretRef`:
/// gateway and query wait for maintain to bootstrap `sys/gc`, and maintain is
/// withheld, so no Deployment is created at all. The bootstrap wait started 6
/// minutes ago, past the stall threshold, and the reason still names the
/// missing field rather than `GcBootstrapStalled`, whose message points at a
/// maintain pod that does not exist.
#[tokio::test]
async fn fresh_per_role_cluster_creates_no_deployment_without_the_key_ref() {
    let six_minutes_ago = SystemTime::now() - Duration::from_secs(6 * 60);
    let (requests, status) = one_pass(Scenario {
        spec: spec(true, None),
        maintain_exists: false,
        waiting_since: Some(humantime::format_rfc3339_seconds(six_minutes_ago).to_string()),
    })
    .await;
    let deployments = format!("/apis/apps/v1/namespaces/{NAMESPACE}/deployments/");
    // A server-side apply is a PATCH; the DELETEs the pass sends are sweeps of
    // objects this spec does not render (the ingest router), which 404 here.
    let creates: Vec<&String> = requests
        .iter()
        .filter(|r| {
            r.contains(&deployments) && ["PATCH ", "PUT ", "POST "].iter().any(|m| r.starts_with(m))
        })
        .collect();
    assert!(
        creates.is_empty(),
        "no Deployment may be created on a fresh per-role cluster without the key: {creates:?}"
    );
    assert_maintain_untouched(&requests);
    assert_degraded(
        &status,
        "MaintainKeySecretRefMissing",
        "spec.maintain.keySecretRef",
    );
    assert_eq!(
        status["status"]["gcBootstrapWaitingSince"],
        json!(humantime::format_rfc3339_seconds(six_minutes_ago).to_string()),
        "the bootstrap wait is still tracked: {status}"
    );
}

/// A per-role cluster whose `maintain.keySecretRef` names a Secret that does
/// not exist, with a maintain Deployment from before ADR-3130 running: that
/// Deployment is left untouched, and because it reports a ready replica the
/// gateway and query tiers are still applied.
#[tokio::test]
async fn missing_key_secret_leaves_existing_maintain_alone_and_applies_the_rest() {
    let (requests, status) = one_pass(Scenario {
        spec: spec(true, Some(MAINTAIN_KEYS)),
        maintain_exists: true,
        waiting_since: None,
    })
    .await;
    assert_maintain_untouched(&requests);
    assert_request_serving_applied(&requests);
    assert_degraded(&status, "SecretNotFound", MAINTAIN_KEYS);
}
