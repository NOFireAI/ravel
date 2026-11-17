//! Issue #4323 (ADR-4314 decision 5): the operator copies the state of the
//! HorizontalPodAutoscalers it owns into the RavelCluster, `status.hpaReplicas`
//! with each tier's `desiredReplicas` and `currentReplicas`, and the
//! `AutoscalingDegraded` condition while an owned HPA reports
//! `ScalingActive=False` or `AbleToScale=False`. An HPA it does not own is
//! never read for either.
//!
//! Each test runs one reconcile pass against a loopback fake apiserver that
//! records every request, and asserts on the final status PATCH the operator
//! sent, under the field names docs/guides/kubernetes.md ("Autoscaling",
//! "Status") tells a reader to look at.

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
const CLUSTER_UID: &str = "00000000-0000-0000-0000-000000004323";
const CREDENTIALS_RV: &str = "cred-1";
/// Base64 of 64 `0` characters: a well-formed audit-token key and maintain
/// key, so neither the query nor the maintain tier is held back.
const KEY_B64: &str =
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMA==";

/// One fake cluster: the gateway spec, the stored status overrides, and the
/// HPAs the namespace list returns. The gateway Deployment is live, its
/// replicas already handed over (no handover this pass).
struct Scenario {
    gateway: Value,
    status: Value,
    hpas: Vec<Value>,
}

fn spec(scenario: &Scenario) -> Value {
    json!({
        "image": "ravel-server:test",
        "shards": 1,
        "storage": { "s3": {
            "bucket": "ravel",
            "region": "us-east-2",
            "credentialsSecretRef": { "name": "ravel-s3" },
        }},
        "auditTokenKeySecretRef": { "name": "audit-key" },
        "gateway": scenario.gateway,
        "maintain": { "enabled": true, "keySecretRef": { "name": "maintain-keys" } },
    })
}

fn cluster(scenario: &Scenario) -> Value {
    let spec = spec(scenario);
    let typed: RavelClusterSpec = serde_json::from_value(spec.clone()).expect("spec parses");
    let mut status = json!({
        "storeQualifiedHash": qualify_job_input_hash(&typed, Some(CREDENTIALS_RV)),
    });
    if let (Some(status), Some(overrides)) = (status.as_object_mut(), scenario.status.as_object()) {
        for (key, value) in overrides {
            status.insert(key.clone(), value.clone());
        }
    }
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": CLUSTER_UID,
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": spec,
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

/// The live gateway Deployment at `replicas`, with `spec.replicas` owned by
/// the HPA's scale updates rather than `ravel-operator`.
fn live_gateway(replicas: i32) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{INSTANCE}-gateway"),
            "namespace": NAMESPACE,
            "resourceVersion": "7",
            "generation": 1,
            "managedFields": [
                {
                    "manager": "ravel-operator",
                    "operation": "Apply",
                    "apiVersion": "apps/v1",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": { "f:spec": { "f:template": {} } },
                },
                {
                    "manager": "kube-controller-manager",
                    "operation": "Update",
                    "apiVersion": "apps/v1",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": { "f:spec": { "f:replicas": {} } },
                    "subresource": "scale",
                },
            ],
        },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": { "app.kubernetes.io/component": "gateway" } },
            "template": {
                "metadata": { "labels": { "app.kubernetes.io/component": "gateway" } },
                "spec": { "containers": [{ "name": "ravel", "image": "ravel-server:test" }] },
            },
        },
        "status": {
            "observedGeneration": 1,
            "replicas": replicas,
            "updatedReplicas": replicas,
            "readyReplicas": replicas,
            "availableReplicas": replicas,
        },
    })
}

fn hpa_condition(kind: &str, status: &str, reason: &str, message: &str) -> Value {
    json!({
        "type": kind,
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": "2026-11-17T00:00:00Z",
    })
}

/// The HPA status a healthy HPA pinned at `maxReplicas` reports: it can
/// scale and has metrics, and `ScalingLimited=True` says it is at its bound.
fn at_max_status(replicas: i32) -> Value {
    json!({
        "desiredReplicas": replicas,
        "currentReplicas": replicas,
        "conditions": [
            hpa_condition("AbleToScale", "True", "ReadyForNewScale", "recommended size matches current size"),
            hpa_condition("ScalingActive", "True", "ValidMetricFound", "the HPA was able to compute the replica count"),
            hpa_condition("ScalingLimited", "True", "TooManyReplicas", "the desired replica count is more than the maximum replica count"),
        ],
    })
}

/// What an HPA reports without metrics-server: `ScalingActive=False` with
/// reason `FailedGetResourceMetric`.
fn no_metrics_status(desired: i32, current: i32) -> Value {
    json!({
        "desiredReplicas": desired,
        "currentReplicas": current,
        "conditions": [
            hpa_condition("AbleToScale", "True", "SucceededGetScale", "the HPA controller was able to get the target's current scale"),
            hpa_condition("ScalingActive", "False", "FailedGetResourceMetric",
                "the HPA was unable to compute the replica count: failed to get cpu utilization"),
        ],
    })
}

/// An HPA at `<instance>-gateway` with `status`. `owner_uid` is the uid its
/// ownerReference names (`None`: no ownerReference); `labelled` gives it the
/// operator's labels for this instance.
fn gateway_hpa(labelled: bool, owner_uid: Option<&str>, status: Value) -> Value {
    let mut hpa = json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": format!("{INSTANCE}-gateway"),
            "namespace": NAMESPACE,
            "uid": "hpa-uid-gateway",
        },
        "spec": {
            "scaleTargetRef": { "apiVersion": "apps/v1", "kind": "Deployment", "name": format!("{INSTANCE}-gateway") },
            "maxReplicas": 6,
        },
        "status": status,
    });
    if labelled {
        hpa["metadata"]["labels"] = json!({
            "app.kubernetes.io/name": "ravel",
            "app.kubernetes.io/instance": INSTANCE,
            "app.kubernetes.io/component": "gateway",
            "app.kubernetes.io/managed-by": "ravel-operator",
        });
    }
    if let Some(uid) = owner_uid {
        hpa["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "ravel.nofire.ai/v1alpha1",
            "kind": "RavelCluster",
            "name": INSTANCE,
            "uid": uid,
            "controller": true,
        }]);
    }
    hpa
}

/// The operator's own gateway HPA: its labels and this cluster's uid.
fn owned_gateway_hpa(status: Value) -> Value {
    gateway_hpa(true, Some(CLUSTER_UID), status)
}

fn list(kind: &str, api_version: &str, items: &[Value]) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": { "resourceVersion": "1" },
        "items": items,
    })
}

fn failure(code: u16, reason: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Failure",
        "reason": reason,
        "code": code,
    })
}

#[derive(Clone, Debug)]
struct Call {
    method: String,
    path: String,
    body: Value,
}

struct Fake {
    scenario: Scenario,
    calls: Mutex<Vec<Call>>,
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

fn status_path() -> String {
    format!("/apis/ravel.nofire.ai/v1alpha1/namespaces/{NAMESPACE}/ravelclusters/{INSTANCE}/status")
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
        .is_some_and(|q| q.split('&').any(|pair| pair == "watch=true"));
    let body = req
        .into_body()
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .unwrap_or_default();
    if method == Method::GET && is_watch {
        std::future::pending::<()>().await;
    }
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    fake.calls.lock().expect("lock").push(Call {
        method: method.to_string(),
        path: path.clone(),
        body: body.clone(),
    });

    let secrets = format!("/api/v1/namespaces/{NAMESPACE}/secrets/");
    let response = match (&method, path.as_str()) {
        (&Method::GET, "/apis/ravel.nofire.ai/v1alpha1/ravelclusters") => json_response(
            StatusCode::OK,
            &list(
                "RavelClusterList",
                "ravel.nofire.ai/v1alpha1",
                &[cluster(&fake.scenario)],
            ),
        ),
        (&Method::GET, "/apis/apps/v1/deployments") => {
            json_response(StatusCode::OK, &list("DeploymentList", "apps/v1", &[]))
        }
        (&Method::GET, "/api/v1/services") => {
            json_response(StatusCode::OK, &list("ServiceList", "v1", &[]))
        }
        (&Method::GET, "/apis/networking.k8s.io/v1/ingresses") => json_response(
            StatusCode::OK,
            &list("IngressList", "networking.k8s.io/v1", &[]),
        ),
        // The owned-HPA watch's initial list.
        (&Method::GET, "/apis/autoscaling/v2/horizontalpodautoscalers") => json_response(
            StatusCode::OK,
            &list("HorizontalPodAutoscalerList", "autoscaling/v2", &[]),
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
        (&Method::GET, p)
            if p == format!(
                "/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{INSTANCE}-gateway"
            ) =>
        {
            json_response(StatusCode::OK, &live_gateway(3))
        }
        (&Method::GET, p)
            if p == format!(
                "/apis/autoscaling/v2/namespaces/{NAMESPACE}/horizontalpodautoscalers"
            ) =>
        {
            json_response(
                StatusCode::OK,
                &list(
                    "HorizontalPodAutoscalerList",
                    "autoscaling/v2",
                    &fake.scenario.hpas,
                ),
            )
        }
        (&Method::PATCH, p) if p == status_path() => {
            json_response(StatusCode::OK, &cluster(&fake.scenario))
        }
        // Every server-side apply echoes the applied object back.
        (&Method::PATCH, _) => json_response(StatusCode::OK, &body),
        _ => json_response(StatusCode::NOT_FOUND, &failure(404, "NotFound")),
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

fn is_final_status(call: &Call) -> bool {
    call.method == "PATCH"
        && call.path == status_path()
        && call.body["status"]["conditions"].is_array()
}

/// Run the operator against the fake until its first status write that
/// carries conditions, and return that write's `status`.
async fn final_status(scenario: Scenario) -> Value {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = Arc::new(Fake {
        scenario,
        calls: Mutex::default(),
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
        let calls = fake.calls.lock().expect("lock").clone();
        if let Some(call) = calls.iter().find(|c| is_final_status(c)) {
            status = Some(call.body["status"].clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    operator.abort();
    status.expect("the pass wrote a final status")
}

fn condition<'a>(status: &'a Value, kind: &str) -> Option<&'a Value> {
    status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == kind)
}

fn managed_gateway() -> Value {
    json!({
        "replicas": 3,
        "resources": { "requests": { "cpu": "500m", "memory": "512Mi" } },
        "autoscaling": { "mode": "Managed", "minReplicas": 2, "maxReplicas": 6,
            "targetCPUUtilizationPercentage": 70 },
    })
}

fn external_gateway() -> Value {
    json!({
        "replicas": 3,
        "autoscaling": { "mode": "External", "minReplicas": 2, "maxReplicas": 6 },
    })
}

/// The gateway is already autoscaled (its entry is stored), so the pass
/// renders its HPA and reads the listing's report.
fn autoscaled_status() -> Value {
    json!({ "autoscaledTiers": [{ "tier": "gateway", "declaredReplicas": 3 }] })
}

/// No metrics-server: the owned gateway HPA says `ScalingActive=False`,
/// reason `FailedGetResourceMetric`. The status names the tier, the HPA
/// condition, its reason and its message, and carries the HPA's desired and
/// current replicas. Fails against an implementation that looks for
/// `ScalingLimited` instead.
#[tokio::test]
async fn hpa_without_metrics_sets_autoscaling_degraded() {
    let status = final_status(Scenario {
        gateway: managed_gateway(),
        status: autoscaled_status(),
        hpas: vec![owned_gateway_hpa(no_metrics_status(4, 3))],
    })
    .await;

    let degraded = condition(&status, "AutoscalingDegraded").expect("AutoscalingDegraded");
    assert_eq!(degraded["status"], "True", "{status}");
    assert_eq!(degraded["reason"], "HorizontalPodAutoscalerNotScaling");
    let message = degraded["message"].as_str().expect("message");
    assert!(
        message.contains(
            "gateway: ScalingActive=False reason FailedGetResourceMetric: the HPA was \
             unable to compute the replica count: failed to get cpu utilization"
        ),
        "{message}"
    );
    assert_eq!(
        status["hpaReplicas"],
        json!([{ "tier": "gateway", "desiredReplicas": 4, "currentReplicas": 3 }]),
        "{status}"
    );
    // The HPA's own trouble is not a failed pass.
    assert!(condition(&status, "Degraded").is_none(), "{status}");
}

/// `AbleToScale=False` (the HPA cannot read or update the scale target) sets
/// the condition too.
#[tokio::test]
async fn able_to_scale_false_sets_autoscaling_degraded() {
    let status = final_status(Scenario {
        gateway: managed_gateway(),
        status: autoscaled_status(),
        hpas: vec![owned_gateway_hpa(json!({
            "desiredReplicas": 3,
            "currentReplicas": 3,
            "conditions": [hpa_condition("AbleToScale", "False", "FailedGetScale",
                "the HPA controller was unable to get the target's current scale")],
        }))],
    })
    .await;
    let degraded = condition(&status, "AutoscalingDegraded").expect("AutoscalingDegraded");
    let message = degraded["message"].as_str().expect("message");
    assert!(
        message.contains("gateway: AbleToScale=False reason FailedGetScale: the HPA controller"),
        "{message}"
    );
}

/// A healthy owned HPA at `maxReplicas` (`ScalingLimited=True`): replica
/// fields, no `AutoscalingDegraded`. With the condition stored from an earlier
/// pass, this pass removes it. Fails against an implementation that sets the
/// condition on `ScalingLimited`.
#[tokio::test]
async fn a_healthy_hpa_clears_autoscaling_degraded() {
    for stored in [
        json!([]),
        json!([{
            "type": "AutoscalingDegraded", "status": "True",
            "reason": "HorizontalPodAutoscalerNotScaling",
            "message": "gateway: ScalingActive=False reason FailedGetResourceMetric: earlier",
            "lastTransitionTime": "2026-11-16T00:00:00Z",
        }]),
    ] {
        let mut stored_status = autoscaled_status();
        stored_status["conditions"] = stored.clone();
        let status = final_status(Scenario {
            gateway: managed_gateway(),
            status: stored_status,
            hpas: vec![owned_gateway_hpa(at_max_status(6))],
        })
        .await;
        assert!(
            condition(&status, "AutoscalingDegraded").is_none(),
            "stored {stored}: {status}"
        );
        assert_eq!(
            status["hpaReplicas"],
            json!([{ "tier": "gateway", "desiredReplicas": 6, "currentReplicas": 6 }]),
            "{status}"
        );
    }
}

/// HPAs the operator does not own report `ScalingActive=False`: a user's HPA
/// scaling an `External` gateway, and, on a `Managed` gateway, a user HPA
/// that copies the operator's labels but names no ownerReference to this
/// cluster (the tier is withheld). Neither raises `AutoscalingDegraded` or
/// gets a `hpaReplicas` entry. Fails against an implementation that reads
/// every HPA in the namespace.
#[tokio::test]
async fn an_hpa_the_operator_does_not_own_is_not_reported() {
    for (case, gateway, hpa) in [
        (
            "External, user HPA",
            external_gateway(),
            gateway_hpa(false, None, no_metrics_status(4, 3)),
        ),
        (
            "Managed, labelled user HPA",
            managed_gateway(),
            gateway_hpa(true, None, no_metrics_status(4, 3)),
        ),
        (
            "Managed, another cluster's uid",
            managed_gateway(),
            gateway_hpa(
                true,
                Some("00000000-0000-0000-0000-00000000ffff"),
                no_metrics_status(4, 3),
            ),
        ),
    ] {
        let status = final_status(Scenario {
            gateway,
            status: autoscaled_status(),
            hpas: vec![hpa],
        })
        .await;
        assert!(
            condition(&status, "AutoscalingDegraded").is_none(),
            "{case}: {status}"
        );
        assert_eq!(status["hpaReplicas"], Value::Null, "{case}: {status}");
    }
}

/// A pass held for store qualification (the stored hash is for inputs other
/// than the current ones) lists no HPA, so it writes the stored
/// `AutoscalingDegraded` back as it was, its `observedGeneration` included,
/// beside the stored `hpaReplicas`. The owned HPA here is healthy, so a pass
/// that read it would clear the condition instead.
#[tokio::test]
async fn a_qualification_hold_keeps_the_stored_autoscaling_degraded() {
    let stored = json!({
        "type": "AutoscalingDegraded", "status": "True",
        "observedGeneration": 0,
        "reason": "HorizontalPodAutoscalerNotScaling",
        "message": "gateway: ScalingActive=False reason FailedGetResourceMetric: earlier",
        "lastTransitionTime": "2026-11-16T00:00:00Z",
    });
    let replicas = json!([{ "tier": "gateway", "desiredReplicas": 4, "currentReplicas": 3 }]);
    let mut stored_status = autoscaled_status();
    stored_status["storeQualifiedHash"] = json!("hash-of-the-inputs-before-an-edit");
    stored_status["hpaReplicas"] = replicas.clone();
    stored_status["conditions"] = json!([stored.clone()]);
    let status = final_status(Scenario {
        gateway: managed_gateway(),
        status: stored_status,
        hpas: vec![owned_gateway_hpa(at_max_status(6))],
    })
    .await;

    let qualified = condition(&status, "StoreQualified").expect("StoreQualified");
    assert_eq!(qualified["status"], "False", "the pass is held: {status}");
    let degraded = condition(&status, "AutoscalingDegraded").expect("AutoscalingDegraded");
    for field in [
        "status",
        "observedGeneration",
        "reason",
        "message",
        "lastTransitionTime",
    ] {
        assert_eq!(degraded[field], stored[field], "{field}: {status}");
    }
    assert_eq!(status["hpaReplicas"], replicas, "{status}");
}

/// No owned HPA at all: both surfaces are absent, the field as an explicit
/// `null` so the merge patch clears a stored value.
#[tokio::test]
async fn no_owned_hpa_clears_the_replica_fields() {
    let status = final_status(Scenario {
        gateway: json!({ "replicas": 3 }),
        status: json!({
            "hpaReplicas": [{ "tier": "gateway", "desiredReplicas": 4, "currentReplicas": 3 }],
        }),
        hpas: Vec::new(),
    })
    .await;
    assert!(
        status
            .as_object()
            .expect("status object")
            .contains_key("hpaReplicas"),
        "{status}"
    );
    assert_eq!(status["hpaReplicas"], Value::Null, "{status}");
    assert!(condition(&status, "AutoscalingDegraded").is_none());
}
