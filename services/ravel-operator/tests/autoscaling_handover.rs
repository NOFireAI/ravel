//! Issue #4322 (ADR-4314): handing a tier's `spec.replicas` to a horizontal
//! scaler without the tier dropping to one replica, the `Managed` HPA, the
//! tiers where autoscaling is withheld, and turning autoscaling off without
//! losing the live count.
//!
//! Each test runs one reconcile pass against a loopback fake apiserver that
//! records every request (method, path, query and body) in arrival order, so
//! the assertions are on the calls the operator made and their order.

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
use ravel_operator::crd::RavelClusterSpec;
use ravel_operator::health;
use ravel_operator::reconcile::qualify_job_input_hash;
use serde_json::{Value, json};
use tokio::net::TcpListener;

const NAMESPACE: &str = "default";
const INSTANCE: &str = "rc";
const CLUSTER_UID: &str = "00000000-0000-0000-0000-000000004322";
const CREDENTIALS_RV: &str = "cred-1";
/// Base64 of 64 `0` characters: a well-formed audit-token key and maintain
/// key, so neither the query nor the maintain tier is held back.
const KEY_B64: &str =
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMA==";

/// One fake cluster.
struct Scenario {
    /// `spec.gateway`, `spec.query` and `spec.maintain` overrides, merged
    /// over a minimal spec.
    gateway: Value,
    query: Value,
    maintain: Value,
    /// `status.autoscaledTiers` and `status.replicaAdoptions` as stored.
    status: Value,
    /// Live Deployments by component.
    live: BTreeMap<&'static str, Value>,
    /// The HorizontalPodAutoscalers the fake apiserver lists for this instance.
    hpas: Vec<Value>,
    /// Every status PATCH answers 500.
    status_fails: bool,
    /// The handover apply (`fieldManager=ravel-operator-handover`) answers 409.
    handover_conflicts: bool,
    /// Every HorizontalPodAutoscaler DELETE answers 500.
    hpa_delete_fails: bool,
    /// The HorizontalPodAutoscaler list answers 500.
    hpa_list_fails: bool,
    /// Every listed HPA has since been deleted and recreated at the same name
    /// by someone else, with a new uid: a DELETE preconditioned on the listed
    /// uid answers 409, and an unconditioned one deletes the new HPA.
    hpa_recreated_after_list: bool,
    /// The RavelCluster's `metadata.generation`.
    generation: i64,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            gateway: json!({}),
            query: json!({}),
            maintain: json!({}),
            status: json!({}),
            live: BTreeMap::new(),
            hpas: Vec::new(),
            status_fails: false,
            handover_conflicts: false,
            hpa_delete_fails: false,
            hpa_list_fails: false,
            hpa_recreated_after_list: false,
            generation: 1,
        }
    }
}

fn spec(scenario: &Scenario) -> Value {
    let mut maintain = json!({ "enabled": true, "keySecretRef": { "name": "maintain-keys" } });
    merge(&mut maintain, &scenario.maintain);
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
        "query": scenario.query,
        "maintain": maintain,
    })
}

fn merge(base: &mut Value, overrides: &Value) {
    if let (Some(base), Some(overrides)) = (base.as_object_mut(), overrides.as_object()) {
        for (key, value) in overrides {
            base.insert(key.clone(), value.clone());
        }
    }
}

fn cluster(scenario: &Scenario) -> Value {
    let spec = spec(scenario);
    let typed: RavelClusterSpec = serde_json::from_value(spec.clone()).expect("spec parses");
    let mut status = json!({
        "storeQualifiedHash": qualify_job_input_hash(&typed, Some(CREDENTIALS_RV)),
    });
    merge(&mut status, &scenario.status);
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": CLUSTER_UID,
            "resourceVersion": "1",
            "generation": scenario.generation,
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

/// Who owns the live Deployment's `spec.replicas`.
#[derive(Clone, Copy)]
enum ReplicasOwner {
    /// `ravel-operator`, by apply.
    Operator,
    /// `kubectl`, by an update (`kubectl scale`); the operator still owns
    /// the rest of the spec.
    Kubectl,
}

/// A live Deployment for `component` at `replicas`.
fn live(component: &str, replicas: i32, owner: ReplicasOwner) -> Value {
    let operator_fields = match owner {
        ReplicasOwner::Operator => json!({ "f:spec": { "f:replicas": {}, "f:template": {} } }),
        ReplicasOwner::Kubectl => json!({ "f:spec": { "f:template": {} } }),
    };
    let mut managed = vec![json!({
        "manager": "ravel-operator",
        "operation": "Apply",
        "apiVersion": "apps/v1",
        "fieldsType": "FieldsV1",
        "fieldsV1": operator_fields,
    })];
    if matches!(owner, ReplicasOwner::Kubectl) {
        managed.push(json!({
            "manager": "kubectl",
            "operation": "Update",
            "apiVersion": "apps/v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": { "f:spec": { "f:replicas": {} } },
            "subresource": "scale",
        }));
    }
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{INSTANCE}-{component}"),
            "namespace": NAMESPACE,
            "resourceVersion": "7",
            "generation": 1,
            "managedFields": managed,
        },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": { "app.kubernetes.io/component": component } },
            "template": {
                "metadata": { "labels": { "app.kubernetes.io/component": component } },
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

/// A live Deployment the operator owns `spec.replicas` on (its managedFields
/// claim the field) but whose spec omits the count: the corrupt object item 5
/// errors on rather than handing over an invented one.
fn live_without_replicas(component: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{INSTANCE}-{component}"),
            "namespace": NAMESPACE,
            "resourceVersion": "7",
            "generation": 1,
            "managedFields": [{
                "manager": "ravel-operator",
                "operation": "Apply",
                "apiVersion": "apps/v1",
                "fieldsType": "FieldsV1",
                "fieldsV1": { "f:spec": { "f:replicas": {}, "f:template": {} } },
            }],
        },
        "spec": {
            "selector": { "matchLabels": { "app.kubernetes.io/component": component } },
            "template": {
                "metadata": { "labels": { "app.kubernetes.io/component": component } },
                "spec": { "containers": [{ "name": "ravel", "image": "ravel-server:test" }] },
            },
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

fn hpa_list(scenario: &Scenario) -> Value {
    json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscalerList",
        "metadata": { "resourceVersion": "1" },
        "items": scenario.hpas,
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

/// One request the operator sent.
#[derive(Clone, Debug)]
struct Call {
    method: String,
    path: String,
    /// The query string's `key=value` pairs.
    query: Vec<String>,
    body: Value,
}

impl Call {
    fn has_param(&self, pair: &str) -> bool {
        self.query.iter().any(|p| p == pair)
    }
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

fn deployment_path(component: &str) -> String {
    format!("/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{INSTANCE}-{component}")
}

fn hpa_path(component: &str) -> String {
    format!(
        "/apis/autoscaling/v2/namespaces/{NAMESPACE}/horizontalpodautoscalers/{INSTANCE}-{component}"
    )
}

fn hpa_list_path() -> String {
    format!("/apis/autoscaling/v2/namespaces/{NAMESPACE}/horizontalpodautoscalers")
}

/// An HPA carrying the operator's own label set for this instance and an
/// ownerReference to this RavelCluster, named after the tier Deployment, as
/// `desired_hpa` renders it.
fn owned_hpa(component: &str) -> Value {
    let mut hpa = labelled_user_hpa(component);
    hpa["metadata"]["uid"] = json!(listed_hpa_uid(component));
    hpa["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "name": INSTANCE,
        "uid": CLUSTER_UID,
        "controller": true,
    }]);
    hpa
}

/// The uid the HPA list reports for the operator's `component` HPA.
fn listed_hpa_uid(component: &str) -> String {
    format!("hpa-uid-{component}")
}

/// A user-created HPA at the colliding `<instance>-<component>` name that
/// copies the operator's labels but carries no ownerReference to this
/// RavelCluster: the labels alone do not make it the operator's.
fn labelled_user_hpa(component: &str) -> Value {
    json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": format!("{INSTANCE}-{component}"),
            "namespace": NAMESPACE,
            "labels": {
                "app.kubernetes.io/name": "ravel",
                "app.kubernetes.io/instance": INSTANCE,
                "app.kubernetes.io/component": component,
                "app.kubernetes.io/managed-by": "ravel-operator",
            },
        },
    })
}

/// A user-created HPA at the colliding `<instance>-<component>` name, carrying
/// none of the operator's labels (what `kubectl autoscale deployment
/// <instance>-gateway` leaves behind).
fn user_hpa(component: &str) -> Value {
    json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": format!("{INSTANCE}-{component}"),
            "namespace": NAMESPACE,
        },
    })
}

/// Another cluster's operator-owned HPA: the operator's labels, but a
/// different `instance`, so it is out of this cluster's scope.
fn other_cluster_hpa(component: &str) -> Value {
    json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": format!("other-{component}"),
            "namespace": NAMESPACE,
            "labels": {
                "app.kubernetes.io/name": "ravel",
                "app.kubernetes.io/instance": "other",
                "app.kubernetes.io/component": component,
                "app.kubernetes.io/managed-by": "ravel-operator",
            },
        },
    })
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
    let query: Vec<String> = req
        .uri()
        .query()
        .map(|q| q.split('&').map(str::to_string).collect())
        .unwrap_or_default();
    let is_watch = query.iter().any(|pair| pair == "watch=true");
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
    let call = Call {
        method: method.to_string(),
        path: path.clone(),
        query,
        body: body.clone(),
    };
    fake.calls.lock().expect("lock").push(call.clone());

    let secrets = format!("/api/v1/namespaces/{NAMESPACE}/secrets/");
    let deployments = format!("/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{INSTANCE}-");
    let live = |p: &str| {
        p.strip_prefix(&deployments)
            .and_then(|component| fake.scenario.live.get(component))
            .cloned()
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
        (&Method::GET, p) if live(p).is_some() => {
            json_response(StatusCode::OK, &live(p).unwrap_or_default())
        }
        (&Method::PATCH, p) if p == status_path() => {
            if fake.scenario.status_fails {
                json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &failure(500, "InternalError"),
                )
            } else {
                json_response(StatusCode::OK, &cluster(&fake.scenario))
            }
        }
        // The HPA list the operator reads once per pass to decide which HPAs
        // it owns and which tier names are taken.
        (&Method::GET, p) if p == hpa_list_path() => {
            if fake.scenario.hpa_list_fails {
                json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &failure(500, "InternalError"),
                )
            } else {
                json_response(StatusCode::OK, &hpa_list(&fake.scenario))
            }
        }
        // The owned-HPA watch's initial list.
        (&Method::GET, "/apis/autoscaling/v2/horizontalpodautoscalers") => json_response(
            StatusCode::OK,
            &empty_list("HorizontalPodAutoscalerList", "autoscaling/v2"),
        ),
        // The listed HPA was replaced by a same-named one with a new uid: the
        // apiserver refuses a DELETE whose uid precondition names the old one.
        (&Method::DELETE, p)
            if fake.scenario.hpa_recreated_after_list
                && p.starts_with(&format!("{}/", hpa_list_path())) =>
        {
            if body["preconditions"]["uid"].is_string() {
                json_response(StatusCode::CONFLICT, &failure(409, "Conflict"))
            } else {
                json_response(
                    StatusCode::OK,
                    &json!({ "apiVersion": "v1", "kind": "Status", "metadata": {},
                             "status": "Success", "code": 200 }),
                )
            }
        }
        // Every HPA DELETE fails, to exercise the turn-off path failing before
        // the forced apply.
        (&Method::DELETE, p)
            if fake.scenario.hpa_delete_fails
                && p.starts_with(&format!("{}/", hpa_list_path())) =>
        {
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &failure(500, "InternalError"),
            )
        }
        // The handover apply conflicts (409), to exercise the handover failing
        // before the replica-less apply.
        (&Method::PATCH, _)
            if call.has_param("fieldManager=ravel-operator-handover")
                && fake.scenario.handover_conflicts =>
        {
            json_response(StatusCode::CONFLICT, &failure(409, "Conflict"))
        }
        // The handover apply carries no selector or template, so it answers
        // with the live object at the handed-over count.
        (&Method::PATCH, p)
            if call.has_param("fieldManager=ravel-operator-handover") && live(p).is_some() =>
        {
            let mut object = live(p).unwrap_or_default();
            object["spec"]["replicas"] = body["spec"]["replicas"].clone();
            json_response(StatusCode::OK, &object)
        }
        // Every other server-side apply echoes the applied object back.
        (&Method::PATCH, _) => json_response(StatusCode::OK, &body),
        // Nothing else exists: every other read and every delete is a 404.
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

/// Every request of one pass, up to and including its first status write
/// that carries conditions (the pass's final write, or the degraded write of
/// a failed pass).
struct Pass {
    calls: Vec<Call>,
}

fn is_final_status(call: &Call) -> bool {
    call.method == "PATCH"
        && call.path == status_path()
        && call.body["status"]["conditions"].is_array()
}

/// Run the operator against the fake until its first final status write,
/// then stop it.
async fn one_pass(scenario: Scenario) -> Pass {
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

    let mut done = None;
    for _ in 0..200 {
        let calls = fake.calls.lock().expect("lock").clone();
        if let Some(end) = calls.iter().position(is_final_status) {
            done = Some(calls[..=end].to_vec());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    operator.abort();
    Pass {
        calls: done.expect("the pass wrote a final status"),
    }
}

impl Pass {
    /// Indices and calls matching `method` and `path`.
    fn find(&self, method: &str, path: &str) -> Vec<(usize, &Call)> {
        self.calls
            .iter()
            .enumerate()
            .filter(|(_, c)| c.method == method && c.path == path)
            .collect()
    }

    /// The tier's handover applies.
    fn handovers(&self, component: &str) -> Vec<(usize, &Call)> {
        self.find("PATCH", &deployment_path(component))
            .into_iter()
            .filter(|(_, c)| c.has_param("fieldManager=ravel-operator-handover"))
            .collect()
    }

    /// The tier's Deployment applies under `ravel-operator`.
    fn applies(&self, component: &str) -> Vec<(usize, &Call)> {
        self.find("PATCH", &deployment_path(component))
            .into_iter()
            .filter(|(_, c)| c.has_param("fieldManager=ravel-operator"))
            .collect()
    }

    /// The one `ravel-operator` apply of the tier's Deployment.
    fn apply(&self, component: &str) -> (usize, &Call) {
        let applies = self.applies(component);
        let [apply] = applies.as_slice() else {
            panic!(
                "one {component} apply expected, got {}: {:#?}",
                applies.len(),
                self.summary()
            );
        };
        *apply
    }

    fn hpa_patches(&self, component: &str) -> Vec<(usize, &Call)> {
        self.find("PATCH", &hpa_path(component))
    }

    fn hpa_deletes(&self, component: &str) -> Vec<(usize, &Call)> {
        self.find("DELETE", &hpa_path(component))
    }

    /// Status PATCHes that carry only `replicaAdoptions` (the adoption
    /// record written ahead of the applies).
    fn record_writes(&self) -> Vec<(usize, &Call)> {
        self.find("PATCH", &status_path())
            .into_iter()
            .filter(|(_, c)| !c.body["status"]["conditions"].is_array())
            .collect()
    }

    fn final_status(&self) -> &Value {
        let last = self.calls.last().expect("calls");
        assert!(is_final_status(last));
        &last.body["status"]
    }

    fn summary(&self) -> Vec<String> {
        self.calls
            .iter()
            .map(|c| format!("{} {}?{}", c.method, c.path, c.query.join("&")))
            .collect()
    }
}

fn managed(min: i32, max: i32) -> Value {
    json!({ "mode": "Managed", "minReplicas": min, "maxReplicas": max,
            "targetCPUUtilizationPercentage": 70 })
}

fn external(min: i32, max: i32) -> Value {
    json!({ "mode": "External", "minReplicas": min, "maxReplicas": max })
}

fn cpu_request() -> Value {
    json!({ "requests": { "cpu": "500m", "memory": "512Mi" } })
}

fn entry(tier: &str, declared: i32) -> Value {
    json!({ "tier": tier, "declaredReplicas": declared })
}

fn adoption(tier: &str, adopted: i32, declared: i32) -> Value {
    json!({ "tier": tier, "adoptedReplicas": adopted, "declaredReplicas": declared })
}

/// The handover apply carries the live count and nothing else in its spec,
/// under the handover manager and without force.
fn assert_handover(call: &Call, replicas: i32) {
    assert!(
        call.has_param("fieldManager=ravel-operator-handover"),
        "{call:?}"
    );
    assert!(
        !call.has_param("force=true"),
        "handover is not forced: {call:?}"
    );
    assert_eq!(
        call.body["spec"],
        json!({ "replicas": replicas }),
        "the handover spec holds only replicas: {call:?}"
    );
    assert_eq!(call.body["kind"], "Deployment");
    assert_eq!(
        call.body["metadata"],
        json!({ "name": format!("{INSTANCE}-gateway"), "namespace": NAMESPACE })
    );
}

fn assert_forced_replicas(call: &Call, replicas: i32) {
    assert!(call.has_param("force=true"), "{call:?}");
    assert_eq!(
        call.body["spec"]["replicas"],
        json!(replicas),
        "applied replicas"
    );
}

fn assert_released(call: &Call) {
    assert!(
        call.body["spec"].get("replicas").is_none(),
        "the apply releases spec.replicas: {}",
        call.body["spec"]
    );
    assert!(call.body["spec"]["template"].is_object());
}

fn condition<'a>(status: &'a Value, kind: &str) -> Option<&'a Value> {
    status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == kind)
}

/// Enabling `Managed` autoscaling on a running gateway the operator owns the
/// replicas of: the live count is handed over first, then the Deployment is
/// applied without `replicas`, then the HPA.
#[tokio::test]
async fn enabling_autoscaling_hands_replicas_over_before_dropping_them() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 5, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;

    let handovers = pass.handovers("gateway");
    let [(handover_at, handover)] = handovers.as_slice() else {
        panic!("one handover expected: {:#?}", pass.summary());
    };
    assert_handover(handover, 3);
    let (apply_at, apply) = pass.apply("gateway");
    assert_released(apply);
    let hpas = pass.hpa_patches("gateway");
    let [(hpa_at, hpa)] = hpas.as_slice() else {
        panic!("one HPA apply expected: {:#?}", pass.summary());
    };
    assert!(
        handover_at < &apply_at && apply_at < *hpa_at,
        "order: handover, replica-less apply, HPA: {:#?}",
        pass.summary()
    );

    let hpa = &hpa.body;
    assert_eq!(hpa["apiVersion"], "autoscaling/v2");
    assert_eq!(hpa["kind"], "HorizontalPodAutoscaler");
    assert_eq!(hpa["metadata"]["name"], format!("{INSTANCE}-gateway"));
    assert_eq!(
        hpa["metadata"]["ownerReferences"][0]["kind"],
        "RavelCluster"
    );
    // The uid is what makes the HPA the operator's on the next pass's list.
    assert_eq!(hpa["metadata"]["ownerReferences"][0]["uid"], CLUSTER_UID);
    assert_eq!(
        hpa["metadata"]["labels"]["app.kubernetes.io/component"],
        "gateway"
    );
    assert_eq!(
        hpa["spec"]["scaleTargetRef"],
        json!({ "apiVersion": "apps/v1", "kind": "Deployment", "name": format!("{INSTANCE}-gateway") })
    );
    assert_eq!(hpa["spec"]["minReplicas"], json!(2));
    assert_eq!(hpa["spec"]["maxReplicas"], json!(4));
    assert_eq!(
        hpa["spec"]["metrics"],
        json!([{ "type": "Resource", "resource": { "name": "cpu",
            "target": { "type": "Utilization", "averageUtilization": 70 } } }])
    );
    assert_eq!(
        hpa["spec"]["behavior"],
        json!({ "scaleDown": { "policies": [{ "type": "Pods", "value": 1, "periodSeconds": 60 }] } })
    );

    assert_eq!(
        pass.final_status()["autoscaledTiers"],
        json!([entry("gateway", 5)])
    );
    assert!(pass.hpa_deletes("gateway").is_empty());
}

/// The handover keeps a live count above `maxReplicas`: the HPA brings it
/// into range, the operator does not.
#[tokio::test]
async fn handover_does_not_clamp_the_live_count() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        live: BTreeMap::from([("gateway", live("gateway", 7, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    let handovers = pass.handovers("gateway");
    let [(_, handover)] = handovers.as_slice() else {
        panic!("one handover expected: {:#?}", pass.summary());
    };
    assert_handover(handover, 7);
}

/// No live Deployment: it is created at `minReplicas` under the operator's
/// own manager, with no handover, no replica-less apply and no HPA yet. ADR
/// step 0 records nothing until the handover, so `autoscaledTiers` stays empty
/// on the create pass (item 2).
#[tokio::test]
async fn a_new_tier_starts_at_min_replicas_and_records_nothing() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        ..Scenario::default()
    })
    .await;
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 2);
    assert!(
        pass.handovers("gateway").is_empty(),
        "{:#?}",
        pass.summary()
    );
    assert!(
        pass.hpa_patches("gateway").is_empty(),
        "{:#?}",
        pass.summary()
    );
    // Recorded only after the handover: the create pass writes no entry. Fails
    // against ea484335f, where the create plan carried `autoscaled: Some(..)`
    // (autoscaling.rs `None => TierPlan { .. autoscaled, .. }`).
    assert_eq!(pass.final_status()["autoscaledTiers"], Value::Null);
}

/// Turning autoscaling off with `replicas` unchanged: the live count is
/// recorded first, then the operator's own HPA is deleted, then the count is
/// forced and the tier leaves `autoscaledTiers`. The delete precedes the
/// forced apply so the HPA cannot rescale between the adoption read and the
/// apply (item 3). Fails against ea484335f, which applied then deleted
/// (controller.rs: the forced apply ran before `if plan.delete_hpa {
/// delete_if_present(..) }`).
#[tokio::test]
async fn turning_autoscaling_off_records_then_keeps_the_live_count() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Kubectl))]),
        hpas: vec![owned_hpa("gateway")],
        ..Scenario::default()
    })
    .await;

    let records = pass.record_writes();
    let [(record_at, record)] = records.as_slice() else {
        panic!("one adoption record write expected: {:#?}", pass.summary());
    };
    assert_eq!(
        record.body,
        json!({ "status": { "replicaAdoptions": [adoption("gateway", 9, 1)] } })
    );
    let (apply_at, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 9);
    let deletes = pass.hpa_deletes("gateway");
    let [(delete_at, _)] = deletes.as_slice() else {
        panic!("one HPA delete expected: {:#?}", pass.summary());
    };
    assert!(
        record_at < delete_at && delete_at < &apply_at,
        "order: record, HPA delete, forced apply: {:#?}",
        pass.summary()
    );
    let status = pass.final_status();
    assert_eq!(status["autoscaledTiers"], Value::Null, "{status}");
    assert_eq!(
        status["replicaAdoptions"],
        json!([adoption("gateway", 9, 1)])
    );
}

/// A later pass with the record and the same declared count keeps applying
/// the adopted count.
#[tokio::test]
async fn an_adoption_record_keeps_the_count_while_replicas_is_unchanged() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({ "replicaAdoptions": [adoption("gateway", 9, 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 9);
    assert!(pass.record_writes().is_empty());
    assert_eq!(
        pass.final_status()["replicaAdoptions"],
        json!([adoption("gateway", 9, 1)])
    );
}

/// Once `replicas` changes, the declared count applies and the record is
/// cleared.
#[tokio::test]
async fn changing_replicas_ends_the_adoption() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 4 }),
        status: json!({ "replicaAdoptions": [adoption("gateway", 9, 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 4);
    assert_eq!(pass.final_status()["replicaAdoptions"], Value::Null);
}

/// `replicas` edited in the same change that removes the block applies at
/// once, with no record.
#[tokio::test]
async fn replicas_changed_with_the_turn_off_applies_at_once() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 5 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 3)] }),
        live: BTreeMap::from([("gateway", live("gateway", 12, ReplicasOwner::Kubectl))]),
        hpas: vec![owned_hpa("gateway")],
        ..Scenario::default()
    })
    .await;
    let (apply_at, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 5);
    assert!(pass.record_writes().is_empty(), "{:#?}", pass.summary());
    let deletes = pass.hpa_deletes("gateway");
    let [(delete_at, _)] = deletes.as_slice() else {
        panic!("one HPA delete expected: {:#?}", pass.summary());
    };
    assert!(
        delete_at < &apply_at,
        "the HPA is deleted before the forced apply: {:#?}",
        pass.summary()
    );
    let status = pass.final_status();
    assert_eq!(status["replicaAdoptions"], Value::Null, "{status}");
    assert_eq!(status["autoscaledTiers"], Value::Null, "{status}");
}

/// The block removed with `replicas` left as it was: the live 12 is kept.
#[tokio::test]
async fn replicas_unchanged_at_turn_off_keeps_the_live_count() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 3)] }),
        live: BTreeMap::from([("gateway", live("gateway", 12, ReplicasOwner::Kubectl))]),
        ..Scenario::default()
    })
    .await;
    let records = pass.record_writes();
    let [(_, record)] = records.as_slice() else {
        panic!("one adoption record write expected: {:#?}", pass.summary());
    };
    assert_eq!(
        record.body["status"]["replicaAdoptions"],
        json!([adoption("gateway", 12, 3)])
    );
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 12);
}

/// The adoption record cannot be written: nothing is applied to the tier and
/// its HPA is kept, so the scaler still holds the count.
#[tokio::test]
async fn a_failed_record_write_applies_nothing() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Kubectl))]),
        status_fails: true,
        ..Scenario::default()
    })
    .await;
    assert_eq!(pass.record_writes().len(), 1, "{:#?}", pass.summary());
    assert!(pass.applies("gateway").is_empty(), "{:#?}", pass.summary());
    assert!(
        pass.hpa_deletes("gateway").is_empty(),
        "{:#?}",
        pass.summary()
    );
}

/// A tier never autoscaled whose replicas `kubectl scale` took: today's
/// behaviour, the declared count is forced and nothing is recorded.
#[tokio::test]
async fn a_never_autoscaled_tier_is_forced_to_its_declared_count() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 2 }),
        live: BTreeMap::from([("gateway", live("gateway", 6, ReplicasOwner::Kubectl))]),
        ..Scenario::default()
    })
    .await;
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 2);
    assert!(pass.record_writes().is_empty());
    assert!(pass.handovers("gateway").is_empty());
    assert_eq!(pass.final_status()["replicaAdoptions"], Value::Null);
}

/// After the handover, `ravel-operator` no longer owns `spec.replicas`: no
/// second handover, and the apply still omits it.
#[tokio::test]
async fn a_released_tier_is_not_handed_over_again() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        status: json!({ "autoscaledTiers": [entry("gateway", 3)] }),
        live: BTreeMap::from([("gateway", live("gateway", 4, ReplicasOwner::Kubectl))]),
        ..Scenario::default()
    })
    .await;
    assert!(
        pass.handovers("gateway").is_empty(),
        "{:#?}",
        pass.summary()
    );
    let (_, apply) = pass.apply("gateway");
    assert_released(apply);
    assert_eq!(pass.hpa_patches("gateway").len(), 1);
}

/// A withheld tier keeps today's forced replicas, gets no HPA and no
/// handover, and is named in `AutoscalingWithheld`.
fn assert_withheld(pass: &Pass, replicas: i32, reason_fragment: &str) {
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, replicas);
    assert!(
        pass.handovers("gateway").is_empty(),
        "{:#?}",
        pass.summary()
    );
    assert!(
        pass.hpa_patches("gateway").is_empty(),
        "{:#?}",
        pass.summary()
    );
    let status = pass.final_status();
    let withheld = condition(status, "AutoscalingWithheld").expect("AutoscalingWithheld");
    assert_eq!(withheld["status"], "True");
    let message = withheld["message"].as_str().expect("message");
    assert!(message.contains("gateway: "), "{message}");
    assert!(message.contains(reason_fragment), "{message}");
    assert_eq!(status["autoscaledTiers"], Value::Null);
}

#[tokio::test]
async fn managed_without_a_cpu_request_is_withheld() {
    let pass = one_pass(Scenario {
        gateway: json!({
            "replicas": 3,
            "resources": { "requests": { "memory": "512Mi" } },
            "autoscaling": managed(2, 4),
        }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    assert_withheld(&pass, 3, "CPU request");
}

#[tokio::test]
async fn gateway_on_ingress_nginx_affinity_is_withheld() {
    let pass = one_pass(Scenario {
        gateway: json!({
            "replicas": 3,
            "resources": cpu_request(),
            "autoscaling": external(2, 4),
            "ingestAffinity": { "enabled": true, "backend": "ingressNginx" },
        }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    assert_withheld(&pass, 3, "ingressNginx");
}

/// No tier withheld: no `AutoscalingWithheld` condition.
#[tokio::test]
async fn no_withheld_condition_without_a_withheld_tier() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    assert!(condition(pass.final_status(), "AutoscalingWithheld").is_none());
}

/// `External`: the handover and the replica-less apply, and no HPA.
#[tokio::test]
async fn external_mode_releases_replicas_without_an_hpa() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "autoscaling": external(2, 6) }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    let handovers = pass.handovers("gateway");
    let [(handover_at, handover)] = handovers.as_slice() else {
        panic!("one handover expected: {:#?}", pass.summary());
    };
    assert_handover(handover, 3);
    let (apply_at, apply) = pass.apply("gateway");
    assert_released(apply);
    assert!(handover_at < &apply_at);
    assert!(
        pass.hpa_patches("gateway").is_empty(),
        "{:#?}",
        pass.summary()
    );
    assert_eq!(
        pass.final_status()["autoscaledTiers"],
        json!([entry("gateway", 3)])
    );
}

/// The maintain HPA scales down one pod per 600 s.
#[tokio::test]
async fn maintain_hpa_scales_down_one_pod_per_ten_minutes() {
    let pass = one_pass(Scenario {
        maintain: json!({
            "replicas": 2,
            "resources": cpu_request(),
            "autoscaling": {
                "mode": "Managed", "minReplicas": 1, "maxReplicas": 3,
                "targetCPUUtilizationPercentage": 80, "scaleDownStabilizationSeconds": 900,
            },
        }),
        live: BTreeMap::from([("maintain", live("maintain", 2, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    let hpas = pass.hpa_patches("maintain");
    let [(_, hpa)] = hpas.as_slice() else {
        panic!("one maintain HPA apply expected: {:#?}", pass.summary());
    };
    assert_eq!(
        hpa.body["spec"]["behavior"],
        json!({ "scaleDown": { "stabilizationWindowSeconds": 900,
            "policies": [{ "type": "Pods", "value": 1, "periodSeconds": 600 }] } })
    );
    assert!(pass.hpa_patches("gateway").is_empty());
}

/// No block anywhere: replicas applied as today, no HPA touched, no
/// autoscaling status.
#[tokio::test]
async fn an_absent_block_changes_nothing() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3 }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        ..Scenario::default()
    })
    .await;
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 3);
    for component in ["gateway", "query", "maintain"] {
        assert!(pass.hpa_patches(component).is_empty());
        assert!(pass.hpa_deletes(component).is_empty());
    }
    let status = pass.final_status();
    assert_eq!(status["autoscaledTiers"], Value::Null);
    assert_eq!(status["replicaAdoptions"], Value::Null);
    assert!(condition(status, "AutoscalingWithheld").is_none());
}

/// A DELETE to another cluster's HPA by its full name.
fn other_cluster_hpa_deletes<'a>(pass: &'a Pass, component: &str) -> Vec<(usize, &'a Call)> {
    let path = format!(
        "/apis/autoscaling/v2/namespaces/{NAMESPACE}/horizontalpodautoscalers/other-{component}"
    );
    pass.find("DELETE", &path)
}

/// External mode with a user's own HPA at the colliding `<instance>-gateway`
/// name, plus another cluster's labelled HPA: the operator touches neither,
/// over two passes. Fails against ea484335f, whose fixed-name delete removed
/// `rc-gateway` (controller.rs `if plan.delete_hpa { delete_if_present(ctx.hpas,
/// &hpa) }`), and against a label-only sweep that ignores the instance label,
/// which would delete another cluster's `other-gateway`.
#[tokio::test]
async fn external_mode_leaves_a_user_hpa_alone() {
    let scenario = || Scenario {
        gateway: json!({ "replicas": 3, "autoscaling": external(2, 6) }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        hpas: vec![user_hpa("gateway"), other_cluster_hpa("gateway")],
        ..Scenario::default()
    };
    for pass_number in 1..=2 {
        let pass = one_pass(scenario()).await;
        assert!(
            pass.hpa_deletes("gateway").is_empty(),
            "pass {pass_number}: no DELETE to the user HPA: {:#?}",
            pass.summary()
        );
        assert!(
            pass.hpa_patches("gateway").is_empty(),
            "pass {pass_number}: no PATCH to the user HPA: {:#?}",
            pass.summary()
        );
        assert!(
            other_cluster_hpa_deletes(&pass, "gateway").is_empty(),
            "pass {pass_number}: another cluster's HPA is out of scope: {:#?}",
            pass.summary()
        );
    }
}

/// An orphan: a rendered (owned) HPA whose tier has no block and no
/// `autoscaledTiers` entry, the shape a status write that failed after the HPA
/// landed leaves behind. The ownership sweep deletes it even though no status
/// entry points the per-tier path at it. Fails against ea484335f, which keyed
/// the delete on the block/entry and so never reached this HPA.
#[tokio::test]
async fn an_orphaned_rendered_hpa_is_swept() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3 }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        hpas: vec![owned_hpa("gateway")],
        ..Scenario::default()
    })
    .await;
    assert_eq!(
        pass.hpa_deletes("gateway").len(),
        1,
        "the orphan is swept: {:#?}",
        pass.summary()
    );
}

/// The handover apply conflicts (409): the tier fails before the replica-less
/// apply, so `spec.replicas` is never dropped. Reverting the `?` on the
/// handover patch (controller.rs, `ctx.deployments.patch(..).await?`) would let
/// the apply proceed and drop the field.
#[tokio::test]
async fn a_conflicting_handover_applies_no_replica_less_deployment() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        handover_conflicts: true,
        ..Scenario::default()
    })
    .await;
    assert_eq!(
        pass.handovers("gateway").len(),
        1,
        "the handover was attempted: {:#?}",
        pass.summary()
    );
    assert!(
        pass.applies("gateway").is_empty(),
        "no replica-less apply after the conflict: {:#?}",
        pass.summary()
    );
}

/// Turn-off where the HPA delete fails: the adoption record is written, the
/// delete is attempted, and the forced apply does not run, so the scaler's
/// count still stands for the next pass. Fails against ea484335f, which applied
/// the forced count before deleting the HPA, so the apply ran regardless.
#[tokio::test]
async fn a_failed_hpa_delete_at_turn_off_applies_nothing() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Kubectl))]),
        hpas: vec![owned_hpa("gateway")],
        hpa_delete_fails: true,
        ..Scenario::default()
    })
    .await;
    assert_eq!(
        pass.record_writes().len(),
        1,
        "the adoption record was written: {:#?}",
        pass.summary()
    );
    assert_eq!(
        pass.hpa_deletes("gateway").len(),
        1,
        "the delete was attempted: {:#?}",
        pass.summary()
    );
    assert!(
        pass.applies("gateway").is_empty(),
        "no forced apply after the failed delete: {:#?}",
        pass.summary()
    );
    assert_eq!(
        pass.final_status()["autoscaledTiers"],
        json!([entry("gateway", 1)]),
        "the entry is kept for the next pass"
    );
}

/// The pass after a failed turn-off: the entry and the earlier record are both
/// in status, and the scaler has since moved the tier from 9 to 11. The pass
/// takes the turn-off again, reads the live count anew, and records and
/// applies 11, not the 9 recorded before.
#[tokio::test]
async fn a_retried_turn_off_records_the_live_count_anew() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({
            "autoscaledTiers": [entry("gateway", 1)],
            "replicaAdoptions": [adoption("gateway", 9, 1)],
        }),
        live: BTreeMap::from([("gateway", live("gateway", 11, ReplicasOwner::Kubectl))]),
        hpas: vec![owned_hpa("gateway")],
        ..Scenario::default()
    })
    .await;
    let records = pass.record_writes();
    let [(_, record)] = records.as_slice() else {
        panic!("one adoption record write expected: {:#?}", pass.summary());
    };
    assert_eq!(
        record.body,
        json!({ "status": { "replicaAdoptions": [adoption("gateway", 11, 1)] } })
    );
    let (_, apply) = pass.apply("gateway");
    assert_forced_replicas(apply, 11);
}

/// A live Deployment the operator owns `spec.replicas` on but that carries no
/// count: no handover and no replica-less apply, and the tier reports the
/// error. Fails against ea484335f, which handed over `live.replicas.unwrap_or(1)`
/// (autoscaling.rs) and so dropped the tier to one replica.
#[tokio::test]
async fn an_owned_live_without_replicas_hands_over_nothing() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        live: BTreeMap::from([("gateway", live_without_replicas("gateway"))]),
        ..Scenario::default()
    })
    .await;
    assert!(
        pass.handovers("gateway").is_empty(),
        "no handover of an invented count: {:#?}",
        pass.summary()
    );
    assert!(
        pass.applies("gateway").is_empty(),
        "no replica-less apply: {:#?}",
        pass.summary()
    );
    let degraded = condition(pass.final_status(), "Degraded").expect("Degraded");
    assert_eq!(degraded["reason"], "AutoscalingHandoverReplicasMissing");
}

/// Mode Managed where an HPA this RavelCluster does not own already sits at
/// `<instance>-gateway`: the tier is withheld with forced replicas, and the
/// user HPA gets no PATCH and no DELETE. The second case copies the operator's
/// labels but has no ownerReference to this cluster, so a labels-only
/// ownership check would take it over.
#[tokio::test]
async fn managed_mode_refuses_to_take_over_a_user_hpa() {
    for (case, hpa) in [
        ("unlabelled", user_hpa("gateway")),
        ("labelled, no ownerReference", labelled_user_hpa("gateway")),
    ] {
        let pass = one_pass(Scenario {
            gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
            live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
            hpas: vec![hpa],
            ..Scenario::default()
        })
        .await;
        assert!(
            pass.hpa_deletes("gateway").is_empty(),
            "{case}: no DELETE to the user HPA: {:#?}",
            pass.summary()
        );
        assert_withheld(&pass, 3, "an HPA this RavelCluster does not own");
    }
}

/// A pass that ends in the degraded status write (the query handover
/// conflicts) still names the gateway whose HPA name is taken in
/// `AutoscalingWithheld`: that write carries the pass's finding, not only what
/// the spec alone shows.
#[tokio::test]
async fn a_degraded_pass_still_names_a_taken_hpa_name() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        query: json!({ "replicas": 2, "autoscaling": external(2, 4) }),
        live: BTreeMap::from([
            ("gateway", live("gateway", 3, ReplicasOwner::Operator)),
            ("query", live("query", 2, ReplicasOwner::Operator)),
        ]),
        hpas: vec![user_hpa("gateway")],
        handover_conflicts: true,
        ..Scenario::default()
    })
    .await;
    let status = pass.final_status();
    let degraded = condition(status, "Degraded").expect("Degraded");
    assert_eq!(degraded["status"], "True", "{status}");
    let withheld = condition(status, "AutoscalingWithheld").expect("AutoscalingWithheld");
    let message = withheld["message"].as_str().expect("message");
    assert!(
        message.contains("gateway: mode Managed renders its HorizontalPodAutoscaler"),
        "{message}"
    );
}

/// Turn-off while the HPA list fails: the operator cannot tell whether it owns
/// the HPA it would delete, so the tier fails before the adoption record and
/// before any apply, and keeps its `autoscaledTiers` entry.
#[tokio::test]
async fn a_failed_hpa_list_at_turn_off_applies_nothing() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Kubectl))]),
        hpas: vec![owned_hpa("gateway")],
        hpa_list_fails: true,
        ..Scenario::default()
    })
    .await;
    assert!(
        pass.applies("gateway").is_empty(),
        "no Deployment apply: {:#?}",
        pass.summary()
    );
    assert!(
        pass.record_writes().is_empty(),
        "no adoption record: {:#?}",
        pass.summary()
    );
    assert!(
        pass.hpa_deletes("gateway").is_empty(),
        "no HPA DELETE: {:#?}",
        pass.summary()
    );
    let status = pass.final_status();
    assert_eq!(status["autoscaledTiers"], json!([entry("gateway", 1)]));
    let degraded = condition(status, "Degraded").expect("Degraded");
    assert_eq!(degraded["reason"], "AutoscalingHpaListFailed");
}

/// Mode Managed while the HPA list fails: whether the name is taken is
/// unknown, so the tier neither hands over nor applies, and is not reported as
/// withheld.
#[tokio::test]
async fn a_failed_hpa_list_in_managed_mode_applies_nothing() {
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 4) }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        hpas: vec![user_hpa("gateway")],
        hpa_list_fails: true,
        ..Scenario::default()
    })
    .await;
    assert!(
        pass.handovers("gateway").is_empty(),
        "no handover: {:#?}",
        pass.summary()
    );
    assert!(
        pass.applies("gateway").is_empty(),
        "no Deployment apply: {:#?}",
        pass.summary()
    );
    assert!(
        pass.hpa_patches("gateway").is_empty(),
        "no HPA apply: {:#?}",
        pass.summary()
    );
    let status = pass.final_status();
    assert!(condition(status, "AutoscalingWithheld").is_none());
    let degraded = condition(status, "Degraded").expect("Degraded");
    assert_eq!(degraded["reason"], "AutoscalingHpaListFailed");
}

/// `maintain.enabled: false` with a Managed block: the disabled tier desires
/// no HPA, so the one the operator rendered while it was enabled is swept.
#[tokio::test]
async fn disabling_maintain_sweeps_its_hpa() {
    let pass = one_pass(Scenario {
        maintain: json!({
            "enabled": false,
            "replicas": 2,
            "resources": cpu_request(),
            "autoscaling": managed(1, 3),
        }),
        hpas: vec![owned_hpa("maintain")],
        ..Scenario::default()
    })
    .await;
    assert_eq!(
        pass.hpa_deletes("maintain").len(),
        1,
        "the disabled tier's HPA is swept: {:#?}",
        pass.summary()
    );
    assert!(
        pass.hpa_patches("maintain").is_empty(),
        "no HPA apply for a disabled tier: {:#?}",
        pass.summary()
    );
}

/// An HPA with the operator's labels but no ownerReference to this cluster is
/// not the operator's: neither the orphan sweep nor the turn-off delete
/// touches it.
#[tokio::test]
async fn a_labelled_hpa_without_this_clusters_owner_reference_is_left_alone() {
    for (case, status) in [
        ("orphan sweep", json!({})),
        (
            "turn-off",
            json!({ "autoscaledTiers": [entry("gateway", 3)] }),
        ),
    ] {
        let pass = one_pass(Scenario {
            gateway: json!({ "replicas": 3 }),
            status,
            live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Kubectl))]),
            hpas: vec![labelled_user_hpa("gateway")],
            ..Scenario::default()
        })
        .await;
        assert!(
            pass.hpa_deletes("gateway").is_empty(),
            "{case}: no DELETE: {:#?}",
            pass.summary()
        );
        assert!(
            pass.hpa_patches("gateway").is_empty(),
            "{case}: no PATCH: {:#?}",
            pass.summary()
        );
    }
}

/// The operator's HPA was replaced, between the pass's list and its delete, by
/// a same-named HPA someone else created. Each delete of an owned HPA carries
/// the uid the list read as a precondition, so the apiserver answers 409 and
/// the new HPA survives; the 409 fails the pass like any failed delete. At
/// turn-off that means no forced apply and the `autoscaledTiers` entry kept;
/// in the orphan sweep, a `Degraded` pass. Fails against an unconditioned
/// delete (`DeleteParams::default()`), which the fake answers by deleting the
/// new HPA, after which the turn-off applies.
#[tokio::test]
async fn an_hpa_recreated_after_the_list_is_not_deleted() {
    let turn_off = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Kubectl))]),
        hpas: vec![owned_hpa("gateway")],
        hpa_recreated_after_list: true,
        ..Scenario::default()
    })
    .await;
    let sweep = one_pass(Scenario {
        gateway: json!({ "replicas": 3 }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        hpas: vec![owned_hpa("gateway")],
        hpa_recreated_after_list: true,
        ..Scenario::default()
    })
    .await;
    for (case, pass) in [("turn-off", &turn_off), ("orphan sweep", &sweep)] {
        let deletes = pass.hpa_deletes("gateway");
        let [(_, delete)] = deletes.as_slice() else {
            panic!("{case}: one HPA delete expected: {:#?}", pass.summary());
        };
        assert_eq!(
            delete.body["preconditions"],
            json!({ "uid": listed_hpa_uid("gateway") }),
            "{case}: the delete is conditioned on the listed uid"
        );
        let degraded = condition(pass.final_status(), "Degraded").expect("Degraded");
        assert_eq!(degraded["status"], "True", "{case}");
    }
    assert!(
        turn_off.applies("gateway").is_empty(),
        "no forced apply after the refused delete: {:#?}",
        turn_off.summary()
    );
    assert_eq!(
        turn_off.final_status()["autoscaledTiers"],
        json!([entry("gateway", 1)]),
        "the entry is kept for the next pass"
    );
}

/// An owned HPA the list returned without a uid: the delete cannot be
/// conditioned on the listed uid, so it is not sent. At turn-off the tier
/// fails before its forced apply and keeps its `autoscaledTiers` entry; in the
/// orphan sweep the pass is `Degraded`. Both report reason
/// `AutoscalingHpaUidMissing`. Fails against an implementation that sends an
/// unconditioned delete when the uid is missing.
#[tokio::test]
async fn an_owned_hpa_listed_without_a_uid_is_not_deleted() {
    let uid_less = || {
        let mut hpa = owned_hpa("gateway");
        if let Some(metadata) = hpa["metadata"].as_object_mut() {
            metadata.remove("uid");
        }
        hpa
    };
    let turn_off = one_pass(Scenario {
        gateway: json!({ "replicas": 1 }),
        status: json!({ "autoscaledTiers": [entry("gateway", 1)] }),
        live: BTreeMap::from([("gateway", live("gateway", 9, ReplicasOwner::Kubectl))]),
        hpas: vec![uid_less()],
        ..Scenario::default()
    })
    .await;
    let sweep = one_pass(Scenario {
        gateway: json!({ "replicas": 3 }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        hpas: vec![uid_less()],
        ..Scenario::default()
    })
    .await;
    for (case, pass) in [("turn-off", &turn_off), ("orphan sweep", &sweep)] {
        assert!(
            pass.hpa_deletes("gateway").is_empty(),
            "{case}: no HPA DELETE: {:#?}",
            pass.summary()
        );
        let degraded = condition(pass.final_status(), "Degraded").expect("Degraded");
        assert_eq!(degraded["reason"], "AutoscalingHpaUidMissing", "{case}");
    }
    assert!(
        turn_off.applies("gateway").is_empty(),
        "no forced apply: {:#?}",
        turn_off.summary()
    );
    assert_eq!(
        turn_off.final_status()["autoscaledTiers"],
        json!([entry("gateway", 1)]),
        "the entry is kept for the next pass"
    );
}

/// A failed HPA list keeps the stored `hpaReplicas` and `AutoscalingDegraded`
/// as they were, the condition's observedGeneration included, on a pass at a
/// later generation. Fails against an implementation that rebuilds the
/// condition from the stored message, which stamps generation 2.
#[tokio::test]
async fn a_failed_hpa_list_keeps_the_stored_hpa_report() {
    let stored_degraded = json!({
        "type": "AutoscalingDegraded", "status": "True", "observedGeneration": 1,
        "lastTransitionTime": "2026-11-16T00:00:00Z",
        "reason": "HorizontalPodAutoscalerNotScaling",
        "message": "a HorizontalPodAutoscaler the operator renders reports it cannot scale \
                    its tier: gateway: ScalingActive=False reason FailedGetResourceMetric: x",
    });
    let stored_replicas =
        json!([{ "tier": "gateway", "desiredReplicas": 4, "currentReplicas": 3 }]);
    let pass = one_pass(Scenario {
        gateway: json!({ "replicas": 3, "resources": cpu_request(), "autoscaling": managed(2, 6) }),
        status: json!({
            "autoscaledTiers": [entry("gateway", 3)],
            "hpaReplicas": stored_replicas,
            "conditions": [stored_degraded],
        }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Kubectl))]),
        hpas: vec![owned_hpa("gateway")],
        hpa_list_fails: true,
        generation: 2,
        ..Scenario::default()
    })
    .await;
    let status = pass.final_status();
    assert_eq!(
        condition(status, "Degraded").expect("Degraded")["reason"],
        "AutoscalingHpaListFailed"
    );
    assert_eq!(
        condition(status, "AutoscalingDegraded"),
        Some(&stored_degraded),
        "{status}"
    );
    assert_eq!(status["hpaReplicas"], stored_replicas, "{status}");
}

/// A cluster that has never used autoscaling (no block, no `autoscaledTiers`
/// entry) does not need the HPA list, so a failing list changes nothing: every
/// tier gets the same Deployment apply as on a pass whose list succeeds, at
/// its declared count, no HPA is written, and the pass is not `Degraded`.
#[tokio::test]
async fn a_failed_hpa_list_leaves_a_cluster_without_autoscaling_untouched() {
    let scenario = |hpa_list_fails| Scenario {
        gateway: json!({ "replicas": 3 }),
        query: json!({ "replicas": 2 }),
        maintain: json!({ "replicas": 1 }),
        live: BTreeMap::from([("gateway", live("gateway", 3, ReplicasOwner::Operator))]),
        hpa_list_fails,
        ..Scenario::default()
    };
    let listed = one_pass(scenario(false)).await;
    let failed = one_pass(scenario(true)).await;
    assert!(
        !failed.find("GET", &hpa_list_path()).is_empty(),
        "the pass listed HPAs: {:#?}",
        failed.summary()
    );
    for (component, replicas) in [("gateway", 3), ("query", 2), ("maintain", 1)] {
        let (_, apply) = failed.apply(component);
        assert_forced_replicas(apply, replicas);
        assert_eq!(
            apply.body,
            listed.apply(component).1.body,
            "{component}: the same apply as with a working list"
        );
        assert!(failed.hpa_patches(component).is_empty());
        assert!(failed.hpa_deletes(component).is_empty());
    }
    let status = failed.final_status();
    assert!(condition(status, "Degraded").is_none(), "{status}");
    assert_eq!(status["autoscaledTiers"], Value::Null);
    assert_eq!(status["hpaReplicas"], Value::Null);
    assert!(condition(status, "AutoscalingDegraded").is_none());
}
