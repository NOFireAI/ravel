//! ADR-3853 D2: the two-level RLOG merge needs local scratch space, and on
//! Kubernetes that is an ephemeral volume on the maintain Deployment.
//! `spec.maintain.scratch` renders four things together: an `emptyDir` named
//! `scratch` limited to `sizeGi`, its mount in the ravel-server container, the
//! `--maintain-scratch-dir` flag pointing at that mount, and an
//! `ephemeral-storage` request of the same size. Without the field none of
//! them render.
//!
//! One reconcile pass runs against a loopback fake apiserver that records the
//! body of every server-side apply, so the assertions are on the maintain
//! Deployment the operator actually sent.

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
use ravel_operator::ravel_cluster_crd;
use ravel_operator::reconcile::qualify_job_input_hash;
use serde_json::{Value, json};
use tokio::net::TcpListener;

const NAMESPACE: &str = "default";
const INSTANCE: &str = "rc";
const CREDENTIALS_RV: &str = "cred-1";
/// Base64 of 64 `0` characters: a well-formed key for both the audit-token
/// Secret and the maintain key Secret, so no tier is withheld.
const KEY_B64: &str =
    "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMA==";
const MAINTAIN_KEYS: &str = "maintain-keys";
const DEFAULT_MOUNT: &str = "/var/lib/ravel/scratch";

/// A maintained cluster; `scratch` is inserted verbatim as
/// `spec.maintain.scratch` when given.
fn spec(scratch: Option<Value>) -> Value {
    let mut maintain = json!({
        "enabled": true,
        "keySecretRef": { "name": MAINTAIN_KEYS },
    });
    if let Some(scratch) = scratch {
        maintain["scratch"] = scratch;
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

fn cluster(spec: &Value) -> Value {
    let typed: RavelClusterSpec = serde_json::from_value(spec.clone()).expect("spec parses");
    json!({
        "apiVersion": "ravel.nofire.ai/v1alpha1",
        "kind": "RavelCluster",
        "metadata": {
            "name": INSTANCE,
            "namespace": NAMESPACE,
            "uid": "00000000-0000-0000-0000-000000003876",
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": spec,
        // Already qualified for these inputs, so the pass reaches the
        // Deployments.
        "status": {
            "storeQualifiedHash": qualify_job_input_hash(&typed, Some(CREDENTIALS_RV)),
        },
    })
}

fn secret(name: &str, data: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": NAMESPACE, "resourceVersion": CREDENTIALS_RV },
        "data": data,
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

/// The cluster served, every apply body by path, and the last status patch.
struct Fake {
    spec: Value,
    applied: Mutex<BTreeMap<String, Value>>,
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

    let secrets = format!("/api/v1/namespaces/{NAMESPACE}/secrets/");
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
                "items": [cluster(&fake.spec)],
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
        (&Method::GET, p) if p == format!("{secrets}{MAINTAIN_KEYS}") => json_response(
            StatusCode::OK,
            &secret(MAINTAIN_KEYS, json!({ "keys": KEY_B64 })),
        ),
        (&Method::PATCH, p) if p == status => {
            *fake.status.lock().expect("lock") = Some(applied());
            json_response(StatusCode::OK, &cluster(&fake.spec))
        }
        // Every server-side apply is recorded and echoed back.
        (&Method::PATCH, p) => {
            fake.applied
                .lock()
                .expect("lock")
                .insert(p.to_string(), applied());
            json_response(StatusCode::OK, &applied())
        }
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

/// Run the operator against the fake until its first status write, then stop
/// it. Returns the maintain Deployment it applied.
async fn applied_maintain(spec: Value) -> Value {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let fake = Arc::new(Fake {
        spec,
        applied: Mutex::default(),
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

    for _ in 0..200 {
        if fake.status.lock().expect("lock").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    operator.abort();
    let path = format!("/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{INSTANCE}-maintain");
    let applied = fake.applied.lock().expect("lock");
    applied
        .get(&path)
        .cloned()
        .unwrap_or_else(|| panic!("no maintain Deployment applied: {:?}", applied.keys()))
}

fn pod_spec(deployment: &Value) -> &Value {
    &deployment["spec"]["template"]["spec"]
}

fn server_container(deployment: &Value) -> &Value {
    pod_spec(deployment)["containers"]
        .as_array()
        .expect("containers")
        .iter()
        .find(|c| c["name"] == "ravel-server")
        .expect("the ravel-server container")
}

fn scratch_volumes(deployment: &Value) -> Vec<&Value> {
    pod_spec(deployment)["volumes"]
        .as_array()
        .map(|v| v.iter().filter(|v| v["name"] == "scratch").collect())
        .unwrap_or_default()
}

fn scratch_mounts(deployment: &Value) -> Vec<&Value> {
    server_container(deployment)["volumeMounts"]
        .as_array()
        .map(|m| m.iter().filter(|m| m["name"] == "scratch").collect())
        .unwrap_or_default()
}

/// Every value following `--maintain-scratch-dir` in the container's args.
fn scratch_dir_flags(deployment: &Value) -> Vec<String> {
    let args: Vec<&str> = server_container(deployment)["args"]
        .as_array()
        .expect("args")
        .iter()
        .map(|a| a.as_str().expect("string arg"))
        .collect();
    args.windows(2)
        .filter(|w| w[0] == "--maintain-scratch-dir")
        .map(|w| w[1].to_string())
        .collect()
}

/// All four scratch renders, together, for `size` GiB at `mount`.
fn assert_scratch_rendered(deployment: &Value, size: &str, mount: &str) {
    let volumes = scratch_volumes(deployment);
    assert_eq!(volumes.len(), 1, "exactly one scratch volume: {volumes:?}");
    assert_eq!(
        volumes[0]["emptyDir"],
        json!({ "sizeLimit": size }),
        "an emptyDir on node disk limited to {size}"
    );

    let mounts = scratch_mounts(deployment);
    assert_eq!(mounts.len(), 1, "exactly one scratch mount: {mounts:?}");
    let mount_path = mounts[0]["mountPath"].as_str().expect("mountPath");
    assert_eq!(mount_path, mount);

    let flags = scratch_dir_flags(deployment);
    assert_eq!(
        flags.len(),
        1,
        "exactly one --maintain-scratch-dir: {flags:?}"
    );
    assert_eq!(
        flags[0], mount_path,
        "the flag names the directory the volume is mounted at"
    );

    assert_eq!(
        server_container(deployment)["resources"]["requests"]["ephemeral-storage"],
        json!(size),
        "the pod requests the node disk the volume may use"
    );
}

#[tokio::test]
async fn a_scratch_size_renders_the_volume_mount_and_flag() {
    let maintain = applied_maintain(spec(Some(json!({ "sizeGi": 90 })))).await;
    assert_scratch_rendered(&maintain, "90Gi", DEFAULT_MOUNT);
    // The default resource request is kept beside the added one.
    assert_eq!(
        server_container(&maintain)["resources"]["requests"],
        json!({ "cpu": "100m", "memory": "256Mi", "ephemeral-storage": "90Gi" })
    );
}

#[tokio::test]
async fn a_custom_mount_path_is_used_for_both_mount_and_flag() {
    let maintain = applied_maintain(spec(Some(
        json!({ "sizeGi": 12, "mountPath": "/scratch/merge" }),
    )))
    .await;
    assert_scratch_rendered(&maintain, "12Gi", "/scratch/merge");
}

/// The scratch size replaces an `ephemeral-storage` request set in
/// `spec.maintain.resources` and keeps every other request and limit.
#[tokio::test]
async fn the_scratch_request_replaces_an_explicit_ephemeral_storage_request() {
    let mut cluster = spec(Some(json!({ "sizeGi": 90 })));
    cluster["maintain"]["resources"] = json!({
        "requests": { "cpu": "2", "ephemeral-storage": "5Gi" },
        "limits": { "memory": "8Gi" },
    });
    let maintain = applied_maintain(cluster).await;
    assert_eq!(
        server_container(&maintain)["resources"],
        json!({
            "requests": { "cpu": "2", "ephemeral-storage": "90Gi" },
            "limits": { "memory": "8Gi" },
        })
    );
}

/// Without the field the Deployment is the one rendered before it existed,
/// and with it the four scratch renders are the only difference.
#[tokio::test]
async fn without_scratch_the_maintain_deployment_is_unchanged() {
    let without = applied_maintain(spec(None)).await;
    let explicit_null = applied_maintain(spec(Some(Value::Null))).await;
    assert_eq!(without, explicit_null);

    let rendered = without.to_string();
    for absent in ["scratch", "ephemeral-storage"] {
        assert!(
            !rendered.contains(absent),
            "{absent} rendered without spec.maintain.scratch: {rendered}"
        );
    }

    let mut with = applied_maintain(spec(Some(json!({ "sizeGi": 90 })))).await;
    assert_ne!(with, without);
    let pod = &mut with["spec"]["template"]["spec"];
    pod["volumes"]
        .as_array_mut()
        .expect("volumes")
        .retain(|v| v["name"] != "scratch");
    let container = pod["containers"]
        .as_array_mut()
        .expect("containers")
        .iter_mut()
        .find(|c| c["name"] == "ravel-server")
        .expect("the ravel-server container");
    container["volumeMounts"]
        .as_array_mut()
        .expect("volumeMounts")
        .retain(|m| m["name"] != "scratch");
    container["resources"]["requests"]
        .as_object_mut()
        .expect("requests")
        .remove("ephemeral-storage");
    let args = container["args"].as_array_mut().expect("args");
    let at = args
        .iter()
        .position(|a| a == "--maintain-scratch-dir")
        .expect("the scratch flag");
    args.drain(at..at + 2);
    assert_eq!(
        with, without,
        "removing the four scratch renders yields the Deployment without the field"
    );
}

#[test]
fn the_crd_schema_carries_minimum_one_on_scratch_size() {
    let crd = ravel_cluster_crd();
    let schema = serde_json::to_value(
        crd.spec.versions[0]
            .schema
            .as_ref()
            .expect("schema")
            .open_api_v3_schema
            .as_ref()
            .expect("root schema"),
    )
    .expect("schema serializes");
    let scratch = &schema["properties"]["spec"]["properties"]["maintain"]["properties"]["scratch"];
    assert_eq!(scratch["required"], json!(["sizeGi"]));
    assert_eq!(scratch["properties"]["sizeGi"]["minimum"], json!(1.0));
    assert_eq!(scratch["properties"]["sizeGi"]["type"], json!("integer"));
    assert_eq!(
        scratch["properties"]["mountPath"]["default"],
        json!(DEFAULT_MOUNT)
    );
    let description = scratch["description"].as_str().expect("description");
    assert!(description.contains("cache only"), "{description}");
    assert!(description.contains("1.3 times"), "{description}");
}
