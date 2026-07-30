//! The thin I/O layer: watch `RavelCluster`, render the desired objects with
//! the pure functions in [`crate::reconcile`], and apply them.
//!
//! Everything here that talks to the API server is kept as small as possible;
//! all object construction lives in [`crate::reconcile`] so it can be tested
//! without a cluster. This layer resolves the pieces that are not in the CRD
//! spec (the token Secret's tenant-name keys), stamps namespace and owner
//! references onto the rendered objects, and server-side-applies them.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Secret, Service};
use kube::api::{DeleteParams, Patch, PatchParams};
use kube::{Api, Client, Resource, ResourceExt};
use kube_runtime::controller::{Action, Controller};
use kube_runtime::watcher;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{info, warn};

use crate::crd::{Condition, LocalSecretRef, RavelCluster, RavelClusterStatus};
use crate::reconcile::{
    RenderCtx, desired_gateway_deployment, desired_gateway_service, desired_maintain_deployment,
    desired_query_deployment, desired_query_service,
};

/// Server-side-apply field manager name.
const FIELD_MANAGER: &str = "ravel-operator";

/// Requeue interval for a successful reconcile (a periodic resync in addition
/// to event-driven wakeups).
const RESYNC: Duration = Duration::from_secs(300);

/// Requeue interval after a failed reconcile.
const RETRY: Duration = Duration::from_secs(30);

/// Reconcile errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A `RavelCluster` with no namespace reached the reconciler. Namespaced
    /// objects always have one in practice; this guards the `Option`.
    #[error("RavelCluster has no namespace")]
    MissingNamespace,

    /// A Kubernetes API call failed.
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),

    /// Serializing a rendered object for apply failed.
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Shared reconcile context.
pub struct Context {
    /// Kubernetes API client.
    pub client: Client,
}

/// Read the tenant names (keys) from the token Secret named by the spec.
///
/// Returns an empty list when no token Secret is configured. Keys are sorted so
/// the rendered args and env are deterministic and do not churn the Pod
/// template on unrelated Secret map reordering.
async fn resolve_tenant_names(
    client: &Client,
    namespace: &str,
    secret_ref: Option<&LocalSecretRef>,
) -> Result<Vec<String>, Error> {
    let Some(secret_ref) = secret_ref else {
        return Ok(Vec::new());
    };
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api.get(&secret_ref.name).await?;
    let mut names: Vec<String> = Vec::new();
    if let Some(data) = secret.data {
        names.extend(data.into_keys());
    }
    if let Some(string_data) = secret.string_data {
        names.extend(string_data.into_keys());
    }
    names.sort();
    names.dedup();
    Ok(names)
}

/// Server-side-apply a typed object. The object's `apiVersion`/`kind` are
/// injected from its [`Resource`] impl because `k8s-openapi` types do not
/// serialize a `TypeMeta`, and server-side apply requires both.
async fn apply<K>(api: &Api<K>, name: &str, obj: &K) -> Result<K, Error>
where
    K: Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone + std::fmt::Debug,
{
    let mut value = serde_json::to_value(obj)?;
    if let serde_json::Value::Object(map) = &mut value {
        map.insert(
            "apiVersion".to_string(),
            serde_json::Value::String(K::api_version(&()).into_owned()),
        );
        map.insert(
            "kind".to_string(),
            serde_json::Value::String(K::kind(&()).into_owned()),
        );
    }
    let params = PatchParams::apply(FIELD_MANAGER).force();
    let applied = api.patch(name, &params, &Patch::Apply(value)).await?;
    Ok(applied)
}

/// Reconcile one `RavelCluster` to its desired Deployments and Services.
async fn reconcile(obj: Arc<RavelCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let namespace = obj.namespace().ok_or(Error::MissingNamespace)?;
    let instance = obj.name_any();
    let client = &ctx.client;

    let tenant_names = resolve_tenant_names(
        client,
        &namespace,
        obj.spec.tenant_tokens_secret_ref.as_ref(),
    )
    .await?;
    let render_ctx = RenderCtx { tenant_names };

    let owner = obj.controller_owner_ref(&()).map(|owner| vec![owner]);

    let deployments: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
    let services: Api<Service> = Api::namespaced(client.clone(), &namespace);

    // Gateway.
    let mut gateway = desired_gateway_deployment(&obj.spec, &instance, &render_ctx);
    gateway.metadata.namespace = Some(namespace.clone());
    gateway.metadata.owner_references = owner.clone();
    let gateway_applied = apply(&deployments, &child(&instance, "gateway"), &gateway).await?;

    let mut gateway_svc = desired_gateway_service(&obj.spec, &instance);
    gateway_svc.metadata.namespace = Some(namespace.clone());
    gateway_svc.metadata.owner_references = owner.clone();
    apply(&services, &child(&instance, "gateway"), &gateway_svc).await?;

    // Query.
    let mut query = desired_query_deployment(&obj.spec, &instance, &render_ctx);
    query.metadata.namespace = Some(namespace.clone());
    query.metadata.owner_references = owner.clone();
    let query_applied = apply(&deployments, &child(&instance, "query"), &query).await?;

    let mut query_svc = desired_query_service(&obj.spec, &instance);
    query_svc.metadata.namespace = Some(namespace.clone());
    query_svc.metadata.owner_references = owner.clone();
    apply(&services, &child(&instance, "query"), &query_svc).await?;

    // Maintain: apply when enabled, delete when not.
    let maintain_name = child(&instance, "maintain");
    let maintain_ready = match desired_maintain_deployment(&obj.spec, &instance) {
        Some(mut maintain) => {
            maintain.metadata.namespace = Some(namespace.clone());
            maintain.metadata.owner_references = owner.clone();
            let applied = apply(&deployments, &maintain_name, &maintain).await?;
            ready_replicas(&applied)
        }
        None => {
            // Ignore a not-found error: the Deployment may never have existed.
            if let Err(err) = deployments
                .delete(&maintain_name, &DeleteParams::default())
                .await
                && !is_not_found(&err)
            {
                return Err(err.into());
            }
            None
        }
    };

    write_status(
        client,
        &namespace,
        &instance,
        obj.metadata.generation,
        ready_replicas(&gateway_applied),
        ready_replicas(&query_applied),
        maintain_ready,
    )
    .await?;

    Ok(Action::requeue(RESYNC))
}

/// Ready-replica count from a Deployment's status, if reported yet.
fn ready_replicas(deployment: &Deployment) -> Option<i32> {
    deployment
        .status
        .as_ref()
        .and_then(|status| status.ready_replicas)
}

/// Whether a kube error is a 404 Not Found.
fn is_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(response) if response.code == 404)
}

/// `<instance>-<component>`, matching [`crate::reconcile`]'s naming.
fn child(instance: &str, component: &str) -> String {
    format!("{instance}-{component}")
}

/// Write the status subresource: observed generation, per-mode ready replicas,
/// and an `Available` condition derived from whether the gateway and query
/// tiers report ready replicas.
#[allow(clippy::too_many_arguments)]
async fn write_status(
    client: &Client,
    namespace: &str,
    instance: &str,
    observed_generation: Option<i64>,
    gateway_ready: Option<i32>,
    query_ready: Option<i32>,
    maintain_ready: Option<i32>,
) -> Result<(), Error> {
    let available = gateway_ready.unwrap_or(0) > 0 && query_ready.unwrap_or(0) > 0;
    let condition = Condition {
        r#type: "Available".to_string(),
        status: if available { "True" } else { "False" }.to_string(),
        observed_generation,
        last_transition_time: None,
        reason: if available {
            "MinimumReplicasAvailable".to_string()
        } else {
            "MinimumReplicasUnavailable".to_string()
        },
        message: if available {
            "gateway and query tiers report ready replicas".to_string()
        } else {
            "waiting for gateway and query tiers to become ready".to_string()
        },
    };
    let status = RavelClusterStatus {
        observed_generation,
        gateway_ready_replicas: gateway_ready,
        query_ready_replicas: query_ready,
        maintain_ready_replicas: maintain_ready,
        conditions: vec![condition],
    };
    let api: Api<RavelCluster> = Api::namespaced(client.clone(), namespace);
    let patch = serde_json::json!({ "status": status });
    api.patch_status(instance, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Requeue with a fixed backoff on reconcile failure.
fn error_policy(_obj: Arc<RavelCluster>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!(%error, "reconcile failed; requeueing");
    Action::requeue(RETRY)
}

/// Run the controller until the process is signalled. Builds a client from the
/// in-cluster or kubeconfig environment, watches `RavelCluster` cluster-wide,
/// and owns the Deployments and Services it creates.
pub async fn run() -> Result<(), Error> {
    let client = Client::try_default().await?;
    let clusters: Api<RavelCluster> = Api::all(client.clone());
    let deployments: Api<Deployment> = Api::all(client.clone());
    let services: Api<Service> = Api::all(client.clone());
    let context = Arc::new(Context { client });

    info!("starting ravel-operator controller");
    Controller::new(clusters, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(services, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => info!(object = ?obj, "reconciled"),
                Err(error) => warn!(%error, "reconcile stream error"),
            }
        })
        .await;
    Ok(())
}
