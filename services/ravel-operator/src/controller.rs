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
///
/// This is also the worst-case latency for the secrets-checksum pod roll
/// (`reconcile::SECRETS_CHECKSUM_ANNOTATION`): the controller watches
/// `RavelCluster` and its owned Deployments/Services, not Secrets directly, so
/// a Secret content change (a token rotation or revocation) only takes effect
/// on the next reconcile of its `RavelCluster` -- an event-driven one (a spec
/// edit) or, absent that, this periodic resync. A revoked tenant token can
/// therefore keep authenticating for up to `RESYNC` after revocation. Watching
/// Secrets directly to shrink this bound is a named follow-up, not built here.
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

    /// A Secret the spec references does not exist in the namespace. Surfaced
    /// as a `Degraded` status condition (with the Secret name) before the error
    /// propagates, so `kubectl wait`/`describe` shows why nothing came up
    /// rather than just timing out.
    #[error("secret {name} not found: {reason}")]
    SecretNotFound {
        /// The missing Secret's name.
        name: String,
        /// Why the operator needed it (which spec field pointed at it).
        reason: String,
    },

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

/// What the controller resolves from the token Secret: the tenant names (its
/// keys) and its `resourceVersion` (fed into the secrets checksum).
struct TokenSecret {
    /// Sorted, deduplicated tenant names (the Secret's keys).
    tenant_names: Vec<String>,
    /// The Secret's `resourceVersion`, or `None` when no token Secret is
    /// configured. Bumps whenever the Secret's content changes, so it is a
    /// cheap change-detection signal for the pod-template checksum.
    resource_version: Option<String>,
}

/// Read the tenant names (keys) and `resourceVersion` from the token Secret
/// named by the spec.
///
/// Returns empties when no token Secret is configured. Keys are sorted so the
/// rendered args and env are deterministic and do not churn the Pod template on
/// unrelated Secret map reordering. A missing Secret maps to
/// [`Error::SecretNotFound`] so the reconcile can surface a `Degraded` status.
async fn resolve_token_secret(
    client: &Client,
    namespace: &str,
    secret_ref: Option<&LocalSecretRef>,
) -> Result<TokenSecret, Error> {
    let Some(secret_ref) = secret_ref else {
        return Ok(TokenSecret {
            tenant_names: Vec::new(),
            resource_version: None,
        });
    };
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api
        .get(&secret_ref.name)
        .await
        .map_err(|err| secret_error(err, &secret_ref.name, "tenantTokensSecretRef"))?;
    let resource_version = secret.resource_version();
    let mut names: Vec<String> = Vec::new();
    if let Some(data) = secret.data {
        names.extend(data.into_keys());
    }
    if let Some(string_data) = secret.string_data {
        names.extend(string_data.into_keys());
    }
    names.sort();
    names.dedup();
    Ok(TokenSecret {
        tenant_names: names,
        resource_version,
    })
}

/// Read a Secret's `resourceVersion` without pulling its values into operator
/// memory. Used for the credentials Secret, whose keys are fixed
/// (`accessKeyId`/`secretAccessKey`) so only change-detection is needed.
async fn secret_resource_version(
    client: &Client,
    namespace: &str,
    name: &str,
    field: &str,
) -> Result<Option<String>, Error> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api
        .get(name)
        .await
        .map_err(|err| secret_error(err, name, field))?;
    Ok(secret.resource_version())
}

/// Map a Secret `get` error: a 404 becomes [`Error::SecretNotFound`] naming the
/// Secret and the spec field that referenced it; anything else stays a
/// [`Error::Kube`].
fn secret_error(err: kube::Error, name: &str, field: &str) -> Error {
    if is_not_found(&err) {
        Error::SecretNotFound {
            name: name.to_string(),
            reason: format!("referenced by spec.{field} but absent from the namespace"),
        }
    } else {
        Error::Kube(err)
    }
}

/// A deterministic change-detection checksum over the Secrets the pods depend
/// on, built from their `resourceVersion`s.
///
/// A Secret's `resourceVersion` changes exactly when its content changes and is
/// stable otherwise, so this value is stable across reconciles that see the
/// same Secrets (it does not churn pods every reconcile) and changes the moment
/// a token is rotated or a credential is rewritten. The hash is
/// `DefaultHasher` (SipHash with fixed keys), which is deterministic across
/// processes, so an operator restart does not roll pods. It is not a security
/// boundary, only a signal.
fn compute_secrets_checksum(token_rv: Option<&str>, credentials_rv: Option<&str>) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token_rv.unwrap_or("").hash(&mut hasher);
    credentials_rv.unwrap_or("").hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Server-side-apply a typed object. The object's `apiVersion`/`kind` are set
/// explicitly from its [`Resource`] impl as a defensive belt-and-suspenders
/// measure: server-side apply requires both, and `k8s-openapi` types do in fact
/// serialize their `TypeMeta`, so this injection writes identical values and is
/// redundant, not a workaround for a real gap. It is kept so the applied
/// document is self-describing regardless of the source object's `TypeMeta`
/// state.
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
///
/// Wraps [`reconcile_inner`] so that any failure before the success-path status
/// write still leaves a visible `Degraded` condition on `.status` (finding 3):
/// otherwise a missing Secret or an apply error leaves `.status` empty forever
/// and `kubectl wait --for=condition=Available` just times out with no reason.
/// The original error is still returned so [`error_policy`]'s retry/backoff is
/// unchanged.
async fn reconcile(obj: Arc<RavelCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let namespace = obj.namespace().ok_or(Error::MissingNamespace)?;
    let instance = obj.name_any();
    let client = &ctx.client;

    match reconcile_inner(&obj, client, &namespace, &instance).await {
        Ok(action) => Ok(action),
        Err(err) => {
            let (reason, message) = degraded_reason(&err);
            // Best-effort: if even the status write fails, log and still return
            // the original error for retry.
            if let Err(status_err) = write_degraded_status(
                client,
                &namespace,
                &instance,
                obj.metadata.generation,
                &reason,
                &message,
            )
            .await
            {
                warn!(%status_err, "failed to write Degraded status after reconcile error");
            }
            Err(err)
        }
    }
}

/// The reconcile body proper. Every fallible step here runs before the
/// success-path [`write_status`]; a failure returns `Err` to [`reconcile`],
/// which records a `Degraded` status first.
async fn reconcile_inner(
    obj: &RavelCluster,
    client: &Client,
    namespace: &str,
    instance: &str,
) -> Result<Action, Error> {
    let token_secret = resolve_token_secret(
        client,
        namespace,
        obj.spec.tenant_tokens_secret_ref.as_ref(),
    )
    .await?;
    // The credentials Secret is always referenced; read its resourceVersion so
    // a credential rotation also rolls pods via the checksum.
    let credentials_rv = secret_resource_version(
        client,
        namespace,
        &obj.spec.storage.s3.credentials_secret_ref.name,
        "storage.s3.credentialsSecretRef",
    )
    .await?;
    let secrets_checksum = compute_secrets_checksum(
        token_secret.resource_version.as_deref(),
        credentials_rv.as_deref(),
    );
    let render_ctx = RenderCtx {
        tenant_names: token_secret.tenant_names,
        secrets_checksum,
    };

    let owner = obj.controller_owner_ref(&()).map(|owner| vec![owner]);

    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);

    // Gateway.
    let mut gateway = desired_gateway_deployment(&obj.spec, instance, &render_ctx);
    gateway.metadata.namespace = Some(namespace.to_string());
    gateway.metadata.owner_references = owner.clone();
    let gateway_applied = apply(&deployments, &child(instance, "gateway"), &gateway).await?;

    let mut gateway_svc = desired_gateway_service(&obj.spec, instance);
    gateway_svc.metadata.namespace = Some(namespace.to_string());
    gateway_svc.metadata.owner_references = owner.clone();
    apply(&services, &child(instance, "gateway"), &gateway_svc).await?;

    // Query.
    let mut query = desired_query_deployment(&obj.spec, instance, &render_ctx);
    query.metadata.namespace = Some(namespace.to_string());
    query.metadata.owner_references = owner.clone();
    let query_applied = apply(&deployments, &child(instance, "query"), &query).await?;

    let mut query_svc = desired_query_service(&obj.spec, instance);
    query_svc.metadata.namespace = Some(namespace.to_string());
    query_svc.metadata.owner_references = owner.clone();
    apply(&services, &child(instance, "query"), &query_svc).await?;

    // Maintain: apply when enabled, delete when not.
    let maintain_name = child(instance, "maintain");
    let maintain_ready = match desired_maintain_deployment(&obj.spec, instance, &render_ctx) {
        Some(mut maintain) => {
            maintain.metadata.namespace = Some(namespace.to_string());
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
        namespace,
        instance,
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

/// Build a status Condition, stamping the current time as its transition time.
///
/// `last_transition_time` is a display-only Kubernetes status field, not
/// durability/correctness time, so using the system clock directly here is
/// idiomatic and correct; the injected-clock discipline (no `SystemTime::now`
/// in library logic) governs storage/query time, not an advisory Condition
/// timestamp.
fn condition(
    r#type: &str,
    status: bool,
    observed_generation: Option<i64>,
    reason: &str,
    message: &str,
) -> Condition {
    Condition {
        r#type: r#type.to_string(),
        status: if status { "True" } else { "False" }.to_string(),
        observed_generation,
        last_transition_time: Some(now_rfc3339()),
        reason: reason.to_string(),
        message: message.to_string(),
    }
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
    let condition = if available {
        condition(
            "Available",
            true,
            observed_generation,
            "MinimumReplicasAvailable",
            "gateway and query tiers report ready replicas",
        )
    } else {
        condition(
            "Available",
            false,
            observed_generation,
            "MinimumReplicasUnavailable",
            "waiting for gateway and query tiers to become ready",
        )
    };
    let status = RavelClusterStatus {
        observed_generation,
        gateway_ready_replicas: gateway_ready,
        query_ready_replicas: query_ready,
        maintain_ready_replicas: maintain_ready,
        conditions: vec![condition],
    };
    patch_status(client, namespace, instance, &status).await
}

/// Write a `Degraded` status when reconcile failed before the success-path
/// status write (finding 3), so the failure is visible on `.status` rather than
/// only in operator logs. Also flips `Available` to `False` with the same
/// reason so a `kubectl wait --for=condition=Available` fails fast with an
/// explanation instead of silently timing out.
async fn write_degraded_status(
    client: &Client,
    namespace: &str,
    instance: &str,
    observed_generation: Option<i64>,
    reason: &str,
    message: &str,
) -> Result<(), Error> {
    let status = RavelClusterStatus {
        observed_generation,
        gateway_ready_replicas: None,
        query_ready_replicas: None,
        maintain_ready_replicas: None,
        conditions: vec![
            condition("Degraded", true, observed_generation, reason, message),
            condition("Available", false, observed_generation, reason, message),
        ],
    };
    patch_status(client, namespace, instance, &status).await
}

/// Merge-patch the status subresource of `instance`.
async fn patch_status(
    client: &Client,
    namespace: &str,
    instance: &str,
    status: &RavelClusterStatus,
) -> Result<(), Error> {
    let api: Api<RavelCluster> = Api::namespaced(client.clone(), namespace);
    let patch = serde_json::json!({ "status": status });
    api.patch_status(instance, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Map a reconcile error to a `(reason, message)` for its `Degraded` condition.
/// A missing Secret gets a specific `SecretNotFound` reason naming the Secret;
/// anything else gets a generic `ReconcileError` with the error text.
fn degraded_reason(err: &Error) -> (String, String) {
    match err {
        Error::SecretNotFound { name, reason } => (
            "SecretNotFound".to_string(),
            format!("Secret \"{name}\" not found: {reason}"),
        ),
        other => ("ReconcileError".to_string(), other.to_string()),
    }
}

/// Current UTC time as an RFC3339 string for a status Condition's
/// `lastTransitionTime`. Uses the system clock directly (see [`condition`]).
fn now_rfc3339() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    format_rfc3339_utc(secs)
}

/// Format a Unix timestamp (seconds) as an RFC3339 UTC string
/// (`YYYY-MM-DDThh:mm:ssZ`). Kept dependency-free via Howard Hinnant's
/// civil-from-days algorithm so no date/time crate enters the operator for one
/// advisory status field.
fn format_rfc3339_utc(unix_secs: i64) -> String {
    let days = unix_secs.div_euclid(86_400);
    let secs_of_day = unix_secs.rem_euclid(86_400);
    let (hour, minute, second) = (
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60,
    );
    // civil_from_days: days since 1970-01-01 -> (year, month, day).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
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

    // Scope the owned-object watches to this operator's objects only. Without a
    // label selector, `.owns()` builds a cluster-wide reflector cache of every
    // Deployment and Service in the cluster; the selector keeps only the ones
    // this operator manages.
    let managed = watcher::Config::default().labels("app.kubernetes.io/managed-by=ravel-operator");

    info!("starting ravel-operator controller");
    Controller::new(clusters, watcher::Config::default())
        .owns(deployments, managed.clone())
        .owns(services, managed)
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn secrets_checksum_changes_with_content_and_is_otherwise_stable() {
        // Stable across reconciles seeing the same resourceVersions: no pod
        // churn when nothing changed.
        let a = compute_secrets_checksum(Some("100"), Some("200"));
        let a_again = compute_secrets_checksum(Some("100"), Some("200"));
        assert_eq!(a, a_again, "same inputs must yield the same checksum");

        // A token Secret content change (its resourceVersion bumps) changes the
        // checksum, so pods roll.
        let token_rotated = compute_secrets_checksum(Some("101"), Some("200"));
        assert_ne!(a, token_rotated, "token rotation must change the checksum");

        // A credentials Secret content change likewise.
        let creds_rotated = compute_secrets_checksum(Some("100"), Some("201"));
        assert_ne!(
            a, creds_rotated,
            "credential change must change the checksum"
        );

        // Absent token Secret is a distinct, stable value.
        let none = compute_secrets_checksum(None, Some("200"));
        assert_eq!(none, compute_secrets_checksum(None, Some("200")));
        assert_ne!(none, a);
    }

    #[test]
    fn rfc3339_formats_known_instants() {
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00Z");
        // 2026-07-30T12:34:56Z (Unix 1785414896).
        assert_eq!(format_rfc3339_utc(1_785_414_896), "2026-07-30T12:34:56Z");
    }

    #[test]
    fn degraded_reason_names_the_missing_secret() {
        let err = Error::SecretNotFound {
            name: "ravel-tokens".to_string(),
            reason: "referenced by spec.tenantTokensSecretRef".to_string(),
        };
        let (reason, message) = degraded_reason(&err);
        assert_eq!(reason, "SecretNotFound");
        assert!(message.contains("ravel-tokens"), "message names the Secret");
    }
}
