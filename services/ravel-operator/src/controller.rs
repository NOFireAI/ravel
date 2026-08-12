//! The thin I/O layer: watch `RavelCluster`, render the desired objects with
//! the pure functions in [`crate::reconcile`], and apply them.
//!
//! Everything here that talks to the API server is kept as small as possible;
//! all object construction lives in [`crate::reconcile`] so it can be tested
//! without a cluster. This layer resolves the pieces that are not in the CRD
//! spec (the token Secret's tenant-name keys), stamps namespace and owner
//! references onto the rendered objects, and server-side-applies them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Secret, Service};
use kube::api::{DeleteParams, Patch, PatchParams};
use kube::{Api, Client, Resource, ResourceExt};
use kube_runtime::controller::{Action, Controller};
use kube_runtime::watcher;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::s3::{S3Config, S3Store};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{info, warn};

use crate::crd::{Condition, LocalSecretRef, RavelCluster, RavelClusterSpec, RavelClusterStatus};
use crate::reconcile::{
    DEPLOYMENT_KEY_SECRET_KEY, RenderCtx, S3_ACCESS_KEY_ID_KEY, S3_SECRET_ACCESS_KEY_KEY,
    desired_gateway_deployment, desired_gateway_service, desired_maintain_deployment,
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

    /// A referenced Secret was missing a required key, or the key's value was
    /// not in the expected shape (a malformed deployment key, or non-UTF-8 S3
    /// credentials). Surfaced with the Secret name and field so `kubectl
    /// describe` names exactly what to fix.
    #[error("secret {name} field {field}: {reason}")]
    InvalidSecretValue {
        /// The Secret's name.
        name: String,
        /// The field within the Secret that was missing or malformed.
        field: String,
        /// Why the value was rejected.
        reason: String,
    },

    /// Building the S3 backend for `sys/auth` reconciliation failed.
    #[error("object store error: {0}")]
    Store(#[from] ravel_object_store::StoreError),
}

/// Shared reconcile context.
pub struct Context {
    /// Kubernetes API client.
    pub client: Client,
}

/// What the controller resolves from the token Secret: the tenant names (its
/// keys), their live token values, and the Secret's `resourceVersion` (fed
/// into the secrets checksum).
struct TokenSecret {
    /// Sorted, deduplicated tenant names (the Secret's keys).
    tenant_names: Vec<String>,
    /// The Secret's `resourceVersion`, or `None` when no token Secret is
    /// configured. Bumps whenever the Secret's content changes, so it is a
    /// cheap change-detection signal for the pod-template checksum.
    resource_version: Option<String>,
    /// Tenant name to raw token bytes, the Secret's live values. Only read
    /// into operator memory for `sys/auth` reconciliation (issue #897); the
    /// Deployment/Service render path never sees these, it only gets
    /// `tenant_names` (the token values reach pods via `$(VAR)` expansion
    /// from the Secret directly, see `reconcile::tenant_token_env`).
    token_values: BTreeMap<String, Vec<u8>>,
}

/// Read a single key's raw bytes from a Secret. `data` (already
/// base64-decoded by the API server into a [`k8s_openapi::ByteString`]) takes
/// priority; `string_data` is the write-only convenience field some manifests
/// use instead. `None` when the key is present in neither.
fn secret_value(secret: &Secret, key: &str) -> Option<Vec<u8>> {
    if let Some(bytes) = secret.data.as_ref().and_then(|d| d.get(key)) {
        return Some(bytes.0.clone());
    }
    secret
        .string_data
        .as_ref()
        .and_then(|d| d.get(key))
        .map(|s| s.as_bytes().to_vec())
}

/// Read the tenant names (keys), live token values, and `resourceVersion` from
/// the token Secret named by the spec.
///
/// Returns empties when no token Secret is configured. Names are sorted so the
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
            token_values: BTreeMap::new(),
        });
    };
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api
        .get(&secret_ref.name)
        .await
        .map_err(|err| secret_error(err, &secret_ref.name, "tenantTokensSecretRef"))?;
    let resource_version = secret.resource_version();
    let mut token_values: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    if let Some(data) = &secret.data {
        for (name, bytes) in data {
            token_values.insert(name.clone(), bytes.0.clone());
        }
    }
    if let Some(string_data) = &secret.string_data {
        for (name, value) in string_data {
            token_values.insert(name.clone(), value.as_bytes().to_vec());
        }
    }
    let tenant_names: Vec<String> = token_values.keys().cloned().collect();
    Ok(TokenSecret {
        tenant_names,
        resource_version,
        token_values,
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

/// Read the `resourceVersion` of every credential Secret the spec references,
/// keyed by Secret name: the shared `storage.s3.credentialsSecretRef` plus each
/// per-tier `credentialsSecretRef` override (ADR-0055 section 5).
///
/// Each distinct Secret is read once (two tiers pointing at the same override,
/// or an override equal to the shared credential, collapse to one `get`). A
/// missing Secret surfaces as [`Error::SecretNotFound`], the same as the shared
/// credential does, so a typo in an override name is reported rather than
/// silently ignored. The resulting map feeds each tier's pod-template checksum
/// in [`crate::reconcile`].
async fn resolve_credential_resource_versions(
    client: &Client,
    namespace: &str,
    spec: &crate::crd::RavelClusterSpec,
) -> Result<std::collections::BTreeMap<String, String>, Error> {
    let mut versions = std::collections::BTreeMap::new();
    let refs = [
        (
            spec.storage.s3.credentials_secret_ref.name.as_str(),
            "storage.s3.credentialsSecretRef",
        ),
        (
            spec.gateway
                .credentials_secret_ref
                .as_ref()
                .map(|r| r.name.as_str())
                .unwrap_or(""),
            "gateway.credentialsSecretRef",
        ),
        (
            spec.query
                .credentials_secret_ref
                .as_ref()
                .map(|r| r.name.as_str())
                .unwrap_or(""),
            "query.credentialsSecretRef",
        ),
        (
            spec.maintain
                .credentials_secret_ref
                .as_ref()
                .map(|r| r.name.as_str())
                .unwrap_or(""),
            "maintain.credentialsSecretRef",
        ),
    ];
    for (name, field) in refs {
        // Empty name means the tier has no override; skip. Already-read names
        // (shared == override, or two identical overrides) are read once.
        if name.is_empty() || versions.contains_key(name) {
            continue;
        }
        if let Some(rv) = secret_resource_version(client, namespace, name, field).await? {
            versions.insert(name.to_string(), rv);
        }
    }
    Ok(versions)
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

/// Parse a deployment key from a Secret field: either 64 hex characters or
/// exactly 32 raw bytes. Duplicated from `ravel-cli`'s `tenancy::
/// parse_deployment_key` (that crate is out of this task's scope to edit;
/// `ravel-types` exposes no public parser to share instead) rather than
/// imported, since this task's scope is `ravel-catalog`/`ravel-cli`/
/// `ravel-operator` only.
fn parse_deployment_key(raw: &[u8]) -> Result<[u8; 32], String> {
    if let Ok(text) = std::str::from_utf8(raw) {
        let trimmed = text.trim();
        if trimmed.len() == 64 && trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
            let bytes = hex::decode(trimmed).map_err(|e| format!("key is not valid hex: {e}"))?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| "hex key did not decode to 32 bytes".to_string())?;
            return Ok(arr);
        }
    }
    if raw.len() == 32 {
        let mut key = [0u8; 32];
        key.copy_from_slice(raw);
        return Ok(key);
    }
    Err(format!(
        "must contain a 32-byte deployment key: either 64 hex characters or exactly 32 raw \
         bytes (got {} bytes)",
        raw.len()
    ))
}

/// What the controller resolves from the deployment key Secret: the parsed
/// 32-byte key and the Secret's `resourceVersion`. Every tier's pod template
/// mounts this same Secret, so its `resourceVersion` feeds the shared
/// secrets checksum alongside the token and credential Secrets (review
/// finding 8, #897): a key rotation must roll pods the same way a token or
/// credential rotation does.
struct DeploymentKeySecret {
    /// The parsed key, or `None` when no deployment key Secret is configured
    /// -- the CRD's additive-opt-in state (ADR-0072 decision 4): `sys/auth`
    /// reconciliation and the keyed tenant hash both stay off.
    key: Option<[u8; 32]>,
    /// The Secret's `resourceVersion`, or `None` when no deployment key
    /// Secret is configured.
    resource_version: Option<String>,
}

/// Read and parse the deployment key from `secret_ref`'s
/// [`DEPLOYMENT_KEY_SECRET_KEY`] field, along with the Secret's
/// `resourceVersion`.
async fn resolve_deployment_key(
    client: &Client,
    namespace: &str,
    secret_ref: Option<&LocalSecretRef>,
) -> Result<DeploymentKeySecret, Error> {
    let Some(secret_ref) = secret_ref else {
        return Ok(DeploymentKeySecret {
            key: None,
            resource_version: None,
        });
    };
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api
        .get(&secret_ref.name)
        .await
        .map_err(|err| secret_error(err, &secret_ref.name, "deploymentKeySecretRef"))?;
    let resource_version = secret.resource_version();
    let raw = secret_value(&secret, DEPLOYMENT_KEY_SECRET_KEY).ok_or_else(|| {
        Error::InvalidSecretValue {
            name: secret_ref.name.clone(),
            field: DEPLOYMENT_KEY_SECRET_KEY.to_string(),
            reason: "key not present in Secret".to_string(),
        }
    })?;
    let key = parse_deployment_key(&raw).map_err(|reason| Error::InvalidSecretValue {
        name: secret_ref.name.clone(),
        field: DEPLOYMENT_KEY_SECRET_KEY.to_string(),
        reason,
    })?;
    Ok(DeploymentKeySecret {
        key: Some(key),
        resource_version,
    })
}

/// Read the shared `storage.s3.credentialsSecretRef` Secret's live
/// `accessKeyId`/`secretAccessKey` values (UTF-8 strings), for building the
/// `sys/auth` reconciliation's own S3 backend. Unlike
/// [`resolve_credential_resource_versions`], which only reads
/// `resourceVersion` for the pod-template checksum, this needs the actual
/// credential values because the operator process itself must authenticate to
/// S3 here, not just detect that the Secret changed.
async fn resolve_s3_credentials(
    client: &Client,
    namespace: &str,
    secret_ref: &LocalSecretRef,
) -> Result<(String, String), Error> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api
        .get(&secret_ref.name)
        .await
        .map_err(|err| secret_error(err, &secret_ref.name, "storage.s3.credentialsSecretRef"))?;
    let field = |key: &str| -> Result<String, Error> {
        let raw = secret_value(&secret, key).ok_or_else(|| Error::InvalidSecretValue {
            name: secret_ref.name.clone(),
            field: key.to_string(),
            reason: "key not present in Secret".to_string(),
        })?;
        String::from_utf8(raw).map_err(|_| Error::InvalidSecretValue {
            name: secret_ref.name.clone(),
            field: key.to_string(),
            reason: "value is not valid UTF-8".to_string(),
        })
    };
    Ok((
        field(S3_ACCESS_KEY_ID_KEY)?,
        field(S3_SECRET_ACCESS_KEY_KEY)?,
    ))
}

/// Build the S3 backend `sys/auth` reconciliation reads and writes, from
/// `spec.storage.s3` plus its credential Secret. Mirrors `ravel-server`'s
/// `build_store` S3 branch: `force_path_style: true` (path-style addressing,
/// what MinIO/most S3-compatible endpoints expect), `allow_http:
/// endpoint.is_some()` (only a custom endpoint may be plain HTTP; real AWS S3
/// always uses TLS), `kms_key_id: None` (no server-side KMS encryption
/// configured here).
async fn build_auth_store(
    client: &Client,
    namespace: &str,
    spec: &RavelClusterSpec,
) -> Result<S3Store, Error> {
    let (access_key_id, secret_access_key) =
        resolve_s3_credentials(client, namespace, &spec.storage.s3.credentials_secret_ref).await?;
    let endpoint = spec.storage.s3.endpoint.clone();
    let allow_http = endpoint.is_some();
    let config = S3Config {
        bucket: spec.storage.s3.bucket.clone(),
        region: spec.storage.s3.region.clone(),
        endpoint,
        access_key_id,
        secret_access_key,
        allow_http,
        force_path_style: true,
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
    };
    S3Store::new(config).map_err(Error::Store)
}

/// Bounded retry budget for a `sys/auth` primitive call that can fail with
/// [`ravel_catalog::AuthTokenMapError::CasConflict`] under a concurrent
/// writer (another operator replica's own reconcile, or a `ravel-cli` call
/// racing it): re-read, re-apply, up to this many attempts, before giving up
/// (review blocker 4, #897).
const SYS_AUTH_CAS_ATTEMPTS: u32 = 3;

/// Run a `sys/auth` primitive call up to [`SYS_AUTH_CAS_ATTEMPTS`] times,
/// retrying only on [`ravel_catalog::AuthTokenMapError::CasConflict`]: the
/// primitive itself already re-reads the map on every call, so calling `op`
/// again is a fresh read-modify-write, not a blind replay of a stale one.
/// Any other error, or a conflict on the final attempt, is returned as-is.
async fn retry_cas<F, Fut, T>(mut op: F) -> Result<T, ravel_catalog::AuthTokenMapError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ravel_catalog::AuthTokenMapError>>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        match op().await {
            Err(ravel_catalog::AuthTokenMapError::CasConflict)
                if attempt < SYS_AUTH_CAS_ATTEMPTS =>
            {
                continue;
            }
            other => return other,
        }
    }
}

/// Converge `sys/auth` to the token Secret's current contents (ADR-0072
/// decision 4, #897): upsert every tenant present in the Secret to exactly its
/// current token, then remove every *operator-managed* tenant present in
/// `sys/auth` but absent from the Secret.
///
/// Every write and removal here carries or is filtered by
/// [`ravel_catalog::MANAGED_BY_OPERATOR`] (ADR-0072 decision 4 amendment,
/// review blocker 1): a tenant `ravel-cli tenant token upsert` provisioned
/// (or an operator-adjacent tool tagged with its own `--managed-by`), and any
/// pre-amendment entry with no ownership marker at all, is never touched by
/// this pass even when absent from the Secret -- only a tenant this same
/// function previously wrote is ever revoked here.
///
/// A Secret that resolves to zero tenants -- no `tenantTokensSecretRef`
/// configured, or one configured but empty -- skips both the upsert and
/// remove passes entirely (review blocker 2) and logs one warning. Treating
/// "no tokens observed this cycle" as "revoke every operator-managed tenant"
/// would turn a missing or misconfigured Secret into a mass revocation;
/// `reconcile_inner` still reconciles workloads in this case, only `sys/auth`
/// convergence is skipped.
///
/// The remove pass reads the tenant set to remove from `sys/auth` itself
/// (durable, in object storage), never from anything this process remembered
/// from a previous cycle -- what makes revocation correct after an operator
/// restart with zero in-memory history (the #875 regression this closes).
///
/// Each primitive call is wrapped in [`retry_cas`]. A tenant whose resulting
/// entry set is already identical to its stored one resolves to
/// [`ravel_catalog::AuthSetOutcome::Unchanged`] without a write (review
/// blocker 3), so a steady-state reconcile of an unchanged Secret issues zero
/// `sys/auth` PUTs.
///
/// A tenant whose token collides with a DIFFERENT tenant's is a per-tenant
/// error, not a whole-pass one (ADR-0072 decision 4, second amendment, #897
/// flap follow-up): [`ravel_catalog::AuthTokenMapError::CrossTenantTokenCollision`]
/// is logged (tenant ids and a token fingerprint, never the token value) and
/// that one tenant is skipped this cycle, while every other tenant's upsert
/// and the remove pass below still run. This is what makes the loop
/// converge: the refused write is never retried into a takeover, so the same
/// input yields the same typed skip -- and zero `sys/auth` PUTs -- every
/// cycle, instead of two tenants trading the hash back and forth forever.
async fn reconcile_sys_auth(
    deployment_key: &[u8; 32],
    token_values: &BTreeMap<String, Vec<u8>>,
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
) -> Result<(), ravel_catalog::AuthTokenMapError> {
    if token_values.is_empty() {
        warn!(
            "sys/auth reconcile skipped: the tenant tokens Secret is absent or resolved to \
             zero tenants this cycle; an empty read is never treated as \"revoke every \
             operator-managed tenant\" (ADR-0072 decision 4 amendment, #897)"
        );
        return Ok(());
    }

    for (tenant_id, token) in token_values {
        let result = retry_cas(|| {
            ravel_catalog::replace_tenant_tokens(
                store,
                deployment_key,
                tenant_id,
                std::slice::from_ref(token),
                Some(ravel_catalog::MANAGED_BY_OPERATOR),
                now_ns,
            )
        })
        .await;
        match result {
            Ok(_) => {}
            Err(ravel_catalog::AuthTokenMapError::CrossTenantTokenCollision {
                token_fingerprint,
                existing_tenant,
                attempted_tenant,
            }) => {
                warn!(
                    %token_fingerprint,
                    %existing_tenant,
                    %attempted_tenant,
                    "sys/auth reconcile: this tenant's token collides with a different \
                     tenant's; skipping it this cycle rather than taking the token over \
                     (ADR-0072 decision 4, second amendment, #897)"
                );
            }
            Err(err) => return Err(err),
        }
    }

    let operator_managed_tenants: BTreeSet<String> =
        match ravel_catalog::read_auth_map(store, deployment_key).await? {
            Some((map, _version)) => map
                .entries
                .into_iter()
                .filter(|e| e.managed_by.as_deref() == Some(ravel_catalog::MANAGED_BY_OPERATOR))
                .map(|e| e.tenant_id)
                .collect(),
            None => BTreeSet::new(),
        };
    for tenant_id in operator_managed_tenants {
        if !token_values.contains_key(&tenant_id) {
            retry_cas(|| {
                ravel_catalog::remove_tokens_by_tenant_owned_by(
                    store,
                    deployment_key,
                    &tenant_id,
                    ravel_catalog::MANAGED_BY_OPERATOR,
                    now_ns,
                )
            })
            .await?;
        }
    }
    Ok(())
}

/// Best-effort wrapper around [`reconcile_sys_auth`]: a `sys/auth` failure
/// that survives [`retry_cas`]'s budget is logged and swallowed here, never
/// propagated (review blocker 4, #897). `reconcile_inner` calls this without
/// `?` -- the `()` return type is itself the enforcement that a sys/auth
/// failure cannot abort Deployment/Service reconciliation, not just a
/// convention a future edit could accidentally break.
async fn reconcile_sys_auth_best_effort(
    deployment_key: &[u8; 32],
    token_values: &BTreeMap<String, Vec<u8>>,
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
) {
    if let Err(err) = reconcile_sys_auth(deployment_key, token_values, store, now_ns).await {
        warn!(
            %err,
            "sys/auth reconcile failed after exhausting its retry budget; continuing to \
             Deployment/Service reconciliation without it (review blocker 4, #897)"
        );
    }
}

/// Current Unix time in nanoseconds, for `sys/auth` CAS-record timestamps.
/// Direct `SystemTime::now()` use is acceptable here: this is the binary/
/// wiring layer (analogous to `ravel-cli`'s own `now_ns()`), not the injected-
/// clock-only "library logic" the testing-patterns rule governs.
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
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

    // Converge sys/auth to the token Secret whenever a deployment key is
    // configured (ADR-0072 decision 4, #897): this is the first in-process
    // writer of the durable bearer-token map, and it must run every cycle --
    // not just on a token Secret change -- so an operator-managed tenant
    // removed from the Secret is revoked even after an operator restart wiped
    // any in-memory history of what used to be there (the #875 regression).
    // Best-effort (review blocker 4): a sys/auth failure that survives its
    // retry budget is logged, never propagated, so it cannot block the
    // Deployment/Service reconciliation below.
    let deployment_key_secret = resolve_deployment_key(
        client,
        namespace,
        obj.spec.deployment_key_secret_ref.as_ref(),
    )
    .await?;
    if let Some(deployment_key) = deployment_key_secret.key {
        let store = build_auth_store(client, namespace, &obj.spec).await?;
        reconcile_sys_auth_best_effort(
            &deployment_key,
            &token_secret.token_values,
            &store,
            now_ns(),
        )
        .await;
    }

    // Read the resourceVersion of every credential Secret the spec references:
    // the shared storage.s3 credential plus any per-tier override (ADR-0055
    // section 5). Each Deployment's pod-template checksum is later computed (in
    // reconcile) from the one Secret its tier resolves to, so a per-role
    // credential rotation rolls only the Deployment(s) that consume it. The
    // shared credential is always referenced (some tier falls back to it unless
    // all three override); reading it always keeps the no-override path
    // identical to before.
    let credential_resource_versions =
        resolve_credential_resource_versions(client, namespace, &obj.spec).await?;
    let render_ctx = RenderCtx {
        tenant_names: token_secret.tenant_names,
        token_resource_version: token_secret.resource_version,
        credential_resource_versions,
        deployment_key_resource_version: deployment_key_secret.resource_version,
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
        Error::InvalidSecretValue {
            name,
            field,
            reason,
        } => (
            "InvalidSecretValue".to_string(),
            format!("Secret \"{name}\" field \"{field}\": {reason}"),
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

    #[test]
    fn parse_deployment_key_accepts_hex_and_raw_bytes() {
        let hex_key = "ab".repeat(32);
        assert_eq!(
            parse_deployment_key(hex_key.as_bytes()).expect("hex key parses"),
            [0xab; 32]
        );
        assert_eq!(
            parse_deployment_key(&[7u8; 32]).expect("raw key parses"),
            [7u8; 32]
        );
        assert!(parse_deployment_key(b"too short").is_err());
    }

    fn memory_store() -> ravel_object_store::memory::MemoryStore {
        ravel_object_store::memory::MemoryStore::new()
    }

    #[tokio::test]
    async fn reconcile_sys_auth_upserts_every_tenant_in_the_secret() {
        let store = memory_store();
        let deployment_key = [1u8; 32];
        let mut token_values = BTreeMap::new();
        token_values.insert("tenant-a".to_string(), b"token-a".to_vec());
        token_values.insert("tenant-b".to_string(), b"token-b".to_vec());

        reconcile_sys_auth(&deployment_key, &token_values, &store, 1_000)
            .await
            .expect("reconcile succeeds");

        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        let mut tenants: Vec<&str> = map.entries.iter().map(|e| e.tenant_id.as_str()).collect();
        tenants.sort();
        assert_eq!(tenants, vec!["tenant-a", "tenant-b"]);
    }

    /// #875 regression pin: a tenant present in `sys/auth` (written here to
    /// simulate a token the operator itself upserted in a *previous* reconcile
    /// cycle, by a process that has since restarted) but absent from the
    /// current token Secret must be revoked. This runs against a freshly
    /// constructed `MemoryStore` with no reconcile history in this test's own
    /// process state -- `reconcile_sys_auth` learns "tenant-gone used to have
    /// a token" only by reading `sys/auth` itself, never from anything an
    /// earlier call left in memory, which is what makes this correct after an
    /// operator restart. Seeded with `MANAGED_BY_OPERATOR` (review blocker 1,
    /// #897): only an entry the operator itself owns is ever revoked by this
    /// pass.
    #[tokio::test]
    async fn reconcile_sys_auth_revokes_a_tenant_missing_from_a_fresh_secret_read() {
        let store = memory_store();
        let deployment_key = [2u8; 32];

        // Simulate a previous cycle's write: tenant-gone had a token, before
        // this (fresh) process ever ran.
        ravel_catalog::upsert_token_owned(
            &store,
            &deployment_key,
            b"stale-token",
            "tenant-gone",
            Some(ravel_catalog::MANAGED_BY_OPERATOR),
            500,
        )
        .await
        .expect("seed write succeeds");

        // The current token Secret no longer mentions tenant-gone.
        let mut token_values = BTreeMap::new();
        token_values.insert("tenant-still-here".to_string(), b"token-x".to_vec());

        reconcile_sys_auth(&deployment_key, &token_values, &store, 1_000)
            .await
            .expect("reconcile succeeds");

        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        assert!(
            map.entries.iter().all(|e| e.tenant_id != "tenant-gone"),
            "tenant-gone must be revoked even though this process never saw its token \
             upserted"
        );
        assert!(
            map.entries
                .iter()
                .any(|e| e.tenant_id == "tenant-still-here"),
            "tenant-still-here must remain"
        );
    }

    #[tokio::test]
    async fn reconcile_sys_auth_rotates_a_tenants_token() {
        let store = memory_store();
        let deployment_key = [3u8; 32];

        let mut first = BTreeMap::new();
        first.insert("tenant-a".to_string(), b"old-token".to_vec());
        reconcile_sys_auth(&deployment_key, &first, &store, 1_000)
            .await
            .expect("first reconcile succeeds");

        let mut rotated = BTreeMap::new();
        rotated.insert("tenant-a".to_string(), b"new-token".to_vec());
        reconcile_sys_auth(&deployment_key, &rotated, &store, 2_000)
            .await
            .expect("second reconcile succeeds");

        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        assert_eq!(
            map.entries.len(),
            1,
            "old token must be gone, not just supplemented"
        );
        let old_hash = ravel_catalog::token_hash(&deployment_key, b"old-token");
        let new_hash = ravel_catalog::token_hash(&deployment_key, b"new-token");
        assert!(map.entries.iter().all(|e| e.token_hash != old_hash));
        assert!(map.entries.iter().any(|e| e.token_hash == new_hash));
    }

    /// Review blocker 1 (#897): the remove pass must touch only entries the
    /// operator itself manages. A CLI-provisioned tenant and a pre-amendment
    /// unmanaged entry both survive a reconcile whose Secret does not name
    /// them; an operator-managed tenant absent from the Secret is still
    /// revoked.
    #[tokio::test]
    async fn reconcile_sys_auth_only_removes_operator_managed_tenants() {
        let store = memory_store();
        let deployment_key = [9u8; 32];

        // The operator's own first reconcile: creates tenant-op tagged
        // MANAGED_BY_OPERATOR.
        let mut first = BTreeMap::new();
        first.insert("tenant-op".to_string(), b"op-token".to_vec());
        reconcile_sys_auth(&deployment_key, &first, &store, 1_000)
            .await
            .expect("first reconcile succeeds");

        // A CLI-provisioned tenant the operator never managed.
        ravel_catalog::upsert_token_owned(
            &store,
            &deployment_key,
            b"cli-token",
            "tenant-cli",
            Some(ravel_catalog::MANAGED_BY_CLI),
            1_000,
        )
        .await
        .expect("cli upsert succeeds");

        // A v1-shaped unmanaged entry (a pre-amendment writer).
        ravel_catalog::upsert_token(
            &store,
            &deployment_key,
            b"legacy-token",
            "tenant-legacy",
            1_000,
        )
        .await
        .expect("legacy upsert succeeds");

        // The next cycle's Secret names none of the three tenants above.
        let mut second = BTreeMap::new();
        second.insert("tenant-other".to_string(), b"other-token".to_vec());
        reconcile_sys_auth(&deployment_key, &second, &store, 2_000)
            .await
            .expect("second reconcile succeeds");

        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        let tenants: BTreeSet<&str> = map.entries.iter().map(|e| e.tenant_id.as_str()).collect();
        assert!(
            !tenants.contains("tenant-op"),
            "an operator-managed tenant absent from the Secret must still be revoked"
        );
        assert!(
            tenants.contains("tenant-cli"),
            "a CLI-provisioned tenant must survive an operator reconcile that never managed it"
        );
        assert!(
            tenants.contains("tenant-legacy"),
            "an unmanaged (pre-amendment) entry must survive an operator reconcile"
        );
        assert!(
            tenants.contains("tenant-other"),
            "a tenant present in the current Secret must exist"
        );
    }

    /// Review blocker 2 (#897): a deployment key configured with no token
    /// Secret (or one that resolves to zero tenants) must leave `sys/auth`
    /// untouched, not wipe every operator-managed tenant.
    #[tokio::test]
    async fn reconcile_sys_auth_skips_entirely_on_an_empty_secret() {
        let store = memory_store();
        let deployment_key = [11u8; 32];

        let mut first = BTreeMap::new();
        first.insert("tenant-op".to_string(), b"op-token".to_vec());
        reconcile_sys_auth(&deployment_key, &first, &store, 1_000)
            .await
            .expect("first reconcile succeeds");

        let empty = BTreeMap::new();
        reconcile_sys_auth(&deployment_key, &empty, &store, 2_000)
            .await
            .expect("reconcile against an empty secret is a clean no-op");

        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        assert!(
            map.entries.iter().any(|e| e.tenant_id == "tenant-op"),
            "an empty secret read must never wipe sys/auth: {map:?}"
        );
    }

    /// Review blocker 3 (#897): two consecutive reconciles against an
    /// unchanged Secret must perform zero additional `sys/auth` PUTs on the
    /// second run. A `Sequence` of passthrough steps on `Op::Put` against the
    /// `sys/auth` key counts every PUT that actually reaches the backend,
    /// standing in for the request counter `MemoryStore` does not expose
    /// publicly.
    #[tokio::test]
    async fn reconcile_sys_auth_is_a_no_op_write_on_an_unchanged_secret() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};

        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains(ravel_catalog::AUTH_KEY)
                .with_steps(vec![SequenceStep::Passthrough; 10]),
        );
        let store = FaultStore::new(memory_store(), plan);
        let deployment_key = [12u8; 32];

        let mut tokens = BTreeMap::new();
        tokens.insert("tenant-a".to_string(), b"token-a".to_vec());

        reconcile_sys_auth(&deployment_key, &tokens, &store, 1_000)
            .await
            .expect("first reconcile succeeds");
        let puts_after_first = store.sequence_progress(0);
        assert!(
            puts_after_first > 0,
            "the first reconcile of a fresh tenant must write at least once"
        );

        reconcile_sys_auth(&deployment_key, &tokens, &store, 2_000)
            .await
            .expect("second reconcile, unchanged secret, succeeds");
        let puts_after_second = store.sequence_progress(0);
        assert_eq!(
            puts_after_second, puts_after_first,
            "an unchanged secret must perform exactly zero additional sys/auth PUTs"
        );
    }

    /// The flap acceptance test (#897, ADR-0072 decision 4 second amendment):
    /// two tenants, "acme" and "globex", both present in the Secret with the
    /// SAME token value. Against the prior round's last-writer-wins takeover,
    /// each identical reconcile cycle would take the hash back for whichever
    /// tenant runs last (alphabetical `BTreeMap` order: "acme" then
    /// "globex"), so cycle two would look identical to cycle one on the
    /// surface (one entry, `Ok(())`) while actually performing a full
    /// `sys/auth` PUT every single cycle forever -- the flap. This test
    /// proves convergence directly on that PUT count: a SECOND identical
    /// reconcile must perform ZERO additional `sys/auth` PUTs, using the same
    /// `Sequence`-of-passthroughs counter as the review-blocker-3 no-op test
    /// above, because it stands in for a PUT counter `MemoryStore` does not
    /// expose publicly.
    ///
    /// Flipped line: with the prior round's `replace_tenant_tokens`, the
    /// `collides_with_desired` retain predicate matched on hash alone, so
    /// "globex" would evict "acme"'s entry and take the hash over on cycle
    /// one, and "acme" would take it back on cycle two -- `puts_after_second`
    /// would be strictly greater than `puts_after_first`, not equal.
    #[tokio::test]
    async fn reconcile_sys_auth_cross_tenant_collision_converges_with_zero_puts_on_repeat() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};

        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains(ravel_catalog::AUTH_KEY)
                .with_steps(vec![SequenceStep::Passthrough; 10]),
        );
        let store = FaultStore::new(memory_store(), plan);
        let deployment_key = [21u8; 32];

        let mut tokens = BTreeMap::new();
        tokens.insert("acme".to_string(), b"shared-tok".to_vec());
        tokens.insert("globex".to_string(), b"shared-tok".to_vec());

        reconcile_sys_auth(&deployment_key, &tokens, &store, 1_000)
            .await
            .expect("a cross-tenant collision is a per-tenant skip, not a whole-pass error");
        let puts_after_first = store.sequence_progress(0);
        assert!(
            puts_after_first > 0,
            "the winning tenant's first write must still happen"
        );

        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        assert_eq!(
            map.entries.len(),
            1,
            "exactly one tenant holds the shared token; the other was refused, not taken over"
        );
        assert_eq!(
            ravel_catalog::tenant_for_token(&map, &deployment_key, b"shared-tok"),
            Some("acme"),
            "BTreeMap iterates \"acme\" before \"globex\": acme, the first writer this cycle, \
             keeps the token"
        );

        reconcile_sys_auth(&deployment_key, &tokens, &store, 2_000)
            .await
            .expect("second, identical reconcile still succeeds");
        let puts_after_second = store.sequence_progress(0);
        assert_eq!(
            puts_after_second, puts_after_first,
            "an identical second cycle must perform zero additional sys/auth PUTs: acme is \
             already converged (Unchanged, no write) and globex is refused again, identically, \
             never taking the hash over -- the exact property last-writer-wins takeover violated"
        );
    }

    /// A cross-tenant collision must not brick the rest of the reconcile
    /// pass (ADR-0072 decision 4, second amendment, #897): a third,
    /// unrelated tenant in the same Secret still gets its token upserted
    /// even though "acme" and "globex" collide with each other.
    /// `reconcile_sys_auth_best_effort_swallows_a_persistent_cas_conflict`
    /// above already pins that a `sys/auth` failure never blocks
    /// `reconcile_inner`'s Deployment/Service reconciliation downstream of
    /// this function; this test pins the sibling property one layer in --
    /// one tenant's refusal does not block ITS OWN sibling tenants within
    /// the same `sys/auth` reconcile call.
    #[tokio::test]
    async fn reconcile_sys_auth_cross_tenant_collision_does_not_block_other_tenants() {
        let store = memory_store();
        let deployment_key = [22u8; 32];

        let mut tokens = BTreeMap::new();
        tokens.insert("acme".to_string(), b"shared-tok".to_vec());
        tokens.insert("globex".to_string(), b"shared-tok".to_vec());
        tokens.insert("tenant-healthy".to_string(), b"healthy-tok".to_vec());

        reconcile_sys_auth(&deployment_key, &tokens, &store, 1_000)
            .await
            .expect("the colliding pair must not abort the rest of the reconcile");

        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        assert!(
            map.entries.iter().any(|e| e.tenant_id == "tenant-healthy"),
            "an unrelated tenant in the same Secret must still be upserted: {map:?}"
        );
        assert_eq!(
            map.entries.len(),
            2,
            "the winner of the colliding pair, plus the healthy tenant; the loser was refused"
        );
    }

    /// Review blocker 4 (#897): a transient CAS conflict on the first write
    /// must be absorbed by `retry_cas` and still converge within the retry
    /// budget.
    #[tokio::test]
    async fn reconcile_sys_auth_retries_a_transient_cas_conflict() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
                .with_key_contains(ravel_catalog::AUTH_KEY)
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(memory_store(), plan);
        let deployment_key = [13u8; 32];

        let mut tokens = BTreeMap::new();
        tokens.insert("tenant-a".to_string(), b"token-a".to_vec());

        reconcile_sys_auth(&deployment_key, &tokens, &store, 1_000)
            .await
            .expect("reconcile converges despite one transient CAS conflict");

        assert_eq!(
            store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            1,
            "the injected conflict must actually have fired"
        );
        let (map, _version) = ravel_catalog::read_auth_map(&store, &deployment_key)
            .await
            .expect("read succeeds")
            .expect("map exists");
        assert!(map.entries.iter().any(|e| e.tenant_id == "tenant-a"));
    }

    /// Review blocker 4 (#897): a persistent CAS conflict (every write fails)
    /// exhausts the retry budget but `reconcile_sys_auth_best_effort` still
    /// returns cleanly rather than propagating -- the property that lets
    /// `reconcile_inner` call it without `?` and keep reconciling
    /// Deployments/Services regardless of `sys/auth`'s outcome.
    #[tokio::test]
    async fn reconcile_sys_auth_best_effort_swallows_a_persistent_cas_conflict() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
                .with_key_contains(ravel_catalog::AUTH_KEY)
                .with_occurrence(Occurrence::Always),
        );
        let store = FaultStore::new(memory_store(), plan);
        let deployment_key = [14u8; 32];

        let mut tokens = BTreeMap::new();
        tokens.insert("tenant-a".to_string(), b"token-a".to_vec());

        // Must return () and must not panic: this is the decoupling itself.
        reconcile_sys_auth_best_effort(&deployment_key, &tokens, &store, 1_000).await;

        assert_eq!(
            store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            u64::from(SYS_AUTH_CAS_ATTEMPTS),
            "the full retry budget must be spent, not abandoned early"
        );
    }

    #[test]
    fn secret_value_prefers_data_over_string_data() {
        let mut secret = Secret::default();
        let mut data = BTreeMap::new();
        data.insert(
            "key".to_string(),
            k8s_openapi::ByteString(b"from-data".to_vec()),
        );
        secret.data = Some(data);
        let mut string_data = BTreeMap::new();
        string_data.insert("key".to_string(), "from-string-data".to_string());
        secret.string_data = Some(string_data);

        assert_eq!(secret_value(&secret, "key"), Some(b"from-data".to_vec()));
        assert_eq!(secret_value(&secret, "missing"), None);
    }
}
