//! Pure rendering of `RavelCluster` specs into Kubernetes objects (ADR-0034
//! decision 3).
//!
//! Every function here is a pure function of its inputs: it performs no I/O,
//! needs no tokio runtime, and returns the exact object the reconcile loop
//! should apply. All the actual `Api::patch` calls live in
//! [`crate::controller`], which calls these functions and applies their
//! results. This split is deliberate and load-bearing: there is no live API
//! server (nor a Rust `envtest`) in this environment, so the only way to test
//! object construction is to keep it free of I/O and assert on the returned
//! structs directly (see the tests at the bottom of this file).

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy};
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Affinity, Capabilities, Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction,
    KeyToPath, PodAffinityTerm, PodAntiAffinity, PodSecurityContext, PodSpec, PodTemplateSpec,
    Probe, ResourceRequirements, SeccompProfile, SecretKeySelector, SecretVolumeSource,
    SecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec, Volume, VolumeMount,
    WeightedPodAffinityTerm,
};
use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
    IngressServiceBackend, IngressSpec, IngressTLS, ServiceBackendPort,
};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::api::rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};

use crate::crd::{
    AffinityBackend, AffinityKeySource, GatewayReference, IngestAffinitySpec, LocalSecretRef,
    RavelClusterSpec, ResourceRequirementsSpec,
};

/// A spec that cannot be rendered into a valid object set.
///
/// Surfaced by [`desired_objects`] (and the router renderers it calls) rather
/// than panicking or emitting a broken object, so the controller records a
/// `Degraded` status the same way a missing Secret does (ADR-0080 decision 3,
/// deliverable 1). Kept typed and `PartialEq` so a reconcile unit test can
/// assert the exact variant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    /// `backend: ravelNative` is selected but `ingestAffinity.routerImage` is
    /// unset. The router is a different binary from `spec.image`, so there is no
    /// image to fall back to; rendering a Deployment with an empty `image:`
    /// would never schedule and hide the misconfiguration.
    #[error(
        "gateway.ingestAffinity.backend is ravelNative but gateway.ingestAffinity.routerImage \
         is not set; set routerImage to the ravel-ingest-router container image"
    )]
    RouterImageMissing,

    /// `key.source: canonicalTenant` is selected but no resolver is configured.
    /// `ravel-ingest-router`'s own CLI refuses to start with `--key-source
    /// canonical-tenant` unless at least one resolver flag is set (a
    /// `--tenant-token`, `--oidc-issuer`/`--oidc-jwks-url`, `--mtls-enabled`, or
    /// `--dev-insecure-tenant-header`). The only resolver surface this CRD
    /// exposes today is `tenantTokensSecretRef`, which renders the
    /// `--tenant-token` flags; with it absent the rendered command line would
    /// crashloop the router at startup, so the operator renders no router
    /// objects and surfaces this instead (mirrors [`Self::RouterImageMissing`]).
    #[error(
        "gateway.ingestAffinity.key.source is canonicalTenant but no resolver is configured; \
         set tenantTokensSecretRef so the router renders at least one --tenant-token resolver \
         (canonical-tenant cannot start with no resolver)"
    )]
    CanonicalTenantResolverMissing,

    /// A `spec.gc` horizon field holds a value that is not a valid humantime
    /// duration (or is empty). Rendering it verbatim would put a flag on the
    /// maintain pod that `ravel-server` rejects at startup, crash-looping the
    /// tier; the render fails here instead so the controller records a
    /// `Degraded` condition whose message names the offending field. `field` is
    /// the camelCase CRD field name (`protectionHorizon` or `grace`).
    #[error(
        "spec.gc.{field} value {value:?} is not a valid humantime duration; use a duration such \
         as 24h or 25h5m matching the value stored in sys/gc (read it with ravel-cli gc-config \
         show)"
    )]
    GcHorizonInvalid {
        /// The camelCase CRD field name that failed to validate.
        field: &'static str,
        /// The rejected value, as written in the spec.
        value: String,
    },
}

/// HTTP listener port (OTLP/HTTP, query API, and the `/healthz` `/readyz`
/// probes). Fixed to match the Dockerfile's `EXPOSE` and the server's default.
pub const HTTP_PORT: i32 = 4318;

/// gRPC listener port (OTLP/gRPC), exposed by the gateway tier only.
pub const GRPC_PORT: i32 = 4317;

/// Secret key holding the S3 access key id.
pub(crate) const S3_ACCESS_KEY_ID_KEY: &str = "accessKeyId";
/// Secret key holding the S3 secret access key.
pub(crate) const S3_SECRET_ACCESS_KEY_KEY: &str = "secretAccessKey";

/// Pod-template annotation carrying a checksum of the Secrets the pod depends
/// on (credentials and, where applicable, tenant tokens). Changing it changes
/// the pod template, which makes the Deployment controller roll a new
/// ReplicaSet, so a Secret value change (e.g. a token rotation or revocation)
/// takes effect on running pods. The controller computes the value (see
/// [`crate::controller`]) and threads it in through [`RenderCtx`].
pub const SECRETS_CHECKSUM_ANNOTATION: &str = "ravel.nofire.ai/secrets-checksum";

/// Inputs the controller resolves from the cluster that are not part of the
/// CRD spec, threaded into the otherwise-pure render functions.
///
/// `tenant_names` are the keys of the `tenantTokensSecretRef` Secret (the
/// tenant names). They are not in the spec — the spec only names the Secret —
/// so the controller reads them (RBAC grants `get` on Secrets) and passes them
/// here. Keeping them a plain input rather than an I/O call inside the render
/// keeps these functions pure and testable.
///
/// The credential/token `resourceVersion`s feed the per-Deployment
/// [`SECRETS_CHECKSUM_ANNOTATION`]. Each Deployment's checksum is computed from
/// the Secrets that Deployment actually consumes: the shared token Secret plus
/// the one credential Secret its tier resolves to (its own
/// `credentialsSecretRef` override, or the shared `storage.s3.credentialsSecretRef`
/// when unset). A `resourceVersion` changes exactly when a Secret's content
/// changes, so a per-role credential rotation rolls only the Deployment(s) that
/// consume that Secret (ADR-0055 section 5). The controller reads these
/// (it holds the API client); a pure render function must never do I/O.
#[derive(Debug, Clone, Default)]
pub struct RenderCtx {
    /// Tenant names (the token Secret's keys), rendered in the given order.
    pub tenant_names: Vec<String>,

    /// `resourceVersion` of the tenant-token Secret, shared by every tier, or
    /// `None` when no token Secret is configured.
    pub token_resource_version: Option<String>,

    /// `resourceVersion` of each credential Secret the spec references, keyed by
    /// Secret name: the shared `storage.s3.credentialsSecretRef` plus any
    /// per-tier `credentialsSecretRef` override. A Secret whose `resourceVersion`
    /// could not be read is simply absent from the map (its checksum component
    /// is then the empty string, matching the pre-existing "no version" case).
    pub credential_resource_versions: BTreeMap<String, String>,

    /// `resourceVersion` of the `deploymentKeySecretRef` Secret, or `None` when
    /// no deployment key is configured. Every pod that mounts the deployment
    /// key must roll when it rotates, the same as a
    /// credential or tenant-token rotation does; folded into
    /// [`secrets_checksum`] alongside the other two.
    pub deployment_key_resource_version: Option<String>,

    /// `resourceVersion` of the Secret named by `auditTokenKeySecretRef`
    /// (#1487), or `None` when the cluster has a `deploymentKeySecretRef`
    /// (the server derives the key from it, no Secret to read) or when
    /// neither ref is set (nothing to read; the query tier's apply is
    /// withheld for that pass, see [`crate::controller`]). Folded only into
    /// the query tier's checksum: gateway and maintain never read this
    /// Secret, so their checksums must not move when it rotates.
    pub audit_token_key_resource_version: Option<String>,
}

/// Resolve the credential Secret name a tier consumes: its own
/// `credentialsSecretRef` override when present, otherwise the shared
/// `storage.s3.credentialsSecretRef` (ADR-0055 section 5). This is the single
/// place the override-else-shared fallback lives, so the credential env
/// ([`s3_credential_env`]) and the change-detection checksum
/// ([`tier_secrets_checksum`]) can never disagree about which Secret a tier
/// actually uses.
fn tier_credentials_secret_name<'a>(
    spec: &'a RavelClusterSpec,
    tier_override: Option<&'a LocalSecretRef>,
) -> &'a str {
    tier_override
        .map(|r| r.name.as_str())
        .unwrap_or(&spec.storage.s3.credentials_secret_ref.name)
}

/// A deterministic change-detection checksum over the Secrets a tier
/// consumes: the shared token Secret, the tier's resolved credential Secret,
/// and the deployment-key Secret (every tier mounts the same one, when
/// configured).
///
/// Built from their `resourceVersion`s, which change exactly when a Secret's
/// content changes and are stable otherwise, so this value is stable across
/// reconciles that see the same Secrets (no pod churn) and changes the moment a
/// token is rotated, the tier's credential is rewritten, or the deployment key
/// is rotated. The hash is `blake3`, a fixed algorithm, so the value depends
/// only on the inputs and is stable across processes AND Rust toolchain
/// versions: an operator restart or a compiler upgrade does not roll pods.
/// (std's `DefaultHasher` is not stable across Rust releases, so an upgrade
/// would have rolled every tier Deployment for unchanged inputs.) It is not a
/// security boundary, only a signal. Stamped onto the tier's pod template as
/// [`SECRETS_CHECKSUM_ANNOTATION`]; when tiers resolve to different credential
/// Secrets, a change to one rolls only the tier(s) that consume it.
pub fn secrets_checksum(
    token_rv: Option<&str>,
    credentials_rv: Option<&str>,
    deployment_key_rv: Option<&str>,
    audit_token_key_rv: Option<&str>,
) -> String {
    // The audit-token-key field is folded in only when `Some` (#1487 rework):
    // a cluster with no audit-token-key Secret in play must hash the same
    // three fields main did before this field existed, or every such
    // gateway/maintain checksum (and every unkeyed query checksum) would
    // change on the operator upgrade alone and roll pods for no Secret
    // rotation. `blake3_hex`'s `0xff` separator makes the three- and
    // four-field forms genuinely different hashes, not just cosmetically
    // different calls, so this is not a no-op either way.
    let mut fields = vec![
        token_rv.unwrap_or(""),
        credentials_rv.unwrap_or(""),
        deployment_key_rv.unwrap_or(""),
    ];
    if let Some(rv) = audit_token_key_rv {
        fields.push(rv);
    }
    blake3_hex(&fields)
}

/// A stable hex digest of an ordered list of string fields, using `blake3`
/// (finding 3, issue #36). Each field is fed followed by a `0xff` separator
/// byte, which cannot appear in UTF-8, so no concatenation of adjacent fields
/// can collide with a different split (`"ab" + "c"` differs from `"a" + "bc"`).
/// The digest is rendered as lowercase hex. Used by [`secrets_checksum`] and
/// [`qualify_job_input_hash`]; both are change signals, not security
/// boundaries.
fn blake3_hex(fields: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for field in fields {
        hasher.update(field.as_bytes());
        hasher.update(&[0xff]);
    }
    hasher.finalize().to_hex().to_string()
}

/// The [`SECRETS_CHECKSUM_ANNOTATION`] value for a tier, resolving which
/// credential Secret the tier consumes and hashing its `resourceVersion`
/// together with the token Secret's and the deployment-key Secret's.
///
/// `audit_key_rv` is folded in only by the query tier's call site (#1487):
/// gateway and maintain always pass `None` here regardless of
/// `ctx.audit_token_key_resource_version`, so a rotation of that Secret never
/// moves their checksum.
fn tier_secrets_checksum(
    spec: &RavelClusterSpec,
    ctx: &RenderCtx,
    tier_override: Option<&LocalSecretRef>,
    audit_key_rv: Option<&str>,
) -> String {
    let secret_name = tier_credentials_secret_name(spec, tier_override);
    let credentials_rv = ctx
        .credential_resource_versions
        .get(secret_name)
        .map(String::as_str);
    secrets_checksum(
        ctx.token_resource_version.as_deref(),
        credentials_rv,
        ctx.deployment_key_resource_version.as_deref(),
        audit_key_rv,
    )
}

/// Standard object labels for a component of a named `RavelCluster`.
fn labels(instance: &str, component: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), "ravel".to_string()),
        (
            "app.kubernetes.io/instance".to_string(),
            instance.to_string(),
        ),
        (
            "app.kubernetes.io/component".to_string(),
            component.to_string(),
        ),
        (
            "app.kubernetes.io/managed-by".to_string(),
            "ravel-operator".to_string(),
        ),
    ])
}

/// Name of a component's child object: `<cluster>-<component>`.
fn child_name(instance: &str, component: &str) -> String {
    format!("{instance}-{component}")
}

/// The env vars every mode needs: S3 credentials sourced from the credentials
/// Secret via `valueFrom.secretKeyRef` (never literal values).
///
/// `RAVEL_S3_ACCESS_KEY` and `RAVEL_S3_SECRET_KEY` are read directly from the
/// environment by `ravel-server` (clap `env`), so no `$(VAR)` argument trick is
/// needed for these two: the env vars alone configure the store.
///
/// `tier_override` is the calling tier's own `credentialsSecretRef` (ADR-0055
/// section 5): when `Some`, the tier sources its credentials from that Secret;
/// when `None`, it falls back to the shared `storage.s3.credentialsSecretRef`,
/// so a spec with no overrides renders exactly as before.
fn s3_credential_env(
    spec: &RavelClusterSpec,
    tier_override: Option<&LocalSecretRef>,
) -> Vec<EnvVar> {
    let secret = tier_credentials_secret_name(spec, tier_override);
    vec![
        EnvVar {
            name: "RAVEL_S3_ACCESS_KEY".to_string(),
            value_from: Some(secret_key_env(secret, S3_ACCESS_KEY_ID_KEY)),
            ..Default::default()
        },
        EnvVar {
            name: "RAVEL_S3_SECRET_KEY".to_string(),
            value_from: Some(secret_key_env(secret, S3_SECRET_ACCESS_KEY_KEY)),
            ..Default::default()
        },
    ]
}

/// An `EnvVarSource` reading `key` from Secret `secret`.
fn secret_key_env(secret: &str, key: &str) -> EnvVarSource {
    EnvVarSource {
        secret_key_ref: Some(SecretKeySelector {
            name: secret.to_string(),
            key: key.to_string(),
            optional: Some(false),
        }),
        ..Default::default()
    }
}

/// Per-tenant token env vars: `RAVEL_TENANT_TOKEN_<i>` sourced from the token
/// Secret's `<tenant>` key. Paired with the `--tenant-token` args from
/// [`tenant_token_args`]; the two must use the same index order.
///
/// Rendering the value through an env var (and the arg through `$(VAR)`
/// expansion) keeps the raw token out of the Pod spec and its logs. Only the
/// tenant name and the env-var name appear in the API object.
fn tenant_token_env(spec: &RavelClusterSpec, ctx: &RenderCtx) -> Vec<EnvVar> {
    let Some(secret_ref) = spec.tenant_tokens_secret_ref.as_ref() else {
        return Vec::new();
    };
    ctx.tenant_names
        .iter()
        .enumerate()
        .map(|(i, tenant)| EnvVar {
            name: format!("RAVEL_TENANT_TOKEN_{i}"),
            value_from: Some(secret_key_env(&secret_ref.name, tenant)),
            ..Default::default()
        })
        .collect()
}

/// The repeatable `--tenant-token $(RAVEL_TENANT_TOKEN_<i>)=<tenant>` args.
///
/// The value is `$(RAVEL_TENANT_TOKEN_<i>)=<tenant>`: kubelet expands the
/// `$(VAR)` to the token from the Secret (see [`tenant_token_env`]) before the
/// process sees it, so the token never appears in the Pod spec.
///
/// IMPORTANT — pre-existing behavior in a different crate, out of this task's
/// scope: `ravel-server`'s `Cli::parse_tenant_tokens`
/// (`services/ravel-server/src/config.rs`) splits each argument on the FIRST
/// `=` via `str::split_once('=')`. After kubelet expansion the argument is
/// `<token>=<tenant>`, so a token VALUE that itself contains `=` is
/// mis-parsed: the substring after the first `=` becomes part of the tenant.
/// The CRD field doc on `tenant_tokens_secret_ref` states this so a user does
/// not hit it silently; the operator cannot see token values (they live only
/// in the Secret) so it cannot detect or reject the case here.
fn tenant_token_args(spec: &RavelClusterSpec, ctx: &RenderCtx) -> Vec<String> {
    if spec.tenant_tokens_secret_ref.is_none() {
        return Vec::new();
    }
    let mut args = Vec::new();
    for (i, tenant) in ctx.tenant_names.iter().enumerate() {
        args.push("--tenant-token".to_string());
        args.push(format!("$(RAVEL_TENANT_TOKEN_{i})={tenant}"));
    }
    args
}

/// Name of the Volume mounting the `deploymentKeySecretRef` Secret, when
/// configured.
const DEPLOYMENT_KEY_VOLUME_NAME: &str = "deployment-key";

/// Directory the deployment-key Secret is mounted at.
pub(crate) const DEPLOYMENT_KEY_MOUNT_DIR: &str = "/etc/ravel/deployment-key";

/// Key within the deployment-key Secret holding the 32-byte key, and the
/// filename it is projected to inside [`DEPLOYMENT_KEY_MOUNT_DIR`]. Also the
/// key [`crate::controller`] reads to get the live value for `sys/auth`
/// reconciliation, so the mounted path and the value the operator hashes
/// with can never name two different Secret keys.
pub(crate) const DEPLOYMENT_KEY_SECRET_KEY: &str = "key";

/// Env var the query tier reads the query-audit token key from (#1487).
/// Gateway and maintain never render this: only the query tier records
/// query text for audit.
pub(crate) const AUDIT_TOKEN_KEY_ENV: &str = "RAVEL_AUDIT_TOKEN_KEY";

/// Key within an explicit `auditTokenKeySecretRef` Secret holding the 64 hex
/// characters, mirroring [`DEPLOYMENT_KEY_SECRET_KEY`]'s naming for the
/// deployment-key Secret.
pub(crate) const AUDIT_TOKEN_KEY_SECRET_KEY: &str = "key";

/// `Degraded` reason when a cluster has neither `deploymentKeySecretRef` nor
/// `auditTokenKeySecretRef` set (#1487 rework): the operator does not
/// generate a Secret for this (issue #126's `secrets get`-only posture
/// forbids the create/patch a generated Secret would need), so the platform
/// owner must provide one, the same way they provide the S3 credentials and
/// tenant-tokens Secrets.
pub(crate) const AUDIT_TOKEN_KEY_MISSING_REASON: &str = "AuditTokenKeyMissing";

/// Message for [`AUDIT_TOKEN_KEY_MISSING_REASON`], naming the field to set
/// and the shape the Secret it points at must have.
pub(crate) const AUDIT_TOKEN_KEY_MISSING_MESSAGE: &str = "no deploymentKeySecretRef is set, and spec.auditTokenKeySecretRef is not set; reference a \
     Secret whose \"key\" field holds 64 hex characters (32 bytes) to enable \
     query-audit token key derivation, or set deploymentKeySecretRef to derive it from the \
     deployment key. The query tier's Deployment is left unchanged until then";

/// The query tier's `RAVEL_AUDIT_TOKEN_KEY` env var, or `None` when no
/// Secret should be read (#1487).
///
/// An explicit `spec.audit_token_key_secret_ref` always wins, even when
/// `deploymentKeySecretRef` is also set. Otherwise, when
/// `deploymentKeySecretRef` is set, the server derives the key from the
/// deployment key and needs no env var at all. Otherwise there is nothing to
/// source the env var from: [`audit_token_key_missing`] is true and the
/// controller withholds the query tier's Deployment apply entirely, so this
/// return value never actually reaches a rendered, applied Pod spec in that
/// case.
fn audit_token_key_env(spec: &RavelClusterSpec) -> Option<EnvVar> {
    let explicit = spec.audit_token_key_secret_ref.as_ref()?;
    Some(EnvVar {
        name: AUDIT_TOKEN_KEY_ENV.to_string(),
        value_from: Some(secret_key_env(&explicit.name, AUDIT_TOKEN_KEY_SECRET_KEY)),
        ..Default::default()
    })
}

/// Whether a cluster has no way to source a query-audit token key this pass
/// (#1487 rework): true only when neither `auditTokenKeySecretRef` nor
/// `deploymentKeySecretRef` is set. The operator no longer generates a
/// Secret for this case (issue #126: `secrets get` only, no create/patch), so
/// `true` here means the query tier's Deployment apply is withheld and a
/// `Degraded` condition with reason [`AUDIT_TOKEN_KEY_MISSING_REASON`] is
/// recorded, rather than the operator ever picking a key on the cluster's
/// behalf.
pub fn audit_token_key_missing(spec: &RavelClusterSpec) -> bool {
    spec.audit_token_key_secret_ref.is_none() && spec.deployment_key_secret_ref.is_none()
}

/// Args shared by every mode: store selection, shard count, and the S3
/// bucket/region/endpoint flags. Access/secret keys are NOT here (they are env
/// vars, see [`s3_credential_env`]).
///
/// Also carries the tenant-hash scheme flag (ADR-0050 section 3 / ADR-0072
/// decision 4): a fresh bucket refuses to start unless it is told a scheme.
/// When `spec.deployment_key_secret_ref` is set, this renders
/// `--tenant-hash-key-file` pointed at the mounted Secret (see
/// [`deployment_key_volume`]/[`deployment_key_volume_mount`]) so the cluster
/// runs the keyed derivation and the operator's own `sys/auth`
/// reconciliation has the same key available. Otherwise it
/// renders `--tenant-hash-unkeyed`, keeping every existing and new
/// keyless `RavelCluster` on the v1-unkeyed derivation. This is an
/// additive opt-in, not a migration of existing unkeyed clusters.
///
/// Also unconditionally carries `--require-bucket-protection` (ADR-0072
/// decision 3): "the operator sets it for production profiles", and every
/// `RavelCluster` the operator reconciles is a production deployment -- this
/// CRD has no dev/staging profile field to gate on, so there is nothing to
/// make it conditional on. `ravel-server`'s own default stays off for the
/// dev binary and any other direct invocation; only the operator's rendered
/// command line turns it on.
fn common_store_args(spec: &RavelClusterSpec) -> Vec<String> {
    let mut args = vec![
        "--store".to_string(),
        "s3".to_string(),
        "--shards".to_string(),
        spec.shards.to_string(),
        "--s3-bucket".to_string(),
        spec.storage.s3.bucket.clone(),
        "--s3-region".to_string(),
        spec.storage.s3.region.clone(),
        "--require-bucket-protection".to_string(),
    ];
    if spec.deployment_key_secret_ref.is_some() {
        args.push("--tenant-hash-key-file".to_string());
        args.push(format!(
            "{DEPLOYMENT_KEY_MOUNT_DIR}/{DEPLOYMENT_KEY_SECRET_KEY}"
        ));
    } else {
        args.push("--tenant-hash-unkeyed".to_string());
    }
    if let Some(endpoint) = &spec.storage.s3.endpoint {
        args.push("--s3-endpoint".to_string());
        args.push(endpoint.clone());
    }
    args
}

/// The Volume projecting the `deploymentKeySecretRef` Secret's `key` entry
/// into the pod, or `None` when the spec carries no deployment key.
fn deployment_key_volume(spec: &RavelClusterSpec) -> Option<Volume> {
    let secret_ref = spec.deployment_key_secret_ref.as_ref()?;
    Some(Volume {
        name: DEPLOYMENT_KEY_VOLUME_NAME.to_string(),
        secret: Some(SecretVolumeSource {
            secret_name: Some(secret_ref.name.clone()),
            items: Some(vec![KeyToPath {
                key: DEPLOYMENT_KEY_SECRET_KEY.to_string(),
                path: DEPLOYMENT_KEY_SECRET_KEY.to_string(),
                ..Default::default()
            }]),
            optional: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// The container's mount of [`deployment_key_volume`], read-only, or `None`
/// when the spec carries no deployment key.
fn deployment_key_volume_mount(spec: &RavelClusterSpec) -> Option<VolumeMount> {
    spec.deployment_key_secret_ref.as_ref()?;
    Some(VolumeMount {
        name: DEPLOYMENT_KEY_VOLUME_NAME.to_string(),
        mount_path: DEPLOYMENT_KEY_MOUNT_DIR.to_string(),
        read_only: Some(true),
        ..Default::default()
    })
}

/// Convert the CRD resource spec into a Kubernetes `ResourceRequirements`.
fn resources(spec: Option<&ResourceRequirementsSpec>) -> Option<ResourceRequirements> {
    let spec = spec?;
    let map = |m: Option<&BTreeMap<String, String>>| {
        m.map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), Quantity(v.clone())))
                .collect::<BTreeMap<_, _>>()
        })
    };
    Some(ResourceRequirements {
        requests: map(spec.requests.as_ref()),
        limits: map(spec.limits.as_ref()),
        claims: None,
    })
}

/// Liveness and readiness probes pointed at `/healthz` and `/readyz` on the
/// gateway/query/maintain HTTP port ([`HTTP_PORT`], ADR-0034 decision 4).
/// Returned as `(liveness, readiness)`.
fn probes() -> (Probe, Probe) {
    probes_on(HTTP_PORT)
}

/// [`probes`] parameterized by port, so the ravel-native router (which listens
/// on its own [`ROUTER_HTTP_PORT`], not the ravel-server `HTTP_PORT`) shares the
/// exact same `/healthz` `/readyz` probe shape.
fn probes_on(port: i32) -> (Probe, Probe) {
    let http_get = |path: &str| HTTPGetAction {
        path: Some(path.to_string()),
        port: IntOrString::Int(port),
        ..Default::default()
    };
    let liveness = Probe {
        http_get: Some(http_get("/healthz")),
        initial_delay_seconds: Some(5),
        period_seconds: Some(10),
        timeout_seconds: Some(2),
        failure_threshold: Some(3),
        ..Default::default()
    };
    let readiness = Probe {
        http_get: Some(http_get("/readyz")),
        initial_delay_seconds: Some(2),
        period_seconds: Some(10),
        timeout_seconds: Some(2),
        failure_threshold: Some(3),
        ..Default::default()
    };
    (liveness, readiness)
}

/// The hardened container-level SecurityContext stamped on every container the
/// operator renders (issue #126, ADR-0034 hardening amendment). Runs non-root,
/// forbids privilege escalation, drops every Linux capability, and mounts the
/// root filesystem read-only.
///
/// `read_only_root_filesystem` is correct for every container the operator
/// renders: ravel-server's only local-disk write path is the opt-in ADR-0046
/// disk cache tier (`--cache-dir`), which the operator renders no flag for, so
/// the server and the ingest-router run RAM-only and write nothing outside the
/// read-only mounts the pod already carries (the deployment-key Secret, mounted
/// read-only). Object storage is the only durable backend, so no rendered
/// container needs a writable root. A future CRD field that wires `--cache-dir`
/// must back it with its own writable volume mount, which is writable
/// independently of the root filesystem.
fn container_security_context() -> SecurityContext {
    SecurityContext {
        run_as_non_root: Some(true),
        allow_privilege_escalation: Some(false),
        read_only_root_filesystem: Some(true),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".to_string()]),
            add: None,
        }),
        ..Default::default()
    }
}

/// The hardened pod-level SecurityContext stamped on every PodSpec the operator
/// renders (issue #126): run as non-root and confine every container to the
/// `RuntimeDefault` seccomp profile.
fn pod_security_context() -> PodSecurityContext {
    PodSecurityContext {
        run_as_non_root: Some(true),
        seccomp_profile: Some(SeccompProfile {
            type_: "RuntimeDefault".to_string(),
            localhost_profile: None,
        }),
        ..Default::default()
    }
}

/// Preferred (soft) pod anti-affinity spreading a component's replicas across
/// nodes (issue #126). Preferred, never required: a required rule leaves a
/// single-node cluster (a `kind` dev cluster, the reference environment) unable
/// to schedule a second replica, wedging every multi-replica tier. The term
/// selects the component's own pods by the standard `instance`+`component`
/// labels and spreads on `kubernetes.io/hostname`.
fn pod_anti_affinity(instance: &str, component: &str) -> Affinity {
    Affinity {
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight: 100,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_labels: Some(labels(instance, component)),
                            ..Default::default()
                        }),
                        topology_key: "kubernetes.io/hostname".to_string(),
                        ..Default::default()
                    },
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Build a Deployment for `component` from its rendered args, env, container
/// ports, replica count, and strategy. The single place the common container
/// shape (image, probes, resources) is assembled, so gateway/query/maintain
/// cannot drift in how they wire the server.
#[allow(clippy::too_many_arguments)]
fn deployment(
    spec: &RavelClusterSpec,
    instance: &str,
    component: &str,
    args: Vec<String>,
    env: Vec<EnvVar>,
    ports: Vec<ContainerPort>,
    replicas: i32,
    resources_spec: Option<&ResourceRequirementsSpec>,
    strategy_type: &str,
    secrets_checksum: &str,
) -> Deployment {
    let labels = labels(instance, component);
    let (liveness, readiness) = probes();
    let volume_mount = deployment_key_volume_mount(spec);
    let volume = deployment_key_volume(spec);
    let container = Container {
        name: "ravel-server".to_string(),
        image: Some(spec.image.clone()),
        image_pull_policy: spec.image_pull_policy.clone(),
        args: Some(args),
        env: if env.is_empty() { None } else { Some(env) },
        ports: if ports.is_empty() { None } else { Some(ports) },
        liveness_probe: Some(liveness),
        readiness_probe: Some(readiness),
        resources: resources(resources_spec),
        volume_mounts: volume_mount.map(|m| vec![m]),
        security_context: Some(container_security_context()),
        ..Default::default()
    };
    Deployment {
        metadata: ObjectMeta {
            name: Some(child_name(instance, component)),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            strategy: Some(DeploymentStrategy {
                type_: Some(strategy_type.to_string()),
                // Recreate takes no rollingUpdate block; leaving it None is
                // required (Kubernetes rejects rollingUpdate with Recreate).
                rolling_update: None,
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(BTreeMap::from([(
                        SECRETS_CHECKSUM_ANNOTATION.to_string(),
                        secrets_checksum.to_string(),
                    )])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    volumes: volume.map(|v| vec![v]),
                    security_context: Some(pod_security_context()),
                    affinity: Some(pod_anti_affinity(instance, component)),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

/// The gateway Deployment: `--mode gateway`, HTTP + gRPC listeners, tenant
/// tokens, and fold tuning. RollingUpdate strategy.
pub fn desired_gateway_deployment(
    spec: &RavelClusterSpec,
    instance: &str,
    ctx: &RenderCtx,
) -> Deployment {
    let mut args = vec![
        "--mode".to_string(),
        "gateway".to_string(),
        "--listen-http".to_string(),
        format!("0.0.0.0:{HTTP_PORT}"),
        "--listen-grpc".to_string(),
        format!("0.0.0.0:{GRPC_PORT}"),
    ];
    args.extend(common_store_args(spec));
    args.extend(tenant_token_args(spec, ctx));
    if let Some(fold) = &spec.gateway.fold {
        if fold.disabled {
            args.push("--disable-fold".to_string());
        }
        if let Some(secs) = fold.interval_secs {
            args.push("--fold-interval-secs".to_string());
            args.push(secs.to_string());
        }
    }

    let tier_override = spec.gateway.credentials_secret_ref.as_ref();
    let mut env = s3_credential_env(spec, tier_override);
    env.extend(tenant_token_env(spec, ctx));

    let ports = vec![
        ContainerPort {
            name: Some("http".to_string()),
            container_port: HTTP_PORT,
            ..Default::default()
        },
        ContainerPort {
            name: Some("grpc".to_string()),
            container_port: GRPC_PORT,
            ..Default::default()
        },
    ];

    deployment(
        spec,
        instance,
        "gateway",
        args,
        env,
        ports,
        spec.gateway.replicas,
        spec.gateway.resources.as_ref(),
        "RollingUpdate",
        &tier_secrets_checksum(spec, ctx, tier_override, None),
    )
}

/// The query Deployment: `--mode query`, HTTP listener only, tenant tokens.
/// RollingUpdate strategy.
pub fn desired_query_deployment(
    spec: &RavelClusterSpec,
    instance: &str,
    ctx: &RenderCtx,
) -> Deployment {
    let mut args = vec![
        "--mode".to_string(),
        "query".to_string(),
        "--listen-http".to_string(),
        format!("0.0.0.0:{HTTP_PORT}"),
    ];
    args.extend(common_store_args(spec));
    args.extend(tenant_token_args(spec, ctx));

    let tier_override = spec.query.credentials_secret_ref.as_ref();
    let mut env = s3_credential_env(spec, tier_override);
    env.extend(tenant_token_env(spec, ctx));
    if let Some(audit_env) = audit_token_key_env(spec) {
        env.push(audit_env);
    }

    let ports = vec![ContainerPort {
        name: Some("http".to_string()),
        container_port: HTTP_PORT,
        ..Default::default()
    }];

    deployment(
        spec,
        instance,
        "query",
        args,
        env,
        ports,
        spec.query.replicas,
        spec.query.resources.as_ref(),
        "RollingUpdate",
        &tier_secrets_checksum(
            spec,
            ctx,
            tier_override,
            ctx.audit_token_key_resource_version.as_deref(),
        ),
    )
}

/// The maintain Deployment, or `None` when `maintain.enabled` is false (the
/// controller deletes the Deployment in that case).
///
/// Replica count comes from `spec.maintain.replicas` (default 1) and the
/// strategy is `RollingUpdate`, superseding ADR-0034 decision 3's
/// single-replica `Recreate` guidance. ADR-0065's ownership protocol makes
/// `replicas > 1` safe: every maintain pod runs `--mode maintain`, which spawns
/// the heartbeat/rendezvous machinery that partitions the `(tenant, signal,
/// shard)` unit space across the live worker set discovered from the shared
/// store. The coordination is entirely store-side (self-owned keys under
/// `sys/maintain/`), so the pod spec needs no extra ownership flags beyond
/// `--mode maintain` and the shared store args every tier already carries; a
/// rolling restart is safe because a briefly-overlapping owner during the
/// handoff window only duplicates idempotent work, never corrupts it (ADR-0065
/// decision 2). Maintain serves `--listen-http` for the health probes only;
/// retention flags render here because retention is enforced by this tier.
///
/// Maintain DOES get the same `--tenant-token` args and env as gateway and
/// query, but for a different reason: it authenticates no incoming requests, so
/// it does not need the tokens for AUTHENTICATION. It needs the tenant LIST.
/// `ravel-server` derives the set of tenants it compacts, retires, and GCs
/// entirely from the parsed `--tenant-token` map (`fold_tenants` in
/// `services/ravel-server/src/main.rs`, also used as maintain's tenant list);
/// with zero tokens rendered, `maintain::spawn` gets an empty tenant list and
/// returns `MaintenanceTasks::none()`, so the whole tier is a silent no-op that
/// still reports `Ready`. Rendering the tokens here is what gives maintain a
/// non-empty tenant list to act on. The `$(VAR)` expansion keeps the raw token
/// out of the Pod spec exactly as it does for gateway/query.
pub fn desired_maintain_deployment(
    spec: &RavelClusterSpec,
    instance: &str,
    ctx: &RenderCtx,
) -> Result<Option<Deployment>, RenderError> {
    // Validate before the enabled check: an invalid `spec.gc` is a
    // misconfiguration to surface even when no maintain Deployment renders,
    // otherwise it would sit silent until the tier is turned on.
    validate_gc_spec(spec)?;
    if !spec.maintain.enabled {
        return Ok(None);
    }
    let mut args = vec![
        "--mode".to_string(),
        "maintain".to_string(),
        "--listen-http".to_string(),
        format!("0.0.0.0:{HTTP_PORT}"),
    ];
    args.extend(common_store_args(spec));
    args.extend(tenant_token_args(spec, ctx));
    if let Some(secs) = spec.maintain.interval_secs {
        args.push("--maintain-interval-secs".to_string());
        args.push(secs.to_string());
    }
    // GC horizons (#1083): rendered only on this tier, and only when set, since
    // maintain is the only role that validates itself against `sys/gc`. The
    // value is validated as a humantime duration and then rendered verbatim, so
    // an invalid string is a typed render error naming the field rather than a
    // flag the maintain pod would reject at startup, and a valid one is never
    // reinterpreted.
    if let Some(gc) = &spec.gc {
        if let Some(horizon) = &gc.protection_horizon {
            args.push("--gc-protection-horizon".to_string());
            args.push(horizon.clone());
        }
        if let Some(grace) = &gc.grace {
            args.push("--gc-grace".to_string());
            args.push(grace.clone());
        }
    }
    if let Some(retention) = &spec.retention {
        if let Some(default) = &retention.default {
            args.push("--retention-default".to_string());
            args.push(default.clone());
        }
        for (tenant, dur) in &retention.tenants {
            args.push("--retention-tenant".to_string());
            args.push(format!("{tenant}={dur}"));
        }
    }

    let tier_override = spec.maintain.credentials_secret_ref.as_ref();
    let mut env = s3_credential_env(spec, tier_override);
    env.extend(tenant_token_env(spec, ctx));

    let ports = vec![ContainerPort {
        name: Some("http".to_string()),
        container_port: HTTP_PORT,
        ..Default::default()
    }];

    Ok(Some(deployment(
        spec,
        instance,
        "maintain",
        args,
        env,
        ports,
        spec.maintain.replicas,
        spec.maintain.resources.as_ref(),
        "RollingUpdate",
        &tier_secrets_checksum(spec, ctx, tier_override, None),
    )))
}

/// Validate every set `spec.gc` field, whether or not the maintain tier
/// renders this pass.
fn validate_gc_spec(spec: &RavelClusterSpec) -> Result<(), RenderError> {
    if let Some(gc) = &spec.gc {
        if let Some(horizon) = &gc.protection_horizon {
            validate_gc_duration("protectionHorizon", horizon)?;
        }
        if let Some(grace) = &gc.grace {
            validate_gc_duration("grace", grace)?;
        }
    }
    Ok(())
}

/// Validate a `spec.gc` horizon string the way the `ravel-server` flags do: a
/// humantime duration that is positive and fits in nanoseconds. An empty,
/// unparseable, zero, or too-large value is a [`RenderError::GcHorizonInvalid`]
/// naming `field`; a valid one returns `Ok` and the caller renders the original
/// string verbatim.
fn validate_gc_duration(field: &'static str, value: &str) -> Result<(), RenderError> {
    let invalid = || RenderError::GcHorizonInvalid {
        field,
        value: value.to_string(),
    };
    if value.is_empty() {
        return Err(invalid());
    }
    let dur = humantime::parse_duration(value).map_err(|_| invalid())?;
    let ns = i64::try_from(dur.as_nanos()).map_err(|_| invalid())?;
    if ns <= 0 {
        return Err(invalid());
    }
    Ok(())
}

/// Build a ClusterIP Service selecting `component`'s pods on the given ports.
fn service(instance: &str, component: &str, ports: Vec<ServicePort>) -> Service {
    let labels = labels(instance, component);
    Service {
        metadata: ObjectMeta {
            name: Some(child_name(instance, component)),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(labels),
            ports: Some(ports),
            ..Default::default()
        }),
        status: None,
    }
}

/// The gateway Service, exposing HTTP (4318) and gRPC (4317).
pub fn desired_gateway_service(_spec: &RavelClusterSpec, instance: &str) -> Service {
    service(
        instance,
        "gateway",
        vec![
            ServicePort {
                name: Some("http".to_string()),
                port: HTTP_PORT,
                target_port: Some(IntOrString::Int(HTTP_PORT)),
                ..Default::default()
            },
            ServicePort {
                name: Some("grpc".to_string()),
                port: GRPC_PORT,
                target_port: Some(IntOrString::Int(GRPC_PORT)),
                ..Default::default()
            },
        ],
    )
}

/// The query Service, exposing HTTP (4318) only.
pub fn desired_query_service(_spec: &RavelClusterSpec, instance: &str) -> Service {
    service(
        instance,
        "query",
        vec![ServicePort {
            name: Some("http".to_string()),
            port: HTTP_PORT,
            target_port: Some(IntOrString::Int(HTTP_PORT)),
            ..Default::default()
        }],
    )
}

/// ingress-nginx annotation selecting the consistent-hash key. Its value is an
/// nginx variable; the controller hashes it with ketama, so only a few keys
/// remap when the endpoint set changes.
pub const UPSTREAM_HASH_BY_ANNOTATION: &str = "nginx.ingress.kubernetes.io/upstream-hash-by";

/// ingress-nginx annotation turning on subset mode: the key selects a GROUP of
/// endpoints rather than one endpoint, and a member of that group is then
/// chosen uniformly at random per request. This is what makes the subset a
/// subset rather than a pin to a single replica.
pub const UPSTREAM_HASH_BY_SUBSET_ANNOTATION: &str =
    "nginx.ingress.kubernetes.io/upstream-hash-by-subset";

/// ingress-nginx annotation setting how many endpoints are in each subset. The
/// controller's own default is 3; the operator always renders this explicitly
/// from `subsetSize` so the effective value is what the `RavelCluster` says and
/// never what the controller version happens to default to.
pub const UPSTREAM_HASH_BY_SUBSET_SIZE_ANNOTATION: &str =
    "nginx.ingress.kubernetes.io/upstream-hash-by-subset-size";

/// ingress-nginx annotation choosing between per-Pod endpoints and the
/// Service's single ClusterIP as the upstream. Always rendered as `"false"`.
///
/// This is not decoration. With `service-upstream: true` the upstream has ONE
/// server (the ClusterIP) and kube-proxy picks the pod, so the hash has nothing
/// to distribute over and affinity silently does nothing. The controller's
/// default is false, but it is settable cluster-wide in the ingress-nginx
/// ConfigMap; rendering it explicitly makes the affinity independent of that.
pub const SERVICE_UPSTREAM_ANNOTATION: &str = "nginx.ingress.kubernetes.io/service-upstream";

/// ingress-nginx annotation selecting the protocol spoken to the backend.
/// Rendered as `GRPC` on the gRPC Ingress only. It is per-Ingress, which is why
/// OTLP/HTTP and OTLP/gRPC need two objects rather than two paths on one.
pub const BACKEND_PROTOCOL_ANNOTATION: &str = "nginx.ingress.kubernetes.io/backend-protocol";

/// Suffix of the OTLP/HTTP ingest Ingress: `<cluster>-gateway-ingest`.
const INGEST_INGRESS_COMPONENT: &str = "gateway-ingest";

/// Suffix of the OTLP/gRPC ingest Ingress: `<cluster>-gateway-ingest-grpc`.
const INGEST_GRPC_INGRESS_COMPONENT: &str = "gateway-ingest-grpc";

/// The OTLP/HTTP ingest paths the HTTP Ingress serves: exactly the routes
/// `services/ravel-server/src/otlp_http.rs` mounts. Disjoint from
/// [`INGEST_GRPC_PATHS`], so the two Ingress objects never claim the same
/// `(host, path)` and ingress-nginx never drops one location as a duplicate.
const INGEST_HTTP_PATHS: &[&str] = &["/v1/metrics", "/v1/logs", "/v1/traces"];

/// The OTLP/gRPC ingest paths the gRPC Ingress serves: one full service name
/// per gRPC service registered on the ingest surface in
/// `services/ravel-server/src/lib.rs`. This list must track that registration
/// site; the next gRPC service added there must be added here too, or its RPCs
/// have no Ingress route.
///
/// Each entry is a complete gRPC service name, which is a complete path element
/// under Kubernetes `PathType: Prefix` element-wise matching (split on `/`), so
/// `/<service>` correctly prefixes `/<service>/<method>` under both the strict
/// spec semantics and nginx's string-prefix rendering. A truncated common
/// prefix like `/opentelemetry.proto.collector.` would be a single element that
/// equals none of these, matching nothing under the spec.
const INGEST_GRPC_PATHS: &[&str] = &[
    "/opentelemetry.proto.collector.metrics.v1.MetricsService",
    "/opentelemetry.proto.collector.logs.v1.LogsService",
    "/opentelemetry.proto.collector.trace.v1.TraceService",
    "/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService",
];

/// Every Ingress name the ingest-affinity render can produce for `instance`, in
/// a fixed order, whether or not the current spec asks for them.
///
/// The controller applies the ones [`desired_gateway_ingresses`] returns and
/// deletes every other name in this list, so turning `enabled` off (or dropping
/// `grpc`) converges back to the affinity-absent state instead of leaving an
/// orphaned Ingress routing traffic under rules the spec no longer describes.
pub fn possible_ingest_ingress_names(instance: &str) -> Vec<String> {
    vec![
        child_name(instance, INGEST_INGRESS_COMPONENT),
        child_name(instance, INGEST_GRPC_INGRESS_COMPONENT),
    ]
}

/// The nginx variable holding tenant identity for a given key source.
///
/// A header name is lowercased with every character outside `[a-z0-9]` mapped
/// to `_`, which is exactly nginx's own `$http_<name>` mangling (`X-Scope-OrgID`
/// becomes `$http_x_scope_orgid`). That mapping is also the security property:
/// no user-supplied character can survive into the rendered annotation as nginx
/// syntax, so a header name cannot inject a directive. An absent or
/// all-punctuation header name falls back to `authorization`, which is the
/// same key the default source uses and therefore still a real tenant key
/// rather than a constant; the CRD's CEL rule rejects that manifest at
/// admission anyway.
fn affinity_hash_variable(key: &crate::crd::AffinityKeySpec) -> String {
    match key.source {
        AffinityKeySource::AuthorizationHeader => "$http_authorization".to_string(),
        AffinityKeySource::MtlsSubject => "$ssl_client_s_dn".to_string(),
        // canonical-tenant hashing is a ravel-native-router concern; ingress-nginx
        // cannot run the resolver chain, and the CANONICAL_TENANT_BACKEND_RULE CEL
        // rule (crd.rs) rejects this key source under backend: ingressNginx at
        // admission, so this ingress-nginx-only renderer can never see it.
        AffinityKeySource::CanonicalTenant => {
            unreachable!("CEL admission rejects canonical-tenant under backend: ingressNginx")
        }
        AffinityKeySource::Header => {
            let name = key.header_name.as_deref().unwrap_or("authorization");
            let mangled: String = name
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() {
                        c.to_ascii_lowercase()
                    } else {
                        '_'
                    }
                })
                .collect();
            let mangled = if mangled.chars().all(|c| c == '_') {
                "authorization".to_string()
            } else {
                mangled
            };
            format!("$http_{mangled}")
        }
    }
}

/// The annotation map for an ingest Ingress: the operator's affinity
/// annotations layered over the spec's passthrough map.
///
/// Order matters and is deliberate. The passthrough entries go in first and the
/// computed affinity entries overwrite them, so an operator can add
/// `proxy-body-size` or a different controller's annotations without being able
/// to accidentally turn off the affinity this object exists to express.
fn ingest_ingress_annotations(
    affinity: &IngestAffinitySpec,
    grpc: bool,
) -> BTreeMap<String, String> {
    let mut annotations = affinity.annotations.clone();
    annotations.insert(
        UPSTREAM_HASH_BY_ANNOTATION.to_string(),
        affinity_hash_variable(&affinity.key),
    );
    annotations.insert(
        UPSTREAM_HASH_BY_SUBSET_ANNOTATION.to_string(),
        "true".to_string(),
    );
    annotations.insert(
        UPSTREAM_HASH_BY_SUBSET_SIZE_ANNOTATION.to_string(),
        affinity.subset_size.to_string(),
    );
    annotations.insert(SERVICE_UPSTREAM_ANNOTATION.to_string(), "false".to_string());
    if grpc {
        annotations.insert(BACKEND_PROTOCOL_ANNOTATION.to_string(), "GRPC".to_string());
    }
    annotations
}

/// One ingest Ingress: `paths` to the gateway Service's `port_name` port, under
/// the affinity annotations from [`ingest_ingress_annotations`].
///
/// The path set is passed in rather than hardcoded because OTLP/HTTP and
/// OTLP/gRPC MUST route on disjoint paths. `backend-protocol` is per-Ingress,
/// so gRPC needs its own object, and two Ingresses sharing a `(host, path)` is
/// a duplicate-path conflict in ingress-nginx: it keeps one location and drops
/// the other's, which would silently disable the gRPC object's `grpc_pass` and
/// its affinity annotations.
fn ingest_ingress(
    instance: &str,
    component: &str,
    affinity: &IngestAffinitySpec,
    port_name: &str,
    grpc: bool,
    paths: &[&str],
) -> Ingress {
    let backend = IngressBackend {
        service: Some(IngressServiceBackend {
            // The gateway Service, whose EndpointSlices are what ingress-nginx
            // hashes over. Named ports, so this cannot drift from the
            // ServicePort names in `desired_gateway_service`.
            name: child_name(instance, "gateway"),
            port: Some(ServiceBackendPort {
                name: Some(port_name.to_string()),
                number: None,
            }),
        }),
        resource: None,
    };
    let http = HTTPIngressRuleValue {
        paths: paths
            .iter()
            .map(|path| HTTPIngressPath {
                path: Some((*path).to_string()),
                path_type: "Prefix".to_string(),
                backend: backend.clone(),
            })
            .collect(),
    };
    // No hosts configured renders one host-less rule, which matches whatever
    // host reaches the controller. That is the right default for a cluster
    // fronting Ravel with a single ingest endpoint.
    let rules: Vec<IngressRule> = if affinity.hosts.is_empty() {
        vec![IngressRule {
            host: None,
            http: Some(http),
        }]
    } else {
        affinity
            .hosts
            .iter()
            .map(|host| IngressRule {
                host: Some(host.clone()),
                http: Some(http.clone()),
            })
            .collect()
    };
    let tls = affinity.tls_secret_name.as_ref().map(|secret| {
        vec![IngressTLS {
            // An empty host list means the certificate applies to every host
            // this Ingress serves, matching the host-less rule above.
            hosts: if affinity.hosts.is_empty() {
                None
            } else {
                Some(affinity.hosts.clone())
            },
            secret_name: Some(secret.clone()),
        }]
    });
    Ingress {
        metadata: ObjectMeta {
            name: Some(child_name(instance, component)),
            // The gateway component labels, so the controller's
            // managed-by-scoped `.owns()` watch sees these objects.
            labels: Some(labels(instance, "gateway")),
            annotations: Some(ingest_ingress_annotations(affinity, grpc)),
            ..Default::default()
        },
        spec: Some(IngressSpec {
            ingress_class_name: affinity.ingress_class_name.clone(),
            rules: Some(rules),
            tls,
            default_backend: None,
        }),
        status: None,
    }
}

/// The ingest-affinity Ingress objects (ADR-0076 decision 1), or an empty vec
/// when `gateway.ingestAffinity` is absent or disabled.
///
/// Empty is the backward-compatible state: the operator renders no Ingress at
/// all today, so an existing `RavelCluster` keeps routing ingest exactly the
/// way its platform owner already routes it.
pub fn desired_gateway_ingresses(spec: &RavelClusterSpec, instance: &str) -> Vec<Ingress> {
    let Some(affinity) = spec.gateway.ingest_affinity.as_ref() else {
        return Vec::new();
    };
    // nginx Ingress objects are the `ingressNginx` backend's exposure. Under
    // `backend: ravelNative` the router renders instead (ADR-0080 decision 3),
    // so this path renders nothing -- and would otherwise reach
    // `affinity_hash_variable`'s `CanonicalTenant` unreachable arm.
    if !affinity.enabled || !matches!(affinity.backend, AffinityBackend::IngressNginx) {
        return Vec::new();
    }
    let mut ingresses = vec![ingest_ingress(
        instance,
        INGEST_INGRESS_COMPONENT,
        affinity,
        "http",
        false,
        INGEST_HTTP_PATHS,
    )];
    if affinity.grpc {
        ingresses.push(ingest_ingress(
            instance,
            INGEST_GRPC_INGRESS_COMPONENT,
            affinity,
            "grpc",
            true,
            INGEST_GRPC_PATHS,
        ));
    }
    ingresses
}

/// Component suffix of the ravel-native ingest router's managed objects:
/// `<cluster>-ingest-router`. The Deployment, Service, ServiceAccount, Role, and
/// RoleBinding all share this one base name (ADR-0080 decision 3).
const ROUTER_COMPONENT: &str = "ingest-router";

/// The router's HTTP listener and container port, matching the frozen
/// `services/ravel-ingest-router` CLI default (`--listen-http 0.0.0.0:8080`).
///
/// There is intentionally no gRPC port: the router has no `--listen-grpc` flag
/// on main (#184, T6b, adds it concurrently in a different crate), so this task
/// renders an HTTP-only Deployment with this single container port. Wiring the
/// gRPC listener and a second container port once #184 lands is a documented
/// follow-up, not guessed at here.
pub const ROUTER_HTTP_PORT: i32 = 8080;

/// Default replica count for the router Deployment.
///
/// The CRD has no per-router replica field yet (a `ingestAffinity.routerReplicas`
/// surface is a documented follow-up); two, so a single router pod loss does not
/// take ingest down.
const DEFAULT_ROUTER_REPLICAS: i32 = 2;

/// The ingest-affinity spec when the ravel-native router should be rendered:
/// affinity present, `enabled`, and `backend: ravelNative`. `None` otherwise.
///
/// Every router renderer shares this one gate, so they cannot disagree about
/// which objects the router set contains. The Gateway-API backendRef switch
/// ([`desired_gateway_routes`]) needs a STRICTER condition than this, though:
/// affinity being `ravelNative` is necessary but not sufficient for the router's
/// Service to exist after a reconcile pass, because the router's own render can
/// fail this pass (missing `routerImage`, or a canonical-tenant resolver that
/// resolves to zero tenants). So the route switch keys off the router's actual
/// render outcome (`router_available`, threaded from [`desired_objects`]), not
/// this gate alone -- otherwise a route could point at a router Service the same
/// pass deletes.
fn ravel_native_affinity(spec: &RavelClusterSpec) -> Option<&IngestAffinitySpec> {
    let affinity = spec.gateway.ingest_affinity.as_ref()?;
    (affinity.enabled && matches!(affinity.backend, AffinityBackend::RavelNative))
        .then_some(affinity)
}

/// The `--key-source` CLI value for a key source, matching the frozen
/// `services/ravel-ingest-router` contract byte for byte.
///
/// A mismatch here compiles and passes CI yet crashloops the router pod (its own
/// CLI rejects an unknown value at startup), which is exactly why
/// [`backend_ravel_native_renders_router_deployment_with_frozen_cli_args`] pins
/// the whole rendered arg list against a hardcoded expected vector.
fn router_key_source_value(source: AffinityKeySource) -> &'static str {
    match source {
        AffinityKeySource::AuthorizationHeader => "authorization-header",
        AffinityKeySource::Header => "header",
        AffinityKeySource::MtlsSubject => "mtls-subject",
        AffinityKeySource::CanonicalTenant => "canonical-tenant",
    }
}

/// The router container's args, rendered from the frozen CLI contract.
///
/// `--gateway-service-name`/`--gateway-service-namespace` name this
/// `RavelCluster`'s own gateway Service (the router watches its EndpointSlices);
/// `--subset-size` and `--key-source` come from the affinity spec. The
/// OIDC/mTLS/tenant-token/dev-header flags are REJECTED by the router's own CLI
/// unless `--key-source canonical-tenant`, so the only such flag rendered here
/// (`--tenant-token`, when a `tenantTokensSecretRef` exists) is gated on that
/// source. The router's OIDC and mTLS configuration has NO operator-side CRD
/// surface today -- `ravel-server`'s own OIDC/mTLS config is CLI-flag-only with
/// no CRD field the operator threads -- so those flags are intentionally not
/// rendered; giving the canonical-tenant resolver its OIDC/mTLS config is a
/// separate, larger CRD design task (documented in the task report), not
/// invented here.
fn router_args(
    affinity: &IngestAffinitySpec,
    instance: &str,
    namespace: &str,
    spec: &RavelClusterSpec,
    ctx: &RenderCtx,
) -> Vec<String> {
    let mut args = vec![
        "--gateway-service-name".to_string(),
        child_name(instance, "gateway"),
        "--gateway-service-namespace".to_string(),
        namespace.to_string(),
        // Pin the gateway port explicitly. The gateway Service exposes two named
        // ports (`http`/4318, `grpc`/4317); an unset `--gateway-port-name` uses
        // the endpoint's first port, which is not guaranteed to be `http`. The
        // router only ever proxies HTTP today (see desired_gateway_routes: the
        // GRPCRoute still targets the gateway Service directly), so pin `http`.
        "--gateway-port-name".to_string(),
        "http".to_string(),
        "--subset-size".to_string(),
        affinity.subset_size.to_string(),
        "--key-source".to_string(),
        router_key_source_value(affinity.key.source).to_string(),
    ];
    // The router CLI requires --key-header-name iff --key-source header.
    if affinity.key.source == AffinityKeySource::Header
        && let Some(header_name) = affinity.key.header_name.as_ref()
    {
        args.push("--key-header-name".to_string());
        args.push(header_name.clone());
    }
    args.push("--listen-http".to_string());
    args.push(format!("0.0.0.0:{ROUTER_HTTP_PORT}"));
    // tenant-token flags are rejected by the router CLI unless the key source is
    // canonical-tenant, so render them only then. Reuses the same
    // $(VAR)-expansion machinery ravel-server's tiers use (see
    // [`tenant_token_args`]/[`tenant_token_env`]), so a raw token never appears
    // in the Pod spec.
    if affinity.key.source == AffinityKeySource::CanonicalTenant {
        args.extend(tenant_token_args(spec, ctx));
    }
    args
}

/// The ravel-native ingest router's Deployment (ADR-0080 decision 3), or `None`
/// when `backend` is not `ravelNative` or affinity is absent/disabled.
///
/// Returns [`RenderError::RouterImageMissing`] when the router is called for but
/// `ingestAffinity.routerImage` is unset: the router is a different binary from
/// `spec.image`, so there is nothing to fall back to, and rendering an empty
/// `image:` would only surface as an un-schedulable pod. The controller turns
/// this error into a `Degraded` status condition (deliverable 1), so a
/// misconfigured CR fails visibly at reconcile time rather than silently.
///
/// The container image is always `routerImage`, never `spec.image`. Health
/// probes hit `/healthz` `/readyz` on [`ROUTER_HTTP_PORT`], and the pod runs
/// under the router's own ServiceAccount ([`desired_router_service_account`]) so
/// its EndpointSlice/Service reads use the least-privilege Role, not the
/// operator's identity.
pub fn desired_router_deployment(
    spec: &RavelClusterSpec,
    instance: &str,
    namespace: &str,
    ctx: &RenderCtx,
) -> Result<Option<Deployment>, RenderError> {
    let Some(affinity) = ravel_native_affinity(spec) else {
        return Ok(None);
    };
    let Some(image) = affinity.router_image.as_ref() else {
        return Err(RenderError::RouterImageMissing);
    };
    // canonical-tenant needs at least one resolver or the router's own CLI
    // crashloops at startup. Gate on the ACTUAL rendered resolver flags, not on
    // `tenant_tokens_secret_ref.is_none()`: the flags come from
    // `tenant_token_args`, which emits one `--tenant-token` per
    // `ctx.tenant_names`, and `tenant_names` is the tenant-tokens Secret's live
    // keys (see controller.rs). The Secret ref can be SET on the spec while the
    // Secret itself resolves to zero keys this cycle -- a reachable live state
    // distinct from the Secret being absent entirely (an absent Secret is a hard
    // `Error::SecretNotFound` that aborts the reconcile before this function is
    // ever called; a present-but-empty Secret is not). In the zero-keys case no
    // `--tenant-token` renders and the router crashloops just the same. If no
    // resolver flags would actually render, reject at render time (see
    // RenderError::CanonicalTenantResolverMissing) rather than ship a
    // guaranteed-crashlooping Deployment.
    if affinity.key.source == AffinityKeySource::CanonicalTenant
        && tenant_token_args(spec, ctx).is_empty()
    {
        return Err(RenderError::CanonicalTenantResolverMissing);
    }
    let args = router_args(affinity, instance, namespace, spec, ctx);
    let env = if affinity.key.source == AffinityKeySource::CanonicalTenant {
        tenant_token_env(spec, ctx)
    } else {
        Vec::new()
    };
    let labels = labels(instance, ROUTER_COMPONENT);
    let (liveness, readiness) = probes_on(ROUTER_HTTP_PORT);
    let container = Container {
        name: "ravel-ingest-router".to_string(),
        image: Some(image.clone()),
        image_pull_policy: spec.image_pull_policy.clone(),
        args: Some(args),
        env: if env.is_empty() { None } else { Some(env) },
        ports: Some(vec![ContainerPort {
            name: Some("http".to_string()),
            container_port: ROUTER_HTTP_PORT,
            ..Default::default()
        }]),
        liveness_probe: Some(liveness),
        readiness_probe: Some(readiness),
        security_context: Some(container_security_context()),
        ..Default::default()
    };
    Ok(Some(Deployment {
        metadata: ObjectMeta {
            name: Some(child_name(instance, ROUTER_COMPONENT)),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(DEFAULT_ROUTER_REPLICAS),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".to_string()),
                rolling_update: None,
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    // The router reads EndpointSlices/Services under its own
                    // least-privilege ServiceAccount (deliverable 7).
                    service_account_name: Some(child_name(instance, ROUTER_COMPONENT)),
                    security_context: Some(pod_security_context()),
                    affinity: Some(pod_anti_affinity(instance, ROUTER_COMPONENT)),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }))
}

/// The router's ClusterIP Service on [`ROUTER_HTTP_PORT`], or `None` when the
/// router is not rendered. The Gateway-API routes point their backendRef at this
/// Service under `backend: ravelNative` (deliverable 4).
pub fn desired_router_service(spec: &RavelClusterSpec, instance: &str) -> Option<Service> {
    ravel_native_affinity(spec)?;
    Some(service(
        instance,
        ROUTER_COMPONENT,
        vec![ServicePort {
            name: Some("http".to_string()),
            port: ROUTER_HTTP_PORT,
            target_port: Some(IntOrString::Int(ROUTER_HTTP_PORT)),
            ..Default::default()
        }],
    ))
}

/// The router's ServiceAccount, or `None` when the router is not rendered. The
/// Deployment runs as this account and the Role/RoleBinding
/// ([`desired_router_role`]/[`desired_router_role_binding`]) scope its
/// permissions to EndpointSlices and Services in its own namespace only.
pub fn desired_router_service_account(
    spec: &RavelClusterSpec,
    instance: &str,
) -> Option<ServiceAccount> {
    ravel_native_affinity(spec)?;
    Some(ServiceAccount {
        metadata: ObjectMeta {
            name: Some(child_name(instance, ROUTER_COMPONENT)),
            labels: Some(labels(instance, ROUTER_COMPONENT)),
            ..Default::default()
        },
        ..Default::default()
    })
}

/// The router's namespaced Role granting `get`/`list`/`watch` on
/// `endpointslices` (discovery.k8s.io) and `services` (core) ONLY, or `None`
/// when the router is not rendered.
///
/// Least-privilege, consistent with this repo's per-role credential-scoping
/// convention (ADR-0055, ADR-0072): no cluster-wide grant, no other resource,
/// and no write verbs. This is exactly what the router's EndpointSlice watcher
/// needs to compute subsets and dial gateway pods directly.
pub fn desired_router_role(spec: &RavelClusterSpec, instance: &str) -> Option<Role> {
    ravel_native_affinity(spec)?;
    Some(Role {
        metadata: ObjectMeta {
            name: Some(child_name(instance, ROUTER_COMPONENT)),
            labels: Some(labels(instance, ROUTER_COMPONENT)),
            ..Default::default()
        },
        rules: Some(vec![
            PolicyRule {
                api_groups: Some(vec!["discovery.k8s.io".to_string()]),
                resources: Some(vec!["endpointslices".to_string()]),
                verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec![String::new()]),
                resources: Some(vec!["services".to_string()]),
                verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
                ..Default::default()
            },
        ]),
    })
}

/// The router's namespaced RoleBinding tying its ServiceAccount to its Role, or
/// `None` when the router is not rendered. `namespace` scopes the Subject to the
/// `RavelCluster`'s own namespace.
pub fn desired_router_role_binding(
    spec: &RavelClusterSpec,
    instance: &str,
    namespace: &str,
) -> Option<RoleBinding> {
    ravel_native_affinity(spec)?;
    let name = child_name(instance, ROUTER_COMPONENT);
    Some(RoleBinding {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            labels: Some(labels(instance, ROUTER_COMPONENT)),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: name.clone(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name,
            namespace: Some(namespace.to_string()),
            ..Default::default()
        }]),
    })
}

/// Every router-owned object name the render can produce for `instance`, in a
/// fixed order, whether or not the current spec asks for them.
///
/// The Deployment, Service, ServiceAccount, Role, and RoleBinding all share this
/// one base name, so a single-element list covers every kind: the controller
/// sweeps this name across each kind's own `Api`, deleting whichever objects the
/// current mode did not render (deliverable 6). Switching `backend` away from
/// `ravelNative`, or disabling affinity, therefore cleans up every router-owned
/// object instead of orphaning it.
pub fn possible_router_object_names(instance: &str) -> Vec<String> {
    vec![child_name(instance, ROUTER_COMPONENT)]
}

/// Gateway API group for the exposure routes (ADR-0080 decision 2).
///
/// `k8s-openapi` does not vendor this CRD group and there is no `gateway-api`
/// crate in the workspace, so the operator renders `HTTPRoute`/`GRPCRoute` as
/// [`DynamicObject`]s built from a [`GroupVersionKind`] rather than typed
/// structs, the standard `kube` approach for a resource kind whose CRD the
/// operator does not own.
pub const GATEWAY_API_GROUP: &str = "gateway.networking.k8s.io";

/// Gateway API version the operator targets for the exposure routes.
pub const GATEWAY_API_VERSION: &str = "v1";

/// Component suffix of the exposure HTTPRoute: `<cluster>-gateway-route`.
const GATEWAY_HTTPROUTE_COMPONENT: &str = "gateway-route";

/// Component suffix of the exposure GRPCRoute: `<cluster>-gateway-route-grpc`.
const GATEWAY_GRPCROUTE_COMPONENT: &str = "gateway-route-grpc";

/// The [`ApiResource`] for the exposure `HTTPRoute` kind, with the plural given
/// explicitly rather than guessed.
pub fn httproute_api_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(GATEWAY_API_GROUP, GATEWAY_API_VERSION, "HTTPRoute"),
        "httproutes",
    )
}

/// The [`ApiResource`] for the exposure `GRPCRoute` kind, with the plural given
/// explicitly rather than guessed.
pub fn grpcroute_api_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(GATEWAY_API_GROUP, GATEWAY_API_VERSION, "GRPCRoute"),
        "grpcroutes",
    )
}

/// Every Gateway API route name the exposure render can produce for `instance`,
/// in a fixed order, whether or not the current spec asks for them.
///
/// The controller applies the ones [`desired_gateway_routes`] returns and
/// deletes every other name in this list, so removing `exposure` (or turning
/// `grpc` off) converges back to the exposure-absent state instead of leaving an
/// orphaned route attached to the Gateway.
pub fn possible_gateway_route_names(instance: &str) -> Vec<String> {
    vec![
        child_name(instance, GATEWAY_HTTPROUTE_COMPONENT),
        child_name(instance, GATEWAY_GRPCROUTE_COMPONENT),
    ]
}

/// One Gateway API route ([`DynamicObject`]) attached to `gateway_ref`, backing
/// onto the gateway Service's `port`.
///
/// `parentRefs` carries the referenced `Gateway`'s name, and its namespace only
/// when explicitly set: an omitted `parentRefs` namespace resolves, per Gateway
/// API, to the route's own namespace, which the controller renders into the
/// `RavelCluster`'s namespace -- exactly the "defaults to the CR's own
/// namespace" contract, without this pure function needing to know the
/// namespace. `backendRefs` targets `backend_name` on `port`: the gateway
/// Service directly for every existing CR, or the ravel-native router's Service
/// under `backend: ravelNative` (ADR-0080 decision 3, deliverable 4), decided by
/// the one caller [`desired_gateway_routes`].
fn gateway_route(
    instance: &str,
    component: &str,
    api_resource: &ApiResource,
    backend_name: &str,
    port: i32,
    gateway_ref: &GatewayReference,
    hostnames: &[String],
) -> DynamicObject {
    let mut parent_ref = serde_json::json!({ "name": gateway_ref.name });
    if let Some(namespace) = gateway_ref.namespace.as_ref() {
        parent_ref["namespace"] = serde_json::Value::String(namespace.clone());
    }
    let mut route_spec = serde_json::json!({
        "parentRefs": [parent_ref],
        "rules": [{
            "backendRefs": [{
                // By numeric port: Gateway API backendRefs reference a port
                // number, not the named port an Ingress uses.
                "name": backend_name,
                "port": port,
            }],
        }],
    });
    // Empty hostnames renders no `hostnames`, which matches every hostname the
    // parent Gateway's listeners accept -- the route-level analogue of the
    // host-less Ingress rule.
    if !hostnames.is_empty() {
        route_spec["hostnames"] = serde_json::json!(hostnames);
    }
    let mut route = DynamicObject::new(&child_name(instance, component), api_resource)
        .data(serde_json::json!({ "spec": route_spec }));
    // The gateway component labels, matching the Ingress objects, so the
    // controller's managed-by-scoped watch sees these objects.
    route.metadata.labels = Some(labels(instance, "gateway"));
    route
}

/// The Gateway API exposure routes (ADR-0080 decision 2): `(HTTPRoute, GRPCRoute)`.
///
/// Returns `(None, None)` when `gateway.exposure.gatewayApi` is absent, exactly
/// mirroring [`desired_gateway_ingresses`]'s affinity-absent-renders-nothing
/// contract. When present, the HTTPRoute is always rendered and the GRPCRoute is
/// rendered only when `grpc` is true. Exposure is independent of affinity: this
/// renders whether affinity is absent, disabled, or enabled on
/// `backend: RavelNative` (the CEL rule rejects the one forbidden combination,
/// enabled `backend: ingressNginx`, at admission).
pub fn desired_gateway_routes(
    spec: &RavelClusterSpec,
    instance: &str,
    router_available: bool,
) -> (Option<DynamicObject>, Option<DynamicObject>) {
    let Some(gateway_api) = spec
        .gateway
        .exposure
        .as_ref()
        .and_then(|exposure| exposure.gateway_api.as_ref())
    else {
        return (None, None);
    };
    let gateway = child_name(instance, "gateway");
    // HTTPRoute backendRef (ADR-0080 decision 3, deliverable 4): point the HTTP
    // route at the router's Service ONLY when the router's Service will actually
    // exist after this reconcile pass -- `router_available` is threaded in from
    // `desired_objects`, which computes it from the router's own render outcome
    // this pass. A static `ravel_native_affinity(spec).is_some()` check is NOT
    // enough: on a router render error (RouterImageMissing, or F1's
    // CanonicalTenantResolverMissing from a transiently-empty token Secret) the
    // controller applies routes BEFORE the router block and its delete-sweep
    // removes every router-owned object that did not render this pass. Pointing
    // the HTTPRoute at the router Service in that pass would apply a route at a
    // Service the same pass then deletes -- an ingest outage that does not
    // self-heal. So for every other case (affinity absent/disabled, ingressNginx,
    // OR a router that will not be present this pass) fall back to the gateway
    // Service exactly as before -- zero behavior change on the healthy paths.
    let (http_backend, http_port) = if router_available {
        (child_name(instance, ROUTER_COMPONENT), ROUTER_HTTP_PORT)
    } else {
        (gateway.clone(), HTTP_PORT)
    };
    // GRPCRoute backendRef ALWAYS targets the gateway Service, regardless of
    // backend. The router deliberately renders an HTTP-only Deployment today
    // (one container port, no --listen-grpc): #184 has since added a
    // --listen-grpc flag, but ravel-ingest-router's EndpointStore holds a single
    // `port_name` per snapshot, shared by the HTTP and gRPC listeners in the
    // same process, so one router Deployment cannot proxy HTTP to the gateway's
    // `http` port and gRPC to its `grpc` port at once. Wiring gRPC through the
    // router (two Deployments pinned to different --gateway-port-name, or a
    // per-listener port surface in ravel-ingest-router) is a follow-up, not a
    // gap here; until then the GRPCRoute must keep hitting the gateway Service
    // directly so gRPC ingest keeps working exactly as it does on main.
    let (grpc_backend, grpc_port) = (gateway, GRPC_PORT);
    let httproute = gateway_route(
        instance,
        GATEWAY_HTTPROUTE_COMPONENT,
        &httproute_api_resource(),
        &http_backend,
        http_port,
        &gateway_api.gateway_ref,
        &gateway_api.hostnames,
    );
    let grpcroute = gateway_api.grpc.then(|| {
        gateway_route(
            instance,
            GATEWAY_GRPCROUTE_COMPONENT,
            &grpcroute_api_resource(),
            &grpc_backend,
            grpc_port,
            &gateway_api.gateway_ref,
            &gateway_api.hostnames,
        )
    });
    (Some(httproute), grpcroute)
}

/// A tier whose Deployment one reconcile pass applies.
///
/// The order these are applied in is a correctness concern rather than
/// cosmetics; [`GcBootstrapPlan`] decides it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentTier {
    /// The request-serving ingest tier (`--mode gateway`).
    Gateway,
    /// The request-serving query tier (`--mode query`).
    Query,
    /// The maintenance tier (`--mode maintain`).
    Maintain,
}

impl DeploymentTier {
    /// The component suffix of this tier's child objects, which is also the
    /// `--mode` value the tier runs.
    pub fn component(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Query => "query",
            Self::Maintain => "maintain",
        }
    }
}

/// Whether the cluster runs per-role storage credentials: at least one
/// per-Deployment `credentialsSecretRef` override is set (ADR-0055 section 5).
///
/// With no override every tier shares one credential, so whichever tier reaches
/// a fresh bucket first can create `sys/gc`. With any override in play the
/// credentials are the per-role policies under `deploy/iam/`, where only
/// Maintain and Admin carry `PutObject` on `sys/gc` while Gateway and Query hold
/// `GetObject` only. One override is enough to put the cluster in that world:
/// the overriding tier is the one whose grants narrowed.
pub fn per_role_credentials(spec: &RavelClusterSpec) -> bool {
    spec.gateway.credentials_secret_ref.is_some()
        || spec.query.credentials_secret_ref.is_some()
        || spec.maintain.credentials_secret_ref.is_some()
}

/// `Available=False` reason while the gateway and query Deployments are held
/// back for the `sys/gc` bootstrap. A progress state, not a failure: the pass
/// also writes `Degraded=False` under this same reason and requeues.
pub const WAITING_FOR_GC_BOOTSTRAP_REASON: &str = "WaitingForGcBootstrap";

/// Message paired with [`WAITING_FOR_GC_BOOTSTRAP_REASON`].
pub const WAITING_FOR_GC_BOOTSTRAP_MESSAGE: &str = "waiting for the maintain Deployment to report a ready replica and create sys/gc; \
     the gateway and query Deployments hold their per-role storage credentials, which \
     carry no write grant on sys/gc, so they are applied only once it exists";

/// `Degraded=True` reason once a `sys/gc` bootstrap hold has continued past
/// [`GC_BOOTSTRAP_STALL_AFTER`] without maintain reporting a ready replica. The
/// hold itself continues and keeps requeueing at `BOOTSTRAP_POLL`; this added
/// condition is the only thing that distinguishes "stuck" from "still
/// bootstrapping" (a wrong `maintain.credentialsSecretRef`, a bad image), and it
/// clears on its own once maintain reports ready.
pub const GC_BOOTSTRAP_STALLED_REASON: &str = "GcBootstrapStalled";

/// How long the gateway and query Deployments may be held for the `sys/gc`
/// bootstrap before the wait is reported stalled via
/// [`GC_BOOTSTRAP_STALLED_REASON`]. Five minutes: long enough that an ordinary
/// maintain rollout does not trip it, short enough that a genuinely stuck
/// bootstrap surfaces before an operator is left guessing.
pub const GC_BOOTSTRAP_STALL_AFTER: Duration = Duration::from_secs(5 * 60);

/// `Degraded=True` reason when per-role storage credentials are in use and
/// `maintain.enabled` is false, so no pod the operator renders holds a write
/// grant on `sys/gc`.
pub const GC_BOOTSTRAP_UNAVAILABLE_REASON: &str = "GcBootstrapUnavailable";

/// Message paired with [`GC_BOOTSTRAP_UNAVAILABLE_REASON`].
pub const GC_BOOTSTRAP_UNAVAILABLE_MESSAGE: &str = "maintain.enabled is false and the per-role storage credentials give the gateway and \
     query Deployments no write grant on sys/gc, so their pods restart until sys/gc \
     exists; enable spec.maintain, or create sys/gc with \"ravel-cli gc-config set\" \
     under the Admin credential";

/// Which `sys/gc` bootstrap ordering regime a spec is in.
///
/// Every `ravel-server` mode creates-if-absent and then validates `sys/gc` at
/// startup, so on a fresh bucket some process has to write it before any other
/// can serve. Which processes are able to is a property of the credentials the
/// spec hands them, which is what this enum reads off the spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcBootstrapGate {
    /// One shared credential across every tier, so any tier can create
    /// `sys/gc`. No ordering constraint at all.
    SharedCredential,
    /// Per-role credentials with maintain enabled: only the maintain tier can
    /// create `sys/gc`, so it is applied first and the request-serving tiers
    /// wait for it to report a ready replica.
    MaintainFirst,
    /// Per-role credentials with maintain disabled: no tier the operator renders
    /// holds a write grant on `sys/gc`. The request-serving tiers are still
    /// applied, but their pods restart until `sys/gc` is created out of band,
    /// so the pass degrades the cluster instead of waiting.
    Unavailable,
}

/// The Deployment apply ordering one reconcile pass follows, derived from the
/// spec alone.
///
/// [`crate::controller`] applies exactly [`Self::apply_sequence`]'s contents, in
/// that order and nothing else, so a test asserting on a sequence here is
/// asserting on the operator's real apply order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcBootstrapPlan {
    /// The ordering regime.
    pub gate: GcBootstrapGate,
    /// Whether `spec.maintain.enabled` is set, i.e. whether the maintain
    /// Deployment renders at all.
    pub maintain_enabled: bool,
}

impl GcBootstrapPlan {
    /// The exact sequence of Deployment applies for one pass, given the
    /// ready-replica count the live maintain Deployment reports at the start of
    /// it (`None` when that Deployment does not exist yet, which is the fresh
    /// cluster case) and whether either request-serving Deployment already
    /// exists on the cluster.
    ///
    /// Under [`GcBootstrapGate::MaintainFirst`] the hold on the request-serving
    /// tiers is only meaningful before either of them exists: once one does,
    /// its existence proves a prior pass already got past the bootstrap gate
    /// (or an older operator applied it), so holding it back would stop a
    /// serving cluster from reconciling through a maintain rollout or outage.
    ///
    /// Under [`GcBootstrapGate::SharedCredential`] this is the order the
    /// operator has always used, and both inputs are ignored: any tier can
    /// create `sys/gc` there, so waiting would only slow a fresh cluster down.
    ///
    /// Under [`GcBootstrapGate::Unavailable`] the request-serving tiers are
    /// applied unconditionally: maintain is disabled there, so there is no
    /// maintain Deployment to apply, and the controller degrades the cluster
    /// until `sys/gc` is created out of band.
    pub fn apply_sequence(
        &self,
        maintain_ready: Option<i32>,
        request_serving_exists: bool,
    ) -> Vec<DeploymentTier> {
        match self.gate {
            GcBootstrapGate::SharedCredential => {
                let mut sequence = vec![DeploymentTier::Gateway, DeploymentTier::Query];
                if self.maintain_enabled {
                    sequence.push(DeploymentTier::Maintain);
                }
                sequence
            }
            GcBootstrapGate::MaintainFirst => {
                let mut sequence = vec![DeploymentTier::Maintain];
                if maintain_ready.unwrap_or(0) > 0 || request_serving_exists {
                    sequence.push(DeploymentTier::Gateway);
                    sequence.push(DeploymentTier::Query);
                }
                sequence
            }
            GcBootstrapGate::Unavailable => {
                vec![DeploymentTier::Gateway, DeploymentTier::Query]
            }
        }
    }

    /// Whether this pass holds the gateway and query Deployments back waiting
    /// for maintain to create `sys/gc`. The pass records
    /// [`WAITING_FOR_GC_BOOTSTRAP_REASON`] and requeues when true. Only a fresh
    /// [`GcBootstrapGate::MaintainFirst`] cluster with neither request-serving
    /// Deployment yet and no ready maintain replica waits.
    pub fn waiting_for_bootstrap(
        &self,
        maintain_ready: Option<i32>,
        request_serving_exists: bool,
    ) -> bool {
        matches!(self.gate, GcBootstrapGate::MaintainFirst)
            && !request_serving_exists
            && maintain_ready.unwrap_or(0) <= 0
    }

    /// Whether the controller must read live cluster state (the maintain
    /// Deployment's ready replicas and whether a request-serving Deployment
    /// exists) before it can order this pass. False under the shared
    /// credential, so an unchanged single-credential cluster costs no extra
    /// API call.
    pub fn reads_live_state(&self) -> bool {
        matches!(self.gate, GcBootstrapGate::MaintainFirst)
    }
}

/// The [`GcBootstrapPlan`] for a spec.
pub fn gc_bootstrap_plan(spec: &RavelClusterSpec) -> GcBootstrapPlan {
    let gate = match (per_role_credentials(spec), spec.maintain.enabled) {
        (false, _) => GcBootstrapGate::SharedCredential,
        (true, true) => GcBootstrapGate::MaintainFirst,
        (true, false) => GcBootstrapGate::Unavailable,
    };
    GcBootstrapPlan {
        gate,
        maintain_enabled: spec.maintain.enabled,
    }
}

/// Component suffix of the one-shot store-qualification Job (issue #36): its
/// child name is `<cluster>-qualify`.
pub const QUALIFY_COMPONENT: &str = "qualify";

/// Annotation on the qualify Job carrying [`qualify_job_input_hash`], so a
/// change to the inputs qualification proves against (bucket, region, endpoint,
/// image, credentials Secret name and its resourceVersion) re-runs it and a
/// no-op reconcile does not.
pub const QUALIFY_SPEC_HASH_ANNOTATION: &str = "ravel.nofire.ai/qualify-spec-hash";

/// `backoffLimit` for the qualify Job: one retry absorbs a transient S3 error
/// (a backend still coming up) without spinning on a genuine qualification
/// failure (a backend that fails conditional-create atomicity or list
/// consistency). Two attempts total (the initial run plus one retry), then the
/// Job reports `Failed` and the controller surfaces it on the `StoreQualified`
/// condition.
///
/// Kept small deliberately: `activeDeadlineSeconds` is a Job-WIDE bound on the
/// total active time across every retry (it takes precedence over
/// `backoffLimit`), so the two knobs must be sized together. A larger retry
/// count would either not fit under the deadline (a slow-but-healthy attempt
/// plus a retry would trip `DeadlineExceeded` before the retry budget was
/// spent) or force the deadline so high that a hung Job ran for many minutes
/// before it was cut off. Two attempts is the value that lets one retry
/// complete a full slow-but-healthy run within
/// [`QUALIFY_JOB_ACTIVE_DEADLINE_SECONDS`].
pub const QUALIFY_JOB_BACKOFF_LIMIT: i32 = 1;

/// `ttlSecondsAfterFinished` for the qualify Job: one hour, so a finished Job
/// does not accumulate across reconciles or re-qualifications. The qualified
/// state itself is durable in `status.storeQualifiedHash`, so a Job
/// garbage-collected after success does not re-trigger qualification.
pub const QUALIFY_JOB_TTL_SECONDS: i32 = 3600;

/// `activeDeadlineSeconds` for the qualify Job: a wall-clock bound so a qualify
/// pod that hangs (an S3 endpoint that accepts the TCP connection and then
/// never answers) becomes a `Failed` Job with reason `DeadlineExceeded`
/// instead of running indefinitely. `backoffLimit` bounds only how many
/// *failed* attempts run; it does nothing for one attempt that never
/// terminates, which leaves `StoreQualified` stuck at `Pending` forever.
///
/// This deadline is JOB-WIDE, not per-attempt: Kubernetes counts it against
/// the Job's total active time summed across every retry, and it takes
/// precedence over `backoffLimit` (whichever limit is hit first fails the
/// Job). So the value must fit the intended retries end to end, not just one
/// attempt, or a slow-but-healthy first attempt plus a retry would trip
/// `DeadlineExceeded` before the retry budget ([`QUALIFY_JOB_BACKOFF_LIMIT`])
/// was ever spent.
///
/// Arithmetic. `ravel-cli store qualify` runs 28 sequential object operations
/// against the bucket: probe create-if-absent (put, put, get = 3), probe CAS
/// version (put, put, put, get = 4), probe read-after-write ([`super`]'s
/// `CONSISTENCY_CYCLES` = 5 put+get = 10), probe list-after-write (5 put+list
/// = 10), and the final `sys/qualification` create-if-absent write (1); the two
/// informational probes issue no request through the `ObjectStoreBackend`
/// contract. Each operation's per-request ceiling is the S3 client's 20 s
/// `request_timeout` (ravel-object-store `S3HttpConfig::default`), so one
/// slow-but-healthy attempt whose every operation approaches that ceiling
/// without retrying is bounded by 28 * 20 s = 560 s. Adding ~140 s per attempt
/// for pod scheduling and image pull gives a 700 s per-attempt budget. With
/// `QUALIFY_JOB_BACKOFF_LIMIT` = 1 the Job runs at most two attempts, so the
/// Job-wide deadline is 2 * 700 s = 1400 s: a slow-but-healthy initial attempt
/// AND a full retry both complete before it fires. A single hung attempt still
/// terminates, at the 1400 s Job-wide bound rather than running forever
/// (a hung endpoint stalls each operation at ~200 s = `retry_timeout` 180 s +
/// `request_timeout` 20 s).
///
/// Not part of [`qualify_job_input_hash`]: tuning this deadline (or the backoff
/// limit) must not re-run a qualification that already passed.
pub const QUALIFY_JOB_ACTIVE_DEADLINE_SECONDS: i64 = 1400;

/// `StoreQualified` reason while the qualify Job is being created or is still
/// running: serving is held until it reports success.
pub const STORE_QUALIFIED_PENDING_REASON: &str = "Pending";

/// `StoreQualified` reason once the store has been qualified for the current
/// inputs; serving proceeds.
pub const STORE_QUALIFIED_SUCCEEDED_REASON: &str = "Succeeded";

/// `StoreQualified` reason when the qualify Job exhausted its `backoffLimit`
/// without succeeding; serving stays held and the pass requeues on backoff.
pub const STORE_QUALIFIED_FAILED_REASON: &str = "Failed";

/// Message paired with [`STORE_QUALIFIED_PENDING_REASON`].
pub const STORE_QUALIFYING_MESSAGE: &str = "running store qualification (ravel-cli store qualify) against the cluster's bucket; the \
     gateway, query, and maintain Deployments are held until it succeeds so the cluster never \
     serves on a backend that fails the object-store contract (docs/object-store-contract.md)";

/// Message paired with [`STORE_QUALIFIED_SUCCEEDED_REASON`].
pub const STORE_QUALIFIED_MESSAGE: &str =
    "the object store passed ravel-cli store qualify; serving Deployments may be created";

/// A deterministic change-detection hash over the inputs store qualification
/// proves against (issue #36): the bucket, region, endpoint, server image, the
/// credentials Secret NAME, and that credentials Secret's `resourceVersion`.
/// When any of these changes the store the cluster would serve on, or the
/// credentials it would serve with, is different, so qualification is re-run; an
/// unrelated spec edit (a replica count, a fold interval) leaves this stable and
/// does not re-qualify.
///
/// The `resourceVersion` is what makes a fixed-name credential ROTATION
/// re-qualify (finding, issue #36): rotating the Secret in place keeps its name
/// but bumps its `resourceVersion`, so without it a rotation to credentials that
/// no longer pass the object-store contract would skip qualification entirely
/// once the prior Job had been TTL-garbage-collected. Only the `resourceVersion`
/// (opaque API metadata) enters the hash, never any Secret DATA: the operator
/// never reads the credential values here (it resolves only the
/// `resourceVersion`, see `controller::resolve_credential_resource_versions`),
/// and this value reaches neither the status nor a log line. The operator does
/// not watch Secrets (the per-namespace `ravel-operator-secrets` RoleBinding
/// grants `get` only, not `watch`), so a rotation is noticed on the next
/// periodic requeue (`controller::RESYNC`, 300 s): that pass reads the new
/// `resourceVersion`, this hash changes, and the gate recreates the Job. The
/// bound on noticing a rotation is therefore one `RESYNC` interval.
///
/// `blake3` is a fixed algorithm, so the hash depends only on the inputs and is
/// stable across processes AND Rust toolchain versions: neither an operator
/// restart nor a compiler upgrade spuriously re-qualifies. (std's
/// `DefaultHasher` is not stable across Rust releases, so an upgrade would have
/// re-run qualification for every cluster on unchanged inputs.) It is a change
/// signal, not a security boundary, exactly like [`secrets_checksum`].
pub fn qualify_job_input_hash(
    spec: &RavelClusterSpec,
    credentials_resource_version: Option<&str>,
) -> String {
    // Endpoint presence is significant (finding 3, issue #36): common_store_args
    // and desired_qualify_job emit the endpoint flag/env only when Some, so
    // endpoint: null and endpoint: "" select different stores, yet
    // as_deref().unwrap_or("") fed the hasher the same "" for both and hashed
    // them identically. A distinct presence byte in front of the value keeps the
    // two forms apart, so editing between them re-qualifies: 0x01 then the value
    // for Some, a lone 0x00 for None. The 0xff field separator fixes the field
    // boundary; this byte fixes presence, which the separator alone cannot.
    let endpoint = match spec.storage.s3.endpoint.as_deref() {
        Some(value) => format!("\u{1}{value}"),
        None => "\u{0}".to_string(),
    };
    // The credentials resourceVersion carries the same presence byte as the
    // endpoint (finding 3, issue #36): an unresolved Secret (None) and a Secret
    // whose resourceVersion resolved to the empty string are different states,
    // yet unwrap_or("") fed the hasher the same "" for both and hashed them
    // identically. 0x01 then the value for Some, a lone 0x00 for None keeps the
    // two apart, so the field separator's boundary is joined by a presence byte
    // the separator alone cannot supply.
    let credentials_rv = match credentials_resource_version {
        Some(value) => format!("\u{1}{value}"),
        None => "\u{0}".to_string(),
    };
    blake3_hex(&[
        spec.storage.s3.bucket.as_str(),
        spec.storage.s3.region.as_str(),
        endpoint.as_str(),
        spec.image.as_str(),
        spec.storage.s3.credentials_secret_ref.name.as_str(),
        credentials_rv.as_str(),
    ])
}

/// The one-shot store-qualification Job for a cluster (issue #36).
///
/// Runs `ravel-cli store qualify` (the same `ravel-cli` binary the server image
/// ships) against the cluster's bucket before any serving Deployment is created,
/// so a backend that fails the object-store contract is caught at deploy time
/// rather than crash-looping every server pod on a fresh bucket. Image and
/// credentials mirror the server Deployment's shared
/// `storage.s3.credentialsSecretRef`; the bucket, region, and endpoint reach
/// `ravel-cli` through the same `RAVEL_S3_*` env vars it reads (clap `env`), the
/// exact shape the kind lane's hand-run Job used.
///
/// The Job carries [`QUALIFY_SPEC_HASH_ANNOTATION`] so the controller re-runs it
/// when its inputs change and skips it when they do not, and sets
/// `restartPolicy: Never`, a small [`QUALIFY_JOB_BACKOFF_LIMIT`],
/// [`QUALIFY_JOB_ACTIVE_DEADLINE_SECONDS`] so a hung attempt fails rather than
/// runs forever, and [`QUALIFY_JOB_TTL_SECONDS`] so a finished Job does not
/// accumulate.
///
/// `credentials_resource_version` is the shared credentials Secret's resolved
/// `resourceVersion` (the controller reads it before the gate); it flows into
/// [`qualify_job_input_hash`] so the recorded annotation matches the gate's
/// desired hash and a fixed-name credential rotation re-qualifies.
pub fn desired_qualify_job(
    spec: &RavelClusterSpec,
    instance: &str,
    credentials_resource_version: Option<&str>,
) -> Job {
    let labels = labels(instance, QUALIFY_COMPONENT);
    let mut env = vec![
        EnvVar {
            name: "RAVEL_S3_BUCKET".to_string(),
            value: Some(spec.storage.s3.bucket.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "RAVEL_S3_REGION".to_string(),
            value: Some(spec.storage.s3.region.clone()),
            ..Default::default()
        },
    ];
    if let Some(endpoint) = &spec.storage.s3.endpoint {
        env.push(EnvVar {
            name: "RAVEL_S3_ENDPOINT".to_string(),
            value: Some(endpoint.clone()),
            ..Default::default()
        });
    }
    // Credentials mirror the server Deployment exactly: sourced from the shared
    // credentials Secret via secretKeyRef, never literal values.
    env.extend(s3_credential_env(spec, None));

    let container = Container {
        name: "qualify".to_string(),
        image: Some(spec.image.clone()),
        image_pull_policy: spec.image_pull_policy.clone(),
        command: Some(vec!["/usr/local/bin/ravel-cli".to_string()]),
        args: Some(vec![
            "--store".to_string(),
            "s3".to_string(),
            "store".to_string(),
            "qualify".to_string(),
        ]),
        env: Some(env),
        security_context: Some(container_security_context()),
        ..Default::default()
    };

    Job {
        metadata: ObjectMeta {
            name: Some(child_name(instance, QUALIFY_COMPONENT)),
            labels: Some(labels.clone()),
            annotations: Some(BTreeMap::from([(
                QUALIFY_SPEC_HASH_ANNOTATION.to_string(),
                qualify_job_input_hash(spec, credentials_resource_version),
            )])),
            ..Default::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(QUALIFY_JOB_BACKOFF_LIMIT),
            active_deadline_seconds: Some(QUALIFY_JOB_ACTIVE_DEADLINE_SECONDS),
            ttl_seconds_after_finished: Some(QUALIFY_JOB_TTL_SECONDS),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    restart_policy: Some("Never".to_string()),
                    containers: vec![container],
                    security_context: Some(pod_security_context()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

/// The terminal state of the live qualify Job this reconcile pass observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualifyJobPhase {
    /// Neither `Complete` nor `Failed` reported yet.
    Running,
    /// The Job reported `Complete=True`.
    Succeeded,
    /// The Job reported `Failed=True` (its `backoffLimit` was exhausted). Carries
    /// the Job's terminal condition message for the `StoreQualified` condition.
    Failed(String),
}

/// What the controller observed about the live qualify Job for a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualifyJobObservation {
    /// No qualify Job exists (a fresh cluster, or one whose finished Job has been
    /// TTL-garbage-collected).
    Absent,
    /// A qualify Job exists. `spec_hash` is its [`QUALIFY_SPEC_HASH_ANNOTATION`]
    /// value (`None` if the annotation is missing), `phase` its terminal state.
    Present {
        /// The Job's recorded input hash, or `None` when the annotation is
        /// absent.
        spec_hash: Option<String>,
        /// The Job's terminal state this pass.
        phase: QualifyJobPhase,
    },
}

/// The gate decision one reconcile pass takes on store qualification (issue #36).
///
/// [`crate::controller`] acts on exactly this, so a test asserting on a decision
/// here is asserting on the operator's real gating behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualificationDecision {
    /// The store is qualified for the current inputs: create the serving
    /// Deployments. No qualify-Job write this pass.
    Proceed,
    /// (Re)run qualification and hold the serving Deployments. `recreate` is
    /// true when a Job for stale inputs must be deleted first (a config edit
    /// landed); the controller deletes it and lets the next pass create a fresh
    /// one. False when no Job exists and one must be created now.
    Qualify {
        /// Whether a stale Job must be deleted before a fresh one is created.
        recreate: bool,
    },
    /// The qualify Job is still running: hold the serving Deployments, no write.
    Waiting,
    /// The qualify Job failed: hold the serving Deployments and requeue on
    /// backoff. Carries the Job's terminal message.
    Failed(String),
}

/// Decide the qualification gate for a pass, purely from the desired input hash,
/// the durably-recorded qualified hash (`status.storeQualifiedHash`), and the
/// live Job observation.
///
/// A recorded qualified hash equal to the desired one short-circuits to
/// [`QualificationDecision::Proceed`]: qualification already passed for these
/// inputs and is never re-run on a schedule, so the Job's absence (after TTL GC)
/// is not a reason to re-qualify. Otherwise the decision follows the live Job:
/// absent means create one; a Job for a different (or missing) hash means a
/// config edit landed and the stale Job is recreated; a Job for the current hash
/// reports the qualification's progress.
pub fn qualification_decision(
    desired_hash: &str,
    qualified_hash: Option<&str>,
    observation: &QualifyJobObservation,
) -> QualificationDecision {
    if qualified_hash == Some(desired_hash) {
        return QualificationDecision::Proceed;
    }
    match observation {
        QualifyJobObservation::Absent => QualificationDecision::Qualify { recreate: false },
        QualifyJobObservation::Present { spec_hash, phase } => {
            if spec_hash.as_deref() != Some(desired_hash) {
                QualificationDecision::Qualify { recreate: true }
            } else {
                match phase {
                    QualifyJobPhase::Running => QualificationDecision::Waiting,
                    QualifyJobPhase::Succeeded => QualificationDecision::Proceed,
                    QualifyJobPhase::Failed(message) => {
                        QualificationDecision::Failed(message.clone())
                    }
                }
            }
        }
    }
}

/// Base delay of the qualify-Job recreation backoff (issue #36, finding 3): the
/// first consecutive failure holds this long before a fresh Job is created. Equal
/// to the controller's failure requeue, so a single failure behaves as it did
/// before the backoff existed.
pub const QUALIFY_RETRY_BASE_SECONDS: i64 = 30;

/// Ceiling of the capped exponential qualify-Job recreation backoff: the
/// per-failure hold doubles from [`QUALIFY_RETRY_BASE_SECONDS`] but never exceeds
/// this, so a persistently failing store is retried at most this often before the
/// terminal cooldown takes over.
pub const QUALIFY_RETRY_CEILING_SECONDS: i64 = 480;

/// Consecutive failures after which the gate stops recreating on the exponential
/// backoff and holds in the terminal cooldown ([`QUALIFY_RETRY_COOLDOWN_SECONDS`])
/// instead. Bounds cross-Job churn: `backoffLimit` and `activeDeadlineSeconds`
/// bound attempts WITHIN one Job only, so without this an unchanged store that
/// keeps failing qualification would be re-Jobbed forever on the backoff.
pub const QUALIFY_RETRY_TERMINAL_THRESHOLD: i32 = 6;

/// Terminal-cooldown hold once [`QUALIFY_RETRY_TERMINAL_THRESHOLD`] consecutive
/// failures are reached: one hour. During it the gate creates no Job; only an
/// input change (the qualified-input hash moving, which resets the count) or the
/// cooldown's own expiry lets qualification run again. Equal to the qualify Job's
/// [`QUALIFY_JOB_TTL_SECONDS`], so the Failed Job that records which inputs are
/// failing survives most of the hold and a config edit during the cooldown is
/// seen as a stale Job and resets at once.
pub const QUALIFY_RETRY_COOLDOWN_SECONDS: i64 = 3600;

/// Seconds to hold before the next qualify-Job recreation after `failure_count`
/// consecutive failures (issue #36, finding 3).
///
/// Capped exponential for the first [`QUALIFY_RETRY_TERMINAL_THRESHOLD`] - 1
/// failures: [`QUALIFY_RETRY_BASE_SECONDS`] doubling each failure, clamped to
/// [`QUALIFY_RETRY_CEILING_SECONDS`]. At or beyond the threshold the hold is the
/// terminal cooldown [`QUALIFY_RETRY_COOLDOWN_SECONDS`]. `failure_count` is the
/// number of failures INCLUDING the one just observed (1 for the first). The
/// exact sequence for counts 1..=7 is 30, 60, 120, 240, 480, 3600, 3600.
pub fn qualify_retry_backoff_seconds(failure_count: i32) -> i64 {
    if failure_count >= QUALIFY_RETRY_TERMINAL_THRESHOLD {
        return QUALIFY_RETRY_COOLDOWN_SECONDS;
    }
    let doublings = u32::try_from(failure_count - 1).unwrap_or(0).min(31);
    let scaled = QUALIFY_RETRY_BASE_SECONDS
        .checked_shl(doublings)
        .unwrap_or(QUALIFY_RETRY_CEILING_SECONDS);
    scaled.clamp(QUALIFY_RETRY_BASE_SECONDS, QUALIFY_RETRY_CEILING_SECONDS)
}

/// The qualify-Job mutation a non-Proceed pass performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualifyJobAction {
    /// Create the qualify Job for the current inputs: none exists and the backoff
    /// (if any) has elapsed.
    Create,
    /// Delete the existing Job with foreground propagation. Used when a config
    /// edit made the Job's inputs stale, and when a Failed Job's backoff has
    /// elapsed and it must be recreated.
    DeleteStale,
    /// Leave the Job untouched: it is running, or it is the Failed record kept in
    /// place during the backoff hold.
    None,
}

/// The `StoreQualified` reason a non-Proceed pass records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualifyStoreReason {
    /// Qualification is in progress (creating or running the Job).
    Pending,
    /// The last attempt failed; the gate is holding on the retry backoff or the
    /// terminal cooldown.
    Failed,
}

/// The retry-aware plan for a reconcile pass that did NOT reach
/// [`QualificationDecision::Proceed`] (issue #36, finding 3). Pure over the
/// persisted retry state and an injected `now_unix`, so the backoff schedule, the
/// terminal cooldown, and the input-change reset are unit-tested without a clock.
/// The controller performs `action`, records a `StoreQualified=False` condition
/// from `reason`/`job_message`, persists `failure_count`/`next_retry_unix`, and
/// requeues after `requeue_seconds`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifyGatePlan {
    /// The Job mutation to perform.
    pub action: QualifyJobAction,
    /// The `StoreQualified` reason.
    pub reason: QualifyStoreReason,
    /// The failed Job's terminal message when this pass observed it, carried onto
    /// the condition; `None` on the Pending arms and while holding without a Job
    /// to read.
    pub job_message: Option<String>,
    /// Failure count to persist (`None` clears the field, on the input-change
    /// reset and on a fresh cluster).
    pub failure_count: Option<i32>,
    /// Next-retry instant to persist as RFC3339 (`None` clears the field).
    pub next_retry_unix: Option<i64>,
    /// The qualified-input hash the persisted retry state is keyed to (`None`
    /// clears the field). Set whenever `failure_count` is, so a later pass can
    /// tell whether the desired inputs still match the ones the failures were
    /// recorded against.
    pub retry_hash: Option<String>,
    /// Requeue delay in seconds.
    pub requeue_seconds: i64,
}

/// Map a non-Proceed [`QualificationDecision`] plus the persisted retry state to
/// its [`QualifyGatePlan`], bounding cross-Job churn (issue #36, finding 3).
///
/// The naive gate deleted a Failed Job, requeued on the failure backoff, saw the
/// Job Absent next pass, and created a fresh one, forever: the Job's own
/// `backoffLimit`/`activeDeadlineSeconds` bound attempts WITHIN one Job, never the
/// number of Jobs. This function bounds the recreations. Each consecutive failure
/// schedules the next recreation on a capped exponential backoff
/// ([`qualify_retry_backoff_seconds`]); after [`QUALIFY_RETRY_TERMINAL_THRESHOLD`]
/// failures the gate holds in a terminal cooldown that only an input change (which
/// resets the count) or the cooldown's expiry clears.
///
/// The persisted retry state is keyed by `retry_hash`, the qualified-input hash
/// the failures were recorded against. An input change resets the budget and
/// qualifies the new inputs at once whether or not the failing Job still exists:
/// when `desired_hash` differs from `retry_hash` the count and next-retry are
/// dropped before the decision is planned, so a config edit made AFTER the Failed
/// Job's TTL collected it (both a changed and an unchanged pass then observe the
/// Job absent, so the Job's presence cannot distinguish them) qualifies at once
/// rather than waiting out the old inputs' cooldown and inheriting their count.
/// The cooldown is kept only while the hashes match. A stale Job that is still
/// present ([`QualificationDecision::Qualify`] `recreate: true`) is deleted on the
/// same pass. The retry hash is set in the plan whenever the failure count is, so
/// the controller persists the two together.
///
/// Deleting the Job clears no retry state: the count is cleared only on the
/// success path (in the controller) or on an input change, and `next_retry_unix`
/// is cleared only when the replacement Job is actually created (the Absent
/// recreation arm), never in the delete arm, so a failed Job that lingers under
/// foreground deletion is not counted twice.
pub fn plan_qualify_gate(
    decision: &QualificationDecision,
    desired_hash: &str,
    retry_hash: Option<&str>,
    failure_count: i32,
    next_retry_unix: Option<i64>,
    now_unix: i64,
    poll_seconds: i64,
) -> QualifyGatePlan {
    // The persisted retry budget was recorded against `retry_hash`. If the desired
    // inputs have moved since, that budget belongs to inputs no longer desired:
    // drop it so the new inputs qualify from a clean slate at once, independent of
    // whether the failing Job survives. Keep it only when the hashes match.
    let (failure_count, next_retry_unix) = if retry_hash == Some(desired_hash) {
        (failure_count, next_retry_unix)
    } else {
        (0, None)
    };
    let kept_count = (failure_count > 0).then_some(failure_count);
    let mut plan = match decision {
        // A Job for stale inputs is present: a config edit landed. Delete it and
        // reset the retry budget so the fresh inputs qualify from a clean slate,
        // at once (the next pass sees Absent with a zero count and creates one).
        QualificationDecision::Qualify { recreate: true } => QualifyGatePlan {
            action: QualifyJobAction::DeleteStale,
            reason: QualifyStoreReason::Pending,
            job_message: None,
            failure_count: None,
            next_retry_unix: None,
            retry_hash: None,
            requeue_seconds: poll_seconds,
        },
        // No Job exists: a fresh cluster (count 0) creates one now; after a failure
        // this is the recreation point, gated on the backoff having elapsed.
        QualificationDecision::Qualify { recreate: false } => match next_retry_unix {
            Some(due) if now_unix < due => QualifyGatePlan {
                action: QualifyJobAction::None,
                reason: QualifyStoreReason::Failed,
                job_message: None,
                failure_count: kept_count,
                next_retry_unix: Some(due),
                retry_hash: None,
                requeue_seconds: (due - now_unix).max(1),
            },
            // Backoff elapsed (or never set): create the replacement Job and clear
            // next_retry so the new attempt's failure is counted afresh. The count
            // is kept; only success or an input change resets it.
            _ => QualifyGatePlan {
                action: QualifyJobAction::Create,
                reason: if failure_count > 0 {
                    QualifyStoreReason::Failed
                } else {
                    QualifyStoreReason::Pending
                },
                job_message: None,
                failure_count: kept_count,
                next_retry_unix: None,
                retry_hash: None,
                requeue_seconds: poll_seconds,
            },
        },
        // The Job is still running: hold, touch neither the Job nor the retry
        // state.
        QualificationDecision::Waiting => QualifyGatePlan {
            action: QualifyJobAction::None,
            reason: QualifyStoreReason::Pending,
            job_message: None,
            failure_count: kept_count,
            next_retry_unix,
            retry_hash: None,
            requeue_seconds: poll_seconds,
        },
        // The Job for the current inputs Failed and is Present.
        QualificationDecision::Failed(message) => match next_retry_unix {
            // A newly-observed failure (no backoff scheduled): count it once and
            // schedule the backoff. Keep the Job as the failure record.
            None => {
                let count = failure_count.saturating_add(1);
                let delay = qualify_retry_backoff_seconds(count);
                QualifyGatePlan {
                    action: QualifyJobAction::None,
                    reason: QualifyStoreReason::Failed,
                    job_message: Some(message.clone()),
                    failure_count: Some(count),
                    next_retry_unix: Some(now_unix + delay),
                    retry_hash: None,
                    requeue_seconds: delay,
                }
            }
            // Still holding: keep the Job and the count, do not re-count.
            Some(due) if now_unix < due => QualifyGatePlan {
                action: QualifyJobAction::None,
                reason: QualifyStoreReason::Failed,
                job_message: Some(message.clone()),
                failure_count: kept_count,
                next_retry_unix: Some(due),
                retry_hash: None,
                requeue_seconds: (due - now_unix).max(1),
            },
            // Backoff elapsed: delete the Failed Job so a later pass sees Absent
            // and recreates. Keep next_retry set (the Absent arm clears it on the
            // actual create) so a Job lingering under foreground deletion is not
            // counted a second time.
            Some(due) => QualifyGatePlan {
                action: QualifyJobAction::DeleteStale,
                reason: QualifyStoreReason::Failed,
                job_message: Some(message.clone()),
                failure_count: kept_count,
                next_retry_unix: Some(due),
                retry_hash: None,
                requeue_seconds: poll_seconds,
            },
        },
        QualificationDecision::Proceed => {
            unreachable!("Proceed is handled on the success path, never planned as a hold")
        }
    };
    // Key the persisted retry state to the inputs it was recorded against: set the
    // hash exactly when a failure count is persisted, so a later pass can compare
    // the desired hash against it and reset on an input change even after the
    // Failed Job's TTL collected it.
    plan.retry_hash = plan.failure_count.map(|_| desired_hash.to_string());
    plan
}

/// The [`QualifyJobPhase`] a live Job reports, read from its status conditions:
/// `Complete=True` is success, `Failed=True` is failure, anything else is still
/// running.
///
/// A `Failed` condition's `reason` is prepended to its `message` (`"<reason>:
/// <message>"`) so the `StoreQualified` condition names why the Job failed, not
/// only the human message. This matters for a deadline-exceeded Job: the Job
/// controller sets reason `DeadlineExceeded` with a generic message ("Job was
/// active longer than specified deadline"), and the reason is the part an
/// operator needs to see. When only one of reason/message is present that one
/// is used verbatim; when neither is, a fixed fallback is.
pub fn qualify_job_phase(job: &Job) -> QualifyJobPhase {
    let Some(conditions) = job.status.as_ref().and_then(|s| s.conditions.as_ref()) else {
        return QualifyJobPhase::Running;
    };
    for c in conditions {
        if c.status != "True" {
            continue;
        }
        match c.type_.as_str() {
            "Complete" => return QualifyJobPhase::Succeeded,
            "Failed" => {
                let reason = c.reason.clone().filter(|r| !r.is_empty());
                let message = c.message.clone().filter(|m| !m.is_empty());
                let text = match (reason, message) {
                    (Some(reason), Some(message)) => format!("{reason}: {message}"),
                    (Some(reason), None) => reason,
                    (None, Some(message)) => message,
                    (None, None) => "the store qualification Job failed".to_string(),
                };
                return QualifyJobPhase::Failed(text);
            }
            _ => {}
        }
    }
    QualifyJobPhase::Running
}

/// A tier's PodDisruptionBudget (issue #126, deliverable 4), capping how many of
/// its pods a voluntary disruption (a node drain, a cluster upgrade) may take
/// down at once.
///
/// `maxUnavailable: 1` rather than a `minAvailable`: it protects a multi-replica
/// tier (gateway/query) during a rolling node drain while never blocking a drain
/// on a single-replica tier (maintain, or a one-replica gateway on a single-node
/// `kind` cluster). A `minAvailable` equal to the replica count would wedge a
/// node drain on such a cluster, the same failure the preferred (not required)
/// anti-affinity avoids. The selector matches the tier's own pods by the
/// standard `instance`+`component` labels, exactly as its Deployment selector
/// does.
fn pod_disruption_budget(instance: &str, component: &str) -> PodDisruptionBudget {
    let labels = labels(instance, component);
    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(child_name(instance, component)),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            max_unavailable: Some(IntOrString::Int(1)),
            selector: Some(LabelSelector {
                match_labels: Some(labels),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: None,
    }
}

/// One PodDisruptionBudget per rendered tier (issue #126, deliverable 4): always
/// gateway and query, plus maintain when `maintain.enabled`. The count equals
/// the number of tiers the reconcile renders a Deployment for. The controller
/// applies and sweeps these exactly like the Deployments, so a manual patch does
/// not survive a reconcile. The router is an optional affinity backend, not a
/// tier, and gets no PDB (its Deployment is swept as a unit with its other
/// objects when the backend is switched away).
pub fn desired_pod_disruption_budgets(
    spec: &RavelClusterSpec,
    instance: &str,
) -> Vec<PodDisruptionBudget> {
    let mut tiers = vec![DeploymentTier::Gateway, DeploymentTier::Query];
    if spec.maintain.enabled {
        tiers.push(DeploymentTier::Maintain);
    }
    tiers
        .into_iter()
        .map(|tier| pod_disruption_budget(instance, tier.component()))
        .collect()
}

/// Every PodDisruptionBudget name the render can produce for `instance`, in a
/// fixed order, whether or not the current spec renders that tier. The
/// controller applies the ones [`desired_pod_disruption_budgets`] returns and
/// deletes every other name here, so disabling `maintain` removes its PDB
/// instead of orphaning one that guards a Deployment that no longer exists.
pub fn possible_pod_disruption_budget_names(instance: &str) -> Vec<String> {
    [
        DeploymentTier::Gateway,
        DeploymentTier::Query,
        DeploymentTier::Maintain,
    ]
    .into_iter()
    .map(|tier| child_name(instance, tier.component()))
    .collect()
}

/// Everything one `RavelCluster` reconcile applies, rendered in one place.
///
/// [`crate::controller`] builds this and applies exactly its contents, so a
/// test that asserts on a `DesiredObjects` is asserting on the operator's real
/// render path rather than on a helper that nothing calls.
#[derive(Debug, Clone)]
pub struct DesiredObjects {
    /// The gateway Deployment.
    pub gateway_deployment: Deployment,
    /// The gateway Service.
    pub gateway_service: Service,
    /// The ingest-affinity Ingress objects; empty when affinity is off.
    pub gateway_ingresses: Vec<Ingress>,
    /// The Gateway API exposure routes `(HTTPRoute, GRPCRoute)` (ADR-0080
    /// decision 2); each `None` when exposure is off or, for the GRPCRoute, when
    /// `grpc` is false.
    pub gateway_routes: (Option<DynamicObject>, Option<DynamicObject>),
    /// The ravel-native ingest router's objects (ADR-0080 decision 3); every
    /// field `None` unless `backend: ravelNative` affinity is enabled. Rendered
    /// as a set so the controller applies (and sweeps) them together.
    pub router_deployment: Option<Deployment>,
    /// The router's Service.
    pub router_service: Option<Service>,
    /// The router's ServiceAccount.
    pub router_service_account: Option<ServiceAccount>,
    /// The router's namespaced Role.
    pub router_role: Option<Role>,
    /// The router's namespaced RoleBinding.
    pub router_role_binding: Option<RoleBinding>,
    /// A render error from the router path (a missing `routerImage`, or
    /// `canonicalTenant` with no resolver), or `None` when the router rendered
    /// cleanly or was not requested.
    ///
    /// Captured here rather than propagated out of [`desired_objects`] so a
    /// misconfigured router degrades ONLY the router: when this is `Some`, every
    /// `router_*` field above is `None` (render nothing for the router), the
    /// controller records a `Degraded` condition, and every other tier
    /// (gateway/query/maintain) still reconciles. Propagating it instead would
    /// abort the whole reconcile (ADR-0080 decision 3; crd.rs documents "renders
    /// no router objects" when misconfigured, i.e. everything else still runs).
    pub router_render_error: Option<RenderError>,
    /// The query Deployment.
    pub query_deployment: Deployment,
    /// The query Service.
    pub query_service: Service,
    /// The maintain Deployment, or `None` when `maintain.enabled` is false (the
    /// controller deletes it in that case).
    pub maintain_deployment: Option<Deployment>,
    /// One PodDisruptionBudget per rendered tier (issue #126, deliverable 4).
    /// The controller applies and sweeps these like the Deployments, so a manual
    /// patch does not survive a reconcile.
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    /// The order this pass applies the three Deployments in, and the condition
    /// it records while an ordering constraint holds the request-serving tiers
    /// back.
    pub gc_bootstrap: GcBootstrapPlan,
}

/// Render every Kubernetes object a `RavelCluster` reconcile applies.
///
/// Returns `Result` for a render error that must fail the whole reconcile: an
/// invalid `spec.gc` horizon (#1083) propagates here so the controller records a
/// `Degraded` condition rather than shipping a maintain pod with a flag the
/// server rejects at startup. The router's render error (the ravel-native
/// router's, ADR-0080 decision 3) is instead captured in
/// [`DesiredObjects::router_render_error`] rather than propagated, so a
/// misconfigured router degrades only the router and every other tier still
/// reconciles. Both paths end at a `Degraded` condition; they differ only in
/// blast radius.
pub fn desired_objects(
    spec: &RavelClusterSpec,
    instance: &str,
    namespace: &str,
    ctx: &RenderCtx,
) -> Result<DesiredObjects, RenderError> {
    // Capture the router render result instead of `?`-propagating it: a router
    // misconfiguration must not abort the gateway/query/maintain tiers below.
    // On error, render none of the router objects (all `None`) and surface the
    // error for the controller to degrade the router alone.
    let (
        router_deployment,
        router_service,
        router_service_account,
        router_role,
        router_role_binding,
        router_render_error,
    ) = match desired_router_deployment(spec, instance, namespace, ctx) {
        Ok(deployment) => (
            deployment,
            desired_router_service(spec, instance),
            desired_router_service_account(spec, instance),
            desired_router_role(spec, instance),
            desired_router_role_binding(spec, instance, namespace),
            None,
        ),
        Err(err) => (None, None, None, None, None, Some(err)),
    };
    // The HTTPRoute may only point at the router's Service when that Service will
    // actually exist after this pass. `router_service.is_some()` is exactly that:
    // it is `Some` only under `backend: ravelNative` AND a clean router render
    // (the error branch above set it to `None`), so it flips to `false` on any
    // router render error this pass. See `desired_gateway_routes` for why a static
    // spec check would strand the route at a deleted Service.
    let router_available = router_service.is_some();
    // The gateway and query Deployments always render; the bootstrap plan only
    // changes the order the controller applies the three tiers in and, under
    // `GcBootstrapGate::Unavailable`, records a Degraded condition while their
    // pods restart waiting on `sys/gc`. Rendering them even there is what lets
    // the operator's own remedy (create `sys/gc` out of band) unblock the
    // cluster: the Deployments exist, so they become ready once the object does.
    let gc_bootstrap = gc_bootstrap_plan(spec);
    Ok(DesiredObjects {
        gateway_deployment: desired_gateway_deployment(spec, instance, ctx),
        gateway_service: desired_gateway_service(spec, instance),
        gateway_ingresses: desired_gateway_ingresses(spec, instance),
        gateway_routes: desired_gateway_routes(spec, instance, router_available),
        router_deployment,
        router_service,
        router_service_account,
        router_role,
        router_role_binding,
        router_render_error,
        query_deployment: desired_query_deployment(spec, instance, ctx),
        query_service: desired_query_service(spec, instance),
        maintain_deployment: desired_maintain_deployment(spec, instance, ctx)?,
        pod_disruption_budgets: desired_pod_disruption_budgets(spec, instance),
        gc_bootstrap,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::crd::{
        AffinityBackend, AffinityKeySpec, DEFAULT_AFFINITY_SUBSET_SIZE, FoldSpec,
        GatewayApiExposureSpec, GatewayExposureSpec, GatewayReference, GatewaySpec, GcSpec,
        IngestAffinitySpec, LocalSecretRef, MaintainSpec, QuerySpec, RetentionSpec, S3Spec,
        StorageSpec,
    };
    use std::collections::BTreeMap;

    /// A fully-populated spec used as a test baseline; individual tests tweak
    /// fields on a clone.
    fn base_spec() -> RavelClusterSpec {
        RavelClusterSpec {
            image: "registry.example/ravel:v1".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            shards: 8,
            storage: StorageSpec {
                s3: S3Spec {
                    bucket: "ravel-data".to_string(),
                    region: "eu-west-1".to_string(),
                    endpoint: Some("http://minio:9000".to_string()),
                    credentials_secret_ref: LocalSecretRef {
                        name: "ravel-s3".to_string(),
                    },
                },
            },
            tenant_tokens_secret_ref: Some(LocalSecretRef {
                name: "ravel-tokens".to_string(),
            }),
            deployment_key_secret_ref: None,
            audit_token_key_secret_ref: None,
            gateway: GatewaySpec {
                replicas: 3,
                resources: None,
                credentials_secret_ref: None,
                fold: Some(FoldSpec {
                    disabled: false,
                    interval_secs: Some(120),
                }),
                ingest_affinity: None,
                exposure: None,
            },
            query: QuerySpec {
                replicas: 2,
                resources: None,
                credentials_secret_ref: None,
            },
            maintain: MaintainSpec {
                enabled: true,
                replicas: 1,
                interval_secs: Some(600),
                resources: None,
                credentials_secret_ref: None,
            },
            gc: None,
            retention: Some(RetentionSpec {
                default: Some("30d".to_string()),
                tenants: BTreeMap::from([("acme".to_string(), "7d".to_string())]),
            }),
            shard_overrides: None,
        }
    }

    fn ctx() -> RenderCtx {
        // Baseline ctx: the shared credential Secret ("ravel-s3", from
        // `base_spec`) has a resourceVersion, as does the token Secret. Tests
        // exercising per-tier overrides add the override Secrets' versions.
        RenderCtx {
            tenant_names: vec!["acme".to_string(), "globex".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: BTreeMap::from([(
                "ravel-s3".to_string(),
                "cred-1".to_string(),
            )]),
            deployment_key_resource_version: None,
            audit_token_key_resource_version: None,
        }
    }

    /// The pod-template [`SECRETS_CHECKSUM_ANNOTATION`] value on a Deployment.
    fn checksum_of(dep: &Deployment) -> Option<String> {
        dep.spec
            .as_ref()?
            .template
            .metadata
            .as_ref()?
            .annotations
            .as_ref()?
            .get(SECRETS_CHECKSUM_ANNOTATION)
            .cloned()
    }

    /// The `secretKeyRef` Secret name backing an env var on a Deployment.
    fn env_secret_name(dep: &Deployment, var: &str) -> Option<String> {
        container_of(dep)
            .env
            .as_ref()?
            .iter()
            .find(|e| e.name == var)?
            .value_from
            .as_ref()?
            .secret_key_ref
            .as_ref()
            .map(|s| s.name.clone())
    }

    /// Read a container's args as a single joined string for substring checks.
    fn container_of(dep: &Deployment) -> &Container {
        &dep.spec
            .as_ref()
            .expect("spec")
            .template
            .spec
            .as_ref()
            .expect("pod spec")
            .containers[0]
    }

    fn args_of(dep: &Deployment) -> Vec<String> {
        container_of(dep).args.clone().expect("args")
    }

    /// Find the value following the first occurrence of `flag` in an arg list.
    fn arg_value(args: &[String], flag: &str) -> Option<String> {
        args.iter()
            .position(|a| a == flag)
            .map(|i| args[i + 1].clone())
    }

    #[test]
    fn shards_render_identically_into_gateway_and_query() {
        // The whole point of the single CRD field (ADR-0034 decision 2): the
        // gateway and query --shards values come from one source and cannot
        // disagree.
        let spec = base_spec();
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let q = desired_query_deployment(&spec, "prod", &ctx());
        assert_eq!(arg_value(&args_of(&g), "--shards").as_deref(), Some("8"));
        assert_eq!(arg_value(&args_of(&q), "--shards").as_deref(), Some("8"));
        // And also into maintain, which sweeps every shard.
        let m = desired_maintain_deployment(&spec, "prod", &ctx())
            .expect("no gc render error")
            .expect("maintain enabled");
        assert_eq!(arg_value(&args_of(&m), "--shards").as_deref(), Some("8"));
    }

    #[test]
    fn gc_horizons_render_the_two_flags_on_maintain_only() {
        // #1083. protectionHorizon and grace on spec.gc render as
        // --gc-protection-horizon and --gc-grace with those exact values on the
        // maintain container, and on no other tier: only maintain validates
        // itself against sys/gc, so only maintain carries the horizons.
        let mut spec = base_spec();
        spec.gc = Some(GcSpec {
            protection_horizon: Some("26h".to_string()),
            grace: Some("25h".to_string()),
        });
        let m = desired_maintain_deployment(&spec, "prod", &ctx())
            .expect("no gc render error")
            .expect("maintain enabled");
        let margs = args_of(&m);
        assert_eq!(
            arg_value(&margs, "--gc-protection-horizon").as_deref(),
            Some("26h"),
            "maintain must render the protection horizon verbatim: {margs:?}"
        );
        assert_eq!(
            arg_value(&margs, "--gc-grace").as_deref(),
            Some("25h"),
            "maintain must render the grace verbatim: {margs:?}"
        );

        // Neither flag reaches gateway or query.
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let q = desired_query_deployment(&spec, "prod", &ctx());
        for dep in [&g, &q] {
            let args = args_of(dep);
            assert!(
                !args.iter().any(|a| a.starts_with("--gc-")),
                "no --gc- flag belongs on gateway/query: {args:?}"
            );
        }
    }

    #[test]
    fn unset_gc_renders_no_gc_flag_on_any_container() {
        // #1083. With spec.gc unset the maintain pod carries no --gc- flag at
        // all, so the server's compiled-in default applies, byte-identical to a
        // cluster that never set it. Asserted as the exact absence of any
        // --gc-* arg, not a count.
        let spec = base_spec();
        assert_eq!(spec.gc, None);
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let q = desired_query_deployment(&spec, "prod", &ctx());
        let m = desired_maintain_deployment(&spec, "prod", &ctx())
            .expect("no gc render error")
            .expect("maintain enabled");
        for dep in [&g, &q, &m] {
            let args = args_of(dep);
            assert!(
                !args.iter().any(|a| a.starts_with("--gc-")),
                "unset spec.gc must render no --gc- flag: {args:?}"
            );
        }
    }

    #[test]
    fn invalid_gc_horizon_is_a_render_error_even_with_maintain_disabled() {
        // With no maintain Deployment to render, an invalid spec.gc would
        // otherwise pass silently and only surface once the tier is enabled.
        let mut spec = base_spec();
        spec.maintain.enabled = false;
        spec.gc = Some(GcSpec {
            protection_horizon: None,
            grace: Some("not-a-duration".to_string()),
        });
        assert_eq!(
            desired_maintain_deployment(&spec, "prod", &ctx())
                .expect_err("an invalid grace must be a render error with maintain disabled"),
            RenderError::GcHorizonInvalid {
                field: "grace",
                value: "not-a-duration".to_string(),
            }
        );
        assert!(
            matches!(
                desired_objects(&spec, "prod", "ns", &ctx()),
                Err(RenderError::GcHorizonInvalid { field: "grace", .. })
            ),
            "the error must propagate out of desired_objects with maintain disabled"
        );
    }

    #[test]
    fn zero_gc_horizon_is_rejected_like_the_server_rejects_it() {
        // ravel-server refuses a non-positive --gc-* duration at startup, so a
        // zero must be a render error rather than a flag on a crash-looping pod.
        for (horizon, grace, field, value) in [
            (Some("0s"), None, "protectionHorizon", "0s"),
            (Some("24h"), Some("0h"), "grace", "0h"),
        ] {
            let mut spec = base_spec();
            spec.gc = Some(GcSpec {
                protection_horizon: horizon.map(str::to_string),
                grace: grace.map(str::to_string),
            });
            assert_eq!(
                desired_maintain_deployment(&spec, "prod", &ctx())
                    .expect_err("a zero duration must be a render error"),
                RenderError::GcHorizonInvalid {
                    field,
                    value: value.to_string(),
                }
            );
        }
        // A positive value still renders.
        let mut spec = base_spec();
        spec.gc = Some(GcSpec {
            protection_horizon: Some("1s".to_string()),
            grace: None,
        });
        assert!(
            desired_maintain_deployment(&spec, "prod", &ctx())
                .expect("a positive duration renders")
                .is_some()
        );
    }

    #[test]
    fn invalid_gc_horizon_is_a_render_error_naming_the_field() {
        // #1083. A non-duration or empty value must be a typed render error
        // naming the field, never a flag the maintain pod would reject at
        // startup. The error propagates out of desired_objects, so the
        // controller degrades the cluster instead of shipping a broken pod.
        let mut spec = base_spec();
        spec.gc = Some(GcSpec {
            protection_horizon: Some("not-a-duration".to_string()),
            grace: None,
        });
        assert_eq!(
            desired_maintain_deployment(&spec, "prod", &ctx())
                .expect_err("an invalid horizon must be a render error"),
            RenderError::GcHorizonInvalid {
                field: "protectionHorizon",
                value: "not-a-duration".to_string(),
            }
        );
        assert!(
            matches!(
                desired_objects(&spec, "prod", "ns", &ctx()),
                Err(RenderError::GcHorizonInvalid {
                    field: "protectionHorizon",
                    ..
                })
            ),
            "an invalid horizon must propagate out of desired_objects"
        );

        // An empty grace names grace specifically, not the horizon.
        let mut spec = base_spec();
        spec.gc = Some(GcSpec {
            protection_horizon: Some("24h".to_string()),
            grace: Some(String::new()),
        });
        assert_eq!(
            desired_maintain_deployment(&spec, "prod", &ctx())
                .expect_err("an empty grace must be a render error"),
            RenderError::GcHorizonInvalid {
                field: "grace",
                value: String::new(),
            }
        );

        // The error text names the field, so the Degraded message the
        // controller derives from it points the operator at the exact knob.
        assert!(
            RenderError::GcHorizonInvalid {
                field: "protectionHorizon",
                value: "nope".to_string(),
            }
            .to_string()
            .contains("spec.gc.protectionHorizon")
        );
    }

    #[test]
    fn keyless_spec_every_deployment_carries_tenant_hash_unkeyed() {
        // ADR-0050 section 3: a fresh bucket refuses to start unless told a
        // scheme. A spec with no deploymentKeySecretRef must keep every
        // deployment on --tenant-hash-unkeyed (today's v1-unkeyed behavior),
        // never silently switching to keyed.
        let spec = base_spec();
        assert!(spec.deployment_key_secret_ref.is_none());
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let q = desired_query_deployment(&spec, "prod", &ctx());
        let m = desired_maintain_deployment(&spec, "prod", &ctx())
            .expect("no gc render error")
            .expect("maintain enabled");
        for dep in [&g, &q, &m] {
            let args = args_of(dep);
            assert!(
                args.iter().any(|a| a == "--tenant-hash-unkeyed"),
                "missing --tenant-hash-unkeyed in {args:?}"
            );
            assert!(
                !args.iter().any(|a| a == "--tenant-hash-key-file"),
                "keyless spec must not render --tenant-hash-key-file: {args:?}"
            );
            let pod_spec = dep
                .spec
                .as_ref()
                .expect("spec")
                .template
                .spec
                .as_ref()
                .expect("pod spec");
            assert!(
                pod_spec.volumes.is_none(),
                "keyless spec must mount no deployment-key volume"
            );
        }
    }

    #[test]
    fn deployment_key_spec_renders_tenant_hash_key_file_and_mounts_the_secret() {
        // ADR-0072 decision 4: when deploymentKeySecretRef is set, every
        // deployment must render --tenant-hash-key-file pointed at the mounted
        // Secret instead of --tenant-hash-unkeyed, and the pod must carry a
        // read-only Volume/VolumeMount sourcing that Secret's `key` entry.
        let mut spec = base_spec();
        spec.deployment_key_secret_ref = Some(LocalSecretRef {
            name: "ravel-deployment-key".to_string(),
        });
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let q = desired_query_deployment(&spec, "prod", &ctx());
        let m = desired_maintain_deployment(&spec, "prod", &ctx())
            .expect("no gc render error")
            .expect("maintain enabled");
        for dep in [&g, &q, &m] {
            let args = args_of(dep);
            assert!(
                !args.iter().any(|a| a == "--tenant-hash-unkeyed"),
                "deployment-key spec must not render --tenant-hash-unkeyed: {args:?}"
            );
            assert_eq!(
                arg_value(&args, "--tenant-hash-key-file").as_deref(),
                Some("/etc/ravel/deployment-key/key"),
                "args: {args:?}"
            );

            let pod_spec = dep
                .spec
                .as_ref()
                .expect("spec")
                .template
                .spec
                .as_ref()
                .expect("pod spec");
            let volumes = pod_spec.volumes.as_ref().expect("volumes");
            assert_eq!(volumes.len(), 1);
            let secret = volumes[0].secret.as_ref().expect("secret volume source");
            assert_eq!(secret.secret_name.as_deref(), Some("ravel-deployment-key"));
            let items = secret.items.as_ref().expect("items");
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].key, "key");
            assert_eq!(items[0].path, "key");

            let mounts = container_of(dep).volume_mounts.as_ref().expect("mounts");
            assert_eq!(mounts.len(), 1);
            assert_eq!(mounts[0].mount_path, "/etc/ravel/deployment-key");
            assert_eq!(mounts[0].read_only, Some(true));
        }
    }

    #[test]
    fn builds_gateway_deployment_from_spec() {
        let spec = base_spec();
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let args = args_of(&g);
        assert_eq!(arg_value(&args, "--mode").as_deref(), Some("gateway"));
        assert_eq!(
            arg_value(&args, "--listen-http").as_deref(),
            Some("0.0.0.0:4318")
        );
        assert_eq!(
            arg_value(&args, "--listen-grpc").as_deref(),
            Some("0.0.0.0:4317")
        );
        assert_eq!(arg_value(&args, "--store").as_deref(), Some("s3"));
        assert_eq!(
            arg_value(&args, "--s3-bucket").as_deref(),
            Some("ravel-data")
        );
        assert_eq!(
            arg_value(&args, "--s3-region").as_deref(),
            Some("eu-west-1")
        );
        assert_eq!(
            arg_value(&args, "--s3-endpoint").as_deref(),
            Some("http://minio:9000")
        );
    }

    #[test]
    fn builds_query_deployment_from_spec() {
        let spec = base_spec();
        let q = desired_query_deployment(&spec, "prod", &ctx());
        let args = args_of(&q);
        assert_eq!(arg_value(&args, "--mode").as_deref(), Some("query"));
        assert!(
            arg_value(&args, "--listen-grpc").is_none(),
            "query must not expose gRPC"
        );
        let ports = container_of(&q).ports.clone().expect("ports");
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].container_port, HTTP_PORT);
    }

    #[test]
    fn tenant_tokens_render_as_var_expansion_never_literal_values() {
        let spec = base_spec();
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let args = args_of(&g);
        // Two tenants -> two --tenant-token pairs using $(VAR) expansion.
        let joined = args.join(" ");
        assert!(joined.contains("--tenant-token $(RAVEL_TENANT_TOKEN_0)=acme"));
        assert!(joined.contains("--tenant-token $(RAVEL_TENANT_TOKEN_1)=globex"));

        // The env vars source those tokens from the token Secret's per-tenant
        // keys via secretKeyRef; no literal token appears anywhere.
        let env = container_of(&g).env.clone().expect("env");
        let tok0 = env
            .iter()
            .find(|e| e.name == "RAVEL_TENANT_TOKEN_0")
            .expect("token env 0");
        let sel = tok0
            .value_from
            .as_ref()
            .expect("value_from")
            .secret_key_ref
            .as_ref()
            .expect("secret_key_ref");
        assert_eq!(sel.name, "ravel-tokens");
        assert_eq!(sel.key, "acme");
        assert!(
            env.iter().all(|e| e.value.is_none()),
            "no env var may carry a literal value"
        );
    }

    #[test]
    fn s3_credentials_come_from_secret_not_flags() {
        let spec = base_spec();
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let args = args_of(&g);
        assert!(
            !args
                .iter()
                .any(|a| a == "--s3-access-key" || a == "--s3-secret-key"),
            "credentials must never be CLI flags"
        );
        let env = container_of(&g).env.clone().expect("env");
        for (var, key) in [
            ("RAVEL_S3_ACCESS_KEY", "accessKeyId"),
            ("RAVEL_S3_SECRET_KEY", "secretAccessKey"),
        ] {
            let e = env.iter().find(|e| e.name == var).expect("cred env");
            let sel = e
                .value_from
                .as_ref()
                .expect("value_from")
                .secret_key_ref
                .as_ref()
                .expect("secret_key_ref");
            assert_eq!(sel.name, "ravel-s3");
            assert_eq!(sel.key, key);
        }
    }

    #[test]
    fn maintain_deployment_defaults_to_one_replica_rolling_update() {
        // ADR-0065 supersedes ADR-0034's single-replica Recreate guidance: the
        // maintain tier now renders from spec.maintain.replicas (default 1) with
        // a RollingUpdate strategy. The default (base_spec sets replicas=1) must
        // still yield exactly one pod, so an existing cluster is unaffected.
        let spec = base_spec();
        let m = desired_maintain_deployment(&spec, "prod", &ctx())
            .expect("no gc render error")
            .expect("enabled");
        let dspec = m.spec.as_ref().expect("spec");
        assert_eq!(dspec.replicas, Some(1));
        assert_eq!(
            dspec.strategy.as_ref().expect("strategy").type_.as_deref(),
            Some("RollingUpdate"),
            "maintain must use a >1-safe strategy (ADR-0065)"
        );
        // RollingUpdate must not carry a rollingUpdate block the render path
        // never sets; a Recreate leftover here would be a silent mismatch.
        assert!(
            dspec
                .strategy
                .as_ref()
                .expect("strategy")
                .rolling_update
                .is_none()
        );
        let args = args_of(&m);
        assert_eq!(arg_value(&args, "--mode").as_deref(), Some("maintain"));
        assert_eq!(
            arg_value(&args, "--maintain-interval-secs").as_deref(),
            Some("600")
        );
        assert_eq!(
            arg_value(&args, "--retention-default").as_deref(),
            Some("30d")
        );
        assert_eq!(
            arg_value(&args, "--retention-tenant").as_deref(),
            Some("acme=7d")
        );
    }

    #[test]
    fn maintain_gets_the_tenant_list_from_the_same_tokens_as_gateway_and_query() {
        // Regression guard: maintain derives its compaction/retention/GC tenant
        // set from the parsed --tenant-token map (ravel-server's fold_tenants).
        // Rendering zero tokens made the whole tier a silent no-op that still
        // reported Ready. Maintain must get the SAME --tenant-token args and
        // env as gateway and query, using the same RenderCtx tenant list.
        let spec = base_spec();
        let ctx = ctx();
        let m = desired_maintain_deployment(&spec, "prod", &ctx)
            .expect("no gc render error")
            .expect("enabled");
        let g = desired_gateway_deployment(&spec, "prod", &ctx);

        let margs = args_of(&m);
        let gargs = args_of(&g);
        // Same --tenant-token $(VAR)=<tenant> pairs as gateway, in the same order.
        let tenant_pairs = |args: &[String]| -> Vec<String> {
            args.iter()
                .enumerate()
                .filter(|(_, a)| *a == "--tenant-token")
                .map(|(i, _)| args[i + 1].clone())
                .collect()
        };
        assert_eq!(
            tenant_pairs(&margs),
            vec![
                "$(RAVEL_TENANT_TOKEN_0)=acme".to_string(),
                "$(RAVEL_TENANT_TOKEN_1)=globex".to_string(),
            ],
            "maintain must render the tenant list or its tasks never spawn"
        );
        assert_eq!(tenant_pairs(&margs), tenant_pairs(&gargs));

        // And the token env vars, sourced from the token Secret's per-tenant
        // keys via secretKeyRef, exactly as gateway gets them.
        let env = container_of(&m).env.clone().expect("env");
        let tok0 = env
            .iter()
            .find(|e| e.name == "RAVEL_TENANT_TOKEN_0")
            .expect("token env 0");
        let sel = tok0
            .value_from
            .as_ref()
            .expect("value_from")
            .secret_key_ref
            .as_ref()
            .expect("secret_key_ref");
        assert_eq!(sel.name, "ravel-tokens");
        assert_eq!(sel.key, "acme");
        assert!(
            env.iter().all(|e| e.value.is_none()),
            "no env var may carry a literal token value"
        );
    }

    #[test]
    fn maintain_disabled_yields_none() {
        let mut spec = base_spec();
        spec.maintain.enabled = false;
        assert!(
            desired_maintain_deployment(&spec, "prod", &ctx())
                .expect("no gc render error")
                .is_none()
        );
    }

    #[test]
    fn maintain_replicas_two_renders_two_pods_with_rolling_update_and_ownership_wiring() {
        // ADR-0065 makes >1 maintain replica safe in-process (rendezvous over
        // a heartbeat-derived live worker set). This asserts that setting
        // spec.maintain.replicas = 2 renders a two-pod Deployment with a
        // >1-safe strategy, and that each pod carries the ownership-protocol
        // wiring the in-process ownership tests rely on.
        //
        // The ownership protocol coordinates entirely through the shared object
        // store (self-owned keys under `sys/maintain/`, discovered at runtime),
        // so there is no per-pod heartbeat/rendezvous flag to render: the
        // wiring that reaches it is `--mode maintain` (which spawns the
        // heartbeat + rendezvous machinery) plus the shared store args
        // (identical `--shards`/`--s3-bucket`, so both replicas coordinate over
        // one store) and the tenant list (without which maintain spawns no
        // tasks at all). True multi-pod convergence onto disjoint ownership is
        // covered in-process by ravel-server's maintain tests
        // (two_replicas_partition_units_without_double_pay); this operator test
        // proves only that a real deployment can now stand up two such pods.
        let mut spec = base_spec();
        spec.maintain.replicas = 2;
        let ctx = ctx();
        let m = desired_maintain_deployment(&spec, "prod", &ctx)
            .expect("no gc render error")
            .expect("enabled");
        let dspec = m.spec.as_ref().expect("spec");

        assert_eq!(dspec.replicas, Some(2), "maintain must render two pods");
        assert_eq!(
            dspec.strategy.as_ref().expect("strategy").type_.as_deref(),
            Some("RollingUpdate"),
            "two replicas require a >1-safe strategy, not Recreate"
        );

        // Ownership-protocol wiring: --mode maintain spawns the heartbeat +
        // rendezvous machinery in-process.
        let margs = args_of(&m);
        assert_eq!(arg_value(&margs, "--mode").as_deref(), Some("maintain"));

        // Both replicas coordinate over one shared store: the store flags that
        // scope the `sys/maintain/` keyspace must match what gateway/query
        // render from the same spec, so every replica sees the same world.
        let g = desired_gateway_deployment(&spec, "prod", &ctx);
        let gargs = args_of(&g);
        assert_eq!(
            arg_value(&margs, "--s3-bucket"),
            arg_value(&gargs, "--s3-bucket"),
            "all replicas must share one store bucket to coordinate ownership"
        );
        assert_eq!(
            arg_value(&margs, "--shards"),
            arg_value(&gargs, "--shards"),
            "the shard count defines the unit space the workers partition"
        );

        // The tenant list must render, or maintain::spawn gets an empty tenant
        // set and every replica is a silent no-op regardless of replica count.
        let tenant_pairs: Vec<String> = margs
            .iter()
            .enumerate()
            .filter(|(_, a)| *a == "--tenant-token")
            .map(|(i, _)| margs[i + 1].clone())
            .collect();
        assert_eq!(
            tenant_pairs,
            vec![
                "$(RAVEL_TENANT_TOKEN_0)=acme".to_string(),
                "$(RAVEL_TENANT_TOKEN_1)=globex".to_string(),
            ],
            "each maintain replica needs the tenant list to own any units"
        );
    }

    #[test]
    fn gateway_rolling_update_and_replicas() {
        let spec = base_spec();
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let dspec = g.spec.as_ref().expect("spec");
        assert_eq!(dspec.replicas, Some(3));
        assert_eq!(
            dspec.strategy.as_ref().expect("strategy").type_.as_deref(),
            Some("RollingUpdate")
        );
    }

    #[test]
    fn probes_point_at_health_routes_on_http_port() {
        let spec = base_spec();
        for dep in [
            desired_gateway_deployment(&spec, "prod", &ctx()),
            desired_query_deployment(&spec, "prod", &ctx()),
            desired_maintain_deployment(&spec, "prod", &ctx())
                .expect("no gc render error")
                .expect("enabled"),
        ] {
            let c = container_of(&dep);
            let live = c
                .liveness_probe
                .as_ref()
                .expect("liveness")
                .http_get
                .as_ref()
                .expect("httpGet");
            assert_eq!(live.path.as_deref(), Some("/healthz"));
            assert_eq!(live.port, IntOrString::Int(HTTP_PORT));
            let ready = c
                .readiness_probe
                .as_ref()
                .expect("readiness")
                .http_get
                .as_ref()
                .expect("httpGet");
            assert_eq!(ready.path.as_deref(), Some("/readyz"));
            assert_eq!(ready.port, IntOrString::Int(HTTP_PORT));
        }
    }

    #[test]
    fn secrets_checksum_is_stamped_on_every_pod_template() {
        // The checksum annotation on the pod template is what makes the
        // Deployment controller roll pods when a Secret value changes
        // (ADR-0034 decision 2). It must appear on all three tiers. With no
        // per-tier overrides, all three resolve to the shared credential Secret,
        // so all three carry the same checksum (they roll together).
        let spec = base_spec();
        let ctx = ctx();
        let expected = secrets_checksum(Some("tok-1"), Some("cred-1"), None, None);
        for dep in [
            desired_gateway_deployment(&spec, "prod", &ctx),
            desired_query_deployment(&spec, "prod", &ctx),
            desired_maintain_deployment(&spec, "prod", &ctx)
                .expect("no gc render error")
                .expect("enabled"),
        ] {
            assert_eq!(
                checksum_of(&dep).as_deref(),
                Some(expected.as_str()),
                "every tier's pod template must carry the secrets checksum"
            );
        }
    }

    #[test]
    fn services_expose_expected_ports_and_select_their_component() {
        let spec = base_spec();
        let gsvc = desired_gateway_service(&spec, "prod");
        let gspec = gsvc.spec.as_ref().expect("spec");
        let gports: Vec<i32> = gspec
            .ports
            .as_ref()
            .expect("ports")
            .iter()
            .map(|p| p.port)
            .collect();
        assert_eq!(gports, vec![HTTP_PORT, GRPC_PORT]);
        assert_eq!(
            gspec
                .selector
                .as_ref()
                .expect("selector")
                .get("app.kubernetes.io/component")
                .map(String::as_str),
            Some("gateway")
        );

        let qsvc = desired_query_service(&spec, "prod");
        let qspec = qsvc.spec.as_ref().expect("spec");
        let qports: Vec<i32> = qspec
            .ports
            .as_ref()
            .expect("ports")
            .iter()
            .map(|p| p.port)
            .collect();
        assert_eq!(qports, vec![HTTP_PORT]);
    }

    #[test]
    fn no_tenant_secret_means_no_token_args_or_env() {
        let mut spec = base_spec();
        spec.tenant_tokens_secret_ref = None;
        // Even with tenant names in the ctx, absence of the Secret ref means no
        // token wiring can be rendered.
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let args = args_of(&g);
        assert!(!args.iter().any(|a| a == "--tenant-token"));
        let env = container_of(&g).env.clone().expect("env");
        assert!(
            !env.iter()
                .any(|e| e.name.starts_with("RAVEL_TENANT_TOKEN_"))
        );
    }

    #[test]
    fn region_and_endpoint_optionality() {
        let mut spec = base_spec();
        spec.storage.s3.endpoint = None;
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let args = args_of(&g);
        assert!(arg_value(&args, "--s3-endpoint").is_none());
        // Region still renders (it always has a value, defaulted upstream).
        assert_eq!(
            arg_value(&args, "--s3-region").as_deref(),
            Some("eu-west-1")
        );
    }

    #[test]
    fn fold_flags_render_only_when_configured() {
        let mut spec = base_spec();
        spec.gateway.fold = Some(FoldSpec {
            disabled: true,
            interval_secs: None,
        });
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let args = args_of(&g);
        assert!(args.iter().any(|a| a == "--disable-fold"));
        assert!(arg_value(&args, "--fold-interval-secs").is_none());

        spec.gateway.fold = None;
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let args = args_of(&g);
        assert!(!args.iter().any(|a| a == "--disable-fold"));
    }

    #[test]
    fn secrets_checksum_is_deterministic_and_changes_with_content() {
        // The pure hash behind every tier's pod-template annotation (ADR-0055
        // section 5): stable for the same inputs (no pod churn), and changing
        // when either the token or the credential resourceVersion moves.
        let a = secrets_checksum(Some("100"), Some("200"), Some("300"), Some("400"));
        assert_eq!(
            a,
            secrets_checksum(Some("100"), Some("200"), Some("300"), Some("400"))
        );
        assert_ne!(
            a,
            secrets_checksum(Some("101"), Some("200"), Some("300"), Some("400"))
        );
        assert_ne!(
            a,
            secrets_checksum(Some("100"), Some("201"), Some("300"), Some("400"))
        );
        assert_ne!(
            a,
            secrets_checksum(Some("100"), Some("200"), Some("301"), Some("400")),
            "the deployment-key resourceVersion must also feed the checksum, \
             so its rotation rolls pods that mount it"
        );
        assert_ne!(
            a,
            secrets_checksum(Some("100"), Some("200"), Some("300"), Some("401")),
            "the audit-token-key resourceVersion must also feed the checksum, \
             so its rotation rolls pods that mount it"
        );
        // Absent versions collapse to a distinct, stable value.
        let none = secrets_checksum(None, Some("200"), None, None);
        assert_eq!(none, secrets_checksum(None, Some("200"), None, None));
        assert_ne!(none, a);
    }

    #[test]
    fn per_tier_credential_overrides_render_three_distinct_secret_sources() {
        // ADR-0055 section 5: each tier can carry its own credentialsSecretRef.
        // With all three set to different Secrets, the three Deployments source
        // RAVEL_S3_ACCESS_KEY / RAVEL_S3_SECRET_KEY from three different Secret
        // names -- proven through the real desired_* render path, not a helper.
        let mut spec = base_spec();
        spec.gateway.credentials_secret_ref = Some(LocalSecretRef {
            name: "gw-creds".to_string(),
        });
        spec.query.credentials_secret_ref = Some(LocalSecretRef {
            name: "qy-creds".to_string(),
        });
        spec.maintain.credentials_secret_ref = Some(LocalSecretRef {
            name: "mt-creds".to_string(),
        });
        // Every referenced credential Secret has a resourceVersion, as the
        // controller would have read.
        let ctx = RenderCtx {
            tenant_names: vec!["acme".to_string(), "globex".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: BTreeMap::from([
                ("ravel-s3".to_string(), "cred-shared".to_string()),
                ("gw-creds".to_string(), "cred-gw".to_string()),
                ("qy-creds".to_string(), "cred-qy".to_string()),
                ("mt-creds".to_string(), "cred-mt".to_string()),
            ]),
            deployment_key_resource_version: None,
            audit_token_key_resource_version: None,
        };

        let g = desired_gateway_deployment(&spec, "prod", &ctx);
        let q = desired_query_deployment(&spec, "prod", &ctx);
        let m = desired_maintain_deployment(&spec, "prod", &ctx)
            .expect("no gc render error")
            .expect("enabled");

        for (dep, expect) in [(&g, "gw-creds"), (&q, "qy-creds"), (&m, "mt-creds")] {
            assert_eq!(
                env_secret_name(dep, "RAVEL_S3_ACCESS_KEY").as_deref(),
                Some(expect),
                "access key must source from the tier's own Secret"
            );
            assert_eq!(
                env_secret_name(dep, "RAVEL_S3_SECRET_KEY").as_deref(),
                Some(expect),
                "secret key must source from the tier's own Secret"
            );
        }
        // The shared credential is used by none of them here.
        for dep in [&g, &q, &m] {
            assert_ne!(
                env_secret_name(dep, "RAVEL_S3_ACCESS_KEY").as_deref(),
                Some("ravel-s3")
            );
        }
    }

    #[test]
    fn overrides_unset_render_is_backward_compatible_with_shared_credential() {
        // True backward compatibility: a spec with all three tier overrides
        // unset must render byte-identically to the same spec whose overrides
        // are set explicitly to the shared credential Secret -- i.e. the None
        // path is a pure passthrough to storage.s3.credentialsSecretRef, so an
        // existing cluster that never sets an override is unaffected. Field
        // identity is asserted over the whole serialized Deployment (args, env,
        // and the checksum annotation), for all three tiers.
        let none_spec = base_spec(); // all tier overrides are None
        let shared = LocalSecretRef {
            name: "ravel-s3".to_string(), // == storage.s3.credentialsSecretRef
        };
        let mut explicit_spec = base_spec();
        explicit_spec.gateway.credentials_secret_ref = Some(shared.clone());
        explicit_spec.query.credentials_secret_ref = Some(shared.clone());
        explicit_spec.maintain.credentials_secret_ref = Some(shared.clone());

        let ctx = ctx();
        let pairs = [
            (
                desired_gateway_deployment(&none_spec, "prod", &ctx),
                desired_gateway_deployment(&explicit_spec, "prod", &ctx),
            ),
            (
                desired_query_deployment(&none_spec, "prod", &ctx),
                desired_query_deployment(&explicit_spec, "prod", &ctx),
            ),
            (
                desired_maintain_deployment(&none_spec, "prod", &ctx)
                    .expect("no gc render error")
                    .expect("enabled"),
                desired_maintain_deployment(&explicit_spec, "prod", &ctx)
                    .expect("no gc render error")
                    .expect("enabled"),
            ),
        ];
        for (unset, explicit) in pairs {
            assert_eq!(
                serde_json::to_value(&unset).expect("serialize unset"),
                serde_json::to_value(&explicit).expect("serialize explicit"),
                "override-unset render must equal the shared-credential render"
            );
            // And every tier sources from the shared Secret, as it always has.
            assert_eq!(
                env_secret_name(&unset, "RAVEL_S3_ACCESS_KEY").as_deref(),
                Some("ravel-s3")
            );
        }

        // All three carry the SAME checksum: with no overrides they consume one
        // shared Secret, so they roll together exactly as before this change.
        let g = desired_gateway_deployment(&none_spec, "prod", &ctx);
        let q = desired_query_deployment(&none_spec, "prod", &ctx);
        let m = desired_maintain_deployment(&none_spec, "prod", &ctx)
            .expect("no gc render error")
            .expect("enabled");
        let shared_checksum = secrets_checksum(Some("tok-1"), Some("cred-1"), None, None);
        assert_eq!(checksum_of(&g).as_deref(), Some(shared_checksum.as_str()));
        assert_eq!(checksum_of(&q).as_deref(), Some(shared_checksum.as_str()));
        assert_eq!(checksum_of(&m).as_deref(), Some(shared_checksum.as_str()));
    }

    #[test]
    fn tier_secret_change_rolls_only_that_tier_but_shared_change_rolls_all() {
        // ADR-0055 section 5: with per-role overrides set, a change to one
        // tier's credential Secret (its resourceVersion bumps) must change only
        // that tier's pod-template checksum -- rolling only its Deployment --
        // and leave the other two untouched. With no overrides, a change to the
        // one shared credential must roll all three, matching today's behavior.
        let mut over_spec = base_spec();
        over_spec.gateway.credentials_secret_ref = Some(LocalSecretRef {
            name: "gw-creds".to_string(),
        });
        over_spec.query.credentials_secret_ref = Some(LocalSecretRef {
            name: "qy-creds".to_string(),
        });
        over_spec.maintain.credentials_secret_ref = Some(LocalSecretRef {
            name: "mt-creds".to_string(),
        });
        let base_versions = BTreeMap::from([
            ("ravel-s3".to_string(), "shared-1".to_string()),
            ("gw-creds".to_string(), "gw-1".to_string()),
            ("qy-creds".to_string(), "qy-1".to_string()),
            ("mt-creds".to_string(), "mt-1".to_string()),
        ]);
        let before = RenderCtx {
            tenant_names: vec!["acme".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: base_versions.clone(),
            deployment_key_resource_version: Some("dk-1".to_string()),
            audit_token_key_resource_version: None,
        };
        // Rotate ONLY the gateway credential Secret.
        let mut after_versions = base_versions.clone();
        after_versions.insert("gw-creds".to_string(), "gw-2".to_string());
        let after = RenderCtx {
            tenant_names: vec!["acme".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: after_versions,
            deployment_key_resource_version: Some("dk-1".to_string()),
            audit_token_key_resource_version: None,
        };

        let g_before = desired_gateway_deployment(&over_spec, "prod", &before);
        let g_after = desired_gateway_deployment(&over_spec, "prod", &after);
        let q_before = desired_query_deployment(&over_spec, "prod", &before);
        let q_after = desired_query_deployment(&over_spec, "prod", &after);
        let m_before = desired_maintain_deployment(&over_spec, "prod", &before)
            .expect("no gc render error")
            .expect("enabled");
        let m_after = desired_maintain_deployment(&over_spec, "prod", &after)
            .expect("no gc render error")
            .expect("enabled");

        assert_ne!(
            checksum_of(&g_before),
            checksum_of(&g_after),
            "gateway secret rotated: its Deployment must roll"
        );
        assert_eq!(
            checksum_of(&q_before),
            checksum_of(&q_after),
            "query secret unchanged: its Deployment must NOT roll"
        );
        assert_eq!(
            checksum_of(&m_before),
            checksum_of(&m_after),
            "maintain secret unchanged: its Deployment must NOT roll"
        );

        // No overrides: a shared-credential change rolls all three.
        let shared_spec = base_spec();
        let shared_before = RenderCtx {
            tenant_names: vec!["acme".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: BTreeMap::from([(
                "ravel-s3".to_string(),
                "shared-1".to_string(),
            )]),
            deployment_key_resource_version: Some("dk-1".to_string()),
            audit_token_key_resource_version: None,
        };
        let shared_after = RenderCtx {
            tenant_names: vec!["acme".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: BTreeMap::from([(
                "ravel-s3".to_string(),
                "shared-2".to_string(),
            )]),
            deployment_key_resource_version: Some("dk-1".to_string()),
            audit_token_key_resource_version: None,
        };
        let gb = desired_gateway_deployment(&shared_spec, "prod", &shared_before);
        let ga = desired_gateway_deployment(&shared_spec, "prod", &shared_after);
        let qb = desired_query_deployment(&shared_spec, "prod", &shared_before);
        let qa = desired_query_deployment(&shared_spec, "prod", &shared_after);
        let mb = desired_maintain_deployment(&shared_spec, "prod", &shared_before)
            .expect("no gc render error")
            .expect("enabled");
        let ma = desired_maintain_deployment(&shared_spec, "prod", &shared_after)
            .expect("no gc render error")
            .expect("enabled");
        assert_ne!(checksum_of(&gb), checksum_of(&ga), "gateway must roll");
        assert_ne!(checksum_of(&qb), checksum_of(&qa), "query must roll");
        assert_ne!(checksum_of(&mb), checksum_of(&ma), "maintain must roll");
    }

    /// #1487 rework: a cluster with neither `auditTokenKeySecretRef` nor
    /// `deploymentKeySecretRef` set renders no `RAVEL_AUDIT_TOKEN_KEY` at all
    /// -- the operator does not generate a Secret for this case (issue #126's
    /// `secrets get`-only posture). [`crate::controller`] reads
    /// [`audit_token_key_missing`] to decide the `Degraded` condition and
    /// withhold the query tier's Deployment apply; see
    /// `unkeyed_cluster_without_audit_key_ref_reports_missing_and_skips_the_query_tier`
    /// there. Flip [`audit_token_key_missing`]'s `&&` to `||` and this fails:
    /// a keyed-only or ref-only cluster would also report missing.
    #[test]
    fn unkeyed_cluster_without_audit_key_ref_renders_no_env_and_is_reported_missing() {
        let spec = base_spec(); // both refs unset
        let q = desired_query_deployment(&spec, "prod", &ctx());
        assert!(
            env_secret_name(&q, AUDIT_TOKEN_KEY_ENV).is_none(),
            "no Secret to source RAVEL_AUDIT_TOKEN_KEY from: the operator generates none"
        );
        assert!(
            audit_token_key_missing(&spec),
            "a cluster with neither ref set must be reported missing"
        );
    }

    /// #1487: a cluster with an explicit `auditTokenKeySecretRef` and no
    /// `deploymentKeySecretRef` renders `RAVEL_AUDIT_TOKEN_KEY` from that
    /// Secret's `key` field, required (`optional: Some(false)`). Flip
    /// [`audit_token_key_env`]'s `AUDIT_TOKEN_KEY_SECRET_KEY` to a different
    /// literal and the key assertion fails; flip `secret_key_env`'s
    /// `optional: Some(false)` to `None` and the optional assertion fails.
    #[test]
    fn unkeyed_cluster_with_audit_key_ref_renders_the_env_from_that_secret() {
        let mut spec = base_spec(); // no deploymentKeySecretRef
        spec.audit_token_key_secret_ref = Some(LocalSecretRef {
            name: "audit-key".to_string(),
        });
        let q = desired_query_deployment(&spec, "prod", &ctx());
        let env = container_of(&q)
            .env
            .as_ref()
            .expect("query renders env vars")
            .iter()
            .find(|e| e.name == AUDIT_TOKEN_KEY_ENV)
            .expect("RAVEL_AUDIT_TOKEN_KEY must be present");
        let secret_ref = env
            .value_from
            .as_ref()
            .expect("valueFrom")
            .secret_key_ref
            .as_ref()
            .expect("secretKeyRef");
        assert_eq!(secret_ref.name, "audit-key");
        assert_eq!(secret_ref.key, "key");
        assert_eq!(secret_ref.optional, Some(false));
        assert!(
            !audit_token_key_missing(&spec),
            "an explicit ref means the key is not missing"
        );
    }

    /// #1487: a cluster with `deploymentKeySecretRef` set and no
    /// `auditTokenKeySecretRef` renders no `RAVEL_AUDIT_TOKEN_KEY` at all --
    /// the server derives the key from the deployment key. Flip
    /// [`audit_token_key_env`] to fall through to some other source instead
    /// of returning `None`, and this fails: the env var appears, sourced
    /// from a Secret the operator never creates.
    #[test]
    fn keyed_cluster_without_audit_key_ref_renders_no_env() {
        let mut spec = base_spec();
        spec.deployment_key_secret_ref = Some(LocalSecretRef {
            name: "dk".to_string(),
        });
        let q = desired_query_deployment(&spec, "prod", &ctx());
        assert!(
            env_secret_name(&q, AUDIT_TOKEN_KEY_ENV).is_none(),
            "a keyed cluster must render no RAVEL_AUDIT_TOKEN_KEY env var"
        );
        assert!(
            !audit_token_key_missing(&spec),
            "deploymentKeySecretRef alone must not be reported missing"
        );
    }

    /// #1487: an explicit `auditTokenKeySecretRef` wins even when
    /// `deploymentKeySecretRef` is ALSO set. Flip [`audit_token_key_env`]'s
    /// field order so the `deployment_key_secret_ref.is_some()` check runs
    /// before the explicit-ref check and this fails: the env var disappears
    /// even though an explicit ref was given.
    #[test]
    fn explicit_audit_key_ref_wins_on_a_keyed_cluster() {
        let mut spec = base_spec();
        spec.deployment_key_secret_ref = Some(LocalSecretRef {
            name: "dk".to_string(),
        });
        spec.audit_token_key_secret_ref = Some(LocalSecretRef {
            name: "explicit-audit".to_string(),
        });
        let q = desired_query_deployment(&spec, "prod", &ctx());
        assert_eq!(
            env_secret_name(&q, AUDIT_TOKEN_KEY_ENV).as_deref(),
            Some("explicit-audit"),
            "an explicit auditTokenKeySecretRef must win over the deployment-key derivation"
        );
    }

    /// #1487: gateway and maintain never read a query-audit token, whether
    /// the cluster has an explicit ref or not. Flip
    /// [`desired_gateway_deployment`] (or [`desired_maintain_deployment`]) to
    /// also push `audit_token_key_env(spec)` into its env list, the same as
    /// query, and this fails: one of the two non-query tiers would carry the
    /// env var.
    #[test]
    fn gateway_and_maintain_carry_no_audit_token_key_env() {
        let mut spec = base_spec();
        spec.audit_token_key_secret_ref = Some(LocalSecretRef {
            name: "audit-key".to_string(),
        });
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let m = desired_maintain_deployment(&spec, "prod", &ctx())
            .expect("no gc render error")
            .expect("enabled");
        assert!(
            env_secret_name(&g, AUDIT_TOKEN_KEY_ENV).is_none(),
            "gateway must never carry RAVEL_AUDIT_TOKEN_KEY"
        );
        assert!(
            env_secret_name(&m, AUDIT_TOKEN_KEY_ENV).is_none(),
            "maintain must never carry RAVEL_AUDIT_TOKEN_KEY"
        );
        let q = desired_query_deployment(&spec, "prod", &ctx());
        assert!(
            env_secret_name(&q, AUDIT_TOKEN_KEY_ENV).is_some(),
            "sanity: query itself does carry it when a ref is set"
        );
    }

    /// #1487: the query tier's checksum must move when the audit-token-key
    /// Secret's resourceVersion changes, and gateway/maintain's checksums must
    /// NOT move -- proving the new field is isolated to the one tier that
    /// reads that Secret. Flip [`tier_secrets_checksum`]'s query call site to
    /// pass `None` instead of `ctx.audit_token_key_resource_version.as_deref()`
    /// and the first assertion fails: the query checksum stops moving.
    #[test]
    fn query_tier_checksum_changes_with_the_audit_key_secret() {
        let spec = base_spec();
        let before = RenderCtx {
            audit_token_key_resource_version: Some("a1".to_string()),
            ..ctx()
        };
        let after = RenderCtx {
            audit_token_key_resource_version: Some("a2".to_string()),
            ..ctx()
        };

        let q_before = desired_query_deployment(&spec, "prod", &before);
        let q_after = desired_query_deployment(&spec, "prod", &after);
        assert_ne!(
            checksum_of(&q_before),
            checksum_of(&q_after),
            "query checksum must move with the audit-token-key Secret's resourceVersion"
        );

        let g_before = desired_gateway_deployment(&spec, "prod", &before);
        let g_after = desired_gateway_deployment(&spec, "prod", &after);
        assert_eq!(
            checksum_of(&g_before),
            checksum_of(&g_after),
            "gateway checksum must NOT move with the audit-token-key Secret"
        );
        let m_before = desired_maintain_deployment(&spec, "prod", &before)
            .expect("no gc render error")
            .expect("enabled");
        let m_after = desired_maintain_deployment(&spec, "prod", &after)
            .expect("no gc render error")
            .expect("enabled");
        assert_eq!(
            checksum_of(&m_before),
            checksum_of(&m_after),
            "maintain checksum must NOT move with the audit-token-key Secret"
        );
    }

    /// #1487: with no audit-token-key resourceVersion set, every tier's
    /// checksum (not just gateway/maintain's) must equal exactly the
    /// pre-#1487 three-field `blake3_hex` composition -- the field must be
    /// folded in only when `Some`, never as an empty fourth field, or an
    /// operator upgrade alone would roll every unkeyed cluster's pods with no
    /// Secret having changed. Flip [`secrets_checksum`] back to always
    /// appending `audit_token_key_rv.unwrap_or("")` as a fourth field and
    /// this fails: the computed checksum stops matching the pre-#1487
    /// three-field literal.
    #[test]
    fn gateway_and_maintain_checksums_are_unchanged_when_no_audit_key_is_set() {
        let spec = base_spec();
        let render_ctx = ctx(); // audit_token_key_resource_version: None

        // The pre-#1487 algorithm, recomputed directly: exactly three fields,
        // never a fourth empty one.
        let expected = blake3_hex(&[
            render_ctx.token_resource_version.as_deref().unwrap_or(""),
            "cred-1",
            render_ctx
                .deployment_key_resource_version
                .as_deref()
                .unwrap_or(""),
        ]);

        let gateway = desired_gateway_deployment(&spec, "prod", &render_ctx);
        let maintain = desired_maintain_deployment(&spec, "prod", &render_ctx)
            .expect("no gc render error")
            .expect("enabled");
        let query = desired_query_deployment(&spec, "prod", &render_ctx);
        assert_eq!(
            checksum_of(&gateway),
            Some(expected.clone()),
            "gateway checksum must equal main's pre-#1487 algorithm when no audit key is set"
        );
        assert_eq!(
            checksum_of(&maintain),
            Some(expected.clone()),
            "maintain checksum must equal main's pre-#1487 algorithm when no audit key is set"
        );
        assert_eq!(
            checksum_of(&query),
            Some(expected),
            "query checksum must also equal main's pre-#1487 algorithm when no audit key is set"
        );
    }

    /// `base_spec` with a per-mode credential Secret on every tier: the
    /// per-role storage credential model, where the gateway and query policies
    /// hold `GetObject` on `sys/gc` and only maintain can create it.
    fn per_role_spec() -> RavelClusterSpec {
        let mut spec = base_spec();
        spec.gateway.credentials_secret_ref = Some(LocalSecretRef {
            name: "ravel-s3-gateway".to_string(),
        });
        spec.query.credentials_secret_ref = Some(LocalSecretRef {
            name: "ravel-s3-query".to_string(),
        });
        spec.maintain.credentials_secret_ref = Some(LocalSecretRef {
            name: "ravel-s3-maintain".to_string(),
        });
        spec
    }

    /// `base_spec` with a per-mode credential Secret on exactly one tier.
    fn single_override_spec(tier: DeploymentTier) -> RavelClusterSpec {
        let mut spec = base_spec();
        let secret = Some(LocalSecretRef {
            name: "ravel-s3-role".to_string(),
        });
        match tier {
            DeploymentTier::Gateway => spec.gateway.credentials_secret_ref = secret,
            DeploymentTier::Query => spec.query.credentials_secret_ref = secret,
            DeploymentTier::Maintain => spec.maintain.credentials_secret_ref = secret,
        }
        spec
    }

    #[test]
    fn per_role_credentials_apply_maintain_first_and_hold_the_request_serving_tiers() {
        let spec = per_role_spec();
        assert!(per_role_credentials(&spec));
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        let plan = objects.gc_bootstrap;
        assert_eq!(plan.gate, GcBootstrapGate::MaintainFirst);
        assert!(
            plan.reads_live_state(),
            "the pass must read live cluster state to order itself"
        );

        let both = vec![
            DeploymentTier::Maintain,
            DeploymentTier::Gateway,
            DeploymentTier::Query,
        ];
        // A fresh cluster: neither request-serving Deployment exists and
        // maintain is not ready, so this pass applies maintain and nothing
        // else. The exact sequence, not a containment check: applying gateway
        // or query here is the bug.
        assert_eq!(
            plan.apply_sequence(None, false),
            vec![DeploymentTier::Maintain]
        );
        assert_eq!(
            plan.apply_sequence(Some(0), false),
            vec![DeploymentTier::Maintain]
        );
        assert!(plan.waiting_for_bootstrap(None, false));
        assert!(plan.waiting_for_bootstrap(Some(0), false));
        assert_eq!(WAITING_FOR_GC_BOOTSTRAP_REASON, "WaitingForGcBootstrap");

        // An existing request-serving Deployment proves a prior pass got past
        // the gate, so the hold lifts even while maintain reports nothing: a
        // serving cluster keeps reconciling both tiers through a maintain
        // rollout or outage.
        assert_eq!(plan.apply_sequence(None, true), both);
        assert_eq!(plan.apply_sequence(Some(0), true), both);
        assert!(!plan.waiting_for_bootstrap(None, true));
        assert!(!plan.waiting_for_bootstrap(Some(0), true));

        // Once maintain reports a ready replica it has created `sys/gc`, so the
        // pass applies maintain first and then both request-serving tiers even
        // on a fresh cluster.
        assert_eq!(plan.apply_sequence(Some(1), false), both);
        assert!(!plan.waiting_for_bootstrap(Some(1), false));

        // All three tiers still render; only the order they are applied in
        // changed.
        assert_eq!(
            objects.gateway_deployment.metadata.name.as_deref(),
            Some("prod-gateway")
        );
        assert_eq!(
            objects.query_deployment.metadata.name.as_deref(),
            Some("prod-query")
        );
        assert!(objects.maintain_deployment.is_some());

        // One override is enough. Whichever tier narrowed its credential, the
        // cluster is on the per-role policies and the same ordering holds.
        for tier in [
            DeploymentTier::Gateway,
            DeploymentTier::Query,
            DeploymentTier::Maintain,
        ] {
            let spec = single_override_spec(tier);
            assert!(
                per_role_credentials(&spec),
                "a {} override alone is per-role credentials",
                tier.component()
            );
            let plan = gc_bootstrap_plan(&spec);
            assert_eq!(plan.gate, GcBootstrapGate::MaintainFirst);
            assert_eq!(
                plan.apply_sequence(None, false),
                vec![DeploymentTier::Maintain],
                "a {} override alone must still hold the request-serving tiers",
                tier.component()
            );
        }
    }

    #[test]
    fn per_role_credentials_with_maintain_disabled_still_render_and_apply_the_request_serving_tiers()
     {
        let mut spec = per_role_spec();
        spec.maintain.enabled = false;
        let objects =
            desired_objects(&spec, "prod", "default", &ctx()).expect("render never aborts");
        let plan = objects.gc_bootstrap;
        assert_eq!(plan.gate, GcBootstrapGate::Unavailable);

        // Both request-serving tiers render: the operator applies them so that
        // creating `sys/gc` out of band (the remedy its message names) unblocks
        // the cluster. There is no maintain Deployment to apply here.
        assert_eq!(
            objects.gateway_deployment.metadata.name.as_deref(),
            Some("prod-gateway"),
            "gateway still renders when maintain is disabled under per-role credentials"
        );
        assert_eq!(
            objects.query_deployment.metadata.name.as_deref(),
            Some("prod-query"),
            "query still renders when maintain is disabled under per-role credentials"
        );
        assert!(objects.maintain_deployment.is_none());

        // The pass always applies gateway and query, regardless of the live
        // state inputs, and never waits: this is a Degraded state to surface,
        // not a wait to sit through.
        for ready in [None, Some(0), Some(1)] {
            for exists in [false, true] {
                assert_eq!(
                    plan.apply_sequence(ready, exists),
                    vec![DeploymentTier::Gateway, DeploymentTier::Query],
                    "gateway and query always apply for readiness {ready:?} exists {exists}"
                );
                assert!(
                    !plan.waiting_for_bootstrap(ready, exists),
                    "unavailable is a misconfiguration to report, not a wait"
                );
            }
        }
        assert!(
            !plan.reads_live_state(),
            "only MaintainFirst reads live cluster state"
        );

        // The named reason and what its message must tell the operator to do.
        assert_eq!(GC_BOOTSTRAP_UNAVAILABLE_REASON, "GcBootstrapUnavailable");
        assert!(GC_BOOTSTRAP_UNAVAILABLE_MESSAGE.contains("spec.maintain"));
        assert!(GC_BOOTSTRAP_UNAVAILABLE_MESSAGE.contains("ravel-cli gc-config set"));
    }

    #[test]
    fn shared_credential_keeps_the_original_apply_order_with_no_wait() {
        let spec = base_spec();
        assert!(
            !per_role_credentials(&spec),
            "base_spec sets no per-Deployment override"
        );
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        let plan = objects.gc_bootstrap;
        assert_eq!(plan.gate, GcBootstrapGate::SharedCredential);
        assert!(
            !plan.reads_live_state(),
            "a single-credential cluster must cost no extra API read"
        );
        // The order the operator has always used, independent of both inputs:
        // any tier holding the shared credential can create sys/gc.
        for ready in [None, Some(0), Some(1)] {
            for exists in [false, true] {
                assert_eq!(
                    plan.apply_sequence(ready, exists),
                    vec![
                        DeploymentTier::Gateway,
                        DeploymentTier::Query,
                        DeploymentTier::Maintain
                    ],
                    "unchanged order for readiness {ready:?} exists {exists}"
                );
                assert!(!plan.waiting_for_bootstrap(ready, exists));
            }
        }
        assert_eq!(
            objects.gateway_deployment.metadata.name.as_deref(),
            Some("prod-gateway")
        );
        assert_eq!(
            objects.query_deployment.metadata.name.as_deref(),
            Some("prod-query")
        );

        // Maintain off under one shared credential is unchanged too: the
        // gateway or query pod creates sys/gc with the credential it holds.
        let mut no_maintain = base_spec();
        no_maintain.maintain.enabled = false;
        let objects = desired_objects(&no_maintain, "prod", "default", &ctx()).expect("render");
        assert_eq!(objects.gc_bootstrap.gate, GcBootstrapGate::SharedCredential);
        assert_eq!(
            objects.gc_bootstrap.apply_sequence(None, false),
            vec![DeploymentTier::Gateway, DeploymentTier::Query]
        );
        assert_eq!(
            objects.gateway_deployment.metadata.name.as_deref(),
            Some("prod-gateway")
        );
        assert_eq!(
            objects.query_deployment.metadata.name.as_deref(),
            Some("prod-query")
        );
        assert!(objects.maintain_deployment.is_none());
    }

    /// An affinity block with everything at its default: what an operator gets
    /// from writing `ingestAffinity: {}`.
    fn default_affinity() -> IngestAffinitySpec {
        IngestAffinitySpec::default()
    }

    /// The annotations on a rendered Ingress.
    fn annotations_of(ingress: &Ingress) -> BTreeMap<String, String> {
        ingress
            .metadata
            .annotations
            .clone()
            .expect("ingest Ingress must carry annotations")
    }

    /// The single backend path of a rendered Ingress' first rule.
    fn first_path_of(ingress: &Ingress) -> HTTPIngressPath {
        ingress
            .spec
            .as_ref()
            .expect("ingress spec")
            .rules
            .as_ref()
            .expect("rules")[0]
            .http
            .as_ref()
            .expect("http rule")
            .paths[0]
            .clone()
    }

    /// Every `(host, path)` pair a rendered Ingress claims, across all of its
    /// rules and paths. `host` is `None` for a host-less rule.
    fn host_paths_of(ingress: &Ingress) -> Vec<(Option<String>, String)> {
        ingress
            .spec
            .as_ref()
            .expect("ingress spec")
            .rules
            .as_ref()
            .expect("rules")
            .iter()
            .flat_map(|rule| {
                let host = rule.host.clone();
                rule.http
                    .as_ref()
                    .expect("http rule")
                    .paths
                    .iter()
                    .map(move |p| (host.clone(), p.path.clone().expect("path")))
            })
            .collect()
    }

    /// The distinct paths a rendered Ingress serves (order preserved, first
    /// rule), for asserting the OTLP/HTTP vs OTLP/gRPC path split.
    fn paths_of(ingress: &Ingress) -> Vec<String> {
        ingress
            .spec
            .as_ref()
            .expect("ingress spec")
            .rules
            .as_ref()
            .expect("rules")[0]
            .http
            .as_ref()
            .expect("http rule")
            .paths
            .iter()
            .map(|p| p.path.clone().expect("path"))
            .collect()
    }

    #[test]
    fn affinity_enabled_renders_ingest_ingress_keyed_on_tenant_identity_with_subset_two() {
        // ADR-0076 decision 1, the acceptance test. Asserted through
        // `desired_objects`, which is the exact call `controller::
        // reconcile_inner` makes and applies the contents of -- not a builder
        // in isolation that no reconcile path constructs.
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            ingress_class_name: Some("nginx".to_string()),
            hosts: vec!["ingest.example.com".to_string()],
            tls_secret_name: Some("ingest-tls".to_string()),
            ..default_affinity()
        });
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");

        let names: Vec<String> = objects
            .gateway_ingresses
            .iter()
            .map(|i| i.metadata.name.clone().expect("ingress name"))
            .collect();
        assert_eq!(
            names,
            vec![
                "prod-gateway-ingest".to_string(),
                "prod-gateway-ingest-grpc".to_string()
            ],
            "affinity must render an ingest Ingress for OTLP/HTTP and one for OTLP/gRPC"
        );

        let http_ingress = &objects.gateway_ingresses[0];
        let annotations = annotations_of(http_ingress);

        // The session-affinity key is tenant identity, read from the
        // authentication material. NOT the URL path (OTLP connections are
        // long-lived and Ravel resolves tenancy server-side from the
        // credential, so a path rule cannot see the tenant).
        assert_eq!(
            annotations
                .get("nginx.ingress.kubernetes.io/upstream-hash-by")
                .map(String::as_str),
            Some("$http_authorization"),
            "the hash key must be the tenant's credential, not the request URI"
        );
        assert!(
            !annotations
                .values()
                .any(|v| v.contains("$request_uri") || v.contains("$uri")),
            "no annotation may key routing on the URL path: {annotations:?}"
        );

        // Subset mode on, and the default subset size is TWO replicas -- not
        // one, so a single replica loss does not concentrate a tenant on one
        // process.
        assert_eq!(
            annotations
                .get("nginx.ingress.kubernetes.io/upstream-hash-by-subset")
                .map(String::as_str),
            Some("true"),
            "without subset mode the key pins a tenant to exactly one replica"
        );
        assert_eq!(
            annotations
                .get("nginx.ingress.kubernetes.io/upstream-hash-by-subset-size")
                .map(String::as_str),
            Some("2"),
            "ADR-0076 decision 1: the default subset is two replicas"
        );
        assert_eq!(DEFAULT_AFFINITY_SUBSET_SIZE, 2);

        // Rendered explicitly false: with service-upstream true the upstream is
        // the single ClusterIP, the hash has nothing to distribute over, and
        // the affinity silently does nothing.
        assert_eq!(
            annotations
                .get("nginx.ingress.kubernetes.io/service-upstream")
                .map(String::as_str),
            Some("false")
        );

        // The backend is the gateway Service on its named http port, and that
        // Service really exposes a port of that name in the same render.
        let path = first_path_of(http_ingress);
        assert_eq!(path.path_type, "Prefix");
        // The HTTP Ingress serves exactly the OTLP/HTTP routes, never `/`.
        // Sharing `/` with the gRPC Ingress is a duplicate-path conflict:
        // ingress-nginx keeps one location and drops the other's, silently
        // disabling the gRPC object's grpc_pass and affinity annotations.
        assert_eq!(
            paths_of(http_ingress),
            vec!["/v1/metrics", "/v1/logs", "/v1/traces"],
            "the HTTP Ingress serves the OTLP/HTTP routes, not `/`"
        );
        let backend = path.backend.service.expect("service backend");
        assert_eq!(backend.name, "prod-gateway");
        assert_eq!(
            backend.port.expect("backend port").name.as_deref(),
            Some("http")
        );
        let service_port_names: Vec<String> = objects
            .gateway_service
            .spec
            .as_ref()
            .expect("service spec")
            .ports
            .as_ref()
            .expect("ports")
            .iter()
            .filter_map(|p| p.name.clone())
            .collect();
        assert!(
            service_port_names.contains(&"http".to_string())
                && service_port_names.contains(&"grpc".to_string()),
            "the Ingress backend names ports the gateway Service must expose: {service_port_names:?}"
        );

        // Ingress plumbing: class, host rule, TLS.
        let ispec = http_ingress.spec.as_ref().expect("ingress spec");
        assert_eq!(ispec.ingress_class_name.as_deref(), Some("nginx"));
        assert_eq!(
            ispec.rules.as_ref().expect("rules")[0].host.as_deref(),
            Some("ingest.example.com")
        );
        let tls = ispec.tls.as_ref().expect("tls");
        assert_eq!(tls[0].secret_name.as_deref(), Some("ingest-tls"));

        // The gRPC Ingress is the same rule under backend-protocol GRPC on the
        // gRPC port; it exists because that annotation is per-Ingress.
        let grpc_ingress = &objects.gateway_ingresses[1];
        let grpc_annotations = annotations_of(grpc_ingress);
        assert_eq!(
            grpc_annotations
                .get("nginx.ingress.kubernetes.io/backend-protocol")
                .map(String::as_str),
            Some("GRPC")
        );
        assert_eq!(
            grpc_annotations
                .get("nginx.ingress.kubernetes.io/upstream-hash-by")
                .map(String::as_str),
            Some("$http_authorization"),
            "OTLP/gRPC carries the bulk of ingest; it must be pinned too"
        );
        assert_eq!(
            grpc_annotations
                .get("nginx.ingress.kubernetes.io/upstream-hash-by-subset-size")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            first_path_of(grpc_ingress)
                .backend
                .service
                .expect("service backend")
                .port
                .expect("port")
                .name
                .as_deref(),
            Some("grpc")
        );
        assert!(
            !annotations.contains_key("nginx.ingress.kubernetes.io/backend-protocol"),
            "the HTTP Ingress must not claim a gRPC backend"
        );

        // Both objects carry the managed-by label the controller's owned-object
        // watch selects on, or the operator would never see them change.
        for ingress in &objects.gateway_ingresses {
            assert_eq!(
                ingress
                    .metadata
                    .labels
                    .as_ref()
                    .expect("labels")
                    .get("app.kubernetes.io/managed-by")
                    .map(String::as_str),
                Some("ravel-operator")
            );
        }
    }

    #[test]
    fn rendered_ingresses_never_share_a_host_and_path() {
        // The regression this whole fix exists for. ingress-nginx builds one
        // `location <path>` per server block; two Ingress objects claiming the
        // same (host, path) is a duplicate-path conflict that keeps one and
        // silently drops the other's location. The OTLP/HTTP and OTLP/gRPC
        // objects previously both rendered path `/`, so the gRPC object's
        // grpc_pass and affinity annotations never took effect.
        //
        // Asserted over the WHOLE rendered set, across every permutation of grpc
        // on/off and the host cardinalities (none, one, several) -- not
        // per-object, which is exactly the blind spot that let the defect ship.
        for grpc in [true, false] {
            for hosts in [
                vec![],
                vec!["ingest.example.com".to_string()],
                vec![
                    "a.example.com".to_string(),
                    "b.example.com".to_string(),
                    "c.example.com".to_string(),
                ],
            ] {
                let mut spec = base_spec();
                spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
                    grpc,
                    hosts: hosts.clone(),
                    ..default_affinity()
                });
                let ingresses = desired_gateway_ingresses(&spec, "prod");

                let mut seen: BTreeMap<(Option<String>, String), String> = BTreeMap::new();
                for ingress in &ingresses {
                    let name = ingress.metadata.name.clone().expect("name");
                    for pair in host_paths_of(ingress) {
                        if let Some(other) = seen.insert(pair.clone(), name.clone()) {
                            panic!(
                                "Ingress objects {other} and {name} both claim \
                                 (host, path) {pair:?}; ingress-nginx drops one \
                                 location as a duplicate-path conflict \
                                 (grpc={grpc}, hosts={hosts:?})"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn grpc_ingress_paths_cover_every_registered_ingest_grpc_service() {
        // The gRPC Ingress must route every gRPC service registered on the
        // ingest surface in `services/ravel-server/src/lib.rs`. There are FOUR:
        // the three OTLP collectors plus the OTAP ArrowMetricsService
        // (lib.rs, registered behind its config but routed unconditionally --
        // an unrouted path costs nothing, an unroutable service breaks ingest).
        // The next gRPC service added at that registration site must be added
        // to INGEST_GRPC_PATHS, or its RPCs have no Ingress route.
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(default_affinity());
        let ingresses = desired_gateway_ingresses(&spec, "prod");
        let grpc_ingress = ingresses
            .iter()
            .find(|i| i.metadata.name.as_deref() == Some("prod-gateway-ingest-grpc"))
            .expect("gRPC Ingress");
        let paths = paths_of(grpc_ingress);
        for service in [
            "/opentelemetry.proto.collector.metrics.v1.MetricsService",
            "/opentelemetry.proto.collector.logs.v1.LogsService",
            "/opentelemetry.proto.collector.trace.v1.TraceService",
            "/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService",
        ] {
            assert!(
                paths.iter().any(|p| p == service),
                "the gRPC Ingress must route {service}; got {paths:?}"
            );
        }
    }

    #[test]
    fn affinity_absent_renders_no_ingress_and_nothing_else_changes() {
        // Deliverable 4: affinity off is today's behavior exactly. Two claims.
        //
        // First: no Ingress at all, which is what the operator rendered before
        // this change (it managed only Deployments and Services).
        let spec = base_spec();
        assert!(spec.gateway.ingest_affinity.is_none());
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        assert!(
            objects.gateway_ingresses.is_empty(),
            "a RavelCluster with no affinity field must render no Ingress"
        );

        // Second: the affinity field cannot reach any other object. Rendering
        // the same spec WITH affinity enabled must produce byte-identical
        // Deployments and Services -- which is only possible if the affinity
        // code path touches nothing but the Ingress list, so the affinity-absent
        // render of every other object is necessarily unchanged from today.
        let mut affinity_spec = base_spec();
        affinity_spec.gateway.ingest_affinity = Some(default_affinity());
        let with_affinity =
            desired_objects(&affinity_spec, "prod", "default", &ctx()).expect("render");
        for (off, on, what) in [
            (
                serde_json::to_value(&objects.gateway_deployment),
                serde_json::to_value(&with_affinity.gateway_deployment),
                "gateway Deployment",
            ),
            (
                serde_json::to_value(&objects.gateway_service),
                serde_json::to_value(&with_affinity.gateway_service),
                "gateway Service",
            ),
            (
                serde_json::to_value(&objects.query_deployment),
                serde_json::to_value(&with_affinity.query_deployment),
                "query Deployment",
            ),
            (
                serde_json::to_value(&objects.query_service),
                serde_json::to_value(&with_affinity.query_service),
                "query Service",
            ),
            (
                serde_json::to_value(&objects.maintain_deployment),
                serde_json::to_value(&with_affinity.maintain_deployment),
                "maintain Deployment",
            ),
        ] {
            assert_eq!(
                off.expect("serialize affinity-off"),
                on.expect("serialize affinity-on"),
                "{what} must not change when affinity is enabled, so the \
                 affinity-off render is necessarily unchanged from before"
            );
        }

        // And the gateway Service in particular is still the plain ClusterIP
        // Service it always was: no sessionAffinity was bolted on. Client-IP
        // affinity keys on the collector's or gateway proxy's address, not the
        // tenant, so it would look like affinity while pinning the wrong thing.
        let gspec = objects.gateway_service.spec.as_ref().expect("service spec");
        assert_eq!(gspec.session_affinity, None);
        assert_eq!(gspec.session_affinity_config, None);
        assert_eq!(
            gspec.type_, None,
            "the gateway Service stays a default ClusterIP"
        );
    }

    #[test]
    fn affinity_disabled_or_grpc_off_removes_the_matching_ingress() {
        // `enabled: false` is the incident switch: it must converge all the way
        // back to the affinity-absent render, not leave a stale Ingress.
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            enabled: false,
            ..default_affinity()
        });
        assert!(desired_gateway_ingresses(&spec, "prod").is_empty());

        // gRPC off renders the HTTP Ingress only.
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            grpc: false,
            ..default_affinity()
        });
        let ingresses = desired_gateway_ingresses(&spec, "prod");
        assert_eq!(ingresses.len(), 1);
        assert_eq!(
            ingresses[0].metadata.name.as_deref(),
            Some("prod-gateway-ingest")
        );
    }

    #[test]
    fn every_renderable_ingress_name_is_in_the_delete_sweep_list() {
        // The controller applies what `desired_gateway_ingresses` returns and
        // deletes every OTHER name in `possible_ingest_ingress_names`. A name
        // the render can produce but that list omits would be an Ingress that
        // routes live traffic forever after affinity is turned off.
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(default_affinity());
        let renderable: Vec<String> = desired_gateway_ingresses(&spec, "prod")
            .iter()
            .map(|i| i.metadata.name.clone().expect("name"))
            .collect();
        let sweepable = possible_ingest_ingress_names("prod");
        for name in &renderable {
            assert!(
                sweepable.contains(name),
                "{name} can be rendered but is never swept: {sweepable:?}"
            );
        }
        assert_eq!(sweepable.len(), renderable.len());
    }

    #[test]
    fn subset_size_is_configurable_for_a_high_volume_tenant() {
        // A fixed subset is a throughput ceiling by construction (ADR-0076
        // decision 1), so the size must scale up.
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            subset_size: 6,
            ..default_affinity()
        });
        for ingress in desired_gateway_ingresses(&spec, "prod") {
            assert_eq!(
                annotations_of(&ingress)
                    .get("nginx.ingress.kubernetes.io/upstream-hash-by-subset-size")
                    .map(String::as_str),
                Some("6")
            );
        }
    }

    #[test]
    fn key_sources_render_the_matching_nginx_variable() {
        let mut spec = base_spec();
        let hash_by = |spec: &RavelClusterSpec| -> String {
            annotations_of(&desired_gateway_ingresses(spec, "prod")[0])
                .get("nginx.ingress.kubernetes.io/upstream-hash-by")
                .cloned()
                .expect("upstream-hash-by")
        };

        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            key: AffinityKeySpec {
                source: AffinityKeySource::MtlsSubject,
                header_name: None,
            },
            ..default_affinity()
        });
        assert_eq!(hash_by(&spec), "$ssl_client_s_dn");

        // A header name is mangled exactly the way nginx names the variable.
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            key: AffinityKeySpec {
                source: AffinityKeySource::Header,
                header_name: Some("X-Scope-OrgID".to_string()),
            },
            ..default_affinity()
        });
        assert_eq!(hash_by(&spec), "$http_x_scope_orgid");

        // Every character outside [a-z0-9] becomes '_', so no header name can
        // reach the annotation as nginx syntax. (The CRD's `pattern` rejects
        // this at admission; the renderer is the second belt.)
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            key: AffinityKeySpec {
                source: AffinityKeySource::Header,
                header_name: Some("x; evil $var".to_string()),
            },
            ..default_affinity()
        });
        let mangled = hash_by(&spec);
        assert_eq!(mangled, "$http_x__evil__var");
        assert!(!mangled[1..].contains(['$', ';', ' ']));
    }

    #[test]
    fn passthrough_annotations_cannot_disable_the_affinity() {
        // The extra-annotations map exists for proxy-body-size and other
        // controllers. It is merged FIRST, so an entry colliding with an
        // affinity annotation loses rather than silently unpinning every tenant.
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            annotations: BTreeMap::from([
                (
                    "nginx.ingress.kubernetes.io/proxy-body-size".to_string(),
                    "16m".to_string(),
                ),
                (
                    "nginx.ingress.kubernetes.io/upstream-hash-by-subset".to_string(),
                    "false".to_string(),
                ),
                (
                    "nginx.ingress.kubernetes.io/service-upstream".to_string(),
                    "true".to_string(),
                ),
            ]),
            ..default_affinity()
        });
        let annotations = annotations_of(&desired_gateway_ingresses(&spec, "prod")[0]);
        assert_eq!(
            annotations
                .get("nginx.ingress.kubernetes.io/proxy-body-size")
                .map(String::as_str),
            Some("16m"),
            "an unrelated passthrough annotation must survive"
        );
        assert_eq!(
            annotations
                .get("nginx.ingress.kubernetes.io/upstream-hash-by-subset")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            annotations
                .get("nginx.ingress.kubernetes.io/service-upstream")
                .map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn no_hosts_renders_one_host_less_rule() {
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(default_affinity());
        let ingress = desired_gateway_ingresses(&spec, "prod").remove(0);
        let ispec = ingress.spec.as_ref().expect("spec");
        let rules = ispec.rules.as_ref().expect("rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].host, None);
        assert_eq!(ispec.tls, None, "no TLS Secret configured renders no tls");
        assert_eq!(ispec.ingress_class_name, None);
    }

    #[test]
    fn deployment_key_secret_rotation_rolls_every_tier() {
        // Every tier mounts the SAME
        // `deploymentKeySecretRef` Secret (unlike per-tier credential
        // overrides), so its rotation must roll all three, the same as the
        // shared-credential case above.
        let spec = base_spec();
        let before = RenderCtx {
            tenant_names: vec!["acme".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: BTreeMap::from([(
                "ravel-s3".to_string(),
                "shared-1".to_string(),
            )]),
            deployment_key_resource_version: Some("dk-1".to_string()),
            audit_token_key_resource_version: None,
        };
        let after = RenderCtx {
            deployment_key_resource_version: Some("dk-2".to_string()),
            ..before.clone()
        };

        let gb = desired_gateway_deployment(&spec, "prod", &before);
        let ga = desired_gateway_deployment(&spec, "prod", &after);
        let qb = desired_query_deployment(&spec, "prod", &before);
        let qa = desired_query_deployment(&spec, "prod", &after);
        let mb = desired_maintain_deployment(&spec, "prod", &before)
            .expect("no gc render error")
            .expect("enabled");
        let ma = desired_maintain_deployment(&spec, "prod", &after)
            .expect("no gc render error")
            .expect("enabled");
        assert_ne!(
            checksum_of(&gb),
            checksum_of(&ga),
            "gateway must roll when the deployment key rotates"
        );
        assert_ne!(
            checksum_of(&qb),
            checksum_of(&qa),
            "query must roll when the deployment key rotates"
        );
        assert_ne!(
            checksum_of(&mb),
            checksum_of(&ma),
            "maintain must roll when the deployment key rotates"
        );
    }

    /// The `spec` body of a rendered Gateway API route.
    fn route_spec(route: &DynamicObject) -> serde_json::Value {
        route
            .data
            .get("spec")
            .expect("route must carry a spec")
            .clone()
    }

    /// A rendered route's `kind` from its `TypeMeta`.
    fn route_kind(route: &DynamicObject) -> String {
        route.types.as_ref().expect("route types").kind.clone()
    }

    /// An exposure spec pointing at a `Gateway`, for the render tests.
    fn exposure_with(
        gateway_ref: GatewayReference,
        hostnames: Vec<String>,
        grpc: bool,
    ) -> GatewaySpec {
        GatewaySpec {
            exposure: Some(GatewayExposureSpec {
                gateway_api: Some(GatewayApiExposureSpec {
                    gateway_ref,
                    hostnames,
                    grpc,
                }),
            }),
            ..GatewaySpec::default()
        }
    }

    #[test]
    fn exposure_gateway_api_renders_httproute_and_grpcroute_independent_of_affinity() {
        // ADR-0080 decision 2, the acceptance test. Asserted through
        // `desired_objects`, which is the exact render the controller applies.
        //
        // Case 1: exposure with affinity absent (plain routing, no pinning),
        // grpc default true, one hostname, gatewayRef namespace omitted.
        let mut spec = base_spec();
        spec.gateway = exposure_with(
            GatewayReference {
                name: "public-gw".to_string(),
                namespace: None,
            },
            vec!["ingest.example.com".to_string()],
            true,
        );
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        let (http, grpc) = objects.gateway_routes;
        let http = http.expect("HTTPRoute always rendered when exposure is set");
        let grpc = grpc.expect("GRPCRoute rendered when grpc is true");

        // Kinds, versions, and names.
        assert_eq!(route_kind(&http), "HTTPRoute");
        assert_eq!(route_kind(&grpc), "GRPCRoute");
        for route in [&http, &grpc] {
            assert_eq!(
                route.types.as_ref().expect("types").api_version,
                "gateway.networking.k8s.io/v1"
            );
            assert_eq!(
                route.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/component"],
                "gateway"
            );
        }
        assert_eq!(http.metadata.name.as_deref(), Some("prod-gateway-route"));
        assert_eq!(
            grpc.metadata.name.as_deref(),
            Some("prod-gateway-route-grpc")
        );

        // parentRefs name only (namespace omitted -> Gateway API defaults it to
        // the route's own namespace, i.e. the CR's namespace).
        let http_spec = route_spec(&http);
        assert_eq!(
            http_spec["parentRefs"][0]["name"],
            serde_json::json!("public-gw")
        );
        assert!(
            http_spec["parentRefs"][0].get("namespace").is_none(),
            "omitted gatewayRef namespace must not render a parentRef namespace"
        );

        // backendRefs point at the gateway Service, HTTP route on 4318, gRPC on
        // 4317.
        assert_eq!(
            http_spec["rules"][0]["backendRefs"][0]["name"],
            serde_json::json!("prod-gateway")
        );
        assert_eq!(
            http_spec["rules"][0]["backendRefs"][0]["port"],
            serde_json::json!(HTTP_PORT)
        );
        assert_eq!(
            route_spec(&grpc)["rules"][0]["backendRefs"][0]["port"],
            serde_json::json!(GRPC_PORT)
        );
        assert_eq!(
            route_spec(&grpc)["rules"][0]["backendRefs"][0]["name"],
            serde_json::json!("prod-gateway")
        );

        // Hostnames carried on both route kinds.
        assert_eq!(
            http_spec["hostnames"],
            serde_json::json!(["ingest.example.com"])
        );
        assert_eq!(
            route_spec(&grpc)["hostnames"],
            serde_json::json!(["ingest.example.com"])
        );

        // Case 2: grpc: false renders only the HTTPRoute.
        let mut spec = base_spec();
        spec.gateway = exposure_with(
            GatewayReference {
                name: "public-gw".to_string(),
                namespace: None,
            },
            Vec::new(),
            false,
        );
        // Affinity absent: router not present, HTTPRoute stays on the gateway.
        let (http, grpc) = desired_gateway_routes(&spec, "prod", false);
        assert!(
            http.is_some(),
            "HTTPRoute still rendered when grpc is false"
        );
        assert!(grpc.is_none(), "no GRPCRoute when grpc is false");
        // Empty hostnames renders no `hostnames` key.
        assert!(
            route_spec(&http.expect("HTTPRoute"))
                .get("hostnames")
                .is_none(),
            "empty hostnames must not render a hostnames key"
        );

        // Case 3: exposure combined with affinity enabled on backend
        // ravelNative (allowed by the CEL rule) still renders both routes,
        // independent of affinity, with an explicit gatewayRef namespace.
        let mut spec = base_spec();
        spec.gateway = GatewaySpec {
            ingest_affinity: Some(IngestAffinitySpec {
                backend: AffinityBackend::RavelNative,
                ..IngestAffinitySpec::default()
            }),
            ..exposure_with(
                GatewayReference {
                    name: "public-gw".to_string(),
                    namespace: Some("gw-ns".to_string()),
                },
                Vec::new(),
                true,
            )
        };
        // Routes render independent of affinity; router_available is irrelevant
        // to this assertion (it checks parentRefs, not the backendRef).
        let (http, grpc) = desired_gateway_routes(&spec, "prod", false);
        let http = http.expect("HTTPRoute rendered alongside ravelNative affinity");
        assert!(
            grpc.is_some(),
            "GRPCRoute rendered alongside ravelNative affinity"
        );
        // The gateway also still renders its affinity nothing here (ravelNative
        // renders no Ingress), and the route parentRefs carry the explicit
        // namespace.
        assert_eq!(
            route_spec(&http)["parentRefs"][0]["namespace"],
            serde_json::json!("gw-ns")
        );

        // Case 4: removing exposure renders nothing, and the delete-sweep names
        // both route kinds so the controller can remove an existing pair.
        let mut spec = base_spec();
        spec.gateway = GatewaySpec::default();
        assert_eq!(
            desired_gateway_routes(&spec, "prod", false),
            (None, None),
            "exposure absent renders no routes"
        );
        let sweep = possible_gateway_route_names("prod");
        assert_eq!(
            sweep,
            vec![
                "prod-gateway-route".to_string(),
                "prod-gateway-route-grpc".to_string()
            ],
            "the delete-sweep must name both route kinds so both are removed"
        );
    }

    /// A spec with `backend: ravelNative` affinity, `routerImage` set, and no
    /// tenant-tokens Secret (so the frozen-args assertion stays clean).
    fn ravel_native_spec(subset_size: u32, source: AffinityKeySource) -> RavelClusterSpec {
        let mut spec = base_spec();
        spec.tenant_tokens_secret_ref = None;
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            backend: AffinityBackend::RavelNative,
            router_image: Some("registry.example/ravel-ingest-router:v1".to_string()),
            subset_size,
            key: AffinityKeySpec {
                source,
                header_name: None,
            },
            ..IngestAffinitySpec::default()
        });
        spec
    }

    #[test]
    fn backend_ravel_native_renders_router_deployment_with_frozen_cli_args() {
        // ADR-0080 decision 3, deliverables 3 and 5. The whole point of this
        // test: a future accidental rename of the router's own flag names (in
        // #184's crate) becomes a caught test failure here instead of a
        // crashlooping pod. The args are pinned as a full ordered vector, not a
        // contains-check.
        //
        // canonical-tenant needs a resolver, so this fixture carries a
        // tenantTokensSecretRef (a valid, non-crashing config): the router
        // renders --tenant-token resolver flags, and the frozen vector pins them
        // too. The render-time rejection of canonical-tenant WITHOUT a resolver
        // is proven separately in
        // `canonical_tenant_without_resolver_is_a_typed_error_not_a_panic`.
        let mut spec = ravel_native_spec(3, AffinityKeySource::CanonicalTenant);
        spec.tenant_tokens_secret_ref = Some(LocalSecretRef {
            name: "ravel-tokens".to_string(),
        });
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        let deployment = objects
            .router_deployment
            .expect("ravelNative renders a router Deployment");

        assert_eq!(
            args_of(&deployment),
            vec![
                "--gateway-service-name",
                "prod-gateway",
                "--gateway-service-namespace",
                "default",
                "--gateway-port-name",
                "http",
                "--subset-size",
                "3",
                "--key-source",
                "canonical-tenant",
                "--listen-http",
                "0.0.0.0:8080",
                // canonical-tenant with a resolver renders the --tenant-token
                // flags, one per tenant in ctx() order.
                "--tenant-token",
                "$(RAVEL_TENANT_TOKEN_0)=acme",
                "--tenant-token",
                "$(RAVEL_TENANT_TOKEN_1)=globex",
            ]
        );

        // The image is routerImage, NEVER spec.image (a different binary).
        let container = container_of(&deployment);
        assert_eq!(
            container.image.as_deref(),
            Some("registry.example/ravel-ingest-router:v1")
        );
        assert_ne!(container.image.as_deref(), Some(spec.image.as_str()));
        assert_eq!(container.name, "ravel-ingest-router");

        // HTTP-only: exactly one container port, 8080, no gRPC port (#184).
        let ports = container.ports.as_ref().expect("router ports");
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].container_port, ROUTER_HTTP_PORT);

        // Runs under its own ServiceAccount (deliverable 7).
        let pod = deployment
            .spec
            .as_ref()
            .expect("spec")
            .template
            .spec
            .as_ref()
            .expect("pod spec");
        assert_eq!(
            pod.service_account_name.as_deref(),
            Some("prod-ingest-router")
        );

        // The full router object set is rendered.
        assert!(objects.router_service.is_some());
        assert!(objects.router_service_account.is_some());
        assert!(objects.router_role.is_some());
        assert!(objects.router_role_binding.is_some());

        // The Role is least-privilege: get/list/watch on endpointslices and
        // services only, no write verbs, no other resource.
        let role = objects.router_role.expect("router Role");
        let rules = role.rules.expect("role rules");
        assert_eq!(rules.len(), 2);
        for rule in &rules {
            assert_eq!(
                rule.verbs,
                vec!["get".to_string(), "list".to_string(), "watch".to_string()]
            );
        }
        assert_eq!(
            rules[0].api_groups.as_deref(),
            Some(["discovery.k8s.io".to_string()].as_slice())
        );
        assert_eq!(
            rules[0].resources.as_deref(),
            Some(["endpointslices".to_string()].as_slice())
        );
        assert_eq!(
            rules[1].api_groups.as_deref(),
            Some([String::new()].as_slice())
        );
        assert_eq!(
            rules[1].resources.as_deref(),
            Some(["services".to_string()].as_slice())
        );

        // The RoleBinding subject is the router SA in this namespace, bound to
        // the router Role.
        let binding = objects.router_role_binding.expect("router RoleBinding");
        assert_eq!(binding.role_ref.kind, "Role");
        assert_eq!(binding.role_ref.name, "prod-ingest-router");
        let subjects = binding.subjects.expect("subjects");
        assert_eq!(subjects[0].kind, "ServiceAccount");
        assert_eq!(subjects[0].name, "prod-ingest-router");
        assert_eq!(subjects[0].namespace.as_deref(), Some("default"));
    }

    #[test]
    fn router_renders_key_header_name_and_tenant_tokens_only_where_valid() {
        // key.source header renders --key-header-name; a non-canonical source
        // renders NO --tenant-token even with a tenant-tokens Secret (the router
        // CLI rejects those flags unless --key-source canonical-tenant).
        let mut spec = ravel_native_spec(2, AffinityKeySource::Header);
        spec.tenant_tokens_secret_ref = Some(LocalSecretRef {
            name: "ravel-tokens".to_string(),
        });
        if let Some(a) = spec.gateway.ingest_affinity.as_mut() {
            a.key.header_name = Some("X-Scope-OrgID".to_string());
        }
        let args = args_of(
            &desired_router_deployment(&spec, "prod", "ns", &ctx())
                .expect("render")
                .expect("router deployment"),
        );
        assert_eq!(arg_value(&args, "--key-source").as_deref(), Some("header"));
        assert_eq!(
            arg_value(&args, "--key-header-name").as_deref(),
            Some("X-Scope-OrgID")
        );
        assert!(
            !args.iter().any(|a| a == "--tenant-token"),
            "non-canonical key source must render no --tenant-token: {args:?}"
        );

        // canonical-tenant WITH a tenant-tokens Secret renders --tenant-token.
        let mut spec = ravel_native_spec(2, AffinityKeySource::CanonicalTenant);
        spec.tenant_tokens_secret_ref = Some(LocalSecretRef {
            name: "ravel-tokens".to_string(),
        });
        let deployment = desired_router_deployment(&spec, "prod", "ns", &ctx())
            .expect("render")
            .expect("router deployment");
        let args = args_of(&deployment);
        assert!(
            args.iter().any(|a| a == "--tenant-token"),
            "canonical-tenant with a tokens Secret must render --tenant-token: {args:?}"
        );
        // And the token reaches the container via $(VAR) env, never inline.
        assert!(
            container_of(&deployment)
                .env
                .as_ref()
                .expect("token env")
                .iter()
                .any(|e| e.name.starts_with("RAVEL_TENANT_TOKEN_"))
        );
    }

    #[test]
    fn backend_ingress_nginx_renders_no_router_objects() {
        // Default backend: zero router objects, zero behavior change for every
        // existing CR. Also true when affinity is absent entirely.
        for affinity in [
            None,
            Some(IngestAffinitySpec::default()), // backend: ingressNginx (default)
            Some(IngestAffinitySpec {
                enabled: false,
                backend: AffinityBackend::RavelNative,
                router_image: Some("x".to_string()),
                ..IngestAffinitySpec::default()
            }), // ravelNative but disabled
        ] {
            let mut spec = base_spec();
            spec.gateway.ingest_affinity = affinity;
            let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
            assert!(objects.router_deployment.is_none());
            assert!(objects.router_service.is_none());
            assert!(objects.router_service_account.is_none());
            assert!(objects.router_role.is_none());
            assert!(objects.router_role_binding.is_none());
        }
    }

    #[test]
    fn router_image_absent_under_ravel_native_is_a_typed_error_not_a_panic() {
        // ADR-0080 decision 3, deliverable 1: routerImage is required under
        // ravelNative. Absent -> a typed RenderError (which the controller turns
        // into a Degraded condition), never a panic and never a Deployment with
        // an empty image.
        let mut spec = base_spec();
        spec.gateway.ingest_affinity = Some(IngestAffinitySpec {
            backend: AffinityBackend::RavelNative,
            router_image: None,
            ..IngestAffinitySpec::default()
        });
        assert_eq!(
            desired_router_deployment(&spec, "prod", "default", &ctx()),
            Err(RenderError::RouterImageMissing)
        );
        // desired_objects captures the router error rather than propagating it
        // (Fix 5): it returns Ok, the router objects are None, and the error is
        // surfaced in router_render_error for the controller to degrade the
        // router alone.
        let objects =
            desired_objects(&spec, "prod", "default", &ctx()).expect("render never aborts");
        assert_eq!(
            objects.router_render_error,
            Some(RenderError::RouterImageMissing)
        );
        assert!(objects.router_deployment.is_none());
    }

    #[test]
    fn canonical_tenant_without_resolver_is_a_typed_error_not_a_panic() {
        // Fix 1 (B1): canonical-tenant with no resolver would render a command
        // line the router's own CLI rejects at startup (a guaranteed crashloop).
        // tenantTokensSecretRef is the only resolver surface this CRD exposes
        // today; absent it, the operator renders no router objects and surfaces a
        // typed error, never a panic and never a crash-looping Deployment.
        let spec = ravel_native_spec(2, AffinityKeySource::CanonicalTenant);
        assert!(spec.tenant_tokens_secret_ref.is_none());
        assert_eq!(
            desired_router_deployment(&spec, "prod", "default", &ctx()),
            Err(RenderError::CanonicalTenantResolverMissing)
        );
        // Captured, not propagated (Fix 5): every other tier still renders.
        let objects =
            desired_objects(&spec, "prod", "default", &ctx()).expect("render never aborts");
        assert_eq!(
            objects.router_render_error,
            Some(RenderError::CanonicalTenantResolverMissing)
        );
        assert!(objects.router_deployment.is_none());
        assert!(objects.router_service.is_none());
        assert!(objects.router_service_account.is_none());
        assert!(objects.router_role.is_none());
        assert!(objects.router_role_binding.is_none());

        // Happy path: the same config WITH a resolver renders a router
        // Deployment cleanly.
        let mut ok = spec.clone();
        ok.tenant_tokens_secret_ref = Some(LocalSecretRef {
            name: "ravel-tokens".to_string(),
        });
        assert!(
            desired_router_deployment(&ok, "prod", "default", &ctx())
                .expect("render")
                .is_some(),
            "canonical-tenant with a resolver must render a router Deployment"
        );
    }

    #[test]
    fn canonical_tenant_with_secret_ref_but_no_live_tenants_is_a_typed_error() {
        // F1: the resolver guard must check the ACTUAL rendered resolver flags,
        // not `tenant_tokens_secret_ref.is_none()`. A Secret ref SET on the spec
        // while the Secret resolves to zero live keys this cycle is a reachable
        // state distinct from the Secret being absent entirely (controller.rs
        // derives ctx.tenant_names from the Secret's live keys; an absent Secret
        // is a hard error that aborts the reconcile before rendering is ever
        // reached, a present-but-empty one is not). In the zero-keys case
        // `tenant_token_args` renders zero `--tenant-token` flags and the
        // router's own CLI still crashloops at startup -- the exact case the
        // spec-field guard missed. The render must reject it, not ship a
        // crashlooping Deployment.
        let mut spec = ravel_native_spec(2, AffinityKeySource::CanonicalTenant);
        spec.tenant_tokens_secret_ref = Some(LocalSecretRef {
            name: "ravel-tokens".to_string(),
        });
        // The Secret resolves to zero tenants this cycle.
        let empty_ctx = RenderCtx {
            tenant_names: Vec::new(),
            ..ctx()
        };
        assert_eq!(
            desired_router_deployment(&spec, "prod", "default", &empty_ctx),
            Err(RenderError::CanonicalTenantResolverMissing),
            "a set secret_ref that resolves to zero live tenants must still be a \
             typed resolver-missing error, not a crashlooping Deployment"
        );
        // And it is captured (not propagated) by desired_objects, exactly like the
        // secret-ref-absent case: router objects None, error surfaced.
        let objects =
            desired_objects(&spec, "prod", "default", &empty_ctx).expect("render never aborts");
        assert_eq!(
            objects.router_render_error,
            Some(RenderError::CanonicalTenantResolverMissing)
        );
        assert!(objects.router_deployment.is_none());

        // Sanity: the same spec with a non-empty live tenant set (the existing
        // fixture shape) still renders cleanly -- the guard did not over-reject.
        assert!(
            desired_router_deployment(&spec, "prod", "default", &ctx())
                .expect("render")
                .is_some(),
            "non-empty tenant_names must still render a router Deployment"
        );
    }

    #[test]
    fn router_render_failure_still_renders_every_other_tier() {
        // Fix 5 (non-blocking): a router render error must degrade ONLY the
        // router, never abort the whole reconcile. desired_objects returns the
        // gateway/query/maintain tiers as usual for BOTH router render errors,
        // with zero router objects and the error captured for the controller to
        // turn into a Degraded condition (see degraded_reason tests in
        // controller.rs). Proving it at the render layer is the testable proof:
        // the controller applies exactly desired_objects' contents (no live API
        // server in this crate's tests, see the module doc).
        let router_image_missing = {
            let mut s = base_spec();
            s.gateway.ingest_affinity = Some(IngestAffinitySpec {
                backend: AffinityBackend::RavelNative,
                router_image: None,
                ..IngestAffinitySpec::default()
            });
            s
        };
        let canonical_no_resolver = {
            let mut s = ravel_native_spec(2, AffinityKeySource::CanonicalTenant);
            s.tenant_tokens_secret_ref = None;
            s
        };
        for spec in [router_image_missing, canonical_no_resolver] {
            let objects =
                desired_objects(&spec, "prod", "default", &ctx()).expect("render never aborts");
            assert!(
                objects.router_render_error.is_some(),
                "the router error is captured, not propagated"
            );
            assert!(objects.router_deployment.is_none());
            assert!(objects.router_service.is_none());
            assert!(objects.router_service_account.is_none());
            assert!(objects.router_role.is_none());
            assert!(objects.router_role_binding.is_none());
            // Every other tier still renders.
            assert_eq!(
                objects.gateway_deployment.metadata.name.as_deref(),
                Some("prod-gateway"),
                "gateway still renders when the router render fails"
            );
            assert_eq!(
                objects.query_deployment.metadata.name.as_deref(),
                Some("prod-query"),
                "query still renders when the router render fails"
            );
            assert!(
                objects.maintain_deployment.is_some(),
                "maintain still renders when the router render fails"
            );
        }
    }

    #[test]
    fn gateway_route_backend_ref_targets_router_service_under_ravel_native() {
        // ADR-0080 decision 3, deliverable 4, as corrected by Fix 3 (B3). Under
        // exposure + ravelNative affinity, ONLY the HTTPRoute switches to the
        // router Service; the GRPCRoute keeps targeting the gateway Service,
        // because the router serves HTTP only today (single port_name per
        // process) and switching gRPC to it would break gRPC ingest that works
        // now. The same exposure under the default ingressNginx backend still
        // targets the gateway Service for both (regression guard on Wave 2
        // behavior).
        let router_gw = GatewayReference {
            name: "public-gw".to_string(),
            namespace: None,
        };

        // ravelNative: HTTPRoute -> router Service on the router's HTTP port;
        // GRPCRoute -> STILL the gateway Service on the gateway's gRPC port.
        let mut spec = ravel_native_spec(2, AffinityKeySource::CanonicalTenant);
        spec.gateway.exposure = exposure_with(router_gw.clone(), Vec::new(), true).exposure;
        // Router renders cleanly here (router_available = true): the HTTPRoute
        // switches to the router Service.
        let (http, grpc) = desired_gateway_routes(&spec, "prod", true);
        let http = http.expect("HTTPRoute");
        let grpc = grpc.expect("GRPCRoute");
        assert_eq!(
            route_spec(&http)["rules"][0]["backendRefs"][0]["name"],
            serde_json::json!("prod-ingest-router")
        );
        assert_eq!(
            route_spec(&http)["rules"][0]["backendRefs"][0]["port"],
            serde_json::json!(ROUTER_HTTP_PORT)
        );
        assert_eq!(
            route_spec(&grpc)["rules"][0]["backendRefs"][0]["name"],
            serde_json::json!("prod-gateway"),
            "GRPCRoute must keep targeting the gateway Service: the router serves \
             HTTP only today, so routing gRPC through it would break gRPC ingest"
        );
        assert_eq!(
            route_spec(&grpc)["rules"][0]["backendRefs"][0]["port"],
            serde_json::json!(GRPC_PORT),
            "GRPCRoute must keep the gateway's gRPC port, exactly as on main"
        );

        // Default ingressNginx backend + same exposure: still the gateway
        // Service on 4318/4317, exactly as before.
        let mut spec = base_spec();
        spec.gateway = exposure_with(router_gw, Vec::new(), true);
        // ingressNginx backend: no router, HTTPRoute stays on the gateway Service.
        let (http, grpc) = desired_gateway_routes(&spec, "prod", false);
        assert_eq!(
            route_spec(&http.expect("HTTPRoute"))["rules"][0]["backendRefs"][0]["name"],
            serde_json::json!("prod-gateway")
        );
        assert_eq!(
            route_spec(&grpc.expect("GRPCRoute"))["rules"][0]["backendRefs"][0]["name"],
            serde_json::json!("prod-gateway")
        );
    }

    #[test]
    fn httproute_falls_back_to_gateway_service_on_router_render_error() {
        // F2: on ANY router render error the same reconcile pass would apply the
        // router's objects as no-ops and the delete-sweep removes the router's
        // Service, yet a static `backend: ravelNative` check would still point the
        // HTTPRoute at that (now-deleted) Service -- an ingest outage that does not
        // self-heal. The route renderer must instead fall back to the gateway
        // Service whenever the router will not be present this pass. Asserted
        // through `desired_objects`, the exact render the controller applies, so
        // the router-outcome-to-route wiring is what is under test.
        for spec in [
            // CanonicalTenantResolverMissing (F1's path): ravelNative + canonical,
            // no tenant-tokens Secret ref.
            {
                let mut s = ravel_native_spec(2, AffinityKeySource::CanonicalTenant);
                s.tenant_tokens_secret_ref = None;
                s.gateway.exposure = exposure_with(
                    GatewayReference {
                        name: "public-gw".to_string(),
                        namespace: None,
                    },
                    Vec::new(),
                    true,
                )
                .exposure;
                s
            },
            // RouterImageMissing: ravelNative with routerImage unset.
            {
                let mut s = base_spec();
                s.gateway.ingest_affinity = Some(IngestAffinitySpec {
                    backend: AffinityBackend::RavelNative,
                    router_image: None,
                    ..IngestAffinitySpec::default()
                });
                s.gateway.exposure = exposure_with(
                    GatewayReference {
                        name: "public-gw".to_string(),
                        namespace: None,
                    },
                    Vec::new(),
                    true,
                )
                .exposure;
                s
            },
        ] {
            let objects =
                desired_objects(&spec, "prod", "default", &ctx()).expect("render never aborts");
            assert!(
                objects.router_render_error.is_some(),
                "precondition: the router render failed this pass"
            );
            assert!(
                objects.router_service.is_none(),
                "precondition: no router Service will exist after this pass"
            );
            let (http, _grpc) = objects.gateway_routes;
            let http = http.expect("HTTPRoute still rendered");
            assert_eq!(
                route_spec(&http)["rules"][0]["backendRefs"][0]["name"],
                serde_json::json!("prod-gateway"),
                "on a router render error the HTTPRoute must fall back to the \
                 gateway Service, never point at the router Service the same pass \
                 deletes"
            );
            assert_eq!(
                route_spec(&http)["rules"][0]["backendRefs"][0]["port"],
                serde_json::json!(HTTP_PORT),
                "fallback uses the gateway's HTTP port, not the router's"
            );
        }
    }

    #[test]
    fn httproute_targets_router_service_through_desired_objects_when_router_renders() {
        // The positive half of the F2 wiring: `gateway_route_backend_ref_
        // targets_router_service_under_ravel_native` above passes
        // `router_available` as a literal `true` straight to
        // `desired_gateway_routes`, so it only proves that function's own
        // internal branch -- it does not prove `desired_objects` ever computes
        // `router_available = true` on a genuinely successful router render. A
        // mutation to `desired_objects` that always passed `false` through (permanently
        // disabling the ADR-0080 decision 3 / deliverable 4 switch) would leave
        // that test green. Go through `desired_objects`, the render the
        // controller actually calls, so this wiring is what's under test.
        let mut spec = ravel_native_spec(2, AffinityKeySource::CanonicalTenant);
        spec.tenant_tokens_secret_ref = Some(LocalSecretRef {
            name: "ravel-tokens".to_string(),
        });
        spec.gateway.exposure = exposure_with(
            GatewayReference {
                name: "public-gw".to_string(),
                namespace: None,
            },
            Vec::new(),
            true,
        )
        .exposure;

        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        assert!(
            objects.router_render_error.is_none(),
            "precondition: the router renders cleanly this pass"
        );
        assert!(
            objects.router_service.is_some(),
            "precondition: a router Service will exist after this pass"
        );

        let (http, _grpc) = objects.gateway_routes;
        let http = http.expect("HTTPRoute");
        assert_eq!(
            route_spec(&http)["rules"][0]["backendRefs"][0]["name"],
            serde_json::json!("prod-ingest-router"),
            "on a clean router render, desired_objects must switch the HTTPRoute \
             to the router Service -- this is the deliverable, not just \
             desired_gateway_routes's own internal branch"
        );
        assert_eq!(
            route_spec(&http)["rules"][0]["backendRefs"][0]["port"],
            serde_json::json!(ROUTER_HTTP_PORT)
        );
    }

    #[test]
    fn switching_backend_away_from_ravel_native_sweeps_router_objects() {
        // ADR-0080 decision 3, deliverable 6. When ravelNative is active the
        // render produces the router objects under one shared name; the
        // controller's apply-then-sweep (controller.rs) deletes every name in
        // `possible_router_object_names` the current render did NOT produce.
        // Prove both halves: the names produced under ravelNative, and that
        // switching back renders nothing under those very names (so the sweep
        // deletes them), rather than merely that the possible-names list grew.
        let sweep = possible_router_object_names("prod");
        assert_eq!(sweep, vec!["prod-ingest-router".to_string()]);

        // Active: every router object carries the swept name. Key source is
        // authorization-header (needs no resolver) since this test is about
        // object naming and the sweep, not the key source.
        let spec = ravel_native_spec(2, AffinityKeySource::AuthorizationHeader);
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        assert_eq!(
            objects
                .router_deployment
                .as_ref()
                .and_then(|d| d.metadata.name.as_deref()),
            Some("prod-ingest-router")
        );
        assert_eq!(
            objects
                .router_service
                .as_ref()
                .and_then(|s| s.metadata.name.as_deref()),
            Some("prod-ingest-router")
        );
        assert!(sweep.contains(&"prod-ingest-router".to_string()));

        // Switched back to ingressNginx: nothing rendered under that name, so
        // the sweep (which iterates `possible_router_object_names` and deletes
        // whatever the render omitted) removes every router-owned object. The
        // key source stays authorization-header, a legacy-valid source (a
        // canonicalTenant + ingressNginx combination is CEL-rejected and cannot
        // reach reconcile).
        let mut switched = spec.clone();
        if let Some(a) = switched.gateway.ingest_affinity.as_mut() {
            a.backend = AffinityBackend::IngressNginx;
            a.key.source = AffinityKeySource::AuthorizationHeader;
        }
        let objects = desired_objects(&switched, "prod", "default", &ctx()).expect("render");
        assert!(objects.router_deployment.is_none());
        assert!(objects.router_service.is_none());
        assert!(objects.router_service_account.is_none());
        assert!(objects.router_role.is_none());
        assert!(objects.router_role_binding.is_none());
        // The name the previous mode created is still in the sweep list, so the
        // controller deletes it on this pass.
        assert!(possible_router_object_names("prod").contains(&"prod-ingest-router".to_string()));
    }

    /// Every Deployment the reconciler can render for a fully-populated spec,
    /// paired with a label naming its container: the three tiers plus the
    /// ravel-native ingest router. The hardening sweeps assert over this exact
    /// set, so a newly added rendered container or pod cannot escape the
    /// SecurityContext unnoticed. `AuthorizationHeader` affinity is used because
    /// it needs no tenant-token resolver, so the router renders even with
    /// `tenant_tokens_secret_ref` cleared.
    fn every_rendered_deployment() -> Vec<(&'static str, Deployment)> {
        let mut spec = ravel_native_spec(3, AffinityKeySource::AuthorizationHeader);
        spec.maintain.enabled = true;
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        let deployments = vec![
            ("gateway", objects.gateway_deployment),
            ("query", objects.query_deployment),
            (
                "maintain",
                objects
                    .maintain_deployment
                    .expect("maintain.enabled renders a maintain Deployment"),
            ),
            (
                "ingest-router",
                objects
                    .router_deployment
                    .expect("ravelNative renders a router Deployment"),
            ),
        ];
        // Guard against a future render path silently dropping out of this
        // enumeration: the four names above are every container the operator
        // ships today.
        assert_eq!(
            deployments.len(),
            4,
            "expected exactly four rendered Deployments to sweep"
        );
        deployments
    }

    fn pod_spec_of(dep: &Deployment) -> &PodSpec {
        dep.spec
            .as_ref()
            .expect("deployment spec")
            .template
            .spec
            .as_ref()
            .expect("pod spec")
    }

    #[test]
    fn every_rendered_container_drops_all_capabilities_and_runs_non_root() {
        // Deliverable 1: the container-level SecurityContext is on EVERY
        // container the reconciler can render, not just the shared tier builder.
        // Render-level assertion: it checks the object the operator applies, not
        // a live pod (the k8s CI lane covers the runtime effect).
        for (tier, dep) in every_rendered_deployment() {
            for container in &pod_spec_of(&dep).containers {
                let sc = container
                    .security_context
                    .as_ref()
                    .unwrap_or_else(|| panic!("{tier}: container has no SecurityContext"));
                assert_eq!(
                    sc.run_as_non_root,
                    Some(true),
                    "{tier}: runAsNonRoot must be true"
                );
                assert_eq!(
                    sc.allow_privilege_escalation,
                    Some(false),
                    "{tier}: allowPrivilegeEscalation must be false"
                );
                assert_eq!(
                    sc.read_only_root_filesystem,
                    Some(true),
                    "{tier}: readOnlyRootFilesystem must be true"
                );
                let dropped = sc
                    .capabilities
                    .as_ref()
                    .and_then(|c| c.drop.as_ref())
                    .unwrap_or_else(|| panic!("{tier}: capabilities.drop unset"));
                assert_eq!(
                    dropped,
                    &vec!["ALL".to_string()],
                    "{tier}: every capability must be dropped"
                );
            }
        }
    }

    #[test]
    fn every_rendered_pod_spec_sets_a_non_root_security_context() {
        // Deliverable 2: the pod-level SecurityContext (runAsNonRoot + the
        // RuntimeDefault seccomp profile) is on EVERY rendered PodSpec.
        // Render-level assertion.
        for (tier, dep) in every_rendered_deployment() {
            let pod = pod_spec_of(&dep);
            let sc = pod
                .security_context
                .as_ref()
                .unwrap_or_else(|| panic!("{tier}: PodSpec has no securityContext"));
            assert_eq!(
                sc.run_as_non_root,
                Some(true),
                "{tier}: pod runAsNonRoot must be true"
            );
            let profile = sc
                .seccomp_profile
                .as_ref()
                .unwrap_or_else(|| panic!("{tier}: no seccompProfile"));
            assert_eq!(
                profile.type_, "RuntimeDefault",
                "{tier}: seccompProfile must be RuntimeDefault"
            );
        }
    }

    #[test]
    fn each_tier_renders_a_pod_disruption_budget_and_anti_affinity() {
        // Deliverable 3 and 4, render-level. The PDB count equals the number of
        // tiers the reconcile renders a Deployment for, and every tier PodSpec
        // carries preferred (soft) anti-affinity keyed on its own labels.
        //
        // With maintain enabled: three tiers, three PDBs. Disabling maintain
        // drops it to two, proving the count tracks the rendered tiers rather
        // than a constant.
        let mut spec = base_spec();
        spec.maintain.enabled = true;
        let objects = desired_objects(&spec, "prod", "default", &ctx()).expect("render");
        assert_eq!(
            objects.pod_disruption_budgets.len(),
            3,
            "one PDB per rendered tier (gateway, query, maintain)"
        );
        // Each PDB caps voluntary disruption at one pod and selects its tier's
        // own labels.
        for (pdb, component) in objects
            .pod_disruption_budgets
            .iter()
            .zip(["gateway", "query", "maintain"])
        {
            let pdb_spec = pdb.spec.as_ref().expect("pdb spec");
            assert_eq!(
                pdb_spec.max_unavailable,
                Some(IntOrString::Int(1)),
                "{component}: maxUnavailable must be 1"
            );
            assert_eq!(
                pdb.metadata.name.as_deref(),
                Some(child_name("prod", component).as_str()),
                "{component}: PDB name must match the tier"
            );
            assert_eq!(
                pdb_spec
                    .selector
                    .as_ref()
                    .and_then(|s| s.match_labels.clone()),
                Some(labels("prod", component)),
                "{component}: PDB selects its tier labels"
            );
        }

        // Preferred, not required: a required term would wedge scheduling on a
        // single-node cluster. Assert the preferred list is populated and the
        // required list is empty, on every tier.
        for (tier, dep) in [
            ("gateway", objects.gateway_deployment),
            ("query", objects.query_deployment),
            (
                "maintain",
                objects.maintain_deployment.expect("maintain enabled"),
            ),
        ] {
            let anti = pod_spec_of(&dep)
                .affinity
                .as_ref()
                .and_then(|a| a.pod_anti_affinity.as_ref())
                .unwrap_or_else(|| panic!("{tier}: no podAntiAffinity"));
            assert!(
                anti.required_during_scheduling_ignored_during_execution
                    .as_ref()
                    .map(|r| r.is_empty())
                    .unwrap_or(true),
                "{tier}: anti-affinity must be preferred, never required"
            );
            let preferred = anti
                .preferred_during_scheduling_ignored_during_execution
                .as_ref()
                .unwrap_or_else(|| panic!("{tier}: no preferred anti-affinity term"));
            assert_eq!(preferred.len(), 1, "{tier}: one preferred term");
            assert_eq!(
                preferred[0].pod_affinity_term.topology_key, "kubernetes.io/hostname",
                "{tier}: spread across nodes"
            );
            assert_eq!(
                preferred[0]
                    .pod_affinity_term
                    .label_selector
                    .as_ref()
                    .and_then(|s| s.match_labels.clone()),
                Some(labels("prod", tier)),
                "{tier}: term selects the tier's own pods"
            );
        }

        // Disabling maintain drops its PDB, proving the count tracks tiers.
        let mut disabled = base_spec();
        disabled.maintain.enabled = false;
        let objects = desired_objects(&disabled, "prod", "default", &ctx()).expect("render");
        assert_eq!(
            objects.pod_disruption_budgets.len(),
            2,
            "maintain disabled: only gateway and query PDBs"
        );
    }

    #[test]
    fn rbac_manifest_never_grants_secrets_cluster_wide() {
        // Issue #126 manifest lint. A dependency-free text scan of the shipped
        // rbac.yaml, run by the existing `cargo test -p ravel-operator` gate.
        // The operator must hold no standing cluster-wide Secret read: the
        // cluster-wide ClusterRole (ravel-operator, the one the
        // ClusterRoleBinding binds) grants no `secrets`, the secrets read lives
        // in a separate ravel-operator-secrets ClusterRole, and NO
        // ClusterRoleBinding may reference that secrets ClusterRole (a
        // ClusterRoleBinding would make it cluster-wide; only RoleBindings, which
        // are namespace-scoped, may bind it).
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/k8s/operator/rbac.yaml");
        let manifest = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

        // Split into YAML documents and classify each by its `kind:` line. A
        // ClusterRoleBinding also contains the text "ClusterRole", so match the
        // kind line exactly rather than with a substring. `has_name` matches a
        // metadata (or roleRef) name line exactly, so `ravel-operator` does not
        // also match `ravel-operator-secrets`.
        let docs: Vec<&str> = manifest.split("\n---").collect();
        let kind_is =
            |doc: &str, kind: &str| doc.lines().any(|l| l.trim() == format!("kind: {kind}"));
        let has_name =
            |doc: &str, name: &str| doc.lines().any(|l| l.trim() == format!("name: {name}"));

        // No ClusterRoleBinding may bind the secrets ClusterRole cluster-wide.
        for doc in &docs {
            if kind_is(doc, "ClusterRoleBinding") {
                assert!(
                    !doc.contains("ravel-operator-secrets"),
                    "the secrets read must never be bound cluster-wide by a \
                     ClusterRoleBinding; bind it per namespace with a RoleBinding"
                );
            }
        }

        // The cluster-wide operator ClusterRole grants no secrets.
        let main_cluster_role = docs
            .iter()
            .find(|d| kind_is(d, "ClusterRole") && has_name(d, "ravel-operator"))
            .expect("rbac.yaml defines the ravel-operator ClusterRole");
        assert!(
            !main_cluster_role.contains("secrets"),
            "the cluster-wide ravel-operator ClusterRole must not grant a secrets rule"
        );

        // The secrets read lives in its own ClusterRole, get only, bound per
        // namespace by a RoleBinding (ravel-system ships one).
        let secrets_cluster_role = docs
            .iter()
            .find(|d| kind_is(d, "ClusterRole") && has_name(d, "ravel-operator-secrets"))
            .expect("rbac.yaml defines the ravel-operator-secrets ClusterRole");
        assert!(
            secrets_cluster_role.contains("secrets")
                && secrets_cluster_role.contains("verbs: [\"get\"]"),
            "the ravel-operator-secrets ClusterRole grants secrets get only"
        );
        let secrets_binding = docs
            .iter()
            .find(|d| kind_is(d, "RoleBinding") && d.contains("ravel-operator-secrets"))
            .expect("a RoleBinding binds the secrets ClusterRole in a namespace");
        assert!(
            secrets_binding.contains("namespace: ravel-system"),
            "the shipped secrets RoleBinding binds ravel-system, the operator's own namespace"
        );
    }

    /// Does any `verbs:` list in the manifest grant `update`?
    ///
    /// Scans the comment-stripped `verbs:` lines and, when a `verbs:` line opens
    /// a block sequence, its items, for a bare `update` token. Flow style
    /// (`verbs: ["create", "update"]`) and block style (`- update`, quoted or
    /// not) both count; a comment or a blank line inside a block sequence does
    /// not end it. Nothing outside a verbs list is scanned, so a resource named
    /// `updates` or the word in a comment cannot fail the caller's assertion for
    /// the wrong reason.
    fn verbs_grant_update(manifest: &str) -> bool {
        let has_update = |text: &str| {
            text.split(|c: char| !c.is_ascii_alphanumeric())
                .any(|token| token == "update")
        };
        let mut in_verbs_block = false;
        for raw in manifest.lines() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if let Some(rest) = line.strip_prefix("verbs:") {
                if has_update(rest) {
                    return true;
                }
                // A `verbs:` key with nothing after it opens a block sequence
                // whose items are the verbs.
                in_verbs_block = rest.trim().is_empty();
                continue;
            }
            if in_verbs_block && !line.is_empty() {
                match line.strip_prefix("- ") {
                    Some(item) if has_update(item) => return true,
                    Some(_) => {}
                    // The next key ends the sequence.
                    None => in_verbs_block = false,
                }
            }
        }
        false
    }

    /// [`verbs_grant_update`] catches the `update` verb in both YAML spellings a
    /// rules list can use, and reports nothing for an `update` outside a verbs
    /// list. The block-style cases are the ones a quoted-substring check missed;
    /// the negative cases are why the scan is narrowed to verbs lines instead of
    /// running over every line in the file.
    #[test]
    fn the_update_verb_scan_covers_both_yaml_styles_and_nothing_else() {
        // Flow style, the shape rbac.yaml ships.
        assert!(verbs_grant_update("    verbs: [\"create\", \"update\"]\n"));
        // Block style, quoted and unquoted.
        assert!(verbs_grant_update(
            "    verbs:\n      - create\n      - update\n      - delete\n"
        ));
        assert!(verbs_grant_update("    verbs:\n      - \"update\"\n"));
        // A comment or a blank line inside the sequence does not end it.
        assert!(verbs_grant_update(
            "    verbs:\n      - create\n      # rationale\n\n      - update\n"
        ));

        // Narrowed: an `update` outside a verbs list is not a grant.
        assert!(!verbs_grant_update("    resources: [\"updates\"]\n"));
        assert!(!verbs_grant_update("    verbs: [\"get\"] # never update\n"));
        assert!(!verbs_grant_update("    # verbs: [\"update\"]\n"));
        assert!(!verbs_grant_update("    strategy: update\n"));
        // The next key ends a block sequence, so a later value is out of scope.
        assert!(!verbs_grant_update(
            "    verbs:\n      - get\n    resources: [\"update\"]\n"
        ));
    }

    /// Finding 1 verb sweep. Every operator write is server-side apply (a PATCH,
    /// with `create` for objects that do not yet exist) or a delete, never a
    /// PUT, so no rule grants the `update` verb. Each rule grants exactly the
    /// verbs the reconcile loop calls (or that RBAC escalation prevention forces
    /// for the router Role): the table below pins the exact verbs line for every
    /// rule in rbac.yaml, so any verb change to any rule (a reintroduced dead
    /// verb, a dropped read, a widened grant) fails the gate, not only the
    /// batch/jobs rule.
    #[test]
    fn rbac_grants_only_the_verbs_the_reconcile_loop_calls() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/k8s/operator/rbac.yaml");
        let manifest = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

        // No rule anywhere grants `update`. The scan covers the verbs lines only
        // (the same lines the count assertion below identifies) and their block
        // sequence items, so a reformat of any rule to block style (`- update`,
        // unquoted) cannot make it go quiet while an `update` anywhere else in
        // the file cannot fail it for the wrong reason.
        assert!(
            !verbs_grant_update(&manifest),
            "server-side apply is a PATCH, not a PUT: no rule may grant the update verb"
        );

        // Every rule's apiGroups/resources/verbs lines, in rbac.yaml's fixed
        // ordering: the `resources:` line uniquely identifies each rule, with its
        // `apiGroups:` line immediately before and its `verbs:` line immediately
        // after.
        let rules: &[(&str, &str, &str)] = &[
            (
                "- apiGroups: [\"apps\"]",
                "resources: [\"deployments\"]",
                "verbs: [\"create\", \"patch\", \"get\", \"list\", \"watch\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"\"]",
                "resources: [\"services\"]",
                "verbs: [\"create\", \"patch\", \"get\", \"list\", \"watch\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"networking.k8s.io\"]",
                "resources: [\"ingresses\"]",
                "verbs: [\"create\", \"patch\", \"list\", \"watch\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"gateway.networking.k8s.io\"]",
                "resources: [\"httproutes\", \"grpcroutes\"]",
                "verbs: [\"create\", \"patch\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"\"]",
                "resources: [\"serviceaccounts\"]",
                "verbs: [\"create\", \"patch\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"rbac.authorization.k8s.io\"]",
                "resources: [\"roles\", \"rolebindings\"]",
                "verbs: [\"create\", \"patch\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"discovery.k8s.io\"]",
                "resources: [\"endpointslices\"]",
                "verbs: [\"get\", \"list\", \"watch\"]",
            ),
            (
                "- apiGroups: [\"ravel.nofire.ai\"]",
                "resources: [\"ravelclusters\"]",
                "verbs: [\"list\", \"watch\"]",
            ),
            (
                "- apiGroups: [\"ravel.nofire.ai\"]",
                "resources: [\"ravelclusters/status\"]",
                "verbs: [\"patch\"]",
            ),
            (
                "- apiGroups: [\"policy\"]",
                "resources: [\"poddisruptionbudgets\"]",
                "verbs: [\"create\", \"patch\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"batch\"]",
                "resources: [\"jobs\"]",
                "verbs: [\"create\", \"patch\", \"get\", \"delete\"]",
            ),
            (
                "- apiGroups: [\"\"]",
                "resources: [\"secrets\"]",
                "verbs: [\"get\"]",
            ),
        ];

        let lines: Vec<&str> = manifest.lines().map(str::trim).collect();

        // Exhaustiveness: the table must pin every rule in the manifest. Count the
        // `verbs:` lines and require the table length to match, so a thirteenth
        // rule cannot be added without a matching table row.
        let verbs_lines = lines.iter().filter(|l| l.starts_with("verbs:")).count();
        assert_eq!(
            verbs_lines,
            rules.len(),
            "the table must pin every rule: rbac.yaml has {verbs_lines} verbs lines, \
             the table has {} rows",
            rules.len()
        );

        for rule in rules {
            let (apigroups_line, resources_line, expected_verbs) = *rule;
            // The resources line identifies the rule; assert it occurs exactly
            // once so a duplicated resources line with wider verbs cannot hide
            // behind the first match.
            let occurrences = lines.iter().filter(|l| **l == resources_line).count();
            assert_eq!(
                occurrences, 1,
                "the {resources_line} rule must appear exactly once"
            );
            let idx = lines
                .iter()
                .position(|l| *l == resources_line)
                .unwrap_or_else(|| panic!("rbac.yaml defines a rule for {resources_line}"));
            assert_eq!(
                idx.checked_sub(1).and_then(|i| lines.get(i)).copied(),
                Some(apigroups_line),
                "the {resources_line} rule must be under {apigroups_line}"
            );
            assert_eq!(
                lines.get(idx + 1).copied(),
                Some(expected_verbs),
                "the {resources_line} rule must grant exactly {expected_verbs}"
            );
        }
    }

    /// A job condition of the given type/status, the shape
    /// [`qualify_job_phase`] reads.
    fn job_with_condition(type_: &str, status: &str, message: Option<&str>) -> Job {
        use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};
        Job {
            status: Some(JobStatus {
                conditions: Some(vec![JobCondition {
                    type_: type_.to_string(),
                    status: status.to_string(),
                    message: message.map(str::to_string),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A Job reporting `Failed=True` with the given `reason` and `message`, the
    /// shape the Kubernetes Job controller sets when it fails a Job (a
    /// deadline-exceeded Job carries reason `DeadlineExceeded`).
    fn job_with_failed_reason(reason: &str, message: Option<&str>) -> Job {
        use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};
        Job {
            status: Some(JobStatus {
                conditions: Some(vec![JobCondition {
                    type_: "Failed".to_string(),
                    status: "True".to_string(),
                    reason: Some(reason.to_string()),
                    message: message.map(str::to_string),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// The input hash is stable for one spec and changes for each of the five
    /// inputs it covers, and only those. Pins the exact set of fields that
    /// re-trigger qualification (issue #36): a change to any of them is a
    /// different store to prove, an unrelated change is not.
    #[test]
    fn qualify_input_hash_tracks_exactly_the_qualified_inputs() {
        let spec = base_spec();
        let base = qualify_job_input_hash(&spec, Some("rv-1"));
        assert_eq!(
            base,
            qualify_job_input_hash(&spec, Some("rv-1")),
            "the hash is deterministic for one spec and resourceVersion"
        );

        let mut bucket = spec.clone();
        bucket.storage.s3.bucket = "other-bucket".to_string();
        let mut region = spec.clone();
        region.storage.s3.region = "us-east-1".to_string();
        let mut endpoint = spec.clone();
        endpoint.storage.s3.endpoint = Some("http://other:9000".to_string());
        let mut image = spec.clone();
        image.image = "registry.example/ravel:v2".to_string();
        let mut creds = spec.clone();
        creds.storage.s3.credentials_secret_ref.name = "other-s3".to_string();
        for (label, changed) in [
            ("bucket", &bucket),
            ("region", &region),
            ("endpoint", &endpoint),
            ("image", &image),
            ("credentials secret", &creds),
        ] {
            assert_ne!(
                base,
                qualify_job_input_hash(changed, Some("rv-1")),
                "a change to {label} re-triggers qualification"
            );
        }

        // A fixed-name credential rotation (same Secret name, new
        // resourceVersion) is a different credential and must re-trigger
        // qualification, even though every spec field is unchanged.
        assert_ne!(
            base,
            qualify_job_input_hash(&spec, Some("rv-2")),
            "a new credentials resourceVersion (rotation in place) re-qualifies"
        );

        // A field qualification does not depend on leaves the hash unchanged, so
        // an unrelated spec edit does not re-run qualification.
        let mut replicas = spec.clone();
        replicas.gateway.replicas = spec.gateway.replicas + 5;
        assert_eq!(
            base,
            qualify_job_input_hash(&replicas, Some("rv-1")),
            "an unrelated spec edit (replica count) does not re-qualify"
        );

        // Endpoint presence is significant (finding 3): endpoint: null and
        // endpoint: "" emit a different qualify Job and run the servers against a
        // different store (the flag/env is omitted only for None), so the two must
        // hash differently. as_deref().unwrap_or("") fed the hasher "" for both.
        let mut endpoint_none = spec.clone();
        endpoint_none.storage.s3.endpoint = None;
        let mut endpoint_empty = spec.clone();
        endpoint_empty.storage.s3.endpoint = Some(String::new());
        assert_ne!(
            qualify_job_input_hash(&endpoint_none, Some("rv-1")),
            qualify_job_input_hash(&endpoint_empty, Some("rv-1")),
            "endpoint: null and endpoint: \"\" are different stores and must hash differently"
        );

        // Credentials resourceVersion presence is significant the same way
        // (finding 3): an unresolved Secret (None) and a resolved empty
        // resourceVersion (Some("")) are different states, so they must hash
        // differently. unwrap_or("") fed the hasher "" for both.
        assert_ne!(
            qualify_job_input_hash(&spec, None),
            qualify_job_input_hash(&spec, Some("")),
            "credentials resourceVersion None and Some(\"\") are different states and \
             must hash differently"
        );
    }

    /// The rendered qualify Job: name, image, command/args, credentials, the
    /// spec-hash annotation, and the one-shot execution shape. Exact values, so
    /// the Job the operator applies is pinned (issue #36).
    #[test]
    fn qualify_job_renders_a_one_shot_store_qualify() {
        let spec = base_spec();
        let job = desired_qualify_job(&spec, "prod", Some("rv-1"));

        assert_eq!(job.metadata.name.as_deref(), Some("prod-qualify"));
        assert_eq!(
            job.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(QUALIFY_SPEC_HASH_ANNOTATION))
                .map(String::as_str),
            Some(qualify_job_input_hash(&spec, Some("rv-1")).as_str()),
            "the annotation records the input hash so a change re-runs the Job"
        );

        let job_spec = job.spec.as_ref().expect("qualify Job has a spec");
        assert_eq!(job_spec.backoff_limit, Some(QUALIFY_JOB_BACKOFF_LIMIT));
        assert_eq!(
            job_spec.ttl_seconds_after_finished,
            Some(QUALIFY_JOB_TTL_SECONDS)
        );
        let pod = job_spec
            .template
            .spec
            .as_ref()
            .expect("qualify Job has a pod spec");
        assert_eq!(pod.restart_policy.as_deref(), Some("Never"));
        assert_eq!(pod.containers.len(), 1, "one qualify container");
        let container = &pod.containers[0];
        assert_eq!(
            container.image.as_deref(),
            Some("registry.example/ravel:v1")
        );
        assert_eq!(
            container.command.as_deref(),
            Some(["/usr/local/bin/ravel-cli".to_string()].as_slice())
        );
        assert_eq!(
            container.args.as_deref(),
            Some(
                [
                    "--store".to_string(),
                    "s3".to_string(),
                    "store".to_string(),
                    "qualify".to_string(),
                ]
                .as_slice()
            )
        );
        let env = container.env.as_ref().expect("qualify container has env");
        let env_value = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .and_then(|e| e.value.clone())
        };
        assert_eq!(env_value("RAVEL_S3_BUCKET").as_deref(), Some("ravel-data"));
        assert_eq!(env_value("RAVEL_S3_REGION").as_deref(), Some("eu-west-1"));
        assert_eq!(
            env_value("RAVEL_S3_ENDPOINT").as_deref(),
            Some("http://minio:9000")
        );
        // Credentials come from the shared Secret via secretKeyRef, never a
        // literal value in the pod spec.
        let access = env
            .iter()
            .find(|e| e.name == "RAVEL_S3_ACCESS_KEY")
            .expect("access key env present");
        assert!(access.value.is_none(), "the access key is never a literal");
        assert_eq!(
            access
                .value_from
                .as_ref()
                .and_then(|s| s.secret_key_ref.as_ref())
                .map(|r| (r.name.as_str(), r.key.as_str())),
            Some(("ravel-s3", S3_ACCESS_KEY_ID_KEY)),
        );
    }

    /// `qualify_job_phase` reads the Job's terminal condition: `Complete=True`
    /// is success, `Failed=True` carries its message, nothing terminal is still
    /// running.
    #[test]
    fn qualify_job_phase_reads_the_terminal_condition() {
        assert_eq!(
            qualify_job_phase(&Job::default()),
            QualifyJobPhase::Running,
            "a Job with no status is still running"
        );
        assert_eq!(
            qualify_job_phase(&job_with_condition("Complete", "True", None)),
            QualifyJobPhase::Succeeded
        );
        assert_eq!(
            qualify_job_phase(&job_with_condition(
                "Failed",
                "True",
                Some("backend rejected CAS")
            )),
            QualifyJobPhase::Failed("backend rejected CAS".to_string()),
            "the failure carries the Job's own terminal message"
        );
        // A condition that is not True does not count as terminal.
        assert_eq!(
            qualify_job_phase(&job_with_condition("Failed", "False", None)),
            QualifyJobPhase::Running
        );
    }

    /// The rendered qualify Job sets `activeDeadlineSeconds` to the chosen value,
    /// so a hung attempt (an endpoint that accepts the connection and never
    /// answers) becomes a `Failed` Job rather than running forever. `backoffLimit`
    /// bounds only failed attempts, not one attempt that never terminates.
    #[test]
    fn qualify_job_bounds_a_hung_attempt() {
        let spec = base_spec();
        let job = desired_qualify_job(&spec, "prod", None);
        let job_spec = job.spec.as_ref().expect("qualify Job has a spec");
        assert_eq!(
            job_spec.active_deadline_seconds,
            Some(QUALIFY_JOB_ACTIVE_DEADLINE_SECONDS),
            "the qualify Job bounds its total active time with activeDeadlineSeconds"
        );
        assert_eq!(
            job_spec.backoff_limit,
            Some(QUALIFY_JOB_BACKOFF_LIMIT),
            "the qualify Job caps its retries with backoffLimit"
        );
        // activeDeadlineSeconds is Job-wide (summed across every retry) and takes
        // precedence over backoffLimit, so the deadline must fit the intended
        // attempts end to end: 700 s per attempt (560 s of ops + ~140 s pod
        // scheduling/pull) * (backoffLimit + 1) attempts.
        assert_eq!(
            QUALIFY_JOB_BACKOFF_LIMIT, 1,
            "one retry (two attempts total)"
        );
        assert_eq!(
            QUALIFY_JOB_ACTIVE_DEADLINE_SECONDS, 1400,
            "the Job-wide deadline is 1400 s = 700 s per attempt * 2 attempts, so a \
             slow-but-healthy first attempt plus one full retry both fit before it fires"
        );
        assert_eq!(
            QUALIFY_JOB_ACTIVE_DEADLINE_SECONDS,
            700 * i64::from(QUALIFY_JOB_BACKOFF_LIMIT + 1),
            "the deadline and the backoff limit are sized together"
        );
    }

    /// A Job the Kubernetes Job controller failed for exceeding its
    /// `activeDeadlineSeconds` reports `Failed=True` with reason
    /// `DeadlineExceeded`. That reads as [`QualificationDecision::Failed`] (which
    /// the controller renders as `StoreQualified=False`), and its message names
    /// the reason exactly so an operator sees why qualification did not finish.
    #[test]
    fn a_deadline_exceeded_job_reads_as_failed_with_its_reason() {
        let job = job_with_failed_reason(
            "DeadlineExceeded",
            Some("Job was active longer than specified deadline"),
        );
        let phase = qualify_job_phase(&job);
        let message = match &phase {
            QualifyJobPhase::Failed(message) => message.clone(),
            other => panic!("a Failed=True Job reads as Failed, got {other:?}"),
        };
        assert!(
            message.contains("DeadlineExceeded"),
            "the failure message names the DeadlineExceeded reason exactly: {message:?}"
        );

        // The gate reports a Present Job for the current inputs in the Failed
        // phase as Failed, carrying that message; the controller turns Failed
        // into StoreQualified=False with STORE_QUALIFIED_FAILED_REASON.
        let decision = qualification_decision(
            "h",
            None,
            &QualifyJobObservation::Present {
                spec_hash: Some("h".to_string()),
                phase,
            },
        );
        assert_eq!(decision, QualificationDecision::Failed(message));
    }

    /// The `activeDeadlineSeconds` and `backoffLimit` are tuning knobs, not
    /// store-identity inputs: [`qualify_job_input_hash`] covers exactly the
    /// bucket, region, endpoint, image, credentials Secret name, and credentials
    /// `resourceVersion`, and never either knob, so tuning them leaves the hash
    /// equal and does not re-run a qualification that already passed.
    #[test]
    fn the_deadline_is_not_part_of_the_qualified_input_hash() {
        let spec = base_spec();

        // Recompute the hash over exactly the six qualified inputs, deliberately
        // excluding both knobs, through the same `blake3_hex` composition the
        // production hasher uses. If it folded either knob in, this reference
        // would diverge and the assertion would fail.
        let endpoint = match spec.storage.s3.endpoint.as_deref() {
            Some(value) => format!("\u{1}{value}"),
            None => "\u{0}".to_string(),
        };
        let credentials_rv = format!("\u{1}{}", "rv-1");
        let expected = blake3_hex(&[
            spec.storage.s3.bucket.as_str(),
            spec.storage.s3.region.as_str(),
            endpoint.as_str(),
            spec.image.as_str(),
            spec.storage.s3.credentials_secret_ref.name.as_str(),
            credentials_rv.as_str(),
        ]);

        assert_eq!(
            qualify_job_input_hash(&spec, Some("rv-1")),
            expected,
            "the qualified-input hash covers exactly the six store-identity inputs, never the \
             deadline or the backoff limit"
        );
    }

    /// Golden value for [`blake3_hex`] (finding 3): a fixed input set hashes to
    /// a fixed literal. This pins the algorithm (blake3), the `0xff` field
    /// separator, and the lowercase-hex rendering by construction, so a future
    /// change to any of them fails here instead of silently re-running
    /// qualification and rolling every tier Deployment on a toolchain upgrade.
    #[test]
    fn blake3_hex_golden_is_stable_by_construction() {
        assert_eq!(
            blake3_hex(&["field-a", "field-b", "field-c"]),
            "cef8162179abe2fd9467777a9e0957f18af3abff59960b15b0210ee312836274",
        );
        // The `0xff` separator makes the field split significant: no other
        // grouping of the same bytes collides.
        assert_ne!(
            blake3_hex(&["field-a", "field-b", "field-c"]),
            blake3_hex(&["field-afield-b", "field-c"]),
        );
    }

    /// Golden value for [`secrets_checksum`] (finding 3): a fixed set of
    /// resourceVersions hashes to a fixed literal, so a toolchain upgrade that
    /// changed the algorithm would fail here rather than roll every tier's pods.
    #[test]
    fn secrets_checksum_golden_is_stable_by_construction() {
        // This literal pins the all-`Some` four-field case: #1487 added the
        // audit-token-key resourceVersion as a 4th field, folded in only
        // when it is `Some`. A `None` audit-token-key composes exactly the
        // pre-#1487 three fields instead (see
        // `gateway_and_maintain_checksums_are_unchanged_when_no_audit_key_is_set`),
        // so this literal is unaffected by that case and did not need to
        // change from what #1487 originally pinned here.
        assert_eq!(
            secrets_checksum(Some("100"), Some("200"), Some("300"), Some("400")),
            "35396940142d0f2998ba074acda24baf7f660d6e4b300d74f4e828d4707f3168",
        );
    }

    /// Golden value for [`qualify_job_input_hash`] (finding 3): the six
    /// store-identity inputs of [`base_spec`] plus a fixed credentials
    /// `resourceVersion` hash to a fixed literal, stable by construction across
    /// Rust releases. The literal changed when the endpoint slot gained a
    /// presence byte (0x01 before a Some value) so endpoint: null and
    /// endpoint: "" no longer collide, and again when the credentials
    /// `resourceVersion` slot gained the same presence byte so an unresolved
    /// Secret (None) and a resolved empty version (Some("")) no longer collide.
    #[test]
    fn qualify_job_input_hash_golden_is_stable_by_construction() {
        assert_eq!(
            qualify_job_input_hash(&base_spec(), Some("rv-golden")),
            "9dbbf674caab21157d312101c70e8d6947f8d86b14d075910a1e02779d7610fd",
        );
    }

    /// The qualify retry backoff is a capped exponential for the first
    /// [`QUALIFY_RETRY_TERMINAL_THRESHOLD`] - 1 consecutive failures and the
    /// terminal cooldown at or past the threshold (issue #36, finding 3). The
    /// exact per-failure sequence is asserted, not a monotonicity band.
    #[test]
    fn qualify_retry_backoff_sequence_is_exact() {
        let sequence: Vec<i64> = (1..=7).map(qualify_retry_backoff_seconds).collect();
        assert_eq!(sequence, vec![30, 60, 120, 240, 480, 3600, 3600]);
        // The ceiling caps the exponential region: the fifth failure is 480, the
        // sixth crosses into the cooldown, never a doubled 960.
        assert_eq!(
            qualify_retry_backoff_seconds(5),
            QUALIFY_RETRY_CEILING_SECONDS
        );
        assert_eq!(
            qualify_retry_backoff_seconds(QUALIFY_RETRY_TERMINAL_THRESHOLD),
            QUALIFY_RETRY_COOLDOWN_SECONDS
        );
    }

    /// A newly-observed failure (no backoff scheduled) is counted exactly once,
    /// schedules the next retry at `now + backoff(count)`, keeps the Failed Job in
    /// place (no Job mutation), and requeues at the backoff. A second pass still
    /// inside the window does not re-count.
    #[test]
    fn plan_first_failure_counts_once_and_schedules_backoff() {
        let msg = "backend rejected CAS".to_string();
        let first = plan_qualify_gate(
            &QualificationDecision::Failed(msg.clone()),
            "h",
            Some("h"),
            0,
            None,
            1_000,
            10,
        );
        assert_eq!(first.action, QualifyJobAction::None);
        assert_eq!(first.reason, QualifyStoreReason::Failed);
        assert_eq!(first.failure_count, Some(1));
        assert_eq!(first.next_retry_unix, Some(1_030));
        assert_eq!(first.requeue_seconds, 30);
        assert_eq!(first.job_message.as_deref(), Some("backend rejected CAS"));

        // Still holding at count 1: no re-count, requeue shrinks to time remaining.
        let holding = plan_qualify_gate(
            &QualificationDecision::Failed(msg),
            "h",
            Some("h"),
            1,
            Some(1_030),
            1_020,
            10,
        );
        assert_eq!(holding.action, QualifyJobAction::None);
        assert_eq!(holding.failure_count, Some(1));
        assert_eq!(holding.next_retry_unix, Some(1_030));
        assert_eq!(holding.requeue_seconds, 10);
    }

    /// Once the backoff elapses, the Failed Job is deleted (foreground) so a later
    /// pass sees it Absent and recreates it; `next_retry_unix` is NOT cleared on
    /// the delete, so a Job lingering under foreground deletion is not counted a
    /// second time. The Absent recreation then creates the Job and clears
    /// `next_retry_unix`, keeping the count.
    #[test]
    fn plan_recreates_after_backoff_without_double_counting() {
        let msg = "still failing".to_string();
        // Backoff elapsed with the Failed Job still present: delete it, keep the
        // retry state so the recreation is not re-counted.
        let deleting = plan_qualify_gate(
            &QualificationDecision::Failed(msg),
            "h",
            Some("h"),
            2,
            Some(1_030),
            1_030,
            10,
        );
        assert_eq!(deleting.action, QualifyJobAction::DeleteStale);
        assert_eq!(deleting.failure_count, Some(2));
        assert_eq!(deleting.next_retry_unix, Some(1_030));

        // Next pass, Job Absent, backoff elapsed: create and clear next_retry so the
        // new attempt's failure counts afresh; count is kept, reason stays Failed.
        let creating = plan_qualify_gate(
            &QualificationDecision::Qualify { recreate: false },
            "h",
            Some("h"),
            2,
            Some(1_030),
            1_030,
            10,
        );
        assert_eq!(creating.action, QualifyJobAction::Create);
        assert_eq!(creating.reason, QualifyStoreReason::Failed);
        assert_eq!(creating.failure_count, Some(2));
        assert_eq!(creating.next_retry_unix, None);
        assert_eq!(creating.requeue_seconds, 10);
    }

    /// At the terminal threshold the gate holds in the cooldown: it creates NO Job
    /// before the cooldown expires and DOES recreate one at (and after) expiry. The
    /// hold is expressed on both the Failed-present and Absent arms.
    #[test]
    fn plan_terminal_hold_blocks_recreation_until_cooldown_expiry() {
        let due = 1_000 + QUALIFY_RETRY_COOLDOWN_SECONDS;

        // Failed Job present, before expiry: no mutation.
        let held_present = plan_qualify_gate(
            &QualificationDecision::Failed("terminal".to_string()),
            "h",
            Some("h"),
            QUALIFY_RETRY_TERMINAL_THRESHOLD,
            Some(due),
            1_000,
            10,
        );
        assert_eq!(held_present.action, QualifyJobAction::None);

        // Job Absent, before expiry: still no Create.
        let held_absent = plan_qualify_gate(
            &QualificationDecision::Qualify { recreate: false },
            "h",
            Some("h"),
            QUALIFY_RETRY_TERMINAL_THRESHOLD,
            Some(due),
            due - 1,
            10,
        );
        assert_eq!(held_absent.action, QualifyJobAction::None);
        assert_eq!(held_absent.reason, QualifyStoreReason::Failed);

        // Job Absent, at expiry: recreate.
        let released = plan_qualify_gate(
            &QualificationDecision::Qualify { recreate: false },
            "h",
            Some("h"),
            QUALIFY_RETRY_TERMINAL_THRESHOLD,
            Some(due),
            due,
            10,
        );
        assert_eq!(released.action, QualifyJobAction::Create);
    }

    /// An input change during any hold (a `recreate: true` decision, meaning a Job
    /// for stale inputs is present) resets the retry budget and deletes the stale
    /// Job at once: `failure_count` and `next_retry_unix` are cleared, so the next
    /// pass creates a fresh Job immediately rather than waiting out the cooldown.
    #[test]
    fn plan_input_change_resets_count_and_recreates_at_once() {
        let due = 1_000 + QUALIFY_RETRY_COOLDOWN_SECONDS;
        let reset = plan_qualify_gate(
            &QualificationDecision::Qualify { recreate: true },
            "h",
            Some("h"),
            QUALIFY_RETRY_TERMINAL_THRESHOLD,
            Some(due),
            1_000,
            10,
        );
        assert_eq!(reset.action, QualifyJobAction::DeleteStale);
        assert_eq!(reset.reason, QualifyStoreReason::Pending);
        assert_eq!(reset.failure_count, None);
        assert_eq!(reset.next_retry_unix, None);

        // Next pass sees the fresh inputs' Job Absent with a zero count: create now.
        let fresh = plan_qualify_gate(
            &QualificationDecision::Qualify { recreate: false },
            "h",
            Some("h"),
            0,
            None,
            1_000,
            10,
        );
        assert_eq!(fresh.action, QualifyJobAction::Create);
        assert_eq!(fresh.reason, QualifyStoreReason::Pending);
        assert_eq!(fresh.failure_count, None);
        assert_eq!(fresh.requeue_seconds, 10);
    }

    /// An input change observed AFTER the Failed Job's TTL collected it (the Job is
    /// Absent, so the decision is `Qualify { recreate: false }` for a changed and an
    /// unchanged hash alike) still resets and qualifies at once (issue #36): the
    /// desired hash no longer matches the hash the failures were recorded against,
    /// so the terminal cooldown is dropped, a Job is created this pass, and the
    /// persisted retry state is cleared. Without the recorded-hash comparison this
    /// pass would hold out the remaining cooldown and inherit the old count.
    #[test]
    fn plan_absent_changed_hash_qualifies_at_once() {
        let due = 1_000 + QUALIFY_RETRY_COOLDOWN_SECONDS;
        let changed = plan_qualify_gate(
            &QualificationDecision::Qualify { recreate: false },
            "new",
            Some("old"),
            QUALIFY_RETRY_TERMINAL_THRESHOLD,
            Some(due),
            1_000,
            10,
        );
        assert_eq!(changed.action, QualifyJobAction::Create);
        assert_eq!(changed.reason, QualifyStoreReason::Pending);
        assert_eq!(changed.failure_count, None);
        assert_eq!(changed.next_retry_unix, None);
        assert_eq!(changed.retry_hash, None);
        assert_eq!(changed.requeue_seconds, 10);
    }

    /// The converse of the reset (issue #36): with the Job Absent, the desired hash
    /// still equal to the recorded retry hash, and the cooldown not yet expired, the
    /// plan holds exactly as before. The count is kept, no Job is created, and the
    /// requeue is the exact time remaining. This is the case the recorded-hash
    /// comparison must NOT flip.
    #[test]
    fn plan_absent_unchanged_hash_holds_in_cooldown() {
        let due = 1_000 + QUALIFY_RETRY_COOLDOWN_SECONDS;
        let held = plan_qualify_gate(
            &QualificationDecision::Qualify { recreate: false },
            "same",
            Some("same"),
            QUALIFY_RETRY_TERMINAL_THRESHOLD,
            Some(due),
            1_000,
            10,
        );
        assert_eq!(held.action, QualifyJobAction::None);
        assert_eq!(held.reason, QualifyStoreReason::Failed);
        assert_eq!(held.failure_count, Some(QUALIFY_RETRY_TERMINAL_THRESHOLD));
        assert_eq!(held.next_retry_unix, Some(due));
        assert_eq!(held.retry_hash.as_deref(), Some("same"));
        assert_eq!(held.requeue_seconds, due - 1_000);
    }

    /// A newly-observed failure records the desired hash as the retry key alongside
    /// the count (issue #36), so a later pass can compare against it once the Job is
    /// gone. The key is the desired hash, not the (absent) prior one.
    #[test]
    fn plan_first_failure_records_retry_hash() {
        let first = plan_qualify_gate(
            &QualificationDecision::Failed("boom".to_string()),
            "desired",
            None,
            0,
            None,
            1_000,
            10,
        );
        assert_eq!(first.failure_count, Some(1));
        assert_eq!(first.retry_hash.as_deref(), Some("desired"));
    }

    /// The persisted retry bookkeeping (failure count, next-retry time) is NOT part
    /// of the qualified-input hash (issue #36, finding 3): the hash is recomputed
    /// here over exactly the store-identity inputs, so folding either retry field
    /// into it would diverge from this reconstruction and fail the test. Keeping
    /// them out is what lets a retry converge instead of reading every backoff pass
    /// as a new input.
    #[test]
    fn qualify_input_hash_excludes_retry_state() {
        let spec = base_spec();
        let endpoint = match spec.storage.s3.endpoint.as_deref() {
            Some(value) => format!("\u{1}{value}"),
            None => "\u{0}".to_string(),
        };
        let credentials_rv = format!("\u{1}{}", "rv");
        let expected = blake3_hex(&[
            spec.storage.s3.bucket.as_str(),
            spec.storage.s3.region.as_str(),
            endpoint.as_str(),
            spec.image.as_str(),
            spec.storage.s3.credentials_secret_ref.name.as_str(),
            credentials_rv.as_str(),
        ]);
        assert_eq!(qualify_job_input_hash(&spec, Some("rv")), expected);
    }

    /// A cluster already qualified for the current inputs proceeds without
    /// re-running qualification, whatever the live Job says -- even absent
    /// (TTL-garbage-collected). This is what keeps qualification from re-running
    /// on a schedule.
    #[test]
    fn qualified_inputs_proceed_without_rerunning() {
        let hash = "abc123".to_string();
        for observation in [
            QualifyJobObservation::Absent,
            QualifyJobObservation::Present {
                spec_hash: Some(hash.clone()),
                phase: QualifyJobPhase::Succeeded,
            },
        ] {
            assert_eq!(
                qualification_decision(&hash, Some(&hash), &observation),
                QualificationDecision::Proceed,
                "recorded qualified inputs short-circuit to Proceed"
            );
        }
    }

    /// A fresh cluster (nothing qualified, no Job) creates the qualify Job
    /// without recreating anything, and holds serving until it completes.
    #[test]
    fn fresh_cluster_creates_the_qualify_job() {
        assert_eq!(
            qualification_decision("h", None, &QualifyJobObservation::Absent),
            QualificationDecision::Qualify { recreate: false },
        );
    }

    /// A running Job for the current inputs holds serving; a completed one lets
    /// it proceed; a failed one surfaces the Job's message and stays held.
    #[test]
    fn matching_job_reports_its_progress() {
        let present = |phase| QualifyJobObservation::Present {
            spec_hash: Some("h".to_string()),
            phase,
        };
        assert_eq!(
            qualification_decision("h", None, &present(QualifyJobPhase::Running)),
            QualificationDecision::Waiting,
        );
        assert_eq!(
            qualification_decision("h", None, &present(QualifyJobPhase::Succeeded)),
            QualificationDecision::Proceed,
        );
        assert_eq!(
            qualification_decision(
                "h",
                None,
                &present(QualifyJobPhase::Failed("boom".to_string()))
            ),
            QualificationDecision::Failed("boom".to_string()),
        );
    }

    /// When the inputs change on an already-qualified cluster, a Job for the old
    /// inputs (or one missing the annotation) is recreated, not read as the
    /// current one. The old qualified hash does not short-circuit because it no
    /// longer equals the desired hash.
    #[test]
    fn changed_inputs_recreate_a_stale_job() {
        let stale = QualifyJobObservation::Present {
            spec_hash: Some("old".to_string()),
            phase: QualifyJobPhase::Succeeded,
        };
        assert_eq!(
            qualification_decision("new", Some("old"), &stale),
            QualificationDecision::Qualify { recreate: true },
        );
        // A Job with no recorded hash is also stale against any desired hash.
        let unannotated = QualifyJobObservation::Present {
            spec_hash: None,
            phase: QualifyJobPhase::Succeeded,
        };
        assert_eq!(
            qualification_decision("new", Some("old"), &unannotated),
            QualificationDecision::Qualify { recreate: true },
        );
    }

    /// Fixed-name credential rotation (issue #36): the credentials Secret keeps
    /// its name but its content is rotated in place, so its `resourceVersion`
    /// bumps. The qualified-input hash changes, so an already-qualified cluster
    /// whose last Job succeeded is driven back to `Qualify { recreate: true }`
    /// (the controller renders `StoreQualified=Pending` and recreates the Job).
    /// The converse: an unchanged `resourceVersion` yields the same hash and
    /// `Proceed`, so a steady-state reconcile never re-runs qualification.
    #[test]
    fn fixed_name_credential_rotation_re_qualifies() {
        let spec = base_spec();
        let before = qualify_job_input_hash(&spec, Some("rv-1"));
        let after = qualify_job_input_hash(&spec, Some("rv-2"));
        assert_ne!(
            before, after,
            "a rotated credentials Secret (new resourceVersion) is a new qualified input"
        );

        // Rotation: the durable qualified hash is the pre-rotation one, the
        // desired hash is the post-rotation one, and the live Job (if not yet
        // GC'd) still carries the pre-rotation hash and succeeded.
        let rotated = qualification_decision(
            &after,
            Some(&before),
            &QualifyJobObservation::Present {
                spec_hash: Some(before.clone()),
                phase: QualifyJobPhase::Succeeded,
            },
        );
        assert_eq!(
            rotated,
            QualificationDecision::Qualify { recreate: true },
            "a fixed-name rotation must recreate the Job and flip StoreQualified to Pending"
        );
        // Even after the prior Job's TTL GC (Absent) a rotation re-runs.
        assert_eq!(
            qualification_decision(&after, Some(&before), &QualifyJobObservation::Absent),
            QualificationDecision::Qualify { recreate: false },
            "a rotation after TTL GC creates a fresh Job"
        );

        // Converse: same resourceVersion, same hash, no re-run.
        assert_eq!(
            qualification_decision(
                &before,
                Some(&before),
                &QualifyJobObservation::Present {
                    spec_hash: Some(before.clone()),
                    phase: QualifyJobPhase::Succeeded,
                },
            ),
            QualificationDecision::Proceed,
            "an unchanged resourceVersion must not re-run qualification"
        );
    }
}
