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

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction, KeyToPath, PodSpec,
    PodTemplateSpec, Probe, ResourceRequirements, SecretKeySelector, SecretVolumeSource, Service,
    ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::crd::{LocalSecretRef, RavelClusterSpec, ResourceRequirementsSpec};

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
    /// key (review finding 8, #897) must roll when it rotates, the same as a
    /// credential or tenant-token rotation does; folded into
    /// [`secrets_checksum`] alongside the other two.
    pub deployment_key_resource_version: Option<String>,
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
/// configured -- review finding 8, #897).
///
/// Built from their `resourceVersion`s, which change exactly when a Secret's
/// content changes and are stable otherwise, so this value is stable across
/// reconciles that see the same Secrets (no pod churn) and changes the moment a
/// token is rotated, the tier's credential is rewritten, or the deployment key
/// is rotated. The hash is `DefaultHasher` (SipHash with fixed keys),
/// deterministic across processes, so an operator restart does not roll pods.
/// It is not a security boundary, only a signal. Stamped onto the tier's pod
/// template as [`SECRETS_CHECKSUM_ANNOTATION`]; when tiers resolve to
/// different credential Secrets, a change to one rolls only the tier(s) that
/// consume it.
pub fn secrets_checksum(
    token_rv: Option<&str>,
    credentials_rv: Option<&str>,
    deployment_key_rv: Option<&str>,
) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token_rv.unwrap_or("").hash(&mut hasher);
    credentials_rv.unwrap_or("").hash(&mut hasher);
    deployment_key_rv.unwrap_or("").hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// The [`SECRETS_CHECKSUM_ANNOTATION`] value for a tier, resolving which
/// credential Secret the tier consumes and hashing its `resourceVersion`
/// together with the token Secret's and the deployment-key Secret's.
fn tier_secrets_checksum(
    spec: &RavelClusterSpec,
    ctx: &RenderCtx,
    tier_override: Option<&LocalSecretRef>,
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
/// reconciliation (issue #897) has the same key available. Otherwise it
/// renders `--tenant-hash-unkeyed`, keeping every existing and new
/// keyless `RavelCluster` on today's v1-unkeyed derivation. This is an
/// additive opt-in, not a migration of existing unkeyed clusters (EM-T10
/// #773 owns that story).
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
/// HTTP port (issue #246's routes; ADR-0034 decision 4). Returned as
/// `(liveness, readiness)`.
fn probes() -> (Probe, Probe) {
    let http_get = |path: &str| HTTPGetAction {
        path: Some(path.to_string()),
        port: IntOrString::Int(HTTP_PORT),
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
        &tier_secrets_checksum(spec, ctx, tier_override),
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
        &tier_secrets_checksum(spec, ctx, tier_override),
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
) -> Option<Deployment> {
    if !spec.maintain.enabled {
        return None;
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

    Some(deployment(
        spec,
        instance,
        "maintain",
        args,
        env,
        ports,
        spec.maintain.replicas,
        spec.maintain.resources.as_ref(),
        "RollingUpdate",
        &tier_secrets_checksum(spec, ctx, tier_override),
    ))
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::crd::{
        FoldSpec, GatewaySpec, LocalSecretRef, MaintainSpec, QuerySpec, RetentionSpec, S3Spec,
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
            gateway: GatewaySpec {
                replicas: 3,
                resources: None,
                credentials_secret_ref: None,
                fold: Some(FoldSpec {
                    disabled: false,
                    interval_secs: Some(120),
                }),
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
            retention: Some(RetentionSpec {
                default: Some("30d".to_string()),
                tenants: BTreeMap::from([("acme".to_string(), "7d".to_string())]),
            }),
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
        let m = desired_maintain_deployment(&spec, "prod", &ctx()).expect("maintain enabled");
        assert_eq!(arg_value(&args_of(&m), "--shards").as_deref(), Some("8"));
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
        let m = desired_maintain_deployment(&spec, "prod", &ctx()).expect("maintain enabled");
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
        // ADR-0072 decision 4 / #897: when deploymentKeySecretRef is set, every
        // deployment must render --tenant-hash-key-file pointed at the mounted
        // Secret instead of --tenant-hash-unkeyed, and the pod must carry a
        // read-only Volume/VolumeMount sourcing that Secret's `key` entry.
        let mut spec = base_spec();
        spec.deployment_key_secret_ref = Some(LocalSecretRef {
            name: "ravel-deployment-key".to_string(),
        });
        let g = desired_gateway_deployment(&spec, "prod", &ctx());
        let q = desired_query_deployment(&spec, "prod", &ctx());
        let m = desired_maintain_deployment(&spec, "prod", &ctx()).expect("maintain enabled");
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
        let m = desired_maintain_deployment(&spec, "prod", &ctx()).expect("enabled");
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
        let m = desired_maintain_deployment(&spec, "prod", &ctx).expect("enabled");
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
        assert!(desired_maintain_deployment(&spec, "prod", &ctx()).is_none());
    }

    #[test]
    fn maintain_replicas_two_renders_two_pods_with_rolling_update_and_ownership_wiring() {
        // Issue #918 reachability test. Epic EI / ADR-0065 makes >1 maintain
        // replica safe in-process (rendezvous over a heartbeat-derived live
        // worker set), but that machinery was unreachable through the operator:
        // the CRD had no maintain replica field and the Deployment used
        // Recreate, pinning the tier to one pod. This asserts that setting
        // spec.maintain.replicas = 2 renders a two-pod Deployment with a
        // >1-safe strategy, and that each pod carries the ownership-protocol
        // wiring the in-process EI tests rely on.
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
        let m = desired_maintain_deployment(&spec, "prod", &ctx).expect("enabled");
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
            desired_maintain_deployment(&spec, "prod", &ctx()).expect("enabled"),
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
        let expected = secrets_checksum(Some("tok-1"), Some("cred-1"), None);
        for dep in [
            desired_gateway_deployment(&spec, "prod", &ctx),
            desired_query_deployment(&spec, "prod", &ctx),
            desired_maintain_deployment(&spec, "prod", &ctx).expect("enabled"),
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
        let a = secrets_checksum(Some("100"), Some("200"), Some("300"));
        assert_eq!(a, secrets_checksum(Some("100"), Some("200"), Some("300")));
        assert_ne!(a, secrets_checksum(Some("101"), Some("200"), Some("300")));
        assert_ne!(a, secrets_checksum(Some("100"), Some("201"), Some("300")));
        assert_ne!(
            a,
            secrets_checksum(Some("100"), Some("200"), Some("301")),
            "the deployment-key resourceVersion must also feed the checksum \
             (review finding 8, #897), so its rotation rolls pods that mount it"
        );
        // Absent versions collapse to a distinct, stable value.
        let none = secrets_checksum(None, Some("200"), None);
        assert_eq!(none, secrets_checksum(None, Some("200"), None));
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
        };

        let g = desired_gateway_deployment(&spec, "prod", &ctx);
        let q = desired_query_deployment(&spec, "prod", &ctx);
        let m = desired_maintain_deployment(&spec, "prod", &ctx).expect("enabled");

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
                desired_maintain_deployment(&none_spec, "prod", &ctx).expect("enabled"),
                desired_maintain_deployment(&explicit_spec, "prod", &ctx).expect("enabled"),
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
        let m = desired_maintain_deployment(&none_spec, "prod", &ctx).expect("enabled");
        let shared_checksum = secrets_checksum(Some("tok-1"), Some("cred-1"), None);
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
        };
        // Rotate ONLY the gateway credential Secret.
        let mut after_versions = base_versions.clone();
        after_versions.insert("gw-creds".to_string(), "gw-2".to_string());
        let after = RenderCtx {
            tenant_names: vec!["acme".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: after_versions,
            deployment_key_resource_version: Some("dk-1".to_string()),
        };

        let g_before = desired_gateway_deployment(&over_spec, "prod", &before);
        let g_after = desired_gateway_deployment(&over_spec, "prod", &after);
        let q_before = desired_query_deployment(&over_spec, "prod", &before);
        let q_after = desired_query_deployment(&over_spec, "prod", &after);
        let m_before = desired_maintain_deployment(&over_spec, "prod", &before).expect("enabled");
        let m_after = desired_maintain_deployment(&over_spec, "prod", &after).expect("enabled");

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
        };
        let shared_after = RenderCtx {
            tenant_names: vec!["acme".to_string()],
            token_resource_version: Some("tok-1".to_string()),
            credential_resource_versions: BTreeMap::from([(
                "ravel-s3".to_string(),
                "shared-2".to_string(),
            )]),
            deployment_key_resource_version: Some("dk-1".to_string()),
        };
        let gb = desired_gateway_deployment(&shared_spec, "prod", &shared_before);
        let ga = desired_gateway_deployment(&shared_spec, "prod", &shared_after);
        let qb = desired_query_deployment(&shared_spec, "prod", &shared_before);
        let qa = desired_query_deployment(&shared_spec, "prod", &shared_after);
        let mb =
            desired_maintain_deployment(&shared_spec, "prod", &shared_before).expect("enabled");
        let ma = desired_maintain_deployment(&shared_spec, "prod", &shared_after).expect("enabled");
        assert_ne!(checksum_of(&gb), checksum_of(&ga), "gateway must roll");
        assert_ne!(checksum_of(&qb), checksum_of(&qa), "query must roll");
        assert_ne!(checksum_of(&mb), checksum_of(&ma), "maintain must roll");
    }

    #[test]
    fn deployment_key_secret_rotation_rolls_every_tier() {
        // Review finding 8 (#897): every tier mounts the SAME
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
        };
        let after = RenderCtx {
            deployment_key_resource_version: Some("dk-2".to_string()),
            ..before.clone()
        };

        let gb = desired_gateway_deployment(&spec, "prod", &before);
        let ga = desired_gateway_deployment(&spec, "prod", &after);
        let qb = desired_query_deployment(&spec, "prod", &before);
        let qa = desired_query_deployment(&spec, "prod", &after);
        let mb = desired_maintain_deployment(&spec, "prod", &before).expect("enabled");
        let ma = desired_maintain_deployment(&spec, "prod", &after).expect("enabled");
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
}
