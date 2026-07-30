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
    Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction, PodSpec, PodTemplateSpec, Probe,
    ResourceRequirements, SecretKeySelector, Service, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::crd::{RavelClusterSpec, ResourceRequirementsSpec};

/// HTTP listener port (OTLP/HTTP, query API, and the `/healthz` `/readyz`
/// probes). Fixed to match the Dockerfile's `EXPOSE` and the server's default.
pub const HTTP_PORT: i32 = 4318;

/// gRPC listener port (OTLP/gRPC), exposed by the gateway tier only.
pub const GRPC_PORT: i32 = 4317;

/// Secret key holding the S3 access key id.
const S3_ACCESS_KEY_ID_KEY: &str = "accessKeyId";
/// Secret key holding the S3 secret access key.
const S3_SECRET_ACCESS_KEY_KEY: &str = "secretAccessKey";

/// Inputs the controller resolves from the cluster that are not part of the
/// CRD spec, threaded into the otherwise-pure render functions.
///
/// `tenant_names` are the keys of the `tenantTokensSecretRef` Secret (the
/// tenant names). They are not in the spec — the spec only names the Secret —
/// so the controller reads them (RBAC grants `get` on Secrets) and passes them
/// here. Keeping them a plain input rather than an I/O call inside the render
/// keeps these functions pure and testable.
#[derive(Debug, Clone, Default)]
pub struct RenderCtx {
    /// Tenant names (the token Secret's keys), rendered in the given order.
    pub tenant_names: Vec<String>,
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
fn s3_credential_env(spec: &RavelClusterSpec) -> Vec<EnvVar> {
    let secret = &spec.storage.s3.credentials_secret_ref.name;
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

/// Args shared by every mode: store selection, shard count, and the S3
/// bucket/region/endpoint flags. Access/secret keys are NOT here (they are env
/// vars, see [`s3_credential_env`]).
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
    ];
    if let Some(endpoint) = &spec.storage.s3.endpoint {
        args.push("--s3-endpoint".to_string());
        args.push(endpoint.clone());
    }
    args
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
) -> Deployment {
    let labels = labels(instance, component);
    let (liveness, readiness) = probes();
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
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
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

    let mut env = s3_credential_env(spec);
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

    let mut env = s3_credential_env(spec);
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
    )
}

/// The maintain Deployment, or `None` when `maintain.enabled` is false (the
/// controller deletes the Deployment in that case).
///
/// Single replica with the `Recreate` strategy (ADR-0034 decision 3): there is
/// only ever 0 or 1, so there is no replica field on the spec side. Maintain
/// serves `--listen-http` for the health probes only and does not need tenant
/// tokens (it authenticates no requests); retention flags render here because
/// retention is enforced by this tier.
pub fn desired_maintain_deployment(spec: &RavelClusterSpec, instance: &str) -> Option<Deployment> {
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

    let env = s3_credential_env(spec);

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
        1,
        spec.maintain.resources.as_ref(),
        "Recreate",
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
            gateway: GatewaySpec {
                replicas: 3,
                resources: None,
                fold: Some(FoldSpec {
                    disabled: false,
                    interval_secs: Some(120),
                }),
            },
            query: QuerySpec {
                replicas: 2,
                resources: None,
            },
            maintain: MaintainSpec {
                enabled: true,
                interval_secs: Some(600),
                resources: None,
            },
            retention: Some(RetentionSpec {
                default: Some("30d".to_string()),
                tenants: BTreeMap::from([("acme".to_string(), "7d".to_string())]),
            }),
        }
    }

    fn ctx() -> RenderCtx {
        RenderCtx {
            tenant_names: vec!["acme".to_string(), "globex".to_string()],
        }
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
        let m = desired_maintain_deployment(&spec, "prod").expect("maintain enabled");
        assert_eq!(arg_value(&args_of(&m), "--shards").as_deref(), Some("8"));
    }

    #[test]
    fn gateway_renders_mode_listeners_and_store_flags() {
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
    fn query_has_no_grpc_listener_or_port() {
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
    fn maintain_uses_recreate_single_replica_and_retention() {
        let spec = base_spec();
        let m = desired_maintain_deployment(&spec, "prod").expect("enabled");
        let dspec = m.spec.as_ref().expect("spec");
        assert_eq!(dspec.replicas, Some(1));
        assert_eq!(
            dspec.strategy.as_ref().expect("strategy").type_.as_deref(),
            Some("Recreate")
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
        // Maintain authenticates nothing, so no tenant tokens.
        assert!(!args.iter().any(|a| a == "--tenant-token"));
    }

    #[test]
    fn maintain_disabled_yields_none() {
        let mut spec = base_spec();
        spec.maintain.enabled = false;
        assert!(desired_maintain_deployment(&spec, "prod").is_none());
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
            desired_maintain_deployment(&spec, "prod").expect("enabled"),
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
}
