//! The `RavelCluster` custom resource (ADR-0034 decision 2).
//!
//! Group `ravel.nofire.ai`, version `v1alpha1`, namespaced, with a status
//! subresource. The spec is built entirely from plain Rust types and a few
//! small local structs, never from `k8s-openapi` API types: the reconcile
//! layer ([`crate::reconcile`]) maps this spec into the actual Kubernetes
//! objects. Keeping `k8s-openapi` types out of the schema-derived spec avoids
//! coupling the CRD's `schemars` derivation to `k8s-openapi`'s optional
//! `schemars` support, which tracks a different `schemars` major.

use std::collections::BTreeMap;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, ValidationRule,
};
use kube::CustomResource;
use kube::CustomResourceExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// `RavelCluster` spec. Each field maps onto `ravel-server`'s real CLI surface
/// (`services/ravel-server/src/config.rs`); the memory store is deliberately
/// unrepresentable (ADR-0034 decision 2), so `storage.s3` is mandatory.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "ravel.nofire.ai",
    version = "v1alpha1",
    kind = "RavelCluster",
    namespaced,
    status = "RavelClusterStatus",
    shortname = "rc",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct RavelClusterSpec {
    /// Container image for every mode's pod (`ravel-server`).
    pub image: String,

    /// Image pull policy (`Always`, `IfNotPresent`, `Never`). Defaults to the
    /// Kubernetes default (`IfNotPresent`, or `Always` for a `:latest` tag)
    /// when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,

    /// Shard count, rendered into `--shards` for gateway, query, and maintain
    /// alike so the must-match invariant is unrepresentable to break
    /// (ADR-0034 decision 2). Immutable after creation, enforced by a CEL
    /// transition rule on this field's schema (see [`ravel_cluster_crd`]).
    pub shards: u32,

    /// Object storage backend. S3 only: a per-process memory store is
    /// incoherent across pods (ADR-0034 decision 2).
    pub storage: StorageSpec,

    /// Secret whose keys are tenant names and whose values are the bearer
    /// tokens for those tenants. The operator renders `--tenant-token
    /// $(VAR)=<tenant>` using kubelet `$(VAR)` expansion so token values never
    /// appear in the Pod spec.
    ///
    /// WARNING: `ravel-server`'s `parse_tenant_tokens`
    /// (`services/ravel-server/src/config.rs`) splits each `--tenant-token`
    /// argument on the FIRST `=` via `str::split_once('=')`. A token VALUE that
    /// contains `=` is therefore mis-parsed (the suffix after the first `=` is
    /// taken as part of the tenant, not the token). This is pre-existing
    /// behavior in a different crate, out of this operator's scope to change;
    /// choose token values without `=` until a native token source lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_tokens_secret_ref: Option<LocalSecretRef>,

    /// Secret with a single key `key` holding the deployment's 32-byte
    /// deployment key (64 hex characters or 32 raw bytes, the same format
    /// `--tenant-hash-key-file` reads). This one key doubles as the v2-keyed
    /// tenant-hash key and the `sys/auth` token-hashing key (ADR-0072
    /// decision 4): when set, the operator mounts it and renders
    /// `--tenant-hash-key-file` instead of `--tenant-hash-unkeyed`, and
    /// reconciles `sys/auth` from `tenantTokensSecretRef` each cycle. Additive
    /// opt-in, not a migration of existing unkeyed clusters; omit to keep
    /// unkeyed behavior unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_key_secret_ref: Option<LocalSecretRef>,

    /// Gateway (ingest + query API) tier.
    #[serde(default)]
    pub gateway: GatewaySpec,

    /// Query tier.
    #[serde(default)]
    pub query: QuerySpec,

    /// Background maintenance tier (compaction, retention, GC). Defaults to a
    /// single replica; the bounded multi-worker ownership protocol (ADR-0065)
    /// makes `replicas > 1` safe, superseding ADR-0034's single-replica
    /// guidance.
    #[serde(default)]
    pub maintain: MaintainSpec,

    /// Age-based retention: a default window plus per-tenant overrides. Enforced
    /// only by the maintain tier, so these render onto the maintain Deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionSpec>,
}

/// Reference to a Secret in the same namespace as the `RavelCluster`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalSecretRef {
    /// Name of the Secret.
    pub name: String,
}

/// Object storage configuration.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// S3 (or S3-compatible) backend.
    pub s3: S3Spec,
}

/// S3 backend configuration. `bucket`/`region`/`endpoint` render as CLI flags;
/// credentials are injected as env vars from a Secret via `valueFrom`, never as
/// literal flag values (ADR-0034 decision 2).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3Spec {
    /// Bucket name (`--s3-bucket`).
    pub bucket: String,

    /// AWS region (`--s3-region`). Defaults to `us-east-1`.
    #[serde(default = "default_region")]
    pub region: String,

    /// Custom endpoint (`--s3-endpoint`) for S3-compatible backends such as
    /// MinIO or floci. Omit for real AWS S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Secret with keys `accessKeyId` and `secretAccessKey`, injected as the
    /// `RAVEL_S3_ACCESS_KEY` / `RAVEL_S3_SECRET_KEY` env vars.
    pub credentials_secret_ref: LocalSecretRef,
}

/// Gateway tier: replicas, resources, and optional fold tuning.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewaySpec {
    /// Replica count for the gateway Deployment.
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// Container resource requests/limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirementsSpec>,

    /// Per-role S3 credential override for the gateway tier (ADR-0055 section 5).
    /// When set, the gateway Deployment sources `RAVEL_S3_ACCESS_KEY` /
    /// `RAVEL_S3_SECRET_KEY` from this Secret (keys `accessKeyId` /
    /// `secretAccessKey`) instead of the shared `storage.s3.credentialsSecretRef`,
    /// so an operator can scope the gateway to a narrower IAM credential. Falls
    /// back to the shared credential when unset, so existing single-credential
    /// clusters are unaffected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret_ref: Option<LocalSecretRef>,

    /// Catalog fold tuning. Fold is a pure query-cost optimization and only
    /// runs in the gateway tier, so it is a gateway-only field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fold: Option<FoldSpec>,
}

impl Default for GatewaySpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            resources: None,
            credentials_secret_ref: None,
            fold: None,
        }
    }
}

/// Catalog fold tuning (`--disable-fold`, `--fold-interval-secs`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FoldSpec {
    /// Disable the background fold task entirely (`--disable-fold`). Never
    /// changes query results, only their cost.
    #[serde(default)]
    pub disabled: bool,

    /// How often each tenant's fold task wakes, in seconds
    /// (`--fold-interval-secs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// Query tier: replicas and resources.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuerySpec {
    /// Replica count for the query Deployment.
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// Container resource requests/limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirementsSpec>,

    /// Per-role S3 credential override for the query tier (ADR-0055 section 5).
    /// When set, the query Deployment sources `RAVEL_S3_ACCESS_KEY` /
    /// `RAVEL_S3_SECRET_KEY` from this Secret (keys `accessKeyId` /
    /// `secretAccessKey`) instead of the shared `storage.s3.credentialsSecretRef`,
    /// so an operator can scope the query tier to a narrower IAM credential. Falls
    /// back to the shared credential when unset, so existing single-credential
    /// clusters are unaffected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret_ref: Option<LocalSecretRef>,
}

impl Default for QuerySpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            resources: None,
            credentials_secret_ref: None,
        }
    }
}

/// Maintain tier: enabled flag, replicas, interval, resources.
///
/// `replicas` defaults to 1 (today's behavior). ADR-0065's bounded
/// multi-worker ownership protocol (self-owned heartbeat keys under
/// `sys/maintain/workers/` plus rendezvous-hash unit partitioning) makes
/// `replicas > 1` safe: every maintain pod runs `--mode maintain`, discovers
/// the live worker set from the shared store, and independently owns a
/// disjoint slice of the `(tenant, signal, shard)` unit space, so N replicas
/// partition the work rather than each paying for all of it. This supersedes
/// ADR-0034 decision 3's single-replica `Recreate` guidance (see ADR-0065's
/// deployment-model consequence).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaintainSpec {
    /// Whether the maintain Deployment exists at all. Defaults to true.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Replica count for the maintain Deployment. Defaults to 1. Values above 1
    /// are made safe by the ADR-0065 ownership protocol (rendezvous over a
    /// heartbeat-derived live worker set); the workers coordinate entirely
    /// through the shared object store, so no additional per-pod wiring is
    /// needed beyond `--mode maintain` and the shared store flags. Omit to keep
    /// today's single-replica behavior.
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// How often the maintenance task wakes, in seconds
    /// (`--maintain-interval-secs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Container resource requests/limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirementsSpec>,

    /// Per-role S3 credential override for the maintain tier (ADR-0055 section 5).
    /// When set, the maintain Deployment sources `RAVEL_S3_ACCESS_KEY` /
    /// `RAVEL_S3_SECRET_KEY` from this Secret (keys `accessKeyId` /
    /// `secretAccessKey`) instead of the shared `storage.s3.credentialsSecretRef`.
    /// Maintain is the only role ADR-0055 grants delete, so this is where an
    /// operator points the delete-capable credential. Falls back to the shared
    /// credential when unset, so existing single-credential clusters are
    /// unaffected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret_ref: Option<LocalSecretRef>,
}

impl Default for MaintainSpec {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            replicas: default_replicas(),
            interval_secs: None,
            resources: None,
            credentials_secret_ref: None,
        }
    }
}

/// Retention policy (`--retention-default`, repeatable `--retention-tenant`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct RetentionSpec {
    /// Default age-based retention window as a humantime duration (`30d`,
    /// `720h`). Omitted means no default retention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,

    /// Per-tenant retention overrides: tenant name to humantime duration.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tenants: BTreeMap<String, String>,
}

/// Container resource requests and limits, mapping onto the Kubernetes
/// `ResourceRequirements` shape (`cpu`/`memory` quantity strings).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirementsSpec {
    /// Minimum guaranteed resources (`resources.requests`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<BTreeMap<String, String>>,

    /// Resource ceilings (`resources.limits`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<BTreeMap<String, String>>,
}

/// Observed status (ADR-0034 decision 2). Written by the operator on the status
/// subresource.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct RavelClusterStatus {
    /// The `.metadata.generation` this status reflects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Ready replicas reported by the gateway Deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_ready_replicas: Option<i32>,

    /// Ready replicas reported by the query Deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_ready_replicas: Option<i32>,

    /// Ready replicas reported by the maintain Deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintain_ready_replicas: Option<i32>,

    /// Standard Kubernetes conditions: `Available`, `Progressing`, `Degraded`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A standard Kubernetes status condition (the `metav1.Condition` shape). Kept
/// as a local type rather than `k8s-openapi`'s `Condition` so the status schema
/// derives from `schemars` without the `k8s-openapi` schemars coupling.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Condition type, e.g. `Available`.
    pub r#type: String,

    /// `True`, `False`, or `Unknown`.
    pub status: String,

    /// The `.metadata.generation` the condition was set against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// RFC3339 timestamp of the last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Machine-readable reason (PascalCase).
    pub reason: String,

    /// Human-readable message.
    pub message: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_replicas() -> i32 {
    1
}

fn default_true() -> bool {
    true
}

/// Build the `CustomResourceDefinition` for [`RavelCluster`], with the CEL
/// immutability rule injected onto the `shards` field.
///
/// The base CRD is generated by the `CustomResource` derive
/// ([`CustomResourceExt::crd`]); this function then stamps an
/// `x-kubernetes-validations` transition rule (`self == oldSelf`) onto the
/// `shards` property so the API server rejects any update that changes the
/// shard count (ADR-0034 decision 2). Injecting it here, rather than through a
/// derive attribute, keeps the rule visible and directly unit-testable without
/// a live API server.
pub fn ravel_cluster_crd() -> CustomResourceDefinition {
    let mut crd = RavelCluster::crd();
    inject_shards_immutability(&mut crd);
    inject_minimum_bounds(&mut crd);
    crd
}

/// Immutability message surfaced to a user who tries to change `shards`.
const SHARDS_IMMUTABLE_MESSAGE: &str =
    "shards is immutable after creation; resharding is out of scope (ADR-0034)";

/// Attach the `self == oldSelf` CEL transition rule to the `shards` property of
/// every stored version's schema.
fn inject_shards_immutability(crd: &mut CustomResourceDefinition) {
    let rule = ValidationRule {
        rule: "self == oldSelf".to_string(),
        message: Some(SHARDS_IMMUTABLE_MESSAGE.to_string()),
        ..Default::default()
    };
    for version in &mut crd.spec.versions {
        let Some(schema) = version.schema.as_mut() else {
            continue;
        };
        let Some(root) = schema.open_api_v3_schema.as_mut() else {
            continue;
        };
        let Some(props) = root.properties.as_mut() else {
            continue;
        };
        let Some(spec) = props.get_mut("spec") else {
            continue;
        };
        let Some(spec_props) = spec.properties.as_mut() else {
            continue;
        };
        let Some(shards) = spec_props.get_mut("shards") else {
            continue;
        };
        shards.x_kubernetes_validations = Some(vec![rule.clone()]);
    }
}

/// Attach OpenAPI `minimum: 1` bounds to the count fields that must be positive:
/// `spec.shards`, `spec.gateway.replicas`, `spec.query.replicas`, and
/// `spec.maintain.replicas`.
///
/// Without this, `shards: 0` or a negative replica count passes CRD validation
/// and only fails much later as a confusing Deployment-apply error or a
/// catalog-config panic. schemars 1.2.2 renders these fields as plain
/// integers (a `u32`'s implicit `minimum: 0` is not a positive floor, and
/// `i32` replicas get none), so the `minimum` keyword is injected here for the
/// same reason the CEL rule is: post-hoc injection keeps it directly
/// unit-testable without a live API server, and does not depend on which
/// numeric-bound attributes this schemars major happens to emit.
fn inject_minimum_bounds(crd: &mut CustomResourceDefinition) {
    for version in &mut crd.spec.versions {
        let Some(schema) = version.schema.as_mut() else {
            continue;
        };
        let Some(root) = schema.open_api_v3_schema.as_mut() else {
            continue;
        };
        let Some(props) = root.properties.as_mut() else {
            continue;
        };
        let Some(spec) = props.get_mut("spec") else {
            continue;
        };
        let Some(spec_props) = spec.properties.as_mut() else {
            continue;
        };
        if let Some(shards) = spec_props.get_mut("shards") {
            shards.minimum = Some(1.0);
        }
        for tier in ["gateway", "query", "maintain"] {
            if let Some(replicas) = spec_props
                .get_mut(tier)
                .and_then(|t| t.properties.as_mut())
                .and_then(|p| p.get_mut("replicas"))
            {
                replicas.minimum = Some(1.0);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn crd_has_expected_identity() {
        let crd = ravel_cluster_crd();
        assert_eq!(crd.spec.group, "ravel.nofire.ai");
        assert_eq!(crd.spec.names.kind, "RavelCluster");
        assert_eq!(crd.spec.scope, "Namespaced");
        let version = &crd.spec.versions[0];
        assert_eq!(version.name, "v1alpha1");
        assert!(version.served);
        assert!(version.storage);
        // The status subresource must be declared for status writes to work.
        assert!(
            version
                .subresources
                .as_ref()
                .expect("subresources present")
                .status
                .is_some(),
            "status subresource must be enabled"
        );
    }

    #[test]
    fn crd_schema_includes_shard_immutability_rule() {
        let crd = ravel_cluster_crd();
        let version = &crd.spec.versions[0];
        let root = version
            .schema
            .as_ref()
            .expect("schema")
            .open_api_v3_schema
            .as_ref()
            .expect("root schema");
        let spec = root
            .properties
            .as_ref()
            .expect("root props")
            .get("spec")
            .expect("spec prop");
        let shards = spec
            .properties
            .as_ref()
            .expect("spec props")
            .get("shards")
            .expect("shards prop");
        let rules = shards
            .x_kubernetes_validations
            .as_ref()
            .expect("shards must carry a validation rule");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].rule, "self == oldSelf");
        assert!(rules[0].message.is_some());
    }

    #[test]
    fn count_fields_carry_minimum_one_bound() {
        let crd = ravel_cluster_crd();
        let version = &crd.spec.versions[0];
        let root = version
            .schema
            .as_ref()
            .expect("schema")
            .open_api_v3_schema
            .as_ref()
            .expect("root schema");
        let spec_props = root
            .properties
            .as_ref()
            .expect("root props")
            .get("spec")
            .expect("spec prop")
            .properties
            .as_ref()
            .expect("spec props");

        assert_eq!(
            spec_props.get("shards").expect("shards prop").minimum,
            Some(1.0),
            "shards must reject 0"
        );
        for tier in ["gateway", "query", "maintain"] {
            let replicas = spec_props
                .get(tier)
                .expect("tier prop")
                .properties
                .as_ref()
                .expect("tier props")
                .get("replicas")
                .expect("replicas prop");
            assert_eq!(
                replicas.minimum,
                Some(1.0),
                "{tier}.replicas must reject negative/zero"
            );
        }
    }

    #[test]
    fn per_tier_credential_overrides_are_in_the_schema_and_optional() {
        // ADR-0055 section 5: each tier gains an optional credentialsSecretRef.
        // The JsonSchema derive must surface them under each tier's properties,
        // and they must be absent by default (Option::is_none skip) so an
        // existing spec that omits them still deserializes.
        let crd = ravel_cluster_crd();
        let version = &crd.spec.versions[0];
        let spec_props = version
            .schema
            .as_ref()
            .expect("schema")
            .open_api_v3_schema
            .as_ref()
            .expect("root schema")
            .properties
            .as_ref()
            .expect("root props")
            .get("spec")
            .expect("spec prop")
            .properties
            .as_ref()
            .expect("spec props");
        for tier in ["gateway", "query", "maintain"] {
            let tier_props = spec_props
                .get(tier)
                .expect("tier prop")
                .properties
                .as_ref()
                .expect("tier props");
            assert!(
                tier_props.contains_key("credentialsSecretRef"),
                "{tier} must expose credentialsSecretRef in its schema"
            );
        }

        // A spec omitting every override still deserializes, with all None.
        let json = serde_json::json!({
            "image": "ravel:dev",
            "shards": 4,
            "storage": { "s3": { "bucket": "b", "credentialsSecretRef": { "name": "creds" } } }
        });
        let spec: RavelClusterSpec = serde_json::from_value(json).expect("deserialize");
        assert_eq!(spec.gateway.credentials_secret_ref, None);
        assert_eq!(spec.query.credentials_secret_ref, None);
        assert_eq!(spec.maintain.credentials_secret_ref, None);
    }

    #[test]
    fn deployment_key_secret_ref_is_in_the_schema_and_optional() {
        // ADR-0072 decision 4 / #897: the CRD gains an optional
        // deploymentKeySecretRef at the top level (sibling of
        // tenantTokensSecretRef, not per-tier: one deployment key covers the
        // whole cluster). Must be visible in the schema and default to None so
        // an existing keyless spec still deserializes unchanged.
        let crd = ravel_cluster_crd();
        let version = &crd.spec.versions[0];
        let spec_props = version
            .schema
            .as_ref()
            .expect("schema")
            .open_api_v3_schema
            .as_ref()
            .expect("root schema")
            .properties
            .as_ref()
            .expect("root props")
            .get("spec")
            .expect("spec prop")
            .properties
            .as_ref()
            .expect("spec props");
        assert!(
            spec_props.contains_key("deploymentKeySecretRef"),
            "spec must expose deploymentKeySecretRef in its schema"
        );

        let json = serde_json::json!({
            "image": "ravel:dev",
            "shards": 4,
            "storage": { "s3": { "bucket": "b", "credentialsSecretRef": { "name": "creds" } } }
        });
        let spec: RavelClusterSpec = serde_json::from_value(json).expect("deserialize");
        assert_eq!(spec.deployment_key_secret_ref, None);
    }

    #[test]
    fn spec_defaults_are_applied_on_deserialize() {
        // A minimal spec: only the required fields. Optional tiers and region
        // must fall back to their defaults.
        let json = serde_json::json!({
            "image": "ravel:dev",
            "shards": 4,
            "storage": { "s3": { "bucket": "b", "credentialsSecretRef": { "name": "creds" } } }
        });
        let spec: RavelClusterSpec = serde_json::from_value(json).expect("deserialize");
        assert_eq!(spec.storage.s3.region, "us-east-1");
        assert_eq!(spec.gateway.replicas, 1);
        assert_eq!(spec.query.replicas, 1);
        // Maintain replicas is optional and defaults to 1: an existing spec
        // that omits it keeps single-replica behavior.
        assert_eq!(spec.maintain.replicas, 1);
        assert!(spec.maintain.enabled);
        assert_eq!(spec.image_pull_policy, None);
    }
}
