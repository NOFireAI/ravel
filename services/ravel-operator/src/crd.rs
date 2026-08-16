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

    /// Per-tenant shard-count overrides (#147, ADR-0076 decision 2): the
    /// primary operator-facing cost control, wired to the already-shipped
    /// ADR-0052 online-resharding mechanism. Distinct from the immutable,
    /// cluster-wide `shards` field above: this drives a durable
    /// `append_generation` call per named tenant, never a Deployment
    /// `--shards` flag, and does not relax `shards`'s own CEL immutability
    /// rule (see [`ravel_cluster_crd`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_overrides: Option<ShardOverridesSpec>,
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

    /// Layer-7 ingest affinity (ADR-0076 decision 1). Omit to keep today's
    /// behavior exactly: no Ingress is rendered and ingest traffic reaches the
    /// gateway Service however the platform owner already routes it.
    ///
    /// Ingest buffers are per `(tenant, signal, shard)` PER REPLICA, so a
    /// tenant whose writes spray across every gateway replica pays one
    /// age-triggered flush stream per replica for the same data. Pinning a
    /// tenant to a stable subset divides that term. Affinity is best-effort:
    /// `writer_id` and `epoch` already disambiguate concurrent writers in the
    /// object key, so a reroute is always correct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingest_affinity: Option<IngestAffinitySpec>,

    /// Gateway API exposure (ADR-0080 decision 2), independent of
    /// [`GatewaySpec::ingest_affinity`]. Omit to keep today's behavior exactly:
    /// no `HTTPRoute`/`GRPCRoute` is rendered and exposure for a plain
    /// deployment stays entirely user-managed outside Ravel.
    ///
    /// Unlike `ingest_affinity`, this renders standard
    /// `gateway.networking.k8s.io` routes onto a user-provided `Gateway`; it can
    /// be set with affinity disabled (plain routing, no tenant pinning) or
    /// combined with `backend: ravelNative`. It cannot be combined with an
    /// enabled `ingestAffinity` on `backend: ingressNginx`: the Gateway API path
    /// and the annotation-driven Ingress path are two independent routes to the
    /// same Service, so traffic on the Gateway API route would bypass the nginx
    /// subset annotations entirely. That combination is rejected at admission by
    /// a CEL rule attached to the `gateway` object (see [`ravel_cluster_crd`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exposure: Option<GatewayExposureSpec>,
}

impl Default for GatewaySpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            resources: None,
            credentials_secret_ref: None,
            fold: None,
            ingest_affinity: None,
            exposure: None,
        }
    }
}

/// Ravel-owned exposure for the gateway tier (ADR-0080 decision 2), separate
/// from ingest affinity.
///
/// A single wrapper struct so a later exposure mechanism (an alternative to
/// Gateway API) becomes a new sibling field here rather than another optional
/// on `GatewaySpec`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewayExposureSpec {
    /// Gateway API (`gateway.networking.k8s.io`) exposure: the operator renders
    /// one `HTTPRoute` and, when `grpc` is true, one `GRPCRoute` attached to an
    /// existing `Gateway`. Omit for no Gateway API routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_api: Option<GatewayApiExposureSpec>,
}

/// Gateway API exposure config (ADR-0080 decision 2).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewayApiExposureSpec {
    /// The existing `Gateway` the rendered routes attach to via `parentRefs`.
    pub gateway_ref: GatewayReference,

    /// Hostnames the rendered routes answer on. Empty renders routes with no
    /// `hostnames`, which match every hostname the parent `Gateway`'s listeners
    /// accept.
    #[serde(default)]
    pub hostnames: Vec<String>,

    /// Also render a `GRPCRoute` for OTLP/gRPC (port 4317). Defaults to true,
    /// mirroring [`IngestAffinitySpec::grpc`]: gRPC is the main OTLP ingest
    /// path.
    #[serde(default = "default_true")]
    pub grpc: bool,
}

/// Reference to an existing `Gateway` (`gateway.networking.k8s.io`) the rendered
/// routes attach to.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewayReference {
    /// Name of the `Gateway`.
    pub name: String,

    /// Namespace of the `Gateway`. Defaults to the `RavelCluster`'s own
    /// namespace when omitted (Gateway API resolves an omitted `parentRefs`
    /// namespace to the route's own namespace, which the operator renders into
    /// the `RavelCluster`'s namespace).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Layer-7 ingest affinity for the gateway tier (ADR-0076 decision 1).
///
/// The operator renders an Ingress (two, when `grpc` is on) carrying
/// ingress-nginx's consistent-hash annotations
/// (`nginx.ingress.kubernetes.io/upstream-hash-by` plus its subset pair). The
/// hash key is tenant identity taken from authentication material, never from
/// the URL path: OTLP connections are long-lived and Ravel resolves tenancy
/// server-side from the credential, so a path-based rule cannot see the tenant.
///
/// This deliberately does NOT use the core Service `sessionAffinity: ClientIP`
/// field. That keys on the client's source address, which is the collector's or
/// the gateway proxy's address rather than the tenant's, and would look like
/// affinity while pinning the wrong thing.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IngestAffinitySpec {
    /// Whether affinity is active. Defaults to true, so declaring the block is
    /// enough. Set it to false to turn affinity off during an incident without
    /// deleting the rest of the configuration; the Ingress objects are then
    /// removed and rendering returns to the affinity-absent baseline.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Which implementation carries out the pinning (ADR-0080 decision 1).
    /// Defaults to [`AffinityBackend::IngressNginx`], so a CR written before
    /// this field existed keeps the backend it already runs, with the same
    /// `subsetSize` and `key` semantics.
    ///
    /// `ingressNginx` is deprecated: ingress-nginx is retiring upstream, and a
    /// cluster using it gets an `IngestAffinityBackendDeprecated` condition on
    /// its `RavelCluster` status.
    #[serde(default)]
    pub backend: AffinityBackend,

    /// Container image for the `ravel-ingest-router` Deployment rendered under
    /// [`AffinityBackend::RavelNative`] (ADR-0080 decision 3).
    ///
    /// The router is a DIFFERENT binary from `spec.image` (which is
    /// `ravel-server`), so its image cannot be derived from `spec.image` and is
    /// named explicitly here. Deriving it by string-manipulating `spec.image`
    /// would be fragile: an image reference host can carry a port
    /// (`localhost:5000/ravel-server:v1`), so any split-on-`:` guess breaks.
    ///
    /// Required when `backend: ravelNative`: the operator surfaces a `Degraded`
    /// status condition (reason `RouterImageMissing`) and renders no router
    /// objects when it is absent, rather than a Deployment with an empty
    /// `image:` that would never schedule. Has no effect under
    /// `backend: ingressNginx`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub router_image: Option<String>,

    /// How many replicas a tenant's traffic is pinned to. Defaults to
    /// [`DEFAULT_AFFINITY_SUBSET_SIZE`] (two), not one, so a single replica
    /// loss does not concentrate a tenant on one process.
    ///
    /// A subset is a throughput ceiling by construction: a tenant can use at
    /// most `subsetSize` gateway replicas no matter how many the Deployment
    /// runs. Raise it for a high-volume tenant (see
    /// `docs/guides/ingest-affinity.md`); the request-cost divisor is
    /// `replicas / subsetSize`.
    ///
    /// This is a per-cluster setting, not per-tenant: ingress-nginx's subset
    /// size is one number per Ingress. A tenant needing a materially larger
    /// subset than its siblings belongs in its own `RavelCluster`.
    #[serde(default = "default_subset_size")]
    pub subset_size: u32,

    /// Where the load balancer reads tenant identity from. Defaults to the
    /// `Authorization` header, which is what Ravel itself resolves tenancy
    /// from.
    #[serde(default)]
    pub key: AffinityKeySpec,

    /// `spec.ingressClassName` on the rendered Ingress objects. Omit to let the
    /// cluster's default IngressClass apply.
    ///
    /// Legacy `backend: ingressNginx` only. Has no effect under
    /// `backend: ravelNative`, and no effect on `gateway.exposure.gatewayAPI`
    /// routing (Gateway API attachment goes through `exposure.gatewayAPI
    /// .gatewayRef` instead, ADR-0080 decision 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_class_name: Option<String>,

    /// Hostnames the ingest Ingress answers on. Empty renders a single
    /// host-less rule, which matches any host reaching the controller.
    ///
    /// Legacy `backend: ingressNginx` only. `gateway.exposure.gatewayAPI
    /// .hostnames` is the equivalent field for Gateway API exposure; the two
    /// are independent and neither reads the other.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,

    /// TLS Secret for `spec.tls` on the rendered Ingress objects. Strongly
    /// recommended and effectively required for the gRPC Ingress: ingress-nginx
    /// serves HTTP/2 to clients over TLS, and OTLP/gRPC needs HTTP/2. Tenant
    /// tokens are bearer tokens, so plaintext ingest exposes them.
    ///
    /// Legacy `backend: ingressNginx` only. Gateway API exposure terminates
    /// TLS at the referenced `Gateway`'s own listener, which this operator
    /// does not render; configure `tls.certificateRefs` on that Gateway
    /// directly (ADR-0080 decision 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_secret_name: Option<String>,

    /// Also render a second Ingress for OTLP/gRPC (port 4317) carrying
    /// `nginx.ingress.kubernetes.io/backend-protocol: GRPC`. Defaults to true:
    /// gRPC is the main OTLP ingest path, and affinity that covers only
    /// OTLP/HTTP would miss most of the traffic generating the request bill.
    /// A separate object is required because `backend-protocol` is per-Ingress.
    #[serde(default = "default_true")]
    pub grpc: bool,

    /// Extra annotations merged onto both rendered Ingress objects, for an
    /// ingress controller other than ingress-nginx or for tuning
    /// (`nginx.ingress.kubernetes.io/proxy-body-size` is the common one: the
    /// ingress-nginx default of 1m rejects larger OTLP/HTTP payloads).
    ///
    /// The affinity annotations the operator computes are applied AFTER this
    /// map, so an entry here can never silently disable the affinity it is
    /// meant to configure.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

impl Default for IngestAffinitySpec {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            backend: AffinityBackend::default(),
            router_image: None,
            subset_size: default_subset_size(),
            key: AffinityKeySpec::default(),
            ingress_class_name: None,
            hosts: Vec::new(),
            tls_secret_name: None,
            grpc: default_true(),
            annotations: BTreeMap::new(),
        }
    }
}

/// Which implementation carries out subset pinning (ADR-0080 decision 1).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub enum AffinityBackend {
    /// Render ingress-nginx `Ingress` objects carrying the
    /// `upstream-hash-by-subset` annotation family. The default, so an existing
    /// CR that never set `backend` keeps running exactly what it runs today.
    ///
    /// Deprecated: ingress-nginx is retiring upstream. A cluster on this
    /// backend gets an `IngestAffinityBackendDeprecated` condition on its
    /// status; migrate to [`AffinityBackend::RavelNative`] or to an equivalent
    /// Gateway API exposure (`docs/guides/ingest-affinity.md`).
    #[default]
    IngressNginx,

    /// Ravel's own subset router: deterministic rendezvous-hash selection over
    /// the gateway's live endpoints, independent of which ingress or Gateway
    /// implementation terminates the connection.
    RavelNative,
}

/// Which piece of authentication material the load balancer hashes to identify
/// a tenant.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct AffinityKeySpec {
    /// The identity source. Defaults to
    /// [`AffinityKeySource::AuthorizationHeader`].
    #[serde(default)]
    pub source: AffinityKeySource,

    /// Header name when `source` is `header`. Required in that case, enforced
    /// by a CEL rule on this object (see [`ravel_cluster_crd`]), and
    /// constrained to HTTP token characters by an OpenAPI `pattern` so nothing
    /// user-supplied can reach the rendered nginx directive as syntax.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_name: Option<String>,
}

/// The identity source behind [`AffinityKeySpec`].
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub enum AffinityKeySource {
    /// Hash the `Authorization` header (nginx `$http_authorization`). This is
    /// the credential Ravel itself resolves tenancy from, so it is stable per
    /// tenant with no extra client configuration. Two consequences worth
    /// knowing: a tenant using several distinct tokens hashes to several
    /// subsets, and rotating a token moves that tenant to a new subset.
    #[default]
    AuthorizationHeader,

    /// Hash a named header (`headerName`), e.g. a tenant header a trusted
    /// upstream proxy stamps. Only use this when that header cannot be set by
    /// the client, or a tenant can choose its own subset.
    Header,

    /// Hash the mTLS client certificate subject (nginx `$ssl_client_s_dn`).
    /// Requires the ingress controller to terminate TLS with client-certificate
    /// authentication configured; the variable is empty otherwise, which
    /// collapses every tenant onto one subset.
    MtlsSubject,

    /// Hash the canonical `TenantId` resolved by the `ravel-tenant-resolve`
    /// auth chain (ADR-0080 decision 3), immune to bearer-token rotation unlike
    /// hashing the raw `Authorization` header.
    ///
    /// Only meaningful under [`AffinityBackend::RavelNative`]: ingress-nginx has
    /// no way to run the tenant-resolution chain, so this key source combined
    /// with `backend: ingressNginx` is rejected at admission by a CEL rule on
    /// the `ingestAffinity` object (see [`ravel_cluster_crd`]).
    ///
    /// Requires a resolver: `ravel-ingest-router` refuses to start under
    /// `canonical-tenant` unless at least one resolver is configured. The only
    /// resolver surface this CRD exposes today is `tenantTokensSecretRef`;
    /// selecting `canonicalTenant` without it makes the operator surface a
    /// `Degraded` condition (reason `CanonicalTenantResolverMissing`) and render
    /// no router objects, rather than a Deployment that would crashloop.
    CanonicalTenant,
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

/// Per-tenant shard-count overrides (#147, ADR-0076 decision 2), patterned on
/// [`RetentionSpec`]'s per-tenant map.
///
/// Costs an operator takes on by lowering a tenant's count are documented in
/// `docs/guides/shard-overrides.md` (single-actor throughput ceiling,
/// shard-0 concentration at one shard, coarser ADR-0065 maintenance units),
/// not just discovered after the fact.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ShardOverridesSpec {
    /// Per-tenant shard-count targets: tenant name to desired shard count.
    /// Applied independently to every ingested signal (metrics, logs, spans).
    /// A tenant absent here keeps whatever count its provisioning record (or,
    /// absent one, the cluster's day-one `spec.shards`) already has.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tenants: BTreeMap<String, u32>,

    /// Hours of lead time before the new count activates, passed to
    /// `append_generation` as `activation_hour = now_hour + leadHours`.
    /// Enforced to be at least [`MIN_RESHARD_LEAD_HOURS`] -- the same bound
    /// `ravel-cli provision reshard` enforces (`services/ravel-cli/src/
    /// provision.rs`) -- rejected outright rather than silently clamped up,
    /// since a shorter lead could let a live writer route past the
    /// activation on a view it had not yet refreshed.
    #[serde(default = "default_reshard_lead_hours")]
    pub lead_hours: u32,
}

/// Minimum lead hours accepted for a `shardOverrides` reshard, mirroring
/// `ravel-cli provision reshard`'s own `MIN_LEAD_HOURS`
/// (`services/ravel-cli/src/provision.rs`): `ceil(C) + 1` with the router's
/// default 60s refresh interval `C` (ADR-0052 section 3).
pub const MIN_RESHARD_LEAD_HOURS: u32 = 2;

fn default_reshard_lead_hours() -> u32 {
    MIN_RESHARD_LEAD_HOURS
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

/// Default ingest-affinity subset size (ADR-0076 decision 1): **two** replicas,
/// not one, so a single replica loss does not concentrate a tenant on one
/// process.
pub const DEFAULT_AFFINITY_SUBSET_SIZE: u32 = 2;

fn default_subset_size() -> u32 {
    DEFAULT_AFFINITY_SUBSET_SIZE
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
    inject_ingest_affinity_constraints(&mut crd);
    inject_gateway_exposure_affinity_guard(&mut crd);
    inject_canonical_tenant_backend_guard(&mut crd);
    crd
}

/// Immutability message surfaced to a user who tries to change `shards`.
///
/// `shards` itself stays immutable: it renders into every tier's `--shards`
/// flag (ADR-0034 decision 2), and editing it directly would desync that
/// must-match invariant across the gateway/query/maintain Deployments. This is
/// not a claim that resharding at large is out of scope -- ADR-0052 shipped
/// online resharding, and `spec.shardOverrides` (#147, ADR-0076 decision 2)
/// reaches it through a distinct field this CEL rule does not touch.
const SHARDS_IMMUTABLE_MESSAGE: &str = "shards is immutable after creation; use \
    spec.shardOverrides for per-tenant resharding (ADR-0052)";

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

/// Message surfaced to a user who selects the `header` key source without
/// naming a header.
const AFFINITY_HEADER_NAME_MESSAGE: &str =
    "gateway.ingestAffinity.key.headerName is required when key.source is \"header\"";

/// Allowed characters for `gateway.ingestAffinity.key.headerName`.
///
/// The header name is turned into an nginx variable (`X-Scope-OrgID` becomes
/// `$http_x_scope_orgid`) inside the rendered
/// `nginx.ingress.kubernetes.io/upstream-hash-by` annotation. The renderer
/// already maps every character outside `[a-z0-9]` to `_`, so nothing
/// user-supplied can reach the nginx configuration as syntax; this pattern is
/// the second belt, rejecting the mistake at admission with a clear message
/// instead of silently hashing a mangled variable name.
const AFFINITY_HEADER_NAME_PATTERN: &str = "^[A-Za-z0-9][A-Za-z0-9-]{0,62}$";

/// Attach the ingest-affinity constraints (ADR-0076 decision 1):
/// `minimum: 1` on `subsetSize`, a character `pattern` on `key.headerName`, and
/// a CEL rule requiring `headerName` whenever `key.source` is `header`.
///
/// Injected post-hoc for the same reason the other two injectors are: it keeps
/// each rule directly unit-testable without a live API server, and does not
/// depend on which validation attributes this `schemars` major happens to emit.
/// Every step bails out rather than panicking if the schema shape is not what
/// it expects, matching the existing injectors.
fn inject_ingest_affinity_constraints(crd: &mut CustomResourceDefinition) {
    // `has(self.key)` and `has(self.key.source)` guard the field accesses: both
    // have serde defaults, so a submitted manifest may omit either, and a CEL
    // rule that reads a missing field errors instead of passing.
    let rule = ValidationRule {
        rule: "!has(self.key) || !has(self.key.source) || self.key.source != 'header' || \
               (has(self.key.headerName) && self.key.headerName != '')"
            .to_string(),
        message: Some(AFFINITY_HEADER_NAME_MESSAGE.to_string()),
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
        let Some(affinity) = spec_props
            .get_mut("gateway")
            .and_then(|g| g.properties.as_mut())
            .and_then(|p| p.get_mut("ingestAffinity"))
        else {
            continue;
        };
        affinity.x_kubernetes_validations = Some(vec![rule.clone()]);
        let Some(affinity_props) = affinity.properties.as_mut() else {
            continue;
        };
        if let Some(subset_size) = affinity_props.get_mut("subsetSize") {
            subset_size.minimum = Some(1.0);
        }
        if let Some(header_name) = affinity_props
            .get_mut("key")
            .and_then(|k| k.properties.as_mut())
            .and_then(|p| p.get_mut("headerName"))
        {
            header_name.pattern = Some(AFFINITY_HEADER_NAME_PATTERN.to_string());
        }
    }
}

/// Message surfaced to a user who combines Gateway API exposure with an enabled
/// ingress-nginx ingest affinity (ADR-0080 decision 2).
const GATEWAY_EXPOSURE_AFFINITY_MESSAGE: &str = "gateway.exposure.gatewayApi cannot be \
    combined with an enabled gateway.ingestAffinity on backend: ingressNginx -- traffic on the \
    Gateway API path would bypass the nginx subset annotations entirely; use backend: \
    ravelNative or disable ingestAffinity";

/// The CEL rule rejecting Gateway API exposure alongside an enabled
/// ingress-nginx affinity (ADR-0080 decision 2), kept as a constant so the
/// injected rule and the logic unit test cannot drift.
///
/// It passes (no rejection) unless ALL of these hold at once: `exposure.gatewayApi`
/// is set, `ingestAffinity` is present and `enabled`, and its `backend` is
/// `ingressNginx`. The disjunction reads as "the combination is fine if any
/// precondition of the bad case is absent".
const GATEWAY_EXPOSURE_AFFINITY_RULE: &str = "!has(self.exposure) || \
    !has(self.exposure.gatewayApi) || !has(self.ingestAffinity) || \
    !has(self.ingestAffinity.enabled) || !self.ingestAffinity.enabled || \
    !has(self.ingestAffinity.backend) || self.ingestAffinity.backend != 'ingressNginx'";

/// Attach the Gateway-API-exposure-versus-legacy-affinity guard (ADR-0080
/// decision 2) to the `gateway` property itself.
///
/// The rule spans two sibling fields (`exposure` and `ingestAffinity`) rather
/// than validating one struct against its own fields as the `headerName`
/// precedent does, so it is attached at the `gateway` object level, not nested
/// inside `ingestAffinity`. Injected post-hoc for the same reason the other
/// injectors are: it keeps the rule directly unit-testable without a live API
/// server, and does not depend on which validation attributes this `schemars`
/// major happens to emit. Every step bails out rather than panicking if the
/// schema shape is not what it expects, matching the existing injectors.
fn inject_gateway_exposure_affinity_guard(crd: &mut CustomResourceDefinition) {
    // `has()` guards on every field in the path -- exposure, exposure.gatewayApi,
    // ingestAffinity, ingestAffinity.enabled, ingestAffinity.backend -- because
    // each is optional or carries a serde default: a submitted manifest may omit
    // any of them, and a CEL rule that reads a missing field errors instead of
    // passing. `backend` is materialized to `ingressNginx` by the schema default
    // before CEL evaluates, so the `has(self.ingestAffinity.backend)` guard is a
    // defensive belt, not the load-bearing check.
    let rule = ValidationRule {
        rule: GATEWAY_EXPOSURE_AFFINITY_RULE.to_string(),
        message: Some(GATEWAY_EXPOSURE_AFFINITY_MESSAGE.to_string()),
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
        let Some(gateway) = spec_props.get_mut("gateway") else {
            continue;
        };
        // Append rather than replace: leave room for a future rule on `gateway`
        // without silently dropping this one.
        let mut rules = gateway.x_kubernetes_validations.take().unwrap_or_default();
        rules.push(rule.clone());
        gateway.x_kubernetes_validations = Some(rules);
    }
}

/// Message surfaced to a user who selects the `canonicalTenant` key source on
/// the legacy `ingressNginx` backend (ADR-0080 decision 3).
const CANONICAL_TENANT_BACKEND_MESSAGE: &str = "gateway.ingestAffinity.key.source \
    canonicalTenant requires backend: ravelNative -- ingress-nginx cannot run the \
    ravel-tenant-resolve auth chain that canonical-tenant hashing needs";

/// The CEL rule rejecting the `canonicalTenant` key source under
/// `backend: ingressNginx` (ADR-0080 decision 3), kept as a constant so the
/// injected rule and the logic unit test cannot drift.
///
/// It is attached at the `gateway` object level (so `self` is the gateway
/// struct), following the exact shape of the exposure guard rather than nesting
/// inside `ingestAffinity`: that keeps `ingestAffinity`'s own rule set to just
/// its `headerName` rule. It passes (no rejection) unless BOTH
/// `ingestAffinity.key.source` is `canonicalTenant` and `ingestAffinity.backend`
/// is `ingressNginx`. The disjunction reads as "the combination is fine if any
/// precondition of the bad case is absent". `backend` is materialized to
/// `ingressNginx` by the schema default before CEL evaluates, so a manifest that
/// sets `key.source: canonicalTenant` while omitting `backend` is still rejected
/// (the omitted default IS `ingressNginx`); the `has()` guards are a defensive
/// belt against a missing serde default, matching the other injectors.
const CANONICAL_TENANT_BACKEND_RULE: &str = "!has(self.ingestAffinity) || \
    !has(self.ingestAffinity.key) || !has(self.ingestAffinity.key.source) || \
    self.ingestAffinity.key.source != 'canonicalTenant' || \
    !has(self.ingestAffinity.backend) || self.ingestAffinity.backend != 'ingressNginx'";

/// Attach the canonical-tenant-versus-legacy-backend guard (ADR-0080
/// decision 3) to the `gateway` property.
///
/// Attached at the `gateway` level and APPENDED (not replaced), the exact shape
/// of [`inject_gateway_exposure_affinity_guard`], so the exposure guard already
/// on `gateway` is preserved and `ingestAffinity`'s own rule set stays at just
/// its `headerName` rule. Injected post-hoc for the same reason the other
/// injectors are, and every step bails out rather than panicking if the schema
/// shape is not what it expects.
fn inject_canonical_tenant_backend_guard(crd: &mut CustomResourceDefinition) {
    let rule = ValidationRule {
        rule: CANONICAL_TENANT_BACKEND_RULE.to_string(),
        message: Some(CANONICAL_TENANT_BACKEND_MESSAGE.to_string()),
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
        let Some(gateway) = spec_props.get_mut("gateway") else {
            continue;
        };
        // Append rather than replace: the exposure guard is already on `gateway`
        // and dropping it would silently re-admit the exposure+ingressNginx
        // combination.
        let mut rules = gateway.x_kubernetes_validations.take().unwrap_or_default();
        rules.push(rule.clone());
        gateway.x_kubernetes_validations = Some(rules);
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
        // ADR-0072 decision 4: the CRD carries an optional
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
    fn ingest_affinity_is_optional_and_defaults_to_a_two_replica_subset() {
        // ADR-0076 decision 1. The block is optional: an existing RavelCluster
        // that never heard of affinity deserializes with None and therefore
        // renders no Ingress at all. Declaring it empty enables it with the
        // documented defaults, of which the load-bearing one is subsetSize = 2.
        let crd = ravel_cluster_crd();
        let gateway_props = crd.spec.versions[0]
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
            .expect("spec props")
            .get("gateway")
            .expect("gateway prop")
            .properties
            .as_ref()
            .expect("gateway props")
            .clone();
        assert!(
            gateway_props.contains_key("ingestAffinity"),
            "gateway must expose ingestAffinity in its schema"
        );

        let base = serde_json::json!({
            "image": "ravel:dev",
            "shards": 4,
            "storage": { "s3": { "bucket": "b", "credentialsSecretRef": { "name": "creds" } } }
        });
        let spec: RavelClusterSpec = serde_json::from_value(base.clone()).expect("deserialize");
        assert_eq!(spec.gateway.ingest_affinity, None);

        let mut with_affinity = base;
        with_affinity["gateway"] = serde_json::json!({ "ingestAffinity": {} });
        let spec: RavelClusterSpec = serde_json::from_value(with_affinity).expect("deserialize");
        let affinity = spec
            .gateway
            .ingest_affinity
            .expect("ingestAffinity present");
        assert!(affinity.enabled, "declaring the block enables affinity");
        assert_eq!(
            affinity.subset_size, DEFAULT_AFFINITY_SUBSET_SIZE,
            "the default subset is two replicas, not one"
        );
        assert_eq!(affinity.subset_size, 2);
        assert_eq!(affinity.key.source, AffinityKeySource::AuthorizationHeader);
        assert_eq!(affinity.key.header_name, None);
        assert!(affinity.grpc, "OTLP/gRPC is pinned by default");
        assert_eq!(affinity.hosts, Vec::<String>::new());
    }

    #[test]
    fn ingest_affinity_schema_carries_its_bounds_and_header_rule() {
        let crd = ravel_cluster_crd();
        let affinity = crd.spec.versions[0]
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
            .expect("spec props")
            .get("gateway")
            .expect("gateway prop")
            .properties
            .as_ref()
            .expect("gateway props")
            .get("ingestAffinity")
            .expect("ingestAffinity prop")
            .clone();

        // subsetSize must reject 0: a zero subset routes nowhere.
        let affinity_props = affinity.properties.as_ref().expect("affinity props");
        assert_eq!(
            affinity_props
                .get("subsetSize")
                .expect("subsetSize prop")
                .minimum,
            Some(1.0)
        );

        // headerName is constrained to HTTP token characters, so nothing
        // user-supplied can reach the rendered nginx directive as syntax.
        assert_eq!(
            affinity_props
                .get("key")
                .expect("key prop")
                .properties
                .as_ref()
                .expect("key props")
                .get("headerName")
                .expect("headerName prop")
                .pattern
                .as_deref(),
            Some(AFFINITY_HEADER_NAME_PATTERN)
        );

        // And source: header without headerName is refused at admission rather
        // than silently falling back to a different key.
        let rules = affinity
            .x_kubernetes_validations
            .as_ref()
            .expect("ingestAffinity must carry a validation rule");
        assert_eq!(rules.len(), 1);
        assert!(
            rules[0].rule.contains("headerName"),
            "rule must be about headerName: {}",
            rules[0].rule
        );
        assert_eq!(
            rules[0].message.as_deref(),
            Some(AFFINITY_HEADER_NAME_MESSAGE)
        );
    }

    #[test]
    fn affinity_backend_defaults_to_ingress_nginx_for_an_existing_cr() {
        // ADR-0080 decision 1: `backend` is additive. A CR serialized before
        // the field existed must deserialize onto the backend it already runs,
        // with subsetSize and key semantics untouched -- an upgrade that
        // silently moved either would be a behavior change.
        let affinity: IngestAffinitySpec = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "subsetSize": 3,
            "key": { "source": "header", "headerName": "x-tenant" },
            "ingressClassName": "nginx",
            "hosts": ["ingest.example.com"],
            "tlsSecretName": "ingest-tls",
            "grpc": true,
        }))
        .expect("legacy ingestAffinity JSON deserializes");
        assert_eq!(affinity.backend, AffinityBackend::IngressNginx);
        assert_eq!(affinity.subset_size, 3);
        assert_eq!(affinity.key.source, AffinityKeySource::Header);
        assert_eq!(affinity.key.header_name.as_deref(), Some("x-tenant"));

        assert_eq!(
            IngestAffinitySpec::default().backend,
            AffinityBackend::IngressNginx
        );

        // The schema exposes the field, so a CR can opt into the new backend.
        let affinity_props = ravel_cluster_crd().spec.versions[0]
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
            .expect("spec props")
            .get("gateway")
            .expect("gateway prop")
            .properties
            .as_ref()
            .expect("gateway props")
            .get("ingestAffinity")
            .expect("ingestAffinity prop")
            .properties
            .clone()
            .expect("affinity props");
        assert!(
            affinity_props.contains_key("backend"),
            "ingestAffinity must expose backend in its schema"
        );
    }

    #[test]
    fn affinity_backends_serialize_as_documented_camel_case() {
        // Public CRD enum values, quoted in the guide and in manifests.
        for (backend, text) in [
            (AffinityBackend::IngressNginx, "ingressNginx"),
            (AffinityBackend::RavelNative, "ravelNative"),
        ] {
            assert_eq!(
                serde_json::to_value(backend).expect("serialize"),
                serde_json::Value::String(text.to_string())
            );
        }
    }

    #[test]
    fn affinity_key_sources_serialize_as_documented_camel_case() {
        // These strings are the CRD's public enum values and appear in the
        // operations guide; renaming a variant silently would break manifests.
        for (source, text) in [
            (
                AffinityKeySource::AuthorizationHeader,
                "authorizationHeader",
            ),
            (AffinityKeySource::Header, "header"),
            (AffinityKeySource::MtlsSubject, "mtlsSubject"),
            (AffinityKeySource::CanonicalTenant, "canonicalTenant"),
        ] {
            assert_eq!(
                serde_json::to_value(source).expect("serialize"),
                serde_json::Value::String(text.to_string())
            );
        }
    }

    /// Rust mirror of [`CANONICAL_TENANT_BACKEND_RULE`]: returns whether the
    /// combination is ADMITTED (the CEL rule evaluates true). `backend` is
    /// always materialized when the affinity block is present (serde/schema
    /// default `ingressNginx`), so it is modeled as always present. The exact
    /// rule string is pinned separately in
    /// [`canonical_tenant_key_source_rejected_with_ingress_nginx_backend`], so a
    /// change to the real rule that diverges from this model is caught there.
    fn canonical_tenant_cel_admits(key_source: &str, backend: &str) -> bool {
        key_source != "canonicalTenant" || backend != "ingressNginx"
    }

    #[test]
    fn canonical_tenant_key_source_rejected_with_ingress_nginx_backend() {
        // ADR-0080 decision 3. Test venue: schema assertion plus a plain-Rust
        // logic mirror, the same venue #156/T2 established for this repo's CEL
        // rules (no envtest exists here; x-kubernetes-validations runs in the
        // API server, which a reconcile unit test cannot reach).

        // The one rejected combination:
        assert!(
            !canonical_tenant_cel_admits("canonicalTenant", "ingressNginx"),
            "canonicalTenant + ingressNginx must be rejected"
        );

        // Every allowed combination named in the deliverable:
        assert!(
            canonical_tenant_cel_admits("canonicalTenant", "ravelNative"),
            "canonicalTenant + ravelNative is allowed"
        );
        for backend in ["ingressNginx", "ravelNative"] {
            for source in ["authorizationHeader", "header", "mtlsSubject"] {
                assert!(
                    canonical_tenant_cel_admits(source, backend),
                    "{source} + {backend} is allowed"
                );
            }
        }

        // The schema carries the exact rule and message on the `gateway`
        // property (the same shape as the exposure guard), appended alongside it
        // rather than nested inside ingestAffinity.
        let gateway = ravel_cluster_crd().spec.versions[0]
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
            .expect("spec props")
            .get("gateway")
            .expect("gateway prop")
            .clone();
        let rules = gateway
            .x_kubernetes_validations
            .as_ref()
            .expect("gateway must carry CEL rules");
        let guard = rules
            .iter()
            .find(|r| r.rule == CANONICAL_TENANT_BACKEND_RULE)
            .expect("gateway must carry the exact canonical-tenant/backend rule");
        assert_eq!(
            guard.message.as_deref(),
            Some(CANONICAL_TENANT_BACKEND_MESSAGE)
        );
        // The exposure guard already on `gateway` is not dropped by appending.
        assert!(
            rules
                .iter()
                .any(|r| r.rule == GATEWAY_EXPOSURE_AFFINITY_RULE),
            "appending the canonical-tenant rule must not drop the exposure guard"
        );
        // ingestAffinity still carries only its own headerName rule (the
        // canonical-tenant rule lives on gateway, not here).
        let affinity_rules = gateway
            .properties
            .as_ref()
            .expect("gateway props")
            .get("ingestAffinity")
            .expect("ingestAffinity prop")
            .x_kubernetes_validations
            .clone()
            .unwrap_or_default();
        assert!(
            affinity_rules
                .iter()
                .all(|r| r.rule != CANONICAL_TENANT_BACKEND_RULE),
            "the canonical-tenant rule must live on `gateway`, not on `ingestAffinity`"
        );
    }

    #[test]
    fn router_image_is_optional_and_defaults_to_none() {
        // ADR-0080 decision 3, deliverable 1. A sibling of `backend`, optional
        // and skipped when None so an existing CR is byte-identical.
        assert_eq!(IngestAffinitySpec::default().router_image, None);

        let affinity: IngestAffinitySpec = serde_json::from_value(serde_json::json!({
            "backend": "ravelNative",
            "routerImage": "registry.example/ravel-ingest-router:v1"
        }))
        .expect("deserialize");
        assert_eq!(
            affinity.router_image.as_deref(),
            Some("registry.example/ravel-ingest-router:v1")
        );

        // Omitted routerImage deserializes to None, and the schema exposes it.
        let affinity: IngestAffinitySpec =
            serde_json::from_value(serde_json::json!({ "backend": "ravelNative" }))
                .expect("deserialize");
        assert_eq!(affinity.router_image, None);
        let affinity_props = ravel_cluster_crd().spec.versions[0]
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
            .expect("spec props")
            .get("gateway")
            .expect("gateway prop")
            .properties
            .as_ref()
            .expect("gateway props")
            .get("ingestAffinity")
            .expect("ingestAffinity prop")
            .properties
            .clone()
            .expect("affinity props");
        assert!(
            affinity_props.contains_key("routerImage"),
            "ingestAffinity must expose routerImage in its schema"
        );
    }

    #[test]
    fn shard_overrides_is_optional_and_lead_hours_defaults_to_the_minimum() {
        // #147, ADR-0076 decision 2. Absent by default, like retention: an
        // existing RavelCluster that never heard of shardOverrides
        // deserializes with None and the reconcile step (controller.rs) does
        // nothing. Declaring an empty tenants map still surfaces
        // leadHours == MIN_RESHARD_LEAD_HOURS.
        let crd = ravel_cluster_crd();
        let spec_props = crd.spec.versions[0]
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
            spec_props.contains_key("shardOverrides"),
            "spec must expose shardOverrides in its schema"
        );

        let base = serde_json::json!({
            "image": "ravel:dev",
            "shards": 4,
            "storage": { "s3": { "bucket": "b", "credentialsSecretRef": { "name": "creds" } } }
        });
        let spec: RavelClusterSpec = serde_json::from_value(base.clone()).expect("deserialize");
        assert_eq!(spec.shard_overrides, None);

        let mut with_overrides = base;
        with_overrides["shardOverrides"] = serde_json::json!({
            "tenants": { "tenant-a": 1 }
        });
        let spec: RavelClusterSpec = serde_json::from_value(with_overrides).expect("deserialize");
        let overrides = spec.shard_overrides.expect("shardOverrides present");
        assert_eq!(overrides.tenants.get("tenant-a"), Some(&1));
        assert_eq!(
            overrides.lead_hours, MIN_RESHARD_LEAD_HOURS,
            "omitting leadHours must default to the minimum, not zero"
        );
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

    /// The `gateway` schema property for `ravel_cluster_crd()`.
    fn gateway_schema_props() -> BTreeMap<
        String,
        k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::JSONSchemaProps,
    > {
        ravel_cluster_crd().spec.versions[0]
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
            .expect("spec props")
            .get("gateway")
            .expect("gateway prop")
            .properties
            .clone()
            .expect("gateway props")
    }

    #[test]
    fn exposure_is_optional_and_defaults_to_none_with_grpc_defaulting_true() {
        // ADR-0080 decision 2. The block is a new sibling of ingestAffinity and
        // is optional: an existing RavelCluster that never heard of exposure
        // deserializes with None, so it renders zero routes -- zero behavior
        // change. GatewaySpec::default() also carries exposure: None.
        assert_eq!(GatewaySpec::default().exposure, None);
        assert!(
            gateway_schema_props().contains_key("exposure"),
            "gateway must expose the exposure block in its schema"
        );

        let base = serde_json::json!({
            "image": "ravel:dev",
            "shards": 4,
            "storage": { "s3": { "bucket": "b", "credentialsSecretRef": { "name": "creds" } } }
        });
        let spec: RavelClusterSpec = serde_json::from_value(base.clone()).expect("deserialize");
        assert_eq!(spec.gateway.exposure, None);

        // Declaring gatewayApi with only a gatewayRef enables it with grpc
        // defaulting to true and empty hostnames, and the gatewayRef namespace
        // defaulting to None (rendered as the CR's own namespace).
        let mut with_exposure = base;
        with_exposure["gateway"] = serde_json::json!({
            "exposure": { "gatewayApi": { "gatewayRef": { "name": "public-gw" } } }
        });
        let spec: RavelClusterSpec = serde_json::from_value(with_exposure).expect("deserialize");
        let gateway_api = spec
            .gateway
            .exposure
            .expect("exposure present")
            .gateway_api
            .expect("gatewayApi present");
        assert_eq!(gateway_api.gateway_ref.name, "public-gw");
        assert_eq!(gateway_api.gateway_ref.namespace, None);
        assert!(gateway_api.grpc, "grpc defaults to true");
        assert_eq!(gateway_api.hostnames, Vec::<String>::new());
    }

    #[test]
    fn exposure_gateway_api_carries_the_cel_guard_at_the_gateway_property() {
        // ADR-0080 decision 2: the rule spans two sibling fields (exposure and
        // ingestAffinity), so it must sit on the `gateway` object itself, NOT
        // nested inside ingestAffinity where the headerName rule lives. A
        // reconcile unit test cannot exercise admission-time CEL at all; this
        // asserts the generated schema carries the correct rule and message.
        let crd = ravel_cluster_crd();
        let gateway = crd.spec.versions[0]
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
            .expect("spec props")
            .get("gateway")
            .expect("gateway prop")
            .clone();

        let rules = gateway
            .x_kubernetes_validations
            .as_ref()
            .expect("gateway must carry the exposure/affinity CEL rule");
        let guard = rules
            .iter()
            .find(|r| r.rule == GATEWAY_EXPOSURE_AFFINITY_RULE)
            .expect("gateway must carry the exact exposure/affinity rule");
        assert_eq!(
            guard.message.as_deref(),
            Some(GATEWAY_EXPOSURE_AFFINITY_MESSAGE)
        );
        // The rule keys off both sibling fields.
        assert!(guard.rule.contains("self.exposure.gatewayApi"));
        assert!(guard.rule.contains("self.ingestAffinity"));
        assert!(guard.rule.contains("ingressNginx"));

        // The rule is NOT nested inside ingestAffinity (that property keeps only
        // its own headerName rule).
        let affinity_rules = gateway
            .properties
            .as_ref()
            .expect("gateway props")
            .get("ingestAffinity")
            .expect("ingestAffinity prop")
            .x_kubernetes_validations
            .clone()
            .unwrap_or_default();
        assert!(
            affinity_rules
                .iter()
                .all(|r| r.rule != GATEWAY_EXPOSURE_AFFINITY_RULE),
            "the cross-field guard must live on `gateway`, not on `ingestAffinity`"
        );

        // The rule's `!has(...)` guards fail OPEN (admit) when `backend` or
        // `enabled` is omitted from a submitted manifest, relying entirely on
        // schema defaulting (which the API server applies before CEL runs) to
        // materialize `backend: ingressNginx` and `enabled: true` first. If
        // either default is ever removed, the rejected combination
        // (exposure.gatewayAPI + an omitted-affinity-config CR) silently
        // starts being admitted instead -- exactly the silent degrade this
        // rule exists to forbid -- with every other test here still green.
        // Pin both defaults so that regression fails loudly.
        let affinity_props = gateway
            .properties
            .as_ref()
            .expect("gateway props")
            .get("ingestAffinity")
            .expect("ingestAffinity prop")
            .properties
            .as_ref()
            .expect("ingestAffinity props");
        assert_eq!(
            affinity_props
                .get("backend")
                .expect("backend prop")
                .default
                .as_ref()
                .map(|j| &j.0),
            Some(&serde_json::json!("ingressNginx")),
            "the omitted-backend CEL case relies on this default"
        );
        assert_eq!(
            affinity_props
                .get("enabled")
                .expect("enabled prop")
                .default
                .as_ref()
                .map(|j| &j.0),
            Some(&serde_json::json!(true)),
            "the omitted-enabled CEL case relies on this default"
        );
    }

    /// Rust mirror of [`GATEWAY_EXPOSURE_AFFINITY_RULE`]: returns whether the
    /// combination is ADMITTED (the CEL rule evaluates true). `enabled` and
    /// `backend` are always materialized when the affinity block is present
    /// (serde/schema defaults), so they are modeled as present whenever the
    /// block is. A separate test
    /// ([`exposure_gateway_api_carries_the_cel_guard_at_the_gateway_property`])
    /// pins the exact rule string in the schema, so a change to the real rule
    /// that diverges from this model is caught there.
    fn cel_admits(exposure_gateway_api: bool, affinity: Option<(bool, &str)>) -> bool {
        if !exposure_gateway_api {
            return true; // !has(self.exposure.gatewayApi)
        }
        let Some((enabled, backend)) = affinity else {
            return true; // !has(self.ingestAffinity)
        };
        if !enabled {
            return true; // !self.ingestAffinity.enabled
        }
        backend != "ingressNginx"
    }

    #[test]
    fn cel_rule_rejects_only_exposure_plus_enabled_ingress_nginx_affinity() {
        // ADR-0080 decision 2. The one rejected combination:
        assert!(
            !cel_admits(true, Some((true, "ingressNginx"))),
            "exposure + enabled ingressNginx affinity must be rejected"
        );

        // Every allowed combination named in the deliverable:
        assert!(
            cel_admits(true, None),
            "exposure alone (affinity absent) is allowed"
        );
        assert!(
            cel_admits(true, Some((true, "ravelNative"))),
            "exposure + enabled ravelNative affinity is allowed"
        );
        assert!(
            cel_admits(false, Some((true, "ingressNginx"))),
            "ingressNginx affinity alone (no exposure) is allowed"
        );
        assert!(
            cel_admits(true, Some((false, "ingressNginx"))),
            "exposure + disabled ingressNginx affinity is allowed"
        );
        // And the remaining benign corner: exposure + disabled ravelNative.
        assert!(
            cel_admits(true, Some((false, "ravelNative"))),
            "exposure + disabled ravelNative affinity is allowed"
        );
    }
}
