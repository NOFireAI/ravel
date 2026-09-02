# Running Ravel on Kubernetes

Ravel runs on Kubernetes through an operator. You create one `RavelCluster`
custom resource, and the operator reconciles it into the gateway, query, and
maintain Deployments and their Services. This guide covers the local kind
development environment (the fastest way to see the whole thing work), the
`RavelCluster` field reference, and what the health probes mean.

**The custom resource is `v1alpha1`.** It makes no compatibility promise: the
schema can change in an incompatible way, with no conversion webhook, until it
is promoted to a stable version. Do not plan around it as a stable API.

Ravel's disposability model keeps the shape small. Every mode is stateless,
and object storage is the only durable state. There are therefore no
StatefulSets, no PersistentVolumeClaims, no leader election, and nothing to
back up besides the bucket. For the flag behind any custom-resource field, see
the generated
[server flag reference](../reference/ravel-server-flags.md); for how to choose
a value, [operations.md](operations.md).

**One of the three Deployments deletes objects, and it is the maintain one.**
Only a `maintain` mode process runs compaction, retention, the garbage-collection
sweep and the at-rest scrubber. The gateway and query Deployments never delete
anything. So a cluster with `maintain.enabled: false`, or one scaled to zero
maintain replicas, never compacts and never expires data: its L0 segments
accumulate unmerged and nothing is ever reclaimed, however the retention fields
below are set. That is a real operational state, not a degraded one the
operator reports, so it is worth checking before concluding that retention is
broken.

![Ravel Kubernetes operator reconcile loop](../diagrams/k8s-operator-reconcile.svg)

## The kind development environment

Three scripts bring up a complete cluster on your machine: the operator, a
fake S3 backend, and one reconciled `RavelCluster`.

```sh
scripts/kind-up.sh      # cluster, images, fake S3, operator, RavelCluster
scripts/kind-demo.sh    # OTLP ingest through the gateway, query back through
                        # the query Deployment, assert the value
scripts/kind-down.sh    # delete the cluster
```

You need `docker`, `kind`, `kubectl`, and (for `kind-demo.sh`) a Rust
toolchain. The first `kind-up.sh` run builds both container images from the
root `Dockerfile`. This is a full release build of the workspace and takes a
while. Later runs reuse the docker layer cache.

![kind local development environment](../diagrams/k8s-dev-environment.svg)

`kind-up.sh` does these steps in order:

1. It creates a kind cluster (default name `ravel-dev`) from a node image pinned
   by tag and digest. It reuses an existing cluster of that name.
2. It builds the `server` and `operator` targets of the root `Dockerfile`.
3. It runs `kind load docker-image` on both, so the cluster needs no registry
   and the `IfNotPresent` pull policy resolves against the node's own image
   store.
4. It deploys the fake S3 backend, waits for it to actually serve S3, and
   creates the `ravel` bucket.
5. It installs the CRD, RBAC, and operator Deployment from `deploy/k8s/operator/`.
6. It applies a `RavelCluster` named `dev`, pointed at that backend and those
   image tags.
7. It waits for `condition=Available` on the `RavelCluster`.

That last step is the meaningful one. The operator sets `Available=True` only
after the gateway and query Deployments report ready replicas. It therefore
succeeds only if the images really run and the pods really pass `/readyz`
against the real backend. It is not a check that objects were created.

If any step fails, the script dumps the namespace's objects, the
`RavelCluster`'s status, pod descriptions, and the operator's logs. It then
leaves the cluster running so you can look at it.

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `RAVEL_KIND_CLUSTER` | `ravel-dev` | kind cluster name. All three scripts read it. |
| `RAVEL_KIND_NODE_IMAGE` | pinned `kindest/node` | Control-plane version to test against. |
| `RAVEL_SERVER_IMAGE` | `ravel-server:kind-dev` | Server image tag. |
| `RAVEL_OPERATOR_IMAGE` | `ravel-operator:kind-dev` | Operator image tag. |
| `RAVEL_SKIP_IMAGE_BUILD` | `0` | `1` skips `docker build` and uses the two tags as-is; they must already exist in the local docker daemon. This is how CI reuses host-built binaries. |
| `RAVEL_FAKE_S3_BACKEND` | `floci` | `floci` or `minio`. |
| `RAVEL_TENANT_NAME` | `demo-tenant` | Tenant to provision. |
| `RAVEL_TENANT_TOKEN` | `demo-token` | Its bearer token. |

### The fake S3 backend

`deploy/k8s/floci.yaml` and `deploy/k8s/minio.yaml` are the same shape: a
single-replica Deployment, a Service, and a bucket-create Job. The Job retries
until the endpoint serves S3, creates the `ravel` bucket, and then verifies
that it exists rather than assume the create took.

floci is the default, gated by the `floci_contract` test in the object-store
crate. That test runs the full object-store contract suite plus the mandatory
capability and multipart probes against a real floci in CI. MinIO is the named
fallback: if a floci
release ever stops satisfying that contract, `RAVEL_FAKE_S3_BACKEND=minio`
switches the whole environment to the backend this repository has proven
longest. Ravel maintains both manifests regardless of which one is the default.

Neither is suitable for anything but development. There is no persistent
volume, so the bucket lives in the pod's ephemeral filesystem and is gone when
the pod restarts. floci also accepts any credentials without verifying
request signatures.

### Secrets

`kind-up.sh` creates the two Secrets that the `RavelCluster` references, rather
than commit them as manifests. A committed Secret manifest puts credentials in
git and invites someone to copy it into a real cluster.

- `ravel-s3-credentials`, keys `accessKeyId` and `secretAccessKey`.
- `ravel-tenant-tokens`, where each key is a tenant name and its value is that
  tenant's bearer token.

### The same environment in CI

The `k8s-integration` job in `.github/workflows/ci.yml` runs these same three
scripts in CI, so the local and CI paths cannot drift.

![k8s-integration CI lane vs local dev](../diagrams/k8s-ci-integration.svg)

There are two differences, both about build time, not about what is tested:

- `helm/kind-action` (pinned by commit SHA) creates the cluster, not
  `kind-up.sh`. The action reads its node image out of `kind-up.sh`, so there
  is only one pinned digest, and `kind-up.sh` then reuses that cluster. The job
  fails if the cluster it expects is not there, so a name drift cannot turn
  into a silently self-provisioned second cluster.
- The runner builds the two release binaries, where the workflow's cargo and
  sccache caches apply. `Dockerfile.prebuilt` (the runtime stages only) then
  assembles the images from them under `RAVEL_SKIP_IMAGE_BUILD=1`. The root
  `Dockerfile`'s builder stage would instead recompile the workspace inside
  Docker with no cache, which was measured at 57 minutes. The job smoke-runs
  `--help` in both assembled images before it creates the cluster. A binary
  that cannot exec on the runtime base then fails with the dynamic linker's
  message instead of as a `CrashLoopBackOff`. A build outside Docker has one
  consequence worth knowing about: binaries built on the runner need a newer
  glibc than the shipping image's Debian 12 base has, so the CI images use a
  Debian 13 distroless base instead. `Dockerfile.prebuilt` records the measured
  symbols and the alternatives.

`kind-demo.sh` asserts the round-trip value and exits nonzero on any failure,
so the job needs no extra proof-of-run check over its output.

## Installing the operator yourself

```sh
kubectl create namespace ravel-system
kubectl apply -f deploy/k8s/operator/crd.yaml
kubectl apply -f deploy/k8s/operator/rbac.yaml
kubectl apply -f deploy/k8s/operator/operator.yaml
```

Order matters: apply the CRD before the operator Deployment (its watch fails
until the cluster serves the `RavelCluster` kind), and RBAC before it too
(otherwise its API calls get 403). `operator.yaml` carries a placeholder
`ravel-operator:latest` image tag; point it at a real one.

`crd.yaml` is generated from the Rust spec types, not hand-written. To
regenerate it, run `cargo run -p ravel-operator -- --print-crd`.

The operator watches `RavelCluster` cluster-wide and manages Deployments and
Services in whatever namespace each `RavelCluster` lives in. Its ClusterRole
grants the full lifecycle of Deployments, Services, Ingresses,
`gateway.networking.k8s.io` HTTPRoutes/GRPCRoutes, and the ServiceAccounts,
Roles, and RoleBindings it renders for the `ravelNative` ingest router,
plus `RavelCluster` and its status subresource, `get` on Secrets, and
`get`/`list`/`watch` on `endpointslices` (needed to create the router's own
least-privilege Role). It never lists, writes, or watches Secrets.

## `RavelCluster` reference

Group `ravel.nofire.ai`, version `v1alpha1`, namespaced, short name `rc`.
`v1alpha1` makes no compatibility promise. The schema can change without
conversion webhooks until it is promoted.

A minimal example is in
[`deploy/k8s/examples/ravelcluster-dev.yaml`](../../deploy/k8s/examples/ravelcluster-dev.yaml).

| Field | Type | Default | Notes |
|---|---|---|---|
| `spec.image` | string | required | Server image for all three Deployments. |
| `spec.imagePullPolicy` | string | none | Standard Kubernetes values. |
| `spec.shards` | integer | required | Feeds `--shards` to the gateway, query, and maintain from one field, so nothing can break the must-match invariant. **Immutable after creation** through a CEL rule; use `spec.shardOverrides` for per-tenant resharding. |
| `spec.shardOverrides.leadHours` | integer | `2` | Minimum hours of lead time an override needs before its shard count takes effect, matching the resharding mechanism's activation-hour semantics. Rejected below the mechanism's own floor. |
| `spec.shardOverrides.tenants` | map | none | Per-tenant target shard count, tenant name to integer. A target that differs from the tenant's current active shard count drives a durable `append_generation` reshard; a target equal to the current count is a no-op. Lowering a tenant's shard count is the primary operator-facing cost control, and it has costs of its own (a single-actor throughput ceiling, shard-0 concentration, coarser maintenance units) documented in [shard-overrides.md](shard-overrides.md). |
| `spec.storage.s3.bucket` | string | required | |
| `spec.storage.s3.region` | string | `us-east-1` | |
| `spec.storage.s3.endpoint` | string | none | Omit for real AWS S3. Path-style addressing is always used. |
| `spec.storage.s3.credentialsSecretRef.name` | string | required | Secret with keys `accessKeyId` and `secretAccessKey`. |
| `spec.tenantTokensSecretRef.name` | string | none | Secret whose keys are tenant names and whose values are bearer tokens. |
| `spec.deploymentKeySecretRef.name` | string | none | Secret with one key, `key` (64 hex characters or 32 raw bytes): the deployment key. Enables the keyed tenant hash and `sys/auth` bearer-token reconciliation, see "`sys/auth` ownership" below. Omit to leave both off. |
| `spec.gateway.replicas` | integer | `1` | |
| `spec.gateway.resources` | object | none | `requests` / `limits` maps, as in a Pod spec. |
| `spec.gateway.fold.disabled` | boolean | `false` | `--disable-fold`. Fold is a query-cost optimization only; disabling it never changes results. |
| `spec.gateway.fold.intervalSecs` | integer | none | `--fold-interval-secs`. |
| `spec.gateway.ingestAffinity` | object | none | Layer-7 ingest affinity. Omit and nothing is rendered. Present, it pins tenant identity to a stable subset of gateway replicas, cutting flush PUTs by `replicas / subsetSize`, via one of two backends. Full reference, backend comparison, and sizing guidance in [ingest-affinity.md](ingest-affinity.md). |
| `spec.gateway.ingestAffinity.enabled` | boolean | `true` | `false` deletes the rendered objects and returns to the affinity-absent render. |
| `spec.gateway.ingestAffinity.backend` | string | `ingressNginx` | `ingressNginx` (deprecated, renders Ingress objects) or `ravelNative` (renders the `ravel-ingest-router` service). Omitting it keeps an existing CR's backend. |
| `spec.gateway.ingestAffinity.routerImage` | string | none | The `ravel-ingest-router` image. Required under `backend: ravelNative` (a different binary from `spec.image`); unset there degrades the router with reason `RouterImageMissing`. No effect under `ingressNginx`. |
| `spec.gateway.ingestAffinity.subsetSize` | integer | `2` | Replicas a tenant is pinned to. Two, not one, so a single replica loss does not concentrate a tenant on one process. A subset is a throughput ceiling; raise it for a high-volume tenant. |
| `spec.gateway.ingestAffinity.key.source` | string | `authorizationHeader` | `authorizationHeader`, `header` (with `key.headerName`), `mtlsSubject`, or `canonicalTenant` (requires `backend: ravelNative`). The key must come from authentication material: Ravel resolves tenancy server-side from the credential, so a URL path carries nothing routable. |
| `spec.gateway.ingestAffinity.ingressClassName` | string | none | Legacy `ingressNginx` only. |
| `spec.gateway.ingestAffinity.hosts` | list | `[]` | Legacy `ingressNginx` only. Empty renders one host-less rule. |
| `spec.gateway.ingestAffinity.tlsSecretName` | string | none | Legacy `ingressNginx` only. Renders `spec.tls`. Effectively required for OTLP/gRPC, which needs HTTP/2. |
| `spec.gateway.ingestAffinity.grpc` | boolean | `true` | Legacy `ingressNginx` only. Also render the OTLP/gRPC Ingress. |
| `spec.gateway.ingestAffinity.annotations` | map | `{}` | Legacy `ingressNginx` only. Extra Ingress annotations, merged before the affinity annotations. `nginx.ingress.kubernetes.io/proxy-body-size` belongs here: the ingress-nginx default of `1m` rejects larger OTLP/HTTP exports. |
| `spec.gateway.exposure.gatewayApi` | object | none | Gateway API exposure, independent of `ingestAffinity`. Renders `HTTPRoute`/`GRPCRoute` onto an existing `Gateway` instead of Ingress objects. Fields `gatewayRef.name`/`gatewayRef.namespace`, `hostnames`, `grpc` (default true). See [ingest-affinity.md](ingest-affinity.md). |
| `spec.query.replicas` | integer | `1` | |
| `spec.query.resources` | object | none | |
| `spec.maintain.enabled` | boolean | `true` | `false` deletes the maintain Deployment. |
| `spec.maintain.intervalSecs` | integer | none | `--maintain-interval-secs`. |
| `spec.maintain.resources` | object | none | |
| `spec.retention.default` | string | none | Duration string, e.g. `30d`. |
| `spec.retention.tenants` | map | none | Per-tenant overrides, tenant name to duration. |

There is deliberately no way to select the memory store. A non-durable
per-process store is incoherent across multiple pods, so `storage.s3` is
mandatory. There is also no field that can produce
`--dev-insecure-tenant-header`; the operator never sets it under any
configuration.

Tenant tokens are injected as env vars from the Secret and rendered into
`--tenant-token $(RAVEL_TENANT_TOKEN_<i>)=<tenant>` with kubelet `$(VAR)`
expansion, so token values never appear in the API object. They do still appear
in process argv on the node, because `ravel-server` reads tenant tokens from
flags and has no env or file token source. A checksum annotation on each pod
template rolls the pods when either Secret changes.

### `sys/auth` ownership

When `spec.deploymentKeySecretRef` is set, the operator also converges
`sys/auth`, the durable deployment-wide bearer-token map at the bucket root,
to `spec.tenantTokensSecretRef`'s current contents, every reconcile cycle.
This runs alongside, not instead of, `ravel-cli tenant token upsert|revoke`:
the two writers share the map, and each entry is tagged with who owns it.

- Every tenant present in the token Secret is upserted with
  `managed_by=operator`. A tenant present in `sys/auth` but absent from the
  Secret is revoked, but **only if** its entry is tagged
  `managed_by=operator`. A tenant provisioned by `ravel-cli tenant token
  upsert` (tagged `managed_by=cli` by default, or a value passed via
  `--managed-by`) is never touched by this pass, and neither is a v1-shaped
  entry with no `managed_by` field at all (unmanaged: written before this
  field existed, or deliberately declared unowned). The operator only ever
  removes what it itself put there.
- If the CRD sets a deployment key but no `tenantTokensSecretRef`, or the
  Secret resolves to zero tenants, the operator skips the whole `sys/auth`
  pass for that cycle, with no upserts and no removals, and logs a warning
  instead. An empty read is never treated as "revoke every operator-managed
  tenant."
- A reconcile against an unchanged token Secret performs zero `sys/auth`
  writes: each tenant's entry is compared against its current stored value
  first, and rewritten only on an actual difference.
- A `sys/auth` write is retried a bounded number of times against a
  concurrent writer (another operator replica, or a `ravel-cli` call racing
  it). If it still fails after that budget, the operator logs the failure
  and continues on to reconcile the Deployments and Services below:
  `sys/auth` reconciliation never blocks or fails the rest of the cycle.
- `spec.deploymentKeySecretRef`'s `resourceVersion` feeds the same
  pod-template secrets checksum as the token and credential Secrets (see
  "Tenant tokens are injected..." above), so rotating the deployment key
  rolls all three Deployments' pods, the same as rotating a tenant token or a
  credential does.

For the `sys/auth` format itself and `ravel-cli tenant token`'s own
subcommands, see
[operations/configuration.md](operations/configuration.md#tenancy-setup) and
the [CLI flag reference](../reference/ravel-cli-flags.md).

### Managed objects

For a `RavelCluster` named `dev`:

| Object | Kind | Notes |
|---|---|---|
| `dev-gateway` | Deployment | `--mode gateway`, RollingUpdate. |
| `dev-gateway` | Service | Ports 4318 (HTTP/OTLP/query API) and 4317 (OTLP/gRPC). |
| `dev-query` | Deployment | `--mode query`, RollingUpdate. |
| `dev-query` | Service | Port 4318. |
| `dev-maintain` | Deployment | `--mode maintain`, one replica, `Recreate` strategy. Absent when `maintain.enabled` is `false`. |
| `dev-gateway-ingest` | Ingress | OTLP/HTTP ingest under the tenant-affinity hash. Only under `ingestAffinity` enabled on `backend: ingressNginx`. |
| `dev-gateway-ingest-grpc` | Ingress | The same for OTLP/gRPC. Additionally absent when `ingestAffinity.grpc` is `false`. |
| `dev-ingest-router` | Deployment, Service, ServiceAccount, Role, RoleBinding | The `ravel-ingest-router` and its least-privilege RBAC. Only under `ingestAffinity` enabled on `backend: ravelNative`. See [ingest-affinity.md](ingest-affinity.md). |
| `dev-gateway-route` | HTTPRoute | Gateway API exposure. Only under `gateway.exposure.gatewayApi`, independent of the backend. |
| `dev-gateway-route-grpc` | GRPCRoute | The same for OTLP/gRPC. Absent when `exposure.gatewayApi.grpc` is `false`. |

Maintain is pinned to one replica with `Recreate` to avoid rolling-update
overlap. This is not an at-most-one guarantee, and correctness does not need
one. Because of the CAS commit protocol, a second concurrent maintainer cannot
corrupt committed state; it only wastes work.

### Status

```sh
kubectl get -n ravel-system ravelcluster dev -o jsonpath='{.status}'
```

`status` carries `observedGeneration`, `gatewayReadyReplicas`,
`queryReadyReplicas`, `maintainReadyReplicas`, and conditions. The operator
writes two condition types: `Available` and `Degraded`. It emits no
`Progressing` condition, so do not wait on one.

`Available=True` means the gateway and query Deployments both report ready
replicas. `kubectl wait --for=condition=Available` is therefore a usable
readiness gate for scripts and CI. If a reconcile fails (a missing Secret,
an apply error), the operator writes a `Degraded=True` condition with the
reason and flips `Available` to `False`. A `kubectl wait` then fails with an
explanation instead of timing out silently.

## Probe semantics

All three modes serve two routes on the HTTP port, and the operator points a
liveness probe and a readiness probe at them. The gRPC port has no health
service and gets no probe.

- `/healthz` (liveness): 200 whenever the HTTP listener is serving. It means
  the event loop is alive, and it never depends on store reachability, so a
  store outage cannot get healthy pods killed.
- `/readyz` (readiness): 200 after startup completes (config parsed, the store
  capability gate passed, listeners bound) and while the background
  store-reachability probe is healthy. 503 before startup completes, and after
  four consecutive failed probes until the next successful one.

`/-/healthy` and `/-/ready` are aliases for `/healthz` and `/readyz`, served by
the same handlers for clients that probe Prometheus' own paths. Either
spelling works in a probe.

`/readyz` performs **no object-store call per probe**. The kubelet reads an
in-memory value that one background probe per process maintains on
`--store-probe-interval`, so a store operation is never paid per kubelet probe
per pod, and a single transient blip cannot eject every pod from its Service
at once: four failures down, one success up. See
[readiness and the store reachability probe](operations/deployment.md#readiness-and-the-store-reachability-probe)
for the hysteresis and the two `/metrics` samples that make an outage
visible.

## Production notes

The kind environment is a development tool. A few things differ in a real
cluster.

- Point `spec.storage.s3.endpoint` at real S3 (or omit it) and supply real
  credentials in the Secret.
- Bucket lifecycle is the platform owner's job. The operator provisions no
  buckets; the create-bucket Jobs exist only in the dev manifests.
- The operator does not expose the query Service outside the cluster. It renders
  ingest exposure only when you ask for it: `gateway.ingestAffinity` on
  `backend: ingressNginx` renders an ingest Ingress, `backend: ravelNative`
  renders the subset router, and `gateway.exposure.gatewayApi` renders
  `HTTPRoute`/`GRPCRoute` onto a `Gateway` you provide (all in
  [ingest-affinity.md](ingest-affinity.md)). Otherwise add an Ingress or a
  `LoadBalancer` Service yourself. Either way put TLS in front of it: tenant
  tokens are bearer tokens.
- On a multi-replica gateway, consider turning on `gateway.ingestAffinity`.
  Ingest buffers are per replica, so a tenant spraying across every replica pays
  one flush stream per replica for the same data; object-storage request
  charges, not stored bytes, dominate the bill.

## Storage credential roles

By default a `RavelCluster` points all three Deployments at one Secret
(`spec.storage.s3.credentialsSecretRef`), so the gateway, query, and maintain
pods all use one bucket-wide S3 credential. You can hand each Deployment a
distinct, narrower storage credential role instead, so a leak from one can only
do what that mode legitimately does, and only the maintain Deployment can
delete anything at all.

Each of the operator's three Deployments maps to one storage credential role:

| Deployment | `--mode` | Storage credential role | Scope in one line |
|---|---|---|---|
| `<name>-gateway` | `gateway` | Gateway | Ingest writes (L0, commit records, idempotency, adopt) plus catalog fold writes, plus fleet-admission reconciliation snapshots. No delete. |
| `<name>-query` | `query` | Query | Reads commit and catalog objects, runs fold, appends query audit. No delete. |
| `<name>-maintain` | `maintain` | Maintain | Compaction, retention, sweep. The only one granted any delete, and only over `l0/`, `l1/`, `c/`, `idem/`. |

A fourth role, **Admin**, backs `ravel-cli` and is deliberately not managed by
the operator: there is no CRD field for it and no pod runs it. It is used only
by out-of-band operator/CI invocations. See
[the Admin credential](operations/deployment.md#the-admin-credential).

The exact per-role AWS IAM policy JSON, the MinIO equivalent for dev/CI, and
the first-deployment bootstrap notes all live in one place:
[storage credential roles](operations/configuration.md#storage-credential-roles).
This section covers only the Kubernetes wiring.

### Per-mode credential Secrets

Create one Secret per role you want to scope, each with the same two keys as
the shared Secret (`accessKeyId`, `secretAccessKey`), holding that role's
narrower access key:

```sh
kubectl create secret generic ravel-s3-gateway \
  --from-literal=accessKeyId=... --from-literal=secretAccessKey=...
kubectl create secret generic ravel-s3-query \
  --from-literal=accessKeyId=... --from-literal=secretAccessKey=...
kubectl create secret generic ravel-s3-maintain \
  --from-literal=accessKeyId=... --from-literal=secretAccessKey=...
```

Then reference each from its own Deployment with an additive
`credentialsSecretRef` field, alongside the existing shared one under
`spec.storage.s3`:

```yaml
apiVersion: ravel.nofire.ai/v1alpha1
kind: RavelCluster
metadata:
  name: prod
  namespace: ravel-system
spec:
  image: ravel-server:1.0.0
  shards: 8
  storage:
    s3:
      bucket: my-ravel-bucket
      region: us-west-2
      # Shared fallback. Any Deployment that omits its own credentialsSecretRef
      # below uses this one, exactly as in the single-credential model.
      credentialsSecretRef:
        name: ravel-s3-shared
  gateway:
    replicas: 3
    credentialsSecretRef:
      name: ravel-s3-gateway
  query:
    replicas: 3
    credentialsSecretRef:
      name: ravel-s3-query
  maintain:
    enabled: true
    credentialsSecretRef:
      name: ravel-s3-maintain
  tenantTokensSecretRef:
    name: ravel-tenant-tokens
```

The per-Deployment `spec.<mode>.credentialsSecretRef` fields are additive and
optional: omit one and that Deployment falls back to the shared
`spec.storage.s3.credentialsSecretRef`, unchanged. A `RavelCluster` that sets
no override at all runs one shared credential across all three Deployments, so
adopting the split needs no migration and can be rolled out one Deployment at
a time. Unlike the shared Secret, `kind-up.sh` does **not** create these
Secrets: the local kind environment deliberately keeps the single shared
credential for development convenience, because the per-role split is a
production hardening (see
[Storage credential roles](operations/configuration.md#storage-credential-roles)),
and `kind-up.sh` is not meant to be modified to adopt it. To exercise the
split in a kind cluster anyway, create the per-mode Secrets yourself the same
way as above (`kubectl create secret generic ...`) before applying a
`RavelCluster` that references them.

## Background

The operator's design, its condition set and its reconcile model are
[ADR-0034](../adrs/0034-k8s-operator.md). The per-mode storage credential
roles are ADR-0055; the deployment key and `sys/auth` ownership are ADR-0072;
per-tenant resharding is ADR-0052; ingest affinity and the Gateway API
exposure are ADR-0076 decision 1 and ADR-0080.
