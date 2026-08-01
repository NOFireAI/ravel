# Running Ravel on Kubernetes

Ravel runs on Kubernetes through an operator. You create one `RavelCluster`
custom resource, and the operator reconciles it into the gateway, query, and
maintain Deployments and their Services. This guide covers the local kind
development environment (the fastest way to see the whole thing work), the
`RavelCluster` field reference, and what the health probes mean.

Ravel's disposability model makes this simple. Every mode is stateless, and
object storage is the only durable state. There are therefore no StatefulSets,
no PersistentVolumeClaims, no leader election, and nothing to back up besides
the bucket. For the full flag reference behind the CRD fields, see
[operations.md](operations.md). For the reason the design is shaped this way,
see [../adrs/0034-k8s-operator.md](../adrs/0034-k8s-operator.md).

![Ravel Kubernetes operator reconcile loop](../diagrams/k8s-operator-reconcile.svg)

## The kind development environment

Three scripts bring up a complete cluster on your machine: the operator, a
fake S3 backend, and one reconciled `RavelCluster`.

```sh
scripts/kind-up.sh      # cluster, images, fake S3, operator, RavelCluster
scripts/kind-demo.sh    # OTLP ingest through the gateway, query back through
                        # the query tier, assert the value
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

floci is the default. The `floci_contract` test in
`crates/ravel-object-store/tests/contract.rs` gates it. That test runs the full
object-store contract suite plus the `Capabilities::mandatory()` and multipart
probes against a real floci in CI. MinIO is the named fallback. If a floci
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
scripts on every pull request, so the local and CI paths cannot drift.

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
grants the full lifecycle of Deployments and Services, `RavelCluster` plus its
status subresource, and `get` on Secrets. It never lists, writes, or watches
Secrets.

## `RavelCluster` reference

Group `ravel.nofire.ai`, version `v1alpha1`, namespaced, short name `rc`.
`v1alpha1` makes no compatibility promise. The schema can change without
conversion webhooks until it is promoted.

A minimal example is in
[`deploy/k8s/examples/ravelcluster-dev.yaml`](../../deploy/k8s/examples/ravelcluster-dev.yaml).

| Field | Type | Default | Notes |
|---|---|---|---|
| `spec.image` | string | required | Server image for all three tiers. |
| `spec.imagePullPolicy` | string | — | Standard Kubernetes values. |
| `spec.shards` | integer | required | Feeds `--shards` to the gateway, query, and maintain from one field, so nothing can break the must-match invariant. **Immutable after creation** through a CEL rule; resharding is out of scope. |
| `spec.storage.s3.bucket` | string | required | |
| `spec.storage.s3.region` | string | `us-east-1` | |
| `spec.storage.s3.endpoint` | string | — | Omit for real AWS S3. Path-style addressing is always used. |
| `spec.storage.s3.credentialsSecretRef.name` | string | required | Secret with keys `accessKeyId` and `secretAccessKey`. |
| `spec.tenantTokensSecretRef.name` | string | — | Secret whose keys are tenant names and whose values are bearer tokens. |
| `spec.gateway.replicas` | integer | `1` | |
| `spec.gateway.resources` | object | — | `requests` / `limits` maps, as in a Pod spec. |
| `spec.gateway.fold.disabled` | boolean | `false` | `--disable-fold`. Fold is a query-cost optimization only; disabling it never changes results. |
| `spec.gateway.fold.intervalSecs` | integer | — | `--fold-interval-secs`. |
| `spec.query.replicas` | integer | `1` | |
| `spec.query.resources` | object | — | |
| `spec.maintain.enabled` | boolean | `true` | `false` deletes the maintain Deployment. |
| `spec.maintain.intervalSecs` | integer | — | `--maintain-interval-secs`. |
| `spec.maintain.resources` | object | — | |
| `spec.retention.default` | string | — | Duration string, e.g. `30d`. |
| `spec.retention.tenants` | map | — | Per-tenant overrides, tenant name to duration. |

There is deliberately no way to select the memory store. A non-durable
per-process store is incoherent across multiple pods, so `storage.s3` is
mandatory. There is also no field that can produce
`--dev-insecure-tenant-header`; the operator never sets it under any
configuration.

Tenant tokens are injected as env vars from the Secret and rendered into
`--tenant-token $(RAVEL_TENANT_TOKEN_<i>)=<tenant>` with kubelet `$(VAR)`
expansion, so token values never appear in the API object. They do still appear
in process argv on the node. A native env or file token source in `ravel-server`
is a known follow-up. A checksum annotation on each pod template rolls the pods
when either Secret changes.

### Managed objects

For a `RavelCluster` named `dev`:

| Object | Kind | Notes |
|---|---|---|
| `dev-gateway` | Deployment | `--mode gateway`, RollingUpdate. |
| `dev-gateway` | Service | Ports 4318 (HTTP/OTLP/query API) and 4317 (OTLP/gRPC). |
| `dev-query` | Deployment | `--mode query`, RollingUpdate. |
| `dev-query` | Service | Port 4318. |
| `dev-maintain` | Deployment | `--mode maintain`, one replica, `Recreate` strategy. Absent when `maintain.enabled` is `false`. |

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
writes two condition types today: `Available` and `Degraded`. ADR-0034 also
names `Progressing`, but the operator does not emit it yet, so do not wait on
it.

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
  the event loop is alive.
- `/readyz` (readiness): 200 after startup completes: config parsed, the store
  capability gate passed, listeners bound. 503 before that.

`/-/healthy` and `/-/ready` are aliases for `/healthz` and `/readyz`, served by
the same handlers for clients that probe Prometheus' own paths. Either
spelling works in a probe.

`/readyz` performs **no object-store call per probe**. This is deliberate. A
store operation on every kubelet probe of every pod costs real money against
real S3, and a transient S3 blip would eject every pod from its Service at the
same time. Continuous store-health probing is a separate decision and a
named follow-up, not an oversight.

## Production notes

The kind environment is a development tool. A few things differ in a real
cluster.

- Point `spec.storage.s3.endpoint` at real S3 (or omit it) and supply real
  credentials in the Secret.
- Bucket lifecycle is the platform owner's job. The operator provisions no
  buckets; the create-bucket Jobs exist only in the dev manifests.
- The operator does not expose the gateway Service or the query Service outside
  the cluster. Add an Ingress or a `LoadBalancer` Service yourself, and put
  TLS in front of it: tenant tokens are bearer tokens.
