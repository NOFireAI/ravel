# ADR-0034: Kubernetes operator, kind development environment, and k8s CI lane

Status: Proposed (2026-07-30). Deployment and operations tooling only: no
change to any frozen contract (RSEG layout, proto schemas, series
identity, commit tokens, object key layout) and no change to ingest or
query semantics.

## Context

`services/ravel-server` is one binary with `--mode all|gateway|query|
maintain`, configured entirely by CLI flags and env vars: listen
addresses (HTTP 4318, gRPC 4317), `--store memory|s3`, `--shards` (must
match across gateway and query), repeatable `--tenant-token
TOKEN=TENANT`, S3 endpoint/bucket/region/credentials, fold and maintain
intervals, retention defaults and per-tenant overrides. Every mode is
documented as disposable and stateless: object storage is the only
durable state, any process can be killed at any time, there is no leader
election and nothing to back up besides the bucket. SIGTERM is already
handled.

Two gaps matter for Kubernetes. First, no health endpoints exist;
docs/architecture.md claims maintain mode binds HTTP "for liveness only"
but that router is empty, so "liveness" today means a TCP accept.
Second, no deployment tooling of any kind exists: no Dockerfile, no
manifests, no chart. The only precedent is `deploy/docker-compose/
minio.yml` and the CI `object-store-contract` job, which runs MinIO via
`docker run`, waits with a retry loop, and runs the env-gated
`minio_contract` test.

Every mode enforces `Capabilities::mandatory()` at startup
(consistent_read, consistent_list, create_if_absent, cas_version,
suffix_range, upload_checksum, prefix_list); maintain additionally
requires multipart. `ravel-server` hardcodes `force_path_style: true`
and derives `allow_http` from the presence of a custom endpoint.

Floci, the requested fake S3 backend, is a LocalStack-class emulator
shipped as a single native-binary container, S3 on port 4566,
path-style addressing (compatible with the hardcoded path-style),
env-var configured. It has no upstream Helm chart, manifests, or
operator; wiring it into a cluster is this epic's work. Whether floci's
S3 implements Ravel's mandatory capability set (conditional PUT for
create_if_absent, cas_version, suffix range GET, multipart) is
unverified. That is the largest open risk in this design and is handled
explicitly in decision 8.

CI is 7 jobs on `ubuntu-latest`; a `free-disk-space` composite action
exists for disk-hungry jobs.

## Decision

![Ravel Kubernetes operator reconcile loop](../diagrams/k8s-operator-reconcile.svg)

1. **Operator in Rust with kube-rs.** New crate
   `services/ravel-operator` using `kube`, `kube-runtime`,
   `k8s-openapi`, and `schemars` (CRD schema derived from the Rust spec
   structs). The workspace gates apply unchanged: fmt, clippy with `-D
   warnings`, `unsafe` forbidden, no unwrap/expect in production paths,
   nextest. The reconcile loop is small (one CRD, stateless Deployments
   and Services), well within kube-rs's proven range. `kube`,
   `kube-runtime`, `k8s-openapi`, and `schemars` are genuinely new
   external dependencies and are flagged as such.

2. **One namespaced CRD, `RavelCluster`**, group `ravel.nofire.ai`,
   version `v1alpha1`. Spec fields map onto the existing flag surface:
   - `image`, `imagePullPolicy`.
   - `shards`: feeds both gateway and query args, so the must-match
     invariant is unrepresentable to break. Immutable after creation
     via a CEL validation rule; safe resharding is out of scope.
   - `storage.s3`: `bucket`, `region` (default us-east-1), optional
     `endpoint`, `credentialsSecretRef` (keys `accessKeyId`,
     `secretAccessKey`, injected as `RAVEL_S3_ACCESS_KEY` /
     `RAVEL_S3_SECRET_KEY` via `valueFrom`). The memory store is not
     representable in the CRD: a non-durable per-process store is
     incoherent across multiple pods. This is a deliberate mismatch
     with the CLI, stated here rather than papered over.
   - `tenantTokensSecretRef`: a Secret whose keys are tenant names and
     values are tokens. The operator injects each value as an env var
     from a `secretKeyRef` and renders `--tenant-token
     $(RAVEL_TENANT_TOKEN_<i>)=<tenant>` using kubelet `$(VAR)`
     expansion, so token values never appear in the API object. They
     do still appear in process argv on the node; a native env or file
     token source in ravel-server would close that and is a named
     follow-up, not part of this epic. A checksum annotation on the
     pod template rolls pods when the Secret changes.
   - `gateway`: `replicas`, `resources`, `fold` (`disabled`,
     `intervalSecs`).
   - `query`: `replicas`, `resources`.
   - `maintain`: `enabled` (default true), `intervalSecs`,
     `resources`. No replica field (see decision 3).
   - `retention`: `default` plus a per-tenant map.
   - `status`: `observedGeneration`, per-mode ready replicas,
     conditions `Available`, `Progressing`, `Degraded`.
   The operator never sets `--dev-insecure-tenant-header` under any
   configuration; there is no CRD field that can produce it.

3. **Managed objects.** Deployments, never StatefulSets: gateway and
   query as RollingUpdate Deployments with a Service each (gateway
   exposes 4318 and 4317; query exposes 4318), listening on 0.0.0.0.
   Maintain runs as a single-replica Deployment with the `Recreate`
   strategy. Reasoning on maintain concurrency rather than assertion:
   the CAS commit protocol means a second concurrent maintainer cannot
   corrupt committed state (the CAS loser fails and retries; orphaned
   uploads are collected later), so brief overlap during a node failure
   degrades to wasted work, inside the crash-restart envelope the store
   already tolerates. But sustained N>1 multiplies object-store traffic
   for zero throughput, and GC's age-window has been validated against
   crash-restart, not against a long-lived concurrent peer. So the
   operator pins maintain to one replica, uses Recreate to avoid
   rolling-update overlap, and claims no at-most-one guarantee
   (Kubernetes cannot give one without leases; correctness does not
   require one). One-shot `ravel-cli maintain` subcommands stay outside
   the operator's scope.

   **Superseded note (ADR-0065, issue #749).** The "sustained N>1
   multiplies object-store traffic for zero throughput" reasoning above
   no longer holds. ADR-0065 decisions 1 and 2 give every maintain
   process a self-owned heartbeat identity and partition
   `(tenant, signal, shard)` units across the live set by rendezvous
   hash, so a live N>1 divides the unit set instead of every replica
   re-walking all of it, with automatic takeover of a dead peer's units
   within `3 * heartbeat_interval`. The single-replica pin and Recreate
   strategy here were this decision's consequence, not an independent
   requirement, and are stale for that reason. The CRD's `maintain`
   block still has no `replicas` field (decision 3 above) and the
   Deployment strategy in this ADR is unchanged: widening the schema and
   switching the strategy is a follow-up for whoever next touches the
   operator, not done by EI-T5.

4. **Health endpoints are in scope**, as the epic's prerequisite server
   task. `/healthz` returns 200 whenever the HTTP listener is serving
   (liveness: the event loop is alive). `/readyz` returns 200 once
   startup has completed: config parsed, the store capability gate
   passed, listeners bound; 503 before that. `/readyz` performs no
   object-store call per probe: a store operation on every kubelet
   probe of every pod adds real S3 cost, and a transient S3 blip would
   eject every pod from its Service simultaneously, an availability
   policy that deserves its own decision. Continuous store health
   probing is a named follow-up, not silently dropped. Maintain mode
   serves both routes on `--listen-http`, which makes
   docs/architecture.md's liveness claim true; that doc is corrected in
   the same commit. All modes get HTTP liveness and readiness probes on
   the HTTP port (the gRPC port has no health service and gets none).

5. **Container images.** One multi-mode server image (`--mode` rendered
   from the CRD) and one operator image, from a single multi-stage
   Dockerfile with two final stages. Builder: pinned
   `rust:<version>-bookworm`; server built `--release --features sql`
   (`flight-sql` stays off while unimplemented); `ravel-cli` is
   included in the server image for one-shot maintain and inspection.
   Runtime base: `gcr.io/distroless/cc-debian12:nonroot` for both.
   Reasons: glibc, so no untested musl allocator behavior; the
   distroless images ship `ca-certificates`, so `object_store`'s TLS
   path works against real AWS S3 while plain-HTTP floci/MinIO
   endpoints need nothing; no shell or package manager in the runtime
   layer. Same image serves production S3 and the fake-S3 dev path.

6. **kind development environment.** Manifests under `deploy/k8s/`:
   `floci.yaml` (Deployment plus Service on 4566, plus a bucket-create
   Job mirroring the minio.yml sidecar), `operator/` (CRD, RBAC,
   operator Deployment), `examples/ravelcluster-dev.yaml`. Scripts
   following the existing conventions (`set -euo pipefail`, trap
   cleanup, `wait_for` retry loops, `ROOT_DIR`):
   - `scripts/kind-up.sh`: create a kind cluster from a pinned node
     image, build both images (env overrides accept prebuilt tags and
     skip the build), `kind load docker-image`, deploy floci and wait,
     run the bucket Job, install the operator, apply the sample
     `RavelCluster`, `kubectl wait` for the `Available` condition.
   - `scripts/kind-demo.sh`: port-forward gateway and query, run an
     OTLP ingest plus query round-trip and assert the value, the same
     shape as `scripts/demo.sh`.
   - `scripts/kind-down.sh`: delete the cluster.
   CI runs these same scripts, so the local and CI paths cannot drift.

7. **CI lane.** One new job, `k8s-integration`, in
   `.github/workflows/ci.yml` on `ubuntu-latest`. Steps: the existing
   `free-disk-space` action first (kind node image, two Ravel images,
   and floci together consume several GB), the shared Rust cache, host
   `cargo build --release` of the two binaries (so the cache applies),
   image assembly from a runtime-only Dockerfile target that copies the
   prebuilt binaries, cluster provisioning with `helm/kind-action@v1`
   (pinned by SHA like the other actions), then `scripts/kind-up.sh`
   with the prebuilt image tags and `scripts/kind-demo.sh`. The demo
   script exits nonzero unless the round-trip asserted, so no grep
   proof-of-run is needed. Budget expectation: this becomes the 8th and
   slowest job, dominated by the release build; roughly 15 to 25
   minutes warm-cache. Acceptable for one job; if it grows past that,
   splitting the binary build into a shared artifact job is the named
   remedy.

8. **Floci capability gate, first task of the epic.** Add an env-gated
   `floci_contract` test beside `minio_contract` in
   `crates/ravel-object-store/tests/contract.rs` (gated on
   `RAVEL_FLOCI_URL` etc., default-skip, identical shape), and a CI
   step in the existing `object-store-contract` job that launches floci
   via `docker run` with the same wait-loop pattern as MinIO. The test
   runs the full contract suite plus the `Capabilities::mandatory()`
   and multipart probes. Everything downstream treats the backend as
   endpoint-plus-credentials, so the fake backend is a one-manifest
   choice. Fallback, decided now rather than discovered later: if floci
   fails any mandatory capability or multipart, the kind environment
   and CI lane ship with MinIO (already proven in this repo's CI),
   `deploy/k8s/floci.yaml` becomes `minio.yaml`, the failing
   capabilities are reported upstream, and floci adoption becomes a
   follow-up gated on the same contract test going green. The epic does
   not block on floci.

9. **Documentation**, in the same commits as the behavior: a new
   `docs/guides/kubernetes.md` (operator install, CRD field reference,
   kind quickstart, probe semantics), a deployment section and the
   liveness correction in `docs/architecture.md` (which already governs
   `services/*` in the doc map), an index entry in `docs/README.md`, a
   README.md pointer, and a PROGRESS.md entry.

## Rejected alternatives

1. **Go operator (kubebuilder/controller-runtime).** The larger example
   ecosystem is real, but it would be the first non-Rust production
   code in the repo: a second toolchain in CI, and none of the
   workspace gates (clippy `-D warnings`, forbidden `unsafe`,
   no-unwrap) would cover it. This reconcile loop is one CRD producing
   stateless Deployments and Services; it does not need
   controller-runtime's breadth, and kube-rs (CNCF-hosted) covers it.

2. **Helm chart or kustomize instead of an operator.** Honestly, a
   chart gets a stateless system most of the way. It loses the parts
   this design leans on: making shards disagreement between gateway and
   query unrepresentable, shards immutability, the structural ban on
   `--dev-insecure-tenant-header`, rolling pods on token Secret change,
   and a status condition CI can `kubectl wait` on. The ask is also
   explicitly an operator.

3. **Per-mode CRDs** (RavelGateway, RavelQuery, RavelMaintain). Makes
   the must-agree fields (shards, storage, tenants) independently
   editable and therefore breakable; the single CRD removes that state
   space. No per-mode RBAC requirement exists to justify the split.

4. **StatefulSets or PVCs.** Every mode is documented disposable with
   object storage as the only durable state. There is no identity or
   volume to keep stable.

5. **Maintain as a CronJob of `ravel-cli` one-shots.** Duplicates the
   scheduler `--mode maintain --maintain-interval-secs` already
   implements in-process, and adds concurrencyPolicy, backoff, and job
   history knobs as new failure modes for no benefit.

6. **Per-mode container images.** Three images differing only in argv:
   triple the build and kind-load cost for zero isolation gain.

7. **scratch or alpine runtime base.** scratch has no CA bundle
   (breaks real AWS S3 TLS) and forces static musl; alpine brings a
   libc this workload has never been tested against. distroless/cc
   keeps glibc and certs without a shell.

8. **TCP-only probes for v1.** Cheapest, but a TCP accept on the empty
   maintain router proves nothing and would perpetuate the
   architecture.md fiction. The two routes in decision 4 are small and
   honest.

9. **LocalStack as the fallback fake S3.** The fallback must be the
   backend already known to pass the contract, which is MinIO in this
   repo's CI, not a second unverified emulator.

## Consequences

- A new crate `services/ravel-operator` exists; `kube`, `kube-runtime`,
  `k8s-openapi`, and `schemars` enter the workspace as new external
  dependencies.
- `ravel-server` gains `/healthz` and `/readyz`; the architecture doc's
  liveness claim becomes true. Deeper store-health probing and a
  non-argv tenant token source are named follow-ups.
- CI gains its slowest job. The free-disk-space action and host-side
  cargo cache bound it; splitting a shared binary-build job is the
  remedy if it degrades.
- The epic's outcome is independent of floci: worst case the
  environment ships on MinIO and floci lands later behind the contract
  test from decision 8, which remains the permanent gate for any
  backend swap.
- `v1alpha1` makes no compatibility promise; the CRD schema can change
  without conversion webhooks until it is promoted.
- The operator provisions no buckets outside the dev Job; production
  bucket lifecycle belongs to the platform owner.
- Shard count is immutable in the CRD; a resharding story, if ever
  needed, is a separate ADR.
