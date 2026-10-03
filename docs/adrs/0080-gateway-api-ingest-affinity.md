# 0080. Gateway API exposure and Ravel-native subset affinity

Status: accepted

## Context

`spec.gateway.ingestAffinity` today has exposure (hostnames, TLS, HTTP/gRPC
routing, Ingress attachment) and affinity (subset-of-S tenant pinning) fully
conflated in one CRD struct and one reconcile path, both hard-wired to the
retiring `ingress-nginx` controller.

`IngestAffinitySpec` (`services/ravel-operator/src/crd.rs:208-332`, nested at
`GatewaySpec.ingest_affinity: Option<IngestAffinitySpec>`, `crd.rs:180`) holds
`enabled`, `subset_size` (default 2), `key: AffinityKeySpec` (source =
`AuthorizationHeader` default | `Header` | `MtlsSubject`, plus `header_name`),
and, mixed into the same struct, the purely exposure-shaped fields
`ingress_class_name`, `hosts`, `tls_secret_name`, `grpc`, and an `annotations`
passthrough. `desired_gateway_ingresses` (`reconcile.rs:965-991`) returns an
empty `Vec` whenever `ingest_affinity` is `None` or `enabled == false` — the
operator renders **no** Ingress at all for the non-affinity case today;
exposure for a plain deployment is entirely user-managed outside Ravel. There
is no Ravel-owned exposure concept independent of affinity to build on; this
ADR introduces one.

The reconcile path renders one `Ingress` per protocol
(`ingest_ingress`, `reconcile.rs:879-957`) because `nginx.ingress.kubernetes.io
/backend-protocol` is per-Ingress and ingress-nginx drops one of two Ingresses
sharing `(host, path)` as a duplicate — a real ingress-nginx limitation, not
an accidental design choice. Subset pinning is expressed entirely as three
annotations computed by `ingest_ingress_annotations` (`reconcile.rs:846-868`):
`upstream-hash-by` (a per-request nginx variable derived from the configured
key source, `affinity_hash_variable`, `reconcile.rs:813-837`),
`upstream-hash-by-subset: "true"`, and `upstream-hash-by-subset-size`;
`ingest_ingress_annotations` also always sets
`service-upstream: "false"` (`SERVICE_UPSTREAM_ANNOTATION`,
`reconcile.rs:846-868`), which is what makes subset hashing possible at
all — it tells ingress-nginx to balance directly across the Service's
endpoints rather than through the Service's cluster IP, since a per-tenant
subset can't be expressed against a single load-balanced VIP. This is
genuinely a stable-subset-of-S scheme, not ordinary single-backend consistent
hashing — `docs/guides/ingest-affinity.md:98-104` currently claims HAProxy,
Istio, and Envoy hash-based load balancing are "equivalent," which is false:
those configurations (as cited) give S=1 behavior. That claim is corrected as
part of this ADR's documentation work, not deferred.

No Gateway API type (`gateway.networking.k8s.io`, `HTTPRoute`, `GRPCRoute`)
appears anywhere in this repository today, and `k8s-openapi` does not vendor
that CRD group (it is installed separately from core Kubernetes), so any
Gateway API rendering needs hand-authored local Rust structs — the same
`#[derive(Serialize, Deserialize, JsonSchema)]`-without-`#[kube(...)]` pattern
the operator already uses for its own `Condition` type (`crd.rs:499-521`) to
avoid a hard dependency on that type living in `k8s-openapi`.

The operator's status convention (`controller.rs:838-920`) builds a
`Condition` (`type`, `status`, `observed_generation`, `reason`, `message`,
`last_transition_time`) via a `condition()` helper, and both `write_status`
(success path) and `write_degraded_status` (failure path) construct a fresh
`RavelClusterStatus{ conditions: vec![...] }` and PATCH it with
`Patch::Merge` (`patch_status`, `controller.rs:923-934`) — RFC 7386 JSON
Merge Patch, which replaces the `conditions` array wholesale rather than
merging it by `type`. There is no read-modify-write or upsert-by-type helper
today because at most two conditions (`Available`, optionally `Degraded`)
ever coexist and both are always known at the same call site. A third,
independently-lifecycled condition (`IngestAffinityBackendDeprecated`) cannot
be bolted on by copy-pasting into one call site alone: whichever status
writer runs on a given reconcile pass would silently drop it. The fix is not
a live read-back (race-prone against a concurrent status writer) but
computing the full condition set — `Available`/`Degraded` plus, when
applicable, the deprecation condition — once per reconcile pass, since the
reconcile function already knows this pass's affinity backend before either
status writer is called.

Canonical tenant identity: `TenantId` (`crates/ravel-types/src/lib.rs:47`) is
"resolved by authentication," and its resolution
(`services/ravel-server/src/tenant.rs:31`, `FallbackResolver::resolve`) runs
entirely inside `ravel-server`, per request, after trying an ordered list of
`TenantResolver` impls (Authorization/OIDC, dev-header, mTLS subject via
`MtlsResolver`). No upstream component resolves or forwards a pre-authenticated
tenant identity — there is no header or gRPC metadata key carrying one
between components today. This means canonical-tenant-ID hashing at the
routing layer (ahead of `ravel-server`) is only possible if the routing layer
runs the same resolution logic itself, not by trusting a wire signal nothing
currently produces.

The workspace already depends on `blake3 = "1.8"` (root `Cargo.toml:104`); no
other general-purpose hash crate (`xxhash`, `ahash`, `sha2`, `siphash`) is
present. No prior art exists in this repository for watching
`EndpointSlice`/`Endpoints` via kube-runtime (the only `.endpoints()` hits are
an unrelated `DistributedConfig` accessor in `ravel-sql`); a new watcher has
no local pattern to imitate.

## Decision

Split the concern into three layers, all additive to the existing CRD and
independently testable:

1. **Legacy backend, marked deprecated (phase 1).** The existing
   `ingress-nginx` rendering path keeps working exactly as-is —
   annotations, two-Ingress split, defaults — for any CR that already uses
   it. `IngestAffinitySpec` gains one new field:

   ```rust
   pub enum AffinityBackend {
       #[default]
       IngressNginx,
       RavelNative,
   }
   ```

   defaulting to `IngressNginx` so an existing CR's serialized state (which
   never set this field) deserializes to the same backend it already runs,
   with the same `subset_size` and `key` semantics — no upgrade silently
   changes either. When the effective backend is `IngressNginx`, the
   reconcile loop computes one additional condition alongside `Available`:

   ```rust
   condition(
       "IngestAffinityBackendDeprecated",
       true,
       observed_generation,
       "IngressNginxRetired",
       "ingestAffinity is using the ingress-nginx backend, which is \
        retired upstream; migrate to backend: ravelNative or an \
        equivalent Gateway API exposure (see docs/guides/ingest-affinity.md)",
   )
   ```

   Both `write_status` and `write_degraded_status` gain an
   `extra_conditions: Vec<Condition>` parameter appended to the vec they
   already build, populated by the reconcile function once per pass — no
   read-modify-write against live cluster state, so no race with a
   concurrent status writer. This is a mechanical, additive change to two
   existing call sites, not a new merge-by-type helper (unnecessary while
   at most three conditions ever coexist and all are known upfront).

2. **Exposure, separated from affinity (phase 2).** `GatewaySpec` gains a new
   optional sibling to `ingest_affinity`:

   ```rust
   pub struct GatewayExposureSpec {
       pub gateway_api: Option<GatewayApiExposureSpec>,
   }
   pub struct GatewayApiExposureSpec {
       pub gateway_ref: GatewayReference,   // namespace/name of an existing Gateway
       pub hostnames: Vec<String>,
       pub grpc: bool,                       // default true, mirrors today's IngestAffinitySpec.grpc
   }
   ```

   `exposure` is independent of `ingest_affinity`: it can be set with
   affinity disabled (plain Gateway API HTTP/gRPC routing, no tenant
   pinning), or combined with `backend: RavelNative`. It cannot be combined
   with `backend: IngressNginx` while affinity is enabled: the Gateway API
   path and the annotation-driven Ingress path are two independent routes
   to the same Service, and traffic on the Gateway API route would bypass
   the nginx subset annotations entirely — affinitized on one path,
   unpinned on the other, with no signal to the operator. That is exactly
   the silent degradation the epic's non-goals forbid, so it is rejected
   at admission rather than left as a footgun: a CEL validation rule (the
   same mechanism already guarding `headerName`, `crd.yaml:155-156`)
   rejects a CR that sets `exposure.gatewayApi` while
   `ingestAffinity.enabled && backend == IngressNginx`. The rule spans two
   sibling fields (`exposure` and `ingestAffinity`) rather than validating
   one struct against its own fields as the `headerName` precedent does,
   so it is attached at the `gateway` object level, not nested inside
   `ingestAffinity`; it also relies on `backend`'s schema default
   (`ingressNginx`) being materialized on the object before CEL evaluates
   it, which `#[derive(CustomResource)]`'s generated default already
   guarantees. Because `x-kubernetes-validations` runs in the API server
   at admission, not in the operator process, this rule is proven by an
   envtest (or an equivalent direct CEL-expression evaluation against the
   generated schema) rather than a `ravel-operator` reconcile unit test —
   a reconcile test cannot exercise admission-time rejection at all. The
   operator renders
   one `HTTPRoute` and, when `grpc` is true, one `GRPCRoute` — both standard
   `gateway.networking.k8s.io` kinds, no vendor extension — with
   `parentRefs` pointing at `gateway_ref` and `backendRefs` pointing at
   whichever Service currently serves ingest traffic for this CR (the
   gateway Service directly when affinity is off or using
   `RavelNative`'s own Service; see below). A single `HTTPRoute` and a
   single `GRPCRoute` can each carry all configured hostnames and both sets
   of paths without the two-object identity collision that forced
   ingress-nginx's per-protocol split — Gateway API's route-merge model
   doesn't have that limitation. `IngestAffinitySpec.hosts`,
   `.tls_secret_name`, and `.ingress_class_name` are kept, unchanged, as the
   legacy backend's own exposure config (they only apply when
   `backend: IngressNginx`) — not removed, not renamed, documented as
   legacy-backend-only in the same commit that adds `exposure`.

3. **Ravel-native subset affinity (phase 3).** A new library crate,
   `crates/ravel-affinity`, implements deterministic rendezvous (HRW)
   top-K subset selection with no Kubernetes dependency:

   ```rust
   pub fn rank<'a>(tenant_key: &[u8], replicas: &'a [ReplicaId]) -> Vec<&'a ReplicaId>;
   pub fn subset<'a>(tenant_key: &[u8], replicas: &'a [ReplicaId], size: usize) -> Vec<&'a ReplicaId> {
       rank(tenant_key, replicas).into_iter().take(size).collect()
   }
   ```

   `rank` scores every `(tenant_key, replica_id)` pair via `blake3`
   (reusing the workspace's existing hash dependency rather than adding a
   second one) and returns *all* replicas sorted by score, ties broken by
   replica ID so ordering of the input slice never affects the result;
   `subset` is the top-`size` convenience wrapper used for the CRD's
   `subsetSize` semantics. The router needs the full ranking, not just the
   top `size`, because its unready-member fallback (below) walks past
   position `size` into the rest of the order — a function that only ever
   returned the top `size` couldn't support that without re-hashing.
   `size >= replicas.len()` returns all replicas (sorted by score, not
   truncated); `size == 1` is the plain HRW single-winner case. Adding or
   removing one replica moves only the tenants whose score ranking crosses
   the boundary at position `size` — the standard HRW guarantee.

   `ReplicaId` is the gateway pod's identity as reported by the
   `EndpointSlice` (pod UID, not IP — an IP can be reused by a different
   pod after churn). The limited-reassignment guarantee holds for scale
   events (one endpoint added or removed while the rest are unchanged); it
   does not and cannot hold across a full rolling update of the gateway
   Deployment, since every pod's UID changes at once and the entire
   replica set is new by definition — this is inherent to any
   identity-keyed subset scheme, not a gap specific to this design, and is
   documented as an operational property (a gateway rollout causes a
   one-time full reassignment, not a bug) rather than glossed over.
   `crates/ravel-tenant-resolve`
   is extracted from `services/ravel-server/src/tenant.rs` as a mechanical
   move (own commit, no behavior change, existing tenant-resolution tests
   port with it) so both `ravel-server` and the new
   `services/ravel-ingest-router` depend on one resolver implementation
   rather than two that can drift. `AffinityKeySource` gains a
   `CanonicalTenant` variant (in addition to the existing
   `AuthorizationHeader`/`Header`/`MtlsSubject`, all preserved unchanged)
   that resolves the tenant via `ravel-tenant-resolve` and hashes the
   resulting `TenantId`'s bytes — immune to bearer-token rotation, unlike
   hashing the raw `Authorization` header. `services/ravel-ingest-router`
   is a new, horizontally scalable service: it watches `EndpointSlice`
   objects (kube-runtime, no prior art in this repo, built fresh) for the
   gateway Service, computes the subset via `ravel-affinity` per request,
   and proxies HTTP and gRPC **directly to the chosen pod endpoint address
   from the EndpointSlice** — never through the gateway Service's
   cluster-IP, which would hand the connection back to kube-proxy's own
   load balancing and undo the subset selection the router just computed.
   Within the S-member subset the router picks by local round-robin,
   skipping any member its own `EndpointSlice` view marks not-Ready; if
   fewer than `S` members are Ready, it falls further down the same
   HRW-ranked order (position `S+1`, `S+2`, ...) rather than narrowing to
   an unbalanced smaller set. Canonical-tenant resolution needs the same
   auth configuration `ravel-server` already receives (OIDC issuer/JWKS,
   mTLS CA, dev-header allow-list) — the operator threads the identical
   Secret/ConfigMap references already wired into the gateway Deployment's
   env into the router Deployment's env too, rather than inventing a
   second config surface. If `key.source: CanonicalTenant` and resolution
   fails for a request, the router rejects it (401 / gRPC
   `UNAUTHENTICATED`) rather than falling back to an uncanonical key —
   fail-closed, consistent with this repo's existing fail-closed
   conventions (ADR-0050), and never silently routes on a weaker key than
   configured. When
   `backend: RavelNative`, the operator renders this service's
   Deployment/Service (parameterized with `subset_size` and `key`) and, if
   `exposure.gateway_api` is set, points the rendered `HTTPRoute` at the
   router's Service instead of the gateway Service directly; the `GRPCRoute`
   continues to target the gateway Service, because the router serves HTTP
   only today (a single `port_name` per process cannot proxy HTTP and gRPC
   to the gateway's distinct listener ports at once) — wiring gRPC through
   the router (a per-listener-port surface or a two-Deployment split) is
   tracked as follow-up #194. It also renders a namespaced `Role`/`RoleBinding` granting the
   router's `ServiceAccount` `get`/`list`/`watch` on `endpointslices` and
   `services` in that namespace only — least-privilege, consistent with
   this repo's existing per-tenant/per-role credential scoping conventions
   (ADR-0055, ADR-0072), not a cluster-wide grant.

   Switching a CR's `backend` (or adding/removing `exposure`) means the
   reconcile loop's delete-sweep, today scoped only to the two possible
   Ingress names (`possible_ingest_ingress_names`, `reconcile.rs:795-800`),
   is generalized to a per-kind "possible managed child names" list
   covering Ingress, HTTPRoute, GRPCRoute, and the router
   Deployment/Service/Role/RoleBinding, so switching modes cleans up
   whichever objects the previous mode created instead of leaving them
   orphaned.

![Exposure and affinity, decoupled: the CR's exposure.gatewayApi and ingestAffinity fields feed independent rendering paths — standard HTTPRoute/GRPCRoute to a user-provided Gateway, versus legacy Ingress (deprecated) or the new ravel-ingest-router, both of which dial gateway pods directly rather than through the Service VIP.](assets/0080-architecture.svg)

![ravel-affinity's rendezvous (HRW) subset selection: a canonical tenant ID and the Ready replica set both feed a per-pair blake3 score; rank() produces the full deterministic order, subset() takes the top S, and the router picks one member, falling back through the rank order if a subset member isn't Ready.](assets/0080-hrw-flow.svg)

## Rejected alternatives

**Translate the nginx annotations 1:1 onto another vendor's subset/hash
extension (e.g. a controller-specific `BackendTrafficPolicy` or LB CRD).**
Rejected: swaps one hard vendor dependency for another and leaves the epic's
own goal (2) and non-goal ("replace ingress-nginx with another
vendor-specific controller and call the migration complete") unmet. The
whole point of owning subset selection in `ravel-affinity` is that it no
longer matters which Gateway implementation terminates the connection.

**Present a Gateway-implementation's single-backend consistent hashing
(session-persistence extensions, Envoy Gateway's `consistentHash`, Istio's
`DestinationRule` hash LB) as the migrated equivalent of subset affinity.**
Rejected outright by the epic's non-goals. It is a real, useful mode
(`S=1`), but it is not what `subsetSize: 2` means and is documented as a
distinct, weaker option, never substituted silently.

**Have the router trust an in-band header carrying a pre-resolved tenant ID
from an upstream identity-aware proxy.** Rejected: no such component exists
in this architecture — the audit found tenant resolution happens only
inside `ravel-server`, per request. Inventing an upstream identity gateway
is a materially larger, separately-scoped change with its own trust-boundary
questions (who is allowed to set that header, over what transport). Instead
the router runs the same resolver logic via a shared crate.

**Duplicate tenant resolution logic into the router by copy-paste instead of
extracting `ravel-tenant-resolve`.** Rejected: the two copies drift the
moment either one gains a resolver or changes precedence, which silently
breaks the property canonical-tenant hashing exists to provide (affinity
survives token rotation) the day the copies disagree about what the
canonical tenant is.

**Default new CRs to `backend: RavelNative` immediately.** Rejected: this
epic's deliverable 7 requires that upgrades never silently change
`subsetSize` or key semantics, and a new default is indistinguishable from a
silent behavior change for anyone applying a CR without an explicit opinion.
`RavelNative` becomes the *recommended*, documented choice for new
deployments (phase 4) without ever being the implicit default; the schema
default stays `IngressNginx` for backward compatibility until the phase 5
removal criteria (a future ADR) are met.

**Restructure `ingestAffinity` into a fully incompatible new schema shape
(breaking the CRD).** Rejected per deliverable 3's explicit guidance:
additive fields (`backend`, the new `exposure` sibling, the `CanonicalTenant`
key source) meet every goal here without forcing existing CRs to be rewritten
or the CRD version bumped.

## Consequences

- `IngestAffinitySpec` gains `backend` (default `IngressNginx`) and
  `AffinityKeySource` gains `CanonicalTenant`; both are purely additive to
  the generated `crd.yaml` OpenAPI schema. No existing field is renamed,
  removed, or has its default changed.
- `GatewaySpec` gains `exposure: Option<GatewayExposureSpec>`, independent of
  `ingest_affinity`. A CR that sets neither continues to get exactly zero
  operator-rendered networking objects, as today.
- Two new crates enter the workspace: `ravel-affinity` (pure library, no k8s
  dependency, the deterministic-selection surface that carries most of this
  ADR's test burden) and `services/ravel-ingest-router` (a new deployable
  service with its own RBAC, Deployment, and Service, rendered by the
  operator only when `backend: RavelNative`). `ravel-tenant-resolve` is
  extracted from `ravel-server` as a mechanical, behavior-preserving move.
- The reconcile delete-sweep generalizes from a fixed two-Ingress-name list
  to a per-kind list covering Ingress, HTTPRoute, GRPCRoute, and the router's
  Deployment/Service/Role/RoleBinding, so mode switches clean up fully.
- `write_status`/`write_degraded_status` gain an `extra_conditions`
  parameter; every existing call site is updated to pass an empty vec except
  the one new call site that computes the deprecation condition. This is a
  signature change inside `ravel-operator` only, not a public-crate API used
  elsewhere.
- `docs/guides/ingest-affinity.md` is rewritten in the same change that adds
  `backend` and `exposure`, removing the false equivalence claim at
  lines 98-104 and adding the legacy/Gateway-API/Ravel-native/S=1-vs-S>1
  distinctions the epic requires; it is not deferred to a later phase.
- Existing ingress-nginx users see no behavior change until they opt into
  `backend: RavelNative` or add `exposure.gatewayApi` themselves; they gain
  only the new `IngestAffinityBackendDeprecated` condition on their
  `RavelCluster` status.
- Phase 5 (removal criteria and version boundary for the ingress-nginx
  rendering path) is explicitly out of this ADR's decision — it depends on
  migration telemetry this ADR doesn't yet produce — and is deferred to a
  follow-up ADR once `RavelNative` has field experience.
- Gateway API exposure terminates TLS at the user's own `Gateway` listener,
  not a Ravel-rendered resource — `IngestAffinitySpec.tls_secret_name`
  stays legacy-backend-only and has no Gateway API equivalent to carry
  forward automatically; the migration doc must tell a migrating user to
  configure their listener's `tls.certificateRefs` themselves, pointing at
  the same or an equivalent Secret.
- Multiple `ravel-ingest-router` replicas watch `EndpointSlice`
  independently; between one replica observing a membership change and
  another catching up, two replicas can transiently compute different
  subsets for the same tenant. This is bounded by informer resync/watch
  latency (seconds), self-heals without intervention, and has no
  durability or correctness impact — it is a routing decision, not data
  correctness — but is worth naming so it isn't mistaken for a bug during
  rollout.
- Test coverage this ADR commits to, matching the epic's deliverable 8:
  `ravel-affinity` gets property tests for determinism (same inputs, same
  output), ordering-independence (shuffled replica input, same result),
  and bounded reassignment on single add/remove (only tenants whose rank
  crosses position `S` move); the CEL rejection of `exposure.gatewayApi` +
  legacy `backend` is proven by an envtest against the real API server (or
  a direct CEL-expression evaluation over the generated schema), since
  `x-kubernetes-validations` runs at admission and no reconcile-loop unit
  test can exercise it; `ravel-operator` reconcile tests separately cover
  HTTPRoute and GRPCRoute rendering, the delete-sweep across all four
  resource kinds on mode switch, and the `IngestAffinityBackendDeprecated`
  condition appearing/disappearing correctly across both status-writer
  paths;
  `ravel-ingest-router` gets integration tests proving it dials pod
  endpoints directly (not the Service VIP), falls through to the next
  HRW-ranked replica when a subset member is unready, and rejects a
  request on `CanonicalTenant` resolution failure rather than falling
  back to a weaker key.

## Refs

Refs: #150
