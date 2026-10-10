# Ingest affinity

Ingest affinity pins each tenant to a small, stable subset of gateway
replicas. The same data is then flushed once per subset and not once per
replica. Request charges, not stored bytes, dominate the object-storage bill
of Ravel. The number of requests scales with the number of gateway replicas
that the writes of a tenant land on.

Affinity is configuration. It changes no format, no contract, and no
acknowledgement latency. The *routing mechanism* comes in layers: a
deprecated ingress-nginx path, a Ravel-owned router, and a separate Gateway
API exposure concept. The layers share one semantic contract.

## Why it saves requests

An ingest buffer is per `(tenant, signal, shard)` **per replica**. Every
buffer flushes on its own age timer, and every flush issues a data PUT plus a
commit PUT. A tenant whose exporters reach all `R` gateway replicas keeps `R`
independent flush streams alive for one logical stream of data. It pays `R`
times the PUT pairs:

```
PUTs/day = 2 x tenants x signals x shards x replicas x (86400 / age_threshold_s)
```

A subset of size `S` replaces `replicas` with `S` in that product for that
tenant. At 10 replicas and the default subset of 2, that is a
**5x reduction** in flush PUTs for every tenant. There is no latency cost:
each replica still acknowledges a strict write when its own commit PUT
returns.

The saving multiplies with the other levers on the same bill (shard count,
flush cadence), because they are different terms of the same product.

There is a read-side benefit too. Fewer, larger L0 objects mean fewer
open-hour segments for a query to open, which lowers the per-query request
budget.

### Shards stay shared

Affinity narrows which *replicas* a tenant reaches, not which *shards*.
Within a replica, the series of a tenant still hash across all shards of
that replica. Affinity therefore does not isolate a tenant from a per-shard
object-store stall.

The store applies `503 SlowDown` per key prefix. When the key prefix of one
tenant is throttled, its stalled flushes hold up to
`--max-inflight-flushes-per-tenant` of the permits on that shard (default 3 of
4). Once stalled tenants hold every permit, the flushes of co-resident tenants
queue behind them on every replica in the subset. A smaller or different
subset does not change that. The shard actor
keeps running, so the writes of those tenants are still accepted and their
age triggers still fire. They wait for a permit to flush on.

The control for cross-tenant flush isolation on a shard is
`max_inflight_flushes` together with its per-tenant share (see
[Shard actor](../ingest.md#shard-actor)). The subset size and the shard count
do not control it. Under the operator, set `spec.gateway.maxInflightFlushes`
and `spec.gateway.maxInflightFlushesPerTenant` on the `RavelCluster` (see
[kubernetes.md](kubernetes.md)). They render `--max-inflight-flushes` and
`--max-inflight-flushes-per-tenant` onto the gateway Deployment; the server
defaults are 4 and `max(1, N - 1)`.

## What it costs

**The subset bounds the ingest throughput of a tenant.** This cost is
structural. A tenant pinned to 2 replicas gets the CPU, memory, and network
of 2 pods, for any size of the gateway Deployment. If the traffic of that
tenant outgrows 2 pods, use a larger subset. A larger Deployment does not
help.

Two smaller costs:

- **Memory concentrates.** A subset holds the buffers for every tenant
  hashed onto it. A subset that draws several large tenants carries more
  buffered bytes than an evenly loaded replica. The process-wide ingest
  buffer budget still caps this, and a hot subset reaches the cap sooner.
- **Load is only as even as the hash.** With a handful of tenants, the
  distribution across subsets is uneven. Affinity pays off with many
  tenants, not with three.

Affinity does not affect correctness. It is best-effort: `writer_id` and
`epoch` already disambiguate concurrent writers in the object key. A request
that lands on a replica outside its usual subset writes a valid object. A
reroute costs requests and never costs correctness.

## Five separate concepts

This guide uses the word "affinity" for five distinct things. They are
independent, and they ship at different maturities. An operator who confuses
them can expect subset pinning from a cluster that has `S=1`, or none at
all.

- **(a) The affinity semantic contract.** Subset-of-`S` pinning: the writes
  of a tenant reach a stable set of `S` replicas, chosen by a hash of tenant
  identity. This is what `subsetSize` *means*. The contract is independent
  of any implementation.
- **(b) The legacy `backend: ingressNginx` implementation.** The operator
  renders `Ingress` objects that carry the `upstream-hash-by-subset`
  annotation family of ingress-nginx. It works and is unchanged. It is
  **deprecated**, because ingress-nginx is retiring upstream.
- **(c) Gateway API exposure (`gateway.exposure.gatewayApi`).** A separate
  field, independent of affinity. It renders standard
  `HTTPRoute`/`GRPCRoute` objects onto a `Gateway` that you already run. It
  is *exposure*: by itself it pins nothing.
- **(d) `backend: ravelNative`, the subset router of Ravel.** A horizontally
  scalable service, `ravel-ingest-router`, that watches EndpointSlices,
  computes the subset with rendezvous hashing, and dials gateway pods
  directly. It delivers the (a) contract with no dependency on any ingress
  or Gateway implementation.
- **(e) Single-backend consistent hashing.** What most Gateway API and mesh
  implementations offer natively (ring-hash, Maglev, `consistentHash`). It
  maps one key onto **one** backend. That is `S=1`, a real and useful mode,
  but it is *not* subset-of-`S`, and Ravel never presents it as a migration
  of one.

(a) is the contract. (b) and (d) implement it. (c) is orthogonal. (e) is
weaker than the contract.

## What does the routing

Kubernetes cannot express subset affinity by itself. A core `Service` offers
only `sessionAffinity: ClientIP`, which keys on the source address of the
client. That is the address of the OpenTelemetry Collector or of the gateway
proxy in front of it, not of the tenant. Under a shared collector, every
tenant maps onto one key. The operator therefore never sets it.

Tenant identity lives in the authentication material of the request. Ravel
resolves tenancy server-side from the bearer token. OTLP connections are
long-lived, and the URL path carries nothing routable. The routing decision
must therefore be made at layer 7, by something that can read a header or
run tenant resolution. Two implementations do this: the legacy ingress-nginx
backend (b) and the Ravel-native router (d). The affinity-free exposure path
(c) and the weaker `S=1` fallback (e) sit beside them.

### (b) Legacy ingress-nginx backend (deprecated)

**`backend: ingressNginx` is the default and it keeps working unchanged.**
Under it, the operator ships the `Ingress` object and the annotations that
configure it.

**Ravel does not ship an ingress controller, and the legacy backend does not
add one.** The cluster must already run an ingress controller that
understands the annotations. The supported and tested target is
[ingress-nginx](https://kubernetes.github.io/ingress-nginx/), whose
`upstream-hash-by` family expresses the subset-of-`S` model:

| Annotation | Rendered value | What it does |
|---|---|---|
| `nginx.ingress.kubernetes.io/upstream-hash-by` | an nginx variable, e.g. `$http_authorization` | The key. Hashed with ketama, so only a few keys remap when the endpoint set changes. |
| `nginx.ingress.kubernetes.io/upstream-hash-by-subset` | `true` | The key selects a *group* of endpoints, not one endpoint. |
| `nginx.ingress.kubernetes.io/upstream-hash-by-subset-size` | `spec.gateway.ingestAffinity.subsetSize` | How many replicas are in each group. |
| `nginx.ingress.kubernetes.io/service-upstream` | `false` | Balance over pod endpoints, not the ClusterIP. |
| `nginx.ingress.kubernetes.io/backend-protocol` | `GRPC` (gRPC Ingress only) | Speak gRPC to the gateway. |

Within a subset, ingress-nginx picks a member uniformly at random per
request. A tenant therefore keeps using both of its replicas continuously.
The loss of one replica costs half of its capacity and not all of it, and
nothing has to fail over.

The operator always renders `service-upstream: false` explicitly. With
`true`, the upstream has one server, the ClusterIP of the Service. kube-proxy
then picks the pod, the hash has nothing to distribute over, and the
affinity silently does nothing. The default of the controller is `false`,
but the ingress-nginx ConfigMap can set it cluster-wide.

**Deprecation.** ingress-nginx is retiring upstream, so this backend is
deprecated. A cluster on it gets an `IngestAffinityBackendDeprecated`
condition on its `RavelCluster` status, with reason `IngressNginxRetired`.
The condition disappears once the cluster moves off that backend. An
existing CR does not change on upgrade: a CR that never set `backend`
deserializes to `ingressNginx`, with the same `subsetSize` and the same key.
The migration target is `backend: ravelNative` (see
[Migrating from `ingressNginx` to `ravelNative`](#migrating-from-ingressnginx-to-ravelnative)).

### (d) The `ravelNative` router

`backend: ravelNative` delivers the (a) contract without any ingress
controller. The operator renders `ravel-ingest-router`, a horizontally
scalable service. The router does three things:

- It **watches `EndpointSlice` objects** for the gateway Service of this
  `RavelCluster`, so it always has the live set of Ready gateway pods and
  their addresses.
- It **computes the subset** with deterministic rendezvous (HRW) hashing
  over that endpoint set, keyed on tenant identity. These are the same
  subset-of-`S` semantics that the nginx annotations express, owned in the
  `ravel-affinity` crate of Ravel.
- It **dials the chosen pod addresses directly**, from the EndpointSlice,
  never through the ClusterIP of the gateway Service. A connection through
  the ClusterIP goes back to the load balancing of kube-proxy, which undoes
  the subset selection.

Within the `S`-member subset the router picks by local round-robin. It skips
any member that its own EndpointSlice view marks not-Ready. If fewer than `S`
members are Ready, it continues down the same HRW-ranked order (position
`S+1`, `S+2`, …) and does not narrow to a smaller, unbalanced set.

**No ingress dependency.** Subset selection lives in Ravel, so any Gateway
implementation, or none, can terminate the connection. With Gateway API
exposure (c), you get subset pinning behind a conformant `Gateway`. With no
exposure, the router still pins, and you choose how to route to the Service
of the router.

**Rebalance identity.** The pod UID that the EndpointSlice reports
identifies a replica, not the IP. A different pod can reuse an IP after
churn. For scale events the HRW guarantee holds: when one endpoint is added
or removed, only the tenants whose rank crosses position `S` move. The
guarantee does *not* hold across a full rolling update of the gateway
Deployment. The UID of every pod changes, so the whole replica set is new,
and a rollout causes a one-time full reassignment. Any identity-keyed subset
scheme has this property. It is a rebalance (a cost event), not a
correctness event. See
[Rolling restarts and replica loss](#rolling-restarts-and-replica-loss).

**Transient split view.** Router replicas watch EndpointSlice independently.
After a membership change, two replicas can briefly compute different
subsets for the same tenant, until both have observed the change. Informer
and watch latency (seconds) bounds this. It heals with no intervention. It
has no durability or correctness impact, because it is a routing decision.
During a rollout this is expected behavior.

#### Router objects and RBAC

Under `backend: ravelNative` the operator renders a set of objects that all
share the base name `<cluster>-ingest-router`:

| Object | Kind | Notes |
|---|---|---|
| `<cluster>-ingest-router` | Deployment | The router pods. Image is `ingestAffinity.routerImage`. HTTP-only (see the limitation below). |
| `<cluster>-ingest-router` | Service | ClusterIP on port 8080. Gateway API exposure (c) points its HTTPRoute here. |
| `<cluster>-ingest-router` | ServiceAccount | The identity the Deployment runs as. |
| `<cluster>-ingest-router` | Role | Namespaced. `get`/`list`/`watch` on `endpointslices` (`discovery.k8s.io`) and `services` (core), in this namespace only. |
| `<cluster>-ingest-router` | RoleBinding | Binds the Role to the ServiceAccount. |

The Role is least-privilege: no cluster-wide grant, no other resource, no
write verbs. It grants what the EndpointSlice watcher needs to compute
subsets and dial gateway pods, consistent with the storage credential role
scoping that Ravel uses elsewhere.

When you switch `backend` away from `ravelNative`, or disable affinity, the
operator deletes all of these objects on the next reconcile. The
delete-sweep covers all five kinds under the shared name, so the switch
leaves no orphan.

#### The `canonicalTenant` key source

`key.source: canonicalTenant` is a key source that only `ravelNative` offers.
The router runs the tenant-resolution chain of Ravel, the same code that
`ravel-server` uses, and hashes the resulting canonical `TenantId`. It does
not hash a raw header value.

This key is **immune to bearer-token rotation**. A token rotation does not
move the tenant to a different subset, because the key is the resolved
tenant and not the token. `authorizationHeader` moves a tenant on every
rotation (see [Choosing the key](#choosing-the-key)).

`canonicalTenant` works only for clusters that authenticate with static
tenant tokens. **The only resolver the CRD wires through is static tenant
tokens**, via `spec.tenantTokensSecretRef`. OIDC is a resolver that the
chain can run, but no CRD field threads an issuer or JWKS URL
into the router. mTLS resolution is not available at any layer: the router
builds one resolver chain that every listener shares. The mTLS resolver
trusts a client-supplied identity header and does not verify it.
`ravel-ingest-router` therefore refuses `--mtls-enabled`, so that no client
can set that header and pick its own tenant. A CRD field cannot change that.
To isolate the resolver, the router needs a dedicated mTLS listener, and it
has none.

If you rely on OIDC or mTLS for tenancy, use `authorizationHeader`, which
hashes the token bytes.

Resolution is **fail-closed**. If the router cannot resolve a tenant for a
request under `canonicalTenant`, it rejects the request (HTTP 401 / gRPC
`UNAUTHENTICATED`). It never routes on a key other than the one you
configured.

#### HTTP-only router limitation

**The operator-rendered router Deployment is HTTP-only.** It renders a single
HTTP container port (8080) and no `--listen-grpc` flag. The router binary
has a gRPC listener, but the rendered Deployment does not use it. When
`backend: ravelNative` is combined with Gateway API exposure (c):

- The rendered **HTTPRoute** points at the Service of the router
  (subset-pinned).
- The rendered **GRPCRoute** still targets the **gateway Service directly**,
  as it does with affinity off.

gRPC ingest keeps working after a switch to `ravelNative`, but the router
does **not** subset-pin OTLP/gRPC. The Gateway implementation load-balances
it across all gateway pods, which for gRPC is `S=1` or worse, not
subset-of-`S`.

The reason is structural. The router resolves one gateway port per process
and cannot proxy the distinct HTTP and gRPC listener ports of the gateway at
once. gRPC through the router needs either a per-listener-port surface or a
two-Deployment split, and neither exists.

If most of your ingest request bill comes from OTLP/gRPC, weigh this before
you migrate: `ravelNative` pins your OTLP/HTTP traffic but not your gRPC.

### (c) Gateway API exposure

`gateway.exposure.gatewayApi` is a separate field, independent of
`ingestAffinity`. It renders standard `gateway.networking.k8s.io`
`HTTPRoute` and `GRPCRoute` objects attached to an existing `Gateway`, in
place of the ingress-nginx-specific `Ingress` objects. It carries no vendor
extension, so it works with any conformant Gateway API implementation (Envoy
Gateway, NGINX Gateway Fabric, Cilium, Istio, a managed cloud
implementation). Ravel does not couple its CRD to one.

```yaml
spec:
  gateway:
    exposure:
      gatewayApi:
        gatewayRef:
          name: public-gateway
          # namespace: defaults to this RavelCluster's own namespace
        hostnames: [ingest.example.com]
        grpc: true   # also render a GRPCRoute; default true
```

By itself, exposure has **no tenant-affinity effect**. Routing goes straight
to the gateway Service, load-balanced the way the Gateway implementation
load-balances a Service backendRef (typically endpoint-aware round robin,
not subset-of-`S`). With the two affinity backends:

- **With `backend: ravelNative`**, the operator points the rendered
  **HTTPRoute** at the Service of the router, so OTLP/HTTP is subset-pinned.
  The **GRPCRoute** still targets the gateway Service directly (see the
  [HTTP-only router limitation](#http-only-router-limitation)).
- **With an *enabled* `backend: ingressNginx`**, the combination is
  **rejected at admission by a CEL rule**. In that combination, traffic on
  the Gateway API path bypasses the nginx subset annotations: pinned on the
  Ingress path, unpinned on the Gateway API path, with no signal to the
  operator (see
  [Admission rejections](#admission-rejections)). Use `ravelNative`, or
  disable `ingestAffinity`.

The operator does not render TLS here. Gateway API exposure terminates TLS
at the listener of the referenced `Gateway`, which you configure directly
(`tls.certificateRefs`). There is no `tlsSecretName` equivalent under
`exposure.gatewayApi`, unlike the legacy `ingestAffinity.tlsSecretName`.

Gateway API exposure requires Gateway API **v1.1 or newer** in the cluster.
`GRPCRoute` reached the stable `v1` API version in Gateway API v1.1 (it was
`v1alpha2` in v1.0), and the operator renders it at `v1`.

### (e) Single-backend consistent hashing

HAProxy, Traefik, Istio, Envoy and cloud L7 load balancers have built-in
hashing. In those layers **there is no subset-of-`S` configuration, and the
closest thing is weaker.** They offer single-backend consistent hashing.
HAProxy's `balance hdr(...)`, Istio's
`DestinationRule.trafficPolicy.loadBalancer.consistentHash.httpHeaderName`,
Envoy's ring-hash and Maglev policies, and the session-persistence
extensions in Gateway API implementations all map one key onto **one**
backend. That is `S=1`.

`S=1` is a real and useful mode, and it still divides the flush cost by
`replicas`. It is not what `subsetSize: 2` means. A tenant pinned to a
single replica loses all of its capacity when that replica restarts, and it
must be rehashed to another replica. The default subset of two avoids that
failure.

Ravel does not present those configurations as a migration of subset
affinity, and the operator does not generate them. Do not present them that
way in a runbook. You have two options:

- Use `spec.gateway.ingestAffinity.annotations` to carry the annotations of
  your controller onto the legacy Ingress objects.
- Configure that layer yourself and leave `ingestAffinity` unset.

In both cases, read what you configure as `S=1` unless the layer implements
subset-of-`S` selection. For subset-of-`S` behind any Gateway
implementation, use `backend: ravelNative`.

## Admission rejections

The API server rejects two combinations at admission (CEL
`x-kubernetes-validations` rules). A bad manifest fails on `kubectl apply`
with a clear message and does not degrade silently at runtime.

1. **Gateway API exposure with an enabled legacy backend.** Setting
   `gateway.exposure.gatewayApi` while `ingestAffinity` is enabled on
   `backend: ingressNginx` is rejected:

   > `gateway.exposure.gatewayApi cannot be combined with an enabled
   > gateway.ingestAffinity on backend: ingressNginx -- traffic on the Gateway
   > API path would bypass the nginx subset annotations entirely; use backend:
   > ravelNative or disable ingestAffinity`

2. **Canonical-tenant key on the legacy backend.** Setting
   `key.source: canonicalTenant` while `backend` is `ingressNginx` is
   rejected, because ingress-nginx cannot run the tenant-resolution chain
   that canonical-tenant hashing needs:

   > `gateway.ingestAffinity.key.source canonicalTenant requires backend:
   > ravelNative -- ingress-nginx cannot run the ravel-tenant-resolve auth chain
   > that canonical-tenant hashing needs`

   `backend` defaults to `ingressNginx`, so a manifest that sets
   `canonicalTenant` and omits `backend` is rejected too.

## Render-time degradation

The operator catches two misconfigurations at render time and not at
admission, because they depend on cluster state that the API server does not
see. In both cases the operator renders **no router objects** and writes a
`Degraded` condition on the `RavelCluster` status. The cluster fails
visibly, and the operator schedules no pod that cannot run or that
crashloops.

- **`routerImage` unset under `backend: ravelNative`.** The router is a
  different binary from `spec.image` (which is `ravel-server`), so there is
  no image to fall back to. The reason of the `Degraded` condition is
  **`RouterImageMissing`**.
- **`key.source: canonicalTenant` with no resolver configured.** The CLI of
  the router refuses to start under `canonical-tenant` unless at least one
  resolver is present. The only resolver the CRD wires through is
  `tenantTokensSecretRef`. If that Secret is absent, or present but resolves
  to zero tenant keys this reconcile, no `--tenant-token` flag renders. A
  router in that state crashloops at startup, so the operator renders
  nothing. The reason of the `Degraded` condition is
  **`CanonicalTenantResolverMissing`**.

Only the router degrades. The gateway, query, and maintain Deployments still
reconcile normally. Fix the field that the condition message names, and the
router renders on the next reconcile.

If Gateway API exposure is also configured, a degraded router pass does not
strand the HTTPRoute. The operator computes the render outcome of the router
before it renders routes. When the router will not exist that pass, the
HTTPRoute targets the gateway Service directly.

## Migrating from `ingressNginx` to `ravelNative`

The supported migration is from `backend: ingressNginx` to
`backend: ravelNative`. The ingress-nginx rendering path is deprecated but
not removed: `ingressNginx` remains the schema default and keeps working,
and there is no removal timeline.

On an existing production cluster the switch is a **migration with behavior
differences**. Plan for each of them:

- **A new service to run.** `ravelNative` renders a `ravel-ingest-router`
  Deployment (and Service/ServiceAccount/Role/RoleBinding). It needs an
  image. Set `ingestAffinity.routerImage`, or the operator degrades with
  `RouterImageMissing`.
- **RBAC to grant.** The operator must be able to create the ServiceAccount,
  Role, and RoleBinding of the router. The shipped ClusterRole of the
  operator includes this (see [kubernetes.md](kubernetes.md)). Make sure
  that your deployment of the operator carries it.
- **HTTP-only.** The router does not subset-pin OTLP/gRPC (see the
  [HTTP-only router limitation](#http-only-router-limitation)). If gRPC
  dominates your request bill, the saving is smaller than the HTTP math
  suggests.
- **TLS moves.** `ingestAffinity.tlsSecretName` applies to the legacy
  backend only. Under Gateway API exposure, configure `tls.certificateRefs`
  on the listener of your `Gateway` yourself. Point it at the same or an
  equivalent Secret. The operator carries nothing forward automatically.
- **A one-time rebalance.** The cutover changes the routing layer, and the
  key of every tenant now hashes through it differently. Expect a bounded
  PUT-rate bump as buffers re-home, as for any rebalance (see
  [Rolling restarts and replica loss](#rolling-restarts-and-replica-loss)).

A workable sequence:

1. Set `routerImage`.
2. If you use the tenant-tokens Secret, make sure that it is populated.
3. Apply `backend: ravelNative`.
4. If you also want Gateway API exposure, add `exposure.gatewayApi` in the
   same or a following apply. Then move the TLS of your `Gateway` listener
   across.
5. Watch the flush/PUT rate settle (see
   [Verifying it works](#verifying-it-works)).
6. When traffic has moved, decommission the old ingress-nginx `Ingress` for
   this cluster.

## Turning it on

The legacy backend, with everything else defaulted:

```yaml
apiVersion: ravel.nofire.ai/v1alpha1
kind: RavelCluster
metadata:
  name: prod
spec:
  # ... image, shards, storage, tenantTokensSecretRef ...
  gateway:
    replicas: 10
    ingestAffinity:
      ingressClassName: nginx
      hosts: [ingest.example.com]
      tlsSecretName: ingest-tls
```

Everything else defaults: enabled, backend `ingressNginx`, subset size 2,
key = the `Authorization` header, and a second Ingress for OTLP/gRPC.

The Ravel-native backend, recommended for new deployments:

```yaml
spec:
  gateway:
    replicas: 10
    ingestAffinity:
      backend: ravelNative
      routerImage: ghcr.io/nofireai/ravel-ingest-router:latest
      # subsetSize, key default as above
    exposure:
      gatewayApi:
        gatewayRef:
          name: public-gateway
        hostnames: [ingest.example.com]
```

### Fields

`spec.gateway.ingestAffinity`:

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | boolean | `true` | `false` deletes the rendered objects and returns to the pre-affinity render. The incident switch. |
| `backend` | enum | `ingressNginx` | `ingressNginx` (deprecated) or `ravelNative`. Omitting it keeps the backend an existing CR already runs. |
| `routerImage` | string | none | The `ravel-ingest-router` container image. **Required** when `backend: ravelNative` (it is a different binary from `spec.image`); unset there degrades the router with reason `RouterImageMissing`. No effect under `ingressNginx`. |
| `subsetSize` | integer | `2` | Replicas per tenant. Must be at least 1. |
| `key.source` | enum | `authorizationHeader` | `authorizationHeader`, `header`, `mtlsSubject`, or `canonicalTenant`. `canonicalTenant` requires `backend: ravelNative` (rejected on `ingressNginx`). |
| `key.headerName` | string | none | Required when `key.source` is `header`. Constrained to `^[A-Za-z0-9][A-Za-z0-9-]{0,62}$`. |
| `ingressClassName` | string | none | **Legacy `ingressNginx` only.** Omit to use the cluster's default IngressClass. |
| `hosts` | list | `[]` | **Legacy `ingressNginx` only.** Empty renders one host-less rule matching any host that reaches the controller. |
| `tlsSecretName` | string | none | **Legacy `ingressNginx` only.** Renders `spec.tls`. Effectively required, see below. No Gateway API equivalent. |
| `grpc` | boolean | `true` | **Legacy `ingressNginx` only.** Also render an Ingress for OTLP/gRPC on port 4317. (Gateway API exposure has its own `exposure.gatewayApi.grpc`.) |
| `annotations` | map | `{}` | **Legacy `ingressNginx` only.** Merged onto both Ingress objects, *before* the affinity annotations, which therefore always win. |

`spec.gateway.exposure.gatewayApi` (independent of `ingestAffinity`):

| Field | Type | Default | Notes |
|---|---|---|---|
| `gatewayRef.name` | string | required | Name of the existing `Gateway` the routes attach to via `parentRefs`. |
| `gatewayRef.namespace` | string | this CR's namespace | Namespace of the `Gateway`. Omitted resolves to the `RavelCluster`'s own namespace. |
| `hostnames` | list | `[]` | Hostnames the routes answer on. Empty renders routes with no `hostnames`, matching every hostname the parent `Gateway`'s listeners accept. |
| `grpc` | boolean | `true` | Also render a `GRPCRoute` (OTLP/gRPC, port 4317). |

### Managed objects

For a `RavelCluster` named `prod`, the rendered objects depend on the
backend and on whether exposure is set.

Under `backend: ingressNginx` (enabled):

| Object | Kind | Notes |
|---|---|---|
| `prod-gateway-ingest` | Ingress | OTLP/HTTP on port 4318, on the `/v1/metrics`, `/v1/logs`, and `/v1/traces` paths only. |
| `prod-gateway-ingest-grpc` | Ingress | OTLP/gRPC on port 4317, `backend-protocol: GRPC`, on the four gRPC ingest service paths. Absent when `grpc: false`. |

Under `backend: ravelNative` (enabled, `routerImage` set): the five
`prod-ingest-router` objects listed in
[Router objects and RBAC](#router-objects-and-rbac). No `Ingress` is
rendered.

Under `gateway.exposure.gatewayApi` (independent of backend):

| Object | Kind | Notes |
|---|---|---|
| `prod-gateway-route` | HTTPRoute | Attached to `gatewayRef`. Backs onto the router's Service under `ravelNative`, otherwise the gateway Service. |
| `prod-gateway-route-grpc` | GRPCRoute | Attached to `gatewayRef`. **Always** backs onto the gateway Service directly (see the HTTP-only limitation). Absent when `exposure.gatewayApi.grpc: false`. |

The `RavelCluster` owns every object, and each is deleted with it. All are
also deleted when `enabled` becomes `false` or the mode changes. A switch of
modes therefore converges and leaves no orphan that routes live traffic.

#### The two legacy Ingress objects

The legacy backend needs two Ingress objects, because `backend-protocol` is
a per-Ingress annotation. One Ingress cannot speak HTTP to one port and gRPC
to another.

**The two Ingress objects route on disjoint paths, never a shared `/`.**
OTLP/HTTP and OTLP/gRPC have disjoint path namespaces. Each Ingress serves
only its own paths, and the two never collide, with or without `hosts`.

If both Ingress objects claimed the same host and path `/`, the result is a
duplicate-path conflict. ingress-nginx builds one `location` per path in a
server block. In a conflict it keeps one location (ordered by
CreationTimestamp, tie-broken by namespace and name) and drops the other
with a warning. `prod-gateway-ingest` sorts before
`prod-gateway-ingest-grpc`, so the HTTP object wins the conflict. The
`grpc_pass` of the gRPC object and its affinity annotations then never take
effect, and OTLP/gRPC, the primary ingest path, is proxied as HTTP/1.1 and
fails.

The HTTP Ingress serves the three OTLP/HTTP routes:

- `/v1/metrics`
- `/v1/logs`
- `/v1/traces`

The gRPC Ingress serves the full service name of every gRPC service that the
gateway registers on the ingest surface. There are four, including the
`ArrowMetricsService` of OTAP:

- `/opentelemetry.proto.collector.metrics.v1.MetricsService`
- `/opentelemetry.proto.collector.logs.v1.LogsService`
- `/opentelemetry.proto.collector.trace.v1.TraceService`
- `/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService`

Each path is a full gRPC service name, which is a complete path element
under `PathType: Prefix`. Kubernetes matches Prefix element-wise and splits
on `/`. A full service name therefore prefixes `/<service>/<method>`
correctly under both the strict spec semantics and the rendering of
ingress-nginx. A truncated common prefix such as
`/opentelemetry.proto.collector.` is a single element that equals none of
the service names. It matches nothing under the spec, and it also omits the
OTAP service.

### TLS and body size

The operator does not set these two for you.

**TLS.** Under the legacy backend, ingress-nginx serves HTTP/2 to clients
over TLS, and OTLP/gRPC needs HTTP/2. Without `tlsSecretName`, the gRPC
Ingress does not work for most clients. Tenant tokens are also bearer
tokens, so plaintext ingest exposes the credential of every tenant on the
wire. Under Gateway API exposure the equivalent is `tls.certificateRefs` on
the listener of your `Gateway`, which you configure directly.

**Body size.** ingress-nginx defaults `proxy-body-size` to `1m` and rejects
larger requests with a 413. A batched OTLP/HTTP export can exceed that. The
operator does not raise it. Pick a value and set it yourself (legacy
backend):

```yaml
      annotations:
        nginx.ingress.kubernetes.io/proxy-body-size: "16m"
```

## Sizing the subset

Start at the default of 2. Raise it only for a measured reason.

- **The saving is `replicas / subsetSize`.** A move from 2 to 4 halves the
  saving. Raise it only as far as the throughput of a tenant requires.
- **The ceiling is the throughput of a tenant.** If the exporters of a
  tenant are throttled, or the pods of its subset are saturated while the
  rest of the Deployment idles, its subset is too small. That is the signal
  to raise `subsetSize`.
- **`subsetSize` >= `replicas` disables the saving.** Every tenant then
  reaches every replica. That is the pre-affinity behaviour with extra
  moving parts.
- **Keep `replicas` a multiple of `subsetSize`** where you can. The legacy
  ingress-nginx backend partitions the endpoint list into groups of
  `subsetSize`. A remainder produces one undersized group whose tenants get
  less capacity than the others. The rendezvous hashing of `ravelNative`
  does not partition into fixed groups, so the effect is less pronounced
  there, but a multiple still keeps the distribution evenest.
- **Do not size for the largest tenant.** `subsetSize` is one number for the
  whole cluster. There is no per-tenant subset size, so a raise for one
  tenant raises the cost of every tenant. A tenant that needs a much larger
  subset than the others belongs in its own `RavelCluster`, which also gives
  it its own gateway Deployment to be bounded by.

A practical rule: `subsetSize = 2`, with `replicas` sized so that a subset
carries the peak of the largest tenant with one replica to spare.

## Choosing the key

**`authorizationHeader` (default).** Hashes the `Authorization` header, which
is the credential that Ravel resolves tenancy from. Nothing on the client
needs to change. Two consequences:

- A tenant that uses several distinct tokens (per-agent credentials, for
  example) hashes to several subsets. Affinity still works, but it divides
  less. One token per tenant gives the full saving.
- **A token rotation moves that tenant to a different subset**, which is a
  rebalance (see
  [Rolling restarts and replica loss](#rolling-restarts-and-replica-loss)).
  This is usually invisible. A token rotation and a rolling restart at the
  same moment move a tenant twice. `canonicalTenant` avoids this.

**`header` + `headerName`.** Hashes a named header, such as one that a
trusted upstream proxy stamps. Use this only if clients cannot set the
header themselves. Otherwise a tenant can choose its own subset, and a
misbehaving tenant can pin itself onto a busy subset.

**`mtlsSubject`.** Hashes the mTLS client certificate subject
(`$ssl_client_s_dn` under ingress-nginx). The terminating layer must do TLS
with client-certificate authentication configured. If it does not, the
subject is empty, every request hashes to the same key, and **every tenant
lands on one subset**, a much worse outcome than no affinity. Make sure that
client-certificate authentication is on before you select this.

The router reads the subject from the identity header that the terminating
layer stamps, and never verifies a certificate itself. Under `ravelNative`,
a client that can reach the router directly can set that header and choose
its own subset, the same way it can under `header`. Use this only when
clients cannot reach the router without passing through the terminating
layer.

**`canonicalTenant` (`ravelNative` only).** Hashes the canonical `TenantId`
that the resolver of Ravel produces, not any raw wire value. It is **immune
to token rotation**: a token rotation does not move the tenant to a new
subset. No other key source has this property. Its limits:

- It is rejected on `backend: ingressNginx`, because nginx cannot run the
  resolver.
- It works only for clusters that authenticate with static tenant tokens
  (`tenantTokensSecretRef`). OIDC has no CRD surface. mTLS resolution is not
  an option, because `ravel-ingest-router` refuses `--mtls-enabled`
  unconditionally.
- With no resolver, the router degrades with
  `CanonicalTenantResolverMissing`.

See [The `canonicalTenant` key source](#the-canonicaltenant-key-source).

Under the legacy backend, header names are lowercased, and every character
outside `[a-z0-9]` maps to `_`. This matches the `$http_<name>` variable
naming of nginx: `X-Scope-OrgID` becomes `$http_x_scope_orgid`. The CRD also
rejects header names outside HTTP token characters
(`^[A-Za-z0-9][A-Za-z0-9-]{0,62}$`), so nothing user-supplied can reach the
nginx configuration as syntax.

## Rolling restarts and replica loss

Both are rebalance events, on both backends. Neither is a correctness event.

**What happens on a rebalance.** The endpoint list changes, subsets are
recomputed, and some tenants move to a different subset. The next write of a
moved tenant opens a fresh buffer on its new replica with a new `writer_id`
and `epoch`. Its old replica still holds an unflushed buffer, which its own
age timer flushes shortly after. A rebalance therefore costs one extra flush
per moved `(tenant, signal, shard)`: a brief bounded uptick in PUTs, not a
step change. Nothing is lost. The flush of the old replica completes and
commits normally, and both objects are valid because writer identity is part
of the key.

**Replica loss.** The routing layer drops the endpoint as soon as the
EndpointSlice updates. ingress-nginx watches it, and `ravelNative` watches
it directly. A tenant whose subset lost a member keeps writing to its
surviving member, at full correctness and roughly half its previous subset
capacity. That lasts until the replacement pod is Ready and the subsets
recompute. With a subset of one, a replica loss stalls that tenant until the
reroute completes. For that reason the default subset is two.

**Rolling restart.** Every pod is replaced, so the endpoint set changes
several times and most tenants move at least once. Under `ravelNative`, the
pod UID identifies a replica, so a full gateway rollout replaces the entire
set and causes a one-time full reassignment. Expect the PUT-rate bump to
cover the whole roll. Under either backend, conservative
`maxSurge`/`maxUnavailable` values change the endpoint set in small steps,
which moves fewer tenants per step.

**Scaling on the legacy backend.** ketama hashing bounds how many *keys*
remap when the endpoint set changes. ingress-nginx builds subsets by
partitioning the endpoint list, so a change in the number of endpoints can
reshuffle subset *membership* more broadly than the key remapping alone
suggests. A scale from 10 replicas to 11 is not a 1-in-11 disturbance. It
regroups the partition. The rendezvous hashing of `ravelNative` has the
tighter HRW guarantee: on a single add or remove, only tenants whose rank
crosses position `S` move. Treat any scaling event as a rebalance with a
cost, and scale in steps of `subsetSize` where you can.

**Long-lived OTLP connections do not pin anything.** A collector holds one
HTTP/2 connection to the routing layer for hours. The routing decision is
re-evaluated per request (per gRPC call, per HTTP export), so a rebalance
takes effect on the next request and the client does not reconnect. The
long-lived connection is between the client and the *router/ingress*, not
between the client and a gateway pod. A client that keeps a connection open
through a full gateway rollout sees no change, because the routing layer
absorbs it.

## Verifying it works

Affinity can fail silently, so check it.

Under the legacy backend, check the rendered annotations:

```sh
kubectl get ingress prod-gateway-ingest -o jsonpath='{.metadata.annotations}'
```

All four affinity annotations must be present, and `service-upstream` must be
`false`.

Under `ravelNative`, check that the router objects rendered and that the
status is not degraded:

```sh
kubectl get deploy,svc,role,rolebinding,sa \
  -l app.kubernetes.io/component=ingest-router
kubectl get ravelcluster prod -o jsonpath='{.status.conditions}'
```

A `Degraded` condition with reason `RouterImageMissing` or
`CanonicalTenantResolverMissing` means that the router did not render. Fix
the field that it names.

Then, for either backend, check the effect on the flush and PUT rate. Watch
the flush counters of the gateway Deployment (see
[observability.md](observability.md)) while you enable affinity. With `R`
replicas and subset size 2, the flush rate falls toward `2/R` of its
previous value once every buffer has aged out. If the rate does not move,
one of these is the cause:

- The traffic does not go through the routing layer.
- (Legacy) `service-upstream` is true somewhere.
- Every request carries the same key.

## Turning it off

```sh
kubectl patch ravelcluster prod --type merge \
  -p '{"spec":{"gateway":{"ingestAffinity":{"enabled":false}}}}'
```

On the next reconcile the operator deletes the rendered objects: the Ingress
objects under the legacy backend, or the router objects under `ravelNative`.
It then returns to the pre-affinity render. Ingest keeps working through
whatever else routes to the gateway Service.

Removal of the whole `ingestAffinity` block does the same thing. With
`enabled: false` you turn affinity off and keep the rest of the
configuration.

## See also

- [kubernetes.md](kubernetes.md): the operator and the full `RavelCluster`
  reference.
- [ingest.md](ingest.md): the OTLP endpoints and how tenancy is authenticated.
- [observability.md](observability.md): the metrics to watch the saving on.
- [cost-model.md](cost-model.md): the other levers on the same request bill.

## Background

- Why request cost dominates, and the other three levers on it, are
  [ADR-0076](../adrs/0076-reducing-s3-request-cost.md). Subset affinity is
  its decision 1.
- The exposure and affinity split, the Ravel-native router, and the
  canonical-tenant key source are
  [ADR-0080](../adrs/0080-gateway-api-ingest-affinity.md), decisions 2, 3
  and 1.
- The process-wide ingest buffer budget is ADR-0069 decision 1.
