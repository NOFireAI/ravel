# Ingest affinity: pinning a tenant to a subset of gateway replicas

Ravel's object-storage bill is dominated by request charges, not stored bytes,
and the number of requests scales with how many gateway replicas a tenant's
writes land on. Ingest affinity pins each tenant to a small, stable subset of
replicas so the same data is flushed once per subset instead of once per
replica.

It changes no format, no contract, and no acknowledgement latency. It is
configuration. The *routing mechanism* comes in layers: a deprecated
ingress-nginx path, a Ravel-owned router, and a separate Gateway API exposure
concept. The layers share the semantic contract this guide opens with.

## Why it saves requests

An ingest buffer is per `(tenant, signal, shard)` **per replica**. Every buffer
flushes on its own age timer and every flush issues a data PUT plus a commit
PUT. So a tenant whose exporters spray across all `R` gateway replicas keeps `R`
independent flush streams alive for one logical stream of data, and pays `R`
times the PUT pairs for it:

```
PUTs/day = 2 x tenants x signals x shards x replicas x (86400 / age_threshold_s)
```

Pinning a tenant to a subset of size `S` replaces `replicas` with `S` in that
product for that tenant. At 10 replicas and the default subset of 2, that is a
**5x reduction** in flush PUTs for every tenant, with no latency cost: each
replica still acknowledges a strict write the moment its own commit PUT returns.

The saving compounds with the other levers on the same bill (shard count,
flush cadence), because they are different terms of the same product.

There is a read-side benefit too. Fewer, larger L0 objects mean fewer open-hour
segments for a query to open, which lowers the per-query request budget.

## What it costs

**A tenant's ingest throughput is bounded by its subset.** This is the real
price and it is structural, not a tuning artifact. A tenant pinned to 2 replicas
gets the CPU, memory, and network of 2 pods no matter how many the gateway
Deployment runs. If that tenant's traffic outgrows 2 pods, the correct response
is a larger subset, not a larger Deployment.

Two smaller costs:

- **Memory concentrates.** A subset holds the buffers for every tenant hashed
  onto it, so a subset that draws several large tenants carries more buffered
  bytes than an evenly sprayed replica would. The process-wide ingest buffer
  budget still caps this; the effect is that the cap is reached sooner on a hot
  subset.
- **Load is only as even as the hash.** With a handful of tenants the
  distribution across subsets is visibly lumpy. Affinity pays off with many
  tenants, not with three.

Correctness is not on the list. Affinity is best-effort by design: `writer_id`
and `epoch` already disambiguate concurrent writers in the object key, so a
request that lands on a replica outside its usual subset writes a perfectly
valid object. Rerouting is a cost event, never a correctness event.

## Five separate concepts, one at a time

The word "affinity" gets stretched over five distinct things below. They are
independent, they ship at different maturities, and confusing them is how a
cluster ends up thinking it has subset pinning when it has `S=1`, or none at
all. Name them separately:

- **(a) The affinity semantic contract.** Subset-of-`S` pinning: a tenant's
  writes reach a stable set of exactly `S` replicas, chosen by hashing tenant
  identity. This is what `subsetSize` *means*. It is defined independently of
  any implementation: everything below is a way to deliver it, or a weaker
  thing that is not it.
- **(b) The legacy `backend: ingressNginx` implementation.** The original
  delivery: the operator renders `Ingress` objects carrying ingress-nginx's
  `upstream-hash-by-subset` annotation family. It works and is unchanged, but
  ingress-nginx is retiring upstream, so it is **deprecated**.
- **(c) Gateway API exposure (`gateway.exposure.gatewayApi`).** A separate,
  affinity-independent field that renders standard `HTTPRoute`/`GRPCRoute`
  objects onto a `Gateway` you already run. It is *exposure*, not affinity: by
  itself it pins nothing.
- **(d) `backend: ravelNative`, Ravel's own subset router.** A horizontally
  scalable service, `ravel-ingest-router`, that watches EndpointSlices,
  computes the subset itself with rendezvous hashing, and dials gateway pods
  directly. It delivers the (a) contract with no dependency on any ingress or
  Gateway implementation. Documented in full below.
- **(e) Single-backend consistent hashing.** What most Gateway API and mesh
  implementations offer natively (ring-hash, Maglev, `consistentHash`). It maps
  one key onto **one** backend. That is `S=1`, a real and useful mode, but it
  is *not* subset-of-`S`, and Ravel never presents it as a migration of one.

(a) is the contract. (b) and (d) implement it. (c) is orthogonal. (e) is the
weaker cousin. The rest of this guide is organized around these five.

## What actually does the routing

Kubernetes cannot express subset affinity on its own. A core `Service` offers
only `sessionAffinity: ClientIP`, which keys on the client's source address:
the address of the OpenTelemetry Collector or the gateway proxy in front of it,
not of the tenant. Under a shared collector that maps every tenant onto one key.
The operator therefore never sets it.

Tenant identity lives in the request's authentication material. Ravel resolves
tenancy server-side from the bearer token, and OTLP connections are long-lived,
so the URL path carries nothing routable either. The routing decision has to be
made at layer 7 by something that can read a header or run tenant resolution.
Two implementations do this: the legacy ingress-nginx backend (b) and the
Ravel-native router (d). Alongside them sit the affinity-free exposure path
(c) and the weaker `S=1` fallback (e).

### (b) The legacy ingress-nginx backend (deprecated)

**Ravel does not ship an ingress controller, and the legacy backend does not add
one.** What the operator ships under `backend: ingressNginx` is the `Ingress`
object and the annotations that configure it. The cluster must already run an
ingress controller that understands them. The supported and tested target is
[ingress-nginx](https://kubernetes.github.io/ingress-nginx/), whose
`upstream-hash-by` family expresses exactly the ADR's model:

| Annotation | Rendered value | What it does |
|---|---|---|
| `nginx.ingress.kubernetes.io/upstream-hash-by` | an nginx variable, e.g. `$http_authorization` | The key. Hashed with ketama, so only a few keys remap when the endpoint set changes. |
| `nginx.ingress.kubernetes.io/upstream-hash-by-subset` | `true` | The key selects a *group* of endpoints, not one endpoint. |
| `nginx.ingress.kubernetes.io/upstream-hash-by-subset-size` | `spec.gateway.ingestAffinity.subsetSize` | How many replicas are in each group. |
| `nginx.ingress.kubernetes.io/service-upstream` | `false` | Balance over pod endpoints, not the ClusterIP. |
| `nginx.ingress.kubernetes.io/backend-protocol` | `GRPC` (gRPC Ingress only) | Speak gRPC to the gateway. |

Within a subset, ingress-nginx picks a member uniformly at random per request.
That is the point of a subset larger than one: a tenant keeps using both of its
replicas continuously, so losing one costs half its capacity rather than all of
it, and nothing has to fail over.

`service-upstream: false` is not decoration. With it set to `true` the upstream
has exactly one server, the Service's ClusterIP, so kube-proxy picks the pod
and the hash has nothing to distribute over. The affinity would silently do nothing.
The controller's own default is `false`, but it is settable cluster-wide in the
ingress-nginx ConfigMap, so the operator always renders it explicitly.

**`backend: ingressNginx` is the default and it keeps working unchanged**, but
ingress-nginx is retiring upstream, so it is deprecated. A
cluster on it gets an `IngestAffinityBackendDeprecated` condition on its
`RavelCluster` status, with reason `IngressNginxRetired`; the condition
disappears once the cluster moves off that backend. Nothing about an existing CR
changes on upgrade: a CR that never set `backend` deserializes to `ingressNginx`,
with the same `subsetSize` and the same key. The migration target is
`backend: ravelNative` (see below).

### (d) `backend: ravelNative`, Ravel's own subset router

`backend: ravelNative` delivers the (a) contract without any ingress controller
at all. The operator renders `ravel-ingest-router`, a horizontally scalable
service that:

- **watches `EndpointSlice` objects** for this `RavelCluster`'s gateway Service,
  so it always has the live set of Ready gateway pods and their addresses;
- **computes the subset itself** with deterministic rendezvous (HRW) hashing
  over that endpoint set, keyed on tenant identity: the same subset-of-`S`
  semantics the nginx annotations express, but owned in Ravel's own
  `ravel-affinity` crate;
- **dials the chosen pod addresses directly**, from the EndpointSlice, never
  through the gateway Service's ClusterIP. Going through the ClusterIP would
  hand the connection back to kube-proxy's own load balancing and undo the
  subset selection the router just made.

Within the `S`-member subset the router picks by local round-robin, skipping any
member its own EndpointSlice view marks not-Ready. If fewer than `S` members are
Ready it falls further down the same HRW-ranked order (position `S+1`, `S+2`, …)
rather than narrowing to a smaller, unbalanced set.

**Why it exists.** It removes the ingress-nginx dependency entirely. Subset
selection lives in Ravel, so it does not matter which Gateway implementation,
if any, terminates the connection. Combine it with Gateway API exposure (c)
and you get subset pinning behind a conformant `Gateway`; run it with no
exposure at all and it still pins, reachable however you route to the router's
own Service.

**Rebalance identity.** A replica is identified by its pod UID as reported by
the EndpointSlice, not its IP (an IP can be reused by a different pod after
churn). The HRW guarantee, that adding or removing one endpoint moves only the
tenants whose rank crosses position `S`, holds for scale events. It does *not*
hold across a full rolling update of the gateway Deployment: every pod's UID
changes at once, so the whole replica set is new and a rollout causes a
one-time full reassignment. This is inherent to any identity-keyed subset
scheme, not specific to this router, and it is a rebalance (a cost event), not
a correctness event. See
[Rolling restarts and replica loss](#rolling-restarts-and-replica-loss).

**Transient split view.** Multiple router replicas watch EndpointSlice
independently. Between one replica observing a membership change and another
catching up, two replicas can briefly compute different subsets for the same
tenant. This is bounded by informer/watch latency (seconds), self-heals with no
intervention, and has no durability or correctness impact: it is a routing
decision, not data correctness. Named here only so it is not mistaken for a bug
during a rollout.

#### The managed objects and their RBAC

Under `backend: ravelNative` the operator renders a set of objects that all
share the base name `<cluster>-ingest-router`:

| Object | Kind | Notes |
|---|---|---|
| `<cluster>-ingest-router` | Deployment | The router pods. Image is `ingestAffinity.routerImage`. HTTP-only (see the limitation below). |
| `<cluster>-ingest-router` | Service | ClusterIP on port 8080. Gateway API exposure (c) points its HTTPRoute here. |
| `<cluster>-ingest-router` | ServiceAccount | The identity the Deployment runs as. |
| `<cluster>-ingest-router` | Role | Namespaced. `get`/`list`/`watch` on `endpointslices` (`discovery.k8s.io`) and `services` (core), in this namespace only. |
| `<cluster>-ingest-router` | RoleBinding | Binds the Role to the ServiceAccount. |

The Role is deliberately least-privilege: no cluster-wide grant, no other
resource, no write verbs. It is exactly what the EndpointSlice watcher needs to
compute subsets and dial gateway pods, and nothing more, consistent with the
storage credential role scoping Ravel uses elsewhere.

Switching `backend` away from `ravelNative`, or disabling affinity, deletes
every one of these objects on the next reconcile; the delete-sweep covers all
five kinds under the shared name, so a mode switch leaves nothing orphaned.

#### The `canonicalTenant` key source

`key.source: canonicalTenant` is a key source only `ravelNative` can offer. Instead
of hashing a raw header value, the router runs Ravel's own tenant-resolution
chain, the same code `ravel-server` uses, and hashes the
resulting canonical `TenantId`. This is **immune to bearer-token rotation**:
rotating a token does not move the tenant to a different subset, because the key
is the resolved tenant, not the token. `authorizationHeader`, by contrast, moves
a tenant on every rotation (see [Choosing the key](#choosing-the-key)).

There is a real gap to know before choosing it: **the only resolver the CRD
wires through is static tenant tokens**, via `spec.tenantTokensSecretRef`. The
resolver chain itself can do OIDC and mTLS resolution, but there is no CRD
field that threads OIDC issuer or JWKS, or mTLS CA configuration, into the
router. So `canonicalTenant` works
only for clusters authenticating with static tenant tokens. If you rely on OIDC
or mTLS for tenancy, `canonicalTenant` is not usable for you; use
`authorizationHeader`, which hashes the token bytes.

Resolution is **fail-closed**: if the router cannot resolve a tenant for a
request under `canonicalTenant`, it rejects the request (HTTP 401 / gRPC
`UNAUTHENTICATED`) rather than falling back to a weaker key. It never silently
routes on something other than what you configured.

#### Current limitation: HTTP-only, no gRPC through the router

**The operator-rendered router Deployment is HTTP-only.** It renders a single
HTTP container port (8080) and no `--listen-grpc` flag, even though the router
binary itself has a gRPC listener. When `backend: ravelNative` is combined
with Gateway API exposure (c):

- the rendered **HTTPRoute** points at the router's Service (subset-pinned), but
- the rendered **GRPCRoute** continues to target the **gateway Service
  directly**, exactly as it does with affinity off.

So switching to `ravelNative` does not break gRPC ingest, which keeps working,
but OTLP/gRPC is **not subset-pinned by the router**. It is load-balanced by the
Gateway implementation across all gateway pods, which for gRPC is `S=1` or
worse, not subset-of-`S`. The reason is structural: the router resolves one
gateway port per process and cannot proxy the gateway's distinct HTTP and gRPC
listener ports at once. Wiring gRPC through the router would need either a
per-listener-port surface or a two-Deployment split, and neither exists.

If most of your ingest request bill comes from OTLP/gRPC, weigh this before
migrating: `ravelNative` pins your OTLP/HTTP traffic but not your gRPC.

### (c) Gateway API exposure

`gateway.exposure.gatewayApi` is a separate, independent field from
`ingestAffinity`: it renders standard
`gateway.networking.k8s.io` `HTTPRoute` and `GRPCRoute` objects attached to an
existing `Gateway`, instead of the ingress-nginx-specific `Ingress` objects. It
carries no vendor extension, so it works with any conformant Gateway API
implementation (Envoy Gateway, NGINX Gateway Fabric, Cilium, Istio, a managed
cloud implementation); Ravel does not couple its CRD to one.

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

By itself, exposure has **no tenant-affinity effect**: routing goes straight to
the gateway Service, load-balanced however the Gateway implementation
load-balances a Service backendRef (typically endpoint-aware round robin, not
subset-of-`S`). Its relationship with the two affinity backends:

- **With `backend: ravelNative`** the operator points the rendered **HTTPRoute**
  at the router's Service, so OTLP/HTTP is subset-pinned. The **GRPCRoute** still
  targets the gateway Service directly (HTTP-only router limitation, above).
- **With an *enabled* `backend: ingressNginx`** the combination is **rejected at
  admission by a CEL rule**, because traffic on the Gateway API path would
  bypass the nginx subset annotations entirely: pinned on the Ingress path,
  unpinned on the Gateway API path, with no signal to the operator (see
  [Admission rejections](#admission-rejections)). Use `ravelNative`, or disable
  `ingestAffinity`.

TLS is not rendered by the operator here: Gateway API exposure terminates TLS at
the referenced `Gateway`'s own listener, which you configure directly
(`tls.certificateRefs`). There is no `tlsSecretName` equivalent under
`exposure.gatewayApi`, unlike the legacy `ingestAffinity.tlsSecretName`.

Requires Gateway API **v1.1 or newer** in the cluster: `GRPCRoute` only reached
the stable `v1` API version in Gateway API v1.1 (it was `v1alpha2` in v1.0), and
the operator renders it at `v1`.

### (e) Single-backend consistent hashing is `S=1`, not this

If you run HAProxy, Traefik, Istio, Envoy, or a cloud L7 load balancer and reach
for its built-in hashing, **there is no subset-of-`S` configuration, and the
closest thing is weaker.** What those layers offer is single-backend consistent
hashing: HAProxy's `balance hdr(...)`, Istio's
`DestinationRule.trafficPolicy.loadBalancer.consistentHash.httpHeaderName`,
Envoy's ring-hash and Maglev policies, and the session-persistence extensions in
Gateway API implementations all map one key onto **one** backend. That is `S=1`.
It is a real and useful mode, and it still divides the flush cost by
`replicas`, but it is not what `subsetSize: 2` means: a tenant pinned to a single replica
loses all of its capacity when that replica restarts and has to be rehashed
somewhere else, which is exactly the failure the default subset of two exists to
avoid. Ravel does not present those configurations as a migration of subset
affinity, and neither should a runbook. The operator does not generate them
either. You can use `spec.gateway.ingestAffinity.annotations` to carry your
controller's own annotations onto the legacy Ingress objects, or configure that
layer yourself and leave `ingestAffinity` unset, but read what you configure as
`S=1` unless the layer genuinely implements subset-of-`S` selection. For real
subset-of-`S` behind any Gateway implementation, use `backend: ravelNative`.

## Admission rejections

Two combinations are rejected by the API server at admission (CEL
`x-kubernetes-validations` rules), so a bad manifest fails on `kubectl apply`
with a clear message rather than degrading silently at runtime:

1. **Gateway API exposure with an enabled legacy backend.** Setting
   `gateway.exposure.gatewayApi` while `ingestAffinity` is enabled on
   `backend: ingressNginx` is rejected:

   > `gateway.exposure.gatewayApi cannot be combined with an enabled
   > gateway.ingestAffinity on backend: ingressNginx -- traffic on the Gateway
   > API path would bypass the nginx subset annotations entirely; use backend:
   > ravelNative or disable ingestAffinity`

2. **Canonical-tenant key on the legacy backend.** Setting
   `key.source: canonicalTenant` while `backend` is `ingressNginx` is rejected,
   because ingress-nginx has no way to run the tenant-resolution chain that
   canonical-tenant hashing needs:

   > `gateway.ingestAffinity.key.source canonicalTenant requires backend:
   > ravelNative -- ingress-nginx cannot run the ravel-tenant-resolve auth chain
   > that canonical-tenant hashing needs`

   Because `backend` defaults to `ingressNginx`, a manifest that sets
   `canonicalTenant` but omits `backend` is rejected too: the omitted default
   *is* `ingressNginx`.

## Render-time degradation (not a crashloop)

Two misconfigurations are caught by the operator at render time rather than at
admission, because they depend on cluster state the API server does not see. In
both cases the operator renders **no router objects** and writes a `Degraded`
condition on the `RavelCluster` status. This is a deliberate operator-side
validation, so the cluster fails visibly instead of scheduling a pod that would
never run or would crashloop:

- **`routerImage` unset under `backend: ravelNative`.** The router is a
  different binary from `spec.image` (which is `ravel-server`), so there is
  nothing to fall back to. The `Degraded` condition's reason is
  **`RouterImageMissing`**.
- **`key.source: canonicalTenant` with no resolver configured.** The router's
  own CLI refuses to start under `canonical-tenant` unless at least one resolver
  is present, and the only resolver the CRD wires through is
  `tenantTokensSecretRef`. If that Secret is absent, or present but resolves to
  zero tenant keys this reconcile, no `--tenant-token` flag would render and
  the router would crashloop at startup. The operator renders nothing instead;
  the `Degraded` condition's reason is **`CanonicalTenantResolverMissing`**.

Only the router degrades: the gateway, query, and maintain Deployments still
reconcile normally. Fix the field named in the condition message and the router
renders on the next reconcile.

If Gateway API exposure is also configured, a degraded router pass does not
strand the HTTPRoute either: the operator computes the router's render outcome
before rendering routes, so the HTTPRoute falls back to targeting the gateway
Service directly whenever the router will not exist that pass, rather than
pointing at a router Service the same reconcile just swept away.

## Migrating from `ingressNginx` to `ravelNative`

Moving from `backend: ingressNginx` to `backend: ravelNative` is the supported
migration. The ingress-nginx rendering path is deprecated but not removed:
`ingressNginx` remains the schema default and keeps working, and there is no
removal timeline.

Switching an existing production cluster is a **real migration, not a drop-in
toggle.** It has behavior differences you must plan for:

- **A new service to run.** `ravelNative` renders a `ravel-ingest-router`
  Deployment (and Service/ServiceAccount/Role/RoleBinding). It needs an image:
  set `ingestAffinity.routerImage` or the operator degrades with
  `RouterImageMissing`.
- **RBAC to grant.** The operator must be able to create the router's
  ServiceAccount, Role, and RoleBinding. This is part of the operator's shipped
  ClusterRole (see [kubernetes.md](kubernetes.md)); confirm your deployment of
  the operator carries it.
- **HTTP-only.** OTLP/gRPC is not subset-pinned through the router (see
  [the limitation above](#current-limitation-http-only-no-grpc-through-the-router)).
  If your request bill is gRPC-dominated, the saving is smaller than the HTTP
  math suggests.
- **TLS moves.** If you were relying on `ingestAffinity.tlsSecretName`, that
  field is legacy-backend-only. Under Gateway API exposure you configure
  `tls.certificateRefs` on your `Gateway`'s listener yourself, pointing at the
  same or an equivalent Secret; there is nothing the operator carries forward
  automatically.
- **A one-time rebalance.** Cutting over changes the routing layer, which every
  tenant's key now hashes through differently. Expect a bounded PUT-rate bump as
  buffers re-home, the same shape as any rebalance below.

A workable sequence: set `routerImage` and (if using it) confirm the
tenant-tokens Secret is populated; apply `backend: ravelNative`; if you also
want Gateway API exposure, add `exposure.gatewayApi` in the same or a following
apply and move your `Gateway` listener's TLS across; watch the flush/PUT rate
settle (see [Verifying it works](#verifying-it-works)); then decommission the
old ingress-nginx `Ingress` for this cluster once traffic has moved.

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

Everything else defaults: enabled, backend `ingressNginx`, subset size 2, key =
the `Authorization` header, and a second Ingress for OTLP/gRPC.

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

For a `RavelCluster` named `prod`, which objects render depends on the backend
and on whether exposure is set.

Under `backend: ingressNginx` (enabled):

| Object | Kind | Notes |
|---|---|---|
| `prod-gateway-ingest` | Ingress | OTLP/HTTP on port 4318, on the `/v1/metrics`, `/v1/logs`, and `/v1/traces` paths only. |
| `prod-gateway-ingest-grpc` | Ingress | OTLP/gRPC on port 4317, `backend-protocol: GRPC`, on the four gRPC ingest service paths. Absent when `grpc: false`. |

Under `backend: ravelNative` (enabled, `routerImage` set): the five
`prod-ingest-router` objects listed in
[The managed objects and their RBAC](#the-managed-objects-and-their-rbac). No
`Ingress` is rendered.

Under `gateway.exposure.gatewayApi` (independent of backend):

| Object | Kind | Notes |
|---|---|---|
| `prod-gateway-route` | HTTPRoute | Attached to `gatewayRef`. Backs onto the router's Service under `ravelNative`, otherwise the gateway Service. |
| `prod-gateway-route-grpc` | GRPCRoute | Attached to `gatewayRef`. **Always** backs onto the gateway Service directly (see the HTTP-only limitation). Absent when `exposure.gatewayApi.grpc: false`. |

Every object is owned by the `RavelCluster` and deleted with it. All are also
deleted when `enabled` becomes `false` or the mode changes, so switching modes
converges rather than leaving an orphan routing live traffic.

Two legacy Ingress objects are needed because `backend-protocol` is a per-Ingress
annotation: one Ingress cannot speak HTTP to one port and gRPC to another.

**The two Ingress objects route on disjoint paths, never a shared `/`.**
ingress-nginx builds one `location` per path in a server block, so if both
Ingress objects claimed the same host and path `/` it would be a duplicate-path
conflict: ingress-nginx keeps one location (ordered by CreationTimestamp,
tie-broken by namespace and name) and drops the other's with a warning.
`prod-gateway-ingest` sorts before `prod-gateway-ingest-grpc`, so the HTTP object
would win and the gRPC object's `grpc_pass` and its affinity annotations would
never take effect, so OTLP/gRPC, the primary ingest path, would be proxied as
HTTP/1.1 and fail. OTLP/HTTP and OTLP/gRPC have disjoint path namespaces, so each
Ingress serves only its own paths and the two never collide, with or without
`hosts`.

The HTTP Ingress serves the three OTLP/HTTP routes:

- `/v1/metrics`
- `/v1/logs`
- `/v1/traces`

The gRPC Ingress serves the full service name of every gRPC service the gateway
registers on the ingest surface. There are four, including OTAP's
`ArrowMetricsService`:

- `/opentelemetry.proto.collector.metrics.v1.MetricsService`
- `/opentelemetry.proto.collector.logs.v1.LogsService`
- `/opentelemetry.proto.collector.trace.v1.TraceService`
- `/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService`

Each path is a full gRPC service name, which is a complete path element under
`PathType: Prefix` (Kubernetes matches Prefix element-wise, splitting on `/`).
A full service name therefore prefixes `/<service>/<method>` correctly under
both the strict spec semantics and ingress-nginx's rendering. A truncated
common prefix such as `/opentelemetry.proto.collector.` is a single element that
equals none of the service names and matches nothing under the spec, and it
would also silently omit the OTAP service, so the full names are used.

### Two things to set that the operator will not set for you

**TLS.** Under the legacy backend, ingress-nginx serves HTTP/2 to clients over
TLS. OTLP/gRPC needs HTTP/2. Without `tlsSecretName` the gRPC Ingress will not
work for most clients. Independently: tenant tokens are bearer tokens, so
plaintext ingest exposes every tenant's credential on the wire. Under Gateway
API exposure the equivalent is `tls.certificateRefs` on your `Gateway`'s
listener, which you configure directly.

**Body size.** ingress-nginx defaults `proxy-body-size` to `1m` and rejects
larger requests with a 413. A batched OTLP/HTTP export can exceed that. The
operator does not silently raise it: pick a value and set it yourself (legacy
backend):

```yaml
      annotations:
        nginx.ingress.kubernetes.io/proxy-body-size: "16m"
```

## Sizing the subset

Start at the default of 2 and raise it only for a measured reason.

- **The saving is `replicas / subsetSize`.** Going from 2 to 4 halves the saving.
  There is no point raising it past the point where a tenant is actually
  throughput-bound.
- **The ceiling is a tenant's throughput.** If a tenant's exporters are being
  throttled, or its subset's pods are pegged while the rest of the Deployment
  idles, its subset is too small. That is the signal to raise `subsetSize`.
- **`subsetSize` >= `replicas` disables the saving.** Every tenant then reaches
  every replica, which is exactly the pre-affinity behaviour with extra moving
  parts.
- **Keep `replicas` a multiple of `subsetSize`** where you can. The legacy
  ingress-nginx backend partitions the endpoint list into groups of `subsetSize`;
  a remainder produces one undersized group whose tenants get less capacity than
  their siblings. (`ravelNative`'s rendezvous hashing does not partition into
  fixed groups, so this is less pronounced there, but a multiple still keeps the
  distribution evenest.)
- **Do not size for the largest tenant.** `subsetSize` is one number for the
  whole cluster: there is no per-tenant subset size, so raising it for one tenant
  raises everyone's cost. A tenant that genuinely needs a much larger subset than
  its siblings belongs in its own `RavelCluster`, which also gives it its own
  gateway Deployment to be bounded by.

A practical rule: `subsetSize = 2`, `replicas` sized so that a subset carries the
largest tenant's peak with one replica to spare.

## Choosing the key

**`authorizationHeader` (default).** Hashes the `Authorization` header, which is
the credential Ravel itself resolves tenancy from. Nothing on the client needs
to change. Two consequences to know:

- A tenant using several distinct tokens (per-agent credentials, for example)
  hashes to several subsets. Affinity still works, it just divides less. One
  token per tenant gives the full saving.
- **Rotating a token moves that tenant to a different subset**, which is a
  rebalance (see below). This is usually invisible, but it means a token
  rotation and a rolling restart at the same moment move a tenant twice.
  `canonicalTenant` avoids this; see below.

**`header` + `headerName`.** Hashes a named header, such as one a trusted
upstream proxy stamps. Only use this if clients cannot set the header
themselves. Otherwise a tenant can choose its own subset, and a misbehaving one
can pin itself onto a busy subset.

**`mtlsSubject`.** Hashes the mTLS client certificate subject
(`$ssl_client_s_dn` under ingress-nginx). Requires the terminating layer to do
TLS with client-certificate authentication configured. If it is not, the subject
is empty, every request hashes to the same key, and **every tenant lands on one
subset**, a much worse outcome than no affinity. Verify client-cert
authentication is actually on before selecting this.

**`canonicalTenant` (`ravelNative` only).** Hashes the canonical `TenantId` that
Ravel's own resolver produces, not any raw wire value. **Immune to token
rotation**: rotating a tenant's token does not move it to a new subset. This is
the one key source that survives a rotation cleanly. Caveats: it is rejected on
`backend: ingressNginx` (nginx cannot run the resolver), and it only works for
clusters authenticating with static tenant tokens (`tenantTokensSecretRef`),
because OIDC and mTLS resolution have no CRD surface. Selecting
`canonicalTenant` without a resolver degrades the router with
`CanonicalTenantResolverMissing`. See
[The `canonicalTenant` key source](#the-canonicaltenant-key-source).

Under the legacy backend, header names are lowercased with every character
outside `[a-z0-9]` mapped to `_`, matching nginx's own `$http_<name>` variable
naming: `X-Scope-OrgID` becomes `$http_x_scope_orgid`. The CRD additionally
rejects header names outside HTTP token characters (`^[A-Za-z0-9][A-Za-z0-9-]{0,62}$`),
so nothing user-supplied can reach the nginx configuration as syntax.

## Rolling restarts and replica loss

Both are rebalance events. Neither is a correctness event. This applies to both
backends; the mechanism differs slightly.

**What happens on a rebalance.** The endpoint list changes, subsets are
recomputed, and some tenants move to a different subset. A moved tenant's next
write opens a fresh buffer on its new replica with a new `writer_id` and
`epoch`, while its old replica still holds an unflushed buffer that its own age
timer will flush shortly after. So a rebalance costs one extra flush per moved
`(tenant, signal, shard)`, a brief bounded uptick in PUTs, not a step change.
Nothing is lost: the old replica's flush completes and commits normally, and
both objects are valid because writer identity is part of the key.

**Replica loss.** The routing layer drops the endpoint as soon as the
EndpointSlice updates (ingress-nginx watches it; `ravelNative` watches it
directly). A tenant whose subset lost a member keeps writing to its surviving
member at full correctness and roughly half its previous subset capacity, until
the replacement pod is Ready and the subsets recompute. This is exactly why the
default subset is two and not one: at one, a replica loss stalls that tenant
until the reroute completes.

**Rolling restart.** Every pod is replaced, so the endpoint set changes several
times and most tenants move at least once. Under `ravelNative`, because a replica
is identified by pod UID, a full gateway rollout replaces the entire set at once
and causes a one-time full reassignment by construction, so expect the PUT-rate
bump to cover the whole roll. Under either backend, keeping
`maxSurge`/`maxUnavailable` conservative keeps the endpoint set changing in
small steps, which moves fewer tenants per step.

**One caveat for the legacy backend.** ketama hashing bounds how many *keys*
remap when the endpoint set changes, but ingress-nginx builds subsets by
partitioning the endpoint list, so a change in the number of endpoints can
reshuffle subset *membership* more broadly than the key remapping alone
suggests. A scale from 10 replicas to 11 is not a 1-in-11 disturbance; it
regroups the partition. `ravelNative`'s rendezvous hashing has the tighter HRW
guarantee here (only tenants whose rank crosses position `S` move on a single
add/remove), but treat any scaling event as a rebalance whose cost you have
chosen to pay, and scale in steps of `subsetSize` where you can.

**Long-lived OTLP connections do not pin anything.** A collector holds one
HTTP/2 connection to the routing layer for hours, but the routing decision is
re-evaluated per request (per gRPC call, per HTTP export), so a rebalance takes
effect on the next request without the client reconnecting. The long-lived
connection is between the client and the *router/ingress*, not between the client
and a gateway pod. The one thing that does persist is the reverse: a client that
keeps a connection open through a full gateway rollout never notices, because the
routing layer absorbs it.

## Verifying it works

Affinity failing silently is the main risk, so check rather than assume.

Under the legacy backend, check the rendered annotations:

```sh
kubectl get ingress prod-gateway-ingest -o jsonpath='{.metadata.annotations}'
```

All four affinity annotations must be present, and `service-upstream` must be
`false`.

Under `ravelNative`, check that the router objects rendered and that its status
is not degraded:

```sh
kubectl get deploy,svc,role,rolebinding,sa \
  -l app.kubernetes.io/component=ingest-router
kubectl get ravelcluster prod -o jsonpath='{.status.conditions}'
```

A `Degraded` condition with reason `RouterImageMissing` or
`CanonicalTenantResolverMissing` means the router did not render; fix the field
it names.

Then confirm the effect where it matters, for either backend: the flush and PUT
rate. Watch the gateway Deployment's flush counters (see
[observability.md](observability.md)) across enabling affinity. With `R`
replicas and subset size 2 the flush rate should fall toward `2/R` of its
previous value once every buffer has aged out. If it does not move, the traffic
is not going through the routing layer, or (legacy) `service-upstream` is true
somewhere, or every request is carrying the same key.

## Turning it off

```sh
kubectl patch ravelcluster prod --type merge \
  -p '{"spec":{"gateway":{"ingestAffinity":{"enabled":false}}}}'
```

The operator deletes the rendered objects (Ingress objects under the legacy
backend, or the router objects under `ravelNative`) on the next reconcile and
renders exactly what it rendered before affinity existed. Ingest keeps working
through whatever else routes to the gateway Service. Removing the
`ingestAffinity` block entirely does the same thing; `enabled: false` is there so
you can do it without losing the rest of the configuration.

## See also

- [kubernetes.md](kubernetes.md): the operator and the full `RavelCluster`
  reference.
- [ingest.md](ingest.md): the OTLP endpoints and how tenancy is authenticated.
- [observability.md](observability.md): the metrics to watch the saving on.
- [cost-model.md](cost-model.md): the other levers on the same request bill.

## Background

Why request cost dominates, and the other three levers on it, are
[ADR-0076](../adrs/0076-reducing-s3-request-cost.md); subset affinity itself is
its decision 1. The exposure and affinity split, the Ravel-native router, and
the canonical-tenant key source are
[ADR-0080](../adrs/0080-gateway-api-ingest-affinity.md), decisions 2, 3 and 1.
The process-wide ingest buffer budget the memory note refers to is ADR-0069
decision 1.
