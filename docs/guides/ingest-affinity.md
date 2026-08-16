# Ingest affinity: pinning a tenant to a subset of gateway replicas

Ravel's object-storage bill is dominated by request charges, not stored bytes,
and the number of requests scales with how many gateway replicas a tenant's
writes land on. Ingest affinity pins each tenant to a small, stable subset of
replicas so the same data is flushed once per subset instead of once per
replica.

This is ADR-0076 decision 1. It changes no format, no contract, and no
acknowledgement latency. It is configuration.

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

The saving compounds with the other ADR-0076 levers (shard count, flush
cadence), because they are different terms of the same product.

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
  bytes than an evenly sprayed replica would. The ADR-0069 global ingest budget
  still caps this per process; the effect is that the cap is reached sooner on a
  hot subset.
- **Load is only as even as the hash.** With a handful of tenants the
  distribution across subsets is visibly lumpy. Affinity pays off with many
  tenants, not with three.

Correctness is not on the list. Affinity is best-effort by design: `writer_id`
and `epoch` already disambiguate concurrent writers in the object key, so a
request that lands on a replica outside its usual subset writes a perfectly
valid object. Rerouting is a cost event, never a correctness event.

## What actually does the routing

Kubernetes cannot express this on its own. A core `Service` offers only
`sessionAffinity: ClientIP`, which keys on the client's source address — the
address of the OpenTelemetry Collector or the gateway proxy in front of it, not
of the tenant. Under a shared collector that maps every tenant onto one key.
The operator therefore never sets it.

Tenant identity lives in the request's authentication material. Ravel resolves
tenancy server-side from the bearer token, and OTLP connections are long-lived,
so the URL path carries nothing routable either. The routing decision has to be
made at layer 7 by something that can read a header.

**Ravel does not ship an ingress controller, and this feature does not add
one.** What the operator ships is the `Ingress` object and the annotations that
configure it. The cluster must already run an ingress controller that
understands them. The supported and tested target is
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
has exactly one server — the Service's ClusterIP — kube-proxy picks the pod, and
the hash has nothing to distribute over. The affinity would silently do nothing.
The controller's own default is `false`, but it is settable cluster-wide in the
ingress-nginx ConfigMap, so the operator always renders it explicitly.

If you run HAProxy, Traefik, Istio, Envoy, or a cloud L7 load balancer instead,
**there is no equivalent configuration, and the closest thing is weaker.** What
those layers offer is single-backend consistent hashing: HAProxy's
`balance hdr(...)`, Istio's
`DestinationRule.trafficPolicy.loadBalancer.consistentHash.httpHeaderName`,
Envoy's ring-hash and Maglev policies, and the session-persistence extensions in
Gateway API implementations all map one key onto **one** backend. That is `S=1`.
It is a real and useful mode — it still divides the flush cost by `replicas` —
but it is not what `subsetSize: 2` means: a tenant pinned to a single replica
loses all of its capacity when that replica restarts and has to be rehashed
somewhere else, which is exactly the failure the default subset of two exists to
avoid. Ravel does not present those configurations as a migration of subset
affinity, and neither should a runbook. The operator does not generate them
either. You can use `spec.gateway.ingestAffinity.annotations` to carry your
controller's own annotations onto the same Ingress objects, or configure that
layer yourself and leave `ingestAffinity` unset — but read what you configure as
`S=1` unless the layer genuinely implements subset-of-`S` selection.

### The ingress-nginx backend is deprecated

`backend: ingressNginx` is the default and it keeps working unchanged, but
ingress-nginx is retiring upstream, so it is deprecated (ADR-0080 decision 1).
A cluster on it gets an `IngestAffinityBackendDeprecated` condition on its
`RavelCluster` status, with reason `IngressNginxRetired`; the condition
disappears once the cluster moves off that backend. Nothing about an existing CR
changes on upgrade: a CR that never set `backend` deserializes to
`ingressNginx`, with the same `subsetSize` and the same key. The migration
target is `backend: ravelNative`, Ravel's own subset router, which does
rendezvous-hash subset-of-`S` selection independent of whichever ingress or
Gateway implementation terminates the connection. That backend ships in a later
change; until it does, `ravelNative` is accepted by the CRD and silences the
condition but the rendered objects are still today's. Do not switch a production
cluster to it before its own release note says the router is rendered.

### Gateway API exposure

`gateway.exposure.gatewayAPI` is a separate, independent field from
`ingestAffinity` (ADR-0080 decision 2): it renders standard
`gateway.networking.k8s.io` `HTTPRoute` and `GRPCRoute` objects attached to an
existing `Gateway`, instead of the ingress-nginx-specific `Ingress` objects
above. It carries no vendor extension, so it works with any conformant
Gateway API implementation (Envoy Gateway, NGINX Gateway Fabric, Cilium,
Istio, a managed cloud implementation) — Ravel does not couple its CRD to one.

```yaml
spec:
  gateway:
    exposure:
      gatewayAPI:
        gatewayRef:
          name: public-gateway
          # namespace: defaults to this RavelCluster's own namespace
        hostnames: [ingest.example.com]
        grpc: true   # also render a GRPCRoute; default true
```

This has no tenant-affinity effect by itself: routing goes straight to the
gateway Service, load-balanced however the Gateway implementation load-balances
a Service backendRef (typically endpoint-aware round robin, not subset-of-`S`).
It can be combined with `backend: ravelNative` affinity once that backend ships
(a later change points the rendered routes at the router's Service instead);
it **cannot** be combined with an *enabled* `backend: ingressNginx` affinity —
the CRD rejects that combination at admission with a CEL rule, because traffic
on the Gateway API path would bypass the nginx subset annotations entirely,
silently losing tenant pinning on that path while the Ingress objects keep
enforcing it on theirs. If you need Gateway API exposure today, either leave
`ingestAffinity` unset/disabled, or wait for `backend: ravelNative`.

TLS is not rendered by the operator here: Gateway API exposure terminates TLS
at the referenced `Gateway`'s own listener, which you configure directly
(`tls.certificateRefs`) — there is no `tlsSecretName` equivalent under
`exposure.gatewayAPI`, unlike the legacy `ingestAffinity.tlsSecretName`.

Requires Gateway API **v1.1 or newer** in the cluster: `GRPCRoute` only
reached the stable `v1` API version in Gateway API v1.1 (it was `v1alpha2`
in v1.0), and the operator renders it at `v1`.

## Turning it on

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

That is the whole thing. Everything else defaults: enabled, subset size 2, key
= the `Authorization` header, and a second Ingress for OTLP/gRPC.

### Fields

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | boolean | `true` | `false` deletes the Ingress objects and returns to the pre-affinity render. The incident switch. |
| `backend` | enum | `ingressNginx` | `ingressNginx` (deprecated, see above) or `ravelNative`. Omitting it keeps the backend an existing CR already runs. |
| `subsetSize` | integer | `2` | Replicas per tenant. Must be at least 1. |
| `key.source` | enum | `authorizationHeader` | `authorizationHeader`, `header`, or `mtlsSubject`. |
| `key.headerName` | string | — | Required when `key.source` is `header`. |
| `ingressClassName` | string | — | Omit to use the cluster's default IngressClass. |
| `hosts` | list | `[]` | Empty renders one host-less rule matching any host that reaches the controller. |
| `tlsSecretName` | string | — | Renders `spec.tls`. Effectively required, see below. |
| `grpc` | boolean | `true` | Also render an Ingress for OTLP/gRPC on port 4317. |
| `annotations` | map | `{}` | Merged onto both Ingress objects, *before* the affinity annotations, which therefore always win. |

### Managed objects

For a `RavelCluster` named `prod`:

| Object | Kind | Notes |
|---|---|---|
| `prod-gateway-ingest` | Ingress | OTLP/HTTP on port 4318, on the `/v1/metrics`, `/v1/logs`, and `/v1/traces` paths only. |
| `prod-gateway-ingest-grpc` | Ingress | OTLP/gRPC on port 4317, `backend-protocol: GRPC`, on the four gRPC ingest service paths. Absent when `grpc: false`. |

Both are owned by the `RavelCluster` and deleted with it. Both are also deleted
when `enabled` becomes `false`, so turning affinity off converges rather than
leaving an orphan routing live traffic.

Two objects are needed because `backend-protocol` is a per-Ingress annotation:
one Ingress cannot speak HTTP to one port and gRPC to another.

**The two objects route on disjoint paths, never a shared `/`.** ingress-nginx
builds one `location` per path in a server block, so if both Ingress objects
claimed the same host and path `/` it would be a duplicate-path conflict:
ingress-nginx keeps one location (ordered by CreationTimestamp, tie-broken by
namespace and name) and drops the other's with a warning. `prod-gateway-ingest`
sorts before `prod-gateway-ingest-grpc`, so the HTTP object would win and the
gRPC object's `grpc_pass` and its affinity annotations would never take effect —
OTLP/gRPC, the primary ingest path, would be proxied as HTTP/1.1 and fail.
OTLP/HTTP and OTLP/gRPC have disjoint path namespaces, so each Ingress serves
only its own paths and the two never collide, with or without `hosts`.

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

**TLS.** ingress-nginx serves HTTP/2 to clients over TLS. OTLP/gRPC needs
HTTP/2. Without `tlsSecretName` the gRPC Ingress will not work for most clients.
Independently: tenant tokens are bearer tokens, so plaintext ingest exposes
every tenant's credential on the wire.

**Body size.** ingress-nginx defaults `proxy-body-size` to `1m` and rejects
larger requests with a 413. A batched OTLP/HTTP export can exceed that. The
operator does not silently raise it — pick a value and set it yourself:

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
- **Keep `replicas` a multiple of `subsetSize`** where you can. ingress-nginx
  partitions the endpoint list into groups of `subsetSize`; a remainder produces
  one undersized group whose tenants get less capacity than their siblings.
- **Do not size for the largest tenant.** `subsetSize` is one number for the
  whole cluster: ingress-nginx has no per-tenant subset size, so raising it for
  one tenant raises everyone's cost. A tenant that genuinely needs a much larger
  subset than its siblings belongs in its own `RavelCluster`, which also gives
  it its own gateway Deployment to be bounded by.

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

**`header` + `headerName`.** Hashes a named header, such as one a trusted
upstream proxy stamps. Only use this if clients cannot set the header
themselves — otherwise a tenant can choose its own subset, and a misbehaving one
can pin itself onto a busy subset.

**`mtlsSubject`.** Hashes the mTLS client certificate subject
(`$ssl_client_s_dn`). Requires the ingress controller to terminate TLS with
client-certificate authentication configured. If it is not, the variable is
empty, every request hashes to the same key, and **every tenant lands on one
subset** — a much worse outcome than no affinity. Verify client-cert
authentication is actually on before selecting this.

Header names are lowercased with every character outside `[a-z0-9]` mapped to
`_`, matching nginx's own `$http_<name>` variable naming: `X-Scope-OrgID`
becomes `$http_x_scope_orgid`. The CRD additionally rejects header names outside
HTTP token characters, so nothing user-supplied can reach the nginx
configuration as syntax.

## Rolling restarts and replica loss

Both are rebalance events. Neither is a correctness event.

**What happens on a rebalance.** The endpoint list changes, subsets are
recomputed, and some tenants move to a different subset. A moved tenant's next
write opens a fresh buffer on its new replica with a new `writer_id` and
`epoch`, while its old replica still holds an unflushed buffer that its own age
timer will flush shortly after. So a rebalance costs one extra flush per moved
`(tenant, signal, shard)` — a brief, bounded uptick in PUTs, not a step change.
Nothing is lost: the old replica's flush completes and commits normally, and
both objects are valid because writer identity is part of the key.

**Replica loss.** ingress-nginx drops the endpoint as soon as the EndpointSlice
updates. A tenant whose subset lost a member keeps writing to its surviving
member at full correctness and roughly half its previous subset capacity, until
the replacement pod is Ready and the subsets recompute. This is exactly why the
default subset is two and not one: at one, a replica loss stalls that tenant
until the reroute completes.

**Rolling restart.** Every pod is replaced, so the endpoint set changes several
times and most tenants move at least once. Expect a PUT-rate bump for the
duration of the roll and a return to baseline after. Keeping
`maxSurge`/`maxUnavailable` conservative keeps the endpoint set changing in
small steps, which moves fewer tenants per step.

**One caveat the ADR does not cover.** ketama hashing bounds how many *keys*
remap when the endpoint set changes, but ingress-nginx builds subsets by
partitioning the endpoint list, so a change in the number of endpoints can
reshuffle subset *membership* more broadly than the key remapping alone
suggests. A scale from 10 replicas to 11 is not a 1-in-11 disturbance; it
regroups the partition. Scale the gateway in steps of `subsetSize` when you can,
and treat a scaling event as a rebalance whose cost you have chosen to pay.

**Long-lived OTLP connections do not pin anything.** A collector holds one
HTTP/2 connection to the ingress controller for hours, but nginx re-evaluates
the upstream per request (per gRPC call, per HTTP export), so a rebalance takes
effect on the next request without the client reconnecting. The long-lived
connection is between the client and the *ingress*, not between the client and a
gateway pod. The one thing that does persist is the reverse: a client that keeps
a connection open through a full gateway rollout never notices, because the
ingress absorbs it.

## Verifying it works

Affinity failing silently is the main risk, so check rather than assume.

```sh
kubectl get ingress prod-gateway-ingest -o jsonpath='{.metadata.annotations}'
```

All four affinity annotations must be present, and `service-upstream` must be
`false`.

```sh
kubectl exec -n ingress-nginx deploy/ingress-nginx-controller -- \
  cat /etc/nginx/nginx.conf | grep -A3 'upstream_balancer'
```

Then confirm the effect where it matters: the flush and PUT rate. Watch the
gateway tier's flush counters (see [observability.md](observability.md)) across
enabling affinity. With `R` replicas and subset size 2 the flush rate should
fall toward `2/R` of its previous value once every buffer has aged out. If it
does not move, the traffic is not going through the Ingress, or
`service-upstream` is true somewhere, or every request is carrying the same key.

## Turning it off

```sh
kubectl patch ravelcluster prod --type merge \
  -p '{"spec":{"gateway":{"ingestAffinity":{"enabled":false}}}}'
```

The operator deletes both Ingress objects on the next reconcile and renders
exactly what it rendered before affinity existed. Ingest keeps working through
whatever else routes to the gateway Service. Removing the `ingestAffinity` block
entirely does the same thing; `enabled: false` is there so you can do it without
losing the host and TLS configuration.

## See also

- [../adrs/0076-reducing-s3-request-cost.md](../adrs/0076-reducing-s3-request-cost.md)
  — why request cost dominates, and the other three levers.
- [kubernetes.md](kubernetes.md) — the operator and the full `RavelCluster`
  reference.
- [ingest.md](ingest.md) — the OTLP endpoints and how tenancy is authenticated.
- [observability.md](observability.md) — the metrics to watch the saving on.
