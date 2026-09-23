# Deployment (day 1)

Bringing a cluster up against a bucket for the first time, in the order the
steps have to happen. Everything here assumes the decisions on the
[configuration page](configuration.md) are already made.

The short version:

1. [Qualify the store](#qualify-the-store). A fresh production bucket cannot
   run a server until this has passed once.
2. [Configure bucket protection](#the-bucket-protection-contract) at the bucket
   layer, and decide whether startup gates on it.
3. [Start the first process](#the-first-deployment-against-a-fresh-bucket) and
   let it bootstrap the two control objects it needs.
4. [Check readiness](#readiness-and-the-store-reachability-probe), which is a
   live statement about store reachability, not just about startup.
5. If you are running distributed reads, provision
   [the fragment listener's TLS material](#the-dedicated-fragment-listener)
   and, if you federate, [the remote cluster credentials](#federating-to-a-remote-cluster).

## Qualify the store

Ravel's commit protocol and catalog assume the backing store honors conditional
writes, so a losing writer is rejected rather than silently overwriting, and
that a listing reflects every write that has already completed. A backend that
advertises those and does not deliver them violates durability without saying
so. Before a production store is trusted, it is qualified empirically, once per
bucket:

```sh
ravel-cli store qualify --store s3 --s3-endpoint ... --s3-bucket ...
```

On a pass this records a durable `sys/qualification` object naming the backend,
the suite version and the time. It is once per bucket, never per boot, and never
overwritten: a second run leaves the existing record alone.

**A fresh production deployment must run this before any server can start.** On
any store other than `memory`, `ravel-server` reads `sys/qualification` at
startup, in every mode, before any listener binds, and refuses to start when the
record is:

- **absent**: the backend has never been qualified. Run `ravel-cli store
  qualify`, then start the server.
- **stale**: recorded under a suite version below this binary's required floor.
  Re-run `ravel-cli store qualify` with a current build, then restart.

The two are reported as distinct, named errors. Unlike the tenancy marker and
the durable garbage-collection configuration, an absent qualification record is
deliberately not a bootstrap-and-continue case. There is no "assume qualified"
path, because a never-qualified backend has never been shown to honor the
guarantees Ravel's durability depends on. `--store memory` is exempt and never
needs qualification.

`store qualify` writes transient scratch objects under `sys/qualify/<run-id>/`
while it runs its suite, not only the final record. The Admin policy grants no
delete anywhere, so that scratch is never cleaned up by the credential itself.
It is bounded per run, but not small: the two listing probes write two keys
more than the declared page size each, so a run at the default page size
leaves about two thousand small objects, and repeated runs against the same
bucket accumulate them. Sweep the prefix from a runbook when it matters.

## The bucket protection contract

Some of what protects a Ravel bucket is configured at the bucket and policy
layer, not by any Ravel process. Nothing in `ravel-server` configures or
verifies it, because the object-store client exposes no such API. The normative
statement is
[the object store contract](../../object-store-contract.md)'s required bucket
configuration section; this is the operational summary.

Object Lock enabled on the bucket, versioning, and the lifecycle rules that go
with erasure obligations are bucket-layer settings. The compliance-mode
retention on the control prefixes (`sys/*`, the provisioning records, commit
records and the catalog keyspace `t/*/catalog/*/*`) is per-object retention
that an operator-run mechanism applies, because Object Lock has no prefix
scope of its own; the contract page describes the two shapes that mechanism
can take. Three of those four prefix families are never touched by the three
mechanisms that physically remove tenant data (supersession GC, retention
deletion, and subject erasure), so locking them costs nothing against those
three. Commit records are not exempt even that far: a maintenance sweep
physically removes a superseded commit record, and a still-locked one refuses
that delete until its retention period elapses, so the retention period chosen
for commit records is also a bound on how long that sweep can pause; the
contract page's "Required bucket configuration" names the default window to
keep it inside.

One of the other three families does carry a further cost, from a fourth
mechanism: a compliance lock on `t/*/catalog/*/*` there
costs an erasure obligation, not only a reclamation delay. The unreferenced-catalog sweep
deletes the snapshot and index objects the current HEAD no longer names, and
for a tenant that declares a typed string or bytes attribute column a per-part
column-statistics object among them holds that subject's own column value; a
lock over the keyspace delays that delete, and the value persists until the
fold reconciles that hour and then a further retention period. The maintenance
IAM policy Ravel ships permits that delete, with its catalog deny scoped to
`catalog/<signal>/HEAD`; a copy of that template predating the narrowing
denies it outright and leaves the bound open-ended until it is re-applied. The four-step mechanism, the exact bound, the IAM
ceiling and the HEAD-scoping advice are in the contract page's "Required
bucket configuration" section, "A lock on the catalog family".

**One lifecycle rule is not optional for any bucket Ravel writes to.**
Configure `AbortIncompleteMultipartUpload` with a cleanup period of seven days
or less. Nothing in `ravel-server` reaps orphaned multipart parts. A best-effort
abort that itself fails, or an upload future dropped mid-flight, leaves cleanup
unconfirmed, and S3 can apply an abort and still return an error, so parts may
remain billed until this rule reaps them. Two store counters make that
otherwise-silent failure visible: `multipart_abort_failures` counts best-effort
aborts whose request returned an error, and `multipart_uploads_unreaped` counts
uploads that ended without a confirmed successful abort for any reason. A
sustained rise in either means the lifecycle rule is the only thing bounding
orphaned-part cost on that bucket.

`--require-bucket-protection` turns the conformance probes, which are otherwise
informational, into a startup gate, so a deployment cannot go into production
silently unprotected:

- **Disabled**, or a versioning-without-expiration alarm, refuses to start.
- **Unknown**, which is what every backend reachable only through the
  object-store contract reports, because no adapter can answer the query,
  logs one warning and sets `ravel_bucket_protection_unknown` to `1` at
  `/metrics` rather than blocking startup.
- **Enabled** with no alarms starts clean, with the gauge at `0`.

The flag is off by default, so a development process that does not pass it
starts without the gate. The Kubernetes operator sets it unconditionally for
every cluster it reconciles: the custom resource carries no development or
staging profile field to gate on.

## The first deployment against a fresh bucket

Two control objects are created on the first authorized contact with an
empty bucket, `sys/tenancy` by any server role and `sys/gc` by Maintain or
Admin only, which is why their write grants are slightly broader than the
per-role tables imply. Know this before your first deployment:

- **`sys/tenancy`**, the marker that pins the tenant hash scheme, is created by
  whichever of the three server roles reaches a fresh bucket first. That is why
  Gateway, Query and Maintain all carry a write grant on it, not just Admin. It
  does not weaken the delete boundary: a create-if-absent write cannot overwrite
  or delete an existing object, and `sys/tenancy` is deny-delete for every role.
  The effect is only that a fresh operator-managed cluster boots without a
  manual bootstrap step.
- **`sys/gc`**, the durable garbage-collection configuration, is created by
  the first process to reach a fresh bucket, and only the Maintain and Admin
  roles carry a write grant on it. Under per-role credentials, start the
  `maintain` process first on a fresh bucket, or create the object with
  `ravel-cli gc-config set` under Admin; a gateway or query process that
  reaches an empty bucket under its scoped credential cannot create the object
  and does not start. The mutation path that changes an existing `sys/gc` is
  Admin-only, matching that it is an explicit operator action rather than
  something a server does on its own.

`sys/qualification` gets no such exception. It is written by the Admin
credential running `store qualify`, one time for the life of the bucket, and no
server-role policy grants a write on it.

A fresh bucket with the keyed tenant hash scheme, which is the default, refuses
to start without `--tenant-hash-key-file`. Pass `--tenant-hash-unkeyed`
explicitly if you intend the unkeyed scheme; the choice is permanent for that
bucket.

<a id="the-admin-credential"></a>

### The Admin credential

`ravel-cli` uses the Admin role, and unlike the three server roles it is not
provisioned by the Kubernetes operator. There is no cluster resource field for
it and no pod runs it. It is the broadest of the four credentials, able to read
every prefix and write every control object, so treat it as a privileged
operator credential rather than a service credential:

- Store it wherever your operators or CI jobs get their `RAVEL_S3_*` values for
  running `ravel-cli`, such as a CI secret store or an operator's short-lived
  session. Never in a long-running Deployment, and never in a Secret mounted
  into a server pod.
- It is used only by out-of-band operator and CI invocations: `store qualify`,
  `gc-config set`, `provision adopt`, legal holds, and the read-only inspection
  subcommands. No continuously running process should hold it.
- Even Admin cannot delete any of the protected prefixes, and it cannot delete
  anything else either, because it has no delete grant at all. A leaked Admin
  key can forge or overwrite control objects within its write grant, but it
  cannot make existing data disappear.

## Readiness and the store reachability probe

`/readyz` reflects store reachability, not just startup completion. It also
reflects ingest health: an `all` or `gateway` process turns 503 permanently once
one of its ingest shard actors is condemned (`shards_condemned > 0` at
`/metrics`, on any `signal`), which no probe can recover and which needs the
process rolled. The metrics pipeline condemns a shard only after it exhausts its
respawn budget, but the logs and spans pipelines never respawn, so a single
shard-actor death on either condemns immediately -- see
[troubleshooting.md](troubleshooting.md). The rest of
this section is about the store condition, the one that recovers on its own.

Each process
runs one background probe that reads the fixed `sys/tenancy` object every
`--store-probe-interval` (default `30s`, jittered so replicas do not probe in
lockstep). Readiness ANDs the startup latch, the drain latch, ingest health and
this probe's health; for the probe alone:

- After **four consecutive** failed probes, readiness flips and `/readyz`, and
  its Prometheus spelling `/-/ready`, return 503.
- The **first successful** probe flips it back to 200. The asymmetry is
  deliberate: four failures down, one success up.

At the default interval that is roughly two minutes of hysteresis before a fleet
is marked unready. A store outage that long means every data path is failing,
and marking the fleet unready is the truthful signal: traffic then fails fast at
the load balancer instead of timing out per request. The threshold is a fixed
constant rather than a flag, so it cannot be lowered to one and reintroduce
single-blip mass ejection.

`/readyz` itself makes no object-store call. The kubelet reads only an in-memory
value the background probe maintains. `/healthz`, liveness, is deliberately
unaffected by the probe and still means only that the process is alive: a store
outage must never make liveness fail and get healthy processes killed.

Plan for one consequence at rollout time. A deployment gated on readiness will,
correctly, halt while the store is unreachable.

The probe exports two samples at `/metrics`, so the outage is visible even where
nothing consumes `/readyz`:

- `ravel_store_reachable`, a gauge labeled by mode: 1 healthy, 0 unhealthy.
- `ravel_store_probe_failures_total`, a counter labeled by mode, incremented on
  every failed probe cycle even below the readiness threshold.

## Graceful shutdown and the pod grace period

On SIGTERM the server flips readiness to draining first, then flushes and joins
every ingest shard actor before it exits. That drain is what protects
buffered-mode ingest data on a rolling update: if the process is killed before
the drain finishes, the unflushed buffers are lost. The drain is bounded by
`--shutdown-timeout` (default `25s`). The full SIGTERM-to-exit worst case at the
shipped defaults is `32.5s`: the `25s` drain, plus up to `2.5s` for the
pre-drain heartbeat stop and readiness settle, plus a `5s` hard cap on the final
OTLP trace-exporter flush.

The operator sizes the pod's shutdown lifecycle against that budget, so the two
numbers cannot drift apart:

- `terminationGracePeriodSeconds` is set to **45s** on every ravel-server pod
  (gateway, query, and maintain). Kubernetes runs the `preStop` hook inside the
  grace period and only sends SIGTERM once it returns, so the grace period has
  to cover the `preStop` sleep plus the `32.5s` server budget plus headroom:
  `10s + 32.5s + 2.5s`, rounded up. The error is deliberately on the long side.
  A grace period shorter than the server's budget lets SIGKILL land mid-drain
  and lose buffered data, which is irreversible; a longer one only slows a
  rolling update's pod turnover by a few seconds.
- A `preStop` hook sleeps **10s** before SIGTERM. Endpoint removal and SIGTERM
  are concurrent, not ordered: when a pod is deleted, the kubelet sends SIGTERM
  at the same time the EndpointSlice removal begins propagating to every node's
  kube-proxy and to any external load balancer. Without the sleep the server can
  start draining while new requests are still routed to it. The `10s` covers
  kube-proxy reprogramming across nodes and typical cloud load-balancer drain
  under load. The hook uses the native `sleep` lifecycle action, not an
  `exec` of a `sleep` binary, because the container runs with a read-only root
  filesystem and every Linux capability dropped.

The operator renders no `--shutdown-timeout` flag, so the server runs at its
compiled default and `45s` is the correct grace period today. `--shutdown-timeout`
is configurable on the server itself; if a future CRD field exposes it, the grace
period must track it, staying above the new server budget plus the `preStop`
sleep.

## Durable auth refresh

On a keyed bucket, a request-serving process (`all`, `gateway`, `query`)
resolves bearer tokens against a cached copy of the durable `sys/auth` map as
well as the static and OIDC resolvers, and keeps that copy current with a
background refresh loop. The durable resolver is appended after the static and
OIDC chain, so it only ever answers a request the others could not.

The loop re-reads `sys/auth`. On success it advances the staleness gate; on any
read or decode failure it keeps the last known map and leaves the gate where it
is. If it cannot refresh for a hard multiple of the refresh horizon, the cached
map is treated as untrustworthy and token resolution fails closed. An unkeyed
bucket has no keyed-hash token map, so durable auth is unavailable there.

Three counters, all labeled by mode, surface the loop's health:

- `ravel_durable_auth_refresh_failures_total`: background refreshes that could
  not read or decode `sys/auth`.
- `ravel_durable_auth_on_miss_rereads_total`: off-horizon re-reads begun after
  the rate limiter, when the request path saw an unknown token.
- `ravel_durable_auth_stale_fail_closed_total`: token resolutions refused
  because the cached map was hard-stale.

Alert on the first of those, not the third. It begins incrementing the moment
refresh fails, one refresh interval apart, while the last known map still
serves. The third only starts once the horizon has already been crossed and auth
is failing closed. The gap between them is the grace window a fix has to land
in. Both alert rules are in
[troubleshooting](troubleshooting.md#durable-auth-refresh-is-failing).

<a id="the-dedicated-fragment-listener"></a>

## The dedicated fragment listener

Only relevant under `--distributed-query`. The flag requires
`--fragment-key-file`, and a process with the flag and no key file refuses to
start rather than exposing an unauthenticated fetch surface.

`--fragment-key-file` holds a short list of 32-byte cluster fragment keys, one
per non-empty line, each line 64 hexadecimal characters. Blank lines and lines
beginning with `#` are ignored. A file with no key line, or any line that is not
exactly 64 hexadecimal characters, fails startup. Several keys are accepted so a
key can be rotated by adding the new one, rolling the fleet, and then removing
the old one. It is a file rather than an inline value or an environment
variable, so the secret never appears in a process listing.

These keys mint and verify a per-tenant, per-query capability. A fragment fetch
is authorized by that capability and by nothing else. There is no shared
cluster-internal bearer token.

With the key file in place, the cluster-internal fragment surface, where one
query worker fetches a slice for another, can be moved off the public gRPC
listener onto a dedicated listener that terminates TLS in-process:
`--fragment-listener <addr>`, with `--fragment-tls-cert`, `--fragment-tls-key`
and `--fragment-tls-ca`. The public gRPC listener then serves only cross-cluster
federation with ordinary tenant credentials and refuses pinned fetches; the
dedicated listener serves pinned fetches only and refuses federation. Startup
refuses a `--fragment-listener` address equal to `--listen-http`,
`--listen-grpc` or `--mtls-listener`, so the separation holds by construction.

TLS here provides channel confidentiality, because per-tenant, per-query
capabilities travel on it, and server authenticity, so a coordinator can confirm
it dialed a real cluster worker rather than an interceptor that could harvest
capabilities. Authorization is always the capability, never the certificate:
coordinators verify every worker certificate against the pinned
`--fragment-tls-ca` with one fixed expected server name, `ravel-fragment`,
carried as a dNSName SAN in every worker certificate. Per-process certificate
identity is deliberately not required, so any certificate the dedicated CA
signed means "a fragment worker of this cluster". No identity is ever parsed
from a certificate.

TLS on this listener is mutual. The same `--fragment-tls-ca` is the CA the
listener verifies its callers against, so a peer presenting no certificate from
it is refused at the handshake, before any capability is read. A coordinator
presents this process's own `--fragment-tls-cert` and `--fragment-tls-key` when
it dials a peer: one key pair in both directions, because every fragment
process is both a worker and a coordinator. This narrows who may present a
capability; it does not change what authorizes a fetch.

**Ravel mints no certificates or keys.** The operator provisions the PEM files
out of band. The certificate and key are read once at startup, so certificate
rotation is a rolling restart. Requirements for the worker certificate:

- A `ravel-fragment` dNSName SAN. The SAN is verified, not the CN.
- `extendedKeyUsage = serverAuth, clientAuth`. Both are required: the same
  certificate serves inbound fragment fetches and is presented as client
  identity on outbound ones. A `serverAuth`-only certificate serves fragments
  but cannot dial them, a `clientAuth`-only one dials them but cannot serve
  them, and in each case the handshake in the missing direction fails.
  `anyExtendedKeyUsage` does not stand in for either: the TLS stack matches the
  required purpose exactly. Startup reads the certificate and refuses when
  either usage is absent, naming the file and the missing usage, so an upgrade
  from a release that documented `serverAuth` alone fails loudly instead of
  degrading every fan-out to coordinator-local execution. A certificate
  carrying no `extendedKeyUsage` extension at all is unconstrained and starts.
- Signed by the CA distributed as `--fragment-tls-ca` to every query node.

### With cert-manager

Issue one certificate per query node, or a shared one since identity is not per
process, from a cluster-internal issuer, with the fixed SAN:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ravel-fragment
spec:
  secretName: ravel-fragment-tls   # projects tls.crt, tls.key, ca.crt
  duration: 720h                    # 30d; rotation is a rolling restart
  renewBefore: 168h
  privateKey:
    algorithm: ECDSA
    size: 256
  usages:
    - server auth
    - client auth                   # coordinators present it when they dial
  dnsNames:
    - ravel-fragment                # the one fixed expected server name
  issuerRef:
    name: ravel-fragment-ca         # a dedicated cluster-internal CA issuer
    kind: Issuer
    group: cert-manager.io
```

Mount the Secret and point the flags at the projected paths:

```sh
ravel-server --mode all --distributed-query \
  --listen-grpc 0.0.0.0:4317 \
  --fragment-key-file /etc/ravel/fragment-keys \
  --fragment-listener 0.0.0.0:4319 \
  --fragment-tls-cert /etc/ravel/fragment-tls/tls.crt \
  --fragment-tls-key  /etc/ravel/fragment-tls/tls.key \
  --fragment-tls-ca   /etc/ravel/fragment-tls/ca.crt \
  --advertise-fragment-endpoint "$POD_IP"
```

Both listeners bind the wildcard so they answer on the pod's own address, but
the heartbeat record must publish addresses siblings can dial, so
`--advertise-fragment-endpoint` is required here. Project the pod IP with the
downward API (`fieldRef: status.podIP`), or pass the pod's stable DNS name from
a headless Service. Without it, startup refuses rather than publishing
`0.0.0.0:4319` for every peer to fail against.

`--listen-grpc` is not optional in this example. The advertised host applies to
both published endpoints, and the Flight SQL endpoint the SQL lane dials is
always the public gRPC listener, which defaults to `127.0.0.1:4317`. Leaving
the default in place advertises `$POD_IP:4317` to peers while nothing outside
the pod's own loopback answers there, so every distributed SQL slice fetch
fails at connect.

cert-manager rewrites the Secret on renewal, but Ravel reads the files only at
startup, so schedule a rolling restart of the query fleet on the renewal
cadence.

### With a hand-provisioned CA

Run a small cluster-internal CA by hand and issue a worker certificate with the
fixed SAN:

```sh
# One dedicated CA for the fragment surface.
openssl ecparam -genkey -name prime256v1 -out fragment-ca.key
openssl req -x509 -new -key fragment-ca.key -sha256 -days 3650 \
  -subj "/CN=ravel-fragment-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out fragment-ca.crt

# One worker certificate, SAN = ravel-fragment, EKU serverAuth + clientAuth.
openssl ecparam -genkey -name prime256v1 -out fragment.key
openssl req -new -key fragment.key -subj "/CN=ravel-fragment" -out fragment.csr
cat > fragment.ext <<'EOF'
subjectAltName = DNS:ravel-fragment
extendedKeyUsage = serverAuth, clientAuth
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
EOF
openssl x509 -req -in fragment.csr -CA fragment-ca.crt -CAkey fragment-ca.key \
  -CAcreateserial -days 365 -sha256 -extfile fragment.ext -out fragment.crt
```

Distribute `fragment-ca.crt` to every query node as `--fragment-tls-ca`, and
`fragment.crt` with `fragment.key` as `--fragment-tls-cert` and
`--fragment-tls-key`. Reissuing the worker certificate, or rotating the CA,
takes effect on the next rolling restart.

Check an existing certificate before the restart that turns the listener on:

```sh
openssl x509 -in fragment.crt -noout -ext extendedKeyUsage
```

It must list both `TLS Web Server Authentication` and `TLS Web Client
Authentication`. A certificate issued against a release that documented
`serverAuth` alone lists only the first, and startup refuses it.

### Rolling onto the dedicated listener

The dedicated listener is opt-in per process. A query node without
`--fragment-listener` keeps serving the fragment surface on the public gRPC
listener, so a fleet migrates one rolling restart at a time: nodes that have the
flag advertise their TLS fragment endpoint and refuse pinned fetches on the
public port, while nodes that do not keep serving them there. Results stay
identical throughout. Only which nodes a slice can fan out to changes during the
roll.

## Federating to a remote cluster

`--remote-cluster` points this coordinator at another Ravel cluster's fragment
fetch surface. One flag per remote, as a comma-separated `key=value` spec:

```
ravel-server --mode query \
  --remote-cluster name=eu,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu.token \
  --remote-cluster name=apac,endpoint=apac.internal:9443,credential-file=/etc/ravel/apac.token,tls-ca-file=/etc/ravel/apac-ca.pem,soft-timeout=15s
```

`name`, `endpoint` and `credential-file` are required. `tenant`, `tls` (default
`true`), `tls-ca-file`, `skip-unavailable` (default `false`) and `soft-timeout`
are optional. `--remote-cluster-soft-timeout` sets the default soft timeout for
every remote that does not name its own; a remote that does not answer within
its bound is treated as unavailable, which fails the query unless that remote
has `skip-unavailable`.

The credential is an operator secret read from a file, never an inline value. It
is the principal the remote sees. A federated query never forwards the calling
client's credential across a cluster boundary.

**One remote credential per local tenant.** That credential authorizes one
tenant's data on the remote, so it belongs to one local tenant. `tenant` names
it, and a query from any other local tenant never dials that remote. A
coordinator serving several local tenants writes one spec per local tenant, each
with its own `name` and its own `credential-file`:

```
ravel-server --mode query \
  --tenant-token acme-token:acme \
  --tenant-token beta-token:beta \
  --remote-cluster name=eu-acme,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu-acme.token,tenant=acme \
  --remote-cluster name=eu-beta,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu-beta.token,tenant=beta
```

`--tenant-token` on the command line puts every bearer token into argv, which
a pod spec or process listing exposes. `--tenant-token-file PATH` (env
`RAVEL_TENANT_TOKEN_FILE` for the path only) reads the same `TOKEN=TENANT`
pairs from a file instead, one per line, blank lines and `#` comments
skipped; mount it from a Secret rather than templating tokens into args.
`--tenant-token` and `--tenant-token-file` are mutually exclusive; startup
refuses if both are set. The file is read once, at startup: rotating the
mounted Secret still needs a pod restart to pick up the new tokens. An empty
or comment-only file (a Secret mount that failed to populate looks exactly
like this) parses to an empty map with no startup error: it authenticates
nothing, and unless `--maintain-tenant` names tenants, background fold,
compaction and retention widen to every tenant storage discovers.

A local tenant no remote names gets local data only, reported as a complete
result: a remote it holds no credential for is outside its query, not missing
from it, so no warning and no `partial: true` appear.

Omitting `tenant` leaves the remote reachable by every local tenant, which is
correct only where the coordinator runs queries for one. A coordinator that runs
queries for more than one therefore **refuses to start** with such a spec, rather
than fanning every local tenant's selectors and discovery out under the one
credential and returning another tenant's series. A coordinator runs queries for
more than one local tenant when:

- two or more `--tenant-token` values or `--tenant-token-file` lines name
  different tenants;
- an `--alert-rules-file` names a tenant no `--tenant-token` or
  `--tenant-token-file` does. The alert
  evaluator runs one query loop per tenant in that file, against the same engine
  federation is installed on, so those queries federate even though no request
  produced them;
- any dynamic resolver is enabled (`--dev-insecure-tenant-header`,
  `--oidc-issuer`, or `--mtls-enabled`, each of which derives the tenant from a
  request header or a token claim).

The startup error names every spec needing a `tenant` and what makes the
deployment multi-tenant:

```
--remote-cluster 'eu' names no local tenant on a coordinator that runs queries
for more than one local tenant (2 distinct static bearer tenants are
configured). A remote cluster holds one remote credential and cannot express one
credential per local tenant ... Add tenant=<local tenant> to each of those specs
...
```

Startup also refuses a `tenant` named by no `--tenant-token`,
`--tenant-token-file` line, or `--alert-rules-file` rule, where the tenant set
is fully known (static bearer tokens and alert rules, no dynamic resolver): the
mapping could never fire, and the only symptom would be a remote that quietly
answers nobody. A tenant that only alert rules name is a valid target, and
mapping a remote to it is the supported way to give alert rules over data that
lives partly on a remote.

**TLS is on unless the spec says otherwise.** Neither spec above names `tls`,
and both dial over TLS, verifying the remote against the system trust roots plus
`tls-ca-file` when one is set. A spec carrying `tls-ca-file` and no `tls` key
means "TLS on, with this CA trusted"; there is no need to pair the two.

`tls=false` is the escape hatch for a hop already encrypted and access
controlled at a lower layer, such as a service mesh sidecar or an encrypted
tunnel. It is an explicit, logged choice, because with TLS off the operator
credential, the federated query and every returned result stream cross the
network in cleartext, where anyone on the path can read and replay the
credential:

```
WARN SECURITY: --remote-cluster 'eu' is configured with tls=off. The operator
bearer credential presented to this remote, every federated query, and every
returned result stream travel in cleartext to 'eu.internal:9443'. ...
```

One line is logged per plaintext remote, and a TLS remote logs nothing. If you
see this warning and did not intend plaintext, drop the `tls=false` key. Setting
`tls=false` together with `tls-ca-file` fails startup outright, because the CA
bundle would be inert.

## Background

Decision records behind this page:
[fail-closed isolation and startup invariants](../../adrs/0050-fail-closed-isolation-and-startup-invariants.md),
[tenant-scoped credentials and control-plane protection](../../adrs/0072-tenant-scoped-credentials-and-control-plane-protection.md),
[credential scoping](../../adrs/0055-storage-credential-scoping.md),
[distributed read fan-out](../../adrs/0071-distributed-read-fanout.md),
and [format migration machinery](../../adrs/0066-format-migration-machinery.md).
