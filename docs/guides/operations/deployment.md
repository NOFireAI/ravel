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
It is bounded, one run's worth of small objects per invocation, and harmless to
leave, but repeated runs against the same bucket accumulate it.

## The bucket protection contract

Some of what protects a Ravel bucket is configured at the bucket and policy
layer, not by any Ravel process. Nothing in `ravel-server` configures or
verifies it, because the object-store client exposes no such API. The normative
statement is
[the object store contract](../../object-store-contract.md)'s required bucket
configuration section; this is the operational summary.

Object Lock in compliance mode on the control prefixes (`sys/*`, the
provisioning records, commit records and the catalog HEAD history), plus the
versioning and lifecycle-rule requirements that go with erasure obligations,
are all bucket-layer settings.

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
  object-store contract reports today because no adapter can answer the query,
  logs one warning and sets `ravel_bucket_protection_unknown` to `1` at
  `/metrics` rather than blocking startup.
- **Enabled** with no alarms starts clean, with the gauge at `0`.

The flag is off by default, so a development process that does not pass it
starts exactly as it did before the gate existed. The Kubernetes operator sets
it unconditionally for every cluster it reconciles: the custom resource carries
no development or staging profile field to gate on.

## The first deployment against a fresh bucket

Two control objects are written by whichever process boots first against an
empty bucket, which is why their write grants are slightly broader than the
per-role tables imply. Know this before your first deployment:

- **`sys/tenancy`**, the marker that pins the tenant hash scheme, is created by
  whichever of the three server roles reaches a fresh bucket first. That is why
  Gateway, Query and Maintain all carry a write grant on it, not just Admin. It
  does not weaken the delete boundary: a create-if-absent write cannot overwrite
  or delete an existing object, and `sys/tenancy` is deny-delete for every role.
  The effect is only that a fresh operator-managed cluster boots without a
  manual bootstrap step.
- **`sys/gc`**, the durable garbage-collection configuration, is bootstrapped by
  Maintain on a fresh bucket, hence Maintain's write grant on it. The mutation
  path that changes an existing `sys/gc` is Admin-only, matching that it is an
  explicit operator action rather than something a server does on its own.

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

`/readyz` reflects store reachability, not just startup completion. Each process
runs one background probe that reads the fixed `sys/tenancy` object every
`--store-probe-interval` (default `30s`, jittered so replicas do not probe in
lockstep). Readiness is the startup latch and this probe's health together:

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

**Ravel mints no certificates or keys.** The operator provisions the PEM files
out of band. The certificate and key are read once at startup, so certificate
rotation is a rolling restart. Requirements for the worker certificate:

- A `ravel-fragment` dNSName SAN. The SAN is verified, not the CN.
- `extendedKeyUsage = serverAuth`.
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
  --fragment-key-file /etc/ravel/fragment-keys \
  --fragment-listener 0.0.0.0:4319 \
  --fragment-tls-cert /etc/ravel/fragment-tls/tls.crt \
  --fragment-tls-key  /etc/ravel/fragment-tls/tls.key \
  --fragment-tls-ca   /etc/ravel/fragment-tls/ca.crt
```

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

# One worker certificate, SAN = ravel-fragment, EKU serverAuth.
openssl ecparam -genkey -name prime256v1 -out fragment.key
openssl req -new -key fragment.key -subj "/CN=ravel-fragment" -out fragment.csr
cat > fragment.ext <<'EOF'
subjectAltName = DNS:ravel-fragment
extendedKeyUsage = serverAuth
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

`name`, `endpoint` and `credential-file` are required. `tls` (default `true`),
`tls-ca-file`, `skip-unavailable` (default `false`) and `soft-timeout` are
optional. `--remote-cluster-soft-timeout` sets the default soft timeout for
every remote that does not name its own; a remote that does not answer within
its bound is treated as unavailable, which fails the query unless that remote
has `skip-unavailable`.

The credential is an operator secret read from a file, never an inline value. It
is the principal the remote sees. A federated query never forwards the calling
client's credential across a cluster boundary.

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
