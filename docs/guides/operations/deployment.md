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
while it runs its suite, not only the final record. The Admin policy's one
delete grant, `AdminQualifyDelete`, covers `sys/qualify/*` only, so the
checksum echo probe below can remove its own object; the suite itself leaves
its scratch in place. It is bounded per run, but not small: the two listing probes write two keys
more than the declared page size each, so a run at the default page size
leaves about two thousand small objects, and repeated runs against the same
bucket accumulate them. Sweep the prefix from a runbook when it matters.

Against `--store s3`, `store qualify` also prints the bucket's Object Lock,
versioning and lifecycle state read from the bucket itself, as informational
lines that never fail the run, and one stored-checksum line. That check PUTs a
probe object with the `--s3-upload-integrity` checksum, reads it back whole
with the stored checksum requested, and says what the endpoint did:

- `verified`: the endpoint returned a stored checksum (CRC-64/NVME or
  CRC-32C), and the same algorithm computed over the body read matched it.
  The returned value is not compared with the one the PUT sent, so a CRC-32C
  the endpoint computed itself also reads as `verified`.
- `not returned`: the endpoint returned none, so every whole-object read in
  production is served and counted unverified.
- `not checked (...)`: the check did not run, with the reason: upload
  integrity `off`, `sha256` (whose stored checksum is not recomputed on read),
  `--s3-request-stored-checksum=false`, or a store other than S3.
- `FAIL`: the endpoint returned a stored checksum that does not match the
  same algorithm computed over the body read, or the probe PUT or GET failed.
  Qualification fails and nothing is recorded.

The probe object sits under `sys/qualify/<run-id>/` and is deleted afterwards.
On a versioned bucket that delete only adds a delete marker: the probe's
object version stays under `sys/qualify/` as a noncurrent version. If the
credential cannot delete it, a `note:` line
names the object left in place and the outcome stands.

`store qualify --list-page-size` builds the S3 store with the page size and
the selected checksum settings together, so a small page size (the weekly
disaster recovery rehearsal runs `--list-page-size 2`) still checksums every
PUT.

## The bucket protection contract

Some of what protects a Ravel bucket is configured at the bucket and policy
layer, not by any Ravel process. Nothing in `ravel-server` configures it,
because the object-store client exposes no such API; on S3,
`--require-bucket-protection` reads it back at startup with read-only
requests, as [described below](#bucket-protection-at-startup). The normative
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
deletes a superseded commit record. Object Lock does not refuse that delete:
Ravel's delete names no version, so on the versioned bucket Object Lock
requires it succeeds and inserts a delete marker, and the sweep carries on.
The locked version stays in storage until its retain-until has passed and
the noncurrent-version expiration rule removes it, so the retention period
chosen for commit records bounds how long their bytes physically remain, not
how long the sweep waits; the contract page's "Required bucket configuration"
gives the bound.

One of the other three families does carry a further cost, from a fourth
mechanism: a compliance lock on `t/*/catalog/*/*` there
costs an erasure obligation, not only a reclamation delay. The unreferenced-catalog sweep
deletes the snapshot and index objects the current HEAD no longer names, and
for a tenant that declares a typed string or bytes attribute column a per-part
column-statistics object among them holds that subject's own column value. A
lock over the keyspace does not delay that delete, which succeeds as a delete
marker, but the locked version keeps the value in storage: the sweep deletes
the object only once the fold has reconciled that hour and the object is older
than `protection_horizon`, and the value then persists until its retain-until
has passed and the noncurrent-version expiration has fired. The maintenance
IAM policy Ravel ships permits that delete, with its catalog deny scoped to
`catalog/<signal>/HEAD`; a copy of that template predating the narrowing
denies it outright and leaves the bound open-ended until it is re-applied.
The four-step mechanism, the exact bound, the IAM ceiling and the
HEAD-scoping advice are in the contract page's "Required bucket
configuration" section, "A lock on the catalog family".

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

### Bucket protection at startup

`--require-bucket-protection` turns the bucket's protection configuration
into a startup gate, so a deployment cannot go into production silently
unprotected. On `--store s3` the check reads the bucket's configuration once,
with three read-only GETs (`?versioning`, `?lifecycle`, `?object-lock`) signed
with the store's own credentials, so that identity needs
`s3:GetBucketVersioning`, `s3:GetLifecycleConfiguration` and
`s3:GetBucketObjectLockConfiguration`; a denied call reads as unknown, not
as a failure. None of the IAM templates under `deploy/iam/` grants those three
actions to a server role, so on AWS a
server running under a shipped template reads every condition as unknown and
starts with a warning: attach the three actions to its role yourself for the
check to see the bucket. Under `--tenant-kms-config` it reads the same bucket: every
tenant's objects live in the one bucket, and only the encryption key differs
per tenant. Each condition comes back passed, failed or unknown:

- These failures refuse to start, naming each failed condition:
  `object-lock` (Object Lock disabled on the bucket), `abort-multipart` (no
  enabled `AbortIncompleteMultipartUpload` rule of seven days or less
  covering the data), `no-foreign-rule` (another expiration or transition
  rule targets `t/` or `sys/`), and `noncurrent-expiration` on a bucket
  whose versioning the check read as on. That last one fails when no enabled rule expires noncurrent
  versions over all of `t/`, when a covering rule also keeps
  `NewerNoncurrentVersions`, when covering rules disagree on
  `NoncurrentDays`, or when a rule over part of `t/` expires noncurrent
  versions sooner than the covering rules do. The server has no expected
  `E_v`, so it does not compare a covering rule's `NoncurrentDays` with one;
  `ravel-cli store verify-protection --expected-noncurrent-days` checks the
  value.
- Any other failed condition (`versioning`, `expired-delete-marker`,
  `rule-scope`, or `noncurrent-expiration` on a bucket whose versioning is
  off or could not be read) logs one warning and starts.
- An unknown condition logs one warning and starts. On every backend other
  than S3 the check cannot read the configuration, so every condition is
  unknown.
- The bucket-configuration read is bounded to 10 seconds. The Kubernetes
  operator's liveness probe restarts a pod on its third consecutive failure,
  between about 25 and 35 seconds after the pod starts. The read-cache
  warm-up also runs before the main HTTP listener binds, bounded by its own
  10 seconds, so the two bounds together leave at least 5 seconds for the
  rest of startup. A read that has not finished by
  then leaves every condition unknown, which warns and starts.
- The bound covers the bucket-configuration read only. The
  `sys/qualification` read runs before it, on the store's ordinary retrying
  path, and is bounded only by the store's own request timeout and retries. An
  endpoint that stalls every request holds startup at that read, where the
  liveness probe can restart the pod; the bound helps when only the three
  configuration GETs stall.
- `delete-marker-replication` and `object-retention` are not checked at
  startup. `ravel-cli store verify-protection` checks the first; no Ravel
  command checks object retention yet, so verify it by hand as the disaster
  recovery guide describes.

The check sets `ravel_bucket_protection_conditions_failed`,
`ravel_bucket_protection_conditions_unknown` and
`ravel_bucket_protection_unknown` at `/metrics`; see
[Observability](../observability.md#bucket-protection-ravel_bucket_protection_)
for what a zero means. The check runs once per start: a bucket changed under
a running process is seen at the next restart.

The flag is off by default, so a development process that does not pass it
starts without the gate and sends none of those GETs. The Kubernetes operator sets it unconditionally for
every cluster it reconciles: the custom resource carries no development or
staging profile field to gate on. Every bucket a `RavelCluster` points at must
therefore be created with Object Lock enabled, versioning on, and the
sanctioned lifecycle rules, or its pods refuse to start. The dev bucket Jobs in
`deploy/k8s/floci.yaml` and `deploy/k8s/rustfs.yaml`, and the compose
`createbucket` one-shots, create such a bucket: one enabled rule over the whole
bucket with `ExpiredObjectDeleteMarker`, `NoncurrentDays` 1 and
`AbortIncompleteMultipartUpload` after 7 days.

`ravel-cli store verify-protection --expected-noncurrent-days <E_v>` checks
the whole bucket half of the contract against an S3 bucket's own
configuration and exits `0` only when every expected condition passes, `1`
when any fails, and `2` when any could not be verified. Schedule it;
[running the checklist with ravel-cli](../disaster-recovery.md#running-the-checklist-with-ravel-cli)
describes its flags, output, and the read-only permissions it needs.

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
  `ravel-cli gc-config set` under Admin. A gateway or query process that
  reaches an empty bucket under its scoped credential is refused and exits
  with an error saying that `sys/gc` could not be created with its
  credential and naming that fix; it keeps exiting until the object exists,
  so a restart policy brings it up after maintain has created it. A maintain
  process refused that create instead gets an error naming the PutObject grant
  on `sys/gc` (and `kms:GenerateDataKey` under SSE-KMS) its role needs, checked
  against `deploy/iam/maintain.json`, not start-order advice. Under one
  shared credential any server process creates `sys/gc`, so start order does
  not matter there. The mutation path that changes an existing `sys/gc` is
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
- Even Admin cannot delete any of the protected prefixes, and its one delete
  grant, `AdminQualifyDelete`, covers only the `sys/qualify/*` scratch that
  `store qualify` writes. A leaked Admin key can forge or overwrite control
  objects within its write grant, but it cannot make existing data disappear.

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

The probe exports three samples at `/metrics`, so the outage is visible even
where nothing consumes `/readyz`:

- `ravel_store_reachable`, a gauge labeled by mode: 1 healthy, 0 unhealthy.
- `ravel_store_probe_failures_total`, a counter labeled by mode, incremented on
  every failed probe cycle even below the readiness threshold.
- `ravel_store_probe_last_run_timestamp_seconds`, a gauge labeled by mode: Unix
  time of the probe's last completed cycle or of its spawn, whichever is later.
  Its AGE, not its value, is the signal: the other two move only while the probe
  task is alive, so a task that dies freezes them at a healthy-looking value
  while this one stops advancing. Alert on the age, not on the value; see
  [the store-probe liveness alert](../observability.md#the-store-probe-liveness-alert).

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
  `10s + 32.5s + 2.5s`, rounded up. With `spec.probes.dedicatedHealthPort`
  set, stopping the health listener adds up to 6s to the server's budget
  (38.5s), and the operator sets **51s** instead: `10s + 38.5s + 2.5s`. The
  error is deliberately on the long side.
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
compiled default and `45s` (`51s` with the dedicated health port) is the correct
grace period today. `--shutdown-timeout`
is configurable on the server itself; if a future CRD field exposes it, the grace
period must track it, staying above the new server budget plus the `preStop`
sleep.

## Durable auth refresh

On a keyed bucket, a request-serving process (`all`, `gateway`, `query`)
resolves bearer tokens against a cached copy of the durable `sys/auth` map as
well as the static and OIDC resolvers, and keeps that copy current with a
background refresh loop. The durable resolver is appended after the static and
OIDC chain, so it only ever answers a request the others could not. The
shipped Gateway and Query policy documents grant the read on `sys/auth`; of the
four shipped roles only Admin may write it, through `ravel-cli tenant token
upsert` and `revoke`.

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

The Flight SQL lane signs its tickets with a key file of its own,
`--sql-ticket-key-file`: the same shape and rotation rule as
`--fragment-key-file`, read by every node in the cluster, and kept with the
same care as the fragment key file. Give it different keys from the fragment key file. It
requires `--distributed-query`; setting it alone fails startup. It is optional
in this release. A node without it derives its SQL ticket secret from the
first fragment key, as earlier releases did, but this release turns that
secret into separate client and slice keys, so upgrading from an earlier
release has the same client-ticket window described below until every node
runs this release. A node in `all` or `query` mode without it logs a startup
warning naming what release B, the release after the operator renders the
dedicated listener, requires: with `--distributed-query`, both
`--fragment-listener` and `--sql-ticket-key-file`. A node missing only
`--fragment-listener` logs the same warning.

Two nodes that do not share a SQL ticket key disagree on every ticket, and a
rolling deploy is exactly the window in which they coexist. Rolling a brand
new key file straight onto a fleet on the derived key causes two failures
until the roll finishes:

- A client query fails. `GetFlightInfo` returns one endpoint with no location,
  so a client behind a balancer may send `DoGet` to any node. A ticket minted
  on a node on the file and redeemed on a node still on the derived key, or
  the reverse, fails with `invalid_argument` ("malformed flight ticket").
- SQL slices between two such nodes fail the worker's MAC and run on the
  coordinator instead, so the query loses parallelism.

Once every node runs this release, switch without that window: make the
first key file equal to the key the nodes already use, then rotate. Doing
this in the same roll as the upgrade does not avoid the window, because the
upgrade itself changes the keys an older node uses.

1. Compute the key each node derives today. `ravel-server` does not print it
   and has no flag that does. It is the BLAKE3 derive-key of the first key in
   the fragment key file, as its 64 lowercase hex characters, under the
   fixed context string the command below passes to `--derive-key`.
   [`b3sum`](https://github.com/BLAKE3-team/BLAKE3) (`cargo install b3sum`)
   computes it:

   ```sh
   first=$(grep -v '^[[:space:]]*#' fragment.keys | grep -m1 '[^[:space:]]' \
     | tr -d '[:space:]' | tr 'A-F' 'a-f')
   printf '%s' "$first" \
     | b3sum --derive-key 'ravel-sql flight ticket MAC key 2026-08 (RFT1 v4, ADR-0071)' \
       --no-names > sql-ticket.keys
   ```

   Run it where the fragment key file already lives. The output line is a
   secret with the same custody as the fragment key file. `ravel-server`'s
   `sql_distrib` unit tests pin the context string, the lowercase-hex input
   and the resulting key; nothing checks this page's copy of the command.
2. Roll `--sql-ticket-key-file` pointing at that one-line file onto every
   node. A node on the file and a node on the derived key now hold the same
   key, so nothing fails while the roll is in progress.
3. Rotate off the derived key, which anyone holding the fragment key file can
   recompute. Add a freshly generated key (`openssl rand -hex 32`) as the
   *second* line and roll, so every node verifies it before any node mints
   with it. Then move it to the first line and roll again: every node now
   mints under it and still verifies the old key. Once the longest Flight SQL
   ticket TTL has passed since that roll finished, delete the old line and
   roll a last time.

Adding the new key as the first line in a single roll, the way the fragment
key file is rotated, reopens the mixed window for client tickets: a node
already on `[new, old]` mints under the new key, and a node still on `[old]`
cannot verify that ticket.

Where SQL slice tickets travel depends on `--fragment-listener`. Without it,
the SQL lane dials each worker's `--listen-grpc` address and slice tickets
travel there in plaintext. A slice ticket read off that network is a
replayable read capability for its tenant and segment set until its deadline.
Every `--distributed-query` process in `--mode all` or `--mode query` that
serves Flight SQL logs this once at startup, and keeping the public gRPC port
on a network you trust is the only mitigation. With `--fragment-listener`, SQL
slices ride the dedicated TLS listener described below and that line is not
logged.

With the key file in place, the cluster-internal fragment surface, where one
query worker fetches a slice for another, can be moved off the public gRPC
listener onto a dedicated listener that terminates TLS in-process:
`--fragment-listener <addr>`, with `--fragment-tls-cert`, `--fragment-tls-key`
and `--fragment-tls-ca`. Both distributed lanes move onto it:

- The PromQL lane's pinned fragment fetches. The public gRPC listener then
  serves only cross-cluster federation with ordinary tenant credentials and
  refuses pinned fetches; the dedicated listener serves pinned fetches and
  refuses federation.
- The SQL lane's slice `DoGet`. The dedicated listener serves `DoGet` for a
  slice ticket and refuses every other Flight and Flight SQL method; no method
  other than a slice `DoGet` returns data there. Client Flight SQL methods
  answer `permission_denied`, methods the service does not implement answer
  `unimplemented`, and a `DoGet` that is not a valid slice capability answers
  `unauthenticated` (or `permission_denied` for a client ticket). The public
  gRPC listener keeps the client Flight SQL surface and refuses, with
  `permission_denied` ("slice fetch rejected: wrong_surface"), a slice ticket
  whose MAC verifies under this node's slice keys. A forged slice ticket, or
  one under a key this node lacks, takes the client path and is refused there,
  uncounted. A coordinator dials each worker's advertised `fragment_endpoint`
  over TLS. A worker with the flag advertises its dedicated listener there; a
  worker without it advertises its public gRPC address, so during a rolling
  deploy a coordinator with the flag dials that address over `https` and the
  TLS handshake fails before any request is sent.

Startup refuses a `--fragment-listener` address equal to `--listen-http`,
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

The same certificate, key and CA serve SQL slice `DoGet`. There is no second
certificate for the SQL lane: a coordinator dials SQL slices with the same
pinned CA, the same `ravel-fragment` server name and the same client
certificate it dials fragment fetches with.

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
  --sql-ticket-key-file /etc/ravel/sql-ticket-keys \
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

`--listen-grpc` is not optional in this example. The public gRPC listener
carries client Flight SQL and federation, and it defaults to `127.0.0.1:4317`,
which nothing outside the pod's own loopback reaches. The advertised host
applies to both published endpoints; with `--fragment-listener` set, SQL
slices dial the fragment endpoint, so the published public gRPC address is
only dialed by a peer that still runs without `--fragment-listener`, and this
node refuses the slice tickets such a peer sends there.

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
flag advertise their TLS fragment endpoint and refuse pinned fetches and SQL
slice tickets on the public port, while nodes that do not keep serving them
there. A slice between a node with the flag and a node without it fails its
first dial (a TLS dial to a plaintext port, or a slice ticket the public
listener refuses), is re-dispatched once to another worker, and runs
coordinator-local only if that attempt fails too. Results stay identical
throughout. Only which nodes a slice can fan out to changes during the roll.

The release that moves SQL slices onto the dedicated listener also moves the
`queryfrag` protocol version from 4 to 5. Coordinators
drop workers advertising another version at routing time, before any dial, so
during the one rolling deploy onto this release a node on version 5 and a node
on version 4 send each other no slices on either lane: those slices run
coordinator-local. The fleet loses parallelism for that deploy, and results do
not change. Once every node runs the new release, fan-out resumes.

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

Federation requests carry the `queryfrag` protocol version, and a remote
refuses a request on another version. The release that moved it from 4 to 5
therefore splits federation: a cluster on that release and a remote on an
earlier one fail every federated query with a `Federation` error naming the
remote, or, for a remote with `skip-unavailable`, skip it with a
partial-coverage warning, until both run the same release.

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
