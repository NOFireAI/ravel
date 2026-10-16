# Deployment (day 1)

Bring a cluster up against a bucket for the first time. Make the decisions on
the [configuration page](configuration.md) before you start.

1. [Qualify the store](#qualify-the-store). A server cannot start on a fresh
   production bucket until qualification passes once.
2. [Configure bucket protection](#the-bucket-protection-contract) at the bucket
   layer. Decide whether startup gates on it.
3. [Start the first process](#the-first-deployment-against-a-fresh-bucket). It
   creates the two control objects that it needs.
4. [Check readiness](#readiness-and-the-store-reachability-probe). Readiness
   reports live store reachability as well as startup.
5. If you run distributed reads, provision
   [the TLS material of the fragment listener](#the-dedicated-fragment-listener).
   If you federate, also provision
   [the remote cluster credentials](#federating-to-a-remote-cluster).

## Qualify the store

Run `store qualify` once per bucket, before the first server starts:

```sh
ravel-cli store qualify --store s3 --s3-endpoint ... --s3-bucket ...
```

Ravel's commit protocol and catalog need two things from the store. The store
must honor conditional writes, so that it rejects a losing writer and does not
overwrite silently. A listing must show every write that is complete.
`store qualify` tests both against the real backend. A backend that advertises
them and does not deliver them breaks durability without an error.

On a pass, the command records a durable `sys/qualification` object. The
object names the backend, the suite version and the time. A second run leaves
the existing record as it is.

On every store other than `memory`, `ravel-server` reads `sys/qualification`
at startup, in every mode, before any listener binds. It refuses to start in
two cases, and each case has its own named error:

- **absent**: the backend was never qualified. Run `ravel-cli store qualify`,
  then start the server.
- **stale**: the record has a suite version below the floor that this binary
  requires. Run `ravel-cli store qualify` again with a current build, then
  restart.

No server process creates the qualification record, and no option skips the
check. `--store memory` is exempt and never needs qualification.

While the suite runs, `store qualify` also writes scratch objects under
`sys/qualify/<run-id>/`. The suite leaves them in place. Each of the two
listing probes writes two keys more than the declared page size. A run at the
default page size therefore leaves about two thousand small objects, and
repeated runs against the same bucket add more. Sweep the prefix from a
runbook when the count matters. The Admin policy's delete grant for it,
`AdminQualifyDelete`, covers `sys/qualify/*` only. The stored-checksum probe
uses that grant to delete its own object.

Against `--store s3`, `store qualify` prints two more things:

- The Object Lock, versioning and lifecycle state that it reads from the
  bucket. These lines are informational and never fail the run.
- One stored-checksum line. The check PUTs a probe object with the
  `--s3-upload-integrity` checksum. Then it reads the whole object back and
  requests the stored checksum.

The stored-checksum line has one of four values:

- `verified`: the endpoint returned a stored checksum (CRC-64/NVME or
  CRC-32C), and the same algorithm computed over the body read matched it.
  The check does not compare the returned value with the value that the PUT
  sent. A CRC-32C that the endpoint computed itself therefore also reads as
  `verified`.
- `not returned`: the endpoint returned no stored checksum. In production,
  every whole-object read is then served and counted unverified.
- `not checked (...)`: the check did not run. The line gives the reason:
  upload integrity `off`, upload integrity `sha256` (whose stored checksum is
  not recomputed on read), `--s3-request-stored-checksum=false`, or a store
  other than S3.
- `FAIL`: the endpoint returned a stored checksum that does not match the
  same algorithm computed over the body read, or the probe PUT or GET failed.
  Qualification fails and nothing is recorded.

The probe object is under `sys/qualify/<run-id>/`, and the check deletes it
afterwards. On a versioned bucket, that delete only adds a delete marker. The
probe's object version stays under `sys/qualify/` as a noncurrent version. If
the credential cannot delete the object, a `note:` line names the object left
in place and the outcome stands.

`store qualify --list-page-size` applies the page size and the selected
checksum settings to the S3 store together. A run with a small page size
therefore still checksums every PUT. The weekly disaster recovery rehearsal
runs `--list-page-size 2`.

## The bucket protection contract

You configure some of a Ravel bucket's protection at the bucket and policy
layer. No Ravel process configures it, because the object-store client has no
API for it. On S3, `--require-bucket-protection` reads it back at startup
with read-only requests (see
[bucket protection at startup](#bucket-protection-at-startup)).
[The object store contract](../../object-store-contract.md) is the normative
statement, in its "Required bucket configuration" section.

Set these at the bucket layer:

- Object Lock, enabled on the bucket.
- Versioning.
- The lifecycle rules that go with erasure obligations.

Compliance-mode retention on the control prefixes is per-object retention. An
operator-run mechanism applies it, because Object Lock has no prefix scope.
The contract page describes the two shapes that this mechanism can take. The
control prefixes are four families: `sys/*`, the provisioning records, the
commit records, and the catalog keyspace `t/*/catalog/*/*`.

Three mechanisms physically remove tenant data: supersession GC, retention
deletion and subject erasure. They never touch three of the four families, so
a lock on those three costs nothing against these mechanisms. Two families
carry a cost.

**Commit records.** A maintenance sweep deletes a superseded commit record.
Object Lock does not refuse that delete. Ravel's delete names no version, so
on the versioned bucket that Object Lock requires, the delete succeeds and
inserts a delete marker. The sweep continues. The locked version stays in
storage until its retain-until passes and the noncurrent-version expiration
rule removes it. The retention period that you choose for commit records
therefore bounds how long their bytes physically remain. It does not bound
how long the sweep waits. The contract page's "Required bucket configuration"
section gives the bound.

**The catalog keyspace.** A fourth mechanism, the unreferenced-catalog sweep,
gives a compliance lock on `t/*/catalog/*/*` a further cost. The lock
costs an erasure obligation as well as a reclamation delay.

- The unreferenced-catalog sweep deletes the snapshot and index objects that
  the current HEAD no longer names.
- For a tenant that declares a typed string or bytes attribute column, one of
  those objects is a per-part column-statistics object. It holds the subject's
  own column value.
- The lock does not delay that delete, which succeeds as a delete marker. The
  locked version keeps the value in storage.
- The sweep deletes the object only after the fold reconciles that hour and
  the object is older than `protection_horizon`.
- The value then stays until its retain-until passes and the
  noncurrent-version expiration fires.

The maintenance IAM policy that Ravel ships permits that delete. Its catalog
deny is scoped to `catalog/<signal>/HEAD`. A copy of the template from before
that scope was narrowed denies the delete. The bound is then open-ended until
you apply the current template. The contract page's "Required bucket
configuration" section, under "A lock on the catalog family", has the
four-step mechanism, the exact bound, the IAM ceiling and the HEAD-scoping
advice.

**Every bucket that Ravel writes to must have one lifecycle rule.** Configure
`AbortIncompleteMultipartUpload` with a cleanup period of seven days or less.
Nothing in `ravel-server` reaps orphaned multipart parts. Cleanup stays
unconfirmed in three cases:

- A best-effort abort fails.
- An upload future is dropped mid-flight.
- S3 applies an abort and still returns an error.

The parts can then stay billed until this rule removes them. Two store
counters show the failure:

- `multipart_abort_failures` counts best-effort aborts whose request returned
  an error.
- `multipart_uploads_unreaped` counts uploads that ended without a confirmed
  successful abort, for any reason.

A sustained rise in either counter means that only the lifecycle rule bounds
the cost of orphaned parts on that bucket.

### Bucket protection at startup

Pass `--require-bucket-protection` to make the bucket's protection
configuration a startup gate. A deployment then cannot go into production
unprotected without a signal.

The flag is off by default. A development process that does not pass it starts
without the gate and sends none of the GETs below. The Kubernetes operator
sets the flag for every cluster that it reconciles, because the custom
resource has no development or staging profile field. Create every bucket
that a `RavelCluster` points at with Object Lock enabled, versioning on, and
the sanctioned lifecycle rules. If you do not, its pods refuse to start. The
dev bucket Jobs in `deploy/k8s/floci.yaml` and `deploy/k8s/rustfs.yaml`, and
the compose `createbucket` one-shots, create such a bucket. It has one enabled
rule over the whole bucket with `ExpiredObjectDeleteMarker`, `NoncurrentDays`
1 and `AbortIncompleteMultipartUpload` after 7 days.

On `--store s3`, the check reads the bucket's configuration once per start. A
bucket changed under a running process is seen at the next restart. The check
sends three read-only GETs (`?versioning`, `?lifecycle`, `?object-lock`),
signed with the store's own credentials. That identity needs
`s3:GetBucketVersioning`, `s3:GetLifecycleConfiguration` and
`s3:GetBucketObjectLockConfiguration`. A denied call reads as unknown, and
does not read as a failure.

None of the IAM templates under `deploy/iam/` grants those three actions to a
server role. On AWS, a server that runs under a shipped template therefore
reads every condition as unknown and starts with a warning. Attach the three
actions to its role so that the check can read the bucket.

Under `--tenant-kms-config` the check reads the same bucket. Every tenant's
objects are in the one bucket, and only the encryption key differs per
tenant.

Each condition reads as passed, failed or unknown:

- These failed conditions refuse to start, and the error names each one:
  - `object-lock`: Object Lock is disabled on the bucket.
  - `abort-multipart`: no enabled `AbortIncompleteMultipartUpload` rule of
    seven days or less covers the data.
  - `no-foreign-rule`: another expiration or transition rule targets `t/` or
    `sys/`.
  - `noncurrent-expiration`, on a bucket whose versioning the check read as
    on.
- Any other failed condition logs one warning and starts: `versioning`,
  `expired-delete-marker`, `rule-scope`, or `noncurrent-expiration` on a
  bucket whose versioning is off or that the check cannot read.
- An unknown condition logs one warning and starts. On every backend other
  than S3 the check cannot read the configuration, so every condition is
  unknown.
- `delete-marker-replication` and `object-retention` are not checked at
  startup. `ravel-cli store verify-protection` checks the first. No Ravel
  command checks object retention yet, so verify it by hand as the disaster
  recovery guide describes.

`noncurrent-expiration` fails in four cases:

- No enabled rule expires noncurrent versions over all of `t/`.
- A covering rule also keeps `NewerNoncurrentVersions`.
- Covering rules disagree on `NoncurrentDays`.
- A rule over part of `t/` expires noncurrent versions sooner than the
  covering rules do.

The server has no expected `E_v`, so it does not compare a covering rule's
`NoncurrentDays` with one.
`ravel-cli store verify-protection --expected-noncurrent-days` checks the
value.

The bucket-configuration read is bounded to 10 seconds:

- The Kubernetes operator's liveness probe restarts a pod on its third
  consecutive failure, between about 25 and 35 seconds after the pod starts.
- The read-cache warm-up also runs before the main HTTP listener binds, and
  has its own bound of 10 seconds. The two bounds together leave at least 5
  seconds for the rest of startup.
- A read that is not complete at the bound leaves every condition unknown,
  which warns and starts.
- The bound covers the bucket-configuration read only. The
  `sys/qualification` read runs before it, on the store's ordinary retrying
  path. Only the store's own request timeout and retries bound that read.
- An endpoint that stalls every request holds startup at the
  `sys/qualification` read, where the liveness probe can restart the pod. The
  bound helps when only the three configuration GETs stall.

The check sets `ravel_bucket_protection_conditions_failed`,
`ravel_bucket_protection_conditions_unknown` and
`ravel_bucket_protection_unknown` at `/metrics`. See
[Observability](../observability.md#bucket-protection-ravel_bucket_protection_)
for what a zero means.

`ravel-cli store verify-protection --expected-noncurrent-days <E_v>` checks
the whole bucket half of the contract against an S3 bucket's own
configuration. Schedule it. Its exit codes are:

- `0`: every expected condition passes.
- `1`: a condition fails.
- `2`: the command cannot verify a condition.

[Running the checklist with ravel-cli](../disaster-recovery.md#running-the-checklist-with-ravel-cli)
describes its flags, output, and the read-only permissions it needs.

## The first deployment against a fresh bucket

On a fresh bucket, do these steps in order:

1. Run `ravel-cli store qualify` under the Admin credential. It writes
   `sys/qualification`, which no server process creates. Until the object
   exists, every server process exits with the error that names
   `store qualify`.
2. Under per-role credentials, start the `maintain` process first. It creates
   `sys/gc`.
3. Start the other processes. The process that starts first creates
   `sys/tenancy`.

The first authorized contact with an empty bucket creates the two control
objects, `sys/tenancy` and `sys/gc`. Their write grants are therefore
slightly broader than the per-role tables imply.

**`sys/tenancy`** is the marker that pins the tenant hash scheme.

- Whichever of the three server roles reaches a fresh bucket first creates
  it. Gateway, Query and Maintain all carry a write grant on it for that
  reason, as well as Admin.
  A fresh operator-managed cluster therefore boots without a manual bootstrap
  step.
- The delete boundary stays the same. A create-if-absent write cannot
  overwrite or delete an existing object, and `sys/tenancy` is deny-delete for
  every role.
- On AWS S3 under per-role credentials this needs the current templates. See
  [AWS S3](#aws-s3).

**`sys/gc`** is the durable garbage-collection configuration. The first
process to reach a fresh bucket creates it, and only the Maintain and Admin
roles carry a write grant on it.

- Under one shared credential, any server process creates `sys/gc`, so start
  order does not matter.
- Under per-role credentials, you can create the object with `ravel-cli
  gc-config set` under Admin and not start `maintain` first.
- If you use `gc-config set`, give it the protection horizon and grace that
  the maintain process runs with (`--gc-protection-horizon` and `--gc-grace`,
  or their defaults). Maintain refuses to start against a `sys/gc` whose
  horizon or grace differs from its own.
- Also give it a max query duration and a max flush lifetime that match the
  `--gc-max-query-duration` and `--gc-max-flush-lifetime` that the processes
  run with.
- A gateway or query process that reaches an empty bucket under its scoped
  credential is refused and exits. The error says that its credential cannot
  create `sys/gc` and names that fix. The process keeps exiting
  until the object exists, so a restart policy brings it up after maintain
  creates the object.
- A maintain process that is refused the create gets a different error. It
  names the PutObject grant on `sys/gc` that its role needs, and gives no
  start-order advice. Under SSE-KMS it also names `kms:GenerateDataKey` on the
  bucket's default key, which the shipped templates grant only on the tenant
  key.
- Only Admin can change an existing `sys/gc`, because a change is an explicit
  operator action.

`sys/qualification` has no bootstrap path of this kind. The Admin credential
writes it with `store qualify`, one time for the life of the bucket. No
server-role policy grants a write on it.

A fresh bucket with the keyed tenant hash scheme, which is the default,
refuses to start without `--tenant-hash-key-file`. If you intend the unkeyed
scheme, pass `--tenant-hash-unkeyed` explicitly. The choice is permanent for
that bucket.

The per-tenant objects need no step:

- A server process started with `--tenant-kms-config` creates each named
  tenant's key-epoch record at startup if the record is absent. `ravel-cli`
  never creates it.
- The gateway creates a tenant's provisioning record on the tenant's first
  write.
- The gateway creates the tenant's metric metadata record on the first
  metadata flush.
- A tenant with no config record runs on the deployment defaults.
- A keyed bucket with no `sys/auth` has no durable tokens yet.

### AWS S3

Under the current templates in `deploy/iam/`, a fresh AWS bucket starts the
same way as any other store.

On AWS S3, a GET of an absent key is refused, and is not reported missing,
unless the credential holds an `s3:ListBucket` grant that covers that key. The
gateway, query and maintain templates grant one on the keys that each process
reads where absence is normal, and on nothing else. See "Bootstrap keys" in
`deploy/iam/README.md`. Under these templates, every process sees an absent
`sys/qualification`, `sys/tenancy` or `sys/gc` as absent.

Two cases need more:

- **The bucket's default encryption is a customer-managed KMS key.** Creating
  `sys/tenancy` and `sys/gc` there also needs `kms:GenerateDataKey` on that
  key. Add it to the roles that create them.
- **The templates are copies from before these list grants.** They grant no
  list that covers `sys/tenancy` or `sys/gc`. On a fresh AWS bucket every
  server process is then refused the read of `sys/tenancy` before it reaches
  `sys/gc`, and no `ravel-cli` command creates `sys/tenancy`. `ravel-cli
  gc-config set` under the Admin credential still creates `sys/gc` there, but
  that is not sufficient. Update the policies from `deploy/iam/`. As an
  alternative, run the first startup under one shared credential that can
  create both objects, then move to per-role credentials.

<a id="the-admin-credential"></a>

### The Admin credential

`ravel-cli` uses the Admin role. The Kubernetes operator provisions the three
server roles and does not provision Admin: no cluster resource field holds it
and no pod runs it. Admin is the broadest of the four credentials. It can read
every prefix and write every control object, so treat it as a privileged
operator credential and not as a service credential:

- Store it where your operators or CI jobs get their `RAVEL_S3_*` values to
  run `ravel-cli`, such as a CI secret store or an operator's short-lived
  session. Never store it in a long-running Deployment or in a Secret mounted
  into a server pod.
- Only out-of-band operator and CI invocations use it: `store qualify`,
  `gc-config set`, `provision adopt`, legal holds, `tenant parquet-grant add`
  and `remove`, and the read-only inspection subcommands. A continuously
  running process must never hold it.
- Admin cannot delete any of the protected prefixes. Its two delete grants
  cover only scratch. `AdminQualifyDelete` covers the `sys/qualify/*` objects
  that `store qualify` writes. `AdminProbeDelete` covers the `sys/pq-probe/*`
  object that the bucket probe of `tenant parquet-grant add` writes and
  removes. A leaked Admin key can forge or overwrite control objects within
  its write grant, but it cannot make existing data disappear.

Seven `ravel-cli` commands take the Maintain credential. Admin holds none of
their grants, so run these seven with the `RAVEL_S3_*` values of the Maintain
role:

- `parquet sweep`, which deletes superseded Parquet table manifests.
- `parquet repair --delete` and `--delete-version`, which delete forged
  Parquet table manifest keys. `--delete` removes the keys above the version
  bound or naming no version. `--delete-version` removes one named version
  that an operator has judged forged. See
  [repairing a forged Parquet table version](maintenance.md#repairing-a-forged-parquet-table-version).
- `maintain compact-bucket` and `maintain compact-tenant`, which take
  compaction claims under `sys/maintain/claims/compaction/` and write L1
  segments and compaction records.
- `maintain migrate`, which rewrites L1 segments and compaction records,
  deletes its cursor and raises the format floor.
- `maintain sweep`, which deletes superseded and expired segments and commit
  records and quarantines orphans.
- `catalog fold`, which writes the catalog objects that the scheduled fold
  writes. The Query credential also works for it.

See
[the IAM templates](../../../deploy/iam/README.md#which-credential-each-ravel-cli-command-takes)
for each grant.

On a deployment that runs `--tenant-kms-config`, pass the same file to the
four of those commands that write tenant data: `maintain compact-bucket`,
`maintain compact-tenant`, `maintain migrate` and `catalog fold`.

- With the flag, the commands route their data writes through the tenant's
  key as the servers do.
- The commands never record a tenant's key. If the tenant's key-epoch record
  is absent, or names a different current key than the file, the command
  refuses before it writes anything. A `--dry-run` reads the record and
  refuses the same way.
- Roll a key out to the servers first. Then run these commands with the file
  that the servers run with.
- Without the flag, the commands write under the bucket's default encryption
  and nothing fails. Only the encryption key of what they wrote differs.
- The control records that the Admin credential writes stay under the
  bucket's default encryption by design, because Admin is decrypt-only on the
  tenant keys.

See
[Encrypting objects with SSE-KMS](configuration.md#encrypting-objects-with-sse-kms).

## Readiness and the store reachability probe

`/readyz` reports store reachability and ingest health as well as startup
completion. Readiness ANDs four inputs: the startup latch, the drain latch,
ingest health and the store probe's health.

**Ingest health.** An `all` or `gateway` process turns 503 permanently after
one of its ingest shard actors is condemned (`shards_condemned > 0` at
`/metrics`, on any `signal`). No probe can recover this condition. Roll the
process. The metrics pipeline condemns a shard only after the shard exhausts
its respawn budget. The logs and spans pipelines never respawn, so one
shard-actor death on either pipeline condemns the shard immediately. See
[troubleshooting.md](troubleshooting.md).

**Store reachability.** This condition recovers without operator action. Each
process runs one background probe that reads the fixed `sys/tenancy` object
every `--store-probe-interval`. The default is `30s`, with jitter so that
replicas do not probe in lockstep.

- After **four consecutive** failed probes, `/readyz` and its Prometheus
  spelling `/-/ready` return 503.
- The **first successful** probe returns them to 200.

At the default interval, a fleet is marked unready after roughly two minutes
of failed probes. After a store outage of that length every data path fails.
Traffic then fails fast at the load balancer and does not time out per
request. The threshold is a fixed constant with no flag, so one failed probe
can never eject a whole fleet.

`/readyz` makes no object-store call. The kubelet reads only an in-memory
value that the background probe maintains. `/healthz`, the liveness endpoint,
ignores the probe and means only that the process is alive. A store outage
therefore never fails liveness and never gets healthy processes killed.

A deployment gated on readiness halts while the store is unreachable. Plan
for this at rollout time.

The probe exports three samples at `/metrics`, so the outage is visible where
nothing consumes `/readyz`:

- `ravel_store_reachable`, a gauge labeled by mode: 1 healthy, 0 unhealthy.
- `ravel_store_probe_failures_total`, a counter labeled by mode. It increments
  on every failed probe cycle, also below the readiness threshold.
- `ravel_store_probe_last_run_timestamp_seconds`, a gauge labeled by mode. It
  holds the Unix time of the probe's last completed cycle or of its spawn,
  whichever is later. Alert on the age of this gauge and not on its value. The
  other two samples move only while the probe task is alive. A task that dies
  freezes them at a healthy-looking value, while this gauge stops advancing.
  See
  [the store-probe liveness alert](../observability.md#the-store-probe-liveness-alert).

## Graceful shutdown

On SIGTERM the server first flips readiness to draining. Then it flushes and
joins every ingest shard actor, and exits. `--shutdown-timeout` (default
`25s`) bounds the drain.

The drain protects buffered-mode ingest data on a rolling update. If the
process is killed before the drain finishes, the unflushed buffers are lost.

At the shipped defaults, the worst case from SIGTERM to exit is `32.5s`:

- the `25s` drain,
- up to `2.5s` for the pre-drain heartbeat stop and readiness settle,
- a `5s` hard cap on the final OTLP trace-exporter flush.

The operator sizes the pod's shutdown lifecycle against that budget:

- `terminationGracePeriodSeconds` is **45s** on every ravel-server pod
  (gateway, query, and maintain). Kubernetes runs the `preStop` hook inside
  the grace period and sends SIGTERM only after the hook returns. The grace
  period therefore covers the `preStop` sleep, the `32.5s` server budget and
  headroom: `10s + 32.5s + 2.5s`, rounded up.
- With `spec.probes.dedicatedHealthPort` set, stopping the health listener
  adds up to 6s to the server's budget (38.5s). The operator then sets
  **51s**: `10s + 38.5s + 2.5s`.
- Do not set a grace period shorter than the server's budget. SIGKILL then
  lands mid-drain and buffered data is lost, which is irreversible. A longer
  grace period only slows a rolling update's pod turnover by a few seconds, so
  the operator rounds up.
- A `preStop` hook sleeps **10s** before SIGTERM. When a pod is deleted,
  endpoint removal and SIGTERM are concurrent. The kubelet sends SIGTERM at
  the same time as the EndpointSlice removal begins to propagate to every
  node's kube-proxy and to any external load balancer. Without the sleep, the
  server can start draining while new requests are still routed to it. The
  `10s` covers kube-proxy reprogramming across nodes and typical cloud
  load-balancer drain under load.
- The hook uses the native `sleep` lifecycle action and not an `exec` of a
  `sleep` binary. The container runs with a read-only root filesystem and
  every Linux capability dropped.

The operator renders no `--shutdown-timeout` flag, so the server runs at its
compiled default. `45s` (`51s` with the dedicated health port) is the correct
grace period today. `--shutdown-timeout` is configurable on the server
itself. If a future CRD field exposes it, the grace period must stay above
the new server budget plus the `preStop` sleep.

## Durable auth refresh

On a keyed bucket, a request-serving process (`all`, `gateway`, `query`)
resolves bearer tokens against a cached copy of the durable `sys/auth` map, as
well as the static and OIDC resolvers. A background refresh loop keeps that
copy current. The durable resolver comes after the static and OIDC chain, so
it answers only a request that the others cannot answer. An unkeyed bucket
has no keyed-hash token map, so durable auth is unavailable there.

The shipped Gateway and Query policy documents grant the read on `sys/auth`.
Of the four shipped roles only Admin can write it, through `ravel-cli tenant
token upsert` and `revoke`.

The loop re-reads `sys/auth`:

- On success, it advances the staleness gate.
- On any read or decode failure, it keeps the last known map and leaves the
  gate where it is.
- If it cannot refresh for a hard multiple of the refresh horizon, the cached
  map is treated as untrustworthy and token resolution fails closed.

Three counters, all labeled by mode, show the loop's health:

- `ravel_durable_auth_refresh_failures_total`: background refreshes that
  failed to read or decode `sys/auth`.
- `ravel_durable_auth_on_miss_rereads_total`: off-horizon re-reads begun after
  the rate limiter, when the request path saw an unknown token.
- `ravel_durable_auth_stale_fail_closed_total`: token resolutions refused
  because the cached map was hard-stale.

Alert on the first counter and not on the third. The first increments from the
moment that refresh fails, one refresh interval apart, while the last known
map still serves. The third starts only after the horizon is crossed and auth
fails closed. The gap between them is the time in which a fix must land. Both
alert rules are in
[troubleshooting](troubleshooting.md#durable-auth-refresh-is-failing).

<a id="the-dedicated-fragment-listener"></a>

## The dedicated fragment listener

This section applies only under `--distributed-query`. The flag requires
`--fragment-key-file`, `--fragment-listener` with its three `--fragment-tls-*`
files, and, in a build that serves Flight SQL, `--sql-ticket-key-file`. The
published image is such a build. A process with the flag and any of these
missing refuses to start, so it never exposes an unauthenticated fetch surface
and never dials an intra-cluster fragment or SQL slice in plaintext.

### The fragment key file

`--fragment-key-file` holds a short list of 32-byte cluster fragment keys:

- One key per non-empty line, each line 64 hexadecimal characters.
- Blank lines and lines that begin with `#` are ignored.
- A file with no key line fails startup. So does any line that is not 64
  hexadecimal characters.
- The file accepts several keys. To rotate a key, add the new one, roll the
  fleet, and then remove the old one.
- The secret is in a file and not in an inline value or an environment
  variable, so it never appears in a process listing.

These keys mint and verify a per-tenant, per-query capability. That capability
and nothing else authorizes a fragment fetch. There is no shared
cluster-internal bearer token.

### The SQL ticket key file

The Flight SQL lane signs its tickets with a separate key file,
`--sql-ticket-key-file`:

- It has the same shape and rotation rule as `--fragment-key-file`. Every node
  in the cluster reads it. Keep it with the same care as the fragment key
  file.
- Give it different keys from the fragment key file.
- It requires `--distributed-query`. Setting it alone fails startup.
- `--distributed-query` requires it in a build that serves Flight SQL.
- No node derives a SQL ticket key from the fragment key file. A node of the
  previous release that ran without this file did: it derived its SQL ticket
  secret from the first fragment key, and turned that secret into separate
  client and slice keys.

Two nodes that do not share a SQL ticket key disagree on every ticket, and
they coexist during a rolling deploy. If you roll a key file holding a new key
onto a fleet whose nodes still use the derived key, two things fail until the
roll finishes:

- A client query fails. `GetFlightInfo` returns one endpoint with no location,
  so a client behind a balancer can send `DoGet` to any node. A ticket minted
  on a node on the file and redeemed on a node still on the derived key, or
  the reverse, fails with `invalid_argument` ("malformed flight ticket").
- SQL slices between two such nodes fail the worker's MAC and run on the
  coordinator, so the query loses parallelism.

To upgrade a fleet that runs the previous release without the file, make the
first key file equal to the key that its nodes derive, and then rotate. A
node on that one-line file and a node of the previous release on the derived
key hold the same client and slice keys, so the upgrade roll itself can carry
the file. An upgrade straight from a release before the one that added
`--sql-ticket-key-file` has the window either way: such a node signs with the
derived key itself, not with keys derived from it.

1. Compute the key that each node derives today. `ravel-server` does not print
   it and has no flag that does. It is the BLAKE3 derive-key of the first key
   in the fragment key file, as its 64 lowercase hex characters, under the
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

   Run it where the fragment key file already is. The output line is a secret
   with the same custody as the fragment key file. `ravel-server`'s
   `sql_distrib` unit tests pin the context string, the lowercase-hex input
   and the resulting key. Nothing checks this page's copy of the command.
2. Roll `--sql-ticket-key-file`, pointing at that one-line file, onto every
   node, in the same roll as the upgrade or before it. A node on the file and
   a node on the derived key now hold the same keys, so nothing fails during
   the roll.
3. Add a freshly generated key (`openssl rand -hex 32`) as the *second* line
   and roll. Every node then verifies the new key before any node mints with
   it. This step starts the rotation off the derived key, which anyone who
   holds the fragment key file can recompute.
4. Move the new key to the first line and roll again. Every node now mints
   under it and still verifies the old key.
5. Wait until the longest Flight SQL ticket TTL has passed since that roll
   finished. Then delete the old line and roll a last time.

Do not add the new key as the first line in a single roll, the way the
fragment key file is rotated. That reopens the mixed window for client
tickets: a node already on `[new, old]` mints under the new key, and a node
still on `[old]` cannot verify that ticket.

### Where the fragment surface listens

The cluster-internal fragment surface, on which one query worker fetches a
slice for another, has its own listener. The dedicated listener terminates
TLS in-process: `--fragment-listener <addr>`, with `--fragment-tls-cert`,
`--fragment-tls-key` and `--fragment-tls-ca`. Both distributed lanes use it.

**The PromQL lane's pinned fragment fetches.** On the fragment surface, the
public gRPC listener serves only cross-cluster federation with ordinary tenant
credentials and refuses pinned fetches. The dedicated listener serves pinned fetches and
refuses federation.

**The SQL lane's slice `DoGet`.** The dedicated listener serves `DoGet` for a
slice ticket and refuses every other Flight and Flight SQL method. No method
other than a slice `DoGet` returns data there:

- Client Flight SQL methods answer `permission_denied`.
- Methods that the service does not implement answer `unimplemented`.
- A `DoGet` that is not a valid slice capability answers `unauthenticated`, or
  `permission_denied` for a client ticket.

The public gRPC listener keeps the client Flight SQL surface:

- It refuses, with `permission_denied` ("slice fetch rejected:
  wrong_surface"), a slice ticket whose MAC verifies under this node's slice
  keys.
- A forged slice ticket, or one under a key this node lacks, takes the client
  path and is refused there, uncounted.

A coordinator dials each worker's advertised `fragment_endpoint` over TLS, on
both lanes. A worker advertises its dedicated listener there.

Startup refuses a `--fragment-listener` address equal to `--listen-http`,
`--listen-grpc` or `--mtls-listener`, so the surfaces always stay separate.

### TLS on the fragment listener

TLS on this listener provides two things:

- Channel confidentiality, because per-tenant, per-query capabilities travel
  on it.
- Server authenticity, so a coordinator can confirm that it dialed a real
  cluster worker and not an interceptor that can harvest capabilities.

The capability always authorizes a fetch, and the certificate never does.
Coordinators verify every worker certificate against the pinned
`--fragment-tls-ca` with one fixed expected server name, `ravel-fragment`.
Every worker certificate carries that name as a dNSName SAN. Per-process
certificate identity is not required. Any certificate that the dedicated CA
signed means "a fragment worker of this cluster". No identity is ever parsed
from a certificate.

TLS on this listener is mutual. The listener verifies its callers against the
same `--fragment-tls-ca`. A peer that presents no certificate from that CA is
refused at the handshake, before any capability is read. A coordinator
presents this process's own `--fragment-tls-cert` and `--fragment-tls-key`
when it dials a peer. One key pair serves both directions, because every
fragment process is both a worker and a coordinator. Mutual TLS narrows who
can present a capability.

**Ravel mints no certificates or keys.** The operator provisions the PEM files
out of band. The certificate and key are read once at startup, so certificate
rotation is a rolling restart. The worker certificate must have:

- A `ravel-fragment` dNSName SAN. The SAN is verified, not the CN.
- `extendedKeyUsage = serverAuth, clientAuth`. Both are required, because the
  same certificate serves inbound fragment fetches and is the client identity
  on outbound ones.
- A signature from the CA distributed as `--fragment-tls-ca` to every query
  node.

The `extendedKeyUsage` cases are:

| Certificate | Result |
|---|---|
| `serverAuth` only | Serves fragments but cannot dial them. The handshake in the missing direction fails. |
| `clientAuth` only | Dials fragments but cannot serve them. The handshake in the missing direction fails. |
| `anyExtendedKeyUsage` | Does not stand in for either usage. The TLS stack accepts only the required purpose itself. |
| No `extendedKeyUsage` extension at all | Unconstrained. The process starts. |

Startup reads the certificate and refuses when either usage is absent. The
error names the file and the missing usage. An upgrade from a release that
documented `serverAuth` alone therefore fails at startup, and does not
degrade every fan-out to coordinator-local execution.

The same certificate, key and CA serve SQL slice `DoGet`. The SQL lane has no
second certificate. A coordinator dials SQL slices with the same pinned CA,
the same `ravel-fragment` server name and the same client certificate that it
dials fragment fetches with.

### With cert-manager

Issue one certificate per query node from a cluster-internal issuer, with the
fixed SAN. A shared certificate also works, because identity is not per
process:

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

`--advertise-fragment-endpoint` is required in this example. Both listeners
bind the wildcard so that they answer on the pod's own address, but the
heartbeat record must publish an address that siblings can dial. Project the
pod IP with the downward API (`fieldRef: status.podIP`), or pass the pod's
stable DNS name from a headless Service. Without the flag, startup refuses and
does not publish `0.0.0.0:4319` for every peer to fail against.

`--listen-grpc` is also required in this example. The public gRPC listener
carries client Flight SQL and federation. It defaults to `127.0.0.1:4317`,
which nothing outside the pod's own loopback reaches.

The heartbeat record publishes one endpoint, the fragment listener, and both
distributed lanes dial it. This node refuses a slice ticket sent to its public
gRPC address.

cert-manager rewrites the Secret on renewal, but Ravel reads the files only at
startup. Schedule a rolling restart of the query fleet on the renewal cadence.

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
`--fragment-tls-key`. A reissued worker certificate, or a rotated CA, takes
effect on the next rolling restart.

Check an existing certificate before the restart that turns the listener on:

```sh
openssl x509 -in fragment.crt -noout -ext extendedKeyUsage
```

It must list both `TLS Web Server Authentication` and `TLS Web Client
Authentication`. A certificate issued against a release that documented
`serverAuth` alone lists only the first, and startup refuses it.

### Rolling onto the dedicated listener

`--distributed-query` refuses to start without `--fragment-listener` and,
in a Flight SQL build, without `--sql-ticket-key-file`. The previous release
accepts both flags, so a fleet that runs it without them moves in two rolls:
turn both flags on while still on the previous release, then upgrade. Results
stay identical throughout. Only which nodes a slice can fan out to changes
during a roll.

A fleet that upgrades a node of the previous release without the dedicated
listener straight onto this release mixes the two layouts for one roll:

- An upgraded node advertises its TLS fragment endpoint. It refuses pinned
  fetches and SQL slice tickets on the public port.
- A node of the previous release without the flag advertises its public gRPC
  address as its fragment endpoint and keeps serving the fragment surface
  there.
- An upgraded node's first dial to a node of the previous release fails: it
  is a TLS dial to a plaintext port. A node of the previous release fails its
  first PromQL dial to an upgraded node, a plaintext dial to a TLS port, and
  sends it no SQL slice at all, because the upgraded node's record carries no
  public gRPC address for that node's SQL lane to dial.
- A failed dial is re-dispatched once to another worker. The slice runs
  coordinator-local only if that attempt fails too.

The release that moves SQL slices onto the dedicated listener also moves the
`queryfrag` protocol version from 4 to 5. Coordinators drop workers that
advertise another version at routing time, before any dial. During the one
rolling deploy onto this release, a node on version 5 and a node on version 4
therefore send each other no slices on either lane. Those slices run
coordinator-local. The fleet loses parallelism for that deploy, and results do
not change. After every node runs the new release, fan-out resumes.

## Federating to a remote cluster

`--remote-cluster` points this coordinator at another Ravel cluster's fragment
fetch surface. Pass one flag per remote, as a comma-separated `key=value`
spec:

```
ravel-server --mode query \
  --remote-cluster name=eu,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu.token,tenant=acme \
  --remote-cluster name=apac,endpoint=apac.internal:9443,credential-file=/etc/ravel/apac.token,tenant=acme,tls-ca-file=/etc/ravel/apac-ca.pem,soft-timeout=15s
```

`name`, `endpoint` and `credential-file` are required. `tenant` is required on
a keyed bucket, which is the default for a fresh bucket; see [One credential per
local tenant](#one-credential-per-local-tenant). `tls` (default `true`),
`tls-ca-file`, `skip-unavailable` (default `false`) and `soft-timeout` are
optional. `--remote-cluster-soft-timeout` sets the default soft timeout for
every remote that does not name its own. A remote that does not answer within
its bound is treated as unavailable. That fails the query unless the remote
has `skip-unavailable`.

Federation requests carry the `queryfrag` protocol version, and a remote
refuses a request on another version. The release that moved the version from
4 to 5 therefore splits federation until both clusters run the same release.
A cluster on that release and a remote on an earlier one fail every federated
query with a `Federation` error that names the remote. For a remote with
`skip-unavailable`, the query skips the remote with a partial-coverage
warning.

The credential is an operator secret read from a file, never an inline value.
It is the principal that the remote sees. A federated query never forwards the
calling client's credential across a cluster boundary.

### One credential per local tenant

A remote credential authorizes one tenant's data on the remote, so it belongs
to one local tenant. `tenant` names that local tenant, and a query from any
other local tenant never dials that remote. A coordinator that serves several
local tenants writes one spec per local tenant, each with its own `name` and
its own `credential-file`:

```
ravel-server --mode query \
  --tenant-token acme-token=acme \
  --tenant-token beta-token=beta \
  --remote-cluster name=eu-acme,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu-acme.token,tenant=acme \
  --remote-cluster name=eu-beta,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu-beta.token,tenant=beta
```

`--tenant-token` on the command line puts every bearer token into argv, which
a pod spec or process listing exposes. Use `--tenant-token-file PATH` to read
the same `TOKEN=TENANT` pairs from a file (env `RAVEL_TENANT_TOKEN_FILE`, for
the path only):

- The file has one pair per line. Blank lines and `#` comments are skipped.
- Mount the file from a Secret and do not template tokens into args.
- `--tenant-token` and `--tenant-token-file` are mutually exclusive. Startup
  refuses if both are set.
- The file is read once, at startup. After you rotate the mounted Secret,
  restart the pod to load the new tokens.
- An empty or comment-only file parses to an empty map with no startup error.
  A Secret mount that failed to populate looks the same. The file then
  authenticates nothing. Unless `--maintain-tenant` names tenants, background
  fold, compaction and retention also widen to every tenant that storage
  discovers.

A local tenant that no remote names gets local data only, reported as a
complete result. A remote that the tenant holds no credential for is outside
its query and is not missing from it. No warning and no `partial: true`
appear.

A spec without `tenant` leaves the remote reachable by every local tenant.
That is correct only where the coordinator runs queries for one local tenant,
on a bucket created with `--tenant-hash-unkeyed`. A process that can serve
more than one local tenant **refuses to start** with such a spec. It does not
fan every local tenant's selectors and discovery out under the one credential
and return another tenant's series. A process can serve more than one local
tenant in four cases:

- Two or more `--tenant-token` values or `--tenant-token-file` lines name
  different tenants.
- An `--alert-rules-file` names a tenant that no `--tenant-token` or
  `--tenant-token-file` does. The alert evaluator runs one query loop per
  tenant in that file, against the same engine that federation is installed
  on. Those queries federate although no request produced them.
- Any dynamic resolver is enabled: `--dev-insecure-tenant-header`,
  `--oidc-issuer`, or `--mtls-enabled`. Each of them derives the tenant from a
  request header or a token claim.
- `--tenant-hash-key-file` is set (a keyed bucket, which is the default for a
  fresh bucket) in All, Gateway or Query mode. Those three modes install the
  durable `sys/auth` bearer resolver, so a tenant can be onboarded without a
  restart, and the guard applies in all three; of them, only All and Query
  serve queries. On a keyed bucket every `--remote-cluster` needs `tenant=`,
  even with a single `--tenant-token`.

The startup error names every spec that needs a `tenant` and what makes the
deployment multi-tenant:

```
--remote-cluster 'eu' names no local tenant on a process that can serve more
than one local tenant (2 distinct static bearer tenants are configured). A remote cluster holds one remote credential and cannot express one
credential per local tenant ... Add tenant=<local tenant> to each of those specs
...
```

Startup also refuses a `tenant` that no `--tenant-token`,
`--tenant-token-file` line, or `--alert-rules-file` rule names. This check
applies where the tenant set is fully known: static bearer tokens and alert
rules, with no dynamic resolver and no durable `sys/auth` map. Such a mapping
can never fire, and its only symptom is a remote that answers nobody. On a
keyed bucket a tenant provisioned only in `sys/auth` is a valid target. A tenant that only alert rules
name is a valid target. Mapping a remote to it is the supported way to run
alert rules over data that is partly on a remote.

### TLS to a remote

**TLS is on unless the spec says otherwise.** The specs in the examples above
do not name `tls`, and they dial over TLS. They verify the remote against the
system trust roots, plus `tls-ca-file` when one is set. A spec with
`tls-ca-file` and no `tls` key means "TLS on, with this CA trusted". You do
not need to pair the two.

Use `tls=false` only for a hop that a lower layer already encrypts and access
controls, such as a service mesh sidecar or an encrypted tunnel. With TLS off,
the operator credential, the federated query and every returned result stream
cross the network in cleartext. Anyone on the path can read and replay the
credential. The choice is explicit and logged:

```
WARN SECURITY: --remote-cluster 'eu' is configured with tls=off. The operator
bearer credential presented to this remote, every federated query, and every
returned result stream travel in cleartext to 'eu.internal:9443'. ...
```

One line is logged per plaintext remote, and a TLS remote logs nothing. If you
see this warning and did not intend plaintext, remove the `tls=false` key.
`tls=false` together with `tls-ca-file` fails startup, because the CA bundle
has no effect with TLS off.

## Background

Decision records behind this page:
[fail-closed isolation and startup invariants](../../adrs/0050-fail-closed-isolation-and-startup-invariants.md),
[tenant-scoped credentials and control-plane protection](../../adrs/0072-tenant-scoped-credentials-and-control-plane-protection.md),
[credential scoping](../../adrs/0055-storage-credential-scoping.md),
[distributed read fan-out](../../adrs/0071-distributed-read-fanout.md),
and [format migration machinery](../../adrs/0066-format-migration-machinery.md).
