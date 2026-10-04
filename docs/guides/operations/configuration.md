# Configuration (day 0)

Everything you decide before you start a process for the first time. Some of
these choices are permanent for the lifetime of a bucket (the tenant hash
scheme, a tenant's shard count), and some are a restart away (cache sizes,
admission limits). The permanent ones are called out where they appear.

The exhaustive list of what every flag is called, its environment variable and
its default lives in [the generated server flag reference](../../reference/ravel-server-flags.md)
and [the generated CLI flag reference](../../reference/ravel-cli-flags.md).
Those pages are rendered from the binaries' own command definitions and a test
fails when they drift. This page explains how to choose a value, not what the
flags are.

- [Process modes](#process-modes)
- [Storage backend and credentials](#storage-backend-and-credentials)
- [Storage credential roles](#storage-credential-roles)
- [Encrypting objects with SSE-KMS](#encrypting-objects-with-sse-kms)
- [Admission limits](#admission-limits)
- [Read cache tiers](#read-cache-tiers)
- [Retention and garbage-collection configuration](#retention-and-garbage-collection-configuration)
- [Tenancy setup](#tenancy-setup)
- [Durable shard count](#durable-shard-count)
- [Logs fetch policy and store cost profile](#logs-fetch-policy-and-store-cost-profile)
- [Indexed fields and typed attribute columns](#indexed-fields-and-typed-attribute-columns)
- [Per-query budgets](#per-query-budgets)

## Process modes

`--mode` decides which jobs a process runs. It is the single most consequential
flag on this page, because a deployment missing a mode is missing the work that
mode does, silently.

| Mode | Runs |
|---|---|
| `all` | Ingest (OTLP and Remote Write), the query API, the catalog fold over every tenant, alert evaluation. No maintenance. |
| `gateway` | Ingest. No scheduled catalog fold. |
| `query` | The query API, alert evaluation, and the on-demand fold route. No scheduled catalog fold. |
| `maintain` | Compaction, retention, the sweeper, the at-rest scrubber, and the catalog fold over the tenants it owns. No ingest, no query API. It still binds `--listen-http` for liveness, and it needs a backend that reports the `multipart` capability. |

The scheduled catalog fold runs in `maintain` and `all`; a `maintain` fleet
divides it across replicas by ownership. Every maintenance
loop runs only in `maintain`. A deployment made of `all` processes alone
therefore folds its catalog but never compacts, never applies retention and
deletes no durable data (its one delete is the admission reconcile's reap of
dead ingest processes' admission snapshots), and a deployment made of `gateway` and `query`
processes alone folds nothing on a timer. Read
[Maintenance](maintenance.md) before you decide you do not need a `maintain`
process.

## Storage backend and credentials

`--store memory` is an in-process store for tests and local experiments.
Nothing survives process exit. `--store s3` is the only durable choice.

Ravel does not use the AWS credential chain (profiles, `AWS_ACCESS_KEY_ID`,
`~/.aws/config`). It reads the `RAVEL_S3_*` environment variables and their
matching flags, and nothing else. `force_path_style` is not configurable: the
client always uses path-style addressing.

### Plaintext endpoints

The client speaks plaintext HTTP only when `--s3-endpoint` itself says
`http://`. An `https://` endpoint, and real AWS S3 with no endpoint at all,
never fall back to plaintext, so a redirect or a misconfigured proxy cannot
downgrade the connection.

A plaintext endpoint puts every object this process writes and reads, and the
credentials signing those requests, on the network in the clear. Startup
therefore refuses an `http://` endpoint whose host is not loopback unless
`--s3-allow-http` (`RAVEL_S3_ALLOW_HTTP`) is set. The refusal names the flag.

- `http://127.0.0.1:9000`, `http://localhost:9000`, `http://[::1]:9000`:
  allowed with no flag. The traffic never leaves the host.
- `http://rustfs:9000`, `http://rustfs.ravel-system.svc:9000`, or any other
  name or address on the network: refused unless the flag is passed. A
  container or a pod reaches its object store over the network, never over
  loopback, so a plaintext in-cluster RustFS or floci needs the flag even
  though the traffic stays inside the cluster.
- `https://...`: unaffected, and the flag does nothing.
- `rustfs:9000`, or any endpoint written with no scheme: refused at startup.
  An endpoint that begins with neither `https://` nor `http://` is not a
  usable URL, so the refusal quotes the endpoint as it was written and asks
  for the scheme. `--s3-allow-http` does not accept it: the flag chooses
  between TLS and plaintext, and an endpoint with no scheme has asked for
  neither. The scheme itself is matched without regard to case, so
  `HTTPS://rustfs:9000` is an `https` endpoint.

`ravel-cli` applies the same rule from the same code, with the same
`--s3-allow-http` flag and `RAVEL_S3_ALLOW_HTTP` variable. It ships in the
server image and talks to the same bucket with the same credentials, so a
`ravel-cli` command against a plaintext non-loopback endpoint is refused
unless the flag is passed, and an `https://` endpoint never enables plaintext
there either.

Under the Kubernetes operator the same decision is `spec.storage.s3.allowHttp`
on the `RavelCluster` (default `false`), which renders the flag into every
server container's arguments and `RAVEL_S3_ALLOW_HTTP=true` into the
store-qualification Job that runs `ravel-cli store qualify` before any server
pod exists. A cluster with a plaintext in-cluster endpoint therefore needs
`allowHttp: true` for qualification to run at all.

Prefer terminating TLS at the object store over setting the flag. The flag is
for a development backend that speaks no TLS, not for a production one whose
certificate is inconvenient.

RustFS, for local development (loopback, so no flag):

```sh
--store s3 --s3-endpoint http://127.0.0.1:9000 --s3-bucket ravel-dev \
--s3-access-key ravel --s3-secret-key ravel-dev-secret
```

AWS S3, with a static key pair (omit `--s3-endpoint`, which is what selects real
S3):

```sh
--store s3 --s3-bucket my-ravel-bucket --s3-region us-west-2 \
--s3-access-key AKIA... --s3-secret-key ...
```

A `--store s3` process with no bucket or no credentials fails at startup with an
error naming the missing one. It never starts in a half-configured state.

### Upload and read checksums

Every PUT carries a CRC64-NVME checksum (`x-amz-checksum-crc64nvme`) by
default, including those from the per-tenant stores `--tenant-kms-config`
routes to. An object of any size up to S3's 5 GiB single-request limit is
sent as one checksummed PUT rather than in parts. The endpoint verifies the body against it and rejects a PUT whose
bytes changed on the way, so a corrupted object never becomes visible, and it
stores the checksum with the object. Every request except a LIST also asks the
endpoint to return that stored checksum (`x-amz-checksum-mode: ENABLED`), and a
full-object read is verified against it before the bytes are used. A mismatch is an error,
not a wrong answer. This is the only check a commit record gets: it is a bare
protobuf with no checksum of its own.

- `--s3-upload-integrity` (`RAVEL_S3_UPLOAD_INTEGRITY`): `crc64nvme` (the
  default), `sha256`, or `off`. `sha256` is verified by the endpoint on upload
  only: Ravel cannot recompute it on read, so a read of an object stored with
  it counts as unverified.
- `--s3-request-stored-checksum` (`RAVEL_S3_REQUEST_STORED_CHECKSUM`): `true`
  (the default) or `false`, written `--s3-request-stored-checksum=false`.
  Turned off, no request asks for the stored checksum, and every full-object
  read is served unverified and counted.

AWS S3 and RustFS accept both headers. An endpoint that does not accept the
upload checksum header fails every PUT with the endpoint's error. Startup
writes nothing to an existing bucket, so the process can report ready first
and fail at its first flush. The remedy is `--s3-upload-integrity off`, and
its cost is that every object the process writes, commit records included,
has no transport checksum to verify against. An endpoint that rejects the
checksum-mode request header needs `--s3-request-stored-checksum=false`. Both
flags are ignored under `--store memory`. The per-tenant stores that
`--tenant-kms-config` routes to apply both flags exactly as the default store
does.

A read that finds no stored checksum it can check is served, never refused, and
counted in `ravel_store_get_unverified_total` (see
[Observability](../observability.md)). Objects written before upload checksums
were on carry none, so the counter moves on an upgraded bucket until retention
or a rewrite replaces them. An object larger than one request body (8 MiB by
default) is read in several responses, none of which covers the whole object,
so every whole read of one is counted too: scrub, compaction and quarantine
read large data objects whole, and the counter keeps growing on an honest
endpoint. The counter does not separate that case from an endpoint that
returns no stored checksum: a count that grows while no scrub, compaction or
quarantine pass is reading, on a bucket written with `crc64nvme`, points at
the endpoint.

Under the Kubernetes operator the same two settings are
`spec.storage.s3.uploadIntegrity` and `spec.storage.s3.requestStoredChecksum`
on the `RavelCluster`. They also govern the operator's own S3 client.
`ravel-cli` takes the same two options with the same defaults; see
[Upload checksums](#upload-checksums) under its store options.

### Choosing a credential source

`--s3-auth` picks where the credentials come from.

- `static` (the default) takes the access key and secret key from the flags or
  the environment. Both are required.
- `instance-role` takes short-lived credentials from the EC2 instance metadata
  service instead, so nothing static is stored on the instance, in the
  environment, or in logs. Only `--s3-bucket` is then required, and passing any
  of `--s3-access-key`, `--s3-secret-key`, `--s3-session-token` or
  `--s3-credentials-file` alongside it is a startup error naming the conflict
  rather than a precedence rule to reason about. An exported
  `RAVEL_S3_ACCESS_KEY` counts. The first credential fetch happens at startup,
  so a misconfigured instance role fails to start rather than failing its first
  request.

On EC2, attach the instance role and start with no credential flags at all:

```sh
ravel-server --store s3 --s3-bucket my-bucket --s3-region us-east-1 \
  --s3-auth instance-role
```

Under `static` there are two further sources, both for credentials that rotate:

- `--s3-session-token` pairs a temporary token with the key and secret for
  credentials issued by a token service.
- `--s3-credentials-file` names a JSON file of `access_key_id`,
  `secret_access_key` and an optional `session_token` that an external process
  rewrites on disk. It wins over the inline flags, including the session token.
  It is read once at startup, so an unreadable or malformed file fails startup;
  after that it is re-read on the request path only when its modification time
  changes, and a parse failure during a rotation keeps serving the last good
  credential with a rate-limited warning.

`ravel-cli` accepts the same store flags and environment variables, including
`--s3-auth`, with one gap: it has no `--s3-kms-key` and never sets a key id on
its writes.

`--store` unset means `memory`, and the fallback is not silent. Every
`ravel-cli` command that walks tenant data opens its report with the store it
resolved:

```
store: memory (default)
store: memory
store: s3
```

On the defaulted memory store only, a walk that reaches no data at all is
refused rather than reported as a healthy zero:

```
--store defaulted to memory, which holds no data for tenant "clickbench";
maintain compact-tenant found no objects there and would have reported a
healthy zero-work result. Pass --store s3 (with RAVEL_S3_BUCKET and its
credentials) to run against the real bucket, or load data first.
```

An explicit `--store memory` keeps the zero-count report: that store was
chosen, so an empty result is an answer.

### Upload checksums

`ravel-cli --store s3` attaches a server-verified checksum to every PUT and
asks for the stored one back on every read:

- `--s3-upload-integrity` (`RAVEL_S3_UPLOAD_INTEGRITY`): `crc64nvme`, the
  default, attaches `x-amz-checksum-crc64nvme`; `sha256` attaches
  `x-amz-checksum-sha256`; `off` attaches none. The endpoint verifies the body
  against the checksum, rejects a PUT whose bytes do not match, and stores the
  checksum with the object. An endpoint that does not support the header fails
  the first write loudly; `off` is the remedy there, and commit records written
  under it are unverified. With a checksum on, every object goes out as one
  PUT rather than in parts, so an overwrite above S3's 5 GiB single-request
  limit is refused (with `off` named as the remedy); no `ravel-cli` write
  comes near that size.
- `--s3-request-stored-checksum` (`RAVEL_S3_REQUEST_STORED_CHECKSUM`): on by
  default, it sends `x-amz-checksum-mode: ENABLED`, so a whole-object read is
  checked against a returned CRC-64/NVME or CRC-32C checksum before its bytes
  are used, and a mismatch is an error. A read that comes back with no
  checksum, or with a SHA-256 one, is served unverified.
  `--s3-request-stored-checksum=false` stops sending the header, for an
  endpoint that rejects it.

`ravel-cli store qualify` reports whether the endpoint returns the stored
checksum; see [qualify the store](deployment.md#qualify-the-store).

## Storage credential roles

Every Ravel process holds one S3 credential and uses it for every object-store
call it makes. With a single bucket-wide credential, a leak from any one process
can read, overwrite or delete anything in the bucket. Scoping the credential to
the job the process actually does means a leaked credential can only do what
that job legitimately does, and only one of the four can delete durable data.

This is enforced entirely at the storage backend's own policy layer (AWS IAM,
or whatever policy layer an S3-compatible store exposes). Ravel's code plays no part in it: there
is no in-process authorization check and no change to the `RAVEL_S3_*` contract.
You provision a narrower credential per role and attach the policy.

Using one credential for everything is still supported, and it is the right
choice for a development or single-operator deployment.

### The four roles

| Role | Process | What it does |
|---|---|---|
| Gateway | `--mode gateway`, and the ingest half of `--mode all` | Writes L0 segments and their commit records, idempotency markers, a tenant's provisioning record on adopt, and on a keyed bucket each tenant's recovery manifest under `sys/t/`. Runs no catalog fold: the scheduled fold runs in `--mode maintain` and `--mode all`, though `gateway.json` still carries the fold's catalog grants. On a keyed bucket, reads the durable token map `sys/auth`. Reads each tenant's config record `t/<hash>/config` for its admission-limit overrides, and reads and writes its metric metadata record `t/<hash>/m/meta`. Deletes the admission snapshots of dead ingest processes under `t/<hash>/<signal>/admission/`, its one delete grant; it deletes no durable object. |
| Query | `--mode query`, and the query half of `--mode all` | Lists and reads commit records, catalog objects and segment data. Runs the catalog fold only through the on-demand fold route, writing catalog snapshot parts, `HEAD` and index objects when it does, and appends query-audit records. On a keyed bucket, reads the durable token map `sys/auth`. Reads each tenant's config record `t/<hash>/config` for its declared typed-column overrides, and its metric metadata record `t/<hash>/m/meta`. Runs the alert evaluator, so it writes alert transitions under `t/<hash>/a/l0/` and `t/<hash>/a/c/` and reads and writes each tenant's alert lease `t/<hash>/a/alert-lease` and state memo `t/<hash>/a/state/latest`. For Parquet table queries, lists and reads the table manifests under `t/<hash>/pq/t/` and reads the location grants record `t/<hash>/pq/grants`. Runs `CREATE EXTERNAL TABLE` and `DROP TABLE` over `POST /api/v1/sql`, so it creates new table manifest versions under `t/<hash>/pq/t/` (create only: it cannot overwrite an existing version) and writes and deletes its own bucket-probe scratch objects under `sys/pq-probe/`. Deletes no data, catalog or control-plane object. |
| Maintain | `--mode maintain` | Compaction, retention and the sweeper. Runs the scheduled catalog fold, so it also writes catalog snapshot parts, `HEAD`, and index objects (name postings and column stats). The only role that deletes durable data: L0 and L1 segments, commit records, idempotency markers, the query-audit shard, erasure requests (`del/*.dreq`) and superseded Parquet table manifests under `t/<hash>/pq/t/`. It also deletes superseded catalog snapshot parts and index objects, quarantined copies and dead worker records. `ravel-cli parquet sweep`, `ravel-cli maintain compact-bucket` and `ravel-cli maintain compact-tenant` run under this credential. Reads each tenant's config record `t/<hash>/config` to resolve the retention window, and the alert state memo `t/<hash>/a/state/latest` for alert retention. |
| Admin | `ravel-cli` | One-off bootstrap and mutation commands. Invoked by an operator or a CI job, never by a long-running server. The broadest of the four. Writes each tenant's config record `t/<hash>/config` (`typed-attr-column`, `clustering-key` and `bloom-scope` set commands) and Parquet location grants record `t/<hash>/pq/grants` (`tenant parquet-grant add` and `remove`; `add` also writes and deletes a probe object under `sys/pq-probe/`). See [the Admin credential](deployment.md#the-admin-credential). |

Under `--tenant-kms-config`, Gateway, Query and Maintain also read and write each
configured tenant's key-epoch record `t/<hash>/enc` at startup.

Maintain runs the scheduled catalog fold and Query runs the on-demand fold
route, so both hold the catalog write grants. Gateway runs no fold, but
`gateway.json` still carries the same catalog grants from when it did.

### The shipped policy documents

One policy document per role lives in [`deploy/iam/`](../../../deploy/iam/)
rather than being transcribed here, so a policy edit is a diff that a test
checks against the real object-key layout in CI. Replace `my-ravel-bucket` with
your bucket in each file, then attach each document to the principal whose
access key that role's deployment uses.

The KMS statement's `Resource` value is a JSON array, shipped with exactly one
entry: the placeholder `arn:aws:kms:us-east-1:111122223333:key/REPLACE-WITH-TENANT-KEY-ID`.
That single-entry array is correct as shipped only for a deployment with one
KMS key. Two independent flags put keys in play (see
[Encrypting objects with SSE-KMS](#encrypting-objects-with-sse-kms)), and each
role's policy must authorize every key its own writes and reads can reach, so
that array needs an exact ARN for each of:

- the key configured with `--s3-kms-key`, if the deployment sets it. It is
  applied to the default store, so it encrypts every PUT the process makes that
  no per-tenant key overrides. Omit it and Gateway, Query and Maintain PUTs fail
  with `AccessDenied`, and reads of objects already written under it fail KMS
  decryption for every role including Admin.
- every key configured in the `--tenant-kms-config` file, one entry each.

Add each ARN as its own entry, rather than replacing the single placeholder with
your one key and calling it done. A configured key missing from the array
does not fail at startup: the process starts normally, and the gap surfaces
only when a request first uses that key, as `AccessDenied` on that KMS call, at
runtime rather than at deploy time.

Three facts about those documents are worth knowing before you edit them.

**Every role denies delete on the protected prefixes.** Query's one delete
grant covers only its own bucket-probe scratch objects under
`sys/pq-probe/*`, Gateway's one delete grant covers only its dead processes'
admission snapshots, and Admin's two cover only the scratch prefixes
`sys/qualify/*` and `sys/pq-probe/*`. All three still carry the same explicit `Deny` on
`s3:DeleteObject` and `s3:DeleteObjectVersion` over the protected control
prefixes. An explicit `Deny` overrides any `Allow`, so those prefixes are
undeletable even by Maintain.

**The audit prefix has two shards that are treated differently.** The legal-hold
shard (`t/*/u/*/0000/*`) is deny-delete for every role including Maintain, so a
legal hold cannot be destroyed. The query-audit shard (`t/*/u/*/0001/*`) is
compacted and age-swept on a 90-day window by the Maintain process, so Maintain
alone grants delete on it. The two shard paths are disjoint, but Maintain's
level-based delete grants are not confined to them: an audit object is keyed
`t/<hash>/u/<level>/<shard>/...`, so `t/*/*/l0/*`, `t/*/*/c/*` and
`t/*/*/l1/*` match legal-hold keys too. What keeps a legal hold safe is the
explicit `Deny`, which names both `s3:DeleteObject` and
`s3:DeleteObjectVersion` on that shard, and a `Deny` overrides an `Allow` only
for the actions it names. If you edit these policies, keep the deny's action
list at least as wide as every delete action an `Allow` grants on those keys.

**Tenant discovery needs a bare prefix entry.** Discovery lists the bare,
delimited `t/` prefix rather than a per-tenant subpath, and under AWS
`StringLike` none of the `t/*/...` wildcards match the literal string `t/`.
Every role that performs discovery (Gateway, Query and Maintain) therefore needs
a separate `t/` entry in its `ListBucket` condition alongside the per-key
wildcards. This does not widen what those roles can read: listing a prefix
enumerates keys, it does not grant `GetObject` on them.

One more note for anyone reading the policies: create-if-absent, compare-and-set
and plain overwrite are all `s3:PutObject` at the policy layer. The difference
between them is a request precondition header, not a separate action. So a
role's write grant is a `PutObject` allow on its write prefixes, and the
create-only and compare-and-set semantics are enforced by Ravel's own request.
The key-layout the policies reference is documented normatively in
[the catalog and MVCC contract](../../catalog-and-mvcc.md).

### Subject-erasure grants

Selective subject erasure adds one object prefix, `t/<hash>/<sig>/del/`, holding
an erasure request (`<request_id>.dreq`, which contains the subject identifier)
and its completion marker (`<request_id>.done`, which does not). The rewrite
pass and physical sweep that erasure drives touch only prefixes Maintain already
has, so only the new prefix needs grants:

- Admin creates the request and deletes nothing.
- Query and Maintain read the prefix, to attach pending predicates at resolve
  time and to scope the rewrite pass.
- Maintain deletes the request only, after its completion marker exists and the
  protection horizon passes.
- No role, Maintain included, may delete a completion marker.

Add each statement to the same policy file as the rest of that role's grants.
The request and completion suffixes are disjoint key paths, so the Maintain
delete allow and the completion deny never overlap.

```json
{
  "Sid": "AdminErasureSubmit",
  "Effect": "Allow",
  "Action": "s3:PutObject",
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*"
}
```

```json
{
  "Sid": "ErasureRead",
  "Effect": "Allow",
  "Action": "s3:GetObject",
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*"
}
```

```json
{
  "Sid": "MaintainErasureDeleteRequest",
  "Effect": "Allow",
  "Action": "s3:DeleteObject",
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*.dreq"
}
```

```json
{
  "Sid": "DenyDeleteErasureCompletion",
  "Effect": "Deny",
  "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*.done"
}
```

Add `t/*/*/del/*` to the Query and Maintain `ListBucket` prefix conditions as
well, and add the completion deny to all four policy documents.

### S3-compatible stores

The four documents under `deploy/iam/` are ordinary S3 policy JSON: the same
actions, the same `arn:aws:s3:::<bucket>/<prefix>` resources, the same explicit
`Deny` semantics. A store that exposes an S3-compatible policy layer takes them
unchanged; load them with that store's own administrative tooling, and attach
one credential per role.

The local development and CI object store here is RustFS, provisioned with a
single shared credential across every process, deliberately: the per-role split
is a production hardening, and neither environment needs it. Ravel does not
depend on any store-specific admin API, so nothing in this repository drives
one.

## Encrypting objects with SSE-KMS

Two independent flags, both off by default.

`--s3-kms-key <arn>` encrypts every PUT the process makes with one key. There is
no routing and no new object: the single store every deployment already builds
is constructed with that key id.

`--tenant-kms-config <path>` names a TOML file of per-tenant keys. Only this
flag inserts the routing decorator into the store chain. It routes writes for a
configured tenant's keyspace to a lazily built store constructed with that
tenant's own key; every other tenant, and every read, falls through to the
default store unchanged. It requires `--store s3` and refuses to start under
`--store memory`.

```toml
# --tenant-kms-config kms-tenants.toml
[tenants]
acme = "arn:aws:kms:us-east-1:111122223333:key/acme-key"
other = "arn:aws:kms:us-east-1:111122223333:key/other-key"
```

The first time a tenant's key is configured, and on every later rotation to a
different key, startup bootstraps that tenant's key-epoch history at
`t/<hash>/enc`. Epoch 0 records an empty key (the deployment-default
convention) with an activation time at the start of Unix time, which is at or
before any tenant's earliest live object, so the custody check never meets an
object that predates epoch 0. Epoch 1 follows immediately with the real key and
the activation time of the moment of configuration. A restart with the same key
is a no-op; a restart with a different key appends a rotation epoch. The epoch
record is written before routing is switched to the new key, so a crash between
the two can never leave data flowing through a key with no epoch record.

**Both halves of the grant are required.** The key policy grants usage to the
principal, and the principal's own policy must allow the action, or the request
is denied before it reaches the key policy at all. Without both, the first
encrypted PUT a role makes for a configured tenant fails closed with
`AccessDenied`: once a tenant is named in the file, its writes route through
that key unconditionally and there is no fallback to the default key.

A minimal per-tenant key policy, scoped to the roles that deployment actually
runs. Every principal added here widens the blast radius the key policy exists
to narrow.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RavelRolesMayUseThisKey",
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::111122223333:role/ravel-gateway",
          "arn:aws:iam::111122223333:role/ravel-query",
          "arn:aws:iam::111122223333:role/ravel-maintain"
        ]
      },
      "Action": ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*"],
      "Resource": "*"
    },
    {
      "Sid": "KeyAdministration",
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::111122223333:role/ravel-admin" },
      "Action": ["kms:*"],
      "Resource": "*"
    }
  ]
}
```

The matching role-side statement in each `deploy/iam/*.json` template holds a
placeholder tenant key ARN that you must replace with your own (see
[the shipped policy documents](#the-shipped-policy-documents)), scoped to real
keys rather than to every key. Replace it with the exact ARN configured with
`--s3-kms-key`, if the deployment sets that flag, plus one exact ARN for every
key in the `--tenant-kms-config` file. The two flags are independent: the
`--s3-kms-key` ARN covers every PUT that no per-tenant key overrides, and its
absence from a role's array fails that role's PUTs, and every role's reads of
objects written under it, with `AccessDenied`. With the keys in place:

- Gateway and Maintain write tenant data through the routing store and read some
  of what they write, so they hold encrypt, generate-data-key and decrypt.
- Query reads tenant data and writes routed objects under `t/<hash>/`: the
  catalog snapshot, `HEAD` and index objects its fold publishes, query-audit
  records under `u/`, the `enc` key-epoch record, and the alert evaluator's
  lease, state memo and transition objects. It holds encrypt,
  generate-data-key and decrypt.
- Admin holds decrypt only, deliberately without generate-data-key: granting it
  would let a leaked Admin credential mint ciphertext under tenant keys it has
  no write role for.

`ravel-cli` separates tenant data from control records. The commands that
write tenant data under the Maintain credential take the same
`--tenant-kms-config` flag, read the same file, and route the same way the
server does: `maintain compact-bucket`, `maintain compact-tenant` and
`maintain migrate` (L1 segments and compaction records, and for `migrate` its
cursor and the floor raise in `prov`) and `catalog fold` (catalog snapshot
parts, `HEAD` and index objects). Pass them the file the servers use. For a
tenant the file names, the command first reads that tenant's `t/<hash>/enc`
key-epoch record. When its current key is the file's the command leaves it
alone. When the record is absent, or its current key differs, the command
refuses before any write, because only server startup records a configured or
changed key and the record is append-only: start the server with the file
first, then run the command. No `ravel-cli` command creates the record; the
one write a command makes to it completes a record holding only the bootstrap
epoch 0, which a server began and did not finish. The command then writes its
data under `t/<hash>/` under the tenant's key. The epoch record itself is a
control record, written under the bucket's default encryption. The command
applies only the entry for its own `--tenant`. A tenant the file does not
name is written under the bucket's default encryption, as the server writes
it. The flag requires `--store s3`. A `--dry-run` validates the file, reads
the key-epoch record and refuses as the real run would, prints the same
routing line, and writes nothing. Maintain already holds encrypt and generate-data-key on the tenant
keys and the `t/*/enc` write, so this needs no new grant.

Admin stays decrypt-only, so no Admin command takes the flag, and what Admin
writes under `t/<hash>/` stays under the bucket's default encryption whatever
the file says: provisioning records, legal holds, reconstructed commit
records, erasure requests, the tenant config record and the Parquet location
grants record. These are control records, not tenant data. Admin's missing
generate-data-key refuses none of these writes unless the bucket's default
encryption is itself a customer-managed KMS key, in which case Admin needs
generate-data-key on that key. `maintain verify-custody` checks write times
against the key-epoch history, not the key each object is encrypted under, so
it reports none of these records; for a tenant with a recorded epoch it prints
a `control records:` line saying so.

Two other `ravel-cli` writers under `t/<hash>/` take no `--tenant-kms-config`
and write under the bucket's default encryption. `maintain sweep` runs under
the Maintain credential and writes only unnamed-since markers there; the
quarantine copies it writes are outside `t/` and so would not route in the
server either. `load`, the bulk loader, writes L0 segments, their commit
records and, through `validate_or_adopt`, the tenant's provisioning record; a
deployment that needs bulk-loaded data under a tenant's own key
cannot get that from `ravel-cli` today.
The `t/<hash>/enc` epoch record has its own grant: Gateway, Query and Maintain
read and write it, because startup bootstraps it in every mode, and Admin reads
it for `verify-custody`. Every template denies its deletion.

Bytes written to the local read cache are not covered by any of this. See
[read cache tiers](#read-cache-tiers).

## Admission limits

`--limits-file` names a TOML file with a `[defaults]` table and zero or more
`[tenants.<id>]` override tables. Every field is optional and independently
overridable: a tenant table only needs the fields that differ from
`[defaults]`, which only needs the fields that differ from the shipped defaults.

| Field | Meaning |
|---|---|
| `max_active_series` | Cap on concurrently active metric series for the tenant. |
| `max_active_streams` | Cap on concurrently active log streams for the tenant. |
| `ingest_bytes_per_sec` / `ingest_byte_burst` | Token-bucket rate and burst for ingested bytes. |
| `series_creation_rate_per_sec` / `series_creation_burst` | Token-bucket rate and burst for new series and stream creation. |

Any of the four count or rate fields, but not the two burst-only fields, accepts
the literal string `"unlimited"` in place of a number, to opt a tenant out of
that cap. With no `--limits-file`, every tenant gets the shipped defaults.

Validation is fail-closed. The process refuses to start, rather than quietly
keeping the shipped defaults, on a file that is not valid TOML, an unknown key
in any table, an empty tenant id, a count or rate of zero or below, a burst set
without a rate to pair with, or a burst set alongside `unlimited` for the same
rate.

### Shipped defaults, and what they cost in memory

```
max_active_series            = 200000
max_active_streams           = 200000
ingest_bytes_per_sec         = 33554432   (32 MiB/s)
ingest_byte_burst            = 67108864   (64 MiB)
series_creation_rate_per_sec = 10000
series_creation_burst        = 100000
```

The two active-count caps are the ones to size deliberately, because each
tracked identity costs resident memory. Measured entry cost, including hash-table
slot overhead, power-of-two table sizing at 7/8 load and allocator headroom, is
35 to 56 bytes, not the roughly 16 bytes a naive estimate gives. The admission
controller tracks active series and active streams in a two-epoch rotating set,
so both epochs can be live at once:

```
cap x bytes_per_entry x 2 epochs x 2 signals (series + streams)
```

At the shipped 200,000 caps that is 27 to 43 MiB per fully active tenant, so ten
simultaneously fully active tenants cost 267 to 427 MiB in the worst case. At a
1,000,000 cap the same arithmetic gives 134 to 214 MiB per tenant, and 1.3 to
2.1 GiB for ten. Raise a tenant's ceiling explicitly in its own table when it
needs one, sized against this formula.

### Transient decompression memory

Accepting gzip on OTLP over HTTP adds a second, transient memory demand that the
ingest buffer budget does not account for. A gzip request is decompressed into a
fresh buffer bounded by the 64 MiB decompressed cap, held only while the request
holds an ingest concurrency permit:

```
max_inflight_ingest_requests x 64 MiB
```

At the default 1024 permits that is a 64 GiB worst case, far past what a small
host has. Size `--max-inflight-ingest-requests` down so this product fits the
headroom you have alongside the ingest buffer budget and the active-identity
memory above. The three are additive and none bounds the others. The gRPC path
is bounded at 16 MiB per in-flight request instead, so the same arithmetic
applies with a 16 MiB factor.

## Read cache tiers

The read cache has a RAM tier, on unless `--disable-cache` is set or its
ceiling resolves to `0` (what a gateway resolves when no ceiling flag is
set), and an opt-in
local-disk tier. `--cache-dir <path>` attaches the disk tier at that
directory to both the query fetcher cache and the catalog byte cache, so a RAM
eviction is served from local disk instead of paying the object-store round trip
again:

```sh
ravel-server --store s3 --s3-bucket my-bucket --cache-dir /var/cache/ravel
```

There is no separate capacity flag for the disk tier. Each tier is bounded by
that cache's own resolved RAM ceiling, read once at startup with no live
resize.

The fetcher cache and the catalog byte cache are two independent LRU caches
with their own flags: `--cache-max-bytes` bounds the fetcher cache only, and
`--catalog-cache-max-bytes` bounds the catalog byte cache only. Unset, the two
derive separately from the process memory budget (see "Per-query budgets"
below): the fetcher cache at 25% of it, or a larger 40% against a loopback
`--s3-endpoint`, and the catalog byte cache always at a smaller 5%
(`7516192768` and `1503238553` on the 30 GiB reference host at the 25% share).
Startup refuses to start, rather than silently clamping, if the two resolved
hard caps together reach or exceed the process memory budget (never in
`--mode gateway`, which derives no budget). Both ceilings are LRU
caps, not reservations: neither pre-allocates, each holds only the bytes it
has admitted, and the sum of the two cache ceilings and the SQL memory pools
(which derive from raw host memory, not the process memory budget) may
exceed physical RAM by design (the caches fill only under a working set that
large, and a SQL query aborts rather than growing past its own pool).
`--disable-cache` turns both off and holds no read-cache memory.

The disk tier is disposable by design. The directory is created lazily on first
admission and is never required to exist. A missing, full or corrupt cache
directory degrades to a store read, never to a query error, so a node whose
cache directory is deleted while it is running answers every query correctly and
only more slowly.

**Cache bytes are not encrypted by SSE-KMS.** Server-side encryption protects
object bytes at rest in the store, not the bytes this process writes to
`--cache-dir`. An operator who needs encryption at rest for the cache directory
provides it at the filesystem or volume layer, for example by mounting an
encrypted volume there.

Once a disk tier is configured, each cache's counters gain a tier label
alongside the existing cache label, so RAM and disk hit rates are reported
separately. With no `--cache-dir` no tier label appears at all. See
[the caching guide](../caching.md) for the full metric list and sizing advice.

## Retention and garbage-collection configuration

These values govern when a deleted object's bytes actually go away, and they must
agree with each other or a reader can lose a segment out from under it. The
governing inequalities are:

```
protection_horizon >= max_query_duration + grace + clock_skew_allowance
protection_horizon >= max_compaction_lifetime + 4 * clock_skew_allowance
```

`max_compaction_lifetime` is compiled in (1h). The second bound keeps a late
compaction or erasure-rewrite run from changing which inputs a sweep may delete
after their horizon has passed.

The first three values are recorded once, deployment-wide, in a durable
`sys/gc` object at the bucket root, and every mode validates itself against it
at startup. That is what stops three independently deployed process
configurations from drifting apart with nothing checking the constraint. The
`clock_skew_allowance` term is not stored in `sys/gc`: it is an input to the
check, taken from `gc-config set`'s own `--clock-skew-allowance` at write time
and from the running sweeper's allowance (default 5m) at maintain startup, so a
horizon that does not cover the sweeper's clock skew can neither be written nor
run against. [Deletion and garbage collection](../../deletion-and-gc.md) has the
argument.

**Bootstrap never blocks a fresh deployment under a credential that may
create the object.** The first such process to touch a fresh bucket writes
`sys/gc` from the maintain defaults, which satisfy the
constraint by construction, then validates against the object it just wrote. If
several processes start together against one empty bucket, one wins the create
and the others read and validate against the winner's object. Under the
per-role storage credentials only the Maintain and Admin roles may create the
object, so on a fresh bucket start the `maintain` process first, or create it
with `ravel-cli gc-config set` under Admin; see
[the first deployment](deployment.md#the-first-deployment-against-a-fresh-bucket).
With per-role credential Secrets the Kubernetes operator applies the maintain
Deployment first on a fresh cluster and holds the gateway and query
Deployments until maintain reports a ready replica, so no manual step is
needed; with `maintain.enabled: false` it still applies both, their pods
restart until `sys/gc` exists, and the cluster reports `Degraded=True` until
you create the object under Admin. The Kubernetes guide describes the
conditions the operator records while it waits.

**What each mode validates:**

- `maintain`: its configured protection horizon and grace must **equal** the
  stored values. They are must-match, not independent knobs. A flag value that
  satisfies the inequality but differs from the durable value still refuses to
  start.
- query-serving modes (`query`, `all`): the engine deadline must be less than or
  equal to the stored `max_query_duration`, and the HEAD cache TTL the catalog
  runs on must be less than or equal to the stored `head_cache_ttl`. A
  format version 1 `sys/gc` records no `head_cache_ttl`, and the compiled
  default (30 s) applies; `gc-config set --head-cache-ttl` writes format
  version 2, which records one. The
  [maintenance guide](maintenance.md#upgrading-sysgc-to-format-version-2) gives
  the upgrade order.
- Flight SQL, in a build that has it: the ticket time-to-live ceiling must be
  less than or equal to `protection_horizon - grace`. The server reads that
  ceiling from `sys/gc` rather than a compiled-in default, so it tracks the
  durable authority automatically.

### The flags, and the order to change them in

Each knob has a `ravel-server` flag, a humantime duration defaulting to its
shipped value:

- `--gc-protection-horizon` and `--gc-grace` feed the maintain compactor and
  must **equal** the durable values. Set them to whatever the last
  `gc-config set` wrote.
- `--gc-max-query-duration` sets the enforced deadline for every query engine
  the process builds, and must stay at or below the durable
  `max_query_duration` (default 1h). A value above it is rejected at startup,
  never clamped down.
- `--gc-max-flush-lifetime` sets the compactor's flush lifetime, which is the
  seal margin, the orphan age gate, and the retention floor. It is not part of
  the must-match set, but it has its own floor: the process refuses to start,
  and `gc-config set` refuses to write, a value below the ingest pipeline's
  own compiled-in `max_flush_lifetime` (fixed at 1h; there is no flag to
  change it). A lower value would call a bucket sealed before a real writer's
  flush interlock has actually elapsed, letting the erasure completion gate
  report a pending erasure request complete while a flush that can still
  publish into that bucket is in flight.

Each flag feeds both the startup validation and the real compactor or query
engine, so a value that passes validation is the value actually enforced. The
practical consequence of the must-match rule: changing a horizon is not a
rolling config change. Change the durable object first, then bring every
process's flags into line, and expect a process started against the old value to
refuse rather than to run with it.

```sh
ravel-cli gc-config show
ravel-cli gc-config set --protection-horizon 25h5m --grace 24h \
  --max-query-duration 1h --max-flush-lifetime 1h
```

`gc-config set` is the single mutation path. It enforces the inequality at write
time, refusing a violating proposal without writing anything, and swaps the
object with a compare-and-set so a concurrent `gc-config set` is a reported
conflict rather than a silent overwrite. Every value must be strictly positive:
an all-zero configuration would satisfy the inequality trivially and be
impossible for any mode to match, so it is rejected.

The Kubernetes operator carries a `spec.gc` block with `protectionHorizon` and
`grace` for exactly this case. On a fresh bucket with a shared credential the
first pod bootstraps `sys/gc` from the shipped defaults and every pod validates
trivially; with per-role credential Secrets the operator applies the maintain
Deployment first and its pod creates the object, as above. On a bucket whose
stored protection horizon or grace was set to a non-default value with
`gc-config set`, set `spec.gc.protectionHorizon` and `spec.gc.grace` to the
stored values, which you read with `ravel-cli gc-config show`, and the operator
renders `--gc-protection-horizon` and `--gc-grace` onto the maintain pods so
they satisfy the must-match rule and start. Leave the block, or either field,
unset to keep the shipped default; the other two stored values do not affect
startup.

### Age-based retention

Retention is a separate concept from the GC safety horizons above, and it is off
by default. `--retention-default <duration>` sets the window applied to every
tenant with no override, and `--retention-tenant TENANT=DURATION` overrides it
per tenant. Both take a humantime duration (`30d`, `720h`). Omitting both means
nothing is age-deleted at all.

A window is validated at startup against a floor of
`max_ingest_lag + max_flush_lifetime + clock_skew_allowance` plus one bucket
span, so a bucket can never be tombstoned before it is sealed. A window below
the floor fails startup rather than being clamped up to it.

Both retention flags are read only in `--mode maintain`. Setting them on a
process that runs no maintenance loop configures nothing.

Query-audit records have their own window, independent of tenant data
retention. `--audit-retention <duration>` sets the age past which the
maintenance loop deletes a query-audit record, measured from the newest event
the record logs; the default is `90d`. Set it to your audit retention
obligation. `0` keeps every query-audit record forever. Any nonzero window is
accepted: every flush writes its own immutable record, and the sweep deletes a
record only once every event in it is older than the window, so a short window
never deletes an event younger than itself. A record is also kept until it is
past the protection horizon, so a window shorter than the horizon behaves as
the horizon. A legal hold covering the query-audit shard blocks the delete
whatever the window. Like the tenant retention flags, it takes effect only in
`--mode maintain`, but an unparseable value fails startup in every mode.

## Tenancy setup

Repeated `--tenant-token TOKEN=TENANT` flags configure tenants entirely. There is
no tenant database and no admin API. To add, remove or rotate a token, restart
with a different flag set. That is safe: every process is stateless, so a
restart has no data migration to do. With no `--tenant-token`, no
`--tenant-token-file`, and no OIDC or mTLS resolver configured, every request
to a tenant-protected route is rejected; the health and `/metrics` routes
carry no tenant, and `--dev-insecure-tenant-header` on a loopback listener is
the development exception.

`--tenant-token-file PATH` (env `RAVEL_TENANT_TOKEN_FILE` for the path only,
never a token value) is a file-based alternative to repeating `--tenant-token`,
so a token never has to sit in argv or a process listing: one `TOKEN=TENANT`
pair per line, blank lines and `#` comments skipped, each line split on the
first `=` the same way `--tenant-token` is. A leading UTF-8 byte order mark is
stripped before parsing. `--tenant-token` and `--tenant-token-file` are
mutually exclusive; startup refuses if both are set. An empty or
comment-only file parses to an empty map, the same as passing no
`--tenant-token` at all: that authenticates nothing, and unless
`--maintain-tenant` names tenants, background fold, compaction and retention
widen to every tenant storage discovers rather than refusing startup. A Secret
mount that failed to populate produces exactly this, with no error at startup.

Tenant identity affects only key prefixing and authorization. It carries no
other per-tenant configuration.

A `TENANT` ending in `;ddl` (the tenant is the text before the LAST `;`)
grants that token the `ddl` capability: absent by default, and the only thing
that reads it is `CREATE EXTERNAL TABLE` and `DROP TABLE` over
`POST /api/v1/sql`. Any other suffix, or an
empty tenant before the `;`, refuses startup naming the flag position or the
token file's line number, never the pair's text. A tenant with no `;` is
unchanged and never carries the capability. See [Background](#background)
for the decision behind this.

`ravel-ingest-router` accepts the same `--tenant-token` spelling and strips the
`;ddl` suffix, so it routes `acme;ddl` by the tenant `acme`; it never grants
the capability. Tokens in the durable `sys/auth` map cannot carry `ddl`: the
map is read without suffix parsing, so an entry written as `acme;ddl` names a
tenant literally called `acme;ddl`, with no capability and no error. Grant
`ddl` only through `--tenant-token`, `--tenant-token-file` or
`--oidc-ddl-claim`.

### Production authentication

Two additive resolvers join the same first-success chain. Enabling them does not
disable the bearer resolver, which stays the local and development path.

**OIDC.** Set `--oidc-issuer` and `--oidc-jwks-url` together; setting one
without the other refuses to start. At least one `--oidc-audience` is also
required, and OIDC with none set fails startup: without an audience, any
correctly signed unexpired token from that issuer authenticates regardless of
which relying party it was minted for. Every request's bearer token is verified
against the issuer's key set: signature, issuer, expiry and audience. The
signature algorithm is pinned from the key that
verifies the token, never from the token's own header, so `alg: none` and
algorithm-confusion tokens are rejected. A symmetric key in the key set is
rejected outright, because a key set is a public document and a symmetric key
inside one is a published verification secret. The tenant is read from
`--oidc-tenant-claim` (default `tenant`) as a string, with no fallback to any
other claim. `--oidc-ddl-claim <CLAIM>` names a second, optional claim that
grants the same `ddl` capability the `;ddl` tenant-token suffix grants: the
capability is present only when the verified token carries that claim as
the JSON boolean `true`, never for a string, a number, an array, or a
missing claim. Unset (the default), OIDC never grants the capability. The
key set is cached in memory and refreshed on
`--oidc-jwks-refresh-interval-secs`, so the request path never makes a network
call, and the fetch is bounded by a timeout so a stalled host cannot wedge the
refresh loop or the readiness gate. The first fetch must succeed before the
server reports ready. A plaintext `http://` key-set URL to a non-loopback host
is refused at startup: that response is the entire trust root for verification,
and fetching it in plaintext lets anyone on the path substitute their own keys.

**mTLS, forwarded by a proxy.** Ravel does not terminate TLS or verify client
certificates itself. `--mtls-enabled` reads a header (default
`x-ravel-client-cert-cn`, override with `--mtls-header`) that a TLS-terminating
reverse proxy sets to the already-verified certificate CN or SAN. This is a
forwarded-header trust boundary: it is authoritative only because a trusted hop
set it, and forgeable by anyone if that hop is absent.

The resolver is installed on its own dedicated listener and nowhere else, so
`--mtls-enabled` requires `--mtls-listener <addr>` and refuses to start without
it. The public HTTP and gRPC listeners never consult the header at all, and the
mTLS address must differ from every other listener address, which is checked at
startup. Put the verifying proxy in front of the mTLS listener only, and have it
strip or overwrite any client-supplied value of the header before forwarding.
Binding `--mtls-listener` to the same address as a `--listen-http` that has
`--dev-insecure-tenant-header` set is also refused, so the mTLS surface cannot
inherit the development bypass. Enabling mTLS logs a startup warning naming the
trusted header.

`--mtls-listener` must bind a loopback address unless
`--mtls-trust-forwarded-header` is also passed. Loopback is the one bind where
the topology itself proves that only a local proxy can supply the header; on any
other address, whether the header is trustworthy depends on a proxy Ravel cannot
see. The flag turns nothing on and grants the resolver no trust it did not
already have. It records that the operator chose the non-loopback bind
deliberately and has a verifying proxy in front of it.

This is a behavior change for an existing deployment: a proxy-fronted mTLS
listener bound to anything other than loopback (`0.0.0.0:9443`, a pod IP, a
host address) now fails startup with a message naming the address and the flag.
Add `--mtls-trust-forwarded-header` to the argument vector to keep it starting.
Nothing else about the deployment changes. A loopback-bound mTLS listener is
unaffected.

Dependent flags fail fast: `--oidc-tenant-claim`, `--oidc-ddl-claim`, or
`--oidc-audience` without OIDC enabled, `--mtls-header` or `--mtls-listener`
without `--mtls-enabled`,
`--mtls-enabled` without `--mtls-listener`, and `--mtls-trust-forwarded-header`
without `--mtls-listener`, all refuse to start rather than quietly doing
nothing.

### The tenant hash scheme is permanent per bucket

The object-key prefix for a tenant is a hash of the tenant id, pinned per bucket
at the bucket's birth by a `sys/tenancy` marker. One binary carries both
schemes and selects one at startup:

- **v1 unkeyed**: a plain hash of the tenant id. Tenant names are not in keys,
  but anyone with list access can confirm a guessed tenant id offline.
- **v2 keyed**, the default for new buckets: the prefix is keyed by a 32-byte
  deployment key loaded from `--tenant-hash-key-file`. It is a file, never an
  inline value, so the secret never appears in a process listing. Without the
  key, prefixes reveal nothing about which tenants exist.

Startup pinning:

- A fresh bucket refuses to start with no key unless `--tenant-hash-unkeyed` is
  passed explicitly. Keyed is the default and the choice is permanent.
- An existing keyed bucket refuses to start when the configured key's
  fingerprint disagrees with the marker. A wrong key is a failed deploy, not a
  silent parallel namespace. `ravel-cli tenancy show --tenant-hash-key-file
  <path>` verifies a key against a bucket offline.
- A bucket with data and no marker is adopted as v1 unkeyed once, logged and
  counted at `/metrics` as `ravel_tenancy_v1_unkeyed_adoptions_total`. Its
  existing prefixes are unchanged.

**Key custody.** For a keyed bucket the deployment key is durable state that
lives outside the object store, and losing it makes every tenant prefix
unattributable. Bucket plus key is always enough to recover the full mapping
from tenant id to prefix, through the per-tenant recovery manifests under
`sys/t/`; the bucket alone reveals nothing.

There is no migration between the two schemes. Moving a bucket between them
would relocate every object and is not built. A deployment that needs to change
schemes starts a new bucket and drains into it.

## Durable shard count

`--shards` is a default for tenants that have not yet been provisioned. It is
not a per-tenant setting you can change after the fact for existing data:
generation 0's shard count is fixed forever once a tenant's data for a signal
is written across it. The flag sets both the ingest router's shard count and
the query-side catalog's shard count for new tenants, which is why there is no
separate query-side flag.

The first write for a tenant and signal records its shard count as generation 0
of a durable, append-only shard-generation history in a provisioning record at
`t/<tenant_hash>/<signal>/prov`. Every later ingest, query, and maintenance
touch reads that history and routes each hour over the shard count active for
that hour, not over a single fixed count: `ravel-cli provision reshard` appends
a new generation with a different shard count, taking effect at a future
activation hour, without moving or re-keying existing data. Relying on
generation 0's count alone misses any later reshard; always route from the
persisted generation history.

An already-provisioned tenant keeps its own generation history: changing the
global `--shards` default (for example, lowering it for new tenants) does not
affect a tenant that already has a record, and does not refuse startup, fail its
queries, or skip its maintenance. This drift between a tenant's generation-0
recorded count and the live default is expected and is surfaced as an
informational metric, not an error.

The one case still refused is a record whose shard count would hide existing
data if adopted, and an unreadable (corrupt or future-format) record whose true
shard count cannot be trusted; both fail closed.

A brand-new tenant with no prior writes has no record yet, so a fresh
deployment, including an operator-managed cluster that starts with zero data and
configured tokens, starts normally. The record is created on the tenant's first
write and pins the live `--shards` default as that tenant's count.

**Adopting data written before the record existed.** A tenant and signal that
already had data is adopted the first time a server ingests or maintains it, or
deliberately ahead of a rollout:

```sh
ravel-cli provision adopt --tenant <name> --shards <n>
```

Adoption writes the record only when every observed shard index is below
`--shards`. If any observed index is at or above it, adoption refuses and writes
nothing, because that value is provably hiding data. Run `provision adopt` before
rolling out a version that enforces the record, so a refusal surfaces as a CLI
error you can act on rather than as a server that will not start mid-rollout.

## Logs fetch policy and store cost profile

The logs read path chooses, per object, whether to fetch the whole object in one
request or to fetch only the projected byte ranges. On an intra-region S3
deployment transfer is free and the bill is requests, so a ranged read spends a
billed request to save bytes that cost nothing. Elsewhere the reverse holds.
Three flags size this, all read at startup only.

`--logs-fetch-policy` takes one of four values, spelled exactly as here.
Unset, it resolves `cost-based` on every deployment, including a `--store s3`
deployment against a loopback `--s3-endpoint`.

| Value | Optimizes for | Pick it when |
|---|---|---|
| `request-minimal` | Fewest object-store requests. An object at or under the fetch bound is read whole in one covering request with no footer probe; a larger object is read as covering sub-range requests. | The backend bills requests and not transfer, so a saved request is a saved dollar and the bytes it costs are free. |
| `byte-minimal` | Fewest transferred bytes. Ranged reads wherever they save more bytes than a request is worth. | The backend bills egress, or the network is the constraint, so moved bytes are the cost that matters. |
| `cost-based` | Whichever of the two is cheaper under the active store cost profile, resolved at startup from the profile's prices and its measured request timings. | You want the shape the deployment's own prices and timings imply. At the reference intra-region profile, on every store including a loopback one, a request costs 6,300,000 bytes (its time term), so a projection that skips more than 31,500,000 bytes of an object reads ranged and every object of 31,500,000 bytes or less reads whole. |
| `latency-first` | Fewest transferred bytes, exactly like `byte-minimal`. An intent, not a tuning constant: it says spend requests to save wall time, and leaves how up to the concurrency you configure. | Cold wall-clock matters more than the request bill, and you are willing to raise the object-store GET concurrency and the SQL scan width explicitly to cash in the trade: measured over 3 reps on a 42-statement reference corpus, true cold in the warm-up-empty state, at GET concurrency 256: 5.30x the GET requests (570,752 against 107,781) for 52% less cold time, with a per-rep range of 50.3% to 54.2%. That ratio is a measurement of two code paths at one point in the project's history, not a property of the policy, and it has already moved once as the cost-based side changed; the decision record for the fetch objective names the exact build it was taken on. Re-measure against the build you run rather than treating it as a constant. |

For any policy value a query returns exactly the same rows. Only request counts
and timing differ.

`latency-first` resolves `--store-get-concurrency`, `--sql-partition-count`,
and `--promql-fetch-fanout` the same way every other policy does -- it sets no
default of its own. The measured trade above only pays off once you raise
the GET permits and the SQL scan width together to the concurrency the
measurement used; `--fetch-concurrency` raises all three at once. Selecting
the policy on its own is not inert: the byte quantities change immediately, so
a logs read is routed the way `byte-minimal` routes it, taking ranged reads
wherever they save more bytes than a request costs and whole-object reads
where they do not. On the reference corpus against real S3 that shape at the
default concurrency measured slower than the default policy, not faster. Treat the
concurrency as a precondition, not a suggestion. The startup line says which
side of it this process is on, and it reports the precondition met only when
both the GET permits and the scan width have been raised.
Raising concurrency also raises in-flight fetch memory, and that memory is not
yet bounded by a process-wide budget: watch process memory yourself when
trying this policy, since an under-provisioned raise can end in an
out-of-memory kill instead of a faster query.

The policy is an operator surface only. It is never derivable from query text, a
header or a ticket: under request billing, a tenant that could force
`byte-minimal` per query would multiply the deployment's request bill by the
measured amplification factor. The running engine also never changes its own
policy. If a measurement shows the default is wrong for a deployment, set
`--logs-fetch-policy` explicitly.

The resolved policy's source -- `flag` (explicit) or `default` (unset) -- is
logged at startup alongside the policy itself on the `logs fetch policy
resolved` line. Unset always means `default`/`cost-based`, on every store
including a loopback one: an operator does not need to know which store this
process is against to know what an unset flag resolved to.

### The store cost profile

`--store-cost-profile <path>` names a TOML file of this deployment's
object-store prices and, optionally, two request timings measured from its
hosts. It is read only when resolving `cost-based`; no price ever
reaches the fetch layer, which runs on byte quantities alone. The same file is
read by `ravel-bench`, so the engine and the ledger price a run the same way.
Omitted, the reference profile `s3-intra-region-2026` is used.

```toml
name = "s3-intra-region-2026"
put_class_nanodollars = 5000          # PUT/COPY/POST/LIST class, per request
get_class_nanodollars = 400           # GET/SELECT/HEAD class, per request
delete_class_nanodollars = 0          # optional; DELETE class, per request
transfer_nanodollars_per_gib = 0      # egress, per GiB
retrieval_nanodollars_per_gib = 0     # per-GiB retrieval on classes that bill it
request_latency_micros = 70000        # optional; one request's latency
per_connection_throughput_bytes_per_s = 90000000  # optional; set with the latency
timings_measured = "measured on the reference box of the reference suite, intra-region against the object store"
```

Prices are integer nanodollars, never floats, because they are exact decimal
contract figures. The reference values model S3 standard intra-region 2026 list
prices: PUT class $5.00 per million requests, GET class $0.40 per million,
transfer and retrieval free. One PUT costs 12.5 GETs at those prices. Every
price is a modeled figure under a named profile, not a billed amount, and the
same run under a different profile reprices to different numbers.

The two timings are measured constants, not prices: the latency of one request
from the deployment's hosts and the bytes one connection transfers per second,
with `timings_measured` naming when and where they were measured. The reference
values were measured on the reference box of the reference suite,
intra-region against the object store. They are optional, and set together
or not at all.

Every field except `delete_class_nanodollars` and the three timing fields is
required. Loading is fail-closed: an unreadable file, invalid TOML, an unknown
or misspelled key, one timing without the other (the error names the missing
one), or a blank name refuses startup with an error naming the flag. There is no silent
fallback to the reference prices, because a deployment that named a profile and
got the reference prices instead would stamp one profile into its reports while
resolving its fetch policy from another.

**How `cost-based` resolves.** It converts the profile into the one byte
quantity the fetch layer runs on: how many transferred bytes one saved request
is worth. Two terms can answer that, and the larger one is the rate. The price
term is what a request costs in bytes at the profile's prices; the time term is
the bytes one connection could have moved during the request's latency.

```
price term = get_class_nanodollars x BYTES_PER_GIB
             / (transfer_nanodollars_per_gib + retrieval_nanodollars_per_gib)
time term  = request_latency_micros x per_connection_throughput_bytes_per_s / 1,000,000
request_cost_bytes = the larger of the two terms
```

`BYTES_PER_GIB` is 2^30, and the arithmetic multiplies before it divides in
128-bit so a sub-nanodollar per-byte price does not truncate to zero. Retrieval
is a per-byte charge exactly like transfer and enters the denominator the same
way, so a profile with free transfer but priced retrieval still routes
byte-minimally rather than reporting retrieval dollars a request-minimal plan
would never have spent. The result is floor-rounded, held at a minimum of one
byte, and clamped to the coalescing-gap and routing-threshold floors. Two cases
saturate the price term: a zero denominator, where no per-byte cost exists, and
quotient overflow from a near-free but nonzero per-byte price. A saturated
price term yields to the time term, so the rate itself saturates, meaning "read
whole always", only on a profile with neither per-byte prices nor timings; that
is logged at startup naming the profile. The startup line's `rate_term` field
says which term the rate came from: `price`, `time` or `saturated` (or `flag`
when `--logs-request-cost-bytes` set it).

At the reference profile both per-byte prices are zero, so the price term
saturates and the rate is the time term: 70,000 microseconds at 90,000,000
bytes per second, 6,300,000 bytes. At egress list prices (GET class $0.40 per
million against $0.09 per GiB transfer plus $0.01 per GiB retrieval) and no
timings it resolves to 4,294 bytes, which the floors then clamp.

Under `cost-based`, and only there, a finite rate derived from the profile also
sets the projection break-even: the bytes a narrow projection must save before it is read ranged
instead of whole, the larger of the routing threshold
(`--logs-block-range-threshold`, 524,288 bytes by default) and five request
costs. At the reference profile that is 31,500,000 bytes, so a one-column read
of a 35 MB object reads its column ranges while every object of 31,500,000
bytes or less, such as a 3 MB flush object, still reads whole. The same figure
is the object size at or below which the ranged fetch reads the whole object
anyway. The startup line reports the break-even in force as
`projection_break_even_bytes` with `break_even_source="profile"`; under the
other policies and when `--logs-request-cost-bytes` is set, which keep the
routing threshold as the break-even, it reports that threshold (524,288 bytes
by default) with `break_even_source="routing-threshold"`. The
coalescing gap, the largest hole between two wanted ranges that one request
reads through, stays one request cost (at least 64 KiB) under every policy, so
at the reference profile it is 6,300,000 bytes.

### The covering-read bound and flag precedence

`--logs-max-fetch-run-bytes` caps the length of one covering request. Its
default is 64 MiB, it applies under every policy, and zero is refused with an
error because the segmented fallback divides the object size by it. An object at
or under the bound is read in one covering request; an object above it is read
as sequential block-aligned covering sub-ranges, so no single request moves more
than the bound however large an object grows.

`--logs-request-cost-bytes`, when set explicitly, wins over the policy-derived
rate. The policy is the intent layer and this is the expert escape hatch, so a
deployment can select `cost-based` and still pin the one derived quantity when
it has measured a better value.

A saturated rate additionally overrides an explicitly set
`--logs-block-range-threshold`: `request-minimal`, and `cost-based` on a
profile with neither per-byte prices nor timings, saturate both routing
thresholds regardless of that flag, and a set-but-overridden threshold is
logged at startup so the override is visible. Otherwise that flag keeps its
normal role, including under `cost-based` at the reference profile.

### What this does not touch

The fetch policy and the cost profile govern the logs read path only. Metrics
fetching consults neither: its suffix probe window, coalescing gap, whole-object
threshold and concurrency limit are compiled-in constants. An operator tuning
metrics fetch behavior will not find a knob here, because there is none.

Any report carrying a request or modeled-cost figure stamps the active profile,
all its prices, and the resolved policy, split into what was requested and what
actually governed the run. A lane that cannot know what governed its fetches
stamps its effective value as `n/a` rather than echoing the request as if it
were confirmed. Two request or dollar figures are comparable only once both are
known to have priced the run the same way.

## Indexed fields and typed attribute columns

Two per-tenant declarations that change query cost, and in one case the SQL
schema. Both are day-0 decisions because changing them later means a restart or
a durable record write.

### Indexed fields

Block-level pruning for an attribute equality predicate on logs is driven by an
index over named fields. `--indexed-field FIELD`, repeated, names the fields for
every tenant with no override, and `--indexed-field-tenant TENANT=field1,field2`
replaces that list for one tenant. An empty list for a tenant
(`--indexed-field-tenant acme=`) turns the index off for it.

The shipped default list is `service.name`, `k8s.namespace.name` and
`http.status_code`. **Any value you pass replaces that list rather than adding
to it.** Indexing is opt-in per field, an unindexed field still works through
the bloom filter and the exact scan, and a missing index changes query cost, not
query correctness.

### Typed attribute columns

The `logs` SQL table exposes every attribute through one merged
`attrs: Map(Utf8, Utf8)` column, so a numeric or boolean comparison over an
attribute is a cast over a stringified value. Declaring an attribute key
promotes it to a native typed column, appended after `attrs` in declaration
order, and the same value then reads back as a real `Int64`, `Boolean`,
`Dictionary(Int32, Utf8)` or `Binary` Arrow column.

A promoted `str` column is dictionary-encoded and stays a dictionary over the
Flight SQL wire. HTTP JSON row values are unchanged, one string per row, but the
JSON envelope's column type reads `Dictionary(Int32, Utf8)` rather than
`Utf8`, and the Arrow IPC schema and batch columns carry the dictionary type
verbatim. Both are client-visible changes a consumer must expect. The key still
appears in `attrs` as well, so `SELECT attrs` and `SELECT *` keep working.

There are two ways to declare, with one resolution order:

- `--typed-attr-column KEY:TYPE` and `--typed-attr-column-tenant TENANT:KEY:TYPE`
  are the deployment default and its per-tenant override. Changing them is a
  restart. `TYPE` is one of `str`, `i64`, `bool` or `bytes`, case-insensitive.
  There is no shipped default, because a promotion changes the SQL schema a
  tenant's queries see.
- The durable per-tenant record, written by `ravel-cli typed-attr-column set`,
  is the no-restart path. When present it replaces the flag-derived declaration
  for that tenant outright, **including when it is present and empty**. An empty
  declaration means "this tenant promotes nothing", which is a different state
  from having no override, in which case the flags apply.

```sh
ravel-cli typed-attr-column show <tenant>
ravel-cli typed-attr-column set <tenant> http.status_code:i64 user.id:str
```

`set` replaces the tenant's declaration wholesale. It is not additive and there
is no per-key remove, so pass the full intended list. It validates on the same
rules the flags do (an empty key, a duplicate key, the same key with two types,
or a key colliding with one of the nine fixed logs columns `ts`, `observed_ts`,
`severity_num`, `severity_text`, `body`, `trace_id`, `span_id`, `flags`,
`attrs`), then swaps the record with a compare-and-set so a concurrent write is
a reported conflict rather than a silent overwrite.

**Staleness.** A query-serving process reads the durable override per tenant on
a 60-second staleness horizon, so a `set` takes effect within 60 seconds and
during that window two replicas can answer the same query against different
declarations. A failed read never fails a query: the process serves the last
declaration it resolved, or the flag-derived one if it never resolved for that
tenant, and a failed read is not retried for one second, so a degraded config
store costs at most one failed request per tenant per second. That fallback is a
real degradation, so it is counted rather than silent, in
`ravel_typed_attr_columns_stale_fallback_total`.

**Cost note.** A predicate on a promoted column prunes blocks before decode: an
`i64` or `bool` comparison through the skip index, and a `str` or `bytes`
equality through the same POSTINGS index that `attrs['k'] = 'v'` uses. Promote
for typed comparisons and aggregates (`k > 5`, `SUM(k)`), which are impossible
over the map; an equality that already prunes gains nothing from promotion.

There is also a per-object budget on how many distinct attribute name and type
pairs get a real column at write time. Pairs beyond the budget fold into an
overflow column and lose columnar access. Watch for that in
[the observability guide](../observability.md).

## Per-query budgets

Six flags bound what one query may spend. Unset, each resolves at startup, but
only some resolve from host resources: `--store-get-concurrency`,
`--sql-partition-count`, and `--promql-fetch-fanout` (or the legacy
`--fetch-concurrency`, which sets all three) follow the core count, and the two
SQL ceilings follow memory (shares of `MemTotal`, capped by the cgroup memory
limit when the process runs in a container), while `--max-segments` is a fixed
1,000,000 on every host. Set, the flag value is used verbatim, with one
reconciliation: the per-query SQL pool is clamped to an explicit per-tenant
ceiling set below it, and the startup log says so. The reference-host column
is a 16-core, 30 GB host, the shape the published ClickBench run used.

| Flag | Default (unset) | Reference host | Choose against |
|---|---|---|---|
| `--fetch-concurrency` | derived: `max(8, 2 x cores)` | 32 | Legacy combined knob: sets `--store-get-concurrency`, `--sql-partition-count`, and `--promql-fetch-fanout` together (source `legacy-flag`). Combining it with any of the three is a startup error naming both flags. |
| `--store-get-concurrency` | derived: `max(8, 2 x cores)` | 32 | Permit count for the one process-wide `GetLimiter` every fetcher (RSEG, RLOG, RSPAN) shares. Host cores and the store's request budget. |
| `--sql-partition-count` | derived: `max(8, 2 x cores)` | 32 | DataFusion `target_partitions` for every SQL session the server builds. Host cores and query parallelism vs. per-partition overhead. |
| `--promql-fetch-fanout` | derived: `max(8, 2 x cores)` | 32 | PromQL/analytics per-query segment fetch fan-out. Host cores and the store's request budget. |
| `--max-segments` | fixed: 1,000,000 (host-independent) | 1,000,000 | How many sealed objects a wide scan touches. Only the recent set, roughly the last two hours, is exempt, so a tenant with a lot of sealed history hits this before you expect. Lower it to bound plan width on a host you share with something else. |
| `--sql-max-query-bytes` | derived: 50% of MemTotal, 256 MiB if memory is unknown | 16,106,127,360 | Per-query SQL memory pool ceiling. Process-wide, not per-tenant. The derived value equals the tenant's whole SQL share, so a lone statement may use all of it; concurrent statements still share the per-tenant ceiling. To keep the earlier split, set this flag to half of `--sql-tenant-max-bytes`, 25% of MemTotal. Held at or below `--sql-tenant-max-bytes`: an explicit value here raises a non-explicit (derived or fallback) tenant ceiling to fit, but an explicit tenant ceiling clamps this down and warns. |
| `--sql-tenant-max-bytes` | derived: 50% of MemTotal, 1 GiB if memory is unknown | 16,106,127,360 | The multi-tenant isolation bound: SQL memory one tenant may hold across its concurrent queries. Process-wide, and not itself per-tenant-overridable. |

The two SQL ceilings derive to the same 50% share of `MemTotal`. The per-query
pool nests inside the per-tenant pool, so the tenant's total is unchanged by the
per-query share: statements running together share the tenant ceiling, and a
statement that arrives while another holds most of it gets what is left, not a
reserved quarter. One tenant's SQL memory is therefore still at most 50% of
`MemTotal`. The two caches carve the memory budget (`MemTotal` less the 2 GiB
reserve) rather than `MemTotal`, so the three ceilings together come to about
78% of `MemTotal` on the reference host (25,125,558,681 of 32,212,254,720),
and more on a loopback store, where the fetcher cache derives at 40%.

A value of `0` in any of `--fetch-concurrency`, `--store-get-concurrency`,
`--sql-partition-count`, or `--promql-fetch-fanout` is a startup error naming
that flag, raised before any fetcher, engine, or SQL session exists.

`--catalog-resolve-concurrency` derives from the same startup resolution, but
from query concurrency rather than from cores or memory directly, and it
bounds the process rather than one query. It is the ceiling on every
object-store request the catalog resolve path keeps in flight across every
concurrent query: prefix LISTs, commit-record GETs, snapshot-part GETs, and
the postings and column-stats reads that go with them. Unset, it resolves to
`clamp(Q * 128, 128, 4096)` held at an interim 1,024, where `Q` is
`--max-concurrent-queries` when that flag bounds queries and the same
`max(8, 2 x cores)` the flags above use when queries are unbounded. That flag
is the fleet-wide query ceiling, not a per-replica one, so with several
replicas each one sizes its resolve ceiling for the whole fleet's queries and
is correspondingly generous. 128 is
what one shard-hour prefix sustains, so `Q` concurrent resolves over `Q`
different shard-hours each get one prefix's worth. Worked examples:
`--max-concurrent-queries 1` resolves to 128, `--max-concurrent-queries 4` to
512, and an unbounded 8-core host to 1,024 (its `Q` of 16 derives 2,048, held
at the interim cap). Set explicitly, the flag value is used verbatim and the
interim cap does not apply to it; `0` and any value above 4,096 are startup
errors. A second bound the flag does not reach holds each individual key
prefix to 128 requests whatever this ceiling is. Every resolve-path request
is bounded this way, keyed by its own key prefix: a commit record by its
shard-hour prefix, a snapshot's parts by the one directory they share, its
postings and column stats by theirs, and a LIST by the prefix it lists. So
raising this ceiling adds breadth across prefixes and never depth within one.

Three more settings are derived the same way: `--cache-max-bytes` (fetcher
cache, 25% normally or 40% against a loopback `--s3-endpoint`),
`--catalog-cache-max-bytes` (catalog byte cache, always a separate 5%
ceiling; 256 MiB each if memory is unknown and `--memory-budget-bytes` is
unset) and `--gc-max-query-duration` (11
minutes). Each cache flag bounds only its own cache; setting one never
changes the other. Memory is read from `/proc/meminfo`'s
`MemTotal` on Linux and is "unknown" everywhere else; cores come from the
process's available parallelism, floored at 1. Percentages truncate.

Unlike the two SQL ceilings above, `--cache-max-bytes` does not derive from
raw `MemTotal`: it derives from a process-wide memory budget, which starts
from available memory rather than raw total whenever that is possible: with
no cgroup memory limit
and a readable `MemAvailable` (Linux's own `/proc/meminfo` estimate of memory
a new allocation could claim without swapping), the budget is
`min(MemTotal - RESERVE, max(FLOOR, MemAvailable + own RSS - RESERVE))`,
every subtraction saturating at zero. `RESERVE` is the same fixed 2 GiB
overhead reserve as before; `FLOOR` is a 1 GiB floor under the
`MemAvailable + own RSS - RESERVE` term only, not under the final budget:
binding it logs at `WARN` with the `MemAvailable` reading that hit it and
`--memory-budget-bytes` named as the remedy, but the outer `min` against
`MemTotal - RESERVE` keeps the final budget at or below the pre-amendment
figure, so a host whose `MemTotal` is at or below the reserve still derives
0 and is refused at startup, as before.
The process's own resident set counts as available because the kernel does
not call a process's own resident pages "available" even though this
process may reuse them rather than compete with them. A cgroup memory
limit, when present, keeps the pre-amendment rule instead: the limit is
already this process's whole share, so a whole-host `MemAvailable` would
only be wrong to consult, and the budget is that limit minus the reserve.
With no cgroup limit, a readable `MemTotal`, and no readable `MemAvailable`
(an unusual Linux kernel or container runtime whose `/proc/meminfo` parses
`MemTotal` but not `MemAvailable`), the budget is `MemTotal` minus the
reserve, same as before the amendment. A host with neither a readable
`MemTotal` nor a cgroup limit (every non-Linux build, or a Linux host with no
cgroup limit whose `/proc/meminfo` cannot be read) derives no budget at all:
the budget is unlimited, same as before this amendment existed. When
`/proc/meminfo` cannot be read but a cgroup limit is set, the limit is the
memory figure and the source is `derived-cgroup`. Set `--memory-budget-bytes` to
override every one of these branches outright; it still goes through the
same startup refusal as a derived budget (below). This is the one setting
to reach for on a host where this process shares memory with another one it
cannot see: the available-memory derivation reads `MemAvailable` once at
startup and cannot anticipate a sibling process claiming memory afterward.
Whatever of the resulting budget the two resolved cache ceilings do not
claim sizes a shared memory accountant the SQL executor's per-tenant
tracking reserves against, so raising `--cache-max-bytes` on a
memory-constrained host leaves less headroom for concurrent SQL queries
even though the two are configured by separate flags. A derived (not
explicit-flag) `--sql-max-query-bytes` or `--sql-tenant-max-bytes` is
additionally held at or below 90% of that remainder: the two SQL ceilings
above derive from raw `MemTotal` and so can otherwise outrun what the
budget actually leaves once the caches are carved out. The cap binds on
every deployment whose store is on loopback, where the 40% fetch-cache share
leaves a remainder whose 90% is below 50% of `MemTotal`. It also binds on an
S3 deployment whose effective memory is below about 9.7 GiB, cgroup pods
included, and on a host with co-resident processes where available memory is
well below total. An explicit flag on either is never capped this way. This budget, its two
cache carves, the SQL cap, and the remainder are computed once at startup
from the host profile observed at that moment; nothing about it changes
while the process runs, and a container whose cgroup limit or available
memory changes later is not noticed until the next restart. Startup refuses outright when
the two cache ceilings leave no strictly positive remainder, naming both
figures; `--disable-cache` is exempt, because a process that builds neither
cache claims nothing against the budget and the remainder is all of it.
`--mode gateway` derives no budget at all: it builds no query surface and
runs no fold, so nothing in it reads through either cache or reserves
against the accountant. No overhead reserve is subtracted for it, so it
starts under any cgroup memory limit, including one of 2 GiB or less. It
still needs memory for its ingest buffers (bounded by
`--max-ingest-buffer-bytes`, 512 MiB by default) plus allocator and runtime
overhead, so size its limit above that bound; its startup log prints one
line saying the memory budget is
not applicable in gateway mode, and its `ravel_memory_budget_bytes` reads
`u64::MAX`. Unless `--catalog-cache-max-bytes` is set, a gateway builds no
catalog byte cache, so its `/metrics` carries no `cache="catalog"` series
for `ravel_cache_hits_total`, `ravel_cache_misses_total`,
`ravel_cache_resident_entries`, `ravel_cache_resident_bytes` or
`ravel_cache_max_bytes`. Likewise, unless `--cache-max-bytes` is set, a
gateway builds no fetcher cache, and under `--cache-dir` no disk tier for it,
so its `/metrics` carries no `cache="fetch"` series for any of those
families. With neither flag set, no `ravel_cache_*` family renders at all.
In any mode, a `--cache-max-bytes` of `0` builds no fetcher cache.
Every other mode (`all`, `query`, `maintain`) still needs
effective memory above the 2 GiB reserve plus whatever its two cache
ceilings claim. The current state is visible
live at `/metrics`: `ravel_memory_budget_bytes` (the ceiling of that shared
accountant, which is the startup log's `memory_remainder_bytes`, the budget
MINUS the two cache ceilings, not the pre-carve `memory_budget_bytes` figure
logged beside it; `u64::MAX` means unlimited, which is what any host with
unreadable memory reports regardless of the caps set on it),
`ravel_memory_reserved_bytes` split by a
`component` label (`fetch` is the bytes held by fetch reservations on the
PromQL and SQL paths, including distributed fragment slices and the startup
cache warm pass; `sql` is the rest of the reserved total, what the SQL
executor's per-tenant accountants hold), and
`ravel_memory_handoff_overlap_bytes` (the part of the `fetch` share whose
bytes went through the read cache, hit or miss, whether or not the cache
kept them; `0` when no read cache is configured).
`--cache-max-bytes` changes less than it used to about how many times a logs
statement moves a given object's bytes: a query's plan-phase whole-object
read (the `has_word`/text and other skip-index-undecidable fallback) is now
carried into the scan for a bounded number of segments regardless of cache
size, so those objects cross the wire once. The bound is the SQL partition
count times object size, not the corpus, so undersizing this flag can
still turn the remaining segments' one wire GET into two; removing that
residual duplication needs the carry to stream per partition instead of
being held at the plan barrier, which is a separate, not-yet-shipped change.

Every resolved value is logged once at startup with the source it came from:
`flag` (the operator set it, used verbatim), `legacy-flag` (no flag for this
setting, but the legacy `--fetch-concurrency` was set and its value is used),
`derived` (computed from the host profile, or from a host-independent rule),
`derived-available` (`memory_budget_bytes` only: no cgroup limit, derived
from `MemAvailable` per the available-memory branch above),
`derived-cgroup` (`memory_budget_bytes` only: a cgroup memory limit is
present, so the budget is that limit minus the reserve, ignoring
`MemAvailable`), `budget-carve` (a fixed share of `memory_budget_bytes`
rather than of raw `MemTotal`, which is what the two cache ceilings resolve
to on a host whose memory could be read or whose budget was set with
`--memory-budget-bytes`), `budget-carve-loopback` (the
fetcher cache's larger 40% share, resolved instead of `budget-carve` when
the store is `s3` against a loopback endpoint and `--cache-max-bytes` is
unset), or `fallback` (no flag and no readable `MemTotal`, so the
compiled-in constant is used). `memory_budget_bytes` resolved with source
`flag` means `--memory-budget-bytes` won over every derivation branch. So
`journalctl -u ravel-server | grep
'performance default resolved'` answers "what is this process actually running
with" without reading the unit file:

```
INFO performance default resolved setting="fetch_concurrency" value=32 source="derived"
INFO performance default resolved setting="store_get_concurrency" value=32 source="derived"
INFO performance default resolved setting="sql_partition_count" value=32 source="derived"
INFO performance default resolved setting="promql_fetch_fanout" value=32 source="derived"
INFO performance default resolved setting="max_segments" value=1000000 source="derived"
INFO performance default resolved setting="cache_max_bytes" value=7516192768 source="budget-carve"
INFO performance default resolved setting="catalog_cache_max_bytes" value=1503238553 source="budget-carve"
INFO performance default resolved setting="memory_budget_bytes" value=30064771072 source="derived"
INFO performance default resolved setting="memory_overhead_reserve_bytes" value=2147483648 source="derived"
INFO performance default resolved setting="memory_hard_caps_bytes" value=9019431321 source="derived"
INFO performance default resolved setting="memory_remainder_bytes" value=21045339751 source="derived"
INFO performance default resolved setting="sql_max_query_bytes" value=16106127360 source="derived" clamped=false remainder_capped=false
INFO performance default resolved setting="sql_tenant_max_bytes" value=16106127360 source="derived" raised=false remainder_capped=false
INFO performance default resolved setting="gc_max_query_duration" value_ms=660000 source="derived"
```

`source="derived"` on `memory_budget_bytes` above means this host's `MemTotal`
was readable but its `MemAvailable` was not (an unusual Linux kernel or
container runtime): the budget is plain `MemTotal` minus the reserve, the
pre-amendment rule. A non-Linux build, or a Linux host with no cgroup limit
whose `/proc/meminfo` cannot be read, never reaches this source: `MemTotal`
itself is unknown there, so the source reads `fallback` and the budget is
unlimited (with a cgroup limit set, that host reads `derived-cgroup` instead),
regardless of any cache or SQL caps set on it. On a Linux host with a
readable `MemAvailable` and no cgroup memory limit, the source instead reads
`derived-available` and the value comes from the available-memory formula above;
[`docs/internal/clickbench.md`](../../internal/clickbench.md#deriving-the-reference-sizes)
has a worked example from a real measured host, including the SQL-pool cap at
90% of the remainder. When a cgroup memory limit is present, the source reads
`derived-cgroup` and `MemAvailable` is not consulted at all: the limit minus the
reserve is used directly, unchanged from before the amendment. When
`MemAvailable` plus this process's own resident set would collapse the budget
below the 1 GiB floor, one extra line appears inside the block above, after
the `memory_remainder_bytes` line and before the two SQL pool lines:

```
WARN memory_budget_bytes was held at MEMORY_BUDGET_FLOOR_BYTES: MemAvailable plus this process's own resident set left little or no room after the overhead reserve, most likely a co-resident process claiming most of the host; the subsequent min against MemTotal - MEMORY_OVERHEAD_RESERVE_BYTES can still clip memory_budget_bytes below this floor, down to 0 on a genuinely tiny host; set --memory-budget-bytes to size the budget explicitly memory_budget_bytes=1073741824 mem_available_bytes=2147483648 own_rss_bytes=0
```

The last two flags are meaningful only in a build with the `sql` feature. See
[the query guide](../query.md#operator-configurable-budgets-server-flags) for
worked sizing.

## Background

Decision records behind the choices on this page:
[credential scoping](../../adrs/0055-storage-credential-scoping.md),
[tenant-scoped credentials and control-plane protection](../../adrs/0072-tenant-scoped-credentials-and-control-plane-protection.md),
[encryption posture](../../adrs/0062-encryption-posture-and-evidential-audit.md),
[instance-role credentials](../../adrs/0106-s3-instance-role-credentials.md),
[selective subject erasure](../../adrs/0064-selective-subject-erasure.md),
[tenant admission control](../../adrs/0051-tenant-admission-control.md),
[fleet-global admission reconciliation](../../adrs/0057-fleet-global-admission-reconciliation.md),
[gzip ingest](../../adrs/0084-otlp-gzip-ingest.md),
[the read cache tier](../../adrs/0046-read-cache-tier.md),
[fail-closed isolation and startup invariants](../../adrs/0050-fail-closed-isolation-and-startup-invariants.md),
[age-based retention](../../adrs/0019-age-based-retention.md),
[compliance and custody](../../adrs/0042-compliance-custody.md),
[request-cost-aware fetching](../../adrs/0996-request-cost-aware-fetching.md),
[the request-cost latency knob](../../adrs/0904-request-cost-latency-knob.md),
[logs postings](../../adrs/0049-rlog-postings.md),
[typed attribute columns](../../adrs/0090-typed-attribute-columns-logs-sql.md),
[wide-schema load](../../adrs/0100-wide-schema-load-and-sql-latency.md),
[operator-configurable query budgets](../../adrs/0088-operator-configurable-query-budgets.md),
and [who may run DDL](../../adrs/2040-parquet-tables-queried-in-place.md).
