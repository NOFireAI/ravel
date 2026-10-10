# Configuration (day 0)

Make these decisions before you start a process for the first time. You can
change some of them with a restart (cache sizes, admission limits). Others are
permanent for the lifetime of a bucket (the tenant hash scheme, a tenant's
shard count), and each section says which.

For the name, the environment variable and the default of every flag, see
[the generated server flag reference](../../reference/ravel-server-flags.md)
and [the generated CLI flag reference](../../reference/ravel-cli-flags.md).
The sections below tell you how to choose a value.

- [Process modes](#process-modes)
- [Storage backend and credentials](#storage-backend-and-credentials)
- [Storage credential roles](#storage-credential-roles)
- [Encrypting objects with SSE-KMS](#encrypting-objects-with-sse-kms)
- [Admission limits](#admission-limits)
- [Read cache tiers](#read-cache-tiers)
- [SQL spill](#sql-spill)
- [Retention and garbage-collection configuration](#retention-and-garbage-collection-configuration)
- [Tenancy setup](#tenancy-setup)
- [Durable shard count](#durable-shard-count)
- [Logs fetch policy and store cost profile](#logs-fetch-policy-and-store-cost-profile)
- [Indexed fields and typed attribute columns](#indexed-fields-and-typed-attribute-columns)
- [Per-query budgets](#per-query-budgets)

## Process modes

`--mode` decides which jobs a process runs. A deployment that lacks a mode
lacks the work of that mode, and nothing reports the gap.

| Mode | Runs |
|---|---|
| `all` | Ingest (OTLP and Remote Write), the query API, the catalog fold over every tenant, alert evaluation. No maintenance. |
| `gateway` | Ingest. No scheduled catalog fold. |
| `query` | The query API, alert evaluation, and the on-demand fold route. No scheduled catalog fold. |
| `maintain` | Compaction, retention, the sweeper, the at-rest scrubber, and the catalog fold over the tenants it owns. No ingest, no query API. It still binds `--listen-http` for liveness, and it needs a backend that reports the `multipart` capability. |

- The scheduled catalog fold runs in `maintain` and `all`. A `maintain` fleet
  divides it across replicas by ownership.
- Every maintenance loop runs only in `maintain`.
- A deployment of `all` processes alone folds its catalog. It never compacts,
  never applies retention and deletes no durable data. Its one delete is the
  admission reconcile's reap of dead ingest processes' admission snapshots.
- A deployment of `gateway` and `query` processes alone folds nothing on a
  timer.

Read [Maintenance](maintenance.md) before you decide that you do not need a
`maintain` process.

## Storage backend and credentials

`--store s3` is the only durable store. `--store memory` is an in-process
store for tests and local experiments, and nothing in it survives process
exit.

Ravel reads the `RAVEL_S3_*` environment variables and their matching flags,
and nothing else. It does not use the AWS credential chain (profiles,
`AWS_ACCESS_KEY_ID`, `~/.aws/config`). `force_path_style` is not configurable:
the client always uses path-style addressing.

RustFS, for local development (a loopback endpoint, so it needs no
`--s3-allow-http`):

```sh
--store s3 --s3-endpoint http://127.0.0.1:9000 --s3-bucket ravel-dev \
--s3-access-key ravel --s3-secret-key ravel-dev-secret
```

AWS S3, with a static key pair. Omit `--s3-endpoint` to select real S3:

```sh
--store s3 --s3-bucket my-ravel-bucket --s3-region us-west-2 \
--s3-access-key AKIA... --s3-secret-key ...
```

A `--store s3` process with no bucket or no credentials fails at startup with
an error that names the missing one.

### Plaintext endpoints

The client uses TLS for an `https://` endpoint and for real AWS S3 with no
endpoint. Those never fall back to plaintext, so a redirect or a misconfigured
proxy cannot downgrade the connection. The client speaks plaintext HTTP only
when `--s3-endpoint` itself says `http://`.

A plaintext endpoint puts every object that the process writes and reads on
the network in the clear, with the credentials that sign those requests. So
startup refuses an `http://` endpoint whose host is not loopback unless
`--s3-allow-http` (`RAVEL_S3_ALLOW_HTTP`) is set. The refusal names the flag.

| Endpoint | Result |
|---|---|
| `http://127.0.0.1:9000`, `http://localhost:9000`, `http://[::1]:9000` | Allowed with no flag. The traffic never leaves the host. |
| `http://rustfs:9000`, `http://rustfs.ravel-system.svc:9000`, or any other name or address on the network | Refused unless the flag is passed. |
| `https://...` | Unaffected. The flag does nothing. |
| `rustfs:9000`, or any endpoint written with no scheme | Refused at startup. `--s3-allow-http` does not accept it. |

- A container or a pod reaches its object store over the network, never over
  loopback. So a plaintext in-cluster RustFS or floci needs the flag, although
  the traffic stays inside the cluster.
- An endpoint that begins with neither `https://` nor `http://` is not a
  usable URL. The refusal quotes the endpoint as it was written and asks for
  the scheme. The flag chooses between TLS and plaintext, and an endpoint with
  no scheme asked for neither.
- The scheme is matched without regard to case, so `HTTPS://rustfs:9000` is an
  `https` endpoint.

`ravel-cli` ships in the server image and applies the same rule, with the
same `--s3-allow-http` flag and `RAVEL_S3_ALLOW_HTTP` variable.

Under the Kubernetes operator, set `spec.storage.s3.allowHttp` on the
`RavelCluster` (default `false`). The operator renders the flag into the
arguments of every server container. It also renders
`RAVEL_S3_ALLOW_HTTP=true` into the store-qualification Job, which runs
`ravel-cli store qualify` before any server pod exists. So a cluster with a
plaintext in-cluster endpoint needs `allowHttp: true` for qualification to
run.

Terminate TLS at the object store in preference to the flag. The flag is for
a development backend that speaks no TLS. It is not for a production backend
whose certificate is inconvenient.

### Upload and read checksums

By default every PUT carries a CRC64-NVME checksum
(`x-amz-checksum-crc64nvme`), and every full-object read is verified against
the stored checksum.

On a write:

- The default also applies to PUTs from the per-tenant stores that
  `--tenant-kms-config` routes to.
- An object of any size up to S3's 5 GiB single-request limit is sent as one
  checksummed PUT, not in parts.
- The endpoint verifies the body against the checksum and rejects a PUT whose
  bytes changed on the way. So a corrupted object never becomes visible.
- The endpoint stores the checksum with the object.

On a read:

- Every request except a LIST asks the endpoint to return the stored checksum
  (`x-amz-checksum-mode: ENABLED`).
- A full-object read is verified against the stored checksum before the bytes
  are used. A mismatch is an error, not a wrong answer.
- This is the only check that a commit record gets. A commit record is a bare
  protobuf with no checksum of its own.

Two flags change the defaults:

| Flag | Environment variable | Values |
|---|---|---|
| `--s3-upload-integrity` | `RAVEL_S3_UPLOAD_INTEGRITY` | `crc64nvme` (the default), `sha256`, or `off` |
| `--s3-request-stored-checksum` | `RAVEL_S3_REQUEST_STORED_CHECKSUM` | `true` (the default) or `false`, written `--s3-request-stored-checksum=false` |

- With `sha256`, the endpoint verifies on upload only. Ravel cannot recompute
  it on read, so a read of an object stored with it counts as unverified.
- With `--s3-request-stored-checksum=false`, no request asks for the stored
  checksum. Every full-object read is served unverified and counted.
- Both flags are ignored under `--store memory`.
- The per-tenant stores that `--tenant-kms-config` routes to apply both flags
  as the default store does.

AWS S3 and RustFS accept both headers. For an endpoint that does not:

| The endpoint rejects | Result | Remedy |
|---|---|---|
| The upload checksum header | Every PUT fails with the endpoint's error. Startup writes nothing to an existing bucket, so the process can report ready first and fail at its first flush. | `--s3-upload-integrity off`. Every object that the process writes, commit records included, then has no transport checksum to verify against. |
| The checksum-mode request header | | `--s3-request-stored-checksum=false` |

A read that finds no stored checksum that it can check is served, never
refused. It is counted in `ravel_store_get_unverified_total` (see
[Observability](../observability.md)). The counter moves in three cases:

- Objects written before upload checksums were on carry none. The counter
  moves on an upgraded bucket until retention or a rewrite replaces them.
- An object larger than one request body (8 MiB by default) is read in
  several responses, and none covers the whole object. So every whole read of
  such an object is counted. Scrub, compaction and quarantine read large data
  objects whole, and the counter keeps growing on an honest endpoint.
- The endpoint returns no stored checksum. The counter does not separate this
  case from the large-object case. A count that grows while no scrub,
  compaction or quarantine pass is reading, on a bucket written with
  `crc64nvme`, points at the endpoint.

Under the Kubernetes operator the same two settings are
`spec.storage.s3.uploadIntegrity` and `spec.storage.s3.requestStoredChecksum`
on the `RavelCluster`. They also govern the operator's own S3 client.
`ravel-cli` takes the same two options with the same defaults. See
[Upload checksums](#upload-checksums).

### Choosing a credential source

`--s3-auth` selects where the credentials come from.

| Value | Source | Required |
|---|---|---|
| `static` (the default) | The access key and secret key from the flags or the environment. | Both keys. |
| `instance-role` | Short-lived credentials from the EC2 instance metadata service. Nothing static is stored on the instance, in the environment, or in logs. | Only `--s3-bucket`. |

Under `instance-role`:

- Any of `--s3-access-key`, `--s3-secret-key`, `--s3-session-token` or
  `--s3-credentials-file` is a startup error that names the conflict. No
  precedence rule applies. An exported `RAVEL_S3_ACCESS_KEY` counts.
- The first credential fetch happens at startup. So a misconfigured instance
  role fails to start, and does not fail on its first request.

On EC2, attach the instance role and start with no credential flags:

```sh
ravel-server --store s3 --s3-bucket my-bucket --s3-region us-east-1 \
  --s3-auth instance-role
```

Under `static`, two further sources serve credentials that rotate:

- `--s3-session-token` pairs a temporary token with the key and secret, for
  credentials that a token service issues.
- `--s3-credentials-file` names a JSON file of `access_key_id`,
  `secret_access_key` and an optional `session_token`. An external process
  rewrites the file on disk.
  - The file wins over the inline flags, including the session token.
  - It is read once at startup, so an unreadable or malformed file fails
    startup.
  - After startup it is re-read on the request path only when its
    modification time changes.
  - A parse failure during a rotation keeps serving the last good credential,
    with a rate-limited warning.

### Store options in ravel-cli

`ravel-cli` accepts the same store flags and environment variables, including
`--s3-auth`. It has one gap: it has no `--s3-kms-key` and never sets a key id
on its writes.

With `--store` unset, `ravel-cli` uses `memory` and reports it. Every
`ravel-cli` command that walks tenant data opens its report with the store
that it resolved:

```
store: memory (default)
store: memory
store: s3
```

On the defaulted memory store only, a walk that reaches no data is refused.
It is not reported as a healthy zero:

```
--store defaulted to memory, which holds no data for tenant "clickbench";
maintain compact-tenant found no objects there and would have reported a
healthy zero-work result. Pass --store s3 (with RAVEL_S3_BUCKET and its
credentials) to run against the real bucket, or load data first.
```

An explicit `--store memory` keeps the zero-count report.

### Upload checksums

`ravel-cli --store s3` attaches a server-verified checksum to every PUT and
asks for the stored one on every read, as the server does (see
[Upload and read checksums](#upload-and-read-checksums)).

- `--s3-upload-integrity` (`RAVEL_S3_UPLOAD_INTEGRITY`): `crc64nvme`, the
  default, attaches `x-amz-checksum-crc64nvme`. `sha256` attaches
  `x-amz-checksum-sha256`. `off` attaches none.
- An endpoint that does not support the header fails the first write. `off`
  is the remedy there, and commit records written under it are unverified.
- With a checksum on, every object goes out as one PUT, not in parts. So an
  overwrite above S3's 5 GiB single-request limit is refused, and the refusal
  names `off` as the remedy. No `ravel-cli` write comes near that size.
- `--s3-request-stored-checksum` (`RAVEL_S3_REQUEST_STORED_CHECKSUM`): on by
  default, it sends `x-amz-checksum-mode: ENABLED`. A whole-object read is
  checked against a returned CRC-64/NVME or CRC-32C checksum before its bytes
  are used, and a mismatch is an error.
- A read that returns no checksum, or a SHA-256 one, is served unverified.
- `--s3-request-stored-checksum=false` stops sending the header, for an
  endpoint that rejects it.

`ravel-cli store qualify` reports whether the endpoint returns the stored
checksum. See [qualify the store](deployment.md#qualify-the-store).

## Storage credential roles

You can give each process an S3 credential that is scoped to its job. A
leaked scoped credential can do only what that job does, and only one of the
four storage credential roles can delete durable data.

Every Ravel process holds one S3 credential and uses it for every
object-store call. With a single bucket-wide credential, a leak from any one
process can read, overwrite or delete anything in the bucket.

The policy layer of the storage backend enforces the scope (AWS IAM, or the
policy layer that an S3-compatible store exposes). Ravel has no in-process
authorization check, and the `RAVEL_S3_*` contract does not change. You
provision a narrower credential for each storage credential role and attach
the policy.

One credential for everything is still supported. It is the right choice for
a development or single-operator deployment.

### The four roles

| Role | Process | What it does |
|---|---|---|
| Gateway | `--mode gateway`, and the ingest half of `--mode all` | Writes L0 segments and their commit records, idempotency markers, a tenant's provisioning record on adopt, and on a keyed bucket each tenant's recovery manifest under `sys/t/`. Runs no catalog fold: the scheduled fold runs in `--mode maintain` and `--mode all`, though `gateway.json` still carries the fold's catalog grants. On a keyed bucket, reads the durable token map `sys/auth`. Reads each tenant's config record `t/<hash>/config` for its admission-limit overrides, and reads and writes its metric metadata record `t/<hash>/m/meta`. Deletes the admission snapshots of dead ingest processes under `t/<hash>/<signal>/admission/`, its one delete grant; it deletes no durable object. |
| Query | `--mode query`, and the query half of `--mode all` | Lists and reads commit records, catalog objects and segment data. Runs the catalog fold only through the on-demand fold route, writing catalog snapshot parts, `HEAD` and index objects when it does, and appends query-audit records. On a keyed bucket, reads the durable token map `sys/auth`. Reads each tenant's config record `t/<hash>/config` for its declared typed-column overrides, and its metric metadata record `t/<hash>/m/meta`. Runs the alert evaluator, so it writes alert transitions under `t/<hash>/a/l0/` and `t/<hash>/a/c/` and reads and writes each tenant's alert lease `t/<hash>/a/alert-lease` and state memo `t/<hash>/a/state/latest`. For Parquet table queries, lists and reads the table manifests under `t/<hash>/pq/t/` and reads the location grants record `t/<hash>/pq/grants`. Runs `CREATE EXTERNAL TABLE` and `DROP TABLE` over `POST /api/v1/sql`, so it creates new table manifest versions under `t/<hash>/pq/t/` (create only: it cannot overwrite an existing version) and writes and deletes its own bucket-probe scratch objects under `sys/pq-probe/`. Deletes no data, catalog or control-plane object. |
| Maintain | `--mode maintain` | Compaction, retention and the sweeper. Runs the scheduled catalog fold, so it also writes catalog snapshot parts, `HEAD`, and index objects (name postings and column stats). The only role that deletes durable data: L0 and L1 segments, commit records, idempotency markers, the query-audit shard, erasure requests (`del/*.dreq`) and superseded Parquet table manifests under `t/<hash>/pq/t/`. It also deletes superseded catalog snapshot parts and index objects, quarantined copies and dead worker records. `ravel-cli parquet sweep`, `ravel-cli parquet repair --delete` and `--delete-version`, `ravel-cli maintain compact-bucket` and `ravel-cli maintain compact-tenant` run under this credential. Reads each tenant's config record `t/<hash>/config` to resolve the retention window, and the alert state memo `t/<hash>/a/state/latest` for alert retention. |
| Admin | `ravel-cli` | One-off bootstrap and mutation commands. Invoked by an operator or a CI job, never by a long-running server. The broadest of the four. Writes each tenant's config record `t/<hash>/config` (`typed-attr-column`, `clustering-key` and `bloom-scope` set commands) and Parquet location grants record `t/<hash>/pq/grants` (`tenant parquet-grant add` and `remove`; `add` also writes and deletes a probe object under `sys/pq-probe/`). See [the Admin credential](deployment.md#the-admin-credential). |

Under `--tenant-kms-config`, Gateway, Query and Maintain also read and write
the key-epoch record `t/<hash>/enc` of each configured tenant at startup.

Maintain runs the scheduled catalog fold and Query runs the on-demand fold
route, so both hold the catalog write grants.

### The shipped policy documents

[`deploy/iam/`](../../../deploy/iam/) holds one policy document for each
storage credential role.

1. In each file, replace `my-ravel-bucket` with your bucket.
2. Set the KMS key ARNs, as described below.
3. Attach each document to the principal whose access key the deployment of
   that role uses.

**KMS key ARNs.** The `Resource` value of the KMS statement is a JSON array.
It ships with one entry, the placeholder
`arn:aws:kms:us-east-1:111122223333:key/REPLACE-WITH-TENANT-KEY-ID`. One entry
is correct only for a deployment with one KMS key.

Two independent flags put keys in play (see
[Encrypting objects with SSE-KMS](#encrypting-objects-with-sse-kms)). The
policy of each role must authorize every key that its own writes and reads
can reach. So the array needs an exact ARN for each of these keys:

- The key configured with `--s3-kms-key`, if the deployment sets it. This key
  is applied to the default store, so it encrypts every PUT that no
  per-tenant key overrides. If you omit it, Gateway, Query and Maintain PUTs
  fail with `AccessDenied`. Reads of objects already written under it fail
  KMS decryption for every role, including Admin.
- Every key configured in the `--tenant-kms-config` file, one entry each.

Add each ARN as its own entry. A configured key that is missing from the
array does not fail at startup. The process starts normally, and the first
request that uses that key fails with `AccessDenied` on that KMS call.

Know three facts before you edit the documents.

**Every role denies delete on the protected prefixes.**

| Role | Delete grants |
|---|---|
| Query | One: its own bucket-probe scratch objects under `sys/pq-probe/*`. |
| Gateway | One: the admission snapshots of its dead processes. |
| Admin | Two: the scratch prefixes `sys/qualify/*` and `sys/pq-probe/*`. |

All three still carry the same explicit `Deny` on `s3:DeleteObject` and
`s3:DeleteObjectVersion` over the protected control prefixes. An explicit
`Deny` overrides any `Allow`, so those prefixes are undeletable even by
Maintain.

**The audit prefix has two shards with different treatment.**

- The legal-hold shard (`t/*/u/*/0000/*`) is deny-delete for every role
  including Maintain, so a legal hold cannot be destroyed.
- The query-audit shard (`t/*/u/*/0001/*`) is compacted and age-swept on a
  90-day window by the Maintain process, so only Maintain grants delete on
  it.

The two shard paths are disjoint, but the level-based delete grants of
Maintain are not confined to them. An audit object is keyed
`t/<hash>/u/<level>/<shard>/...`, so `t/*/*/l0/*`, `t/*/*/c/*` and
`t/*/*/l1/*` match legal-hold keys too.

The explicit `Deny` keeps a legal hold safe. It names both `s3:DeleteObject`
and `s3:DeleteObjectVersion` on that shard, and a `Deny` overrides an `Allow`
only for the actions that it names. If you edit these policies, keep the
action list of the deny at least as wide as every delete action that an
`Allow` grants on those keys.

**Tenant discovery needs a bare prefix entry.** Discovery lists the bare,
delimited `t/` prefix and not a per-tenant subpath. Under AWS `StringLike`,
none of the `t/*/...` wildcards match the literal string `t/`. So every role
that performs discovery (Gateway, Query and Maintain) needs a separate `t/`
entry in its `ListBucket` condition, beside the per-key wildcards. This entry
does not widen what those roles can read: a prefix listing enumerates keys
and does not grant `GetObject` on them.

**Every write is `s3:PutObject`.** Create-if-absent, compare-and-set and
plain overwrite are all `s3:PutObject` at the policy layer. The difference
between them is a request precondition header, not a separate action. So the
write grant of a role is a `PutObject` allow on its write prefixes, and
Ravel's own request enforces the create-only and compare-and-set semantics.

[The catalog and MVCC contract](../../catalog-and-mvcc.md) is the normative
document for the key layout that the policies reference.

### Subject-erasure grants

Selective subject erasure adds one object prefix, `t/<hash>/<sig>/del/`. The
prefix holds two kinds of object:

- an erasure request (`<request_id>.dreq`), which contains the subject
  identifier
- its completion marker (`<request_id>.done`), which does not

The rewrite pass and the physical sweep that erasure drives touch only
prefixes that Maintain already has. So only the new prefix needs grants:

- Admin creates the request and deletes nothing.
- Query and Maintain read the prefix, to attach pending predicates at resolve
  time and to scope the rewrite pass.
- Maintain deletes the request only, after its completion marker exists and
  the protection horizon passes.
- No role, Maintain included, can delete a completion marker.

Add each statement to the same policy file as the other grants of that role.
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

Also add `t/*/*/del/*` to the Query and Maintain `ListBucket` prefix
conditions, and add the completion deny to all four policy documents.

### S3-compatible stores

A store that exposes an S3-compatible policy layer takes the four documents
under `deploy/iam/` unchanged. They are ordinary S3 policy JSON: the same
actions, the same `arn:aws:s3:::<bucket>/<prefix>` resources, the same
explicit `Deny` semantics. Load them with the administrative tooling of that
store, and attach one credential for each role.

The local development and CI object store in this repository is RustFS, with
one shared credential for every process. The per-role split is a production
hardening, and neither environment needs it. Ravel depends on no
store-specific admin API, so nothing in this repository drives one.

## Encrypting objects with SSE-KMS

Two independent flags encrypt objects with SSE-KMS. Both are off by default.

`--s3-kms-key <arn>` encrypts every PUT that the process makes with one key.
It adds no routing and no new object. The single store that every deployment
builds is constructed with that key id.

`--tenant-kms-config <path>` names a TOML file of per-tenant keys.

- Only this flag inserts the routing decorator into the store chain.
- It routes writes for the keyspace of a configured tenant to a lazily built
  store, which is constructed with the key of that tenant.
- Every other tenant, and every read, goes to the default store unchanged.
- It requires `--store s3` and refuses to start under `--store memory`.

```toml
# --tenant-kms-config kms-tenants.toml
[tenants]
acme = "arn:aws:kms:us-east-1:111122223333:key/acme-key"
other = "arn:aws:kms:us-east-1:111122223333:key/other-key"
```

### Key epochs

Startup bootstraps the key-epoch history of a tenant at `t/<hash>/enc`. It
does so the first time the tenant's key is configured, and on every later
rotation to a different key.

- Epoch 0 records an empty key (the deployment-default convention) with an
  activation time at the start of Unix time. That time is at or before the
  earliest live object of any tenant, so the custody check never meets an
  object that predates epoch 0.
- Epoch 1 follows immediately, with the real key and the activation time of
  the moment of configuration.
- A restart with the same key is a no-op. A restart with a different key
  appends a rotation epoch.
- Startup writes the epoch record before it switches routing to the new key.
  So a crash between the two cannot leave data flowing through a key with no
  epoch record.

### Key grants

**Both halves of the grant are required.** The key policy grants usage to the
principal, and the principal's own policy must allow the action. If it does
not, the request is denied before it reaches the key policy.

Without both, the first encrypted PUT that a role makes for a configured
tenant fails closed with `AccessDenied`. After a tenant is named in the file,
its writes always route through that key, with no fallback to the default
key.

A minimal per-tenant key policy follows, scoped to the roles that the
deployment runs. Every principal that you add widens the blast radius that
the key policy narrows.

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
placeholder tenant key ARN. Replace it with the exact ARNs of your real keys,
not with every key. [The shipped policy documents](#the-shipped-policy-documents)
lists which ARNs each array needs. With the keys in place:

- Gateway and Maintain write tenant data through the routing store and read
  some of what they write. They hold encrypt, generate-data-key and decrypt.
- Query reads tenant data and writes routed objects under `t/<hash>/`: the
  catalog snapshot, `HEAD` and index objects that its fold publishes,
  query-audit records under `u/`, the `enc` key-epoch record, and the lease,
  state memo and transition objects of the alert evaluator. It holds encrypt,
  generate-data-key and decrypt.
- Admin holds decrypt only, without generate-data-key. With that grant, a
  leaked Admin credential can mint ciphertext under tenant keys that it has
  no write role for.

The `t/<hash>/enc` epoch record has its own grant. Gateway, Query and
Maintain read and write it, because startup bootstraps it in every mode.
Admin reads it for `verify-custody`. Every template denies its deletion.

### SSE-KMS in ravel-cli

`ravel-cli` separates tenant data from control records.

**Commands that route.** The commands that write tenant data under the
Maintain credential take the same `--tenant-kms-config` flag, read the same
file, and route as the server does:

| Command | Writes |
|---|---|
| `maintain compact-bucket`, `maintain compact-tenant`, `maintain migrate` | L1 segments and compaction records. `migrate` also writes its cursor and the floor raise in `prov`. |
| `catalog fold` | Catalog snapshot parts, `HEAD` and index objects. |

Pass these commands the file that the servers use. The flag requires
`--store s3`.

- The command applies only the entry for its own `--tenant`.
- A tenant that the file does not name is written under the bucket's default
  encryption, as the server writes it.
- For a tenant that the file names, the command first reads the
  `t/<hash>/enc` key-epoch record of that tenant.

| Key-epoch record | Result |
|---|---|
| Its current key is the key in the file | The command leaves the record alone and writes its data under `t/<hash>/` under the tenant's key. |
| It is absent, or its current key differs | The command refuses before any write. Start the server with the file first, then run the command. |

The command refuses because only server startup records a configured or
changed key, and the record is append-only. No `ravel-cli` command creates
the record. The one write that a command makes to it completes a record that
holds only the bootstrap epoch 0, which a server began and did not finish.

The epoch record itself is a control record, written under the bucket's
default encryption.

A `--dry-run` validates the file, reads the key-epoch record, refuses as the
real run refuses, prints the same routing line, and writes nothing.

Maintain already holds encrypt and generate-data-key on the tenant keys, and
the `t/*/enc` write. So these commands need no new grant.

**Admin commands.** Admin stays decrypt-only, so no Admin command takes the
flag. What Admin writes under `t/<hash>/` stays under the bucket's default
encryption, whatever the file says:

- provisioning records
- legal holds
- reconstructed commit records
- erasure requests
- the tenant config record
- the Parquet location grants record

These are control records, not tenant data. The missing generate-data-key
grant refuses none of these writes, with one exception. If the bucket's
default encryption is itself a customer-managed KMS key, Admin needs
generate-data-key on that key.

`maintain verify-custody` checks write times against the key-epoch history,
not the key that each object is encrypted under. So it reports none of these
records. For a tenant with a recorded epoch, it prints a `control records:`
line that says so.

**Commands that do not route.** Two other `ravel-cli` writers under
`t/<hash>/` take no `--tenant-kms-config` and write under the bucket's
default encryption:

- `maintain sweep` runs under the Maintain credential and writes only
  unnamed-since markers there. The quarantine copies that it writes are
  outside `t/`, so the server does not route them either.
- `load`, the bulk loader, writes L0 segments, their commit records and,
  through `validate_or_adopt`, the provisioning record of the tenant. A
  deployment that needs bulk-loaded data under a tenant's own key cannot get
  that from `ravel-cli` today.

SSE-KMS does not cover bytes written to the local read cache. See
[read cache tiers](#read-cache-tiers).

## Admission limits

`--limits-file` names a TOML file with a `[defaults]` table and zero or more
`[tenants.<id>]` override tables. With no `--limits-file`, every tenant gets
the shipped defaults.

Every field is optional, and you can override each one independently. A
tenant table needs only the fields that differ from `[defaults]`. The
`[defaults]` table needs only the fields that differ from the shipped
defaults.

| Field | Meaning |
|---|---|
| `max_active_series` | Cap on concurrently active metric series for the tenant. |
| `max_active_streams` | Cap on concurrently active log streams for the tenant. |
| `ingest_bytes_per_sec` / `ingest_byte_burst` | Token-bucket rate and burst for ingested bytes. |
| `series_creation_rate_per_sec` / `series_creation_burst` | Token-bucket rate and burst for new series and stream creation. |

Each of the four count or rate fields accepts the literal string
`"unlimited"` in place of a number, to opt a tenant out of that cap. The two
burst-only fields do not.

Validation is fail-closed. The process refuses to start, and does not fall
back to the shipped defaults, on any of these:

- a file that is not valid TOML
- an unknown key in any table
- an empty tenant id
- a count or rate of zero or below
- a burst set without a rate to pair with
- a burst set alongside `unlimited` for the same rate

### Shipped defaults and memory cost

```
max_active_series            = 200000
max_active_streams           = 200000
ingest_bytes_per_sec         = 33554432   (32 MiB/s)
ingest_byte_burst            = 67108864   (64 MiB)
series_creation_rate_per_sec = 10000
series_creation_burst        = 100000
```

Size the two active-count caps with care, because each tracked identity costs
resident memory. The measured entry cost is 35 to 56 bytes. That figure
includes hash-table slot overhead, power-of-two table sizing at 7/8 load and
allocator headroom. A naive estimate gives approximately 16 bytes.

The admission controller tracks active series and active streams in a
two-epoch rotating set, so both epochs can be live at once:

```
cap x bytes_per_entry x 2 epochs x 2 signals (series + streams)
```

| Cap | One fully active tenant | Ten fully active tenants at once, worst case |
|---|---|---|
| 200,000 (shipped) | 27 to 43 MiB | 267 to 427 MiB |
| 1,000,000 | 134 to 214 MiB | 1.3 to 2.1 GiB |

When a tenant needs a higher ceiling, raise it in the table of that tenant
and size it against this formula.

### Transient decompression memory

Gzip on OTLP over HTTP adds a second, transient memory demand that the ingest
buffer budget does not account for. A gzip request is decompressed into a
fresh buffer, bounded by the 64 MiB decompressed cap. The buffer is held only
while the request holds an ingest concurrency permit:

```
max_inflight_ingest_requests x 64 MiB
```

At the default 1024 permits that is a 64 GiB worst case, far more than a
small host has. Lower `--max-inflight-ingest-requests` until this product
fits your headroom, beside the ingest buffer budget and the active-identity
memory above. The three are additive and none bounds the others.

The gRPC path is bounded at 16 MiB per in-flight request, so the same
arithmetic applies with a 16 MiB factor.

## Read cache tiers

The read cache has a RAM tier and an opt-in local-disk tier.

| Tier | State |
|---|---|
| RAM tier | On, unless `--disable-cache` is set or its ceiling resolves to `0`. A gateway resolves `0` when no ceiling flag is set. |
| Disk tier | Off, unless `--cache-dir <path>` is set. |

`--cache-dir <path>` attaches the disk tier at that directory to both the
query fetcher cache and the catalog byte cache. A RAM eviction is then served
from local disk and does not pay the object-store round trip again:

```sh
ravel-server --store s3 --s3-bucket my-bucket --cache-dir /var/cache/ravel
```

The disk tier has no separate capacity flag. Each tier is bounded by the
resolved RAM ceiling of its own cache, read once at startup with no live
resize.

### Cache ceilings

The fetcher cache and the catalog byte cache are two independent LRU caches.
Each flag bounds only its own cache.

| Cache | Flag | Unset, derives from the process memory budget |
|---|---|---|
| Fetcher cache | `--cache-max-bytes` | 25%, or a larger 40% against a loopback `--s3-endpoint`. `7516192768` on the 30 GiB reference host at the 25% share. |
| Catalog byte cache | `--catalog-cache-max-bytes` | Always a smaller 5%. `1503238553` on the 30 GiB reference host. |

[Per-query budgets](#per-query-budgets) describes the process memory budget.

- Startup refuses to start, and does not clamp, if the two resolved hard caps
  together reach or exceed the process memory budget. This never happens in
  `--mode gateway`, which derives no budget.
- Both ceilings are LRU caps, not reservations. Neither cache pre-allocates,
  and each holds only the bytes that it admitted.
- The sum of the two cache ceilings and the SQL memory pools can exceed
  physical RAM by design. The SQL pools derive from raw host memory, not from
  the process memory budget. The caches fill only under a working set that
  large, and a SQL query aborts before it grows past its own pool.
- `--disable-cache` turns both caches off and holds no read-cache memory.

### Disk tier behaviour

The disk tier is disposable. The directory is created lazily on first
admission and is never required to exist. A missing, full or corrupt cache
directory degrades to a store read, never to a query error. So a node whose
cache directory is deleted while it runs answers every query correctly, only
more slowly.

SQL spill under the same directory is the exception: it is checked at
startup. See [SQL spill](#sql-spill).

**Cache bytes are not encrypted by SSE-KMS.** Server-side encryption protects
object bytes at rest in the store, not the bytes that this process writes to
`--cache-dir`. To encrypt the cache directory at rest, use the filesystem or
volume layer, for example an encrypted volume mounted there.

With a disk tier configured, the counters of each cache gain a tier label
beside the cache label, so RAM and disk hit rates are reported separately.
With no `--cache-dir`, no tier label appears. See
[the caching guide](../caching.md) for the full metric list and sizing
advice.

## SQL spill

In a build with SQL, a query whose memory pool fills can spill its working
state to local disk and finish, when its plan qualifies. A query that does
not qualify fails with a resources-exhausted error.

- A plan qualifies when it is an aggregation built only from `COUNT`, `SUM`
  over integers and `AVG` over integers, with no float `GROUP BY` key.
- The aggregation can have an `ORDER BY` placed directly over it. See
  [sort order over an aggregation](#sort-order-over-an-aggregation).
- Any other aggregate (`MIN`, `MAX`, or `SUM` or `AVG` over floats) keeps the
  query on the refusal.

Spilled files belong to one query and are removed when it ends. Nothing reads
them afterwards.

### Spill settings

The server takes the spill settings from the first of these sources that is
configured:

1. `--sql-spill off` disables spill, whatever the other two sources say. Use
   it on a node with no safe local scratch storage. The other value, `auto`,
   is the default.
2. `RAVEL_SQL_SPILL_DIR` and `RAVEL_SQL_SPILL_MAX_BYTES`, both set: spill
   goes under that directory. The queries of the process together can hold at
   most that many bytes of spill at once. The directory must already exist.
   The server does not create it.
3. `--cache-dir` set and `RAVEL_SQL_SPILL_DIR` unset: spill goes under
   `<cache-dir>/sql-spill/<instance-id>`, with a derived ceiling.
   `RAVEL_SQL_SPILL_MAX_BYTES` set alone replaces the derived ceiling.
4. None of the above: spill is off, and a query that outgrows its pool fails.

Two combinations refuse startup with an error that names the missing
variable:

- `RAVEL_SQL_SPILL_DIR` without `RAVEL_SQL_SPILL_MAX_BYTES`
- `RAVEL_SQL_SPILL_MAX_BYTES` alone with no `--cache-dir`

### Derived ceiling

The derived ceiling is computed once at startup:

1. Start from the free bytes on the volume that backs `<cache-dir>/sql-spill`.
2. Subtract the most that the read cache's disk tier in the same directory
   can hold. That is `cache_max_bytes` plus `catalog_cache_max_bytes` in the
   startup log, or nothing under `--disable-cache`.
3. Take half of the result.
4. Cap it at four times the process memory budget (`memory_budget_bytes` in
   the startup log).
5. If that cap is below 1 GiB, raise the ceiling to 1 GiB.

```text
min((free_bytes - read_cache_bytes) / 2, 4 * memory_budget_bytes), at least 1 GiB
```

When `(free_bytes - read_cache_bytes) / 2` is below 1 GiB, spill is off. Both
startup lines then read `value=none source=cache-dir-insufficient-space`, and
a WARN line says why. So a derived ceiling is never below 1 GiB and never
more than half of what the read cache leaves free. The `sql_spill_max_bytes`
line carries the two figures that it was derived from, as `free_bytes` and
`read_cache_bytes`.

For example, take a volume with 200 GiB free and the defaults of a 30 GiB
host (a 30,064,771,072-byte budget, a 9,019,431,321-byte read cache). The
ceiling is 102,864,466,739 bytes. That is half of what the read cache leaves,
and it is below the 120,259,084,288-byte cap.

The ceiling is one budget for the whole process:

- A qualifying query reserves its own spill limit from the ceiling before it
  starts. The limit is the ceiling, or what is left of it if that is less.
- When less than 64 MiB is left, the query runs with spill off and a WARN
  line says why. Under a ceiling smaller than 64 MiB, the threshold is the
  whole ceiling.
- The reservation is returned when the query ends.
- A query that starts while no other holds a reservation takes the whole
  ceiling. So a second qualifying query that starts while it runs gets no
  spill.
- Spill has no per-tenant quota, and nothing adds up the ceilings of several
  processes that share one cache volume. Give each spilling process its own
  volume.

### Startup log

Startup logs the result on two `performance default resolved` lines, in the
same `setting=... value=... source=...` layout as `sql_max_query_bytes`:

| `setting` | `value` | `source` |
|---|---|---|
| `sql_spill_dir` | the spill directory, or `none` | `env`, `cache-dir`, `cache-dir-insufficient-space`, `flag-off` or `unset` |
| `sql_spill_max_bytes` | the process spill ceiling in bytes, or `none` | `env`, `env-override` (the variable alone over a `--cache-dir` root), `derived`, `cache-dir-insufficient-space`, `flag-off` or `unset` |

### Spill directory

Under `--cache-dir` the layout is:

```text
<cache-dir>/sql-spill/<instance-id>/.owner.lock
<cache-dir>/sql-spill/<instance-id>/ravel-spill-<pid>-<nonce>-<n>/   one per spilling query
```

`<instance-id>` is the worker id of the process, a UUID drawn at each start
in every mode. A `maintain`-mode process also heartbeats under it. So every
process and every restart gets its own directory.

When spill resolves under `--cache-dir`, the process creates its directory
before it serves a query. It holds an exclusive lock on `.owner.lock` until
it shuts down. The operating system releases the lock however the process
exits.

The spill directory is checked at startup, unlike the read cache. With
`--cache-dir` set and spill resolving there, the server refuses to start if
it cannot create `<cache-dir>/sql-spill`, measure its free space, or take the
lock of its own directory. `--sql-spill off` starts without touching it.

### Startup sweep

A process that serves SQL sweeps `<cache-dir>/sql-spill` once at startup when
all of these hold:

- `--cache-dir` is set.
- `--sql-spill` is not `off`.
- `<cache-dir>/sql-spill` already exists.

A `maintain` or `gateway` process does not sweep. A process under
`--sql-spill off` does not sweep.

When the sweep runs:

- It runs before the free space is measured and before spill is resolved. So
  it also runs when spill resolves to the directory of the
  `RAVEL_SQL_SPILL_DIR` pair, or resolves off for lack of space.
- The free space measured afterwards includes what the sweep reclaimed.
  Consider a volume whose free space was below the 1 GiB floor of the derived
  ceiling only because of directories that a crashed process left behind.
  That volume gets spill back at that start.
- The sweep does not create `<cache-dir>/sql-spill`, and it runs before the
  process creates its own directory.

What the sweep deletes:

- It deletes a directory under `<cache-dir>/sql-spill` only when this process
  can take the lock of that directory itself, which means that its owner is
  gone.
- It leaves in place a directory whose lock is held, or that has no
  `.owner.lock`.
- It deletes a `.swept-` directory without a lock check. An earlier sweep
  moved such a directory aside and did not finish deleting it.
- It removes only what it lists under `<cache-dir>/sql-spill`. Do not make
  that directory a symbolic link, because the sweep follows it.

What the sweep logs:

- Nothing for a held lock.
- A WARN with the path for a directory whose ownership it cannot settle (no
  `.owner.lock`, or any other error taking the lock).
- After the sweep, an INFO line with the path and a reason for every
  directory still left there.
- For a `.swept-` directory that is still there afterwards, the INFO line
  says that its removal failed or is still in progress in another process.

Put `--cache-dir` on a local volume unless `--sql-spill off` is set, because
the sweep runs even when spill resolves elsewhere. The lock proves ownership
only between processes on the same host. On a network mount whose locks are
local to each client (NFS mounted with `nolock` or `local_lock`), a process
on one host can take the lock of a directory that a process on another host
still uses. Its sweep then deletes that live directory.

### Sort order over an aggregation

An `ORDER BY` gets the `GROUP BY` columns of an aggregation appended as
trailing tiebreak terms when both of these hold:

- The aggregation is built only from `COUNT`, `SUM` and `AVG`, with no float
  `GROUP BY` column.
- The `ORDER BY` is placed directly over the aggregation, or over the select
  list directly above it.

The terms are ascending with nulls last. They are appended whatever the spill
setting and whatever the types of the aggregated columns. The order is then
total. So a qualifying statement returns the same rows in the same order with
spill forced, with spill off, and in memory.

- A `GROUP BY` column that the select list renames is matched under its new
  name.
- A `GROUP BY` column that the select list leaves out is carried to the sort
  and dropped again.
- An alias that only shares the name of a `GROUP BY` column is not taken for
  it.

An `ORDER BY` is left as written, and the query does not spill, when it is
over any of these:

- any other aggregate (such as `MAX` or `MIN`)
- a float `GROUP BY` column
- grouping sets
- a `HAVING` filter
- a nested subquery

## Retention and garbage-collection configuration

These values govern when the bytes of a deleted object go away. They must
agree with each other, or a reader can lose a segment while it reads. The
governing inequalities are:

```
protection_horizon >= max_query_duration + grace + clock_skew_allowance
protection_horizon >= max_compaction_lifetime + 4 * clock_skew_allowance
```

`max_compaction_lifetime` is compiled in (1h). The second bound keeps a late
compaction or erasure-rewrite run from changing which inputs a sweep can
delete after their horizon passed.

The first three values are recorded once, deployment-wide, in a durable
`sys/gc` object at the bucket root. Every mode validates itself against that
object at startup. So independently deployed process configurations cannot
drift apart unchecked.

`clock_skew_allowance` is not stored in `sys/gc`. It is an input to the check:

- At write time it comes from the `--clock-skew-allowance` of
  `gc-config set`.
- At maintain startup it comes from the allowance of the running sweeper
  (default 5m).

So a horizon that does not cover the sweeper's clock skew can neither be
written nor run against. [Deletion and garbage collection](../../deletion-and-gc.md)
has the argument.

### Bootstrap on a fresh bucket

**Bootstrap never blocks a fresh deployment under a credential that can
create the object.** The first such process to touch a fresh bucket writes
`sys/gc` from the maintain defaults, which satisfy the constraint by
construction. It then validates against the object that it wrote. If several
processes start together against one empty bucket, one wins the create. The
others read and validate against the object of the winner.

Under the per-role storage credentials, only the Maintain and Admin roles can
create the object. So on a fresh bucket, start the `maintain` process first,
or create the object with `ravel-cli gc-config set` under Admin. See
[the first deployment](deployment.md#the-first-deployment-against-a-fresh-bucket).

With per-role credential Secrets, the Kubernetes operator does this:

| Maintain | Operator behaviour on a fresh cluster |
|---|---|
| Enabled | It applies the maintain Deployment first. It holds the gateway and query Deployments until maintain reports a ready replica. No manual step is needed. |
| `maintain.enabled: false` | It still applies the gateway and query Deployments. Their pods restart until `sys/gc` exists, and the cluster reports `Degraded=True` until you create the object under Admin. |

The Kubernetes guide describes the conditions that the operator records while
it waits.

### What each mode validates

- `maintain`: its configured protection horizon and grace must **equal** the
  stored values. They are must-match values, not independent settings. A flag
  value that satisfies the inequality but differs from the durable value
  still refuses to start.
- Query-serving modes (`query`, `all`): the engine deadline must be less than
  or equal to the stored `max_query_duration`. The HEAD cache TTL that the
  catalog runs on must be less than or equal to the stored `head_cache_ttl`.
  - A format version 1 `sys/gc` records no `head_cache_ttl`, and the compiled
    default (30 s) applies.
  - `gc-config set --head-cache-ttl` writes format version 2, which records
    one. The
    [maintenance guide](maintenance.md#upgrading-sysgc-to-format-version-2)
    gives the upgrade order.
- Flight SQL, in a build that has it: the ticket time-to-live ceiling must be
  less than or equal to `protection_horizon - grace`. The server reads that
  ceiling from `sys/gc` and not from a compiled-in default, so it tracks the
  durable object automatically.

### Flags and change order

Each value has a `ravel-server` flag. Each flag takes a humantime duration
and defaults to its shipped value.

- `--gc-protection-horizon` and `--gc-grace` feed the maintain compactor and
  must **equal** the durable values. Set them to what the last
  `gc-config set` wrote.
- `--gc-max-query-duration` sets the enforced deadline for every query engine
  that the process builds. It must stay at or below the durable
  `max_query_duration` (default 1h). A value above it is rejected at startup,
  never clamped down.
- `--gc-max-flush-lifetime` sets the flush lifetime of the compactor, which is
  the seal margin, the orphan age gate, and the retention floor. It is not in
  the must-match set, but it has its own floor.
  - The floor is the compiled-in `max_flush_lifetime` of the ingest pipeline.
    It is fixed at 1h, and no flag changes it.
  - The process refuses to start with a value below the floor, and
    `gc-config set` refuses to write one.
  - A lower value calls a bucket sealed before the flush interlock of a real
    writer has elapsed. The erasure completion gate can then report a pending
    erasure request complete while a flush that can still publish into that
    bucket is in flight.

Each flag feeds both the startup validation and the real compactor or query
engine, so a value that passes validation is the value enforced.

Because of the must-match rule, a horizon change is not a rolling
configuration change:

1. Change the durable object.
2. Bring the flags of every process into line.

A process started against the old value refuses to start. It does not run
with the old value.

```sh
ravel-cli gc-config show
ravel-cli gc-config set --protection-horizon 25h5m --grace 24h \
  --max-query-duration 1h --max-flush-lifetime 1h
```

`gc-config set` is the single mutation path:

- It enforces the inequality at write time. It refuses a violating proposal
  and writes nothing.
- It swaps the object with a compare-and-set. So a concurrent `gc-config set`
  is a reported conflict, not a silent overwrite.
- Every value must be strictly positive. An all-zero configuration satisfies
  the inequality trivially and no mode can match it, so it is rejected.

The Kubernetes operator carries a `spec.gc` block with `protectionHorizon`
and `grace`:

| Bucket | What to do |
|---|---|
| Fresh bucket, shared credential | Nothing. The first pod bootstraps `sys/gc` from the shipped defaults and every pod validates trivially. |
| Fresh bucket, per-role credential Secrets | Nothing. The operator applies the maintain Deployment first and its pod creates the object. |
| Stored protection horizon or grace set to a non-default value with `gc-config set` | Set `spec.gc.protectionHorizon` and `spec.gc.grace` to the stored values. Read them with `ravel-cli gc-config show`. The operator renders `--gc-protection-horizon` and `--gc-grace` onto the maintain pods, so they satisfy the must-match rule and start. |

Leave the block, or either field, unset to keep the shipped default. The
other two stored values do not affect startup.

### Age-based retention

Age-based retention is off by default. It is a separate concept from the GC
safety horizons above.

| Flag | Sets |
|---|---|
| `--retention-default <duration>` | The window applied to every tenant with no override. |
| `--retention-tenant TENANT=DURATION` | The window for one tenant, which overrides the default. |

Both flags take a humantime duration (`30d`, `720h`). With neither set,
nothing is age-deleted.

- Startup validates a window against a floor of
  `max_ingest_lag + max_flush_lifetime + clock_skew_allowance` plus one
  bucket span. So a bucket can never be tombstoned before it is sealed.
- A window below the floor fails startup. It is not clamped up to the floor.
- Both flags are read only in `--mode maintain`. On a process that runs no
  maintenance loop, they configure nothing.

Query-audit records have their own window, independent of tenant data
retention. `--audit-retention <duration>` sets the age past which the
maintenance loop deletes a query-audit record. The age is measured from the
newest event that the record logs. The default is `90d`. Set it to your audit
retention obligation.

- `0` keeps every query-audit record forever.
- Any nonzero window is accepted. Every flush writes its own immutable
  record, and the sweep deletes a record only after every event in it is
  older than the window. So a short window never deletes an event younger
  than itself.
- A record is also kept until it is past the protection horizon. So a window
  shorter than the horizon behaves as the horizon.
- A legal hold that covers the query-audit shard blocks the delete, whatever
  the window.
- The flag takes effect only in `--mode maintain`, as the tenant retention
  flags do. An unparseable value fails startup in every mode.

## Tenancy setup

Repeated `--tenant-token TOKEN=TENANT` flags configure tenants completely.
Ravel has no tenant database and no admin API. To add, remove or rotate a
token, restart with a different flag set. Every process is stateless, so a
restart has no data migration to do.

With no `--tenant-token`, no `--tenant-token-file`, and no OIDC or mTLS
resolver configured, every request to a tenant-protected route is rejected.
The health and `/metrics` routes carry no tenant.
`--dev-insecure-tenant-header` on a loopback listener is the development
exception.

Tenant identity affects only key prefixing and authorization. It carries no
other per-tenant configuration.

### Token file

`--tenant-token-file PATH` is a file-based alternative to repeated
`--tenant-token` flags, so that a token never sits in argv or a process
listing. The environment variable `RAVEL_TENANT_TOKEN_FILE` carries the path
only, never a token value.

- The file has one `TOKEN=TENANT` pair per line.
- Blank lines and `#` comments are skipped.
- Each line is split on the first `=`, as `--tenant-token` is.
- A leading UTF-8 byte order mark is stripped before parsing.

`--tenant-token` and `--tenant-token-file` are mutually exclusive. Startup
refuses if both are set.

An empty or comment-only file parses to an empty map, the same as no
`--tenant-token` at all. That authenticates nothing. Unless
`--maintain-tenant` names tenants, background fold, compaction and retention
then widen to every tenant that storage discovers, and startup does not
refuse. A Secret mount that failed to populate produces this state, with no
error at startup.

### The ddl capability

A `TENANT` that ends in `;ddl` grants that token the `ddl` capability. The
tenant is the text before the LAST `;`. The capability is absent by default.
Only `CREATE EXTERNAL TABLE` and `DROP TABLE` over `POST /api/v1/sql` read
it.

- Any other suffix, or an empty tenant before the `;`, refuses startup. The
  error names the flag position or the line number in the token file, never
  the text of the pair.
- A tenant with no `;` is unchanged and never carries the capability.
- [Background](#background) links the decision behind this.

`ravel-ingest-router` accepts the same `--tenant-token` spelling and strips
the `;ddl` suffix, so it routes `acme;ddl` by the tenant `acme`. It never
grants the capability.

Tokens in the durable `sys/auth` map cannot carry `ddl`. The map is read
without suffix parsing, so an entry written as `acme;ddl` names a tenant
literally called `acme;ddl`, with no capability and no error. Grant `ddl`
only through `--tenant-token`, `--tenant-token-file` or `--oidc-ddl-claim`.

### Production authentication

Two additive resolvers join the same first-success chain. They do not disable
the bearer resolver, which stays the local and development path.

**OIDC.**

- Set `--oidc-issuer` and `--oidc-jwks-url` together. One without the other
  refuses to start.
- Set at least one `--oidc-audience`. OIDC with none set fails startup.
  Without an audience, any correctly signed unexpired token from that issuer
  authenticates, whatever relying party it was minted for.
- The bearer token of every request is verified against the key set of the
  issuer: signature, issuer, expiry and audience.
- The signature algorithm is pinned from the key that verifies the token,
  never from the token's own header. So `alg: none` and algorithm-confusion
  tokens are rejected.
- A symmetric key in the key set is rejected. A key set is a public document,
  and a symmetric key inside one is a published verification secret.
- The tenant is read from `--oidc-tenant-claim` (default `tenant`) as a
  string, with no fallback to any other claim.
- `--oidc-ddl-claim <CLAIM>` names a second, optional claim that grants the
  same `ddl` capability as the `;ddl` tenant-token suffix. The capability is
  present only when the verified token carries that claim as the JSON boolean
  `true`. A string, a number, an array, or a missing claim never grants it.
  Unset (the default), OIDC never grants the capability.
- The key set is cached in memory and refreshed on
  `--oidc-jwks-refresh-interval-secs`, so the request path never makes a
  network call. A timeout bounds the fetch, so a stalled host cannot block
  the refresh loop or the readiness gate.
- The first fetch must succeed before the server reports ready.
- A plaintext `http://` key-set URL to a non-loopback host is refused at
  startup. That response is the whole trust root for verification, and a
  plaintext fetch lets anyone on the path substitute their own keys.

**mTLS, forwarded by a proxy.** Ravel does not terminate TLS or verify client
certificates itself. `--mtls-enabled` reads a header that a TLS-terminating
reverse proxy sets to the already-verified certificate CN or SAN. The header
is `x-ravel-client-cert-cn` by default, and `--mtls-header` overrides it.

This is a forwarded-header trust boundary. The header is authoritative only
because a trusted hop set it, and anyone can forge it if that hop is absent.

- The resolver is installed on its own dedicated listener and nowhere else.
  So `--mtls-enabled` requires `--mtls-listener <addr>` and refuses to start
  without it.
- The public HTTP and gRPC listeners never consult the header.
- The mTLS address must differ from every other listener address. Startup
  checks this.
- Put the verifying proxy in front of the mTLS listener only. Configure the
  proxy to strip or overwrite any client-supplied value of the header before
  it forwards.
- Startup also refuses a `--mtls-listener` bound to the same address as a
  `--listen-http` that has `--dev-insecure-tenant-header` set. So the mTLS
  surface cannot inherit the development bypass.
- With mTLS enabled, startup logs a warning that names the trusted header.

`--mtls-listener` must bind a loopback address unless
`--mtls-trust-forwarded-header` is also passed.

- On loopback, the topology proves that only a local proxy can supply the
  header. On any other address, trust in the header depends on a proxy that
  Ravel cannot see.
- The flag turns nothing on and grants the resolver no trust that it did not
  already have. It records that you chose the non-loopback bind and have a
  verifying proxy in front of it.
- A proxy-fronted mTLS listener bound to anything other than loopback
  (`0.0.0.0:9443`, a pod IP, a host address) fails startup without the flag.
  The message names the address and the flag. Add
  `--mtls-trust-forwarded-header` to the argument vector to keep such a
  deployment starting. Nothing else about the deployment changes.
- A loopback-bound mTLS listener is unaffected.

Dependent flags fail fast. Each of these combinations refuses to start:

- `--oidc-tenant-claim`, `--oidc-ddl-claim`, or `--oidc-audience` without
  OIDC enabled
- `--mtls-header` or `--mtls-listener` without `--mtls-enabled`
- `--mtls-enabled` without `--mtls-listener`
- `--mtls-trust-forwarded-header` without `--mtls-listener`

### Tenant hash scheme

The tenant hash scheme is permanent for a bucket. The object-key prefix for a
tenant is a hash of the tenant id. A `sys/tenancy` marker pins the scheme for
the bucket when the bucket is first used. One binary carries both schemes and
selects one at startup:

- **v1 unkeyed**: a plain hash of the tenant id. Tenant names are not in
  keys, but anyone with list access can confirm a guessed tenant id offline.
- **v2 keyed**, the default for new buckets: the prefix is keyed by a 32-byte
  deployment key loaded from `--tenant-hash-key-file`. It is a file, never an
  inline value, so the secret never appears in a process listing. Without the
  key, prefixes reveal nothing about which tenants exist.

Startup pinning:

- A fresh bucket refuses to start with no key unless `--tenant-hash-unkeyed`
  is passed explicitly. Keyed is the default and the choice is permanent.
- An existing keyed bucket refuses to start when the fingerprint of the
  configured key disagrees with the marker. A wrong key is a failed deploy,
  and it does not create a parallel namespace.
  `ravel-cli tenancy show --tenant-hash-key-file <path>` verifies a key
  against a bucket offline.
- A bucket with data and no marker is adopted as v1 unkeyed once. The
  adoption is logged and counted at `/metrics` as
  `ravel_tenancy_v1_unkeyed_adoptions_total`. Its existing prefixes are
  unchanged.

**Key custody.** For a keyed bucket, the deployment key is durable state that
lives outside the object store. If you lose it, every tenant prefix is
unattributable. Bucket plus key is always enough to recover the full mapping
from tenant id to prefix, through the per-tenant recovery manifests under
`sys/t/`. The bucket alone reveals nothing.

No migration between the two schemes is possible. A move between them
relocates every object and is not built. To change schemes, start a new
bucket and drain into it.

## Durable shard count

`--shards` is the default shard count for tenants that are not yet
provisioned. You cannot change the shard count of existing data with it. The
shard count of generation 0 is fixed permanently after the data of a tenant
for a signal is written across it.

The flag sets both the shard count of the ingest router and the shard count
of the query-side catalog for new tenants. So the query side has no separate
flag.

The first write for a tenant and signal records its shard count as
generation 0 of a durable, append-only shard-generation history. The history
is in a provisioning record at `t/<tenant_hash>/<signal>/prov`. Every later
ingest, query, and maintenance touch reads that history. It routes each hour
over the shard count that is active for that hour, not over a single fixed
count.

`ravel-cli provision reshard` appends a new generation with a different shard
count. The generation takes effect at a future activation hour, and existing
data is not moved or re-keyed. Always route from the persisted generation
history: the count of generation 0 alone misses any later reshard.

| Tenant | Behaviour |
|---|---|
| Already provisioned | It keeps its own generation history. A change to the global `--shards` default (for example, lowering it for new tenants) does not affect it. Startup does not refuse, its queries do not fail, and its maintenance is not skipped. The drift between the generation-0 recorded count and the live default is expected. It is reported as an informational metric, not an error. |
| New, with no prior writes | It has no record yet, so a fresh deployment starts normally. This includes an operator-managed cluster that starts with zero data and configured tokens. The first write of the tenant creates the record and pins the live `--shards` default as the count of that tenant. |
| Its record has a shard count that hides existing data if adopted | Refused. It fails closed. |
| Its record is unreadable (corrupt or future-format), so its true shard count cannot be trusted | Refused. It fails closed. |

**Adopt data written before the record existed.** A tenant and signal that
already had data is adopted the first time a server ingests or maintains it.
You can also adopt it ahead of a rollout:

```sh
ravel-cli provision adopt --tenant <name> --shards <n>
```

Adoption writes the record only when every observed shard index is below
`--shards`. If any observed index is at or above it, adoption refuses and
writes nothing, because that value hides data.

Run `provision adopt` before you roll out a version that enforces the record.
A refusal then shows as a CLI error that you can act on, and not as a server
that does not start mid-rollout.

## Logs fetch policy and store cost profile

For each object, the logs read path either fetches the whole object in one
request or fetches only the projected byte ranges. Three flags size this
choice, and all three are read at startup only.

On an intra-region S3 deployment, transfer is free and the bill is requests.
A ranged read there spends a billed request to save bytes that cost nothing.
Elsewhere the reverse holds.

`--logs-fetch-policy` takes one of four values, spelled as in the table.
Unset, it resolves `cost-based` on every deployment, including a `--store s3`
deployment against a loopback `--s3-endpoint`.

| Value | Optimizes for | Pick it when |
|---|---|---|
| `request-minimal` | Fewest object-store requests. An object at or under the fetch bound is read whole in one covering request with no footer probe; a larger object is read as covering sub-range requests. | The backend bills requests and not transfer, so a saved request is a saved dollar and the bytes it costs are free. |
| `byte-minimal` | Fewest transferred bytes. Ranged reads wherever they save more bytes than a request is worth. | The backend bills egress, or the network is the constraint, so moved bytes are the cost that matters. |
| `cost-based` | Whichever of the two is cheaper under the active store cost profile, resolved at startup from the profile's prices and its measured request timings. | You want the shape the deployment's own prices and timings imply. At the reference intra-region profile, on every store including a loopback one, a request costs 6,300,000 bytes (its time term), so a projection that skips more than 18,900,000 bytes of an object reads ranged and every object of 18,900,000 bytes or less reads whole. |
| `latency-first` | Fewest transferred bytes, as `byte-minimal`. It states an intent: spend requests to save wall time. The concurrency that you configure decides how. | Cold wall-clock matters more than the request bill, and you will raise the object-store GET concurrency and the SQL scan width explicitly. See [the latency-first trade](#the-latency-first-trade). |

For any policy value a query returns the same rows. Only request counts and
timing differ.

The policy is an operator setting only. It is never derived from query text,
a header or a ticket. Under request billing, a tenant that can force
`byte-minimal` per query multiplies the request bill of the deployment by the
measured amplification factor. The running engine also never changes its own
policy. If a measurement shows that the default is wrong for a deployment,
set `--logs-fetch-policy` explicitly.

Startup logs the resolved policy and its source on the
`logs fetch policy resolved` line. The source is `flag` (explicit) or
`default` (unset). Unset always means `default`/`cost-based`, on every store
including a loopback one.

### The latency-first trade

`latency-first` against the default policy, as measured:

| Item | Figure |
|---|---|
| Conditions | 3 reps on a 42-statement reference corpus, true cold in the warm-up-empty state, at GET concurrency 256 |
| GET requests | 5.30x (570,752 against 107,781) |
| Cold time | 52% less, with a per-rep range of 50.3% to 54.2% |

That ratio is a measurement of two code paths on one build, not a property of
the policy. The decision record for the fetch objective names the build that
it was taken on.
Measure again on the build that you run, and do not treat the ratio as a
constant.

- `latency-first` resolves `--store-get-concurrency`,
  `--sql-partition-count`, and `--promql-fetch-fanout` as every other policy
  does. It sets no default of its own.
- The measured trade pays off only after you raise the GET permits and the
  SQL scan width together to the concurrency that the measurement used.
  `--fetch-concurrency` raises all three at once.
- The policy alone is not inert. The byte quantities change immediately, so a
  logs read is routed as `byte-minimal` routes it. It takes ranged reads
  where they save more bytes than a request costs, and whole-object reads
  where they do not.
- On the reference corpus against real S3, that shape at the default
  concurrency measured slower than the default policy.
- Treat the concurrency as a precondition. The startup line says which side
  of it this process is on. It reports the precondition met only when both
  the GET permits and the scan width are raised.
- A concurrency raise also raises in-flight fetch memory, and no process-wide
  budget bounds that memory yet. Watch process memory when you try this
  policy. An under-provisioned raise can end in an out-of-memory kill.

### The store cost profile

`--store-cost-profile <path>` names a TOML file with the object-store prices
of this deployment and, optionally, two request timings measured from its
hosts.

- The file is read only to resolve `cost-based`. No price reaches the fetch
  layer, which runs on byte quantities alone.
- `ravel-bench` reads the same file, so the engine and the ledger price a run
  the same way.
- Omitted, the reference profile `s3-intra-region-2026` is used.

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

**Prices.** Prices are integer nanodollars, never floats, because they are
exact decimal contract figures. The reference values model S3 standard
intra-region 2026 list prices: PUT class $5.00 per million requests, GET
class $0.40 per million, transfer and retrieval free. One PUT costs 12.5 GETs
at those prices. Every price is a modeled figure under a named profile, not a
billed amount. The same run under a different profile reprices to different
numbers.

**Timings.** The two timings are measured constants, not prices: the latency
of one request from the hosts of the deployment, and the bytes that one
connection transfers per second. They are optional, and you set both or
neither.

`timings_measured` is a free-text note of their provenance, and nothing
derives a figure from it. The reference values were measured on the reference
box of the reference suite, intra-region against the object store. The note
of the reference profile says so and records no date.

**Validation.** Every field is required except `delete_class_nanodollars` and
the three timing fields. Loading is fail-closed. Each of these refuses
startup with an error that names the flag:

- an unreadable file
- invalid TOML
- an unknown or misspelled key
- one timing without the other (the error names the missing one)
- a blank name

Ravel never falls back to the reference prices. With a fallback, a deployment
stamps one profile into its reports while it resolves its fetch policy from
another.

**How `cost-based` resolves.** It converts the profile into the one byte
quantity that the fetch layer runs on: how many transferred bytes one saved
request is worth. Two terms can answer that, and the larger one is the rate.

- The price term is what a request costs in bytes at the prices of the
  profile.
- The time term is the bytes that one connection can move during the latency
  of the request.

```
price term = get_class_nanodollars x BYTES_PER_GIB
             / (transfer_nanodollars_per_gib + retrieval_nanodollars_per_gib)
time term  = request_latency_micros x per_connection_throughput_bytes_per_s / 1,000,000
request_cost_bytes = the larger of the two terms
```

- `BYTES_PER_GIB` is 2^30. The arithmetic multiplies before it divides, in
  128-bit, so a sub-nanodollar per-byte price does not truncate to zero.
- Retrieval is a per-byte charge, as transfer is, and enters the denominator
  the same way. So a profile with free transfer but priced retrieval still
  routes byte-minimally. It does not report retrieval dollars that a
  request-minimal plan never spends.
- The result is floor-rounded, held at a minimum of one byte, and clamped to
  the coalescing-gap and routing-threshold floors.
- Two cases saturate the price term: a zero denominator, where no per-byte
  cost exists, and quotient overflow from a near-free but nonzero per-byte
  price.
- A saturated price term yields to the time term. So the rate itself
  saturates, which means "read whole always", only on a profile with neither
  per-byte prices nor timings. Startup logs that case and names the profile.
- The `rate_term` field of the startup line says which term the rate came
  from: `price`, `time` or `saturated` (or `flag` when
  `--logs-request-cost-bytes` set it).

Two worked cases:

| Profile | Result |
|---|---|
| The reference profile | Both per-byte prices are zero, so the price term saturates and the rate is the time term: 70,000 microseconds at 90,000,000 bytes per second, 6,300,000 bytes. |
| Egress list prices (GET class $0.40 per million against $0.09 per GiB transfer plus $0.01 per GiB retrieval) and no timings | It resolves to 4,294 bytes, which the floors then clamp. |

**The projection break-even.** Under `cost-based`, and only there, a finite
rate derived from the profile also sets the projection break-even. That is
the number of bytes that a narrow projection must save before it is read
ranged and not whole. It is the larger of the routing threshold
(`--logs-block-range-threshold`, 524,288 bytes by default) and three request
costs. Three, because a ranged read of a narrow projection issues about four
GETs per object against one for a whole read, so it pays only when the bytes
it skips exceed the cost of the three extra requests.

At the reference profile the break-even is 18,900,000 bytes. So a one-column
read of a 35 MB object reads its column ranges, while every object of
18,900,000 bytes or less, such as a 3 MB flush object, still reads whole. The
same figure is the object size at or below which the ranged fetch reads the
whole object anyway.

The startup line reports the break-even in force as
`projection_break_even_bytes`:

| Case | `break_even_source` | Value |
|---|---|---|
| `cost-based` with a finite rate from the profile | `break_even_source="profile"` | The break-even above. |
| The other policies, and when `--logs-request-cost-bytes` is set. These keep the routing threshold as the break-even. | `break_even_source="routing-threshold"` | The routing threshold: 524,288 bytes by default. |
| `request-minimal`, and a `cost-based` resolution whose profile prices bytes at zero and records no timings | `break_even_source="routing-threshold"` | The threshold is saturated and the line prints 18446744073709551615. |

The coalescing gap is the largest hole between two wanted ranges that one
request reads through. It stays one request cost (at least 64 KiB) under
every policy, so at the reference profile it is 6,300,000 bytes.

### Covering-read bound and precedence

`--logs-max-fetch-run-bytes` caps the length of one covering request.

- Its default is 64 MiB, and it applies under every policy.
- Zero is refused with an error, because the segmented fallback divides the
  object size by it.
- An object at or under the bound is read in one covering request.
- An object above the bound is read as sequential block-aligned covering
  sub-ranges. So no single request moves more than the bound, however large
  an object grows.

`--logs-request-cost-bytes`, when set explicitly, wins over the
policy-derived rate. So a deployment can select `cost-based` and still pin
the one derived quantity when it has measured a better value.

A saturated rate also overrides an explicitly set
`--logs-block-range-threshold`. `request-minimal`, and `cost-based` on a
profile with neither per-byte prices nor timings, saturate both routing
thresholds whatever that flag says. Startup logs a set-but-overridden
threshold, so the override is visible. In every other case that flag keeps
its normal function, including under `cost-based` at the reference profile.

### What these flags cover

The fetch policy and the cost profile govern the logs read path only. Metrics
fetching consults neither. Its suffix probe window, coalescing gap,
whole-object threshold and concurrency limit are compiled-in constants, so
metrics fetch behavior has no setting to tune.

Any report that carries a request or modeled-cost figure stamps the active
profile, all its prices, and the resolved policy. The policy is split into
what was requested and what governed the run. A lane that cannot know what
governed its fetches stamps its effective value as `n/a`, and does not echo
the request as confirmed. Two request or dollar figures are comparable only
when both are known to have priced the run the same way.

## Indexed fields and typed attribute columns

Two per-tenant declarations change query cost, and one of them also changes
the SQL schema. Decide both on day 0, because a later change means a restart
or a durable record write.

### Indexed fields

An index over named fields drives block-level pruning for an attribute
equality predicate on logs.

| Flag | Effect |
|---|---|
| `--indexed-field FIELD`, repeated | Names the fields for every tenant with no override. |
| `--indexed-field-tenant TENANT=field1,field2` | Replaces that list for one tenant. An empty list for a tenant (`--indexed-field-tenant acme=`) turns the index off for it. |

The shipped default list is `service.name`, `k8s.namespace.name` and
`http.status_code`. **Any value that you pass replaces that list. It does not
add to it.**

Indexing is opt-in per field. An unindexed field still works through the
bloom filter and the exact scan. A missing index changes query cost, not
query correctness.

### Typed attribute columns

You can declare an attribute key to promote it to a native typed column. The
column is appended after `attrs` in declaration order. The value then reads
back as an `Int64`, `Boolean`, `Dictionary(Int32, Utf8)` or `Binary` Arrow
column.

Without a declaration, the `logs` SQL table exposes every attribute through
one merged `attrs: Map(Utf8, Utf8)` column. A numeric or boolean comparison
over an attribute is then a cast over a stringified value.

A promoted key still appears in `attrs`, so `SELECT attrs` and `SELECT *`
keep working. A promoted `str` column is dictionary-encoded, and a consumer
must expect two client-visible changes:

- The column stays a dictionary over the Flight SQL wire. The Arrow IPC
  schema and batch columns carry the dictionary type verbatim.
- HTTP JSON row values are unchanged, one string per row. But the column
  type in the JSON envelope reads `Dictionary(Int32, Utf8)` and not `Utf8`.

You can declare in two ways, with one resolution order:

- `--typed-attr-column KEY:TYPE` and
  `--typed-attr-column-tenant TENANT:KEY:TYPE` are the deployment default and
  its per-tenant override. A change to them needs a restart. `TYPE` is one of
  `str`, `i64`, `bool` or `bytes`, case-insensitive. They have no shipped
  default, because a promotion changes the SQL schema that the queries of a
  tenant see.
- The durable per-tenant record, written by `ravel-cli typed-attr-column set`,
  needs no restart. When present, it replaces the flag-derived declaration
  for that tenant completely, **including when it is present and empty**. An
  empty declaration means "this tenant promotes nothing". With no override
  at all, the flags apply.

```sh
ravel-cli typed-attr-column show <tenant>
ravel-cli typed-attr-column set <tenant> http.status_code:i64 user.id:str
```

`set` replaces the whole declaration of the tenant. It is not additive and
has no per-key remove, so pass the full intended list.

`set` validates on the same rules as the flags. It rejects an empty key, a
duplicate key, the same key with two types, and a key that collides with one
of the nine fixed logs columns (`ts`, `observed_ts`, `severity_num`,
`severity_text`, `body`, `trace_id`, `span_id`, `flags`, `attrs`). It then
swaps the record with a compare-and-set, so a concurrent write is a reported
conflict and not a silent overwrite.

**Staleness.** A query-serving process reads the durable override per tenant
on a 60-second staleness horizon.

- A `set` takes effect within 60 seconds. During that window two replicas can
  answer the same query against different declarations.
- A failed read never fails a query. The process serves the last declaration
  that it resolved, or the flag-derived one if it never resolved for that
  tenant.
- A failed read is not retried for one second. So a degraded config store
  costs at most one failed request per tenant per second.
- The fallback is a real degradation, and
  `ravel_typed_attr_columns_stale_fallback_total` counts it.

**Cost note.** A predicate on a promoted column prunes blocks before decode.
An `i64` or `bool` comparison prunes through the skip index. A `str` or
`bytes` equality prunes through the same POSTINGS index that
`attrs['k'] = 'v'` uses. Promote for typed comparisons and aggregates
(`k > 5`, `SUM(k)`), which are impossible over the map. An equality that
already prunes gains nothing from promotion.

A per-object budget also limits how many distinct attribute name and type
pairs get a real column at write time. Pairs beyond the budget go into an
overflow column and lose columnar access. Watch for that in
[the observability guide](../observability.md).

## Per-query budgets

Six flags bound what one query can spend. Unset, each resolves at startup:

| Flags | Unset, resolves from |
|---|---|
| `--store-get-concurrency`, `--sql-partition-count`, and `--promql-fetch-fanout` (or the legacy `--fetch-concurrency`, which sets all three) | The core count. |
| The two SQL ceilings | Memory: shares of `MemTotal`, capped by the cgroup memory limit when the process runs in a container. |
| `--max-segments` | No host resource. It is a fixed 1,000,000 on every host. |

Set, the flag value is used verbatim, with one reconciliation. The per-query
SQL pool is clamped to an explicit per-tenant ceiling set below it, and the
startup log says so.

The reference-host column is a 16-core, 30 GB host, the shape that the
published ClickBench run used.

| Flag | Default (unset) | Reference host | Choose against |
|---|---|---|---|
| `--fetch-concurrency` | derived: `max(8, 2 x cores)` | 32 | Legacy combined knob: sets `--store-get-concurrency`, `--sql-partition-count`, and `--promql-fetch-fanout` together (source `legacy-flag`). Combining it with any of the three is a startup error naming both flags. |
| `--store-get-concurrency` | derived: `max(8, 2 x cores)` | 32 | Permit count for the one process-wide `GetLimiter` every fetcher (RSEG, RLOG, RSPAN) shares. Host cores and the store's request budget. |
| `--sql-partition-count` | derived: `max(8, 2 x cores)` | 32 | DataFusion `target_partitions` for every SQL session the server builds. Host cores and query parallelism vs. per-partition overhead. |
| `--promql-fetch-fanout` | derived: `max(8, 2 x cores)` | 32 | PromQL/analytics per-query segment fetch fan-out. Host cores and the store's request budget. |
| `--max-segments` | fixed: 1,000,000 (host-independent) | 1,000,000 | How many sealed objects a wide scan touches. Only the recent set, roughly the last two hours, is exempt, so a tenant with a lot of sealed history hits this before you expect. Lower it to bound plan width on a host you share with something else. |
| `--sql-max-query-bytes` | derived: 50% of MemTotal, 256 MiB if memory is unknown | 16,106,127,360 | Per-query SQL memory pool ceiling. Process-wide, not per-tenant. The derived value equals the tenant's whole SQL share, so a lone statement may use all of it; concurrent statements still share the per-tenant ceiling. To keep the earlier split, set this flag to half of `--sql-tenant-max-bytes`, 25% of MemTotal. Held at or below `--sql-tenant-max-bytes`: an explicit value here raises a non-explicit (derived or fallback) tenant ceiling to fit, but an explicit tenant ceiling clamps this down and warns. |
| `--sql-tenant-max-bytes` | derived: 50% of MemTotal, 1 GiB if memory is unknown | 16,106,127,360 | The multi-tenant isolation bound: SQL memory one tenant may hold across its concurrent queries. Process-wide, and not itself per-tenant-overridable. |

`--sql-max-query-bytes` and `--sql-tenant-max-bytes` are meaningful only in a
build with the `sql` feature. See
[the query guide](../query.md#operator-configurable-budgets-server-flags) for
worked sizing.

A value of `0` in any of `--fetch-concurrency`, `--store-get-concurrency`,
`--sql-partition-count`, or `--promql-fetch-fanout` is a startup error that
names that flag. Startup raises it before any fetcher, engine, or SQL session
exists.

### SQL ceilings

The two SQL ceilings derive to the same 50% share of `MemTotal`. The
per-query pool nests inside the per-tenant pool, so the per-query share does
not change the total of the tenant.

- Statements that run together share the tenant ceiling.
- A statement that arrives while another holds most of the ceiling gets what
  is left, not a reserved quarter.
- So the SQL memory of one tenant is still at most 50% of `MemTotal`.

The two caches carve the memory budget (`MemTotal` less the overhead reserve:
2 GiB from 8 GiB of memory up, scaled down below it at the default ingest
ceiling, as described below), not `MemTotal`. So the three ceilings together come to about 78% of
`MemTotal` on the reference host (25,125,558,681 of 32,212,254,720). They
come to more on a loopback store, where the fetcher cache derives at 40%.

### Catalog resolve concurrency

`--catalog-resolve-concurrency` bounds the process, not one query. It is the
ceiling on every object-store request that the catalog resolve path keeps in
flight across every concurrent query: prefix LISTs, commit-record GETs,
snapshot-part GETs, and the postings and column-stats reads that go with
them.

It derives from the same startup resolution, but from query concurrency and
not from cores or memory directly. Unset, it resolves to
`clamp(Q * 128, 128, 4096)` held at an interim 1,024.

- `Q` is `--max-concurrent-queries` when that flag bounds queries.
- `Q` is the same `max(8, 2 x cores)` that the flags above use when queries
  are unbounded.
- `--max-concurrent-queries` is the fleet-wide query ceiling, not a
  per-replica one. So with several replicas, each one sizes its resolve
  ceiling for the queries of the whole fleet and is correspondingly generous.
- 128 is what one shard-hour prefix sustains. So `Q` concurrent resolves over
  `Q` different shard-hours each get the worth of one prefix.

| Configuration | Resolves to |
|---|---|
| `--max-concurrent-queries 1` | 128 |
| `--max-concurrent-queries 4` | 512 |
| Unbounded, 8-core host | 1,024. Its `Q` of 16 derives 2,048, held at the interim cap. |

Set explicitly, the flag value is used verbatim and the interim cap does not
apply to it. `0` and any value above 4,096 are startup errors.

A second bound, which the flag does not reach, holds each individual key
prefix to 128 requests whatever this ceiling is. Every resolve-path request
is bounded this way, keyed by its own key prefix:

- a commit record by its shard-hour prefix
- the parts of a snapshot by the one directory that they share
- its postings and column stats by theirs
- a LIST by the prefix that it lists

So a higher ceiling adds breadth across prefixes and never depth within one.

### Other derived settings

Three more settings are derived the same way:

| Setting | Derived value |
|---|---|
| `--cache-max-bytes` (fetcher cache) | 25% normally, or 40% against a loopback `--s3-endpoint`. 256 MiB if memory is unknown and `--memory-budget-bytes` is unset. |
| `--catalog-cache-max-bytes` (catalog byte cache) | Always a separate 5% ceiling. 256 MiB if memory is unknown and `--memory-budget-bytes` is unset. |
| `--gc-max-query-duration` | 11 minutes. |

- Memory is read from the `MemTotal` of `/proc/meminfo` on Linux and is
  "unknown" everywhere else.
- Cores come from the available parallelism of the process, floored at 1.
- Percentages truncate.

### Process memory budget

`--cache-max-bytes` does not derive from raw `MemTotal`, as the two SQL
ceilings do. It derives from a process-wide memory budget. Where possible,
the budget starts from available memory and not from raw total.

| Host | Budget | Logged source |
|---|---|---|
| No cgroup memory limit, and a readable `MemAvailable` | `min(MemTotal - RESERVE, max(FLOOR, MemAvailable + own RSS - RESERVE))`, every subtraction saturating at zero | `derived-available` |
| A cgroup memory limit is present | That limit minus the reserve. `MemAvailable` is not consulted. | `derived-cgroup` |
| No cgroup limit, a readable `MemTotal`, and no readable `MemAvailable` (an unusual Linux kernel or container runtime whose `/proc/meminfo` parses `MemTotal` but not `MemAvailable`) | `MemTotal` minus the reserve | `derived` |
| Neither a readable `MemTotal` nor a cgroup limit (every non-Linux build, or a Linux host with no cgroup limit whose `/proc/meminfo` cannot be read) | No budget is derived: the budget is unlimited | `fallback` |
| `/proc/meminfo` cannot be read but a cgroup limit is set | The limit is the memory figure | `derived-cgroup` |
| `--memory-budget-bytes` is set | The flag value. It overrides every branch above. | `flag` |

Terms in the first row:

- `MemAvailable` is the estimate in `/proc/meminfo` on Linux of the memory
  that a new allocation can claim without swapping.
- `RESERVE` is the overhead reserve: `max(min(2 GiB, memory / 4), held)` of
  the memory the budget starts from (`MemTotal`, or the cgroup memory limit
  when one applies). `held` is what the process holds outside the budget: a
  provisional, uncalibrated 256 MiB baseline, plus the
  `--max-ingest-buffer-bytes` ceiling in `--mode all`, or 2 GiB when that flag
  is `0`, since an unbounded buffer cannot be accounted. `held` wins over the
  2 GiB cap, so the budget, the ingest ceiling and the baseline fit in memory
  at every bounded ceiling.
- At an ingest ceiling of 1.75 GiB or less, the reserve is a fixed 2 GiB from
  8 GiB of memory up. A larger ceiling makes the reserve the ceiling plus
  256 MiB at every size: at `--max-ingest-buffer-bytes 3221225472` it is
  3.25 GiB, so an 8 GiB host derives a budget of at most 4,864 MiB.
- Below 8 GiB, a t3a.small (`MemTotal` 1,912 MiB) reserves 768 MiB in
  `--mode all` at the default 512 MiB ingest ceiling, for a budget of at most
  1,144 MiB. In `--mode query` and `--mode maintain` it reserves 478 MiB, a
  quarter, for a budget of at most 1,434 MiB.
- `FLOOR` is a 1 GiB floor under the `MemAvailable + own RSS - RESERVE` term
  only, not under the final budget. When the floor binds, startup logs at
  `WARN` with the `MemAvailable` reading that hit it, and names
  `--memory-budget-bytes` as the remedy.
- The outer `min` against `MemTotal - RESERVE` still applies after the floor,
  and keeps the final budget at or below that figure.
- A derived budget below 256 MiB refuses to start. The message names
  `MemTotal` (or the cgroup limit), the reserve, the budget and
  `--memory-budget-bytes`, and `--max-ingest-buffer-bytes` when the ingest
  ceiling set the reserve. A host or container needs 512 MiB of memory to
  clear it in `--mode query` or `--mode maintain`. In `--mode all` it needs a
  bounded ingest ceiling plus 512 MiB: 1 GiB at the default ingest ceiling.
- The resident set of the process counts as available. The kernel does not
  call the resident pages of a process "available", but this process can
  reuse them and does not compete with them.

A cgroup limit is already the whole share of this process, so a whole-host
`MemAvailable` is the wrong figure to consult there.

`--memory-budget-bytes` still goes through the same startup refusal as a
derived budget (below). Use it on a host where this process shares memory
with another process that it cannot see. The available-memory derivation
reads `MemAvailable` once at startup and cannot anticipate a sibling process
that claims memory afterward.

What the budget feeds:

- The part of the budget that the two resolved cache ceilings do not claim
  sizes a shared memory accountant. The per-tenant tracking of the SQL
  executor reserves against that accountant. So a higher `--cache-max-bytes`
  on a memory-constrained host leaves less headroom for concurrent SQL
  queries, although separate flags configure the two.
- A derived (not explicit-flag) `--sql-max-query-bytes` or
  `--sql-tenant-max-bytes` is also held at or below 90% of that remainder.
  The two SQL ceilings derive from raw `MemTotal`, so without the cap they
  can exceed what the budget leaves after the caches are carved out. An
  explicit flag on either is never capped this way.

The 90% cap binds in these cases:

- Every deployment whose store is on loopback. There the 40% fetch-cache
  share leaves a remainder whose 90% is below 50% of `MemTotal`.
- An S3 deployment whose effective memory is below about 9.7 GiB, cgroup pods
  included.
- A host with co-resident processes where available memory is well below
  total.

The budget, its two cache carves, the SQL cap, and the remainder are computed
once at startup from the host profile observed at that moment. Nothing about
them changes while the process runs. A container whose cgroup limit or
available memory changes later is not noticed until the next restart.

Startup refuses when the two cache ceilings leave no strictly positive
remainder, and the error names both figures. `--disable-cache` is exempt: a
process that builds neither cache claims nothing against the budget, and the
remainder is all of it.

Every mode except gateway (`all`, `query`, `maintain`) needs a derived budget
of at least 256 MiB after the overhead reserve, plus room for what its two
cache ceilings claim.

### Memory admission wait

A query that arrives while the shared accountant is nearly full waits at
admission instead of starting and failing its first fetch reservation. The
wait applies to PromQL, metadata, SQL and Flight SQL queries (both the
`GetFlightInfo` and the `DoGet` call):

- While the reserved bytes are at or above
  `--query-memory-admission-fraction` times the accountant's limit, the
  query waits and re-checks every 10 ms.
- It is admitted as soon as the reserved bytes drop below that threshold.
  The time it waited comes out of its deadline.
- After 2 s of waiting, or after half its deadline if that comes first, it
  is admitted anyway, and its own reservations decide as they would with no
  wait. The wait never refuses a query, but a query that needed nearly all
  of its deadline can time out after waiting.

A query that is already running never waits: a reservation it makes while
running is refused at once when it does not fit, as before. The internal
slice fetches of a distributed scan do not wait either.

Waiting queries are not queued. When the reserved bytes drop below the
threshold, every waiting query whose re-check lands before the bytes rise
again is admitted, so several can start together and still fail their
first fetch with "query memory budget exhausted: the process could not
reserve memory to fetch segment data; retry".

The fixed 2 s cap and that fetch refusal's 503 status are interim: an
accepted design makes the wait configurable and answers memory refusals
with 422, and is not applied yet.

The fraction defaults to `0.75` and accepts values from `0` to `1`. `0`
disables the wait. An unlimited accountant (unreadable host memory) never
waits. The default leaves a quarter of the budget for the queries already
admitted to fetch into. The startup log reports the outcome on a `performance default resolved`
line:

```
INFO performance default resolved setting="query_memory_admission_fraction" value=0.75 source="derived" threshold_bytes=11940000000 enabled=true
```

`threshold_bytes` is the reserved byte count at which queries start to
wait, and is `0` with `enabled=false` when the wait is off.

Raise the fraction when queries wait while plenty of the budget is free.
Lower it when running queries still fail with "query memory budget
exhausted" at their fetches: a lower threshold leaves each admitted query
more headroom. Both counters below show which case applies.

### Gateway mode memory

`--mode gateway` derives no budget. It builds no query surface and runs no
fold, so nothing in it reads through either cache or reserves against the
accountant.

- No overhead reserve is subtracted for it. So it starts under any cgroup
  memory limit, including one of 2 GiB or less.
- Every mode that buffers ingest (`all` and `gateway`) holds its ingest
  buffers outside the memory budget, bounded by `--max-ingest-buffer-bytes`
  (512 MiB by default), plus allocator and runtime overhead. In `--mode all`
  the overhead reserve covers that bound. A gateway reserves nothing, so size
  its limit above it.
- On a small host, lowering `--max-ingest-buffer-bytes` is the lever in both
  modes: it lowers what a gateway's limit must hold, and in `--mode all` it
  lowers the reserve and leaves a larger budget.
- Its startup log prints one line that says the memory budget is not
  applicable in gateway mode, and its `ravel_memory_budget_bytes` reads
  `u64::MAX`.
- Unless `--catalog-cache-max-bytes` is set, a gateway builds no catalog byte
  cache. Its `/metrics` then carries no `cache="catalog"` series for
  `ravel_cache_hits_total`, `ravel_cache_misses_total`,
  `ravel_cache_resident_entries`, `ravel_cache_resident_bytes` or
  `ravel_cache_max_bytes`.
- Unless `--cache-max-bytes` is set, a gateway builds no fetcher cache, and
  under `--cache-dir` no disk tier for it. Its `/metrics` then carries no
  `cache="fetch"` series for any of those families.
- With neither flag set, no `ravel_cache_*` family renders at all.

In any mode, a `--cache-max-bytes` of `0` builds no fetcher cache.

### Memory metrics

The current state is visible live at `/metrics`:

| Metric | Meaning |
|---|---|
| `ravel_memory_budget_bytes` | The ceiling of the shared accountant. It is the `memory_remainder_bytes` of the startup log: the budget MINUS the two cache ceilings. It is not the pre-carve `memory_budget_bytes` figure logged beside it. `u64::MAX` means unlimited, which is what any host with unreadable memory reports regardless of the caps set on it. |
| `ravel_memory_reserved_bytes` | The reserved total, split by a `component` label. `fetch` is the bytes held by fetch reservations on the PromQL and SQL paths, including distributed fragment slices and the startup cache warm pass. `sql` is the rest of the reserved total: what the per-tenant accountants of the SQL executor hold. |
| `ravel_memory_handoff_overlap_bytes` | The part of the `fetch` share whose bytes went through the read cache, hit or miss, whether or not the cache kept them. `0` when no read cache is configured. |
| `ravel_memory_admission_waits_total` | Admissions that had to wait for the reserved bytes to drop below the [memory admission wait](#memory-admission-wait) threshold. A Flight SQL statement that waits at both `GetFlightInfo` and `DoGet` counts twice. |
| `ravel_memory_admission_waits_expired_total` | The waits that reached 2 s, or half the query's deadline, with the reserved bytes still at or above the threshold. Those queries were admitted anyway. A subset of the waits counter. |

`--cache-max-bytes` has a limited effect on how many times a logs statement
moves the bytes of a given object. The plan-phase whole-object read of a
query (the `has_word`/text and other skip-index-undecidable fallback) is
carried into the scan for a bounded number of segments, whatever the cache
size. Those objects cross the wire once. The bound is the SQL partition count
times object size, not the corpus. So an undersized `--cache-max-bytes` can
still turn the one wire GET of the remaining segments into two.

### Resolved values at startup

Every resolved value is logged once at startup with the source that it came
from:

| Source | Meaning |
|---|---|
| `flag` | The operator set it, and it is used verbatim. On `memory_budget_bytes`, it means that `--memory-budget-bytes` won over every derivation branch. A `flag` or `fallback` budget subtracts no overhead reserve, so the `memory_overhead_reserve_bytes` line is printed only on a derived source. |
| `legacy-flag` | No flag for this setting, but the legacy `--fetch-concurrency` was set and its value is used. |
| `derived` | Computed from the host profile, or from a host-independent rule. |
| `derived-available` | `memory_budget_bytes` only: no cgroup limit, derived from `MemAvailable`. |
| `derived-cgroup` | `memory_budget_bytes` only: a cgroup memory limit is present, so the budget is that limit minus the reserve, ignoring `MemAvailable`. |
| `budget-carve` | A fixed share of `memory_budget_bytes` and not of raw `MemTotal`. The two cache ceilings resolve to this on a host whose memory could be read or whose budget was set with `--memory-budget-bytes`. |
| `budget-carve-loopback` | The larger 40% share of the fetcher cache. It is resolved in place of `budget-carve` when the store is `s3` against a loopback endpoint and `--cache-max-bytes` is unset. |
| `fallback` | No flag and no readable `MemTotal`, so the compiled-in constant is used. |

To see what a process runs with, without reading the unit file, run
`journalctl -u ravel-server | grep 'performance default resolved'`:

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

In this example, `source="derived"` on `memory_budget_bytes` means that
`MemTotal` was readable on this host but `MemAvailable` was not. The budget
is `MemTotal` minus the reserve.
[Process memory budget](#process-memory-budget) lists the other sources. A
`fallback` budget is unlimited, regardless of any cache or SQL caps set on
the host.

For the `derived-available` case,
[`docs/internal/clickbench.md`](../../internal/clickbench.md#deriving-the-reference-sizes)
has a worked example from a real measured host, including the SQL-pool cap at
90% of the remainder.

When `MemAvailable` plus the resident set of this process puts the budget
below the 1 GiB floor, one extra line appears inside the block above. It
comes after the `memory_remainder_bytes` line and before the two SQL pool
lines:

```
WARN memory_budget_bytes was held at MEMORY_BUDGET_FLOOR_BYTES: MemAvailable plus this process's own resident set left little or no room after the overhead reserve, most likely a co-resident process claiming most of the host; the subsequent min against MemTotal less the overhead reserve can still clip memory_budget_bytes below this floor on a small host; set --memory-budget-bytes to size the budget explicitly memory_budget_bytes=1073741824 mem_available_bytes=2147483648 own_rss_bytes=0
```

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
