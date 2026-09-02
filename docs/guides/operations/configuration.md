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
| `all` | OTLP ingest, the query API, the catalog fold, alert evaluation. No maintenance. |
| `gateway` | OTLP ingest and the catalog fold. |
| `query` | The query API and the catalog fold. |
| `maintain` | Compaction, retention, the sweeper and the at-rest scrubber. No ingest, no query API, no catalog fold. It still binds `--listen-http` for liveness, and it needs a backend that reports the `multipart` capability. |

The catalog fold runs in every mode except `maintain`. Every maintenance loop
runs only in `maintain`. A deployment made of `all` processes alone therefore
folds its catalog but never compacts, never applies retention and never deletes
an object. Read the maintenance page before you decide you do not need a
`maintain` process.

## Storage backend and credentials

`--store memory` is an in-process store for tests and local experiments.
Nothing survives process exit. `--store s3` is the only durable choice.

Ravel does not use the AWS credential chain (profiles, `AWS_ACCESS_KEY_ID`,
`~/.aws/config`). It reads the `RAVEL_S3_*` environment variables and their
matching flags, and nothing else. `allow_http` and `force_path_style` are not
configurable: the client enables `allow_http` when `--s3-endpoint` is set, and
always uses path-style addressing.

MinIO, for local development:

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

## Storage credential roles

Every Ravel process holds one S3 credential and uses it for every object-store
call it makes. With a single bucket-wide credential, a leak from any one process
can read, overwrite or delete anything in the bucket. Scoping the credential to
the job the process actually does means a leaked credential can only do what
that job legitimately does, and only one of the four can delete anything.

This is enforced entirely at the storage backend's own policy layer (AWS IAM, or
MinIO policies for development and CI). Ravel's code is unchanged by it: there
is no in-process authorization check and no change to the `RAVEL_S3_*` contract.
You provision a narrower credential per role and attach the policy.

Using one credential for everything is still supported, and it is the right
choice for a development or single-operator deployment.

### The four roles

| Role | Process | What it does |
|---|---|---|
| Gateway | `--mode gateway`, and the ingest half of `--mode all` | Writes L0 segments and their commit records, idempotency markers, and a tenant's provisioning record on adopt. Runs the catalog fold, so it also writes catalog snapshot parts, `HEAD`, and name-postings objects. |
| Query | `--mode query`, and the query half of `--mode all` | Lists and reads commit records, catalog objects and segment data. Runs the catalog fold too, and appends query-audit records. |
| Maintain | `--mode maintain` | Compaction, retention and the sweeper. The only role that may delete anything, and only under the L0, L1, commit and idempotency prefixes plus the query-audit shard. |
| Admin | `ravel-cli` | One-off bootstrap and mutation commands. Invoked by an operator or a CI job, never by a long-running server. The broadest of the four. See [the Admin credential](deployment.md#the-admin-credential). |

Gateway and Query both run the catalog fold, which is why both hold the same
catalog write grants. That is not an artifact of the split; it is what the
shipped topology does.

### The shipped policy documents

One policy document per role lives in [`deploy/iam/`](../../../deploy/iam/)
rather than being transcribed here, so a policy edit is a diff that a test
checks against the real object-key layout in CI. Replace `my-ravel-bucket` with
your bucket in each file, then attach each document to the principal whose
access key that role's deployment uses.

Three facts about those documents are worth knowing before you edit them.

**Every role denies delete on the protected prefixes.** Gateway, Query and Admin
have no delete grant at all, and they still carry the same explicit `Deny` on
`s3:DeleteObject` and `s3:DeleteObjectVersion` over the protected control
prefixes. An explicit `Deny` overrides any `Allow`, so those prefixes are
undeletable even by Maintain.

**The audit prefix has two shards that are treated differently.** The legal-hold
shard (`t/*/u/*/0000/*`) is deny-delete for every role including Maintain, so a
legal hold cannot be destroyed. The query-audit shard (`t/*/u/*/0001/*`) is
compacted and age-swept on a 90-day window by the Maintain process, so Maintain
alone grants delete on it. The two are disjoint key paths, so neither grant
reaches the other shard.

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

### MinIO

MinIO's policy language is the same JSON, verbatim: the same actions, the same
`arn:aws:s3:::<bucket>/<prefix>` resources, the same explicit `Deny` semantics.
Load the files directly:

```sh
# one policy document per role, straight from deploy/iam/
mc admin policy create myminio ravel-gateway  deploy/iam/gateway.json
mc admin policy create myminio ravel-query    deploy/iam/query.json
mc admin policy create myminio ravel-maintain deploy/iam/maintain.json
mc admin policy create myminio ravel-admin    deploy/iam/admin.json

# one MinIO user per role, each attached to its policy
mc admin user add myminio gateway-key  gateway-secret
mc admin policy attach myminio ravel-gateway --user gateway-key
# ...repeat for query, maintain, admin
```

The local kind environment provisions MinIO with a single shared credential
across all pods, deliberately. The per-role split is a production hardening and
the development environment does not need it.

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

The matching role-side statement in each `deploy/iam/*.json` template is scoped
to the tenant key ARNs rather than to every key:

- Gateway and Maintain write tenant data through the routing store and read some
  of what they write, so they hold encrypt, generate-data-key and decrypt.
- Query reads tenant data, so it holds decrypt.
- Admin holds decrypt only, deliberately without generate-data-key: granting it
  would let a leaked Admin credential mint ciphertext under tenant keys it has
  no write role for.

Two gaps to plan around rather than assume covered. Query and Admin hold narrow
write grants under the tenant keyspace (catalog objects and the query-audit
shard for Query, reconstructed commit records for Admin). Those writes route
through the per-tenant key like any other, but neither role carries
generate-data-key, so a deployment that relies on them for a routed tenant must
grant it manually. When Admin hits this on a reconstruct write,
`ravel-cli commit reconstruct` names this exact condition in its error text.
Separately, the `t/<hash>/enc` epoch record needs its own read and write grant,
which the shipped templates do not yet carry.

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

At the shipped 200,000 caps that is 28 to 45 MiB per fully active tenant, so ten
simultaneously fully active tenants cost 280 to 450 MiB in the worst case. At a
1,000,000 cap the same arithmetic gives 140 to 224 MiB per tenant, and 1.4 to
2.2 GiB for ten. Raise a tenant's ceiling explicitly in its own table when it
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

The read cache has a RAM tier, always on unless `--disable-cache`, and an
opt-in local-disk tier. `--cache-dir <path>` attaches the disk tier at that
directory to both the query fetcher cache and the catalog byte cache, so a RAM
eviction is served from local disk instead of paying the object-store round trip
again:

```sh
ravel-server --store s3 --s3-bucket my-bucket --cache-dir /var/cache/ravel
```

There is no separate capacity flag for the disk tier. Each tier is bounded by
the single `--cache-max-bytes` number, read once at startup with no live resize.

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

Four values govern when a deleted object's bytes actually go away, and they must
agree with each other or a reader can lose a segment out from under it. The
governing inequality is:

```
protection_horizon >= max_query_duration + grace
```

These values are recorded once, deployment-wide, in a durable `sys/gc` object at
the bucket root, and every mode validates itself against it at startup. That is
what stops three independently deployed process configurations from drifting
apart with nothing checking the constraint.

**Bootstrap never blocks a fresh deployment.** The first process to touch a
fresh bucket writes `sys/gc` from the maintain defaults, which satisfy the
constraint by construction, then validates against the object it just wrote. If
several processes start together against one empty bucket, one wins the create
and the others read and validate against the winner's object.

**What each mode validates:**

- `maintain`: its configured protection horizon and grace must **equal** the
  stored values. They are must-match, not independent knobs. A flag value that
  satisfies the inequality but differs from the durable value still refuses to
  start.
- query-serving modes (`query`, `all`): the engine deadline must be less than or
  equal to the stored `max_query_duration`.
- Flight SQL, in a build that has it: the ticket time-to-live ceiling must be
  less than or equal to `protection_horizon - grace`. The server reads that
  ceiling from `sys/gc` rather than a compiled-in default, so it tracks the
  durable authority automatically.

### The flags, and the order to change them in

Each knob has a `ravel-server` flag, a humantime duration defaulting to its
shipped value, so a process that sets none of them is unchanged:

- `--gc-protection-horizon` and `--gc-grace` feed the maintain compactor and
  must **equal** the durable values. Set them to whatever the last
  `gc-config set` wrote.
- `--gc-max-query-duration` sets the enforced deadline for every query engine
  the process builds, and must stay at or below the durable
  `max_query_duration` (default 1h). A value above it is rejected at startup,
  never clamped down.
- `--gc-max-flush-lifetime` sets the compactor's flush lifetime, which is the
  seal margin and the orphan age gate. It is not part of the must-match set.

Each flag feeds both the startup validation and the real compactor or query
engine, so a value that passes validation is the value actually enforced. The
practical consequence of the must-match rule: changing a horizon is not a
rolling config change. Change the durable object first, then bring every
process's flags into line, and expect a process started against the old value to
refuse rather than to run with it.

```sh
ravel-cli gc-config show
ravel-cli gc-config set --protection-horizon 25h --grace 24h \
  --max-query-duration 1h --max-flush-lifetime 1h
```

`gc-config set` is the single mutation path. It enforces the inequality at write
time, refusing a violating proposal without writing anything, and swaps the
object with a compare-and-set so a concurrent `gc-config set` is a reported
conflict rather than a silent overwrite. Every value must be strictly positive:
an all-zero configuration would satisfy the inequality trivially and be
impossible for any mode to match, so it is rejected.

The Kubernetes operator exposes no GC-horizon fields, and does not need to: it
deploys every pod with the same shipped defaults, so the first pod bootstraps
`sys/gc` from those defaults and every pod validates trivially.

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

## Tenancy setup

Repeated `--tenant-token TOKEN=TENANT` flags configure tenants entirely. There is
no tenant database and no admin API. To add, remove or rotate a token, restart
with a different flag set. That is safe: every process is stateless, so a
restart has no data migration to do. With no `--tenant-token` at all, every
request is unauthenticated and rejected.

Tenant identity affects only key prefixing and authorization. It carries no
other per-tenant configuration.

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
other claim. The key set is cached in memory and refreshed on
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

Dependent flags fail fast: `--oidc-tenant-claim` or `--oidc-audience` without
OIDC enabled, `--mtls-header` or `--mtls-listener` without `--mtls-enabled`, and
`--mtls-enabled` without `--mtls-listener`, all refuse to start rather than
quietly doing nothing.

### The tenant hash scheme is permanent per bucket

The object-key prefix for a tenant is a hash of the tenant id, pinned per bucket
at the bucket's birth by a `sys/tenancy` marker. One binary carries both
schemes and selects one at startup:

- **v1 unkeyed**: the original derivation. Every bucket created before the
  marker existed is pinned to it permanently. Tenant names are not in keys, but
  anyone with list access can confirm a guessed tenant id offline.
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

`--shards` is immutable per tenant and signal. Once a tenant's data for a signal
is written across N shards, resolution iterates `0..N`, so serving that tenant
with a lower `--shards` would silently omit every series in the missing shards.
It also sets both the ingest router's shard count and the query-side catalog's
shard count, which is why there is no separate query-side flag.

To make a mismatch loud instead of silent, the first write for a tenant and
signal records the value in a durable provisioning record at
`t/<tenant_hash>/<signal>/prov`, and every later ingest, query and maintenance
touch validates against it. Lowering `--shards` for a tenant that has data in
higher shards is refused by construction.

A brand-new tenant with no prior writes has no record yet, so a fresh
deployment, including an operator-managed cluster that starts with zero data and
configured tokens, starts normally. The record is created on the tenant's first
write.

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

`--logs-fetch-policy` takes one of three values, spelled exactly as here. Its
default is `cost-based`.

| Value | Optimizes for | Pick it when |
|---|---|---|
| `request-minimal` | Fewest object-store requests. An object at or under the fetch bound is read whole in one covering request with no footer probe; a larger object is read as covering sub-range requests. | The backend bills requests and not transfer, so a saved request is a saved dollar and the bytes it costs are free. |
| `byte-minimal` | Fewest transferred bytes. Ranged reads wherever they save more bytes than a request is worth. | The backend bills egress, or the network is the constraint, so moved bytes are the cost that matters. |
| `cost-based` | Whichever of the two is cheaper under the active store cost profile, resolved from the profile's prices at startup. | You want the shape the deployment's own prices imply. At the reference intra-region profile this resolves to request-minimal. |

For any policy value a query returns exactly the same rows. Only request counts
and timing differ.

The policy is an operator surface only. It is never derivable from query text, a
header or a ticket: under request billing, a tenant that could force
`byte-minimal` per query would multiply the deployment's request bill by the
measured amplification factor. The running engine also never changes its own
policy. If a measurement shows the default is wrong for a deployment, set
`--logs-fetch-policy` explicitly.

### The store cost profile

`--store-cost-profile <path>` names a TOML file of this deployment's
object-store prices. It is read only when resolving `cost-based`; no price ever
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
```

Prices are integer nanodollars, never floats, because they are exact decimal
contract figures. The reference values model S3 standard intra-region 2026 list
prices: PUT class $5.00 per million requests, GET class $0.40 per million,
transfer and retrieval free. One PUT costs 12.5 GETs at those prices. Every
price is a modeled figure under a named profile, not a billed amount, and the
same run under a different profile reprices to different numbers.

Every field except `delete_class_nanodollars` is required. Loading is
fail-closed: an unreadable file, invalid TOML, an unknown or misspelled key, or
a blank name refuses startup with an error naming the flag. There is no silent
fallback to the reference prices, because a deployment that named a profile and
got the reference prices instead would stamp one profile into its reports while
resolving its fetch policy from another.

**How `cost-based` resolves.** It converts the profile's prices into the one
byte quantity the fetch layer runs on: how many transferred bytes one saved
request is worth.

```
request_cost_bytes = get_class_nanodollars x BYTES_PER_GIB
                     / (transfer_nanodollars_per_gib + retrieval_nanodollars_per_gib)
```

`BYTES_PER_GIB` is 2^30, and the arithmetic multiplies before it divides in
128-bit so a sub-nanodollar per-byte price does not truncate to zero. Retrieval
is a per-byte charge exactly like transfer and enters the denominator the same
way, so a profile with free transfer but priced retrieval still routes
byte-minimally rather than reporting retrieval dollars a request-minimal plan
would never have spent. The result is floor-rounded, held at a minimum of one
byte, and clamped to the coalescing-gap and routing-threshold floors. Two cases
saturate the rate, both meaning "read whole always": a zero denominator, where
no per-byte cost exists, and quotient overflow from a near-free but nonzero
per-byte price, which is logged at startup naming the profile.

At the reference profile both per-byte prices are zero, so `cost-based` resolves
to request-minimal behavior. At egress list prices (GET class $0.40 per million
against $0.09 per GiB transfer plus $0.01 per GiB retrieval) it resolves to
about 4.4 KB, which the floors then clamp.

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

`request-minimal` additionally overrides an explicitly set
`--logs-block-range-threshold`: it saturates both routing thresholds regardless
of that flag, and a set-but-overridden threshold is logged at startup so the
override is visible. Under the other two policies that flag keeps its normal
role.

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

**Cost note.** Promoting a key does not make equality predicates on it faster,
and today it makes them slower: `attrs['k'] = 'v'` prunes blocks through the
index, while `k = 'v'` on the promoted column is evaluated as a residual filter
above the scan. Promote for typed comparisons and aggregates (`k > 5`,
`SUM(k)`), which are impossible over the map, not to speed up an equality that
already prunes.

There is also a per-object budget on how many distinct attribute name and type
pairs get a real column at write time. Pairs beyond the budget fold into an
overflow column and lose columnar access. Watch for that in
[the observability guide](../observability.md).

## Per-query budgets

Four flags bound what one query may spend. Each defaults to the value that was
compiled in before the flag existed, so leaving them unset changes nothing.

| Flag | Default | Choose against |
|---|---|---|
| `--fetch-concurrency` | 8 | Host cores and the store's request budget. One knob with three coupled effects: it also sets the SQL scan partition count and the object-store request concurrency. |
| `--max-segments` | 1024 | How many sealed objects a wide scan touches. Only the recent set, roughly the last two hours, is exempt, so a tenant with a lot of sealed history hits this before you expect. |
| `--sql-max-query-bytes` | 256 MiB | Per-query SQL memory pool ceiling. Process-wide, not per-tenant. |
| `--sql-tenant-max-bytes` | 1 GiB | The multi-tenant isolation bound: SQL memory one tenant may hold across its concurrent queries. Process-wide, and not itself per-tenant-overridable. |

The last two are meaningful only in a build with the `sql` feature. See
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
and [operator-configurable query budgets](../../adrs/0088-operator-configurable-query-budgets.md).
