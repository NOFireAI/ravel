# Operations

## `ravel-server` flags

All flags, verified against [services/ravel-server/src/config.rs](../../services/ravel-server/src/config.rs):

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--mode <all\|gateway\|query\|maintain>` | | `all` | Which roles this process runs. `all` and `gateway` serve OTLP ingest; `all` and `query` serve `/api/v1/*`. `maintain` serves neither. It runs only the background maintenance loop (compaction, retention, sweep), and still binds `--listen-http` for liveness. It needs a backend that reports the `multipart` capability. |
| `--listen-http <addr>` | | `127.0.0.1:4318` | HTTP listener for OTLP ingest (`POST /v1/metrics`) and the query API. |
| `--listen-grpc <addr>` | | `127.0.0.1:4317` | gRPC listener for the OTLP `MetricsService`. Bound only when the process runs ingest (`all`/`gateway`). |
| `--store <memory\|s3>` | | `memory` | Object store backend. `memory` is in-process only, for tests and local experiments; nothing survives process exit. |
| `--shards <n>` | | `4` | Ingest shard count. It sets both the ingest router's shard count and the query-side catalog's shard count, so they must agree. There is no separate query-side flag. |
| `--tenant-token TOKEN=TENANT` | | none, repeatable | Registers one bearer token for the static resolver. Pass it once per tenant. With no `--tenant-token` at all, every request is unauthenticated and rejected. |
| `--dev-insecure-tenant-header` | | off | Adds tenant resolution through the `x-ravel-tenant` request header, tried only when bearer lookup fails. If `--listen-http` does not bind a loopback address, the process refuses to start with this set. |
| `--require-bucket-protection` | `RAVEL_REQUIRE_BUCKET_PROTECTION` | off | Gates startup on the ADR-0072 decision 3 bucket-protection contract: an affirmatively-disabled Object Lock/versioning probe, or a bucket-configuration alarm (ADR-0064 section 7), refuses to start; an unknown probe result (every backend today) logs one warning and sets `ravel_bucket_protection_unknown`. Off by default: unset, startup is unchanged from before this flag existed. `ravel-operator` sets this for every `RavelCluster` it reconciles. See "Bucket protection contract" below. |
| `--oidc-issuer <url>` | | none | OIDC issuer, the exact `iss` every JWT must carry. Set together with `--oidc-jwks-url` to enable the OIDC resolver (ADR-0042 decision 6). Setting only one of the pair refuses to start. |
| `--oidc-jwks-url <url>` | | none | URL of the issuer's JWKS document (its public signing keys), fetched directly (no OIDC discovery). Enables OIDC together with `--oidc-issuer`. Refuses a plaintext `http://` URL to a non-loopback host at startup. |
| `--oidc-audience <aud>` | | none, repeatable | Acceptable JWT `aud` value. With none set, audience is not checked. Set without OIDC enabled refuses to start. |
| `--oidc-tenant-claim <claim>` | | `tenant` (when OIDC on) | String claim the tenant id is read from. A token missing it, or whose value is not a non-empty string, is rejected with no fallback to `sub`. Set without OIDC enabled refuses to start. |
| `--oidc-jwks-refresh-interval-secs <n>` | | `300` | How often the JWKS document is refetched. The first fetch is awaited before the server reports ready; if it fails, an OIDC-enabled server refuses to start rather than serve with an empty key cache. |
| `--mtls-enabled` | | off | Enables the mTLS resolver, which maps a trusted, proxy-forwarded client-certificate identity header to a tenant. Opt-in because the header is a client-forgeable trust boundary unless a verifying proxy sets and sanitizes it (see Tenancy setup). |
| `--mtls-header <name>` | | `x-ravel-client-cert-cn` (when `--mtls-enabled`) | Header the reverse proxy forwards the verified client-certificate CN/SAN in. Set without `--mtls-enabled` refuses to start. |
| `--s3-endpoint <url>` | `RAVEL_S3_ENDPOINT` | none | Custom S3 endpoint (MinIO, or any S3-compatible store). Unset means real AWS S3. It also turns on `allow_http` for that endpoint. |
| `--s3-bucket <name>` | `RAVEL_S3_BUCKET` | none | Required when `--store s3`. |
| `--s3-region <region>` | `RAVEL_S3_REGION` | `us-east-1` | |
| `--s3-access-key <key>` | `RAVEL_S3_ACCESS_KEY` | none | Required when `--store s3` under the default `--s3-auth static`. Must be unset under `--s3-auth instance-role`. |
| `--s3-secret-key <secret>` | `RAVEL_S3_SECRET_KEY` | none | Required when `--store s3` under the default `--s3-auth static`. Must be unset under `--s3-auth instance-role`. |
| `--s3-auth <static\|instance-role>` | `RAVEL_S3_AUTH` | `static` | Where `--store s3` gets its credentials (ADR-0106). `static` is unchanged behavior: the access key and secret key are both required. `instance-role` drops that requirement and fetches short-lived credentials from the EC2 instance metadata service (IMDSv2) instead, so an EC2 deployment stores no static keys at all; the first fetch happens at startup, so a misconfigured instance role fails to start rather than failing its first request. Combining it with `--s3-access-key`, `--s3-secret-key`, `--s3-session-token`, or `--s3-credentials-file` is a startup error naming the conflicting flag, not a precedence rule. With SSE-KMS on, the instance role needs `kms:GenerateDataKey` and `kms:Decrypt` on the key. |
| `--s3-session-token <token>` | `RAVEL_S3_SESSION_TOKEN` | none | Temporary AWS session token paired with the access key and secret key for STS-issued credentials (ADR-0072 decision 1). Ignored when `--s3-credentials-file` is set: the file wins. Only meaningful under `--s3-auth static`. |
| `--s3-credentials-file <path>` | `RAVEL_S3_CREDENTIALS_FILE` | none | JSON file of `{access_key_id, secret_access_key, session_token}` (`session_token` optional) that an external process rotates on disk (ADR-0072 decision 1). Read and parsed once at startup, so an unreadable or malformed file fails startup; after that it is re-read on the request path only when its mtime changes, and a parse failure while rotating keeps serving the last-good credential with a rate-limited warning. Wins over the inline key flags. Only meaningful under `--s3-auth static`. |
| `--s3-instance-metadata-endpoint <url>` | `RAVEL_S3_INSTANCE_METADATA_ENDPOINT` | none (the AWS link-local address) | Base URL of the instance metadata service, used only under `--s3-auth instance-role`. Exists so tests and unusual deployments can redirect IMDS; leave it unset on EC2. |
| `--s3-kms-key <arn>` | `RAVEL_S3_KMS_KEY` | none | Single-key SSE-KMS (ADR-0062 decision 1c): every PUT the default store makes is encrypted with this KMS key ARN, applied to `S3Config.kms_key_id`. Off by default: unset, the store's SSE behavior is unchanged from before this flag existed (whatever bucket-default SSE the deployment has). |
| `--tenant-kms-config <path>` | `RAVEL_TENANT_KMS_CONFIG` | none | TOML file mapping tenant name to KMS key ARN (`[tenants]` table, ADR-0062 decision 1, ADR-0072 decision 2). Requires `--store s3` — refuses to start under `--store memory`, since the per-tenant routing decorator always constructs a real `S3Store`. Off by default: unset, `build_store` produces exactly the store it always has, with no per-tenant routing decorator in the chain at all. See "Per-tenant SSE-KMS routing" below. |
| `--disable-fold` | | off | Disables the per-(tenant, signal) background catalog fold task. Folding only lowers query resolve cost; disabling it never changes query results. |
| `--fold-interval-secs <n>` | | `300` | How often each tenant's fold task wakes up to check for newly sealed hours. |
| `--maintain-interval-secs <n>` | | `300` | Used only in `--mode maintain`. How often each tenant's maintenance task wakes to run retention, compaction, and the sweeper over every shard of both signals. |
| `--maintain-interior-reverify <duration>` | | `6h` | Used only in `--mode maintain`. The slow safety-net cadence for the ADR-0065 interior zone (the ingest hours that aren't in the head or tail zones this tick): an interior bucket's memoized terminal state is re-verified no less often than this, and the sweeper runs a full-keyspace pass (instead of its per-tick head+tail-only pass) on the same cadence. Head and tail hours are unaffected and are always evaluated every tick. A zero duration disables the safety net (every interior bucket is always due), matching pre-ADR-0065 behavior for that zone; only an unparseable duration fails startup. |
| `--maintain-unit-concurrency <n>` | | `4` | Used only in `--mode maintain`. The maximum number of owned `(signal, shard)` units this process maintains at once within one tenant's tick (ADR-0065 decision 2's stuck-owner mitigation), replacing the pre-ADR-0065 strictly-sequential per-shard walk so one pathological unit cannot starve the rest of this process's ownership. Clamped to at least 1; a value of `0` degrades to a sequential walk rather than deadlocking. Scale this up on a process that owns many units and has spare I/O concurrency to spend, down on a process sharing a host with other work. |
| `--maintain-stalled-after-intervals <n>` | | `3` | Used only in `--mode maintain`. Consecutive failed ticks an owned `(tenant, signal, shard)` unit must accrue, with no intervening success, before it counts toward `ravel_maintain_units_stalled` (ADR-0065 decision 2's stuck-owner mitigation). A single success resets that unit's counter to zero. Lower it to page sooner on a flaky unit at the cost of more noise from transient single-tick faults; raise it to tolerate a noisier store at the cost of a slower page. |
| `--retention-default <duration>` | | none | Used only in `--mode maintain`. The default age-based retention window applied to every tenant with no explicit override, as a humantime duration (`30d`, `720h`). Omitted means no default retention: nothing is age-deleted unless a per-tenant window is set. It is validated at startup against the ADR-0019 floor; a window below the floor fails startup rather than being clamped. |
| `--retention-tenant TENANT=DURATION` | | none, repeatable | Used only in `--mode maintain`. The per-tenant retention window; it overrides `--retention-default` for that tenant. Parsed with `humantime::parse_duration`. Same below-floor validation. |
| `--indexed-field FIELD` | | shipped default list, repeatable | POSTINGS indexed field for a tenant with no `--indexed-field-tenant` override (ADR-0049 decision 3). Pass the flag once per field. The shipped default is `service.name`, `k8s.namespace.name`, and `http.status_code`. Any value you pass replaces the shipped default list, not adds to it. |
| `--indexed-field-tenant TENANT=FIELDS` | | none, repeatable | Per-tenant POSTINGS indexed-field override, as `TENANT=field1,field2`. It replaces the default list for that tenant only. An empty field list (`--indexed-field-tenant acme=`) turns off POSTINGS indexing for that tenant. |
| `--typed-attr-column KEY:TYPE` | | none, repeatable | Declares an attribute key as a native typed `logs` SQL column for every tenant with no override (ADR-0090 decision 1). `TYPE` is one of `str`, `i64`, `bool`, `bytes`, case-insensitive; `f64`, date, and timestamp are deferred. Pass the flag once per column; declaration order is the order the columns are appended to the schema. Unset declares nothing: there is no shipped default, because a declared column changes the SQL schema a tenant's queries see. An empty key, a duplicate key, the same key with two types, or a key colliding with one of the nine fixed logs columns (`ts`, `observed_ts`, `severity_num`, `severity_text`, `body`, `trace_id`, `span_id`, `flags`, `attrs`) fails startup. Meaningful only in a build with the `sql` feature. See "Declared typed attribute columns" below. |
| `--typed-attr-column-tenant TENANT:KEY:TYPE` | | none, repeatable | Per-tenant declaration override, as `TENANT:KEY:TYPE`. Repeating the flag for one tenant accumulates that tenant's ordered declaration. It replaces the default for that tenant outright rather than adding to it, the same way `--indexed-field-tenant` overrides `--indexed-field`. A tenant id may not contain `:`; an attribute key may (the type is split off the right). |
| `--limits-file <path>` | | none (shipped defaults) | TOML admission-limits file (ADR-0051 section 3): `[defaults]` plus per-tenant `[tenants.<id>]` overrides. Parsed and validated at startup; an unparseable file, an unknown key, or a nonsensical limit (zero, or a burst set with no rate to pair it with) fails startup rather than falling back to defaults. See "Admission limits file" below. |
| `--cache-max-bytes <n>` | | `268435456` (256 MiB) | Maximum resident bytes for the ADR-0046 read cache's RAM tier. Read once at startup; there is no live resize. Ignored when `--disable-cache` is set. See [guides/caching.md](caching.md). |
| `--cache-dir <path>` | | none | Directory for the read cache's local-disk tier (ADR-0046). Opt-in: absent, only the RAM tier exists and behavior is exactly as before. Set, both the query fetcher cache and the catalog byte cache gain a disk tier at this path, each bounded by `--cache-max-bytes` (there is no separate disk-tier capacity flag). The directory is created lazily and never required to exist; a missing, full, or corrupt cache directory degrades to a store read, never a query error. Bytes written here are **not** SSE-KMS encrypted (see below). See [guides/caching.md](caching.md). |
| `--disable-cache` | | off | Disables the ADR-0046 read cache entirely. Query behavior becomes byte-for-byte identical to a build with no read cache wiring at all. |
| `--metrics-tenant-labels` | | off | Emits real per-tenant `tenant_hash` labels on the `ravel_admission_*` family at `/metrics` (ADR-0051 section 6) instead of folding every tenant into `tenant_hash="other"`. A deliberate cardinality trade; off by default so `/metrics` cardinality never scales with tenant count unless an operator opts in. See "Admission usage" above. |
| `--store-probe-interval <duration>` | | `30s` | How often the background store-reachability probe GETs `sys/tenancy` (ADR-0050 section 7), as a humantime duration, jittered. After four consecutive failures `/readyz` returns 503; one success recovers it. See "Store reachability probe and `/readyz`" below. |
| `--scrub-period <duration>` | | `7d` | Used only in `--mode maintain`. The at-rest scrub period `P` (ADR-0059 decision 1), as a humantime duration. The content-tier scrubber rotates through the whole object corpus once per `P`, so sustained scrub read bandwidth is bounded at `corpus_bytes / P`. A zero or unparseable duration fails startup. See "At-rest integrity scrubber" below. |
| `--fetch-concurrency <n>` | | `8` | Per-query concurrent in-flight segment fetches (ADR-0088), threaded into `EngineConfig::fetch_concurrency`. One knob, three coupled effects, NOT decoupled by this flag: it also sets the SQL scan partition count (`target_partitions`) and object-store GET concurrency (ADR-0087). Size against host cores and the store's request budget. Default is today's compiled-in value, so unset is byte-identical to before. See [guides/query.md](query.md#operator-configurable-budgets-server-flags). |
| `--max-segments <n>` | | `1024` | Per-query cap on segments fanned out over (ADR-0088), threaded into `EngineConfig::max_segments`. Only the recent set (`SegmentOrigin::Recent`, ~last two hours) is exempt; older sealed/compacted objects count toward the cap, so a wide scan over a tenant with many sealed objects hits it. Raise it for such workloads. Default is today's compiled-in value. |
| `--sql-max-query-bytes <bytes>` | | `268435456` (256 MiB) | Per-query ceiling on the SQL DataFusion memory pool (ADR-0088), threaded into `SqlConfig::max_query_bytes`. Process-wide, not per-tenant. Meaningful only in a build with the `sql` feature. Default is today's compiled-in value. |
| `--sql-tenant-max-bytes <bytes>` | | `1073741824` (1 GiB) | Per-tenant ceiling on the SQL memory one tenant may hold across its concurrent queries (ADR-0088): the multi-tenant isolation bound, four times the per-query default. Process-wide, not itself per-tenant-overridable (the `--limits-file` per-tenant query overrides are inert today). Meaningful only in a build with the `sql` feature. Default is today's compiled-in value. |
| `--distributed-query` | | off | Turns on ADR-0071 distributed read fan-out. A query-serving process (`all`, `query`) both registers the cluster-internal `SeriesFetch` fragment gRPC surface and acts as a coordinator that may fan a large query's pinned snapshot out to live query workers. Requires `--fragment-auth-token-file`; set without it, the process refuses to start rather than exposing an unauthenticated fetch surface. Off means a process serves queries exactly as before and never registers the fragment surface. |
| `--fragment-auth-token-file <path>` | | none | File holding the shared cluster-internal bearer token that guards the fragment surface. A file, never an inline value or env var, so the secret never appears in a process listing. Every worker and coordinator in one cluster reads the same file: a coordinator presents this token on each slice dispatch, and a worker refuses any fragment request whose token is missing or unequal. The fragment surface binds only on the cluster-internal gRPC listener, never the external HTTP or mTLS listeners. Meaningful only with `--distributed-query`. |
| `--max-inflight-fragments <n>` | | `32` | The admission cap for inbound fragment (`SeriesFetch`) requests: the maximum slice fetches this process serves concurrently for remote coordinators. A distinct workload class from the client-query cap, so a coordinator waiting on its own dispatched fragments cannot deadlock behind client queries. Over the cap a fragment request queues rather than being rejected. Clamped to at least 1. |
| `--distribute-bytes-threshold <n>` | | `268435456` (256 MiB) | The estimated-store-bytes axis of the fan-out cost gate. A query whose pre-fetch cost estimate reaches this many bytes is distributed; a query below both axes runs fully locally. Meaningful only with `--distributed-query`. |
| `--distribute-segments-threshold <n>` | | `256` | The segment-count axis of the fan-out cost gate. Either axis alone trips the gate. Meaningful only with `--distributed-query`. |
| `--max-parallel-slices <n>` | | `8` | Ceiling on concurrently dispatched slices per distributed query, bounding fan-out width so a wide snapshot cannot spawn an unbounded number of remote fetches. Clamped to at least 1. Meaningful only with `--distributed-query`. |
| `--fragment-listener <addr>` | | none | The dedicated TLS fragment listener (ADR-0071 amendment decision 1): a fourth listener, alongside `--listen-http`, `--listen-grpc`, and `--mtls-listener`, that terminates TLS in-process and serves `Pinned` intra-cluster fragment fetches ONLY. When set, the public gRPC listener stops serving `Pinned` scope (it keeps serving `Resolve`/federation with ordinary tenant credentials), and this listener rejects `Resolve` outright. Requires `--distributed-query` and all three of `--fragment-tls-cert`/`--fragment-tls-key`/`--fragment-tls-ca`, and must not equal any other listener address (startup refuses an alias). Without this flag the fragment surface stays on the public gRPC listener (the pre-amendment layout), so distribution keeps working during a rolling deploy. See [Dedicated fragment listener TLS](#dedicated-fragment-listener-tls-adr-0071-amendment-decision-1). |
| `--fragment-tls-cert <path>` | | none | PEM server certificate the dedicated fragment listener presents. Operator-provisioned; Ravel mints no certificates. Must carry a `ravel-fragment` dNSName SAN, the one fixed name every coordinator verifies against. Read once at startup; rotation is a rolling restart. Required with `--fragment-listener`. |
| `--fragment-tls-key <path>` | | none | PEM private key for `--fragment-tls-cert`. Operator-provisioned; read once at startup. Required with `--fragment-listener`. |
| `--remote-cluster <spec>` | | none, repeatable | A remote cluster this coordinator federates queries out to (ADR-0071 cross-cluster federation), as a comma-separated `key=value` spec. Required keys: `name`, `endpoint` (`host:port`), `credential-file` (a file holding the operator bearer token this coordinator presents to that remote). Optional: `tls` (`true`/`false`, **default `true`**), `tls-ca-file`, `skip-unavailable` (`true`/`false`, default `false`), `soft-timeout`. TLS on verifies the remote against the system trust roots, plus `tls-ca-file` when set; a spec carrying `tls-ca-file` and no `tls` key means "TLS on, with this CA trusted". `tls=false` is the plaintext escape hatch, and startup logs a `SECURITY:` WARN naming that remote and endpoint. `tls=false` together with `tls-ca-file` fails startup: the CA bundle would be inert. See [Federating to a remote cluster](#federating-to-a-remote-cluster-adr-0071). |
| `--remote-cluster-soft-timeout <duration>` | | `ravel_query::distrib::DEFAULT_REMOTE_SOFT_TIMEOUT` | The default per-remote soft timeout for a federated fetch: a remote that does not answer within this bound is treated as unavailable (failing the query, or skipped, per that remote's `skip-unavailable`). A `soft-timeout` key on an individual `--remote-cluster` overrides it for that remote. A zero duration fails startup. |
| `--fragment-tls-ca <path>` | | none | PEM CA bundle the coordinator's outbound fragment dial verifies remote workers against. The CA is dedicated to this surface, so any certificate it signed means "a fragment worker of this cluster"; per-process certificate identity is deliberately not required (the capability, not the certificate, is the authorization). Read once at startup. Required with `--fragment-listener`. |

`--store s3` without `--s3-bucket`/`--s3-access-key`/`--s3-secret-key` (through
flag or env) fails at startup with an explicit error that names the missing
one. It does not start in a broken state. Under `--s3-auth instance-role` only
`--s3-bucket` is required: the two key flags must be absent, and startup fails
naming both `--s3-auth instance-role` and the offending flag if either is set
(an exported `RAVEL_S3_ACCESS_KEY` counts).

The same `--s3-*` flags and `RAVEL_S3_*` env vars, including `--s3-auth`, work
the same on `ravel-cli`, with one gap: `ravel-cli` has no `--s3-kms-key` and
never sets `kms_key_id`.

Note: the real S3 env var flags are `RAVEL_S3_ACCESS_KEY` and
`RAVEL_S3_SECRET_KEY`, above; use those.
`allow_http` and `force_path_style` are not configurable at all. The code
derives `allow_http` from whether `--s3-endpoint` is set, and it always passes
`force_path_style: true`.

### Running on EC2 with an instance role

Attach an IAM role to the instance granting S3 access to the bucket (plus
`kms:GenerateDataKey` and `kms:Decrypt` on the key when SSE-KMS routing is
on), then start the server with no credential flags at all:

    ravel-server --store s3 --s3-bucket my-bucket --s3-region us-east-1 --s3-auth instance-role

Credentials come from the instance metadata service at startup and refresh
before expiry. No static key is stored on the instance, in the environment,
or in logs; a missing or misconfigured role fails startup with a typed error
rather than failing the first request.

## Read cache disk tier (ADR-0046)

The read cache has a RAM tier (always on unless `--disable-cache`) and an
opt-in local-disk tier. `--cache-dir <path>` attaches the disk tier at that
directory to both the query fetcher cache and the catalog byte cache, so a RAM
eviction is served from local disk instead of re-paying the S3 round trip:

    ravel-server --store s3 --s3-bucket my-bucket --cache-dir /var/cache/ravel

The disk tier is opt-in and disposable. With no `--cache-dir`, only the RAM
tier exists and query behavior is exactly as before. The directory is created
lazily on first admission and is never required to exist; a missing, full, or
corrupt cache directory degrades to a store read, never a query error, so a
node whose cache directory is deleted mid-flight answers every query correctly
and only more slowly. There is no separate CLI flag for disk-tier capacity:
each tier is bounded by the single `--cache-max-bytes` number.

**Encryption at rest (ADR-0046 decision 6).** Bytes this process writes to the
cache directory are **not** encrypted by the SSE-KMS object-storage path.
SSE-KMS (`--s3-kms-key`, `--tenant-kms-config`) protects object bytes at rest
in the store, not the bytes written to the local cache. An operator who needs
bytes-at-rest encryption for the cache directory must provide it at the
filesystem/volume layer, for example an encrypted volume mounted at
`--cache-dir`. This mirrors ADR-0042's precedent of delegating encryption at
rest to the platform rather than making Ravel a key-management system.

**Metrics.** Once a disk tier is configured, each cache's `ravel_cache_*`
counters gain a `tier="ram"`/`tier="disk"` label alongside the existing
`cache="fetch"`/`cache="catalog"` label, so RAM-tier and disk-tier hit rates
are reported separately. With no `--cache-dir`, no `tier=` label appears and
the exposition is byte-for-byte as before. See
[guides/caching.md](caching.md) for the full metric list.

## Admission limits file

`--limits-file` (ADR-0051 section 3) points at a TOML file with a
`[defaults]` table and zero or more `[tenants.<id>]` override tables. Every
field is optional and independently overridable; a `[tenants.<id>]` table
only needs to name the fields that differ from `[defaults]`, which itself
only needs to name the fields that differ from the shipped defaults below.
Fields:

| Field | Meaning |
|---|---|
| `max_active_series` | Exact cap on concurrently active metric series for the tenant. |
| `max_active_streams` | Exact cap on concurrently active log streams for the tenant. |
| `ingest_bytes_per_sec` / `ingest_byte_burst` | Token-bucket rate and burst for ingested bytes. |
| `series_creation_rate_per_sec` / `series_creation_burst` | Token-bucket rate and burst for new-series/new-stream creation. |

Any of the four count/rate fields (not the two burst-only fields) accepts
the literal string `"unlimited"` instead of a number, to opt a tenant out of
that cap entirely. With no `--limits-file` at all, every tenant gets the
shipped defaults with no override.

Validation is fail-closed: the process refuses to start, rather than
silently keeping shipped defaults, on any of:

- a file that is not valid TOML;
- an unknown key in `[defaults]` or any `[tenants.<id>]` table;
- an empty tenant id (`[tenants.""]`);
- a count or rate of zero, or a negative number;
- a burst set without the rate it belongs to (or vice versa), when the
  underlying rate is `unlimited` and there is nothing to pair the burst
  with;
- a burst set alongside `unlimited` for the same rate in the same table.

### Shipped defaults and their memory cost

```
max_active_series            = 200000
max_active_streams           = 200000
ingest_bytes_per_sec         = 33554432   (32 MiB/s)
ingest_byte_burst            = 67108864   (64 MiB)
series_creation_rate_per_sec = 10000
series_creation_burst        = 100000
```

The rate defaults match ADR-0051 section 3. The two active-count caps do
not: the ADR sets both at 1,000,000, sized against an assumed ~16 bytes per
tracked entry. Measurement of the actual `HashSet` entry cost (hashbrown
slot overhead, power-of-two table sizing at 7/8 load, allocator headroom) put
it at 35-56 bytes, a 2-4x underestimate. `AdmissionController` tracks each of
active series and active streams in a two-epoch rotating set
(`ACTIVE_EPOCH_NS`), so both epochs' sets can be live at once. Worst-case
resident memory for one fully active tenant is:

```
cap × bytes_per_entry × 2 epochs × 2 signals (series + streams)
```

At the ADR's original 1,000,000/1,000,000 caps this is 1,000,000 × 35-56 ×
2 × 2 = 140-224 MiB per fully active tenant. At the 200,000/200,000 caps
shipped here it is 200,000 × 35-56 × 2 × 2 = 28-45 MiB per fully active
tenant, roughly a fifth. Ten simultaneously fully active tenants at the
shipped defaults is therefore 280-450 MiB worst case; at the ADR's original
caps it would have been 1.4-2.2 GiB. Operators who need a higher per-tenant
active-series ceiling can still set it explicitly in `[tenants.<id>]`
(or `unlimited`), sized against this same formula.

### Transient gzip decompression memory (ADR-0084)

Accepting gzip on OTLP HTTP adds a second, transient, memory demand that
ADR-0069's ingest buffer budget does not account for. A gzip request is
decompressed into a fresh buffer bounded by the 64 MiB HTTP decompressed cap,
held only while the request holds an ingest concurrency permit
(`--max-inflight-ingest-requests`, default 1024). Worst-case peak decompression
memory is therefore:

```
max_inflight_ingest_requests × 64 MiB
```

At the default 1024 permits that is a 64 GiB worst case, far past what the
small hosts this project targets have. On such a host, size
`--max-inflight-ingest-requests` down so this product fits the headroom you
have alongside the ingest buffer budget and the active-identity memory above;
the three are additive and none is bounded by the others. gRPC's transient
decompression is bounded at 16 MiB per in-flight request by
`max_decoding_message_size`, so the same arithmetic with a 16 MiB factor
applies to the gRPC side.

## `ravel-cli` flags

Every subcommand shares the same store flags as `ravel-server`
([services/ravel-cli/src/store.rs](../../services/ravel-cli/src/store.rs)):
`--store <memory|s3>`, `--s3-endpoint`, `--s3-bucket`, `--s3-region`,
`--s3-access-key`, `--s3-secret-key` (same `RAVEL_S3_*` env names as
above).

| Command | Args | Does |
|---|---|---|
| `ravel-cli segment inspect <path>` | local file path or object store key | Parses one RSEG segment: trailer, footer fields, section list, decoded series count. |
| `ravel-cli commit decode <key>` | local file path or object store key | Decodes one commit record: identity, referenced data object key/size/hash, sample/series counts, timestamps. |
| `ravel-cli commit decode-compaction <key>` | local file path or object store key | Decodes one `CompactionRecord`: identity, `input_set_hash`, each input identity, and each part's summary (`part_index`, series-id range, content hash, sizes, level, `segment_format_version`). |
| `ravel-cli commit decode-tombstone <key>` | local file path or object store key | Decodes one `RetentionTombstone`: identity, `retired_at_ns`, `retention_window_ns`, observed record count. |
| `ravel-cli maintain compact-bucket --tenant <n> --signal <metrics\|logs\|spans> --shard <n> --hour <n> [--dry-run]` | | Runs one compaction pass over a single sealed bucket and prints the outcome. `--dry-run` computes the same plan (part count, publish outcome) but writes no L1 parts or record. |
| `ravel-cli maintain sweep --tenant <n> --signal <metrics\|logs\|spans> --shard <n> [--dry-run]` | | Runs one sweep pass (orphan GC, superseded inputs, unreferenced parts) over a shard and prints the four delete counts. `--dry-run` reports the eligible set but deletes nothing. |
| `ravel-cli maintain status --tenant <n> --signal <metrics\|logs\|spans> --shard <n> --hour <n>` | | Reports a bucket's state (sealed, tombstoned, compacted, L0 record count, superseded-input count, L1 parts present, unreferenced-part count). Read-only, so no `--dry-run`. |
| `ravel-cli maintain audit-versions --tenant <n> [--shards <n>]` | `--shards` default `4` | Audits live on-object format versions across all three signals. It flags any RSEG object at a version other than the one supported version (currently v7, ADR-0027 and ADR-0092), reports the RLOG population by trailer version (only v3 is supported, ADR-0095; v1 and v2 objects are anomalies to re-ingest), and the RSPAN population by trailer version. Exits nonzero on any anomaly. Pre-release the reader is single-version with no N-1 window, so a surviving v6 object is a rejected anomaly to re-ingest, not a `migrate` target (ADR-0092 decision 7). |
| `ravel-cli maintain migrate --tenant <n> --signal <metrics\|logs\|spans> [--shards <n>] [--target-version <n>] [--family <name>] [--budget-records <n>]` | `--shards` default `4`, `--budget-records` default `0` (unlimited) | Migrates every live record below `--target-version` (defaults to the signal's current supported version) and raises the recorded format floor once a fresh re-audit confirms nothing below it survives (see "Format migration" below). Resumable: re-run to continue from the durable cursor after a budget stop. A refused raise ("FOUND STRAGGLERS") means genuine live data still below target, not a self-inflicted false positive -- no interleaved `sweep` is needed for this to converge. |
| `ravel-cli maintain verify-custody --tenant <n> [--shards <n>]` | `--shards` default `4` | Read-only, no `--dry-run` (nothing is written or deleted). Re-verifies the content-addressed chain at rest: every live data object's key-embedded `hash16` against its actual content hash, and every surviving compaction record's referenced inputs (a mismatch is an anomaly; an input the sweeper already legitimately reclaimed past its protection horizon is reported separately, not as an anomaly). Exits nonzero on any anomaly. |
| `ravel-cli catalog list --tenant <name> [--hours <n>] [--shards <n>]` | `--hours` default `1`, `--shards` default `4` | Lists commit records that the catalog resolves for that tenant over the last `hours` hours. `--shards` must match the shard count the data was written with. |
| `ravel-cli catalog fold --tenant <name> [--shards <n>]` | `--shards` default `4` | One-shot catalog fold: seals every eligible hour into a new snapshot part and CAS-advances HEAD. Prints the fold report (watermark before/after, buckets folded, entry count, request counts). This is the same operation that the background fold task runs on a timer. |
| `ravel-cli catalog inspect --tenant <name>` | | Decodes and prints HEAD and every referenced snapshot part: watermark, part keys, hashes, entry counts. It reports rather than errors when no HEAD exists yet. |
| `ravel-cli catalog verify --tenant <name>` | | Re-lists every sealed commit record and diffs it against the current snapshot. Prints counts of entries missing from or mismatched against the snapshot; exits nonzero on any divergence. It reports rather than errors when no HEAD exists yet. |
| `ravel-cli provision adopt --tenant <name> --shards <n> [--signal <metrics\|logs\|spans>]` | | Writes the durable `shard_count` provisioning record for a tenant with pre-ADR data, ahead of any server touching it (ADR-0050 section 5). Runs the same adoption path the server runs: writes the record only when every observed shard index is below `--shards`, and refuses (writing nothing, exiting nonzero) when a higher index proves `--shards` would hide data. Prints one line per signal. A signal with no data and no record is left untouched (its record is written on first ingest). |
| `ravel-cli typed-attr-column show <tenant>` | | Prints the tenant's durable declared typed attribute columns (ADR-0090 decision 1) from `TenantConfig.typed_attr_columns`, in schema-append order. Distinguishes three states: no config record, a record with no declaration (both leave the deployment default in force), and a present declaration (which replaces the default, including when it is explicitly empty). |
| `ravel-cli typed-attr-column set <tenant> [KEY:TYPE ...]` | | Replaces the tenant's durable declaration wholesale, validated on the same rules the server's flags are (empty key, duplicate key, conflicting types, fixed-column collision), then swapped with `CasVersion` so a concurrent write is a reported conflict rather than a silent overwrite. Not additive and with no per-key remove: pass the full intended list. Passing no declaration writes an explicit empty one, which means "this tenant declares nothing" and is distinct from having no override. Every other field of the record is carried through unchanged. A query-serving process picks the change up within its staleness horizon (60s); no restart is needed. |

`segment inspect` and `commit decode` accept a local file path or an
object-store key. A path that exists on disk is read directly; otherwise it is
fetched from the configured store.

## Catalog fold and verify

The catalog fold (ADR-0020) is a query-cost
optimization, not a durability mechanism. `resolve` always falls back to
listing commit records directly. A folder that never runs, crashes, or
falls behind therefore never loses or hides data; it only makes queries pay
Phase 1 listing cost for a wider window (docs/consistency-model.md "Catalog
snapshot staleness"; every row is exercised end to end in
`crates/ravel-failure-tests/tests/folder_crash_matrix.rs`).

**Seal-margin config discipline.** A fold seals an hour only after
`now >= hour_end + max_flush_lifetime + clock_skew_allowance +
fold_safety_margin` (defaults 1h + 5m + 15m = 1h20m,
`crates/ravel-catalog/src/config.rs`). These three margins give
every writer's flush for that hour time to land before the fold treats it
as closed. If you widen `max_flush_lifetime` (writers then hold flushes open
longer) or the tolerated wall-clock skew between writers and the folder,
and you do not also review `fold_safety_margin`, you risk the failure mode
below. `--fold-interval-secs` only controls how often the background task
*checks* for newly sealed hours. It has no bearing on when an hour becomes
eligible to seal.

**If a folder's clock runs fast beyond its margin**, it can seal an hour
before every writer's flush for it has landed. A commit published into
that already-sealed bucket becomes invisible to non-token queries. A
`min_commit_token` query is unaffected: it always GETs its exact commit
key directly, never through the snapshot. This is the one failure mode
that needs an operator repair rather than resolving itself:

1. Run `ravel-cli catalog verify --tenant <name>` (per signal). A nonzero
   exit and a nonempty "missing from snapshot" count confirm sealed
   commits that the snapshot does not know about.
2. Delete the tenant's HEAD object for the affected signal:
   `t/<tenant_hash_hex>/catalog/<signal>/HEAD` (`m` for metrics). There is
   no `ravel-cli` subcommand for this today; use the store's own tooling
   (`mc rm` against MinIO, `aws s3 rm` against S3). Deleting HEAD is safe.
   `Catalog::fold` treats an absent HEAD as "no snapshot yet" and rebuilds
   one from a full listing rather than errors.
3. Run `ravel-cli catalog fold --tenant <name> --shards <n>` (or wait for
   the next background fold tick). The fold report's `rebuilt: true` line
   confirms that it rebuilt from scratch rather than extended the prior
   snapshot.
4. Re-run `ravel-cli catalog verify --tenant <name>` to confirm that the
   divergence is gone.

There is no `catalog fold --force-rebuild` flag. Deleting HEAD is the
supported way to force one, because it reuses the same absent-HEAD path that
a brand-new tenant takes on its first fold.

**Routine verification.** `catalog verify` is safe to run at any time
against a live tenant; it only lists and compares, and never mutates. Run
it on a schedule after you deploy or reconfigure seal margins. This is the
cheapest way to catch the clock-skew failure mode before it is noticed at
query time.

### Catalog isolation-breach metric and alert

`ravel_catalog_isolation_breach_total` (counter, labeled by `mode`, no
`tenant_hash` label per ADR-0044) renders at the existing `GET /metrics`
endpoint beside the `ravel_catalog_interlock_violations_total` and
`ravel_catalog_compaction_input_set_conflicts_total` anomaly counters
(docs/catalog-and-mvcc.md). It increments and fails the query, per ADR-0050
section 2, on: a `tenant_hash` mismatch on a catalog HEAD or postings object,
or a resolve-path listing result whose key does not begin with the
requesting tenant's prefix. Unlike the two counters beside it, which tally a
harmless-overlap anomaly the query still resolves past, every increment here
is a query that failed with an explicit isolation-fault error.

Coverage is not yet complete: the PromQL/remote-read and SQL query paths
share one `Catalog` instance and both count here, but a `tenant_hash`
mismatch on a commit or compaction record (`crates/ravel-catalog/src/
catalog.rs`'s `validate_expected_fields` / `validate_compaction_expected_fields`)
hard-fails its query without incrementing this counter, and a
foreign postings object that fails its part-binding check first degrades
silently before the tenant_hash comparison ever runs. A silent
gap for snapshot parts also exists: a part's own `tenant_hash`
is never checked against the requesting tenant at all.

Default alert rule:

| Condition | Query | Why |
|---|---|---|
| Isolation breach | `increase(ravel_catalog_isolation_breach_total[5m]) > 0` | Every increment already failed a query with a hard error; there is no sustained-condition or dilution case to wait out, unlike the mass-orphan breaker below. Any nonzero increase is a cross-tenant key-layout or hashing bug an operator needs to see immediately, not a rate to threshold. |

## Durable shard count (ADR-0050 section 5)

`--shards` is immutable per (tenant, signal): once a tenant's data for a
signal is written across N shards, resolution iterates `0..N`, so serving
that tenant with a lower `--shards` would silently omit every series in the
missing shards. To make that a loud failure instead of silent data loss, the
first write for a (tenant, signal) records `--shards` in a durable
provisioning record at `t/<tenant_hash>/<signal>/prov`, and every later
ingest, query, and maintenance touch validates the configured value against
it.

**A startup refusal from a shard_count mismatch** means this process was
configured with a different `--shards` than a statically-known tenant's data
was written under. The error names the tenant, signal, expected (recorded),
and actual (configured) values. It is not transient and does not clear on
restart: the object storage records the true shard count, and the fix is to
set `--shards` back to the recorded value (never lower it below what a tenant
already used). Lowering `--shards` for a tenant that has data in higher
shards is a data-hiding operation and is refused by construction.

A brand-new tenant with no prior writes has no record yet, so a fresh
deployment (including an operator-managed cluster that starts with zero data
and configured tenant tokens) starts normally; the record is created on the
tenant's first write. Only a tenant whose record already disagrees, or whose
pre-ADR data a lower value would hide, refuses.

For a **dynamically-resolved tenant** (OIDC/mTLS), a mismatch is not known
until a request arrives: that one request fails with a typed error and
`ravel_provisioning_shard_count_mismatch_total` increments; the process is
never taken down for a single tenant's mismatch. Alert on any increase:

| Condition | Query | Why |
|---|---|---|
| shard_count mismatch | `increase(ravel_provisioning_shard_count_mismatch_total[5m]) > 0` | A dynamic tenant's provisioning check failed: either a real shard_count disagreement against the durable record (that one request fails, per above), or an unreadable record (corrupt or a future format version) caught on the maintain per-tenant loop, which skips that tenant's tick rather than failing a request. Either way, a nonzero increase means a config-vs-data problem an operator must reconcile, not a rate to threshold. |

**Adopting pre-ADR data.** A (tenant, signal) that already had data before
this record existed is adopted the first time a server ingests or maintains
it, or deliberately ahead of a rollout with `ravel-cli provision adopt
--tenant <name> --shards <n>`. Adoption writes the record from `--shards`
only when every observed shard index is below it; if any observed index is at
or above `--shards`, adoption refuses and writes nothing (the value is
provably hiding data). Run `provision adopt` before rolling out a version
that will enforce the record, so an adoption refusal surfaces as a CLI error
you can act on rather than a server that refuses to start mid-rollout.

## Durable GC config (ADR-0050 section 4)

`protection_horizon >= max_query_duration + grace` is what keeps the GC sweeper
from deleting a segment a pinned in-flight reader still needs. Before ADR-0050
these four values lived in three unlinked per-process configs (the maintain
sweep config, the query engine deadline, the Flight SQL ticket ceiling) that
could be deployed independently, with nothing validating the constraint. They
are now recorded once, deployment-wide, in a durable object `sys/gc` at the
bucket root, and every mode validates itself against it at startup.

**Bootstrap is automatic and never blocks a fresh deployment.** The first
process to touch a fresh bucket writes `sys/gc` from the maintain defaults
(which satisfy the constraint by construction), then validates against the
object it just wrote. If several processes start together against one empty
bucket (a fresh operator-managed cluster's gateway, query, and maintain pods),
one wins the `CreateIfAbsent` and the others re-read and validate against the
winner's object. A fresh, never-bootstrapped bucket does not fail startup for
any process; only a *present* object a mode really violates refuses.

**What each mode validates:**

- **maintain**: its configured `protection_horizon` and `grace` must EQUAL the
  stored values (they are must-match, not independent knobs). A process flag
  that merely satisfies the inequality but differs from the durable value still
  refuses.
- **query modes** (`--mode query`, `--mode all`): the engine deadline must be
  `<= max_query_duration`.
- **Flight SQL** (only when built with the `flight-sql` feature): the ticket-TTL
  ceiling must be `<= protection_horizon - grace`. The server sources this
  ceiling from `sys/gc` rather than a hardcoded default, so it tracks the
  durable authority automatically.

**A startup refusal from a GC-config mismatch** names the configured and stored
values and the exact rule violated. It is not transient and does not clear on
restart: `sys/gc` records the deployment's true GC configuration. The fix is to
align the process's configuration with the durable object, or to change the
durable object deliberately (below) if the new values are intended.

**Inspecting and changing `sys/gc`.** `ravel-cli gc-config show` prints the
stored values (and whether the bucket is bootstrapped yet). `ravel-cli
gc-config set --protection-horizon 25h --grace 24h --max-query-duration 1h
--max-flush-lifetime 1h` is the single mutation path: it enforces
`protection_horizon >= max_query_duration + grace` at write time (refusing a
violating proposal without writing anything) and swaps the durable object with
`CasVersion`, so a concurrent `gc-config set` is caught as a conflict rather
than silently overwritten. Every `sys/gc` value must be strictly positive; a
set with a zero or negative duration is refused at write time (an all-zero
config would trivially satisfy the horizon constraint yet be impossible for any
mode to match, so it is rejected rather than written).

After changing `sys/gc`, every mode's process configuration must be brought
into line with it, or those processes will refuse to start against the new
object. The `ravel-server` binary exposes one `--gc-*` flag per knob for
exactly this, each a humantime duration defaulting to its shipped value (so a
process that sets none of them is unchanged):

- `--gc-protection-horizon` and `--gc-grace` feed the maintain compactor and
  must be set to EQUAL the durable `protection_horizon` and `grace`
  (must-match); set them to whatever the last `gc-config set` wrote.
- `--gc-max-query-duration` sets the enforced deadline for every query engine
  this process builds -- PromQL, SQL, and Flight SQL alike, all from the same
  resolved value -- and must be kept `<=` the durable `max_query_duration`
  (default 1h). A value ABOVE `max_query_duration` is REJECTED at startup (a
  hard error from `validate_query`), never clamped down: raise the durable
  `max_query_duration` first with `ravel-cli gc-config set`, then set this flag,
  or lower this flag if a `gc-config set` tightened `max_query_duration` below
  the current deadline.
- `--gc-max-flush-lifetime` sets the compactor's flush lifetime (seal margin
  and orphan age gate); not part of the must-match set, but kept in the same
  group.

Each flag feeds both the startup validation and the real compactor/query
engine, so a value that passes validation is the value actually enforced.

The Kubernetes operator does not expose GC-horizon flags in its CRD, and does
not need to: it deploys every pod with the same shipped defaults, so the first
pod bootstraps `sys/gc` from those defaults and every pod validates trivially.
`spec.retention.default` is age-based retention (ADR-0019), a separate concept
from these GC-safety horizons and unrelated to `sys/gc`.
## POSTINGS indexed-field metrics (ADR-0049)

POSTINGS gives the log store exact block-level pruning for an attribute
equality predicate. An operator names the indexed fields with
`--indexed-field` and `--indexed-field-tenant` (see the flag table above).
Indexing is opt-in per field. An unindexed field still works through the
bloom and the exact scan. A missing index changes query cost, not query
correctness (ADR-0049 decision 5).

### Write-side POSTINGS metrics

`ravel_logs_postings_*` renders at `GET /metrics` for the log ingest
pipeline. Every sample carries `mode` and `signal` labels. Each name is a
counter, cumulative over flushed log objects that carried a POSTINGS
section:

- `ravel_logs_postings_objects_total`: flushed log objects that carried a
  POSTINGS section.
- `ravel_logs_postings_bytes_total`: encoded POSTINGS section bytes, summed
  across flushed objects.
- `ravel_logs_postings_indexed_fields_total`: indexed fields that emitted a
  posting list, summed across flushed objects.
- `ravel_logs_postings_distinct_values_total`: distinct values across
  non-capped indexed fields, summed across flushed objects.
- `ravel_logs_postings_capped_fields_total`: indexed fields dropped from a
  flushed object for exceeding the per-field distinct-value cap (ADR-0049
  decision 4).

### Dynamic-column budget metrics (ADR-0100)

Each RLOG object gives the first `max_dynamic_columns` (default 1000) distinct
`(name, type)` attribute pairs a real typed column, ordered lexicographically
by name bytes then type byte; the rest fold into the `attrs_raw` overflow
column and lose columnar access. These `ravel_logs_dynamic_columns_*` metrics
render at `GET /metrics` for the log ingest pipeline with `mode` and `signal`
labels, so an operator can see a load approaching or crossing the budget
instead of it being silent (ADR-0100 decision 1).

- `ravel_logs_dynamic_columns_used_total` (counter): distinct `(name, type)`
  pairs that received a real dynamic column, summed across flushed objects.
- `ravel_logs_dynamic_columns_overflowed_total` (counter): distinct
  `(name, type)` pairs that overflowed the budget and folded into `attrs_raw`,
  summed across flushed objects. Nonzero means some loads crossed the budget.
- `ravel_logs_dynamic_columns_used_max` (gauge): the largest per-object used
  count seen so far. It rises toward `max_dynamic_columns` before any object
  overflows, so it signals budget pressure a total cannot show.

### Query-side prune-selectivity metrics

`ravel_logs_prune_*` renders at `GET /metrics` for the logs query path.
Every sample carries a `mode` label and a constant `signal="logs"` label.
Each name is a counter, cumulative across queries:

- `ravel_logs_prune_blocks_total`: blocks a logs scan considered before
  postings pruning. It is the denominator of prune selectivity.
- `ravel_logs_prune_blocks_survived_total`: blocks that survived postings
  pruning. The scan then read these blocks. It is the numerator of prune
  selectivity.
- `ravel_logs_prune_blocks_pruned_by_postings_total`: blocks the POSTINGS
  index dropped before the scan read them.

Prune selectivity is `blocks_survived` divided by `blocks_total`. A ratio of
1.0 means the query pruned no blocks. A lower ratio means POSTINGS did more
work.

## Declared typed attribute columns (ADR-0090)

The `logs` SQL table exposes every attribute through one merged
`attrs: Map(Utf8, Utf8)` column, so a numeric or boolean comparison over an
attribute is a `CAST(attrs['k'] AS ...)` over a stringified value. An operator
*declares* a per-tenant set of attribute keys as native typed columns, appended
after `attrs` in declaration order, and the same value then reads back as a
real `Int64`/`Boolean`/`Dictionary(Int32, Utf8)` (for a `str` column)/`Binary`
Arrow column. A declared `str` column is dictionary-encoded and stays a
dictionary over the Flight SQL wire. HTTP JSON row values are unchanged (a
string per row), but the JSON envelope's declared `columns[].type` reads
`Dictionary(Int32, Utf8)` rather than `Utf8`, and the Arrow IPC schema and
batch columns carry the dictionary type verbatim -- both client-visible changes
from the plain `Utf8` column a consumer must expect. A declared key still
appears in `attrs` as well, so a `SELECT attrs` or `SELECT *` query keeps
working.

Two ways to declare, one resolution:

- The process flags `--typed-attr-column` and `--typed-attr-column-tenant`
  (see the flag table above) are the deployment default and its per-tenant
  override. Changing them is a restart.
- The durable per-tenant record `TenantConfig.typed_attr_columns`, written by
  `ravel-cli typed-attr-column set`, is the no-restart path. When present it
  replaces the flag-derived declaration for that tenant outright, **including
  when it is present but empty**: an empty declaration means "this tenant
  declares nothing", which is a different state from having no durable override
  at all (in which case the flags apply). This is the same
  present-but-empty-versus-absent distinction `indexed_fields` carries in the
  same record.

### The staleness contract

A query-serving process reads the durable override cache-aside, per tenant, on
a 60s staleness horizon: a resolution newer than that is served from cache with
no store read, and a stale or missing entry triggers one `TenantConfig` GET on
the query's own path. So an operator's `typed-attr-column set` takes effect
within 60s, and during that window two replicas can answer the same query
against different declarations. A failed read never fails a query: the process
serves the last declaration it successfully resolved, or the flag-derived one if
it has never resolved for that tenant, and a failed read is not retried for one
second (so a degraded config store costs at most one failed GET per tenant per
second).

That fallback is a real degradation, so it is counted, never silent:

- `ravel_typed_attr_columns_stale_fallback_total` (counter, labels `mode` and a
  constant `signal="logs"`) counts every resolution served from a stale cache
  entry, a backoff-suppressed read, a failed `TenantConfig` read, or a durable
  declaration that failed validation.

A brief rise right after a config write is expected. A counter that keeps
climbing means the tenant config object is unreadable and the declarations in
effect are not the ones written: page on a sustained increase.

### Cost note

Declaring a key does not make predicates on it faster, and for equality it
makes them slower until typed-predicate pushdown lands: `attrs['k'] = 'v'`
prunes blocks through POSTINGS today, while `k = 'v'` on the declared column is
evaluated as a residual filter above the scan. Declare for typed comparisons
and aggregates (`k > 5`, `SUM(k)`), which are impossible over the map, not to
speed up an equality that already prunes.

## At-rest integrity scrubber (ADR-0059)

Ravel's checksum hierarchy (whole-object blake3 at write time, footer/section
crc32c on read) is otherwise verified only when a query happens to touch the
covered bytes, so bytes nobody queries are never checked. The scrubber
re-verifies them on a schedule instead. It runs only in `--mode maintain` (the
one mode that runs background housekeeping over durable objects, independent of
ingest and query traffic), spawned per process alongside the maintenance loop.

Each tick it re-discovers tenants from storage and, for every `(tenant, signal,
shard)`, verifies a bounded slice of the shard's committed L0 data objects: a
footer/section crc re-check plus a whole-object blake3 rehash against the
recorded content hash. A persisted per-shard cursor
(`t/<hash>/<sig>/maint/scrub/<shard>.cursor`) advances the slice each tick so a
full rotation over the corpus completes in about the configured period `P`.
Detection only: an anomaly is reported, never auto-repaired (there is no
redundant copy to repair a corrupt segment from, ADR-0058).

### Sizing `--scrub-period`

`P` is the operator-facing budget knob. Because the content tier must read each
object in full to rehash it, sustained scrub read bandwidth is

```
sustained scrub read bandwidth = corpus_bytes / P
```

so a larger corpus or a shorter `P` costs proportionally more read bandwidth,
and `P` is the worst-case staleness before any given object is re-verified.
Default `P = 7d`. This is the first scheduled task whose cost scales with data
volume rather than metadata volume (ADR-0059 consequences): size `P` against
the corpus the same way `--admission-reconcile-interval` (`R`) is sized, and
watch `ravel_scrub_cursor_position` (below) to confirm rotations keep pace.

### Metrics

Rendered at `GET /metrics` only in `--mode maintain`. Every sample carries a
`mode` label and a `signal` label; there is deliberately no `tenant_hash` label
on this unauthenticated route (ADR-0044 section 4), matching every other
maintenance family.

- `ravel_scrub_checksum_mismatch_total{signal}` (counter): data objects that
  failed at-rest integrity re-verification — a whole-object blake3 mismatch
  against the recorded content hash (bit rot or a partial write), or a
  footer/section crc failure. Both are data-object corruption, so both land on
  this one counter.
- `ravel_scrub_postings_disagreement_total{signal}` (counter): objects whose
  covering name-postings object omitted a `__name__` the object really carries
  (a false negative).
- `ravel_scrub_seal_divergence_total{signal,reason}` (counter, ADR-0059 decision
  2): divergences between the folded snapshot and the re-listed sealed commit
  history, checked once per tick on the fold cadence (metadata-cost, not gated
  behind the content-tier cursor). `reason="missing"` is a sealed commit record
  absent from the snapshot (a folder under-count); `reason="mismatched"`
  is a snapshot entry whose `content_hash` disagrees with the sealed record.
  Orphaned entries — a snapshot entry with no surviving commit record — are the
  expected shape once retention deletes a folded commit record and are never
  counted (no `reason="orphaned"` value exists). This is the scheduled form of
  `ravel-cli catalog verify`, which stays for manual/ad-hoc use.
- `ravel_scrub_cursor_position{signal}` (gauge, `[0,1]`): fraction of the
  current rotation the content-tier cursor has covered so far.

| Alarm | Rule | Why this rule |
|---|---|---|
| Checksum mismatch | `increase(ravel_scrub_checksum_mismatch_total[1h]) > 0` | There is no redundant copy to repair from, so any nonzero increase is at-rest corruption an operator must investigate immediately, not a rate to threshold. |
| Postings disagreement | `increase(ravel_scrub_postings_disagreement_total[1h]) > 0` | A false negative means a query filtering on that name silently skips matching data; any nonzero increase is a correctness bug to page on. |
| Seal divergence | `increase(ravel_scrub_seal_divergence_total[1h]) > 0` | A `missing` or `mismatched` divergence means the folded snapshot under-counts the sealed commit history (late-commit seal loss); a query reading the snapshot silently omits committed data. Page on any nonzero increase. |
| Scrub falling behind | `ravel_scrub_cursor_position` stuck near 0 across a period longer than `P` | A rotation that never advances means scrubbing is not keeping pace with `P`; the effective staleness bound is no longer `P`. |

### Storage credential impact (ADR-0055)

The scrubber's reads (commit records under `c/`, data objects under `l0/`) are
already covered by the Maintain role's existing `MaintainRead`/`MaintainList`
grants. Its one write — the per-shard cursor — is placed under the existing
`maint/` control prefix (`t/<hash>/<sig>/maint/scrub/<shard>.cursor`), so it
falls under the Maintain role's existing `t/*/*/maint/*` `PutObject` grant with
**no new IAM prefix**: ADR-0055's Maintain grants are explicit per-prefix (not a
blanket `t/*/*/`), and `maint/` is one they already name. No storage-policy
change is required to enable the scrubber.

## Storage backend configuration

**MinIO (local development):** see
[deploy/docker-compose/minio.yml](../../deploy/docker-compose/minio.yml) and
[docs/guides/getting-started.md](getting-started.md#bring-up-minio). Point
both `ravel-server` and `ravel-cli` at it with:

```sh
--store s3 --s3-endpoint http://127.0.0.1:9000 --s3-bucket ravel-dev \
--s3-access-key ravel --s3-secret-key ravel-dev-secret
```

**AWS S3:** omit `--s3-endpoint` (S3 is the default when unset), and supply
a real bucket, region, and credentials:

```sh
--store s3 --s3-bucket my-ravel-bucket --s3-region us-west-2 \
--s3-access-key AKIA... --s3-secret-key ...
```

Ravel does not use the AWS credential chain (profiles, instance roles,
`AWS_ACCESS_KEY_ID`). It reads only the `RAVEL_S3_*` flags/env above.

The examples above use one credential for everything, which is the simplest
deployment and still fully supported. It is not the only option. Each Ravel
process only ever touches the subset of the bucket its role needs, so you can
hand each process a distinct, narrower S3 credential instead of one
bucket-wide key. The `RAVEL_S3_*` contract does not change: every process
still reads exactly one access-key/secret pair; you just provision a
different, tighter pair per role. See "Storage credential roles" immediately
below for the four roles and their exact IAM/MinIO policies.

## Storage credential roles (ADR-0055)

Every Ravel process holds one S3 credential and uses it for every object-store
call it makes. With a single bucket-wide credential, a leak from any one
process can read, overwrite, or delete anything in the bucket. ADR-0055 scopes
credentials to the process roles the system already has, so a leaked credential
can only do what that one role legitimately does, and only Maintain can delete
anything at all.

This is enforced entirely at the storage backend's own IAM/bucket-policy layer
(AWS IAM, or MinIO policies for dev/CI). Ravel's code is unchanged: there is no
in-process authorization check, no new service, and no change to the
`RAVEL_S3_*` flag contract. You provision a narrower credential per role the
same way you already provision the bucket, and attach the policies below.

### The four roles

| Role | Process / `--mode` | Deployment | What it does |
|---|---|---|---|
| **Gateway** | `--mode gateway` (and the gateway half of `--mode all`) | `<name>-gateway` | Serves OTLP ingest: writes L0 segments and their L0 commit records, idempotency markers, and the tenant's provisioning record on adopt. Also runs the catalog fold task, so it writes catalog snapshot parts, `HEAD`, and name-postings index objects. |
| **Query** | `--mode query` (and the query half of `--mode all`) | `<name>-query` | Serves `/api/v1/*` reads: lists and reads commit records and catalog objects. Also runs fold (same catalog writes as Gateway) and appends query-audit records. |
| **Maintain** | `--mode maintain` | `<name>-maintain` | Runs the background maintenance loop: compaction (writes L1 parts and compaction records), retention (writes tombstones), and the sweeper. **The only role that may delete anything**, and only under `l0/`, `l1/`, `c/`, `idem/`. |
| **Admin** | `ravel-cli` subcommands | none (out of band) | One-off bootstrap and mutation commands (`store qualify`, `gc-config set`, `provision adopt`/`reshard`, `hold set`/`clear`). Invoked by an operator or CI job, never a long-running server. It is the broadest of the four credentials and is **not** managed by the Kubernetes operator; see "The Admin credential" below. |

Gateway and Query both run the catalog fold task (`fold::spawn` runs in every
mode except Maintain), which is why both hold the same catalog write grants,
not just Query. This is not a bug in the split; it matches what the shipped
topology actually does.

### Object-key layout these policies reference

The policies below map the ADR's per-role grants onto the actual object-key
layout (docs/catalog-and-mvcc.md). All tenant data lives under
`t/<tenant_hash>/`; a single bucket holds every tenant, so the policies use
`t/*` to span all tenants. `<sig>` is the one-letter signal segment (`m`
metrics, `l` logs, `s` spans). Control objects live at the bucket root under
`sys/`.

| ADR shorthand | Actual key prefix | Wildcard used below |
|---|---|---|
| `l0/` | `t/<hash>/<sig>/l0/…` | `t/*/*/l0/*` |
| `c/` (commit / compaction / tombstone) | `t/<hash>/<sig>/c/…` | `t/*/*/c/*` |
| `l1/` | `t/<hash>/<sig>/l1/…` | `t/*/*/l1/*` |
| `idem/` | `t/<hash>/<sig>/idem/…` | `t/*/*/idem/*` |
| `maint/<shard>/cursor` | `t/<hash>/<sig>/maint/…` | `t/*/*/maint/*` |
| `maint/scrub/<shard>.cursor` | `t/<hash>/<sig>/maint/scrub/…` | `t/*/*/maint/*` |
| `admission/` | `t/<hash>/<sig>/admission/<process_id>.snapshot` | `t/*/*/admission/*` |
| `prov` | `t/<hash>/<sig>/prov` | `t/*/*/prov` |
| `catalog/<sig>/…` | `t/<hash>/catalog/<sig>/…` | `t/*/catalog/*/*` |
| audit prefix (`u/…`), read/write | `t/<hash>/u/…` | `t/*/u/*` |
| legal-hold shard (deny-delete) | `t/<hash>/u/*/0000/…` | `t/*/u/*/0000/*` |
| query-audit shard (Maintain delete) | `t/<hash>/u/*/0001/…` | `t/*/u/*/0001/*` |
| erasure request + completion (`del/…`) | `t/<hash>/<sig>/del/…` | `t/*/*/del/*` |
| erasure request only (`.dreq`, Maintain delete) | `t/<hash>/<sig>/del/*.dreq` | `t/*/*/del/*.dreq` |
| erasure completion (`.done`, deny-delete) | `t/<hash>/<sig>/del/*.done` | `t/*/*/del/*.done` |
| tenant discovery (delimited list) | `t/` | `t/` |
| `sys/tenancy`, `sys/qualification`, `sys/gc` | bucket root | as written |

Tenant discovery (`ravel-maintain::discover_tenants`, and the Gateway/Query
fold and cache-warm code that calls it) lists the bare, delimited `t/`
prefix — not a per-tenant subpath. Under AWS `StringLike`, none of the
`t/*/…` wildcards above match the literal string `t/`, so every role that
performs discovery (Gateway, Query, Maintain) needs a separate `t/` entry
in its `ListBucket` condition alongside the per-key wildcards (ADR-0072
decision 5). This does not widen what those roles can read: listing a
prefix only enumerates keys, it does not grant `GetObject` on them, and
each role's read grants are unchanged.

The `c/` commit prefix (`t/<hash>/<sig>/c/…`) and the catalog prefix
(`t/<hash>/catalog/<sig>/…`) are distinct paths: no wildcard for one matches
the other, so Maintain's delete grant on `c/` never reaches catalog objects.

The audit prefix (`t/<hash>/u/…`) holds two fixed control-plane shards that the
policies treat differently on delete (ADR-0055 §3). Every audit object key
carries its shard as a four-digit segment
(`t/<hash>/u/<l0|c|l1>/<shard>/…`), so `0000` and `0001` name disjoint paths a
wildcard can separate:

- **Legal-hold shard `0000`** (`t/*/u/*/0000/*`): hold set/clear records. Every
  role — including Maintain — denies delete here, so a legal hold can never be
  destroyed. This is the only audit path in `DenyDeleteProtected`.
- **Query-audit shard `0001`** (`t/*/u/*/0001/*`): the append-only query
  activity log. It is compacted and age-swept on a 90-day retention window by
  the Maintain process (`ravel-maintain`'s `sweep_audit_retention`), so
  **Maintain — and only Maintain — grants delete on it** (`MaintainDelete`).
  Because `0000` and `0001` are separate key paths, Maintain's delete grant on
  `0001` never reaches the legal-hold shard, and the `0000` deny never blocks a
  query-audit sweep.

`CreateIfAbsent`, `CasVersion`, and plain `Put` are all `s3:PutObject` at the
IAM layer; the difference between them is a request precondition header
(`If-None-Match`, a version id), not a separate IAM action. So a role's write
grant is a `PutObject` allow on its write prefixes; the create-only / CAS
semantics are enforced by Ravel's own request, not by the policy. The one
place this matters: Gateway writes only L0 commit records under `c/` (never
compaction records), but both are `.cmt` objects under the same `c/` prefix,
so the policy grants `PutObject` on `c/` as a whole and the L0-only restriction
is a code invariant, not an IAM one.

### AWS IAM policies

One policy document per role, checked into [`deploy/iam/`](../../deploy/iam/)
rather than inlined here, so a policy edit is a diff `ravel-commit`'s
`tests/iam_templates.rs` can check against the object-key layout in CI
(ADR-0072 decision 5) instead of hand-transcribed prose going stale. Replace
`my-ravel-bucket` with your bucket in each file. Attach each to the IAM
principal (user or role) whose access key you put in that role's Kubernetes
Secret. Every policy — including Gateway's, Query's, and Admin's, which have
no delete grant at all — carries the same explicit `Deny` on
`s3:DeleteObject`/`s3:DeleteObjectVersion` over the six protected prefixes
(ADR-0055 section 3). An explicit `Deny` overrides any
`Allow`, so this makes those prefixes undeletable even by a role that otherwise
has delete rights (Maintain). The sixth of those prefixes is the audit
**legal-hold shard** only (`t/*/u/*/0000/*`), not the whole audit prefix: it
was narrowed there so the query-audit shard (`t/*/u/*/0001/*`) can
be age-swept, and added that query-audit path to Maintain's delete grant alone.

**Gateway:** [`deploy/iam/gateway.json`](../../deploy/iam/gateway.json).

The `GatewayList` statement's `s3:prefix` condition carries the bare `t/`
discovery entry described above, alongside the per-key wildcards:

```json
"s3:prefix": ["t/", "t/*/*/l0/*", "t/*/*/c/*", "t/*/*/admission/*", "t/*/catalog/*/*"]
```

`GatewayRead` includes `t/*/catalog/*/*`: fold runs in Gateway mode as well
as Query mode (it is not a query-only responsibility, ADR-0055 section 1),
and folding incrementally means reading the prior HEAD, snapshot parts, and
name postings before writing the next ones — not only writing new output.

Gateway also carries `t/*/*/admission/*` on all three of list, read, and
write (ADR-0057): the fleet-global admission reconciliation loop writes this
process's own usage snapshot every reconciliation interval, and lists then
reads every sibling process's snapshot under the same per-(tenant, signal)
prefix to compute the fleet-wide share the local admission check enforces
against. Each snapshot key is owned exclusively by one process
(`t/<hash>/<sig>/admission/<process_id>.snapshot`), so the write is a plain
overwrite with no CAS. Query does not get this grant: reconciliation is
wired only in Gateway (and the gateway half of `--mode all`), the same modes
that construct the `AdmissionController` at all, so Query and Maintain never
run it. This keyspace is not one of ADR-0055's six protected prefixes and is
absent from `DenyDeleteProtected`: nothing deletes it (ADR-0057 section 5
leaves stale snapshots to staleness detection rather than a sweep), so no
role holds a delete grant for it to need denying.

**Query:** [`deploy/iam/query.json`](../../deploy/iam/query.json).

The `QueryList` statement's `s3:prefix` condition carries the same bare
`t/` discovery entry:

```json
"s3:prefix": ["t/", "t/*/*/c/*", "t/*/catalog/*/*", "admission/query/*"]
```

`QueryRead` includes `t/*/*/l0/*` and `t/*/*/l1/*`: the query fetchers GET
segment data directly (footer-first ranged reads) once Phase 1 resolve has
found the relevant commit records under `c/*` — `c/*` alone names the
records, it is not the data those records point at.

`QueryList`/`QueryRead`/`QueryWrite` also carry `admission/query/*` — a
bucket-**root** prefix, not under any `t/<hash>/` tenant prefix. This is the
fleet-global query concurrency ceiling's keyspace (ADR-0061 decision 2,
`admission/query/<process_id>.snapshot`): each query-serving process writes its
own in-flight query count to its own key (PutObject) and lists/reads its
siblings' keys (ListBucket/GetObject) to compute the fleet-wide share the local
admission check enforces against, on the same ADR-0057 reconciliation pattern
the ingest admission uses. It is deliberately root-scoped rather than
tenant-scoped because the ceiling is fleet-global, not per-tenant: the resource
being bounded is aggregate query fan-out across all tenants, which has no single
tenant to scope under. This is distinct from the per-tenant ingest admission
grant `t/<hash>/<sig>/admission/*` above, which only Gateway holds: that one
reconciles ingest active-series/rate caps and is never touched by the query
path. Query (and the query half of `--mode all`) holds the `admission/query/*`
grant because reconciliation for the query ceiling is wired only in the
query-serving modes. Like the ingest admission keyspace, `admission/query/*` is
not one of ADR-0055's six protected prefixes and is absent from
`DenyDeleteProtected`: nothing deletes it (a stale snapshot is handled by the
`2R` staleness window, not a sweep), so no role holds a delete grant for it to
need denying.

**Maintain:** [`deploy/iam/maintain.json`](../../deploy/iam/maintain.json).

The `MaintainList` statement's `s3:prefix` condition carries the same bare
`t/` discovery entry:

```json
"s3:prefix": ["t/", "t/*/*/l0/*", "t/*/*/c/*", "t/*/*/l1/*", "t/*/*/idem/*"]
```

Maintain's `MaintainDelete` grants delete on `l0/`, `l1/`, `c/`, `idem/` — the
four prefixes Ravel's own sweep and retention code deletes from today — plus
the query-audit shard `t/*/u/*/0001/*`: the Maintain process
compacts and age-sweeps the query-audit activity log on its own 90-day window
(`ravel-maintain`'s `sweep_audit_retention`), so it needs delete there. The
`DenyDeleteProtected` block still applies to Maintain: its `c/` delete grant
covers commit records, compaction records, and tombstones
(`t/<hash>/<sig>/c/…`), but the catalog objects at `t/<hash>/catalog/<sig>/…`
are a different path the `Deny` protects, so Maintain cannot delete them. The
same is true within the audit prefix: the deny covers only the legal-hold shard
`t/*/u/*/0000/*`, a separate path from the query-audit shard `…/0001/*`
`MaintainDelete` grants, so Maintain can age-sweep query-audit records but can
never delete a legal-hold record.
`MaintainRead` includes `t/*/*/l1/*` (the lost-CAS-race convergence path
HEADs a compacted part to re-verify it exists before retrying a publish) and
`t/*/*/maint/*` (the advisory scan cursor is read before its own CAS
mutation).

**Admin (`ravel-cli`):** [`deploy/iam/admin.json`](../../deploy/iam/admin.json).

Admin's `AdminList` condition (`s3:prefix": ["t/*", "sys/*"]`) already
admits the bare `t/` discovery listing incidentally — `t/*` matches `t/`
under `StringLike` — but Admin performs no tenant discovery, so this is
unchanged from before ADR-0072.

Admin reads everything (broad `GetObject` on `t/*` and `sys/*`, including the
single-key `idem/` inspect) and writes only the control objects its commands
mutate. It has no delete grant: delete stays exclusively Maintain's.
`AdminWrite` includes `sys/qualify/*`: `ravel-cli store qualify` writes
transient scratch objects under `sys/qualify/<run-id>/` while running its
conformance suite, not only the final `sys/qualification` record. Since
Admin's policy grants no delete anywhere, that scratch is never cleaned up
by the credential itself — it is bounded (one run's worth of small objects
per `store qualify` invocation) and harmless to leave, but an operator who
runs `store qualify` repeatedly against the same bucket will see it
accumulate.

`AdminWrite` includes `t/*/*/c/*`: `ravel-cli commit reconstruct` (ADR-0058,
the commit-record reconstruction tool) writes rebuilt L0 commit records back
under the `c/` prefix, the same prefix Gateway writes L0 commit records to at
ingest time. This is the one Admin write that touches a tenant data prefix
rather than a `sys/`, `prov`, or audit object. It is create-only in practice
(the tool only ever writes `CreateIfAbsent` and reports a conflict rather than
overwriting an existing record), and it grants no delete: `c/` is not one of
the deny-delete prefixes (Maintain legitimately sweeps superseded records
there), so Admin's continued absence from any delete grant is unchanged. See
ADR-0055's 2026-08-07 amendment for the rationale.

### Selective erasure grants (ADR-0064)

Selective subject erasure (GDPR/DSAR deletion, ADR-0064) adds one new object
prefix, `t/<hash>/<sig>/del/`, holding two key suffixes: the erasure request
`<request_id>.dreq` (which contains the subject identifier) and its completion
marker `<request_id>.done` (which does not). The rewrite pass and physical
sweep that erasure drives write and delete only under `l0/`, `l1/`, and `c/` —
prefixes Maintain already has — so no policy change is needed for those. Only
the `del/` prefix needs new grants, scoped exactly to the ADR-0055 amendment
(EJ, 2026-08-13):

- **Admin** creates the `.dreq` (`ravel-cli erase submit`) and deletes
  nothing.
- **Query** and **Maintain** read `del/` to attach pending predicates at
  resolve time and to scope the rewrite pass.
- **Maintain** deletes the `.dreq` **only** (after its `.done` exists and the
  protection horizon passes), and no role — Maintain included — may delete a
  `.done`.

Add each statement to the same policy file as the rest of that role's grants
(`deploy/iam/*.json`); the JSON below shows the statements to add, with
`my-ravel-bucket` standing in for your bucket. The `.dreq` and `.done`
suffixes are disjoint key paths, so the Maintain `.dreq` delete allow and the
`.done` deny never overlap.

`AdminErasureSubmit` — add to `deploy/iam/admin.json` (create-only, no
delete):

```json
{
  "Sid": "AdminErasureSubmit",
  "Effect": "Allow",
  "Action": "s3:PutObject",
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*"
}
```

`QueryErasureRead` / `MaintainErasureRead` — add to `deploy/iam/query.json`
and `deploy/iam/maintain.json`; add `t/*/*/del/*` to each role's existing
`ListBucket` `s3:prefix` condition as well:

```json
{
  "Sid": "ErasureRead",
  "Effect": "Allow",
  "Action": "s3:GetObject",
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*"
}
```

`MaintainErasureDeleteRequest` — add to `deploy/iam/maintain.json`. Delete is
granted on the `.dreq` suffix only, never on `del/*` as a whole, so it can
never reach a `.done`:

```json
{
  "Sid": "MaintainErasureDeleteRequest",
  "Effect": "Allow",
  "Action": "s3:DeleteObject",
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*.dreq"
}
```

`DenyDeleteErasureCompletion` — add to **every** role's policy
(`gateway.json`, `query.json`, `maintain.json`, `admin.json`), alongside the
existing `DenyDeleteProtected` block. An explicit `Deny` overrides any
`Allow`, so a `.done` marker is undeletable even by Maintain, whose
`.dreq`-suffixed allow above does not match it:

```json
{
  "Sid": "DenyDeleteErasureCompletion",
  "Effect": "Deny",
  "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
  "Resource": "arn:aws:s3:::my-ravel-bucket/t/*/*/del/*.done"
}
```

### Per-tenant SSE-KMS routing (ADR-0062 decision 1, ADR-0072 decision 2)

The `KmsRoutingStore` decorator
(`crates/ravel-object-store/src/kms_routing.rs`) wires into `ravel-server`'s single
store-construction site. Two independent flags, both off by default:

- `--s3-kms-key <arn>`: single-key SSE-KMS on the one default `S3Store`
  every deployment already builds (ADR-0062 decision 1c). No routing, no
  new object key, no `KmsRoutingStore` — every PUT this process makes is
  simply encrypted with this one key.
- `--tenant-kms-config <path>`: a TOML file naming per-tenant keys. Only
  when this is set does `build_store` insert `KmsRoutingStore` between the
  default `S3Store` and `InstrumentedStore`. It routes `put`/`put_multipart`
  for a configured tenant's `t/<hash>/` keys to a lazily-built `S3Store`
  constructed with that tenant's own `kms_key_id`; every other tenant, and
  every non-write operation, falls through to the default store unchanged.

```toml
# --tenant-kms-config kms-tenants.toml
[tenants]
acme = "arn:aws:kms:us-east-1:111122223333:key/acme-key"
other = "arn:aws:kms:us-east-1:111122223333:key/other-key"
```

On first configuration of a tenant's key (and on every later rotation to a
different key), the same startup path bootstraps that tenant's key-epoch
history at `t/<hash>/enc` (`crates/ravel-catalog/src/key_epoch.rs`): epoch 0
is recorded with an empty `key_arn` (the deployment-default-key convention,
ADR-0062 decision 1c) and `activated_ns = 0` — the start of Unix time, which
is trivially at or before any tenant's actual earliest live object, so
`verify-custody`'s epoch-consistency check never sees a pre-existing object
that predates epoch 0. Epoch 1 follows immediately with the operator's real
key ARN and `activated_ns` set to the moment of configuration. A restart
with the same key is a no-op (the last recorded epoch already names it); a
restart with a different key appends a new epoch, the rotation record.

The `t/<hash>/enc` bootstrap write happens *before* `KmsRoutingStore` is told
to route that tenant's data through the new key (never the other way
around, so a crash between the two can never leave data flowing through a
key with no epoch record), which means the epoch record itself is written
through whichever key was in effect a moment earlier, not the key it is
in the process of establishing.

**KMS key policy.** ADR-0072 decision 2's posture: in a hostile
multi-tenant deployment, each tenant's KMS key policy should grant decrypt
only to the Ravel role principals of that specific deployment, so a leaked
role credential alone yields ciphertext — compromise requires the
credential *and* the KMS grant. A minimal per-tenant key policy, alongside
the IAM role templates above:

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

Scope `RavelRolesMayUseThisKey`'s principal list to only the roles a given
deployment actually runs with `--tenant-kms-config` set (§"Storage
credential roles" above) — a role that never routes writes through this key
gains nothing from being able to decrypt it, and every principal added here
widens the compromise blast radius this key policy exists to narrow.

**Role-side grant (required too).** The key policy above only
grants usage to the *principal*; AWS also requires the calling principal's
own IAM policy to allow the `kms:*` action, or the request is denied before
it reaches the key policy at all. Each `deploy/iam/*.json` role template
carries a matching `<Role>TenantKms` statement, scoped to the tenant key
ARNs rather than `"*"` (replace the placeholder
`arn:aws:kms:us-east-1:111122223333:key/*` with your account/region and
either your tenant keys' exact ARNs or an alias pattern your deployment
reserves for them):

- **Gateway, Maintain** (write tenant data objects under `t/<hash>/...`
  through `KmsRoutingStore`, and also read some of what they write — fold,
  compaction inputs): `kms:Encrypt`, `kms:GenerateDataKey*`, `kms:Decrypt`.
- **Query** (read-only against tenant data): `kms:Decrypt`.
- **Admin** (`GetObject`-only per ADR-0055): `kms:Decrypt`, deliberately
  without `kms:GenerateDataKey*` — granting it would let a leaked Admin
  credential mint ciphertext under tenant keys it has no write role for.

Without both halves — the key policy's principal grant *and* this role-side
statement — the first SSE-KMS `PutObject` a role makes against a
`--tenant-kms-config` tenant fails closed with `AccessDenied`, because
`KmsRoutingStore` routes that write through the per-tenant key
unconditionally once the tenant is configured (`crates/ravel-object-store/
src/kms_routing.rs`); there is no fallback to unencrypted or default-key
writes once `--tenant-kms-config` names a tenant.
`crates/ravel-commit/tests/iam_templates.rs` checks the KMS action sets
shipped in `deploy/iam/*.json` (which roles carry `kms:GenerateDataKey*`,
which are Decrypt-only) so a future template edit that drops one of these
grants fails CI instead of a production `--tenant-kms-config` rollout.

Query and Admin also hold narrow `PutObject` grants under `t/<hash>/...`
(Query: catalog snapshots/HEAD/idx, the query-audit shard; Admin: `c/*` via
`ravel-cli commit reconstruct`, and the audit prefix). Those writes route
through the same per-tenant key once one is configured, the same as any
other `t/<hash>/...` PUT, but this change does not add `kms:GenerateDataKey*`
to either role's grant — Query and Admin are scoped to `kms:Decrypt` per the
mapping above. A `--tenant-kms-config` deployment relying on those specific
write paths for a KMS-routed tenant should treat this as an open gap, not a
covered one, and grant `kms:GenerateDataKey*` to that role manually pending
a follow-up.

**Known gap, not fixed by this change:** the `t/<hash>/enc` key-epoch
record needs its own IAM read/write grant, which the shipped
`deploy/iam/*.json` policies do not yet carry — see ADR-0055's 2026-08-12
amendment for the exact grant and why updating those policy files is
out of this change's scope.

### MinIO policies (dev / CI)

MinIO's policy language **is** the AWS IAM JSON in `deploy/iam/`, verbatim:
same `Action` names (`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`,
`s3:ListBucket`), same `arn:aws:s3:::<bucket>/<prefix>` resources, same
explicit `Deny` semantics. To apply the four roles to a MinIO deployment,
load the files in `deploy/iam/` directly with `mc`:

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

`scripts/kind-up.sh` already provisions a MinIO backend for the local kind
environment (`RAVEL_FAKE_S3_BACKEND=minio`), but it uses a single shared
credential across all pods for development convenience. That is fine for dev
and CI: the per-role split is a production hardening, and nothing in the dev
environment needs it. The policies in `deploy/iam/` are how you would apply
the same four-role model against a MinIO-backed staging or production
deployment; do not modify `kind-up.sh` to adopt them for local development.

### First deployment against a fresh bucket

Two control objects are written by whichever process boots first against an
empty bucket, so their write grants are slightly broader than the strict
per-role tables imply (ADR-0055 section 4). You need to know this before your
first deployment against a fresh bucket:

- **`sys/tenancy`** is created (`CreateIfAbsent`) by whichever of the three
  server roles reaches a fresh bucket first — that is why Gateway, Query, and
  Maintain all carry a `PutObject` grant on `sys/tenancy`, not just Admin. This
  does not weaken the delete-deny boundary: `CreateIfAbsent` cannot overwrite
  or delete an existing object, and `sys/tenancy` is in the `Deny`-delete set
  for every role. The effect is only that a fresh operator-managed cluster
  boots without requiring you to run a manual `ravel-cli` bootstrap step first.
- **`sys/gc`** is bootstrapped (`CreateIfAbsent`) by Maintain on a fresh bucket
  — hence Maintain's `PutObject` grant on `sys/gc`. The *mutation* path that
  changes an existing `sys/gc` (`ravel-cli gc-config set`, a `CasVersion`
  overwrite) is Admin-only, matching that it is already an explicit
  CLI-operator action, not something any server does on its own.

`sys/qualification` gets no such exception. It is written exactly once, by
Admin's `ravel-cli store qualify` run, which a fresh production deployment must
run before any server can start (see "Store qualification" below). No server
role writes it, and no server-role policy grants `PutObject` on it.

### The Admin credential

`ravel-cli` uses the Admin role, and unlike the three server roles it is **not**
provisioned by the Kubernetes operator. There is no `RavelCluster` field for
it, and no pod runs it. It is the broadest of the four credentials — it can
read every prefix and write every control object — so treat it as a privileged
operator credential, not a service credential:

- Store it wherever your operators or CI jobs get their `RAVEL_S3_*` values for
  running `ravel-cli` (a CI secret store, an operator's short-lived session),
  never in a long-running Deployment or a Secret the operator mounts into a
  server pod.
- It is used only by out-of-band operator/CI invocations: `store qualify`,
  `gc-config set`, `provision adopt`/`reshard`, legal holds, and the read-only
  inspection subcommands. No continuously-running process should ever hold it.
- Even the Admin credential cannot delete any of the six protected prefixes
  (its policy carries the same `DenyDeleteProtected` block), and it cannot
  delete anything else either — it has no delete grant at all. A leaked Admin
  key can forge or overwrite control objects within its write grant, but it
  cannot make existing data disappear.

## Dedicated fragment listener TLS (ADR-0071 amendment decision 1)

Under `--distributed-query`, the cluster-internal `Pinned` fragment surface
(one query worker fetching a slice for another) moves off the public gRPC
listener onto a dedicated listener that terminates TLS in-process:
`--fragment-listener <addr>`, with `--fragment-tls-cert`, `--fragment-tls-key`,
and `--fragment-tls-ca`. The public gRPC listener then serves only `Resolve`
(cross-cluster federation) with ordinary tenant credentials and refuses
`Pinned`; the dedicated listener serves `Pinned` only and refuses `Resolve`.
Startup refuses a `--fragment-listener` address equal to `--listen-http`,
`--listen-grpc`, or `--mtls-listener`, so the separation holds by construction.

TLS here provides channel confidentiality (per-tenant, per-query capabilities
travel on it) and server authenticity (a coordinator confirms it dialed a real
cluster worker, not an interceptor that could harvest capabilities).
Authorization is always the capability, never the certificate: **coordinators
verify every worker certificate against the pinned `--fragment-tls-ca` with one
fixed expected server name, `ravel-fragment`**, carried as a dNSName SAN in
every worker certificate. Per-process certificate identity is deliberately not
required — any certificate the dedicated CA signed means "a fragment worker of
this cluster". This is not the external-proxy mTLS posture of ADR-0050: no
identity is ever parsed from a certificate.

**Ravel mints no certificates or keys.** The operator provisions the PEM files
out of band. The certificate/key are read once at startup: **certificate
rotation is a rolling restart** (there is no live reload in this release), which
cluster-internal CA lifetimes make an operator-scheduled event.

Requirements for the worker certificate:

- A `ravel-fragment` dNSName SAN (rustls verifies the SAN, not the CN).
- `extendedKeyUsage = serverAuth`.
- Signed by the CA distributed as `--fragment-tls-ca` to every query node.

### Kubernetes with cert-manager

Issue one certificate per query node (or a shared one, since identity is not
per-process) from a cluster-internal `Issuer`, with the fixed SAN:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ravel-fragment
spec:
  secretName: ravel-fragment-tls   # projects tls.crt, tls.key, ca.crt
  duration: 720h                    # 30d; rotation = a rolling restart
  renewBefore: 168h
  privateKey:
    algorithm: ECDSA
    size: 256
  usages:
    - server auth
  dnsNames:
    - ravel-fragment                # the one fixed expected server name
  issuerRef:
    name: ravel-fragment-ca         # a dedicated cluster-internal CA Issuer
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
cadence (for example, a `renewBefore` window ahead of `duration`).

### Hand-provisioned CA (no cert-manager)

Run a small cluster-internal CA by hand and issue a worker certificate with the
fixed SAN. With OpenSSL:

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
`fragment.crt`/`fragment.key` as `--fragment-tls-cert`/`--fragment-tls-key`.
Reissuing the worker certificate (or rotating the CA) takes effect on the next
rolling restart.

### Rolling deploy and mixed versions

The dedicated listener is opt-in per process. A query node without
`--fragment-listener` keeps the pre-amendment layout (the `Pinned` surface on
the public gRPC listener), so a fleet can be migrated one rolling restart at a
time: nodes that have the flag advertise their TLS fragment endpoint and refuse
`Pinned` on the public port, while nodes that do not keep serving it there.
Results stay byte-identical throughout; only which nodes a slice can fan out to
changes during the roll.

## Federating to a remote cluster (ADR-0071)

`--remote-cluster` points this coordinator at another Ravel cluster's fragment
`SeriesFetch` surface. One flag per remote:

```
ravel-server --mode query \
  --remote-cluster name=eu,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu.token \
  --remote-cluster name=apac,endpoint=apac.internal:9443,credential-file=/etc/ravel/apac.token,tls-ca-file=/etc/ravel/apac-ca.pem,soft-timeout=15s
```

The credential is an operator secret read from a file, never an inline value:
it is the principal the remote sees. A federated query never forwards the
calling client's credential across a cluster boundary.

### TLS is the default

Neither spec above names `tls`, and both dial `https://`. TLS is on unless the
spec says otherwise, verifying the remote against the system trust roots plus
`tls-ca-file` when one is set. A spec that carries `tls-ca-file` and no `tls`
key means "TLS on, with this CA trusted"; there is no need to pair the two.

`tls=false` is the escape hatch for a hop that is already encrypted and
access-controlled at a lower layer (a service mesh sidecar, an encrypted
tunnel). It is an explicit, logged choice: startup emits a warning naming that
remote, because with TLS off the operator bearer credential, the federated
query, and every returned result stream cross the network in cleartext, where
anyone on the path can read and replay the credential.

```
WARN SECURITY: --remote-cluster 'eu' is configured with tls=off. The operator
bearer credential presented to this remote, every federated query, and every
returned result stream travel in cleartext to 'eu.internal:9443'. ...
```

One such line is logged per plaintext remote; a TLS remote logs nothing. If you
see this warning and did not intend plaintext, drop the `tls=false` key. Setting
`tls=false` together with `tls-ca-file` fails startup outright, since the CA
bundle would be inert.

## Store qualification (ADR-0050 section 6)

Ravel's commit protocol and catalog assume the backing store honors conditional
writes (`CreateIfAbsent`/`CasVersion` reject a losing writer) and strong
read/list-after-write consistency. A backend that advertises these but does not
deliver them silently violates durability. Before a production store is trusted,
it is qualified empirically, once per bucket:

```sh
ravel-cli store qualify --store s3 --s3-endpoint ... --s3-bucket ...
```

On a pass, this records a durable `sys/qualification` object (backend identity,
suite version, timestamp) via `CreateIfAbsent`. It is once per bucket, never
per boot, and never overwritten: a second run leaves the existing record alone.

**A fresh production deployment must run `store qualify` before the server can
start at all.** On any non-`memory` store, `ravel-server` reads
`sys/qualification` at startup, in every mode, before any listener binds, and
refuses to start when the record is:

- **absent** -- the backend has never been qualified; run `ravel-cli store
  qualify`, then start the server; or
- **stale** -- recorded under a suite version below this binary's required
  floor; re-run `ravel-cli store qualify` with a current build, then restart.

The two conditions are reported as distinct, named errors. This is intentional,
not a bug to route around: unlike the tenancy marker (below) and the durable GC
config, an absent qualification record is **not** a fresh-bucket
bootstrap-and-continue case. There is no "assume qualified" path, because a
never-qualified backend has never been shown to honor the guarantees Ravel's
durability depends on. `--store memory` (the semantics oracle used in
development and tests) is exempt and never needs qualification.

## Bucket protection contract (ADR-0072 decision 3)

`docs/object-store-contract.md`'s "Required bucket configuration" section is
the normative version of this; this is the operational summary. Object
Lock (compliance mode) on `sys/*`, `t/*/*/prov`, commit records `t/*/*/c/*`,
and `t/*/catalog/*/*` HEAD history, plus the ADR-0064 section 7 versioning
and lifecycle-rule requirements, is enforced at the bucket/IAM layer
(ADR-0042 decision 3) -- nothing in `ravel-server` configures or verifies it
in-process, because `object_store` 0.14 exposes no such API.

`--require-bucket-protection` turns the existing, previously
informational-only conformance probes into a startup gate so a deployment
cannot go into production silently unprotected:

- **Disabled**, or a versioning-without-expiration alarm, refuses to start.
- **Unknown** -- what every backend reachable only through the
  `ObjectStoreBackend` contract reports today, since no adapter can answer
  this query -- logs one warning and sets `ravel_bucket_protection_unknown`
  to `1` at `GET /metrics`, rather than blocking startup.
- **Enabled** with no alarms starts clean, gauge at `0`.

Off by default, so a dev/test process (and any other direct invocation that
does not pass the flag) starts exactly as it did before this gate existed.
`ravel-operator` sets `--require-bucket-protection` unconditionally for
every `RavelCluster` it reconciles: the CRD carries no dev/staging profile
field to gate on, so every cluster the operator manages gets the flag.

| Condition | Query | Why |
|---|---|---|
| Bucket protection unknown | `ravel_bucket_protection_unknown == 1` | `--require-bucket-protection` is on and the backend cannot confirm Object Lock/versioning is configured. Not necessarily misconfigured -- most backends have no query for this today -- but it means the platform cannot see the protection it depends on, and an operator should confirm it out of band. |

## Store reachability probe and `/readyz` (ADR-0050 section 7)

`/readyz` (readiness) now reflects store reachability, not just startup
completion. Each process runs one background probe that GETs the fixed
`sys/tenancy` object every `--store-probe-interval` (default `30s`, jittered so
replicas do not probe in lockstep). Readiness is the AND of the startup latch
and this probe's health:

- After **4 consecutive** failed probes, readiness flips and `/readyz` (and its
  Prometheus spelling `/-/ready`) returns 503.
- The **first successful** probe flips it back to 200 immediately (asymmetric:
  four failures down, one success up).

At the default interval this is roughly two minutes of hysteresis before a
fleet is marked unready -- a store outage that long means every data path is
failing, and marking the fleet unready is the truthful signal (traffic fails
fast at the load balancer instead of timing out per request). The threshold is
a fixed constant, not a flag, so it cannot be lowered to 1 and reintroduce the
single-blip mass-ejection failure mode this design exists to prevent.

`/readyz` still does **no** object-store call on the probe path itself: the
kubelet reads only an in-memory atomic the background probe maintains. And
`/healthz` (liveness) is deliberately unaffected by the probe -- it still means
only "the process is alive." A store outage must never make liveness fail and
get healthy processes killed and restarted; that is exactly the failure mode
the two objections documented in `health.rs` are about.

`/readyz` flipping on store outages changes rollout semantics: a deployment
gated on readiness will (correctly) halt while the store is unreachable.

The probe exports two samples at `GET /metrics`, so an operator sees the outage
even on a metrics-only monitoring setup where nothing consumes `/readyz`:

- `ravel_store_reachable` (gauge, labeled by `mode`): 1 = healthy, 0 = unhealthy.
- `ravel_store_probe_failures_total` (counter, labeled by `mode`): every failed
  probe cycle, monotonic, incremented even below the readiness threshold.

Default alert rule:

| Condition | Query | Why |
|---|---|---|
| Store unreachable | `ravel_store_reachable == 0` | The background probe has failed four consecutive GETs of `sys/tenancy`; every data path through this process is almost certainly failing and it has already stopped advertising readiness. Alert on the sustained gauge state, not an `increase()`: this is a live "store is down right now" condition, and it clears itself the moment a single probe succeeds. |

## Durable auth refresh (ADR-0066 decision 6)

When `--deployment-key` is set in a request-serving mode (`all`, `gateway`,
`query`), the process resolves bearer tokens against a cached copy of the
durable `sys/auth` map and keeps that copy current with a background refresh
loop. The loop re-reads `sys/auth` from object storage; on success it advances
the staleness gate, on any read or decode failure it keeps the last-known map
and leaves the gate un-advanced. If it cannot refresh for a hard multiple of
the refresh horizon, the cached map is treated as untrustworthy and token
resolution fails closed. Three counters, all labeled by `mode`, surface the
loop's health:

- `ravel_durable_auth_refresh_failures_total`: background refreshes that could
  not read or decode `sys/auth`.
- `ravel_durable_auth_on_miss_rereads_total`: off-horizon on-miss re-reads begun
  after the rate limiter, when the request path saw an unknown token.
- `ravel_durable_auth_stale_fail_closed_total`: token resolutions refused
  because the cached map was hard-stale.

The point of `ravel_durable_auth_refresh_failures_total` is to page on a broken
storage credential (or a corrupt/wrong-key `sys/auth` object) **before** the
hard-stale horizon starts refusing every durable token. It begins incrementing
the moment refresh fails, one refresh interval apart, while the last-known map
still serves; `ravel_durable_auth_stale_fail_closed_total` only starts once the
horizon has already been crossed and auth is failing closed. Alert on the first,
so the fix lands inside the grace window rather than after the cliff:

| Condition | Query | Why |
|---|---|---|
| Durable auth refresh failing | `increase(ravel_durable_auth_refresh_failures_total[15m]) > 0` | The refresh loop cannot read or decode `sys/auth`: most often the storage credential broke or lost read on the key, or the object is corrupt or written under a different deployment key. The cached map still serves for now, but the staleness gate is not advancing, so this is the early warning that auth will fail closed at the hard-stale horizon if it is not fixed. A nonzero increase is a credential or object problem to reconcile now, not a rate to threshold. |
| Durable auth failing closed | `increase(ravel_durable_auth_stale_fail_closed_total[5m]) > 0` | The cached map is already past the hard-stale bound and durable tokens are being refused. This is the cliff the refresh-failure alert above exists to keep you off; if it fires, the refresh has been broken for a full hard-stale window and every `sys/auth` token is now rejected until a refresh succeeds. |

## Tenancy setup

Repeated `--tenant-token TOKEN=TENANT` flags on `ravel-server` configure
tenants entirely. There is no tenant database or admin API. To add, remove,
or rotate a tenant token, restart `ravel-server` with a different flag set.
This is safe: every process is stateless, so a restart with new tenant tokens
has no data migration to do. Tenant identity affects only key prefixing
(`t/<tenant_hash>/...`, where `tenant_hash` is a BLAKE3 hash of the tenant
name) and query and ingest authorization. It carries no other per-tenant
configuration today (no per-tenant quotas, no per-tenant storage backend).

### Real authn: OIDC and mTLS (ADR-0042 decision 6)

The static `--tenant-token` bearer resolver stays the local/dev path and is
unchanged. For production, two additive resolvers join the same first-success
chain; enabling them does not disable the bearer resolver.

- **OIDC (JWT).** Set `--oidc-issuer` and `--oidc-jwks-url` together. Every
  request's `Authorization: Bearer <jwt>` is verified against the issuer's
  JWKS: signature, `iss`, and `exp` (and `aud` if any `--oidc-audience` is
  set). The signature algorithm is pinned from the JWKS key that verifies the
  token, never from the token's own `alg` header, so `alg: none` and
  algorithm-confusion tokens are rejected; a symmetric (HMAC) key in the JWKS
  is rejected outright, since a JWKS is a public document and a symmetric key
  inside one is a published verification secret, never a usable one. The
  tenant is read from `--oidc-tenant-claim` (default `tenant`) as a string,
  with no fallback to any other claim. The JWKS is cached in memory and
  refreshed on `--oidc-jwks-refresh-interval-secs`; the request path never
  makes a network call, and the fetch is bounded by a timeout so a stalled
  JWKS host cannot wedge the refresh loop or the readiness gate. The first
  fetch must succeed before the server reports ready. `--oidc-jwks-url`
  refuses a plaintext `http://` URL to a non-loopback host at startup: the
  JWKS response is the entire trust root for JWT verification, and fetching
  it in plaintext lets an on-path attacker substitute their own keys.

- **mTLS (proxy-forwarded).** Ravel does **not** terminate TLS or verify
  client certificates itself. `--mtls-enabled` reads a header (default
  `x-ravel-client-cert-cn`, override with `--mtls-header`) that a
  TLS-terminating reverse proxy is expected to set to the already-verified
  certificate CN or SAN. This is an `X-Forwarded-For`-class trust boundary: it
  is authoritative only because a trusted hop set it, and forgeable by anyone
  if that hop is absent. Enable it **only** behind a proxy that (a) actually
  performs mTLS client-certificate verification and (b) strips or overwrites
  any client-supplied value of the header before forwarding **on every
  ingress this process exposes** - the HTTP listener, and the gRPC listener
  (Flight SQL and OTLP gRPC ingest read the same header, since gRPC metadata
  is copied into the same header map). Sanitizing only the HTTP vhost and
  forgetting the gRPC one leaves a live bypass. It is off by default and
  opt-in for exactly this reason, and enabling it logs a startup warning
  naming the trusted header.

Dependent flags fail fast at startup: OIDC needs both its issuer and JWKS URL;
`--oidc-tenant-claim`/`--oidc-audience` without OIDC, or `--mtls-header`
without `--mtls-enabled`, refuse to start rather than silently do nothing.

## Legal hold

`ravel-cli hold set --tenant <id> --scope <prefix> [--reason <text>]`,
`ravel-cli hold clear --tenant <id> --scope <prefix>`, and `ravel-cli hold
list --tenant <id>` write and read the ADR-0040 audit records that both
maintenance drivers check before any destructive pass (ADR-0048 decisions
1-2). A `--signal`/`--shard` form writes all the prefixes one shard needs
in a single command, so the documented L0-only-hold mistake isn't possible
from the CLI.

**The hold is not effective the instant the command returns.** Each
maintenance tick refreshes its hold snapshot once, before its destructive
pass; a hold set after that tick's refresh is not honored until the next
one (ADR-0048 decision 1). The exposure window is one
`--maintain-interval-secs` interval, 5 minutes by default. After placing an
urgent hold, run `ravel-cli hold list --tenant <id>` and confirm the scope
is present before assuming the data is protected; the `hold set` command
returning success only means the record was written, not that a
maintenance pass has picked it up.

## Disposability

You can kill every Ravel process (any `--mode`) at any time. Correctness needs
no special shutdown sequence:

- **Ingest shard actors** hold buffered-but-not-yet-flushed points only in
  memory. If you kill the process, you lose that buffer. In strict mode,
  nothing in that buffer was ever acknowledged, so no acknowledged write is
  lost. In buffered mode, the acknowledged-but-unflushed window (bounded by
  `max_flush_delay`, 2s default, ADR-0076 decision 4) is lost, by design.
  ([docs/consistency-model.md](../consistency-model.md))
- **Gateway and query processes** hold no durable state at all. They read
  and write the object store, and otherwise hold only in-flight request
  state.
- **Recovery** is just to start a new process against the same object
  store and bucket. There is no replication to catch up, no leader
  election, no consensus round. Any process can serve any request for any
  tenant, as long as it has the right `--tenant-token`/S3 credentials.
- **Nothing to back up** besides the object store bucket itself: no local
  volumes, no WAL, no on-disk state directory. The bucket, however, is a
  single point of loss. Ravel builds no in-product backup, export, or failover
  mechanism; disaster recovery is operator-owned bucket-level controls
  (versioning, noncurrent-version expiration, cross-region cross-account
  replication) proven by a rehearsed restore. The normative runbook —
  the three configuration tiers and their erasure-bound trade-offs, the
  mandatory `DeleteMarkerReplication`, the deliberate verified restore
  procedure (replica as restore source, never a live failover target), and
  the rehearsal record from which the only published RPO/RTO numbers come — is
  [disaster-recovery.md](disaster-recovery.md) (ADR-0077). Read it before
  relying on "just back up the bucket."

## Garbage collection and retention

Ravel deletes data through two independent triggers. The background
maintenance loop (`ravel-server --mode maintain`) drives both, or one-shot
from `ravel-cli maintain`. Objects are immutable throughout. Deletion
removes whole objects; nothing is ever modified in place
([docs/consistency-model.md](../consistency-model.md#deletion-and-gc),
ADR-0018/ADR-0019). All of it is
signal-generic: metrics (RSEG) and logs (RLOG) go through the same code.

### What runs

- **Compaction (L0 → L1)**: after an ingest-hour bucket is sealed (its end
  plus `max_flush_lifetime` + `clock_skew_allowance`, so no further commit
  can appear), the compactor rewrites its many small L0 segments into a
  handful of large L1 parts. It publishes one `CompactionRecord` that names
  the L0 inputs it superseded. It copies pages verbatim and never decodes a
  sample, so a query over the L1 output is bit-identical to a query over the
  L0 inputs. This is the primary win: object count per hour drops from
  thousands to a handful.
- **Age-based retention** (ADR-0019): if a sealed bucket's newest event is
  older than the tenant's retention window `R`, Ravel *tombstones* it with a
  durable `RetentionTombstone`. This immediately excludes the whole bucket
  from new query snapshots. Retention is off by default; configure it with
  `--retention-default` / `--retention-tenant`. `R` is validated at startup
  against a floor (`max_ingest_lag + max_flush_lifetime +
  clock_skew_allowance` + one bucket span), so a bucket can never be
  tombstoned before it is sealed. A window below the floor fails startup.
  Retention runs before compaction, so an expired bucket is tombstoned,
  never compacted first.

### The three sweep rules (physical deletion)

The sweeper is the only component that issues `delete`. All three rules
re-verify their precondition against a fresh strongly consistent listing
immediately before each delete, and every delete is idempotent:

1. **Orphan GC**: an `l0/` data object with no commit record, older than
   `grace + max_flush_lifetime`. The writer interlock guarantees that such an
   object can never gain a commit record later, so deleting it cannot
   orphan a future reader.
2. **Superseded-input sweep**: the L0 commit records and data objects that a
   `CompactionRecord` names, after `now ≥ record.created_unix_ns +
   protection_horizon`. Records are deleted before data objects, so a
   crash mid-sweep never leaves a commit record that points at a deleted
   object.
3. **Unreferenced-part cleanup**: an `l1/` object that no compaction record
   in its bucket references, after a compaction record exists for that bucket
   and the object is older than `grace + max_compaction_lifetime`.

Retention's own physical sweep deletes everything in a tombstoned bucket
(L0 records, compaction records, L0 data, L1 parts, then the tombstone
last) after `now ≥ retired_at_ns + protection_horizon`, and only after a
verifying listing shows the bucket empty but for its tombstone.

### Timing

- `grace` (default 24h): floor for the orphan and unreferenced-part age
  gates.
- `protection_horizon` (default `max_query_duration + grace`, 25h): the
  gap between a deletion anchor (a compaction record's `created_unix_ns`,
  a tombstone's `retired_at_ns`) and physical deletion. A query resolved
  just before the anchor then still has time to read the inputs it pinned.

These are compaction-config defaults, not yet exposed as CLI flags. The
maintenance loop uses the defaults.

### Running it

- **Continuously**: `ravel-server --mode maintain` runs the loop per
  tenant over all three signals and every shard on
  `--maintain-interval-secs`. It needs a `multipart`-capable backend and
  serves no ingest or query routes.
- **One-shot / inspection**: `ravel-cli maintain compact-bucket`,
  `maintain sweep`, `maintain status`, `maintain audit-versions`,
  `maintain migrate` (below), and `maintain verify-custody` (see the CLI
  table above). `compact-bucket` and `sweep` take `--dry-run` to report
  exactly what a real run would write or delete, without mutating
  anything; `verify-custody` is read-only and has no `--dry-run` since
  there is nothing to simulate.

### Format migration (`maintain migrate`)

`ravel-cli maintain migrate --tenant T --signal metrics` raises a `(tenant,
signal, format family)`'s recorded format floor to a target on-object format
version. One invocation:

1. walks buckets in `(shard, ingest_hour)` order from a durable cursor,
   rewriting every sealed, un-tombstoned, not-yet-compacted bucket that still
   has an L0 commit record below the target format version (this reuses the
   compaction rewrite primitive, so the rewrite is bucket-atomic and produces
   a compaction record exactly as compaction does);
2. stops early and persists the cursor once `--budget-records` is spent
   (0 = unlimited; re-run to resume), or, once the walk drains, re-audits
   fresh and raises the floor only if that re-audit finds zero records below
   the target.

A refused raise ("FOUND STRAGGLERS") means the fresh re-audit found genuine
live data still below the target -- e.g. a bucket too recently landed to be
sealed and migrated yet, or data that arrived after the walk passed. Re-run
`migrate`; the target data will migrate once it is sealed.

The re-audit's liveness definition already excludes a bucket's pre-rewrite L0
commit records once that bucket carries a compaction or rewrite record: those
records are dead, sweepable leftovers of a rewrite this same invocation may
have just performed, not stragglers. Because of this, a clean
migration converges and raises the floor in one invocation; running `sweep`
in between is never required for `migrate` to converge. `sweep`'s
superseded-input rule (above) still physically deletes those pre-rewrite
records on its own schedule, independent of `migrate` -- that is a storage-
reclamation concern, not a correctness precondition for the floor raise.

**Rollout order across a format bump: readers before writers (ADR-0066
decision 1).** When a release bumps a bulk data-object format (RSEG, RLOG, or
RSPAN) from version N to N+1, roll the fleet in this order, never the reverse:

1. Deploy the release that *reads* N+1 (the N/N-1 window with N+1 as the new
   newest) to every process that opens objects -- query, maintenance, and the
   catalog fold -- and confirm it is live fleet-wide. A process that writes N+1
   before its peers can read N+1 produces objects the rest of the fleet
   fail-closes on (typed `UnsupportedVersion`, never a silent misread), so
   writers must never lead.
2. Only then enable writing N+1 (compaction and flush emit the new version).
   From this point new and rewritten objects are N+1; existing N objects stay
   readable through the N-1 half of the window.
3. Converge the existing N objects toward N+1: retention ages them out for
   free, and `ravel-cli maintain migrate --tenant T --signal S` rewrites the
   rest and raises each `(tenant, signal, family)` format floor once a fresh
   re-audit confirms nothing below N+1 survives. Watch `audit-versions` for the
   remaining below-target population.
4. Delete the reader for the now-retired version N only after every bucket's
   recorded floor is >= N+1 -- a checkable fact from the floors `migrate`
   raised, in its own later reviewed change. Deleting an old reader on any
   other basis is the wipe-and-hope ADR-0066 replaced.

### Maintenance safety metrics and alerts

`--mode maintain` renders five additional samples on the existing `GET
/metrics` endpoint (ADR-0044 section 4), alongside the tenant-discovery
gauges: `ravel_maintain_legal_hold_refresh_failures_total`
(counter), `ravel_maintain_conservation_aborts_total` (counter, labeled
by `signal`), `ravel_maintain_orphan_breaker_tripped_total` (counter,
labeled by `signal`), `ravel_maintain_orphans_withheld` (gauge,
labeled by `signal`), and `ravel_maintain_orphans_present` (gauge,
labeled by `signal`). No new label is added to the renderer's
compile-time-closed allowlist; these reuse the existing `mode` and
`signal` labels only. ADR-0048 names `tenant_hash` as a label on the
orphan-breaker-trip counter, but ADR-0044 blocks any
`tenant_hash`-labeled sample on the unauthenticated `/metrics` route
unless the opt-in `--metrics-tenant-labels` flag is set (see below); by
default all five samples stay process-wide totals, not broken out per
tenant.

### Maintenance ownership, concurrency, and merge-memory metrics and alerts (ADR-0065)

`--mode maintain` also renders the ADR-0065 leased-distributed-maintenance
family, one process-wide series per name (`mode` label only, plus `kind` on
the merge-memory gauge -- no `tenant_hash`, per ADR-0044 section 4):

| Metric | Kind | What |
|---|---|---|
| `ravel_maintain_workers_live` | gauge | In-process maintenance workers this supervisor currently sees as live under its own heartbeat/liveness protocol (ADR-0065 decision 1). `1` in a single-replica deployment; a healthy multi-replica deployment holds at the replica count. |
| `ravel_maintain_units_owned` | gauge | Owned `(tenant, signal, shard)` units this process is currently maintaining, recomputed from scratch every discovery cycle (ADR-0065 decision 2). Summed across every live worker this equals the total unit count, with no unit double-owned or unowned. |
| `ravel_maintain_units_stalled` | gauge | Owned units whose consecutive failing ticks have crossed `--maintain-stalled-after-intervals` (default `3`), with no intervening success (ADR-0065 decision 2's stuck-owner mitigation). |
| `ravel_maintain_memo_warm_start_units_total` | counter | Units seeded from a durable memo snapshot on handoff or startup, instead of rescanning cold (ADR-0065 decision 3). Only increments when a membership change hands this process a unit another worker's snapshot already covers; a stable single-replica deployment sees this stay at its startup value. |
| `ravel_maintain_full_sweep_passes_total` | counter | Full (unscoped) sweep passes run, as opposed to a zone-scoped sweep (ADR-0065 decision 3). A cold-started memo runs one of these per owned unit on the first tick, then only on the `--maintain-interior-reverify` cadence afterward. |
| `ravel_maintain_rlog_merge_peak_bytes{kind="transient"\|"total"}` | gauge | High-water mark of RLOG k-way merge memory (ADR-0065 decision 4): `transient` is in-flight fetched-minus-released block bytes, `total` adds the buffered writer output. One process-wide tracker shared across every tenant's merges, so this is not a per-merge or per-tenant number. |

Default alert rules:

| Condition | Query | Why |
|---|---|---|
| No live maintenance workers | `ravel_maintain_workers_live == 0` while `--mode maintain` is running | A process that cannot see itself as live (a heartbeat write persistently failing, or a liveness-window read persistently erroring, ADR-0065 decision 1's fail-open path only masks a *transient* fault) owns nothing under the rendezvous hash. Every unit it used to own either sits unmaintained or, in a multi-replica deployment, is now double-covered by a sibling racing to pick it up. Fire on the level, not on an `increase()`: there is no counter here to accrue against, only a gauge that should never read zero while this role is up. |
| Units stalled, sustained | `ravel_maintain_units_stalled > 0` for `30m` | A unit crossing the stall threshold means its last `--maintain-stalled-after-intervals` ticks all failed with no intervening success -- the same unit, not a rotating cast of transient faults (`observe_unit_tick` resets the streak on any single success). A momentary blip during a store hiccup can cross the threshold and clear itself within a cycle or two; a stall that survives multiple maintenance intervals (30 minutes covers several cycles at the default 300s `--maintain-interval-secs`) means that unit's retention, compaction, or sweep pass is genuinely stuck and needs an operator, not just its next scheduled retry. |

`ravel_maintain_memo_warm_start_units_total` and
`ravel_maintain_full_sweep_passes_total` are not alert targets: both are
expected-activity counters (a warm start is the *intended* outcome of a
membership change, and a full sweep is the *intended* outcome of a cold
start or the interior-reverify cadence), not failure signals. Graph them to
confirm the warm-start path is exercised during a rolling restart, or to
see the interior-reverify cadence's periodic full-sweep cost, rather than
alerting on either.

`ravel_maintain_rlog_merge_peak_bytes` is an inspection gauge for sizing a
process's memory headroom against real observed merge peaks (ADR-0065
decision 4), not an alert target on its own: pair it with the process's
actual memory limit and alert on that ratio if this deployment enforces
one, rather than on an absolute byte threshold this doc would have to pick
for every possible workload.

### Admission usage (ADR-0051 section 6)

`ravel_admission_admitted_total`, `ravel_admission_rejected_total`
(labeled by `reason`: `byte_rate`, `series_rate`, or `series_cap`),
`ravel_admission_active_series`, and `ravel_admission_active_streams`
(all labeled by `signal`) export the admission controller's
per-(tenant, signal) usage counters. By default every tenant folds
into `tenant_hash="other"` and the families sum across tenants, so
cardinality stays bounded regardless of tenant count. Pass
`--metrics-tenant-labels` to emit real per-tenant `tenant_hash` values
instead -- one series per (tenant, signal, reason) -- which is a
cardinality trade an operator opts into deliberately, not a default.

### Per-query cost accounting (ADR-0044)

A query reports what it spent on object storage to the client that ran it.
An operator can then see cost per tenant and per workload, and never reads
a query's text to do it.

**Coverage is complete for read queries.** Every read surface folds its
cost into `GET /metrics`. This covers `POST /api/v1/sql` and
`POST /api/v1/analytics`. This covers the Prometheus-shaped
`GET /api/v1/query`, `GET /api/v1/query_range`, `GET /api/v1/labels`, and
`GET /api/v1/series`. This covers every Flight SQL request. Read each
`ravel_query_*` number below as all read traffic. A Flight SQL statement
records two folds. The plan request records the first fold. The fetch
request records the second fold. The two folds sum to one whole-query
estimate beside the summed whole-query actual.

A Flight fetch records when its result stream ends. A client that
disconnects after the first batch still ends the stream, so its partial
cost is recorded and counts as one query. This is deliberate: the bytes
were spent, and the alternative is to lose them. An unusually low
cost-per-query ratio on the Flight path can therefore mean early client
disconnects rather than cheap queries.

**In the response.** `POST /api/v1/sql` and `POST /api/v1/analytics` add a
`stats` object beside `data`, carrying this query's `accounting` (the
actual counters: object-store requests and bytes split by `get`/`list`/`head`,
cache hits and misses, decompressed bytes, segments opened, series matched,
bytes reused, and the peak intermediate footprint) and its `estimate` (the
pre-execution upper-envelope of requests, store bytes, and decompressed
bytes). The Prometheus-shaped `GET /api/v1/query` and `/api/v1/query_range`
already carry the same `stats.accounting`/`stats.estimate` under their `data`
object. An Arrow IPC (`Accept: application/vnd.apache.arrow.stream`) SQL
response is a bare columnar payload with no envelope for a JSON object, so it
reports no in-body stats; the `/metrics` aggregation below still captures the
query regardless of its encoding.

**At `/metrics`.** The `ravel_query_*` family aggregates each accounted
query. Its labels are `mode`, `tenant_hash`, and `workload_class`. Only
`workload_class="interactive"` occurs in this release. No production caller
runs a query as `background` yet. The actual and the estimate render as
separate series with different names. An operator can then measure their
divergence directly in PromQL:

| Metric | What |
|---|---|
| `ravel_query_queries_total` | Accounted queries. This is the denominator for per-query averages. |
| `ravel_query_s3_requests_total` / `ravel_query_s3_bytes_total` | Actual object-store requests and bytes. |
| `ravel_query_cache_hits_total` / `ravel_query_cache_misses_total` | In-process read-cache outcomes attributed to queries. |
| `ravel_query_decompressed_bytes_total` | Actual decompressed sample bytes decoded. |
| `ravel_query_estimated_requests_total` | Pre-execution estimate of object-store requests. |
| `ravel_query_estimated_store_bytes_total` | Pre-execution estimate of object-store bytes. |
| `ravel_query_estimated_decompressed_bytes_total` | Pre-execution estimate of decompressed bytes. |

The estimate is an upper envelope, never a prediction (ADR-0044 section 3):
the ratio `ravel_query_s3_requests_total / ravel_query_estimated_requests_total`
staying at or below 1 is the health signal that a later admission ADR could
enforce on. Nothing in this release rejects a query on it; this is
measurement only.

Like the admission family, per-tenant `tenant_hash` values render only under
`--metrics-tenant-labels`, and only for tenants that have explicit admission
limits configured. Every other tenant folds into `tenant_hash="other"` at
record time, so `/metrics` cardinality is bounded by the configured tenant
count regardless of how many distinct tenants query -- the same
disclosure-and-cardinality trade the admission family makes on this
unauthenticated route. Off (the default), every tenant folds into `other`.

A query that fails records nothing. A deadline breach, an admission
rejection, and an execution error all return before the fold, and the error
type carries no accounting snapshot. The runaway query that the ratio below
exists to show is therefore the one query the ratio can miss. Read a sudden
drop in `ravel_query_queries_total`, against steady request logs, as
failures rather than as idle capacity.

Suggested operator uses: alert on
`increase(ravel_query_s3_requests_total[5m]) / increase(ravel_query_estimated_requests_total[5m]) > 1`
for a sustained window (an actual exceeding its own upper-envelope estimate is
either a cost-model gap or a runaway to investigate); rank tenants by
`sum by (tenant_hash) (rate(ravel_query_s3_bytes_total[1h]))` to find the
tenant whose queries cost the most object-store traffic.

Default alert rules:

| Condition | Query | Why |
|---|---|---|
| Legal hold refresh failing | `increase(ravel_maintain_legal_hold_refresh_failures_total[15m]) > 0` | Every failure already skips that tenant's tick entirely (fail-closed, ADR-0048 decision 1); a sustained failure means a tenant is silently receiving no maintenance at all. |
| Compaction conservation gate aborting | `increase(ravel_maintain_conservation_aborts_total[15m]) > 0` | Each abort means a compaction publish was refused because input and output record counts disagreed (ADR-0048 decision 6); nothing was written, but a bucket stuck retrying every tick without ever compacting needs an operator, not just a retry. |
| Mass-orphan circuit breaker trip | `increase(ravel_maintain_orphan_breaker_tripped_total[5m]) > 0` | Fire on the **first trip**, not on a sustained condition. The trip condition can clear itself (dilution or partial restoration, see below) while the underlying record loss and the pass's withheld deletions persist; a sustained-state alert (`orphan_breaker_tripped_total` treated as a level) can clear before anyone looks. The counter only increments, so any `increase() > 0` is a real trip that happened, whether or not the shard is still tripping now. |
| Orphans present (small-scale loss) | `ravel_maintain_orphans_present > 0` for `12h` | Catches the breaker's blind spot (ADR-0058 decision 1): delete a handful of commit records for one shard and the candidate count never reaches `orphan_breaker_min_count` or `orphan_breaker_max_ratio`, so the breaker never trips and `orphans_withheld` stays `0`, yet the orphaned data objects are deleted at the grace horizon like ordinary abandoned flushes. A sustained nonzero here is either that loss or a genuinely stuck abandoned flush; both warrant a look before the grace window (default 25h) elapses. Twelve hours is roughly half the grace window, long enough that a single normal abandoned-flush cleanup between passes doesn't page, short enough that real loss alarms with hours to spare. |
| Discovered tenants not maintained | `ravel_maintain_tenants_maintained < ravel_maintain_tenants_discovered` for `10m` | A prefix under `t/` holds data with no maintaining owner, the exact `maintained < discovered` condition ADR-0048 decision 3 names, and the same class of gap recurring for a different reason (ADR-0048 Context). Ten minutes is two cycles at the default 300s `--maintain-interval-secs`, long enough that a single tick's transient gap (a restart, a tenant mid-onboarding) doesn't page, short enough that a real gap alarms within the hour. |
| Tenant discovery failing | `increase(ravel_maintain_tenant_discovery_failures_total[5m]) > 0` | A failed `LIST t/` skips the *entire* cycle, every tenant, not just one (ADR-0048 decision 3): the supervisor deliberately never treats a failed enumeration as "no tenants" so it can't be confused with healthy idleness, but that means a sustained failure is a fully silent maintenance outage. Alarm on the first occurrence rather than waiting for a sustained window, faster than the gauge condition above, because a skipped cycle is worse than one tenant falling behind: nothing is being maintained at all. |

`ravel_maintain_orphans_withheld` is a gauge, not an alert target: it
reflects only the most recent sweep pass and drops to zero on the very
next non-tripping pass, including one that stopped tripping only
because of dilution or partial restoration (see below). It is for
inspecting the size of the most recent withheld set once the trip
counter has already told you a trip happened, not for detecting the
trip itself.

`ravel_maintain_orphans_present` is the companion gauge that closes the
breaker's blind spot for small-scale loss (ADR-0058 decision 1). It
carries the most recent pass's total orphan-candidate count
(`orphans_deleted + orphans_withheld`, exactly one of which is nonzero),
whether or not the breaker tripped, so it is nonzero exactly in the case
`orphans_withheld` cannot report: a few commit records lost for one
shard, below the breaker's count and ratio thresholds, whose orphaned
data objects would otherwise be deleted at the grace horizon with no
operator-visible signal. Unlike the withheld gauge it *is* an alert
target (the `orphans present` rule above), because a sustained nonzero
is the only warning of that loss. It is still a gauge, not a counter: it
reflects only the latest pass and drops as candidates are deleted or
their records restored, so a drop is not "resolved," only this pass's
count. A genuinely abandoned flush also shows up here for a pass or two
before orphan GC clears it, which is why the alert waits for the
condition to sustain rather than firing on any single nonzero reading.

### Mass-orphan circuit breaker runbook

A trip means: the current sweep pass found at least
`orphan_breaker_min_count` (default 50) orphan-GC candidates, and they
were more than `orphan_breaker_max_ratio` (default 10%) of the shard's
listed L0 objects. Both conditions must hold. The pass deleted nothing
and halted; the other two sweep rules (superseded-input,
unreferenced-part) are unaffected and still ran, since they are
anchored on durable records, never on record absence.

**It is not self-clearing in the sense an operator expects.** The
predicate is recomputed from live counts on every pass, with no memory
of a prior trip. A shard can stop tripping while the missing commit
records are still missing, through either of two mechanisms
(docs/consistency-model.md "Deletion and GC"):

- **Dilution**: new well-recorded writes to the same shard lower the
  orphan ratio below `orphan_breaker_max_ratio` even though the orphan
  count itself hasn't changed (55 orphans among 500 objects trips at
  11%; 200 further writes with no data loss give 55/700 = 7.9%, which
  does not trip, and the 55 still-orphaned objects get deleted on the
  next pass).
- **Partial restoration**: an operator restores some but not all of the
  missing commit records, and the remaining candidate count crosses
  below `orphan_breaker_min_count` (55 orphans trips; restoring 6 leaves
  49 candidates, under the default floor of 50, so the very next pass
  stops tripping and deletes the other 49 before they were restored).

Relying on the breaker to hold a shard open until every missing record
is back is relying on a guarantee the code does not provide. The only
durable way to stop deletion is to restore the missing records before
the next pass runs, not to assume a trip persists or that a clear
`ravel_maintain_orphan_breaker_tripped_total` increase rate means the
loss was resolved.

**Inspecting what was withheld**: run `ravel-cli maintain sweep
--tenant <t> --signal <metrics|logs|spans> --shard <n> --dry-run`
(without `--override-orphan-breaker`) to recompute the same candidate
set and print the withheld count without deleting or clearing anything;
the `ravel_maintain_orphans_withheld` gauge on `/metrics` shows the
count from the most recent real pass. Neither one tells you why the
records are missing; that requires the operator's own investigation
into what deleted or corrupted them out of band.

**Forcing a pass through a trip**: `ravel-cli maintain sweep --tenant
<t> --signal <metrics|logs|spans> --shard <n>
--override-orphan-breaker` runs exactly one overridden pass, deleting
the withheld candidates despite the trip. This sets
`CompactorConfig::force_orphan_gc` for that single invocation only; the
server itself never sets it, and the breaker has no memory across
invocations, so an un-overridden pass afterward evaluates fresh. Use
this only after confirming (by restoring records, or by independently
verifying the candidates really are abandoned data) that deletion is
safe, since the same record-absence signal orphan GC re-verifies
against is exactly what out-of-band record loss forges.

**Known blind spots (open gaps, not fixed by this design)**:

- **No protection below the floor.** The breaker never trips below
  `orphan_breaker_min_count` (default 50) regardless of ratio, so total
  loss on a small shard is always deletable in one pass.
- **Up to the ratio ceiling is deletable per pass.** Because the breaker
  only trips once the candidate ratio exceeds `orphan_breaker_max_ratio`
  (default 10%), up to that fraction of a large shard's objects can be
  deleted in a single pass without ever tripping.
- **Silent un-trip via dilution or partial restoration.** See above:
  the predicate has no memory of a prior trip, so either mechanism can
  let a pass through the remaining loss without an operator's
  intervention.
- **No cross-shard or cross-tenant aggregation.** Each (tenant, signal,
  shard) is evaluated in isolation, so loss spread thin across many
  shards can stay under every single shard's threshold even though the
  total loss across the tenant or the deployment is large.

### Stop maintenance before restoring or reconstructing commit records

If commit records for a shard have been lost out of band (an accidental
delete, a bad S3 lifecycle rule, a fat-fingered prefix delete), the data
objects they named are invisible to readers and, once past the orphan grace
horizon, will be physically deleted by the sweeper's orphan-GC rule. The
recovery path is `ravel-cli commit reconstruct` (ADR-0058), which rebuilds
each record-less L0 data object's commit record from the object's own footer.
Before running it, stop maintenance for the affected tenant so the sweeper is
not racing your restore: a running `--mode maintain` loop keeps sweeping every
tick, and its orphan-GC rule deletes the very objects you are trying to
reattach.

1. **Stop maintenance for the tenant.** Stop the `--mode maintain` process
   entirely. This is the one method that reliably protects a tenant under
   repair regardless of its config-record status. `--maintain-tenant` only
   excludes tenants that do not yet carry a durable `t/<hash>/config` record;
   once a tenant carries one, no CLI flag can exclude it from maintenance, so
   restarting restricted to other tenants will not keep the sweeper off a
   config-recorded tenant (a durable per-tenant maintenance-exclusion
   mechanism is not yet built). Do not rely on the mass-orphan breaker to
   hold the shard open either: it is not self-clearing in the way an operator
   expects (see the runbook above), and small-scale record loss may never trip
   it at all.
2. **Reconstruct the missing records**, one shard at a time:

   ```sh
   ravel-cli commit reconstruct --tenant <name> --signal <metrics|logs> --shard <n>
   ```

   The command lists the shard's record-less L0 data objects, rebuilds a
   commit record for each from its footer, and writes it `CreateIfAbsent`
   (it never overwrites an existing record, and never deletes). It prints a
   per-object report (reconstructed / already-present-skipped / failed) and
   exits nonzero if any candidate failed. Repeat per shard for the affected
   range.
3. **Verify custody and catalog state** before resuming maintenance:

   ```sh
   ravel-cli maintain verify-custody --tenant <name>
   ravel-cli catalog verify --tenant <name>
   ```

   `verify-custody` re-hashes every live data object against its key and
   confirms every surviving record's data is present; `catalog verify`
   re-lists sealed records and diffs them against the snapshot. Both must be
   clean (exit zero) before you trust the repair.
4. **Resume maintenance.** Restart the `--mode maintain` process without the
   `--maintain-tenant` restriction (or restart the stopped one). The sweeper
   now sees the reconstructed records and treats their data objects as
   referenced, not as orphans.

Reconstruction rebuilds two fields as honest approximations rather than exact
copies of the original record: `created_unix_ns` (from the data object's own
`last_modified`, since it is in no footer) and, for logs, `ingest_hour_bucket`
(derived from the earliest observed sample, since RLOG footers do not carry
it). Both are argued safe for every downstream consumer in ADR-0058; the
rebuilt record is honestly a reconstruction, not a claim of byte-for-byte
provenance. Reconstruction also does not detect bit rot: it rebuilds a record
describing whatever bytes are currently stored (use `verify-custody` for the
content-hash check).

## Known limitations

As of the Phase 1 vertical slice:

- Catalog snapshot resolution removes
  most of the per-query listing cost for sealed history, but only where
  the background fold has run. Two cases still list commit records per
  (tenant, shard, hour) bucket on every query: the open window above the
  fold watermark (bounded by `max_ingest_lag`, default 2h), and any tenant
  with folding disabled or not yet caught up. That path does not scale past
  roughly 10,000 commits in one bucket.
- `promql-parser` (the upstream crate that Ravel's evaluator sits on) is not
  yet differentially validated against real Prometheus across a broad
  query corpus.
- RSEG encode throughput drops sharply at high series cardinality in one
  segment: about 14.7M samples/s at 100 series, down to about 235K/s at
  100,000 series, in the committed microbenchmarks.
- Parenthesized PromQL expressions (`(up)`) are rejected as unsupported.
  This is a known gap, not a silent reinterpretation.
- No exactly-once ingestion guarantee. Delivery is at-least-once. A
  client-side retry after a lost ack response re-ingests the same points
  as a duplicate (both copies are stored; a query takes the last value at
  a given timestamp). An idempotency-key window to collapse these is
  planned, not built.

### Tenant hash scheme

The object-key prefix for a tenant is a hash of the tenant id, pinned per
bucket at the bucket's birth by a `sys/tenancy` marker (ADR-0050 section 3).
Two schemes exist and one binary carries both, selected once at startup:

- v1-unkeyed: the original derivation. Every bucket created before ADR-0050
  is pinned to it permanently. Tenant names are not in keys, but anyone with
  list access can confirm a guessed tenant id offline.
- v2-keyed (the default for new buckets): the prefix is keyed by a 32-byte
  deployment key loaded from `--tenant-hash-key-file` (a file, so the secret
  never appears in a process listing). Without the key, prefixes reveal
  nothing about which tenants exist.

Startup pinning:

- A fresh bucket refuses to start with no key unless `--tenant-hash-unkeyed`
  is passed explicitly (keyed is the default; the choice is permanent).
- An existing keyed bucket refuses to start if the configured key's
  fingerprint disagrees with the marker: a wrong key is a failed deploy, not
  a silent parallel namespace. `ravel-cli tenancy show
  --tenant-hash-key-file <path>` verifies a key against a bucket offline.
- A pre-ADR bucket (data present, no marker) is adopted as v1-unkeyed once,
  logged and counted at `/metrics`
  (`ravel_tenancy_v1_unkeyed_adoptions_total`). Its existing prefixes are
  unchanged.

Key custody: for a keyed bucket the deployment key is tier-0 durable state
outside the object store. Losing it makes every `t/<hash>/` prefix
unattributable. Bucket-plus-key is always sufficient to recover the full
tenant-id-to-prefix mapping, via the per-tenant `sys/t/<tenant_hash>`
recovery manifests; the bucket alone reveals nothing.

There is no re-key migration between schemes. Moving a bucket between schemes
would relocate every object and is not built: a deployment that needs to
change schemes starts a new bucket and drains into it operationally.
