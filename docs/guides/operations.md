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
| `--s3-access-key <key>` | `RAVEL_S3_ACCESS_KEY` | none | Required when `--store s3`. |
| `--s3-secret-key <secret>` | `RAVEL_S3_SECRET_KEY` | none | Required when `--store s3`. |
| `--disable-fold` | | off | Disables the per-(tenant, signal) background catalog fold task (docs/metric-index-plan.md section 4). Folding only lowers query resolve cost; disabling it never changes query results. |
| `--fold-interval-secs <n>` | | `300` | How often each tenant's fold task wakes up to check for newly sealed hours. |
| `--maintain-interval-secs <n>` | | `300` | Used only in `--mode maintain`. How often each tenant's maintenance task wakes to run retention, compaction, and the sweeper over every shard of both signals. |
| `--retention-default <duration>` | | none | Used only in `--mode maintain`. The default age-based retention window applied to every tenant with no explicit override, as a humantime duration (`30d`, `720h`). Omitted means no default retention: nothing is age-deleted unless a per-tenant window is set. It is validated at startup against the ADR-0019 floor; a window below the floor fails startup rather than being clamped. |
| `--retention-tenant TENANT=DURATION` | | none, repeatable | Used only in `--mode maintain`. The per-tenant retention window; it overrides `--retention-default` for that tenant. Parsed with `humantime::parse_duration`. Same below-floor validation. |
| `--limits-file <path>` | | none (shipped defaults) | TOML admission-limits file (ADR-0051 section 3): `[defaults]` plus per-tenant `[tenants.<id>]` overrides. Parsed and validated at startup; an unparseable file, an unknown key, or a nonsensical limit (zero, or a burst set with no rate to pair it with) fails startup rather than falling back to defaults. See "Admission limits file" below. |
| `--cache-max-bytes <n>` | | `268435456` (256 MiB) | Maximum resident bytes for the ADR-0046 read cache's RAM tier. Read once at startup; there is no live resize. Ignored when `--disable-cache` is set. See [guides/caching.md](caching.md). |
| `--cache-dir <path>` | | none | Directory for the read cache's local-disk tier. Not wired to anything yet: the query fetchers only accept a RAM cache. Setting this flag fails startup rather than silently running with no disk tier. See [guides/caching.md](caching.md#known-gaps). |
| `--disable-cache` | | off | Disables the ADR-0046 read cache entirely. Query behavior becomes byte-for-byte identical to a build with no read cache wiring at all. |
| `--metrics-tenant-labels` | | off | Emits real per-tenant `tenant_hash` labels on the `ravel_admission_*` family at `/metrics` (ADR-0051 section 6) instead of folding every tenant into `tenant_hash="other"`. A deliberate cardinality trade; off by default so `/metrics` cardinality never scales with tenant count unless an operator opts in. See "Admission usage" above. |
| `--store-probe-interval <duration>` | | `30s` | How often the background store-reachability probe GETs `sys/tenancy` (ADR-0050 section 7), as a humantime duration, jittered. After four consecutive failures `/readyz` returns 503; one success recovers it. See "Store reachability probe and `/readyz`" below. |

`--store s3` without `--s3-bucket`/`--s3-access-key`/`--s3-secret-key` (through
flag or env) fails at startup with an explicit error that names the missing
one. It does not start in a broken state.

Note: [BENCHMARKS.md](../../BENCHMARKS.md) documents the S3 env vars as
`RAVEL_S3_ACCESS_KEY_ID` and `RAVEL_S3_SECRET_ACCESS_KEY`. The real flags
are `RAVEL_S3_ACCESS_KEY` and `RAVEL_S3_SECRET_KEY`, above; use those.
`allow_http` and `force_path_style` are not configurable at all. The code
derives `allow_http` from whether `--s3-endpoint` is set, and it always passes
`force_path_style: true`.

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
tracked entry. Issue #491 measured the actual `HashSet` entry cost (hashbrown
slot overhead, power-of-two table sizing at 7/8 load, allocator headroom) at
35-56 bytes, a 2-4x underestimate. `AdmissionController` tracks each of
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
| `ravel-cli maintain audit-versions --tenant <n> [--shards <n>]` | `--shards` default `4` | Audits live on-object format versions across all three signals. It flags any RSEG object at a version other than the one supported version (ADR-0027), reports the RLOG population by trailer version (1 vs 2, ADR-0032), and the RSPAN population by trailer version. Exits nonzero on any anomaly. |
| `ravel-cli maintain verify-custody --tenant <n> [--shards <n>]` | `--shards` default `4` | Read-only, no `--dry-run` (nothing is written or deleted). Re-verifies the content-addressed chain at rest: every live data object's key-embedded `hash16` against its actual content hash, and every surviving compaction record's referenced inputs (a mismatch is an anomaly; an input the sweeper already legitimately reclaimed past its protection horizon is reported separately, not as an anomaly). Exits nonzero on any anomaly. |
| `ravel-cli catalog list --tenant <name> [--hours <n>] [--shards <n>]` | `--hours` default `1`, `--shards` default `4` | Lists commit records that the catalog resolves for that tenant over the last `hours` hours. `--shards` must match the shard count the data was written with. |
| `ravel-cli catalog fold --tenant <name> [--shards <n>]` | `--shards` default `4` | One-shot catalog fold: seals every eligible hour into a new snapshot part and CAS-advances HEAD. Prints the fold report (watermark before/after, buckets folded, entry count, request counts). This is the same operation that the background fold task runs on a timer. |
| `ravel-cli catalog inspect --tenant <name>` | | Decodes and prints HEAD and every referenced snapshot part: watermark, part keys, hashes, entry counts. It reports rather than errors when no HEAD exists yet. |
| `ravel-cli catalog verify --tenant <name>` | | Re-lists every sealed commit record and diffs it against the current snapshot. Prints counts of entries missing from or mismatched against the snapshot; exits nonzero on any divergence. It reports rather than errors when no HEAD exists yet. |
| `ravel-cli provision adopt --tenant <name> --shards <n> [--signal <metrics\|logs\|spans>]` | | Writes the durable `shard_count` provisioning record for a tenant with pre-ADR data, ahead of any server touching it (ADR-0050 section 5). Runs the same adoption path the server runs: writes the record only when every observed shard index is below `--shards`, and refuses (writing nothing, exiting nonzero) when a higher index proves `--shards` would hide data. Prints one line per signal. A signal with no data and no record is left untouched (its record is written on first ingest). |

`segment inspect` and `commit decode` accept a local file path or an
object-store key. A path that exists on disk is read directly; otherwise it is
fetched from the configured store.

## Catalog fold and verify

The catalog fold (docs/metric-index-plan.md, ADR-0020) is a query-cost
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
in docs/metric-index-plan.md 5.3 that needs an operator repair rather than
resolves itself:

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
hard-fails its query without incrementing this counter (issue #529), and a
foreign postings object that fails its part-binding check first degrades
silently before the tenant_hash comparison ever runs (issue #528). A silent
gap for snapshot parts also exists (issue #527): a part's own `tenant_hash`
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
  resolved value -- and must be kept `<=` the durable `max_query_duration`;
  lower it if a `gc-config set` tightened `max_query_duration` below the
  current deadline.
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
  `max_flush_delay`, 500ms default) is lost, by design.
  ([docs/consistency-model.md](../consistency-model.md))
- **Gateway and query processes** hold no durable state at all. They read
  and write the object store, and otherwise hold only in-flight request
  state.
- **Recovery** is just to start a new process against the same object
  store and bucket. There is no replication to catch up, no leader
  election, no consensus round. Any process can serve any request for any
  tenant, as long as it has the right `--tenant-token`/S3 credentials.
- **Nothing to back up** besides the object store bucket itself: no local
  volumes, no WAL, no on-disk state directory. To back up Ravel, back up
  the bucket (or rely on its durability).

## Garbage collection and retention

Ravel deletes data through two independent triggers. The background
maintenance loop (`ravel-server --mode maintain`) drives both, or one-shot
from `ravel-cli maintain`. Objects are immutable throughout. Deletion
removes whole objects; nothing is ever modified in place
([docs/consistency-model.md](../consistency-model.md#deletion-and-gc),
docs/compaction-retention-plan.md, ADR-0018/ADR-0019). All of it is
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
  `maintain sweep`, `maintain status`, `maintain audit-versions`, and
  `maintain verify-custody` (see the CLI table above). `compact-bucket`
  and `sweep` take `--dry-run` to report exactly what a real run would
  write or delete, without mutating anything; `verify-custody` is
  read-only and has no `--dry-run` since there is nothing to simulate.

### Maintenance safety metrics and alerts

`--mode maintain` renders four additional samples on the existing `GET
/metrics` endpoint (ADR-0044 section 4), alongside the tenant-discovery
gauges (issue #504): `ravel_maintain_legal_hold_refresh_failures_total`
(counter), `ravel_maintain_conservation_aborts_total` (counter, labeled
by `signal`), `ravel_maintain_orphan_breaker_tripped_total` (counter,
labeled by `signal`), and `ravel_maintain_orphans_withheld` (gauge,
labeled by `signal`). No new label is added to the renderer's
compile-time-closed allowlist; these reuse the existing `mode` and
`signal` labels only. ADR-0048 names `tenant_hash` as a label on the
orphan-breaker-trip counter, but ADR-0044 blocks any
`tenant_hash`-labeled sample on the unauthenticated `/metrics` route
unless the opt-in `--metrics-tenant-labels` flag is set (see below); by
default all four samples stay process-wide totals, not broken out per
tenant.

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

### Per-query cost accounting (ADR-0044, issue #425)

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
| Discovered tenants not maintained | `ravel_maintain_tenants_maintained < ravel_maintain_tenants_discovered` for `10m` | A prefix under `t/` holds data with no maintaining owner, the exact `maintained < discovered` condition ADR-0048 decision 3 names, and the same S2-17/S5-09 finding recurring for a different reason (ADR-0048 Context). Ten minutes is two cycles at the default 300s `--maintain-interval-secs`, long enough that a single tick's transient gap (a restart, a tenant mid-onboarding) doesn't page, short enough that a real gap alarms within the hour. |
| Tenant discovery failing | `increase(ravel_maintain_tenant_discovery_failures_total[5m]) > 0` | A failed `LIST t/` skips the *entire* cycle, every tenant, not just one (ADR-0048 decision 3): the supervisor deliberately never treats a failed enumeration as "no tenants" so it can't be confused with healthy idleness, but that means a sustained failure is a fully silent maintenance outage. Alarm on the first occurrence rather than waiting for a sustained window, faster than the gauge condition above, because a skipped cycle is worse than one tenant falling behind: nothing is being maintained at all. |

`ravel_maintain_orphans_withheld` is a gauge, not an alert target: it
reflects only the most recent sweep pass and drops to zero on the very
next non-tripping pass, including one that stopped tripping only
because of dilution or partial restoration (see below). It is for
inspecting the size of the most recent withheld set once the trip
counter has already told you a trip happened, not for detecting the
trip itself.

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

**Known blind spots (tracked as open gaps in issue #500, not fixed by
this design)**:

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

## Known limitations

From [PROGRESS.md](../../PROGRESS.md), as of the Phase 1 vertical slice:

- Catalog snapshot resolution (docs/metric-index-plan.md phase 4) removes
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
  100,000 series, in the committed microbenchmarks
  ([BENCHMARKS.md](../../BENCHMARKS.md)).
- Parenthesized PromQL expressions (`(up)`) are rejected as unsupported.
  This is a known gap (issue #10), not a silent reinterpretation.
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
