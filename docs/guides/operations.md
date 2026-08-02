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

`--store s3` without `--s3-bucket`/`--s3-access-key`/`--s3-secret-key` (through
flag or env) fails at startup with an explicit error that names the missing
one. It does not start in a broken state.

Note: [BENCHMARKS.md](../../BENCHMARKS.md) documents the S3 env vars as
`RAVEL_S3_ACCESS_KEY_ID` and `RAVEL_S3_SECRET_ACCESS_KEY`. The real flags
are `RAVEL_S3_ACCESS_KEY` and `RAVEL_S3_SECRET_KEY`, above; use those.
`allow_http` and `force_path_style` are not configurable at all. The code
derives `allow_http` from whether `--s3-endpoint` is set, and it always passes
`force_path_style: true`.

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
