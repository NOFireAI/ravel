# Operations

## `ravel-server` flags

All flags, verified against [services/ravel-server/src/config.rs](../../services/ravel-server/src/config.rs):

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--mode <all\|gateway\|query>` | | `all` | Which roles this process runs. `all` and `gateway` serve OTLP ingest; `all` and `query` serve `/api/v1/*`. |
| `--listen-http <addr>` | | `127.0.0.1:4318` | HTTP listener for OTLP ingest (`POST /v1/metrics`) and the query API. |
| `--listen-grpc <addr>` | | `127.0.0.1:4317` | gRPC listener for the OTLP `MetricsService`. Only bound when the process runs ingest (`all`/`gateway`). |
| `--store <memory\|s3>` | | `memory` | Object store backend. `memory` is in-process only, for tests and local experiments; nothing survives process exit. |
| `--shards <n>` | | `4` | Ingest shard count. Sets both the ingest router's shard count and the query-side catalog's shard count, so they must agree; there's no separate query-side flag. |
| `--tenant-token TOKEN=TENANT` | | none, repeatable | Registers one bearer token for the static resolver. Pass it once per tenant. With no `--tenant-token` at all, every request is unauthenticated and rejected. |
| `--dev-insecure-tenant-header` | | off | Adds tenant resolution via the `x-ravel-tenant` request header, tried only when bearer lookup fails. The process refuses to start with this set unless `--listen-http` binds a loopback address. |
| `--s3-endpoint <url>` | `RAVEL_S3_ENDPOINT` | none | Custom S3 endpoint (MinIO, or any S3-compatible store). Unset means real AWS S3. Setting it also turns on `allow_http` for that endpoint. |
| `--s3-bucket <name>` | `RAVEL_S3_BUCKET` | none | Required when `--store s3`. |
| `--s3-region <region>` | `RAVEL_S3_REGION` | `us-east-1` | |
| `--s3-access-key <key>` | `RAVEL_S3_ACCESS_KEY` | none | Required when `--store s3`. |
| `--s3-secret-key <secret>` | `RAVEL_S3_SECRET_KEY` | none | Required when `--store s3`. |
| `--disable-fold` | | off | Disables the per-(tenant, signal) background catalog fold task (docs/metric-index-plan.md section 4). Folding only lowers query resolve cost; disabling it never changes query results. |
| `--fold-interval-secs <n>` | | `300` | How often each tenant's fold task wakes up to check for newly sealed hours. |

`--store s3` without `--s3-bucket`/`--s3-access-key`/`--s3-secret-key` (via
flag or env) fails at startup with an explicit error naming the missing
one, rather than starting in a broken state.

Note: [BENCHMARKS.md](../../BENCHMARKS.md) documents S3 env vars as
`RAVEL_S3_ACCESS_KEY_ID` and `RAVEL_S3_SECRET_ACCESS_KEY`. The real flags
are `RAVEL_S3_ACCESS_KEY` and `RAVEL_S3_SECRET_KEY`, above; use those.
`allow_http` and `force_path_style` are not configurable at all: the code
derives `allow_http` from whether `--s3-endpoint` is set and always passes
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
| `ravel-cli catalog list --tenant <name> [--hours <n>] [--shards <n>]` | `--hours` default `1`, `--shards` default `4` | Lists commit records the catalog resolves for that tenant over the last `hours` hours. `--shards` must match the shard count the data was written with. |
| `ravel-cli catalog fold --tenant <name> [--shards <n>]` | `--shards` default `4` | One-shot catalog fold: seals every eligible hour into a new snapshot part and CAS-advances HEAD. Prints the fold report (watermark before/after, buckets folded, entry count, request counts). Same operation the background fold task runs on a timer. |
| `ravel-cli catalog inspect --tenant <name>` | | Decodes and prints HEAD and every referenced snapshot part: watermark, part keys, hashes, entry counts. Reports rather than errors when no HEAD exists yet. |
| `ravel-cli catalog verify --tenant <name>` | | Re-lists every sealed commit record and diffs it against the current snapshot. Prints counts of entries missing from or mismatched against the snapshot; exits nonzero on any divergence. Reports rather than errors when no HEAD exists yet. |

`segment inspect` and `commit decode` accept a local file path or an
object-store key; a path that exists on disk is read directly, otherwise
it's fetched from the configured store.

## Catalog fold and verify

The catalog fold (docs/metric-index-plan.md, ADR-0020) is a query-cost
optimization, not a durability mechanism: `resolve` always falls back to
listing commit records directly, so a folder that never runs, crashes, or
falls behind never loses or hides data, it only makes queries pay Phase 1
listing cost for a wider window (docs/consistency-model.md "Catalog
snapshot staleness"; every row is exercised end to end in
`crates/ravel-failure-tests/tests/folder_crash_matrix.rs`).

**Seal-margin config discipline.** A fold only seals an hour once
`now >= hour_end + max_flush_lifetime + clock_skew_allowance +
fold_safety_margin` (defaults 1h + 5m + 15m = 1h20m,
`crates/ravel-catalog/src/config.rs`). These three margins exist to give
every writer's flush for that hour time to land before the fold treats it
as closed. Widening `max_flush_lifetime` (writers hold flushes open
longer) or the tolerated wall-clock skew between writers and the folder
without also reviewing `fold_safety_margin` risks the failure mode below.
`--fold-interval-secs` only controls how often the background task
*checks* for newly sealed hours; it has no bearing on when an hour becomes
eligible to seal.

**If a folder's clock runs fast beyond its margin**, it can seal an hour
before every writer's flush for it has landed. A commit published into
that already-sealed bucket becomes invisible to non-token queries (a
`min_commit_token` query is unaffected: it always GETs its exact commit
key directly, never through the snapshot). This is the one failure mode
in docs/metric-index-plan.md 5.3 that needs an operator repair rather than
resolving itself:

1. Run `ravel-cli catalog verify --tenant <name>` (per signal). A nonzero
   exit and a nonempty "missing from snapshot" count confirms sealed
   commits the snapshot doesn't know about.
2. Delete the tenant's HEAD object for the affected signal:
   `t/<tenant_hash_hex>/catalog/<signal>/HEAD` (`m` for metrics). There is
   no `ravel-cli` subcommand for this today; use the store's own tooling
   (`mc rm` against MinIO, `aws s3 rm` against S3). Deleting HEAD is safe:
   `Catalog::fold` treats an absent HEAD as "no snapshot yet" and rebuilds
   one from a full listing rather than erroring.
3. Run `ravel-cli catalog fold --tenant <name> --shards <n>` (or wait for
   the next background fold tick). The fold report's `rebuilt: true` line
   confirms it rebuilt from scratch rather than extending the prior
   snapshot.
4. Re-run `ravel-cli catalog verify --tenant <name>` to confirm the
   divergence is gone.

There is no `catalog fold --force-rebuild` flag; deleting HEAD is the
supported way to force one, since it reuses the same absent-HEAD path a
brand-new tenant takes on its first fold.

**Routine verification.** `catalog verify` is safe to run at any time
against a live tenant (it only lists and compares, never mutates); running
it on a schedule after deploying or reconfiguring seal margins is the
cheapest way to catch the clock-skew failure mode before it's noticed at
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

**AWS S3:** omit `--s3-endpoint` (S3 is the default when unset) and supply
a real bucket, region, and credentials:

```sh
--store s3 --s3-bucket my-ravel-bucket --s3-region us-west-2 \
--s3-access-key AKIA... --s3-secret-key ...
```

Ravel does not use the AWS credential chain (profiles, instance roles,
`AWS_ACCESS_KEY_ID`); it only reads the `RAVEL_S3_*` flags/env above.

## Tenancy setup

Tenants are configured entirely through repeated `--tenant-token
TOKEN=TENANT` flags on `ravel-server`; there is no tenant database or
admin API. Adding, removing, or rotating a tenant token means restarting
`ravel-server` with a different flag set, which is safe: every process is
stateless, so a restart with new tenant tokens has no data migration to
do. Tenant identity only affects key prefixing (`t/<tenant_hash>/...`,
where `tenant_hash` is a BLAKE3 hash of the tenant name) and query/ingest
authorization; it carries no other per-tenant configuration today (no
per-tenant quotas, no per-tenant storage backend).

## Disposability

Every Ravel process (any `--mode`) can be killed at any time with no
special shutdown sequence required for correctness:

- **Ingest shard actors** hold buffered-but-not-yet-flushed points only in
  memory. Killing the process loses that buffer; in strict mode, nothing
  in that buffer was ever acknowledged, so no acknowledged write is lost.
  In buffered mode, the acknowledged-but-unflushed window (bounded by
  `max_flush_delay`, 500ms default) is lost, by design.
  ([docs/consistency-model.md](../consistency-model.md))
- **Gateway and query processes** hold no durable state at all; they read
  and write the object store and otherwise only hold in-flight request
  state.
- **Recovery** is just starting a new process against the same object
  store and bucket. There is no replication to catch up, no leader
  election, no consensus round: any process can serve any request for any
  tenant, as long as it has the right `--tenant-token`/S3 credentials.
- **Nothing to back up** besides the object store bucket itself: no local
  volumes, no WAL, no on-disk state directory. Backing up Ravel means
  backing up (or relying on the durability of) the bucket.

## Garbage collection and retention

What's implemented today: nothing deletes data. Segments and commit
records, once written, stay forever.

What's designed but not built
([docs/consistency-model.md](../consistency-model.md#deletion-and-gc)):

- **Orphan GC**: a data object written but never committed (the process
  crashed between the data PUT and the commit PUT) is invisible to queries
  but still occupies space. The design calls for a sweep that considers
  only objects older than `grace` (default 24h) plus `max_flush_lifetime`
  (default 1h), re-verifies the commit record is still absent immediately
  before deleting (relying on the object store's listing being strongly
  consistent), and only then deletes. No such sweep exists in the codebase
  yet; orphans accumulate.
- **Tombstone-based deletion**: a durable tombstone transaction, then
  logical exclusion from new snapshots, then physical removal once nothing
  live references the object and its protection horizon has passed. Not
  implemented.
- **Compaction, rollups, retention windows**: none of these exist. Every
  ingested L0 segment is retained at full resolution indefinitely.

Operationally, this means: today, running Ravel against a bucket for a
long time only ever grows that bucket. Plan storage capacity accordingly
until compaction and retention ship.

## Known limitations

From [PROGRESS.md](../../PROGRESS.md), as of the Phase 1 vertical slice:

- Catalog snapshot resolution (docs/metric-index-plan.md phase 4) removes
  most of the per-query listing cost for sealed history, but only where
  the background fold has run: the open window above the fold watermark
  (bounded by `max_ingest_lag`, default 2h) and any tenant with folding
  disabled or not yet caught up still list commit records per (tenant,
  shard, hour) bucket on every query. That path does not scale past
  roughly 10,000 commits in one bucket.
- `promql-parser` (the upstream crate Ravel's evaluator sits on) has not
  been differentially validated against real Prometheus across a broad
  query corpus yet.
- RSEG encode throughput drops sharply at high series cardinality in one
  segment: about 14.7M samples/s at 100 series, down to about 235K/s at
  100,000 series, in the committed microbenchmarks
  ([BENCHMARKS.md](../../BENCHMARKS.md)).
- Parenthesized PromQL expressions (`(up)`) are rejected as unsupported,
  tracked as a known gap (issue #10) rather than being silently
  reinterpreted.
- No exactly-once ingestion guarantee: delivery is at-least-once, and a
  client-side retry after a lost ack response re-ingests the same points
  as a duplicate (both copies are stored; a query takes the last value at
  a given timestamp). An idempotency-key window to collapse these is
  planned, not built.
