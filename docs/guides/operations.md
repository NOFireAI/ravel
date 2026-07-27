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

`segment inspect` and `commit decode` accept a local file path or an
object-store key; a path that exists on disk is read directly, otherwise
it's fetched from the configured store.

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

- Catalog snapshot resolution lists commit records per (tenant, shard,
  hour) bucket on every query. This does not scale past roughly 10,000
  commits in one bucket; catalog snapshots (a precomputed index object)
  are planned to replace it before that becomes a problem in practice.
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
