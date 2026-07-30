# Ravel

[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/pmoust/b45c736cf13204279b05507186c24325/raw/coverage.json)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

Ravel is an OpenTelemetry-native observability database. It stores metrics
(logs, traces, and profiles are planned) directly on S3-compatible object
storage, with no local disk state and no coordination service. A query talks
PromQL over HTTP; an ingest client talks OTLP over HTTP or gRPC.

The core property: the object store is the only durable thing in the system.
Every Ravel process (gateway, ingest shard, query frontend) is disposable and
stateless. Once a write is acknowledged in strict mode, that data survives the
crash, restart, or redeployment of any Ravel process, because it survives
anything the object store survives.

## Status

Research prototype: a working vertical slice for metrics, OTLP ingest through
PromQL and SQL query, running end to end against MinIO or S3. See
[PROGRESS.md](PROGRESS.md) for the living log of what has been built and
when.

What exists today:

- OTLP HTTP and gRPC metrics ingest (gauges and cumulative sums), with
  admission limits, event-time skew bounds, and strict or buffered
  acknowledgement.
- Object-native commit: every flush produces one immutable segment object and
  one immutable commit record, both content-addressed.
- A catalog that resolves a consistent snapshot of segments per query via
  listing.
- L0-to-L1 compaction, age-based retention, and a garbage-collecting sweeper
  (orphan GC, superseded-input sweep, unreferenced-part cleanup), signal-
  generic across metrics and logs. Runs continuously via `ravel-server
  --mode maintain`, or one-shot via `ravel-cli maintain
  compact-bucket|sweep|status|audit-versions`. See
  [docs/compaction-retention-plan.md](docs/compaction-retention-plan.md),
  [ADR-0018](docs/adrs/0018-l0-l1-compaction.md),
  [ADR-0019](docs/adrs/0019-age-based-retention.md). The continuous
  background loop needs a multipart-capable object store backend, which
  no shipped backend implements yet (issue #243); one-shot compaction/sweep
  via `ravel-cli` work today regardless.
- PromQL: vector/matrix selectors (all matcher types, `offset`, `@`, 5m
  lookback), binary operators, and most of the function library (`rate`,
  `histogram_quantile`, the `*_over_time` family, label and math functions)
  over `/api/v1/query` and `/api/v1/query_range`, plus `/api/v1/labels`,
  `/api/v1/label/{name}/values`, and `/api/v1/series`. Aggregation operators
  (`sum by (...)`, `topk`, ...) and subqueries are not implemented yet.
- Native (exponential) histograms end to end: ingested and stored (RSEG v5),
  queried, and reduced to floats by the native-histogram PromQL functions
  `histogram_count`, `histogram_sum`, and `histogram_avg`; `histogram_quantile`
  and `histogram_fraction` over a native histogram; `rate`/`increase`/`delta`
  over a histogram range; and `sum`/`avg` aggregation over histogram-valued
  series. Native `histogram_quantile`/`histogram_fraction` currently
  interpolate within a bucket linearly, where Prometheus 3.x interpolates
  exponentially, so their values can differ from Prometheus for interior
  quantiles.
- SQL over the same data via DataFusion (`ravel-sql`): `POST /api/v1/sql`,
  behind `ravel-server`'s `sql` cargo feature, with a read-only `samples`
  table and the same duplicate-sample resolution as PromQL, bit-for-bit. See
  [ADR-0013](docs/adrs/0013-arrow-zero-copy-and-datafusion.md).
- Post-evaluation analytics (`ravel-analytics`): `POST /api/v1/analytics`
  runs a range query and applies change point detection or robust summary
  statistics per series. See
  [ADR-0028](docs/adrs/0028-analytics-stage.md).
- `ravel-server` (dev binary, all roles in one process) and `ravel-cli`
  (segment/commit/catalog inspection).
- Flight SQL: the same query path as `/api/v1/sql`, over Arrow Flight's gRPC
  surface, behind `ravel-server`'s `flight-sql` cargo feature (implies `sql`).

What is planned, not built:

- Remote Write 1.0/2.0, OTel logs/traces/profiles, and exemplars.
- PromQL aggregation operators and subqueries, with a differential test
  suite against Prometheus for everything the evaluator does support.
- Exact rollups (a second, aggregated level beyond L0/L1).
- Catalog snapshots (an index object instead of per-query listing), needed
  before listing-based discovery runs out of headroom.
- OTAP (OpenTelemetry Arrow) ingest, scaffolded but not wired into the
  gateway.

## Quickstart

Prerequisites: Rust via `rust-toolchain.toml` (rustc pinned, installed
automatically by `rustup` if you have it), Docker with `docker compose`.

```sh
make minio   # starts MinIO + bucket creation via docker compose
make demo    # builds ravel-server/ravel-cli, ingests one OTLP export, queries it back
```

`make demo` runs [scripts/demo.sh](scripts/demo.sh): it starts `ravel-server`
against MinIO, sends a generated OTLP metrics export over HTTP, prints the
commit token it got back, and queries that metric by `min_commit_token`. Full
walkthrough with expected output: [docs/guides/getting-started.md](docs/guides/getting-started.md).

The same flow by hand, once MinIO is up:

```sh
cargo run -p ravel-server -- \
  --store s3 --s3-endpoint http://127.0.0.1:9000 --s3-bucket ravel-dev \
  --s3-access-key ravel --s3-secret-key ravel-dev-secret \
  --tenant-token devtoken=acme &

curl -X POST http://127.0.0.1:4318/v1/metrics \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @your_otlp_export.pb -D -

curl -G http://127.0.0.1:4318/api/v1/query \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=your_metric_name" \
  --data-urlencode "min_commit_token=<token from the export response>"
```

## Querying

Once data is ingested (see Quickstart), query it either as PromQL or as SQL.
Both read the same catalog and segments and apply the same duplicate-sample
resolution, so they agree on results.

### PromQL

`/api/v1/query` (instant) and `/api/v1/query_range` (a grid of steps) support
selectors, `offset`/`@`, binary operators, and most of the function library
(`rate`, `histogram_quantile`, the `*_over_time` family, label and math
functions). Aggregation operators (`sum by (...)`, `topk`, ...) and
subqueries are not implemented yet.

Native (exponential) histograms are queryable: `histogram_count`,
`histogram_sum`, `histogram_avg`, `histogram_quantile`, and
`histogram_fraction` over a native histogram, plus `rate`/`increase`/`delta`
over a histogram range and `sum`/`avg` aggregation over histogram-valued
series. Native `histogram_quantile`/`histogram_fraction` interpolate within a
bucket linearly rather than exponentially, so interior-quantile values can
differ from Prometheus 3.x.

```sh
# Instant: a binary expression over a function result
curl -G http://127.0.0.1:4318/api/v1/query \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=rate(http_requests_total[5m]) > 0" \
  --data-urlencode "time=<unix seconds>"

# Range: the same selector evaluated over a grid of steps
curl -G http://127.0.0.1:4318/api/v1/query_range \
  -H "Authorization: Bearer devtoken" \
  --data-urlencode "query=rate(http_requests_total[5m])" \
  --data-urlencode "start=<unix seconds>" \
  --data-urlencode "end=<unix seconds>" \
  --data-urlencode "step=15s"
```

### SQL

`POST /api/v1/sql` runs a read-only SQL statement via DataFusion, against
either of two tables: `samples(ts, value, ...)` (metrics) or `logs(ts,
severity_text, body, attrs, ...)` (ADR-0033). A query references one or the
other, never both. It is off by default: build or run `ravel-server` with the
`sql` cargo feature to enable it.

```sh
cargo run -p ravel-server --features sql -- \
  --store s3 --s3-endpoint http://127.0.0.1:9000 --s3-bucket ravel-dev \
  --s3-access-key ravel --s3-secret-key ravel-dev-secret \
  --tenant-token devtoken=acme &

curl -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "SELECT ts, value FROM samples ORDER BY ts",
        "start": 0,
        "end": 1893456000
      }'
# {"status":"success","data":{"rows":[[100,1.0],[200,2.5]]}}

curl -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "SELECT ts, body FROM logs WHERE has_word(body, '\''timeout'\'') ORDER BY ts",
        "start": 0,
        "end": 1893456000
      }'
# {"status":"success","data":{"rows":[[150,"connection timeout"]]}}
```

Only `SELECT` is accepted (no `INSERT`/`COPY`/`CREATE EXTERNAL TABLE`/`SET`/
multi-statement bodies); rejected statements and execution errors come back
as a redacted `{"status":"error","errorType":...,"error":...}` body, never
raw backend or DataFusion plan text. Send `Accept:
application/vnd.apache.arrow.stream` instead of JSON for a bit-exact Arrow
IPC stream (needed for `NaN`/`-0.0` payloads, which JSON cannot represent
exactly). Flight SQL (the gRPC equivalent) is available behind
`ravel-server`'s `flight-sql` cargo feature; see docs/guides/query.md for the
full `logs` table reference (schema, supported predicates, known gaps).

### Analytics

`POST /api/v1/analytics` runs a range query exactly as `/api/v1/query_range`
does (same planner, budgets, staleness handling, and deadline), then applies
one analytic operation to each series of the result (ADR-0028). Two ops are
available: `change_point` (PELT change point detection, classifying spikes,
dips, step changes, trend changes, and distribution changes) and `summary`
(exact median, MAD, percentiles, standard deviation, and variance). It shares
the query listener and needs no cargo feature.

```sh
curl -X POST http://127.0.0.1:4318/api/v1/analytics \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "http_requests_total",
        "start": 0,
        "end": 1893456000,
        "step": "30s",
        "op": {"type": "change_point", "downsample": false}
      }'
# {"status":"success","data":{"resultType":"analytics","result":[
#   {"metric":{"__name__":"http_requests_total"},
#    "result":{"kind":"step_change","ts_ns":1735691400000000000,"score":42.1,
#              "downsampled":false,"original_points":120,"nan_excluded":0}}]}}
```

`start`/`end`/`step` use the same syntax as `/api/v1/query_range`. A series
over 2000 points needs `"downsample": true` to run `change_point`
(approximation is opt-in and visible); a call matching over 1000 series is
rejected. See [docs/analytics.md](docs/analytics.md) for the request and
response schema, a worked example per op, and the error table.

## Architecture

![architecture](docs/diagrams/architecture.svg)

OTLP arrives at the gateway, which authenticates the request, resolves a
tenant, and checks admission limits before anything is buffered. Points route
to a shard actor by `hash(tenant, series_id)`; each shard actor is a single
task with no locks, buffering points until a size or age trigger fires a
flush. A flush serializes an RSEG segment, PUTs it to the object store,
mints an immutable commit record, and PUTs that too, in that order; only
then are waiting requests acknowledged with a commit token.

On the query side, `/api/v1/*` resolves a snapshot of segments from the
catalog (by listing commit records in the relevant shard/hour buckets),
fetches each segment's footer, prunes series by label matchers, fetches the
needed pages, and hands the merged samples to the PromQL evaluator.

Everything downstream of "PUT succeeded" is derived, replaceable, and
disposable. See [docs/architecture.md](docs/architecture.md) for the full
crate dependency graph and [docs/adrs/](docs/adrs/) for why each piece is
built this way.

## Repository layout

- `crates/`: libraries (types, object store, segment format, commit
  protocol, catalog, OTLP/OTAP decode, ingest actors, PromQL, query engine,
  and `ravel-sql`, a DataFusion-backed SQL pipeline, in progress)
- `services/`: `ravel-server` (gateway + ingest + query, dev binary) and
  `ravel-cli` (segment/commit/catalog inspector)
- `docs/`: specs, ADRs, diagrams, and the user guides in `docs/guides/`
- `proto/`: protobuf schemas, vendored OTAP protos
- `deploy/docker-compose/`: local MinIO stack
- `scripts/`: `demo.sh`, the end-to-end demo driver

## Documentation

- [docs/README.md](docs/README.md): index of every guide and spec
- [docs/segment-format.md](docs/segment-format.md): the RSEG data format —
  a self-contained specification of RSEG v5 (the columnar catalog, native
  histograms, multi-run compaction layout, and the optional sparse catalog),
  the only version readable or writable pre-release (ADR-0027), with the
  byte-layout diagram in
  [docs/diagrams/rseg-layout.svg](docs/diagrams/rseg-layout.svg)
- [docs/guides/](docs/guides/): getting started, ingest, query, operations,
  inspecting data
- [docs/adrs/](docs/adrs/): one decision record per architectural choice
- [BENCHMARKS.md](BENCHMARKS.md): measured numbers, with the exact commands
  and environment that produced them

## Reading the history

[PROGRESS.md](PROGRESS.md) is a living log, newest first, of what has been
built, what broke, and what's next; it's the fastest way to see how the
project actually got here versus what the specs describe in the abstract.
The GitHub issue tracker carries the same story in smaller, resolved-or-not
pieces.
