# Ravel

[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/pmoust/b45c736cf13204279b05507186c24325/raw/coverage.json)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

Ravel is an OpenTelemetry-native observability database. It stores metrics
directly on S3-compatible object storage. It keeps no local disk state and uses
no coordination service. Logs, traces, and profiles are planned. A query uses
PromQL over HTTP. An ingest client uses OTLP over HTTP or gRPC.

The main property is this: the object store is the only durable part of the
system. Every Ravel process (gateway, ingest shard, query frontend) is
disposable and stateless. If Ravel acknowledges a write in strict mode, that
data survives the crash, restart, or redeployment of any Ravel process. It
survives because the object store survives it.

## Status

Ravel is a research prototype. It is a working vertical slice for metrics: OTLP
ingest through PromQL and SQL query, from end to end against MinIO or S3. For a
log of what is built and when, see [PROGRESS.md](PROGRESS.md).

Ravel has these features today:

- OTLP HTTP and gRPC metrics ingest (gauges and cumulative sums). It has
  admission limits, event-time skew bounds, and strict or buffered
  acknowledgement.
- Object-native commit: each flush produces one immutable segment object and
  one immutable commit record. Both are content-addressed.
- A catalog that resolves a consistent snapshot of segments for each query
  through listing.
- L0-to-L1 compaction, age-based retention, and a garbage-collecting sweeper
  (orphan GC, superseded-input sweep, unreferenced-part cleanup). It is signal-
  generic across metrics and logs. It runs continuously with `ravel-server
  --mode maintain`, or one-shot with `ravel-cli maintain
  compact-bucket|sweep|status|audit-versions`. See
  [docs/compaction-retention-plan.md](docs/compaction-retention-plan.md),
  [ADR-0018](docs/adrs/0018-l0-l1-compaction.md),
  [ADR-0019](docs/adrs/0019-age-based-retention.md). The continuous background
  loop needs a multipart-capable object store backend. No shipped backend
  implements one yet (issue #243). One-shot compaction and sweep via
  `ravel-cli` work today regardless.
- PromQL: vector and matrix selectors (all matcher types, `offset`, `@`, 5m
  lookback), binary operators, and most of the function library (`rate`,
  `histogram_quantile`, the `*_over_time` family, label and math functions).
  These work over `/api/v1/query` and `/api/v1/query_range`, plus
  `/api/v1/labels`, `/api/v1/label/{name}/values`, and `/api/v1/series`, with
  `/api/v1/status/buildinfo`, `/api/v1/metadata`, `/-/healthy`, and `/-/ready`
  for clients (Grafana's Prometheus datasource) that probe them.
  Aggregation operators (`sum by (...)`, `topk`, ...) and subqueries do not work
  yet.
- Native (exponential) histograms from end to end. Ravel ingests and stores
  them (RSEG v5), queries them, and reduces them to floats with the native-
  histogram PromQL functions: `histogram_count`, `histogram_sum`, and
  `histogram_avg`; `histogram_quantile` and `histogram_fraction` over a native
  histogram; `rate`/`increase`/`delta` over a histogram range; and `sum`/`avg`
  aggregation over histogram-valued series. Native
  `histogram_quantile`/`histogram_fraction` interpolate linearly within a
  bucket, but Prometheus 3.x interpolates exponentially. Their values can
  therefore differ from Prometheus for interior quantiles.
- SQL over the same data through DataFusion (`ravel-sql`): `POST /api/v1/sql`,
  behind `ravel-server`'s `sql` cargo feature. It has a read-only `samples`
  table and the same duplicate-sample resolution as PromQL, bit-for-bit. See
  [ADR-0013](docs/adrs/0013-arrow-zero-copy-and-datafusion.md).
- Post-evaluation analytics (`ravel-analytics`): `POST /api/v1/analytics` runs a
  range query, then applies change point detection or robust summary statistics
  to each series. See
  [ADR-0028](docs/adrs/0028-analytics-stage.md).
- `ravel-server` (dev binary, all roles in one process) and `ravel-cli`
  (segment, commit, and catalog inspection).
- Flight SQL: the same query path as `/api/v1/sql`, over Arrow Flight's gRPC
  surface, behind `ravel-server`'s `flight-sql` cargo feature (which implies
  `sql`).

These features are planned, not built:

- Remote Write 1.0/2.0, OTel logs/traces/profiles, and exemplars.
- PromQL aggregation operators and subqueries, with a differential test
  suite against Prometheus for every construct the evaluator supports.
- Exact rollups (a second, aggregated level beyond L0/L1).
- Catalog snapshots (an index object instead of per-query listing). These are
  necessary before listing-based discovery runs out of headroom.
- OTAP (OpenTelemetry Arrow) ingest. It is scaffolded but not yet wired into
  the gateway.

## Quickstart

Prerequisites: Rust through `rust-toolchain.toml` (the rustc version is pinned;
`rustup` installs it automatically if you have it), and Docker with `docker
compose`.

```sh
make minio   # starts MinIO + bucket creation via docker compose
make demo    # builds ravel-server/ravel-cli, ingests one OTLP export, queries it back
```

`make demo` runs [scripts/demo.sh](scripts/demo.sh). It starts `ravel-server`
against MinIO, sends a generated OTLP metrics export over HTTP, prints the
commit token it receives, and queries that metric by `min_commit_token`. For the
full walkthrough with expected output, see
[docs/guides/getting-started.md](docs/guides/getting-started.md).

After MinIO is up, here is the same flow by hand:

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

### On Kubernetes

Here is the same round trip against a real local Kubernetes cluster. The Ravel
operator drives it instead of a local process:

```sh
scripts/kind-up.sh     # kind cluster, both images, fake S3, operator, RavelCluster
scripts/kind-demo.sh   # OTLP ingest via the gateway, query back via the query tier
scripts/kind-down.sh   # delete the cluster
```

This needs `docker`, `kind`, and `kubectl`. For the full walkthrough, the
`RavelCluster` field reference, and probe semantics, see
[docs/guides/kubernetes.md](docs/guides/kubernetes.md).

### Container images

`ravel-server` and `ravel-operator` images are built in CI on native amd64
runners and published to GitHub Container Registry (ADR-0037):

```sh
docker pull ghcr.io/nofireai/ravel-server:latest
docker pull ghcr.io/nofireai/ravel-operator:latest
```

GHCR creates a package private on its first push. Until someone flips
`ravel-server` and `ravel-operator` to public in the package settings (a
one-time step after the first tag is published), the pull above needs
`docker login ghcr.io` with a PAT that has `read:packages` first.

Tags: `X.Y.Z`, `X.Y`, `X`, and `latest` on a `vX.Y.Z` git tag push;
`manual-<short-sha>` from a manual `workflow_dispatch` run of
[`publish-images.yml`](.github/workflows/publish-images.yml). There is no
image for an ordinary push to `main`; see the ADR for why.

Building an amd64 image locally on an Apple Silicon Mac (`docker build
--platform linux/amd64 ...`) is not supported: Docker Desktop cross-emulates
amd64 through QEMU, and `rustc` reliably segfaults under that emulation. Pull
the published image, or build natively on an amd64 host, instead.

## Querying

After you ingest data (see Quickstart), you can query it as PromQL or as SQL.
Both read the same catalog and segments. Both apply the same duplicate-sample
resolution, so they agree on results.

### PromQL

`/api/v1/query` (instant) and `/api/v1/query_range` (a grid of steps) support
selectors, `offset`/`@`, binary operators, and most of the function library
(`rate`, `histogram_quantile`, the `*_over_time` family, label and math
functions). Aggregation operators (`sum by (...)`, `topk`, ...) and
subqueries do not work yet.

You can query native (exponential) histograms: `histogram_count`,
`histogram_sum`, `histogram_avg`, `histogram_quantile`, and
`histogram_fraction` over a native histogram, plus `rate`/`increase`/`delta`
over a histogram range and `sum`/`avg` aggregation over histogram-valued
series. Native `histogram_quantile`/`histogram_fraction` interpolate linearly
within a bucket, not exponentially. Interior-quantile values can therefore
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

`POST /api/v1/sql` runs a read-only SQL statement through DataFusion. It uses
one of two tables: `samples(ts, value, ...)` (metrics) or `logs(ts,
severity_text, body, attrs, ...)` (ADR-0033). A query uses one table or the
other, never both. This endpoint is off by default. To enable it, build or run
`ravel-server` with the `sql` cargo feature.

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

The endpoint accepts only `SELECT` (no `INSERT`/`COPY`/`CREATE EXTERNAL
TABLE`/`SET`/multi-statement bodies). Rejected statements and execution errors
return a redacted `{"status":"error","errorType":...,"error":...}` body. They
never return raw backend or DataFusion plan text. For a bit-exact Arrow IPC
stream, send `Accept: application/vnd.apache.arrow.stream` instead of JSON. This
is necessary for `NaN`/`-0.0` payloads, which JSON cannot represent exactly.
Flight SQL (the gRPC equivalent) is available behind `ravel-server`'s
`flight-sql` cargo feature. For the full `logs` table reference (schema,
supported predicates, known gaps), see docs/guides/query.md.

### Analytics

`POST /api/v1/analytics` runs a range query exactly as `/api/v1/query_range`
does (same planner, budgets, staleness handling, and deadline). It then applies
one analytic operation to each series of the result (ADR-0028). Two ops are
available. `change_point` does PELT change point detection; it classifies
spikes, dips, step changes, trend changes, and distribution changes. `summary`
computes the exact median, MAD, percentiles, standard deviation, and variance.
This endpoint shares the query listener and needs no cargo feature.

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

`start`/`end`/`step` use the same syntax as `/api/v1/query_range`. A series with
more than 2000 points needs `"downsample": true` to run `change_point`
(approximation is opt-in and visible). A call that matches more than 1000 series
is rejected. For the request and response schema, a worked example for each op,
and the error table, see [docs/analytics.md](docs/analytics.md).

## Architecture

![architecture](docs/diagrams/architecture.svg)

OTLP arrives at the gateway. The gateway authenticates the request, resolves a
tenant, and checks admission limits before it buffers anything. Points route to
a shard actor by `hash(tenant, series_id)`. Each shard actor is a single task
with no locks. It buffers points until a size or age trigger fires a flush. A
flush does these steps in order: it serializes an RSEG segment, PUTs it to the
object store, mints an immutable commit record, and PUTs that record too. Only
then does Ravel acknowledge the waiting requests with a commit token.

On the query side, `/api/v1/*` resolves a snapshot of segments from the catalog.
It lists commit records in the relevant shard and hour buckets. It then fetches
each segment's footer, prunes series by label matchers, fetches the necessary
pages, and hands the merged samples to the PromQL evaluator.

Everything after "PUT succeeded" is derived, replaceable, and disposable. For
the full crate dependency graph, see
[docs/architecture.md](docs/architecture.md). For the reason each piece is built
this way, see [docs/adrs/](docs/adrs/).

## Repository layout

- `crates/`: libraries (types, object store, segment format, commit
  protocol, catalog, OTLP/OTAP decode, ingest actors, PromQL, query engine,
  and `ravel-sql`, a DataFusion-backed SQL pipeline, in progress)
- `services/`: `ravel-server` (gateway + ingest + query, dev binary) and
  `ravel-cli` (segment/commit/catalog inspector)
- `docs/`: specs, ADRs, diagrams, and the user guides in `docs/guides/`
- `proto/`: protobuf schemas, vendored OTAP protos
- `deploy/docker-compose/`: local MinIO stack
- `deploy/k8s/`: operator manifests (CRD, RBAC, Deployment), an example
  `RavelCluster`, and the fake-S3 backends for the kind environment
- `scripts/`: `demo.sh`, the end-to-end demo driver, and `kind-up.sh` /
  `kind-demo.sh` / `kind-down.sh`, the same round trip on Kubernetes

## Documentation

- [docs/README.md](docs/README.md): index of every guide and spec
- [docs/segment-format.md](docs/segment-format.md): the RSEG data format. It is
  a self-contained specification of RSEG v5 (the columnar catalog, native
  histograms, multi-run compaction layout, and the optional sparse catalog).
  RSEG v5 is the only version you can read or write pre-release (ADR-0027). The
  byte-layout diagram is in
  [docs/diagrams/rseg-layout.svg](docs/diagrams/rseg-layout.svg).
- [docs/guides/](docs/guides/): getting started, ingest, query, operations,
  inspecting data, Kubernetes
- [docs/adrs/](docs/adrs/): one decision record per architectural choice
- [docs/sql-conformance.md](docs/sql-conformance.md): the SQL surface
  conformance table and score. It classifies every construct as supported,
  intentionally rejected, or unclassified (ADR-0035).
- [BENCHMARKS.md](BENCHMARKS.md): measured numbers, with the exact commands
  and environment that produced them

## Reading the history

[PROGRESS.md](PROGRESS.md) is a living log, newest first, of what is built, what
broke, and what is next. It is the fastest way to see how the project got here,
against what the specs describe in the abstract. The GitHub issue tracker
carries the same story in smaller pieces, resolved or not.
