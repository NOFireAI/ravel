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

Research prototype. Phase 1 is a complete, working vertical slice for metrics:
OTLP ingest, object-native commit, PromQL selector queries, all running end to
end against MinIO or S3, with 229 tests passing (unit, property, and a crash
matrix that asserts every row of the consistency model). See
[PROGRESS.md](PROGRESS.md) for the full log.

What exists today:

- OTLP HTTP and gRPC metrics ingest (gauges and cumulative sums), with
  admission limits, event-time skew bounds, and strict or buffered
  acknowledgement.
- Object-native commit: every flush produces one immutable segment object and
  one immutable commit record, both content-addressed.
- A catalog that resolves a consistent snapshot of segments per query via
  listing (no compaction yet).
- PromQL vector selectors (all matcher types, `offset`, 5m lookback) over
  `/api/v1/query` and `/api/v1/query_range`, plus `/api/v1/labels`,
  `/api/v1/label/{name}/values`, and `/api/v1/series`.
- `ravel-server` (dev binary, all roles in one process) and `ravel-cli`
  (segment/commit/catalog inspection).

What is planned, not built:

- Remote Write 1.0/2.0, OTel logs/traces/profiles, native histograms and
  exemplars.
- A full PromQL evaluator (aggregations, binary operators, functions,
  subqueries) with a differential test suite against Prometheus.
- L0-to-L1 compaction, exact rollups, retention, and deletion GC. Today,
  orphaned L0 objects (data written but never committed) are the only thing
  garbage collected, and that collector is a documented design, not shipped
  code yet.
- Catalog snapshots (an index object instead of per-query listing), needed
  before listing-based discovery runs out of headroom.
- OTAP (OpenTelemetry Arrow) ingest, scaffolded but not wired into the
  gateway.
- SQL over the metrics data: a `ravel-sql` crate (DataFusion) implementing
  the scan -> sort-preserving-merge -> dedup pipeline, matching PromQL's
  duplicate-sample resolution bit-for-bit. In progress; not yet wired to an
  HTTP or Flight SQL endpoint. See [ADR-0013](docs/adrs/0013-arrow-zero-copy-and-datafusion.md).

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
