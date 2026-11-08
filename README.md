# Ravel

[![Release](https://img.shields.io/github/v/release/NOFireAI/ravel)](https://github.com/NOFireAI/ravel/releases/latest)
[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/NOFireAI/ravel/graph/badge.svg)](https://codecov.io/gh/NOFireAI/ravel)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

**Ravel acknowledges a write when S3 has it.**

Ravel is a database for OpenTelemetry metrics, logs, and traces that keeps
all of its state in object storage: any S3-compatible store, or Google Cloud
Storage. There is no write-ahead log, no replicated ingest quorum, and no
StatefulSet to run. Kill any Ravel process at any moment, and every strictly
acknowledged write is still in the bucket.

- **Query with what you have.** A Prometheus-compatible API for Grafana, and
  SQL over metrics, logs, and traces.
- **Read your own write.** Each acknowledgement returns a commit token. A
  query that carries it is guaranteed to see that write.
- **Serve many tenants.** Per-tenant encryption keys (SSE-KMS), legal hold,
  and admission limits.

![A sample is ingested under strict acknowledgement, the ravel-server container is killed with SIGKILL and replaced by a fresh one, and the pre-kill sample is read back by its commit token.](docs/demo.gif)

## Quickstart

You need git and Docker with Compose. Nothing is compiled.

```sh
git clone https://github.com/NOFireAI/ravel.git
cd ravel
docker compose -f deploy/docker-compose/ravel.yml up -d
```

This pulls published images and starts RustFS for object storage,
`ravel-server` 0.31.0, an OpenTelemetry Collector that sends your host's
metrics and its own logs, and Grafana. Wait for the server to report ready:

```sh
for i in $(seq 30); do curl -fs http://127.0.0.1:4318/readyz && echo ready && break; sleep 2; done
```

It prints `ready` within a few seconds; if not, check
`docker compose -f deploy/docker-compose/ravel.yml logs ravel-server`. Host
metrics arrive about 20 seconds later. Query CPU load with PromQL, using the
demo bearer token:

<!-- ravel:run status=200; json:.status=success -->
```sh
curl -s -H "Authorization: Bearer demo-token" \
  'http://127.0.0.1:4318/api/v1/query?query=system_cpu_load_average_1m'
```

Success is `"status":"success"` with one series under `data.result`. An empty
`result` in the first 20 seconds is expected.

Count the Collector's log lines over the last hour. Logs answer PromQL through
the reserved `ravel_log_lines` metric name:

<!-- ravel:run status=200; json:.data.resultType=vector; nonempty:.data.result -->
```sh
curl -s -H "Authorization: Bearer demo-token" \
  --data-urlencode 'query=sum by (job) (count_over_time(ravel_log_lines[1h]))' \
  'http://127.0.0.1:4318/api/v1/query'
```

Read raw samples with SQL:

<!-- ravel:run status=200; nonempty:.data.rows -->
```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM samples LIMIT 5"}'
```

Open Grafana at <http://127.0.0.1:3000> (`admin` / `admin`). The Ravel
datasource and a host-metrics dashboard are already set up.

> [!WARNING]
> Every credential in this stack is a fixed development value, and every port
> binds `127.0.0.1` only. Do not expose it to a network.

Stop the stack:

```sh
docker compose -f deploy/docker-compose/ravel.yml down
```

The stored data stays in `rustfs-data/`, and the next `up` reuses it. To
delete it, run `rm -rf rustfs-data` after `down` (with `sudo` on Linux). That
cannot be undone.

The [getting started guide](docs/guides/getting-started.md) explains each
response and when an empty result is correct.

### Kill the server, keep the data

With the stack up, run [demo/kill-and-recover.sh](demo/kill-and-recover.sh).
It writes one sample, kills `ravel-server` with `SIGKILL`, starts a fresh
container, and reads the sample back by its commit token. It exits non-zero if
the sample is missing. The GIF above is a recording of it.

## Durability

Strict acknowledgement is the default. Ravel answers a write only after its
data object and commit record are in the bucket, and returns one commit token
per shard the write went through. A query without the token sees the write
once the catalog's listing picks up its commit record.

Buffered acknowledgement is opt-in per request. It answers before the flush,
for lower latency, and returns no commit token. A crash before the flush
loses the buffered window, and a flush that runs past its time budget is
abandoned and loses its acknowledged rows without any crash.

The [consistency model](docs/consistency-model.md) is the normative statement
of both modes, visibility, and crash behavior. The protocols behind it are
model-checked in TLA+ under stated bounds
([formal verification](docs/guides/formal-verification.md)) and exercised by
a deterministic fault-injection simulation.

## Is Ravel a fit

Ravel fits when you want metrics, logs, and traces on a bucket you already
run, with no stateful ingest layer and no local disk in the durability path.

Wait if:

- Your dashboards span months of high-cardinality metrics. Ravel keeps no
  downsampled rollups, so a wide query reads every raw hour it covers.
- You depend on LogQL, TraceQL, or the Jaeger UI. Ravel serves PromQL and
  SQL, and it has no Jaeger or Tempo API.
- You need write acknowledgement in single-digit milliseconds. Strict
  acknowledgement pays an object-store round trip.

Know these limits before you send data:

- Metrics must be cumulative. Ravel rejects delta `Sum` and `Histogram`
  points. Convert them in the Collector
  ([how](docs/guides/ingest.md#delta-temporality-metrics)).
- A structured log body (an array or a map) is stored as JSON text.
- Exemplars are kept from OTLP only. Profiles are not ingested.

> [!IMPORTANT]
> Ravel is pre-1.0. A data-format version change is one-way: once a new
> build writes the new version, an older build cannot read it. See
> [rolling a format bump](docs/guides/operations/maintenance.md#rolling-a-format-bump)
> before you upgrade a deployment that holds data you keep.

## What works

The published image is built with `--features sql,flight-sql,otap,agents,mcp`.
A source build has no default features: pass the ones you need to cargo. Some
surfaces also need a runtime flag.

<!-- BEGIN SUPPORT MATRIX -->

| Surface | Signals | Feature gate | In published image | Runtime switch |
|---|---|---|---|---|
| OTLP ingest, HTTP and gRPC | metrics, logs, traces | `none` | yes | none |
| Prometheus Remote Write 1.0 and 2.0 | metrics | `none` | yes | none |
| OTAP ingest, gRPC | metrics | `otap` | yes | `--otap` |
| PromQL HTTP API | metrics, logs | `none` | yes | none |
| SQL, `POST /api/v1/sql` | metrics, logs, traces, alerts, audit | `sql` | yes | none |
| Trace lookup, `GET /api/traces/<trace_id>` | traces | `sql` | yes | none |
| Flight SQL, gRPC | metrics, logs, traces, alerts, audit | `flight-sql` | yes | none |
| Parquet tables over SQL | Parquet files in a granted location | `sql` | yes | `--parquet-profiles` |
| ES&vert;QL, `POST /api/v1/esql` | logs | `esql` | no | none |
| Ravel Agents, `/api/v1/agents/` | durable agent state | `agents` | yes | `--agents` |
| MCP server, `POST /mcp` | metrics, logs, traces | `mcp` | yes | `--mcp` |

<!-- END SUPPORT MATRIX -->

"In published image" describes the image built from `main`; every such entry
is also in the 0.31.0 image the quickstart pins. Query surfaces serve in
`--mode all` and `--mode query`.

- SQL tables: `samples` (metrics, without native histograms, which PromQL
  serves), `logs`, `spans`, `alerts`, and `audit`.
- PromQL reads logs through the reserved `ravel_log_lines` and
  `ravel_log_bytes` names
  ([PromQL over logs](docs/guides/query.md#promql-over-logs)).
- Parquet tables read files in place from a location an operator grants the
  tenant ([Parquet table DDL](docs/guides/query.md#parquet-table-ddl)).

The [query guide](docs/guides/query.md) covers each surface, and the
[HTTP API reference](docs/reference/http-api.md) lists every route.

## How it works

![Ravel architecture: OTLP clients ingest through the gateway, the ingest router, and shard actors down to L0 segments and commit records, while Prometheus API consumers query through the PromQL evaluator, query workers, and catalog resolution. The object store sits in the middle as the single durable center, and every box above it is disposable.](docs/diagrams/architecture.svg)

An ingest shard builds an immutable columnar segment in memory, writes it to
the bucket, writes a commit record, and then answers the sender. Queries
resolve commit records into a snapshot through the catalog. Background
maintenance compacts segments and applies retention. Every process is
disposable: a replacement reads its state from the bucket. The
[architecture overview](docs/architecture.md#in-brief) explains the parts.

## Ravel Agents

Ravel Agents runs durable agents on the same bucket as your telemetry. An
agent is a named unit of state, such as one order or one conversation. Your
worker runs a handler for each message, and Ravel commits the new state, the
result, and the events in one conditional write. Any server or worker can
stop, and the next one continues from the bucket.

Start with the [Ravel Agents guide](docs/guides/agents.md), then the
[TypeScript SDK guide](docs/guides/agents-sdk.md). The
[operator guide](docs/guides/operations/agents.md) lists the limits to know
before production.

## MCP server

The MCP server lets an AI client, such as a coding assistant, query your
telemetry. The MCP server ships behind the `mcp` cargo feature and the
runtime `--mcp` flag, both off by default. The published image has the
feature, so there you pass only the flag, plus `--mcp-allowed-origins` when the
listener is reachable from outside localhost. All nine tools are served,
`ravel_capabilities`, `ravel_describe_data` and seven more, and every tool is
read-only. The [MCP server guide](docs/guides/mcp-server.md) shows how to
connect a client.

## Documentation

- [Getting started](docs/guides/getting-started.md), then the
  [ingest](docs/guides/ingest.md) and [query](docs/guides/query.md) guides.
- [Operations](docs/guides/operations.md) to run Ravel on your own bucket,
  including [container images](docs/guides/operations/deployment.md#container-images).
- The [documentation index](docs/README.md) routes every other task, from
  references to contributing ([CONTRIBUTING.md](CONTRIBUTING.md)).
- The [changelog](CHANGELOG.md) and the [AI policy](AI_POLICY.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
