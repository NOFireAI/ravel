# Ravel

[![Release](https://img.shields.io/github/v/release/NOFireAI/ravel)](https://github.com/NOFireAI/ravel/releases/latest)
[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/NOFireAI/ravel/graph/badge.svg)](https://codecov.io/gh/NOFireAI/ravel)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

**Ravel acknowledges a write when S3 has it.**

Ravel is a database for OpenTelemetry metrics, logs, and traces. Object
storage is its only durable component: Ravel has no write-ahead log, no
replicated ingest quorum, and no StatefulSet. You can kill any Ravel process
at any instant, and every strictly acknowledged write is still there.

![Ingesting one sample under strict acknowledgement, SIGKILLing the ravel-server container, replacing it with a fresh one, and reading the pre-kill sample back by its commit token.](docs/demo.gif)

Strict acknowledgement is the default. Ravel acknowledges a request only after
every batch its points contributed to has its data object durably stored and
its commit record created. The response carries one commit token per shard
those points flushed through.

Buffered acknowledgement is opt-in per request. It
acknowledges after admission and enqueue to a shard actor, and it returns no
commit token. It gives up the crash guarantee of strict acknowledgement for
lower write latency. A crash between that acknowledgement and the flush loses
the buffered window. A flush whose store calls exceed the flush lifetime
budget is abandoned, and it drops rows that are already acknowledged, with no
crash.

The [consistency model](docs/consistency-model.md) is normative for both
acknowledgement modes. [How Ravel is verified](#how-ravel-is-verified) lists
the checks behind the durability guarantee.

## The design trade

Ravel writes to the object store first. An ingest shard builds an immutable
columnar segment in memory, PUTs it, PUTs a commit record, and then answers
the exporter. Pass the commit token from the response back to a query, and
you read your own write with no listing race.

The common design buffers writes in a replicated ingest layer with local
disks and ships them to object storage later. To run that design, you run a
write-ahead log, PersistentVolumeClaims, replication factors, and rollout
ordering. With Ravel you operate none of that layer, and you pay object-store
latency on the write path.

## Is Ravel a fit

**What it is for.**

- Metrics, logs, and traces on any S3-compatible object store, with no
  stateful ingest layer to operate and no local disk in the durability path.
- A Prometheus-compatible query API, so existing Grafana dashboards work
  against it.
- Read-your-write on an object store. A strict acknowledgement returns a
  commit token, and a query that carries the token reads that write.
- Multi-tenancy with per-tenant server-side encryption using a key management
  service (SSE-KMS), legal hold, and admission limits.

**What it does not do.**

- No downsampled or pre-aggregated rollups. A wide-range metrics query reads
  every raw hour it covers.
- Traces are queryable over SQL only, as the `spans` table. Logs are queryable
  over SQL, as the `logs` table, and over PromQL, through the reserved
  `ravel_log_lines` and `ravel_log_bytes` metric names (see
  [PromQL over logs](docs/guides/query.md#promql-over-logs)). Neither surface
  gets you LogQL, TraceQL, a trace-by-ID endpoint, or a Jaeger or Tempo API.
- Profiles have a reserved object-key prefix only. No path ingests or queries
  them.
- Exemplars are stored from the OpenTelemetry Protocol (OTLP) only. Remote Write
  and OTAP decode exemplars and then discard them.
- Distributed read fan-out is off unless `--distributed-query` is given, and
  that flag requires `--fragment-key-file`, the dedicated fragment listener
  (`--fragment-listener` with its TLS files) and `--sql-ticket-key-file`. The
  PromQL lane and the SQL lane (on the Flight SQL service) are both in the
  published image.

**Who should wait.** Wait if one of these describes you:

- Your dashboards range over months of high-cardinality metrics. The missing
  downsampled storage costs you on every panel.
- Your logs workflow depends on the line-pipeline browsing style of LogQL, or
  your traces workflow depends on TraceQL or the Jaeger UI. Ravel has nothing
  to point them at. A Grafana Prometheus datasource can chart log volume and
  error rate against Ravel through PromQL, and it can filter the lines it
  counts by body text. It cannot browse the lines. To read log lines, use SQL.
- You need write acknowledgement in single-digit milliseconds. Strict
  acknowledgement pays an object-store round trip, and buffered
  acknowledgement gives up the crash guarantee.

**Before v1.0.** Ravel is pre-1.0, and until v1.0 ships it can break backward
compatibility. The persistent formats are versioned contracts. Before v1.0, a
version change retires the old version and does not keep it readable, and the
surfaces around the formats still move.

A version change of a bulk data-object format (RSEG, RLOG, RSPAN) is a
forward-only data migration that you cannot roll back:

- The reader admits one on-object version at a time.
- After data is written at a new version, a build that predates the change
  cannot read it.
- Objects left at the retired version are wiped or re-ingested.

The other persistent formats evolve without a one-way step. They are the
rebuildable fold outputs and the additive records with frozen field numbers.
The two-version reader window, which makes an upgrade reversible across one
version boundary, opens at v1.0. Until then, treat a format upgrade as
one-way. The [RSEG](docs/segment-format.md), [RLOG](docs/log-segment-format.md),
and [RSPAN](docs/span-segment-format.md) specs state the posture of each
format.

## What works

Ravel ingests OTLP natively and answers PromQL and SQL. Each surface below is
in the published container image or behind a named cargo feature, and the
matrix says which.

The matrix describes `main`. Its "In published image" column means that the
image built from `main` compiles the surface in. A surface that lands after
the tag that the quickstart pins is not yet in the quickstart image. Every
entry below is in the pinned `0.23.0` image. After the next feature lands,
`git log v0.23.0..main` tells you whether that is still true. `CHANGELOG.md`
records the release that each surface first shipped in.

<!-- BEGIN SUPPORT MATRIX -->

| Surface | Signals | Feature gate | In published image |
|---|---|---|---|
| OTLP ingest, HTTP and gRPC | metrics, logs, traces | `none` | yes |
| Prometheus Remote Write 1.0 and 2.0 ingest | metrics | `none` | yes |
| OTAP ingest, gRPC | metrics | `otap` | yes |
| PromQL HTTP API | metrics, logs as `ravel_log_lines` and `ravel_log_bytes` | `none` | yes |
| SQL over `POST /api/v1/sql` | metrics as `samples` (scalar samples only, never native histograms), logs as `logs`, traces as `spans`, alert history as `alerts`, audit records as `audit` | `sql` | yes |
| Flight SQL | the same five tables, with the same `samples` limit | `flight-sql` | yes |
| Parquet tables over both SQL surfaces | Parquet files read in place from an S3, GCS or Azure location granted to the tenant (`ravel-cli tenant parquet-grant add`, `ravel-server --parquet-profiles`); described below the matrix | `sql` | yes |

<!-- END SUPPORT MATRIX -->

The published `ravel-server` image is built with
`--features sql,flight-sql,otap`. No crate in the workspace declares a default
feature set, so a source build gets the same surfaces only when you pass the
same `--features` list to cargo.

- `POST /api/v1/sql` and Flight SQL answer as soon as the server is up.
- Flight SQL is a gRPC service on the gRPC listener. It runs ad-hoc statements
  and returns `unimplemented` for prepared statements.
- OTAP ingest is registered only when the process starts with `--otap`
  ([docs/otap-ingest.md](docs/otap-ingest.md)).
- SQL is the only way to query traces, alerts, and audit records.

A tenant can also query Parquet tables. These are Parquet files read in place,
in an S3, GCS or Azure location that an operator granted the tenant with
`ravel-cli tenant parquet-grant add`. The read goes through a credential
profile from the file that `ravel-server --parquet-profiles` names. A
statement reads only Parquet tables or one of the five tables above, never
both. A table reads only the file versions it was defined over. A caller with
the `ddl` capability, which is absent by default, creates a table with
`CREATE EXTERNAL TABLE ... STORED AS PARQUET LOCATION '...'` and removes it
with `DROP TABLE` (see the [query guide](docs/guides/query.md)). Such a table
is queried by name:

```text
SELECT URL, count(*) AS c FROM hits WHERE CounterID = 62 GROUP BY URL ORDER BY c DESC LIMIT 10
```

The `samples` table has no column that can hold a native histogram. Query
native histograms over PromQL. See the
[query guide](docs/guides/query.md#sql-over-samples-logs-and-spans).

Also live:

- `/api/v1/metadata` returns per-metric type, help, and unit for metrics whose
  ingest carried that metadata. OTLP metric names get the standard
  Prometheus-style unit and `_total` suffixes at ingest. See
  [metric metadata](docs/guides/ingest.md#metric-metadata-and-otlp-name-suffixing).
- Exemplars that link a metric sample to its trace
  ([correlation guide](docs/guides/correlation.md)).
- Alert rules whose every transition is written to object storage as immutable
  data, and readable back through the `alerts` SQL table
  ([alerting guide](docs/guides/alerting.md)).
- An analytics endpoint for change point detection and summary statistics.
- Compaction, age-based retention, and garbage collection for metrics, logs,
  and spans, with audit compacted and retained on its own separate schedule.
  Alert transitions have no maintenance path yet.
- A Kubernetes operator with a `RavelCluster` custom resource. The
  [Kubernetes guide](docs/guides/kubernetes.md) runs the ingest and query
  round trip on a local `kind` cluster.
- Per-tenant typed attribute columns on the `logs` SQL table, so typed
  comparisons and aggregates need no `CAST` over the stringified `attrs` map
  ([query guide](docs/guides/query.md#declaring-typed-attribute-columns)).
- A clustering key and a bloom scope in a tenant's config record, which the
  tenant's RLOG objects carry
  ([ingest guide](docs/guides/ingest.md#setting-a-tenants-clustering-key-and-bloom-scope)).

The [query engine spec](docs/query-engine.md) holds the PromQL conformance
table, and the [SQL conformance table](docs/sql-conformance.md) covers SQL.
Both tables are generated. Agreement with Prometheus is a separate row of the
PromQL table, and that row reads `not measured in this run` in the committed
table.

Two ingest limits apply before you point a sender at Ravel:

- Metrics must be cumulative. Ravel rejects a delta-temporality `Sum` or
  `Histogram`, and the OTLP response says so. Convert in the collector with
  the `deltatocumulative` processor. The
  [ingest guide](docs/guides/ingest.md#delta-temporality-metrics) has the
  configuration. A sender that you control can usually export cumulative
  directly.
- A structured log body (an array or a map) is stored as canonical JSON text,
  not as a nested value. It reads back as a JSON string, so a query that wants
  a field inside it parses that string.

## Quickstart

One command starts the whole stack from published images: RustFS for object
storage, `ravel-server`, an OpenTelemetry Collector that feeds it your host's own
metrics, and Grafana with a provisioned Ravel datasource. You need no Rust
toolchain and no compile step.

```sh
docker compose -f deploy/docker-compose/ravel.yml up -d
```

Open Grafana at <http://127.0.0.1:3000> (`admin` / `admin`). The Ravel datasource
is already wired up. The first dashboard shows your machine's metrics after a few
scrape intervals.

Query the data back over the Prometheus-compatible API. Every query needs the
demo bearer token, as a real deployment needs a real one:

<!-- ravel:run status=200; json:.status=success -->
```sh
curl -s -H "Authorization: Bearer demo-token" \
  'http://127.0.0.1:4318/api/v1/query?query=system_cpu_load_average_1m'
```

Logs answer the same PromQL API, through the reserved `ravel_log_lines` and
`ravel_log_bytes` metric names:

<!-- ravel:run status=200; json:.data.resultType=vector; nonempty:.data.result -->
```sh
curl -s -H "Authorization: Bearer demo-token" \
  --data-urlencode 'query=sum by (job) (count_over_time(ravel_log_lines[5m]))' \
  'http://127.0.0.1:4318/api/v1/query'
```

The published image carries the `sql` feature, so `POST /api/v1/sql` answers
by default:

<!-- ravel:run status=200; nonempty:.data.rows -->
```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM samples LIMIT 5"}'
```

To watch the read-your-write path, run
[demo/walkthrough.sh](demo/walkthrough.sh) while the stack is up. It ingests one
export, captures its commit token, and reads that write back.

Stop the stack:

```sh
docker compose -f deploy/docker-compose/ravel.yml down
```

The [getting started guide](docs/guides/getting-started.md) walks this same path
with what each response means, how long to wait for data, and what an empty
result looks like when it is expected.

### Kill the server, keep the data

The GIF above is a recording of [demo/kill-and-recover.sh](demo/kill-and-recover.sh),
which demonstrates the durability claim against the running stack:

```sh
demo/kill-and-recover.sh
```

1. It ingests one export under strict acknowledgement and captures the
   `x-ravel-commit-token` from the response.
2. It `SIGKILL`s the `ravel-server` container, so the process cannot flush
   anything on its way out.
3. It deletes that container and starts a fresh one with an empty filesystem.
4. It reads the pre-kill sample back with `min_commit_token`.

Nothing crosses the kill except what is in RustFS. The script asserts every
step and exits non-zero if the sample is absent or the token comes back
unsatisfiable. Run it against your own stack to check a change to the
quickstart; no CI lane runs it.

### Beyond the demo stack

Every credential in [deploy/docker-compose/ravel.yml](deploy/docker-compose/ravel.yml)
is a fixed development value, and every published host port binds loopback
(`127.0.0.1`) only. None of these values are for a deployment that a network
can reach.

| To do this | Read |
|---|---|
| Point `--store s3` at any S3-compatible store, or use `--s3-auth instance-role` on EC2 in place of static keys | [Choosing a credential source](docs/guides/operations/configuration.md#choosing-a-credential-source) |
| Size or switch off the read cache that sits in front of object storage | [Caching guide](docs/guides/caching.md) |
| Run your own build with `make demo`, which does not build the `sql` feature | [Development guide](docs/internal/development.md) |

## How it fits together

![Ravel architecture: OTLP clients ingest through the gateway, the ingest router, and shard actors down to L0 segments and commit records, while Prometheus API consumers query through the PromQL evaluator, query workers, and catalog resolution. The object store sits in the middle as the single durable center, and every box above it is disposable.](docs/diagrams/architecture.svg)

A write is durable once its commit record is on the object store. A reader sees
it once the catalog resolves that commit into a snapshot. The
[ingest guide](docs/guides/ingest.md) covers the write path, the
[query guide](docs/guides/query.md) covers the read path, and
[architecture](docs/architecture.md) is the one-page overview.

All query endpoints live under `/api/v1` on the HTTP listener, which binds
`127.0.0.1:4318` by default. They need `Authorization: Bearer <token>`, the same
as ingest. The [HTTP API reference](docs/reference/http-api.md) lists every
route. One of them, `POST /api/v1/admin/fold`, triggers a
[catalog fold on demand](docs/architecture.md#on-demand-catalog-fold).

## Agents (MCP)

The MCP agent surface is documented in
[docs/guides/agents.md](docs/guides/agents.md). It ships behind the `mcp`
cargo feature (`cargo build -p ravel-server --features mcp`) and the
runtime `--mcp` flag, both off by default: build with the feature, then pass
`--mcp` (and `--mcp-allowed-origins`, required whenever the MCP listener is
reachable from outside localhost) to mount `POST /mcp` on the query router.
One tool is served today, `ravel_capabilities`; the other eight are
catalogued and refuse calls until their bodies land.

## Container images

`ravel-server`, `ravel-operator`, and `ravel-ingest-router` publish to the
GitHub Container Registry on every `vX.Y.Z` release tag, built from the root
`Dockerfile`. Both `linux/amd64` and `linux/arm64` are published. Each published
object is an OCI image index that carries an SBOM and full build provenance. The
quickstart pins `ghcr.io/nofireai/ravel-server:0.23.0`. Override it with
`RAVEL_IMAGE`.

```sh
docker pull ghcr.io/nofireai/ravel-server:latest
docker pull ghcr.io/nofireai/ravel-operator:latest
docker pull ghcr.io/nofireai/ravel-ingest-router:latest
```

`X.Y.Z` is write-once. A new patch release supersedes a bad release, and the
tag is never re-pushed. `latest`, `X`, and `X.Y` move with the newest matching
release. When you need an immutable reference, pin by digest
(`ghcr.io/nofireai/ravel-server@sha256:...`).

### Verifying signatures

Every published index digest is signed with
[cosign](https://github.com/sigstore/cosign) in keyless mode. The signing
certificate binds the signature to the release workflow's identity, so you can
verify a pull without a pre-shared key. Releases are cut from this repository,
so that is the identity in the certificate:

```sh
cosign verify \
  --certificate-identity 'https://github.com/NOFireAI/ravel/.github/workflows/publish-images.yml@refs/tags/v0.23.0' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  ghcr.io/nofireai/ravel-server:0.23.0
```

Replace `v0.23.0` and `0.23.0` with the release you are verifying. The tag ref in
`--certificate-identity` must be the exact tag that produced the image.

## How Ravel is verified

- TLC checked five finite TLA+ models of the commit, catalog, lifecycle,
  resharding, and maintenance protocols, over one shared object-store model,
  under stated bounds and assumptions. Negative controls show that each
  invariant can fail. Every checked property traces to a Rust symbol and, for
  all but five recorded rows, a named test. The
  [formal verification guide](docs/guides/formal-verification.md) has the
  bounds, the assumptions, the lanes, and the results.
- A deterministic simulation harness drives the full ingest, fold, compact,
  sweep, and query cycle under injected faults. It checks read-your-write,
  strict-ack durability, compaction equivalence, record-count conservation, and
  orphan-free sweeps every cycle. Any violation prints its master seed and a
  one-command replay. A nightly job sweeps 200 seeds.
- The PromQL evaluator is differentially tested against a pinned real Prometheus
  binary.
- The compiler forbids `unsafe` across the workspace.
- Property tests cover every codec and parser. Fuzz targets run on the segment
  and span formats.
- A fault-injection store fails operations by kind, key, and occurrence, and the
  failure-path tests assert its counters.
- Tests assert the crash matrix of the
  [consistency model](docs/consistency-model.md).

## Repository layout

- `crates/` holds the library crates, from the segment formats (RSEG for
  metrics, RLOG for logs, RSPAN for spans) to the query engine and SQL. See
  the [crate map](docs/architecture.md#crate-map).
- `services/` holds `ravel-server` (the `all`, `gateway`, `query`, and
  `maintain` modes in one binary), `ravel-cli` (a segment, commit, and catalog
  inspector, and the Parquet bulk loader), `ravel-ingest-router` (the optional
  tenant-affinity front door), and the Kubernetes operator.
- `docs/` holds the guides, the specs, the decision records, and the diagrams.
- `deploy/` holds the quickstart compose stack, the Collector and Grafana
  provisioning, and the Kubernetes manifests.

## Documentation

- [Getting started](docs/guides/getting-started.md) is the recommended path from
  nothing to a first query.
- The [documentation index](docs/README.md) lists every guide, spec, and decision
  record.
- [Architecture](docs/architecture.md) is the mental model.
- [Contributing](CONTRIBUTING.md), the [changelog](CHANGELOG.md), and the
  [AI policy](AI_POLICY.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
