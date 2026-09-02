# Ravel

[![Release](https://img.shields.io/github/v/release/NOFireAI/ravel)](https://github.com/NOFireAI/ravel/releases/latest)
[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/NOFireAI/ravel/graph/badge.svg)](https://codecov.io/gh/NOFireAI/ravel)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

**The write is acknowledged when S3 has it. Not before.**

Ravel is a database for OpenTelemetry metrics, logs, and traces where object
storage is the only durable component. No write-ahead log. No replicated ingest
quorum. No StatefulSet. Kill any Ravel process at any instant, and every
strictly acknowledged write is still there.

![Ingesting one sample under strict acknowledgement, SIGKILLing the ravel-server container, replacing it with a fresh one, and reading the pre-kill sample back by its commit token.](docs/demo.gif)

Strict acknowledgement is the default. A request is acknowledged only after
every batch its points contributed to has its data object durably stored and
its commit record created, and the response carries one commit token per shard
those points flushed through. After a strict acknowledgement, no crash of any
Ravel process loses that data.

Buffered acknowledgement is the opt-out, set per tenant or per request. It
acknowledges after admission and enqueue to a shard actor, and it returns no
commit token: it trades the guarantee above for write latency. A crash between
that acknowledgement and the flush loses the buffered window, bounded by the
maximum flush delay. A clean shutdown drains the window, so the loss is
specific to a crash. The [consistency model](docs/consistency-model.md) is
normative for both modes.

## Why it is built this way

Every self-hosted observability stack ends up storing data on object storage.
Almost none of them start there. Mimir, Loki, Tempo, and Thanos buffer writes in
a replicated ingest layer with local disks, then ship to object storage later.
That is why running them means running a write-ahead log,
PersistentVolumeClaims, replication factors, and rollout ordering.

Ravel makes the object store the first stop. An ingest shard builds an immutable
columnar segment in memory, PUTs it, PUTs a commit record, and only then answers
the exporter. The response carries a commit token. Pass that token back to a
query and you read your own write, with no listing race.

The trade is explicit. You pay object-store latency on the write path, and you
delete the entire replicated stateful layer. Ravel's job is to make that a good
trade.

## Is Ravel a fit

**What it is for.** Metrics, logs, and traces on any S3-compatible object store,
with no stateful ingest layer to operate and no local disk in the durability
path. A Prometheus-compatible query API, so existing Grafana dashboards work
against it. Read-your-write on an object store: a strict acknowledgement hands
back a commit token, and a query that carries the token back reads exactly that
write. Multi-tenancy with per-tenant server-side encryption using a key
management service (SSE-KMS), legal hold, and admission limits.

**What it does not do.**

- No downsampled or pre-aggregated rollups. A wide-range metrics query reads
  every raw hour it covers.
- Logs and traces are queryable over SQL only, as the `logs` and `spans` tables.
  There is no LogQL, no PromQL surface for logs, no TraceQL, no trace-by-ID
  endpoint, and no Jaeger or Tempo API.
- Alert rule transitions and audit records are written to object storage, and no
  shipped query surface can read them back.
- Flight SQL and OTAP (OpenTelemetry Arrow) ingest exist in the source and no
  published image builds them. Both need a source build with their cargo
  feature turned on.
- Profiles are a reserved object-key prefix only. No ingest, no query.
- Exemplars are stored from the OpenTelemetry Protocol (OTLP) only. Remote Write
  and OTAP decode exemplars and then discard them.
- Distributed read fan-out is off unless `--distributed-query` and
  `--fragment-key-file` are both given. The PromQL lane is in the published
  image; the SQL lane exists only in a `flight-sql` build, which no published
  image is.

**Who should wait.** If your dashboards range over months of high-cardinality
metrics, the missing downsampled storage will cost you on every panel. If your
logs or traces workflow depends on LogQL, TraceQL, or the Jaeger UI, there is
nothing here to point them at. If you need alert history to be queryable, it is
not. If you need write acknowledgement in single-digit milliseconds, strict mode
pays an object-store round trip and buffered mode gives up the crash guarantee
above. Ravel is pre-1.0: the persistent formats are versioned contracts, and the
surfaces around them still move.

## What works today

Ravel ingests OTLP natively and answers PromQL and SQL. Everything below is
either in the published container image or behind a named cargo feature, and the
matrix says which.

<!-- BEGIN SUPPORT MATRIX -->

| Surface | Signals | Feature gate | In published image |
|---|---|---|---|
| OTLP ingest, HTTP and gRPC | metrics, logs, traces | `none` | yes |
| Prometheus Remote Write 1.0 and 2.0 ingest | metrics | `none` | yes |
| OTAP ingest, gRPC | metrics | `otap` | no |
| PromQL HTTP API | metrics | `none` | yes |
| SQL over `POST /api/v1/sql` | metrics as `samples`, logs as `logs`, traces as `spans` | `sql` | yes |
| Flight SQL | the same three tables | `flight-sql` | no |

<!-- END SUPPORT MATRIX -->

No crate in the workspace declares a default feature set, so `sql`,
`flight-sql`, and `otap` are off in any build that does not ask for them. The
published `ravel-server` image is built with `--features sql`, so
`POST /api/v1/sql` answers there. OTAP ingest needs the `otap` feature at build
time and the `--otap` flag at startup, and Flight SQL needs the `flight-sql`
feature; no published image carries either. Flight SQL runs ad-hoc statements,
and prepared statements return `unimplemented`.

Exactly three SQL tables are registered: `samples`, `logs`, and `spans`. SQL is
the only way to query logs and traces.

Also live:

- A Prometheus-compatible HTTP API, so existing Grafana dashboards work.
  `/api/v1/metadata` returns real per-metric type, help, and unit for metrics
  whose ingest carried that metadata, and OTLP metric names get the standard
  Prometheus-style unit and `_total` suffixes at ingest (a monotonic `foo` with
  `unit: "By"` lands as `foo_bytes_total`), so the same metric matches whether
  it arrives over OTLP or through a collector's Prometheus exporter.
- Exemplars that link a metric sample to its trace.
- Alert rules whose every transition is written to object storage as immutable
  data. Reading those records back needs a query surface Ravel does not
  ship.
- An analytics endpoint for change point detection and summary statistics.
- Compaction, age-based retention, and garbage collection across all signals.
- A Kubernetes operator with a `RavelCluster` custom resource.
- Per-tenant typed attribute columns on the `logs` SQL table, so typed
  comparisons and aggregates need no `CAST` over the stringified `attrs` map.
  See the [query guide](docs/guides/query.md).

The [SQL conformance table](docs/sql-conformance.md) and the PromQL conformance
table in the [query engine spec](docs/query-engine.md) classify every construct
as supported, intentionally rejected, or unclassified. Both are generated, not
written: the PromQL table from a differential test against a real Prometheus
binary, and the SQL table from the conformance suite's recorded verdict for
each construct. The gaps are measured rather than claimed.

## Quickstart

One command starts the whole stack from published images: MinIO for object
storage, `ravel-server`, an OpenTelemetry Collector that feeds it your host's own
metrics, and Grafana with a provisioned Ravel datasource. No Rust toolchain, no
compile.

```sh
docker compose -f deploy/docker-compose/ravel.yml up -d
```

Open Grafana at <http://127.0.0.1:3000> (`admin` / `admin`). The Ravel datasource
is already wired up. The first dashboard shows your machine's metrics after a few
scrape intervals.

Query the data back over the Prometheus-compatible API. Every query needs the
demo bearer token, exactly as a real deployment needs a real one:

<!-- ravel:run status=200; json:.status=success -->
```sh
curl -s -H "Authorization: Bearer demo-token" \
  'http://127.0.0.1:4318/api/v1/query?query=system_cpu_load_average_1m'
```

The published image is built with `--features sql`, so `POST /api/v1/sql` answers
by default. The registered tables are `samples`, `logs`, and `spans`:

<!-- ravel:run status=200; nonempty:.data.rows -->
```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM samples LIMIT 5"}'
```

To watch the read-your-write path directly, run
[demo/walkthrough.sh](demo/walkthrough.sh) while the stack is up. It ingests one
export, captures its commit token, and reads that exact write back.

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

It ingests one export under strict acknowledgement and captures the
`x-ravel-commit-token` from the response. It then `SIGKILL`s the `ravel-server`
container, so the process cannot flush anything on its way out. It deletes that
container, starts a fresh one with an empty filesystem, and reads the pre-kill
sample back with `min_commit_token`. Nothing crosses the kill except what is in
MinIO.

The script asserts every step and exits non-zero if the sample is absent or the
token comes back unsatisfiable. A passing run is evidence, not a demonstration
you have to watch closely. CI runs it against a live stack on every change to the
quickstart.

### Security of the demo stack

Every credential in [deploy/docker-compose/ravel.yml](deploy/docker-compose/ravel.yml)
is a fixed development value: the `demo-token` bearer token and the MinIO
`ravel` / `ravel-dev-secret` pair. Every published host port binds loopback
(`127.0.0.1`) only, so the checked-in token never fronts an ingest endpoint on
your network. None of these values are for a deployment that a network can reach.

### Beyond the demo stack

A real deployment points `--store s3` at any S3-compatible store, exactly as the
quickstart points it at MinIO, and on EC2 it can drop static keys entirely with
`--s3-auth instance-role`. The [operations guide](docs/guides/operations.md)
documents every storage flag, including temporary session tokens and a rotating
credentials file.

Every query byte comes from object storage, so a read cache sits in front of it.
The RAM tier is on by default, bounded by `--cache-max-bytes` and switched off
with `--disable-cache`; `--cache-dir <path>` adds a second, disposable disk
tier. Neither tier holds durable state. See the
[caching guide](docs/guides/caching.md).

Changing Ravel's code? `make demo` builds from source and runs the same round
trip. It does not build the `sql` feature, so `POST /api/v1/sql` is unavailable
on that path while PromQL and ingest behave the same. The
[development guide](docs/internal/development.md) covers the source workflow.

## How it fits together

![Ravel architecture: OTLP clients ingest through the gateway, the ingest router, and shard actors down to L0 segments and commit records, while Prometheus API consumers query through the PromQL evaluator, query workers, and catalog resolution. The object store sits in the middle as the single durable center, and every box above it is disposable.](docs/diagrams/architecture.svg)

A write is durable once its commit record is on the object store. A reader sees
it once the catalog resolves that commit into a snapshot. The
[ingest guide](docs/guides/ingest.md) covers the write path, the
[query guide](docs/guides/query.md) covers the read path, and
[architecture](docs/architecture.md) is the one-page overview.

All query endpoints live under `/api/v1` on the HTTP listener, which binds
`127.0.0.1:4318` by default. They need `Authorization: Bearer <token>`, the same
as ingest.

One maintenance route sits alongside them: `POST /api/v1/admin/fold` triggers a
catalog fold for the authenticated tenant and one named signal, instead of
waiting for the background fold's next tick. It takes the same bearer token the
query routes take, and its response says which of four things happened: a
snapshot was `published`, `nothing_eligible` was found to fold, a concurrent fold
won the `HEAD` compare-and-swap (`lost_cas`), or the call was `throttled`. Right
after a load the honest answer is `nothing_eligible`: an ingest hour is not
foldable until the sealing window behind it has elapsed. See
[architecture](docs/architecture.md#on-demand-catalog-fold).

## Kubernetes

The operator runs the same ingest and query round trip on a real cluster. This
needs `docker`, `kind`, and `kubectl`:

```sh
scripts/kind-up.sh     # cluster, images, fake S3, operator, RavelCluster
scripts/kind-demo.sh   # ingest through gateway mode, query through query mode
scripts/kind-down.sh
```

See the [Kubernetes guide](docs/guides/kubernetes.md).

## Container images

`ravel-server`, `ravel-operator`, and `ravel-ingest-router` publish to the
GitHub Container Registry on every `vX.Y.Z` release tag, built from the root
`Dockerfile`. Both `linux/amd64` and `linux/arm64` are published. Each published
object is an OCI image index that carries an SBOM and full build provenance. The
quickstart pins `ghcr.io/nofireai/ravel-server:0.11.0`. Override it with
`RAVEL_IMAGE`.

```sh
docker pull ghcr.io/nofireai/ravel-server:latest
docker pull ghcr.io/nofireai/ravel-operator:latest
docker pull ghcr.io/nofireai/ravel-ingest-router:latest
```

`X.Y.Z` is write-once. A bad release is superseded by a new patch release, never
by re-pushing the tag. `latest`, `X`, and `X.Y` move with the newest matching
release. Pin by digest (`ghcr.io/nofireai/ravel-server@sha256:...`) when you need
an immutable reference.

### Verifying signatures

Every published index digest is signed with
[cosign](https://github.com/sigstore/cosign) in keyless mode. The signing
certificate binds the signature to the release workflow's identity, so you can
verify a pull without a pre-shared key. Releases are cut from this repository,
so that is the identity in the certificate:

```sh
cosign verify \
  --certificate-identity 'https://github.com/NOFireAI/ravel/.github/workflows/publish-images.yml@refs/tags/v0.9.0' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  ghcr.io/nofireai/ravel-server:0.9.0
```

Replace `v0.9.0` and `0.9.0` with the release you are verifying. The tag ref in
`--certificate-identity` must be the exact tag that produced the image.

## How Ravel is verified

Durability claims are cheap to write and hard to keep. These are the checks that
hold Ravel to them:

- A deterministic simulation harness drives the full ingest, fold, compact,
  sweep, and query cycle under injected faults. It checks read-your-write,
  strict-ack durability, compaction equivalence, record-count conservation, and
  orphan-free sweeps every cycle. Any violation prints its master seed and a
  one-command replay. A nightly job sweeps 200 seeds.
- The PromQL evaluator is differentially tested against a pinned real Prometheus
  binary, and the per-construct result is published as a conformance table.
- `unsafe` is forbidden workspace-wide, at the compiler rather than by review.
- Property tests cover every codec and parser. Fuzz targets run on the segment
  and span formats.
- A fault-injection store fails operations by kind, key, and occurrence, and the
  failure-path tests assert its counters.
- The [consistency model](docs/consistency-model.md) is normative, and its crash
  matrix is test-asserted.

## Repository layout

- `crates/` holds the types, the object store, the segment formats (RSEG for
  metrics, RLOG for logs, RSPAN for spans), the commit protocol, the catalog, the
  decoders, the ingest actors, PromQL, the query engine, and DataFusion-backed
  SQL.
- `services/` holds `ravel-server` (gateway, ingest, query, and maintain modes in
  one binary), `ravel-cli` (a segment, commit, and catalog inspector, and the
  Parquet bulk loader), and the Kubernetes operator.
- `docs/` holds the guides, the specs, the decision records, and the diagrams.
- `deploy/` holds the quickstart compose stack, the Collector and Grafana
  provisioning, and the Kubernetes manifests.

## Documentation

- [Getting started](docs/guides/getting-started.md) is the recommended path from
  nothing to a first query.
- The [documentation index](docs/README.md) lists every guide, spec, and decision
  record.
- [Architecture](docs/architecture.md) is the mental model, and the
  [consistency model](docs/consistency-model.md) is normative for
  acknowledgement, visibility, and crash behavior.
- Formats: [RSEG](docs/segment-format.md) for metrics,
  [RLOG](docs/log-segment-format.md) for logs, and
  [RSPAN](docs/span-segment-format.md) for spans.
- [Contributing](CONTRIBUTING.md), the [changelog](CHANGELOG.md), and the
  [AI policy](AI_POLICY.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
