# Ravel

[![Release](https://img.shields.io/github/v/release/NOFireAI/ravel)](https://github.com/NOFireAI/ravel/releases/latest)
[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/NOFireAI/ravel/graph/badge.svg)](https://codecov.io/gh/NOFireAI/ravel)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

**The write is acknowledged when S3 has it. Not before.**

Ravel is an OpenTelemetry-native database for metrics, logs, and traces where
object storage is the only durable component. No write-ahead log. No ingester
quorum. No StatefulSet. Kill any Ravel process at any instant, and every
acknowledged write is still there.

![Ingesting one sample under strict acknowledgement, SIGKILLing the ravel-server container, replacing it with a fresh one, and reading the pre-kill sample back by its commit token.](docs/demo.gif)

## Why it is built this way

Every self-hosted observability stack ends up storing data on object storage.
Almost none of them start there. Mimir, Loki, Tempo, and Thanos buffer writes in
a replicated ingest tier with local disks, then ship to object storage later.
That is why running them means running a write-ahead log, PersistentVolumeClaims,
replication factors, and rollout ordering.

Ravel makes the object store the first stop. An ingest shard builds an immutable
columnar segment in memory, PUTs it, PUTs a commit record, and only then answers
the exporter. The response carries a commit token. Pass that token back to a
query and you read your own write, with no listing race.

The trade is explicit. You pay object-store latency on the write path, and you
delete the entire stateful tier. Ravel's job is to make that a good trade.

## What works today

| Signal | Ingest | Query |
|---|---|---|
| Metrics | OTLP (HTTP, gRPC), Prometheus Remote Write 1.0 and 2.0 | PromQL, SQL (`samples`), Flight SQL |
| Logs | OTLP (HTTP, gRPC) | SQL (`logs`), Flight SQL |
| Traces | OTLP (HTTP, gRPC) | SQL (`spans`) |

Also live:

- A Prometheus-compatible HTTP API, so existing Grafana dashboards work.
  `/api/v1/metadata` returns the real type, help text, and unit for each
  metric. At ingest, OTLP metric names get the standard Prometheus unit and
  `_total` suffixes. A monotonic `foo` with `unit: "By"` lands as
  `foo_bytes_total`. The same metric matches whether it arrives over OTLP or
  through a collector's Prometheus exporter.
- Exemplars that link a metric sample to its trace.
- Alerting that records every rule transition as an immutable data record on
  object storage.
- An analytics endpoint for change point detection and summary statistics.
- Compaction, age-based retention, and garbage collection across all signals.
- Multi-tenancy with per-tenant SSE-KMS encryption, legal hold, and admission
  limits.
- A Kubernetes operator with a `RavelCluster` custom resource.
- Distributed read fan-out and cross-cluster federation, off by default and
  byte-identical to local execution.
- OTAP (OpenTelemetry Arrow) ingest, behind a cargo feature.
- Per-tenant typed attribute columns on the `logs` SQL table. An operator
  declares an attribute key with `ravel-cli typed-attr-column set acme
  http.duration_ms:i64`, or with the `--typed-attr-column` server flags. The
  key becomes a native `Int64`, `Boolean`, or `Binary` column, or a
  `Dictionary(Int32, Utf8)` column for a declared string. Typed comparisons
  and aggregates then need no `CAST` over the stringified `attrs` map. A
  declared string column keeps its dictionary encoding through Flight SQL and
  Arrow IPC. JSON row values are unchanged, but the JSON envelope reports the
  dictionary type.

Not there yet:

- No downsampled tier. A wide-range query reads every raw hour.
- Flight SQL runs ad-hoc statements. Prepared statements return `unimplemented`.

The [SQL conformance table](docs/sql-conformance.md) and the PromQL conformance
table in the [query engine spec](docs/query-engine.md) classify every construct
as supported, intentionally rejected, or unclassified. The PromQL table comes
from a differential test against a real Prometheus binary. The SQL table comes
from a differential test against an independent reference implementation. The
gaps are measured, not claimed.

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

The [getting started guide](docs/guides/getting-started.md) walks the same path
with expected output.

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

### The from-source path

`make demo` builds Ravel from source and runs the same round trip. It does not
build the `sql` feature, so `POST /api/v1/sql` is unavailable there. PromQL,
ingest, and the rest work on both paths. Use `make demo` when you are changing
Ravel's code. See the [development guide](docs/guides/development.md).

### Security of the demo stack

Every credential in [deploy/docker-compose/ravel.yml](deploy/docker-compose/ravel.yml)
is a fixed development value: the `demo-token` bearer token and the MinIO
`ravel` / `ravel-dev-secret` pair. Every published host port binds loopback
(`127.0.0.1`) only, so the checked-in token never fronts an ingest endpoint on
your network. None of these values are for a deployment that a network can reach.

### Storage beyond the demo stack

A real deployment points `--store s3` at any S3-compatible store with static
keys, exactly as the quickstart points it at MinIO. On EC2, you can drop the
static keys. Attach an IAM role to the instance and start with

    ravel-server --store s3 --s3-bucket my-bucket --s3-region us-east-1 --s3-auth instance-role

Ravel fetches credentials from the instance metadata service at startup. It
refreshes them before they expire, so the instance stores nothing long-lived.
The [operations guide](docs/guides/operations.md) documents every storage flag.
It also covers temporary session tokens and a rotating credentials file for
non-EC2 deployments.

### Read cache

Every query byte comes from object storage, so Ravel puts a read cache in front
of it. The RAM tier is always on. `--cache-max-bytes` bounds it, and
`--disable-cache` turns it off. Adding `--cache-dir <path>` attaches a second,
local-disk tier at that directory. A RAM eviction is then served from disk
instead of re-paying the S3 round trip:

    ravel-server --store s3 --s3-bucket my-bucket --cache-dir /var/cache/ravel

The disk tier is opt-in and disposable. With no `--cache-dir`, the cache is
RAM-only. A missing, full, or corrupt cache directory degrades to a store read,
never a query error.

Ravel does not encrypt the bytes it writes to the cache directory, even with
SSE-KMS configured for object storage. SSE-KMS protects object bytes at rest in
the store, not the local cache. If you need bytes-at-rest encryption for the
cache directory, provide it at the filesystem or volume layer.

`/metrics` splits each cache's `ravel_cache_*` counters by a
`tier="ram"`/`tier="disk"` label once a disk tier is configured. See the
[caching guide](docs/guides/caching.md).

## How it fits together

![architecture](docs/diagrams/architecture.svg)

A write is durable once its commit record is on the object store. A reader sees
it once the catalog resolves that commit into a snapshot. The
[ingest guide](docs/guides/ingest.md) covers the write path and the
[query guide](docs/guides/query.md) covers the read path. The
[consistency model](docs/consistency-model.md) is normative for what
acknowledgement, visibility, and crash recovery mean.

![ingest and commit sequence](docs/diagrams/ingest-commit-sequence.svg)

All query endpoints live under `/api/v1` on the HTTP listener, which binds
`127.0.0.1:4318` by default. They need `Authorization: Bearer <token>`, the same
as ingest. The [query guide](docs/guides/query.md) and the
[distributed query guide](docs/guides/distributed-query.md) cover PromQL, SQL,
Flight SQL, exemplars, and analytics. The
[traces guide](docs/guides/traces.md) covers querying spans over the `spans`
SQL table.

One maintenance route sits alongside them. `POST /api/v1/admin/fold` triggers
a catalog fold for the authenticated tenant and one named signal, instead of
waiting for the background fold's next tick. It takes the same bearer token the
query routes take. Its response reports which of three things happened. A
snapshot was `published`, `nothing_eligible` was found to fold, or a concurrent
fold won the `HEAD` compare-and-swap (`lost_cas`). Right after a load, the
honest answer is `nothing_eligible`. An ingest hour is not foldable until the
sealing window behind it has elapsed. See
[architecture](docs/architecture.md#on-demand-catalog-fold).

## Kubernetes

The operator runs the same ingest and query round trip on a real cluster. This
needs `docker`, `kind`, and `kubectl`:

```sh
scripts/kind-up.sh     # cluster, images, fake S3, operator, RavelCluster
scripts/kind-demo.sh   # ingest via the gateway, query via the query tier
scripts/kind-down.sh
```

See the [Kubernetes guide](docs/guides/kubernetes.md).

## Container images

`ravel-server`, `ravel-operator`, and `ravel-ingest-router` publish to the
GitHub Container Registry on every `vX.Y.Z` release tag, built from the root
`Dockerfile`. The release publishes both `linux/amd64` and `linux/arm64`.
Each published object is an OCI image index that carries an SBOM and full build
provenance. The quickstart pins `ghcr.io/nofireai/ravel-server:0.11.0`. Override
it with `RAVEL_IMAGE`.

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
- Ravel differentially tests the PromQL evaluator against a pinned real
  Prometheus binary. It publishes the per-construct result as a conformance
  table.
- The workspace denies `unsafe` everywhere, at the compiler and not by review.
- Property tests cover every codec and parser. Fuzz targets run on the segment
  and span formats.
- A fault-injection store fails operations by kind, key, and occurrence. The
  failure-path tests assert its counters.
- The [consistency model](docs/consistency-model.md) is normative, and a test
  asserts its crash matrix.

## Where things live

- `crates/` contains types, the object store, the segment formats (RSEG for
  metrics, RLOG for logs, RSPAN for spans), the commit protocol, the catalog,
  OTLP, OTAP and Remote Write decode, ingest actors, PromQL, the query engine,
  and DataFusion-backed SQL.
- `services/` contains `ravel-server` (gateway, ingest, query, and maintain modes
  in one binary), `ravel-cli` (a segment, commit, and catalog inspector, and the
  Parquet bulk loader), and the Kubernetes operator.
- `docs/` contains specs, decision records, diagrams, and guides.
- `deploy/` contains the quickstart compose stack, the Collector and Grafana
  provisioning, and Kubernetes manifests.

## Documentation

- [Guides](docs/guides/) cover getting started, ingest, query, distributed query,
  operations, observability, tracing, admission limits, caching, correlation,
  disaster recovery, inspecting data, Kubernetes, and development.
- [Documentation index](docs/README.md) lists every guide and spec.
- [Architecture](docs/architecture.md) and the
  [consistency model](docs/consistency-model.md).
- Formats: [RSEG](docs/segment-format.md) for metrics,
  [RLOG](docs/log-segment-format.md) for logs, and
  [RSPAN](docs/span-segment-format.md) for spans.
- [Decision records](docs/adrs/), one per architectural choice.
- [Contributing](CONTRIBUTING.md), the [changelog](CHANGELOG.md), and the
  [AI policy](AI_POLICY.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
