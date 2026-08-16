# Ravel

[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/NOFireAI/ravel/graph/badge.svg)](https://codecov.io/gh/NOFireAI/ravel)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

An OpenTelemetry-native observability database whose only durable backend is
object storage.

## Quickstart

One command brings up the whole stack from published images — MinIO for
object storage, `ravel-server`, an OpenTelemetry Collector feeding it your
host's own metrics, and Grafana with a provisioned Ravel datasource. No Rust
toolchain, no cargo, no compile.

```sh
docker compose -f deploy/docker-compose/ravel.yml up -d
```

Open Grafana at <http://127.0.0.1:3000> (`admin` / `admin`); the Ravel
datasource is already wired up and the first dashboard shows your machine's
metrics within a few scrape intervals.

Query the data back over Ravel's Prometheus-compatible API. Every query needs
the demo bearer token, exactly as a real deployment requires a real one:

<!-- ravel:run status=200; json:.status=success -->
```sh
curl -s -H "Authorization: Bearer demo-token" \
  'http://127.0.0.1:4318/api/v1/query?query=system_cpu_load_average_1m'
```

The published image is built with `--features sql`, so `POST /api/v1/sql`
answers out of the box. The registered tables are `samples` (metrics), `logs`,
and `spans`:

<!-- ravel:run status=200; nonempty:.data.rows -->
```sh
curl -s -X POST http://127.0.0.1:4318/api/v1/sql \
  -H "Authorization: Bearer demo-token" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM samples LIMIT 5"}'
```

To see Ravel's read-your-write path directly — ingest one export, capture its
commit token, and read that exact write back — run
[demo/walkthrough.sh](demo/walkthrough.sh) while the stack is up. Stop the
stack with:

```sh
docker compose -f deploy/docker-compose/ravel.yml down
```

The [getting started guide](docs/guides/getting-started.md) walks the same path
with expected output.

### Kill the server, keep the data

Every Ravel process is disposable because the object store holds everything
durable. [demo/kill-and-recover.sh](demo/kill-and-recover.sh) demonstrates that
against the running stack rather than asserting it in prose:

![Ingesting one sample under strict acknowledgement, SIGKILLing the ravel-server container, replacing it with a fresh one, and reading the pre-kill sample back by its commit token.](docs/demo.gif)


```sh
demo/kill-and-recover.sh
```

It ingests one export under strict acknowledgement and captures the
`x-ravel-commit-token` the response carries, `SIGKILL`s the `ravel-server`
container (`docker compose kill`, so the process gets no chance to shut down
cleanly or flush anything on its way out), deletes that container, starts a
fresh one with an empty filesystem, and reads the pre-kill sample back by
passing the captured token as `min_commit_token`. Nothing crosses the kill
except what is in MinIO.

The script asserts every step itself and exits non-zero if the sample is absent
or the commit token comes back unsatisfiable, so a passing run is evidence and
not a demonstration you have to watch closely. CI runs it against a live stack
on every change to the quickstart.

### Capabilities and the from-source path

The quickstart's SQL surface exists because the published image carries
`--features sql`. The from-source contributor path, `make demo`, does **not**
build that feature, so `POST /api/v1/sql` is unavailable there; PromQL,
ingest, and the rest work on both. If you are changing Ravel's code and need to
run your own build, use `make demo` — see the
[development guide](docs/guides/development.md).

### Security

Everything in [deploy/docker-compose/ravel.yml](deploy/docker-compose/ravel.yml)
is a fixed **development** value: the demo bearer token (`demo-token`) and the
MinIO credentials (`ravel` / `ravel-dev-secret`). Every published host port
binds loopback (`127.0.0.1`) only, so the checked-in token never fronts an
ingest endpoint on your LAN. None of these values are for any deployment
reachable from a network.

## Why object storage

Metrics, logs, and traces land as immutable segments and commit records on S3
or MinIO. Every Ravel process — gateway, ingest shard, query frontend,
maintenance worker — is disposable: when Ravel acknowledges a write in strict
mode, that data survives the crash, restart, or redeployment of any process,
because it lives on the object store rather than in a process's memory or on
its local disk.

Metrics and logs run end to end today, from OTLP or Remote Write ingest through
PromQL, SQL, and Flight SQL query. Traces ingest, compact, and retain end to
end, and are queryable through the `spans` SQL table on `POST /api/v1/sql`.

## Container images

`ravel-server` and `ravel-operator` publish to the GitHub Container Registry
(GHCR) on every `vX.Y.Z` release tag, built from the root `Dockerfile` (see
[ADR-0037](docs/adrs/0037-container-image-ci-registry.md)). Only `linux/amd64`
is published; each published object is an OCI image index carrying an SBOM and
full build provenance. The quickstart pins
`ghcr.io/nofireai/ravel-server:0.9.2` by default; override it by setting
`RAVEL_IMAGE` before `docker compose up`.

```sh
docker pull ghcr.io/nofireai/ravel-server:latest
docker pull ghcr.io/nofireai/ravel-operator:latest
```

Tag scheme:

- `X.Y.Z` is write-once: a bad release is superseded by a new patch release,
  never by re-pushing the tag.
- `latest`, `X`, and `X.Y` are moving tags that track the newest matching
  release.

Consumers who need an immutable reference pin by digest
(`ghcr.io/nofireai/ravel-server@sha256:...`); consumers who need trust verify
the signature.

### Verifying signatures

Every published index digest is signed with
[cosign](https://github.com/sigstore/cosign) in keyless mode. The signing
certificate binds the signature to the release workflow's identity, so a pull
can be verified without any pre-shared key. Releases are cut from the public
mirror `NOFireAI/ravel`, so that is the identity in the certificate:

```sh
cosign verify \
  --certificate-identity 'https://github.com/NOFireAI/ravel/.github/workflows/publish-images.yml@refs/tags/v0.9.0' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  ghcr.io/nofireai/ravel-server:0.9.0
```

Replace `v0.9.0`/`0.9.0` with the release you are verifying; the tag ref in
`--certificate-identity` must be the exact tag that produced the image.

## Kubernetes

The same ingest/query round trip on a real Kubernetes cluster, driven by the
operator, needs `docker`, `kind`, and `kubectl`:

```sh
scripts/kind-up.sh     # cluster, images, fake S3, operator, RavelCluster
scripts/kind-demo.sh   # ingest via the gateway, query via the query tier
scripts/kind-down.sh
```

See the [Kubernetes guide](docs/guides/kubernetes.md) for details.

## How it fits together

![architecture](docs/diagrams/architecture.svg)

A write is durable once its commit record is on the object store, and a reader
sees it once the catalog resolves that commit into a snapshot. The
[ingest guide](docs/guides/ingest.md) covers the write path and the
[query guide](docs/guides/query.md) the read path; the
[consistency model](docs/consistency-model.md) is normative for what
acknowledgement, visibility, and crash recovery mean.

![ingest and commit sequence](docs/diagrams/ingest-commit-sequence.svg)

## Query surfaces

All query endpoints live under `/api/v1` on the HTTP listener (default bind
`127.0.0.1:4318`) and require `Authorization: Bearer <token>`, the same as
ingest. PromQL, SQL, Flight SQL, exemplars, alerting, and analytics surfaces
are each covered in the [query guide](docs/guides/query.md) and the
[distributed query guide](docs/guides/distributed-query.md).

## Where things live

- `crates/` — types, object store, the segment formats (RSEG for metrics, RLOG
  for logs, RSPAN for spans), commit protocol, catalog, OTLP/OTAP/Remote Write
  decode, ingest actors, PromQL, the query engine, and DataFusion-backed SQL
- `services/` — `ravel-server` (gateway, ingest, query, and maintain modes in
  one binary), `ravel-cli` (segment, commit, and catalog inspector), and the
  Kubernetes operator
- `docs/` — specs, decision records, diagrams, and guides
- `deploy/` — the container-first compose stack, the local MinIO stack, the
  OpenTelemetry Collector and Grafana provisioning, and Kubernetes manifests

## Documentation

- [Guides](docs/guides/) — getting started, ingest, query, distributed query,
  operations, observability, tracing, admission limits, caching, correlation,
  disaster recovery, inspecting data, Kubernetes, development
- [Documentation index](docs/README.md) — every guide and spec
- [Architecture](docs/architecture.md) and the
  [consistency model](docs/consistency-model.md)
- Formats: [RSEG](docs/segment-format.md) (metrics),
  [RLOG](docs/log-segment-format.md) (logs),
  [RSPAN](docs/span-segment-format.md) (spans)
- [Decision records](docs/adrs/) — one per architectural choice
- [SQL conformance](docs/sql-conformance.md) — every construct classified as
  supported, intentionally rejected, or unclassified
- [Contributing](CONTRIBUTING.md), the [changelog](CHANGELOG.md), and the
  [AI policy](AI_POLICY.md)

## License

Apache 2.0. See [LICENSE](LICENSE).
