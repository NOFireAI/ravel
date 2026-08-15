# Ravel

[![CI](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml/badge.svg)](https://github.com/NOFireAI/ravel/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/NOFireAI/ravel/graph/badge.svg)](https://codecov.io/gh/NOFireAI/ravel)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.97.1-orange.svg)](rust-toolchain.toml)

An OpenTelemetry-native observability database whose only durable backend is
object storage.

Metrics, logs, and traces land as immutable segments and commit records on S3
or MinIO. Every Ravel process — gateway, ingest shard, query frontend,
maintenance worker — is disposable: when Ravel acknowledges a write in strict
mode, that data survives the crash, restart, or redeployment of any process,
because it lives on the object store rather than in a process's memory or on
its local disk.

Metrics and logs run end to end today, from OTLP or Remote Write ingest through
PromQL, SQL, and Flight SQL query. Traces ingest, compact, and retain end to
end; a query surface over them is not built yet.

## Run it

```sh
make minio   # MinIO plus bucket creation, via docker compose
make demo    # ingest one OTLP export and query it back
```

`make demo` starts `ravel-server` against MinIO, sends a generated OTLP metrics
export, prints the commit token it receives, and queries the metric back by
`min_commit_token`. For the walkthrough with expected output, see the
[getting started guide](docs/guides/getting-started.md).

The same round trip on a real Kubernetes cluster, driven by the operator, needs
`docker`, `kind`, and `kubectl`:

```sh
scripts/kind-up.sh     # cluster, images, fake S3, operator, RavelCluster
scripts/kind-demo.sh   # ingest via the gateway, query via the query tier
scripts/kind-down.sh
```

## Container images

`ravel-server` and `ravel-operator` publish to the GitHub Container Registry
(GHCR) on every `vX.Y.Z` release tag, built from the root `Dockerfile` (see
[ADR-0037](docs/adrs/0037-container-image-ci-registry.md)). Only `linux/amd64`
is published; each published object is an OCI image index carrying an SBOM and
full build provenance.

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

## How it fits together

![architecture](docs/diagrams/architecture.svg)

A write is durable once its commit record is on the object store, and a reader
sees it once the catalog resolves that commit into a snapshot. The
[ingest guide](docs/guides/ingest.md) covers the write path and the
[query guide](docs/guides/query.md) the read path; the
[consistency model](docs/consistency-model.md) is normative for what
acknowledgement, visibility, and crash recovery mean.

![ingest and commit sequence](docs/diagrams/ingest-commit-sequence.svg)

## Query it

```sh
# PromQL, instant
curl -s 'localhost:8080/api/v1/query?query=http_requests_total'

# SQL over metrics, with ravel-server built and run with --features sql
curl -s localhost:8080/api/v1/sql -d '{"query":"SELECT * FROM metrics LIMIT 10"}'
```

PromQL, SQL, Flight SQL, exemplars, alerting, and analytics surfaces are each
covered in the [query guide](docs/guides/query.md) and the
[distributed query guide](docs/guides/distributed-query.md).

## Where things live

- `crates/` — types, object store, the segment formats (RSEG for metrics, RLOG
  for logs, RSPAN for spans), commit protocol, catalog, OTLP/OTAP/Remote Write
  decode, ingest actors, PromQL, the query engine, and DataFusion-backed SQL
- `services/` — `ravel-server` (gateway, ingest, query, and maintain modes in
  one binary), `ravel-cli` (segment, commit, and catalog inspector), and the
  Kubernetes operator
- `docs/` — specs, decision records, diagrams, and guides
- `deploy/` — local MinIO stack and Kubernetes manifests

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
- [Contributing](CONTRIBUTING.md) and the [changelog](CHANGELOG.md)

## License

Apache 2.0. See [LICENSE](LICENSE).
