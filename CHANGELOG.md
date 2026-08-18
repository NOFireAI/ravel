# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.3]

### Added

- `ravel-ingest-router`, a Ravel-native ingest router that steers OTLP over
  HTTP and gRPC (HTTP/2) to a stable subset of ingest replicas, published as
  its own container image.
- gzip-compressed OTLP ingest over HTTP.
- Exemplars carried end to end over the OTLP HTTP ingest path.
- An optional `Authorization` credential on alert-sink delivery.
- Operator support for Gateway API ingress exposure and a Ravel-native
  ingest-affinity backend, per-tenant shard overrides, and an
  operator-settable flush cadence.
- Durable per-tenant indexed-field overrides applied at ingest, and a
  per-tenant PUT attribution metric family.
- Multi-architecture container images: `linux/amd64` and `linux/arm64` are
  each built on a native runner and the merged index is signed.
- A container-first quickstart whose marked README command blocks are
  asserted against a live stack in CI.

### Fixed

- Bump `h2` to 0.4.16 for RUSTSEC-2026-0258.
- `ravel-ingest-router` supervises its background tasks and redacts secrets
  from `Debug` output.

## [0.9.2]

### Added

- RSPAN v4 span segment format: per-key typed attribute columns replace the
  single opaque per-row attribute blob, and span events, including the
  exception stack traces they carry, are promoted into scan-queryable nested
  columns.

### Fixed

- Set the workspace version to the real release version so the image-publish
  version-tag gate passes; `0.9.0` and `0.9.1` had shipped from a `0.1.0`
  placeholder.

## [0.9.1]

### Added

- Selective subject erasure across metrics, logs, and traces: `ravel-cli
  erase submit` and `erase status`, resolver-side exclusion of erased
  subjects, and a segment-rewrite pass that removes their data from stored
  objects.
- A `spans` SQL table alongside `samples` and `logs`, over both HTTP and
  Flight SQL, with service name, duration, and status-code predicate
  pushdown.
- OIDC and mTLS tenant resolvers, the latter served on a dedicated listener,
  for authenticating tenants without static bearer tokens.
- Per-tenant query cost governance: bytes-scanned and S3-request budgets
  enforced during scans, with per-query cost accounting exported on
  `/metrics`.
- Online resharding through a generation-versioned shard count, with
  maintenance work leased across workers.
- Query-path OTLP trace export, enabled with `--otlp-trace-endpoint`.
- A local read-cache tier over RAM and disk in front of object-store reads.
- Signed and attested release images: every published index is cosign-signed
  in keyless mode and carries an SBOM and build provenance, and a tag publish
  is gated on a passing CI run for the tagged commit.

### Changed

- Cross-cluster federation defaults to TLS and warns on plaintext.
- Ingest flushes are pipelined with an adaptive flush delay, and process-wide
  ingest memory is bounded with idle-tenant eviction.

### Security

- Constant-time bearer-token lookup and a decode panic guard on the OTAP
  ingest path.
- Require an OIDC audience, and bump `jsonwebtoken` to 10.4 for
  CVE-2026-25537.

## [0.9.0]

First public release. Ravel is an OpenTelemetry-native observability database
whose only durable backend is S3-compatible object storage; every compute
process is disposable.

### Added

- OTLP ingest over HTTP and gRPC for metrics, logs, and traces, plus
  Prometheus Remote Write 1.0/2.0, with per-tenant admission limits and
  strict or buffered acknowledgement.
- Immutable segment formats on object storage: RSEG for metrics (including
  native exponential histograms and exemplars), RLOG for logs, and RSPAN for
  traces, each committed through a two-object create-if-absent protocol.
- PromQL query over `/api/v1/query` and `/api/v1/query_range`, with a
  differential-tested evaluator, and the Prometheus exemplar and HTTP API
  compatibility surface for Grafana.
- SQL query through Apache DataFusion over `samples` and `logs` tables,
  exposed over HTTP and Arrow Flight SQL.
- A post-evaluation analytics endpoint for change point detection and robust
  summary statistics.
- A unified alerting and detection engine that stores every rule transition
  as immutable, queryable data.
- Compaction, age-based retention, and garbage collection across all signals,
  with per-tenant SSE-KMS encryption, legal hold, and custody verification.
- Optional distributed read fan-out and cross-cluster federation, off by
  default and byte-identical to local execution.
- A Kubernetes operator with a `RavelCluster` custom resource, and published
  `ravel-server` and `ravel-operator` container images.
