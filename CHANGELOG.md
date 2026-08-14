# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
