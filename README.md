# Ravel

Research prototype of an OpenTelemetry-native, multi-tenant observability and
security database in Rust.

Core properties:

- Object storage (S3 / MinIO) is the **only** durable backend. No Kafka, no
  etcd, no local WAL, no persistent volumes. Every compute process is
  disposable and rebuildable from configuration plus the object store.
- Object-native LSM: an L0 object is simultaneously the durable ingest record,
  an immediately queryable segment, and compaction input.
- Strict durability by default: ingest is acknowledged only after the L0 data
  object and its immutable commit record are durably in the object store.
- Exact semantics by default: raw telemetry is lossless; approximation is
  always opt-in and visible in query semantics.
- Signals: OTLP metrics/logs/traces (profiles feature-gated later), Prometheus
  Remote Write, PromQL-compatible query API, and RavelQL, a unified pipe
  language for observability and security analytics (Sigma/OCSF support).

## Status

Phase 1 (end-to-end metrics vertical slice) in progress. See
[PROGRESS.md](PROGRESS.md) and [docs/adrs/](docs/adrs/) for decisions.

## Development

```sh
make check          # fmt + clippy + tests
make minio          # local MinIO via docker compose
make demo           # end-to-end: ingest OTLP metrics, query via PromQL API
```

## Layout

- `crates/`: libraries (types, object store, segment format, commit protocol,
  catalog, OTLP decode, ingest actors, PromQL, query engine)
- `services/`: `ravel-server` (gateway/ingester/query modes in one binary for
  development) and `ravel-cli` (object/manifest inspector)
- `docs/`: architecture, format specs, ADRs
- `proto/`: protobuf schemas for on-object metadata (segment footers, commit
  records, catalog snapshots)
- `bench/`: workload generator and benchmark harness
