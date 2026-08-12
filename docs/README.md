# Documentation index

## User guides

Start here to run Ravel, ingest into it, or query it.

- [guides/getting-started.md](guides/getting-started.md): install
  prerequisites, bring up MinIO, run `make demo`, and do the same ingest-then-query
  flow by hand. Read this first.
- [guides/ingest.md](guides/ingest.md): OTLP ingest endpoints, authentication,
  strict vs. buffered acknowledgement, every rejection reason, admission
  limits, and commit tokens. Read this to write data into Ravel.
- [guides/admission-limits.md](guides/admission-limits.md): the per-tenant
  ingest admission limits (body size, byte rate, series/stream caps,
  series-creation rate, event-time skew), their defaults, what a breach
  looks like to a client, per-process enforcement, and the coordinated-raise
  rule for `max_ingest_lag`. Read this to size or debug ingest rejections.
- [guides/query.md](guides/query.md): the five `/api/v1` endpoints, the
  supported PromQL subset, query budgets, and HTTP status codes. Read this
  to read data back out.
- [guides/distributed-query.md](guides/distributed-query.md): distributed read
  fan-out and cross-cluster federation (ADR-0071) for operators -- the cost gate
  and its two thresholds, `--distributed-query` and the fragment token file, the
  `sys/query/workers` heartbeat registry, `--remote-cluster` federation setup
  with `skip_unavailable` and the partial-coverage warnings a client sees,
  the `stats.fragments[]` block, the `ravel_distrib_*` metrics, and every
  failure behavior an operator will observe. Read this to scale a read across
  processes or clusters.
- [guides/correlation.md](guides/correlation.md): how an exemplar links a
  metric sample to a trace. It covers the storage, the admission cap, the query
  over `/api/v1/query_exemplars`, and the Grafana metric-to-trace link. Read
  this to go from a metric point to its trace.
- [guides/operations.md](guides/operations.md): full `ravel-server`/`ravel-cli`
  flag and env reference, storage backend setup, tenancy, the disposability
  model, and what garbage collection does and does not do today. Read this to
  deploy or operate Ravel.
- [guides/observability.md](guides/observability.md): the `GET /metrics`
  Prometheus endpoint -- every metric family it renders, the closed label
  allowlist and the `tenant_hash="other"` fold, how to read the per-query
  cost estimate against the actual, and three worked diagnosis procedures.
  Read this to understand a sample on `/metrics`.
- [guides/tracing.md](guides/tracing.md): the query-path `tracing` spans --
  the request-level and phase spans, the fields each records, the `RUST_LOG`
  filter that turns the debug phase spans on, how to read span byte and
  request counts to attribute a slow query to a phase, and the optional
  `--otlp-trace-endpoint` OTLP/gRPC export (ADR-0060). Read this to place a
  slow query's time in a phase, or to ship spans to a collector.
- [guides/caching.md](guides/caching.md): the ADR-0046 read cache -- what it
  stores, its CLI flags, startup warmup, its `/metrics` counters, and its
  known gaps (the disk tier, spans, `alerts`/`audit`). Read this to size or
  tune the cache, or to understand a warm-vs-cold latency difference.
- [guides/kubernetes.md](guides/kubernetes.md): the Ravel operator and the
  `RavelCluster` custom resource, the kind development environment
  (`scripts/kind-up.sh`), and what the `/healthz` and `/readyz` probes mean.
  Read this to deploy Ravel on Kubernetes.
- [guides/inspecting-data.md](guides/inspecting-data.md): `ravel-cli`
  worked examples that read segments, commit records, and catalog listings
  directly from the object store. Read this to see what is actually stored.
- [guides/development.md](guides/development.md): the fast local iteration
  loop (`cargo check` while you edit, the full gate list before you commit) and
  how CI shares build work across jobs with sccache and nextest. Read this
  to change Ravel's code.

## Specs

These are implementer-facing design documents. Read them to change Ravel's
code, not only to use it.

- [architecture.md](architecture.md): end-to-end system diagram and crate
  dependency graph. Start here for the implementer's view.
- [ingest.md](ingest.md): the ingest pipeline's internal structure (gateway,
  shard actors, flush) and sizing defaults.
- [query-engine.md](query-engine.md): the query engine's internal structure
  (snapshot resolution, segment fetch, pruning, evaluation) and budgets. It
  includes the generated PromQL conformance table (ADR-0035). Its
  distributed-read sections are the internal specification behind
  [guides/distributed-query.md](guides/distributed-query.md).
- [reviews/2026-08-adversarial-program/RAVEL-DISTRIBUTED-SEARCH-STATUS.md](reviews/2026-08-adversarial-program/RAVEL-DISTRIBUTED-SEARCH-STATUS.md):
  the as-built status of the distributed read epic (ADR-0071) against the
  pre-implementation review next to it -- what shipped file by file, the two
  places the shipped code deviates from the design and why, the erasure
  correctness gap the epic found and closed, the fault matrix with its
  enforcement points, and the deferrals now tracked as their own epics.
- [sql-conformance.md](sql-conformance.md): the generated SQL conformance
  table (ADR-0035). It classifies every construct in Ravel's claimed SQL
  surface as supported, intentionally rejected, or unclassified.
- [analytics.md](analytics.md): the post-evaluation analytics stage (ADR-0028):
  `ravel-analytics`' pure per-series change point detection and robust summary
  statistics, their parameters, error taxonomy, and evidence gate.
- [consistency-model.md](consistency-model.md): the normative durability and
  visibility guarantees: acknowledgement modes, the crash matrix, read-your-write,
  late and skewed data handling, deletion and GC. It is the source of truth for
  what Ravel actually promises.
- [catalog-and-mvcc.md](catalog-and-mvcc.md): object key layout, the commit
  protocol, commit tokens, and snapshot resolution.
- [segment-format.md](segment-format.md): the RSEG v6 on-object-store binary
  format (the only supported version pre-release, ADR-0027 and ADR-0047).
- [log-segment-format.md](log-segment-format.md): the RLOG v1 on-object-store
  binary format for logs (ADR-0029).
- [object-store-contract.md](object-store-contract.md): the
  `ObjectStoreBackend` trait that every storage vendor (memory, S3/MinIO) must
  satisfy, and the durability arguments made against it.
- [otap-ingest.md](otap-ingest.md): plan for OTAP (OpenTelemetry Arrow)
  ingest. It describes a feature-gated crate and gateway wiring. It is not yet
  wired into `ravel-server`.
- [arrow-datafusion-plan.md](arrow-datafusion-plan.md): the SQL query path
  (ADR-0013): `ravel-sql`'s DataFusion scan/merge/dedup pipeline, pushdown
  under the pruning-soundness invariant, and the planned HTTP/Flight SQL
  endpoints. It is in progress. For the adversarial review that redesigned it
  before implementation, see [reviews/2026-07-27-arrow-datafusion-plan-review.md](reviews/2026-07-27-arrow-datafusion-plan-review.md).
- [benchmarking.md](benchmarking.md): benchmarking methodology. See
  [BENCHMARKS.md](../BENCHMARKS.md) for the actual measured numbers.

## Decision records

- [adrs/](adrs/): one ADR per architectural decision (object-native L0, the
  commit protocol, catalog discovery, the RSEG format, series identity,
  the query engine and PromQL approach, wrapping `object_store`, tenant
  isolation, and more). [adrs/README.md](adrs/README.md) is the index.

## Diagrams

- [diagrams/](diagrams/): SVGs that the README and guides above reference
  (architecture, ingest commit sequence, RSEG layout, query path, tenancy
  key layout).
