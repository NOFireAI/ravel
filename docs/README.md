# Documentation index

## User guides

Start here to run Ravel, ingest into it, or query it.

- [guides/getting-started.md](guides/getting-started.md): install the
  prerequisites, bring up MinIO, run `make demo`, and do the ingest-then-query
  flow by hand. Read this first.
- [guides/ingest.md](guides/ingest.md): the OTLP ingest endpoints,
  authentication, strict and buffered acknowledgement, every rejection reason,
  admission limits, and commit tokens. Read this to write data into Ravel.
- [guides/admission-limits.md](guides/admission-limits.md): the per-tenant
  ingest admission limits and their defaults. It covers body size, byte rate,
  series and stream caps, series-creation rate, and event-time skew. It shows
  what a breach looks like to a client, per-process enforcement, and the
  coordinated-raise rule for `max_ingest_lag`. Read this to size or debug ingest
  rejections.
- [guides/query.md](guides/query.md): the Prometheus-compatible query endpoints,
  the supported PromQL subset, query budgets, and HTTP status codes. Read this
  to read data back out.
- [guides/traces.md](guides/traces.md): querying spans over the `spans` SQL
  table. It covers how Ravel stores spans and why a trace-by-id lookup is a
  bounded scan of one shard. It gives the table schema, the predicates that
  prune at the skip-index and bloom levels, worked queries, and what an
  incomplete trace is. Read this to investigate traces.
- [guides/distributed-query.md](guides/distributed-query.md): distributed read
  fan-out and cross-cluster federation for operators. It covers the cost gate
  and its two thresholds, `--distributed-query` and the fragment key file, the
  `sys/query/workers` heartbeat registry, and `--remote-cluster` federation with
  `skip_unavailable`. It also covers the partial-coverage warnings a client
  sees, the `stats.fragments[]` block, the `ravel_distrib_*` metrics, and every
  failure an operator will observe. Read this to scale a read across processes
  or clusters.
- [guides/correlation.md](guides/correlation.md): how an exemplar links a metric
  sample to a trace. It covers the storage, the admission cap, the query over
  `/api/v1/query_exemplars`, and the Grafana metric-to-trace link. Read this to
  go from a metric point to its trace.
- [guides/operations.md](guides/operations.md): the full `ravel-server` and
  `ravel-cli` flag and env reference, storage backend setup, tenancy, the
  disposability model, and what garbage collection does and does not do today.
  Read this to deploy or operate Ravel.
- [guides/observability.md](guides/observability.md): the `GET /metrics`
  Prometheus endpoint. It covers every metric family, the closed label
  allowlist, and the `tenant_hash="other"` fold. It shows how to read the
  per-query cost estimate against the actual, with three worked diagnosis
  procedures. Read this to understand a sample on `/metrics`.
- [guides/tracing.md](guides/tracing.md): the query-path `tracing` spans. It
  covers the request-level and phase spans, the fields each records, and the
  `RUST_LOG` filter that turns the debug phase spans on. It shows how to read
  span byte and request counts to place a slow query in a phase. It also covers
  the optional `--otlp-trace-endpoint` OTLP/gRPC export. Read this to place a
  slow query's time in a phase, or to ship spans to a collector.
- [guides/caching.md](guides/caching.md): the read cache. It covers what it
  stores, its CLI flags, startup warmup, its `/metrics` counters, and its known
  gaps (the disk tier, spans, `alerts`/`audit`). Read this to size or tune the
  cache, or to understand a warm-vs-cold latency difference.
- [guides/kubernetes.md](guides/kubernetes.md): the Ravel operator and the
  `RavelCluster` custom resource, the kind development environment
  (`scripts/kind-up.sh`), and what the `/healthz` and `/readyz` probes mean.
  Read this to deploy Ravel on Kubernetes.
- [guides/ingest-affinity.md](guides/ingest-affinity.md): pinning each tenant to
  a stable subset of gateway replicas. It covers what it saves, the throughput
  ceiling it costs, how to size the subset, and what a rolling restart or
  replica loss does. It also covers the routing options: the deprecated legacy
  `ingressNginx` backend, the Ravel-native subset router (`ravelNative`),
  Gateway API exposure, and why single-backend consistent hashing is only
  `S=1`. Read this to cut object-storage request cost without touching latency
  or any format.
- [guides/shard-overrides.md](guides/shard-overrides.md): lowering one tenant's
  shard count via `spec.shardOverrides`. It covers the request-cost savings, the
  throughput ceiling and shard-0 concentration it costs, and coarser maintenance
  units. Read this to cut per-tenant object-storage request cost.
- [guides/cost-model.md](guides/cost-model.md): the PUTs/day formula. It shows
  how tenant count, signal mix, shard count, replica affinity, and flush cadence
  each move the request cost, with a measured before-and-after of the
  flush-cadence default. It also covers the read side, where
  `--logs-request-cost-bytes` sets how many transferred bytes one saved round
  trip is worth. That setting controls how often a logs scan reads whole objects
  instead of ranges. Read this to predict the S3 request bill before you deploy,
  and to decide whether your read path is tuned for latency or cost.
- [guides/disaster-recovery.md](guides/disaster-recovery.md): recovering a
  deployment from object storage alone. It covers what state is reconstructable,
  the commit-record and catalog rebuild path, and the operator steps. Read this
  to plan or rehearse recovery.
- [guides/inspecting-data.md](guides/inspecting-data.md): `ravel-cli` worked
  examples that read segments, commit records, and catalog listings directly
  from the object store. Read this to see what is stored.
- [guides/development.md](guides/development.md): the fast local iteration loop
  (`cargo check` while you edit, the full gate list before you commit) and how
  CI shares build work across jobs with sccache and nextest. Read this to change
  Ravel's code.
- [guides/clickbench.md](guides/clickbench.md): running the public ClickBench
  `hits` workload against Ravel. It covers fetching the dataset, loading it,
  declaring the typed columns from the checked-in mapping, and measuring with
  `sql_latency_bench`. It also lists the statements the construct gate rejects.
  Read this to reproduce or extend the ClickBench numbers.
- [guides/clickbench-aws-runbook.md](guides/clickbench-aws-runbook.md): the same
  workload end to end on AWS. It covers creating the bucket, credentials, SSM
  parameters, IAM instance role, and EC2 box. It then covers the load, compact,
  fold, declare, and measure commands, the report integrity checks, and
  teardown. Read this to run a measured pass from nothing.
- [guides/coderabbit-runbook.md](guides/coderabbit-runbook.md): enabling,
  checking, operating, rotating, and removing the maintainer-gated CodeRabbit
  integration, including the controls that live in GitHub and CodeRabbit
  administration. Read this before you touch anything CodeRabbit-related.

## Specs

These are implementer-facing design documents. Read them to change Ravel's code,
not only to use it.

- [architecture.md](architecture.md): the end-to-end system diagram and crate
  dependency graph. Start here for the implementer's view.
- [explorer/](explorer/index.html): the interactive map. An isometric city of
  the crates and services, the flows that cross them (ingest to commit, PromQL
  and SQL reads, maintenance, alerting, boot), and live per-crate counts and
  dependencies. Open the file in a browser. Regenerate the numbers with `make
  archmap`.
- [ingest.md](ingest.md): the ingest pipeline's internal structure (gateway,
  shard actors, flush) and sizing defaults.
- [query-engine.md](query-engine.md): the query engine's internal structure
  (snapshot resolution, segment fetch, pruning, evaluation) and budgets. It
  includes the generated PromQL conformance table. Its distributed-read sections
  are the internal specification behind
  [guides/distributed-query.md](guides/distributed-query.md).
- [sql-conformance.md](sql-conformance.md): the generated SQL conformance table.
  It classifies every construct in Ravel's claimed SQL surface as supported,
  intentionally rejected, or unclassified.
- [analytics.md](analytics.md): the post-evaluation analytics stage.
  `ravel-analytics` does pure per-series change point detection and summary
  statistics. It covers their parameters, error taxonomy, and evidence gate.
- [consistency-model.md](consistency-model.md): the normative durability and
  visibility guarantees. It covers acknowledgement modes, the crash matrix,
  read-your-write, late and skewed data handling, deletion, and GC. It is the
  source of truth for what Ravel promises.
- [catalog-and-mvcc.md](catalog-and-mvcc.md): the object key layout, the commit
  protocol, commit tokens, and snapshot resolution.
- [segment-format.md](segment-format.md): the RSEG v7 on-object-store binary
  format for metrics. It is the only supported version.
- [log-segment-format.md](log-segment-format.md): the RLOG v4 on-object-store
  binary format for logs. It is the only supported version.
- [span-segment-format.md](span-segment-format.md): the RSPAN on-object-store
  binary format for spans.
- [object-store-contract.md](object-store-contract.md): the `ObjectStoreBackend`
  trait that every storage backend (memory, S3/MinIO) must satisfy, and the
  durability arguments made against it.
- [otap-ingest.md](otap-ingest.md): OTAP (OpenTelemetry Arrow) ingest. The
  decode stack is a feature-gated crate, linked by `ravel-server`'s `otap` cargo
  feature. The service registers at runtime only when the process starts with
  `--otap`. An end-to-end test drives a real OpenTelemetry collector against
  that endpoint.

## Decision records

- [adrs/](adrs/): one ADR per architectural decision (object-native L0, the
  commit protocol, catalog discovery, the RSEG format, series identity, the
  query engine and PromQL approach, wrapping `object_store`, tenant isolation,
  and more). [adrs/README.md](adrs/README.md) is the index.

## Diagrams

- [diagrams/](diagrams/): the SVGs that the README and guides reference
  (architecture, ingest commit sequence, RSEG layout, query path, tenancy key
  layout).
