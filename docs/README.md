# Documentation index

Ravel's documentation is arranged by what you are trying to do. Each lane
assumes the lanes above it, so read down the page in order the first time.
Come back to one lane when you have a specific question.

## Start

- The [README](../README.md) says what Ravel is and whether it is a fit for
  you. Its support matrix states for every surface whether it is in the
  published image or behind a cargo feature.

## Learn

- [guides/getting-started.md](guides/getting-started.md): the recommended
  path from nothing to a first query, on the container quickstart. It
  explains what each response means, how long to wait for data, and when an
  empty result is the right answer. Building from source is at the end of
  the page. Read this first.

## Understand

- [concepts.md](concepts.md): the ideas that every other page assumes, and
  the glossary. It defines every Ravel term once and expands every acronym
  once.
- [architecture.md](architecture.md): the one-page overview. It covers the
  components, the durable state, the disposable processes, and how ingest,
  query and maintenance meet. It also covers what Ravel requires of the
  object store, the trust and failure boundaries, and behavior under
  concurrency and after a retry.
- [consistency-model.md](consistency-model.md): the normative statement of
  what Ravel promises. Acknowledgement, visibility, read-your-write,
  snapshot isolation, and the crash matrix come first. A test asserts every
  claim on the page.
- [deletion-and-gc.md](deletion-and-gc.md): how retention, erasure and the
  sweep delete data and keep the guarantees of the consistency model. It
  describes the mechanism. The consistency model holds the promise.

## Use

- [guides/ingest.md](guides/ingest.md): how to write data into Ravel. Every
  accepted protocol and endpoint, authentication, strict and buffered
  acknowledgement, every rejection a client can see, and commit tokens.
- [guides/query.md](guides/query.md): how to read data back. The query
  routes, PromQL support and its rejected constructs, budgets, SQL over the
  `samples`, `logs`, `spans`, `alerts` and `audit` tables, and the HTTP
  status codes.
- [guides/traces.md](guides/traces.md): querying spans over the `spans`
  table, why one trace is a bounded read, which predicates prune, and what
  an incomplete trace is.
- [guides/correlation.md](guides/correlation.md): how an exemplar links a
  metric sample to its trace, from storage and the admission cap to the
  exemplar query and the Grafana link.
- [guides/alerting.md](guides/alerting.md): writing a rules file, which
  process modes evaluate it, the interval and lookback settings, and the
  four sink kinds. Alert history is queryable through the `alerts` table.
  The page has the columns, the predicates that prune, and the query that
  folds the history to current state per alert.
- [guides/audit.md](guides/audit.md): what Ravel records for every executed
  statement, every legal hold and every reshard, who writes those records,
  and how long each shard keeps them. The `audit` table reads them back,
  and reading the trail is itself audited.
- [guides/inspecting-data.md](guides/inspecting-data.md): `ravel-cli`
  worked examples that read segments, commit records and catalog listings
  from the bucket, to show what is stored.
- [guides/agents.md](guides/agents.md): the MCP surface for an AI agent
  host. How to connect, the nine tools grouped by task, the result envelope
  field by field, budgets and cursors, and what an empty result means.

## Operate

- [guides/operations.md](guides/operations.md) is the entry point to
  running a cluster, in four pages:
  - [configuration](guides/operations/configuration.md): what to decide
    before you start anything.
  - [deployment](guides/operations/deployment.md): bringing a cluster up
    against a bucket for the first time.
  - [maintenance](guides/operations/maintenance.md): compaction, retention,
    the sweep, the scrubber and format migration.
  - [troubleshooting](guides/operations/troubleshooting.md): symptom,
    cause, confirmation and action, with the two incident runbooks at the
    top.
- [guides/observability.md](guides/observability.md): every metric family
  on `GET /metrics`, the closed label allowlist, how to read the per-query
  cost estimate against the actual, and three worked diagnoses.
- [guides/tracing.md](guides/tracing.md): the query-path spans, what each
  records, the log filter that turns the phase spans on, and the optional
  export of Ravel's own spans to a collector.
- [guides/caching.md](guides/caching.md): the read cache, its RAM and disk
  tiers, its flags and warmup, its counters, and the workloads it does not
  cover.
- [guides/admission-limits.md](guides/admission-limits.md): the per-tenant
  ingest limits, their defaults, what a breach looks like to a client, and
  how to size them.
- [guides/cost-model.md](guides/cost-model.md): why request charges, and
  not stored bytes, are the bill. The write-side formula and the levers
  that move it, and the read-side knob that trades round trips for bytes.
- [guides/distributed-query.md](guides/distributed-query.md): fan-out of
  one read across processes and federation across clusters, both off by
  default. The cost gate, the fragment keys and their rotation, the remote
  cluster specification, and every failure an operator will observe.
- [guides/ingest-affinity.md](guides/ingest-affinity.md): pinning each
  tenant to a small, stable subset of gateway replicas to cut request
  cost, what it costs in throughput, and how to size the subset.
- [guides/shard-overrides.md](guides/shard-overrides.md): lowering or
  raising one tenant's shard count without touching the cluster-wide
  count, and what that trades.
- [guides/kubernetes.md](guides/kubernetes.md): the operator and the
  `RavelCluster` custom resource, the local kind environment, and what the
  health and readiness probes mean.
- [guides/disaster-recovery.md](guides/disaster-recovery.md): recovering a
  deployment from the bucket alone, the three configuration levels and
  what each requires, the verified restore procedure, and the rehearsal
  record behind every published recovery figure.

## Look up

- [reference/http-api.md](reference/http-api.md): every HTTP route the
  server exposes, with its method, what it accepts and returns, its status
  codes, whether it needs a bearer token, which modes serve it, and its
  cargo feature gate where it has one. Derived from the router.
- [reference/mcp.md](reference/mcp.md): every MCP tool, its inputs, the
  envelope blocks it uses, its bounds, and its failure classes, plus the
  shared envelope, cursor, budget, and protocol-header contracts.
- [reference/ravel-server-flags.md](reference/ravel-server-flags.md):
  every `ravel-server` flag, its environment variable and its default,
  generated from the binary's own definition and checked by a test.
- [reference/ravel-cli-flags.md](reference/ravel-cli-flags.md): every
  `ravel-cli` flag by subcommand, generated the same way.
- [sql-conformance.md](sql-conformance.md): every SQL construct Ravel
  claims, classified as supported, intentionally rejected, or unclassified,
  generated from the conformance suite's recorded verdicts.
- [query-engine.md](query-engine.md) holds the PromQL conformance table. It
  is generated from a Ravel-only run that measures which constructs Ravel
  reaches and answers. Agreement with the pinned Prometheus binary is a
  separate row there. That row reads `not measured in this run` in the
  committed table, and the differential lane's counts are in its job log.

## Deep dives

Implementer contracts. Each is normative for the crate it names, and each
cites the decision records that govern it. Read one when you need the
protocol or byte layout, or when you are changing the code.

- [catalog-and-mvcc.md](catalog-and-mvcc.md): the object key layout, the
  commit protocol, commit tokens, catalog folds and snapshot resolution.
- [segment-format.md](segment-format.md): RSEG, the segment format for
  metrics. One supported version.
- [log-segment-format.md](log-segment-format.md): RLOG, the segment format
  for logs. One supported version.
- [span-segment-format.md](span-segment-format.md): RSPAN, the segment
  format for spans. One supported version.
- [object-store-contract.md](object-store-contract.md): the backend trait
  every storage vendor must satisfy, the capabilities Ravel refuses to
  start without, and the bucket configuration the durability argument
  assumes.
- [ingest.md](ingest.md): the ingest pipeline's internal structure and
  sizing.
- [query-engine.md](query-engine.md): the query engine's internal
  structure, budgets, and the distributed read protocol.
- [analytics.md](analytics.md): the analytics stage behind
  `POST /api/v1/analytics`: change point detection and summary statistics
  with a robust (median and scaled median absolute deviation) centre.
- [otap-ingest.md](otap-ingest.md): OpenTelemetry Arrow ingest, metrics only,
  compiled into the published image and registered by the `--otap` flag.
- [explorer/](explorer/index.html): an interactive map of the crates and
  the flows that cross them. Open the file in a browser.

## Decision records

- [adrs/](adrs/): one record per architectural decision, indexed in
  [adrs/README.md](adrs/README.md). A record explains why a choice was
  made at the time it was made. It is history. The pages above describe
  the current system.

## For people changing Ravel

These pages are outside the user manual. They are held to the same currency
rule as every other page.

- [internal/development.md](internal/development.md): the local iteration
  loop, the gate list, and how CI shares build work.
- [internal/clickbench.md](internal/clickbench.md): running the public
  ClickBench workload against Ravel and reading its report.
- [internal/clickbench-aws-runbook.md](internal/clickbench-aws-runbook.md):
  the same workload end to end on AWS, from an empty account to a measured
  pass.
- [internal/diagrams.md](internal/diagrams.md): what each diagram under
  `diagrams/` shows, which page it illustrates, and the visual language
  they share.
- [internal/loader-memory-2613.md](internal/loader-memory-2613.md): where
  `ravel-cli load` puts its memory at large `--batch-rows` on ClickBench,
  by allocation site, and the ranked reductions.
- [guides/formal-verification.md](guides/formal-verification.md): what the
  TLA+ suite under `formal/tla/` checks, what it does not establish, and
  how to run it, read its results, and add a model.
- [../formal/tla/README.md](../formal/tla/README.md): machine-checked models
  of the commit, catalog, lifecycle, resharding, and maintenance protocols,
  and the harness that runs them. See
  [../formal/tla/REPORT.md](../formal/tla/REPORT.md) there for the
  suite-wide report and
  [../formal/tla/TRACEABILITY.md](../formal/tla/TRACEABILITY.md) for the
  index into each area's Rust traceability table.
