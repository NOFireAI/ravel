# Documentation index

Ravel's documentation is arranged by what you are trying to do, not by
which part of the code implements it. Read down this page in order the
first time: each lane assumes the ones above it. Come back to one lane
when you have a specific question.

## Start

- The [README](../README.md) says what Ravel is, what it is built to do,
  and whether it is a fit for you. It carries the support matrix, which
  states for every surface whether it is in the published image or behind
  a cargo feature.

## Learn

- [guides/getting-started.md](guides/getting-started.md): the one
  recommended path from nothing to a first query, on the container
  quickstart. It explains what each response means, how long to wait for
  data, and when an empty result is the right answer. Building from source
  sits at the end of the page. Read this first.

## Understand

The mental model, in the order it makes sense.

- [concepts.md](concepts.md): the smallest set of ideas every other page
  assumes, and the glossary. Every Ravel term has one definition here, and
  every acronym is expanded here once.
- [architecture.md](architecture.md): the one-page overview. Which
  components exist, which state is durable, which processes are
  disposable, how ingest, query and maintenance meet, what Ravel requires
  of the object store, where the trust and failure boundaries are, and how
  the system behaves under concurrency and after a retry.
- [consistency-model.md](consistency-model.md): the normative statement of
  what Ravel promises. Acknowledgement, visibility, read-your-write,
  snapshot isolation, and the crash matrix come first; every claim on the
  page is asserted by a test.
- [deletion-and-gc.md](deletion-and-gc.md): how retention, erasure and the
  sweep delete data without breaking the guarantee above. Mechanism, not
  promise; the promise stays in the consistency model.

## Use

- [guides/ingest.md](guides/ingest.md): every accepted protocol and
  endpoint, authentication, strict against buffered acknowledgement, every
  rejection a client can see, and commit tokens. Read this to write data
  into Ravel.
- [guides/query.md](guides/query.md): the query routes, PromQL support and
  its rejected constructs, budgets, SQL over the `samples`, `logs` and
  `spans` tables, and the HTTP status codes. Read this to read data back.
- [guides/traces.md](guides/traces.md): querying spans over the `spans`
  table, why one trace is a bounded read, which predicates prune, and what
  an incomplete trace is.
- [guides/correlation.md](guides/correlation.md): how an exemplar links a
  metric sample to its trace, from storage and the admission cap to the
  exemplar query and the Grafana link.
- [guides/alerting.md](guides/alerting.md): writing a rules file, which
  process modes evaluate it, the interval and lookback settings, and the
  four sink kinds. Alert history is written durably and no shipped surface
  reads it back; the page says so up front.
- [guides/inspecting-data.md](guides/inspecting-data.md): `ravel-cli`
  worked examples that read segments, commit records and catalog listings
  straight from the bucket. Read this to see what is actually stored.

## Operate

- [guides/operations.md](guides/operations.md) is the entry point to
  running a cluster, in four pages:
  [configuration](guides/operations/configuration.md) for what to decide
  before starting anything,
  [deployment](guides/operations/deployment.md) for bringing a cluster up
  against a bucket for the first time,
  [maintenance](guides/operations/maintenance.md) for compaction, retention,
  the sweep, the scrubber and format migration, and
  [troubleshooting](guides/operations/troubleshooting.md) for symptom,
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
- [guides/cost-model.md](guides/cost-model.md): why request charges, not
  stored bytes, are the bill; the write-side formula and the levers that
  move it; and the read-side knob that trades round trips for bytes.
- [guides/distributed-query.md](guides/distributed-query.md): fan-out of
  one read across processes and federation across clusters, both off by
  default: the cost gate, the fragment keys and their rotation, the remote
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
  cargo feature gate where it has one. Derived from the router, not from
  another page.
- [reference/ravel-server-flags.md](reference/ravel-server-flags.md):
  every `ravel-server` flag, its environment variable and its default,
  generated from the binary's own definition and checked by a test.
- [reference/ravel-cli-flags.md](reference/ravel-cli-flags.md): every
  `ravel-cli` flag by subcommand, generated the same way.
- [sql-conformance.md](sql-conformance.md): every SQL construct Ravel
  claims, classified as supported, intentionally rejected, or unclassified,
  generated from the conformance suite's recorded verdicts.
- The PromQL conformance table, generated from a differential test against
  a real Prometheus binary, is in [query-engine.md](query-engine.md).

## Deep dives

Implementer contracts. Each is normative for the crate it names, and each
cites the decision records that govern it. Read one when you need the
exact protocol or byte layout, or when you are changing the code.

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
- [otap-ingest.md](otap-ingest.md): OpenTelemetry Arrow ingest, behind a
  cargo feature and a runtime flag, metrics only.
- [explorer/](explorer/index.html): an interactive map of the crates and
  the flows that cross them. Open the file in a browser.

## Decision records

- [adrs/](adrs/): one record per architectural decision, indexed in
  [adrs/README.md](adrs/README.md). A record explains why a choice was
  made at the time it was made. It is history, not a description of the
  current system; the pages above are.

## For people changing Ravel

Not part of the user manual. Held to the same currency rule as everything
else, and indexed here so nothing is reachable from nowhere.

- [internal/development.md](internal/development.md): the local iteration
  loop, the gate list, and how CI shares build work.
- [internal/clickbench.md](internal/clickbench.md): running the public
  ClickBench workload against Ravel and reading its report.
- [internal/clickbench-aws-runbook.md](internal/clickbench-aws-runbook.md):
  the same workload end to end on AWS, from an empty account to a measured
  pass.
- [internal/coderabbit-runbook.md](internal/coderabbit-runbook.md):
  operating the maintainer-gated code review integration, including the
  controls that live outside this repository.
- [internal/diagrams.md](internal/diagrams.md): what each diagram under
  `diagrams/` shows, which page it illustrates, and the visual language
  they share.
