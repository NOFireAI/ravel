# Documentation index

## User guides

Start here if you want to run Ravel, ingest into it, or query it.

- [guides/getting-started.md](guides/getting-started.md): install
  prerequisites, bring up MinIO, run `make demo`, and do the same ingest-then-query
  flow by hand. Read this first.
- [guides/ingest.md](guides/ingest.md): OTLP ingest endpoints, authentication,
  strict vs. buffered acknowledgement, every rejection reason, admission
  limits, and commit tokens. Read this if you're writing data into Ravel.
- [guides/query.md](guides/query.md): the five `/api/v1` endpoints, the
  supported PromQL subset, query budgets, and HTTP status codes. Read this
  if you're reading data back out.
- [guides/operations.md](guides/operations.md): full `ravel-server`/`ravel-cli`
  flag and env reference, storage backend setup, tenancy, the disposability
  model, and what garbage collection does and doesn't do today. Read this if
  you're deploying or operating Ravel.
- [guides/inspecting-data.md](guides/inspecting-data.md): `ravel-cli`
  worked examples for reading segments, commit records, and catalog listings
  directly out of the object store. Read this when you need to see what's
  actually stored.
- [guides/development.md](guides/development.md): the fast local iteration
  loop (`cargo check` while editing, full gate list before committing) and
  how CI shares build work across jobs with sccache and nextest. Read this
  if you're changing Ravel's code.

## Specs

Implementer-facing design documents. Read these if you're changing Ravel's
code, not just using it.

- [architecture.md](architecture.md): end-to-end system diagram and crate
  dependency graph. Start here for the implementer's view.
- [ingest.md](ingest.md): the ingest pipeline's internal structure (gateway,
  shard actors, flush) and sizing defaults.
- [query-engine.md](query-engine.md): the query engine's internal structure
  (snapshot resolution, segment fetch, pruning, evaluation) and budgets.
- [consistency-model.md](consistency-model.md): the normative durability and
  visibility guarantees: acknowledgement modes, the crash matrix, read-your-write,
  late/skewed data handling, deletion and GC. The source of truth for "what
  does Ravel actually promise."
- [catalog-and-mvcc.md](catalog-and-mvcc.md): object key layout, the commit
  protocol, commit tokens, and snapshot resolution.
- [segment-format.md](segment-format.md): the RSEG v1 on-object-store binary
  format.
- [object-store-contract.md](object-store-contract.md): the
  `ObjectStoreBackend` trait every storage vendor (memory, S3/MinIO) must
  satisfy, and the durability arguments made against it.
- [otap-ingest.md](otap-ingest.md): plan for OTAP (OpenTelemetry Arrow)
  ingest. Describes a feature-gated crate and gateway wiring; not yet
  wired into `ravel-server`.
- [arrow-datafusion-plan.md](arrow-datafusion-plan.md): the SQL query path
  (ADR-0013): `ravel-sql`'s DataFusion scan/merge/dedup pipeline, pushdown
  under the pruning-soundness invariant, and the planned HTTP/Flight SQL
  endpoints. In progress; see [reviews/2026-07-27-arrow-datafusion-plan-review.md](reviews/2026-07-27-arrow-datafusion-plan-review.md)
  for the adversarial review that redesigned it before implementation.
- [benchmarking.md](benchmarking.md): benchmarking methodology. See
  [BENCHMARKS.md](../BENCHMARKS.md) for the actual measured numbers.

## Decision records

- [adrs/](adrs/): one ADR per architectural decision (object-native L0, the
  commit protocol, catalog discovery, the RSEG format, series identity,
  the query engine and PromQL approach, wrapping `object_store`, tenant
  isolation, and more). [adrs/README.md](adrs/README.md) is the index.

## Diagrams

- [diagrams/](diagrams/): SVGs referenced from the README and guides above
  (architecture, ingest commit sequence, RSEG layout, query path, tenancy
  key layout).
