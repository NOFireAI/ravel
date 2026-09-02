# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.12.0]

Object-store request cost becomes an input that the logs read path and
compaction plan against, typed attribute column statistics ride on commit
records so aggregates over the live tail are answered without a scan, and the
RLOG compaction merge runs under a memory budget. The RLOG version 3 reader is
removed.

### Added

- **Request-cost-aware logs fetching** (ADR-0996). `--logs-fetch-policy`
  (`request-minimal`, `byte-minimal`, or the default `cost-based`) is resolved
  at startup into the byte quantities the fetch layer runs on, and
  `--logs-max-fetch-run-bytes` bounds one covering GET (default 64 MiB).
  `--logs-request-cost-bytes` states what one saved object-store round trip is
  worth in saved transfer bytes, and `--store-cost-profile` loads this
  deployment's per-request and per-GiB prices; a profile that fails to parse
  is refused at startup.
- **An S3 request ledger.** Billed HTTP requests are counted below the retry
  loop, so a GET that retried nine times counts ten attempts instead of one
  call, and KMS-routed traffic is counted too. GET requests are split per phase
  beside the wire bytes, the number of distinct data objects a query touched
  rides on the distributed query protocol as an additive field, and PromQL
  `query` and `query_range` responses render the per-phase split under
  `stats.phases`. Bench reports model request cost from the same ledger on the
  instrumented lanes; the Flight lane reports no cost rather than a false zero.
- **Typed attribute column statistics on commit records** (ADR-0873). Log
  ingest stamps each typed attribute column's exact min, max, and null count on
  the commit record, the catalog carries the stamps onto the segment reference,
  and compaction recomputes them for the segments it writes. SQL `MIN`/`MAX`
  over a typed attribute column is answered from the union of those stamps and
  the fold-built `.cstat` statistics with zero data GETs, which covers the live
  tail and token-resolved segments for the first time. Column statistics also
  carry an exact per-object integer sum, so `SUM(col + k)` and `AVG` over an
  integer column are answered from statistics as well.
- **`.cstat` re-keyed to snapshot-part binding** (ADR-0942): an envelope
  version 2 keyed by data-object content hash, and an additive snapshot HEAD
  field that references it. The column-statistics cache runs under a byte
  budget.
- **Bounded ephemeral spill** (ADR-0954). An opt-in, bounded scratch area for
  SQL operators whose exactness does not depend on holding the whole input,
  configured with `RAVEL_SQL_SPILL_DIR` and `RAVEL_SQL_SPILL_MAX_BYTES`. Off by
  default; a statement that exceeds its memory budget without it is still
  refused rather than approximated.
- **Advisory compaction claims** (ADR-1029). One small advisory object per unit
  of compaction work under `sys/maintain/claims/compaction/`, so two processes
  that would merge the same sealed bucket do not both pay for the whole merge.
  Correctness still rests on the compaction record's create-if-absent publish;
  a claim only saves cost.
- **MetricsBench** (ADR-0927): a versioned metrics workload and PromQL corpus,
  a Remote Write 1.0 ingest lane that replays one sample stream into Ravel and
  into config-supplied comparators, pinned comparator deployments, and a
  request-cost regression gate that fails a candidate report outside its
  per-figure bands.
- **Operator surfaces**: `spec.gc.protectionHorizon` and `spec.gc.grace` render
  the GC horizon flags on the maintain Deployment, so a bucket whose `sys/gc`
  holds non-default values no longer crash-loops. On a fresh cluster under
  per-role credentials the operator applies maintain first and holds the
  request-serving Deployments until `sys/gc` exists; a cluster whose
  request-serving Deployments already exist is never held. A bootstrap that
  has stalled for five minutes is reported on the cluster's conditions.
- **`ravel-cli` levers**: `maintain compact-tenant --bucket-concurrency`
  compacts independent buckets at once, its memory knobs
  (`--l1-part-memory-target-bytes`, `--max-l1-part-bytes`,
  `--input-read-concurrency`) are reachable, and its report attributes peak
  memory by phase. `load --max-flush-delay` raises the age trigger so a large
  `--target-bytes` is reachable, a `--target-bytes` that changed no object
  layout is reported rather than silently ignored, and the load report counts
  each shard's flushes by trigger (size, age, final).
- **`/metrics`** renders the ingest exemplar counters and the remaining flush
  counters (adaptive age flushes, grace-extended stale flushes, in-flight
  flushes).
- **Server-verified upload checksums** in the object-store crate. The S3
  backend can attach an `x-amz-checksum` value (CRC64-NVME or SHA-256) on
  single-part writes so the store verifies or rejects the bytes it received.
  Multipart uploads are excluded, and no `ravel-server` or `ravel-cli` flag
  exposes the setting yet, so the shipped binaries still write without one.
- **Documentation** (ADR-1040): a documentation architecture with a docs gate
  in CI, an HTTP API reference, generated `ravel-server` and `ravel-cli` flag
  references, a concepts page, an alerting guide, and operations pages for
  configuration, deployment, maintenance, and troubleshooting.

### Changed

- **The published `ravel-server` image builds every opt-in surface.** It is now
  built with `--features sql,flight-sql,otap`, so Flight SQL answers on the gRPC
  listener and `--otap` is accepted at startup without a source build. OTAP
  ingest is still registered only when `--otap` is given. The CI lanes that
  assemble images from host-built binaries build the same feature set.
- **Bounded-memory RLOG compaction merge** (ADR-0979). The merge opens an
  input's cursor only once its timestamp range can overlap the record about to
  be emitted, holds decoded blocks in their columnar form and charges them at
  their heap estimate, prices cursor admission from block shape and reconciles
  after decode, releases each closed segment's bytes at PUT, and runs under a
  merge budget; `compact-tenant` divides the budget across concurrent buckets
  only while it still carries the box-sized default. The admission change
  emits the same records and the same segment boundaries as opening every
  cursor at once; the number of open cursors becomes the input overlap depth
  rather than the input count.
- **`--max-l1-part-bytes` bounds encoded object bytes** (#872). The RLOG
  merge closed an L1 segment against a pre-compression payload proxy, so
  stored sizes missed the target in both directions, by several times on a
  compressible schema. The merge now encodes to measure the real object bytes
  and closes on that count, with the probe step capped so overshoot past the
  target is bounded. For the same inputs, segment boundaries differ from
  those 0.11.0 wrote.
- **Equality matchers resolve by dictionary ordinal.** Below the sparse-series
  threshold, a metrics catalog decode whose matchers are all positive
  equalities resolves each value to its dictionary ordinal once and
  materializes a label set only for a series that matched. Fetched bytes are
  unchanged; on a deterministic in-memory fixture of 4000 series the decode
  took 38.1 percent less wall time at 1 percent selectivity.
- **The catalog fold** reads each covered object once in the dual publish and
  keeps its statistics tally cache across HEAD CAS retries, so a lost CAS no
  longer refetches every object.
- **Typed attribute column reads** in SQL build their resolvers once per block
  rather than once per chunk.
- **Distributed query protocol**: the data-objects-touched count is an
  additive slice field. An older peer omits it and the merged figure degrades
  to the coordinator's own count. The protocol version is unchanged.

### Removed

- **The RLOG version 3 reader** (ADR-0892). RLOG now accepts exactly one
  trailer version, as RSEG and RSPAN already did under ADR-0027 decision 7 and
  ADR-0066 decision 1. Log objects written by releases before 0.11.0 are no
  longer readable, and `maintain migrate` reads the same single-version window,
  so a tenant that still holds them is wiped or re-ingested.

### Fixed

- Column-statistics objects a resolvable snapshot still referenced were
  treated as orphans by the unreferenced-catalog-object sweep and deleted once
  past the protection horizon, which broke queries that resolve typed-column
  statistics through the snapshot. Both statistics carriers on HEAD are now in
  the sweep's reachability set.
- Three SQL exact-aggregate paths (`COUNT` under a not-equal predicate,
  `GROUP BY` counts, and `SUM`/`AVG`) answered from a `.cstat` entry whose row
  accounting had not been reconciled against the segment it was joined to. All
  four readers now go through one reconciliation.
- The shipped IAM templates granted the maintain role no write on
  `sys/maintain/` and the query role nothing under `sys/query/`, so a maintain
  process failed closed with `AccessDenied` on its first liveness heartbeat,
  and a query worker on its membership heartbeat.
- On a fresh bucket with per-role credential Secrets, gateway and query pods
  raced maintain to create `sys/gc`, failed the create, and crash-looped. The
  operator now orders the bootstrap, and validates `spec.gc` even when
  maintain is disabled.
- Compaction convergence reported a bucket converged while the winner record
  referenced a segment that was absent and could not be re-put from this run;
  it now fails so the bucket is retried. The scope opener emits its request
  report on every outcome, and opener election is atomic and
  cancellation-safe.
- A refused row-major write into a columnar ingest buffer still left its
  records' extrema in the typed attribute column statistics accumulator, so
  the next flush stamped min, max, and non-null count for records the object
  does not hold. A refused write no longer contributes.
- `make demo` had failed on a fresh bucket since the keyed-tenancy gate
  landed; the dev bucket is pinned unkeyed, as the compose quickstart already
  was.
- The startup log reported a Flight SQL listener state the build was not in.
- A `load` at a raised `--max-flush-delay` did not complete at its own
  settings; the drain now sweeps tail stragglers with a re-flush ticker and
  leaves reserve headroom in the delay ceiling.
- `ravel-cli` walk-shaped commands name the effective store in their header
  and refuse a defaulted in-memory walk that reaches no data, instead of
  reporting zero counters at exit 0.

### Known limitations

- Query latency still depends on the tenant's working set fitting in the read
  cache; removing the full-scan floor is tracked in #849. ClickBench `q33`
  still exceeds the per-query memory budget (#837): the bounded spill relieves
  the aggregate, and the scan's share of the memory remains.
- Logs and spans return overlapping records twice when two compaction records
  with overlapping input sets are published for one bucket (#1070). Metrics
  are unaffected, because query-time dedup collapses the overlap. A fix is in
  review.
- After a selective-erasure rewrite lands in a sealed hour outside the fold's
  reconcile window, the superseded-input sweep can delete inputs a HEAD-named
  snapshot part still resolves, and queries over that hour then fail closed
  with `SnapshotInvalidated` until the fold reconciles the hour (#1085).
  Subject erasure stays correct throughout. A fix is in review.

## [0.11.0]

The log segment format moves to RLOG v4 and the logs query path becomes
columnar end to end. Measured on the ClickBench `hits` corpus (12.03 GB, 99.99M
rows, 42 timed statements on an r6a.4xlarge against in-region S3), the hot total
falls from 96.40 s to 72.52 s and the cold total from 320.18 s to 222.19 s.

### Added

- **Typed column statistics for logs** (ADR-0850). The fold writes exact
  per-object statistics for typed attribute columns, and `MIN`/`MAX` over a
  typed attribute column can be answered from the catalog without opening a
  segment.
- **SQL surface**: a fail-closed scalar and window function registry
  (ADR-0097), `LIKE`/`NOT LIKE` on the logs table with substring pruning
  (ADR-0105), and typed predicate pushdown for declared logs columns. Functions
  outside the registry now produce a typed error rather than a late failure.
- **Aggregation pushdown** for order-insensitive aggregates (ADR-0103), and a
  metadata-only rewrite that answers predicate-free `COUNT(*)` shapes with zero
  object-store GETs.
- **Native histograms through range evaluation** (ADR-0108): range counter and
  `_over_time` functions carry native histograms, and they distribute over the
  fan-out path for the first time.
- **Operator surfaces**: `--cache-dir` attaches the ADR-0046 disk cache tier end
  to end; `--s3-auth` and the S3 credential flags add an instance-role
  credential source (ADR-0106); `ravel-cli maintain compact-tenant` compacts a
  whole tenant and can seal sooner for measurement.
- **Intra-segment scan partitioning and a spill policy** for logs (ADR-0102),
  and late materialization for wide `TopK` projections (ADR-0774) so a sort
  reads the narrow set and fetches the rest only for surviving rows.

### Changed

- **RLOG bumped to v4** (ADR-0699): row groups plus a `PAGE_DIR` section, which
  makes per-column extents individually addressable. A narrow projection over a
  v4 object can fetch only the columns it needs instead of the whole object.
  The reader accepts v3 and v4; writers emit v4.
- **Columnar decode to Arrow** (ADR-0099). Logs and metrics scans build batches
  from a borrowed columnar block view, and declared string columns keep their
  dictionary form end to end rather than being materialized per row.
- **Pruning-proportional logs fetch** (ADR-0107): the fetch layer issues block
  ranges proportional to what pruning actually selected, and the whole-segment
  fast path now consults projection width before choosing a whole-object read.
- **Distributed query protocol bumped to version 4**, adding a
  `PartialAggregate` wire frame so pushed-down aggregates cross the fan-out
  boundary. Version 3 (ADR-0096) added per-sample dedup provenance and resolved
  0.10.0's run-merged limitation below.
- **Clustered compaction and object pruning** (ADR-0815), and a bulk-load
  columnar fast path with revised write-concurrency defaults (ADR-0109,
  ADR-0807).

### Fixed

- The 0.10.0 known limitation on run-merged series and the distributed query
  path is resolved. `ravel.queryfrag.v1` (protocol version 3, ADR-0096) carries
  per-sample dedup provenance on the wire, native histograms distribute for the
  first time, and both the run-merged and histogram refusals are removed. A
  distributed query over either shape now returns results bit-identical to the
  same query run locally.
- Native histograms were being silently dropped in three PromQL paths; they now
  carry through. `histogram_rate`/`sum_histograms` no longer panic on a schema
  mismatch, and `irate`/`idelta` had their reset direction corrected.
- Query text is guarded against a parser stack overflow.
- A fold lifetime whose seal margin would overflow is refused rather than
  accepted and silently sealing nothing.

### Known limitations

- Query latency still depends on the tenant's working set fitting in the read
  cache. When it does not, every full-scan statement re-reads its objects from
  object storage on each run: the eviction policy is scan-resistant (S3-FIFO,
  ADR-0046) but cannot create reuse that a scan-everything access pattern does
  not have. The published ClickBench figures above were measured with a cache
  larger than the corpus and do not characterize a tenant whose data greatly
  exceeds its cache. Removing the full-scan floor is tracked in #849.
- One ClickBench statement (`q33`) fails on connection-pool exhaustion (#837),
  so the totals above are over 42 of the suite's 43 statements.

## [0.10.0]

The metrics segment format moves to RSEG v7 and the L1 compactor stops
copying runs verbatim. Measured over 500 series at a 15-second scrape, an
L1 object falls from 26.52 to 8.88 bytes per sample, a 2.99x reduction.

### Changed

- RSEG segment format bumped to v7 (ADR-0092). v7 is v6 plus three additive
  changes: an optional per-sample dedup provenance extension in the whole
  SERIES_META (so an L1 run can merge several writes' samples and still preserve
  exact dedup order); two value page encodings, `VAL_ALP` (18) and
  `VAL_GCD_DELTA_FOR` (19), and one timestamp encoding, `TS_GCD_I64` (2), each
  selected per page against the prior encoding and kept only when smaller; and
  two page-level byte savings (a run's first timestamp stored as a delta from
  the run minimum, and single-sample raw-`f64` value pages dropping the 8-byte
  alignment pad). `docs/segment-format.md` is rewritten as the self-contained v7
  specification.
- Pre-release single-version policy (ADR-0027): v6 read and write support is
  deleted in the same change. The reader accepts trailer `version = 7` only and
  fails closed on any other version, including a stray v6 object, with a typed
  `UnsupportedVersion`. There is no v6 reader and no v6-to-v7 migration path.
- L1 compaction merges runs instead of preserving them verbatim (ADR-0092,
  reversing ADR-0018's choice). An L1 object now holds one run per series
  rather than one run per input object per series, carrying each sample's
  dedup key in v7's per-sample provenance columns so late duplicates still
  resolve exactly. A series with a single contributing run keeps its bytes
  and carries no column, so an L0 flush is unchanged. Part splitting now
  accumulates encoded output bytes rather than predicted input bytes, since
  per-page codec selection makes output size a function of the data's shape.

### Known limitations

- A run-merged series cannot be executed over the distributed query path.
  `ravel.queryfrag.v1`'s `Run` message carries run-wide dedup provenance
  only, so a distributed fetch would resolve an overlapping timestamp to a
  different winner than the same query run locally. The worker refuses the
  merged shape and the coordinator falls back to local execution, which is
  exact. Any query touching run-merged L1 therefore loses read fan-out until
  the wire format carries per-sample provenance (#348). Results stay correct;
  the cost is parallelism.

## [0.9.5]

Documentation only. No code changed since 0.9.4, so the binaries and images
this release publishes are rebuilt from the same source.

### Added

- An interactive architecture explorer in the documentation.
- A release badge in README.md, pointing at the latest release.

### Changed

- ADR-0086 records that its required-checks decision has been applied:
  `supply-chain`, `docker-build`, `fuzz`, `object-store-contract`,
  `promql-difftest` and `actionlint` now gate merges to `main`.

## [0.9.4]

### Added

- GitHub Releases are published for every `vX.Y.Z` tag, carrying per-architecture
  binaries for `ravel-server`, `ravel-cli`, `ravel-operator` and
  `ravel-ingest-router`, separated debug symbols, a `SHA256SUMS` file, and a
  keyless cosign signature over it. The binaries are extracted from the
  published images rather than rebuilt, so each is byte-identical to the one
  inside the signed image.
- CI lints workflow files with actionlint and shellcheck, and fails if
  shellcheck is not genuinely available rather than silently checking less.
- CI fails when a path dependency's version drifts from
  `[workspace.package] version`.

### Changed

- Container images are roughly a quarter of their previous size. The builder
  now separates debug info with `objcopy` and ships stripped binaries carrying
  a `.gnu_debuglink`, so the `ravel-server` image drops from 923 MB to 209 MB.
  Symbols are published with each release. `[profile.release] debug = 1` is
  unchanged.
- A release compiles the workspace twice instead of six times. The publish
  matrix is now one job per platform, building all three image targets against
  a shared builder layer.

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
- A post-evaluation analytics endpoint for change point detection and
  robust (median and scaled median absolute deviation) summary statistics.
- A unified alerting and detection engine that stores every rule transition
  as immutable, queryable data.
- Compaction, age-based retention, and garbage collection across all signals,
  with per-tenant SSE-KMS encryption, legal hold, and custody verification.
- Optional distributed read fan-out and cross-cluster federation, off by
  default and byte-identical to local execution.
- A Kubernetes operator with a `RavelCluster` custom resource, and published
  `ravel-server` and `ravel-operator` container images.
