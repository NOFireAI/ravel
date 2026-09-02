# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
