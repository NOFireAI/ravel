# Architecture Decision Records

One decision per document. Status: Proposed | Accepted | Superseded.

Numbering: ADR 0001 through 0109 are sequential. From ADR 0110 onward the
number is the GitHub issue number of the issue that produced it (the epic
when the decision spans the epic, the ticket when an epic has several
decisions in flight at once, as ADR-0774 under epic #680 does), so the
sequence jumps. GitHub allocates issue numbers atomically, which removes
the collision that sequential numbering had between parallel sessions and
the reservation commit that used to work around it.

| # | Title | Status |
|---|-------|--------|
| [0001](0001-object-native-l0.md) | Object-native L0, no local WAL | Accepted |
| [0002](0002-commit-protocol.md) | Two-object commit protocol with create-if-absent commit records | Accepted |
| [0003](0003-catalog-discovery.md) | Listing-based discovery first, immutable catalog snapshots second | Accepted |
| [0004](0004-rseg-format.md) | RSEG v1: hand-specified layout, protobuf footer, per-page compression | Accepted |
| [0005](0005-series-identity.md) | BLAKE3-128 canonical series identity with stored-label collision verification | Accepted |
| [0006](0006-query-engine.md) | Custom signal-aware engine first; Arrow/DataFusion evaluated at Phase 3 | Accepted |
| [0007](0007-promql-approach.md) | promql-parser crate for parsing, own evaluator, differential testing gate | Accepted |
| [0008](0008-object-store-crate.md) | Wrap `object_store` crate behind our ObjectStoreBackend trait | Accepted |
| [0009](0009-tenant-isolation.md) | Tenant-hashed prefixes, gateway auth, dev-mode header tenancy behind flag | Accepted |
| [0010](0010-spec-amendments-review-1.md) | Spec amendments from the first design review | Accepted |
| [0011](0011-otap-arrow-ingest.md) | OTAP (OpenTelemetry Arrow) ingest, not generic Arrow Flight | Accepted |
| [0012](0012-simd-policy.md) | SIMD policy: dependencies and autovectorization first, explicit SIMD behind benchmark-gated review | Accepted |
| [0013](0013-arrow-zero-copy-and-datafusion.md) | Targeted Arrow zero-copy, DataFusion for SQL and relational operators only | Accepted |
| [0015](0015-remote-write-ingest.md) | Prometheus Remote Write 1.0/2.0 ingest surface | Accepted |
| [0016](0016-otlp-classic-histogram-summary-explosion.md) | OTLP explicit-bounds histograms and summaries explode to Prometheus-convention series | Accepted |
| [0017](0017-native-histograms-rseg-v3.md) | Native exponential histograms: span-based value model, RSEG v3 after RSEG v2 phase 6 closes | Amended by 0027 |
| [0018](0018-l0-l1-compaction.md) | L0 to L1 compaction: verbatim rewrite of sealed ingest-hour buckets | Amended by 0026, 0027 |
| [0019](0019-age-based-retention.md) | Age-based retention via bucket tombstones and horizon-gated sweep | Accepted |
| [0020](0020-metric-index.md) | Metric index: catalog snapshots as the commit index, async fold, name postings gated | Accepted |
| [0021](0021-promql-full-evaluator.md) | Full PromQL evaluator scope and phasing, differential harness against pinned Prometheus | Accepted |
| [0022](0022-floating-aggregate-exactness.md) | Floating aggregate exactness: allowlisted v1 subset, avg admitted via a sequential UDAF, second-moment family excluded | Accepted |
| [0023](0023-grouped-min-max-total-order-udaf.md) | Grouped MIN/MAX restored via a total-order min/max UDAF replacing the built-ins | Accepted |
| [0024](0024-sum-sequential-fold.md) | Replace the built-in `sum` aggregate with a sequential-fold UDAF | Proposed |
| [0025](0025-promql-differential-float-precision-residue.md) | PromQL differential float-precision residue: rate/deriv/predict_linear vs. atanh | Accepted |
| [0026](0026-rseg-v5-sparse-id-index.md) | RSEG v5: sparse id index and chunked SERIES_META as the default compaction output | Amended by 0027 |
| [0027](0027-single-rseg-version-pre-release.md) | Single supported RSEG version until first release; v1-v4 support removed | Accepted |
| [0028](0028-analytics-stage.md) | Post-evaluation analytics stage: change point detection and robust statistics in a new ravel-analytics crate | Accepted |
| [0029](0029-rlog-v1-log-segment.md) | RLOG v1: columnar log segment format, a sibling to RSEG | Accepted |
| [0030](0030-promql-subquery-point-cap-divergence.md) | PromQL subquery point-cap divergence from Prometheus: Ravel's documented cap rejects two cases Prometheus succeeds on, by design | Accepted |
| [0031](0031-empty-label-identity.md) | Empty-valued labels absent from series identity everywhere; empty-named labels always rejected | Proposed |
| [0032](0032-rlog-compaction-and-generic-maintain.md) | RLOG compaction, and a signal-generic ravel-maintain | Accepted |
| [0033](0033-sql-query-over-logs.md) | SQL query over logs (log storage phase 3) | Accepted |
| [0034](0034-k8s-operator.md) | Kubernetes operator, kind development environment, and k8s CI lane | Accepted |
| [0035](0035-conformance-scoring.md) | Conformance scoring: three-state classification over the full PromQL and SQL surfaces, scored on the claimed subset | Accepted |
| [0036](0036-performance-investigation-methodology.md) | Performance investigation methodology and scope | Accepted |
| [0037](0037-container-image-ci-registry.md) | CI-built container images published to GHCR, tag-push/dispatch only, public after a manual visibility flip | Accepted |
| [0038](0038-empty-value-label-drop-otlp-otap.md) | Drop empty-valued labels at OTLP and OTAP admission, matching remote-write | Accepted |
| [0039](0039-prometheus-http-api-compat.md) | Prometheus HTTP API compatibility surface for Grafana | Accepted |
| [0040](0040-alerts-and-audit-signals.md) | `Signal::Alerts` and `Signal::Audit`, sharing RLOG's format | Accepted |
| [0041](0041-rspan-v1-span-segment-format.md) | RSPAN v1 span segment format and trace routing | Amended by 0045 |
| [0042](0042-compliance-custody.md) | Compliance-grade custody - legal hold, per-tenant KMS, pluggable auth, verify-custody | Accepted |
| [0043](0043-unified-alerting-engine.md) | Unified alerting engine - observability alerts and detection rules, stored as data | Accepted |
| [0044](0044-query-cost-accounting.md) | Per-query cost accounting, a bounded metrics endpoint, and a two-part pre-execution cost estimate | Accepted |
| [0045](0045-rspan-v2-trace-investigation.md) | RSPAN v2 and v4: pruning columns, a shared codec crate, and a reachable spans table | Accepted |
| [0046](0046-read-cache-tier.md) | A content-addressed read cache at the read funnels, not a store decorator | Accepted |
| [0047](0047-exemplars.md) | Exemplars: an RSEG section, a capped admission, and a correlation surface | Accepted |
| [0048](0048-maintenance-safety-and-coverage.md) | Maintenance safety and coverage: legal hold wired, storage-derived tenant set, mass-orphan circuit breaker, compaction conservation gate | Accepted |
| [0049](0049-rlog-postings.md) | RLOG POSTINGS: exact block-level attribute pruning, opt-in per field | Accepted |
| [0050](0050-fail-closed-isolation-and-startup-invariants.md) | Fail-closed isolation and startup invariants: dedicated mTLS listener, hard tenant_hash mismatch errors, keyed tenant hash default, durable GC config and shard_count, store qualification, readiness store probe | Accepted |
| [0051](0051-tenant-admission-control.md) | Tenant admission control and ingest-time correctness | Accepted |
| [0052](0052-online-resharding.md) | Online resharding: generation-versioned shard_count appended to the provisioning record; no data movement, per-hour scan sets, commit tokens unchanged | Accepted |
| [0053](0053-ci-latency-and-delivery-process-hardening.md) | CI latency and delivery process hardening | Accepted |
| [0054](0054-rspan-v3-bloom-and-service-name.md) | RSPAN v3: block bloom filters and a service_name column | Accepted |
| [0055](0055-storage-credential-scoping.md) | Per-role storage credential scoping | Accepted |
| [0056](0056-catalog-resolve-prefix-list-traversal.md) | Prefix-list traversal for catalog snapshot resolution: a per-shard recursive LIST replacing the per-(shard, hour) loop for wide windows, with a runtime request cap | Accepted |
| [0057](0057-fleet-global-admission-reconciliation.md) | Fleet-global admission via periodic self-owned-key reconciliation | Accepted |
| [0058](0058-commit-record-reconstruction-and-dr-posture.md) | Commit-record reconstruction and DR posture | Amended by 0077 |
| [0059](0059-durability-hardening.md) | Durability hardening: scrub, postings verification, reorder harness | Accepted |
| [0060](0060-query-path-otlp-trace-export.md) | Query-path OTLP trace export | Accepted |
| [0061](0061-query-cost-governance.md) | Query cost governance: per-tenant bytes-scanned budget, fleet-global concurrency ceiling, regex postings pruning | Accepted |
| [0062](0062-encryption-posture-and-evidential-audit.md) | Encryption posture and evidential audit: per-tenant SSE-KMS via key-prefix routing, non-lossy audit pipeline, bounded audit keyspace, PII tokenization | Accepted |
| [0063](0063-multi-part-parallel-fold.md) | Multi-part parallel fold: hour-range-partitioned snapshot parts, parallel fold I/O, one CAS pointer | Accepted |
| [0064](0064-selective-subject-erasure.md) | Selective subject erasure and required bucket lifecycle configuration | Accepted |
| [0065](0065-leased-distributed-maintenance.md) | Leased distributed maintenance: worker membership, rendezvous ownership, durable incremental cursor, bounded RLOG compaction memory | Accepted |
| [0066](0066-format-migration-machinery.md) | Format migration machinery and restart-free tenant lifecycle | Accepted |
| [0067](0067-pipelined-ingest-flushes.md) | Pipelined ingest flushes with adaptive flush delay | Accepted |
| [0068](0068-deterministic-simulation-harness.md) | Deterministic whole-system simulation harness (ravel-sim) | Accepted |
| [0069](0069-global-ingest-memory-bounds.md) | Global ingest memory bounds and idle-tenant state eviction | Accepted |
| [0070](0070-store-request-scheduling-and-perf-gate.md) | Request-class scheduling for object-store traffic (two handles, one weighted scheduler, off by default until panel-sized) and a two-tier CI benchmark gate (exact byte gates hard, criterion compare advisory on the reference runner) | Accepted |
| [0071](0071-distributed-read-fanout.md) | Distributed read fan-out and cross-cluster federation: cost-gated shard-major slice dispatch to heartbeat-registered workers over an internal gRPC surface, plus per-remote federated resolve with `skip_unavailable` partial marking; results byte-identical to local, off by default | Accepted |
| [0072](0072-tenant-scoped-credentials-and-control-plane-protection.md) | Tenant-scoped credentials and control-plane write protection: cryptographic tenant isolation via wired per-tenant KMS, fail-closed bucket-protection startup check, a durable sys/auth owner with revoke-by-tenant, tested IAM templates | Accepted |
| [0073](0073-recent-hours-read-path.md) | Recent-hours read path: open/sealing-hour segments exempt from max_segments, governed by a per-query S3 request budget through one admission seam | Accepted |
| [0074](0074-benchmark-driven-distribution-thresholds.md) | Benchmark-driven distributed-query thresholds | Accepted |
| [0075](0075-shard-aware-query-request-budget.md) | Shard-aware query request budget | Accepted |
| [0076](0076-reducing-s3-request-cost.md) | Reducing S3 request cost without weakening durability | Accepted |
| [0077](0077-dr-posture-and-chaos-evidence.md) | Operator-owned DR via replicated-bucket controls, a rehearsed restore, and a process-kill evidence lane | Accepted |
| [0078](0078-fold-retention-frontier-deployment-default.md) | Fold retention-frontier reconcile honors the deployment-wide retention default | Accepted |
| [0079](0079-indexed-fields-durable-override-cache.md) | Indexed-fields durable override cache: cache-aside overlay over TenantConfig.indexed_fields | Accepted |
| [0080](0080-gateway-api-ingest-affinity.md) | Gateway API exposure and Ravel-native subset affinity: additive backend enum deprecating ingress-nginx, exposure/affinity split, HRW subset selection via a new ravel-affinity crate and ravel-ingest-router service | Accepted |
| [0081](0081-container-first-quickstart.md) | Container-first quickstart: a published-image `docker compose` path as the documented first run, with README command blocks executed in CI | Accepted |
| [0082](0082-provisioning-shard-count-drift-tolerance.md) | Provisioning shard-count drift tolerance for an evolving default | Accepted |
| [0083](0083-alert-sink-auth.md) | Alert sink delivery supports optional credentials | Accepted |
| [0084](0084-otlp-gzip-ingest.md) | Accept gzip-compressed OTLP ingest on HTTP and gRPC, with a decompressed-size cap and an explicit decision on which bytes the ingest byte-rate charges | Accepted |
| [0085](0085-metric-metadata-and-otlp-suffixing.md) | Metric metadata store and OTLP name suffixing | Accepted |
| [0086](0086-github-releases-and-release-hygiene.md) | GitHub Releases, downloadable binaries, and release hygiene | Accepted |
| [0087](0087-streaming-projected-logs-scan.md) | Streaming, column-projecting logs SQL scan | Accepted |
| [0088](0088-operator-configurable-query-budgets.md) | Operator-configurable query budgets | Accepted |
| [0089](0089-bulk-import-logs-signal.md) | Bulk import of structured event data into the logs signal | Accepted |
| [0090](0090-typed-attribute-columns-logs-sql.md) | Typed attribute columns for the logs SQL table | Accepted |
| [0091](0091-maintainer-gated-coderabbit-reviews.md) | Maintainer-gated CodeRabbit reviews: a workflow started by hand or by a `/coderabbit review` comment, which verifies `role_name` is maintain or admin, keeps the credential behind a main-only protected environment, loads policy from main by absolute path, and never executes pull-request code; amendment 2 (2026-08-26) turns the App's automatic review back on for every pull request, with every write-capable surface still off | Proposed |
| [0092](0092-run-merged-l1-and-rseg-v7.md) | Run-merged L1 compaction and RSEG v7: per-sample dedup provenance columns, first timestamp as a delta from the run minimum, no alignment pad on single-sample raw value pages, and three measured page encodings, landed as one version bump | Accepted |
| [0093](0093-typed-column-pushdown-logs.md) | Skip-index and postings pushdown for declared typed logs columns: one resolver dispatching to two existing prune primitives (NumRange for I64/Bool, POSTINGS Equals for Str/Bytes), envelope-range IN, allowlist-only extraction | Proposed |
| [0094](0094-parallel-final-aggregation-exact-typed.md) | Parallel final aggregation for exact-typed inputs | Proposed |
| [0095](0095-numstat-crosstype-declared-column-agreement.md) | NumStat cross-type resolution fix and RLOG v3 | Accepted |
| [0096](0096-queryfrag-per-sample-provenance-and-histograms.md) | Query fan-out frame carries per-sample provenance and histograms | Accepted |
| [0097](0097-sql-scalar-function-surface.md) | The SQL scalar and window function surface: extend the fail-closed registry gate beyond aggregates | Proposed |
| [0099](0099-columnar-decode-to-arrow.md) | Columnar decode-to-Arrow path for SQL scans: a block view out of ravel-logseg, dictionary pages preserved end to end, and SoA buffer adoption on the metrics scan | Accepted |
| [0100](0100-wide-schema-load-and-sql-latency.md) | Wide-schema load validation and SQL query latency measurement: dynamic-column budget counters, declared columns derived from a load mapping, a versioned analytical query corpus, and a cold/warm per-query latency harness | Accepted |
| [0101](0101-declared-column-type-vocabulary.md) | Declarable f64 typed attribute columns: one additive TypedAttrColumnType value, Float64 projection, the NaN pruning rule, and a readers-before-writers rollout | Accepted |
| [0102](0102-intra-segment-scan-partitioning-and-spill-policy.md) | Multi-core SQL execution: intra-segment scan partitioning gated on the read cache, accept ADR-0094's parallel final aggregation in place, disable DataFusion's disk manager so budget exhaustion fails typed, a core-count scaling benchmark | Accepted |
| [0104](0104-ingest-profiling-and-baselines.md) | Per-stage ingest profiling and regression baselines: a feature-gated monotonic timing seam over both ingest pipelines, end-to-end allocation coverage, byte-denominated decode throughput, and one object-store counting implementation | Accepted |
| [0105](0105-substring-pruning-for-like.md) | GRAM_IDX, an optional byte-trigram block-postings section (kind 7) giving sound infix substring pruning for LIKE on opt-in declared Str columns, with the granularity arithmetic that bounds its effectiveness | Accepted |
| [0106](0106-s3-instance-role-credentials.md) | S3 credentials from EC2 IAM instance roles: an explicit auth mode on S3Config so a server on EC2 needs no static keys | Accepted |
| [0107](0107-pruning-proportional-logs-fetch.md) | Pruning-proportional block-range fetches for logs scans: a coalescing RLOG block-range fetcher mirroring SegmentFetcher (etag pinning, per-block cache admission), scoped to block-level pruning since column-level fetch savings need a frozen-format change | Proposed |
| [0699](0699-rlog-row-groups-and-page-directory.md) | RLOG row groups with column-major pages and a PAGE_DIR section: a scan fetches only the projected columns' chunks, per-page checksums keep every read verifiable, trailer version 4 with the version-3 reader kept as N-1 | Proposed |
| [0774](0774-topk-late-materialization-logs-scan.md) | TopK late materialization for the logs scan: a physical optimizer rule splitting a wide `ORDER BY ... LIMIT k` into a narrow row-ref-carrying scan and a k-row block fetch | Proposed |
| [0807](0807-bulk-load-write-concurrency-defaults.md) | Bulk-load write concurrency: audit of every write-path queue-depth bound, the `shards * min(pipeline_depth, max_inflight_flushes)` ceiling, expose `--max-inflight-flushes` on the loader, keep both defaults at 1 until flush cancellation lands | Proposed |
