# Architecture Decision Records

One decision per document. Status: Proposed | Accepted | Superseded.

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
| [0010](0010-spec-amendments-review-1.md) | Spec amendments from the first adversarial design review | Accepted |
| [0011](0011-otap-arrow-ingest.md) | OTAP (OpenTelemetry Arrow) ingest, not generic Arrow Flight | Accepted |
| [0012](0012-simd-policy.md) | SIMD policy: dependencies and autovectorization first, explicit SIMD behind benchmark-gated review | Accepted |
| [0013](0013-arrow-zero-copy-and-datafusion.md) | Targeted Arrow zero-copy, DataFusion for SQL and relational operators only | Accepted |
| [0014](0014-rseg-v2-series-catalog.md) | RSEG v2: compact columnar series catalog, raw-f64 page alignment | Superseded by 0027 |
| [0015](0015-remote-write-ingest.md) | Prometheus Remote Write 1.0/2.0 ingest surface | Accepted |
| [0016](0016-otlp-classic-histogram-summary-explosion.md) | OTLP explicit-bounds histograms and summaries explode to Prometheus-convention series | Accepted |
| [0017](0017-native-histograms-rseg-v3.md) | Native exponential histograms: span-based value model, RSEG v3 after RSEG v2 phase 6 closes | Amended by 0027 |
| [0018](0018-l0-l1-compaction.md) | L0 to L1 compaction: verbatim rewrite of sealed ingest-hour buckets | Amended by 0026, 0027 |
| [0019](0019-age-based-retention.md) | Age-based retention via bucket tombstones and horizon-gated sweep | Accepted |
| [0020](0020-metric-index.md) | Metric index: catalog snapshots as the commit index, async fold, name postings gated | Accepted |
| [0021](0021-promql-full-evaluator.md) | Full PromQL evaluator scope and phasing, differential harness against pinned Prometheus | Accepted |
| [0022](0022-floating-aggregate-exactness.md) | Floating aggregate exactness: allowlisted v1 subset, avg admitted via a sequential UDAF, second-moment family excluded | Accepted |
| [0023](0023-grouped-min-max-total-order-udaf.md) | Grouped MIN/MAX restored via a total-order min/max UDAF replacing the built-ins | Accepted |
| [0024](0024-sum-sequential-fold.md) | Replace the built-in `sum` aggregate with a sequential-fold UDAF | Proposed, not decided |
| [0025](0025-promql-differential-float-precision-residue.md) | PromQL differential float-precision residue: rate/deriv/predict_linear vs. atanh | Accepted |
| [0026](0026-rseg-v5-sparse-id-index.md) | RSEG v5: sparse id index and chunked SERIES_META as the default compaction output | Amended by 0027 |
| [0027](0027-single-rseg-version-pre-release.md) | Single supported RSEG version until first release; v1-v4 support removed | Accepted |
| [0028](0028-analytics-stage.md) | Post-evaluation analytics stage: change point detection and robust statistics in a new ravel-analytics crate | Accepted |
| [0029](0029-rlog-v1-log-segment.md) | RLOG v1: columnar log segment format, a sibling to RSEG | Accepted |
| [0030](0030-promql-subquery-point-cap-divergence.md) | PromQL subquery point-cap divergence from Prometheus: Ravel's documented cap rejects two cases Prometheus succeeds on, by design | Accepted |
| [0031](0031-empty-label-identity.md) | Empty-valued labels absent from series identity everywhere; empty-named labels always rejected | Proposed |
| [0032](0032-rlog-compaction-and-generic-maintain.md) | RLOG compaction, and a signal-generic ravel-maintain | Accepted |
| [0033](0033-sql-query-over-logs.md) | SQL query over logs (log storage phase 3) | Proposed |
| [0034](0034-k8s-operator.md) | Kubernetes operator, kind development environment, and k8s CI lane | Proposed |
| [0035](0035-conformance-scoring.md) | Conformance scoring: three-state classification over the full PromQL and SQL surfaces, scored on the claimed subset | Proposed |
| [0036](0036-performance-investigation-methodology.md) | Performance investigation methodology and scope | Accepted |
| [0037](0037-container-image-ci-registry.md) | CI-built container images published to GHCR, tag-push/dispatch only, public after a manual visibility flip | Proposed |
| [0038](0038-empty-value-label-drop-otlp-otap.md) | Drop empty-valued labels at OTLP and OTAP admission, matching remote-write | Accepted |
| [0039](0039-prometheus-http-api-compat.md) | Prometheus HTTP API compatibility surface for Grafana | Proposed |
| [0040](0040-alerts-and-audit-signals.md) | `Signal::Alerts` and `Signal::Audit`, sharing RLOG's format | Accepted |
| [0041](0041-rspan-v1-span-segment-format.md) | RSPAN v1 span segment format and trace routing | Amended by 0045 |
| [0042](0042-compliance-custody.md) | Compliance-grade custody - legal hold, per-tenant KMS, pluggable auth, verify-custody | Accepted |
| [0043](0043-unified-alerting-engine.md) | Unified alerting engine - observability alerts and detection rules, stored as data | Accepted |
| [0048](0048-maintenance-safety-and-coverage.md) | Maintenance safety and coverage: legal hold wired, storage-derived tenant set, mass-orphan circuit breaker, compaction conservation gate | Accepted |
| [0044](0044-query-cost-accounting.md) | Per-query cost accounting, a bounded metrics endpoint, and a two-part pre-execution cost estimate | Accepted |
| [0045](0045-rspan-v2-trace-investigation.md) | RSPAN v2 and v3: pruning columns, a shared codec crate, and a reachable spans table | Accepted |
| [0046](0046-read-cache-tier.md) | A content-addressed read cache at the read funnels, not a store decorator | Accepted |
| [0047](0047-exemplars.md) | Exemplars: an RSEG section, a capped admission, and a correlation surface | Accepted |
| [0049](0049-rlog-postings.md) | RLOG POSTINGS: exact block-level attribute pruning, opt-in per field | Accepted |
| [0051](0051-tenant-admission-control.md) | Tenant admission control and ingest-time correctness | Accepted |
| [0050](0050-fail-closed-isolation-and-startup-invariants.md) | Fail-closed isolation and startup invariants: dedicated mTLS listener, hard tenant_hash mismatch errors, keyed tenant hash default, durable GC config and shard_count, store qualification, readiness store probe | Proposed |
| [0052](0052-online-resharding.md) | Online resharding: generation-versioned shard_count appended to the EC5 provisioning record; no data movement, per-hour scan sets, commit tokens unchanged | Accepted |
| [0056](0056-catalog-resolve-prefix-list-traversal.md) | Prefix-list traversal for catalog snapshot resolution: a per-shard recursive LIST replacing the per-(shard, hour) loop for wide windows, with a runtime request cap | Accepted |
| [0072](0072-tenant-scoped-credentials-and-control-plane-protection.md) | Tenant-scoped credentials and control-plane write protection: cryptographic tenant isolation via wired per-tenant KMS, fail-closed bucket-protection startup check, a durable sys/auth owner with revoke-by-tenant, tested IAM templates | Accepted |
| [0073](0073-recent-hours-read-path.md) | Recent-hours read path: open/sealing-hour segments exempt from max_segments, governed by a per-query S3 request budget through one admission seam | Accepted |
| [0070](0070-store-request-scheduling-and-perf-gate.md) | Request-class scheduling for object-store traffic (two handles, one weighted scheduler, off by default until panel-sized) and a two-tier CI benchmark gate (exact byte gates hard, criterion compare advisory on the reference runner) | Accepted |
| [0071](0071-distributed-read-fanout.md) | Distributed read fan-out and cross-cluster federation: cost-gated shard-major slice dispatch to heartbeat-registered workers over an internal gRPC surface, plus per-remote federated resolve with `skip_unavailable` partial marking; results byte-identical to local, off by default | Proposed |
