# Architecture Decision Records

One decision per document. Status: Proposed | Accepted | Superseded.

Numbering: ADR 0001 through 0109 are sequential, with no 0014 and no 0091
(0091 recorded the authorization boundary for a review integration that was
removed outright; ADR-1586 replaces it). From ADR 0110 onward the
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
| [0022](0022-floating-aggregate-exactness.md) | Floating aggregate exactness: allowlisted v1 subset, avg admitted via a sequential UDAF, second-moment family excluded | Amended by 0825 |
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
| [0092](0092-run-merged-l1-and-rseg-v7.md) | Run-merged L1 compaction and RSEG v7: per-sample dedup provenance columns, first timestamp as a delta from the run minimum, no alignment pad on single-sample raw value pages, and three measured page encodings, landed as one version bump | Accepted |
| [0093](0093-typed-column-pushdown-logs.md) | Skip-index and postings pushdown for declared typed logs columns: one resolver dispatching to two existing prune primitives (NumRange for I64/Bool, POSTINGS Equals for Str/Bytes), envelope-range IN, allowlist-only extraction | Proposed |
| [0094](0094-parallel-final-aggregation-exact-typed.md) | Parallel final aggregation for exact-typed inputs | Amended by 0825 |
| [0095](0095-numstat-crosstype-declared-column-agreement.md) | NumStat cross-type resolution fix and RLOG v3 | Accepted |
| [0096](0096-queryfrag-per-sample-provenance-and-histograms.md) | Query fan-out frame carries per-sample provenance and histograms | Accepted |
| [0097](0097-sql-scalar-function-surface.md) | The SQL scalar and window function surface: extend the fail-closed registry gate beyond aggregates | Proposed |
| [0098](0098-shared-label-string-representation.md) | Share one label set per series run | Proposed |
| [0099](0099-columnar-decode-to-arrow.md) | Columnar decode-to-Arrow path for SQL scans: a block view out of ravel-logseg, dictionary pages preserved end to end, and SoA buffer adoption on the metrics scan | Accepted |
| [0100](0100-wide-schema-load-and-sql-latency.md) | Wide-schema load validation and SQL query latency measurement: dynamic-column budget counters, declared columns derived from a load mapping, a versioned analytical query corpus, and a cold/warm per-query latency harness | Accepted |
| [0101](0101-declared-column-type-vocabulary.md) | Declarable f64 typed attribute columns: one additive TypedAttrColumnType value, Float64 projection, the NaN pruning rule, and a readers-before-writers rollout | Accepted |
| [0102](0102-intra-segment-scan-partitioning-and-spill-policy.md) | Multi-core SQL execution: intra-segment scan partitioning gated on the read cache, accept ADR-0094's parallel final aggregation in place, disable DataFusion's disk manager so budget exhaustion fails typed, a core-count scaling benchmark | Accepted |
| [0103](0103-order-insensitive-aggregation-pushdown.md) | Order-insensitive aggregation pushdown | Accepted (amends ADR-0071) |
| [0104](0104-ingest-profiling-and-baselines.md) | Per-stage ingest profiling and regression baselines: a feature-gated monotonic timing seam over both ingest pipelines, end-to-end allocation coverage, byte-denominated decode throughput, and one object-store counting implementation | Accepted |
| [0105](0105-substring-pruning-for-like.md) | GRAM_IDX, an optional byte-trigram block-postings section (kind 7) giving sound infix substring pruning for LIKE on opt-in declared Str columns, with the granularity arithmetic that bounds its effectiveness | Accepted |
| [0106](0106-s3-instance-role-credentials.md) | S3 credentials from EC2 IAM instance roles: an explicit auth mode on S3Config so a server on EC2 needs no static keys | Accepted |
| [0107](0107-pruning-proportional-logs-fetch.md) | Pruning-proportional block-range fetches for logs scans: a coalescing RLOG block-range fetcher mirroring SegmentFetcher (etag pinning, per-block cache admission), scoped to block-level pruning since column-level fetch savings need a frozen-format change | Proposed |
| [0108](0108-range-eval-over-native-histograms.md) | Histogram-aware range evaluation for PromQL | Proposed |
| [0109](0109-columnar-bulk-load-fast-path.md) | Columnar bulk-load fast path for Parquet ingest | Accepted |
| [0110](0110-columnar-spans-scan.md) | Columnar decode-to-Arrow for the SQL spans scan | Proposed |
| [0117](0117-per-series-alert-evaluation.md) | A PromQL rule raises one alert per matched series, identity over series labels (minus `__name__`) overlaid by rule labels, capped at 1000 alerts per rule with a typed error above it, and series that stop matching resolve; SQL rules and every format unchanged. Amends ADR-1294 | Accepted (2026-09-16) |
| [0531](0531-format-lifecycle-activation-milestone.md) | What "first public release" means for the format-lifecycle policy: it is the v1.0 release, distinct from the software's 0.9.0 release, so ADR-0027's pre-release regime holds until v1.0 and the N/N-1 reader window does not open before it; records the format-bump rollback stance | Accepted |
| [0593](0593-l2-cross-hour-exact-compaction-tier.md) | L2 is a run-merging rewrite of consecutive sealed hours' L1 outputs using ADR-0815 decision 7's replicated, version-2 cross-hour record with tombstone deferral; erasure rewrite replication is a precondition and the achievable factor is stated as a bound. Amends ADR-0018. Design direction only, implementation not scheduled | Accepted (2026-09-16) |
| [0699](0699-rlog-row-groups-and-page-directory.md) | RLOG row groups with column-major pages and a PAGE_DIR section: a scan fetches only the projected columns' chunks, per-page checksums keep every read verifiable, trailer version 4 with the version-3 reader kept as N-1 | Proposed |
| [0716](0716-series-value-kind-migration.md) | Series value-kind migration: maintenance merges group by (series_id, value_kind) with kind-homogeneous output parts, unblocking compaction (#716) and selective erasure (#789) over migrated series; identity, RSEG layout, and the ingest contract unchanged; the within-buffer mismatch remaps to 400/INVALID_ARGUMENT | Proposed |
| [0774](0774-topk-late-materialization-logs-scan.md) | TopK late materialization for the logs scan: a physical optimizer rule splitting a wide `ORDER BY ... LIMIT k` into a narrow row-ref-carrying scan and a k-row block fetch | Proposed |
| [0807](0807-bulk-load-write-concurrency-defaults.md) | Bulk-load write concurrency: audit of every write-path queue-depth bound, the `shards * min(pipeline_depth, max_inflight_flushes)` ceiling, expose `--max-inflight-flushes` on the loader, keep both defaults at 1 until flush cancellation lands | Proposed |
| [0815](0815-clustered-compaction-and-object-pruning.md) | Clustered compaction and object-level pruning: compaction sorts the merge by a time-leading clustering key and cuts parts on its boundaries so per-part bounds on each `CompactionPart`/`SegmentRef` go narrow, reusing the existing per-part event-bound exclusion plumbing (additive per-part min/max bound fields plus key descriptors for a non-time override key), no segment-format bump | Proposed |
| [0825](0825-grouped-aggregation-accumulator-path.md) | Grouped aggregation accumulator path: flat per-group GroupsAccumulators replace the per-group boxed-accumulator adapter, and integer-input avg moves to exact i128 accumulation, deterministic by construction and admitted to the parallel final | Accepted |
| [0849](0849-snapshot-bound-index-plane.md) | A snapshot-bound index plane on object storage | Accepted |
| [0850](0850-logs-typed-column-statistics.md) | exact per-object statistics for declared logs columns | Accepted. Builds on ADR-0090 (declared typed attribute columns) |
| [0873](0873-commit-record-declared-min-max.md) | Per-declared-column min/max (and null count) hoisted onto commit records, compaction parts, snapshot entries, and SegmentRef, so DataFusion's stock AggregateStatistics rule answers MIN/MAX/COUNT from statistics on live tails and token-resolved segments; additive fields 20/12/15, no version bump, eligibility allowlisted to I64/Bool with an explicit float gate, union reader over stamps and .cstat | Proposed |
| [0892](0892-drop-rlog-version-3-reader.md) | Drop the RLOG version-3 reader | Proposed |
| [0904](0904-request-cost-latency-knob.md) | an operator knob for the request-cost vs latency trade | Proposed |
| [0913](0913-declared-exact-materialisation.md) | Declared exact materialisations: tenant-declared aggregate shapes materialised as immutable, snapshot-bound per-part monoid states under a definition hash, selected only on provable answerability with fail-open fallback to scan, a 64 MiB materialised-plan byte budget, exact-until-budget `COUNT DISTINCT`, and float `SUM`/`AVG` deferred behind ADR-0024 | Proposed |
| [0927](0927-metricsbench-benchmark-contract.md) | MetricsBench, a reproducible metrics and PromQL benchmark contract | Proposed. Issue #927, task T1 of the epic. Builds on ADR-0044 |
| [0942](0942-cstat-snapshot-part-binding.md) | re-key `.cstat` column statistics to snapshot-part binding | Accepted (2026-08-30). Builds on ADR-0850 |
| [0954](0954-bounded-ephemeral-spill.md) | Bounded ephemeral spill for eligible SQL operators | Accepted (2026-08-30) |
| [0979](0979-bounded-memory-rlog-compaction-merge.md) | Bounded-memory RLOG compaction merge | Proposed |
| [0996](0996-request-cost-aware-fetching.md) | request-cost-aware fetching and the S3 request ledger | Proposed |
| [1029](1029-advisory-compaction-claims.md) | advisory compaction claims over object-store CAS | Proposed |
| [1040](1040-documentation-architecture.md) | Documentation architecture, canonical vocabulary, and a docs gate | Proposed |
| [1101](1101-alerts-and-audit-sql-tables.md) | Register the `alerts` and `audit` SQL tables | Accepted (2026-09-03) |
| [1103](1103-promql-over-logs.md) | PromQL over logs: `ravel_log_lines` and `ravel_log_bytes` | Accepted |
| [1113](1113-tla-verification-suite.md) | TLA+ verification suite for the commit, catalog, lifecycle, resharding, and maintenance protocols | Proposed |
| [1170](1170-process-memory-budget.md) | One process-wide memory budget: a `MemoryBudget` accountant the tenant accountants adapt to, byte reservations at the fetch layer where the unit is known, a static carve under one number, and the aggregate logged and gauged | Proposed |
| [1195](1195-unbundle-fetch-concurrency.md) | Unbundle `--fetch-concurrency`, and make GET concurrency a process-wide limit | Proposed |
| [1196](1196-fetch-objective-cost-first-default-latency-first-policy.md) | Keep the cost-first fetch default, add a latency-first policy | Proposed |
| [1199](1199-bounded-query-io-and-tail-checkpoints.md) | Bounded query I/O accounting, and a pre-registered measured gate deciding whether an L0.5 tail checkpoint gets built at all | Proposed |
| [1294](1294-alert-state-memo.md) | A durable, derived alert-state memo per tenant that bounds each alert evaluation tick to a memo GET, a lease GET, one tail LIST, and the transitions since the memo, instead of re-folding the whole `Signal::Alerts` history every tick | Proposed |
| [1295](1295-per-local-tenant-federation-credentials.md) | Key each `--remote-cluster` credential by the one local tenant it belongs to, with `Federation::fetch` selecting remotes by the caller's tenant before dispatch, so a multi-tenant coordinator can federate at all; an unmapped local tenant gets local data only reported complete, and an unkeyed remote is still refused on a coordinator resolving more than one local tenant. Amends ADR-0071 | Accepted (2026-09-14) |
| [1302](1302-cas-guarded-re-record-of-a-stale-qualification.md) | Make the `sys/qualification` record re-recordable once per bucket per suite version instead of write-once, so a `CONFORMANCE_SUITE_VERSION` bump does not strand already-qualified buckets, and guard the replacing write with `CasVersion` on the read version so concurrent `qualify` runs from a newer binary cannot be silently downgraded. Supersedes ADR-0050 section 6 | Accepted (2026-09-12) |
| [1307](1307-per-writer-monotonic-flush-clock.md) | Per-writer monotonic flush clock: each shard actor raises the flush-open `created_unix_ns` stamp to a per-writer in-process monotonic floor so a backwards wall-clock step cannot invert query-time duplicate resolution, counting each absorbed step; the floor is per-process only and does not order a restarted writer against its predecessor (`writer_id` is not part of the dedup comparator, so it does not close this), no format change | Proposed |
| [1331](1331-rewrite-parts-and-the-format-floor.md) | A live rewrite record whose parts sit below the migration target keeps blocking the format floor and is reported as a blocked bucket with the reason `RewriteParts`, never migrated by the migration job, since a superseding record set over an erased bucket is the ADR-0064 two-record-set hazard. Amends ADR-0066 decision 4 and decision 5 | Accepted (2026-09-16) |
| [1374](1374-agent-mcp-server.md) | A first-party MCP server behind a shared query service layer, with a bounded, evidence-bearing agent result contract | Proposed |
| [1413](1413-split-cstat-per-part.md) | Split `.cstat` column statistics per snapshot part, so a wide table's statistics fit the reader's guard and the process memory budget, with a per-part ceiling the writer also enforces | Accepted (2026-09-08) |
| [1586](1586-fleet-review-bot-as-the-review-path.md) | The fleet review bot is Ravel's agent review path: `@claude-fleet review` as the trigger, the previous third-party integration and its repository-side control plane deleted outright, a `write`-level trigger accepted on the record with the role gate filed on the bot, and a missing review classified into five states rather than one | Accepted |
| [1642](1642-flush-permit-acquired-in-the-flush-task.md) | Acquire the `max_inflight_flushes` permit inside the spawned flush task rather than on the shard actor, in all three ingest pipelines, so a stalled flush cannot stop a co-resident tenant's channel drain or age trigger; backpressure at the bound moves to the ADR-0069 byte budget and the in-flight gauge counts permit-waiting flushes. Supersedes ADR-0067 decision 2 | Accepted (2026-09-12) |
| [1658](1658-documentation-claims-registry.md) | A YAML claims registry binds verbatim sentences in the normative docs to code and test symbols, with a stdlib gate on quote presence, symbol definition, unregistered negative-claim lines and unissued contradictions, seeded from the review's claim matrix. Amends ADR-1040 | Accepted (2026-09-16) |
| [1685](1685-writer-clock-skew-refusal-against-store-time.md) | Observe the store's clock from response `Date` headers in the S3 HTTP service and refuse a flush whose raw clock reading lags it by more than `clock_skew_allowance`, one-sided and skipped-but-counted when no observation exists, so a slow writer gets a retryable 503 instead of publishing into a sealed hour. Amends ADR-1307 | Accepted (2026-09-16) |
| [1686](1686-scrub-resume-with-a-start-after-marker.md) | The scrub cursor becomes a start-after marker over the hour-ordered commit shard prefix, listed with `list_after` and cut off at the per-tick budget, so `--scrub-period` bounds the metadata half of a tick as well as the content reads, with no persisted corpus and no new object. Amends ADR-0059 decision 1 | Accepted (2026-09-16) |
| [1688](1688-alert-signal-retention.md) | Alert transitions get a hard-scoped, tombstone-free age sweep shaped like the query-audit sweep, default 90 days via `--alert-retention`, that keeps each identity's current-state record named by the alert state memo and skips a tenant whose memo is unusable, so a cold-start fold over the survivors equals a full fold | Accepted (2026-09-16) |
| [1689](1689-flight-sql-slice-capability-and-fragment-listener-consolidation.md) | Move Flight SQL slice DoGet onto the dedicated TLS fragment listener in a `SliceOnly` role, make the slice ticket the expiring capability under its own `--sql-ticket-key-file` with context-derived keys, take the tenant from the ticket instead of a forwarded client credential, bump `PROTOCOL_VERSION` to 5, and delete `FragmentListenerRole::Combined` one release after the operator renders the dedicated listener. Amends ADR-0071 | Accepted (2026-09-16) |
| [1692](1692-per-shard-ingest-skew-metrics.md) | Narrow ADR-0044 rejected alternative 6: a per-shard ingest skew family with no tenant and no operation label is permitted, bounded by `--shards`, with `shard` joining the closed `Label` enum as a `u32` and every configured shard rendered, idle ones included. Amends ADR-0044 | Accepted (2026-09-16) |
| [1693](1693-partitioned-catalog-fold-ownership.md) | Move the scheduled catalog fold to the maintain role, gated on `owns_unit` for shard 0 of each (tenant, signal) under the existing maintain live set, with no new heartbeat prefix; gateway and query processes stop folding, `all` keeps folding unpartitioned so the quickstart is unchanged. Amends ADR-0065 decision 2 | Accepted (2026-09-16) |
| [1696](1696-commit-record-integrity-through-store-checksums.md) | Commit-family record integrity comes from the store transport, not a framed record: every PUT carries a server-verified CRC64-NVME checksum by default and every full-object GET verifies the body against the stored checksum, returning `Corrupted` on mismatch and counting reads with no checksum; the record layout is unchanged | Accepted (2026-09-16) |
| [1713](1713-resumable-bulk-import-marker.md) | A load marker at `t/<th>/<signal>/idem/<filedigest32>.ldm` holds per-cursor committed row offsets, created with `CreateIfAbsent` and advanced by `CasVersion` after every acked batch, so a rerun resumes or refuses; class C, CAS-mutable. Amends ADR-0089 | Accepted (2026-09-16) |
| [1727](1727-bucket-protection-verification.md) | Verify bucket protection through hand-rolled read-only SigV4 GETs over the existing reqwest pin behind a sibling `BucketControlPlane` trait on `S3Store`, a `ravel-cli store verify-protection` subcommand whose exit codes separate failed from unverifiable, and `conditions_failed` and `conditions_unknown` gauges for the startup gate. Amends ADR-0042 decision 3 and ADR-0072 decision 3 | Accepted (2026-09-16) |
| [1731](1731-operator-health-metrics-and-topology.md) | Single replica with `Recreate` is the supported operator topology; a hyper listener serves `/healthz`, `/readyz` and `/metrics` with four metrics, liveness means the controller task is running, and the `Deployment` gains probes. Amends ADR-0034 | Accepted (2026-09-16) |
| [1733](1733-catalog-resolve-request-concurrency-derivation.md) | Keep 128 as the per-prefix catalog resolve bound and add a per-process ceiling derived from query concurrency (clamp of Q x 128 to 4,096), with in-flight bytes bounded by memory-budget reservation and the limiter kept separate from ADR-1195's `GetLimiter` | Accepted (2026-09-16) |
| [1737](1737-idle-flush-byte-floor-and-buffered-durability-window.md) | Add `idle_flush_byte_floor` (default 0, today's behaviour): below the floor a buffer with no strict waiter waits `max_flush_lifetime`, stated as a one-hour buffered-mode loss window the operator opts into, with the shutdown-drain residue and memory occupancy named. Amends ADR-0051 section 7 and ADR-0076 decision 4 | Accepted (2026-09-16) |
| [1746](1746-format-floor-evidence-and-writer-policy.md) | A format floor records its observation basis (entries, newest `created_unix_ns`, shards) as additive fields under a two-release `ProvisioningRecord` version bump, `audit-versions` classifies a stored floor as `Current`, `Stale`, `Contradicted` or `Unknown`, and a writer whose newest version sits below a floor warns at startup and refuses only in the write path, with an explicit override that contradicts the floor. Amends ADR-0066 decision 3 | Accepted (2026-09-16) |
| [1751](1751-bulk-import-and-export-for-all-signals.md) | `ravel-cli load` takes `--signal` with a per-signal mapping section, historical metric samples bucket by load time, and a Parquet export mirrors load as a store read at a snapshot so `load(export(x))` round-trips every mapped field. Amends ADR-0089 | Accepted (2026-09-16) |

## Amending an ADR

A decision is never edited in place once it is Accepted. The change goes in
an amendment section at the end of the file, and the earlier text it
changes gets an inline pointer to that section, so a reader who lands on
the decision learns it has moved on.

`scripts/guards/check-amendment-integrity.sh` checks the amendment's claim
about its own effect against the document. Every amendment heading (any
heading below the title whose text contains a word starting "amend" or
"correction", at any level) must carry at least one marker in its own
block, an HTML comment saying what the amendment did. A marker is one
line: the guard reads it with a single-line pattern, so a marker wrapped
across two lines reads as no marker at all.

### The markers

Nothing was retired, so nothing points here:

```markdown
## Amendment (2026-09-07): the loopback refusal covers every listener the shared resolver backs

<!-- amendment-applies: none reason="the Decision's loopback refusal stands exactly as written; this records an implementation that did not honour it and the tightened guard, and retires no earlier wording" -->
```

The reason is required. `none` is the marker that turns the checks off, so
it is the one that has to justify itself: use it when the amendment adds a
decision, records an outcome, resolves an open item, rejects a proposal, or
amends a different document. If it supersedes, retires, replaces, narrows,
or corrects earlier text in this ADR, it is not `none`.

Named sections carry an inline pointer to the amendment:

```markdown
## Amendment: the queued-flush cap (issue #1740)

<!-- amendment-applies: sections="Consequences|Rejected alternatives" pointer="queued-flush cap amendment" -->
```

Each named heading must exist exactly once in the file, matched on its exact
text, and its own prose must contain `pointer` somewhere. `pointer` is free
text, matched case-insensitively with whitespace collapsed, so the
parenthetical an author already writes ("see the queued-flush cap amendment
below") is what the marker names. `|` separates the names; a heading
carrying one in its own text is named with `\|`:

```markdown
<!-- amendment-applies: sections="2. The fetch policy: `request-minimal \| byte-minimal \| cost-based`" pointer="latency-first amendment" -->
```

A wording the amendment retires must not survive elsewhere unqualified:

```markdown
<!-- amendment-supersedes: phrase="`max_inflight_flushes` binds first for per-shard overlap" pointer="#800 amendment" -->
```

Every occurrence outside the amendment's own block must be qualified: the
pointer appears in the same sentence (formally, on the match's own lines,
the line above, or the line below), or an
`<!-- amendment-supersedes-allow: <reason> -->` marker with a non-empty
reason sits in that window, for prose that cites the retired wording on
purpose.

An amendment may carry several markers. Marker comments are not prose: they
never qualify a phrase and never count as an occurrence of one.

### What the exit codes mean

- **0**: every claim the markers make is true of the document.
- **1**: a finding, something the document says that is not true of it: a
  named section without its pointer, a retired phrase still standing
  unqualified, or `amendment-applies: none` with no reason.
- **70**: the claim could not be checked at all, which is not a pass: an
  amendment heading with no marker, a marker line that does not parse
  (wrapped across lines, or a misspelled name), a marker missing `sections=`,
  `pointer=` or `phrase=`, an empty `sections=`, a named heading that does
  not exist or exists more than once, no such directory, no ADR files, or
  zero amendments scanned. Fix the marker or the heading it names; do not
  leave it at 70.
- **64**: bad usage (more than one argument, or a directory outside the
  repository).
