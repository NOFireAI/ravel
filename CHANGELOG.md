# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Prometheus alert rules ship in `deploy/prometheus/ravel.rules.yaml`**
  (issue #1730). `deploy/` previously held one dashboard graphing host CPU,
  no rule file and no `PrometheusRule` manifest, so about twenty alert
  conditions lived only as prose rows in the troubleshooting guide and four
  complete rules inside fenced blocks in the observability guide. The shipped
  file carries those conditions with the thresholds and durations the guides
  state. A test parses it and asserts every metric it names appears in a
  rendered `/metrics` body, so a metric rename fails the build instead of
  leaving a rule that matches no series and pages nobody, and it compares the
  observability guide's reprinted copies against the shipped file so the two
  cannot drift.

- **`GET /api/v1/rules` serves the loaded alert rules in the Prometheus rules
  shape** (issue #1711). A process running an alert evaluator (`all` or `query`
  mode) now answers the Prometheus rules API with the rule set it parsed at
  startup, as one group per tenant named `ravel-alert-rules`, so an operator
  can confirm which rules a process is actually evaluating without reading the
  file on its host. The tenant comes from the request's credential, and a
  tenant with no rules gets an empty group list rather than anybody else's
  rules. Each rule's `query` is its whole firing expression, with a PromQL
  rule's threshold comparison appended to the query text. `health` and `state`
  are reported as `unknown` and a rule carries no `alerts` array, because the
  endpoint serves the loaded configuration and reads no evaluation outcome;
  `/api/v1/alerts` is not served.

- **A Grafana dashboard over Ravel's own metrics ships in
  `deploy/grafana/dashboards-standalone/ravel.json`** (issue #1730). `deploy/` shipped a
  rule file that pages on 37 metric names and one quickstart dashboard that
  graphs host CPU, so an operator who got paged had nothing to open. The new
  dashboard carries 36 panels in 6 rows (ingest, query, catalog fold,
  maintenance, object store and probe, alerting) over 107 distinct `ravel_`
  names, on a data-source variable rather than a hardcoded uid. The test that
  pins the rule file's names now pins the dashboard's too: it extracts metric
  names from each panel target's PromQL expression and asserts each one appears
  on a `# TYPE` line of a rendered `/metrics` body, through the same scanner and
  the same rendered bodies the rule file goes through, and it asserts that every
  name the shipped rules alert on is graphed by some panel. The check covers
  metric names; it does not validate the PromQL around them or the dashboard
  against Grafana's schema.

- **`ravel_store_probe_last_run_timestamp_seconds` gauge for the store-probe
  task's own liveness** (issue #1728). The background store-reachability
  probe (`store_probe::spawn`) runs as a single `tokio::spawn` with no restart
  path and no `JoinHandle` observation; if it dies, `ravel_store_reachable`
  and `ravel_store_probe_failures_total` freeze at their last values and
  `/readyz` reads that stale state as healthy forever. The new gauge is set
  from an injected clock at the end of every completed probe cycle, whatever
  its outcome, and once by `store_probe::spawn` before the loop's first sleep,
  so its AGE (not its value) is the signal that the task itself has stopped: a
  failing-but-alive probe keeps advancing it every cycle, and a task that dies
  before its first cycle ages out from its spawn stamp. A single shipped alert
  covers every dead-probe state:
  `RavelStoreProbeStalled` in `deploy/prometheus/ravel.rules.yaml` fires on
  `time() - ravel_store_probe_last_run_timestamp_seconds > 132` held for
  `5m`, with no sentinel guard term and no companion never-ran rule.
  `docs/guides/observability.md` documents the alert and derives its
  threshold from the probe interval, noting that it must be recomputed for a
  non-default `--store-probe-interval`.

- **`ravel-bench`'s ingest and end-to-end reports break out queue-deadline
  abandonment as its own `abandoned_queue_deadline` counter instead of
  leaving it unreported** (issue #1823). `ingest_bench` and `s3_e2e_bench`
  already reported `abandoned_retry_exhausted` and `abandoned_input_rejected`
  on the `abandoned` line of their human-readable and JSON report output, but
  a flush `ravel-ingest`'s pre-acquire queue-deadline guard abandons before
  any store call had no field in either `Report` type and was silently
  absent from bench output. Both `Report` types
  (`ravel_bench::ingest::Report`, `ravel_bench::e2e::Report`) now carry the
  field, both `run()` implementations copy it from the router's
  `IngestMetricsSnapshot`, and both bins render it on the `abandoned` line,
  pinned by `ingest_bench::tests::abandoned_queue_deadline_appears_in_rendered_report`
  and `s3_e2e_bench::tests::abandoned_queue_deadline_appears_in_rendered_report`.
  `ravel_bench::ingest::tests::queue_deadline_abandonment_is_reported_under_its_own_reason`
  and the equivalent test in `ravel_bench::e2e` assert the exact counter
  split (`abandoned_queue_deadline == 1`, `abandoned_retry_exhausted == 0`,
  `abandoned_input_rejected == 0`) for a flush whose deadline has already
  elapsed while queued for a permit.

- **A teardown drain that loses acknowledged buffered rows, or overruns
  `--shutdown-timeout`, is now visible on `/metrics`** (issue #1742).
  `ravel_ingest_flush_all_residue_tenants_total` renders the residual tenant
  count a `DrainIntent::Teardown` flush already logged at ERROR, for all
  three ingest signals; `ravel_shutdown_drain_overrun_total` counts graceful
  shutdowns that ran past their timeout, incremented only on that branch. The
  client-facing listener (which also serves `/metrics`) stops accepting new
  connections before either value can change during the shutdown that sets
  it, so a live scrape is unlikely to observe the exact event; see
  [Reachability during shutdown](docs/guides/observability.md#reachability-during-shutdown).
  The accompanying log line remains the reliable single-event signal.

- **The operator-facing record-cache figures in `docs/guides/caching.md`,
  `docs/guides/operations.md` and `docs/catalog-and-mvcc.md` are checked
  against the `ravel-catalog` constants they are derived from** (issue
  #1927). Issue #1904 let the derived per-tenant capacity change while five
  prose restatements of the capacity, the per-entry byte rate, the memory
  budget and the derived entry cap drifted out of sync with each other and
  with the shipped constants, which would have led an operator sizing a host
  from the docs to under-provision. A new test,
  `ravel-catalog`'s `operator_docs_record_cache_figures.rs`, computes each
  figure from `RECORD_CACHE_ENTRY_BYTES`, `RECORD_CACHES_PER_TENANT`,
  `MAX_RECORD_CACHE_BYTES_PER_TENANT`, `DEFAULT_CACHE_CAPACITY_PER_TENANT`
  and `MAX_CACHE_CAPACITY_PER_TENANT` and asserts the three guides state
  exactly that value, so the next constant change fails in this file instead
  of shipping stale prose. Figures the guides state more than once are
  pinned by an occurrence count, so a restatement in different words cannot
  drift unpinned.

- **A cargo-free guard checks that every operator-facing record-cache figure
  matches the constants it is derived from** (issue #1927).
  `scripts/guards/check-doc-figures.sh` re-derives each figure from
  `RECORD_CACHE_ENTRY_BYTES`, `RECORD_CACHES_PER_TENANT`,
  `MAX_RECORD_CACHE_BYTES_PER_TENANT` and
  `DEFAULT_CACHE_CAPACITY_PER_TENANT`, then counts every occurrence in the
  three guides and in `--disable-cache`'s long help, so a restatement that
  drifts, or one nobody accounted for, fails in under a second rather than in
  CI. The cargo tests that pin the same figures still run in CI; this is the
  half that runs where cargo does not.

### Changed

- **The quickstart and CI object store moved from MinIO to RustFS, and the
  `mc` client to the AWS CLI** (issue #2001). MinIO withdrew anonymous access
  to its public images on 2026-09-24, from Docker Hub and quay.io alike, so
  every compose file, Kubernetes manifest and CI job that started MinIO failed
  at `docker run` with `unauthorized`, and a mirror is no fix because the
  images cannot be pulled without credentials at all. The object store is now
  `ghcr.io/rustfs/rustfs:1.0.0`, the S3 client is
  `public.ecr.aws/aws-cli/aws-cli:2.37.2`, and both stay pinned by digest;
  neither registry shares Docker Hub's per-IP anonymous pull allowance, though
  ECR Public throttles unauthenticated pulls per source IP on its own, so the
  CI steps that pull the client retry a throttled pull with backoff. An
  operator running the old quickstart compose file should stop the stack
  (`docker compose -f deploy/docker-compose/ravel.yml down`), pull the current
  files, and bring the stack back up. RustFS starts from an empty
  `rustfs-data/`, and nothing here establishes that it can read a MinIO data
  directory, so treat `minio-data/` as local development data to delete rather
  than to rename; the store-qualify one-shot qualifies the fresh bucket. The
  endpoint, bucket name and development credentials are unchanged, so nothing
  else in a local configuration moves. `make minio` and `make minio-down` are
  now `make rustfs` and `make rustfs-down`, `deploy/docker-compose/minio.yml` is
  `deploy/docker-compose/rustfs.yml`, `deploy/k8s/minio.yaml` is
  `deploy/k8s/rustfs.yaml`, and `RAVEL_FAKE_S3_BACKEND=minio` is
  `RAVEL_FAKE_S3_BACKEND=rustfs`. The `RAVEL_MINIO_*` variables that gate the
  object-store contract test, and the `minio_contract` test name itself, keep
  their names: they name the gate rather than the vendor, and every checkout
  and lane that sets them would otherwise break.

- **The shipped admission defaults are now one value, `ravel-ingest`'s
  `AdmissionLimits::default()`, instead of two that had drifted** (issue #23).
  **No deployment's effective limits move.** A shipped `ravel-server` already
  applied 200,000 for `max_active_series` and `max_active_streams`, and it
  still does; what changed is where that number lives. `ravel-server`'s
  `config::limits::shipped_defaults` built its own `AdmissionLimits` literal
  and never called the `Default` impl, so `ravel-ingest` kept 1,000,000 for
  every other caller with nothing failing when the two disagreed. The library
  constants are now 200,000 and the server returns them, so an embedder
  building an `AdmissionController` from `AdmissionLimits::default()` gets the
  caps the server ships rather than a 5x looser pair. A test asserts the two
  are equal field by field.

- **ADR-0051 section 2's per-entry memory estimate is corrected from about 16
  bytes to the measured 35 to 56 bytes** (issue #22). The worst case also
  multiplies by the two tracked signals, not just the two rotating epochs:
  `cap x bytes-per-entry x 2 epochs x 2 signals`. At the 1,000,000 the ADR
  proposed that is 140,000,000 to 224,000,000 bytes (134 to 214 MiB) per fully
  active tenant, 4.4x to 7x what the original figure implied; at the 200,000
  that ships it is 28,000,000 to 44,800,000 bytes (27 to 43 MiB). The ADR's section 3
  defaults table now carries the shipped figure with the proposed one beside
  it. Documentation only, no behavior change.

- **A shard now refuses a flush trigger once `--max-queued-flushes` (default
  8) flush tasks are spawned and unacked, leaving the rows buffered for the
  next tick** (issue #1740). Before this, every trigger spawned a task, so a
  shard whose writes were slow kept spawning while each task held its built
  batch resident, with the worst case set by how long the object store stayed
  slow rather than by anything configured. `/metrics` reports it with two new
  families, both by `{mode, signal}`: `ravel_ingest_queued_flushes`, the
  spawned-and-unreaped depth summed across shards, and
  `ravel_ingest_flush_trigger_deferred_total`, the refusals that bound it.
  Two things to know: a
  flush that crosses the per-tenant memory backstop is **exempt** and spawns
  even at the cap, because a bounded queue of tasks is worth less than a
  bounded buffer, so the queue can exceed the cap and under
  `--max-ingest-buffer-bytes 0` only the length of a store stall bounds the
  overshoot; and an `--max-inflight-flushes` above `--max-queued-flushes`
  **raises the effective cap to match**, logging a warning that names both
  numbers, rather than refusing to start. Only a spawned task can hold a
  permit, so the cap has to be at least the permit count; raising it there
  keeps a cluster running `spec.gateway.maxInflightFlushes` above 8 starting
  on upgrade, which a refusal would have crash-looped with no field on the
  `RavelCluster` CRD able to raise the cap in response.

  A third thing to know: a deferred flush pins the ingest hour it eventually
  opens in, not the one its refused trigger fired in, so a long deferral moves
  which ingest hour the rows land in. Past two hours that is more than the
  read side's scan slack covers, and a straggler deferred at the cap while a
  `shard_count` decrease is activating can land in an hour the retiring
  generation no longer scans. Watch
  `ravel_ingest_flush_trigger_deferred_total`: a nonzero rate is the signal,
  and it means the object store is the thing to look at. Pinning the hour
  before the deferral instead was tried and reverted, because it moves the
  same overrun onto the catalog's sealed-hour watermark, where a late record
  is never read again rather than missed by one generation; `docs/ingest.md`
  and ADR-1642 carry the arithmetic. Bounding the deferral itself is issue
  #1916.
- **The at-rest scrub corpus now covers compaction and rewrite output parts,
  not only original L0 segments, and `ravel_scrub_checksum_mismatch_total`
  carries a new `level` label** (issue #1686). The corpus previously skipped
  every compaction and rewrite record it listed, so a bit flip in an L1 or
  rewrite part was never checksummed; a live compaction or rewrite record's
  parts now join the same rotation an L0 segment does. The lineage filter
  applies to the parts only, and leaves out three shapes: a compaction or
  rewrite record another rewrite record names in `superseded_record_key`, a
  compaction record that loses its bucket's overlap component to another
  compaction record (the state a compactor race leaves behind, resolved through
  the same selection the snapshot resolver, the index fold and the sweep use),
  and either kind in a bucket a retention tombstone has retired. No query reads
  a superseded or tombstoned record's parts, and an overlap loser's parts are
  read by no node that has adopted the overlap rule, so no operator is paged on
  rot in bytes nothing serves. L0 commit records carry no supersession, overlap
  or tombstone check and are scrubbed whatever their lineage, so a `level="l0"`
  mismatch on an hour a live compaction has already folded may name a redundant
  copy rather than data a query can still reach. The mismatch counter now
  carries `level="l0"`, `level="l1"`, or `level="rewrite"` instead of one
  undifferentiated series per signal; a dashboard or alert rule that sums
  over `signal` alone still sees the same total, but one that names the
  metric without also grouping by `level` now gets three series back instead
  of one. Neither the tick cadence nor the rotation period changes: the
  per-tick byte budget is `total_corpus_bytes * tick / period`, so it scales
  with the corpus and a full rotation still completes in about the configured
  `--scrub-period`. What rises is the scrubber's steady-state read cost,
  approximately in proportion to the share of the shard's bytes that now sit
  in compaction or rewrite parts. On a fully compacted shard, whose compaction
  output parts hold roughly as many bytes as the L0 segments they folded and
  which are still listed alongside them, that is close to a doubling of scrub
  GET bytes per tick. Size scrub read bandwidth against the corpus with parts
  included, not against the L0 total.

  of one. The scrub tick cadence is unchanged: an operator should expect the
  first tick after upgrading to cover a larger corpus within the same
  per-tick byte budget, which can extend how long a full rotation takes on a
  bucket with many compacted or rewritten hours.
- **A distributed query coordinator now decodes a slice incrementally under a
  frame cap and a byte cap instead of draining the whole stream into memory
  first** (issue #1687). Both fetch clients, intra-cluster and federated,
  buffered every response frame a remote sent and only then decoded them, so
  the remote decided how much the coordinator held. Each slice is now capped at
  1048576 response frames and at 230331648 wire bytes, both checked before a
  frame is decoded, and the client stops pulling at the first breach, which
  cancels the RPC. The byte ceiling is derived from the sample budget rather
  than picked as a round number: it is `DEFAULT_MAX_SAMPLES` (10000000) times
  the widest wire cost of one scalar sample (18 bytes), plus the frame cap
  times 48 bytes of per-frame framing as headroom, so a slice of plain scalar
  runs carrying the whole sample budget, every sample at its widest encoding,
  encodes inside it. The ceiling is reachable and is meant to be: per-sample
  provenance columns, long labels, and native-histogram frames all cost more
  than that derivation counts, and only a federated (resolve-scope) slice is
  bounded at `max_samples` by the worker itself, while an intra-cluster slice
  has no per-slice sample limit at all. A slice that does cross it is refused
  as a budget error naming both figures. The ceiling is fixed: it applies with
  no configuration at all, and nothing raises or lowers it. In particular it
  is independent of `max_bytes_scanned`, which budgets the compressed store
  bytes a slice reads rather than the uncompressed bytes it sends back. Both
  caps are per slice, and what multiplies them depends on the path: a local
  fan-out runs up to `promql_fetch_fanout` times `max_parallel_slices`
  decoders at once (64 at the defaults), while a federated query runs one per
  remote cluster and `max_parallel_slices` does not bound it. A breach is a
  refusal rather than an outage: HTTP 422 naming the observed figure and the
  cap, in the same class a local budget trip uses, not the redacted 503 other
  slice failures become. A refusal fails
  the query and so carries no stats block; the wire bytes a slice made the
  coordinator accept are reported as `wireBytesConsumed` in `stats.fragments[]`
  on the slices that completed.

  1048576 response frames and at the coordinator's own `max_bytes_scanned` in
  wire bytes, both checked before a frame is decoded, and the client stops
  pulling at the first breach, which cancels the RPC. A breach is a refusal
  rather than an outage: HTTP 422 naming the observed count and the cap, in the
  same class a local budget trip uses, not the redacted 503 other slice
  failures become. The wire bytes a slice made the coordinator accept are
  reported as `wireBytesConsumed` in `stats.fragments[]`.
- **The catalog's per-tenant commit-record cache capacity is now derived from
  the shard count and the configured max flush delay, not a flat 10,000-entry
  constant** (issue #1735). The new `ravel_catalog::derive_cache_capacity_per_tenant`
  computes `shards * ceil(3600 / flush_secs) * 3`, floored at the old
  constant; `build_catalog` calls it with the server's resolved
  `--max-flush-delay` instead of the flat default. At the shipped defaults (4
  shards, a 2-second max flush delay) this holds 21,600 entries, about 16 MB
  per actively-queried tenant at roughly 750 bytes per cached record, up from
  the old flat bound. No new CLI flag is added; the capacity is a function of
  existing ingest configuration. A repository guard now fails a pull request
  that touches `crates/` or `services/` and carries no changelog entry, which
  is why this internal sizing change carries one.
- **The published PromQL conformance figure now says what it measures**
  (issue #1698). The committed table read `132/132 = 100%` under a heading
  that invites it to be read as agreement with Prometheus, while the block is
  regenerated with no Prometheus in the loop: the number counted constructs
  Ravel *reaches*. The row is now split. `reached` keeps the old meaning and
  the old number; `agreed with Prometheus` is scored only over the constructs
  a differential run actually compared, reads `not measured in this run` when
  no run report was folded in, and reports `not compared` and
  `accepted divergence` as their own counts rather than folding either into
  the ratio. An ADR-accepted divergence is never counted as a match: a
  construct whose every entry is one is counted on its own line, and a
  construct that mixes them with ordinary entries is scored on the ordinary
  ones and names the rest in its evidence column. The difftest lane now
  writes the report the agreed row reads, so
  the figure can move at all.
- **`--s3-endpoint` now decides whether plaintext is allowed, and a
  non-loopback `http://` endpoint is refused at startup** (issue #1707).
  `allow_http` was true whenever any endpoint was set, so a deployment
  pointing at an `https` endpoint still permitted a downgrade, and a plaintext
  endpoint naming a host on the network moved credentials and telemetry in
  clear with nothing refusing it. The flag now follows the URL scheme. An
  endpoint carrying no scheme at all enables no plaintext, and issue #1911
  below refuses it at startup rather than letting it reach the S3 client. A
  non-loopback
  `http://` endpoint needs `--s3-allow-http` (env `RAVEL_S3_ALLOW_HTTP`), and
  the refusal names the flag; loopback `http` is unchanged, which is what the
  dev compose stack, kind, and the tests use. `ravel-cli` applies the same rule
  from the same function rather than a second copy of it, with the same
  `--s3-allow-http` flag and `RAVEL_S3_ALLOW_HTTP` variable: it ships in the
  server image and reaches the same bucket with the same credentials, and the
  operator's store-qualification Job runs it before any server pod exists. The
  operator gains `spec.storage.s3.allowHttp` for an in-cluster MinIO,
  rendering the flag on every server container and `RAVEL_S3_ALLOW_HTTP=true`
  on the qualify Job. The operator applies the rule from the same function at
  its own two remaining sites: it refuses a plaintext non-loopback endpoint at
  render time, with a `Degraded` condition whose reason is
  `PlaintextS3Endpoint` and whose message names
  `spec.storage.s3.allowHttp`, rather than creating Deployments
  that crashloop with the refusal only in their pod logs; and its own S3
  client, the one that reconciles `sys/auth` and applies `shardOverrides` with
  the cluster's credentials, no longer derives plaintext from endpoint presence
  either. Editing `allowHttp` re-runs store qualification, so the remediation
  is re-checked instead of skipped. **On upgrade**, a deployment already
  pointing at a plaintext non-loopback endpoint will not start until the flag
  or the environment variable is set, a `RavelCluster` in that state is refused
  with the field named in its status, and a `ravel-cli` invocation against one
  is refused the same way.
- **`ravel-cli load` reports a `--skip-rows` value past the end of the file
  instead of succeeding quietly** (issue #1713). The value was clamped to the
  file's row count and the run exited 0 having written nothing, and the
  summary printed the clamped figure, so a resume script reading the exit code
  recorded the load as done and a human reading the output could not see that
  the requested offset had missed the file. A request strictly larger than the
  row count now prints a warning naming both numbers. A request equal to the
  row count is the legitimate resume of an already-complete file and stays
  silent.
- **A distributed query now sends each slice the full byte budget instead of
  an even share, and a worker clamps every wire budget to its own
  `EngineConfig`** (issues #1725 and #1687). The per-slice share failed a query
  that was well under its total budget whenever one slice scanned more than an
  even fraction, which is the normal shape of a skewed fan-out, and the
  residual surfaced as a retryable 503. The coordinator still enforces the
  total on the folded figures, so the worst case is a bounded over-scan before
  it refuses. A cap refusal now renders as HTTP 422 wherever it comes from,
  including across clusters; only a fan-out failure or a worker out of fetch
  memory keeps the retryable 503. On the worker side a wire budget of `0` or
  absent now means the worker's own limit rather than unlimited, and the
  federation `Resolve` path enforces the worker's `max_series` and
  `max_samples` too.
- **The catalog's per-tenant record cache capacity is now derived from the
  shard count, the signal count and the configured max flush delay, not a flat
  10,000-entry constant, and is capped at a stated per-tenant memory budget**
  (issue #1735). The new `ravel_catalog::derive_cache_capacity_per_tenant`
  computes `shards * 6 signals * ceil(3600 / flush_secs) * 3 unsealed hours`,
  floored at the old constant and capped at 25,000 entries;
  `build_catalog` calls it with the server's resolved `--max-flush-delay`
  instead of the flat default. The signal term matters because the caches are
  partitioned by tenant and not by (tenant, signal): a tenant ingesting
  metrics, logs and spans keeps three unsealed tails in one LRU, and a
  single-signal derivation under-sizes it by that multiple and leaves it
  thrashing. The cap matters because one capacity bounds two caches per tenant
  (commit records and L1 compaction records), so the worst case is
  `25,000 x 900 bytes x 2` = 45 MB per actively-queried tenant, held constant
  across every deployment shape: `--shards 64` would otherwise derive
  2,073,600 entries and 3.7 GB per tenant, with nothing process-wide bounding
  the next tenant. NEITHER cache is bounded by its entry count alone, because
  neither record type has a bounded size: each is also held to an equal share
  of that budget in BYTES (22.5 MB at the cap), charging each entry an
  estimate of the live heap it holds and evicting until the tenant's summed
  charge is inside the share. `CommitRecord.declared_column_stats` is a
  repeated field capped neither by the proto, by `validate`, nor by the
  tenant-config declared-column path, so a record declaring 200 typed
  attribute columns charges about 20 KB against the 864 an ordinary one does;
  the commit-record cache evicts least-recently-used against
  `commit_cache_max_bytes_per_tenant`. A `CompactionRecord` carries one
  `CompactionInputIdentity` per compacted L0 segment, capped neither by the
  proto nor by `validate_compaction`, so one L1 record over 1,800 L0 segments
  charges about 137 KB, 150 times the per-entry planning rate; the L1
  compaction-record cache evicts oldest-first against
  `compaction_cache_max_bytes_per_tenant`. An entry-count bound would have let
  one tenant exceed its share of the 45 MB by two orders of magnitude on
  either side. The 900 bytes is a planning rate the capacity is derived
  against, not a per-entry cap, so the capacity is an entry cap rather than a
  guaranteed residency: a tenant whose records carry typed attribute column
  statistics holds proportionally fewer of them and the memory stays inside
  the figure. These caches sit outside ADR-1170's carved shares, so an
  operator budgets 45 MB times the number of
  concurrently queried tenants on top of them. The capacity covers a tenant's
  unsealed tail up to the cap, not whatever the tail actually is: at the
  shipped defaults the estimate is already 129,600 entries, the flush-cadence
  term counts the age trigger only, so a tenant flushing on object size seals
  more records per shard-hour than it assumes, and the three-unsealed-hours
  term assumes the default seal parameters, under-counting by about 1.8x at
  `--gc-max-flush-lifetime 4h`. Over the bound a resolve pays per-record GETs;
  a tenant whose records carry declared-column statistics now holds fewer than
  the flat 10,000 the old cache held, so it can pay GETs it did not pay before,
  which is the hit rate the enforced memory bound costs. `--disable-cache` keeps the flat
  10,000-entry capacity rather than the derived one, and with it a 9 MB byte
  budget for each of the two caches, so the memory-constrained-container flag
  stays on the lowest capacity the code supports short of disabling the
  resolve path's record cache entirely. No new CLI flag is added; the
  capacity is a function of existing ingest configuration.
  10,000-entry capacity rather than the derived one, and with it a 7.5 MB
  compaction-record byte budget, so the memory-constrained-container flag
  never costs more record-cache memory than it did before this change. No new
  CLI flag is added; the capacity is a function of existing ingest
  configuration.
  the next tenant. These caches sit outside ADR-1170's carved shares, so an
  operator budgets 45 MB times the number of concurrently queried tenants on
  top of them. The capacity covers a tenant's unsealed tail up to the cap, not
  whatever the tail actually is: at the shipped defaults the estimate is
  already 129,600 entries, and the flush-cadence term counts the age trigger
  only, so a tenant flushing on object size seals more records per shard-hour
  than it assumes. Over the bound a resolve pays per-record GETs as it did
  before, so the direction is safe. `--disable-cache` keeps the flat
  10,000-entry capacity rather than the derived one, so the
  memory-constrained-container flag never costs more record-cache memory than
  it did before this change. No new CLI flag is added; the capacity is a
  function of existing ingest configuration.
  top of them. No new CLI flag is added; the capacity is a function of
  existing ingest configuration. A repository guard now fails a pull request
  that touches `crates/` or `services/` and carries no changelog entry, which
  is why this internal sizing change carries one.
- **A distributed deployment now refuses three unsafe listener shapes at
  startup** (issues #1724, #1703, #1690). Starting with `--distributed-query`
  and a wildcard bind refuses unless `--advertise-fragment-endpoint` names the
  host peers should dial, because the server previously advertised `0.0.0.0`
  to its peers; in the combined layout, where both lanes share one socket,
  that flag takes a host only and a `host:port` value is refused. A
  non-loopback `--mtls-listener` refuses unless
  `--mtls-trust-forwarded-header` says the operator meant to trust a forwarded
  identity from anything that can reach the port. The dedicated fragment
  listener now requires a client certificate signed by the configured CA, and
  the coordinator presents its own identity when dialling; a certificate
  provisioned against the previous documentation carries `serverAuth` only, so
  startup parses it and refuses when `clientAuth` is missing rather than
  letting every outbound dial fail at the handshake and fall back to local
  execution. Regenerate the fragment certificate with both usages before
  upgrading.

- **The physical retention sweep is now all-or-nothing under a legal
  hold** (issue #1697). A hold on any key the pass would delete (a commit,
  compaction or rewrite record, an L0 data object, an L1 object, or the
  tombstone) makes the pass delete nothing and return `SweptPartial`; before,
  the pass skipped only the held keys and deleted the commit records and
  tombstone that named the held bytes. A bucket parked this way counts on the
  new `held_by_lease_buckets_total` counter. `ravel hold set --scope` now
  refuses a scope that covers only part of one shard's three hold prefixes
  (for example `t/<hex>/m/l0/0000/`); tenant-wide and signal-wide scopes are
  still accepted. A script that set such a partial scope must move to the
  `--signal`/`--shard` form.
- **`POST /api/v1/sql` now refuses a request body over 64 KiB, down from
  1 MiB, and refuses any statement over 1,000 structural tokens** (issue
  #1680). Both bounds return 400 on the HTTP surface, `InvalidArgument` on
  Flight SQL, and a `validation` error on MCP. The token bound is a pre-parse
  scan: a deep expression tree, which a flat operator chain can build without
  nesting anywhere, previously reached the planner and aborted the process on
  stack overflow, taking every tenant on the node with it. A statement that
  now returns 400 was previously executed, so a generated or machine-built
  statement near either bound is the case to check on upgrade. The token
  count is not a character count: a literal, an identifier and a keyword each
  cost one whatever their length, so quoting does not change the verdict.
  `docs/query-engine.md` states how the bound is calibrated and
  `docs/reference/http-api.md` documents the body cap.
- **Retention no longer deletes a metric object whose format version this
  build's reader does not admit** (issue #530). The horizon-gated physical
  sweep probes each object's trailer first and distinguishes an unadmitted
  version from corruption: an unadmitted version holds the whole bucket (the
  tombstone stays, the outcome is `SweptPartial`, and the new
  `ravel_maintain_retention_held_out_of_window_objects_total` counter rises),
  because the other side of a rolling upgrade reads that object normally. A
  corrupt object is still swept. A held bucket retains data past its retention
  window until the upgrade, migration, or rollback completes, so a nonzero
  counter rate needs operator action. Metrics (RSEG) only; logs and spans keep
  today's sweep.
- **RavelClusters without a deployment key must now reference an audit token
  key Secret through `spec.auditTokenKeySecretRef`.** Until they do, the
  operator reports `AuditTokenKeyMissing` and leaves the query Deployment as
  it is. Clusters with `deploymentKeySecretRef` need no action.
- **The operator's qualified-input hash now distinguishes an absent credentials
  `resourceVersion` from an empty one** (issue #36). The credentials
  `resourceVersion` slot carries the same one-byte presence marker the S3
  `endpoint` slot got in 0.15.0, so an unresolved credentials Secret no longer
  collides with one whose `resourceVersion` resolved to the empty string. The
  encoding of that slot changes for every cluster, so every persisted
  `status.storeQualifiedHash` changes and the first reconcile after upgrading
  the operator re-qualifies each existing cluster once against its unchanged
  store. The qualify Job is a one-shot that touches no Deployment, so no serving
  pod is restarted and there is no downtime. Subsequent reconciles are stable.
- **The distributed query client no longer carries an unbounded slice decode
  path** (issue #1912). `ravel-query`'s `RemoteSliceFetcher` drained every
  frame a remote sent into a `Vec` and decoded it afterwards, so the remote
  decided how much the coordinator held. Issue #1687 replaced that with an
  incremental capped decode for the metrics signal only, leaving the log and
  span helpers (`decode_log_slice_frames`, `decode_span_slice_frames`) and the
  collect-then-decode fetch that fed them as the last unbounded path. Both
  helpers and that fetch are removed, and `RemoteSliceFetcher::fetch` now
  decodes through the same `SliceStreamDecoder` the rest of the coordinator
  uses, under the per-slice caps #1687 introduced (1048576 response frames and
  230331648 wire bytes, refused as HTTP 422 naming both figures). No deployed
  query changes behavior: nothing served a log or span slice through
  `RemoteSliceFetcher`, and the log and span fetches on the `SliceFetcher`
  trait were, and remain, the defaults that report `Unsupported` and send the
  coordinator to whole-query local execution. The removed items were public in
  `ravel_query::distrib::client`, so any out-of-tree caller of them has to move
  to `SliceStreamDecoder`. `RemoteSliceFetcher` gains `with_max_frames` and
  `with_max_bytes`, `pub(crate)` test seams that replace either cap outright
  (not lower it).
- **Native histogram samples whose shape Prometheus itself rejects are now
  refused at ingest, on both the OTLP and Remote Write surfaces**
  (issue #1858). This is a behaviour change at the ingest boundary: a sender
  emitting any shape below had the sample accepted and stored before this
  release and now has it refused, so check your senders before upgrading.
  Refused on both surfaces: a `zero_threshold` that is NaN, positive or
  negative infinity, or negative. Refused on Remote Write for a custom-buckets
  histogram (`schema == -53`), which has neither a negative side nor a zero
  bucket: non-empty negative spans, a `zero_threshold` that is not zero, and a
  `zero_count` that is not zero. The boundary-list rule is tightened at the
  same time: on top of non-empty and strictly ascending (and absent under any
  other schema), every bound must be finite and the positive buckets sent must
  not outnumber the bounds by more than one, since `n` bounds define at most
  `n + 1` buckets and the last `+Inf` one is implicit. Those last two are
  separate rules rather than consequences of ascendingness: a lone `NaN` has no
  adjacent pair to compare, and a trailing `+Inf` is strictly greater than its
  predecessor, so both passed before and reached the query side, where reading
  past the boundary list yields `+Inf` and leaves the final bucket spanning the
  degenerate interval `[+Inf, +Inf]` for `histogram_quantile` to interpolate
  over. OTLP enforces all of these by refusing
  `scale == -53` outright, since it has no field to carry bucket boundaries,
  so no custom-buckets shape reaches its normalizer at all. Under the
  exponential schemas the zero side is untouched: a populated zero bucket
  stays admitted, as does a `zero_threshold` of `+0.0`, `-0.0`, or a
  subnormal, because Prometheus' `Histogram.Validate` reads `ZeroThreshold`
  only under the
  custom-buckets schema and never screens an exponential-schema value for
  magnitude. For the custom-buckets rules `+0.0` and `-0.0` both count as
  zero, matching Prometheus writing that rule as `ZeroThreshold == 0` in Go;
  every other pattern, a subnormal and a NaN included, does not. Each refusal
  is per-sample, not per-request, and behaves like every other structural
  ingest refusal on its surface: on OTLP the sample is counted in
  `rejected_data_points` with the reason in the partial-success
  `error_message` and under the `structural` normalize-reject counter; on
  Remote Write the request still answers `204`, the sample is counted into the
  surface's dropped-points counter, and the
  `X-Prometheus-Remote-Write-Histograms-Written` header excludes it. Each of
  the four Remote Write messages names the field it refused on. Refusing new
  samples does not clean up data already stored with any of these shapes, so
  the query-side zero-bucket guards remain in place.

- **Every PromQL parse of caller text runs the pre-parse complexity guard,
  because one function now does both** (issue #1817). The guard that keeps an
  over-bound query from overflowing the stack inside promql-parser, which
  aborts the process and takes every tenant on the node with it, used to be a
  separate call each parse site was expected to make first. The five sites that
  parse PromQL, including one in the query coordinator's federated path, now
  call `complexity_guard::parse_guarded`, which checks and then parses. No
  query that was accepted before is rejected now and no error message changes:
  each caller maps the funnel's two failure modes onto the error it already
  reported. A new gate check refuses a parse that reaches promql-parser by
  naming or importing it anywhere else in either crate, so a future entry point
  cannot skip the guard by not knowing about it.

- **ADR-0057's cost argument for the fleet admission reconciliation loop is
  marked as resting on a premise ADR-0069 reversed** (issue #1922). ADR-0057
  sizes the loop on "most processes see most tenants never" and on a process
  dropping a `(tenant, signal)` once the tenant goes idle; ADR-0069 decided
  that the admission map grows with tenant count instead, and the code
  implements ADR-0069. The ADR now records what the loop costs on the code as
  it stands -- a floor of `2 * T * S` sequential object-store round trips per
  cycle, with `T` counting every tenant the process has served, including one
  whose only request was rejected -- rather than a justification that has not
  held since ADR-0069 landed. No decision and no code changed; bounding the
  loop belongs with the ADR-0069 follow-up.

### Fixed

- **The shipped Maintain IAM template now grants the delete the dead-worker
  reaper issues** (issue #1975). `MaintainDelete` named nothing under `sys/`,
  so every `sys/maintain/workers/<process_id>` delete from
  `WorkerSet::reap_keys` came back `AccessDenied` on a deployment using the
  template. No heartbeat key was ever removed, and the prefix the live-set
  LIST reads once per maintain tick grew with every maintain process that had
  ever run. `deploy/iam/maintain.json` now allows `s3:DeleteObject` on
  `sys/maintain/workers/*` only: the memo snapshots and compaction claims that
  share `sys/maintain/` are not the reaper's to delete. Re-apply the Maintain
  policy to pick this up. A test drives all four heartbeat operations against
  the shipped policy with keys built by `heartbeat_key` itself.

- **What a `0` on `ravel_store_probe_last_run_timestamp_seconds` means is
  documented in one place, and it states all three causes** (issue #1982). The
  explanation was written out across the store-probe source, its
  tests, the shipped Prometheus rule file and the observability guide, and
  every copy gave the reading a single cause. Two were missing, and one of the
  two pages until an operator fixes something the alert's description does not
  mention. Four sweeps had already tried to keep the copies consistent; two of
  them added copies while removing others. The "What `0` means" section of
  `docs/guides/observability.md` is now the one statement of the causes, every
  other site points at it, and
  `scripts/guards/check-claim-single-source.sh` fails the build on a
  restatement that is not a pointer, on a registered pointer that stops
  pointing, and on a canonical block that has moved or lost a cause. The one
  exception is the gauge's `HELP` line, which ships in `/metrics` output where
  a reader has no link to follow, so it carries a one-line summary naming the
  three; the guard checks that summary too. No behaviour changed.

- **`--max-ingest-buffer-bytes`'s help text and generated reference page now
  state what `0` actually leaves unbounded, instead of calling it a disabled
  ceiling and nothing more** (issue #1740). `-h` and the generator that renders
  `docs/reference/ravel-server-flags.md` both show only the doc comment's first
  paragraph (`--help` shows all of it), so the fuller explanation further down
  the comment never reached either surface. An operator reading either one saw
  "`0` disables the ceiling (the gauge is still tracked for `/metrics`)" and
  nothing else, which is how a `0` setting produced a flush queue bounded only
  by host memory under a sustained object-store stall with no warning in the
  documentation they read. The first paragraph now says directly that `0` does
  not leave spawned-flush memory unbounded on its own -- `--max-queued-flushes`
  still caps the ordinary flush queue at every setting of this flag -- and names
  the one exemption from that cap that can keep growing under `0`: a buffer that
  has crossed its per-(shard, tenant) memory backstop, which with the byte
  ceiling disabled is bounded only by the length of the stall. A test pins both
  to the short help. No runtime behavior changes; the reference page is
  regenerated from the updated doc comment with `RAVEL_UPDATE_CLI_REFERENCE=1
  cargo test -p ravel-server --test cli_reference`.

- **`ravel-cli gc-config set --max-flush-lifetime`'s help text and generated
  reference page now state the floor the flag is refused below** (issue
  #1961). `set_gc_config` has always rejected a `max_flush_lifetime` below the
  ingest pipeline's own default `max_flush_lifetime`
  (`ingest_max_flush_lifetime_floor_ns`, read from
  `ravel_ingest::IngestConfig::default()` rather than duplicated as a
  constant, so the validator tracks that default automatically), but
  neither the flag's clap doc comment nor
  `docs/reference/ravel-cli-flags.md` said so: an operator who supplied a
  shorter duration got a refusal with no indication of what value would have
  worked. The doc comment now names the floor and where it comes from, and
  the reference page is regenerated from it. The help text quotes the
  current value as `currently 1h`; that figure is prose and is not derived,
  so it is hedged rather than stated flat.

- **The shipped Maintain IAM template now grants the permissions the orphan
  quarantine needs** (issue #1957). ADR-0058 decision 6 has the orphan sweep
  copy an orphaned L0 object to a top-level `quarantine/` key space, delete the
  original, and physically remove the copy once the quarantine horizon elapses,
  but `deploy/iam/maintain.json` named no resource and no `s3:prefix` under
  `quarantine/` at all. IAM is default-deny, so on a deployment running the
  shipped template the quarantine was refused at its first `PutObject`: the
  sweep could not quarantine a single orphan, and because the original is
  deleted only after the copy succeeds, nothing was lost but nothing was
  reclaimed either, and the reaper that drains the quarantine had no keys to
  find and no authority to list for them. The grant set is derived from every
  call site that touches the prefix rather than from one function:
  `MaintainWrite` reaches `quarantine/t/*/*/l0/*` for the copy
  `quarantine_object` PUTs, `MaintainList` admits the `s3:prefix`
  `quarantine/t/*/*/l0/*` that `sweep_quarantine` LISTs, and `MaintainDelete`
  reaches `quarantine/t/*/*/l0/*` for the reaper's physical delete. The GET of
  the original and the delete of the original are the same live L0 key the
  existing `t/*/*/l0/*` read and delete grants already cover, so neither needs a
  new pattern, and nothing reads a quarantined object back, so no `GetObject`
  is granted under `quarantine/`. No new pattern reaches a key outside
  `quarantine/`, and no other role's template reaches one at all. An operator
  who already applied an earlier copy of `maintain.json` must re-apply it; the
  fix is in the template, not in any running binary, so upgrading Ravel alone
  changes nothing. Re-applying lets the sweep quarantine orphans again, and the
  copies it writes become deletable once each one's `quarantine_horizon_ns`
  (7 days by default) elapses, not at once.

- **The shipped IAM templates now grant every permission the selective-erasure
  lifecycle needs** (issue #1849). ADR-0064 section 6 gives Maintain read on
  `del/**` and delete on `del/*.dreq`, but `deploy/iam/maintain.json` named no
  resource and no `s3:prefix` under `del/` at all. IAM is default-deny, so on a
  deployment running the shipped template the `.dreq` sweep was refused
  outright: completed erasure requests were never retired, and the query-time
  exclusion filter that reads them grew without bound for the life of the
  deployment. The grant set is now derived from every call site that touches
  the prefix, not from one function: `MaintainList` admits the `s3:prefix`
  `t/*/*/del/*` the sweep LISTs; `MaintainRead` reaches `t/*/*/del/*`, which
  covers both the completion record whose timestamp anchors the protection
  horizon and the `.dreq` body the erasure rewrite pass decodes;
  `MaintainWrite` reaches `t/*/*/del/*.done` for the completion record that
  pass writes; `MaintainDelete` reaches `t/*/*/del/*.dreq` for the request
  object itself; and `AdminWrite` reaches `t/*/*/del/*.dreq`, which is what
  `ravel-cli erase submit` PUTs. Each grant alone is unreachable without the
  others: the sweep fails on the `ListBucket` before it sees a request object,
  and with no `.done` writable it counts every request as still pending and
  deletes nothing. Completion records remain undeletable by every role,
  including Maintain, as the ADR requires, and no new pattern reaches a key
  outside `del/`. An operator who already applied an earlier copy of either
  template must re-apply it; the fix is in the templates, not in any running
  binary, so upgrading Ravel alone changes nothing. Re-applying lets the
  lifecycle run again and the backlog drains over subsequent passes as each
  request's protection horizon elapses, not at once.
- **An `--s3-endpoint` written with no URL scheme is refused at startup**
  (issue #1911). `minio:9000` used to be accepted by the endpoint rule, which
  only decides whether plaintext is allowed, and then killed the process from
  inside the S3 client the first time it signed a request, on
  `request valid: InvalidUri(InvalidUri(InvalidFormat))` and exit code 101: a
  message naming neither the endpoint, nor the flag, nor the fix. The one
  decision every binary routes through now refuses an endpoint that begins
  with neither `https://` nor `http://`, quoting it as it was written and
  asking for the scheme, so `ravel-server`, `ravel-cli`, and the operator all
  fail in their own pre-flight pass instead of at first request.
  `--s3-allow-http` does not accept such an endpoint: the flag chooses between
  TLS and plaintext, and an endpoint with no scheme has asked for neither. The
  scheme is matched at the front of the endpoint and without regard to case,
  so `HTTPS://minio:9000` is an `https` endpoint and a host named
  `my-http-proxy:9000` still carries no scheme. Under the operator the
  refusal is its own render error rather than the plaintext one: a
  `RavelCluster` whose `spec.storage.s3.endpoint` has no scheme goes
  `Degraded=True` with reason `SchemelessS3Endpoint` and a message naming the
  field and the endpoint, and no Deployment, Service, or store-qualification
  Job is created.
- **A `RavelCluster` held at the store-qualification gate keeps its
  `StoreQualified` condition when the reconcile then fails** (issue #36). The
  degraded status write replaces the whole `conditions` array and previously
  reconstructed only the `StoreQualified=True` case, so a pass held at the gate
  that then failed a write left a `Degraded` object carrying no `StoreQualified`
  condition and nothing to say the store had not qualified. The gate now carries
  out the exact condition it built, `True` or `False` with its Pending or Failed
  reason, and records it before it creates or deletes the qualify Job, so a
  failure in that API call cannot drop it either.
- **`sum`/`avg`, `rate`/`increase`/`delta`, `irate`/`idelta` and `resets` over
  custom-bucket (NHCB) native histograms now compare the bucket boundaries,
  not just the custom-buckets schema sentinel** (issue #1851). Three reducers
  guarded a mixed exponential/custom group by comparing `uses_custom_buckets()`
  alone: `histogram::sum_histograms` (behind `sum` and `avg`),
  `histogram::histogram_rate` (behind `rate`, `increase` and `delta`) and
  `functions::rate::instant_value_hist` (behind `irate` and `idelta`). Two
  custom-bucket histograms both carry the `-53` sentinel scale, so that check
  passed them through whatever boundaries they held, and the fold then merged
  bucket `i` of one boundary set into bucket `i` of another: a `sum` over
  series with different bounds, a `rate` window whose bounds changed
  mid-window, or an `idelta` over two adjacent samples whose bounds differ,
  each produced a histogram whose buckets combined unrelated value ranges,
  with nothing to say so. All three now compare the boundaries with
  `FloatHistogram::custom_bounds_match`, the same bit-pattern comparison the
  `h + h`/`h - h` binary-operator path already aligns its operands with, so
  bounds differing only in the sign of zero count as different. A group,
  window or adjacent pair that fails the comparison yields no sample.
  `irate`/`idelta` also raise a `vector contains histograms with mismatched
  custom buckets` warning, since that path has an annotation channel; the
  other two return a bare `Option` and drop silently. The drop is
  conservative rather than a match for Prometheus v3.13.1, which re-buckets
  differing bounds onto the intersection of the two boundary sets and raises
  an info annotation, as `combine_custom_reconciled` already does for the
  binary-operator path. Reconciling in the reducers instead would need their
  callers in `aggregate.rs` and `over_time.rs` and is not done here.
  `FloatHistogram::detect_reset` carried the same defect in comparison rather
  than combination form: it compares per-bucket populations by absolute index,
  so a boundary change read as "no reset" whenever no index happened to shrink.
  It now reports a boundary change as a reset, the same answer it already gave
  an exponential/custom mix. `resets` is the only caller whose answer changes:
  the three reducers above drop a differing-bounds window before any reset
  detection runs.
- **`--gc-max-flush-lifetime`, and its compiled-in default when the flag is
  absent, now refuse a resolved value below the ingest pipeline's own
  compiled-in flush lifetime, instead of accepting one** (issue #1744). The
  flag had no lower floor, and neither did the default it falls back to when
  unset: a resolved value below ravel-ingest's fixed (and currently
  unconfigurable) `max_flush_lifetime` let the compactor call a bucket sealed
  before a real writer's flush interlock had actually elapsed, which could
  void the erasure completion gate (`bucket_erasure_completion` reporting a
  pending erasure request complete while a flush that can still publish into
  that bucket was still in flight) and undercut the retention floor derived
  from the same value. `--gc-max-flush-lifetime`'s startup check
  (`Cli::validate` and `resolve_gc_runtime`, the latter being the single
  point the real compactor is built from) now floor-checks the resolved
  value whether it came from the flag or the default, read fresh from
  `ravel_ingest::IngestConfig::default().max_flush_lifetime` rather than a
  duplicated constant, so a `ravel-server` invocation with no flag at all is
  held to the same floor as one that names a value explicitly. A new test
  pins `DEFAULT_MAX_FLUSH_LIFETIME_NS` equal to that same ingest default, so
  the two compiled-in constants cannot drift apart silently.
  The durable `sys/gc` mutation path (`gc-config set`) and bootstrap path
  (a fresh bucket's first touch) also enforce the same floor read from the
  same function, so `sys/gc` -- the durable, operator-facing record of the
  deployment's intended GC values -- can never hold a value the process
  itself would refuse to run with. `sys/gc`'s own `max_flush_lifetime_ns`
  field is not currently read into any compactor config (that floor is
  defence in depth on the record, not a mechanism that itself prevents an
  early seal); the compactor's actual value comes only from
  `--gc-max-flush-lifetime`/its default, floor-checked as described above.

- **No disk-tier file operation runs on a runtime worker thread any more**
  (issue #1891). A process configured with a disk cache tier read and wrote
  cache files on the async runtime's worker threads in two remaining places:
  the block-range read path, which peeks both tiers per extent and admits the
  bytes it fetched, and the ADR-0064 background age sweeper, whose periodic
  tick walks the whole cache directory. A slow or contended disk parked a
  worker for the length of that file operation, so unrelated queries and
  ingest work sharing the runtime stalled behind it, the same starvation
  `get_or_fetch` was moved off the worker for. Both now run under
  `spawn_blocking`: async callers reach the tiered cache through
  `TieredCache::get_off_worker` / `insert_off_worker`, which keep the RAM tier
  inline (it is not I/O, and a RAM hit stays the fast path) and dispatch only
  the file operation, and each sweeper tick dispatches its directory walk.
  Cache semantics are unchanged: the same read-through, the same dual-tier
  admission, the same max-age bound in sweep intervals. A RAM-only cache
  (no `--cache-dir`) is unaffected, since it never touched the disk tier.

- **The shipped IAM templates now let the unreferenced-catalog sweep delete
  what it finds** (issue #1847). `deploy/iam/maintain.json`'s
  `DenyDeleteProtected` statement denied `s3:DeleteObject` and
  `s3:DeleteObjectVersion` on the whole catalog family, `t/*/catalog/*/*`,
  which also covers the snapshot and index objects
  (`t/*/catalog/<signal>/snap/*`, `t/*/catalog/<signal>/idx/*`) that the
  unreferenced-catalog sweep physically removes once they are unreferenced,
  aged past the protection horizon, and unleased. IAM's explicit Deny
  overrides any Allow for the actions it names, so on a deployment running
  the shipped template every sweep pass was refused on its first delete:
  catalog garbage, including any unreferenced snapshot or index object
  holding an erased subject's value, was never reclaimed. The deny is now
  narrowed to the catalog HEAD pointer alone (`t/*/catalog/*/HEAD`), the one
  catalog object the sweep never deletes, and `MaintainDelete` now grants
  delete on `snap/` and `idx/` keys to match what the sweep already does. A
  new pinning test asserts both directions: HEAD stays denied, and a snap
  key and an idx key built from the same key constructors the sweep uses are
  deletable. This changes a shipped IAM template: an operator running
  `maintain.json` from before this change must re-apply it. Until they do,
  the sweep keeps failing its first delete every pass, exactly as before.
- **Four axis and description issues in the standalone Grafana dashboard are
  fixed** (issue #1962). "Workers and units" and "Declared-stat stamp
  coverage" carried `"fieldConfig": {"defaults": {}, "overrides": []}`, so
  every series on each panel shared one unitless auto-scaled axis: the L0
  records pending backlog crowded out the three small worker/unit counts on
  the first, and the drop counter sat flat at the bottom of the same axis as
  two hourly volume counters on the second. Both now carry overrides that
  move the outlier series to its own axis. The drop counter's override uses
  `byRegexp` against `^drops .*$`, not `byName`, because its legend format is
  `drops {{carrier}}`, which a `byName` match on `"drops"` never matches.
  "Cache residency and disk tier" rendered four axes over seven series
  because the unit overrides for `resident entries`, `bytes (served|admitted)`
  and the disk-tier rates all set `axisPlacement: right`, so the disk-error
  rates, the panel's alarm signal, shared a side with the byte-rate series
  instead of standing alone. The disk-tier rates keep the right-hand axis to
  themselves; `resident entries` and `bytes (served|admitted)` move to
  `hidden`, so they still scale on their own units without drawing an axis.
  Two visible axes remain: the default byte axis for resident/max bytes, and
  the disk-tier ops axis on the right. "Log block pruning"'s description
  named the pruned-share metric's `clamp_min(..., 1e-9)` denominator guard
  but never said what an idle system renders as, so an idle fleet showed the
  same falling-to-zero shape the description calls the alarm; it now carries
  the same idle clause as its sibling panel, "Query result cache hit ratio".

- **A denied read on a catalog HEAD, a covered snapshot part, or a
  column-stats object no longer degrades silently into "nothing here yet"**
  (issue #1976). This is the follow-up #1964 left open. Five more GETs
  treated any store error, including `AccessDenied` from a missing IAM read
  grant, the same as a genuine `NotFound`: the column-stats HEAD and stats
  object reads, the catalog HEAD and per-part reads behind the scrubber's
  postings tier, and the catalog HEAD read in seal-divergence verification.
  A permission fault there read as "no statistics yet", "not covered" or
  "nothing folded yet" on every attempt, with nothing an operator could see.
  `NotFound` still degrades exactly as before at all five. Any other error
  now surfaces, naming the failing key. What changes for a caller: on the
  column-stats HEAD and stats-object reads, a retryable error (throttling, a
  timeout, a transient blip) still degrades quietly, so only a non-retryable
  error such as `AccessDenied` fails the query (the client sees "upstream
  storage temporarily unavailable", not an integrity failure), instead of
  running without column-statistics pruning; the scrubber logs the postings-tier and
  seal-divergence reads (at `error` and `warn` respectively) and retries next
  tick, as it already did for other failures there; and `ravel-cli catalog
  verify` exits nonzero instead of reporting "nothing folded yet". The
  shipped query policy in `deploy/iam/query.json` already grants these
  reads.

- **A denied read on a catalog `idx/` object no longer degrades silently
  into "nothing to reuse"** (issue #1964). Three GETs of catalog index
  objects treated any store error, including `AccessDenied` from a missing
  IAM read grant, the same as a genuine `NotFound`: `load_covering_postings`
  returned `Ok(None)` as if no postings ref existed yet, and `fold_inner`'s
  two reuse-baseline reads (the prior part's column-stats object and the
  prior postings object) logged the same `warn!` and fell back to a full
  rebuild regardless of why the read failed. A permission fault recurs on
  every tick, so this reads as the postings tier or the reuse baseline
  never applying, with no signal an operator could act on.
  `NotFound` still degrades exactly as before: `load_covering_postings`
  returns `Ok(None)` and `fold_inner` falls back to a rebuild with a
  `warn!`. Any other error now surfaces: `load_covering_postings` returns
  `Err(LoadPostingsError::Store)` naming the key, which the scrub tick logs
  at `error!` before skipping the postings tier for that tick, and
  `fold_inner`'s two reads log at `error!` with the failing key instead of
  `warn!`, though the fold still falls back to a rebuild either way, since
  reuse is an optimization and not a correctness gate. The new signal is a
  log line: no metric counts these faults, so an alert on them has to come
  from logs rather than from `/metrics`.

- **A `/readyz` test now pins the false-healthy window the store-probe
  liveness gauge exists to expose** (issue #1963).
  `store_probe_last_run_gauge_goes_stale_while_readyz_stays_green` never
  started a server or queried `/readyz` despite its name, so nothing
  covered the behaviour issue #1728 added the gauge for: a dead probe task
  leaves `store_reachable()` frozen at `true`, so `/readyz` keeps answering
  200 while the gauge goes stale. The test now starts a server as the
  neighbouring `/readyz` tests do and asserts the endpoint through the
  whole sequence -- 200 once the store is reachable, still 200 while the
  gauge goes stale and reachability holds, and 503 once reachability flips.
  That last assertion is what makes the coverage real: removing
  `store_reachable()` from `Readiness::is_ready`'s conjunction leaves
  `/readyz` at 200 and breaks it, where a test asserting only the 200 cases
  would have passed against the broken code. It also pins that a failing
  probe cycle still advances the gauge, which a success-branch-only
  implementation would leave stuck.

- **The shipped rule file's test now validates `for:` and `expr:` values, not
  just structure** (issue #1928). `shipped_rules_name_emitted_metrics.rs`
  parsed `deploy/prometheus/ravel.rules.yaml` into groups and rules but read
  both fields as opaque scalar text, so `for: 10 minutes` and an unbalanced
  bracket inside an `expr` block scalar each parsed there while Prometheus
  refuses either on load, killing every alert in the file with no signal from
  the test. Every `for:` is now checked against Prometheus's duration
  grammar, and every `expr:` is parsed with Ravel's own PromQL parser
  (`ravel_promql::complexity_guard::parse_guarded`), each pinned as a literal
  count so a silently-empty extractor fails loudly rather than passing over
  nothing. No shipped rule changed.

## [0.15.0] - 2026-09-08

### Added

- **The operator qualifies the object store before serving** (issue #36). Each
  reconcile pass renders a one-shot `<cluster>-qualify` Job running
  `ravel-cli store qualify` with the server image, credentials Secret, bucket,
  region, and endpoint of the tiers it gates, and creates the gateway, query,
  and maintain Deployments only once that Job reports Complete. A
  `StoreQualified` status condition carries the gate state (Pending while the
  Job runs, Succeeded on completion, Failed with the Job's message once its
  backoffLimit is exhausted). The qualified inputs (bucket, region, endpoint,
  image, credentials Secret name and resourceVersion) are hashed into
  `status.storeQualifiedHash`, so a later pass with unchanged inputs proceeds
  without re-running qualification even after the Job's TTL garbage-collects
  it; an input change deletes and recreates the Job and flips the condition
  back to Pending. The Job as a whole is bounded by `activeDeadlineSeconds`
  (1400 s across both attempts), so a hung attempt fails rather than holding
  `StoreQualified` at Pending; a Failed Job is
  deleted and recreated on the next pass instead of sitting until its TTL,
  and a status-only reconcile no longer re-triggers the gate. The kind
  lane's hand-run qualification Job is removed since the lane now deploys
  through the operator.
- **The object-store conformance suite probes concurrent creates, listing
  order, and deletes** (issue #1302). Four new probes close gaps the
  qualification suite left open: `ConcurrentCreateIfAbsentSingleWinner` races
  eight writers on one absent key and checks for exactly one winner,
  `LexicographicListingOrder` and `CrossPageListing` check listing order and
  `start_after` resumption across page boundaries, and `DeleteVisibility`
  checks that a delete is reflected in both a follow-up get and a follow-up
  listing. The suite tolerates the behaviors the object-store contract
  explicitly permits: a key repeated across a listing page boundary, and a
  losing racer's write landing before its own retryable-conflict response is
  retried. The shipped Admin IAM template gains `s3:DeleteObject` on
  `sys/qualify/*` so the delete probe can run under it; no other resource
  gets a delete grant. `CONFORMANCE_SUITE_VERSION` stays at 1 on purpose, so
  a bucket that already carries a `sys/qualification` record does not
  re-qualify and never runs these four new probes.
- **A derived per-tenant alert-state memo bounds alert evaluation cost**
  (issue #1294). The alert evaluator previously re-folded a tenant's entire
  `Signal::Alerts` transition history on every tick, so cost grew with the
  cumulative transition count rather than the rule count. A durable memo at
  `t/<tenant_hash>/a/state/latest` now seeds each tick's fold, and a tick
  reads only the commit records after the memo's watermark. The watermark is
  bound to the reader's own seal-bound hour: a watermark the memo carries
  above that seal-bound hour is clamped down to it rather than trusted, so
  an evaluator with a stale or ahead clock cannot skip a legally late
  transition. A memo with a duplicate `alert_id` is treated as a decode
  failure rather than served, and a failed memo encode leaves the previous
  memo untouched instead of overwriting it with corrupt bytes. A missing or
  unreadable memo falls back to a full fold.
- **A bounded top-k optimizer rule for `GROUP BY ... ORDER BY ... LIMIT k`**
  (issue #1402). A grouped aggregate whose only consumer is an
  `ORDER BY ... LIMIT k` on a `min`/`max` aggregate over a non-nullable,
  non-float column now keeps only the top k groups in a priority map instead
  of materializing one accumulator per distinct group; ADR-0013's
  exact-semantics contract is unaffected because the rule fires only for an
  aggregate expression that is provably exact under the bound (max under
  DESC, min under ASC).
- **The operator hardens every rendered pod and scopes its own Secrets RBAC
  per namespace, keeping a cluster-wide watch on `RavelCluster`.** Every
  rendered gateway, query, maintain, and ingest-router container now carries
  a `SecurityContext`: `runAsNonRoot`, no privilege escalation, every Linux
  capability dropped, and a read-only root filesystem (safe because the only
  local write any of these processes can opt into is the disk cache, and the
  operator renders no `--cache-dir` flag). The operator's ServiceAccount no
  longer holds a cluster-wide read on every Secret; it reads a
  `RavelCluster`'s referenced Secrets through a namespaced grant in that
  object's own namespace, while its watch on `RavelCluster` objects stays
  cluster-wide so a cluster outside `ravel-system` still reconciles. Serving
  a namespace other than `ravel-system` also needs the namespaced
  RoleBinding this release ships (`deploy/k8s/operator/secrets-rolebinding.yaml`)
  copied into that namespace, or Secret resolution 403s. The same change
  adds a PodDisruptionBudget per tier (`maxUnavailable: 1`) and preferred
  pod anti-affinity to every rendered Deployment.

### Changed

- **`ravel-server` in mode `all` or `query` now installs the query-audit
  pipeline and records query text tokenized by default (`--audit-text
  redacted`).** A process with no key refuses to start: an unkeyed deployment
  (`--tenant-hash-unkeyed`) must set `RAVEL_AUDIT_TOKEN_KEY` to 64 hex
  characters or pass `--audit-text plaintext`; a keyed deployment
  (`--tenant-hash-key-file`) derives the key and needs no action. Gateway and
  maintain processes are unaffected. The docker-compose quickstart now ships a
  development key.
- **CAS-mutable sys records (provisioning, tenant config, metric metadata)
  move to format_version 2** (ADR-0066). An earlier release added fields to
  these records without a version bump, so a lagging binary reading a
  current record silently dropped the fields it did not model and wrote the
  stripped record back under compare-and-swap. Every reader gate for these
  three records now accepts exactly the version set {1, 2} instead of a
  ceiling-only check, and a CAS rewrite now refuses a record whose version
  exceeds what this build's writer stamps rather than re-encoding it through
  an older field set. Every writer now stamps new records at version 2. The
  background metric-metadata refresh reads through the same permissive
  reader the cache miss path uses, so a tenant's record upgraded to version 2
  during the writer rollout is no longer rejected by refresh and stuck
  serving a stale pre-upgrade snapshot for the rest of the process's life.
  The auth token map and key epoch record read gates gained the same floor:
  a version-0, unstamped record now fails closed instead of being decoded
  and rewritten.
- **Every query transport (HTTP, gRPC, mTLS, Flight SQL) now runs through one
  query service layer** (issue #1377). Each surface previously carried its
  own copy of admission, cost accounting, and audit controls, and the copies
  had drifted: analytics and exemplars ran outside the fleet-wide
  concurrency ceiling, exemplars recorded no cost, only the SQL surface
  billed a query a client disconnected mid-flight, admission was taken
  before authentication on some routes, and a malformed `match[]` selector
  could consume an admission permit before being rejected. All four
  transports now authenticate, then admit, then run, then audit in the same
  order; a malformed selector is rejected with 400 before any permit is
  taken; a cancelled, timed-out, or failed PromQL, metadata, or analytics
  query now bills the spend it reached instead of zero; `label_values` audit
  events are tagged as their own audit language, distinguishable from the
  other label route; an audit-sink submission failure is reported with the
  audit pipeline's own failure message instead of the storage-outage string;
  and the mTLS listener gets its own instance of the service layer so a
  certificate-identified caller is resolved against the mTLS tenant resolver
  rather than the public listener's bearer-token resolver.
- **The store-request ceiling (`max_s3_requests`, `--max-s3-requests`)
  is now checked immediately after catalog resolution, combining every
  lane's spend so far, not only during segment fetches** (issue #1376). A
  query whose snapshot resolved to zero segments never reached the
  incremental checks in the fetch loop, so it could spend more catalog
  requests during resolve than the ceiling allowed and still succeed; a
  mixed metrics-and-logs query is now checked against its combined resolve
  cost across both lanes. Applies to PromQL, SQL execute, SQL explain, and
  the Flight SQL `resolve_snapshot` path.
- **Metrics compaction releases each L1 segment's encoded bytes at PUT
  instead of holding them until the record publishes.** Peak memory during
  RSEG compaction previously carried a term that grew with the whole
  bucket's L1 output; the RLOG path already had this shape (ADR-0979
  decision 3), and this applies it to RSEG.
- **The RLOG plan phase's whole-object read is carried into the scan, bound
  by plan fan-out rather than the corpus.** On the whole-object fallback,
  the plan phase and the scan each fetched the same object; the plan
  phase's bytes are now handed to the scan and charged to `bytesReused`,
  with retention bounded to the plan fan-out in objects times the object
  size. On ClickBench q20 that was 6,785 GETs and 21.1 GB, where a
  corpus-sized cache gives 4,533 GETs and 11.24 GB.
- **The operator's qualified-input hash distinguishes an absent S3 `endpoint`
  from an empty one** (issue #36). The endpoint carries a one-byte presence
  marker before its value, so `endpoint: null` no longer hashes the same as
  `endpoint: ""`. The two select different stores, so editing between them now
  re-runs qualification instead of being read as an unchanged input.
- **The operator now bounds qualify-Job recreations for a store that keeps
  failing qualification** (issue #36). A failing `ravel-cli store qualify` Job is
  recreated on a capped exponential backoff (30 s doubling to a 480 s ceiling)
  and, after 6 consecutive failures, held in a one-hour terminal cooldown that
  only an input change or the cooldown's expiry clears, rather than looping
  delete/recreate every retry interval. The failure count and next-retry instant
  are persisted in `RavelCluster` status, and the `StoreQualified=False`
  condition names the attempt count and the next retry time.
- **The operator's Secret change-detection checksum is now a `blake3` digest, 64
  hex characters** (issue #36). It replaces a 16-character standard-hasher value
  that was not stable across Rust toolchain versions. Because the annotation
  value changes, the first reconcile after upgrading the operator rolls every
  rendered gateway, query, and enabled maintain Deployment once, even when their
  referenced Secrets are unchanged; subsequent reconciles are stable. Schedule
  the operator upgrade in a maintenance window that tolerates one rolling restart
  of each serving tier.
- **`latency-first`'s published trade is re-measured and now names the commit it
  was taken on** (issue #1316). Over 3 reps on the reference corpus,
  42-statement basis, true cold in the warm-up-empty state, at concurrency 256:
  **5.30x the GET requests (570,752 against 107,781) for 52% less cold time
  (235.7 s against 493.0 s mean), per-rep range 50.3% to 54.2%**. ADR-1196
  records the commit and basis. The 0.14.0 entry below states 5.45x for 41%,
  which is correct about the run it described but predates the changes to the
  `cost-based` arm that form the ratio's denominator. The numerator (570,752)
  is unchanged and was bit-identical across all three reps, so only the
  denominator moved. The ratio is a measurement of two code paths at a point in
  time, not a property of the policy. The timing half also carries more noise
  than previously assumed (6.7% to 14.8% per-arm spread across reps on the
  measurement host), so the figure is a mean with a range, not a constant.

### Fixed

- **OTLP HTTP gzip inflate is charged against the ingest byte budget as it
  decompresses** (issue #1297). The OTLP HTTP metrics/logs/traces handlers
  decompressed request bodies up to 64 MiB before the router's own buffer
  charge ran, so up to `--max-inflight-ingest-requests` copies of a 64 MiB
  inflate could exist at once, entirely outside `--max-ingest-buffer-bytes`.
  Each chunk is now charged to the process-wide ingest budget before it is
  retained, in exactly-sized allocations rather than a growing buffer, and a
  decompression whose running total would cross the ceiling is shed
  mid-inflate (429) rather than allocated in full; a body over
  `MAX_DECOMPRESSED_OTLP_BODY_BYTES` is refused with 413 at the first byte
  past the cap rather than after the whole body is decompressed.
- **`--dev-insecure-tenant-header` is refused unless every listener (HTTP
  and gRPC) binds loopback, not just `--listen-http`** (ADR-0009, issue #94).
  The flag was reachable, unguarded, on a non-loopback `--listen-grpc`,
  letting an unauthenticated request forge tenant identity on the gRPC and
  Flight SQL surfaces. The same commit also refuses `--distributed-query`
  and `--fragment-listener` under gateway-only and maintain mode; a
  configuration that previously started with either flag set under one of
  those modes now fails at startup.
- **RLOG scan decompressed-byte accounting is complete** (issue #1401). A
  logs scan now reports the zstd work it actually does: the directory
  sections a segment open decompresses, each block page decode, the POSTINGS
  probe's term-block decode, and the plan phase's fallback and eager-funnel
  decodes (the alerts and audit scans) all charge `ScanStats.decompressed_bytes`.
  Several of these sites previously charged nothing, so a query relying on a
  text arm, a stream filter, or a below-threshold object could report zero
  decompressed bytes however much it actually decoded.
- **A column-stats object the reader refuses to decode (for example one
  whose uncompressed body exceeds the 256 MiB decode ceiling) now logs one
  warning per (tenant, signal, key) and degrades to no statistics, instead
  of doing so silently.** Previously this folded into the same silent
  "no statistics" outcome as a segment with no stats object at all. The
  per-tenant refusal marks are swept so a tenant that never resolves does
  not accumulate them without bound.
- **Duplicate attribute keys within one record now resolve last-wins by a
  fixed, documented order, consistently across SQL, ingest, and
  maintenance.** The winner is fixed by the order `rebuild_record` lays a
  record's attributes out: columnar occurrences first, ascending by
  FIELD_DIR type byte, then `attrs_raw` overflow occurrences ascending by
  canonical encoded value bytes, last entry wins. This is the record's
  on-disk layout order, not its write order, which the format does not
  preserve; four consumers of the merged attribute view previously
  disagreed about which occurrence a query resolved to.
- **RLOG corruption returns HTTP 500, not a retryable 503.** The local
  PromQL log path folded every RLOG fault, including corruption, into the
  same error class as a transient store error, so a Prometheus client
  retried a query against corrupted stored data forever.
- **Non-retryable OTLP HTTP write errors return 400 instead of 503** (issue
  #1298). A permanent failure such as a series value-kind mismatch
  previously fell through a catch-all that mapped it to 503, so a
  well-behaved exporter retried a request that could never succeed; the HTTP
  handlers now mirror the gRPC side's retryable/non-retryable distinction.
- **A raced `CreateIfAbsent` write that the backend answers with a
  retryable 409 is retried instead of reported as a permanent conflict.**
  `object_store` 0.14 maps every raw 409 to `AlreadyExists`, including AWS's
  own retryable `ConditionalRequestConflict` 409, which is distinct from a
  genuine already-exists. The S3 adapter now disambiguates with a HEAD: key
  present stays `AlreadyExists`, key absent becomes a retryable `Transient`.
  A HEAD probe that itself fails transiently (throttled, timed out) is now
  surfaced as retryable rather than folded into `AlreadyExists`, which had
  made the commit-publish path treat a race it did not lose as lost.
- **Erasure requests no longer complete while a bucket in their scope is
  still unsealed at acknowledgement** (ADR-0064). A pending erasure request
  could be marked done while an in-scope bucket was still unsealed,
  including one that had not yet published a commit record at all and so
  never appeared in the completion pass's own listing; the subject's
  records then reappeared as soon as the request's exclusion filter was
  released. Completion now blocks on every ack-time-open bucket, whether or
  not the listing discovers it.
- **The shipped KMS IAM templates scope their KMS actions to a placeholder
  tenant key ARN instead of every key in the account and region.** The four
  templates (gateway, query, maintain, admin) previously granted their KMS
  actions on `key/*`: gateway, query, and maintain hold
  `kms:Encrypt`/`GenerateDataKey*`/`Decrypt`, and admin holds `kms:Decrypt`
  only. An operator using SSE-KMS must add, not replace: one array entry
  for the `--s3-kms-key` ARN if that flag is set, plus one for every key in
  `--tenant-kms-config`, in every role's KMS Resource array; otherwise
  gateway, query, and maintain PUTs fail with AccessDenied and reads of
  objects written under that key fail decryption for every role.
- **`ravel-cli maintain migrate` reports a permanently blocked straggler
  correctly and only counts a bucket as permanently blocked when it
  actually is.** A below-target L0 segment that only a losing compaction
  record names is live data the migration walk cannot touch, and CLI/guide
  text now says re-running will not help instead of telling the operator to
  re-run; conversely, a bucket flagged by a concurrent compaction or
  erasure landing mid-walk is no longer counted as permanently blocked, and
  a record retention deletes between the walk's listing and its read no
  longer aborts the migration or produces a false permanent-block report.
- **Compaction refuses to compact a bucket whose erasure-rewrite record is
  already durable when the compactor lists it.** Publishing a second
  compaction record set over inputs a rewrite already covered could
  resurrect records the rewrite had erased once the pending-request filter
  that was hiding them was removed. The two passes still list-then-act with
  no CAS between them, so the maintenance driver's per-bucket serialization
  stays load-bearing.
- **The catalog's min-token fallback no longer serves parts from a losing
  compaction record.** Two overlapping live compaction records in one
  bucket could serve the losing record's parts in addition to the winning
  record's already-resolved parts; logs and spans have no query-time dedup,
  so this returned duplicate rows.
- **A carried whole-object read is now bound to the exact segment and
  tenant that produced it.** A carry could previously be handed to a read
  of a different object with no error, decoding the wrong object's rows
  wherever both objects were decodable.

## [0.14.0]

### Added

- **A `latency-first` logs fetch policy** (ADR-0996 amendment, superseded by
  ADR-1196). Measured on a reference cold-cache corpus, the `cost-based`
  default resolves to whole-object reads and moves 3x the bytes of
  `byte-minimal` at a deployment where transfer and retrieval are free,
  because the derived per-request rate saturates. `--logs-fetch-policy
  latency-first` resolves the same byte quantities as `byte-minimal`. It is
  an intent, not a tuning constant: it carries no concurrency default of its
  own, and resolves `--store-get-concurrency`, `--sql-partition-count`, and
  `--promql-fetch-fanout` exactly as every other policy does. `cost-based`
  stays the default; `latency-first` is an operator opt-in for deployments
  where cold wall-clock matters more than the request bill, and it pays off
  only once the operator raises concurrency explicitly to the measured
  configuration (`ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY`, 256), at
  a measured cost of about 5.45x the GET requests for about 41% less cold
  time (re-measured after this release as 5.30x for 52%; see the Unreleased
  entry and issue #1316). Raising that concurrency also raises in-flight
  fetch memory, which
  is not yet bounded by a process-wide budget (issues #1170, #1007); the
  flag's own documentation and a startup log line under `latency-first` make
  that precondition operator-visible.
- **The `alerts` and `audit` SQL tables** (ADR-1101). `POST /api/v1/sql` and
  Flight SQL serve five tables, and Flight `GetTables` lists all five; naming
  two in one query is still rejected before any listing. `alerts` is alert
  history, one row per state transition, and each row carries the write
  identity of its record (`writer_id`, `writer_epoch`, `writer_seq`), so a
  `ROW_NUMBER()` fold ordered by time and write identity returns exactly one
  current row per alert even when two evaluators overlap at a lease handover.
  `audit` reads back a tenant's own legal-hold and reshard records, which the
  maintenance process writes directly. It also serves query-audit records, but
  no shipped startup path installs the pipeline those go through, so
  `attrs['kind'] = 'query'` selects nothing until a deployment attaches one.
- **A read-side shard floor for fixed-shard signals** (ADR-1101). Alert and
  audit writers pin their shards by constant and neither signal is provisioned,
  so the catalog's scan-set derivations now take the maximum of the
  provisioning history and the signal's fixed shard count. An `audit` query on
  a `--shards 1` deployment reads the query-audit shard instead of silently
  omitting it, and a wider deployment scans exactly as before.
- **`ravel-memory`, one process-wide memory budget** (ADR-1170). The server
  derived four memory ceilings from the host and enforced each in a component
  that knew nothing of the others, so N tenants could each reserve half the
  box. `ravel-memory` is a leaf crate holding one counter every ledger draws
  from: a compare-and-swap reserve for the SQL adapter and an RAII reservation
  for the fetch layer.
- **Bloom pruning for PromQL `__body__` matchers** (ADR-1103 follow-up). A
  `__body__` equality, or an anchored regex with a token-bounded mandatory
  literal run, now pushes that literal onto the scan as a `has_word`
  predicate, so the RLOG block bloom skips blocks before decode. The
  per-record check still runs on every decoded record: the pushed word only
  prunes, it never decides. Negated matchers, unsupported metacharacters and
  `+`-quantified patterns are rejected by the extractor rather than pushed,
  because token matching is not a superset of substring matching.
- **A gate against wall-clock waits in injected-clock tests.**
  `scripts/check-injected-clock-helpers.sh`, run by `gates.sh` and CI, fails on
  `thread::sleep`, `tokio::time::sleep`, `tokio::time::timeout`, `Instant::`,
  `SystemTime`, a bare `sleep(...)` or `.elapsed()` inside a helper that takes
  a `TestClock` or `FixedClock`, unless the line carries
  `// allow-wall-clock: <reason>`. Its default scope is the loader's test
  module in `ravel-cli`; the one wait it found there was made clock-driven
  rather than exempted.
- **`scripts/verify-dispatch-gates.sh --with-gates`** runs `gates.sh` itself
  inside the cold worktree instead of a hand-listed command set, so a
  dispatched branch is checked against the same feature lanes CI runs and the
  run leaves a gate receipt the merge script can reuse.

### Changed

- **Cache warm-up keys off each tenant's latest ingest hour, not the current
  hour.** On a tenant whose data is older than the warm-up window the previous
  pass issued about 1,900 small object reads from the first query's own path
  before warming nothing; those reads are gone, and the first query on a cold
  process is about 3 s faster on the reference corpus. The replacement probe
  costs about 1 s at startup, so the end-to-end saving on a cold start is
  about 2.5 s, not 3. The probe fans out on the configured resolve concurrency,
  asserts tenant isolation on every listed key, and is bounded per shard.
- **Catalog resolve GET concurrency is configurable, default 128.** Every
  record GET in `Catalog::resolve_impl` passed through a fixed bound of 16.
  Measured against S3 on a 10,000-record unsealed tail, one cold resolve each:
  23.2 s at 16, 4.4 s at 64, 2.3 s at 128, with the same 10,001 GETs at every
  level. Cold resolve is concurrency-bound; this was the lever.
- **SQL reservations are charged to the process budget** (ADR-1170). Every SQL
  reservation now flows query, then tenant, then process on the way up, with a
  process refusal rolling the tenant and query charges back before surfacing
  as `ResourcesExhausted` naming the process figures. The infallible grow path
  trips a ceiling breach when the process limit is exceeded, so a DataFusion
  overshoot still ends in a typed error on the stream's next poll rather than
  an unaccounted allocation.

- **`--fetch-concurrency` unbundled into three flags** (ADR-1195): the SQL
  scan partition count, the PromQL/analytics per-query fetch fan-out, and the
  object-store GET concurrency were one knob with three coupled effects; they
  are now `--sql-partition-count`, `--promql-fetch-fanout`, and
  `--store-get-concurrency`, each independently sizeable. `--fetch-concurrency`
  still sets all three together for a config that predates the split (source
  `legacy-flag` in the startup log). Combining it with any of the three new
  flags is a startup error naming both flags, and a value of `0` in any of the
  four is a startup error naming that flag, raised before any fetcher, engine,
  or SQL session exists.
- **GET concurrency is process-wide, not per engine** (ADR-1195): `ravel-server`
  now builds exactly one `Arc<GetLimiter>` where it assembles its shared state
  and hands that same `Arc` to every fetcher- and engine-construction site in
  the process (the PromQL query path, the SQL executor's RSEG/RLOG/RSPAN
  fetchers, the distributed fragment path, cache warming, exemplars, and
  alerting). Before this, each RSEG and RLOG fetcher held its own semaphore, so
  N fetchers each configured to "8 concurrent GETs" could together put 8N GETs
  in flight against the store. Two behaviour changes follow. RSEG fetchers now
  honour the configured limit instead of the compiled default of 16, so a host
  whose derived value is below 16 issues fewer concurrent GETs than before.
  RSPAN fetchers are bounded for the first time: span reads previously ran with
  no GET limit at all, so a span-heavy deployment can see lower read
  concurrency and should size `--store-get-concurrency` for it. No fetcher in
  the server process owns a private limiter anymore.

### Fixed

- **The RLOG plan phase's whole-object read is carried into the scan.** On the
  whole-object fallback, `plan_segment` fetched each object to plan it and the
  scan fetched the same object again. The plan phase now hands its bytes to
  the scan, which short-circuits on them before any GET and charges them to a
  `bytesReused` figure rather than a cache hit. Retention is bounded: the
  first segments to complete their plan keep their buffer, up to the SQL
  partition count, and every later segment is re-fetched exactly as before, so
  peak retained bytes are the partition count times the object size, never
  the corpus. The saving is therefore about one duplicate read per unit of
  plan fan-out; removing the rest needs the carry to stream per partition
  instead of being held at the plan barrier, tracked separately.
  **Correction.** This change landed after the 0.14.0 tag and ships in
  0.15.0, where the retention bound is the plan fan-out rather than the SQL
  partition count.
- **The RLOG raw prefetch is gated on the cursor budget before `try_join!`**
  (ADR-0979 decision 4). The merge cursor's refill fetched the next two
  row-group blocks before the budget had been checked, so up to twice the
  group size was allocated and only accounted for on the following iteration.
  The pending fetch window is now priced from resident metadata and reserved
  before the fetch is issued.

## [0.13.0]

A stock `ravel-server` now sizes its query budgets from the host it runs on, so
a deployment no longer has to know six flags to scan a large tenant, and a
container sizes against the memory it may actually use. The two catalog defects
the 0.12.0 notes listed as known limitations are fixed, and the object-store
contract is checked by a TLA+ harness in CI.

### Added

- **TLA+ verification harness** (ADR-1113). `scripts/check-tla.sh` runs TLC over
  every area under `formal/tla` with `smoke`, `exhaustive`, `negative`,
  `traceability`, `ci`, and `all` subcommands; the TLC jar is pinned by sha256
  (or supplied through `RAVEL_TLA_TOOLS_JAR` and verified, never downloaded),
  Java 17 or newer is required, and every run writes one row per config to
  `.cache/tla/last-run.tsv`. The first area models the object-store contract
  (`docs/object-store-contract.md`): create-if-absent single winner, CAS on a
  fresh version, read-after-write including lost responses, monotonic versions
  across delete and recreate, multipart invisible until complete, listing
  completeness and consumer consistency. Three negative controls must fail with
  the exit code and property their `.expect` file pins (two invariants, one
  liveness property), state-space bands are enforced on passing runs, and a
  traceability table maps each requirement to its invariant and Rust symbol,
  naming the rows whose backend half is still an assumption. CI runs the fast
  lane when a formal area, the harness, an implementation crate the models cite,
  or a normative document changes; `tla-nightly.yml` runs the exhaustive lane on
  a schedule.
- **`ravel-cli cache reclaim-legacy --cache-dir <dir> [--apply]`** (#826).
  Lists (dry run) or deletes cache entry files left at the pre-namespacing
  `<cache-dir>/<shard>/<file>` layout, which the current cache never reads,
  evicts, or counts. Only entry files whose names map back to a cache key are
  touched; a foreign file keeps its directory. Safe while a node is live.
- **Partial multi-shard commits are reported for metrics and spans** (#1130).
  `WriteError` and `SpanWriteError` gain a `PartialWrite` variant matching the
  log router's: both routers now await every shard's acknowledgement and return
  the durable sibling tokens when some shards committed and others failed. The
  partial-commit count is exported as `ravel_ingest_partial_writes_total` for
  all three signals.
- **`sql_latency_bench --logs-fetch-policy` and `--logs-block-range-threshold`**
  (#1139), mirroring the server's flags with the same names and defaults, so the
  in-process lane routes logs fetches the way `ravel-server` does: at the default
  cost-based policy every object is read whole in one covering GET.
  `--logs-request-cost-bytes` is now optional and wins over the policy when set.
  Report provenance records the policy and the effective threshold, and a figure
  the report cannot know is labelled "not recorded" rather than as the server's
  configuration.

### Changed

- **Server budgets are resolved at startup, most of them from the host**
  (#1141, amending ADR-0088). When the flag is unset, `ravel-server` now
  resolves: `--fetch-concurrency` to twice the available cores (floor 8), the
  fetcher read cache (`--cache-max-bytes`) to 80% of usable memory and the
  catalog byte cache to 5%, `--sql-max-query-bytes` to 25% and
  `--sql-tenant-max-bytes` to 50%. `--max-segments` (1,000,000) and
  `--gc-max-query-duration` (11 minutes, still validated against the durable
  `sys/gc` ceiling) are fixed defaults that do not vary with the host. Usable
  memory is `/proc/meminfo`'s `MemTotal` **capped by the cgroup memory limit**
  (cgroup v2 `memory.max`, else v1 `memory.limit_in_bytes`; `max`, the v1
  no-limit sentinel, `0`, and malformed content are treated as no cap), so a
  container no longer sizes its caches and pools against host memory it cannot
  use. An explicit flag wins; an explicit per-query SQL pool raises a
  non-explicit tenant ceiling rather than being clamped by it, and an explicit
  `--cache-max-bytes` bounds both caches as before. Where memory cannot be read
  (a non-Linux host), the memory-derived values fall back to the previous
  constants. The startup log names each resolved value, its source, and the
  resolved deadline in milliseconds. These ceilings are LRU caps, not
  reservations. Before this change a freshly loaded ClickBench tenant (8,424
  objects) could not be scanned at all against the previous 1,024 segment cap;
  the measured ClickBench figures for a server at these defaults are recorded on
  #968.
- **Overlapping compaction records resolve to one authoritative record**
  (#1070). When two compaction records in one sealed bucket name overlapping
  input sets, the catalog keeps one winner per overlap group (largest input set,
  then smallest `input_set_hash`, then record key), serves its parts, and serves
  an input only a losing record names as a raw L0 segment, so logs and spans are
  served once instead of twice. The superseded-input sweep and the erasure
  completion gate follow the same choice, so an input only a loser names is
  never deleted from under a query. Publish-time refusal of a second overlapping
  record is left to a follow-up in `ravel-maintain`.
- **Declared-column statistics are stamped in one slot-keyed pass per record**
  (#1135). The bulk-load stamp no longer rescans a record's occurrences per slot
  or allocates per record; on the 104-column ClickBench shape it measured 11.39x
  faster per record on the measuring host, with byte-identical output. The
  bundled benchmark enforces a 2x floor, not the measured ratio, which is host
  dependent.
- **A timed-out or cancelled query records the cost it incurred** (#840) instead
  of a zero-cost outcome; an object-store GET is counted when it is issued, its
  bytes when it completes.
- **Alerts and audit scan sets are floored at their pinned shard counts.** A
  `--shards 1` deployment silently dropped every query-audit record from every
  audit query, with no error and no counter; the fixed read-shard count is now a
  floor (1 for alerts, 2 for audit).
- **CI: each push to `main` has its own concurrency group** (#1145), so a queued
  main run is no longer cancelled by the next merge and a release commit can
  always obtain the green `ci.yml` run the publish gate needs.

### Fixed

- **Erasure and GC holds** (#1085, ADR-0064 amended in #1140). The
  superseded-input sweep is gated on live-HEAD reachability, so an input a
  HEAD-named snapshot part still resolves is held rather than deleted; a
  supersession chain is deleted as one unit, its own records last of all, so a
  rewrite record outlives every input it superseded; an erasure request's
  `.dreq` and its query-time filter are held past their horizon while any input
  a rewrite applying that request superseded is still in the store, with the
  hold read off the sweep itself rather than a completion field the production
  writer never populates; the hold is observed on every chain in scope, young or
  aged; request ids are compared in one canonical form; a chain group with a
  legally held key is skipped whole; and a part reference whose declared bounds
  disagree with its header blocks fail-closed. Before these fixes an erased
  subject could become servable again after its filter was retired while its
  pre-rewrite inputs were still present.
- **Idempotent retry of a partially committed write** (#1130). The consistency
  model and the counter comments claimed a keyed retry of a timed-out or
  partially committed write is deduplicated; the idempotency marker is written
  only after a fully acknowledged write, so the key deduplicates from the first
  retry that commits in full. Every partial-commit warning carries the tenant
  hash.
- **`cache reclaim-legacy`** removes regular files only (a symlink or directory
  with an entry-shaped name is left alone) and fails on a listing error instead
  of under-reporting (#826).

### Documentation

- ADR-1103 decides PromQL over logs: the logs signal exposed to the existing
  PromQL engine as `ravel_log_lines` and `ravel_log_bytes`, with a `__body__`
  matcher. A decision record only; no endpoint ships in this release.
- ADR-0873 is amended to the shipped behaviour: an erasure rewrite part carries
  no declared min/max stamp at all, replacing decision 3's never-implemented
  recompute.
- The catalog and concepts pages state the overlapping-record guarantee and the
  full tie-break; the deletion and GC document states the real inputs of the
  erasure hold and why it terminates; the ingest and consistency pages qualify
  partial-commit retryability; the query, configuration, caching and
  admission-limits guides state which budgets resolve from host resources and
  which are fixed; the ClickBench internal pages record the new bench flags and
  note that passes taken before them are not comparable with passes at defaults.

### Known limitations

- Query latency still depends on the tenant's working set fitting in the read
  cache; removing the full-scan floor is tracked in #849. The derived cache
  default makes that working set fit on a host sized for the tenant, but does
  not remove the floor.
- The heaviest ClickBench aggregates over the whole table can exceed the derived
  per-query SQL pool on a 30 GB host and abort with `query memory budget
  exhausted`. Raise `--sql-max-query-bytes` (and the tenant ceiling with it) to
  run them.
- The read cache and the SQL pools are sized independently, so their ceilings
  can sum past the host's memory. They are LRU caps rather than reservations, so
  this is a policy gap rather than a measured fault; coordinating them under one
  process-wide budget is tracked in #1170.
- Completion records carry no per-bucket dropped counts from the production
  writer; the erasure hold no longer depends on them.

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
copying runs verbatim. Measured over 500 series at a 15-second scrape, a
merged L1 object costs 2.50 to 3.00 bytes per sample on representative value
shapes (integer and low-precision-decimal gauges and counters), the arms
ADR-0092's 2026-08-21 amendment identifies as representative. The 26.52 to
8.88 bytes per sample and 2.99x reduction quoted here previously is the
incompressible-value control arm (full-mantissa random floats), which that
amendment reclassifies as a worst-case bound rather than the representative
cost.

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
