# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **`ravel-server`'s catalog byte cache is sized independently of
  `--cache-max-bytes`** (ADR-2023, issue #2023). `--cache-max-bytes` now
  bounds the query fetcher cache only; the catalog byte cache derives its
  own share of the process memory budget (`CATALOG_CACHE_MEMORY_PERCENT`,
  5%) regardless of `--cache-max-bytes`, or takes a new
  `--catalog-cache-max-bytes <BYTES>` flag explicitly. A deployment that
  relied on `--cache-max-bytes` to grow the catalog byte cache above its
  default now also needs `--catalog-cache-max-bytes` set explicitly to get
  the same catalog cache size. Startup still refuses to start when the two
  caches' resolved ceilings together exceed the process memory budget,
  exempting `--disable-cache` as before.
- **`ravel-server` derives a larger fetcher-cache share on a loopback S3
  store** (ADR-2023, issue #2023). Unset, `--cache-max-bytes` used to
  always derive 25% of the process memory budget; now, a `--store s3`
  deployment whose `--s3-endpoint` is loopback (the same predicate
  ADR-2014 uses for `--logs-fetch-policy`) derives 40% instead, since a
  loopback store has no network cost to amortize with a larger cache. An
  explicit `--cache-max-bytes` always wins, and every other deployment
  keeps the 25% share. The resolved value's source (`budget-carve-loopback`)
  is logged on the `performance default resolved` startup line alongside
  `cache_max_bytes`.

## [0.18.0] - 2026-09-26

### Changed

- **`ravel-server` defaults to `byte-minimal` logs fetching against a
  loopback S3 endpoint** (ADR-2014). Unset, `--logs-fetch-policy` used to
  always resolve to `cost-based`; now, a `--store s3` deployment whose
  `--s3-endpoint` is loopback (`localhost` or a loopback IPv4/IPv6 literal)
  resolves to `byte-minimal` instead, because on such a store the cold path
  is disk-bound rather than network-bound and the whole-object reads
  `cost-based` picks at the reference cost profile spend local disk I/O a
  ranged read would have skipped. Measured on the ClickBench reference
  machine (RustFS on loopback, 42 statements): cold wall-clock 1,720.4s to
  1,186.6s, hot wall-clock 272.3s to 88.5s. An explicit `--logs-fetch-policy`
  always wins, including an explicit `cost-based` on a loopback endpoint, and
  a non-loopback `--s3-endpoint` (or `--store memory`) sees no change. The
  resolved policy's source (`flag`, `default`, or
  `derived-loopback-endpoint`) is now logged alongside the policy on the
  `logs fetch policy resolved` startup line.

## [0.17.0] - 2026-09-25

### Fixed

- **`ravel-bench` passes `clippy -D warnings` under `--all-features`**
  (issue #1925). The `read_path_accounting` binary's `Backend` enum carried
  an unboxed `S3Config` in its S3 variant next to a data-less variant, so
  `clippy::large_enum_variant` failed `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`. The build and tests were never affected;
  only the lint gate failed. The field is now boxed, with no change in
  behavior.

### Changed

- **The remaining MinIO identifiers are renamed to RustFS** (issue #2008).
  The object store moved from MinIO to RustFS in 0.16.0 (#2002), which kept
  the old names; they now match. The `RAVEL_MINIO_*` variables that gate the
  S3 contract suite and the bench smoke tests are now `RAVEL_RUSTFS_*`, and
  the old names are no longer read, so anyone running those suites locally
  must rename them. The tests `minio_contract` and `minio_ingest_read_smoke`
  are now `rustfs_contract` and `rustfs_ingest_read_smoke`, and CI's checks
  that those tests really ran match the new names. `sql_latency_bench`
  reports the backend label `"rustfs"` instead of `"minio"`, and
  `read_path_accounting`'s `Backend::Minio` is `Backend::RustFs`. Test
  fixture hostnames and doc comments follow; where a comment stated how
  MinIO specifically behaved, it now makes a store-neutral statement
  instead.

## [0.16.1] - 2026-09-25

### Fixed

- **A release whose changelog section is too long for a GitHub release body
  now publishes with that section's entry headlines instead of failing**
  (ADR-0086, amendment 2026-09-25). The v0.16.0 tag published its images, then
  its release job failed at `gh release create` with "body is too long (maximum
  is 125000 characters)": the 0.16.0 section alone is 165,435 characters, so
  v0.16.0 has no GitHub Release. `publish-images.yml` now measures the composed
  notes against a 120,000-byte budget. Over it, the section keeps its headings
  and each entry's bold headline, followed by a link to the full section in the
  tagged `CHANGELOG.md`; the generated pull request list and the downloads text
  are unchanged. If the notes still do not fit, the job fails with both sizes
  before it creates the Release.

## [0.16.0] - 2026-09-25

### Added

- **`ravel_ingest_resource_attrs_dropped_total` counts metric resource
  attributes dropped for sitting outside the label allowlist** (issue #116).
  `build_resource_labels` turns `service.name`/`service.namespace` into `job`,
  `service.instance.id` into `instance`, and a fixed allowlist of other
  resource attributes into labels; every other attribute was silently
  dropped, with no rejection, no counter, and no partial-success detail, and
  two resources differing only in such an attribute would flatten to the same
  label set and merge into one series with no signal that it had happened.
  Attributes outside the allowlist are **still dropped**: this is visibility
  only, not a fix to the drop itself or a way to configure the allowlist
  (not configurable today). The count (not the dropped keys, which are
  caller-controlled and unbounded) is carried internally as an informational
  `Rejection::ResourceAttributesDropped` from `ravel-otlp`, rendered on
  `GET /metrics` as `ravel_ingest_resource_attrs_dropped_total` by tenant,
  for the metrics signal only (mirroring `ravel_ingest_body_conversions_total`,
  not a `reason` on `ravel_admission_rejected_total`, since it is counted
  before the series cap and the write, so not a count of stored points), and
  never reaches the OTLP partial-success response: every stock OpenTelemetry
  SDK resource carries `telemetry.sdk.*` attributes the default allowlist
  does not cover, so surfacing this to senders would flag nearly every clean
  export as partial. Covers OTLP HTTP and OTLP gRPC ingest only; OTAP builds
  no resource labels at all, so it is not covered.

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

- **`Catalog::fold_with_refold_request` re-folds already-sealed ingest hours
  that receive a late compaction or rewrite record** (issue #526). The
  fold's fixed reconcile window and its retention-frontier band each cover
  only the hours near their own edge, so a rewrite landing in an hour
  outside both bands left its snapshot part naming pre-rewrite inputs
  indefinitely: the unreferenced-object sweep's HEAD-reachability gate kept
  holding those inputs, and they occupied storage until retention dropped
  the hour. A new `RefoldRequest` names the ingest hours to re-run through
  the same per-bucket classify-and-diff pass the two existing reconcile
  passes use, landing in the same single HEAD compare-and-swap; hours
  already covered by another pass, or named by no snapshot entry, are
  dropped, and the request is capped at `frontier_reconcile_max_hours`
  (default 168), spent oldest-first. A request is a hint, never a
  durability dependency: an unrequested or dropped hour just keeps today's
  behavior. The maintain-tier wiring that turns the unreferenced-object
  sweep's blocked hours into a `RefoldRequest` is separate follow-up work;
  until it lands, this entry point has no production caller and
  `Catalog::fold` behaves exactly as before.

- **Per-signal fold-liveness metrics:
  `ravel_catalog_fold_cycles_total`, `ravel_catalog_fold_failures_total`,
  and `ravel_catalog_fold_last_success_timestamp_seconds`** (issues #1306
  and #1625). A stalled catalog fold was invisible: the scheduled fold
  loop logged its outcome and dropped it, so nothing distinguished a fold
  running every five minutes from one that had not run in a day, and the
  first symptom was a slow query weeks later as unsealed ingest hours
  piled up. The three families are accumulated inside `Catalog::fold`
  itself, the single point every fold path (the scheduled loop, the
  on-demand admin route, the CLI, and the resolve bench) goes through, so
  a metric fed by only one caller cannot happen; a no-op fold still counts
  as a cycle, since a loop that wakes and finds nothing sealed is working
  correctly. Because the server spawns one fold loop per signal with no
  supervisor, the families are keyed by `signal` rather than global, so a
  dead loop for one signal cannot hide behind two healthy ones. The
  last-success gauge takes a plain store rather than a max, since a
  forward clock step under a max would latch permanently and mask a later
  genuine stall; a plain store only risks a bounded, self-clearing false
  stall from a backwards step. `docs/guides/observability.md` ships
  `RavelCatalogFoldStalled`, derived from the seal window
  (`max_flush_lifetime` 3600s + `clock_skew_allowance` 300s +
  `fold_safety_margin` 900s = 4800s) rather than a round number, plus an
  `absent()` branch so a scrape target list that drops every folding
  process entirely still fires.

- **Per-part (v3) column-statistics objects** (issue #1482, ADR-1413; the
  whole-snapshot v1 and v2 statistics they were first written alongside are
  retired later in this release, see the #1600 entry). The fold now writes one
  `.cstat` object per newly-written snapshot part, referenced by an additive
  field 7 (`column_stats`) on `SnapshotPartRef`. A part whose statistics would
  exceed `DEFAULT_MAX_COLUMN_STATS_BYTES` (256 MiB, the same ceiling the v3
  reader already enforces) degrades rather than stalls the fold: the largest
  remaining dictionary is dropped and the part re-measured, repeating until it
  fits, clearing only `dictionary_present`/`dictionary` and keeping
  min/max/count/sum exact; the fold fails a part only once no dictionary is left
  to drop and it is still over the ceiling.
  `FoldReport::column_stats_dictionaries_dropped` reports how many dictionaries
  a fold cleared this way. Objects are keyed by the content hash of their own
  bytes rather than the part's hash, so two folds that recompute a
  byte-identical part but hit different segment-fetch outcomes cannot collide on
  a key naming bytes that were never stored there; the v3 object is built and
  bound-checked before the part's own `.csnap` is written, so a refused part
  leaves no orphaned `.csnap` behind. The degrade loop tracks each dropped
  dictionary's exact byte contribution instead of re-measuring the whole part
  per drop, so a part needing tens of thousands of drops still finishes. An
  incremental fold also skips the v3 baseline fetch for any old part it is not
  genuinely re-deriving, cutting one object GET per untouched sealed part on
  every incremental fold. `ravel-maintain`'s unreferenced-object sweep now
  carries every part's field-7 key into its referenced-key set; without that, a
  v3 object outlived only by the sealed part naming it would have crossed the
  protection horizon and been swept from under it.

- **`ravel-cli catalog fold --json`, and full column-statistics visibility in
  `catalog fold` and `catalog inspect`** (issue #1598). `catalog fold`'s human
  report used to print only 10 of `FoldReport`'s then-23 fields, silently
  dropping counters such as `column_stats_dictionaries_dropped`; it now renders
  every field, and `--json` emits the whole struct as a JSON document (the store
  selection, `signal`, and `seal_margin` ride along as extra top-level keys
  rather than being lost). `catalog inspect` prints each column-statistics
  reference (the per-part field 7) as `key=... size=...`, or an explicit
  `ABSENT` marker, so an omitted line and an unset field are no longer
  indistinguishable. A new `ravel-cli inspect cstat <key>` decodes a `.cstat`
  object's envelope and header without decompressing its body, so an object
  whose declared uncompressed length exceeds the decode ceiling still yields
  every header field and an over-ceiling verdict instead of an error; under the
  ceiling it goes on to list each column's `dictionary_present`.
  `FoldReport::put_requests` previously undercounted: three of the six fold PUT
  call sites only incremented on success or `AlreadyExists`, so a store-side
  error on an otherwise-issued PUT went uncounted even though the object may
  have been durably written as an orphan; all six sites now increment
  unconditionally once the request resolves.

- **`ravel_declared_stats_drops_observed_total`,
  `ravel_catalog_fold_stamped_records_total`, and
  `ravel_catalog_fold_stamped_entries_total` for ADR-0873 declared-column
  statistics coverage** (issue #1747). The per-carrier drop tally existed
  only as an unrendered crate-internal counter, and the fold reported
  nothing about how many commit records it read with statistics stamps or
  how many snapshot entries it wrote carrying them, so a deployment
  transitioning to statistics stamping had no figure to confirm the
  rollout was actually reaching the snapshot. The drop family (labeled by
  `carrier`: `commit-record`, `compaction-part`, `snapshot-entry`,
  `cstat`) renders in every mode, including `maintain`; the coverage pair
  renders only in a mode that can fold at all, by either the background
  loop or the on-demand admin route, and covers snapshot entries built
  from both commit-record and compaction-part carriage.
  `docs/guides/observability.md` ships two alerts:
  `RavelFoldStampCoverageShortfall`,
  firing when the fold reads more stamped records than it writes stamped
  entries over an hour, and `RavelFoldStampCoverageMissing`, firing on
  `absent()` of the coverage family while log flushes are still
  happening, since a fold that predates these counters cannot emit them.

- **`FoldReport::refold_hours_reconciled`, printed by `ravel-cli`'s fold
  report** (issue #1763). The targeted re-fold pass added for issue #526
  counted the hours it reconciled only in a test-only thread-local, with
  no way for an operator to read the number. The count is now a
  `FoldReport` field, always zero on a no-op fold regardless of what a
  `RefoldRequest` named, since a no-op fold returns before reaching the
  targeted pass.

- **A structured OTLP log body (an array or a map) is now stored instead of
  being rejected, and every admission-layer rejection reason is now counted**
  (issues #1308, #1309). Normalization is admission layer 3, but its
  decisions were invisible to operators: a delta-temporality metric or a
  too-old log record was reported to the sender through OTLP partial success
  and then dropped with no counter movement, and on the OTAP surface a fully
  rejected batch left no trace at all beyond the missing points.
  `ravel_admission_rejected_total`'s reason label now classifies every
  rejection as skew or structural, so a tenant switching transport between
  OTLP and OTAP sees the same figures. Separately, a structured log body
  converts to canonical JSON text (map keys ordered by the canonical
  attribute ordering already used for stream identity, duplicate keys kept
  and ordered by encoded value, array order preserved, bytes as lowercase
  hex, non-finite doubles as `"NaN"`, `"+Inf"`, or `"-Inf"`) rather than
  being rejected outright; two exports of the same body always produce
  byte-identical stored text. A string-table-reference body is still
  rejected, since no string table travels with the export request and the
  referenced text is not reachable. Delta-temporality metrics are still
  rejected too: converting them needs a running total held between
  requests, which a disposable compute process cannot keep, so a
  collector-side `deltatocumulative` processor ahead of `batch` is the
  supported path, and the quickstart collector config now ships that way.
  Two review fixes landed alongside the conversion: a log record that was
  stored with an oversized attribute dropped, but no whole record rejected,
  previously reported nothing on the partial-success response, reading as a
  fully clean write; it now reports the drop even though the rejected-record
  count is correctly zero. And a request whose structured body was large
  enough to make conversion itself costly was, before conversion ran under a
  running byte budget, measured re-encoding a comparator on every duplicate
  key it compared; a body built to maximize that cost previously cost
  about 16 seconds of one core and built a 44 MB `String` for a 412 KB
  gzip request the size limit was about to reject anyway.

- **PromQL binary operators (`+`, `-`, `*`, `/`, comparisons) now work
  between two native-histogram series, matching the exact operator set
  Prometheus supports rather than refusing every histogram pairing with an
  error** (issue #1700). Arithmetic between differently-shaped histograms
  needed reconciliation this crate did not have: two operands at different
  exponential scales were combined bucket-index-to-bucket-index without
  first aligning scale, silently merging unrelated value ranges, and two
  custom-bucket histograms with different bounds, or two exponential
  histograms with different zero thresholds, were treated as unalignable
  and dropped, when Prometheus actually reconciles both onto a common
  layout before combining. Both gaps are now closed: a scale mismatch is
  down-converted to the coarser scale before merging, mismatched
  custom-bucket bounds are re-bucketed onto their intersection, and
  mismatched zero thresholds widen to the larger one, folding the buckets
  it swallows into the zero count, matching Prometheus' own reconciliation.
  A genuinely unalignable pair, one exponential and one custom-buckets
  operand, still drops the sample, now carrying the same warning text
  Prometheus' own evaluator emits rather than Ravel-authored wording. Two
  further correctness bugs surfaced during that work: a merge on the
  equal-threshold fast path, the common case, was deleting any bucket that
  merely straddled the zero threshold instead of keeping it, so a
  histogram added to itself could answer with no buckets at all and
  silently lose its count; and a zero threshold recorded as NaN sent the
  reconciliation routine into a comparison that never terminates, hanging
  the query thread until the process restarted. Both are fixed, and the
  NaN case is now also refused before it can reach storage: an exponential
  histogram's `zero_threshold` that is NaN, infinite, or negative is
  rejected at normalization on both the OTLP and Remote Write surfaces,
  through one shared predicate, closing the root cause rather than only
  the query-side symptom.

- **The in-flight-flush and flush-permit-wait gauges are now rendered for
  every ingest signal, not only metrics** (issue #1741). The log and span
  ingest pipelines moved their flush-permit acquire off the shard actor
  alongside the metrics pipeline, but the gauges that make a stalled permit
  wait visible, `ravel_ingest_in_flight_flushes` and the new
  `ravel_ingest_flush_permit_wait_seconds_total`, were rendered only inside
  a metrics-only code path. A logs-only or spans-only process therefore
  rendered no sample for either gauge at all, even though a real (and
  possibly zero) value existed for it. Both are now flat fields rendered
  for every `{mode, signal}` combination.

- **A process-wide memory budget now bounds SQL execution, with three new
  `/metrics` gauges** (issues #1170, #1254). A SQL statement's pooled
  reservation draws on one process-wide `MemoryBudget`. The fetch layer can
  reserve against the same budget (every RSEG `ensure_ranges` coalesced read,
  every RLOG block-range and whole-object fetch, and every RSPAN whole-object
  fetch reserves the bytes its GET will materialize before issuing it), but no
  shipped binary hands the fetchers that budget yet, so fetch buffers are not
  counted against it and `component="fetch"` reads 0 (see the #1255 entry).
  Where a fetcher is given the budget, a fetch-side refusal fails typed as
  `FetchMemoryExhausted { requested, reserved, limit }`, mapped to the frozen
  gRPC `BudgetExceeded` code rather than `Unavailable`: `Unavailable` is this
  codebase's re-dispatch-and-run-locally class, so mapping a budget refusal to
  it re-dispatched the refused slice to another worker and then ran it on the
  coordinator, amplifying the load the budget exists to shed.
  `ravel_memory_budget_bytes` (the resolved ceiling, `u64::MAX` meaning
  unlimited), `ravel_memory_reserved_bytes` by `{component="sql"|"fetch"}`, and
  `ravel_memory_handoff_overlap_bytes` are now on `/metrics`. Two startup
  defects in the derived budget were closed alongside this: a host where memory
  could not be measured (any non-Linux host) used to derive a budget of `0`
  instead of unlimited, and a `--cache-max-bytes` value that, with the derived
  catalog cache, landed at or above the derived budget used to be accepted
  rather than refused; both used to leave `MemoryBudget::new(0)` in place, which
  refuses every real SQL or fetch reservation while a statement that reserves
  nothing (`SELECT 1`) kept answering.

- **`stats.io` on both the SQL and PromQL JSON responses now reports
  `unfoldedRecordsServedFromCache`, the count of commit records a query's
  resolve served from the resolve cache instead of fetching** (issues #1199,
  #1219). The two engines share one `QueryIoShape`, so the field is read
  from `QueryAccountingSnapshot::commit_record_cache_hits` at the resolve
  site rather than inferred from a pooled counter, and a query that runs
  both a metrics and a log lane sums the field across both lanes' resolves,
  each of which runs its own resolve serially. This is a record count, not a
  segment count: the resolve's listing window is padded by
  `max_ingest_lag_ns` and always runs to the current hour, so a resolve can
  prewarm commit-record buckets a query's own time range never touches; the
  figure is meant as the numerator of a cold-resolve fraction, not a segment
  tally.

- **`/api/v1/sql`'s JSON response now carries `stats.phases` and `stats.io`,
  the same per-phase (resolve/plan/probe/scan) I/O accounting the PromQL
  endpoints already report** (issue #1367). The SQL executor's internal
  accounting seam is retyped from one pooled `QueryAccounting` handle to a
  `PhaseAccounting` split, and a new `sql_io_shape` helper derives dependency
  depth, list-page depth, service batches and plan classification the same
  way the PromQL engine's `io_shape_for_resolve` does. `RavelTableProvider`
  and `LogsTableProvider` (the metrics and logs tables) carry the phase
  split through to their scan operators; spans, alerts and audit stay on the
  pre-existing pooled accounting for now. Flight SQL constructs no
  `SqlOutcome` and has no stats envelope to extend, and the Arrow-IPC
  encoding of `/api/v1/sql` carries no stats at all, matching its existing
  behavior for the accounting and estimate fields it already omits: this is
  the one shipping surface that gains the new fields.

- **A SQL page planner turns a `SELECT` into a resumable, keyset-paginated
  statement, as internal plumbing for the paging tools landing on top of it**
  (issues #1374, #1571). `page_plan` is a pure text-to-text rewrite: given a
  statement and an optional resume position, it returns the statement to run
  next, the effective `ORDER BY`, and whether that ordering is a total order.
  The samples table gets a total order for free (each scan emits one winner
  per `(series_id, ts)`, appended as a deterministic tiebreak); the RLOG- and
  RSPAN-backed tables have no row identity under at-least-once ingest, so no
  tiebreak is appended and the plan reports why instead of claiming an
  ordering the scan can't back up. Every unsafe shape is a typed refusal
  rather than a silently wrong page: a statement carrying its own `LIMIT`,
  `OFFSET`, `FETCH`, `TOP`, a pipe operator, `ORDER BY ALL`, an order term
  that isn't a column reference or isn't projected, an explicit `NULLS
  FIRST`/`NULLS LAST` or `WITH FILL`, an order term the statement text can't
  prove `NOT NULL` on the queried table (a keyset comparison against `NULL`
  selects no rows, so those rows would silently never appear on any page), a
  resume tuple whose arity doesn't match the ordering terms, and a
  non-finite float in a resume value. `SqlOutcome` also now carries the
  three resolve inputs a cursor pins (ADR-1374 decision 5): the target
  signal, the typed attribute column set the query resolved, and the
  erasure
  predicates pending in the snapshot it read, each read off the successful
  attempt's own snapshot so a retry can't substitute another attempt's
  values. As of this release nothing in `ravel-server` or `ravel-mcp` calls
  `page_plan` yet; it is tested directly and awaits its caller in a later
  wave.

- **A native MCP (Model Context Protocol) adapter is available behind the
  off-by-default `mcp` build feature and a `--mcp` runtime flag, exposing its
  nine-tool catalog over `POST /mcp`** (issue #1379, ADR-1374 decision 9). The
  new `ravel-mcp` crate ships `ravel_capabilities`, `ravel_describe_data`,
  `ravel_find_labels`, `ravel_explain_query`, `ravel_query_sql`,
  `ravel_query_promql`, `ravel_search_logs`, `ravel_get_trace` and
  `ravel_analyze_timeseries` by name. Only `ravel_capabilities` has a body in
  this release: the other eight answer every call with a typed `NotShipped`
  protocol error naming the tool, and `ravel_capabilities` reports which tools
  are served apart from the full catalog. The `mcp` feature implies `sql`, since
  four of the nine tools are built to execute SQL through the query service.
  Every tool response is bounded before it reaches the wire: cursors are opaque,
  MAC'd, self-describing tokens bound to the call that minted them, envelope
  cells are sized by their serialized length rather than by field count, and a
  compact text rendering is capped at 20 rows and 64 KiB with every caller
  string escaped and control characters stripped. `ravel-sql` gained a
  `pin-codec` feature so the cursor codec can reuse `FlightTicket`, `TicketKey`
  and `SegmentPin`'s keyed-MAC pattern without linking Arrow Flight.

- **A hex-string `trace_id` literal now plans, alongside the existing
  `X'...'` byte-literal form** (issue #1709). The traces guide documents
  looking up a trace by its 32-character hex string, but comparing the
  `FixedSizeBinary(16)` `trace_id` column against a `Utf8` literal failed
  type coercion, so the documented query returned a planning error and only
  the byte-literal spelling worked. A new expression planner rewrites a
  `trace_id` comparison against a 32-character hex literal (case-insensitive,
  either operand order) into the binary form for both `=` and `!=`; a
  literal of the wrong length or containing a non-hex character is left
  alone and still fails to plan, rather than silently matching nothing.

- **The spans table gains a structured `events` column,
  `List<Struct{ts_unix_nano, name, attrs}>`, decoded from the RSPAN v4 event
  columns instead of requiring callers to decode `_events_raw` protobuf by
  hand** (issue #1710). Event attributes use the same label-map type as the
  rest of the schema. Projections that exclude `events` still take the
  columnar fast path, and pushdown ignores the column. `_events_raw` stays
  for compatibility, and span links remain hex-attribute-only until a
  follow-up lands. The JSON output encoder gained `List` and `Struct` cases
  to render it, since the column is reachable from both the HTTP and Flight
  surfaces.

- **A SQL query over the samples table now warns in the response when it
  silently excluded native-histogram data** (issue #1738). The samples
  table's value column is a non-nullable `Float64` with no way to carry a
  native histogram, so a histogram sample never became a row: a `COUNT(*)`
  on a tenant that ingests native histograms was short by the whole
  histogram population, answered with HTTP 200 and no indication, and on a
  histogram-only tenant the answer was `0`. The JSON success body now
  carries a top-level `warnings` array of strings (omitted when empty, the
  same convention the PromQL endpoints already use), populated from
  `SqlOutcome::warnings` and sourced from a count the scalar fetch already
  produces for free while filtering histogram-kind series out of its
  results. Two surfaces still can't carry it: an Arrow-IPC response has no
  envelope to put a warning in, and a statement executed through the
  distributed scan lane counts nothing, because the worker that dropped the
  series streams rows rather than its own counters back to the coordinator.
  The samples table's column count is unchanged; this makes an existing
  exclusion visible, it does not narrow it further.

- **The log fetcher's assembly-buffer pool now reports the live (in-flight)
  buffer set on top of the pool's existing idle-retention figures** (issue
  #1771). `AssemblyBufferStats` described only what the pool retains between
  reads; nothing described what a running scan currently holds, which is the
  figure a memory question actually needs (under the byte-minimal fetch
  policy, a query holds one object-sized buffer per in-flight ranged read).
  New `live_bytes` and `peak_live_bytes` fields are charged when a buffer is
  acquired and released when it is returned, before the pool's retention
  bounds decide whether to keep it or drop it, so a buffer that gets dropped
  rather than pooled still leaves the live set. Charging is by the buffer's
  resident length rather than its requested length: a reused buffer keeps
  the length of the largest object it has ever served, and those bytes are
  held whether or not the current read addresses all of them.

- **`ravel_maintain_l0_records_pending` and
  `ravel_maintain_objects_deleted_total` render on `/metrics`** (issue #1729).
  The compaction scan and the retention sweep previously reported these figures
  to tracing only, so an operator could not see how much L0 compaction work was
  queued, or how many objects maintenance had actually deleted, without reading
  logs on every process. `ravel_maintain_l0_records_pending` is now published by
  `signal`, summed across every bucket this process owns and republished once
  per maintenance cycle (default 300 seconds) after the cycle has covered all of
  them, so a mid-cycle scrape reads the previous cycle's complete total rather
  than a partial sum; a bucket the cadence memo skipped for being safely below
  threshold still contributes its last-known record count, so the buckets an
  operator most needs to watch cannot silently drop out of the total.
  `ravel_maintain_objects_deleted_total` is published by `kind`, one series for
  each of the four `SweepReport` counts that record a physical delete (including
  `kind="quarantine_reaped"`; a move to quarantine and a withheld candidate are
  not deletes and are not counted). The troubleshooting guide documents both,
  and says that a dip in the pending gauge is not corroborated by
  `ravel_maintain_units_stalled`: that gauge only moves for a per-unit failure
  repeated past its stall threshold, and the paths that remove the most records
  from the pending total (a tenant skipped whole-tick for a failed legal-hold
  refresh, or by the provisioning or shard-generation check) never reach
  per-unit accounting, so `units_stalled` can sit at zero while the pending
  population moves for an unrelated reason.

- **The process memory budget now exposes gauges on `/metrics`, and startup
  refuses a container the budget cannot fit** (issues #1255, #1395).
  `ravel_memory_budget_bytes` renders the resolved ceiling (`u64::MAX` meaning
  unlimited), `ravel_memory_reserved_bytes` by component (`sql`, `fetch`), and
  `ravel_memory_handoff_overlap_bytes`; `component="fetch"` renders 0 for now,
  since nothing yet charges the fetch layer against this budget. The budget
  itself is now derived from the cgroup-effective memory ceiling minus a fixed
  overhead reserve, not raw `MemTotal`, so a container capped below the host's
  total memory is sized correctly rather than against memory it can never use.
  A process whose derived budget cannot cover its resolved cache ceilings now
  refuses to start with a typed `MemoryBudgetExceeded` error naming the
  shortfall, instead of starting and letting the caches or the SQL executor
  exceed the container's real limit later.

- **`--disable-cache` now passes the memory budget startup check** (issue
  #1436). The check previously compared the fetcher and catalog cache ceilings
  against the derived budget without accounting for `--disable-cache`, so a
  host with cache limits set above the budget was refused even though it
  builds no cache at all, and any container whose effective memory sat at or
  below the overhead reserve derived a 0 budget that no flag value could
  satisfy, including `--disable-cache` itself. The check now recognizes that
  `--disable-cache` builds neither the fetcher cache nor the catalog byte
  cache, so the full budget is available to the shared SQL and fetch
  accounting and the container starts.

- **A new MCP (Model Context Protocol) surface can be mounted on the
  query-serving listeners, behind its own feature and flag** (issue #1381).
  `--mcp` opts a build carrying the `mcp` cargo feature into serving `POST
  /mcp`; `--mcp-allowed-origins` is a mandatory origin allowlist (an empty list
  is accepted only on a loopback listener, and fails startup on any other
  address); `--mcp-max-body-bytes` caps the request body (default 1 MiB) and
  refuses a value of 0. The route runs the same tenant resolution, origin check,
  and body cap as the HTTP query surfaces before anything reaches the protocol
  layer, and each tool call is billed through the same admission permit,
  deadline clamp, cost record, usage guard, audit submission, partial-result
  gate, and error redaction as an HTTP query. Starting the process with `--mcp`
  under `--mode gateway` or `--mode maintain` now fails at startup, naming the
  flag and the mode, instead of silently mounting nothing. `ravel_capabilities`
  reports which tools are actually served (`tools.enabled`) separately from the
  full catalog (`tools.catalogued`). A `finish` response now names
  `visibility.snapshot_id`, `visibility.watermark_hour`, `ids.query_id`, or
  `ids.audit_ref` in its `warnings` list when the underlying operation left that
  field unmeasured, rather than rendering an empty string a caller cannot
  distinguish from a genuinely empty value. A malformed MCP budget argument is
  now refused rather than silently defaulted, and `row_cap_hit` together with
  the produced row count now survive through to `finish` instead of being
  dropped along the way.

- **A `--max-ingest-lag` flag replaces the hardcoded 2h ingest admission
  bound** (issue #1682). The value drives both the catalog listing window and
  the OTLP, OTAP, Remote Write, and span-surface admission bounds together, so
  the two can never be set inconsistently: the listing window widens first and
  the admission bound is derived from it. Startup validates the configured
  window against the ADR-0019 retention floor and refuses a lag that would
  outrun retention, and refuses a bound that admits data wider than the
  catalog can list. This lets a deployment replay telemetry older than 2h
  after an outage or a bulk import, which the previous hardcoded bound
  rejected outright with `Rejection::TooOld` on every ingest surface.

- **A `--tenant-token-file` flag loads static bearer tokens from a file
  instead of the command line** (issue #1706). The file source strips a
  leading UTF-8 byte-order mark before parsing, and an empty or comment-only
  file parses to an empty map with no startup error. In the same change, a
  malformed token line (missing `=`, or an empty tenant after the split) no
  longer echoes the offending pair back into the error message: for the file
  source that text is the bearer token itself, and the previous error text
  printed the secret straight into the process's stderr log on the most likely
  operator mistake, a Secret mounted with the token alone and no `=TENANT`.

- **The operator now detects the cluster's Kubernetes minor version and gates
  the preStop `SleepAction` on it** (issue #1714). Kubernetes reports
  `PodLifecycleSleepAction` (KEP-3960) as beta and enabled by default from
  1.30, not GA from 1.32 as first assumed; the floor is now minor version 30.
  A cluster below the floor gets `KubernetesVersionUnsupported` and no preStop
  hook rendered, rather than a field the API server silently drops or rejects.
  An unreadable version check (a transient API error) fails open and logs once
  rather than flapping the condition.

- **A separate admission class bounds federated fragment resolves** (issue
  #1722). `--max-inflight-federated-resolves` (default 8) caps this new
  "Resolve" class independently of the existing `--max-inflight-fragments`
  (default 32), which continues to bound the "Pinned" class alone, so a burst
  of federated resolves can no longer starve pinned fragment reads of their
  own permits.

- **The Kubernetes operator now renders default CPU and memory requests when a
  `RavelCluster` spec omits them** (issue #1726). Gateway and maintain pods
  default to 100m CPU and 256Mi memory; query pods default to 200m CPU and
  512Mi memory. A spec that sets its own requests is unaffected.

- **A new CRD field, `spec.gateway.maxInflightFlushes`, renders
  `--max-inflight-flushes` on the gateway Deployment** (issue #1743). This is
  the per-shard cross-tenant flush isolation bound: without it, an
  operator-managed cluster was stuck at the server's compiled-in default of 1,
  so one tenant's stalled flush could block every co-resident tenant's flush on
  that shard. The field is gateway-only, since ingest and its flush isolation
  only run under `--mode gateway`; when unset, nothing is rendered and the
  cluster keeps the server's own default. A value of 0 is refused, matching the
  server's own refusal of `--max-inflight-flushes 0` as a flush deadlock.

- **New maintenance-safety and admission-reconciliation counters and gauges
  are exported on `/metrics`** (issue #1762).
  `ravel_maintain_orphans_quarantined_total`,
  `ravel_maintain_orphans_quarantine_refused_total`, and
  `ravel_maintain_quarantine_reaped_total` (all labelled by mode and signal)
  report what each sweep pass did to orphaned data, figures the sweep already
  computed but the exporter did not read.
  `ravel_admission_reconciliation_cycle_duration_seconds`,
  `ravel_admission_reconciliation_siblings_observed`, and
  `ravel_admission_reconciliation_stale_keys_skipped` (labelled by mode alone,
  since one cycle reconciles every tenant the process tracks) report the last
  completed reconciliation cycle and can fall between scrapes;
  `ravel_admission_reconciliation_keys_reaped_total` accumulates across cycles
  instead.

- **Alerting pipeline metrics are exported on `/metrics`** (issue #532). Six
  cumulative quantities (rules evaluated, rules failed, records written,
  repeats queued, notifications delivered, notifications failed) become
  counters labelled by mode, and the three mutually-exclusive per-tick
  outcomes (history unavailable, lease not held, lease unavailable) collapse
  into one `ravel_alert_ticks_total` counter split by outcome alongside the
  evaluated case. `ravel_alert_last_tick_completed_timestamp_seconds` stamps
  on every tick, including one that skipped evaluation because a peer holds
  the lease, so only its age signals a stalled loop. The whole family is
  omitted unless this process built an evaluator. Because the evaluator spawns
  one task per tenant but every task stamps the same process-global gauge, a
  dead evaluator for one tenant stays hidden as long as any other tenant on
  the process keeps ticking; this is a process-wide signal, not a per-tenant
  one.

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

- **The catalog resolve path's request ceiling now derives from the server's
  query concurrency instead of sitting at a flat 128** (issue #1733, ADR-1733).
  Only the unset default moves: an explicit `--catalog-resolve-concurrency` is
  still used verbatim, and `0` or a value above 4,096 is still a startup
  refusal. Unset, the ceiling resolves to `clamp(Q * 128, 128, 4096)`, where
  `Q` is `--max-concurrent-queries` when that flag bounds queries and the
  derived `max(8, 2 x cores)` when it does not. So a server run with
  `--max-concurrent-queries 1` keeps 128, one run with
  `--max-concurrent-queries 4` gets 512, and an unbounded 8-core host
  (`Q` = 16, deriving 2,048) gets 1,024. The resolved number is logged at
  startup beside the other derived performance defaults, with the `Q` and the
  input it came from. A second bound, per key prefix and with no flag, holds
  any single prefix to 128 requests whatever the ceiling is, so a higher
  ceiling buys concurrency across prefixes and never more pressure on one.
  Every resolve-path request is bounded that way, keyed by its own key
  prefix: a commit record by its shard-hour prefix, a snapshot's parts by the
  one directory they share, its postings and column stats by theirs, and a
  LIST by the prefix it lists. A prefix's semaphore is created on the first
  request that needs it and removed once no request holds or waits on it,
  including when the last requests on that prefix finish at the same moment,
  so a long-running process holds one entry per prefix in flight rather than
  one per prefix the bucket has ever had. The 1,024 is an interim cap: the ADR bounds
  in-flight resolve memory by reserving each request's listed size against
  the ADR-1170 process budget, that reservation is not wired up yet, and
  until it is a derived ceiling is held at 1,024 rather than allowed to reach
  the 4,096 the clamp permits.

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

- **`LIMIT` now pushes into the logs scan instead of stopping at a
  `LocalLimitExec` DataFusion inserts above it** (issue #362). A previous
  attempt at this added an internal row-count stop inside the scan and
  measured no effect, because DataFusion's `LimitPushdown` already inserts a
  per-partition limit node above any scan that doesn't implement `fetch()`,
  and that node already stopped polling the scan. `LogsScanExec` now
  implements `fetch()`/`with_fetch()`, so `LimitPushdown` pushes the limit
  into the scan and removes the extra plan node per partition instead of
  leaving the scan's own bookkeeping unused. The per-partition fetch is a
  bound each partition may stop at on its own; the query's real limit across
  all partitions is still enforced above it, so no partition returns fewer
  rows than its share.

- **Reading one attribute through `attrs['k']` no longer costs 50x-220x the
  CPU of reading the same value through a typed attribute column of the
  same name**
  (issues #913, #1768). The literal keys referenced through `attrs[...]` in
  the projection and in residual predicates are now collected up front, and
  when nothing in the plan needs the whole attributes map, only those keys'
  columns (plus `attrs_raw`) are resolved and built directly as `Utf8`,
  instead of selecting every dynamic column's pages, rebuilding a
  `Vec<(String, AttrValue)>` map per row, and materializing a full
  `Map(Utf8, Utf8)` column that `get_field` then read one key out of. On the
  measurement quoted in the commit (40 objects, 200,000 rows, 33 record
  attributes), an equality statement went from 1070.7 ms to 4.8 ms and from
  84,691 to 3,531 stored page bytes decoded. The rewrite only narrows which
  columns resolve, never what a query returns: a bare `attrs` reference,
  `SELECT *`, an aggregate argument, a grouping set, a projected filter, or
  any plan node the rewrite doesn't recognise all keep the whole-map row
  path, and a per-key column still renders the map form's value, including
  the ADR-0090 decision 7 case where a non-`Str` value under a `Str`
  declaration renders as text through the map but `NULL` through the
  typed attribute column. This is a CPU-only change: the shipped fetch
  policy reads
  whole objects regardless of projection, so no wire byte or GET count
  changes.

- **The catalog now accepts only v3 per-part `.cstat` column-statistics
  objects; the v1 and v2 whole-object decode paths are retired** (issue
  #1600, ADR-1413 decision 6). `column_stats_resolve.rs` and `catalog.rs`
  drop the v2-then-v1 fallback ladder and the declared-entry-count coverage
  comparison entirely: a covered part whose per-part reference is absent,
  not found, or undecodable is now scanned directly rather than falling back
  to a whole-object read, with the existing warn-once and
  `column_stats_decode_refusals` counter still firing on that path.
  `SnapshotHead`'s whole-object v1/v2 reference fields become reserved in
  `proto/ravel/catalog.proto`, matching the fold no longer publishing either
  form. The v2 encoder is retained but gated to test code, since it is still
  useful for exercising header/envelope-mismatch and decode-refusal paths
  without a second production code path per version.

- **Orphan garbage collection now quarantines a candidate instead of deleting
  it, and runs on the full-sweep cadence instead of every maintain tick**
  (issues #528, #1734). Previously, a data object whose commit record was lost
  out of band (a bucket lifecycle rule, a prefix delete, a persistent LIST
  omission) was deleted outright at the orphan horizon (about 25 hours) with
  no recovery window, and a loss small enough to stay under
  `orphan_breaker_min_count` and `orphan_breaker_max_ratio` never tripped the
  mass-orphan breaker that exists to catch exactly this. A candidate is now
  moved to a `quarantine/<original key>/q<timestamp>` copy first, and the live
  key is deleted only after the copy succeeds, so a crash between the two
  steps can never destroy the only copy. The copy is physically deleted only
  after a second, independent horizon, `quarantine_horizon_ns` (default 7
  days), giving an operator a real window to notice and recover before data is
  gone for good. `ravel_maintain_orphans_present` (the existing mass-orphan
  gauge) now also counts a candidate whose quarantine copy failed, since a
  refused quarantine leaves the object live and is exactly the store-fault
  case the gauge exists to surface; new `orphans_quarantined`,
  `orphans_quarantine_refused` and `quarantine_reaped` counters make the event
  itself visible, each logged at warn level on any nonzero count.
  Because listing the whole L0 data prefix on every tick (every 300 seconds by
  default) was the single most expensive thing a maintain tick did, and it
  answers a question that changes slowly, orphan candidate selection, the
  quarantine reaper, and the mass-orphan breaker check now all run together on
  the same full-sweep cadence (6 hours by default, `interior_reverify_ns`)
  rather than on every tick. Gating the two together this way opened a gap of
  its own: a tick that skips selection never evaluates the breaker either, so
  it reads as "not tripped" and would have let the reaper run through a live
  incident on every skipped tick; the reaper is now chained to run only on a
  pass that actually ran selection and found the breaker clear, so a record
  loss that widens past the breaker's thresholds days after it started still
  holds its earliest quarantined copies rather than reaping them on the next
  skipped tick. Both gauges now hold their last completed-pass value across a
  skipped tick rather than reporting zero, and the per-tick sweep log line and
  both gauges say which pass kind produced them, so a tick that skipped
  selection no longer logs `orphans=0` in a way that reads as a measurement.
  `docs/deletion-and-gc.md` and ADR-0058 describe the quarantine mechanism,
  its horizon, and the incident runbook's restore-from-quarantine step
  normatively.

- **The object-store conformance suite now refuses a bucket qualified under an
  older, smaller probe set** (issue #1302). The suite grew from four probes to
  eight, but `CONFORMANCE_SUITE_VERSION` had stayed at 1, so a bucket
  qualified under the old four-probe suite still read as a current pass on
  startup while the four newer properties were never checked.
  `CONFORMANCE_SUITE_VERSION` is now 2, and the once-per-bucket re-record rule
  relaxes to once-per-suite-version, so `ravel-cli store qualify` overwrites a
  below-floor record with a current pass instead of leaving startup
  permanently refused. Startup also now compares the record's
  `backend_identity` against the connecting backend's own, warning (not
  refusing) on a mismatch: the identity is endpoint-derived, so an endpoint
  rename or a path-style/virtual-host switch changes it with no actual backend
  change underneath.

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

- **A distributed query's recorded cost now covers every slice attempt, not
  only the one that survived** (issue #1723). `RoutingSliceFetcher::dispatch`
  runs a slice up to three times (the primary worker, one re-dispatch to the
  next rendezvous worker, then coordinator-local), and all three sit below the
  `SliceFetcher` seam. A worker that had already fetched part of its slice and
  then took a store error reported its failure with a zero accounting snapshot;
  the retry classification in `try_remote` dropped whatever an abandoned attempt
  had spent; and a slice whose final attempt ended in `Err` was mapped straight
  to a `QueryError`, folding nothing. The recorded cost was therefore one
  attempt's spend where the store had really served up to three, and a slice
  that failed on every attempt was charged for nothing at all. As an
  illustration of the scale, a tenant that had configured an 8 GiB byte budget
  could drive 24 GiB of real GET traffic with the recorded total still inside
  it; the shipped default for `max_bytes_scanned` is `Unlimited`, so this is a
  gap in what an operator who sets a budget gets, not in a default deployment.
  A failed attempt's real spend now travels on its terminal summary frame, in
  the same shape the byte-budget short-circuit already used, is carried across
  re-dispatches, and is folded into the coordinator's live accounting handle on
  every terminal status and on the error path too, where a slice that failed
  outright carries on the error itself whatever its attempts had managed to
  report. This is an operator-visible behavior change: byte-budget enforcement
  (`bytes_scanned_exceeded`, and the coordinator's in-loop check) now reads the
  sum over all attempts, so a tenant near its limit is refused earlier than
  before, and a query that retried slices and previously completed can now trip
  `TooManyBytesScanned`. The bytes it is refused for are bytes the store really
  served. Per-fragment stats report the same figure: a successful fragment's
  `bytes_reported` is the sum over its attempts, and a failed one reports what
  its attempts carried instead of a flat zero. Two gaps remain. When one slice
  of a fan-out fails, the query stops and its in-flight sibling slices are
  cancelled, so the GETs their workers already issued are not reported. And
  an attempt reports its own cost only once its terminal
  summary is decoded, so any attempt that ends before that point contributes
  zero rather than a guess. That covers a stream broken mid-flight, a decode
  fault before the summary, and EVERY coordinator byte-cap or frame-cap
  refusal, since a worker streams its summary last and
  `SliceStreamDecoder::push` checks both caps before it stores a frame. Such a
  slice still carries the spend of any EARLIER abandoned attempt, so a refusal
  on a re-dispatch reports the primary's cost and not its own. Closing that gap
  needs the worker to send its accounting ahead of the frames, which is a wire
  change.

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
  overrides any Allow for the actions it names, and the template's
  `ListBucket` prefixes did not admit the catalog `snap/` and `idx/` paths at
  all, so on a deployment running the shipped template every sweep pass was
  refused at its first listing, before any delete was attempted: catalog
  garbage, including any unreferenced snapshot or index object
  holding an erased subject's value, was never reclaimed. The deny is now
  narrowed to the catalog HEAD pointer alone (`t/*/catalog/*/HEAD`), the one
  catalog object the sweep never deletes, and `MaintainDelete` now grants
  delete on `snap/` and `idx/` keys to match what the sweep already does. The
  template also gains the reads the sweep needs before it can delete:
  `MaintainList`'s `s3:prefix` adds `t/*/catalog/*/snap/*` and
  `t/*/catalog/*/idx/*`, and `MaintainRead` adds `t/*/catalog/*/HEAD`,
  `t/*/catalog/*/snap/*` and `t/*/catalog/*/idx/*`. A
  new pinning test asserts both directions: HEAD stays denied, and a snap
  key and an idx key built from the same key constructors the sweep uses are
  deletable. This changes a shipped IAM template: an operator running
  `maintain.json` from before this change must re-apply it. Until they do,
  every sweep pass is refused at its `ListBucket`, exactly as before.
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

- **f64 range predicates in `ravel-logseg` no longer prune a block that
  carries a NaN value** (issue #1699). Under the `total_cmp` order the
  skip index's min/max stats and the SQL layer's numeric-range predicate
  both use for `f64`, a `+NaN` row sorts above every finite value and a
  `-NaN` row sorts below every finite value, so a half-open range arm
  like `[x, inf)` or `(-inf, x]` can be satisfied by a NaN row that a
  block's finite `[min, max]` stat says nothing about. `stat_disjoint`
  ignored the block's `has_nan` flag entirely and could prune such a
  block, dropping a matching row from a range-scanned query. It now
  declines to prune whenever an f64 stat has `has_nan` set, before
  applying the bounds test; this is deliberately coarser than necessary
  (it declines both arm directions, since the flag does not record which
  sign of NaN was present) but always sound.

- **The read cache's single-flight `get_or_fetch` path no longer blocks a
  runtime worker thread on disk I/O** (issue #1702). The tiered disk
  cache read and wrote files with `std::fs`, called directly from the
  async single-flight path, so every disk hit and every disk fill on that
  path occupied a tokio worker for the length of the file operation,
  starving unrelated queries and ingest work sharing the runtime. The
  calls now run under `spawn_blocking`; a join error is treated as a
  cache miss on read and a dropped fill on write, never a panic, and the
  synchronous entry points keep their signatures.

- **Disk-cache entries and directories, and SQL spill scratch directories,
  are now created owner-only instead of at the ambient umask** (issue
  #1708). Neither `ravel-cache`'s disk read cache nor `ravel-sql`'s spill
  scratch set a file mode, so on a node running at the common umask
  default of 0022, cache entries (raw, unencrypted segment bytes under a
  filename carrying the tenant hash) and their shard/namespace
  directories were world-readable and world-listable, and per-query spill
  directories were world-listable. Cache inserts now create shard
  directories at mode 0700 and entry files at mode 0600; SQL spill
  scratch directories are created at mode 0700. A node upgraded in place
  also had a namespace root and shard directories an older build already
  created at the ambient umask: on startup, the cache now narrows the
  namespaced root and each shard directory it walks to owner-only and
  each live entry it seeds to owner read-write, without touching anything
  above the namespace or an operator-created root. This does not reach a
  pre-namespace cache tree left over from before the per-instance
  namespace layout; `docs/guides/caching.md` points at the existing
  `reclaim-legacy --apply` command to remove one.

- **A stalled flush no longer stalls every co-resident tenant on the same
  shard** (issues #1292, #1641). A shard actor acquired the
  `--max-inflight-flushes` permit on the actor task itself, before spawning
  the flush. At the default bound of one permit, a flush stalled retrying a
  slow object-store PUT held the only permit, which parked the actor's
  whole event loop: the channel stopped draining, the age-flush tick
  stopped firing, and finished flushes stopped being reaped. Because object
  keys are tenant-prefixed and S3 throttles per key prefix, one tenant being
  throttled by the store stalled every other tenant sharing that shard,
  including their age-triggered flushes, with no data lost but no
  availability either. Fixed first for the metrics shard actor, then for
  logs and spans, by acquiring the permit inside the spawned flush task
  instead: the actor now returns the moment it spawns a flush, so a stalled
  flush parks only its own task. Backpressure at the permit bound now comes
  from the process-wide ingest byte budget instead of from the actor
  blocking, since a flush holds its byte charge from the moment it leaves
  the actor. `--max-inflight-flushes` is documented as the per-shard flush
  isolation control it is: raising it lets healthy tenants keep flushing
  while one key prefix is being throttled.

- **A shard actor that dies and is respawned no longer silently halves
  ingest capacity forever, and the process now reacts to it** (issue
  #1299). A metrics shard actor whose flush task panicked left its channel
  closed, so every later write routed to that shard failed for the rest of
  the process lifetime with no signal to the orchestrator. The router now
  respawns a dead shard actor with a fresh writer identity, up to a bound
  of 3 respawns, and once that budget is exhausted the next death condemns
  the shard: `/readyz` turns unhealthy and Kubernetes replaces the pod,
  which is the only recovery left for a shard that cannot come back in
  process. A respawn restores write capacity but not the dead actor's
  buffered rows, which were never acknowledged and so are lost within the
  documented at-least-once contract, and the write that observed the death
  still gets a retryable error so the client retries onto the fresh actor.
  Three follow-on corrections shipped with it: the respawn budget is now a
  decaying allowance rather than a process-lifetime count, so a shard that
  fails once an hour is no longer condemned on its fourth outage regardless
  of how far apart those outages are; the shard's monotonic clock floor
  used to detect backwards clock steps (per-shard, not per-writer, so it
  survives the writer identity changing) is now carried across a respawn
  instead of resetting to zero, so the guarantee it provides holds for the
  shard's whole process lifetime rather than resetting on every respawn;
  and a readiness-registration bug that held an extra reference to the
  ingest router was fixed, which had silently prevented shard actors from
  being joined during every graceful shutdown.

- **The log and span ingest pipelines now condemn a shard and shed the pod
  on its first dead actor, matching the metrics pipeline** (issue #1691).
  Unlike the metrics pipeline, the log and span routers never respawn a
  dead shard actor, so its first death is already permanent; before this
  change only the metrics pipeline reported that condemnation to
  `/readyz` and to a `shards_condemned` counter, so a permanently dead log
  or span shard kept serving errors with its pod still in the Kubernetes
  Service rotation. Both pipelines now expose the same readiness signal and
  counter the metrics pipeline already had, so Kubernetes sheds the pod as
  soon as either shard actor dies for good.

- **A flush queued behind a stalled tenant no longer has its abandonment
  deadline silently spent by the wait** (issue #1739). A flush's
  abandonment deadline was pinned when the flush opened, before it waited
  for the shard's flush-concurrency permit. At the default
  `--max-inflight-flushes` of 1, a tenant whose object-store writes were
  slow held the shard's only permit, so a co-resident tenant's flush queued
  behind it spent its whole deadline sitting in that queue; if the stall
  outlasted `--max-flush-lifetime`, every flush that had opened during it
  was abandoned before it ever attempted a store write, dropping rows that
  had already been acknowledged to the client in buffered mode, with no
  crash and no store error. The deadline is now re-derived from the moment
  the permit is actually granted, so a flush that queues behind a stall
  gets its full flush lifetime for its own store calls once it holds a
  permit; a flush already past its deadline before it even reaches the
  queue is abandoned there without taking a permit. The abandonment counter
  is also now split by cause: `ravel_ingest_abandoned_queue_deadline_total`
  counts a flush abandoned while queued for a permit, separately from
  abandonments after a permit was held and the store calls themselves
  failed.

- **The flush trigger no longer over- or under-charges the object it is
  about to write, and the per-buffer memory ceiling now scales with a
  smaller configured budget** (issue #1305). The size-based flush trigger
  compared a buffer's in-memory footprint, which counts a fixed struct
  overhead behind every label or attribute, against `target_bytes`, so a
  buffer of many small series could accumulate several times
  `target_bytes` of real payload before it was ever charged with reaching
  the target: a nominal 8 MiB target fired at a small fraction of that in
  actual object bytes. The trigger now estimates the object's own size (a
  fixed per-series and per-record term, one copy of label and attribute
  text per series, no struct overhead) so the written object lands at or
  under the configured target instead of drifting over it. Separately, the
  per-buffer memory backstop that exists to flush before one tenant's
  buffer can exhaust process memory was a flat 64 MiB constant justified as
  an eighth of the 512 MiB default `--max-ingest-buffer-bytes` ceiling. On
  a process started with a smaller ceiling, one label-heavy buffer could
  fill to the full 64 MiB and consume the entire configured budget by
  itself, shedding every other tenant's writes with HTTP 429 while the
  backstop itself never triggered. The backstop now scales down with a
  configured ceiling below the default, so it can no longer exceed the
  budget it exists to protect.

- **A backward host clock step across a graceful shutdown or rolling
  restart no longer destroys already-acknowledged buffered rows** (issue
  #1307). The commit record's creation timestamp, the primary key of
  query-time duplicate resolution, was range-checked but never
  order-checked against a shard's previous flush, so an NTP step backward
  could let a stale write outrank the correction that was meant to replace
  it. A per-writer monotonic floor was added to keep each shard's
  timestamps non-decreasing within one process lifetime, but the flush a
  large backward step could not absorb was refused by dropping its
  tenant's entire buffered rows outright, including rows already
  acknowledged to the client in buffered mode; if that refusal landed
  during a graceful shutdown or a channel close, those rows were destroyed
  with the actor exiting right behind it. The refusal is now retryable
  rather than terminal: a flush that crosses the absorption bound
  re-anchors the floor and re-inserts its buffer instead of dropping it, so
  the next trigger retries it against the corrected floor, and the
  shutdown drain itself now retries over fresh snapshots until every
  tenant's buffer empties or a bounded number of passes is exhausted. Only
  a clock that keeps regressing across every one of those passes can still
  leave residue at teardown; that narrow remaining case is what
  `ravel_ingest_flush_all_residue_tenants_total` reports.

- **An OTLP Remote Write request's decompressed size is now charged against
  the ingest byte budget** (issue #1419). The process-wide ingest byte
  budget is meant to bound the transient memory ingest inflates during
  decompression, but only the OTLP HTTP gzip path charged it: a Remote
  Write request could snappy-decompress up to 64 MiB with nothing charged
  against any ceiling, so as many concurrent requests as
  `--max-inflight-ingest-requests` allows could each inflate that far
  outside every configured budget. The handler now reads the exact
  decompressed size from the snappy block's own header and charges it
  before allocating, so the charge matches what is actually allocated
  rather than the request's compressed size or its wire-size cap; a
  request already over the wire-size cap still gets rejected before it
  takes a charge, and a request the budget cannot admit is shed with a
  retryable response before its output buffer exists. OTLP gRPC and the
  OTAP zstd payload remain outside this ceiling; the codec that inflates
  them cannot be intercepted before the fact, and the documentation now
  says so plainly instead of claiming coverage it does not have.

- **A `RavelCluster` upgrade with `spec.gateway.maxInflightFlushes` set
  above the queued-flush cap no longer crash-loops every gateway pod**
  (issue #1642). The server refused to start when `--max-inflight-flushes`
  exceeded `--max-queued-flushes` (default 8), but the operator CRD exposes
  `maxInflightFlushes` with no corresponding field for the queue cap, so an
  already-running cluster configured above 8 would fail every gateway pod
  on upgrade with no custom-resource edit able to recover it. Startup now
  raises the effective queue cap to match the configured permit count
  instead of refusing, logging a warning that names both values.

- **OTLP no longer allocates unbounded memory exploding a wide classic
  histogram, closing a memory-exhaustion vector** (issue #1681). One
  histogram data point with N explicit bucket boundaries normalizes into
  roughly N+3 points, each copying the point's full label set, and nothing
  bounded N: a single request under the existing wire-size limit, holding
  a handful of histograms whose bucket-boundary lists filled the body,
  could explode into on the order of a million and a half points and
  allocate accordingly before any per-tenant series limit had a chance to
  run. A data point with more than 160 explicit bucket boundaries (an
  order of magnitude above what real exporters emit) is now rejected before
  any per-bucket allocation happens, and the total point count a request
  would explode into is also compared against the per-request data-point
  limit before that allocation, closing the remaining case of many
  small-but-numerous histograms in one request.

- **The OTAP gRPC metrics surface gained the same histogram-explosion cap
  OTLP already has, closing the same memory-exhaustion vector on that
  surface** (issue #1753). OTAP exploded a classic histogram the same way
  OTLP does but carried neither of OTLP's two bounds: a single very wide
  histogram on the OTAP surface could exhaust process memory in a way the
  equivalent OTLP request would already have rejected. OTAP now applies the
  same per-point bucket-count cap and the same post-explosion
  per-request total check OTLP applies, verified by a differential test
  that both surfaces now reject the same over-cap and over-total requests
  identically. Three related exemplar-accounting gaps closed alongside it:
  a request rejected for exceeding the exploded-point total had its
  exemplars decoded before that check ran, so the rejection discarded
  them without ever counting them as dropped; the check now runs before
  exemplar decode, so that path has nothing left to discard uncounted. A
  second rejection, on raw wire bucket count, never decoded exemplars at
  all and so could not count them by the same means; it now derives the
  dropped count from the columnar payload's own row count instead of
  decoding. And a third payload type, exponential-histogram data points,
  was missing from the dropped-exemplar count entirely on the path that
  rejects them as unsupported. All three OTAP rejection paths now report
  dropped exemplars with the same fidelity OTLP does.

- **The ingest router no longer forwards a client-supplied identity header
  to the upstream store when the mTLS header has been renamed away from
  its default** (issue #1704). The router trusted a client-supplied
  identity header for tenant resolution with no certificate verification
  of its own, so any client could set that header and pick its own tenant.
  The router now refuses `--mtls-enabled` outright rather than installing
  an unverified mTLS resolver into its shared chain (a dedicated mTLS
  listener able to isolate the resolver safely does not exist yet;
  `--tenant-token` and `--oidc-issuer`/`--oidc-jwks-url` are the supported
  alternatives), and it strips the client-supplied identity header from
  every forwarded request on both the HTTP and gRPC paths before dialing
  the upstream. That strip initially covered only the header's default
  name: a deployment that renamed it via `--mtls-header` still forwarded
  the client-supplied value upstream untouched, reopening the same
  spoofing gap on exactly the deployments that had customized the header.
  The configured header name, not just the default one, is now resolved
  once and stripped under every key source. An operator who set a custom
  `--mtls-header` should upgrade to pick up the fix; routing selection by
  the mTLS-subject key source is unaffected, since a client can already
  choose its own shard by that mechanism today and that is unchanged.

- **Admission reconciliation's read cost no longer grows without bound as
  processes come and go** (issue #1679). A per-`(tenant, signal)` admission
  cycle lists a snapshot prefix and reads every key in it to enforce a
  fleet-wide series cap; the prefix gained one key per process that had
  ever run and nothing ever removed one, so the read cost grew with every
  process that had ever existed. Past a few thousand stale keys a
  reconciliation cycle took longer than its own staleness window, every
  sibling snapshot read as stale, and each process silently fell back to
  enforcing the whole fleet cap on its own, with every listing and read
  still succeeding and no counter to show it happening. A stale key is now
  skipped without a read once its last-modified time is already past the
  staleness window, and a key past a wider horizon is deleted from the same
  listing, bounding both the reads and the listing itself. The same fix
  shape was applied to the maintenance worker heartbeat registry, which
  had the identical unbounded-growth defect one prefix over.

- **A `SELECT labels` result that a stock 4 MiB Arrow Flight client could
  not read past about a dozen distinct series now fits well past that**
  (issue #1519). `RsegDedupExec` keeps each deduplicated winner row as a
  one-row slice of its source batch; slicing a `DictionaryArray` rewrites
  only the key run and retains the whole source dictionary values buffer, so
  concatenating ~1024 such one-row slices per flush appended every slice's
  full dictionary, and the labels dictionary grew with the row count rather
  than the distinct-series count. The flushed batch's labels column is now
  rebuilt so its dictionary holds only the distinct label sets its surviving
  rows actually reference, with rows re-keyed to the compacted entries: one
  entry per distinct series in the batch regardless of how many rows
  reference it. A test over the real scan-to-dedup pipeline pins the
  flushed dictionary's length to the distinct label-set count rather than
  the row count, and asserts the largest single Arrow IPC body (25 series
  over 60,000 rows) fits inside the 4 MiB Flight default.

- **A transient labels-dictionary blowup during dedup flush, and repeated
  per-row rebuild work, are both closed** (issue #1582), following up on
  #1519's per-flush dictionary compaction. `concat_batches` still
  transiently materialized the full blown-up dictionary before that
  per-flush compaction ran, measured at 350,192,304 bytes peak on a
  many-series corpus; each one-row slice is now compacted in `finalize()`
  before it is pushed to the pending output, so `concat_batches` only ever
  sees at-most-one-entry dictionaries and the measured peak drops to
  853,232 bytes (410x). `DedupStream::finalize` also memoizes its per-row
  labels-dictionary rebuild by the source dictionary's values pointer plus
  key, so consecutive winner rows from the same upstream batch and the same
  dictionary key reuse the already-built array instead of rebuilding a
  bit-identical one; the memo compares the values array by pointer as well
  as by key; because a new upstream batch renumbers its own dictionary from
  zero, a key-only memo would have relabeled one series' rows with
  another's.

- **A per-tenant bytes-scanned or S3-request budget check on the three
  shipping SQL execution paths (`plan_pinned`, `plan_pinned_distributed`,
  `worker_fragment_stream`) no longer quadruple-counts the same bytes and
  refuses queries after a quarter of their real budget** (issue #1665).
  `RsegScanExec`'s budget checks read a reduction that sums four
  `PhaseAccounting` phase snapshots, which is only correct when the four
  phases are independent handles; those three paths instead build the
  handle with `pooled_over`, whose four phases are clones of one shared
  counter, so the same reduction read that one counter four times. A shared
  flag set once at construction now lets a new `pooled_snapshot()` method
  pick the correct reduction (the resolve phase's own snapshot when
  aliased, the existing summed reduction otherwise), so every caller reading
  an aliased handle's total is correct by construction rather than by
  remembering which constructor built it.

- **Three ways to bypass the SQL complexity guard that aborts the process on an
  over-bound statement are closed** (issue #1678), hardening the guard that
  issue #1760 (below) later made impossible to skip entirely by construction. A
  `/*! ... */` MySQL-style hint comment was scanned as an ordinary comment and
  skipped, so five characters of wrapping hid an arbitrarily long operator
  chain: `SELECT 1/*!` followed by `+1` 2,000 times and `*/` scored 7 while the
  tokenizer actually produced 4,003 tokens from it. The scan now gives the hint
  region its own mode that counts every non-whitespace character inside it and
  enters no sub-mode of its own (an earlier fix that made it fall through to
  ordinary counting mode reopened the same bypass through a line comment inside
  the hint body). Separately, the switch from counting characters to counting
  tokens undercounted a digit-then-word sequence like `1AND` as one alphanumeric
  run costing one unit for two tokens, which halved the guard's effective bound
  on a boolean chain: `SELECT 1` followed by `AND 1` 998 times scored exactly
  1,000 units and built a 998-level parse tree, next to the roughly 1,050-1,080
  levels at which the planner aborts on a 2 MiB thread. A digit run now stops at
  the first non-digit; a run starting with a letter or `_` still consumes
  alphanumerics, since `a1` is one identifier. Finally, the audit redaction path
  (`redact`, used when `--audit-text` is left at its default of `redacted`)
  parsed and walked caller text with no complexity guard at all, so a 64 KiB
  statement that `validate` had already rejected as too complex still reached
  `redact` and aborted the process there; `redact` now runs the same guard
  `validate` does before it parses.

- **`DistributedScanExec` no longer fails an entire statement for the whole
  `3 * H` staleness window because one assigned worker is dead but still
  registered** (issue #1684). Each scan slice now runs the same three-step
  sequence the PromQL lane's routing fetcher already uses: the assigned
  worker, exactly one re-dispatch to a different location, then a
  coordinator-local read of the same slice ticket (`worker_fragment` over
  the ticket's pinned segments against the same object store, which is
  byte-identical to the remote result it replaces) before finally returning
  a typed `SqlError::Execution` naming the last cause. Each attempt is
  probed for its first batch before the partition emits anything, so a
  fallback can never feed the coordinator's merge a second run of the same
  rows, and `SliceFallbackCounters` counts a re-dispatch and a
  coordinator-local read as separate figures. The coordinator's own record
  is also dropped from the SQL worker roster, since it always serves its
  own slices through the local path and dispatching one to itself over
  Flight was a wasted hop.

- **A federated coordinator that can resolve more than one local tenant can
  no longer leak one tenant's remote series to another** (issue #1295).
  Federation held one remote credential per process with no way to say which
  local tenant it belonged to, so on a coordinator serving more than one
  local tenant (two or more `--tenant-token` values, or any dynamic resolver
  such as `--dev-insecure-tenant-header`, `--oidc-issuer`, or
  `--mtls-enabled`), every local tenant's metric selectors and discovery
  calls fanned out to the remotes under that single shared credential: each
  local tenant received the remote tenant's series, and the remote tenant's
  data reached whichever local tenant happened to ask. `--remote-cluster`
  now takes a tenant key naming the one local tenant whose queries may use
  that remote's credential, and `Federation::fetch` selects remotes by the
  caller's tenant before dispatch, so an unmapped local tenant presents no
  credential and is answered from local data alone; an unkeyed
  `--remote-cluster` spec is refused at startup on any coordinator that can
  resolve more than one local tenant, naming the offending clusters and the
  remedy. A follow-up closed the same leak on the alerting path: the
  multi-tenant check originally counted only distinct `--tenant-token`
  values, missing that `--alert-rules-file` starts one evaluator per tenant
  against the same shared engine with no incoming request to carry a tenant
  key, so a single-token deployment with a second tenant's alert rules read
  as single-tenant and let that tenant's rules evaluate against the remote
  tenant's series. Both checks now read the union of the token-derived
  tenants and the alert-rules file's tenant keys.

- **Every parse of caller text in `ravel-sql` now runs the pre-parse
  complexity guard, because one function does both** (issue #1760),
  matching what issue #1817 already did on the PromQL side. Three functions
  in the crate built their own parser over caller text: `validate`, the
  audit redactor (`redact`), and the page planner (`page_plan`'s
  `parse_query`). Only two of the three ran the guard by convention: the
  redactor gained its call in a review round (issue #1678, above), and the
  page planner had never had one, resting on the unenforced assumption that
  `validate` had already accepted the same text first. `parse_guarded` is
  now the crate's only parse of caller text: it runs the complexity check
  and then builds the parser, so `validate`, `redact` and `page_plan` all go
  through it and a fourth entry point cannot reach the parser without the
  guard in front of it. A new gate script,
  `scripts/guards/check-guarded-sql-parse.sh`, refuses any mention of a SQL
  parser front end under `crates/ravel-sql/src/` outside that one function.

- **Nine PromQL evaluator code paths that used to abort the whole process on
  an ordinary query shape now return a typed error instead** (issue #1701).
  A parsed tenant query could reach an `unreachable!()` arm for: an unknown
  aggregator token, an aggregate whose inner expression evaluates to a
  non-vector, a missing or wrongly-typed `limitk`/`count_values` parameter, a
  binary operator whose operands are neither both scalar nor both vector, a
  `ManyToMany` vector match on a non-set operator, and a matrix-typed
  function argument reaching a non-matrix AST node - each on the mistaken
  assumption that promql-parser's own type checking had already ruled the
  shape out. Each arm now returns `Error::Unsupported` naming the operator
  or type, with a test built from a synthetic AST the parser cannot
  currently produce, demonstrating the arm panics if reverted. A follow-up
  found one more gap in the same area: `eval_binary` dispatches "if
  `is_comparison(op)` then `apply_cmp` else `apply_arith`", so a
  `Scalar/Scalar` or `Scalar/Vector` expression using `and`, `or`, or
  `unless` reached `apply_arith`'s fallback and aborted, because nothing in
  Ravel (only promql-parser's own `check_ast`, which sits on a caret version
  range) narrowed those shapes out. `eval_binary` now checks the operator's
  class before dispatching on operand types, rejecting a set operator over
  scalar operands with a typed error before the scalar-handling functions
  run; `Vector/Vector` set operators keep their existing path unchanged. A
  new guard script, `scripts/guards/check-promql-unreachable.sh`, requires
  every remaining `unreachable!()` under `crates/ravel-promql/src` to name
  the check that narrows it out of reach.

- **A log segment scan with one overflowing `attrs_raw` block no longer drops
  the rest of its partition's block list onto the slower row-decode path**
  (issue #1769). Blocks past a block whose attributes overflow the per-object
  dynamic-column budget used to stay on the row path for the remainder of the
  scan, even blocks with no overflow at all, because the scan only knew how to
  reopen the segment once and commit to row mode from there. `LogSegmentScan`
  now falls back for the offending block only and resumes columnar decoding
  after it, since its columnar and row cursor-advance paths already share one
  primitive. The narrowing is bounded rather than unconditional: a tenant with
  more than about a hundred distinct declared attribute names has overflow in
  most blocks of an object, and reopening once per block would cost quadratic
  redecode work on exactly the tenants already slowest on this path, so after
  two consecutive fallbacks with no clean block in between, the scan commits the
  rest of the partition's list to the row path in one reopen, capping any one
  segment at two reopens regardless of its block count.

- **A log lane query's reported `segments_pruned` and `segments_fetched`
  figures are now derived from the actual set of segments each fetch
  touched, instead of being summed or maxed across a query's plans** (issue
  #1228). The log lane's `stats.segments_pruned` first silently
  under-reported because `prefetch` discarded the count `fetch_log_series`
  already computed per plan and substituted the catalog resolve's own
  figure, which is structurally always `0` for this lane (the resolve
  passes no name filter to prune against). Summing each plan's own pruned
  count fixed that but introduced a double-count: every plan in a log lane
  re-walks the same resolved segment list under the same padded window, so
  a segment one plan pruned could be exactly the segment another plan
  fetched, and a two-plan query where each plan pruned the other's segment
  reported `pruned=2, fetched=1` over a 2-segment snapshot that had pruned
  nothing. `fetch_log_series` now reports which segments it fetched as
  indexes into the shared segment slice; the log lane unions these indexes
  across its plans and derives `segments_fetched` as the union's size and
  `segments_pruned` as the remainder, so the two figures sum to the
  resolved segment count by construction for any plan count, saturating at
  zero to stay fail-closed if a future caller passes a subslice.

- **A wide tenant's per-segment column statistics could silently disable
  pruning for every query, and are now split into one bounded object per
  snapshot part instead of one growing-without-bound object per tenant**
  (issues #1413, #1483, ADR-1413). The prior `.cstat` object held every
  `ColumnStatsSegment` record for a whole (tenant, signal) as one compressed
  frame, decoded whole to serve any part of it, and refused to inflate
  anything over a 256 MiB safety ceiling. On a measured 104-column,
  703-segment tenant the object decoded to a body of 2,000,102,795 bytes,
  7.5x that ceiling: the decode was refused, the refusal was silently
  degraded to "no column statistics for this tenant", and every query on it
  fell back to a full scan (7,645 GETs for a single-column `COUNT(*)` where
  statistics would have pruned). The fold now emits one per-part `.cstat`
  object alongside each part, referenced from the part's own
  `SnapshotPartRef`, so decoding one part's statistics costs only that
  part's bytes; an over-ceiling part degrades (drops to an unpruned scan for
  that part only) instead of refusing the whole tenant's statistics. Issue
  #1483 closed a gap in the migration window between the old and new
  format: the reader treated any successful v2 (whole-object) fetch as
  answering every segment and stopped consulting v1, but a published v2
  object can legitimately omit a segment its fold couldn't build
  statistics for, so a part v2 omitted with no v3 object yet got no
  statistics at all and scanned silently. The reader now tracks the parts
  still needing a fallback explicitly and only clears that list once the
  entries v2 actually decoded meet or exceed the entry count the snapshot
  HEAD declares.

- **Age-based retention now counts selective-erasure rewrite records, not
  only L0 commit records and compaction records** (issues #1313, #1321). A
  bucket's newest-event computation and its physical delete sweep both read
  only two of the three record kinds a bucket can hold, so an
  ADR-0064 rewrite record was invisible to both. Once the rewrite's own
  superseded inputs were swept, the rewrite record became the only live
  record the bucket held (its durable steady state, since compaction and
  migration both decline a bucket carrying one), so expiry evaluation saw no
  records at all and treated the bucket as never expired: the retention
  window stopped applying to it permanently. If such a bucket was tombstoned
  before its inputs were swept, the physical sweep deleted every other
  record but left the rewrite record behind, so the verifying listing found
  residue on every pass and the sweep outcome stayed `SweptPartial` forever,
  with the tombstone never deleted. Expiry evaluation now decodes and
  verifies every rewrite record it lists and folds its timestamp into the
  same maximum as the other two kinds (a rewrite record with no surviving
  parts, which the schema permits, contributes its own publish time rather
  than pinning the bucket forever); the physical sweep deletes rewrite
  records in the same pass, between compaction records and L0 data objects,
  so the existing delete ordering and the tombstone-deleted-last invariant
  are unchanged. The tombstone's recorded object count now includes rewrite
  records too, so a rewrite-only bucket's audit evidence no longer reads as
  a bucket that was never written to.

- **A production panic in selective-erasure rewrite on an empty, never-compacted
  L0 bucket is fixed** (issue #1410). A windowless erasure request (one covering
  a whole series, with no time-range restriction) passed the rewrite's overlap
  prefilter for every bucket with any live record, because that filter
  short-circuits to true for a windowless request before it can apply its usual
  empty-range check. Against a bucket with zero L0 commits and nothing ever
  compacted, that left the rewrite build with an empty input set and no
  superseded record to point to, which is a caller-contract violation the code
  enforces with a panic. Any caller that fed the derived set of not-yet-sealed
  hours into rewrite would panic on almost every request, since that set is
  empty for most shards once sealed, making this a live production path rather
  than an edge case. The rewrite now checks for this one shape before it builds
  anything, and reports it the same way it already reports a bucket with no
  overlapping request at all: nothing to do, nothing written. No data was at
  risk; the defect was an availability one, a panic instead of a no-op.

- **The listing conformance suite now certifies both entry points a listing
  call can use, and bounds every page-drain against a backend that never
  terminates a listing** (issue #1448). The suite's key-ordering probe
  drained its full pass through `list_after` only, and `list` and
  `list_after` are separately implemented on a real backend (a native S3
  client overrides each), so a backend whose `list` delivered keys out of
  order, or re-delivered an earlier key across a page boundary, while its
  `list_after` stayed correct, could pass qualification and then fail every
  production catalog scan, which drains through `list`. The suite now runs
  the ordering and distinct-set checks against a full pass through each
  entry point, and every listing failure the suite reports now names which
  one failed. Separately, the page-drain loop looped until a page carried no
  continuation token, so a backend that kept returning the same token spun
  forever, silently, because the existing de-duplication hid the repeat
  without ever stopping it; every caller that drains a prefix, including
  every catalog scan, inherited that risk. A drain now recognizes a repeated
  continuation token as a spinning backend and returns a typed error rather
  than looping, and is capped at 100,000 pages (100 million keys at the
  contract's 1000-key page size) against a backend that keeps returning new
  tokens without ever finishing. `docs/object-store-contract.md` documents
  both entry points as judged on their raw delivery sequence and states the
  new page and repeat bounds.

- **The listing conformance suite's delete-visibility check now actually
  exercises the `list_after` entry point it claims to test** (issue #1498).
  The probe previously drained only `list` and asserted a deleted key was
  absent there, so a backend whose `list_after` kept showing a deleted key
  as present could still pass qualification: `list` and `list_after` are
  separately implemented methods, and a delete visible through one but not
  the other is a real defect the probe must catch on its own rather than
  depend on which call a caller happens to use. The probe now drains both
  and asserts the deleted key is absent from each; a new fixture whose
  delete genuinely applies (so `get` and `list` see it gone) but whose
  `list_after` still re-injects the deleted key proves the new half of the
  check actually fails when it should, since the assertion could otherwise
  read correct while no fixture in the suite ever reached it. Every
  listing-drain failure the suite reports (a listing error, or a pager that
  never terminates) now also names the entry point it happened on, matching
  the ordering and distinct-set failures, which already did.

- **Selective-erasure rewrite carries exemplars through its output, filtered
  per record so an erased instant's exemplar is dropped exactly like its
  sample** (issue #1512). A rewrite previously wrote every output segment
  with no exemplars at all, silently dropping every exemplar in a rewritten
  bucket, including exemplars belonging to series an erasure request never
  touched; this went unnoticed because exemplars are not counted samples, so
  the rewrite's own sample-count conservation check stayed clean regardless.
  Exemplars are now carried into the output and matched one at a time
  against the same per-record predicate the sample rows use: an exemplar
  survives only if its own series has at least one surviving sample in the
  output and no pending erasure request's window covers the exemplar's own
  timestamp. This distinction matters because once a request's completion
  record is written, its erasure request record is removed and no later
  query-time filter applies, so this rewrite pass is the only place a
  windowed request's exemplars are ever checked against the erasure window
  they fall in; a series-level check alone (keep every exemplar on a series
  with any surviving sample) would carry forward the value, trace ID, span
  ID and attributes of an exemplar sitting inside an otherwise-erased
  window. A series whose labels cannot be resolved during the rewrite now
  drops the exemplar rather than keeps it, since an erasure path must favor
  deletion over retention when it cannot verify a candidate. The rewrite
  report now counts `exemplars_kept` and `exemplars_dropped` so this is
  visible at the point of the rewrite rather than only inferable later.

- **The maintenance loop (retention, compaction, and garbage collection) now
  survives a panic and reports its own liveness, instead of silently dying
  and leaving every maintenance metric frozen** (issue #1683). The loop ran
  as a single spawned task with no restart and no completion signal of its
  own; every maintenance gauge on `/metrics` is written only at the end of a
  completed cycle, so a panic anywhere in the loop's discovery or sweep call
  graph left the process Running and Ready while `tenants_maintained`,
  `units_stalled`, and every safety gauge froze at their last healthy
  values. Retention stopped deleting expired data, compaction stopped
  folding, and the sweeper stopped reclaiming space, with nothing on
  `/metrics` moving to say so until a query failed on stale or missing data,
  potentially days later. The loop is now wrapped so a panicking cycle is
  caught rather than taking the process down, counted on the new
  `ravel_maintain_loop_panics_total`, and restarted after a backoff (1
  second, doubling to a 60-second ceiling, reset whenever an attempt
  completes at least one cycle, whether or not it later panics); a new gauge,
  `ravel_maintain_last_cycle_completed_timestamp_seconds`, is stamped at the
  end of every completed cycle, so its age, not its value, is the operator
  signal that the loop itself has stopped making progress. `docs/guides/
  observability.md` documents a `RavelMaintenanceLoopStalled` alert on the
  gauge's age (staleness over 1800 seconds, six default 5-minute maintain
  intervals) and a `RavelMaintenanceLoopCrashLooping` alert on a sustained
  rate of panics (more than 3 in an hour, held 15 minutes), because a loop
  that completes a cycle
  between every panic re-stamps the liveness gauge and resets its own
  backoff, so the gauge alone would stay quiet through that crash-loop
  shape. A related no-full-sweep alert is gated on the process actually
  owning at least one unit, so an empty cluster or a replica holding no
  units under the current ownership split does not page for correctly
  completing zero sweeps.

- **The listing conformance suite's pagination probes now force a real
  continuation-token boundary instead of passing on a fixed, small key count**
  (issue #1695). `S3Store`'s declared page size does not change the wire-level
  page size a real S3-compatible backend uses: each listing call opens its own
  lazy stream, pulls at most the declared number of entries off it, and drops
  the stream, so the backend's own continuation token goes unfollowed only when
  the declared size is smaller than what one real response actually carries. The
  suite's probes wrote a fixed handful of keys and accepted "more than one page"
  as proof of correct pagination, which an in-memory backend could satisfy with
  a trailing empty page emitted purely to mark the end of a listing, proving
  nothing about a real backend's pagination at all. Both probes now write enough
  keys, relative to the declared page size, to force at least two pages that
  actually carry objects, and fail qualification naming the real page count
  otherwise. `ravel-cli store qualify` gains a `--list-page-size` flag (default:
  the production S3 page size) so a qualification run exercises a real boundary
  against the store it is qualifying; a qualification run against the default
  page size now leaves about 2,018 scratch objects behind rather than a handful,
  which `docs/object-store-contract.md` now states so an operator's cleanup
  sweep sizes for the right number. The conformance suite version is unchanged:
  this changes how existing probes size their input, not which properties are
  checked, so no previously qualified store needs re-qualification.

- **The quarantine reaper is now held for the full duration of a live
  mass-orphan incident, and a legal hold now reaches a quarantined copy**
  (issue #1748). The reaper previously ran on every pass regardless of
  whether the mass-orphan breaker had tripped, so a record loss that grows
  over days (quarantining a few objects early, then widening until the
  breaker trips on every pass by day 7) still had its earliest quarantined
  copies physically deleted at the first quarantine horizon, which is
  exactly the permanence the quarantine mechanism exists to prevent, just
  arriving one horizon later. The reaper is now skipped for the whole time
  the breaker is tripped; a `force_orphan_gc` override is not a trip and
  still reclaims for an operator who has made that call. Separately, a
  legal hold on a tenant's data could not protect an object once it was
  quarantined, because hold scopes and the hold check are both plain
  prefix matches under `t/<tenant>/`, and a `quarantine/...` key never
  matches that prefix; the reap now also checks the hold against the
  recovered original key. `quarantine/` is a root-level key prefix alongside
  `t/` and `sys/` that was named in no key-layout document, leaving an
  operator writing a lifecycle rule or an IAM prefix policy with no
  documented reason to include it; `docs/deletion-and-gc.md`'s incident
  runbook also gained a restore-from-quarantine step, since the previous
  procedure (reconstruct from the live prefix) reads nothing once an object
  has been quarantined past the first horizon. Separately in this same
  review round, `docs/query-engine.md`'s worked example of the derived
  per-query S3 request budget is corrected from 48,200 to 15,800: the
  48,200 figure used
  a 500ms flush-cadence reference pair that a stock server, which ships a
  2-second flush cadence, does not actually run at.

- **A query coordinator's per-worker heartbeat key is now reaped, bounding a
  cost that previously grew for the life of the deployment** (issue #1761).
  A query-worker heartbeat key under `sys/query/workers/` was deleted only
  on a graceful drain, so a worker lost to a panic, a kill, an
  out-of-memory event, or a node loss left its key behind forever; the
  coordinator's liveness check lists the whole prefix and pays one read per
  key found, so the cost of a call made on every distributed query grew
  with the total count of query workers that had ever run, not the count
  currently alive. Liveness itself stayed correct throughout, since a stale
  heartbeat is read as dead, so nothing failed loudly while the cost grew.
  The liveness check now skips the extra read for a key the listing already
  shows as older than the liveness window, and a key past the reap horizon
  (twice that window, a clock-skew margin between the object store's clock
  and the reader's, not a second independent duration) is deleted outright,
  reusing the same shared predicates already shipped for the maintain and
  admission worker sets. On a deployment running the shipped query IAM
  template, this delete is currently denied
  (the template grants no `s3:DeleteObject` at all), so the reap logs a
  warning and the prefix does not shrink even though the extra-read savings
  still apply; granting `s3:DeleteObject` on `sys/query/workers/*` to the
  query role is required for the reap itself to take effect.

- **A graceful shutdown no longer drops acknowledged, buffered rows, and an
  overrun of `--shutdown-timeout` now fails the process instead of exiting
  clean** (issue #1291). Two ordering defects previously lost buffered records
  even though a drain ran: the router flush depended on an `Arc` unwrap that a
  still-live sweep task's clone made fail silently, and the drain awaited
  every open listener, under the same overall budget, before flushing at all,
  so a slow client holding a connection open could exhaust the whole shutdown
  budget before any flush ran. The flush now runs unconditionally before the
  listener join, the listener join runs inside its own sub-budget (four fifths
  of `--shutdown-timeout`), and an overrun of `--shutdown-timeout` now returns
  an error and a non-zero exit rather than logging "shutdown complete" after
  silently dropping the tail of the drain. This is a fix to the drain ordering
  itself, distinct from the residual-tenant and overrun metrics already
  covered under issue #1742. To keep the grace period ahead of the new drain's
  worst case, the operator now sizes `terminationGracePeriodSeconds` on every
  rendered `ravel-server` pod at 45 seconds (a 10 second preStop sleep plus
  the server's pinned 32.5 second SIGTERM-to-exit worst case at shipped
  defaults, plus headroom) and adds the preStop sleep itself, rather than
  leaving Kubernetes' 30 second default in place.

- **Flight SQL clients dialed the wrong listener** (issue #1296). The SQL
  distributed lane now dials a query worker's dedicated Flight SQL endpoint (a
  new `flight_sql_endpoint` field on the worker record) instead of the TLS-only
  fragment listener, which never spoke the Flight SQL protocol.

- **MCP cursors now pin the inputs a page was resolved from, not an
  enumeration of the segments that resolution produced** (issues #1501,
  #1529). Enumerating segments could push a token past its own bound and past
  the 256 KiB response floor on a large range; the cursor now carries the
  signal, the half-open event-time range, the minimum commit-token watermark,
  the pending erasure predicates in force, the typed attribute column set, and
  a keyset position, and is re-resolved deterministically on redemption. The
  cursor now has its own 4 KiB bound, tracked separately from the shared
  scalar allowance it previously competed with the plan and failure message
  for; a cursor that would exceed its own bound is now an internal-error
  condition rather than a silent drop with a warning, since only a server
  defect can produce one. Redemption now also expires a cursor whose pinned
  data a compaction has since taken apart, or whose signal and event-time
  range now intersect an erasure predicate that came into force after the
  cursor was minted (`cursor_expired`), keeping this distinct from
  `cursor_invalid` for a tampered or wrong-tenant token. An envelope that
  already carried a failure (`budget_exceeded`, `unavailable`) no longer has
  that failure overwritten by the generic message an over-bound cursor
  produces; the first, more specific failure is now preserved.
  `CURSOR_VERSION` is 4; a cursor minted under an earlier version is refused.

- **An MCP tool result can now carry a large unsigned 64-bit integer as an
  exact integer cell** (issue #1525). The wire cell type was a 64-bit signed
  integer, so a value above `i64::MAX` fell back to a string cell, giving an
  unsigned column a different cell type from a signed one. The cell now
  carries the union of the signed and unsigned 64-bit ranges, and only a
  genuine float becomes a float cell.

- **The MCP protection horizon is minted as a future instant** (issue #1560).
  It was computed in the past, which made every cursor redemption's deadline
  re-clamp against it fail immediately.

- **OTLP metric writes that silently dropped informational data (histogram
  min/max, exemplars, integer precision) now report that drop in the
  partial-success response** (issue #1585). The partial-success gate previously
  keyed off the rejected point count, and an informational drop rejects nothing,
  so it took the `None` arm and discarded the `error_message` naming what was
  dropped; a sender lost min/max on every write and saw a response identical to
  a clean one. The gate now keys off whether anything was rejected at all, so a
  `rejected_data_points` count of 0 can still carry a populated `error_message`,
  which is what the OTLP proto reserves that field for.

- **The quickstart deploy's MinIO images and OpenTelemetry Collector image move
  off Docker Hub** (issue #1645). Docker Hub's anonymous pull allowance is
  scoped by source IP and shared with every other project on a runner, so an
  exhausted allowance failed the quickstart job with a message that pointed at
  credentials rather than at the real limit. Eight MinIO image references (both
  images in `docker-compose/minio.yml`, `docker-compose/ravel.yml`,
  `k8s/minio.yaml`, and `metricsbench/docker-compose.yml`) and the two `mc`
  invocations in the chaos and demo scripts move to quay.io, preserving the
  existing digest pins, which quay.io serves under the identical digest. The
  Collector moves to the `ghcr.io` path the upstream project publishes it under.
  Grafana's image stays on Docker Hub: no anonymous mirror was found on
  `ghcr.io`, `quay.io`, or `public.ecr.aws`, so the quickstart job still makes
  one anonymous Docker Hub pull, down from three.

- **Every image the quickstart deploy compose files and Kubernetes manifests
  pull is now pinned to a release tag plus an immutable digest** (issue #1720).
  Across the two quickstart compose files, 8 image lines are scanned and 6
  require a digest (Ravel's own released image is exempt by exact match); of the
  6 images referenced by the Kubernetes manifests, 4 are now pinned the same
  way, and Ravel's own locally built `ravel-server:latest` and
  `ravel-operator:latest` are exempt by exact match. A repo-wide check now scans
  both compose files and the manifests so the two cannot drift apart unnoticed.

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
