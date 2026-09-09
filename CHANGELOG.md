# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

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

### Fixed

- **A `RavelCluster` held at the store-qualification gate keeps its
  `StoreQualified` condition when the reconcile then fails** (issue #36). The
  degraded status write replaces the whole `conditions` array and previously
  reconstructed only the `StoreQualified=True` case, so a pass held at the gate
  that then failed a write left a `Degraded` object carrying no `StoreQualified`
  condition and nothing to say the store had not qualified. The gate now carries
  out the exact condition it built, `True` or `False` with its Pending or Failed
  reason, and records it before it creates or deletes the qualify Job, so a
  failure in that API call cannot drop it either.

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
