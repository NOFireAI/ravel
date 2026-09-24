### Added

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

### Fixed

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
