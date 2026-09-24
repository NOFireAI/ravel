### Added

- **`ravel_maintain_l0_records_pending` and `ravel_maintain_objects_deleted_total`
  render on `/metrics`** (issue #1729). The compaction scan and the retention
  sweep previously reported these figures to tracing only, so an operator
  could not see how much L0 compaction work was queued, or how many objects
  maintenance had actually deleted, without reading logs on every process.
  `ravel_maintain_l0_records_pending` is now published by `signal`, summed
  across every bucket this process owns and republished once per
  maintenance cycle (default 300 seconds) after the cycle has covered all
  of them, so a mid-cycle scrape reads the previous cycle's complete total
  rather than a partial sum; a bucket the cadence memo
  skipped for being safely below threshold still contributes its last-known
  record count, so the buckets an operator most needs to watch cannot silently
  drop out of the total. `ravel_maintain_objects_deleted_total` is published
  by `kind`, one series per `SweepReport` count (including
  `kind="quarantine_reaped"`). The troubleshooting guide documents both, and
  says that a dip in the pending gauge is not corroborated by
  `ravel_maintain_units_stalled`: that gauge only moves for a per-unit failure
  repeated past its stall threshold, and the paths that remove the most
  records from the pending total (a tenant skipped whole-tick for a failed
  legal-hold refresh, or by the provisioning or shard-generation check) never
  reach per-unit accounting, so `units_stalled` can sit at zero while the
  pending population moves for an unrelated reason.

### Changed

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

### Fixed

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

- **A production panic in selective-erasure rewrite on an empty, never-
  compacted L0 bucket is fixed** (issue #1410). A windowless erasure request
  (one covering a whole series, with no time-range restriction) passed the
  rewrite's overlap prefilter for every bucket with any live record, because
  that filter short-circuits to true for a windowless request before it can
  apply its usual empty-range check. Against a bucket with zero L0 commits
  and nothing ever compacted, that left the rewrite build with an empty
  input set and no superseded record to point to, which is a caller-contract
  violation the code enforces with a panic. Any caller that fed the derived
  set of not-yet-sealed hours into rewrite would panic on almost every
  request, since that set is empty for most shards once sealed, making this
  a live production path rather than an edge case. The rewrite now checks
  for this one shape before it builds anything, and reports it the same way
  it already reports a bucket with no overlapping request at all: nothing to
  do, nothing written. No data was at risk; the defect was an availability
  one, a panic instead of a no-op.

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
  continuation-token boundary instead of passing on a fixed, small key
  count** (issue #1695). `S3Store`'s declared page size does not change the
  wire-level page size a real S3-compatible backend uses: each listing call
  opens its own lazy stream, pulls at most the declared number of entries
  off it, and drops the stream, so the backend's own continuation token
  goes unfollowed only when the declared size is smaller than what one real
  response actually carries. The suite's probes wrote a fixed handful of keys and
  accepted "more than one page" as proof of correct pagination, which an
  in-memory backend could satisfy with a trailing empty page emitted purely
  to mark the end of a listing, proving nothing about a real backend's
  pagination at all. Both probes now write enough keys, relative to the
  declared page size, to force at least two pages that actually carry
  objects, and fail qualification naming the real page count otherwise.
  `ravel-cli store qualify` gains a `--list-page-size` flag (default: the
  production S3 page size) so a qualification run exercises a real boundary
  against the store it is qualifying; a qualification run against the
  default page size now leaves about 2,018 scratch objects behind rather
  than a handful, which `docs/guides/kubernetes.md` and
  `docs/object-store-contract.md` now state so an operator's cleanup sweep
  sizes for the right number. The conformance suite version is unchanged:
  this changes how existing probes size their input, not which properties
  are checked, so no previously qualified store needs re-qualification.

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
