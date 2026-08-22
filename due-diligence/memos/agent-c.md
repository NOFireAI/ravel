# Agent C: Catalog, MVCC, compaction, retention and GC

Reviewed at commit 527a16db2e4d47b2924e4de4a4db32d7583fda33. Scope:
crates/ravel-catalog, crates/ravel-commit, crates/ravel-maintain,
docs/catalog-and-mvcc.md, docs/consistency-model.md, ADRs 0003, 0018, 0019,
0020 (via docs), 0032, 0048, 0063, 0064, 0065, 0092. All paths below are
relative to the repo root.

## Verdict

No counterexample was found that loses durable, acknowledged data. The
commit protocol, compaction publish, orphan GC, retention tombstone flow, and
their crash matrices are defended with unusual care: durable-record-first
ordering, content-addressed idempotent PUTs, fresh re-verify before every
physical delete, fail-closed handling of unreadable state, and a two-fence
clock-skew budget on the protection horizon.

One structural hole was found and confirmed against a test that pins it:
the fold's reconcile machinery covers late compaction and rewrite records
only within a 26-hour window behind the watermark plus a retention-frontier
band, while the superseded-input GC sweep (rule 2) has no HEAD-reachability
gate. A compaction or erasure rewrite published into an hour more than 26h
behind the fold watermark is never applied to the folded snapshot, and 24-25h
later the sweep physically deletes the L0 objects that snapshot still names.
Result: persistent SnapshotInvalidated (503) for every query touching those
hours, self-healing only when the hour ages into the retention frontier
(weeks later, or never for a tenant without retention). No data is destroyed
(the L1 parts hold everything), so this is an availability and visibility
defect, not data loss. The erasure variant of the same hole (acknowledged as
open in ADR-0064) additionally opens a window where erased-subject records
become servable again after the `.dreq` predicate is swept. A second,
independent defect makes retention permanently incomplete on any bucket that
went through a selective-erasure rewrite.

## Evidence

Read paths, MVCC, and discovery:

- Phase 1 discovery: one LIST per (shard, hour) bucket or a per-shard prefix
  scan above the crossover, both funneled through the same classifier;
  fail-loud on unknown key shapes; tombstone excludes the bucket and
  invalidates its record cache. crates/ravel-catalog/src/catalog.rs:1283-1539
  (list_hour_bucket, process_bucket), 1565-1680 (prefix scan with runtime
  LIST cap, CatalogError::WindowTooWide, maps to 422 per
  docs/catalog-and-mvcc.md:800-820).
- Phase 2: hours at or below the HEAD watermark are served exclusively from
  snapshot parts; only the suffix above the watermark is listed.
  crates/ravel-catalog/src/catalog.rs:977-1000 (listing_start_hour =
  watermark + 1). Fallback to full listing on HEAD/part read failure, hard
  fail on tenant_hash/shard_count mismatch (isolation breach counters):
  crates/ravel-catalog/src/snapshot_resolve.rs:359 onward,
  docs/catalog-and-mvcc.md:822-882.
- Read-your-write: min_token exact GET with separate NotFound and transient
  retry budgets, then fallback to compaction-input / rewrite-effective-input /
  tombstone-zero-segments. crates/ravel-catalog/src/catalog.rs:1900-2150.
- Fold: incremental over (W_old, W_new], fixed reconcile window
  [W_old - 26h, W_old] triggered only by compaction/tombstone/rewrite records
  (L0-only buckets skipped), retention-frontier pass over snapshot-named hours
  at or below frontier_hi = (now - R + horizon), capped strictly below the
  fixed window, 168 hours per fold, remainder carried.
  crates/ravel-catalog/src/fold.rs:591-600 (bucket_needs_reconcile),
  949-977 (fixed window), 1016-1067 (frontier), 1025 (frontier_hi capped
  below fixed window), 1026-1034 (candidates are snapshot-named hours only).
- Compaction: seal gate, tombstone gate, already-compacted gate, min-inputs
  gate, no upper age bound. crates/ravel-maintain/src/compact.rs:49-71.
  Publish: parts first (content-addressed CreateIfAbsent), record last;
  abandonment mirror of the writer interlock at
  crates/ravel-maintain/src/publish.rs:127-135; sample-count conservation
  gate at 137-160; racing-loser convergence with part repair and fail-loud
  InputSetHashDivergence at 198-275.
- GC rules: crates/ravel-maintain/src/sweep.rs module doc 1-95. Orphan GC
  with batched re-verify LIST and mass-orphan circuit breaker (both
  min_count AND ratio must trip): sweep.rs:313-390. Superseded-input sweep,
  horizon-only gate, records before data: sweep.rs:448-543.
  Unreferenced-part sweep with exact-branch re-verify: sweep.rs:763-910.
  Catalog-object sweep with LIST-before-HEAD and pre-delete HEAD re-verify:
  sweep.rs:1121-1210.
- Retention: expiry from commit and compaction records only, tombstone
  CreateIfAbsent, horizon-gated physical sweep with an ADR-0020
  HEAD-reachability gate (Named blocks, Unreadable blocks fail-closed,
  Absent proceeds), verifying LIST before tombstone delete.
  crates/ravel-maintain/src/retention.rs:344-425 (expiry and tombstone),
  554-640 (physical_sweep), 156-220 (bucket_gate).
- Clock-skew fence: protection_horizon >= max_query_duration + grace +
  clock_skew_allowance, enforced at sys/gc write time and re-asserted at
  maintain startup against the running sweeper's own skew config.
  docs/consistency-model.md:361-431.
- Writer interlock exists in ingest: flush abandonment at
  max_flush_lifetime, crates/ravel-ingest/src/shard.rs:660, 1203;
  span_shard.rs:770.

## Counterexample attempts

### CE1. Late compaction record behind the folded watermark, then superseded-input sweep. HOLE (P1)

1. Fold runs in the serving process every 5 minutes
   (services/ravel-server/src/fold.rs:37-38, 94) and HEAD's watermark tracks
   the newest sealed hour. Compaction runs in a separate maintain process.
2. Compaction lags the watermark by more than 26 hours. Realistic triggers:
   first enablement of `--mode maintain` on a store with existing history; a
   maintain outage longer than about a day followed by automatic catch-up
   (scan_and_maintain full-scans every present hour,
   crates/ravel-maintain/src/scan.rs:1132-1216, and compact_bucket has no
   age ceiling, compact.rs:49-71); a v1-retirement campaign
   (min_compaction_inputs = 1 is legal config, ADR-0018 decision 1).
3. The compaction record lands in hour H where H < watermark - 26. The fixed
   reconcile window [W_old - 26, W_old] never reaches H (fold.rs:949-956),
   and the frontier pass covers only snapshot-named hours at or below
   now - R + horizon for tenants with a retention window (fold.rs:1016-1034);
   a tenant with no retention gets no frontier pass at all. The snapshot
   keeps serving hour H's L0 entries. This exact state is pinned as expected
   behavior by the test `reconcile_ignores_late_record_outside_window`
   (crates/ravel-catalog/src/fold.rs:3507-3557: "the out-of-window compaction
   is not applied ... hour 5 still holds the original L0").
4. 25h05m after the record's created_unix_ns, sweep rule 2 deletes the L0
   commit records and data objects the record names. Rule 2's only gate is
   the horizon (sweep.rs:479-484 for compaction, 496-501 for rewrite). It
   has NO HEAD-reachability gate; only the retention rule has one
   (retention.rs:563-577, docs/consistency-model.md:433-439 table row 2 vs
   row 4).
5. Every query over hour H now resolves the deleted L0 data keys from the
   snapshot (SegmentRefs reconstruct data keys from snapshot entry identity
   fields, no commit-record GET needed), the fetch gets NotFound, the engine
   re-resolves exactly once and gives up with SnapshotInvalidated
   (crates/ravel-query/src/engine.rs:1077, 1142). The re-resolve reads the
   same HEAD, so the failure is persistent: no fold ever re-lists hour H
   (for a retention tenant it heals when H ages into the frontier band,
   roughly age R - 25h, weeks later; for a no-retention tenant, never
   without operator intervention such as deleting HEAD to force a rebuild).
6. No counter or alarm detects the state; detection is user-facing 503s or
   an out-of-band `ravel-cli catalog verify`.

The comment at crates/ravel-catalog/src/config.rs:76-85 claims the 26h
window guarantees "any late record whose supersession could invalidate a
folded snapshot entry is observed by a reconcile pass before its inputs can
be physically deleted" and calls the out-of-window case "a stated, bounded
staleness tradeoff, not a bug." The claim silently assumes the record's
target hour is within 26h of the watermark at publish time; the horizon runs
from record publish, the window runs from the target hour, and nothing
enforces the assumption. This is the same failure shape
docs/consistency-model.md:460-462 records as "the shipped failure" that the
retention HEAD gate was built to fix; the superseded-input rule never got
the equivalent gate, and the fold never got a compaction-lag pass. VERIFIED
(code plus pinning test).

### CE2. Erasure rewrite outside the reconcile window. HOLE, partially acknowledged (P1)

1. ADR-0064 §3.1 scopes the rewrite pass to any sealed bucket regardless of
   age, so every DSAR over data older than 26h behind the watermark lands
   out-of-window by construction. ADR-0064's own correction
   (docs/adrs/0064-selective-subject-erasure.md:340-354) states the
   out-of-window case "remains open": the folded snapshot keeps serving
   pre-erasure inputs until something forces a re-fold.
2. While the `.dreq` is live, resolve attaches its predicate and the scan
   layer filters the subject (catalog.rs:1115-1123), so the acknowledged
   erasure bound holds.
3. The completion gate is listing-based, not snapshot-based:
   `bucket_erasure_completion` reconstructs the LIVE bucket view through
   resolve_rewrite_supersession (crates/ravel-maintain/src/
   erasure_rewrite.rs:1990-2115). The live listing correctly shows the
   rewrite superseding its inputs, so `.done` is written even though the
   folded snapshot (outside the window) still serves the pre-rewrite L0s.
   The ADR's binding requirement ("or must force a reconcile of every bucket
   in a request's scope regardless of window", ADR-0064:363-368) is
   satisfied only in its first, weaker branch.
4. `.dreq` is deleted at completed_ns + horizon (sweep.rs:1340-1390); rule 2
   deletes the pre-rewrite inputs at rewrite created_unix_ns + horizon.
   These fire on independent sweep cadences in either order. If the `.dreq`
   goes first, queries over the stale snapshot serve the erased subject's
   records unfiltered from the still-present pre-rewrite L0 objects until
   rule 2 removes them: a silent erasure violation window. If rule 2 goes
   first, the hour degrades to CE1's persistent 503.

Label: the stale-serving half is a DOCUMENTED CLAIM (ADR-0064 admits it);
the post-`.dreq`-sweep servable window and the post-sweep persistent 503 are
VERIFIED consequences of the same code paths as CE1.

### CE3. Retention never completes on a rewritten bucket. HOLE (P2)

1. Erasure rewrite runs on bucket B; after the horizon, rule 2 deletes B's
   superseded L0 commit records and data (or the superseded compaction
   record and parts). B's commit prefix now holds only `rw.<hash>.cmt` plus
   its L1 output parts.
2. Retention expiry reads only commit records and compaction records
   (retention.rs:410-424; max_event_ts at 481-496 takes no rewrite record).
   BucketListing carries rewrite_record_keys
   (crates/ravel-maintain/src/read.rs:47-60) but retention.rs never touches
   them (zero occurrences of "rewrite" in the file). max_event_ts = None, so
   is_expired returns false (retention.rs:504-509): the bucket is never
   tombstoned and its rewrite output parts are retained forever, past any R.
3. Alternative interleaving: B expires and is tombstoned while records are
   still present (possible only within ~25h of the rewrite). physical_sweep
   deletes listing.commit_keys, compaction_record_keys, L0 data, and the l1/
   prefix, but never listing.rewrite_record_keys (retention.rs:616-619).
   The verifying LIST then finds the rewrite record and returns false
   (bucket_is_empty_but_tombstone, retention.rs:676-683: any non-tombstone
   entry fails), so the outcome is SweptPartial forever: the tombstone and
   `rw.cmt` persist, and the pass re-runs every tick.
4. Either way retention never reaches Swept on an erased bucket. This
   CONTRADICTS docs/consistency-model.md:438, whose retention rule targets
   "everything in a tombstoned bucket," and ADR-0019 decision 7's bound that
   physical bytes are gone within R + horizon + a sweep interval. Visibility
   is unaffected (the tombstone or the rewrite supersession keeps exclusion
   correct), so this is a compliance and cost defect, not loss. No test in
   crates/ravel-maintain/tests/retention.rs exercises a bucket holding a
   rewrite record.

### CE4. Late L0 commit into a sealed, folded hour (clock skew or interlock violation). Designed-in assumption (P3)

Sequence: writer clock skew exceeds clock_skew_allowance (or a writer
violates the max_flush_lifetime abandonment), so a commit record lands in
bucket H after the fold sealed and folded H. The reconcile pass skips
L0-only buckets without any diff (fold.rs:591-600), so the record is never
added to the snapshot; Phase 2 never lists hours at or below the watermark
(catalog.rs:977-1000). The data is silently invisible to general queries
(min-token readers still see it via the exact GET). Defense: the seal
margin arithmetic (docs/catalog-and-mvcc.md:439-473), the config discipline
that folders adopt raised margins before writers, and out-of-band
`ravel-cli catalog verify`. The residual is an explicitly documented clock
assumption, detectable but not self-healing. WEAKLY VERIFIED (code paths
confirmed; the triggering condition requires a declared-bound violation).

### CE5. Concurrent duplicate compactors. DEFENDED

Both list the same sealed input set, derive the same input_set_hash, PUT
content-addressed parts (AlreadyExists is success), and race one
CreateIfAbsent record PUT. The loser GETs the winner, verifies the key,
HEADs every winner part and re-PUTs any missing one it built
(publish.rs:228-275). Divergent input sets on one sealed bucket return
MaintainError::InputSetHashDivergence, alarm, delete nothing
(publish.rs:240-246); the resolver includes both parts sets plus uncovered
L0s under overlap harmlessness and raises
compaction_input_set_conflicts (catalog.rs:1489-1498). VERIFIED.

### CE6. Compactor crash after part PUTs, before record PUT. DEFENDED

Record-less parts are invisible (visibility is carried entirely by the
record). A re-run reuses them via CreateIfAbsent; a divergent re-build
orphans them, and rule 3 collects them only after grace +
max_compaction_lifetime with the exact branch re-verified immediately before
delete (sweep.rs:798-827), which is safe because the publish path refuses to
publish past that same deadline (publish.rs:127-135). A bucket with neither
a record nor a tombstone keeps its record-less parts (classify_part,
sweep.rs:853-865). VERIFIED, exercised by
crates/ravel-maintain/tests/sweep_crash_matrix.rs
(unreferenced_part_swept_only_after_age_gate:564,
recovery_over_abandoned_parts_never_loses_a_named_part:742).

### CE7. Old reader pinned while GC deletes its inputs. DEFENDED (within declared bounds)

Every horizon-gated rule anchors on a durable timestamp, and the two-fence
budget (write-time gc-config validation plus maintain-startup re-assert of
this sweeper's own skew) guarantees protection_horizon covers
max_query_duration + grace + clock_skew_allowance
(docs/consistency-model.md:378-431). A reader that outlives its declared
deadline gets SnapshotInvalidated and one re-resolve
(engine.rs:1077-1142); post-supersession the re-resolve serves L1, so no
loss. Residual: mis-declared hardware skew or an unenforced query deadline,
both named in the doc. VERIFIED; sweep_crash_matrix.rs
row9_pinned_query_races_sweep_then_reresolves_against_l1:311 and
no_delete_before_horizon_boundary_stepped:495 pin it.

### CE8. Mass-orphan false positive destroying live data. DEFENDED

Orphan deletion requires the identity to be absent from two strongly
consistent commit-prefix LISTs (initial plus a batched fresh re-verify),
plus the age gate, plus the breaker: at least orphan_breaker_min_count
candidates AND more than orphan_breaker_max_ratio of listed L0s trips it and
deletes nothing (sweep.rs:313-390). A false positive below the breaker
requires the store to omit an existing record from two consistent LISTs,
i.e. a violation of the qualified store contract. Same-identity,
different-hash leftovers are conservatively treated as referenced
(sweep.rs:392-426). VERIFIED.

### CE9. Compaction races retention tombstone. DEFENDED

Retention runs before compaction and a tombstoned outcome skips compaction
(retention.rs:453-477); a racing compactor that publishes anyway is covered
by bucket-wide tombstone exclusion, and the physical sweep's verifying LIST
catches records that landed after its listing, leaving the tombstone for the
next pass (retention.rs:26-34, 629-639). VERIFIED.

### CE10. Retention sweeper clock skew retiring young data. Residual (P3)

is_expired compares max_event_ts against the sweeper's own now - R
(retention.rs:504-509) with no skew term, so a sweeper whose clock leads
true time by D tombstones data at age R - D; the horizon then protects
readers but the destruction is early by D. ADR-0019 decision 1 calls R an
exact floor without stating the clock assumption. Bounded by real skew,
same class as the documented sweeper-skew residual. IMPLEMENTED as designed,
assumption undocumented at this one site.

### CE11. Two maintain workers overlap during membership transition. DEFENDED

Rendezvous ownership is membership, not a lease; overlap yields idempotent
duplicate work (CreateIfAbsent publishes, idempotent deletes), documented at
docs/catalog-and-mvcc.md:64-105. Fold CAS serializes HEAD; a losing fold
retries next tick and its superseded parts are collected by sweep rule 5,
which reads HEAD after the LIST and re-verifies HEAD immediately before
deleting, and sweeps nothing when HEAD is absent (sweep.rs:1121-1210).
VERIFIED.

## Tests or commands run

No cargo commands were run (prohibited by charter; central build lock).
Evidence gathered by reading source and tests plus grep/find/ls only.
Key test suites read as coverage evidence:

- crates/ravel-catalog/src/fold.rs in-module tests:
  reconcile_applies_late_compaction_before_horizon:3305,
  reconcile_ignores_late_record_outside_window:3507 (pins the CE1 hole),
  reconcile_applies_late_tombstone:3562,
  frontier_reconcile_applies_out_of_window_tombstone:3632,
  frontier_reconcile_is_bounded_and_carries_remainder:3729.
- crates/ravel-maintain/tests/retention.rs:
  sweep_blocked_when_head_names_bucket:649,
  sweep_proceeds_when_head_absent:704,
  sweep_blocked_fail_closed_when_head_undecodable:753,
  sweep_respects_pre_fold_head_then_deletes_after_fold_drops_bucket:814,
  retention_of_out_of_window_hour_never_leaves_snapshot_naming_deleted_objects:891
  (retention only; no rule-2 analogue exists).
- crates/ravel-maintain/tests/sweep_crash_matrix.rs rows 7, 8, 9, 12,
  convergence, horizon-boundary, age-gate, abandoned-parts recovery.
- crates/ravel-catalog/tests/{compaction_resolution.rs, snapshot_resolve.rs,
  erasure_resolution.rs, fold_compaction_differential.rs, window_ceiling.rs}
  (presence and scope confirmed; not exhaustively read).

## Unknowns

- Whether any deployment guidance forbids enabling maintain (or catch-up
  compaction) against a store where fold has been running: I found none in
  docs/guides/operations.md, and no code gate exists (compact.rs:49-71).
- The exact end-to-end behavior of ravel-query's distributed path when a
  snapshot names deleted objects (assessed only to the coordinator's single
  re-resolve at engine.rs:1077-1142). NOT ASSESSED beyond that.
- Whether `ravel-cli catalog verify` (referenced in
  docs/catalog-and-mvcc.md:466-468) detects the CE1 poisoned state
  specifically. NOT ASSESSED.
- Postings exactness and query-side dedup (Agent D territory). NOT ASSESSED.

## Severity-ranked findings

1. P1. Compaction published >26h behind the fold watermark poisons the
   folded snapshot; superseded-input sweep then makes it persistent.
   VERIFIED. The fold's fixed reconcile window is anchored on the target
   hour (fold.rs:949-956) and the frontier pass covers only the retirement
   band (fold.rs:1016-1034); sweep rule 2 gates on the horizon alone with no
   HEAD-reachability check (sweep.rs:479-501), unlike retention
   (retention.rs:563-577). The stale state is pinned as expected by
   fold.rs:3507-3557, and the config comment's safety claim
   (config.rs:76-85) does not hold when compaction lags the watermark by
   more than the window. Consequence: persistent SnapshotInvalidated 503s
   over the affected hours (engine.rs:1077-1142), healing only via the
   retention frontier weeks later or never for no-retention tenants; no
   alarm. Triggers are ordinary operations: first maintain enablement on an
   existing store, a >1 day maintain outage with fold up, retirement
   campaigns. Fix shape already exists in-tree: give rule 2 the same HEAD
   gate retention has, or add a compaction-lag reconcile analogous to the
   frontier pass.
2. P1. Erasure out-of-window: same mechanism, acknowledged open in
   ADR-0064:340-354, plus a servable-erased-data window. DOCUMENTED CLAIM /
   VERIFIED consequence. The completion gate is live-listing-based
   (erasure_rewrite.rs:1990-2115), so `.done` and the subsequent `.dreq`
   sweep (sweep.rs:1340-1390) can remove the query-time predicate while a
   stale folded snapshot still serves the pre-rewrite inputs; until rule 2
   deletes those inputs, erased-subject records are servable again.
3. P2. Retention never completes on rewritten buckets. VERIFIED /
   CONTRADICTED (docs/consistency-model.md:438). Expiry ignores rewrite
   records (retention.rs:410-424, 481-496: rw-only buckets never expire);
   physical_sweep never deletes rewrite record keys (retention.rs:616-619)
   and the verifying LIST then fails forever (retention.rs:676-683),
   leaving permanent SweptPartial. Over-retention and unbounded re-work,
   not data loss. Untested combination.
4. P3. Late L0 into a folded hour is permanently invisible below the
   watermark if writer clock skew exceeds the declared allowance; the
   reconcile pass deliberately skips L0-only buckets (fold.rs:591-600).
   IMPLEMENTED per the documented seal-lemma assumption; detection is
   out-of-band only.
5. P3. Retention expiry carries no clock-skew term: a fast sweeper clock
   destroys data early by its skew (retention.rs:504-509). IMPLEMENTED,
   assumption undocumented at the site.
6. P3. Stale module doc: sweep.rs:1115-1120 claims "No production driver
   calls it yet" for sweep_unreferenced_catalog_objects, but the maintain
   tick drives it at services/ravel-server/src/maintain.rs:1353-1390.
   CONTRADICTED (doc bug; reported here per repo rule, not fixed).
7. P3. Compaction hierarchy is single-level per (shard, hour) with no
   cross-hour L2 (ADR-0018 names L2 as follow-up; ADR-0092 reduces bytes,
   not object count), so long-window queries touch at least one part per
   (shard, hour) and catch-up compaction over deep backlogs is exactly the
   operation finding 1 makes hazardous. IMPLEMENTED as designed; noted as a
   scaling boundary, not a defect.

## Confidence

High on findings 1-3 and 6: each is confirmed by direct code reading at the
cited lines, and finding 1's mechanism is additionally pinned by an in-tree
test asserting the stale state. Medium on finding 2's exact race ordering
(cadence-dependent) and on CE4/CE10, which require declared-bound violations
to trigger. Every conclusion is from source at the frozen commit; no build
or test execution was performed, so runtime behavior claims rest on the
cited code paths and existing test assertions rather than fresh runs.
