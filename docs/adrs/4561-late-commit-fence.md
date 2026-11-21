# ADR-4561: maintenance plans a sealed bucket only from on-time commit records

Status: Accepted pending owner sign-off on the PR. Issue #4561, follow-up of
#4449 and #4560. The full analysis, with `file:line` citations on the
checkout it was written against, is
`antithesis/design-4561-late-commit-fence.md`.

Migration classes touched (ADR-0066 decision 4): none. No persistent format,
proto schema, series identity, commit token or object key changes. The rule
reads the `last_modified` the store already returns on every listing.

## Context

### A commit PUT can land after the maintain seal

The writer interlock (ADR-0010 §11, ADR-2708 D3) stops a flush from
*issuing* its commit PUT for hour `H` after `end(H) + max_flush_lifetime`.
Nothing bounds when the store *applies* a PUT already sent. A commit record
can therefore become visible in `H`'s bucket after a maintenance pass has
listed `H` as sealed (`end(H) + max_flush_lifetime + clock_skew_allowance`):

- a buffered flush whose PUT the client gave up on after the rows were
  acknowledged, and which the store applied later;
- a strict flush or an idempotency-keyed replay, under a writer clock far
  behind the store with the lag check not enforcing, a process suspension
  the timers do not count, or a maintainer clock far ahead of the store.

Such a record is a late L0. In buffered mode it can belong to a write the
client was told is durable, with every clock inside its contract. So no fix
may delete it, hide it or stop serving it on the theory that nobody was
acknowledged.

### What a late L0 did

Readers serve a late L0 as an extra segment wherever a bucket is resolved
from its commit records (ADR-0018, read-side paragraph). Maintenance took
one LIST of a sealed bucket as the complete and final input set (ADR-0018
decision 1). Two passes over one bucket could therefore plan from different
sets: an erasure rewrite that listed before the late L0 landed, and a
compaction that listed after. The compaction record then names an input no
live rewrite holds, so ADR-1029's dominance rule cannot drop it. It is a
conflict:

- both records are served, so surviving rows of the shared inputs can be
  returned twice;
- the compaction's parts can hold the erased rows, which only the `.dreq`
  filter hides (ADR-1029, #4560 amendment);
- every later erasure pass over the bucket fails with
  `MultipleLiveRecords`.

The claim and the pre-publish re-list do not fence this ordering. The
claim's residual window (ADR-1029, 2026-11-20 amendment) is reached by a
stall, and even with no stall the two listings differ.

## Decision

Maintenance plans a sealed bucket only from the commit records the store
stamped before a fixed per-hour cutoff, waits for the store's own clock to
pass that cutoff before it plans, and leaves every later record exactly
where and as it is.

For a bucket of hour `H` under configuration `config`:

1. **R1, cutoff.** `C(H) = end(H) + config.seal_margin_ns()`, truncated
   down to a whole second (`Bucket::late_commit_cutoff_ns`). The seal margin
   is `max_flush_lifetime + clock_skew_allowance`, 65 minutes at the
   defaults. The truncation keeps the rule exact under a store whose stamps
   have 1-second granularity.
2. **R2, late.** A listed commit record is late when
   `last_modified_unix_ms * 1_000_000 >= C(H)` (`read::is_late_stamp`).
   Every other listed commit record is on time. Compaction records, rewrite
   records and tombstones are never classified.
3. **R3, plannable.** A pass may plan the bucket from raw L0 only when its
   own clock reads `now >= C(H) + SETTLE`, and the store's clock, read
   through `observed_store_time_ns()` after the planning LIST returned, is
   either absent or `>= C(H) + SETTLE` (`Bucket::is_plannable`). Otherwise
   the outcome is the existing `NotSealed`. `SETTLE` is 60 s
   (`LATE_COMMIT_SETTLE_NS`), three times the S3 adapter's 20 s request
   timeout. It is a chosen allowance for the gap between a stamp and the
   record's visibility in a LIST, not a derived bound. An absent observation
   is counted in `ravel_maintain_store_clock_unobserved_total{signal}`.
4. **R4, inputs.** Compaction, the migrate primitive and a raw-L0 erasure
   rewrite take the on-time commit records as their whole L0 set: for the
   minimum-input count, for the inputs they load, and for the identities the
   published record names.
5. **R5, re-list.** The pre-publish re-list compares the on-time commit
   records, classified under the planned listing's cutoff, plus the other
   three record kinds as before. A late arrival between plan and publish
   does not abandon the pass; an on-time arrival still does.
6. **R6, everything else sees every record.** The erasure completion gate,
   migrate's raw-served set and straggler audit, the legal-hold check,
   retention, the sweep and every read path keep using the full listing. A
   late record is served, kept, held, expired and counted exactly as before.
   Under `Enforce` the completion gate judges a bucket only once it is
   plannable by R3, with the store's observed clock, the same test the
   raw-L0 erasure rewrite plans under (`may_judge_completion`); before then
   it blocks every request whose scope held the bucket at acknowledgement,
   as it blocks for an unsealed bucket. A sealed bucket is not enough: up to
   `SETTLE` past the cutoff an on-time record stamped just before it may not
   be listed yet, and a `.done` written then would never be revisited.
7. **R7, switch.** `CompactorConfig::late_commit_fence` is `Off` or
   `Enforce`. The library default is `Off`, the behaviour before this ADR.
   `ravel-server`'s maintenance configuration and `ravel-cli maintain
   compact-bucket`, `compact-tenant` and `migrate` set `Enforce` unless
   `--late-commit-fence off` is given.
8. **R8, visibility.** Each plan that leaves a late record out logs its key
   at warn and counts it in `ravel_maintain_late_commit_records_total{signal}`.
   `ravel-cli maintain status --hour` prints `late_commit_records` beside
   `l0_commit_records`. `maintain migrate` names a bucket whose remaining
   below-target commit records are late under `not_migrated` as
   `LateCommitRecords { records }`, and the format floor stays down for that
   stated reason. Two invariants evaluate the rule under the
   `ravel-invariant` capture build: "maintain: a plan left a late commit
   record out" (sometimes) and "maintain: a published record names no late
   commit record" (always).
9. **R9, the `.dreq` keep.** The erasure-request sweep (rule 6) keeps a
   candidate `.dreq` past its horizon while a bucket of the request's tenant
   and signal holds a late record (R2) whose event-time span the request's
   window may overlap (`bucket_may_overlap`; a request with no window
   overlaps every bucket). It does so whatever the fence setting.

### Why this removes the partial cover

Under R1 to R3, a LIST that a plan may use runs once the store's clock is
past `C(H) + SETTLE`. Every commit record stamped before `C(H)` is visible
to that LIST, given the settle allowance and the list-after-write guarantee
the seal lemma already relies on. Every commit record stamped at or after
`C(H)` is excluded by R2 whenever it arrives. So every plan of the bucket,
by any pass at any later time, has the same L0 set, provided every pass
derives `C(H)` from the same seal margin. The margin is each process's own
configuration (R1), so this needs every maintainer of the store to run the
same `max_flush_lifetime` and `clock_skew_allowance` while the fence is on:
two passes with different margins draw different cutoffs, disagree on which
records are late, and can again plan different sets. A compaction that
publishes beside a rewrite names inputs the rewrite holds in full, and
`erasure_dominance` resolves it as dominated. No stall length changes this,
because the argument uses no time after the LIST.

Two store properties are assumed and not tested here:

- **[external]** a record's `LastModified` does not change after it is
  written, on the path the store serves (a copy, replication or restore that
  re-stamps breaks this);
- **[external]** the store stamps a record less than `SETTLE` before the
  record becomes visible in a LIST.

When either fails for a bucket, that bucket gets the behaviour before this
ADR, and `ravel_catalog_erasure_overlap_conflicts_total` still reports any
conflict it produces.

### Why no acknowledged write is lost

R6. No code path deletes, moves or stops serving a commit record because it
is late. The fence only removes inputs from plans.

### Why the library default is `Off`

Three callers publish commit records into long-sealed hours, with a fresh
store stamp, on purpose: `ravel-cli commit reconstruct` (every rebuilt
record is late by R2), the benches that seed a synthetic past hour and then
compact it, and many maintenance tests that seed after moving the store's
clock. With `Enforce` as the library default the benches would compact
nothing and exit zero, and the fixtures would change meaning. The two
service entry points set `Enforce`, so the production maintainer and the
Antithesis rigs, which run the real server, get the fence.

### Why a late record keeps a completed request's `.dreq`

No plan takes a late record (R4), so no rewrite erases it. The completion
gate reads it (R6), but only if it is stored when the gate runs. A late
record the store applies after the request's `.done` was written is an
ordering the gate never sees: the request is complete, its `.dreq` passes
its horizon, rule 6 deletes it, and the query-time filter that was hiding
the late record's rows of the subject goes with it. The superseded-input
hold does not cover the case either, since the late record is an input of
no record and so of no chain.

R9 closes it at rule 6. The sweep already lists the signal's commit
keyspace once for its observing rule-2 pass; it classifies each commit
record of that listing by R2 against its own bucket's cutoff, GETs each late
one for its event-time span, and skips a bucket with a retention tombstone,
which serves nothing. Only when there is a late record does it GET each
candidate `.dreq` to compare windows. A kept request is counted in
`ravel_maintain_dreq_held_by_superseded_inputs_total`, the counter every
other rule-6 keep uses, and logged once at warn naming the bucket. The keep
is recomputed every pass and never persisted, so the `.dreq` is released on
the first pass after the late record is deleted or its bucket tombstoned.

The classification is applied whatever the fence setting, because a stamp
says what the store applied late whether or not this pass's planners fence
it. With the fence off, a later compaction supersedes the late record and
rule 2 reclaims it, which ends the keep. That compaction copies the late
record's rows into L1, where the rewrite of an already completed erasure
request never reaches them, so once the keep ends those rows are served
again. Before running with the fence off over a bucket holding a late
record, the operator lists the tenant's erasure requests (`ravel-cli erase
status --tenant <t> --signal <s>`) and resubmits, after the run, every
completed one whose window overlaps the affected hours; see the residual
below.

## Options considered

The design document compares each in full; in short:

- **(a) Fold and reader side: readers ignore late records.** It hides an
  acknowledged write, which section "A commit PUT can land after the
  maintain seal" rules out until writers acknowledge only on store evidence.
- **(b1) Maintenance planning side, this decision.** Deletes nothing, hides
  nothing, changes no read and no format.
- **(c) Writer side: a commit PUT that cannot land late.** S3 offers no
  conditional PUT on the store's clock; a signature expiry bounds it only
  coarsely and is not testable here.
- **(d1) The writer acknowledges on store evidence.** The right long-term
  step (follow-up 1 below), but it does not stop a late record from landing.
- **(d2) Record the sealed set in the store.** A new object per bucket and a
  key-layout change.
- **(d3) Readers resolve a partial cover.** A second dominance case in every
  bucket resolution, with a loss surface in the sweep; it repairs existing
  conflicts and is the backstop for the residuals, as its own ticket.

## Consequences

- **Liveness cost.** The first plan of hour `H` waits until `C(H) + 60 s`
  on both clocks, about a minute later than before. A bucket whose commit
  records were all re-stamped after it sealed (reconstructed, or restored
  by a re-stamping copy) is never compacted under `Enforce`: every plan
  counts all its records as late and plans nothing. The operator runs that
  shard's maintenance once with `--late-commit-fence off` after its writers
  are gone. This is loud (counter and warn per key) and reversible.
- **A late L0 stays raw for the life of its bucket.** An erasure request
  whose window overlaps it stays blocked at the completion gate, which reads
  every record (R6), and raises the deadline alarm. A format floor stays
  down while it is stored. Both were already true of a late L0 that landed
  after a bucket's compaction; the fence extends them to one that lands
  between the seal and the first plan.
- **A late record holds a completed request's `.dreq` while it is stored**
  (R9), when its event times may overlap the request's window. The subject
  identifier the `.dreq` carries is retained that much longer.
- **No repair.** A conflicting compaction record already stored stays a
  conflict; the #4560 keep still protects its erased rows.
- **`CompactorConfig` gains a field.** Every struct literal that spells the
  config out names it or takes `..Default::default()`.

## Residuals and follow-ups

Residuals of the fence itself:

- A stamp-to-visible gap longer than `SETTLE`, or a maintainer on a store
  that gives no clock observation and whose own clock runs more than
  `SETTLE` ahead of the store. An on-time record can then appear after a
  plan's LIST. The pre-publish re-list still catches it up to the record
  PUT; past that it is the window before this ADR.
- A wrong store-clock observation. The latest observation wins, so one wrong
  `Date` moves it. R3 also requires the pass's own clock, so a wrongly high
  observation gives the behaviour before this ADR and a wrongly low one
  gives `NotSealed` until the next response.
- R9 sees the late records of the commit listing its pass took. A late
  record the store applies after that LIST and before the same pass deletes
  a `.dreq` is not seen by that pass.
- A compaction run with `--late-commit-fence off` over a bucket holding a
  late record copies the record's rows into L1. A completed request's
  rewrite never reaches them there, so once rule 2 reclaims the late record
  R9 releases the `.dreq` and those rows are served again. Before such a run
  the operator lists the tenant's erasure requests with `ravel-cli erase
  status --tenant <t> --signal <s>` and resubmits, after the run, every
  completed request whose window overlaps the affected hours, so a new
  rewrite covers the L1 segments. Follow-up 2 removes the need.
- The cutoff derives from each process's seal margin (R1). Every maintainer
  of a store, `ravel-server`'s maintenance (`--gc-max-flush-lifetime` and
  the clock-skew allowance) and every `ravel-cli maintain` run, must use the
  same `max_flush_lifetime` and `clock_skew_allowance` while the fence is
  on, or two passes disagree on which records are late and can rebuild the
  partial cover. `ravel-cli maintain compact-bucket` and `compact-tenant`
  refuse `--max-flush-lifetime` other than the default unless
  `--late-commit-fence off` is given. Nothing checks the server's setting
  against its peers: a rolling change of either value is done with the
  fence off on every maintainer, or by draining every maintainer on the old
  configuration before any starts on the new one.

Follow-ups, in order:

1. Writers acknowledge on store evidence, so a strict `Ok` implies an
   on-time record.
2. Dispose of late L0s without loss: a maintenance step that takes them into
   the bucket's live record, so an erasure and a format floor can complete.
   The rewrite record cannot express "these inputs plus that predecessor"
   today, so this is a format decision with its own ADR.
3. A store-side bound on the commit PUT, if the residuals are ever observed.
4. Readers ignore late records, only behind a compatibility certificate that
   no writer without follow-up 1 remains.

## Tests

In `crates/ravel-maintain/tests/late_commit_fence.rs`, with stamps set on
`MemoryStore` before each seed:

- `a_commit_record_stamped_at_the_cutoff_is_left_out_of_every_plan`: of
  three inputs stamped `C(H) - 1 ms`, `C(H)` and `C(H) + 1 h`, a compaction,
  the migrate primitive and a raw-L0 erasure rewrite each name exactly the
  first (R2, R4). With the fence off it names all three.
- `a_late_l0_between_the_two_listings_is_a_full_cover`: the erasure rewrite
  is parked after its re-list, a late L0 lands, a compaction plans, re-lists
  and is parked, then both publish. The rewrite dominates the compaction
  record, every surviving row is served once, the erased series is not
  served, the late row is served from its L0, and the erasure rerun reports
  `AlreadyApplied`. With the fence off the same script ends in a conflict
  and `MultipleLiveRecords`
  (`with_the_fence_off_a_late_l0_between_the_listings_is_a_partial_cover`).
- `an_acknowledged_write_that_lands_late_is_served_and_never_swept` (R6):
  after a compaction, an erasure rewrite and two sweeps past every horizon,
  the late commit record and data object are stored, and a listing resolve,
  a token resolve and a resolve from a folded HEAD each serve its row once.
  With the fence off the sweep deletes it.
- `a_plan_waits_until_the_store_clock_passes_the_cutoff` (R3), with exact
  counter deltas for the absent observation.
- `a_late_arrival_does_not_abort_a_publish` and
  `an_on_time_arrival_still_aborts_a_publish` (R5).
- `a_late_l0_holding_the_subject_keeps_the_request_blocked`,
  `a_late_l0_outside_the_window_lets_the_request_complete` and
  `a_late_l0_keeps_a_windowless_request_blocked_whatever_it_holds`: the
  completion gate reads every record and decides on window overlap.
- `completion_waits_until_the_bucket_is_plannable` (R3, R6): under the
  fence a sealed bucket that is not yet plannable, by the pass's clock or
  the store's, keeps an in-scope request blocked and is not listed; once
  plannable it is judged. With the fence off it is judged at the seal. In
  `ravel-server`,
  `run_erasure_pass_completes_only_once_the_ack_hour_is_plannable` shows no
  `.done` is written before then.
- `a_fenced_run_refuses_a_flush_lifetime_override` and
  `compact_binaries_refuse_a_flush_lifetime_override_under_the_fence`
  (`ravel-cli`): the seal-margin residual's refusal.
- `a_late_l0_landing_after_completion_keeps_the_dreq` (R9): the request
  completes with no late record present, its `.dreq` and `.done` are
  written, and then a late record holding the subject inside the window
  lands. Past the horizon, after rule 2 has removed the rewrite's
  superseded inputs, rule 6 keeps the `.dreq` and counts it as held.
  Without the keep it deletes it.
  `a_late_l0_keeps_a_windowless_dreq_whatever_it_holds`,
  `a_late_l0_outside_the_window_does_not_keep_the_dreq`,
  `the_dreq_is_released_once_the_late_l0_is_gone` and
  `a_late_l0_in_a_tombstoned_bucket_does_not_keep_the_dreq` pin the
  window rule, the release and the tombstone skip.
- `a_late_below_target_l0_is_named_and_the_walk_goes_on` (site 10).
- `determinism::the_split_is_the_same_for_every_order_and_page_boundary`,
  a property test over insertion order and two-key listing pages, and
  `the_cutoff_is_the_seal_truncated_to_a_whole_second` (R1, R2).
- `with_the_fence_off_every_commit_record_is_planned` (R7).
- `a_fenced_compaction_evaluates_both_invariants` (R8), in the capture
  build.
