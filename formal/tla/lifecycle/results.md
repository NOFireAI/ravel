# Lifecycle GC model-checking results

Toolchain: tla2tools 1.7.4, TLC2 version 2.19 (rev 5a47802), OpenJDK 21. All
runs below are the executor's; the exhaustive configuration was not run by the
executor (see the last section). TLC checked this finite model under the bounds
and assumptions in each `.cfg`. This model verifies the protocol design;
implementation conformance is argued in `traceability.md` and asserted by the
named Rust tests, not proved.

## Smoke

`smoke.cfg` against `Spec`, all fourteen invariants, `VIEW View`, `MaxClock = 2`,
`FullEnv = TRUE`, every switch at its shipped value.

- Result: PASS.
- States generated: 465912. Distinct: 82284. Diameter (depth): 21. Wall: under 10 s.
- Band (`bands.tsv`): distinct within 82150 to 82420. Observed 82284 is inside.
- The distinct-state count and depth both moved up from the prior round's
  committed band (79200-79500, depth 19) after round four's three fixes
  (below): each widens the reachable state graph rather than pruning it (an
  absent HEAD now permits a delete it used to block; `SweepTombstone` adds a
  wholly new reachable state per swept bucket). `bands.tsv` is re-measured
  against this round's final model and updated to 82150-82420, depth 21.

## Negative controls

Each flips one switch and must break one named invariant (exit 12). Each
control runs with `FullEnv = TRUE` and the full thirteen-invariant list from
`smoke.cfg` (finding 5: six of the seven previously ran with `FullEnv = FALSE`
and a list scoped to `TypeOK` plus the one target, with no cfg comment
justifying the reduction). Widening to the full environment and invariant
list was checked to still isolate each control's target before the cfgs were
changed: a scratch run of each control's constants against the full invariant
list reported the same single invariant and no other, so none needed to keep
the reduction. All seven fire their target and only their target:

| control | switch flipped | invariant violated |
|---|---|---|
| delete-before-horizon | DeleteBeforeHorizon | NoDeleteInsideProtectionWindow |
| refresh-failure-is-no-hold | RefreshFailureSweepsAnyway | RefreshFailureNeverSweeps |
| superseded-sweep-ungated | SupersededSweepUngated | HeadNamedObjectNeverDeletedBySupersededSweep |
| dreq-ignores-held-inputs | DreqIgnoresHeldInputs | DreqSweepRespectsLegalHold |
| rewrite-identity-omits-requests | RewriteIdentityOmitsRequests | IdenticalInputSetsDoNotCollide |
| rewrite-keeps-erased-records | RewriteKeepsErasedRecords | RewriteOutputsAreInputsMinusErased |
| complete-ignores-served-set | CompleteIgnoresServedSet | CompletionImpliesNoPreRewriteExposure |

`dreq-ignores-held-inputs` was retargeted this round (finding 1): the switch
is named for the held-input clause, but before this round it only broke
`DreqRemovalCannotResurrect`'s pinned-reader clause, a different scenario
its own comment described incorrectly. It now breaks
`DreqSweepRespectsLegalHold`, the invariant that names the held-input
clause the switch actually bypasses; see
`counterexamples/dreq-ignores-held-inputs.md`.

Traces are under `counterexamples/`; each control reaches its violation within a
few actions.

## Non-vacuity mutants

Seven invariants are shown breakable by mutating the behaviour (not a switch) in
a scratch copy under `/tmp` and running TLC against `smoke.cfg` (or, where noted,
a scratch cfg narrowed to one target invariant the same way a negative control
is). Exact lines:

- `HeldObjectNeverDeleted` (removed the `~HeldObject` gate from both the
  retention and superseded sweeps; TLC reaches the violation through the
  superseded sweep):
  `Error: Invariant HeldObjectNeverDeleted is violated.`
- `TombstoneExcludesBeforeDelete` (removed the `PresentObj("tombB1")` gate from
  both `RetentionSweep` and `DropRetiredBucketFromHead`; removing it from the
  sweep alone is unreachable because HEAD can only be emptied of the bucket after
  the tombstone exists):
  `Error: Invariant TombstoneExcludesBeforeDelete is violated.`
- `ErasedSubjectNeverServedAfterRequest` (made `RequestErasure` skip the `.dreq`
  write):
  `Error: Invariant ErasedSubjectNeverServedAfterRequest is violated.`
- `RewriteOutputsAreInputsMinusErased`, kept direction (finding 3: dropped a
  surviving, non-erased record from the rewrite output; before this fix the
  model had only one record and one subject, and the sole request erased that
  subject, so the "kept" direction of the `<=>` was a state-independent FALSE
  no behaviour could reach):
  `Error: Invariant RewriteOutputsAreInputsMinusErased is violated.`
- `CompletionRespectsLegalHold` (finding 1: removed `CompleteErasure`'s
  unconditional `~HeldInputServes("s1")` gate; a hold placed before completion
  no longer blocks it):
  `Error: Invariant CompletionRespectsLegalHold is violated.`
- `DreqRemovalCannotResurrect` (finding 1, incidental to fixing the held-input
  gate: removed `DreqSweep`'s unconditional `~ServesAny("s1")` gate, run
  against a scratch cfg scoped to this one invariant so the report names it
  specifically rather than the `ErasedSubjectNeverServedAfterRequest` a
  full-list run reports first for the same state):
  `Error: Invariant DreqRemovalCannotResurrect is violated.`
- `TombstoneNotDeletedBeforeBucketEmpty` (round four, finding 3: removed the
  `\A o \in DataObjects : Bucket(o) = "b1" => ~PresentObj(o)` conjunct from a
  scratch copy of `SweepTombstone`, so it deletes the tombstone while a raw
  input in the same bucket is still present):
  `Error: Invariant TombstoneNotDeletedBeforeBucketEmpty is violated.`

Details under `counterexamples/held-object-mutant.md`,
`counterexamples/tombstone-mutant.md`, `counterexamples/erased-subject-mutant.md`,
`counterexamples/rewrite-output-surviving-record-mutant.md`,
`counterexamples/completion-ignores-legal-hold-mutant.md`,
`counterexamples/dreq-removal-pinned-reader-mutant.md`,
`counterexamples/sweep-tombstone-empty-bucket-mutant.md`. Together with the
seven negative controls (one of which, `rewrite-keeps-erased-records`, targets
an invariant already covered by a mutant above), all thirteen named safety
invariants have a recorded TLC violation, so none is vacuously true.

## Per-invariant store/witness audit

Every safety invariant reads the modeled store, the current HEAD, or the `lastGc`
witness recording what a delete OBSERVED at its own step. None reads a bookkeeping
flag the action sets to certify itself, and none reduces to a constant.

- `NoDeleteInsideProtectionWindow`: reads `lastGc` (rule, deleted, permittedNeeds,
  atClock) and `supersededAt`; the witness records the permitted in-window query
  needs at the delete step.
- `HeldObjectNeverDeleted`: reads `lastGc.held`, recorded from `heldBuckets` at
  the delete step, and `lastGc.deleted`.
- `RefreshFailureNeverSweeps`: reads `lastGc.refreshWasFailed` and `lastGc.deleted`.
- `TombstoneExcludesBeforeDelete`: reads the store (`PresentObj("tombB1")`),
  `tombRetiredAt`, and the witness rule/deleted.
- `TombstoneNotDeletedBeforeBucketEmpty`: reads `lastGc.rule` and the store
  (`PresentObj(o)` over `DataObjects` in the bucket), not a bookkeeping flag
  `SweepTombstone` sets about itself.
- `ErasedSubjectNeverServedAfterRequest`: reads the store and HEAD via
  `ServedRead`, and `erasureRequested`.
- `RewriteOutputsAreInputsMinusErased`: reads the materialised output content
  `objContent["rwA"]` via `ServesSubject`.
- `CompletionImpliesNoPreRewriteExposure`: reads the store (`PresentObj("doneR1")`)
  and the current HEAD content via `ServesNow`.
- `CompletionRespectsLegalHold`: reads `lastGc.rule` and `lastGc.heldInputServed`,
  the per-step witness `CompletionWitness` sets from `HeldInputServes("s1")`
  (store presence plus `heldBuckets`) at `CompleteErasure`'s own transition, not
  the live value a later hold change could flip.
- `DreqRemovalCannotResurrect`: reads the store and HEAD/pinned-query serving
  state via `ServesAny`.
- `DreqSweepRespectsLegalHold`: reads `lastGc.rule` and `lastGc.heldInputServed`,
  the same per-step witness as `CompletionRespectsLegalHold`, set from
  `GcWitness("dreq", ...)` at `DreqSweep`'s own transition.
- `IdenticalInputSetsDoNotCollide`: reads `variantKey`, which records the actual
  names `PerformRewrite` stored for the two variants.
- `HeadNamedObjectNeverDeletedBySupersededSweep`: reads the current HEAD and the
  store (`PresentObj`) over the raw inputs.

## Candidate #1133: CONFIRMED unsafe

`candidate-1133.cfg` sets `HorizonGuardsPinnedQueries = FALSE`: a sweep delete
gates on the horizon and an unnamed HEAD but not on an in-window pinned query.
TLC violates `NoDeleteInsideProtectionWindow` (exit 12).

Trace (six states): a reader pins on the current HEAD naming `raw1`; a rewrite
supersedes `raw1` (`supersededAt = 0`) and materialises `rwA`; the clock reaches
the horizon; `HeadAdvanceRewrite` advances HEAD to `{rwA}` so the input is no
longer HEAD-named; the superseded sweep then deletes `raw1` while the pinned
query, still in window at `clock = 1 <= deadline = 1`, needs it. The witness
records `rule = superseded`, `deleted = {raw1}`, `permittedNeeds = {raw1}`, so
the intersection is non-empty and the invariant fails. Full trace in
`counterexamples/candidate-1133.md`.

Verdict: the horizon plus unnamed-HEAD gate is not sufficient; the pinned-query
clause is load-bearing. The shipped model keeps `HorizonGuardsPinnedQueries`
TRUE, under which this trace has no successor.

## Findings from the epic

- #1132 and #1134 are modeled as fixed: the shipped model is safe and their
  ungated variants are carried as the `superseded-sweep-ungated` and
  `dreq-ignores-held-inputs` negative controls.
- #1131 is a liveness limitation, not a safety defect. `EventuallySwept` and
  `EventuallyCompleted` are conditional on fairness, on the fold's and the
  sweep's retention windows agreeing, and (round two, finding 2) on the
  environment eventually going quiet on hold state, HEAD read state, and
  refresh outcome; when any of those keeps changing forever, or the retention
  windows disagree, the properties are intentionally false. `exhaustive.cfg`
  itself was not run by this task; a reduced, single-property diagnosis was,
  and both properties fail at `MaxClock = 2` for the reasons in "Liveness,
  reduced diagnosis" below. This is consistent with, not contradicting, the
  orchestrator's `exhaustive.cfg` report of a liveness violation.

## Review findings (issue #1122)

The fifteen adversarial review findings and their disposition:

1. Guard-mirror invariants replaced by store/witness reads: every safety
   invariant now reads the store, HEAD, or the `lastGc` witness (audit above).
2. Serving is store-derived via `objContent`; `ServesSubject`/`ServesNow` read
   the modeled object content, not a bookkeeping set.
3. `supersededAt` records when a rewrite superseded its inputs, so the
   superseded sweep's horizon gate is observed, not assumed.
4. Rewrite identity is store-derived: `variantKey` records the actual output
   names, and `IdenticalInputSetsDoNotCollide` reads them.
5. Sweeps fail closed on a non-present HEAD (`HeadDeletable`): an absent or
   unreadable read blocks the pass rather than being treated as empty.
6. The superseded sweep gates on `clock >= supersededAt + horizon` and on the
   pinned query, closing the delete-before-horizon path for superseded inputs.
7. Rewrite output content excludes erased subjects (`RewriteOutputContent`), and
   `RewriteOutputsAreInputsMinusErased` reads the materialised content.
8. Completion checks the served set (`~ServesNow`) at HEAD level; the pinned
   reader path is handled by the `.dreq` sweep and `DreqRemovalCannotResurrect`.
9. The config horizon inequality is an `ASSUME` on the constants, not a runtime
   invariant that would only restate a constant.
10. Non-vacuity is proven by behaviour mutation with recorded TLC lines, not by a
    switch the invariant reads.
11. Negative controls are target-scoped (`TypeOK` plus one invariant) so none can
    pass on the wrong first invariant.
12. The tombstone non-vacuity mutant removes the gate from both the sweep and the
    HEAD-drop, because the single-gate mutant is unreachable under finding 5.
13. `bands.tsv` carries only the measured smoke row; the unmeasured exhaustive
    row was removed rather than shipped as a fabricated band.
14. The candidate trace is the honest superseded-sweep path, not the removed
    absent-HEAD shortcut that finding 5 closed.
15. README and traceability claim only what the lanes this task ran showed, use
    ADR-1113 D12 language, and separate safety from liveness.

## Round two findings (issue #1122)

A follow-up adversarial review of the round-one fix above raised five more
findings. Disposition:

1. (CRITICAL) `CompleteErasure` and `DreqSweep` did not consult `heldBuckets`,
   so both could fire while a legal hold protected a still-present superseded
   input serving the erased subject, unlike the real code (`bucket_is_held`
   gates `bucket_erasure_completion` in `erasure_rewrite.rs`;
   `chain_groups_held_by_legal_hold` folds into `held_request_ids` and gates
   the `.dreq` sweep in `sweep.rs`). Fixed: both actions now gate on
   `HeldInputServes("s1")`, `CompleteErasure` unconditionally (the code has no
   bypass) and `DreqSweep` under the existing `DreqIgnoresHeldInputs` switch.
   Two new invariants, `CompletionRespectsLegalHold` and
   `DreqSweepRespectsLegalHold`, read a new per-step witness field
   (`lastGc.heldInputServed`) rather than the live `HeldInputServes`, so a
   hold placed or released strictly after a legitimate completion or sweep
   does not retroactively fail them (see the per-invariant audit above). The
   `dreq-ignores-held-inputs` negative control now targets
   `DreqSweepRespectsLegalHold`, the invariant its own name actually
   describes, instead of `DreqRemovalCannotResurrect` (see
   `counterexamples/dreq-ignores-held-inputs.md`). A dedicated behaviour
   mutant proves `CompleteErasure`'s new unconditional gate is load-bearing
   (`counterexamples/completion-ignores-legal-hold-mutant.md`). "Legal hold"
   is documented as a `.dreq` and completion hold reason alongside the
   existing HEAD-named and pinned-query reasons in the invariants section of
   `README.md`.
2. (MAJOR) `exhaustive.cfg` (`FairSpec`, `MaxClock = 4`) is reported by the
   orchestrator to exit 13 (a liveness violation) via a short lasso through
   repeated hold placement and release, not one of the three conditions the
   README already named (a legal hold, a stopped maintainer, or a fold/sweep
   retention-window disagreement, #1131). Diagnosed without running
   `exhaustive.cfg`: each of `EventuallySwept` and `EventuallyCompleted`,
   checked alone at `MaxClock = 2`, fails, for the same root cause as the
   orchestrator's lasso (`FairSpec` gives no fairness to any environment
   action, so one cycling forever can recurrently disable a fairly-scheduled
   action's guard). See "Liveness, reduced diagnosis" below for the two TLC
   traces. Fixed by documentation: README now states both properties under
   the wider hypothesis that the environment (holds, HEAD read state, refresh
   outcome) eventually goes quiet, rather than by adding fairness to any of
   `PlaceHold`/`ReleaseHold`/`SetHeadState`/`SetRefresh` (none of their
   real-world analogs has a cited implementation guarantee of progress).
3. (MEDIUM) `RewriteOutputsAreInputsMinusErased` had a decorative guard: the
   "kept" direction of the multiset claim was unfalsifiable because the model
   had only one record and one subject, and the sole request erased that
   subject, making that direction a state-independent FALSE. Fixed by adding
   a second subject and record (`s2`/`rec2`) that no request erases;
   `counterexamples/rewrite-output-surviving-record-mutant.md` proves the
   direction is now falsifiable.
4. (MEDIUM) Disposition of prior finding 10, `PredecessorChainRepresentable`:
   this was a constant operator (`Predecessors("rwA") == RawInputs`
   unconditionally, reading no store or witness state), so no reachable
   behaviour could ever falsify a claim built on it; round one deleted the
   invariant in commit `cf95fe6` with no comment, no results.md entry, and no
   README mention. That deletion is recorded here now. `Predecessors` itself
   remains, used correctly inside `RewriteOutputsAreInputsMinusErased` as an
   existential source (`\E i \in Predecessors("rwA") : ServesSubject(i, s)`),
   which does not claim predecessor chains are representable, only that the
   rewrite output's served set is drawn from its actual predecessors' actual
   content. Checked: neither `README.md` nor `traceability.md` claims
   predecessor-chain representability anywhere; no replacement is needed.
5. (MEDIUM) Six of the seven negative controls ran with `FullEnv = FALSE` and
   checked only `TypeOK` plus their one target invariant, with no cfg comment
   justifying the reduction. Fixed: all seven now run with `FullEnv = TRUE`
   and the full thirteen-invariant list; each still fires only its target
   (see the "Negative controls" section above). No control needed to keep the
   reduction.

Disposition of prior finding 15: round one's item 15 above ("README and
traceability claim only what the lanes this task ran showed... and separate
safety from liveness") is still the operative rule; this round's finding 2
fix changes what the README says it showed (a per-property reduced run and
its hypothesis, replacing "makes no claim"), but does not add a claim the
lanes run here did not support. See "Liveness" below.

## Liveness, reduced diagnosis (finding 2)

The orchestrator reported `exhaustive.cfg` (`FairSpec`, `MaxClock = 4`) exiting
13 via a 7-state lasso through repeated `PlaceHold`/`ReleaseHold`, not one of
the three conditions the README named at the time (a legal hold left in place,
a stopped maintainer, or a fold/sweep retention-window disagreement, #1131).
This task did not run `exhaustive.cfg` (forbidden; the run is long enough to
risk the idle-kill timeout). Instead, each of the two liveness properties was
checked alone, in a scratch cfg scoped to `TypeOK` plus that one property, at
`MaxClock = 2` (small enough to reproduce a violation quickly), `FullEnv =
TRUE`, `SPECIFICATION FairSpec`, every switch at its shipped value. Both fail:

- `EventuallySwept`: TLC exit 13. A 9-state behavior ending in stuttering, not
  a lasso. `SetRefresh` sets `refreshFailed = TRUE` (state 4) and no later
  state clears it; `superseded = {"raw1"}` and `RawInputs \cap head = {}` hold
  from state 9 on (the antecedent is true), `raw1` stays present, and
  `SupersededSweep` is fail-closed on `refreshFailed` (`RefreshFailureNeverSweeps`),
  so it stays disabled forever. `SetRefresh(FALSE)` is enabled at state 9 (it
  would clear the failure) but `FairSpec` grants it no fairness, so the
  environment is free to never call it again. Exact line:
  `Error: Temporal properties were violated.` with the counter-example above
  (`/tmp/liveness-swept.out` this session; not committed, scratch only).
- `EventuallyCompleted`: TLC exit 13. An 11-state behavior with a genuine
  4-state lasso, states 8 through 11 repeating forever (`Back to state 8`).
  `dreqR1` is present throughout (the antecedent), `doneR1` never becomes
  present. The cycle is `RetentionSweep` (deletes `rwA`) -> `PerformRewrite`
  (recreates `rwA`) -> `SetHeadState` (`headState' = "absent"`) -> (next lap)
  `SetHeadState` again (`headState' = "present"`), so `headState` is
  `"present"` only instantaneously and never stays there. `CompleteErasure`
  requires `HeadDeletable` (`headState = "present"`); `SetHeadState` has no
  fairness in `FairSpec`, so the environment can keep flipping `headState` in
  and out of `"present"` forever, defeating `WF_vars(CompleteErasure)` without
  it ever being continuously enabled. Exact line:
  `Error: Temporal properties were violated.` with the counter-example above
  (`/tmp/liveness-completed.out` this session; not committed, scratch only).

Neither reduced trace matches the orchestrator's `PlaceHold`/`ReleaseHold`
lasso literally, but both are the same failure class: `FairSpec` grants weak
fairness only to the maintainer and store actions
(`SupersededSweep`, `HeadAdvanceRewrite`, `RetentionSweep`, `CompleteErasure`),
never to a `FullEnv`-gated environment action (`PlaceHold`, `ReleaseHold`,
`SetHeadState`, `SetRefresh`), so any one of them cycling forever can
recurrently disable a fairly-scheduled action's guard just before it fires.
Disposition: state both properties under the wider hypothesis that the
environment eventually goes quiet on all three of these (README, "Liveness"
section), rather than adding fairness to any of them. `PlaceHold`/`ReleaseHold`
is a business decision with no code-side liveness guarantee, so fairness there
would claim something the implementation does not promise. `SetHeadState` and
`SetRefresh` model read/refresh flakiness rather than the fold's or the legal
hold subsystem's own forward progress (already covered by
`WF_vars(HeadAdvanceRewrite)`), so adding fairness to them would likewise claim
a retry guarantee this model does not have a cited Rust-side source for; adding
it without one would be exactly the kind of claim ADR-1113 D12 asks this
document not to make.

## Checkpoint finding 1: liveness hypothesis was false as written

The round-two disposition above (restate as "environment eventually quiet on
`PlaceHold`/`ReleaseHold`, `SetHeadState`, `SetRefresh`") was itself checked by
the checkpoint review (issue #1122) and found false: counterexamples exist
where all four of those actions never fire, yet both `EventuallySwept` and
`EventuallyCompleted` still fail, because `Tick`, `ExpireQuery`, and
`PerformRewrite` were themselves unfair.

Diagnosis, done with a scratch copy of `LifecycleGC.tla` (`LifecycleGCQuiet`,
`/tmp/tla-live1/`, not committed) whose `Next` structurally excludes
`PlaceHold`, `ReleaseHold`, `SetHeadState`, and `SetRefresh`, so any remaining
failure cannot be blamed on those four:

1. Baseline (four actions excluded, no new fairness): `EventuallySwept` fails
   at `MaxClock = 2` via a 2-state stutter at `clock = 0` — `Tick` is enabled
   and never taken, confirming the round-two hypothesis is false even under
   literal permanent quiescence of the named actions.
2. Adding `WF_vars(Tick)`, `WF_vars(ExpireQuery)`, `WF_vars(PerformRewrite)`
   (unconditional) fixes `EventuallyCompleted` but `EventuallySwept` now fails
   via a 2-state lasso: `PerformRewrite` (state 11) <-> `RetentionSweep`
   (state 12). `RetentionSweep` ranges over `DataObjects`, so it deletes the
   rewrite output `rwA`; the now-unconditionally-fair `PerformRewrite`
   recreates it and re-stamps the shared `supersededAt' = clock` every time,
   perpetually resetting the horizon countdown for `raw1`, which is otherwise
   already superseded and off HEAD. This is a livelock introduced by the new
   fairness, not present in the unfair baseline.
3. Restricting `PerformRewrite`'s fairness to `WF_vars(PerformRewrite /\
   superseded = {})` (first firing only) closes that livelock:
   `EventuallyCompleted` still passes (exit 0). `EventuallySwept` still fails,
   but now via a stutter at state 9, `supersededAt = 2`, `sysgc.ph = 1`,
   `MaxClock = 2`: `2 + 1 > 2`, so the horizon guard can never clear. This is a
   finite-clock-ceiling artifact, not a fairness gap: `Tick`'s own guard
   (`clock < MaxClock`) disables further clock advancement once `PerformRewrite`
   lands at `clock = MaxClock`, for any finite bound. Confirmed structural by
   re-running the same scratch check at `MaxClock = 4`: TLC exit 13, identical
   pattern (`supersededAt = 4`, last tick).
4. Restating `EventuallySwept`'s antecedent as `<>[](...)` ("eventually
   permanently", not just "eventually") over `supersededAt + sysgc.ph <=
   MaxClock` does not close this alone: TLC still finds a stuttering
   counterexample at state 10/11, this time because a pinned query
   (`query.active`, `query.deadline = 2 = MaxClock`) permanently blocks
   `QueryPermits`, and `ExpireQuery`'s own guard (`clock > query.deadline`) can
   never clear for the same reason as step 3 — the same finite-clock-ceiling
   class, manifesting through a second, independent time-gated guard.
   Confirms this is structural to any bounded `Nat` clock, not specific to the
   supersession horizon.
5. Final form: state `EventuallySwept`'s antecedent as `SupersededSweep(o)`'s
   own enabling guard in full (superseded, present, not held, past horizon,
   query-permitted, HEAD present, no unrecovered refresh failure) holding
   permanently (`<>[]`), rather than enumerating individual timing gates.
   Symmetrically, restate `EventuallyCompleted`'s antecedent as
   `CompleteErasure`'s own guard held permanently. TLC exit 0 for both, on the
   scratch quiescent variant at `MaxClock = 2` (`swept-quiet.cfg`,
   `completed-quiet.cfg`) and, for `EventuallySwept`, at `MaxClock = 4`
   (`swept-quiet-mc4.cfg`; 594029 states generated, 180915 distinct, depth 20,
   `Model checking completed. No error has been found.`).

Shipped fix (`LifecycleGC.tla`): `FairSpec` gains `WF_vars(Tick)`,
`WF_vars(ExpireQuery)`, and `WF_vars(PerformRewrite /\ superseded = {})` on
top of the four already-fair maintainer/store actions; `PlaceHold`,
`ReleaseHold`, `SetHeadState`, `SetRefresh` remain unfair (same rationale as
round two: no code-side progress guarantee to cite). Both `EventuallySwept`
and `EventuallyCompleted` are restated as leads-to from each action's own
enabling guard held permanently, replacing the disproven quiescence
hypothesis.

Re-verified against the real, non-quiescent `LifecycleGC.tla` (all four
environment actions present in `Next`, still unfair) with the same reduced
per-property cfg pattern as round two (`TypeOK` plus one `PROPERTY`, `FullEnv
= TRUE`, `SPECIFICATION FairSpec`, shipped switch values), `MaxClock = 2`:

- `EventuallySwept`: TLC exit 0, `Model checking completed. No error has been
  found.` (464571 states generated, 79358 distinct, depth 19).
- `EventuallyCompleted`: TLC exit 0, `Model checking completed. No error has
  been found.` (464571 states generated, 79358 distinct, depth 19).

`MaxClock = 4` was confirmed for `EventuallySwept` only on the quiescent
diagnostic variant (step 5 above), not re-run against the full non-quiescent
model in this task: the non-quiescent `MaxClock = 4` state space was still
growing past 4.6M states generated / 940K distinct after several minutes when
stopped, which is exhaustive-scale and out of this task's reduced-configuration
budget; README states only the `MaxClock = 2` full-model outcome plus the
`MaxClock = 4` quiescent-variant outcome, nothing stronger.

## Checkpoint finding 2: legal-hold gate widened past raw inputs

The checkpoint review (issue #1122) found `HeldInputServes` scoped only to
`RawInputs`, so a legal hold landing on the rewrite output after its raw
input was swept could not be represented, unlike the shipped Rust gate
(`bucket_is_held` in `erasure_rewrite.rs`, `chain_groups_held_by_legal_hold`
in `sweep.rs`), which blocks on any live protected key in the bucket
regardless of which object it is. Fixed: `HeldInputServes` now ranges over
`DataObjects` (raw inputs and rewrite outputs) and drops the `ServesSubject`
conjunct entirely, matching the shipped gate's content-blind, per-live-key
semantics (`counterexamples/rewrite-output-hold-probe.md` has the full
reasoning for why scope alone is insufficient: `RewriteOutputContent` derives
`rwA`'s served set from the constant `Requests` set, not the `erasureRequested`
state variable, so a correctly computed `rwA` never serves the erased subject
and any `ServesSubject`-gated check on it is always vacuously satisfied).

- Probe (`ProbeNoCompletionUnderBucketHold`, `counterexamples/rewrite-output-hold-probe.md`):
  against the scope-only widening, TLC exit 12, `Error: Invariant
  ProbeNoCompletionUnderBucketHold is violated.` Against the shipped
  content-blind fix, TLC exit 0, `Model checking completed. No error has been
  found.` (464571 states generated, 79358 distinct, depth 19). The probe is
  now unreachable.
- Mutant (`CompleteErasure`'s unconditional gate removed,
  `counterexamples/completion-ignores-legal-hold-mutant.md`, "Re-run after
  finding 2" section): re-applied against the widened, content-blind model.
  TLC exit 12, `Error: Invariant CompletionRespectsLegalHold is violated.`
  7-state trace, `raw1` present and held, `lastGc.heldInputServed = TRUE`
  under `rule = "complete"`. The gate stays load-bearing under the wider
  predicate.
- Smoke re-measured after the fix: distinct states dropped from the prior
  band (80500-81000) to 79358, since the wider gate now prunes reachable
  states it previously let through. `bands.tsv` updated to 79200-79500 (see
  "Smoke" above).
- All seven negative controls re-run and still isolate their named target
  (see "Negative controls" above); no control depended on the narrower
  `HeldInputServes`.

## Exhaustive

`exhaustive.cfg` checks every invariant and both liveness properties against
`FairSpec`, every switch at its shipped value.

`MaxClock = 4` does not complete: a bounded spot-check reported it still
growing monotonically, 2.19M states generated / 550K distinct after about
64s, with no sign of quiescence. The FairSpec liveness pass ranges over both
temporal properties on top of the full safety invariant list, so the time
dimension dominates the graph the same way it did for the reduced
per-property diagnosis above, only worse because nothing is projected away.

Shipped fix: `MaxClock = 3`, one clock tick above smoke's `MaxClock = 2` and
the smallest bound above smoke this task measured. Run directly (not via a
separate orchestrator lane):

- Result: PASS. States generated: 2455254. Distinct: 413443. Diameter
  (depth): 22. Wall: 43s (round four; two runs, both 42-43s, both exactly
  2455254/413443/22, well inside the 3600s executor ceiling).
- Band (`bands.tsv`): distinct within 413300 to 413600, depth 22 to 22.
  Observed 413443/22 is inside; re-measured this round after the three
  fixes below moved the state count and depth up from the prior committed
  band (387000-387500, depth 21), for the same reason as smoke: each fix
  widens reachable behaviour rather than pruning it.
- Coverage given up: `MaxClock = 4`'s extra tick buys one additional
  interleaving depth of environment churn (an extra hold/release or
  head-read flip cycle) beyond what `MaxClock = 3` reaches; it does not
  gate any of the four behaviours below; see the probes.

Reachability of the four behaviours a shrink could silently drop was checked
by probe at `MaxClock = 3` rather than assumed (`counterexamples/reachability-probes.md`
has the full probe module and traces): a rewrite and its supersession, a
legal hold that actually blocks a completion, the protection horizon gating
a delete, and the .dreq hold blocking the request-marker sweep. All four
probes violate (TLC exit 12), so all four remain reachable at the shrunk
bound.

## Round three findings (issue #1122)

A further adversarial review of the pull request raised three findings.
Disposition:

1. (MAJOR) `EventuallySwept` and `EventuallyCompleted` were self-negating:
   `EventuallySwept`'s antecedent required `PresentObj(o)`, but the action
   the leads-to describes, `SupersededSweep`, deletes `o` as its own effect,
   so the antecedent could never hold permanently once the action's other
   guards held; the leads-to was therefore trivially true regardless of
   whether the protocol actually swept anything. `EventuallyCompleted` had
   the same shape with `~PresentObj("doneR1")`, negated by `CompleteErasure`
   writing `.done`. Fixed by dropping exactly the self-negating conjunct from
   each antecedent (`PresentObj(o)` from `EventuallySwept`,
   `~PresentObj("doneR1")` from `EventuallyCompleted`), keeping the `~>`
   connective and every other conjunct (including `<>[]`) unchanged. Both
   properties re-run alone, in the same per-property reduced configuration
   the earlier rounds established (`TypeOK` plus the one `PROPERTY`,
   `FullEnv = TRUE`, `SPECIFICATION FairSpec`, every switch at its shipped
   value, `MaxClock = 2`, run in a scratch cfg not committed):
   - `EventuallySwept`: TLC exit 0, `Model checking completed. No error has
     been found.` (464571 states generated, 79358 distinct, depth 19).
   - `EventuallyCompleted`: TLC exit 0, `Model checking completed. No error
     has been found.` (464571 states generated, 79358 distinct, depth 19).
   Both properties also re-verified together against the full model
   (`exhaustive.cfg`, `FairSpec`, every safety invariant, `MaxClock = 3`):
   PASS, 2357319 states generated, 387264 distinct, depth 21, 48s, matching
   the previously recorded band exactly, so removing the self-negating
   conjuncts changed neither property's outcome nor the reachable state
   count.
2. (MINOR by label, treated as load-bearing) The `.dreq` horizon clause of
   `NoDeleteInsideProtectionWindow` was vacuous: the only clause covering
   `lastGc.rule = "dreq"` was
   `lastGc.rule \in HorizonGatedRules => (lastGc.deleted \cap
   lastGc.permittedNeeds) = {}`, and for the `"dreq"` rule
   `lastGc.deleted = {"dreqR1"}` (a control object) while
   `lastGc.permittedNeeds` is always a subset of `DataObjects`
   (`PermittedNeeds` reads `query.needs`, which `PinQuery` draws only from
   `head \subseteq DataObjects`), so the intersection is empty in every
   state and the clause cannot fail no matter what `DreqSweep` gates on.
   Removing `DreqSweep`'s horizon guard confirmed this: it did not make the
   existing clause fire. Fixed by adding a dedicated clause,
   `lastGc.rule = "dreq" => lastGc.atClock >= dreqHorizon`, stated the same
   way as the existing retention/superseded horizon clauses (reading the
   live `dreqHorizon`, which is frozen the moment `RequestErasure` sets it
   and never changes again, the same per-step-witness reasoning already
   applied to `tombRetiredAt`/`supersededAt`). Proved non-vacuous by
   removing `DreqSweep`'s `clock >= dreqHorizon` conjunct in a scratch copy
   outside the repository (`/tmp`, not committed) and running TLC against
   `smoke.cfg`: `Error: Invariant NoDeleteInsideProtectionWindow is
   violated.` (TLC exit 12; 12671 states generated, 3914 distinct states
   found). The reported trace's final state has `lastGc = [rule |-> "dreq",
   deleted |-> {"dreqR1"}, atClock |-> 1, ...]` with `dreqHorizon = 2`, so
   `1 >= 2` is false and the new clause is what fails; the retention and
   superseded clauses stay vacuously true (wrong rule) and the
   `permittedNeeds` clause stays vacuously true for the reason above,
   confirming the new clause, not an existing one, caught the removed
   guard.
   Audit of the same invariant's other clauses for the same
   control-object/data-object emptiness: the `permittedNeeds` intersection
   clause is meaningful for `"retention"` and `"superseded"` (both rules
   only ever delete `DataObjects`, which `permittedNeeds` also ranges over)
   and vacuous only for `"dreq"`, now covered by the new clause instead.
   `HeldObjectNeverDeleted` reads `lastGc.held`, computed as `\E o \in
   lastGc.deleted : HeldObject(o, heldBuckets)`; for the `"dreq"` rule this
   is always `FALSE` too (`HeldObject` filters to `DataObjects`, and
   `dreqR1` is a control object), but that is not a mistaken check: legal
   holds never cover the `.dreq` prefix (README, "Abstraction boundary"),
   so `.dreq` deletions are correctly exempt from this invariant, and the
   hold-over-`.dreq` case is already covered on purpose by the separate,
   already-proven-non-vacuous `DreqSweepRespectsLegalHold`. No other clause
   in `NoDeleteInsideProtectionWindow`, or elsewhere in the invariant list,
   intersects a control-object-typed field with a data-object-typed one.
3. (TRIVIAL) `TypeOK` did not conjoin the shared store module's own type
   invariant (`S!StoreTypeOK`), so `lastModified`, `uploads`, and
   `listState` were unconstrained, and `lastGc.atClock` had no type
   constraint at all. Fixed: added `S!StoreTypeOK` and
   `lastGc.atClock \in 0..MaxClock` to `TypeOK`. Re-run: `smoke.cfg` PASS,
   464571 states generated, 79358 distinct, depth 19, unchanged from the
   recorded band (invariants do not restrict the transition relation, so
   strengthening `TypeOK` could not and did not change the reachable state
   count; the added conjuncts narrow the states it accepts, they do not
   widen them).

Smoke, negative controls, traceability, and exhaustive were all re-run after
the three fixes above and produced the same figures already recorded in this
document (smoke: 464571/79358/19; exhaustive: 2357319/387264/21; all seven
negative controls VIOLATED as expected; all fifteen traceability rows
resolve); `bands.tsv` is unchanged.

## Round four findings (issue #1122)

A further adversarial code review, comparing the model against the shipped
Rust implementation rather than re-reading the model alone, raised three
Major findings. Disposition:

1. (MAJOR) `RetentionSweep` and `SupersededSweep` gated deletion on
   `HeadDeletable` (`headState = "present"`), blocking any delete whenever
   the modeled HEAD read came back absent. The real gates,
   `reachability.rs::bucket_gate` and `object_gate`, treat an absent HEAD as
   `Covering::Clear` per ADR-0020 (a missing HEAD degrades to a fresh
   listing; it does not block a sweep) and block only on an unreadable HEAD.
   So an absent HEAD, which the code sweeps through, the model refused to
   sweep at all: a real bucket that had already lost its HEAD object would
   sit un-swept in the model long after the code would have collected it,
   understating what the maintainer actually deletes. Fixed by gating both
   sweeps on `HeadReadable` (present or absent, not unreadable) and checking
   bucket/object emptiness against `EffectiveHead`, which reads as empty
   when the HEAD state is absent, rather than the raw `head` variable, so
   the model's notion of "nothing names this object" matches what a real
   reader observes on both a present-and-empty and an absent HEAD.
   Relaxing the gate exposed a second, latent gap: `SetHeadState` let
   `headState` flip to `"absent"` while `head` still named an object, a
   transition no real GET can produce (a GET of an existing key never
   returns absent on this store), and with the sweep gate relaxed this let
   `HeadNamedObjectNeverDeletedBySupersededSweep` be violated by a
   head-unchanged flip from absent to present straight after a delete,
   asserting the same object was reachable, deleted, and reachable again.
   Fixed by requiring `head = {}` whenever `SetHeadState` reports absent.
   Two reachability probes (`counterexamples/absent-head-delete-probes.md`)
   confirm both sweeps still reach a delete under a genuinely absent HEAD
   after the fix, and the existing superseded-sweep-ungated negative control
   still fires against the restated invariant.
2. (MAJOR) `PerformRewrite` was gated only on its own output not existing
   yet, with no requirement that an erasure request exist first and no
   requirement that no rewrite had already superseded the inputs. The model
   could therefore materialize a rewrite output and supersede the raw
   inputs before any erasure request existed at all, an ordering
   `erasure_rewrite.rs` never produces: `pending_erasure_requests` only
   rewrites a bucket that has a `.dreq` and no matching `.done`, and
   `ErasureRewriteOutcome::AlreadyApplied` skips a bucket already rewritten
   for every pending request, so a live implementation never runs the
   rewrite except in response to a request, and never runs it twice for the
   same request. Fixed by adding `PresentObj("dreqR1")` and
   `superseded = {}` as gating conjuncts on `PerformRewrite`. A
   reachability probe (`counterexamples/perform-rewrite-reachable-probe.md`)
   confirms the action still fires, through the same
   `RequestErasure`-then-rewrite ordering the implementation requires.
3. (MAJOR) `physical_sweep` (`crates/ravel-maintain/src/retention.rs`)
   deletes a swept bucket's commit records, compaction records, data
   objects, and L1 parts, then calls `bucket_is_empty_but_tombstone` to
   verify a fresh listing shows only the tombstone remaining, then deletes
   the tombstone itself (through the same `LeaseCheck` instance used for
   every other delete in the function) and reports the bucket swept. The
   model stopped one step early, at the data-object delete: no reachable
   state showed the tombstone itself gone, so the model understated how far
   a real sweep actually clears a retired bucket. Fixed by adding
   `SweepTombstone`, gated the same way the code is: the same
   `HeadReadable`/`EffectiveHead` bucket gate `RetentionSweep` now uses (see
   finding 1 above), the same refresh-failure fail-closed guard the other
   sweeps carry (the code's `LeaseCheck` also guards the tombstone delete,
   so a failed refresh blocks it too), and the bucket holding nothing but
   the tombstone. Added `TombstoneNotDeletedBeforeBucketEmpty`, split out
   from the existing `TombstoneExcludesBeforeDelete` rather than folded
   into it, so the two claims (tombstone exists before any data delete;
   tombstone outlives every data delete) stay independently falsifiable. A
   reachability probe (`counterexamples/sweep-tombstone-reachable-probe.md`,
   trace `RetireBucket -> DropRetiredBucketFromHead -> Tick ->
   RetentionSweep -> SweepTombstone`) confirms the new action fires. A
   non-vacuity mutant that drops the bucket-empty precondition
   (`counterexamples/sweep-tombstone-empty-bucket-mutant.md`) shows the new
   invariant catching a tombstone deleted while a raw input in the same
   bucket is still present. Wiring the new action in first surfaced that it
   needed the refresh-failure guard fixed 3 lacked initially: an early
   smoke run without it broke `RefreshFailureNeverSweeps`, confirming the
   tombstone delete is subject to the same fail-closed semantics as every
   other sweep, not a special case.

All three findings widen the reachable state graph rather than pruning it
(an absent HEAD now permits deletes it used to block; `SweepTombstone` adds
a wholly new reachable state per swept bucket), so smoke and exhaustive
figures both moved up and `bands.tsv` is re-measured against this round's
model, not carried forward:

- Smoke: 464571/79358/19 (prior committed band) to 465912 states generated,
  82284 distinct, depth 21 (new band 82150-82420, depth 21-21).
- Exhaustive: 2357319/387264/21 (prior committed band) to 2455254 states
  generated, 413443 distinct, depth 22, 43s wall (new band 413300-413600,
  depth 22-22), run twice with identical figures both times, well inside
  the 3600s executor ceiling.

Smoke, negative controls, traceability, and exhaustive were all re-run after
the three fixes above against the updated `bands.tsv`: smoke PASS
(465912/82284/21, inside band), exhaustive PASS (2455254/413443/22, inside
band, 42-43s both runs), all seven negative controls VIOLATED as expected,
all sixteen traceability rows resolve, `check_docs.py` clean.

## Round five findings (issue #1122)

A further adversarial code review, again checked against the shipped Rust
before touching the model, raised one Major and one Minor finding.
Disposition:

1. (MAJOR) `PerformRewrite` was still incomplete in two ways beyond round
   four's gate. First, completion did not close the rewrite: `CompleteErasure`
   writes `doneR1` but the model never checked for it, so `RetireBucket`
   followed by `DropRetiredBucketFromHead` can make `CompleteErasure` fire
   while `superseded` is still `{}`, after which the old three-conjunct gate
   still let `PerformRewrite` fire against an erasure that had already
   completed. `pending_erasure_requests`
   (`crates/ravel-maintain/src/erasure_rewrite.rs`) never lets this happen in
   the shipped code: it filters out any `.dreq` whose `request_id` has a
   matching `.done`, so a completed request is never seen as pending again.
   Second, the rewrite did not check that what it rewrites still exists:
   `RewriteOutputContent` derived from `InitContent`, a static function of
   initial content, so `Tick` followed by `RetentionSweep(raw1)` could delete
   a predecessor while `dreqR1` was present and `superseded = {}`, and the
   model would still derive `rwA`'s content from the now-deleted `raw1`.
   `erasure_rewrite_bucket` never does this either: `resolve_live_inputs`
   does a fresh `list_bucket` read at rewrite time and derives the output
   only from inputs still present. Fixed by adding `~PresentObj("doneR1")`
   and `\A i \in Predecessors("rwA") : PresentObj(i)` as gating conjuncts on
   `PerformRewrite`, and changing `RewriteOutputContent` to read `objContent`
   (the current-state variable) instead of `InitContent`. Three probes
   (`counterexamples/perform-rewrite-completion-guard-probe.md`,
   `perform-rewrite-predecessor-presence-probe.md`,
   `perform-rewrite-still-reachable-after-guards-probe.md`) show each new
   guard closing its reachable bad state (TLC exit 12 without the guard,
   exit 0 with it) and confirm `PerformRewrite` still fires at all
   afterward (TLC exit 12 on the positive reachability probe).

   One related gap noted but out of scope for this fix: `erasure_rewrite_bucket`
   returns `ErasureRewriteOutcome::Tombstoned` and refuses to rewrite a
   tombstoned bucket at all; the model has no equivalent gate on
   `PerformRewrite`. The two new guards above do not stand in for it:
   `RetireBucket` (writing `tombB1`) has no dependency on the raw input's
   presence or on any erasure request, so `tombB1` can be present while
   `raw1` is still present, `dreqR1` is present, `doneR1` is absent, and
   `superseded = {}` -- a state where the fixed `PerformRewrite` still
   fires despite the bucket already being tombstoned in the shipped code's
   sense. This is a genuine, currently unaddressed gap between the model
   and the code, reported here rather than silently folded into this
   fix since it is a distinct question from either finding this round.

2. (MINOR) The traceability row for `SweepTombstone` /
   `TombstoneNotDeletedBeforeBucketEmpty` claimed the model's guarantee
   covers the bucket's data objects, compaction records, and commit
   records. The model only inspects `DataObjects` (raw inputs and the
   rewrite output); `bucket_is_empty_but_tombstone`
   (`crates/ravel-maintain/src/retention.rs`) additionally lists a
   `commit_prefix` (covering commit and compaction records) and a separate
   `l1_prefix` (L1 parts), neither of which the model or its invariant
   checks. Narrowed the row to name only `DataObjects` and call out the
   three object classes outside the model's scope, rather than widen the
   model for a Minor traceability gap.

Both `PerformRewrite` guards narrow reachable behavior (a previously
reachable completed-then-rewritten and swept-then-rewritten ordering are
now excluded), so smoke and exhaustive state counts both fall and
`bands.tsv` is re-measured against this round's model:

- Smoke: 465912/82284/21 (prior committed band) to 393481 states generated,
  71018 distinct, depth 21 (new band 70950-71090, depth 21-21).
- Exhaustive: 2455254/413443/22 (prior committed band) to 1947121 states
  generated, 334207 distinct, depth 22, 67-77s wall (new band
  334100-334300, depth 22-22), run twice with identical figures both
  times, well inside the 3600s executor ceiling.

The drop is 13718 distinct states at smoke (13.7%) and 79236 at exhaustive
(19.2%). Both guards each close an entire family of continuations following
one bug condition (every clock tick and interleaving reachable after an
early `CompleteErasure` for guard one, after an early `RetentionSweep(raw1)`
for guard two), so a double-digit-percentage drop in this small bounded
instance is the expected size for closing two such orderings, not a sign
either guard excludes more than intended.

Smoke, negative controls, traceability, and exhaustive were all re-run after
both fixes above against the updated `bands.tsv`: smoke PASS
(393481/71018/21, inside band), exhaustive PASS (1947121/334207/22, inside
band, 67-77s across two runs), all seven negative controls VIOLATED as
expected, all sixteen traceability rows resolve, `check_docs.py` clean.

## Round six findings (issue #1122)

Round five reported one gap as out of its scope and needing its own
decision: `erasure_rewrite_bucket` (`crates/ravel-maintain/src/erasure_rewrite.rs`)
does a fresh `list_bucket` read at the top of the function and returns
`ErasureRewriteOutcome::Tombstoned` whenever `listing.tombstone_key.is_some()`,
refusing to rewrite a tombstoned bucket at all. `PerformRewrite`'s guard set
had no equivalent: `RetireBucket` (writing `tombB1`) has no dependency on
`dreqR1`, `doneR1`, or `superseded`, so `tombB1` can be present while every
one of `PerformRewrite`'s existing conjuncts still passes, and the model
could rewrite a bucket the shipped code had already refused.

Decision: model it. Fixed by adding `~PresentObj("tombB1")` as a fifth
gating conjunct on `PerformRewrite`, testing the same object the shipped
code tests (the tombstone key's presence in a fresh listing), not a proxy
for it.

Proved both ways with three probes (`counterexamples/perform-rewrite-tombstone-guard-probe.md`
has the full probe module and traces):

- Without the new guard, `ProbeNoRewriteOverTombstonedBucket`
  (`PresentObj("tombB1") => ~(ENABLED PerformRewrite)`) is VIOLATED: TLC exit
  12, 324 states generated, 169 distinct, at a state with `tombB1` present
  and `raw1`, `dreqR1` present, `doneR1` absent, `superseded = {}`.
- With the new guard, the same invariant HOLDS: TLC exit 0, "Model checking
  completed. No error has been found.", 276015 states generated, 50102
  distinct, depth 21.
- With the new guard, `ProbeReachesPerformRewrite`
  (`~(PresentObj("rwA") /\ RawInputs \subseteq superseded)`) is still
  VIOLATED (reachable): TLC exit 12, 203 states generated, 113 distinct, via
  the ordinary `RequestErasure` then `PerformRewrite` path with no
  tombstone ever written -- the guard excludes the tombstoned state without
  killing the action.

The guard narrows reachable behavior (a previously reachable
tombstoned-then-rewritten ordering is now excluded), so smoke and
exhaustive state counts both fall and `bands.tsv` is re-measured against
this round's model:

- Smoke: 393481/71018/21 (prior committed band 70950-71090, depth 21) to
  276015 states generated, 50102 distinct, depth 21 (new band
  50040-50160, depth 21-21).
- Exhaustive: 1947121/334207/22 (prior committed band 334100-334300, depth
  22) to 1340669 states generated, 230815 distinct, depth 22, 62s per run
  (new band 230750-230900, depth 22-22), run twice with identical figures
  both times, well inside the 3600s executor ceiling.

The drop is 20916 distinct states at smoke (29.5%) and 103392 at
exhaustive (30.9%). The guard closes an entire family of continuations
following one condition (`RetireBucket` firing, then every clock tick and
interleaving that used to still let `PerformRewrite` fire), so a
double-digit-percentage drop in this small bounded instance is the
expected size for closing that one ordering, not a sign the guard excludes
more than intended.

Traceability gained one row: `PerformRewrite` (tombstone guard) cites
`crates/ravel-maintain/src/erasure_rewrite.rs::erasure_rewrite_bucket::ErasureRewriteOutcome::Tombstoned`,
with a noted gap that no production test isolates the tombstoned-bucket
branch of `erasure_rewrite_bucket` from its other refusal outcomes.

Smoke, negative controls, traceability, and exhaustive were all re-run
after this fix against the updated `bands.tsv`: smoke PASS
(276015/50102/21, inside band), exhaustive PASS (1340669/230815/22, inside
band, 62s across two runs), all seven negative controls VIOLATED as
expected, all seventeen traceability rows resolve, `check_docs.py` clean.

No new CodeRabbit findings on pull request 1206 were folded in: the `gh`
CLI is unavailable in this executor's environment, so the pull request
could not be checked. This should be verified separately before merge.
