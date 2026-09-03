# Lifecycle GC model-checking results

Toolchain: tla2tools 1.7.4, TLC2 version 2.19 (rev 5a47802), OpenJDK 21. All
runs below are the executor's; the exhaustive configuration was not run by the
executor (see the last section). TLC checked this finite model under the bounds
and assumptions in each `.cfg`. This model verifies the protocol design;
implementation conformance is argued in `traceability.md` and asserted by the
named Rust tests, not proved.

## Smoke

`smoke.cfg` against `Spec`, all thirteen invariants, `VIEW View`, `MaxClock = 2`,
`FullEnv = TRUE`, every switch at its shipped value.

- Result: PASS.
- States generated: 473807. Distinct: 80714. Diameter (depth): 19. Wall: 3 s.
- Band (`bands.tsv`): distinct within 80500 to 81000. Observed 80714 is inside.
- The distinct-state count is above the prior round's committed band
  (78000-79000, measured before this round's finding 3 and finding 1 model
  changes: `Subjects`/`AllRecords` gained a second member, and `lastGc`
  gained the `heldInputServed` field). `bands.tsv` is re-measured against
  this round's final model and updated to 80500-81000; the depth (19) is
  unchanged.

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

Six invariants are shown breakable by mutating the behaviour (not a switch) in
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

Details under `counterexamples/held-object-mutant.md`,
`counterexamples/tombstone-mutant.md`, `counterexamples/erased-subject-mutant.md`,
`counterexamples/rewrite-output-surviving-record-mutant.md`,
`counterexamples/completion-ignores-legal-hold-mutant.md`,
`counterexamples/dreq-removal-pinned-reader-mutant.md`. Together with the
seven negative controls (one of which, `rewrite-keeps-erased-records`, targets
an invariant already covered by a mutant above), all twelve named safety
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
  `EventuallyCompleted` are conditional on fairness and on the fold's and the
  sweep's retention windows agreeing; when they disagree the sweep waits on the
  fold forever and the properties are intentionally false. Whether they hold
  under `FairSpec` is checked only in `exhaustive.cfg`, which this task did not
  run, so this document makes no claim here that they hold.

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
   retention-window disagreement, #1131). See the "Liveness" section below
   for the reduced per-property diagnosis; the exhaustive configuration
   itself was not run here (out of scope for this task; the orchestrator
   measures it).
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

## Exhaustive

`exhaustive.cfg` checks every invariant and both liveness properties against
`FairSpec` over `MaxClock = 4`. Not run by the executor; see the orchestrator's
run. `bands.tsv` carries no exhaustive row because the executor did not measure
one; the orchestrator adds it after the run. The FairSpec liveness pass makes it
heavier than smoke, so it is left to the orchestrator's longer run.
