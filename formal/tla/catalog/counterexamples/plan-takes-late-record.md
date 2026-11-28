# plan-takes-late-record

Negative control. Switch `PlanTakesLateRecord = TRUE` makes a compaction plan
every commit record of its hour, late ones included, instead of the on-time
set alone. This breaks ADR-4561 R4
(`crates/ravel-maintain/src/read.rs::BucketListing::planning_commit_keys`): a
plan takes only the records the store stamped before the bucket's late-commit
cutoff, and leaves every later one where it is, still served.

Violated invariant: `PlanLeavesLateRecordsOut` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. State 2: `DoTick` advances the clock to 1, the maintenance seal of hour 0,
   which is not yet fold-sealed (`FoldSealDelay = 1`).
2. State 3: `DoCommit` lands `rA` in hour 0. The hour is maintenance-sealed,
   so the commit is late: `late[0] = {rA}`.
3. State 4: `DoCompact` plans hour 0. With the switch set, `PlanInputs(0)` is
   `l0[0] = {rA}`, not `l0[0] \ late[0] = {}`, and the compaction publishes
   `crec[0][g1].in = {rA}`.

At State 4 `crec[0][g1].in \cap late[0] = {rA}`, so `PlanLeavesLateRecordsOut`
is false: a published compaction record names a late commit record. With the
switch off, `DoCompact` is not enabled at State 3, because an hour whose
records are all late has nothing to plan.

Recorded run (one hour, `Hours = {0}`): 80 states generated, 60 distinct
states found, depth 4 of search, exit 12.

## Why it is the right control

The crate's planning passes take `planning_commit_keys`, the listed commit
records less the ones `classify_late` marked late
(`crates/ravel-maintain/src/read.rs::BucketListing::classify_late`), pinned by
`crates/ravel-maintain/tests/late_commit_fence.rs::a_commit_record_stamped_at_the_cutoff_is_left_out_of_every_plan`.
The control drops exactly that exclusion and leaves the late flag, which
`DoCommit` sets from the clock, untouched, so the invariant reads the store's
stamps, not the switch. It is the model side of the Antithesis mutant
`catalog/plan-takes-late-record` (ADR-4326, the late-fence mutant amendment),
whose `inv_mutant!` site is the first line of `planning_commit_keys`.
