# late-record-left-out-nonvacuity

Non-vacuity probe, not a mutation. `negative/late-record-left-out-nonvacuity.cfg`
checks `NoLateRecordLeftOut` (no compaction ever publishes while its hour's
listing holds a late record the plan left out) on the correct protocol, at
the bounds of `positive/plan-leaves-late-record-out.cfg`. The probe MUST be
violated: it proves the positive pass checks `PlanLeavesLateRecordsOut` in a
state where the fence does work. `smoke.cfg` (one record) and
`exhaustive.cfg` (two records, `MaxOps = 2`) cannot reach that state: a
compaction beside a late record needs two commits and the compaction, three
publishes.

Violated invariant: `NoLateRecordLeftOut` (probe, TLC exit 12).

## Trace shape (from the recorded run)

1. State 2: `DoCommit` lands `rA` in hour 0 at clock 0, on time.
2. State 3: `DoTick` advances the clock to 1, hour 0's maintenance seal.
3. State 4: `DoCommit` lands `rB` in hour 0. The hour is maintenance-sealed,
   so `late[0] = {rB}`.
4. State 5: `DoCompact` plans hour 0 from `PlanInputs(0) = {rA}` and
   publishes `crec[0][g1].in = {rA}` with the witness `leftOut = {rB}`.

At State 5 `LateRecordLeftOutExercised` holds, so `NoLateRecordLeftOut` is
false.

Recorded run (one hour, `Hours = {0}`): 148 states generated, 109 distinct
states found, depth 5 of search, exit 12.
