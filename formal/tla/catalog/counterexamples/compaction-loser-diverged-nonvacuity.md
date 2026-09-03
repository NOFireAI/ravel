# compaction-loser-diverged-nonvacuity

Non-vacuity probe, not a mutation. `negative/compaction-loser-diverged-nonvacuity.cfg`
checks `NoCompactionLoserDivergence` (a losing compaction publish never takes
the "diverged" branch of the widened `lastCompact.outcome` alphabet) over
bounds where a commit can land between a winner's publish and a losing
retry. The probe MUST be violated: it proves the STORE-derived
`"converged"`/`"diverged"` split added to `DoCompactLoser` for finding 6 is
exercised, not a value the model can define but never reach.

Violated invariant: `NoCompactionLoserDivergence` (probe, TLC exit 12).

## Trace shape (from the recorded run)

1. An L0 input (`rA`) is committed into hour 0.
2. A compactor publishes `crec[0][g1]` with `in = {rA}` (`DoCompact`), at the
   maintenance-seal tick, within `LagBound`.
3. At the same clock tick, before hour 0 fold-seals (`FoldSealDelay = 1`
   keeps the commit window open one tick past the maintenance seal), a
   second L0 input (`rB`) is committed into hour 0, so `l0[0] = {rA, rB}`.
4. `DoCompactLoser` fires on `(0, g1)`: `crec[0][g1].used` is already true,
   so it reads the winner's record back. Its own view, `l0[0] = {rA, rB}`,
   differs from the winner's stored `in = {rA}`, so
   `lastCompact.outcome = "diverged"` and `NoCompactionLoserDivergence` is
   false. `crec[0][g1]` is untouched (`CompactionRecordImmutable` still
   holds): divergence is reported by the outcome witness, not resolved by
   overwriting.

Recorded run: state 6 / depth 6, 1063 states generated, 595 distinct states
found, exit 12.

## Why it is here

At `smoke.cfg`'s `Records = {rA}` bound there is only one record identity, so
a second commit can never land in the same hour after the first (`DoCommit`
requires the record not already in `l0[H]`) and the "diverged" branch is
structurally unreachable. Without this probe, at the two-record bounds where
divergence is reachable, `lastCompact.outcome = "diverged"` could sit defined
but dead, and the finding-6 claim that the model's outcome alphabet matches
`resolve_already_exists`'s real behavior (converged, diverged, or the
switch-only overwrite bug) would rest on an alphabet member no run ever hits.
See finding 6 in `results.md`.
