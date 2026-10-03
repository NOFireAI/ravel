# overlap-nonvacuity

Non-vacuity probe, not a mutation. `negative/overlap-nonvacuity.cfg` checks
`NoOverlappingL1Output` (no two published L1 compaction records ever name
overlapping `out` sets) over three-record, two-hour bounds where the same
record identity can be committed into two different hours and separately
compacted in each. The probe MUST be violated: it proves the overlapping-output
case issue #1121 round six's review finding describes is genuinely reachable,
not a case `smoke.cfg` (one record) or `exhaustive.cfg` (two records,
`MaxOps = 2`) can ever reach.

Violated invariant: `NoOverlappingL1Output` (probe, TLC exit 12).

## Trace shape (from the recorded run)

1. Commit `rB` into hour 0, then commit `rA` into hour 0. Hour 0's L0 now holds
   `{rA, rB}`.
2. Commit `rB` into hour 1, then commit `rC` into hour 1. Hour 1's L0 holds
   `{rB, rC}`. `DoCommit` only checks the current hour's L0 set for a
   duplicate identity, so committing `rB` a second time into a different hour
   is unconstrained.
3. Tick the clock to satisfy `FoldSealDelay`.
4. Compact hour 0 with `g1`: `crec[0][g1].out = {rA, rB}`.
5. Compact hour 1 with `g1`: `crec[1][g1].out = {rB, rC}`. Both compaction
   records are published and immutable, and their `out` sets overlap on `rB`.

## Before: violated

Against the pre-fix whole-entry `Dedup` (`DedupSurvivors` kept or dropped an
entire L1 entry per identity, never a single contribution), a query that pins
both L1 entries above serves `rB` twice: once as part of `<<"l1", 0, g1>>`,
once as part of `<<"l1", 1, g1>>`. Each identity's most-constrained-first
replacement independently sees a lone survivor among the two entries it is
served by, so neither entry is dropped.

`Error: Invariant SignalDedupContract is violated.` 94884 states generated,
45619 distinct states found, 31712 states left on queue, depth 10, TLC
exit 12.

## After: holds

The fixed `Dedup` resolves survivors as `(entry, identity)` contributions
(`ContribSurvivors`) rather than whole entries, so the pass can drop `rB`'s
contribution from one of the two overlapping entries while keeping the entry
itself, and the other identities it still serves, intact. Exhaustively
re-exploring `overlap.cfg` against the fixed `Dedup` did not terminate in
reasonable time (23,990,591 states generated within about five minutes and
still climbing, matching the same intractability `dedup-starvation-fixed.md`
records for `DedupPreservesCoverage`), so the fixed behavior was checked by
random simulation over the same config instead: 3000 traces to depth 14,
42046 states checked, no violation, TLC exit 0, finished in 05s. A second run
added `-coverage 1` to confirm `DoCommit` (14035 evaluations) and `DoCompact`
(2621 evaluations) both fire at the volumes the trace above needs; it
completed within its 90-second budget with no violation recorded.

## Why it is here

Without this fix, the same record identity committed into two different
hours and compacted separately in each produced two published L1 outputs
that overlapped on that identity, and the whole-entry dedup pass kept both,
serving the identity twice. `DoCommit` only checks the current hour's L0
set, so nothing in the model, or the runtime it mirrors, prevented the same
identity from being committed into more than one hour. See finding 7 in
`results.md`.
