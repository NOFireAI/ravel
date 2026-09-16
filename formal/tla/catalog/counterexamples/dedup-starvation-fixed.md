# dedup-starvation-fixed

Historical bug demonstration, not a permanent negative control. Issue #1121
finding 1: the prior `Dedup(P)` picked each identity's survivor independently
against the pre-dedup set (`CHOOSE e \in Sources(r, P) : TRUE`), then removed
the union of every identity's rejected entries. Two identities that shared one
L1 source but disagreed on which entry to keep could each independently
choose a different survivor, and the shared source then fell out of both
rejection sets' complement and was removed outright, leaving the identity
whose only source that was with none.

`DedupPreservesCoverage` (`CatalogMVCC.tla`) is the invariant this bug
violates: `\A r \in Records : Sources(r, FallbackView) # {} => Sources(r,
Dedup(FallbackView)) # {}`. It is store-derived (recomputes `Dedup` fresh
against `FallbackView` at every reachable state) rather than backed by a
mutation switch, so there is no permanent `negative/*.cfg` counterpart the
way the other invariants have; the bug it catches is a defect in `Dedup`
itself, not a behaviour a switch can re-enable. This file records the
before/after demonstration instead, against a scratch bounds probe (not
committed) reusing `negative/metrics-dedup-dropped.cfg`'s bounds: `Hours =
{0, 1}`, `Records = {rA, rB}`, `CompIds = {g1}`, `MaxClock = 4`, `MaxOps = 4`,
`DedupBySignal = TRUE`.

## Before: violated

Trace (breadth-first, exhaustive at these bounds):

1. Commit `rB` into hour 0.
2. Commit `rB` into hour 1.
3. Commit `rA` into hour 0. Hour 0's L0 now holds `{rA, rB}`.
4. Tick the clock to 1 (satisfies `FoldSealDelay`).
5. Compact hour 0 with `g1`: `crec[0][g1] = [used |-> TRUE, in |-> {rA, rB},
   out |-> {rA, rB}, at |-> 1]`. `rA`'s only source is now this compaction
   record; `rB` has two sources (the same record, and its own uncompacted L0
   entry in hour 1).

The prior `Dedup` chose each identity's survivor independently: if `rB`'s
independent choice picked its own hour-1 L0 entry over the shared compaction
record, the record fell out of the removal union's complement for `rA` too,
and `rA` lost its only source.

`Error: Invariant DedupPreservesCoverage is violated.` State 6, depth 6,
827 states generated, 463 distinct states found, TLC exit 12.

## After: holds

The same scratch probe fully explored (breadth-first, buggy `Dedup`, only
`TypeOK` checked so TLC does not stop at the first violation) generates tens
of millions of states within minutes and does not finish in reasonable time
at these bounds; the 827-state "before" count above is TLC halting at the
first violation, not the size of the reachable space. Exhaustively re-running
the fixed `Dedup` to completion at these same bounds is impractical for the
same reason, so the fixed `Dedup` was instead checked by random simulation
over the identical config: 3000 traces to depth 12, 292087 states checked,
no violation, TLC exit 0, "Finished in 07s". A second run added `-coverage 1`
to confirm `DoCommit`, `DoTick`, and `DoCompact` all fire at the volumes the
trace above needs; the coverage instrumentation's overhead kept it from
finishing in the 90s allotted (killed, exit 124), but its partial output
before the kill already showed all three actions firing with no violation
recorded, consistent with the completed first run.

`smoke.cfg` and `exhaustive.cfg` both carry `DedupPreservesCoverage` as a
permanent `INVARIANT` now (see `results.md`); neither config's bounds reach
this two-source conflict (`exhaustive.cfg`'s `MaxOps = 2` is below the four
operations the trace above needs), so both reproduce their pre-fix distinct
state counts and depths exactly, and this invariant did not change either
gate's baseline. It is checked over their entire reachable graphs regardless.

## Why it is here

Without this fix, a shared L1 compaction output could be dropped from a
resolved view even though it was the only source an identity had, silently
losing that identity from a query result. The independent-`CHOOSE` design
had no mechanism to notice that two identities' choices interacted; the
sequential most-constrained-first replacement (`DedupSurvivors`) protects a
forced survivor before a flexible identity can pick around it. See finding 1
in `results.md`.
