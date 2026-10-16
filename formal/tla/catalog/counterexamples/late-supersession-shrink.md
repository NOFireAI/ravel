# late-supersession-shrink (recorded shrink)

`LateSupersessionEventuallyReflected` is defined in `CatalogMVCC.tla` but is not
checked by the smoke, negative, or exhaustive harness lanes. TLC finds a real
stuttering counter-example under `FairSpec`, and this note records the shrunk
trace and why the property cannot hold in a bounded model rather than forcing it
green.

## Reproducing

The paired `late-supersession-shrink.cfg` in this directory checks the property
under `FairSpec` at the bounds cited below. It is not run by any harness lane
(the harness matches invariant violations, and a liveness failure prints
`Temporal properties were violated` instead); run it directly to reproduce.
`MCCatalogMVCC.tla` lives in the parent directory and `RavelObjectStore.tla`
in `../common`, so run from `formal/tla/catalog` with the config path and
library path both given explicitly:

```
cd formal/tla/catalog
java -DTLA-Library=../common:. -cp <tla2tools.jar> tlc2.TLC \
  -config counterexamples/late-supersession-shrink.cfg -workers auto \
  -deadlock MCCatalogMVCC
```

TLC exits 13 with `Temporal properties were violated` and prints the
stuttering trace whose shape is described below.

## The property

A used compaction record at hour `H`, once published, should eventually be
reflected in a valid HEAD: its L1 part enters the snapshot and its superseded L0
inputs leave it.

## The shrunk counter-example

Constants as `late-supersession-shrink.cfg` (`Hours = {0, 1}`, `MaxClock = 4`,
`FoldSealDelay = 1`, `MaintSealDelay = 0`, `ProtectionHorizon = 1`). A run of
the reproduction command above:

- States 2 and 4: `DoCommit` ingests `rA` into hour 0's L0 set, then into hour
  1's.
- States 3, 5, and 6: `DoTick` advances the clock to 3.
- State 7: `DoFoldStart` opens a fold.
- State 8: `DoCompact` publishes a compaction record over hour 1: `crec[1][g1]
  = [used |-> TRUE, in |-> {rA}, out |-> {rA}, at |-> 3]`. Hour 1 is already
  fold-sealed and already folded, so HEAD still names the L0 input `l0/1/rA`,
  not the new L1 part.
- States 9 to 11: `DoQueryResolve`, `DoQueryRun`, and `DoFoldPutPart` interleave
  (the query and fold actors are free to take these steps; they do not affect
  the shape below).
- State 12: `DoFoldCas` CAS-swaps a valid HEAD at watermark 1: `head = [wm |->
  1, entries |-> {l0/0/rA, l0/1/rA}, status |-> "valid"]`.
- State 13: `DoTick` advances the clock to `MaxClock = 4`.
- State 14: stutter. The watermark is already 1, the maximum hour, so no future
  fold can advance it, and reconcile runs only on a watermark-advancing fold
  (F16/F17). The compaction published at State 8 is never re-reconciled into
  HEAD, so the leads-to target never becomes true.

Recorded run: 2,427,940 states generated, 947,275 distinct states found, depth
14, exit 13.

## Why this is a finite-model limitation, not a defect

Reflection of a late compaction depends on a subsequent fold whose watermark
advances past the compaction's hour and whose reconcile window covers it
(`crates/ravel-catalog/src/fold.rs::reconcile_applies_late_compaction_before_horizon`).
Real deployments always get that fold: wall-clock time advances without bound,
later hours seal, and each watermark-advancing fold re-reconciles the recent
window. A bounded model clock cannot supply an unbounded sequence of watermark
advances: `Hours` is finite, the watermark saturates at its maximum, and a
compaction published after the last advance has no later fold to catch it.

Broadening `DoFoldStart` to reconcile without a watermark advance would make the
property pass, but it would be unfaithful to the design and would falsify
`ReconcileOnlyOnWatermarkAdvance`, which the code (and the `reconcile-on-tick`
negative control) shows is the real contract. The correct reading is that
eventual reflection is a liveness guarantee conditioned on continued watermark
progress, which is exactly the assumption a finite clock removes.

## The stale window is safe

While HEAD is stale (names an L0 input a compaction has superseded), a reader
that sees both the L0 input and its L1 replacement still gets exact answers:
metrics dedup serves each identity at most once, pinned by `SignalDedupContract`
and its `metrics-dedup-dropped` negative control. So the unreflected window is a
liveness gap, never a correctness one. `QueryTerminates` is checked in its place
as a liveness property that holds under the bounded clock.
