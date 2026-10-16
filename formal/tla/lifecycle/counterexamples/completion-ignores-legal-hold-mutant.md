# Non-vacuity mutant: CompletionRespectsLegalHold

Proves `CompleteErasure`'s legal-hold gate (finding 1) is load-bearing: without
it, `CompleteErasure` can write `.done` while a held, present raw input still
serves the erased subject, which `CompletionRespectsLegalHold` then catches.

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. The
unconditional gate is deleted from `CompleteErasure`:

```tla
     /\ (CompleteIgnoresServedSet \/ ~ServesNow("s1"))
-    /\ ~HeldInputServes("s1")
     /\ clock > 0
```

Nothing else changed; the run used the base `smoke.cfg` with every switch at
its shipped value (`DreqIgnoresHeldInputs = FALSE` included, since this gate
has no switch of its own in either the model or the code).

## Result

TLC exit 12. Exact line:

```text
Error: Invariant CompletionRespectsLegalHold is violated.
```

Eight states: `Init`, `Tick`, `Tick`, `PlaceHold`, `PerformRewrite`,
`RequestErasure`, `HeadAdvanceRewrite`, `CompleteErasure`. The hold lands on
`b1` before the erasure request and the rewrite that supersedes `raw1`, so at
the final step `raw1` is present, held, and still serves `s1`; with the gate
removed, `CompleteErasure` writes `doneR1` anyway. `CompletionWitness`
records `HeldInputServes("s1") = TRUE` under `rule = "complete"` in the same
transition, and `CompletionRespectsLegalHold` reads that witness and fails.
Restoring the deleted conjunct makes the invariant hold again (smoke passes),
which is the non-vacuity argument for the gate.

## Re-run after finding 2 (widened, content-blind `HeldInputServes`)

Re-applied the same one-line removal to the post-finding-2 model (`DataObjects`,
no `ServesSubject` conjunct; see `rewrite-output-hold-probe.md`) against the
same base `smoke.cfg`. TLC exit 12. Exact line:

```text
Error: Invariant CompletionRespectsLegalHold is violated.
```

7-state trace: `Init`, `Tick`, `PlaceHold`, `RequestErasure`, `PerformRewrite`,
`HeadAdvanceRewrite`, `CompleteErasure` (8549 states generated, 2751 distinct
states found). Final state: `raw1` present and held, `heldBuckets = {"b1"}`,
`lastGc.heldInputServed = TRUE` under `rule = "complete"`, same failure as
before the widening. The gate stays load-bearing under the wider predicate.
