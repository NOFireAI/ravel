# Negative control: dreq-ignores-held-inputs

Switch: `DreqIgnoresHeldInputs = TRUE` (drops the `~HeldInputServes("s1")`
clause from the `.dreq` sweep, so the `.dreq` can be swept out from under a
legally held, present raw input that still serves the erased subject). All
other switches at base.

Target invariant: `DreqSweepRespectsLegalHold` (finding 1: the switch's
own name says "held inputs," so it must mutate the held-input clause and
fire the invariant that names that clause, not the unrelated
`DreqRemovalCannotResurrect`, which covers only current-HEAD and pinned-query
reachability and never claimed anything about a held-but-unreachable input).
TLC exit 12.

```
Error: Invariant DreqSweepRespectsLegalHold is violated.
```

Trace: `RequestErasure` writes the `.dreq` and marks `s1` erased; `Tick`;
`PerformRewrite`; `HeadAdvanceRewrite` switches HEAD to the rewrite output
(so `raw1` is now superseded, off HEAD, and no longer reachable through any
live or pinned read); `CompleteErasure` writes `.done` (legitimate: no hold
exists yet); `PlaceHold` places a legal hold on `b1`, which still holds
`raw1`, still present and still serving `s1`; `DreqSweep` deletes `dreqR1`
anyway, because the switch drops its held-input clause while `~ServesAny`
alone stays satisfied (superseded `raw1` is unreachable through HEAD or a
pinned query). `GcWitness("dreq", ...)` records
`HeldInputServes("s1") = TRUE` under `rule = "dreq"` in that same
transition, and `DreqSweepRespectsLegalHold` reads that witness and fails.
Restoring the switch's clause (`DreqIgnoresHeldInputs = FALSE`, the shipped
value) makes the invariant hold again.
