# Negative control: dreq-ignores-held-inputs

Switch: `DreqIgnoresHeldInputs = TRUE` (drops the `NoPinnedReaderServes` clause
from the `.dreq` sweep, so the `.dreq` can be swept out from under a permitted
pinned query that still reaches an object serving the erased subject). All other
switches at base.

Target invariant: `DreqRemovalCannotResurrect`. TLC exit 12.

```
Error: Invariant DreqRemovalCannotResurrect is violated.
```

Trace: a query pins on the current HEAD; `RequestErasure` writes the `.dreq` and
marks `s1` erased; `Tick`; `PerformRewrite`; `HeadAdvanceRewrite` switches HEAD
to the rewrite output; `CompleteErasure` writes `.done`; `DreqSweep` deletes the
`.dreq` while the pinned query can still reach the pre-rewrite input serving
`s1`. With the `.dreq` gone, `ServedRead`'s read-time filter is gone, so
`ServesAny("s1")` holds while `s1 \in erasureRequested` and `dreqR1` is absent,
failing the invariant.
