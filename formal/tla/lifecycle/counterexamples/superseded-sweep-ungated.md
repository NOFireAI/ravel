# Negative control: superseded-sweep-ungated

Switch: `SupersededSweepUngated = TRUE` (drops the object-granular HEAD gate on
the superseded sweep, so an input the HEAD still names can be deleted). All other
switches at base.

Target invariant: `HeadNamedObjectNeverDeletedBySupersededSweep`. TLC exit 12.

```
Error: Invariant HeadNamedObjectNeverDeletedBySupersededSweep is violated.
```

Trace: `PerformRewrite` writes `rwA` and marks the raw inputs superseded without
advancing HEAD, so the inputs are still HEAD-named. `SupersededSweep` then
deletes one such input. The witness records `rule |-> superseded` with
`headNamed` non-empty, so the invariant (`headNamed = {}`) fails.
