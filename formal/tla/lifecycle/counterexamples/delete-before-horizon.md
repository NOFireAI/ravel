# Negative control: delete-before-horizon

Switch: `DeleteBeforeHorizon = TRUE` (drops the retention horizon gate
`clock >= tombRetiredAt[b1] + sysgc.ph`). All other switches at base.

Target invariant: `NoDeleteInsideProtectionWindow`. TLC exit 12.

```text
Error: Invariant NoDeleteInsideProtectionWindow is violated.
```

Trace: `RetireBucket` writes the tombstone at `clock = 0`; `DropRetiredBucketFromHead`
clears the bucket from HEAD; `RetentionSweep` then deletes the object at
`clock = 0`, before `tombRetiredAt[b1] + sysgc.ph`. The witness records
`rule |-> retention, atClock |-> 0`, so clause 1 (`atClock >= retired_at + ph`)
fails.
