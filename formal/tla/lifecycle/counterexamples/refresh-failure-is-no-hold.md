# Negative control: refresh-failure-is-no-hold

Switch: `RefreshFailureSweepsAnyway = TRUE` (a failed hold refresh no longer
skips the sweep tick). `FullEnv = TRUE` so the refresh-failed state is reachable.
All other switches at base.

Target invariant: `RefreshFailureNeverSweeps`. TLC exit 12.

```
Error: Invariant RefreshFailureNeverSweeps is violated.
```

Trace: `SetHeadState` then `SetRefresh(TRUE)` puts the refresh in the failed
state; `PerformRewrite` marks the raw inputs superseded; `SupersededSweep`
deletes one while `refreshFailed = TRUE`. The witness records a non-empty
`deleted` with `refreshWasFailed |-> TRUE`, so the invariant
(`deleted # {} => refreshWasFailed = FALSE`) fails.
